//! The hour-ranged measurement lane (ADR-3692 decision 8):
//! `--ingest-hour-stride` and `--ingest-hour-start`.
//!
//! A strided load writes batch `i` (0-based, counting batches that carry at
//! least one row) into ingest hour `start + i * stride`. It does so by
//! handing the router an [`OffsetClock`] that reads `start + i * stride`
//! hours plus the time elapsed since the batch began, so every reading the
//! write path takes during batch `i` (the flush-open stamp, which sets both
//! `ingest_hour_bucket` and `created_unix_ns`, the age trigger, the routing
//! cache) comes from the shifted clock. The lane forces `--pipeline-depth 1`,
//! `--read-cursors 1` and `--target-bytes 1` (`PER_BATCH_TARGET_BYTES`): each
//! Strict batch flushes inside its own write, one object per shard, and is
//! fully acked before the clock moves to the next batch's hour. The synthetic
//! hours are therefore non-decreasing, and hour `start + i * stride` holds
//! exactly batch `i`'s flushes.

use std::future::Future;
use std::path::Path;
use std::pin::Pin as StdPin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use ravel_catalog::CatalogConfig;
use ravel_ingest::Clock;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};
use ravel_types::{Signal, TenantId};
use serde::{Deserialize, Serialize};

use super::input::{FileInput, read_input_metadata, row_group_row_counts};
use super::{PER_BATCH_TARGET_BYTES, SignalArg};

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// The process exit code `ravel-cli load` uses for an
/// [`IngestHourStrideRefused`], the same code clap uses for a usage error:
/// the invocation, not the data or the store, is what has to change.
pub const INGEST_HOUR_STRIDE_REFUSED_EXIT: i32 = 2;

/// The value the report's `ingest_hour_flush` field carries: the mechanism
/// that confines each batch to one flush per shard.
pub const INGEST_HOUR_FLUSH_MECHANISM: &str = "per_batch_target_bytes";

/// `--ingest-hour-stride` and `--ingest-hour-start`, given together.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestHourStride {
    /// Hours between consecutive batches' ingest hours. At least 1.
    pub stride_hours: u32,
    /// Unix hour (`unix_seconds / 3600`) batch 0 is written into.
    pub start_hour: u32,
}

/// The four fields a strided load adds to the load report JSON, plus the
/// flush mechanism that makes each hour hold exactly one batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyntheticIngestHours {
    pub ingest_hour_stride: u32,
    pub ingest_hour_start: u32,
    /// The ingest hour of the last batch submitted; `None` when no batch
    /// carried a row.
    pub ingest_hour_last: Option<u32>,
    /// Always `true`: the ingest hours were assigned by the loader, not read
    /// off the wall clock.
    pub synthetic_ingest_hours: bool,
    /// [`INGEST_HOUR_FLUSH_MECHANISM`].
    pub ingest_hour_flush: String,
}

/// A refusal of `--ingest-hour-stride` before any object was written. `load`
/// exits [`INGEST_HOUR_STRIDE_REFUSED_EXIT`] on it.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct IngestHourStrideRefused(pub String);

/// A [`Clock`] that reads `inner` plus an offset set by
/// [`OffsetClock::advance_to_hour`]: right after `advance_to_hour(h)` it reads
/// the start of hour `h`, and from then on advances at `inner`'s rate.
/// `sleep` is `inner`'s, so a timer waits real (or test-injected) time.
pub struct OffsetClock {
    inner: Arc<dyn Clock>,
    offset_ns: AtomicI64,
    hour: AtomicU32,
}

impl OffsetClock {
    /// A clock that reads the start of `start_hour` now.
    pub fn new(inner: Arc<dyn Clock>, start_hour: u32) -> Self {
        let clock = Self {
            inner,
            offset_ns: AtomicI64::new(0),
            hour: AtomicU32::new(start_hour),
        };
        clock.anchor(start_hour);
        clock
    }

    fn anchor(&self, hour: u32) {
        let offset = (i64::from(hour) * NS_PER_HOUR).saturating_sub(self.inner.now_ns());
        self.offset_ns.store(offset, Ordering::SeqCst);
        self.hour.store(hour, Ordering::SeqCst);
    }

    /// Re-anchor so the clock reads the start of `hour`. A call naming the
    /// hour already in force changes nothing, so the reading never steps
    /// back within an hour.
    pub fn advance_to_hour(&self, hour: u32) {
        if hour != self.hour.load(Ordering::SeqCst) {
            self.anchor(hour);
        }
    }

    /// The hour the clock was last anchored to.
    pub fn hour(&self) -> u32 {
        self.hour.load(Ordering::SeqCst)
    }

    /// What this clock adds to `inner`'s reading.
    pub fn offset_ns(&self) -> i64 {
        self.offset_ns.load(Ordering::SeqCst)
    }
}

impl Clock for OffsetClock {
    fn now_ns(&self) -> i64 {
        self.inner.now_ns().saturating_add(self.offset_ns())
    }

    fn sleep(&self, dur: Duration) -> StdPin<Box<dyn Future<Output = ()> + Send + '_>> {
        self.inner.sleep(dur)
    }
}

/// A strided load's state: the shifted clock and the batch counter.
pub struct HourStrideLane {
    stride: IngestHourStride,
    clock: Arc<OffsetClock>,
    batches_begun: AtomicU32,
    planned_batches: u64,
}

impl HourStrideLane {
    /// A lane over `inner` that may submit at most `planned_batches` batches.
    pub fn new(stride: IngestHourStride, inner: Arc<dyn Clock>, planned_batches: u64) -> Self {
        Self {
            stride,
            clock: Arc::new(OffsetClock::new(inner, stride.start_hour)),
            batches_begun: AtomicU32::new(0),
            planned_batches,
        }
    }

    /// The shifted clock the router and every loader decision read.
    pub fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock) as Arc<dyn Clock>
    }

    /// `inner`, with its observed store clock shifted by the same offset as
    /// [`Self::clock`]. The shard actor refuses a flush whose clock lags the
    /// store's observed clock beyond the clock-skew allowance (ADR-1685), and
    /// a clock shifted into the past always would on a store that reports its
    /// own time (S3's `Date` header).
    pub fn store(&self, inner: Arc<dyn ObjectStoreBackend>) -> Arc<dyn ObjectStoreBackend> {
        Arc::new(ShiftedObservationStore {
            inner,
            clock: Arc::clone(&self.clock),
        })
    }

    /// Moves the clock to the next batch's hour and returns it. Called
    /// immediately before a batch is submitted, once the previous batch is
    /// acked. Refuses a batch the guard did not plan for, and a previous batch
    /// whose readings left its hour (it took longer than an hour), since
    /// either would break "hour `start + i * stride` holds exactly batch
    /// `i`".
    pub fn begin_batch(&self) -> Result<u32, String> {
        let index = self.batches_begun.load(Ordering::SeqCst);
        if u64::from(index) >= self.planned_batches {
            return Err(format!(
                "--ingest-hour-stride: batch {index} was not planned for: the guard counted \
                 {} batches from the Parquet footer",
                self.planned_batches
            ));
        }
        if index > 0 {
            self.check_hour_kept()?;
        }
        let hour = u64::from(self.stride.start_hour)
            + u64::from(index) * u64::from(self.stride.stride_hours);
        let hour = u32::try_from(hour)
            .map_err(|_| format!("--ingest-hour-stride: batch {index}'s hour {hour} overflows"))?;
        self.clock.advance_to_hour(hour);
        self.batches_begun.store(index + 1, Ordering::SeqCst);
        Ok(hour)
    }

    /// Refuses when the clock has left the hour of the batch in progress.
    /// Run before the next batch and once after the last.
    pub fn check_hour_kept(&self) -> Result<(), String> {
        let hour = self.clock.hour();
        let end_ns = (i64::from(hour) + 1) * NS_PER_HOUR;
        let now_ns = self.clock.now_ns();
        if now_ns >= end_ns {
            return Err(format!(
                "--ingest-hour-stride: the batch written into ingest hour {hour} ran past the end \
                 of that hour (the shifted clock reads {now_ns} ns), so a later flush of it may \
                 have landed in hour {}; lower --batch-rows so one batch takes under an hour",
                u64::from(hour) + 1
            ));
        }
        Ok(())
    }

    /// The report fields for this lane.
    pub fn report(&self) -> SyntheticIngestHours {
        let begun = self.batches_begun.load(Ordering::SeqCst);
        let last = begun.checked_sub(1).and_then(|last| {
            let hour = u64::from(self.stride.start_hour)
                + u64::from(last) * u64::from(self.stride.stride_hours);
            u32::try_from(hour).ok()
        });
        SyntheticIngestHours {
            ingest_hour_stride: self.stride.stride_hours,
            ingest_hour_start: self.stride.start_hour,
            ingest_hour_last: last,
            synthetic_ingest_hours: true,
            ingest_hour_flush: INGEST_HOUR_FLUSH_MECHANISM.to_string(),
        }
    }
}

/// How many non-empty batches a one-cursor load of `total_rows` rows deals at
/// `batch_rows`, after `--skip-rows`: blocks of `batch_rows` file rows, minus
/// those the skip covers whole. An upper bound when a batch maps to no record.
pub fn planned_batches(total_rows: u64, skip_rows: u64, batch_rows: usize) -> u64 {
    let batch_rows = batch_rows.max(1) as u64;
    if skip_rows >= total_rows {
        return 0;
    }
    total_rows.div_ceil(batch_rows) - skip_rows / batch_rows
}

/// The loader flags the lane fixes, as given.
#[derive(Debug, Clone, Copy)]
pub struct StrideLevers {
    pub read_cursors: Option<usize>,
    pub pipeline_depth: Option<usize>,
    pub target_bytes: Option<usize>,
    pub fold_after_load: bool,
}

impl StrideLevers {
    /// The flags with the lane's forced values filled in.
    pub fn forced(self) -> Self {
        Self {
            read_cursors: Some(1),
            pipeline_depth: Some(1),
            target_bytes: Some(PER_BATCH_TARGET_BYTES),
            fold_after_load: self.fold_after_load,
        }
    }
}

fn refused(reason: String) -> anyhow::Error {
    anyhow::Error::new(IngestHourStrideRefused(reason))
}

/// The guard a strided load runs once, before any object is written:
///
/// - the flags the lane fixes, and the signal, are ones it supports;
/// - `start` is strictly before the current hour of `inner`;
/// - `start + batches * stride` is below the current hour, `batches` counted
///   from the Parquet footer with [`planned_batches`];
/// - the signal's catalog HEAD does not exist;
/// - one LIST page of the signal's commit prefix `t/<hex>/<signal>/c/` is
///   empty.
///
/// A failed check is an [`IngestHourStrideRefused`]. A store error while
/// checking is a plain error: the guard could not tell.
#[allow(clippy::too_many_arguments)]
pub async fn prepare_hour_stride(
    stride: IngestHourStride,
    store: &Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
    parquet_path: &Path,
    batch_rows: usize,
    skip_rows: u64,
    levers: StrideLevers,
    inner: Arc<dyn Clock>,
) -> anyhow::Result<HourStrideLane> {
    let signal = match signal {
        SignalArg::Logs => Signal::Logs,
        SignalArg::Spans => Signal::Spans,
        SignalArg::Metrics => {
            return Err(refused(
                "--ingest-hour-stride supports --signal logs and --signal spans; this metrics \
                 load was refused before any row was read or written"
                    .to_string(),
            ));
        }
    };
    let word = signal_word(signal);
    if stride.stride_hours == 0 {
        return Err(refused(
            "--ingest-hour-stride must be at least 1 hour; 0 was given".to_string(),
        ));
    }
    if levers.fold_after_load {
        return Err(refused(
            "--ingest-hour-stride cannot be combined with --fold-after-load: fold the loaded \
             hours with `ravel-cli catalog fold --writers-stopped` once the load has exited"
                .to_string(),
        ));
    }
    for (flag, given, forced) in [
        ("--pipeline-depth", levers.pipeline_depth, 1),
        ("--read-cursors", levers.read_cursors, 1),
        (
            "--target-bytes",
            levers.target_bytes,
            PER_BATCH_TARGET_BYTES,
        ),
    ] {
        if let Some(given) = given
            && given != forced
        {
            return Err(refused(format!(
                "--ingest-hour-stride forces {flag} {forced} (one batch, fully acked, per \
                 synthetic hour); {flag} {given} was given"
            )));
        }
    }

    let now_hour = inner.now_ns().div_euclid(NS_PER_HOUR);
    let start = i64::from(stride.start_hour);
    if start >= now_hour {
        return Err(refused(format!(
            "--ingest-hour-start {start} is not strictly in the past: the current ingest hour is \
             {now_hour}"
        )));
    }
    let metadata = read_input_metadata(&FileInput { path: parquet_path })?;
    let total_rows: u64 = row_group_row_counts(&metadata).iter().sum();
    let batches = planned_batches(total_rows, skip_rows, batch_rows);
    let end = u64::from(stride.start_hour)
        .saturating_add(batches.saturating_mul(u64::from(stride.stride_hours)));
    if i128::from(end) >= i128::from(now_hour) {
        return Err(refused(format!(
            "--ingest-hour-start {start} plus {batches} batches times --ingest-hour-stride {} \
             reaches hour {end}, which is not below the current ingest hour {now_hour}; move \
             the start back or load fewer batches",
            stride.stride_hours
        )));
    }

    let tenant_hash = TenantId::new(tenant).hash();
    let head_key = crate::catalog::head_key(&tenant_hash, signal);
    match store.head(&head_key).await {
        Ok(_) => {
            let catalog =
                crate::catalog::enforcing_catalog(Arc::clone(store), CatalogConfig::default());
            // A HEAD that exists but does not decode still refuses; it only
            // has no watermark to name.
            let watermark = match catalog {
                Ok(catalog) => catalog
                    .head_watermark_hour(&tenant_hash, signal)
                    .await
                    .ok()
                    .flatten(),
                Err(_) => None,
            };
            let watermark = watermark.map_or_else(|| "none".to_string(), |h| h.to_string());
            return Err(refused(format!(
                "ingest-hour-stride is only honored on a tenant whose {word} HEAD does not exist \
                 (found watermark hour {watermark})"
            )));
        }
        Err(StoreError::NotFound) => {}
        Err(err) => {
            anyhow::bail!(
                "--ingest-hour-stride: could not check for the {word} catalog HEAD {head_key}: \
                 {err}"
            )
        }
    }
    let prefix = commit_prefix(&tenant_hash, signal);
    let page = store
        .list(&prefix, None)
        .await
        .map_err(|err| anyhow::anyhow!("--ingest-hour-stride: could not list {prefix}: {err}"))?;
    if let Some(found) = page.objects.first() {
        return Err(refused(format!(
            "ingest-hour-stride is only honored on a tenant with no {word} commit records \
             (found {} under {prefix})",
            found.key
        )));
    }
    Ok(HourStrideLane::new(stride, inner, batches))
}

/// The prefix covering every commit record of one (tenant, signal), across
/// shards and hours: [`ravel_commit::keys::commit_shard_prefix`] without its
/// shard component.
fn commit_prefix(tenant: &ravel_types::TenantHash, signal: Signal) -> String {
    format!("t/{}/{}/c/", tenant.to_hex(), signal.key_prefix())
}

fn signal_word(signal: Signal) -> &'static str {
    match signal {
        Signal::Spans => "spans",
        _ => "logs",
    }
}

/// [`HourStrideLane::store`]'s decorator: every call goes to `inner`, and
/// `observed_store_time_ns` is shifted by the lane clock's offset, so the
/// ingest path's lag check compares two readings on the same shifted scale.
/// The signatures are `async_trait`'s expansion, written out because the
/// macro is a dev-dependency of this crate.
struct ShiftedObservationStore {
    inner: Arc<dyn ObjectStoreBackend>,
    clock: Arc<OffsetClock>,
}

type StoreFuture<'a, T> = StdPin<Box<dyn Future<Output = Result<T, StoreError>> + Send + 'a>>;

impl ObjectStoreBackend for ShiftedObservationStore {
    fn put<'life0, 'life1, 'async_trait>(
        &'life0 self,
        key: &'life1 str,
        data: Bytes,
        opts: PutOptions,
    ) -> StoreFuture<'async_trait, PutOutcome>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.put(key, data, opts)
    }

    fn get<'life0, 'life1, 'async_trait>(
        &'life0 self,
        key: &'life1 str,
        range: GetRange,
    ) -> StoreFuture<'async_trait, GetOutcome>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.get(key, range)
    }

    fn get_pinned<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        key: &'life1 str,
        range: GetRange,
        pin: &'life2 Pin,
    ) -> StoreFuture<'async_trait, PinnedRead>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.get_pinned(key, range, pin)
    }

    fn get_with_pin<'life0, 'life1, 'async_trait>(
        &'life0 self,
        key: &'life1 str,
        range: GetRange,
    ) -> StoreFuture<'async_trait, PinnedRead>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.get_with_pin(key, range)
    }

    fn pin_of<'life0, 'life1, 'async_trait>(
        &'life0 self,
        key: &'life1 str,
    ) -> StoreFuture<'async_trait, (ObjectMeta, Pin)>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.pin_of(key)
    }

    fn put_multipart<'a, 'life0, 'async_trait>(
        &'a self,
        key: &'life0 str,
    ) -> StoreFuture<'async_trait, Box<dyn MultipartUpload + 'a>>
    where
        'a: 'async_trait,
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.put_multipart(key)
    }

    fn head<'life0, 'life1, 'async_trait>(
        &'life0 self,
        key: &'life1 str,
    ) -> StoreFuture<'async_trait, ObjectMeta>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.head(key)
    }

    fn list<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: &'life1 str,
        page: Option<PageToken>,
    ) -> StoreFuture<'async_trait, ListPage>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.list(prefix, page)
    }

    fn list_after<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        prefix: &'life1 str,
        start_after: Option<&'life2 str>,
        page: Option<PageToken>,
    ) -> StoreFuture<'async_trait, ListPage>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.list_after(prefix, start_after, page)
    }

    fn list_delimited<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: &'life1 str,
    ) -> StoreFuture<'async_trait, DelimitedList>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.list_delimited(prefix)
    }

    fn delete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        key: &'life1 str,
    ) -> StoreFuture<'async_trait, ()>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        self.inner.delete(key)
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner
            .observed_store_time_ns()
            .map(|observed| observed.saturating_add(self.clock.offset_ns()))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::load::test_support::FixedClock;

    const NOW_NS: i64 = 1_700_000_000_000_000_000;

    #[test]
    fn planned_batches_counts_blocks_after_the_skip() {
        assert_eq!(planned_batches(24, 0, 1), 24);
        assert_eq!(planned_batches(25, 0, 2), 13);
        assert_eq!(planned_batches(25, 3, 2), 12);
        assert_eq!(planned_batches(25, 4, 2), 11);
        assert_eq!(planned_batches(25, 99, 2), 0);
        assert_eq!(planned_batches(0, 0, 2), 0);
    }

    #[test]
    fn commit_prefix_is_the_shard_prefix_without_its_shard() {
        let tenant = TenantId::new("acme").hash();
        for signal in [Signal::Logs, Signal::Spans] {
            let shard = ravel_commit::keys::commit_shard_prefix(&tenant, signal, 3).unwrap();
            let prefix = commit_prefix(&tenant, signal);
            assert!(shard.starts_with(&prefix), "{shard} under {prefix}");
            assert_eq!(shard[prefix.len()..].matches('/').count(), 1, "{shard}");
        }
    }

    #[test]
    fn lane_steps_hours_per_batch_and_reports_them() {
        let start = u32::try_from(NOW_NS / NS_PER_HOUR - 10).unwrap();
        let lane = HourStrideLane::new(
            IngestHourStride {
                stride_hours: 3,
                start_hour: start,
            },
            Arc::new(FixedClock(NOW_NS)),
            3,
        );
        let clock = lane.clock();
        assert_eq!(clock.now_ns(), i64::from(start) * NS_PER_HOUR);
        assert_eq!(lane.report().ingest_hour_last, None);
        assert_eq!(lane.begin_batch(), Ok(start));
        assert_eq!(lane.begin_batch(), Ok(start + 3));
        assert_eq!(clock.now_ns(), i64::from(start + 3) * NS_PER_HOUR);
        assert_eq!(lane.begin_batch(), Ok(start + 6));
        assert!(
            lane.begin_batch().is_err(),
            "a fourth batch was not planned"
        );
        assert_eq!(
            lane.report(),
            SyntheticIngestHours {
                ingest_hour_stride: 3,
                ingest_hour_start: start,
                ingest_hour_last: Some(start + 6),
                synthetic_ingest_hours: true,
                ingest_hour_flush: "per_batch_target_bytes".to_string(),
            }
        );
    }

    /// A store that reports its own clock reads it on the lane's shifted scale.
    #[test]
    fn lane_store_shifts_the_observed_store_clock() {
        struct Observing(ravel_object_store::memory::MemoryStore, i64);
        #[async_trait::async_trait]
        impl ObjectStoreBackend for Observing {
            async fn put(
                &self,
                key: &str,
                data: Bytes,
                opts: PutOptions,
            ) -> Result<PutOutcome, StoreError> {
                self.0.put(key, data, opts).await
            }
            async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
                self.0.get(key, range).await
            }
            async fn put_multipart<'a>(
                &'a self,
                key: &str,
            ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
                self.0.put_multipart(key).await
            }
            async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
                self.0.head(key).await
            }
            async fn list(
                &self,
                prefix: &str,
                page: Option<PageToken>,
            ) -> Result<ListPage, StoreError> {
                self.0.list(prefix, page).await
            }
            async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
                self.0.list_delimited(prefix).await
            }
            async fn delete(&self, key: &str) -> Result<(), StoreError> {
                self.0.delete(key).await
            }
            fn capabilities(&self) -> Capabilities {
                self.0.capabilities()
            }
            fn observed_store_time_ns(&self) -> Option<i64> {
                Some(self.1)
            }
        }
        let start = u32::try_from(NOW_NS / NS_PER_HOUR - 48).unwrap();
        let lane = HourStrideLane::new(
            IngestHourStride {
                stride_hours: 1,
                start_hour: start,
            },
            Arc::new(FixedClock(NOW_NS)),
            1,
        );
        let store = lane.store(Arc::new(Observing(
            ravel_object_store::memory::MemoryStore::new(),
            NOW_NS,
        )));
        assert_eq!(
            store.observed_store_time_ns(),
            Some(lane.clock().now_ns()),
            "the store's clock and the lane's are read on one scale"
        );
    }
}
