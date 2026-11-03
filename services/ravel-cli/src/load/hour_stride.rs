//! The hour-ranged measurement lane (ADR-3692 decision 8):
//! `--ingest-hour-stride` and `--ingest-hour-start`.
//!
//! A strided load writes batch `i` (0-based, counting batches that carry at
//! least one row) into ingest hour `start + i * stride`. It does so by
//! handing the router an [`OffsetClock`] that reads `start + i * stride`
//! hours plus the time elapsed since the batch began. The router's and shard
//! actors' readings during batch `i` (the flush-open stamp, which sets both
//! `ingest_hour_bucket` and `created_unix_ns`, the age trigger, the routing
//! cache) come from the shifted clock. The loader's own `now_ns`, read once
//! at the start of the load, is not shifted: the decode admission check
//! (future skew) and the provisioning record use the real time. The lane
//! forces `--pipeline-depth 1`, `--read-cursors 1` and `--target-bytes 1`
//! (`PER_BATCH_TARGET_BYTES`): each Strict batch flushes inside its own
//! write, one object per shard at the tested batch sizes, and is fully acked
//! before the clock moves to the next batch's hour. The synthetic hours are
//! therefore non-decreasing, and hour `start + i * stride` holds exactly
//! batch `i`'s flushes.
//!
//! Before each batch [`HourStrideLane::admit_batch`] re-reads the signal's
//! catalog HEAD and checks the batch's latest event time against its hour's
//! end plus the catalog's `max_ingest_lag`, so a fold that ran during the
//! load, or a row the catalog's listing window would not reach, stops the
//! load before that batch writes anything.

use std::future::Future;
use std::path::Path;
use std::pin::Pin as StdPin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::time::Duration;

use bytes::Bytes;
use ravel_catalog::{CatalogConfig, DEFAULT_MAX_CATALOG_LIST_REQUESTS, DEFAULT_MAX_INGEST_LAG_NS};
use ravel_ingest::{Clock, MIN_PLAUSIBLE_INGEST_CLOCK_NS};
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError,
};
use ravel_types::{Signal, TenantHash, TenantId};
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

/// Why a strided load stopped before submitting batch `batch`, once the load
/// has begun. Nothing of that batch was written; earlier batches are durable.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HourStrideBatchError {
    #[error(
        "--ingest-hour-stride: batch {batch} was not planned for: the guard counted {planned} \
         batches from the Parquet footer"
    )]
    Unplanned { batch: u32, planned: u64 },
    #[error("--ingest-hour-stride: batch {batch}'s hour {hour} overflows")]
    HourOverflow { batch: u32, hour: u64 },
    #[error(
        "--ingest-hour-stride: the batch written into ingest hour {hour} ran past the end of \
         that hour (the shifted clock reads {now_ns} ns), so a later flush of it may have \
         landed in hour {next_hour}; lower --batch-rows so one batch takes under an hour"
    )]
    HourOverrun {
        hour: u32,
        now_ns: i64,
        next_hour: u64,
    },
    #[error(
        "--ingest-hour-stride stopped the load before batch {batch}: the {signal} catalog HEAD \
         appeared during the load (found watermark hour {watermark}). A fold or maintain \
         process ran against the tenant and may have sealed the hours this batch and later \
         ones would commit into, where queries without a commit token would not see them"
    )]
    HeadAppeared {
        batch: u32,
        signal: &'static str,
        watermark: String,
    },
    #[error(
        "--ingest-hour-stride stopped the load before batch {batch}: could not check for the \
         {signal} catalog HEAD: {cause}"
    )]
    HeadUnreadable {
        batch: u32,
        signal: &'static str,
        cause: String,
    },
    #[error(
        "--ingest-hour-stride refused batch {batch} before submitting it: its latest event time \
         {max_event_ns} ns is not before {bound_ns} ns, the end of its ingest hour {hour} plus \
         max_ingest_lag ({lag_ns} ns). A query window starting after that bound does not list \
         hour {hour}, so those rows would be stored and not found; give each batch event times \
         before its hour's end plus the lag"
    )]
    EventTimePastLag {
        batch: u32,
        hour: u32,
        max_event_ns: i64,
        bound_ns: i64,
        lag_ns: i64,
    },
}

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

/// Where a lane reads the catalog HEAD it checks before every batch.
pub struct LaneTarget {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub tenant: TenantHash,
    pub signal: Signal,
}

/// A strided load's state: the shifted clock, the batch counter, and the
/// (store, tenant, signal) whose HEAD must stay absent.
pub struct HourStrideLane {
    stride: IngestHourStride,
    clock: Arc<OffsetClock>,
    batches_begun: AtomicU32,
    planned_batches: u64,
    target: LaneTarget,
    max_ingest_lag_ns: i64,
}

impl HourStrideLane {
    /// A lane over `inner` that may submit at most `planned_batches` batches
    /// into `target`.
    pub fn new(
        stride: IngestHourStride,
        inner: Arc<dyn Clock>,
        planned_batches: u64,
        target: LaneTarget,
    ) -> Self {
        Self {
            stride,
            clock: Arc::new(OffsetClock::new(inner, stride.start_hour)),
            batches_begun: AtomicU32::new(0),
            planned_batches,
            target,
            max_ingest_lag_ns: DEFAULT_MAX_INGEST_LAG_NS,
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

    /// Everything checked before the next batch is submitted, then the clock
    /// move: one HEAD request confirming the signal's catalog HEAD is still
    /// absent (a fold during the load would have sealed hours later batches
    /// write into), then [`Self::begin_batch`]. `max_event_ns` is the batch's
    /// latest event time, as its commit records will carry it.
    pub async fn admit_batch(&self, max_event_ns: i64) -> Result<u32, HourStrideBatchError> {
        let batch = self.batches_begun.load(Ordering::SeqCst);
        let signal = signal_word(self.target.signal);
        match head_watermark(&self.target.store, &self.target.tenant, self.target.signal).await {
            Ok(None) => {}
            Ok(Some(watermark)) => {
                return Err(HourStrideBatchError::HeadAppeared {
                    batch,
                    signal,
                    watermark,
                });
            }
            Err(err) => {
                return Err(HourStrideBatchError::HeadUnreadable {
                    batch,
                    signal,
                    cause: err.to_string(),
                });
            }
        }
        self.begin_batch(max_event_ns)
    }

    /// Moves the clock to the next batch's hour and returns it. Called
    /// immediately before a batch is submitted, once the previous batch is
    /// acked. Refuses a batch the guard did not plan for, a previous batch
    /// whose readings left its hour (it took longer than an hour), since
    /// either would break "hour `start + i * stride` holds exactly batch
    /// `i`", and a batch whose latest event time is at or past its hour's end
    /// plus `max_ingest_lag`, which the catalog's listing window would miss.
    /// A refusal leaves the clock where it was.
    pub fn begin_batch(&self, max_event_ns: i64) -> Result<u32, HourStrideBatchError> {
        let batch = self.batches_begun.load(Ordering::SeqCst);
        if u64::from(batch) >= self.planned_batches {
            return Err(HourStrideBatchError::Unplanned {
                batch,
                planned: self.planned_batches,
            });
        }
        if batch > 0 {
            self.check_hour_kept()?;
        }
        let hour = u64::from(self.stride.start_hour)
            + u64::from(batch) * u64::from(self.stride.stride_hours);
        let hour =
            u32::try_from(hour).map_err(|_| HourStrideBatchError::HourOverflow { batch, hour })?;
        let bound_ns = (i64::from(hour) + 1)
            .saturating_mul(NS_PER_HOUR)
            .saturating_add(self.max_ingest_lag_ns);
        if max_event_ns >= bound_ns {
            return Err(HourStrideBatchError::EventTimePastLag {
                batch,
                hour,
                max_event_ns,
                bound_ns,
                lag_ns: self.max_ingest_lag_ns,
            });
        }
        self.clock.advance_to_hour(hour);
        self.batches_begun.store(batch + 1, Ordering::SeqCst);
        Ok(hour)
    }

    /// Refuses when the clock has left the hour of the batch in progress.
    /// Run before the next batch and once after the last.
    pub fn check_hour_kept(&self) -> Result<(), HourStrideBatchError> {
        let hour = self.clock.hour();
        let end_ns = (i64::from(hour) + 1) * NS_PER_HOUR;
        let now_ns = self.clock.now_ns();
        if now_ns >= end_ns {
            return Err(HourStrideBatchError::HourOverrun {
                hour,
                now_ns,
                next_hour: u64::from(hour) + 1,
            });
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
/// - `start` is not below the write path's plausible-clock floor,
///   [`MIN_PLAUSIBLE_INGEST_CLOCK_NS`];
/// - the signal's catalog HEAD does not exist;
/// - the signal's commit prefix `t/<hex>/<signal>/c/` lists no addressable
///   key, walking pages until one appears or the listing ends.
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
    let floor_hour = MIN_PLAUSIBLE_INGEST_CLOCK_NS.div_euclid(NS_PER_HOUR);
    if start < floor_hour {
        return Err(refused(format!(
            "--ingest-hour-start {start} is below hour {floor_hour}, the earliest ingest clock \
             the write path accepts (MIN_PLAUSIBLE_INGEST_CLOCK_NS, 2020-01-01T00:00:00Z)"
        )));
    }
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
    match head_watermark(store, &tenant_hash, signal).await {
        Ok(Some(watermark)) => {
            return Err(refused(format!(
                "ingest-hour-stride is only honored on a tenant whose {word} HEAD does not exist \
                 (found watermark hour {watermark})"
            )));
        }
        Ok(None) => {}
        Err(err) => {
            anyhow::bail!(
                "--ingest-hour-stride: could not check for the {word} catalog HEAD {}: {err}",
                crate::catalog::head_key(&tenant_hash, signal)
            )
        }
    }
    let prefix = commit_prefix(&tenant_hash, signal);
    if let Some(found) = first_listed_key(store, &prefix).await? {
        return Err(refused(format!(
            "ingest-hour-stride is only honored on a tenant with no {word} commit records \
             (found {found} under {prefix})"
        )));
    }
    Ok(HourStrideLane::new(
        stride,
        inner,
        batches,
        LaneTarget {
            store: Arc::clone(store),
            tenant: tenant_hash,
            signal,
        },
    ))
}

/// The watermark hour of the signal's catalog HEAD, `"none"` for a HEAD
/// that exists but does not decode, or `None` when there is no HEAD.
async fn head_watermark(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant: &TenantHash,
    signal: Signal,
) -> Result<Option<String>, StoreError> {
    match store.head(&crate::catalog::head_key(tenant, signal)).await {
        Ok(_) => {}
        Err(StoreError::NotFound) => return Ok(None),
        Err(err) => return Err(err),
    }
    let watermark =
        match crate::catalog::enforcing_catalog(Arc::clone(store), CatalogConfig::default()) {
            Ok(catalog) => catalog
                .head_watermark_hour(tenant, signal)
                .await
                .ok()
                .flatten(),
            Err(_) => None,
        };
    Ok(Some(
        watermark.map_or_else(|| "none".to_string(), |h| h.to_string()),
    ))
}

/// The first addressable key under `prefix`, walking pages past ones that
/// carry no addressable key (a page can hold only unaddressable ones) until
/// the listing ends, within the catalog's list-request cap.
async fn first_listed_key(
    store: &Arc<dyn ObjectStoreBackend>,
    prefix: &str,
) -> anyhow::Result<Option<String>> {
    let mut token = None;
    for _ in 0..DEFAULT_MAX_CATALOG_LIST_REQUESTS {
        let page = store
            .list(prefix, token)
            .await
            .map_err(|err| anyhow::anyhow!("--ingest-hour-stride: could not list {prefix}: {err}"))?;
        if let Some(found) = page.objects.first() {
            return Ok(Some(found.key.clone()));
        }
        match page.next {
            Some(next) => token = Some(next),
            None => return Ok(None),
        }
    }
    anyhow::bail!(
        "--ingest-hour-stride: listing {prefix} did not end within \
         {DEFAULT_MAX_CATALOG_LIST_REQUESTS} pages"
    )
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

    fn target() -> LaneTarget {
        LaneTarget {
            store: Arc::new(ravel_object_store::memory::MemoryStore::new()),
            tenant: TenantId::new("acme").hash(),
            signal: Signal::Logs,
        }
    }

    fn event_in(hour: u32) -> i64 {
        i64::from(hour) * NS_PER_HOUR
    }

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
            target(),
        );
        let clock = lane.clock();
        assert_eq!(clock.now_ns(), i64::from(start) * NS_PER_HOUR);
        assert_eq!(lane.report().ingest_hour_last, None);
        assert_eq!(lane.begin_batch(event_in(start)), Ok(start));
        assert_eq!(lane.begin_batch(event_in(start + 3)), Ok(start + 3));
        assert_eq!(clock.now_ns(), i64::from(start + 3) * NS_PER_HOUR);
        assert_eq!(lane.begin_batch(event_in(start + 6)), Ok(start + 6));
        assert_eq!(
            lane.begin_batch(event_in(start + 9)),
            Err(HourStrideBatchError::Unplanned {
                batch: 3,
                planned: 3
            }),
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

    /// The bound is exclusive at the hour's end plus the lag, and a refused
    /// batch leaves the clock and the batch count where they were.
    #[test]
    fn begin_batch_refuses_events_past_the_hour_end_plus_the_lag() {
        let start = u32::try_from(NOW_NS / NS_PER_HOUR - 10).unwrap();
        let lane = HourStrideLane::new(
            IngestHourStride {
                stride_hours: 1,
                start_hour: start,
            },
            Arc::new(FixedClock(NOW_NS)),
            2,
            target(),
        );
        let bound_ns = event_in(start + 1) + DEFAULT_MAX_INGEST_LAG_NS;
        assert_eq!(
            lane.begin_batch(bound_ns),
            Err(HourStrideBatchError::EventTimePastLag {
                batch: 0,
                hour: start,
                max_event_ns: bound_ns,
                bound_ns,
                lag_ns: DEFAULT_MAX_INGEST_LAG_NS,
            })
        );
        assert_eq!(lane.report().ingest_hour_last, None);
        assert_eq!(lane.begin_batch(bound_ns - 1), Ok(start));
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
            target(),
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
