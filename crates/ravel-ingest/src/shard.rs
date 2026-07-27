//! One actor per shard: actor-local buffering, adaptive flush, and the
//! pinned-identity commit sequence (docs/ingest.md "Shard actor",
//! docs/catalog-and-mvcc.md "Pinned flush identity" and "Commit sequence").

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use bytes::Bytes;
use rand::RngExt as _;
use ravel_commit::keys;
use ravel_commit::publish::{self, PublishError, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::NormalizedPoint;
use ravel_proto::commit::v1::CommitRecord;
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_types::{CommitToken, LabelSet, Sample, SeriesId, Signal, TenantId};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;
use uuid::Uuid;

use crate::clock::Clock;
use crate::config::{IngestConfig, SEGMENT_FORMAT_V1, SEGMENT_FORMAT_V2};
use crate::error::WriteError;
use crate::metrics::{FlushTrigger, IngestMetrics};

const NS_PER_HOUR: i64 = 3_600_000_000_000;

pub(crate) type Ack = oneshot::Sender<Result<CommitToken, WriteError>>;

pub(crate) enum ShardMsg {
    Write {
        tenant: TenantId,
        points: Vec<NormalizedPoint>,
        ack: Option<Ack>,
    },
    /// Flush every buffered tenant now, regardless of size/age thresholds.
    FlushNow { done: oneshot::Sender<()> },
    /// Flush every buffered tenant, then stop the actor loop.
    Shutdown { done: oneshot::Sender<()> },
}

struct SeriesAccum {
    labels: LabelSet,
    samples: Vec<Sample>,
}

#[derive(Default)]
struct TenantBuf {
    series: HashMap<SeriesId, SeriesAccum>,
    est_bytes: usize,
    oldest_arrival_ns: Option<i64>,
    min_ingest_ts_ns: Option<i64>,
    max_ingest_ts_ns: Option<i64>,
    waiters: Vec<Ack>,
}

impl TenantBuf {
    fn note_arrival(&mut self, arrival_ns: i64) {
        self.oldest_arrival_ns.get_or_insert(arrival_ns);
        self.min_ingest_ts_ns = Some(match self.min_ingest_ts_ns {
            Some(m) => m.min(arrival_ns),
            None => arrival_ns,
        });
        self.max_ingest_ts_ns = Some(match self.max_ingest_ts_ns {
            Some(m) => m.max(arrival_ns),
            None => arrival_ns,
        });
    }

    /// Merges `points` into this buffer, returning the estimated byte cost
    /// added (samples * 16 plus label bytes the first time a series is
    /// seen), per docs/ingest.md's `est_bytes` rule.
    ///
    /// Fails loud on a series-id collision (ADR-0005): before mutating the
    /// buffer, every incoming point's `series_id` is checked against the
    /// canonical label set that id already claims, whether from a series
    /// already buffered for this tenant or from an earlier point in this same
    /// batch. Two distinct label sets under one id return
    /// [`WriteError::SeriesIdCollision`] and the buffer is left untouched, so
    /// the accepted stream for non-colliding series is unaffected. Without
    /// this check a collision would silently merge the losing series' samples
    /// under the winning label set (the id-keyed `HashMap` below cannot tell
    /// them apart), which ADR-0005 forbids.
    fn merge(
        &mut self,
        points: Vec<NormalizedPoint>,
        arrival_ns: i64,
    ) -> Result<usize, WriteError> {
        let mut batch_labels: HashMap<SeriesId, &LabelSet> = HashMap::new();
        for point in &points {
            let claimed = self
                .series
                .get(&point.series_id)
                .map(|accum| &accum.labels)
                .or_else(|| batch_labels.get(&point.series_id).copied());
            match claimed {
                Some(labels) if *labels != point.labels => {
                    return Err(WriteError::SeriesIdCollision(format!(
                        "series_id {:?} maps to two distinct label sets in one shard buffer",
                        point.series_id
                    )));
                }
                Some(_) => {}
                None => {
                    batch_labels.insert(point.series_id, &point.labels);
                }
            }
        }
        drop(batch_labels);

        self.note_arrival(arrival_ns);
        let mut bytes_added = 0usize;
        for point in points {
            match self.series.entry(point.series_id) {
                Entry::Occupied(mut occ) => {
                    occ.get_mut().samples.push(point.sample);
                }
                Entry::Vacant(vac) => {
                    let label_bytes: usize = point
                        .labels
                        .iter()
                        .map(|l| l.name.len() + l.value.len())
                        .sum();
                    bytes_added += label_bytes;
                    vac.insert(SeriesAccum {
                        labels: point.labels,
                        samples: vec![point.sample],
                    });
                }
            }
            bytes_added += 16;
        }
        self.est_bytes += bytes_added;
        Ok(bytes_added)
    }
}

pub(crate) struct ShardActor {
    shard: u32,
    signal: Signal,
    writer_id: Uuid,
    epoch: u64,
    next_seq: u64,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<dyn Clock>,
    config: IngestConfig,
    metrics: Arc<IngestMetrics>,
    rx: mpsc::Receiver<ShardMsg>,
    tenants: HashMap<TenantId, TenantBuf>,
}

impl ShardActor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        shard: u32,
        signal: Signal,
        writer_id: Uuid,
        epoch: u64,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<dyn Clock>,
        config: IngestConfig,
        metrics: Arc<IngestMetrics>,
        rx: mpsc::Receiver<ShardMsg>,
    ) -> Self {
        ShardActor {
            shard,
            signal,
            writer_id,
            epoch,
            next_seq: 0,
            store,
            clock,
            config,
            metrics,
            rx,
            tenants: HashMap::new(),
        }
    }

    pub(crate) async fn run(mut self) {
        let mut ticker = tokio::time::interval(self.config.flush_tick);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(ShardMsg::Write { tenant, points, ack }) => {
                            self.handle_write(tenant, points, ack).await;
                        }
                        Some(ShardMsg::FlushNow { done }) => {
                            self.flush_all(FlushTrigger::Manual).await;
                            let _ = done.send(());
                        }
                        Some(ShardMsg::Shutdown { done }) => {
                            self.flush_all(FlushTrigger::Manual).await;
                            let _ = done.send(());
                            break;
                        }
                        None => break,
                    }
                }
                _ = ticker.tick() => {
                    self.flush_aged().await;
                }
            }
        }
    }

    async fn handle_write(
        &mut self,
        tenant: TenantId,
        points: Vec<NormalizedPoint>,
        ack: Option<Ack>,
    ) {
        if points.is_empty() && ack.is_none() {
            return;
        }
        let arrival_ns = self.clock.now_ns();
        let points_len = points.len() as u64;
        let target_bytes = self.config.target_bytes;

        let buf = self.tenants.entry(tenant.clone()).or_default();
        // Merge before enqueuing the waiter: a series-id collision rejects
        // the whole batch fail-loud (ADR-0005) and leaves the buffer
        // untouched, so its ack must carry the error rather than ride the
        // next flush of the surviving series.
        let bytes_added = match buf.merge(points, arrival_ns) {
            Ok(bytes_added) => bytes_added,
            Err(err) => {
                self.metrics.record_series_id_collision();
                if let Some(ack) = ack {
                    self.ack_waiters(vec![ack], Err(err));
                }
                return;
            }
        };
        if let Some(ack) = ack {
            buf.waiters.push(ack);
        }
        self.metrics.record_buffered(bytes_added as u64, points_len);

        let should_flush = self
            .tenants
            .get(&tenant)
            .map(|b| b.est_bytes >= target_bytes)
            .unwrap_or(false);
        if should_flush && let Some(buf) = self.tenants.remove(&tenant) {
            self.flush_tenant(tenant, buf, FlushTrigger::Size).await;
        }
    }

    async fn flush_aged(&mut self) {
        let now = self.clock.now_ns();
        let max_delay_ns = self.config.max_flush_delay.as_nanos() as i64;
        let due: Vec<TenantId> = self
            .tenants
            .iter()
            .filter(|(_, buf)| {
                buf.oldest_arrival_ns
                    .map(|t| now.saturating_sub(t) >= max_delay_ns)
                    .unwrap_or(false)
            })
            .map(|(t, _)| t.clone())
            .collect();
        for tenant in due {
            if let Some(buf) = self.tenants.remove(&tenant) {
                self.flush_tenant(tenant, buf, FlushTrigger::Age).await;
            }
        }
    }

    async fn flush_all(&mut self, trigger: FlushTrigger) {
        let tenants: Vec<TenantId> = self.tenants.keys().cloned().collect();
        for tenant in tenants {
            if let Some(buf) = self.tenants.remove(&tenant) {
                self.flush_tenant(tenant, buf, trigger).await;
            }
        }
    }

    /// Runs the full pinned-identity commit sequence for one tenant's
    /// buffer: `seq`, `ingest_hour_bucket`, the serialized segment, and its
    /// blake3 hash are each computed exactly once here and reused verbatim
    /// by every retry inside `put_data_object_with_retry` and
    /// `publish_with_retry` (docs/catalog-and-mvcc.md "Pinned flush
    /// identity"). Nothing below may re-serialize, accrete new samples, or
    /// re-read the clock.
    async fn flush_tenant(&mut self, tenant: TenantId, buf: TenantBuf, trigger: FlushTrigger) {
        let TenantBuf {
            series,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            waiters,
            ..
        } = buf;
        if series.is_empty() {
            return;
        }
        self.metrics.record_flush(trigger);

        let tenant_hash = tenant.hash();
        let seq = self.next_seq;
        self.next_seq += 1;
        let flush_open_ns = self.clock.now_ns();
        let ingest_hour_bucket = u32::try_from(flush_open_ns.div_euclid(NS_PER_HOUR)).unwrap_or(0);
        let deadline_ns =
            flush_open_ns.saturating_add(self.config.max_flush_lifetime.as_nanos() as i64);

        let series_inputs: Vec<SeriesInput> = series
            .into_iter()
            .map(|(series_id, accum)| SeriesInput {
                series_id,
                labels: accum.labels,
                samples: accum.samples,
            })
            .collect();
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: self.shard,
            writer_id: self.writer_id.to_string(),
            writer_epoch: self.epoch,
            writer_seq: seq,
        };
        let min_ingest_ts_ns = min_ingest_ts_ns.unwrap_or(flush_open_ns);
        let max_ingest_ts_ns = max_ingest_ts_ns.unwrap_or(flush_open_ns);
        let ingest_bounds = IngestBounds {
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        };

        // Resolved exactly once and reused for both the writer branch below
        // and the commit record's `segment_format_version` stamp further
        // down: an unrecognized config value normalizes to
        // `SEGMENT_FORMAT_V1` here, at the single read site, so the writer
        // call and the stamp can never disagree about which version this
        // flush actually produced (docs/rseg-v2-plan.md P6).
        let segment_version = match self.config.segment_format_version {
            SEGMENT_FORMAT_V2 => SEGMENT_FORMAT_V2,
            _ => SEGMENT_FORMAT_V1,
        };
        let written = match segment_version {
            SEGMENT_FORMAT_V2 => SegmentWriter::write_v2(series_inputs, identity, ingest_bounds),
            _ => SegmentWriter::write(series_inputs, identity, ingest_bounds),
        };
        let written = match written {
            Ok(w) => w,
            Err(e) => {
                self.metrics.record_abandoned();
                self.ack_waiters(waiters, Err(WriteError::SegmentBuild(e.to_string())));
                return;
            }
        };

        let data_key = match keys::data_key(
            &tenant_hash,
            self.signal,
            self.shard,
            self.writer_id,
            self.epoch,
            seq,
            &written.summary.blake3,
        ) {
            Ok(k) => k,
            Err(e) => {
                self.metrics.record_abandoned();
                self.ack_waiters(waiters, Err(WriteError::SegmentBuild(e.to_string())));
                return;
            }
        };

        if !self
            .put_data_object_with_retry(&data_key, written.bytes.clone(), deadline_ns)
            .await
        {
            self.metrics.record_abandoned();
            self.ack_waiters(
                waiters,
                Err(WriteError::Abandoned(
                    "data object put exhausted retry budget or exceeded max_flush_lifetime".into(),
                )),
            );
            return;
        }

        let record = match record::build(NewCommitRecord {
            tenant_hash,
            signal: self.signal,
            shard: self.shard,
            writer_id: self.writer_id,
            writer_epoch: self.epoch,
            writer_seq: seq,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            segment_format_version: u32::from(segment_version),
            created_unix_ns: flush_open_ns,
            ingest_hour_bucket,
        }) {
            Ok(r) => r,
            Err(e) => {
                self.metrics.record_abandoned();
                self.ack_waiters(waiters, Err(WriteError::SegmentBuild(e.to_string())));
                return;
            }
        };

        match self.publish_with_retry(&record, deadline_ns).await {
            Some(token) => {
                self.ack_waiters(waiters, Ok(token));
            }
            None => {
                self.metrics.record_abandoned();
                self.ack_waiters(
                    waiters,
                    Err(WriteError::Abandoned(
                        "commit publish exhausted retry budget or exceeded max_flush_lifetime"
                            .into(),
                    )),
                );
            }
        }
    }

    fn ack_waiters(&self, waiters: Vec<Ack>, result: Result<CommitToken, WriteError>) {
        let ok = result.is_ok();
        self.metrics.record_acks(waiters.len(), ok);
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }

    /// Retries the data-object PUT with the caller's own budget (separate
    /// from `ravel_commit::publish`'s internal `RetryPolicy`, which only
    /// governs the commit-record PUT). Reuses the same pinned `key`/`bytes`
    /// on every attempt; `put_data_object` never re-derives either.
    async fn put_data_object_with_retry(&self, key: &str, bytes: Bytes, deadline_ns: i64) -> bool {
        let mut attempt: u32 = 0;
        loop {
            match publish::put_data_object(self.store.as_ref(), key, bytes.clone()).await {
                Ok(()) => return true,
                Err(PublishError::Store { source, .. }) if source.is_retryable() => {
                    attempt += 1;
                    if attempt >= self.config.put_retry_max_attempts
                        || self.clock.now_ns() >= deadline_ns
                    {
                        return false;
                    }
                    self.metrics.record_put_retry();
                    self.backoff_sleep(attempt).await;
                }
                Err(_) => return false,
            }
        }
    }

    /// Retries the commit-record PUT with the caller's own budget. Passes
    /// `ravel_commit::publish::publish` a zero-retry policy so it attempts
    /// exactly once per call, letting this loop check `deadline_ns` between
    /// attempts (the crate's own internal retry loop has no such hook).
    async fn publish_with_retry(
        &self,
        record: &CommitRecord,
        deadline_ns: i64,
    ) -> Option<CommitToken> {
        let single_attempt = RetryPolicy {
            max_attempts: 0,
            base_delay: self.config.put_retry_base_delay,
            max_delay: self.config.put_retry_max_delay,
        };
        let mut attempt: u32 = 0;
        loop {
            match publish::publish(self.store.as_ref(), record, &single_attempt).await {
                Ok(token) => return Some(token),
                Err(PublishError::SplitBrain { this, stored }) => {
                    // Identity is pinned at flush open, so this cannot fire
                    // on a benign retry (docs/catalog-and-mvcc.md "Commit
                    // sequence"); it means the pinning invariant was broken
                    // upstream. Crash loudly rather than silently corrupt.
                    panic!(
                        "ravel-ingest: fatal split-brain on pinned flush identity: this={this} stored={stored}"
                    );
                }
                Err(PublishError::Store { source, .. }) if source.is_retryable() => {
                    attempt += 1;
                    if attempt >= self.config.put_retry_max_attempts
                        || self.clock.now_ns() >= deadline_ns
                    {
                        return None;
                    }
                    self.metrics.record_put_retry();
                    self.backoff_sleep(attempt).await;
                }
                Err(_) => return None,
            }
        }
    }

    async fn backoff_sleep(&self, attempt: u32) {
        let shift = attempt.min(20);
        let exp = self
            .config
            .put_retry_base_delay
            .saturating_mul(1u32 << shift);
        let capped = exp.min(self.config.put_retry_max_delay);
        let capped_ms = u64::try_from(capped.as_millis()).unwrap_or(u64::MAX);
        let jittered_ms = rand::rng().random_range(0..=capped_ms);
        tokio::time::sleep(Duration::from_millis(jittered_ms)).await;
    }
}
