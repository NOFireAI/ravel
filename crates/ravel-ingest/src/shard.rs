//! One actor per shard: actor-local buffering, adaptive flush, and the
//! pinned-identity commit sequence (docs/ingest.md "Shard actor",
//! docs/catalog-and-mvcc.md "Pinned flush identity" and "Commit sequence").
//!
//! Buffer ownership and flush execution are split (ADR-0067 decision 1): the
//! actor is the single-threaded owner of buffered state and, at flush
//! trigger, pins the flush's identity and moves its `TenantBuf` (including
//! its waiters) into a task spawned onto [`FlushCtx::run_flush`], then keeps
//! draining its channel. `max_inflight_flushes` (ADR-0067 decision 2) bounds
//! how many such tasks may run at once per shard via a semaphore acquired
//! before spawning; at the bound, the acquire blocks the flush trigger (and
//! therefore the actor's ability to pull its next message), which is exactly
//! where backpressure is meant to propagate. The adaptive age trigger
//! (ADR-0067 decision 3) is `age_threshold_ns`/`adaptive_age_threshold_ns`
//! below.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;

use bytes::Bytes;
use ravel_commit::keys;
use ravel_commit::publish::{self, PublishError, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::rng::RngSource;
use ravel_object_store::ObjectStoreBackend;
use ravel_proto::commit::v1::CommitRecord;
use ravel_segment::{
    ExemplarInput, HistogramSample, IngestBounds, SegmentIdentity, SegmentWriter, SeriesInputV3,
    SeriesValues,
};
use ravel_types::{
    CommitToken, ExemplarCap, Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Duration;
use uuid::Uuid;

use crate::budget::IngestByteCharge;
use crate::clock::Clock;
use crate::config::{IngestConfig, SEGMENT_FORMAT_VERSION, checked_ingest_hour_bucket};
use crate::error::WriteError;
use crate::metrics::{FlushTrigger, IngestMetrics};
#[cfg(feature = "stage-timing")]
use crate::stage_timing::{MetricStage, MetricStageTimings};
use crate::value::{IngestExemplar, IngestPoint, IngestValue, ValueKind};

pub(crate) type Ack = oneshot::Sender<Result<CommitToken, WriteError>>;

pub(crate) enum ShardMsg {
    Write {
        tenant: TenantId,
        points: Vec<IngestPoint>,
        /// Exemplars for series routed to this shard (ADR-0047). Routed by the
        /// same `shard_for(series_id)` their samples are, so an exemplar and
        /// the sample it illustrates always land in the same actor's buffer
        /// and therefore in the same flushed object.
        exemplars: Vec<IngestExemplar>,
        ack: Option<Ack>,
        /// This request's global ingest-byte-budget charge (ADR-0069), cloned
        /// into every shard message the request fanned out to. The shard holds
        /// it in the tenant buffer and moves it into the flush task, whose
        /// completion (or failure) drops it and refunds the bytes. Held as an
        /// `Arc` so one request's single charge is refunded exactly once, when
        /// the last shard buffer holding a clone has flushed. `None` only for a
        /// test write that bypasses the budget (production always charges via
        /// [`crate::IngestRouter`]).
        charge: Option<Arc<IngestByteCharge>>,
    },
    /// Flush every buffered tenant now, regardless of size/age thresholds.
    FlushNow { done: oneshot::Sender<()> },
    /// Flush every buffered tenant, then stop the actor loop.
    Shutdown { done: oneshot::Sender<()> },
}

/// A series' accumulated sample payload for one shard buffer: exactly one
/// of scalar or histogram samples, fixed for the series' whole life in
/// the buffer (mirrors `ravel_segment::SeriesValues`'s v3 invariant, but
/// kept as its own type so v1/v2-only ravel-ingest callers never need to
/// know about `ravel_segment::SeriesValues`).
enum SeriesAccumValues {
    Scalar(Vec<Sample>),
    Histogram(Vec<HistogramSample>),
}

impl SeriesAccumValues {
    fn kind(&self) -> ValueKind {
        match self {
            SeriesAccumValues::Scalar(_) => ValueKind::Scalar,
            SeriesAccumValues::Histogram(_) => ValueKind::Histogram,
        }
    }

    fn len(&self) -> usize {
        match self {
            SeriesAccumValues::Scalar(v) => v.len(),
            SeriesAccumValues::Histogram(v) => v.len(),
        }
    }

    fn new_with(value: IngestValue) -> Self {
        match value {
            IngestValue::Scalar(s) => SeriesAccumValues::Scalar(vec![s]),
            IngestValue::Histogram(h) => SeriesAccumValues::Histogram(vec![h]),
        }
    }

    /// Appends `value` if its kind matches `self`; returns `false`
    /// (leaving `self` unchanged) on a kind mismatch instead of
    /// panicking, so the caller can turn it into a typed rejection.
    /// `TenantBuf::merge` checks kinds up front, so in practice this only
    /// ever returns `false` if that check is ever bypassed.
    fn try_push(&mut self, value: IngestValue) -> bool {
        match (self, value) {
            (SeriesAccumValues::Scalar(v), IngestValue::Scalar(s)) => {
                v.push(s);
                true
            }
            (SeriesAccumValues::Histogram(v), IngestValue::Histogram(h)) => {
                v.push(h);
                true
            }
            _ => false,
        }
    }

    fn into_series_values(self) -> SeriesValues {
        match self {
            SeriesAccumValues::Scalar(v) => SeriesValues::Scalar(v),
            SeriesAccumValues::Histogram(v) => SeriesValues::Histogram(v),
        }
    }
}

struct SeriesAccum {
    /// The series' shared label set (ADR-0098). Moved in from the first
    /// point of the run that opened this accumulator; the points that
    /// followed cloned the same `Arc` and dropped their clones as they
    /// merged, so by flush time the accumulator holds the last reference and
    /// [`Arc::try_unwrap`] hands `SeriesInputV3` the allocation by move.
    labels: Arc<LabelSet>,
    values: SeriesAccumValues,
}

#[derive(Default)]
struct TenantBuf {
    series: HashMap<SeriesId, SeriesAccum>,
    /// Exemplars buffered alongside the samples they illustrate (ADR-0047
    /// decision 1), in arrival order. Not keyed by series: the flush needs
    /// them ordered newest-first across the whole buffer before offering them
    /// to the flush-scoped cap, and the sample-side `HashMap` would lose the
    /// arrival order that breaks ties.
    exemplars: Vec<IngestExemplar>,
    est_bytes: usize,
    oldest_arrival_ns: Option<i64>,
    min_ingest_ts_ns: Option<i64>,
    max_ingest_ts_ns: Option<i64>,
    waiters: Vec<Ack>,
    /// Global ingest-byte-budget charges (ADR-0069) for every request whose
    /// points or exemplars this buffer currently holds. Moved into the flush
    /// task with the rest of the buffer at flush open, and dropped there when
    /// the flush completes or fails -- that drop is the budget refund. An empty
    /// (exemplar-only) buffer that never spawns a flush drops these directly in
    /// `flush_tenant`, and a fail-loud rejection in `merge` never adds one, so
    /// no path holds a charge past the bytes it covers.
    charges: Vec<Arc<IngestByteCharge>>,
    /// Arrival timestamp of the last `merge` call into this buffer, and an
    /// EWMA (alpha = 1/4) of the gaps between successive arrivals. Feeds the
    /// adaptive age trigger's observed-arrival-rate signal (ADR-0067
    /// decision 3, `adaptive_age_threshold_ns`). Scoped to the buffer's own
    /// lifetime (reset on flush) deliberately: the buffer's lifetime already
    /// spans exactly the window "since this tenant's last flush", which is
    /// the rate a fresh flush decision should react to. `avg_gap_ns == 0`
    /// (no gap observed yet, e.g. the buffer's first arrival) clamps to the
    /// corridor floor in `adaptive_age_threshold_ns` with no special case.
    last_arrival_ns: Option<i64>,
    avg_gap_ns: i64,
}

impl TenantBuf {
    fn note_arrival(&mut self, arrival_ns: i64) {
        if let Some(last) = self.last_arrival_ns {
            let gap = arrival_ns.saturating_sub(last).max(0);
            self.avg_gap_ns = if self.avg_gap_ns == 0 {
                gap
            } else {
                self.avg_gap_ns + (gap - self.avg_gap_ns) / 4
            };
        }
        self.last_arrival_ns = Some(arrival_ns);
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
    /// added (samples * 16, plus each label's `Label` struct header and its
    /// name/value bytes the first time a series is seen), per
    /// docs/ingest.md's `est_bytes` rule. The header term is the same one
    /// [`IngestPoint::est_charge_bytes`] and [`IngestExemplar::est_bytes`]
    /// apply: a `Label` is two `String` headers whatever the strings hold, so
    /// leaving it out understates a label-heavy buffer by roughly an order of
    /// magnitude and both flush triggers fire late.
    ///
    /// Fails loud on a series-id collision (ADR-0005) or a value-kind
    /// mismatch (a series is scalar or
    /// histogram for its whole life, never both): before mutating the
    /// buffer, every incoming point's `series_id` is checked against the
    /// canonical label set and value kind that id already claims, whether
    /// from a series already buffered for this tenant or from an earlier
    /// point in this same batch. A label mismatch returns
    /// [`WriteError::SeriesIdCollision`]; a value-kind mismatch returns
    /// [`WriteError::SeriesValueKindMismatch`]. Either way the buffer is
    /// left untouched, so the accepted stream for non-colliding series is
    /// unaffected. Without this check a collision would silently merge the
    /// losing series' samples under the winning label set (the id-keyed
    /// `HashMap` below cannot tell them apart), which ADR-0005 forbids.
    fn merge(&mut self, points: Vec<IngestPoint>, arrival_ns: i64) -> Result<usize, WriteError> {
        let mut batch_claims: HashMap<SeriesId, (&Arc<LabelSet>, ValueKind)> = HashMap::new();
        for point in &points {
            let point_kind = point.value.kind();
            let claimed = self
                .series
                .get(&point.series_id)
                .map(|accum| (&accum.labels, accum.values.kind()))
                .or_else(|| batch_claims.get(&point.series_id).copied());
            match claimed {
                // ADR-0098 decision 3: two points sharing the cached label set
                // settle in one pointer comparison. The structural comparison
                // is the fallback for genuinely distinct `Arc`s, so the
                // collision check keeps exactly the strength it had before:
                // ptr_eq is a fast path only, never the whole check.
                Some((labels, _))
                    if !Arc::ptr_eq(labels, &point.labels) && **labels != *point.labels =>
                {
                    return Err(WriteError::SeriesIdCollision(format!(
                        "series_id {:?} maps to two distinct label sets in one shard buffer",
                        point.series_id
                    )));
                }
                Some((_, kind)) if kind != point_kind => {
                    return Err(WriteError::SeriesValueKindMismatch(format!(
                        "series_id {:?} received both scalar and histogram points in one shard buffer",
                        point.series_id
                    )));
                }
                Some(_) => {}
                None => {
                    batch_claims.insert(point.series_id, (&point.labels, point_kind));
                }
            }
        }
        drop(batch_claims);

        self.note_arrival(arrival_ns);
        let mut bytes_added = 0usize;
        for point in points {
            match self.series.entry(point.series_id) {
                Entry::Occupied(mut occ) => {
                    occ.get_mut().values.try_push(point.value);
                }
                Entry::Vacant(vac) => {
                    let label_bytes: usize = point
                        .labels
                        .iter()
                        .map(|l| size_of::<Label>() + l.name.len() + l.value.len())
                        .sum();
                    bytes_added += label_bytes;
                    vac.insert(SeriesAccum {
                        labels: point.labels,
                        values: SeriesAccumValues::new_with(point.value),
                    });
                }
            }
            bytes_added += 16;
        }
        self.est_bytes += bytes_added;
        Ok(bytes_added)
    }

    /// Buffers `exemplars` and returns the estimated byte cost added. Called
    /// after [`TenantBuf::merge`] accepted the same request's points, so a
    /// batch rejected for a series-id collision leaves no exemplars behind
    /// either.
    ///
    /// No admission decision happens here: the cap is flush-scoped
    /// (`FlushCtx::admit_exemplars`), because a cap that outlived a flush
    /// would hold an unbounded per-series map for the shard's lifetime.
    /// Buffering an exemplar the flush will drop costs its record width
    /// once; keeping the cap costs a map entry per series forever.
    fn absorb_exemplars(&mut self, exemplars: Vec<IngestExemplar>) -> usize {
        let mut bytes_added = 0usize;
        self.exemplars.reserve(exemplars.len());
        for e in exemplars {
            bytes_added += e.est_bytes();
            self.exemplars.push(e);
        }
        self.est_bytes += bytes_added;
        bytes_added
    }
}

/// Bounded-history PUT round-trip tracker feeding the adaptive-delay ceiling
/// (ADR-0067 decision 3). A simple quantile estimate over the most recent
/// samples, not an EWMA of the mean: an EWMA-of-mean tracks the typical
/// case, but the ceiling needs the tail (p99) the ADR names. Fed from
/// spawned flush tasks (concurrent with each other and with the actor) and
/// read by the actor's own task (`ShardActor::age_threshold_ns`), so it must
/// tolerate concurrent writers without becoming a hot-path bottleneck; a
/// `Mutex` guarding a small bounded `Vec` is cheap enough for both, since it
/// is touched at most once per PUT attempt, never per message.
struct RttTracker {
    samples: Mutex<Vec<i64>>,
}

impl RttTracker {
    const CAPACITY: usize = 64;

    fn new() -> Self {
        RttTracker {
            samples: Mutex::new(Vec::with_capacity(Self::CAPACITY)),
        }
    }

    fn record(&self, sample_ns: i64) {
        let mut samples = self
            .samples
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if samples.len() == Self::CAPACITY {
            samples.remove(0);
        }
        samples.push(sample_ns);
    }

    /// The p99 sample over the current window, or `None` with no
    /// observations yet.
    fn p99_ns(&self) -> Option<i64> {
        let samples = self
            .samples
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if samples.is_empty() {
            return None;
        }
        let mut sorted = samples.clone();
        sorted.sort_unstable();
        let idx = sorted
            .len()
            .saturating_mul(99)
            .div_ceil(100)
            .saturating_sub(1)
            .min(sorted.len() - 1);
        Some(sorted[idx])
    }
}

/// Corridor ceiling for the adaptive age trigger: `budget_ns` (the caller's
/// `IngestConfig::strict_visibility_budget_ns`, ADR-0067 decision 3 /
/// ADR-0076 decision 4, docs/consistency-model.md's strict-mode ack
/// contract -- never a free-standing constant chosen for this feature
/// alone) minus two PUT round trips (data object, then commit record) at
/// their observed p99, minus one retry's base backoff as headroom, floored
/// at `floor_ns` so the corridor never inverts. `None` RTT (no observations
/// yet) collapses the ceiling to `floor_ns`: with nothing measured,
/// adapting upward would be a guess the budget cannot back, and a bursty
/// tenant must keep today's behavior from the first flush, not just after
/// warm-up.
fn visibility_ceiling_ns(
    floor_ns: i64,
    rtt_p99_ns: Option<i64>,
    retry_headroom_ns: i64,
    budget_ns: i64,
) -> i64 {
    let Some(rtt_p99_ns) = rtt_p99_ns else {
        return floor_ns;
    };
    let ceiling = budget_ns
        .saturating_sub(2 * rtt_p99_ns)
        .saturating_sub(retry_headroom_ns);
    ceiling.max(floor_ns)
}

/// The adaptive age threshold for one (shard, tenant): the tenant's observed
/// inter-arrival gap, clamped into `[floor_ns, ceiling_ns]`. A bursty tenant
/// (small gap) clamps up to `floor_ns` -- the fixed `max_flush_delay`
/// behavior, unchanged; a trickle tenant (large gap) clamps down to `ceiling_ns`
/// instead of waiting indefinitely for a full `target_bytes` batch.
fn adaptive_age_threshold_ns(avg_gap_ns: i64, floor_ns: i64, ceiling_ns: i64) -> i64 {
    avg_gap_ns.clamp(floor_ns, ceiling_ns)
}

/// Everything one flush task needs to encode, PUT twice, and ack, bundled so
/// it can be handed to a spawned task by move (ADR-0067 decision 1: "no
/// shared mutable state is introduced"). Built once per shard actor and
/// shared by every flush's task through an `Arc`; nothing here is mutated
/// after construction except the interior-mutable [`RttTracker`] and the
/// atomics inside `metrics`, both already safe for concurrent access from
/// many in-flight flush tasks at once.
struct FlushCtx {
    shard: u32,
    signal: Signal,
    writer_id: Uuid,
    epoch: u64,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<dyn Clock>,
    rng: Arc<dyn RngSource>,
    config: IngestConfig,
    metrics: Arc<IngestMetrics>,
    rtt: Arc<RttTracker>,
    /// Per-stage timing accumulator (ADR-0104 decision 1), shared by `Arc` with
    /// the router and every shard. The flush task records `encode` here; the
    /// actor reaches it through `self.ctx` to record `merge`. Present only under
    /// the `stage-timing` feature.
    #[cfg(feature = "stage-timing")]
    stage_timings: Arc<MetricStageTimings>,
}

/// One flush's identity and payload, pinned by the actor before the flush
/// task takes over (docs/catalog-and-mvcc.md "Pinned flush identity"):
/// `seq`, `ingest_hour_bucket`, and every field derived from the clock are
/// fixed here and carried verbatim into the task. Nothing in
/// [`FlushCtx::run_flush`] may re-read the clock or re-derive any of these.
struct PinnedFlush {
    tenant_hash: TenantHash,
    seq: u64,
    identity: SegmentIdentity,
    ingest_bounds: IngestBounds,
    ingest_hour_bucket: u32,
    flush_open_ns: i64,
    deadline_ns: i64,
    min_ingest_ts_ns: i64,
    max_ingest_ts_ns: i64,
    series: HashMap<SeriesId, SeriesAccum>,
    exemplars: Vec<IngestExemplar>,
    waiters: Vec<Ack>,
    /// The global ingest-byte-budget charges this flush's buffer held (ADR-0069).
    /// Carried into the flush task purely so they are dropped -- and the bytes
    /// refunded -- when the flush's terminal outcome is reached, no earlier.
    charges: Vec<Arc<IngestByteCharge>>,
}

impl FlushCtx {
    /// Runs the full pinned-identity commit sequence for one flush: the
    /// serialized segment and its blake3 hash are each computed exactly once
    /// here and reused verbatim by every retry inside
    /// `put_data_object_with_retry` and `publish_with_retry`
    /// (docs/catalog-and-mvcc.md "Pinned flush identity"). Nothing below may
    /// re-serialize, accrete new samples, or re-read the clock for identity
    /// purposes (RTT sampling inside the retry helpers is the one clock read
    /// that is not identity-affecting).
    async fn run_flush(&self, pinned: PinnedFlush) {
        let PinnedFlush {
            tenant_hash,
            seq,
            identity,
            ingest_bounds,
            ingest_hour_bucket,
            flush_open_ns,
            deadline_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            series,
            exemplars,
            waiters,
            charges,
        } = pinned;
        // Held to this flush's terminal outcome (every early `return` below is
        // still inside this scope), then dropped here: that drop is the
        // ADR-0069 budget refund for exactly the bytes this buffer held.
        let _charges = charges;

        let exemplar_inputs = self.admit_exemplars(exemplars, &series);

        // Encode: the SeriesInputV3 build plus the RSEG serialization only,
        // excluding the exemplar admission above and the object-store PUT below
        // (decision 5 measures the PUT separately).
        #[cfg(feature = "stage-timing")]
        let encode_start = std::time::Instant::now();
        // ADR-0027: every flush emits v5, scalar and histogram batches alike.
        // The raw-sample adapter frames each series into a single run, so the
        // writer choice is no longer version- or content-driven; the buffer's
        // per-series value kind (scalar or histogram) is carried through
        // `into_series_values` and the writer picks the VAL/HIST page per
        // series.
        let series_inputs: Vec<SeriesInputV3> = series
            .into_iter()
            .map(|(series_id, accum)| SeriesInputV3 {
                series_id,
                // ADR-0098: the accumulator holds the last reference to the
                // run's shared label set by flush time (the points that shared
                // it were consumed into `values`), so this unwraps the `Arc`
                // by move on the common path; a surviving clone (none today,
                // but a future cross-request memo could keep one) degrades to a
                // deep copy rather than aliasing.
                labels: Arc::try_unwrap(accum.labels).unwrap_or_else(|arc| (*arc).clone()),
                values: accum.values.into_series_values(),
            })
            .collect();
        let segment_version = SEGMENT_FORMAT_VERSION;
        let written = SegmentWriter::write_histograms_with_exemplars(
            series_inputs,
            identity,
            ingest_bounds,
            exemplar_inputs,
        );
        let written = match written {
            Ok(w) => w,
            Err(e) => {
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(WriteError::SegmentBuild(e.to_string())));
                return;
            }
        };
        #[cfg(feature = "stage-timing")]
        self.stage_timings
            .record(MetricStage::Encode, encode_start.elapsed());

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
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(WriteError::SegmentBuild(e.to_string())));
                return;
            }
        };

        if !self
            .put_data_object_with_retry(&data_key, written.bytes.clone(), deadline_ns)
            .await
        {
            self.metrics.record_abandoned_retry_exhausted();
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
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(WriteError::SegmentBuild(e.to_string())));
                return;
            }
        };

        match self.publish_with_retry(&record, deadline_ns).await {
            Some(token) => {
                // Both PUTs landed: attribute this flush's PUT cost to the
                // tenant (ADR-0076 decision 2, success-time). `tenant_hash` is
                // `Copy` and untouched by the commit-record build above.
                self.metrics.record_flush_puts(tenant_hash);
                self.ack_waiters(waiters, Ok(token));
            }
            None => {
                self.metrics.record_abandoned_retry_exhausted();
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

    /// Turn one flush's buffered exemplars into the writer's batch, dropping
    /// those the object cannot carry and counting every drop (ADR-0047
    /// decision 2).
    ///
    /// Two filters, in this order:
    ///
    /// 1. An exemplar whose parent sample is not in this flush is dropped.
    ///    The object carries no measurement for it, and the writer treats such
    ///    an exemplar as an error rather than a silent drop
    ///    (docs/segment-format.md "Writer edge rules"), so the flush site owes
    ///    it this check. It runs *before* the cap so a parentless candidate
    ///    never claims a window a writable one could have used.
    /// 2. `ExemplarCap` keeps at most one exemplar per series per window. The
    ///    cap is built here, per flush, and dropped with this call: its
    ///    per-series map is unbounded, so a shard-lived cap would be a
    ///    memory-growth vector.
    ///
    /// Candidates are offered to the cap newest-first, because
    /// [`ExemplarCap::admit`] is first-wins and never retracts: "keep the
    /// newest in a window" is the caller's ordering duty, exactly as
    /// `ravel_otlp::normalize`'s own call site does it. The sort is stable, so
    /// two candidates sharing a timestamp keep their arrival order and the
    /// choice does not depend on a sort implementation detail.
    fn admit_exemplars(
        &self,
        mut exemplars: Vec<IngestExemplar>,
        series: &HashMap<SeriesId, SeriesAccum>,
    ) -> Vec<ExemplarInput> {
        if exemplars.is_empty() {
            return Vec::new();
        }
        exemplars.sort_by_key(|e| std::cmp::Reverse(e.exemplar.ts_ns));

        let mut cap = ExemplarCap::new(self.config.exemplar_cap_window_ns);
        let mut admitted = Vec::with_capacity(exemplars.len());
        let mut dropped = 0u64;
        for candidate in exemplars {
            if !series.contains_key(&candidate.series_id) {
                dropped += 1;
                continue;
            }
            if !cap.admit(candidate.series_id, candidate.exemplar.ts_ns) {
                dropped += 1;
                continue;
            }
            admitted.push(candidate.into_exemplar_input());
        }
        self.metrics
            .record_exemplars(admitted.len() as u64, dropped);
        admitted
    }

    /// Acks exactly this flush's own waiters with exactly this flush's own
    /// result (ADR-0067 decision 1's ack-isolation requirement): `waiters`
    /// was moved out of this flush's `TenantBuf` at pin time and never
    /// merged with another flush's, so there is no other waiter list this
    /// call could reach.
    fn ack_waiters(&self, waiters: Vec<Ack>, result: Result<CommitToken, WriteError>) {
        let ok = result.is_ok();
        self.metrics.record_acks(waiters.len(), ok);
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }

    /// Races `fut` against the remaining budget to `deadline_ns` on the
    /// injected `Clock`, returning `None` if the deadline is already past or
    /// elapses while `fut` is still in flight. This is what stops a store
    /// call that is merely slow -- never errors, just never returns -- from
    /// carrying a flush past `max_flush_lifetime` on its own:
    /// without this, `deadline_ns` was only ever consulted between retryable
    /// -error attempts, never against an attempt that is still running.
    ///
    /// Deliberately built on `tokio::select!` racing `self.clock.sleep(..)`
    /// rather than `tokio::time::timeout`: the latter schedules off
    /// `tokio::time::Instant`/the real timer wheel, which is exactly the
    /// real-time read this crate's `Clock` injection exists to keep out of
    /// actor logic (crate module docs, `clock.rs`). Racing the injected
    /// clock's own `sleep` keeps this on the same clock a test can pin and
    /// advance, so a deadline can be crossed deterministically with no real
    /// wall-clock wait.
    async fn bound_to_deadline<F, T>(&self, deadline_ns: i64, fut: F) -> Option<T>
    where
        F: Future<Output = T>,
    {
        let remaining_ns = deadline_ns.saturating_sub(self.clock.now_ns());
        if remaining_ns <= 0 {
            return None;
        }
        let remaining = Duration::from_nanos(u64::try_from(remaining_ns).unwrap_or(u64::MAX));
        tokio::select! {
            result = fut => Some(result),
            () = self.clock.sleep(remaining) => None,
        }
    }

    /// Retries the data-object PUT with the caller's own budget (separate
    /// from `ravel_commit::publish`'s internal `RetryPolicy`, which only
    /// governs the commit-record PUT). Reuses the same pinned `key`/`bytes`
    /// on every attempt; `put_data_object` never re-derives either. Each
    /// attempt itself is bounded to `deadline_ns` via `bound_to_deadline`, so
    /// a timeout (like a retryable store error) never retries past the
    /// deadline and is treated exactly like the existing abandonment path.
    /// Every attempt that actually completes (whether it succeeds or fails)
    /// feeds its wall time into `self.rtt`, the observed-RTT input to the
    /// adaptive-delay ceiling (ADR-0067 decision 3); an attempt that never
    /// completes before `deadline_ns` teaches nothing about RTT and is not
    /// recorded.
    async fn put_data_object_with_retry(&self, key: &str, bytes: Bytes, deadline_ns: i64) -> bool {
        let mut attempt: u32 = 0;
        loop {
            let call = publish::put_data_object(self.store.as_ref(), key, bytes.clone());
            let started_ns = self.clock.now_ns();
            let outcome = self.bound_to_deadline(deadline_ns, call).await;
            if outcome.is_some() {
                self.rtt
                    .record(self.clock.now_ns().saturating_sub(started_ns));
            }
            match outcome {
                Some(Ok(())) => return true,
                Some(Err(PublishError::Store { source, .. })) if source.is_retryable() => {
                    // `put_retry_max_attempts` is the number of retries after
                    // the first attempt (total attempts = this + 1), matching
                    // `ravel_commit::publish::RetryPolicy::max_attempts`'s own
                    // convention. Check the budget before consuming a retry so
                    // the first attempt is not itself counted against it.
                    if attempt >= self.config.put_retry_max_attempts
                        || self.clock.now_ns() >= deadline_ns
                    {
                        return false;
                    }
                    self.metrics.record_put_retry();
                    self.backoff_sleep(attempt).await;
                    attempt += 1;
                }
                Some(Err(_)) | None => return false,
            }
        }
    }

    /// Retries the commit-record PUT with the caller's own budget. Passes
    /// `ravel_commit::publish::publish` a zero-retry policy so it attempts
    /// exactly once per call, letting this loop check `deadline_ns` between
    /// attempts (the crate's own internal retry loop has no such hook). Each
    /// attempt itself is bounded to `deadline_ns` via `bound_to_deadline`,
    /// same as `put_data_object_with_retry`, and feeds `self.rtt` the same
    /// way.
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
            let call = publish::publish(self.store.as_ref(), record, &single_attempt);
            let started_ns = self.clock.now_ns();
            let outcome = self.bound_to_deadline(deadline_ns, call).await;
            if outcome.is_some() {
                self.rtt
                    .record(self.clock.now_ns().saturating_sub(started_ns));
            }
            match outcome {
                Some(Ok(token)) => return Some(token),
                Some(Err(PublishError::SplitBrain { this, stored })) => {
                    // Identity is pinned at flush open, so this cannot fire
                    // on a benign retry (docs/catalog-and-mvcc.md "Commit
                    // sequence"); it means the pinning invariant was broken
                    // upstream. Crash loudly rather than silently corrupt.
                    panic!(
                        "ravel-ingest: fatal split-brain on pinned flush identity: this={this} stored={stored}"
                    );
                }
                Some(Err(PublishError::Store { source, .. })) if source.is_retryable() => {
                    // See `put_data_object_with_retry`: `put_retry_max_attempts`
                    // is retries after the first attempt (total = this + 1).
                    if attempt >= self.config.put_retry_max_attempts
                        || self.clock.now_ns() >= deadline_ns
                    {
                        return None;
                    }
                    self.metrics.record_put_retry();
                    self.backoff_sleep(attempt).await;
                    attempt += 1;
                }
                Some(Err(_)) | None => return None,
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
        let jittered_ms = self.rng.jitter_ms(capped_ms);
        // Route the backoff wait through the injected `Clock`, not the tokio
        // timer, so retry timing shares the one clock the rest of the flush
        // path already uses (`bound_to_deadline`) and a test can drive it
        // deterministically by advancing that clock, with no real sleep.
        self.clock.sleep(Duration::from_millis(jittered_ms)).await;
    }
}

/// Handles one reaped flush task's outcome. A panic inside
/// [`FlushCtx::run_flush`] (the `SplitBrain` panic on a broken pinning
/// invariant, or any other) must still take this shard actor down with it,
/// exactly as it did before flush execution moved into its own spawned task
/// (`tests/shard_death_observable.rs`):
/// resuming the unwind here propagates it out of `run()`'s own task, which
/// drops this actor (and `rx` with it), so the router observes the closed
/// mailbox and reports `ShardUnavailable` exactly as it did when the flush
/// ran inline. A task ending by cancellation (never triggered in today's
/// code; `flushes` is never explicitly aborted) is merely logged, since it
/// carries no panic payload to propagate.
fn handle_flush_join_result(shard: u32, result: Result<(), tokio::task::JoinError>) {
    if let Err(join_err) = result {
        if join_err.is_panic() {
            std::panic::resume_unwind(join_err.into_panic());
        }
        tracing::error!(
            shard,
            error = %join_err,
            "ravel-ingest: flush task ended abnormally (cancelled)"
        );
    }
}

/// RAII in-flight-flush accounting: incremented when a flush task is
/// spawned, decremented on `Drop` when it ends, including on panic. Moved
/// into the spawned task itself (not held by the actor) so the decrement
/// fires exactly once, whenever that task's future is finally dropped,
/// with no separate bookkeeping the actor could get out of sync with.
struct InFlightFlushGuard {
    metrics: Arc<IngestMetrics>,
    shard: u32,
}

impl Drop for InFlightFlushGuard {
    fn drop(&mut self) {
        self.metrics.record_inflight_flush_delta(self.shard, -1);
    }
}

pub(crate) struct ShardActor {
    shard: u32,
    writer_id: Uuid,
    epoch: u64,
    next_seq: u64,
    clock: Arc<dyn Clock>,
    config: IngestConfig,
    metrics: Arc<IngestMetrics>,
    /// Shared with `ctx` (both hold the same `Arc`): the actor reads it in
    /// `age_threshold_ns`, flush tasks spawned from `ctx` write to it after
    /// every completed PUT attempt.
    rtt: Arc<RttTracker>,
    /// Immutable bundle handed by `Arc::clone` to every spawned flush task
    /// (ADR-0067 decision 1).
    ctx: Arc<FlushCtx>,
    /// Bounds concurrently in-flight flush tasks (ADR-0067 decision 2).
    semaphore: Arc<Semaphore>,
    /// Tracks spawned flush tasks so `join_all_flushes` can await durability
    /// before `FlushNow`/`Shutdown`/the channel-close drain return, and so
    /// the actor loop can opportunistically reap finished ones (the `select!`
    /// branch in `run`) rather than growing this set for the shard's whole
    /// lifetime.
    flushes: JoinSet<()>,
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
        rng: Arc<dyn RngSource>,
        config: IngestConfig,
        metrics: Arc<IngestMetrics>,
        rx: mpsc::Receiver<ShardMsg>,
        #[cfg(feature = "stage-timing")] stage_timings: Arc<MetricStageTimings>,
    ) -> Self {
        let rtt = Arc::new(RttTracker::new());
        let ctx = Arc::new(FlushCtx {
            shard,
            signal,
            writer_id,
            epoch,
            store,
            clock: Arc::clone(&clock),
            rng,
            config,
            metrics: Arc::clone(&metrics),
            rtt: Arc::clone(&rtt),
            #[cfg(feature = "stage-timing")]
            stage_timings,
        });
        ShardActor {
            shard,
            writer_id,
            epoch,
            next_seq: 0,
            clock,
            config,
            metrics,
            rtt,
            ctx,
            semaphore: Arc::new(Semaphore::new(config.max_inflight_flushes as usize)),
            flushes: JoinSet::new(),
            rx,
            tenants: HashMap::new(),
        }
    }

    pub(crate) async fn run(mut self) {
        // The flush-tick cadence runs on the injected `Clock`, not the tokio
        // timer, so age-based flush timing shares the one clock the age check
        // itself reads (docs/ingest.md "Shard actor"). A test
        // that advances the injected clock past `max_flush_delay` therefore
        // drives a flush tick deterministically, with no real sleep.
        //
        // `next_tick_ns` is the next tick deadline in injected-clock
        // nanoseconds, recomputed after every tick. Anchoring the deadline
        // rather than restarting a relative sleep each loop iteration keeps
        // the cadence fixed under a burst of writes: a busy shard cannot
        // starve the age check of a quiet tenant sharing the actor, matching
        // the old `tokio::time::interval` with `MissedTickBehavior::Delay`.
        let clock = Arc::clone(&self.clock);
        let flush_tick_ns = i64::try_from(self.config.flush_tick.as_nanos()).unwrap_or(i64::MAX);
        let mut next_tick_ns = clock.now_ns().saturating_add(flush_tick_ns);
        loop {
            let until_ns = next_tick_ns.saturating_sub(clock.now_ns()).max(0);
            let until = Duration::from_nanos(u64::try_from(until_ns).unwrap_or(u64::MAX));
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(ShardMsg::Write { tenant, points, exemplars, ack, charge }) => {
                            self.handle_write(tenant, points, exemplars, ack, charge).await;
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
                        None => {
                            // Every `mpsc::Sender` was dropped: the router was
                            // dropped without `shutdown()` (services/ravel-server
                            // lib.rs reaches this when `Arc::try_unwrap` fails or
                            // `Running::shutdown` returns early). A graceful
                            // teardown is not a crash, and buffered-mode points
                            // are only permitted to be lost to a crash
                            // (docs/consistency-model.md "Buffered mode"). Flush
                            // before breaking so acknowledged points are not
                            // silently discarded; log first so the close is
                            // observable even if the flush itself is abandoned
                            // and counted (docs/ingest.md "Shard actor" step 5).
                            if !self.tenants.is_empty() {
                                let (tenant_count, buffered_points) = self.buffered_summary();
                                tracing::warn!(
                                    shard = self.shard,
                                    tenant_count,
                                    buffered_points,
                                    "shard actor channel closed without shutdown; \
                                     flushing buffered tenants before stopping"
                                );
                            }
                            self.flush_all(FlushTrigger::Manual).await;
                            break;
                        }
                    }
                }
                _ = clock.sleep(until) => {
                    self.flush_aged().await;
                    next_tick_ns = clock.now_ns().saturating_add(flush_tick_ns);
                }
                Some(result) = self.flushes.join_next(), if !self.flushes.is_empty() => {
                    handle_flush_join_result(self.shard, result);
                }
            }
        }
    }

    async fn handle_write(
        &mut self,
        tenant: TenantId,
        points: Vec<IngestPoint>,
        exemplars: Vec<IngestExemplar>,
        ack: Option<Ack>,
        charge: Option<Arc<IngestByteCharge>>,
    ) {
        if points.is_empty() && exemplars.is_empty() && ack.is_none() {
            // Nothing buffered: drop the charge now so its bytes are refunded
            // rather than held for a message that touched no buffer.
            return;
        }
        let arrival_ns = self.clock.now_ns();
        let points_len = points.len() as u64;
        let target_bytes = self.config.target_bytes;

        // Grab the timing handle before the mutable buffer borrow so recording
        // `merge` does not clash with the `&mut self.tenants` borrow held below.
        #[cfg(feature = "stage-timing")]
        let merge_timings = Arc::clone(&self.ctx.stage_timings);
        let buf = self.tenants.entry(tenant.clone()).or_default();
        // Merge before enqueuing the waiter: a series-id collision rejects
        // the whole batch fail-loud (ADR-0005) and leaves the buffer
        // untouched, so its ack must carry the error rather than ride the
        // next flush of the surviving series.
        #[cfg(feature = "stage-timing")]
        let merge_start = std::time::Instant::now();
        let mut bytes_added = match buf.merge(points, arrival_ns) {
            Ok(bytes_added) => bytes_added,
            Err(err) => {
                // Fail-loud rejection: the buffer is untouched, so this
                // request's bytes were never held (no merge sample recorded:
                // the batch was rejected before any append). Returning drops
                // `charge`, refunding them (ADR-0069) rather than pinning the
                // budget to a batch that was rejected.
                self.metrics.record_series_id_collision();
                if let Some(ack) = ack {
                    self.ctx.ack_waiters(vec![ack], Err(err));
                }
                return;
            }
        };
        // Merge times only the sample-buffer append, matching the logs merge;
        // the exemplar absorb below is a separate ADR-0047 concern, excluded.
        #[cfg(feature = "stage-timing")]
        merge_timings.record(MetricStage::Merge, merge_start.elapsed());
        bytes_added += buf.absorb_exemplars(exemplars);
        // The batch is now in the buffer: keep its charge alive with the buffer
        // until it flushes (ADR-0069). It moves into the flush task in
        // `flush_tenant` and is refunded when that flush's outcome is reached.
        if let Some(charge) = charge {
            buf.charges.push(charge);
        }
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

    /// A buffer with a strict-mode waiter or at least `min_flush_bytes`
    /// already justifies a PUT on the fast age clock; anything else is idle
    /// and waits for the slower `max_flush_delay_idle` instead (ADR-0051
    /// section 7). Strict-mode ack latency is unaffected: a strict
    /// write always leaves `waiters` non-empty for its whole flush window.
    ///
    /// The fast clock itself is either the fixed `max_flush_delay` (2 s
    /// default, and always the value used when `adaptive_flush_delay` is
    /// off) or, with adaptive delay on, a per-tenant threshold within
    /// `[max_flush_delay, ceiling]` derived from this tenant's observed
    /// arrival rate and this shard's observed PUT RTT (ADR-0067 decision 3).
    /// Returns the trigger to record alongside the threshold, so a flush the
    /// adaptive corridor actually stretched past the floor is distinguished
    /// from one that used the fixed value (`IngestMetrics`'s
    /// `flushes_by_age` vs `flushes_by_age_adaptive`).
    fn age_threshold_ns(&self, buf: &TenantBuf) -> (i64, FlushTrigger) {
        let has_priority = !buf.waiters.is_empty() || buf.est_bytes >= self.config.min_flush_bytes;
        if !has_priority {
            return (
                self.config.max_flush_delay_idle.as_nanos() as i64,
                FlushTrigger::Age,
            );
        }
        let floor_ns = self.config.max_flush_delay.as_nanos() as i64;
        if !self.config.adaptive_flush_delay {
            return (floor_ns, FlushTrigger::Age);
        }
        let retry_headroom_ns = self.config.put_retry_base_delay.as_nanos() as i64;
        let ceiling_ns = visibility_ceiling_ns(
            floor_ns,
            self.rtt.p99_ns(),
            retry_headroom_ns,
            self.config.strict_visibility_budget_ns,
        );
        let threshold_ns = adaptive_age_threshold_ns(buf.avg_gap_ns, floor_ns, ceiling_ns);
        let trigger = if threshold_ns > floor_ns {
            FlushTrigger::AgeAdaptive
        } else {
            FlushTrigger::Age
        };
        (threshold_ns, trigger)
    }

    async fn flush_aged(&mut self) {
        let now = self.clock.now_ns();
        let due: Vec<(TenantId, FlushTrigger)> = self
            .tenants
            .iter()
            .filter_map(|(tenant, buf)| {
                let oldest = buf.oldest_arrival_ns?;
                let (threshold_ns, trigger) = self.age_threshold_ns(buf);
                if now.saturating_sub(oldest) >= threshold_ns {
                    Some((tenant.clone(), trigger))
                } else {
                    None
                }
            })
            .collect();
        for (tenant, trigger) in due {
            if let Some(buf) = self.tenants.remove(&tenant) {
                self.flush_tenant(tenant, buf, trigger).await;
            }
        }
    }

    /// Returns `(tenant_count, buffered_point_count)` across every currently
    /// buffered tenant, for the channel-close log line. Point count is the
    /// sum of all buffered samples, matching `record_buffered`'s point unit.
    fn buffered_summary(&self) -> (usize, u64) {
        let points: u64 = self
            .tenants
            .values()
            .flat_map(|buf| buf.series.values())
            .map(|accum| accum.values.len() as u64)
            .sum();
        (self.tenants.len(), points)
    }

    async fn flush_all(&mut self, trigger: FlushTrigger) {
        let tenants: Vec<TenantId> = self.tenants.keys().cloned().collect();
        for tenant in tenants {
            if let Some(buf) = self.tenants.remove(&tenant) {
                self.flush_tenant(tenant, buf, trigger).await;
            }
        }
        self.join_all_flushes().await;
    }

    /// Awaits every spawned flush task, not only ones triggered by this
    /// call: any still in flight from an earlier size/age trigger too. So a
    /// caller of `flush_all` (`FlushNow`, `Shutdown`, or the channel-close
    /// drain) only observes completion once every flush this shard has ever
    /// opened is durable or abandoned. Without this, pipelining would let
    /// `Shutdown` return (and the process exit) while an earlier flush's PUT
    /// was still in flight, silently discarding an acknowledged point --
    /// docs/consistency-model.md's "Buffered mode" tolerates only crash
    /// loss, not a graceful shutdown racing its own flushes.
    async fn join_all_flushes(&mut self) {
        while let Some(result) = self.flushes.join_next().await {
            handle_flush_join_result(self.shard, result);
        }
    }

    /// Pins `buf`'s flush identity, then moves `buf`'s payload and waiters
    /// into a task spawned onto [`FlushCtx::run_flush`] (ADR-0067 decision
    /// 1). Everything up to and including the semaphore acquire runs here,
    /// on the actor; nothing after it does, so a slow encode or a slow PUT
    /// never blocks the actor from processing its next message once a
    /// permit is free (the ADR's "encode leaves the actor task" consequence,
    /// true even at `max_inflight_flushes == 1`: the actor still returns
    /// from this call, and therefore drains its channel, the moment the task
    /// is spawned rather than when that task finishes).
    ///
    /// An empty buffer (exemplars only, no samples) never reaches the
    /// semaphore or a spawned task at all: there is nothing to encode, and a
    /// flush identity pinned for nothing would burn a `seq` for no object.
    async fn flush_tenant(&mut self, tenant: TenantId, buf: TenantBuf, trigger: FlushTrigger) {
        let TenantBuf {
            series,
            exemplars,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            waiters,
            charges,
            ..
        } = buf;
        if series.is_empty() {
            // Nothing to write. Exemplars without any buffered sample cannot
            // be written at all (an exemplar points at a measurement), so they
            // are dropped, and counted so the drop stays visible. Dropping
            // `charges` here refunds the budget for those exemplars (ADR-0069):
            // no flush task will run to do it, since this buffer never spawns one.
            drop(charges);
            if !exemplars.is_empty() {
                self.metrics.record_exemplars(0, exemplars.len() as u64);
            }
            // `waiters` is empty here by construction: the router mints a
            // strict-mode ack only for a shard that received points, so a
            // shard holding nothing but exemplars has nobody to answer. If
            // that ever changes, this returns without acking and the router
            // reads the dropped oneshot as a dead shard.
            debug_assert!(waiters.is_empty());
            return;
        }
        self.metrics.record_flush(trigger);

        let tenant_hash = tenant.hash();
        let seq = self.next_seq;
        self.next_seq += 1;
        let flush_open_ns = self.clock.now_ns();
        let ingest_hour_bucket = match checked_ingest_hour_bucket(flush_open_ns) {
            Ok(bucket) => bucket,
            Err(msg) => {
                self.metrics.record_abandoned_input_rejected();
                self.ctx
                    .ack_waiters(waiters, Err(WriteError::SegmentBuild(msg)));
                return;
            }
        };
        let deadline_ns =
            flush_open_ns.saturating_add(self.config.max_flush_lifetime.as_nanos() as i64);

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

        let pinned = PinnedFlush {
            tenant_hash,
            seq,
            identity,
            ingest_bounds,
            ingest_hour_bucket,
            flush_open_ns,
            deadline_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            series,
            exemplars,
            waiters,
            charges,
        };

        // ADR-0067 decision 2: the only place a flush trigger blocks. At
        // `max_inflight_flushes` already-spawned tasks, this await parks
        // until one ends and releases its permit; because `flush_tenant` is
        // itself awaited from `handle_write`/`flush_aged`/`flush_all`, that
        // park keeps the actor from pulling its next channel message,
        // exactly the backpressure path the bounded mpsc already relies on.
        let permit = match Arc::clone(&self.semaphore).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => panic!(
                "ravel-ingest: flush semaphore closed unexpectedly on shard {}",
                self.shard
            ),
        };
        self.metrics.record_inflight_flush_delta(self.shard, 1);
        let guard = InFlightFlushGuard {
            metrics: Arc::clone(&self.metrics),
            shard: self.shard,
        };
        let ctx = Arc::clone(&self.ctx);
        self.flushes.spawn(async move {
            let _permit = permit;
            let _guard = guard;
            ctx.run_flush(pinned).await;
        });
    }
}

#[cfg(test)]
mod adaptive_delay_tests {
    use super::{IngestConfig, adaptive_age_threshold_ns, visibility_ceiling_ns};

    const FLOOR_NS: i64 = 500_000_000;
    const RETRY_HEADROOM_NS: i64 = 100_000_000;
    // Mirrors `IngestConfig::default().strict_visibility_budget_ns`
    // (ADR-0076 decision 4); not read from `IngestConfig` directly since
    // these tests exercise the pure corridor functions in isolation.
    const BUDGET_NS: i64 = 2_000_000_000;

    /// ADR-0067 decision 3's corridor, `[max_flush_delay, ceiling]`, exercised
    /// directly against the two pure functions it is built from rather than
    /// through a live shard actor: the ceiling is a budget-minus-RTT
    /// computation and the threshold is a plain clamp, so their correctness
    /// is a property of the math, not of timing a real flush. Racing a real
    /// actor's periodic age-check tick against injected-clock arrivals to
    /// observe the same corridor is possible but inherently timing-fragile
    /// (the tick that would prove a threshold was capped necessarily fires
    /// after enough clock time has already elapsed to satisfy the
    /// uncapped value too); this is the deterministic alternative.
    /// `adaptive_flush_delay_true_uses_the_corridor_above_the_floor` and
    /// `adaptive_flush_delay_false_keeps_the_fixed_floor_under_the_same_pattern`
    /// (both in `tests/adaptive_flush_delay.rs`) cover the remaining question
    /// this test cannot: that `IngestConfig::adaptive_flush_delay` actually
    /// reaches this code path in a live shard actor.
    #[test]
    fn adaptive_delay_respects_visibility_ceiling() {
        // No RTT observed yet: the ceiling collapses to the floor, so a
        // trickle tenant gets today's fixed-delay behavior until at least
        // one PUT has been timed.
        assert_eq!(
            visibility_ceiling_ns(FLOOR_NS, None, RETRY_HEADROOM_NS, BUDGET_NS),
            FLOOR_NS
        );

        // A cheap, fast backend leaves most of the 2s strict-visibility
        // budget spare, so the ceiling sits well above the floor.
        let cheap_rtt_ns = 50_000_000; // 50ms
        let ceiling_cheap =
            visibility_ceiling_ns(FLOOR_NS, Some(cheap_rtt_ns), RETRY_HEADROOM_NS, BUDGET_NS);
        assert_eq!(
            ceiling_cheap,
            BUDGET_NS - 2 * cheap_rtt_ns - RETRY_HEADROOM_NS
        );
        assert!(ceiling_cheap > FLOOR_NS);

        // As observed RTT grows, two round trips' worth of it eat further
        // into the budget, so the ceiling shrinks monotonically.
        let pricier_rtt_ns = 200_000_000; // 200ms
        let ceiling_pricier =
            visibility_ceiling_ns(FLOOR_NS, Some(pricier_rtt_ns), RETRY_HEADROOM_NS, BUDGET_NS);
        assert!(
            ceiling_pricier < ceiling_cheap,
            "a slower observed backend must narrow the corridor, not widen it"
        );

        // A backend slow enough that 2*rtt plus retry headroom would eat the
        // whole budget (or overshoot it) must never invert the corridor: the
        // ceiling is floored at floor_ns, exactly like the None case.
        let very_slow_rtt_ns = 1_100_000_000; // 1.1s: 2*rtt alone exceeds the 2s budget
        assert_eq!(
            visibility_ceiling_ns(
                FLOOR_NS,
                Some(very_slow_rtt_ns),
                RETRY_HEADROOM_NS,
                BUDGET_NS
            ),
            FLOOR_NS,
            "a corridor that inverted (ceiling < floor) would make an already-slow \
             backend flush even less often; the floor must win"
        );

        // Corridor clamp, ceiling comfortably above the floor (550ms, from
        // the cheap-RTT case above).
        let ceiling_ns = ceiling_cheap;

        // A bursty tenant's short observed gap clamps *up* to the floor:
        // unchanged from today's fixed-delay behavior.
        assert_eq!(
            adaptive_age_threshold_ns(10_000_000, FLOOR_NS, ceiling_ns),
            FLOOR_NS
        );
        // No gap observed yet (a buffer's first arrival) is the same
        // fixed-floor case, not a special-cased zero.
        assert_eq!(adaptive_age_threshold_ns(0, FLOOR_NS, ceiling_ns), FLOOR_NS);

        // A gap that already lands inside the corridor passes through
        // unchanged.
        let inside_gap_ns = (FLOOR_NS + ceiling_ns) / 2;
        assert_eq!(
            adaptive_age_threshold_ns(inside_gap_ns, FLOOR_NS, ceiling_ns),
            inside_gap_ns
        );

        // A trickle tenant's long observed gap clamps *down* to the
        // ceiling, never waiting the full raw gap: this is the cap the
        // strict-visibility budget requires.
        let trickle_gap_ns = ceiling_ns * 10;
        assert_eq!(
            adaptive_age_threshold_ns(trickle_gap_ns, FLOOR_NS, ceiling_ns),
            ceiling_ns,
            "the corridor must cap a large observed gap at the ceiling, not pass it through"
        );
    }

    /// Regression for the checkpoint-review bug: `IngestConfig::default()`
    /// once set `strict_visibility_budget_ns` exactly equal to
    /// `max_flush_delay`, so `visibility_ceiling_ns`'s subtraction always
    /// left the ceiling at the floor regardless of RTT, making
    /// `FlushTrigger::AgeAdaptive` permanently unreachable. Built on
    /// `IngestConfig::default()` directly (not a hand-assembled config the
    /// real server can never build), across a range of plausible observed
    /// RTTs, so a regression in the real default is what this test would
    /// catch.
    #[test]
    fn default_config_visibility_ceiling_clears_the_floor() {
        let cfg = IngestConfig::default();
        let floor_ns = cfg.max_flush_delay.as_nanos() as i64;
        let retry_headroom_ns = cfg.put_retry_base_delay.as_nanos() as i64;
        for rtt_p99_ns in [0i64, 10_000_000, 50_000_000] {
            let ceiling_ns = visibility_ceiling_ns(
                floor_ns,
                Some(rtt_p99_ns),
                retry_headroom_ns,
                cfg.strict_visibility_budget_ns,
            );
            assert!(
                ceiling_ns > floor_ns,
                "IngestConfig::default()'s visibility_ceiling_ns must clear max_flush_delay's \
                 floor ({floor_ns}ns) at rtt_p99_ns={rtt_p99_ns}, got {ceiling_ns}ns -- a \
                 strict_visibility_budget_ns equal to max_flush_delay collapses the corridor \
                 unconditionally"
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod buffer_accounting_tests {
    use super::*;

    fn labels_of(count: usize) -> LabelSet {
        let labels = (0..count)
            .map(|i| Label {
                name: format!("k{i}"),
                value: "v".to_string(),
            })
            .collect();
        LabelSet::new(labels).expect("distinct label names")
    }

    fn point(series: u8, labels: LabelSet) -> IngestPoint {
        let mut id = [0u8; 16];
        id[0] = series;
        IngestPoint {
            series_id: SeriesId(id),
            labels: Arc::new(labels),
            value: IngestValue::Scalar(Sample {
                ts_ns: 1_000,
                value: 1.0,
            }),
        }
    }

    /// The flush triggers read `TenantBuf::est_bytes`; the process-wide ceiling
    /// reads `IngestPoint::est_charge_bytes`. On a batch of first-sighting
    /// series the two must produce the same number, or one of them is lying
    /// about the same bytes -- which is exactly how the label-header term went
    /// missing from the budget side while the exemplar side kept it.
    #[test]
    fn merge_accounting_matches_the_budget_charge() {
        for width in [1usize, 10, 64] {
            let points: Vec<IngestPoint> = (0..3u8)
                .map(|i| {
                    let mut labels: Vec<Label> = labels_of(width).iter().cloned().collect();
                    labels.push(Label {
                        name: "series".to_string(),
                        value: i.to_string(),
                    });
                    point(i, LabelSet::new(labels).expect("distinct label names"))
                })
                .collect();
            let charged: u64 = points.iter().map(IngestPoint::est_charge_bytes).sum();

            let mut buf = TenantBuf::default();
            let added = buf
                .merge(points, 1_000)
                .expect("distinct series ids, one value kind") as u64;

            assert_eq!(
                added, charged,
                "{width} labels: buffer counted {added} bytes, budget charged {charged}"
            );
            assert_eq!(buf.est_bytes as u64, charged);
        }
    }

    /// ADR-0098 test 3. Two points with the same `series_id` and genuinely
    /// different label sets, not sharing an `Arc`, still produce
    /// `SeriesIdCollision`: the `Arc::ptr_eq` fast path does not fire on
    /// distinct pointers, so the structural comparison runs and catches the
    /// collision. If ptr_eq were the whole check this would be missed.
    #[test]
    fn distinct_arcs_with_different_labels_still_collide() {
        let mut buf = TenantBuf::default();
        let a = point(1, labels_of(3));
        let b = point(1, {
            let mut v: Vec<Label> = labels_of(3).iter().cloned().collect();
            v.push(Label {
                name: "extra".to_string(),
                value: "x".to_string(),
            });
            LabelSet::new(v).expect("distinct label names")
        });
        assert!(
            !Arc::ptr_eq(&a.labels, &b.labels),
            "test setup: the two points must hold distinct Arcs"
        );
        let err = buf
            .merge(vec![a, b], 1_000)
            .expect_err("same id, different labels is a collision");
        assert!(matches!(err, WriteError::SeriesIdCollision(_)), "{err:?}");
    }

    /// The complement pinning that `Arc::ptr_eq` is a fast path ONLY: two
    /// points with the same `series_id` and structurally EQUAL labels but
    /// distinct `Arc`s must be accepted, not rejected. The structural
    /// comparison, not pointer identity, decides.
    #[test]
    fn distinct_arcs_with_equal_labels_do_not_collide() {
        let mut buf = TenantBuf::default();
        let a = point(1, labels_of(3));
        let b = point(1, labels_of(3));
        assert!(
            !Arc::ptr_eq(&a.labels, &b.labels),
            "test setup: the two points must hold distinct Arcs"
        );
        buf.merge(vec![a, b], 1_000)
            .expect("equal labels under one id are not a collision");
        assert_eq!(buf.series.len(), 1, "both points merged into one series");
    }

    /// ADR-0098 consequence: after a run of points that shared one
    /// `Arc<LabelSet>` merges into the accumulator, the accumulator holds the
    /// LAST reference to that set (strong_count == 1). This is what lets the
    /// flush `Arc::try_unwrap` move the allocation into `SeriesInputV3` rather
    /// than deep-copy it on the common path. The points that shared the set are
    /// consumed by `merge` and their `Arc` clones dropped; nothing outside the
    /// buffer keeps one, because the request-scoped memo does not outlive the
    /// normalize call.
    #[test]
    fn accumulator_holds_the_last_reference_after_a_shared_run() {
        // Five points of one series run sharing a single Arc, as the normalizer
        // produces them. The original binding is dropped when this block ends,
        // so only the point clones (about to be consumed by merge) reference it.
        let pts: Vec<IngestPoint> = {
            let shared = Arc::new(labels_of(3));
            (0..5)
                .map(|i| IngestPoint {
                    series_id: SeriesId([1u8; 16]),
                    labels: Arc::clone(&shared),
                    value: IngestValue::Scalar(Sample {
                        ts_ns: 1_000 + i,
                        value: i as f64,
                    }),
                })
                .collect()
        };
        let mut buf = TenantBuf::default();
        buf.merge(pts, 1_000).expect("one series, one value kind");
        let accum = buf
            .series
            .get(&SeriesId([1u8; 16]))
            .expect("series present");
        assert_eq!(
            Arc::strong_count(&accum.labels),
            1,
            "the accumulator must hold the last reference so flush unwraps by move"
        );
    }

    /// A second point for a series already in the buffer costs only its sample:
    /// the label bytes (headers included) are already counted. This is the
    /// deliberate asymmetry with `est_charge_bytes`, which cannot see buffer
    /// state and so over-counts in the safe direction.
    #[test]
    fn repeat_series_adds_only_the_sample() {
        let labels = labels_of(10);
        let mut buf = TenantBuf::default();
        let first = buf
            .merge(vec![point(1, labels.clone())], 1_000)
            .expect("first sighting");
        let repeat = buf
            .merge(vec![point(1, labels)], 2_000)
            .expect("same series again");
        assert_eq!(repeat, 16, "a repeat sighting charges the sample only");
        assert_eq!(first, 16 + 10 * (size_of::<Label>() + 3));
    }
}
