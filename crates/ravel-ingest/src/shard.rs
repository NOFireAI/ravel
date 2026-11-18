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
//! INSIDE the spawned task (issue #1292): the actor spawns and returns, so a
//! stalled flush -- a tenant whose S3 key prefix is being throttled -- parks
//! only its own task on the permit, never the actor, so every co-resident
//! tenant's age tick and channel drain keep running. Backpressure at the bound
//! propagates through the ADR-0069 byte budget (charges held until a flush
//! completes), not through parking the actor; ADR-1642 supersedes ADR-0067
//! decision 2 with that. The permit wait is measured as
//! its own span (`flush_permit_wait_ns`, issue #865) inside the task. The
//! adaptive age trigger (ADR-0067
//! decision 3) is `age_threshold_ns`/`adaptive_age_threshold_ns` below.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use bytes::Bytes;
use ravel_commit::keys;
use ravel_commit::publish::{self, PublishError, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::rng::RngSource;
use ravel_object_store::ObjectStoreBackend;
use ravel_proto::commit::v1::CommitRecord;
use ravel_segment::{
    ExemplarInput, HistogramSample, IngestBounds, SegmentIdentity, SegmentVersion, SegmentWriter,
    SeriesInputV3, SeriesValues,
};
use ravel_types::{
    CommitToken, ExemplarCap, Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId,
    shard_for,
};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Duration;
use uuid::Uuid;

use crate::budget::{BufferBudgetCeiling, IngestByteCharge};
use crate::clock::Clock;
use crate::compat::{
    CompatPublishRefusal, PublishGrant, PutAbort, stale_view_wait, warn_publish_refused,
};
use crate::config::{
    DrainIntent, FlushClockError, IngestConfig, LagCheck, MAX_FLUSH_ALL_PASSES,
    MAX_FLUSH_CLOCK_HOLD_NS, StoreClockLag, checked_ingest_hour_bucket, duration_nanos_saturating,
    grant_deadline_ns, idle_age_threshold, memory_backstop_crossed, object_backstop_crossed,
    size_trigger_fires, store_clock_lag,
};
use crate::deferral::{DEFERRAL_CAP_ABANDONED, DeferralCapFlag, deferral_cap_reached};
use crate::error::WriteError;
use crate::flush_share::TenantFlushShares;
use crate::format_floor::{
    BelowFormatFloor, FormatFamily, warn_flush_refused, warn_flush_written_below,
};
use crate::generation::{
    FloorCheck, FlushScope, HandBackArrival, HandBackReason, HandedBack, HoldState,
    STALE_VIEW_ABANDONED, ScanCheck, StaleViewHold, mismatch_retry_spent, reread_and_check,
    send_hand_back,
};
use crate::metrics::{FlushTrigger, IngestMetrics};
#[cfg(feature = "stage-timing")]
use crate::stage_timing::{MetricStage, MetricStageTimings};
use crate::value::{IngestExemplar, IngestPoint, IngestValue, ValueKind};
use crate::write_gate::{GATE_DEADLINE_MESSAGE, WriteGateSlot};
use ravel_cpu_gate::WriteSite;

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
        /// Set for a [`crate::WriteMode::StrictArrival`] write that carries an
        /// ack: the actor opens the tenant's flush on arrival rather than on
        /// the age tick (ADR-2793 decision 2).
        arrival: bool,
    },
    /// Rows a retiring shard handed back at flush open instead of writing them
    /// outside the scan set of the hour they would have pinned (ADR-1642
    /// scan-set amendment), routed to this shard under the tenant's current
    /// generation. Already admitted and charged: the charges travel with them,
    /// and no strict waiter rides along, since the source answered it.
    HandBack {
        tenant: TenantId,
        points: Vec<IngestPoint>,
        exemplars: Vec<IngestExemplar>,
        charges: Vec<Arc<IngestByteCharge>>,
        arrival: HandBackArrival,
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
    /// Estimated bytes the object this buffer's flush writes will hold, the
    /// size trigger's own figure (issue #1305). `est_bytes` above stays the
    /// buffered-memory charge the ADR-0069 ceiling reads; the two are not
    /// interchangeable, and on a label-heavy series they differ by more than an
    /// order of magnitude. See [`IngestPoint::est_object_sample_bytes`] for the
    /// model and its bound direction.
    flush_est_bytes: usize,
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
    /// When this buffer's flush trigger was first refused at the queued-flush
    /// cap, carried across every later refusal so the deferral is measured
    /// from its start (issue #1916). `None` for a buffer never refused.
    deferred_since_ns: Option<i64>,
    /// Set once this buffer's flush was kept closed on a generation view the
    /// router cannot trust, so the episode counts once on
    /// `stale_provisioning_flushes` however many triggers retry it.
    stale_view_counted: bool,
    /// This buffer's hold on a generation view the router cannot trust (issue
    /// #4363): when it started, and, once `max_flush_lifetime` has passed,
    /// the background read requested to confirm the view, whose failure drops
    /// and counts the buffer. Cleared when a flush open gets a scan-set or
    /// hand-back verdict, or finds the tenant's view refreshed by a read
    /// issued since the hold started.
    stale_view_hold: StaleViewHold,
    /// Set once a hand-back from this buffer found a target shard not live, so
    /// the episode counts and logs once however many triggers retry it.
    hand_back_blocked: bool,
    /// When a generation-mismatch hand-back from this buffer first left rows
    /// undelivered, so later triggers retry it only until the deferral cap
    /// ([`mismatch_retry_spent`]).
    mismatch_held_since_ns: Option<i64>,
    /// Set when a strict-arrival write merged while this tenant already had a
    /// flush in flight, so the reap that brings it to zero opens this buffer's
    /// flush (ADR-2793 decision 2). Cleared whenever the buffer leaves the map
    /// for a flush ([`ShardActor::take_for_flush`]), so a buffer the flush puts
    /// back never carries it.
    arrival_pending: bool,
}

impl TenantBuf {
    /// Merges rows that are already admitted and charged: rows handed back
    /// to this shard, or rows a failed hand-back send returned. Their arrival
    /// bookkeeping travels with them. Leaves the buffer untouched on a
    /// collision, as [`TenantBuf::merge`] does.
    fn absorb(
        &mut self,
        points: Vec<IngestPoint>,
        exemplars: Vec<IngestExemplar>,
        charges: Vec<Arc<IngestByteCharge>>,
        arrival: HandBackArrival,
    ) -> Result<(), WriteError> {
        self.merge_unstamped(points)?;
        self.absorb_exemplars(exemplars);
        self.charges.extend(charges);
        arrival.fold_into(
            &mut self.oldest_arrival_ns,
            &mut self.min_ingest_ts_ns,
            &mut self.max_ingest_ts_ns,
        );
        Ok(())
    }

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
    /// magnitude and the memory ceiling is charged too little.
    ///
    /// The same pass also accumulates `flush_est_bytes`, the object-bytes
    /// estimate the size trigger reads (issue #1305). The returned figure stays
    /// the memory one: it is what the caller records on the buffered-bytes
    /// gauge and what the ADR-0069 charge covers.
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
        let bytes_added = self.merge_unstamped(points)?;
        self.note_arrival(arrival_ns);
        Ok(bytes_added)
    }

    /// [`TenantBuf::merge`] without the arrival stamp, for handed-back rows
    /// whose arrival bookkeeping travels with them ([`HandBackArrival`]).
    fn merge_unstamped(&mut self, points: Vec<IngestPoint>) -> Result<usize, WriteError> {
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

        let mut bytes_added = 0usize;
        let mut object_bytes_added = 0usize;
        for point in points {
            object_bytes_added += point.est_object_sample_bytes();
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
                    object_bytes_added += point.est_object_series_bytes();
                    vac.insert(SeriesAccum {
                        labels: point.labels,
                        values: SeriesAccumValues::new_with(point.value),
                    });
                }
            }
            bytes_added += 16;
        }
        self.est_bytes += bytes_added;
        self.flush_est_bytes += object_bytes_added;
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
        let mut object_bytes_added = 0usize;
        self.exemplars.reserve(exemplars.len());
        for e in exemplars {
            bytes_added += e.est_bytes();
            object_bytes_added += e.est_object_bytes();
            self.exemplars.push(e);
        }
        self.est_bytes += bytes_added;
        self.flush_est_bytes += object_bytes_added;
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
    /// The ADR-1702 write gate the encode runs on, shared with the router.
    write_gate: WriteGateSlot,
    /// NoPublishAfterAbandon evidence: commit keys of flushes abandoned at the
    /// commit PUT whose record a HEAD found absent at the abandon, oldest
    /// first. Only the instrumented build fills it; later flushes on this
    /// shard re-read the keys once their outcome is decided.
    abandoned_commits: Mutex<VecDeque<AbandonedCommit>>,
}

/// A flush abandoned at its commit PUT with no commit record in the store at
/// the abandon. The default build never builds one, and only assertions read
/// the key and tenant.
#[allow(dead_code)]
struct AbandonedCommit {
    tenant_hash: TenantHash,
    commit_key: String,
    ingest_hour_bucket: u32,
    /// [`FoldSealMargin::sealed_at_ns`] of the record's hour, under
    /// [`IngestConfig::fold_seal`]. A fold re-reads a sealed hour's buckets
    /// only for a compaction record, tombstone or rewrite record (ravel-catalog
    /// fold.rs `reconcile_one_bucket`, the seal lemma), so a commit record the
    /// store stamps at or after this instant may never be folded: lost.
    ///
    /// [`FoldSealMargin::sealed_at_ns`]: crate::config::FoldSealMargin::sealed_at_ns
    sealed_at_ns: i64,
}

/// The most abandoned commit keys a shard keeps for re-reading; the oldest is
/// dropped past it.
const MAX_ABANDONED_COMMITS: usize = 64;

/// The most abandoned commit keys one flush re-reads, oldest first; the rest
/// wait for later flushes.
const MAX_ABANDONED_RECHECKS_PER_FLUSH: usize = 4;

/// What a flush's encode job hands back to the flush task: the segment, and
/// the byte charges the job held while it waited and ran.
struct EncodedFlush {
    written: Result<ravel_segment::WrittenSegment, ravel_segment::WriteError>,
    charges: Vec<Arc<IngestByteCharge>>,
    #[cfg(feature = "stage-timing")]
    encode_elapsed: std::time::Duration,
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
    /// The buffer's `est_bytes`, which sizes the encode against the write
    /// gate's inline floor.
    input_bytes: u64,
    waiters: Vec<Ack>,
    /// The global ingest-byte-budget charges this flush's buffer held (ADR-0069).
    /// Carried into the flush task purely so they are dropped -- and the bytes
    /// refunded -- when the flush's terminal outcome is reached, no earlier.
    charges: Vec<Arc<IngestByteCharge>>,
}

impl FlushCtx {
    /// Runs one flush to its outcome ([`Self::commit_flush`]), then re-reads
    /// abandoned commit keys, so a slow re-read cannot move this flush past
    /// its deadline.
    ///
    /// The per-shard off-actor skew (issue #865) brackets only
    /// [`Self::commit_flush`], the flush proper: the abandoned-key re-reads run
    /// after that measurement, so a slow re-read does not inflate the figure's
    /// meaning (the time this flush spent off the actor), and the re-reads are
    /// themselves bounded to one `EVIDENCE_HEAD_TIMEOUT`.
    async fn run_flush(&self, pinned: PinnedFlush, grant: PublishGrant) {
        let started_ns = self.clock.now_ns();
        self.commit_flush(pinned, grant).await;
        let off_actor_ns = self.clock.now_ns().saturating_sub(started_ns).max(0) as u64;
        self.metrics
            .record_shard_off_actor_ns(self.shard, off_actor_ns);
        self.recheck_abandoned_commits().await;
    }

    /// Runs the full pinned-identity commit sequence for one flush: the
    /// serialized segment and its blake3 hash are each computed exactly once
    /// here and reused verbatim by every retry inside
    /// `put_data_object_with_retry` and `publish_with_retry`
    /// (docs/catalog-and-mvcc.md "Pinned flush identity"). Nothing below may
    /// re-serialize, accrete new samples, or re-read the clock for identity
    /// purposes (RTT sampling inside the retry helpers is the one clock read
    /// that is not identity-affecting).
    async fn commit_flush(&self, pinned: PinnedFlush, grant: PublishGrant) {
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
            input_bytes,
            waiters,
            charges,
        } = pinned;

        let waiters = if ravel_invariant::inv_mutant!("commit/ack-before-commit") {
            let token = CommitToken {
                shard: self.shard,
                writer_id: self.writer_id,
                epoch: self.epoch,
                seq,
                ingest_hour_bucket,
            };
            self.ack_waiters(waiters, Ok(token));
            Vec::new()
        } else {
            waiters
        };

        let exemplar_inputs = self.admit_exemplars(exemplars, &series);

        // The version was chosen at permit grant (ADR-4233 decision 2); with
        // no activation policy it is `SegmentVersion::CURRENT`. The buffer's
        // per-series value kind (scalar or histogram) is carried through
        // `into_series_values` and the writer picks the VAL/HIST page per
        // series.
        let Some(segment_version) = SegmentVersion::from_number(grant.version()) else {
            self.refuse_compat(tenant_hash, waiters, grant.unencodable());
            drop(charges);
            return;
        };
        // The encode runs on the write gate when one is attached (ADR-1702).
        // The job takes the byte charges with the buffer and returns them with
        // the segment, so they stay held through the permit wait and the encode
        // whether or not this task is still waiting. The wait is bounded by the
        // flush's deadline, as its store calls are.
        let encode = self
            .write_gate
            .encode(WriteSite::MetricsFlush, input_bytes, move || {
                // Encode: the SeriesInputV3 build plus the RSEG serialization
                // only, excluding the exemplar admission above, the permit wait
                // and the object-store PUT below (decision 5 measures the PUT
                // separately).
                #[cfg(feature = "stage-timing")]
                let encode_start = std::time::Instant::now();
                let series_inputs: Vec<SeriesInputV3> = series
                    .into_iter()
                    .map(|(series_id, accum)| SeriesInputV3 {
                        series_id,
                        // ADR-0098: the accumulator holds the last reference to
                        // the run's shared label set by flush time (the points
                        // that shared it were consumed into `values`), so this
                        // unwraps the `Arc` by move on the common path; a
                        // surviving clone (none today, but a future
                        // cross-request memo could keep one) degrades to a deep
                        // copy rather than aliasing.
                        labels: Arc::try_unwrap(accum.labels).unwrap_or_else(|arc| (*arc).clone()),
                        values: accum.values.into_series_values(),
                    })
                    .collect();
                let written = SegmentWriter::write_histograms_with_exemplars_at(
                    segment_version,
                    series_inputs,
                    identity,
                    ingest_bounds,
                    exemplar_inputs,
                );
                EncodedFlush {
                    written,
                    charges,
                    #[cfg(feature = "stage-timing")]
                    encode_elapsed: encode_start.elapsed(),
                }
            });
        let encoded = match self.bound_to_deadline(deadline_ns, encode).await {
            Some(Ok(encoded)) => encoded,
            Some(Err(err)) => {
                // Gate fault: see `GATE_DEADLINE_MESSAGE`.
                self.metrics.record_flush_gate_fault();
                tracing::error!(
                    shard = self.shard,
                    error = %err,
                    "ravel-ingest: write gate returned no flush encode result"
                );
                self.ack_waiters(waiters, Err(WriteError::ShardUnavailable));
                return;
            }
            None => {
                // Deadline: see `GATE_DEADLINE_MESSAGE`.
                self.metrics.record_abandoned_queue_deadline();
                ravel_invariant::inv_reachable!(
                    "commit: flush abandoned",
                    std::collections::BTreeMap::from([
                        ("at", "encode".to_owned()),
                        ("tenant", tenant_hash.to_hex()),
                    ])
                );
                self.ack_waiters(
                    waiters,
                    Err(WriteError::Abandoned(GATE_DEADLINE_MESSAGE.into())),
                );
                return;
            }
        };
        // Held to this flush's terminal outcome (every early `return` below is
        // still inside this scope), then dropped here: that drop is the
        // ADR-0069 budget refund for exactly the bytes this buffer held.
        let _charges = encoded.charges;
        let written = match encoded.written {
            Ok(w) => w,
            Err(e) => {
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(WriteError::SegmentBuild(e.to_string())));
                return;
            }
        };
        #[cfg(feature = "stage-timing")]
        self.stage_timings
            .record(MetricStage::Encode, encoded.encode_elapsed);

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

        // ADR-4233 decision 4: the bucket is fenced before the first PUT of
        // an object above the family's baseline.
        if let Err(refused) = grant
            .ensure_fenced(
                self.store.as_ref(),
                self.signal,
                self.clock.as_ref(),
                self.rng.as_ref(),
                deadline_ns,
            )
            .await
        {
            self.refuse_compat(tenant_hash, waiters, refused);
            return;
        }

        match self
            .put_data_object_with_retry(&data_key, written.bytes.clone(), deadline_ns, &grant)
            .await
        {
            Ok(()) => {}
            Err(PutAbort::Refused(refused)) => {
                self.refuse_compat(tenant_hash, waiters, refused);
                return;
            }
            Err(PutAbort::Exhausted) => {
                self.metrics.record_abandoned_retry_exhausted();
                ravel_invariant::inv_reachable!(
                    "commit: flush abandoned",
                    std::collections::BTreeMap::from([
                        ("at", "data put".to_owned()),
                        ("tenant", tenant_hash.to_hex()),
                    ])
                );
                self.ack_waiters(
                    waiters,
                    Err(WriteError::Abandoned(
                        "data object put exhausted retry budget or exceeded max_flush_lifetime"
                            .into(),
                    )),
                );
                return;
            }
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
            segment_format_version: u32::from(segment_version.number()),
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

        match self
            .publish_with_retry(tenant_hash, &record, deadline_ns, &grant)
            .await
        {
            Ok(token) => {
                // Both PUTs landed: attribute this flush's PUT cost to the
                // tenant (ADR-0076 decision 2, success-time). `tenant_hash` is
                // `Copy` and untouched by the commit-record build above.
                self.metrics.record_flush_puts(tenant_hash);
                ravel_invariant::inv_reachable!(
                    "commit: strict ack sent",
                    std::collections::BTreeMap::from([("signal", "metrics")])
                );
                self.ack_waiters(waiters, Ok(token));
            }
            Err(PutAbort::Refused(refused)) => {
                self.refuse_compat(tenant_hash, waiters, refused);
            }
            Err(PutAbort::Exhausted) => {
                self.metrics.record_abandoned_retry_exhausted();
                ravel_invariant::inv_reachable!(
                    "commit: flush abandoned",
                    std::collections::BTreeMap::from([
                        ("at", "commit put".to_owned()),
                        ("tenant", tenant_hash.to_hex()),
                    ])
                );
                self.ack_waiters(
                    waiters,
                    Err(WriteError::Abandoned(
                        "commit publish exhausted retry budget or exceeded max_flush_lifetime"
                            .into(),
                    )),
                );
                // The HEAD runs in the condition, so the default build never
                // issues it and records nothing.
                ravel_invariant::inv_sometimes!(
                    self.note_abandoned_commit(tenant_hash, &record).await,
                    "commit: an abandoned flush's commit record is absent at the abandon",
                    std::collections::BTreeMap::from([
                        ("tenant", tenant_hash.to_hex()),
                        (
                            "commit_key",
                            keys::commit_key_for_record(&record).unwrap_or_default(),
                        ),
                    ])
                );
            }
        }
    }

    /// Remembers an abandoned flush's commit key for
    /// [`Self::recheck_abandoned_commits`] when a HEAD finds no record there,
    /// and returns whether it did. A record found present landed before the
    /// abandon (a lost PUT response), which is the documented ambiguity and
    /// not a violation; a HEAD error is no evidence either way. Only an
    /// invariant condition calls this.
    #[allow(dead_code)]
    async fn note_abandoned_commit(&self, tenant_hash: TenantHash, record: &CommitRecord) -> bool {
        let Ok(commit_key) = keys::commit_key_for_record(record) else {
            return false;
        };
        match self.store.head(&commit_key).await {
            Err(ravel_object_store::StoreError::NotFound) => {}
            Ok(_) | Err(_) => return false,
        }
        let ingest_hour_bucket = record.ingest_hour_bucket;
        let sealed_at_ns = self.config.fold_seal.sealed_at_ns(ingest_hour_bucket);
        let mut abandoned = self
            .abandoned_commits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if abandoned.len() >= MAX_ABANDONED_COMMITS {
            abandoned.pop_front();
        }
        abandoned.push_back(AbandonedCommit {
            tenant_hash,
            commit_key,
            ingest_hour_bucket,
            sealed_at_ns,
        });
        true
    }

    /// NoPublishAfterAbandon (formal/tla/commit), judged from store evidence:
    /// an abandoned flush's commit record may still land (a PUT the flush
    /// stopped waiting for), but never with a store `last_modified` at or
    /// past [`AbandonedCommit::sealed_at_ns`]. Re-reads at most
    /// [`MAX_ABANDONED_RECHECKS_PER_FLUSH`] keys, oldest first. A record found
    /// is evaluated once and forgotten; an absent one is evaluated and kept
    /// until the clock passes its bound by another `max_flush_lifetime`; a
    /// HEAD error, or a HEAD still running after
    /// [`publish::EVIDENCE_HEAD_TIMEOUT`], evaluates nothing and keeps the
    /// key. Only the instrumented build remembers abandoned keys, so the
    /// default build finds none here.
    ///
    /// This is the metrics shard only: the logs and spans shards do not record
    /// abandoned keys, because their catalog-maintenance workload writes across
    /// forward clock jumps of +9000 s and more, where a flush still in flight
    /// at a write timeout could land past the seal on correct code (see the
    /// note at their commit-PUT abandon sites).
    async fn recheck_abandoned_commits(&self) {
        let due: Vec<AbandonedCommit> = {
            let mut abandoned = self
                .abandoned_commits
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if abandoned.is_empty() {
                return;
            }
            let due = abandoned.len().min(MAX_ABANDONED_RECHECKS_PER_FLUSH);
            abandoned.drain(..due).collect()
        };
        let lifetime_ns = duration_nanos_saturating(self.config.max_flush_lifetime);
        let mut kept = Vec::with_capacity(due.len());
        // One deadline bounds the whole batch, not each key: the HEADs share a
        // single `EVIDENCE_HEAD_TIMEOUT` sleep, so a stalled store holds the
        // flush permit for one timeout rather than one per due key. A key not
        // judged by the deadline evaluates nothing and is kept.
        let deadline = self.clock.sleep(publish::EVIDENCE_HEAD_TIMEOUT);
        tokio::pin!(deadline);
        let mut due = due.into_iter();
        loop {
            let Some(abandoned) = due.next() else { break };
            let head = tokio::select! {
                head = self.store.head(&abandoned.commit_key) => head,
                () = &mut deadline => {
                    kept.push(abandoned);
                    kept.extend(due);
                    break;
                }
            };
            let landed_ms = match head {
                Ok(meta) => Some(meta.last_modified_unix_ms),
                Err(ravel_object_store::StoreError::NotFound) => None,
                Err(_) => {
                    kept.push(abandoned);
                    continue;
                }
            };
            let checked_ns = self.clock.now_ns();
            ravel_invariant::inv_always_or_unreachable!(
                landed_ms.is_none_or(|ms| ms.saturating_mul(1_000_000) < abandoned.sealed_at_ns),
                "commit: an abandoned flush's commit record never lands past the fold's seal margin",
                std::collections::BTreeMap::from([
                    ("tenant", abandoned.tenant_hash.to_hex()),
                    ("commit_key", abandoned.commit_key.clone()),
                    (
                        "ingest_hour_bucket",
                        abandoned.ingest_hour_bucket.to_string()
                    ),
                    ("sealed_at_ns", abandoned.sealed_at_ns.to_string()),
                    (
                        "last_modified_unix_ms",
                        landed_ms.map_or_else(|| "absent".to_owned(), |ms| ms.to_string()),
                    ),
                    ("checked_ns", checked_ns.to_string()),
                ])
            );
            if landed_ms.is_none()
                && checked_ns <= abandoned.sealed_at_ns.saturating_add(lifetime_ns)
            {
                kept.push(abandoned);
            }
        }
        if !kept.is_empty() {
            let mut abandoned = self
                .abandoned_commits
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            abandoned.extend(kept);
            let excess = abandoned.len().saturating_sub(MAX_ABANDONED_COMMITS);
            abandoned.drain(..excess);
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

    /// Abandons a flush whose flush-open deadline already elapsed while it was
    /// queued for a permit, before it took one or attempted any store call
    /// (issue #1739). Its waiters are acked with a retryable `Abandoned` error
    /// and its byte charges are dropped, refunding the ADR-0069 budget.
    fn abandon_in_queue(&self, pinned: PinnedFlush) {
        let PinnedFlush {
            waiters, charges, ..
        } = pinned;
        self.metrics.record_abandoned_queue_deadline();
        ravel_invariant::inv_reachable!(
            "commit: flush abandoned",
            std::collections::BTreeMap::from([
                ("at", "permit queue".to_owned()),
                ("tenant", pinned.tenant_hash.to_hex()),
            ])
        );
        self.ack_waiters(
            waiters,
            Err(WriteError::Abandoned(
                "flush lifetime elapsed while queued for a shard flush permit".into(),
            )),
        );
        drop(charges);
    }

    /// Abandons a flush granted its permit at or past its pinned ingest hour's
    /// end plus `max_flush_lifetime`, before any store call (ADR-2708 D3). Its
    /// waiters are acked with a retryable `Abandoned` error and its byte
    /// charges are dropped.
    fn abandon_hour_bound(&self, pinned: PinnedFlush) {
        let PinnedFlush {
            waiters, charges, ..
        } = pinned;
        self.metrics.record_abandoned_hour_bound();
        ravel_invariant::inv_reachable!(
            "commit: flush abandoned",
            std::collections::BTreeMap::from([
                ("at", "hour bound".to_owned()),
                ("tenant", pinned.tenant_hash.to_hex()),
            ])
        );
        self.ack_waiters(
            waiters,
            Err(WriteError::Abandoned(
                "flush permit granted past its ingest hour's flush bound".into(),
            )),
        );
        drop(charges);
    }

    /// Fails a flush the activation policy refused (ADR-4233), with nothing
    /// of it stored: counted under its reason, and its waiters acked with the
    /// refusal. The caller drops the flush's byte charges.
    fn refuse_compat(
        &self,
        tenant_hash: TenantHash,
        waiters: Vec<Ack>,
        refused: CompatPublishRefusal,
    ) {
        self.metrics.record_compat_refused(refused.reason());
        warn_publish_refused(self.signal, Some(self.shard), tenant_hash, &refused);
        self.ack_waiters(waiters, Err(WriteError::CompatRefused(refused)));
    }

    /// Abandons a flush the activation policy refused at permit grant, before
    /// any store call, dropping its byte charges.
    fn abandon_compat(&self, pinned: PinnedFlush, refused: CompatPublishRefusal) {
        let PinnedFlush {
            tenant_hash,
            exemplars,
            waiters,
            charges,
            ..
        } = pinned;
        if !exemplars.is_empty() {
            self.metrics.record_exemplars(0, exemplars.len() as u64);
        }
        self.refuse_compat(tenant_hash, waiters, refused);
        drop(charges);
    }

    /// Abandons a flush whose version chosen at permit grant sits below the
    /// tenant's format floor (ADR-1746 decision 4), before any store call.
    fn abandon_below_floor(&self, pinned: PinnedFlush, refused: BelowFormatFloor) {
        let PinnedFlush {
            tenant_hash,
            exemplars,
            waiters,
            charges,
            ..
        } = pinned;
        warn_flush_refused(self.signal, self.shard, tenant_hash, &refused);
        if !exemplars.is_empty() {
            self.metrics.record_exemplars(0, exemplars.len() as u64);
        }
        self.ack_waiters(waiters, Err(WriteError::BelowFormatFloor(refused)));
        drop(charges);
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
    /// The activation policy is asked before every attempt (ADR-4233
    /// decision 3).
    async fn put_data_object_with_retry(
        &self,
        key: &str,
        bytes: Bytes,
        deadline_ns: i64,
        grant: &PublishGrant,
    ) -> Result<(), PutAbort> {
        let mut attempt: u32 = 0;
        loop {
            grant
                .attempt_when_fresh(self.clock.as_ref(), deadline_ns)
                .await
                .map_err(PutAbort::Refused)?;
            let call = publish::put_data_object(self.store.as_ref(), key, bytes.clone());
            let started_ns = self.clock.now_ns();
            let outcome = self.bound_to_deadline(deadline_ns, call).await;
            if outcome.is_some() {
                self.rtt
                    .record(self.clock.now_ns().saturating_sub(started_ns));
            }
            match outcome {
                Some(Ok(())) => return Ok(()),
                Some(Err(PublishError::Store { source, .. })) if source.is_retryable() => {
                    // `put_retry_max_attempts` is the number of retries after
                    // the first attempt (total attempts = this + 1), matching
                    // `ravel_commit::publish::RetryPolicy::max_attempts`'s own
                    // convention. Check the budget before consuming a retry so
                    // the first attempt is not itself counted against it.
                    if attempt >= self.config.put_retry_max_attempts
                        || self.clock.now_ns() >= deadline_ns
                    {
                        return Err(PutAbort::Exhausted);
                    }
                    self.metrics.record_put_retry();
                    ravel_invariant::inv_reachable!(
                        "commit: put retried",
                        std::collections::BTreeMap::from([("object", "data")])
                    );
                    self.backoff_sleep(attempt).await;
                    attempt += 1;
                }
                Some(Err(_)) | None => return Err(PutAbort::Exhausted),
            }
        }
    }

    /// Retries the commit-record PUT with the caller's own budget. Passes
    /// `ravel_commit::publish::publish` a zero-retry policy so it attempts
    /// exactly once per call, letting this loop check `deadline_ns` between
    /// attempts (the crate's own internal retry loop has no such hook). Each
    /// attempt itself is bounded to `deadline_ns` via `bound_to_deadline`,
    /// same as `put_data_object_with_retry`, and feeds `self.rtt` the same
    /// way, and asks the activation policy before every attempt the same way.
    async fn publish_with_retry(
        &self,
        tenant_hash: TenantHash,
        record: &CommitRecord,
        deadline_ns: i64,
        grant: &PublishGrant,
    ) -> Result<CommitToken, PutAbort> {
        let single_attempt = RetryPolicy {
            max_attempts: 0,
            base_delay: self.config.put_retry_base_delay,
            max_delay: self.config.put_retry_max_delay,
        };
        let mut attempt: u32 = 0;
        loop {
            if let Err(refused) = grant
                .attempt_when_fresh(self.clock.as_ref(), deadline_ns)
                .await
            {
                // On a retry iteration (attempt > 0) a commit-record PUT was
                // already issued this flush; the activation policy now refuses
                // the retry, but that PUT may still land, so record the key for
                // the abandon recheck exactly as the Exhausted arm below does.
                // The first iteration refuses before any PUT, so nothing can
                // have landed and nothing is recorded.
                if attempt > 0 {
                    ravel_invariant::inv_sometimes!(
                        self.note_abandoned_commit(tenant_hash, record).await,
                        "commit: an abandoned flush's commit record is absent at the abandon",
                        std::collections::BTreeMap::from([
                            ("tenant", tenant_hash.to_hex()),
                            (
                                "commit_key",
                                keys::commit_key_for_record(record).unwrap_or_default(),
                            ),
                        ])
                    );
                }
                return Err(PutAbort::Refused(refused));
            }
            let call =
                publish::publish_deferring_evidence(self.store.as_ref(), record, &single_attempt);
            let started_ns = self.clock.now_ns();
            let outcome = self.bound_to_deadline(deadline_ns, call).await;
            if outcome.is_some() {
                self.rtt
                    .record(self.clock.now_ns().saturating_sub(started_ns));
            }
            match outcome {
                Some(Ok(token)) => {
                    // After the race and the RTT sample, so the instrumented
                    // build's evidence read cannot abandon a landed commit,
                    // and bounded so it cannot hold the ack either.
                    publish::assert_commit_has_data_unless(self.store.as_ref(), record, || {
                        self.clock.sleep(publish::EVIDENCE_HEAD_TIMEOUT)
                    })
                    .await;
                    return Ok(token);
                }
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
                        return Err(PutAbort::Exhausted);
                    }
                    self.metrics.record_put_retry();
                    ravel_invariant::inv_reachable!(
                        "commit: put retried",
                        std::collections::BTreeMap::from([("object", "commit record")])
                    );
                    self.backoff_sleep(attempt).await;
                    attempt += 1;
                }
                Some(Err(_)) | None => return Err(PutAbort::Exhausted),
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

/// RAII in-flight-flush accounting. [`InFlightFlushGuard::new`] is the only way
/// to construct one and it performs the `+1`; `Drop` performs the `-1`. Pairing
/// them in one value is what keeps the gauge unbiased: an increment written as a
/// separate statement inside the spawned task never runs for a task dropped
/// before its first poll (a `JoinSet` dropped with tasks still queued, when the
/// actor unwinds out of `handle_flush_join_result` or the router drops the
/// actor), while dropping that task's future still fires the `Drop`. The gauge
/// clamps at 0 on read so nothing underflows, but the negative bias persists in
/// the map, and the router hands the same [`IngestMetrics`] to the respawned
/// actor for that shard index, so a shard biased to -2 would report 0 in flight
/// while two real flushes ran.
///
/// The guard is constructed on the actor, in the same non-awaiting region that
/// moves the buffer into the flush task, so the gauge counts a flush from the
/// moment its memory leaves the actor: a task still waiting for a permit holds
/// its buffer and its ADR-0069 byte charge exactly as an executing one does, and
/// that memory is what ADR-0067's in-flight consequence is about.
struct InFlightFlushGuard {
    metrics: Arc<IngestMetrics>,
    shard: u32,
}

impl InFlightFlushGuard {
    fn new(metrics: Arc<IngestMetrics>, shard: u32) -> Self {
        metrics.record_inflight_flush_delta(shard, 1);
        InFlightFlushGuard { metrics, shard }
    }
}

impl Drop for InFlightFlushGuard {
    fn drop(&mut self) {
        self.metrics.record_inflight_flush_delta(self.shard, -1);
    }
}

/// Which arm of a shard actor's `flush_tenant` wrote a generation-mismatch
/// flush in place.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum InPlaceCause {
    /// A hand-back target was dead, condemned or closed, or a drain found its
    /// mailbox still full.
    DeadTarget,
    /// The hand-back kept failing for the flush deferral cap.
    RetrySpent,
}

/// Most entries [`InPlaceWarnings`] holds. Past it, entries older than the
/// hour being recorded are dropped, and if that is not enough the map is
/// cleared; each clear lets every tenant and cause warn once more, so with
/// more pairs than this writing in place in one hour every such write warns.
pub(crate) const IN_PLACE_WARN_ENTRIES_MAX: usize = 4096;

/// For each tenant and [`InPlaceCause`], the last pinned ingest hour that
/// cause warned for, so a shard actor logs one in-place generation-mismatch
/// WARN per tenant, cause and ingest hour. Logging-only: nothing but the
/// decision to emit that WARN may read it.
#[derive(Debug, Default)]
pub(crate) struct InPlaceWarnings {
    last_warned: HashMap<(TenantHash, InPlaceCause), u32>,
}

impl InPlaceWarnings {
    /// Whether an in-place write of `tenant` for `cause`, pinned to
    /// `ingest_hour`, warns. When it does, the hour is recorded.
    pub(crate) fn should_warn(
        &mut self,
        tenant: TenantHash,
        cause: InPlaceCause,
        ingest_hour: u32,
    ) -> bool {
        let key = (tenant, cause);
        if self.last_warned.get(&key) == Some(&ingest_hour) {
            return false;
        }
        self.last_warned.insert(key, ingest_hour);
        if self.last_warned.len() > IN_PLACE_WARN_ENTRIES_MAX {
            self.last_warned.retain(|_, hour| *hour >= ingest_hour);
            if self.last_warned.len() > IN_PLACE_WARN_ENTRIES_MAX {
                self.last_warned.clear();
                self.last_warned.insert(key, ingest_hour);
            }
        }
        true
    }

    /// Ends `tenant`'s episode for both causes, so its next in-place write
    /// warns whatever its hour.
    pub(crate) fn rearm(&mut self, tenant: TenantHash) {
        self.last_warned.remove(&(tenant, InPlaceCause::DeadTarget));
        self.last_warned.remove(&(tenant, InPlaceCause::RetrySpent));
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.last_warned.len()
    }
}

pub(crate) struct ShardActor {
    shard: u32,
    writer_id: Uuid,
    epoch: u64,
    next_seq: u64,
    /// Monotonic floor for the flush-open stamp (ADR-1307): the largest
    /// `created_unix_ns` stamped for this SHARD, not just by this actor.
    /// `flush_tenant` raises each raw clock reading to this floor before
    /// stamping, so a backwards wall-clock step never mints a
    /// `created_unix_ns` below one already committed.
    ///
    /// Owned by the router's `ShardHandle` and shared with every incarnation of
    /// the shard's actor, so a supervisor respawn (issue #1299) carries the
    /// floor forward instead of restarting it at 0. Scoping it to one actor
    /// would narrow ADR-1307's guarantee to an incarnation, and a fresh
    /// `writer_id` does not make that safe: `writer_id` is not part of the
    /// query-time duplicate-resolution comparator (ADR-1307 "Known
    /// limitation").
    ///
    /// In-process state only, never persisted: it starts at 0 on process start
    /// by construction, so the guarantee is per-process and ADR-1307 records
    /// the cross-restart limitation (see
    /// [`ShardActor::monotonic_flush_open_ns`]).
    flush_floor_ns: Arc<AtomicI64>,
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
    /// Bounds concurrently in-flight flush tasks (ADR-0067 decision 2). The
    /// shard's one FIFO flush semaphore.
    semaphore: Arc<Semaphore>,
    /// Each tenant's flush share on this shard (ADR-2708 D3).
    shares: TenantFlushShares,
    /// Tracks spawned flush tasks so `join_all_flushes` can await durability
    /// before `FlushNow`/`Shutdown`/the channel-close drain return, and so
    /// the actor loop can opportunistically reap finished ones (the `select!`
    /// branch in `run`) rather than growing this set for the shard's whole
    /// lifetime.
    flushes: JoinSet<()>,
    rx: mpsc::Receiver<ShardMsg>,
    tenants: HashMap<TenantId, TenantBuf>,
    /// The configured ADR-0069 ceiling, shared live with the router so the
    /// per-buffer memory backstop is a fraction of the limit the operator set.
    /// Read at trigger time rather than resolved once, because the router
    /// installs the budget after the actors are spawned.
    backstop_ceiling: BufferBudgetCeiling,
    /// [`IngestConfig::flush_deferral_cap_ns`], resolved once.
    deferral_cap_ns: i64,
    /// The earliest `deferred_since_ns` across `tenants`, `None` when no buffer
    /// is deferred. Lowered on every refusal and recomputed after each age
    /// tick and drain, so between those it can only read older than the
    /// truth, which errs toward refusing an append rather than accepting one.
    /// Published to `cap_flag` on every change.
    oldest_deferral_ns: Option<i64>,
    /// This shard's at-cap flag, shared with the router's `ShardHandle` so the
    /// router refuses a write to a shard at the deferral cap before enqueue,
    /// buffered mode included.
    cap_flag: DeferralCapFlag,
    /// The router's generation state, for the scan-set check at flush open
    /// and the hand-back it may lead to (ADR-1642 scan-set amendment).
    scope: Arc<dyn FlushScope<ShardMsg>>,
    /// Set by a hand-back and cleared by the next flush that opens in place
    /// with nothing handed back, inside the scan set or written in place over
    /// a generation mismatch, so a run of hand-backs on this shard logs one
    /// WARN.
    handing_back: bool,
    /// Which in-place generation-mismatch writes already warned, per tenant,
    /// cause and ingest hour. A flush of the tenant that opens inside the scan
    /// set, or hands every row over, re-arms the tenant. Logging-only.
    in_place_warnings: InPlaceWarnings,
    /// Set for a drain's last pass over buffers a generation-mismatch
    /// hand-back left undelivered: a hand-back that fails again there writes
    /// the rows in place, so the drain does not answer with them buffered.
    mismatch_in_place: bool,
    /// Inside a drain, the tenants whose provisioning record this drain has
    /// already re-read after the scan-set check found no trusted view, so each
    /// is re-read once per drain rather than once per pass. `None` outside a
    /// drain, where an untrusted view only starts a background re-read.
    drain_rereads: Option<HashSet<TenantHash>>,
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
        flush_floor_ns: Arc<AtomicI64>,
        backstop_ceiling: BufferBudgetCeiling,
        cap_flag: DeferralCapFlag,
        scope: Arc<dyn FlushScope<ShardMsg>>,
        write_gate: WriteGateSlot,
        #[cfg(feature = "stage-timing")] stage_timings: Arc<MetricStageTimings>,
    ) -> Self {
        // A fresh incarnation starts with no buffers, so nothing it inherits
        // from a dead one is deferred.
        cap_flag.publish(None, clock.now_ns());
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
            write_gate,
            abandoned_commits: Mutex::new(VecDeque::new()),
        });
        ShardActor {
            shard,
            writer_id,
            epoch,
            next_seq: 0,
            flush_floor_ns,
            clock,
            config,
            metrics,
            rtt,
            ctx,
            semaphore: Arc::new(Semaphore::new(config.max_inflight_flushes as usize)),
            shares: TenantFlushShares::new(config.flush_share_per_tenant()),
            flushes: JoinSet::new(),
            rx,
            tenants: HashMap::new(),
            backstop_ceiling,
            deferral_cap_ns: config.flush_deferral_cap_ns(),
            oldest_deferral_ns: None,
            cap_flag,
            scope,
            handing_back: false,
            in_place_warnings: InPlaceWarnings::default(),
            mismatch_in_place: false,
            drain_rereads: None,
        }
    }

    pub(crate) async fn run(mut self) {
        // Dropped after every other local and before `self`, so a return or a
        // panic clears this actor's deferral before its mailbox closes.
        let _clear_cap_on_exit = self.cap_flag.clear_on_exit();
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
        let flush_tick_ns =
            i64::try_from(self.config.effective_flush_tick().as_nanos()).unwrap_or(i64::MAX);
        let mut next_tick_ns = clock.now_ns().saturating_add(flush_tick_ns);
        loop {
            let until_ns = next_tick_ns.saturating_sub(clock.now_ns()).max(0);
            let until = Duration::from_nanos(u64::try_from(until_ns).unwrap_or(u64::MAX));
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(ShardMsg::Write { tenant, points, exemplars, ack, charge, arrival }) => {
                            // Per-shard skew (issue #865): time the serial on-actor
                            // section only. The flush this may open runs in a spawned
                            // task, timed off-actor, and the `max_inflight_flushes`
                            // acquire now parks that task rather than the actor
                            // (issue #1292), so there is no on-actor permit wait left
                            // to exclude: this delta is pure merge-and-pin work.
                            let started_ns = self.clock.now_ns();
                            self.handle_write(tenant, points, exemplars, ack, charge, arrival)
                                .await;
                            let on_actor_ns =
                                self.clock.now_ns().saturating_sub(started_ns).max(0) as u64;
                            self.metrics.record_shard_processed(self.shard, on_actor_ns);
                        }
                        Some(ShardMsg::HandBack { tenant, points, exemplars, charges, arrival }) => {
                            self.handle_hand_back(tenant, points, exemplars, charges, arrival)
                                .await;
                        }
                        Some(ShardMsg::FlushNow { done }) => {
                            // Not a teardown: this arm does not break, so the
                            // actor keeps running and anything the drain could
                            // not stamp is retried by a later trigger.
                            self.flush_all(FlushTrigger::Manual, DrainIntent::Retryable)
                                .await;
                            let _ = done.send(());
                        }
                        Some(ShardMsg::Shutdown { done }) => {
                            let unprocessed = self.close_mailbox().await;
                            self.flush_all(FlushTrigger::Manual, DrainIntent::Teardown)
                                .await;
                            drop(unprocessed);
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
                            self.flush_all(FlushTrigger::Manual, DrainIntent::Teardown)
                                .await;
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
                    self.shares.prune();
                    self.flush_arrival_pending().await;
                    self.record_queued_flushes();
                }
            }
        }
    }

    /// Closes this actor's mailbox before its teardown flush, so every later
    /// send fails as closed and its sender keeps the message, and absorbs every
    /// hand-back already queued: a hand-back this mailbox accepted is written
    /// by this drain, unless its rows collide with a series id already
    /// buffered under another label set, which drops them with an ERROR
    /// (`absorb_rows`). Any other message still queued is returned for the
    /// caller to drop after the drain, which is when the receiver dropped it
    /// before this existed. A `Write` cannot be queued here through the
    /// router, whose `shutdown` consumes it.
    async fn close_mailbox(&mut self) -> Vec<ShardMsg> {
        self.rx.close();
        let mut unprocessed = Vec::new();
        while let Some(msg) = self.rx.recv().await {
            match msg {
                ShardMsg::HandBack {
                    tenant,
                    points,
                    exemplars,
                    charges,
                    arrival,
                } => {
                    self.absorb_rows(&tenant, points, exemplars, charges, arrival);
                }
                other => unprocessed.push(other),
            }
        }
        unprocessed
    }

    /// Merges one write into its tenant buffer and opens a size-triggered flush
    /// if the buffer crossed the threshold. Never parks on the flush permit: the
    /// acquire moved into the spawned flush task (issue #1292), so the whole
    /// on-actor path here is bounded by merge and pin work, never by a stalled
    /// prior flush.
    async fn handle_write(
        &mut self,
        tenant: TenantId,
        points: Vec<IngestPoint>,
        exemplars: Vec<IngestExemplar>,
        mut ack: Option<Ack>,
        charge: Option<Arc<IngestByteCharge>>,
        arrival: bool,
    ) {
        if points.is_empty() && exemplars.is_empty() && ack.is_none() {
            // Nothing buffered: drop the charge now so its bytes are refunded
            // rather than held for a message that touched no buffer.
            return;
        }
        let arrival_ns = self.clock.now_ns();
        if self.refuse_at_deferral_cap(&mut ack, arrival_ns) {
            return;
        }
        let points_len = points.len() as u64;
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
            .map(|b| {
                size_trigger_fires(
                    b.flush_est_bytes,
                    b.est_bytes,
                    &self.config,
                    self.backstop_ceiling.get(),
                )
            })
            .unwrap_or(false);
        if should_flush {
            if let Some(buf) = self.take_for_flush(&tenant) {
                self.flush_tenant(tenant, buf, FlushTrigger::Size, LagCheck::Enforced)
                    .await;
            }
        } else if arrival {
            self.open_arrival_flush(tenant).await;
        }
    }

    /// Removes `tenant`'s buffer to hand it to [`Self::flush_tenant`], clearing
    /// `arrival_pending`: a buffer `flush_tenant` puts back waits for its next
    /// trigger like any other, rather than being reopened by the reap as an
    /// arrival flush.
    fn take_for_flush(&mut self, tenant: &TenantId) -> Option<TenantBuf> {
        let mut buf = self.tenants.remove(tenant)?;
        buf.arrival_pending = false;
        Some(buf)
    }

    /// Opens `tenant`'s flush for a strict-arrival write when the tenant has
    /// none in flight, or marks the buffer for the reap that brings it to zero.
    /// Opening only at zero in flight keeps the queued-flush cap and the
    /// tenant share from ever refusing it (ADR-2793 decision 2).
    async fn open_arrival_flush(&mut self, tenant: TenantId) {
        if self.shares.in_flight(tenant.hash()) == 0 {
            if let Some(buf) = self.take_for_flush(&tenant) {
                self.flush_tenant(tenant, buf, FlushTrigger::Arrival, LagCheck::Enforced)
                    .await;
            }
        } else if let Some(buf) = self.tenants.get_mut(&tenant) {
            buf.arrival_pending = true;
            self.metrics.record_arrival_coalesced();
        }
    }

    /// Opens the flush of every buffer a strict-arrival write marked pending
    /// whose tenant now has no flush in flight, in [`Self::flush_aged`]'s
    /// order. The scan is over buffers the actor already holds, so it costs
    /// the tenant count per reap and nothing per write.
    async fn flush_arrival_pending(&mut self) {
        let mut due: Vec<(i64, i64, TenantId)> = self
            .tenants
            .iter()
            .filter(|(tenant, buf)| {
                buf.arrival_pending
                    && !buf.waiters.is_empty()
                    && self.shares.in_flight(tenant.hash()) == 0
            })
            .map(|(tenant, buf)| {
                (
                    buf.deferred_since_ns.unwrap_or(i64::MAX),
                    buf.oldest_arrival_ns.unwrap_or(i64::MAX),
                    tenant.clone(),
                )
            })
            .collect();
        if due.is_empty() {
            return;
        }
        due.sort_unstable_by_key(|(since, oldest, _)| (*since, *oldest));
        for (_, _, tenant) in due {
            if let Some(buf) = self.take_for_flush(&tenant) {
                self.flush_tenant(tenant, buf, FlushTrigger::Arrival, LagCheck::Enforced)
                    .await;
            }
        }
        self.refresh_oldest_deferral();
    }

    /// Merges rows another shard handed back at flush open (ADR-1642 scan-set
    /// amendment). They skip the deferral-cap refusal a new strict append
    /// gets, since they carry no waiter and were admitted once already, and
    /// their charges join this buffer's to be refunded by its flush. The
    /// arrival bookkeeping they carry widens this buffer's, so its age trigger
    /// sees how long they have waited.
    async fn handle_hand_back(
        &mut self,
        tenant: TenantId,
        points: Vec<IngestPoint>,
        exemplars: Vec<IngestExemplar>,
        charges: Vec<Arc<IngestByteCharge>>,
        arrival: HandBackArrival,
    ) {
        if !self.absorb_rows(&tenant, points, exemplars, charges, arrival) {
            return;
        }
        let should_flush = self.tenants.get(&tenant).is_some_and(|buf| {
            size_trigger_fires(
                buf.flush_est_bytes,
                buf.est_bytes,
                &self.config,
                self.backstop_ceiling.get(),
            )
        });
        if should_flush && let Some(buf) = self.take_for_flush(&tenant) {
            self.flush_tenant(tenant, buf, FlushTrigger::Size, LagCheck::Enforced)
                .await;
        }
    }

    /// Merges rows that are already admitted and charged into `tenant`'s
    /// buffer: a hand-back arriving here, or rows a failed hand-back send
    /// returned to this shard. Returns `false` when the rows were dropped on a
    /// collision.
    fn absorb_rows(
        &mut self,
        tenant: &TenantId,
        points: Vec<IngestPoint>,
        exemplars: Vec<IngestExemplar>,
        charges: Vec<Arc<IngestByteCharge>>,
        arrival: HandBackArrival,
    ) -> bool {
        let buf = self.tenants.entry(tenant.clone()).or_default();
        if let Err(err) = buf.absorb(points, exemplars, charges, arrival) {
            // A series id claiming a second label set is the ADR-0005 collision
            // a fresh write would have been refused for; there is no caller
            // left to answer, so the rows are dropped loudly.
            self.metrics.record_series_id_collision();
            tracing::error!(
                shard = self.shard,
                error = %err,
                "ravel-ingest: handed-back rows collide with this shard's buffer; dropping them"
            );
            return false;
        }
        true
    }

    /// Hands `buf`'s rows to the shards of the `count`-shard set that
    /// `shard_for` routes them to, instead of writing them under this shard's
    /// index outside the scan set of the hour it would pin, or into an hour
    /// another generation owns, as `reason` says (ADR-1642 scan-set
    /// amendment). Every target's liveness is checked before anything is taken
    /// apart, so a dead or closed target returns the buffer untouched, waiters
    /// included, for the caller to keep or write. Past that point strict
    /// waiters are answered `Abandoned` (outcome unknown: the rows are
    /// written, by a target shard or, if none can take them, by this one),
    /// and each target gets a clone of every charge, so each is refunded
    /// once, by the last of those flushes, and never charged twice.
    ///
    /// A target that closed after the liveness check fails its send, which
    /// returns the message: its rows are gathered into the returned
    /// [`HandedBack::kept`] buffer with the charges, once however many sends
    /// failed, for the caller to keep or write. Their waiters were already
    /// answered, so nothing is acknowledged twice.
    ///
    /// The send awaits the target's mailbox on this actor only when the target
    /// set is strictly smaller than this one ([`FlushScope::may_wait_on`]), so
    /// a chain of such waits ends and cannot cycle. A retired-index hand-back
    /// always qualifies (the target count is at most `scan_count`, which this
    /// index is not below); a generation-mismatch hand-back to a larger set
    /// sends only into a mailbox with room, and a full one fails the send.
    async fn hand_back(
        &mut self,
        tenant: &TenantId,
        mut buf: TenantBuf,
        count: u32,
        reason: HandBackReason,
    ) -> HandedBack<TenantBuf> {
        let mut targets: Vec<u32> = buf
            .series
            .keys()
            .chain(buf.exemplars.iter().map(|e| &e.series_id))
            .map(|id| shard_for(id, count))
            .collect();
        targets.sort_unstable();
        targets.dedup();
        let mut senders = Vec::with_capacity(targets.len());
        for &target in &targets {
            match self.scope.live_sender(count, target) {
                Some(tx) => senders.push(tx),
                None => {
                    return HandedBack {
                        delivered: false,
                        kept: Some(buf),
                        target_dead: true,
                    };
                }
            }
        }

        let waiters = std::mem::take(&mut buf.waiters);
        self.ctx.ack_waiters(
            waiters,
            Err(WriteError::Abandoned(reason.abandoned_message().into())),
        );
        let TenantBuf {
            series,
            exemplars,
            oldest_arrival_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            charges,
            deferred_since_ns,
            hand_back_blocked,
            mismatch_held_since_ns,
            ..
        } = buf;
        let arrival = HandBackArrival {
            oldest_arrival_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        };
        let mut points: HashMap<u32, Vec<IngestPoint>> = HashMap::new();
        for (series_id, accum) in series {
            let into = points.entry(shard_for(&series_id, count)).or_default();
            let labels = accum.labels;
            match accum.values {
                SeriesAccumValues::Scalar(samples) => {
                    into.extend(samples.into_iter().map(|s| IngestPoint {
                        series_id,
                        labels: Arc::clone(&labels),
                        value: IngestValue::Scalar(s),
                    }))
                }
                SeriesAccumValues::Histogram(samples) => {
                    into.extend(samples.into_iter().map(|h| IngestPoint {
                        series_id,
                        labels: Arc::clone(&labels),
                        value: IngestValue::Histogram(h),
                    }))
                }
            }
        }
        let mut exemplars_by: HashMap<u32, Vec<IngestExemplar>> = HashMap::new();
        for exemplar in exemplars {
            exemplars_by
                .entry(shard_for(&exemplar.series_id, count))
                .or_default()
                .push(exemplar);
        }
        let wait = self.scope.may_wait_on(count);
        let mut delivered = false;
        let mut kept: Option<TenantBuf> = None;
        let mut target_dead = false;
        for (target, tx) in targets.into_iter().zip(senders) {
            let msg = ShardMsg::HandBack {
                tenant: tenant.clone(),
                points: points.remove(&target).unwrap_or_default(),
                exemplars: exemplars_by.remove(&target).unwrap_or_default(),
                charges: charges.clone(),
                arrival,
            };
            let Err(refused) = send_hand_back(&tx, msg, wait).await else {
                delivered = true;
                continue;
            };
            target_dead |= refused.closed;
            let ShardMsg::HandBack {
                points,
                exemplars,
                charges: returned,
                ..
            } = refused.msg
            else {
                continue;
            };
            let charges = if kept.is_some() { Vec::new() } else { returned };
            let rest = kept.get_or_insert_with(|| TenantBuf {
                deferred_since_ns,
                hand_back_blocked,
                mismatch_held_since_ns,
                ..TenantBuf::default()
            });
            // Rows of one buffer cannot claim two label sets for one id.
            if let Err(err) = rest.absorb(points, exemplars, charges, arrival) {
                self.metrics.record_series_id_collision();
                tracing::error!(
                    shard = self.shard,
                    error = %err,
                    "ravel-ingest: returned hand-back rows could not merge; dropping them"
                );
            }
        }
        HandedBack {
            delivered,
            kept,
            target_dead,
        }
    }

    /// A buffer with a strict-mode waiter, or one whose flush would write at
    /// least `min_flush_bytes` of object,
    /// already justifies a PUT on the fast age clock; anything else is idle
    /// and waits for the slower `max_flush_delay_idle` instead (ADR-0051
    /// section 7), or, below a non-zero `idle_flush_byte_floor`, for the
    /// sub-floor hold, `max_flush_lifetime` less one `effective_flush_tick()` (ADR-1737,
    /// [`idle_age_threshold`]). "Worth a PUT"
    /// is a claim about the object, so this reads the
    /// object-bytes estimate, not the buffered-memory charge (issue #1305).
    /// Strict-mode ack latency is unaffected: a strict
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
        let has_priority =
            !buf.waiters.is_empty() || buf.flush_est_bytes >= self.config.min_flush_bytes;
        if !has_priority {
            return idle_age_threshold(buf.flush_est_bytes, &self.config);
        }
        let floor_ns = duration_nanos_saturating(self.config.max_flush_delay);
        if !self.config.adaptive_flush_delay {
            return (floor_ns, FlushTrigger::Age);
        }
        let retry_headroom_ns = duration_nanos_saturating(self.config.put_retry_base_delay);
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

    /// Fires every due age trigger. A deferred buffer is always due, since its
    /// trigger already fired once, possibly a size trigger on a buffer still
    /// below its age threshold, and stripping its waiters at the deferral cap
    /// can raise that threshold; it must not wait either out. Deferred
    /// buffers go first, oldest deferral first, then the rest by oldest row,
    /// so a freed queue slot goes to the flush that has waited longest rather
    /// than to whichever tenant `HashMap` order yields.
    async fn flush_aged(&mut self) {
        let now = self.clock.now_ns();
        let mut due: Vec<(i64, i64, TenantId, FlushTrigger)> = self
            .tenants
            .iter()
            .filter_map(|(tenant, buf)| {
                let oldest = buf.oldest_arrival_ns?;
                let (threshold_ns, trigger) = self.age_threshold_ns(buf);
                (buf.deferred_since_ns.is_some() || now.saturating_sub(oldest) >= threshold_ns)
                    .then(|| {
                        let since = buf.deferred_since_ns.unwrap_or(i64::MAX);
                        (since, oldest, tenant.clone(), trigger)
                    })
            })
            .collect();
        due.sort_unstable_by_key(|(since, oldest, ..)| (*since, *oldest));
        for (_, _, tenant, trigger) in due {
            if let Some(buf) = self.take_for_flush(&tenant) {
                self.flush_tenant(tenant, buf, trigger, LagCheck::Enforced)
                    .await;
            }
        }
        self.refresh_oldest_deferral();
    }

    /// Answers a strict-mode write with [`WriteError::DeferralCapReached`],
    /// taking its ack, when a deferred flush on this shard has been deferred
    /// for at least the deferral cap at `now_ns`. The router refuses such a
    /// write before enqueue; this catches one that was enqueued before the
    /// shard reached the cap. The caller returns on `true`, dropping the
    /// write's charge, which refunds it (ADR-0069). A buffered-mode write
    /// carries no ack because the router already acknowledged it, so it is
    /// never refused here and merges as usual.
    fn refuse_at_deferral_cap(&self, ack: &mut Option<Ack>, now_ns: i64) -> bool {
        let reached = self
            .oldest_deferral_ns
            .is_some_and(|since| deferral_cap_reached(since, now_ns, self.deferral_cap_ns));
        match ack.take() {
            Some(ack) if reached => {
                self.metrics.record_deferral_cap_refused();
                self.cap_flag.note_refusal(self.ctx.signal, self.shard);
                self.ctx
                    .ack_waiters(vec![ack], Err(WriteError::DeferralCapReached));
                true
            }
            other => {
                *ack = other;
                false
            }
        }
    }

    /// Recomputes `oldest_deferral_ns` from the buffers, clearing it once every
    /// deferred flush has opened, and publishes it to the router.
    fn refresh_oldest_deferral(&mut self) {
        self.oldest_deferral_ns = self
            .tenants
            .values()
            .filter_map(|buf| buf.deferred_since_ns)
            .min();
        self.cap_flag
            .publish(self.oldest_deferral_ns, self.clock.now_ns());
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

    /// Drains every buffered tenant, then awaits durability (ADR-1307 finding
    /// F1). A clock-refused flush re-buffers its tenant (`flush_tenant`'s
    /// `RegressionRefused` arm), so a single snapshot of the key set is not
    /// exhaustive: a tenant re-inserted after the snapshot consumed its key
    /// would never be retried in this call. On the `Shutdown` and channel-close
    /// paths there is no later actor tick to retry it, so the re-buffered
    /// tenant would drop on teardown -- the exact loss the channel-close arm
    /// forbids. Retry over fresh snapshots until the map empties, bounded by
    /// [`MAX_FLUSH_ALL_PASSES`]. A *regression* refusal (ADR-1307) re-anchors
    /// the monotonic floor to the raw reading, so the next pass stamps it and
    /// proceeds; for that refusal the bound only guards a pathological clock
    /// stepping back on every reading.
    ///
    /// A *lag* refusal (ADR-1685) re-anchors nothing, so the bound is what ends
    /// the enforced loop and a [`DrainIntent::Teardown`] then keeps making
    /// passes with [`LagCheck::BypassedAtTeardown`] while tenants remain, under
    /// the same bound, rather than strand acknowledged buffered-mode rows; see
    /// [`MAX_FLUSH_ALL_PASSES`] for why those passes are a loop, and the
    /// ADR-1685 teardown amendment for what teardown residue takes.
    /// [`DrainIntent::Retryable`] never bypasses: its
    /// actor keeps running, so a later trigger retries once the host clock
    /// converges.
    ///
    /// Residue left by the bounds is never dropped silently, but it is only a
    /// durability defect when nothing will retry it, so `intent` decides how it
    /// is reported: an ERROR and the `flush_all_residue_tenants` bump on a
    /// [`DrainIntent::Teardown`], a WARN on [`DrainIntent::Retryable`], where
    /// the residue is still in the tenant map with its arrival bookkeeping and
    /// the actor is still running to flush it.
    ///
    /// Rows a generation-mismatch hand-back returned from a full mailbox get
    /// more chances; a target that is not live has its rows written in place
    /// by the flush that found it. Between passes the actor yields, so the
    /// target can take them on the next pass. A [`DrainIntent::Retryable`]
    /// drain then makes one last pass, with the lag check still enforced,
    /// over the buffers still holding such rows, writing them in place if the
    /// hand-back fails again; a [`DrainIntent::Teardown`] reaches the same
    /// write on its bypass passes. So at most [`MAX_FLUSH_ALL_PASSES`]
    /// enforced passes, then one last pass or that many bypass passes. The
    /// drain can still answer with such rows buffered: on that last pass a
    /// lag or regression refusal, a view still `Unknown` after the drain's
    /// re-read, or a verdict that flipped to a retired index whose hand-back
    /// fails keeps them for a later trigger, and on a teardown rows a
    /// regression refusal keeps through every bypass pass are residue like
    /// any other.
    async fn flush_all(&mut self, trigger: FlushTrigger, intent: DrainIntent) {
        self.drain_rereads = Some(HashSet::new());
        let mut passes = 0;
        while !self.tenants.is_empty() && passes < MAX_FLUSH_ALL_PASSES {
            self.flush_all_pass(trigger, LagCheck::Enforced).await;
            passes += 1;
            if self.holds_mismatch_rows() {
                tokio::task::yield_now().await;
            }
        }
        if matches!(intent, DrainIntent::Retryable) && self.holds_mismatch_rows() {
            self.mismatch_in_place = true;
            let held: Vec<TenantId> = self
                .tenants
                .iter()
                .filter(|(_, buf)| buf.mismatch_held_since_ns.is_some())
                .map(|(tenant, _)| tenant.clone())
                .collect();
            for tenant in held {
                if let Some(buf) = self.take_for_flush(&tenant) {
                    self.flush_tenant(tenant, buf, trigger, LagCheck::Enforced)
                        .await;
                }
            }
            self.mismatch_in_place = false;
        }
        let mut bypass_passes = 0;
        if matches!(intent, DrainIntent::Teardown) {
            while !self.tenants.is_empty() && bypass_passes < MAX_FLUSH_ALL_PASSES {
                self.flush_all_pass(trigger, LagCheck::BypassedAtTeardown)
                    .await;
                bypass_passes += 1;
            }
        }
        if !self.tenants.is_empty() {
            let (tenant_count, buffered_points) = self.buffered_summary();
            match intent {
                DrainIntent::Teardown => {
                    self.metrics.record_flush_all_residue(tenant_count as u64);
                    tracing::error!(
                        shard = self.shard,
                        tenant_count,
                        buffered_points,
                        passes,
                        bypass_passes,
                        "ravel-ingest: flush_all left buffered tenants unflushed after \
                         exhausting retry passes; acknowledged buffered-mode rows lost \
                         on this graceful drain"
                    );
                }
                DrainIntent::Retryable => {
                    tracing::warn!(
                        shard = self.shard,
                        tenant_count,
                        buffered_points,
                        passes,
                        "ravel-ingest: flush_all left buffered tenants unflushed after \
                         exhausting retry passes; the actor keeps running and these \
                         tenants stay buffered, so the next trigger (age tick or \
                         explicit flush) retries them"
                    );
                }
            }
        }
        self.drain_rereads = None;
        self.join_all_flushes().await;
        self.refresh_oldest_deferral();
    }

    /// Whether a buffer still holds rows a generation-mismatch hand-back left
    /// undelivered.
    fn holds_mismatch_rows(&self) -> bool {
        self.tenants
            .values()
            .any(|buf| buf.mismatch_held_since_ns.is_some())
    }

    /// One drain pass: a fresh snapshot of the buffered tenant keys, each
    /// flushed under `lag_check`. A refused flush re-inserts its key, which the
    /// next pass's snapshot picks up.
    async fn flush_all_pass(&mut self, trigger: FlushTrigger, lag_check: LagCheck) {
        let tenants: Vec<TenantId> = self.tenants.keys().cloned().collect();
        for tenant in tenants {
            if let Some(buf) = self.take_for_flush(&tenant) {
                self.flush_tenant(tenant, buf, trigger, lag_check).await;
            }
        }
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
            self.record_queued_flushes();
        }
        self.shares.prune();
    }

    /// The flush-open stamp for this flush: `raw_ns` (the single flush-open
    /// clock reading) raised to this writer's monotonic floor (ADR-1307).
    ///
    /// `raw_ns` is plausibility-checked here first
    /// (`checked_ingest_hour_bucket`), so a sub-floor or otherwise garbage
    /// reading fails loud before the floor can hide it: raising a garbage
    /// reading to an armed floor would defeat that check and stamp a valid-
    /// looking `created_unix_ns` onto a flush whose clock is broken.
    ///
    /// The reading carries no ordering guarantee on its own: a backwards
    /// wall-clock step (an NTP correction, a manual set) can read below a stamp
    /// this writer already committed. Stamping that raw reading as
    /// `created_unix_ns` would let a stale duplicate sample outrank its own
    /// correction under the query-time dedup order (docs/catalog-and-mvcc.md
    /// "Cross-segment duplicate samples"), whose primary key is
    /// `created_unix_ns`. Raising each reading to the shard's floor keeps
    /// stamps non-decreasing for this shard within the process lifetime,
    /// across supervisor respawns as well as within one actor, and counts
    /// every step it absorbs (`clock_regressions`).
    ///
    /// A backwards step is absorbed only up to [`MAX_FLUSH_CLOCK_HOLD_NS`].
    /// Beyond that the flush is refused with a typed error rather than stamped
    /// (`clock_regressions_refused`) and the floor re-anchors to `raw_ns`, so a
    /// single spurious forward glitch that ratcheted the floor into a future
    /// ingest hour cannot pin every later flush there: exactly the one flush
    /// that crosses the bound fails, and the next normal reading proceeds. See
    /// [`MAX_FLUSH_CLOCK_HOLD_NS`] for why an unbounded hold must fail loud.
    ///
    /// The floor is in-process state, never persisted and never read back after
    /// a restart, so it resets to 0 by construction. That default preserves the
    /// fail-loud path for a non-positive clock: the raw check above rejects the
    /// reading before the floor is consulted. The guarantee is per-process;
    /// ADR-1307 records the cross-restart limitation.
    ///
    /// The floor lives in the router's per-shard `ShardHandle` rather than in
    /// this actor, so a respawn after a shard death (issue #1299) continues the
    /// same floor. One incarnation of a shard runs at a time (the router
    /// spawns the replacement only after observing the previous actor's channel
    /// close), so the atomic is uncontended here; the `fetch_max` on the
    /// advancing path keeps it correct anyway, while the refusal path below
    /// deliberately re-anchors DOWNWARD and so must store rather than max.
    fn monotonic_flush_open_ns(
        &mut self,
        raw_ns: i64,
        lag_check: LagCheck,
    ) -> Result<i64, FlushClockError> {
        checked_ingest_hour_bucket(raw_ns).map_err(FlushClockError::InvalidReading)?;
        match store_clock_lag(raw_ns, self.ctx.store.observed_store_time_ns()) {
            StoreClockLag::WithinAllowance => {}
            StoreClockLag::Unobserved => self.metrics.record_clock_lag_unchecked(),
            StoreClockLag::Refused { lag_ns, msg } => match lag_check {
                LagCheck::Enforced => {
                    self.metrics.record_clock_lag_refused();
                    tracing::warn!(
                        shard = self.shard,
                        raw_ns,
                        lag_ns,
                        "ravel-ingest: flush clock lags the object store's observed clock beyond the clock-skew allowance; refusing the flush"
                    );
                    return Err(FlushClockError::LagRefused(msg));
                }
                LagCheck::BypassedAtTeardown => {
                    self.metrics.record_clock_lag_bypassed_at_shutdown();
                    tracing::warn!(
                        shard = self.shard,
                        raw_ns,
                        lag_ns,
                        "ravel-ingest: teardown bypass pass: publishing despite store-clock lag; the commit may land in a sealed hour"
                    );
                }
            },
        }
        let floor_ns = self.flush_floor_ns.load(Ordering::Acquire);
        if raw_ns >= floor_ns {
            self.flush_floor_ns.fetch_max(raw_ns, Ordering::AcqRel);
            return Ok(raw_ns);
        }
        let held_ns = floor_ns - raw_ns;
        if held_ns > MAX_FLUSH_CLOCK_HOLD_NS {
            self.flush_floor_ns.store(raw_ns, Ordering::Release);
            self.metrics.record_clock_regression_refused();
            tracing::warn!(
                shard = self.shard,
                held_ns,
                bound_ns = MAX_FLUSH_CLOCK_HOLD_NS,
                "ravel-ingest: flush clock regressed beyond the monotonic hold bound; refusing the flush and re-anchoring the floor"
            );
            return Err(FlushClockError::RegressionRefused(format!(
                "flush clock regressed {held_ns} ns below the previous flush-open stamp, \
                 beyond the monotonic hold bound of {MAX_FLUSH_CLOCK_HOLD_NS} ns; \
                 refusing the flush (ADR-1307)"
            )));
        }
        self.metrics.record_clock_regression();
        tracing::warn!(
            shard = self.shard,
            regression_ns = held_ns,
            "ravel-ingest: flush clock stepped backwards; held flush-open stamp to the shard's monotonic floor"
        );
        Ok(floor_ns)
    }

    /// Whether `trigger` must be refused because this shard already has
    /// `max_queued_flushes` flush tasks spawned and not yet reaped (issue
    /// #1740). `JoinSet::len` is exactly that count: a task leaves the set only
    /// when the select loop's reap arm or a drain joins it, so it covers both
    /// the flushes executing against the object store and the ones parked on
    /// the `max_inflight_flushes` semaphore. That parked queue is what ADR-1642
    /// left unbounded, and under `IngestByteBudgetLimit::Unlimited` nothing
    /// else bounds it.
    ///
    /// [`FlushTrigger::Manual`] is never refused. A drain (`FlushNow`,
    /// shutdown, channel close) has no later tick to retry a refusal, so
    /// refusing there would strand acknowledged buffered-mode rows on
    /// teardown; it also needs no bound of its own, since `flush_all` awaits
    /// every spawned flush before it returns.
    ///
    /// A buffer that has crossed its per-(shard, tenant) memory backstop
    /// ([`memory_backstop_crossed`]) is never refused either, whatever the
    /// trigger: that backstop is the ONLY bound on one buffer's resident
    /// memory, and under [`crate::IngestByteBudgetLimit::Unlimited`] nothing
    /// sheds behind it. Refusing there would hold this many flush windows AND
    /// let the buffer grow for as long as the store stays stalled, which is a
    /// worse failure than the queue this cap exists to bound. So the queue may
    /// exceed `max_queued_flushes` under memory pressure, by one window per
    /// backstop's worth of buffered memory (PR #1903 review finding 1): a
    /// bounded buffer plus a queue that grows only as fast as memory fills,
    /// rather than a bounded queue plus an unbounded buffer. Slower is not
    /// bounded: no count bounds the exempt windows, and under
    /// [`crate::IngestByteBudgetLimit::Unlimited`] with the store stalled only
    /// the length of the stall does. The exempt path is
    /// this early return, and it is the only way past the length check below;
    /// a refusal is always counted on `flush_trigger_deferred`, so a queue
    /// reading above the cap with no deferral behind it is the exemption at
    /// work.
    ///
    /// ADR-2708 D3 adds the per-tenant share on top. A buffer that has crossed
    /// the object backstop ([`object_backstop_crossed`]) is exempt like a
    /// memory-backstop crossing. Otherwise a tenant already holding its
    /// [`IngestConfig::flush_share_per_tenant`] of spawned flushes is refused
    /// whatever the queue length, and a tenant with none spawned is never
    /// refused by the queue cap, so a neighbour's stalled windows cannot fill
    /// the queue against it. Both refusals take the same deferral path.
    fn queued_flush_cap_reached(
        &self,
        trigger: FlushTrigger,
        tenant_hash: TenantHash,
        buf: &TenantBuf,
    ) -> bool {
        if matches!(trigger, FlushTrigger::Manual) {
            return false;
        }
        if memory_backstop_crossed(buf.est_bytes, &self.config, self.backstop_ceiling.get())
            || object_backstop_crossed(buf.flush_est_bytes, &self.config)
        {
            return false;
        }
        if self.shares.at_share(tenant_hash) {
            return true;
        }
        if self.shares.in_flight(tenant_hash) == 0 {
            return false;
        }
        self.flushes.len() >= self.config.queued_flush_cap()
    }

    /// The scan-set check at flush open (ADR-1642 scan-set amendment). Inside
    /// a drain, a tenant whose view cannot be trusted is re-read once, waiting
    /// at most `max_flush_lifetime`, and checked again, so one drain flushes
    /// what the re-read confirms; elsewhere an untrusted view only starts a
    /// background re-read and the next trigger retries, and the actor never
    /// waits on the store. The drain first moves the buffer's stale-view hold
    /// (`hold`), so the read it waits on is the one a spent hold requested,
    /// and skips the wait when the hold already drops the buffer.
    async fn scan_check(
        &mut self,
        tenant: TenantHash,
        hour: u32,
        now_ns: i64,
        hold: &mut StaleViewHold,
        lag_check: LagCheck,
    ) -> ScanCheck {
        let verdict = self.scope.check(tenant, self.shard, hour, now_ns);
        if verdict != ScanCheck::Unknown
            || !self
                .drain_rereads
                .as_mut()
                .is_some_and(|reread| reread.insert(tenant))
            || hold.advance(
                self.scope.as_ref(),
                tenant,
                now_ns,
                self.config.max_flush_lifetime,
                matches!(lag_check, LagCheck::Enforced),
            ) == HoldState::Drop
        {
            return verdict;
        }
        reread_and_check(
            self.scope.as_ref(),
            self.clock.as_ref(),
            self.config.max_flush_lifetime,
            tenant,
            self.shard,
            hour,
            now_ns,
        )
        .await
    }

    /// Whether a flush on a generation view this router cannot trust stays
    /// unopened, its buffer kept whole for a later trigger (ADR-1642 scan-set
    /// amendment). Always, except on a teardown bypass pass, which a drain
    /// reaches only after its own synchronous re-read failed: a drain that is
    /// about to end the actor writes the buffer in place instead, since a row
    /// written where the read side may not scan can still be found and a row
    /// dropped at shutdown cannot. That write is logged.
    fn keeps_unconfirmed_flush(&self, lag_check: LagCheck, reason: &str) -> bool {
        if matches!(lag_check, LagCheck::Enforced) {
            return true;
        }
        tracing::warn!(
            signal = ?self.ctx.signal,
            shard = self.shard,
            reason,
            "ravel-ingest: teardown bypass pass: writing a flush in place although it could \
             not be confirmed inside the read-side scan set"
        );
        false
    }

    /// Drops a buffer whose flush was held closed on an untrusted generation
    /// view for `max_flush_lifetime` with no provisioning read succeeding, and
    /// for which a read issued after that did not confirm the view (issue
    /// #4363, [`StaleViewHold`]). Nothing is written: strict waiters get
    /// `Abandoned`, and dropping `charges` refunds the byte budget. A teardown
    /// pass drops a spent hold too, rather than writing it in place.
    fn drop_stale_view_flush(&self, tenant_hash: TenantHash, buf: TenantBuf, lag_check: LagCheck) {
        let rows: usize = buf.series.values().map(|accum| accum.values.len()).sum();
        self.metrics.record_abandoned_stale_provisioning();
        if !buf.exemplars.is_empty() {
            self.metrics.record_exemplars(0, buf.exemplars.len() as u64);
        }
        tracing::error!(
            signal = ?self.ctx.signal,
            shard = self.shard,
            tenant_hash = %tenant_hash.to_hex(),
            rows,
            bytes = buf.est_bytes,
            teardown = !matches!(lag_check, LagCheck::Enforced),
            "ravel-ingest: dropping a flush held closed for max_flush_lifetime on a provisioning \
             view this router cannot trust: a read issued after the hold was spent did not \
             confirm the view; its rows are not written"
        );
        self.ctx.ack_waiters(
            buf.waiters,
            Err(WriteError::Abandoned(STALE_VIEW_ABANDONED.into())),
        );
    }

    /// Publishes this shard's spawned-but-unreaped flush-task count (issue
    /// #1740). Called wherever `flushes` changes length, so the gauge always
    /// reads the value the next trigger will be tested against.
    fn record_queued_flushes(&self) {
        self.metrics
            .record_shard_flushes_queued(self.shard, self.flushes.len() as u64);
    }

    /// Pins `buf`'s flush identity, then moves `buf`'s payload and waiters
    /// into a task spawned onto [`FlushCtx::run_flush`] (ADR-0067 decision
    /// 1). Everything runs here on the actor EXCEPT the `max_inflight_flushes`
    /// acquire and the flush itself, both of which run inside the spawned task
    /// (issue #1292): the actor returns from this call the moment the task is
    /// spawned, so a slow encode, a stalled PUT, or a full-permit wait never
    /// blocks it from processing its next message or firing its age tick, even
    /// at `max_inflight_flushes == 1`.
    ///
    /// An empty buffer (exemplars only, no samples) never reaches the
    /// semaphore or a spawned task at all: there is nothing to encode, and a
    /// flush identity pinned for nothing would burn a `seq` for no object.
    async fn flush_tenant(
        &mut self,
        tenant: TenantId,
        mut buf: TenantBuf,
        trigger: FlushTrigger,
        lag_check: LagCheck,
    ) {
        if buf.series.is_empty() {
            // Nothing to write. Exemplars without any buffered sample cannot
            // be written at all (an exemplar points at a measurement), so they
            // are dropped, and counted so the drop stays visible. Dropping
            // `charges` here refunds the budget for those exemplars (ADR-0069):
            // no flush task will run to do it, since this buffer never spawns one.
            if !buf.exemplars.is_empty() {
                self.metrics.record_exemplars(0, buf.exemplars.len() as u64);
            }
            drop(buf.charges);
            // `waiters` is empty here by construction: the router mints a
            // strict-mode ack only for a shard that received points, so a
            // shard holding nothing but exemplars has nobody to answer. If
            // that ever changes, this returns without acking and the router
            // reads the dropped oneshot as a dead shard.
            debug_assert!(buf.waiters.is_empty());
            return;
        }
        let raw_ns = self.clock.now_ns();
        let tenant_hash = tenant.hash();
        let refused = self.queued_flush_cap_reached(trigger, tenant_hash, &buf);
        if refused {
            buf.deferred_since_ns.get_or_insert(raw_ns);
        }
        if buf
            .deferred_since_ns
            .is_some_and(|since| deferral_cap_reached(since, raw_ns, self.deferral_cap_ns))
            && !buf.waiters.is_empty()
        {
            // This buffer has been deferred for the whole deferral cap, so a
            // flush opening from here on pins an ingest hour past what the
            // read-side slack covers for its strict rows. Do not acknowledge
            // them from that flush. The rows are still written, by the flush
            // that opens past the cap, so the answer is the outcome-unknown
            // `Abandoned` (503), not a refusal: a client retry stores a second
            // copy, which query-time dedup by `(series_id, ts)` collapses.
            let waiters = std::mem::take(&mut buf.waiters);
            self.ctx.ack_waiters(
                waiters,
                Err(WriteError::Abandoned(DEFERRAL_CAP_ABANDONED.into())),
            );
        }
        if refused {
            // Issue #1740: this shard is already holding `max_queued_flushes`
            // flush windows, and this buffer is still under its memory
            // backstop. Refuse the trigger rather than spawn another task
            // to park on the semaphore, and put the buffer back exactly as it
            // arrived: series, exemplars, waiters, `charges`, and the trigger
            // bookkeeping (`flush_est_bytes`, `est_bytes`, `oldest_arrival_ns`)
            // all ride back, so the age clock is not reset and the next tick
            // re-fires this same trigger once a flush has been reaped. Nothing
            // is acked and nothing is dropped, so this is a deferral, not a
            // shed (ADR-1642 amendment).
            //
            // The rows carry no ingest-hour bucket across the deferral. The
            // bucket is pinned below, from the reading taken by the flush that
            // finally opens, so a deferral adds its own length to the gap
            // between a record's routing and its bucket. What keeps that gap
            // inside `ravel_catalog::FLUSH_BOUND_SLACK_HOURS` for every
            // acknowledged strict row is the deferral cap: `deferred_since_ns`
            // dates the first refusal, a strict waiter is never acknowledged
            // from a flush that opens past the cap (above), and once the oldest
            // deferral on this shard reaches the cap the router and this actor
            // refuse new writes until it opens. Pinning before this check
            // instead would move the overrun past the catalog's sealed-hour
            // watermark, where it is unrecoverable rather than bounded; this
            // module's tests hold both.
            self.metrics.record_shard_flush_trigger_deferred(self.shard);
            let since = buf.deferred_since_ns.unwrap_or(raw_ns);
            self.oldest_deferral_ns = Some(self.oldest_deferral_ns.map_or(since, |o| o.min(since)));
            self.cap_flag.publish(self.oldest_deferral_ns, raw_ns);
            self.tenants.insert(tenant, buf);
            return;
        }
        // The flush-open stamp is decided before the buffer is consumed and
        // before `record_flush`: a refused flush never touched the store, so it
        // must not be counted as a flush that happened, and (on the retryable
        // arm) its rows must be re-buffered rather than dropped (ADR-1307
        // finding 1).
        let flush_open_ns = match self.monotonic_flush_open_ns(raw_ns, lag_check) {
            Ok(ns) => ns,
            Err(FlushClockError::InvalidReading(msg)) => {
                // A grossly broken raw reading is fail-loud and non-retryable
                // (`SegmentBuild`, 400): the buffer is dropped. Exemplars are
                // counted so the drop stays visible (finding 2); `charges` refund
                // on drop.
                if !buf.exemplars.is_empty() {
                    self.metrics.record_exemplars(0, buf.exemplars.len() as u64);
                }
                self.metrics.record_abandoned_input_rejected();
                self.ctx
                    .ack_waiters(buf.waiters, Err(WriteError::SegmentBuild(msg)));
                return;
            }
            Err(FlushClockError::RegressionRefused(msg) | FlushClockError::LagRefused(msg)) => {
                // Both arms are already counted inside the helper
                // (`clock_regressions_refused`, `clock_lag_refused`) and are
                // transient server conditions a later flush recovers from, so
                // both are retryable (`Abandoned`, 503), not a client
                // `SegmentBuild` (400) that would drop the buffered rows on a
                // conformant exporter. What recovers them differs.
                //
                // A REGRESSION refusal re-anchored the floor to `raw_ns` inside
                // the helper, so the next reading at or above `raw_ns`
                // proceeds: the bound is per backwards step, not global, and a
                // single backwards step refuses exactly one flush. It is not a
                // guarantee that only one flush is refused over the process
                // lifetime (ADR-1307 Consequences). A clock that keeps stepping
                // back beyond the bound refuses every flush for as long as that
                // continues, and within one drain the absorb path returns the held
                // stamp without advancing the floor, so a receding clock can refuse
                // one tenant, absorb the next few against the re-anchored value,
                // and refuse again.
                //
                // A LAG refusal (ADR-1685) re-anchors nothing, so every retry
                // against the same clock refuses identically until the host
                // clock converges. `MAX_FLUSH_ALL_PASSES` carries what that
                // means inside a drain, including the teardown bypass passes.
                //
                // Re-buffer the rows
                // so that next trigger flushes them (finding 1): `charges` ride
                // back with the buffer (the byte budget is not refunded, the bytes
                // are still held), and the whole buffer -- series, exemplars, and
                // the trigger bookkeeping (`flush_est_bytes`, which drives the
                // size trigger, `est_bytes`, which drives the memory backstop,
                // and `oldest_arrival_ns`) -- is preserved intact. Only `waiters` are acked here and taken out of
                // the re-inserted buffer: a waiter left in it would be re-acked by
                // the next flush against an already-answered oneshot. A strict-mode
                // waiter that retries on the 503 re-enqueues rows this buffer still
                // holds; the query-time dedup by `(series_id, ts)` collapses the
                // duplicate, so the retry is safe.
                let waiters = std::mem::take(&mut buf.waiters);
                self.ctx
                    .ack_waiters(waiters, Err(WriteError::Abandoned(msg)));
                self.tenants.insert(tenant, buf);
                return;
            }
        };
        let ingest_hour_bucket = match checked_ingest_hour_bucket(flush_open_ns) {
            Ok(bucket) => bucket,
            Err(msg) => {
                // Defensive: `flush_open_ns` was already hour-bucket-validated (it
                // is either `raw_ns`, checked at the helper entry, or the floor,
                // itself a previously-checked raw reading). If it ever fails here
                // it is fail-loud like InvalidReading: drop the buffer, count the
                // dropped exemplars so the drop stays visible (finding 2), refund
                // `charges` on drop.
                if !buf.exemplars.is_empty() {
                    self.metrics.record_exemplars(0, buf.exemplars.len() as u64);
                }
                self.metrics.record_abandoned_input_rejected();
                self.ctx
                    .ack_waiters(buf.waiters, Err(WriteError::SegmentBuild(msg)));
                return;
            }
        };
        // ADR-1642 scan-set amendment: write under this shard index only if
        // the read side scans it for the hour this flush is about to pin, and
        // no other generation owns that hour alone (issue #2429).
        let verdict = self
            .scan_check(
                tenant_hash,
                ingest_hour_bucket,
                raw_ns,
                &mut buf.stale_view_hold,
                lag_check,
            )
            .await;
        let buf = match verdict {
            ScanCheck::InScanSet => {
                self.handing_back = false;
                self.in_place_warnings.rearm(tenant_hash);
                buf.stale_view_hold.clear();
                buf
            }
            ScanCheck::HandBack {
                target_count,
                reason: HandBackReason::GenerationMismatch,
                ..
            } if mismatch_retry_spent(
                buf.mismatch_held_since_ns,
                buf.deferred_since_ns,
                raw_ns,
                self.deferral_cap_ns,
            ) =>
            {
                // This flush opens at or past the cap, so, as on the deferral
                // path, no strict waiter is acknowledged from it.
                buf.stale_view_hold.clear();
                let waiters = std::mem::take(&mut buf.waiters);
                self.ctx.ack_waiters(
                    waiters,
                    Err(WriteError::Abandoned(DEFERRAL_CAP_ABANDONED.into())),
                );
                self.metrics.record_generation_mismatch_written_in_place();
                self.handing_back = false;
                if self.in_place_warnings.should_warn(
                    tenant_hash,
                    InPlaceCause::RetrySpent,
                    ingest_hour_bucket,
                ) {
                    tracing::warn!(
                        signal = ?self.ctx.signal,
                        shard = self.shard,
                        tenant_hash = %tenant_hash.to_hex(),
                        ingest_hour_bucket,
                        target_count,
                        "ravel-ingest: a generation-mismatch hand-back kept failing for the \
                         flush deferral cap; writing the flush in place in an ingest hour \
                         another shard generation owns; readers find the rows, but a pushdown \
                         split over this hour may see their series at two shard indices"
                    );
                }
                buf
            }
            ScanCheck::HandBack {
                scan_count,
                target_count,
                reason,
            } => {
                let handed = self.hand_back(&tenant, buf, target_count, reason).await;
                if handed.delivered {
                    self.metrics.record_rerouted_flush(reason);
                    if !std::mem::replace(&mut self.handing_back, true) {
                        tracing::warn!(
                            signal = ?self.ctx.signal,
                            shard = self.shard,
                            ingest_hour_bucket,
                            scan_count,
                            target_count,
                            reason = reason.label(),
                            "ravel-ingest: flush would write outside the read-side scan set \
                             of its ingest hour, or into an hour another shard generation \
                             owns; handing its rows to that hour's shard generation"
                        );
                    }
                }
                let target_dead = handed.target_dead;
                let Some(mut buf) = handed.kept else {
                    self.in_place_warnings.rearm(tenant_hash);
                    return;
                };
                buf.stale_view_counted = false;
                buf.stale_view_hold.clear();
                let enforced = matches!(lag_check, LagCheck::Enforced);
                // A target that is not live cannot take the rows on a later
                // flush, so only a full mailbox is retried.
                if reason == HandBackReason::GenerationMismatch
                    && (target_dead || !enforced || self.mismatch_in_place)
                {
                    self.metrics.record_generation_mismatch_written_in_place();
                    if !handed.delivered {
                        self.handing_back = false;
                    }
                    if self.in_place_warnings.should_warn(
                        tenant_hash,
                        InPlaceCause::DeadTarget,
                        ingest_hour_bucket,
                    ) {
                        tracing::warn!(
                            signal = ?self.ctx.signal,
                            shard = self.shard,
                            tenant_hash = %tenant_hash.to_hex(),
                            ingest_hour_bucket,
                            target_count,
                            target_dead,
                            "ravel-ingest: writing a flush in place in an ingest hour another \
                             shard generation owns, because a hand-back target shard is dead, \
                             condemned or closed, or a drain found its mailbox still full; \
                             readers find the rows, but a pushdown split over this hour may \
                             see their series at two shard indices"
                        );
                    }
                    buf
                } else if enforced {
                    if reason == HandBackReason::GenerationMismatch {
                        buf.mismatch_held_since_ns.get_or_insert(raw_ns);
                    }
                    if !std::mem::replace(&mut buf.hand_back_blocked, true) {
                        self.metrics.record_hand_back_failure();
                        tracing::warn!(
                            signal = ?self.ctx.signal,
                            shard = self.shard,
                            tenant_hash = %tenant_hash.to_hex(),
                            ingest_hour_bucket,
                            target_count,
                            "ravel-ingest: a hand-back target shard is dead, closed, or its \
                             mailbox full on a send that may not wait; keeping the rows in \
                             this shard's buffer for a later flush to retry (the next write \
                             routed to a dead metrics shard respawns it)"
                        );
                    }
                    self.tenants.insert(tenant, buf);
                    return;
                } else {
                    self.metrics.record_teardown_unscanned_write();
                    tracing::error!(
                        signal = ?self.ctx.signal,
                        shard = self.shard,
                        tenant_hash = %tenant_hash.to_hex(),
                        ingest_hour_bucket,
                        scan_count,
                        target_count,
                        "ravel-ingest: teardown bypass pass: writing a flush in place under a \
                         shard index readers do not scan for its ingest hour, because a \
                         hand-back target shard is dead or closed; the rows are stored but no \
                         query returns them"
                    );
                    buf
                }
            }
            ScanCheck::Unknown => {
                let mut buf = buf;
                if !std::mem::replace(&mut buf.stale_view_counted, true) {
                    self.metrics.record_stale_provisioning_flush();
                }
                let enforced = matches!(lag_check, LagCheck::Enforced);
                if buf
                    .stale_view_hold
                    .advance(
                        self.scope.as_ref(),
                        tenant_hash,
                        raw_ns,
                        self.config.max_flush_lifetime,
                        enforced,
                    )
                    .drops(enforced)
                {
                    self.drop_stale_view_flush(tenant_hash, buf, lag_check);
                    return;
                }
                if self.keeps_unconfirmed_flush(lag_check, "no trusted generation view") {
                    self.tenants.insert(tenant, buf);
                    return;
                }
                buf
            }
        };
        // ADR-1746 decision 4: a flush whose object would sit below the
        // tenant's recorded RSEG floor is refused here, on the cached record,
        // before a `seq` is pinned, the flush is counted, or the task that
        // takes a permit and PUTs is spawned.
        let open_version = self.scope.writing_version(FormatFamily::Rseg);
        let buf = match self
            .scope
            .floor_check(tenant_hash, FormatFamily::Rseg, open_version)
        {
            FloorCheck::Write => buf,
            FloorCheck::WriteBelow(below) => {
                warn_flush_written_below(self.ctx.signal, self.shard, tenant_hash, &below);
                buf
            }
            FloorCheck::Refuse(refused) => {
                warn_flush_refused(self.ctx.signal, self.shard, tenant_hash, &refused);
                if !buf.exemplars.is_empty() {
                    self.metrics.record_exemplars(0, buf.exemplars.len() as u64);
                }
                self.ctx
                    .ack_waiters(buf.waiters, Err(WriteError::BelowFormatFloor(refused)));
                return;
            }
            FloorCheck::NoView => {
                if self.keeps_unconfirmed_flush(lag_check, "no cached format floor") {
                    self.tenants.insert(tenant, buf);
                    return;
                }
                buf
            }
        };
        let seq = self.next_seq;
        self.next_seq += 1;
        let TenantBuf {
            series,
            exemplars,
            est_bytes,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            waiters,
            charges,
            ..
        } = buf;
        self.metrics.record_flush(trigger);
        if trigger == FlushTrigger::Size {
            self.metrics.record_shard_size_flush(self.shard);
        }
        // The deadline pinned here is the flush-open deadline. It bounds how
        // long the flush may sit queued for a permit: the spawned task checks it
        // before acquiring and abandons a flush already past it without taking a
        // permit (issue #1739 part 2). The budget for the flush's own store
        // calls is re-derived from the moment the permit is granted (part 1), so
        // a flush that waited behind a stalled prefix does not spend its lifetime
        // in the queue and drop already-acked rows with no PUT.
        // ADR-1307 finding 4: it derives from the raw clock reading, not the
        // (possibly floor-raised) stamp, so absorbing a backwards step never
        // extends the budget.
        let deadline_ns =
            raw_ns.saturating_add(duration_nanos_saturating(self.config.max_flush_lifetime));

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
            input_bytes: est_bytes as u64,
            waiters,
            charges,
        };

        // ADR-0067 decision 2, amended for tenant isolation (issue #1292): the
        // `max_inflight_flushes` acquire runs INSIDE the spawned flush task, not
        // on the actor. Acquiring on the actor parked the whole `select!` loop at
        // the bound -- it stopped pulling channel messages, stopped the age-flush
        // tick, and stopped reaping finished flushes -- so one tenant whose
        // key prefix was throttled (S3 `503 SlowDown` is per-prefix) stalled
        // every co-resident tenant on the shard, including their age triggers.
        // Handing the acquire to the task keeps the actor draining and ticking no
        // matter how long a flush is stalled. Backpressure now propagates through
        // the ADR-0069 global byte budget: each flush's `charges` are held until
        // it completes, so a shard wedged on a throttled prefix drains the budget
        // and `try_charge` sheds at the ceiling, rather than parking the actor and
        // filling the bounded channel. ADR-1642 supersedes ADR-0067 decision 2
        // with exactly this.
        //
        // The in-flight gauge is incremented here, by the guard's constructor,
        // and decremented by its `Drop` inside the task: see
        // [`InFlightFlushGuard`] for why the two must be one value. A flush
        // still waiting for a permit counts as in flight, because it is holding
        // a flush window of memory.
        let guard = InFlightFlushGuard::new(Arc::clone(&self.metrics), self.shard);
        let slot = self.shares.enter(tenant_hash);
        let semaphore = Arc::clone(&self.semaphore);
        let ctx = Arc::clone(&self.ctx);
        // Per-shard skew (issue #865): the flush runs here off the actor
        // (ADR-0067). [`FlushCtx::run_flush`] brackets the flush proper
        // (`commit_flush`) on the injected clock and records the off-actor
        // figure itself, so the measurement stays out of the pinned-identity
        // path, captures every exit the flush takes (abandonment included), and
        // excludes the abandoned-key re-reads that follow it. The permit wait is
        // bracketed separately, before the flush's own span, so the two never
        // overlap.
        let clock = Arc::clone(&self.clock);
        let metrics = Arc::clone(&self.metrics);
        let scope = Arc::clone(&self.scope);
        let shard = self.shard;
        self.flushes.spawn(async move {
            let _guard = guard;
            let slot = slot;
            let mut pinned = pinned;
            // Issue #1739 part 2: a flush whose flush-open deadline already
            // elapsed while it sat in the spawn queue must not take a permit
            // only to fail the deadline check inside `run_flush` and waste the
            // slot. Check the pinned flush-open deadline here, before the
            // acquire; if it has passed, abandon in the queue with no permit
            // taken. In normal operation the deadline is a full
            // `max_flush_lifetime` ahead of flush-open, so this fires only when
            // this task is scheduled pathologically late.
            if ctx
                .bound_to_deadline(pinned.deadline_ns, std::future::ready(()))
                .await
                .is_none()
            {
                ctx.abandon_in_queue(pinned);
                return;
            }
            // Wait for a flush permit here, off the actor. At the bound this task
            // parks; the actor does not. The wait is this shard's
            // `flush_permit_wait_ns` (issue #865), measured on the injected clock
            // and no longer subtracted from any on-actor figure because it is no
            // longer on-actor time. The tenant's share permit is taken first
            // (ADR-2708 D3), so a backstop flush over the share waits on its own
            // tenant rather than on the shard's last permit.
            let mut first_grant_deadline_ns = None;
            let (_permit, grant) = loop {
                let permit_wait_start_ns = clock.now_ns();
                let permit = match slot.acquire(&semaphore).await {
                    Ok(permit) => permit,
                    Err(_) => {
                        panic!("ravel-ingest: flush semaphore closed unexpectedly on shard {shard}")
                    }
                };
                let permit_wait_ns =
                    clock.now_ns().saturating_sub(permit_wait_start_ns).max(0) as u64;
                metrics.record_shard_flush_permit_wait_ns(shard, permit_wait_ns);
                // Issue #1739 part 1: re-derive the abandonment deadline from the
                // moment the permit is granted, not from flush-open. A flush that
                // queued behind a stalled prefix must get its full
                // `max_flush_lifetime` for its own store calls; pinning at flush-open
                // spent that budget in the queue and dropped already-acked buffered
                // rows with no PUT. The re-derived value only ever moves the deadline
                // later (grant is at or after open), so it never shortens a flush's
                // store budget. A flush requeued for a stale view keeps the
                // deadline of its first grant.
                //
                // ADR-2708 D3 clamps it to the pinned hour's end plus
                // `max_flush_lifetime`, the bound maintain seals behind, and abandons
                // a flush granted past that bound without a PUT. The hour is not
                // re-pinned: the rows keep the bucket their flush opened in.
                pinned.deadline_ns = match first_grant_deadline_ns {
                    Some(deadline_ns) => deadline_ns,
                    None => match grant_deadline_ns(
                        clock.now_ns(),
                        pinned.ingest_hour_bucket,
                        ctx.config.max_flush_lifetime,
                    ) {
                        Some(deadline_ns) => deadline_ns,
                        None => {
                            ctx.abandon_hour_bound(pinned);
                            return;
                        }
                    },
                };
                first_grant_deadline_ns = Some(pinned.deadline_ns);
                // ADR-4233 decision 2: the version is chosen here, at grant, from
                // the activation policy's view as it stands now. A stale view
                // hands the permit back and asks again until the deadline; a
                // version other than the one checked against the floor at flush
                // open is checked again.
                match scope.compat_grant(pinned.tenant_hash, FormatFamily::Rseg) {
                    Ok(grant) => break (permit, grant),
                    Err(refused) if refused.is_view_stale() => {
                        drop(permit);
                        if !stale_view_wait(clock.as_ref(), pinned.deadline_ns).await {
                            ctx.abandon_compat(pinned, refused);
                            return;
                        }
                    }
                    Err(refused) => {
                        ctx.abandon_compat(pinned, refused);
                        return;
                    }
                }
            };
            if u32::from(grant.version()) != open_version {
                match scope.floor_check(
                    pinned.tenant_hash,
                    FormatFamily::Rseg,
                    u32::from(grant.version()),
                ) {
                    FloorCheck::Write | FloorCheck::NoView => {}
                    FloorCheck::WriteBelow(below) => {
                        warn_flush_written_below(ctx.signal, shard, pinned.tenant_hash, &below);
                    }
                    FloorCheck::Refuse(refused) => {
                        ctx.abandon_below_floor(pinned, refused);
                        return;
                    }
                }
            }
            ctx.run_flush(pinned, grant).await;
        });
        self.record_queued_flushes();
    }
}

#[cfg(test)]
mod adaptive_delay_tests {
    use super::{
        IngestConfig, adaptive_age_threshold_ns, duration_nanos_saturating, visibility_ceiling_ns,
    };

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
        let floor_ns = duration_nanos_saturating(cfg.max_flush_delay);
        let retry_headroom_ns = duration_nanos_saturating(cfg.put_retry_base_delay);
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

#[cfg(test)]
mod inflight_guard_tests {
    use super::*;

    /// A flush task spawned and then dropped before its first poll has to leave
    /// the gauge exactly where it found it. `JoinSet`'s `Drop` does that to
    /// every queued-but-unpolled task, which happens whenever the actor unwinds
    /// out of `handle_flush_join_result` or the router drops the actor.
    ///
    /// This is what forces the `+1` and the `-1` to live in one value: an
    /// increment written as the spawned task's first statement never runs for
    /// such a task, while dropping the task's future still fires the guard's
    /// `Drop`. The public reader clamps at 0, so the resulting negative bias is
    /// invisible there and persists across an actor respawn, which shares the
    /// same [`IngestMetrics`] for the shard index. The assertion reads the raw
    /// signed count for that reason.
    #[test]
    fn a_flush_task_dropped_before_its_first_poll_leaves_the_gauge_balanced() {
        let metrics = Arc::new(IngestMetrics::new(1));
        assert_eq!(metrics.in_flight_flushes_signed(0), 0);

        let guard = InFlightFlushGuard::new(Arc::clone(&metrics), 0);
        assert_eq!(
            metrics.in_flight_flushes_signed(0),
            1,
            "the buffer has left the actor, so the flush counts as in flight"
        );

        let flush_task = async move {
            let _guard = guard;
        };
        drop(flush_task);

        assert_eq!(
            metrics.in_flight_flushes_signed(0),
            0,
            "the guard's own Drop ran, so the pair balanced"
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::pin::Pin;

    use ravel_commit::record;
    use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, list_all};
    use ravel_otlp::normalize::NormalizedPoint;
    use ravel_types::METRIC_NAME_LABEL;
    use tokio::sync::watch;

    use crate::compat_get_store::{CompatGet, CompatGetStore};

    use super::*;
    use crate::budget::{IngestByteBudget, IngestByteBudgetLimit};
    use crate::config::NS_PER_HOUR;
    use crate::generation::ScriptedRead;
    use crate::router::{IngestRouter, WriteMode};
    use ravel_catalog::{
        DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
        DEFAULT_MAX_FLUSH_LIFETIME_NS, FLUSH_BOUND_SLACK_HOURS,
    };

    /// An exact ingest-hour boundary (unix hour 472223), and a base one second
    /// before it. Every fixture below routes its records in the hour that ends
    /// at `BOUNDARY_NS` and then crosses it during the deferral, so "the bucket
    /// the trigger fired in" and "the bucket the flush opened in" are different
    /// numbers rather than the same one read twice.
    const BOUNDARY_NS: i64 = 472_223 * NS_PER_HOUR;
    const BASE_NS: i64 = BOUNDARY_NS - 1_000_000_000;

    /// Past `max_flush_delay` (50 ms) on every tick.
    const TICK_ADVANCE_NS: i64 = 100_000_000;

    /// Deterministic injected clock; the shard actor's flush tick sleeps on it.
    /// Restated here because a unit test cannot import the integration-test
    /// harness (mirrors `log_shard`'s and `tests/common`'s).
    struct TestClock {
        now_ns: AtomicI64,
        wake_tx: watch::Sender<()>,
    }

    impl TestClock {
        fn new(start_ns: i64) -> Arc<Self> {
            let (wake_tx, _rx) = watch::channel(());
            Arc::new(TestClock {
                now_ns: AtomicI64::new(start_ns),
                wake_tx,
            })
        }

        fn advance_ns(&self, delta_ns: i64) {
            self.now_ns.fetch_add(delta_ns, Ordering::SeqCst);
            let _ = self.wake_tx.send(());
        }
    }

    impl Clock for TestClock {
        fn now_ns(&self) -> i64 {
            self.now_ns.load(Ordering::SeqCst)
        }

        fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            let deadline = self
                .now_ns()
                .saturating_add(i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX));
            let mut rx = self.wake_tx.subscribe();
            Box::pin(async move {
                loop {
                    if self.now_ns() >= deadline {
                        return;
                    }
                    if rx.changed().await.is_err() {
                        return;
                    }
                }
            })
        }
    }

    /// Yields until `probe` is true. Every caller's `probe` reads a metric the
    /// shard actor publishes, so this is a cooperative wait rather than a timed
    /// one; a probe that never becomes true hangs, which the runner reports
    /// under this test's own name.
    async fn until(mut probe: impl FnMut() -> bool) {
        while !probe() {
            tokio::task::yield_now().await;
        }
    }

    /// Like [`until`], but gives up after a fixed number of scheduler turns and
    /// reports whether `probe` came true, for a probe the code under test may
    /// rightly leave false: an assertion on the result fails where `until`
    /// would hang. Bounded by yields, not time.
    async fn settles(mut probe: impl FnMut() -> bool) -> bool {
        for _ in 0..10_000 {
            if probe() {
                return true;
            }
            tokio::task::yield_now().await;
        }
        probe()
    }

    fn one_permit_one_queue(max_flush_lifetime: Duration) -> IngestConfig {
        IngestConfig {
            shard_count: 1,
            // Only the age trigger can ever fire.
            target_bytes: 8 * 1024 * 1024,
            max_flush_delay: Duration::from_millis(50),
            flush_tick: Duration::from_millis(10),
            // One permit and one queue slot: the first flush parks at the held
            // PUT, and the very next trigger is at the cap.
            max_inflight_flushes: 1,
            max_queued_flushes: 1,
            // Long enough that the injected deferral below never reaches the
            // parked flush's own deadline: this fixture is about the pin, and
            // the abandonment path must not compete for the evidence.
            max_flush_lifetime,
            put_retry_base_delay: Duration::from_millis(1),
            put_retry_max_delay: Duration::from_millis(5),
            ..IngestConfig::default()
        }
    }

    /// Room for `tenants + 1` parked flushes and one more, a flush share of
    /// two so a tenant with one flush parked is under its share, and a
    /// queued-flush cap the parked flushes fill exactly: the cap, not the
    /// share, then decides which deferred buffer a freed slot goes to.
    fn queue_order(tenants: usize) -> IngestConfig {
        IngestConfig {
            max_inflight_flushes: (tenants + 2) as u32,
            max_inflight_flushes_per_tenant: Some(2),
            max_queued_flushes: tenants + 1,
            ..one_permit_one_queue(Duration::from_secs(3600))
        }
    }

    /// Holds `tenant`'s first PUT, which is its first flush's data object.
    fn hold_first_put(store: &FaultStore<MemoryStore>, tenant: &TenantId) -> GateHandle {
        store.hold(
            Op::Put,
            Some(format!("t/{}/", tenant.hash().to_hex())),
            Occurrence::Nth(1),
        )
    }

    /// Releases `tenant`'s held PUT, leaving every other held call parked.
    fn release_tenant(gate: &GateHandle, tenant: &TenantId) {
        let hex = tenant.hash().to_hex();
        let (id, _, _) = gate
            .held_details()
            .into_iter()
            .find(|(_, _, key)| key.contains(&hex))
            .expect("the tenant's PUT is held");
        assert!(gate.release(id));
    }

    fn point(tenant: &TenantId, host: &str) -> NormalizedPoint {
        let labels = LabelSet::new(vec![
            Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: "cpu_usage".to_string(),
            },
            Label {
                name: "host".to_string(),
                value: host.to_string(),
            },
        ])
        .expect("distinct label names");
        let series_id = SeriesId::compute(tenant, "cpu_usage", &labels).expect("series id");
        NormalizedPoint {
            series_id,
            labels: Arc::new(labels),
            sample: Sample {
                ts_ns: 1_000,
                value: 1.0,
            },
            is_monotonic_sum: false,
        }
    }

    /// What one deferred flush produced: when its record was routed, and
    /// either the commit record the deferred write was acknowledged with or
    /// the error it was answered with instead.
    struct DeferredFlush {
        routed_ns: i64,
        outcome: Result<CommitRecord, WriteError>,
    }

    impl DeferredFlush {
        fn record(self) -> CommitRecord {
            self.outcome
                .unwrap_or_else(|e| panic!("the deferred write was refused: {e}"))
        }
    }

    /// Drives exactly one deferred flush and returns its evidence.
    ///
    /// One tenant, one shard, one permit, one queue slot. The first write's
    /// flush parks at a held data PUT and occupies both; the second write's
    /// trigger is therefore refused. The clock then jumps `deferral_ns`,
    /// crossing at least one hour boundary, and the refusal repeats on the
    /// tick that jump fires, so the buffer is deferred across more than one
    /// round. Releasing the gate drains the parked flush and the next tick
    /// finally opens the deferred one, in a later ingest hour than the trigger
    /// fired in. `lifetime` must exceed the deferral so the parked flush's own
    /// deadline never competes. When the deferral reaches the deferral cap that
    /// `lifetime` leaves, the deferred write is answered with the
    /// outcome-unknown `Abandoned` rather than acknowledged.
    async fn drive_one_deferred_flush(deferral_ns: i64, lifetime: Duration) -> DeferredFlush {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        assert!(
            i64::try_from(lifetime.as_nanos()).expect("lifetime fits i64") > deferral_ns,
            "the parked flush must outlive the deferral"
        );
        let router = Arc::new(
            IngestRouter::new(
                one_permit_one_queue(lifetime),
                Arc::clone(&store),
                Signal::Metrics,
                clock.clone(),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
            .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)),
        );
        let acme = TenantId::new("acme");

        // `Nth(1)` so releasing this one call lets the parked flush drain
        // rather than re-holding its retry.
        let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

        let write = |host: &'static str| {
            let router = Arc::clone(&router);
            let tenant = acme.clone();
            tokio::spawn(async move {
                let points = vec![point(&tenant, host)];
                router
                    .write(tenant, points, WriteMode::Strict, Duration::from_secs(60))
                    .await
            })
        };

        // The first flush takes the permit and parks in the store.
        let parked = write("h0");
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| in_flight(&router) == 1).await;
        gate.wait_until_held(1).await;

        // The second write is routed here, and nothing advances the clock
        // between this reading and the row reaching the buffer.
        let routed_ns = clock.now_ns();
        let deferred = write("h1");
        until(|| router.metrics().snapshot().buffered_points_total >= 2).await;

        // Round one: the trigger fires and is refused, which starts the
        // deferral. No bucket is pinned here.
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| deferred_triggers(&router) >= 1 || in_flight(&router) > 1).await;
        assert_eq!(
            in_flight(&router),
            1,
            "the trigger past the cap must be deferred, not spawned"
        );

        // The deferral itself, crossing at least one ingest-hour boundary.
        // Round two: the tick this jump fires finds the shard still at its
        // queued-flush cap and refuses again, so the buffer is deferred across
        // more than one round and keeps the deferral start of round one.
        clock.advance_ns(deferral_ns);
        until(|| deferred_triggers(&router) >= 2 || in_flight(&router) > 1).await;
        assert_eq!(
            in_flight(&router),
            1,
            "the second round is deferred too, so the flush below is the first \
             one this buffer opens"
        );

        // Drain the parked flush, freeing both the permit and the queue slot.
        for id in gate.held() {
            assert!(gate.release(id));
        }
        parked
            .await
            .expect("parked write task")
            .expect("parked write acks once the gate is released");
        until(|| in_flight(&router) == 0 && queued_gauge(&router) == 0).await;

        // One more tick: a deferred buffer is always due, so it opens now.
        clock.advance_ns(TICK_ADVANCE_NS);
        let outcome = match deferred.await.expect("deferred write task") {
            Ok(receipt) => {
                assert_eq!(receipt.tokens.len(), 1);
                let token = receipt.tokens[0].clone();
                let commit_key = keys::commit_key_for_token(&acme.hash(), Signal::Metrics, &token)
                    .expect("commit key");
                let bytes = store
                    .get(&commit_key, GetRange::Full)
                    .await
                    .expect("get commit record")
                    .data;
                let decoded = record::decode(&bytes).expect("decode commit record");
                assert_eq!(
                    decoded.ingest_hour_bucket, token.ingest_hour_bucket,
                    "the token and the object it points at must name one ingest hour"
                );
                Ok(decoded)
            }
            Err(e) => Err(e),
        };

        router.flush_all().await;
        DeferredFlush { routed_ns, outcome }
    }

    fn in_flight(router: &IngestRouter) -> u64 {
        router
            .metrics()
            .in_flight_flushes_by_shard()
            .into_iter()
            .map(|(_, n)| n)
            .sum()
    }

    fn queued_gauge(router: &IngestRouter) -> u64 {
        router
            .metrics()
            .shard_skew_by_shard()
            .into_iter()
            .map(|(_, s)| s.flushes_queued)
            .sum()
    }

    fn deferred_triggers(router: &IngestRouter) -> u64 {
        router
            .metrics()
            .shard_skew_by_shard()
            .into_iter()
            .map(|(_, s)| s.flush_trigger_deferred)
            .sum()
    }

    fn hour_of(ns: i64) -> u32 {
        u32::try_from(ns / NS_PER_HOUR).expect("plausible ingest clock fits a u32 hour")
    }

    /// Where a deferred flush's ingest-hour bucket comes from: the reading
    /// taken by the flush that finally opens, not the one the refused trigger
    /// fired on.
    ///
    /// The deferral here runs from one second before an ingest-hour boundary
    /// to well past it, so those are two different hours rather than the same
    /// one read twice, and the assertion can tell them apart. The deferral's
    /// own length therefore lands in the gap
    /// `ravel_catalog::FLUSH_BOUND_SLACK_HOURS` bounds, and what keeps it
    /// inside that slack is the deferral cap (issue #1916), which the next
    /// test holds. Two seconds is well inside the cap, so this write is
    /// acknowledged.
    #[tokio::test]
    async fn a_deferred_flush_takes_the_ingest_hour_it_opened_in() {
        // Two seconds spans the boundary one second ahead of `BASE_NS`.
        let flush = drive_one_deferred_flush(2_000_000_000, Duration::from_secs(8)).await;

        let routed_ns = flush.routed_ns;
        let record = flush.record();
        let fired_hour = hour_of(routed_ns);
        let created_hour = hour_of(record.created_unix_ns);
        assert_eq!(
            fired_hour,
            hour_of(BASE_NS),
            "the trigger fired in the hour that ends at BOUNDARY_NS"
        );
        assert_eq!(
            created_hour,
            fired_hour + 1,
            "the deferral crossed an ingest-hour boundary, so the flush opened \
             in the next hour: without this the assertion below would hold for \
             a run that never left one hour and would prove nothing"
        );
        assert_eq!(
            record.ingest_hour_bucket, created_hour,
            "a deferred flush pins the ingest hour it opened in \
             ({created_hour}), not the one its trigger fired in ({fired_hour})"
        );
    }

    /// The read-side scan slack (`ravel_catalog::FLUSH_BOUND_SLACK_HOURS`,
    /// ADR-0052 section 3) is meant to cover the whole span between a record
    /// being routed and the ingest hour its flush pins, plus the flush's own
    /// lifetime. A queued-flush cap deferral adds its own length to that span,
    /// and an unbounded one acknowledged a strict write with a bucket hours
    /// past its routing. This drives such a deferral on a live shard actor and
    /// asserts the write is not acknowledged: its waiter gets the
    /// outcome-unknown `Abandoned`, so no acknowledged write carries a pin past
    /// the slack.
    ///
    /// The fixture's 90 minute lifetime leaves a nonzero deferral cap, and the
    /// same fixture with a deferral just inside that cap is acknowledged, with
    /// its routing-to-pin span plus the lifetime inside the slack. A cap
    /// forced to 0 answers that second write too, so this test fails on it.
    ///
    /// Behavioural on purpose. An arithmetic-only guard restates the terms
    /// someone believed the code produces and passes whether or not a deferral
    /// moved the pin, which is how this gap shipped under a green test in the
    /// first place.
    ///
    /// Neither one-line fix would have closed it.
    /// `FLUSH_BOUND_SLACK_HOURS` is a frozen read-side contract
    /// (`DEFAULT_SCAN_SLACK_HOURS`, ADR-0052 section 3), and raising it does
    /// not bound an unbounded number of rounds anyway; pinning the bucket
    /// before the cap check moves the overrun past the catalog's sealed-hour
    /// watermark instead, which
    /// `carrying_a_pre_deferral_pin_would_write_into_a_sealed_hour` shows is
    /// the worse of the two.
    #[tokio::test]
    async fn a_deferred_flush_is_never_acked_past_the_flush_bound_slack() {
        let slack_ns = i64::from(FLUSH_BOUND_SLACK_HOURS) * NS_PER_HOUR;
        let lifetime_ns = 90 * 60 * 1_000_000_000_i64;
        let lifetime = Duration::from_nanos(u64::try_from(lifetime_ns).expect("positive"));
        let config = one_permit_one_queue(lifetime);
        // 7200 s less the 5400 s lifetime less the 40 s idle delay and one
        // 10 ms tick.
        let cap_ns = config.flush_deferral_cap_ns();
        assert_eq!(cap_ns, 1_759_990_000_000);

        // One hour of deferral: past the cap, inside the parked flush's own
        // lifetime, and with the lifetime the deferred flush then gets, past
        // the slack.
        let deferral_ns = NS_PER_HOUR;
        assert!(deferral_ns > cap_ns && deferral_ns < lifetime_ns);
        assert!(
            deferral_ns + lifetime_ns > slack_ns,
            "the fixture must out-run the slack, or it cannot show that a \
             deferral past it is not acknowledged"
        );
        let flush = drive_one_deferred_flush(deferral_ns, lifetime).await;
        let routed_hour = hour_of(flush.routed_ns);
        match flush.outcome {
            Err(WriteError::Abandoned(msg)) => assert!(
                msg.contains("deferral cap"),
                "the outcome-unknown answer names the deferral cap: {msg}"
            ),
            Ok(record) => panic!(
                "a record routed in hour {routed_hour} was acknowledged in ingest \
                 hour {} with a {lifetime_ns}ns lifetime still ahead of it, past \
                 the {FLUSH_BOUND_SLACK_HOURS} hours FLUSH_BOUND_SLACK_HOURS \
                 covers: the deferral cap did not withhold the ack",
                record.ingest_hour_bucket,
            ),
            Err(other) => panic!("expected the outcome-unknown Abandoned, got {other}"),
        }

        // Just inside the cap: the flush opens one `TICK_ADVANCE_NS` after the
        // second round and the deferral started one before the jump, so this
        // opens 100 ms short of the cap and is acknowledged.
        let within_ns = cap_ns - 2 * TICK_ADVANCE_NS;
        let flush = drive_one_deferred_flush(within_ns, lifetime).await;
        let routed_ns = flush.routed_ns;
        let record = flush.record();
        let span_ns = record.created_unix_ns - routed_ns;
        assert!(
            span_ns + lifetime_ns <= slack_ns,
            "an acknowledged deferred write's routing-to-pin span ({span_ns}ns) \
             plus max_flush_lifetime ({lifetime_ns}ns) must fit inside \
             FLUSH_BOUND_SLACK_HOURS ({slack_ns}ns)"
        );

        // The derivation at the shipped defaults: a row's routing-to-pin bound
        // outside the sub-floor hold is the idle clock plus one tick, and the
        // deferral cap is exactly what the slack leaves after that and the
        // lifetime it reserves.
        let shipped = IngestConfig::default();
        let shipped_lifetime_ns = shipped.max_flush_lifetime.as_nanos() as i64;
        let tick_ns = shipped.effective_flush_tick().as_nanos() as i64;
        let bound_ns = shipped.flush_trigger_age_bound_ns();
        assert_eq!(
            bound_ns,
            shipped.max_flush_delay_idle.as_nanos() as i64 + tick_ns,
            "an idle buffered row waits out the idle clock, the slowest trigger \
             outside the sub-floor hold"
        );
        assert_eq!(
            bound_ns + shipped.flush_deferral_cap_ns() + shipped_lifetime_ns,
            slack_ns,
            "the deferral cap is exactly what the slack leaves after the \
             routing-to-pin bound ({bound_ns}ns) and the lifetime it reserves"
        );
        // A buffered row with no deferral is the case the constant itself was
        // derived for: the sub-floor hold plus the tick the age check may take
        // to notice it is the worst buffer age at flush open (ADR-1737
        // decision 3 as amended), and with the lifetime it fits the slack.
        let hold_ns = sub_floor_hold_ns(&shipped);
        assert!(
            hold_ns + tick_ns <= shipped_lifetime_ns,
            "the sub-floor hold ({hold_ns}ns) plus one {tick_ns}ns tick must not \
             exceed max_flush_lifetime ({shipped_lifetime_ns}ns)"
        );
        assert!(
            hold_ns + tick_ns + shipped_lifetime_ns <= slack_ns,
            "the sub-floor hold ({hold_ns}ns) plus one {tick_ns}ns tick plus \
             max_flush_lifetime ({shipped_lifetime_ns}ns) must fit inside \
             FLUSH_BOUND_SLACK_HOURS ({slack_ns}ns)"
        );
    }

    /// A shard whose oldest deferred flush reaches the deferral cap refuses
    /// every new write before enqueue with `DeferralCapReached`, from any
    /// tenant and in both write modes, answers the strict waiter already on the
    /// capped buffer with the outcome-unknown `Abandoned`, and accepts again
    /// once its queue drains and the deferred flush opens. The cap here is the
    /// one a 4000 s lifetime leaves (7200 s less 4000 s less the 40 s idle
    /// delay and one 10 ms tick), reached by a single clock jump that stays
    /// inside the parked flush's own 4000 s lifetime, so the abandonment path
    /// never frees the slot.
    #[tokio::test]
    async fn a_shard_at_the_deferral_cap_refuses_appends_until_it_drains() {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        let config = one_permit_one_queue(Duration::from_secs(4000));
        let cap_ns = config.flush_deferral_cap_ns();
        assert_eq!(cap_ns, 3_159_990_000_000);
        let router = Arc::new(
            IngestRouter::new(
                config,
                Arc::clone(&store),
                Signal::Metrics,
                clock.clone(),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
            .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)),
        );
        let acme = TenantId::new("acme");
        let globex = TenantId::new("globex");
        let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
        let write = |tenant: &TenantId, host: &'static str, mode: WriteMode| {
            let router = Arc::clone(&router);
            let tenant = tenant.clone();
            tokio::spawn(async move {
                let points = vec![point(&tenant, host)];
                router
                    .write(tenant, points, mode, Duration::from_secs(60))
                    .await
            })
        };
        let buffered = || router.metrics().snapshot().buffered_points_total;
        let refusals = || router.metrics().snapshot().deferral_cap_refused;
        let enqueued = || -> u64 {
            router
                .metrics()
                .shard_skew_by_shard()
                .into_iter()
                .map(|(_, s)| s.messages_enqueued)
                .sum()
        };

        let parked = write(&acme, "h0", WriteMode::Strict);
        until(|| buffered() >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| in_flight(&router) == 1).await;
        gate.wait_until_held(1).await;

        // The first refusal starts the deferral.
        let deferred = write(&acme, "h1", WriteMode::Strict);
        until(|| buffered() >= 2).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| deferred_triggers(&router) >= 1).await;
        assert!(
            !deferred.is_finished(),
            "below the cap the write still waits"
        );

        // One jump to the cap: the next tick finds the deferral at the cap and
        // withholds the deferred write's ack. Its row stays buffered, so the
        // answer is outcome-unknown, not a refusal.
        clock.advance_ns(cap_ns);
        until(|| deferred_triggers(&router) >= 2).await;
        let stripped = deferred.await.expect("deferred write task");
        assert!(
            matches!(&stripped, Err(WriteError::Abandoned(msg)) if msg.contains("deferral cap")),
            "the deferred write is answered outcome-unknown at the cap, got {stripped:?}"
        );
        assert_eq!(refusals(), 0, "a withheld ack is not a refused write");

        // At the cap the whole shard refuses, not only the deferred tenant, in
        // both modes, and before enqueue: nothing reaches the shard's channel
        // and nothing is buffered.
        let (rows_before, enqueued_before) = (buffered(), enqueued());
        let shed = write(&globex, "g0", WriteMode::Strict)
            .await
            .expect("globex write task");
        assert!(
            matches!(shed, Err(WriteError::DeferralCapReached)),
            "a shard at the deferral cap refuses a strict write, got {shed:?}"
        );
        let shed = write(&globex, "g1", WriteMode::Buffered)
            .await
            .expect("globex write task");
        assert!(
            matches!(shed, Err(WriteError::DeferralCapReached)),
            "a shard at the deferral cap refuses a buffered write, got {shed:?}"
        );
        assert!(WriteError::DeferralCapReached.is_retryable());
        assert_eq!(buffered(), rows_before, "a refused write buffers nothing");
        assert_eq!(enqueued(), enqueued_before, "refused before enqueue");
        assert_eq!(refusals(), 2);

        // Drain: the parked flush lands, the next tick opens the deferred one.
        for id in gate.held() {
            assert!(gate.release(id));
        }
        parked
            .await
            .expect("parked write task")
            .expect("parked write acks once the gate is released");
        until(|| in_flight(&router) == 0 && queued_gauge(&router) == 0).await;
        let flushes_before = router.metrics().snapshot().flushes_by_age;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| {
            router.metrics().snapshot().flushes_by_age > flushes_before
                && in_flight(&router) == 0
                && queued_gauge(&router) == 0
        })
        .await;

        // Accepting again, in both modes.
        let receipt = write(&globex, "g2", WriteMode::Buffered)
            .await
            .expect("buffered write task")
            .expect("the shard accepts a buffered write once its deferred flush opened");
        assert!(receipt.tokens.is_empty());
        until(|| buffered() == rows_before + 1).await;
        let accepted = write(&globex, "g3", WriteMode::Strict);
        until(|| accepted.is_finished() || buffered() > rows_before + 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        let receipt = accepted
            .await
            .expect("accepted write task")
            .expect("the shard accepts again once its deferred flush opened");
        assert_eq!(receipt.tokens.len(), 1);
        assert_eq!(refusals(), 2);
        router.flush_all().await;
    }

    /// The cap boundary on a live actor: a strict write that arrives one
    /// `effective_flush_tick()` before the oldest deferral reaches the cap is accepted and
    /// buffered, and one that arrives at the cap is refused. The first write
    /// reaches the shard actor, so it fails if `refuse_at_deferral_cap` refuses
    /// whenever any buffer is deferred, whatever the cap.
    #[tokio::test]
    async fn a_write_one_tick_before_the_deferral_cap_is_accepted_and_one_at_it_refused() {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        // The parked flush's 4000 s lifetime outlasts the jump to the cap.
        let config = one_permit_one_queue(Duration::from_secs(4000));
        let cap_ns = config.flush_deferral_cap_ns();
        let tick_ns = config.effective_flush_tick().as_nanos() as i64;
        let router = Arc::new(
            IngestRouter::new(
                config,
                Arc::clone(&store),
                Signal::Metrics,
                clock.clone(),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
            .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)),
        );
        let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
        let write = |tenant: &'static str| {
            let router = Arc::clone(&router);
            let tenant = TenantId::new(tenant);
            tokio::spawn(async move {
                let points = vec![point(&tenant, "h")];
                router
                    .write(tenant, points, WriteMode::Strict, Duration::from_secs(60))
                    .await
            })
        };
        let buffered = || router.metrics().snapshot().buffered_points_total;

        // acme parks its own first flush, so its next trigger is deferred at
        // its flush share (ADR-2708 D3).
        let parked = write("acme");
        until(|| buffered() >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| in_flight(&router) == 1).await;
        gate.wait_until_held(1).await;

        // The deferral starts on this tick.
        let deferred = write("acme");
        until(|| buffered() >= 2).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| deferred_triggers(&router) >= 1).await;

        // One tick short of the cap. The tick this jump fires refuses the
        // deferred buffer again, which also proves the actor is past it.
        clock.advance_ns(cap_ns - tick_ns);
        until(|| deferred_triggers(&router) >= 2).await;
        let early = write("globex");
        until(|| buffered() >= 3 || early.is_finished()).await;
        assert!(
            !early.is_finished(),
            "a strict write one tick before the cap is accepted and waits for \
             its flush, not answered at arrival"
        );
        assert_eq!(buffered(), 3, "the accepted write is buffered");

        // At the cap.
        clock.advance_ns(tick_ns);
        until(|| deferred_triggers(&router) >= 3).await;
        let refused = write("initech").await.expect("initech write task");
        assert!(
            matches!(refused, Err(WriteError::DeferralCapReached)),
            "a strict write at the cap is refused, got {refused:?}"
        );
        assert_eq!(buffered(), 3, "the refused write buffers nothing");

        for id in gate.held() {
            assert!(gate.release(id));
        }
        parked
            .await
            .expect("parked write task")
            .expect("parked write acks once the gate is released");
        assert!(matches!(
            deferred.await.expect("deferred write task"),
            Err(WriteError::Abandoned(_))
        ));
        // The deferred buffer opens on the first tick after the drain and
        // takes the slot; globex, refused on that tick, opens on a later one.
        let mut opened = false;
        for _ in 0..2 {
            until(|| in_flight(&router) == 0 && queued_gauge(&router) == 0).await;
            clock.advance_ns(TICK_ADVANCE_NS);
            if settles(|| early.is_finished()).await {
                opened = true;
                break;
            }
        }
        assert!(opened, "globex's flush opens within two ticks of the drain");
        let receipt = early
            .await
            .expect("globex write task")
            .expect("the write accepted before the cap is acknowledged");
        assert_eq!(receipt.tokens.len(), 1);
        router.flush_all().await;
    }

    /// A deferred buffer is due on the next tick even when it is below its own
    /// age threshold. Here a size trigger on a buffered-mode buffer is refused:
    /// with no strict waiter and less than `min_flush_bytes` of object its age
    /// threshold is the 40 s idle clock, so only its deferral makes it due on
    /// the first tick after the queue drains.
    #[tokio::test]
    async fn a_deferred_buffer_below_its_age_threshold_is_still_due() {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        // Every write fires the size trigger, well under min_flush_bytes and
        // under the object backstop's four targets, which would exempt it.
        let probe = point(&TenantId::new("idle"), "h");
        let one_point_object_bytes = crate::value::SCALAR_SAMPLE_OBJECT_BYTES
            + crate::value::SERIES_OBJECT_OVERHEAD_BYTES
            + probe
                .labels
                .iter()
                .map(|l| l.name.len() + l.value.len())
                .sum::<usize>();
        let config = IngestConfig {
            target_bytes: one_point_object_bytes / 2,
            ..one_permit_one_queue(Duration::from_secs(3600))
        };
        assert!(config.max_flush_delay_idle >= Duration::from_secs(40));
        let router = Arc::new(
            IngestRouter::new(
                config,
                Arc::clone(&store),
                Signal::Metrics,
                clock.clone(),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
            .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)),
        );
        let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
        let parked = {
            let router = Arc::clone(&router);
            tokio::spawn(async move {
                // The same tenant as the buffered write below, so the share
                // (ADR-2708 D3) refuses that write's trigger.
                let tenant = TenantId::new("idle");
                let points = vec![point(&tenant, "h")];
                router
                    .write(tenant, points, WriteMode::Strict, Duration::from_secs(60))
                    .await
            })
        };
        until(|| in_flight(&router) == 1).await;
        gate.wait_until_held(1).await;

        let tenant = TenantId::new("idle");
        let points = vec![point(&tenant, "h")];
        router
            .write(tenant, points, WriteMode::Buffered, Duration::from_secs(60))
            .await
            .expect("buffered write acks at enqueue");
        until(|| deferred_triggers(&router) >= 1).await;

        for id in gate.held() {
            assert!(gate.release(id));
        }
        parked
            .await
            .expect("parked write task")
            .expect("parked write acks once the gate is released");
        until(|| in_flight(&router) == 0 && queued_gauge(&router) == 0).await;

        let flushes_before = router.metrics().snapshot().flushes_by_age;
        clock.advance_ns(TICK_ADVANCE_NS);
        assert!(
            settles(|| router.metrics().snapshot().flushes_by_age > flushes_before).await,
            "the deferred buffer, {TICK_ADVANCE_NS}ns old against a 40 s idle \
             threshold, must open on the first tick after the drain"
        );
        router.flush_all().await;
    }

    /// Oldest deferral first, with a fixture where deferral order and row
    /// arrival order disagree. Tenant `early` buffers a row first, in buffered
    /// mode, so it waits for the 40 s idle clock; tenant `late` arrives after
    /// it with a strict waiter, so its trigger fires and is refused first. Once
    /// `parked` drains, the single slot must go to `late`, the older
    /// deferral, which sorting by oldest row would give to `early`.
    #[tokio::test]
    async fn deferred_flushes_retry_by_deferral_age_not_row_age() {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        let config = queue_order(2);
        let idle_ns = config.max_flush_delay_idle.as_nanos() as i64;
        let router = Arc::new(
            IngestRouter::new(
                config,
                Arc::clone(&store),
                Signal::Metrics,
                clock.clone(),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
            .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)),
        );
        let write = |tenant: &'static str, host: &'static str, mode: WriteMode| {
            let router = Arc::clone(&router);
            let tenant = TenantId::new(tenant);
            tokio::spawn(async move {
                let points = vec![point(&tenant, host)];
                router
                    .write(tenant, points, mode, Duration::from_secs(60))
                    .await
            })
        };
        let buffered = || router.metrics().snapshot().buffered_points_total;

        // `parked`, `early` and `late` each park one flush, filling the
        // queued-flush cap with `early` and `late` under their share.
        let parked_tenant = TenantId::new("parked");
        let gate = hold_first_put(&fault_store, &parked_tenant);
        let _gates = [
            hold_first_put(&fault_store, &TenantId::new("early")),
            hold_first_put(&fault_store, &TenantId::new("late")),
        ];
        let parked = write("parked", "h", WriteMode::Strict);
        let firsts = [
            write("early", "first", WriteMode::Strict),
            write("late", "first", WriteMode::Strict),
        ];
        until(|| buffered() >= 3).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| in_flight(&router) == 3).await;
        gate.wait_until_held(3).await;

        // `early`'s row arrives first; `late`'s one millisecond after it,
        // short of the next tick.
        write("early", "h", WriteMode::Buffered)
            .await
            .expect("early write task")
            .expect("buffered write acks at enqueue");
        until(|| buffered() >= 4).await;
        clock.advance_ns(1_000_000);
        let late = write("late", "h", WriteMode::Strict);
        until(|| buffered() >= 5).await;

        // `late` is past its 50 ms strict threshold and is refused first.
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| deferred_triggers(&router) >= 1).await;
        // `early` reaches the idle clock 40 s later and is refused then.
        clock.advance_ns(idle_ns);
        until(|| deferred_triggers(&router) >= 3).await;

        release_tenant(&gate, &parked_tenant);
        parked
            .await
            .expect("parked write task")
            .expect("parked write acks once the gate is released");
        until(|| in_flight(&router) == 2).await;

        clock.advance_ns(TICK_ADVANCE_NS);
        assert!(
            settles(|| late.is_finished()).await,
            "the first tick after the drain must open `late`, deferred 40 s \
             before `early` though its row arrived after `early`'s"
        );
        late.await
            .expect("late write task")
            .expect("late acks from the flush it got the slot for");
        for id in gate.held() {
            assert!(gate.release(id));
        }
        for w in firsts {
            w.await
                .expect("first write task")
                .expect("first write acks");
        }
        router.flush_all().await;
    }

    /// Deferred flushes retry oldest deferral first. Eight tenants are
    /// deferred one tick apart at the queued-flush cap; once `parked` drains,
    /// each tick frees exactly one slot, and the tenant that gets it must be
    /// the one deferred earliest. `HashMap` order would match this sequence
    /// by chance once in 8! runs.
    #[tokio::test]
    async fn deferred_flushes_retry_oldest_first() {
        const TENANTS: usize = 8;
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        let router = Arc::new(
            IngestRouter::new(
                queue_order(TENANTS),
                Arc::clone(&store),
                Signal::Metrics,
                clock.clone(),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
            .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)),
        );
        let write = |tenant: TenantId, host: &'static str| {
            let router = Arc::clone(&router);
            tokio::spawn(async move {
                let points = vec![point(&tenant, host)];
                router
                    .write(tenant, points, WriteMode::Strict, Duration::from_secs(60))
                    .await
            })
        };
        let buffered = || router.metrics().snapshot().buffered_points_total;

        // `parked` and every t_i park one flush, filling the queued-flush cap
        // with every t_i under its share, so the cap orders the retries.
        let parked_tenant = TenantId::new("parked");
        let tenants: Vec<TenantId> = (0..TENANTS)
            .map(|i| TenantId::new(format!("t{i}")))
            .collect();
        let gate = hold_first_put(&fault_store, &parked_tenant);
        let _gates: Vec<GateHandle> = tenants
            .iter()
            .map(|t| hold_first_put(&fault_store, t))
            .collect();
        let parked = write(parked_tenant.clone(), "h");
        let firsts: Vec<_> = tenants.iter().map(|t| write(t.clone(), "first")).collect();
        until(|| buffered() >= (TENANTS + 1) as u64).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| in_flight(&router) == (TENANTS + 1) as u64).await;
        gate.wait_until_held(TENANTS + 1).await;

        // Tenant i is first refused on tick i, so tick i refuses i + 1
        // buffers and the deferral start times are strictly increasing in i.
        let mut writes = Vec::new();
        let mut refusals = 0u64;
        for (i, t) in tenants.iter().enumerate() {
            writes.push(write(t.clone(), "h"));
            let want = (TENANTS + 2 + i) as u64;
            until(|| buffered() >= want).await;
            clock.advance_ns(TICK_ADVANCE_NS);
            refusals += (i + 1) as u64;
            until(|| deferred_triggers(&router) >= refusals).await;
        }
        assert!(writes.iter().all(|w| !w.is_finished()));

        // Freeing the slot `parked` holds lets one deferred flush open per
        // tick; each opened flush acks and frees it again.
        release_tenant(&gate, &parked_tenant);
        parked
            .await
            .expect("parked write task")
            .expect("parked write acks once the gate is released");
        until(|| in_flight(&router) == TENANTS as u64).await;

        for step in 0..TENANTS {
            clock.advance_ns(TICK_ADVANCE_NS);
            until(|| writes.iter().filter(|w| w.is_finished()).count() > step).await;
            let finished: Vec<usize> = (0..TENANTS).filter(|&i| writes[i].is_finished()).collect();
            assert_eq!(
                finished,
                (0..=step).collect::<Vec<_>>(),
                "tick {step} after the drain must open tenant t{step}, the oldest \
                 deferral left"
            );
            until(|| in_flight(&router) == TENANTS as u64).await;
        }
        for (i, w) in writes.into_iter().enumerate() {
            w.await
                .expect("write task")
                .unwrap_or_else(|e| panic!("t{i} acks: {e}"));
        }
        for id in gate.held() {
            assert!(gate.release(id));
        }
        for (i, w) in firsts.into_iter().enumerate() {
            w.await
                .expect("first write task")
                .unwrap_or_else(|e| panic!("t{i}'s first write acks: {e}"));
        }
        router.flush_all().await;
    }

    /// Why the deferral is bounded rather than closed by pinning the bucket before the
    /// cap check and carrying it across the deferral, which is the obvious
    /// fix and a worse one.
    ///
    /// The catalog seals an ingest hour `H` once `max_flush_lifetime +
    /// clock_skew_allowance + fold_safety_margin` have passed since `H` ended
    /// (`ravel_catalog` fold, ADR-0020). That margin is the budget for the
    /// span between a flush pinning `H` and its commit record landing, and it
    /// is sized for one flush lifetime, because today the pin is taken when
    /// the flush opens. A pre-deferral pin would spend the deferral out of
    /// that budget instead of out of the read-side scan slack, and a record
    /// that lands past it is worse than one that lands past the slack: it is
    /// not read again at all. Resolution starts its listing at
    /// `watermark_hour + 1`, so the folded snapshot is all that represents
    /// `H` from then on, and it was written without that record. The fold
    /// watermark only moves forward, so nothing re-folds `H`.
    ///
    /// The arithmetic below is the worst case, a trigger firing at the very
    /// end of `H`, and one deferral round is already enough to overrun it.
    #[test]
    fn carrying_a_pre_deferral_pin_would_write_into_a_sealed_hour() {
        // A trigger firing at the very end of `H` has spent all of `H`
        // already, so the seal margin is the whole of what is left.
        let budget_ns = DEFAULT_MAX_FLUSH_LIFETIME_NS
            + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
            + DEFAULT_FOLD_SAFETY_MARGIN_NS;

        // What a carried pin would have to fit into it: the deferral, then
        // the flush it finally opens. A queued flush leaves the `JoinSet`
        // only when it finishes or reaches its deadline, so one round costs
        // up to `max_flush_lifetime`, and the flush that then opens gets its
        // own before it is abandoned.
        let shipped = IngestConfig::default();
        let one_round_ns = shipped.max_flush_lifetime.as_nanos() as i64;
        let spend_ns = one_round_ns + one_round_ns;

        assert!(
            spend_ns > budget_ns,
            "one deferral round ({one_round_ns}ns) plus the flush it opens \
             ({one_round_ns}ns) is {spend_ns}ns, which must out-run the \
             {budget_ns}ns a sealed-hour watermark allows after an hour ends, \
             or a pre-deferral pin would be a safe fix and this test is \
             recording a constraint that no longer exists"
        );
    }

    /// The age threshold the shard actor applies to an empty-waiter buffer
    /// below a non-zero `idle_flush_byte_floor`, read from the predicate
    /// itself rather than restated, so a change to the hold moves this too.
    fn sub_floor_hold_ns(config: &IngestConfig) -> i64 {
        let floored = IngestConfig {
            idle_flush_byte_floor: 1,
            ..*config
        };
        let (hold_ns, trigger) = idle_age_threshold(0, &floored);
        assert_eq!(trigger, FlushTrigger::AgeFloor);
        hold_ns
    }

    /// One metrics shard actor (index 3) whose scan-set check and hand-back
    /// target the test scripts, over a `MemoryStore` and a clock that never
    /// advances, so only the test's `FlushNow`/`Shutdown` messages flush.
    struct HandBackRig {
        tx: mpsc::Sender<ShardMsg>,
        scope: Arc<crate::generation::ScriptedScope<ShardMsg>>,
        metrics: Arc<IngestMetrics>,
        store: Arc<dyn ObjectStoreBackend>,
        budget: Arc<IngestByteBudget>,
        clock: Arc<TestClock>,
        task: tokio::task::JoinHandle<()>,
    }

    /// Handing back to a two-shard set, which these rows split across.
    const HAND_BACK: ScanCheck = ScanCheck::HandBack {
        scan_count: 3,
        target_count: 2,
        reason: HandBackReason::RetiredIndex,
    };

    /// The same two-shard target, for an hour another generation owns.
    const MISMATCH: ScanCheck = ScanCheck::HandBack {
        scan_count: 4,
        target_count: 2,
        reason: HandBackReason::GenerationMismatch,
    };

    /// Age clocks at 50 ms on a 10 ms tick, so a buffer is due on the tick
    /// after its write once the clock moves.
    fn ticking_config() -> IngestConfig {
        IngestConfig {
            shard_count: 4,
            max_flush_delay: Duration::from_millis(50),
            max_flush_delay_idle: Duration::from_millis(50),
            flush_tick: Duration::from_millis(10),
            ..IngestConfig::default()
        }
    }

    /// A hand-back target whose two-slot mailbox is already full.
    fn full_target() -> (mpsc::Sender<ShardMsg>, mpsc::Receiver<ShardMsg>) {
        let (tx, rx) = mpsc::channel(2);
        fill(&tx);
        (tx, rx)
    }

    fn fill(tx: &mpsc::Sender<ShardMsg>) {
        while tx
            .try_send(ShardMsg::FlushNow {
                done: oneshot::channel().0,
            })
            .is_ok()
        {}
    }

    /// Advances the clock one 10 ms tick at a time, yielding after each,
    /// until `probe` holds or 400 ticks pass.
    async fn tick_until(clock: &TestClock, mut probe: impl FnMut() -> bool) -> bool {
        for _ in 0..400 {
            if probe() {
                return true;
            }
            clock.advance_ns(10_000_000);
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
        }
        probe()
    }

    impl HandBackRig {
        fn new(target: Option<mpsc::Sender<ShardMsg>>) -> Self {
            let config = IngestConfig {
                shard_count: 4,
                ..IngestConfig::default()
            };
            Self::with(HAND_BACK, target, config)
        }

        fn with(
            verdict: ScanCheck,
            target: Option<mpsc::Sender<ShardMsg>>,
            config: IngestConfig,
        ) -> Self {
            let (rig, start) = Self::parked(verdict, target, config);
            start.send(()).expect("actor task waits for its start");
            rig
        }

        /// [`Self::with`], with the actor not running until the returned
        /// sender fires, so a test can queue messages in the order the actor
        /// is to find them.
        fn parked(
            verdict: ScanCheck,
            target: Option<mpsc::Sender<ShardMsg>>,
            config: IngestConfig,
        ) -> (Self, oneshot::Sender<()>) {
            let scope = crate::generation::ScriptedScope::new(verdict, target);
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let metrics = Arc::new(IngestMetrics::new(4));
            let clock = TestClock::new(BASE_NS);
            scope.set_clock(clock.clone());
            let (tx, rx) = mpsc::channel(64);
            let actor = Self::actor(
                config,
                scope.clone(),
                Arc::clone(&store),
                Arc::clone(&metrics),
                clock.clone(),
                rx,
            );
            let (start, started) = oneshot::channel::<()>();
            let rig = HandBackRig {
                tx,
                scope,
                metrics,
                store,
                budget: IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1 << 30)),
                clock,
                task: tokio::spawn(async move {
                    if started.await.is_ok() {
                        actor.run().await;
                    }
                }),
            };
            (rig, start)
        }

        /// The shard-3 actor a rig runs.
        fn actor(
            config: IngestConfig,
            scope: Arc<crate::generation::ScriptedScope<ShardMsg>>,
            store: Arc<dyn ObjectStoreBackend>,
            metrics: Arc<IngestMetrics>,
            clock: Arc<TestClock>,
            rx: mpsc::Receiver<ShardMsg>,
        ) -> ShardActor {
            ShardActor::new(
                3,
                Signal::Metrics,
                Uuid::new_v4(),
                1,
                store,
                clock,
                Arc::new(ravel_commit::rng::SystemRng),
                config,
                metrics,
                rx,
                Arc::new(AtomicI64::new(0)),
                BufferBudgetCeiling::unlimited(),
                DeferralCapFlag::new(config.flush_deferral_cap_ns()),
                scope,
                WriteGateSlot::default(),
                #[cfg(feature = "stage-timing")]
                Arc::new(MetricStageTimings::new()),
            )
        }

        /// An actor over `config` that never runs, for a test that calls its
        /// methods directly.
        fn idle_actor(config: IngestConfig) -> ShardActor {
            Self::actor(
                config,
                crate::generation::ScriptedScope::new(ScanCheck::InScanSet, None),
                Arc::new(MemoryStore::new()),
                Arc::new(IngestMetrics::new(4)),
                TestClock::new(BASE_NS),
                mpsc::channel(1).1,
            )
        }

        /// Commit records written under this rig's shard index, 3.
        async fn own_commits(&self) -> usize {
            list_all(self.store.as_ref(), "t/")
                .await
                .expect("list")
                .into_iter()
                .filter(|o| o.key.contains("/c/0003/"))
                .count()
        }

        /// Samples across the commit records written under shard index 3.
        async fn own_samples(&self) -> u64 {
            let mut samples = 0;
            for object in list_all(self.store.as_ref(), "t/").await.expect("list") {
                if object.key.contains("/c/0003/") {
                    let bytes = self
                        .store
                        .get(&object.key, GetRange::Full)
                        .await
                        .expect("get commit record")
                        .data;
                    samples += record::decode(&bytes).expect("decode").sample_count;
                }
            }
            samples
        }

        /// Six points of tenant `acme`, which split across a two-shard set.
        fn six_points() -> Vec<IngestPoint> {
            let tenant = TenantId::new("acme");
            let points: Vec<IngestPoint> = (0..6)
                .map(|i| {
                    let p = point(&tenant, &format!("host-{i}"));
                    IngestPoint {
                        series_id: p.series_id,
                        labels: p.labels,
                        value: IngestValue::Scalar(p.sample),
                    }
                })
                .collect();
            let targets: HashSet<u32> = points.iter().map(|p| shard_for(&p.series_id, 2)).collect();
            assert_eq!(targets.len(), 2, "the rows split across both targets");
            points
        }

        /// Buffers six points of tenant `acme`, under one charge of `bytes`.
        async fn write(&self, bytes: u64) -> Arc<IngestByteCharge> {
            self.write_acked(bytes, None).await
        }

        /// [`Self::write`] with a strict waiter, answered on the returned
        /// receiver.
        async fn write_strict(
            &self,
            bytes: u64,
        ) -> (
            Arc<IngestByteCharge>,
            oneshot::Receiver<Result<CommitToken, WriteError>>,
        ) {
            let (ack, answer) = oneshot::channel();
            (self.write_acked(bytes, Some(ack)).await, answer)
        }

        async fn write_acked(&self, bytes: u64, ack: Option<Ack>) -> Arc<IngestByteCharge> {
            let charge = Arc::new(self.budget.try_charge(bytes).expect("charge"));
            self.tx
                .send(ShardMsg::Write {
                    arrival: false,
                    tenant: TenantId::new("acme"),
                    points: Self::six_points(),
                    exemplars: Vec::new(),
                    ack,
                    charge: Some(Arc::clone(&charge)),
                })
                .await
                .expect("send write");
            charge
        }

        /// Buffers six points of `tenant`, under one charge of `bytes`.
        async fn write_tenant(&self, tenant: &str, bytes: u64) -> Arc<IngestByteCharge> {
            let tenant = TenantId::new(tenant);
            let points = Self::points_of(&tenant);
            let charge = Arc::new(self.budget.try_charge(bytes).expect("charge"));
            self.tx
                .send(ShardMsg::Write {
                    arrival: false,
                    tenant,
                    points,
                    exemplars: Vec::new(),
                    ack: None,
                    charge: Some(Arc::clone(&charge)),
                })
                .await
                .expect("send write");
            charge
        }

        fn points_of(tenant: &TenantId) -> Vec<IngestPoint> {
            (0..6)
                .map(|i| {
                    let p = point(tenant, &format!("host-{i}"));
                    IngestPoint {
                        series_id: p.series_id,
                        labels: p.labels,
                        value: IngestValue::Scalar(p.sample),
                    }
                })
                .collect()
        }

        /// Queues six uncharged points of `tenant` without waiting for room in
        /// the mailbox.
        fn try_write_tenant(&self, tenant: &str) -> bool {
            let tenant = TenantId::new(tenant);
            let points = Self::points_of(&tenant);
            self.tx
                .try_send(ShardMsg::Write {
                    arrival: false,
                    tenant,
                    points,
                    exemplars: Vec::new(),
                    ack: None,
                    charge: None,
                })
                .is_ok()
        }

        async fn flush_now(&self) {
            let (done, wait) = oneshot::channel();
            self.tx
                .send(ShardMsg::FlushNow { done })
                .await
                .expect("send FlushNow");
            wait.await.expect("FlushNow answered");
        }

        /// Hands the buffer to a live test-owned target and returns every
        /// point and charge the actor sends it.
        async fn drain_to_live_target(&self) -> (usize, Vec<Arc<IngestByteCharge>>) {
            let (target, mut inbox) = mpsc::channel(16);
            self.scope.set(HAND_BACK, Some(target));
            self.flush_now().await;
            let (mut points, mut charges) = (0, Vec::new());
            while let Ok(msg) = inbox.try_recv() {
                if let ShardMsg::HandBack {
                    points: p,
                    charges: c,
                    ..
                } = msg
                {
                    points += p.len();
                    charges.extend(c);
                }
            }
            (points, charges)
        }
    }

    /// A target whose mailbox closes between the liveness check and the send
    /// (the scripted scope returns it as live): every row goes back into the
    /// source buffer and its charge is held exactly once, so the next hand-back
    /// delivers all six rows with the one charge per target message.
    ///
    /// Guard: the `rest.absorb(..)` call in `hand_back`'s failed-send branch.
    /// Without it the returned message is dropped, the charge refunds at
    /// once, and the held-bytes assertion reads 0.
    #[tokio::test]
    async fn a_target_closed_after_the_liveness_check_returns_the_rows() {
        let (closed, inbox) = mpsc::channel(16);
        drop(inbox);
        let rig = HandBackRig::new(Some(closed));
        let charge = rig.write(4_096).await;
        let charge_ptr = Arc::as_ptr(&charge);
        drop(charge);

        rig.flush_now().await;
        let snap = rig.metrics.snapshot();
        assert_eq!(
            rig.budget.in_flight_bytes(),
            4_096,
            "the charge is still held"
        );
        assert_eq!(
            snap.hand_back_failures, 1,
            "one episode, however many sends and passes failed"
        );
        assert_eq!(snap.rerouted_flushes, 0, "no target took rows");

        let (points, charges) = rig.drain_to_live_target().await;
        assert_eq!(points, 6, "every row stayed in the source buffer");
        assert_eq!(
            charges.len(),
            2,
            "one clone per target message, not one per failed send"
        );
        assert!(
            charges
                .iter()
                .all(|c| std::ptr::eq(Arc::as_ptr(c), charge_ptr))
        );
        assert_eq!(rig.budget.in_flight_bytes(), 4_096);
        drop(charges);
        assert_eq!(
            rig.budget.in_flight_bytes(),
            0,
            "refunded once the rows leave"
        );
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A target that is not live at the check keeps the whole buffer, counted
    /// once however many drain passes retry it.
    ///
    /// Guard: `self.tenants.insert(tenant, buf)` in the `else if enforced`
    /// branch of `flush_tenant`'s hand-back arm. Without it the buffer is
    /// dropped and the live target later receives no rows.
    #[tokio::test]
    async fn a_dead_target_keeps_the_whole_source_buffer() {
        let rig = HandBackRig::new(None);
        drop(rig.write(4_096).await);

        rig.flush_now().await;
        rig.flush_now().await;
        assert_eq!(rig.metrics.snapshot().hand_back_failures, 1);
        assert_eq!(rig.budget.in_flight_bytes(), 4_096);
        let (points, charges) = rig.drain_to_live_target().await;
        assert_eq!(points, 6);
        drop(charges);
        assert_eq!(rig.budget.in_flight_bytes(), 0);
        assert!(
            list_all(rig.store.as_ref(), "t/")
                .await
                .expect("list")
                .is_empty(),
            "nothing was written under the shard"
        );
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A teardown whose hand-back target is not live writes the rows in place,
    /// where readers do not scan them, and counts the write.
    ///
    /// Guard: `self.metrics.record_teardown_unscanned_write()` in the last
    /// branch of `flush_tenant`'s hand-back arm, and the `buf` it falls
    /// through with. Without the first the counter reads 0; with the arm
    /// keeping the buffer instead, nothing is written and the residue counter
    /// moves.
    #[tokio::test]
    async fn a_teardown_with_a_dead_target_writes_in_place_and_counts_it() {
        let rig = HandBackRig::new(None);
        drop(rig.write(4_096).await);

        let (done, wait) = oneshot::channel();
        rig.tx
            .send(ShardMsg::Shutdown { done })
            .await
            .expect("send Shutdown");
        wait.await.expect("Shutdown answered");
        let snap = rig.metrics.snapshot();
        assert_eq!(snap.teardown_unscanned_writes, 1);
        assert_eq!(snap.flush_all_residue_tenants, 0);
        assert_eq!(rig.budget.in_flight_bytes(), 0);
        let commits: Vec<String> = list_all(rig.store.as_ref(), "t/")
            .await
            .expect("list")
            .into_iter()
            .map(|o| o.key)
            .filter(|k| k.contains("/c/0003/"))
            .collect();
        assert_eq!(commits.len(), 1, "one commit under shard 3, {commits:?}");
        rig.task.await.expect("actor ends");
    }

    /// A generation-mismatch hand-back to a set it may not wait on finds the
    /// target mailbox full: the rows go back to this shard's buffer with their
    /// charge, and a later trigger with room hands them over. With the mailbox
    /// kept full, a buffer is retried until the flush deferral cap and then
    /// written in place, counted once, and a strict waiter still attached at
    /// the cap is answered the deferral-cap `Abandoned`, not `Ok`.
    ///
    /// Guards: the `mismatch_retry_spent(..)` arm of `flush_tenant`: without
    /// it the rows stay buffered past the cap and the in-place counter reads
    /// 0. The waiter strip in that arm: without it the waiter reads `Ok`. And
    /// the `rest.absorb(..)` call in `hand_back`: without it the rows of a
    /// failed send are dropped and the held-bytes assertion reads 0.
    #[tokio::test]
    async fn a_full_upward_mailbox_is_retried_until_the_deferral_cap() {
        let config = ticking_config();
        let (target, mut inbox) = full_target();
        let rig = HandBackRig::with(MISMATCH, Some(target.clone()), config);
        rig.scope.set_wait(false);
        drop(rig.write(4_096).await);

        assert!(
            tick_until(&rig.clock, || rig.metrics.snapshot().hand_back_failures
                == 1)
            .await,
            "the age trigger tries the hand-back"
        );
        let snap = rig.metrics.snapshot();
        assert_eq!(
            rig.budget.in_flight_bytes(),
            4_096,
            "the rows and their charge are back in the source buffer"
        );
        assert_eq!(snap.rerouted_flushes, 0);
        assert_eq!(snap.generation_mismatch_written_in_place, 0);

        while inbox.try_recv().is_ok() {}
        assert!(
            tick_until(&rig.clock, || rig.metrics.snapshot().rerouted_flushes == 1).await,
            "a later trigger with room hands the rows over"
        );
        let (mut points, mut charges) = (0, Vec::new());
        while let Ok(msg) = inbox.try_recv() {
            if let ShardMsg::HandBack {
                points: p,
                charges: c,
                ..
            } = msg
            {
                points += p.len();
                charges.extend(c);
            }
        }
        assert_eq!(points, 6, "every row is handed over");
        assert_eq!(
            rig.metrics.snapshot().rerouted_flushes_generation_mismatch,
            1
        );
        drop(charges);
        assert_eq!(rig.budget.in_flight_bytes(), 0);

        fill(&target);
        drop(rig.write(4_096).await);
        assert!(
            tick_until(&rig.clock, || rig.metrics.snapshot().hand_back_failures
                == 2)
            .await,
            "the second buffer's hand-back fails on the full mailbox"
        );
        let cap_ns = config.flush_deferral_cap_ns();
        rig.clock.advance_ns(cap_ns - 1);
        assert!(!settles(|| rig.budget.in_flight_bytes() == 0).await);
        assert_eq!(
            rig.metrics.snapshot().generation_mismatch_written_in_place,
            0,
            "retried, not written, short of the cap"
        );
        // A strict write joining after that retry is still attached when the
        // cap is reached.
        let (charge, answer) = rig.write_strict(4_096).await;
        drop(charge);
        // Before the clock moves, or the tick can win the actor's select and
        // write the buffer before the strict write joins it.
        assert!(
            settles(|| rig.tx.capacity() == rig.tx.max_capacity()).await,
            "the actor takes the strict write"
        );
        assert!(
            tick_until(&rig.clock, || rig.budget.in_flight_bytes() == 0).await,
            "written once the deferral reaches the cap"
        );
        ticks(&rig.clock, 5).await;
        let snap = rig.metrics.snapshot();
        assert_eq!(snap.generation_mismatch_written_in_place, 1);
        assert_eq!(snap.rerouted_flushes, 1);
        assert_eq!(rig.own_commits().await, 1, "written under shard 3");
        match answer.await.expect("waiter answered") {
            Err(WriteError::Abandoned(msg)) => assert_eq!(
                msg, DEFERRAL_CAP_ABANDONED,
                "not acknowledged from a flush at the cap"
            ),
            other => panic!("expected the deferral-cap Abandoned, got {other:?}"),
        }
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A drain whose generation-mismatch hand-back finds the target mailbox
    /// full does not answer with the rows still buffered: its last pass
    /// writes them in place, counted once.
    ///
    /// Guard: the `DrainIntent::Retryable` last pass in `flush_all`. Without
    /// it `FlushNow` is answered with the rows still buffered and charged, and
    /// nothing is written.
    #[tokio::test]
    async fn a_drain_with_a_full_upward_target_answers_with_nothing_buffered() {
        let config = IngestConfig {
            shard_count: 4,
            ..IngestConfig::default()
        };
        let (target, _inbox) = full_target();
        let rig = HandBackRig::with(MISMATCH, Some(target), config);
        rig.scope.set_wait(false);
        drop(rig.write(4_096).await);

        rig.flush_now().await;
        let snap = rig.metrics.snapshot();
        assert_eq!(
            rig.budget.in_flight_bytes(),
            0,
            "nothing is left buffered when the drain answers"
        );
        assert_eq!(snap.generation_mismatch_written_in_place, 1);
        assert_eq!(snap.rerouted_flushes, 0);
        assert_eq!(rig.own_commits().await, 1, "written under shard 3");
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// Ticks until the first age-triggered flush of a generation-mismatch
    /// buffer has either written it in place or kept it for a retry, then a
    /// few more, and returns the rig's snapshot.
    async fn first_mismatch_flush(rig: &HandBackRig) -> crate::metrics::IngestMetricsSnapshot {
        assert!(
            tick_until(&rig.clock, || {
                let snap = rig.metrics.snapshot();
                snap.generation_mismatch_written_in_place + snap.hand_back_failures > 0
            })
            .await,
            "the age trigger opens the flush"
        );
        ticks(&rig.clock, 5).await;
        rig.metrics.snapshot()
    }

    /// A generation-mismatch hand-back whose target is not live at the check
    /// (a dead or condemned shard, or a closed mailbox) writes the rows in
    /// place on the first flush that sees it, with no retry: one flush, the
    /// in-place counter at exactly 1, every row durable, nothing left
    /// buffered. The strict waiter is still on the buffer and is answered
    /// `Ok` from that write.
    ///
    /// Guard: `target_dead` in the condition of the in-place branch of
    /// `flush_tenant`'s hand-back arm. Without it the buffer takes the retry
    /// branch: `hand_back_failures` reads 1, nothing is written, and the
    /// charge stays held.
    #[tokio::test]
    async fn a_mismatch_to_a_target_not_live_at_the_check_is_written_in_place_at_once() {
        let rig = HandBackRig::with(MISMATCH, None, ticking_config());
        rig.scope.set_wait(false);
        let (charge, answer) = rig.write_strict(4_096).await;
        drop(charge);

        let snap = first_mismatch_flush(&rig).await;
        assert_eq!(snap.hand_back_failures, 0, "not kept for a retry");
        assert_eq!(snap.generation_mismatch_written_in_place, 1);
        assert_eq!(snap.flushes_by_age, 1, "exactly one flush");
        assert_eq!(snap.rerouted_flushes, 0);
        answer
            .await
            .expect("waiter answered")
            .expect("the in-place write acknowledges the strict waiter");
        assert!(settles(|| rig.budget.in_flight_bytes() == 0).await);
        assert_eq!(rig.own_commits().await, 1, "written under shard 3");
        assert_eq!(rig.own_samples().await, 6, "every row, once");
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// The same for a target whose mailbox closes between the liveness check
    /// and the send: written in place at once. The strict waiter was answered
    /// `Abandoned` before the send, as on every hand-back past the check.
    ///
    /// Guard: `target_dead |= refused.closed` in `hand_back`. Without it a
    /// closed mailbox reads as a full one and the buffer is kept for a retry.
    #[tokio::test]
    async fn a_mismatch_to_a_target_closed_at_the_send_is_written_in_place_at_once() {
        let (closed, inbox) = mpsc::channel(16);
        drop(inbox);
        let rig = HandBackRig::with(MISMATCH, Some(closed), ticking_config());
        rig.scope.set_wait(false);
        let (charge, answer) = rig.write_strict(4_096).await;
        drop(charge);

        let snap = first_mismatch_flush(&rig).await;
        assert_eq!(snap.hand_back_failures, 0, "not kept for a retry");
        assert_eq!(snap.generation_mismatch_written_in_place, 1);
        assert_eq!(snap.flushes_by_age, 1, "exactly one flush");
        assert!(matches!(
            answer.await.expect("waiter answered"),
            Err(WriteError::Abandoned(_))
        ));
        assert!(settles(|| rig.budget.in_flight_bytes() == 0).await);
        assert_eq!(rig.own_commits().await, 1, "written under shard 3");
        assert_eq!(rig.own_samples().await, 6, "every row, once");
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A hand-back already queued when the actor takes `Shutdown` is absorbed
    /// as the actor closes its mailbox and written by the teardown flush: its
    /// rows are durable exactly once, their charge is released, and no
    /// tenant is left unflushed.
    ///
    /// Guard: the `self.absorb_rows(..)` call in `close_mailbox`. Pushing the
    /// queued `HandBack` to `unprocessed` instead drops it after the drain:
    /// nothing is written and the commit count reads 0.
    #[tokio::test]
    async fn a_hand_back_queued_behind_shutdown_is_written_exactly_once() {
        let config = IngestConfig {
            shard_count: 4,
            ..IngestConfig::default()
        };
        let (rig, start) = HandBackRig::parked(ScanCheck::InScanSet, None, config);
        let charge = Arc::new(rig.budget.try_charge(4_096).expect("charge"));
        let (done, answered) = oneshot::channel();
        rig.tx
            .send(ShardMsg::Shutdown { done })
            .await
            .expect("send Shutdown");
        rig.tx
            .send(ShardMsg::HandBack {
                tenant: TenantId::new("acme"),
                points: HandBackRig::six_points(),
                exemplars: Vec::new(),
                charges: vec![Arc::clone(&charge)],
                arrival: HandBackArrival {
                    oldest_arrival_ns: Some(BASE_NS),
                    min_ingest_ts_ns: Some(BASE_NS),
                    max_ingest_ts_ns: Some(BASE_NS),
                },
            })
            .await
            .expect("send HandBack behind Shutdown");
        drop(charge);
        start.send(()).expect("actor task waits for its start");

        answered.await.expect("Shutdown answered");
        let snap = rig.metrics.snapshot();
        assert_eq!(snap.flush_all_residue_tenants, 0);
        assert_eq!(snap.series_id_collisions, 0);
        assert_eq!(rig.budget.in_flight_bytes(), 0, "the charge is released");
        assert_eq!(rig.own_commits().await, 1, "one teardown write");
        assert_eq!(rig.own_samples().await, 6, "every handed-back row, once");
        rig.task.await.expect("actor ends");
    }

    /// Text of the hand-back WARN, and of the two in-place WARNs of
    /// `flush_tenant`'s generation-mismatch arms.
    const HAND_BACK_WARN: &str = "handing its rows to that hour's shard generation";
    const DEAD_TARGET_WARN: &str = "because a hand-back target shard is dead";
    const RETRY_SPENT_WARN: &str = "kept failing for the flush deferral cap";

    /// Three flushes in a row that each write a generation mismatch in place,
    /// for a hand-back target that is not live, log one WARN and move the
    /// in-place counter by exactly 3. A flush that opens inside the scan set
    /// ends the run, so the next in-place write warns again.
    ///
    /// Guards: `self.in_place_warnings.should_warn(..)` gating the WARN in
    /// the in-place branch of `flush_tenant`'s hand-back arm: without it the
    /// run logs three WARNs. And `self.in_place_warnings.rearm(tenant_hash)`
    /// in the `InScanSet` arm: without it the write after the in-scan flush
    /// logs nothing.
    #[tokio::test]
    async fn a_run_of_in_place_mismatch_writes_warns_once() {
        let warns = crate::generation::WarnCapture::default();
        let _capture = warns.install();
        let config = IngestConfig {
            shard_count: 4,
            ..IngestConfig::default()
        };
        let rig = HandBackRig::with(MISMATCH, None, config);
        for _ in 0..3 {
            drop(rig.write(4_096).await);
            rig.flush_now().await;
        }
        assert_eq!(
            rig.metrics.snapshot().generation_mismatch_written_in_place,
            3
        );
        assert_eq!(rig.own_commits().await, 3);
        assert_eq!(warns.count(DEAD_TARGET_WARN), 1, "one WARN for the run");

        rig.scope.set(ScanCheck::InScanSet, None);
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        rig.scope.set(MISMATCH, None);
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(
            rig.metrics.snapshot().generation_mismatch_written_in_place,
            4
        );
        assert_eq!(rig.own_commits().await, 5);
        assert_eq!(
            warns.count(DEAD_TARGET_WARN),
            2,
            "the in-scan flush ended the run"
        );
        assert_eq!(warns.count(HAND_BACK_WARN), 0);
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// The same for the arm that writes in place once a generation-mismatch
    /// hand-back has kept failing for the deferral cap, here zero: each drain
    /// retries the hand-back once, then writes in place, and two such drains
    /// log one WARN.
    ///
    /// Guard: `self.in_place_warnings.should_warn(..)` gating the WARN in
    /// `flush_tenant`'s `mismatch_retry_spent(..)` arm: without it the two
    /// writes log two WARNs.
    #[tokio::test]
    async fn a_run_of_retry_spent_in_place_writes_warns_once() {
        let warns = crate::generation::WarnCapture::default();
        let _capture = warns.install();
        let config = IngestConfig {
            shard_count: 4,
            max_flush_lifetime: Duration::from_secs(7200),
            ..IngestConfig::default()
        };
        assert_eq!(config.flush_deferral_cap_ns(), 0);
        let (target, _inbox) = full_target();
        let rig = HandBackRig::with(MISMATCH, Some(target), config);
        rig.scope.set_wait(false);
        for _ in 0..2 {
            drop(rig.write(4_096).await);
            rig.flush_now().await;
        }
        let snap = rig.metrics.snapshot();
        assert_eq!(snap.hand_back_failures, 2, "each buffer was retried once");
        assert_eq!(snap.generation_mismatch_written_in_place, 2);
        assert_eq!(warns.count(RETRY_SPENT_WARN), 1, "one WARN for the run");
        assert_eq!(warns.count(DEAD_TARGET_WARN), 0);
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A run of hand-backs logs one WARN, and a flush written in place over
    /// a generation mismatch, with nothing handed back, ends the run: the
    /// next hand-back warns again.
    ///
    /// Guard: `self.handing_back = false` under `!handed.delivered` in the
    /// in-place branch of `flush_tenant`'s hand-back arm: without it the
    /// last hand-back logs nothing.
    #[tokio::test]
    async fn an_in_place_write_ends_a_hand_back_run() {
        let warns = crate::generation::WarnCapture::default();
        let _capture = warns.install();
        let config = IngestConfig {
            shard_count: 4,
            ..IngestConfig::default()
        };
        let (target, _inbox) = mpsc::channel(64);
        let rig = HandBackRig::with(HAND_BACK, Some(target.clone()), config);
        for _ in 0..2 {
            drop(rig.write(4_096).await);
            rig.flush_now().await;
        }
        assert_eq!(rig.metrics.snapshot().rerouted_flushes, 2);
        assert_eq!(warns.count(HAND_BACK_WARN), 1, "one WARN for the run");

        rig.scope.set(MISMATCH, None);
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(
            rig.metrics.snapshot().generation_mismatch_written_in_place,
            1
        );
        assert_eq!(warns.count(DEAD_TARGET_WARN), 1);

        rig.scope.set(HAND_BACK, Some(target));
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(rig.metrics.snapshot().rerouted_flushes, 3);
        assert_eq!(
            warns.count(HAND_BACK_WARN),
            2,
            "the in-place write ended the run"
        );
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A config whose flush deferral cap is zero, so a generation-mismatch
    /// hand-back that fails once is written in place by the drain's last pass.
    fn spent_at_once_config() -> IngestConfig {
        let config = IngestConfig {
            shard_count: 4,
            max_flush_lifetime: Duration::from_secs(7200),
            ..IngestConfig::default()
        };
        assert_eq!(config.flush_deferral_cap_ns(), 0);
        config
    }

    /// A strict waiter on a buffer held closed because no trusted provisioning
    /// view arrives is held until `max_flush_lifetime` after the first hold,
    /// then answered the retryable `Abandoned`, its bytes refunded and nothing
    /// written (issue #4363).
    ///
    /// Guard: `ack_waiters` in `drop_stale_view_flush`. Dropping the waiters
    /// with the buffer instead of answering them closes the receiver without
    /// an answer, and holding past the bound leaves it waiting.
    #[tokio::test]
    async fn a_strict_waiter_on_a_spent_stale_view_hold_is_abandoned() {
        let config = IngestConfig {
            shard_count: 4,
            max_flush_lifetime: Duration::from_secs(60),
            ..IngestConfig::default()
        };
        let rig = HandBackRig::with(ScanCheck::Unknown, None, config);
        let (charge, mut answer) = rig.write_strict(4_096).await;
        drop(charge);
        rig.flush_now().await;
        rig.clock.advance_ns(60_000_000_000 - 1);
        rig.flush_now().await;
        assert!(
            matches!(answer.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "held one nanosecond inside the bound"
        );
        assert_eq!(rig.budget.in_flight_bytes(), 4_096);

        rig.clock.advance_ns(1);
        rig.flush_now().await;
        match answer.try_recv() {
            Ok(Err(WriteError::Abandoned(msg))) => assert_eq!(msg, STALE_VIEW_ABANDONED),
            other => panic!("expected the stale-view Abandoned, got {other:?}"),
        }
        let snap = rig.metrics.snapshot();
        assert_eq!(snap.abandoned_stale_provisioning, 1);
        assert_eq!(snap.stale_provisioning_flushes, 1);
        assert_eq!(rig.budget.in_flight_bytes(), 0, "the bytes are refunded");
        assert_eq!(rig.own_commits().await, 0, "nothing is written");
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// The stale-view bound of [`stale_view_config`].
    const STALE_BOUND_NS: i64 = 60_000_000_000;

    fn stale_view_config() -> IngestConfig {
        IngestConfig {
            shard_count: 4,
            max_flush_lifetime: Duration::from_secs(60),
            ..IngestConfig::default()
        }
    }

    /// Drains the rig under an untrusted view and returns how many flushes it
    /// has dropped on a spent stale-view hold so far.
    async fn unknown_flush(rig: &HandBackRig) -> u64 {
        rig.scope.set(ScanCheck::Unknown, None);
        rig.flush_now().await;
        rig.metrics.snapshot().abandoned_stale_provisioning
    }

    /// Asserts one more bound after the hold restarted drops the buffer, and
    /// ends the rig.
    async fn assert_dropped_one_bound_later(rig: HandBackRig) {
        rig.clock.advance_ns(STALE_BOUND_NS - 1);
        assert_eq!(unknown_flush(&rig).await, 0, "inside the restarted hold");
        assert_eq!(rig.budget.in_flight_bytes(), 4_096);
        rig.clock.advance_ns(1);
        assert_eq!(unknown_flush(&rig).await, 1, "the restarted hold is spent");
        assert_eq!(rig.budget.in_flight_bytes(), 0);
        assert_eq!(rig.own_commits().await, 0);
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A provisioning read that succeeds during a stale-view hold restarts it.
    /// A buffer first held at t0 whose tenant view a read then refreshed is
    /// not dropped by a flush that finds no trusted view at t0 + 2 bounds,
    /// with no flush attempt in between (as when its flushes are deferred);
    /// the hold starts over there and is spent one bound later.
    ///
    /// Guard: the `refreshed_at_ns` restart at the top of `StaleViewHold::step`.
    /// Without it, and with a hold restarted at the refresh time instead of
    /// the flush, the flush at t0 + 2 bounds drops the buffer; a hold cleared
    /// whenever any view is cached never drops it.
    #[tokio::test]
    async fn a_successful_read_during_a_stale_view_hold_restarts_it() {
        let rig = HandBackRig::with(ScanCheck::Unknown, None, stale_view_config());
        drop(rig.write(4_096).await);
        assert_eq!(unknown_flush(&rig).await, 0);
        rig.scope.set_refreshed_at(Some(BASE_NS + 1_000_000));
        rig.clock.advance_ns(2 * STALE_BOUND_NS);
        assert_eq!(unknown_flush(&rig).await, 0, "the read restarted the hold");
        assert_eq!(rig.metrics.snapshot().stale_provisioning_flushes, 1);
        assert_dropped_one_bound_later(rig).await;
    }

    /// A view refreshed before the hold began does not restart it: the buffer
    /// is dropped one bound after its first hold.
    ///
    /// Guard: the `at >= since` comparison in `StaleViewHold::step`. A hold
    /// cleared whenever any view is cached is still held at the bound.
    #[tokio::test]
    async fn a_read_from_before_a_stale_view_hold_does_not_restart_it() {
        let rig = HandBackRig::with(ScanCheck::Unknown, None, stale_view_config());
        rig.scope.set_refreshed_at(Some(BASE_NS - 1));
        drop(rig.write(4_096).await);
        assert_eq!(unknown_flush(&rig).await, 0);
        rig.clock.advance_ns(STALE_BOUND_NS - 1);
        assert_eq!(unknown_flush(&rig).await, 0);
        rig.clock.advance_ns(1);
        assert_eq!(unknown_flush(&rig).await, 1, "no read since the hold began");
        assert_eq!(rig.budget.in_flight_bytes(), 0);
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A held buffer whose hand-back is attempted and keeps its rows restarts
    /// its stale-view hold: a later flush on an untrusted view at the first
    /// hold's bound does not drop it. `target` is the hand-back target: `None`
    /// for a dead one, which returns the buffer itself, or a full mailbox,
    /// which returns the refused rows in a new buffer.
    async fn assert_kept_hand_back_restarts_the_hold(target: Option<mpsc::Sender<ShardMsg>>) {
        let rig = HandBackRig::with(ScanCheck::Unknown, None, stale_view_config());
        rig.scope.set_wait(false);
        drop(rig.write(4_096).await);
        assert_eq!(unknown_flush(&rig).await, 0);
        rig.scope.set(HAND_BACK, target);
        rig.clock.advance_ns(1_000_000_000);
        rig.flush_now().await;
        assert_eq!(rig.metrics.snapshot().hand_back_failures, 1);
        assert_eq!(rig.budget.in_flight_bytes(), 4_096, "the rows were kept");
        rig.clock.advance_ns(STALE_BOUND_NS - 1_000_000_000);
        assert_eq!(unknown_flush(&rig).await, 0, "the hold restarted here");
        assert_dropped_one_bound_later(rig).await;
    }

    /// Guard: `buf.stale_view_hold.clear()` where `hand_back` returns kept
    /// rows. Without it the flush at the first hold's bound drops the buffer.
    #[tokio::test]
    async fn a_hand_back_to_a_dead_target_restarts_the_stale_view_hold() {
        assert_kept_hand_back_restarts_the_hold(None).await;
    }

    /// Guard: the refused rows return in a new `TenantBuf` that carries no
    /// hold, and the clear above. Carrying `stale_view_hold` into that
    /// buffer with the clear removed drops it at the first hold's bound.
    #[tokio::test]
    async fn a_hand_back_to_a_full_mailbox_restarts_the_stale_view_hold() {
        let (target, _inbox) = full_target();
        assert_kept_hand_back_restarts_the_hold(Some(target)).await;
    }

    /// A held buffer whose flush opens on a scan-set verdict, and is then
    /// held by the format-floor check, restarts its stale-view hold.
    ///
    /// Guard: `buf.stale_view_hold.clear()` in the `InScanSet` arm.
    /// Without it the flush at the first hold's bound drops the buffer.
    #[tokio::test]
    async fn a_scan_set_verdict_restarts_the_stale_view_hold() {
        let rig = HandBackRig::with(ScanCheck::Unknown, None, stale_view_config());
        rig.scope.set_floor(FloorCheck::NoView);
        drop(rig.write(4_096).await);
        assert_eq!(unknown_flush(&rig).await, 0);
        rig.scope.set(ScanCheck::InScanSet, None);
        rig.clock.advance_ns(1_000_000_000);
        rig.flush_now().await;
        assert_eq!(rig.budget.in_flight_bytes(), 4_096, "the floor held it");
        rig.clock.advance_ns(STALE_BOUND_NS - 1_000_000_000);
        assert_eq!(unknown_flush(&rig).await, 0, "the hold restarted here");
        assert_dropped_one_bound_later(rig).await;
    }

    /// A held buffer whose generation-mismatch retry is spent, so its flush
    /// opens in place (and is then held by the format-floor check), restarts
    /// its stale-view hold.
    ///
    /// Guard: `buf.stale_view_hold.clear()` in the retry-spent arm.
    /// Without it the flush one second after it drops the buffer, a deferral
    /// cap after the hold began.
    #[tokio::test]
    async fn a_spent_mismatch_retry_restarts_the_stale_view_hold() {
        let config = stale_view_config();
        let cap_ns = config.flush_deferral_cap_ns();
        assert!(cap_ns > STALE_BOUND_NS);
        let rig = HandBackRig::with(MISMATCH, None, config);
        rig.scope.set_wait(false);
        rig.scope.set_floor(FloorCheck::NoView);
        let (target, _inbox) = full_target();
        rig.scope.set(MISMATCH, Some(target.clone()));
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(rig.metrics.snapshot().hand_back_failures, 1);
        rig.clock.advance_ns(1_000_000_000);
        assert_eq!(unknown_flush(&rig).await, 0, "the hold begins");
        rig.clock.advance_ns(cap_ns);
        rig.scope.set(MISMATCH, Some(target));
        let in_place = rig.metrics.snapshot().generation_mismatch_written_in_place;
        rig.flush_now().await;
        assert!(rig.metrics.snapshot().generation_mismatch_written_in_place > in_place);
        assert_eq!(rig.budget.in_flight_bytes(), 4_096, "the floor held it");
        rig.clock.advance_ns(1_000_000_000);
        assert_eq!(unknown_flush(&rig).await, 0, "the hold restarted");
        assert_dropped_one_bound_later(rig).await;
    }

    /// [`stale_view_config`] on [`ticking_config`]'s 10 ms tick, so the age
    /// trigger, not a drain, opens every flush.
    fn stale_ticking_config() -> IngestConfig {
        IngestConfig {
            max_flush_lifetime: Duration::from_secs(60),
            ..ticking_config()
        }
    }

    /// Polls without moving the clock, so the tick the last clock move
    /// released runs and no later one does.
    async fn yield_rounds() {
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
    }

    /// A rig whose `acme` buffer the age trigger has held on an untrusted view
    /// up to its bound, with every provisioning read doing as `read` says.
    /// Returns once the tick at the bound, which spends the hold, has run,
    /// with no tick after it.
    async fn spent_age_hold(read: ScriptedRead) -> HandBackRig {
        let rig = HandBackRig::with(ScanCheck::Unknown, None, stale_ticking_config());
        rig.scope.set_read(read);
        drop(rig.write(4_096).await);
        assert!(
            tick_until(&rig.clock, || rig
                .metrics
                .snapshot()
                .stale_provisioning_flushes
                == 1)
            .await,
            "the age trigger holds the flush"
        );
        assert_eq!(
            rig.scope.reads(),
            0,
            "no read is requested inside the bound"
        );
        rig.clock.advance_ns(STALE_BOUND_NS);
        yield_rounds().await;
        rig
    }

    /// Ends a rig whose provisioning read hangs: the teardown drain waits out
    /// its bounded re-read, so the clock moves until the actor ends.
    async fn end_past_hanging_reads(rig: HandBackRig) {
        let HandBackRig {
            tx, clock, task, ..
        } = rig;
        drop(tx);
        for _ in 0..10 {
            if task.is_finished() {
                break;
            }
            clock.advance_ns(STALE_BOUND_NS);
            yield_rounds().await;
        }
        task.await.expect("actor ends");
    }

    /// The hash of the tenant `beta`, which [`HandBackRig::write_tenant`]
    /// writes.
    fn beta() -> TenantHash {
        TenantId::new("beta").hash()
    }

    /// A spent stale-view hold whose provisioning read never answers does not
    /// delay another tenant: `beta`'s buffer is published on the age tick it
    /// is due at, while `acme`'s stays held (issue #4363).
    ///
    /// Guard: the actor only requesting the read (`StaleViewHold::advance`)
    /// outside a drain, never awaiting it. Awaiting the re-read on the
    /// trigger that finds the hold spent, bounded by `max_flush_lifetime` or
    /// unbounded, or awaiting the spawned read's answer, stops every tick behind it, so `beta` is still charged.
    #[tokio::test]
    async fn a_hanging_read_on_a_spent_hold_does_not_delay_another_tenant() {
        let rig = spent_age_hold(ScriptedRead::Hangs).await;
        assert_eq!(rig.scope.reads(), 1, "the spent hold requested a read");
        rig.scope.set_tenant_verdict(beta(), ScanCheck::InScanSet);
        drop(rig.write_tenant("beta", 8_192).await);
        yield_rounds().await;
        assert_eq!(rig.budget.in_flight_bytes(), 4_096 + 8_192);

        rig.clock.advance_ns(50_000_000);
        yield_rounds().await;
        rig.clock.advance_ns(10_000_000);
        yield_rounds().await;
        assert_eq!(
            rig.budget.in_flight_bytes(),
            4_096,
            "beta is published within one tick of being due; acme is held"
        );
        assert_eq!(rig.own_commits().await, 1);
        let snap = rig.metrics.snapshot();
        assert_eq!(snap.abandoned_stale_provisioning, 0);
        assert_eq!(rig.scope.reads(), 1, "the later triggers joined the read");
        end_past_hanging_reads(rig).await;
    }

    /// A spent stale-view hold whose provisioning read never answers does not
    /// stop the actor taking its mailbox: twice its capacity of writes is
    /// queued without waiting while the age trigger keeps finding the hold
    /// spent.
    ///
    /// Guard: as for
    /// [`a_hanging_read_on_a_spent_hold_does_not_delay_another_tenant`].
    /// An actor parked on the read stops draining the mailbox, and the 65th
    /// write finds it full.
    #[tokio::test]
    async fn a_hanging_read_on_a_spent_hold_does_not_fill_the_mailbox() {
        let rig = spent_age_hold(ScriptedRead::Hangs).await;
        rig.scope.set_tenant_verdict(beta(), ScanCheck::InScanSet);
        for i in 0..2 * rig.tx.max_capacity() {
            assert!(rig.try_write_tenant("beta"), "write {i} found room");
            rig.clock.advance_ns(1_000_000);
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
        }
        assert_eq!(rig.metrics.snapshot().abandoned_stale_provisioning, 0);
        assert_eq!(rig.budget.in_flight_bytes(), 4_096, "acme is still held");
        end_past_hanging_reads(rig).await;
    }

    /// A read requested once the hold is spent that confirms the view opens
    /// the held flush on the next trigger, and nothing is dropped, though a
    /// read issued before the spend had failed.
    ///
    /// Guard: the rule that only a failed read issued at or after the spend
    /// drops the buffer. Dropping at the spend (the rule before #4363's
    /// re-read), or on any failed read of the tenant including the earlier
    /// one, drops the buffer the store now vouches for.
    #[tokio::test]
    async fn a_read_after_the_spend_that_confirms_the_view_publishes() {
        use crate::generation::FlushScope;
        let rig = HandBackRig::with(ScanCheck::Unknown, None, stale_ticking_config());
        drop(rig.write(4_096).await);
        assert!(
            tick_until(&rig.clock, || rig
                .metrics
                .snapshot()
                .stale_provisioning_flushes
                == 1)
            .await
        );
        let acme = TenantId::new("acme").hash();
        assert!(rig.scope.request_read(acme).is_some(), "a read fails early");
        assert!(rig.scope.failed_read_at(acme).is_some());
        rig.scope
            .set_read(ScriptedRead::Installs(ScanCheck::InScanSet));
        rig.clock.advance_ns(STALE_BOUND_NS);
        yield_rounds().await;
        assert!(
            tick_until(&rig.clock, || rig.budget.in_flight_bytes() == 0).await,
            "the held flush lands"
        );
        assert_eq!(rig.scope.reads(), 2, "one read after the spend");
        assert_eq!(rig.metrics.snapshot().abandoned_stale_provisioning, 0);
        assert_eq!(rig.own_commits().await, 1);
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A spent hold is dropped on the first trigger after a read issued at
    /// the spend failed, once: the strict waiter gets the stale-view
    /// `Abandoned`, every byte is refunded, nothing is written, and the
    /// counter stays at 1 on later ticks.
    ///
    /// Guard: the `failed >= issued` drop in `StaleViewHold::step`. Never
    /// dropping on it holds the buffer until the deadline a bound later;
    /// dropping at the spend without a read requests none, so `reads` is 0 at
    /// the drop; checking for the failure only after requesting the read, on
    /// the same trigger, drops at the spend.
    #[tokio::test]
    async fn a_spent_hold_drops_once_a_read_after_the_spend_fails() {
        let rig = HandBackRig::with(ScanCheck::Unknown, None, stale_ticking_config());
        rig.scope.set_read(ScriptedRead::Fails);
        let (charge, mut answer) = rig.write_strict(4_096).await;
        drop(charge);
        assert!(
            tick_until(&rig.clock, || rig
                .metrics
                .snapshot()
                .stale_provisioning_flushes
                == 1)
            .await
        );
        rig.clock.advance_ns(STALE_BOUND_NS);
        yield_rounds().await;
        assert_eq!(rig.scope.reads(), 1, "the spend requested one read");
        assert_eq!(
            rig.metrics.snapshot().abandoned_stale_provisioning,
            0,
            "the spending trigger keeps the buffer"
        );
        assert!(matches!(
            answer.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        rig.clock.advance_ns(10_000_000);
        yield_rounds().await;
        assert_eq!(
            rig.metrics.snapshot().abandoned_stale_provisioning,
            1,
            "the next trigger finds the read failed"
        );
        match answer.try_recv() {
            Ok(Err(WriteError::Abandoned(msg))) => assert_eq!(msg, STALE_VIEW_ABANDONED),
            other => panic!("expected the stale-view Abandoned, got {other:?}"),
        }
        assert_eq!(rig.budget.in_flight_bytes(), 0, "the bytes are refunded");
        assert!(
            !tick_until(&rig.clock, || rig
                .metrics
                .snapshot()
                .abandoned_stale_provisioning
                > 1)
            .await
        );
        assert_eq!(rig.scope.reads(), 1);
        assert_eq!(rig.own_commits().await, 0, "nothing is written");
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A read requested inside the read backoff window counts as not yet
    /// issued: the spent hold is kept across every trigger in the window,
    /// though an earlier read had failed, and is dropped only after the
    /// window ends and the read issued then fails.
    ///
    /// Guard: `request_read` returning `None` in the backoff window and the
    /// hold asking again on the next trigger. Treating a backed-off request
    /// as a failed read, or counting the failure from before the spend,
    /// drops the buffer inside the window; asking only once, at the spend,
    /// never drops it before the deadline.
    #[tokio::test]
    async fn a_backed_off_read_drops_nothing_until_a_later_read_fails() {
        use crate::generation::FlushScope;
        let rig = HandBackRig::with(ScanCheck::Unknown, None, stale_ticking_config());
        drop(rig.write(4_096).await);
        assert!(
            tick_until(&rig.clock, || rig
                .metrics
                .snapshot()
                .stale_provisioning_flushes
                == 1)
            .await
        );
        let acme = TenantId::new("acme").hash();
        assert!(rig.scope.request_read(acme).is_some(), "a read fails early");
        rig.scope.set_read(ScriptedRead::BackedOff);
        rig.clock.advance_ns(STALE_BOUND_NS);
        yield_rounds().await;
        assert!(
            !tick_until(&rig.clock, || rig
                .metrics
                .snapshot()
                .abandoned_stale_provisioning
                > 0)
            .await,
            "held through the backoff window"
        );
        assert_eq!(rig.budget.in_flight_bytes(), 4_096);
        assert_eq!(rig.scope.reads(), 1, "only the early read was issued");

        rig.scope.set_read(ScriptedRead::Fails);
        assert!(
            tick_until(&rig.clock, || rig
                .metrics
                .snapshot()
                .abandoned_stale_provisioning
                == 1)
            .await,
            "dropped once the read after the window fails"
        );
        assert_eq!(rig.scope.reads(), 2);
        assert_eq!(rig.budget.in_flight_bytes(), 0);
        assert_eq!(rig.own_commits().await, 0);
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A spent hold whose read never answers is kept until one bound after
    /// the spend, then dropped.
    ///
    /// Guard: the `now - spent_at >= bound` deadline in
    /// `StaleViewHold::step`. Without it the buffer is held for as long as
    /// the read hangs; counting the deadline from the hold's start instead of
    /// the spend drops it at the spend.
    #[tokio::test]
    async fn a_spent_hold_with_a_hanging_read_drops_at_its_deadline() {
        let rig = spent_age_hold(ScriptedRead::Hangs).await;
        assert_eq!(rig.metrics.snapshot().abandoned_stale_provisioning, 0);
        rig.clock.advance_ns(STALE_BOUND_NS - 10_000_000);
        yield_rounds().await;
        assert_eq!(
            rig.metrics.snapshot().abandoned_stale_provisioning,
            0,
            "held one tick inside the deadline"
        );
        assert_eq!(rig.budget.in_flight_bytes(), 4_096);
        rig.clock.advance_ns(10_000_000);
        yield_rounds().await;
        assert_eq!(
            rig.metrics.snapshot().abandoned_stale_provisioning,
            1,
            "dropped at the deadline"
        );
        assert_eq!(rig.budget.in_flight_bytes(), 0);
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A rig with no age trigger inside the test, so only drains open
    /// flushes, whose `acme` buffer a drain has started holding on an
    /// untrusted view.
    async fn drain_hold() -> HandBackRig {
        let config = IngestConfig {
            max_flush_delay: Duration::from_secs(3600),
            max_flush_delay_idle: Duration::from_secs(3600),
            ..stale_view_config()
        };
        let rig = HandBackRig::with(ScanCheck::Unknown, None, config);
        drop(rig.write(4_096).await);
        assert_eq!(unknown_flush(&rig).await, 0, "the hold begins");
        rig
    }

    /// Sends a drain and returns its answer, without waiting for it.
    async fn send_drain(rig: &HandBackRig) -> oneshot::Receiver<()> {
        let (done, answered) = oneshot::channel();
        rig.tx
            .send(ShardMsg::FlushNow { done })
            .await
            .expect("send FlushNow");
        answered
    }

    fn unanswered(answered: &mut oneshot::Receiver<()>) -> bool {
        matches!(
            answered.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        )
    }

    /// A drain still waits on the store for a spent hold, for exactly
    /// `max_flush_lifetime`. A read that hangs through that whole wait leaves
    /// no time before the spend's deadline, so the drain then drops the
    /// buffer, once.
    ///
    /// Guard: the drain-only `reread_and_check` in `scan_check`, reached
    /// while the hold is inside its deadline. Skipping the drain's wait keeps
    /// the buffer and answers the drain before the clock moves; waiting with
    /// no bound, or past the bound, leaves it unanswered at the bound; a
    /// shorter wait answers it a nanosecond early.
    #[tokio::test]
    async fn a_drain_waits_its_bound_on_a_spent_hold_with_a_hanging_read() {
        let rig = drain_hold().await;
        rig.scope.set_read(ScriptedRead::Hangs);
        let rereads = rig.scope.rereads();
        rig.clock.advance_ns(STALE_BOUND_NS);

        let mut answered = send_drain(&rig).await;
        assert!(
            settles(|| rig.scope.rereads() > rereads).await,
            "the drain re-reads"
        );
        yield_rounds().await;
        assert!(unanswered(&mut answered), "the drain waits on its re-read");
        rig.clock.advance_ns(STALE_BOUND_NS - 1);
        yield_rounds().await;
        assert!(unanswered(&mut answered), "a nanosecond inside its bound");
        assert_eq!(rig.metrics.snapshot().abandoned_stale_provisioning, 0);

        rig.clock.advance_ns(1);
        assert!(
            settles(|| !unanswered(&mut answered)).await,
            "the wait ends at its bound"
        );
        assert_eq!(rig.scope.rereads(), rereads + 1, "one wait");
        assert_eq!(rig.metrics.snapshot().abandoned_stale_provisioning, 1);
        assert_eq!(rig.budget.in_flight_bytes(), 0);
        assert_eq!(rig.own_commits().await, 0);
        end_past_hanging_reads(rig).await;
    }

    /// A drain that finds a spent hold whose read is backed off keeps the
    /// buffer, and a drain once the spend's deadline is gone drops it without
    /// waiting on the store again.
    ///
    /// Guard: the `HoldState::Drop` short-cut before `reread_and_check` in
    /// `scan_check`. Waiting on the re-read past the deadline leaves the
    /// second drain unanswered while the read hangs. Treating the backed-off
    /// request as a failed read drops the buffer at the first drain.
    #[tokio::test]
    async fn a_drain_past_a_spent_holds_deadline_drops_without_waiting() {
        let rig = drain_hold().await;
        rig.scope.set_read(ScriptedRead::BackedOff);
        rig.clock.advance_ns(STALE_BOUND_NS);
        rig.flush_now().await;
        assert_eq!(rig.metrics.snapshot().abandoned_stale_provisioning, 0);
        assert_eq!(rig.budget.in_flight_bytes(), 4_096, "the drain kept it");

        rig.scope.set_read(ScriptedRead::Hangs);
        rig.clock.advance_ns(STALE_BOUND_NS);
        let rereads = rig.scope.rereads();
        let mut answered = send_drain(&rig).await;
        assert!(
            settles(|| !unanswered(&mut answered)).await,
            "the drain is answered without the clock moving"
        );
        assert_eq!(rig.scope.rereads(), rereads, "no wait");
        assert_eq!(rig.metrics.snapshot().abandoned_stale_provisioning, 1);
        assert_eq!(rig.budget.in_flight_bytes(), 0);
        end_past_hanging_reads(rig).await;
    }

    /// One tenant's in-place WARN does not silence another's: two tenants
    /// that each write in place in the same hour log one WARN each.
    ///
    /// Guard: the tenant in `InPlaceWarnings::should_warn`'s key: keyed
    /// without it, as one entry per shard, the second tenant logs nothing.
    #[tokio::test]
    async fn in_place_mismatch_warnings_are_kept_per_tenant() {
        let warns = crate::generation::WarnCapture::default();
        let _capture = warns.install();
        let rig = HandBackRig::with(MISMATCH, None, spent_at_once_config());
        drop(rig.write_tenant("acme", 4_096).await);
        rig.flush_now().await;
        drop(rig.write_tenant("beta", 4_096).await);
        rig.flush_now().await;
        assert_eq!(
            rig.metrics.snapshot().generation_mismatch_written_in_place,
            2
        );
        assert_eq!(warns.count(DEAD_TARGET_WARN), 2, "one WARN per tenant");
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A tenant that warned for a dead target and then writes in place
    /// because its retry is spent, in the same hour, warns again with the
    /// retry-spent text.
    ///
    /// Guard: the cause in `InPlaceWarnings::should_warn`'s key: with one
    /// key for both causes the retry-spent write logs nothing.
    #[tokio::test]
    async fn a_second_in_place_cause_in_the_same_hour_warns_again() {
        let warns = crate::generation::WarnCapture::default();
        let _capture = warns.install();
        let rig = HandBackRig::with(MISMATCH, None, spent_at_once_config());
        rig.scope.set_wait(false);
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(warns.count(DEAD_TARGET_WARN), 1);

        let (target, _inbox) = full_target();
        rig.scope.set(MISMATCH, Some(target));
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        let snap = rig.metrics.snapshot();
        assert_eq!(snap.hand_back_failures, 1, "retried once");
        assert_eq!(snap.generation_mismatch_written_in_place, 2);
        assert_eq!(warns.count(RETRY_SPENT_WARN), 1, "the second cause warns");
        assert_eq!(warns.count(DEAD_TARGET_WARN), 1);
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A tenant that warned for a cause in one ingest hour warns again for
    /// the same cause in the next hour, with no in-scan flush between.
    ///
    /// Guard: the hour in `InPlaceWarnings::should_warn`'s comparison
    /// `self.last_warned.get(&key) == Some(&ingest_hour)`: compared on the
    /// key's presence alone, the next hour's write logs nothing.
    #[tokio::test]
    async fn an_in_place_write_in_the_next_hour_warns_again() {
        let warns = crate::generation::WarnCapture::default();
        let _capture = warns.install();
        let rig = HandBackRig::with(MISMATCH, None, spent_at_once_config());
        for _ in 0..2 {
            drop(rig.write(4_096).await);
            rig.flush_now().await;
        }
        assert_eq!(warns.count(DEAD_TARGET_WARN), 1, "one WARN for the hour");

        rig.clock.advance_ns(3_600_000_000_000);
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(
            rig.metrics.snapshot().generation_mismatch_written_in_place,
            3
        );
        assert_eq!(warns.count(DEAD_TARGET_WARN), 2, "the next hour warns");
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A flush that hands every row of the tenant over ends its in-place
    /// episode: the next in-place write, in the same hour, warns again.
    ///
    /// Guard: `self.in_place_warnings.rearm(tenant_hash)` where `handed.kept`
    /// is `None` in `flush_tenant`'s hand-back arm: without it the last write
    /// logs nothing.
    #[tokio::test]
    async fn a_full_hand_over_ends_an_in_place_episode() {
        let warns = crate::generation::WarnCapture::default();
        let _capture = warns.install();
        let rig = HandBackRig::with(MISMATCH, None, spent_at_once_config());
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(warns.count(DEAD_TARGET_WARN), 1);

        let (target, _inbox) = mpsc::channel(64);
        rig.scope.set(MISMATCH, Some(target));
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(rig.metrics.snapshot().rerouted_flushes, 1);

        rig.scope.set(MISMATCH, None);
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(
            rig.metrics.snapshot().generation_mismatch_written_in_place,
            2
        );
        assert_eq!(
            warns.count(DEAD_TARGET_WARN),
            2,
            "the full hand-over ended the episode"
        );
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A flush that hands some rows over and writes the rest in place, its
    /// other target closed, stays in the hand-back run: the next hand-back
    /// logs no second WARN.
    ///
    /// Guard: `if !handed.delivered` around `self.handing_back = false` in
    /// the in-place branch of `flush_tenant`'s hand-back arm: unconditional,
    /// the partial flush ends the run and the last hand-back warns again.
    #[tokio::test]
    async fn a_partial_hand_over_written_in_place_stays_in_the_hand_back_run() {
        let warns = crate::generation::WarnCapture::default();
        let _capture = warns.install();
        let rig = HandBackRig::with(MISMATCH, None, spent_at_once_config());
        let (live, _inbox) = mpsc::channel(64);
        let (closed, gone) = mpsc::channel(64);
        drop(gone);
        rig.scope.set_split(MISMATCH, vec![live.clone(), closed]);
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        let snap = rig.metrics.snapshot();
        assert_eq!(snap.rerouted_flushes, 1, "one target took its rows");
        assert_eq!(snap.generation_mismatch_written_in_place, 1);
        assert_eq!(warns.count(HAND_BACK_WARN), 1);
        assert_eq!(warns.count(DEAD_TARGET_WARN), 1);

        rig.scope.set(HAND_BACK, Some(live));
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(rig.metrics.snapshot().rerouted_flushes, 2);
        assert_eq!(warns.count(HAND_BACK_WARN), 1, "still one run");
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// A retry-spent in-place write ends a hand-back run: the next hand-back
    /// warns again.
    ///
    /// Guard: `self.handing_back = false` in `flush_tenant`'s
    /// `mismatch_retry_spent(..)` arm: without it the last hand-back logs
    /// nothing.
    #[tokio::test]
    async fn a_retry_spent_write_ends_a_hand_back_run() {
        let warns = crate::generation::WarnCapture::default();
        let _capture = warns.install();
        let (live, _inbox) = mpsc::channel(64);
        let rig = HandBackRig::with(HAND_BACK, Some(live.clone()), spent_at_once_config());
        rig.scope.set_wait(false);
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(warns.count(HAND_BACK_WARN), 1);

        let (full, _full_inbox) = full_target();
        rig.scope.set(MISMATCH, Some(full));
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(warns.count(RETRY_SPENT_WARN), 1);

        rig.scope.set(HAND_BACK, Some(live));
        drop(rig.write(4_096).await);
        rig.flush_now().await;
        assert_eq!(rig.metrics.snapshot().rerouted_flushes, 2);
        assert_eq!(
            warns.count(HAND_BACK_WARN),
            2,
            "the retry-spent write ended the run"
        );
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// The in-place WARN state stays within its bound. An entry over the
    /// bound first drops every entry older than the hour being recorded,
    /// keeping the current hour's, and clears the rest only when that is not
    /// enough; a repeat in a kept hour still logs nothing.
    ///
    /// Guards: the `self.last_warned.len() > IN_PLACE_WARN_ENTRIES_MAX` block
    /// in `InPlaceWarnings::should_warn`: without it the map holds every
    /// tenant and the length assertions fail. And its `retain(..)`: without
    /// it the stale-hour case clears the map and the current hour's tenants
    /// warn again. And the insert after its `clear()`: without it the entry
    /// that overflowed the map is not recorded and warns again.
    #[test]
    fn in_place_warnings_stay_within_their_bound() {
        let tenant = |i: usize| TenantId::new(format!("tenant-{i}")).hash();
        let mut warnings = InPlaceWarnings::default();
        for i in 0..IN_PLACE_WARN_ENTRIES_MAX + 10 {
            assert!(warnings.should_warn(tenant(i), InPlaceCause::DeadTarget, 7));
            assert!(warnings.len() <= IN_PLACE_WARN_ENTRIES_MAX);
        }
        assert_eq!(warnings.len(), 10);
        assert!(!warnings.should_warn(
            tenant(IN_PLACE_WARN_ENTRIES_MAX),
            InPlaceCause::DeadTarget,
            7
        ));

        let half = IN_PLACE_WARN_ENTRIES_MAX / 2;
        let mut warnings = InPlaceWarnings::default();
        for i in 0..half {
            assert!(warnings.should_warn(tenant(i), InPlaceCause::DeadTarget, 7));
        }
        for i in half..IN_PLACE_WARN_ENTRIES_MAX {
            assert!(warnings.should_warn(tenant(i), InPlaceCause::RetrySpent, 8));
        }
        assert_eq!(warnings.len(), IN_PLACE_WARN_ENTRIES_MAX);
        assert!(warnings.should_warn(tenant(usize::MAX), InPlaceCause::RetrySpent, 8));
        assert_eq!(warnings.len(), IN_PLACE_WARN_ENTRIES_MAX - half + 1);
        assert!(!warnings.should_warn(tenant(half), InPlaceCause::RetrySpent, 8));
        assert!(warnings.should_warn(tenant(0), InPlaceCause::DeadTarget, 7));
    }

    /// A `max_flush_delay` past `i64::MAX` nanoseconds gives a buffer with a
    /// waiter an age threshold of `i64::MAX`, with the adaptive corridor off
    /// and on, and such a buffer is never flushed by age. Cast with `as i64`
    /// the delay wraps to -1, which every buffer age exceeds, so the buffer
    /// is flushed on the first tick. A `put_retry_base_delay` past `i64::MAX`
    /// nanoseconds leaves the adaptive ceiling at the floor; cast, it wraps to
    /// -1 and widens the ceiling by a nanosecond instead of collapsing it.
    ///
    /// Guards: `duration_nanos_saturating(self.config.max_flush_delay)` in
    /// `age_threshold_ns`: cast, the thresholds read -1 and the buffer is
    /// flushed. And `duration_nanos_saturating(self.config.put_retry_base_delay)`
    /// there: cast, the trickle tenant's threshold is the budget plus one
    /// nanosecond, on the adaptive trigger.
    #[tokio::test]
    async fn a_max_flush_delay_past_i64_nanos_never_ages_a_waiting_buffer() {
        let past = Duration::from_nanos(u64::MAX);
        let config = IngestConfig {
            max_flush_delay: past,
            ..ticking_config()
        };
        let rig = HandBackRig::with(ScanCheck::InScanSet, None, config);
        let (charge, mut answer) = rig.write_strict(4_096).await;
        drop(charge);
        assert!(
            settles(|| rig.tx.capacity() == rig.tx.max_capacity()).await,
            "the actor takes the strict write"
        );
        assert!(
            !tick_until(&rig.clock, || rig.metrics.snapshot().flushes_by_age > 0).await,
            "a waiting buffer is not due by age"
        );
        assert!(matches!(
            answer.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(rig.own_commits().await, 0);
        drop(rig.tx);
        rig.task.await.expect("actor ends");

        let mut buf = TenantBuf::default();
        buf.waiters.push(oneshot::channel().0);
        let fixed = HandBackRig::idle_actor(config);
        assert_eq!(fixed.age_threshold_ns(&buf), (i64::MAX, FlushTrigger::Age));
        let adaptive = HandBackRig::idle_actor(IngestConfig {
            adaptive_flush_delay: true,
            ..config
        });
        adaptive.rtt.record(0);
        assert_eq!(
            adaptive.age_threshold_ns(&buf),
            (i64::MAX, FlushTrigger::Age)
        );

        let wide_retry = HandBackRig::idle_actor(IngestConfig {
            adaptive_flush_delay: true,
            put_retry_base_delay: past,
            ..ticking_config()
        });
        wide_retry.rtt.record(0);
        buf.avg_gap_ns = i64::MAX;
        assert_eq!(
            wide_retry.age_threshold_ns(&buf),
            (50_000_000, FlushTrigger::Age),
            "no retry headroom is left under the budget, so the ceiling is the floor"
        );
    }

    /// A `max_flush_lifetime` past `i64::MAX` nanoseconds is an unbounded
    /// lifetime: the flush is written and its strict waiter answered `Ok`.
    /// Cast with `as i64` the lifetime wraps to -1, so every deadline built
    /// from it is already past and the flush is abandoned.
    ///
    /// Guards: `duration_nanos_saturating(self.config.max_flush_lifetime)` in
    /// `flush_tenant`'s flush-open deadline: cast, the flush is abandoned in
    /// the queue and `abandoned_queue_deadline` reads 1. And
    /// `duration_nanos_saturating(ctx.config.max_flush_lifetime)` in the
    /// deadline re-derived at the permit grant: cast, the data PUT is never
    /// attempted and `abandoned_retry_exhausted` reads 1.
    #[tokio::test]
    async fn a_max_flush_lifetime_past_i64_nanos_never_abandons_a_flush() {
        let config = IngestConfig {
            shard_count: 4,
            max_flush_lifetime: Duration::from_nanos(u64::MAX),
            ..IngestConfig::default()
        };
        let rig = HandBackRig::with(ScanCheck::InScanSet, None, config);
        let (charge, answer) = rig.write_strict(4_096).await;
        drop(charge);

        rig.flush_now().await;
        let snap = rig.metrics.snapshot();
        assert_eq!(snap.abandoned_queue_deadline, 0);
        assert_eq!(snap.abandoned_retry_exhausted, 0);
        answer
            .await
            .expect("waiter answered")
            .expect("the flush is written");
        assert_eq!(rig.own_commits().await, 1);
        assert_eq!(rig.own_samples().await, 6, "every row, once");
        drop(rig.tx);
        rig.task.await.expect("actor ends");
    }

    /// One permit, so tenant `parked`'s held flush leaves tenant `late`'s
    /// flush queued on the shard semaphore (`late` has nothing in flight, so
    /// neither its share nor the queue cap refuses it). Every flush opens in
    /// the hour that ends at `BOUNDARY_NS`.
    struct GrantRig {
        fault: Arc<FaultStore<MemoryStore>>,
        router: Arc<IngestRouter>,
        clock: Arc<TestClock>,
        budget: Arc<IngestByteBudget>,
        parked: TenantId,
        late: TenantId,
        parked_gate: GateHandle,
        late_gate: GateHandle,
    }

    impl GrantRig {
        /// `late`'s every PUT is held, so `held_for(late_gate, late)` counts
        /// the PUTs it attempted.
        async fn new(lifetime: Duration) -> (Self, ParkedWrites) {
            let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
            let store: Arc<dyn ObjectStoreBackend> = fault.clone();
            let clock = TestClock::new(BASE_NS);
            let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1 << 30));
            let router = Arc::new(
                IngestRouter::new(
                    IngestConfig {
                        max_queued_flushes: 4,
                        ..one_permit_one_queue(lifetime)
                    },
                    store,
                    Signal::Metrics,
                    clock.clone(),
                    Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
                )
                .with_budget(Arc::clone(&budget)),
            );
            let parked = TenantId::new("parked");
            let late = TenantId::new("late");
            let parked_gate = hold_first_put(&fault, &parked);
            let late_gate = fault.hold(
                Op::Put,
                Some(format!("t/{}/", late.hash().to_hex())),
                Occurrence::Always,
            );
            let rig = GrantRig {
                fault,
                router,
                clock,
                budget,
                parked,
                late,
                parked_gate,
                late_gate,
            };

            let first = rig.write(&rig.parked.clone(), WriteMode::Strict);
            until(|| rig.router.metrics().snapshot().buffered_points_total >= 1).await;
            rig.clock.advance_ns(TICK_ADVANCE_NS);
            until(|| in_flight(&rig.router) == 1).await;
            rig.parked_gate.wait_until_held(1).await;

            let queued = rig.write(&rig.late.clone(), WriteMode::Strict);
            until(|| rig.router.metrics().snapshot().buffered_points_total >= 2).await;
            rig.clock.advance_ns(TICK_ADVANCE_NS);
            until(|| in_flight(&rig.router) == 2).await;
            assert!(
                rig.clock.now_ns() < BOUNDARY_NS,
                "both flushes open, and pin, in the hour ending at BOUNDARY_NS"
            );
            (rig, ParkedWrites { first, queued })
        }

        fn write(
            &self,
            tenant: &TenantId,
            mode: WriteMode,
        ) -> tokio::task::JoinHandle<Result<crate::router::WriteReceipt, WriteError>> {
            let router = Arc::clone(&self.router);
            let tenant = tenant.clone();
            tokio::spawn(async move {
                let points = vec![point(&tenant, "h")];
                router
                    .write(tenant, points, mode, Duration::from_secs(60))
                    .await
            })
        }

        /// Releases every held call, now and from now on, and drains.
        async fn drain(self) {
            let gates = [self.parked_gate.clone(), self.late_gate.clone()];
            let releaser = tokio::spawn(async move {
                loop {
                    for gate in &gates {
                        for id in gate.held() {
                            gate.release(id);
                        }
                    }
                    tokio::task::yield_now().await;
                }
            });
            self.router.flush_all().await;
            releaser.abort();
            drop(self.fault);
        }
    }

    /// Held calls under `tenant`'s prefix. A gate's view is the whole store's
    /// held set, and an abandoned call stays in it.
    fn held_for(gate: &GateHandle, tenant: &TenantId) -> usize {
        let hex = tenant.hash().to_hex();
        gate.held_details()
            .into_iter()
            .filter(|(_, _, key)| key.contains(&hex))
            .count()
    }

    struct ParkedWrites {
        first: tokio::task::JoinHandle<Result<crate::router::WriteReceipt, WriteError>>,
        queued: tokio::task::JoinHandle<Result<crate::router::WriteReceipt, WriteError>>,
    }

    /// A flush whose permit is granted at or past `end(hour) +
    /// max_flush_lifetime` for the hour it pinned at flush-open is abandoned
    /// before any PUT and counted under `abandoned_hour_bound` (ADR-2708 D3).
    /// Re-pinning it to the grant's hour instead would PUT it into that later
    /// hour, which `late`'s held-PUT count and the store both show.
    #[tokio::test]
    async fn a_flush_granted_past_its_pinned_hours_lifetime_is_abandoned_without_a_put() {
        let lifetime = Duration::from_secs(3600);
        let (rig, writes) = GrantRig::new(lifetime).await;
        let objects_before = list_all(rig.fault.as_ref(), "t/")
            .await
            .expect("list")
            .len();

        // Past the hour bound. `parked`'s own deadline passes too, so its held
        // PUT is abandoned and the permit goes to `late`, too late.
        let hour_bound_ns = BOUNDARY_NS + lifetime.as_nanos() as i64;
        rig.clock
            .advance_ns(hour_bound_ns + 1_000_000_000 - rig.clock.now_ns());
        until(|| writes.queued.is_finished()).await;

        let snap = rig.router.metrics().snapshot();
        assert_eq!(snap.abandoned_hour_bound, 1);
        assert_eq!(
            held_for(&rig.late_gate, &rig.late),
            0,
            "the flush granted past its hour's bound attempted no PUT"
        );
        assert_eq!(
            list_all(rig.fault.as_ref(), "t/")
                .await
                .expect("list")
                .len(),
            objects_before,
            "no object was written into the sealed hour or a later one"
        );
        let answer = writes.queued.await.expect("write task");
        assert!(
            matches!(answer, Err(WriteError::Abandoned(_))),
            "the strict waiter is answered Abandoned: {answer:?}"
        );
        let _ = writes.first.await;
        rig.drain().await;
    }

    /// A flush granted after its pinned hour ends but before that hour's bound
    /// runs until the bound, not for a full `max_flush_lifetime` from the
    /// grant: its held PUT is abandoned one second past `end(hour) +
    /// max_flush_lifetime`, ten minutes before `grant + max_flush_lifetime`.
    #[tokio::test]
    async fn a_flush_granted_inside_the_hour_is_clamped_to_the_hour_end() {
        let lifetime = Duration::from_secs(7200);
        let (rig, writes) = GrantRig::new(lifetime).await;

        // Grant `late` its permit 600 s past the hour's end, well inside
        // `parked`'s own deadline, by releasing `parked`'s PUT there.
        let grant_ns = BOUNDARY_NS + 600 * 1_000_000_000;
        rig.clock.advance_ns(grant_ns - rig.clock.now_ns());
        release_tenant(&rig.parked_gate, &rig.parked);
        until(|| held_for(&rig.late_gate, &rig.late) == 1).await;
        assert!(
            !writes.queued.is_finished(),
            "late's PUT is held, inside its deadline"
        );

        let hour_bound_ns = BOUNDARY_NS + lifetime.as_nanos() as i64;
        rig.clock
            .advance_ns(hour_bound_ns + 1_000_000_000 - rig.clock.now_ns());
        assert!(rig.clock.now_ns() < grant_ns + lifetime.as_nanos() as i64);
        assert!(
            settles(|| writes.queued.is_finished()).await,
            "the held PUT is abandoned at the hour bound, not at grant + lifetime"
        );
        let answer = writes.queued.await.expect("write task");
        assert!(
            answer.is_err(),
            "late's strict write is not acked: {answer:?}"
        );
        assert_eq!(
            rig.router.metrics().snapshot().abandoned_hour_bound,
            0,
            "granted inside the bound, so not abandoned at the grant"
        );
        writes
            .first
            .await
            .expect("write task")
            .expect("parked's flush lands once released");
        rig.drain().await;
    }

    /// A flush queued on the permit still holds its buffer's byte charge: the
    /// budget is released when the flush finishes, not when it is spawned.
    #[tokio::test]
    async fn a_flush_parked_on_the_permit_still_holds_its_charge() {
        let (rig, writes) = GrantRig::new(Duration::from_secs(3600)).await;
        let charged = rig.budget.in_flight_bytes();
        assert!(charged > 0);
        for _ in 0..1_000 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            rig.budget.in_flight_bytes(),
            charged,
            "late's flush is parked on the permit and its charge is still held"
        );
        assert_eq!(in_flight(&rig.router), 2);

        release_tenant(&rig.parked_gate, &rig.parked);
        until(|| held_for(&rig.late_gate, &rig.late) == 1).await;
        assert!(
            rig.budget.in_flight_bytes() > 0,
            "late's charge outlives the grant"
        );
        let budget = Arc::clone(&rig.budget);
        rig.drain().await;
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "every charge is refunded once drained"
        );
        let _ = writes.first.await;
        let _ = writes.queued.await;
    }

    fn histogram_point(tenant: &TenantId, metric: &str, ts_ns: i64) -> IngestPoint {
        let labels = LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: metric.to_string(),
        }])
        .expect("one label");
        let series_id = SeriesId::compute(tenant, metric, &labels).expect("series id");
        IngestPoint {
            series_id,
            labels: Arc::new(labels),
            value: IngestValue::Histogram(ravel_segment::HistogramSample {
                ts_ns,
                value: ravel_segment::HistogramValue {
                    scale: 2,
                    zero_threshold: 1e-9,
                    sum: Some(42.5),
                    custom_values: None,
                    positive_spans: vec![ravel_segment::HistogramSpan {
                        offset: 0,
                        length: 3,
                    }],
                    negative_spans: vec![],
                    counts: ravel_segment::HistogramCounts::Int {
                        zero_count: 1,
                        count: 7,
                        positive: vec![2, 3, 1],
                        negative: vec![],
                    },
                    reset_hint: ravel_segment::ResetHint::Yes,
                },
            }),
        }
    }

    /// The object estimate a batch adds to an empty buffer: every sample,
    /// plus each series once.
    fn batch_object_bytes(points: &[IngestPoint]) -> usize {
        let mut seen = HashSet::new();
        points
            .iter()
            .map(|p| {
                let series = if seen.insert(p.series_id) {
                    p.est_object_series_bytes()
                } else {
                    0
                };
                p.est_object_sample_bytes() + series
            })
            .sum()
    }

    /// A native histogram is charged 16 bytes of buffered memory per sample
    /// but carries several times that in the object, so a buffer deferred at
    /// its flush share never reaches the memory backstop while its object
    /// estimate grows. The object backstop spawns it, past the share, once
    /// the estimate reaches four times `target_bytes`.
    #[tokio::test]
    async fn a_deferred_native_histogram_buffer_spawns_at_the_object_backstop() {
        const TARGET: usize = 4096;
        const BATCH: usize = 20;
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        let clock = TestClock::new(BASE_NS);
        let config = IngestConfig {
            shard_count: 1,
            target_bytes: TARGET,
            max_flush_delay: Duration::from_millis(50),
            max_flush_delay_idle: Duration::from_millis(50),
            flush_tick: Duration::from_millis(10),
            max_inflight_flushes: 1,
            max_queued_flushes: 8,
            ..IngestConfig::default()
        };
        assert_eq!(config.flush_share_per_tenant(), 1);
        let router = Arc::new(
            IngestRouter::new(
                config,
                store,
                Signal::Metrics,
                clock.clone(),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
            .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)),
        );
        let tenant = TenantId::new("histograms");
        let gate = fault.hold(
            Op::Put,
            Some(format!("t/{}/", tenant.hash().to_hex())),
            Occurrence::Always,
        );
        let write = |points: Vec<IngestPoint>| {
            let router = Arc::clone(&router);
            let tenant = tenant.clone();
            tokio::spawn(async move {
                router
                    .write_values(tenant, points, WriteMode::Buffered, Duration::from_secs(60))
                    .await
            })
        };

        // A size-triggered first flush fills the tenant's share and parks.
        let first: Vec<_> = (0..100)
            .map(|i| histogram_point(&tenant, "first", 1_000 + i))
            .collect();
        let first_bytes = batch_object_bytes(&first);
        assert!((TARGET..4 * TARGET).contains(&first_bytes));
        write(first)
            .await
            .expect("write task")
            .expect("buffered ack");
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| in_flight(&router) == 1).await;
        gate.wait_until_held(1).await;

        // Batches of the same size, each over `target_bytes` together with
        // what is already buffered, until the second flush spawns.
        let mut buffered = 0;
        let mut batch_bytes = 0;
        let mut spawned = false;
        for b in 0..200 {
            let batch: Vec<_> = (0..BATCH)
                .map(|i| histogram_point(&tenant, &format!("s{b}"), 1_000 + i as i64))
                .collect();
            batch_bytes = batch_object_bytes(&batch);
            buffered += batch_bytes;
            write(batch)
                .await
                .expect("write task")
                .expect("buffered ack");
            clock.advance_ns(TICK_ADVANCE_NS);
            if settles(|| in_flight(&router) == 2).await {
                spawned = true;
                break;
            }
        }
        assert!(
            spawned,
            "the deferred buffer never spawned; it holds {buffered} object bytes"
        );
        assert!(
            buffered >= 4 * TARGET && buffered <= 4 * TARGET + batch_bytes,
            "spawned at {buffered} object bytes, want [{}, {}]",
            4 * TARGET,
            4 * TARGET + batch_bytes
        );
        assert!(
            deferred_triggers(&router) >= 1,
            "the size trigger was deferred at the share before the backstop"
        );

        let releaser = tokio::spawn(async move {
            loop {
                for id in gate.held() {
                    gate.release(id);
                }
                tokio::task::yield_now().await;
            }
        });
        router.flush_all().await;
        releaser.abort();
    }

    /// Advances the clock `n` 10 ms ticks, yielding after each.
    async fn ticks(clock: &TestClock, n: usize) {
        for _ in 0..n {
            clock.advance_ns(10_000_000);
            for _ in 0..20 {
                tokio::task::yield_now().await;
            }
        }
    }

    /// The activation policy's clock, set by the test apart from the ingest
    /// clock so a view can age without moving any flush deadline.
    struct CompatTestClock(AtomicI64);

    impl CompatTestClock {
        fn at(ns: i64) -> Arc<Self> {
            Arc::new(CompatTestClock(AtomicI64::new(ns)))
        }

        fn advance_ns(&self, delta_ns: i64) {
            self.0.fetch_add(delta_ns, Ordering::SeqCst);
        }
    }

    impl ravel_catalog::CompatClock for CompatTestClock {
        fn now_unix_ns(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn compat_keys() -> ravel_catalog::CompatKeys {
        ravel_catalog::CompatKeys::from_file_keys(&[[9; 32]]).expect("one key")
    }

    /// A `sys/compat` record at `compat_epoch` naming `rseg` the active RSEG
    /// version, the other families at their writers' versions.
    fn compat_record(
        compat_epoch: u64,
        rseg: u32,
        max_publish_lifetime_ns: i64,
    ) -> ravel_proto::sys::v1::CompatRecord {
        use ravel_catalog::compat::{FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN};
        let family = |name: &str, version: u32| ravel_proto::sys::v1::CompatFamily {
            family: name.to_string(),
            active_write_version: version,
            ..Default::default()
        };
        ravel_proto::sys::v1::CompatRecord {
            compat_epoch,
            families: vec![
                family(FAMILY_RSEG, rseg),
                family(FAMILY_RLOG, FormatFamily::Rlog.writable_version_under(None)),
                family(
                    FAMILY_RSPAN,
                    FormatFamily::Rspan.writable_version_under(None),
                ),
            ],
            max_publish_lifetime_ns,
            ..Default::default()
        }
    }

    async fn provision(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
        ravel_catalog::validate_or_adopt(
            store,
            &tenant.hash(),
            Signal::Metrics,
            1,
            BASE_NS,
            ravel_catalog::AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision");
    }

    /// The `compat_epoch` of `tenant`'s metrics provisioning record in the
    /// store.
    async fn bucket_compat_epoch(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> u64 {
        use prost::Message;
        let key = ravel_catalog::provisioning_key(&tenant.hash(), Signal::Metrics);
        let bytes = store
            .get(&key, GetRange::Full)
            .await
            .expect("provisioning record")
            .data;
        ravel_proto::sys::v1::ProvisioningRecord::decode(bytes.as_ref())
            .expect("decode provisioning record")
            .compat_epoch
    }

    /// Every key under `tenant`'s metrics prefix.
    async fn tenant_keys(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> Vec<String> {
        let prefix = format!(
            "t/{}/{}/",
            tenant.hash().to_hex(),
            Signal::Metrics.key_prefix()
        );
        list_all(store, &prefix)
            .await
            .expect("list")
            .into_iter()
            .map(|meta| meta.key)
            .collect()
    }

    fn objects(keys: &[String]) -> usize {
        keys.iter()
            .filter(|k| k.contains("/l0/") || k.contains("/c/"))
            .count()
    }

    fn refused(router: &IngestRouter) -> crate::compat::CompatRefusalSnapshot {
        router.metrics().snapshot().compat_publish_refused
    }

    /// A one-shard router over `store` with `policy` installed.
    fn compat_router(
        config: IngestConfig,
        store: &Arc<dyn ObjectStoreBackend>,
        clock: &Arc<TestClock>,
        policy: crate::compat::IngestCompatPolicy,
    ) -> Arc<IngestRouter> {
        Arc::new(
            IngestRouter::new(
                config,
                Arc::clone(store),
                Signal::Metrics,
                clock.clone(),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
            .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(
                1 << 30,
            )))
            .with_compat_policy(Some(Arc::new(policy)))
            .expect("max_flush_lifetime within the record's bound"),
        )
    }

    /// `sys/compat` written as `record` and a view that has read it.
    async fn compat_view(
        store: &Arc<dyn ObjectStoreBackend>,
        compat_clock: &Arc<CompatTestClock>,
        record: Option<ravel_proto::sys::v1::CompatRecord>,
    ) -> ravel_catalog::CompatView {
        if let Some(record) = record {
            ravel_catalog::compat::create_compat(store.as_ref(), &compat_keys(), record)
                .await
                .expect("create sys/compat");
        }
        let view =
            ravel_catalog::CompatView::new(Arc::clone(store), compat_keys(), compat_clock.clone());
        view.refresh().await.expect("read sys/compat");
        view
    }

    fn spawn_write(
        router: &Arc<IngestRouter>,
        tenant: &TenantId,
        host: &'static str,
        mode: WriteMode,
    ) -> tokio::task::JoinHandle<Result<crate::router::WriteReceipt, WriteError>> {
        let router = Arc::clone(router);
        let tenant = tenant.clone();
        tokio::spawn(async move {
            let points = vec![point(&tenant, host)];
            router
                .write(tenant, points, mode, Duration::from_secs(60))
                .await
        })
    }

    fn assert_policy_refusal(
        err: &WriteError,
        want: impl Fn(&ravel_catalog::CompatRefusal) -> bool,
    ) {
        assert!(
            matches!(
                err,
                WriteError::CompatRefused(crate::compat::CompatPublishRefusal::Policy {
                    family: FormatFamily::Rseg,
                    refusal,
                }) if want(refusal)
            ),
            "{err:?}"
        );
        assert!(err.is_retryable(), "{err:?}");
    }

    /// ADR-4233 decisions 2 to 4. The test seam lowers the RSEG baseline to
    /// 6 and lets the policy name v8, so the build's v7 is a version above
    /// the baseline and needs the bucket fence, and v8 is a version the
    /// policy allows that this writer cannot encode.
    ///
    /// `other`'s flush is granted v7 and the only permit, and is held at its
    /// data PUT, by which point its bucket already carries the view's epoch:
    /// a fence raised after the first PUT leaves it at 0 there. `acme`'s
    /// flush opens behind it while the view names v7, and the view then moves
    /// RSEG to v8. `other`'s commit PUT is refused as moved off its granted
    /// v7. `acme`'s flush, granted after the move, gets v8 and is refused as
    /// unencodable with no object stored; a version chosen when the flush
    /// opened would have been refused as moved instead.
    #[tokio::test]
    async fn version_chosen_at_permit_grant_and_first_activated_publish_fences_the_bucket() {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let (other, acme) = (TenantId::new("other"), TenantId::new("acme"));
        provision(store.as_ref(), &other).await;
        provision(store.as_ref(), &acme).await;
        let view = compat_view(&store, &compat_clock, Some(compat_record(1, 7, 0))).await;
        let policy = crate::compat::IngestCompatPolicy::new(view.clone(), compat_clock.clone())
            .with_family_versions(FormatFamily::Rseg, vec![7, 8], 6);
        let router = compat_router(
            one_permit_one_queue(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );
        let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

        let first = spawn_write(&router, &other, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        gate.wait_until_held(1).await;
        assert_eq!(
            bucket_compat_epoch(store.as_ref(), &other).await,
            1,
            "the bucket is fenced to the view's epoch before the data PUT"
        );
        assert_eq!(bucket_compat_epoch(store.as_ref(), &acme).await, 0);

        let second = spawn_write(&router, &acme, "h1", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 2).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| in_flight(&router) == 2).await;
        ravel_catalog::compat::activate_family(
            store.as_ref(),
            &compat_keys(),
            ravel_catalog::compat::FAMILY_RSEG,
            8,
            "test",
            BASE_NS,
        )
        .await
        .expect("activate v8");
        view.refresh().await.expect("read sys/compat");

        for id in gate.held() {
            assert!(gate.release(id));
        }
        let err = first
            .await
            .expect("first write task")
            .expect_err("the view moved off v7 before the commit PUT");
        assert!(
            matches!(
                err,
                WriteError::CompatRefused(
                    crate::compat::CompatPublishRefusal::ActiveVersionMoved {
                        family: FormatFamily::Rseg,
                        granted: 7,
                        active: 8,
                    }
                )
            ),
            "{err:?}"
        );
        let other_keys = tenant_keys(store.as_ref(), &other).await;
        assert_eq!(
            other_keys.iter().filter(|k| k.contains("/c/")).count(),
            0,
            "{other_keys:?}"
        );

        let err = second
            .await
            .expect("second write task")
            .expect_err("granted under v8, which this build cannot write");
        assert_policy_refusal(&err, |r| {
            matches!(
                r,
                ravel_catalog::CompatRefusal::ActiveVersionNotWritable { active: 8, .. }
            )
        });
        assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 0);
        assert_eq!(bucket_compat_epoch(store.as_ref(), &acme).await, 0);
        assert_eq!(refused(&router).active_version_not_writable, 1);
        assert_eq!(refused(&router).active_version_moved, 1);
        assert_eq!(refused(&router).total(), 2);
        router.flush_all().await;
    }

    /// The fence's result refreshes the switch's cached record: the second
    /// flush above the baseline into the same bucket neither reads nor
    /// raises the provisioning record again. Its read is held, so a second
    /// fence attempt would leave the write parked.
    #[tokio::test]
    async fn a_fenced_bucket_is_not_fenced_again_by_the_next_flush() {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let acme = TenantId::new("acme");
        provision(store.as_ref(), &acme).await;
        let view = compat_view(&store, &compat_clock, Some(compat_record(1, 7, 0))).await;
        let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock.clone())
            .with_family_versions(FormatFamily::Rseg, vec![7], 6);
        let router = compat_router(
            one_permit_one_queue(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );

        let first = spawn_write(&router, &acme, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        first
            .await
            .expect("write task")
            .expect("fenced and written");
        assert_eq!(bucket_compat_epoch(store.as_ref(), &acme).await, 1);

        let gate = fault_store.hold(Op::Get, Some("/prov".to_string()), Occurrence::Nth(1));
        let second = spawn_write(&router, &acme, "h1", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 2).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        assert!(
            settles(|| second.is_finished()).await,
            "the second flush waited on a provisioning read: {:?}",
            gate.held_details()
        );
        second.await.expect("write task").expect("written");
        assert_eq!(gate.held_count(), 0);
        assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 4);
        router.flush_all().await;
    }

    /// Advances `clock` one [`crate::compat::STALE_VIEW_RETRY`] at a time,
    /// `steps` times, letting a flush waiting out a stale view ask again
    /// after each.
    async fn stale_retries(clock: &TestClock, steps: usize) {
        let step = i64::try_from(crate::compat::STALE_VIEW_RETRY.as_nanos()).expect("fits");
        for _ in 0..steps {
            clock.advance_ns(step);
            for _ in 0..50 {
                tokio::task::yield_now().await;
            }
        }
    }

    /// A view that ages to the horizon after the flush's grant, here while
    /// the bucket fence is in flight, holds the flush before its data PUT
    /// (ADR-4233 decision 3) without dropping its acknowledged rows: the
    /// flush asks again until a refresh makes the view fresh, then publishes.
    /// A check made only at grant (or at flush open) lets the PUT through
    /// while stale; one that refuses the attempt drops the rows.
    #[tokio::test]
    async fn a_view_stale_between_grant_and_put_waits_for_a_refresh_then_publishes() {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let acme = TenantId::new("acme");
        provision(store.as_ref(), &acme).await;
        let view = compat_view(&store, &compat_clock, Some(compat_record(1, 7, 0))).await;
        let policy = crate::compat::IngestCompatPolicy::new(view.clone(), compat_clock.clone())
            .with_family_versions(FormatFamily::Rseg, vec![7], 6);
        let router = compat_router(
            one_permit_one_queue(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );
        let gate = fault_store.hold(Op::Put, Some("/prov".to_string()), Occurrence::Nth(1));

        let write = spawn_write(&router, &acme, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        gate.wait_until_held(1).await;
        let before = tenant_keys(store.as_ref(), &acme).await;
        compat_clock.advance_ns(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS);
        for id in gate.held() {
            assert!(gate.release(id));
        }

        stale_retries(&clock, 5).await;
        assert!(!write.is_finished(), "the flush waits for a fresh view");
        let held = tenant_keys(store.as_ref(), &acme).await;
        assert_eq!(objects(&held), 0, "no PUT while the view is stale");
        assert_eq!(held.len(), before.len());
        assert_eq!(refused(&router).total(), 0);

        view.refresh()
            .await
            .expect("the view reads sys/compat again");
        stale_retries(&clock, 2).await;
        write
            .await
            .expect("write task")
            .expect("published once the view is fresh");
        assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 2);
        assert_eq!(
            committed_rseg_versions(store.as_ref(), &acme).await,
            vec![7]
        );
        assert_eq!(refused(&router).total(), 0);
        router.flush_all().await;
    }

    /// The same flush when the view stays stale: refused `view_stale` only
    /// once its `max_flush_lifetime` from grant elapses, with nothing stored.
    #[tokio::test]
    async fn a_view_stale_between_grant_and_put_is_refused_only_past_the_lifetime() {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let acme = TenantId::new("acme");
        provision(store.as_ref(), &acme).await;
        let view = compat_view(&store, &compat_clock, Some(compat_record(1, 7, 0))).await;
        let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock.clone())
            .with_family_versions(FormatFamily::Rseg, vec![7], 6);
        let lifetime = Duration::from_secs(60);
        let router = compat_router(one_permit_one_queue(lifetime), &store, &clock, policy);
        let gate = fault_store.hold(Op::Put, Some("/prov".to_string()), Occurrence::Nth(1));

        let write = spawn_write(&router, &acme, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        gate.wait_until_held(1).await;
        compat_clock.advance_ns(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS);
        for id in gate.held() {
            assert!(gate.release(id));
        }

        stale_retries(&clock, 30).await;
        assert!(!write.is_finished(), "inside the lifetime the flush waits");
        assert_eq!(refused(&router).total(), 0);
        stale_retries(&clock, 31).await;
        let err = write
            .await
            .expect("write task")
            .expect_err("the lifetime elapsed with the view still stale");
        assert_policy_refusal(&err, |r| {
            matches!(r, ravel_catalog::CompatRefusal::ViewStale { .. })
        });
        assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 0);
        assert_eq!(refused(&router).view_stale, 1);
        assert_eq!(refused(&router).total(), 1);
        router.flush_all().await;
    }

    struct FullJitter;

    impl ravel_commit::rng::RngSource for FullJitter {
        fn jitter_ms(&self, max_ms: u64) -> u64 {
            max_ms
        }

        fn new_uuid(&self) -> Uuid {
            Uuid::from_u128(0x4244)
        }
    }

    /// The view is asked before every PUT attempt, retries included: the
    /// first attempt of `key_fragment`'s PUT is throttled, and the view ages
    /// to the horizon during the backoff. The retry is not sent while the
    /// view is stale; with `recover` a refresh makes it fresh and the retry
    /// goes out inside the same lifetime, and without it the attempt is
    /// refused `view_stale` once the lifetime elapses. A check made only
    /// before the first attempt (or at grant) sends the retry stale; one that
    /// refuses at once drops the rows even when the view recovers.
    async fn stale_at_retry(
        key_fragment: &str,
        recover: bool,
    ) -> (
        Result<crate::router::WriteReceipt, WriteError>,
        Vec<String>,
        crate::compat::CompatRefusalSnapshot,
    ) {
        use ravel_object_store::fault::{Rule, ScriptedFault};
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Throttled { retry_after_ms: 0 })
                .with_key_contains(key_fragment)
                .with_occurrence(Occurrence::Nth(1)),
        );
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let acme = TenantId::new("acme");
        provision(store.as_ref(), &acme).await;
        let view = compat_view(&store, &compat_clock, Some(compat_record(1, 7, 0))).await;
        let policy = crate::compat::IngestCompatPolicy::new(view.clone(), compat_clock.clone());
        // The full backoff, never a zero-jitter one, so the flush parks in it
        // until the clock moves.
        let router = Arc::new(
            IngestRouter::with_rng(
                one_permit_one_queue(Duration::from_secs(60)),
                Arc::clone(&store),
                Signal::Metrics,
                clock.clone(),
                Arc::new(FullJitter),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
            .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited))
            .with_compat_policy(Some(Arc::new(policy)))
            .expect("no publish bound"),
        );

        let write = spawn_write(&router, &acme, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| router.metrics().snapshot().put_retries >= 1).await;
        compat_clock.advance_ns(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS);
        stale_retries(&clock, 30).await;
        assert!(!write.is_finished(), "inside the lifetime the flush waits");
        let held = tenant_keys(store.as_ref(), &acme).await;
        assert_eq!(
            held.iter().filter(|k| k.contains(key_fragment)).count(),
            0,
            "no retry is sent while the view is stale: {held:?}"
        );
        assert_eq!(router.metrics().snapshot().put_retries, 1);
        assert_eq!(refused(&router).total(), 0);
        if recover {
            view.refresh()
                .await
                .expect("the view reads sys/compat again");
        }
        while !write.is_finished() {
            stale_retries(&clock, 1).await;
        }

        let written = write.await.expect("write task");
        if recover {
            assert_eq!(router.metrics().snapshot().put_retries, 1);
        }
        let keys = tenant_keys(store.as_ref(), &acme).await;
        let snapshot = refused(&router);
        router.flush_all().await;
        (written, keys, snapshot)
    }

    #[tokio::test]
    async fn a_view_stale_at_a_data_put_retry_waits_for_a_refresh_then_retries() {
        let (written, keys, refused) = stale_at_retry("/l0/", true).await;
        written.expect("published once the view is fresh");
        assert_eq!(objects(&keys), 2, "{keys:?}");
        assert_eq!(refused.total(), 0);
    }

    #[tokio::test]
    async fn a_view_stale_at_a_data_put_retry_is_refused_only_past_the_lifetime() {
        let (written, keys, refused) = stale_at_retry("/l0/", false).await;
        let err = written.expect_err("the lifetime elapsed with the view still stale");
        assert_policy_refusal(&err, |r| {
            matches!(r, ravel_catalog::CompatRefusal::ViewStale { .. })
        });
        assert_eq!(objects(&keys), 0, "{keys:?}");
        assert_eq!(refused.view_stale, 1);
        assert_eq!(refused.total(), 1);
    }

    #[tokio::test]
    async fn a_view_stale_at_a_commit_put_retry_waits_for_a_refresh_then_retries() {
        let (written, keys, refused) = stale_at_retry("/c/", true).await;
        written.expect("published once the view is fresh");
        assert_eq!(
            keys.iter().filter(|k| k.contains("/c/")).count(),
            1,
            "the commit record went out after the refresh: {keys:?}"
        );
        assert_eq!(refused.total(), 0);
    }

    #[tokio::test]
    async fn a_view_stale_at_a_commit_put_retry_is_refused_only_past_the_lifetime() {
        let (written, keys, refused) = stale_at_retry("/c/", false).await;
        let err = written.expect_err("the lifetime elapsed with the view still stale");
        assert_policy_refusal(&err, |r| {
            matches!(r, ravel_catalog::CompatRefusal::ViewStale { .. })
        });
        assert_eq!(
            keys.iter().filter(|k| k.contains("/c/")).count(),
            0,
            "no commit record: {keys:?}"
        );
        assert_eq!(
            keys.iter().filter(|k| k.contains("/l0/")).count(),
            1,
            "the data object went out before the view aged: {keys:?}"
        );
        assert_eq!(refused.view_stale, 1);
        assert_eq!(refused.total(), 1);
    }

    /// ADR-4233 decision 4: the bucket fence is a CAS on the provisioning
    /// record, so a concurrent writer can win the race. Two lost races (the
    /// `/prov` CAS put refused `PreconditionFailed`) are retried after the
    /// jittered backoff; the third attempt wins, the data and commit PUTs go
    /// out, and the write succeeds. `fence_failed` stays zero.
    #[tokio::test]
    async fn a_lost_fence_cas_race_is_retried_twice_then_wins() {
        use ravel_object_store::fault::{ScriptedFault, Sequence};
        // Provision before arming the fault: the record's own create PUT is a
        // `/prov` PUT too, and the fault plan would otherwise fail it.
        let inner = MemoryStore::new();
        let acme = TenantId::new("acme");
        provision(&inner, &acme).await;
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Put)
                .with_key_contains("/prov")
                .then_fault(ScriptedFault::FailedConditionalWrite)
                .then_fault(ScriptedFault::FailedConditionalWrite),
        );
        let fault_store = Arc::new(FaultStore::new(inner, plan));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let view = compat_view(&store, &compat_clock, Some(compat_record(1, 7, 0))).await;
        let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock.clone())
            .with_family_versions(FormatFamily::Rseg, vec![7], 6);
        let router = compat_router(
            one_permit_one_queue(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );

        let write = spawn_write(&router, &acme, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        // The fence retries park in the jittered backoff (up to 50 ms); advance
        // the clock past it each turn so the two retries proceed.
        while !write.is_finished() {
            clock.advance_ns(100_000_000);
            tokio::task::yield_now().await;
        }
        write
            .await
            .expect("write task")
            .expect("fenced after two lost races and written");
        assert_eq!(
            fault_store.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::FailedConditionalWrite
            ),
            2,
            "two CAS races were lost before the win"
        );
        assert_eq!(bucket_compat_epoch(store.as_ref(), &acme).await, 1);
        assert_eq!(refused(&router).fence_failed, 0);
        assert_eq!(refused(&router).total(), 0);
        router.flush_all().await;
    }

    /// ADR-4233 decision 4: a fence that loses every CAS race up to
    /// `FENCE_ATTEMPTS` abandons the flush, refused `FenceFailed` and counted
    /// under `fence_failed`, with no data or commit object stored and the
    /// bucket left unfenced.
    #[tokio::test]
    async fn an_exhausted_fence_cas_race_is_refused_fence_failed() {
        use ravel_object_store::fault::{Rule, ScriptedFault};
        // Provision before arming the fault, which fails every `/prov` PUT
        // including the record's own create.
        let inner = MemoryStore::new();
        let acme = TenantId::new("acme");
        provision(&inner, &acme).await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("/prov"),
        );
        let fault_store = Arc::new(FaultStore::new(inner, plan));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let view = compat_view(&store, &compat_clock, Some(compat_record(1, 7, 0))).await;
        let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock.clone())
            .with_family_versions(FormatFamily::Rseg, vec![7], 6);
        let router = compat_router(
            one_permit_one_queue(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );

        let write = spawn_write(&router, &acme, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        while !write.is_finished() {
            clock.advance_ns(100_000_000);
            tokio::task::yield_now().await;
        }
        let err = write
            .await
            .expect("write task")
            .expect_err("every fence CAS race was lost");
        assert!(
            matches!(
                err,
                WriteError::CompatRefused(crate::compat::CompatPublishRefusal::FenceFailed {
                    compat_epoch: 1,
                    ..
                })
            ),
            "{err:?}"
        );
        assert_eq!(
            fault_store.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::FailedConditionalWrite
            ),
            u64::from(crate::compat::FENCE_ATTEMPTS),
            "the fence tried FENCE_ATTEMPTS times before giving up"
        );
        assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 0);
        assert_eq!(bucket_compat_epoch(store.as_ref(), &acme).await, 0);
        assert_eq!(refused(&router).fence_failed, 1);
        assert_eq!(refused(&router).total(), 1);
        router.flush_all().await;
    }

    /// A router dropped without `shutdown()` while a row sits buffered under a
    /// compat policy still publishes it: the per-tenant compat-epoch cache the
    /// scope holds by `Arc` survives the switch, so the teardown flush grants
    /// and fences from it instead of being refused `no_bucket_view` and
    /// dropping acknowledged rows (docs/consistency-model.md: only a crash may
    /// lose acknowledged rows). Before the scope held the cache, the grant
    /// upgraded the dropped switch to `None` and was refused `no_bucket_view`;
    /// a grant that instead fell back to an unmanaged write would publish at
    /// `CURRENT` and leave the bucket unfenced, which the version and epoch
    /// assertions below also rule out.
    #[tokio::test]
    async fn a_dropped_router_flushes_buffered_rows_under_a_policy() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let acme = TenantId::new("acme");
        provision(store.as_ref(), &acme).await;
        let view = compat_view(&store, &compat_clock, Some(compat_record(1, 7, 0))).await;
        let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock.clone())
            .with_family_versions(FormatFamily::Rseg, vec![7], 6);
        let router = compat_router(
            one_permit_one_queue(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );
        let metrics = router.metrics_handle();

        // Buffer one row and let it reach the shard. No clock tick fires, so no
        // normal flush runs: the row is live buffered state at the drop.
        spawn_write(&router, &acme, "h0", WriteMode::Buffered)
            .await
            .expect("write task")
            .expect("buffered");
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;

        // Drop the router without shutdown(): the switch drops, every actor's
        // mailbox closes, and each runs its teardown flush.
        drop(router);

        // The teardown flush publishes: a data object and a commit record
        // appear, and the fence raised the bucket from the cached epoch.
        let mut published = false;
        for _ in 0..20_000 {
            if objects(&tenant_keys(store.as_ref(), &acme).await) >= 2 {
                published = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            published,
            "the teardown flush never published: {:?}",
            tenant_keys(store.as_ref(), &acme).await
        );
        let snapshot = metrics.snapshot().compat_publish_refused;
        assert_eq!(
            snapshot.no_bucket_view, 0,
            "no teardown flush was refused no_bucket_view"
        );
        assert_eq!(snapshot.total(), 0, "no teardown flush was refused at all");
        assert_eq!(
            committed_rseg_versions(store.as_ref(), &acme).await,
            vec![7],
            "the teardown flush wrote the active version, not CURRENT"
        );
        assert_eq!(
            bucket_compat_epoch(store.as_ref(), &acme).await,
            1,
            "the teardown fence raised the bucket from the cached epoch"
        );
    }

    /// How a router's shards reach their teardown flush.
    #[derive(Debug, Clone, Copy)]
    enum Teardown {
        /// `shutdown()`: each actor's `Shutdown` drain, with the switch live.
        Shutdown,
        /// The router dropped: each actor's channel-closed drain, with the
        /// switch gone, so the scan-set check answers `Unknown` and the floor
        /// check `NoView` and only the drain's bypass passes open the flush.
        Drop,
    }

    /// Tears `router` down as `teardown` says, in a task that ends once the
    /// teardown call returns: after every shard's drain for `Shutdown`, at
    /// once for `Drop`.
    fn spawn_teardown(
        router: Arc<IngestRouter>,
        teardown: Teardown,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            match teardown {
                Teardown::Shutdown => {
                    Arc::try_unwrap(router)
                        .ok()
                        .expect("the test holds the only router handle")
                        .shutdown()
                        .await;
                }
                Teardown::Drop => drop(router),
            }
        })
    }

    /// One shard, one permit, and no age, size or arrival trigger inside the
    /// test's clock steps, so the only flush of a buffered write is the one
    /// the teardown drain opens.
    fn teardown_only(max_flush_lifetime: Duration) -> IngestConfig {
        IngestConfig {
            max_flush_delay: Duration::from_secs(3600),
            max_flush_delay_idle: Duration::from_secs(3600),
            ..one_permit_one_queue(max_flush_lifetime)
        }
    }

    /// Every key under `tenant`'s metrics prefix with its bytes, the
    /// provisioning record included, in key order.
    async fn tenant_objects(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantId,
    ) -> Vec<(String, Bytes)> {
        let mut keys = tenant_keys(store, tenant).await;
        keys.sort();
        let mut objects = Vec::new();
        for key in keys {
            let bytes = store.get(&key, GetRange::Full).await.expect("get").data;
            objects.push((key, bytes));
        }
        objects
    }

    /// #4362 (ADR-4233 decision 4 as narrowed after the #4243 drill): after
    /// an activation the teardown flush goes through the write policy like
    /// every other flush. A buffered row acknowledged while the view was
    /// fresh is still buffered when the view ages past the horizon (record
    /// seen) and the router is torn down, by `shutdown()` and by a drop. The
    /// teardown flush asks the view at grant, waits inside its
    /// `max_flush_lifetime`, and is then refused `view_stale` and counted,
    /// with nothing written: every key under the tenant, the fenced
    /// provisioning record included, is byte for byte what it was. The
    /// v0.33.0 bypass (publishing the teardown flush whatever the view says)
    /// and a teardown that skips the grant on a dropped switch each store a
    /// data object and a commit record; a grant that refuses a stale view at
    /// once counts the refusal inside the lifetime.
    #[tokio::test]
    async fn a_teardown_flush_under_a_stale_view_after_an_activation_writes_nothing() {
        for teardown in [Teardown::Shutdown, Teardown::Drop] {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let clock = TestClock::new(BASE_NS);
            let compat_clock = CompatTestClock::at(BASE_NS);
            let acme = TenantId::new("acme");
            provision(store.as_ref(), &acme).await;
            ravel_catalog::raise_compat_fence(
                store.as_ref(),
                &acme.hash(),
                Signal::Metrics,
                1,
                BASE_NS,
            )
            .await
            .expect("fence acme at the activation's epoch");
            let view = compat_view(&store, &compat_clock, Some(compat_record(1, 7, 0))).await;
            let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock.clone())
                .with_family_versions(FormatFamily::Rseg, vec![7], 6);
            let router = compat_router(
                teardown_only(Duration::from_secs(60)),
                &store,
                &clock,
                policy,
            );
            let metrics = router.metrics_handle();

            spawn_write(&router, &acme, "h0", WriteMode::Buffered)
                .await
                .expect("write task")
                .expect("acknowledged while the view is fresh");
            until(|| metrics.snapshot().buffered_points_total >= 1).await;
            let before = tenant_objects(store.as_ref(), &acme).await;
            assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 0);
            compat_clock.advance_ns(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS);

            let torn_down = spawn_teardown(router, teardown);
            until(|| metrics.snapshot().flushes_manual >= 1).await;
            stale_retries(&clock, 30).await;
            assert_eq!(
                metrics.snapshot().compat_publish_refused.total(),
                0,
                "{teardown:?}: inside its lifetime the teardown flush waits"
            );
            assert_eq!(
                objects(&tenant_keys(store.as_ref(), &acme).await),
                0,
                "{teardown:?}: no PUT while the view is stale"
            );

            stale_retries(&clock, 31).await;
            assert!(
                settles(|| metrics.snapshot().compat_publish_refused.view_stale == 1).await,
                "{teardown:?}: {:?}",
                metrics.snapshot().compat_publish_refused
            );
            tokio::time::timeout(Duration::from_secs(30), torn_down)
                .await
                .expect("the teardown ends")
                .expect("teardown task");
            let snapshot = metrics.snapshot();
            assert_eq!(snapshot.compat_publish_refused.total(), 1, "{teardown:?}");
            assert_eq!(snapshot.flushes_manual, 1, "{teardown:?}");
            assert_eq!(snapshot.flushes_by_age, 0, "{teardown:?}");
            assert_eq!(
                tenant_objects(store.as_ref(), &acme).await,
                before,
                "{teardown:?}: the teardown wrote nothing"
            );
            assert_eq!(bucket_compat_epoch(store.as_ref(), &acme).await, 1);
        }
    }

    /// #4362, ADR-4233 decision 1: on a store that never ran compat init the
    /// teardown flush publishes the acknowledged buffered row at the
    /// baseline, as v0.33.0 did, by `shutdown()` and by a drop, both when the
    /// view read `sys/compat` absent and when it is blind (its only read
    /// refused `AccessDenied`) and past the horizon. Nothing is refused, the
    /// bucket stays unfenced, and no `sys/compat` object appears. A teardown
    /// refused for want of a live switch, or one that treats a blind view as
    /// stale, stores nothing.
    #[tokio::test]
    async fn a_teardown_flush_on_a_never_activated_store_publishes_at_the_baseline() {
        for teardown in [Teardown::Shutdown, Teardown::Drop] {
            for get in [CompatGet::Passthrough, CompatGet::AccessDenied] {
                let case = format!("{teardown:?}, {get:?}");
                let (compat_store, compat_clock, view) = view_after_one_refresh(get).await;
                if get == CompatGet::AccessDenied {
                    compat_clock.advance_ns(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS);
                }
                let store: Arc<dyn ObjectStoreBackend> = compat_store.clone();
                let clock = TestClock::new(BASE_NS);
                let acme = TenantId::new("acme");
                let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock);
                let router = compat_router(
                    teardown_only(Duration::from_secs(60)),
                    &store,
                    &clock,
                    policy,
                );
                let metrics = router.metrics_handle();

                spawn_write(&router, &acme, "h0", WriteMode::Buffered)
                    .await
                    .expect("write task")
                    .expect("acknowledged");
                until(|| metrics.snapshot().buffered_points_total >= 1).await;
                assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 0);

                let torn_down = spawn_teardown(router, teardown);
                let mut stored = 0;
                for _ in 0..20_000 {
                    stored = objects(&tenant_keys(store.as_ref(), &acme).await);
                    if stored == 2 {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
                assert_eq!(stored, 2, "{case}: the teardown flush published");
                tokio::time::timeout(Duration::from_secs(30), torn_down)
                    .await
                    .expect("the teardown ends")
                    .expect("teardown task");
                assert_eq!(
                    committed_rseg_versions(store.as_ref(), &acme).await,
                    vec![u32::from(crate::SEGMENT_FORMAT_VERSION)],
                    "{case}"
                );
                let snapshot = metrics.snapshot();
                assert_eq!(snapshot.compat_publish_refused.total(), 0, "{case}");
                assert_eq!(snapshot.flushes_manual, 1, "{case}");
                assert_eq!(
                    bucket_compat_epoch(store.as_ref(), &acme).await,
                    0,
                    "{case}"
                );
                assert!(
                    list_all(store.as_ref(), "sys/")
                        .await
                        .expect("list sys/")
                        .is_empty(),
                    "{case}: no sys/compat object"
                );
            }
        }
    }

    /// The view names an RSEG version this build cannot write: a strict and
    /// a buffered write are both refused at admission, retryably, counted,
    /// and nothing is buffered, charged or stored.
    #[tokio::test]
    async fn an_active_version_this_build_cannot_write_is_refused_at_admission() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let acme = TenantId::new("acme");
        provision(store.as_ref(), &acme).await;
        let view = compat_view(&store, &compat_clock, Some(compat_record(1, 8, 0))).await;
        let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock.clone());
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1 << 30));
        let router = Arc::new(
            IngestRouter::new(
                one_permit_one_queue(Duration::from_secs(3600)),
                Arc::clone(&store),
                Signal::Metrics,
                clock.clone(),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
            .with_budget(Arc::clone(&budget))
            .with_compat_policy(Some(Arc::new(policy)))
            .expect("no publish bound"),
        );

        for mode in [WriteMode::Strict, WriteMode::Buffered] {
            let err = spawn_write(&router, &acme, "h0", mode)
                .await
                .expect("write task")
                .expect_err("v8 is not writable");
            assert_policy_refusal(&err, |r| {
                matches!(
                    r,
                    ravel_catalog::CompatRefusal::ActiveVersionNotWritable { active: 8, .. }
                )
            });
        }
        let snapshot = router.metrics().snapshot();
        assert_eq!(snapshot.buffered_points_total, 0);
        assert_eq!(
            snapshot.compat_publish_refused.active_version_not_writable,
            2
        );
        assert_eq!(snapshot.compat_publish_refused.total(), 2);
        assert_eq!(budget.in_flight_bytes(), 0);
        router.flush_all().await;
        assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 0);
    }

    /// A bucket fenced at a nonzero `compat_epoch` with no `sys/compat` in
    /// the store is refused as `CompatRecordMissing` (the record was deleted
    /// or lost); a bucket at epoch 0 in the same store writes at the
    /// baseline.
    #[tokio::test]
    async fn a_fenced_bucket_with_sys_compat_absent_is_refused_as_record_missing() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let (acme, other) = (TenantId::new("acme"), TenantId::new("other"));
        provision(store.as_ref(), &acme).await;
        provision(store.as_ref(), &other).await;
        ravel_catalog::raise_compat_fence(
            store.as_ref(),
            &acme.hash(),
            Signal::Metrics,
            1,
            BASE_NS,
        )
        .await
        .expect("fence acme");
        let view = compat_view(&store, &compat_clock, None).await;
        let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock.clone());
        let router = compat_router(
            one_permit_one_queue(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );

        let err = spawn_write(&router, &acme, "h0", WriteMode::Buffered)
            .await
            .expect("write task")
            .expect_err("acme's bucket names an epoch sys/compat does not hold");
        assert_policy_refusal(&err, |r| {
            matches!(
                r,
                ravel_catalog::CompatRefusal::CompatRecordMissing {
                    bucket_compat_epoch: 1,
                    seen_compat_epoch: 0,
                }
            )
        });
        assert_eq!(refused(&router).compat_record_missing, 1);

        let written = spawn_write(&router, &other, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        written
            .await
            .expect("write task")
            .expect("an unfenced bucket writes at the baseline");
        assert_eq!(refused(&router).total(), 1);
        router.flush_all().await;
    }

    /// The `segment_format_version` of every commit record under `tenant`'s
    /// metrics prefix.
    async fn committed_rseg_versions(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantId,
    ) -> Vec<u32> {
        let mut versions = Vec::new();
        for key in tenant_keys(store, tenant).await {
            if key.contains("/c/") {
                let bytes = store
                    .get(&key, GetRange::Full)
                    .await
                    .expect("commit record")
                    .data;
                versions.push(
                    record::decode(&bytes)
                        .expect("decode commit record")
                        .segment_format_version,
                );
            }
        }
        versions
    }

    /// A store with `acme` provisioned in it whose `sys/compat` GET answers
    /// as `get` says, and a view that refreshed once against it. A
    /// `Passthrough` refresh reads the record absent, a successful read.
    async fn view_after_one_refresh(
        get: CompatGet,
    ) -> (
        Arc<CompatGetStore>,
        Arc<CompatTestClock>,
        ravel_catalog::CompatView,
    ) {
        let compat_store = Arc::new(CompatGetStore::new(get));
        let store: Arc<dyn ObjectStoreBackend> = compat_store.clone();
        let compat_clock = CompatTestClock::at(BASE_NS);
        provision(store.as_ref(), &TenantId::new("acme")).await;
        let view =
            ravel_catalog::CompatView::new(Arc::clone(&store), compat_keys(), compat_clock.clone());
        let refreshed = view.refresh().await;
        match get {
            CompatGet::Passthrough => assert!(!refreshed.expect("absent reads").present),
            CompatGet::AccessDenied => {
                assert!(refreshed.expect_err("denied").is_access_denied());
            }
            CompatGet::ServerError => {
                assert!(!refreshed.expect_err("5xx").is_access_denied());
            }
        }
        assert_eq!(
            compat_store.failed(),
            usize::from(get != CompatGet::Passthrough),
            "{get:?}"
        );
        assert!(!view.record_seen());
        (compat_store, compat_clock, view)
    }

    /// One strict write of `acme` through `router`, flushed by one tick.
    async fn strict_write(
        router: &Arc<IngestRouter>,
        clock: &TestClock,
        host: &'static str,
    ) -> Result<crate::router::WriteReceipt, WriteError> {
        let before = router.metrics().snapshot().buffered_points_total;
        let write = spawn_write(router, &TenantId::new("acme"), host, WriteMode::Strict);
        until(|| write.is_finished() || router.metrics().snapshot().buffered_points_total > before)
            .await;
        clock.advance_ns(TICK_ADVANCE_NS);
        write.await.expect("write task")
    }

    /// ADR-4233 decision 3: a view that no refresh has succeeded for and
    /// whose latest refresh was refused with `AccessDenied` (a credential
    /// without the `sys/compat` grant) is blind. On a bucket at
    /// `compat_epoch` 0 it keeps publishing at the baseline past the horizon,
    /// because each attempt asks the view with the bucket's record. A bare
    /// freshness check before each PUT refuses the first one.
    #[tokio::test]
    async fn an_access_denied_never_read_view_on_an_unfenced_bucket_publishes_at_the_baseline() {
        let (compat_store, compat_clock, view) =
            view_after_one_refresh(CompatGet::AccessDenied).await;
        compat_clock.advance_ns(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS);
        let store: Arc<dyn ObjectStoreBackend> = compat_store.clone();
        let clock = TestClock::new(BASE_NS);
        let acme = TenantId::new("acme");
        let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock.clone());
        let router = compat_router(
            one_permit_one_queue(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );

        strict_write(&router, &clock, "h0")
            .await
            .expect("written at the baseline");
        assert_eq!(
            committed_rseg_versions(store.as_ref(), &acme).await,
            vec![u32::from(crate::SEGMENT_FORMAT_VERSION)]
        );
        assert_eq!(bucket_compat_epoch(store.as_ref(), &acme).await, 0);
        assert_eq!(refused(&router).total(), 0);
        router.flush_all().await;
    }

    /// ADR-4233 decision 3: a view that no refresh has succeeded for and
    /// whose latest refresh failed with a store error other than
    /// `AccessDenied` (here a 503 at startup) cannot tell whether an
    /// activation happened, so it is not blind. A write on a bucket at
    /// `compat_epoch` 0 is refused under `view_stale` at admission, inside
    /// the horizon, before anything is acknowledged: a buffered write that
    /// admission let through would be acknowledged and then dropped at
    /// grant. Once a refresh succeeds and reads `sys/compat` absent, writes
    /// publish at the baseline with the bucket left unfenced.
    #[tokio::test]
    async fn a_never_read_view_behind_a_5xx_refuses_admission_until_a_refresh_then_publishes_at_the_baseline()
     {
        let (compat_store, compat_clock, view) =
            view_after_one_refresh(CompatGet::ServerError).await;
        let store: Arc<dyn ObjectStoreBackend> = compat_store.clone();
        let clock = TestClock::new(BASE_NS);
        let acme = TenantId::new("acme");
        let policy = crate::compat::IngestCompatPolicy::new(view.clone(), compat_clock.clone());
        let router = compat_router(
            one_permit_one_queue(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );

        let buffered = spawn_write(&router, &acme, "h0", WriteMode::Buffered)
            .await
            .expect("write task");
        let err = buffered.expect_err("a never-read view behind a 5xx is not blind");
        assert_policy_refusal(&err, |r| {
            matches!(
                r,
                ravel_catalog::CompatRefusal::ViewStale {
                    last_read_unix_ns: None,
                    now_unix_ns: BASE_NS,
                }
            )
        });
        assert_eq!(router.metrics().snapshot().buffered_points_total, 0);
        let err = strict_write(&router, &clock, "h1")
            .await
            .expect_err("still not blind");
        assert_policy_refusal(&err, |r| {
            matches!(r, ravel_catalog::CompatRefusal::ViewStale { .. })
        });
        assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 0);
        assert_eq!(bucket_compat_epoch(store.as_ref(), &acme).await, 0);
        assert_eq!(refused(&router).view_stale, 2);
        assert_eq!(refused(&router).total(), 2);

        compat_store.set_get(CompatGet::Passthrough);
        assert!(!view.refresh().await.expect("absent reads").present);
        strict_write(&router, &clock, "h2")
            .await
            .expect("written at the baseline");
        assert_eq!(
            committed_rseg_versions(store.as_ref(), &acme).await,
            vec![u32::from(crate::SEGMENT_FORMAT_VERSION)]
        );
        assert_eq!(bucket_compat_epoch(store.as_ref(), &acme).await, 0);
        assert_eq!(refused(&router).total(), 2);
        router.flush_all().await;
    }

    /// ADR-4233 decision 3: an outage of `sys/compat` that outlasts the
    /// horizon after a read that found it absent. Admission refuses new
    /// writes `view_stale`. A buffered write acknowledged before the outage
    /// is not dropped at grant: its flush waits out the stale view and
    /// publishes at the baseline once a refresh succeeds inside its
    /// `max_flush_lifetime`, or is refused `view_stale`, counted and with
    /// nothing stored, only once that lifetime elapses.
    async fn outage_after_an_absent_read(recover: bool) {
        let (compat_store, compat_clock, view) =
            view_after_one_refresh(CompatGet::Passthrough).await;
        let store: Arc<dyn ObjectStoreBackend> = compat_store.clone();
        let clock = TestClock::new(BASE_NS);
        let acme = TenantId::new("acme");
        let policy = crate::compat::IngestCompatPolicy::new(view.clone(), compat_clock.clone());
        let router = compat_router(
            buffered_on_the_tick(Duration::from_secs(60)),
            &store,
            &clock,
            policy,
        );

        spawn_write(&router, &acme, "h0", WriteMode::Buffered)
            .await
            .expect("write task")
            .expect("acknowledged while the view is fresh");
        compat_store.set_get(CompatGet::ServerError);
        compat_clock.advance_ns(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS);
        assert!(view.refresh().await.is_err(), "the outage fails the read");

        let err = spawn_write(&router, &acme, "h1", WriteMode::Buffered)
            .await
            .expect("write task")
            .expect_err("admission refuses past the horizon");
        assert_policy_refusal(&err, |r| {
            matches!(r, ravel_catalog::CompatRefusal::ViewStale { .. })
        });
        assert_eq!(router.metrics().snapshot().buffered_points_total, 1);
        assert_eq!(refused(&router).view_stale, 1);

        clock.advance_ns(TICK_ADVANCE_NS);
        stale_retries(&clock, 30).await;
        assert_eq!(
            objects(&tenant_keys(store.as_ref(), &acme).await),
            0,
            "no PUT while the view is stale"
        );
        assert_eq!(refused(&router).view_stale, 1, "the flush is not dropped");

        if recover {
            compat_store.set_get(CompatGet::Passthrough);
            view.refresh().await.expect("the outage ends");
            for _ in 0..5 {
                if objects(&tenant_keys(store.as_ref(), &acme).await) == 2 {
                    break;
                }
                stale_retries(&clock, 1).await;
            }
            assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 2);
            assert_eq!(
                committed_rseg_versions(store.as_ref(), &acme).await,
                vec![u32::from(crate::SEGMENT_FORMAT_VERSION)]
            );
            assert_eq!(bucket_compat_epoch(store.as_ref(), &acme).await, 0);
            assert_eq!(refused(&router).total(), 1);
        } else {
            stale_retries(&clock, 31).await;
            assert_eq!(
                refused(&router).view_stale,
                2,
                "abandoned past the lifetime"
            );
            assert_eq!(refused(&router).total(), 2);
            assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 0);
        }
        router.flush_all().await;
    }

    /// [`one_permit_one_queue`] with a buffered write's flush, too, opened
    /// by the first tick past [`TICK_ADVANCE_NS`].
    fn buffered_on_the_tick(max_flush_lifetime: Duration) -> IngestConfig {
        IngestConfig {
            max_flush_delay_idle: Duration::from_millis(50),
            ..one_permit_one_queue(max_flush_lifetime)
        }
    }

    /// ADR-4233 decision 7: `max_publish_lifetime_ns` is never lowered, so a
    /// record whose bound is below a live router's `max_flush_lifetime` is
    /// one `compat init` wrote while the router ran. A buffered write
    /// acknowledged before init still publishes, at the baseline, inside the
    /// router's own lifetime; only the write after init is refused, at
    /// admission, `publish_lifetime_above_compat`. A grant that re-checks the
    /// bound drops the acknowledged rows; an admission that does not check it
    /// acknowledges the second write.
    #[tokio::test]
    async fn compat_init_below_a_live_routers_lifetime_refuses_admission_and_publishes_admitted_rows()
     {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let acme = TenantId::new("acme");
        provision(store.as_ref(), &acme).await;
        let view = compat_view(&store, &compat_clock, None).await;
        assert!(!view.record_seen());
        let policy = crate::compat::IngestCompatPolicy::new(view.clone(), compat_clock.clone());
        let router = compat_router(
            buffered_on_the_tick(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );

        spawn_write(&router, &acme, "h0", WriteMode::Buffered)
            .await
            .expect("write task")
            .expect("acknowledged before compat init");
        let current = FormatFamily::Rseg.writable_version_under(None);
        ravel_catalog::compat::create_compat(
            store.as_ref(),
            &compat_keys(),
            compat_record(1, current, 60_000_000_000),
        )
        .await
        .expect("compat init");
        assert!(view.refresh().await.expect("read sys/compat").present);

        let err = spawn_write(&router, &acme, "h1", WriteMode::Buffered)
            .await
            .expect("write task")
            .expect_err("refused at admission");
        assert!(
            matches!(
                err,
                WriteError::CompatRefused(
                    crate::compat::CompatPublishRefusal::PublishLifetimeAboveCompat {
                        family: FormatFamily::Rseg,
                        max_flush_lifetime_ns: 3_600_000_000_000,
                        max_publish_lifetime_ns: 60_000_000_000,
                    }
                )
            ),
            "{err:?}"
        );
        assert!(err.is_retryable());
        assert_eq!(router.metrics().snapshot().buffered_points_total, 1);

        clock.advance_ns(TICK_ADVANCE_NS);
        let mut stored = 0;
        for _ in 0..20_000 {
            stored = objects(&tenant_keys(store.as_ref(), &acme).await);
            if stored == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(stored, 2, "the acknowledged flush published");
        assert_eq!(
            committed_rseg_versions(store.as_ref(), &acme).await,
            vec![current]
        );
        assert_eq!(bucket_compat_epoch(store.as_ref(), &acme).await, 0);
        assert_eq!(refused(&router).publish_lifetime_above_compat, 1);
        assert_eq!(refused(&router).total(), 1);
        router.flush_all().await;
    }

    #[tokio::test]
    async fn an_outage_after_an_absent_read_publishes_acknowledged_rows_once_the_view_recovers() {
        outage_after_an_absent_read(true).await;
    }

    #[tokio::test]
    async fn an_outage_after_an_absent_read_abandons_acknowledged_rows_only_past_the_lifetime() {
        outage_after_an_absent_read(false).await;
    }

    /// ADR-4233 decision 3: past the horizon, a blind view (its only refresh
    /// refused with `AccessDenied`) refuses a bucket that carries a fence,
    /// and a view that has read `sys/compat`, present or absent, refuses an
    /// unfenced bucket. Each is refused at admission, counted under
    /// `view_stale`, and stores nothing.
    #[tokio::test]
    async fn past_the_horizon_a_fenced_bucket_or_a_read_view_is_refused_view_stale() {
        let acme = TenantId::new("acme");
        let (denied_store, denied_clock, denied_view) =
            view_after_one_refresh(CompatGet::AccessDenied).await;
        denied_clock.advance_ns(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS);
        let denied: Arc<dyn ObjectStoreBackend> = denied_store;
        ravel_catalog::raise_compat_fence(
            denied.as_ref(),
            &acme.hash(),
            Signal::Metrics,
            1,
            BASE_NS,
        )
        .await
        .expect("fence acme");

        let (absent_store, absent_clock, absent_view) =
            view_after_one_refresh(CompatGet::Passthrough).await;
        absent_clock.advance_ns(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS);
        let absent: Arc<dyn ObjectStoreBackend> = absent_store;

        let seen: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        provision(seen.as_ref(), &acme).await;
        let seen_clock = CompatTestClock::at(BASE_NS);
        let seen_view = compat_view(&seen, &seen_clock, Some(compat_record(1, 7, 0))).await;
        seen_clock.advance_ns(ravel_catalog::compat::COMPAT_VIEW_HORIZON_NS);

        for (case, store, view, compat_clock, last_read) in [
            ("fenced, blind", denied, denied_view, denied_clock, None),
            (
                "read absent",
                absent,
                absent_view,
                absent_clock,
                Some(BASE_NS),
            ),
            ("read a record", seen, seen_view, seen_clock, Some(BASE_NS)),
        ] {
            let clock = TestClock::new(BASE_NS);
            let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock);
            let router = compat_router(
                one_permit_one_queue(Duration::from_secs(3600)),
                &store,
                &clock,
                policy,
            );
            let err = strict_write(&router, &clock, "h0")
                .await
                .expect_err("the view is past the horizon");
            assert_policy_refusal(&err, |r| {
                matches!(
                    r,
                    ravel_catalog::CompatRefusal::ViewStale { last_read_unix_ns, .. }
                        if *last_read_unix_ns == last_read
                )
            });
            assert_eq!(
                objects(&tenant_keys(store.as_ref(), &acme).await),
                0,
                "{case}"
            );
            assert_eq!(refused(&router).view_stale, 1, "{case}");
            assert_eq!(refused(&router).total(), 1, "{case}");
            router.flush_all().await;
        }
    }

    /// ADR-4233 decision 3: the view moves RSEG from the granted v7 to v8
    /// while the data PUT is in flight. The commit PUT's check gets v8 back
    /// and abandons the flush under `active_version_moved`; a check that
    /// asks only whether the view refuses publishes the v7 commit record.
    #[tokio::test]
    async fn an_active_version_moved_after_grant_abandons_the_flush_before_its_commit_put() {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let acme = TenantId::new("acme");
        provision(store.as_ref(), &acme).await;
        let view = compat_view(&store, &compat_clock, Some(compat_record(1, 7, 0))).await;
        let policy = crate::compat::IngestCompatPolicy::new(view.clone(), compat_clock.clone())
            .with_family_versions(FormatFamily::Rseg, vec![7, 8], 7);
        let router = compat_router(
            one_permit_one_queue(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );
        let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

        let write = spawn_write(&router, &acme, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        gate.wait_until_held(1).await;
        ravel_catalog::compat::activate_family(
            store.as_ref(),
            &compat_keys(),
            ravel_catalog::compat::FAMILY_RSEG,
            8,
            "test",
            BASE_NS,
        )
        .await
        .expect("activate v8");
        view.refresh().await.expect("read sys/compat");
        for id in gate.held() {
            assert!(gate.release(id));
        }

        let err = write
            .await
            .expect("write task")
            .expect_err("the view moved off the granted version");
        assert!(
            matches!(
                err,
                WriteError::CompatRefused(
                    crate::compat::CompatPublishRefusal::ActiveVersionMoved {
                        family: FormatFamily::Rseg,
                        granted: 7,
                        active: 8,
                    }
                )
            ),
            "{err:?}"
        );
        assert!(err.is_retryable());
        let keys = tenant_keys(store.as_ref(), &acme).await;
        assert_eq!(
            keys.iter().filter(|k| k.contains("/c/")).count(),
            0,
            "{keys:?}"
        );
        assert_eq!(
            keys.iter().filter(|k| k.contains("/l0/")).count(),
            1,
            "{keys:?}"
        );
        assert_eq!(refused(&router).active_version_moved, 1);
        assert_eq!(refused(&router).total(), 1);
        router.flush_all().await;
    }

    /// The floor checks at admission and at flush open compare the tenant's
    /// RSEG floor with the version the policy names, not the build's
    /// `CURRENT`. The view names v8 over a floor of 8, so each write passes
    /// the floor and reaches the grant, which refuses v8 as a version this
    /// writer cannot encode. Compared with `CURRENT` (v7) at either check,
    /// the write is refused there as below the floor.
    #[tokio::test]
    async fn the_floor_is_checked_against_the_version_the_policy_names() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let acme = TenantId::new("acme");
        provision(store.as_ref(), &acme).await;
        ravel_catalog::raise_format_floor(
            store.as_ref(),
            &acme.hash(),
            Signal::Metrics,
            FormatFamily::Rseg.as_str(),
            8,
            ravel_catalog::FloorBasis {
                observed_entries: 0,
                observed_newest_created_unix_ns: 0,
                observed_shards: 1,
            },
            "test",
            BASE_NS,
        )
        .await
        .expect("raise the RSEG floor to 8");
        let view = compat_view(&store, &compat_clock, Some(compat_record(1, 8, 0))).await;
        let policy = crate::compat::IngestCompatPolicy::new(view, compat_clock.clone())
            .with_family_versions(FormatFamily::Rseg, vec![7, 8], 7);
        assert_eq!(FormatFamily::Rseg.writable_version_under(Some(&policy)), 8);
        let router = compat_router(
            one_permit_one_queue(Duration::from_secs(3600)),
            &store,
            &clock,
            policy,
        );

        for host in ["h0", "h1"] {
            let err = strict_write(&router, &clock, host)
                .await
                .expect_err("v8 is not encodable");
            assert_policy_refusal(&err, |r| {
                matches!(
                    r,
                    ravel_catalog::CompatRefusal::ActiveVersionNotWritable { active: 8, .. }
                )
            });
        }
        assert_eq!(refused(&router).active_version_not_writable, 2);
        assert_eq!(refused(&router).total(), 2);
        router.flush_all().await;
        assert_eq!(objects(&tenant_keys(store.as_ref(), &acme).await), 0);
    }

    /// ADR-4233 decision 7: a router refuses to start under a policy whose
    /// record bounds publication below its `max_flush_lifetime`; a record
    /// bound of zero, an equal bound and no policy all start.
    #[tokio::test]
    async fn a_max_flush_lifetime_above_the_records_publish_bound_is_refused_at_construction() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = TestClock::new(BASE_NS);
        let compat_clock = CompatTestClock::at(BASE_NS);
        let lifetime = Duration::from_secs(3600);
        let lifetime_ns = i64::try_from(lifetime.as_nanos()).expect("fits");
        let router = || {
            IngestRouter::new(
                one_permit_one_queue(lifetime),
                Arc::clone(&store),
                Signal::Metrics,
                clock.clone(),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
        };
        let view = compat_view(
            &store,
            &compat_clock,
            Some(compat_record(1, 7, lifetime_ns - 1)),
        )
        .await;
        let policy = Arc::new(crate::compat::IngestCompatPolicy::new(
            view.clone(),
            compat_clock.clone(),
        ));

        let err = router()
            .with_compat_policy(Some(Arc::clone(&policy)))
            .err()
            .expect("the record bounds publication below max_flush_lifetime");
        assert_eq!(
            err,
            crate::compat::PublishLifetimeAboveCompat {
                max_flush_lifetime_ns: lifetime_ns,
                max_publish_lifetime_ns: lifetime_ns - 1,
            }
        );
        assert!(router().with_compat_policy(None).is_ok());

        for bound in [lifetime_ns, 0] {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let view = compat_view(&store, &compat_clock, Some(compat_record(1, 7, bound))).await;
            let policy = Arc::new(crate::compat::IngestCompatPolicy::new(
                view,
                compat_clock.clone(),
            ));
            assert!(
                router().with_compat_policy(Some(policy)).is_ok(),
                "bound {bound}"
            );
        }
    }

    const NO_PUBLISH_AFTER_ABANDON: &str =
        "commit: an abandoned flush's commit record never lands past the fold's seal margin";
    const ABSENT_AT_ABANDON: &str =
        "commit: an abandoned flush's commit record is absent at the abandon";
    const NO_COMMIT_WITHOUT_DATA: &str =
        "commit: a commit record never exists without its data object";
    const EVIDENCE_COMPLETES: &str =
        "commit: a commit's data-object evidence read completes before it gives up";
    const STRICT_METRICS_ACK: &str = "commit: a strict metrics ack names a durable commit record";
    const STRICT_ACK_COMPLETES: &str =
        "commit: a strict metrics ack's durable-commit evidence read completes before it gives up";

    const NS_PER_MINUTE: i64 = 60 * 1_000_000_000;

    /// A fold whose margins differ from each other, from the catalog
    /// defaults, and from the flush lifetime the routers below run with, so
    /// a bound built from any other source reads a different instant.
    const TEST_FOLD_SEAL: crate::config::FoldSealMargin = crate::config::FoldSealMargin {
        max_flush_lifetime_ns: 7 * NS_PER_MINUTE,
        clock_skew_allowance_ns: 3 * NS_PER_MINUTE,
        fold_safety_margin_ns: 11 * NS_PER_MINUTE,
    };

    /// One minute into the hour that ends at `BOUNDARY_NS`.
    const EARLY_NS: i64 = BOUNDARY_NS - NS_PER_HOUR + NS_PER_MINUTE;

    /// The instant a fold under `TEST_FOLD_SEAL` seals the hour holding
    /// `EARLY_NS`: that hour's end plus 7 + 3 + 11 minutes, restated here
    /// rather than read from the code under test.
    const EARLY_HOUR_SEALED_AT_NS: i64 = BOUNDARY_NS + 21 * NS_PER_MINUTE;

    /// Drives the injected clock in backoff-sized steps until `write` ends.
    async fn retry_until_finished<T>(clock: &TestClock, write: &tokio::task::JoinHandle<T>) {
        while !write.is_finished() {
            clock.advance_ns(5_000_000);
            for _ in 0..50 {
                tokio::task::yield_now().await;
            }
        }
    }

    /// The prefix of `tenant`'s metrics commit records.
    fn commit_prefix(tenant: &TenantId) -> String {
        format!(
            "t/{}/{}/c/",
            tenant.hash().to_hex(),
            Signal::Metrics.key_prefix()
        )
    }

    /// A one-shard router over `store` whose flushes end at the
    /// `lifetime` deadline, never on the attempt budget.
    fn deadline_bounded_router(
        store: &Arc<dyn ObjectStoreBackend>,
        clock: &Arc<TestClock>,
        lifetime: Duration,
    ) -> Arc<IngestRouter> {
        attempt_bounded_router(store, clock, lifetime, u32::MAX)
    }

    /// A one-shard router over `store` whose flushes retry a failed PUT
    /// `retries` times before abandoning, under `TEST_FOLD_SEAL`.
    fn attempt_bounded_router(
        store: &Arc<dyn ObjectStoreBackend>,
        clock: &Arc<TestClock>,
        lifetime: Duration,
        retries: u32,
    ) -> Arc<IngestRouter> {
        Arc::new(
            IngestRouter::with_rng(
                IngestConfig {
                    put_retry_max_attempts: retries,
                    fold_seal: TEST_FOLD_SEAL,
                    ..one_permit_one_queue(lifetime)
                },
                Arc::clone(store),
                Signal::Metrics,
                clock.clone(),
                Arc::new(FullJitter),
                Arc::new(crate::pending_ack::PendingAckBudget::new(0)),
            )
            .with_budget(IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)),
        )
    }

    /// A store whose PUTs run on their own task: a caller that stops waiting
    /// does not cancel the PUT, which completes when the store lets it, as a
    /// request already sent to a real object store does. A PUT whose key
    /// contains the second field's prefix is reported timed out at once
    /// while it still runs.
    struct DetachedPutStore(Arc<FaultStore<MemoryStore>>, Option<String>);

    #[async_trait::async_trait]
    impl ObjectStoreBackend for DetachedPutStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: ravel_object_store::PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
            let inner = Arc::clone(&self.0);
            let reports_timeout = self.1.as_ref().is_some_and(|p| key.contains(p.as_str()));
            let key = key.to_owned();
            let put = tokio::spawn(async move { inner.put(&key, data, opts).await });
            if reports_timeout {
                return Err(ravel_object_store::StoreError::Timeout);
            }
            put.await
                .unwrap_or_else(|e| Err(ravel_object_store::StoreError::Transient(e.to_string())))
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
            self.0.get(key, range).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, ravel_object_store::StoreError>
        {
            self.0.put_multipart(key).await
        }

        async fn head(
            &self,
            key: &str,
        ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
            self.0.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
            self.0.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
            self.0.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
            self.0.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.0.capabilities()
        }
    }

    /// Stalls every commit-record HEAD (a `/c/` key) once `stall` is set, so a
    /// test can let the abandoned keys accumulate with the flag off and then
    /// stall exactly the re-reads a later flush issues. A stalled HEAD never
    /// answers; the caller's own `EVIDENCE_HEAD_TIMEOUT` race must end it.
    struct StallCommitHeadStore {
        inner: Arc<FaultStore<MemoryStore>>,
        stall: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for StallCommitHeadStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: ravel_object_store::PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
            self.inner.get(key, range).await
        }

        async fn head(
            &self,
            key: &str,
        ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
            if self.stall.load(std::sync::atomic::Ordering::SeqCst) && key.contains("/c/") {
                std::future::pending().await
            } else {
                self.inner.head(key).await
            }
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// NoPublishAfterAbandon when no abandoned commit lands later. Four
    /// flushes on one shard:
    /// 1. `retried`: the commit PUT is throttled once and its retry publishes.
    /// 2. `abandoned`: every commit PUT is throttled until the deadline. The
    ///    record is absent at the abandon, so its key is re-read by every
    ///    later flush, and it stays absent.
    /// 3. `landed`: the first commit PUT applies but its response is lost,
    ///    then every retry is throttled until the deadline. The record landed
    ///    before the abandon, the documented ambiguity: it is found at the
    ///    abandon, so its key is never re-read and nothing evaluates false,
    ///    although the record exists.
    /// 4. `retried` again, whose flush re-reads the abandoned key.
    #[tokio::test]
    async fn no_publish_after_abandon_holds_when_no_abandoned_commit_lands_later() {
        use ravel_object_store::fault::{FaultKind, Rule, ScriptedFault, Sequence};

        if !crate::router::invariant_capture::capturing() {
            return;
        }
        let retried = TenantId::new("no-publish-after-abandon-retried");
        let abandoned = TenantId::new("no-publish-after-abandon-abandoned");
        let landed = TenantId::new("no-publish-after-abandon-landed");
        let commits = commit_prefix;
        let throttled = || ScriptedFault::Throttled { retry_after_ms: 0 };
        let plan = FaultPlan::empty()
            .with_sequence(
                Sequence::new(Op::Put)
                    .with_key_contains(commits(&landed))
                    .then_fault(ScriptedFault::DuplicateDelivery),
            )
            .with_rule(
                Rule::new(Op::Put, throttled())
                    .with_key_contains(commits(&retried))
                    .with_occurrence(Occurrence::Nth(1)),
            )
            .with_rule(
                Rule::new(Op::Put, throttled())
                    .with_key_contains(commits(&abandoned))
                    .with_occurrence(Occurrence::Always),
            )
            .with_rule(
                Rule::new(Op::Put, throttled())
                    .with_key_contains(commits(&landed))
                    .with_occurrence(Occurrence::Always),
            );
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        provision(store.as_ref(), &retried).await;
        provision(store.as_ref(), &abandoned).await;
        provision(store.as_ref(), &landed).await;
        let lifetime = Duration::from_secs(1);
        let clock = TestClock::new(BASE_NS);
        let router = deadline_bounded_router(&store, &clock, lifetime);

        let write = spawn_write(&router, &retried, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| router.metrics().snapshot().put_retries >= 1).await;
        retry_until_finished(&clock, &write).await;
        let receipt = write
            .await
            .expect("write task")
            .expect("the retried commit publishes inside the lifetime");
        assert_eq!(
            fault_store.fault_count(Op::Put, FaultKind::Throttled),
            1,
            "the first flush's commit PUT was throttled once"
        );
        assert_eq!(router.metrics().snapshot().put_retries, 1);
        assert!(!receipt.tokens.is_empty());

        let opened_ns = clock.now_ns();
        let write = spawn_write(&router, &abandoned, "h1", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 2).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| router.metrics().snapshot().put_retries >= 2).await;
        retry_until_finished(&clock, &write).await;
        let err = write
            .await
            .expect("write task")
            .expect_err("every commit PUT is throttled until the deadline");
        assert!(matches!(err, WriteError::Abandoned(_)), "{err:?}");
        let lifetime_ns = i64::try_from(lifetime.as_nanos()).expect("fits");
        assert!(
            clock.now_ns() - opened_ns >= lifetime_ns,
            "the flush was abandoned at its deadline, not by an attempt budget"
        );
        let throttles = fault_store.fault_count(Op::Put, FaultKind::Throttled);
        assert!(
            throttles >= 3,
            "the second flush's commit PUT was throttled on every attempt: {throttles}"
        );
        assert_eq!(router.metrics().snapshot().abandoned_retry_exhausted, 1);
        let keys = tenant_keys(store.as_ref(), &abandoned).await;
        assert!(
            !keys.iter().any(|k| k.contains("/c/")),
            "no commit record of the abandoned flush exists: {keys:?}"
        );
        let absent =
            crate::router::invariant_capture::evaluations(ABSENT_AT_ABANDON, &commits(&abandoned));
        assert_eq!(absent.len(), 1, "{absent:?}");
        assert!(absent[0].0, "{absent:?}");
        let abandon_site = crate::router::invariant_capture::evaluations(
            "commit: flush abandoned",
            &abandoned.hash().to_hex(),
        );
        assert_eq!(abandon_site.len(), 1, "{abandon_site:?}");
        assert!(abandon_site[0].1.contains("commit put"), "{abandon_site:?}");

        let write = spawn_write(&router, &landed, "h2", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 3).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        retry_until_finished(&clock, &write).await;
        let err = write
            .await
            .expect("write task")
            .expect_err("every commit PUT after the lost response is throttled");
        assert!(matches!(err, WriteError::Abandoned(_)), "{err:?}");
        assert_eq!(
            fault_store.fault_count(Op::Put, FaultKind::DuplicateDelivery),
            1,
            "the landed flush's first commit PUT applied and lost its response"
        );
        assert_eq!(router.metrics().snapshot().abandoned_retry_exhausted, 2);
        let keys = tenant_keys(store.as_ref(), &landed).await;
        assert!(
            keys.iter().any(|k| k.contains("/c/")),
            "the lost-response commit record exists: {keys:?}"
        );
        let found =
            crate::router::invariant_capture::evaluations(ABSENT_AT_ABANDON, &commits(&landed));
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(
            !found[0].0,
            "the record was found at the abandon: {found:?}"
        );

        let write = spawn_write(&router, &retried, "h3", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 4).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        retry_until_finished(&clock, &write).await;
        write
            .await
            .expect("write task")
            .expect("the last flush commits");

        let rechecked = crate::router::invariant_capture::evaluations(
            NO_PUBLISH_AFTER_ABANDON,
            &commits(&abandoned),
        );
        assert!(
            !rechecked.is_empty(),
            "no later flush re-read the abandoned key"
        );
        assert!(rechecked.iter().all(|(held, _)| *held), "{rechecked:?}");
        let never = crate::router::invariant_capture::evaluations(
            NO_PUBLISH_AFTER_ABANDON,
            &commits(&landed),
        );
        assert!(never.is_empty(), "{never:?}");
        router.flush_all().await;
    }

    /// The value of `key` in a captured evaluation's details.
    fn detail(details: &str, key: &str) -> String {
        details
            .split_once(&format!("\"{key}\": \""))
            .and_then(|(_, rest)| rest.split_once('"'))
            .map(|(value, _)| value.to_owned())
            .unwrap_or_default()
    }

    /// One flush of `tenant` whose commit PUT is reported timed out while the
    /// PUT itself is held, so the flush abandons on its attempt budget well
    /// inside its lifetime. The flush is granted at `EARLY_NS`, early in its
    /// ingest hour. Returns the router, the fault store, the clock, and the
    /// held PUT's gate and commit key.
    async fn abandon_on_a_reported_failure(
        tenant: &TenantId,
        next: &TenantId,
        head_plan: FaultPlan,
    ) -> (
        Arc<IngestRouter>,
        Arc<FaultStore<MemoryStore>>,
        Arc<TestClock>,
        GateHandle,
        String,
    ) {
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), head_plan));
        let gate = fault_store.hold(Op::Put, Some(commit_prefix(tenant)), Occurrence::Nth(1));
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(DetachedPutStore(
            Arc::clone(&fault_store),
            Some(commit_prefix(tenant)),
        ));
        provision(store.as_ref(), tenant).await;
        provision(store.as_ref(), next).await;
        let clock = TestClock::new(EARLY_NS);
        let router = attempt_bounded_router(&store, &clock, Duration::from_secs(60), 0);

        let write = spawn_write(&router, tenant, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        gate.wait_until_held(1).await;
        let (_, _, commit_key) = gate.held_details()[0].clone();
        retry_until_finished(&clock, &write).await;
        let err = write
            .await
            .expect("write task")
            .expect_err("the commit PUT was reported timed out with no retry left");
        assert!(matches!(err, WriteError::Abandoned(_)), "{err:?}");
        assert_eq!(router.metrics().snapshot().abandoned_retry_exhausted, 1);
        let absent = crate::router::invariant_capture::evaluations(ABSENT_AT_ABANDON, &commit_key);
        assert_eq!(absent.len(), 1, "{absent:?}");
        assert!(
            absent[0].0,
            "the held record is absent at the abandon: {absent:?}"
        );
        (router, fault_store, clock, gate, commit_key)
    }

    /// Releases the held commit PUT with the store clock at `landed_ms`, the
    /// `last_modified` MemoryStore stamps on the record, and waits until the
    /// record exists.
    async fn land_held_put(
        fault_store: &FaultStore<MemoryStore>,
        gate: &GateHandle,
        commit_key: &str,
        landed_ms: i64,
    ) {
        fault_store
            .inner()
            .set_clock_ms(u64::try_from(landed_ms).expect("positive"));
        for id in gate.held() {
            assert!(gate.release(id));
        }
        for _ in 0..10_000 {
            if fault_store.inner().head(commit_key).await.is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let meta = fault_store
            .inner()
            .head(commit_key)
            .await
            .expect("the released PUT stored the commit record");
        assert_eq!(meta.last_modified_unix_ms, landed_ms);
    }

    /// Runs one committed flush of `next`, whose re-read follows its outcome.
    async fn commit_one_flush(
        router: &Arc<IngestRouter>,
        clock: &Arc<TestClock>,
        next: &TenantId,
        host: &'static str,
        buffered: u64,
    ) {
        let write = spawn_write(router, next, host, WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= buffered).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        retry_until_finished(clock, &write).await;
        write
            .await
            .expect("write task")
            .expect("the next flush commits");
    }

    /// A commit PUT reported failed that still lands after the abandon. The
    /// abandoning flush's own re-read finds the record absent; once the PUT
    /// lands with store `last_modified` at `landed_ms`, the next flush's
    /// re-read evaluates NoPublishAfterAbandon on it. Returns that
    /// evaluation.
    async fn abandoned_record_landing_at(name: &str, landed_ms: i64) -> (bool, String) {
        let tenant = TenantId::new(format!("no-publish-after-abandon-{name}"));
        let next = TenantId::new(format!("no-publish-after-abandon-{name}-next"));
        let (router, fault_store, clock, gate, commit_key) =
            abandon_on_a_reported_failure(&tenant, &next, FaultPlan::empty()).await;
        assert!(
            clock.now_ns() < EARLY_NS + NS_PER_MINUTE,
            "the flush abandoned early in its hour, long before the hour's end"
        );

        let absent =
            crate::router::invariant_capture::evaluations(NO_PUBLISH_AFTER_ABANDON, &commit_key);
        assert_eq!(absent.len(), 1, "{absent:?}");
        assert!(absent[0].0, "{absent:?}");
        assert_eq!(detail(&absent[0].1, "last_modified_unix_ms"), "absent");
        assert_eq!(
            detail(&absent[0].1, "ingest_hour_bucket"),
            (EARLY_NS / NS_PER_HOUR).to_string(),
            "{absent:?}"
        );
        assert_eq!(
            detail(&absent[0].1, "sealed_at_ns"),
            EARLY_HOUR_SEALED_AT_NS.to_string(),
            "the bound is the fold's seal point of the record's hour: {absent:?}"
        );

        land_held_put(&fault_store, &gate, &commit_key, landed_ms).await;
        commit_one_flush(&router, &clock, &next, "h1", 2).await;

        let evaluations =
            crate::router::invariant_capture::evaluations(NO_PUBLISH_AFTER_ABANDON, &commit_key);
        assert_eq!(evaluations.len(), 2, "{evaluations:?}");
        assert_eq!(
            detail(&evaluations[1].1, "last_modified_unix_ms"),
            landed_ms.to_string(),
            "{evaluations:?}"
        );
        router.flush_all().await;
        evaluations[1].clone()
    }

    /// A commit PUT granted early in its hour, reported failed, that lands
    /// with store `last_modified` one millisecond before the fold seals that
    /// hour, nearly an hour after the flush's own deadline:
    /// NoPublishAfterAbandon holds.
    #[tokio::test]
    async fn an_abandoned_commit_landing_just_before_the_seal_point_holds() {
        if !crate::router::invariant_capture::capturing() {
            return;
        }
        let (held, details) =
            abandoned_record_landing_at("inside", EARLY_HOUR_SEALED_AT_NS / 1_000_000 - 1).await;
        assert!(held, "{details}");
    }

    /// The same PUT landing at the seal point itself, when a fold may
    /// already have sealed the hour: NoPublishAfterAbandon evaluates false.
    #[tokio::test]
    async fn an_abandoned_commit_landing_at_the_seal_point_breaks_no_publish_after_abandon() {
        if !crate::router::invariant_capture::capturing() {
            return;
        }
        let (held, details) =
            abandoned_record_landing_at("past", EARLY_HOUR_SEALED_AT_NS / 1_000_000).await;
        assert!(!held, "{details}");
    }

    /// A transient HEAD error on the re-read evaluates nothing and keeps the
    /// key: the record has landed past the seal margin, two re-reads fail
    /// with no evaluation, and the third evaluates false.
    #[tokio::test]
    async fn a_transient_head_error_on_the_re_read_evaluates_nothing() {
        use ravel_object_store::fault::{FaultKind, ScriptedFault, Sequence};

        if !crate::router::invariant_capture::capturing() {
            return;
        }
        let tenant = TenantId::new("no-publish-after-abandon-head-error");
        let next = TenantId::new("no-publish-after-abandon-head-error-next");
        let transient = || ScriptedFault::Transient("head blip".into());
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Head)
                .with_key_contains(commit_prefix(&tenant))
                .then_passthrough()
                .then_fault(transient())
                .then_fault(transient()),
        );
        let (router, fault_store, clock, gate, commit_key) =
            abandon_on_a_reported_failure(&tenant, &next, plan).await;
        let erring = |reads| {
            assert_eq!(
                fault_store.fault_count(Op::Head, FaultKind::Transient),
                reads,
                "every re-read so far failed"
            );
            let evaluations = crate::router::invariant_capture::evaluations(
                NO_PUBLISH_AFTER_ABANDON,
                &commit_key,
            );
            assert!(evaluations.is_empty(), "{evaluations:?}");
        };
        erring(1);

        let late_ms = EARLY_HOUR_SEALED_AT_NS / 1_000_000 + 1_000;
        land_held_put(&fault_store, &gate, &commit_key, late_ms).await;
        commit_one_flush(&router, &clock, &next, "h1", 2).await;
        erring(2);

        commit_one_flush(&router, &clock, &next, "h2", 3).await;
        let evaluations =
            crate::router::invariant_capture::evaluations(NO_PUBLISH_AFTER_ABANDON, &commit_key);
        assert_eq!(evaluations.len(), 1, "{evaluations:?}");
        assert!(!evaluations[0].0, "{evaluations:?}");
        router.flush_all().await;
    }

    /// The abandoned-key re-reads run after a flush's outcome is decided: a
    /// re-read HEAD held past the next flush's deadline does not delay that
    /// flush's ack or abandon it.
    #[tokio::test]
    async fn a_slow_re_read_does_not_hold_the_next_flushs_ack() {
        use ravel_object_store::fault::{Rule, ScriptedFault};

        if !crate::router::invariant_capture::capturing() {
            return;
        }
        let abandoned = TenantId::new("abandoned-re-read-slow");
        let next = TenantId::new("abandoned-re-read-slow-next");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Throttled { retry_after_ms: 0 })
                .with_key_contains(commit_prefix(&abandoned))
                .with_occurrence(Occurrence::Always),
        );
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        // HEAD 1 is the abandon's own, HEAD 2 the abandoning flush's re-read,
        // HEAD 3 the next flush's re-read.
        let gate = fault_store.hold(
            Op::Head,
            Some(commit_prefix(&abandoned)),
            Occurrence::Nth(3),
        );
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        provision(store.as_ref(), &abandoned).await;
        provision(store.as_ref(), &next).await;
        let clock = TestClock::new(BASE_NS);
        let lifetime = Duration::from_secs(1);
        let router = attempt_bounded_router(&store, &clock, lifetime, 0);

        let write = spawn_write(&router, &abandoned, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        retry_until_finished(&clock, &write).await;
        let err = write
            .await
            .expect("write task")
            .expect_err("the throttled commit PUT has no retry left");
        assert!(matches!(err, WriteError::Abandoned(_)), "{err:?}");

        let write = spawn_write(&router, &next, "h1", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 2).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        tokio::time::timeout(Duration::from_secs(10), gate.wait_until_held(1))
            .await
            .expect("the next flush re-read the abandoned key");
        clock.advance_ns(2 * i64::try_from(lifetime.as_nanos()).expect("fits"));
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        let receipt = tokio::time::timeout(Duration::from_secs(10), write)
            .await
            .expect("the ack does not wait for the held re-read")
            .expect("write task")
            .expect("the next flush commits");
        assert_eq!(receipt.tokens.len(), 1);
        assert_eq!(gate.held_count(), 1, "the re-read is still held");
        assert_eq!(router.metrics().snapshot().abandoned_retry_exhausted, 1);
        for id in gate.held() {
            assert!(gate.release(id));
        }
        router.flush_all().await;
    }

    /// Each flush re-reads at most four abandoned keys, oldest first, and
    /// moves the ones it keeps to the back. Six flushes each abandon one
    /// absent key; the re-read counts per key are the round-robin's.
    #[tokio::test]
    async fn each_flush_re_reads_at_most_four_abandoned_keys_oldest_first() {
        use ravel_object_store::fault::{Rule, ScriptedFault};

        if !crate::router::invariant_capture::capturing() {
            return;
        }
        let tenants: Vec<TenantId> = (0..6)
            .map(|i| TenantId::new(format!("abandoned-re-read-bound-{i}")))
            .collect();
        let plan = tenants.iter().fold(FaultPlan::empty(), |plan, tenant| {
            plan.with_rule(
                Rule::new(Op::Put, ScriptedFault::Throttled { retry_after_ms: 0 })
                    .with_key_contains(commit_prefix(tenant))
                    .with_occurrence(Occurrence::Always),
            )
        });
        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(FaultStore::new(MemoryStore::new(), plan));
        for tenant in &tenants {
            provision(store.as_ref(), tenant).await;
        }
        let clock = TestClock::new(BASE_NS);
        let router = attempt_bounded_router(&store, &clock, Duration::from_secs(60), 0);

        const HOSTS: [&str; 6] = ["h0", "h1", "h2", "h3", "h4", "h5"];
        for (i, tenant) in tenants.iter().enumerate() {
            let write = spawn_write(&router, tenant, HOSTS[i], WriteMode::Strict);
            until(|| router.metrics().snapshot().buffered_points_total > i as u64).await;
            clock.advance_ns(TICK_ADVANCE_NS);
            retry_until_finished(&clock, &write).await;
            let err = write
                .await
                .expect("write task")
                .expect_err("the throttled commit PUT has no retry left");
            assert!(matches!(err, WriteError::Abandoned(_)), "{err:?}");
        }

        let reads: Vec<usize> = tenants
            .iter()
            .map(|tenant| {
                let evaluations = crate::router::invariant_capture::evaluations(
                    NO_PUBLISH_AFTER_ABANDON,
                    &commit_prefix(tenant),
                );
                assert!(evaluations.iter().all(|(held, _)| *held), "{evaluations:?}");
                evaluations.len()
            })
            .collect();
        assert_eq!(reads, vec![6, 5, 4, 2, 1, 0]);
        router.flush_all().await;
    }

    /// The NoCommitWithoutData evidence read runs after the commit PUT's
    /// deadline race: a HEAD held past the flush deadline delays the ack of a
    /// landed commit but does not abandon it.
    #[tokio::test]
    async fn a_slow_evidence_read_does_not_abandon_a_landed_commit() {
        if !crate::router::invariant_capture::capturing() {
            return;
        }
        let slow = TenantId::new("no-commit-without-data-slow-head");
        let data_prefix = format!(
            "t/{}/{}/l0/",
            slow.hash().to_hex(),
            Signal::Metrics.key_prefix()
        );
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let gate = fault_store.hold(Op::Head, Some(data_prefix.clone()), Occurrence::Nth(1));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        provision(store.as_ref(), &slow).await;
        let clock = TestClock::new(BASE_NS);
        // Twice this stays inside `EVIDENCE_HEAD_TIMEOUT`, so the held read
        // still completes and evaluates.
        let lifetime = Duration::from_millis(500);
        let router = deadline_bounded_router(&store, &clock, lifetime);

        let write = spawn_write(&router, &slow, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        gate.wait_until_held(1).await;
        assert!(
            tenant_keys(store.as_ref(), &slow)
                .await
                .iter()
                .any(|k| k.contains("/c/")),
            "the commit record landed before the evidence read"
        );
        clock.advance_ns(2 * i64::try_from(lifetime.as_nanos()).expect("fits"));
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        for id in gate.held() {
            assert!(gate.release(id));
        }
        let receipt = write
            .await
            .expect("write task")
            .expect("the landed commit is acked although its evidence read outlived the deadline");
        assert_eq!(receipt.tokens.len(), 1);
        assert_eq!(router.metrics().snapshot().abandoned_retry_exhausted, 0);
        let evaluations =
            crate::router::invariant_capture::evaluations(NO_COMMIT_WITHOUT_DATA, &data_prefix);
        assert_eq!(evaluations.len(), 1, "{evaluations:?}");
        assert!(evaluations[0].0, "{evaluations:?}");
        router.flush_all().await;
    }

    /// A NoCommitWithoutData evidence read that never answers holds the ack
    /// only until `EVIDENCE_HEAD_TIMEOUT` on the injected clock, and its
    /// expiry evaluates nothing.
    #[tokio::test]
    async fn a_stalled_evidence_read_gives_up_at_the_timeout() {
        if !crate::router::invariant_capture::capturing() {
            return;
        }
        let stalled = TenantId::new("no-commit-without-data-stalled-head");
        let data_prefix = format!(
            "t/{}/{}/l0/",
            stalled.hash().to_hex(),
            Signal::Metrics.key_prefix()
        );
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let gate = fault_store.hold(Op::Head, Some(data_prefix.clone()), Occurrence::Nth(1));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        provision(store.as_ref(), &stalled).await;
        let clock = TestClock::new(BASE_NS);
        let router = deadline_bounded_router(&store, &clock, Duration::from_secs(60));

        let write = spawn_write(&router, &stalled, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        tokio::time::timeout(Duration::from_secs(10), gate.wait_until_held(1))
            .await
            .expect("the commit's evidence read reached the store");
        assert!(
            !settles(|| write.is_finished()).await,
            "the ack waits for the evidence read until the timeout"
        );
        clock.advance_ns(i64::try_from(publish::EVIDENCE_HEAD_TIMEOUT.as_nanos()).expect("fits"));
        let receipt = tokio::time::timeout(Duration::from_secs(10), write)
            .await
            .expect("the ack follows the evidence timeout")
            .expect("write task")
            .expect("the landed commit is acked");
        assert_eq!(receipt.tokens.len(), 1);
        assert_eq!(gate.held_count(), 1, "the evidence read is still held");
        assert_eq!(router.metrics().snapshot().abandoned_retry_exhausted, 0);
        let evaluations =
            crate::router::invariant_capture::evaluations(NO_COMMIT_WITHOUT_DATA, &data_prefix);
        assert!(evaluations.is_empty(), "{evaluations:?}");
        let completed = crate::router::invariant_capture::evaluations(
            EVIDENCE_COMPLETES,
            &commit_prefix(&stalled),
        );
        assert_eq!(completed.len(), 1, "{completed:?}");
        assert!(!completed[0].0, "{completed:?}");
        for id in gate.held() {
            assert!(gate.release(id));
        }
        router.flush_all().await;
    }

    /// An abandoned-key re-read HEAD that never answers holds the flush's
    /// permit only until `EVIDENCE_HEAD_TIMEOUT` on the injected clock: a
    /// flush queued behind that permit then runs. The expiry evaluates
    /// nothing and keeps the key, which the queued flush re-reads.
    #[tokio::test]
    async fn a_stalled_re_read_does_not_hold_a_queued_flush_past_the_timeout() {
        use ravel_object_store::fault::{Rule, ScriptedFault};

        if !crate::router::invariant_capture::capturing() {
            return;
        }
        let abandoned = TenantId::new("abandoned-re-read-stalled");
        let next = TenantId::new("abandoned-re-read-stalled-next");
        // A tenant with no flush in flight, so its flush is spawned and waits
        // for the permit rather than deferring at the queued-flush cap.
        let third = TenantId::new("abandoned-re-read-stalled-third");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Throttled { retry_after_ms: 0 })
                .with_key_contains(commit_prefix(&abandoned))
                .with_occurrence(Occurrence::Always),
        );
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        // HEAD 1 is the abandon's own, HEAD 2 the abandoning flush's re-read,
        // HEAD 3 the second flush's re-read, HEAD 4 the queued flush's.
        let gate = fault_store.hold(
            Op::Head,
            Some(commit_prefix(&abandoned)),
            Occurrence::Nth(3),
        );
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        provision(store.as_ref(), &abandoned).await;
        provision(store.as_ref(), &next).await;
        provision(store.as_ref(), &third).await;
        let clock = TestClock::new(BASE_NS);
        let router = attempt_bounded_router(&store, &clock, Duration::from_secs(60), 0);

        let write = spawn_write(&router, &abandoned, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        retry_until_finished(&clock, &write).await;
        let err = write
            .await
            .expect("write task")
            .expect_err("the throttled commit PUT has no retry left");
        assert!(matches!(err, WriteError::Abandoned(_)), "{err:?}");

        let write = spawn_write(&router, &next, "h1", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 2).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        tokio::time::timeout(Duration::from_secs(10), gate.wait_until_held(1))
            .await
            .expect("the second flush re-read the abandoned key");
        tokio::time::timeout(Duration::from_secs(10), write)
            .await
            .expect("the second flush's ack does not wait for its re-read")
            .expect("write task")
            .expect("the second flush commits");

        let queued = spawn_write(&router, &third, "h2", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 3).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        assert!(
            settles(|| queued_gauge(&router) == 2).await,
            "the third flush's task is spawned beside the re-reading one"
        );
        assert!(
            !settles(|| queued.is_finished()).await,
            "the third flush waits for the permit the held re-read keeps"
        );
        clock.advance_ns(i64::try_from(publish::EVIDENCE_HEAD_TIMEOUT.as_nanos()).expect("fits"));
        let receipt = tokio::time::timeout(Duration::from_secs(10), queued)
            .await
            .expect("the queued flush runs once the re-read times out")
            .expect("write task")
            .expect("the queued flush commits");
        assert_eq!(receipt.tokens.len(), 1);
        assert_eq!(gate.held_count(), 1, "the timed-out re-read is still held");

        let rechecks = || {
            crate::router::invariant_capture::evaluations(
                NO_PUBLISH_AFTER_ABANDON,
                &commit_prefix(&abandoned),
            )
        };
        // The queued flush re-reads after its ack.
        settles(|| rechecks().len() >= 2).await;
        let evaluations = rechecks();
        assert_eq!(
            evaluations.len(),
            2,
            "HEAD 2 and HEAD 4 evaluate, the timed-out HEAD 3 does not: {evaluations:?}"
        );
        assert!(evaluations.iter().all(|(held, _)| *held), "{evaluations:?}");
        for id in gate.held() {
            assert!(gate.release(id));
        }
        router.flush_all().await;
    }

    /// A stalled commit HEAD in the strict-ack evidence read does not hang the
    /// ack: the read races `EVIDENCE_HEAD_TIMEOUT` on the injected clock and
    /// evaluates nothing on expiry, while the write still returns its token.
    /// Fails against the bare `store.head` the fix replaced: the ack would hang
    /// on the held HEAD forever.
    #[tokio::test]
    async fn a_stalled_strict_ack_evidence_read_does_not_hang_the_ack() {
        if !crate::router::invariant_capture::capturing() {
            return;
        }
        let stalled = TenantId::new("strict-metrics-ack-stalled");
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        // The first HEAD on the commit prefix is the strict ack's evidence read:
        // the flush PUTs the commit record and HEADs only the data key before it.
        let gate = fault_store.hold(Op::Head, Some(commit_prefix(&stalled)), Occurrence::Nth(1));
        let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
        provision(store.as_ref(), &stalled).await;
        let clock = TestClock::new(BASE_NS);
        let router = deadline_bounded_router(&store, &clock, Duration::from_secs(60));

        let write = spawn_write(&router, &stalled, "h0", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        tokio::time::timeout(Duration::from_secs(10), gate.wait_until_held(1))
            .await
            .expect("the strict ack's evidence read reached the store");
        assert!(
            !settles(|| write.is_finished()).await,
            "the ack waits for the evidence read until the timeout"
        );
        clock.advance_ns(i64::try_from(publish::EVIDENCE_HEAD_TIMEOUT.as_nanos()).expect("fits"));
        let receipt = tokio::time::timeout(Duration::from_secs(10), write)
            .await
            .expect("the ack follows the evidence timeout")
            .expect("write task")
            .expect("the strict write is acked although its evidence read timed out");
        assert_eq!(receipt.tokens.len(), 1);
        let acks = crate::router::invariant_capture::evaluations(
            STRICT_METRICS_ACK,
            &stalled.hash().to_hex(),
        );
        assert!(
            acks.is_empty(),
            "the evidence read evaluated nothing: {acks:?}"
        );
        let completes = crate::router::invariant_capture::evaluations(
            STRICT_ACK_COMPLETES,
            &stalled.hash().to_hex(),
        );
        assert_eq!(completes.len(), 1, "{completes:?}");
        assert!(!completes[0].0, "the read gave up: {completes:?}");
        for id in gate.held() {
            assert!(gate.release(id));
        }
        router.flush_all().await;
    }

    /// A flush's abandoned-key recheck bounds the whole batch by ONE
    /// `EVIDENCE_HEAD_TIMEOUT`, not one per key: with two stalled re-read HEADs,
    /// the flush that owns the recheck leaves flight after a single timeout.
    /// Fails against a per-key deadline (one fresh sleep per key), which needs
    /// two timeouts for two keys, so after one the flush is still in flight.
    #[tokio::test]
    async fn a_recheck_of_several_stalled_keys_is_bounded_by_one_timeout() {
        use ravel_object_store::fault::{Rule, ScriptedFault};

        if !crate::router::invariant_capture::capturing() {
            return;
        }
        let a1 = TenantId::new("recheck-batch-abandoned-one");
        let a2 = TenantId::new("recheck-batch-abandoned-two");
        let committing = TenantId::new("recheck-batch-committing");
        let plan = FaultPlan::empty()
            .with_rule(
                Rule::new(Op::Put, ScriptedFault::Throttled { retry_after_ms: 0 })
                    .with_key_contains(commit_prefix(&a1))
                    .with_occurrence(Occurrence::Always),
            )
            .with_rule(
                Rule::new(Op::Put, ScriptedFault::Throttled { retry_after_ms: 0 })
                    .with_key_contains(commit_prefix(&a2))
                    .with_occurrence(Occurrence::Always),
            );
        let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let stall = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(StallCommitHeadStore {
            inner: Arc::clone(&fault_store),
            stall: Arc::clone(&stall),
        });
        provision(store.as_ref(), &a1).await;
        provision(store.as_ref(), &a2).await;
        provision(store.as_ref(), &committing).await;
        let clock = TestClock::new(BASE_NS);
        // No retry budget: each throttled commit PUT abandons at once, with its
        // record absent, so its key is remembered for the recheck.
        let router = attempt_bounded_router(&store, &clock, Duration::from_secs(600), 0);

        for (tenant, host) in [(&a1, "h0"), (&a2, "h1")] {
            let before = router.metrics().snapshot().buffered_points_total;
            let write = spawn_write(&router, tenant, host, WriteMode::Strict);
            until(|| router.metrics().snapshot().buffered_points_total > before).await;
            clock.advance_ns(TICK_ADVANCE_NS);
            retry_until_finished(&clock, &write).await;
            let err = write
                .await
                .expect("write task")
                .expect_err("the throttled commit PUT abandons with no retry left");
            assert!(matches!(err, WriteError::Abandoned(_)), "{err:?}");
        }
        assert_eq!(router.metrics().snapshot().abandoned_retry_exhausted, 2);

        // Both abandoned keys are now remembered. Stall every commit HEAD, so
        // the committing flush's recheck re-reads both against a store that never
        // answers and must rely on the shared deadline.
        stall.store(true, std::sync::atomic::Ordering::SeqCst);
        let before = router.metrics().snapshot().buffered_points_total;
        let write = spawn_write(&router, &committing, "h2", WriteMode::Strict);
        until(|| router.metrics().snapshot().buffered_points_total > before).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        // The committing flush takes the permit, commits, and parks in its
        // recheck holding the in-flight guard.
        assert!(
            settles(|| in_flight(&router) == 1).await,
            "the committing flush is in flight on its stalled recheck"
        );
        assert!(
            !settles(|| in_flight(&router) == 0).await,
            "the recheck holds the flush in flight before the timeout"
        );
        clock.advance_ns(i64::try_from(publish::EVIDENCE_HEAD_TIMEOUT.as_nanos()).expect("fits"));
        assert!(
            settles(|| in_flight(&router) == 0).await,
            "one timeout bounds the whole batch of stalled re-reads"
        );
        // Let the strict ack's own evidence read give up too, so the write task
        // ends rather than leaking.
        clock.advance_ns(i64::try_from(publish::EVIDENCE_HEAD_TIMEOUT.as_nanos()).expect("fits"));
        let _ = tokio::time::timeout(Duration::from_secs(10), write).await;
        stall.store(false, std::sync::atomic::Ordering::SeqCst);
        router.flush_all().await;
    }
}
