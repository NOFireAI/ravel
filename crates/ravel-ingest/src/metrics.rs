//! Self-observability counters (docs/ingest.md "Metrics"). Plain atomics for
//! now; scraping/otel export is a later task.
//!
//! # Counting convention
//!
//! Every counter in [`IngestMetricsSnapshot`] is a monotonic process-global
//! total with **no per-shard and no per-tenant dimension**: a single
//! [`IngestMetrics`] is constructed once by the router and shared by every
//! shard actor through an `Arc`, so a value is the sum across all shards and
//! all tenants of this process. Two per-shard dimensions sit outside that flat
//! snapshot, each read through its own accessor: the in-flight-flush gauge
//! ([`IngestMetrics::in_flight_flushes_by_shard`]) and the ingest-skew figures
//! ([`IngestMetrics::shard_skew_by_shard`], issue #865: per-shard message
//! throughput, queue depth, and the three-way on-actor / flush-permit-wait /
//! off-actor time split). The per-tenant dimensioned model and per-shard
//! latency histograms docs/ingest.md still lists as future work are not
//! implemented here.
//!
//! Two timing conventions coexist, and mixing them up misreads the numbers:
//!
//! - **Attempt-time.** [`record_flush`](IngestMetrics::record_flush) fires when
//!   a flush is *opened* (`shard.rs` `flush_tenant`), before the segment build,
//!   the data-object PUT, or the commit-record PUT. A flush that is later
//!   abandoned is therefore counted in both `flushes_by_*` **and** one of the
//!   `abandoned_*` counters. Flushes that actually reached a durable commit
//!   are the three trigger counters summed, minus `abandoned_retry_exhausted`
//!   and `abandoned_input_rejected`; the bare trigger sum overcounts.
//! - **Success-time.** `acks_ok`/`acks_err` are recorded when a flush's strict
//!   waiters are acked, i.e. at the flush's terminal outcome. They count
//!   strict-mode waiters only: a buffered-mode flush, or an age/size flush with
//!   no strict waiter attached, records zero on both. They are an ack-outcome
//!   counter, not a flush-outcome counter, despite sitting next to one.
//!
//! `flushes_manual` covers every `FlushTrigger::Manual` flush: an explicit
//! `FlushNow`, the `Shutdown` drain, and the channel-close drop-path drain
//! (`shard.rs` `run`). It is not exclusively operator-requested flushes.
//!
//! `put_retries` counts every retried PUT attempt on both the data-object and
//! the commit-record path (`put_data_object_with_retry` and
//! `publish_with_retry`); the first attempt of each is not a retry and is not
//! counted.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use ravel_types::TenantHash;

use crate::attribution::TenantPutAttribution;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushTrigger {
    Size,
    /// Buffer aged past the fixed `max_flush_delay` (or `max_flush_delay_idle`)
    /// threshold: adaptive delay disabled, or no observed-rate/RTT data yet.
    Age,
    /// Buffer aged past a per-(shard, tenant) threshold computed within the
    /// adaptive-delay corridor (ADR-0067 decision 3), rather than the fixed
    /// `max_flush_delay` constant.
    AgeAdaptive,
    Manual,
}

#[derive(Debug, Default)]
pub struct IngestMetrics {
    /// Flushes opened because the tenant buffer reached `target_bytes`.
    /// Attempt-time: incremented at flush open, so it includes flushes later
    /// abandoned.
    flushes_by_size: AtomicU64,
    /// Flushes opened because the tenant buffer aged past the fixed
    /// `max_flush_delay`/`max_flush_delay_idle` threshold. Attempt-time, same
    /// as `flushes_by_size`. When adaptive delay is enabled this still covers
    /// idle-threshold flushes and any flush opened before an adaptive
    /// estimate exists; see `flushes_by_age_adaptive` for the corridor-driven
    /// case (ADR-0067 decision 3).
    flushes_by_age: AtomicU64,
    /// Flushes opened because the tenant buffer aged past a per-(shard,
    /// tenant) threshold computed within the adaptive-delay corridor, rather
    /// than the fixed `max_flush_delay` constant (ADR-0067 decision 3).
    /// Zero unless adaptive delay is enabled. Attempt-time, same as
    /// `flushes_by_size`.
    flushes_by_age_adaptive: AtomicU64,
    /// Flushes opened by any `FlushTrigger::Manual` path: an explicit
    /// `FlushNow`, the `Shutdown` drain, or the channel-close drop-path drain.
    /// Attempt-time.
    flushes_manual: AtomicU64,
    /// Retried PUT attempts across both the data-object and commit-record
    /// paths. Excludes each path's first attempt.
    put_retries: AtomicU64,
    /// Flushes abandoned because a PUT exhausted its retry budget or
    /// `max_flush_lifetime` elapsed first (`WriteError::Abandoned`). A
    /// durability signal: the input was fine, the object store did not accept
    /// it in time. Nothing was acknowledged and the whole write stays
    /// retryable.
    abandoned_retry_exhausted: AtomicU64,
    /// Flushes abandoned because the input could not be turned into a durable
    /// object at all: the segment build, data-key derivation, or commit-record
    /// build failed (`WriteError::SegmentBuild`). A client signal: identical
    /// input will fail again, so the write is not retryable. Split out from
    /// `abandoned_retry_exhausted` because `error.rs` already treats the two
    /// causes differently.
    abandoned_input_rejected: AtomicU64,
    /// Cumulative bytes admitted into shard buffers at enqueue time.
    buffered_bytes_total: AtomicU64,
    /// Cumulative sample count admitted into shard buffers at enqueue time.
    buffered_points_total: AtomicU64,
    /// Strict-mode waiters acked with a commit token (success-time). Zero for
    /// buffered-mode and for flushes with no strict waiter.
    acks_ok: AtomicU64,
    /// Strict-mode waiters acked with a `WriteError` (success-time). Zero for
    /// buffered-mode and for flushes with no strict waiter.
    acks_err: AtomicU64,
    /// Batches rejected because two points shared a `series_id` under
    /// distinct canonical label sets (ADR-0005 fail-loud collision check).
    series_id_collisions: AtomicU64,
    /// Exemplars written into a flushed object's EXEMPLARS section
    /// (ADR-0047). Attempt-time, like `flushes_by_*`: counted when the flush
    /// is built, so a flush later abandoned counts here too.
    exemplars_written_total: AtomicU64,
    /// Exemplars discarded at flush rather than written: either their parent
    /// sample was not in this flush (so the object carries no measurement for
    /// them, and the writer would reject them) or they lost the flush-scoped
    /// per-series window cap. Keeps the drop visible now that some exemplars
    /// are kept (ADR-0047 decision 2), alongside the wire-side drop counters
    /// the normalize paths already report.
    exemplars_dropped_total: AtomicU64,
    /// Distinct shard actors observed dead by the router: its send half or a
    /// strict-mode ack found the shard channel closed, meaning the actor task
    /// ended (e.g. panicked) without the router shutting it down. Counted
    /// once per shard on the first observation, so it never exceeds
    /// `shard_count` and makes a permanently degraded process observable
    /// (docs/ingest.md "Metrics (self-observability)").
    shard_deaths: AtomicU64,
    /// Flushes failed closed because the router's cached provisioning view for
    /// the tenant was older than the refresh interval `C` (ADR-0052 section 3).
    /// The load-bearing staleness signal: a nonzero, growing value means the
    /// background refresher is not keeping views current and writes are being
    /// refused rather than routed on a possibly-missed activation.
    stale_provisioning_flushes: AtomicU64,
    /// Flushes routed on a last-known-good provisioning view past the refresh
    /// interval `C`, inside the bounded grace window, because the
    /// provisioning re-read could not complete but the cached view's validity
    /// horizon had not been crossed (`GenerationSwitch::try_grace_extend`).
    /// Degraded, not failed: distinct from `stale_provisioning_flushes`, which
    /// counts a flush that failed closed outright. A sustained rise here means
    /// the store is slow/throttled and this router is degraded-but-available
    /// rather than fleet-wide-outed.
    grace_extended_stale_flushes: AtomicU64,
    /// Per-shard count of flushes whose flush task has been spawned but has
    /// not yet acked its waiters (ADR-0067 decision 2 consequence: pipelining
    /// raises per-shard memory by up to `(max_inflight_flushes - 1)` flush
    /// windows, and this gauge is how that stays observable). Keyed by shard
    /// index; a shard with no flush in flight has no entry, equivalent to 0.
    /// Not part of [`IngestMetricsSnapshot`]'s flat counters because it is a
    /// gauge with a per-shard dimension, unlike everything else in this
    /// struct (see the module docs' "no per-shard dimension" note); read it
    /// via [`IngestMetrics::in_flight_flushes_by_shard`].
    in_flight_flushes: Mutex<HashMap<u32, i64>>,
    /// Per-shard ingest-skew accounting (issue #865): message throughput and
    /// the on-actor / flush-permit-wait / off-actor time split, so an argument
    /// about shard-actor throughput rests on measured per-shard figures rather
    /// than assertion.
    /// Like `in_flight_flushes` above, it carries a per-shard dimension the
    /// flat `Copy` [`IngestMetricsSnapshot`] cannot hold, so it is read via
    /// [`IngestMetrics::shard_skew_by_shard`] rather than folded into the
    /// snapshot.
    ///
    /// Lock-free, one [`ShardSkewAtomics`] per shard, indexed by shard number.
    /// `record_shard_enqueued`/`record_shard_processed` run once per `Write`
    /// message, so this path must not serialise every shard on one lock: a
    /// process-wide mutex here would add a contention point to the very path
    /// #865 measures and could manufacture the throughput limit it exists to
    /// test for. Preallocated to `shard_count` by [`IngestMetrics::new`] (the
    /// shard set is fixed for the process); `IngestMetrics::default()` allocates
    /// none, and any shard index outside the slice is dropped rather than
    /// counted, matching the best-effort nature of this observability.
    shard_skew: Box<[ShardSkewAtomics]>,
    /// Metadata-record GETs issued by the metric metadata sink's flush window
    /// (ADR-0085 decision 1). The ADR budgets one GET per tenant per window
    /// with a non-empty pending set, so this is the counter an operator checks
    /// that budget against. Exported as
    /// `ingest_metadata_flush_gets_total`.
    metadata_flush_gets: AtomicU64,
    /// Metadata-record CAS PUTs *attempted* by the sink's flush window
    /// (ADR-0085 decision 1), including an attempt that lost its CAS and was
    /// retried, so `puts - conflicts` is the number that landed. The ADR
    /// budgets at most one PUT per tenant per window in the conflict-free
    /// case. Exported as `ingest_metadata_flush_puts_total`.
    metadata_flush_puts: AtomicU64,
    /// Flush windows whose metadata update was dropped: a CAS that still
    /// conflicted after `max_cas_retries`, or any other read/write failure
    /// against the record. Never fatal to an ingest request (the flush is off
    /// the acknowledgement path entirely) and never silent, which is what this
    /// counter is for. Exported as `ingest_metadata_flush_dropped_total`.
    metadata_flush_dropped: AtomicU64,
    /// Metric family names not added to a tenant's metadata record because it
    /// was already at the per-tenant entry cap (ADR-0085 decision 1). The
    /// points themselves are still ingested and queryable; only the metadata
    /// entry is dropped. Exported as
    /// `ingest_metadata_entries_dropped_total`.
    metadata_entries_dropped: AtomicU64,
    /// Bounded-cardinality per-tenant PUT attribution (ADR-0076 decision 2).
    /// Unlike every flat counter above it carries a per-tenant dimension, so
    /// it is kept bounded by a top-K cap rather than exposed as an unbounded
    /// label, and is read via [`IngestMetrics::tenant_put_attribution`] rather
    /// than folded into [`IngestMetricsSnapshot`] (which is `Copy`). See
    /// [`crate::attribution`] for the counting convention and eviction policy.
    put_attribution: TenantPutAttribution,
}

/// Accumulator behind [`IngestMetrics::shard_skew`], one per shard. Each field
/// is an independent [`AtomicU64`] so the per-message enqueue/process path
/// updates its own shard with no lock and no cross-shard contention; the public
/// [`ShardSkewStats`] is its point-in-time copy with `queue_depth` derived. All
/// updates and reads use `Relaxed`: these are monotonic self-observability
/// counters, not a synchronisation edge, so a snapshot that catches
/// `messages_processed` incremented a moment before its matching `on_actor_ns`
/// addend simply reports a cumulative sum one message behind, which self-heals.
#[derive(Debug, Default)]
struct ShardSkewAtomics {
    messages_enqueued: AtomicU64,
    messages_processed: AtomicU64,
    on_actor_ns: AtomicU64,
    flush_permit_wait_ns: AtomicU64,
    off_actor_ns: AtomicU64,
}

/// Point-in-time per-shard ingest-skew figures (issue #865). Read via
/// [`IngestMetrics::shard_skew_by_shard`]; a benchmark reaches it through
/// [`crate::IngestRouter::metrics`], the same handle it already reads
/// [`IngestMetrics::in_flight_flushes_by_shard`] through.
///
/// # The three time spans
///
/// `on_actor_ns`, `flush_permit_wait_ns`, and `off_actor_ns` partition a
/// shard's flush pipeline into three spans that share no nanosecond of any
/// sampled interval. Each is bracketed on the injected [`crate::Clock`] at a
/// distinct code boundary in `shard.rs`, and the three boundaries are
/// consecutive:
///
/// 1. `on_actor_ns` runs from the actor pulling a `Write` message off its
///    channel to `handle_write` returning, **minus** any permit wait nested
///    inside that call.
/// 2. `flush_permit_wait_ns` runs from `flush_tenant` reaching the
///    `max_inflight_flushes` semaphore acquire to that acquire granting a
///    permit.
/// 3. `off_actor_ns` runs from the spawned flush task entering `run_flush`
///    (permit already held) to `run_flush` returning.
///
/// Spans 1 and 2 both accrue on the actor task and are therefore disjoint in
/// wall time as well: together they account for the actor's whole
/// `Write`-handling window, and `on_actor_ns + flush_permit_wait_ns` can never
/// exceed the wall time the actor spent handling messages. Span 3 accrues in
/// spawned tasks that run *concurrently* with the actor, which is the entire
/// point of ADR-0067's pipelining, so it is a sum over concurrent tasks: at
/// `max_inflight_flushes > 1` it can legitimately exceed wall time, and it
/// overlaps spans 1 and 2 in wall time. Overlapping in wall time is not
/// double-counting: no single sampled interval is added to two counters. In
/// particular, the interval an actor spends parked in span 2 is wall-time
/// concurrent with a *prior* flush's span 3, and is charged to the actor side
/// exactly once, as permit wait, never as actor work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ShardSkewStats {
    /// `Write` messages the router sent into this shard's channel.
    pub messages_enqueued: u64,
    /// `Write` messages this shard's actor pulled and handled. `FlushNow` and
    /// `Shutdown` are excluded from both this and `messages_enqueued`: they are
    /// control messages, not ingest load, and counting them would break the
    /// enqueued-minus-processed depth identity below.
    pub messages_processed: u64,
    /// Messages enqueued into this shard's channel but not yet processed by its
    /// actor, at snapshot time: `messages_enqueued - messages_processed`.
    /// Saturating, so a read that catches the two counters mid-update (enqueue
    /// runs on the router task, process on the actor task) reports 0 rather
    /// than underflowing.
    pub queue_depth: u64,
    /// Injected-`Clock` nanoseconds the actor task spent processing this
    /// shard's `Write` messages: the serial merge-and-pin work the actor
    /// genuinely serialises, and nothing else. It excludes the flush, which
    /// ADR-0067 moved into a spawned task (`off_actor_ns`), and it excludes the
    /// wait for a flush permit (`flush_permit_wait_ns`), which is a prior
    /// flush's duration rather than work this actor performs.
    pub on_actor_ns: u64,
    /// Injected-`Clock` nanoseconds this shard spent parked on the
    /// `max_inflight_flushes` semaphore before a flush task could be spawned
    /// (ADR-0067 decision 2). This runs on the actor task, but it is not actor
    /// work: the actor is stalled waiting for an earlier flush of this same
    /// shard to release its permit, so this is the shard's flush backpressure
    /// signal. A rising figure here says flushing is the bottleneck; a rising
    /// `on_actor_ns` says the single-threaded actor is.
    ///
    /// Counted for every flush trigger, not only the size trigger inside
    /// `handle_write`: an age or manual flush parks on the same semaphore.
    pub flush_permit_wait_ns: u64,
    /// Injected-`Clock` nanoseconds spent in this shard's flush tasks, which
    /// run OFF the actor (ADR-0067): exemplar admission, encode, and both PUTs.
    /// Measured from inside the spawned task with the permit already held, so
    /// it never re-counts `flush_permit_wait_ns`. Kept distinct from
    /// `on_actor_ns` so a measurement can tell an actor-thread bottleneck from
    /// a flush bottleneck -- the exact ambiguity a single figure cannot
    /// resolve.
    pub off_actor_ns: u64,
}

/// Point-in-time copy of [`IngestMetrics`] for scraping. See the
/// [module docs](self) for each field's timing convention.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestMetricsSnapshot {
    pub flushes_by_size: u64,
    pub flushes_by_age: u64,
    pub flushes_by_age_adaptive: u64,
    pub flushes_manual: u64,
    pub put_retries: u64,
    pub abandoned_retry_exhausted: u64,
    pub abandoned_input_rejected: u64,
    pub buffered_bytes_total: u64,
    pub buffered_points_total: u64,
    pub acks_ok: u64,
    pub acks_err: u64,
    pub series_id_collisions: u64,
    pub shard_deaths: u64,
    pub exemplars_written_total: u64,
    pub exemplars_dropped_total: u64,
    pub stale_provisioning_flushes: u64,
    pub grace_extended_stale_flushes: u64,
    /// `ingest_metadata_flush_gets_total` (ADR-0085 decision 1).
    pub metadata_flush_gets_total: u64,
    /// `ingest_metadata_flush_puts_total` (ADR-0085 decision 1). Counts PUT
    /// attempts, so a CAS-conflicted attempt is included.
    pub metadata_flush_puts_total: u64,
    /// `ingest_metadata_flush_dropped_total` (ADR-0085 decision 1).
    pub metadata_flush_dropped_total: u64,
    /// `ingest_metadata_entries_dropped_total` (ADR-0085 decision 1).
    pub metadata_entries_dropped_total: u64,
    /// Sum across shards of [`IngestMetrics::in_flight_flushes_by_shard`] at
    /// snapshot time. The per-shard breakdown does not fit this struct's flat
    /// Copy shape; call `in_flight_flushes_by_shard` directly for that.
    pub in_flight_flushes_total: u64,
}

impl IngestMetrics {
    /// Metrics for a router of `shard_count` shards. Preallocates the lock-free
    /// per-shard skew accumulators (issue #865): the shard set is fixed for the
    /// process, so the per-message path indexes straight into the slice with no
    /// lock. `IngestMetrics::default()` allocates none, which suits the many
    /// tests and sinks that never touch the per-shard counters.
    pub fn new(shard_count: u32) -> Self {
        IngestMetrics {
            shard_skew: (0..shard_count)
                .map(|_| ShardSkewAtomics::default())
                .collect(),
            ..Default::default()
        }
    }

    pub(crate) fn record_flush(&self, trigger: FlushTrigger) {
        let counter = match trigger {
            FlushTrigger::Size => &self.flushes_by_size,
            FlushTrigger::Age => &self.flushes_by_age,
            FlushTrigger::AgeAdaptive => &self.flushes_by_age_adaptive,
            FlushTrigger::Manual => &self.flushes_manual,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Adjusts shard `shard`'s in-flight-flush gauge by `delta` (+1 when a
    /// flush task is spawned, -1 when it ends, including on panic via
    /// `shard.rs`'s `InFlightFlushGuard`). Poison recovery rather than a
    /// panic on a poisoned lock: a gauge is best-effort self-observability,
    /// not a durability path, so a prior panicked holder must not take this
    /// one down with it.
    pub(crate) fn record_inflight_flush_delta(&self, shard: u32, delta: i64) {
        let mut map = self
            .in_flight_flushes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = map.entry(shard).or_insert(0);
        *entry += delta;
    }

    /// Point-in-time per-shard in-flight-flush counts, sorted by shard index.
    /// A shard with none in flight is simply absent, equivalent to 0.
    pub fn in_flight_flushes_by_shard(&self) -> Vec<(u32, u64)> {
        let map = self
            .in_flight_flushes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut counts: Vec<(u32, u64)> = map
            .iter()
            .map(|(&shard, &count)| (shard, count.max(0) as u64))
            .collect();
        counts.sort_unstable_by_key(|&(shard, _)| shard);
        counts
    }

    /// One `Write` message sent by the router into shard `shard`'s channel
    /// (issue #865). Enqueue-time: counted at the router's `send`, so
    /// `messages_enqueued - messages_processed` is the depth still in the
    /// channel.
    pub(crate) fn record_shard_enqueued(&self, shard: u32) {
        if let Some(s) = self.shard_skew.get(shard as usize) {
            s.messages_enqueued.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One `Write` message pulled and handled by shard `shard`'s actor, plus
    /// the injected-`Clock` nanoseconds the actor spent handling it -- the
    /// on-actor serial section, excluding both the flush that runs off the
    /// actor and any flush-permit wait the caller already subtracted (issue
    /// #865). The `on_actor_ns` addend is stored before the processed count is
    /// bumped, so a concurrent reader is at worst one message behind on the
    /// count, never crediting a processed message with no time (see
    /// [`ShardSkewAtomics`] on the `Relaxed` ordering).
    pub(crate) fn record_shard_processed(&self, shard: u32, on_actor_ns: u64) {
        if let Some(s) = self.shard_skew.get(shard as usize) {
            s.on_actor_ns.fetch_add(on_actor_ns, Ordering::Relaxed);
            s.messages_processed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// One flush trigger's injected-`Clock` wait on shard `shard`'s
    /// `max_inflight_flushes` semaphore (issue #865). Recorded by `flush_tenant`
    /// for every trigger, and excluded by the actor loop from the `on_actor_ns`
    /// it reports for the same message: the wait is a prior flush's duration,
    /// so charging it as actor work would say "the actor is busy" exactly when
    /// the truth is "flushes are backed up".
    pub(crate) fn record_shard_flush_permit_wait_ns(&self, shard: u32, wait_ns: u64) {
        if let Some(s) = self.shard_skew.get(shard as usize) {
            s.flush_permit_wait_ns.fetch_add(wait_ns, Ordering::Relaxed);
        }
    }

    /// One completed flush task's injected-`Clock` nanoseconds, attributed to
    /// the shard it flushed (issue #865). Bracketed inside the spawned task
    /// with the permit already held (ADR-0067), so it re-counts neither the
    /// actor's `on_actor_ns` nor the `flush_permit_wait_ns` that preceded the
    /// spawn. It does run concurrently with the actor in wall time, which is
    /// what pipelining is for; see [`ShardSkewStats`] for why that is not
    /// double-counting.
    pub(crate) fn record_shard_off_actor_ns(&self, shard: u32, off_actor_ns: u64) {
        if let Some(s) = self.shard_skew.get(shard as usize) {
            s.off_actor_ns.fetch_add(off_actor_ns, Ordering::Relaxed);
        }
    }

    /// Point-in-time per-shard skew figures, sorted by shard index (issue
    /// #865). A shard with no recorded activity is simply absent. `queue_depth`
    /// is derived here as `messages_enqueued - messages_processed`, saturating
    /// at 0.
    pub fn shard_skew_by_shard(&self) -> Vec<(u32, ShardSkewStats)> {
        // Preallocated by shard index, so iteration is already shard-ordered. A
        // shard that never enqueued or processed a message reads all-zero and is
        // omitted, matching the prior map's "absent, equivalent to all-zero".
        self.shard_skew
            .iter()
            .enumerate()
            .filter_map(|(shard, s)| {
                let messages_enqueued = s.messages_enqueued.load(Ordering::Relaxed);
                let messages_processed = s.messages_processed.load(Ordering::Relaxed);
                let on_actor_ns = s.on_actor_ns.load(Ordering::Relaxed);
                let flush_permit_wait_ns = s.flush_permit_wait_ns.load(Ordering::Relaxed);
                let off_actor_ns = s.off_actor_ns.load(Ordering::Relaxed);
                if messages_enqueued == 0
                    && messages_processed == 0
                    && on_actor_ns == 0
                    && flush_permit_wait_ns == 0
                    && off_actor_ns == 0
                {
                    return None;
                }
                Some((
                    shard as u32,
                    ShardSkewStats {
                        messages_enqueued,
                        messages_processed,
                        queue_depth: messages_enqueued.saturating_sub(messages_processed),
                        on_actor_ns,
                        flush_permit_wait_ns,
                        off_actor_ns,
                    },
                ))
            })
            .collect()
    }

    /// Attribute one completed flush's PUTs to `tenant` (success-time,
    /// ADR-0076 decision 2). Called from the terminal success path of
    /// `shard.rs`'s `run_flush`, after both the data-object and commit-record
    /// PUTs have landed.
    pub(crate) fn record_flush_puts(&self, tenant: TenantHash) {
        self.put_attribution.record_flush(tenant);
    }

    /// The bounded-cardinality per-tenant PUT attribution (ADR-0076 decision
    /// 2). A caller outside this crate reads the current top-K via
    /// [`TenantPutAttribution::top_n`]; a later operator-facing endpoint (T4)
    /// consumes it. Reachable from `IngestRouter` through
    /// [`crate::IngestRouter::metrics`].
    pub fn tenant_put_attribution(&self) -> &TenantPutAttribution {
        &self.put_attribution
    }

    pub(crate) fn record_put_retry(&self) {
        self.put_retries.fetch_add(1, Ordering::Relaxed);
    }

    /// A flush abandoned by retry-budget or lifetime exhaustion
    /// (`WriteError::Abandoned`): a durability signal, retryable.
    pub(crate) fn record_abandoned_retry_exhausted(&self) {
        self.abandoned_retry_exhausted
            .fetch_add(1, Ordering::Relaxed);
    }

    /// A flush abandoned because the input could not be built into a durable
    /// object (`WriteError::SegmentBuild`): a client signal, not retryable.
    pub(crate) fn record_abandoned_input_rejected(&self) {
        self.abandoned_input_rejected
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_buffered(&self, bytes: u64, points: u64) {
        self.buffered_bytes_total
            .fetch_add(bytes, Ordering::Relaxed);
        self.buffered_points_total
            .fetch_add(points, Ordering::Relaxed);
    }

    pub(crate) fn record_acks(&self, count: usize, ok: bool) {
        let counter = if ok { &self.acks_ok } else { &self.acks_err };
        counter.fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(crate) fn record_series_id_collision(&self) {
        self.series_id_collisions.fetch_add(1, Ordering::Relaxed);
    }

    /// One flush's exemplar outcome: `written` reached the object's EXEMPLARS
    /// section, `dropped` did not (no parent sample in the flush, or lost the
    /// flush-scoped window cap).
    pub(crate) fn record_exemplars(&self, written: u64, dropped: u64) {
        self.exemplars_written_total
            .fetch_add(written, Ordering::Relaxed);
        self.exemplars_dropped_total
            .fetch_add(dropped, Ordering::Relaxed);
    }

    pub(crate) fn record_shard_death(&self) {
        self.shard_deaths.fetch_add(1, Ordering::Relaxed);
    }

    /// One flush refused because the router's cached provisioning view for the
    /// tenant exceeded the refresh interval `C` (ADR-0052 section 3, fail
    /// closed).
    pub(crate) fn record_stale_provisioning_flush(&self) {
        self.stale_provisioning_flushes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// One flush routed on a last-known-good provisioning view inside the
    /// bounded grace window (ADR-0052 degraded-safe fallback).
    pub(crate) fn record_grace_extended_stale_flush(&self) {
        self.grace_extended_stale_flushes
            .fetch_add(1, Ordering::Relaxed);
    }

    /// One metadata-record GET issued by a flush window (ADR-0085 decision 1).
    pub(crate) fn record_metadata_flush_get(&self) {
        self.metadata_flush_gets.fetch_add(1, Ordering::Relaxed);
    }

    /// One metadata-record CAS PUT attempted by a flush window. Counted per
    /// attempt, so a conflicted-and-retried write counts more than once.
    pub(crate) fn record_metadata_flush_put(&self) {
        self.metadata_flush_puts.fetch_add(1, Ordering::Relaxed);
    }

    /// One flush window's metadata update dropped (CAS retries exhausted, or a
    /// read/write failure against the record). Visible, never fatal.
    pub(crate) fn record_metadata_flush_dropped(&self) {
        self.metadata_flush_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// `count` new family names not stored because the tenant's record was
    /// already at the per-tenant entry cap. The points stay ingested.
    pub(crate) fn record_metadata_entries_dropped(&self, count: u64) {
        self.metadata_entries_dropped
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> IngestMetricsSnapshot {
        IngestMetricsSnapshot {
            flushes_by_size: self.flushes_by_size.load(Ordering::Relaxed),
            flushes_by_age: self.flushes_by_age.load(Ordering::Relaxed),
            flushes_by_age_adaptive: self.flushes_by_age_adaptive.load(Ordering::Relaxed),
            flushes_manual: self.flushes_manual.load(Ordering::Relaxed),
            put_retries: self.put_retries.load(Ordering::Relaxed),
            abandoned_retry_exhausted: self.abandoned_retry_exhausted.load(Ordering::Relaxed),
            abandoned_input_rejected: self.abandoned_input_rejected.load(Ordering::Relaxed),
            buffered_bytes_total: self.buffered_bytes_total.load(Ordering::Relaxed),
            buffered_points_total: self.buffered_points_total.load(Ordering::Relaxed),
            acks_ok: self.acks_ok.load(Ordering::Relaxed),
            acks_err: self.acks_err.load(Ordering::Relaxed),
            series_id_collisions: self.series_id_collisions.load(Ordering::Relaxed),
            shard_deaths: self.shard_deaths.load(Ordering::Relaxed),
            exemplars_written_total: self.exemplars_written_total.load(Ordering::Relaxed),
            exemplars_dropped_total: self.exemplars_dropped_total.load(Ordering::Relaxed),
            stale_provisioning_flushes: self.stale_provisioning_flushes.load(Ordering::Relaxed),
            grace_extended_stale_flushes: self.grace_extended_stale_flushes.load(Ordering::Relaxed),
            metadata_flush_gets_total: self.metadata_flush_gets.load(Ordering::Relaxed),
            metadata_flush_puts_total: self.metadata_flush_puts.load(Ordering::Relaxed),
            metadata_flush_dropped_total: self.metadata_flush_dropped.load(Ordering::Relaxed),
            metadata_entries_dropped_total: self.metadata_entries_dropped.load(Ordering::Relaxed),
            in_flight_flushes_total: self
                .in_flight_flushes_by_shard()
                .into_iter()
                .map(|(_, count)| count)
                .sum(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_reflects_recorded_counters() {
        let metrics = IngestMetrics::default();
        metrics.record_flush(FlushTrigger::Size);
        metrics.record_flush(FlushTrigger::Age);
        metrics.record_flush(FlushTrigger::Age);
        metrics.record_flush(FlushTrigger::Manual);
        metrics.record_put_retry();
        metrics.record_abandoned_retry_exhausted();
        metrics.record_buffered(100, 3);
        metrics.record_acks(2, true);
        metrics.record_acks(1, false);
        metrics.record_series_id_collision();
        metrics.record_shard_death();
        metrics.record_exemplars(2, 5);

        let snap = metrics.snapshot();
        assert_eq!(snap.exemplars_written_total, 2);
        assert_eq!(snap.exemplars_dropped_total, 5);
        assert_eq!(snap.flushes_by_size, 1);
        assert_eq!(snap.flushes_by_age, 2);
        assert_eq!(snap.flushes_manual, 1);
        assert_eq!(snap.put_retries, 1);
        assert_eq!(snap.abandoned_retry_exhausted, 1);
        assert_eq!(snap.abandoned_input_rejected, 0);
        assert_eq!(snap.buffered_bytes_total, 100);
        assert_eq!(snap.buffered_points_total, 3);
        assert_eq!(snap.acks_ok, 2);
        assert_eq!(snap.acks_err, 1);
        assert_eq!(snap.series_id_collisions, 1);
        assert_eq!(snap.shard_deaths, 1);
    }

    #[test]
    fn age_adaptive_trigger_counts_separately_from_fixed_age() {
        let metrics = IngestMetrics::default();
        metrics.record_flush(FlushTrigger::Age);
        metrics.record_flush(FlushTrigger::AgeAdaptive);
        metrics.record_flush(FlushTrigger::AgeAdaptive);

        let snap = metrics.snapshot();
        assert_eq!(snap.flushes_by_age, 1);
        assert_eq!(snap.flushes_by_age_adaptive, 2);
    }

    #[test]
    fn in_flight_flush_gauge_tracks_per_shard_deltas() {
        let metrics = IngestMetrics::default();
        metrics.record_inflight_flush_delta(0, 1);
        metrics.record_inflight_flush_delta(0, 1);
        metrics.record_inflight_flush_delta(1, 1);
        assert_eq!(metrics.in_flight_flushes_by_shard(), vec![(0, 2), (1, 1)]);
        assert_eq!(metrics.snapshot().in_flight_flushes_total, 3);

        metrics.record_inflight_flush_delta(0, -1);
        assert_eq!(metrics.in_flight_flushes_by_shard(), vec![(0, 1), (1, 1)]);
        assert_eq!(metrics.snapshot().in_flight_flushes_total, 2);
    }

    #[test]
    fn shard_skew_tracks_per_shard_throughput_and_time_split() {
        let metrics = IngestMetrics::new(2);
        // Shard 0 takes three messages, shard 1 takes one: a skewed load.
        metrics.record_shard_enqueued(0);
        metrics.record_shard_enqueued(0);
        metrics.record_shard_enqueued(0);
        metrics.record_shard_enqueued(1);
        // Two of shard 0's three, and shard 1's one, have been processed; the
        // third message for shard 0 is still in the channel.
        metrics.record_shard_processed(0, 100);
        metrics.record_shard_processed(0, 200);
        metrics.record_shard_processed(1, 50);
        // Flush time lands off the actor, and only on shard 0 here.
        metrics.record_shard_off_actor_ns(0, 9_000);
        // Shard 0's second flush trigger found the semaphore at its bound and
        // parked; that wait is its own span, not part of either figure above.
        metrics.record_shard_flush_permit_wait_ns(0, 700);

        let skew = metrics.shard_skew_by_shard();
        assert_eq!(
            skew,
            vec![
                (
                    0,
                    ShardSkewStats {
                        messages_enqueued: 3,
                        messages_processed: 2,
                        queue_depth: 1,
                        on_actor_ns: 300,
                        flush_permit_wait_ns: 700,
                        off_actor_ns: 9_000,
                    }
                ),
                (
                    1,
                    ShardSkewStats {
                        messages_enqueued: 1,
                        messages_processed: 1,
                        queue_depth: 0,
                        on_actor_ns: 50,
                        flush_permit_wait_ns: 0,
                        off_actor_ns: 0,
                    }
                ),
            ]
        );
    }

    /// The three time spans accumulate independently: a permit wait recorded
    /// for a shard must move neither `on_actor_ns` nor `off_actor_ns`, and
    /// repeated waits sum rather than replace. Pins that the third counter was
    /// added as its own accumulator, not carved out of one of the other two.
    #[test]
    fn flush_permit_wait_accumulates_without_touching_the_other_two_spans() {
        let metrics = IngestMetrics::new(1);
        metrics.record_shard_processed(0, 40);
        metrics.record_shard_off_actor_ns(0, 500);
        metrics.record_shard_flush_permit_wait_ns(0, 60);
        metrics.record_shard_flush_permit_wait_ns(0, 90);

        let skew = metrics.shard_skew_by_shard();
        assert_eq!(
            skew,
            vec![(
                0,
                ShardSkewStats {
                    messages_enqueued: 0,
                    messages_processed: 1,
                    queue_depth: 0,
                    on_actor_ns: 40,
                    flush_permit_wait_ns: 150,
                    off_actor_ns: 500,
                }
            )]
        );
    }

    #[test]
    fn shard_queue_depth_saturates_when_processed_leads_enqueued() {
        // The two counters are updated on different tasks (enqueue on the
        // router, process on the actor), so a snapshot can catch a processed
        // increment before its enqueue is visible. Depth must read 0, never
        // underflow to u64::MAX.
        let metrics = IngestMetrics::new(3);
        metrics.record_shard_processed(2, 0);
        let skew = metrics.shard_skew_by_shard();
        assert_eq!(
            skew,
            vec![(
                2,
                ShardSkewStats {
                    messages_enqueued: 0,
                    messages_processed: 1,
                    queue_depth: 0,
                    on_actor_ns: 0,
                    flush_permit_wait_ns: 0,
                    off_actor_ns: 0,
                }
            )]
        );
    }

    /// Item 3 (issue #865 review): the per-message enqueue/process path is
    /// lock-free per shard. It must still count exactly under concurrent
    /// multi-shard load, and concurrent writers to one shard must not lose an
    /// increment to a racing `fetch_add`. Several threads hammer each shard at
    /// once: every increment must land on its own shard, and every shard's
    /// totals must equal the number of messages driven into it.
    ///
    /// This is the evidence the review asked for that the lock-free rewrite
    /// loses no counts: an atomic `fetch_add` is read-modify-write, so a lost
    /// update would show here as a per-shard total below the exact expected
    /// figure. The process-wide mutex it replaced was the contention point on
    /// the very path #865 measures; per-shard atomics remove it without giving
    /// up exactness.
    #[test]
    fn shard_skew_counts_are_exact_under_concurrent_multi_shard_load() {
        use std::sync::Arc;
        use std::thread;

        const SHARDS: u32 = 8;
        const THREADS_PER_SHARD: u64 = 4;
        const MSGS_PER_THREAD: u64 = 5_000;
        const PER_MSG_NS: u64 = 7;
        let expected_per_shard = THREADS_PER_SHARD * MSGS_PER_THREAD;

        let metrics = Arc::new(IngestMetrics::new(SHARDS));
        let mut handles = Vec::new();
        for shard in 0..SHARDS {
            for _ in 0..THREADS_PER_SHARD {
                let metrics = Arc::clone(&metrics);
                handles.push(thread::spawn(move || {
                    for _ in 0..MSGS_PER_THREAD {
                        metrics.record_shard_enqueued(shard);
                        metrics.record_shard_processed(shard, PER_MSG_NS);
                    }
                }));
            }
        }
        for h in handles {
            h.join().expect("skew writer thread");
        }

        let skew = metrics.shard_skew_by_shard();
        assert_eq!(
            skew.len(),
            SHARDS as usize,
            "every shard recorded activity: {skew:?}"
        );
        for (shard, stats) in skew {
            assert_eq!(
                stats.messages_enqueued, expected_per_shard,
                "shard {shard} enqueued count is exact under concurrent load"
            );
            assert_eq!(
                stats.messages_processed, expected_per_shard,
                "shard {shard} processed count is exact under concurrent load"
            );
            assert_eq!(
                stats.on_actor_ns,
                expected_per_shard * PER_MSG_NS,
                "shard {shard} on-actor time sums every message exactly"
            );
            assert_eq!(
                stats.queue_depth, 0,
                "shard {shard} enqueued and processed counts match, so depth is 0"
            );
        }
    }

    #[test]
    fn abandoned_causes_are_counted_separately() {
        let metrics = IngestMetrics::default();
        // Two durability abandonments (retry/lifetime exhaustion) and one
        // input rejection (segment build failed). The split lets an operator
        // tell a store problem from a bad-input problem by counter alone, which a single
        // `abandoned_flushes` counter could not.
        metrics.record_abandoned_retry_exhausted();
        metrics.record_abandoned_retry_exhausted();
        metrics.record_abandoned_input_rejected();

        let snap = metrics.snapshot();
        assert_eq!(snap.abandoned_retry_exhausted, 2);
        assert_eq!(snap.abandoned_input_rejected, 1);
    }
}
