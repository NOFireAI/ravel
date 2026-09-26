//! Background at-rest integrity scrubber task (ADR-0059 decisions 1, 3, 4). The scheduling half of the durability-hardening scrubber: the
//! per-object verification logic lives in [`ravel_maintain::scrub`] (part 1 of
//! this issue) and is unit-tested there; this module is the periodic loop that
//! drives it over a real object corpus, exactly the mechanism/lifecycle split
//! every other background loop in this crate uses ([`crate::maintain`],
//! [`crate::admission_reconcile`]).
//!
//! # One tick
//!
//! Each tick re-discovers the tenant set from storage via the flag-restriction
//! [`crate::tenant_discovery::discover_and_restrict`] and, for every
//! `(tenant, signal, shard)` it holds data for, runs the content tier over the
//! shard's committed data objects at every level: L0 commit records, and the
//! L1 and rewrite parts a compaction or erasure-rewrite record supersedes them
//! with.
//!
//! The lineage filter applies to the parts only, and it leaves out three
//! shapes. A compaction or rewrite record another rewrite record names in
//! `superseded_record_key` is left out, and so is one in a tombstoned bucket:
//! retention's sweep may delete either one's parts at any time, and no query
//! reads them meanwhile. A compaction record that loses its bucket's overlap
//! component to another compaction record (two compactors racing, resolved
//! through `ravel_catalog::select_authoritative_compaction_records`, the same
//! selection the read path uses) is left out too, with a caveat the first two
//! shapes do not carry: a node that has not adopted the overlap rule may still
//! serve the loser's parts, and the loser is not horizon-bounded, so the sweep
//! never reclaims them. L0 commit records carry no such check (the commit
//! record arm below tests neither supersession, overlap, nor a tombstone for
//! them), so an L0 object a live compaction already folded is
//! still scrubbed, and a `level="l0"` mismatch on an already-compacted hour
//! may name a copy nothing reads: the catalog puts a live compaction record's
//! input identities into the query-time excluded set. (The
//! maintenance and fold supervisors
//! have since moved to the lifecycle-aware
//! [`crate::tenant_discovery::discover_and_restrict_by_lifecycle`] under
//! ADR-0066 decision 6; the scrubber has not yet been migrated, so its tenant
//! set is still the startup flag restriction rather than the durable per-tenant
//! lifecycle records.)
//!
//! The content tier walks the shard with a start-after marker (ADR-1686), so a
//! tick's LIST and GET count depends on its budget, not on the corpus size,
//! except on the tick that opens a rotation: its LIST-only count (step 1)
//! lists the whole commit shard prefix.
//!
//! 1. Load this shard's persisted [`ScrubCursor`]. A cursor GET that fails for
//!    any reason other than `NotFound` skips the tick and leaves the stored
//!    cursor alone, since starting over would discard the rotation's progress
//!    on a transient fault. When the cursor has no entry count for the current
//!    rotation (the first rotation, the one after a completed rotation, or a
//!    cursor an older build wrote), count the entries under the commit shard
//!    prefix with a LIST-only pass and open a rotation.
//! 2. Otherwise recount the rotation's tail window, the last two ingest hours
//!    the previous count met and everything after them, with a LIST-only
//!    pass, and add its growth to the rotation's estimate
//!    ([`TailTally`]), so a shard that keeps committing cannot outrun the
//!    walk. Recounting whole hours catches a commit from every writer, not
//!    only from one whose id sorts above the keys already listed.
//! 3. Plan the tick ([`ScrubCursor::plan_tick`]): the rotation is allotted
//!    `min(P, retention window / 2)` and what is left to cover is divided by the
//!    ticks remaining before that deadline, bounded by
//!    [`ravel_maintain::SCRUB_MAX_CATCHUP`] times the sustained rate. A plan
//!    past that ceiling is `behind`: the rotation cannot finish in time, which
//!    increments `ravel_scrub_behind_total` and logs both numbers.
//! 4. List strictly after the cursor's marker and consume entries until either
//!    cap of the plan's budget is filled, verifying each unit's objects as the
//!    walk reaches them. A commit record is a unit on its own; the compaction
//!    records, rewrite records, and tombstone of an hour, which sort after
//!    every commit record under that hour, form one unit with the rest of the
//!    hour, and when an earlier tick already consumed part of that set the
//!    unit re-lists the hour so lineage selection still sees all of it.
//!    Decoding a unit yields the objects it names ([`ScrubTarget`]s), each
//!    with the record to verify it against and its
//!    [`ravel_maintain::ScrubLevel`]. The level lives beside the target alone,
//!    so the label a mismatch is counted under has one owner. Every listing
//!    page, every record GET attempt, and
//!    [`ravel_maintain::SCRUB_REQUESTS_PER_OBJECT`] per object verified count
//!    against the request cap, so a tick whose GETs all fail still stops.
//! 5. Verify each object via
//!    [`scrub_one_object`](ravel_maintain::scrub_one_object) and record any
//!    anomaly on the metrics counters below.
//! 6. A unit where any GET, of a record or of an object it names, failed with
//!    a retryable store error ([`StoreError::is_retryable`]: `Throttled`,
//!    `Timeout`, `Transient`) is not consumed: nothing it found is counted,
//!    the marker stays behind it and the tick ends, so the next tick retries
//!    the whole unit rather than skipping its objects for a whole rotation.
//!    Every other failure moves the marker on. A GET that fails with an error
//!    retrying cannot clear (`Permanent`, `AccessDenied`, `Corrupted`, and
//!    every other kind but `NotFound`), and a record whose bytes do not
//!    decode, are counted once each on `ravel_scrub_unreadable_total` at the
//!    level of the object or record that failed, with `reason` telling an
//!    access denial from every other kind. They are not checksum mismatches:
//!    `ravel_scrub_checksum_mismatch_total` counts only bytes that were read
//!    and did not verify. A record or object that is `NotFound` (deleted after
//!    it was listed) and an object whose check hit an input inconsistency are
//!    logged and skipped.
//! 7. Persist the cursor with the marker on the last entry consumed. When the
//!    listing ended past the marker, the rotation rolls over instead: the
//!    marker clears and the next rotation is counted afresh, completing a full
//!    rotation over the shard within `min(P, retention window / 2)` whenever the
//!    shard's commit rate stays inside the catch-up ceiling.
//!
//! # Why Maintain-mode only
//!
//! Scrubbing is at-rest verification: it depends on neither ingest nor query
//! traffic and it is the same class of background housekeeping compaction,
//! retention, and the GC sweep already are (`crate::maintain`, gated on
//! [`Mode::Maintain`](crate::config::Mode::Maintain)). Running it in an
//! ingest- or query-serving process would burn `O(corpus bytes / P)` sustained
//! read bandwidth on a process whose job is the hot path. So this task is
//! spawned in exactly the same mode the maintenance loop is, and nowhere
//! else.
//!
//! ADR-0065 makes the Maintain role N-replica, so the persisted per-shard
//! cursor ([`persist_cursor`], written `PutOptions::default()` = Overwrite, no
//! CAS) no longer has a true single writer: two processes can both own a shard
//! briefly during a membership transition, or both fall back to `{self}` during
//! a live-set read outage (worker_set.rs), and both `Overwrite` the same
//! cursor. The worst case of that clobber is bounded precisely, not merely
//! "benign":
//!
//! - A BACKWARD clobber (a replica writes a cursor position behind another
//!   replica's) only makes the next tick re-scrub a slice already verified this
//!   rotation: wasted read bandwidth, no coverage lost.
//! - A FORWARD clobber (a replica writes a position ahead of another's) makes
//!   the trailing replica resume past a slice that neither verified this
//!   rotation, so that slice goes unscrubbed until the rotation wraps. It is not
//!   lost: the next rotation starts again from the head of the listing and
//!   re-covers every slice. The effect is purely a promptness cost -- it
//!   DELAYS detection of a corruption in that one slice by at most one full
//!   rotation period (the scrub period `P`, [`DEFAULT_SCRUB_PERIOD`] = 7 days
//!   at defaults).
//!
//! Nothing is lost because scrub is detection-only (it never repairs, see
//! below): delaying detection by up to one rotation changes only when an alarm
//! fires, never what is or is not recoverable. So this is a bounded-promptness
//! property on a racing overwrite, not a single-writer invariant. Adding CAS to
//! [`persist_cursor`] would remove even that one-rotation forward-clobber delay,
//! but it is not required for safety and is deliberately not done here.
//!
//! # Detection only, never repair
//!
//! Like the library it drives, this task detects and alarms; it never repairs
//! (ADR-0059 consequences; there is no redundant copy to repair a corrupt
//! segment from, ADR-0058). A [`ScrubResult::ReadError`] is a retryable store
//! error, a missing object, or a decode inconsistency, not corruption: it is
//! logged and never counted as an anomaly, and only the retryable kind holds
//! the marker for a retry on the next tick. A [`ScrubResult::Unreadable`]
//! object is not corruption either, since its bytes were never read; it is
//! counted apart from the corruption counters.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_commit::keys;
use ravel_ingest::{Clock as _, SystemClock};
use ravel_maintain::{
    Clock, RetentionConfig, SCRUB_REQUESTS_PER_OBJECT, ScrubLevel, ScrubResult, ScrubTarget,
    UnreadableReason, WorkerSet, scrub_one_object,
};
use ravel_maintain::{ScrubCursor, TailTally};
use ravel_object_store::{
    DrainStep, GetRange, MAX_LIST_PAGES, ObjectStoreBackend, PageToken, PutOptions, StoreError,
    drain_pages,
};
use ravel_types::{Signal, TenantHash};
use std::collections::VecDeque;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::fold::jittered;
use crate::maintain::MAINTAINED_SIGNALS;
use crate::tenant_discovery::discover_and_restrict;

/// Default scrub period `P` (ADR-0059 decision 1): a full content-tier rotation
/// over the whole corpus completes in about 7 days, so sustained scrub read
/// bandwidth is bounded at `corpus_bytes / (7 * 86400)` bytes/sec. Mirrors the
/// precedent of `ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL` and
/// `ravel_maintain::config`'s GC defaults: one named constant an operator sizes
/// against their own corpus (the `--scrub-period` flag overrides it).
pub const DEFAULT_SCRUB_PERIOD: Duration = Duration::from_secs(7 * 86_400);

/// Upper bound on the loop's tick cadence. A rotation of period `P` is walked
/// in ticks of at most this length, so a long default `P` still wakes at a
/// steady cadence and makes bounded progress each tick rather than doing one
/// enormous slice per day. When `P` itself is shorter than this (a small test
/// period), the tick shrinks to `P` so the whole corpus is covered in one tick.
const DEFAULT_SCRUB_TICK: Duration = Duration::from_secs(3600);

/// Position of `signal` within [`MAINTAINED_SIGNALS`], and therefore within
/// [`ScrubMetrics`]'s per-signal arrays. Exhaustive over the signals this task
/// loops over; a signal from outside that set is a caller bug, matching
/// [`crate::maintain`]'s own `signal_index`.
fn signal_index(signal: Signal) -> usize {
    match signal {
        Signal::Metrics => 0,
        Signal::Logs => 1,
        Signal::Spans => 2,
        other => {
            unreachable!("scrub metrics only track MAINTAINED_SIGNALS, got {other:?}")
        }
    }
}

/// Number of [`ScrubLevel`] variants, and therefore the width of
/// [`ScrubMetrics`]'s per-(signal, level) `checksum_mismatch` array.
const SCRUB_LEVELS: usize = 3;

/// Position of `level` within a signal's `checksum_mismatch` row. Exhaustive
/// over every [`ScrubLevel`] variant, matching [`signal_index`]'s discipline.
fn level_index(level: ScrubLevel) -> usize {
    match level {
        ScrubLevel::L0 => 0,
        ScrubLevel::L1 => 1,
        ScrubLevel::Rewrite => 2,
    }
}

/// Number of [`UnreadableReason`] variants, the innermost width of
/// [`ScrubMetrics`]'s `unreadable` array.
const UNREADABLE_REASONS: usize = UnreadableReason::ALL.len();

/// Position of `reason` within one (signal, level) cell of `unreadable`.
fn reason_index(reason: UnreadableReason) -> usize {
    match reason {
        UnreadableReason::AccessDenied => 0,
        UnreadableReason::Permanent => 1,
        UnreadableReason::RetryExhausted => 2,
    }
}

/// Process-global counters for the scrubber (ADR-0059 decision 3), rendered on
/// the existing `GET /metrics` endpoint by
/// [`crate::metrics::render_scrub_family`] with no second registry, following
/// [`crate::maintain::MaintenanceSafetyMetrics`]'s conventions exactly:
/// per-signal counters indexed by [`signal_index`], and deliberately no
/// `tenant_hash` label (ADR-0044 section 4 blocks per-tenant series on the
/// unauthenticated `/metrics` route).
///
/// Structural corruption and a content-hash mismatch are bytes that were read
/// and did not verify, so both increment `checksum_mismatch`. An object or
/// record the store refuses with an error retrying cannot clear
/// ([`ScrubResult::Unreadable`]), or a record that does not decode, was never
/// verified at all and increments `unreadable` under its
/// [`UnreadableReason`]; a [`ScrubResult::ReadError`] increments nothing. Both
/// carry a [`ScrubLevel`] dimension (`l0`/`l1`/`rewrite`, [`level_index`]):
/// the postings tier only ever runs against L0 objects, so
/// `postings_disagreement` stays signal-only.
///
/// Seal divergence (ADR-0059 decision 2) is a distinct, metadata-cost
/// check on the same tick: sealed commit records re-listed and diffed against the
/// folded snapshot. `missing` and `mismatched` divergences increment
/// `seal_divergence_*`; `orphaned` is the expected retention-after-fold shape and
/// increments nothing.
#[derive(Debug, Default)]
pub struct ScrubMetrics {
    checksum_mismatch: [[AtomicU64; SCRUB_LEVELS]; MAINTAINED_SIGNALS.len()],
    /// Objects and records the scrub could not read at all, per signal, level
    /// and [`UnreadableReason`]: `ravel_scrub_unreadable_total`.
    unreadable: [[[AtomicU64; UNREADABLE_REASONS]; SCRUB_LEVELS]; MAINTAINED_SIGNALS.len()],
    postings_disagreement: [AtomicU64; MAINTAINED_SIGNALS.len()],
    /// Sealed commit records absent from the folded snapshot (an under-count),
    /// per signal. The `reason="missing"` value of
    /// `ravel_scrub_seal_divergence_total`.
    seal_divergence_missing: [AtomicU64; MAINTAINED_SIGNALS.len()],
    /// Snapshot entries whose `content_hash` disagrees with the sealed commit
    /// record, per signal. The `reason="mismatched"` value of
    /// `ravel_scrub_seal_divergence_total`.
    seal_divergence_mismatched: [AtomicU64; MAINTAINED_SIGNALS.len()],
    /// Listing entries consumed so far in the current rotation, per signal
    /// (numerator of the cursor-position gauge, ADR-1686). Last-observed
    /// value, overwritten each shard tick, matching the "most recent pass"
    /// gauge discipline `orphans_withheld` keeps in [`crate::maintain`].
    rotation_covered: [AtomicU64; MAINTAINED_SIGNALS.len()],
    /// Listing entries the current rotation's LIST-only count found under the
    /// commit shard prefix, per signal (denominator of the cursor-position
    /// gauge).
    rotation_total: [AtomicU64; MAINTAINED_SIGNALS.len()],
    /// Shard ticks whose rotation cannot finish inside its window at the
    /// catch-up ceiling, per signal. `ravel_scrub_behind_total`: a nonzero
    /// rate means a sustained commit rate above the ceiling, cycles slower
    /// than a tick, or a marker held on a unit that keeps failing retryably,
    /// and some objects may expire unverified.
    rotation_behind: [AtomicU64; MAINTAINED_SIGNALS.len()],
}

impl ScrubMetrics {
    pub fn checksum_mismatch(&self, signal: Signal, level: ScrubLevel) -> u64 {
        self.checksum_mismatch[signal_index(signal)][level_index(level)].load(Ordering::Relaxed)
    }

    pub fn unreadable(&self, signal: Signal, level: ScrubLevel, reason: UnreadableReason) -> u64 {
        self.unreadable[signal_index(signal)][level_index(level)][reason_index(reason)]
            .load(Ordering::Relaxed)
    }

    pub fn postings_disagreement(&self, signal: Signal) -> u64 {
        self.postings_disagreement[signal_index(signal)].load(Ordering::Relaxed)
    }

    pub fn seal_divergence_missing(&self, signal: Signal) -> u64 {
        self.seal_divergence_missing[signal_index(signal)].load(Ordering::Relaxed)
    }

    pub fn seal_divergence_mismatched(&self, signal: Signal) -> u64 {
        self.seal_divergence_mismatched[signal_index(signal)].load(Ordering::Relaxed)
    }

    /// Shard ticks that reported a rotation which cannot finish inside its
    /// allotted window, for `signal`.
    pub fn rotation_behind(&self, signal: Signal) -> u64 {
        self.rotation_behind[signal_index(signal)].load(Ordering::Relaxed)
    }

    /// Fraction of the current rotation covered so far for `signal`, in
    /// `[0.0, 1.0]` (ADR-0059's `ravel_scrub_cursor_position` gauge, entry-based
    /// since ADR-1686). Derived from the entries-consumed / entries-counted pair
    /// the last shard tick recorded; `0.0` when the shard lists no entries
    /// (nothing to rotate over).
    pub fn cursor_position(&self, signal: Signal) -> f64 {
        let index = signal_index(signal);
        let total = self.rotation_total[index].load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        let covered = self.rotation_covered[index].load(Ordering::Relaxed);
        (covered as f64 / total as f64).clamp(0.0, 1.0)
    }

    fn record_checksum_mismatch(&self, signal: Signal, level: ScrubLevel) {
        self.checksum_mismatch[signal_index(signal)][level_index(level)]
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_unreadable(&self, signal: Signal, level: ScrubLevel, reason: UnreadableReason) {
        self.unreadable[signal_index(signal)][level_index(level)][reason_index(reason)]
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_postings_disagreement(&self, signal: Signal) {
        self.postings_disagreement[signal_index(signal)].fetch_add(1, Ordering::Relaxed);
    }

    fn record_seal_divergence_missing(&self, signal: Signal, count: u64) {
        self.seal_divergence_missing[signal_index(signal)].fetch_add(count, Ordering::Relaxed);
    }

    fn record_seal_divergence_mismatched(&self, signal: Signal, count: u64) {
        self.seal_divergence_mismatched[signal_index(signal)].fetch_add(count, Ordering::Relaxed);
    }

    fn record_rotation_behind(&self, signal: Signal) {
        self.rotation_behind[signal_index(signal)].fetch_add(1, Ordering::Relaxed);
    }

    fn record_cursor_position(&self, signal: Signal, covered: u64, total: u64) {
        let index = signal_index(signal);
        self.rotation_covered[index].store(covered, Ordering::Relaxed);
        self.rotation_total[index].store(total, Ordering::Relaxed);
    }
}

/// The service-layer wall clock for [`ravel_maintain::Clock`], delegating to
/// `ravel-ingest`'s [`SystemClock`] (the one blessed wall clock in this
/// process), matching [`crate::maintain`]'s own `WallClock` so no scrub code
/// path reads `SystemTime::now()` directly.
struct WallClock;

impl Clock for WallClock {
    fn now_ns(&self) -> i64 {
        SystemClock.now_ns()
    }
}

/// Handle to the spawned scrub task, so shutdown can stop it cleanly (mirrors
/// [`crate::admission_reconcile::AdmissionReconcileTask`]).
pub struct ScrubTask {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl ScrubTask {
    /// No task (every mode but Maintain, which alone runs at-rest verification).
    pub fn none() -> Self {
        ScrubTask {
            shutdown: None,
            handle: None,
        }
    }

    pub async fn shutdown(self) {
        if let Some(tx) = self.shutdown {
            let _ = tx.send(());
        }
        if let Some(handle) = self.handle {
            let _ = handle.await;
        }
    }
}

/// Spawn the scrub loop over `store`, sizing each tick's byte budget so a full
/// rotation over the corpus completes in about `period`. `restrict` is the
/// merged `--tenant-token`/`--maintain-tenant` set (empty means unconfigured:
/// every discovered tenant is scrubbed), matching the maintenance supervisor's
/// tenant scoping. Returns immediately; the task runs until
/// [`ScrubTask::shutdown`]. The first cycle sleeps a full (jittered) interval
/// before its first read, so co-started replicas do not scrub in lockstep.
#[allow(clippy::too_many_arguments)]
pub fn spawn(
    store: Arc<dyn ObjectStoreBackend>,
    restrict: Vec<TenantHash>,
    period: Duration,
    shard_count: u32,
    metrics: Arc<ScrubMetrics>,
    worker: Arc<WorkerSet>,
    retention: Arc<RetentionConfig>,
) -> ScrubTask {
    let restrict = if restrict.is_empty() {
        None
    } else {
        Some(restrict)
    };
    // A rotation of length `period` is walked in ticks of at most
    // `DEFAULT_SCRUB_TICK`; a shorter period (a small test period) shrinks the
    // tick to itself so the whole corpus is covered promptly.
    let tick = period.min(DEFAULT_SCRUB_TICK).max(Duration::from_millis(1));
    let period_secs = period.as_secs().max(1);
    let tick_secs = tick.as_secs().max(1);

    let (tx, mut rx) = oneshot::channel();
    // Production OS-entropy jitter (ADR-0068 decision 2), the same default the
    // fold and maintenance loops use; the harness does not drive scrub.
    let rng: Arc<dyn ravel_commit::rng::RngSource> = Arc::new(ravel_commit::rng::SystemRng);
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(jittered(tick, rng.as_ref())) => {}
                _ = &mut rx => return,
            }
            // Ownership gating (ADR-0065 decision 2): scrub shares the maintain
            // role's single `WorkerSet` (one process_id per process, so the
            // fleet sees one worker, not one per background loop). The maintain
            // supervisor writes the heartbeat on its `H` cadence; scrub only
            // reads the resulting live set each cycle to gate which shards it
            // rotates over. A live-set read failure falls back to `{self}`
            // (owns everything: the fail-open direction, never scrubbing less
            // than a lone process would), ADR-0065 decision 1.
            let now = SystemClock.now_ns();
            let live_set = worker
                .live_set(store.as_ref(), now)
                .await
                .unwrap_or_else(|err| {
                    tracing::warn!(
                        error = %err,
                        "scrub: worker live-set read failed; treating self as sole owner this cycle"
                    );
                    worker.solo_live_set()
                });
            run_cycle(
                store.as_ref(),
                restrict.as_deref(),
                shard_count,
                period_secs,
                tick_secs,
                metrics.as_ref(),
                worker.as_ref(),
                &live_set,
                Some(retention.as_ref()),
            )
            .await;
        }
    });
    ScrubTask {
        shutdown: Some(tx),
        handle: Some(handle),
    }
}

/// One discovery cycle: re-enumerate tenants from storage, narrow to `restrict`
/// when configured, then run one content-tier tick for each `(tenant, signal,
/// shard)`. A discovery failure (the LIST erroring) skips the whole cycle and
/// is logged, never falling back to an empty set (the same silent-failure trap
/// [`crate::maintain::run_discovery_cycle`] avoids). Split out from the loop so
/// a test can drive one deterministic cycle without the timer.
#[allow(clippy::too_many_arguments)]
pub async fn run_cycle(
    store: &dyn ObjectStoreBackend,
    restrict: Option<&[TenantHash]>,
    shard_count: u32,
    period_secs: u64,
    tick_secs: u64,
    metrics: &ScrubMetrics,
    worker: &WorkerSet,
    live_set: &[Uuid],
    retention: Option<&RetentionConfig>,
) {
    let outcome = match discover_and_restrict(store, restrict).await {
        Ok(outcome) => outcome,
        Err(err) => {
            tracing::error!(
                error = %err,
                "scrub: tenant discovery failed; skipping this cycle entirely, retried next cycle"
            );
            return;
        }
    };

    let clock = WallClock;
    for tenant in &outcome.maintained {
        // A rotation must not outlive the data it verifies: an object retention
        // deletes before the walk reaches it is never verified at all. Half the
        // tenant's own window (ADR-0019) caps the rotation length below the
        // configured scrub period (`ScrubCursor::plan_tick` halves it); `None`
        // means unlimited retention, which caps nothing.
        let retention_secs = retention.and_then(|cfg| cfg.window_for(tenant)).map(|ns| {
            let secs = ns / 1_000_000_000;
            u64::try_from(secs).unwrap_or(u64::MAX).max(1)
        });
        for signal in MAINTAINED_SIGNALS {
            // Resolve the covering name-postings object once per (tenant,
            // signal) per tick: postings cover a whole snapshot
            // HEAD (one `head.postings` bound to every part), not one per shard,
            // so it is loaded here, before the shard loop, and the owned data is
            // threaded by reference into every shard's tick. A load failure or
            // an absent postings ref yields `None`, which every shard's tick
            // treats as "no postings tier this tick" exactly as before this
            // wiring landed (the documented "no postings ref yet" case,
            // ADR-0059). That degrade is this caller's own: since #1964 the
            // callee returns `Err` on a non-`NotFound` failure rather than
            // `Ok(None)`, so the two cases arrive here distinguishably even
            // though both end as `None` for the tick. A `tenant_hash`-binding
            // breach (ADR-0050 §2) is logged and likewise degrades to `None`
            // for this tick, never wedging the
            // content and structural tiers that do not depend on postings.
            let scan_shards = scan_shards(store, tenant, signal, shard_count).await;

            // Ownership gate (ADR-0065 decision 2): scrub's per-shard rotation
            // double-pays across replicas today; gate each shard on ownership
            // under the current live set. If this process owns no shard of this
            // (tenant, signal), skip the covering-postings load and the
            // seal-divergence tier too rather than pay their reads for work it
            // will not do.
            // A degenerate `scan_shards == 0` (no valid shard_count/generation
            // configured for this tenant, which should not happen in a valid
            // deployment) also skips the seal-divergence tier below, since it
            // is checked for shard 0 ownership specifically after this loop
            // and this `continue` never lets that check run. Narrow and
            // harmless (there is nothing to verify with zero shards), but
            // worth naming: this early exit did not exist pre-ADR-0065.
            let owns_any =
                (0..scan_shards).any(|shard| worker.owns_unit(live_set, tenant, signal, shard));
            if !owns_any {
                continue;
            }

            let covering = match ravel_catalog::load_covering_postings(store, tenant, signal).await
            {
                Ok(loaded) => loaded,
                // error!, not warn!: this is the same class of fault as the
                // fold's `.cstat`/`.npost` reuse failures, which log at error,
                // and a permission fault here recurs every tick.
                Err(err) => {
                    tracing::error!(
                        tenant = %tenant.to_hex(), signal = ?signal, error = %err,
                        "scrub: covering-postings load failed; postings tier skipped this tick, retried"
                    );
                    None
                }
            };
            for shard in 0..scan_shards {
                if !worker.owns_unit(live_set, tenant, signal, shard) {
                    continue;
                }
                run_shard_tick(
                    store,
                    &clock,
                    tenant,
                    signal,
                    shard,
                    period_secs,
                    tick_secs,
                    retention_secs,
                    covering.as_ref(),
                    metrics,
                )
                .await;
            }
            // Seal-divergence tier (ADR-0059 decision 2): once per
            // (tenant, signal) per tick, not per shard and not gated behind the
            // content-tier cursor. It is metadata-cost, matching the structural
            // tier's cost class rather than the content tier's corpus-scan
            // budget, and `verify_seal_divergence` re-lists every shard itself.
            // Gated on ownership of shard 0 (ADR-0065 decision 2), like the
            // maintain idempotency-marker sweep: the single owner of shard 0
            // runs the whole-signal check so replicas do not double-pay it.
            if worker.owns_unit(live_set, tenant, signal, 0) {
                run_seal_divergence_tick(store, tenant, signal, metrics).await;
            }
        }
    }
}

/// One seal-divergence tick over one `(tenant, signal)` (ADR-0059 decision 2): re-list the sealed commit records and diff them against the
/// folded snapshot via [`ravel_catalog::verify_seal_divergence`] (the exact
/// comparison `ravel-cli catalog verify` runs), recording missing and
/// mismatched counts on the metrics.
///
/// `orphaned` divergences never increment anything: a snapshot entry with no
/// surviving sealed commit record is the expected shape once retention deletes a
/// folded record. An absent HEAD (nothing folded yet) is normal, not a fault. A
/// read or decode failure is logged and skipped for this tick, never counted as
/// an anomaly (a transient store error must not read as seal loss).
async fn run_seal_divergence_tick(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    metrics: &ScrubMetrics,
) {
    match ravel_catalog::verify_seal_divergence(store, tenant, signal).await {
        Ok(Some(report)) => {
            if !report.missing.is_empty() {
                tracing::error!(
                    tenant = %tenant.to_hex(), signal = ?signal,
                    missing = report.missing.len(),
                    "scrub: sealed commit records missing from the folded snapshot (seal divergence)"
                );
                metrics.record_seal_divergence_missing(signal, report.missing.len() as u64);
            }
            if !report.mismatched.is_empty() {
                tracing::error!(
                    tenant = %tenant.to_hex(), signal = ?signal,
                    mismatched = report.mismatched.len(),
                    "scrub: snapshot entries disagree with the sealed commit record content hash"
                );
                metrics.record_seal_divergence_mismatched(signal, report.mismatched.len() as u64);
            }
        }
        Ok(None) => {
            // No HEAD yet for this (tenant, signal): nothing folded, nothing to
            // verify. Normal, not a fault.
        }
        Err(err) => {
            tracing::warn!(
                tenant = %tenant.to_hex(), signal = ?signal, error = %err,
                "scrub: seal-divergence check failed to read or decode; skipped this tick, retried"
            );
        }
    }
}

/// The shard range to scrub for `(tenant, signal)`: the widest `shard_count`
/// across the tenant's generation history (ADR-0052 section 4), so a
/// reshard-increase's new shards are covered rather than silently skipped, the
/// same range [`crate::maintain::run_tick`] scans. Falls back to the process's
/// static `shard_count` when there is no generation record or the read fails
/// (best-effort verification, retried next tick).
async fn scan_shards(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard_count: u32,
) -> u32 {
    match ravel_catalog::read_generations_from_store(store, tenant, signal).await {
        Ok(Some(generations)) => generations
            .iter()
            .map(|g| g.shard_count)
            .max()
            .unwrap_or(shard_count),
        Ok(None) => shard_count,
        Err(err) => {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                error = %err,
                "scrub: shard-generation history read failed; scrubbing the static shard range \
                 this tick, retried next tick"
            );
            shard_count
        }
    }
}

/// One content-tier tick over one `(tenant, signal, shard)` (ADR-1686): load
/// the shard's persisted cursor, open a rotation with a LIST-only entry count
/// when it needs one or count the tail's appends when it does not, plan the
/// tick against the rotation's deadline, then walk the commit shard prefix
/// from the cursor's start-after marker, verifying each unit's objects as the
/// walk reaches them, until either cap of the plan's budget is filled. The
/// walk never builds the whole corpus: it lists and GETs only the entries this
/// tick consumes, plus at most one partial page. Every store error is logged
/// and the tick is retried next cycle; nothing here mutates durable data.
#[allow(clippy::too_many_arguments)]
async fn run_shard_tick(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    period_secs: u64,
    tick_secs: u64,
    retention_secs: Option<u64>,
    covering: Option<&ravel_catalog::LoadedCoveringPostings>,
    metrics: &ScrubMetrics,
) {
    let prefix = match keys::commit_shard_prefix(tenant, signal, shard) {
        Ok(prefix) => prefix,
        Err(err) => {
            tracing::warn!(
                tenant = %tenant.to_hex(), signal = ?signal, shard, error = %err,
                "scrub: could not build commit shard prefix; skipping shard this tick"
            );
            return;
        }
    };

    let Some(mut cursor) = load_cursor(store, tenant, signal, shard, clock.now_ns()).await else {
        return;
    };
    if cursor.needs_entry_count() {
        match count_entries_after(store, &prefix, None).await {
            Ok(tally) => cursor.start_rotation(&tally, clock.now_ns()),
            Err(err) => {
                tracing::warn!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, error = %err,
                    "scrub: LIST-only entry count failed; rotation not started, retried next tick"
                );
                return;
            }
        }
    } else {
        // Entries committed since the rotation opened are part of what this
        // rotation has to cover; counting them each tick is what keeps a shard
        // that keeps committing from growing its tail as fast as the walk
        // consumes it. A count failure only leaves the estimate where it was,
        // so the tick still runs on the entries already observed.
        match count_entries_after(store, &prefix, cursor.tail_count_start()).await {
            Ok(tally) => cursor.observe_tail(&tally),
            Err(err) => {
                tracing::warn!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, error = %err,
                    "scrub: LIST-only tail count failed; sizing this tick from the entries \
                     already observed"
                );
            }
        }
    }

    let plan = cursor.plan_tick(period_secs, tick_secs, retention_secs, clock.now_ns());
    if plan.behind {
        tracing::error!(
            tenant = %tenant.to_hex(), signal = ?signal, shard,
            needed_entries_per_tick = plan.needed_entries,
            budgeted_entries_per_tick = plan.budget.max_entries,
            rotation_window_secs = plan.rotation_secs,
            scrub_period_secs = period_secs,
            "scrub: rotation cannot finish inside its window at the catch-up ceiling; some \
             objects will expire unverified"
        );
        metrics.record_rotation_behind(signal);
    }

    // Build the borrowing `CoveringPostings` once for this signal's tick from
    // the owned data loaded per (tenant, signal) in `run_cycle`.
    // `CoveringPostings` is `Copy`, so it is passed by value into each object's
    // scrub. When no covering postings resolved (no postings ref yet, or a
    // degrade-to-None load), this stays `None` and `scrub_one_object` runs only
    // the structural and content tiers, exactly as before this wiring landed.
    let covering_postings = covering.map(|loaded| ravel_maintain::CoveringPostings {
        bytes: &loaded.bytes,
        part_blake3: &loaded.part_blake3,
        covered_entries: &loaded.covered_entries,
        max_postings_bytes: ravel_catalog::DEFAULT_MAX_POSTINGS_BYTES,
    });

    // Consume the listing in units until either cap of the budget is filled,
    // verifying each unit's objects before moving on so the verification GETs
    // are charged to this tick's request cap rather than running unbounded
    // after the walk. The marker stays behind a unit only when one of its GETs
    // failed with a retryable error: skipping it would leave its objects
    // unverified for a whole rotation, and retrying can clear the error. A
    // record or object that was not found or failed with an error retrying
    // cannot clear, and a record that failed to decode, move the marker on, so
    // one bad record cannot pin the rotation; those that were found but could
    // not be read are counted as unreadable.
    let mut listing = MarkerListing::new(store, &prefix, cursor.last_commit_key.clone());
    let mut slice_entries = 0u64;
    let mut requests = 0u64;
    let mut listing_failed = false;
    let mut unit_held = false;
    while !plan.budget.is_filled(slice_entries, requests) {
        let pages_before = listing.pages;
        let unit = match next_unit(store, &mut listing).await {
            Ok(Some(unit)) => unit,
            Ok(None) => break,
            Err(err) => {
                tracing::warn!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, error = %err,
                    "scrub: LIST of commit records failed; scrubbing the slice so far, resumed \
                     next tick"
                );
                listing_failed = true;
                break;
            }
        };
        requests = requests
            .saturating_add((listing.pages - pages_before) as u64)
            .saturating_add(unit.context_pages);
        let outcome = unit_targets(store, &unit).await;
        requests = requests.saturating_add(outcome.gets);
        if outcome.retry {
            // Leave the marker where it is: this unit is retried next tick.
            unit_held = true;
            break;
        }
        requests = requests.saturating_add(
            (outcome.targets.len() as u64).saturating_mul(SCRUB_REQUESTS_PER_OBJECT),
        );
        let Some(verdicts) = verify_slice(
            store,
            clock,
            tenant,
            signal,
            shard,
            &outcome.targets,
            covering_postings,
        )
        .await
        else {
            // An object read failed retryably: the whole unit, findings
            // included, is retried next tick, so nothing is counted twice.
            unit_held = true;
            break;
        };
        for record in &outcome.unreadable {
            tracing::error!(
                tenant = %tenant.to_hex(), signal = ?signal, shard, record_key = %record.key,
                level = record.level.as_str(), reason = record.reason.as_str(),
                error = %record.error,
                "scrub: record unreadable; the objects it names are not verified this rotation"
            );
            metrics.record_unreadable(signal, record.level, record.reason);
        }
        record_verdicts(tenant, signal, shard, &outcome.targets, verdicts, metrics);
        let bytes = outcome.targets.iter().fold(0u64, |sum, entry| {
            sum.saturating_add(entry.target.object_size)
        });
        let entries = unit.advance.len() as u64;
        if let Some(last) = unit.advance.into_iter().next_back() {
            cursor.consume(last, entries, bytes);
        }
        slice_entries = slice_entries.saturating_add(entries);
    }
    let rotation_complete = !listing_failed && !unit_held && listing.ended();

    // Cursor-position gauge (ADR-1686 decision 5): listing entries consumed
    // this rotation over the rotation's estimated entry count. A completed
    // rotation reads as full coverage before it rolls over.
    let total = cursor.estimated_rotation_entries();
    if rotation_complete {
        metrics.record_cursor_position(signal, total, total);
        cursor.complete_rotation(clock.now_ns());
    } else {
        metrics.record_cursor_position(signal, cursor.rotation_entries_visited, total);
    }

    persist_cursor(store, tenant, signal, shard, &cursor).await;
}

/// Verify one unit's objects. Split out of [`run_shard_tick`] so verification
/// runs inside the walk, where its requests are charged to the tick's budget.
///
/// Returns each object's result in slice order, or `None` as soon as one
/// object's GET fails with a retryable error: the caller then holds the marker
/// behind the unit and the next tick verifies all of it again.
#[allow(clippy::too_many_arguments)]
async fn verify_slice(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    slice: &[SliceEntry],
    covering_postings: Option<ravel_maintain::CoveringPostings<'_>>,
) -> Option<Vec<ScrubResult>> {
    let mut verdicts = Vec::with_capacity(slice.len());
    for entry in slice {
        // The structural + content tiers always run (footer crc re-verify, then
        // whole-object blake3 vs the recorded content hash). The postings tier
        // runs additionally when `covering_postings` is `Some`: the object's
        // true `__name__` set is re-derived and diffed against what the covering
        // postings object claims for it (the false-negative check). Postings
        // only ever cover L0 commit records (an L1/rewrite part's covering
        // ordinal is not meaningfully defined), so L1 and rewrite targets never
        // get the postings tier regardless of whether it loaded this tick.
        let postings_for_object = if entry.level == ScrubLevel::L0 {
            covering_postings
        } else {
            None
        };
        let verdict = scrub_one_object(store, clock, &entry.record, postings_for_object).await;
        if let ScrubResult::ReadError {
            detail,
            retryable: true,
        } = &verdict
        {
            tracing::warn!(
                tenant = %tenant.to_hex(), signal = ?signal, shard,
                object_key = %entry.target.object_key, detail = %detail,
                "scrub: retryable read error; unit held, retried next tick"
            );
            return None;
        }
        verdicts.push(verdict);
    }
    Some(verdicts)
}

/// Record the anomalies among one consumed unit's verification results.
fn record_verdicts(
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    slice: &[SliceEntry],
    verdicts: Vec<ScrubResult>,
    metrics: &ScrubMetrics,
) {
    for (entry, verdict) in slice.iter().zip(verdicts) {
        let key = &entry.target.object_key;
        let level = entry.level;
        match verdict {
            ScrubResult::Clean => {}
            ScrubResult::ChecksumMismatch { .. } => {
                tracing::error!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, object_key = %key,
                    level = level.as_str(),
                    "scrub: content-hash mismatch at rest (bit rot or partial write)"
                );
                metrics.record_checksum_mismatch(signal, level);
            }
            ScrubResult::StructuralCorruption { detail } => {
                tracing::error!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, object_key = %key,
                    level = level.as_str(), detail = %detail,
                    "scrub: structural corruption at rest (footer/section crc)"
                );
                metrics.record_checksum_mismatch(signal, level);
            }
            ScrubResult::Unreadable { detail, reason } => {
                tracing::error!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, object_key = %key,
                    level = level.as_str(), reason = reason.as_str(), detail = %detail,
                    "scrub: object unreadable (non-retryable store error); not verified this \
                     rotation"
                );
                metrics.record_unreadable(signal, level, reason);
            }
            ScrubResult::PostingsDisagreement { name, ordinal } => {
                tracing::error!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, object_key = %key,
                    name = %name, ordinal,
                    "scrub: postings disagreement (false negative)"
                );
                metrics.record_postings_disagreement(signal);
            }
            ScrubResult::ReadError { detail, .. } => {
                // A missing object or an input/decode inconsistency: a retry
                // would hit it again, and it is not bit rot either. The
                // retryable kind never reaches here; `verify_slice` holds the
                // unit on it.
                tracing::warn!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, object_key = %key,
                    detail = %detail,
                    "scrub: read error, not a finding; object skipped until the next rotation"
                );
            }
        }
    }
}

/// One object a tick's slice verifies: its key and size, the record
/// [`scrub_one_object`] checks it against, and the level a mismatch counts
/// under. The level lives here alone, so it has one owner.
struct SliceEntry {
    target: ScrubTarget,
    record: ravel_proto::commit::v1::CommitRecord,
    level: ScrubLevel,
}

/// The LIST-only pass that opens a rotation and the one that counts a
/// rotation's appends (ADR-1686 decision 3, amended): tally every entry under
/// the commit shard prefix strictly after `start_after`, with no GETs.
///
/// `start_after` of `None` counts the whole prefix, which is what opening a
/// rotation needs. A tail count starts at the cursor's tail window
/// ([`ScrubCursor::tail_count_start`]), so it lists the last two ingest hours
/// the previous count met plus anything after them, never the whole prefix.
async fn count_entries_after(
    store: &dyn ObjectStoreBackend,
    prefix: &str,
    start_after: Option<&str>,
) -> Result<TailTally, StoreError> {
    let mut tally = TailTally::default();
    drain_pages::<StoreError, _, _, _>(
        prefix,
        start_after,
        MAX_LIST_PAGES,
        |start, token| async move { store.list_after(prefix, start.as_deref(), token).await },
        |meta| {
            tally.observe(&meta.key);
            Ok(DrainStep::Continue)
        },
    )
    .await?;
    Ok(tally)
}

/// The commit shard prefix listed strictly after a start-after marker, one
/// page at a time and only when the walk needs another entry. Applies the same
/// listing rules as [`drain_pages`]: a repeated key is dropped, a key that
/// sorts below the last one delivered or a repeated continuation token is a
/// typed error, and [`MAX_LIST_PAGES`] bounds the walk.
struct MarkerListing<'a> {
    store: &'a dyn ObjectStoreBackend,
    prefix: &'a str,
    start_after: Option<String>,
    token: Option<PageToken>,
    buffered: VecDeque<String>,
    last_key: Option<String>,
    exhausted: bool,
    pages: usize,
}

impl<'a> MarkerListing<'a> {
    fn new(
        store: &'a dyn ObjectStoreBackend,
        prefix: &'a str,
        start_after: Option<String>,
    ) -> Self {
        MarkerListing {
            store,
            prefix,
            last_key: start_after.clone(),
            start_after,
            token: None,
            buffered: VecDeque::new(),
            exhausted: false,
            pages: 0,
        }
    }

    /// Every listed entry has been consumed and the store reported no further
    /// page: the listing, and with it the rotation, has ended.
    fn ended(&self) -> bool {
        self.exhausted && self.buffered.is_empty()
    }

    async fn fill(&mut self) -> Result<(), StoreError> {
        while self.buffered.is_empty() && !self.exhausted {
            if self.pages >= MAX_LIST_PAGES {
                return Err(StoreError::ListPageCeiling {
                    prefix: self.prefix.to_string(),
                    ceiling: MAX_LIST_PAGES,
                });
            }
            self.pages += 1;
            let page = self
                .store
                .list_after(self.prefix, self.start_after.as_deref(), self.token.clone())
                .await?;
            for meta in page.objects {
                match self.last_key.as_deref() {
                    Some(last) if meta.key.as_str() < last => {
                        return Err(StoreError::ListOrderViolation {
                            prefix: self.prefix.to_string(),
                            previous: last.to_string(),
                            offending: meta.key,
                        });
                    }
                    Some(last) if meta.key.as_str() == last => {}
                    _ => {
                        self.last_key = Some(meta.key.clone());
                        self.buffered.push_back(meta.key);
                    }
                }
            }
            match page.next {
                Some(next) if self.token.as_ref() == Some(&next) => {
                    return Err(StoreError::ListRepeatedToken {
                        prefix: self.prefix.to_string(),
                    });
                }
                Some(next) => self.token = Some(next),
                None => self.exhausted = true,
            }
        }
        Ok(())
    }

    async fn peek(&mut self) -> Result<Option<&str>, StoreError> {
        self.fill().await?;
        Ok(self.buffered.front().map(String::as_str))
    }

    async fn pop(&mut self) -> Result<Option<String>, StoreError> {
        self.fill().await?;
        Ok(self.buffered.pop_front())
    }
}

/// One unit of listing entries the walk consumes, and the lineage context that
/// unit's exclusions are resolved against.
struct Unit {
    /// Entries this unit consumes from the walk. The marker advances to the
    /// last of them, and only these entries' records have their parts
    /// expanded into scrub targets.
    advance: Vec<String>,
    /// Every compaction record, rewrite record, and tombstone under the same
    /// ingest hour, including ones an earlier tick already consumed. Lineage
    /// selection reads all of them; `advance` is a subset. For a commit-record
    /// unit this is empty (a commit record carries no lineage of its own).
    context: Vec<String>,
    /// Listing pages the context re-list cost, charged to the tick's request
    /// budget.
    context_pages: u64,
}

/// The next unit of listing entries the walk consumes (ADR-1686 decisions 2
/// and 6, amended). An L0 commit record, or an entry whose shape is not
/// recognized, is a unit on its own. The first compaction record, rewrite
/// record, or tombstone of an hour starts a unit holding every remaining entry
/// under that hour: those shapes all sort after every commit record under the
/// hour, and the lineage filter needs the hour's whole set of them at once,
/// since a tombstone, a superseding rewrite record, or an overlapping
/// compaction record anywhere in the hour changes which parts are live. So the
/// marker lands only on a commit record or on an hour's last entry, and a unit
/// never splits a lineage set across ticks.
///
/// Sorting after the marker is not enough on its own. A compaction record can
/// land in an hour the marker has already passed (a compactor publishing late,
/// or a second compactor racing the first), and that record then forms a unit
/// holding only itself, with its overlap rival left outside the unit. Overlap
/// selection run on that one record makes it authoritative by default and a
/// loser's parts get scrubbed, which the module docs say never happens. So
/// when the marker lies inside the hour's own lineage set, the hour is listed
/// again from its start and the whole set becomes the unit's `context`, while
/// only the entries past the marker are consumed.
async fn next_unit(
    store: &dyn ObjectStoreBackend,
    listing: &mut MarkerListing<'_>,
) -> Result<Option<Unit>, StoreError> {
    let Some(first) = listing.pop().await? else {
        return Ok(None);
    };
    if matches!(
        keys::partition_bucket_entry(&first),
        Ok(keys::BucketEntry::CommitRecord(_)) | Err(_)
    ) {
        return Ok(Some(Unit {
            advance: vec![first],
            context: Vec::new(),
            context_pages: 0,
        }));
    }
    let hour_dir = match first.rfind('/') {
        Some(slash) => first[..=slash].to_string(),
        None => {
            return Ok(Some(Unit {
                advance: vec![first],
                context: Vec::new(),
                context_pages: 0,
            }));
        }
    };
    let mut advance = vec![first];
    while let Some(next) = listing.peek().await? {
        if !next.starts_with(&hour_dir) {
            break;
        }
        if let Some(key) = listing.pop().await? {
            advance.push(key);
        }
    }

    // The marker sits inside this hour's lineage set only when a previous tick
    // consumed part of it: a marker on one of the hour's commit records, or on
    // any key outside the hour, leaves the whole set here in `advance`.
    let split = match listing.start_after.as_deref() {
        Some(marker) => {
            marker.starts_with(&hour_dir)
                && !matches!(
                    keys::partition_bucket_entry(marker),
                    Ok(keys::BucketEntry::CommitRecord(_))
                )
        }
        None => false,
    };
    if !split {
        let context = advance.clone();
        return Ok(Some(Unit {
            advance,
            context,
            context_pages: 0,
        }));
    }

    let mut context: Vec<String> = Vec::new();
    let mut pages = 0u64;
    let hour_prefix = hour_dir.as_str();
    drain_pages::<StoreError, _, _, _>(
        hour_prefix,
        None,
        MAX_LIST_PAGES,
        |_start, token| {
            pages += 1;
            async move { store.list(hour_prefix, token).await }
        },
        |meta| {
            if !matches!(
                keys::partition_bucket_entry(&meta.key),
                Ok(keys::BucketEntry::CommitRecord(_))
            ) {
                context.push(meta.key);
            }
            Ok(DrainStep::Continue)
        },
    )
    .await?;
    Ok(Some(Unit {
        advance,
        context,
        context_pages: pages,
    }))
}

/// What decoding one unit produced: the objects to verify, the GET attempts it
/// made, whether any of them failed retryably, and the records it could not
/// read at all.
struct UnitOutcome {
    targets: Vec<SliceEntry>,
    /// Record GET attempts, successful or not. Every attempt is charged to the
    /// tick's request budget, since a failing GET costs a request and moves no
    /// bytes.
    gets: u64,
    /// A record GET failed with a retryable error ([`StoreError::is_retryable`]:
    /// throttled, timeout, transient). The caller leaves the marker behind this
    /// unit and retries it next tick, rather than skipping objects it never
    /// verified.
    retry: bool,
    /// Records of this unit's `advance` set that could not be read: a GET that
    /// failed with an error retrying cannot clear (anything but `NotFound` and
    /// the retryable kinds), or bytes that do not decode. Each counts once on
    /// `ravel_scrub_unreadable_total` once the unit is consumed, and the marker
    /// moves past it, so one unreadable record cannot pin the rotation.
    unreadable: Vec<UnreadableRecord>,
}

/// One record a unit could not read, counted at the record's own level.
struct UnreadableRecord {
    key: String,
    level: ScrubLevel,
    reason: UnreadableReason,
    error: String,
}

/// How a failed GET affects the scrub (ADR-1686 amendment).
enum GetFailure {
    /// Retryable ([`StoreError::is_retryable`]): hold the marker and retry
    /// the unit next tick.
    Retry,
    /// `NotFound`: retention deleted the object after it was listed. Not a
    /// fault to retry and not counted.
    Gone,
    /// Any other error. Retrying cannot clear it, so the object is counted as
    /// unreadable and the marker moves on.
    Unreadable(UnreadableReason),
}

fn classify_get_failure(err: &StoreError) -> GetFailure {
    if err.is_retryable() {
        GetFailure::Retry
    } else if matches!(err, StoreError::NotFound) {
        GetFailure::Gone
    } else {
        GetFailure::Unreadable(UnreadableReason::of(err))
    }
}

/// Apply [`classify_get_failure`] to one failed record GET. An unreadable
/// record is kept only when it is in the unit's `advance` set: a context
/// record an earlier tick consumed was already counted then.
fn note_record_get_failure(
    key: &str,
    what: &str,
    err: &StoreError,
    level: ScrubLevel,
    advancing: bool,
    retry: &mut bool,
    unreadable: &mut Vec<UnreadableRecord>,
) {
    match classify_get_failure(err) {
        GetFailure::Retry => {
            *retry = true;
            tracing::warn!(
                key = %key, error = %err,
                "scrub: {what} GET failed with a retryable error; unit held, retried next tick"
            );
        }
        GetFailure::Gone => {
            tracing::warn!(
                key = %key,
                "scrub: {what} not found (deleted after it was listed); skipping it"
            );
        }
        GetFailure::Unreadable(reason) => {
            if advancing {
                unreadable.push(UnreadableRecord {
                    key: key.to_string(),
                    level,
                    reason,
                    error: err.to_string(),
                });
            }
        }
    }
}

/// A record whose bytes were read and do not decode. Counted like a
/// non-retryable GET failure, under `reason="permanent"`, when the record is
/// in the unit's `advance` set; a context record was counted when an earlier
/// tick consumed it, so it is only logged.
fn note_record_decode_failure(
    key: &str,
    what: &str,
    err: &dyn std::fmt::Display,
    level: ScrubLevel,
    advancing: bool,
    unreadable: &mut Vec<UnreadableRecord>,
) {
    if advancing {
        unreadable.push(UnreadableRecord {
            key: key.to_string(),
            level,
            reason: UnreadableReason::Permanent,
            error: format!("{what} decode failed: {err}"),
        });
    } else {
        tracing::warn!(
            key = %key, error = %err,
            "scrub: {what} decode failed; lineage selection runs without it this tick"
        );
    }
}

/// The objects one unit of listing entries names: the L0 data object behind
/// every commit record, and every part of a compaction or rewrite record that
/// is not superseded, not an overlap loser, and not in a tombstoned bucket.
/// The commit record (for an L0 object) or the `CompactionPart` (for a part)
/// carries the object's size and the content hash `scrub_one_object`
/// re-verifies against. A record whose decode fails names nothing; when it is
/// in the unit's `advance` set it is reported in [`UnitOutcome::unreadable`].
///
/// Exclusions are resolved over the unit's whole ingest-hour lineage set
/// ([`Unit::context`]), which can hold records an earlier tick already
/// consumed; parts are expanded only for the records in [`Unit::advance`], so
/// no object is verified twice in one rotation.
async fn unit_targets(store: &dyn ObjectStoreBackend, unit: &Unit) -> UnitOutcome {
    let mut gets = 0u64;
    let mut retry = false;
    let mut unreadable: Vec<UnreadableRecord> = Vec::new();
    let lineage: &[String] = if unit.context.is_empty() {
        &unit.advance
    } else {
        &unit.context
    };
    let advancing: std::collections::HashSet<&str> =
        unit.advance.iter().map(String::as_str).collect();
    // Retention's physical sweep deletes every object in a tombstoned bucket
    // (L0 commit records, L1 parts, rewrite parts) as one unit, but does not
    // do so atomically with the listing: a compaction/rewrite record can
    // still be listed for an hour whose tombstone has already landed.
    // Collect tombstoned hours first (filename-only classification, no GETs)
    // so the second pass can skip their compaction/rewrite records rather
    // than racing a sweep that may delete their parts mid-tick.
    let mut tombstoned_hours: std::collections::HashSet<u32> = std::collections::HashSet::new();
    for key in lineage {
        if let Ok(keys::BucketEntry::Tombstone(parsed)) = keys::partition_bucket_entry(key) {
            tombstoned_hours.insert(parsed.ingest_hour_bucket);
        }
    }

    let mut out: Vec<SliceEntry> = Vec::new();
    // Compaction and rewrite records are decoded in the pass below but their
    // parts are not expanded there: a bucket can hold a superseded generation
    // alongside the live one until a horizon-gated sweep retires it, and can
    // hold two compaction records whose input sets overlap, and only the
    // hour's full set says which record of each pair the read path serves.
    // Buffer them, resolve both exclusions once, then expand. Each record is
    // still fetched exactly once.
    let mut compaction_records: Vec<(String, ravel_proto::commit::v1::CompactionRecord)> =
        Vec::new();
    let mut rewrite_records: Vec<(String, ravel_proto::commit::v1::RewriteRecord)> = Vec::new();
    for key in lineage {
        // L0 commit records, and the L1/rewrite parts a compaction or
        // erasure-rewrite record supersedes them with, all name objects
        // `scrub_one_object` can verify (its API is commit-record based; L1
        // and rewrite parts are wrapped in a synthetic record built from
        // their own part fields). Tombstone records carry no object of their
        // own. Listed explicitly so a new bucket-entry shape fails to
        // compile here rather than being silently swallowed.
        match keys::partition_bucket_entry(key) {
            Ok(keys::BucketEntry::CommitRecord(_)) => {
                gets += 1;
                let got = match store.get(key, GetRange::Full).await {
                    Ok(got) => got,
                    Err(err) => {
                        note_record_get_failure(
                            key,
                            "commit record",
                            &err,
                            ScrubLevel::L0,
                            advancing.contains(key.as_str()),
                            &mut retry,
                            &mut unreadable,
                        );
                        continue;
                    }
                };
                let record = match ravel_commit::record::decode(&got.data) {
                    Ok(record) => record,
                    Err(err) => {
                        note_record_decode_failure(
                            key,
                            "commit record",
                            &err,
                            ScrubLevel::L0,
                            advancing.contains(key.as_str()),
                            &mut unreadable,
                        );
                        continue;
                    }
                };
                let data_key = match keys::reconstruct_data_key(&record) {
                    Ok(key) => key,
                    Err(err) => {
                        tracing::warn!(
                            key = %key, error = %err,
                            "scrub: could not reconstruct data key; skipping this object this tick"
                        );
                        continue;
                    }
                };
                out.push(SliceEntry {
                    target: ScrubTarget {
                        object_key: data_key,
                        object_size: record.object_size,
                    },
                    record,
                    level: ScrubLevel::L0,
                });
            }
            Ok(keys::BucketEntry::CompactionRecord(parsed)) => {
                if tombstoned_hours.contains(&parsed.ingest_hour_bucket) {
                    continue;
                }
                gets += 1;
                let got = match store.get(key, GetRange::Full).await {
                    Ok(got) => got,
                    Err(err) => {
                        note_record_get_failure(
                            key,
                            "compaction record",
                            &err,
                            ScrubLevel::L1,
                            advancing.contains(key.as_str()),
                            &mut retry,
                            &mut unreadable,
                        );
                        continue;
                    }
                };
                let rec = match ravel_commit::record::decode_compaction(&got.data) {
                    Ok(rec) => rec,
                    Err(err) => {
                        note_record_decode_failure(
                            key,
                            "compaction record",
                            &err,
                            ScrubLevel::L1,
                            advancing.contains(key.as_str()),
                            &mut unreadable,
                        );
                        continue;
                    }
                };
                compaction_records.push((key.clone(), rec));
            }
            Ok(keys::BucketEntry::RewriteRecord(parsed)) => {
                if tombstoned_hours.contains(&parsed.ingest_hour_bucket) {
                    continue;
                }
                gets += 1;
                let got = match store.get(key, GetRange::Full).await {
                    Ok(got) => got,
                    Err(err) => {
                        note_record_get_failure(
                            key,
                            "rewrite record",
                            &err,
                            ScrubLevel::Rewrite,
                            advancing.contains(key.as_str()),
                            &mut retry,
                            &mut unreadable,
                        );
                        continue;
                    }
                };
                let rec = match ravel_commit::erasure::decode_rewrite(&got.data) {
                    Ok(rec) => rec,
                    Err(err) => {
                        note_record_decode_failure(
                            key,
                            "rewrite record",
                            &err,
                            ScrubLevel::Rewrite,
                            advancing.contains(key.as_str()),
                            &mut unreadable,
                        );
                        continue;
                    }
                };
                rewrite_records.push((key.clone(), rec));
            }
            Ok(keys::BucketEntry::Tombstone(_)) => continue,
            Err(err) => {
                tracing::warn!(
                    key = %key, error = %err,
                    "scrub: unrecognized bucket entry shape; skipping"
                );
                continue;
            }
        }
    }

    // Drop superseded generations before expanding their parts. A rewrite
    // record names the compaction or rewrite record it replaced in
    // `superseded_record_key`, and each generation fully supersedes the one
    // before it, so one hop resolves the live set (the same rule
    // `ravel_maintain::erasure_rewrite` resolves a rewrite against). The
    // superseded generation's parts survive until a horizon-gated sweep, and
    // no query reads them: scrubbing them would page an operator on rot in
    // bytes nothing depends on, and would spend tick budget that belongs to
    // live data. Unlike the rewrite pass, an unresolvable shape here is not
    // fatal; the scrub keeps whatever survives the filter and the next
    // rotation tries again.
    let superseded: std::collections::HashSet<&str> = rewrite_records
        .iter()
        .filter(|(_, rec)| !rec.superseded_record_key.is_empty())
        .map(|(_, rec)| rec.superseded_record_key.as_str())
        .collect();

    // Supersession and the tombstone check in the listing pass are two of the
    // three ways a compaction record's parts stop being served. The third is
    // overlap (issue #1070): two compactors
    // racing leave two records in one bucket whose input sets share an L0
    // input, and the catalog keeps one authoritative record per overlap
    // component and ignores every other record's parts. Resolve it through
    // the same helper snapshot resolution, the index fold, the sweep, migrate
    // and the erasure completion gate use, so the corpus and the read path
    // derive identical bucket state. Per bucket, because an overlap component
    // is a property of one ingest-hour bucket: that is the unit the resolver
    // reads. A loser is not horizon-bounded the way a superseded generation
    // is -- the losing record keeps its parts referenced for as long as it
    // exists, so the sweep reclaims none of them -- and rot in one would
    // otherwise page an operator on every rotation, indefinitely, for bytes
    // no query on a fleet that has adopted the overlap rule reads.
    let losing_compaction_records: std::collections::HashSet<String> = {
        let mut by_bucket: std::collections::HashMap<
            u32,
            Vec<(&str, &ravel_proto::commit::v1::CompactionRecord)>,
        > = std::collections::HashMap::new();
        for (record_key, rec) in &compaction_records {
            by_bucket
                .entry(rec.ingest_hour_bucket)
                .or_default()
                .push((record_key.as_str(), rec));
        }
        let mut losing: std::collections::HashSet<String> = std::collections::HashSet::new();
        for in_bucket in by_bucket.values() {
            for record_key in ravel_catalog::select_authoritative_compaction_records(in_bucket) {
                losing.insert(record_key.to_string());
            }
        }
        losing
    };

    for (record_key, rec) in &compaction_records {
        if !advancing.contains(record_key.as_str())
            || superseded.contains(record_key.as_str())
            || losing_compaction_records.contains(record_key.as_str())
        {
            continue;
        }
        for part in &rec.parts {
            let part_key = match keys::reconstruct_l1_part_key(rec, part) {
                Ok(key) => key,
                Err(err) => {
                    tracing::warn!(
                        key = %record_key, error = %err,
                        "scrub: could not reconstruct L1 part key; skipping this part this tick"
                    );
                    continue;
                }
            };
            let synthetic = ravel_proto::commit::v1::CommitRecord {
                signal: rec.signal,
                object_key: part_key.clone(),
                object_size: part.object_size,
                content_hash: part.content_hash.clone(),
                ..Default::default()
            };
            out.push(SliceEntry {
                target: ScrubTarget {
                    object_key: part_key,
                    object_size: part.object_size,
                },
                record: synthetic,
                level: ScrubLevel::L1,
            });
        }
    }

    for (record_key, rec) in &rewrite_records {
        if !advancing.contains(record_key.as_str()) || superseded.contains(record_key.as_str()) {
            continue;
        }
        for part in &rec.parts {
            let part_key = match keys::reconstruct_rewrite_part_key(rec, part) {
                Ok(key) => key,
                Err(err) => {
                    tracing::warn!(
                        key = %record_key, error = %err,
                        "scrub: could not reconstruct rewrite part key; skipping this part this tick"
                    );
                    continue;
                }
            };
            let synthetic = ravel_proto::commit::v1::CommitRecord {
                signal: rec.signal,
                object_key: part_key.clone(),
                object_size: part.object_size,
                content_hash: part.content_hash.clone(),
                ..Default::default()
            };
            out.push(SliceEntry {
                target: ScrubTarget {
                    object_key: part_key,
                    object_size: part.object_size,
                },
                record: synthetic,
                level: ScrubLevel::Rewrite,
            });
        }
    }

    UnitOutcome {
        targets: out,
        gets,
        retry,
        unreadable,
    }
}

/// The persisted per-shard cursor's object key. Nested under the existing
/// `maint/` control prefix (`t/<hash>/<sig>/maint/scrub/<shard>.cursor`) so it
/// falls under the Maintain role's existing `t/*/*/maint/*` IAM grant
/// (ADR-0055) with no new prefix, alongside the compactor's own advisory scan
/// cursor. Not a true single writer under ADR-0065's N-replica Maintain role
/// (see the module docs); a racing overwrite is benign.
fn cursor_key(tenant: &TenantHash, signal: Signal, shard: u32) -> String {
    format!(
        "t/{}/{}/maint/scrub/{:04}.cursor",
        tenant.to_hex(),
        signal.key_prefix(),
        shard,
    )
}

/// The on-disk cursor: only the rotation-relative state needs persisting;
/// tenant/signal/shard are known from the key context on load. Every field
/// ADR-1686 added carries a serde default, so a cursor an older build wrote
/// (holding `last_object_key`, which is ignored) loads as the start of a fresh
/// rotation.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedCursor {
    #[serde(default)]
    last_commit_key: Option<String>,
    rotation_started_unix_ns: i64,
    #[serde(default)]
    rotation_bytes_seen: u64,
    #[serde(default)]
    last_rotation_bytes: Option<u64>,
    #[serde(default)]
    rotation_total_entries: Option<u64>,
    #[serde(default)]
    rotation_entries_visited: u64,
    #[serde(default)]
    rotation_appended_entries: u64,
    #[serde(default)]
    rotation_tail_dir: Option<String>,
    #[serde(default)]
    rotation_tail_entries: u64,
}

/// Load this shard's persisted cursor, or a fresh one at the start of a
/// rotation when none exists yet or it fails to decode (a decode failure is
/// treated as "start over," never an anomaly: the cursor is advisory scheduling
/// state, not durable data).
///
/// A GET that fails for any reason other than `NotFound` returns `None` and
/// the caller skips the tick. Starting a fresh rotation there would rewind the
/// marker to the head of the listing and drop the rotation's progress on a
/// transient throttle, so a store having a bad minute would keep restarting
/// the rotation instead of finishing one. `NotFound` is the only answer that
/// really means there is no cursor.
async fn load_cursor(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    now_ns: i64,
) -> Option<ScrubCursor> {
    let key = cursor_key(tenant, signal, shard);
    match store.get(&key, GetRange::Full).await {
        Ok(got) => match serde_json::from_slice::<PersistedCursor>(&got.data) {
            Ok(persisted) => Some(ScrubCursor {
                tenant_hash: *tenant,
                signal,
                shard,
                last_commit_key: persisted.last_commit_key,
                rotation_started_unix_ns: persisted.rotation_started_unix_ns,
                rotation_bytes_seen: persisted.rotation_bytes_seen,
                last_rotation_bytes: persisted.last_rotation_bytes,
                rotation_total_entries: persisted.rotation_total_entries,
                rotation_entries_visited: persisted.rotation_entries_visited,
                rotation_appended_entries: persisted.rotation_appended_entries,
                rotation_tail_dir: persisted.rotation_tail_dir,
                rotation_tail_entries: persisted.rotation_tail_entries,
            }),
            Err(err) => {
                tracing::warn!(
                    key = %key, error = %err,
                    "scrub: cursor decode failed; starting a fresh rotation for this shard"
                );
                Some(ScrubCursor::new(*tenant, signal, shard, now_ns))
            }
        },
        Err(StoreError::NotFound) => Some(ScrubCursor::new(*tenant, signal, shard, now_ns)),
        Err(err) => {
            tracing::warn!(
                key = %key, error = %err,
                "scrub: cursor GET failed; skipping this shard's tick and keeping the stored \
                 cursor, retried next tick"
            );
            None
        }
    }
}

/// Persist the advanced cursor (overwrite; a racing overwrite from another
/// owner is benign, see the module docs). A write failure is logged and left
/// for the next tick to retry: the worst case is one shard re-scrubbing the
/// same slice, never a correctness problem.
async fn persist_cursor(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    cursor: &ScrubCursor,
) {
    let key = cursor_key(tenant, signal, shard);
    let persisted = PersistedCursor {
        last_commit_key: cursor.last_commit_key.clone(),
        rotation_started_unix_ns: cursor.rotation_started_unix_ns,
        rotation_bytes_seen: cursor.rotation_bytes_seen,
        last_rotation_bytes: cursor.last_rotation_bytes,
        rotation_total_entries: cursor.rotation_total_entries,
        rotation_entries_visited: cursor.rotation_entries_visited,
        rotation_appended_entries: cursor.rotation_appended_entries,
        rotation_tail_dir: cursor.rotation_tail_dir.clone(),
        rotation_tail_entries: cursor.rotation_tail_entries,
    };
    let bytes = match serde_json::to_vec(&persisted) {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::warn!(key = %key, error = %err, "scrub: cursor encode failed; not persisted");
            return;
        }
    };
    if let Err(err) = store
        .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
    {
        tracing::warn!(
            key = %key, error = %err,
            "scrub: cursor persist failed; next tick re-scrubs this slice, retried"
        );
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use bytes::Bytes;
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
    use ravel_proto::commit::v1::CommitRecord;
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};
    use uuid::Uuid;

    use super::*;

    /// A single-replica worker for the scrub tests: its solo live set
    /// (`{self}`) owns every unit, so `run_cycle` gates nothing away and
    /// behaves exactly as the pre-ADR-0065 unconditional per-shard rotation.
    fn solo_worker() -> WorkerSet {
        WorkerSet::with_defaults(0)
    }

    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    fn tenant() -> TenantId {
        TenantId::new("scrub-server-test")
    }

    /// Publish a real RSEG segment plus its commit record into `(tenant,
    /// Metrics, shard 0)`, exactly as ingest would, so an unmodified object
    /// scrubs clean. Returns the data-object key.
    async fn publish_segment(store: &MemoryStore, seq: u64, metrics: &[&str]) -> String {
        publish_segment_at(store, seq, metrics, 500_000).await
    }

    /// [`publish_segment`] into ingest-hour bucket `hour`.
    async fn publish_segment_at(
        store: &MemoryStore,
        seq: u64,
        metrics: &[&str],
        hour: u32,
    ) -> String {
        let tenant_id = tenant();
        let tenant_hash = tenant_id.hash();
        let writer_id = Uuid::from_u128(u128::from(1_000 + seq));
        let created_unix_ns = i64::from(hour) * NS_PER_HOUR;
        let ingest_hour_bucket = hour;
        let series: Vec<SeriesInput> = metrics
            .iter()
            .map(|metric| {
                let labels = LabelSet::new(vec![Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: (*metric).to_string(),
                }])
                .expect("valid labels");
                let series_id = SeriesId::compute(&tenant_id, metric, &labels).expect("series id");
                SeriesInput {
                    series_id,
                    labels,
                    samples: vec![Sample {
                        ts_ns: created_unix_ns,
                        value: 1.0,
                    }],
                }
            })
            .collect();
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: seq,
        };
        let min_ingest_ts_ns = created_unix_ns - 1_000;
        let max_ingest_ts_ns = created_unix_ns;
        let bounds = IngestBounds {
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        };
        let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
        let record: CommitRecord = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            segment_format_version: 1,
            created_unix_ns,
            ingest_hour_bucket,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        store
            .put(&data_key, written.bytes, PutOptions::default())
            .await
            .expect("put data object");
        // Publish the commit record so the scrubber's LIST discovers it.
        ravel_commit::publish::publish(
            store,
            &record,
            &ravel_commit::publish::RetryPolicy::default(),
        )
        .await
        .expect("publish commit record");
        data_key
    }

    /// A full tick over a clean corpus: discovery -> load cursor -> count ->
    /// walk from the marker -> scrub -> persist cursor. No anomaly is
    /// recorded, and the per-shard cursor is persisted so a later tick resumes
    /// from it.
    #[tokio::test]
    async fn clean_shard_tick_scrubs_and_persists_cursor() {
        let store = MemoryStore::new();
        let tenant_hash = tenant().hash();
        publish_segment(&store, 1, &["cpu", "mem"]).await;
        publish_segment(&store, 2, &["disk"]).await;

        let metrics = ScrubMetrics::default();
        let worker = solo_worker();
        // period == tick so the whole corpus is one slice: a full rotation in
        // one tick.
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;

        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0
        );
        assert_eq!(metrics.postings_disagreement(Signal::Metrics), 0);
        // A completed rotation reads as full coverage.
        assert_eq!(metrics.cursor_position(Signal::Metrics), 1.0);

        // The cursor was persisted for the scrubbed shard.
        let key = cursor_key(&tenant_hash, Signal::Metrics, 0);
        let got = store
            .get(&key, GetRange::Full)
            .await
            .expect("cursor persisted");
        let cursor: PersistedCursor = serde_json::from_slice(&got.data).expect("cursor decodes");
        // A completed rotation wraps to the start.
        assert_eq!(cursor.last_commit_key, None);
    }

    /// A single-bit flip in a committed data object surfaces as a checksum
    /// mismatch through the real tick path (discovery included), the anomaly
    /// ADR-0059's acceptance criterion is about. Corruption is injected with
    /// the proven GET/flip/Overwrite pattern
    /// (crates/ravel-failure-tests/tests/corruption.rs).
    #[tokio::test]
    async fn corrupted_object_surfaces_as_checksum_mismatch() {
        let store = MemoryStore::new();
        let data_key = publish_segment(&store, 1, &["cpu", "mem"]).await;

        // Flip a byte in the object's page region (not the footer), so the
        // content tier's blake3 is what catches it.
        let existing = store
            .get(&data_key, GetRange::Full)
            .await
            .expect("get object");
        let mut corrupted = existing.data.to_vec();
        corrupted[0] ^= 0x01;
        store
            .put(&data_key, Bytes::from(corrupted), PutOptions::default())
            .await
            .expect("overwrite corrupted object");

        let metrics = ScrubMetrics::default();
        let worker = solo_worker();
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;

        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            1,
            "the injected bit flip must surface as a scrub checksum mismatch"
        );
    }

    /// The cursor advances across ticks under a budget that admits one entry
    /// per tick: two ticks cover a two-record corpus, and the mid-
    /// rotation cursor-position gauge reads a partial fraction.
    #[tokio::test]
    async fn cursor_advances_across_ticks_under_a_tight_budget() {
        let store = MemoryStore::new();
        publish_segment(&store, 1, &["cpu"]).await;
        publish_segment(&store, 2, &["mem"]).await;

        let metrics = ScrubMetrics::default();
        let worker = solo_worker();
        // period (100s) >> tick (1s): the per-tick budget covers only a
        // fraction of the corpus, so a rotation takes multiple ticks.
        run_cycle(
            &store,
            None,
            1,
            100,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        let after_first = metrics.cursor_position(Signal::Metrics);
        assert!(
            after_first > 0.0 && after_first < 1.0,
            "a mid-rotation tick covers part of the corpus, got {after_first}"
        );

        // Keep ticking until the rotation completes and wraps.
        for _ in 0..10 {
            run_cycle(
                &store,
                None,
                1,
                100,
                1,
                &metrics,
                &worker,
                &worker.solo_live_set(),
                None,
            )
            .await;
            let tenant_hash = tenant().hash();
            let key = cursor_key(&tenant_hash, Signal::Metrics, 0);
            let got = store.get(&key, GetRange::Full).await.expect("cursor");
            let cursor: PersistedCursor = serde_json::from_slice(&got.data).expect("decode");
            if cursor.last_commit_key.is_none() {
                // Rotation wrapped: coverage was complete at some tick.
                return;
            }
        }
        panic!("rotation never completed across repeated ticks");
    }

    /// An empty store scrubs cleanly: discovery finds no tenant, so no anomaly
    /// and no cursor is written.
    #[tokio::test]
    async fn empty_store_tick_is_clean() {
        let store = MemoryStore::new();
        let metrics = ScrubMetrics::default();
        let worker = solo_worker();
        run_cycle(
            &store,
            None,
            4,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0
        );
        assert_eq!(metrics.postings_disagreement(Signal::Metrics), 0);
    }

    /// HEAD object key for `(tenant, Metrics)` (docs/catalog-and-mvcc.md key
    /// layout), duplicated for the tests the same way the production modules do.
    fn head_key_for(tenant_hash: &TenantHash) -> String {
        format!(
            "t/{}/catalog/{}/HEAD",
            tenant_hash.to_hex(),
            Signal::Metrics.key_prefix(),
        )
    }

    /// Fold `(tenant, Metrics)` at `now_ns` so a real HEAD with a real,
    /// correctly-bound name-postings object exists to load. `now_ns` must be
    /// past the seal margins for the published segment's hour.
    async fn fold(store: Arc<MemoryStore>, now_ns: i64) {
        let dyn_store: Arc<dyn ObjectStoreBackend> = store;
        let catalog = ravel_catalog::Catalog::new(
            dyn_store,
            ravel_catalog::CatalogConfig {
                shard_count: 1,
                ..ravel_catalog::CatalogConfig::default()
            },
        )
        .expect("catalog")
        .with_provisioning_enforcement();
        catalog
            .fold(
                &tenant().hash(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_ns,
                &[],
                None,
            )
            .await
            .expect("fold produces a HEAD with postings");
    }

    /// A clean folded snapshot: `run_shard_tick` loads the real covering
    /// postings and passes `Some(covering)` to `scrub_one_object`, but the
    /// postings accurately describe the segment, so no disagreement is recorded.
    /// This proves the `Some` path is wired without false positives.
    #[tokio::test]
    async fn folded_clean_postings_record_no_disagreement() {
        let store = Arc::new(MemoryStore::new());
        // publish_segment fixes the segment's hour at 500_000; fold three hours
        // later so the record is sealed and folded (past the ~20min margins).
        publish_segment(store.as_ref(), 1, &["cpu", "mem"]).await;
        let now = 500_003 * NS_PER_HOUR;
        fold(store.clone(), now).await;

        let metrics = ScrubMetrics::default();
        let worker = solo_worker();
        run_cycle(
            store.as_ref(),
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;

        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0
        );
        assert_eq!(
            metrics.postings_disagreement(Signal::Metrics),
            0,
            "accurate postings must not record a disagreement"
        );
    }

    /// A real postings disagreement, injected at rest: the covering postings
    /// object is replaced with a valid, correctly-bound object whose claims omit
    /// a name the segment really carries, and the HEAD's postings ref is pointed
    /// at it (matching blake3) so the load accepts it. `run_shard_tick` must
    /// then pass `Some(covering)` to `scrub_one_object`, which re-derives the
    /// segment's true name set and records the false negative. This is the
    /// direct `MemoryStore`-backed proof that the `Some(covering)` path fires.
    #[tokio::test]
    async fn postings_disagreement_surfaces_through_run_shard_tick() {
        let store = Arc::new(MemoryStore::new());
        publish_segment(store.as_ref(), 1, &["cpu", "mem"]).await;
        let now = 500_003 * NS_PER_HOUR;
        fold(store.clone(), now).await;

        // Read the folded HEAD and its covered part(s)' blake3.
        let head_key = head_key_for(&tenant().hash());
        let head_bytes = store
            .get(&head_key, GetRange::Full)
            .await
            .expect("head present")
            .data;
        let mut head = ravel_catalog::decode_head(&head_bytes).expect("decode head");
        let part_blake3: Vec<[u8; 32]> = head
            .parts
            .iter()
            .map(|p| <[u8; 32]>::try_from(p.blake3.as_slice()).expect("32-byte part blake3"))
            .collect();

        // Encode a NEW, internally valid postings object that claims only "cpu"
        // for ordinal 0, omitting "mem" (which the segment really carries): a
        // genuine false negative, not a bit flip. A raw bit flip would surface
        // as StructuralCorruption; this must surface as PostingsDisagreement.
        let names = vec![ravel_catalog::NamePostings {
            name: "cpu".to_string(),
            ordinals: vec![0],
        }];
        let tampered = ravel_catalog::encode_postings(
            tenant().hash().0,
            Signal::Metrics as u32,
            &part_blake3,
            1,
            &names,
        )
        .expect("encode tampered postings");
        let tampered_hash = *blake3::hash(&tampered).as_bytes();

        // Overwrite the postings object over its real key, then repoint the
        // HEAD's postings ref at it with the matching blake3/size so
        // load_covering_postings accepts it instead of degrading to None.
        let postings_ref = head.postings.as_mut().expect("postings ref after fold");
        store
            .put(
                &postings_ref.key,
                Bytes::from(tampered.clone()),
                PutOptions::default(),
            )
            .await
            .expect("overwrite postings object");
        postings_ref.blake3 = tampered_hash.to_vec();
        postings_ref.size = tampered.len() as u64;
        let rewritten = ravel_catalog::encode_head(&head).expect("re-encode head");
        store
            .put(&head_key, Bytes::from(rewritten), PutOptions::default())
            .await
            .expect("overwrite head");

        let metrics = ScrubMetrics::default();
        let worker = solo_worker();
        run_cycle(
            store.as_ref(),
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;

        assert_eq!(
            metrics.postings_disagreement(Signal::Metrics),
            1,
            "the omitted name must surface as a postings disagreement through the real tick"
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0,
            "the segment data is untouched: no checksum mismatch"
        );
    }

    /// Compaction parts join the scrub corpus tagged by their own level
    /// (issue #1686): a bit flip in an L1 part must count under
    /// `level="l1"`, not `level="l0"`, and must leave `level="l0"` at zero.
    /// Drives the real production compactor (`ravel_maintain::compact_bucket`)
    /// over two real L0 segments so the resulting L1 part carries a
    /// byte-correct `content_hash`; a fabricated hash would make the
    /// pre-corruption assertion below fail for the wrong reason.
    #[tokio::test]
    async fn corrupted_l1_part_is_counted_under_its_level_after_compaction() {
        let store = MemoryStore::new();
        let tenant_hash = tenant().hash();
        publish_segment(&store, 1, &["cpu"]).await;
        publish_segment(&store, 2, &["mem"]).await;

        let bucket = ravel_maintain::Bucket::new(tenant_hash, Signal::Metrics, 0, 500_000);
        let compact_clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let outcome = ravel_maintain::compact_bucket(
            &store,
            &compact_clock,
            &ravel_maintain::CompactorConfig::default(),
            &bucket,
        )
        .await
        .expect("compact");
        assert!(
            matches!(outcome, ravel_maintain::CompactionOutcome::Compacted { .. }),
            "two sealed L0 inputs must compact, got {outcome:?}"
        );

        // Find the published compaction record and its (single) L1 part key.
        let prefix = keys::commit_shard_hour_prefix(&tenant_hash, Signal::Metrics, 0, 500_000)
            .expect("prefix");
        let metas = list_all(&store, &prefix).await.expect("list bucket");
        let record_key = metas
            .iter()
            .map(|m| m.key.clone())
            .find(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .expect("a compaction record was published");
        let record_bytes = store
            .get(&record_key, GetRange::Full)
            .await
            .expect("get compaction record")
            .data;
        let record = ravel_commit::record::decode_compaction(&record_bytes)
            .expect("decode compaction record");
        assert_eq!(
            record.parts.len(),
            1,
            "two small inputs fit in a single L1 part"
        );
        let part_key =
            keys::reconstruct_l1_part_key(&record, &record.parts[0]).expect("l1 part key");

        let metrics = ScrubMetrics::default();
        let worker = solo_worker();

        // Pre-corruption: a full tick over the real (L0 + L1) corpus is clean
        // at both levels.
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0
        );

        // Flip a byte in the L1 part's page region, the same proven
        // GET/flip/Overwrite pattern `corrupted_object_surfaces_as_checksum_mismatch`
        // uses for an L0 object.
        corrupt_first_byte(&store, &part_key).await;

        let metrics = ScrubMetrics::default();
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            1,
            "the injected L1 bit flip must surface under level=l1"
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0,
            "an L1 corruption must not be counted under level=l0"
        );
    }

    /// A rewrite output part joins the scrub corpus tagged `level="rewrite"`
    /// (issue #1686): a bit flip in it must count there, not under
    /// `level="l0"` or `level="l1"`. Manually publishes a `RewriteRecord` plus
    /// a real stored part object at its reconstructed key, mirroring
    /// `ravel_maintain::compact`'s own `rewrite_part`/`put_rewrite_record`
    /// test helpers, but with the part's `content_hash` computed as the real
    /// blake3 of the stored bytes (those helpers use an arbitrary
    /// `vec![tag; 32]`, which would fail the pre-corruption "reads 0 on the
    /// unmodified corpus" assertion below since the content tier's rehash
    /// would never match a fabricated hash).
    #[tokio::test]
    async fn corrupted_rewrite_part_is_counted_under_its_level() {
        use ravel_commit::erasure;
        use ravel_proto::commit::v1::{
            CompactionInputIdentity, CompactionPart, RewriteDrop, RewriteRecord,
        };

        let store = MemoryStore::new();
        let tenant_id = tenant();
        let tenant_hash = tenant_id.hash();
        let shard = 0u32;
        let ingest_hour_bucket = 500_000u32;
        let created_unix_ns = 500_000 * NS_PER_HOUR;

        // A real RSEG segment, not fabricated bytes: the scrub content tier
        // rehashes the *whole object*, but the structural tier runs first and
        // rejects anything that is not a well-formed RSEG segment, so the
        // pre-corruption "reads 0" assertion below needs a real segment, the
        // same way the L1 test above needs a byte-correct `content_hash`.
        let labels = LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "cpu".to_string(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant_id, "cpu", &labels).expect("series id");
        let series = vec![SeriesInput {
            series_id,
            labels,
            samples: vec![Sample {
                ts_ns: created_unix_ns,
                value: 1.0,
            }],
        }];
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: Uuid::from_u128(2_000).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: created_unix_ns - 1_000,
            max_ingest_ts_ns: created_unix_ns,
        };
        let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
        let part_bytes = written.bytes;
        let part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xffu8; 16],
            content_hash: written.summary.blake3.to_vec(),
            object_size: part_bytes.len() as u64,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            run_count: 1,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            declared_column_stats: Vec::new(),
        };
        let inputs = vec![CompactionInputIdentity {
            writer_id: Uuid::from_u128(1).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        }];
        let request_id = Uuid::from_u128(0xEA5E);
        let request_ids = vec![request_id.to_string()];
        let input_set_hash = erasure::compute_rewrite_input_set_hash(&inputs, None, &request_ids);
        let record = RewriteRecord {
            format_version: 1,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            shard,
            ingest_hour_bucket,
            inputs,
            input_set_hash: input_set_hash.to_vec(),
            parts: vec![part.clone()],
            drops: vec![RewriteDrop {
                request_id: request_id.to_string(),
                dropped_count: 1,
            }],
            created_unix_ns,
            superseded_record_key: String::new(),
        };
        let part_key = keys::reconstruct_rewrite_part_key(&record, &part).expect("part key");
        store
            .put(&part_key, part_bytes, PutOptions::default())
            .await
            .expect("put rewrite part object");
        let record_key = keys::rewrite_record_key_for(&record).expect("rewrite record key");
        store
            .put(
                &record_key,
                ravel_commit::erasure::encode_rewrite(&record),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put rewrite record");

        let metrics = ScrubMetrics::default();
        let worker = solo_worker();

        // Pre-corruption: a full tick over the unmodified rewrite part is
        // clean at every level.
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::Rewrite),
            0
        );

        // Flip a byte in the rewrite part object.
        corrupt_first_byte(&store, &part_key).await;

        let metrics = ScrubMetrics::default();
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::Rewrite),
            1,
            "the injected rewrite-part bit flip must surface under level=rewrite"
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0
        );
    }

    /// A superseded generation's parts stay out of the corpus (issue #1686).
    /// Compacts a bucket, then publishes a rewrite record naming that
    /// compaction record in `superseded_record_key`, so the bucket holds both
    /// generations at once, which is the state an erasure rewrite leaves
    /// behind until a horizon-gated sweep retires the older one. Both parts
    /// are then corrupted in the same tick: the live rewrite part must be
    /// counted, the superseded L1 part must not. Corrupting both is what
    /// makes the `l1 == 0` assertion mean "filtered out" rather than "the
    /// corpus was empty".
    #[tokio::test]
    async fn a_superseded_compaction_part_is_left_out_of_the_corpus() {
        use ravel_commit::erasure;
        use ravel_proto::commit::v1::{
            CompactionInputIdentity, CompactionPart, RewriteDrop, RewriteRecord,
        };

        let store = MemoryStore::new();
        let tenant_id = tenant();
        let tenant_hash = tenant_id.hash();
        let shard = 0u32;
        let ingest_hour_bucket = 500_000u32;
        let created_unix_ns = 500_000 * NS_PER_HOUR;

        publish_segment(&store, 1, &["cpu"]).await;
        publish_segment(&store, 2, &["mem"]).await;

        let bucket = ravel_maintain::Bucket::new(tenant_hash, Signal::Metrics, shard, 500_000);
        let compact_clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let outcome = ravel_maintain::compact_bucket(
            &store,
            &compact_clock,
            &ravel_maintain::CompactorConfig::default(),
            &bucket,
        )
        .await
        .expect("compact");
        assert!(
            matches!(outcome, ravel_maintain::CompactionOutcome::Compacted { .. }),
            "two sealed L0 inputs must compact, got {outcome:?}"
        );

        let prefix = keys::commit_shard_hour_prefix(&tenant_hash, Signal::Metrics, shard, 500_000)
            .expect("prefix");
        let metas = list_all(&store, &prefix).await.expect("list bucket");
        let compaction_record_key = metas
            .iter()
            .map(|m| m.key.clone())
            .find(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .expect("a compaction record was published");
        let compaction_record_bytes = store
            .get(&compaction_record_key, GetRange::Full)
            .await
            .expect("get compaction record")
            .data;
        let compaction_record = ravel_commit::record::decode_compaction(&compaction_record_bytes)
            .expect("decode compaction record");
        let l1_part_key =
            keys::reconstruct_l1_part_key(&compaction_record, &compaction_record.parts[0])
                .expect("l1 part key");

        // The rewrite generation: a real RSEG segment as its single part, so
        // the content tier can rehash it, and `superseded_record_key` naming
        // the compaction record above.
        let labels = LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "cpu".to_string(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant_id, "cpu", &labels).expect("series id");
        let series = vec![SeriesInput {
            series_id,
            labels,
            samples: vec![Sample {
                ts_ns: created_unix_ns,
                value: 1.0,
            }],
        }];
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: Uuid::from_u128(3_000).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: created_unix_ns - 1_000,
            max_ingest_ts_ns: created_unix_ns,
        };
        let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
        let rewrite_part_bytes = written.bytes;
        let rewrite_part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xffu8; 16],
            content_hash: written.summary.blake3.to_vec(),
            object_size: rewrite_part_bytes.len() as u64,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            run_count: 1,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            declared_column_stats: Vec::new(),
        };
        let request_id = Uuid::from_u128(0xEA5F);
        let request_ids = vec![request_id.to_string()];
        // A supersession-only rewrite names no raw inputs: exactly one of
        // `inputs` and `superseded_record_key` may be set.
        let no_inputs: Vec<CompactionInputIdentity> = Vec::new();
        let input_set_hash = erasure::compute_rewrite_input_set_hash(
            &no_inputs,
            Some(compaction_record_key.as_str()),
            &request_ids,
        );
        let rewrite_record = RewriteRecord {
            format_version: 1,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            shard,
            ingest_hour_bucket,
            inputs: Vec::new(),
            input_set_hash: input_set_hash.to_vec(),
            parts: vec![rewrite_part.clone()],
            drops: vec![RewriteDrop {
                request_id: request_id.to_string(),
                dropped_count: 1,
            }],
            created_unix_ns,
            superseded_record_key: compaction_record_key.clone(),
        };
        let rewrite_part_key =
            keys::reconstruct_rewrite_part_key(&rewrite_record, &rewrite_part).expect("part key");
        store
            .put(&rewrite_part_key, rewrite_part_bytes, PutOptions::default())
            .await
            .expect("put rewrite part object");
        let rewrite_record_key =
            keys::rewrite_record_key_for(&rewrite_record).expect("rewrite record key");
        store
            .put(
                &rewrite_record_key,
                ravel_commit::erasure::encode_rewrite(&rewrite_record),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put rewrite record");

        let metrics = ScrubMetrics::default();
        let worker = solo_worker();

        // Pre-corruption: both generations' parts are byte-correct, so a full
        // tick is clean at every level whatever the filter admits.
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::Rewrite),
            0
        );

        corrupt_first_byte(&store, &l1_part_key).await;
        corrupt_first_byte(&store, &rewrite_part_key).await;

        let metrics = ScrubMetrics::default();
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::Rewrite),
            1,
            "the live rewrite part is still scrubbed, so the corpus is not empty"
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0,
            "the superseded compaction part must be left out of the corpus"
        );
    }

    /// An overlap loser's parts stay out of the corpus (issue #1686, the
    /// second exclusion mechanism after supersession). Compacts a bucket to
    /// get a real two-input compaction record, then publishes a second record
    /// in the same bucket whose single input is one of those two: the shared
    /// input puts both in one overlap component, and
    /// `select_authoritative_compaction_records` gives the larger input set
    /// the win, so the hand-built record is the loser and the catalog serves
    /// none of its parts.
    ///
    /// Two ticks, because one cannot say both things. The first corrupts only
    /// the loser's part: `l1 == 0` says the loser is excluded, but on its own
    /// that also holds if the filter dropped the whole component. The second
    /// corrupts the winner's part as well, leaving both corrupt in one tick:
    /// `l1 == 1` says the winner is still scrubbed (so the corpus is not
    /// empty and the component was not dropped wholesale) and that exactly
    /// one of the two overlapping records contributed parts (so the winner is
    /// chosen per component, not per record).
    #[tokio::test]
    async fn an_overlap_loser_part_is_left_out_of_the_corpus() {
        use ravel_commit::erasure;
        use ravel_proto::commit::v1::{CompactionPart, CompactionRecord};

        let store = MemoryStore::new();
        let tenant_id = tenant();
        let tenant_hash = tenant_id.hash();
        let shard = 0u32;
        let ingest_hour_bucket = 500_000u32;
        let created_unix_ns = 500_000 * NS_PER_HOUR;

        publish_segment(&store, 1, &["cpu"]).await;
        publish_segment(&store, 2, &["mem"]).await;

        let bucket = ravel_maintain::Bucket::new(tenant_hash, Signal::Metrics, shard, 500_000);
        let compact_clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let outcome = ravel_maintain::compact_bucket(
            &store,
            &compact_clock,
            &ravel_maintain::CompactorConfig::default(),
            &bucket,
        )
        .await
        .expect("compact");
        assert!(
            matches!(outcome, ravel_maintain::CompactionOutcome::Compacted { .. }),
            "two sealed L0 inputs must compact, got {outcome:?}"
        );

        let prefix = keys::commit_shard_hour_prefix(&tenant_hash, Signal::Metrics, shard, 500_000)
            .expect("prefix");
        let metas = list_all(&store, &prefix).await.expect("list bucket");
        let winner_record_key = metas
            .iter()
            .map(|m| m.key.clone())
            .find(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .expect("a compaction record was published");
        let winner_record_bytes = store
            .get(&winner_record_key, GetRange::Full)
            .await
            .expect("get compaction record")
            .data;
        let winner = ravel_commit::record::decode_compaction(&winner_record_bytes)
            .expect("decode compaction record");
        assert_eq!(
            winner.inputs.len(),
            2,
            "the compactor's record must name both sealed L0 inputs, so its input set is \
             strictly larger than the loser's and the tie-break is decided"
        );
        let winner_part_key =
            keys::reconstruct_l1_part_key(&winner, &winner.parts[0]).expect("winner l1 part key");

        // The loser: a second compaction record in the same bucket naming one
        // of the winner's inputs. Sharing an input is what puts the two in
        // one overlap component; naming strictly fewer is what makes the
        // winner's win deterministic rather than hash-order dependent.
        let shared_inputs = vec![winner.inputs[0].clone()];
        let labels = LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "cpu".to_string(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant_id, "cpu", &labels).expect("series id");
        let series = vec![SeriesInput {
            series_id,
            labels,
            samples: vec![Sample {
                ts_ns: created_unix_ns,
                value: 1.0,
            }],
        }];
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: Uuid::from_u128(4_000).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: created_unix_ns - 1_000,
            max_ingest_ts_ns: created_unix_ns,
        };
        let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
        let loser_part_bytes = written.bytes;
        let loser_part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xffu8; 16],
            content_hash: written.summary.blake3.to_vec(),
            object_size: loser_part_bytes.len() as u64,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            run_count: 1,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            declared_column_stats: Vec::new(),
        };
        let loser = CompactionRecord {
            format_version: 1,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            shard,
            ingest_hour_bucket,
            level: 1,
            input_set_hash: erasure::compute_compaction_input_set_hash(&shared_inputs).to_vec(),
            inputs: shared_inputs,
            parts: vec![loser_part.clone()],
            created_unix_ns,
        };
        let loser_record_key = keys::compaction_record_key_for(&loser).expect("loser record key");
        assert_ne!(
            loser_record_key, winner_record_key,
            "the two records must be distinct objects in one bucket"
        );
        let loser_part_key =
            keys::reconstruct_l1_part_key(&loser, &loser_part).expect("loser l1 part key");
        assert_ne!(
            loser_part_key, winner_part_key,
            "the two records' parts must be distinct objects"
        );
        store
            .put(&loser_part_key, loser_part_bytes, PutOptions::default())
            .await
            .expect("put loser l1 part object");
        store
            .put(
                &loser_record_key,
                ravel_commit::record::encode_compaction(&loser),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put loser compaction record");

        let worker = solo_worker();

        // Pre-corruption: both records' parts are byte-correct, so a full
        // tick is clean at every level whatever the filter admits.
        let metrics = ScrubMetrics::default();
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::Rewrite),
            0
        );

        corrupt_first_byte(&store, &loser_part_key).await;

        let metrics = ScrubMetrics::default();
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0,
            "the overlap loser's part must be left out of the corpus"
        );

        corrupt_first_byte(&store, &winner_part_key).await;

        let metrics = ScrubMetrics::default();
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            1,
            "with both overlapping records' parts corrupt in one tick, exactly one is counted: \
             the authoritative record's part is still scrubbed and the loser's is not"
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0,
            "the two L0 segments are untouched"
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::Rewrite),
            0,
            "this bucket holds no rewrite record"
        );
    }

    /// A tombstoned bucket's compaction parts stay out of the corpus (issue
    /// #1686, the third exclusion after supersession and overlap). Retention's
    /// sweep may delete a tombstoned bucket's parts at any moment after the
    /// tombstone lands and no query reads them meanwhile, so rot in them must
    /// not page an operator. Two buckets in two ingest hours: the compactor's
    /// own bucket, tombstoned after it compacts exactly as retention would
    /// tombstone it, and a hand-built compaction record in the next hour with
    /// a real RSEG part, alone in its bucket so it is nobody's overlap loser.
    ///
    /// Two ticks, because one cannot say both things. The first corrupts only
    /// the tombstoned bucket's part: `l1 == 0` says it is excluded, but on its
    /// own that also holds on an empty corpus. The second corrupts the live
    /// bucket's part as well, leaving both corrupt in one tick: `l1 == 1` says
    /// the untombstoned part is still scrubbed and that the tombstone took
    /// exactly its own bucket's parts out, not every part on the shard.
    #[tokio::test]
    async fn a_tombstoned_buckets_parts_are_left_out_of_the_corpus() {
        use ravel_commit::erasure;
        use ravel_proto::commit::v1::{
            CompactionInputIdentity, CompactionPart, CompactionRecord, RetentionTombstone,
        };

        let store = MemoryStore::new();
        let tenant_id = tenant();
        let tenant_hash = tenant_id.hash();
        let shard = 0u32;
        let tombstoned_hour = 500_000u32;
        let live_hour = 500_001u32;
        let live_created_unix_ns = 500_001 * NS_PER_HOUR;

        publish_segment(&store, 1, &["cpu"]).await;
        publish_segment(&store, 2, &["mem"]).await;

        let bucket =
            ravel_maintain::Bucket::new(tenant_hash, Signal::Metrics, shard, tombstoned_hour);
        let compact_clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let outcome = ravel_maintain::compact_bucket(
            &store,
            &compact_clock,
            &ravel_maintain::CompactorConfig::default(),
            &bucket,
        )
        .await
        .expect("compact");
        assert!(
            matches!(outcome, ravel_maintain::CompactionOutcome::Compacted { .. }),
            "two sealed L0 inputs must compact, got {outcome:?}"
        );

        let prefix =
            keys::commit_shard_hour_prefix(&tenant_hash, Signal::Metrics, shard, tombstoned_hour)
                .expect("prefix");
        let metas = list_all(&store, &prefix).await.expect("list bucket");
        let tombstoned_record_key = metas
            .iter()
            .map(|m| m.key.clone())
            .find(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .expect("a compaction record was published");
        let tombstoned_record_bytes = store
            .get(&tombstoned_record_key, GetRange::Full)
            .await
            .expect("get compaction record")
            .data;
        let tombstoned_record = ravel_commit::record::decode_compaction(&tombstoned_record_bytes)
            .expect("decode compaction record");
        let tombstoned_part_key =
            keys::reconstruct_l1_part_key(&tombstoned_record, &tombstoned_record.parts[0])
                .expect("tombstoned l1 part key");

        // Retire the compacted bucket the way retention does: a real
        // `RetentionTombstone` at the bucket's fixed `retire.tmb` key. The
        // scrubber classifies it by filename alone and never reads the body.
        let tombstone = RetentionTombstone {
            format_version: 1,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            shard,
            ingest_hour_bucket: tombstoned_hour,
            retired_at_ns: 500_004 * NS_PER_HOUR,
            retention_window_ns: NS_PER_HOUR as u64,
            record_count_observed: 3,
        };
        let tombstone_key =
            keys::retention_tombstone_key_for(&tombstone).expect("retention tombstone key");
        store
            .put(
                &tombstone_key,
                record::encode_tombstone(&tombstone),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put retention tombstone");

        // The live bucket: a compaction record in the next ingest hour whose
        // single part is a real RSEG segment, so the content tier can rehash
        // it, with one input of its own so the record has the shape the
        // compactor writes.
        let labels = LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "cpu".to_string(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant_id, "cpu", &labels).expect("series id");
        let series = vec![SeriesInput {
            series_id,
            labels,
            samples: vec![Sample {
                ts_ns: live_created_unix_ns,
                value: 1.0,
            }],
        }];
        let live_writer_id = Uuid::from_u128(5_000).to_string();
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: live_writer_id.clone(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: live_created_unix_ns - 1_000,
            max_ingest_ts_ns: live_created_unix_ns,
        };
        let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
        let live_part_bytes = written.bytes;
        let live_part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xffu8; 16],
            content_hash: written.summary.blake3.to_vec(),
            object_size: live_part_bytes.len() as u64,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            run_count: 1,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            declared_column_stats: Vec::new(),
        };
        let live_inputs = vec![CompactionInputIdentity {
            writer_id: live_writer_id,
            writer_epoch: 1,
            writer_seq: 1,
        }];
        let live = CompactionRecord {
            format_version: 1,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            shard,
            ingest_hour_bucket: live_hour,
            level: 1,
            input_set_hash: erasure::compute_compaction_input_set_hash(&live_inputs).to_vec(),
            inputs: live_inputs,
            parts: vec![live_part.clone()],
            created_unix_ns: live_created_unix_ns,
        };
        let live_record_key = keys::compaction_record_key_for(&live).expect("live record key");
        let live_part_key =
            keys::reconstruct_l1_part_key(&live, &live_part).expect("live l1 part key");
        assert_ne!(
            live_part_key, tombstoned_part_key,
            "the two records' parts must be distinct objects"
        );
        store
            .put(&live_part_key, live_part_bytes, PutOptions::default())
            .await
            .expect("put live l1 part object");
        store
            .put(
                &live_record_key,
                ravel_commit::record::encode_compaction(&live),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put live compaction record");

        let worker = solo_worker();

        // Pre-corruption: both buckets' parts are byte-correct, so a full
        // tick is clean at every level whatever the filter admits.
        let metrics = ScrubMetrics::default();
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::Rewrite),
            0
        );

        corrupt_first_byte(&store, &tombstoned_part_key).await;

        let metrics = ScrubMetrics::default();
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0,
            "the tombstoned bucket's compaction part must be left out of the corpus"
        );

        corrupt_first_byte(&store, &live_part_key).await;

        let metrics = ScrubMetrics::default();
        run_cycle(
            &store,
            None,
            1,
            1,
            1,
            &metrics,
            &worker,
            &worker.solo_live_set(),
            None,
        )
        .await;
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            1,
            "with both buckets' parts corrupt in one tick, exactly one is counted: the live \
             bucket's part is still scrubbed and the tombstoned bucket's is not"
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0,
            "the two L0 segments are untouched"
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::Rewrite),
            0,
            "neither bucket holds a rewrite record"
        );
    }

    /// Flip the first byte of a stored object, so the content tier's
    /// whole-object blake3 disagrees with the hash its record recorded.
    async fn corrupt_first_byte(store: &MemoryStore, key: &str) {
        let existing = store.get(key, GetRange::Full).await.expect("get part");
        let mut corrupted = existing.data.to_vec();
        corrupted[0] ^= 0x01;
        store
            .put(key, Bytes::from(corrupted), PutOptions::default())
            .await
            .expect("overwrite corrupted part");
    }

    /// Run `ticks` content-tier ticks over shard 0 of a corpus of `records`
    /// one-series L0 segments, one per ingest hour, listed four entries per
    /// page, and return each tick's `(LIST calls, GET calls)`.
    async fn per_tick_request_counts(
        records: u64,
        period_secs: u64,
        ticks: usize,
    ) -> Vec<(u64, u64)> {
        let memory = Arc::new(MemoryStore::with_page_size(4));
        for seq in 1..=records {
            let hour = 500_000 + u32::try_from(seq).expect("small seq");
            publish_segment_at(&memory, seq, &["cpu"], hour).await;
        }
        let store = ravel_object_store::InstrumentedStore::new(memory.clone());
        let counters = store.metrics();
        let tenant_hash = tenant().hash();
        let metrics = ScrubMetrics::default();
        let clock = ravel_maintain::FixedClock::new(500_001 * NS_PER_HOUR);
        let mut out = Vec::with_capacity(ticks);
        for _ in 0..ticks {
            let before = counters.snapshot();
            run_shard_tick(
                &store,
                &clock,
                &tenant_hash,
                Signal::Metrics,
                0,
                period_secs,
                1,
                None,
                None,
                &metrics,
            )
            .await;
            let after = counters.snapshot();
            out.push((
                after.list.calls - before.list.calls,
                after.get.calls - before.get.calls,
            ));
        }
        out
    }

    /// ADR-1686 decisions 1 to 3: a tick resumes the listing from its marker
    /// and stops once its budget is spent, so over an unchanged corpus every
    /// tick after the first issues the same LIST and GET count whatever the
    /// corpus size. Both corpora run on a two-entry budget (`ceil(8 / 4)` and
    /// `ceil(16 / 8)`), so a steady tick is two LISTs (the tail count over the
    /// last two ingest hours, whose two entries fit in one page, and the walk's
    /// one listing page) and, on GETs, one cursor GET plus per consumed record one
    /// record GET and the scrub's footer and whole-object GETs: `1 + 2 * 3 =
    /// 7`. Only tick 1 differs, by the rotation's one LIST-only count:
    /// `ceil(N / 4)` full pages plus the empty page a full page's continuation
    /// leads to, and it pays no tail count because it opens the rotation.
    #[tokio::test]
    async fn scrub_ticks_over_an_unchanged_corpus_issue_a_constant_request_count() {
        let small = per_tick_request_counts(8, 4, 4).await;
        let large = per_tick_request_counts(16, 8, 4).await;

        assert_eq!(small[0], (3 + 1, 7), "8 records: tick 1 counts in 3 pages");
        assert_eq!(large[0], (5 + 1, 7), "16 records: tick 1 counts in 5 pages");
        for tick in 1..4 {
            assert_eq!(small[tick], (2, 7), "8 records, tick {}", tick + 1);
            assert_eq!(large[tick], (2, 7), "16 records, tick {}", tick + 1);
        }
    }

    /// A delegating store that records the key of every whole-object GET, so a
    /// test can name exactly which objects the content tier read.
    struct FullGetLog {
        inner: Arc<dyn ObjectStoreBackend>,
        full_gets: parking_lot::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for FullGetLog {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            if matches!(range, GetRange::Full) {
                self.full_gets.lock().push(key.to_string());
            }
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list_after(prefix, start_after, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// ADR-1686 decision 6: one rotation of one-entry ticks over a corpus that
    /// mixes plain L0 hours with a compacted hour visits every object exactly
    /// once: the seven L0 data objects (the two the compaction folded included,
    /// as before) and the compacted hour's single L1 part. The corpus is eight
    /// listing entries (seven commit records and one compaction record) listed
    /// two per page, so the one-entry budget `ceil(8 / 100)` finishes the
    /// rotation on tick 8 and the marker stays set until then.
    #[tokio::test]
    async fn a_full_rotation_visits_every_record_once() {
        let memory = Arc::new(MemoryStore::with_page_size(2));
        let tenant_hash = tenant().hash();
        let mut data_keys = Vec::new();
        for (seq, hour) in [(1, 500_000), (2, 500_000)] {
            data_keys.push(publish_segment_at(&memory, seq, &["cpu"], hour).await);
        }
        let bucket = ravel_maintain::Bucket::new(tenant_hash, Signal::Metrics, 0, 500_000);
        let compact_clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let outcome = ravel_maintain::compact_bucket(
            memory.as_ref(),
            &compact_clock,
            &ravel_maintain::CompactorConfig::default(),
            &bucket,
        )
        .await
        .expect("compact");
        assert!(
            matches!(outcome, ravel_maintain::CompactionOutcome::Compacted { .. }),
            "two sealed L0 inputs must compact, got {outcome:?}"
        );
        for (seq, hour) in [
            (3, 500_001),
            (4, 500_001),
            (5, 500_001),
            (6, 500_002),
            (7, 500_002),
        ] {
            data_keys.push(publish_segment_at(&memory, seq, &["cpu"], hour).await);
        }

        let shard_prefix =
            keys::commit_shard_prefix(&tenant_hash, Signal::Metrics, 0).expect("prefix");
        let listed = list_all(memory.as_ref(), &shard_prefix)
            .await
            .expect("list shard");
        assert_eq!(
            listed.len(),
            8,
            "seven commit records and one compaction record"
        );
        let record_key = listed
            .iter()
            .map(|m| m.key.clone())
            .find(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .expect("a compaction record was published");
        let record_bytes = memory
            .get(&record_key, GetRange::Full)
            .await
            .expect("get compaction record")
            .data;
        let record = ravel_commit::record::decode_compaction(&record_bytes)
            .expect("decode compaction record");
        assert_eq!(record.parts.len(), 1, "two small inputs fit in one L1 part");
        let part_key =
            keys::reconstruct_l1_part_key(&record, &record.parts[0]).expect("l1 part key");

        let mut expected: Vec<String> = data_keys.clone();
        expected.push(part_key.clone());
        expected.sort();
        let mut expected_bytes = 0u64;
        for key in &expected {
            expected_bytes += memory.head(key).await.expect("head object").size;
        }

        let store = FullGetLog {
            inner: memory.clone(),
            full_gets: parking_lot::Mutex::new(Vec::new()),
        };
        let clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let metrics = ScrubMetrics::default();
        for tick in 1..=8u64 {
            run_shard_tick(
                &store,
                &clock,
                &tenant_hash,
                Signal::Metrics,
                0,
                100,
                1,
                None,
                None,
                &metrics,
            )
            .await;
            let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
                .await
                .expect("cursor loads");
            if tick < 8 {
                assert!(cursor.last_commit_key.is_some(), "tick {tick}: marker set");
                assert_eq!(cursor.rotation_entries_visited, tick, "tick {tick}");
                assert_eq!(cursor.rotation_total_entries, Some(8), "tick {tick}");
            } else {
                assert_eq!(cursor.last_commit_key, None, "tick 8 ends the rotation");
                assert_eq!(cursor.rotation_total_entries, None);
                assert_eq!(cursor.last_rotation_bytes, Some(expected_bytes));
            }
        }

        let mut visited: Vec<String> = store
            .full_gets
            .lock()
            .iter()
            .filter(|key| !key.contains("/c/") && !key.contains("/maint/"))
            .cloned()
            .collect();
        visited.sort();
        assert_eq!(visited, expected, "every object once, and nothing else");
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0)
                + metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0
        );
    }

    /// ADR-1686 decision 1: a cursor an older build wrote, holding only
    /// `last_object_key` and `rotation_started_unix_ns`, still loads. The new
    /// fields take their defaults and the old marker is ignored, so the next
    /// tick opens a fresh rotation with a LIST-only count and consumes the
    /// first commit record.
    #[tokio::test]
    async fn an_old_format_cursor_loads_with_defaults() {
        let store = MemoryStore::new();
        let tenant_hash = tenant().hash();
        publish_segment(&store, 1, &["cpu"]).await;
        publish_segment(&store, 2, &["mem"]).await;
        let old = br#"{"last_object_key":"t/old/data/key.rseg","rotation_started_unix_ns":7}"#;
        store
            .put(
                &cursor_key(&tenant_hash, Signal::Metrics, 0),
                Bytes::from_static(old),
                PutOptions::default(),
            )
            .await
            .expect("put old cursor");

        let loaded = load_cursor(&store, &tenant_hash, Signal::Metrics, 0, 99)
            .await
            .expect("an old-format cursor still loads");
        assert_eq!(loaded.last_commit_key, None);
        assert_eq!(loaded.rotation_started_unix_ns, 7);
        assert_eq!(loaded.rotation_bytes_seen, 0);
        assert_eq!(loaded.last_rotation_bytes, None);
        assert_eq!(loaded.rotation_total_entries, None);
        assert_eq!(loaded.rotation_entries_visited, 0);
        assert_eq!(loaded.rotation_appended_entries, 0);
        assert_eq!(loaded.rotation_tail_dir, None);
        assert_eq!(loaded.rotation_tail_entries, 0);

        let clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let metrics = ScrubMetrics::default();
        run_shard_tick(
            &store,
            &clock,
            &tenant_hash,
            Signal::Metrics,
            0,
            100,
            1,
            None,
            None,
            &metrics,
        )
        .await;

        let shard_prefix =
            keys::commit_shard_prefix(&tenant_hash, Signal::Metrics, 0).expect("prefix");
        let listed = list_all(&store, &shard_prefix).await.expect("list shard");
        let cursor = load_cursor(&store, &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(cursor.rotation_total_entries, Some(2));
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(listed[0].key.as_str())
        );
        assert_eq!(cursor.rotation_entries_visited, 1);
        assert_eq!(cursor.rotation_started_unix_ns, 500_003 * NS_PER_HOUR);
    }

    /// ADR-1686 amendment, decision 2: a record GET that fails transiently
    /// stops the tick at that unit. The marker stays behind the failed record,
    /// so the unit is retried next tick instead of being skipped for a whole
    /// rotation, and the tick's requests stay inside the plan's request cap
    /// (every GET attempt, failed included, is charged to it).
    #[tokio::test]
    async fn a_failed_record_get_holds_the_marker_and_the_tick_stays_in_budget() {
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault,
        };

        let memory = Arc::new(MemoryStore::with_page_size(4));
        for seq in 1..=8u64 {
            publish_segment(&memory, seq, &["cpu"]).await;
        }
        let tenant_hash = tenant().hash();
        let shard_prefix =
            keys::commit_shard_prefix(&tenant_hash, Signal::Metrics, 0).expect("prefix");
        let listed = list_all(memory.as_ref(), &shard_prefix)
            .await
            .expect("list shard");
        assert_eq!(listed.len(), 8, "eight commit records");
        let failing = listed[2].key.clone();

        // The tick's plan, computed from the same inputs the tick sees: four
        // entries (`ceil(8 / 2)` a tick over a two-tick rotation) and, at eight
        // requests an entry, a cap of 32 requests.
        let clock = ravel_maintain::FixedClock::new(500_001 * NS_PER_HOUR);
        let mut probe = ScrubCursor::new(tenant_hash, Signal::Metrics, 0, clock.now_ns());
        let mut opening = TailTally::default();
        for meta in &listed {
            opening.observe(&meta.key);
        }
        probe.start_rotation(&opening, clock.now_ns());
        let plan = probe.plan_tick(2, 1, None, clock.now_ns());
        assert_eq!(plan.budget.max_entries, 4);
        assert_eq!(plan.budget.max_requests, 32);

        let faulted = Arc::new(FaultStore::new(
            memory.clone(),
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Get,
                    ScriptedFault::Transient("scrub: injected record GET fault".to_string()),
                )
                .with_key_contains(failing.clone()),
            ),
        ));
        let store = ravel_object_store::InstrumentedStore::new(faulted.clone());
        let counters = store.metrics();
        let metrics = ScrubMetrics::default();

        let before = counters.snapshot();
        run_shard_tick(
            &store,
            &clock,
            &tenant_hash,
            Signal::Metrics,
            0,
            2,
            1,
            None,
            None,
            &metrics,
        )
        .await;
        let after = counters.snapshot();
        assert_eq!(
            faulted.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the injected fault must have fired exactly once"
        );

        // Three LIST-only counting pages plus the walk's one page; one cursor
        // GET, then per consumed record one record GET and the scrub's footer
        // and whole-object GETs, then the failed record's own GET:
        // `1 + 2 * 3 + 1 = 8`.
        let lists = after.list.calls - before.list.calls;
        let gets = after.get.calls - before.get.calls;
        assert_eq!((lists, gets), (4, 8), "the tick's LIST and GET counts");
        // The walk's own requests: everything but the cursor GET, the cursor
        // PUT, and the three counting pages.
        let walk_requests = lists + gets - 5;
        assert_eq!(walk_requests, 7);
        assert!(
            walk_requests <= plan.budget.max_requests,
            "the tick issued {walk_requests} requests against a cap of {}",
            plan.budget.max_requests
        );

        let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(listed[1].key.as_str()),
            "the marker must stop behind the record whose GET failed"
        );
        assert_eq!(cursor.rotation_entries_visited, 2);
        assert_eq!(cursor.rotation_total_entries, Some(8));

        // The same fault next tick leaves the marker where it is: the unit is
        // retried, never skipped.
        run_shard_tick(
            &store,
            &clock,
            &tenant_hash,
            Signal::Metrics,
            0,
            2,
            1,
            None,
            None,
            &metrics,
        )
        .await;
        let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(listed[1].key.as_str())
        );
        assert_eq!(cursor.rotation_entries_visited, 2);

        // Once the fault clears, the retried unit is consumed and the walk
        // moves on: four more entries at this budget.
        run_shard_tick(
            memory.as_ref(),
            &clock,
            &tenant_hash,
            Signal::Metrics,
            0,
            2,
            1,
            None,
            None,
            &metrics,
        )
        .await;
        let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(listed[5].key.as_str())
        );
        assert_eq!(cursor.rotation_entries_visited, 6);
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0,
            "a transient GET fault is never an anomaly"
        );
    }

    /// The request cap stops a tick whose entry cap is not yet filled: a
    /// compaction record naming eight parts costs its record GET plus
    /// `8 * SCRUB_REQUESTS_PER_OBJECT` requests, more than the whole tick's
    /// cap, so the tick ends after three of its four allowed entries.
    #[tokio::test]
    async fn the_request_cap_binds_before_the_entry_cap_on_a_many_part_unit() {
        use ravel_proto::commit::v1::CompactionPart;

        let memory = Arc::new(MemoryStore::new());
        let tenant_hash = tenant().hash();
        publish_segment_at(&memory, 1, &["cpu"], 500_000).await;
        publish_segment_at(&memory, 2, &["mem"], 500_000).await;
        let bucket = ravel_maintain::Bucket::new(tenant_hash, Signal::Metrics, 0, 500_000);
        let compact_clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let outcome = ravel_maintain::compact_bucket(
            memory.as_ref(),
            &compact_clock,
            &ravel_maintain::CompactorConfig::default(),
            &bucket,
        )
        .await
        .expect("compact");
        assert!(
            matches!(outcome, ravel_maintain::CompactionOutcome::Compacted { .. }),
            "two sealed L0 inputs must compact, got {outcome:?}"
        );
        for seq in 3..=7u64 {
            publish_segment_at(&memory, seq, &["cpu"], 500_001).await;
        }

        // Rewrite the compaction record in place so it names eight parts, each
        // a byte-correct copy of the one real part under its own key.
        let hour_prefix = keys::commit_shard_hour_prefix(&tenant_hash, Signal::Metrics, 0, 500_000)
            .expect("hour prefix");
        let record_key = list_all(memory.as_ref(), &hour_prefix)
            .await
            .expect("list bucket")
            .iter()
            .map(|m| m.key.clone())
            .find(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .expect("a compaction record was published");
        let mut record = ravel_commit::record::decode_compaction(
            &memory
                .get(&record_key, GetRange::Full)
                .await
                .expect("get compaction record")
                .data,
        )
        .expect("decode compaction record");
        assert_eq!(record.parts.len(), 1);
        let part = record.parts[0].clone();
        let part_bytes = memory
            .get(
                &keys::reconstruct_l1_part_key(&record, &part).expect("part key"),
                GetRange::Full,
            )
            .await
            .expect("get part")
            .data;
        record.parts = (0..8u8)
            .map(|index| CompactionPart {
                part_index: u32::from(index),
                first_series_id: vec![index; 16],
                last_series_id: vec![index; 16],
                ..part.clone()
            })
            .collect();
        assert_eq!(
            keys::compaction_record_key_for(&record).expect("record key"),
            record_key,
            "the parts do not move the record's key"
        );
        let mut part_keys = Vec::new();
        for part in &record.parts {
            let key = keys::reconstruct_l1_part_key(&record, part).expect("part key");
            memory
                .put(&key, part_bytes.clone(), PutOptions::default())
                .await
                .expect("put part copy");
            part_keys.push(key);
        }
        memory
            .put(
                &record_key,
                ravel_commit::record::encode_compaction(&record),
                PutOptions::default(),
            )
            .await
            .expect("overwrite compaction record");

        // Eight entries over a two-tick rotation: four entries and 32 requests.
        let clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let mut probe = ScrubCursor::new(tenant_hash, Signal::Metrics, 0, clock.now_ns());
        let shard_prefix =
            keys::commit_shard_prefix(&tenant_hash, Signal::Metrics, 0).expect("prefix");
        let listed = list_all(memory.as_ref(), &shard_prefix)
            .await
            .expect("list shard");
        assert_eq!(
            listed.len(),
            8,
            "seven commit records and one compaction record"
        );
        let mut opening = TailTally::default();
        for meta in &listed {
            opening.observe(&meta.key);
        }
        probe.start_rotation(&opening, clock.now_ns());
        let plan = probe.plan_tick(2, 1, None, clock.now_ns());
        assert_eq!(plan.budget.max_entries, 4);
        assert_eq!(plan.budget.max_requests, 32);

        let store = FullGetLog {
            inner: memory.clone(),
            full_gets: parking_lot::Mutex::new(Vec::new()),
        };
        let metrics = ScrubMetrics::default();
        run_shard_tick(
            &store,
            &clock,
            &tenant_hash,
            Signal::Metrics,
            0,
            2,
            1,
            None,
            None,
            &metrics,
        )
        .await;

        // One walk page, two commit records at `1 + 4` each, then the
        // compaction unit at `1 + 8 * 4`: 44 requests, past the cap of 32
        // with one of the four allowed entries unused.
        let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(cursor.rotation_entries_visited, 3);
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(record_key.as_str()),
            "the tick stops on the compaction record, short of the entry cap"
        );
        let verified_parts = store
            .full_gets
            .lock()
            .iter()
            .filter(|key| part_keys.contains(key))
            .count();
        assert_eq!(verified_parts, 8, "every part of the unit is verified");
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0
        );
    }

    /// A record GET that fails with an error retrying cannot clear makes the
    /// record unreadable, and is not a reason to hold the marker: holding it
    /// would retry the same unit on every tick and verify nothing after it
    /// again. The record counts once on the unreadable counter at its own
    /// level and nothing on the checksum-mismatch counter, the marker moves
    /// past it in the same tick, and the next unit's object is still verified.
    #[tokio::test]
    async fn a_permanent_record_get_error_is_unreadable_and_the_marker_moves_on() {
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault,
        };

        let memory = Arc::new(MemoryStore::with_page_size(4));
        let mut data_keys = Vec::new();
        for seq in 1..=8u64 {
            data_keys.push(publish_segment(&memory, seq, &["cpu"]).await);
        }
        let tenant_hash = tenant().hash();
        let shard_prefix =
            keys::commit_shard_prefix(&tenant_hash, Signal::Metrics, 0).expect("prefix");
        let listed = list_all(memory.as_ref(), &shard_prefix)
            .await
            .expect("list shard");
        assert_eq!(listed.len(), 8, "eight commit records");
        let failing = listed[2].key.clone();
        // Commit keys and data keys both order by writer id, and every seq
        // here has its own writer, so listing entry `i` names `data_keys[i]`.
        let mut sorted_data_keys = data_keys.clone();
        sorted_data_keys.sort();
        assert_eq!(sorted_data_keys, data_keys);

        let faulted = Arc::new(FaultStore::new(
            memory.clone(),
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Get,
                    ScriptedFault::Permanent("scrub: injected record GET fault".to_string()),
                )
                .with_key_contains(failing.clone()),
            ),
        ));
        let store = FullGetLog {
            inner: faulted.clone(),
            full_gets: parking_lot::Mutex::new(Vec::new()),
        };
        let clock = ravel_maintain::FixedClock::new(500_001 * NS_PER_HOUR);
        let metrics = ScrubMetrics::default();

        // Four entries a tick (`ceil(8 / 2)` over a two-tick rotation).
        run_shard_tick(
            &store,
            &clock,
            &tenant_hash,
            Signal::Metrics,
            0,
            2,
            1,
            None,
            None,
            &metrics,
        )
        .await;
        assert_eq!(
            faulted.fault_count(Op::Get, FaultKind::Permanent),
            1,
            "the injected fault must have fired exactly once"
        );

        let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(listed[3].key.as_str()),
            "the marker must move past the unreadable record in the same tick"
        );
        assert_eq!(cursor.rotation_entries_visited, 4);
        assert_eq!(
            metrics.unreadable(Signal::Metrics, ScrubLevel::L0, UnreadableReason::Permanent),
            1,
            "the unreadable record counts exactly once"
        );
        assert_eq!(unreadable_total(&metrics), 1);
        assert_eq!(mismatch_total(&metrics), 0);

        let verified: Vec<String> = store
            .full_gets
            .lock()
            .iter()
            .filter(|key| !key.contains("/c/") && !key.contains("/maint/"))
            .cloned()
            .collect();
        assert_eq!(
            verified,
            vec![
                data_keys[0].clone(),
                data_keys[1].clone(),
                data_keys[3].clone()
            ],
            "the objects before and after the unreadable record are verified"
        );
    }

    /// Run one tick over eight one-object commit records with `fault` on
    /// every GET of the third record's data object, and return the store, the
    /// listing, the data keys in listing order, and the metrics.
    async fn tick_with_data_object_fault(
        fault: ravel_object_store::fault::ScriptedFault,
    ) -> (
        Arc<MemoryStore>,
        Arc<ravel_object_store::fault::FaultStore<Arc<MemoryStore>>>,
        Vec<ravel_object_store::ObjectMeta>,
        Vec<String>,
        ScrubMetrics,
    ) {
        use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule};

        let memory = Arc::new(MemoryStore::with_page_size(4));
        let mut data_keys = Vec::new();
        for seq in 1..=8u64 {
            data_keys.push(publish_segment(&memory, seq, &["cpu"]).await);
        }
        let tenant_hash = tenant().hash();
        let shard_prefix =
            keys::commit_shard_prefix(&tenant_hash, Signal::Metrics, 0).expect("prefix");
        let listed = list_all(memory.as_ref(), &shard_prefix)
            .await
            .expect("list shard");
        assert_eq!(listed.len(), 8, "eight commit records");
        let faulted = Arc::new(FaultStore::new(
            memory.clone(),
            FaultPlan::empty()
                .with_rule(Rule::new(Op::Get, fault).with_key_contains(data_keys[2].clone())),
        ));
        let clock = ravel_maintain::FixedClock::new(500_001 * NS_PER_HOUR);
        let metrics = ScrubMetrics::default();
        // Four entries a tick (`ceil(8 / 2)` over a two-tick rotation).
        run_shard_tick(
            faulted.as_ref(),
            &clock,
            &tenant_hash,
            Signal::Metrics,
            0,
            2,
            1,
            None,
            None,
            &metrics,
        )
        .await;
        (memory, faulted, listed, data_keys, metrics)
    }

    /// A data-object GET that fails with a retryable error holds the marker
    /// behind its unit, exactly as a failed record GET does, so the object is
    /// retried next tick instead of going unverified for a whole rotation.
    #[tokio::test]
    async fn a_retryable_data_object_get_error_holds_the_marker() {
        use ravel_object_store::fault::{FaultKind, Op, ScriptedFault};

        let (memory, faulted, listed, _, metrics) = tick_with_data_object_fault(
            ScriptedFault::Transient("scrub: injected data-object GET fault".to_string()),
        )
        .await;
        assert_eq!(
            faulted.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the injected fault must have fired exactly once"
        );
        let tenant_hash = tenant().hash();
        let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(listed[1].key.as_str()),
            "the marker must stop behind the unit whose object GET failed"
        );
        assert_eq!(cursor.rotation_entries_visited, 2);
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0,
            "a retryable GET fault is never an anomaly"
        );

        // With the fault cleared, the retried unit is consumed and verified.
        let clock = ravel_maintain::FixedClock::new(500_001 * NS_PER_HOUR);
        run_shard_tick(
            memory.as_ref(),
            &clock,
            &tenant_hash,
            Signal::Metrics,
            0,
            2,
            1,
            None,
            None,
            &metrics,
        )
        .await;
        let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(listed[5].key.as_str())
        );
        assert_eq!(cursor.rotation_entries_visited, 6);
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0
        );
    }

    /// A data-object GET that fails with an error retrying cannot clear counts
    /// once as unreadable under `reason="permanent"` at the object's level,
    /// never as a checksum mismatch, and the marker moves past its unit in the
    /// same tick.
    #[tokio::test]
    async fn a_permanent_data_object_get_error_is_unreadable() {
        use ravel_object_store::fault::{FaultKind, Op, ScriptedFault};

        let (memory, faulted, listed, _, metrics) = tick_with_data_object_fault(
            ScriptedFault::Permanent("scrub: injected data-object GET fault".to_string()),
        )
        .await;
        assert_eq!(
            faulted.fault_count(Op::Get, FaultKind::Permanent),
            1,
            "the structural tier's footer GET fails and nothing else is tried"
        );
        let tenant_hash = tenant().hash();
        let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(listed[3].key.as_str()),
            "the marker moves past the unreadable object in the same tick"
        );
        assert_eq!(cursor.rotation_entries_visited, 4);
        assert_eq!(
            metrics.unreadable(Signal::Metrics, ScrubLevel::L0, UnreadableReason::Permanent),
            1,
            "the unreadable object counts exactly once"
        );
        assert_eq!(unreadable_total(&metrics), 1);
        assert_eq!(mismatch_total(&metrics), 0);
    }

    /// A delegating store that fails a GET with whatever `fault` returns for
    /// it. `fault` sees the key and how many earlier GETs named that same key,
    /// and every error it returns is counted in `fired`, so a test can prove
    /// its fault fired. Unlike [`FaultStore`](ravel_object_store::fault), it
    /// can return any [`StoreError`], `AccessDenied` included.
    struct GetFaults {
        inner: Arc<dyn ObjectStoreBackend>,
        #[allow(clippy::type_complexity)]
        fault: Box<dyn Fn(&str, u64) -> Option<StoreError> + Send + Sync>,
        calls: parking_lot::Mutex<std::collections::HashMap<String, u64>>,
        fired: AtomicU64,
    }

    impl GetFaults {
        fn new(
            inner: Arc<dyn ObjectStoreBackend>,
            fault: impl Fn(&str, u64) -> Option<StoreError> + Send + Sync + 'static,
        ) -> Self {
            GetFaults {
                inner,
                fault: Box::new(fault),
                calls: parking_lot::Mutex::new(std::collections::HashMap::new()),
                fired: AtomicU64::new(0),
            }
        }

        fn fired(&self) -> u64 {
            self.fired.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for GetFaults {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            let earlier = {
                let mut calls = self.calls.lock();
                let count = calls.entry(key.to_string()).or_insert(0);
                let earlier = *count;
                *count += 1;
                earlier
            };
            if let Some(err) = (self.fault)(key, earlier) {
                self.fired.fetch_add(1, Ordering::Relaxed);
                return Err(err);
            }
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list_after(prefix, start_after, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// Eight one-object commit records in shard 0, listed four per page.
    /// Returns the listing and the data keys, both in listing order.
    async fn eight_record_shard(
        memory: &MemoryStore,
    ) -> (Vec<ravel_object_store::ObjectMeta>, Vec<String>) {
        let mut data_keys = Vec::new();
        for seq in 1..=8u64 {
            data_keys.push(publish_segment(memory, seq, &["cpu"]).await);
        }
        let shard_prefix =
            keys::commit_shard_prefix(&tenant().hash(), Signal::Metrics, 0).expect("prefix");
        let listed = list_all(memory, &shard_prefix).await.expect("list shard");
        assert_eq!(listed.len(), 8, "eight commit records");
        (listed, data_keys)
    }

    /// One tick over [`eight_record_shard`] at four entries a tick
    /// (`ceil(8 / 2)` over a two-tick rotation).
    async fn tick_eight(store: &dyn ObjectStoreBackend, metrics: &ScrubMetrics) {
        let clock = ravel_maintain::FixedClock::new(500_001 * NS_PER_HOUR);
        run_shard_tick(
            store,
            &clock,
            &tenant().hash(),
            Signal::Metrics,
            0,
            2,
            1,
            None,
            None,
            metrics,
        )
        .await;
    }

    /// Every (level, reason) cell of `ravel_scrub_unreadable_total` for the
    /// metrics signal, summed.
    fn unreadable_total(metrics: &ScrubMetrics) -> u64 {
        let mut total = 0;
        for level in [ScrubLevel::L0, ScrubLevel::L1, ScrubLevel::Rewrite] {
            for reason in UnreadableReason::ALL {
                total += metrics.unreadable(Signal::Metrics, level, reason);
            }
        }
        total
    }

    /// Every level of `ravel_scrub_checksum_mismatch_total` for the metrics
    /// signal, summed.
    fn mismatch_total(metrics: &ScrubMetrics) -> u64 {
        [ScrubLevel::L0, ScrubLevel::L1, ScrubLevel::Rewrite]
            .into_iter()
            .map(|level| metrics.checksum_mismatch(Signal::Metrics, level))
            .sum()
    }

    /// An access denial on a data-object GET is not corruption: the bytes were
    /// never read. It counts once on the unreadable counter under
    /// `reason="access_denied"`, nothing on the checksum-mismatch counter, and
    /// the marker moves past the unit.
    #[tokio::test]
    async fn an_access_denied_data_object_get_is_unreadable_not_a_mismatch() {
        let memory = Arc::new(MemoryStore::with_page_size(4));
        let (listed, data_keys) = eight_record_shard(&memory).await;
        let denied = data_keys[2].clone();
        let store = GetFaults::new(memory.clone(), move |key, _| {
            (key == denied).then(|| StoreError::AccessDenied("injected KMS denial".to_string()))
        });
        let metrics = ScrubMetrics::default();
        tick_eight(&store, &metrics).await;

        assert_eq!(
            store.fired(),
            1,
            "the footer GET is denied and nothing else of that object is tried"
        );
        assert_eq!(
            metrics.unreadable(
                Signal::Metrics,
                ScrubLevel::L0,
                UnreadableReason::AccessDenied
            ),
            1
        );
        assert_eq!(unreadable_total(&metrics), 1);
        assert_eq!(mismatch_total(&metrics), 0, "an access denial is not rot");
        let cursor = load_cursor(memory.as_ref(), &tenant().hash(), Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(listed[3].key.as_str())
        );
    }

    /// A commit record whose bytes were read and do not decode counts once on
    /// the unreadable counter under `reason="permanent"`, at the record's own
    /// level, and the marker moves past it.
    #[tokio::test]
    async fn a_record_that_does_not_decode_is_unreadable_permanent() {
        let memory = Arc::new(MemoryStore::with_page_size(4));
        let (listed, _) = eight_record_shard(&memory).await;
        memory
            .put(
                &listed[2].key,
                Bytes::from_static(b"not a commit record"),
                PutOptions::default(),
            )
            .await
            .expect("overwrite record");
        let metrics = ScrubMetrics::default();
        tick_eight(memory.as_ref(), &metrics).await;

        assert_eq!(
            metrics.unreadable(Signal::Metrics, ScrubLevel::L0, UnreadableReason::Permanent),
            1
        );
        assert_eq!(unreadable_total(&metrics), 1);
        assert_eq!(mismatch_total(&metrics), 0);
        let cursor = load_cursor(memory.as_ref(), &tenant().hash(), Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(listed[3].key.as_str())
        );
    }

    /// ADR-1686 amendment, decision 3: a compaction record that lands in an
    /// hour the marker has already passed is judged against that hour's whole
    /// lineage set, not against itself alone. It loses the overlap against the
    /// record already there, so its part is left out of the corpus exactly as
    /// it would have been had both been listed in one unit.
    #[tokio::test]
    async fn a_late_landing_compaction_record_is_judged_against_its_whole_hour() {
        use ravel_commit::erasure;
        use ravel_proto::commit::v1::{CompactionPart, CompactionRecord};

        let memory = Arc::new(MemoryStore::new());
        let tenant_id = tenant();
        let tenant_hash = tenant_id.hash();
        let shard = 0u32;
        let ingest_hour_bucket = 500_000u32;
        let created_unix_ns = 500_000 * NS_PER_HOUR;

        publish_segment_at(&memory, 1, &["cpu"], 500_000).await;
        publish_segment_at(&memory, 2, &["mem"], 500_000).await;
        let bucket = ravel_maintain::Bucket::new(tenant_hash, Signal::Metrics, shard, 500_000);
        let compact_clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let outcome = ravel_maintain::compact_bucket(
            memory.as_ref(),
            &compact_clock,
            &ravel_maintain::CompactorConfig::default(),
            &bucket,
        )
        .await
        .expect("compact");
        assert!(
            matches!(outcome, ravel_maintain::CompactionOutcome::Compacted { .. }),
            "two sealed L0 inputs must compact, got {outcome:?}"
        );
        // A second hour, so consuming hour 500_000's last entry leaves the
        // marker sitting on it rather than ending the rotation.
        publish_segment_at(&memory, 3, &["cpu"], 500_001).await;
        publish_segment_at(&memory, 4, &["mem"], 500_001).await;

        let hour_prefix =
            keys::commit_shard_hour_prefix(&tenant_hash, Signal::Metrics, shard, 500_000)
                .expect("hour prefix");
        let winner_record_key = list_all(memory.as_ref(), &hour_prefix)
            .await
            .expect("list bucket")
            .iter()
            .map(|m| m.key.clone())
            .find(|k| {
                matches!(
                    keys::partition_bucket_entry(k),
                    Ok(keys::BucketEntry::CompactionRecord(_))
                )
            })
            .expect("a compaction record was published");
        let winner_bytes = memory
            .get(&winner_record_key, GetRange::Full)
            .await
            .expect("get compaction record")
            .data;
        let winner =
            ravel_commit::record::decode_compaction(&winner_bytes).expect("decode compaction");
        assert_eq!(winner.inputs.len(), 2, "the compactor named both inputs");

        // The late arrival: a second compaction record in the same hour naming
        // one of the winner's two inputs, so the two overlap and the winner's
        // strictly larger input set decides the tie. Its own part is a real
        // segment, so an unfiltered corpus would really scrub it.
        let labels = LabelSet::new(vec![Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "cpu".to_string(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant_id, "cpu", &labels).expect("series id");
        let series = vec![SeriesInput {
            series_id,
            labels,
            samples: vec![Sample {
                ts_ns: created_unix_ns,
                value: 1.0,
            }],
        }];
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: Uuid::from_u128(4_000).to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: created_unix_ns - 1_000,
            max_ingest_ts_ns: created_unix_ns,
        };
        let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
        let late_part_bytes = written.bytes;
        let late_part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xffu8; 16],
            content_hash: written.summary.blake3.to_vec(),
            object_size: late_part_bytes.len() as u64,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            run_count: 1,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            declared_column_stats: Vec::new(),
        };
        let build_late = |inputs: Vec<_>| CompactionRecord {
            format_version: 1,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            shard,
            ingest_hour_bucket,
            level: 1,
            input_set_hash: erasure::compute_compaction_input_set_hash(&inputs).to_vec(),
            inputs,
            parts: vec![late_part.clone()],
            created_unix_ns,
        };
        // The record key is ordered by the input set's hash, and the walk only
        // ever reaches a key that sorts after the marker. Either single-input
        // variant overlaps the winner and loses to it, so take the one whose
        // key the marker has not already passed.
        let late = [0usize, 1]
            .into_iter()
            .map(|i| build_late(vec![winner.inputs[i].clone()]))
            .find(|record| {
                keys::compaction_record_key_for(record).is_ok_and(|key| key > winner_record_key)
            })
            .expect("one single-input variant sorts after the record already in the hour");
        let late_record_key = keys::compaction_record_key_for(&late).expect("late record key");
        let late_part_key =
            keys::reconstruct_l1_part_key(&late, &late_part).expect("late l1 part key");

        let store = FullGetLog {
            inner: memory.clone(),
            full_gets: parking_lot::Mutex::new(Vec::new()),
        };
        let clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let metrics = ScrubMetrics::default();
        // Five listing entries over a six-tick rotation, one a tick: the two
        // commit records, then the hour's compaction record, which leaves the
        // marker on it. The late record raises the estimate to six, still one
        // entry a tick.
        for tick in 1..=3u64 {
            run_shard_tick(
                &store,
                &clock,
                &tenant_hash,
                Signal::Metrics,
                0,
                6,
                1,
                None,
                None,
                &metrics,
            )
            .await;
            let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
                .await
                .expect("cursor loads");
            assert_eq!(cursor.rotation_entries_visited, tick, "tick {tick}");
        }
        let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(winner_record_key.as_str()),
            "the marker sits on the hour's last entry"
        );

        memory
            .put(&late_part_key, late_part_bytes, PutOptions::default())
            .await
            .expect("put the late record's l1 part");
        memory
            .put(
                &late_record_key,
                ravel_commit::record::encode_compaction(&late),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put the late compaction record");
        corrupt_first_byte(&memory, &late_part_key).await;
        store.full_gets.lock().clear();

        run_shard_tick(
            &store,
            &clock,
            &tenant_hash,
            Signal::Metrics,
            0,
            6,
            1,
            None,
            None,
            &metrics,
        )
        .await;

        let visited: Vec<String> = store
            .full_gets
            .lock()
            .iter()
            .filter(|key| !key.contains("/c/") && !key.contains("/maint/"))
            .cloned()
            .collect();
        assert_eq!(
            visited,
            Vec::<String>::new(),
            "the late record loses its overlap, so the tick verifies no object at all"
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L1),
            0,
            "the late record's corrupt part must be left out of the corpus"
        );
        let cursor = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            cursor.last_commit_key.as_deref(),
            Some(late_record_key.as_str()),
            "the marker still advances past the entry it judged"
        );
        assert_eq!(cursor.rotation_entries_visited, 4);
        assert_eq!(
            cursor.rotation_appended_entries, 1,
            "the late record lands in an hour the tail window still covers, so it is counted"
        );
    }

    /// ADR-1686 amendment, decision 4: a cursor GET that fails for any reason
    /// other than `NotFound` skips the shard's tick and leaves the stored
    /// cursor alone. Starting a fresh rotation there would rewind the marker to
    /// the head of the listing and drop the rotation's progress on a transient
    /// throttle.
    #[tokio::test]
    async fn a_failed_cursor_get_skips_the_tick_and_keeps_the_stored_cursor() {
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault,
        };

        let memory = Arc::new(MemoryStore::new());
        for seq in 1..=3u64 {
            publish_segment(&memory, seq, &["cpu"]).await;
        }
        let tenant_hash = tenant().hash();
        let shard_prefix =
            keys::commit_shard_prefix(&tenant_hash, Signal::Metrics, 0).expect("prefix");
        let listed = list_all(memory.as_ref(), &shard_prefix)
            .await
            .expect("list shard");
        assert_eq!(listed.len(), 3);

        let opened_at = 500_001 * NS_PER_HOUR;
        let clock = ravel_maintain::FixedClock::new(opened_at);
        let metrics = ScrubMetrics::default();
        for _ in 0..2 {
            run_shard_tick(
                memory.as_ref(),
                &clock,
                &tenant_hash,
                Signal::Metrics,
                0,
                3,
                1,
                None,
                None,
                &metrics,
            )
            .await;
        }
        let before = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            before.last_commit_key.as_deref(),
            Some(listed[1].key.as_str())
        );
        assert_eq!(before.rotation_entries_visited, 2);

        let faulted = Arc::new(FaultStore::new(
            memory.clone(),
            FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Get,
                    ScriptedFault::Transient("scrub: injected cursor GET fault".to_string()),
                )
                .with_key_contains(".cursor"),
            ),
        ));
        // A later clock, so a tick that wrongly opened a fresh rotation would
        // stamp this time on the stored cursor.
        let later = ravel_maintain::FixedClock::new(500_002 * NS_PER_HOUR);
        run_shard_tick(
            faulted.as_ref(),
            &later,
            &tenant_hash,
            Signal::Metrics,
            0,
            3,
            1,
            None,
            None,
            &metrics,
        )
        .await;
        assert_eq!(
            faulted.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the injected fault must have fired exactly once"
        );

        let after = load_cursor(memory.as_ref(), &tenant_hash, Signal::Metrics, 0, 0)
            .await
            .expect("cursor loads");
        assert_eq!(
            after.last_commit_key.as_deref(),
            Some(listed[1].key.as_str()),
            "the stored marker must not move when the cursor could not be read"
        );
        assert_eq!(after.rotation_entries_visited, 2);
        assert_eq!(after.rotation_total_entries, Some(3));
        assert_eq!(
            after.rotation_started_unix_ns, opened_at,
            "the rotation must not restart on a transient cursor read failure"
        );
    }

    /// ADR-1686 decision 3 and its amendment: records committed mid-rotation
    /// are covered by the rotation that is running only when they sort after
    /// its marker. One lands ahead of the marker and one behind it; the
    /// rotation in flight verifies every object from its marker on, including
    /// the one that arrived ahead of it, and the record that landed behind the
    /// marker waits for the next rotation, which then verifies all six.
    #[tokio::test]
    async fn records_committed_mid_rotation_split_across_the_marker() {
        let memory = Arc::new(MemoryStore::new());
        let tenant_hash = tenant().hash();
        // `publish_segment` derives the writer id from the sequence number, and
        // a commit record key leads with it, so a higher sequence sorts later.
        let mut data_keys: Vec<(u64, String)> = Vec::new();
        for seq in 2..=5u64 {
            data_keys.push((seq, publish_segment(&memory, seq, &["cpu"]).await));
        }

        let store = FullGetLog {
            inner: memory.clone(),
            full_gets: parking_lot::Mutex::new(Vec::new()),
        };
        let clock = ravel_maintain::FixedClock::new(500_003 * NS_PER_HOUR);
        let metrics = ScrubMetrics::default();
        async fn tick(
            store: &FullGetLog,
            memory: &MemoryStore,
            clock: &ravel_maintain::FixedClock,
            tenant_hash: &TenantHash,
            metrics: &ScrubMetrics,
        ) -> ScrubCursor {
            run_shard_tick(
                store,
                clock,
                tenant_hash,
                Signal::Metrics,
                0,
                5,
                1,
                None,
                None,
                metrics,
            )
            .await;
            load_cursor(memory, tenant_hash, Signal::Metrics, 0, 0)
                .await
                .expect("cursor loads")
        }

        // Two ticks of the four-entry rotation, one entry each.
        for expected in 1..=2u64 {
            let cursor = tick(&store, memory.as_ref(), &clock, &tenant_hash, &metrics).await;
            assert_eq!(cursor.rotation_entries_visited, expected);
            assert_eq!(cursor.rotation_total_entries, Some(4));
        }

        // Mid-rotation commits: sequence 6 sorts after the marker (and after
        // the rotation's tail), sequence 1 before it.
        data_keys.push((6, publish_segment(&memory, 6, &["cpu"]).await));
        data_keys.push((1, publish_segment(&memory, 1, &["cpu"]).await));
        let key_for = |seq: u64| -> String {
            data_keys
                .iter()
                .find(|(s, _)| *s == seq)
                .map(|(_, key)| key.clone())
                .expect("published sequence")
        };

        let cursor = tick(&store, memory.as_ref(), &clock, &tenant_hash, &metrics).await;
        // Both records land in the hour the tail window covers, so both are
        // counted, the one behind the marker included: the estimate can only
        // overstate what is left to walk. Six entries over a five-tick
        // rotation lift the budget to two a tick.
        assert_eq!(
            cursor.rotation_appended_entries, 2,
            "every record committed into the tail window is an append"
        );
        assert_eq!(cursor.rotation_entries_visited, 4);

        let mut ticks = 3;
        loop {
            let cursor = tick(&store, memory.as_ref(), &clock, &tenant_hash, &metrics).await;
            ticks += 1;
            if cursor.last_commit_key.is_none() {
                break;
            }
            assert!(ticks < 10, "the rotation must finish");
        }
        assert_eq!(ticks, 4, "two one-entry ticks, then two entries and one");

        let mut first_rotation: Vec<String> = store
            .full_gets
            .lock()
            .iter()
            .filter(|key| !key.contains("/c/") && !key.contains("/maint/"))
            .cloned()
            .collect();
        first_rotation.sort();
        let mut expected: Vec<String> = (2..=6u64).map(key_for).collect();
        expected.sort();
        assert_eq!(
            first_rotation, expected,
            "the rotation in flight verifies every record from its marker on, the one \
             committed ahead of the marker included, and not the one committed behind it"
        );

        store.full_gets.lock().clear();
        let mut ticks = 0;
        loop {
            let cursor = tick(&store, memory.as_ref(), &clock, &tenant_hash, &metrics).await;
            ticks += 1;
            if cursor.last_commit_key.is_none() {
                break;
            }
            assert!(ticks < 10, "the next rotation must finish");
        }
        assert_eq!(ticks, 3, "six entries at two a tick");

        let mut second_rotation: Vec<String> = store
            .full_gets
            .lock()
            .iter()
            .filter(|key| !key.contains("/c/") && !key.contains("/maint/"))
            .cloned()
            .collect();
        second_rotation.sort();
        let mut expected: Vec<String> = (1..=6u64).map(key_for).collect();
        expected.sort();
        assert_eq!(
            second_rotation, expected,
            "the next rotation verifies all six, the record that landed behind the old \
             marker included"
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            0
        );
    }
}
