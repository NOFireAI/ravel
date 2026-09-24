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
//! 1. LIST the shard's commit, compaction, rewrite, and tombstone records.
//!    Decode each commit record and reconstruct the data object key it points
//!    at; decode each compaction/rewrite record, drop the tombstoned ones,
//!    the superseded ones, and the overlap losers, and reconstruct the
//!    survivors' parts' keys the same way. Together these build the rotation corpus ([`ScrubTarget`]s in key
//!    order) and, beside it, the per-key map holding the record to verify and
//!    its [`ravel_maintain::ScrubLevel`]. The level lives in that map alone,
//!    so the label a mismatch is counted under has one owner.
//! 2. Load this shard's persisted [`ScrubCursor`], size a per-tick byte budget
//!    from the corpus size and the configured scrub period `P`
//!    ([`per_tick_byte_budget`]), and [`advance_cursor`] to pick the bounded
//!    slice of objects to verify this tick.
//! 3. Verify each object in the slice via
//!    [`scrub_one_object`](ravel_maintain::scrub_one_object) and record any
//!    anomaly on the metrics counters below.
//! 4. Persist the advanced cursor so the next tick resumes where this one
//!    stopped, completing a full rotation over the corpus in about `P`.
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
//!   lost: [`advance_cursor`]'s past-tail wrap re-covers every slice next
//!   rotation. The effect is purely a promptness cost -- it DELAYS detection of
//!   a corruption in that one slice by at most one full rotation period (the
//!   scrub period `P`, [`DEFAULT_SCRUB_PERIOD`] = 7 days at defaults).
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
//! segment from, ADR-0058). A [`ScrubResult::ReadError`] is a transient store
//! or decode failure, not corruption: it is logged and retried on a later tick,
//! never counted as an anomaly.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_commit::keys;
use ravel_ingest::{Clock as _, SystemClock};
use ravel_maintain::{
    Clock, ScrubLevel, ScrubResult, ScrubTarget, WorkerSet, advance_cursor, per_tick_byte_budget,
    scrub_one_object,
};
use ravel_maintain::{ScrubBudget, ScrubCursor};
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError, list_all};
use ravel_types::{Signal, TenantHash};
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

/// Process-global counters for the scrubber (ADR-0059 decision 3), rendered on
/// the existing `GET /metrics` endpoint by
/// [`crate::metrics::render_scrub_family`] with no second registry, following
/// [`crate::maintain::MaintenanceSafetyMetrics`]'s conventions exactly:
/// per-signal counters indexed by [`signal_index`], and deliberately no
/// `tenant_hash` label (ADR-0044 section 4 blocks per-tenant series on the
/// unauthenticated `/metrics` route).
///
/// Structural corruption and a content-hash mismatch are both at-rest integrity
/// failures of the same data object, so both increment
/// `checksum_mismatch`; a [`ScrubResult::ReadError`] is transient and increments
/// nothing. `checksum_mismatch` also carries a [`ScrubLevel`] dimension
/// (`l0`/`l1`/`rewrite`, [`level_index`]): the postings tier only ever runs
/// against L0 objects, so `postings_disagreement` stays signal-only.
///
/// Seal divergence (ADR-0059 decision 2) is a distinct, metadata-cost
/// check on the same tick: sealed commit records re-listed and diffed against the
/// folded snapshot. `missing` and `mismatched` divergences increment
/// `seal_divergence_*`; `orphaned` is the expected retention-after-fold shape and
/// increments nothing.
#[derive(Debug, Default)]
pub struct ScrubMetrics {
    checksum_mismatch: [[AtomicU64; SCRUB_LEVELS]; MAINTAINED_SIGNALS.len()],
    postings_disagreement: [AtomicU64; MAINTAINED_SIGNALS.len()],
    /// Sealed commit records absent from the folded snapshot (an under-count),
    /// per signal. The `reason="missing"` value of
    /// `ravel_scrub_seal_divergence_total`.
    seal_divergence_missing: [AtomicU64; MAINTAINED_SIGNALS.len()],
    /// Snapshot entries whose `content_hash` disagrees with the sealed commit
    /// record, per signal. The `reason="mismatched"` value of
    /// `ravel_scrub_seal_divergence_total`.
    seal_divergence_mismatched: [AtomicU64; MAINTAINED_SIGNALS.len()],
    /// Objects covered so far in the current rotation, per signal (numerator of
    /// the cursor-position gauge). Last-observed value, overwritten each shard
    /// tick, matching the "most recent pass" gauge discipline `orphans_withheld`
    /// keeps in [`crate::maintain`].
    rotation_covered: [AtomicU64; MAINTAINED_SIGNALS.len()],
    /// Total objects in the corpus at the current rotation's start, per signal
    /// (denominator of the cursor-position gauge).
    rotation_total: [AtomicU64; MAINTAINED_SIGNALS.len()],
}

impl ScrubMetrics {
    pub fn checksum_mismatch(&self, signal: Signal, level: ScrubLevel) -> u64 {
        self.checksum_mismatch[signal_index(signal)][level_index(level)].load(Ordering::Relaxed)
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

    /// Fraction of the current rotation covered so far for `signal`, in
    /// `[0.0, 1.0]` (ADR-0059's `ravel_scrub_cursor_position` gauge). Derived
    /// from the objects-covered / corpus-total pair the last shard tick
    /// recorded; `0.0` when the corpus is empty (nothing to rotate over).
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

    fn record_postings_disagreement(&self, signal: Signal) {
        self.postings_disagreement[signal_index(signal)].fetch_add(1, Ordering::Relaxed);
    }

    fn record_seal_divergence_missing(&self, signal: Signal, count: u64) {
        self.seal_divergence_missing[signal_index(signal)].fetch_add(count, Ordering::Relaxed);
    }

    fn record_seal_divergence_mismatched(&self, signal: Signal, count: u64) {
        self.seal_divergence_mismatched[signal_index(signal)].fetch_add(count, Ordering::Relaxed);
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
pub fn spawn(
    store: Arc<dyn ObjectStoreBackend>,
    restrict: Vec<TenantHash>,
    period: Duration,
    shard_count: u32,
    metrics: Arc<ScrubMetrics>,
    worker: Arc<WorkerSet>,
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

/// One content-tier tick over one `(tenant, signal, shard)`: build the rotation
/// corpus from the shard's L0 commit records plus the parts of every
/// compaction and rewrite record that survives the lineage filter the module
/// doc describes, advance this shard's persisted cursor by one budgeted slice,
/// verify each object in the slice, and persist the advanced cursor. Every
/// store error is logged and the tick is retried next cycle; nothing here
/// mutates durable data.
#[allow(clippy::too_many_arguments)]
async fn run_shard_tick(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    period_secs: u64,
    tick_secs: u64,
    covering: Option<&ravel_catalog::LoadedCoveringPostings>,
    metrics: &ScrubMetrics,
) {
    // Build the corpus: every L0 data object referenced by a commit record for
    // this shard, and every part of a compaction or rewrite record that is not
    // superseded, not an overlap loser, and not in a tombstoned bucket, keyed
    // by object key so the cursor's key-ordered resume point is well defined.
    // The commit record (for an L0 object) or the `CompactionPart` (for a
    // part) carries the object's size (for the byte budget) and the content
    // hash `scrub_one_object` re-verifies against.
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
    let metas = match list_all(store, &prefix).await {
        Ok(metas) => metas,
        Err(err) => {
            tracing::warn!(
                tenant = %tenant.to_hex(), signal = ?signal, shard, error = %err,
                "scrub: LIST of commit records failed; retried next tick"
            );
            return;
        }
    };

    // Retention's physical sweep deletes every object in a tombstoned bucket
    // (L0 commit records, L1 parts, rewrite parts) as one unit, but does not
    // do so atomically with the LIST above: a compaction/rewrite record can
    // still be present in `metas` for an hour whose tombstone has already
    // landed. Collect tombstoned hours first (filename-only classification,
    // no GETs) so the second pass can skip their compaction/rewrite records
    // rather than racing a sweep that may delete their parts mid-tick.
    let mut tombstoned_hours: std::collections::HashSet<u32> = std::collections::HashSet::new();
    for meta in &metas {
        if let Ok(keys::BucketEntry::Tombstone(parsed)) = keys::partition_bucket_entry(&meta.key) {
            tombstoned_hours.insert(parsed.ingest_hour_bucket);
        }
    }

    let mut corpus: Vec<ScrubTarget> = Vec::new();
    let mut records: std::collections::HashMap<
        String,
        (ravel_proto::commit::v1::CommitRecord, ScrubLevel),
    > = std::collections::HashMap::new();
    // Compaction and rewrite records are decoded in the pass below but their
    // parts are not expanded there: a bucket can hold a superseded generation
    // alongside the live one until a horizon-gated sweep retires it, and can
    // hold two compaction records whose input sets overlap, and only the full
    // listing says which record of each pair the read path serves. Buffer
    // them, resolve both exclusions once, then expand. Each record is still
    // fetched exactly once.
    let mut compaction_records: Vec<(String, ravel_proto::commit::v1::CompactionRecord)> =
        Vec::new();
    let mut rewrite_records: Vec<(String, ravel_proto::commit::v1::RewriteRecord)> = Vec::new();
    for meta in &metas {
        // L0 commit records, and the L1/rewrite parts a compaction or
        // erasure-rewrite record supersedes them with, all name objects
        // `scrub_one_object` can verify (its API is commit-record based; L1
        // and rewrite parts are wrapped in a synthetic record built from
        // their own part fields). Tombstone records carry no object of their
        // own. Listed explicitly so a new bucket-entry shape fails to
        // compile here rather than being silently swallowed.
        match keys::partition_bucket_entry(&meta.key) {
            Ok(keys::BucketEntry::CommitRecord(_)) => {
                let got = match store.get(&meta.key, GetRange::Full).await {
                    Ok(got) => got,
                    Err(err) => {
                        tracing::warn!(
                            key = %meta.key, error = %err,
                            "scrub: commit record GET failed; skipping this object this tick"
                        );
                        continue;
                    }
                };
                let record = match ravel_commit::record::decode(&got.data) {
                    Ok(record) => record,
                    Err(err) => {
                        tracing::warn!(
                            key = %meta.key, error = %err,
                            "scrub: commit record decode failed; skipping this object this tick"
                        );
                        continue;
                    }
                };
                let data_key = match keys::reconstruct_data_key(&record) {
                    Ok(key) => key,
                    Err(err) => {
                        tracing::warn!(
                            key = %meta.key, error = %err,
                            "scrub: could not reconstruct data key; skipping this object this tick"
                        );
                        continue;
                    }
                };
                corpus.push(ScrubTarget {
                    object_key: data_key.clone(),
                    object_size: record.object_size,
                });
                records.insert(data_key, (record, ScrubLevel::L0));
            }
            Ok(keys::BucketEntry::CompactionRecord(parsed)) => {
                if tombstoned_hours.contains(&parsed.ingest_hour_bucket) {
                    continue;
                }
                let got = match store.get(&meta.key, GetRange::Full).await {
                    Ok(got) => got,
                    Err(err) => {
                        tracing::warn!(
                            key = %meta.key, error = %err,
                            "scrub: compaction record GET failed; skipping this tick"
                        );
                        continue;
                    }
                };
                let rec = match ravel_commit::record::decode_compaction(&got.data) {
                    Ok(rec) => rec,
                    Err(err) => {
                        tracing::warn!(
                            key = %meta.key, error = %err,
                            "scrub: compaction record decode failed; skipping this tick"
                        );
                        continue;
                    }
                };
                compaction_records.push((meta.key.clone(), rec));
            }
            Ok(keys::BucketEntry::RewriteRecord(parsed)) => {
                if tombstoned_hours.contains(&parsed.ingest_hour_bucket) {
                    continue;
                }
                let got = match store.get(&meta.key, GetRange::Full).await {
                    Ok(got) => got,
                    Err(err) => {
                        tracing::warn!(
                            key = %meta.key, error = %err,
                            "scrub: rewrite record GET failed; skipping this tick"
                        );
                        continue;
                    }
                };
                let rec = match ravel_commit::erasure::decode_rewrite(&got.data) {
                    Ok(rec) => rec,
                    Err(err) => {
                        tracing::warn!(
                            key = %meta.key, error = %err,
                            "scrub: rewrite record decode failed; skipping this tick"
                        );
                        continue;
                    }
                };
                rewrite_records.push((meta.key.clone(), rec));
            }
            Ok(keys::BucketEntry::Tombstone(_)) => continue,
            Err(err) => {
                tracing::warn!(
                    key = %meta.key, error = %err,
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
    // fatal; the scrub keeps whatever survives the filter and the next tick
    // tries again.
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
        if superseded.contains(record_key.as_str())
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
            corpus.push(ScrubTarget {
                object_key: part_key.clone(),
                object_size: part.object_size,
            });
            records.insert(part_key, (synthetic, ScrubLevel::L1));
        }
    }

    for (record_key, rec) in &rewrite_records {
        if superseded.contains(record_key.as_str()) {
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
            corpus.push(ScrubTarget {
                object_key: part_key.clone(),
                object_size: part.object_size,
            });
            records.insert(part_key, (synthetic, ScrubLevel::Rewrite));
        }
    }

    // `advance_cursor` requires the corpus in key order (as a strongly
    // consistent LIST returns it); the corpus was built from commit records,
    // not the data-object listing, so sort explicitly.
    corpus.sort_by(|a, b| a.object_key.cmp(&b.object_key));

    let cursor = load_cursor(store, tenant, signal, shard, clock.now_ns()).await;
    let total_bytes: u64 = corpus.iter().map(|t| t.object_size).sum();
    let budget: ScrubBudget = per_tick_byte_budget(total_bytes, period_secs, tick_secs);
    let slice = advance_cursor(&cursor, &corpus, budget, clock.now_ns());

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

    for key in &slice.scrub_keys {
        let Some((record, level)) = records.get(key) else {
            continue;
        };
        let level = *level;
        // The structural + content tiers always run (footer crc re-verify, then
        // whole-object blake3 vs the recorded content hash). The postings tier
        // runs additionally when `covering_postings` is `Some`: the object's
        // true `__name__` set is re-derived and diffed against what the covering
        // postings object claims for it (the false-negative check). Postings
        // only ever cover L0 commit records (an L1/rewrite part's covering
        // ordinal is not meaningfully defined), so L1 and rewrite targets never
        // get the postings tier regardless of whether it loaded this tick.
        let postings_for_object = if level == ScrubLevel::L0 {
            covering_postings
        } else {
            None
        };
        match scrub_one_object(store, clock, record, postings_for_object).await {
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
            ScrubResult::PostingsDisagreement { name, ordinal } => {
                tracing::error!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, object_key = %key,
                    name = %name, ordinal,
                    "scrub: postings disagreement (false negative)"
                );
                metrics.record_postings_disagreement(signal);
            }
            ScrubResult::ReadError { detail } => {
                // Transient store/decode failure: retried next tick, never an
                // anomaly (a throttle or timeout must not read as bit rot).
                tracing::warn!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, object_key = %key,
                    detail = %detail,
                    "scrub: transient read error; retried next tick"
                );
            }
        }
    }

    // Cursor-position gauge: objects covered so far in this rotation over the
    // corpus total at rotation start. A completed rotation reads as full
    // coverage before wrapping.
    let covered = if slice.rotation_complete {
        corpus.len()
    } else {
        match &slice.next_cursor.last_object_key {
            Some(last) => corpus.partition_point(|t| &t.object_key <= last),
            None => 0,
        }
    };
    metrics.record_cursor_position(signal, covered as u64, corpus.len() as u64);

    persist_cursor(store, tenant, signal, shard, &slice.next_cursor).await;
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

/// The on-disk cursor: only the rotation-relative position needs persisting;
/// tenant/signal/shard are known from the key context on load.
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedCursor {
    last_object_key: Option<String>,
    rotation_started_unix_ns: i64,
}

/// Load this shard's persisted cursor, or a fresh one at the start of a
/// rotation when none exists yet or it fails to decode (a decode failure is
/// treated as "start over," never an anomaly: the cursor is advisory scheduling
/// state, not durable data).
async fn load_cursor(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    shard: u32,
    now_ns: i64,
) -> ScrubCursor {
    let key = cursor_key(tenant, signal, shard);
    match store.get(&key, GetRange::Full).await {
        Ok(got) => match serde_json::from_slice::<PersistedCursor>(&got.data) {
            Ok(persisted) => ScrubCursor {
                tenant_hash: *tenant,
                signal,
                shard,
                last_object_key: persisted.last_object_key,
                rotation_started_unix_ns: persisted.rotation_started_unix_ns,
            },
            Err(err) => {
                tracing::warn!(
                    key = %key, error = %err,
                    "scrub: cursor decode failed; starting a fresh rotation for this shard"
                );
                ScrubCursor::new(*tenant, signal, shard, now_ns)
            }
        },
        Err(StoreError::NotFound) => ScrubCursor::new(*tenant, signal, shard, now_ns),
        Err(err) => {
            tracing::warn!(
                key = %key, error = %err,
                "scrub: cursor GET failed; starting a fresh rotation for this shard this tick"
            );
            ScrubCursor::new(*tenant, signal, shard, now_ns)
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
        last_object_key: cursor.last_object_key.clone(),
        rotation_started_unix_ns: cursor.rotation_started_unix_ns,
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
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
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
        let tenant_id = tenant();
        let tenant_hash = tenant_id.hash();
        let writer_id = Uuid::from_u128(u128::from(1_000 + seq));
        let created_unix_ns = 500_000 * NS_PER_HOUR;
        let ingest_hour_bucket = 500_000u32;
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

    /// A full tick over a clean corpus: discovery -> LIST -> load cursor ->
    /// advance -> scrub -> persist cursor. No anomaly is recorded, and the
    /// per-shard cursor is persisted so a later tick resumes from it.
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
        assert_eq!(cursor.last_object_key, None);
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
        )
        .await;

        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics, ScrubLevel::L0),
            1,
            "the injected bit flip must surface as a scrub checksum mismatch"
        );
    }

    /// The cursor advances across ticks under a byte budget that admits one
    /// object per tick: two ticks cover a two-object corpus, and the mid-
    /// rotation cursor-position gauge reads a partial fraction.
    #[tokio::test]
    async fn cursor_advances_across_ticks_under_a_tight_budget() {
        let store = MemoryStore::new();
        publish_segment(&store, 1, &["cpu"]).await;
        publish_segment(&store, 2, &["mem"]).await;

        let metrics = ScrubMetrics::default();
        let worker = solo_worker();
        // period (100s) >> tick (1s): the per-tick byte budget covers only a
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
            )
            .await;
            let tenant_hash = tenant().hash();
            let key = cursor_key(&tenant_hash, Signal::Metrics, 0);
            let got = store.get(&key, GetRange::Full).await.expect("cursor");
            let cursor: PersistedCursor = serde_json::from_slice(&got.data).expect("decode");
            if cursor.last_object_key.is_none() {
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
}
