//! Background at-rest integrity scrubber task (ADR-0059 decisions 1, 3, 4,
//! issue #694). The scheduling half of the durability-hardening scrubber: the
//! per-object verification logic lives in [`ravel_maintain::scrub`] (part 1 of
//! this issue) and is unit-tested there; this module is the periodic loop that
//! drives it over a real object corpus, exactly the mechanism/lifecycle split
//! every other background loop in this crate uses ([`crate::maintain`],
//! [`crate::admission_reconcile`]).
//!
//! # One tick
//!
//! Each tick re-discovers the tenant set from storage (the same
//! [`crate::tenant_discovery::discover_and_restrict`] the maintenance
//! supervisor uses) and, for every `(tenant, signal, shard)` it holds data
//! for, runs the content tier over the shard's committed L0 data objects:
//!
//! 1. LIST the shard's commit records, decode each, and reconstruct the data
//!    object key it points at, building the rotation corpus
//!    ([`ScrubTarget`]s keyed by object key, in key order).
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
//! read bandwidth on a process whose job is the hot path, and the persisted
//! per-shard cursor assumes a single writer, which the Maintain-only gating
//! guarantees (one maintain process per deployment). So this task is spawned in
//! exactly the same mode the maintenance loop is, and nowhere else.
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
    Clock, ScrubResult, ScrubTarget, advance_cursor, per_tick_byte_budget, scrub_one_object,
};
use ravel_maintain::{ScrubBudget, ScrubCursor};
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError, list_all};
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

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
/// nothing.
///
/// Seal divergence (ADR-0059 decision 2, issue #695) is a distinct, metadata-cost
/// check on the same tick: sealed commit records re-listed and diffed against the
/// folded snapshot. `missing` and `mismatched` divergences increment
/// `seal_divergence_*`; `orphaned` is the expected retention-after-fold shape and
/// increments nothing.
#[derive(Debug, Default)]
pub struct ScrubMetrics {
    checksum_mismatch: [AtomicU64; MAINTAINED_SIGNALS.len()],
    postings_disagreement: [AtomicU64; MAINTAINED_SIGNALS.len()],
    /// Sealed commit records absent from the folded snapshot (S2-04 under-count),
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
    pub fn checksum_mismatch(&self, signal: Signal) -> u64 {
        self.checksum_mismatch[signal_index(signal)].load(Ordering::Relaxed)
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

    fn record_checksum_mismatch(&self, signal: Signal) {
        self.checksum_mismatch[signal_index(signal)].fetch_add(1, Ordering::Relaxed);
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
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(jittered(tick)) => {}
                _ = &mut rx => return,
            }
            run_cycle(
                store.as_ref(),
                restrict.as_deref(),
                shard_count,
                period_secs,
                tick_secs,
                metrics.as_ref(),
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
pub async fn run_cycle(
    store: &dyn ObjectStoreBackend,
    restrict: Option<&[TenantHash]>,
    shard_count: u32,
    period_secs: u64,
    tick_secs: u64,
    metrics: &ScrubMetrics,
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
            // signal) per tick (issue #708): postings cover a whole snapshot
            // HEAD (one `head.postings` bound to every part), not one per shard,
            // so it is loaded here, before the shard loop, and the owned data is
            // threaded by reference into every shard's tick. A load failure or
            // an absent postings ref yields `None`, which every shard's tick
            // treats as "no postings tier this tick" exactly as before this
            // wiring landed (the documented "no postings ref yet" case,
            // ADR-0059). A `tenant_hash`-binding breach (ADR-0050 §2) is logged
            // and likewise degrades to `None` for this tick, never wedging the
            // content and structural tiers that do not depend on postings.
            let covering = match ravel_catalog::load_covering_postings(store, tenant, signal).await
            {
                Ok(loaded) => loaded,
                Err(err) => {
                    tracing::warn!(
                        tenant = %tenant.to_hex(), signal = ?signal, error = %err,
                        "scrub: covering-postings load failed; postings tier skipped this tick, retried"
                    );
                    None
                }
            };
            let scan_shards = scan_shards(store, tenant, signal, shard_count).await;
            for shard in 0..scan_shards {
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
            // Seal-divergence tier (ADR-0059 decision 2, issue #695): once per
            // (tenant, signal) per tick, not per shard and not gated behind the
            // content-tier cursor. It is metadata-cost, matching the structural
            // tier's cost class rather than the content tier's corpus-scan
            // budget, and `verify_seal_divergence` re-lists every shard itself.
            run_seal_divergence_tick(store, tenant, signal, metrics).await;
        }
    }
}

/// One seal-divergence tick over one `(tenant, signal)` (ADR-0059 decision 2,
/// issue #695): re-list the sealed commit records and diff them against the
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
/// corpus from the shard's L0 commit records, advance this shard's persisted
/// cursor by one budgeted slice, verify each object in the slice, and persist
/// the advanced cursor. Every store error is logged and the tick is retried
/// next cycle; nothing here mutates durable data.
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
    // Build the corpus: every L0 data object referenced by a surviving commit
    // record for this shard, keyed by data-object key so the cursor's
    // key-ordered resume point is well defined. The commit record carries the
    // object's size (for the byte budget) and the content hash
    // `scrub_one_object` re-verifies against.
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

    let mut corpus: Vec<ScrubTarget> = Vec::new();
    let mut records: std::collections::HashMap<String, ravel_proto::commit::v1::CommitRecord> =
        std::collections::HashMap::new();
    for meta in &metas {
        // Only L0 commit records name an object `scrub_one_object` can verify
        // (its API is commit-record based). Compaction records and tombstones
        // are skipped here; L1 part verification is not part of this wiring.
        match keys::partition_bucket_entry(&meta.key) {
            Ok(keys::BucketEntry::CommitRecord(_)) => {}
            Ok(_) => continue,
            Err(err) => {
                tracing::warn!(
                    key = %meta.key, error = %err,
                    "scrub: unrecognized bucket entry shape; skipping"
                );
                continue;
            }
        }
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
        records.insert(data_key, record);
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
    // the owned data loaded per (tenant, signal) in `run_cycle` (issue #708).
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
        let Some(record) = records.get(key) else {
            continue;
        };
        // The structural + content tiers always run (footer crc re-verify, then
        // whole-object blake3 vs the recorded content hash). The postings tier
        // runs additionally when `covering_postings` is `Some`: the object's
        // true `__name__` set is re-derived and diffed against what the covering
        // postings object claims for it (S2-09 false-negative check).
        match scrub_one_object(store, clock, record, covering_postings).await {
            ScrubResult::Clean => {}
            ScrubResult::ChecksumMismatch { .. } => {
                tracing::error!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, object_key = %key,
                    "scrub: content-hash mismatch at rest (bit rot or partial write)"
                );
                metrics.record_checksum_mismatch(signal);
            }
            ScrubResult::StructuralCorruption { detail } => {
                tracing::error!(
                    tenant = %tenant.to_hex(), signal = ?signal, shard, object_key = %key,
                    detail = %detail,
                    "scrub: structural corruption at rest (footer/section crc)"
                );
                metrics.record_checksum_mismatch(signal);
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
/// cursor. Single writer per shard, guaranteed by the Maintain-mode gating.
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

/// Persist the advanced cursor (overwrite; single writer per shard). A write
/// failure is logged and left for the next tick to retry: the worst case is one
/// shard re-scrubbing the same slice, never a correctness problem.
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
        // period == tick so the whole corpus is one slice: a full rotation in
        // one tick.
        run_cycle(&store, None, 1, 1, 1, &metrics).await;

        assert_eq!(metrics.checksum_mismatch(Signal::Metrics), 0);
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
        run_cycle(&store, None, 1, 1, 1, &metrics).await;

        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics),
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
        // period (100s) >> tick (1s): the per-tick byte budget covers only a
        // fraction of the corpus, so a rotation takes multiple ticks.
        run_cycle(&store, None, 1, 100, 1, &metrics).await;
        let after_first = metrics.cursor_position(Signal::Metrics);
        assert!(
            after_first > 0.0 && after_first < 1.0,
            "a mid-rotation tick covers part of the corpus, got {after_first}"
        );

        // Keep ticking until the rotation completes and wraps.
        for _ in 0..10 {
            run_cycle(&store, None, 1, 100, 1, &metrics).await;
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
        run_cycle(&store, None, 4, 1, 1, &metrics).await;
        assert_eq!(metrics.checksum_mismatch(Signal::Metrics), 0);
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
        run_cycle(store.as_ref(), None, 1, 1, 1, &metrics).await;

        assert_eq!(metrics.checksum_mismatch(Signal::Metrics), 0);
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
    /// segment's true name set and records the S2-09 false negative. This is the
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
        run_cycle(store.as_ref(), None, 1, 1, 1, &metrics).await;

        assert_eq!(
            metrics.postings_disagreement(Signal::Metrics),
            1,
            "the omitted name must surface as a postings disagreement through the real tick"
        );
        assert_eq!(
            metrics.checksum_mismatch(Signal::Metrics),
            0,
            "the segment data is untouched: no checksum mismatch"
        );
    }
}
