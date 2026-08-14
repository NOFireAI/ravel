//! Driver (ADR-0068 deliverables 4/5, issue #818 deliverable 2/3): one seeded
//! ingest -> fold -> compact -> sweep -> query cycle over `MemoryStore`, wired
//! through the same `IngestRouter` -> `Catalog` -> `QueryEngine` path
//! production traffic takes and driving `ravel-maintain`'s real compaction and
//! sweep entry points, all under injected faults from a seed-derived
//! [`FaultSchedule`], plus the invariant checks from deliverable 5.
//!
//! Runs on a single-threaded, paused-clock tokio runtime
//! (`current_thread`/`start_paused`) that this module owns end to end:
//! `SimClock::sleep` delegates to the runtime's virtual timer (the
//! `Clock::sleep` default impl), so every await on it fast-forwards
//! instantly instead of costing wall-clock time, and the whole cycle stays
//! reproducible because nothing here ever reads a real clock.
//!
//! # Phases and their clocks
//!
//! Ingest and fold run on the injected [`SimClock`] as before. Compaction and
//! sweep are then driven at explicit, seal- and horizon-satisfying instants
//! via `ravel-maintain`'s injected `FixedClock`: compaction at a time past
//! every written bucket's seal margin, sweep at a time past the protection
//! horizon and every GC age gate, so the whole cycle -- including the physical
//! deletion of the compacted L0 inputs -- runs deterministically in one pass.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use rand::RngExt;
use rand::rngs::StdRng;
use ravel_catalog::{
    Catalog, CatalogConfig, CatalogError, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_FOLD_SAFETY_MARGIN_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::rng::{RngSource, SeededRng};
use ravel_ingest::{
    Clock, IngestConfig, IngestPoint, IngestRouter, IngestValue, WriteError, WriteMode,
};
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, MaintainError, NoLeases,
    compact_bucket, sweep_shard,
};
use ravel_object_store::fault::{FaultKind, FaultStore, GateHandle, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_promql::Value;
use ravel_query::{EngineConfig, QueryEngine, QueryError};
use ravel_segment::HistogramCounts;
use ravel_types::{CommitToken, METRIC_NAME_LABEL, Signal, TenantHash, TimeRange, TypeError};
use uuid::Uuid;

use crate::digest::{Digest, DigestBuilder, mix_value};
use crate::fault_plan::{FaultSchedule, FaultScheduleConfig};
use crate::seed::MasterSeed;
use crate::workload::{QuerySpec, SeriesSamples, Workload, WorkloadConfig};

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Number of idempotent re-runs the compaction/sweep recovery loop will make
/// before giving up and surfacing the typed error (issue #931 deliverable 2).
/// A single generated compaction-phase fault fires once (`Occurrence::Nth(1)`)
/// and the compaction path issues at most three distinct faultable steps in
/// one bucket (list, L0 read, L1 write), so a budget of six comfortably
/// absorbs every generated schedule while still bounding a genuinely stuck
/// fault (`Occurrence::Always`, only reachable via a test override) into a
/// loud typed error rather than an infinite loop.
const COMPACTION_FAULT_RETRY_BUDGET: usize = 6;

/// A shared handle to an [`ObjectStoreBackend`] that is itself a backend, so a
/// second [`FaultStore`] can wrap the same underlying store the ingest phase
/// writes through. The compaction/sweep fault plans (issue #931) live on
/// [`FaultStore`]s the driver builds only around `compact_bucket` and the
/// sweep's action pass; each wraps a `SharedStore` cloned from the one
/// `Arc<dyn ObjectStoreBackend>` every phase shares, so their rules govern only
/// the calls those two entry points make and every write stays visible to
/// every phase. `MemoryStore` is not `Clone`, so this delegating newtype is
/// how a phase-local fault layer reaches the one backing store.
struct SharedStore(Arc<dyn ObjectStoreBackend>);

#[async_trait::async_trait]
impl ObjectStoreBackend for SharedStore {
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

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
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
}

/// Whether a [`MaintainError`] is one the driver's idempotent compaction/sweep
/// re-run can recover from (issue #931 deliverable 2): a retryable store fault
/// (`Transient`/`Throttled`/`Timeout`) or a not-found blip. `NotFound` is not
/// `StoreError::is_retryable` in general -- a genuine absence is a definite
/// answer -- but the only `NotFound` this harness injects on the compaction
/// read path is a one-shot eventual-consistency blip (`ScriptedFault::NotFoundBlip`,
/// `Occurrence::Nth(1)`), and re-running the stateless, idempotent
/// `compact_bucket` reads the object that is really there. Every other
/// `MaintainError` (an invariant breach, a decode failure, a conservation
/// violation) is surfaced immediately as a typed error, never retried.
fn is_recoverable_maintain_error(e: &MaintainError) -> bool {
    matches!(
        e,
        MaintainError::Store(se) if se.is_retryable() || matches!(se, StoreError::NotFound)
    )
}

/// Knobs for [`run_cycle`]. `shard_count` applies uniformly to both
/// `IngestRouter` and `Catalog` (they must agree: the shard a series routes
/// to at write time is the shard the catalog looks it up under), and to the
/// compaction/sweep drivers (they walk the same shards).
#[derive(Debug, Clone)]
pub struct CycleConfig {
    pub workload: WorkloadConfig,
    pub shard_count: u32,
    pub ack_deadline: Duration,
    pub query_deadline: Duration,
    /// Upper bound on the per-tenant scheduling jitter drawn from the
    /// `"clock"` sub-seed and slept (on the paused virtual clock) before
    /// that tenant's write, so the clock-stepping domain does something
    /// observable independent of the workload domain.
    pub max_jitter: Duration,
    /// Whether to wrap the object store in a seed-derived [`FaultStore`] and
    /// inject the generated fault schedule. Faults are retryable and land only
    /// on the ingest write path, so a clean cycle stays green; disabling this
    /// runs the cycle against a bare `MemoryStore`.
    pub inject_faults: bool,
    /// Config for the seed-derived fault-schedule generator (deliverable 1).
    pub fault_schedule: FaultScheduleConfig,
    /// Whether to arm the schedule's hold/release gates on the store. Off by
    /// default: a held call needs a releaser to make progress -- either the
    /// built-in one ([`GateRelease::Immediate`]) or a caller-driven one
    /// ([`GateRelease::Manual`]) -- so it is only meaningful under
    /// [`run_cycle`]'s own runtime.
    pub enable_gates: bool,
    /// How a held call gets released when `enable_gates` arms the schedule's
    /// gates. Ignored when `enable_gates` is false or the schedule armed no
    /// gates.
    pub gate_release: GateRelease,
    /// When set, bypasses both `inject_faults` and the seed-derived
    /// generator ([`crate::fault_plan::generate`]) and uses this
    /// [`FaultSchedule`] verbatim instead. `None` (the default) keeps the
    /// normal seed-derived plan and gates. This exists so a test can arm a
    /// gate with a matcher it knows the exact reachability of (a specific
    /// `Nth`, or one that can never match), rather than searching the
    /// generator's random output for a seed that happens to produce one --
    /// issue #878 deliverables 2 and 3.
    pub fault_schedule_override: Option<FaultSchedule>,
    /// Whether the driver's own tokio runtime pauses virtual time (the
    /// default) or runs on real wall-clock time. A paused clock makes every
    /// `SimClock` sleep and every `ack_deadline`/`query_deadline` timeout
    /// resolve instantly via tokio's auto-advance-on-idle, which is what
    /// keeps the normal seeded cycle fast and deterministic. That same
    /// auto-advance defeats an indefinitely held gate call once it sits
    /// inside a deadline-guarded wait (`IngestRouter::write_values`'s ack
    /// wait, for the `L0_DATA_SUBSTR` target every armed gate matches): the
    /// instant the runtime goes idle parked on the held call, tokio jumps
    /// straight past `ack_deadline` and the wait fails with a timeout
    /// instead of staying observably blocked. Set this to `false` only for
    /// a test that must prove a held call is still blocking the cycle
    /// (issue #878 deliverable 2) -- real wall-clock time then makes
    /// `ack_deadline` mean what it says, at the cost of the cycle no
    /// longer being instant.
    pub paused_clock: bool,
}

/// How [`run_cycle`] releases a call held by one of the schedule's
/// hold/release gates (ADR-0059 decision 5), when `CycleConfig::enable_gates`
/// is set.
#[derive(Clone, Default)]
pub enum GateRelease {
    /// Release each held call the instant it is held. Exercises the
    /// hold/release code path without ever deadlocking, but -- because
    /// release happens before anything outside the cycle can observe the
    /// held state -- proves nothing about the call having actually blocked.
    #[default]
    Immediate,
    /// Hand the armed gate to `hook` exactly once, right after arming, as a
    /// [`CountingGateHandle`], and release nothing automatically: the caller
    /// owns release timing and can observe the cycle genuinely blocked
    /// before choosing to release it. A hook that never releases every held
    /// call deadlocks the cycle by design -- this is a test-control seam,
    /// not a production mode.
    Manual(Arc<dyn Fn(CountingGateHandle) + Send + Sync>),
}

impl std::fmt::Debug for GateRelease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GateRelease::Immediate => write!(f, "GateRelease::Immediate"),
            GateRelease::Manual(_) => write!(f, "GateRelease::Manual(..)"),
        }
    }
}

impl Default for CycleConfig {
    fn default() -> Self {
        CycleConfig {
            workload: WorkloadConfig::default(),
            shard_count: 2,
            ack_deadline: Duration::from_secs(5),
            query_deadline: Duration::from_secs(5),
            max_jitter: Duration::from_millis(250),
            inject_faults: true,
            fault_schedule: FaultScheduleConfig::default(),
            enable_gates: false,
            gate_release: GateRelease::default(),
            fault_schedule_override: None,
            paused_clock: true,
        }
    }
}

/// A [`GateHandle`] wrapper that counts every call actually released through
/// it (issue #878 deliverable 1). `GateHandle` itself has no notion of
/// cumulative holds -- `held()`/`held_count()` only report calls currently
/// parked, which a released call immediately drops out of -- so a cycle has
/// no way to prove after the fact that a gate ever held anything. Routing
/// every release (the built-in [`GateRelease::Immediate`] releaser and any
/// [`GateRelease::Manual`] hook) through this wrapper's `release` gives
/// [`CycleOutcome::gates_held`] and the never-hit check in [`run_cycle`] a
/// single, always-accurate source for that count.
#[derive(Clone)]
pub struct CountingGateHandle {
    handle: GateHandle,
    held_total: Arc<AtomicUsize>,
    /// One matcher per armed gate, in the order the schedule armed them, so a
    /// released call can be attributed to the gate that held it. Attribution
    /// mirrors the store's own "first matching gate governs the call"
    /// precedence (see `GateRegistry::arm`): the first gate whose op and key
    /// substring match.
    gate_matchers: Arc<Vec<(Op, Option<String>)>>,
    /// One release counter per armed gate, index-aligned with `gate_matchers`.
    /// [`CycleError::GateNeverHit`] is now per-gate: every armed gate's counter
    /// must be positive, closing the vacuity where two gates shared one total
    /// and a reachable gate's releases masked an unreachable one (issue #931
    /// deliverable 4).
    per_gate_held: Arc<Vec<AtomicUsize>>,
}

impl CountingGateHandle {
    /// Waits until at least `n` calls are held simultaneously. See
    /// [`GateHandle::wait_until_held`].
    pub async fn wait_until_held(&self, n: usize) {
        self.handle.wait_until_held(n).await;
    }

    /// Ids of the calls currently held. See [`GateHandle::held`].
    pub fn held(&self) -> Vec<u64> {
        self.handle.held()
    }

    /// How many calls are held right now. See [`GateHandle::held_count`].
    pub fn held_count(&self) -> usize {
        self.handle.held_count()
    }

    /// The armed gate that would govern a call matching `(op, key)`: the first
    /// whose op equals and whose key substring (if any) is contained, matching
    /// the store's own precedence.
    fn attribute(&self, op: Op, key: &str) -> Option<usize> {
        self.gate_matchers
            .iter()
            .position(|(gop, gkey)| *gop == op && gkey.as_deref().is_none_or(|k| key.contains(k)))
    }

    /// Releases the held call `id`, exactly as [`GateHandle::release`], and
    /// -- only on a genuine release -- counts it toward both the cumulative
    /// total this cycle's [`CycleOutcome::gates_held`] reports and the per-gate
    /// counter of the gate that held it. The matcher is read from
    /// [`GateHandle::held_details`] before the release removes the held call.
    pub fn release(&self, id: u64) -> bool {
        let matcher = self
            .handle
            .held_details()
            .into_iter()
            .find(|(cid, _, _)| *cid == id)
            .map(|(_, op, key)| (op, key));
        let released = self.handle.release(id);
        if released {
            self.held_total.fetch_add(1, Ordering::SeqCst);
            if let Some((op, key)) = matcher
                && let Some(idx) = self.attribute(op, &key)
            {
                self.per_gate_held[idx].fetch_add(1, Ordering::SeqCst);
            }
        }
        released
    }
}

/// What a successful cycle produced, for the caller to log or assert on.
#[derive(Debug, Clone)]
pub struct CycleOutcome {
    pub master_seed: MasterSeed,
    pub digest: Digest,
    pub tenants_run: usize,
    pub series_generated: usize,
    pub queries_run: usize,
    /// Buckets `compact_bucket` reported as [`CompactionOutcome::Compacted`]
    /// across every tenant/shard/hour. A cycle that meant to exercise
    /// compaction can assert this is non-zero.
    pub buckets_compacted: usize,
    /// Total records observed by the post-compaction conservation probe
    /// (equal, per invariant (b), to the pre-compaction count).
    pub records_conserved: usize,
    /// Snapshot of the [`FaultStore`] counters after the cycle: how many times
    /// each `(Op, FaultKind)` fired. Empty when `inject_faults` is false.
    pub fault_counters: HashMap<(Op, FaultKind), u64>,
    /// The `(Op, FaultKind)` pairs the schedule intended to inject. A test
    /// asserts each has a positive `fault_counters` entry.
    pub expected_faults: Vec<(Op, FaultKind)>,
    /// How many hold/release gates the schedule armed on the store. Zero
    /// when `enable_gates` is false or the schedule (or its
    /// `fault_schedule_override`) carried none.
    pub gates_armed: usize,
    /// Cumulative count of calls actually released through the armed gate's
    /// [`CountingGateHandle`] this cycle (issue #878 deliverable 1). A test
    /// with `gates_armed > 0` asserts this is also positive to prove a call
    /// was genuinely held, not just that a gate was configured. `run_cycle`
    /// itself already enforces this -- see [`CycleError::GateNeverHit`] --
    /// so a successful [`Ok`] outcome with `gates_armed > 0` implies this is
    /// positive; the field is exposed for the exact count and for
    /// [`GateRelease::Manual`] callers that want the number without an
    /// error path.
    pub gates_held: usize,
}

/// Every variant carries the master seed so a failure message alone is
/// enough to replay it: `RAVEL_SIM_SEED=<seed> cargo test -p ravel-sim
/// <test>`, for a test whose seed comes from
/// [`crate::seed::MasterSeed::from_env_or`] (see that method's doc for which
/// ones do). The invariant-violation variants embed that replay line
/// literally in their `Display` output (issue #818 deliverable 4).
#[derive(Debug, thiserror::Error)]
pub enum CycleError {
    #[error("seed {seed}: workload generation failed: {source}")]
    Workload { seed: u64, source: TypeError },
    #[error("seed {seed}: could not build the paused simulation runtime: {source}")]
    Runtime { seed: u64, source: std::io::Error },
    #[error("seed {seed}: catalog config rejected: {source}")]
    CatalogConfig { seed: u64, source: CatalogError },
    #[error("seed {seed}: ingest write failed: {source}")]
    Ingest { seed: u64, source: WriteError },
    #[error("seed {seed}: catalog fold failed: {source}")]
    Fold { seed: u64, source: CatalogError },
    #[error("seed {seed}: catalog resolve failed: {source}")]
    Resolve { seed: u64, source: CatalogError },
    #[error("seed {seed}: query failed: {source}")]
    Query { seed: u64, source: QueryError },
    #[error("seed {seed}: compaction driver failed: {source}")]
    Compact { seed: u64, source: MaintainError },
    #[error("seed {seed}: sweep driver failed: {source}")]
    Sweep { seed: u64, source: MaintainError },
    #[error("seed {seed}: read-your-write violated for tenant {tenant}: {detail} (token {token})")]
    ReadYourWrite {
        seed: u64,
        tenant: String,
        token: String,
        detail: String,
    },
    #[error(
        "seed {seed}: strict-ack-implies-durable violated for tenant {tenant}: {detail} \
         (token {token})"
    )]
    AckNotDurable {
        seed: u64,
        tenant: String,
        token: String,
        detail: String,
    },
    #[error(
        "seed {seed}: compaction query-equivalence violated for tenant {tenant} ({phase}): \
         digest {before:#018x} != {after:#018x}. \
         replay: RAVEL_SIM_SEED={seed} cargo test -p ravel-sim"
    )]
    CompactionEquivalence {
        seed: u64,
        tenant: String,
        phase: &'static str,
        before: u64,
        after: u64,
    },
    #[error(
        "seed {seed}: record-count conservation violated for tenant {tenant}: \
         {before} records before compaction, {after} after. \
         replay: RAVEL_SIM_SEED={seed} cargo test -p ravel-sim"
    )]
    RecordCountConservation {
        seed: u64,
        tenant: String,
        before: usize,
        after: usize,
    },
    #[error(
        "seed {seed}: sweep left orphan/unreferenced objects past the horizon for tenant \
         {tenant} shard {shard}: {report}. \
         replay: RAVEL_SIM_SEED={seed} cargo test -p ravel-sim"
    )]
    SweepLeak {
        seed: u64,
        tenant: String,
        shard: u32,
        report: String,
    },
    #[error(
        "seed {seed}: compaction-phase fault invariant violated for tenant {tenant} ({phase}): a \
         compaction/sweep-phase fault neither recovered to an equivalent result nor surfaced a \
         typed error -- {before_records} records / digest {before_digest:#018x} before vs \
         {after_records} records / digest {after_digest:#018x} after (silent data loss). \
         replay: RAVEL_SIM_SEED={seed} cargo test -p ravel-sim"
    )]
    CompactionFaultInvariant {
        seed: u64,
        tenant: String,
        phase: &'static str,
        before_records: usize,
        after_records: usize,
        before_digest: u64,
        after_digest: u64,
    },
    #[error(
        "seed {seed}: hold/release gate #{gate_index} of {armed} armed was never held over the \
         whole cycle -- a matcher that never fires (wrong Nth, too few matching calls, or a gate \
         shadowed by an earlier one on the same target) must fail the cycle per gate, not pass \
         because some OTHER armed gate absorbed the shared release count. \
         replay: RAVEL_SIM_SEED={seed} cargo test -p ravel-sim"
    )]
    GateNeverHit {
        seed: u64,
        armed: usize,
        gate_index: usize,
    },
}

/// The recover-or-typed-error invariant's silent-loss guard (issue #931
/// deliverable 2): after a compaction/sweep-phase fault has been recovered by
/// the driver's idempotent re-run, the recovered snapshot must be bit-exactly
/// what the pre-compaction snapshot returned -- same query digest and same
/// record count. Any divergence is silent data loss and fails the cycle loud
/// with the seed. The typed-error branch of the invariant is enforced
/// separately: [`compact_bucket_recover`]/[`sweep_shard_recover`] surface an
/// exhausted or non-recoverable fault as a typed [`CycleError::Compact`] /
/// [`CycleError::Sweep`], never a panic and never a silent success.
///
/// Under a correct `ravel-maintain` the recovered snapshot always matches, so
/// this never fires on a real run; a dedicated unit test drives it with a
/// fabricated mismatch to prove it is not vacuous, then with a match to prove
/// it holds.
fn check_compaction_fault_invariant(
    seed: u64,
    tenant: &str,
    phase: &'static str,
    before: &Probe,
    after: &Probe,
) -> Result<(), CycleError> {
    if before.digest != after.digest || before.record_count != after.record_count {
        return Err(CycleError::CompactionFaultInvariant {
            seed,
            tenant: tenant.to_string(),
            phase,
            before_records: before.record_count,
            after_records: after.record_count,
            before_digest: before.digest.0,
            after_digest: after.digest.0,
        });
    }
    Ok(())
}

/// Compact one bucket, absorbing a recoverable compaction-phase fault with a
/// bounded idempotent re-run (issue #931 deliverable 2). Returns the outcome on
/// the recover branch, or a typed [`CycleError::Compact`] when the budget is
/// exhausted or the error is not recoverable -- the typed-error branch of the
/// recover-or-typed-error invariant. `compact_bucket` returns a `Result`, so a
/// panic is structurally impossible here.
async fn compact_bucket_recover(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    config: &CompactorConfig,
    bucket: &Bucket,
    seed: u64,
) -> Result<CompactionOutcome, CycleError> {
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match compact_bucket(store, clock, config, bucket).await {
            Ok(outcome) => return Ok(outcome),
            Err(e)
                if attempt < COMPACTION_FAULT_RETRY_BUDGET && is_recoverable_maintain_error(&e) =>
            {
                continue;
            }
            Err(source) => return Err(CycleError::Compact { seed, source }),
        }
    }
}

/// Sweep one shard, absorbing a recoverable sweep-phase fault with a bounded
/// idempotent re-run (issue #931 deliverable 2). `sweep_shard` is documented
/// idempotent, so re-running after a mid-pass listing fault converges. Returns
/// the report on the recover branch, or a typed [`CycleError::Sweep`] on the
/// typed-error branch.
#[allow(clippy::too_many_arguments)]
async fn sweep_shard_recover(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    config: &CompactorConfig,
    tenant_hash: &TenantHash,
    shard: u32,
    seed: u64,
) -> Result<(), CycleError> {
    let mut attempt = 0usize;
    loop {
        attempt += 1;
        match sweep_shard(
            store,
            clock,
            config,
            &NoLeases,
            tenant_hash,
            Signal::Metrics,
            shard,
        )
        .await
        {
            Ok(_) => return Ok(()),
            Err(e)
                if attempt < COMPACTION_FAULT_RETRY_BUDGET && is_recoverable_maintain_error(&e) =>
            {
                continue;
            }
            Err(source) => return Err(CycleError::Sweep { seed, source }),
        }
    }
}

/// Clock the driver hands to `IngestRouter`: `now_ns` is a plain atomic
/// counter, and `sleep` uses [`Clock::sleep`]'s default implementation
/// (delegates straight to `tokio::time::sleep`), then advances the counter
/// by the slept duration once that completes. Under a paused tokio runtime
/// this fast-forwards instantly and always advances in the same order the
/// awaited sleeps were issued, so two runs with the same seed see the same
/// sequence of `now_ns` values.
struct SimClock {
    now_ns: AtomicI64,
}

impl SimClock {
    fn new(start_ns: i64) -> Self {
        SimClock {
            now_ns: AtomicI64::new(start_ns),
        }
    }

    /// Advances `now_ns` to `target_ns`, monotonically: a `target_ns` at or
    /// behind the current value is a no-op. Lets the driver track the
    /// clock to each ingest batch's own event time (F13) without ever
    /// stepping it backwards, regardless of generation order.
    fn advance_to(&self, target_ns: i64) {
        self.now_ns.fetch_max(target_ns, Ordering::SeqCst);
    }
}

impl Clock for SimClock {
    fn now_ns(&self) -> i64 {
        self.now_ns.load(Ordering::SeqCst)
    }

    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            tokio::time::sleep(dur).await;
            self.now_ns
                .fetch_add(dur.as_nanos() as i64, Ordering::SeqCst);
        })
    }
}

/// The `now_ns` a fold must be given for `end_ts_ns`'s hour to be sealed:
/// exactly `sealed_watermark_hour`'s threshold, using the same three
/// margins it sums (`docs/catalog-and-mvcc.md`). `Catalog::fold`/`resolve`
/// never read a clock themselves, so this can be handed in as a literal
/// without the simulated clock needing to live through the gap.
fn seal_now_ns(end_ts_ns: i64) -> i64 {
    let margin_ns = DEFAULT_MAX_FLUSH_LIFETIME_NS
        + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
        + DEFAULT_FOLD_SAFETY_MARGIN_NS;
    let hour = end_ts_ns.div_euclid(NS_PER_HOUR);
    (hour + 1) * NS_PER_HOUR + margin_ns
}

/// The unix hour a nanosecond timestamp falls in, as the `u32`
/// `ingest_hour_bucket` a [`Bucket`] names.
fn hour_of(ts_ns: i64) -> u32 {
    ts_ns.div_euclid(NS_PER_HOUR) as u32
}

/// Sample count of one generated series, so the driver can batch ingest by
/// index across every series in a tenant (F13: every series in one tenant
/// shares `start_ts_ns`/`interval_ns`, so index `i` maps to the same event
/// timestamp in every series -- see `workload::generate_tenant_series`).
fn sample_count(series: &crate::workload::GeneratedSeries) -> usize {
    match &series.samples {
        SeriesSamples::Scalar(points) => points.len(),
        SeriesSamples::Histogram(points) => points.len(),
    }
}

/// The event timestamp and ingest point for `series`' sample at index `i`,
/// or `None` if that series has fewer than `i + 1` samples.
fn ingest_point_at(
    series: &crate::workload::GeneratedSeries,
    i: usize,
) -> Option<(i64, IngestPoint)> {
    match &series.samples {
        SeriesSamples::Scalar(points) => points.get(i).map(|sample| {
            (
                sample.ts_ns,
                IngestPoint {
                    series_id: series.series_id,
                    labels: series.labels.clone(),
                    value: IngestValue::Scalar(*sample),
                },
            )
        }),
        SeriesSamples::Histogram(points) => points.get(i).map(|sample| {
            (
                sample.ts_ns,
                IngestPoint {
                    series_id: series.series_id,
                    labels: series.labels.clone(),
                    value: IngestValue::Histogram(sample.clone()),
                },
            )
        }),
    }
}

/// The PromQL selector the driver queries a generated series through: the bare
/// metric name for a scalar series, `histogram_count(<name>)` for a native
/// histogram (the same convention `workload::generate_queries` and
/// `expected_sample` use).
fn series_selector(series: &crate::workload::GeneratedSeries) -> String {
    match &series.samples {
        SeriesSamples::Histogram(_) => format!("histogram_count({})", series.metric_name),
        SeriesSamples::Scalar(_) => series.metric_name.clone(),
    }
}

/// What `check_visible` (F3) expects a query pinned at `t_ms` to return for
/// one generated series: the exact label set the query result carries (for
/// a histogram query, `histogram_count()` drops `__name__` -- ravel-promql's
/// `drop_metric_name`, confirmed by its own
/// `histogram_count_reads_count_and_drops_name` test), a timestamp check,
/// and its value compared by bit pattern.
///
/// Timestamp-check honesty: for a scalar query,
/// `InstantSample::orig_sample_ts_ns` is the sample's own stored timestamp,
/// so the comparison genuinely verifies stored time. For `histogram_count()`
/// results, ravel-promql's `histogram_native.rs` sets `orig_sample_ts_ns`
/// to the evaluation timestamp, and since this check evaluates exactly at
/// the sample's own `t_ms`, the equality holds trivially on that arm: the
/// stored-timestamp guarantee for histogram series rests on the value-bits
/// check and the scalar arm, not on this field.
struct ExpectedSample<'a> {
    query: String,
    t_ms: i64,
    orig_ts_ns: i64,
    label_pairs: Vec<(&'a str, &'a str)>,
    value_bits: u64,
}

fn label_pairs(labels: &ravel_types::LabelSet) -> Vec<(&str, &str)> {
    labels
        .iter()
        .map(|l| (l.name.as_str(), l.value.as_str()))
        .collect()
}

fn expected_sample(series: &crate::workload::GeneratedSeries) -> Option<ExpectedSample<'_>> {
    match &series.samples {
        SeriesSamples::Scalar(points) => {
            let last = points.last()?;
            Some(ExpectedSample {
                query: series.metric_name.clone(),
                t_ms: last.ts_ns / 1_000_000,
                orig_ts_ns: last.ts_ns,
                label_pairs: label_pairs(&series.labels),
                value_bits: last.value.to_bits(),
            })
        }
        SeriesSamples::Histogram(points) => {
            let last = points.last()?;
            let count = match &last.value.counts {
                HistogramCounts::Int { count, .. } => *count as f64,
                HistogramCounts::Float { count, .. } => *count,
            };
            let name_dropped_pairs = series
                .labels
                .iter()
                .filter(|l| l.name != METRIC_NAME_LABEL)
                .map(|l| (l.name.as_str(), l.value.as_str()))
                .collect();
            Some(ExpectedSample {
                query: format!("histogram_count({})", series.metric_name),
                t_ms: last.ts_ns / 1_000_000,
                orig_ts_ns: last.ts_ns,
                label_pairs: name_dropped_pairs,
                value_bits: count.to_bits(),
            })
        }
    }
}

/// The fingerprint and record count of one probe over every generated series:
/// a range query per series, folded (order-independently) into a digest and
/// summed into a total point count. Invariant (a) compares digests across
/// phases; invariant (b) compares record counts.
struct Probe {
    digest: Digest,
    record_count: usize,
}

/// Runs one full seeded cycle: generate the workload and a seed-derived fault
/// schedule, wrap the store in a `FaultStore`, then per tenant ingest through
/// a strict-mode `IngestRouter`, check read-your-write, fold, check
/// strict-ack-implies-durable, drive `ravel-maintain`'s compaction and sweep
/// over the tenant's buckets and check the three compaction/sweep invariants,
/// run the workload's query mix, and fold every result into a reproducibility
/// digest.
pub fn run_cycle(
    master_seed: MasterSeed,
    config: &CycleConfig,
) -> Result<CycleOutcome, CycleError> {
    let seed = master_seed.0;
    let workload = crate::workload::generate(&master_seed, &config.workload)
        .map_err(|source| CycleError::Workload { seed, source })?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(config.paused_clock)
        .build()
        .map_err(|source| CycleError::Runtime { seed, source })?;

    rt.block_on(run_cycle_async(seed, master_seed, workload, config))
}

async fn run_cycle_async(
    seed: u64,
    master_seed: MasterSeed,
    workload: Workload,
    config: &CycleConfig,
) -> Result<CycleOutcome, CycleError> {
    // The seed-derived fault schedule (deliverable 1): the same seed yields
    // the same plan and gates. Wrap the MemoryStore oracle in a FaultStore so
    // ingest, fold, compaction, and sweep all run through the injected faults.
    let schedule = if let Some(override_schedule) = &config.fault_schedule_override {
        override_schedule.clone()
    } else if config.inject_faults {
        crate::fault_plan::generate(&master_seed, &config.fault_schedule)
    } else {
        FaultSchedule::none()
    };

    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), schedule.plan.clone()));
    fault_store
        .inner()
        .set_clock_ms((workload.start_ts_ns / 1_000_000).max(0) as u64);
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::clone(&fault_store) as Arc<dyn ObjectStoreBackend>;

    // Optionally arm the schedule's hold/release gates. Every armed gate's
    // handle shares the store's single held-call registry (see
    // `GateHandle`'s docs), so one `CountingGateHandle` built from the first
    // handle observes and can release calls held by any of them.
    //
    // `GateRelease::Immediate` spawns a background releaser that frees each
    // held call as soon as it is held: under the single-threaded paused
    // runtime it runs whenever a gated call parks on its release channel, so
    // the hold/release path is exercised without ever deadlocking. Aborted
    // at the end of the cycle. `GateRelease::Manual` hands the handle to the
    // caller's hook instead and spawns nothing: the caller owns release
    // timing (issue #878 deliverable 2).
    //
    // Either way, every release is counted (`CountingGateHandle::release`),
    // so `gates_armed > 0` with a total of zero releases at cycle end is
    // fail-loud (`CycleError::GateNeverHit`) rather than the previous
    // silent pass with the parked waiter aborted (issue #878 deliverable 3).
    // Zero, not `schedule.gates.len()`, when `enable_gates` is off: the
    // seed-derived generator emits a gate independently of `enable_gates`
    // (`fault_plan::generate_gates`), and a gate never armed on the store
    // can never be held -- counting it here would make `GateNeverHit` fire
    // for every cycle whose schedule happened to include one, regardless of
    // whether gates were ever meant to be exercised.
    let gates_armed = if config.enable_gates {
        schedule.gates.len()
    } else {
        0
    };
    let held_total = Arc::new(AtomicUsize::new(0));
    // Per-gate release counters and their matchers, index-aligned with the
    // armed gates, so `GateNeverHit` is enforced per gate (issue #931
    // deliverable 4). Sized to the armed gate count.
    let gate_matchers: Arc<Vec<(Op, Option<String>)>> = Arc::new(
        schedule
            .gates
            .iter()
            .map(|g| (g.op, g.key_contains.clone()))
            .collect(),
    );
    let per_gate_held: Arc<Vec<AtomicUsize>> =
        Arc::new((0..gates_armed).map(|_| AtomicUsize::new(0)).collect());
    let gate_releaser: Option<tokio::task::JoinHandle<()>> =
        if config.enable_gates && gates_armed > 0 {
            schedule
                .arm_gates(fault_store.as_ref())
                .into_iter()
                .next()
                .and_then(|handle| {
                    let counting = CountingGateHandle {
                        handle,
                        held_total: Arc::clone(&held_total),
                        gate_matchers: Arc::clone(&gate_matchers),
                        per_gate_held: Arc::clone(&per_gate_held),
                    };
                    match &config.gate_release {
                        GateRelease::Immediate => {
                            let releaser = counting.clone();
                            Some(tokio::spawn(async move {
                                loop {
                                    releaser.wait_until_held(1).await;
                                    for id in releaser.held() {
                                        releaser.release(id);
                                    }
                                }
                            }))
                        }
                        GateRelease::Manual(hook) => {
                            hook(counting);
                            None
                        }
                    }
                })
        } else {
            None
        };

    let sim_clock = Arc::new(SimClock::new(workload.start_ts_ns));
    let clock: Arc<dyn Clock> = Arc::clone(&sim_clock) as Arc<dyn Clock>;
    let mut clock_rng: StdRng = master_seed.rng("clock");

    // Seeded randomness for the ingest component (ADR-0068 decisions 1 and 2):
    // writer-id generation and PUT-retry backoff jitter draw from this
    // master-seed-derived source instead of OS entropy, so a whole cycle's
    // identities and jitter replay from the seed. One source is shared across
    // every tenant's router (its own derived domain), so writer ids stay
    // distinct within a run yet identical across runs of the same seed.
    let ingest_rng: Arc<dyn RngSource> =
        Arc::new(SeededRng::new(master_seed.sub_seed("ingest-rng")));
    // Folder identity for the driver's own `Catalog::fold` calls, from its own
    // derived domain so it neither perturbs nor is perturbed by the ingest
    // source's draw order (ADR-0068 decision 1). Replaces a `Uuid::new_v4()`
    // that made each run's snapshot folder id nondeterministic.
    let fold_rng = SeededRng::new(master_seed.sub_seed("fold-id"));
    // Compactor identity, from its own derived domain (ADR-0068 decision 1):
    // pins the `compactor_writer_id` that would otherwise be a fresh v4 UUID,
    // so a compaction run's record identity replays from the seed.
    let compactor_rng = SeededRng::new(master_seed.sub_seed("compactor-id"));

    let catalog_config = CatalogConfig {
        shard_count: config.shard_count,
        ..CatalogConfig::default()
    };
    let catalog = Arc::new(
        Catalog::new(Arc::clone(&store), catalog_config)
            .map_err(|source| CycleError::CatalogConfig { seed, source })?,
    );
    let engine = QueryEngine::new(
        Arc::clone(&catalog),
        Arc::clone(&store),
        EngineConfig::default(),
    );

    // Compaction/sweep config: the compactor identity is seeded, everything
    // else is the shipped default. `dry_run` stays false: the sweep must
    // really delete the superseded L0 inputs so invariant (c) verifies
    // physical convergence, not a plan.
    let compactor_config = CompactorConfig {
        compactor_writer_id: compactor_rng.new_uuid(),
        ..CompactorConfig::default()
    };

    // Phase-local fault layers over the one shared store (issue #931
    // deliverable 3): the compaction plan governs only `compact_bucket`, the
    // sweep plan only the sweep's action pass, so their rules cannot fire
    // during ingest, fold, or the query probes. Built once for the whole
    // cycle: each rule fires `Occurrence::Nth(1)`, so it lands on the first
    // tenant's compaction/sweep and later tenants run fault-free -- the same
    // once-per-cycle firing the ingest schedule uses. Both wrap a `SharedStore`
    // cloned from `store`, so every write stays visible to every phase.
    let compact_fault_store = Arc::new(FaultStore::new(
        SharedStore(Arc::clone(&store)),
        schedule.compact_plan.clone(),
    ));
    let sweep_fault_store = Arc::new(FaultStore::new(
        SharedStore(Arc::clone(&store)),
        schedule.sweep_plan.clone(),
    ));
    let compact_store: Arc<dyn ObjectStoreBackend> =
        Arc::clone(&compact_fault_store) as Arc<dyn ObjectStoreBackend>;
    let sweep_store: Arc<dyn ObjectStoreBackend> =
        Arc::clone(&sweep_fault_store) as Arc<dyn ObjectStoreBackend>;

    let fold_now_ns = seal_now_ns(workload.end_ts_ns);
    let full_range = TimeRange {
        start_ns: workload.start_ts_ns,
        end_ns: workload.end_ts_ns,
    };
    // The probe/query instant, held identical across every phase so the
    // before/after equivalence comparison is apples to apples.
    let probe_now_ns = fold_now_ns;
    let step_ms = (config.workload.interval_ns / 1_000_000).max(1);
    let start_ms = workload.start_ts_ns / 1_000_000;
    let end_ms = workload.end_ts_ns / 1_000_000;

    let mut digest_builder = DigestBuilder::new();
    let mut series_generated = 0usize;
    let mut queries_run = 0usize;
    let mut buckets_compacted = 0usize;
    let mut records_conserved = 0usize;

    for tenant_wl in &workload.tenants {
        series_generated += tenant_wl.series.len();
        let tenant_hash: TenantHash = tenant_wl.tenant.hash();

        let jitter_ms = clock_rng.random_range(0..=config.max_jitter.as_millis().max(1) as u64);
        clock.sleep(Duration::from_millis(jitter_ms)).await;

        let ingest_config = IngestConfig {
            shard_count: config.shard_count,
            ..IngestConfig::default()
        };
        let router = IngestRouter::with_rng(
            ingest_config,
            Arc::clone(&store),
            Signal::Metrics,
            Arc::clone(&clock),
            Arc::clone(&ingest_rng),
        );

        // F13: ingest in per-sample-index batches rather than one batch for
        // the whole tenant, advancing `sim_clock` to each batch's own event
        // timestamp first. See the long-form note preserved below for why.
        let max_samples = tenant_wl.series.iter().map(sample_count).max().unwrap_or(0);
        let mut tokens: Vec<CommitToken> = Vec::new();
        for i in 0..max_samples {
            let mut batch_ts_ns = None;
            let mut points = Vec::with_capacity(tenant_wl.series.len());
            for series in &tenant_wl.series {
                if let Some((ts_ns, point)) = ingest_point_at(series, i) {
                    batch_ts_ns.get_or_insert(ts_ns);
                    points.push(point);
                }
            }
            if points.is_empty() {
                continue;
            }
            sim_clock.advance_to(batch_ts_ns.unwrap_or(workload.start_ts_ns));

            let receipt = router
                .write_values(
                    tenant_wl.tenant.clone(),
                    points,
                    WriteMode::Strict,
                    config.ack_deadline,
                )
                .await
                .map_err(|source| CycleError::Ingest { seed, source })?;
            tokens.extend(receipt.tokens);
        }

        let clock_after_ingest = clock.now_ns();
        let ack_wall_ns = clock_after_ingest;

        // Invariant (b of the earlier wave), read-your-write: right after ack,
        // every token resolves and a pinned query returns the acked samples,
        // before any fold has run.
        check_visible(
            seed,
            tenant_wl.tenant.as_str(),
            &catalog,
            &engine,
            &tenant_hash,
            full_range,
            &tokens,
            ack_wall_ns,
            config.query_deadline,
            &tenant_wl.series,
            CheckKind::ReadYourWrite,
        )
        .await?;

        catalog
            .fold(
                &tenant_hash,
                Signal::Metrics,
                fold_rng.new_uuid(),
                fold_now_ns,
                &[],
            )
            .await
            .map_err(|source| CycleError::Fold { seed, source })?;

        // Invariant (a of the earlier wave), strict-ack-implies-durable: after
        // the fold, the same tokens still resolve, and a query with NO
        // min_tokens also returns the acked samples.
        check_visible(
            seed,
            tenant_wl.tenant.as_str(),
            &catalog,
            &engine,
            &tenant_hash,
            full_range,
            &tokens,
            fold_now_ns,
            config.query_deadline,
            &tenant_wl.series,
            CheckKind::AckDurable,
        )
        .await?;

        // ---- Compact -> sweep phases and their invariants (this task) ----

        // Baseline: what every series returns, and how many records, from the
        // pre-compaction (L0-backed) snapshot.
        let probe_before = probe_all_series(
            seed,
            &engine,
            tenant_hash,
            &tenant_wl.series,
            start_ms,
            end_ms,
            step_ms,
            probe_now_ns,
            config.query_deadline,
        )
        .await?;

        // Compact every (shard, hour) bucket this tenant may have written,
        // driving ravel-maintain's real `compact_bucket`. Non-qualifying
        // buckets (unsealed, too few inputs, empty) return a non-`Compacted`
        // outcome and are skipped harmlessly.
        //
        // The compaction clock is two hours past every bucket's seal margin.
        // The extra two hours are load-bearing for the re-fold below: a fold
        // whose new watermark hour does not exceed the previous fold's is a
        // no-op (`Catalog::fold` short-circuits when `watermark_hour_old >=
        // watermark_hour`), so it would never run the reconcile pass that
        // rewrites the just-compacted hour's snapshot entries from L0 to L1.
        // Pushing the clock past the first fold's watermark forces that
        // reconcile, and the compacted hour stays well inside the 26 h
        // reconcile window, so the compaction record is observed before the
        // sweep can delete its L0 inputs.
        let bucket_hi_ns = clock_after_ingest.max(workload.end_ts_ns);
        let compact_now_ns = seal_now_ns(bucket_hi_ns).max(fold_now_ns) + 2 * NS_PER_HOUR;
        let compact_clock = FixedClock::new(compact_now_ns);
        // Objects the compactor publishes get stamped at the compaction wall
        // time, so the sweep's age gates measure their real age.
        fault_store
            .inner()
            .set_clock_ms((compact_now_ns / 1_000_000).max(0) as u64);
        let hour_lo = hour_of(workload.start_ts_ns);
        let hour_hi = hour_of(bucket_hi_ns);
        // Drive compaction through the compaction-phase FaultStore, wrapping
        // each bucket in the bounded idempotent re-run (issue #931
        // deliverables 2/3): the partial-write and missing-object faults fire
        // once and the re-run recovers to an equivalent result, or an exhausted
        // fault surfaces as a typed `CycleError::Compact`.
        for shard in 0..config.shard_count {
            for hour in hour_lo..=hour_hi {
                let bucket = Bucket::new(tenant_hash, Signal::Metrics, shard, hour);
                let outcome = compact_bucket_recover(
                    compact_store.as_ref(),
                    &compact_clock,
                    &compactor_config,
                    &bucket,
                    seed,
                )
                .await?;
                if matches!(outcome, CompactionOutcome::Compacted { .. }) {
                    buckets_compacted += 1;
                }
            }
        }

        // Re-fold so the new snapshot references the freshly written L1 parts
        // and drops the superseded L0 inputs. This must happen before the
        // sweep physically deletes those inputs, or a resolve served from the
        // old snapshot would 404.
        catalog
            .fold(
                &tenant_hash,
                Signal::Metrics,
                fold_rng.new_uuid(),
                compact_now_ns,
                &[],
            )
            .await
            .map_err(|source| CycleError::Fold { seed, source })?;

        // Every subsequent query for this tenant runs through a fresh
        // `Catalog`/`QueryEngine` built over the same store, modeling the
        // disposable query process a real deployment would spin up
        // (CLAUDE.md: "every compute process is disposable"). This is not
        // cosmetic: the original engine's `Catalog` holds a TTL HEAD cache
        // that, under this harness's paused clock, never expires, so it would
        // keep resolving the pre-compaction snapshot (the L0 inputs the sweep
        // below deletes) and never observe the compaction at all. A fresh
        // catalog reads HEAD cold and sees the just-folded L1 snapshot.
        let post_catalog = Arc::new(
            Catalog::new(Arc::clone(&store), catalog_config)
                .map_err(|source| CycleError::CatalogConfig { seed, source })?,
        );
        let post_engine = QueryEngine::new(
            Arc::clone(&post_catalog),
            Arc::clone(&store),
            EngineConfig::default(),
        );

        let probe_after = probe_all_series(
            seed,
            &post_engine,
            tenant_hash,
            &tenant_wl.series,
            start_ms,
            end_ms,
            step_ms,
            probe_now_ns,
            config.query_deadline,
        )
        .await?;

        // Recover-or-typed-error invariant, recover branch (issue #931
        // deliverable 2): a compaction-phase fault that the re-run absorbed
        // must have reproduced a bit-exact snapshot. Silent loss here fails
        // loud with the seed, distinct from the plain (fault-free) equivalence
        // checks below.
        check_compaction_fault_invariant(
            seed,
            tenant_wl.tenant.as_str(),
            "after compaction",
            &probe_before,
            &probe_after,
        )?;

        // Invariant (a): bit-exact query equivalence before vs after
        // compaction. Compaction copies pages verbatim, so every query must
        // return byte-identical results (float values compared via `to_bits`
        // inside the digest).
        if probe_after.digest != probe_before.digest {
            return Err(CycleError::CompactionEquivalence {
                seed,
                tenant: tenant_wl.tenant.as_str().to_string(),
                phase: "after compaction",
                before: probe_before.digest.0,
                after: probe_after.digest.0,
            });
        }
        // Invariant (b): record-count conservation across compaction.
        if probe_after.record_count != probe_before.record_count {
            return Err(CycleError::RecordCountConservation {
                seed,
                tenant: tenant_wl.tenant.as_str().to_string(),
                before: probe_before.record_count,
                after: probe_after.record_count,
            });
        }
        records_conserved += probe_after.record_count;

        // Sweep every shard past the protection horizon and every GC age gate,
        // physically deleting the superseded L0 inputs.
        let sweep_now_ns = compact_now_ns
            .saturating_add(compactor_config.protection_horizon_ns)
            .saturating_add(compactor_config.grace_ns)
            .saturating_add(NS_PER_HOUR);
        let sweep_clock = FixedClock::new(sweep_now_ns);
        // Action pass through the sweep-phase FaultStore with the bounded
        // idempotent re-run (issue #931 deliverables 2/3): the pagination fault
        // fires on the first paginated listing and the re-run recovers, or an
        // exhausted fault surfaces as a typed `CycleError::Sweep`.
        for shard in 0..config.shard_count {
            sweep_shard_recover(
                sweep_store.as_ref(),
                &sweep_clock,
                &compactor_config,
                &tenant_hash,
                shard,
                seed,
            )
            .await?;
        }

        // Invariant (c): no orphan objects or unreferenced parts left past the
        // sweep horizon. A verification pass at the same (past-horizon) clock
        // must find nothing left to collect -- had the first pass left an
        // orphan or an unreferenced part behind, this pass would delete it and
        // report a non-zero count. Runs on the base `store` (no sweep faults),
        // so it verifies real physical convergence, not a fault-perturbed pass.
        for shard in 0..config.shard_count {
            let report = sweep_shard(
                store.as_ref(),
                &sweep_clock,
                &compactor_config,
                &NoLeases,
                &tenant_hash,
                Signal::Metrics,
                shard,
            )
            .await
            .map_err(|source| CycleError::Sweep { seed, source })?;
            if report.orphans_deleted != 0
                || report.unreferenced_parts_deleted != 0
                || report.superseded_records_deleted != 0
                || report.superseded_data_deleted != 0
                || report.orphan_breaker_tripped
            {
                return Err(CycleError::SweepLeak {
                    seed,
                    tenant: tenant_wl.tenant.as_str().to_string(),
                    shard,
                    report: format!("{report:?}"),
                });
            }
        }

        // Post-sweep durability: with the L0 inputs physically gone, every
        // query must still return the byte-identical baseline result, now
        // served entirely from the compacted L1 parts.
        let probe_after_sweep = probe_all_series(
            seed,
            &post_engine,
            tenant_hash,
            &tenant_wl.series,
            start_ms,
            end_ms,
            step_ms,
            probe_now_ns,
            config.query_deadline,
        )
        .await?;
        // Recover-or-typed-error invariant, recover branch, over the sweep:
        // with the L0 inputs physically gone, a sweep-phase fault the re-run
        // absorbed must leave the L1-served snapshot bit-exact.
        check_compaction_fault_invariant(
            seed,
            tenant_wl.tenant.as_str(),
            "after sweep",
            &probe_before,
            &probe_after_sweep,
        )?;
        if probe_after_sweep.digest != probe_before.digest {
            return Err(CycleError::CompactionEquivalence {
                seed,
                tenant: tenant_wl.tenant.as_str().to_string(),
                phase: "after sweep",
                before: probe_before.digest.0,
                after: probe_after_sweep.digest.0,
            });
        }

        // ---- Reproducibility digest over the workload's own query mix ----
        digest_builder.mix_str(tenant_wl.tenant.as_str());
        for query in &tenant_wl.queries {
            let value = run_query(
                &post_engine,
                tenant_hash,
                query,
                fold_now_ns,
                config.query_deadline,
            )
            .await
            .map_err(|source| CycleError::Query { seed, source })?;
            mix_value(&mut digest_builder, &value);
            queries_run += 1;
        }
    }

    if let Some(task) = gate_releaser {
        task.abort();
    }

    let gates_held = held_total.load(Ordering::SeqCst);
    // Per-gate never-hit check (issue #931 deliverable 4): every armed gate
    // must have held at least one call. Checking the shared total instead let
    // a partially-vacuous multi-gate schedule (some gates reachable, others
    // not) pass, because a reachable gate's releases pushed the one shared
    // counter positive. Attribution mirrors the store's precedence.
    for idx in 0..gates_armed {
        if per_gate_held[idx].load(Ordering::SeqCst) == 0 {
            return Err(CycleError::GateNeverHit {
                seed,
                armed: gates_armed,
                gate_index: idx,
            });
        }
    }

    // Merge the ingest- and compaction/sweep-phase fault counters and expected
    // sets (issue #931 deliverable 3), so the existing "every expected fault
    // fired" acceptance assertion (`compaction_equivalence_under_faults`, and
    // the nightly seed batch) automatically covers the new faults.
    let mut fault_counters = fault_store.counters_snapshot();
    for (key, count) in compact_fault_store
        .counters_snapshot()
        .into_iter()
        .chain(sweep_fault_store.counters_snapshot())
    {
        *fault_counters.entry(key).or_insert(0) += count;
    }
    let mut expected_faults = schedule.expected_faults;
    expected_faults.extend(schedule.expected_compaction_faults);

    Ok(CycleOutcome {
        master_seed,
        digest: digest_builder.finish(),
        tenants_run: workload.tenants.len(),
        series_generated,
        queries_run,
        buckets_compacted,
        records_conserved,
        fault_counters,
        expected_faults,
        gates_armed,
        gates_held,
    })
}

/// Runs a full-window range query for every generated series, folding results
/// into one order-independent digest and summing the total returned points.
#[allow(clippy::too_many_arguments)]
async fn probe_all_series(
    seed: u64,
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    series_list: &[crate::workload::GeneratedSeries],
    start_ms: i64,
    end_ms: i64,
    step_ms: i64,
    now_ns: i64,
    deadline: Duration,
) -> Result<Probe, CycleError> {
    let mut builder = DigestBuilder::new();
    let mut record_count = 0usize;
    for series in series_list {
        let selector = series_selector(series);
        let value = engine
            .range(
                tenant_hash,
                &selector,
                start_ms,
                end_ms,
                step_ms,
                &[],
                now_ns,
                deadline,
            )
            .await
            .map_err(|source| CycleError::Query { seed, source })?;
        if let Value::Matrix(matrix) = &value {
            for (_, points) in matrix {
                record_count += points.len();
            }
        }
        mix_value(&mut builder, &value);
    }
    Ok(Probe {
        digest: builder.finish(),
        record_count,
    })
}

#[derive(Debug, Clone, Copy)]
enum CheckKind {
    ReadYourWrite,
    AckDurable,
}

/// Checks both earlier-wave invariants for every one of `series_list`'s
/// generated series (F4): every token resolves (F12: a genuine store/decode
/// `CatalogError` propagates as [`CycleError::Resolve`], distinct from
/// [`CatalogError::UnsatisfiableToken`]), and a query pinned at each series'
/// own last sample returns exactly that sample back.
#[allow(clippy::too_many_arguments)]
async fn check_visible(
    seed: u64,
    tenant_label: &str,
    catalog: &Catalog,
    engine: &QueryEngine,
    tenant_hash: &TenantHash,
    full_range: TimeRange,
    tokens: &[CommitToken],
    now_ns: i64,
    deadline: Duration,
    series_list: &[crate::workload::GeneratedSeries],
    kind: CheckKind,
) -> Result<(), CycleError> {
    for token in tokens {
        let min_tokens = std::slice::from_ref(token);
        match catalog
            .resolve(tenant_hash, Signal::Metrics, full_range, min_tokens, now_ns)
            .await
        {
            Ok(_) => {}
            Err(CatalogError::UnsatisfiableToken { .. }) => {
                return Err(violation(
                    seed,
                    tenant_label,
                    token,
                    kind,
                    "commit token did not resolve".to_string(),
                ));
            }
            Err(source) => {
                return Err(CycleError::Resolve { seed, source });
            }
        }
    }

    let query_min_tokens: &[CommitToken] = match kind {
        CheckKind::ReadYourWrite => tokens,
        CheckKind::AckDurable => &[],
    };
    let fallback_token = tokens.first().cloned().unwrap_or(CommitToken {
        shard: 0,
        writer_id: Uuid::nil(),
        epoch: 0,
        seq: 0,
        ingest_hour_bucket: 0,
    });

    for series in series_list {
        let Some(expected) = expected_sample(series) else {
            continue;
        };
        let value = engine
            .instant(
                *tenant_hash,
                &expected.query,
                expected.t_ms,
                query_min_tokens,
                now_ns,
                deadline,
            )
            .await
            .map_err(|source| CycleError::Query { seed, source })?;
        let matched = match &value {
            Value::Vector(v) => v.iter().any(|sample| {
                label_pairs(&sample.labels) == expected.label_pairs
                    && sample.orig_sample_ts_ns == expected.orig_ts_ns
                    && sample.value.to_bits() == expected.value_bits
            }),
            _ => false,
        };
        if !matched {
            return Err(violation(
                seed,
                tenant_label,
                &fallback_token,
                kind,
                format!(
                    "every token resolved, but query {} did not return the acked sample \
                     (label set / timestamp / value bits mismatch)",
                    expected.query
                ),
            ));
        }
    }
    Ok(())
}

/// `detail` says which check actually failed -- token resolution or a
/// content mismatch on an already-resolved token -- so a value corruption is
/// never reported as a token-durability failure against an innocent token.
fn violation(
    seed: u64,
    tenant_label: &str,
    token: &CommitToken,
    kind: CheckKind,
    detail: String,
) -> CycleError {
    let token = token.encode();
    match kind {
        CheckKind::ReadYourWrite => CycleError::ReadYourWrite {
            seed,
            tenant: tenant_label.to_string(),
            token,
            detail,
        },
        CheckKind::AckDurable => CycleError::AckNotDurable {
            seed,
            tenant: tenant_label.to_string(),
            token,
            detail,
        },
    }
}

async fn run_query(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    query: &QuerySpec,
    now_ns: i64,
    deadline: Duration,
) -> Result<Value, QueryError> {
    match query {
        QuerySpec::Instant { query, t_ms } => {
            engine
                .instant(tenant_hash, query, *t_ms, &[], now_ns, deadline)
                .await
        }
        QuerySpec::Range {
            query,
            start_ms,
            end_ms,
            step_ms,
        } => {
            engine
                .range(
                    tenant_hash,
                    query,
                    *start_ms,
                    *end_ms,
                    *step_ms,
                    &[],
                    now_ns,
                    deadline,
                )
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(digest: u64, record_count: usize) -> Probe {
        Probe {
            digest: Digest(digest),
            record_count,
        }
    }

    /// The recover-or-typed-error invariant's silent-loss guard is not vacuous:
    /// feeding it a fabricated post-recovery mismatch (the "injected silent
    /// loss") makes it fire, and it is the exact function the seeded cycle runs
    /// on real probe data. This is the flip that proves the assertion, per
    /// issue #931 deliverable 2 / the prove-the-test discipline.
    #[test]
    fn compaction_fault_invariant_fires_on_injected_silent_loss() {
        // Injected silent loss: same query digest changed, or the record count
        // dropped, after a fault the driver "recovered" -- either divergence is
        // a violation.
        let before = probe(0xABCD, 100);

        // A dropped-record variant (fewer records after "recovery").
        let after_fewer = probe(0xABCD, 96);
        let err = check_compaction_fault_invariant(
            "tenant-a".len() as u64,
            "tenant-a",
            "after compaction",
            &before,
            &after_fewer,
        )
        .expect_err("a post-recovery record drop must trip the invariant");
        assert!(
            matches!(
                err,
                CycleError::CompactionFaultInvariant {
                    before_records: 100,
                    after_records: 96,
                    ..
                }
            ),
            "expected CompactionFaultInvariant, got: {err}"
        );

        // A changed-digest variant (same count, different values).
        let after_changed = probe(0x1234, 100);
        let err =
            check_compaction_fault_invariant(7, "tenant-a", "after sweep", &before, &after_changed)
                .expect_err("a post-recovery digest change must trip the invariant");
        assert!(matches!(err, CycleError::CompactionFaultInvariant { .. }));
    }

    /// The removed-injection half of the flip: with the recovered snapshot
    /// bit-exactly matching the pre-compaction one -- what a correct re-run
    /// always produces -- the same guard holds (returns `Ok`), so it never
    /// fires on a real cycle.
    #[test]
    fn compaction_fault_invariant_holds_when_equivalent() {
        let before = probe(0xABCD, 100);
        let after = probe(0xABCD, 100);
        check_compaction_fault_invariant(7, "tenant-a", "after compaction", &before, &after)
            .expect("an equivalent recovered snapshot must satisfy the invariant");
    }

    /// A recoverable store fault (retryable, or a not-found blip) is retried;
    /// every other `MaintainError` surfaces immediately as a typed error.
    #[test]
    fn recoverable_maintain_error_classification() {
        assert!(is_recoverable_maintain_error(&MaintainError::Store(
            StoreError::Transient("blip".into())
        )));
        assert!(is_recoverable_maintain_error(&MaintainError::Store(
            StoreError::Throttled { retry_after_ms: 1 }
        )));
        assert!(is_recoverable_maintain_error(&MaintainError::Store(
            StoreError::Timeout
        )));
        assert!(is_recoverable_maintain_error(&MaintainError::Store(
            StoreError::NotFound
        )));
        // Not recoverable: a genuine precondition failure or an invariant breach.
        assert!(!is_recoverable_maintain_error(&MaintainError::Store(
            StoreError::PreconditionFailed
        )));
        assert!(!is_recoverable_maintain_error(&MaintainError::Invariant(
            "breach".into()
        )));
    }
}
