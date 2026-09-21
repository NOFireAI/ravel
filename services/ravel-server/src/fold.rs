//! Per-signal background catalog fold task (ADR-0020; storage-derived tenant
//! set is ADR-0048 decision 3). Periodically calls [`Catalog::fold`] so query resolve can
//! serve sealed history from snapshots instead of full listing.
//!
//! Never runs on the ingest or query path, and never affects correctness:
//! every failure here is logged and retried on the next tick. Disabling this
//! task (`--disable-fold`) only changes query cost, never query results.
//!
//! One loop per [`FOLD_SIGNALS`] entry, not one per tenant: each tick
//! re-enumerates tenants from storage
//! ([`crate::tenant_discovery::discover_and_restrict_by_lifecycle`]) and folds
//! every tenant the cycle discovers, narrowed by each tenant's durable
//! lifecycle state and the flag fallback (ADR-0066 decision 6: a config record
//! keeps a tenant in the set unconditionally regardless of its token, and no
//! flag can exclude a config-recorded tenant). A tenant onboarded mid-run is
//! folded starting the next tick with no restart.
//! A discovery failure skips that signal's whole cycle -- no tenant is folded
//! -- and is retried next tick; it never falls back to an empty set, since
//! fold is best-effort and a quiet failure here would look identical to
//! "nothing new to fold."

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use ravel_catalog::{Catalog, FoldReport, RefoldRequest};
use ravel_commit::rng::{RngSource, SystemRng};
use ravel_ingest::{Clock, SystemClock};
use ravel_maintain::RetentionConfig;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::tenant_discovery::discover_and_restrict_by_lifecycle;

/// Default [`RefoldQueue`] capacity: 256 pending `(tenant, signal)` requests.
///
/// Sized against the producer, not against tenant count: one maintain tick
/// enqueues at most one request per owned `(tenant, signal)` pair, and the
/// fold loops drain everything for their signal at the head of every tick, so
/// the depth only grows while a fold tick is slower than a maintain tick. 256
/// covers 85 tenants across the three [`FOLD_SIGNALS`] backing up for a whole
/// fold interval without a single drop, and a drop costs nothing but latency:
/// the next sweep still sees the hour blocked and re-sends it.
pub const DEFAULT_REFOLD_QUEUE_CAPACITY: usize = 256;

/// One sweep-to-fold hand-off: the ingest hours the sweep held on
/// [`ravel_maintain::SnapshotBlock::Named`] for one `(tenant, signal)`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RefoldEntry {
    tenant: TenantHash,
    signal: Signal,
    hours: BTreeSet<u32>,
}

/// The bounded sweep-to-fold re-fold channel (issue #1763 part b).
///
/// The maintain tick is the only producer: after sweeping a `(tenant, signal)`
/// it sends the union of [`ravel_maintain::SweepReport::blocked_named_hours`]
/// over the shards it swept. Each per-signal fold loop is the only consumer of
/// its own signal's entries, and drains them at the head of its next tick into
/// [`RefoldRequest`]s it passes to
/// [`Catalog::fold_with_refold_request`].
///
/// Bounded with drop-oldest. `capacity` is a hard cap on queue depth; a send
/// into a full queue discards the FRONT entry (the oldest, least likely to
/// still be accurate) and counts it in [`Self::dropped_requests`]. Dropping is
/// safe by construction rather than by luck: the sweep is stateless and
/// re-derives the blocked set from the store every pass, so a dropped request
/// is re-sent by the next sweep of that unit. The cost of a drop is one more
/// fold interval of an hour naming pre-rewrite inputs, never a permanently
/// uncollectable object.
///
/// Not a `tokio::sync::mpsc` channel: a `Sender` cannot evict the queue's
/// front, so a bounded mpsc can only block the producer or drop the NEWEST
/// value. Blocking the maintain tick on a lagging fold is the wrong trade for
/// a best-effort hint, and dropping the newest inverts the intended policy.
#[derive(Debug)]
pub struct RefoldQueue {
    capacity: usize,
    pending: Mutex<VecDeque<RefoldEntry>>,
    dropped: AtomicU64,
}

impl RefoldQueue {
    /// A queue holding at most `capacity` pending requests. A `capacity` of 0
    /// is raised to 1: a queue that drops everything it is given would make
    /// the hand-off silently dead rather than bounded.
    pub fn with_capacity(capacity: usize) -> Self {
        RefoldQueue {
            capacity: capacity.max(1),
            pending: Mutex::new(VecDeque::new()),
            dropped: AtomicU64::new(0),
        }
    }

    /// Enqueue one `(tenant, signal)`'s blocked hours, evicting the oldest
    /// pending entry if the queue is full. An empty `hours` set is not
    /// enqueued: there is nothing for a fold to reconcile, and enqueuing it
    /// would let ordinary ticks evict real requests.
    pub fn send(&self, tenant: TenantHash, signal: Signal, hours: BTreeSet<u32>) {
        if hours.is_empty() {
            return;
        }
        let entry = RefoldEntry {
            tenant,
            signal,
            hours,
        };
        // A poisoned lock cannot corrupt anything here (the queue is a plain
        // deque of owned values), and refusing the hand-off would be worse
        // than serving a stale view of it, so recover the guard either way.
        let mut pending = match self.pending.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        while pending.len() >= self.capacity {
            pending.pop_front();
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        pending.push_back(entry);
    }

    /// Remove and return every pending request for `signal`, merged per
    /// tenant (a tenant swept twice between two fold ticks contributes the
    /// union of both blocked sets, not the later one alone). Entries for other
    /// signals are left in the queue for their own fold loop.
    fn take_for_signal(&self, signal: Signal) -> BTreeMap<TenantHash, RefoldRequest> {
        let mut pending = match self.pending.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut taken: BTreeMap<TenantHash, RefoldRequest> = BTreeMap::new();
        let mut kept = VecDeque::with_capacity(pending.len());
        for entry in pending.drain(..) {
            if entry.signal == signal {
                let request = taken.entry(entry.tenant).or_default();
                for hour in entry.hours {
                    request.insert(hour);
                }
            } else {
                kept.push_back(entry);
            }
        }
        *pending = kept;
        taken
    }

    /// Requests evicted by a full queue since this queue was created. `0` in
    /// the steady state; a rising value means the fold is not keeping up with
    /// the sweep and some hours are taking extra fold intervals to reconcile.
    pub fn dropped_requests(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Pending requests across every signal. Test and diagnostic accessor;
    /// the loops themselves only ever drain.
    pub fn pending_len(&self) -> usize {
        match self.pending.lock() {
            Ok(guard) => guard.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        }
    }
}

/// Default `fold_interval`: 5 minutes.
pub const DEFAULT_FOLD_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy)]
pub struct FoldTaskConfig {
    pub enabled: bool,
    pub fold_interval: Duration,
}

impl Default for FoldTaskConfig {
    fn default() -> Self {
        FoldTaskConfig {
            enabled: true,
            fold_interval: DEFAULT_FOLD_INTERVAL,
        }
    }
}

/// Handle to every spawned fold task, so shutdown can stop them cleanly
/// (mirrors [`crate::Running`]'s listener shutdown handles).
pub struct FoldTasks {
    shutdown: Vec<oneshot::Sender<()>>,
    handles: Vec<JoinHandle<()>>,
}

impl FoldTasks {
    pub fn none() -> Self {
        FoldTasks {
            shutdown: Vec::new(),
            handles: Vec::new(),
        }
    }

    pub async fn shutdown(self) {
        for tx in self.shutdown {
            let _ = tx.send(());
        }
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}

/// Every signal `ravel-server` folds. One fold loop is spawned per
/// (tenant, signal) pair, so adding a signal here adds one loop per tenant
/// without changing any loop's shape. `FoldTaskConfig` (enabled,
/// fold_interval) is shared across signals for v1: all currently want the
/// same 5-minute cadence (ADR-0033); a per-signal interval is a config-shape
/// follow-up if that changes.
///
/// [`Signal::Spans`] is added once span compaction exists (ADR-0041 phase 3):
/// span buckets now produce L1 `.rspan` parts the resolver serves from
/// snapshots, exactly as logs do. Spans fold through the same
/// [`Catalog::fold`] path as logs and share the same accepted no-op-postings
/// cost described below (an `.rspan` object carries signal=3, so the
/// RSEG-specific postings build fails to decode it and skips writing a
/// postings ref, same as logs' signal=2).
///
/// `pub(crate)` so `crate::metrics` renders exactly one fold-liveness series
/// per entry: the series set at `/metrics` is then the set of loops that are
/// supposed to be alive, and a loop that dies leaves its own series standing
/// and going stale rather than hiding behind its siblings.
pub(crate) const FOLD_SIGNALS: [Signal; 3] = [Signal::Metrics, Signal::Logs, Signal::Spans];

/// Spawns one fold loop per signal in [`FOLD_SIGNALS`], not one per tenant:
/// each tick re-derives the tenant set from storage. [`run_loop`] is
/// signal-generic; a new signal is added by extending that array, not by
/// restructuring this function (ADR-0033 gap 1; folding is per
/// (tenant, signal) throughout).
///
/// [`Signal::Logs`] folds through the same [`Catalog::fold`] path as metrics
/// and produces `catalog/l/HEAD` plus snapshot parts, but no name-postings
/// object: `Catalog::fold` always attempts the RSEG-specific postings build
/// (`build_postings`/`fetch_entry_names`), which for a log entry issues one
/// full-object GET and then fails to decode the bytes as RSEG (an RLOG object
/// carries signal=2, RSEG expects signal=1), so `build_postings` returns
/// `None` and the fold skips writing a postings ref without failing. That
/// wasted GET-plus-failed-decode recurs every fold cycle for each log entry
/// newly covered since the last fold: real I/O and CPU proportional to new
/// log volume. It is accepted for v1 (ADR-0033), not a bug; fixing it would
/// mean a signal-aware short-circuit inside `ravel-catalog`, deliberately out
/// of scope here.
///
/// `fallback_allow` is the merged static tenant set (`--tenant-token` or
/// `--tenant-token-file`, plus `--maintain-tenant`):
/// empty means unconfigured, and it otherwise governs only tenants with no
/// durable config record (ADR-0048 decision 3, ADR-0066 decision 6). A tenant
/// carrying a config record is maintained unconditionally, so no flag can
/// exclude it. Returns immediately; tasks run in the background until
/// [`FoldTasks::shutdown`].
///
/// `retention` is the same CLI-derived [`RetentionConfig`] the Maintain-mode
/// physical sweep uses (`main.rs`, threaded into `MaintenanceTaskConfig`).
/// Each tick resolves `retention.window_for(tenant)` per tenant and passes it
/// into [`Catalog::fold`] as the deployment-default retention window, so the
/// fold's retention-frontier reconcile runs for a tenant configured only by
/// CLI flags with no durable `TenantConfig.retention_ns` record (ADR-0078).
/// An unconfigured `RetentionConfig` resolves to `None` for every tenant, so
/// this is inert when no retention flag is set.
pub fn spawn(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    fallback_allow: &[TenantHash],
    config: FoldTaskConfig,
    retention: Arc<RetentionConfig>,
) -> FoldTasks {
    spawn_with_refold(catalog, store, fallback_allow, config, retention, None)
}

/// [`spawn`], with the sweep-to-fold re-fold channel connected.
///
/// `refold` is the consumer end of the queue a maintain tick in the same
/// process sends to (issue #1763 part b). Each spawned loop drains only its
/// own signal's requests, at the head of every tick, and passes them to
/// [`Catalog::fold_with_refold_request`] so the targeted reconcile pass
/// re-reads those hours even when they sit outside the fixed reconcile
/// window. `None` (what [`spawn`] passes) folds exactly as before: an empty
/// request on every tick.
pub fn spawn_with_refold(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    fallback_allow: &[TenantHash],
    config: FoldTaskConfig,
    retention: Arc<RetentionConfig>,
    refold: Option<Arc<RefoldQueue>>,
) -> FoldTasks {
    if !config.enabled {
        return FoldTasks::none();
    }

    // Production OS-entropy randomness (ADR-0068 decision 2): the folder id
    // and the per-tick loop jitter both draw from this one source instead of
    // `Uuid::new_v4()` / `rand::rng()` directly. The server always uses the
    // OS-entropy default; only the simulation harness injects a seeded source,
    // and it does not drive this loop.
    let rng: Arc<dyn RngSource> = Arc::new(SystemRng);
    // One folder_id per process start (proto/ravel/catalog.proto,
    // `SnapshotHead.folder_id`), shared by every signal loop in this process.
    let folder_id = rng.new_uuid();
    let fallback_allow = if fallback_allow.is_empty() {
        None
    } else {
        Some(fallback_allow.to_vec())
    };
    let mut shutdown = Vec::new();
    let mut handles = Vec::new();
    for signal in FOLD_SIGNALS {
        let (tx, rx) = oneshot::channel();
        let catalog = catalog.clone();
        let store = store.clone();
        let fallback_allow = fallback_allow.clone();
        let interval = config.fold_interval;
        let rng = Arc::clone(&rng);
        let retention = Arc::clone(&retention);
        let refold = refold.clone();
        let handle = tokio::spawn(async move {
            run_loop(
                catalog,
                store,
                signal,
                fallback_allow,
                folder_id,
                interval,
                rng,
                retention,
                refold,
                rx,
            )
            .await;
        });
        shutdown.push(tx);
        handles.push(handle);
    }
    FoldTasks { shutdown, handles }
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    signal: Signal,
    fallback_allow: Option<Vec<TenantHash>>,
    folder_id: Uuid,
    interval: Duration,
    rng: Arc<dyn RngSource>,
    retention: Arc<RetentionConfig>,
    refold: Option<Arc<RefoldQueue>>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval, rng.as_ref())) => {}
            _ = &mut shutdown => return,
        }

        let outcome = match discover_and_restrict_by_lifecycle(
            store.as_ref(),
            fallback_allow.as_deref(),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                tracing::error!(
                    signal = ?signal,
                    error = %err,
                    "catalog fold: tenant discovery failed; skipping this cycle entirely, retried next tick"
                );
                continue;
            }
        };
        if outcome.excluded > 0 {
            tracing::debug!(
                signal = ?signal,
                excluded = outcome.excluded,
                "catalog fold: flag restriction excluded discovered tenants"
            );
        }

        // Drain this signal's pending re-fold requests once per cycle, after
        // discovery: a request for a tenant this cycle does not maintain has
        // no fold to attach to, and holding it would keep evicting fresher
        // ones. It is taken out of the queue either way, and the next sweep of
        // that unit re-derives it.
        let mut requests = take_refold_requests(refold.as_deref(), signal);

        for tenant in outcome.maintained {
            // The deployment-default retention window for this tenant, resolved
            // per tick from the CLI-derived RetentionConfig (ADR-0078). The fold
            // overlays the durable TenantConfig.retention_ns on top of it.
            let default_retention_ns = retention.window_for(&tenant);
            let request = requests.remove(&tenant).unwrap_or_default();
            run_tenant_tick(
                catalog.as_ref(),
                store.as_ref(),
                &tenant,
                signal,
                folder_id,
                interval,
                SystemClock.now_ns(),
                default_retention_ns,
                &request,
            )
            .await;
        }
    }
}

/// Take every pending re-fold request for `signal` off the queue, merged per
/// tenant. `None` (no queue connected) yields no requests, which is exactly
/// what a fold with no sweep in its process sees.
///
/// `pub(crate)` so the maintain-side hand-off test drives the same drain the
/// loop does instead of reconstructing a request by hand.
pub(crate) fn take_refold_requests(
    refold: Option<&RefoldQueue>,
    signal: Signal,
) -> BTreeMap<TenantHash, RefoldRequest> {
    match refold {
        Some(queue) => queue.take_for_signal(signal),
        None => BTreeMap::new(),
    }
}

/// One fold attempt for one tenant: the HEAD freshness peek, then
/// [`Catalog::fold_with_refold_request`] if it's stale. Split out from
/// [`run_loop`] so discovery and the per-tenant fold logic stay independently
/// readable.
///
/// `refold_request` is what the sweep asked this fold to reconcile. It is
/// passed through even when empty, which is the same call
/// [`Catalog::fold`] makes internally. A non-empty request does NOT bypass the
/// freshness peek: a HEAD another replica folded within the last interval has
/// already run its own targeted pass over whatever its own sweep found, and
/// this replica's request survives in the store (the hour stays blocked) to be
/// re-derived by the next sweep.
///
/// `now_ns` is the caller's clock reading, taken once per tenant by
/// [`run_loop`] from [`SystemClock`]. Taking it as a parameter is what lets a
/// test fold at a determined instant (CLAUDE.md testing patterns: time is
/// injected) instead of at whatever the host's wall clock says, which for a
/// fixture built on low ingest hours decides whether anything is sealed at
/// all.
///
/// Returns the fold's own [`FoldReport`], or `None` when the freshness peek
/// skipped this tick or the fold itself failed. [`run_loop`] logs and
/// discards it; the maintain-side hand-off test asserts
/// [`FoldReport::refold_hours_reconciled`] on it.
///
/// `pub(crate)` so that test folds through this exact function rather than
/// calling the catalog directly.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_tenant_tick(
    catalog: &Catalog,
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    folder_id: Uuid,
    interval: Duration,
    now_ns: i64,
    default_retention_ns: Option<i64>,
    refold_request: &RefoldRequest,
) -> Option<FoldReport> {
    if head_fresh_enough(store, tenant, signal, interval, now_ns).await {
        tracing::debug!(
            tenant = %tenant.to_hex(),
            signal = ?signal,
            refold_hours_requested = refold_request.len(),
            "catalog fold: HEAD already fresh, skipping this tick"
        );
        return None;
    }

    match catalog
        .fold_with_refold_request(
            tenant,
            signal,
            folder_id,
            now_ns,
            &[],
            default_retention_ns,
            refold_request,
        )
        .await
    {
        Ok(report) => {
            tracing::info!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                no_op = report.no_op,
                rebuilt = report.rebuilt,
                watermark_hour = ?report.watermark_hour,
                previous_watermark_hour = ?report.previous_watermark_hour,
                buckets_folded = report.buckets_folded,
                entry_count = report.entry_count,
                part_bytes = report.part_bytes,
                list_requests = report.list_requests,
                get_requests = report.get_requests,
                put_requests = report.put_requests,
                refold_hours_requested = refold_request.len(),
                refold_hours_reconciled = report.refold_hours_reconciled,
                "catalog fold complete"
            );
            Some(report)
        }
        Err(err) => {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                error = %err,
                "catalog fold failed; the index degrades to listing until a later fold succeeds"
            );
            None
        }
    }
}

/// Adds up to 10% jitter on top of `base`, so multiple replicas' fold tasks
/// (started at roughly the same time) don't tick in lockstep forever. Shared
/// with the store-reachability probe ([`crate::store_probe`]), which reuses this
/// single helper rather than growing a second copy of the same jitter rule.
pub(crate) fn jittered(base: Duration, rng: &dyn RngSource) -> Duration {
    let jitter_bound_ms = u64::try_from(base.as_millis() / 10).unwrap_or(u64::MAX);
    if jitter_bound_ms == 0 {
        return base;
    }
    let extra_ms = rng.jitter_ms(jitter_bound_ms);
    base + Duration::from_millis(extra_ms)
}

/// Cheap duplicate-work avoidance across replicas: peeks at HEAD directly
/// (bypassing the
/// catalog's own HEAD cache and fold logic) and skips this tick if another
/// replica folded within the last `interval`. Correctness never depends on
/// this: `catalog.fold` performs its own authoritative HEAD read and no-ops
/// safely if there is nothing new to fold, so a wrong (or racing) answer here
/// only costs a redundant fold attempt, never a missed one.
async fn head_fresh_enough(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    interval: Duration,
    now_ns: i64,
) -> bool {
    let key = head_key(tenant, signal);
    let Ok(got) = store.get(&key, GetRange::Full).await else {
        return false;
    };
    let Ok(head) = ravel_catalog::decode_head(&got.data) else {
        return false;
    };
    head_is_fresh(head.created_unix_ns, now_ns, interval)
}

/// Whether a HEAD published at `created_unix_ns` is younger than `interval` as
/// of `now_ns`. The freshness predicate behind [`head_fresh_enough`], factored
/// out so the on-demand route ([`crate::fold_on_demand`]) applies the same rule
/// to a HEAD it has already read rather than growing a second freshness notion.
/// A zero `interval` is never fresh (age is always `>= 0`), which is how a
/// caller opts out of the gate.
pub(crate) fn head_is_fresh(created_unix_ns: i64, now_ns: i64, interval: Duration) -> bool {
    let age_ns = now_ns.saturating_sub(created_unix_ns);
    age_ns >= 0 && (age_ns as u128) < interval.as_nanos()
}

/// HEAD object key (docs/catalog-and-mvcc.md key layout, frozen format):
/// `t/<tenant_hash>/catalog/<signal>/HEAD`. Reconstructed here rather than
/// imported because it names a `pub(crate)` helper inside `ravel-catalog`;
/// this task only ever uses it for the freshness peek above, never to
/// mutate the object, and so does
/// [`crate::fold_on_demand`], which shares this one spelling of the key
/// rather than growing a second copy.
pub(crate) fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}
