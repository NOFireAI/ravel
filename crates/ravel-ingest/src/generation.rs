//! Router live switch for online resharding (ADR-0052 sections 2-3).
//!
//! Each ingest router owns a fixed shard-actor set built once at construction.
//! Resharding replaces that with a *generation-versioned* view: a tenant's
//! provisioning record ([`ravel_catalog`]) carries an append-only history of
//! `(generation, shard_count, activation_hour)` entries, and a record routed at
//! wall-clock hour `h` uses the count of the latest generation whose
//! `activation_hour <= h` (the pure rule [`ravel_catalog::active_shard_count`]).
//!
//! [`GenerationSwitch`] is the shared mechanism all three routers embed. It:
//!
//! - keeps one shard-actor set per distinct `shard_count` seen, keyed by count
//!   and shared across every tenant currently routing at that count (a set is
//!   constructed lazily via the router-supplied factory the first time a count
//!   becomes active, and old sets are never force-closed: they keep draining
//!   and flushing under their original shard indices, ADR-0052 section 2); and
//! - caches each tenant's decoded generation history plus the wall-clock time
//!   its view was last refreshed.
//!
//! **Refresh interval `C` and fail-closed staleness (ADR-0052 section 3).** A
//! router routes on a cached view only while it is younger than `C`
//! ([`DEFAULT_REFRESH_INTERVAL_NS`]). When the cache is older than `C` (or the
//! tenant has never been resolved from a persisted record), the router re-reads
//! the provisioning record and refreshes before routing, so it never routes on
//! a view older than `C`. If that re-read cannot complete (a store error, or a
//! corrupt/undecodable record whose true generation set is unknown), the router
//! **fails the flush closed** with a typed error and a metrics-visible counter,
//! rather than route on a stale or untrusted view. This is the load-bearing
//! safety property: the CLI computes a reshard's activation lead
//! `L >= ceil(C) + 1` hours, so a writer that keeps writing always refreshes and
//! observes a new generation within `C` — well before it activates — and a
//! writer that cannot reach the record stops rather than route past an
//! activation it may not have seen.
//!
//! Nothing here moves, rewrites, or re-keys existing data: a switch only
//! changes which shard-actor set *new* records route to (ADR-0052, "No data
//! movement anywhere").

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

use prost::Message;
use ravel_catalog::{
    ShardGeneration, active_shard_count, provisioning_key, read_generations_checked,
};
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreError};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::{Signal, TenantHash};

/// Default router refresh interval `C` (ADR-0052 section 3, an open question
/// the ADR left to the implementing task). 60 seconds: frequent enough that a
/// reshard's minimum lead of `ceil(C) + 1 = 2` hours leaves a wide margin, and
/// cheap enough that re-reading one small provisioning record per active
/// (tenant, signal) at most once per `C` is negligible. A router whose cached
/// view for a tenant is older than this re-reads the record before routing, and
/// fails the flush closed if the re-read cannot complete.
pub const DEFAULT_REFRESH_INTERVAL_NS: i64 = 60 * 1_000_000_000;

/// Nanoseconds per unix hour, the unit `activation_hour` counts in.
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Wall-clock hour bucket for a `now_ns` reading, clamped to `u32` (the width
/// `activation_hour` uses). A non-positive reading maps to hour 0.
fn hour_of(now_ns: i64) -> u32 {
    u32::try_from(now_ns.div_euclid(NS_PER_HOUR).max(0)).unwrap_or(u32::MAX)
}

/// Ceiling of `ns` in whole hours, mirroring the ADR-0052 section 3 lead-time
/// floor `L >= ceil(C) + 1`. Zero or negative maps to zero hours.
fn ceil_hours(ns: i64) -> u32 {
    if ns <= 0 {
        return 0;
    }
    u32::try_from((ns + NS_PER_HOUR - 1) / NS_PER_HOUR).unwrap_or(u32::MAX)
}

/// The ADR-0052 section 3 minimum reshard lead time `L >= ceil(C) + 1` for a
/// refresh interval `C`, in hours. The grace window reuses
/// this exact bound in reverse: a generation appended after a router's last
/// successful refresh cannot activate within `min_lead_hours(C)` hours of that
/// refresh, so a cached view stays provably authoritative for any wall-clock
/// hour strictly before that horizon, even once the view is older than `C`
/// itself. See [`GenerationSwitch::try_grace_extend`].
fn min_lead_hours(refresh_interval_ns: i64) -> u32 {
    ceil_hours(refresh_interval_ns) + 1
}

/// A tenant's cached view of its provisioning record's generation history, plus
/// when the router last refreshed it. `refreshed_at_ns` is what the staleness
/// guard compares against `C`.
///
/// `last_touched_ns` is a separate stamp used only by idle-tenant eviction
/// (ADR-0069 decision 2): it advances on every access (a cache-hit route as
/// well as a refresh), whereas `refreshed_at_ns` advances only on a refresh.
/// The two differ precisely for a tenant whose view stays fresh under `C` and
/// is hit repeatedly without a re-read: it is not idle, but its
/// `refreshed_at_ns` does not move. Eviction keys off last touch so such a
/// tenant is never evicted mid-use.
#[derive(Debug, Clone)]
struct TenantView {
    generations: Vec<ShardGeneration>,
    refreshed_at_ns: i64,
    last_touched_ns: i64,
}

/// The outcome of consulting the cache for a tenant's write ([`GenerationSwitch::route_cached`]).
pub enum Routed<H> {
    /// A cached view younger than `C` routed the write to this shard-actor set.
    Fresh(Arc<Vec<H>>),
    /// The cached view is older than `C`. The caller must re-read the
    /// provisioning record and call [`GenerationSwitch::refresh`] before
    /// routing, and fail closed if that read cannot complete.
    Stale,
}

/// A provisioning-record read for the live switch could not be trusted, so the
/// flush must fail closed (ADR-0052 section 3): a store error, or a
/// corrupt/undecodable record whose true generation set is unknown. A genuine
/// absence is not an error — it resolves to the default (generation 0) view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GenerationLoadError;

struct Inner<H> {
    /// The count a tenant with no persisted record routes at: the process's
    /// configured `shard_count`, which is generation 0's count for every tenant
    /// (the provisioning record's scalar, ADR-0050 section 5).
    default_count: u32,
    /// One shard-actor set per distinct active `shard_count`, shared across
    /// tenants at that count. Never removed: an old generation's actors drain
    /// and flush on their own (ADR-0052 section 2).
    sets: HashMap<u32, Arc<Vec<H>>>,
    /// Per-tenant cached generation history and last-refresh time.
    views: HashMap<TenantHash, TenantView>,
}

/// The generation-versioned shard-actor topology of one router (ADR-0052
/// sections 2-3). Generic over the router's shard-handle type `H` so the
/// metrics, log, and span routers share one implementation.
pub struct GenerationSwitch<H> {
    inner: Mutex<Inner<H>>,
    /// Builds a fresh shard-actor set of the given size. Called once per
    /// distinct active count. Each call mints a fresh writer identity inside
    /// the router, so two sets never collide on a commit key for the same shard
    /// index.
    factory: Box<dyn Fn(u32) -> Vec<H> + Send + Sync>,
    /// The refresh interval `C`: a cached tenant view older than this is
    /// re-read before routing.
    refresh_interval_ns: i64,
}

impl<H> GenerationSwitch<H> {
    /// Build a switch whose default (generation 0) set has `default_count`
    /// shards, constructed eagerly via `factory` so a never-resharded tenant
    /// routes with no lazy construction on its first write.
    pub fn new(
        default_count: u32,
        refresh_interval_ns: i64,
        factory: impl Fn(u32) -> Vec<H> + Send + Sync + 'static,
    ) -> Self {
        let mut sets = HashMap::new();
        sets.insert(default_count, Arc::new(factory(default_count)));
        GenerationSwitch {
            inner: Mutex::new(Inner {
                default_count,
                sets,
                views: HashMap::new(),
            }),
            factory: Box::new(factory),
            refresh_interval_ns,
        }
    }

    /// The refresh interval `C` this switch enforces.
    pub fn refresh_interval_ns(&self) -> i64 {
        self.refresh_interval_ns
    }

    /// Lock the inner state, recovering the guard if a prior holder panicked.
    /// The inner state is plain maps with no cross-field invariant a panic mid
    /// update could half-break, so recovering the poisoned guard is safe and
    /// keeps routing available rather than propagating a panic (no `expect` on a
    /// production path).
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner<H>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The default (generation 0) shard count, used to resolve a tenant with no
    /// persisted provisioning record.
    pub fn default_count(&self) -> u32 {
        let inner = self.lock();
        inner.default_count
    }

    /// Route a write against the tenant's cached view. Returns [`Routed::Fresh`]
    /// with the active shard-actor set when a view younger than `C` exists, and
    /// [`Routed::Stale`] when the cached view is older than `C`, or when the
    /// tenant has never been resolved from a persisted record, and must be
    /// (re-)read before routing.
    ///
    /// A never-seen tenant cannot be assumed to be at generation 0: the ADR-0052
    /// section 3 safety argument ("a reshard's lead time `L >= ceil(C) + 1`
    /// means every *live* writer refreshes and observes a new generation before
    /// it activates") only covers a process that was already routing before the
    /// append happened. A process that starts after the append — the common
    /// case for the first write this process ever sees for a tenant — has never
    /// read the record and has no basis for assuming generation 0 is still
    /// active; a prior process could have appended and activated a new
    /// generation before this one even started. So a first-touch tenant must go
    /// through the same real store read as a stale one, not a synthesized
    /// default view.
    pub fn route_cached(&self, tenant: TenantHash, now_ns: i64) -> Routed<H> {
        let mut inner = self.lock();
        // Stamp last-touch on a cache hit so idle-tenant eviction (ADR-0069
        // decision 2) never reaps a view that is still routing writes, even one
        // whose `refreshed_at_ns` has not moved since its last re-read. `get_mut`
        // so the stamp is recorded; the borrow ends before `ensure_set`.
        let count = match inner.views.get_mut(&tenant) {
            Some(view)
                if now_ns.saturating_sub(view.refreshed_at_ns) <= self.refresh_interval_ns =>
            {
                view.last_touched_ns = now_ns;
                active_shard_count(&view.generations, hour_of(now_ns))
            }
            Some(_) | None => return Routed::Stale,
        };
        Routed::Fresh(self.ensure_set(&mut inner, count))
    }

    /// Bounded degraded-safe fallback (ADR-0052 sections 2-3):
    /// called by a router whose provisioning re-read could not complete after
    /// [`GenerationSwitch::route_cached`] returned [`Routed::Stale`], so it can
    /// continue routing on the last-known-good view instead of failing every
    /// flush closed for as long as the store stays slow or unreachable — but
    /// only inside a bounded grace window where doing so is still provably
    /// correct, never as a general staleness override.
    ///
    /// Safety predicate: continue-on-stale is safe only when the cached
    /// generation's validity horizon has not been crossed AND no pending
    /// generation change is knowable. Concretely, this returns a set only when
    /// `hour_of(now_ns) < hour_of(view.refreshed_at_ns) + min_lead_hours(C)`.
    /// The ADR's own lead-time floor `L >= ceil(C) + 1` guarantees a generation
    /// appended after `refreshed_at_ns` cannot activate before
    /// `hour_of(refreshed_at_ns) + min_lead_hours(C)`; while `now_ns` maps to an
    /// hour strictly before that horizon, no unseen append could have already
    /// activated, so `active_shard_count` computed against the *cached* history
    /// is exactly what a fresh read would also return — the cached view is not
    /// approximated, it is proven current. A genuine, already-visible future
    /// generation already in the cached history is not a risk either way, since
    /// `active_shard_count` accounts for it regardless of staleness; the only
    /// risk this predicate rules out is an append this router has never seen.
    /// Once the horizon is crossed an unseen append becomes possible and this
    /// returns `None`, so the caller fails closed exactly as it does today —
    /// correctness beats availability once the bound cannot be proven.
    ///
    /// Returns `None` for a tenant never resolved from a real read (no cached
    /// view exists to extend) and once the horizon above is crossed. The
    /// caller is responsible for emitting the "routing on grace-extended stale
    /// view" metric on `Some` and the ordinary stale-flush metric on `None`;
    /// this method only decides, it does not record.
    pub fn try_grace_extend(&self, tenant: TenantHash, now_ns: i64) -> Option<Arc<Vec<H>>> {
        let mut inner = self.lock();
        let refreshed_at_ns = inner.views.get(&tenant)?.refreshed_at_ns;
        let horizon =
            hour_of(refreshed_at_ns).saturating_add(min_lead_hours(self.refresh_interval_ns));
        if hour_of(now_ns) >= horizon {
            return None;
        }
        let count = {
            let view = inner.views.get_mut(&tenant)?;
            view.last_touched_ns = now_ns;
            active_shard_count(&view.generations, hour_of(now_ns))
        };
        Some(self.ensure_set(&mut inner, count))
    }

    /// Install a freshly-read generation history for a tenant, stamp the refresh
    /// time to `now_ns`, and return the active shard-actor set for the write.
    /// Called by the router after a successful re-read, and by the server's
    /// background refresher. An empty history is ignored (the caller passes the
    /// normalized, never-empty history [`ravel_catalog::read_generations`]
    /// returns) and falls back to the default view.
    pub fn refresh(
        &self,
        tenant: TenantHash,
        generations: Vec<ShardGeneration>,
        now_ns: i64,
    ) -> Arc<Vec<H>> {
        let mut inner = self.lock();
        let generations = if generations.is_empty() {
            vec![ShardGeneration {
                generation: 0,
                shard_count: inner.default_count,
                activation_hour: 0,
                appended_unix_ns: 0,
            }]
        } else {
            generations
        };
        let count = active_shard_count(&generations, hour_of(now_ns));
        let set = self.ensure_set(&mut inner, count);
        inner.views.insert(
            tenant,
            TenantView {
                generations,
                refreshed_at_ns: now_ns,
                last_touched_ns: now_ns,
            },
        );
        set
    }

    /// Evict every cached tenant view last touched before `now_ns - ttl_ns`
    /// (ADR-0069 decision 2, idle-tenant state eviction). Returns the number of
    /// views dropped.
    ///
    /// Only the per-tenant `views` map is swept: the `sets` map holds one
    /// shard-actor set per distinct active `shard_count`, shared across every
    /// tenant at that count and drained on its own (ADR-0052 section 2), so it
    /// is topology, not per-tenant idle state, and is never touched here.
    ///
    /// Evicting a view is safe because it is re-derivable: the next
    /// [`GenerationSwitch::route_cached`] for that tenant returns
    /// [`Routed::Stale`] exactly as a first-touch tenant does, so the router
    /// re-reads the provisioning record and refreshes before routing. The cost
    /// is one provisioning-record read on the tenant's next write, bounded and
    /// rare (ADR-0069 consequences).
    ///
    /// `ttl_ns` is the injected idle threshold and `now_ns` the injected clock
    /// reading; this type never reads a clock (the caller's sweep loop supplies
    /// both), so eviction is deterministic under test.
    pub fn evict_idle(&self, now_ns: i64, ttl_ns: i64) -> usize {
        let mut inner = self.lock();
        let before = inner.views.len();
        inner
            .views
            .retain(|_, view| now_ns.saturating_sub(view.last_touched_ns) <= ttl_ns);
        before - inner.views.len()
    }

    /// Every live shard-actor set, for the router's `flush_all`/`shutdown`
    /// fan-out. Includes retiring generations so their buffers flush too.
    pub fn all_sets(&self) -> Vec<Arc<Vec<H>>> {
        let inner = self.lock();
        inner.sets.values().cloned().collect()
    }

    /// Get or lazily construct the shard-actor set for `count`.
    fn ensure_set(&self, inner: &mut Inner<H>, count: u32) -> Arc<Vec<H>> {
        if let Some(set) = inner.sets.get(&count) {
            return Arc::clone(set);
        }
        let set = Arc::new((self.factory)(count));
        inner.sets.insert(count, Arc::clone(&set));
        set
    }
}

/// Read and decode the current shard-generation history for one (tenant,
/// signal) for a live-switch refresh (ADR-0052 section 3). A genuine absence
/// (`NotFound`) resolves to the single implicit generation 0 at `default_count`
/// — a tenant whose record has not been written yet routes at the configured
/// count. A store error, an undecodable/corrupt record, a record from a future
/// format version, or a record misfiled for a different tenant or signal is a
/// [`GenerationLoadError`]: the caller fails the flush closed rather than route
/// on an untrusted view. Uses [`read_generations_checked`], not
/// [`read_generations`] directly, because this path reads the record straight
/// off the store rather than through [`ravel_catalog::validate_or_adopt`],
/// which is where that guard normally lives.
pub(crate) async fn load_generations(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    tenant: &TenantHash,
    default_count: u32,
) -> Result<Vec<ShardGeneration>, GenerationLoadError> {
    let key = provisioning_key(tenant, signal);
    match store.get(&key, GetRange::Full).await {
        Ok(outcome) => {
            let record = sysproto::ProvisioningRecord::decode(outcome.data.as_ref())
                .map_err(|_| GenerationLoadError)?;
            read_generations_checked(&record, &key, tenant, signal).map_err(|_| GenerationLoadError)
        }
        Err(StoreError::NotFound) => Ok(vec![ShardGeneration {
            generation: 0,
            shard_count: default_count,
            activation_hour: 0,
            appended_unix_ns: 0,
        }]),
        Err(_) => Err(GenerationLoadError),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn gen0(count: u32) -> ShardGeneration {
        ShardGeneration {
            generation: 0,
            shard_count: count,
            activation_hour: 0,
            appended_unix_ns: 0,
        }
    }

    fn gen1(count: u32, activation_hour: u32) -> ShardGeneration {
        ShardGeneration {
            generation: 1,
            shard_count: count,
            activation_hour,
            appended_unix_ns: 0,
        }
    }

    fn index_switch(default_count: u32, c_ns: i64) -> GenerationSwitch<u32> {
        GenerationSwitch::new(default_count, c_ns, |count| (0..count).collect())
    }

    fn tenant(byte: u8) -> TenantHash {
        TenantHash([byte; 16])
    }

    /// A never-seen tenant reports Stale on its first write: this process has
    /// never read the tenant's provisioning record, so it cannot assume
    /// generation 0 is still active (a prior process could have appended and
    /// activated a reshard before this one started). The caller must do a real
    /// store read via `load_generations` and `refresh` before routing.
    #[test]
    fn first_touch_tenant_is_stale_and_requires_a_real_read() {
        let sw = index_switch(4, DEFAULT_REFRESH_INTERVAL_NS);
        assert!(matches!(
            sw.route_cached(tenant(1), 10 * NS_PER_HOUR),
            Routed::Stale
        ));
        // After the caller refreshes from the (in this case NotFound-mapped)
        // record, routing resumes from cache at the default count.
        let set = sw.refresh(tenant(1), vec![gen0(4)], 10 * NS_PER_HOUR);
        assert_eq!(set.len(), 4);
    }

    /// The live switch: after refreshing to a history whose latest active
    /// generation is 8 shards, routing lands on the count-8 set, while the
    /// count-4 set handed out before the switch stays valid (an in-flight write
    /// on the old generation completes; the old actors are not force-closed).
    #[test]
    fn refresh_switches_routing_and_old_set_survives() {
        let sw = index_switch(4, DEFAULT_REFRESH_INTERVAL_NS);
        let t = tenant(2);
        let history = vec![gen0(4), gen1(8, 100)];

        // Before activation (hour 50): active count is still 4.
        let before_ns = 50 * NS_PER_HOUR;
        let old_set = sw.refresh(t, history.clone(), before_ns);
        assert_eq!(old_set.len(), 4);

        // At activation (hour 100): active count becomes 8.
        let after_ns = 100 * NS_PER_HOUR;
        let new_set = sw.refresh(t, history, after_ns);
        assert_eq!(new_set.len(), 8);

        // The set handed out before the switch is still alive and usable.
        assert_eq!(
            old_set.len(),
            4,
            "the old generation's set survives the switch"
        );
    }

    /// A cached view younger than `C` routes from cache; once it ages past `C`
    /// the switch reports `Stale` so the router re-reads before routing.
    #[test]
    fn cached_view_goes_stale_past_c() {
        let c = DEFAULT_REFRESH_INTERVAL_NS;
        let sw = index_switch(4, c);
        let t = tenant(3);
        let t0 = 10 * NS_PER_HOUR;
        sw.refresh(t, vec![gen0(4)], t0);

        // Just within C: routes from cache.
        assert!(
            matches!(sw.route_cached(t, t0 + c), Routed::Fresh(_)),
            "within C routes from cache"
        );
        // Past C: stale, must re-read.
        assert!(
            matches!(sw.route_cached(t, t0 + c + 1), Routed::Stale),
            "past C reports stale"
        );
        // A refresh clears staleness.
        sw.refresh(t, vec![gen0(4)], t0 + c + 1);
        assert!(matches!(sw.route_cached(t, t0 + c + 1), Routed::Fresh(_)));
    }

    /// ADR-0069 decision 2: an idle tenant's generation view is
    /// evicted once it has gone untouched past the TTL, its next access
    /// re-derives it by re-reading the provisioning record and succeeds, and a
    /// tenant still being written survives the same sweep.
    ///
    /// Deterministic via the injected `now_ns`: no clock is read anywhere on
    /// this path. The re-read step drives the real [`load_generations`] free
    /// function against a `MemoryStore` (a `NotFound` record resolves to the
    /// implicit generation 0, a genuine successful re-read), then `refresh`
    /// reinstalls the view exactly as the router's `active_set` does after a
    /// `Routed::Stale`.
    #[tokio::test]
    async fn idle_tenant_state_evicted_and_rederived() {
        use ravel_object_store::memory::MemoryStore;

        let ttl_ns = 100 * NS_PER_HOUR;
        // A refresh interval `C` far larger than the whole test window, so a
        // cached view never goes stale-by-`C`: this isolates the last-touch
        // refinement (which advances on a cache-hit route, not only a refresh)
        // as the sole thing keeping the active tenant alive across the sweep.
        let big_c = 1_000_000 * NS_PER_HOUR;
        let sw = index_switch(4, big_c);
        let idle = tenant(1);
        let active = tenant(2);

        // Both tenants routed at t0: each gets a cached view.
        let t0 = 1_000 * NS_PER_HOUR;
        sw.refresh(idle, vec![gen0(4)], t0);
        sw.refresh(active, vec![gen0(4)], t0);

        // The active tenant keeps writing: a cache-hit route just before the
        // sweep advances its last-touch even though its `refreshed_at_ns` (t0)
        // does not move. This is the case last-touch tracking exists to protect.
        let sweep_ns = t0 + ttl_ns + 1;
        assert!(
            matches!(sw.route_cached(active, sweep_ns), Routed::Fresh(_)),
            "the active tenant routes from cache right before the sweep"
        );

        // Sweep: the idle tenant is past the TTL (last touched at t0), the
        // active tenant was just touched, so exactly one view is evicted.
        let evicted = sw.evict_idle(sweep_ns, ttl_ns);
        assert_eq!(evicted, 1, "only the idle tenant's view is evicted");

        // The active tenant's view survives: still routes from cache.
        assert!(
            matches!(sw.route_cached(active, sweep_ns), Routed::Fresh(_)),
            "the active tenant's view survives the sweep"
        );
        // The idle tenant's view is gone: it now reports Stale exactly like a
        // first-touch tenant, which is the trigger for the router to re-read.
        assert!(
            matches!(sw.route_cached(idle, sweep_ns), Routed::Stale),
            "the evicted view forces a re-read on the next access"
        );

        // Re-derive: the router's stale path re-reads the provisioning record
        // via `load_generations` and refreshes. The record is absent, which is
        // a successful read resolving to the implicit generation 0.
        let store = MemoryStore::new();
        let generations = load_generations(&store, Signal::Metrics, &idle, sw.default_count())
            .await
            .expect("re-read of the provisioning record succeeds");
        let set = sw.refresh(idle, generations, sweep_ns);
        assert_eq!(
            set.len(),
            4,
            "the re-derived view routes to the default set"
        );
        assert!(
            matches!(sw.route_cached(idle, sweep_ns), Routed::Fresh(_)),
            "after re-derivation the idle tenant routes from cache again"
        );
    }

    /// `min_lead_hours` mirrors the ADR-0052 section 3 lead-time floor `L >=
    /// ceil(C) + 1` for a range of refresh intervals, including the default and
    /// a non-hour-aligned one, and is deterministic (repeat calls agree).
    #[test]
    fn min_lead_hours_matches_ceil_c_plus_one() {
        assert_eq!(min_lead_hours(0), 1, "C = 0 still requires 1 hour of lead");
        assert_eq!(min_lead_hours(DEFAULT_REFRESH_INTERVAL_NS), 2, "C = 60s");
        assert_eq!(
            min_lead_hours(NS_PER_HOUR + 1),
            3,
            "C just over 1h ceils to 2h, plus 1"
        );
        assert_eq!(
            min_lead_hours(DEFAULT_REFRESH_INTERVAL_NS),
            min_lead_hours(DEFAULT_REFRESH_INTERVAL_NS),
            "deterministic: no clock read, repeat calls agree"
        );
    }

    /// A router whose cached view has gone stale by `C` but
    /// whose `shard_count` provably has not (and cannot have) changed continues
    /// routing inside the bounded grace window, rather than failing every flush
    /// closed. `min_lead_hours(C) = 2` for the default `C`, so the grace window
    /// here spans wall-clock hours `[10, 12)` from a refresh at hour 10.
    #[test]
    fn try_grace_extend_continues_routing_within_horizon() {
        let c = DEFAULT_REFRESH_INTERVAL_NS;
        let sw = index_switch(4, c);
        let t = tenant(5);
        let t0 = 10 * NS_PER_HOUR;
        sw.refresh(t, vec![gen0(4)], t0);

        // Past C (a store re-read could not complete), but still inside the
        // horizon: continues routing on the cached view.
        let still_within_horizon_ns = 11 * NS_PER_HOUR;
        let set = sw
            .try_grace_extend(t, still_within_horizon_ns)
            .expect("within the horizon, grace-extension continues routing");
        assert_eq!(
            set.len(),
            4,
            "routes with the cached, unchanged shard_count"
        );
    }

    /// Once the grace horizon is crossed, an unseen generation append
    /// becomes possible, so the switch refuses to extend and the caller must
    /// fail the flush closed exactly as it did before this fallback existed.
    #[test]
    fn try_grace_extend_refuses_once_horizon_crossed() {
        let c = DEFAULT_REFRESH_INTERVAL_NS;
        let sw = index_switch(4, c);
        let t = tenant(6);
        let t0 = 10 * NS_PER_HOUR;
        sw.refresh(t, vec![gen0(4)], t0);

        // Horizon is hour 10 + min_lead_hours(C) = hour 12. At hour 12 an
        // append this router never saw could already have activated, so no
        // amount of "shard_count looks unchanged in the cached view" is provable
        // any more.
        let horizon_crossed_ns = 12 * NS_PER_HOUR;
        assert!(
            sw.try_grace_extend(t, horizon_crossed_ns).is_none(),
            "past the horizon, grace-extension must refuse (fail closed)"
        );
    }

    /// A tenant never resolved from a real read has no cached view to
    /// extend, so grace-extension cannot help it either -- it must go through
    /// the normal re-read (and fail closed if that fails), identically to a
    /// tenant whose view has gone stale.
    #[test]
    fn try_grace_extend_refuses_for_a_never_resolved_tenant() {
        let sw = index_switch(4, DEFAULT_REFRESH_INTERVAL_NS);
        let t = tenant(7);
        assert!(
            sw.try_grace_extend(t, 10 * NS_PER_HOUR).is_none(),
            "no cached view exists yet, so there is nothing provable to extend"
        );
    }

    /// Grace-extension is exactly as safe when a future generation change
    /// is already visible in the cached view -- `active_shard_count` accounts
    /// for it regardless of staleness, so routing through the grace window on a
    /// tenant with a known upcoming reshard still returns the count that will
    /// actually be active at `now_ns`, not a stale pre-reshard count.
    #[test]
    fn try_grace_extend_honors_a_known_future_generation() {
        let c = DEFAULT_REFRESH_INTERVAL_NS;
        let sw = index_switch(4, c);
        let t = tenant(8);
        let t0 = 10 * NS_PER_HOUR;
        // The cached view already knows about a reshard to 8 shards activating
        // at hour 11, refreshed at hour 10.
        sw.refresh(t, vec![gen0(4), gen1(8, 11)], t0);

        // Still within the grace horizon (hour 10 + 2 = 12), and past the known
        // activation: routes at the new count, not the stale old one.
        let after_known_activation_ns = 11 * NS_PER_HOUR;
        let set = sw
            .try_grace_extend(t, after_known_activation_ns)
            .expect("within the horizon, grace-extension continues routing");
        assert_eq!(
            set.len(),
            8,
            "a known future generation is honored even through the grace window"
        );
    }

    /// `all_sets` exposes every live generation's set for the flush/shutdown
    /// fan-out, including a retired one.
    #[test]
    fn all_sets_includes_every_generation() {
        let sw = index_switch(4, DEFAULT_REFRESH_INTERVAL_NS);
        let t = tenant(4);
        sw.refresh(t, vec![gen0(4), gen1(8, 100)], 100 * NS_PER_HOUR);
        let lens: std::collections::BTreeSet<usize> =
            sw.all_sets().iter().map(|s| s.len()).collect();
        assert!(lens.contains(&4), "the default generation set is present");
        assert!(lens.contains(&8), "the activated generation set is present");
    }
}
