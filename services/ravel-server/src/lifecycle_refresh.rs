//! Durable tenant-lifecycle refresh: auth without restart, plus the
//! admission-limit refresh loop (ADR-0066 decision 6, epic EM, EM-T8).
//!
//! This is the server-side *lifecycle* half of the bounded-staleness pattern
//! whose *mechanism* lives in `ravel-ingest` ([`ravel_ingest::StalenessGate`],
//! [`ravel_ingest::refresh_tenant_limits_once`]) -- the same split
//! [`crate::admission_reconcile`] uses over [`ravel_ingest::reconcile_once`].
//! It owns:
//!
//! - [`DurableAuthState`] + [`DurableBearerResolver`]: a [`TenantResolver`] that
//!   resolves an `Authorization: Bearer <token>` against the durable `sys/auth`
//!   keyed-hash token map (EM-T7), read on the bounded-staleness horizon with a
//!   rate-limited on-miss re-read so a freshly provisioned token authenticates
//!   within seconds. Revocation is horizon-bounded in the other direction (a
//!   removed token can still authenticate for up to one horizon), the
//!   documented, accepted tradeoff of ADR-0066 decision 6. Past a hard multiple
//!   of the horizon with no successful refresh, the resolver fails closed
//!   ([`ravel_ingest::StalenessGate::hard_stale`]), mirroring ADR-0052's
//!   prov-staleness rule.
//! - [`spawn`]: the background loop that refreshes the auth map on the horizon
//!   (and on an on-miss signal) and re-invokes the admission controller's
//!   `set_tenant_limits` from each tenant's durable config record, all off the
//!   hot path.
//!
//! The request path never does I/O: [`DurableBearerResolver::resolve`] reads
//! only the in-memory cached map this loop maintains, exactly as the OIDC
//! resolver reads the cache the JWKS loop writes ([`crate::tenant`]).

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use axum::http::HeaderMap;
use ravel_catalog::{AuthTokenMap, AuthTokenMapError, read_auth_map, tenant_for_token};
use ravel_ingest::{
    AdmissionController, AdmissionLimits, DEFAULT_HARD_STALENESS_MULTIPLE,
    DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS, StalenessGate, SystemClock, refresh_tenant_limits_once,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::http::{AuthError, TenantResolver};
use ravel_types::TenantId;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;

use ravel_ingest::Clock as _;

use crate::fold::jittered;

/// Default on-miss re-read rate limit: 1 second. A token miss requests an
/// immediate off-horizon re-read (so a freshly provisioned token authenticates
/// within seconds, not a full horizon), but no more than once per this interval,
/// so a burst of unknown-token requests (or an OIDC JWT falling through to this
/// resolver) cannot drive a re-read storm. Chosen an order of magnitude below
/// the refresh horizon so the "seconds, not a horizon" property holds.
pub const DEFAULT_ON_MISS_INTERVAL_NS: i64 = 1_000_000_000;

/// The on-miss window must stay well under the refresh horizon, or the "a
/// freshly provisioned token authenticates within seconds, not a full horizon"
/// property this module exists to provide would not hold. Enforced at compile
/// time so a future retune of either constant that broke it fails the build.
const _: () = assert!(DEFAULT_ON_MISS_INTERVAL_NS < DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS);

/// The outcome of resolving one bearer token against the cached auth map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthResolution {
    /// The token resolved to this tenant from a trusted (not-hard-stale) map.
    Resolved(TenantId),
    /// The token is not in the cached map. The caller requests an on-miss
    /// re-read (a freshly provisioned token is picked up within the on-miss
    /// window) and, for this attempt, refuses (the chain falls through).
    Miss,
    /// The cached map is older than the hard staleness bound and can no longer
    /// be trusted, so even a token present in it is refused (ADR-0066 decision
    /// 6, fail closed). Distinct from [`Miss`](Self::Miss) so the caller can
    /// count it separately.
    StaleFailClosed,
}

/// Whether a refresh found a `sys/auth` object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthRefresh {
    /// A `sys/auth` object was read and cached.
    Present,
    /// No `sys/auth` object exists (a deployment that has provisioned no durable
    /// tokens yet, or uses only OIDC/mTLS). A valid, fresh state -- the cache
    /// becomes an empty map and the staleness clock is reset.
    Absent,
}

/// Pure resolution over an already-loaded map and staleness gate (ADR-0066
/// decision 6). No I/O and no side effects, so it is exhaustively unit-testable:
/// the hard-stale check comes first (a hard-stale process refuses every token,
/// even one present in the cached map), then the keyed-hash lookup.
fn resolve_over(
    map: Option<&AuthTokenMap>,
    gate: &StalenessGate,
    deployment_key: &[u8; 32],
    token: &[u8],
    now_ns: i64,
) -> AuthResolution {
    if gate.hard_stale(now_ns) {
        return AuthResolution::StaleFailClosed;
    }
    match map.and_then(|m| tenant_for_token(m, deployment_key, token)) {
        Some(tenant_id) => AuthResolution::Resolved(TenantId::new(tenant_id)),
        None => AuthResolution::Miss,
    }
}

/// The shared, background-refreshed state behind [`DurableBearerResolver`]: the
/// cached `sys/auth` map, its staleness gate, and the on-miss re-read
/// rate-limiter. Held in an `Arc`, shared between the sync request-path resolver
/// and the async refresh loop.
pub struct DurableAuthState {
    store: Arc<dyn ObjectStoreBackend>,
    deployment_key: [u8; 32],
    /// The last-known-good decoded map. `None` until the first successful
    /// refresh; an [`AuthRefresh::Absent`] refresh installs an empty map.
    map: RwLock<Option<AuthTokenMap>>,
    gate: StalenessGate,
    on_miss_interval_ns: i64,
    last_on_miss_ns: AtomicI64,
    /// Pinged by the request path on a token miss; awaited by the refresh loop
    /// so a freshly provisioned token triggers a bounded off-horizon re-read.
    miss: Notify,
    refresh_failures: AtomicU64,
    on_miss_rereads: AtomicU64,
    stale_fail_closed: AtomicU64,
}

impl DurableAuthState {
    /// Build the state with an explicit horizon, hard multiple, and on-miss
    /// interval, seeded fresh at `now_ns`. The map starts empty (`None`) until
    /// the first refresh.
    pub fn new(
        store: Arc<dyn ObjectStoreBackend>,
        deployment_key: [u8; 32],
        horizon_ns: i64,
        hard_multiple: i64,
        on_miss_interval_ns: i64,
        now_ns: i64,
    ) -> Self {
        DurableAuthState {
            store,
            deployment_key,
            map: RwLock::new(None),
            gate: StalenessGate::new(horizon_ns, hard_multiple, now_ns),
            on_miss_interval_ns: on_miss_interval_ns.max(1),
            last_on_miss_ns: AtomicI64::new(i64::MIN),
            miss: Notify::new(),
            refresh_failures: AtomicU64::new(0),
            on_miss_rereads: AtomicU64::new(0),
            stale_fail_closed: AtomicU64::new(0),
        }
    }

    /// Build the state with the default horizon, hard multiple, and on-miss
    /// interval, seeded fresh at `now_ns`.
    pub fn with_defaults(
        store: Arc<dyn ObjectStoreBackend>,
        deployment_key: [u8; 32],
        now_ns: i64,
    ) -> Self {
        DurableAuthState::new(
            store,
            deployment_key,
            DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS,
            DEFAULT_HARD_STALENESS_MULTIPLE,
            DEFAULT_ON_MISS_INTERVAL_NS,
            now_ns,
        )
    }

    fn read_map(&self) -> std::sync::RwLockReadGuard<'_, Option<AuthTokenMap>> {
        self.map.read().unwrap_or_else(|p| p.into_inner())
    }

    /// Resolve one bearer token against the cached map, applying the staleness
    /// gate and, on a miss, requesting an on-miss re-read. Called by
    /// [`DurableBearerResolver::resolve`] with `SystemClock.now_ns()`.
    pub fn resolve_token(&self, token: &[u8], now_ns: i64) -> AuthResolution {
        let outcome = {
            let guard = self.read_map();
            resolve_over(
                guard.as_ref(),
                &self.gate,
                &self.deployment_key,
                token,
                now_ns,
            )
        };
        match &outcome {
            AuthResolution::Miss => self.miss.notify_one(),
            AuthResolution::StaleFailClosed => {
                self.stale_fail_closed.fetch_add(1, Ordering::Relaxed);
            }
            AuthResolution::Resolved(_) => {}
        }
        outcome
    }

    /// Read `sys/auth` and replace the cached map. On success (present or
    /// genuinely absent) the staleness clock is reset; on a store, decode,
    /// wrong-key, or corruption error the last-known map is kept and the gate is
    /// NOT advanced, so a sustained inability to refresh eventually drives the
    /// gate hard-stale and the resolver fails closed (the last-known-value
    /// discipline of ADR-0057, with the fail-closed backstop of ADR-0052).
    pub async fn refresh(&self, now_ns: i64) -> Result<AuthRefresh, AuthTokenMapError> {
        match read_auth_map(self.store.as_ref(), &self.deployment_key).await {
            Ok(Some((map, _version))) => {
                *self.map.write().unwrap_or_else(|p| p.into_inner()) = Some(map);
                self.gate.record_success(now_ns);
                Ok(AuthRefresh::Present)
            }
            Ok(None) => {
                *self.map.write().unwrap_or_else(|p| p.into_inner()) = Some(AuthTokenMap {
                    key_fingerprint: ravel_catalog::key_fingerprint(&self.deployment_key),
                    entries: Vec::new(),
                });
                self.gate.record_success(now_ns);
                Ok(AuthRefresh::Absent)
            }
            Err(err) => {
                self.refresh_failures.fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    /// Whether an on-miss re-read is allowed now (its last one was at least
    /// `on_miss_interval_ns` ago), recording the attempt when it is. Rate-limits
    /// off-horizon re-reads so a burst of unknown-token requests cannot storm
    /// the store. Called only by the refresh loop.
    pub fn try_begin_on_miss_reread(&self, now_ns: i64) -> bool {
        let last = self.last_on_miss_ns.load(Ordering::Relaxed);
        if now_ns.saturating_sub(last) < self.on_miss_interval_ns {
            return false;
        }
        self.last_on_miss_ns.store(now_ns, Ordering::Relaxed);
        self.on_miss_rereads.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Await the next token-miss signal (used by the refresh loop's `select!`).
    async fn wait_for_miss(&self) {
        self.miss.notified().await;
    }

    /// The distinct tenant ids in the cached map, for the limits refresh's known
    /// set. Empty when no map has been loaded yet.
    pub fn cached_tenant_ids(&self) -> Vec<TenantId> {
        self.read_map()
            .as_ref()
            .map(|m| {
                m.entries
                    .iter()
                    .map(|e| TenantId::new(e.tenant_id.as_str()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Count of failed refreshes (store/decode/wrong-key/corruption errors).
    pub fn refresh_failures(&self) -> u64 {
        self.refresh_failures.load(Ordering::Relaxed)
    }

    /// Count of off-horizon on-miss re-reads actually begun (post rate-limit).
    pub fn on_miss_rereads(&self) -> u64 {
        self.on_miss_rereads.load(Ordering::Relaxed)
    }

    /// Count of resolutions refused because the cached map was hard-stale
    /// (ADR-0066 decision 6 fail-closed). The staleness metric.
    pub fn stale_fail_closed(&self) -> u64 {
        self.stale_fail_closed.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    fn install_map(&self, map: AuthTokenMap, now_ns: i64) {
        *self.map.write().unwrap_or_else(|p| p.into_inner()) = Some(map);
        self.gate.record_success(now_ns);
    }
}

/// Reads the `Authorization: Bearer <token>` header, returning the raw token
/// bytes or `None` for a missing, non-ASCII, or non-`Bearer` header. Kept in
/// step with `ravel_query::http`'s own private `bearer_token` (its parser is not
/// exported), so this resolver claims exactly the requests the static bearer and
/// OIDC resolvers do.
fn bearer_token(headers: &HeaderMap) -> Option<Vec<u8>> {
    let raw = headers.get(axum::http::header::AUTHORIZATION)?;
    let raw = raw.to_str().ok()?;
    raw.strip_prefix("Bearer ").map(|t| t.as_bytes().to_vec())
}

/// A [`TenantResolver`] backed by the durable `sys/auth` map (ADR-0066 decision
/// 6). Resolves synchronously against the in-memory cache the refresh loop
/// maintains; does no I/O on the request path. A miss or a hard-stale cache is
/// an [`AuthError`], so in the [`crate::tenant::FallbackResolver`] chain the
/// request falls through to the next resolver (a hard-stale durable map never
/// swallows a request the static bearer or OIDC resolver could still answer).
pub struct DurableBearerResolver {
    state: Arc<DurableAuthState>,
}

impl DurableBearerResolver {
    pub fn new(state: Arc<DurableAuthState>) -> Self {
        DurableBearerResolver { state }
    }
}

impl TenantResolver for DurableBearerResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        let token = bearer_token(headers).ok_or(AuthError)?;
        match self.state.resolve_token(&token, SystemClock.now_ns()) {
            AuthResolution::Resolved(tenant) => Ok(tenant),
            AuthResolution::Miss | AuthResolution::StaleFailClosed => Err(AuthError),
        }
    }
}

/// Inputs for the admission-limits half of the refresh loop (ADR-0066 decision
/// 6): the controller to re-invoke `set_tenant_limits` on, the deployment
/// default limits, and the startup `--limits-file` per-tenant overrides that
/// form each tenant's base before the durable record overlays on top.
pub struct LimitsRefresh {
    pub admission: Arc<AdmissionController>,
    pub defaults: AdmissionLimits,
    pub tenant_overrides: std::collections::HashMap<TenantId, AdmissionLimits>,
}

impl LimitsRefresh {
    /// The base limits a tenant's durable config overlays on: its
    /// `--limits-file` override if it has one, else the deployment default. A
    /// durable record that clears an override therefore reverts the tenant to
    /// its startup value, not to the global default.
    fn base_for(&self, tenant: &TenantId) -> AdmissionLimits {
        self.tenant_overrides
            .get(tenant)
            .copied()
            .unwrap_or(self.defaults)
    }
}

/// Handle to the spawned refresh task, for clean shutdown (mirrors
/// [`crate::admission_reconcile::AdmissionReconcileTask`]).
pub struct LifecycleRefreshTask {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl LifecycleRefreshTask {
    pub fn none() -> Self {
        LifecycleRefreshTask {
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

/// Run one full refresh cycle: refresh the durable auth map (if any), then
/// re-invoke `set_tenant_limits` from every known tenant's durable config
/// record. The known set is the static `--limits-file` tenants unioned with the
/// tenants named in the freshly refreshed auth map, so a durably provisioned
/// tenant's limit overrides are applied without a restart. Every failure is
/// logged and retried next cycle; nothing here touches the hot path.
async fn refresh_cycle(
    auth: Option<&DurableAuthState>,
    store: &dyn ObjectStoreBackend,
    limits: Option<&LimitsRefresh>,
    now_ns: i64,
) {
    if let Some(auth) = auth {
        match auth.refresh(now_ns).await {
            Ok(_) => tracing::debug!("durable auth map refresh complete"),
            Err(err) => tracing::warn!(
                error = %err,
                "durable auth map refresh failed; keeping last-known map (fails closed past the \
                 hard staleness bound, ADR-0066 decision 6)"
            ),
        }
    }

    if let Some(limits) = limits {
        // Static limits-file tenants unioned with the durably provisioned ones
        // named in the auth map, deduplicated and ordered so the cycle is
        // deterministic.
        let mut known: BTreeSet<TenantId> = limits.tenant_overrides.keys().cloned().collect();
        if let Some(auth) = auth {
            known.extend(auth.cached_tenant_ids());
        }
        for tenant in known {
            let base = limits.base_for(&tenant);
            if let Err(err) =
                refresh_tenant_limits_once(limits.admission.as_ref(), store, &tenant, base).await
            {
                tracing::warn!(
                    tenant_hash = %tenant.hash().to_hex(),
                    error = %err,
                    "tenant config-limits refresh failed; keeping last-applied limits, retried \
                     next cycle"
                );
            }
        }
    }
}

/// Spawn the durable lifecycle refresh loop (ADR-0066 decision 6). Returns
/// immediately; the task runs until [`LifecycleRefreshTask::shutdown`]. The loop
/// refreshes on the (jittered) horizon and additionally on a token-miss signal
/// (rate-limited), so a freshly provisioned token authenticates within the
/// on-miss window rather than waiting a full horizon. `store` is the same handle
/// the auth state reads; passing it separately lets the limits refresh run even
/// on an unkeyed bucket with no `auth` state.
pub fn spawn(
    auth: Option<Arc<DurableAuthState>>,
    store: Arc<dyn ObjectStoreBackend>,
    limits: Option<LimitsRefresh>,
    interval: Duration,
) -> LifecycleRefreshTask {
    if auth.is_none() && limits.is_none() {
        return LifecycleRefreshTask::none();
    }
    let (tx, mut rx) = oneshot::channel();
    let handle = tokio::spawn(async move {
        // The horizon deadline is held OUTSIDE the select!, not rebuilt as a
        // fresh `sleep(interval)` on every iteration. The miss arm below fires
        // on every unknown-token request (the rate limiter gates the re-read,
        // not the wakeup), and this resolver sits last in the fallback chain, so
        // it sees every bearer token no other resolver could answer. A
        // re-created timer would be reset by each of those wakeups, and a single
        // misconfigured agent (or any scanner) retrying more often than once per
        // horizon would starve the horizon arm forever, so the durable
        // per-tenant limits refresh would never run.
        let mut next_refresh = tokio::time::Instant::now() + jittered(interval);
        loop {
            tokio::select! {
                _ = tokio::time::sleep_until(next_refresh) => {
                    next_refresh = tokio::time::Instant::now() + jittered(interval);
                    let now_ns = SystemClock.now_ns();
                    refresh_cycle(auth.as_deref(), store.as_ref(), limits.as_ref(), now_ns).await;
                }
                _ = wait_for_optional_miss(auth.as_deref()) => {
                    // A token miss: do a rate-limited off-horizon re-read of the
                    // auth map only (a freshly provisioned token is picked up in
                    // seconds). Limits are not on-miss driven -- a fresh config
                    // taking up to a horizon is the documented, accepted grant
                    // latency (ADR-0066 decision 6).
                    if let Some(auth) = auth.as_deref() {
                        let now_ns = SystemClock.now_ns();
                        if auth.try_begin_on_miss_reread(now_ns) {
                            match auth.refresh(now_ns).await {
                                Ok(_) => tracing::debug!(
                                    "durable auth on-miss re-read complete"
                                ),
                                Err(err) => tracing::warn!(
                                    error = %err,
                                    "durable auth on-miss re-read failed; retried next signal or horizon"
                                ),
                            }
                        }
                    }
                }
                _ = &mut rx => return,
            }
        }
    });
    LifecycleRefreshTask {
        shutdown: Some(tx),
        handle: Some(handle),
    }
}

/// Await the next token-miss signal when a durable auth state exists, else a
/// future that never completes (so the `select!` arm is inert without `auth`).
async fn wait_for_optional_miss(auth: Option<&DurableAuthState>) {
    match auth {
        Some(auth) => auth.wait_for_miss().await,
        None => std::future::pending::<()>().await,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_catalog::{read_auth_map, upsert_token};
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    const KEY: [u8; 32] = [0x42u8; 32];
    const HORIZON_NS: i64 = DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS;
    const ON_MISS_NS: i64 = DEFAULT_ON_MISS_INTERVAL_NS;

    fn state_over(store: Arc<dyn ObjectStoreBackend>, now_ns: i64) -> DurableAuthState {
        DurableAuthState::new(store, KEY, HORIZON_NS, 3, ON_MISS_NS, now_ns)
    }

    /// THE deliverable-2 test: a freshly provisioned token authenticates within
    /// the on-miss rate-limited window, NOT a full horizon. A token is unknown
    /// at first (a miss), then provisioned in `sys/auth`; the on-miss re-read
    /// the miss triggers -- allowed after only `ON_MISS_NS`, an order of
    /// magnitude below the horizon -- picks it up, and it then resolves.
    #[tokio::test]
    async fn freshly_provisioned_token_authenticates_within_the_on_miss_window() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let t0 = 1_000 * HORIZON_NS;
        let state = state_over(store.clone(), t0);

        // First refresh over an empty bucket: an absent map, fresh.
        assert_eq!(
            state.refresh(t0).await.expect("refresh"),
            AuthRefresh::Absent
        );

        // The token is unknown: a miss, which requests an on-miss re-read.
        assert_eq!(state.resolve_token(b"tok", t0), AuthResolution::Miss);

        // The operator provisions the token in sys/auth.
        upsert_token(store.as_ref(), &KEY, b"tok", "acme", t0)
            .await
            .expect("provision token");

        // The on-miss re-read is allowed only ON_MISS_NS after the last one
        // (here, after the seed), far short of a full horizon.
        let t_reread = t0 + ON_MISS_NS;
        assert!(
            state.try_begin_on_miss_reread(t_reread),
            "an on-miss re-read is allowed once the on-miss window has elapsed"
        );
        // A second immediate attempt is rate-limited (no re-read storm).
        assert!(
            !state.try_begin_on_miss_reread(t_reread),
            "a second on-miss re-read inside the window is rate-limited"
        );
        state.refresh(t_reread).await.expect("on-miss re-read");

        // The freshly provisioned token now resolves -- within ON_MISS_NS of
        // being provisioned, not a full horizon later.
        assert_eq!(
            state.resolve_token(b"tok", t_reread),
            AuthResolution::Resolved(TenantId::new("acme"))
        );
        assert_eq!(
            state.on_miss_rereads(),
            1,
            "exactly one on-miss re-read ran"
        );
        assert!(
            t_reread - t0 < HORIZON_NS,
            "the token authenticated within the on-miss window, well under one horizon"
        );
    }

    /// THE deliverable-4 test: past a hard multiple of the horizon with refresh
    /// genuinely unable to succeed (a store fault), the resolver fails closed --
    /// a token it previously accepted is refused rather than served from a map
    /// too stale to trust (ADR-0066 decision 6, mirroring ADR-0052's
    /// prov-staleness fail-closed rule).
    #[tokio::test]
    async fn hard_stale_auth_fails_closed_when_refresh_cannot_succeed() {
        // Seed the token in an inner store, decode the map, then wrap the store
        // in a fault that fails every sys/auth GET so refresh can never succeed.
        let inner = MemoryStore::new();
        let t0 = 2_000 * HORIZON_NS;
        upsert_token(&inner, &KEY, b"tok", "acme", t0)
            .await
            .expect("seed token");
        let (map, _v) = read_auth_map(&inner, &KEY)
            .await
            .expect("read")
            .expect("present");

        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Transient("auth store down".into()))
                .with_key_contains("sys/auth"),
        );
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(inner, plan));
        let state = state_over(store, t0);
        // Install the map as if a first refresh had succeeded at t0.
        state.install_map(map, t0);

        let bound = state.gate.hard_bound_ns();

        // While fresh, the token resolves.
        assert_eq!(
            state.resolve_token(b"tok", t0),
            AuthResolution::Resolved(TenantId::new("acme"))
        );

        // Refresh genuinely cannot succeed: every attempt errors and the gate is
        // never advanced.
        for _ in 0..3 {
            assert!(
                state.refresh(t0).await.is_err(),
                "a faulting sys/auth GET makes refresh fail"
            );
        }
        assert_eq!(
            state.refresh_failures(),
            3,
            "each failed refresh is counted; the fault genuinely fired"
        );

        // Still within the hard bound: the last-known map keeps serving.
        assert_eq!(
            state.resolve_token(b"tok", t0 + bound),
            AuthResolution::Resolved(TenantId::new("acme")),
            "within the hard bound a token is still served from the last-known map"
        );

        // One ns past the hard bound: fail closed, even though the token is still
        // in the cached map.
        assert_eq!(
            state.resolve_token(b"tok", t0 + bound + 1),
            AuthResolution::StaleFailClosed,
            "past the hard staleness bound the resolver refuses a previously-accepted token"
        );
        assert!(
            state.stale_fail_closed() >= 1,
            "the fail-closed staleness metric incremented"
        );
    }

    /// A steady stream of unknown-token requests must NOT starve the horizon
    /// refresh. Every miss wakes the loop's miss arm (the on-miss rate limiter
    /// gates the re-read, not the wakeup), so a horizon timer re-created inside
    /// the `select!` would be reset by each miss and the durable per-tenant
    /// limits refresh would never run. Under paused time this drives 300
    /// simulated seconds of one-miss-per-second traffic against a 60s horizon
    /// and asserts the durable cap was applied anyway, observed through real
    /// admission behaviour (five series admitted where the startup base cap of
    /// two would admit two).
    #[tokio::test(start_paused = true)]
    async fn token_misses_do_not_starve_the_horizon_limits_refresh() {
        use std::collections::HashMap;

        use ravel_catalog::{TenantConfig, TenantLifecycleState, set_tenant_config};
        use ravel_ingest::{CountLimit, RateLimit};
        use ravel_types::SeriesId;

        fn base_limits(cap: u64) -> AdmissionLimits {
            AdmissionLimits {
                max_active_series: CountLimit::Bounded(cap),
                max_active_streams: CountLimit::Unlimited,
                ingest_byte_rate: RateLimit::Unlimited,
                series_creation_rate: RateLimit::Unlimited,
            }
        }

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("acme");
        let base = base_limits(2);

        // A durable config record raises acme's active-series cap to 5. Only the
        // horizon refresh can apply it.
        set_tenant_config(
            store.as_ref(),
            &tenant.hash(),
            &TenantConfig {
                max_active_series: Some(5),
                ..TenantConfig::new(TenantLifecycleState::Active)
            },
            1_000,
        )
        .await
        .expect("write config record");

        let admission = Arc::new(AdmissionController::new(Arc::new(SystemClock), base));
        // Startup seeds the tenant at its --limits-file base cap of 2.
        admission.set_tenant_limits(tenant.clone(), base);

        let now = SystemClock.now_ns();
        let auth = Arc::new(state_over(store.clone(), now));
        let task = spawn(
            Some(auth.clone()),
            store.clone(),
            Some(LimitsRefresh {
                admission: admission.clone(),
                defaults: base,
                tenant_overrides: HashMap::from([(tenant.clone(), base)]),
            }),
            Duration::from_secs(60),
        );

        // Five simulated minutes of one unknown-token request per second: five
        // horizons' worth of misses, each of which wakes the loop.
        for _ in 0..300 {
            assert_eq!(
                auth.resolve_token(b"unknown-token", now),
                AuthResolution::Miss,
                "an unknown token is a miss, which signals the loop"
            );
            tokio::time::advance(Duration::from_secs(1)).await;
        }
        task.shutdown().await;

        // The durable cap of 5 was applied despite the miss traffic: admission
        // now admits five fresh series where the base cap of 2 would admit two.
        let admitted = admission.admit_series(
            &tenant,
            (1u8..=5).map(|b| SeriesId([b; 16])),
            SystemClock.now_ns(),
        );
        assert_eq!(
            admitted.admitted.len(),
            5,
            "the horizon refresh applied the durable cap of 5 despite continuous token misses; \
             a miss-reset horizon timer would leave the startup cap of 2 in force"
        );
        assert!(admitted.rejected.is_empty());
    }

    /// The `TenantResolver` surface: a hard-stale or unknown-token request is an
    /// `AuthError` (so the fallback chain moves on), and a header with no bearer
    /// token is an `AuthError` too (this resolver claims only bearer requests).
    #[test]
    fn resolver_surface_maps_outcomes_to_auth_results() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let now = SystemClock.now_ns();
        let state = Arc::new(state_over(store, now));
        state.install_map(
            AuthTokenMap {
                key_fingerprint: ravel_catalog::key_fingerprint(&KEY),
                entries: vec![],
            },
            now,
        );
        let resolver = DurableBearerResolver::new(state);

        // No Authorization header: AuthError (falls through the chain).
        let empty = HeaderMap::new();
        assert!(resolver.resolve(&empty).is_err());

        // A bearer header with an unknown token: AuthError (a miss).
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            "Bearer nope".parse().expect("valid header"),
        );
        assert!(resolver.resolve(&headers).is_err());
    }
}
