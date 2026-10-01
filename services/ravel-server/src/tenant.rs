//! Shared tenant resolution for both the OTLP ingest path and the query API.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_commit::rng::{RngSource, SystemRng};
use ravel_query::http::{
    DevHeaderTenantResolver, MtlsResolver, OidcJwksCache, OidcResolver, StaticBearerTokenResolver,
    TenantResolver,
};
use ravel_tenant_resolve::Principal;
use ravel_types::TenantId;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::config::AuthResolverSettings;

// `FallbackResolver` moved to the shared `ravel-tenant-resolve` crate (ADR-0080
// decision 3). Re-exported here so `ravel_server::tenant::FallbackResolver`
// keeps resolving for the wiring in `crate::lib` and the real-authn e2e test,
// unchanged.
pub use ravel_tenant_resolve::FallbackResolver;

pub fn build_resolver(
    tokens: HashMap<String, TenantId>,
    dev_header: bool,
) -> Arc<dyn TenantResolver> {
    let bearer: Arc<dyn TenantResolver> = Arc::new(StaticBearerTokenResolver::new(tokens));
    if dev_header {
        Arc::new(FallbackResolver::new(vec![
            bearer,
            Arc::new(DevHeaderTenantResolver::default()),
        ]))
    } else {
        bearer
    }
}

/// The resolver chain plus, when OIDC is configured, everything the caller needs
/// to drive the JWKS refresh background task. The `OidcResolver` in the chain
/// and [`OidcRefreshParams::cache`] share the same [`OidcJwksCache`], so the
/// task's async refresh is what the sync request-path resolver reads.
pub struct ResolverBundle {
    pub resolver: Arc<dyn TenantResolver>,
    pub oidc_refresh: Option<OidcRefreshParams>,
    /// The mTLS resolver, kept out of `resolver` entirely (ADR-0050 section 1).
    /// `None` when `--mtls-enabled` is off. The caller wires this into the
    /// dedicated `--mtls-listener` chain only; it must never be merged into
    /// `resolver`, which backs every public listener.
    pub mtls_resolver: Option<Arc<dyn TenantResolver>>,
}

/// Inputs for the JWKS refresh task: the cache the request-path `OidcResolver`
/// reads, the JWKS URL to fetch, and how often to refetch.
pub struct OidcRefreshParams {
    pub cache: Arc<OidcJwksCache>,
    pub jwks_url: String,
    /// Pause between JWKS refetches (`--oidc-jwks-refresh-interval-secs`). A
    /// zero interval is refused at startup with
    /// [`JwksRefreshSpawnError::ZeroRefreshInterval`].
    pub interval: Duration,
}

/// Why [`spawn_jwks_refresh`] refused to start the refresh loop. Nothing is
/// spawned.
#[derive(Debug, thiserror::Error)]
pub enum JwksRefreshSpawnError {
    /// A zero `interval` would refetch the JWKS back to back.
    #[error(
        "--oidc-jwks-refresh-interval-secs must be non-zero: a zero refresh interval refetches the JWKS back to back"
    )]
    ZeroRefreshInterval,
}

impl OidcRefreshParams {
    /// The refusal [`spawn_jwks_refresh`] applies before starting the loop;
    /// `start` runs it before spawning anything else, and before the initial
    /// JWKS fetch.
    pub fn check_spawnable(&self) -> Result<(), JwksRefreshSpawnError> {
        if self.interval.is_zero() {
            return Err(JwksRefreshSpawnError::ZeroRefreshInterval);
        }
        Ok(())
    }
}

/// Build the full tenant-resolution chain: the static bearer resolver always,
/// plus the optional dev-header and OIDC resolvers (ADR-0042 decision 6). The
/// mTLS resolver, when configured, is returned separately in
/// [`ResolverBundle::mtls_resolver`] and never enters this chain (ADR-0050
/// section 1) - `resolver` backs every public listener, and a resolver that
/// trusts an unauthenticated header has no business there.
///
/// `FallbackResolver` tries every resolver in order and returns the first
/// success, and that first match also decides the `ddl` capability: the
/// resolvers key off disjoint headers or token shapes so no request is
/// normally claimed by two, but if one were, the earlier resolver's principal
/// (tenant and `ddl`) wins and later ones are never consulted. The static
/// bearer map is a cheap `HashMap` lookup, so it goes first to fail fast for a
/// dev/local token; the more expensive JWT verification is last. Delegates to
/// [`build_auth_resolver_with_principals`] with every token carrying
/// `ddl: false` and no OIDC ddl claim, so one code path builds the chain.
pub fn build_auth_resolver(
    tokens: HashMap<String, TenantId>,
    dev_header: bool,
    auth: AuthResolverSettings,
) -> anyhow::Result<ResolverBundle> {
    let principals = tokens
        .into_iter()
        .map(|(token, tenant)| (token, Principal { tenant, ddl: false }))
        .collect();
    build_auth_resolver_with_principals(principals, dev_header, auth, None)
}

/// Same as [`build_auth_resolver`], but the static bearer map carries a
/// [`Principal`] (tenant plus the `ddl` capability, ADR-2040 decision 4) per
/// token instead of a bare [`TenantId`], and `oidc_ddl_claim` (from
/// `--oidc-ddl-claim`, `None` by default) names the boolean OIDC claim that
/// grants the capability when OIDC is configured. This is a parameter rather
/// than a field on [`crate::config::OidcSettings`] so that struct's existing
/// exhaustive constructors (in and out of this crate) keep compiling
/// unchanged.
pub fn build_auth_resolver_with_principals(
    tokens: HashMap<String, Principal>,
    dev_header: bool,
    auth: AuthResolverSettings,
    oidc_ddl_claim: Option<String>,
) -> anyhow::Result<ResolverBundle> {
    let mut resolvers: Vec<Arc<dyn TenantResolver>> =
        vec![Arc::new(StaticBearerTokenResolver::with_principals(tokens))];

    let mtls_resolver: Option<Arc<dyn TenantResolver>> = auth
        .mtls_header
        .map(|header| Arc::new(MtlsResolver::new(header)) as Arc<dyn TenantResolver>);

    if dev_header {
        resolvers.push(Arc::new(DevHeaderTenantResolver::default()));
    }

    let mut oidc_refresh = None;
    if let Some(oidc) = auth.oidc {
        let cache = Arc::new(
            OidcJwksCache::new().map_err(|e| anyhow::anyhow!("failed to build OIDC cache: {e}"))?,
        );
        let mut oidc_resolver = OidcResolver::new(
            cache.clone(),
            oidc.issuer,
            oidc.audiences,
            oidc.tenant_claim,
        );
        if let Some(ddl_claim) = oidc_ddl_claim {
            oidc_resolver = oidc_resolver.with_ddl_claim(ddl_claim);
        }
        resolvers.push(Arc::new(oidc_resolver));
        oidc_refresh = Some(OidcRefreshParams {
            cache,
            jwks_url: oidc.jwks_url,
            interval: oidc.refresh_interval,
        });
    }

    let resolver: Arc<dyn TenantResolver> = Arc::new(FallbackResolver::new(resolvers));
    Ok(ResolverBundle {
        resolver,
        oidc_refresh,
        mtls_resolver,
    })
}

/// Handle to the JWKS refresh task, for clean shutdown (mirrors
/// [`crate::maintain::MaintenanceTasks`]).
pub struct JwksRefreshTask {
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<JoinHandle<()>>,
}

impl JwksRefreshTask {
    pub fn none() -> Self {
        JwksRefreshTask {
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

/// Spawn the periodic JWKS refresh loop for an OIDC-enabled server. Returns
/// immediately; the task runs until [`JwksRefreshTask::shutdown`]. Follows the
/// same shape as [`crate::maintain::spawn`]: a jittered interval and a `oneshot`
/// shutdown. The request path never blocks on this: it only reads the cache the
/// loop writes. A zero `interval` is refused with
/// [`JwksRefreshSpawnError::ZeroRefreshInterval`] and nothing is spawned.
pub fn spawn_jwks_refresh(
    params: OidcRefreshParams,
) -> Result<JwksRefreshTask, JwksRefreshSpawnError> {
    params.check_spawnable()?;
    let (tx, rx) = oneshot::channel();
    // Production OS-entropy jitter (ADR-0068 decision 2); the simulation
    // harness does not drive JWKS refresh, so there is no injected variant.
    let rng: Arc<dyn RngSource> = Arc::new(SystemRng);
    let handle = tokio::spawn(async move {
        refresh_loop(params.cache, params.jwks_url, params.interval, rng, rx).await;
    });
    Ok(JwksRefreshTask {
        shutdown: Some(tx),
        handle: Some(handle),
    })
}

async fn refresh_loop(
    cache: Arc<OidcJwksCache>,
    jwks_url: String,
    interval: Duration,
    rng: Arc<dyn RngSource>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval, rng.as_ref())) => {}
            _ = &mut shutdown => return,
        }
        match cache.refresh(&jwks_url).await {
            Ok(()) => tracing::debug!(jwks_url = %jwks_url, "JWKS refresh complete"),
            // A failed refresh keeps the previously cached keys, so a transient
            // JWKS outage does not start rejecting every OIDC request.
            Err(err) => tracing::warn!(
                jwks_url = %jwks_url,
                error = %err,
                "JWKS refresh failed; keeping previously cached keys"
            ),
        }
    }
}

/// Up to 10% jitter over `base`, so co-started replicas do not refetch the JWKS
/// in lockstep (same rationale as the fold, maintenance, and alert tasks).
fn jittered(base: Duration, rng: &dyn RngSource) -> Duration {
    let jitter_bound_ms = u64::try_from(base.as_millis() / 10).unwrap_or(u64::MAX);
    if jitter_bound_ms == 0 {
        return base;
    }
    let extra_ms = rng.jitter_ms(jitter_bound_ms);
    base + Duration::from_millis(extra_ms)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn params(interval: Duration) -> OidcRefreshParams {
        OidcRefreshParams {
            cache: Arc::new(OidcJwksCache::new().expect("OIDC cache builds")),
            jwks_url: "http://127.0.0.1:9/jwks".to_string(),
            interval,
        }
    }

    /// Flip to watch it fail: delete the `is_zero()` arm of
    /// `OidcRefreshParams::check_spawnable`; the zero interval then spawns.
    #[tokio::test]
    async fn spawn_jwks_refresh_refuses_a_zero_interval() {
        let metrics = tokio::runtime::Handle::current().metrics();
        let alive_before = metrics.num_alive_tasks();
        match spawn_jwks_refresh(params(Duration::ZERO)) {
            Err(JwksRefreshSpawnError::ZeroRefreshInterval) => {}
            Ok(task) => {
                task.shutdown().await;
                panic!("a zero refresh interval must be refused at spawn");
            }
        }
        assert_eq!(metrics.num_alive_tasks(), alive_before);
    }

    #[tokio::test]
    async fn spawn_jwks_refresh_starts_on_a_non_zero_interval() {
        let task = spawn_jwks_refresh(params(Duration::from_secs(300)))
            .expect("a non-zero refresh interval spawns");
        task.shutdown().await;
    }
}
