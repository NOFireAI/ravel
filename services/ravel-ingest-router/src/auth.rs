//! Builds the canonical-tenant resolver chain from [`CanonicalAuthSettings`],
//! reusing the shared [`ravel_tenant_resolve`] implementation so the router and
//! `ravel-server` never drift on what the canonical tenant is (ADR-0080
//! decision 3).
//!
//! The chain mirrors `ravel_server::tenant::build_auth_resolver`: a static
//! bearer map, optionally a dev-header resolver, optionally OIDC, and -- unlike
//! `ravel-server`, which keeps mTLS on a dedicated listener -- optionally the
//! mTLS resolver folded into the one chain, since this router terminates a
//! single ingest path rather than separate public/mTLS listeners. The mTLS
//! resolver still reads a proxy-forwarded header and is only as trustworthy as
//! the proxy that sets it (see [`ravel_tenant_resolve::MtlsResolver`]).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_tenant_resolve::{
    DevHeaderTenantResolver, FallbackResolver, MtlsResolver, OidcJwksCache, OidcResolver,
    StaticBearerTokenResolver, TenantResolver,
};
use ravel_types::TenantId;
use tokio::task::JoinHandle;

use crate::config::CanonicalAuthSettings;

/// The built resolver plus, when OIDC is configured, the shared JWKS cache and
/// refresh parameters the caller drives on a background task.
pub(crate) struct CanonicalResolver {
    pub resolver: Arc<dyn TenantResolver>,
    pub oidc_refresh: Option<OidcRefresh>,
}

/// Inputs for the JWKS refresh loop: the cache the request-path resolver reads,
/// the URL to fetch, and the refetch interval.
pub(crate) struct OidcRefresh {
    pub cache: Arc<OidcJwksCache>,
    pub jwks_url: String,
    pub interval: Duration,
}

/// Build the canonical-tenant resolver chain.
pub(crate) fn build(settings: &CanonicalAuthSettings) -> anyhow::Result<CanonicalResolver> {
    let tokens: HashMap<String, TenantId> = settings
        .tokens
        .iter()
        .map(|(token, tenant)| (token.clone(), TenantId::new(tenant.clone())))
        .collect();

    let mut resolvers: Vec<Arc<dyn TenantResolver>> =
        vec![Arc::new(StaticBearerTokenResolver::new(tokens))];

    if settings.dev_header {
        resolvers.push(Arc::new(DevHeaderTenantResolver::default()));
    }

    let mut oidc_refresh = None;
    if let Some(oidc) = &settings.oidc {
        let cache = Arc::new(
            OidcJwksCache::new().map_err(|e| anyhow::anyhow!("failed to build OIDC cache: {e}"))?,
        );
        resolvers.push(Arc::new(OidcResolver::new(
            cache.clone(),
            oidc.issuer.clone(),
            oidc.audiences.clone(),
            oidc.tenant_claim.clone(),
        )));
        oidc_refresh = Some(OidcRefresh {
            cache,
            jwks_url: oidc.jwks_url.clone(),
            interval: oidc.refresh_interval,
        });
    }

    if let Some(header) = &settings.mtls_header {
        resolvers.push(Arc::new(MtlsResolver::new(header.clone())));
    }

    let resolver: Arc<dyn TenantResolver> = Arc::new(FallbackResolver::new(resolvers));
    Ok(CanonicalResolver {
        resolver,
        oidc_refresh,
    })
}

/// Spawn the periodic JWKS refresh loop. Does one best-effort refresh up front
/// so an OIDC router does not reject every request for a full interval at
/// startup, then refetches on `interval`. A failed refresh keeps the previously
/// cached keys (a transient JWKS outage does not start rejecting every token).
pub(crate) fn spawn_jwks_refresh(params: OidcRefresh) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = params.cache.refresh(&params.jwks_url).await {
            tracing::warn!(error = %err, "initial JWKS refresh failed; will retry on interval");
        }
        let mut ticker = tokio::time::interval(params.interval);
        // Skip the immediate first tick: the up-front refresh above already ran.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match params.cache.refresh(&params.jwks_url).await {
                Ok(()) => tracing::debug!("JWKS refresh complete"),
                Err(err) => {
                    tracing::warn!(error = %err, "JWKS refresh failed; keeping cached keys")
                }
            }
        }
    })
}
