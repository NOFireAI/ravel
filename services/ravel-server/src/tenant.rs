//! Shared tenant resolution for both the OTLP ingest path and the query API.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_commit::rng::{RngSource, SystemRng};
use ravel_query::http::{
    DevHeaderTenantResolver, MtlsResolver, OidcJwksCache, OidcResolver, StaticBearerTokenResolver,
    TenantResolver,
};
use ravel_tenant_resolve::{Principal, SubjectDropCounters};
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
    /// The counters the OIDC resolver records a dropped subject in, the same
    /// [`subject_drop_counters`] handle `/metrics` renders as
    /// `ravel_auth_subject_dropped_total`.
    pub subject_drops: Arc<SubjectDropCounters>,
}

/// The process's subject-drop counters (ADR-3541 decision 3). One set per
/// process, as there is one resolver chain per process, so `/metrics` reads
/// them without a handle threaded through the server configuration.
pub fn subject_drop_counters() -> Arc<SubjectDropCounters> {
    static COUNTERS: std::sync::LazyLock<Arc<SubjectDropCounters>> =
        std::sync::LazyLock::new(|| Arc::new(SubjectDropCounters::default()));
    Arc::clone(&COUNTERS)
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
/// [`build_auth_resolver_with_principals`] with every token carrying no
/// capability and no OIDC capability claim, so one code path builds the chain.
pub fn build_auth_resolver(
    tokens: HashMap<String, TenantId>,
    dev_header: bool,
    auth: AuthResolverSettings,
) -> anyhow::Result<ResolverBundle> {
    let principals = tokens
        .into_iter()
        .map(|(token, tenant)| (token, Principal::tenant_only(tenant)))
        .collect();
    build_auth_resolver_with_principals(
        principals,
        dev_header,
        auth,
        OidcCapabilityClaims::default(),
    )
}

/// The OIDC claims the resolver reads beyond the tenant: the boolean ones
/// that grant a capability and the subject claim, each `None` (never read
/// from OIDC) by default.
#[derive(Debug, Clone, Default)]
pub struct OidcCapabilityClaims {
    /// `--oidc-ddl-claim` (ADR-2040 decision 4).
    pub ddl: Option<String>,
    /// `--oidc-ipcrypt-claim` (ADR-2993 decision 6).
    pub ipcrypt: Option<String>,
    /// `--oidc-agents-claim` (ADR-3331 decision 9).
    pub agents: Option<String>,
    /// `--oidc-agents-worker-claim` (ADR-3331 decision 9).
    pub agents_worker: Option<String>,
    /// `--oidc-agents-admin-claim` (ADR-3331 decision 9).
    pub agents_admin: Option<String>,
    /// `--oidc-subject-claim` (ADR-3541 decision 3): the string claim read as
    /// the principal's subject, not a capability grant.
    pub subject: Option<String>,
}

/// Same as [`build_auth_resolver`], but the static bearer map carries a
/// [`Principal`] (tenant plus its capabilities) per token
/// instead of a bare [`TenantId`], and `claims` names the boolean OIDC claims
/// that grant each capability when OIDC is configured. This is a parameter
/// rather than a field on [`crate::config::OidcSettings`] so that struct's
/// existing exhaustive constructors (in and out of this crate) keep compiling
/// unchanged.
///
/// A claim set while `auth.oidc` is `None` is an error: no OIDC resolver would
/// exist to read the claim, so the operator's grant would silently never
/// apply.
pub fn build_auth_resolver_with_principals(
    tokens: HashMap<String, Principal>,
    dev_header: bool,
    auth: AuthResolverSettings,
    claims: OidcCapabilityClaims,
) -> anyhow::Result<ResolverBundle> {
    if auth.oidc.is_none() {
        for (claim, flag) in [
            (&claims.ddl, "--oidc-ddl-claim"),
            (&claims.ipcrypt, "--oidc-ipcrypt-claim"),
            (&claims.agents, "--oidc-agents-claim"),
            (&claims.agents_worker, "--oidc-agents-worker-claim"),
            (&claims.agents_admin, "--oidc-agents-admin-claim"),
            (&claims.subject, "--oidc-subject-claim"),
        ] {
            if claim.is_some() {
                anyhow::bail!(
                    "{flag} was set but OIDC is not enabled (set --oidc-issuer and \
                     --oidc-jwks-url)"
                );
            }
        }
    }
    let mut resolvers: Vec<Arc<dyn TenantResolver>> =
        vec![Arc::new(StaticBearerTokenResolver::with_principals(tokens))];

    let mtls_resolver: Option<Arc<dyn TenantResolver>> = auth
        .mtls_header
        .map(|header| Arc::new(MtlsResolver::new(header)) as Arc<dyn TenantResolver>);

    if dev_header {
        resolvers.push(Arc::new(DevHeaderTenantResolver::default()));
    }

    let subject_drops = subject_drop_counters();
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
        if let Some(ddl_claim) = claims.ddl {
            oidc_resolver = oidc_resolver.with_ddl_claim(ddl_claim);
        }
        if let Some(ipcrypt_claim) = claims.ipcrypt {
            oidc_resolver = oidc_resolver.with_ipcrypt_claim(ipcrypt_claim);
        }
        if let Some(agents_claim) = claims.agents {
            oidc_resolver = oidc_resolver.with_agents_claim(agents_claim);
        }
        if let Some(agents_worker_claim) = claims.agents_worker {
            oidc_resolver = oidc_resolver.with_agents_worker_claim(agents_worker_claim);
        }
        if let Some(agents_admin_claim) = claims.agents_admin {
            oidc_resolver = oidc_resolver.with_agents_admin_claim(agents_admin_claim);
        }
        if let Some(subject_claim) = claims.subject {
            oidc_resolver = oidc_resolver.with_subject_claim(subject_claim, subject_drops.clone());
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
        subject_drops,
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
    use crate::config::OidcSettings;

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
        let zero = params(Duration::ZERO);
        let metrics = tokio::runtime::Handle::current().metrics();
        let alive_before = metrics.num_alive_tasks();
        match spawn_jwks_refresh(zero) {
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

    fn oidc_settings() -> OidcSettings {
        OidcSettings {
            issuer: "https://issuer.example".to_string(),
            jwks_url: "https://issuer.example/jwks.json".to_string(),
            audiences: vec!["ravel".to_string()],
            tenant_claim: "tenant".to_string(),
            refresh_interval: Duration::from_secs(300),
        }
    }

    #[test]
    fn an_oidc_ddl_claim_without_oidc_is_an_error() {
        let result = build_auth_resolver_with_principals(
            HashMap::new(),
            false,
            AuthResolverSettings::default(),
            OidcCapabilityClaims {
                ddl: Some("ravel_ddl".to_string()),
                ..OidcCapabilityClaims::default()
            },
        );
        let err = result.err().expect("a ddl claim with no OIDC must fail");
        assert!(
            err.to_string().contains("--oidc-ddl-claim"),
            "the error names the flag: {err}"
        );
    }

    #[test]
    fn an_oidc_ipcrypt_claim_without_oidc_is_an_error() {
        let result = build_auth_resolver_with_principals(
            HashMap::new(),
            false,
            AuthResolverSettings::default(),
            OidcCapabilityClaims {
                ipcrypt: Some("ravel_ipcrypt".to_string()),
                ..OidcCapabilityClaims::default()
            },
        );
        let err = result
            .err()
            .expect("an ipcrypt claim with no OIDC must fail");
        assert!(
            err.to_string().contains("--oidc-ipcrypt-claim"),
            "the error names the flag: {err}"
        );
    }

    #[test]
    fn each_oidc_agents_claim_without_oidc_is_an_error_naming_its_flag() {
        let claim = || Some("ravel_agents".to_string());
        for (claims, flag) in [
            (
                OidcCapabilityClaims {
                    agents: claim(),
                    ..OidcCapabilityClaims::default()
                },
                "--oidc-agents-claim",
            ),
            (
                OidcCapabilityClaims {
                    agents_worker: claim(),
                    ..OidcCapabilityClaims::default()
                },
                "--oidc-agents-worker-claim",
            ),
            (
                OidcCapabilityClaims {
                    agents_admin: claim(),
                    ..OidcCapabilityClaims::default()
                },
                "--oidc-agents-admin-claim",
            ),
        ] {
            let err = build_auth_resolver_with_principals(
                HashMap::new(),
                false,
                AuthResolverSettings::default(),
                claims,
            )
            .err()
            .expect("an agents claim with no OIDC must fail");
            assert!(
                err.to_string().contains(&format!("{flag} was set")),
                "the error names {flag}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn an_oidc_ddl_claim_with_oidc_builds() {
        let bundle = build_auth_resolver_with_principals(
            HashMap::new(),
            false,
            AuthResolverSettings {
                oidc: Some(oidc_settings()),
                mtls_header: None,
            },
            OidcCapabilityClaims {
                ddl: Some("ravel_ddl".to_string()),
                ipcrypt: Some("ravel_ipcrypt".to_string()),
                agents: Some("ravel_agents".to_string()),
                agents_worker: Some("ravel_agents_worker".to_string()),
                agents_admin: Some("ravel_agents_admin".to_string()),
                subject: Some("sub".to_string()),
            },
        )
        .expect("capability claims with OIDC configured build");
        assert!(bundle.oidc_refresh.is_some());
    }

    /// Flip to watch it fail: delete the `--oidc-subject-claim` entry from
    /// the no-OIDC check in `build_auth_resolver_with_principals`.
    #[test]
    fn oidc_subject_claim_without_oidc_refuses_startup() {
        let err = build_auth_resolver_with_principals(
            HashMap::new(),
            false,
            AuthResolverSettings::default(),
            OidcCapabilityClaims {
                subject: Some("email".to_string()),
                ..OidcCapabilityClaims::default()
            },
        )
        .err()
        .expect("a subject claim with no OIDC must fail");
        assert!(
            err.to_string()
                .contains("--oidc-subject-claim was set but OIDC is not enabled"),
            "{err}"
        );
    }

    // A P-256 keypair used only by these tests (the same one
    // tests/real_authn_e2e.rs signs with).
    const EC_PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgwdzs5ZXprZ+FrmVM\n\
2x3N9ehoXMhkWfOx7M0217LOtmehRANCAASPG9SIk2E3UTJC4nxm1l6y6yzX8N38\n\
lVNfCuKwwBrIas5yannzOq4NUHTqcDRTLWg1ZyMnLrUgZ/WHc4/TGBXG\n\
-----END PRIVATE KEY-----\n";
    const EC_X: &str = "jxvUiJNhN1EyQuJ8ZtZesuss1_Dd_JVTXwrisMAayGo";
    const EC_Y: &str = "znJqefM6rg1QdOpwNFMtaDVnIycutSBn9Ydzj9MYFcY";
    const KID: &str = "tenant-rs-test-key";

    /// `Authorization: Bearer` with a token signed for [`oidc_settings`]'s
    /// issuer and audience, for tenant `acme`, carrying `email`.
    fn bearer_with_email(email: serde_json::Value) -> axum::http::HeaderMap {
        use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};
        let claims = serde_json::json!({
            "iss": "https://issuer.example",
            "aud": "ravel",
            "exp": get_current_timestamp() + 600,
            "tenant": "acme",
            "email": email,
        });
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(KID.to_string());
        let key = EncodingKey::from_ec_pem(EC_PRIV_PEM.as_bytes()).expect("valid EC PEM");
        let token = encode(&header, &claims, &key).expect("token encodes");
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().expect("header value"),
        );
        headers
    }

    /// The configured claim reaches the built `OidcResolver`: a signed token
    /// carrying it as a string resolves a principal with that subject, source
    /// `oidc` and no capability, and one carrying a number resolves with no subject and
    /// one more `(oidc, not_string)` drop on the counters the bundle returns,
    /// which are the ones `/metrics` renders. Flip to watch it fail: delete
    /// the `with_subject_claim` call in `build_auth_resolver_with_principals`.
    #[tokio::test]
    async fn the_oidc_subject_claim_reaches_the_built_resolver() {
        use ravel_tenant_resolve::{SubjectDropReason, SubjectSource};

        let bundle = build_auth_resolver_with_principals(
            HashMap::new(),
            false,
            AuthResolverSettings {
                oidc: Some(oidc_settings()),
                mtls_header: None,
            },
            OidcCapabilityClaims {
                subject: Some("email".to_string()),
                ..OidcCapabilityClaims::default()
            },
        )
        .expect("a subject claim with OIDC configured builds");
        assert!(Arc::ptr_eq(&bundle.subject_drops, &subject_drop_counters()));
        let jwks: jsonwebtoken::jwk::JwkSet = serde_json::from_value(serde_json::json!({
            "keys": [{
                "kty": "EC", "crv": "P-256", "kid": KID, "alg": "ES256",
                "x": EC_X, "y": EC_Y,
            }]
        }))
        .expect("valid JWKS");
        bundle
            .oidc_refresh
            .as_ref()
            .expect("OIDC is configured")
            .cache
            .install_jwks(&jwks)
            .expect("keys install");

        let principal = bundle
            .resolver
            .resolve_principal(&bearer_with_email(serde_json::json!("alice@example.com")))
            .expect("a signed token resolves");
        assert_eq!(principal.tenant, TenantId::new("acme"));
        assert!(
            !(principal.ddl
                || principal.ipcrypt
                || principal.agents
                || principal.agents_worker
                || principal.agents_admin),
            "a subject grants no capability"
        );
        let subject = principal.subject.expect("the claim sets the subject");
        assert_eq!(subject.value(), "alice@example.com");
        assert_eq!(subject.source(), SubjectSource::Oidc);

        let not_string = |drops: &ravel_tenant_resolve::SubjectDropCounters| {
            drops
                .snapshot()
                .into_iter()
                .find(|(source, reason, _)| {
                    *source == SubjectSource::Oidc && *reason == SubjectDropReason::NotString
                })
                .map(|(_, _, count)| count)
                .expect("the pair is in the snapshot")
        };
        let before = not_string(&bundle.subject_drops);
        let principal = bundle
            .resolver
            .resolve_principal(&bearer_with_email(serde_json::json!(7)))
            .expect("a non-string subject claim still authenticates");
        assert_eq!(principal.subject, None);
        assert_eq!(not_string(&bundle.subject_drops), before + 1);
    }

    #[test]
    fn no_oidc_ddl_claim_without_oidc_still_builds() {
        let bundle = build_auth_resolver_with_principals(
            HashMap::new(),
            false,
            AuthResolverSettings::default(),
            OidcCapabilityClaims::default(),
        )
        .expect("no claim and no OIDC is the default shape");
        assert!(bundle.oidc_refresh.is_none());
    }
}
