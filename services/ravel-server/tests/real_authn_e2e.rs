//! End-to-end coverage for the real-authn resolvers (ADR-0042 decision 6) and the mTLS dedicated-listener isolation (ADR-0050 section 1): an in-process server backed by `MemoryStore` resolves a
//! tenant through a real HTTP request via the OIDC and mTLS resolvers, and
//! rejects unauthenticated requests.
//!
//! The OIDC case installs a locally-built EC (P-256) JWKS directly into the
//! cache, so the whole path (JWT signature + issuer + expiry validation, tenant
//! claim extraction, and the trait boundary the query handler consumes) runs
//! with no network and no live JWKS server. A real asymmetric key is used
//! deliberately: `parse_signing_keys` refuses symmetric (oct/HMAC) JWKS keys
//! outright, since a JWKS is a public document and a symmetric key inside one
//! is a published verification secret, never a usable one.
//!
//! The mTLS cases pin an isolation guarantee: the
//! `MtlsResolver` trusts an unauthenticated `x-ravel-client-cert-cn` header,
//! so it must be reachable only from the dedicated `--mtls-listener`, never
//! from the public HTTP or gRPC/Flight listeners. `forged_mtls_header_rejected_on_public_listeners`
//! runs that check against real server wiring rather
//! than a hand-built rig.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};
use ravel_object_store::memory::MemoryStore;
use ravel_query::http::{OidcJwksCache, OidcResolver, TenantResolver};
use ravel_server::config::AuthResolverSettings;
use ravel_server::tenant::FallbackResolver;
use ravel_server::{FoldTaskConfig, Mode, MtlsListenerConfig, ServerConfig};

const ISSUER: &str = "https://issuer.example.com";
const KID: &str = "itest-key";
const MTLS_HEADER: &str = "x-ravel-client-cert-cn";

// A real P-256 keypair (openssl ecparam -name prime256v1 -genkey), PKCS8
// PEM for jsonwebtoken's `EncodingKey::from_ec_pem`.
const EC_PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgwdzs5ZXprZ+FrmVM\n\
2x3N9ehoXMhkWfOx7M0217LOtmehRANCAASPG9SIk2E3UTJC4nxm1l6y6yzX8N38\n\
lVNfCuKwwBrIas5yannzOq4NUHTqcDRTLWg1ZyMnLrUgZ/WHc4/TGBXG\n\
-----END PRIVATE KEY-----\n";
const EC_X: &str = "jxvUiJNhN1EyQuJ8ZtZesuss1_Dd_JVTXwrisMAayGo";
const EC_Y: &str = "znJqefM6rg1QdOpwNFMtaDVnIycutSBn9Ydzj9MYFcY";

/// Starts a real server, optionally with the dedicated mTLS listener wired up
/// (ADR-0050 section 1). `mtls_resolver` mirrors `--mtls-enabled` +
/// `--mtls-listener`: when `Some`, `ravel_server::start` binds a third
/// listener whose router chain is built from this resolver alone, entirely
/// separate from `resolver`, which backs the public HTTP and gRPC/Flight
/// listeners.
async fn start_with_mtls(
    resolver: Arc<dyn TenantResolver>,
    mtls_resolver: Option<Arc<dyn TenantResolver>>,
) -> ravel_server::Running {
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: resolver,
        mtls_listener: mtls_resolver.map(|resolver| MtlsListenerConfig {
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            resolver,
        }),
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

async fn start_with_resolver(resolver: Arc<dyn TenantResolver>) -> ravel_server::Running {
    start_with_mtls(resolver, None).await
}

fn jwks() -> JwkSet {
    let doc = serde_json::json!({
        "keys": [{
            "kty": "EC", "crv": "P-256", "kid": KID, "alg": "ES256",
            "x": EC_X, "y": EC_Y,
        }]
    });
    serde_json::from_value(doc).expect("valid JWKS")
}

fn sign_token(tenant: &str, exp_offset_secs: i64) -> String {
    let exp = (get_current_timestamp() as i64 + exp_offset_secs).max(0);
    let claims = serde_json::json!({
        "iss": ISSUER,
        "sub": "svc-account",
        "tenant": tenant,
        "exp": exp,
    });
    let mut header = Header::new(Algorithm::ES256);
    header.kid = Some(KID.to_string());
    let key = EncodingKey::from_ec_pem(EC_PRIV_PEM.as_bytes()).expect("valid EC PEM");
    encode(&header, &claims, &key).expect("token encodes")
}

/// The OIDC resolver, wrapped in a FallbackResolver exactly as the wiring does,
/// over a cache pre-loaded with a local JWKS (no network).
fn oidc_chain() -> Arc<dyn TenantResolver> {
    let cache = OidcJwksCache::new().expect("client builds");
    cache.install_jwks(&jwks()).expect("keys install");
    let oidc = Arc::new(OidcResolver::new(
        Arc::new(cache),
        ISSUER,
        Vec::new(),
        "tenant",
    ));
    Arc::new(FallbackResolver::new(vec![oidc]))
}

/// Builds the real `build_auth_resolver` wiring with only the mTLS resolver
/// configured (no static tokens, no dev header, no OIDC), the same shape
/// `--mtls-enabled --mtls-header x-ravel-client-cert-cn` produces. Returns the
/// public chain (never containing the mTLS resolver, ADR-0050 section 1) and
/// the mTLS resolver on its own.
fn mtls_bundle() -> (Arc<dyn TenantResolver>, Arc<dyn TenantResolver>) {
    let bundle = ravel_server::tenant::build_auth_resolver(
        HashMap::new(),
        false,
        AuthResolverSettings {
            oidc: None,
            mtls_header: Some(MTLS_HEADER.to_string()),
        },
    )
    .expect("resolver builds");
    let mtls_resolver = bundle
        .mtls_resolver
        .expect("mtls_header was set above, so build_auth_resolver returns Some");
    (bundle.resolver, mtls_resolver)
}

#[tokio::test]
async fn oidc_valid_jwt_resolves_tenant_end_to_end() {
    let running = start_with_resolver(oidc_chain()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    // A valid, signed, unexpired token carrying the tenant claim: the query
    // handler resolves the tenant through the OIDC resolver and answers 200.
    let token = sign_token("acme", 3600);
    let resp = client
        .get(format!("{base}/api/v1/query"))
        .header("authorization", format!("Bearer {token}"))
        .query(&[("query", "up")])
        .send()
        .await
        .expect("query request succeeds");
    assert_eq!(resp.status(), 200, "valid JWT should authenticate");
    let body: serde_json::Value = resp.json().await.expect("json body");
    assert_eq!(body["status"], "success");

    running.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn oidc_expired_jwt_is_unauthorized_end_to_end() {
    let running = start_with_resolver(oidc_chain()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let token = sign_token("acme", -3600);
    let resp = client
        .get(format!("{base}/api/v1/query"))
        .header("authorization", format!("Bearer {token}"))
        .query(&[("query", "up")])
        .send()
        .await
        .expect("query request succeeds");
    assert_eq!(resp.status(), 401, "expired JWT must be rejected");

    running.shutdown().await.expect("clean shutdown");
}

#[tokio::test]
async fn mtls_header_authenticates_only_on_dedicated_listener() {
    let (resolver, mtls_resolver) = mtls_bundle();
    let running = start_with_mtls(resolver, Some(mtls_resolver)).await;
    let mtls_addr = running
        .mtls_addr
        .expect("mtls_resolver was configured above");
    let base = format!("http://{mtls_addr}");
    let client = reqwest::Client::new();

    // On the dedicated mTLS listener, the header authenticates exactly as a
    // TLS-terminating, header-stripping proxy in front of it would present it.
    let ok = client
        .get(format!("{base}/api/v1/query"))
        .header(MTLS_HEADER, "acme")
        .query(&[("query", "up")])
        .send()
        .await
        .expect("query request succeeds");
    assert_eq!(
        ok.status(),
        200,
        "mTLS header should authenticate on the dedicated listener"
    );

    // Without it, no resolver in the chain matches: unauthenticated.
    let denied = client
        .get(format!("{base}/api/v1/query"))
        .query(&[("query", "up")])
        .send()
        .await
        .expect("query request succeeds");
    assert_eq!(denied.status(), 401, "absent header must be rejected");

    running.shutdown().await.expect("clean shutdown");
}

/// A forged
/// `x-ravel-client-cert-cn` header, naming a victim tenant with no client
/// certificate at all, sent directly to every public listener. Before
/// ADR-0050 section 1 this authenticated as the named tenant on any listener
/// the resolver chain was installed on; the fix makes the public chains
/// structurally incapable of reading the header at all, so this must be
/// rejected on the HTTP query listener, the gRPC OTLP listener, and (where
/// compiled in) the Flight SQL listener, even though the same process has a
/// real mTLS resolver configured and reachable on its own dedicated listener.
#[tokio::test]
async fn forged_mtls_header_rejected_on_public_listeners() {
    let (resolver, mtls_resolver) = mtls_bundle();
    let running = start_with_mtls(resolver, Some(mtls_resolver)).await;

    // HTTP: the public query listener.
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{base}/api/v1/query"))
        .header(MTLS_HEADER, "victim-tenant")
        .query(&[("query", "up")])
        .send()
        .await
        .expect("query request succeeds");
    assert_eq!(
        resp.status(),
        401,
        "forged mTLS header must not authenticate on the public HTTP listener"
    );

    // gRPC: the public OTLP listener, which the same chain resolver backs.
    {
        use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
        use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

        let grpc_addr = running.grpc_addr.expect("gRPC listener binds in All mode");
        let mut client = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
            .await
            .expect("gRPC client connects");
        let mut request = tonic::Request::new(ExportMetricsServiceRequest {
            resource_metrics: Vec::new(),
        });
        request.metadata_mut().insert(
            MTLS_HEADER,
            "victim-tenant".parse().expect("ascii metadata"),
        );
        let status = client
            .export(request)
            .await
            .expect_err("forged header must not authenticate on the public gRPC listener");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    // Flight SQL: rides the same gRPC listener and the same public chain.
    #[cfg(feature = "flight-sql")]
    {
        use arrow_flight::FlightDescriptor;
        use arrow_flight::flight_service_client::FlightServiceClient;
        use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
        use prost::Message as _;

        let grpc_addr = running.grpc_addr.expect("gRPC listener binds in All mode");
        let channel = tonic::transport::Channel::from_shared(format!("http://{grpc_addr}"))
            .expect("valid endpoint uri")
            .connect()
            .await
            .expect("Flight client connects");
        let mut client = FlightServiceClient::new(channel);

        let command = CommandStatementQuery {
            query: "SELECT 1".to_string(),
            transaction_id: None,
        };
        let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
        let mut request = tonic::Request::new(descriptor);
        request.metadata_mut().insert(
            MTLS_HEADER,
            "victim-tenant".parse().expect("ascii metadata"),
        );
        let status = client
            .get_flight_info(request)
            .await
            .expect_err("forged header must not authenticate on the public Flight listener");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
    }

    running.shutdown().await.expect("clean shutdown");
}
