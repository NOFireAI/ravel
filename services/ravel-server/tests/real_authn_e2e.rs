//! End-to-end coverage for the real-authn resolvers (ADR-0042 decision 6,
//! issue #392): an in-process server backed by `MemoryStore` resolves a tenant
//! through a real HTTP request via the OIDC and mTLS resolvers in the
//! `FallbackResolver` chain, and rejects unauthenticated requests.
//!
//! The OIDC case installs a locally-built EC (P-256) JWKS directly into the
//! cache, so the whole path (JWT signature + issuer + expiry validation, tenant
//! claim extraction, and the trait boundary the query handler consumes) runs
//! with no network and no live JWKS server. A real asymmetric key is used
//! deliberately: `parse_signing_keys` refuses symmetric (oct/HMAC) JWKS keys
//! outright, since a JWKS is a public document and a symmetric key inside one
//! is a published verification secret, never a usable one.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode, get_current_timestamp};
use ravel_object_store::memory::MemoryStore;
use ravel_query::http::{OidcJwksCache, OidcResolver, TenantResolver};
use ravel_server::config::AuthResolverSettings;
use ravel_server::tenant::FallbackResolver;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};

const ISSUER: &str = "https://issuer.example.com";
const KID: &str = "itest-key";

// A real P-256 keypair (openssl ecparam -name prime256v1 -genkey), PKCS8
// PEM for jsonwebtoken's `EncodingKey::from_ec_pem`.
const EC_PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgwdzs5ZXprZ+FrmVM\n\
2x3N9ehoXMhkWfOx7M0217LOtmehRANCAASPG9SIk2E3UTJC4nxm1l6y6yzX8N38\n\
lVNfCuKwwBrIas5yannzOq4NUHTqcDRTLWg1ZyMnLrUgZ/WHc4/TGBXG\n\
-----END PRIVATE KEY-----\n";
const EC_X: &str = "jxvUiJNhN1EyQuJ8ZtZesuss1_Dd_JVTXwrisMAayGo";
const EC_Y: &str = "znJqefM6rg1QdOpwNFMtaDVnIycutSBn9Ydzj9MYFcY";

async fn start_with_resolver(resolver: Arc<dyn TenantResolver>) -> ravel_server::Running {
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: resolver,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
    };
    ravel_server::start(config, store)
        .await
        .expect("server starts")
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
async fn mtls_header_resolves_tenant_end_to_end() {
    // Exercise the real wiring: build_auth_resolver assembles the chain with the
    // mTLS resolver enabled (no static tokens, no dev header).
    let bundle = ravel_server::tenant::build_auth_resolver(
        HashMap::new(),
        false,
        AuthResolverSettings {
            oidc: None,
            mtls_header: Some("x-ravel-client-cert-cn".to_string()),
        },
    )
    .expect("resolver builds");
    assert!(bundle.oidc_refresh.is_none());

    let running = start_with_resolver(bundle.resolver).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    // With the trusted client-cert header present, the request authenticates.
    let ok = client
        .get(format!("{base}/api/v1/query"))
        .header("x-ravel-client-cert-cn", "acme")
        .query(&[("query", "up")])
        .send()
        .await
        .expect("query request succeeds");
    assert_eq!(ok.status(), 200, "mTLS header should authenticate");

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
