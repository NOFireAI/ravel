//! Tenant resolution for the HTTP layer. There is deliberately no default
//! resolver: callers must construct one of the impls below (or their own),
//! so an unauthenticated deployment is a conscious choice, not an oversight
//! (docs/query-engine.md "default deny").

use axum::http::HeaderMap;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use jsonwebtoken::jwk::{Jwk, JwkSet, KeyAlgorithm};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};

use ravel_types::TenantId;

/// A request could not be attributed to a tenant.
#[derive(Debug, thiserror::Error)]
#[error("unauthenticated request")]
pub struct AuthError;

/// Reads the `Authorization: Bearer <token>` header, returning the raw token or
/// [`AuthError`] for a missing, non-ASCII, or non-`Bearer` header. Shared by the
/// bearer-token and OIDC resolvers so both parse the header identically.
fn bearer_token(headers: &HeaderMap) -> Result<&str, AuthError> {
    let raw = headers
        .get(axum::http::header::AUTHORIZATION)
        .ok_or(AuthError)?;
    let raw = raw.to_str().map_err(|_| AuthError)?;
    raw.strip_prefix("Bearer ").ok_or(AuthError)
}

/// Resolves the tenant for an incoming request from its headers.
pub trait TenantResolver: Send + Sync {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError>;
}

/// Resolves a tenant from a static `Authorization: Bearer <token>` map.
pub struct StaticBearerTokenResolver {
    tokens: HashMap<String, TenantId>,
}

impl StaticBearerTokenResolver {
    pub fn new(tokens: HashMap<String, TenantId>) -> Self {
        StaticBearerTokenResolver { tokens }
    }
}

impl TenantResolver for StaticBearerTokenResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        let token = bearer_token(headers)?;
        self.tokens.get(token).cloned().ok_or(AuthError)
    }
}

/// Resolves a tenant straight from a header value, for development and
/// tests only. Must be constructed explicitly; never used as a default.
pub struct DevHeaderTenantResolver {
    header_name: String,
}

impl DevHeaderTenantResolver {
    pub fn new(header_name: impl Into<String>) -> Self {
        DevHeaderTenantResolver {
            header_name: header_name.into(),
        }
    }
}

impl Default for DevHeaderTenantResolver {
    fn default() -> Self {
        DevHeaderTenantResolver::new("x-ravel-tenant")
    }
}

impl TenantResolver for DevHeaderTenantResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        let raw = headers.get(self.header_name.as_str()).ok_or(AuthError)?;
        let value = raw.to_str().map_err(|_| AuthError)?;
        if value.is_empty() {
            return Err(AuthError);
        }
        Ok(TenantId::new(value.to_string()))
    }
}

// --- OIDC (JWT) tenant resolution (ADR-0042 decision 6, issue #392) ----------

/// A JWKS refresh or a key parse failed. The request-path resolver
/// ([`OidcResolver::resolve`]) never surfaces this: it maps every validation
/// failure to [`AuthError`]. This type is only for the off-request-path
/// [`OidcJwksCache::refresh`], whose failure the operator needs to see.
#[derive(Debug, thiserror::Error)]
pub enum OidcError {
    /// The shared HTTP client could not be constructed (a TLS backend init
    /// failure at startup, not a per-request condition).
    #[error("failed to build the JWKS HTTP client: {0}")]
    HttpClient(#[source] reqwest::Error),
    /// The JWKS document could not be fetched, was a non-success HTTP status,
    /// or did not parse as a JWK set.
    #[error("failed to fetch or parse the JWKS document: {0}")]
    Fetch(#[source] reqwest::Error),
    /// A key in the JWKS could not be turned into a decoding key.
    #[error("invalid key in JWKS (kid {kid:?}): {reason}")]
    InvalidKey { kid: Option<String>, reason: String },
    /// The JWKS parsed but declared no key usable for signature verification
    /// (every key omitted `alg` or declared a non-signing algorithm). Treated
    /// as an error rather than an empty cache so the resolver never silently
    /// rejects every token with no explanation.
    #[error("JWKS contained no usable signing keys")]
    NoUsableKeys,
}

/// One cached, already-parsed decoding key plus the single signature algorithm
/// it is allowed to verify. The algorithm is taken from the JWKS key's own
/// declared `alg`, never from an incoming token's header: pinning it here is
/// what defeats the algorithm-confusion / `alg: none` attack class, because the
/// [`Validation`] built for a key only ever admits this one algorithm.
struct DecodingEntry {
    kid: Option<String>,
    alg: Algorithm,
    key: DecodingKey,
}

/// The set of decoding keys currently trusted for JWT verification, behind a
/// sync-read / async-write [`RwLock`].
///
/// The request path ([`OidcResolver::resolve`]) is synchronous and must never
/// block on I/O, so it only ever takes a read lock and clones the current
/// `Arc<Vec<DecodingEntry>>` snapshot. The JWKS document is fetched and parsed
/// off the request path by [`refresh`](Self::refresh), which a background task
/// calls on a jittered interval; reads are fast and frequent, writes are rare,
/// which is exactly the access pattern a std `RwLock` suits. This mirrors the
/// sync/async split `ravel_maintain::legal_hold::LegalHoldCheck` uses (async
/// `refresh`, sync per-key read of an in-memory snapshot).
pub struct OidcJwksCache {
    keys: RwLock<Arc<Vec<DecodingEntry>>>,
    http: reqwest::Client,
}

/// How long a single JWKS fetch may take before it is treated as a failure.
/// A stalled fetch with no timeout wedges the readiness gate, the periodic
/// refresh loop, and the refresh task's shutdown alike (see [`OidcJwksCache::new`]).
const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

impl OidcJwksCache {
    /// A cache with no keys yet. Until the first successful
    /// [`refresh`](Self::refresh) (or [`install_jwks`](Self::install_jwks)) it
    /// trusts nothing, so every OIDC request is rejected. Fails only if the
    /// HTTP client cannot be built, a startup misconfiguration.
    pub fn new() -> Result<Self, OidcError> {
        // reqwest applies no timeout by default. A JWKS host that accepts the
        // connection and then stalls would otherwise wedge every caller of
        // `refresh` forever: the readiness gate (which awaits the first
        // refresh before marking the server ready), the periodic refresh
        // loop (permanently stopping key rotation, so every OIDC request
        // starts failing once the IdP rotates keys, with no recovery short
        // of a restart), and the refresh task's shutdown (which joins the
        // same stuck task).
        let http = reqwest::Client::builder()
            .timeout(JWKS_FETCH_TIMEOUT)
            .build()
            .map_err(OidcError::HttpClient)?;
        Ok(OidcJwksCache {
            keys: RwLock::new(Arc::new(Vec::new())),
            http,
        })
    }

    /// Fetch `<jwks_url>` and replace the cached keys with the ones it declares.
    ///
    /// This is the async, off-request-path write. It GETs a directly-configured
    /// JWKS URL (OIDC discovery is deliberately not implemented; a direct JWKS
    /// URL is sufficient for ADR-0042 decision 6 and avoids a second network
    /// round trip), requires a success status, parses the body as a JWK set,
    /// and installs it. A failure leaves the previously-cached keys untouched,
    /// so a transient JWKS outage degrades to "keep trusting the last good
    /// keys" rather than "reject everything".
    pub async fn refresh(&self, jwks_url: &str) -> Result<(), OidcError> {
        let jwks: JwkSet = self
            .http
            .get(jwks_url)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(OidcError::Fetch)?
            .json()
            .await
            .map_err(OidcError::Fetch)?;
        self.install_jwks(&jwks)
    }

    /// Replace the cached keys with the ones declared by an already-parsed JWK
    /// set, without any network I/O. `refresh` calls this after a fetch; tests
    /// call it directly to install a locally-built key set, so no test needs a
    /// live HTTP server.
    pub fn install_jwks(&self, jwks: &JwkSet) -> Result<(), OidcError> {
        let entries = parse_signing_keys(jwks)?;
        if entries.is_empty() {
            return Err(OidcError::NoUsableKeys);
        }
        // Panic-free even on a poisoned lock: the cache is a plain data snapshot
        // with no broken invariant to protect, so recovering the guard is safe.
        let mut guard = self.keys.write().unwrap_or_else(|e| e.into_inner());
        *guard = Arc::new(entries);
        Ok(())
    }

    /// Whether any signing key is currently cached. Used by the readiness gate
    /// to refuse to serve OIDC before the first successful refresh.
    pub fn has_keys(&self) -> bool {
        !self.snapshot().is_empty()
    }

    fn snapshot(&self) -> Arc<Vec<DecodingEntry>> {
        self.keys.read().unwrap_or_else(|e| e.into_inner()).clone()
    }
}

/// Turn a JWK set into decoding entries, keeping only keys that declare a
/// supported, asymmetric *signature* algorithm. A key that omits `alg`,
/// declares an encryption algorithm (`RSA-OAEP`, `RSA1_5`, ...), or declares a
/// symmetric one (`HS256`/`HS384`/`HS512`), is skipped: without a declared
/// signing algorithm there is nothing safe to pin [`Validation`] to, admitting
/// the token's own `alg` instead is the attack this design forbids, and a
/// symmetric key in a *public* JWKS document is a published secret, never a
/// valid one to verify with (see [`signing_algorithm`]). A key whose
/// components will not parse is a hard error, not a skip.
fn parse_signing_keys(jwks: &JwkSet) -> Result<Vec<DecodingEntry>, OidcError> {
    let mut out = Vec::new();
    for jwk in &jwks.keys {
        let Some(alg) = jwk.common.key_algorithm.and_then(signing_algorithm) else {
            // No declared signing algorithm: cannot pin a Validation to it.
            continue;
        };
        let key = decoding_key(jwk)?;
        out.push(DecodingEntry {
            kid: jwk.common.key_id.clone(),
            alg,
            key,
        });
    }
    Ok(out)
}

fn decoding_key(jwk: &Jwk) -> Result<DecodingKey, OidcError> {
    DecodingKey::from_jwk(jwk).map_err(|e| OidcError::InvalidKey {
        kid: jwk.common.key_id.clone(),
        reason: e.to_string(),
    })
}

/// Map a JWKS `alg` to the `jsonwebtoken` signature algorithm it names, or
/// `None` for an algorithm we must not verify tokens with: a non-signature
/// (encryption/key-agreement) algorithm, or a symmetric (HMAC) one. A JWKS is
/// a *public* document by definition (that is the entire point of publishing
/// one), so a symmetric key inside it is a published verification secret, not
/// a usable signing key - admitting it would let anyone who can read the JWKS
/// forge tokens for any tenant.
fn signing_algorithm(alg: KeyAlgorithm) -> Option<Algorithm> {
    Some(match alg {
        KeyAlgorithm::HS256 | KeyAlgorithm::HS384 | KeyAlgorithm::HS512 => return None,
        KeyAlgorithm::ES256 => Algorithm::ES256,
        KeyAlgorithm::ES384 => Algorithm::ES384,
        KeyAlgorithm::RS256 => Algorithm::RS256,
        KeyAlgorithm::RS384 => Algorithm::RS384,
        KeyAlgorithm::RS512 => Algorithm::RS512,
        KeyAlgorithm::PS256 => Algorithm::PS256,
        KeyAlgorithm::PS384 => Algorithm::PS384,
        KeyAlgorithm::PS512 => Algorithm::PS512,
        KeyAlgorithm::EdDSA => Algorithm::EdDSA,
        // Encryption / key-agreement algorithms: never used to verify a JWT
        // signature.
        KeyAlgorithm::RSA1_5 | KeyAlgorithm::RSA_OAEP | KeyAlgorithm::RSA_OAEP_256 => return None,
    })
}

/// Resolves a tenant from a bearer JWT validated against a configured OIDC
/// issuer and its JWKS (ADR-0042 decision 6).
///
/// On each request it verifies the token's signature against the cached JWKS
/// keys and checks the issuer and expiry (and audience, if configured) via
/// [`jsonwebtoken::Validation`], then reads the tenant identity from a
/// configurable string claim (default `tenant`). Every failure maps to
/// [`AuthError`]; malformed input never panics.
///
/// # Algorithm pinning
///
/// The allowed signature algorithm is taken from the JWKS key that verifies the
/// token, never from the token's own `alg` header. A token whose `alg` is not
/// the one its key declares is rejected, which closes the `alg: none` and
/// algorithm-confusion attack class.
///
/// # Tenant claim
///
/// The tenant comes only from the configured claim, read as a string. A token
/// missing that claim, or whose value is not a non-empty string that parses as
/// a [`TenantId`], is [`AuthError`]: there is deliberately no silent fallback to
/// `sub` or any other claim.
pub struct OidcResolver {
    cache: Arc<OidcJwksCache>,
    issuer: String,
    audiences: Vec<String>,
    tenant_claim: String,
}

impl OidcResolver {
    /// Build a resolver reading validated JWTs against `cache`.
    ///
    /// `issuer` is the exact `iss` every token must carry. `audiences`, when
    /// non-empty, is the set of acceptable `aud` values (any match passes);
    /// empty disables audience checking. `tenant_claim` is the string claim the
    /// tenant identity is read from (e.g. `tenant`).
    pub fn new(
        cache: Arc<OidcJwksCache>,
        issuer: impl Into<String>,
        audiences: Vec<String>,
        tenant_claim: impl Into<String>,
    ) -> Self {
        OidcResolver {
            cache,
            issuer: issuer.into(),
            audiences,
            tenant_claim: tenant_claim.into(),
        }
    }

    /// The shared JWKS cache, so the caller can drive its background refresh.
    pub fn cache(&self) -> &Arc<OidcJwksCache> {
        &self.cache
    }
}

impl TenantResolver for OidcResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        let token = bearer_token(headers)?;
        // The header is untrusted; decoding it only tells us which cached key to
        // try, never which algorithm to trust.
        let header = decode_header(token).map_err(|_| AuthError)?;
        let entries = self.cache.snapshot();
        for entry in entries.iter() {
            // A token that names a `kid` is only tried against the key with that
            // `kid`; a token with no `kid` is tried against every cached key.
            match (&header.kid, &entry.kid) {
                (Some(want), Some(have)) if want != have => continue,
                (Some(_), None) => continue,
                _ => {}
            }

            let mut validation = Validation::new(entry.alg);
            validation.set_issuer(&[self.issuer.as_str()]);
            // Require `iss` and `exp` present, not merely valid-if-present, so a
            // token omitting either is rejected rather than silently accepted.
            validation.set_required_spec_claims(&["exp", "iss"]);
            if self.audiences.is_empty() {
                validation.validate_aud = false;
            } else {
                validation.set_audience(&self.audiences);
            }

            let Ok(data) = decode::<serde_json::Value>(token, &entry.key, &validation) else {
                continue;
            };
            // Signature, issuer, expiry (and audience) are now proven for this
            // token. The tenant claim is authoritative from here: a missing or
            // non-string value is a hard failure, never a fallback to another
            // key or claim.
            return match data.claims.get(&self.tenant_claim).and_then(|v| v.as_str()) {
                Some(tenant) if !tenant.is_empty() => Ok(TenantId::new(tenant.to_string())),
                _ => Err(AuthError),
            };
        }
        Err(AuthError)
    }
}

// --- mTLS (proxy-forwarded client certificate) tenant resolution ------------

/// Resolves a tenant from a trusted, reverse-proxy-forwarded client-certificate
/// identity header (ADR-0042 decision 6).
///
/// # Trust boundary (read this before enabling it)
///
/// Ravel does **not** terminate TLS or verify client certificates itself. This
/// resolver reads a plain request header (default `x-ravel-client-cert-cn`)
/// whose value a TLS-terminating reverse proxy in front of Ravel is expected to
/// have set to the already-verified certificate CN or SAN. It is therefore only
/// safe when Ravel is deployed behind a proxy that BOTH:
///
/// 1. actually performs mTLS client-certificate verification (an unverified or
///    self-asserted certificate must never reach this point), AND
/// 2. strips or overwrites any client-supplied value of this header before
///    forwarding, so a client cannot forge the identity by sending the header
///    itself.
///
/// This is the same trust class as `X-Forwarded-For`: a header that is
/// authoritative only because a trusted hop set it, and forgeable by anyone if
/// that hop is absent. Enabling this resolver on a Ravel that is directly
/// exposed, or behind a proxy that forwards the raw client header, hands tenant
/// selection to the client. It must be opt-in for exactly this reason.
///
/// This resolver is reachable from every ingress that ultimately builds an
/// `axum::http::HeaderMap` for a `TenantResolver` to read - which includes the
/// gRPC listener: gRPC metadata keys are copied into the same `HeaderMap` type
/// (see `services/ravel-server/src/otlp_grpc.rs`'s `metadata_to_headers` and
/// `flight_auth`). Point (2) above therefore applies to every listener this
/// process exposes, not only the HTTP one - a proxy that sanitizes the HTTP
/// vhost but forwards gRPC traffic (or a different, unsanitized gRPC vhost)
/// straight through leaves a live bypass on Flight SQL and OTLP gRPC ingest.
///
/// The header value maps straight to a [`TenantId`] with no further parsing:
/// certificate SAN/CN extraction already happened at the proxy, and duplicating
/// it here would be a second, weaker parser of a format Ravel never sees.
pub struct MtlsResolver {
    header_name: String,
}

impl MtlsResolver {
    /// The default header a proxy forwards the verified client-certificate
    /// identity in.
    pub const DEFAULT_HEADER: &'static str = "x-ravel-client-cert-cn";

    pub fn new(header_name: impl Into<String>) -> Self {
        MtlsResolver {
            header_name: header_name.into(),
        }
    }
}

impl Default for MtlsResolver {
    fn default() -> Self {
        MtlsResolver::new(MtlsResolver::DEFAULT_HEADER)
    }
}

impl TenantResolver for MtlsResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        let raw = headers.get(self.header_name.as_str()).ok_or(AuthError)?;
        let value = raw.to_str().map_err(|_| AuthError)?;
        if value.is_empty() {
            return Err(AuthError);
        }
        Ok(TenantId::new(value.to_string()))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    use jsonwebtoken::{EncodingKey, Header, encode, get_current_timestamp};

    const ISSUER: &str = "https://issuer.example.com";
    const KID: &str = "test-key-1";

    // A real P-256 keypair (openssl ecparam -name prime256v1 -genkey), used to
    // sign and verify ES256 tokens in these tests. Deliberately a genuine
    // asymmetric key, not an HMAC secret: `parse_signing_keys` now refuses
    // symmetric (oct) JWKS keys outright (a JWKS is a *public* document, so a
    // symmetric key inside one is a published verification secret), so a test
    // JWKS has to be built from a real keypair like any production one would
    // be.
    const EC_PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgwdzs5ZXprZ+FrmVM\n\
2x3N9ehoXMhkWfOx7M0217LOtmehRANCAASPG9SIk2E3UTJC4nxm1l6y6yzX8N38\n\
lVNfCuKwwBrIas5yannzOq4NUHTqcDRTLWg1ZyMnLrUgZ/WHc4/TGBXG\n\
-----END PRIVATE KEY-----\n";
    const EC_X: &str = "jxvUiJNhN1EyQuJ8ZtZesuss1_Dd_JVTXwrisMAayGo";
    const EC_Y: &str = "znJqefM6rg1QdOpwNFMtaDVnIycutSBn9Ydzj9MYFcY";

    // A second, unrelated P-256 keypair: the JWKS trusts the key above, so a
    // token correctly ES256-signed with THIS key's private half must still
    // fail (it is not the key the JWKS names for `kid`).
    const OTHER_EC_PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgZ6KBsuAiIsR5NQD7\n\
Vae0EmUbQ1GlLHprkMJ5vohUznahRANCAASlmFCeM19pdfZHYImxsgltyYZcRbOA\n\
v6bMjpirtMaaPWvO2P5A4cSa7KfhIJqC4wghlS4L0XBZRxbg48yAf+JK\n\
-----END PRIVATE KEY-----\n";

    /// An EC (P-256) JWK set carrying the real public key above under key id
    /// `kid`, declaring `ES256`.
    fn jwks_with(kid: &str) -> JwkSet {
        let doc = serde_json::json!({
            "keys": [{
                "kty": "EC", "crv": "P-256", "kid": kid, "alg": "ES256",
                "x": EC_X, "y": EC_Y,
            }]
        });
        serde_json::from_value(doc).expect("valid JWKS")
    }

    fn cache_with(jwks: &JwkSet) -> Arc<OidcJwksCache> {
        let cache = OidcJwksCache::new().expect("client builds");
        cache.install_jwks(jwks).expect("keys install");
        Arc::new(cache)
    }

    /// Sign a claims object as an ES256 JWT with `kid`, using the given PEM
    /// EC private key.
    fn sign_es256(claims: &serde_json::Value, kid: &str, priv_pem: &str) -> String {
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(kid.to_string());
        let key = EncodingKey::from_ec_pem(priv_pem.as_bytes()).expect("valid EC PEM");
        encode(&header, claims, &key).expect("token encodes")
    }

    /// Sign a claims object as an HS256 JWT, keyed on the EC public key's raw
    /// coordinate bytes reinterpreted as an HMAC secret - the classic
    /// algorithm-confusion attack shape (an RS256/ES256 public key, which is
    /// by definition published, reused as if it were a symmetric secret).
    fn sign_hs256_confused(claims: &serde_json::Value, kid: &str) -> String {
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(kid.to_string());
        let secret = [EC_X.as_bytes(), EC_Y.as_bytes()].concat();
        encode(&header, claims, &EncodingKey::from_secret(&secret)).expect("token encodes")
    }

    fn bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().expect("valid header"),
        );
        headers
    }

    fn claims(tenant: Option<&str>, exp_offset_secs: i64) -> serde_json::Value {
        let exp = (get_current_timestamp() as i64 + exp_offset_secs).max(0);
        let mut obj = serde_json::Map::new();
        obj.insert("iss".into(), ISSUER.into());
        obj.insert("sub".into(), "user-123".into());
        obj.insert("exp".into(), serde_json::json!(exp));
        if let Some(t) = tenant {
            obj.insert("tenant".into(), t.into());
        }
        serde_json::Value::Object(obj)
    }

    fn resolver(cache: Arc<OidcJwksCache>) -> OidcResolver {
        OidcResolver::new(cache, ISSUER, Vec::new(), "tenant")
    }

    #[test]
    fn valid_token_resolves_claimed_tenant() {
        let cache = cache_with(&jwks_with(KID));
        let token = sign_es256(&claims(Some("acme"), 3600), KID, EC_PRIV_PEM);
        let tenant = resolver(cache)
            .resolve(&bearer(&token))
            .expect("valid token resolves");
        assert_eq!(tenant, TenantId::new("acme"));
    }

    #[test]
    fn expired_token_is_auth_error() {
        let cache = cache_with(&jwks_with(KID));
        // Past the default 60s leeway.
        let token = sign_es256(&claims(Some("acme"), -3600), KID, EC_PRIV_PEM);
        assert!(resolver(cache).resolve(&bearer(&token)).is_err());
    }

    #[test]
    fn token_signed_with_key_not_in_jwks_is_auth_error() {
        // The JWKS trusts EC_PRIV_PEM's public half under `kid`; the token is
        // correctly ES256-signed, but with the OTHER keypair's private half,
        // so signature verification against the cached key fails.
        let cache = cache_with(&jwks_with(KID));
        let token = sign_es256(&claims(Some("acme"), 3600), KID, OTHER_EC_PRIV_PEM);
        assert!(resolver(cache).resolve(&bearer(&token)).is_err());
    }

    #[test]
    fn algorithm_confusion_reusing_the_public_key_as_an_hmac_secret_is_auth_error() {
        // The JWKS declares ES256 for this key, so the resolver only admits
        // ES256 for it. A token whose header claims HS256 - signed with the
        // EC public key's own (published) coordinate bytes reused as an HMAC
        // secret, the classic algorithm-confusion attack - must be rejected
        // regardless of which of jsonwebtoken's two independent gates catches
        // it first: the allow-list this resolver pins Validation to (which
        // only contains ES256 for this key) excludes HS256, and separately
        // the key's family (EC) does not match HS256's family (HMAC).
        let cache = cache_with(&jwks_with(KID));
        let token = sign_hs256_confused(&claims(Some("acme"), 3600), KID);
        assert!(resolver(cache).resolve(&bearer(&token)).is_err());
    }

    #[test]
    fn token_missing_tenant_claim_is_auth_error() {
        // A perfectly valid, correctly-signed token with no `tenant` claim must
        // not fall back to `sub` or anything else.
        let cache = cache_with(&jwks_with(KID));
        let token = sign_es256(&claims(None, 3600), KID, EC_PRIV_PEM);
        assert!(resolver(cache).resolve(&bearer(&token)).is_err());
    }

    #[test]
    fn wrong_issuer_is_auth_error() {
        let cache = cache_with(&jwks_with(KID));
        let mut c = claims(Some("acme"), 3600);
        c["iss"] = "https://evil.example.com".into();
        let token = sign_es256(&c, KID, EC_PRIV_PEM);
        assert!(resolver(cache).resolve(&bearer(&token)).is_err());
    }

    #[test]
    fn missing_authorization_header_is_auth_error() {
        let cache = cache_with(&jwks_with(KID));
        assert!(resolver(cache).resolve(&HeaderMap::new()).is_err());
    }

    #[test]
    fn empty_cache_rejects_every_token() {
        let cache = Arc::new(OidcJwksCache::new().expect("client builds"));
        assert!(!cache.has_keys());
        let token = sign_es256(&claims(Some("acme"), 3600), KID, EC_PRIV_PEM);
        assert!(resolver(cache).resolve(&bearer(&token)).is_err());
    }

    #[test]
    fn jwks_with_no_signing_keys_is_error() {
        // A JWK that omits `alg` declares no signing algorithm, so the set has
        // no usable key and installing it fails loudly rather than silently
        // caching nothing.
        let doc = serde_json::json!({
            "keys": [{ "kty": "EC", "crv": "P-256", "kid": KID, "x": EC_X, "y": EC_Y }]
        });
        let jwks: JwkSet = serde_json::from_value(doc).expect("valid JWKS");
        let cache = OidcJwksCache::new().expect("client builds");
        assert!(matches!(
            cache.install_jwks(&jwks),
            Err(OidcError::NoUsableKeys)
        ));
    }

    #[test]
    fn symmetric_jwks_key_is_never_installed() {
        // A JWKS is a *public* document; an oct (HMAC) key inside one is a
        // published secret, never a valid signing key to trust. Confirm it is
        // rejected outright, not silently accepted as if it were RSA/EC.
        let doc = serde_json::json!({
            "keys": [{ "kty": "oct", "kid": KID, "alg": "HS256", "k": EC_X }]
        });
        let jwks: JwkSet = serde_json::from_value(doc).expect("valid JWKS");
        let cache = OidcJwksCache::new().expect("client builds");
        assert!(matches!(
            cache.install_jwks(&jwks),
            Err(OidcError::NoUsableKeys)
        ));
    }

    #[test]
    fn mtls_maps_header_to_tenant() {
        let resolver = MtlsResolver::default();
        let mut headers = HeaderMap::new();
        headers.insert(MtlsResolver::DEFAULT_HEADER, "acme".parse().unwrap());
        assert_eq!(
            resolver.resolve(&headers).expect("resolves"),
            TenantId::new("acme")
        );
    }

    #[test]
    fn mtls_absent_or_empty_header_is_auth_error() {
        let resolver = MtlsResolver::default();
        assert!(resolver.resolve(&HeaderMap::new()).is_err());

        let mut headers = HeaderMap::new();
        headers.insert(MtlsResolver::DEFAULT_HEADER, "".parse().unwrap());
        assert!(resolver.resolve(&headers).is_err());
    }

    #[test]
    fn mtls_custom_header_name() {
        let resolver = MtlsResolver::new("x-my-cert-cn");
        let mut headers = HeaderMap::new();
        headers.insert("x-my-cert-cn", "beta".parse().unwrap());
        assert_eq!(
            resolver.resolve(&headers).expect("resolves"),
            TenantId::new("beta")
        );
    }
}
