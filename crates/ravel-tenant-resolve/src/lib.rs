//! Shared tenant-resolution primitives, extracted from ravel-query's HTTP
//! layer as a mechanical, behavior-preserving move (ADR-0080 decision 3) so
//! `ravel-server` and the future `ravel-ingest-router` depend on one resolver
//! implementation rather than two copies that can drift.
//!
//! There is deliberately no default resolver: callers must construct one of
//! the impls below (or their own), so an unauthenticated deployment is a
//! conscious choice, not an oversight (docs/query-engine.md "default deny").

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

/// Tries each resolver in order, returning the first successful match.
pub struct FallbackResolver {
    resolvers: Vec<Arc<dyn TenantResolver>>,
}

impl FallbackResolver {
    pub fn new(resolvers: Vec<Arc<dyn TenantResolver>>) -> Self {
        FallbackResolver { resolvers }
    }
}

impl TenantResolver for FallbackResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        for resolver in &self.resolvers {
            if let Ok(tenant) = resolver.resolve(headers) {
                return Ok(tenant);
            }
        }
        Err(AuthError)
    }
}

/// Keyed hash of a bearer token under a per-resolver secret key:
/// `blake3::keyed_hash(hash_key, token)`. Mirrors
/// `ravel_catalog::auth_token_map::token_hash`: the plaintext token is hashed
/// before it is ever used as a lookup key, so a configured secret is never
/// stored in the clear and never compared byte-by-byte against attacker-supplied
/// input. Because `hash_key` is a process-local secret, the hash an attacker's
/// candidate token lands on is unpredictable to them, so the map lookup's
/// comparison order reveals nothing about any configured token.
fn token_hash(hash_key: &[u8; 32], token: &[u8]) -> [u8; 32] {
    *blake3::keyed_hash(hash_key, token).as_bytes()
}

/// A process-local random key, generated once per resolver at construction.
/// [`StaticBearerTokenResolver`] has no natural deployment-key input the way
/// `ravel_catalog::auth_token_map` does (that map is a durable object whose
/// hashes must stay stable across processes and be readable under a configured
/// key; this resolver is in-memory and rebuilt from plaintext every startup), so
/// the key need only be unpredictable within this process's lifetime. Sourced
/// from two v4 UUIDs (244 bits of `getrandom` entropy) to avoid pulling in a
/// separate RNG crate; `uuid` is already a dependency of this crate.
fn random_hash_key() -> [u8; 32] {
    let mut key = [0u8; 32];
    key[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    key[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    key
}

/// Resolves a tenant from a static `Authorization: Bearer <token>` map.
///
/// Configured tokens are never held in the clear: each is stored only as its
/// keyed hash under a process-local secret [`random_hash_key`], and an incoming
/// token is hashed the same way before lookup. This closes the timing
/// side-channel a direct `HashMap<String, _>::get(plaintext)` opens, where the
/// per-byte string comparison against a stored secret leaks how far a candidate
/// matched (mirroring `ravel_catalog::auth_token_map`).
pub struct StaticBearerTokenResolver {
    hash_key: [u8; 32],
    tokens: HashMap<[u8; 32], TenantId>,
}

impl StaticBearerTokenResolver {
    pub fn new(tokens: HashMap<String, TenantId>) -> Self {
        let hash_key = random_hash_key();
        let tokens = tokens
            .into_iter()
            .map(|(token, tenant)| (token_hash(&hash_key, token.as_bytes()), tenant))
            .collect();
        StaticBearerTokenResolver { hash_key, tokens }
    }
}

impl TenantResolver for StaticBearerTokenResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        let token = bearer_token(headers)?;
        let hash = token_hash(&self.hash_key, token.as_bytes());
        self.tokens.get(&hash).cloned().ok_or(AuthError)
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

// --- OIDC (JWT) tenant resolution (ADR-0042 decision 6) ----------

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
        // jsonwebtoken 10.x surfaces an unrecognized `alg` as this variant
        // instead of failing to parse the JWK. An algorithm we do not recognize
        // is one we cannot pin a Validation to, so it is skipped exactly like a
        // key that omits `alg`: never verified against, never silently trusted.
        KeyAlgorithm::UNKNOWN_ALGORITHM => return None,
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
/// Its **one legitimate source** is `--mtls-listener`, per
/// `docs/adrs/0050-fail-closed-isolation-and-startup-invariants.md` section 1:
/// `services/ravel-server` installs this resolver only in the router chain
/// bound to that dedicated listener address. The public HTTP listener
/// (`--listen-http`) and the public gRPC/Flight listener (`--listen-grpc`) are
/// built with a resolver chain that structurally never contains an
/// `MtlsResolver`, so the header has no effect there regardless of what a
/// proxy in front of them does or does not strip - the old bypass (gRPC
/// metadata is copied into the same `HeaderMap` type any `TenantResolver`
/// reads, see `services/ravel-server/src/otlp_grpc.rs`'s
/// `metadata_to_headers` and `flight_auth`) is closed by construction, not by
/// convention. `services/ravel-server` refuses to start rather than run with
/// this resolver reachable from a public listener; see
/// `ravel_server::config::Cli::validate`.
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
    fn fallback_resolver_tries_each_resolver_in_order_returning_first_success() {
        // FallbackResolver returns the first resolver that succeeds, in order,
        // and only AuthError when every resolver in the chain fails. A resolver
        // that always yields a fixed tenant (or always fails) makes the order
        // observable without depending on any header shape.
        struct Always(Option<&'static str>);
        impl TenantResolver for Always {
            fn resolve(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
                self.0.map(TenantId::new).ok_or(AuthError)
            }
        }

        // The first resolver fails, so the second (first success) wins; the
        // third must never be consulted, so its distinct tenant proves order.
        let fallback = FallbackResolver::new(vec![
            Arc::new(Always(None)),
            Arc::new(Always(Some("first-success"))),
            Arc::new(Always(Some("later"))),
        ]);
        assert_eq!(
            fallback
                .resolve(&HeaderMap::new())
                .expect("the second resolver succeeds"),
            TenantId::new("first-success"),
        );

        // Every resolver failing is an AuthError, not a panic or a default.
        let all_fail = FallbackResolver::new(vec![Arc::new(Always(None)), Arc::new(Always(None))]);
        assert!(all_fail.resolve(&HeaderMap::new()).is_err());

        // An empty chain trusts nobody (default deny).
        let empty = FallbackResolver::new(Vec::new());
        assert!(empty.resolve(&HeaderMap::new()).is_err());
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
    fn exp_claim_as_json_string_is_auth_error() {
        // CVE-2026-25537 / GHSA-h395-gr6q-cpjc regression: `exp` sent as a JSON
        // string ("99999999999") instead of a number is the CVE's exact PoC
        // shape. Under the type-confusion bug a wrong-typed claim was treated as
        // absent rather than malformed, so a far-future string `exp` could slip
        // past expiry checking. Our `required_spec_claims` requires `exp`, and
        // the patched crate only counts a correctly-typed (parsed) claim as
        // present, so the malformed string must produce a hard rejection here,
        // not a silent pass.
        let cache = cache_with(&jwks_with(KID));
        let mut c = claims(Some("acme"), 3600);
        c["exp"] = serde_json::json!("99999999999");
        let token = sign_es256(&c, KID, EC_PRIV_PEM);
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

    fn static_bearer(token: &str) -> HeaderMap {
        bearer(token)
    }

    #[test]
    fn static_bearer_stores_only_keyed_hashes_never_plaintext() {
        // The load-bearing security property: no configured token string is
        // ever held as a map key. The map is keyed by the keyed hash under the
        // resolver's process-local secret, so the entry for a token is found
        // only via `token_hash`, and the plaintext appears nowhere in the map.
        let mut tokens = HashMap::new();
        tokens.insert("super-secret-token".to_string(), TenantId::new("acme"));
        let resolver = StaticBearerTokenResolver::new(tokens);

        let expected = token_hash(&resolver.hash_key, b"super-secret-token");
        assert!(
            resolver.tokens.contains_key(&expected),
            "the entry must be keyed by the keyed hash of the token"
        );
        assert_eq!(resolver.tokens.get(&expected), Some(&TenantId::new("acme")));

        // The hash is not the plaintext, and every stored key is a 32-byte
        // hash, so no plaintext secret can be recovered from the map's keys.
        assert_ne!(
            expected.as_slice(),
            b"super-secret-token".as_slice(),
            "sanity: the hash is not the plaintext"
        );
        assert!(
            resolver
                .tokens
                .keys()
                .all(|k| k.len() == 32 && k.as_slice() != b"super-secret-token"),
            "no map key is the raw token; all are keyed hashes"
        );
    }

    #[test]
    fn static_bearer_resolves_valid_and_rejects_invalid() {
        let mut tokens = HashMap::new();
        tokens.insert("tok-a".to_string(), TenantId::new("tenant-a"));
        tokens.insert("tok-b".to_string(), TenantId::new("tenant-b"));
        let resolver = StaticBearerTokenResolver::new(tokens);

        assert_eq!(
            resolver
                .resolve(&static_bearer("tok-a"))
                .expect("valid token resolves"),
            TenantId::new("tenant-a")
        );
        assert_eq!(
            resolver
                .resolve(&static_bearer("tok-b"))
                .expect("valid token resolves"),
            TenantId::new("tenant-b")
        );
        assert!(
            resolver.resolve(&static_bearer("tok-unknown")).is_err(),
            "an unconfigured token is rejected"
        );
        assert!(
            resolver.resolve(&HeaderMap::new()).is_err(),
            "a missing Authorization header is rejected"
        );
    }

    #[test]
    fn static_bearer_hash_key_is_per_resolver() {
        // Two resolvers over the same token map hash it under independent
        // process-local keys, so a stored hash is meaningless outside the
        // resolver that produced it (the key is load-bearing).
        let mut tokens = HashMap::new();
        tokens.insert("tok".to_string(), TenantId::new("t"));
        let a = StaticBearerTokenResolver::new(tokens.clone());
        let b = StaticBearerTokenResolver::new(tokens);
        assert_ne!(
            a.hash_key, b.hash_key,
            "each resolver generates its own random key"
        );
        // Both still resolve their own configured token correctly.
        assert_eq!(
            a.resolve(&static_bearer("tok")).expect("resolves"),
            TenantId::new("t")
        );
        assert_eq!(
            b.resolve(&static_bearer("tok")).expect("resolves"),
            TenantId::new("t")
        );
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

    // A real 2048-bit RSA keypair (openssl genrsa 2048), used to sign and
    // verify RS256 tokens. Real OIDC issuers overwhelmingly sign with RS256
    // (RSA), which exercises a different `DecodingKey::from_jwk` branch and a
    // different key-family check in `jsonwebtoken::decode` than the ES256/EC
    // keypair above. Signed via `EncodingKey::from_rsa_pem` under the
    // `aws_lc_rs` backend; no extra dependency is pulled in for
    // RSA key generation.
    const RSA_PRIV_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCu7ZpSqycgiMDi\n\
kxGgRGL3F+xF2uhh6OgeOEmqvcqyicDeOcZoFxcczrg3ZgT58hr9iVDcbVh4I962\n\
vY2N0XcRoHzcR3N9VGch6u76tSLtRNSNLfh6ja6ziDSVssVYTyBf+e+T1QvT5sUF\n\
8/XlMm0FkTKfHdb6lb1PhXLx9l2WPOdd2bK3YbpWFUkIIv5oXtuFaU+3ikZrco4i\n\
77Xb/mfHago49cD89sbKHjJ9jW0K50KUF3jntUm/M/TTN5ztcnJBagl/kkR7O+ld\n\
XGdmRom4h7KapIGG7sCVNH00Uu9nqpkVSlPmCdoNXlDaxq4ZRc4iTZls8tIRra16\n\
BIZOEN+PAgMBAAECggEAER4FBOPkn0VigolbpzAp8v3vS+Kg7LvKwvJFGyUZSaE7\n\
M0O6C4N+6n27/wfHouGzDG48cGVuy8rOx1kDGgaOPTZUIYYIYhI5SVNg8T16Xndm\n\
yS3fa7ajisPgSWnF799GTr35WKD3WFPzoaJ+xF/L1UihCHr2B21Rqg9n8Q9nlwTT\n\
C1pZ7rncqOM1S78IJJcVxjz1IpzWcLhu5j0e4Z32qdhnTF1FiDBD8Yxi9tJEf2bK\n\
8FSI5Mm0KmGzzX7GO/DoqX8VxKBBMrtONvVIO9i1wH+R0jacHq5xv7CcIRE9j0v2\n\
YJ2KIU7KiRyn3QOzKdAkXDzzrKZKLbXnOou/WYkd+QKBgQDp83Nky96jQfQwI+6e\n\
TQiR9hNuRtDweK6tEhQ8Ye9p4UaHWbJYmM3777JZqLr5mzlvDYtmDdpMzrMf/csu\n\
ShIBNSoJSObIrkrDIclvknDapm9dSNULTnqQoPogqXAN9zJih0xsm0DCuqPUuior\n\
4pIfCzPufePw0sCLG7pLxDc2nQKBgQC/ahq5+jXfH0/Vv+e8S24k8Pzi7zpkN4zt\n\
Dft8FP10yP7Hs5vzutTWb+eJBm6Z2fizWJTjA+f2qzWTrZnzVwvKFzzgaOMOyFTK\n\
Am4nuZw3dx3y99DitWCmWSDwvAL9hVOKZrcEhNSsgQbZylL4jYNqQT3/Vpv1zdUv\n\
cZyV5s2BGwKBgDCpsxcEUQskbOaWksvaui2iQehuUoeykqLtX8gvlt0vPrxoq/BB\n\
2JbPBQohTsMcxpWS+6v+tanEVP4SjHDUd2pI5LWJtHeJyYNNQ9kxXMgeVovQ2n+/\n\
kz8CPQUOOYCuKozUF9F/ebkHmYxmLN90AXDzo5m4FfHB5MsKuXWJGvMBAoGAXopY\n\
cvzK+M3tT4R+P3j+CM7iCG/h5jetqjPavzlaygCwHhBu+V2Q2+zfbcU4gVKwTFx3\n\
BP0b57A+QRdgT1jx4LnDfo8vflCh2DiFEafSKW7y4ttVV3QALYkeBOjHjVH5pgT/\n\
ZgL5S85ahN0yR8MVYjihF2k+lJQ6NDmn/j3FyHsCgYEAseHN0zEs0790JV7nK+G4\n\
HlpT5YAV5RMbK3HYayhebZ060wRNpl412J6sOAJktrYTNRJpGyn5MMfMVPWgmqb4\n\
iZ6SUslXxIqalyqaBs18wn8keaQfGPrJDRlOPFDuBUvCxUeLV7UuZc/IkEntN4gy\n\
JGhDLd2EhXX5RDhGuladnj8=\n\
-----END PRIVATE KEY-----\n";
    // The public modulus (`n`) and exponent (`e`) of RSA_PRIV_PEM, base64url
    // encoded, as an RS256 JWKS carries them.
    const RSA_N: &str = "ru2aUqsnIIjA4pMRoERi9xfsRdroYejoHjhJqr3KsonA3jnGaBcXHM64N2YE-fIa_YlQ3G1YeCPetr2NjdF3EaB83EdzfVRnIeru-rUi7UTUjS34eo2us4g0lbLFWE8gX_nvk9UL0-bFBfP15TJtBZEynx3W-pW9T4Vy8fZdljznXdmyt2G6VhVJCCL-aF7bhWlPt4pGa3KOIu-12_5nx2oKOPXA_PbGyh4yfY1tCudClBd457VJvzP00zec7XJyQWoJf5JEezvpXVxnZkaJuIeymqSBhu7AlTR9NFLvZ6qZFUpT5gnaDV5Q2sauGUXOIk2ZbPLSEa2tegSGThDfjw";
    const RSA_E: &str = "AQAB";
    const RSA_KID: &str = "test-rsa-1";

    /// An RSA JWK set carrying the real public key above under key id `kid`,
    /// declaring `RS256`.
    fn jwks_rsa_with(kid: &str) -> JwkSet {
        let doc = serde_json::json!({
            "keys": [{
                "kty": "RSA", "kid": kid, "alg": "RS256", "use": "sig",
                "n": RSA_N, "e": RSA_E,
            }]
        });
        serde_json::from_value(doc).expect("valid JWKS")
    }

    /// Sign a claims object as an RS256 JWT with `kid`, using the RSA PEM
    /// private key.
    fn sign_rs256(claims: &serde_json::Value, kid: &str) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        let key = EncodingKey::from_rsa_pem(RSA_PRIV_PEM.as_bytes()).expect("valid RSA PEM");
        encode(&header, claims, &key).expect("token encodes")
    }

    /// The RSA algorithm-confusion shape, mirroring `sign_hs256_confused` for
    /// RSA: an HS256 token whose HMAC secret is the RSA public key's own
    /// modulus bytes (base64url-decoded from `RSA_N`). An RSA public key is by
    /// definition published in the JWKS, so reusing it as a symmetric secret is
    /// exactly the forge-any-token attack the resolver must reject.
    fn sign_hs256_confused_rsa(claims: &serde_json::Value, kid: &str) -> String {
        use base64::Engine;
        let modulus = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(RSA_N)
            .expect("RSA_N is valid base64url");
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some(kid.to_string());
        encode(&header, claims, &EncodingKey::from_secret(&modulus)).expect("token encodes")
    }

    #[test]
    fn valid_rs256_token_resolves_claimed_tenant() {
        // Real-world OIDC issuers overwhelmingly use RS256. A validly
        // RS256-signed token must resolve the claimed tenant just like ES256.
        let cache = cache_with(&jwks_rsa_with(RSA_KID));
        let token = sign_rs256(&claims(Some("acme"), 3600), RSA_KID);
        let tenant = resolver(cache)
            .resolve(&bearer(&token))
            .expect("valid RS256 token resolves");
        assert_eq!(tenant, TenantId::new("acme"));
    }

    #[test]
    fn rsa_algorithm_confusion_reusing_the_modulus_as_an_hmac_secret_is_auth_error() {
        // The JWKS declares RS256 for this key, so the resolver only admits
        // RS256 for it. A token whose header claims HS256, signed with the RSA
        // public key's own (published) modulus bytes reused as an HMAC secret -
        // the classic RSA/HMAC algorithm-confusion attack - must be rejected,
        // caught by either the ES256-style allow-list pinning (which excludes
        // HS256) or the key-family mismatch (RSA key vs HMAC family).
        let cache = cache_with(&jwks_rsa_with(RSA_KID));
        let token = sign_hs256_confused_rsa(&claims(Some("acme"), 3600), RSA_KID);
        assert!(resolver(cache).resolve(&bearer(&token)).is_err());
    }
}
