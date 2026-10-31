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
use std::sync::atomic::{AtomicU64, Ordering};
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

/// The tenant side of a `TOKEN=TENANT` pair carried a `;`-suffix
/// [`split_tenant_suffix`] does not grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TenantSuffixError {
    #[error("the tenant before ';' is empty")]
    EmptyTenant,
    #[error("the capability list after ';' is empty or has an empty entry")]
    EmptyCapability,
    #[error(
        "unknown capability after ';', expected ddl, ipcrypt, agents, agents_worker, agents_admin \
         or subject=LABEL"
    )]
    UnknownCapability,
    #[error("a capability after ';' is named more than once")]
    DuplicateCapability,
    #[error("the tenant before the capability list itself ends in a ';' capability list")]
    CapabilityInTenant,
    #[error("subject after ';' takes a label, written subject=LABEL")]
    SubjectWithoutLabel,
    #[error("the subject=LABEL entry after ';' has an empty label")]
    EmptySubjectLabel,
    #[error("the subject=LABEL entry after ';' has a label longer than 64 bytes")]
    SubjectLabelTooLong,
    #[error("the subject=LABEL entry after ';' has a label byte outside A-Z a-z 0-9 . _ @ + -")]
    SubjectLabelCharset,
    #[error("the subject=LABEL entry after ';' appears more than once")]
    DuplicateSubject,
}

/// The capabilities a static token's `;`-suffix grants.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenCapabilities {
    /// Tenant-scoped DDL (ADR-2040 decision 4).
    pub ddl: bool,
    /// The IPCrypt SQL functions (ADR-2993 decision 6).
    pub ipcrypt: bool,
    /// Ravel Agents client operations (ADR-3331 decision 9).
    pub agents: bool,
    /// Ravel Agents worker registration, turn commits and effect completion
    /// (ADR-3331 decision 9).
    pub agents_worker: bool,
    /// Ravel Agents reconciliation (ADR-3331 decision 9).
    pub agents_admin: bool,
}

/// Every word a `;`-suffix may name.
const CAPABILITY_WORDS: [&str; 5] = ["ddl", "ipcrypt", "agents", "agents_worker", "agents_admin"];

/// The key of the one `subject=LABEL` entry a `;`-suffix may hold (ADR-3541
/// decision 3).
const SUBJECT_ENTRY: &str = "subject";

/// The longest static subject label, in bytes (ADR-3541 decision 3).
pub const SUBJECT_LABEL_MAX_BYTES: usize = 64;

/// Parses the label of a `subject=LABEL` suffix entry into a [`Subject`] with
/// [`SubjectSource::StaticToken`]. A label is 1 to
/// [`SUBJECT_LABEL_MAX_BYTES`] bytes drawn from `A-Z a-z 0-9 . _ @ + -`
/// (ADR-3541 decision 3), a strict subset of the [`Subject`] bound, so a comma,
/// semicolon, space or `=` cannot appear in one. The error names the rule and
/// never the label.
pub fn parse_subject_label(label: &str) -> Result<Subject, TenantSuffixError> {
    if label.is_empty() {
        return Err(TenantSuffixError::EmptySubjectLabel);
    }
    if label.len() > SUBJECT_LABEL_MAX_BYTES {
        return Err(TenantSuffixError::SubjectLabelTooLong);
    }
    if !label
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'@' | b'+' | b'-'))
    {
        return Err(TenantSuffixError::SubjectLabelCharset);
    }
    Subject::new(SubjectSource::StaticToken, label)
        .map_err(|_| TenantSuffixError::SubjectLabelCharset)
}

/// Splits the tenant side of a `TOKEN=TENANT` pair on its LAST `;`. The
/// suffix after it is a comma-separated capability list drawn from `ddl`
/// (ADR-2040 decision 4), `ipcrypt` (ADR-2993 decision 6), and `agents`,
/// `agents_worker` and `agents_admin` (ADR-3331 decision 9), each at most
/// once and the list never empty, so `acme;ddl`, `acme;ipcrypt` and
/// `acme;ddl,ipcrypt` all name tenant `acme`. The list may also hold one
/// `subject=LABEL` entry anywhere in it, beside or instead of the words
/// (ADR-3541 decision 3), parsed by [`parse_subject_label`] and returned as
/// the third element. A tenant with no `;` is returned unchanged with no
/// capability and no subject. An unknown word (matching is case-sensitive),
/// a repeated one, an empty list or entry, a bad or repeated `subject=` entry,
/// or an empty tenant before the `;` is a [`TenantSuffixError`], so a typo
/// refuses startup instead of silently granting nothing or naming an empty
/// tenant. So is a tenant whose own last `;` segment names a capability or a
/// subject entry (`acme;ddl;ipcrypt`, `acme;subject=al;ce`), which would
/// otherwise name tenant `acme;ddl` rather than `acme`.
///
/// `ravel-server` and `ravel-ingest-router` both call it, so the two accept
/// and refuse the same spellings. The router discards the capabilities and
/// the subject, as it never routes a request differently by them.
pub fn split_tenant_suffix(
    raw: &str,
) -> Result<(&str, TokenCapabilities, Option<Subject>), TenantSuffixError> {
    let Some(idx) = raw.rfind(';') else {
        return Ok((raw, TokenCapabilities::default(), None));
    };
    let (tenant, suffix) = (&raw[..idx], &raw[idx + 1..]);
    if tenant.is_empty() {
        return Err(TenantSuffixError::EmptyTenant);
    }
    if let Some(tenant_idx) = tenant.rfind(';')
        && tenant[tenant_idx + 1..].split(',').any(|word| {
            CAPABILITY_WORDS.contains(&word)
                || word == SUBJECT_ENTRY
                || word
                    .strip_prefix(SUBJECT_ENTRY)
                    .is_some_and(|rest| rest.starts_with('='))
        })
    {
        return Err(TenantSuffixError::CapabilityInTenant);
    }
    let mut caps = TokenCapabilities::default();
    let mut subject = None;
    for word in suffix.split(',') {
        if word == SUBJECT_ENTRY {
            return Err(TenantSuffixError::SubjectWithoutLabel);
        }
        if let Some(label) = word
            .strip_prefix(SUBJECT_ENTRY)
            .and_then(|rest| rest.strip_prefix('='))
        {
            if subject.is_some() {
                return Err(TenantSuffixError::DuplicateSubject);
            }
            subject = Some(parse_subject_label(label)?);
            continue;
        }
        let slot = match word {
            "" => return Err(TenantSuffixError::EmptyCapability),
            "ddl" => &mut caps.ddl,
            "ipcrypt" => &mut caps.ipcrypt,
            "agents" => &mut caps.agents,
            "agents_worker" => &mut caps.agents_worker,
            "agents_admin" => &mut caps.agents_admin,
            _ => return Err(TenantSuffixError::UnknownCapability),
        };
        if *slot {
            return Err(TenantSuffixError::DuplicateCapability);
        }
        *slot = true;
    }
    Ok((tenant, caps, subject))
}

/// A resolved caller identity: the tenant a request is attributed to and the
/// capabilities it holds. Each capability is absent unless a resolver's
/// configuration explicitly grants it.
///
/// - `ddl`: may run tenant-scoped DDL (ADR-2040 decision 4, "Who may run
///   DDL"). `POST /api/v1/sql` in `ravel-server`
///   (`services/ravel-server/src/sql.rs`) refuses a `CREATE` or `DROP`
///   statement with 403 unless it is set.
/// - `ipcrypt`: may call `ipcrypt_encrypt` and `ipcrypt_prefix_encrypt`
///   (ADR-2993 decision 6). Without it the SQL session does not register
///   them, so a call fails as an unknown function.
/// - `agents`: may run Ravel Agents client operations (ADR-3331 decision 9).
/// - `agents_worker`: may register as a Ravel Agents worker, commit turns and
///   complete effects (ADR-3331 decision 9). It does not imply `agents`.
/// - `agents_admin`: may run Ravel Agents reconciliation (ADR-3331 decision
///   9). It implies neither of the other two.
///
/// `subject` names who, within the tenant, the credential belongs to
/// (ADR-3541 decision 1). It is attribution only and grants nothing: no
/// capability field depends on it, so one OIDC token resolves to the same
/// capabilities whether or not its subject claim is read
/// (`a_subject_grants_nothing`), and outside
/// this crate's tests no code reads the field; the one comparison that sees it
/// is the whole-principal equality in `ravel-server`'s
/// `Cli::parse_tenant_pairs` duplicate-token check. It is `None` unless a
/// source produced one: a static token's `subject=LABEL` entry
/// ([`split_tenant_suffix`]) or the claim
/// [`OidcResolver::with_subject_claim`] names.
/// [`DevHeaderTenantResolver`] never sets one
/// (`the_dev_header_resolver_never_sets_a_subject`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub tenant: TenantId,
    pub ddl: bool,
    pub ipcrypt: bool,
    pub agents: bool,
    pub agents_worker: bool,
    pub agents_admin: bool,
    pub subject: Option<Subject>,
}

/// Where a [`Subject`] came from (ADR-3541 decision 1). A static label `alice`
/// and an OIDC `sub` of `alice` are different claims made by different
/// parties, so the source travels with the value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubjectSource {
    /// A `subject=LABEL` entry on a static bearer token.
    StaticToken,
    /// The claim [`OidcResolver::with_subject_claim`] names.
    Oidc,
    /// A proxy-forwarded mTLS subject header. Declared for ADR-3541's mTLS
    /// source; no resolver produces it yet.
    Mtls,
}

impl SubjectSource {
    /// Every source, in the order [`SubjectDropCounters::snapshot`] reports.
    pub const ALL: [SubjectSource; 3] = [
        SubjectSource::StaticToken,
        SubjectSource::Oidc,
        SubjectSource::Mtls,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            SubjectSource::StaticToken => "static_token",
            SubjectSource::Oidc => "oidc",
            SubjectSource::Mtls => "mtls",
        }
    }

    fn index(self) -> usize {
        match self {
            SubjectSource::StaticToken => 0,
            SubjectSource::Oidc => 1,
            SubjectSource::Mtls => 2,
        }
    }
}

/// The longest subject, in bytes: the OIDC bound on `sub` (ADR-3541 decision
/// 1).
pub const SUBJECT_MAX_BYTES: usize = 255;

/// A value [`Subject::new`] refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SubjectError {
    #[error("the subject is empty")]
    Empty,
    #[error("the subject is {len} bytes, longer than the 255-byte bound")]
    TooLong { len: usize },
    #[error("the subject holds byte {byte:#04x}, outside printable non-space ASCII")]
    Charset { byte: u8 },
}

impl SubjectError {
    /// The drop reason a source records when it leaves a subject absent for
    /// this error.
    pub fn drop_reason(self) -> SubjectDropReason {
        match self {
            SubjectError::Empty => SubjectDropReason::Empty,
            SubjectError::TooLong { .. } => SubjectDropReason::TooLong,
            SubjectError::Charset { .. } => SubjectDropReason::Charset,
        }
    }
}

/// Who, within a tenant, a credential belongs to (ADR-3541 decision 1): 1 to
/// [`SUBJECT_MAX_BYTES`] bytes, every byte in `0x21..=0x7E`, together with
/// its [`SubjectSource`]. [`Subject::new`] is the only constructor, so a
/// `Subject` always holds a value inside that bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subject {
    value: String,
    source: SubjectSource,
}

impl Subject {
    /// Refuses a value that is empty, longer than [`SUBJECT_MAX_BYTES`] bytes,
    /// or holds a byte outside printable non-space ASCII
    /// (`a_subject_outside_the_length_or_charset_bound_is_refused`). Length is
    /// counted in bytes and checked before the charset.
    pub fn new(source: SubjectSource, value: impl Into<String>) -> Result<Subject, SubjectError> {
        let value = value.into();
        if value.is_empty() {
            return Err(SubjectError::Empty);
        }
        if value.len() > SUBJECT_MAX_BYTES {
            return Err(SubjectError::TooLong { len: value.len() });
        }
        if let Some(&byte) = value
            .as_bytes()
            .iter()
            .find(|b| !(0x21..=0x7E).contains(*b))
        {
            return Err(SubjectError::Charset { byte });
        }
        Ok(Subject { value, source })
    }

    pub fn value(&self) -> &str {
        &self.value
    }

    pub fn source(&self) -> SubjectSource {
        self.source
    }
}

/// Why a source left a subject absent (ADR-3541 decision 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubjectDropReason {
    /// The configured claim is not in the verified claim set.
    Missing,
    /// The claim is present but not a JSON string.
    NotString,
    /// [`SubjectError::Empty`].
    Empty,
    /// [`SubjectError::TooLong`].
    TooLong,
    /// [`SubjectError::Charset`].
    Charset,
}

impl SubjectDropReason {
    /// Every reason, in the order [`SubjectDropCounters::snapshot`] reports.
    pub const ALL: [SubjectDropReason; 5] = [
        SubjectDropReason::Missing,
        SubjectDropReason::NotString,
        SubjectDropReason::Empty,
        SubjectDropReason::TooLong,
        SubjectDropReason::Charset,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            SubjectDropReason::Missing => "missing",
            SubjectDropReason::NotString => "not_string",
            SubjectDropReason::Empty => "empty",
            SubjectDropReason::TooLong => "too_long",
            SubjectDropReason::Charset => "charset",
        }
    }

    fn index(self) -> usize {
        match self {
            SubjectDropReason::Missing => 0,
            SubjectDropReason::NotString => 1,
            SubjectDropReason::Empty => 2,
            SubjectDropReason::TooLong => 3,
            SubjectDropReason::Charset => 4,
        }
    }
}

/// Counts of subjects a source dropped to absent, keyed by source and reason,
/// behind `ravel_auth_subject_dropped_total{source,reason}` (ADR-3541
/// decision 3).
#[derive(Debug, Default)]
pub struct SubjectDropCounters {
    counts: [[AtomicU64; SubjectDropReason::ALL.len()]; SubjectSource::ALL.len()],
}

impl SubjectDropCounters {
    pub fn record(&self, source: SubjectSource, reason: SubjectDropReason) {
        self.counts[source.index()][reason.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Every (source, reason) pair with its count, zeros included, sources in
    /// [`SubjectSource::ALL`] order and reasons in [`SubjectDropReason::ALL`]
    /// order within each source.
    pub fn snapshot(&self) -> Vec<(SubjectSource, SubjectDropReason, u64)> {
        SubjectSource::ALL
            .iter()
            .flat_map(|&source| {
                SubjectDropReason::ALL.iter().map(move |&reason| {
                    (
                        source,
                        reason,
                        self.counts[source.index()][reason.index()].load(Ordering::Relaxed),
                    )
                })
            })
            .collect()
    }
}

impl Principal {
    /// A principal for `tenant` holding no capability and no subject.
    pub fn tenant_only(tenant: TenantId) -> Self {
        Principal {
            tenant,
            ddl: false,
            ipcrypt: false,
            agents: false,
            agents_worker: false,
            agents_admin: false,
            subject: None,
        }
    }
}

/// Resolves the tenant for an incoming request from its headers.
pub trait TenantResolver: Send + Sync {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError>;

    /// Resolves the full [`Principal`], including its capabilities. The
    /// default forwards to [`resolve`](Self::resolve) and reports no
    /// capability, so a resolver that does not override this keeps its
    /// current behavior: every capability is opt-in per resolver, never
    /// inferred.
    fn resolve_principal(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        Ok(Principal::tenant_only(self.resolve(headers)?))
    }
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

    fn resolve_principal(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        for resolver in &self.resolvers {
            if let Ok(principal) = resolver.resolve_principal(headers) {
                return Ok(principal);
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
    tokens: HashMap<[u8; 32], Principal>,
}

impl StaticBearerTokenResolver {
    pub fn new(tokens: HashMap<String, TenantId>) -> Self {
        Self::with_principals(
            tokens
                .into_iter()
                .map(|(token, tenant)| (token, Principal::tenant_only(tenant)))
                .collect(),
        )
    }

    /// Same as [`new`](Self::new), but each token carries its own [`Principal`]
    /// (and therefore its own capabilities) instead of defaulting every
    /// token to no capability.
    pub fn with_principals(tokens: HashMap<String, Principal>) -> Self {
        let hash_key = random_hash_key();
        let tokens = tokens
            .into_iter()
            .map(|(token, principal)| (token_hash(&hash_key, token.as_bytes()), principal))
            .collect();
        StaticBearerTokenResolver { hash_key, tokens }
    }
}

impl TenantResolver for StaticBearerTokenResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        self.resolve_principal(headers).map(|p| p.tenant)
    }

    fn resolve_principal(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
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
    ddl_claim: Option<String>,
    ipcrypt_claim: Option<String>,
    agents_claim: Option<String>,
    agents_worker_claim: Option<String>,
    agents_admin_claim: Option<String>,
    subject_claim: Option<(String, Arc<SubjectDropCounters>)>,
}

impl OidcResolver {
    /// Build a resolver reading validated JWTs against `cache`.
    ///
    /// `issuer` is the exact `iss` every token must carry. `audiences`, when
    /// non-empty, is the set of acceptable `aud` values (any match passes);
    /// empty disables audience checking. `tenant_claim` is the string claim the
    /// tenant identity is read from (e.g. `tenant`). No capability claim is
    /// configured, so [`resolve_principal`](Self::resolve_principal) reports
    /// no `ddl` capability until [`with_ddl_claim`](Self::with_ddl_claim) is
    /// used, no `ipcrypt` capability until
    /// [`with_ipcrypt_claim`](Self::with_ipcrypt_claim) is, and none of the
    /// three agents capabilities until
    /// [`with_agents_claim`](Self::with_agents_claim),
    /// [`with_agents_worker_claim`](Self::with_agents_worker_claim) or
    /// [`with_agents_admin_claim`](Self::with_agents_admin_claim) is.
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
            ddl_claim: None,
            ipcrypt_claim: None,
            agents_claim: None,
            agents_worker_claim: None,
            agents_admin_claim: None,
            subject_claim: None,
        }
    }

    /// Configure the claim that grants the `ddl` capability (ADR-2040 decision
    /// 4). The capability is present only when the verified token carries this
    /// claim as the JSON boolean `true`; a string `"true"`, a number, an array,
    /// or a missing claim all mean no capability. With no claim name configured
    /// (the default), the capability is never granted via OIDC.
    pub fn with_ddl_claim(mut self, ddl_claim: impl Into<String>) -> Self {
        self.ddl_claim = Some(ddl_claim.into());
        self
    }

    /// Configure the claim that grants the `ipcrypt` capability (ADR-2993
    /// decision 6), read exactly as [`with_ddl_claim`](Self::with_ddl_claim)
    /// reads its claim: only the JSON boolean `true` grants it, and with no
    /// claim name configured (the default) it is never granted via OIDC.
    pub fn with_ipcrypt_claim(mut self, ipcrypt_claim: impl Into<String>) -> Self {
        self.ipcrypt_claim = Some(ipcrypt_claim.into());
        self
    }

    /// Configure the claim that grants the `agents` capability (ADR-3331
    /// decision 9), read exactly as [`with_ddl_claim`](Self::with_ddl_claim)
    /// reads its claim.
    pub fn with_agents_claim(mut self, agents_claim: impl Into<String>) -> Self {
        self.agents_claim = Some(agents_claim.into());
        self
    }

    /// Configure the claim that grants the `agents_worker` capability
    /// (ADR-3331 decision 9), read exactly as
    /// [`with_ddl_claim`](Self::with_ddl_claim) reads its claim.
    pub fn with_agents_worker_claim(mut self, agents_worker_claim: impl Into<String>) -> Self {
        self.agents_worker_claim = Some(agents_worker_claim.into());
        self
    }

    /// Configure the claim that grants the `agents_admin` capability
    /// (ADR-3331 decision 9), read exactly as
    /// [`with_ddl_claim`](Self::with_ddl_claim) reads its claim.
    pub fn with_agents_admin_claim(mut self, agents_admin_claim: impl Into<String>) -> Self {
        self.agents_admin_claim = Some(agents_admin_claim.into());
        self
    }

    /// Configure the claim whose value becomes the principal's subject, with
    /// [`SubjectSource::Oidc`] (ADR-3541 decision 3). The claim is read from
    /// the verified claim set only, as the tenant claim is. A claim that is
    /// missing, not a JSON string, or outside the [`Subject`] bound leaves the
    /// subject `None`, records the reason in `drops`, and still resolves the
    /// principal
    /// (`the_oidc_subject_claim_sets_the_subject_and_a_missing_or_non_string_claim_leaves_it_absent`).
    /// With no claim configured (the default) no subject is read.
    pub fn with_subject_claim(
        mut self,
        claim: impl Into<String>,
        drops: Arc<SubjectDropCounters>,
    ) -> Self {
        self.subject_claim = Some((claim.into(), drops));
        self
    }

    /// The shared JWKS cache, so the caller can drive its background refresh.
    pub fn cache(&self) -> &Arc<OidcJwksCache> {
        &self.cache
    }

    /// Verify `headers` against the cached JWKS and return the claims of the
    /// first token that validates. Shared by [`resolve`](TenantResolver::resolve)
    /// and [`resolve_principal`](TenantResolver::resolve_principal) so both read
    /// the same proven claim set.
    fn verified_claims(&self, headers: &HeaderMap) -> Result<serde_json::Value, AuthError> {
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
            return Ok(data.claims);
        }
        Err(AuthError)
    }
}

impl TenantResolver for OidcResolver {
    fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        let claims = self.verified_claims(headers)?;
        // Signature, issuer, expiry (and audience) are now proven for this
        // token. The tenant claim is authoritative from here: a missing or
        // non-string value is a hard failure, never a fallback to another
        // key or claim.
        match claims.get(&self.tenant_claim).and_then(|v| v.as_str()) {
            Some(tenant) if !tenant.is_empty() => Ok(TenantId::new(tenant.to_string())),
            _ => Err(AuthError),
        }
    }

    fn resolve_principal(&self, headers: &HeaderMap) -> Result<Principal, AuthError> {
        let claims = self.verified_claims(headers)?;
        let tenant = match claims.get(&self.tenant_claim).and_then(|v| v.as_str()) {
            Some(tenant) if !tenant.is_empty() => TenantId::new(tenant.to_string()),
            _ => return Err(AuthError),
        };
        let granted = |claim: &Option<String>| {
            claim
                .as_ref()
                .and_then(|claim| claims.get(claim))
                .and_then(|v| v.as_bool())
                .unwrap_or(false)
        };
        Ok(Principal {
            tenant,
            ddl: granted(&self.ddl_claim),
            ipcrypt: granted(&self.ipcrypt_claim),
            agents: granted(&self.agents_claim),
            agents_worker: granted(&self.agents_worker_claim),
            agents_admin: granted(&self.agents_admin_claim),
            subject: self.subject_claim.as_ref().and_then(|(claim, drops)| {
                oidc_subject(claims.get(claim))
                    .inspect_err(|&reason| drops.record(SubjectSource::Oidc, reason))
                    .ok()
            }),
        })
    }
}

/// The subject a verified claim value names, or why it names none.
fn oidc_subject(value: Option<&serde_json::Value>) -> Result<Subject, SubjectDropReason> {
    let value = value.ok_or(SubjectDropReason::Missing)?;
    let value = value.as_str().ok_or(SubjectDropReason::NotString)?;
    Subject::new(SubjectSource::Oidc, value).map_err(SubjectError::drop_reason)
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
/// convention.
///
/// What `ravel_server::config::Cli::validate` enforces about that listener, and
/// nothing beyond it:
///
/// - `--mtls-enabled` and `--mtls-listener` must be set together; either alone
///   fails startup.
/// - `--mtls-listener` must not equal `--listen-http` or `--listen-grpc`, and
///   must not equal `--fragment-listener`.
/// - `--mtls-listener` must bind a loopback address unless
///   `--mtls-trust-forwarded-header` is also passed (issue #1703). Loopback is
///   the one bind where the topology itself proves only a local proxy can
///   supply the header; on any other address the flag is the operator's
///   assertion that a verifying proxy fronts it.
///
/// Those are address and flag-pairing checks. Ravel cannot verify that a
/// verifying proxy actually exists, that it strips the client's own copy of the
/// header, or that no route reaches this listener directly. A configuration
/// that passes every check above and is deployed without such a proxy still
/// hands tenant selection to the client.
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

    const NONE: TokenCapabilities = TokenCapabilities {
        ddl: false,
        ipcrypt: false,
        agents: false,
        agents_worker: false,
        agents_admin: false,
    };
    const DDL: TokenCapabilities = TokenCapabilities { ddl: true, ..NONE };
    const IPCRYPT: TokenCapabilities = TokenCapabilities {
        ipcrypt: true,
        ..NONE
    };
    const BOTH: TokenCapabilities = TokenCapabilities {
        ddl: true,
        ipcrypt: true,
        ..NONE
    };
    const AGENTS: TokenCapabilities = TokenCapabilities {
        agents: true,
        ..NONE
    };
    const AGENTS_WORKER: TokenCapabilities = TokenCapabilities {
        agents_worker: true,
        ..NONE
    };
    const AGENTS_ADMIN: TokenCapabilities = TokenCapabilities {
        agents_admin: true,
        ..NONE
    };
    const ALL: TokenCapabilities = TokenCapabilities {
        ddl: true,
        ipcrypt: true,
        agents: true,
        agents_worker: true,
        agents_admin: true,
    };

    /// Each agents word grants exactly its own capability and nothing else,
    /// so `agents_worker` does not imply `agents` (ADR-3331 decision 9).
    #[test]
    fn split_tenant_suffix_agents_words_grant_only_their_own_capability() {
        assert_eq!(
            split_tenant_suffix("acme;agents").unwrap(),
            ("acme", AGENTS, None)
        );
        assert_eq!(
            split_tenant_suffix("acme;agents_worker").unwrap(),
            ("acme", AGENTS_WORKER, None)
        );
        assert_eq!(
            split_tenant_suffix("acme;agents_admin").unwrap(),
            ("acme", AGENTS_ADMIN, None)
        );
    }

    #[test]
    fn split_tenant_suffix_every_word_combines_in_any_order() {
        assert_eq!(
            split_tenant_suffix("acme;ddl,ipcrypt,agents,agents_worker,agents_admin").unwrap(),
            ("acme", ALL, None)
        );
        assert_eq!(
            split_tenant_suffix("acme;agents_admin,agents_worker,agents,ipcrypt,ddl").unwrap(),
            ("acme", ALL, None)
        );
    }

    #[test]
    fn split_tenant_suffix_agents_word_repeated_or_misspelled_is_refused() {
        assert_eq!(
            split_tenant_suffix("acme;agents_worker,agents_worker"),
            Err(TenantSuffixError::DuplicateCapability)
        );
        for raw in [
            "acme;AGENTS",
            "acme;agent",
            "acme;agents-worker",
            "acme;agents_",
        ] {
            assert_eq!(
                split_tenant_suffix(raw),
                Err(TenantSuffixError::UnknownCapability),
                "{raw}"
            );
        }
        assert_eq!(
            split_tenant_suffix("acme;agents;agents_admin"),
            Err(TenantSuffixError::CapabilityInTenant)
        );
    }

    #[test]
    fn split_tenant_suffix_no_semicolon_is_unchanged_no_capability() {
        assert_eq!(split_tenant_suffix("acme").unwrap(), ("acme", NONE, None));
    }

    #[test]
    fn split_tenant_suffix_ddl_grants_capability() {
        assert_eq!(
            split_tenant_suffix("acme;ddl").unwrap(),
            ("acme", DDL, None)
        );
    }

    #[test]
    fn split_tenant_suffix_ipcrypt_grants_capability() {
        assert_eq!(
            split_tenant_suffix("acme;ipcrypt").unwrap(),
            ("acme", IPCRYPT, None)
        );
    }

    #[test]
    fn split_tenant_suffix_capability_list_grants_both_in_either_order() {
        assert_eq!(
            split_tenant_suffix("acme;ddl,ipcrypt").unwrap(),
            ("acme", BOTH, None)
        );
        assert_eq!(
            split_tenant_suffix("acme;ipcrypt,ddl").unwrap(),
            ("acme", BOTH, None)
        );
    }

    #[test]
    fn split_tenant_suffix_splits_on_the_last_semicolon() {
        assert_eq!(
            split_tenant_suffix("ac;me;ipcrypt").unwrap(),
            ("ac;me", IPCRYPT, None)
        );
    }

    #[test]
    fn split_tenant_suffix_capability_in_the_tenant_is_refused() {
        for raw in [
            "acme;ddl;ipcrypt",
            "acme;ipcrypt;ddl",
            "acme;ddl;ddl",
            "acme;ddl,ipcrypt;ddl",
            "ac;me;ipcrypt,x;ddl",
        ] {
            assert_eq!(
                split_tenant_suffix(raw),
                Err(TenantSuffixError::CapabilityInTenant),
                "{raw}"
            );
        }
    }

    #[test]
    fn split_tenant_suffix_wrong_case_is_refused() {
        assert_eq!(
            split_tenant_suffix("acme;DDL"),
            Err(TenantSuffixError::UnknownCapability)
        );
        assert_eq!(
            split_tenant_suffix("acme;IPCRYPT"),
            Err(TenantSuffixError::UnknownCapability)
        );
    }

    #[test]
    fn split_tenant_suffix_different_word_is_refused() {
        assert_eq!(
            split_tenant_suffix("acme;admin"),
            Err(TenantSuffixError::UnknownCapability)
        );
        assert_eq!(
            split_tenant_suffix("acme;ddl,admin"),
            Err(TenantSuffixError::UnknownCapability)
        );
        // Whitespace is not trimmed: ` ipcrypt` is not a capability.
        assert_eq!(
            split_tenant_suffix("acme;ddl, ipcrypt"),
            Err(TenantSuffixError::UnknownCapability)
        );
    }

    #[test]
    fn split_tenant_suffix_duplicate_capability_is_refused() {
        assert_eq!(
            split_tenant_suffix("acme;ddl,ddl"),
            Err(TenantSuffixError::DuplicateCapability)
        );
        assert_eq!(
            split_tenant_suffix("acme;ipcrypt,ddl,ipcrypt"),
            Err(TenantSuffixError::DuplicateCapability)
        );
    }

    #[test]
    fn split_tenant_suffix_trailing_semicolon_is_refused() {
        assert_eq!(
            split_tenant_suffix("acme;"),
            Err(TenantSuffixError::EmptyCapability)
        );
    }

    #[test]
    fn split_tenant_suffix_empty_list_entry_is_refused() {
        for raw in ["acme;ddl,", "acme;,ipcrypt", "acme;ddl,,ipcrypt", "acme;,"] {
            assert_eq!(
                split_tenant_suffix(raw),
                Err(TenantSuffixError::EmptyCapability),
                "{raw}"
            );
        }
    }

    #[test]
    fn split_tenant_suffix_empty_tenant_is_refused() {
        for raw in [";ddl", ";ipcrypt", ";ddl,ipcrypt"] {
            assert_eq!(
                split_tenant_suffix(raw),
                Err(TenantSuffixError::EmptyTenant),
                "{raw}"
            );
        }
    }

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
        assert_eq!(
            resolver.tokens.get(&expected),
            Some(&Principal {
                tenant: TenantId::new("acme"),
                ddl: false,
                ipcrypt: false,
                agents: false,
                agents_worker: false,
                agents_admin: false,
                subject: None,
            })
        );

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

    #[test]
    fn ddl_capability_is_absent_by_default_and_granted_by_token_suffix_or_claim() {
        // ADR-2040 decision 4 ("Who may run DDL"): `ddl` is absent unless a
        // resolver's configuration explicitly grants it.

        // Static token with no configured capability: no ddl.
        let plain = StaticBearerTokenResolver::new(HashMap::from([(
            "tok".to_string(),
            TenantId::new("acme"),
        )]));
        assert_eq!(
            plain
                .resolve_principal(&static_bearer("tok"))
                .expect("resolves"),
            Principal {
                tenant: TenantId::new("acme"),
                ddl: false,
                ipcrypt: false,
                agents: false,
                agents_worker: false,
                agents_admin: false,
                subject: None,
            }
        );

        // Static token configured (the `;ddl` suffix's parsed result, at the
        // config layer) with the capability: has it.
        let granted = StaticBearerTokenResolver::with_principals(HashMap::from([(
            "tok".to_string(),
            Principal {
                tenant: TenantId::new("acme"),
                ddl: true,
                ipcrypt: false,
                agents: false,
                agents_worker: false,
                agents_admin: false,
                subject: None,
            },
        )]));
        assert_eq!(
            granted
                .resolve_principal(&static_bearer("tok"))
                .expect("resolves"),
            Principal {
                tenant: TenantId::new("acme"),
                ddl: true,
                ipcrypt: false,
                agents: false,
                agents_worker: false,
                agents_admin: false,
                subject: None,
            }
        );

        // OIDC: the configured claim as the JSON boolean `true` grants it.
        let cache = cache_with(&jwks_with(KID));
        let mut c = claims(Some("acme"), 3600);
        c["can_ddl"] = serde_json::json!(true);
        let token = sign_es256(&c, KID, EC_PRIV_PEM);
        let oidc = resolver(cache).with_ddl_claim("can_ddl");
        assert_eq!(
            oidc.resolve_principal(&bearer(&token)).expect("resolves"),
            Principal {
                tenant: TenantId::new("acme"),
                ddl: true,
                ipcrypt: false,
                agents: false,
                agents_worker: false,
                agents_admin: false,
                subject: None,
            }
        );

        // OIDC: a string "true" is not the JSON boolean true. Rules out wrong
        // implementation (a): "any truthy claim" grants the capability.
        let cache = cache_with(&jwks_with(KID));
        let mut c = claims(Some("acme"), 3600);
        c["can_ddl"] = serde_json::json!("true");
        let token = sign_es256(&c, KID, EC_PRIV_PEM);
        let oidc = resolver(cache).with_ddl_claim("can_ddl");
        assert!(
            !oidc
                .resolve_principal(&bearer(&token))
                .expect("resolves")
                .ddl
        );

        // OIDC: a number is not the JSON boolean true either.
        let cache = cache_with(&jwks_with(KID));
        let mut c = claims(Some("acme"), 3600);
        c["can_ddl"] = serde_json::json!(1);
        let token = sign_es256(&c, KID, EC_PRIV_PEM);
        let oidc = resolver(cache).with_ddl_claim("can_ddl");
        assert!(
            !oidc
                .resolve_principal(&bearer(&token))
                .expect("resolves")
                .ddl
        );

        // OIDC: a missing claim is no capability.
        let cache = cache_with(&jwks_with(KID));
        let token = sign_es256(&claims(Some("acme"), 3600), KID, EC_PRIV_PEM);
        let oidc = resolver(cache).with_ddl_claim("can_ddl");
        assert!(
            !oidc
                .resolve_principal(&bearer(&token))
                .expect("resolves")
                .ddl
        );

        // OIDC: no claim name configured at all means never, even if the token
        // happens to carry a same-named claim set to `true`.
        let cache = cache_with(&jwks_with(KID));
        let mut c = claims(Some("acme"), 3600);
        c["can_ddl"] = serde_json::json!(true);
        let token = sign_es256(&c, KID, EC_PRIV_PEM);
        let oidc = resolver(cache); // no with_ddl_claim
        assert!(
            !oidc
                .resolve_principal(&bearer(&token))
                .expect("resolves")
                .ddl
        );

        // DevHeaderTenantResolver and MtlsResolver take the default method:
        // never ddl.
        let dev = DevHeaderTenantResolver::default();
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-ravel-tenant",
            "acme".parse().expect("valid header value"),
        );
        assert!(!dev.resolve_principal(&headers).expect("resolves").ddl);

        let mtls = MtlsResolver::default();
        let mut headers = HeaderMap::new();
        headers.insert(MtlsResolver::DEFAULT_HEADER, "acme".parse().unwrap());
        assert!(!mtls.resolve_principal(&headers).expect("resolves").ddl);

        // FallbackResolver returns the same resolver `resolve` would pick,
        // including its capability. Rules out wrong implementation (b):
        // FallbackResolver always reporting no capability regardless of which
        // resolver in the chain actually matched.
        let fallback = FallbackResolver::new(vec![
            Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            Arc::new(granted),
        ]);
        assert_eq!(
            fallback
                .resolve_principal(&static_bearer("tok"))
                .expect("resolves"),
            Principal {
                tenant: TenantId::new("acme"),
                ddl: true,
                ipcrypt: false,
                agents: false,
                agents_worker: false,
                agents_admin: false,
                subject: None,
            }
        );
    }

    /// When two resolvers in the chain both match one request with different
    /// `ddl`, the first one's principal wins outright. A wrong implementation
    /// that ORs `ddl` across the chain reports `true` here.
    #[test]
    fn fallback_first_match_decides_ddl_without_or_across_chain() {
        struct Fixed(Principal);
        impl TenantResolver for Fixed {
            fn resolve(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
                Ok(self.0.tenant.clone())
            }
            fn resolve_principal(&self, _headers: &HeaderMap) -> Result<Principal, AuthError> {
                Ok(self.0.clone())
            }
        }
        let denied = Principal {
            tenant: TenantId::new("acme"),
            ddl: false,
            ipcrypt: false,
            agents: false,
            agents_worker: false,
            agents_admin: false,
            subject: None,
        };
        let granted = Principal {
            tenant: TenantId::new("acme"),
            ddl: true,
            ipcrypt: true,
            agents: false,
            agents_worker: false,
            agents_admin: false,
            subject: None,
        };
        let first_denies = FallbackResolver::new(vec![
            Arc::new(Fixed(denied.clone())),
            Arc::new(Fixed(granted.clone())),
        ]);
        assert_eq!(
            first_denies
                .resolve_principal(&HeaderMap::new())
                .expect("resolves"),
            denied
        );
        let first_grants = FallbackResolver::new(vec![
            Arc::new(Fixed(granted.clone())),
            Arc::new(Fixed(denied)),
        ]);
        assert_eq!(
            first_grants
                .resolve_principal(&HeaderMap::new())
                .expect("resolves"),
            granted
        );
    }

    /// A `[true]` array under the ddl claim is not the JSON boolean `true`, so
    /// it grants nothing (the docs and `--oidc-ddl-claim` help promise this).
    #[test]
    fn oidc_ddl_claim_array_is_no_capability() {
        let cache = cache_with(&jwks_with(KID));
        let mut c = claims(Some("acme"), 3600);
        c["can_ddl"] = serde_json::json!([true]);
        let token = sign_es256(&c, KID, EC_PRIV_PEM);
        let oidc = resolver(cache).with_ddl_claim("can_ddl");
        assert!(
            !oidc
                .resolve_principal(&bearer(&token))
                .expect("resolves")
                .ddl
        );
    }

    /// ADR-2993 decision 6: the claim `--oidc-ipcrypt-claim` names grants
    /// `ipcrypt` only as the JSON boolean `true`, independently of `ddl`.
    #[test]
    fn oidc_ipcrypt_claim_grants_ipcrypt_and_its_absence_does_not() {
        let principal_for = |extra: &[(&str, serde_json::Value)], configured: bool| {
            let cache = cache_with(&jwks_with(KID));
            let mut c = claims(Some("acme"), 3600);
            for (name, value) in extra {
                c[*name] = value.clone();
            }
            let token = sign_es256(&c, KID, EC_PRIV_PEM);
            let oidc = resolver(cache).with_ddl_claim("can_ddl");
            let oidc = if configured {
                oidc.with_ipcrypt_claim("can_ipcrypt")
            } else {
                oidc
            };
            oidc.resolve_principal(&bearer(&token)).expect("resolves")
        };

        assert_eq!(
            principal_for(&[("can_ipcrypt", serde_json::json!(true))], true),
            Principal {
                tenant: TenantId::new("acme"),
                ddl: false,
                ipcrypt: true,
                agents: false,
                agents_worker: false,
                agents_admin: false,
                subject: None,
            }
        );
        assert_eq!(
            principal_for(
                &[
                    ("can_ipcrypt", serde_json::json!(true)),
                    ("can_ddl", serde_json::json!(true)),
                ],
                true
            ),
            Principal {
                tenant: TenantId::new("acme"),
                ddl: true,
                ipcrypt: true,
                agents: false,
                agents_worker: false,
                agents_admin: false,
                subject: None,
            }
        );
        // The ddl claim does not grant ipcrypt.
        let ddl_only = principal_for(&[("can_ddl", serde_json::json!(true))], true);
        assert!(ddl_only.ddl);
        assert!(!ddl_only.ipcrypt);
        // A missing claim, a non-boolean claim, or no configured claim name:
        // no capability.
        assert!(!principal_for(&[], true).ipcrypt);
        assert!(!principal_for(&[("can_ipcrypt", serde_json::json!("true"))], true).ipcrypt);
        assert!(!principal_for(&[("can_ipcrypt", serde_json::json!(1))], true).ipcrypt);
        assert!(!principal_for(&[("can_ipcrypt", serde_json::json!([true]))], true).ipcrypt);
        assert!(!principal_for(&[("can_ipcrypt", serde_json::json!(true))], false).ipcrypt);
    }

    /// Each agents claim grants exactly its own capability, read like the ddl
    /// claim: only the JSON boolean `true` on a configured claim name.
    #[test]
    fn oidc_agents_claims_grant_only_their_own_capability() {
        let principal_for = |extra: &[(&str, serde_json::Value)], configured: bool| {
            let cache = cache_with(&jwks_with(KID));
            let mut c = claims(Some("acme"), 3600);
            for (name, value) in extra {
                c[*name] = value.clone();
            }
            let token = sign_es256(&c, KID, EC_PRIV_PEM);
            let oidc = resolver(cache);
            let oidc = if configured {
                oidc.with_agents_claim("can_agents")
                    .with_agents_worker_claim("can_agents_worker")
                    .with_agents_admin_claim("can_agents_admin")
            } else {
                oidc
            };
            oidc.resolve_principal(&bearer(&token)).expect("resolves")
        };

        assert_eq!(
            principal_for(&[("can_agents_worker", serde_json::json!(true))], true),
            Principal {
                tenant: TenantId::new("acme"),
                ddl: false,
                ipcrypt: false,
                agents: false,
                agents_worker: true,
                agents_admin: false,
                subject: None,
            }
        );
        assert_eq!(
            principal_for(&[("can_agents", serde_json::json!(true))], true),
            Principal {
                tenant: TenantId::new("acme"),
                ddl: false,
                ipcrypt: false,
                agents: true,
                agents_worker: false,
                agents_admin: false,
                subject: None,
            }
        );
        assert_eq!(
            principal_for(&[("can_agents_admin", serde_json::json!(true))], true),
            Principal {
                tenant: TenantId::new("acme"),
                ddl: false,
                ipcrypt: false,
                agents: false,
                agents_worker: false,
                agents_admin: true,
                subject: None,
            }
        );
        // A missing claim, a non-boolean claim, or no configured claim name:
        // no capability.
        let none = Principal::tenant_only(TenantId::new("acme"));
        assert_eq!(principal_for(&[], true), none);
        for value in [
            serde_json::json!("true"),
            serde_json::json!(1),
            serde_json::json!([true]),
        ] {
            assert_eq!(
                principal_for(
                    &[
                        ("can_agents", value.clone()),
                        ("can_agents_worker", value.clone()),
                        ("can_agents_admin", value),
                    ],
                    true
                ),
                none
            );
        }
        assert_eq!(
            principal_for(
                &[
                    ("can_agents", serde_json::json!(true)),
                    ("can_agents_worker", serde_json::json!(true)),
                    ("can_agents_admin", serde_json::json!(true)),
                ],
                false
            ),
            none
        );
    }

    /// ADR-3541 decision 3: one `subject=LABEL` entry rides beside or instead
    /// of the capability words, and every malformed entry refuses with an
    /// error that names neither the label nor the tenant.
    #[test]
    fn a_subject_entry_in_the_token_suffix_sets_the_subject_and_a_bad_label_refuses() {
        let (tenant, caps, subject) =
            split_tenant_suffix("acme;agents_admin,subject=alice").expect("parses");
        assert_eq!((tenant, caps), ("acme", AGENTS_ADMIN));
        let subject = subject.expect("the entry sets a subject");
        assert_eq!(subject.value(), "alice");
        assert_eq!(subject.source(), SubjectSource::StaticToken);

        let (tenant, caps, subject) =
            split_tenant_suffix("acme;subject=bob@example.com").expect("parses");
        assert_eq!((tenant, caps), ("acme", NONE));
        let subject = subject.expect("the entry sets a subject");
        assert_eq!(subject.value(), "bob@example.com");
        assert_eq!(subject.source(), SubjectSource::StaticToken);

        let long_label = "k".repeat(SUBJECT_LABEL_MAX_BYTES + 1);
        let long_raw = format!("acme;subject={long_label}");
        let cases: [(&str, TenantSuffixError, &[&str]); 8] = [
            (
                "acme;subject=a,subject=b",
                TenantSuffixError::DuplicateSubject,
                &["subject=a", "subject=b"],
            ),
            (
                "acme;subject=alpha1,subject=bravo2",
                TenantSuffixError::DuplicateSubject,
                &["alpha1", "bravo2"],
            ),
            ("acme;subject=", TenantSuffixError::EmptySubjectLabel, &[]),
            (
                "acme;subject=al ice",
                TenantSuffixError::SubjectLabelCharset,
                &["al ice"],
            ),
            (
                "acme;subject=al!ce",
                TenantSuffixError::SubjectLabelCharset,
                &["al!ce"],
            ),
            (
                "acme;subject=al;ce",
                TenantSuffixError::CapabilityInTenant,
                &["al;ce", "subject=al"],
            ),
            ("acme;subject", TenantSuffixError::SubjectWithoutLabel, &[]),
            (&long_raw, TenantSuffixError::SubjectLabelTooLong, &["kkkk"]),
        ];
        for (raw, want, needles) in cases {
            let err = split_tenant_suffix(raw).expect_err(raw);
            assert_eq!(err, want, "{raw}");
            let message = err.to_string();
            for needle in needles.iter().chain(&["acme"]) {
                assert!(
                    !message.contains(needle),
                    "{raw}: the message names {needle:?}: {message}"
                );
            }
        }
        assert_eq!(long_label.len(), 65);
        assert_eq!(
            parse_subject_label(&"k".repeat(SUBJECT_LABEL_MAX_BYTES))
                .expect("64 bytes fit")
                .value()
                .len(),
            64
        );
    }

    /// ADR-3541 decision 3: the configured claim is read from the verified
    /// claim set only; a bad value drops to absent, counts once under its
    /// reason, and the request still authenticates.
    #[test]
    fn the_oidc_subject_claim_sets_the_subject_and_a_missing_or_non_string_claim_leaves_it_absent()
    {
        let only = |reason: SubjectDropReason| -> Vec<(SubjectSource, SubjectDropReason, u64)> {
            SubjectSource::ALL
                .iter()
                .flat_map(|&source| {
                    SubjectDropReason::ALL.iter().map(move |&r| {
                        let count = u64::from(source == SubjectSource::Oidc && r == reason);
                        (source, r, count)
                    })
                })
                .collect()
        };
        let no_drops = || SubjectDropCounters::default().snapshot();

        // A sub the bound accepts becomes the subject.
        let drops = Arc::new(SubjectDropCounters::default());
        let oidc =
            resolver(cache_with(&jwks_with(KID))).with_subject_claim("sub", Arc::clone(&drops));
        let token = sign_es256(&claims(Some("acme"), 3600), KID, EC_PRIV_PEM);
        let principal = oidc.resolve_principal(&bearer(&token)).expect("resolves");
        assert_eq!(principal.tenant, TenantId::new("acme"));
        let subject = principal.subject.expect("sub sets the subject");
        assert_eq!(subject.value(), "user-123");
        assert_eq!(subject.source(), SubjectSource::Oidc);
        assert_eq!(drops.snapshot(), no_drops());

        // The same claims signed under a key the cache does not hold: no
        // principal at all, so nothing is read from the unverified token.
        let forged = sign_es256(&claims(Some("acme"), 3600), KID, OTHER_EC_PRIV_PEM);
        assert!(oidc.resolve_principal(&bearer(&forged)).is_err());
        assert_eq!(drops.snapshot(), no_drops());

        // jsonwebtoken itself rejects a token whose `sub` is an array before
        // any claim is read, so the array case runs under a claim it does not
        // validate.
        let mut c = claims(Some("acme"), 3600);
        c["sub"] = serde_json::json!(["user-123"]);
        let token = sign_es256(&c, KID, EC_PRIV_PEM);
        assert!(oidc.resolve_principal(&bearer(&token)).is_err());
        assert_eq!(drops.snapshot(), no_drops());

        let cases: [(&str, Option<serde_json::Value>, SubjectDropReason); 6] = [
            ("sub", None, SubjectDropReason::Missing),
            (
                "sub",
                Some(serde_json::json!(123)),
                SubjectDropReason::NotString,
            ),
            (
                "email",
                Some(serde_json::json!(["user-123"])),
                SubjectDropReason::NotString,
            ),
            ("sub", Some(serde_json::json!("")), SubjectDropReason::Empty),
            (
                "sub",
                Some(serde_json::json!("u".repeat(SUBJECT_MAX_BYTES + 1))),
                SubjectDropReason::TooLong,
            ),
            (
                "sub",
                Some(serde_json::json!("user 123")),
                SubjectDropReason::Charset,
            ),
        ];
        for (claim, value, reason) in cases {
            let drops = Arc::new(SubjectDropCounters::default());
            let oidc =
                resolver(cache_with(&jwks_with(KID))).with_subject_claim(claim, Arc::clone(&drops));
            let mut c = claims(Some("acme"), 3600);
            match value {
                Some(value) => c[claim] = value,
                None => {
                    c.as_object_mut().expect("an object").remove(claim);
                }
            }
            let token = sign_es256(&c, KID, EC_PRIV_PEM);
            let principal = oidc
                .resolve_principal(&bearer(&token))
                .expect("a dropped subject still authenticates");
            assert_eq!(principal.tenant, TenantId::new("acme"), "{reason:?}");
            assert_eq!(principal.subject, None, "{reason:?}");
            assert_eq!(drops.snapshot(), only(reason), "{reason:?}");
        }

        // No claim configured: a token carrying sub resolves with no subject.
        // There is nothing to count against: a resolver without a subject
        // claim holds no drop counters.
        let token = sign_es256(&claims(Some("acme"), 3600), KID, EC_PRIV_PEM);
        let principal = resolver(cache_with(&jwks_with(KID)))
            .resolve_principal(&bearer(&token))
            .expect("resolves");
        assert_eq!(principal.subject, None);
    }

    /// ADR-3541 decision 1: 1 to 255 bytes, every byte in `0x21..=0x7E`.
    #[test]
    fn a_subject_outside_the_length_or_charset_bound_is_refused() {
        let new = |value: &str| Subject::new(SubjectSource::Oidc, value);
        assert_eq!(new(""), Err(SubjectError::Empty));
        assert_eq!(
            new(&"a".repeat(256)),
            Err(SubjectError::TooLong { len: 256 })
        );
        // 255 characters but 256 bytes: the bound counts bytes.
        let wide = format!("{}é", "a".repeat(253));
        assert_eq!(wide.chars().count(), 254);
        assert_eq!(
            new(&format!("a{wide}")),
            Err(SubjectError::TooLong { len: 256 })
        );
        assert_eq!(new("al ice"), Err(SubjectError::Charset { byte: 0x20 }));
        assert_eq!(new("al\tice"), Err(SubjectError::Charset { byte: 0x09 }));
        assert_eq!(
            new("al\u{7f}ice"),
            Err(SubjectError::Charset { byte: 0x7F })
        );
        assert_eq!(new("alé"), Err(SubjectError::Charset { byte: 0xC3 }));

        let max = "a".repeat(SUBJECT_MAX_BYTES);
        assert_eq!(new(&max).expect("255 bytes fit").value(), max);
        let printable: String = (0x21u8..=0x7E).map(char::from).collect();
        let subject = new(&printable).expect("every printable byte fits");
        assert_eq!(subject.value(), printable);
        assert_eq!(subject.source(), SubjectSource::Oidc);
    }

    /// ADR-3541 decision 1: a subject is attribution only. One OIDC token,
    /// resolved by a resolver that reads its subject claim and by one that
    /// does not, yields the same five capabilities for every combination of
    /// capability claims; only the first principal has a subject.
    /// FallbackResolver forwards the matching resolver's subject unchanged.
    #[test]
    fn a_subject_grants_nothing() {
        let configured_resolver = || {
            resolver(cache_with(&jwks_with(KID)))
                .with_ddl_claim("can_ddl")
                .with_ipcrypt_claim("can_ipcrypt")
                .with_agents_claim("can_agents")
                .with_agents_worker_claim("can_agents_worker")
                .with_agents_admin_claim("can_agents_admin")
        };
        let drops = Arc::new(SubjectDropCounters::default());
        let with_subject = configured_resolver().with_subject_claim("sub", Arc::clone(&drops));
        let without_subject = configured_resolver();
        let caps = |p: &Principal| (p.ddl, p.ipcrypt, p.agents, p.agents_worker, p.agents_admin);

        for mask in 0u8..32 {
            let want = (
                mask & 1 != 0,
                mask & 2 != 0,
                mask & 4 != 0,
                mask & 8 != 0,
                mask & 16 != 0,
            );
            let mut c = claims(Some("acme"), 3600);
            c["can_ddl"] = serde_json::json!(want.0);
            c["can_ipcrypt"] = serde_json::json!(want.1);
            c["can_agents"] = serde_json::json!(want.2);
            c["can_agents_worker"] = serde_json::json!(want.3);
            c["can_agents_admin"] = serde_json::json!(want.4);
            let token = sign_es256(&c, KID, EC_PRIV_PEM);

            let with = with_subject
                .resolve_principal(&bearer(&token))
                .expect("resolves with the subject claim");
            let without = without_subject
                .resolve_principal(&bearer(&token))
                .expect("resolves without the subject claim");
            assert_eq!(caps(&with), want, "mask {mask:#07b}");
            assert_eq!(caps(&without), want, "mask {mask:#07b}");
            let subject = with.subject.as_ref().expect("sub sets the subject");
            assert_eq!(subject.value(), "user-123");
            assert_eq!(subject.source(), SubjectSource::Oidc);
            assert_eq!(without.subject, None, "mask {mask:#07b}");
            assert_eq!(with.tenant, without.tenant, "mask {mask:#07b}");
        }
        assert_eq!(drops.snapshot(), SubjectDropCounters::default().snapshot());

        let subject = parse_subject_label("alice").expect("a valid label");
        let configured = Principal {
            subject: Some(subject),
            ..Principal::tenant_only(TenantId::new("acme"))
        };
        let fallback = FallbackResolver::new(vec![
            Arc::new(StaticBearerTokenResolver::new(HashMap::new())),
            Arc::new(StaticBearerTokenResolver::with_principals(HashMap::from([
                ("tok".to_string(), configured.clone()),
            ]))),
        ]);
        assert_eq!(
            fallback
                .resolve_principal(&static_bearer("tok"))
                .expect("resolves"),
            configured
        );
    }

    /// ADR-3541 decision 4: no resolver reads a caller-supplied subject.
    #[test]
    fn the_dev_header_resolver_never_sets_a_subject() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ravel-tenant", "acme".parse().expect("valid header"));
        headers.insert("x-ravel-subject", "alice".parse().expect("valid header"));
        let principal = DevHeaderTenantResolver::default()
            .resolve_principal(&headers)
            .expect("resolves");
        assert_eq!(principal, Principal::tenant_only(TenantId::new("acme")));
        assert_eq!(principal.subject, None);
    }
}
