//! Core Ravel domain types: tenants, signals, labels, series identity,
//! samples, time ranges, shard routing, and commit tokens.
//!
//! This crate is dependency-light and defines the identity rules the rest of
//! the system builds on (see ADR-0005, ADR-0009). Canonical encodings here
//! are persistent contracts: changing them requires a new version domain
//! string, never an in-place edit.

pub mod accounting;
pub mod declared_stats;
pub mod exemplar;
pub mod logstream;

pub use exemplar::{Exemplar, ExemplarCap};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// Telemetry signal kinds. Physical key prefixes are part of the object
/// layout contract (docs/catalog-and-mvcc.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Signal {
    Metrics,
    Logs,
    Spans,
    Profiles,
    Alerts,
    Audit,
}

impl Signal {
    /// Single-letter key prefix used in object paths.
    pub fn key_prefix(self) -> &'static str {
        match self {
            Signal::Metrics => "m",
            Signal::Logs => "l",
            Signal::Spans => "s",
            Signal::Profiles => "p",
            Signal::Alerts => "a",
            Signal::Audit => "u",
        }
    }
}

/// Logical tenant identifier as resolved by authentication. Never appears in
/// object keys; use [`TenantHash`] there (ADR-0009).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TenantId(String);

impl TenantId {
    pub fn new(id: impl Into<String>) -> Self {
        TenantId(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Derive this tenant's object-key prefix hash under the process-wide
    /// scheme installed by [`install_tenant_hash_scheme`]. Absent an install,
    /// the scheme is [`TenantHashScheme::V1Unkeyed`], byte-for-byte identical
    /// to the pre-ADR-0050 derivation, so every existing bucket and every
    /// caller that never opts into keying is unaffected.
    ///
    /// The scheme is consulted here rather than threaded through every call
    /// site (ADR-0050 section 3): the bucket pins one derivation at birth, so
    /// a process-global resolved once at startup covers ingest, query,
    /// maintenance, and the CLI without a scheme parameter on each `.hash()`.
    pub fn hash(&self) -> TenantHash {
        active_tenant_hash_scheme().hash(self)
    }
}

/// The tenant-hash derivation scheme (ADR-0050 section 3). A bucket pins one
/// scheme at birth via its `sys/tenancy` marker; a process resolves that
/// marker once at startup and installs the matching scheme with
/// [`install_tenant_hash_scheme`]. One binary carries both derivations
/// forever; no object is ever rewritten.
///
/// [`Debug`] is hand-written to redact the keyed variant's `hash_key`: the
/// derived hashing key is secret material, and a `#[derive(Debug)]` would leak
/// its 32 raw bytes into any log line, panic message, or debug print that
/// touches this type. Only the variant name is shown.
#[derive(Clone)]
pub enum TenantHashScheme {
    /// Unkeyed BLAKE3 over `"ravel-tenant-v1" || tenant_id`, truncated to 16
    /// bytes. The original derivation and the default for any process with no
    /// scheme installed. Every pre-ADR-0050 bucket is pinned to this
    /// permanently; the derivation is a frozen contract and never changes.
    V1Unkeyed,
    /// Keyed BLAKE3: `keyed_hash(hash_key, tenant_id)[0..16]`. `hash_key` is
    /// `blake3::derive_key("ravel-tenant-v2", deployment_key)`; the raw 32-byte
    /// deployment key is never stored here. Enumeration-resistant: without the
    /// key, a bucket's prefixes reveal nothing about which tenants exist.
    V2Keyed { hash_key: [u8; 32] },
}

impl std::fmt::Debug for TenantHashScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TenantHashScheme::V1Unkeyed => f.write_str("V1Unkeyed"),
            // Never print `hash_key`: it is derived secret material.
            TenantHashScheme::V2Keyed { .. } => f
                .debug_struct("V2Keyed")
                .field("hash_key", &"<redacted>")
                .finish(),
        }
    }
}

/// Key-derivation context for the v2 keyed tenant hash (ADR-0050 section 3).
/// Part of the persistent contract: changing it silently relocates every
/// keyed prefix.
const TENANT_V2_HASH_CONTEXT: &str = "ravel-tenant-v2";

/// Key-derivation context for the v2 key fingerprint recorded in
/// `sys/tenancy` (ADR-0050 section 3).
const TENANT_V2_FINGERPRINT_CONTEXT: &str = "ravel-tenant-v2-fingerprint";

/// Width of the deployment-key fingerprint stored in `sys/tenancy`, in bytes.
/// 16 matches [`TenantHash`]'s own width and gives 128 bits of collision
/// resistance for the wrong-key startup check (ADR-0050 section 3).
pub const TENANT_KEY_FINGERPRINT_LEN: usize = 16;

/// The fingerprint of a 32-byte deployment key recorded in `sys/tenancy`
/// (ADR-0050 section 3): `blake3::derive_key("ravel-tenant-v2-fingerprint",
/// deployment_key)` truncated to [`TENANT_KEY_FINGERPRINT_LEN`] bytes. A
/// fingerprint of the key for detecting a wrong key before it does damage,
/// never the key itself. Shared here so the server (which writes and checks
/// it) and the CLI (`tenancy show`) derive it identically.
pub fn tenant_key_fingerprint(deployment_key: &[u8; 32]) -> [u8; TENANT_KEY_FINGERPRINT_LEN] {
    let derived = blake3::derive_key(TENANT_V2_FINGERPRINT_CONTEXT, deployment_key);
    let mut fp = [0u8; TENANT_KEY_FINGERPRINT_LEN];
    fp.copy_from_slice(&derived[..TENANT_KEY_FINGERPRINT_LEN]);
    fp
}

impl TenantHashScheme {
    /// Build the keyed scheme from a 32-byte deployment key, deriving the
    /// domain-separated hashing key so the deployment key itself is not held
    /// in the scheme.
    pub fn v2_from_deployment_key(deployment_key: &[u8; 32]) -> Self {
        TenantHashScheme::V2Keyed {
            hash_key: blake3::derive_key(TENANT_V2_HASH_CONTEXT, deployment_key),
        }
    }

    /// Derive a tenant's 128-bit prefix hash under this scheme.
    pub fn hash(&self, tenant: &TenantId) -> TenantHash {
        let digest = match self {
            TenantHashScheme::V1Unkeyed => {
                let mut hasher = blake3::Hasher::new();
                hasher.update(b"ravel-tenant-v1");
                hasher.update(tenant.0.as_bytes());
                hasher.finalize()
            }
            TenantHashScheme::V2Keyed { hash_key } => {
                blake3::keyed_hash(hash_key, tenant.0.as_bytes())
            }
        };
        let mut out = [0u8; 16];
        out.copy_from_slice(&digest.as_bytes()[..16]);
        TenantHash(out)
    }
}

static ACTIVE_TENANT_HASH_SCHEME: std::sync::OnceLock<TenantHashScheme> =
    std::sync::OnceLock::new();

/// Install the process-wide tenant-hash scheme resolved from `sys/tenancy`
/// (ADR-0050 section 3). Callable once per process; a second call returns the
/// rejected scheme as `Err` and leaves the first install in force. Real
/// binaries call this exactly once at startup, before the first `.hash()`.
/// Tests that need to exercise a specific derivation call
/// [`TenantHashScheme::hash`] directly rather than mutating this global.
pub fn install_tenant_hash_scheme(scheme: TenantHashScheme) -> Result<(), TenantHashScheme> {
    ACTIVE_TENANT_HASH_SCHEME.set(scheme)
}

/// The installed scheme, or [`TenantHashScheme::V1Unkeyed`] when none was
/// installed. The default is what makes an un-pinned process byte-for-byte
/// compatible with pre-ADR-0050 behavior.
fn active_tenant_hash_scheme() -> &'static TenantHashScheme {
    static DEFAULT_SCHEME: TenantHashScheme = TenantHashScheme::V1Unkeyed;
    ACTIVE_TENANT_HASH_SCHEME.get().unwrap_or(&DEFAULT_SCHEME)
}

/// The tenant-hash derivation a bucket's `sys/tenancy` marker records
/// (ADR-0050 section 3), decoded from the persisted protobuf enum by the
/// caller so this crate stays free of a protobuf dependency. Only the two real
/// derivations appear here; an unspecified or unknown marker value is the
/// caller's own "corrupt marker" refusal and never reaches this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerScheme {
    V1Unkeyed,
    V2Keyed,
}

/// The tenant-hash scheme a process was configured for from its startup or CLI
/// flags, before the bucket's `sys/tenancy` marker is read (ADR-0050 section
/// 3). Held here, next to the derivation it selects, so the server startup
/// path and the CLI resolve a configured scheme against a marker through one
/// shared, fail-closed decision ([`resolve_scheme_against_marker`]) rather than
/// two copies that could drift on the most safety-critical rule in the design.
///
/// The raw deployment key is carried only long enough to derive the hashing
/// scheme (and, server-side, the recovery-manifest AEAD key). [`Debug`] is
/// hand-written to keep those bytes out of any log line.
#[derive(Clone)]
pub enum ConfiguredScheme {
    /// A 32-byte deployment key was supplied (`--tenant-hash-key-file`).
    Keyed(Box<[u8; 32]>),
    /// The operator opted out of keying (`--tenant-hash-unkeyed`).
    Unkeyed,
    /// Neither flag was supplied. A fresh bucket refuses (keyed is the
    /// default and needs a key); an existing bucket takes its marker's scheme.
    Unspecified,
}

impl ConfiguredScheme {
    /// A short human label for startup and refusal messages. Prints only the
    /// choice, never the key bytes.
    pub fn label(&self) -> &'static str {
        match self {
            ConfiguredScheme::Keyed(_) => "v2-keyed",
            ConfiguredScheme::Unkeyed => "v1-unkeyed",
            ConfiguredScheme::Unspecified => "unspecified (keyed-by-default)",
        }
    }
}

impl std::fmt::Debug for ConfiguredScheme {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the deployment key; only which variant is configured.
        f.write_str(match self {
            ConfiguredScheme::Keyed(_) => "Keyed(<redacted>)",
            ConfiguredScheme::Unkeyed => "Unkeyed",
            ConfiguredScheme::Unspecified => "Unspecified",
        })
    }
}

/// A configured scheme is incompatible with what a bucket's marker already
/// pins (ADR-0050 section 3). Every variant is a fail-closed refusal: the
/// marker is authoritative, and a process that disagrees must stop rather than
/// open a silent parallel namespace under the wrong derivation. This is the
/// single most correctness-critical decision in the tenancy design, shared so
/// the server and the CLI cannot drift on it.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum SchemeResolutionError {
    #[error(
        "sys/tenancy pins this bucket to {marker_scheme}, but this process is configured for \
         {configured_scheme}: refusing to open a parallel namespace"
    )]
    SchemeMismatch {
        marker_scheme: &'static str,
        configured_scheme: &'static str,
    },
    #[error(
        "sys/tenancy pins this bucket to v2-keyed, but no deployment key was configured: the \
         bucket's prefixes are unreadable without it"
    )]
    KeyRequired,
    #[error(
        "sys/tenancy pins this bucket to v2-keyed with key fingerprint {expected}, but the \
         configured key has fingerprint {got}: refusing to open a parallel namespace under the \
         wrong key"
    )]
    KeyFingerprintMismatch { expected: String, got: String },
}

/// Resolve a configured scheme against a bucket's *present* `sys/tenancy`
/// marker: the shared fail-closed decision from ADR-0050 section 3. Returns the
/// [`TenantHashScheme`] to install, or a typed refusal. The marker is
/// authoritative; a configured scheme that disagrees is never silently
/// downgraded to another derivation.
///
/// This decides only the marker-present case. What to do when no marker exists
/// (bootstrap a fresh bucket, or adopt a pre-ADR one as unkeyed) is a
/// deployment-specific policy the caller owns: the server writes a marker; the
/// read-only CLI treats an absent marker as unkeyed.
pub fn resolve_scheme_against_marker(
    marker_scheme: MarkerScheme,
    marker_fingerprint: &[u8],
    configured: &ConfiguredScheme,
) -> Result<TenantHashScheme, SchemeResolutionError> {
    match marker_scheme {
        MarkerScheme::V1Unkeyed => match configured {
            ConfiguredScheme::Keyed(_) => Err(SchemeResolutionError::SchemeMismatch {
                marker_scheme: "v1-unkeyed",
                configured_scheme: configured.label(),
            }),
            ConfiguredScheme::Unkeyed | ConfiguredScheme::Unspecified => {
                Ok(TenantHashScheme::V1Unkeyed)
            }
        },
        MarkerScheme::V2Keyed => match configured {
            ConfiguredScheme::Unkeyed => Err(SchemeResolutionError::SchemeMismatch {
                marker_scheme: "v2-keyed",
                configured_scheme: configured.label(),
            }),
            ConfiguredScheme::Unspecified => Err(SchemeResolutionError::KeyRequired),
            ConfiguredScheme::Keyed(key) => {
                let fp = tenant_key_fingerprint(key);
                if marker_fingerprint != fp.as_slice() {
                    return Err(SchemeResolutionError::KeyFingerprintMismatch {
                        expected: hex::encode(marker_fingerprint),
                        got: hex::encode(fp),
                    });
                }
                Ok(TenantHashScheme::v2_from_deployment_key(key))
            }
        },
    }
}

/// 128-bit tenant prefix hash. Hex form (32 chars) appears in object keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TenantHash(pub [u8; 16]);

impl TenantHash {
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(s: &str) -> Result<Self, TypeError> {
        let bytes = hex::decode(s).map_err(|_| TypeError::InvalidTenantHash)?;
        let arr: [u8; 16] = bytes.try_into().map_err(|_| TypeError::InvalidTenantHash)?;
        Ok(TenantHash(arr))
    }
}

/// Reserved label carrying the metric name, mirroring Prometheus.
pub const METRIC_NAME_LABEL: &str = "__name__";

/// One label pair. Names and values are UTF-8; length limits are enforced at
/// ingest admission, not here.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Label {
    pub name: String,
    pub value: String,
}

/// A set of labels sorted by name with unique names. Construction sorts and
/// rejects duplicates so every holder can rely on the invariant.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct LabelSet(Vec<Label>);

impl LabelSet {
    /// Build from arbitrary pairs. Sorts by name; duplicate names are an error.
    pub fn new(mut labels: Vec<Label>) -> Result<Self, TypeError> {
        labels.sort_by(|a, b| a.name.cmp(&b.name));
        for w in labels.windows(2) {
            if w[0].name == w[1].name {
                return Err(TypeError::DuplicateLabelName(w[0].name.clone()));
            }
        }
        Ok(LabelSet(labels))
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.0
            .binary_search_by(|l| l.name.as_str().cmp(name))
            .ok()
            .map(|i| self.0[i].value.as_str())
    }

    pub fn iter(&self) -> impl Iterator<Item = &Label> {
        self.0.iter()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// 128-bit series identity (ADR-0005).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SeriesId(pub [u8; 16]);

impl SeriesId {
    /// Compute the canonical series id. `labels` may include
    /// [`METRIC_NAME_LABEL`]; it is skipped (the name is hashed separately).
    ///
    /// Any component longer than `u16::MAX` bytes, or more than `u16::MAX`
    /// labels, is an error: silent truncation would create a collision
    /// vector in a persistent hash contract (ADR-0010 §6). Admission limits
    /// keep real inputs orders of magnitude below these bounds.
    ///
    /// Canonical encoding (persistent contract, ADR-0005):
    /// ```text
    /// "ravel-series-v1\0"
    /// u16_le(len(tenant)) tenant
    /// u16_le(len(name)) name
    /// u16_le(label_count)
    /// per label sorted by name: u16_le(len(k)) k u16_le(len(v)) v
    /// ```
    pub fn compute(
        tenant: &TenantId,
        metric_name: &str,
        labels: &LabelSet,
    ) -> Result<Self, TypeError> {
        // The canonical byte stream is built in a reused scratch buffer and
        // hashed in one shot: many small `blake3::Hasher::update` calls cost
        // about 2x the single-buffer hash for typical label sets, and this
        // function sits on the per-point ingest path.
        thread_local! {
            static SCRATCH: std::cell::RefCell<Vec<u8>> =
                const { std::cell::RefCell::new(Vec::new()) };
        }
        SCRATCH.with(|cell| {
            let mut buf = cell.borrow_mut();
            buf.clear();
            buf.extend_from_slice(b"ravel-series-v1\0");
            push_len_prefixed(&mut buf, tenant.as_str())?;
            push_len_prefixed(&mut buf, metric_name)?;
            let count = labels
                .iter()
                .filter(|l| l.name != METRIC_NAME_LABEL)
                .count();
            let count = u16::try_from(count).map_err(|_| TypeError::OversizedSeriesComponent)?;
            buf.extend_from_slice(&count.to_le_bytes());
            for label in labels.iter().filter(|l| l.name != METRIC_NAME_LABEL) {
                push_len_prefixed(&mut buf, &label.name)?;
                push_len_prefixed(&mut buf, &label.value)?;
            }
            let digest = blake3::hash(&buf);
            let mut out = [0u8; 16];
            out.copy_from_slice(&digest.as_bytes()[..16]);
            Ok(SeriesId(out))
        })
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }
}

fn push_len_prefixed(buf: &mut Vec<u8>, s: &str) -> Result<(), TypeError> {
    let len = u16::try_from(s.len()).map_err(|_| TypeError::OversizedSeriesComponent)?;
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(s.as_bytes());
    Ok(())
}

/// One metric sample. Nanosecond timestamps throughout the system.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Sample {
    pub ts_ns: i64,
    pub value: f64,
}

/// Closed time range `[start_ns, end_ns]` in event time, matching PromQL
/// range-selector semantics. Use [`TimeRange::overlaps`] for pruning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    pub start_ns: i64,
    pub end_ns: i64,
}

impl TimeRange {
    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.start_ns <= other.end_ns && other.start_ns <= self.end_ns
    }
}

/// Routing v1 (persistent contract): shard from the series id's leading
/// bytes. The id already includes the tenant, so this is tenant-scoped.
pub fn shard_for(series_id: &SeriesId, shard_count: u32) -> u32 {
    debug_assert!(shard_count > 0);
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&series_id.0[..8]);
    (u64::from_le_bytes(prefix) % u64::from(shard_count.max(1))) as u32
}

/// Routing v1 for logs (persistent contract, mirrors [`shard_for`]): shard
/// from the stream id's leading bytes.
///
/// Unlike [`shard_for`], the id itself carries no tenant:
/// [`logstream::log_stream_id`] hashes only the OTLP resource and scope, so
/// two tenants sending the same resource+scope produce the same
/// [`logstream::LogStreamId`]. Routing is still tenant-scoped, because each
/// shard buffers per `TenantId` upstream of this function, not because of
/// anything in the id.
pub fn shard_for_log(stream_id: &logstream::LogStreamId, shard_count: u32) -> u32 {
    debug_assert!(shard_count > 0);
    let mut prefix = [0u8; 8];
    prefix.copy_from_slice(&stream_id.0[..8]);
    (u64::from_le_bytes(prefix) % u64::from(shard_count.max(1))) as u32
}

/// Commit token returned on strict-mode acks; callers pass tokens back as
/// `min_commit_token` for read-your-write (docs/catalog-and-mvcc.md).
///
/// v2 (ADR-0010 §2): carries the pinned ingest-hour bucket so the token
/// fully determines its commit-record key; the catalog resolves it by an
/// exact GET, never by listing. Acks return one token per shard flushed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitToken {
    pub shard: u32,
    pub writer_id: uuid::Uuid,
    pub epoch: u64,
    pub seq: u64,
    /// Unix hours, pinned at flush open (ADR-0010 §1).
    pub ingest_hour_bucket: u32,
}

impl CommitToken {
    pub fn encode(&self) -> String {
        let raw = format!(
            "v2:{}:{}:{}:{}:{}",
            self.shard, self.writer_id, self.epoch, self.seq, self.ingest_hour_bucket
        );
        URL_SAFE_NO_PAD.encode(raw.as_bytes())
    }

    pub fn decode(s: &str) -> Result<Self, TypeError> {
        let raw = URL_SAFE_NO_PAD
            .decode(s.as_bytes())
            .map_err(|_| TypeError::InvalidCommitToken)?;
        let raw = String::from_utf8(raw).map_err(|_| TypeError::InvalidCommitToken)?;
        let mut parts = raw.split(':');
        // Distinguish a token whose version prefix is well-formed but
        // unrecognized (e.g. a future "v3") from genuinely malformed input, so
        // a rolling upgrade can diagnose a too-new token separately from
        // garbage. Same "typed, not flattened" principle as the HEAD
        // fail-closed-on-newer rule (ADR-0066 decision 2).
        match parts.next() {
            Some("v2") => {}
            Some(prefix) if is_version_prefix(prefix) => {
                return Err(TypeError::UnsupportedCommitTokenVersion(prefix.to_string()));
            }
            _ => return Err(TypeError::InvalidCommitToken),
        }
        let (Some(shard), Some(writer), Some(epoch), Some(seq), Some(hour), None) = (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) else {
            return Err(TypeError::InvalidCommitToken);
        };
        Ok(CommitToken {
            shard: shard.parse().map_err(|_| TypeError::InvalidCommitToken)?,
            writer_id: writer.parse().map_err(|_| TypeError::InvalidCommitToken)?,
            epoch: epoch.parse().map_err(|_| TypeError::InvalidCommitToken)?,
            seq: seq.parse().map_err(|_| TypeError::InvalidCommitToken)?,
            ingest_hour_bucket: hour.parse().map_err(|_| TypeError::InvalidCommitToken)?,
        })
    }
}

/// True for a well-formed version prefix: `v` followed by one or more ASCII
/// digits (e.g. `v2`, `v3`, `v10`). Lets [`CommitToken::decode`] tell a
/// recognizable-but-unsupported version apart from a token that has no version
/// shape at all.
fn is_version_prefix(s: &str) -> bool {
    match s.strip_prefix('v') {
        Some(rest) => !rest.is_empty() && rest.bytes().all(|b| b.is_ascii_digit()),
        None => false,
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TypeError {
    #[error("duplicate label name: {0}")]
    DuplicateLabelName(String),
    #[error("invalid tenant hash")]
    InvalidTenantHash,
    #[error("invalid commit token")]
    InvalidCommitToken,
    /// A commit token whose version prefix is well-formed (`v` + digits) but
    /// not a version this build supports, e.g. a future `v3` token reaching an
    /// older process during a rolling upgrade. Distinct from
    /// [`TypeError::InvalidCommitToken`] (genuinely malformed input) so the
    /// too-new case is diagnosable on its own. Carries the unrecognized prefix.
    #[error("unsupported commit token version: {0}")]
    UnsupportedCommitTokenVersion(String),
    #[error("series identity component exceeds u16::MAX bytes or labels")]
    OversizedSeriesComponent,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::logstream::LogStreamId;

    #[test]
    fn shard_for_log_matches_leading_bytes_mod_shard_count() {
        let id = LogStreamId([7u8; 16]);
        let mut prefix = [0u8; 8];
        prefix.copy_from_slice(&id.0[..8]);
        let expected = (u64::from_le_bytes(prefix) % 4) as u32;
        assert_eq!(shard_for_log(&id, 4), expected);
    }

    #[test]
    fn shard_for_log_is_deterministic_across_calls() {
        let id = LogStreamId([9u8; 16]);
        assert_eq!(shard_for_log(&id, 8), shard_for_log(&id, 8));
    }

    fn labels(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(n, v)| Label {
                    name: n.to_string(),
                    value: v.to_string(),
                })
                .collect(),
        )
        .expect("valid labels")
    }

    #[test]
    fn series_id_is_order_independent_and_name_label_agnostic() {
        let tenant = TenantId::new("acme");
        let a = SeriesId::compute(
            &tenant,
            "http_requests_total",
            &labels(&[("a", "1"), ("b", "2")]),
        )
        .expect("id");
        let b = SeriesId::compute(
            &tenant,
            "http_requests_total",
            &labels(&[("b", "2"), ("a", "1")]),
        )
        .expect("id");
        let c = SeriesId::compute(
            &tenant,
            "http_requests_total",
            &labels(&[
                ("a", "1"),
                ("b", "2"),
                (METRIC_NAME_LABEL, "http_requests_total"),
            ]),
        )
        .expect("id");
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn series_id_matches_documented_incremental_encoding() {
        // The buffered one-shot implementation must produce exactly the
        // digest of the canonical stream from ADR-0005, reconstructed here
        // with incremental hasher updates. This pins the persistent
        // contract independently of how `compute` builds its bytes.
        let tenant = TenantId::new("acme");
        let set = labels(&[("region", "eu-1"), ("zone", "a")]);
        let id = SeriesId::compute(&tenant, "http_requests_total", &set).expect("id");

        let mut hasher = blake3::Hasher::new();
        hasher.update(b"ravel-series-v1\0");
        for s in ["acme", "http_requests_total"] {
            hasher.update(&(s.len() as u16).to_le_bytes());
            hasher.update(s.as_bytes());
        }
        hasher.update(&2u16.to_le_bytes());
        for s in ["region", "eu-1", "zone", "a"] {
            hasher.update(&(s.len() as u16).to_le_bytes());
            hasher.update(s.as_bytes());
        }
        let digest = hasher.finalize();
        assert_eq!(id.0, digest.as_bytes()[..16]);
    }

    #[test]
    fn series_id_differs_across_tenants() {
        let l = labels(&[("a", "1")]);
        let a = SeriesId::compute(&TenantId::new("t1"), "m", &l).expect("id");
        let b = SeriesId::compute(&TenantId::new("t2"), "m", &l).expect("id");
        assert_ne!(a, b);
    }

    #[test]
    fn series_id_rejects_oversized_components() {
        let tenant = TenantId::new("acme");
        let long = "x".repeat(usize::from(u16::MAX) + 1);
        let err = SeriesId::compute(&tenant, &long, &labels(&[]));
        assert_eq!(err, Err(TypeError::OversizedSeriesComponent));
        let err = SeriesId::compute(&tenant, "m", &labels(&[("k", long.as_str())]));
        assert_eq!(err, Err(TypeError::OversizedSeriesComponent));
    }

    #[test]
    fn commit_token_roundtrip() {
        let token = CommitToken {
            shard: 3,
            writer_id: uuid::Uuid::new_v4(),
            epoch: 1_753_500_000,
            seq: 42,
            ingest_hour_bucket: 495_972,
        };
        assert_eq!(
            CommitToken::decode(&token.encode()).expect("decodes"),
            token
        );
        assert!(CommitToken::decode("not-a-token").is_err());
    }

    #[test]
    fn commit_token_unrecognized_version_is_distinct_from_garbage() {
        // A future "v3" token reaching this older build: well-formed version
        // prefix, unsupported version. Diagnosable on its own.
        let v3 = URL_SAFE_NO_PAD.encode(b"v3:0:some-writer:1:2:3");
        assert_eq!(
            CommitToken::decode(&v3),
            Err(TypeError::UnsupportedCommitTokenVersion("v3".to_string()))
        );

        // A downgrade-looking "v1" is equally an unsupported version, not
        // garbage.
        let v1 = URL_SAFE_NO_PAD.encode(b"v1:0:w:1:2:3");
        assert_eq!(
            CommitToken::decode(&v1),
            Err(TypeError::UnsupportedCommitTokenVersion("v1".to_string()))
        );

        // No recognizable version shape at all stays InvalidCommitToken.
        let garbage = URL_SAFE_NO_PAD.encode(b"xyzzy:0:1:2:3");
        assert_eq!(
            CommitToken::decode(&garbage),
            Err(TypeError::InvalidCommitToken)
        );
        // "version" prefix that is not `v`+digits (e.g. `v` then letters) is
        // garbage, not an unsupported version.
        let not_versioned = URL_SAFE_NO_PAD.encode(b"vX:0:1:2:3");
        assert_eq!(
            CommitToken::decode(&not_versioned),
            Err(TypeError::InvalidCommitToken)
        );
        // Non-base64 input is still InvalidCommitToken.
        assert_eq!(
            CommitToken::decode("not-a-token"),
            Err(TypeError::InvalidCommitToken)
        );
    }

    #[test]
    fn label_set_rejects_duplicates_and_sorts() {
        let err = LabelSet::new(vec![
            Label {
                name: "x".into(),
                value: "1".into(),
            },
            Label {
                name: "x".into(),
                value: "2".into(),
            },
        ]);
        assert!(matches!(err, Err(TypeError::DuplicateLabelName(_))));
        let set = labels(&[("z", "1"), ("a", "2")]);
        let names: Vec<_> = set.iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, vec!["a", "z"]);
    }

    #[test]
    fn tenant_hash_hex_roundtrip() {
        let h = TenantId::new("acme").hash();
        assert_eq!(TenantHash::from_hex(&h.to_hex()).expect("decodes"), h);
    }

    #[test]
    fn keyed_hash_v2_differs_from_v1_and_roundtrips() {
        let tenant = TenantId::new("acme");
        let key = [7u8; 32];
        let v1 = TenantHashScheme::V1Unkeyed.hash(&tenant);
        let v2 = TenantHashScheme::v2_from_deployment_key(&key).hash(&tenant);
        // The keyed derivation must move the prefix off the unkeyed one; a
        // bucket switching schemes must not land tenants on their old prefixes.
        assert_ne!(v1, v2, "v2 keyed hash must differ from v1 unkeyed");
        // Deterministic given the same key and id: the scheme is rebuilt from
        // the same deployment key and produces the same prefix.
        let v2_again = TenantHashScheme::v2_from_deployment_key(&key).hash(&tenant);
        assert_eq!(v2, v2_again, "v2 hash is deterministic given key + id");
        // A different key yields a different prefix (enumeration resistance):
        // the same tenant id under a different deployment key is unlinkable.
        let v2_other = TenantHashScheme::v2_from_deployment_key(&[9u8; 32]).hash(&tenant);
        assert_ne!(
            v2, v2_other,
            "different deployment key must move the prefix"
        );
    }

    #[test]
    fn v1_unkeyed_scheme_matches_frozen_derivation() {
        // Pin the v1 derivation independently of `hash()`'s implementation:
        // blake3("ravel-tenant-v1" || id)[0..16]. If this ever changes, every
        // existing bucket's prefixes move and this test fails loudly.
        let tenant = TenantId::new("acme");
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"ravel-tenant-v1");
        hasher.update(b"acme");
        let digest = hasher.finalize();
        let mut expected = [0u8; 16];
        expected.copy_from_slice(&digest.as_bytes()[..16]);
        assert_eq!(TenantHashScheme::V1Unkeyed.hash(&tenant).0, expected);
        // The uninstalled default `.hash()` must equal the explicit v1 scheme.
        assert_eq!(tenant.hash(), TenantHashScheme::V1Unkeyed.hash(&tenant));
    }

    #[test]
    fn debug_redacts_keyed_scheme_key() {
        let scheme = TenantHashScheme::v2_from_deployment_key(&[0x11; 32]);
        let rendered = format!("{scheme:?}");
        assert!(rendered.contains("<redacted>"), "got: {rendered}");
        // The raw derived key must not appear in any form.
        assert!(!rendered.contains("11, 11"), "leaked key bytes: {rendered}");
        assert!(!rendered.contains("17"), "leaked key bytes: {rendered}");
        assert_eq!(format!("{:?}", TenantHashScheme::V1Unkeyed), "V1Unkeyed");
    }

    #[test]
    fn debug_redacts_configured_key() {
        let configured = ConfiguredScheme::Keyed(Box::new([0x22; 32]));
        let rendered = format!("{configured:?}");
        assert!(rendered.contains("<redacted>"), "got: {rendered}");
        assert!(!rendered.contains("34"), "leaked key bytes: {rendered}");
        assert_eq!(format!("{:?}", ConfiguredScheme::Unkeyed), "Unkeyed");
    }

    #[test]
    fn resolve_keyed_marker_without_key_is_fail_closed() {
        // A v2-keyed marker with no key configured must refuse, never fall back
        // to the v1 default (the data-safety bug this guards).
        let err = resolve_scheme_against_marker(
            MarkerScheme::V2Keyed,
            &[0u8; TENANT_KEY_FINGERPRINT_LEN],
            &ConfiguredScheme::Unspecified,
        )
        .expect_err("keyed marker, no key, must refuse");
        assert_eq!(err, SchemeResolutionError::KeyRequired);
    }

    #[test]
    fn resolve_keyed_marker_with_correct_key_yields_v2() {
        let key = [7u8; 32];
        let fp = tenant_key_fingerprint(&key);
        let scheme = resolve_scheme_against_marker(
            MarkerScheme::V2Keyed,
            &fp,
            &ConfiguredScheme::Keyed(Box::new(key)),
        )
        .expect("correct key resolves");
        // The resolved scheme reproduces exactly the keyed prefix.
        let tenant = TenantId::new("acme");
        assert_eq!(
            scheme.hash(&tenant),
            TenantHashScheme::v2_from_deployment_key(&key).hash(&tenant)
        );
    }

    #[test]
    fn resolve_keyed_marker_with_wrong_key_is_fingerprint_mismatch() {
        let fp = tenant_key_fingerprint(&[1u8; 32]);
        let err = resolve_scheme_against_marker(
            MarkerScheme::V2Keyed,
            &fp,
            &ConfiguredScheme::Keyed(Box::new([2u8; 32])),
        )
        .expect_err("a wrong key must refuse");
        assert!(matches!(
            err,
            SchemeResolutionError::KeyFingerprintMismatch { .. }
        ));
    }

    #[test]
    fn resolve_unkeyed_marker_rejects_a_key_and_accepts_unkeyed() {
        let err = resolve_scheme_against_marker(
            MarkerScheme::V1Unkeyed,
            &[],
            &ConfiguredScheme::Keyed(Box::new([3u8; 32])),
        )
        .expect_err("keying an unkeyed bucket must refuse");
        assert!(matches!(err, SchemeResolutionError::SchemeMismatch { .. }));

        let scheme =
            resolve_scheme_against_marker(MarkerScheme::V1Unkeyed, &[], &ConfiguredScheme::Unkeyed)
                .expect("unkeyed marker, unkeyed config resolves");
        assert!(matches!(scheme, TenantHashScheme::V1Unkeyed));
    }
}
