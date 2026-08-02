//! Core Ravel domain types: tenants, signals, labels, series identity,
//! samples, time ranges, shard routing, and commit tokens.
//!
//! This crate is dependency-light and defines the identity rules the rest of
//! the system builds on (see ADR-0005, ADR-0009). Canonical encodings here
//! are persistent contracts: changing them requires a new version domain
//! string, never an in-place edit.

pub mod accounting;
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

    pub fn hash(&self) -> TenantHash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"ravel-tenant-v1");
        hasher.update(self.0.as_bytes());
        let digest = hasher.finalize();
        let mut out = [0u8; 16];
        out.copy_from_slice(&digest.as_bytes()[..16]);
        TenantHash(out)
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
        let (Some("v2"), Some(shard), Some(writer), Some(epoch), Some(seq), Some(hour), None) = (
            parts.next(),
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

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TypeError {
    #[error("duplicate label name: {0}")]
    DuplicateLabelName(String),
    #[error("invalid tenant hash")]
    InvalidTenantHash,
    #[error("invalid commit token")]
    InvalidCommitToken,
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
}
