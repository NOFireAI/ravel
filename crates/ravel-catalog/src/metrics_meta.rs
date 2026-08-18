//! Per-tenant metric metadata record (ADR-0085 decision 1).
//!
//! Each tenant carries one record at `t/<tenant_hash>/m/meta` holding the
//! `{type, help, unit}` tuple captured at ingest for every metric family name
//! seen for that tenant. It sits under the metrics signal prefix `m/` like every
//! other metrics-scoped record (`m/l0`, `m/c`), never a new top-level prefix,
//! and it is one entry per metric *family name*, not per series: Prometheus
//! metadata semantics are per name, and Ravel has no per-target scrape concept
//! that would make several tuples per name meaningful.
//!
//! Body shape: the prost message [`sysproto::MetricMetadataRecord`],
//! zstd-compressed, following the precedent every other durable
//! `t/<tenant_hash>/` record sets (`config`, `enc`, `prov` are all versioned
//! prost behind a `format_version` guard; this crate carries no serde
//! dependency), not the JSON shape of the root-level, per-process, ephemeral
//! `admission/query/*` key.
//!
//! Mutation shape: whole-record CAS-replace, mirroring
//! [`crate::tenant_config::set_tenant_config`] and `set_gc_config` (read the
//! current version, swap under `PutMode::CasVersion`), NOT the append-only
//! repeated-field shape of `provisioning`'s `generations` or `key_epoch`'s
//! `epochs`. Those histories are append-only because each entry is an immutable
//! historical fact. Metadata is the opposite: mutable current state whose latest
//! value is the only one that matters, and which must support *shrinking* --
//! pruning a record back under the per-tenant entry cap by evicting the oldest
//! `updated_unix_ns` entries is a change an append-only list cannot express, and
//! it is why no role needs a delete grant on this key.
//!
//! This module owns the durable record and the merge rule only. It deliberately
//! has no production caller yet: the ingest-side metadata sink that reads,
//! merges, and CAS-writes this record on a debounce window is #235
//! (`ravel-ingest`), and the `/api/v1/metadata` read path that serves from it
//! behind a per-process cache is #236 (`ravel-query`). The split is on purpose
//! so both land against one already-tested durable record rather than each
//! inventing its own.
//!
//! Unlike [`crate::tenant_config::set_tenant_config`], [`write_metrics_meta`]
//! does not read-then-swap internally: the caller already holds the version it
//! read (it had to read the record to merge onto it), and hiding a second read
//! inside the write would both double the request count the ADR budgets (one
//! GET and at most one PUT per tenant per window) and throw away the merge the
//! caller computed against the body it actually saw.

use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
use ravel_proto::sys::v1 as sysproto;
use ravel_types::TenantHash;
use std::collections::{BTreeMap, BTreeSet};

/// Format floor written into every metadata record this build emits, and the
/// highest record version it understands. A record declaring a higher version is
/// refused rather than misread under this layout, matching the `config` / `prov`
/// / `enc` records' version guards.
pub const METRICS_META_FORMAT_VERSION: u32 = 1;

/// zstd level for the record body. 3 is the level
/// [`crate::snapshot_format`] already uses for its compressed bodies and zstd's
/// own default; a single fixed level (never a caller-chosen one) is what makes
/// two processes that merged to the same entry set produce identical bytes. The
/// ADR is explicit that compression here buys read latency and egress, not
/// dollars (request count, not bytes, drives the S3 bill), so there is nothing
/// to gain from a slower level: at roughly 250 KB of prost for 2,000 families,
/// level 3 already gets the body to roughly 50 KB.
const ZSTD_LEVEL: i32 = 3;

/// Hard ceiling on the decompressed record body. A record at the ADR's default
/// 20,000-entry cap is on the order of 3 MB, so 32 MiB is roughly ten times the
/// largest legitimate body while still bounding what a corrupt or hostile
/// compressed body can make this process allocate. Decompression streams into a
/// growing buffer and stops at this bound, so a well-formed small record costs
/// its own size, not this cap.
pub const MAX_METRICS_META_DECOMPRESSED_BYTES: usize = 32 * 1024 * 1024;

/// The ADR's default per-tenant entry cap: once a record holds this many
/// families, a *new* family name is dropped rather than added (the point is
/// still ingested and queryable; only its metadata entry is dropped, and the
/// sink counts the drop). Exported as the default for the sink's configurable
/// cap, not enforced here: [`merge_entries`] takes the cap as a parameter so a
/// deployment override needs no change in this module.
pub const DEFAULT_METRICS_META_ENTRY_CAP: usize = 20_000;

/// Object key for a tenant's metric metadata record: `t/<hex>/m/meta`. Under the
/// metrics signal prefix, beside `m/l0` and `m/c` (ADR-0085 decision 1).
pub fn metrics_meta_key(tenant_hash: &TenantHash) -> String {
    format!("t/{}/m/meta", tenant_hash.to_hex())
}

/// A metric family's type, decoded into a domain enum so consumers never touch
/// the proto `i32`. [`MetricKind::Unknown`] is a real, written value -- the
/// protocol carried a point but named no type -- and is distinct from the proto
/// `UNSPECIFIED` sentinel, which no writer emits and which is refused on decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricKind {
    /// A monotonically increasing cumulative value.
    Counter,
    /// A value that can go up or down.
    Gauge,
    /// A distribution with bucket, sum, and count series.
    Histogram,
    /// A distribution with quantile, sum, and count series.
    Summary,
    /// The protocol supplied no type. Treated as "no opinion" by
    /// [`merge_entries`]: it never overwrites a known type already stored.
    Unknown,
}

impl MetricKind {
    /// Map to the persisted proto enum value.
    fn to_proto(self) -> sysproto::MetricMetadataType {
        match self {
            MetricKind::Counter => sysproto::MetricMetadataType::Counter,
            MetricKind::Gauge => sysproto::MetricMetadataType::Gauge,
            MetricKind::Histogram => sysproto::MetricMetadataType::Histogram,
            MetricKind::Summary => sysproto::MetricMetadataType::Summary,
            MetricKind::Unknown => sysproto::MetricMetadataType::Unknown,
        }
    }

    /// Map from the persisted proto `i32`. `UNSPECIFIED` (0) and any unrecognized
    /// value are refused: a written entry always names a real type (`UNKNOWN` is
    /// itself a real type here), so an absent or unknown one is corruption, not a
    /// default to guess at.
    fn from_proto_i32(value: i32) -> Option<Self> {
        match sysproto::MetricMetadataType::try_from(value).ok()? {
            sysproto::MetricMetadataType::Unspecified => None,
            sysproto::MetricMetadataType::Counter => Some(MetricKind::Counter),
            sysproto::MetricMetadataType::Gauge => Some(MetricKind::Gauge),
            sysproto::MetricMetadataType::Histogram => Some(MetricKind::Histogram),
            sysproto::MetricMetadataType::Summary => Some(MetricKind::Summary),
            sysproto::MetricMetadataType::Unknown => Some(MetricKind::Unknown),
        }
    }
}

/// One metric family's metadata, decoded into a plain struct so consumers (the
/// ingest sink, the `/api/v1/metadata` handler) never touch the proto type.
///
/// `help` and `unit` may each be empty: a protocol that carries one but not the
/// other is normal, and [`merge_entries`] composes the two rather than letting
/// one clobber the other.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricMetadataEntry {
    /// The metric family name (the record's key). Empty is invalid and refused
    /// on both read and write.
    pub family_name: String,
    /// The family's type.
    pub kind: MetricKind,
    /// Help text as the protocol supplied it; empty when it supplied none.
    pub help: String,
    /// Unit as the protocol supplied it; empty when it supplied none. For
    /// OTLP/OTAP this is the mapped Prometheus word (`bytes`, `seconds`),
    /// matching the suffix on the name, not the raw UCUM string.
    pub unit: String,
    /// When this entry's fields last actually changed. Informational, and the
    /// eviction order when a record is pruned back under the entry cap.
    pub updated_unix_ns: i64,
}

/// A structural defect in a stored metadata record: a shape no writer in this
/// build can produce, so reading past it would serve corrupt metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricsMetaDefect {
    /// An entry's `family_name` is empty. The name is the record's key; an
    /// entry keyed by nothing can never be looked up.
    EmptyFamilyName,
    /// Two entries share a `family_name`, making an `/api/v1/metadata` lookup
    /// for that name ambiguous.
    DuplicateFamilyName,
}

impl std::fmt::Display for MetricsMetaDefect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            MetricsMetaDefect::EmptyFamilyName => "an entry has an empty family_name",
            MetricsMetaDefect::DuplicateFamilyName => {
                "two entries share a family_name (ambiguous lookup)"
            }
        };
        f.write_str(s)
    }
}

/// A typed metadata-record failure. Every variant is fatal to the touch that
/// raised it, the fail-closed-on-any-anomaly discipline `TenantConfigError` and
/// `AuthTokenMapError` follow. None warn and continue, and none panic: a corrupt
/// body is an error value, never an abort.
#[derive(Debug, thiserror::Error)]
pub enum MetricsMetaError {
    #[error("object store error on metadata record {key:?}: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("metadata record {key:?} could not be compressed: {message}")]
    Compress { key: String, message: String },
    #[error(
        "metadata record {key:?} could not be decompressed (corrupt or truncated body): {message}"
    )]
    Decompress { key: String, message: String },
    #[error(
        "metadata record {key:?} decompresses to more than the {cap}-byte ceiling this build \
         accepts: refusing rather than allocate an unbounded body"
    )]
    DecompressedTooLarge { key: String, cap: usize },
    #[error("metadata record {key:?} could not be decoded: {source}")]
    Decode {
        key: String,
        #[source]
        source: prost::DecodeError,
    },
    #[error(
        "metadata record {key:?} declares format_version {got}, but this build only understands \
         version {METRICS_META_FORMAT_VERSION}: refusing rather than misread a future record format"
    )]
    UnsupportedVersion { key: String, got: u32 },
    /// The record records a different tenant than the key it was read under. It
    /// is another tenant's metadata misfiled here; serving it would leak one
    /// tenant's metric names to another, so it is refused (the
    /// `TenantConfigRecord` misfile guard).
    #[error(
        "metadata record {key:?} is misfiled: it records tenant_hash {actual}, but the key it was \
         read under expects {expected}"
    )]
    MisfiledTenant {
        key: String,
        expected: String,
        actual: String,
    },
    #[error("metadata record {key:?} is corrupt: {defect} (ADR-0085 decision 1)")]
    Corrupt {
        key: String,
        defect: MetricsMetaDefect,
    },
    /// An entry's `type` is the `UNSPECIFIED` sentinel or an unrecognized value.
    /// A written entry always names a real type (`UNKNOWN` included), so this is
    /// corruption rather than a value to default.
    #[error(
        "metadata record {key:?} entry {family_name:?} has an invalid type {got}: a written entry \
         must name counter, gauge, histogram, summary, or unknown (ADR-0085 decision 1)"
    )]
    InvalidMetricKind {
        key: String,
        family_name: String,
        got: i32,
    },
    /// A concurrent write moved the record's version between this write's read
    /// and its `CasVersion` swap (or created it between a not-found read and the
    /// `CreateIfAbsent`). The loser re-reads and merges onto the winner's body
    /// rather than silently overwriting it.
    #[error(
        "a concurrent write changed metadata record {key:?} since this one read it (CAS \
         precondition failed): re-read, re-merge, and retry rather than overwrite the other write"
    )]
    CasConflict { key: String },
}

impl MetricsMetaError {
    fn store(key: &str, source: StoreError) -> Self {
        MetricsMetaError::Store {
            key: key.to_string(),
            source,
        }
    }
}

/// The outcome of a [`merge_entries`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergeOutcome {
    /// The merged entry set, sorted by `family_name`. Deterministic: two
    /// processes merging the same inputs produce the same vector, so the record
    /// they encode is byte-identical and a redundant CAS write is recognizable
    /// as a no-op rather than a spurious change.
    pub merged: Vec<MetricMetadataEntry>,
    /// Whether any field of any entry actually differs from `existing`, or a new
    /// family was added. `false` means the merge is a no-op and the caller must
    /// skip its PUT: this is the common case right after a process restart, when
    /// every name looks new locally but the durable record already reflects it.
    pub changed: bool,
    /// How many distinct *new* family names were dropped because the merged set
    /// was already at `cap`. Counted, never silent: the sink increments
    /// `ingest_metadata_entries_dropped_total` by this.
    pub dropped_over_cap: usize,
}

/// Field-wise merge of `incoming` onto `existing` (ADR-0085 decision 1).
///
/// Pure and synchronous: no clock, no store, no allocation policy beyond the
/// result. `now_ns` is injected rather than read so the merge is deterministic
/// and testable.
///
/// The rules, each of which exists because a real pair of ingest paths would
/// otherwise clobber each other:
///
/// - An entry is keyed by `family_name`; the output is sorted by it.
/// - An incoming empty `help` or empty `unit` never overwrites a populated
///   stored value. RW1 supplying help without unit and OTLP later supplying unit
///   without help must compose, not alternate.
/// - An incoming [`MetricKind::Unknown`] never overwrites a known kind, for the
///   same reason: "no opinion" is not a correction.
/// - [`MergeOutcome::changed`] is true only if some field actually differs after
///   the merge (or a family was added). An incoming entry whose
///   `updated_unix_ns` differs but whose type/help/unit match is not a change:
///   the timestamp records when the fields changed, so treating it as a
///   difference would make every window write.
/// - `updated_unix_ns` is set to `now_ns` only on entries that changed.
/// - A *new* family name arriving when the merged set is already at `cap` is
///   dropped and counted in [`MergeOutcome::dropped_over_cap`] (each distinct
///   dropped name counted once, however many times it arrives). Existing names
///   are never dropped by a merge: shrinking a record is a deliberate pruning
///   write, not a side effect of one more name arriving. A record already above
///   `cap` therefore stays intact and simply accepts nothing new.
///
/// This function validates nothing: it merges what it is given, including an
/// entry with an empty name, so that the *writer* is the single place a corrupt
/// record is refused ([`write_metrics_meta`] rejects it before any PUT). If
/// `existing` itself carries a duplicate name (a hand-built vector; a stored
/// record cannot, decode refuses it) the last occurrence wins.
pub fn merge_entries(
    existing: &[MetricMetadataEntry],
    incoming: &[MetricMetadataEntry],
    now_ns: i64,
    cap: usize,
) -> MergeOutcome {
    let mut merged: BTreeMap<&str, MetricMetadataEntry> = BTreeMap::new();
    for entry in existing {
        merged.insert(entry.family_name.as_str(), entry.clone());
    }

    let mut changed = false;
    let mut dropped: BTreeSet<&str> = BTreeSet::new();

    for inc in incoming {
        if let Some(current) = merged.get_mut(inc.family_name.as_str()) {
            let kind = if inc.kind == MetricKind::Unknown {
                current.kind
            } else {
                inc.kind
            };
            let help = if inc.help.is_empty() {
                &current.help
            } else {
                &inc.help
            };
            let unit = if inc.unit.is_empty() {
                &current.unit
            } else {
                &inc.unit
            };
            if kind != current.kind || help != &current.help || unit != &current.unit {
                let (help, unit) = (help.clone(), unit.clone());
                current.kind = kind;
                current.help = help;
                current.unit = unit;
                current.updated_unix_ns = now_ns;
                changed = true;
            }
        } else if merged.len() < cap {
            merged.insert(
                inc.family_name.as_str(),
                MetricMetadataEntry {
                    family_name: inc.family_name.clone(),
                    kind: inc.kind,
                    help: inc.help.clone(),
                    unit: inc.unit.clone(),
                    updated_unix_ns: now_ns,
                },
            );
            changed = true;
        } else {
            dropped.insert(inc.family_name.as_str());
        }
    }

    MergeOutcome {
        merged: merged.into_values().collect(),
        changed,
        dropped_over_cap: dropped.len(),
    }
}

/// Build the proto record to persist for a tenant's metadata.
fn build_record(
    tenant_hash: &TenantHash,
    entries: &[MetricMetadataEntry],
) -> sysproto::MetricMetadataRecord {
    sysproto::MetricMetadataRecord {
        format_version: METRICS_META_FORMAT_VERSION,
        entries: entries
            .iter()
            .map(|e| sysproto::MetricMetadataEntry {
                family_name: e.family_name.clone(),
                r#type: e.kind.to_proto() as i32,
                help: e.help.clone(),
                unit: e.unit.clone(),
                updated_unix_ns: e.updated_unix_ns,
            })
            .collect(),
        tenant_hash: tenant_hash.0.to_vec(),
    }
}

/// Decode a proto record into domain entries, validating the version, the
/// misfile guard, and every entry's structure fail-closed. A record from a
/// future format, one misfiled under the wrong tenant's key, one with an empty
/// or duplicated family name, or one with an invalid type is refused rather than
/// served as this tenant's metadata.
fn decode_record(
    record: &sysproto::MetricMetadataRecord,
    key: &str,
    tenant_hash: &TenantHash,
) -> Result<Vec<MetricMetadataEntry>, MetricsMetaError> {
    if record.format_version > METRICS_META_FORMAT_VERSION {
        return Err(MetricsMetaError::UnsupportedVersion {
            key: key.to_string(),
            got: record.format_version,
        });
    }
    if record.tenant_hash.as_slice() != tenant_hash.0.as_slice() {
        return Err(MetricsMetaError::MisfiledTenant {
            key: key.to_string(),
            expected: tenant_hash.to_hex(),
            actual: hex::encode(&record.tenant_hash),
        });
    }
    let mut entries = Vec::with_capacity(record.entries.len());
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for e in &record.entries {
        if e.family_name.is_empty() {
            return Err(MetricsMetaError::Corrupt {
                key: key.to_string(),
                defect: MetricsMetaDefect::EmptyFamilyName,
            });
        }
        if !seen.insert(e.family_name.as_str()) {
            return Err(MetricsMetaError::Corrupt {
                key: key.to_string(),
                defect: MetricsMetaDefect::DuplicateFamilyName,
            });
        }
        let kind = MetricKind::from_proto_i32(e.r#type).ok_or_else(|| {
            MetricsMetaError::InvalidMetricKind {
                key: key.to_string(),
                family_name: e.family_name.clone(),
                got: e.r#type,
            }
        })?;
        entries.push(MetricMetadataEntry {
            family_name: e.family_name.clone(),
            kind,
            help: e.help.clone(),
            unit: e.unit.clone(),
            updated_unix_ns: e.updated_unix_ns,
        });
    }
    Ok(entries)
}

/// Encode the record body: prost, then zstd at the fixed [`ZSTD_LEVEL`].
fn encode_body(
    tenant_hash: &TenantHash,
    entries: &[MetricMetadataEntry],
    key: &str,
) -> Result<Vec<u8>, MetricsMetaError> {
    let raw = build_record(tenant_hash, entries).encode_to_vec();
    zstd::bulk::compress(&raw, ZSTD_LEVEL).map_err(|e| MetricsMetaError::Compress {
        key: key.to_string(),
        message: e.to_string(),
    })
}

/// Decompress a stored body, bounded by [`MAX_METRICS_META_DECOMPRESSED_BYTES`].
/// Streams into a growing buffer rather than pre-allocating the ceiling, and
/// refuses a body that would exceed it. A corrupt or truncated frame is a typed
/// [`MetricsMetaError::Decompress`], never a panic.
fn decompress_body(key: &str, body: &[u8]) -> Result<Vec<u8>, MetricsMetaError> {
    use std::io::Read;

    let mut decoder =
        zstd::stream::read::Decoder::new(body).map_err(|e| MetricsMetaError::Decompress {
            key: key.to_string(),
            message: e.to_string(),
        })?;
    let cap = MAX_METRICS_META_DECOMPRESSED_BYTES;
    let mut out = Vec::new();
    // cap + 1 so a body sitting exactly on the ceiling is accepted and the first
    // byte past it is observable as an overflow rather than a silent truncation.
    let read = decoder
        .by_ref()
        .take(cap as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| MetricsMetaError::Decompress {
            key: key.to_string(),
            message: e.to_string(),
        })?;
    if read > cap {
        return Err(MetricsMetaError::DecompressedTooLarge {
            key: key.to_string(),
            cap,
        });
    }
    Ok(out)
}

/// Decode a stored body into domain entries: decompress, prost-decode, validate.
fn decode_body(
    body: &[u8],
    key: &str,
    tenant_hash: &TenantHash,
) -> Result<Vec<MetricMetadataEntry>, MetricsMetaError> {
    let raw = decompress_body(key, body)?;
    let record = sysproto::MetricMetadataRecord::decode(raw.as_slice()).map_err(|source| {
        MetricsMetaError::Decode {
            key: key.to_string(),
            source,
        }
    })?;
    decode_record(&record, key, tenant_hash)
}

/// Refuse an entry set that would make a corrupt record before it is written. A
/// duplicate or empty family name is refused at the writer, so `decode` never has
/// to tolerate one this build produced.
fn validate_entries(entries: &[MetricMetadataEntry], key: &str) -> Result<(), MetricsMetaError> {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for e in entries {
        if e.family_name.is_empty() {
            return Err(MetricsMetaError::Corrupt {
                key: key.to_string(),
                defect: MetricsMetaDefect::EmptyFamilyName,
            });
        }
        if !seen.insert(e.family_name.as_str()) {
            return Err(MetricsMetaError::Corrupt {
                key: key.to_string(),
                defect: MetricsMetaDefect::DuplicateFamilyName,
            });
        }
    }
    Ok(())
}

/// Read a tenant's metric metadata record, returning the decoded entries and the
/// store version needed for a later `CasVersion` swap.
///
/// `Ok(None)` when no record exists: absence means "nothing has been captured
/// for this tenant yet", a valid state, not an error -- the read side serves an
/// empty metadata response for it, and the write side bootstraps with
/// `CreateIfAbsent`. A present record is fully version/misfile/entry-checked, so
/// a corrupt or misfiled record fails closed rather than being served as this
/// tenant's metadata.
pub async fn read_metrics_meta(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
) -> Result<Option<(Vec<MetricMetadataEntry>, Version)>, MetricsMetaError> {
    let key = metrics_meta_key(tenant_hash);
    match store.get(&key, GetRange::Full).await {
        Ok(outcome) => {
            let entries = decode_body(outcome.data.as_ref(), &key, tenant_hash)?;
            Ok(Some((entries, outcome.version)))
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(MetricsMetaError::store(&key, err)),
    }
}

/// Write a tenant's metric metadata record, whole-record CAS-replace (ADR-0085
/// decision 1). The only legal mutation of the record.
///
/// `expected` names the version this write was computed against:
///
/// - `None` -- the caller's read found no record, so this is the first write for
///   the tenant and goes out under `CreateIfAbsent`. A concurrent creator
///   winning that race surfaces as [`MetricsMetaError::CasConflict`], never a
///   silent overwrite of the record this call never saw.
/// - `Some(v)` -- the caller read version `v` and merged onto that body, so the
///   swap goes out under `PutMode::CasVersion(v)`. A concurrent write that moved
///   the version in between is a [`MetricsMetaError::CasConflict`]; the loser
///   re-reads, re-merges onto the winner's body, and retries.
///
/// Returns the new version, so a caller that writes twice in one window (it
/// should not; the ADR budgets at most one PUT per tenant per window) can chain
/// without a second read.
///
/// The entry set is validated before any PUT: an empty or duplicated
/// `family_name` is refused here rather than durably persisted for a later read
/// to refuse.
pub async fn write_metrics_meta(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
    entries: &[MetricMetadataEntry],
    expected: Option<Version>,
) -> Result<Version, MetricsMetaError> {
    let key = metrics_meta_key(tenant_hash);
    validate_entries(entries, &key)?;
    let body = encode_body(tenant_hash, entries, &key)?;
    let create = expected.is_none();
    let mode = match expected {
        None => PutMode::CreateIfAbsent,
        Some(version) => PutMode::CasVersion(version),
    };
    match store
        .put(
            &key,
            body.into(),
            PutOptions {
                mode,
                checksum: None,
            },
        )
        .await
    {
        Ok(outcome) => Ok(outcome.version),
        // Mode-aware mapping per the object-store contract: a refused
        // CreateIfAbsent is AlreadyExists, a refused CasVersion is
        // PreconditionFailed. Both mean the same thing to this caller -- someone
        // else moved the record -- so both become one typed conflict.
        Err(StoreError::AlreadyExists) if create => Err(MetricsMetaError::CasConflict { key }),
        Err(StoreError::PreconditionFailed) => Err(MetricsMetaError::CasConflict { key }),
        Err(err) => Err(MetricsMetaError::store(&key, err)),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use std::sync::Arc;

    fn tenant() -> TenantHash {
        TenantHash([0x11u8; 16])
    }

    fn mem() -> Arc<dyn ObjectStoreBackend> {
        Arc::new(MemoryStore::new())
    }

    fn entry(
        family_name: &str,
        kind: MetricKind,
        help: &str,
        unit: &str,
        updated_unix_ns: i64,
    ) -> MetricMetadataEntry {
        MetricMetadataEntry {
            family_name: family_name.to_string(),
            kind,
            help: help.to_string(),
            unit: unit.to_string(),
            updated_unix_ns,
        }
    }

    fn find<'a>(entries: &'a [MetricMetadataEntry], name: &str) -> Option<&'a MetricMetadataEntry> {
        entries.iter().find(|e| e.family_name == name)
    }

    #[test]
    fn key_is_under_the_metrics_signal_prefix() {
        assert_eq!(
            metrics_meta_key(&tenant()),
            format!("t/{}/m/meta", tenant().to_hex()),
            "the key sits under the tenant's metrics signal prefix, not a new top-level one"
        );
    }

    /// Every field of every entry survives write -> read unchanged, and the read
    /// hands back a version usable for a later CAS.
    #[tokio::test]
    async fn round_trip_preserves_every_field() {
        let store = mem();
        let entries = vec![
            entry(
                "http_requests_total",
                MetricKind::Counter,
                "requests handled",
                "",
                10,
            ),
            entry(
                "process_resident_memory_bytes",
                MetricKind::Gauge,
                "",
                "bytes",
                20,
            ),
            entry(
                "request_duration_seconds",
                MetricKind::Histogram,
                "latency",
                "seconds",
                30,
            ),
            entry("legacy_summary", MetricKind::Summary, "old", "1", 40),
            entry("untyped_thing", MetricKind::Unknown, "", "", 50),
        ];
        let version = write_metrics_meta(store.as_ref(), &tenant(), &entries, None)
            .await
            .expect("first write creates the record");

        let (read, read_version) = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect("read")
            .expect("record present");
        assert_eq!(read, entries, "every field round-trips unchanged");
        assert_eq!(
            read_version, version,
            "the read version matches the one the write returned"
        );
    }

    /// A tenant with no record reads as `Ok(None)`: nothing captured yet is a
    /// valid state, not an error.
    #[tokio::test]
    async fn read_absent_record_is_none_not_error() {
        let store = mem();
        let got = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect("an absent record is not an error");
        assert!(got.is_none(), "no metadata captured for this tenant yet");
    }

    /// `expected: None` on a tenant that already has a record is a losing
    /// first-write race: the store's `CreateIfAbsent` returns `AlreadyExists` and
    /// it surfaces as a typed conflict, never a silent overwrite.
    #[tokio::test]
    async fn create_if_absent_on_existing_record_is_cas_conflict() {
        let store = mem();
        write_metrics_meta(
            store.as_ref(),
            &tenant(),
            &[entry("a", MetricKind::Counter, "h", "", 1)],
            None,
        )
        .await
        .expect("create");

        let err = write_metrics_meta(
            store.as_ref(),
            &tenant(),
            &[entry("b", MetricKind::Gauge, "h", "", 2)],
            None,
        )
        .await
        .expect_err("a second CreateIfAbsent must be refused, not overwrite the first");
        assert!(
            matches!(err, MetricsMetaError::CasConflict { .. }),
            "got: {err}"
        );

        let (read, _v) = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(read.len(), 1);
        assert!(find(&read, "a").is_some(), "the winner's record is intact");
    }

    /// A CAS write against a version that has since moved is refused as a typed
    /// conflict (`PreconditionFailed` mapped).
    #[tokio::test]
    async fn stale_cas_version_is_cas_conflict() {
        let store = mem();
        let v0 = write_metrics_meta(
            store.as_ref(),
            &tenant(),
            &[entry("a", MetricKind::Counter, "h", "", 1)],
            None,
        )
        .await
        .expect("create");
        write_metrics_meta(
            store.as_ref(),
            &tenant(),
            &[entry("a", MetricKind::Counter, "h2", "", 2)],
            Some(v0.clone()),
        )
        .await
        .expect("first CAS wins");

        let err = write_metrics_meta(
            store.as_ref(),
            &tenant(),
            &[entry("a", MetricKind::Counter, "h3", "", 3)],
            Some(v0),
        )
        .await
        .expect_err("a stale version must be refused");
        assert!(
            matches!(err, MetricsMetaError::CasConflict { .. }),
            "got: {err}"
        );
    }

    /// The same conflict, proven with an injected `FailedConditionalWrite` rather
    /// than a natural version race, with the fault counter asserted so the test
    /// cannot pass without the fault firing.
    #[tokio::test]
    async fn injected_conditional_write_failure_is_cas_conflict() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("/m/meta"),
        );
        let store = FaultStore::new(inner, plan);

        let err = write_metrics_meta(
            &store,
            &tenant(),
            &[entry("a", MetricKind::Counter, "h", "", 1)],
            None,
        )
        .await
        .expect_err("a refused conditional write must surface as a typed conflict");
        assert!(
            matches!(err, MetricsMetaError::CasConflict { .. }),
            "got: {err}"
        );
        assert_eq!(
            store.fault_count(
                Op::Put,
                ravel_object_store::fault::FaultKind::FailedConditionalWrite
            ),
            1,
            "the injected precondition failure must have fired"
        );
    }

    /// The acceptance test for ADR-0085 decision 1's concurrent-writer rule.
    ///
    /// Two ingest processes read the same version `v0`. A wins the CAS with its
    /// entry X. B's CAS against `v0` is refused (asserted as a typed
    /// `CasConflict`, so the test cannot pass if the conflict never fires); B
    /// then re-reads A's record, merges its own entry Y **onto that body**, and
    /// writes `v2`. The final record must hold both entries with A's fields
    /// intact.
    ///
    /// What fails against an overwrite implementation (B ignoring the conflict
    /// and PUTting its merge of `v0 + Y`): the
    /// `find(&final_entries, "a_family").expect("A's entry X survives B's
    /// second write")` lookup fails outright, because an overwritten record
    /// holds only the seed entry and Y. The `final_entries.len() == 3` assertion
    /// fails the same way (it would be 2). Neither can pass on a lost update,
    /// which is exactly the failure this test exists to catch.
    #[tokio::test]
    async fn concurrent_writers_converge_under_cas_conflict() {
        let store = mem();

        // A record already exists, so both writers take the CasVersion path.
        let seed = vec![entry(
            "seed_family",
            MetricKind::Gauge,
            "seeded",
            "bytes",
            100,
        )];
        write_metrics_meta(store.as_ref(), &tenant(), &seed, None)
            .await
            .expect("seed the record");

        // Both writers read the same version.
        let (a_base, v0_a) = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect("A reads")
            .expect("present");
        let (b_base, v0_b) = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect("B reads")
            .expect("present");
        assert_eq!(v0_a, v0_b, "both writers observed the same version");

        // A merges its entry X and wins the CAS.
        let x = entry(
            "a_family",
            MetricKind::Counter,
            "A's help",
            "seconds",
            0, // merge stamps now_ns; the incoming timestamp is ignored
        );
        let a_merge = merge_entries(&a_base, &[x], 200, DEFAULT_METRICS_META_ENTRY_CAP);
        assert!(a_merge.changed, "a new family is a change");
        write_metrics_meta(store.as_ref(), &tenant(), &a_merge.merged, Some(v0_a))
            .await
            .expect("A's CAS wins");

        // B merges its entry Y onto the body it read at v0 and loses the CAS.
        let y = entry("b_family", MetricKind::Gauge, "B's help", "bytes", 0);
        let b_merge = merge_entries(
            &b_base,
            std::slice::from_ref(&y),
            300,
            DEFAULT_METRICS_META_ENTRY_CAP,
        );
        let err = write_metrics_meta(store.as_ref(), &tenant(), &b_merge.merged, Some(v0_b))
            .await
            .expect_err("B's CAS against the superseded version must be refused");
        assert!(
            matches!(err, MetricsMetaError::CasConflict { .. }),
            "B must lose with a typed conflict, got: {err}"
        );

        // B re-reads the winner's body, re-merges onto it, and writes v2.
        let (b_reread, v1) = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect("B re-reads")
            .expect("present");
        assert!(
            find(&b_reread, "a_family").is_some(),
            "B's re-read observes A's entry"
        );
        let b_retry = merge_entries(
            &b_reread,
            std::slice::from_ref(&y),
            300,
            DEFAULT_METRICS_META_ENTRY_CAP,
        );
        write_metrics_meta(store.as_ref(), &tenant(), &b_retry.merged, Some(v1))
            .await
            .expect("B's retry against the fresh version wins");

        // Both entries survive, with A's fields untouched by B's write.
        let (final_entries, _v2) = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect("final read")
            .expect("present");
        assert_eq!(
            final_entries.len(),
            3,
            "seed + A's X + B's Y; a lost update would leave 2"
        );
        let a_entry = find(&final_entries, "a_family").expect("A's entry X survives B's write");
        assert_eq!(a_entry.kind, MetricKind::Counter, "A's type intact");
        assert_eq!(a_entry.help, "A's help", "A's help intact");
        assert_eq!(a_entry.unit, "seconds", "A's unit intact");
        assert_eq!(
            a_entry.updated_unix_ns, 200,
            "A's timestamp intact: B's merge did not restamp an entry it did not change"
        );
        let b_entry = find(&final_entries, "b_family").expect("B's entry Y landed");
        assert_eq!(b_entry.help, "B's help");
        assert_eq!(b_entry.updated_unix_ns, 300);
        assert!(
            find(&final_entries, "seed_family").is_some(),
            "the seed entry is untouched by both"
        );
    }

    /// An incoming empty `help` or `unit` never clobbers a populated stored
    /// value: RW1 supplying help without unit and OTLP later supplying unit
    /// without help must compose.
    #[test]
    fn empty_incoming_fields_do_not_clobber() {
        let existing = vec![entry("m", MetricKind::Counter, "the help", "seconds", 1)];
        let incoming = vec![entry("m", MetricKind::Counter, "", "", 999)];
        let out = merge_entries(&existing, &incoming, 500, 100);
        assert!(!out.changed, "an all-empty incoming entry changes nothing");
        assert_eq!(out.merged[0].help, "the help");
        assert_eq!(out.merged[0].unit, "seconds");
        assert_eq!(
            out.merged[0].updated_unix_ns, 1,
            "an unchanged entry keeps its original timestamp"
        );
    }

    /// A populated incoming field does overwrite, and only the fields that
    /// actually differ drive `changed`.
    #[test]
    fn populated_incoming_fields_overwrite_and_restamp() {
        let existing = vec![entry("m", MetricKind::Counter, "old help", "", 1)];
        let incoming = vec![entry("m", MetricKind::Counter, "new help", "bytes", 0)];
        let out = merge_entries(&existing, &incoming, 500, 100);
        assert!(out.changed);
        assert_eq!(out.merged[0].help, "new help");
        assert_eq!(out.merged[0].unit, "bytes");
        assert_eq!(out.merged[0].updated_unix_ns, 500, "restamped on change");
    }

    /// An incoming `Unknown` kind is "no opinion", not a correction: it never
    /// overwrites a known kind. The reverse (a known kind arriving over an
    /// `Unknown`) does overwrite.
    #[test]
    fn unknown_kind_does_not_clobber_known_kind() {
        let existing = vec![entry("m", MetricKind::Counter, "h", "", 1)];
        let incoming = vec![entry("m", MetricKind::Unknown, "", "", 0)];
        let out = merge_entries(&existing, &incoming, 500, 100);
        assert!(!out.changed, "Unknown over Counter is not a change");
        assert_eq!(out.merged[0].kind, MetricKind::Counter);

        let existing = vec![entry("m", MetricKind::Unknown, "h", "", 1)];
        let incoming = vec![entry("m", MetricKind::Counter, "", "", 0)];
        let out = merge_entries(&existing, &incoming, 500, 100);
        assert!(out.changed, "a known kind over Unknown is a real change");
        assert_eq!(out.merged[0].kind, MetricKind::Counter);
        assert_eq!(out.merged[0].updated_unix_ns, 500);
    }

    /// A differing `updated_unix_ns` alone is not a change: the timestamp records
    /// when the fields changed, so treating it as a difference would make every
    /// debounce window write.
    #[test]
    fn identical_re_merge_reports_no_change() {
        let existing = vec![
            entry("a", MetricKind::Counter, "ha", "seconds", 1),
            entry("b", MetricKind::Gauge, "hb", "bytes", 2),
        ];
        let out = merge_entries(&existing, &existing, 900, 100);
        assert!(!out.changed, "re-merging identical entries changes nothing");
        assert_eq!(out.merged, existing, "including the timestamps");
        assert_eq!(out.dropped_over_cap, 0);

        // Same, when the incoming entries differ only in their timestamps.
        let incoming = vec![
            entry("a", MetricKind::Counter, "ha", "seconds", 77),
            entry("b", MetricKind::Gauge, "hb", "bytes", 88),
        ];
        let out = merge_entries(&existing, &incoming, 900, 100);
        assert!(!out.changed, "a timestamp-only difference is not a change");
        assert_eq!(out.merged, existing);
    }

    /// A new family arriving at the cap is dropped and counted; existing names
    /// are never dropped, and an existing name at the cap still merges its
    /// fields.
    #[test]
    fn new_family_over_cap_is_dropped_and_counted() {
        let existing = vec![
            entry("a", MetricKind::Counter, "ha", "", 1),
            entry("b", MetricKind::Gauge, "hb", "", 2),
        ];
        let incoming = vec![
            entry("c", MetricKind::Counter, "hc", "", 0),
            entry("d", MetricKind::Counter, "hd", "", 0),
            // the same over-cap name arriving twice counts once
            entry("c", MetricKind::Counter, "hc", "", 0),
            // an existing name still merges even at the cap
            entry("a", MetricKind::Counter, "ha updated", "", 0),
        ];
        let out = merge_entries(&existing, &incoming, 700, 2);
        assert_eq!(
            out.dropped_over_cap, 2,
            "c and d dropped, c counted once despite arriving twice"
        );
        assert_eq!(out.merged.len(), 2, "existing names are never dropped");
        assert_eq!(
            out.merged
                .iter()
                .map(|e| e.family_name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert!(out.changed, "the in-cap field update is still a change");
        assert_eq!(out.merged[0].help, "ha updated");
    }

    /// A record already above the cap keeps every existing entry and simply
    /// accepts nothing new: shrinking is a deliberate pruning write, never a side
    /// effect of one more name arriving.
    #[test]
    fn record_already_over_cap_is_not_shrunk_by_merge() {
        let existing = vec![
            entry("a", MetricKind::Counter, "", "", 1),
            entry("b", MetricKind::Counter, "", "", 2),
            entry("c", MetricKind::Counter, "", "", 3),
        ];
        let out = merge_entries(&existing, &[entry("d", MetricKind::Gauge, "", "", 0)], 1, 2);
        assert_eq!(out.merged.len(), 3, "nothing existing is evicted");
        assert_eq!(out.dropped_over_cap, 1);
        assert!(!out.changed);
    }

    /// Output order is sorted by family name regardless of input order, so two
    /// processes computing the same merge encode the same record.
    #[test]
    fn output_is_sorted_by_family_name() {
        let existing = vec![
            entry("zeta", MetricKind::Gauge, "", "", 1),
            entry("alpha", MetricKind::Gauge, "", "", 2),
        ];
        let incoming = vec![
            entry("mike", MetricKind::Gauge, "", "", 0),
            entry("bravo", MetricKind::Gauge, "", "", 0),
        ];
        let out = merge_entries(&existing, &incoming, 5, 100);
        assert_eq!(
            out.merged
                .iter()
                .map(|e| e.family_name.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "bravo", "mike", "zeta"]
        );

        // The same inputs in a different order produce the identical vector.
        let reordered_existing = vec![existing[1].clone(), existing[0].clone()];
        let reordered_incoming = vec![incoming[1].clone(), incoming[0].clone()];
        let out2 = merge_entries(&reordered_existing, &reordered_incoming, 5, 100);
        assert_eq!(out.merged, out2.merged, "order-independent, byte-stable");
    }

    /// A record declaring a future format_version is refused rather than misread
    /// under this layout.
    #[tokio::test]
    async fn read_rejects_future_format_version() {
        let store = mem();
        let mut record = build_record(&tenant(), &[entry("a", MetricKind::Counter, "h", "", 1)]);
        record.format_version = METRICS_META_FORMAT_VERSION + 1;
        let body = zstd::bulk::compress(&record.encode_to_vec(), ZSTD_LEVEL).expect("compress");
        store
            .put(
                &metrics_meta_key(&tenant()),
                body.into(),
                PutOptions::default(),
            )
            .await
            .expect("seed future record");
        let err = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect_err("a future format_version must be refused");
        assert!(
            matches!(
                err,
                MetricsMetaError::UnsupportedVersion { got, .. } if got == METRICS_META_FORMAT_VERSION + 1
            ),
            "got: {err}"
        );
    }

    /// A truncated zstd frame is a typed error, never a panic.
    #[tokio::test]
    async fn read_rejects_truncated_body() {
        let store = mem();
        let record = build_record(&tenant(), &[entry("a", MetricKind::Counter, "h", "", 1)]);
        let full = zstd::bulk::compress(&record.encode_to_vec(), ZSTD_LEVEL).expect("compress");
        let truncated = full[..full.len() / 2].to_vec();
        store
            .put(
                &metrics_meta_key(&tenant()),
                truncated.into(),
                PutOptions::default(),
            )
            .await
            .expect("seed truncated body");
        let err = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect_err("a truncated frame must be a typed error");
        assert!(
            matches!(err, MetricsMetaError::Decompress { .. }),
            "got: {err}"
        );
    }

    /// Garbage that is not a zstd frame at all is also a typed error.
    #[tokio::test]
    async fn read_rejects_garbage_body() {
        let store = mem();
        store
            .put(
                &metrics_meta_key(&tenant()),
                vec![0xFFu8; 64].into(),
                PutOptions::default(),
            )
            .await
            .expect("seed garbage");
        let err = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect_err("garbage must be a typed error, not a panic");
        assert!(
            matches!(err, MetricsMetaError::Decompress { .. }),
            "got: {err}"
        );
    }

    /// A well-framed zstd body whose plaintext is not a valid `MetricMetadataRecord`
    /// is a typed decode error.
    #[tokio::test]
    async fn read_rejects_undecodable_proto() {
        let store = mem();
        let body = zstd::bulk::compress(&[0xFFu8, 0xFF, 0xFF, 0x07], ZSTD_LEVEL).expect("compress");
        store
            .put(
                &metrics_meta_key(&tenant()),
                body.into(),
                PutOptions::default(),
            )
            .await
            .expect("seed undecodable proto");
        let err = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect_err("an undecodable proto must be a typed decode error");
        assert!(matches!(err, MetricsMetaError::Decode { .. }), "got: {err}");
    }

    /// A record written for another tenant but stored under this tenant's key is
    /// refused: serving it would hand one tenant's metric names to another.
    #[tokio::test]
    async fn read_rejects_misfiled_tenant() {
        let store = mem();
        let other = TenantHash([0xEE; 16]);
        let record = build_record(&other, &[entry("a", MetricKind::Counter, "h", "", 1)]);
        let body = zstd::bulk::compress(&record.encode_to_vec(), ZSTD_LEVEL).expect("compress");
        store
            .put(
                &metrics_meta_key(&tenant()),
                body.into(),
                PutOptions::default(),
            )
            .await
            .expect("seed misfiled record");
        let err = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect_err("another tenant's record must be refused");
        assert!(
            matches!(err, MetricsMetaError::MisfiledTenant { .. }),
            "got: {err}"
        );
    }

    /// The `UNSPECIFIED` type sentinel is corruption, never defaulted to a kind.
    #[tokio::test]
    async fn read_rejects_unspecified_metric_type() {
        let store = mem();
        let mut record = build_record(&tenant(), &[entry("a", MetricKind::Counter, "h", "", 1)]);
        record.entries[0].r#type = sysproto::MetricMetadataType::Unspecified as i32;
        let body = zstd::bulk::compress(&record.encode_to_vec(), ZSTD_LEVEL).expect("compress");
        store
            .put(
                &metrics_meta_key(&tenant()),
                body.into(),
                PutOptions::default(),
            )
            .await
            .expect("seed record with unspecified type");
        let err = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect_err("an unspecified type must be refused");
        assert!(
            matches!(err, MetricsMetaError::InvalidMetricKind { got: 0, .. }),
            "got: {err}"
        );
    }

    /// A duplicated family name in a stored record makes a lookup ambiguous and is
    /// refused on read.
    #[tokio::test]
    async fn read_rejects_duplicate_family_name() {
        let store = mem();
        let mut record = build_record(&tenant(), &[entry("a", MetricKind::Counter, "h", "", 1)]);
        let dup = record.entries[0].clone();
        record.entries.push(dup);
        let body = zstd::bulk::compress(&record.encode_to_vec(), ZSTD_LEVEL).expect("compress");
        store
            .put(
                &metrics_meta_key(&tenant()),
                body.into(),
                PutOptions::default(),
            )
            .await
            .expect("seed duplicate-name record");
        let err = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect_err("a duplicate family_name must be refused");
        assert!(
            matches!(
                err,
                MetricsMetaError::Corrupt {
                    defect: MetricsMetaDefect::DuplicateFamilyName,
                    ..
                }
            ),
            "got: {err}"
        );
    }

    /// A corrupt entry set is refused before any PUT, so a bad record is never
    /// durably written for a later read to refuse.
    #[tokio::test]
    async fn write_refuses_corrupt_entries_before_any_put() {
        let store = mem();
        let err = write_metrics_meta(
            store.as_ref(),
            &tenant(),
            &[entry("", MetricKind::Counter, "h", "", 1)],
            None,
        )
        .await
        .expect_err("an empty family_name must be refused");
        assert!(
            matches!(
                err,
                MetricsMetaError::Corrupt {
                    defect: MetricsMetaDefect::EmptyFamilyName,
                    ..
                }
            ),
            "got: {err}"
        );

        let err = write_metrics_meta(
            store.as_ref(),
            &tenant(),
            &[
                entry("a", MetricKind::Counter, "h", "", 1),
                entry("a", MetricKind::Gauge, "h2", "", 2),
            ],
            None,
        )
        .await
        .expect_err("a duplicate family_name must be refused");
        assert!(
            matches!(
                err,
                MetricsMetaError::Corrupt {
                    defect: MetricsMetaDefect::DuplicateFamilyName,
                    ..
                }
            ),
            "got: {err}"
        );

        assert!(
            read_metrics_meta(store.as_ref(), &tenant())
                .await
                .expect("read")
                .is_none(),
            "neither refused write reached the store"
        );
    }

    /// An empty entry set is a legitimate record (a tenant whose every family was
    /// pruned), distinct from an absent one.
    #[tokio::test]
    async fn empty_entry_set_round_trips_and_is_distinct_from_absent() {
        let store = mem();
        write_metrics_meta(store.as_ref(), &tenant(), &[], None)
            .await
            .expect("write empty record");
        let (read, _v) = read_metrics_meta(store.as_ref(), &tenant())
            .await
            .expect("read")
            .expect("present, not absent");
        assert!(read.is_empty());
    }

    fn kind_strategy() -> impl Strategy<Value = MetricKind> {
        prop_oneof![
            Just(MetricKind::Counter),
            Just(MetricKind::Gauge),
            Just(MetricKind::Histogram),
            Just(MetricKind::Summary),
            Just(MetricKind::Unknown),
        ]
    }

    fn entry_strategy() -> impl Strategy<Value = MetricMetadataEntry> {
        (
            "[a-z_]{1,12}",
            kind_strategy(),
            "[ -~]{0,24}",
            "[a-z]{0,8}",
            any::<i64>(),
        )
            .prop_map(|(family_name, kind, help, unit, updated_unix_ns)| {
                MetricMetadataEntry {
                    family_name,
                    kind,
                    help,
                    unit,
                    updated_unix_ns,
                }
            })
    }

    /// A vector of entries with distinct, non-empty family names: the shape a
    /// valid record holds.
    fn entries_strategy(max: usize) -> impl Strategy<Value = Vec<MetricMetadataEntry>> {
        proptest::collection::vec(entry_strategy(), 0..max).prop_map(|mut v| {
            let mut seen = std::collections::HashSet::new();
            v.retain(|e| seen.insert(e.family_name.clone()));
            v
        })
    }

    proptest! {
        /// encode -> decode is the identity on any valid entry set, over arbitrary
        /// names, kinds, help/unit strings (including empty), and timestamps.
        #[test]
        fn encode_decode_round_trip_is_identity(entries in entries_strategy(24)) {
            let key = metrics_meta_key(&tenant());
            let body = encode_body(&tenant(), &entries, &key).expect("encode");
            let decoded = decode_body(&body, &key, &tenant()).expect("decode");
            prop_assert_eq!(decoded, entries);
        }

        /// The merge is idempotent: applying the same incoming set to its own
        /// result changes nothing further. Without this, a sink that re-merges an
        /// unflushed pending set every window would rewrite the record forever.
        #[test]
        fn merge_is_idempotent(
            existing in entries_strategy(16),
            incoming in entries_strategy(16),
            cap in 0usize..20,
        ) {
            let first = merge_entries(&existing, &incoming, 1_000, cap);
            let second = merge_entries(&first.merged, &incoming, 1_000, cap);
            prop_assert_eq!(&second.merged, &first.merged);
            prop_assert_eq!(second.dropped_over_cap, first.dropped_over_cap);
            prop_assert!(!second.changed, "a re-merge of the same input is a no-op");
        }

        /// Merge output is always sorted by family name and free of duplicates,
        /// so it is always a valid record body.
        #[test]
        fn merge_output_is_sorted_and_unique(
            existing in entries_strategy(16),
            incoming in entries_strategy(16),
        ) {
            let out = merge_entries(&existing, &incoming, 1_000, 100);
            let names: Vec<&str> = out.merged.iter().map(|e| e.family_name.as_str()).collect();
            let mut sorted = names.clone();
            sorted.sort_unstable();
            sorted.dedup();
            prop_assert_eq!(&names, &sorted);
        }
    }
}
