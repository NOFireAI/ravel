//! `compat init` (ADR-4233 decision 1): adopt an existing store into the
//! retained-data compatibility contract.
//!
//! [`init_compat`] runs a census over every tenant and signal and, only when
//! every object it finds is inside this build's read set, creates `sys/compat`
//! at the baseline versions with `compat_epoch = 0` and the version histogram
//! the census found. The census is strict: a listing failure, a key the store
//! cannot address, an unreadable object or a version outside the read set
//! refuses with a typed error and writes nothing.
//!
//! What the census checks, per tenant (one listing of `t/<h>/`):
//!
//! - `config`: the tenant config's `format_version`;
//! - `<s>/prov`: the provisioning record's `format_version`, and that it
//!   carries no `compat_epoch` (a fenced bucket means `sys/compat` existed and
//!   was lost; it is restored from bucket versioning, never re-created);
//! - `catalog/<s>/HEAD` and every `.csnap`, `.npost`, `.cstat` and `.kidx`
//!   object under `catalog/<s>/`: each object's own version, through the
//!   catalog's decoders;
//! - `<s>/c/...`: every commit, compaction, rewrite and tombstone record,
//!   decoded and validated, plus the data-format stamp each one carries;
//! - `<s>/l0/...` and `<s>/l1/...`: each data object's trailer version (and,
//!   for RLOG and RSPAN, the footer the trailer names).
//!
//! Every other key under the tenant is outside the census and counted as
//! ignored.

use std::collections::BTreeMap;

use prost::Message;
use ravel_catalog::compat::{
    FAMILY_CATALOG_HEAD, FAMILY_COMMIT_RECORD, FAMILY_COMPACTION_RECORD, FAMILY_CSNAP,
    FAMILY_CSTAT, FAMILY_KIDX, FAMILY_NPOST, FAMILY_RETENTION_TOMBSTONE, FAMILY_REWRITE_RECORD,
    FAMILY_RLOG, FAMILY_RSEG, FAMILY_RSPAN, KNOWN_FAMILIES,
};
use ravel_catalog::{
    COMPAT_KEY, CompatClock, CompatError, CompatKeys, HEAD_FORMAT_VERSIONS,
    KEY_INDEX_LEAF_FORMAT_VERSION, KEY_INDEX_LEAF_FORMAT_VERSIONS_KNOWN, KeyIndexLeafRejection,
    PROVISIONING_MAX_READ_VERSION, PROVISIONING_MIN_READ_VERSION, PartLimits, PostingsLimits,
    SnapshotFormatError, TENANT_CONFIG_MAX_READ_VERSION, TENANT_CONFIG_MIN_READ_VERSION,
    decode_column_stats_header, decode_head, decode_part, decode_postings, fold_write_version,
    key_index_leaf_format_version, key_index_leaf_versions_for_signal,
};
use ravel_commit::erasure;
use ravel_commit::keys::{BucketEntry, partition_bucket_entry};
use ravel_commit::record;
use ravel_object_store::{
    GetOutcome, GetRange, ObjectStoreBackend, StoreError, Unaddressable, list_all_reporting,
};
use ravel_proto::sys::v1 as sysproto;
use ravel_segment::{TRAILER_LEN, TrailerClass, classify_trailer};
use ravel_types::{Signal, TenantHash};

use crate::compat_policy::{FamilyVersions, part_family};

/// Every signal, in the fixed order the census and the fence walk visit them.
pub const SIGNALS: [Signal; 6] = [
    Signal::Metrics,
    Signal::Logs,
    Signal::Spans,
    Signal::Profiles,
    Signal::Alerts,
    Signal::Audit,
];

const _: () = assert!(
    ravel_logseg::footer::TRAILER_LEN as u64 == TRAILER_LEN
        && ravel_rspan::footer::TRAILER_LEN as u64 == TRAILER_LEN
);

/// Byte offset of the `u16` version inside the 16-byte RLOG and RSPAN trailer
/// (`footer_len` 4, `footer_crc32c` 4), docs/log-segment-format.md and
/// docs/span-segment-format.md.
const TRAILER_VERSION_OFFSET: usize = 8;

/// Label for a tenant config finding; not a compat family.
pub const WHAT_TENANT_CONFIG: &str = "tenant-config";
/// Label for a provisioning record finding; not a compat family.
pub const WHAT_PROVISIONING: &str = "provisioning";

/// The signal whose key prefix is `prefix`.
pub fn signal_for_prefix(prefix: &str) -> Option<Signal> {
    SIGNALS.into_iter().find(|s| s.key_prefix() == prefix)
}

/// A strict listing could not account for every key under its prefix.
#[derive(Debug, thiserror::Error)]
pub enum StrictListError {
    #[error("{prefix}: listing failed: {source}")]
    Store {
        prefix: String,
        #[source]
        source: StoreError,
    },
    #[error(
        "{prefix}: {count} key(s) the store cannot address, first {key:?}; refusing to skip them"
    )]
    Unaddressable {
        prefix: String,
        key: String,
        count: u64,
    },
    #[error("{prefix}: {name:?} is not a tenant directory")]
    NotATenant { prefix: String, name: String },
}

fn refuse_unaddressable(
    prefix: &str,
    unaddressable: &Unaddressable,
) -> Result<(), StrictListError> {
    if unaddressable.count == 0 {
        return Ok(());
    }
    Err(StrictListError::Unaddressable {
        prefix: prefix.to_string(),
        key: unaddressable
            .sample
            .first()
            .map(|k| k.key.clone())
            .unwrap_or_default(),
        count: unaddressable.count,
    })
}

/// Every tenant under `t/`, sorted, from one delimited listing that refuses an
/// unaddressable key or prefix and any directory that is not a tenant hash.
pub async fn list_tenants_strict(
    store: &dyn ObjectStoreBackend,
) -> Result<Vec<TenantHash>, StrictListError> {
    const PREFIX: &str = "t/";
    let listing = store
        .list_delimited(PREFIX)
        .await
        .map_err(|source| StrictListError::Store {
            prefix: PREFIX.to_string(),
            source,
        })?;
    if let Some(key) = listing.unaddressable.first() {
        return Err(StrictListError::Unaddressable {
            prefix: PREFIX.to_string(),
            key: key.key.clone(),
            count: listing.unaddressable.len() as u64,
        });
    }
    if let Some(prefix) = listing.unaddressable_prefixes.first() {
        return Err(StrictListError::Unaddressable {
            prefix: PREFIX.to_string(),
            key: prefix.clone(),
            count: listing.unaddressable_prefixes.len() as u64,
        });
    }
    if let Some(object) = listing.objects.first() {
        return Err(StrictListError::NotATenant {
            prefix: PREFIX.to_string(),
            name: object.key.clone(),
        });
    }
    let mut tenants = Vec::with_capacity(listing.common_prefixes.len());
    for prefix in &listing.common_prefixes {
        let tenant = prefix
            .strip_prefix(PREFIX)
            .and_then(|rest| rest.strip_suffix('/'))
            .filter(|hex| {
                hex.len() == 32
                    && hex
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
            .and_then(|hex| TenantHash::from_hex(hex).ok());
        let Some(tenant) = tenant else {
            return Err(StrictListError::NotATenant {
                prefix: PREFIX.to_string(),
                name: prefix.clone(),
            });
        };
        tenants.push(tenant);
    }
    tenants.sort();
    tenants.dedup();
    Ok(tenants)
}

/// What `init` records beside the versions, all from configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InitConfig {
    /// The release the store is adopted at (expected v0.34.0).
    pub baseline_release_major: u32,
    pub baseline_release_minor: u32,
    /// `P`, the longest a publish may run (ADR-4233 decision 3). Positive.
    pub max_publish_lifetime_ns: i64,
}

/// Objects found at a version outside this build's read set, grouped by
/// (tenant, signal, family, version).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutsideReadSet {
    pub tenant: TenantHash,
    /// `None` for the tenant config.
    pub signal: Option<Signal>,
    /// A compat family, or [`WHAT_TENANT_CONFIG`] / [`WHAT_PROVISIONING`].
    pub family: String,
    pub version: u32,
    pub count: u64,
    /// The first key found in the group.
    pub sample_key: String,
}

/// What the census found on a clean store.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CensusSummary {
    pub tenants: u64,
    /// Objects whose version the census checked.
    pub objects_checked: u64,
    /// Keys under a tenant that no census rule covers.
    pub objects_ignored: u64,
    /// Objects listed and then gone before their GET.
    pub objects_vanished: u64,
    /// Object count per (family, version), the `adopted` histogram.
    pub adopted: BTreeMap<(String, u32), u64>,
}

/// Whether [`init_compat`] wrote `sys/compat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InitOutcome {
    /// The census was clean and `sys/compat` was created.
    Created,
    /// `sys/compat` already held the record `init` would write; nothing was
    /// written and no census ran.
    AlreadyInitialized,
}

/// The result of a successful [`init_compat`].
#[derive(Debug, Clone, PartialEq)]
pub struct InitReport {
    pub outcome: InitOutcome,
    /// The record now in `sys/compat`.
    pub record: sysproto::CompatRecord,
    /// `None` when the record already existed.
    pub census: Option<CensusSummary>,
    pub finished_unix_ns: i64,
}

/// Why `init` refused. Every refusal writes nothing.
#[derive(Debug, thiserror::Error)]
pub enum CompatInitError {
    #[error(
        "no file keys configured: {COMPAT_KEY} is MACed under the file keys, so an unkeyed store cannot be initialized"
    )]
    Unkeyed,
    #[error("max_publish_lifetime_ns must be positive, got {got}")]
    InvalidPublishLifetime { got: i64 },
    #[error("this build has no single baseline version for family {family}")]
    NoBaselineVersion { family: String },
    #[error(transparent)]
    List(#[from] StrictListError),
    #[error("{key}: GET failed: {source}")]
    Get {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("{key}: {what} object this build cannot read: {detail}")]
    Unreadable {
        key: String,
        what: String,
        detail: String,
    },
    #[error("{key}: no bucket entry shape matches this key")]
    UnknownBucketEntry { key: String },
    #[error("{key}: no format family covers signal {signal:?}'s data")]
    NoFamilyForSignal { key: String, signal: Signal },
    #[error(
        "{key}: provisioning record carries compat_epoch {compat_epoch}; {COMPAT_KEY} existed and is lost, restore it from bucket versioning instead of re-creating it"
    )]
    ProvisioningFenced { key: String, compat_epoch: u64 },
    #[error("{} group(s) of objects outside this build's read set, first {:?}", .findings.len(), .findings.first())]
    OutsideReadSet { findings: Vec<OutsideReadSet> },
    #[error("{COMPAT_KEY} already exists and differs from the record init would write")]
    RecordDiffers {
        existing: Box<sysproto::CompatRecord>,
        wanted: Box<sysproto::CompatRecord>,
    },
    #[error(transparent)]
    Compat(#[from] CompatError),
}

/// Adopt the store: refuse an unkeyed store, return the existing record when
/// it is the one `init` would write (and refuse when it differs), otherwise
/// run the census and create `sys/compat` from it.
pub async fn init_compat(
    store: &dyn ObjectStoreBackend,
    keys: Option<&CompatKeys>,
    clock: &dyn CompatClock,
    config: &InitConfig,
) -> Result<InitReport, CompatInitError> {
    let keys = keys.ok_or(CompatInitError::Unkeyed)?;
    if config.max_publish_lifetime_ns <= 0 {
        return Err(CompatInitError::InvalidPublishLifetime {
            got: config.max_publish_lifetime_ns,
        });
    }
    let wanted = baseline_record(config, &BTreeMap::new())?;
    if let Some((existing, _)) = ravel_catalog::compat::read_compat(store, keys).await? {
        return already_initialized(existing, wanted, clock);
    }

    let census = run_census(store).await?;
    let record = baseline_record(config, &census.adopted)?;
    match ravel_catalog::compat::create_compat(store, keys, record).await {
        Ok((record, _)) => Ok(InitReport {
            outcome: InitOutcome::Created,
            record,
            census: Some(census),
            finished_unix_ns: clock.now_unix_ns(),
        }),
        Err(CompatError::AlreadyExists { .. }) => {
            let Some((existing, _)) = ravel_catalog::compat::read_compat(store, keys).await? else {
                return Err(CompatError::CasConflict {
                    key: COMPAT_KEY.to_string(),
                }
                .into());
            };
            already_initialized(existing, wanted, clock)
        }
        Err(err) => Err(err.into()),
    }
}

/// `adopted` is what one census observed, not a decision: a second `init`
/// over a store that has grown since compares equal.
fn already_initialized(
    existing: sysproto::CompatRecord,
    wanted: sysproto::CompatRecord,
    clock: &dyn CompatClock,
) -> Result<InitReport, CompatInitError> {
    let decided = |record: &sysproto::CompatRecord| sysproto::CompatRecord {
        format_version: 0,
        adopted: Vec::new(),
        mac: Vec::new(),
        ..record.clone()
    };
    if decided(&existing) != decided(&wanted) {
        return Err(CompatInitError::RecordDiffers {
            existing: Box::new(existing),
            wanted: Box::new(wanted),
        });
    }
    Ok(InitReport {
        outcome: InitOutcome::AlreadyInitialized,
        record: existing,
        census: None,
        finished_unix_ns: clock.now_unix_ns(),
    })
}

/// The record `init` writes: every known family at this build's baseline
/// write version, never activated, epoch 0.
fn baseline_record(
    config: &InitConfig,
    adopted: &BTreeMap<(String, u32), u64>,
) -> Result<sysproto::CompatRecord, CompatInitError> {
    let families = KNOWN_FAMILIES
        .into_iter()
        .map(|family| {
            Ok(sysproto::CompatFamily {
                family: family.to_string(),
                active_write_version: baseline_version(family)?,
                ..Default::default()
            })
        })
        .collect::<Result<Vec<_>, CompatInitError>>()?;
    Ok(sysproto::CompatRecord {
        format_version: 0,
        baseline_release_major: config.baseline_release_major,
        baseline_release_minor: config.baseline_release_minor,
        families,
        compat_epoch: 0,
        max_publish_lifetime_ns: config.max_publish_lifetime_ns,
        adopted: adopted
            .iter()
            .map(|((family, version), count)| sysproto::AdoptedVersion {
                family: family.clone(),
                version: *version,
                count: *count,
            })
            .collect(),
        mac: Vec::new(),
    })
}

/// The version a default build of this release writes for `family`.
pub fn baseline_version(family: &str) -> Result<u32, CompatInitError> {
    let none = || CompatInitError::NoBaselineVersion {
        family: family.to_string(),
    };
    match family {
        // ADR-4233's inventory records kidx at the leaf format constant even
        // though the fold writes a higher version for metrics.
        FAMILY_KIDX => Ok(KEY_INDEX_LEAF_FORMAT_VERSION),
        FAMILY_CATALOG_HEAD | FAMILY_CSNAP | FAMILY_NPOST | FAMILY_CSTAT => {
            let mut versions = SIGNALS
                .into_iter()
                .map(|signal| fold_write_version(family, signal));
            let first = versions.next().flatten().ok_or_else(none)?;
            if versions.any(|v| v != Some(first)) {
                return Err(none());
            }
            Ok(u32::from(first))
        }
        FAMILY_RETENTION_TOMBSTONE => Ok(record::TOMBSTONE_FORMAT_VERSION),
        _ => FamilyVersions::for_family(family)
            .map(|v| u32::from(v.baseline))
            .ok_or_else(none),
    }
}

/// The data versions this build reads for a part family.
fn data_read_set_contains(family: &str, version: u32) -> bool {
    let Ok(version) = u16::try_from(version) else {
        return false;
    };
    match family {
        FAMILY_RSEG => ravel_segment::SUPPORTED_VERSIONS.contains(version),
        FAMILY_RLOG => ravel_logseg::footer::SUPPORTED_VERSIONS.contains(version),
        FAMILY_RSPAN => ravel_rspan::footer::SUPPORTED_VERSIONS.contains(version),
        _ => false,
    }
}

/// The census alone: every tenant and signal, refusing as [`init_compat`]
/// does. Writes nothing.
pub async fn run_census(store: &dyn ObjectStoreBackend) -> Result<CensusSummary, CompatInitError> {
    let tenants = list_tenants_strict(store).await?;
    let mut tally = Tally::default();
    for tenant in &tenants {
        census_tenant(store, *tenant, &mut tally).await?;
    }
    if !tally.outside.is_empty() {
        return Err(CompatInitError::OutsideReadSet {
            findings: tally.outside.into_values().collect(),
        });
    }
    tally.summary.tenants = tenants.len() as u64;
    Ok(tally.summary)
}

#[derive(Default)]
struct Tally {
    summary: CensusSummary,
    outside: BTreeMap<(TenantHash, &'static str, String, u32), OutsideReadSet>,
}

impl Tally {
    fn adopt(&mut self, family: &str, version: u32) {
        *self
            .summary
            .adopted
            .entry((family.to_string(), version))
            .or_insert(0) += 1;
    }

    fn outside(
        &mut self,
        tenant: TenantHash,
        signal: Option<Signal>,
        family: &str,
        version: u32,
        key: &str,
    ) {
        let group = (
            tenant,
            signal.map_or("", Signal::key_prefix),
            family.to_string(),
            version,
        );
        self.outside
            .entry(group)
            .or_insert_with(|| OutsideReadSet {
                tenant,
                signal,
                family: family.to_string(),
                version,
                count: 0,
                sample_key: key.to_string(),
            })
            .count += 1;
    }
}

/// Where a key under `t/<h>/` sits in the layout.
enum Class {
    TenantConfig,
    Provisioning(Signal),
    CatalogHead(Signal),
    CatalogSnap,
    CatalogPostings(Signal),
    CatalogStats,
    CatalogKeyIndex(Signal),
    BucketRecord(Signal),
    Data(Signal),
    Other,
}

fn classify(rest: &str) -> Class {
    if rest == "config" {
        return Class::TenantConfig;
    }
    if let Some(catalog) = rest.strip_prefix("catalog/") {
        let Some((signal, tail)) = catalog.split_once('/') else {
            return Class::Other;
        };
        let Some(signal) = signal_for_prefix(signal) else {
            return Class::Other;
        };
        return if tail == "HEAD" {
            Class::CatalogHead(signal)
        } else if tail.starts_with("snap/") && tail.ends_with(".csnap") {
            Class::CatalogSnap
        } else if !tail.starts_with("idx/") {
            Class::Other
        } else if tail.ends_with(".npost") {
            Class::CatalogPostings(signal)
        } else if tail.ends_with(".cstat") {
            Class::CatalogStats
        } else if tail.ends_with(".kidx") {
            Class::CatalogKeyIndex(signal)
        } else {
            Class::Other
        };
    }
    let Some((signal, tail)) = rest.split_once('/') else {
        return Class::Other;
    };
    let Some(signal) = signal_for_prefix(signal) else {
        return Class::Other;
    };
    if tail == "prov" {
        Class::Provisioning(signal)
    } else if tail.starts_with("c/") {
        Class::BucketRecord(signal)
    } else if tail.starts_with("l0/") || tail.starts_with("l1/") {
        Class::Data(signal)
    } else {
        Class::Other
    }
}

async fn census_tenant(
    store: &dyn ObjectStoreBackend,
    tenant: TenantHash,
    tally: &mut Tally,
) -> Result<(), CompatInitError> {
    let prefix = format!("t/{}/", tenant.to_hex());
    let listing =
        list_all_reporting(store, &prefix)
            .await
            .map_err(|source| StrictListError::Store {
                prefix: prefix.clone(),
                source,
            })?;
    refuse_unaddressable(&prefix, &listing.unaddressable)?;
    for meta in &listing.objects {
        let rest = meta.key.strip_prefix(&prefix).unwrap_or(&meta.key);
        let class = classify(rest);
        if matches!(class, Class::Other) {
            tally.summary.objects_ignored += 1;
            continue;
        }
        let checked = check_object(store, tenant, &meta.key, class, tally).await?;
        if checked {
            tally.summary.objects_checked += 1;
        } else {
            tally.summary.objects_vanished += 1;
        }
    }
    Ok(())
}

/// GET `key`; `Ok(None)` when it is gone since the listing.
async fn get(
    store: &dyn ObjectStoreBackend,
    key: &str,
    range: GetRange,
) -> Result<Option<GetOutcome>, CompatInitError> {
    match store.get(key, range).await {
        Ok(got) => Ok(Some(got)),
        Err(StoreError::NotFound) => Ok(None),
        Err(source) => Err(CompatInitError::Get {
            key: key.to_string(),
            source,
        }),
    }
}

fn unreadable(key: &str, what: &str, detail: impl ToString) -> CompatInitError {
    CompatInitError::Unreadable {
        key: key.to_string(),
        what: what.to_string(),
        detail: detail.to_string(),
    }
}

/// Check one object; `Ok(false)` when it vanished before its GET.
async fn check_object(
    store: &dyn ObjectStoreBackend,
    tenant: TenantHash,
    key: &str,
    class: Class,
    tally: &mut Tally,
) -> Result<bool, CompatInitError> {
    if let Class::Data(signal) = class {
        return check_data(store, tenant, signal, key, tally).await;
    }
    let Some(got) = get(store, key, GetRange::Full).await? else {
        return Ok(false);
    };
    let bytes = got.data.as_ref();
    match class {
        Class::TenantConfig => {
            let rec = sysproto::TenantConfigRecord::decode(bytes)
                .map_err(|e| unreadable(key, WHAT_TENANT_CONFIG, e))?;
            let v = rec.format_version;
            if !(TENANT_CONFIG_MIN_READ_VERSION..=TENANT_CONFIG_MAX_READ_VERSION).contains(&v) {
                tally.outside(tenant, None, WHAT_TENANT_CONFIG, v, key);
            }
        }
        Class::Provisioning(signal) => {
            let rec = sysproto::ProvisioningRecord::decode(bytes)
                .map_err(|e| unreadable(key, WHAT_PROVISIONING, e))?;
            let v = rec.format_version;
            if !(PROVISIONING_MIN_READ_VERSION..=PROVISIONING_MAX_READ_VERSION).contains(&v) {
                tally.outside(tenant, Some(signal), WHAT_PROVISIONING, v, key);
            } else if rec.compat_epoch > 0 {
                return Err(CompatInitError::ProvisioningFenced {
                    key: key.to_string(),
                    compat_epoch: rec.compat_epoch,
                });
            }
        }
        Class::CatalogHead(signal) => {
            let raw = ravel_proto::catalog::v1::SnapshotHead::decode(bytes)
                .map_err(|e| unreadable(key, FAMILY_CATALOG_HEAD, e))?;
            if !HEAD_FORMAT_VERSIONS.contains(&raw.format_version) {
                tally.outside(
                    tenant,
                    Some(signal),
                    FAMILY_CATALOG_HEAD,
                    raw.format_version,
                    key,
                );
            } else {
                decode_head(bytes).map_err(|e| unreadable(key, FAMILY_CATALOG_HEAD, e))?;
                tally.adopt(FAMILY_CATALOG_HEAD, raw.format_version);
            }
        }
        Class::CatalogSnap => match decode_part(bytes, &PartLimits::default()) {
            Ok(part) => tally.adopt(FAMILY_CSNAP, part.header.format_version),
            Err(e) => catalog_refusal(tenant, None, FAMILY_CSNAP, key, &e, tally)?,
        },
        Class::CatalogPostings(signal) => {
            match decode_postings(bytes, &PostingsLimits::default(), &[]) {
                Ok(postings) => tally.adopt(FAMILY_NPOST, postings.header.format_version),
                // The envelope and header versions are checked before the part
                // binding, which only HEAD can supply; reaching the binding means
                // the object is at the version this build writes.
                Err(SnapshotFormatError::PostingsPartBindingMismatch) => {
                    let version = fold_write_version(FAMILY_NPOST, signal)
                        .ok_or_else(|| unreadable(key, FAMILY_NPOST, "no npost version"))?;
                    tally.adopt(FAMILY_NPOST, u32::from(version));
                }
                Err(e) => catalog_refusal(tenant, Some(signal), FAMILY_NPOST, key, &e, tally)?,
            }
        }
        Class::CatalogStats => match decode_column_stats_header(bytes) {
            Ok(peek) => tally.adopt(FAMILY_CSTAT, u32::from(peek.envelope_version)),
            Err(e) => catalog_refusal(tenant, None, FAMILY_CSTAT, key, &e, tally)?,
        },
        // The read set is every version the decoder parses for the signal,
        // not the narrower set a lookup accepts: a logs version-1 leaf is
        // decoded and its part treated as uncovered, which is a safe read.
        Class::CatalogKeyIndex(signal) => match key_index_leaf_format_version(bytes) {
            Ok(v)
                if KEY_INDEX_LEAF_FORMAT_VERSIONS_KNOWN.contains(&v)
                    && key_index_leaf_versions_for_signal(signal).contains(&v) =>
            {
                tally.adopt(FAMILY_KIDX, v)
            }
            Ok(v) | Err(KeyIndexLeafRejection::UnsupportedVersion(v)) => {
                tally.outside(tenant, Some(signal), FAMILY_KIDX, v, key);
            }
            Err(e) => return Err(unreadable(key, FAMILY_KIDX, e)),
        },
        Class::BucketRecord(signal) => check_bucket_record(tenant, signal, key, bytes, tally)?,
        Class::Data(_) | Class::Other => {}
    }
    Ok(true)
}

/// A catalog decode failure: a newer version is a finding, anything else
/// refuses as unreadable.
fn catalog_refusal(
    tenant: TenantHash,
    signal: Option<Signal>,
    family: &str,
    key: &str,
    err: &SnapshotFormatError,
    tally: &mut Tally,
) -> Result<(), CompatInitError> {
    match err.newer_part_version() {
        Some(newer) => {
            tally.outside(tenant, signal, family, newer.version, key);
            Ok(())
        }
        None => Err(unreadable(key, family, err)),
    }
}

fn check_bucket_record(
    tenant: TenantHash,
    signal: Signal,
    key: &str,
    bytes: &[u8],
    tally: &mut Tally,
) -> Result<(), CompatInitError> {
    use ravel_proto::commit::v1 as commitproto;

    let entry = partition_bucket_entry(key).map_err(|_| CompatInitError::UnknownBucketEntry {
        key: key.to_string(),
    })?;
    let mut stamps = Vec::new();
    match entry {
        BucketEntry::CommitRecord(_) => {
            let family = FAMILY_COMMIT_RECORD;
            let raw =
                commitproto::CommitRecord::decode(bytes).map_err(|e| unreadable(key, family, e))?;
            if raw.format_version != record::FORMAT_VERSION {
                tally.outside(tenant, Some(signal), family, raw.format_version, key);
                return Ok(());
            }
            let rec = record::decode(bytes).map_err(|e| unreadable(key, family, e))?;
            tally.adopt(family, rec.format_version);
            stamps.push(rec.segment_format_version);
        }
        BucketEntry::CompactionRecord(_) => {
            let family = FAMILY_COMPACTION_RECORD;
            let raw = commitproto::CompactionRecord::decode(bytes)
                .map_err(|e| unreadable(key, family, e))?;
            let window =
                record::COMPACTION_FORMAT_VERSION..=record::COMPACTION_SUPERSEDING_FORMAT_VERSION;
            if !window.contains(&raw.format_version) {
                tally.outside(tenant, Some(signal), family, raw.format_version, key);
                return Ok(());
            }
            let rec = record::decode_compaction(bytes).map_err(|e| unreadable(key, family, e))?;
            tally.adopt(family, rec.format_version);
            stamps.extend(rec.parts.iter().map(|p| p.segment_format_version));
        }
        BucketEntry::RewriteRecord(_) => {
            let family = FAMILY_REWRITE_RECORD;
            let raw = commitproto::RewriteRecord::decode(bytes)
                .map_err(|e| unreadable(key, family, e))?;
            if raw.format_version != erasure::FORMAT_VERSION {
                tally.outside(tenant, Some(signal), family, raw.format_version, key);
                return Ok(());
            }
            let rec = erasure::decode_rewrite(bytes).map_err(|e| unreadable(key, family, e))?;
            tally.adopt(family, rec.format_version);
            stamps.extend(rec.parts.iter().map(|p| p.segment_format_version));
        }
        BucketEntry::Tombstone(_) => {
            let family = FAMILY_RETENTION_TOMBSTONE;
            let raw = commitproto::RetentionTombstone::decode(bytes)
                .map_err(|e| unreadable(key, family, e))?;
            if raw.format_version != record::TOMBSTONE_FORMAT_VERSION {
                tally.outside(tenant, Some(signal), family, raw.format_version, key);
                return Ok(());
            }
            let rec = record::decode_tombstone(bytes).map_err(|e| unreadable(key, family, e))?;
            tally.adopt(family, rec.format_version);
        }
    }
    if stamps.is_empty() {
        return Ok(());
    }
    let data_family = part_family(signal).ok_or_else(|| CompatInitError::NoFamilyForSignal {
        key: key.to_string(),
        signal,
    })?;
    for stamp in stamps {
        if !data_read_set_contains(data_family, stamp) {
            tally.outside(tenant, Some(signal), data_family, stamp, key);
        }
    }
    Ok(())
}

/// Check one data object's trailer version, and for RLOG and RSPAN the footer
/// it names.
async fn check_data(
    store: &dyn ObjectStoreBackend,
    tenant: TenantHash,
    signal: Signal,
    key: &str,
    tally: &mut Tally,
) -> Result<bool, CompatInitError> {
    let family = part_family(signal).ok_or_else(|| CompatInitError::NoFamilyForSignal {
        key: key.to_string(),
        signal,
    })?;
    let Some(tail) = get(store, key, GetRange::Suffix(TRAILER_LEN)).await? else {
        return Ok(false);
    };
    let total = tail.total_size;
    let version = match family {
        FAMILY_RSEG => match classify_trailer(total, tail.data.as_ref()) {
            TrailerClass::Readable(v) => v.number(),
            TrailerClass::OutsideVersionWindow(v) => {
                tally.outside(tenant, Some(signal), family, u32::from(v), key);
                return Ok(true);
            }
            TrailerClass::Corrupt(e) => return Err(unreadable(key, family, e)),
        },
        FAMILY_RLOG | FAMILY_RSPAN => {
            match open_footer(family, tail.data.as_ref(), total) {
                FooterOpen::Ready => {}
                FooterOpen::Outside(v) => {
                    tally.outside(tenant, Some(signal), family, u32::from(v), key);
                    return Ok(true);
                }
                FooterOpen::NeedRange { offset, len } => {
                    let end = offset.saturating_add(len);
                    let Some(footer) = get(store, key, GetRange::Range(offset, end)).await? else {
                        return Ok(false);
                    };
                    match open_footer(family, footer.data.as_ref(), total) {
                        FooterOpen::Ready => {}
                        FooterOpen::Outside(v) => {
                            tally.outside(tenant, Some(signal), family, u32::from(v), key);
                            return Ok(true);
                        }
                        FooterOpen::NeedRange { .. } => {
                            return Err(unreadable(key, family, "footer range does not cover it"));
                        }
                        FooterOpen::Corrupt(detail) => return Err(unreadable(key, family, detail)),
                    }
                }
                FooterOpen::Corrupt(detail) => return Err(unreadable(key, family, detail)),
            }
            trailer_version(tail.data.as_ref())
                .ok_or_else(|| unreadable(key, family, "suffix does not cover the trailer"))?
        }
        _ => {
            return Err(CompatInitError::NoFamilyForSignal {
                key: key.to_string(),
                signal,
            });
        }
    };
    tally.adopt(family, u32::from(version));
    Ok(true)
}

enum FooterOpen {
    Ready,
    Outside(u16),
    NeedRange { offset: u64, len: u64 },
    Corrupt(String),
}

fn open_footer(family: &str, suffix: &[u8], total: u64) -> FooterOpen {
    if family == FAMILY_RLOG {
        match ravel_logseg::open_from_suffix(suffix, total) {
            Ok(ravel_logseg::footer::SuffixOutcome::Ready(_)) => FooterOpen::Ready,
            Ok(ravel_logseg::footer::SuffixOutcome::NeedRange { offset, len }) => {
                FooterOpen::NeedRange { offset, len }
            }
            Err(ravel_logseg::LogSegError::UnsupportedVersion(v)) => FooterOpen::Outside(v),
            Err(e) => FooterOpen::Corrupt(e.to_string()),
        }
    } else {
        match ravel_rspan::open_from_suffix(suffix, total) {
            Ok(ravel_rspan::SuffixOutcome::Ready(_)) => FooterOpen::Ready,
            Ok(ravel_rspan::SuffixOutcome::NeedRange { offset, len }) => {
                FooterOpen::NeedRange { offset, len }
            }
            Err(ravel_rspan::SpanSegError::UnsupportedVersion(v)) => FooterOpen::Outside(v),
            Err(e) => FooterOpen::Corrupt(e.to_string()),
        }
    }
}

/// The RLOG/RSPAN trailer version; call only after the suffix open accepted
/// the trailer.
fn trailer_version(suffix: &[u8]) -> Option<u16> {
    let start = suffix.len().checked_sub(TRAILER_LEN as usize)? + TRAILER_VERSION_OFFSET;
    let bytes = suffix.get(start..start + 2)?;
    Some(u16::from_le_bytes([bytes[0], bytes[1]]))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod census_tests;

#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) mod tests {
    use super::*;
    use bytes::Bytes;
    use ravel_catalog::compat::read_compat;
    use ravel_commit::keys;
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{PutOptions, list_all};
    use std::sync::Arc;
    use uuid::Uuid;

    pub(crate) const T0: i64 = 1_800_000_000_000_000_000;

    pub(crate) struct TestClock(pub i64);

    impl CompatClock for TestClock {
        fn now_unix_ns(&self) -> i64 {
            self.0
        }
    }

    pub(crate) fn tenant(n: u8) -> TenantHash {
        TenantHash([n; 16])
    }

    pub(crate) fn test_keys() -> CompatKeys {
        CompatKeys::from_file_keys(&[[7u8; 32]]).expect("keys")
    }

    pub(crate) fn config() -> InitConfig {
        InitConfig {
            baseline_release_major: 0,
            baseline_release_minor: 34,
            max_publish_lifetime_ns: crate::DEFAULT_PUBLISH_LIFETIME_NS,
        }
    }

    pub(crate) async fn provision(store: &dyn ObjectStoreBackend, t: TenantHash, signal: Signal) {
        ravel_catalog::validate_or_adopt(
            store,
            &t,
            signal,
            1,
            0,
            ravel_catalog::AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("provision");
    }

    /// An empty current-version RSEG object.
    fn rseg_bytes(t: TenantHash) -> Bytes {
        ravel_segment::SegmentWriter::write(
            Vec::new(),
            ravel_segment::SegmentIdentity {
                tenant_hash: t.0,
                shard: 0,
                writer_id: Uuid::nil().to_string(),
                writer_epoch: 1,
                writer_seq: 1,
            },
            ravel_segment::IngestBounds {
                min_ingest_ts_ns: 0,
                max_ingest_ts_ns: 0,
            },
        )
        .expect("write rseg")
        .bytes
    }

    /// A provisioned metrics tenant holding one L0 object and its commit
    /// record. Returns the data key.
    pub(crate) async fn seed_metrics(store: &dyn ObjectStoreBackend, t: TenantHash) -> String {
        provision(store, t, Signal::Metrics).await;
        let data = rseg_bytes(t);
        let content_hash = *blake3::hash(&data).as_bytes();
        let rec = record::build(record::NewCommitRecord {
            tenant_hash: t,
            signal: Signal::Metrics,
            shard: 0,
            writer_id: Uuid::nil(),
            writer_epoch: 1,
            writer_seq: 1,
            object_size: data.len() as u64,
            content_hash,
            sample_count: 0,
            series_count: 0,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
            segment_format_version: u32::from(ravel_segment::SegmentVersion::CURRENT.number()),
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
        })
        .expect("build record");
        let data_key = rec.object_key.clone();
        store
            .put(&data_key, data, PutOptions::default())
            .await
            .expect("put data");
        let commit_key =
            keys::commit_key(&t, Signal::Metrics, 0, 0, Uuid::nil(), 1, 1).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put record");
        data_key
    }

    /// Rewrite the trailer version of the object at `key`.
    pub(crate) async fn restamp_trailer(store: &dyn ObjectStoreBackend, key: &str, version: u16) {
        let got = store.get(key, GetRange::Full).await.expect("get");
        let mut bytes = got.data.to_vec();
        let at = bytes.len() - TRAILER_LEN as usize + TRAILER_VERSION_OFFSET;
        bytes[at..at + 2].copy_from_slice(&version.to_le_bytes());
        store
            .put(key, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("restamp");
    }

    /// A store wrapper that fails every PUT, so a refusal test proves it
    /// attempted none: the counter must stay at zero.
    fn no_put_store(inner: Arc<MemoryStore>) -> FaultStore<Arc<MemoryStore>> {
        FaultStore::new(
            inner,
            FaultPlan::empty().with_rule(Rule::new(
                Op::Put,
                ScriptedFault::Permanent("refusal tests write nothing".into()),
            )),
        )
    }

    #[tokio::test]
    async fn init_creates_the_baseline_record_from_a_clean_census() {
        let mem = Arc::new(MemoryStore::with_page_size(2));
        seed_metrics(mem.as_ref(), tenant(1)).await;
        seed_metrics(mem.as_ref(), tenant(2)).await;

        let report = init_compat(mem.as_ref(), Some(&test_keys()), &TestClock(T0), &config())
            .await
            .expect("init");

        assert_eq!(report.outcome, InitOutcome::Created);
        let census = report.census.expect("census ran");
        assert_eq!(census.tenants, 2);
        // Per tenant: prov, one commit record, one data object.
        assert_eq!(census.objects_checked, 6);
        let rseg = u32::from(ravel_segment::SegmentVersion::CURRENT.number());
        assert_eq!(
            census.adopted,
            BTreeMap::from([
                ((FAMILY_RSEG.to_string(), rseg), 2),
                (
                    (FAMILY_COMMIT_RECORD.to_string(), record::FORMAT_VERSION),
                    2
                ),
            ])
        );
        let (stored, _) = read_compat(mem.as_ref(), &test_keys())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(stored, report.record);
        assert_eq!(stored.compat_epoch, 0);
        assert_eq!(
            stored.max_publish_lifetime_ns,
            crate::DEFAULT_PUBLISH_LIFETIME_NS
        );
        let versions: Vec<(&str, u32)> = stored
            .families
            .iter()
            .map(|f| (f.family.as_str(), f.active_write_version))
            .collect();
        assert_eq!(
            versions,
            vec![
                (FAMILY_RSEG, rseg),
                (
                    FAMILY_RLOG,
                    u32::from(ravel_logseg::footer::WriteVersion::CURRENT.number())
                ),
                (
                    FAMILY_RSPAN,
                    u32::from(ravel_rspan::WriteVersion::CURRENT.number())
                ),
                (FAMILY_CATALOG_HEAD, ravel_catalog::HEAD_FORMAT_VERSION_2),
                (FAMILY_CSNAP, u32::from(ravel_catalog::VERSION)),
                (FAMILY_NPOST, 1),
                (FAMILY_CSTAT, 3),
                (FAMILY_KIDX, KEY_INDEX_LEAF_FORMAT_VERSION),
                (FAMILY_COMMIT_RECORD, record::FORMAT_VERSION),
                (FAMILY_COMPACTION_RECORD, record::COMPACTION_FORMAT_VERSION),
                (FAMILY_REWRITE_RECORD, erasure::FORMAT_VERSION),
                (FAMILY_RETENTION_TOMBSTONE, record::TOMBSTONE_FORMAT_VERSION),
            ]
        );
        assert!(stored.families.iter().all(|f| f.activated_epoch == 0));
    }

    #[tokio::test]
    async fn init_is_idempotent() {
        let mem = Arc::new(MemoryStore::with_page_size(2));
        seed_metrics(mem.as_ref(), tenant(1)).await;
        let first = init_compat(mem.as_ref(), Some(&test_keys()), &TestClock(T0), &config())
            .await
            .expect("first init");
        // The store grows between the two runs; the adopted histogram is an
        // observation, so the second run still matches.
        seed_metrics(mem.as_ref(), tenant(2)).await;
        let before = mem.get(COMPAT_KEY, GetRange::Full).await.expect("get");

        let store = no_put_store(mem.clone());
        let second = init_compat(&store, Some(&test_keys()), &TestClock(T0 + 1), &config())
            .await
            .expect("second init");

        assert_eq!(second.outcome, InitOutcome::AlreadyInitialized);
        assert_eq!(second.census, None);
        assert_eq!(second.record, first.record);
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 0);
        let after = mem.get(COMPAT_KEY, GetRange::Full).await.expect("get");
        assert_eq!(after.version, before.version);
    }

    #[tokio::test]
    async fn init_refuses_a_different_existing_record() {
        let mem = Arc::new(MemoryStore::with_page_size(2));
        init_compat(mem.as_ref(), Some(&test_keys()), &TestClock(T0), &config())
            .await
            .expect("init");
        let other = InitConfig {
            max_publish_lifetime_ns: 2 * crate::DEFAULT_PUBLISH_LIFETIME_NS,
            ..config()
        };
        let store = no_put_store(mem.clone());
        let err = init_compat(&store, Some(&test_keys()), &TestClock(T0), &other)
            .await
            .expect_err("a different record refuses");
        assert!(
            matches!(err, CompatInitError::RecordDiffers { .. }),
            "{err:?}"
        );
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 0);
    }

    #[tokio::test]
    async fn init_refuses_an_out_of_window_object() {
        let mem = Arc::new(MemoryStore::with_page_size(2));
        seed_metrics(mem.as_ref(), tenant(1)).await;
        let data_key = seed_metrics(mem.as_ref(), tenant(2)).await;
        let outside = u16::MAX;
        assert!(!ravel_segment::SUPPORTED_VERSIONS.contains(outside));
        restamp_trailer(mem.as_ref(), &data_key, outside).await;

        let store = no_put_store(mem.clone());
        let err = init_compat(&store, Some(&test_keys()), &TestClock(T0), &config())
            .await
            .expect_err("out-of-window object refuses");

        let CompatInitError::OutsideReadSet { findings } = err else {
            panic!("expected OutsideReadSet, got {err:?}");
        };
        assert_eq!(
            findings,
            vec![OutsideReadSet {
                tenant: tenant(2),
                signal: Some(Signal::Metrics),
                family: FAMILY_RSEG.to_string(),
                version: u32::from(outside),
                count: 1,
                sample_key: data_key,
            }]
        );
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 0);
        assert!(mem.get(COMPAT_KEY, GetRange::Full).await.is_err());
    }

    #[tokio::test]
    async fn init_refuses_an_out_of_window_commit_record_stamp() {
        let mem = Arc::new(MemoryStore::with_page_size(2));
        provision(mem.as_ref(), tenant(1), Signal::Metrics).await;
        let commit_key =
            keys::commit_key(&tenant(1), Signal::Metrics, 0, 0, Uuid::nil(), 1, 1).expect("key");
        let rec = record::build(record::NewCommitRecord {
            tenant_hash: tenant(1),
            signal: Signal::Metrics,
            shard: 0,
            writer_id: Uuid::nil(),
            writer_epoch: 1,
            writer_seq: 1,
            object_size: 1,
            content_hash: [1; 32],
            sample_count: 0,
            series_count: 0,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
            segment_format_version: u32::from(u16::MAX),
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
        })
        .expect("build");
        mem.put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put");

        let err = run_census(mem.as_ref()).await.expect_err("refuses");
        let CompatInitError::OutsideReadSet { findings } = err else {
            panic!("expected OutsideReadSet, got {err:?}");
        };
        assert_eq!(findings.len(), 1);
        assert_eq!(findings[0].family, FAMILY_RSEG);
        assert_eq!(findings[0].sample_key, commit_key);
    }

    #[tokio::test]
    async fn init_refuses_an_unaddressable_key() {
        let mem = Arc::new(MemoryStore::with_page_size(2));
        seed_metrics(mem.as_ref(), tenant(1)).await;
        let prefix =
            keys::commit_shard_hour_prefix(&tenant(1), Signal::Metrics, 0, 0).expect("prefix");
        let foreign = format!("{prefix}bad%key.cmt");
        mem.insert_foreign(&foreign, Bytes::from_static(b"foreign"));

        let store = no_put_store(mem.clone());
        let err = init_compat(&store, Some(&test_keys()), &TestClock(T0), &config())
            .await
            .expect_err("unaddressable key refuses");

        match err {
            CompatInitError::List(StrictListError::Unaddressable { key, count, .. }) => {
                assert_eq!(key, foreign);
                assert_eq!(count, 1);
            }
            other => panic!("expected Unaddressable, got {other:?}"),
        }
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 0);
    }

    #[tokio::test]
    async fn init_refuses_a_listing_failure() {
        let mem = Arc::new(MemoryStore::with_page_size(2));
        seed_metrics(mem.as_ref(), tenant(1)).await;
        let store = FaultStore::new(
            mem.clone(),
            FaultPlan::empty()
                .with_rule(Rule::new(
                    Op::List,
                    ScriptedFault::Transient("list down".into()),
                ))
                .with_rule(Rule::new(
                    Op::Put,
                    ScriptedFault::Permanent("refusal tests write nothing".into()),
                )),
        );
        let err = init_compat(&store, Some(&test_keys()), &TestClock(T0), &config())
            .await
            .expect_err("listing failure refuses");
        assert!(
            matches!(err, CompatInitError::List(StrictListError::Store { .. })),
            "{err:?}"
        );
        assert!(store.fault_count(Op::List, FaultKind::Transient) >= 1);
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 0);
    }

    #[tokio::test]
    async fn init_refuses_a_fenced_provisioning_record() {
        let mem = Arc::new(MemoryStore::with_page_size(2));
        seed_metrics(mem.as_ref(), tenant(1)).await;
        ravel_catalog::raise_compat_fence(mem.as_ref(), &tenant(1), Signal::Metrics, 3, T0)
            .await
            .expect("fence");

        let store = no_put_store(mem.clone());
        let err = init_compat(&store, Some(&test_keys()), &TestClock(T0), &config())
            .await
            .expect_err("fenced bucket refuses");
        assert!(
            matches!(
                err,
                CompatInitError::ProvisioningFenced {
                    compat_epoch: 3,
                    ..
                }
            ),
            "{err:?}"
        );
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 0);
    }

    #[tokio::test]
    async fn init_refuses_an_unkeyed_store() {
        let mem = Arc::new(MemoryStore::with_page_size(2));
        let store = no_put_store(mem.clone());
        let err = init_compat(&store, None, &TestClock(T0), &config())
            .await
            .expect_err("unkeyed refuses");
        assert!(matches!(err, CompatInitError::Unkeyed), "{err:?}");
        assert!(err.to_string().contains("no file keys configured"));
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 0);
        assert!(list_all(mem.as_ref(), "").await.expect("list").is_empty());
    }

    #[test]
    fn every_known_family_has_a_baseline_version() {
        for family in KNOWN_FAMILIES {
            baseline_version(family).expect(family);
        }
    }
}
