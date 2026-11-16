//! The durable activation record `sys/compat` and a process's view of it
//! (ADR-4233 decisions 3 and 4).
//!
//! `sys/compat` is a class C record: protobuf, `format_version` 1, replaced
//! only by a CAS on the store version, and authenticated by a MAC under the
//! deployment key like the heartbeat records. It holds one entry per format
//! family with the version writers emit, and one store-wide `compat_epoch`
//! raised by every activation of any family.
//!
//! The values writers act on are monotone: `compat_epoch`, each family's
//! `active_write_version` and `activated_epoch`, and the certificate's
//! version. [`activate_family`] raises one family by exactly one and the
//! epoch by one; [`update_compat`] refuses any change that would lower one of
//! those or drop a family entry. Other fields are not guarded here: the
//! baseline release, `max_publish_lifetime_ns` and `adopted` are written by
//! `compat init`, and a same-version re-certify (ADR-4233 decision 7) replaces
//! the certificate, resets `late_writes_verified_through_unix_ns` and clears
//! `contradicted`. A
//! [`CompatView`] never lowers a value it has seen in this run, whatever a
//! later read returns.
//!
//! The MAC is `blake3::keyed_hash(record_key, claim)` with
//! `record_key = blake3::derive_key(`[`COMPAT_RECORD_KEY_CONTEXT`]`,
//! file_key)`, one key per configured file key: the first mints, any
//! verifies. The claim is built from field values, not from the encoded
//! bytes, so it does not depend on protobuf field order. All integers are
//! big-endian; each string or byte field is a u64 length followed by its
//! bytes; a repeated field is a u64 count followed by its entries; an
//! optional message is a one-byte presence flag followed by its fields:
//!
//! ```text
//! format_version u32, baseline_release_major u32, baseline_release_minor u32,
//! compat_epoch u64, max_publish_lifetime_ns i64,
//! families: count, then per family:
//!   family str, active_write_version u32, activated_epoch u64,
//!   activated_unix_ns i64, activated_by str,
//!   certificate: present u8, then version u32, certified_unix_ns i64,
//!     member_count u64, newest_created_unix_ns i64, buckets_scanned u64,
//!     tenant_set_digest bytes,
//!   fence_complete_unix_ns i64, late_writes_verified_through_unix_ns i64,
//!   contradicted u8,
//! adopted: count, then per entry: family str, version u32, count u64
//! ```
//!
//! A record whose `format_version` is above [`COMPAT_MAX_READ_VERSION`] is
//! [`CompatError::CompatRecordNewer`] before its MAC is checked: a newer
//! release may authenticate a claim this build cannot rebuild, and a newer
//! record is never read as absent and never overwritten.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::Duration;

use bytes::Bytes;
use parking_lot::RwLock;
use prost::Message;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
use ravel_proto::sys::v1 as sysproto;

/// The object key of the store-wide activation record.
pub const COMPAT_KEY: &str = "sys/compat";

/// The `format_version` this build stamps on `sys/compat`.
pub const COMPAT_FORMAT_VERSION: u32 = 1;
/// The lowest `sys/compat` `format_version` this build reads.
pub const COMPAT_MIN_READ_VERSION: u32 = 1;
/// The highest `sys/compat` `format_version` this build reads. Above it the
/// read is [`CompatError::CompatRecordNewer`].
pub const COMPAT_MAX_READ_VERSION: u32 = 1;

/// The `blake3::derive_key` context the `sys/compat` MAC keys are derived
/// under. Distinct from every other record context, so a MAC minted for one
/// record kind never verifies as another.
pub const COMPAT_RECORD_KEY_CONTEXT: &str = "ravel compat record v1";

/// The hard staleness horizon `H`: a writer refuses to start encoding an
/// object, and refuses every PUT attempt, once its last successful read of
/// `sys/compat` is at least this old.
pub const COMPAT_VIEW_HORIZON: Duration = Duration::from_secs(10 * 60);
/// [`COMPAT_VIEW_HORIZON`] in nanoseconds.
pub const COMPAT_VIEW_HORIZON_NS: i64 = 10 * 60 * 1_000_000_000;

/// How often a process re-reads `sys/compat`: the `GenerationSwitch`
/// refresh interval.
pub const DEFAULT_COMPAT_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Data families (decision 2).
pub const FAMILY_RSEG: &str = "rseg";
pub const FAMILY_RLOG: &str = "rlog";
pub const FAMILY_RSPAN: &str = "rspan";
/// Catalog families (decision 8).
pub const FAMILY_CATALOG_HEAD: &str = "catalog-head";
pub const FAMILY_CSNAP: &str = "csnap";
pub const FAMILY_NPOST: &str = "npost";
pub const FAMILY_CSTAT: &str = "cstat";
pub const FAMILY_KIDX: &str = "kidx";
/// Record families.
pub const FAMILY_COMMIT_RECORD: &str = "commit-record";
pub const FAMILY_COMPACTION_RECORD: &str = "compaction-record";
pub const FAMILY_REWRITE_RECORD: &str = "rewrite-record";
pub const FAMILY_RETENTION_TOMBSTONE: &str = "retention-tombstone";

/// Every family name this build knows, in a fixed order.
pub const KNOWN_FAMILIES: [&str; 12] = [
    FAMILY_RSEG,
    FAMILY_RLOG,
    FAMILY_RSPAN,
    FAMILY_CATALOG_HEAD,
    FAMILY_CSNAP,
    FAMILY_NPOST,
    FAMILY_CSTAT,
    FAMILY_KIDX,
    FAMILY_COMMIT_RECORD,
    FAMILY_COMPACTION_RECORD,
    FAMILY_REWRITE_RECORD,
    FAMILY_RETENTION_TOMBSTONE,
];

/// Whether `family` is a well-formed family name: non-empty, lowercase ASCII
/// letters, digits and `-`. Names this build does not know are well-formed
/// too; a newer release may add a family.
pub fn is_valid_family_name(family: &str) -> bool {
    !family.is_empty()
        && family
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// The `sys/compat` MAC keys: one per configured file key, each
/// `blake3::derive_key(`[`COMPAT_RECORD_KEY_CONTEXT`]`, file_key)`. The first
/// mints; a MAC verifies under any of them. Never empty.
#[derive(Clone)]
pub struct CompatKeys {
    keys: Vec<[u8; 32]>,
}

impl CompatKeys {
    /// Derive one key per file key, in order. An empty list is
    /// [`CompatError::NoKeys`]: an unauthenticated record is never written or
    /// trusted.
    pub fn from_file_keys(file_keys: &[[u8; 32]]) -> Result<Self, CompatError> {
        if file_keys.is_empty() {
            return Err(CompatError::NoKeys);
        }
        let keys = file_keys
            .iter()
            .map(|file_key| blake3::derive_key(COMPAT_RECORD_KEY_CONTEXT, file_key))
            .collect();
        Ok(CompatKeys { keys })
    }

    fn mac(&self, claim: &[u8]) -> blake3::Hash {
        // `from_file_keys` refuses an empty list, so `keys[0]` always exists;
        // the fallback keeps the type total without a panic path.
        let mint = self.keys.first().copied().unwrap_or([0; 32]);
        blake3::keyed_hash(&mint, claim)
    }

    /// Each comparison is `blake3::Hash`'s constant-time equality.
    fn verifies(&self, claim: &[u8], presented: &blake3::Hash) -> bool {
        self.keys
            .iter()
            .any(|key| blake3::keyed_hash(key, claim) == *presented)
    }
}

impl std::fmt::Debug for CompatKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompatKeys")
            .field("keys", &self.keys.len())
            .finish()
    }
}

/// A failure reading or writing `sys/compat`.
#[derive(Debug, thiserror::Error)]
pub enum CompatError {
    #[error("{key}: object store: {source}")]
    Store {
        key: String,
        #[source]
        source: StoreError,
    },
    #[error("{key}: protobuf decode: {source}")]
    Decode {
        key: String,
        #[source]
        source: prost::DecodeError,
    },
    /// The record was written by a newer release. It is neither absent nor
    /// corrupt, and this build never overwrites it.
    #[error(
        "{key}: format_version {got} is newer than this build reads (up to {ceiling}); refusing to read or overwrite it"
    )]
    CompatRecordNewer { key: String, got: u32, ceiling: u32 },
    #[error("{key}: format_version {got} is below the lowest this build reads ({floor})")]
    VersionBelowFloor { key: String, got: u32, floor: u32 },
    #[error("{key}: record carries no MAC")]
    MissingMac { key: String },
    #[error("{key}: MAC does not verify under any configured file key")]
    BadMac { key: String },
    #[error("{key}: corrupt record: {defect}")]
    Corrupt { key: String, defect: String },
    #[error("{key}: already exists")]
    AlreadyExists { key: String },
    /// The record changed between the read and the CAS write. Retryable:
    /// re-read and re-apply.
    #[error("{key}: changed since it was read")]
    CasConflict { key: String },
    #[error("{key}: no record; `compat init` creates it")]
    RecordAbsent { key: String },
    #[error("{key}: the update would lower {what}")]
    WouldLower { key: String, what: String },
    #[error("family {family}: version {requested} is not above the active version {active}")]
    VersionNotAboveActive {
        family: String,
        active: u32,
        requested: u32,
    },
    #[error(
        "family {family}: version {requested} is more than one above the active version {active}"
    )]
    VersionJump {
        family: String,
        active: u32,
        requested: u32,
    },
    #[error("family {family} has no entry in {COMPAT_KEY}")]
    FamilyNotInRecord { family: String },
    #[error("{family:?} is not a family name (lowercase letters, digits and '-')")]
    FamilyInvalid { family: String },
    #[error("{key}: compat_epoch is exhausted")]
    EpochExhausted { key: String },
    #[error("no file keys configured; {COMPAT_KEY} cannot be authenticated")]
    NoKeys,
}

impl CompatError {
    pub fn is_retryable(&self) -> bool {
        match self {
            CompatError::Store { source, .. } => source.is_retryable(),
            CompatError::CasConflict { .. } => true,
            _ => false,
        }
    }

    /// Whether the store refused the request for lack of a grant
    /// ([`StoreError::AccessDenied`]). Every other failure, a 5xx, a timeout
    /// or a record this build cannot read included, is not.
    pub fn is_access_denied(&self) -> bool {
        matches!(
            self,
            CompatError::Store {
                source: StoreError::AccessDenied(_),
                ..
            }
        )
    }

    fn store(err: StoreError) -> Self {
        CompatError::Store {
            key: COMPAT_KEY.to_string(),
            source: err,
        }
    }

    fn corrupt(defect: impl Into<String>) -> Self {
        CompatError::Corrupt {
            key: COMPAT_KEY.to_string(),
            defect: defect.into(),
        }
    }
}

struct Claim(Vec<u8>);

impl Claim {
    fn u8(&mut self, v: u8) {
        self.0.push(v);
    }
    fn u32(&mut self, v: u32) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn u64(&mut self, v: u64) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn i64(&mut self, v: i64) {
        self.0.extend_from_slice(&v.to_be_bytes());
    }
    fn bytes(&mut self, v: &[u8]) {
        self.u64(v.len() as u64);
        self.0.extend_from_slice(v);
    }
}

fn claim(record: &sysproto::CompatRecord) -> Vec<u8> {
    let mut c = Claim(Vec::new());
    c.u32(record.format_version);
    c.u32(record.baseline_release_major);
    c.u32(record.baseline_release_minor);
    c.u64(record.compat_epoch);
    c.i64(record.max_publish_lifetime_ns);
    c.u64(record.families.len() as u64);
    for f in &record.families {
        c.bytes(f.family.as_bytes());
        c.u32(f.active_write_version);
        c.u64(f.activated_epoch);
        c.i64(f.activated_unix_ns);
        c.bytes(f.activated_by.as_bytes());
        match &f.certificate {
            None => c.u8(0),
            Some(cert) => {
                c.u8(1);
                c.u32(cert.version);
                c.i64(cert.certified_unix_ns);
                c.u64(cert.member_count);
                c.i64(cert.newest_created_unix_ns);
                c.u64(cert.buckets_scanned);
                c.bytes(&cert.tenant_set_digest);
            }
        }
        c.i64(f.fence_complete_unix_ns);
        c.i64(f.late_writes_verified_through_unix_ns);
        c.u8(u8::from(f.contradicted));
    }
    c.u64(record.adopted.len() as u64);
    for a in &record.adopted {
        c.bytes(a.family.as_bytes());
        c.u32(a.version);
        c.u64(a.count);
    }
    c.0
}

fn seal(record: &mut sysproto::CompatRecord, keys: &CompatKeys) {
    record.mac = keys.mac(&claim(record)).as_bytes().to_vec();
}

fn check_version(got: u32) -> Result<(), CompatError> {
    if got > COMPAT_MAX_READ_VERSION {
        return Err(CompatError::CompatRecordNewer {
            key: COMPAT_KEY.to_string(),
            got,
            ceiling: COMPAT_MAX_READ_VERSION,
        });
    }
    if got < COMPAT_MIN_READ_VERSION {
        return Err(CompatError::VersionBelowFloor {
            key: COMPAT_KEY.to_string(),
            got,
            floor: COMPAT_MIN_READ_VERSION,
        });
    }
    Ok(())
}

fn validate(record: &sysproto::CompatRecord) -> Result<(), CompatError> {
    let mut seen = HashSet::new();
    for f in &record.families {
        if !is_valid_family_name(&f.family) {
            return Err(CompatError::corrupt(format!(
                "family name {:?} is not a family name",
                f.family
            )));
        }
        if !seen.insert(f.family.as_str()) {
            return Err(CompatError::corrupt(format!(
                "family {} appears more than once",
                f.family
            )));
        }
        if f.activated_epoch > record.compat_epoch {
            return Err(CompatError::corrupt(format!(
                "family {} was activated at epoch {}, above the record's compat_epoch {}",
                f.family, f.activated_epoch, record.compat_epoch
            )));
        }
    }
    for a in &record.adopted {
        if !is_valid_family_name(&a.family) {
            return Err(CompatError::corrupt(format!(
                "adopted family name {:?} is not a family name",
                a.family
            )));
        }
    }
    Ok(())
}

/// Decode and authenticate the bytes of `sys/compat`, in this order: decode,
/// version check (a newer record is refused before its MAC), MAC, then the
/// structural checks.
pub fn decode_compat(
    bytes: &[u8],
    keys: &CompatKeys,
) -> Result<sysproto::CompatRecord, CompatError> {
    let record = sysproto::CompatRecord::decode(bytes).map_err(|source| CompatError::Decode {
        key: COMPAT_KEY.to_string(),
        source,
    })?;
    check_version(record.format_version)?;
    if record.mac.is_empty() {
        return Err(CompatError::MissingMac {
            key: COMPAT_KEY.to_string(),
        });
    }
    let presented: [u8; 32] =
        record
            .mac
            .as_slice()
            .try_into()
            .map_err(|_| CompatError::BadMac {
                key: COMPAT_KEY.to_string(),
            })?;
    if !keys.verifies(&claim(&record), &blake3::Hash::from_bytes(presented)) {
        return Err(CompatError::BadMac {
            key: COMPAT_KEY.to_string(),
        });
    }
    validate(&record)?;
    Ok(record)
}

/// Read `sys/compat`. Absent is `Ok(None)`; a newer record is
/// [`CompatError::CompatRecordNewer`], never absent.
pub async fn read_compat(
    store: &dyn ObjectStoreBackend,
    keys: &CompatKeys,
) -> Result<Option<(sysproto::CompatRecord, Version)>, CompatError> {
    match store.get(COMPAT_KEY, GetRange::Full).await {
        Ok(outcome) => Ok(Some((
            decode_compat(outcome.data.as_ref(), keys)?,
            outcome.version,
        ))),
        Err(StoreError::NotFound) => Ok(None),
        Err(err) => Err(CompatError::store(err)),
    }
}

/// Create `sys/compat` with `CreateIfAbsent`. Stamps
/// [`COMPAT_FORMAT_VERSION`] and the MAC; an existing record, whatever its
/// version, is [`CompatError::AlreadyExists`] and is left untouched.
pub async fn create_compat(
    store: &dyn ObjectStoreBackend,
    keys: &CompatKeys,
    mut record: sysproto::CompatRecord,
) -> Result<(sysproto::CompatRecord, Version), CompatError> {
    record.format_version = COMPAT_FORMAT_VERSION;
    validate(&record)?;
    seal(&mut record, keys);
    match store
        .put(
            COMPAT_KEY,
            Bytes::from(record.encode_to_vec()),
            PutOptions::create_if_absent(),
        )
        .await
    {
        Ok(outcome) => Ok((record, outcome.version)),
        Err(StoreError::AlreadyExists) => Err(CompatError::AlreadyExists {
            key: COMPAT_KEY.to_string(),
        }),
        Err(err) => Err(CompatError::store(err)),
    }
}

/// Refuse `after` if it lowers anything `before` holds: the epoch, a family's
/// active version or activation epoch, a family's certificate version, or a
/// family entry altogether.
fn check_monotone(
    before: &sysproto::CompatRecord,
    after: &sysproto::CompatRecord,
) -> Result<(), CompatError> {
    let lower = |what: String| CompatError::WouldLower {
        key: COMPAT_KEY.to_string(),
        what,
    };
    if after.compat_epoch < before.compat_epoch {
        return Err(lower(format!(
            "compat_epoch from {} to {}",
            before.compat_epoch, after.compat_epoch
        )));
    }
    for old in &before.families {
        let Some(new) = after.families.iter().find(|f| f.family == old.family) else {
            return Err(lower(format!("family {} by removing it", old.family)));
        };
        if new.active_write_version < old.active_write_version {
            return Err(lower(format!(
                "family {} active_write_version from {} to {}",
                old.family, old.active_write_version, new.active_write_version
            )));
        }
        if new.activated_epoch < old.activated_epoch {
            return Err(lower(format!(
                "family {} activated_epoch from {} to {}",
                old.family, old.activated_epoch, new.activated_epoch
            )));
        }
        if let Some(old_cert) = &old.certificate {
            let new_version = new.certificate.as_ref().map(|c| c.version);
            if new_version.is_none_or(|v| v < old_cert.version) {
                return Err(lower(format!(
                    "family {} certificate from version {}",
                    old.family, old_cert.version
                )));
            }
        }
    }
    Ok(())
}

/// One CAS update of `sys/compat`: read it, apply `change`, refuse anything
/// that lowers a value, re-stamp and re-MAC, and replace it only if the store
/// version is still the one read. An absent record is
/// [`CompatError::RecordAbsent`], a newer one
/// [`CompatError::CompatRecordNewer`], and a lost race
/// [`CompatError::CasConflict`]; none of them writes anything.
pub async fn update_compat<F>(
    store: &dyn ObjectStoreBackend,
    keys: &CompatKeys,
    change: F,
) -> Result<(sysproto::CompatRecord, Version), CompatError>
where
    F: FnOnce(&mut sysproto::CompatRecord) -> Result<(), CompatError>,
{
    let Some((before, version)) = read_compat(store, keys).await? else {
        return Err(CompatError::RecordAbsent {
            key: COMPAT_KEY.to_string(),
        });
    };
    let mut after = before.clone();
    change(&mut after)?;
    after.format_version = COMPAT_FORMAT_VERSION;
    check_monotone(&before, &after)?;
    validate(&after)?;
    seal(&mut after, keys);
    let opts = PutOptions {
        mode: PutMode::CasVersion(version),
        checksum: None,
    };
    match store
        .put(COMPAT_KEY, Bytes::from(after.encode_to_vec()), opts)
        .await
    {
        Ok(outcome) => Ok((after, outcome.version)),
        Err(StoreError::PreconditionFailed) => Err(CompatError::CasConflict {
            key: COMPAT_KEY.to_string(),
        }),
        Err(err) => Err(CompatError::store(err)),
    }
}

/// Activate `version` for `family`: raise its `active_write_version` by
/// exactly one and the store-wide `compat_epoch` by one, recording the new
/// epoch, `now_ns` and `activated_by` in the family entry. Refuses an absent
/// record, a family without an entry, an equal or lower version, and a jump
/// of more than one. The certificate and fence fields are kept: the fence of
/// this activation is complete once `fence_complete_unix_ns` is at or after
/// `activated_unix_ns`.
pub async fn activate_family(
    store: &dyn ObjectStoreBackend,
    keys: &CompatKeys,
    family: &str,
    version: u32,
    activated_by: &str,
    now_ns: i64,
) -> Result<(sysproto::CompatRecord, Version), CompatError> {
    if !is_valid_family_name(family) {
        return Err(CompatError::FamilyInvalid {
            family: family.to_string(),
        });
    }
    update_compat(store, keys, |record| {
        let epoch =
            record
                .compat_epoch
                .checked_add(1)
                .ok_or_else(|| CompatError::EpochExhausted {
                    key: COMPAT_KEY.to_string(),
                })?;
        let entry = record
            .families
            .iter_mut()
            .find(|f| f.family == family)
            .ok_or_else(|| CompatError::FamilyNotInRecord {
                family: family.to_string(),
            })?;
        let active = entry.active_write_version;
        if version <= active {
            return Err(CompatError::VersionNotAboveActive {
                family: family.to_string(),
                active,
                requested: version,
            });
        }
        if active.checked_add(1) != Some(version) {
            return Err(CompatError::VersionJump {
                family: family.to_string(),
                active,
                requested: version,
            });
        }
        entry.active_write_version = version;
        entry.activated_epoch = epoch;
        entry.activated_unix_ns = now_ns;
        entry.activated_by = activated_by.to_string();
        record.compat_epoch = epoch;
        Ok(())
    })
    .await
}

/// The wall clock a [`CompatView`] stamps its reads with. Injected so tests
/// control time; the wall-clock implementation lives in the service layer.
pub trait CompatClock: Send + Sync {
    fn now_unix_ns(&self) -> i64;
}

/// Why a [`CompatView`] refuses a publish.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CompatRefusal {
    /// The last successful read of `sys/compat` is at least
    /// [`COMPAT_VIEW_HORIZON`] old, or there has been none. Only a blind view
    /// is exempt: one no refresh has ever succeeded for, whose every refresh
    /// error so far is an `AccessDenied`, writing a bucket that carries no
    /// `compat_epoch`. It answers the baseline (ADR-4233 decision 1).
    #[error(
        "{COMPAT_KEY} view is stale: last successful read {last_read_unix_ns:?}, now {now_unix_ns}"
    )]
    ViewStale {
        last_read_unix_ns: Option<i64>,
        now_unix_ns: i64,
    },
    /// This build cannot write the family's active version.
    #[error(
        "family {family}: active write version {active} is not one this build writes ({writable:?})"
    )]
    ActiveVersionNotWritable {
        family: String,
        active: u32,
        writable: Vec<u16>,
    },
    /// `sys/compat` is absent although an activation is on record: the
    /// bucket's provisioning record carries a `compat_epoch`, or this view
    /// has seen one. The record is restored from bucket versioning, never
    /// re-created at the baseline.
    #[error(
        "{COMPAT_KEY} is absent but an activation is on record (bucket compat_epoch {bucket_compat_epoch}, seen compat_epoch {seen_compat_epoch})"
    )]
    CompatRecordMissing {
        bucket_compat_epoch: u64,
        seen_compat_epoch: u64,
    },
}

impl CompatRefusal {
    /// Staleness and an unwritable version clear on a later refresh or a
    /// rollout; a missing record needs an operator restore.
    pub fn is_retryable(&self) -> bool {
        !matches!(self, CompatRefusal::CompatRecordMissing { .. })
    }
}

/// The highest values a [`CompatView`] has seen for one family in this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeenFamily {
    pub active_write_version: u32,
    pub activated_epoch: u64,
    /// The certificate with the highest certified version seen.
    pub certificate: Option<sysproto::Certificate>,
}

/// A family whose read value was below the value the view had already seen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RolledBackFamily {
    pub family: String,
    pub seen: u32,
    pub read: u32,
}

/// What one [`CompatView::refresh`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatRefresh {
    /// Whether `sys/compat` exists.
    pub present: bool,
    /// The record's `compat_epoch`, if present.
    pub read_epoch: Option<u64>,
    /// The highest epoch the view has seen, after this read.
    pub seen_epoch: u64,
    /// Families whose read version was below the seen one. The view kept the
    /// seen version.
    pub rolled_back: Vec<RolledBackFamily>,
}

#[derive(Debug, Default)]
struct ViewState {
    last_read_unix_ns: Option<i64>,
    /// The start ticket of the read `present` and `last_record` come from.
    /// A read that started earlier never replaces them.
    merged_ticket: Option<u64>,
    present: bool,
    /// Whether any GET of `sys/compat` in this run returned an object, read
    /// or not: a decoded record, or one this build refused to decode.
    record_seen: bool,
    /// Whether any refresh in this run failed with a store error other than
    /// `AccessDenied`. Such a failure says nothing about the grant, so it
    /// ends the blind-view exemption.
    refresh_failed_not_denied: bool,
    last_record: Option<sysproto::CompatRecord>,
    seen_epoch: u64,
    families: BTreeMap<String, SeenFamily>,
}

impl ViewState {
    fn merge(
        &mut self,
        record: Option<sysproto::CompatRecord>,
        ticket: u64,
        read_unix_ns: i64,
    ) -> CompatRefresh {
        let mut rolled_back = Vec::new();
        let read_epoch = record.as_ref().map(|r| r.compat_epoch);
        if let Some(record) = &record {
            self.record_seen = true;
            self.seen_epoch = self.seen_epoch.max(record.compat_epoch);
            for f in &record.families {
                let read_cert = f.certificate.clone();
                match self.families.get_mut(&f.family) {
                    None => {
                        self.families.insert(
                            f.family.clone(),
                            SeenFamily {
                                active_write_version: f.active_write_version,
                                activated_epoch: f.activated_epoch,
                                certificate: read_cert,
                            },
                        );
                    }
                    Some(seen) => {
                        if f.active_write_version < seen.active_write_version {
                            rolled_back.push(RolledBackFamily {
                                family: f.family.clone(),
                                seen: seen.active_write_version,
                                read: f.active_write_version,
                            });
                        } else if f.active_write_version > seen.active_write_version {
                            seen.active_write_version = f.active_write_version;
                        }
                        seen.activated_epoch = seen.activated_epoch.max(f.activated_epoch);
                        if let Some(cert) = read_cert {
                            let higher = seen
                                .certificate
                                .as_ref()
                                .is_none_or(|old| cert.version > old.version);
                            if higher {
                                seen.certificate = Some(cert);
                            }
                        }
                    }
                }
            }
        }
        if self.merged_ticket.is_none_or(|merged| ticket > merged) {
            self.merged_ticket = Some(ticket);
            self.present = record.is_some();
            self.last_record = record;
        }
        self.last_read_unix_ns = Some(
            self.last_read_unix_ns
                .map_or(read_unix_ns, |last| last.max(read_unix_ns)),
        );
        CompatRefresh {
            present: self.present,
            read_epoch,
            seen_epoch: self.seen_epoch,
            rolled_back,
        }
    }
}

struct ViewInner {
    store: Arc<dyn ObjectStoreBackend>,
    keys: CompatKeys,
    clock: Arc<dyn CompatClock>,
    /// Orders reads by when they started, so a slow read that finishes after
    /// a later one does not replace the later one's record.
    next_ticket: AtomicU64,
    state: RwLock<ViewState>,
}

/// A process's monotone view of `sys/compat`. Clones share one view.
///
/// [`CompatView::refresh`] reads the record and merges it in without ever
/// lowering a seen version, epoch or certificate. [`CompatView::write_policy`]
/// answers which version a writer emits for a family, or why it must not
/// publish. Nothing here spawns a task; the owner calls `refresh` at startup
/// and every [`DEFAULT_COMPAT_REFRESH_INTERVAL`].
#[derive(Clone)]
pub struct CompatView {
    inner: Arc<ViewInner>,
}

impl std::fmt::Debug for CompatView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.inner.state.read();
        f.debug_struct("CompatView")
            .field("last_read_unix_ns", &state.last_read_unix_ns)
            .field("present", &state.present)
            .field("seen_epoch", &state.seen_epoch)
            .field("families", &state.families)
            .finish()
    }
}

impl CompatView {
    /// A view that has read nothing yet. Until a refresh finds a record,
    /// `write_policy` answers as for an absent one: the baseline, or, on a
    /// bucket whose provisioning record carries a `compat_epoch`,
    /// [`CompatRefusal::ViewStale`] before the first successful read and
    /// [`CompatRefusal::CompatRecordMissing`] after it. A refresh that fails
    /// with anything but `AccessDenied` before the first successful read
    /// makes the view stale for every bucket until a read succeeds.
    pub fn new(
        store: Arc<dyn ObjectStoreBackend>,
        keys: CompatKeys,
        clock: Arc<dyn CompatClock>,
    ) -> Self {
        CompatView {
            inner: Arc::new(ViewInner {
                store,
                keys,
                clock,
                next_ticket: AtomicU64::new(0),
                state: RwLock::new(ViewState::default()),
            }),
        }
    }

    /// Read `sys/compat` and merge it in. The read is stamped with the clock
    /// before the GET, so staleness is never understated. A failed read
    /// keeps the last successful read and its age. One that failed after the
    /// GET returned an object (a newer, unauthenticated or corrupt record)
    /// still marks a record as seen, so the view refuses once it goes stale.
    /// A store error other than `AccessDenied` ends the blind-view exemption
    /// for the rest of the run.
    /// Overlapping refreshes merge every monotone value they read, but the
    /// record and its presence come from the read that started last.
    pub async fn refresh(&self) -> Result<CompatRefresh, CompatError> {
        let ticket = self.inner.next_ticket.fetch_add(1, AtomicOrdering::SeqCst);
        let started = self.inner.clock.now_unix_ns();
        let read = match read_compat(self.inner.store.as_ref(), &self.inner.keys).await {
            Ok(read) => read,
            Err(err) => {
                if !matches!(err, CompatError::Store { .. }) {
                    self.inner.state.write().record_seen = true;
                } else if !err.is_access_denied() {
                    self.inner.state.write().refresh_failed_not_denied = true;
                }
                return Err(err);
            }
        };
        let mut state = self.inner.state.write();
        Ok(state.merge(read.map(|(record, _)| record), ticket, started))
    }

    /// The version a writer of `family` emits at `now_unix_ns`, given the
    /// versions its build writes. `baseline` is the family's baseline version,
    /// used when the record is absent or has no entry for the family;
    /// `bucket` is the provisioning record of the bucket being written, if
    /// any, whose `compat_epoch` is evidence that an activation happened.
    ///
    /// A view past [`COMPAT_VIEW_HORIZON`] refuses with
    /// [`CompatRefusal::ViewStale`] unless it is blind: no refresh has ever
    /// succeeded, every refresh error so far is an `AccessDenied` from a
    /// credential without the grant, and the bucket carries no
    /// `compat_epoch`. A blind view answers the baseline.
    pub fn write_policy(
        &self,
        now_unix_ns: i64,
        family: &str,
        writable: &[u16],
        baseline: u16,
        bucket: Option<&sysproto::ProvisioningRecord>,
    ) -> Result<u16, CompatRefusal> {
        let bucket_compat_epoch = bucket.map_or(0, |b| b.compat_epoch);
        self.check_fresh(now_unix_ns, bucket_compat_epoch)?;
        let state = self.inner.state.read();
        if !state.present && (bucket_compat_epoch > 0 || state.seen_epoch > 0) {
            return Err(CompatRefusal::CompatRecordMissing {
                bucket_compat_epoch,
                seen_compat_epoch: state.seen_epoch,
            });
        }
        let active = state
            .families
            .get(family)
            .map_or(u32::from(baseline), |seen| seen.active_write_version);
        match u16::try_from(active) {
            Ok(v) if writable.contains(&v) => Ok(v),
            _ => Err(CompatRefusal::ActiveVersionNotWritable {
                family: family.to_string(),
                active,
                writable: writable.to_vec(),
            }),
        }
    }

    /// Whether the view is fresh enough to publish under at `now_unix_ns`,
    /// the freshness rule [`Self::write_policy`] applies, for writers that
    /// recheck it before every PUT. [`CompatRefusal::ViewStale`] when the
    /// last successful read is at least [`COMPAT_VIEW_HORIZON_NS`] old, or
    /// there has been none, unless the view is blind: no refresh has
    /// succeeded, every refresh error so far is an `AccessDenied`, no object
    /// was seen, and `bucket_compat_epoch` is 0.
    pub fn check_fresh(
        &self,
        now_unix_ns: i64,
        bucket_compat_epoch: u64,
    ) -> Result<(), CompatRefusal> {
        let state = self.inner.state.read();
        let fresh = state
            .last_read_unix_ns
            .is_some_and(|last| now_unix_ns.saturating_sub(last) < COMPAT_VIEW_HORIZON_NS);
        let blind = state.last_read_unix_ns.is_none()
            && !state.refresh_failed_not_denied
            && !state.record_seen;
        if !fresh && !(blind && bucket_compat_epoch == 0) {
            return Err(CompatRefusal::ViewStale {
                last_read_unix_ns: state.last_read_unix_ns,
                now_unix_ns,
            });
        }
        Ok(())
    }

    /// The highest values seen for `family`, if any read carried it.
    pub fn seen_family(&self, family: &str) -> Option<SeenFamily> {
        self.inner.state.read().families.get(family).cloned()
    }

    /// The highest `compat_epoch` seen in this run.
    pub fn seen_epoch(&self) -> u64 {
        self.inner.state.read().seen_epoch
    }

    /// The record as last read, `None` if it was absent or never read.
    pub fn last_record(&self) -> Option<sysproto::CompatRecord> {
        self.inner.state.read().last_record.clone()
    }

    /// Whether any refresh in this run found an object at `sys/compat`.
    pub fn record_seen(&self) -> bool {
        self.inner.state.read().record_seen
    }

    /// When the last successful read started.
    pub fn last_read_unix_ns(&self) -> Option<i64> {
        self.inner.state.read().last_read_unix_ns
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault, Sequence,
    };
    use ravel_object_store::memory::MemoryStore;
    use std::sync::atomic::{AtomicI64, Ordering};

    const T0: i64 = 1_800_000_000_000_000_000;

    #[derive(Debug)]
    struct ManualClock(AtomicI64);

    impl ManualClock {
        fn at(ns: i64) -> Arc<Self> {
            Arc::new(ManualClock(AtomicI64::new(ns)))
        }
        fn set(&self, ns: i64) {
            self.0.store(ns, Ordering::SeqCst);
        }
    }

    impl CompatClock for ManualClock {
        fn now_unix_ns(&self) -> i64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn keys() -> CompatKeys {
        CompatKeys::from_file_keys(&[[7; 32]]).expect("one key")
    }

    fn family(name: &str, version: u32) -> sysproto::CompatFamily {
        sysproto::CompatFamily {
            family: name.to_string(),
            active_write_version: version,
            ..Default::default()
        }
    }

    fn baseline_record() -> sysproto::CompatRecord {
        sysproto::CompatRecord {
            format_version: COMPAT_FORMAT_VERSION,
            baseline_release_major: 0,
            baseline_release_minor: 34,
            families: vec![
                family(FAMILY_RSEG, 7),
                family(FAMILY_RLOG, 5),
                family(FAMILY_RSPAN, 5),
            ],
            max_publish_lifetime_ns: 3_600_000_000_000,
            ..Default::default()
        }
    }

    /// Write `record` over whatever is at `sys/compat`, sealed under `keys`,
    /// bypassing every helper's checks: how a rollback or a newer release's
    /// write looks to this build.
    async fn put_raw(store: &dyn ObjectStoreBackend, mut record: sysproto::CompatRecord) {
        seal(&mut record, &keys());
        store
            .put(
                COMPAT_KEY,
                Bytes::from(record.encode_to_vec()),
                PutOptions::default(),
            )
            .await
            .expect("raw put");
    }

    async fn stored_bytes(store: &dyn ObjectStoreBackend) -> (Bytes, Version) {
        let out = store
            .get(COMPAT_KEY, GetRange::Full)
            .await
            .expect("record present");
        (out.data, out.version)
    }

    fn view(store: Arc<dyn ObjectStoreBackend>, clock: Arc<ManualClock>) -> CompatView {
        CompatView::new(store, keys(), clock)
    }

    #[test]
    fn constants_match_the_adr() {
        assert_eq!(COMPAT_KEY, "sys/compat");
        assert_eq!(COMPAT_VIEW_HORIZON, Duration::from_secs(600));
        assert_eq!(
            i128::from(COMPAT_VIEW_HORIZON_NS),
            i128::try_from(COMPAT_VIEW_HORIZON.as_nanos()).expect("fits")
        );
        assert_eq!(DEFAULT_COMPAT_REFRESH_INTERVAL, Duration::from_secs(60));
        assert_eq!(
            (
                COMPAT_FORMAT_VERSION,
                COMPAT_MIN_READ_VERSION,
                COMPAT_MAX_READ_VERSION
            ),
            (1, 1, 1)
        );
        for name in KNOWN_FAMILIES {
            assert!(is_valid_family_name(name), "{name}");
        }
        assert!(!is_valid_family_name("RSEG"));
        assert!(!is_valid_family_name(""));
    }

    #[tokio::test]
    async fn stale_view_past_h_refuses_and_never_lowers_a_seen_version() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut at_six = baseline_record();
        at_six.compat_epoch = 1;
        at_six.families[0] = sysproto::CompatFamily {
            activated_epoch: 1,
            ..family(FAMILY_RSEG, 6)
        };
        create_compat(store.as_ref(), &keys(), at_six)
            .await
            .expect("create");
        let clock = ManualClock::at(T0);
        let view = view(store.clone(), clock.clone());

        // Never read and no record seen: the baseline, unless the bucket
        // carries a compat_epoch.
        assert_eq!(view.write_policy(T0, FAMILY_RSEG, &[5, 6], 5, None), Ok(5));
        let fenced = sysproto::ProvisioningRecord {
            compat_epoch: 1,
            ..Default::default()
        };
        assert!(matches!(
            view.write_policy(T0, FAMILY_RSEG, &[5, 6], 5, Some(&fenced)),
            Err(CompatRefusal::ViewStale {
                last_read_unix_ns: None,
                ..
            })
        ));

        view.refresh().await.expect("refresh");
        assert_eq!(
            view.write_policy(
                T0 + COMPAT_VIEW_HORIZON_NS - 1,
                FAMILY_RSEG,
                &[5, 6],
                5,
                None
            ),
            Ok(6)
        );
        assert_eq!(
            view.write_policy(T0 + COMPAT_VIEW_HORIZON_NS, FAMILY_RSEG, &[5, 6], 5, None),
            Err(CompatRefusal::ViewStale {
                last_read_unix_ns: Some(T0),
                now_unix_ns: T0 + COMPAT_VIEW_HORIZON_NS,
            })
        );

        // A rolled-back record reads 5; the view keeps the 6 it has seen.
        let mut at_five = baseline_record();
        at_five.families[0] = family(FAMILY_RSEG, 5);
        put_raw(store.as_ref(), at_five).await;
        let t1 = T0 + COMPAT_VIEW_HORIZON_NS;
        clock.set(t1);
        let refresh = view.refresh().await.expect("refresh");
        assert_eq!(
            refresh.rolled_back,
            vec![RolledBackFamily {
                family: FAMILY_RSEG.to_string(),
                seen: 6,
                read: 5,
            }]
        );
        assert_eq!(refresh.read_epoch, Some(0));
        assert_eq!(refresh.seen_epoch, 1);
        assert_eq!(view.write_policy(t1, FAMILY_RSEG, &[5, 6], 5, None), Ok(6));
        assert_eq!(
            view.write_policy(t1, FAMILY_RSEG, &[5], 5, None),
            Err(CompatRefusal::ActiveVersionNotWritable {
                family: FAMILY_RSEG.to_string(),
                active: 6,
                writable: vec![5],
            })
        );
        let seen = view.seen_family(FAMILY_RSEG).expect("seen");
        assert_eq!((seen.active_write_version, seen.activated_epoch), (6, 1));
        assert_eq!(view.seen_epoch(), 1);

        // A clone shares the view.
        let clone = view.clone();
        assert_eq!(clone.write_policy(t1, FAMILY_RSEG, &[5, 6], 5, None), Ok(6));
    }

    #[tokio::test]
    async fn failed_refresh_keeps_the_last_read_and_its_age() {
        let inner = MemoryStore::new();
        create_compat(&inner, &keys(), baseline_record())
            .await
            .expect("create");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("down".to_string()))
                .with_key_contains(COMPAT_KEY)
                .with_occurrence(ravel_object_store::fault::Occurrence::Nth(2)),
        );
        let store = Arc::new(FaultStore::new(inner, plan));
        let clock = ManualClock::at(T0);
        let view = view(store.clone(), clock.clone());
        view.refresh().await.expect("first refresh");
        clock.set(T0 + 1_000);
        view.refresh().await.expect_err("second refresh fails");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
        assert_eq!(view.last_read_unix_ns(), Some(T0));
        assert!(matches!(
            view.write_policy(T0 + COMPAT_VIEW_HORIZON_NS, FAMILY_RSEG, &[7], 7, None),
            Err(CompatRefusal::ViewStale { .. })
        ));
    }

    /// Answers every `get` of `sys/compat` with `AccessDenied` while `deny`
    /// is set, as AWS S3 does for a credential without the GET grant or, on an
    /// absent key, without a list grant covering it.
    struct DeniedCompatGet {
        inner: MemoryStore,
        deny: std::sync::atomic::AtomicBool,
        denied: std::sync::atomic::AtomicUsize,
    }

    impl DeniedCompatGet {
        fn new(deny: bool) -> Arc<Self> {
            Arc::new(Self::unshared(deny))
        }

        fn unshared(deny: bool) -> Self {
            DeniedCompatGet {
                inner: MemoryStore::new(),
                deny: std::sync::atomic::AtomicBool::new(deny),
                denied: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for DeniedCompatGet {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }
        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            if key == COMPAT_KEY && self.deny.load(Ordering::SeqCst) {
                self.denied.fetch_add(1, Ordering::SeqCst);
                return Err(StoreError::AccessDenied("no s3:GetObject".to_string()));
            }
            self.inner.get(key, range).await
        }
        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }
        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list_after(prefix, start_after, page).await
        }
        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// A never-activated store whose `sys/compat` reads are refused with
    /// `AccessDenied` from the start: no record is ever seen and no refresh
    /// fails any other way, so the view stays blind and answers the baseline
    /// at and past the horizon, however far past, and refuses only a bucket
    /// whose provisioning record carries a `compat_epoch`.
    #[tokio::test]
    async fn access_denied_on_a_never_activated_store_answers_the_baseline() {
        let denied = DeniedCompatGet::new(true);
        let store: Arc<dyn ObjectStoreBackend> = denied.clone();
        let clock = ManualClock::at(T0);
        let view = view(store, clock.clone());
        for at in [
            T0,
            T0 + COMPAT_VIEW_HORIZON_NS,
            T0 + 1_000 * COMPAT_VIEW_HORIZON_NS,
        ] {
            clock.set(at);
            let err = view.refresh().await.expect_err("denied");
            assert!(
                matches!(
                    err,
                    CompatError::Store {
                        source: StoreError::AccessDenied(_),
                        ..
                    }
                ),
                "got: {err}"
            );
            assert!(err.is_access_denied());
            assert_eq!(view.write_policy(at, FAMILY_RSEG, &[7], 7, None), Ok(7));
        }
        assert_eq!(denied.denied.load(Ordering::SeqCst), 3);
        assert!(!view.record_seen());
        assert_eq!(view.last_read_unix_ns(), None);
        let fenced = sysproto::ProvisioningRecord {
            compat_epoch: 1,
            ..Default::default()
        };
        assert!(matches!(
            view.write_policy(T0, FAMILY_RSEG, &[7], 7, Some(&fenced)),
            Err(CompatRefusal::ViewStale {
                last_read_unix_ns: None,
                ..
            })
        ));
        assert_eq!(view.check_fresh(T0 + 3 * COMPAT_VIEW_HORIZON_NS, 0), Ok(()));
        assert!(matches!(
            view.check_fresh(T0, 1),
            Err(CompatRefusal::ViewStale {
                last_read_unix_ns: None,
                ..
            })
        ));
    }

    /// Once a record has been read, the same `AccessDenied` refusals leave
    /// the view at its last read until the horizon, and refuse from there.
    #[tokio::test]
    async fn access_denied_after_a_seen_record_refuses_past_the_horizon() {
        let denied = DeniedCompatGet::new(false);
        let store: Arc<dyn ObjectStoreBackend> = denied.clone();
        create_compat(store.as_ref(), &keys(), baseline_record())
            .await
            .expect("create");
        let clock = ManualClock::at(T0);
        let view = view(store, clock.clone());
        view.refresh().await.expect("first refresh");
        assert!(view.record_seen());
        denied.deny.store(true, Ordering::SeqCst);
        clock.set(T0 + COMPAT_VIEW_HORIZON_NS - 1);
        view.refresh().await.expect_err("denied");
        assert_eq!(
            view.write_policy(T0 + COMPAT_VIEW_HORIZON_NS - 1, FAMILY_RSEG, &[7], 7, None),
            Ok(7)
        );
        clock.set(T0 + COMPAT_VIEW_HORIZON_NS);
        view.refresh().await.expect_err("denied");
        assert_eq!(denied.denied.load(Ordering::SeqCst), 2);
        assert_eq!(
            view.write_policy(T0 + COMPAT_VIEW_HORIZON_NS, FAMILY_RSEG, &[7], 7, None),
            Err(CompatRefusal::ViewStale {
                last_read_unix_ns: Some(T0),
                now_unix_ns: T0 + COMPAT_VIEW_HORIZON_NS,
            })
        );
        for bucket_compat_epoch in [0, 1] {
            assert_eq!(
                view.check_fresh(T0 + COMPAT_VIEW_HORIZON_NS - 1, bucket_compat_epoch),
                Ok(())
            );
            assert_eq!(
                view.check_fresh(T0 + COMPAT_VIEW_HORIZON_NS, bucket_compat_epoch),
                Err(CompatRefusal::ViewStale {
                    last_read_unix_ns: Some(T0),
                    now_unix_ns: T0 + COMPAT_VIEW_HORIZON_NS,
                })
            );
        }
    }

    fn activated_record() -> sysproto::CompatRecord {
        let mut record = baseline_record();
        record.compat_epoch = 1;
        record.families[0] = sysproto::CompatFamily {
            activated_epoch: 1,
            ..family(FAMILY_RSEG, 8)
        };
        record
    }

    /// A view no read has ever succeeded for has no read inside the horizon,
    /// so it is already past it. Once a refresh fails with anything but
    /// `AccessDenied` (a 5xx, a timeout, throttling) the failure says nothing
    /// about a missing grant, and the view refuses at once and from then on,
    /// on a bucket with no `compat_epoch` too. Here the store holds an
    /// activated record the view never got to read: answering the baseline
    /// would publish version 7 under an activation of 8.
    #[tokio::test]
    async fn a_never_read_view_with_a_non_access_denied_error_goes_stale_after_the_horizon() {
        let faults = [
            ScriptedFault::Transient("503 Service Unavailable".to_string()),
            ScriptedFault::Permanent("500 Internal Error".to_string()),
            ScriptedFault::Timeout,
            ScriptedFault::Throttled { retry_after_ms: 1 },
        ];
        for fault in faults {
            let kind = fault.kind();
            let inner = MemoryStore::new();
            create_compat(&inner, &keys(), activated_record())
                .await
                .expect("create");
            let plan = FaultPlan::empty()
                .with_rule(Rule::new(Op::Get, fault).with_key_contains(COMPAT_KEY));
            let store = Arc::new(FaultStore::new(inner, plan));
            let clock = ManualClock::at(T0);
            let view = view(store.clone(), clock.clone());
            let err = view.refresh().await.expect_err("refresh fails");
            assert!(!err.is_access_denied(), "{kind:?}: {err}");
            assert_eq!(store.fault_count(Op::Get, kind), 1, "{kind:?}");
            for at in [
                T0,
                T0 + COMPAT_VIEW_HORIZON_NS,
                T0 + 3 * COMPAT_VIEW_HORIZON_NS,
            ] {
                assert_eq!(
                    view.check_fresh(at, 0),
                    Err(CompatRefusal::ViewStale {
                        last_read_unix_ns: None,
                        now_unix_ns: at,
                    }),
                    "{kind:?} at {at}"
                );
                assert_eq!(
                    view.write_policy(at, FAMILY_RSEG, &[7, 8], 7, None),
                    Err(CompatRefusal::ViewStale {
                        last_read_unix_ns: None,
                        now_unix_ns: at,
                    }),
                    "{kind:?} at {at}"
                );
            }
            assert!(!view.record_seen());
            assert_eq!(view.last_read_unix_ns(), None);
        }
    }

    /// A read that finds no `sys/compat` is a successful read: the view is
    /// no longer blind, and an outage after it is bounded by the horizon
    /// like any other.
    #[tokio::test]
    async fn a_not_found_read_then_an_outage_goes_stale_after_the_horizon() {
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(COMPAT_KEY)
                .then_passthrough()
                .then_fault(ScriptedFault::Transient("503".to_string()))
                .then_fault(ScriptedFault::Transient("503".to_string())),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let clock = ManualClock::at(T0);
        let view = view(store.clone(), clock.clone());
        let read = view.refresh().await.expect("absent record reads");
        assert!(!read.present);
        assert!(!view.record_seen());
        assert_eq!(view.last_read_unix_ns(), Some(T0));

        clock.set(T0 + COMPAT_VIEW_HORIZON_NS - 1);
        view.refresh().await.expect_err("outage");
        assert_eq!(
            view.write_policy(T0 + COMPAT_VIEW_HORIZON_NS - 1, FAMILY_RSEG, &[7], 7, None),
            Ok(7)
        );
        clock.set(T0 + COMPAT_VIEW_HORIZON_NS);
        view.refresh().await.expect_err("outage");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 2);
        for at in [T0 + COMPAT_VIEW_HORIZON_NS, T0 + 3 * COMPAT_VIEW_HORIZON_NS] {
            assert_eq!(
                view.check_fresh(at, 0),
                Err(CompatRefusal::ViewStale {
                    last_read_unix_ns: Some(T0),
                    now_unix_ns: at,
                })
            );
            assert_eq!(
                view.write_policy(at, FAMILY_RSEG, &[7], 7, None),
                Err(CompatRefusal::ViewStale {
                    last_read_unix_ns: Some(T0),
                    now_unix_ns: at,
                })
            );
        }
    }

    /// A blind view keeps its exemption only while every error is an
    /// `AccessDenied`. A 5xx after one ends it: the view has never read, so it
    /// is past the horizon and refuses, and a later `AccessDenied` does not
    /// restore the exemption.
    #[tokio::test]
    async fn access_denied_then_a_5xx_goes_stale_after_the_horizon() {
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains(COMPAT_KEY)
                .then_passthrough()
                .then_fault(ScriptedFault::Transient("503".to_string())),
        );
        let store = Arc::new(FaultStore::new(DeniedCompatGet::unshared(true), plan));
        let clock = ManualClock::at(T0);
        let view = view(store.clone(), clock.clone());

        let err = view.refresh().await.expect_err("denied");
        assert!(err.is_access_denied(), "{err}");
        assert_eq!(view.write_policy(T0, FAMILY_RSEG, &[7], 7, None), Ok(7));

        clock.set(T0 + 1);
        let err = view.refresh().await.expect_err("5xx");
        assert!(!err.is_access_denied(), "{err}");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);

        clock.set(T0 + 2);
        let err = view.refresh().await.expect_err("denied again");
        assert!(err.is_access_denied(), "{err}");
        assert_eq!(store.inner().denied.load(Ordering::SeqCst), 2);

        for at in [
            T0 + 2,
            T0 + COMPAT_VIEW_HORIZON_NS,
            T0 + 3 * COMPAT_VIEW_HORIZON_NS,
        ] {
            assert_eq!(
                view.write_policy(at, FAMILY_RSEG, &[7], 7, None),
                Err(CompatRefusal::ViewStale {
                    last_read_unix_ns: None,
                    now_unix_ns: at,
                })
            );
            assert_eq!(
                view.check_fresh(at, 0),
                Err(CompatRefusal::ViewStale {
                    last_read_unix_ns: None,
                    now_unix_ns: at,
                })
            );
        }
    }

    /// Holds the first `get` of `sys/compat` after the wrapped read returned,
    /// until released: a slow read that finishes after a later one.
    struct HeldFirstGet {
        inner: MemoryStore,
        gets: std::sync::atomic::AtomicUsize,
        read_done: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for HeldFirstGet {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }
        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            let out = self.inner.get(key, range).await;
            if self.gets.fetch_add(1, Ordering::SeqCst) == 0 {
                self.read_done.notify_one();
                self.release.notified().await;
            }
            out
        }
        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }
        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }
        async fn list_after(
            &self,
            prefix: &str,
            start_after: Option<&str>,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list_after(prefix, start_after, page).await
        }
        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }
        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }
        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// Two overlapping refreshes: the one that started first read the record
    /// before an activation and finishes last. The record and its presence
    /// stay those of the later-started read; the monotone values never drop.
    #[tokio::test]
    async fn an_earlier_started_read_never_replaces_a_later_one() {
        let held = Arc::new(HeldFirstGet {
            inner: MemoryStore::new(),
            gets: std::sync::atomic::AtomicUsize::new(0),
            read_done: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let store: Arc<dyn ObjectStoreBackend> = held.clone();
        // The slow read sees no record at all.
        let clock = ManualClock::at(T0);
        let view = view(store.clone(), clock.clone());
        let slow = {
            let view = view.clone();
            tokio::spawn(async move { view.refresh().await })
        };
        held.read_done.notified().await;

        create_compat(store.as_ref(), &keys(), baseline_record())
            .await
            .expect("create");
        activate_family(store.as_ref(), &keys(), FAMILY_RLOG, 6, "test", T0)
            .await
            .expect("activate");
        clock.set(T0 + 1_000);
        let fast = view.refresh().await.expect("later read");
        assert!(fast.present);
        assert_eq!(fast.seen_epoch, 1);

        held.release.notify_one();
        let slow = slow.await.expect("join").expect("earlier read");
        assert_eq!(slow.read_epoch, None, "the slow read saw no record");
        assert!(slow.present, "presence is the later read's");
        let record = view.last_record().expect("the later read's record");
        assert_eq!(record.compat_epoch, 1);
        assert_eq!(view.seen_epoch(), 1);
        assert_eq!(view.last_read_unix_ns(), Some(T0 + 1_000));
        assert_eq!(
            view.write_policy(T0 + 2_000, FAMILY_RLOG, &[5, 6], 5, None),
            Ok(6)
        );
    }

    #[tokio::test]
    async fn refresh_never_lowers_a_certificate_or_the_epoch() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let cert = |version| sysproto::Certificate {
            version,
            certified_unix_ns: T0,
            ..Default::default()
        };
        let mut high = baseline_record();
        high.compat_epoch = 3;
        high.families[0].certificate = Some(cert(7));
        put_raw(store.as_ref(), high).await;
        let view = view(store.clone(), ManualClock::at(T0));
        view.refresh().await.expect("refresh");

        let mut low = baseline_record();
        low.compat_epoch = 1;
        low.families[0].certificate = Some(cert(6));
        put_raw(store.as_ref(), low).await;
        view.refresh().await.expect("refresh");
        let seen = view.seen_family(FAMILY_RSEG).expect("seen");
        assert_eq!(seen.certificate.map(|c| c.version), Some(7));
        assert_eq!(view.seen_epoch(), 3);

        put_raw(store.as_ref(), baseline_record()).await;
        view.refresh().await.expect("refresh");
        let seen = view.seen_family(FAMILY_RSEG).expect("seen");
        assert_eq!(seen.certificate.map(|c| c.version), Some(7));
    }

    #[tokio::test]
    async fn newer_compat_record_is_typed_and_never_overwritten() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut newer = baseline_record();
        newer.format_version = COMPAT_MAX_READ_VERSION + 1;
        put_raw(store.as_ref(), newer).await;
        let before = stored_bytes(store.as_ref()).await;

        let newer_err = |err: &CompatError| {
            matches!(
                err,
                CompatError::CompatRecordNewer { got, ceiling, .. }
                    if *got == COMPAT_MAX_READ_VERSION + 1 && *ceiling == COMPAT_MAX_READ_VERSION
            )
        };
        let err = read_compat(store.as_ref(), &keys())
            .await
            .expect_err("a newer record is not absent");
        assert!(newer_err(&err), "got: {err}");

        let err = update_compat(store.as_ref(), &keys(), |_| Ok(()))
            .await
            .expect_err("update refuses");
        assert!(newer_err(&err), "got: {err}");

        let err = activate_family(store.as_ref(), &keys(), FAMILY_RSEG, 8, "test", T0)
            .await
            .expect_err("activation refuses");
        assert!(newer_err(&err), "got: {err}");

        let err = create_compat(store.as_ref(), &keys(), baseline_record())
            .await
            .expect_err("create refuses");
        assert!(
            matches!(err, CompatError::AlreadyExists { .. }),
            "got: {err}"
        );

        let view = view(store.clone(), ManualClock::at(T0));
        let err = view.refresh().await.expect_err("refresh refuses");
        assert!(newer_err(&err), "got: {err}");
        assert!(view.record_seen(), "a newer record is a record seen");
        assert!(matches!(
            view.write_policy(T0, FAMILY_RSEG, &[7], 7, None),
            Err(CompatRefusal::ViewStale { .. })
        ));

        let after = stored_bytes(store.as_ref()).await;
        assert_eq!(before.0, after.0, "the newer record's bytes are unchanged");
        assert_eq!(before.1, after.1, "the newer record's version is unchanged");
    }

    #[tokio::test]
    async fn activation_raises_by_exactly_one_and_bumps_the_epoch() {
        let store = MemoryStore::new();
        let err = activate_family(&store, &keys(), FAMILY_RSEG, 8, "op", T0)
            .await
            .expect_err("absent record");
        assert!(
            matches!(err, CompatError::RecordAbsent { .. }),
            "got: {err}"
        );

        create_compat(&store, &keys(), baseline_record())
            .await
            .expect("create");
        let (record, _) = activate_family(&store, &keys(), FAMILY_RSEG, 8, "op-a", T0)
            .await
            .expect("activate rseg 8");
        assert_eq!(record.compat_epoch, 1);
        let rseg = &record.families[0];
        assert_eq!(
            (
                rseg.family.as_str(),
                rseg.active_write_version,
                rseg.activated_epoch,
                rseg.activated_unix_ns,
                rseg.activated_by.as_str()
            ),
            (FAMILY_RSEG, 8, 1, T0, "op-a")
        );
        assert_eq!(record.families[1], family(FAMILY_RLOG, 5));
        assert_eq!(record.families[2], family(FAMILY_RSPAN, 5));
        let (read, _) = read_compat(&store, &keys())
            .await
            .expect("read")
            .expect("present");
        assert_eq!(read, record);

        let before = stored_bytes(&store).await;
        for (version, equal_or_lower) in [(8, true), (7, true), (10, false)] {
            let err = activate_family(&store, &keys(), FAMILY_RSEG, version, "op", T0 + 1)
                .await
                .expect_err("refused");
            if equal_or_lower {
                assert!(
                    matches!(
                        err,
                        CompatError::VersionNotAboveActive { active: 8, requested, .. }
                            if requested == version
                    ),
                    "got: {err}"
                );
            } else {
                assert!(
                    matches!(
                        err,
                        CompatError::VersionJump {
                            active: 8,
                            requested: 10,
                            ..
                        }
                    ),
                    "got: {err}"
                );
            }
        }
        let err = activate_family(&store, &keys(), "csnap", 2, "op", T0 + 1)
            .await
            .expect_err("no entry");
        assert!(
            matches!(err, CompatError::FamilyNotInRecord { .. }),
            "got: {err}"
        );
        let err = activate_family(&store, &keys(), "RSEG", 9, "op", T0 + 1)
            .await
            .expect_err("bad name");
        assert!(
            matches!(err, CompatError::FamilyInvalid { .. }),
            "got: {err}"
        );
        assert_eq!(stored_bytes(&store).await, before, "refusals write nothing");

        let (record, _) = activate_family(&store, &keys(), FAMILY_RLOG, 6, "op-b", T0 + 2)
            .await
            .expect("activate rlog 6");
        assert_eq!(record.compat_epoch, 2);
        assert_eq!(record.families[1].activated_epoch, 2);
        assert_eq!(record.families[0].activated_epoch, 1);
        assert_eq!(record.families[0].active_write_version, 8);
    }

    #[tokio::test]
    async fn update_refuses_to_lower_anything() {
        let store = MemoryStore::new();
        let mut start = baseline_record();
        start.compat_epoch = 2;
        start.families[0].activated_epoch = 2;
        start.families[0].certificate = Some(sysproto::Certificate {
            version: 7,
            ..Default::default()
        });
        create_compat(&store, &keys(), start).await.expect("create");
        let before = stored_bytes(&store).await;
        type Change = fn(&mut sysproto::CompatRecord);
        let changes: [Change; 5] = [
            |r| r.compat_epoch = 1,
            |r| r.families[0].active_write_version = 6,
            |r| r.families[0].activated_epoch = 1,
            |r| r.families[0].certificate = None,
            |r| {
                r.families.remove(1);
            },
        ];
        for change in changes {
            let err = update_compat(&store, &keys(), |r| {
                change(r);
                Ok(())
            })
            .await
            .expect_err("lowering is refused");
            assert!(matches!(err, CompatError::WouldLower { .. }), "got: {err}");
        }
        assert_eq!(stored_bytes(&store).await, before);
    }

    #[tokio::test]
    async fn update_cas_conflict_is_typed_and_retryable() {
        let inner = MemoryStore::new();
        create_compat(&inner, &keys(), baseline_record())
            .await
            .expect("create");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains(COMPAT_KEY),
        );
        let store = FaultStore::new(inner, plan);
        let err = activate_family(&store, &keys(), FAMILY_RSEG, 8, "op", T0)
            .await
            .expect_err("conflict");
        assert!(matches!(err, CompatError::CasConflict { .. }), "got: {err}");
        assert!(err.is_retryable());
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
            1
        );
    }

    #[tokio::test]
    async fn compat_record_missing_when_a_fenced_bucket_exists() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let view = view(store.clone(), ManualClock::at(T0));
        let refresh = view.refresh().await.expect("refresh");
        assert!(!refresh.present);

        let unfenced = sysproto::ProvisioningRecord {
            format_version: 3,
            ..Default::default()
        };
        let fenced = sysproto::ProvisioningRecord {
            format_version: 4,
            compat_epoch: 3,
            ..Default::default()
        };
        // No evidence of an activation: the caller's baseline.
        assert_eq!(view.write_policy(T0, FAMILY_RSEG, &[7], 7, None), Ok(7));
        assert_eq!(
            view.write_policy(T0, FAMILY_RSEG, &[7], 7, Some(&unfenced)),
            Ok(7)
        );
        assert_eq!(
            view.write_policy(T0, FAMILY_RSEG, &[7], 7, Some(&fenced)),
            Err(CompatRefusal::CompatRecordMissing {
                bucket_compat_epoch: 3,
                seen_compat_epoch: 0,
            })
        );

        // A record seen at an epoch above zero and then deleted.
        let mut activated = baseline_record();
        activated.compat_epoch = 1;
        create_compat(store.as_ref(), &keys(), activated)
            .await
            .expect("create");
        view.refresh().await.expect("refresh");
        assert_eq!(
            view.write_policy(T0, FAMILY_RSEG, &[7], 7, Some(&fenced)),
            Ok(7)
        );
        store.delete(COMPAT_KEY).await.expect("delete");
        view.refresh().await.expect("refresh");
        let refusal = view
            .write_policy(T0, FAMILY_RSEG, &[7], 7, None)
            .expect_err("missing");
        assert_eq!(
            refusal,
            CompatRefusal::CompatRecordMissing {
                bucket_compat_epoch: 0,
                seen_compat_epoch: 1,
            }
        );
        assert!(!refusal.is_retryable());
    }

    #[tokio::test]
    async fn mac_is_checked_and_verifies_under_any_key() {
        let store = MemoryStore::new();
        let two = CompatKeys::from_file_keys(&[[1; 32], [7; 32]]).expect("keys");
        create_compat(&store, &keys(), baseline_record())
            .await
            .expect("create");
        // Minted under [7; 32], the second of `two`.
        read_compat(&store, &two)
            .await
            .expect("verifies under any key")
            .expect("present");
        let other = CompatKeys::from_file_keys(&[[1; 32]]).expect("keys");
        let err = read_compat(&store, &other).await.expect_err("bad mac");
        assert!(matches!(err, CompatError::BadMac { .. }), "got: {err}");

        let (bytes, _) = stored_bytes(&store).await;
        let mut tampered = sysproto::CompatRecord::decode(bytes.as_ref()).expect("decode");
        tampered.families[0].active_write_version = 6;
        let err = decode_compat(&tampered.encode_to_vec(), &keys()).expect_err("tampered");
        assert!(matches!(err, CompatError::BadMac { .. }), "got: {err}");
        tampered.mac.clear();
        let err = decode_compat(&tampered.encode_to_vec(), &keys()).expect_err("no mac");
        assert!(matches!(err, CompatError::MissingMac { .. }), "got: {err}");

        assert!(matches!(
            CompatKeys::from_file_keys(&[]),
            Err(CompatError::NoKeys)
        ));
    }

    #[tokio::test]
    async fn malformed_records_are_typed() {
        let store = MemoryStore::new();
        let mut dup = baseline_record();
        dup.families.push(family(FAMILY_RSEG, 7));
        let err = create_compat(&store, &keys(), dup).await.expect_err("dup");
        assert!(matches!(err, CompatError::Corrupt { .. }), "got: {err}");

        let mut upper = baseline_record();
        upper.families[0].family = "RSEG".to_string();
        put_raw(&store, upper).await;
        let err = read_compat(&store, &keys()).await.expect_err("upper");
        assert!(matches!(err, CompatError::Corrupt { .. }), "got: {err}");

        let mut zero = baseline_record();
        zero.format_version = 0;
        let mut bytes_zero = zero.clone();
        seal(&mut bytes_zero, &keys());
        let err = decode_compat(&bytes_zero.encode_to_vec(), &keys()).expect_err("v0");
        assert!(
            matches!(err, CompatError::VersionBelowFloor { got: 0, .. }),
            "got: {err}"
        );
    }

    proptest! {
        #[test]
        fn arbitrary_bytes_never_panic_and_never_verify(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            if let Ok(record) = decode_compat(&bytes, &keys()) {
                // Only a record carrying a genuine MAC can decode.
                let mut resealed = record.clone();
                seal(&mut resealed, &keys());
                prop_assert_eq!(resealed.mac, record.mac);
            }
        }

        #[test]
        fn a_sealed_record_round_trips(epoch in 0u64..1_000, version in 1u32..100, by in "[a-z]{0,8}") {
            let mut record = baseline_record();
            record.compat_epoch = epoch;
            record.families[0].active_write_version = version;
            record.families[0].activated_by = by;
            record.format_version = COMPAT_FORMAT_VERSION;
            seal(&mut record, &keys());
            let decoded = decode_compat(&record.encode_to_vec(), &keys()).expect("round trip");
            prop_assert_eq!(decoded, record);
        }
    }
}
