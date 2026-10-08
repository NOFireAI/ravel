//! `ParquetTableManifest` encode and decode (ADR-2040 decision D1).
//!
//! A manifest version is a Class C immutable record (ADR-0066 decision 4)
//! with a strict reader window: version N+1 is built from version N, so a
//! reader that decoded a newer manifest with its unknown fields dropped would
//! write N+1 without them (the loss mechanism of ADR-0066's R1 amendment).
//! [`decode_manifest`] therefore refuses any `format_version` outside
//! [`PARQUET_TABLE_MIN_READ_VERSION`]`..=`[`PARQUET_TABLE_MAX_READ_VERSION`]
//! before looking at the body.
//!
//! Ravel does not own the files a manifest lists. Each one is identified by
//! the tuple (profile, bucket, key) and pinned by its ETag, plus the backend
//! version or generation where the store reports one. The key is carried as
//! the bytes the listing returned, not as a URL, and only a key that
//! [`key_is_addressable`] accepts is carried at all: the store is read through
//! object_store's `Path`, which percent-encodes or drops some bytes, and a key
//! it would rewrite would read a different object than the one listed.
//!
//! A body records the tenant it was encoded for, and [`decode_manifest`]
//! refuses it as [`ManifestError::Misfiled`] under a key of any other tenant,
//! as it does a body whose table or version differs from its key's.
//!
//! Both directions run [`Manifest::validate`], so a manifest this crate
//! encodes or decodes has a valid table name, a version of at least 1, a
//! 16-byte apply nonce, a location, grant and file list consistent with
//! `dropped`, and files that are individually addressable and distinct.
//! A decoded manifest whose file key fails [`key_is_addressable`] is refused
//! as [`ManifestDefect::UnaddressableKey`] rather than read.
//!
//! From `format_version` 2 a manifest carries a MAC (ADR-2430): a keyed
//! BLAKE3-256 over the body encoded with `mac` empty, under a
//! [`ManifestMacKey`] derived from the deployment key. A create-only object
//! credential can write a manifest version but cannot compute its MAC, so
//! [`decode_authenticated`] tells a version the DDL writer wrote from one put
//! directly in the bucket. This build reads version 2 and still writes
//! version 1 ([`PARQUET_TABLE_FORMAT_VERSION`]), so no reader meets a version
//! it refuses while the readers roll out.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;
use ravel_proto::parquet_table::v1 as pb;
use ravel_types::TenantHash;

use crate::keys::{KeyError, manifest_key, parse_manifest_key};
use crate::names::{NameError, validate_table};

/// Format floor written into every manifest this build emits. Still 1 while
/// readers of version 2 roll out (ADR-2430 decision 1, release A); raising it
/// to [`MAC_FORMAT_VERSION`] is what makes the writer MAC its manifests.
pub const PARQUET_TABLE_FORMAT_VERSION: u32 = 1;

/// Lowest `format_version` this build reads. Version 0 is an unstamped
/// record, which no supported writer produces.
pub const PARQUET_TABLE_MIN_READ_VERSION: u32 = 1;

/// Highest `format_version` this build reads. Version 2 adds the `mac` field
/// (ADR-2430), and is read before any writer stamps it.
pub const PARQUET_TABLE_MAX_READ_VERSION: u32 = 2;

/// The first `format_version` whose manifest carries a MAC. A version 1
/// manifest is never authenticated, whatever its `mac` field holds.
pub const MAC_FORMAT_VERSION: u32 = 2;

/// Bytes in a manifest MAC: a BLAKE3-256 keyed hash.
pub const MANIFEST_MAC_LEN: usize = 32;

/// `blake3::derive_key` context the manifest MAC key is derived from the
/// deployment key under (ADR-2430 decision 2).
pub const MANIFEST_MAC_KEY_CONTEXT: &str = "ravel pqm manifest mac v1";

/// Bytes in a manifest's per-apply nonce.
pub const APPLY_NONCE_LEN: usize = 16;

/// ASCII bytes object_store's `Path` percent-encodes inside a segment, on top
/// of the control characters and every non-ASCII byte.
const PATH_ENCODED: &[u8] = b"\\{^}%`]\">[~<#|*?";

/// True when object_store's `Path` built from `key` addresses exactly `key`:
/// valid UTF-8, printable ASCII with none of the bytes `Path` percent-encodes,
/// no leading or trailing `/`, and no empty, `.` or `..` segment (`Path` drops
/// an empty segment and encodes a dot segment).
pub fn key_is_addressable(key: &[u8]) -> bool {
    let Ok(key) = std::str::from_utf8(key) else {
        return false;
    };
    !key.is_empty()
        && key
            .bytes()
            .all(|b| (0x20..0x7f).contains(&b) && !PATH_ENCODED.contains(&b))
        && key
            .split('/')
            .all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

/// One external Parquet file a manifest pins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetFile {
    /// Credential profile the file is read through.
    pub profile: String,
    /// Bucket or container holding it.
    pub bucket: String,
    /// Object key, the bytes the listing returned; [`key_is_addressable`]
    /// holds for it in any manifest that validates.
    pub key: Vec<u8>,
    pub size: u64,
    /// ETag the footer read reported.
    pub etag: String,
    /// Backend object version or generation, empty when the store reports
    /// none.
    pub version: String,
    pub row_count: u64,
    /// Footer length in bytes, excluding the 8-byte trailer.
    pub footer_len: u32,
}

/// One decoded manifest version. `format_version` and `mac` are not carried:
/// the encoder takes the stamp and key as arguments, a decoded manifest has
/// already passed the read window, and [`decode_authenticated`] reports what
/// its MAC proves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub table: String,
    pub version: u64,
    pub dropped: bool,
    /// The LOCATION URL this version was created from, as given.
    pub location: String,
    /// The grant that admitted `location`, for audit only.
    pub grant: String,
    pub files: Vec<ParquetFile>,
    pub options: BTreeMap<String, String>,
    pub created_by: String,
    pub created_unix_ns: i64,
    pub statement: String,
    /// Per-apply nonce, [`APPLY_NONCE_LEN`] bytes.
    pub apply_nonce: Vec<u8>,
}

/// What made a manifest body invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestDefect {
    ZeroVersion,
    /// An `apply_nonce` that is not [`APPLY_NONCE_LEN`] bytes.
    NonceLen {
        len: usize,
    },
    /// A live (not dropped) manifest with no files.
    NoFiles,
    /// A live manifest with no location or no admitting grant.
    NoLocation,
    NoGrant,
    /// A dropped manifest that still names a location, grant, files or
    /// options.
    DroppedWithContent,
    /// A file with no profile, bucket or key: it names no object.
    UnaddressableFile {
        index: usize,
        field: &'static str,
    },
    /// A file key that [`key_is_addressable`] refuses.
    UnaddressableKey {
        index: usize,
    },
    /// A file with no ETag, which nothing could pin a read to.
    NoEtag {
        index: usize,
    },
    /// Two files with the same (profile, bucket, key).
    DuplicateFile {
        index: usize,
    },
    /// A zero-byte file, which holds no Parquet footer.
    EmptyFile {
        index: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ManifestError {
    #[error("manifest {key:?} could not be decoded: {source}")]
    Decode {
        key: String,
        #[source]
        source: prost::DecodeError,
    },
    #[error(
        "manifest {key:?} declares format_version {got}, above the highest version this build \
         reads ({ceiling}): a newer writer produced it. Upgrade this binary rather than read it \
         with fields dropped"
    )]
    UnsupportedVersion { key: String, got: u32, ceiling: u32 },
    #[error(
        "manifest {key:?} declares format_version {got}, below the lowest version this build \
         reads ({floor}): no supported writer produced it"
    )]
    VersionBelowFloor { key: String, got: u32, floor: u32 },
    #[error("manifest {key:?} is misfiled: its body records {field} {actual}, its key {expected}")]
    Misfiled {
        key: String,
        field: &'static str,
        expected: String,
        actual: String,
    },
    #[error("manifest for table {table:?} version {version} is invalid: {defect:?}")]
    Invalid {
        table: String,
        version: u64,
        defect: ManifestDefect,
    },
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(transparent)]
    Name(#[from] NameError),
}

impl ManifestError {
    /// True only for a manifest a newer build wrote (ADR-0066 decision 2):
    /// retryable at a query surface, where every other variant is corrupt.
    /// One below the floor is false: no supported writer produced it.
    ///
    /// Every variant is named, so a new one fails to compile until it is
    /// classified here. `Key` and `Name` answer false for the whole wrapped
    /// [`KeyError`] or [`NameError`], which carry no format version; a
    /// version-ceiling variant added to either must be delegated to here.
    pub fn is_newer_format_version(&self) -> bool {
        match self {
            ManifestError::UnsupportedVersion { .. } => true,
            ManifestError::Decode { .. }
            | ManifestError::VersionBelowFloor { .. }
            | ManifestError::Misfiled { .. }
            | ManifestError::Invalid { .. }
            | ManifestError::Key(_)
            | ManifestError::Name(_) => false,
        }
    }
}

impl Manifest {
    /// Check the body against the rules in the module docs.
    pub fn validate(&self) -> Result<(), ManifestError> {
        let invalid = |defect| {
            Err(ManifestError::Invalid {
                table: self.table.clone(),
                version: self.version,
                defect,
            })
        };
        validate_table(&self.table)?;
        if self.version == 0 {
            return invalid(ManifestDefect::ZeroVersion);
        }
        if self.apply_nonce.len() != APPLY_NONCE_LEN {
            return invalid(ManifestDefect::NonceLen {
                len: self.apply_nonce.len(),
            });
        }
        if self.dropped {
            if !self.location.is_empty()
                || !self.grant.is_empty()
                || !self.files.is_empty()
                || !self.options.is_empty()
            {
                return invalid(ManifestDefect::DroppedWithContent);
            }
            return Ok(());
        }
        if self.location.is_empty() {
            return invalid(ManifestDefect::NoLocation);
        }
        if self.grant.is_empty() {
            return invalid(ManifestDefect::NoGrant);
        }
        if self.files.is_empty() {
            return invalid(ManifestDefect::NoFiles);
        }
        let mut seen = BTreeSet::new();
        for (index, file) in self.files.iter().enumerate() {
            let missing = if file.profile.is_empty() {
                Some("profile")
            } else if file.bucket.is_empty() {
                Some("bucket")
            } else if file.key.is_empty() {
                Some("key")
            } else {
                None
            };
            if let Some(field) = missing {
                return invalid(ManifestDefect::UnaddressableFile { index, field });
            }
            if !key_is_addressable(&file.key) {
                return invalid(ManifestDefect::UnaddressableKey { index });
            }
            if file.etag.is_empty() {
                return invalid(ManifestDefect::NoEtag { index });
            }
            if file.size == 0 {
                return invalid(ManifestDefect::EmptyFile { index });
            }
            if !seen.insert((&file.profile, &file.bucket, &file.key)) {
                return invalid(ManifestDefect::DuplicateFile { index });
            }
        }
        Ok(())
    }

    /// True unless this version records a DROP.
    pub fn is_live(&self) -> bool {
        !self.dropped
    }
}

/// The key a manifest MAC is taken under, derived from the deployment key
/// (`--tenant-hash-key-file`). It never reaches object storage, so a principal
/// holding only object access cannot compute a MAC.
#[derive(Clone)]
pub struct ManifestMacKey([u8; 32]);

impl ManifestMacKey {
    /// Derive the MAC key from the 32-byte deployment key under
    /// [`MANIFEST_MAC_KEY_CONTEXT`], so it is never the deployment key itself.
    pub fn from_deployment_key(deployment_key: &[u8; 32]) -> Self {
        ManifestMacKey(blake3::derive_key(MANIFEST_MAC_KEY_CONTEXT, deployment_key))
    }

    /// Keyed BLAKE3-256 of `bytes`.
    fn tag(&self, bytes: &[u8]) -> [u8; MANIFEST_MAC_LEN] {
        *blake3::keyed_hash(&self.0, bytes).as_bytes()
    }

    /// True when `tag` is this key's MAC of `bytes`, compared in constant
    /// time.
    pub fn verify(&self, bytes: &[u8], tag: &[u8]) -> bool {
        ct_eq(&self.tag(bytes), tag)
    }
}

impl std::fmt::Debug for ManifestMacKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ManifestMacKey(<redacted>)")
    }
}

/// Constant-time byte-slice comparison, so verifying a MAC does not leak how
/// many leading bytes matched. A length mismatch is refused up front; the
/// length of a tag is not secret.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Whether a stored manifest is authenticated by a key ([`decode_authenticated`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MacStatus {
    /// `format_version` below [`MAC_FORMAT_VERSION`]: the record predates the
    /// MAC and authenticates nothing.
    Unversioned,
    /// A version 2 manifest with an empty `mac`.
    Absent,
    /// A `mac` that is not the key's MAC of the body.
    Invalid,
    Valid,
}

impl MacStatus {
    pub fn is_valid(self) -> bool {
        self == MacStatus::Valid
    }
}

/// The MAC of `body` under `key`: the keyed hash of `body` encoded with `mac`
/// empty. prost encodes fields in field-number order and the `options` map as
/// a `BTreeMap`, so one message has one encoding, and a decoded body
/// re-encodes to the bytes its MAC was taken over.
fn body_mac(key: &ManifestMacKey, body: &pb::ParquetTableManifest) -> [u8; MANIFEST_MAC_LEN] {
    let mut unsigned = body.clone();
    unsigned.mac.clear();
    key.tag(&unsigned.encode_to_vec())
}

fn mac_status(key: &ManifestMacKey, body: &pb::ParquetTableManifest) -> MacStatus {
    if body.format_version < MAC_FORMAT_VERSION {
        MacStatus::Unversioned
    } else if body.mac.is_empty() {
        MacStatus::Absent
    } else if ct_eq(&body_mac(key, body), &body.mac) {
        MacStatus::Valid
    } else {
        MacStatus::Invalid
    }
}

/// The MAC [`encode_manifest_with`] stores for `manifest` as a version of
/// `tenant`'s table stamped `format_version`, or `None` when that version
/// carries no MAC. Refuses a manifest that fails [`Manifest::validate`].
pub fn manifest_mac(
    key: &ManifestMacKey,
    tenant: &TenantHash,
    manifest: &Manifest,
    format_version: u32,
) -> Result<Option<[u8; MANIFEST_MAC_LEN]>, ManifestError> {
    manifest.validate()?;
    if format_version < MAC_FORMAT_VERSION {
        return Ok(None);
    }
    Ok(Some(body_mac(
        key,
        &to_body(tenant, manifest, format_version),
    )))
}

/// Encode `manifest` as a version of `tenant`'s table, stamped with
/// [`PARQUET_TABLE_FORMAT_VERSION`] and carrying no MAC. Refuses a manifest
/// that fails [`Manifest::validate`].
pub fn encode_manifest(tenant: &TenantHash, manifest: &Manifest) -> Result<Vec<u8>, ManifestError> {
    encode_manifest_with(tenant, manifest, PARQUET_TABLE_FORMAT_VERSION, None)
}

/// Encode `manifest` stamped `format_version`. From [`MAC_FORMAT_VERSION`] on,
/// a `key` makes the body carry its MAC; with no key the `mac` field stays
/// empty, which no reader authenticates. Below it the body never carries one.
/// Refuses a manifest that fails [`Manifest::validate`], and a
/// `format_version` outside this build's read window, which it could not read
/// back.
pub fn encode_manifest_with(
    tenant: &TenantHash,
    manifest: &Manifest,
    format_version: u32,
    key: Option<&ManifestMacKey>,
) -> Result<Vec<u8>, ManifestError> {
    manifest.validate()?;
    let mut body = to_body(tenant, manifest, format_version);
    let stamp_key =
        || manifest_key(tenant, &manifest.table, manifest.version).map_err(ManifestError::from);
    if format_version < PARQUET_TABLE_MIN_READ_VERSION {
        return Err(ManifestError::VersionBelowFloor {
            key: stamp_key()?,
            got: format_version,
            floor: PARQUET_TABLE_MIN_READ_VERSION,
        });
    }
    if format_version > PARQUET_TABLE_MAX_READ_VERSION {
        return Err(ManifestError::UnsupportedVersion {
            key: stamp_key()?,
            got: format_version,
            ceiling: PARQUET_TABLE_MAX_READ_VERSION,
        });
    }
    if format_version >= MAC_FORMAT_VERSION
        && let Some(key) = key
    {
        body.mac = body_mac(key, &body).to_vec();
    }
    Ok(body.encode_to_vec())
}

fn to_body(
    tenant: &TenantHash,
    manifest: &Manifest,
    format_version: u32,
) -> pb::ParquetTableManifest {
    pb::ParquetTableManifest {
        format_version,
        tenant_hash: tenant.0.to_vec(),
        table: manifest.table.clone(),
        version: manifest.version,
        dropped: manifest.dropped,
        location: manifest.location.clone(),
        grant: manifest.grant.clone(),
        files: manifest
            .files
            .iter()
            .map(|f| pb::ParquetFile {
                profile: f.profile.clone(),
                bucket: f.bucket.clone(),
                key: f.key.clone(),
                size: f.size,
                etag: f.etag.clone(),
                version: f.version.clone(),
                row_count: f.row_count,
                footer_len: f.footer_len,
            })
            .collect(),
        options: manifest.options.clone(),
        created_by: manifest.created_by.clone(),
        created_unix_ns: manifest.created_unix_ns,
        statement: manifest.statement.clone(),
        apply_nonce: manifest.apply_nonce.clone(),
        mac: Vec::new(),
    }
}

/// Decode the manifest stored at `key`. Refuses, with a typed error: a key
/// that is not a manifest key, bytes that are not a protobuf message, a
/// `format_version` outside the read window, a body whose tenant, table or
/// version differs from the key's, and a body that fails
/// [`Manifest::validate`]. A version 2 body's MAC is not checked here: a
/// query reads an unauthenticated manifest as it reads a version 1 one
/// (ADR-2430 decision 6). [`decode_authenticated`] checks it.
pub fn decode_manifest(key: &str, bytes: &[u8]) -> Result<Manifest, ManifestError> {
    into_manifest(decode_body(key, bytes)?)
}

/// [`decode_manifest`], plus whether `mac_key` authenticates the body.
pub fn decode_authenticated(
    key: &str,
    bytes: &[u8],
    mac_key: &ManifestMacKey,
) -> Result<(Manifest, MacStatus), ManifestError> {
    let body = decode_body(key, bytes)?;
    let status = mac_status(mac_key, &body);
    Ok((into_manifest(body)?, status))
}

/// The body stored at `key`, checked against the read window and the key.
fn decode_body(key: &str, bytes: &[u8]) -> Result<pb::ParquetTableManifest, ManifestError> {
    let parsed = parse_manifest_key(key)?;
    let body = pb::ParquetTableManifest::decode(bytes).map_err(|source| ManifestError::Decode {
        key: key.to_string(),
        source,
    })?;
    if body.format_version < PARQUET_TABLE_MIN_READ_VERSION {
        return Err(ManifestError::VersionBelowFloor {
            key: key.to_string(),
            got: body.format_version,
            floor: PARQUET_TABLE_MIN_READ_VERSION,
        });
    }
    if body.format_version > PARQUET_TABLE_MAX_READ_VERSION {
        return Err(ManifestError::UnsupportedVersion {
            key: key.to_string(),
            got: body.format_version,
            ceiling: PARQUET_TABLE_MAX_READ_VERSION,
        });
    }
    if body.tenant_hash.as_slice() != parsed.tenant_hash.0.as_slice() {
        return Err(ManifestError::Misfiled {
            key: key.to_string(),
            field: "tenant_hash",
            expected: parsed.tenant_hash.to_hex(),
            actual: hex::encode(&body.tenant_hash),
        });
    }
    if body.table != parsed.table {
        return Err(ManifestError::Misfiled {
            key: key.to_string(),
            field: "table",
            expected: parsed.table,
            actual: body.table,
        });
    }
    if body.version != parsed.version {
        return Err(ManifestError::Misfiled {
            key: key.to_string(),
            field: "version",
            expected: parsed.version.to_string(),
            actual: body.version.to_string(),
        });
    }
    Ok(body)
}

/// The [`Manifest`] a checked body holds, refused if it fails
/// [`Manifest::validate`].
fn into_manifest(body: pb::ParquetTableManifest) -> Result<Manifest, ManifestError> {
    let manifest = Manifest {
        table: body.table,
        version: body.version,
        dropped: body.dropped,
        location: body.location,
        grant: body.grant,
        files: body
            .files
            .into_iter()
            .map(|f| ParquetFile {
                profile: f.profile,
                bucket: f.bucket,
                key: f.key,
                size: f.size,
                etag: f.etag,
                version: f.version,
                row_count: f.row_count,
                footer_len: f.footer_len,
            })
            .collect(),
        options: body.options,
        created_by: body.created_by,
        created_unix_ns: body.created_unix_ns,
        statement: body.statement,
        apply_nonce: body.apply_nonce,
    };
    manifest.validate()?;
    Ok(manifest)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::keys::manifest_key;

    const TENANT: TenantHash = TenantHash([0x5c; 16]);
    const OTHER_TENANT: TenantHash = TenantHash([0x6d; 16]);

    #[test]
    fn only_a_version_above_the_ceiling_is_newer() {
        let above = ManifestError::UnsupportedVersion {
            key: "k".into(),
            got: 9,
            ceiling: 1,
        };
        assert!(above.is_newer_format_version());
        let below = ManifestError::VersionBelowFloor {
            key: "k".into(),
            got: 0,
            floor: 1,
        };
        assert!(!below.is_newer_format_version());
        let invalid = ManifestError::Invalid {
            table: "hits".into(),
            version: 1,
            defect: ManifestDefect::ZeroVersion,
        };
        assert!(!invalid.is_newer_format_version());
        assert!(!ManifestError::Key(KeyError::ZeroVersion).is_newer_format_version());
    }

    fn file(seed: u8) -> ParquetFile {
        ParquetFile {
            profile: "prod".into(),
            bucket: "customer-bucket".into(),
            key: format!("data/part-{seed}.parquet").into_bytes(),
            size: 1000 + u64::from(seed),
            etag: format!("\"etag-{seed}\""),
            version: format!("gen-{seed}"),
            row_count: 7,
            footer_len: 321,
        }
    }

    fn live(version: u64) -> Manifest {
        Manifest {
            table: "hits".into(),
            version,
            dropped: false,
            location: "s3://customer-bucket/data/".into(),
            grant: "s3://customer-bucket/data".into(),
            files: vec![file(1), file(2)],
            options: BTreeMap::from([("binary_as_string".into(), "true".into())]),
            created_by: "tenant-a".into(),
            created_unix_ns: 1_700_000_000_000_000_000,
            statement: "CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION \
                        's3://customer-bucket/data/'"
                .into(),
            apply_nonce: vec![9; APPLY_NONCE_LEN],
        }
    }

    fn raw_with_version(format_version: u32, m: &Manifest) -> Vec<u8> {
        let bytes = encode_manifest(&TENANT, m).expect("encode");
        let mut body = pb::ParquetTableManifest::decode(bytes.as_slice()).expect("decode");
        body.format_version = format_version;
        body.encode_to_vec()
    }

    #[test]
    fn release_a_reads_versions_one_and_two_and_writes_one() {
        // ADR-2430 decision 1: readers learn version 2 one release before any
        // writer stamps it.
        assert_eq!(PARQUET_TABLE_FORMAT_VERSION, 1);
        assert_eq!(PARQUET_TABLE_MIN_READ_VERSION, 1);
        assert_eq!(PARQUET_TABLE_MAX_READ_VERSION, 2);
        assert_eq!(MAC_FORMAT_VERSION, 2);
        let key = manifest_key(&TENANT, "hits", 3).expect("key");
        for format_version in [1, 2] {
            let bytes = raw_with_version(format_version, &live(3));
            assert_eq!(
                decode_manifest(&key, &bytes),
                Ok(live(3)),
                "{format_version}"
            );
        }
    }

    #[test]
    fn a_format_version_above_the_ceiling_is_refused() {
        let m = live(3);
        let key = manifest_key(&TENANT, "hits", 3).expect("key");
        let bytes = raw_with_version(PARQUET_TABLE_MAX_READ_VERSION + 1, &m);
        assert_eq!(
            decode_manifest(&key, &bytes),
            Err(ManifestError::UnsupportedVersion {
                key: key.clone(),
                got: 3,
                ceiling: 2,
            })
        );
        let bytes = raw_with_version(u32::MAX, &m);
        assert!(matches!(
            decode_manifest(&key, &bytes),
            Err(ManifestError::UnsupportedVersion { got: u32::MAX, .. })
        ));
    }

    #[test]
    fn a_format_version_below_the_floor_is_refused() {
        let m = live(3);
        let key = manifest_key(&TENANT, "hits", 3).expect("key");
        let bytes = raw_with_version(PARQUET_TABLE_MIN_READ_VERSION - 1, &m);
        assert_eq!(
            decode_manifest(&key, &bytes),
            Err(ManifestError::VersionBelowFloor {
                key,
                got: 0,
                floor: 1,
            })
        );
        // An empty object decodes as an all-default message: version 0.
        let key = manifest_key(&TENANT, "hits", 1).expect("key");
        assert!(matches!(
            decode_manifest(&key, &[]),
            Err(ManifestError::VersionBelowFloor { got: 0, .. })
        ));
    }

    #[test]
    fn a_body_whose_table_or_version_disagrees_with_its_key_is_refused() {
        let bytes = encode_manifest(&TENANT, &live(3)).expect("encode");
        let wrong_version = manifest_key(&TENANT, "hits", 4).expect("key");
        assert_eq!(
            decode_manifest(&wrong_version, &bytes),
            Err(ManifestError::Misfiled {
                key: wrong_version.clone(),
                field: "version",
                expected: "4".into(),
                actual: "3".into(),
            })
        );
        let wrong_table = manifest_key(&TENANT, "other", 3).expect("key");
        assert_eq!(
            decode_manifest(&wrong_table, &bytes),
            Err(ManifestError::Misfiled {
                key: wrong_table.clone(),
                field: "table",
                expected: "other".into(),
                actual: "hits".into(),
            })
        );
        let right = manifest_key(&TENANT, "hits", 3).expect("key");
        assert_eq!(decode_manifest(&right, &bytes), Ok(live(3)));
    }

    #[test]
    fn a_manifest_copied_under_another_tenants_key_is_misfiled() {
        let bytes = encode_manifest(&TENANT, &live(3)).expect("encode");
        let other = manifest_key(&OTHER_TENANT, "hits", 3).expect("key");
        assert_eq!(
            decode_manifest(&other, &bytes),
            Err(ManifestError::Misfiled {
                key: other.clone(),
                field: "tenant_hash",
                expected: OTHER_TENANT.to_hex(),
                actual: TENANT.to_hex(),
            })
        );
        // A body with no tenant at all, or a truncated one, is refused too.
        let mut body = pb::ParquetTableManifest::decode(bytes.as_slice()).expect("decode");
        for tenant_hash in [vec![], TENANT.0[..15].to_vec()] {
            body.tenant_hash = tenant_hash;
            let key = manifest_key(&TENANT, "hits", 3).expect("key");
            assert!(
                matches!(
                    decode_manifest(&key, &body.encode_to_vec()),
                    Err(ManifestError::Misfiled {
                        field: "tenant_hash",
                        ..
                    })
                ),
                "{:?}",
                body.tenant_hash
            );
        }
    }

    #[test]
    fn a_key_the_store_path_would_rewrite_is_refused_in_both_directions() {
        // Each of these reaches a different object, or none, once
        // object_store's Path has dropped the empty segment or encoded the
        // byte, so a manifest carrying it would not read what was listed.
        let unaddressable: Vec<Vec<u8>> = vec![
            b"data//double/slash.parquet".to_vec(),
            b"data/100%25 done/x.parquet".to_vec(),
            vec![b'd', b'a', b't', b'a', b'/', 0xff, 0xfe, b'.', b'p'],
            "data/\u{e9}\u{4e2d}.parquet".as_bytes().to_vec(),
            b"/data/x.parquet".to_vec(),
            b"data/".to_vec(),
            b"data/./x.parquet".to_vec(),
            b"data/../x.parquet".to_vec(),
            b"data/x*.parquet".to_vec(),
            b"data/x\t.parquet".to_vec(),
        ];
        let key = manifest_key(&TENANT, "hits", 1).expect("key");
        for raw in unaddressable {
            let mut m = live(1);
            m.files[1].key = raw.clone();
            let refused = ManifestError::Invalid {
                table: "hits".into(),
                version: 1,
                defect: ManifestDefect::UnaddressableKey { index: 1 },
            };
            assert_eq!(
                encode_manifest(&TENANT, &m),
                Err(refused.clone()),
                "{raw:?}"
            );
            let mut body = pb::ParquetTableManifest::decode(
                encode_manifest(&TENANT, &live(1))
                    .expect("encode")
                    .as_slice(),
            )
            .expect("decode");
            body.files[1].key = raw.clone();
            assert_eq!(
                decode_manifest(&key, &body.encode_to_vec()),
                Err(refused),
                "{raw:?}"
            );
        }
    }

    #[test]
    fn an_addressable_key_survives_the_round_trip_byte_for_byte() {
        let raw: Vec<Vec<u8>> = vec![
            b"data/a b c.parquet".to_vec(),
            b"data/part=1/x-y_z.parquet".to_vec(),
            b"data/!$&'()+,;=@.parquet".to_vec(),
            b"x".to_vec(),
        ];
        let mut m = live(1);
        m.files = raw
            .iter()
            .enumerate()
            .map(|(i, key)| ParquetFile {
                key: key.clone(),
                ..file(i as u8)
            })
            .collect();
        let key = manifest_key(&TENANT, "hits", 1).expect("key");
        let bytes = encode_manifest(&TENANT, &m).expect("encode");
        let decoded = decode_manifest(&key, &bytes).expect("decode");
        let got: Vec<Vec<u8>> = decoded.files.iter().map(|f| f.key.clone()).collect();
        assert_eq!(got, raw);
        assert_eq!(decoded, m);
    }

    #[test]
    fn invalid_bodies_are_refused_on_encode() {
        type Case = (fn(&mut Manifest), ManifestDefect);
        let cases: [Case; 10] = [
            (|m| m.files.clear(), ManifestDefect::NoFiles),
            (
                |m| m.files.push(m.files[0].clone()),
                ManifestDefect::DuplicateFile { index: 2 },
            ),
            (
                |m| m.files[1].size = 0,
                ManifestDefect::EmptyFile { index: 1 },
            ),
            (
                |m| m.files[1].profile.clear(),
                ManifestDefect::UnaddressableFile {
                    index: 1,
                    field: "profile",
                },
            ),
            (
                |m| m.files[0].bucket.clear(),
                ManifestDefect::UnaddressableFile {
                    index: 0,
                    field: "bucket",
                },
            ),
            (
                |m| m.files[0].key.clear(),
                ManifestDefect::UnaddressableFile {
                    index: 0,
                    field: "key",
                },
            ),
            (
                |m| m.files[1].etag.clear(),
                ManifestDefect::NoEtag { index: 1 },
            ),
            (|m| m.location.clear(), ManifestDefect::NoLocation),
            (|m| m.grant.clear(), ManifestDefect::NoGrant),
            (|m| m.dropped = true, ManifestDefect::DroppedWithContent),
        ];
        for (mutate, defect) in cases {
            let mut m = live(1);
            mutate(&mut m);
            assert_eq!(
                encode_manifest(&TENANT, &m),
                Err(ManifestError::Invalid {
                    table: "hits".into(),
                    version: 1,
                    defect: defect.clone(),
                }),
                "{defect:?}"
            );
        }
        assert!(matches!(
            encode_manifest(&TENANT, &live(0)),
            Err(ManifestError::Invalid {
                defect: ManifestDefect::ZeroVersion,
                ..
            })
        ));
    }

    #[test]
    fn a_nonce_of_the_wrong_length_is_refused_in_both_directions() {
        for len in [0, 15, 17, 32] {
            let mut m = live(1);
            m.apply_nonce = vec![1; len];
            assert!(
                matches!(
                    encode_manifest(&TENANT, &m),
                    Err(ManifestError::Invalid {
                        defect: ManifestDefect::NonceLen { len: got },
                        ..
                    }) if got == len
                ),
                "{len}"
            );
        }
        let key = manifest_key(&TENANT, "hits", 1).expect("key");
        let bytes = encode_manifest(&TENANT, &live(1)).expect("encode");
        let mut body = pb::ParquetTableManifest::decode(bytes.as_slice()).expect("decode");
        body.apply_nonce.truncate(4);
        assert!(matches!(
            decode_manifest(&key, &body.encode_to_vec()),
            Err(ManifestError::Invalid {
                defect: ManifestDefect::NonceLen { len: 4 },
                ..
            })
        ));
    }

    #[test]
    fn truncation_inside_the_last_field_is_a_decode_error() {
        let key = manifest_key(&TENANT, "hits", 1).expect("key");
        let bytes = encode_manifest(&TENANT, &live(1)).expect("encode");
        // `tenant_hash` is the highest field number, so it is encoded last and
        // dropping the final byte cuts it mid-value.
        assert!(matches!(
            decode_manifest(&key, &bytes[..bytes.len() - 1]),
            Err(ManifestError::Decode { .. })
        ));
    }

    fn mac_key() -> ManifestMacKey {
        ManifestMacKey::from_deployment_key(&[0x42; 32])
    }

    fn status(bytes: &[u8], key: &ManifestMacKey) -> MacStatus {
        let at = manifest_key(&TENANT, "hits", 3).expect("key");
        decode_authenticated(&at, bytes, key).expect("decode").1
    }

    #[test]
    fn the_mac_key_is_derived_under_its_own_context() {
        let deployment = [0x42; 32];
        let derived = blake3::derive_key("ravel pqm manifest mac v1", &deployment);
        let tag = blake3::keyed_hash(&derived, b"body");
        assert!(mac_key().verify(b"body", tag.as_bytes()));
        // Neither the deployment key itself nor another context's key.
        let raw = blake3::keyed_hash(&deployment, b"body");
        assert!(!mac_key().verify(b"body", raw.as_bytes()));
        let other = blake3::keyed_hash(
            &blake3::derive_key("ravel audit query-text token key v1", &deployment),
            b"body",
        );
        assert!(!mac_key().verify(b"body", other.as_bytes()));
        assert_eq!(format!("{:?}", mac_key()), "ManifestMacKey(<redacted>)");
    }

    #[test]
    fn a_version_two_manifest_macs_under_the_key_and_verifies() {
        let m = live(3);
        let bytes = encode_manifest_with(&TENANT, &m, MAC_FORMAT_VERSION, Some(&mac_key()))
            .expect("encode");
        let at = manifest_key(&TENANT, "hits", 3).expect("key");
        assert_eq!(
            decode_authenticated(&at, &bytes, &mac_key()),
            Ok((m.clone(), MacStatus::Valid))
        );
        assert_eq!(decode_manifest(&at, &bytes), Ok(m.clone()));
        let body = pb::ParquetTableManifest::decode(bytes.as_slice()).expect("decode");
        assert_eq!(body.format_version, 2);
        assert_eq!(body.mac.len(), MANIFEST_MAC_LEN);
        assert_eq!(
            manifest_mac(&mac_key(), &TENANT, &m, MAC_FORMAT_VERSION)
                .expect("mac")
                .map(|t| t.to_vec()),
            Some(body.mac)
        );
        assert_eq!(manifest_mac(&mac_key(), &TENANT, &m, 1).expect("mac"), None);
    }

    #[test]
    fn a_manifest_built_without_the_key_does_not_verify() {
        let m = live(3);
        let unkeyed = encode_manifest_with(&TENANT, &m, MAC_FORMAT_VERSION, None).expect("encode");
        assert_eq!(status(&unkeyed, &mac_key()), MacStatus::Absent);
        let forger = ManifestMacKey::from_deployment_key(&[0x66; 32]);
        let forged =
            encode_manifest_with(&TENANT, &m, MAC_FORMAT_VERSION, Some(&forger)).expect("encode");
        assert_eq!(status(&forged, &mac_key()), MacStatus::Invalid);
        assert_eq!(status(&forged, &forger), MacStatus::Valid);
        // Version 1 never authenticates, even holding a MAC that would verify
        // under version 2.
        let mut body = pb::ParquetTableManifest::decode(
            encode_manifest_with(&TENANT, &m, MAC_FORMAT_VERSION, Some(&mac_key()))
                .expect("encode")
                .as_slice(),
        )
        .expect("decode");
        body.format_version = 1;
        assert_eq!(
            status(&body.encode_to_vec(), &mac_key()),
            MacStatus::Unversioned
        );
        assert_eq!(
            status(&encode_manifest(&TENANT, &m).expect("encode"), &mac_key()),
            MacStatus::Unversioned
        );
    }

    #[test]
    fn every_one_bit_change_to_the_tag_fails_verification() {
        let bytes = encode_manifest_with(&TENANT, &live(3), MAC_FORMAT_VERSION, Some(&mac_key()))
            .expect("encode");
        let body = pb::ParquetTableManifest::decode(bytes.as_slice()).expect("decode");
        assert_eq!(status(&bytes, &mac_key()), MacStatus::Valid);
        for bit in 0..MANIFEST_MAC_LEN * 8 {
            let mut flipped = body.clone();
            flipped.mac[bit / 8] ^= 1 << (bit % 8);
            assert_eq!(
                status(&flipped.encode_to_vec(), &mac_key()),
                MacStatus::Invalid,
                "bit {bit}"
            );
        }
        // A truncated or extended tag is refused before any byte compares.
        for len in [0, 1, MANIFEST_MAC_LEN - 1, MANIFEST_MAC_LEN + 1] {
            let mut tag = body.mac.clone();
            tag.resize(len, 0);
            assert!(!ct_eq(&body.mac, &tag), "{len}");
        }
    }

    #[test]
    fn the_mac_binds_every_field_of_the_body() {
        let bytes = encode_manifest_with(&TENANT, &live(3), MAC_FORMAT_VERSION, Some(&mac_key()))
            .expect("encode");
        let body = pb::ParquetTableManifest::decode(bytes.as_slice()).expect("decode");
        type Edit = fn(&mut pb::ParquetTableManifest);
        let edits: [(&str, Edit); 10] = [
            ("location", |b| {
                b.location = "s3://customer-bucket/other/".into()
            }),
            ("grant", |b| b.grant = "s3://customer-bucket".into()),
            ("files", |b| {
                b.files.pop();
            }),
            ("file etag", |b| b.files[0].etag = "\"etag-x\"".into()),
            ("options", |b| {
                b.options.clear();
            }),
            ("created_by", |b| b.created_by = "intruder".into()),
            ("created_unix_ns", |b| b.created_unix_ns += 1),
            ("statement", |b| b.statement = "DROP TABLE hits".into()),
            ("apply_nonce", |b| b.apply_nonce[0] ^= 1),
            ("dropped", |b| {
                b.dropped = true;
                b.location.clear();
                b.grant.clear();
                b.files.clear();
                b.options.clear();
            }),
        ];
        for (field, edit) in edits {
            let mut edited = body.clone();
            edit(&mut edited);
            assert_eq!(
                status(&edited.encode_to_vec(), &mac_key()),
                MacStatus::Invalid,
                "{field}"
            );
        }
        // The tenant, table and version are bound too: a MACed body copied
        // under another version's key is refused as misfiled, and with the key
        // fields edited to match, its MAC no longer verifies.
        let mut moved = body.clone();
        moved.version = 4;
        let at = manifest_key(&TENANT, "hits", 4).expect("key");
        assert_eq!(
            decode_authenticated(&at, &moved.encode_to_vec(), &mac_key()).map(|(_, s)| s),
            Ok(MacStatus::Invalid)
        );
    }

    #[test]
    fn the_encoder_refuses_a_stamp_outside_the_read_window() {
        let key = manifest_key(&TENANT, "hits", 3).expect("key");
        assert_eq!(
            encode_manifest_with(&TENANT, &live(3), 3, Some(&mac_key())),
            Err(ManifestError::UnsupportedVersion {
                key: key.clone(),
                got: 3,
                ceiling: 2,
            })
        );
        assert_eq!(
            encode_manifest_with(&TENANT, &live(3), 0, None),
            Err(ManifestError::VersionBelowFloor {
                key,
                got: 0,
                floor: 1,
            })
        );
    }

    #[test]
    fn a_dropped_manifest_round_trips() {
        let m = Manifest {
            table: "hits".into(),
            version: 9,
            dropped: true,
            location: String::new(),
            grant: String::new(),
            files: vec![],
            options: BTreeMap::new(),
            created_by: "operator".into(),
            created_unix_ns: 5,
            statement: "DROP TABLE hits".into(),
            apply_nonce: vec![3; APPLY_NONCE_LEN],
        };
        let key = manifest_key(&TENANT, "hits", 9).expect("key");
        let bytes = encode_manifest(&TENANT, &m).expect("encode");
        assert_eq!(decode_manifest(&key, &bytes), Ok(m));
    }

    fn arb_addressable_key() -> impl Strategy<Value = Vec<u8>> {
        prop::collection::vec("[a-zA-Z0-9 !$&'()+,;=@_.-]{1,8}", 1..4)
            .prop_map(|segments| segments.join("/").into_bytes())
            .prop_filter("dot segment", |key| key_is_addressable(key))
    }

    /// Characters around every rule of [`key_is_addressable`], so a random
    /// string hits the edges far more often than an unbiased one would.
    fn arb_edge_string() -> impl Strategy<Value = String> {
        let chars = prop::sample::select(vec![
            'a', 'Z', '0', '-', '_', '=', ' ', '.', '/', '%', '*', '?', '#', '~', '\\', '"', '\t',
            '\r', '\u{7f}', '\u{e9}', '\u{4e2d}',
        ]);
        prop_oneof![
            prop::collection::vec(chars, 0..10).prop_map(|c| c.into_iter().collect()),
            any::<String>(),
        ]
    }

    #[test]
    fn key_is_addressable_follows_each_rule() {
        for (key, want) in [
            (&b"data/x.parquet"[..], true),
            (b"a b/c", true),
            (b"", false),
            (b"/a", false),
            (b"a/", false),
            (b"a//b", false),
            (b".", false),
            (b"a/../b", false),
            (b"a/./b", false),
            (b"a/.../b", true),
            (b"a/.x", true),
            (b"a%2Fb", false),
            (b"a\nb", false),
            (&[b'a', 0xff], false),
            ("\u{e9}".as_bytes(), false),
        ] {
            assert_eq!(key_is_addressable(key), want, "{key:?}");
        }
    }

    #[test]
    fn every_ascii_byte_inside_a_segment_agrees_with_the_object_store_path() {
        for b in 0u8..0x80 {
            let key = [b'a', b, b'b'];
            let s = std::str::from_utf8(&key).expect("ascii");
            let path = object_store::path::Path::from(s);
            assert_eq!(
                key_is_addressable(&key),
                path.as_ref() == s,
                "byte {b:#04x}: {s:?} became {:?}",
                path.as_ref()
            );
        }
    }

    proptest! {
        #[test]
        fn key_is_addressable_matches_the_object_store_path(s in arb_edge_string()) {
            let path = object_store::path::Path::from(s.as_str());
            prop_assert_eq!(
                key_is_addressable(s.as_bytes()),
                !s.is_empty() && path.as_ref() == s,
                "{:?} became {:?}",
                s,
                path.as_ref()
            );
        }
    }

    prop_compose! {
        fn arb_file()(
            profile in "[a-z]{1,8}",
            bucket in "[a-z0-9-]{1,12}",
            key in arb_addressable_key(),
            size in 1u64..=u64::MAX,
            etag in "[!-~]{1,10}",
            version in "[a-z0-9]{0,8}",
            row_count in any::<u64>(),
            footer_len in any::<u32>(),
        ) -> ParquetFile {
            ParquetFile { profile, bucket, key, size, etag, version, row_count, footer_len }
        }
    }

    fn arb_manifest() -> impl Strategy<Value = Manifest> {
        (
            "[a-z_][a-z0-9_]{0,20}",
            1u64..=u64::MAX,
            prop::collection::vec(arb_file(), 1..6),
            prop::collection::btree_map("[a-z._]{1,12}", ".{0,12}", 0..4),
            ".{0,16}",
            any::<i64>(),
            ".{0,40}",
            any::<[u8; APPLY_NONCE_LEN]>(),
        )
            .prop_filter("reserved table name", |(table, ..)| {
                validate_table(table).is_ok()
            })
            .prop_map(
                |(table, version, mut files, options, by, ns, stmt, nonce)| {
                    // Distinct (profile, bucket, key) triples: a duplicate is
                    // a refused manifest, not a round-trip case.
                    files.dedup_by(|a, b| {
                        (&a.profile, &a.bucket, &a.key) == (&b.profile, &b.bucket, &b.key)
                    });
                    Manifest {
                        table,
                        version,
                        dropped: false,
                        location: "s3://b/data/".into(),
                        grant: "s3://b/data".into(),
                        files,
                        options,
                        created_by: by,
                        created_unix_ns: ns,
                        statement: stmt,
                        apply_nonce: nonce.to_vec(),
                    }
                },
            )
    }

    proptest! {
        #[test]
        fn encode_decode_round_trips(m in arb_manifest()) {
            let key = manifest_key(&TENANT, &m.table, m.version).expect("key");
            let bytes = encode_manifest(&TENANT, &m).expect("encode");
            prop_assert_eq!(decode_manifest(&key, &bytes), Ok(m));
        }

        #[test]
        fn a_version_two_manifest_verifies_under_its_key_only(
            m in arb_manifest(),
            other in any::<[u8; 32]>(),
        ) {
            prop_assume!(other != [0x42; 32]);
            let key = manifest_key(&TENANT, &m.table, m.version).expect("key");
            let bytes = encode_manifest_with(&TENANT, &m, MAC_FORMAT_VERSION, Some(&mac_key()))
                .expect("encode");
            prop_assert_eq!(
                decode_authenticated(&key, &bytes, &mac_key()),
                Ok((m.clone(), MacStatus::Valid))
            );
            let wrong = ManifestMacKey::from_deployment_key(&other);
            prop_assert_eq!(
                decode_authenticated(&key, &bytes, &wrong),
                Ok((m, MacStatus::Invalid))
            );
        }

        #[test]
        fn mutated_bytes_never_panic_and_never_decode_to_an_invalid_manifest(
            m in arb_manifest(),
            cut in any::<prop::sample::Index>(),
            flip_at in any::<prop::sample::Index>(),
            flip in 1u8..=255,
        ) {
            let key = manifest_key(&TENANT, &m.table, m.version).expect("key");
            let bytes = encode_manifest(&TENANT, &m).expect("encode");
            let truncated = &bytes[..cut.index(bytes.len())];
            let mut flipped = bytes.clone();
            let at = flip_at.index(flipped.len());
            flipped[at] ^= flip;
            for input in [truncated, flipped.as_slice()] {
                if let Ok(decoded) = decode_manifest(&key, input) {
                    // Anything that decodes still passed every check, so a
                    // mutation can change a free-text field but never yield a
                    // misfiled or structurally invalid manifest.
                    prop_assert!(decoded.validate().is_ok());
                    prop_assert_eq!(&decoded.table, &m.table);
                    prop_assert_eq!(decoded.version, m.version);
                }
            }
        }

        #[test]
        fn arbitrary_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
            let key = manifest_key(&TENANT, "hits", 1).expect("key");
            let _ = decode_manifest(&key, &bytes);
        }
    }
}
