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
//! Both directions run [`Manifest::validate`], so a manifest this crate
//! encodes or decodes has a valid table name, a version of at least 1, a
//! dataset and file list consistent with `dropped`, and file keys that are
//! exactly the content-addressed keys of their own BLAKE3 under the manifest's
//! tenant and dataset.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;
use ravel_proto::parquet_table::v1 as pb;
use ravel_types::TenantHash;

use crate::keys::{KeyError, dataset_object_key, parse_manifest_key};
use crate::names::{NameError, validate_dataset, validate_table};

/// Format floor written into every manifest this build emits.
pub const PARQUET_TABLE_FORMAT_VERSION: u32 = 1;

/// Lowest `format_version` this build reads. Version 0 is an unstamped
/// record, which no supported writer produces.
pub const PARQUET_TABLE_MIN_READ_VERSION: u32 = 1;

/// Highest `format_version` this build reads. Equal to the writer stamp: the
/// record has had no additive change.
pub const PARQUET_TABLE_MAX_READ_VERSION: u32 = PARQUET_TABLE_FORMAT_VERSION;

/// One data object a manifest references.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetFile {
    pub key: String,
    pub size: u64,
    pub blake3: [u8; 32],
    pub row_count: u64,
    /// Footer length in bytes, excluding the 8-byte trailer.
    pub footer_len: u32,
}

/// One decoded manifest version. `format_version` is not carried: an encoded
/// manifest is always stamped [`PARQUET_TABLE_FORMAT_VERSION`], and a decoded
/// one has already passed the read window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub table: String,
    pub version: u64,
    pub dropped: bool,
    pub dataset: String,
    pub files: Vec<ParquetFile>,
    pub options: BTreeMap<String, String>,
    pub created_by: String,
    pub created_unix_ns: i64,
    pub statement: String,
}

/// What made a manifest body invalid.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestDefect {
    ZeroVersion,
    /// A live (not dropped) manifest with no files.
    NoFiles,
    /// A dropped manifest that still names a dataset, files or options.
    DroppedWithContent,
    /// A `blake3` field that is not 32 bytes.
    Blake3Len {
        index: usize,
        len: usize,
    },
    /// A file key that is not the content-addressed key of its own BLAKE3
    /// under this manifest's tenant and dataset.
    FileKeyMismatch {
        index: usize,
        expected: String,
        actual: String,
    },
    DuplicateFile {
        key: String,
    },
    EmptyFile {
        key: String,
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

impl Manifest {
    /// Check the body against the rules in the module docs, with file keys
    /// resolved under `tenant`.
    pub fn validate(&self, tenant: &TenantHash) -> Result<(), ManifestError> {
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
        if self.dropped {
            if !self.dataset.is_empty() || !self.files.is_empty() || !self.options.is_empty() {
                return invalid(ManifestDefect::DroppedWithContent);
            }
            return Ok(());
        }
        validate_dataset(&self.dataset)?;
        if self.files.is_empty() {
            return invalid(ManifestDefect::NoFiles);
        }
        let mut seen = BTreeSet::new();
        for (index, file) in self.files.iter().enumerate() {
            let expected = dataset_object_key(tenant, &self.dataset, &file.blake3)?;
            if file.key != expected {
                return invalid(ManifestDefect::FileKeyMismatch {
                    index,
                    expected,
                    actual: file.key.clone(),
                });
            }
            if file.size == 0 {
                return invalid(ManifestDefect::EmptyFile {
                    key: file.key.clone(),
                });
            }
            if !seen.insert(file.key.as_str()) {
                return invalid(ManifestDefect::DuplicateFile {
                    key: file.key.clone(),
                });
            }
        }
        Ok(())
    }

    /// True unless this version records a DROP.
    pub fn is_live(&self) -> bool {
        !self.dropped
    }
}

/// Encode `manifest` for storage under `tenant`, stamped with
/// [`PARQUET_TABLE_FORMAT_VERSION`]. Refuses a manifest that fails
/// [`Manifest::validate`].
pub fn encode_manifest(tenant: &TenantHash, manifest: &Manifest) -> Result<Vec<u8>, ManifestError> {
    manifest.validate(tenant)?;
    let body = pb::ParquetTableManifest {
        format_version: PARQUET_TABLE_FORMAT_VERSION,
        table: manifest.table.clone(),
        version: manifest.version,
        dropped: manifest.dropped,
        dataset: manifest.dataset.clone(),
        files: manifest
            .files
            .iter()
            .map(|f| pb::ParquetFile {
                key: f.key.clone(),
                size: f.size,
                blake3: f.blake3.to_vec(),
                row_count: f.row_count,
                footer_len: f.footer_len,
            })
            .collect(),
        options: manifest.options.clone(),
        created_by: manifest.created_by.clone(),
        created_unix_ns: manifest.created_unix_ns,
        statement: manifest.statement.clone(),
    };
    Ok(body.encode_to_vec())
}

/// Decode the manifest stored at `key`. Refuses, with a typed error: a key
/// that is not a manifest key, bytes that are not a protobuf message, a
/// `format_version` outside the read window, a body whose table or version
/// differs from the key's, and a body that fails [`Manifest::validate`].
pub fn decode_manifest(key: &str, bytes: &[u8]) -> Result<Manifest, ManifestError> {
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
    let mut files = Vec::with_capacity(body.files.len());
    for (index, f) in body.files.into_iter().enumerate() {
        let blake3: [u8; 32] =
            f.blake3
                .as_slice()
                .try_into()
                .map_err(|_| ManifestError::Invalid {
                    table: body.table.clone(),
                    version: body.version,
                    defect: ManifestDefect::Blake3Len {
                        index,
                        len: f.blake3.len(),
                    },
                })?;
        files.push(ParquetFile {
            key: f.key,
            size: f.size,
            blake3,
            row_count: f.row_count,
            footer_len: f.footer_len,
        });
    }
    let manifest = Manifest {
        table: body.table,
        version: body.version,
        dropped: body.dropped,
        dataset: body.dataset,
        files,
        options: body.options,
        created_by: body.created_by,
        created_unix_ns: body.created_unix_ns,
        statement: body.statement,
    };
    manifest.validate(&parsed.tenant_hash)?;
    Ok(manifest)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::keys::manifest_key;

    const TENANT: TenantHash = TenantHash([0x5c; 16]);

    fn file(tenant: &TenantHash, dataset: &str, seed: u8) -> ParquetFile {
        let blake3 = *blake3::hash(&[seed]).as_bytes();
        ParquetFile {
            key: dataset_object_key(tenant, dataset, &blake3).expect("key"),
            size: 1000 + u64::from(seed),
            blake3,
            row_count: 7,
            footer_len: 321,
        }
    }

    fn live(version: u64) -> Manifest {
        Manifest {
            table: "hits".into(),
            version,
            dropped: false,
            dataset: "hits".into(),
            files: vec![file(&TENANT, "hits", 1), file(&TENANT, "hits", 2)],
            options: BTreeMap::from([("binary_as_string".into(), "true".into())]),
            created_by: "tenant-a".into(),
            created_unix_ns: 1_700_000_000_000_000_000,
            statement: "CREATE EXTERNAL TABLE hits STORED AS PARQUET LOCATION 'hits'".into(),
        }
    }

    fn raw_with_version(format_version: u32, m: &Manifest) -> Vec<u8> {
        let bytes = encode_manifest(&TENANT, m).expect("encode");
        let mut body = pb::ParquetTableManifest::decode(bytes.as_slice()).expect("decode");
        body.format_version = format_version;
        body.encode_to_vec()
    }

    #[test]
    fn the_read_window_is_exactly_version_one() {
        assert_eq!(PARQUET_TABLE_FORMAT_VERSION, 1);
        assert_eq!(PARQUET_TABLE_MIN_READ_VERSION, 1);
        assert_eq!(PARQUET_TABLE_MAX_READ_VERSION, 1);
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
                got: 2,
                ceiling: 1,
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
    fn a_manifest_read_under_another_tenants_key_is_refused() {
        let bytes = encode_manifest(&TENANT, &live(1)).expect("encode");
        let other = manifest_key(&TenantHash([0x77; 16]), "hits", 1).expect("key");
        assert!(matches!(
            decode_manifest(&other, &bytes),
            Err(ManifestError::Invalid {
                defect: ManifestDefect::FileKeyMismatch { index: 0, .. },
                ..
            })
        ));
    }

    #[test]
    fn invalid_bodies_are_refused_on_encode() {
        let mut m = live(1);
        m.files.clear();
        assert!(matches!(
            encode_manifest(&TENANT, &m),
            Err(ManifestError::Invalid {
                defect: ManifestDefect::NoFiles,
                ..
            })
        ));
        let mut m = live(1);
        m.files.push(m.files[0].clone());
        assert!(matches!(
            encode_manifest(&TENANT, &m),
            Err(ManifestError::Invalid {
                defect: ManifestDefect::DuplicateFile { .. },
                ..
            })
        ));
        let mut m = live(1);
        m.files[1].blake3 = [0; 32];
        assert!(matches!(
            encode_manifest(&TENANT, &m),
            Err(ManifestError::Invalid {
                defect: ManifestDefect::FileKeyMismatch { index: 1, .. },
                ..
            })
        ));
        let mut m = live(1);
        m.files[0].size = 0;
        assert!(matches!(
            encode_manifest(&TENANT, &m),
            Err(ManifestError::Invalid {
                defect: ManifestDefect::EmptyFile { .. },
                ..
            })
        ));
        let mut m = live(1);
        m.dropped = true;
        assert!(matches!(
            encode_manifest(&TENANT, &m),
            Err(ManifestError::Invalid {
                defect: ManifestDefect::DroppedWithContent,
                ..
            })
        ));
        assert!(matches!(
            encode_manifest(&TENANT, &live(0)),
            Err(ManifestError::Invalid {
                defect: ManifestDefect::ZeroVersion,
                ..
            })
        ));
    }

    #[test]
    fn a_short_blake3_is_a_typed_error() {
        let key = manifest_key(&TENANT, "hits", 1).expect("key");
        let bytes = encode_manifest(&TENANT, &live(1)).expect("encode");
        let mut body = pb::ParquetTableManifest::decode(bytes.as_slice()).expect("decode");
        body.files[1].blake3.truncate(31);
        assert!(matches!(
            decode_manifest(&key, &body.encode_to_vec()),
            Err(ManifestError::Invalid {
                defect: ManifestDefect::Blake3Len { index: 1, len: 31 },
                ..
            })
        ));
    }

    #[test]
    fn truncation_inside_the_last_field_is_a_decode_error() {
        let key = manifest_key(&TENANT, "hits", 1).expect("key");
        let bytes = encode_manifest(&TENANT, &live(1)).expect("encode");
        // `statement` (field 10) is encoded last and is longer than one byte,
        // so dropping the final byte cuts it mid-value.
        assert!(matches!(
            decode_manifest(&key, &bytes[..bytes.len() - 1]),
            Err(ManifestError::Decode { .. })
        ));
    }

    #[test]
    fn a_dropped_manifest_round_trips() {
        let m = Manifest {
            table: "hits".into(),
            version: 9,
            dropped: true,
            dataset: String::new(),
            files: vec![],
            options: BTreeMap::new(),
            created_by: String::new(),
            created_unix_ns: 5,
            statement: String::new(),
        };
        let key = manifest_key(&TENANT, "hits", 9).expect("key");
        let bytes = encode_manifest(&TENANT, &m).expect("encode");
        assert_eq!(decode_manifest(&key, &bytes), Ok(m));
    }

    fn arb_manifest() -> impl Strategy<Value = (TenantHash, Manifest)> {
        (
            any::<[u8; 16]>(),
            "[a-z_][a-z0-9_]{0,20}",
            1u64..=u64::MAX,
            "[a-z0-9_]{1,8}(/[a-z0-9_]{1,8}){0,3}",
            prop::collection::btree_set(any::<[u8; 32]>(), 1..6),
            prop::collection::btree_map("[a-z._]{1,12}", ".{0,12}", 0..4),
            ".{0,16}",
            any::<i64>(),
            ".{0,40}",
            any::<(u64, u32)>(),
        )
            .prop_filter("reserved table name", |(_, table, ..)| {
                validate_table(table).is_ok()
            })
            .prop_map(
                |(
                    tenant,
                    table,
                    version,
                    dataset,
                    digests,
                    options,
                    by,
                    ns,
                    stmt,
                    (rows, footer),
                )| {
                    let tenant = TenantHash(tenant);
                    let files = digests
                        .into_iter()
                        .enumerate()
                        .map(|(i, blake3)| ParquetFile {
                            key: dataset_object_key(&tenant, &dataset, &blake3)
                                .expect("valid dataset"),
                            size: 1 + i as u64,
                            blake3,
                            row_count: rows,
                            footer_len: footer,
                        })
                        .collect();
                    (
                        tenant,
                        Manifest {
                            table,
                            version,
                            dropped: false,
                            dataset,
                            files,
                            options,
                            created_by: by,
                            created_unix_ns: ns,
                            statement: stmt,
                        },
                    )
                },
            )
    }

    proptest! {
        #[test]
        fn encode_decode_round_trips((tenant, m) in arb_manifest()) {
            let key = manifest_key(&tenant, &m.table, m.version).expect("key");
            let bytes = encode_manifest(&tenant, &m).expect("encode");
            prop_assert_eq!(decode_manifest(&key, &bytes), Ok(m));
        }

        #[test]
        fn mutated_bytes_never_panic_and_never_decode_to_an_invalid_manifest(
            (tenant, m) in arb_manifest(),
            cut in any::<prop::sample::Index>(),
            flip_at in any::<prop::sample::Index>(),
            flip in 1u8..=255,
        ) {
            let key = manifest_key(&tenant, &m.table, m.version).expect("key");
            let bytes = encode_manifest(&tenant, &m).expect("encode");
            let truncated = &bytes[..cut.index(bytes.len())];
            let mut flipped = bytes.clone();
            let at = flip_at.index(flipped.len());
            flipped[at] ^= flip;
            for input in [truncated, flipped.as_slice()] {
                if let Ok(decoded) = decode_manifest(&key, input) {
                    // Anything that decodes still passed every check, so a
                    // mutation can change a free-text field but never yield a
                    // misfiled or structurally invalid manifest.
                    prop_assert!(decoded.validate(&tenant).is_ok());
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
