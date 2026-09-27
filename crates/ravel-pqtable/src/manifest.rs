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
//! the raw bytes the listing returned, never as a URL: a URL would have to be
//! re-parsed to address the object again, and a key may hold bytes no URL
//! round trips.
//!
//! Both directions run [`Manifest::validate`], so a manifest this crate
//! encodes or decodes has a valid table name, a version of at least 1, a
//! 16-byte apply nonce, a location, grant and file list consistent with
//! `dropped`, and files that are individually addressable and distinct.

use std::collections::{BTreeMap, BTreeSet};

use prost::Message;
use ravel_proto::parquet_table::v1 as pb;

use crate::keys::{KeyError, parse_manifest_key};
use crate::names::{NameError, validate_table};

/// Format floor written into every manifest this build emits.
pub const PARQUET_TABLE_FORMAT_VERSION: u32 = 1;

/// Lowest `format_version` this build reads. Version 0 is an unstamped
/// record, which no supported writer produces.
pub const PARQUET_TABLE_MIN_READ_VERSION: u32 = 1;

/// Highest `format_version` this build reads. Equal to the writer stamp: the
/// record has had no additive change.
pub const PARQUET_TABLE_MAX_READ_VERSION: u32 = PARQUET_TABLE_FORMAT_VERSION;

/// Bytes in a manifest's per-apply nonce.
pub const APPLY_NONCE_LEN: usize = 16;

/// One external Parquet file a manifest pins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParquetFile {
    /// Credential profile the file is read through.
    pub profile: String,
    /// Bucket or container holding it.
    pub bucket: String,
    /// Object key, the raw bytes the listing returned.
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

/// One decoded manifest version. `format_version` is not carried: an encoded
/// manifest is always stamped [`PARQUET_TABLE_FORMAT_VERSION`], and a decoded
/// one has already passed the read window.
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

/// Encode `manifest`, stamped with [`PARQUET_TABLE_FORMAT_VERSION`]. Refuses
/// a manifest that fails [`Manifest::validate`].
pub fn encode_manifest(manifest: &Manifest) -> Result<Vec<u8>, ManifestError> {
    manifest.validate()?;
    let body = pb::ParquetTableManifest {
        format_version: PARQUET_TABLE_FORMAT_VERSION,
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
    use ravel_types::TenantHash;

    use super::*;
    use crate::keys::manifest_key;

    const TENANT: TenantHash = TenantHash([0x5c; 16]);

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
        let bytes = encode_manifest(m).expect("encode");
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
        let bytes = encode_manifest(&live(3)).expect("encode");
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
    fn a_raw_key_survives_the_round_trip_byte_for_byte() {
        // Bytes a URL would not round trip: an empty path segment, a percent
        // escape, a space, and a non-ASCII sequence that is not valid UTF-8.
        let raw: Vec<Vec<u8>> = vec![
            b"data//double/slash.parquet".to_vec(),
            b"data/100%25 done/x.parquet".to_vec(),
            b"data/a b c.parquet".to_vec(),
            vec![b'd', b'a', b't', b'a', b'/', 0xff, 0xfe, b'.', b'p'],
            "data/\u{e9}\u{4e2d}.parquet".as_bytes().to_vec(),
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
        let bytes = encode_manifest(&m).expect("encode");
        let decoded = decode_manifest(&key, &bytes).expect("decode");
        let got: Vec<Vec<u8>> = decoded.files.iter().map(|f| f.key.clone()).collect();
        assert_eq!(got, raw);
        assert_eq!(decoded, m);
    }

    #[test]
    fn invalid_bodies_are_refused_on_encode() {
        let cases: [(fn(&mut Manifest), ManifestDefect); 10] = [
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
                encode_manifest(&m),
                Err(ManifestError::Invalid {
                    table: "hits".into(),
                    version: 1,
                    defect: defect.clone(),
                }),
                "{defect:?}"
            );
        }
        assert!(matches!(
            encode_manifest(&live(0)),
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
                    encode_manifest(&m),
                    Err(ManifestError::Invalid {
                        defect: ManifestDefect::NonceLen { len: got },
                        ..
                    }) if got == len
                ),
                "{len}"
            );
        }
        let key = manifest_key(&TENANT, "hits", 1).expect("key");
        let bytes = encode_manifest(&live(1)).expect("encode");
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
        let bytes = encode_manifest(&live(1)).expect("encode");
        // `apply_nonce` is the highest field number, so it is encoded last and
        // dropping the final byte cuts it mid-value.
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
        let bytes = encode_manifest(&m).expect("encode");
        assert_eq!(decode_manifest(&key, &bytes), Ok(m));
    }

    prop_compose! {
        fn arb_file()(
            profile in "[a-z]{1,8}",
            bucket in "[a-z0-9-]{1,12}",
            key in prop::collection::vec(any::<u8>(), 1..24),
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
            let bytes = encode_manifest(&m).expect("encode");
            prop_assert_eq!(decode_manifest(&key, &bytes), Ok(m));
        }

        #[test]
        fn mutated_bytes_never_panic_and_never_decode_to_an_invalid_manifest(
            m in arb_manifest(),
            cut in any::<prop::sample::Index>(),
            flip_at in any::<prop::sample::Index>(),
            flip in 1u8..=255,
        ) {
            let key = manifest_key(&TENANT, &m.table, m.version).expect("key");
            let bytes = encode_manifest(&m).expect("encode");
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
