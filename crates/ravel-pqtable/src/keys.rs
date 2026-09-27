//! Object keys for Parquet tables (ADR-2040 decision D1):
//!
//! ```text
//! t/<tenant_hash>/pq/d/<dataset>/<blake3_hex64>.parquet    data object (content-addressed)
//! t/<tenant_hash>/pq/t/<table>/v/<version:020>.pqm         table manifest version
//! ```
//!
//! `<tenant_hash>` is rendered as 32 lowercase hex characters, as
//! ravel-commit's keys render it, and `<blake3_hex64>` is the object's full
//! 256-bit BLAKE3 as 64 lowercase hex characters, so the key names the bytes.
//! The parsers accept only the exact text the builders produce.

use ravel_types::TenantHash;

use crate::names::{NameError, validate_dataset, validate_table};

/// Filename suffix of a data object.
pub const DATA_SUFFIX: &str = ".parquet";
/// Filename suffix of a manifest version.
pub const MANIFEST_SUFFIX: &str = ".pqm";
/// Digits in a manifest key's zero-padded version, enough for any `u64`.
pub const VERSION_WIDTH: usize = 20;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("malformed Parquet table key {key:?}: {reason}")]
    Malformed { key: String, reason: &'static str },
    #[error(transparent)]
    Name(#[from] NameError),
    #[error("manifest version 0 is not a valid version; versions start at 1")]
    ZeroVersion,
}

/// A parsed [`dataset_object_key`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedDataObjectKey {
    pub tenant_hash: TenantHash,
    pub dataset: String,
    pub blake3: [u8; 32],
}

/// A parsed [`manifest_key`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedManifestKey {
    pub tenant_hash: TenantHash,
    pub table: String,
    pub version: u64,
}

/// `t/<tenant_hash>/pq/`: every Parquet table object of one tenant.
pub fn tenant_pq_prefix(tenant: &TenantHash) -> String {
    format!("t/{}/pq/", tenant.to_hex())
}

/// `t/<tenant_hash>/pq/d/`: every data object of one tenant.
pub fn tenant_data_prefix(tenant: &TenantHash) -> String {
    format!("t/{}/pq/d/", tenant.to_hex())
}

/// `t/<tenant_hash>/pq/t/`: every manifest version of one tenant.
pub fn tenant_manifest_prefix(tenant: &TenantHash) -> String {
    format!("t/{}/pq/t/", tenant.to_hex())
}

/// Digits in a data key's hex digest: the full 256-bit BLAKE3.
pub const BLAKE3_HEX_LEN: usize = 64;

/// A BLAKE3 digest as 64 lowercase hex characters.
pub fn blake3_hex(blake3: &[u8; 32]) -> String {
    hex::encode(blake3)
}

/// `t/<tenant_hash>/pq/d/<dataset>/<blake3_hex64>.parquet`.
pub fn dataset_object_key(
    tenant: &TenantHash,
    dataset: &str,
    blake3: &[u8; 32],
) -> Result<String, KeyError> {
    validate_dataset(dataset)?;
    Ok(format!(
        "{}{dataset}/{}{DATA_SUFFIX}",
        tenant_data_prefix(tenant),
        blake3_hex(blake3)
    ))
}

/// `t/<tenant_hash>/pq/d/<dataset>/`. A recursive LIST of it also returns the
/// objects of every dataset nested under it; a delimited LIST returns only the
/// dataset's own.
pub fn dataset_prefix(tenant: &TenantHash, dataset: &str) -> Result<String, KeyError> {
    validate_dataset(dataset)?;
    Ok(format!("{}{dataset}/", tenant_data_prefix(tenant)))
}

/// `t/<tenant_hash>/pq/t/<table>/v/<version:020>.pqm`.
pub fn manifest_key(tenant: &TenantHash, table: &str, version: u64) -> Result<String, KeyError> {
    if version == 0 {
        return Err(KeyError::ZeroVersion);
    }
    Ok(format!(
        "{}{version:0width$}{MANIFEST_SUFFIX}",
        manifest_prefix(tenant, table)?,
        width = VERSION_WIDTH
    ))
}

/// `t/<tenant_hash>/pq/t/<table>/v/`: every version of one table.
pub fn manifest_prefix(tenant: &TenantHash, table: &str) -> Result<String, KeyError> {
    validate_table(table)?;
    Ok(format!("{}{table}/v/", tenant_manifest_prefix(tenant)))
}

fn malformed(key: &str, reason: &'static str) -> KeyError {
    KeyError::Malformed {
        key: key.to_string(),
        reason,
    }
}

fn is_lower_hex(s: &str) -> bool {
    s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Split `t/<tenant_hash>/pq/<kind>/<rest>` into the tenant and `<rest>`.
fn split_pq_key<'k>(key: &'k str, kind: &str) -> Result<(TenantHash, &'k str), KeyError> {
    let Some(after_root) = key.strip_prefix("t/") else {
        return Err(malformed(key, "expected the key to start with \"t/\""));
    };
    let Some((tenant_hex, after_tenant)) = after_root.split_once('/') else {
        return Err(malformed(key, "expected a tenant hash segment"));
    };
    if tenant_hex.len() != 32 || !is_lower_hex(tenant_hex) {
        return Err(malformed(
            key,
            "tenant hash is not 32 lowercase hex characters",
        ));
    }
    let tenant = TenantHash::from_hex(tenant_hex)
        .map_err(|_| malformed(key, "tenant hash is not valid hex"))?;
    let Some(after_pq) = after_tenant.strip_prefix("pq/") else {
        return Err(malformed(key, "expected a \"pq/\" segment"));
    };
    let Some(rest) = after_pq
        .strip_prefix(kind)
        .and_then(|r| r.strip_prefix('/'))
    else {
        return Err(malformed(key, "unexpected Parquet table key kind"));
    };
    Ok((tenant, rest))
}

/// Parse a key produced by [`dataset_object_key`].
pub fn parse_dataset_object_key(key: &str) -> Result<ParsedDataObjectKey, KeyError> {
    let (tenant_hash, rest) = split_pq_key(key, "d")?;
    let Some((dataset, filename)) = rest.rsplit_once('/') else {
        return Err(malformed(key, "expected <dataset>/<blake3_hex64>.parquet"));
    };
    validate_dataset(dataset)?;
    let Some(digest) = filename.strip_suffix(DATA_SUFFIX) else {
        return Err(malformed(key, "expected a .parquet suffix"));
    };
    if digest.len() != BLAKE3_HEX_LEN || !is_lower_hex(digest) {
        return Err(malformed(key, "digest is not 64 lowercase hex characters"));
    }
    let mut blake3 = [0u8; 32];
    hex::decode_to_slice(digest, &mut blake3)
        .map_err(|_| malformed(key, "digest is not valid hex"))?;
    Ok(ParsedDataObjectKey {
        tenant_hash,
        dataset: dataset.to_string(),
        blake3,
    })
}

/// Parse a key produced by [`manifest_key`].
pub fn parse_manifest_key(key: &str) -> Result<ParsedManifestKey, KeyError> {
    let (tenant_hash, rest) = split_pq_key(key, "t")?;
    let Some((table, after_table)) = rest.split_once('/') else {
        return Err(malformed(key, "expected <table>/v/<version>.pqm"));
    };
    validate_table(table)?;
    let Some(filename) = after_table.strip_prefix("v/") else {
        return Err(malformed(key, "expected a \"v/\" segment"));
    };
    let Some(digits) = filename.strip_suffix(MANIFEST_SUFFIX) else {
        return Err(malformed(key, "expected a .pqm suffix"));
    };
    if digits.len() != VERSION_WIDTH || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(malformed(key, "version is not 20 decimal digits"));
    }
    let version: u64 = digits
        .parse()
        .map_err(|_| malformed(key, "version does not fit in a u64"))?;
    if version == 0 {
        return Err(KeyError::ZeroVersion);
    }
    Ok(ParsedManifestKey {
        tenant_hash,
        table: table.to_string(),
        version,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const TENANT_A: TenantHash = TenantHash([0xa1; 16]);
    const TENANT_B: TenantHash = TenantHash([0xb2; 16]);

    fn digest(seed: u8) -> [u8; 32] {
        *blake3::hash(&[seed]).as_bytes()
    }

    #[test]
    fn data_object_key_has_the_documented_shape_and_round_trips() {
        let d = digest(1);
        let key = dataset_object_key(&TENANT_A, "clickbench/hits", &d).expect("key");
        assert_eq!(
            key,
            format!(
                "t/{}/pq/d/clickbench/hits/{}.parquet",
                "a1".repeat(16),
                hex::encode(d)
            )
        );
        assert_eq!(key.rsplit('/').next().map(str::len), Some(64 + 8));
        let parsed = parse_dataset_object_key(&key).expect("parse");
        assert_eq!(
            parsed,
            ParsedDataObjectKey {
                tenant_hash: TENANT_A,
                dataset: "clickbench/hits".into(),
                blake3: d,
            }
        );
        assert_eq!(
            dataset_prefix(&TENANT_A, "clickbench/hits").expect("prefix"),
            format!("t/{}/pq/d/clickbench/hits/", "a1".repeat(16))
        );
        assert!(key.starts_with(&dataset_prefix(&TENANT_A, "clickbench/hits").expect("prefix")));
    }

    #[test]
    fn manifest_key_has_the_documented_shape_and_round_trips() {
        let key = manifest_key(&TENANT_A, "hits", 42).expect("key");
        assert_eq!(
            key,
            format!("t/{}/pq/t/hits/v/00000000000000000042.pqm", "a1".repeat(16))
        );
        assert_eq!(
            parse_manifest_key(&key).expect("parse"),
            ParsedManifestKey {
                tenant_hash: TENANT_A,
                table: "hits".into(),
                version: 42,
            }
        );
        let max = manifest_key(&TENANT_A, "hits", u64::MAX).expect("max key");
        assert!(max.ends_with("/v/18446744073709551615.pqm"));
        assert_eq!(parse_manifest_key(&max).expect("parse").version, u64::MAX);
        assert!(key.starts_with(&manifest_prefix(&TENANT_A, "hits").expect("prefix")));
        // Zero padding makes lexicographic order equal numeric order.
        let k9 = manifest_key(&TENANT_A, "hits", 9).expect("key");
        let k10 = manifest_key(&TENANT_A, "hits", 10).expect("key");
        assert!(k9 < k10);
    }

    #[test]
    fn a_key_for_tenant_a_never_parses_as_tenant_b() {
        let data = dataset_object_key(&TENANT_A, "hits", &digest(2)).expect("key");
        let manifest = manifest_key(&TENANT_A, "hits", 1).expect("key");
        assert_eq!(
            parse_dataset_object_key(&data).expect("parse").tenant_hash,
            TENANT_A
        );
        assert_ne!(
            parse_dataset_object_key(&data).expect("parse").tenant_hash,
            TENANT_B
        );
        assert_eq!(
            parse_manifest_key(&manifest).expect("parse").tenant_hash,
            TENANT_A
        );
        assert_ne!(
            parse_manifest_key(&manifest).expect("parse").tenant_hash,
            TENANT_B
        );
        assert!(!data.starts_with(&tenant_pq_prefix(&TENANT_B)));
        assert!(!manifest.starts_with(&tenant_pq_prefix(&TENANT_B)));
    }

    #[test]
    fn builders_refuse_invalid_names_and_version_zero() {
        assert!(matches!(
            dataset_object_key(&TENANT_A, "../x", &digest(1)),
            Err(KeyError::Name(NameError::InvalidDataset { .. }))
        ));
        assert!(matches!(
            dataset_prefix(&TENANT_A, "a/"),
            Err(KeyError::Name(NameError::InvalidDataset { .. }))
        ));
        assert!(matches!(
            manifest_key(&TENANT_A, "logs", 1),
            Err(KeyError::Name(NameError::InvalidTable { .. }))
        ));
        assert!(matches!(
            manifest_prefix(&TENANT_A, "Hits"),
            Err(KeyError::Name(NameError::InvalidTable { .. }))
        ));
        assert_eq!(
            manifest_key(&TENANT_A, "hits", 0),
            Err(KeyError::ZeroVersion)
        );
    }

    #[test]
    fn a_hash16_length_key_is_refused() {
        let d = digest(3);
        let full = dataset_object_key(&TENANT_A, "hits", &d).expect("key");
        assert_eq!(parse_dataset_object_key(&full).expect("parse").blake3, d);
        let short = format!(
            "t/{}/pq/d/hits/{}.parquet",
            "a1".repeat(16),
            &hex::encode(d)[..16]
        );
        assert_eq!(
            parse_dataset_object_key(&short),
            Err(KeyError::Malformed {
                key: short.clone(),
                reason: "digest is not 64 lowercase hex characters",
            })
        );
    }

    #[test]
    fn parsers_refuse_every_other_shape() {
        let th = "a1".repeat(16);
        let upper = "A1".repeat(16);
        let h64 = "0123456789abcdef".repeat(4);
        assert!(parse_dataset_object_key(&format!("t/{th}/pq/d/hits/{h64}.parquet")).is_ok());
        let data_bad = [
            format!("x/{th}/pq/d/hits/{h64}.parquet"),
            format!("t/{upper}/pq/d/hits/{h64}.parquet"),
            format!("t/{}/pq/d/hits/{h64}.parquet", "a1".repeat(15)),
            format!("t/{th}/px/d/hits/{h64}.parquet"),
            format!("t/{th}/pq/t/hits/{h64}.parquet"),
            format!("t/{th}/pq/d/{h64}.parquet"),
            format!("t/{th}/pq/d/Hits/{h64}.parquet"),
            format!("t/{th}/pq/d/a//{h64}.parquet"),
            format!("t/{th}/pq/d/hits/{h64}.rseg"),
            format!("t/{th}/pq/d/hits/{}.parquet", h64.to_uppercase()),
            format!("t/{th}/pq/d/hits/{}.parquet", &h64[..63]),
            format!("t/{th}/pq/d/hits/{h64}0.parquet"),
            format!("t/{th}/pq/d/hits/"),
            format!("t/{th}/pq/dd/hits/{h64}.parquet"),
        ];
        for key in &data_bad {
            assert!(parse_dataset_object_key(key).is_err(), "{key:?} parsed");
        }
        let manifest_bad = [
            format!("t/{th}/pq/t/hits/v/0000000000000000001.pqm"),
            format!("t/{th}/pq/t/hits/v/000000000000000000001.pqm"),
            format!("t/{th}/pq/t/hits/v/00000000000000000000.pqm"),
            format!("t/{th}/pq/t/hits/v/99999999999999999999.pqm"),
            format!("t/{th}/pq/t/hits/v/0000000000000000000a.pqm"),
            format!("t/{th}/pq/t/hits/v/00000000000000000001.parquet"),
            format!("t/{th}/pq/t/hits/x/00000000000000000001.pqm"),
            format!("t/{th}/pq/t/hits/v/sub/00000000000000000001.pqm"),
            format!("t/{th}/pq/t/logs/v/00000000000000000001.pqm"),
            format!("t/{th}/pq/t/Hits/v/00000000000000000001.pqm"),
            format!("t/{th}/pq/d/hits/v/00000000000000000001.pqm"),
            format!("t/{upper}/pq/t/hits/v/00000000000000000001.pqm"),
            format!("t/{th}/pq/t/hits"),
        ];
        for key in &manifest_bad {
            assert!(parse_manifest_key(key).is_err(), "{key:?} parsed");
        }
    }
}
