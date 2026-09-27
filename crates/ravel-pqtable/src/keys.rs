//! Object keys for Parquet tables (ADR-2040 decision D1):
//!
//! ```text
//! t/<tenant_hash>/pq/grants                           location grants record
//! t/<tenant_hash>/pq/t/<table>/v/<version:020>.pqm    table manifest version
//! ```
//!
//! `<tenant_hash>` is rendered as 32 lowercase hex characters, as
//! ravel-commit's keys render it. The parsers accept only the exact text the
//! builders produce.
//!
//! Ravel holds no keys for the Parquet files themselves: they stay in the
//! tenant's own bucket under the operator-granted locations, and a manifest
//! records each one as the tuple (profile, bucket, raw key bytes).

use ravel_types::TenantHash;

use crate::names::{NameError, validate_table};

/// Filename suffix of a manifest version.
pub const MANIFEST_SUFFIX: &str = ".pqm";
/// Digits in a manifest key's zero-padded version, enough for any `u64`.
pub const VERSION_WIDTH: usize = 20;
/// Last segment of the grants record key.
pub const GRANTS_SEGMENT: &str = "grants";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("malformed Parquet table key {key:?}: {reason}")]
    Malformed { key: String, reason: &'static str },
    #[error(transparent)]
    Name(#[from] NameError),
    #[error("manifest version 0 is not a valid version; versions start at 1")]
    ZeroVersion,
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

/// `t/<tenant_hash>/pq/t/`: every manifest version of one tenant.
pub fn tenant_manifest_prefix(tenant: &TenantHash) -> String {
    format!("t/{}/pq/t/", tenant.to_hex())
}

/// `t/<tenant_hash>/pq/grants`: one tenant's location grants record.
pub fn grants_key(tenant: &TenantHash) -> String {
    format!("{}{GRANTS_SEGMENT}", tenant_pq_prefix(tenant))
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

/// Split `t/<tenant_hash>/pq/<rest>` into the tenant and `<rest>`.
fn split_tenant_pq<'k>(key: &'k str) -> Result<(TenantHash, &'k str), KeyError> {
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
    let Some(rest) = after_tenant.strip_prefix("pq/") else {
        return Err(malformed(key, "expected a \"pq/\" segment"));
    };
    Ok((tenant, rest))
}

/// Parse a key produced by [`grants_key`].
pub fn parse_grants_key(key: &str) -> Result<TenantHash, KeyError> {
    let (tenant_hash, rest) = split_tenant_pq(key)?;
    if rest != GRANTS_SEGMENT {
        return Err(malformed(
            key,
            "expected a \"grants\" segment and nothing after it",
        ));
    }
    Ok(tenant_hash)
}

/// Parse a key produced by [`manifest_key`].
pub fn parse_manifest_key(key: &str) -> Result<ParsedManifestKey, KeyError> {
    let (tenant_hash, after_pq) = split_tenant_pq(key)?;
    let Some(rest) = after_pq.strip_prefix("t/") else {
        return Err(malformed(key, "unexpected Parquet table key kind"));
    };
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

    #[test]
    fn grants_key_has_the_documented_shape_and_round_trips() {
        let key = grants_key(&TENANT_A);
        assert_eq!(key, format!("t/{}/pq/grants", "a1".repeat(16)));
        assert_eq!(parse_grants_key(&key).expect("parse"), TENANT_A);
        assert_ne!(parse_grants_key(&key).expect("parse"), TENANT_B);
        assert!(key.starts_with(&tenant_pq_prefix(&TENANT_A)));
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
        let manifest = manifest_key(&TENANT_A, "hits", 1).expect("key");
        assert_eq!(
            parse_manifest_key(&manifest).expect("parse").tenant_hash,
            TENANT_A
        );
        assert_ne!(
            parse_manifest_key(&manifest).expect("parse").tenant_hash,
            TENANT_B
        );
        assert!(!manifest.starts_with(&tenant_pq_prefix(&TENANT_B)));
        assert!(!grants_key(&TENANT_A).starts_with(&tenant_pq_prefix(&TENANT_B)));
    }

    #[test]
    fn builders_refuse_invalid_names_and_version_zero() {
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
    fn parsers_refuse_every_other_shape() {
        let th = "a1".repeat(16);
        let upper = "A1".repeat(16);
        let grants_bad = [
            format!("x/{th}/pq/grants"),
            format!("t/{upper}/pq/grants"),
            format!("t/{}/pq/grants", "a1".repeat(15)),
            format!("t/{th}/px/grants"),
            format!("t/{th}/pq/grants/"),
            format!("t/{th}/pq/grants/extra"),
            format!("t/{th}/pq/grant"),
            format!("t/{th}/pq/grantsx"),
            format!("t/{th}/pq/t/hits/v/00000000000000000001.pqm"),
        ];
        for key in &grants_bad {
            assert!(parse_grants_key(key).is_err(), "{key:?} parsed");
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
            format!("t/{th}/pq/grants"),
        ];
        for key in &manifest_bad {
            assert!(parse_manifest_key(key).is_err(), "{key:?} parsed");
        }
    }
}
