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
//! records each one as the tuple (profile, bucket, key).

use ravel_types::TenantHash;

use crate::names::{NameError, validate_table};

/// Filename suffix of a manifest version.
pub const MANIFEST_SUFFIX: &str = ".pqm";
/// Digits in a manifest key's zero-padded version, enough for any `u64`.
pub const VERSION_WIDTH: usize = 20;

/// Highest manifest version a writer creates and a reader resolves: 2^32.
///
/// Versions are dense: every DDL statement on a table writes exactly the next
/// one, so a table reaches this bound only after 2^32 statements, more than a
/// century at one statement per second. A version above it, up to the
/// `u64::MAX` a 20-digit key can spell, can only come from a put that did not
/// go through [`crate::writer::apply`], such as one made directly with the
/// Query credential, whose create-only grant admits any 20-digit version; at
/// `u64::MAX` it would leave the table no successor and every later DDL would
/// fail. [`crate::resolve::newest`] ignores such a version, the writer refuses
/// to create one, and [`crate::repair`] deletes it. The bound does not tell a
/// forged version at or below it from a legitimate one: one put exactly at the
/// bound still leaves the writer no next version, until an operator removes
/// it with [`crate::repair::delete_version`]. The `u64` version type and
/// the 20-digit key stay as they are: the bound is a check on values, so every
/// key this build writes or reads is one an earlier build wrote and read too.
pub const MAX_MANIFEST_VERSION: u64 = 1 << 32;
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
fn split_tenant_pq(key: &str) -> Result<(TenantHash, &str), KeyError> {
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

/// A key listed under a manifest prefix, as a reader treats it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListedManifestKey {
    /// A manifest version key, exactly as [`parse_manifest_key`] reads it.
    Version(ParsedManifestKey),
    /// `t/<tenant_hash>/pq/t/<table>/v/<slot>.pqm` with a valid table name and
    /// a `slot` that is not a version in `1..=u64::MAX`: the wrong length, an
    /// extra path segment, more than `u64::MAX`, all zeros, or not all decimal
    /// digits. The Query grant spells the version as 20 single-character
    /// wildcards, but its `*` binds any run of segments before `/v/`, so it
    /// admits these keys; readers skip them as they skip a version above
    /// [`MAX_MANIFEST_VERSION`].
    InvalidVersion {
        tenant_hash: TenantHash,
        table: String,
        slot: String,
        reason: &'static str,
    },
    /// `t/<tenant_hash>/pq/t/<segment>/v/<20 characters>.pqm` whose
    /// `segment` is not a valid table name: an upper-case or reserved name,
    /// or a path such as `a/b`. The Query grant's `*` binds any such segment,
    /// so it admits these keys, but no table owns them. The tenant-wide
    /// listings skip one the store lists, and `ravel-cli parquet repair
    /// --stray` removes one whose key is its own [`store_path`]. The S3
    /// adapter cannot list a key holding a control character, an empty
    /// segment or a `.` or `..` segment: its listing fails instead. `rest`
    /// is the key text after [`tenant_manifest_prefix`].
    InvalidTable {
        tenant_hash: TenantHash,
        rest: String,
    },
}

/// Split a manifest-shaped key into its tenant, its validated table name and
/// the text between `v/` and the `.pqm` suffix.
fn split_manifest_key(key: &str) -> Result<(TenantHash, &str, &str), KeyError> {
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
    let Some(slot) = filename.strip_suffix(MANIFEST_SUFFIX) else {
        return Err(malformed(key, "expected a .pqm suffix"));
    };
    Ok((tenant_hash, table, slot))
}

/// Parse a key produced by [`manifest_key`].
pub fn parse_manifest_key(key: &str) -> Result<ParsedManifestKey, KeyError> {
    let (tenant_hash, table, digits) = split_manifest_key(key)?;
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

/// The tenant and the text after `pq/t/` of a key the Query grant's pattern
/// `t/<32 characters>/pq/t/*/v/<20 characters>.pqm` admits, whatever `*`
/// binds; `None` for any other key.
fn query_grant_shaped(key: &str) -> Option<(TenantHash, &str)> {
    let (tenant_hash, after_pq) = split_tenant_pq(key).ok()?;
    let rest = after_pq.strip_prefix("t/")?;
    let stem = rest.strip_suffix(MANIFEST_SUFFIX)?;
    let (slot_start, _) = stem.char_indices().rev().nth(VERSION_WIDTH - 1)?;
    stem[..slot_start]
        .ends_with("/v/")
        .then_some((tenant_hash, rest))
}

/// Parse a key a manifest listing returned. A key [`parse_manifest_key`]
/// accepts is [`ListedManifestKey::Version`]; one under a valid table's `v/`
/// prefix ending in `.pqm` whose `slot` names no version is
/// [`ListedManifestKey::InvalidVersion`]; one the Query grant admits whose
/// segment between `pq/t/` and `/v/` is not a valid table name is
/// [`ListedManifestKey::InvalidTable`]. Every other key is an error, as it is
/// for [`parse_manifest_key`]: a suffix other than `.pqm`, or a key with no
/// `/v/<20 characters>.pqm` ending outside a valid table's `v/` prefix.
pub fn parse_listed_manifest_key(key: &str) -> Result<ListedManifestKey, KeyError> {
    let (tenant_hash, table, slot) = match split_manifest_key(key) {
        Ok(parts) => parts,
        Err(err) => {
            return match query_grant_shaped(key) {
                Some((tenant_hash, rest)) => Ok(ListedManifestKey::InvalidTable {
                    tenant_hash,
                    rest: rest.to_string(),
                }),
                None => Err(err),
            };
        }
    };
    let reason = if slot.len() != VERSION_WIDTH || !slot.bytes().all(|b| b.is_ascii_digit()) {
        "version is not 20 decimal digits"
    } else {
        match slot.parse::<u64>() {
            Err(_) => "version does not fit in a u64",
            Ok(0) => "version is zero",
            Ok(_) => return parse_manifest_key(key).map(ListedManifestKey::Version),
        }
    };
    Ok(ListedManifestKey::InvalidVersion {
        tenant_hash,
        table: table.to_string(),
        slot: slot.to_string(),
        reason,
    })
}

/// The key the S3 adapter sends a request for `key` to: `object_store`'s
/// `Path::from`, which drops empty segments, percent-encodes a `.` or `..`
/// segment, and percent-encodes control characters, every non-ASCII byte and
/// ``\ { ^ } % ` ] " > [ ~ < # | * ?`` within a segment. Every key a builder
/// here produces is its own store path.
pub fn store_path(key: &str) -> String {
    object_store::path::Path::from(key).to_string()
}

/// Whether a request for `key` through the S3 adapter reaches `key` itself
/// ([`store_path`] leaves it unchanged). A delete of any other key goes to a
/// different key, so it reports success and leaves `key` in place.
pub fn is_store_path(key: &str) -> bool {
    object_store::path::Path::from(key).as_ref() == key
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
    fn the_version_bound_is_two_to_the_32_and_still_has_a_key() {
        assert_eq!(MAX_MANIFEST_VERSION, 4_294_967_296);
        let key = manifest_key(&TENANT_A, "hits", MAX_MANIFEST_VERSION).expect("key");
        assert!(key.ends_with("/v/00000000004294967296.pqm"), "{key}");
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

    #[test]
    fn a_listed_key_whose_slot_names_no_version_is_named_not_refused() {
        let th = "a1".repeat(16);
        let ok = format!("t/{th}/pq/t/hits/v/00000000000000000007.pqm");
        assert_eq!(
            parse_listed_manifest_key(&ok).expect("parse"),
            ListedManifestKey::Version(parse_manifest_key(&ok).expect("parse"))
        );
        for (slot, reason) in [
            ("99999999999999999999", "version does not fit in a u64"),
            ("00000000000000000000", "version is zero"),
            ("abcdefghijklmnopqrst", "version is not 20 decimal digits"),
            (
                "0000000000000000000\u{e9}",
                "version is not 20 decimal digits",
            ),
            ("000000000/0000000001", "version is not 20 decimal digits"),
            // Wrong length: 19 and 21 digits.
            ("0000000000000000001", "version is not 20 decimal digits"),
            ("000000000000000000001", "version is not 20 decimal digits"),
            // An extra path segment between the table's `v/` and a second
            // `v/`, which the Query grant's `*` binds. The slot the parser
            // reads is everything between the first `v/` and `.pqm`.
            (
                "q/v/00000000000000000001",
                "version is not 20 decimal digits",
            ),
        ] {
            let key = format!("t/{th}/pq/t/hits/v/{slot}.pqm");
            assert_eq!(
                parse_listed_manifest_key(&key).expect("parse"),
                ListedManifestKey::InvalidVersion {
                    tenant_hash: TENANT_A,
                    table: "hits".into(),
                    slot: slot.into(),
                    reason,
                },
                "{key:?}"
            );
        }
        // Only a key with a non-`.pqm` suffix, or with no `/v/<20>.pqm`
        // ending outside a valid table's `v/` prefix, stays foreign.
        for key in [
            format!("t/{th}/pq/t/hits/v/00000000000000000001.parquet"),
            format!("t/{th}/pq/t/hits/x/00000000000000000001.pqm"),
            format!("t/{th}/pq/t/Hits/v/00000000000000000001.parquet"),
            format!("t/{th}/pq/t/Hits/v/0000000000000001.txt"),
            format!("t/{th}/pq/t/Hits/v/00000000000000000001"),
            format!("t/{th}/pq/t/Hits/v/0000000000000000001.pqm"),
            format!("t/{th}/pq/t/Hits/v/000000000000000000001.pqm"),
            format!("t/{th}/pq/t/a/b/00000000000000000001.pqm"),
            format!("t/{th}/pq/t/hits/notes.txt"),
            format!("t/{th}/pq/x/Hits/v/00000000000000000001.pqm"),
        ] {
            assert!(parse_listed_manifest_key(&key).is_err(), "{key:?} parsed");
        }
    }

    #[test]
    fn a_listed_key_the_query_grant_admits_under_an_invalid_table_is_named() {
        let th = "a1".repeat(16);
        for rest in [
            "Hits/v/00000000000000000001.pqm",
            "logs/v/00000000000000000001.pqm",
            "l0/v/00000000000000000001.pqm",
            "a/b/v/00000000000000000001.pqm",
            "hits/x/v/00000000000000000001.pqm",
            "/v/00000000000000000001.pqm",
            // The 20 characters are any characters, as the grant's `?` is.
            "Hits/v/\u{1b}[2Jxxxxxxxxxxxxxxxx.pqm",
            "Hits/v/0000000000000000000\u{e9}.pqm",
        ] {
            let key = format!("t/{th}/pq/t/{rest}");
            assert_eq!(
                parse_listed_manifest_key(&key).expect("parse"),
                ListedManifestKey::InvalidTable {
                    tenant_hash: TENANT_A,
                    rest: rest.into(),
                },
                "{key:?}"
            );
            // Never a manifest key a writer or resolver accepts.
            assert!(parse_manifest_key(&key).is_err(), "{key:?}");
        }
    }
}
