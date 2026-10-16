//! Per-tenant, per-rule key derivation (ADR-2993 decision 2).
//!
//! `key = blake3::derive_key("ravel ipcrypt v1 " + variant, deployment_key
//! || tenant_hash || epoch_as_u64_le)`. Deterministic truncates the 32-byte
//! derived key to 16 bytes; prefix keeps all 32 bytes and refuses if the two
//! 16-byte halves are equal (an `IpcryptPfx::new` with equal halves panics,
//! so this check must run before any key material reaches that
//! constructor).

use ravel_types::TenantHash;

use crate::policy::Variant;

const CONTEXT_PREFIX: &str = "ravel ipcrypt v1 ";

/// A derived IPCrypt key. Holds raw key material; never implements the
/// derived `Debug`, and the hand-written impl below prints a placeholder
/// instead of any byte.
pub(crate) enum DerivedKey {
    Deterministic([u8; 16]),
    Prefix([u8; 32]),
}

impl std::fmt::Debug for DerivedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DerivedKey::Deterministic(_) => f.write_str("DerivedKey::Deterministic(<redacted>)"),
            DerivedKey::Prefix(_) => f.write_str("DerivedKey::Prefix(<redacted>)"),
        }
    }
}

/// Every typed refusal key derivation can return.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum KeyDerivationError {
    /// The derived 32-byte key for a prefix-preserving rule has two equal
    /// 16-byte halves. No key material is in this error: only the rule's
    /// (public) attribute key name and epoch, which identify which rule and
    /// epoch to re-key, not the secret itself.
    #[error(
        "ipcrypt: derived prefix key for rule {rule_key:?} epoch {epoch} has equal 16-byte halves; refusing a degenerate key"
    )]
    DegeneratePrefixKey { rule_key: String, epoch: u32 },
}

pub(crate) fn derive_key(
    deployment_key: &[u8; 32],
    tenant_hash: &TenantHash,
    epoch: u32,
    variant: Variant,
    rule_key: &str,
) -> Result<DerivedKey, KeyDerivationError> {
    let variant_tag = match variant {
        Variant::Deterministic => "deterministic",
        Variant::Prefix => "prefix",
    };
    let context = format!("{CONTEXT_PREFIX}{variant_tag}");

    let mut material = Vec::with_capacity(32 + 16 + 8);
    material.extend_from_slice(deployment_key);
    material.extend_from_slice(&tenant_hash.0);
    material.extend_from_slice(&u64::from(epoch).to_le_bytes());

    let full: [u8; 32] = blake3::derive_key(&context, &material);

    match variant {
        Variant::Deterministic => {
            let mut key = [0u8; 16];
            key.copy_from_slice(&full[..16]);
            Ok(DerivedKey::Deterministic(key))
        }
        Variant::Prefix => prefix_key_from_bytes(full, rule_key, epoch),
    }
}

/// Build a prefix-variant key from 32 derived bytes, refusing a degenerate
/// key whose two 16-byte halves are equal: `IpcryptPfx::new` panics on that
/// input, so this check must run before any key material reaches it.
/// Separated from [`derive_key`] so the refusal is directly testable without
/// needing to find a blake3 output that happens to collide (astronomically
/// unlikely by construction, not something a unit test can trigger through
/// real derivation).
fn prefix_key_from_bytes(
    full: [u8; 32],
    rule_key: &str,
    epoch: u32,
) -> Result<DerivedKey, KeyDerivationError> {
    if full[..16] == full[16..] {
        return Err(KeyDerivationError::DegeneratePrefixKey {
            rule_key: rule_key.to_string(),
            epoch,
        });
    }
    Ok(DerivedKey::Prefix(full))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn tenant_hash(byte: u8) -> TenantHash {
        TenantHash([byte; 16])
    }

    #[test]
    fn fixed_output_deterministic() {
        let dep_key = [7u8; 32];
        let key = derive_key(
            &dep_key,
            &tenant_hash(1),
            1,
            Variant::Deterministic,
            "client.ip",
        )
        .expect("derivation succeeds");
        match key {
            DerivedKey::Deterministic(bytes) => {
                assert_eq!(
                    bytes,
                    [
                        247, 80, 47, 71, 88, 41, 37, 3, 177, 44, 74, 216, 164, 252, 216, 109,
                    ]
                );
            }
            DerivedKey::Prefix(_) => unreachable!("deterministic variant yields Deterministic"),
        }
    }

    #[test]
    fn changing_tenant_changes_key() {
        let dep_key = [7u8; 32];
        let k1 = derive_key(
            &dep_key,
            &tenant_hash(1),
            1,
            Variant::Deterministic,
            "client.ip",
        )
        .expect("derivation succeeds");
        let k2 = derive_key(
            &dep_key,
            &tenant_hash(2),
            1,
            Variant::Deterministic,
            "client.ip",
        )
        .expect("derivation succeeds");
        let (DerivedKey::Deterministic(b1), DerivedKey::Deterministic(b2)) = (k1, k2) else {
            unreachable!("deterministic variant yields Deterministic");
        };
        assert_ne!(b1, b2);
    }

    #[test]
    fn changing_variant_changes_key() {
        let dep_key = [7u8; 32];
        let det = derive_key(
            &dep_key,
            &tenant_hash(1),
            1,
            Variant::Deterministic,
            "client.ip",
        )
        .expect("derivation succeeds");
        let pfx = derive_key(&dep_key, &tenant_hash(1), 1, Variant::Prefix, "client.ip")
            .expect("derivation succeeds");
        let DerivedKey::Deterministic(det_bytes) = det else {
            unreachable!("deterministic variant yields Deterministic");
        };
        let DerivedKey::Prefix(pfx_bytes) = pfx else {
            unreachable!("prefix variant yields Prefix");
        };
        assert_ne!(det_bytes, pfx_bytes[..16]);
    }

    #[test]
    fn changing_epoch_changes_key() {
        let dep_key = [7u8; 32];
        let k1 = derive_key(
            &dep_key,
            &tenant_hash(1),
            1,
            Variant::Deterministic,
            "client.ip",
        )
        .expect("derivation succeeds");
        let k2 = derive_key(
            &dep_key,
            &tenant_hash(1),
            2,
            Variant::Deterministic,
            "client.ip",
        )
        .expect("derivation succeeds");
        let (DerivedKey::Deterministic(b1), DerivedKey::Deterministic(b2)) = (k1, k2) else {
            unreachable!("deterministic variant yields Deterministic");
        };
        assert_ne!(b1, b2);
    }

    #[test]
    fn degenerate_prefix_key_is_refused() {
        let equal_halves = [7u8; 32];
        let err = prefix_key_from_bytes(equal_halves, "client.ip", 1).unwrap_err();
        assert_eq!(
            err,
            KeyDerivationError::DegeneratePrefixKey {
                rule_key: "client.ip".to_string(),
                epoch: 1,
            }
        );
    }

    #[test]
    fn distinct_halves_prefix_key_accepted() {
        let mut distinct = [7u8; 32];
        distinct[31] = 8;
        let key = prefix_key_from_bytes(distinct, "client.ip", 1).expect("halves differ");
        match key {
            DerivedKey::Prefix(bytes) => assert_eq!(bytes, distinct),
            DerivedKey::Deterministic(_) => unreachable!(),
        }
    }

    #[test]
    fn no_key_bytes_in_debug() {
        let dep_key = [9u8; 32];
        let det = derive_key(
            &dep_key,
            &tenant_hash(3),
            1,
            Variant::Deterministic,
            "client.ip",
        )
        .expect("derivation succeeds");
        let pfx = derive_key(&dep_key, &tenant_hash(3), 1, Variant::Prefix, "client.ip")
            .expect("derivation succeeds");
        let det_bytes = match &det {
            DerivedKey::Deterministic(b) => *b,
            DerivedKey::Prefix(_) => unreachable!(),
        };
        let pfx_bytes = match &pfx {
            DerivedKey::Prefix(b) => *b,
            DerivedKey::Deterministic(_) => unreachable!(),
        };
        let rendered = format!("{det:?} {pfx:?}");
        assert!(!rendered.contains(&hex::encode(det_bytes)));
        assert!(!rendered.contains(&hex::encode(pfx_bytes)));
        assert!(rendered.contains("<redacted>"));
    }
}
