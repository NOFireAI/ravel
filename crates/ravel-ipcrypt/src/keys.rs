//! Per-tenant, per-variant, per-epoch key derivation (ADR-2993 decision 2).
//! The rule's attribute key plays no part in the derived material: two rules
//! sharing a (tenant, variant, epoch) share a key (see the crate-level
//! threat-model note).
//!
//! `key = blake3::derive_key("ravel ipcrypt v1 " + variant, deployment_key
//! || tenant_hash || epoch_as_u64_le)`. Deterministic truncates the 32-byte
//! derived key to 16 bytes; prefix keeps all 32 bytes and refuses if the two
//! 16-byte halves are equal (an `IpcryptPfx::new` with equal halves panics,
//! so this check must run before any key material reaches that
//! constructor).

use ipcrypt_rs::{Ipcrypt, IpcryptPfx};
use ravel_types::TenantHash;

use crate::policy::Variant;

const CONTEXT_PREFIX: &str = "ravel ipcrypt v1 ";

/// A derived IPCrypt key, holding the constructed cipher ready to encrypt or
/// decrypt (built once here, after the degenerate-key check, rather than
/// reconstructed from raw bytes on every call -- see [`crate::render`]'s
/// `encrypt_ip`/`decrypt_ip`). Holds key material inside the cipher; never
/// implements the derived `Debug`, and the hand-written impl below prints a
/// placeholder instead of any byte.
// Both variants are boxed: each `Aes128` round-key schedule is large enough
// (one for `Ipcrypt`, two for `IpcryptPfx`) that an unboxed enum would make
// every `DerivedKey` reserve space for its larger variant regardless of
// which one is active (clippy::large_enum_variant).
pub(crate) enum DerivedKey {
    Deterministic(Box<Ipcrypt>),
    Prefix(Box<IpcryptPfx>),
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
            Ok(DerivedKey::Deterministic(Box::new(Ipcrypt::new(key))))
        }
        Variant::Prefix => prefix_key_from_bytes(full, rule_key, epoch),
    }
}

/// Whether a 32-byte prefix-variant key's two 16-byte halves are equal: the
/// one input `IpcryptPfx::new` panics on. Shared by [`derive_key`] (via
/// [`prefix_key_from_bytes`]) and the standalone render entry points
/// ([`crate::render::encrypt_address`], [`crate::render::decrypt_address`]),
/// so both refuse it before any key material reaches that constructor,
/// instead of each re-deriving the check (and risking one of the two
/// forgetting it).
pub(crate) fn is_degenerate_prefix_key(full: &[u8; 32]) -> bool {
    full[..16] == full[16..]
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
    if is_degenerate_prefix_key(&full) {
        return Err(KeyDerivationError::DegeneratePrefixKey {
            rule_key: rule_key.to_string(),
            epoch,
        });
    }
    Ok(DerivedKey::Prefix(Box::new(IpcryptPfx::new(full))))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::net::IpAddr;

    const PROBE_IP: IpAddr = IpAddr::V4(std::net::Ipv4Addr::new(192, 0, 2, 1));

    fn tenant_hash(byte: u8) -> TenantHash {
        TenantHash([byte; 16])
    }

    fn encrypt(key: &DerivedKey, ip: IpAddr) -> IpAddr {
        match key {
            DerivedKey::Deterministic(cipher) => cipher.encrypt_ipaddr(ip),
            DerivedKey::Prefix(cipher) => cipher.encrypt_ipaddr(ip),
        }
    }

    #[test]
    fn fixed_output_deterministic() {
        // `Ipcrypt`/`IpcryptPfx` expose no byte accessor, so this pins the
        // derived key indirectly: encrypting a fixed probe address under it
        // must match encrypting the same address under an independently
        // constructed cipher from the exact bytes blake3 derivation is known
        // to produce for this input (the bytes the pre-item-4 version of
        // this test asserted directly).
        let dep_key = [7u8; 32];
        let key = derive_key(
            &dep_key,
            &tenant_hash(1),
            1,
            Variant::Deterministic,
            "client.ip",
        )
        .expect("derivation succeeds");
        let expected_bytes = [
            247, 80, 47, 71, 88, 41, 37, 3, 177, 44, 74, 216, 164, 252, 216, 109,
        ];
        let expected = Ipcrypt::new(expected_bytes);
        assert_eq!(encrypt(&key, PROBE_IP), expected.encrypt_ipaddr(PROBE_IP));
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
        assert_ne!(encrypt(&k1, PROBE_IP), encrypt(&k2, PROBE_IP));
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
        assert!(matches!(det, DerivedKey::Deterministic(_)));
        assert!(matches!(pfx, DerivedKey::Prefix(_)));
        assert_ne!(encrypt(&det, PROBE_IP), encrypt(&pfx, PROBE_IP));
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
        assert_ne!(encrypt(&k1, PROBE_IP), encrypt(&k2, PROBE_IP));
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
        let expected = IpcryptPfx::new(distinct);
        assert_eq!(encrypt(&key, PROBE_IP), expected.encrypt_ipaddr(PROBE_IP));
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
        let rendered = format!("{det:?} {pfx:?}");
        // Neither cipher exposes its bytes, so the strongest check available
        // here is that the Debug output is exactly the redaction placeholder
        // (no accessor exists to assert the absence of a specific byte
        // string, unlike the pre-item-4 version of this test).
        assert_eq!(
            rendered,
            "DerivedKey::Deterministic(<redacted>) DerivedKey::Prefix(<redacted>)"
        );
    }
}
