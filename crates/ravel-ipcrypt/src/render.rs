//! Value encryption/rendering (ADR-2993 decision 3): IPv4/IPv6 text (no zone
//! id) is replaced by its ciphertext in canonical text (RFC 5952 for IPv6);
//! `ip:port` / `[ipv6]:port` keep the port and bracket by the OUTPUT
//! family; anything else is reported unparsed and returned unchanged. That
//! "anything else" covers a hostname, an address with a zone id, an empty
//! string, and a bracketed IPv6 address with no port (`[2001:db8::1]`,
//! distinct from the `[ipv6]:port` form above, which does parse).
//!
//! [`key_from_bytes`] refuses a degenerate prefix key (equal 16-byte
//! halves) the same way [`crate::keys::derive_key`] does, via the shared
//! [`crate::keys::is_degenerate_prefix_key`] check: `IpcryptPfx::new` panics
//! on that input, so [`encrypt_address`] and [`decrypt_address`], which take
//! caller-supplied key bytes directly rather than a derived [`DerivedKey`],
//! must refuse it themselves before ever constructing one.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use ipcrypt_rs::{Ipcrypt, IpcryptPfx};

use crate::keys::{self, DerivedKey};
use crate::policy::Variant;

/// Whether a value's address syntax was recognized and transformed, or left
/// untouched because it was not an address this crate knows how to parse.
/// Shared by both directions ([`apply_value`] and [`invert_value`]):
/// `Encrypted` on the decrypt path means "the ciphertext form was
/// recognized and decrypted", not literally "an encryption happened".
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RenderOutcome {
    Encrypted,
    Unparsed,
}

/// The longest ciphertext rendering this crate ever produces: a bracketed
/// IPv6 address plus a 5-digit port, `[xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]:65535`
/// (1 + 39 + 1 + 1 + 5 = 47 bytes).
pub const MAX_RENDERED_LEN: usize = 47;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RenderKeyError {
    #[error("ipcrypt: {variant:?} key must be {expected} bytes, got {got}")]
    WrongKeyLen {
        variant: Variant,
        expected: usize,
        got: usize,
    },
    #[error("ipcrypt: prefix key has equal 16-byte halves; refusing a degenerate key")]
    DegeneratePrefixKey,
}

enum Target {
    Address(IpAddr),
    AddressPort(IpAddr, u16),
}

fn parse_target(s: &str) -> Option<Target> {
    if let Ok(ip) = s.parse::<IpAddr>() {
        return Some(Target::Address(ip));
    }
    if let Some(rest) = s.strip_prefix('[') {
        let close = rest.find(']')?;
        let (addr_part, after) = rest.split_at(close);
        let after = &after[1..];
        let port_str = after.strip_prefix(':')?;
        let ip = addr_part.parse::<Ipv6Addr>().ok()?;
        let port = port_str.parse::<u16>().ok()?;
        return Some(Target::AddressPort(IpAddr::V6(ip), port));
    }
    if let Some((addr_part, port_str)) = s.rsplit_once(':') {
        let ip = addr_part.parse::<Ipv4Addr>().ok()?;
        let port = port_str.parse::<u16>().ok()?;
        return Some(Target::AddressPort(IpAddr::V4(ip), port));
    }
    None
}

fn format_with_port(ip: IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(v4) => format!("{v4}:{port}"),
        IpAddr::V6(v6) => format!("[{v6}]:{port}"),
    }
}

fn encrypt_ip(key: &DerivedKey, ip: IpAddr) -> IpAddr {
    match key {
        DerivedKey::Deterministic(cipher) => cipher.encrypt_ipaddr(ip),
        DerivedKey::Prefix(cipher) => cipher.encrypt_ipaddr(ip),
    }
}

fn decrypt_ip(key: &DerivedKey, ip: IpAddr) -> IpAddr {
    match key {
        DerivedKey::Deterministic(cipher) => cipher.decrypt_ipaddr(ip),
        DerivedKey::Prefix(cipher) => cipher.decrypt_ipaddr(ip),
    }
}

pub(crate) fn apply_value(key: &DerivedKey, value: &str) -> (String, RenderOutcome) {
    match parse_target(value) {
        Some(Target::Address(ip)) => (encrypt_ip(key, ip).to_string(), RenderOutcome::Encrypted),
        Some(Target::AddressPort(ip, port)) => (
            format_with_port(encrypt_ip(key, ip), port),
            RenderOutcome::Encrypted,
        ),
        None => (value.to_string(), RenderOutcome::Unparsed),
    }
}

pub(crate) fn invert_value(key: &DerivedKey, value: &str) -> (String, RenderOutcome) {
    match parse_target(value) {
        Some(Target::Address(ip)) => (decrypt_ip(key, ip).to_string(), RenderOutcome::Encrypted),
        Some(Target::AddressPort(ip, port)) => (
            format_with_port(decrypt_ip(key, ip), port),
            RenderOutcome::Encrypted,
        ),
        None => (value.to_string(), RenderOutcome::Unparsed),
    }
}

fn key_from_bytes(variant: Variant, key_bytes: &[u8]) -> Result<DerivedKey, RenderKeyError> {
    match variant {
        Variant::Deterministic => {
            let arr: [u8; 16] = key_bytes
                .try_into()
                .map_err(|_| RenderKeyError::WrongKeyLen {
                    variant,
                    expected: 16,
                    got: key_bytes.len(),
                })?;
            Ok(DerivedKey::Deterministic(Box::new(Ipcrypt::new(arr))))
        }
        Variant::Prefix => {
            let arr: [u8; 32] = key_bytes
                .try_into()
                .map_err(|_| RenderKeyError::WrongKeyLen {
                    variant,
                    expected: 32,
                    got: key_bytes.len(),
                })?;
            if keys::is_degenerate_prefix_key(&arr) {
                return Err(RenderKeyError::DegeneratePrefixKey);
            }
            Ok(DerivedKey::Prefix(Box::new(IpcryptPfx::new(arr))))
        }
    }
}

/// Standalone encrypt entry point taking raw derived key material directly,
/// without needing a [`crate::rules::TenantRules`] (for a future SQL
/// function or CLI that already has the derived key in hand).
pub fn encrypt_address(
    variant: Variant,
    key_bytes: &[u8],
    text: &str,
) -> Result<(String, RenderOutcome), RenderKeyError> {
    let key = key_from_bytes(variant, key_bytes)?;
    Ok(apply_value(&key, text))
}

/// Inverse of [`encrypt_address`] (for a future CLI).
pub fn decrypt_address(
    variant: Variant,
    key_bytes: &[u8],
    text: &str,
) -> Result<(String, RenderOutcome), RenderKeyError> {
    let key = key_from_bytes(variant, key_bytes)?;
    Ok(invert_value(&key, text))
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A prefix key with distinct 16-byte halves (`IpcryptPfx::new` panics on
    /// equal halves; see [`crate::keys::prefix_key_from_bytes`] for the
    /// production-path guard against that).
    fn pfx_key(first_half: u8, second_half: u8) -> [u8; 32] {
        let mut key = [first_half; 32];
        key[16..].fill(second_half);
        key
    }

    #[test]
    fn ipv4_renders_canonical() {
        let key = DerivedKey::Deterministic(Box::new(Ipcrypt::new([1u8; 16])));
        let (out, outcome) = apply_value(&key, "192.0.2.1");
        assert_eq!(outcome, RenderOutcome::Encrypted);
        assert!(
            out.parse::<Ipv6Addr>().is_ok(),
            "deterministic always yields ipv6: {out}"
        );
    }

    #[test]
    fn deterministic_always_yields_ipv6() {
        let key = DerivedKey::Deterministic(Box::new(Ipcrypt::new([1u8; 16])));
        let (v4_out, _) = apply_value(&key, "192.0.2.1");
        let (v6_out, _) = apply_value(&key, "2001:db8::1");
        assert!(v4_out.parse::<Ipv6Addr>().is_ok());
        assert!(v6_out.parse::<Ipv6Addr>().is_ok());
    }

    #[test]
    fn prefix_preserves_family() {
        let key = DerivedKey::Prefix(Box::new(IpcryptPfx::new(pfx_key(2, 9))));
        let (v4_out, _) = apply_value(&key, "192.0.2.1");
        let (v6_out, _) = apply_value(&key, "2001:db8::1");
        assert!(v4_out.parse::<Ipv4Addr>().is_ok(), "{v4_out}");
        assert!(v6_out.parse::<Ipv6Addr>().is_ok(), "{v6_out}");
    }

    #[test]
    fn ipv4_port_keeps_port_and_brackets_by_output_family_v4() {
        // prefix preserves the v4 family, so the output stays unbracketed.
        let key = DerivedKey::Prefix(Box::new(IpcryptPfx::new(pfx_key(2, 9))));
        let (out, outcome) = apply_value(&key, "192.0.2.1:8080");
        assert_eq!(outcome, RenderOutcome::Encrypted);
        assert!(out.ends_with(":8080"));
        assert!(!out.starts_with('['));
    }

    #[test]
    fn ipv4_port_deterministic_output_is_bracketed_ipv6() {
        // deterministic always yields ipv6, so the OUTPUT family forces brackets
        // even though the input was v4:port.
        let key = DerivedKey::Deterministic(Box::new(Ipcrypt::new([1u8; 16])));
        let (out, outcome) = apply_value(&key, "192.0.2.1:8080");
        assert_eq!(outcome, RenderOutcome::Encrypted);
        assert!(out.starts_with('['));
        assert!(out.ends_with(":8080"));
    }

    #[test]
    fn bracketed_ipv6_port_round_trips_shape() {
        let key = DerivedKey::Prefix(Box::new(IpcryptPfx::new(pfx_key(2, 9))));
        let (out, outcome) = apply_value(&key, "[2001:db8::1]:443");
        assert_eq!(outcome, RenderOutcome::Encrypted);
        assert!(out.starts_with('['));
        assert!(out.ends_with(":443"));
    }

    #[test]
    fn hostname_unparsed_unchanged() {
        let key = DerivedKey::Deterministic(Box::new(Ipcrypt::new([1u8; 16])));
        let (out, outcome) = apply_value(&key, "example.com");
        assert_eq!(outcome, RenderOutcome::Unparsed);
        assert_eq!(out, "example.com");
    }

    #[test]
    fn zone_id_unparsed_unchanged() {
        let key = DerivedKey::Deterministic(Box::new(Ipcrypt::new([1u8; 16])));
        let (out, outcome) = apply_value(&key, "fe80::1%eth0");
        assert_eq!(outcome, RenderOutcome::Unparsed);
        assert_eq!(out, "fe80::1%eth0");
    }

    #[test]
    fn bracketed_ipv6_without_port_unparsed_unchanged() {
        // ADR-2993 decision 3: only the `[ipv6]:port` shape parses. A
        // bracketed address with no port is a different shape and is left
        // unchanged, same as a hostname or a zone-id address.
        let key = DerivedKey::Deterministic(Box::new(Ipcrypt::new([1u8; 16])));
        let (out, outcome) = apply_value(&key, "[2001:db8::1]");
        assert_eq!(outcome, RenderOutcome::Unparsed);
        assert_eq!(out, "[2001:db8::1]");
    }

    #[test]
    fn empty_string_unparsed_unchanged() {
        let key = DerivedKey::Deterministic(Box::new(Ipcrypt::new([1u8; 16])));
        let (out, outcome) = apply_value(&key, "");
        assert_eq!(outcome, RenderOutcome::Unparsed);
        assert_eq!(out, "");
    }

    #[test]
    fn longest_output_within_bound() {
        let key = DerivedKey::Deterministic(Box::new(Ipcrypt::new([1u8; 16])));
        let (out, _) = apply_value(&key, "[2001:db8::1]:65535");
        assert!(out.len() <= MAX_RENDERED_LEN, "{out} ({} bytes)", out.len());
    }

    #[test]
    fn encrypt_then_decrypt_round_trips() {
        for key in [
            DerivedKey::Deterministic(Box::new(Ipcrypt::new([3u8; 16]))),
            DerivedKey::Prefix(Box::new(IpcryptPfx::new(pfx_key(4, 9)))),
        ] {
            let (encrypted, outcome) = apply_value(&key, "192.0.2.1:53");
            assert_eq!(outcome, RenderOutcome::Encrypted);
            let (decrypted, outcome) = invert_value(&key, &encrypted);
            assert_eq!(outcome, RenderOutcome::Encrypted);
            assert_eq!(decrypted, "192.0.2.1:53");
        }
    }

    #[test]
    fn standalone_entry_points_match_internal_apply() {
        let key_bytes = [5u8; 16];
        let (via_entry, outcome) =
            encrypt_address(Variant::Deterministic, &key_bytes, "192.0.2.1").expect("ok");
        let (via_internal, _) = apply_value(
            &DerivedKey::Deterministic(Box::new(Ipcrypt::new(key_bytes))),
            "192.0.2.1",
        );
        assert_eq!(via_entry, via_internal);
        assert_eq!(outcome, RenderOutcome::Encrypted);

        let (decrypted, _) =
            decrypt_address(Variant::Deterministic, &key_bytes, &via_entry).expect("ok");
        assert_eq!(decrypted, "192.0.2.1");
    }

    #[test]
    fn wrong_key_length_refused() {
        let err = encrypt_address(Variant::Deterministic, &[0u8; 8], "192.0.2.1").unwrap_err();
        assert_eq!(
            err,
            RenderKeyError::WrongKeyLen {
                variant: Variant::Deterministic,
                expected: 16,
                got: 8
            }
        );
    }

    #[test]
    fn encrypt_address_with_degenerate_prefix_key_is_refused_not_panic() {
        let degenerate = [7u8; 32];
        let err = encrypt_address(Variant::Prefix, &degenerate, "192.0.2.1").unwrap_err();
        assert_eq!(err, RenderKeyError::DegeneratePrefixKey);
    }

    #[test]
    fn decrypt_address_with_degenerate_prefix_key_is_refused_not_panic() {
        let degenerate = [7u8; 32];
        let err = decrypt_address(Variant::Prefix, &degenerate, "192.0.2.1").unwrap_err();
        assert_eq!(err, RenderKeyError::DegeneratePrefixKey);
    }
}
