//! IPCrypt-based pseudonymization of IP-valued attributes (ADR-2993,
//! `docs/adrs/2993-ipcrypt-attribute-pseudonymization.md`).
//!
//! # Threat model limit
//!
//! This crate replaces an IP-valued attribute's text with a ciphertext that
//! decodes back to a valid address, under a key derived from the deployment
//! key and the tenant's identity. It is pseudonymization, not anonymization:
//! the deterministic variant ([`policy::Variant::Deterministic`]) always
//! maps the same plaintext address to the same ciphertext within one
//! (tenant, rule, epoch), by design, so two occurrences of the same real
//! address are still linkable to each other through their shared ciphertext
//! ([`render::RenderOutcome`] does not distinguish "a new address" from "an
//! address seen before"). The prefix variant preserves the subnet
//! structure of the original address: it hides a single host while leaving
//! the containing network observable, which is weaker than deterministic for
//! a single host but is what least disrupts subnet-level grouping in a
//! query. Anyone who holds the deployment key (or a tenant's derived key)
//! can reverse the transform ([`render::decrypt_address`],
//! [`rules::decrypt_value`]): the pseudonym's privacy depends entirely on
//! that key staying out of an attacker's hands, exactly as for any other
//! keyed-encryption scheme. This crate does not attempt k-anonymity, differential
//! privacy, or any guarantee that holds against an adversary who already
//! has the key; see ADR-2993's Decision section for the full threat-model
//! discussion and why deterministic and prefix are the only two variants in
//! scope.

mod keys;

pub mod fingerprint;
pub mod marker;
pub mod policy;
pub mod render;
pub mod rules;
pub mod tally;

pub use keys::KeyDerivationError;
