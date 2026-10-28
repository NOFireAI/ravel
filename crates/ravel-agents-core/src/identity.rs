//! Identity hashing for agents and the ids minted inside them (ADR-3331
//! decision 2).
//!
//! Every derived id hashes [`DOMAIN`] first, then length-prefixed
//! components, so two different component lists never produce the same byte
//! stream. `agent_id` uses the u16 little-endian prefixes of
//! `SeriesId::compute`; the ids minted inside an agent prefix each component
//! with a u64 little-endian length and lead with a purpose label, so an id of
//! one purpose cannot equal an id of another for the same inputs.

use crate::keys::{AgentKey, AgentKind};

/// The one domain-separation constant for every Ravel Agents hash.
pub const DOMAIN: &[u8] = b"ravel-agent-v1";

/// 128-bit agent identity: `blake3(DOMAIN || u16le len(kind) || kind ||
/// u16le len(key) || key)[..16]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct AgentId(pub [u8; 16]);

impl AgentId {
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(s: &str) -> Option<AgentId> {
        let bytes = hex::decode(s).ok()?;
        bytes.try_into().ok().map(AgentId)
    }
}

/// blake3-256 of an immutable object's bytes; names it under `o/`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectHash(pub [u8; 32]);

impl ObjectHash {
    pub fn of(bytes: &[u8]) -> ObjectHash {
        ObjectHash(*blake3::hash(bytes).as_bytes())
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_slice(bytes: &[u8]) -> Option<ObjectHash> {
        <[u8; 32]>::try_from(bytes).ok().map(ObjectHash)
    }
}

/// A 128-bit id minted inside one agent: turn, step, effect or timer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id128(pub [u8; 16]);

impl Id128 {
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_slice(bytes: &[u8]) -> Option<Id128> {
        <[u8; 16]>::try_from(bytes).ok().map(Id128)
    }
}

/// A 256-bit content hash: request hash, step input hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Hash256(pub [u8; 32]);

impl Hash256 {
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_slice(bytes: &[u8]) -> Option<Hash256> {
        <[u8; 32]>::try_from(bytes).ok().map(Hash256)
    }
}

pub fn agent_id(kind: &AgentKind, key: &AgentKey) -> AgentId {
    let mut buf = Vec::with_capacity(DOMAIN.len() + 4 + kind.len() + key.len());
    buf.extend_from_slice(DOMAIN);
    push_u16_prefixed(&mut buf, kind.as_str().as_bytes());
    push_u16_prefixed(&mut buf, key.as_str().as_bytes());
    let digest = blake3::hash(&buf);
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest.as_bytes()[..16]);
    AgentId(out)
}

// AgentKind and AgentKey are capped at keys::MAX_NAME_BYTES (256), so the
// length always fits a u16.
fn push_u16_prefixed(buf: &mut Vec<u8>, bytes: &[u8]) {
    let len = u16::try_from(bytes.len()).unwrap_or(u16::MAX);
    buf.extend_from_slice(&len.to_le_bytes());
    buf.extend_from_slice(bytes);
}

fn derive(purpose: &str, parts: &[&[u8]]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(DOMAIN);
    for part in std::iter::once(purpose.as_bytes()).chain(parts.iter().copied()) {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    *hasher.finalize().as_bytes()
}

pub(crate) fn derive_128(purpose: &str, parts: &[&[u8]]) -> Id128 {
    let full = derive(purpose, parts);
    let mut out = [0u8; 16];
    out.copy_from_slice(&full[..16]);
    Id128(out)
}

/// Hash recorded with a request id: the canonical handler name and payload.
pub fn request_hash(handler: &str, payload: &[u8]) -> Hash256 {
    Hash256(derive("request", &[handler.as_bytes(), payload]))
}

/// Step id from incarnation, the input's accept_seq and the step key. Every
/// attempt at one input (a retry after a retryable failure, a turn resumed
/// under a new epoch) derives the same id for the same key, so a retry
/// replays the failed attempt's completed steps instead of re-running them
/// (`kernel::tests::a_retry_reuses_the_effect_and_step_identities_of_the_failed_attempt`).
pub fn step_id(incarnation: u64, accept_seq: u64, step_key: &str) -> Id128 {
    derive_128(
        "step",
        &[
            &incarnation.to_le_bytes(),
            &accept_seq.to_le_bytes(),
            step_key.as_bytes(),
        ],
    )
}

/// Canonical input hash a step or effect is journaled under.
pub fn step_input_hash(tool_name: &str, tool_version: &str, input: &[u8]) -> Hash256 {
    Hash256(derive(
        "step-input",
        &[tool_name.as_bytes(), tool_version.as_bytes(), input],
    ))
}

/// Effect id from incarnation, the input's accept_seq and the intent key; the
/// id a `provider_idempotency` tool passes to the provider. It does not
/// change across attempts at one input, so a retried turn recommitting the
/// same intent finds the failed attempt's effect record and never dispatches
/// it under a second id.
pub fn effect_id(incarnation: u64, accept_seq: u64, intent_key: &str) -> Id128 {
    derive_128(
        "effect",
        &[
            &incarnation.to_le_bytes(),
            &accept_seq.to_le_bytes(),
            intent_key.as_bytes(),
        ],
    )
}

/// Timer id from incarnation and the caller's stable timer key.
pub fn timer_id(incarnation: u64, timer_key: &str) -> Id128 {
    derive_128("timer", &[&incarnation.to_le_bytes(), timer_key.as_bytes()])
}

/// Turn id from incarnation, the input, the owner epoch that began the turn
/// and the input's earlier failed attempts, so a retried input runs under a
/// new turn id and a fence from the failed attempt cannot act on the retry.
/// Step and effect ids do not include it; see [`step_id`].
pub fn turn_id(incarnation: u64, accept_seq: u64, epoch: u64, failed_attempts: u32) -> Id128 {
    derive_128(
        "turn",
        &[
            &incarnation.to_le_bytes(),
            &accept_seq.to_le_bytes(),
            &epoch.to_le_bytes(),
            &failed_attempts.to_le_bytes(),
        ],
    )
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn kind(s: &str) -> AgentKind {
        AgentKind::new(s).expect("kind")
    }

    fn key(s: &str) -> AgentKey {
        AgentKey::new(s).expect("key")
    }

    #[test]
    fn agent_id_is_the_documented_byte_stream() {
        let mut expected = Vec::new();
        expected.extend_from_slice(b"ravel-agent-v1");
        expected.extend_from_slice(&5u16.to_le_bytes());
        expected.extend_from_slice(b"order");
        expected.extend_from_slice(&3u16.to_le_bytes());
        expected.extend_from_slice(b"o-1");
        let digest = blake3::hash(&expected);
        assert_eq!(
            agent_id(&kind("order"), &key("o-1")).0,
            digest.as_bytes()[..16]
        );
    }

    #[test]
    fn length_prefixes_separate_component_boundaries() {
        assert_ne!(
            agent_id(&kind("ab"), &key("c")),
            agent_id(&kind("a"), &key("bc"))
        );
        assert_ne!(step_id(1, 1, "x"), effect_id(1, 1, "x"));
        assert_ne!(step_id(1, 2, "x"), step_id(1, 1, "x"));
        assert_ne!(request_hash("ab", b"c"), request_hash("a", b"bc"));
    }

    #[test]
    fn object_hash_is_plain_blake3_of_the_bytes() {
        assert_eq!(ObjectHash::of(b"x").0, *blake3::hash(b"x").as_bytes());
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn distinct_kind_key_pairs_never_share_an_agent_id(
            k1 in "\\PC{1,12}", v1 in "\\PC{1,12}",
            k2 in "\\PC{1,12}", v2 in "\\PC{1,12}",
        ) {
            prop_assume!((k1.as_str(), v1.as_str()) != (k2.as_str(), v2.as_str()));
            prop_assert_ne!(
                agent_id(&kind(&k1), &key(&v1)),
                agent_id(&kind(&k2), &key(&v2))
            );
        }

        #[test]
        fn a_split_moved_between_kind_and_key_changes_the_agent_id(
            joined in "\\PC{3,24}", cut_a in 0usize..24, offset in 0usize..24,
        ) {
            // Every char boundary strictly inside `joined`; at least two.
            let bounds: Vec<usize> = joined
                .char_indices()
                .map(|(i, _)| i)
                .filter(|i| *i > 0)
                .collect();
            let n = bounds.len();
            let a = bounds[cut_a % n];
            let b = bounds[(cut_a + 1 + offset % (n - 1)) % n];
            prop_assert_ne!(
                agent_id(&kind(&joined[..a]), &key(&joined[a..])),
                agent_id(&kind(&joined[..b]), &key(&joined[b..]))
            );
        }
    }
}
