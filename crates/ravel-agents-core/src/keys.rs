//! Object keys under `t/<tenant_hash>/ag/` (ADR-3331 decision 2).
//!
//! Every builder checks its result with `is_addressable_key` (or
//! `is_addressable_prefix` for a listing prefix, since a key ending in `/` is
//! never addressable) and returns a typed [`KeyError`] instead of a key the
//! store would rewrite. No user-supplied text reaches a key: agents are named
//! by [`AgentId`], objects by [`ObjectHash`].

use ravel_object_store::{is_addressable_key, is_addressable_prefix};
use ravel_types::TenantHash;

use crate::identity::{AgentId, ObjectHash};

/// Byte cap on an agent kind or key.
pub const MAX_NAME_BYTES: usize = 256;

/// Width of one `_wake/<due_bucket>` bucket: one minute.
pub const WAKE_BUCKET_WIDTH_NS: i64 = 60_000_000_000;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("agent {what} is empty")]
    Empty { what: &'static str },
    #[error("agent {what} is {len} bytes, over the {MAX_NAME_BYTES}-byte cap")]
    TooLong { what: &'static str, len: usize },
    #[error("built key is not addressable by the object store: {key}")]
    Unaddressable { key: String },
    #[error("wake hint due time {due_unix_ns} is negative")]
    NegativeDue { due_unix_ns: i64 },
}

macro_rules! name_newtype {
    ($name:ident, $what:literal) => {
        /// Any non-empty UTF-8 string of at most [`MAX_NAME_BYTES`] bytes.
        #[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, KeyError> {
                let value = value.into();
                if value.is_empty() {
                    return Err(KeyError::Empty { what: $what });
                }
                if value.len() > MAX_NAME_BYTES {
                    return Err(KeyError::TooLong {
                        what: $what,
                        len: value.len(),
                    });
                }
                Ok($name(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            pub fn len(&self) -> usize {
                self.0.len()
            }

            pub fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
        }
    };
}

name_newtype!(AgentKind, "kind");
name_newtype!(AgentKey, "key");

/// `floor(due_unix_ns / WAKE_BUCKET_WIDTH_NS)`, written as 20 decimal digits
/// so lexicographic LIST order is due order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DueBucket(u64);

impl DueBucket {
    pub fn for_due(due_unix_ns: i64) -> Result<DueBucket, KeyError> {
        if due_unix_ns < 0 {
            return Err(KeyError::NegativeDue { due_unix_ns });
        }
        Ok(DueBucket((due_unix_ns / WAKE_BUCKET_WIDTH_NS) as u64))
    }

    pub fn index(self) -> u64 {
        self.0
    }

    fn segment(self) -> String {
        format!("{:020}", self.0)
    }
}

fn checked_key(key: String) -> Result<String, KeyError> {
    if is_addressable_key(&key) {
        Ok(key)
    } else {
        Err(KeyError::Unaddressable { key })
    }
}

fn checked_prefix(prefix: String) -> Result<String, KeyError> {
    if is_addressable_prefix(&prefix) {
        Ok(prefix)
    } else {
        Err(KeyError::Unaddressable { key: prefix })
    }
}

/// `t/<tenant_hash>/ag/`, the listing prefix of every agent object.
pub fn tenant_prefix(tenant: &TenantHash) -> Result<String, KeyError> {
    checked_prefix(format!("t/{}/ag/", tenant.to_hex()))
}

/// `t/<tenant_hash>/ag/<agent_id>/`, the listing prefix of one agent.
pub fn agent_prefix(tenant: &TenantHash, agent: &AgentId) -> Result<String, KeyError> {
    checked_prefix(format!("t/{}/ag/{}/", tenant.to_hex(), agent.to_hex()))
}

/// `t/<tenant_hash>/ag/<agent_id>/head`.
pub fn head_key(tenant: &TenantHash, agent: &AgentId) -> Result<String, KeyError> {
    checked_key(format!("t/{}/ag/{}/head", tenant.to_hex(), agent.to_hex()))
}

/// `t/<tenant_hash>/ag/<agent_id>/o/<object_hash>`.
pub fn object_key(
    tenant: &TenantHash,
    agent: &AgentId,
    object: &ObjectHash,
) -> Result<String, KeyError> {
    checked_key(format!(
        "t/{}/ag/{}/o/{}",
        tenant.to_hex(),
        agent.to_hex(),
        object.to_hex()
    ))
}

/// `t/<tenant_hash>/ag/_wake/<due_bucket>/<agent_id>`.
pub fn wake_key(
    tenant: &TenantHash,
    bucket: DueBucket,
    agent: &AgentId,
) -> Result<String, KeyError> {
    checked_key(format!(
        "t/{}/ag/_wake/{}/{}",
        tenant.to_hex(),
        bucket.segment(),
        agent.to_hex()
    ))
}

/// `t/<tenant_hash>/ag/_wake/`, listed by the wake scan in due order.
pub fn wake_prefix(tenant: &TenantHash) -> Result<String, KeyError> {
    checked_prefix(format!("t/{}/ag/_wake/", tenant.to_hex()))
}

/// `t/<tenant_hash>/ag/_dir/<agent_id>`.
pub fn dir_key(tenant: &TenantHash, agent: &AgentId) -> Result<String, KeyError> {
    checked_key(format!("t/{}/ag/_dir/{}", tenant.to_hex(), agent.to_hex()))
}

/// `t/<tenant_hash>/ag/_dir/`, listed for bounded agent pagination.
pub fn dir_prefix(tenant: &TenantHash) -> Result<String, KeyError> {
    checked_prefix(format!("t/{}/ag/_dir/", tenant.to_hex()))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::identity::agent_id;
    use proptest::prelude::*;

    fn tenant() -> TenantHash {
        TenantHash([0xab; 16])
    }

    #[test]
    fn layout_matches_the_adr_table() {
        let agent = AgentId([0x01; 16]);
        let t = "abababababababababababababababab";
        let a = "01010101010101010101010101010101";
        assert_eq!(
            head_key(&tenant(), &agent).expect("head"),
            format!("t/{t}/ag/{a}/head")
        );
        assert_eq!(
            object_key(&tenant(), &agent, &ObjectHash([0x02; 32])).expect("o"),
            format!("t/{t}/ag/{a}/o/{}", "02".repeat(32))
        );
        let bucket = DueBucket::for_due(3 * WAKE_BUCKET_WIDTH_NS + 5).expect("bucket");
        assert_eq!(
            wake_key(&tenant(), bucket, &agent).expect("wake"),
            format!("t/{t}/ag/_wake/00000000000000000003/{a}")
        );
        assert_eq!(
            dir_key(&tenant(), &agent).expect("dir"),
            format!("t/{t}/ag/_dir/{a}")
        );
        assert_eq!(tenant_prefix(&tenant()).expect("p"), format!("t/{t}/ag/"));
        assert_eq!(
            agent_prefix(&tenant(), &agent).expect("p"),
            format!("t/{t}/ag/{a}/")
        );
    }

    #[test]
    fn names_are_non_empty_and_capped() {
        assert_eq!(AgentKind::new(""), Err(KeyError::Empty { what: "kind" }));
        assert!(AgentKey::new("k".repeat(MAX_NAME_BYTES)).is_ok());
        assert_eq!(
            AgentKey::new("k".repeat(MAX_NAME_BYTES + 1)),
            Err(KeyError::TooLong {
                what: "key",
                len: MAX_NAME_BYTES + 1
            })
        );
        // The cap is bytes, not chars.
        assert!(AgentKey::new("\u{e9}".repeat(MAX_NAME_BYTES / 2 + 1)).is_err());
    }

    #[test]
    fn negative_due_is_typed() {
        assert_eq!(
            DueBucket::for_due(-1),
            Err(KeyError::NegativeDue { due_unix_ns: -1 })
        );
    }

    #[test]
    fn wake_buckets_list_in_due_order() {
        let early = DueBucket::for_due(9 * WAKE_BUCKET_WIDTH_NS).expect("b");
        let late = DueBucket::for_due(10 * WAKE_BUCKET_WIDTH_NS).expect("b");
        let agent = AgentId([0; 16]);
        assert!(
            wake_key(&tenant(), early, &agent).expect("k")
                < wake_key(&tenant(), late, &agent).expect("k")
        );
        assert_eq!(DueBucket::for_due(i64::MAX).expect("b").segment().len(), 20);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(10_000))]

        #[test]
        fn every_built_key_is_addressable(
            tenant_bytes in any::<[u8; 16]>(),
            kind in "\\PC{1,40}",
            key in prop_oneof!["\\PC{1,40}", "[/.~*%\\\\ ]{1,16}", Just("..".to_string())],
            object in any::<[u8; 32]>(),
            due in 0i64..=i64::MAX,
        ) {
            let tenant = TenantHash(tenant_bytes);
            let agent = agent_id(
                &AgentKind::new(kind).expect("kind"),
                &AgentKey::new(key).expect("key"),
            );
            let bucket = DueBucket::for_due(due).expect("bucket");
            for built in [
                head_key(&tenant, &agent),
                object_key(&tenant, &agent, &ObjectHash(object)),
                wake_key(&tenant, bucket, &agent),
                dir_key(&tenant, &agent),
            ] {
                let built = built.expect("key builds");
                prop_assert!(is_addressable_key(&built), "{}", built);
            }
            for built in [
                tenant_prefix(&tenant),
                agent_prefix(&tenant, &agent),
                wake_prefix(&tenant),
                dir_prefix(&tenant),
            ] {
                let built = built.expect("prefix builds");
                prop_assert!(is_addressable_prefix(&built), "{}", built);
            }
        }
    }
}
