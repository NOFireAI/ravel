//! Policy fingerprint (ADR-2993 decision 9): a blake3 digest over a
//! canonical, order-independent encoding of the effective rule set
//! (`[defaults]` plus every tenant, each keyed by tenant hash rather than
//! raw tenant id), covering each rule's key, variant, epoch, and sorted
//! retired epochs. No key material or raw tenant id enters the encoding.
//!
//! An empty policy (no rules anywhere) maps to the documented sentinel
//! [`EMPTY_POLICY_FINGERPRINT`] (all-zero bytes) rather than `None`: this
//! keeps the fingerprint type a plain `[u8; 32]` everywhere, including in
//! [`crate::marker::check_marker`], which needs a concrete byte value to
//! compare and print even when the local policy is empty.

use ravel_types::TenantHash;

use crate::policy::{PolicyFile, Rule, Variant};

pub const EMPTY_POLICY_FINGERPRINT: [u8; 32] = [0u8; 32];

/// Compute the fingerprint of `policy`'s effective rule set. `hash_tenant`
/// maps a `[tenants.<id>]` table's raw id to its [`TenantHash`] (the same
/// mapping `TenantRules::resolve` callers already have a deployment key and
/// `TenantHashScheme` to compute), so this crate never needs to depend on
/// tenant-hash-scheme configuration itself.
pub fn policy_fingerprint(
    policy: &PolicyFile,
    hash_tenant: impl Fn(&str) -> TenantHash,
) -> [u8; 32] {
    let is_empty =
        policy.defaults.rules.is_empty() && policy.tenants.values().all(|t| t.rules.is_empty());
    if is_empty {
        return EMPTY_POLICY_FINGERPRINT;
    }

    let mut buf = Vec::new();
    encode_table(&mut buf, &[0u8; 16], &policy.defaults.rules);

    let mut tenant_entries: Vec<([u8; 16], &Vec<Rule>)> = policy
        .tenants
        .iter()
        .filter(|(_, table)| !table.rules.is_empty())
        .map(|(id, table)| (hash_tenant(id).0, &table.rules))
        .collect();
    tenant_entries.sort_by_key(|entry| entry.0);
    for (hash, rules) in tenant_entries {
        encode_table(&mut buf, &hash, rules);
    }

    blake3::hash(&buf).into()
}

/// `tenant_hash` is all-zero for `[defaults]` (never a real tenant hash,
/// since `TenantHash` is 16 bytes derived from a non-empty tenant id) and
/// the real 16-byte tenant hash for every `[tenants.<id>]` table.
fn encode_table(buf: &mut Vec<u8>, tenant_hash: &[u8; 16], rules: &[Rule]) {
    buf.extend_from_slice(b"TABLE\0");
    buf.extend_from_slice(tenant_hash);

    let mut sorted: Vec<&Rule> = rules.iter().collect();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));

    buf.extend_from_slice(&(sorted.len() as u64).to_le_bytes());
    for rule in sorted {
        buf.extend_from_slice(b"RULE\0");
        buf.extend_from_slice(&(rule.key.len() as u64).to_le_bytes());
        buf.extend_from_slice(rule.key.as_bytes());
        buf.push(match rule.variant {
            Variant::Deterministic => 0,
            Variant::Prefix => 1,
        });
        buf.extend_from_slice(&rule.epoch.to_le_bytes());

        let mut retired = rule.retired_epochs.clone();
        retired.sort_unstable();
        retired.dedup();
        buf.extend_from_slice(&(retired.len() as u64).to_le_bytes());
        for epoch in retired {
            buf.extend_from_slice(&epoch.to_le_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::RuleTable;
    use std::collections::HashMap;

    fn hash_tenant(id: &str) -> TenantHash {
        let mut bytes = [0u8; 16];
        let id_bytes = id.as_bytes();
        let n = id_bytes.len().min(16);
        bytes[..n].copy_from_slice(&id_bytes[..n]);
        TenantHash(bytes)
    }

    fn rule(key: &str, variant: Variant, epoch: u32, retired: &[u32]) -> Rule {
        Rule {
            key: key.to_string(),
            variant,
            epoch,
            retired_epochs: retired.to_vec(),
        }
    }

    #[test]
    fn empty_policy_is_sentinel() {
        let policy = PolicyFile::default();
        assert_eq!(
            policy_fingerprint(&policy, hash_tenant),
            EMPTY_POLICY_FINGERPRINT
        );
    }

    #[test]
    fn empty_tenant_table_does_not_change_fingerprint() {
        let base = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Deterministic, 1, &[])],
            },
            tenants: HashMap::new(),
        };
        let mut with_empty_tenant = base.clone();
        with_empty_tenant
            .tenants
            .insert("acme".to_string(), RuleTable::default());
        assert_eq!(
            policy_fingerprint(&base, hash_tenant),
            policy_fingerprint(&with_empty_tenant, hash_tenant)
        );
    }

    #[test]
    fn unchanged_under_tenant_reordering() {
        let mut tenants_a = HashMap::new();
        tenants_a.insert(
            "acme".to_string(),
            RuleTable {
                rules: vec![rule("client.ip", Variant::Prefix, 1, &[])],
            },
        );
        tenants_a.insert(
            "globex".to_string(),
            RuleTable {
                rules: vec![rule("server.ip", Variant::Deterministic, 1, &[])],
            },
        );
        let policy_a = PolicyFile {
            defaults: RuleTable::default(),
            tenants: tenants_a,
        };

        // Same tenants, inserted in the opposite order; HashMap iteration
        // order is independent of insertion order already, but this also
        // exercises rule-within-table reordering.
        let mut tenants_b = HashMap::new();
        tenants_b.insert(
            "globex".to_string(),
            RuleTable {
                rules: vec![rule("server.ip", Variant::Deterministic, 1, &[])],
            },
        );
        tenants_b.insert(
            "acme".to_string(),
            RuleTable {
                rules: vec![rule("client.ip", Variant::Prefix, 1, &[])],
            },
        );
        let policy_b = PolicyFile {
            defaults: RuleTable::default(),
            tenants: tenants_b,
        };

        assert_eq!(
            policy_fingerprint(&policy_a, hash_tenant),
            policy_fingerprint(&policy_b, hash_tenant)
        );
    }

    #[test]
    fn unchanged_under_retired_epoch_reordering() {
        let policy_a = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Deterministic, 3, &[1, 2])],
            },
            tenants: HashMap::new(),
        };
        let policy_b = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Deterministic, 3, &[2, 1])],
            },
            tenants: HashMap::new(),
        };
        assert_eq!(
            policy_fingerprint(&policy_a, hash_tenant),
            policy_fingerprint(&policy_b, hash_tenant)
        );
    }

    #[test]
    fn changes_with_epoch() {
        let policy_a = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Deterministic, 1, &[])],
            },
            tenants: HashMap::new(),
        };
        let policy_b = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Deterministic, 2, &[])],
            },
            tenants: HashMap::new(),
        };
        assert_ne!(
            policy_fingerprint(&policy_a, hash_tenant),
            policy_fingerprint(&policy_b, hash_tenant)
        );
    }

    #[test]
    fn changes_with_variant() {
        let policy_a = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Deterministic, 1, &[])],
            },
            tenants: HashMap::new(),
        };
        let policy_b = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Prefix, 1, &[])],
            },
            tenants: HashMap::new(),
        };
        assert_ne!(
            policy_fingerprint(&policy_a, hash_tenant),
            policy_fingerprint(&policy_b, hash_tenant)
        );
    }

    #[test]
    fn changes_with_retired_epochs_set() {
        let policy_a = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Deterministic, 3, &[1, 2])],
            },
            tenants: HashMap::new(),
        };
        let policy_b = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Deterministic, 3, &[1])],
            },
            tenants: HashMap::new(),
        };
        assert_ne!(
            policy_fingerprint(&policy_a, hash_tenant),
            policy_fingerprint(&policy_b, hash_tenant)
        );
    }

    #[test]
    fn unchanged_under_duplicate_retired_epoch_entries() {
        // A hand-built PolicyFile (bypassing parse_policy_file, which refuses
        // a duplicate retired_epochs entry) must still fingerprint the same
        // as its deduplicated equivalent: the encoding dedupes after sorting.
        let policy_a = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Deterministic, 3, &[1, 2])],
            },
            tenants: HashMap::new(),
        };
        let policy_b = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Deterministic, 3, &[2, 1, 2, 1])],
            },
            tenants: HashMap::new(),
        };
        assert_eq!(
            policy_fingerprint(&policy_a, hash_tenant),
            policy_fingerprint(&policy_b, hash_tenant)
        );
    }

    #[test]
    fn changes_with_tenant_identity() {
        let mut tenants_a = HashMap::new();
        tenants_a.insert(
            "acme".to_string(),
            RuleTable {
                rules: vec![rule("client.ip", Variant::Prefix, 1, &[])],
            },
        );
        let policy_a = PolicyFile {
            defaults: RuleTable::default(),
            tenants: tenants_a,
        };

        let mut tenants_b = HashMap::new();
        tenants_b.insert(
            "globex".to_string(),
            RuleTable {
                rules: vec![rule("client.ip", Variant::Prefix, 1, &[])],
            },
        );
        let policy_b = PolicyFile {
            defaults: RuleTable::default(),
            tenants: tenants_b,
        };

        assert_ne!(
            policy_fingerprint(&policy_a, hash_tenant),
            policy_fingerprint(&policy_b, hash_tenant)
        );
    }

    #[test]
    fn changes_with_key() {
        let policy_a = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("client.ip", Variant::Deterministic, 1, &[])],
            },
            tenants: HashMap::new(),
        };
        let policy_b = PolicyFile {
            defaults: RuleTable {
                rules: vec![rule("server.ip", Variant::Deterministic, 1, &[])],
            },
            tenants: HashMap::new(),
        };
        assert_ne!(
            policy_fingerprint(&policy_a, hash_tenant),
            policy_fingerprint(&policy_b, hash_tenant)
        );
    }
}
