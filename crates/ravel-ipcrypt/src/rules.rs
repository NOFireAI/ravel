//! Resolved per-tenant rule set (deliverable 3): the effective rules for one
//! tenant, keyed both by raw attribute key (logs/traces lookup) and by
//! sanitized metric label name (metrics lookup), each holding its derived
//! key material.

use std::collections::HashMap;

use ravel_types::TenantHash;

use crate::keys::{self, DerivedKey, KeyDerivationError};
use crate::policy::{PolicyFile, Variant};
use crate::render::{self, RenderKeyError, RenderOutcome};

struct ResolvedRule {
    variant: Variant,
    epoch: u32,
    retired_epochs: Vec<u32>,
    key: DerivedKey,
}

impl std::fmt::Debug for ResolvedRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResolvedRule")
            .field("variant", &self.variant)
            .field("epoch", &self.epoch)
            .field("retired_epochs", &self.retired_epochs)
            .field("key", &self.key)
            .finish()
    }
}

/// A read-only view onto one resolved rule's non-secret metadata.
#[derive(Debug, Clone, Copy)]
pub struct RuleInfo<'a> {
    pub raw_key: &'a str,
    pub variant: Variant,
    pub epoch: u32,
    pub retired_epochs: &'a [u32],
}

/// Every typed refusal building a [`TenantRules`] can return.
#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error(transparent)]
    KeyDerivation(#[from] KeyDerivationError),
}

/// Resolved per-tenant IPCrypt rules: which attribute keys are
/// pseudonymized, under which variant, epoch, and derived key. No `Default`
/// impl or derive (deliverable 3); the one empty constructor is
/// [`TenantRules::none`].
pub struct TenantRules {
    by_key: HashMap<String, ResolvedRule>,
    label_to_key: HashMap<String, String>,
}

impl std::fmt::Debug for TenantRules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TenantRules")
            .field("by_key", &self.by_key)
            .finish()
    }
}

impl TenantRules {
    /// The one empty constructor: no rules, every lookup returns `None`,
    /// every `apply` is a no-op pass-through.
    pub fn none() -> Self {
        TenantRules {
            by_key: HashMap::new(),
            label_to_key: HashMap::new(),
        }
    }

    /// Resolve a tenant's effective rule set: `[defaults]` overlaid, per
    /// `key`, by `[tenants.<tenant_id>]` (see the merge rule documented on
    /// [`crate::policy`]), then derive each rule's key from
    /// `deployment_key`, `tenant_hash` and the rule's configured epoch.
    /// `sanitize` must be the same function used when the policy was
    /// parsed, so label lookups agree with the collision check already run
    /// over the raw keys.
    pub fn resolve(
        policy: &PolicyFile,
        tenant_id: &str,
        tenant_hash: &TenantHash,
        deployment_key: &[u8; 32],
        sanitize: impl Fn(&str) -> String,
    ) -> Result<Self, ResolveError> {
        let mut merged = HashMap::new();
        for rule in &policy.defaults.rules {
            merged.insert(rule.key.clone(), rule.clone());
        }
        if let Some(table) = policy.tenants.get(tenant_id) {
            for rule in &table.rules {
                merged.insert(rule.key.clone(), rule.clone());
            }
        }

        let mut by_key = HashMap::with_capacity(merged.len());
        let mut label_to_key = HashMap::with_capacity(merged.len());
        for (key, rule) in merged {
            let derived =
                keys::derive_key(deployment_key, tenant_hash, rule.epoch, rule.variant, &key)?;
            label_to_key.insert(sanitize(&key), key.clone());
            by_key.insert(
                key,
                ResolvedRule {
                    variant: rule.variant,
                    epoch: rule.epoch,
                    retired_epochs: rule.retired_epochs,
                    key: derived,
                },
            );
        }

        Ok(TenantRules {
            by_key,
            label_to_key,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Look up a rule by its raw attribute key (the logs/traces path).
    pub fn lookup_raw(&self, raw_key: &str) -> Option<RuleInfo<'_>> {
        let (key, rule) = self.by_key.get_key_value(raw_key)?;
        Some(RuleInfo {
            raw_key: key,
            variant: rule.variant,
            epoch: rule.epoch,
            retired_epochs: &rule.retired_epochs,
        })
    }

    /// Look up a rule by its sanitized metric label name (the metrics
    /// path), via the same `sanitize` function `resolve` was built with.
    pub fn lookup_label(&self, label: &str) -> Option<RuleInfo<'_>> {
        let raw_key = self.label_to_key.get(label)?;
        self.lookup_raw(raw_key)
    }

    /// Encrypt `value` under the rule looked up for `raw_key`. Returns
    /// `None` if there is no rule for that key (caller should leave the
    /// value untouched).
    pub fn apply(&self, raw_key: &str, value: &str) -> Option<(String, RenderOutcome)> {
        let rule = self.by_key.get(raw_key)?;
        Some(render::apply_value(&rule.key, value))
    }

    /// The minimum bytes a caller must reserve to store a rendered value
    /// for a field whose raw value is `raw_len` bytes: `max(47, raw_len)`
    /// (deliverable 3; see [`crate::render::MAX_RENDERED_LEN`]).
    pub fn max_stored_len(raw_len: usize) -> usize {
        raw_len.max(render::MAX_RENDERED_LEN)
    }
}

/// Standalone decrypt by raw derived key material, mirroring
/// [`crate::render::decrypt_address`] (re-exported here since a decrypt
/// never needs a resolved [`TenantRules`] -- it is handed the key directly).
pub fn decrypt_value(
    variant: Variant,
    key_bytes: &[u8],
    value: &str,
) -> Result<(String, RenderOutcome), RenderKeyError> {
    render::decrypt_address(variant, key_bytes, value)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::policy::{Rule, RuleTable};

    fn test_sanitize(key: &str) -> String {
        key.replace('.', "_")
    }

    fn sample_policy() -> PolicyFile {
        let mut tenants = HashMap::new();
        tenants.insert(
            "acme".to_string(),
            RuleTable {
                rules: vec![Rule {
                    key: "client.ip".to_string(),
                    variant: Variant::Prefix,
                    epoch: 2,
                    retired_epochs: vec![1],
                }],
            },
        );
        PolicyFile {
            defaults: RuleTable {
                rules: vec![
                    Rule {
                        key: "client.ip".to_string(),
                        variant: Variant::Deterministic,
                        epoch: 1,
                        retired_epochs: vec![],
                    },
                    Rule {
                        key: "server.ip".to_string(),
                        variant: Variant::Deterministic,
                        epoch: 1,
                        retired_epochs: vec![],
                    },
                ],
            },
            tenants,
        }
    }

    #[test]
    fn none_is_empty_and_applies_nothing() {
        let rules = TenantRules::none();
        assert!(rules.is_empty());
        assert!(rules.lookup_raw("client.ip").is_none());
        assert!(rules.apply("client.ip", "192.0.2.1").is_none());
    }

    #[test]
    fn tenant_overlay_replaces_default_rule_for_same_key() {
        let policy = sample_policy();
        let dep_key = [1u8; 32];
        let hash = TenantHash([9u8; 16]);
        let rules =
            TenantRules::resolve(&policy, "acme", &hash, &dep_key, test_sanitize).expect("ok");
        let info = rules.lookup_raw("client.ip").expect("present");
        assert_eq!(info.variant, Variant::Prefix);
        assert_eq!(info.epoch, 2);
        assert_eq!(info.retired_epochs, &[1]);
    }

    #[test]
    fn tenant_without_overlay_inherits_defaults() {
        let policy = sample_policy();
        let dep_key = [1u8; 32];
        let hash = TenantHash([9u8; 16]);
        let rules =
            TenantRules::resolve(&policy, "globex", &hash, &dep_key, test_sanitize).expect("ok");
        let info = rules.lookup_raw("client.ip").expect("present");
        assert_eq!(info.variant, Variant::Deterministic);
        assert_eq!(info.epoch, 1);
        let server_info = rules.lookup_raw("server.ip").expect("present");
        assert_eq!(server_info.variant, Variant::Deterministic);
    }

    #[test]
    fn label_lookup_matches_raw_lookup() {
        let policy = sample_policy();
        let dep_key = [1u8; 32];
        let hash = TenantHash([9u8; 16]);
        let rules =
            TenantRules::resolve(&policy, "globex", &hash, &dep_key, test_sanitize).expect("ok");
        let by_label = rules.lookup_label("client_ip").expect("present");
        assert_eq!(by_label.raw_key, "client.ip");
    }

    #[test]
    fn apply_uses_resolved_rule() {
        let policy = sample_policy();
        let dep_key = [1u8; 32];
        let hash = TenantHash([9u8; 16]);
        let rules =
            TenantRules::resolve(&policy, "globex", &hash, &dep_key, test_sanitize).expect("ok");
        let (out, outcome) = rules.apply("client.ip", "192.0.2.1").expect("rule present");
        assert_eq!(outcome, RenderOutcome::Encrypted);
        assert_ne!(out, "192.0.2.1");
        assert!(rules.apply("unknown.key", "192.0.2.1").is_none());
    }

    #[test]
    fn max_stored_len_floor_is_47() {
        assert_eq!(TenantRules::max_stored_len(0), 47);
        assert_eq!(TenantRules::max_stored_len(10), 47);
        assert_eq!(TenantRules::max_stored_len(100), 100);
    }
}
