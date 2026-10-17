//! Policy file parsing: `[defaults]` + `[tenants.<id>]` TOML tables, each
//! holding a list of rules keyed by attribute key (ADR-2993 decision 1).
//!
//! # Merge rule
//!
//! This mirrors `ravel-server`'s `--limits-file` shape
//! (`[defaults]` + `[tenants.<id>]`, field-by-field overlay where an absent
//! field inherits the base), translated from scalar fields to a list of
//! rules: a tenant table's rule for a given `key` REPLACES the defaults'
//! rule for that same `key` in full (a rule is one atomic unit, not a set of
//! independently-overlayable fields); a default rule for a `key` with no
//! tenant override is inherited unchanged. [`crate::rules::TenantRules`]
//! applies this per-key overlay when resolving a tenant's effective rule
//! set.
//!
//! Duplicate key, empty key, zero epoch, a retired epoch equal to the
//! current epoch, and a retired epoch listed more than once are each scoped
//! to ONE table at a time ([defaults] on its
//! own, then each `[tenants.<id>]` on its own), not to the merged effective
//! set: a duplicate inside `[defaults]` is refused even if no tenant ever
//! references it, and two tenant tables may each reuse a key name the other
//! also uses without conflict, since they are never merged with each other.
//!
//! A sanitize-name collision (ADR-2993 decision 1) is checked differently:
//! against each tenant's EFFECTIVE rule set ([defaults] overlaid by that
//! tenant's table, per the merge rule above), not against either table in
//! isolation. Checking tables in isolation would miss a default rule and a
//! tenant-added rule for a DIFFERENT key that happen to sanitize to the same
//! label: neither table alone ever sees both keys together. `[defaults]` is
//! also checked on its own, since it is the effective set for any tenant
//! with no override table at all. A tenant rule whose raw key equals a
//! default key is a replacement (one entry in the effective set), not a
//! collision; two different tenant tables are still never merged with each
//! other, so a sanitize collision between them is not a collision either.
//! [`crate::rules::TenantRules::resolve`] re-checks this on the effective
//! set it builds, so a hand-built [`PolicyFile`] that never went through
//! [`parse_policy_file`] cannot bypass it.

use std::collections::HashMap;

use serde::Deserialize;

/// Which IPCrypt mode a rule applies (ADR-2993 decision 2). Only the two
/// modes the ADR scopes this crate to; `ipcrypt-nd`/`ipcrypt-ndx` are out of
/// scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Variant {
    Deterministic,
    Prefix,
}

/// One policy rule: an attribute key, the variant to apply, and its epoch
/// history. One key = one rule (deliverable 2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rule {
    pub key: String,
    pub variant: Variant,
    pub epoch: u32,
    pub retired_epochs: Vec<u32>,
}

/// One parsed table (`[defaults]` or one `[tenants.<id>]` entry): the rules
/// it declares, already validated against the refusals in the module docs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RuleTable {
    pub rules: Vec<Rule>,
}

/// A fully parsed and validated policy file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PolicyFile {
    pub defaults: RuleTable,
    pub tenants: HashMap<String, RuleTable>,
}

/// Every typed refusal `parse_policy_file` can return (deliverable 2).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PolicyError {
    #[error("ipcrypt policy: TOML parse error: {0}")]
    Parse(String),
    #[error("ipcrypt policy: {table}: duplicate key {key:?}")]
    DuplicateKey { table: String, key: String },
    #[error("ipcrypt policy: {table}: a rule has an empty key")]
    EmptyKey { table: String },
    #[error("ipcrypt policy: {table}: key {key:?} has epoch 0; epochs must be positive")]
    ZeroEpoch { table: String, key: String },
    #[error(
        "ipcrypt policy: {table}: key {key:?} lists retired_epochs entry 0; retired epochs must be positive"
    )]
    ZeroRetiredEpoch { table: String, key: String },
    #[error(
        "ipcrypt policy: {table}: key {key:?} lists its own current epoch {epoch} in retired_epochs"
    )]
    RetiredEqualsCurrent {
        table: String,
        key: String,
        epoch: u32,
    },
    #[error(
        "ipcrypt policy: {table}: key {key:?} lists retired_epochs entry {epoch} more than once"
    )]
    DuplicateRetiredEpoch {
        table: String,
        key: String,
        epoch: u32,
    },
    #[error(
        "ipcrypt policy: {table}: keys {a:?} and {b:?} both sanitize to metric label {label:?}"
    )]
    SanitizeCollision {
        table: String,
        a: String,
        b: String,
        label: String,
    },
}

fn default_epoch() -> u32 {
    1
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum VariantToml {
    Deterministic,
    Prefix,
}

impl From<VariantToml> for Variant {
    fn from(v: VariantToml) -> Self {
        match v {
            VariantToml::Deterministic => Variant::Deterministic,
            VariantToml::Prefix => Variant::Prefix,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleToml {
    key: String,
    variant: VariantToml,
    #[serde(default = "default_epoch")]
    epoch: u32,
    #[serde(default)]
    retired_epochs: Vec<u32>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleTableToml {
    #[serde(default)]
    rules: Vec<RuleToml>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFileToml {
    #[serde(default)]
    defaults: RuleTableToml,
    #[serde(default)]
    tenants: HashMap<String, RuleTableToml>,
}

/// Parse and validate a policy file's text. `sanitize` is the metric label
/// sanitizer (`ravel_otlp::sanitize_label_name` in callers that have logs or
/// traces metric label collision at stake); this crate takes it as a plain
/// function argument rather than depending on `ravel-otlp`.
pub fn parse_policy_file(
    text: &str,
    sanitize: impl Fn(&str) -> String,
) -> Result<PolicyFile, PolicyError> {
    let file: PolicyFileToml =
        toml::from_str(text).map_err(|err| PolicyError::Parse(err.to_string()))?;

    let defaults = validate_table(&file.defaults, "[defaults]")?;
    check_sanitize_collisions("[defaults]", defaults.rules.iter(), &sanitize)?;

    let mut tenants = HashMap::with_capacity(file.tenants.len());
    let mut tenant_ids: Vec<&String> = file.tenants.keys().collect();
    tenant_ids.sort();
    for id in tenant_ids {
        let table = &file.tenants[id];
        let context = format!("[tenants.{id}]");
        let rules = validate_table(table, &context)?;
        let effective = merge_effective(&defaults.rules, &rules.rules);
        check_sanitize_collisions(&context, effective.into_iter(), &sanitize)?;
        tenants.insert(id.clone(), rules);
    }

    Ok(PolicyFile { defaults, tenants })
}

fn validate_table(table: &RuleTableToml, context: &str) -> Result<RuleTable, PolicyError> {
    let mut rules = Vec::with_capacity(table.rules.len());
    let mut seen_keys: HashMap<String, ()> = HashMap::with_capacity(table.rules.len());

    for r in &table.rules {
        if r.key.is_empty() {
            return Err(PolicyError::EmptyKey {
                table: context.to_string(),
            });
        }
        if seen_keys.contains_key(&r.key) {
            return Err(PolicyError::DuplicateKey {
                table: context.to_string(),
                key: r.key.clone(),
            });
        }
        if r.epoch == 0 {
            return Err(PolicyError::ZeroEpoch {
                table: context.to_string(),
                key: r.key.clone(),
            });
        }
        let mut seen_retired: HashMap<u32, ()> = HashMap::with_capacity(r.retired_epochs.len());
        for &retired in &r.retired_epochs {
            if retired == 0 {
                return Err(PolicyError::ZeroRetiredEpoch {
                    table: context.to_string(),
                    key: r.key.clone(),
                });
            }
            if retired == r.epoch {
                return Err(PolicyError::RetiredEqualsCurrent {
                    table: context.to_string(),
                    key: r.key.clone(),
                    epoch: r.epoch,
                });
            }
            if seen_retired.contains_key(&retired) {
                return Err(PolicyError::DuplicateRetiredEpoch {
                    table: context.to_string(),
                    key: r.key.clone(),
                    epoch: retired,
                });
            }
            seen_retired.insert(retired, ());
        }

        seen_keys.insert(r.key.clone(), ());

        rules.push(Rule {
            key: r.key.clone(),
            variant: r.variant.into(),
            epoch: r.epoch,
            retired_epochs: r.retired_epochs.clone(),
        });
    }

    Ok(RuleTable { rules })
}

/// A tenant's effective rule set: `defaults` overlaid by `tenant`, where a
/// tenant rule REPLACES the default rule for the same raw key (module doc
/// merge rule). Used only to feed [`check_sanitize_collisions`]; the actual
/// per-key overlay used at resolve time lives in
/// [`crate::rules::TenantRules::resolve`].
fn merge_effective<'a>(defaults: &'a [Rule], tenant: &'a [Rule]) -> Vec<&'a Rule> {
    let mut merged: HashMap<&str, &Rule> = HashMap::with_capacity(defaults.len() + tenant.len());
    for r in defaults {
        merged.insert(&r.key, r);
    }
    for r in tenant {
        merged.insert(&r.key, r);
    }
    merged.into_values().collect()
}

/// Refuse two rules in `rules` whose raw keys sanitize to the same label
/// (ADR-2993 decision 1). `context` is the table name reported in the error;
/// for a tenant's effective set this is `[tenants.<id>]`, even though one of
/// the colliding rules may be inherited from `[defaults]`.
fn check_sanitize_collisions<'a>(
    context: &str,
    rules: impl Iterator<Item = &'a Rule>,
    sanitize: &impl Fn(&str) -> String,
) -> Result<(), PolicyError> {
    let mut sorted: Vec<&Rule> = rules.collect();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));

    let mut seen_labels: HashMap<String, &str> = HashMap::with_capacity(sorted.len());
    for r in sorted {
        let label = sanitize(&r.key);
        if let Some(prior_key) = seen_labels.get(&label) {
            return Err(PolicyError::SanitizeCollision {
                table: context.to_string(),
                a: prior_key.to_string(),
                b: r.key.clone(),
                label,
            });
        }
        seen_labels.insert(label, &r.key);
    }
    Ok(())
}
