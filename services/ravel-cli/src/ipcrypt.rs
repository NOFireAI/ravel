//! IPCrypt pseudonymization in the operator CLI (ADR-2993): the parsed
//! `--ipcrypt-config` policy that `load` applies to the rows it writes, and
//! the `sys/ipcrypt/policy` marker check every such command runs before it
//! writes anything.
//!
//! The policy is the server's: the same file, parsed with the same metric
//! label sanitizer, fingerprinted over the same tenant hashes, so a bucket
//! loaded by `ravel-cli load` holds exactly the ciphertext the server would
//! have stored for the same values. Key bytes are never printed: errors name
//! tenant hashes, rule keys, epochs and fingerprints only.

use std::path::Path;
use std::sync::Arc;

use ravel_ipcrypt::fingerprint::{EMPTY_POLICY_FINGERPRINT, policy_fingerprint};
use ravel_ipcrypt::marker::{CheckOutcome, check_marker};
use ravel_ipcrypt::policy::{PolicyFile, parse_policy_file};
use ravel_ipcrypt::rules::TenantRules;
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{ConfiguredScheme, TenantId};

/// The metric label sanitizer rule keys are matched through on metric
/// surfaces (ADR-2993 decision 1), the one the server and the OTLP metric
/// normalizer use, so a rule on `client.ip` matches the `client_ip` label.
pub fn sanitize_rule_key(key: &str) -> String {
    ravel_otlp::normalize::sanitize_label_name(key.to_string())
}

/// The refusal for a non-empty policy with no deployment key: every rule's
/// key is derived from the deployment key (ADR-2993 decision 2).
pub const NO_DEPLOYMENT_KEY: &str = "--ipcrypt-config declares IPCrypt rules, but no deployment \
     key is configured to derive their keys from: pass --tenant-hash-key-file (a bucket pinned \
     unkeyed, or a command run with --tenant-hash-unkeyed, cannot carry an IPCrypt policy)";

/// Load and parse `--ipcrypt-config`. No path is the empty policy.
pub fn read_policy_file(path: Option<&Path>) -> anyhow::Result<PolicyFile> {
    let Some(path) = path else {
        return Ok(PolicyFile::default());
    };
    let text = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("could not read --ipcrypt-config {path:?}: {e}"))?;
    parse_policy_file(&text, sanitize_rule_key)
        .map_err(|e| anyhow::anyhow!("invalid --ipcrypt-config {}: {e}", path.display()))
}

/// The deployment key the global `--tenant-hash-key-file` names, read the way
/// every other command reads it. `None` without the flag.
pub fn deployment_key_from_flags(
    key_file: Option<&Path>,
    unkeyed: bool,
) -> anyhow::Result<Option<[u8; 32]>> {
    match crate::tenancy::configured_scheme_from_flags(key_file, unkeyed)? {
        ConfiguredScheme::Keyed(key) => Ok(Some(*key)),
        ConfiguredScheme::Unkeyed | ConfiguredScheme::Unspecified => Ok(None),
    }
}

/// One command's IPCrypt policy: the parsed file, its fingerprint, and the
/// deployment key its rules derive from.
pub struct IpcryptPolicy {
    policy: PolicyFile,
    fingerprint: [u8; 32],
    deployment_key: Option<Box<[u8; 32]>>,
}

impl std::fmt::Debug for IpcryptPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IpcryptPolicy")
            .field("fingerprint", &hex::encode(self.fingerprint))
            .field("keyed", &self.deployment_key.is_some())
            .finish_non_exhaustive()
    }
}

impl IpcryptPolicy {
    /// Fingerprint `policy` and refuse a non-empty one with no deployment
    /// key. Must run after the tenant-hash scheme is installed: the
    /// fingerprint keys each `[tenants.<id>]` table by `TenantId::hash`.
    pub fn new(policy: PolicyFile, deployment_key: Option<&[u8; 32]>) -> anyhow::Result<Self> {
        let fingerprint = policy_fingerprint(&policy, |id| TenantId::new(id).hash());
        if fingerprint != EMPTY_POLICY_FINGERPRINT && deployment_key.is_none() {
            anyhow::bail!(NO_DEPLOYMENT_KEY);
        }
        Ok(Self {
            policy,
            fingerprint,
            deployment_key: deployment_key.map(|key| Box::new(*key)),
        })
    }

    /// The empty policy, with no deployment key.
    pub fn empty() -> Self {
        Self {
            policy: PolicyFile::default(),
            fingerprint: EMPTY_POLICY_FINGERPRINT,
            deployment_key: None,
        }
    }

    /// The policy a command's `--ipcrypt-config` and the global tenant-hash
    /// flags select.
    pub fn from_flags(
        config: Option<&Path>,
        key_file: Option<&Path>,
        unkeyed: bool,
    ) -> anyhow::Result<Self> {
        let policy = read_policy_file(config)?;
        let key = deployment_key_from_flags(key_file, unkeyed)?;
        Self::new(policy, key.as_ref())
    }

    pub fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }

    pub fn is_empty(&self) -> bool {
        self.fingerprint == EMPTY_POLICY_FINGERPRINT
    }

    /// Compare this policy's fingerprint with the bucket's
    /// `sys/ipcrypt/policy` marker (ADR-2993 decision 9), whether or not
    /// `--ipcrypt-config` was given. No marker and an empty policy writes
    /// nothing; no marker and a non-empty policy creates it. A mismatch names
    /// both fingerprints, and a failure to read the marker refuses: it is
    /// never taken for an absent marker. `command` names the refused command.
    pub async fn check_marker(
        &self,
        store: &dyn ObjectStoreBackend,
        command: &str,
    ) -> anyhow::Result<CheckOutcome> {
        let local = hex::encode(self.fingerprint);
        check_marker(store, self.fingerprint).await.map_err(|err| {
            anyhow::anyhow!(
                "refusing to {command}: the IPCrypt policy marker check failed (local policy \
                 fingerprint {local}): {err}"
            )
        })
    }

    /// `tenant`'s effective rules. A policy with no rule for the tenant, or
    /// no deployment key, gives [`TenantRules::none`].
    pub fn rules_for(&self, tenant: &str) -> anyhow::Result<TenantRules> {
        let Some(deployment_key) = self.deployment_key.as_deref() else {
            return Ok(TenantRules::none());
        };
        let has_rules = !self.policy.defaults.rules.is_empty()
            || self
                .policy
                .tenants
                .get(tenant)
                .is_some_and(|table| !table.rules.is_empty());
        if !has_rules {
            return Ok(TenantRules::none());
        }
        let tenant_hash = TenantId::new(tenant).hash();
        TenantRules::resolve(
            &self.policy,
            tenant,
            &tenant_hash,
            deployment_key,
            sanitize_rule_key,
        )
        .map_err(|err| {
            anyhow::anyhow!(
                "IPCrypt rules for tenant hash {} could not be resolved: {err}",
                tenant_hash.to_hex()
            )
        })
    }

    /// The marker check, then `tenant`'s rules, built once for the run. The
    /// marker line goes to `log` when this run created the marker.
    pub async fn checked_rules(
        &self,
        store: &dyn ObjectStoreBackend,
        tenant: &str,
        command: &str,
        log: &mut dyn std::io::Write,
    ) -> anyhow::Result<Arc<TenantRules>> {
        if self.check_marker(store, command).await? == CheckOutcome::Created {
            let _ = writeln!(
                log,
                "created sys/ipcrypt/policy with policy fingerprint {}",
                hex::encode(self.fingerprint)
            );
        }
        Ok(Arc::new(self.rules_for(tenant)?))
    }
}
