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

use clap::ValueEnum;
use ravel_ipcrypt::fingerprint::{EMPTY_POLICY_FINGERPRINT, policy_fingerprint};
use ravel_ipcrypt::marker::{
    CheckError, CheckOutcome, IPCRYPT_POLICY_KEY, check_marker, read_marker, replace_marker,
};
use ravel_ipcrypt::policy::{PolicyFile, Variant, parse_policy_file};
use ravel_ipcrypt::render::RenderOutcome;
use ravel_ipcrypt::rules::{EpochKey, TenantRules};
use ravel_object_store::{ObjectStoreBackend, StoreError};
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

    /// The key for `tenant`, `variant` and `epoch`, independent of any rule.
    /// `None` without a deployment key.
    pub fn epoch_key(
        &self,
        tenant: &str,
        variant: Variant,
        epoch: u32,
    ) -> anyhow::Result<Option<EpochKey>> {
        let Some(deployment_key) = self.deployment_key.as_deref() else {
            return Ok(None);
        };
        epoch_key(deployment_key, tenant, variant, epoch).map(Some)
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

    /// The marker check `erase submit` runs (ADR-2993 decision 9, erasure
    /// amendment), which never writes the marker. A present marker must carry
    /// this policy's fingerprint, and a marker that cannot be read refuses.
    /// With no marker, the bucket's data was written without this policy, so
    /// a non-empty policy refuses when `encrypted_keys` (the matcher keys the
    /// request would encrypt) is not empty, and proceeds otherwise.
    pub async fn check_marker_for_erasure(
        &self,
        store: &dyn ObjectStoreBackend,
        encrypted_keys: &[&str],
    ) -> anyhow::Result<()> {
        let local = hex::encode(self.fingerprint);
        let refused = |err: &dyn std::fmt::Display| {
            anyhow::anyhow!(
                "refusing to erase submit: the IPCrypt policy marker check failed (local policy \
                 fingerprint {local}): {err}"
            )
        };
        match read_marker(store).await.map_err(|err| refused(&err))? {
            Some((stored, _)) if stored.fingerprint == self.fingerprint => Ok(()),
            Some((stored, _)) => Err(refused(&CheckError::Mismatch {
                stored: hex::encode(stored.fingerprint),
                local: local.clone(),
            })),
            None if self.is_empty() || encrypted_keys.is_empty() => Ok(()),
            None => {
                let keys: Vec<String> = encrypted_keys.iter().map(|k| format!("{k:?}")).collect();
                anyhow::bail!(
                    "refusing to erase submit: the bucket has no {IPCRYPT_POLICY_KEY} marker, so \
                     its data was written without the IPCrypt policy (local policy fingerprint \
                     {local}) and the encrypted matchers on {} would match nothing. Submit the \
                     request without --ipcrypt-config so its matchers stay plaintext. Nothing \
                     was written",
                    keys.join(", ")
                )
            }
        }
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

    /// [`Self::check_marker`], with a line on `log` when this run created the
    /// marker.
    pub async fn check_marker_logged(
        &self,
        store: &dyn ObjectStoreBackend,
        command: &str,
        log: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        if self.check_marker(store, command).await? == CheckOutcome::Created {
            let _ = writeln!(
                log,
                "created sys/ipcrypt/policy with policy fingerprint {}",
                hex::encode(self.fingerprint)
            );
        }
        Ok(())
    }

    /// The marker check, then `tenant`'s rules, built once for the run.
    pub async fn checked_rules(
        &self,
        store: &dyn ObjectStoreBackend,
        tenant: &str,
        command: &str,
        log: &mut dyn std::io::Write,
    ) -> anyhow::Result<Arc<TenantRules>> {
        self.check_marker_logged(store, command, log).await?;
        Ok(Arc::new(self.rules_for(tenant)?))
    }
}

/// The `--variant` of `ravel-cli ipcrypt encrypt|decrypt`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum VariantArg {
    Deterministic,
    Prefix,
}

impl From<VariantArg> for Variant {
    fn from(arg: VariantArg) -> Self {
        match arg {
            VariantArg::Deterministic => Variant::Deterministic,
            VariantArg::Prefix => Variant::Prefix,
        }
    }
}

/// Which way `ravel-cli ipcrypt encrypt|decrypt` transforms each value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Encrypt,
    Decrypt,
}

impl Direction {
    fn command(self) -> &'static str {
        match self {
            Direction::Encrypt => "ipcrypt encrypt",
            Direction::Decrypt => "ipcrypt decrypt",
        }
    }
}

/// `tenant`'s key for `variant` and `epoch` (ADR-2993 decision 2).
pub fn epoch_key(
    deployment_key: &[u8; 32],
    tenant: &str,
    variant: Variant,
    epoch: u32,
) -> anyhow::Result<EpochKey> {
    let tenant_hash = TenantId::new(tenant).hash();
    EpochKey::new(deployment_key, &tenant_hash, variant, epoch).map_err(|err| {
        anyhow::anyhow!(
            "could not derive the IPCrypt key for tenant hash {}, epoch {epoch}: {err}",
            tenant_hash.to_hex()
        )
    })
}

/// Encrypt or decrypt each value under `tenant`'s key for `variant` and
/// `epoch`. A value that is not an address (or, decrypting, not an address
/// rendering) comes back unchanged with [`RenderOutcome::Unparsed`].
pub fn transform_values(
    deployment_key: &[u8; 32],
    tenant: &str,
    variant: Variant,
    epoch: u32,
    direction: Direction,
    values: &[String],
) -> anyhow::Result<Vec<(String, RenderOutcome)>> {
    let key = epoch_key(deployment_key, tenant, variant, epoch)?;
    Ok(values
        .iter()
        .map(|value| match direction {
            Direction::Encrypt => key.encrypt(value),
            Direction::Decrypt => key.decrypt(value),
        })
        .collect())
}

/// `ravel-cli ipcrypt encrypt|decrypt`: one result per line on `out`, in
/// input order, and a note on `err` for each value printed unchanged.
#[allow(clippy::too_many_arguments)]
pub fn run_transform(
    deployment_key: Option<&[u8; 32]>,
    tenant: &str,
    variant: VariantArg,
    epoch: u32,
    direction: Direction,
    values: &[String],
    out: &mut dyn std::io::Write,
    err: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    let Some(deployment_key) = deployment_key else {
        anyhow::bail!(
            "ravel-cli {} needs the deployment key the tenant keys derive from: pass \
             --tenant-hash-key-file",
            direction.command()
        );
    };
    let results = transform_values(
        deployment_key,
        tenant,
        variant.into(),
        epoch,
        direction,
        values,
    )?;
    for (value, outcome) in results {
        writeln!(out, "{value}")?;
        if outcome == RenderOutcome::Unparsed {
            let what = match direction {
                Direction::Encrypt => "an IP address",
                Direction::Decrypt => "an IPCrypt address rendering",
            };
            writeln!(err, "note: {value:?} is not {what}; printed unchanged")?;
        }
    }
    Ok(())
}

/// What `ravel-cli ipcrypt set-policy` did to `sys/ipcrypt/policy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetPolicyOutcome {
    /// The marker already carried this policy's fingerprint.
    Unchanged,
    /// The marker carried `previous` and now carries this policy's
    /// fingerprint.
    Replaced { previous: [u8; 32] },
    /// No marker existed; it now carries this policy's fingerprint.
    Created,
    /// No marker existed and the policy is empty: nothing was written.
    NothingToWrite,
}

/// `ravel-cli ipcrypt set-policy`: read the marker, then replace it with a
/// CAS on the version just read, or create it when absent.
pub async fn set_policy(
    store: &dyn ObjectStoreBackend,
    policy: &IpcryptPolicy,
    out: &mut dyn std::io::Write,
) -> anyhow::Result<SetPolicyOutcome> {
    let current = read_marker(store).await.map_err(|err| {
        anyhow::anyhow!("refusing to set the IPCrypt policy: nothing was written: {err}")
    })?;
    set_policy_over(store, policy, current, out).await
}

/// The write half of [`set_policy`], given the marker state it read. A
/// marker replaced since that read fails the CAS and writes nothing.
pub async fn set_policy_over(
    store: &dyn ObjectStoreBackend,
    policy: &IpcryptPolicy,
    current: Option<(
        ravel_ipcrypt::marker::StoredMarker,
        ravel_object_store::Version,
    )>,
    out: &mut dyn std::io::Write,
) -> anyhow::Result<SetPolicyOutcome> {
    let local = hex::encode(policy.fingerprint);
    let outcome = match current {
        Some((stored, _)) if stored.fingerprint == policy.fingerprint => {
            SetPolicyOutcome::Unchanged
        }
        Some((stored, version)) => {
            replace_marker(store, policy.fingerprint, version)
                .await
                .map_err(|err| match err {
                    StoreError::PreconditionFailed => anyhow::anyhow!(
                        "refusing to set the IPCrypt policy: sys/ipcrypt/policy changed after \
                         it was read (it carried policy fingerprint {}); nothing was written. \
                         Read the new marker and rerun set-policy",
                        hex::encode(stored.fingerprint)
                    ),
                    err => anyhow::anyhow!(
                        "could not replace sys/ipcrypt/policy; nothing was written: {err}"
                    ),
                })?;
            SetPolicyOutcome::Replaced {
                previous: stored.fingerprint,
            }
        }
        None => match policy.check_marker(store, "set the IPCrypt policy").await? {
            CheckOutcome::Created => SetPolicyOutcome::Created,
            CheckOutcome::Matched => SetPolicyOutcome::Unchanged,
            CheckOutcome::ProceededNoMarker => SetPolicyOutcome::NothingToWrite,
        },
    };
    match outcome {
        SetPolicyOutcome::Unchanged => writeln!(
            out,
            "sys/ipcrypt/policy already carries policy fingerprint {local}; left unchanged"
        )?,
        SetPolicyOutcome::Replaced { previous } => writeln!(
            out,
            "replaced sys/ipcrypt/policy: policy fingerprint {} -> {local}",
            hex::encode(previous)
        )?,
        SetPolicyOutcome::Created => writeln!(
            out,
            "created sys/ipcrypt/policy with policy fingerprint {local}"
        )?,
        SetPolicyOutcome::NothingToWrite => writeln!(
            out,
            "no sys/ipcrypt/policy marker and the policy is empty; nothing written"
        )?,
    }
    Ok(outcome)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

    const DEPLOYMENT_KEY: [u8; 32] = [0x5a; 32];
    const DETERMINISTIC: &str =
        "[[defaults.rules]]\nkey = \"client.ip\"\nvariant = \"deterministic\"\n";
    const PREFIX: &str = "[[defaults.rules]]\nkey = \"client.ip\"\nvariant = \"prefix\"\n";
    const ROTATED: &str =
        "[[defaults.rules]]\nkey = \"client.ip\"\nvariant = \"deterministic\"\nepoch = 2\n";

    fn policy(text: &str) -> IpcryptPolicy {
        let file = parse_policy_file(text, sanitize_rule_key).expect("policy parses");
        IpcryptPolicy::new(file, Some(&DEPLOYMENT_KEY)).expect("policy loads")
    }

    fn values(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| v.to_string()).collect()
    }

    async fn stored_fingerprint(store: &dyn ObjectStoreBackend) -> Option<[u8; 32]> {
        read_marker(store)
            .await
            .expect("read marker")
            .map(|(marker, _)| marker.fingerprint)
    }

    #[test]
    fn encrypt_then_decrypt_round_trips_for_both_variants() {
        let addresses = values(&[
            "198.51.100.7",
            "2001:db8::1",
            "198.51.100.7:8080",
            "[2001:db8::1]:443",
        ]);
        for variant in [VariantArg::Deterministic, VariantArg::Prefix] {
            let mut encrypted = Vec::new();
            let mut notes = Vec::new();
            run_transform(
                Some(&DEPLOYMENT_KEY),
                "acme",
                variant,
                3,
                Direction::Encrypt,
                &addresses,
                &mut encrypted,
                &mut notes,
            )
            .expect("encrypt");
            assert!(notes.is_empty(), "{variant:?}");
            let ciphertexts: Vec<String> = String::from_utf8(encrypted)
                .expect("utf-8")
                .lines()
                .map(str::to_string)
                .collect();
            assert_eq!(ciphertexts.len(), 4, "{variant:?}");
            for (ciphertext, address) in ciphertexts.iter().zip(&addresses) {
                assert_ne!(ciphertext, address, "{variant:?}");
            }
            assert!(ciphertexts[2].ends_with(":8080"), "{variant:?}");
            assert!(
                ciphertexts[3].starts_with('[') && ciphertexts[3].ends_with("]:443"),
                "{variant:?}"
            );

            let mut decrypted = Vec::new();
            run_transform(
                Some(&DEPLOYMENT_KEY),
                "acme",
                variant,
                3,
                Direction::Decrypt,
                &ciphertexts,
                &mut decrypted,
                &mut Vec::new(),
            )
            .expect("decrypt");
            assert_eq!(
                String::from_utf8(decrypted).expect("utf-8"),
                "198.51.100.7\n2001:db8::1\n198.51.100.7:8080\n[2001:db8::1]:443\n",
                "{variant:?}"
            );
        }
    }

    /// The key is the tenant's and the epoch's: another tenant or epoch gives
    /// another ciphertext for the same address.
    #[test]
    fn the_key_depends_on_tenant_and_epoch() {
        let one = |tenant: &str, epoch: u32| {
            transform_values(
                &DEPLOYMENT_KEY,
                tenant,
                Variant::Deterministic,
                epoch,
                Direction::Encrypt,
                &values(&["198.51.100.7"]),
            )
            .expect("encrypt")
            .remove(0)
            .0
        };
        assert_ne!(one("acme", 1), one("globex", 1));
        assert_ne!(one("acme", 1), one("acme", 2));
        assert_eq!(one("acme", 1), one("acme", 1));
    }

    #[test]
    fn a_value_that_is_not_an_address_prints_unchanged_with_a_note() {
        let mut out = Vec::new();
        let mut err = Vec::new();
        run_transform(
            Some(&DEPLOYMENT_KEY),
            "acme",
            VariantArg::Deterministic,
            1,
            Direction::Encrypt,
            &values(&["unknown", "198.51.100.7"]),
            &mut out,
            &mut err,
        )
        .expect("encrypt");
        let out = String::from_utf8(out).expect("utf-8");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], "unknown");
        assert_ne!(lines[1], "198.51.100.7");
        assert_eq!(
            String::from_utf8(err).expect("utf-8"),
            "note: \"unknown\" is not an IP address; printed unchanged\n"
        );
    }

    #[test]
    fn transform_without_a_deployment_key_is_refused() {
        let err = run_transform(
            None,
            "acme",
            VariantArg::Prefix,
            1,
            Direction::Decrypt,
            &values(&["198.51.100.7"]),
            &mut Vec::new(),
            &mut Vec::new(),
        )
        .expect_err("no key");
        assert!(err.to_string().contains("--tenant-hash-key-file"), "{err}");
    }

    #[test]
    fn a_policy_with_rules_and_no_deployment_key_is_refused() {
        let file = parse_policy_file(DETERMINISTIC, sanitize_rule_key).expect("parses");
        let err = IpcryptPolicy::new(file, None).expect_err("rules need a key");
        assert_eq!(err.to_string(), NO_DEPLOYMENT_KEY);
        assert!(IpcryptPolicy::new(PolicyFile::default(), None).is_ok());
    }

    #[test]
    fn a_policy_file_that_does_not_parse_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("policy.toml");
        std::fs::write(
            &path,
            "[[defaults.rules]]\nkey = \"client.ip\"\nepoch = 0\n",
        )
        .expect("write policy");
        let err = read_policy_file(Some(&path)).expect_err("epoch 0 is refused");
        assert!(
            err.to_string().contains("invalid --ipcrypt-config"),
            "{err}"
        );
        let err = read_policy_file(Some(&dir.path().join("absent.toml")))
            .expect_err("a missing file is refused");
        assert!(
            err.to_string().contains("could not read --ipcrypt-config"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn set_policy_creates_then_replaces_the_marker() {
        let store = MemoryStore::new();
        let first = policy(DETERMINISTIC);
        let second = policy(PREFIX);
        assert_ne!(first.fingerprint(), second.fingerprint());

        let mut out = Vec::new();
        assert_eq!(
            set_policy(&store, &first, &mut out).await.expect("create"),
            SetPolicyOutcome::Created
        );
        assert_eq!(stored_fingerprint(&store).await, Some(first.fingerprint()));
        assert_eq!(
            set_policy(&store, &first, &mut out).await.expect("same"),
            SetPolicyOutcome::Unchanged
        );
        assert_eq!(
            set_policy(&store, &second, &mut out)
                .await
                .expect("replace"),
            SetPolicyOutcome::Replaced {
                previous: first.fingerprint()
            }
        );
        assert_eq!(stored_fingerprint(&store).await, Some(second.fingerprint()));
        let out = String::from_utf8(out).expect("utf-8");
        assert!(
            out.contains(&format!(
                "replaced sys/ipcrypt/policy: policy fingerprint {} -> {}",
                hex::encode(first.fingerprint()),
                hex::encode(second.fingerprint())
            )),
            "{out}"
        );
    }

    #[tokio::test]
    async fn set_policy_with_an_empty_policy_on_an_unmarked_bucket_writes_nothing() {
        let store = MemoryStore::new();
        assert_eq!(
            set_policy(&store, &IpcryptPolicy::empty(), &mut Vec::new())
                .await
                .expect("set"),
            SetPolicyOutcome::NothingToWrite
        );
        assert_eq!(stored_fingerprint(&store).await, None);
    }

    /// The replace is a CAS on the version read: a marker replaced since that
    /// read fails it, and the newer marker stands.
    #[tokio::test]
    async fn set_policy_over_a_stale_read_fails_and_writes_nothing() {
        let store = MemoryStore::new();
        let first = policy(DETERMINISTIC);
        let second = policy(PREFIX);
        let third = policy(ROTATED);
        set_policy(&store, &first, &mut Vec::new())
            .await
            .expect("create");
        let stale = read_marker(&store).await.expect("read marker");
        assert!(stale.is_some());
        set_policy(&store, &second, &mut Vec::new())
            .await
            .expect("replace");

        let err = set_policy_over(&store, &third, stale, &mut Vec::new())
            .await
            .expect_err("a stale version must fail the CAS");
        assert!(
            err.to_string()
                .contains("sys/ipcrypt/policy changed after it was read"),
            "{err}"
        );
        assert!(
            err.to_string().contains(&hex::encode(first.fingerprint())),
            "{err}"
        );
        assert_eq!(stored_fingerprint(&store).await, Some(second.fingerprint()));
    }

    #[tokio::test]
    async fn set_policy_refuses_when_the_marker_cannot_be_read() {
        use ravel_ipcrypt::marker::IPCRYPT_POLICY_KEY;
        use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
        let store = FaultStore::new(
            MemoryStore::new(),
            FaultPlan::empty().with_rule(
                Rule::new(Op::Get, ScriptedFault::Transient("marker read".into()))
                    .with_key_contains(IPCRYPT_POLICY_KEY),
            ),
        );
        let err = set_policy(&store, &policy(DETERMINISTIC), &mut Vec::new())
            .await
            .expect_err("a failed read refuses");
        assert!(
            err.to_string()
                .contains("refusing to set the IPCrypt policy: nothing was written"),
            "{err}"
        );
        assert_eq!(stored_fingerprint(store.inner()).await, None);
    }
}
