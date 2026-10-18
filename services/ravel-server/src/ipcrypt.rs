//! IPCrypt pseudonymization at ingest (ADR-2993): the parsed
//! `--ipcrypt-config` policy, the per-tenant rule sets every ingest surface
//! hands its normalizer, and the startup check against the bucket's
//! `sys/ipcrypt/policy` marker.
//!
//! One [`IpcryptPolicy`] exists per process, shared behind an `Arc` by every
//! ingest state. A tenant's [`TenantRules`] are derived on its first write
//! from the deployment key, its tenant hash and the policy, then cached for
//! the life of the process: the policy cannot change without a restart, so
//! neither can a tenant's rules.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use ravel_ipcrypt::fingerprint::{EMPTY_POLICY_FINGERPRINT, policy_fingerprint};
use ravel_ipcrypt::marker::{CheckOutcome, check_marker};
use ravel_ipcrypt::policy::PolicyFile;
use ravel_ipcrypt::rules::{ResolveError, TenantRules};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{TenantHash, TenantId};

use crate::config::Mode;

/// The metric label sanitizer rule keys are matched through on metric
/// surfaces (ADR-2993 decision 1): the one the OTLP metric normalizer applies
/// to attribute keys, so a rule on `client.ip` matches the `client_ip` label.
pub fn sanitize_rule_key(key: &str) -> String {
    ravel_otlp::normalize::sanitize_label_name(key.to_string())
}

/// A policy that cannot be used with this bucket.
#[derive(Debug, thiserror::Error)]
pub enum IpcryptConfigError {
    /// ADR-2993 decision 1: every rule's key is derived from the deployment
    /// key, which an unkeyed bucket does not have.
    #[error(
        "--ipcrypt-config declares IPCrypt rules, but this process has no deployment key to \
         derive their keys from: set --tenant-hash-key-file (a bucket pinned unkeyed, or \
         started with --tenant-hash-unkeyed, cannot carry an IPCrypt policy)"
    )]
    NoDeploymentKey,
}

/// A tenant whose rules could not be derived. Names the tenant hash and the
/// rule key involved, never key bytes.
#[derive(Debug, thiserror::Error)]
#[error(
    "ipcrypt rules for tenant hash {tenant_hash} (rule key {rule_key:?}) could not be resolved: {source}"
)]
pub struct TenantRulesError {
    pub tenant_hash: String,
    pub rule_key: String,
    #[source]
    pub source: ResolveError,
}

/// What the client is told when its tenant's rules could not be resolved.
/// The detail stays in the server log.
pub const TENANT_RULES_UNAVAILABLE: &str =
    "IPCrypt rules for this tenant could not be resolved; the write was not stored";

/// The process's IPCrypt policy and its per-tenant rule cache.
pub struct IpcryptPolicy {
    policy: PolicyFile,
    fingerprint: [u8; 32],
    deployment_key: Option<Box<[u8; 32]>>,
    cache: RwLock<HashMap<TenantHash, Arc<TenantRules>>>,
    none: Arc<TenantRules>,
    builds: AtomicU64,
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
    /// Validate `policy` against the deployment key this process resolved
    /// and compute its fingerprint. Must run after the tenant-hash scheme is
    /// installed: the fingerprint keys each `[tenants.<name>]` table by
    /// `TenantId::hash`, the same derivation `--tenant-kms-config` tables go
    /// through.
    pub fn new(
        policy: PolicyFile,
        deployment_key: Option<&[u8; 32]>,
    ) -> Result<Self, IpcryptConfigError> {
        let fingerprint = policy_fingerprint(&policy, |id| TenantId::new(id).hash());
        if fingerprint != EMPTY_POLICY_FINGERPRINT && deployment_key.is_none() {
            return Err(IpcryptConfigError::NoDeploymentKey);
        }
        Ok(Self::from_parts(
            policy,
            fingerprint,
            deployment_key.map(|key| Box::new(*key)),
        ))
    }

    /// The empty policy: every tenant gets the shared no-rules set.
    pub fn empty() -> Self {
        Self::from_parts(PolicyFile::default(), EMPTY_POLICY_FINGERPRINT, None)
    }

    /// The one place a production [`TenantRules::none`] is built: the set
    /// every tenant without rules shares.
    fn from_parts(
        policy: PolicyFile,
        fingerprint: [u8; 32],
        deployment_key: Option<Box<[u8; 32]>>,
    ) -> Self {
        Self {
            policy,
            fingerprint,
            deployment_key,
            cache: RwLock::new(HashMap::new()),
            none: Arc::new(TenantRules::none()),
            builds: AtomicU64::new(0),
        }
    }

    /// [`IpcryptPolicy::empty`] behind the `Arc` every ingest state holds.
    pub fn disabled() -> Arc<Self> {
        Arc::new(Self::empty())
    }

    /// The policy fingerprint the startup marker check compares.
    pub fn fingerprint(&self) -> [u8; 32] {
        self.fingerprint
    }

    pub fn is_empty(&self) -> bool {
        self.fingerprint == EMPTY_POLICY_FINGERPRINT
    }

    /// `tenant`'s rule set: built on first use and cached by tenant hash for
    /// the life of the process. A tenant no rule applies to gets the one
    /// shared no-rules set. On a resolve error nothing is cached, so the
    /// tenant's next write tries again, and the error is logged naming the
    /// tenant hash and the rule key.
    pub fn rules_for(&self, tenant: &TenantId) -> Result<Arc<TenantRules>, TenantRulesError> {
        let Some(deployment_key) = self.deployment_key.as_deref() else {
            return Ok(self.none.clone());
        };
        if !self.has_rules(tenant.as_str()) {
            return Ok(self.none.clone());
        }
        let tenant_hash = tenant.hash();
        if let Some(rules) = self.cache.read().get(&tenant_hash) {
            return Ok(rules.clone());
        }
        let mut cache = self.cache.write();
        if let Some(rules) = cache.get(&tenant_hash) {
            return Ok(rules.clone());
        }
        let resolved = TenantRules::resolve(
            &self.policy,
            tenant.as_str(),
            &tenant_hash,
            deployment_key,
            sanitize_rule_key,
        )
        .map_err(|source| {
            let err = TenantRulesError {
                tenant_hash: tenant_hash.to_hex(),
                rule_key: rule_key_of(&source),
                source,
            };
            tracing::error!(
                tenant_hash = %err.tenant_hash,
                rule_key = %err.rule_key,
                error = %err.source,
                "refusing the tenant's write: its IPCrypt rules could not be resolved"
            );
            err
        })?;
        self.builds.fetch_add(1, Ordering::Relaxed);
        let rules = if resolved.is_empty() {
            self.none.clone()
        } else {
            Arc::new(resolved)
        };
        cache.insert(tenant_hash, rules.clone());
        Ok(rules)
    }

    fn has_rules(&self, tenant_id: &str) -> bool {
        !self.policy.defaults.rules.is_empty()
            || self
                .policy
                .tenants
                .get(tenant_id)
                .is_some_and(|table| !table.rules.is_empty())
    }
}

fn rule_key_of(err: &ResolveError) -> String {
    match err {
        ResolveError::KeyDerivation(ravel_ipcrypt::KeyDerivationError::DegeneratePrefixKey {
            rule_key,
            ..
        }) => rule_key.clone(),
        ResolveError::SanitizeCollision { a, .. } => a.clone(),
    }
}

/// ADR-2993 decision 9: in the `all` and `gateway` modes, compare the local
/// policy's fingerprint with the bucket's `sys/ipcrypt/policy` marker before
/// serving, whether or not `--ipcrypt-config` was given. No marker and an
/// empty policy writes nothing; no marker and a non-empty policy creates it.
/// A mismatch or any failure to read the marker refuses to start: a read
/// failure is never taken for an absent marker. `query` and `maintain` store
/// nothing a policy governs and do not read the marker (`Ok(None)`). The
/// modes that read it are the ones that build the ingest routers,
/// [`Mode::holds_ingest_buffer`].
pub async fn check_startup_marker(
    store: &dyn ObjectStoreBackend,
    mode: Mode,
    policy: &IpcryptPolicy,
) -> anyhow::Result<Option<CheckOutcome>> {
    if !mode.holds_ingest_buffer() {
        return Ok(None);
    }
    let local = hex::encode(policy.fingerprint());
    let outcome = check_marker(store, policy.fingerprint())
        .await
        .map_err(|err| {
            anyhow::anyhow!(
                "refusing to start: the IPCrypt policy marker check failed (local policy \
                 fingerprint {local}): {err}"
            )
        })?;
    tracing::info!(
        fingerprint = %local,
        outcome = ?outcome,
        "IPCrypt policy marker checked"
    );
    Ok(Some(outcome))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_ipcrypt::marker::IPCRYPT_POLICY_KEY;
    use ravel_ipcrypt::policy::{Rule, RuleTable, Variant};
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Op, Rule as FaultRule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;

    const KEY: [u8; 32] = [7u8; 32];

    fn rule(key: &str) -> Rule {
        Rule {
            key: key.to_string(),
            variant: Variant::Deterministic,
            epoch: 1,
            retired_epochs: Vec::new(),
        }
    }

    fn policy_for(tenant: &str, keys: &[&str]) -> PolicyFile {
        let mut tenants = HashMap::new();
        tenants.insert(
            tenant.to_string(),
            RuleTable {
                rules: keys.iter().map(|k| rule(k)).collect(),
            },
        );
        PolicyFile {
            defaults: RuleTable::default(),
            tenants,
        }
    }

    fn defaults_policy(keys: &[&str]) -> PolicyFile {
        PolicyFile {
            defaults: RuleTable {
                rules: keys.iter().map(|k| rule(k)).collect(),
            },
            tenants: HashMap::new(),
        }
    }

    #[test]
    fn non_empty_policy_without_a_deployment_key_is_refused_naming_the_flag() {
        let err = IpcryptPolicy::new(defaults_policy(&["client.ip"]), None)
            .expect_err("a non-empty policy needs the deployment key");
        assert!(matches!(err, IpcryptConfigError::NoDeploymentKey));
        let text = err.to_string();
        assert!(text.contains("--ipcrypt-config"), "{text}");
        assert!(text.contains("--tenant-hash-key-file"), "{text}");
    }

    #[test]
    fn empty_policy_without_a_deployment_key_is_accepted() {
        let policy = IpcryptPolicy::new(PolicyFile::default(), None).expect("empty is fine");
        assert!(policy.is_empty());
        assert_eq!(policy.fingerprint(), EMPTY_POLICY_FINGERPRINT);
        let rules = policy.rules_for(&TenantId::new("acme")).expect("no rules");
        assert!(rules.is_empty());
    }

    #[test]
    fn unparsable_policy_file_is_refused_naming_the_flag() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("ipcrypt.toml");
        std::fs::write(&path, "[defaults\nrules = 3").expect("write");
        let cli = <crate::config::Cli as clap::Parser>::try_parse_from([
            "ravel-server",
            "--ipcrypt-config",
            path.to_str().expect("utf8 path"),
        ])
        .expect("cli parses");
        let err = cli.parse_ipcrypt_config().expect_err("bad TOML refuses");
        assert!(err.to_string().contains("--ipcrypt-config"), "{err}");
    }

    #[test]
    fn absent_flag_parses_to_the_empty_policy() {
        let cli = <crate::config::Cli as clap::Parser>::try_parse_from(["ravel-server"])
            .expect("cli parses");
        let policy = cli.parse_ipcrypt_config().expect("absent is empty");
        assert_eq!(policy, PolicyFile::default());
    }

    #[test]
    fn rules_are_built_once_per_tenant_and_tenants_without_rules_share_one_set() {
        let policy =
            IpcryptPolicy::new(policy_for("acme", &["client.ip"]), Some(&KEY)).expect("keyed");
        let acme = TenantId::new("acme");
        let first = policy.rules_for(&acme).expect("acme resolves");
        let second = policy.rules_for(&acme).expect("acme resolves again");
        assert!(!first.is_empty());
        assert!(
            Arc::ptr_eq(&first, &second),
            "the second call is the cached set"
        );
        assert_eq!(policy.builds.load(Ordering::Relaxed), 1);

        let other = policy
            .rules_for(&TenantId::new("globex"))
            .expect("no rules");
        let third = policy
            .rules_for(&TenantId::new("initech"))
            .expect("no rules");
        assert!(other.is_empty());
        assert!(Arc::ptr_eq(&other, &policy.none));
        assert!(Arc::ptr_eq(&third, &policy.none));
        assert_eq!(
            policy.builds.load(Ordering::Relaxed),
            1,
            "a tenant no rule applies to derives nothing"
        );
    }

    #[test]
    fn concurrent_first_writes_build_the_tenant_once() {
        let policy = Arc::new(
            IpcryptPolicy::new(defaults_policy(&["client.ip"]), Some(&KEY)).expect("keyed"),
        );
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let policy = policy.clone();
                std::thread::spawn(move || policy.rules_for(&TenantId::new("acme")).expect("ok"))
            })
            .collect();
        let sets: Vec<Arc<TenantRules>> = handles
            .into_iter()
            .map(|h| h.join().expect("thread"))
            .collect();
        assert!(sets.windows(2).all(|w| Arc::ptr_eq(&w[0], &w[1])));
        assert_eq!(policy.builds.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn resolve_error_refuses_naming_tenant_hash_and_rule_key_and_caches_nothing() {
        // Built by hand: `parse_policy_file` refuses this collision, which is
        // what makes it reachable here only through a hand-built policy.
        let policy = IpcryptPolicy::new(defaults_policy(&["client.ip", "client_ip"]), Some(&KEY))
            .expect("keyed");
        let acme = TenantId::new("acme");
        let err = policy.rules_for(&acme).expect_err("collision refuses");
        assert_eq!(err.tenant_hash, acme.hash().to_hex());
        assert!(err.rule_key == "client.ip" || err.rule_key == "client_ip");
        let text = err.to_string();
        assert!(text.contains(&acme.hash().to_hex()), "{text}");
        assert!(text.contains(&err.rule_key), "{text}");
        assert!(!text.contains(&hex::encode(KEY)), "no key bytes: {text}");
        assert!(policy.cache.read().is_empty());
        assert!(
            policy.rules_for(&acme).is_err(),
            "the next write tries again"
        );
    }

    fn fingerprint_of(policy: &IpcryptPolicy) -> String {
        hex::encode(policy.fingerprint())
    }

    #[tokio::test]
    async fn marker_mismatch_refuses_naming_both_fingerprints() {
        let store = MemoryStore::new();
        let first = IpcryptPolicy::new(defaults_policy(&["client.ip"]), Some(&KEY)).expect("a");
        check_startup_marker(&store, Mode::All, &first)
            .await
            .expect("first start creates the marker");
        let second = IpcryptPolicy::new(defaults_policy(&["peer.ip"]), Some(&KEY)).expect("b");
        let err = check_startup_marker(&store, Mode::Gateway, &second)
            .await
            .expect_err("a different policy refuses");
        let text = format!("{err:#}");
        assert!(text.contains(&fingerprint_of(&first)), "{text}");
        assert!(text.contains(&fingerprint_of(&second)), "{text}");
    }

    #[tokio::test]
    async fn marker_read_failure_refuses_and_is_not_taken_for_absent() {
        let plan = FaultPlan::empty().with_rule(
            FaultRule::new(Op::Get, ScriptedFault::Transient("store down".into()))
                .with_key_contains(IPCRYPT_POLICY_KEY),
        );
        let store = FaultStore::new(MemoryStore::new(), plan);
        // An empty policy: an implementation that read the failure as an
        // absent marker would proceed here without writing.
        let err = check_startup_marker(&store, Mode::All, &IpcryptPolicy::empty())
            .await
            .expect_err("a read failure refuses");
        assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);
        let text = format!("{err:#}");
        assert!(
            text.contains(&hex::encode(EMPTY_POLICY_FINGERPRINT)),
            "{text}"
        );
        assert!(text.contains(IPCRYPT_POLICY_KEY), "{text}");
    }

    #[tokio::test]
    async fn no_marker_and_empty_policy_starts_and_writes_nothing() {
        let store = MemoryStore::new();
        let outcome = check_startup_marker(&store, Mode::All, &IpcryptPolicy::empty())
            .await
            .expect("starts");
        assert_eq!(outcome, Some(CheckOutcome::ProceededNoMarker));
        let listed = ravel_object_store::list_all(&store, "")
            .await
            .expect("list");
        assert!(listed.is_empty(), "nothing written: {listed:?}");
    }

    #[tokio::test]
    async fn no_marker_and_non_empty_policy_creates_it() {
        let store = MemoryStore::new();
        let policy = IpcryptPolicy::new(defaults_policy(&["client.ip"]), Some(&KEY)).expect("a");
        let outcome = check_startup_marker(&store, Mode::Gateway, &policy)
            .await
            .expect("starts");
        assert_eq!(outcome, Some(CheckOutcome::Created));
        let (stored, _) = ravel_ipcrypt::marker::read_marker(&store)
            .await
            .expect("read")
            .expect("present");
        assert_eq!(stored.fingerprint, policy.fingerprint());
        let again = check_startup_marker(&store, Mode::All, &policy)
            .await
            .expect("restart matches");
        assert_eq!(again, Some(CheckOutcome::Matched));
    }

    #[tokio::test]
    async fn query_and_maintain_do_not_read_the_marker() {
        for mode in [Mode::Query, Mode::Maintain] {
            let plan = FaultPlan::empty().with_rule(
                FaultRule::new(Op::Get, ScriptedFault::Transient("would refuse".into()))
                    .with_key_contains(IPCRYPT_POLICY_KEY),
            );
            let store = FaultStore::new(MemoryStore::new(), plan);
            // Stored bytes that do not decode either: any read would refuse.
            store
                .inner()
                .put(
                    IPCRYPT_POLICY_KEY,
                    b"not a marker".to_vec().into(),
                    PutOptions::create_if_absent(),
                )
                .await
                .expect("seed");
            let outcome = check_startup_marker(&store, mode, &IpcryptPolicy::empty())
                .await
                .expect("not checked");
            assert_eq!(outcome, None);
            assert_eq!(
                store.fault_count(Op::Get, FaultKind::Transient),
                0,
                "{mode:?} issued a GET on {IPCRYPT_POLICY_KEY}"
            );
        }
    }

    /// Every `.rs` file under `dir`, recursively.
    fn rust_sources(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        for entry in std::fs::read_dir(dir).expect("read src dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                rust_sources(&path, out);
            } else if path.extension().is_some_and(|ext| ext == "rs") {
                out.push(path);
            }
        }
    }

    /// The lines of `source` outside `#[cfg(test)]` items, numbered from 1.
    /// A gated item ending in `{` is skipped through the `}` at its own
    /// indent; one ending in `;` is skipped as that one line.
    fn production_lines(source: &str) -> Vec<(usize, &str)> {
        let lines: Vec<&str> = source.lines().collect();
        let mut out = Vec::new();
        let mut i = 0;
        while i < lines.len() {
            let line = lines[i];
            if line.trim() != "#[cfg(test)]" {
                out.push((i + 1, line));
                i += 1;
                continue;
            }
            let indent = &line[..line.len() - line.trim_start().len()];
            let mut item = i + 1;
            while item < lines.len() && lines[item].trim_start().starts_with("#[") {
                item += 1;
            }
            let Some(head) = lines.get(item) else { break };
            if head.trim_end().ends_with('{') {
                let close = format!("{indent}}}");
                let mut end = item + 1;
                while end < lines.len() && lines[end].trim_end() != close {
                    end += 1;
                }
                i = end + 1;
            } else {
                i = item + 1;
            }
        }
        out
    }

    #[test]
    fn no_production_code_builds_its_own_empty_rule_set() {
        let needle = concat!("TenantRules", "::none()");
        let allowed = concat!("none: Arc::new(TenantRules", "::none()),");
        let root = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
        let mut files = Vec::new();
        rust_sources(root, &mut files);
        assert!(files.len() > 20, "scanned only {} files", files.len());

        let mut shared = 0;
        let mut offenders = Vec::new();
        for file in &files {
            let source = std::fs::read_to_string(file).expect("read source");
            for (number, line) in production_lines(&source) {
                if !line.contains(needle) {
                    continue;
                }
                if file.ends_with("ipcrypt.rs") && line.trim() == allowed {
                    shared += 1;
                } else {
                    offenders.push(format!("{}:{number}: {}", file.display(), line.trim()));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "production code must take its rules from IpcryptPolicy::rules_for: {offenders:#?}"
        );
        assert_eq!(shared, 1, "the shared no-rules constructor is the one site");
    }

    #[test]
    fn production_lines_skips_gated_items_only() {
        let source = "fn a() {}\n#[cfg(test)]\nfn b() {\n    x();\n}\n#[cfg(test)]\nuse c;\nfn d() {}\n#[cfg(test)]\n#[allow(dead_code)]\nmod tests {\n    fn e() {\n    }\n}\nfn f() {}\n";
        let kept: Vec<usize> = production_lines(source).iter().map(|(n, _)| *n).collect();
        assert_eq!(kept, vec![1, 8, 15]);
    }
}
