//! Per-tenant SSE-KMS configuration (ADR-0062 decision 1, ADR-0072 decision
//! 2): the `--tenant-kms-config` file that maps tenant name to KMS key ARN,
//! shared by `ravel-server` and the `ravel-cli` commands that write tenant
//! data under the Maintain credential, so both binaries parse the same file
//! the same way and route a tenant's writes through the same key.
//!
//! Parsing ([`parse_tenant_kms_config`]) needs no tenant-hash scheme; applying
//! the parsed config to a live [`KmsRoutingStore`] ([`configure_tenant_kms`])
//! hashes each tenant, so it runs only once the bucket's scheme is installed.

use std::collections::HashMap;

use ravel_object_store::{KmsRoutingStore, ObjectStoreBackend};
use ravel_types::{TenantHash, TenantId};
use serde::Deserialize;

use crate::key_epoch::{KeyEpoch, KeyEpochError, read_epochs_from_store, record_key_epoch};

/// A `--tenant-kms-config` file's parsed, validated content: one KMS key ARN
/// per named tenant. Order is irrelevant; the map is keyed by [`TenantId`] so
/// [`configure_tenant_kms`] can hash each tenant directly.
#[derive(Debug, Clone, Default)]
pub struct TenantKmsConfig {
    tenants: HashMap<TenantId, String>,
}

impl TenantKmsConfig {
    pub fn is_empty(&self) -> bool {
        self.tenants.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&TenantId, &str)> {
        self.tenants.iter().map(|(id, arn)| (id, arn.as_str()))
    }

    /// The key ARN configured for `tenant`, or `None` when the file has no
    /// entry for it (its writes then go to the default store unrouted).
    pub fn key_for(&self, tenant: &TenantId) -> Option<&str> {
        self.tenants.get(tenant).map(String::as_str)
    }

    /// This config narrowed to `tenant`'s entry alone (empty when the file
    /// does not name it), for a caller that writes one tenant's data and must
    /// not bootstrap or rotate any other tenant's key epochs.
    pub fn restricted_to(&self, tenant: &TenantId) -> TenantKmsConfig {
        TenantKmsConfig {
            tenants: self
                .tenants
                .get_key_value(tenant)
                .map(|(id, arn)| (id.clone(), arn.clone()))
                .into_iter()
                .collect(),
        }
    }
}

/// Why a `--tenant-kms-config` file was refused, or why applying it failed.
/// Every message carries no source chain of its own, so a caller wrapping it
/// in context prints exactly one line per layer.
#[derive(Debug, thiserror::Error)]
pub enum TenantKmsError {
    #[error(transparent)]
    Toml(#[from] toml::de::Error),
    #[error("[tenants] in --tenant-kms-config has an entry with an empty tenant id")]
    EmptyTenantId,
    #[error(
        "[tenants] in --tenant-kms-config names tenant {tenant:?} with an empty key_arn: \
         an empty string is reserved to mean the deployment default key and is never a \
         valid operator-configured value"
    )]
    EmptyKeyArn { tenant: String },
    #[error("failed to read key-epoch history for tenant {tenant:?}: {error}")]
    ReadEpochs {
        tenant: String,
        error: KeyEpochError,
    },
    #[error("tenant {tenant:?} has an empty key-epoch history at t/{hash_hex}/enc")]
    EmptyEpochHistory { tenant: String, hash_hex: String },
    #[error("failed to record key-epoch for tenant {tenant:?}: {error}")]
    RecordEpoch {
        tenant: String,
        error: KeyEpochError,
    },
    #[error(
        "tenant {tenant:?} key-epoch bootstrap kept losing a concurrent CAS race after \
         {MAX_EPOCH_CAS_RETRIES} retries"
    )]
    CasRetriesExhausted { tenant: String },
    #[error(
        "tenant {tenant:?} has key {recorded_key:?} recorded as its current key epoch at \
         t/{hash_hex}/enc, but --tenant-kms-config names {configured_key:?}: a key change is \
         recorded by ravel-server at startup, never by this command. Refusing before any \
         write; start ravel-server with the new key first, or run this command with the file \
         the servers run with"
    )]
    KeyChangeRefused {
        tenant: String,
        hash_hex: String,
        recorded_key: String,
        configured_key: String,
    },
    #[error(
        "tenant {tenant:?} has no key-epoch record at t/{hash_hex}/enc, but \
         --tenant-kms-config names {configured_key:?} for it: a tenant's key epochs are \
         recorded by ravel-server at startup, never by this command. Refusing before any \
         write; start ravel-server with this --tenant-kms-config file first, then rerun this \
         command"
    )]
    EpochRecordAbsent {
        tenant: String,
        hash_hex: String,
        configured_key: String,
    },
}

/// What [`configure_tenant_kms_with_policy`] does when a tenant's key-epoch
/// record is absent, or exists and its current key differs from the
/// configured one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyChangePolicy {
    /// Bootstrap an absent record, and append a new epoch at `now_ns` for a
    /// differing key (ADR-0062 decision 1b). Server startup alone records a
    /// configured or changed key, so only ravel-server uses this.
    RecordRotation,
    /// Record no key: an absent record is refused with
    /// [`TenantKmsError::EpochRecordAbsent`] and a differing key with
    /// [`TenantKmsError::KeyChangeRefused`], both before any write. The
    /// `t/<hash>/enc` record is append-only and deny-delete, so a key a
    /// command recorded from a file the servers do not run with could never
    /// be removed. The one key this policy can still record is epoch 1 of a
    /// record holding only the bootstrap epoch 0, a first configuration a
    /// server began and did not finish; it takes that key from the command's
    /// own file, so the command must run with the file the servers run with.
    Refuse,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct TenantKmsFileToml {
    #[serde(default)]
    tenants: HashMap<String, String>,
}

/// Parse and validate a `--tenant-kms-config` document's text (already read
/// from disk by the caller). `key_arn` values of `""` are rejected: that
/// string is reserved internally to mean "the deployment default key"
/// (ADR-0062 decision 1c's `KeyEpoch::key_arn` doc), never an operator-chosen
/// value.
pub fn parse_tenant_kms_config(text: &str) -> Result<TenantKmsConfig, TenantKmsError> {
    let file: TenantKmsFileToml = toml::from_str(text)?;
    let mut tenants = HashMap::with_capacity(file.tenants.len());
    for (id, key_arn) in file.tenants {
        if id.is_empty() {
            return Err(TenantKmsError::EmptyTenantId);
        }
        if key_arn.is_empty() {
            return Err(TenantKmsError::EmptyKeyArn { tenant: id });
        }
        tenants.insert(TenantId::new(id), key_arn);
    }
    Ok(TenantKmsConfig { tenants })
}

/// Bounded retry count for the key-epoch CAS loop below: this runs once per
/// tenant at process startup, against a record only this same bootstrap step
/// ever writes, so a real collision means another replica is bootstrapping
/// the same tenant concurrently, not a busy record under steady-state load. A
/// handful of retries is enough to let one of the two racing writers win;
/// exhausting them fails startup rather than looping forever.
const MAX_EPOCH_CAS_RETRIES: u32 = 5;

/// Apply a parsed `--tenant-kms-config` to a live [`KmsRoutingStore`]: for
/// each configured tenant, register its key with `set_tenant_key` and ensure
/// the ADR-0062 decision 1b key-epoch history at `t/<hash>/enc` reflects it.
///
/// Bootstrap (no epoch history yet) writes epoch 0 with `key_arn: ""`
/// (deployment default) and `activated_ns: 0` — the start of unix time, at or
/// before every real object's write timestamp, so `verify-custody`'s epoch
/// check attributes every object the tenant already wrote to the deployment
/// default rather than reporting a false-positive anomaly — then appends
/// epoch 1 with the operator's real `key_arn` and `activated_ns: now_ns`: the
/// moment this key becomes active. A later restart with the same key is a
/// no-op; a later restart with a *different* key (rotation) appends a new
/// epoch at `now_ns`.
///
/// This is ravel-server's startup path: [`KeyChangePolicy::RecordRotation`].
pub async fn configure_tenant_kms(
    kms: &KmsRoutingStore,
    store: &dyn ObjectStoreBackend,
    config: &TenantKmsConfig,
    now_ns: i64,
) -> Result<(), TenantKmsError> {
    configure_tenant_kms_with_policy(kms, store, config, now_ns, KeyChangePolicy::RecordRotation)
        .await
}

/// [`configure_tenant_kms`] with an explicit [`KeyChangePolicy`] for a record
/// that is absent or whose current key differs from the configured one. The
/// same-key no-op and the completion of an unfinished bootstrap are identical
/// under both policies; a refused tenant's key is never registered.
pub async fn configure_tenant_kms_with_policy(
    kms: &KmsRoutingStore,
    store: &dyn ObjectStoreBackend,
    config: &TenantKmsConfig,
    now_ns: i64,
    policy: KeyChangePolicy,
) -> Result<(), TenantKmsError> {
    for (tenant, key_arn) in config.iter() {
        let hash = tenant.hash();
        bootstrap_tenant_epoch(store, tenant, &hash, key_arn, now_ns, policy).await?;
        kms.set_tenant_key(hash.to_hex(), key_arn.to_string());
    }
    Ok(())
}

/// The read-only half of [`configure_tenant_kms_with_policy`] under
/// [`KeyChangePolicy::Refuse`], for a dry run: reads each configured tenant's
/// key-epoch record and returns the refusal the real run would return. It
/// writes nothing and registers no key. A record holding only the bootstrap
/// epoch 0 passes, since the real run completes it.
pub async fn check_tenant_kms_records(
    store: &dyn ObjectStoreBackend,
    config: &TenantKmsConfig,
) -> Result<(), TenantKmsError> {
    for (tenant, key_arn) in config.iter() {
        let hash = tenant.hash();
        let existing = read_epochs_from_store(store, &hash)
            .await
            .map_err(|error| TenantKmsError::ReadEpochs {
                tenant: tenant.as_str().to_string(),
                error,
            })?;
        epoch_action(
            tenant,
            &hash,
            existing.as_deref(),
            key_arn,
            KeyChangePolicy::Refuse,
        )?;
    }
    Ok(())
}

/// A record holding only the epoch-0 deployment-default entry is a first
/// configuration whose epoch 1 was never written (a lost CAS race or a crash
/// between the two puts), not a key a server recorded.
fn is_unfinished_bootstrap(epochs: &[KeyEpoch]) -> bool {
    matches!(epochs, [only] if only.epoch == 0 && only.key_arn.is_empty() && only.activated_ns == 0)
}

/// What a tenant's key-epoch record needs before `key_arn` routes.
enum EpochAction {
    /// The record's current key is `key_arn`: nothing to write.
    Current,
    /// No record: epoch 0 (deployment default), then `key_arn`.
    Bootstrap,
    /// Append `key_arn` as the next epoch.
    Append,
}

fn epoch_action(
    tenant: &TenantId,
    hash: &TenantHash,
    existing: Option<&[KeyEpoch]>,
    key_arn: &str,
    policy: KeyChangePolicy,
) -> Result<EpochAction, TenantKmsError> {
    let Some(epochs) = existing else {
        return match policy {
            KeyChangePolicy::RecordRotation => Ok(EpochAction::Bootstrap),
            KeyChangePolicy::Refuse => Err(TenantKmsError::EpochRecordAbsent {
                tenant: tenant.as_str().to_string(),
                hash_hex: hash.to_hex(),
                configured_key: key_arn.to_string(),
            }),
        };
    };
    let Some(last) = epochs.last() else {
        return Err(TenantKmsError::EmptyEpochHistory {
            tenant: tenant.as_str().to_string(),
            hash_hex: hash.to_hex(),
        });
    };
    if last.key_arn == key_arn {
        Ok(EpochAction::Current)
    } else if policy == KeyChangePolicy::Refuse && !is_unfinished_bootstrap(epochs) {
        Err(TenantKmsError::KeyChangeRefused {
            tenant: tenant.as_str().to_string(),
            hash_hex: hash.to_hex(),
            recorded_key: last.key_arn.clone(),
            configured_key: key_arn.to_string(),
        })
    } else {
        Ok(EpochAction::Append)
    }
}

async fn bootstrap_tenant_epoch(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    hash: &TenantHash,
    key_arn: &str,
    now_ns: i64,
    policy: KeyChangePolicy,
) -> Result<(), TenantKmsError> {
    for _ in 0..MAX_EPOCH_CAS_RETRIES {
        let existing = read_epochs_from_store(store, hash).await.map_err(|error| {
            TenantKmsError::ReadEpochs {
                tenant: tenant.as_str().to_string(),
                error,
            }
        })?;

        let outcome = match epoch_action(tenant, hash, existing.as_deref(), key_arn, policy)? {
            EpochAction::Current => Ok(()),
            EpochAction::Bootstrap => match record_key_epoch(store, hash, "", 0, now_ns).await {
                Ok(_) => record_key_epoch(store, hash, key_arn, now_ns, now_ns)
                    .await
                    .map(|_| ()),
                Err(err) => Err(err),
            },
            EpochAction::Append => record_key_epoch(store, hash, key_arn, now_ns, now_ns)
                .await
                .map(|_| ()),
        };

        match outcome {
            Ok(()) => return Ok(()),
            Err(KeyEpochError::CasConflict { .. }) => continue,
            Err(error) => {
                return Err(TenantKmsError::RecordEpoch {
                    tenant: tenant.as_str().to_string(),
                    error,
                });
            }
        }
    }
    Err(TenantKmsError::CasRetriesExhausted {
        tenant: tenant.as_str().to_string(),
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{InstrumentedStore, StoreOp};

    const SERVER: KeyChangePolicy = KeyChangePolicy::RecordRotation;
    const CLI: KeyChangePolicy = KeyChangePolicy::Refuse;

    #[test]
    fn empty_file_yields_no_tenants() {
        let parsed = parse_tenant_kms_config("").expect("empty file parses");
        assert!(parsed.is_empty());
    }

    #[test]
    fn parses_tenant_table() {
        let text = r#"
            [tenants]
            acme = "arn:aws:kms:us-east-1:111122223333:key/acme"
            globex = "arn:aws:kms:us-east-1:111122223333:key/globex"
        "#;
        let parsed = parse_tenant_kms_config(text).expect("valid file parses");
        let mut got: Vec<_> = parsed.iter().map(|(id, arn)| (id.as_str(), arn)).collect();
        got.sort();
        assert_eq!(
            got,
            vec![
                ("acme", "arn:aws:kms:us-east-1:111122223333:key/acme"),
                ("globex", "arn:aws:kms:us-east-1:111122223333:key/globex"),
            ]
        );
        assert_eq!(
            parsed.key_for(&TenantId::new("acme")),
            Some("arn:aws:kms:us-east-1:111122223333:key/acme")
        );
        assert_eq!(parsed.key_for(&TenantId::new("initech")), None);

        let acme = parsed.restricted_to(&TenantId::new("acme"));
        let got: Vec<_> = acme.iter().map(|(id, arn)| (id.as_str(), arn)).collect();
        assert_eq!(
            got,
            vec![("acme", "arn:aws:kms:us-east-1:111122223333:key/acme")]
        );
        assert!(parsed.restricted_to(&TenantId::new("initech")).is_empty());
    }

    #[test]
    fn rejects_empty_tenant_id() {
        let text = r#"
            [tenants]
            "" = "arn:aws:kms:us-east-1:111122223333:key/acme"
        "#;
        parse_tenant_kms_config(text).expect_err("an empty tenant id must fail startup");
    }

    #[test]
    fn rejects_empty_key_arn() {
        let text = r#"
            [tenants]
            acme = ""
        "#;
        parse_tenant_kms_config(text)
            .expect_err("an empty key_arn is reserved for the deployment default");
    }

    /// The messages are the ones ravel-server printed before this module moved
    /// out of it, so an operator's startup error reads the same.
    #[test]
    fn refusal_messages_are_unchanged() {
        let empty_id =
            parse_tenant_kms_config("[tenants]\n\"\" = \"arn\"\n").expect_err("empty tenant id");
        assert_eq!(
            empty_id.to_string(),
            "[tenants] in --tenant-kms-config has an entry with an empty tenant id"
        );
        let empty_arn =
            parse_tenant_kms_config("[tenants]\nacme = \"\"\n").expect_err("empty key_arn");
        assert_eq!(
            empty_arn.to_string(),
            "[tenants] in --tenant-kms-config names tenant \"acme\" with an empty key_arn: an \
             empty string is reserved to mean the deployment default key and is never a valid \
             operator-configured value"
        );
    }

    #[test]
    fn rejects_unknown_field() {
        let text = r#"
            [tenants]
            acme = "arn:aws:kms:us-east-1:111122223333:key/acme"
            [bogus]
            x = 1
        "#;
        parse_tenant_kms_config(text).expect_err("an unknown top-level table must fail startup");
    }

    #[tokio::test]
    async fn bootstraps_epoch_zero_then_epoch_one_on_first_configuration() {
        let store = MemoryStore::new();
        let tenant = TenantId::new("acme");
        let hash = tenant.hash();
        let key_arn = "arn:aws:kms:us-east-1:111122223333:key/acme";

        bootstrap_tenant_epoch(&store, &tenant, &hash, key_arn, 1_000, SERVER)
            .await
            .expect("bootstrap");

        let epochs = read_epochs_from_store(&store, &hash)
            .await
            .expect("read back")
            .expect("epoch history now exists");
        assert_eq!(epochs.len(), 2);
        assert_eq!(epochs[0].epoch, 0);
        assert_eq!(epochs[0].key_arn, "");
        assert_eq!(epochs[0].activated_ns, 0);
        assert_eq!(epochs[1].epoch, 1);
        assert_eq!(epochs[1].key_arn, key_arn);
        assert_eq!(epochs[1].activated_ns, 1_000);
    }

    #[tokio::test]
    async fn restarting_with_the_same_key_is_a_no_op() {
        let store = MemoryStore::new();
        let tenant = TenantId::new("acme");
        let hash = tenant.hash();
        let key_arn = "arn:aws:kms:us-east-1:111122223333:key/acme";

        bootstrap_tenant_epoch(&store, &tenant, &hash, key_arn, 1_000, SERVER)
            .await
            .expect("first boot");
        bootstrap_tenant_epoch(&store, &tenant, &hash, key_arn, 2_000, SERVER)
            .await
            .expect("second boot with the same key");

        let epochs = read_epochs_from_store(&store, &hash)
            .await
            .expect("read back")
            .expect("epoch history exists");
        assert_eq!(
            epochs.len(),
            2,
            "an unchanged key across restarts appends nothing new"
        );
    }

    #[tokio::test]
    async fn rotating_to_a_different_key_appends_a_new_epoch() {
        let store = MemoryStore::new();
        let tenant = TenantId::new("acme");
        let hash = tenant.hash();
        let first_key = "arn:aws:kms:us-east-1:111122223333:key/acme-v1";
        let second_key = "arn:aws:kms:us-east-1:111122223333:key/acme-v2";

        bootstrap_tenant_epoch(&store, &tenant, &hash, first_key, 1_000, SERVER)
            .await
            .expect("first boot");
        bootstrap_tenant_epoch(&store, &tenant, &hash, second_key, 2_000, SERVER)
            .await
            .expect("rotation");

        let epochs = read_epochs_from_store(&store, &hash)
            .await
            .expect("read back")
            .expect("epoch history exists");
        assert_eq!(epochs.len(), 3, "rotation appends exactly one new epoch");
        assert_eq!(epochs[2].epoch, 2);
        assert_eq!(epochs[2].key_arn, second_key);
        assert_eq!(epochs[2].activated_ns, 2_000);
    }

    const FIRST_KEY: &str = "arn:aws:kms:us-east-1:111122223333:key/acme-v1";
    const SECOND_KEY: &str = "arn:aws:kms:us-east-1:111122223333:key/acme-v2";

    /// A routing store whose per-tenant builder is never reached: the epoch
    /// writes below go to the store passed beside it, and nothing here writes
    /// through the routing store itself.
    fn routing_store() -> KmsRoutingStore {
        use ravel_object_store::StoreMetrics;
        use ravel_object_store::s3::{S3Config, S3HttpConfig};
        KmsRoutingStore::new(
            std::sync::Arc::new(MemoryStore::new()),
            S3Config {
                bucket: "ravel-test".to_string(),
                region: "us-east-1".to_string(),
                endpoint: Some("http://localhost:0".to_string()),
                access_key_id: "test".to_string(),
                secret_access_key: "test".to_string(),
                allow_http: true,
                force_path_style: true,
                kms_key_id: None,
                session_token: None,
                credentials_file: None,
                auth: Default::default(),
                instance_metadata_endpoint: None,
            },
            S3HttpConfig::default(),
            std::sync::Arc::new(StoreMetrics::default()),
        )
    }

    fn acme_with(key_arn: &str) -> TenantKmsConfig {
        parse_tenant_kms_config(&format!("[tenants]\nacme = \"{key_arn}\"\n")).expect("parses")
    }

    /// A bucket whose `acme` record already holds `key_arn` as written by
    /// server startup, wrapped so a test counts only its own PUTs.
    async fn recorded_with(key_arn: &str) -> InstrumentedStore<MemoryStore> {
        let store = MemoryStore::new();
        let tenant = TenantId::new("acme");
        bootstrap_tenant_epoch(&store, &tenant, &tenant.hash(), key_arn, 1_000, SERVER)
            .await
            .expect("server bootstrap");
        InstrumentedStore::new(store)
    }

    fn puts(store: &InstrumentedStore<MemoryStore>) -> u64 {
        store.metrics().snapshot().op(StoreOp::Put).calls
    }

    async fn epochs_of(store: &dyn ObjectStoreBackend) -> Vec<KeyEpoch> {
        read_epochs_from_store(store, &TenantId::new("acme").hash())
            .await
            .expect("read back")
            .expect("epoch history exists")
    }

    fn absent_record_message(key_arn: &str) -> String {
        format!(
            "tenant \"acme\" has no key-epoch record at t/{}/enc, but --tenant-kms-config names \
             \"{key_arn}\" for it: a tenant's key epochs are recorded by ravel-server at \
             startup, never by this command. Refusing before any write; start ravel-server \
             with this --tenant-kms-config file first, then rerun this command",
            TenantId::new("acme").hash().to_hex()
        )
    }

    /// Non-vacuity: make `epoch_action` return `Ok(EpochAction::Bootstrap)`
    /// for an absent record under `Refuse` too and the call succeeds, writing
    /// epochs 0 and 1.
    #[tokio::test]
    async fn cli_policy_refuses_an_absent_record_and_writes_nothing() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let err = configure_tenant_kms_with_policy(
            &routing_store(),
            &store,
            &acme_with(FIRST_KEY),
            1_000,
            CLI,
        )
        .await
        .expect_err("an absent record is refused");

        let hash_hex = TenantId::new("acme").hash().to_hex();
        assert!(
            matches!(
                &err,
                TenantKmsError::EpochRecordAbsent { tenant, hash_hex: h, configured_key }
                    if tenant == "acme" && *h == hash_hex && configured_key == FIRST_KEY
            ),
            "{err:?}"
        );
        assert_eq!(err.to_string(), absent_record_message(FIRST_KEY));
        assert_eq!(puts(&store), 0, "no key epoch is written");
        assert!(
            read_epochs_from_store(&store, &TenantId::new("acme").hash())
                .await
                .expect("read back")
                .is_none(),
            "the record is still absent"
        );
    }

    /// The dry run's check reaches the real run's verdict for every record
    /// shape and writes nothing.
    ///
    /// Non-vacuity: pass `KeyChangePolicy::RecordRotation` in
    /// `check_tenant_kms_records` and the absent and differing records pass.
    #[tokio::test]
    async fn the_read_only_check_reaches_the_cli_verdict_and_writes_nothing() {
        let absent = InstrumentedStore::new(MemoryStore::new());
        let err = check_tenant_kms_records(&absent, &acme_with(FIRST_KEY))
            .await
            .expect_err("an absent record is refused");
        assert_eq!(err.to_string(), absent_record_message(FIRST_KEY));
        assert_eq!(puts(&absent), 0);

        let recorded = recorded_with(FIRST_KEY).await;
        check_tenant_kms_records(&recorded, &acme_with(FIRST_KEY))
            .await
            .expect("a matching record passes");
        let err = check_tenant_kms_records(&recorded, &acme_with(SECOND_KEY))
            .await
            .expect_err("a differing record is refused");
        assert!(
            matches!(err, TenantKmsError::KeyChangeRefused { .. }),
            "{err:?}"
        );
        assert_eq!(puts(&recorded), 0);

        let inner = MemoryStore::new();
        record_key_epoch(&inner, &TenantId::new("acme").hash(), "", 0, 500)
            .await
            .expect("epoch 0 alone");
        let unfinished = InstrumentedStore::new(inner);
        check_tenant_kms_records(&unfinished, &acme_with(FIRST_KEY))
            .await
            .expect("an unfinished bootstrap passes: the real run completes it");
        assert_eq!(puts(&unfinished), 0);
        assert_eq!(epochs_of(&unfinished).await.len(), 1);
    }

    #[tokio::test]
    async fn cli_policy_writes_nothing_for_a_matching_record() {
        let store = recorded_with(FIRST_KEY).await;
        configure_tenant_kms_with_policy(
            &routing_store(),
            &store,
            &acme_with(FIRST_KEY),
            2_000,
            CLI,
        )
        .await
        .expect("a matching record routes");
        assert_eq!(puts(&store), 0, "a matching record is not rewritten");
        assert_eq!(epochs_of(&store).await.len(), 2);
    }

    /// Non-vacuity: force `CLI` to `KeyChangePolicy::RecordRotation` and the
    /// call succeeds, appending a third epoch.
    #[tokio::test]
    async fn cli_policy_refuses_a_differing_record_and_writes_nothing() {
        let store = recorded_with(FIRST_KEY).await;
        let err = configure_tenant_kms_with_policy(
            &routing_store(),
            &store,
            &acme_with(SECOND_KEY),
            2_000,
            CLI,
        )
        .await
        .expect_err("a key change is refused");

        let hash_hex = TenantId::new("acme").hash().to_hex();
        assert!(
            matches!(
                &err,
                TenantKmsError::KeyChangeRefused { tenant, hash_hex: h, recorded_key, configured_key }
                    if tenant == "acme" && *h == hash_hex && recorded_key == FIRST_KEY
                        && configured_key == SECOND_KEY
            ),
            "{err:?}"
        );
        assert_eq!(
            err.to_string(),
            format!(
                "tenant \"acme\" has key \"{FIRST_KEY}\" recorded as its current key epoch at \
                 t/{hash_hex}/enc, but --tenant-kms-config names \"{SECOND_KEY}\": a key change \
                 is recorded by ravel-server at startup, never by this command. Refusing before \
                 any write; start ravel-server with the new key first, or run this command with \
                 the file the servers run with"
            )
        );
        assert_eq!(puts(&store), 0, "a refused key change writes nothing");
        assert_eq!(epochs_of(&store).await.len(), 2, "no epoch was appended");
    }

    /// The server's entry point keeps recording the rotation.
    #[tokio::test]
    async fn server_entry_point_appends_a_rotation_epoch() {
        let store = recorded_with(FIRST_KEY).await;
        configure_tenant_kms(&routing_store(), &store, &acme_with(SECOND_KEY), 2_000)
            .await
            .expect("server startup records the rotation");
        let epochs = epochs_of(&store).await;
        assert_eq!(
            epochs
                .iter()
                .map(|e| (e.epoch, e.key_arn.as_str(), e.activated_ns))
                .collect::<Vec<_>>(),
            vec![(0, "", 0), (1, FIRST_KEY, 1_000), (2, SECOND_KEY, 2_000)]
        );
        assert_eq!(puts(&store), 1, "exactly one rotation epoch appended");
    }

    /// A record holding only the bootstrap epoch 0 never recorded a key, so
    /// finishing it is the first configuration, not a key change.
    #[tokio::test]
    async fn cli_policy_finishes_an_unfinished_bootstrap() {
        let inner = MemoryStore::new();
        record_key_epoch(&inner, &TenantId::new("acme").hash(), "", 0, 500)
            .await
            .expect("epoch 0 alone");
        let store = InstrumentedStore::new(inner);
        configure_tenant_kms_with_policy(
            &routing_store(),
            &store,
            &acme_with(FIRST_KEY),
            1_000,
            CLI,
        )
        .await
        .expect("the bootstrap is finished");
        assert_eq!(epochs_of(&store).await.len(), 2);
        assert_eq!(puts(&store), 1);
    }
}
