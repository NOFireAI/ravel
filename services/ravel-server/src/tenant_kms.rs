//! Per-tenant SSE-KMS wiring (EL-7, issue #764, ADR-0062 decision 1,
//! ADR-0072 decision 2): `--tenant-kms-config` maps tenant name to KMS key
//! ARN. Parsing (this module's [`parse_tenant_kms_config`]) happens at
//! startup, before the tenant-hash scheme is installed; applying the parsed
//! config to a live [`KmsRoutingStore`] ([`configure_tenant_kms`]) happens
//! after, once `TenantId::hash()` is valid to call (`main.rs` sequences the
//! two around `install_tenant_hash_scheme`).

use std::collections::HashMap;

use ravel_catalog::{KeyEpochError, read_epochs_from_store, record_key_epoch};
use ravel_object_store::{KmsRoutingStore, ObjectStoreBackend};
use ravel_types::{TenantHash, TenantId};
use serde::Deserialize;

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
pub fn parse_tenant_kms_config(text: &str) -> anyhow::Result<TenantKmsConfig> {
    let file: TenantKmsFileToml = toml::from_str(text)?;
    let mut tenants = HashMap::with_capacity(file.tenants.len());
    for (id, key_arn) in file.tenants {
        if id.is_empty() {
            anyhow::bail!("[tenants] in --tenant-kms-config has an entry with an empty tenant id");
        }
        if key_arn.is_empty() {
            anyhow::bail!(
                "[tenants] in --tenant-kms-config names tenant {id:?} with an empty key_arn: \
                 an empty string is reserved to mean the deployment default key and is never a \
                 valid operator-configured value"
            );
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
pub async fn configure_tenant_kms(
    kms: &KmsRoutingStore,
    store: &dyn ObjectStoreBackend,
    config: &TenantKmsConfig,
    now_ns: i64,
) -> anyhow::Result<()> {
    for (tenant, key_arn) in config.iter() {
        let hash = tenant.hash();
        bootstrap_tenant_epoch(store, tenant, &hash, key_arn, now_ns).await?;
        kms.set_tenant_key(hash.to_hex(), key_arn.to_string());
    }
    Ok(())
}

async fn bootstrap_tenant_epoch(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    hash: &TenantHash,
    key_arn: &str,
    now_ns: i64,
) -> anyhow::Result<()> {
    for _ in 0..MAX_EPOCH_CAS_RETRIES {
        let existing = read_epochs_from_store(store, hash).await.map_err(|err| {
            anyhow::anyhow!(
                "failed to read key-epoch history for tenant {:?}: {err}",
                tenant.as_str()
            )
        })?;

        let outcome = match existing {
            None => match record_key_epoch(store, hash, "", 0, now_ns).await {
                Ok(_) => record_key_epoch(store, hash, key_arn, now_ns, now_ns)
                    .await
                    .map(|_| ()),
                Err(err) => Err(err),
            },
            Some(epochs) => {
                let Some(last) = epochs.last() else {
                    anyhow::bail!(
                        "tenant {:?} has an empty key-epoch history at t/{}/enc",
                        tenant.as_str(),
                        hash.to_hex()
                    );
                };
                if last.key_arn == key_arn {
                    Ok(())
                } else {
                    record_key_epoch(store, hash, key_arn, now_ns, now_ns)
                        .await
                        .map(|_| ())
                }
            }
        };

        match outcome {
            Ok(()) => return Ok(()),
            Err(KeyEpochError::CasConflict { .. }) => continue,
            Err(err) => {
                anyhow::bail!(
                    "failed to record key-epoch for tenant {:?}: {err}",
                    tenant.as_str()
                );
            }
        }
    }
    anyhow::bail!(
        "tenant {:?} key-epoch bootstrap kept losing a concurrent CAS race after \
         {MAX_EPOCH_CAS_RETRIES} retries",
        tenant.as_str()
    )
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

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

        bootstrap_tenant_epoch(&store, &tenant, &hash, key_arn, 1_000)
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

        bootstrap_tenant_epoch(&store, &tenant, &hash, key_arn, 1_000)
            .await
            .expect("first boot");
        bootstrap_tenant_epoch(&store, &tenant, &hash, key_arn, 2_000)
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

        bootstrap_tenant_epoch(&store, &tenant, &hash, first_key, 1_000)
            .await
            .expect("first boot");
        bootstrap_tenant_epoch(&store, &tenant, &hash, second_key, 2_000)
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
}
