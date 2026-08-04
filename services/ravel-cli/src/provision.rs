//! `ravel-cli provision adopt` (ADR-0050 section 5, EC5): write the durable
//! `shard_count` provisioning record for a (tenant, signal) with pre-ADR data,
//! ahead of any server touching it.
//!
//! This runs the exact same adoption code path a server runs at ingest,
//! catalog, and maintenance touch: [`ravel_catalog::validate_or_adopt`] with
//! [`ravel_catalog::AbsentPolicy::AdoptIfData`]. There is no CLI-side
//! reimplementation of the adoption rule, so an operator running this before an
//! upgrade rollout gets byte-for-byte the same decision the server would: adopt
//! only when every observed shard index is below the configured `shard_count`,
//! and refuse (writing nothing) when a higher shard index proves the configured
//! value would hide data.

use std::sync::Arc;

use ravel_catalog::{AbsentPolicy, ProvisioningCheck, validate_or_adopt};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{Signal, TenantId};

use crate::maintain::SignalArg;

/// Adopt the provisioning record for one tenant, across one signal (`--signal`)
/// or all three ingested signals. `shards` is the configured shard_count to
/// adopt at. The tenant hash uses the process-wide scheme already resolved from
/// `sys/tenancy` (the CLI installs it before any tenant-hashing command).
///
/// Prints one line per signal. A refusal on any signal (a shard index at or
/// above `shards`, so the value would hide data) is reported per signal and the
/// command exits nonzero, but every signal is still attempted so the operator
/// sees the full picture rather than only the first failure.
pub async fn adopt(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    shards: u32,
    signal: Option<SignalArg>,
    now_ns: i64,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let signals: Vec<Signal> = match signal {
        Some(s) => vec![s.to_signal()],
        None => vec![Signal::Metrics, Signal::Logs, Signal::Spans],
    };

    let mut refused = false;
    for signal in signals {
        let sig = signal.key_prefix();
        match validate_or_adopt(
            store.as_ref(),
            &tenant_hash,
            signal,
            shards,
            now_ns,
            AbsentPolicy::AdoptIfData,
        )
        .await
        {
            Ok(ProvisioningCheck::Written) => {
                println!("{sig}: adopted (wrote provisioning record with shard_count={shards})");
            }
            Ok(ProvisioningCheck::Matched) => {
                println!("{sig}: already provisioned; recorded shard_count matches {shards}");
            }
            Ok(ProvisioningCheck::FreshNoData) => {
                println!(
                    "{sig}: no data and no record; nothing to adopt (the record is written on \
                     the tenant's first ingest for this signal)"
                );
            }
            Err(err) => {
                refused = true;
                println!("{sig}: REFUSED: {err}");
            }
        }
    }

    if refused {
        anyhow::bail!(
            "provision adopt refused for at least one signal: the configured --shards would hide \
             existing data. Nothing was written for a refused signal. Re-run with a shard count \
             that covers every observed shard index."
        );
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, PutOptions, StoreError};

    const TENANT: &str = "acme";

    async fn seed_l0_shard(store: &dyn ObjectStoreBackend, signal: Signal, shard: u32) {
        let hash = TenantId::new(TENANT).hash();
        let key = format!(
            "t/{}/{}/l0/{:04}/writer.0.{:020}.deadbeefdeadbeef.rseg",
            hash.to_hex(),
            signal.key_prefix(),
            shard,
            1u64
        );
        store
            .put(&key, vec![1].into(), PutOptions::default())
            .await
            .expect("seed l0");
    }

    #[tokio::test]
    async fn adopt_writes_record_when_shards_in_range() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        seed_l0_shard(store.as_ref(), Signal::Metrics, 0).await;
        seed_l0_shard(store.as_ref(), Signal::Metrics, 3).await;
        adopt(store.clone(), TENANT, 4, Some(SignalArg::Metrics), 1_000)
            .await
            .expect("adoption succeeds when all shards are in range");
        let key = ravel_catalog::provisioning_key(&TenantId::new(TENANT).hash(), Signal::Metrics);
        store
            .get(&key, GetRange::Full)
            .await
            .expect("provisioning record was written");
    }

    #[tokio::test]
    async fn adopt_refuses_and_writes_nothing_when_shard_out_of_range() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        // Data at shard 5, configured for 4: shard 4 and 5 would be hidden.
        seed_l0_shard(store.as_ref(), Signal::Metrics, 5).await;
        let err = adopt(store.clone(), TENANT, 4, Some(SignalArg::Metrics), 1_000)
            .await
            .expect_err("an out-of-range shard must refuse adoption");
        assert!(err.to_string().contains("refused"), "err: {err}");
        let key = ravel_catalog::provisioning_key(&TenantId::new(TENANT).hash(), Signal::Metrics);
        let got = store.get(&key, GetRange::Full).await;
        assert!(
            matches!(got, Err(StoreError::NotFound)),
            "no record written"
        );
    }

    #[tokio::test]
    async fn adopt_on_fresh_signal_writes_nothing_and_succeeds() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        adopt(store.clone(), TENANT, 4, Some(SignalArg::Metrics), 1_000)
            .await
            .expect("a fresh signal with no data adopts nothing and succeeds");
        let key = ravel_catalog::provisioning_key(&TenantId::new(TENANT).hash(), Signal::Metrics);
        let got = store.get(&key, GetRange::Full).await;
        assert!(
            matches!(got, Err(StoreError::NotFound)),
            "no record written"
        );
    }
}
