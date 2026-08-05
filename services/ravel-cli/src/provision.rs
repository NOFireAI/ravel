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

use ravel_catalog::{AbsentPolicy, ProvisioningCheck, append_generation, validate_or_adopt};
use ravel_maintain::write_reshard_audit;
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

use crate::maintain::SignalArg;

/// Nanoseconds per unix hour, the unit `activation_hour` counts in.
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Minimum reshard lead in hours: `ceil(C) + 1` with the router's default 60s
/// refresh interval `C` (ADR-0052 section 3). `ceil(60s) = 1` hour, `+ 1 = 2`.
/// A shorter lead could let a live writer route past the activation on a view
/// it had not yet refreshed, so the CLI refuses it.
const MIN_LEAD_HOURS: u32 = 2;

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

/// Reshard one (tenant, signal) online (ADR-0052): compute an activation hour
/// `--lead-hours` in the future, append a new shard generation to the
/// provisioning record under `CasVersion` via the same
/// [`ravel_catalog::append_generation`] the server uses, and write a
/// control-plane audit record (ADR-0040/ADR-0042). Existing data is never moved
/// or re-keyed; only data ingested from the activation hour onward routes with
/// the new count.
///
/// Refuses a lead shorter than [`MIN_LEAD_HOURS`] before touching the store, so
/// the activation is always far enough out that a live writer observes it (or
/// fail-stops on staleness) before it takes effect. The append itself re-checks
/// the future-activation, differing-count, and range invariants fail-closed.
pub async fn reshard(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
    shard_count: u32,
    lead_hours: u32,
    now_ns: i64,
) -> anyhow::Result<()> {
    if lead_hours < MIN_LEAD_HOURS {
        anyhow::bail!(
            "--lead-hours {lead_hours} is below the minimum {MIN_LEAD_HOURS} \
             (ceil(C) + 1 with the router's default 60s refresh interval): a shorter lead could \
             let a live writer route past the activation on a view it had not yet refreshed. \
             Nothing was written."
        );
    }
    let signal = signal.to_signal();
    let tenant_hash = TenantId::new(tenant).hash();
    let now_hour = u32::try_from(now_ns.div_euclid(NS_PER_HOUR).max(0))
        .map_err(|_| anyhow::anyhow!("current time is out of hour-bucket range"))?;
    let activation_hour = now_hour.checked_add(lead_hours).ok_or_else(|| {
        anyhow::anyhow!("activation hour overflows: now_hour {now_hour} + lead {lead_hours}")
    })?;

    let outcome = append_generation(
        store.as_ref(),
        &tenant_hash,
        signal,
        shard_count,
        activation_hour,
        now_ns,
    )
    .await
    .map_err(|err| anyhow::anyhow!("reshard append failed: {err}"))?;

    // The count the just-superseded generation routed at, for the audit trail.
    let from_shard_count = outcome
        .generations
        .iter()
        .rev()
        .nth(1)
        .map(|g| g.shard_count)
        .unwrap_or(shard_count);

    // Attributable control-plane mutation: write the immutable audit record
    // through the shared ADR-0040 audit-write path. The generation is already
    // durable at this point; surface an audit-write failure loudly rather than
    // leaving the reshard silently unaudited.
    write_reshard_audit(
        store.as_ref(),
        &tenant_hash,
        Uuid::new_v4(),
        now_ns,
        signal.key_prefix(),
        from_shard_count,
        shard_count,
        outcome.generation,
        activation_hour,
    )
    .await
    .map_err(|err| {
        anyhow::anyhow!(
            "reshard generation {} was appended (shard_count {from_shard_count} -> {shard_count}, \
             activation_hour {activation_hour}), but writing its audit record failed: {err}. The \
             reshard is durable; re-run is not needed, but the audit trail is incomplete.",
            outcome.generation
        )
    })?;

    println!(
        "{}: resharded shard_count {from_shard_count} -> {shard_count}, appended generation {} \
         activating at unix hour {activation_hour} (in {lead_hours}h)",
        signal.key_prefix(),
        outcome.generation
    );
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

    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    /// Seed a generation-0 provisioning record via the shared adoption path so a
    /// reshard has an existing record to append to.
    async fn seed_record(store: &dyn ObjectStoreBackend, signal: Signal, shard_count: u32) {
        validate_or_adopt(
            store,
            &TenantId::new(TENANT).hash(),
            signal,
            shard_count,
            1_000,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("seed generation-0 record");
    }

    /// A valid reshard appends a generation to the record and writes an audit
    /// object under the control-plane audit shard.
    #[tokio::test]
    async fn reshard_appends_generation_and_writes_audit() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        seed_record(store.as_ref(), Signal::Metrics, 4).await;
        // now = hour 10; lead 2 -> activation hour 12.
        reshard(
            store.clone(),
            TENANT,
            SignalArg::Metrics,
            8,
            2,
            10 * NS_PER_HOUR,
        )
        .await
        .expect("a valid reshard succeeds");

        // The record now carries an explicit two-generation history.
        let key = ravel_catalog::provisioning_key(&TenantId::new(TENANT).hash(), Signal::Metrics);
        let bytes = store.get(&key, GetRange::Full).await.expect("record").data;
        let record =
            <ravel_proto::sys::v1::ProvisioningRecord as prost::Message>::decode(bytes.as_ref())
                .expect("decode");
        assert_eq!(record.shard_count, 4, "scalar shard_count unchanged");
        assert_eq!(record.generations.len(), 2);
        assert_eq!(record.generations[1].shard_count, 8);
        assert_eq!(record.generations[1].activation_hour, 12);

        // An audit object was written under the control-plane audit shard.
        let audit_prefix = format!(
            "t/{}/{}/l0/",
            TenantId::new(TENANT).hash().to_hex(),
            Signal::Audit.key_prefix()
        );
        let audit = ravel_object_store::list_all(store.as_ref(), &audit_prefix)
            .await
            .expect("list audit");
        assert_eq!(audit.len(), 1, "one reshard audit object was written");
    }

    /// A lead shorter than the minimum is refused before touching the store.
    #[tokio::test]
    async fn reshard_rejects_lead_below_minimum() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        seed_record(store.as_ref(), Signal::Metrics, 4).await;
        let err = reshard(
            store.clone(),
            TENANT,
            SignalArg::Metrics,
            8,
            1,
            10 * NS_PER_HOUR,
        )
        .await
        .expect_err("a lead below the minimum must be refused");
        assert!(err.to_string().contains("below the minimum"), "err: {err}");
        // No generation was appended.
        let key = ravel_catalog::provisioning_key(&TenantId::new(TENANT).hash(), Signal::Metrics);
        let bytes = store.get(&key, GetRange::Full).await.expect("record").data;
        let record =
            <ravel_proto::sys::v1::ProvisioningRecord as prost::Message>::decode(bytes.as_ref())
                .expect("decode");
        assert!(
            record.generations.is_empty(),
            "a refused reshard appends nothing"
        );
    }

    /// A reshard to the same count is refused by the append (adjacent counts
    /// must differ), surfaced as a CLI error.
    #[tokio::test]
    async fn reshard_rejects_same_count() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        seed_record(store.as_ref(), Signal::Metrics, 4).await;
        let err = reshard(
            store.clone(),
            TENANT,
            SignalArg::Metrics,
            4,
            2,
            10 * NS_PER_HOUR,
        )
        .await
        .expect_err("a same-count reshard must be refused");
        assert!(
            err.to_string().contains("reshard append failed"),
            "err: {err}"
        );
    }
}
