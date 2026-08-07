//! `ravel-cli catalog` subcommands beyond `list` (docs/metric-index-plan.md
//! section 4, ADR-0020): `fold` (one-shot), `inspect` (decode HEAD and every
//! referenced snapshot part), and `verify` (re-list sealed commit records
//! and diff against the snapshot). Built strictly against `ravel-catalog`'s
//! public API: no new methods added to that crate for this tool.

use std::sync::Arc;

use ravel_catalog::{CatalogConfig, PartLimits};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

/// HEAD object key (docs/catalog-and-mvcc.md key layout, frozen format).
/// Duplicated here rather than imported: `ravel-catalog` only exposes the
/// equivalent helper as `pub(crate)` (`ravel_server::fold` duplicates it the
/// same way, for the same reason).
fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

pub async fn fold(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    shard_count: u32,
) -> anyhow::Result<()> {
    let catalog_config = CatalogConfig {
        shard_count,
        ..CatalogConfig::default()
    };
    // Enforcing, exactly as the server's query path (`ravel_server::query`) is:
    // an enforcing fold reads the tenant's real shard-generation history and
    // stamps a correct `shard_generation_count` and fan-out ceiling, instead of
    // short-circuiting to the single implicit generation 0 and enumerating only
    // `0..--shards` (ADR-0052 sections 4/5, Finding 3). Without this the fold is
    // blind to any reshard and writes an under-scanning HEAD.
    let catalog = ravel_catalog::Catalog::new(store, catalog_config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?
        .with_provisioning_enforcement();

    let tenant_hash = TenantId::new(tenant).hash();
    let now = crate::now_ns()?;
    let folder_id = Uuid::new_v4();
    let report = catalog
        .fold(&tenant_hash, Signal::Metrics, folder_id, now, &[])
        .await
        .map_err(|err| anyhow::anyhow!("fold failed: {err}"))?;

    println!("no_op: {}", report.no_op);
    println!("rebuilt: {}", report.rebuilt);
    println!(
        "previous_watermark_hour: {:?}",
        report.previous_watermark_hour
    );
    println!("watermark_hour: {:?}", report.watermark_hour);
    println!("buckets_folded: {}", report.buckets_folded);
    println!("entry_count: {}", report.entry_count);
    println!("part_bytes: {}", report.part_bytes);
    println!("list_requests: {}", report.list_requests);
    println!("get_requests: {}", report.get_requests);
    println!("put_requests: {}", report.put_requests);
    Ok(())
}

pub async fn inspect(store: Arc<dyn ObjectStoreBackend>, tenant: &str) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let key = head_key(&tenant_hash, Signal::Metrics);

    let head_bytes = match store.get(&key, GetRange::Full).await {
        Ok(outcome) => outcome.data,
        Err(err) => {
            println!("HEAD at {key} is absent or unreadable: {err}");
            return Ok(());
        }
    };
    let head = ravel_catalog::decode_head(&head_bytes)
        .map_err(|err| anyhow::anyhow!("HEAD at {key} is corrupt: {err}"))?;

    println!("format_version: {}", head.format_version);
    println!("tenant_hash: {}", hex::encode(&head.tenant_hash));
    println!("signal: {}", head.signal);
    println!("shard_count: {}", head.shard_count);
    println!("watermark_hour: {}", head.watermark_hour);
    println!("folder_id: {}", format_uuid_bytes(&head.folder_id));
    println!("created_unix_ns: {}", head.created_unix_ns);
    println!("parts: {}", head.parts.len());

    let limits = PartLimits::default();
    for part_ref in &head.parts {
        println!(
            "  key={} blake3={} size={} entry_count={}",
            part_ref.key,
            hex::encode(&part_ref.blake3),
            part_ref.size,
            part_ref.entry_count
        );
        let got = store
            .get(&part_ref.key, GetRange::Full)
            .await
            .map_err(|err| anyhow::anyhow!("failed to fetch part {}: {err}", part_ref.key))?;
        let decoded = ravel_catalog::decode_part(&got.data, &limits)
            .map_err(|err| anyhow::anyhow!("part {} is corrupt: {err}", part_ref.key))?;
        println!(
            "    header: format_version={} watermark_hour={} shard_count={} entry_count={} entries_uncompressed_len={}",
            decoded.header.format_version,
            decoded.header.watermark_hour,
            decoded.header.shard_count,
            decoded.header.entry_count,
            decoded.header.entries_uncompressed_len
        );
        println!("    entries (decoded): {}", decoded.entries.len());
    }
    Ok(())
}

fn format_identity(id: &ravel_catalog::EntryIdentity) -> String {
    format!(
        "shard={} ingest_hour_bucket={} writer_id={} writer_epoch={} writer_seq={}",
        id.0,
        id.1,
        Uuid::from_bytes(id.2),
        id.3,
        id.4
    )
}

fn format_uuid_bytes(bytes: &[u8]) -> String {
    <[u8; 16]>::try_from(bytes)
        .map(|raw| Uuid::from_bytes(raw).to_string())
        .unwrap_or_else(|_| hex::encode(bytes))
}

/// Re-lists every sealed commit record directly from the store and diffs it
/// against the current snapshot's entries. Exits nonzero (via the returned
/// error) only for divergences that indicate the folder under-counted:
/// sealed commit records missing from the snapshot, or present with a
/// mismatched `content_hash`. Snapshot entries with no matching commit
/// record are reported but never fail verification: retention deleting a
/// commit record after it has been folded (docs/metric-index-plan.md
/// section 7 reconciliation) produces exactly this shape and is expected,
/// not a divergence.
pub async fn verify(store: Arc<dyn ObjectStoreBackend>, tenant: &str) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();

    // The comparison itself lives in `ravel-catalog` (ADR-0059 decision 2) so
    // the scheduled scrubber and this CLI share one implementation. This
    // command keeps its own presentation: the `println!` report and the nonzero
    // exit (via `anyhow::bail!`) on a real divergence.
    let report =
        match ravel_catalog::verify_seal_divergence(store.as_ref(), &tenant_hash, Signal::Metrics)
            .await
            .map_err(|err| anyhow::anyhow!("{err}"))?
        {
            Some(report) => report,
            None => {
                let key = head_key(&tenant_hash, Signal::Metrics);
                println!("no HEAD found at {key}; nothing folded yet, nothing to verify");
                return Ok(());
            }
        };

    println!("watermark_hour: {}", report.watermark_hour);
    println!(
        "sealed commit records (re-listed): {}",
        report.sealed_record_count
    );
    println!("snapshot entries: {}", report.snapshot_entry_count);
    println!("missing from snapshot: {}", report.missing.len());
    for id in &report.missing {
        println!("  MISSING {}", format_identity(id));
    }
    println!("content_hash mismatches: {}", report.mismatched.len());
    for id in &report.mismatched {
        println!("  MISMATCH {}", format_identity(id));
    }
    println!(
        "snapshot entries with no matching sealed commit record (expected once retention \
         deletes folded commit records): {}",
        report.orphaned.len()
    );
    for id in &report.orphaned {
        println!("  ORPHAN {}", format_identity(id));
    }

    if report.has_divergence() {
        anyhow::bail!(
            "catalog verify found {} missing and {} mismatched entries against the sealed commit history",
            report.missing.len(),
            report.mismatched.len(),
        );
    }
    println!("catalog verify: snapshot matches the sealed commit history");
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    /// Finding 3: the CLI `catalog fold` path must be enforcing, so it reads the
    /// tenant's real shard-generation history and stamps a correct
    /// `shard_generation_count`, rather than short-circuiting to the single
    /// implicit generation 0. A tenant with a reshard history (gen0, gen1) must
    /// produce a HEAD with `shard_generation_count` 2; a non-enforcing fold would
    /// stamp 1 (blind to the reshard) and under-scan.
    #[tokio::test]
    async fn fold_is_enforcing_and_stamps_real_generation_count() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "cli-fold-enforcing";
        let tenant_hash = TenantId::new(tenant).hash();

        ravel_catalog::validate_or_adopt(
            store.as_ref(),
            &tenant_hash,
            Signal::Metrics,
            1,
            0,
            ravel_catalog::AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("create generation 0");
        let now = crate::now_ns().expect("now");
        let activation = (now / NS_PER_HOUR) as u32 + 10;
        ravel_catalog::append_generation(
            store.as_ref(),
            &tenant_hash,
            Signal::Metrics,
            2,
            activation,
            now,
        )
        .await
        .expect("append generation 1");

        fold(store.clone(), tenant, 1).await.expect("cli fold");

        let head_bytes = store
            .get(&head_key(&tenant_hash, Signal::Metrics), GetRange::Full)
            .await
            .expect("HEAD present after fold")
            .data;
        let head = ravel_catalog::decode_head(&head_bytes).expect("decode head");
        assert_eq!(
            head.shard_generation_count, 2,
            "an enforcing CLI fold reads the real generation history (sgc 2), not \
             the implicit generation 0 (sgc 1)"
        );
    }
}
