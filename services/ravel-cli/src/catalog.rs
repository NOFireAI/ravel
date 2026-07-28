//! `ravel-cli catalog` subcommands beyond `list` (docs/metric-index-plan.md
//! section 4, ADR-0020): `fold` (one-shot), `inspect` (decode HEAD and every
//! referenced snapshot part), and `verify` (re-list sealed commit records
//! and diff against the snapshot). Built strictly against `ravel-catalog`'s
//! public API: no new methods added to that crate for this tool.

use std::collections::BTreeMap;
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
    let catalog = ravel_catalog::Catalog::new(store, catalog_config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?;

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

/// Entry identity matching `docs/metric-index-plan.md` section 4's dedup
/// key: (shard, ingest_hour_bucket, writer_id, writer_epoch, writer_seq).
type EntryIdentity = (u32, u32, [u8; 16], u64, u64);

fn format_identity(id: &EntryIdentity) -> String {
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
    let key = head_key(&tenant_hash, Signal::Metrics);

    let head_bytes = match store.get(&key, GetRange::Full).await {
        Ok(outcome) => outcome.data,
        Err(_) => {
            println!("no HEAD found at {key}; nothing folded yet, nothing to verify");
            return Ok(());
        }
    };
    let head = ravel_catalog::decode_head(&head_bytes)
        .map_err(|err| anyhow::anyhow!("HEAD at {key} is corrupt: {err}"))?;

    let limits = PartLimits::default();
    let mut snapshot_entries: BTreeMap<EntryIdentity, Vec<u8>> = BTreeMap::new();
    for part_ref in &head.parts {
        let got = store
            .get(&part_ref.key, GetRange::Full)
            .await
            .map_err(|err| anyhow::anyhow!("failed to fetch part {}: {err}", part_ref.key))?;
        let decoded = ravel_catalog::decode_part(&got.data, &limits)
            .map_err(|err| anyhow::anyhow!("part {} is corrupt: {err}", part_ref.key))?;
        for entry in decoded.entries {
            let writer_id: [u8; 16] = entry.writer_id.as_slice().try_into().map_err(|_| {
                anyhow::anyhow!("part {} has a malformed writer_id entry", part_ref.key)
            })?;
            let identity = (
                entry.shard,
                entry.ingest_hour_bucket,
                writer_id,
                entry.writer_epoch,
                entry.writer_seq,
            );
            snapshot_entries.insert(identity, entry.content_hash);
        }
    }

    let mut ground_truth: BTreeMap<EntryIdentity, Vec<u8>> = BTreeMap::new();
    for shard in 0..head.shard_count {
        let prefix = ravel_commit::keys::commit_shard_prefix(&tenant_hash, Signal::Metrics, shard)
            .map_err(|err| anyhow::anyhow!("failed to build shard prefix: {err}"))?;
        let objects = ravel_object_store::list_all(store.as_ref(), &prefix)
            .await
            .map_err(|err| anyhow::anyhow!("failed to list {prefix}: {err}"))?;
        for object in objects {
            let Ok(parsed) = ravel_commit::keys::parse_commit_key(&object.key) else {
                continue;
            };
            if parsed.ingest_hour_bucket > head.watermark_hour {
                continue;
            }
            let got = store
                .get(&object.key, GetRange::Full)
                .await
                .map_err(|err| anyhow::anyhow!("failed to fetch {}: {err}", object.key))?;
            let record = ravel_commit::record::decode(&got.data).map_err(|err| {
                anyhow::anyhow!("commit record at {} is corrupt: {err}", object.key)
            })?;
            let writer_id = *Uuid::parse_str(&record.writer_id)
                .map_err(|_| {
                    anyhow::anyhow!("commit record at {} has an invalid writer_id", object.key)
                })?
                .as_bytes();
            let identity = (
                record.shard,
                record.ingest_hour_bucket,
                writer_id,
                record.writer_epoch,
                record.writer_seq,
            );
            ground_truth.insert(identity, record.content_hash);
        }
    }

    let mut missing = Vec::new();
    let mut mismatched = Vec::new();
    for (identity, hash) in &ground_truth {
        match snapshot_entries.get(identity) {
            None => missing.push(*identity),
            Some(snap_hash) if snap_hash != hash => mismatched.push(*identity),
            Some(_) => {}
        }
    }
    let orphaned: Vec<EntryIdentity> = snapshot_entries
        .keys()
        .filter(|id| !ground_truth.contains_key(*id))
        .copied()
        .collect();

    println!("watermark_hour: {}", head.watermark_hour);
    println!("sealed commit records (re-listed): {}", ground_truth.len());
    println!("snapshot entries: {}", snapshot_entries.len());
    println!("missing from snapshot: {}", missing.len());
    for id in &missing {
        println!("  MISSING {}", format_identity(id));
    }
    println!("content_hash mismatches: {}", mismatched.len());
    for id in &mismatched {
        println!("  MISMATCH {}", format_identity(id));
    }
    println!(
        "snapshot entries with no matching sealed commit record (expected once retention \
         deletes folded commit records): {}",
        orphaned.len()
    );
    for id in &orphaned {
        println!("  ORPHAN {}", format_identity(id));
    }

    if !missing.is_empty() || !mismatched.is_empty() {
        anyhow::bail!(
            "catalog verify found {} missing and {} mismatched entries against the sealed commit history",
            missing.len(),
            mismatched.len(),
        );
    }
    println!("catalog verify: snapshot matches the sealed commit history");
    Ok(())
}
