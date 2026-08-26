//! `ravel-cli catalog` subcommands beyond `list` (ADR-0020): `fold`
//! (one-shot), `inspect` (decode HEAD and every
//! referenced snapshot part), and `verify` (re-list sealed commit records
//! and diff against the snapshot). Built strictly against `ravel-catalog`'s
//! public API: no new methods added to that crate for this tool.
//!
//! Every one of the three is per (tenant, signal), exactly as
//! `ravel_catalog::Catalog::fold` and the ADR-0020 resolve are: a tenant's
//! logs snapshot is a different object from its metrics snapshot, and folding
//! one leaves the other untouched. `--signal` selects which, defaulting to
//! metrics.

use std::sync::Arc;

use ravel_catalog::{CatalogConfig, FoldReport, PartLimits};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

use crate::maintain::SignalArg;

/// HEAD object key (docs/catalog-and-mvcc.md key layout, frozen format).
/// Duplicated here rather than imported: `ravel-catalog` only exposes the
/// equivalent helper as `pub(crate)` (`ravel_server::fold` duplicates it the
/// same way, for the same reason).
fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

/// Folds one signal's catalog snapshot for a tenant, and returns the report it
/// printed so a caller (the tests) can assert on it rather than on stdout.
///
/// `signal` picks which snapshot is folded. A logs or spans tenant is folded
/// only when `signal` names it: folding metrics on a logs tenant seals nothing
/// and publishes an empty metrics HEAD, leaving every logs query to re-derive
/// its snapshot by listing and reading every commit record (issue #718).
pub async fn fold(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    shard_count: u32,
    signal: SignalArg,
) -> anyhow::Result<FoldReport> {
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
        .fold(&tenant_hash, signal.to_signal(), folder_id, now, &[], None)
        .await
        .map_err(|err| anyhow::anyhow!("fold failed: {err}"))?;

    println!("signal: {}", signal_word(signal.to_signal()));
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
    Ok(report)
}

pub async fn inspect(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
) -> anyhow::Result<()> {
    let mut out = String::new();
    let result = render_inspect(store, tenant, signal, &mut out).await;
    // Print whatever was rendered even on error: a part fetch/decode failure
    // partway through must not discard the HEAD fields and every earlier
    // part's ref this call already rendered -- an operator inspecting a
    // damaged catalog needs exactly that partial output, not just the error
    // string for the one part that failed.
    print!("{out}");
    result
}

/// Decodes the tenant's HEAD for `signal` and every referenced snapshot part into
/// the human-readable report `inspect` prints, appending into `out` as it
/// goes so a failure partway through still leaves everything rendered so far
/// in `out` for the caller to print. Returns `Err` (with `out` still holding
/// everything rendered before the failure) on a fetch/decode error; `inspect`
/// is a thin wrapper that always prints `out`, then propagates the result.
///
/// A multi-part HEAD (epic EH multi-part fold) carries more than one snapshot
/// part, each covering a disjoint `[min_hour, watermark_hour]` hour range
/// (docs/catalog-and-mvcc.md, ADR-0063). The report lists every part with its
/// range and the total part count; a single-part HEAD is the special case of
/// one part whose range starts at hour 0.
pub async fn render_inspect(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
    out: &mut String,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();
    let key = head_key(&tenant_hash, signal.to_signal());

    let head_bytes = match store.get(&key, GetRange::Full).await {
        Ok(outcome) => outcome.data,
        Err(err) => {
            out.push_str(&format!("HEAD at {key} is absent or unreadable: {err}\n"));
            return Ok(());
        }
    };
    let head = ravel_catalog::decode_head(&head_bytes)
        .map_err(|err| anyhow::anyhow!("HEAD at {key} is corrupt: {err}"))?;

    out.push_str(&format!("format_version: {}\n", head.format_version));
    out.push_str(&format!(
        "tenant_hash: {}\n",
        hex::encode(&head.tenant_hash)
    ));
    // Both the word and the numeric proto value: the word is what an operator
    // reads, the number is what is actually on the object, and the two differing
    // from the `--signal` that was asked for is itself the finding.
    out.push_str(&format!(
        "signal: {} ({})\n",
        head_signal_word(head.signal),
        head.signal
    ));
    out.push_str(&format!("shard_count: {}\n", head.shard_count));
    out.push_str(&format!("watermark_hour: {}\n", head.watermark_hour));
    out.push_str(&format!(
        "folder_id: {}\n",
        format_uuid_bytes(&head.folder_id)
    ));
    out.push_str(&format!("created_unix_ns: {}\n", head.created_unix_ns));
    out.push_str(&format!("parts: {}\n", head.parts.len()));

    let limits = PartLimits::default();
    for (index, part_ref) in head.parts.iter().enumerate() {
        // The hour range is read from the HEAD-level ref (its `min_hour` is
        // mirrored from the covering part header, ADR-0063), so the range is
        // reported even before the part object is fetched and decoded.
        out.push_str(&format!(
            "  part[{index}] range={}..={} key={} blake3={} size={} entry_count={}\n",
            part_ref.min_hour,
            part_ref.watermark_hour,
            part_ref.key,
            hex::encode(&part_ref.blake3),
            part_ref.size,
            part_ref.entry_count
        ));
        let got = store
            .get(&part_ref.key, GetRange::Full)
            .await
            .map_err(|err| anyhow::anyhow!("failed to fetch part {}: {err}", part_ref.key))?;
        let decoded = ravel_catalog::decode_part(&got.data, &limits)
            .map_err(|err| anyhow::anyhow!("part {} is corrupt: {err}", part_ref.key))?;
        out.push_str(&format!(
            "    header: format_version={} min_hour={} watermark_hour={} shard_count={} entry_count={} entries_uncompressed_len={}\n",
            decoded.header.format_version,
            decoded.header.min_hour,
            decoded.header.watermark_hour,
            decoded.header.shard_count,
            decoded.header.entry_count,
            decoded.header.entries_uncompressed_len
        ));
        out.push_str(&format!(
            "    entries (decoded): {}\n",
            decoded.entries.len()
        ));
    }
    Ok(())
}

/// The `--signal` spelling of a [`Signal`], so every report names the signal
/// with the same word the flag accepts.
fn signal_word(signal: Signal) -> &'static str {
    match signal {
        Signal::Metrics => "metrics",
        Signal::Logs => "logs",
        Signal::Spans => "spans",
        Signal::Profiles => "profiles",
        Signal::Alerts => "alerts",
        Signal::Audit => "audit",
    }
}

/// The same word for a `SnapshotHead.signal` field as read off the object,
/// which is a raw proto enum value and so can be a value no signal maps to.
fn head_signal_word(raw: u32) -> &'static str {
    match i32::try_from(raw)
        .ok()
        .and_then(|value| ravel_commit::signal::from_proto(value).ok())
    {
        Some(signal) => signal_word(signal),
        None => "unknown",
    }
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
/// commit record after it has been folded produces exactly this shape and is
/// expected, not a divergence.
pub async fn verify(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    signal: SignalArg,
) -> anyhow::Result<()> {
    let tenant_hash = TenantId::new(tenant).hash();

    // The comparison itself lives in `ravel-catalog` (ADR-0059 decision 2) so
    // the scheduled scrubber and this CLI share one implementation. This
    // command keeps its own presentation: the `println!` report and the nonzero
    // exit (via `anyhow::bail!`) on a real divergence.
    let report = match ravel_catalog::verify_seal_divergence(
        store.as_ref(),
        &tenant_hash,
        signal.to_signal(),
    )
    .await
    .map_err(|err| anyhow::anyhow!("{err}"))?
    {
        Some(report) => report,
        None => {
            let key = head_key(&tenant_hash, signal.to_signal());
            println!("no HEAD found at {key}; nothing folded yet, nothing to verify");
            return Ok(());
        }
    };

    println!("signal: {}", signal_word(signal.to_signal()));
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
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_proto::catalog::v1::{SnapshotHead, SnapshotPartRef};

    const NS_PER_HOUR: i64 = 3_600_000_000_000;

    /// Multi-part fold (epic EH): a tenant's HEAD can carry more than one
    /// snapshot part, each covering a disjoint `[min_hour, watermark_hour]`
    /// hour range. `catalog inspect` must list every part with its range and
    /// the correct total count, not assume a single part.
    #[tokio::test]
    async fn inspect_lists_every_part_range_of_a_multi_part_head() {
        let store = Arc::new(MemoryStore::new());
        let tenant = "cli-inspect-multipart";
        let tenant_hash = TenantId::new(tenant).hash();

        // Three disjoint, ascending hour ranges. The head-level ref carries the
        // range (mirrored from each covering part header, ADR-0063); the part
        // object need only decode, so an empty entry set at the matching
        // watermark is enough.
        let ranges = [(0u32, 5u32), (6, 11), (12, 20)];
        let mut parts = Vec::new();
        for (index, (min_hour, watermark_hour)) in ranges.iter().enumerate() {
            let part_bytes = ravel_catalog::encode_part(tenant_hash.0, 0, 1, *watermark_hour, &[])
                .expect("encode part");
            let key = format!("t/{}/catalog/m/snap/part{index}.rcs", tenant_hash.to_hex());
            store
                .put(
                    &key,
                    part_bytes.clone().into(),
                    PutOptions::create_if_absent(),
                )
                .await
                .expect("put part");
            parts.push(SnapshotPartRef {
                key,
                // inspect never verifies the ref blake3 against the fetched
                // bytes; any 32-byte value satisfies `decode_head`.
                blake3: vec![index as u8; 32],
                size: part_bytes.len() as u64,
                entry_count: 0,
                watermark_hour: *watermark_hour,
                min_hour: *min_hour,
            });
        }

        let head = SnapshotHead {
            format_version: ravel_catalog::HEAD_FORMAT_VERSION,
            tenant_hash: tenant_hash.0.to_vec(),
            signal: 0,
            shard_count: 1,
            watermark_hour: 20,
            parts,
            folder_id: vec![0u8; 16],
            created_unix_ns: 0,
            postings: None,
            shard_generation_count: 1,
        };
        let head_bytes = ravel_catalog::encode_head(&head).expect("encode head");
        store
            .put(
                &head_key(&tenant_hash, Signal::Metrics),
                head_bytes.into(),
                PutOptions::default(),
            )
            .await
            .expect("put head");

        let mut report = String::new();
        render_inspect(store.clone(), tenant, SignalArg::Metrics, &mut report)
            .await
            .expect("inspect renders");

        // Total part count.
        assert!(
            report.contains("parts: 3"),
            "report must state the total part count, got:\n{report}"
        );
        // Every part's range, in order.
        assert!(
            report.contains("part[0] range=0..=5"),
            "missing part[0] range, got:\n{report}"
        );
        assert!(
            report.contains("part[1] range=6..=11"),
            "missing part[1] range, got:\n{report}"
        );
        assert!(
            report.contains("part[2] range=12..=20"),
            "missing part[2] range, got:\n{report}"
        );
    }

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

        fold(store.clone(), tenant, 1, SignalArg::Metrics)
            .await
            .expect("cli fold");

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
