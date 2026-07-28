//! Sealed-bucket scan with the advisory CAS cursor
//! (docs/compaction-retention-plan.md §3.2, ADR-0018). Walks the hours of one
//! `(tenant, signal, shard)` upward from the cursor, compacting every sealed,
//! eligible bucket, and advances the cursor past the buckets it finished. The
//! cursor is advisory mutable state (the ADR-0003 HEAD-pointer precedent):
//! losing or corrupting it costs a rescan, never correctness.

use ravel_commit::keys;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, Version};
use ravel_types::{Signal, TenantHash};

use crate::bucket::Bucket;
use crate::clock::Clock;
use crate::compact::{CompactionOutcome, compact_bucket};
use crate::config::CompactorConfig;
use crate::error::{MaintainError, Result};

/// One-byte version tag on the advisory cursor payload. The cursor is not a
/// frozen format; the tag only lets a future encoding change be detected and
/// treated as "no usable cursor" (rescan), never misread.
const CURSOR_TAG: u8 = 1;

/// Outcome of one scan pass over a `(tenant, signal, shard)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanReport {
    /// Buckets newly compacted this pass.
    pub compacted: usize,
    /// Sealed buckets found already done (already-compacted, tombstoned, or
    /// below the input threshold).
    pub already_done: usize,
    /// Buckets skipped because they are not yet sealed (scan stops there).
    pub not_sealed: usize,
    /// The hour the cursor was advanced to, if it moved.
    pub cursor_advanced_to: Option<u32>,
}

/// Scan and compact every eligible sealed bucket for one `(tenant, signal,
/// shard)`, then advance the advisory cursor. Idempotent: re-running after a
/// crash reprocesses at most the buckets past the last persisted cursor, and
/// each bucket's own idempotency (plan §3.4) makes reprocessing harmless.
pub async fn scan_and_compact(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    tenant_hash: TenantHash,
    signal: Signal,
    shard: u32,
) -> Result<ScanReport> {
    let cursor_key = keys::maint_cursor_key(&tenant_hash, signal, shard)?;
    let cursor = read_cursor(store, &cursor_key).await?;
    let start_after = cursor.as_ref().map(|(hour, _)| *hour);

    let shard_prefix = keys::commit_shard_prefix(&tenant_hash, signal, shard)?;
    let listed = store.list_delimited(&shard_prefix).await?;
    let mut hours: Vec<u32> = Vec::new();
    for common in &listed.common_prefixes {
        // common == "<shard_prefix><hour>/"; extract the hour segment.
        let rest = common
            .strip_prefix(&shard_prefix)
            .and_then(|r| r.strip_suffix('/'))
            .unwrap_or("");
        match keys::parse_ingest_hour_string(rest) {
            Ok(hour) => hours.push(hour),
            // A non-hour common prefix under the shard is layout drift.
            Err(e) => return Err(MaintainError::Key(e)),
        }
    }
    hours.sort_unstable();

    let mut report = ScanReport {
        compacted: 0,
        already_done: 0,
        not_sealed: 0,
        cursor_advanced_to: None,
    };
    let mut highest_done: Option<u32> = None;

    for hour in hours {
        if start_after.is_some_and(|after| hour <= after) {
            continue;
        }
        let bucket = Bucket::new(tenant_hash, signal, shard, hour);
        match compact_bucket(store, clock, config, &bucket).await? {
            CompactionOutcome::NotSealed => {
                // Hours are ascending, so every later bucket is also unsealed.
                report.not_sealed += 1;
                break;
            }
            CompactionOutcome::Compacted { .. } => {
                report.compacted += 1;
                highest_done = Some(hour);
            }
            CompactionOutcome::AlreadyCompacted
            | CompactionOutcome::Tombstoned
            | CompactionOutcome::BelowMinInputs { .. } => {
                report.already_done += 1;
                highest_done = Some(hour);
            }
        }
    }

    if let Some(hour) = highest_done {
        // Advisory: a lost CAS race just means another maintainer already
        // moved the cursor, so treat AlreadyExists/PreconditionFailed as fine.
        write_cursor(store, &cursor_key, hour, cursor.map(|(_, v)| v)).await?;
        report.cursor_advanced_to = Some(hour);
    }

    Ok(report)
}

/// Read the advisory cursor. Returns the recorded hour and the object version
/// (for the next CAS). A decode failure is treated as no cursor (advisory
/// rescan), never an error; store faults propagate.
async fn read_cursor(store: &dyn ObjectStoreBackend, key: &str) -> Result<Option<(u32, Version)>> {
    match store.get(key, GetRange::Full).await {
        Ok(got) => {
            let data = got.data;
            if data.len() == 5 && data[0] == CURSOR_TAG {
                let hour = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                Ok(Some((hour, got.version)))
            } else {
                // Unrecognized cursor payload: ignore it (rescan from zero),
                // but keep the version so we can CAS-overwrite it cleanly.
                Ok(Some((0, got.version)))
            }
        }
        Err(StoreError::NotFound) => Ok(None),
        Err(e) => Err(MaintainError::Store(e)),
    }
}

/// Write the advisory cursor. First write is `CreateIfAbsent`; subsequent
/// writes CAS against the version we read. A lost race
/// (`AlreadyExists`/`PreconditionFailed`) is not an error: the cursor is
/// advisory and another maintainer's update is equally valid.
async fn write_cursor(
    store: &dyn ObjectStoreBackend,
    key: &str,
    hour: u32,
    prev_version: Option<Version>,
) -> Result<()> {
    let mut payload = Vec::with_capacity(5);
    payload.push(CURSOR_TAG);
    payload.extend_from_slice(&hour.to_le_bytes());
    let mode = match prev_version {
        Some(v) => PutMode::CasVersion(v),
        None => PutMode::CreateIfAbsent,
    };
    let opts = PutOptions {
        mode,
        checksum: Some(ravel_object_store::UploadChecksum::Crc32c(crc32c::crc32c(
            &payload,
        ))),
    };
    match store.put(key, payload.into(), opts).await {
        Ok(_) | Err(StoreError::AlreadyExists) | Err(StoreError::PreconditionFailed) => Ok(()),
        Err(e) => Err(MaintainError::Store(e)),
    }
}
