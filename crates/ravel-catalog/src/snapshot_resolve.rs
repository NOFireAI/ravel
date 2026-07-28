//! Snapshot-backed resolve (docs/metric-index-plan.md 5.1/5.3, ADR-0020).
//!
//! Reads HEAD through a TTL cache and its parts through an immutable
//! per-key cache, serves window hours at or below the watermark from part
//! entries, and leaves hours above the watermark to Phase 1 listing. Every
//! failure (HEAD absent or corrupt, part missing, hash mismatch, decode
//! error, shard_count mismatch aside) degrades to `Ok(None)`, telling the
//! caller to fall back to full listing: this module can only ever make a
//! query faster, never make it fail or return wrong data.

use std::collections::HashMap;
use std::sync::Arc;

use ravel_commit::{keys, signal};
use ravel_object_store::{GetRange, StoreError};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead};
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::error::CatalogError;
use crate::fold::head_object_key;
use crate::snapshot::SegmentRef;
use crate::snapshot_format::{self, DecodedPart, PartLimits};

/// A usable snapshot: its watermark and every part HEAD named, already
/// verified and decoded.
pub(crate) struct SnapshotWindow {
    pub(crate) watermark_hour: u32,
    parts: Vec<Arc<DecodedPart>>,
}

impl SnapshotWindow {
    /// Extract entries for `[lower_hour, upper_hour]` (inclusive) from every
    /// part, filtered by event-time overlap with `query_range` exactly as
    /// `Catalog::list_hour_bucket` does, deduped into `out` by data key
    /// (docs/metric-index-plan.md 5.1 step 5). Entries are sorted
    /// hour-major within each part (docs/metric-index-plan.md 3.1), so the
    /// matching hour range is one contiguous slice found by
    /// `partition_point`.
    pub(crate) fn extract_into(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        lower_hour: u32,
        upper_hour: u32,
        query_range: &TimeRange,
        out: &mut HashMap<String, SegmentRef>,
    ) -> Result<(), CatalogError> {
        for part in &self.parts {
            let entries = &part.entries;
            let start = entries.partition_point(|e| e.ingest_hour_bucket < lower_hour);
            let end = entries.partition_point(|e| e.ingest_hour_bucket <= upper_hour);
            for entry in &entries[start..end] {
                let event_range = TimeRange {
                    start_ns: entry.min_event_ts_ns,
                    end_ns: entry.max_event_ts_ns,
                };
                if !event_range.overlaps(query_range) {
                    continue;
                }
                let segment_ref = build_segment_ref_from_entry(tenant, signal, entry)?;
                out.entry(segment_ref.data_object_key.clone())
                    .or_insert(segment_ref);
            }
        }
        Ok(())
    }
}

/// Outcome of loading every part a HEAD names.
enum PartLoadOutcome {
    Loaded(Vec<Arc<DecodedPart>>),
    /// A part GET returned `NotFound`: races GC of a just-superseded part
    /// (docs/metric-index-plan.md 5.1 step 2). The caller re-reads HEAD
    /// once and retries before falling back.
    NotFoundRace,
    Unusable,
}

impl Catalog {
    /// Resolve the current snapshot window for (tenant, signal), or `None`
    /// if no snapshot is usable right now (absent, corrupt, or its parts
    /// unreadable). Never returns an error for index-only failures: the
    /// index is a pure optimization (docs/metric-index-plan.md 5.1 step 2).
    pub(crate) async fn resolve_snapshot_window(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        now_ns: i64,
    ) -> Result<Option<SnapshotWindow>, CatalogError> {
        let head_key = head_object_key(tenant, signal);
        let Some(head) = self
            .read_head(tenant, signal, &head_key, now_ns, false)
            .await?
        else {
            return Ok(None);
        };
        match self.load_snapshot_parts(tenant, &head).await {
            PartLoadOutcome::Loaded(parts) => Ok(Some(SnapshotWindow {
                watermark_hour: head.watermark_hour,
                parts,
            })),
            PartLoadOutcome::Unusable => Ok(None),
            PartLoadOutcome::NotFoundRace => {
                // At most one HEAD re-read (docs/metric-index-plan.md 5.1
                // step 2): bypass the TTL cache so a part GC'd since the
                // cached HEAD was read is not raced again.
                let Some(fresh_head) = self
                    .read_head(tenant, signal, &head_key, now_ns, true)
                    .await?
                else {
                    return Ok(None);
                };
                match self.load_snapshot_parts(tenant, &fresh_head).await {
                    PartLoadOutcome::Loaded(parts) => Ok(Some(SnapshotWindow {
                        watermark_hour: fresh_head.watermark_hour,
                        parts,
                    })),
                    PartLoadOutcome::Unusable | PartLoadOutcome::NotFoundRace => Ok(None),
                }
            }
        }
    }

    /// Read HEAD, through the TTL cache unless `bypass_cache`. Any failure
    /// short of a `shard_count` mismatch is logged and folded into `None`
    /// (fall back to listing); a `shard_count` mismatch is the one loud
    /// error (docs/metric-index-plan.md 5.1 step 1: "shard_count mismatch
    /// is a loud/hard error"), since it means this catalog's own config
    /// disagrees with the index it is about to trust.
    async fn read_head(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        head_key: &str,
        now_ns: i64,
        bypass_cache: bool,
    ) -> Result<Option<Arc<SnapshotHead>>, CatalogError> {
        if !bypass_cache
            && let Some(cached) =
                self.head_cache()
                    .get(tenant, signal, now_ns, self.config().head_cache_ttl_ns)
        {
            return Ok(Some(cached));
        }

        let got = match self.store().get(head_key, GetRange::Full).await {
            Ok(got) => got,
            Err(StoreError::NotFound) => return Ok(None),
            Err(err) => {
                tracing::warn!(error = %err, key = %head_key, "HEAD GET failed, falling back to listing");
                return Ok(None);
            }
        };
        let head = match snapshot_format::decode_head(&got.data) {
            Ok(head) => head,
            Err(err) => {
                tracing::warn!(error = %err, key = %head_key, "HEAD failed to decode, falling back to listing");
                return Ok(None);
            }
        };
        if head.tenant_hash.as_slice() != tenant.0.as_slice() {
            tracing::warn!(key = %head_key, "HEAD tenant_hash mismatch, falling back to listing");
            return Ok(None);
        }
        if signal::from_proto(head.signal as i32) != Ok(signal) {
            tracing::warn!(key = %head_key, "HEAD signal mismatch, falling back to listing");
            return Ok(None);
        }
        if head.shard_count != self.config().shard_count {
            return Err(CatalogError::FieldMismatch {
                key: head_key.to_string(),
                field: "shard_count",
                expected: self.config().shard_count.to_string(),
                actual: head.shard_count.to_string(),
            });
        }

        let head = Arc::new(head);
        self.head_cache()
            .insert(*tenant, signal, head.clone(), now_ns);
        Ok(Some(head))
    }

    /// Load and verify every part HEAD names, through the immutable part
    /// cache. Parts are content-addressed, so a cache hit needs no
    /// re-verification.
    async fn load_snapshot_parts(
        &self,
        tenant: &TenantHash,
        head: &SnapshotHead,
    ) -> PartLoadOutcome {
        let mut parts = Vec::with_capacity(head.parts.len());
        for part_ref in &head.parts {
            if let Some(cached) = self.part_cache().get(tenant, &part_ref.key) {
                parts.push(cached);
                continue;
            }
            let got = match self.store().get(&part_ref.key, GetRange::Full).await {
                Ok(got) => got,
                Err(StoreError::NotFound) => {
                    tracing::warn!(key = %part_ref.key, "snapshot part not found, will re-read HEAD once");
                    return PartLoadOutcome::NotFoundRace;
                }
                Err(err) => {
                    tracing::warn!(error = %err, key = %part_ref.key, "snapshot part GET failed, falling back to listing");
                    return PartLoadOutcome::Unusable;
                }
            };
            let digest = blake3::hash(&got.data);
            if digest.as_bytes().as_slice() != part_ref.blake3.as_slice() {
                tracing::warn!(key = %part_ref.key, "snapshot part hash mismatch, falling back to listing");
                return PartLoadOutcome::Unusable;
            }
            let limits = PartLimits {
                max_snapshot_part_bytes: self.config().max_snapshot_part_bytes,
            };
            let decoded = match snapshot_format::decode_part(&got.data, &limits) {
                Ok(decoded) => Arc::new(decoded),
                Err(err) => {
                    tracing::warn!(error = %err, key = %part_ref.key, "snapshot part failed to decode, falling back to listing");
                    return PartLoadOutcome::Unusable;
                }
            };
            self.part_cache().insert(
                *tenant,
                part_ref.key.clone(),
                decoded.clone(),
                self.config().snapshot_cache_parts,
            );
            parts.push(decoded);
        }
        PartLoadOutcome::Loaded(parts)
    }
}

fn build_segment_ref_from_entry(
    tenant: &TenantHash,
    signal: Signal,
    entry: &SnapshotEntry,
) -> Result<SegmentRef, CatalogError> {
    let content_hash: [u8; 32] =
        entry
            .content_hash
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: format!(
                    "snapshot entry (shard {}, hour {})",
                    entry.shard, entry.ingest_hour_bucket
                ),
                field: "content_hash",
                expected: "32 bytes".to_string(),
                actual: format!("{} bytes", entry.content_hash.len()),
            })?;
    let writer_id_bytes: [u8; 16] =
        entry
            .writer_id
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: format!(
                    "snapshot entry (shard {}, hour {})",
                    entry.shard, entry.ingest_hour_bucket
                ),
                field: "writer_id",
                expected: "16 bytes".to_string(),
                actual: format!("{} bytes", entry.writer_id.len()),
            })?;
    let writer_id = Uuid::from_bytes(writer_id_bytes);
    let data_object_key = keys::data_key(
        tenant,
        signal,
        entry.shard,
        writer_id,
        entry.writer_epoch,
        entry.writer_seq,
        &content_hash,
    )?;
    Ok(SegmentRef {
        data_object_key,
        object_size: entry.object_size,
        min_event_ts_ns: entry.min_event_ts_ns,
        max_event_ts_ns: entry.max_event_ts_ns,
        ingest_hour_bucket: entry.ingest_hour_bucket,
        sample_count: entry.sample_count,
        series_count: entry.series_count,
        shard: entry.shard,
        content_hash,
        writer_id,
        writer_epoch: entry.writer_epoch,
        writer_seq: entry.writer_seq,
        created_unix_ns: entry.created_unix_ns,
    })
}
