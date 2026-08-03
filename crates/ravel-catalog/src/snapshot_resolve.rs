//! Snapshot-backed resolve (docs/metric-index-plan.md 5.1/5.3, ADR-0020).
//!
//! Reads HEAD through a TTL cache and its parts through an immutable
//! per-key cache, serves window hours at or below the watermark from part
//! entries, and leaves hours above the watermark to Phase 1 listing. Every
//! failure (HEAD absent or corrupt, part missing, hash mismatch, decode
//! error) degrades to `Ok(None)`, telling the caller to fall back to full
//! listing: this module can only ever make a query faster, never make it
//! return wrong data. Two mismatches are the loud exception, surfaced as a
//! hard `CatalogError::FieldMismatch` instead of a degrade: `shard_count`
//! (this catalog's own config disagrees with the index it is about to
//! trust), and, per ADR-0050 §2, `tenant_hash` on the HEAD or a postings
//! object (an isolation breach, never silently absorbed into a fallback
//! that could serve or reference foreign bytes).

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use ravel_commit::{keys, signal};
use ravel_object_store::{GetRange, StoreError};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::error::CatalogError;
use crate::fold::head_object_key;
use crate::snapshot::{SegmentLevel, SegmentRef};
use crate::snapshot_format::{self, DecodedPart, DecodedPostings, PartLimits, PostingsLimits};

/// A usable snapshot: its watermark and every part HEAD named, already
/// verified and decoded.
pub(crate) struct SnapshotWindow {
    pub(crate) watermark_hour: u32,
    parts: Vec<Arc<DecodedPart>>,
    /// Decoded, part-bound name postings (P5b, docs/metric-index-plan.md
    /// 5.4), or `None` when postings are absent, unreadable, corrupt, or
    /// don't cleanly bind to `parts`. Always safe to treat as absent:
    /// postings are a pure pruning optimization, never a correctness
    /// dependency (`extract_into` falls back to including every entry).
    postings: Option<Arc<DecodedPostings>>,
}

impl SnapshotWindow {
    /// Extract entries for `[lower_hour, upper_hour]` (inclusive) from every
    /// part, filtered by event-time overlap with `query_range` exactly as
    /// `Catalog::list_hour_bucket` does, deduped into `out` by data key
    /// (docs/metric-index-plan.md 5.1 step 5). Entries are sorted
    /// hour-major within each part (docs/metric-index-plan.md 3.1), so the
    /// matching hour range is one contiguous slice found by
    /// `partition_point`.
    ///
    /// `name_filter`, when `Some`, is the query's equality `__name__` value
    /// (P5b): entries this snapshot's postings provably do not carry that
    /// name are skipped before the event-overlap check, and `*pruned` is
    /// incremented once per skipped entry. Pruning only ever activates when
    /// postings are present, decoded, and bound to exactly the parts this
    /// window holds; any other case (`name_filter` is `None`, postings
    /// absent/corrupt, or more than one covered part, which today's
    /// single-part fold never produces but a future compaction phase might)
    /// falls back to considering every entry, exactly as `Catalog::resolve`
    /// always has. This can only ever narrow the result set matched by
    /// `query_range`, never widen it: exact semantics by default.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn extract_into(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        lower_hour: u32,
        upper_hour: u32,
        query_range: &TimeRange,
        name_filter: Option<&str>,
        pruned: &mut u64,
        out: &mut HashMap<String, SegmentRef>,
    ) -> Result<(), CatalogError> {
        let ordinals = self.postings_ordinals_for(name_filter);
        for part in &self.parts {
            let entries = &part.entries;
            let start = entries.partition_point(|e| e.ingest_hour_bucket < lower_hour);
            let end = entries.partition_point(|e| e.ingest_hour_bucket <= upper_hour);
            match ordinals {
                None => {
                    for entry in &entries[start..end] {
                        self.maybe_insert(tenant, signal, entry, query_range, out)?;
                    }
                }
                Some(ords) => {
                    let lo = ords.partition_point(|&o| (o as usize) < start);
                    let hi = ords.partition_point(|&o| (o as usize) < end);
                    *pruned += ((end - start) - (hi - lo)) as u64;
                    for &ordinal in &ords[lo..hi] {
                        self.maybe_insert(
                            tenant,
                            signal,
                            &entries[ordinal as usize],
                            query_range,
                            out,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn maybe_insert(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        entry: &SnapshotEntry,
        query_range: &TimeRange,
        out: &mut HashMap<String, SegmentRef>,
    ) -> Result<(), CatalogError> {
        let event_range = TimeRange {
            start_ns: entry.min_event_ts_ns,
            end_ns: entry.max_event_ts_ns,
        };
        if !event_range.overlaps(query_range) {
            return Ok(());
        }
        let segment_ref = build_segment_ref_from_entry(tenant, signal, entry)?;
        out.entry(segment_ref.data_object_key.clone())
            .or_insert(segment_ref);
        Ok(())
    }

    /// Resolves `name_filter` to the sorted ordinal list of entries carrying
    /// that name, or `None` if pruning cannot safely apply (no filter, no
    /// usable postings, or more than one covered part). `Some(&[])` is a
    /// legitimate result: the name simply does not appear in this snapshot
    /// at all, so every candidate entry is pruned.
    fn postings_ordinals_for(&self, name_filter: Option<&str>) -> Option<&[u64]> {
        let name = name_filter?;
        if self.parts.len() != 1 {
            return None;
        }
        let postings = self.postings.as_ref()?;
        match postings
            .names
            .binary_search_by(|np| np.name.as_str().cmp(name))
        {
            Ok(idx) => Some(postings.names[idx].ordinals.as_slice()),
            Err(_) => Some(&[]),
        }
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
    ///
    /// `want_postings` gates the postings GET (#278 item 3): the postings
    /// object is only ever consulted to prune by an equality `__name__`
    /// filter, so a resolve with no such filter passes `false` and never
    /// fetches or decodes it. Passing `false` is equivalent to postings being
    /// absent, which `extract_into` already handles by considering every
    /// entry.
    pub(crate) async fn resolve_snapshot_window(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        now_ns: i64,
        want_postings: bool,
        accounting: &QueryAccounting,
    ) -> Result<Option<SnapshotWindow>, CatalogError> {
        let head_key = head_object_key(tenant, signal);
        let Some(head) = self
            .read_head(tenant, signal, &head_key, now_ns, false, accounting)
            .await?
        else {
            return Ok(None);
        };
        match self.load_snapshot_parts(tenant, &head, accounting).await {
            PartLoadOutcome::Loaded(parts) => {
                let postings = if want_postings {
                    self.load_snapshot_postings(tenant, &head, &parts, accounting)
                        .await?
                } else {
                    None
                };
                Ok(Some(SnapshotWindow {
                    watermark_hour: head.watermark_hour,
                    parts,
                    postings,
                }))
            }
            PartLoadOutcome::Unusable => Ok(None),
            PartLoadOutcome::NotFoundRace => {
                // At most one HEAD re-read (docs/metric-index-plan.md 5.1
                // step 2): bypass the TTL cache so a part GC'd since the
                // cached HEAD was read is not raced again.
                let Some(fresh_head) = self
                    .read_head(tenant, signal, &head_key, now_ns, true, accounting)
                    .await?
                else {
                    return Ok(None);
                };
                match self
                    .load_snapshot_parts(tenant, &fresh_head, accounting)
                    .await
                {
                    PartLoadOutcome::Loaded(parts) => {
                        let postings = if want_postings {
                            self.load_snapshot_postings(tenant, &fresh_head, &parts, accounting)
                                .await?
                        } else {
                            None
                        };
                        Ok(Some(SnapshotWindow {
                            watermark_hour: fresh_head.watermark_hour,
                            parts,
                            postings,
                        }))
                    }
                    PartLoadOutcome::Unusable | PartLoadOutcome::NotFoundRace => Ok(None),
                }
            }
        }
    }

    /// Load and verify this HEAD's name postings through the immutable
    /// postings cache (P5b, docs/metric-index-plan.md 5.4). Every failure
    /// mode short of a `tenant_hash` mismatch (no postings ref, GET error,
    /// hash mismatch, decode error, part-binding mismatch, entry-count
    /// mismatch) degrades to `Ok(None)`: postings are a pure pruning
    /// optimization, never surfaced as an error and never allowed to make
    /// the snapshot window itself unusable. A `tenant_hash` mismatch is the
    /// one loud exception (ADR-0050 §2): a postings object naming a
    /// different tenant is an isolation breach, not a degrade-and-continue
    /// case, so it is a hard `CatalogError::FieldMismatch` with no fallback.
    async fn load_snapshot_postings(
        &self,
        tenant: &TenantHash,
        head: &SnapshotHead,
        parts: &[Arc<DecodedPart>],
        accounting: &QueryAccounting,
    ) -> Result<Option<Arc<DecodedPostings>>, CatalogError> {
        let Some(postings_ref) = head.postings.as_ref() else {
            return Ok(None);
        };
        let Ok(expected_part_blake3) = head
            .parts
            .iter()
            .map(|p| <[u8; 32]>::try_from(p.blake3.as_slice()))
            .collect::<Result<Vec<[u8; 32]>, _>>()
        else {
            return Ok(None);
        };

        if let Some(cached) = self
            .postings_cache()
            .get(tenant, &postings_ref.key, accounting)
        {
            return Ok(Some(cached));
        }

        let data = match self
            .fetch_content_addressed(
                tenant,
                &postings_ref.key,
                &postings_ref.blake3,
                postings_ref.size,
                accounting,
            )
            .await
        {
            Ok(data) => data,
            Err(err) => {
                tracing::warn!(error = %err, key = %postings_ref.key, "postings GET failed, pruning disabled");
                return Ok(None);
            }
        };
        let digest = blake3::hash(&data);
        if digest.as_bytes().as_slice() != postings_ref.blake3.as_slice() {
            tracing::warn!(key = %postings_ref.key, "postings hash mismatch, pruning disabled");
            return Ok(None);
        }
        let limits = PostingsLimits {
            max_postings_bytes: self.config().max_postings_bytes,
        };
        let decoded = match snapshot_format::decode_postings(&data, &limits, &expected_part_blake3)
        {
            Ok(decoded) => decoded,
            Err(err) => {
                tracing::warn!(error = %err, key = %postings_ref.key, "postings failed to decode, pruning disabled");
                return Ok(None);
            }
        };
        if decoded.header.tenant_hash.as_slice() != tenant.0.as_slice() {
            self.record_isolation_breach();
            return Err(CatalogError::FieldMismatch {
                key: postings_ref.key.clone(),
                field: "tenant_hash",
                expected: tenant.to_hex(),
                actual: hex::encode(&decoded.header.tenant_hash),
            });
        }
        let total_entries: u64 = parts.iter().map(|p| p.entries.len() as u64).sum();
        if decoded.header.entry_count != total_entries {
            tracing::warn!(key = %postings_ref.key, "postings entry_count mismatch, pruning disabled");
            return Ok(None);
        }

        let decoded = Arc::new(decoded);
        self.postings_cache().insert(
            *tenant,
            postings_ref.key.clone(),
            decoded.clone(),
            data.len() as u64,
            self.config().postings_cache_entries,
        );
        Ok(Some(decoded))
    }

    /// Read HEAD, through the TTL cache unless `bypass_cache`. Any failure
    /// short of a `shard_count` or `tenant_hash` mismatch is logged and
    /// folded into `None` (fall back to listing). A `shard_count` mismatch
    /// is a loud error (docs/metric-index-plan.md 5.1 step 1: "shard_count
    /// mismatch is a loud/hard error"), since it means this catalog's own
    /// config disagrees with the index it is about to trust. A
    /// `tenant_hash` mismatch is likewise loud (ADR-0050 §2): a HEAD naming
    /// a different tenant is an isolation breach, so it hard-fails instead
    /// of silently falling back to a listing pass that could still be
    /// influenced by the wrong index.
    #[allow(clippy::too_many_arguments)]
    async fn read_head(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        head_key: &str,
        now_ns: i64,
        bypass_cache: bool,
        accounting: &QueryAccounting,
    ) -> Result<Option<Arc<SnapshotHead>>, CatalogError> {
        if !bypass_cache
            && let Some(cached) = self.head_cache().get(
                tenant,
                signal,
                now_ns,
                self.config().head_cache_ttl_ns,
                accounting,
            )
        {
            return Ok(Some(cached));
        }

        let got = match self.guarded_get(head_key, GetRange::Full, accounting).await {
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
            self.record_isolation_breach();
            return Err(CatalogError::FieldMismatch {
                key: head_key.to_string(),
                field: "tenant_hash",
                expected: tenant.to_hex(),
                actual: hex::encode(&head.tenant_hash),
            });
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

        let bytes = got.data.len() as u64;
        let head = Arc::new(head);
        self.head_cache().insert(
            *tenant,
            signal,
            head.clone(),
            bytes,
            now_ns,
            self.config().head_cache_capacity,
        );
        Ok(Some(head))
    }

    /// Load and verify every part HEAD names, through the immutable part
    /// cache. Parts are content-addressed, so a cache hit needs no
    /// re-verification.
    ///
    /// The uncached part GETs run concurrently under the resolve-wide
    /// semaphore (#278 item 2) rather than one await at a time. `buffered`
    /// preserves HEAD's part order, and the fold returns the first
    /// non-`Loaded` outcome in that order, so a multi-part snapshot yields
    /// exactly the same `NotFoundRace`/`Unusable` decision the sequential
    /// loop did.
    async fn load_snapshot_parts(
        &self,
        tenant: &TenantHash,
        head: &SnapshotHead,
        accounting: &QueryAccounting,
    ) -> PartLoadOutcome {
        // Owned part-ref clones, not borrowed `&SnapshotPartRef`s: a stream
        // closure that borrows each item infers a non-higher-ranked lifetime
        // for the future and fails to unify with axum's `Handler` blanket
        // impl at the HTTP router (the "FnOnce is not general enough" wall).
        let loaded: Vec<OnePartOutcome> = stream::iter(head.parts.iter().cloned())
            .map(|part_ref| async move { self.load_one_part(tenant, &part_ref, accounting).await })
            .buffered(crate::catalog::MAX_CONCURRENT_REQUESTS)
            .collect()
            .await;
        let mut parts = Vec::with_capacity(loaded.len());
        for outcome in loaded {
            match outcome {
                OnePartOutcome::Loaded(part) => parts.push(part),
                OnePartOutcome::NotFoundRace => return PartLoadOutcome::NotFoundRace,
                OnePartOutcome::Unusable => return PartLoadOutcome::Unusable,
            }
        }
        PartLoadOutcome::Loaded(parts)
    }

    /// Load, verify, and decode one snapshot part through the immutable part
    /// cache. The per-part half of [`load_snapshot_parts`](Self::load_snapshot_parts),
    /// factored out so the parts can be fetched concurrently (#278 item 2).
    async fn load_one_part(
        &self,
        tenant: &TenantHash,
        part_ref: &ravel_proto::catalog::v1::SnapshotPartRef,
        accounting: &QueryAccounting,
    ) -> OnePartOutcome {
        if let Some(cached) = self.part_cache().get(tenant, &part_ref.key, accounting) {
            return OnePartOutcome::Loaded(cached);
        }
        let data = match self
            .fetch_content_addressed(
                tenant,
                &part_ref.key,
                &part_ref.blake3,
                part_ref.size,
                accounting,
            )
            .await
        {
            Ok(data) => data,
            Err(StoreError::NotFound) => {
                tracing::warn!(key = %part_ref.key, "snapshot part not found, will re-read HEAD once");
                return OnePartOutcome::NotFoundRace;
            }
            Err(err) => {
                tracing::warn!(error = %err, key = %part_ref.key, "snapshot part GET failed, falling back to listing");
                return OnePartOutcome::Unusable;
            }
        };
        let digest = blake3::hash(&data);
        if digest.as_bytes().as_slice() != part_ref.blake3.as_slice() {
            tracing::warn!(key = %part_ref.key, "snapshot part hash mismatch, falling back to listing");
            return OnePartOutcome::Unusable;
        }
        let limits = PartLimits {
            max_snapshot_part_bytes: self.config().max_snapshot_part_bytes,
        };
        let decoded = match snapshot_format::decode_part(&data, &limits) {
            Ok(decoded) => Arc::new(decoded),
            Err(err) => {
                tracing::warn!(error = %err, key = %part_ref.key, "snapshot part failed to decode, falling back to listing");
                return OnePartOutcome::Unusable;
            }
        };
        self.part_cache().insert(
            *tenant,
            part_ref.key.clone(),
            decoded.clone(),
            data.len() as u64,
            self.config().snapshot_cache_parts,
        );
        OnePartOutcome::Loaded(decoded)
    }
}

/// One part's load result, folded back into a [`PartLoadOutcome`] over the
/// whole part set in HEAD order.
enum OnePartOutcome {
    Loaded(Arc<DecodedPart>),
    NotFoundRace,
    Unusable,
}

fn build_segment_ref_from_entry(
    tenant: &TenantHash,
    signal: Signal,
    entry: &SnapshotEntry,
) -> Result<SegmentRef, CatalogError> {
    let entry_label = || {
        format!(
            "snapshot entry (level {}, shard {}, hour {})",
            entry.level, entry.shard, entry.ingest_hour_bucket
        )
    };
    let content_hash: [u8; 32] =
        entry
            .content_hash
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: entry_label(),
                field: "content_hash",
                expected: "32 bytes".to_string(),
                actual: format!("{} bytes", entry.content_hash.len()),
            })?;

    // Level 1 is a compaction (L1) part. Its identity is carried in the
    // writer_* slots by the fold (`build_l1_snapshot_entry`): writer_id holds
    // the 32-byte input_set_hash, writer_epoch holds the part_index. Rebuild
    // exactly the `SegmentRef` a live listing produces from the compaction
    // record and part (`build_l1_segment_ref`): the L1 data key from
    // `keys::l1_part_key`, and writer_* left nil since an L1 part has no
    // writer identity (docs/metric-index-plan.md section 7).
    if entry.level != 0 {
        let input_set_hash: [u8; 32] =
            entry
                .writer_id
                .clone()
                .try_into()
                .map_err(|_| CatalogError::FieldMismatch {
                    key: entry_label(),
                    field: "writer_id (input_set_hash)",
                    expected: "32 bytes".to_string(),
                    actual: format!("{} bytes", entry.writer_id.len()),
                })?;
        let part_index =
            u32::try_from(entry.writer_epoch).map_err(|_| CatalogError::FieldMismatch {
                key: entry_label(),
                field: "writer_epoch (part_index)",
                expected: "u32".to_string(),
                actual: entry.writer_epoch.to_string(),
            })?;
        let data_object_key = keys::l1_part_key(
            tenant,
            signal,
            entry.shard,
            entry.ingest_hour_bucket,
            &hex16(&input_set_hash),
            part_index,
            &hex16(&content_hash),
        )?;
        return Ok(SegmentRef {
            data_object_key,
            object_size: entry.object_size,
            min_event_ts_ns: entry.min_event_ts_ns,
            max_event_ts_ns: entry.max_event_ts_ns,
            ingest_hour_bucket: entry.ingest_hour_bucket,
            sample_count: entry.sample_count,
            series_count: entry.series_count,
            shard: entry.shard,
            content_hash,
            writer_id: Uuid::nil(),
            writer_epoch: 0,
            writer_seq: 0,
            created_unix_ns: entry.created_unix_ns,
            level: SegmentLevel::L1 {
                input_set_hash,
                part_index,
            },
        });
    }

    let writer_id_bytes: [u8; 16] =
        entry
            .writer_id
            .clone()
            .try_into()
            .map_err(|_| CatalogError::FieldMismatch {
                key: entry_label(),
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
        level: SegmentLevel::L0,
    })
}

/// Lowercase hex of a 32-byte hash's first 8 bytes: the 16-char `hash16`
/// component the L0 data-key and L1 part-key layouts embed
/// (crates/ravel-commit/src/keys.rs), matching `hex::encode(&hash[..8])`.
fn hex16(hash: &[u8; 32]) -> String {
    hash[..8].iter().map(|b| format!("{b:02x}")).collect()
}
