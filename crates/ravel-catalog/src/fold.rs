//! Metric index fold: catalog snapshot construction from commit records
//! (docs/metric-index-plan.md section 4, ADR-0020).
//!
//! Never runs on the ingest or query path. A fold reads the current HEAD,
//! computes the newly sealed watermark, folds previous parts plus newly
//! sealed (shard, hour) buckets into one new content-addressed snapshot
//! part, and CAS-swaps HEAD to name it. Any number of folders may race:
//! parts are content-addressed so a losing PUT is a no-op, and HEAD's CAS
//! precondition serializes the pointer swap. Every failure mode (absent or
//! corrupt HEAD, unreadable previous part, exhausted CAS retries) either
//! falls back to full rebuild from the commit layout or returns a typed
//! error; it never corrupts HEAD or leaves a torn snapshot visible.

use std::collections::HashSet;

use bytes::Bytes;
use ravel_commit::keys::{self, BucketEntry, KeyError};
use ravel_commit::signal;
use ravel_object_store::{
    GetRange, PutMode, PutOptions, StoreError, UploadChecksum, Version, list_all,
};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartRef};
use ravel_proto::commit::v1::CommitRecord;
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::config::CatalogConfig;
use crate::error::CatalogError;
use crate::snapshot_format::{self, HEAD_FORMAT_VERSION, PartLimits};

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Bounded retry budget for HEAD's CAS loop (docs/metric-index-plan.md
/// section 4, step 7): a folder that keeps losing to concurrent folders
/// gives up with [`CatalogError::FoldCasRetriesExhausted`] rather than
/// retrying forever.
const MAX_HEAD_CAS_ATTEMPTS: u32 = 8;

/// Placeholder for future compaction/retention transactions
/// (docs/metric-index-plan.md section 7). `fold`'s entry point accepts a
/// `&[Transaction]` today so a later phase can apply compaction/retention
/// records without a signature change, but this phase never constructs one:
/// no public constructor exists, so callers can only ever pass an empty
/// slice.
#[derive(Debug, Clone)]
pub struct Transaction {
    _private: (),
}

/// Outcome of one [`Catalog::fold`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldReport {
    /// HEAD's watermark_hour after this call. `None` only when no hour has
    /// ever been sealed for this (tenant, signal) yet.
    pub watermark_hour: Option<u32>,
    /// HEAD's watermark_hour before this call.
    pub previous_watermark_hour: Option<u32>,
    /// `true` if nothing was sealed beyond `previous_watermark_hour`: HEAD
    /// was left untouched, no part was written.
    pub no_op: bool,
    /// `true` if this fold discovered entries by listing every commit
    /// prefix up to the watermark (HEAD absent, corrupt, or its parts
    /// unreadable) rather than folding incrementally from a trusted HEAD.
    pub rebuilt: bool,
    /// Number of (shard, hour) commit buckets listed by this fold.
    pub buckets_folded: u64,
    /// Total entries in the new part (previous entries plus newly folded
    /// ones).
    pub entry_count: u64,
    /// Encoded size of the new part, in bytes.
    pub part_bytes: u64,
    pub list_requests: u64,
    pub get_requests: u64,
    pub put_requests: u64,
}

#[derive(Debug, Clone, Copy, Default)]
struct RequestCounters {
    list_requests: u64,
    get_requests: u64,
    put_requests: u64,
}

/// HEAD as read at the top of one fold attempt.
enum HeadState {
    Absent,
    Valid {
        head: SnapshotHead,
        version: Version,
    },
    /// HEAD object exists but failed to decode: logged loudly by
    /// [`Catalog::get_head`] and treated as absent for fold purposes.
    Corrupt {
        version: Version,
    },
}

impl HeadState {
    fn watermark_hour(&self) -> Option<u32> {
        match self {
            HeadState::Valid { head, .. } => Some(head.watermark_hour),
            HeadState::Absent | HeadState::Corrupt { .. } => None,
        }
    }
}

/// Greatest ingest-hour bucket sealed at `now_ns` (docs/catalog-and-mvcc.md,
/// ADR-0020 "Sealed-hour watermark"): the greatest `H` such that
/// `now_ns >= end(H) + max_flush_lifetime + clock_skew_allowance +
/// fold_safety_margin`, where `end(H) = (H + 1) * 1 hour`. `None` if no
/// hour is sealed yet.
fn sealed_watermark_hour(now_ns: i64, config: &CatalogConfig) -> Option<u32> {
    let margin_ns = config.max_flush_lifetime_ns
        + config.clock_skew_allowance_ns
        + config.fold_safety_margin_ns;
    let threshold_ns = now_ns.saturating_sub(margin_ns);
    if threshold_ns < 0 {
        return None;
    }
    let floor_hours = threshold_ns.div_euclid(NS_PER_HOUR);
    if floor_hours < 1 {
        return None;
    }
    u32::try_from(floor_hours - 1).ok()
}

fn head_object_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

fn part_object_key(
    tenant: &TenantHash,
    signal: Signal,
    watermark_hour: u32,
    hash16: &str,
) -> String {
    format!(
        "t/{}/catalog/{}/snap/{}.{}.csnap",
        tenant.to_hex(),
        signal.key_prefix(),
        keys::ingest_hour_string(watermark_hour),
        hash16
    )
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn writer_id_display(bytes: &[u8]) -> String {
    match <[u8; 16]>::try_from(bytes) {
        Ok(arr) => Uuid::from_bytes(arr).to_string(),
        Err(_) => hex_encode(bytes),
    }
}

fn entry_identity(entry: &SnapshotEntry) -> (u32, u32, Vec<u8>, u64, u64) {
    (
        entry.ingest_hour_bucket,
        entry.shard,
        entry.writer_id.clone(),
        entry.writer_epoch,
        entry.writer_seq,
    )
}

fn build_snapshot_entry(key: &str, record: &CommitRecord) -> Result<SnapshotEntry, CatalogError> {
    let writer_id =
        Uuid::parse_str(&record.writer_id).map_err(|_| CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "writer_id",
            expected: "uuid".to_string(),
            actual: record.writer_id.clone(),
        })?;
    Ok(SnapshotEntry {
        level: 0,
        shard: record.shard,
        ingest_hour_bucket: record.ingest_hour_bucket,
        writer_id: writer_id.into_bytes().to_vec(),
        writer_epoch: record.writer_epoch,
        writer_seq: record.writer_seq,
        content_hash: record.content_hash.clone(),
        object_size: record.object_size,
        min_event_ts_ns: record.min_event_ts_ns,
        max_event_ts_ns: record.max_event_ts_ns,
        sample_count: record.sample_count,
        series_count: record.series_count,
        segment_format_version: record.segment_format_version,
        created_unix_ns: record.created_unix_ns,
    })
}

/// (shard, hour) pairs newly sealed since `watermark_hour_old`, exclusive of
/// the old watermark and inclusive of the new one, across every shard.
fn incremental_buckets(
    shard_count: u32,
    watermark_hour_old: u32,
    watermark_hour_new: u32,
) -> Vec<(u32, u32)> {
    let mut buckets = Vec::new();
    for shard in 0..shard_count {
        for hour in (watermark_hour_old + 1)..=watermark_hour_new {
            buckets.push((shard, hour));
        }
    }
    buckets
}

impl Catalog {
    /// Fold previous snapshot parts plus newly sealed commit buckets into a
    /// new snapshot part and CAS-swap HEAD to name it
    /// (docs/metric-index-plan.md section 4). Never runs on the ingest or
    /// query path.
    ///
    /// `now_ns` and `folder_id` are always caller-supplied: this crate never
    /// reads a clock or generates randomness. `folder_id` should be a fresh
    /// UUIDv4 per folder process start (proto/ravel/catalog.proto,
    /// `SnapshotHead.folder_id`).
    ///
    /// `transactions` is the future extension point for compaction/retention
    /// integration (docs/metric-index-plan.md section 7); this phase only
    /// ever receives an empty slice, since [`Transaction`] has no public
    /// constructor yet.
    ///
    /// Returns `Ok` with `no_op: true` if no hour has newly sealed since the
    /// last fold. Every other failure mode that the metric index is allowed
    /// to degrade from (absent/corrupt HEAD, an unreadable previous part)
    /// falls back to a full rebuild from the commit layout rather than
    /// erroring; only a malformed commit record, an unrecognized bucket-key
    /// shape, a duplicate commit identity, or exhausted HEAD CAS retries
    /// surface as [`CatalogError`].
    pub async fn fold(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        folder_id: Uuid,
        now_ns: i64,
        _transactions: &[Transaction],
    ) -> Result<FoldReport, CatalogError> {
        let head_key = head_object_key(tenant, signal);
        let shard_count = self.config().shard_count;
        let mut counters = RequestCounters::default();
        let mut attempt: u32 = 0;

        loop {
            let head_state = self.get_head(&head_key, &mut counters).await?;

            let Some(watermark_hour) = sealed_watermark_hour(now_ns, self.config()) else {
                return Ok(no_op_report(head_state.watermark_hour(), counters));
            };
            if let Some(watermark_hour_old) = head_state.watermark_hour()
                && watermark_hour_old >= watermark_hour
            {
                return Ok(no_op_report(Some(watermark_hour_old), counters));
            }

            let (mut entries, buckets, rebuilt) = match &head_state {
                HeadState::Valid { head, .. } => match self
                    .load_previous_entries(head, &mut counters)
                    .await
                {
                    Ok(entries) => {
                        let buckets =
                            incremental_buckets(shard_count, head.watermark_hour, watermark_hour);
                        (entries, buckets, false)
                    }
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            tenant = %tenant.to_hex(),
                            "previous snapshot part unreadable, rebuilding from commit layout"
                        );
                        let buckets = self
                            .discover_buckets(
                                tenant,
                                signal,
                                shard_count,
                                watermark_hour,
                                &mut counters,
                            )
                            .await?;
                        (Vec::new(), buckets, true)
                    }
                },
                HeadState::Absent | HeadState::Corrupt { .. } => {
                    let buckets = self
                        .discover_buckets(
                            tenant,
                            signal,
                            shard_count,
                            watermark_hour,
                            &mut counters,
                        )
                        .await?;
                    (Vec::new(), buckets, true)
                }
            };

            let mut seen: HashSet<(u32, u32, Vec<u8>, u64, u64)> =
                entries.iter().map(entry_identity).collect();

            for (shard, hour) in &buckets {
                let prefix = keys::commit_shard_hour_prefix(tenant, signal, *shard, *hour)?;
                let listing = list_all(self.store(), &prefix).await?;
                counters.list_requests += 1;

                for meta in &listing {
                    match keys::partition_bucket_entry(&meta.key) {
                        Ok(BucketEntry::CommitRecord(_)) => {
                            let record = self
                                .load_and_validate(tenant, signal, *shard, &meta.key)
                                .await?;
                            counters.get_requests += 1;
                            let entry = build_snapshot_entry(&meta.key, &record)?;
                            if !seen.insert(entry_identity(&entry)) {
                                return Err(CatalogError::DuplicateEntryIdentity {
                                    shard: entry.shard,
                                    ingest_hour_bucket: entry.ingest_hour_bucket,
                                    writer_id: writer_id_display(&entry.writer_id),
                                    writer_epoch: entry.writer_epoch,
                                    writer_seq: entry.writer_seq,
                                });
                            }
                            entries.push(entry);
                        }
                        // Compaction records and the retention tombstone
                        // live in the same bucket (docs/catalog-and-mvcc.md
                        // key layout, ADR-0018/ADR-0019). Recognized but not
                        // yet folded: section 7 integration is deferred past
                        // this phase, hence the `Transaction` extension
                        // point above.
                        Ok(BucketEntry::CompactionRecord(_)) | Ok(BucketEntry::Tombstone(_)) => {}
                        Err(err) => return Err(CatalogError::Key(err)),
                    }
                }
            }

            entries.sort_by(|a, b| {
                (
                    a.ingest_hour_bucket,
                    a.shard,
                    a.writer_id.as_slice(),
                    a.writer_epoch,
                    a.writer_seq,
                )
                    .cmp(&(
                        b.ingest_hour_bucket,
                        b.shard,
                        b.writer_id.as_slice(),
                        b.writer_epoch,
                        b.writer_seq,
                    ))
            });

            let signal_num = signal::to_proto(signal) as u32;
            let part_bytes = snapshot_format::encode_part(
                tenant.0,
                signal_num,
                shard_count,
                watermark_hour,
                &entries,
            )?;
            let part_bytes_len = part_bytes.len() as u64;
            let part_crc = crc32c::crc32c(&part_bytes);
            let part_hash = blake3::hash(&part_bytes);
            let hash16 = &part_hash.to_hex()[..16];
            let part_key = part_object_key(tenant, signal, watermark_hour, hash16);

            match self
                .store()
                .put(
                    &part_key,
                    Bytes::from(part_bytes),
                    PutOptions::create_if_absent().with_checksum(UploadChecksum::Crc32c(part_crc)),
                )
                .await
            {
                Ok(_) => {}
                // Content-addressed key: bytes are identical by
                // construction, so a losing folder's part is as good as its
                // own (mirrors `publish::put_data_object`).
                Err(StoreError::AlreadyExists) => {}
                Err(e) => return Err(CatalogError::Store(e)),
            }
            counters.put_requests += 1;

            let new_head = SnapshotHead {
                format_version: HEAD_FORMAT_VERSION,
                tenant_hash: tenant.0.to_vec(),
                signal: signal_num,
                shard_count,
                watermark_hour,
                parts: vec![SnapshotPartRef {
                    key: part_key,
                    blake3: part_hash.as_bytes().to_vec(),
                    size: part_bytes_len,
                    entry_count: entries.len() as u64,
                    watermark_hour,
                }],
                folder_id: folder_id.into_bytes().to_vec(),
                created_unix_ns: now_ns,
            };
            let head_bytes = snapshot_format::encode_head(&new_head)?;
            let head_crc = crc32c::crc32c(&head_bytes);
            let put_mode = match &head_state {
                HeadState::Valid { version, .. } | HeadState::Corrupt { version } => {
                    PutMode::CasVersion(version.clone())
                }
                HeadState::Absent => PutMode::CreateIfAbsent,
            };

            match self
                .store()
                .put(
                    &head_key,
                    Bytes::from(head_bytes),
                    PutOptions {
                        mode: put_mode,
                        checksum: Some(UploadChecksum::Crc32c(head_crc)),
                    },
                )
                .await
            {
                Ok(_) => {
                    counters.put_requests += 1;
                    return Ok(FoldReport {
                        watermark_hour: Some(watermark_hour),
                        previous_watermark_hour: head_state.watermark_hour(),
                        no_op: false,
                        rebuilt,
                        buckets_folded: buckets.len() as u64,
                        entry_count: entries.len() as u64,
                        part_bytes: part_bytes_len,
                        list_requests: counters.list_requests,
                        get_requests: counters.get_requests,
                        put_requests: counters.put_requests,
                    });
                }
                // Another folder's HEAD CAS won first. Re-GET HEAD next
                // iteration: if the winner's watermark already covers ours,
                // the top-of-loop no-op check stops cleanly; otherwise we
                // rebase onto the winner's parts and retry
                // (docs/metric-index-plan.md section 4, step 7).
                Err(StoreError::PreconditionFailed) | Err(StoreError::AlreadyExists) => {
                    counters.put_requests += 1;
                    attempt += 1;
                    if attempt >= MAX_HEAD_CAS_ATTEMPTS {
                        return Err(CatalogError::FoldCasRetriesExhausted {
                            attempts: attempt,
                            watermark_hour,
                        });
                    }
                }
                Err(e) => return Err(CatalogError::Store(e)),
            }
        }
    }

    async fn get_head(
        &self,
        head_key: &str,
        counters: &mut RequestCounters,
    ) -> Result<HeadState, CatalogError> {
        match self.store().get(head_key, GetRange::Full).await {
            Ok(got) => {
                counters.get_requests += 1;
                match snapshot_format::decode_head(&got.data) {
                    Ok(head) => Ok(HeadState::Valid {
                        head,
                        version: got.version,
                    }),
                    Err(err) => {
                        tracing::warn!(error = %err, key = %head_key, "HEAD failed to decode, treating as absent");
                        Ok(HeadState::Corrupt {
                            version: got.version,
                        })
                    }
                }
            }
            Err(StoreError::NotFound) => {
                counters.get_requests += 1;
                Ok(HeadState::Absent)
            }
            Err(e) => Err(CatalogError::Store(e)),
        }
    }

    /// Load and validate every part HEAD names, verifying each part's bytes
    /// against its recorded blake3 before trusting its entries. Any
    /// failure (GET error, hash mismatch, decode error) is returned to the
    /// caller, which falls back to a full rebuild.
    async fn load_previous_entries(
        &self,
        head: &SnapshotHead,
        counters: &mut RequestCounters,
    ) -> Result<Vec<SnapshotEntry>, CatalogError> {
        let mut entries = Vec::new();
        for part_ref in &head.parts {
            let got = self.store().get(&part_ref.key, GetRange::Full).await?;
            counters.get_requests += 1;
            let digest = blake3::hash(&got.data);
            if digest.as_bytes().as_slice() != part_ref.blake3.as_slice() {
                return Err(CatalogError::FieldMismatch {
                    key: part_ref.key.clone(),
                    field: "blake3",
                    expected: hex_encode(&part_ref.blake3),
                    actual: digest.to_hex().to_string(),
                });
            }
            let decoded = snapshot_format::decode_part(&got.data, &PartLimits::default())?;
            entries.extend(decoded.entries);
        }
        Ok(entries)
    }

    /// Enumerate every (shard, hour) commit bucket at or before
    /// `watermark_hour` by listing the commit-hour directories directly,
    /// rather than trusting a previous snapshot (HEAD absent, corrupt, or
    /// unreadable).
    async fn discover_buckets(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard_count: u32,
        watermark_hour: u32,
        counters: &mut RequestCounters,
    ) -> Result<Vec<(u32, u32)>, CatalogError> {
        let mut buckets = Vec::new();
        for shard in 0..shard_count {
            let prefix = keys::commit_shard_prefix(tenant, signal, shard)?;
            let listing = self.store().list_delimited(&prefix).await?;
            counters.list_requests += 1;
            for common_prefix in &listing.common_prefixes {
                let hour_text = common_prefix
                    .strip_prefix(prefix.as_str())
                    .and_then(|s| s.strip_suffix('/'))
                    .ok_or_else(|| {
                        CatalogError::Key(KeyError::Malformed {
                            key: common_prefix.clone(),
                            reason: "common prefix outside the expected shard prefix".to_string(),
                        })
                    })?;
                let hour = keys::parse_ingest_hour_string(hour_text)?;
                if hour <= watermark_hour {
                    buckets.push((shard, hour));
                }
            }
        }
        buckets.sort_unstable();
        Ok(buckets)
    }
}

fn no_op_report(watermark_hour: Option<u32>, counters: RequestCounters) -> FoldReport {
    FoldReport {
        watermark_hour,
        previous_watermark_hour: watermark_hour,
        no_op: true,
        rebuilt: false,
        buckets_folded: 0,
        entry_count: 0,
        part_bytes: 0,
        list_requests: counters.list_requests,
        get_requests: counters.get_requests,
        put_requests: counters.put_requests,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};

    use super::*;
    use crate::config::{
        DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
        DEFAULT_MAX_FLUSH_LIFETIME_NS,
    };

    const DEFAULT_MARGIN_NS: i64 = DEFAULT_MAX_FLUSH_LIFETIME_NS
        + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
        + DEFAULT_FOLD_SAFETY_MARGIN_NS;

    fn tenant() -> TenantHash {
        TenantHash([0xcd; 16])
    }

    fn config(shard_count: u32) -> CatalogConfig {
        CatalogConfig {
            shard_count,
            ..Default::default()
        }
    }

    /// `now_ns` at which ingest hour `hour` has just sealed under default
    /// margins: the exact boundary of `sealed_watermark_hour`.
    fn now_at_seal(hour: u32) -> i64 {
        (i64::from(hour) + 1) * NS_PER_HOUR + DEFAULT_MARGIN_NS
    }

    async fn publish_segment(
        store: &MemoryStore,
        shard: u32,
        writer_id: Uuid,
        seq: u64,
        ingest_hour_bucket: u32,
        created_unix_ns: i64,
    ) -> CommitRecord {
        let payload = format!("seg-{shard}-{writer_id}-{seq}").into_bytes();
        let content_hash = *blake3::hash(&payload).as_bytes();
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: created_unix_ns - 1_000,
            max_event_ts_ns: created_unix_ns,
            min_ingest_ts_ns: created_unix_ns - 1_000,
            max_ingest_ts_ns: created_unix_ns,
            segment_format_version: 1,
            created_unix_ns,
            ingest_hour_bucket,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store, &data_key, Bytes::from(payload))
            .await
            .expect("put data object");
        publish::publish(store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        record
    }

    #[test]
    fn seal_boundary_is_inclusive_and_hour_by_hour() {
        let cfg = config(1);
        assert_eq!(sealed_watermark_hour(now_at_seal(10), &cfg), Some(10));
        assert_eq!(sealed_watermark_hour(now_at_seal(10) - 1, &cfg), Some(9));
        assert_eq!(sealed_watermark_hour(now_at_seal(0), &cfg), Some(0));
        assert_eq!(sealed_watermark_hour(now_at_seal(0) - 1, &cfg), None);
    }

    #[tokio::test]
    async fn no_hour_sealed_yet_is_a_no_op() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store, config(1)).expect("catalog");
        let report = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), 0, &[])
            .await
            .expect("fold");
        assert!(report.no_op);
        assert_eq!(report.watermark_hour, None);
        assert_eq!(report.put_requests, 0);
    }

    #[tokio::test]
    async fn empty_tenant_still_produces_a_valid_empty_fold() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store, config(2)).expect("catalog");
        let report = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(5),
                &[],
            )
            .await
            .expect("fold");
        assert!(!report.no_op);
        assert!(report.rebuilt);
        assert_eq!(report.watermark_hour, Some(5));
        assert_eq!(report.entry_count, 0);
    }

    #[tokio::test]
    async fn first_fold_rebuilds_from_commit_layout_and_second_call_is_idempotent() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = now_at_seal(10);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 10, now - NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 2, 10, now - NS_PER_HOUR).await;

        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[])
            .await
            .expect("first fold");
        assert!(first.rebuilt);
        assert!(!first.no_op);
        assert_eq!(first.watermark_hour, Some(10));
        assert_eq!(first.previous_watermark_hour, None);
        assert_eq!(first.entry_count, 2);

        // Same now_ns: nothing new sealed, so this must be a clean no-op
        // that touches neither the part nor HEAD.
        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[])
            .await
            .expect("second fold");
        assert!(second.no_op);
        assert_eq!(second.watermark_hour, Some(10));
        assert_eq!(second.put_requests, 0);
    }

    #[tokio::test]
    async fn incremental_fold_preserves_previous_entries_and_folds_only_new_hours() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now_1 = now_at_seal(10);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 10, now_1 - NS_PER_HOUR).await;
        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");
        assert_eq!(first.entry_count, 1);

        let now_2 = now_at_seal(12);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 12, now_2 - NS_PER_HOUR).await;
        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[])
            .await
            .expect("second fold");
        assert!(!second.rebuilt, "a valid HEAD must fold incrementally");
        assert!(!second.no_op);
        assert_eq!(second.previous_watermark_hour, Some(10));
        assert_eq!(second.watermark_hour, Some(12));
        // Hours 11 and 12 across the single shard: two new buckets listed,
        // only one of which has data.
        assert_eq!(second.buckets_folded, 2);
        assert_eq!(second.entry_count, 2);
    }

    #[tokio::test]
    async fn corrupt_head_falls_back_to_rebuild_and_recovers_all_entries() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now_1 = now_at_seal(10);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 10, now_1 - NS_PER_HOUR).await;
        catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");

        // Corrupt HEAD in place: same key, garbage bytes.
        let head_key = head_object_key(&tenant(), Signal::Metrics);
        store
            .put(
                &head_key,
                Bytes::from_static(b"not a head"),
                PutOptions::default(),
            )
            .await
            .expect("overwrite head with garbage");

        let now_2 = now_at_seal(12);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 12, now_2 - NS_PER_HOUR).await;
        let report = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[])
            .await
            .expect("fold after corruption");
        assert!(report.rebuilt);
        assert!(!report.no_op);
        assert_eq!(report.watermark_hour, Some(12));
        assert_eq!(report.entry_count, 2);
    }

    /// Every snapshot part key contains `/snap/`; a HEAD key never does, so
    /// this rule fails exactly the previous-part reads a later incremental
    /// fold performs, and nothing else. Simulates a part that has become
    /// unreadable (e.g. GC raced with a stalled fold) without needing to
    /// know its content-addressed key ahead of time.
    #[tokio::test]
    async fn unreadable_previous_part_falls_back_to_rebuild() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("part unreadable".into()))
                .with_key_contains("/snap/"),
        );
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let now_1 = now_at_seal(10);
        publish_segment(store.inner(), 0, Uuid::new_v4(), 1, 10, now_1 - NS_PER_HOUR).await;
        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");
        assert_eq!(first.entry_count, 1);

        let now_2 = now_at_seal(12);
        publish_segment(store.inner(), 0, Uuid::new_v4(), 1, 12, now_2 - NS_PER_HOUR).await;
        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[])
            .await
            .expect("fold falls back to rebuild");
        assert!(second.rebuilt);
        assert_eq!(second.entry_count, 2);
        assert!(store.fault_count(Op::Get, FaultKind::Permanent) >= 1);
    }

    #[tokio::test]
    async fn two_concurrent_first_folds_race_head_cas_and_only_one_advances() {
        let store = Arc::new(MemoryStore::new());
        let now = now_at_seal(10);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 10, now - NS_PER_HOUR).await;

        let catalog_a = Catalog::new(store.clone(), config(1)).expect("catalog a");
        let catalog_b = Catalog::new(store.clone(), config(1)).expect("catalog b");
        let tenant = tenant();
        let (result_a, result_b) = tokio::join!(
            catalog_a.fold(&tenant, Signal::Metrics, Uuid::new_v4(), now, &[]),
            catalog_b.fold(&tenant, Signal::Metrics, Uuid::new_v4(), now, &[]),
        );
        let report_a = result_a.expect("fold a");
        let report_b = result_b.expect("fold b");

        let no_op_count = [&report_a, &report_b].iter().filter(|r| r.no_op).count();
        assert_eq!(
            no_op_count, 1,
            "exactly one racer must rebase onto the other's HEAD"
        );
        for report in [&report_a, &report_b] {
            assert_eq!(report.watermark_hour, Some(10));
        }
    }

    #[tokio::test]
    async fn duplicate_commit_identity_across_buckets_is_fatal() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = now_at_seal(11);
        let writer_id = Uuid::new_v4();

        // A well-formed record naturally placed in hour 10's directory.
        let record_a = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: 5,
            content_hash: *blake3::hash(b"a").as_bytes(),
            sample_count: 1,
            series_count: 1,
            min_event_ts_ns: 0,
            max_event_ts_ns: 1,
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 1,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: 10,
        })
        .expect("valid record a");
        let key_a =
            keys::commit_key(&tenant(), Signal::Metrics, 0, 10, writer_id, 1, 1).expect("key a");
        store
            .put(
                &key_a,
                record::encode(&record_a),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put a");

        // Same shard/writer_id/epoch/seq/ingest_hour_bucket identity, but
        // physically misplaced under hour 9's directory (a buggy or
        // misbehaving writer). validate_expected_fields never checks the
        // embedded ingest_hour_bucket against the physical directory, so
        // both entries decode and validate cleanly; fold's own identity
        // dedup must be what catches the collision.
        let key_b =
            keys::commit_key(&tenant(), Signal::Metrics, 0, 9, writer_id, 1, 1).expect("key b");
        store
            .put(
                &key_b,
                record::encode(&record_a),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put b");

        let err = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[])
            .await
            .expect_err("must detect duplicate identity");
        assert!(matches!(err, CatalogError::DuplicateEntryIdentity { .. }));
    }
}
