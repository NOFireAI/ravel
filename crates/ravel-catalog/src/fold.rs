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

use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use ravel_commit::keys::{self, BucketEntry, KeyError};
use ravel_commit::signal;
use ravel_object_store::{
    GetRange, ObjectStoreBackend, PutMode, PutOptions, StoreError, UploadChecksum, Version,
    list_all,
};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartRef, SnapshotPostingsRef};
use ravel_proto::commit::v1::{CommitRecord, CompactionPart, CompactionRecord};
use ravel_segment::{ExpectedIdentity, ReaderLimits, SegmentError};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{METRIC_NAME_LABEL, Signal, TenantHash};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::config::CatalogConfig;
use crate::error::CatalogError;
use crate::provisioning::{DEFAULT_SCAN_SLACK_HOURS, ShardGeneration, scan_count};
use crate::snapshot_format::{self, HEAD_FORMAT_VERSION, NamePostings, PartLimits};

/// RSEG section kinds needed to decode a segment's catalog
/// (docs/segment-format.md `SectionKind`). Kinds 1/2 are v1
/// (LABEL_DICT/SERIES_TABLE); 5/6 are v2 (SERIES_IDS/SERIES_META).
/// A failure while building the name-postings index for a fold. Every
/// variant is caught by [`Catalog::build_postings`], logged, and turned into
/// "fold succeeds without a postings ref" (docs/metric-index-plan.md P5a:
/// "Postings build failures leave the fold successful without a postings
/// ref") -- never surfaced as a [`CatalogError`], and never a partial or
/// approximate postings object (ravel CLAUDE.md: "Exact semantics by
/// default").
#[derive(Debug, thiserror::Error)]
pub enum PostingsBuildError {
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("key error: {0}")]
    Key(#[from] KeyError),
    #[error("segment error: {0}")]
    Segment(#[from] SegmentError),
    #[error("snapshot format error: {0}")]
    Format(#[from] snapshot_format::SnapshotFormatError),
    #[error("entry content_hash must be 32 bytes, got {0}")]
    BadContentHashLen(usize),
    #[error("entry writer_id must be 16 bytes, got {0}")]
    BadWriterIdLen(usize),
    #[error("segment series entry has no {METRIC_NAME_LABEL} label")]
    MissingMetricName,
    #[error("unsupported segment format version {0}")]
    UnsupportedSegmentVersion(u16),
}

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
    /// `true` if this fold successfully built and attached a name-postings
    /// index (docs/metric-index-plan.md P5a). `false` covers both "no
    /// postings work was needed" (a no-op fold) and "the build failed and
    /// the fold proceeded without a postings ref".
    pub postings_built: bool,
    /// Encoded size of the postings object, in bytes. `0` when
    /// `postings_built` is `false`.
    pub postings_bytes: u64,
    /// Number of commit-bucket entries this fold skipped rather than
    /// aborting on: an unrecognized bucket-key shape, or a commit record
    /// whose identity duplicates one already folded. Both are layout drift
    /// (docs/catalog-and-mvcc.md key-layout section: "layout drift must be
    /// visible, not swallowed"), logged at `warn!` with the offending
    /// key/identity -- satisfied by the log plus this counter, never by
    /// aborting the fold, since either condition is a permanent property of
    /// the sealed layout and aborting would block the watermark forever.
    pub layout_drift_count: u64,
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

pub(crate) fn head_object_key(tenant: &TenantHash, signal: Signal) -> String {
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

fn postings_object_key(
    tenant: &TenantHash,
    signal: Signal,
    watermark_hour: u32,
    hash16: &str,
) -> String {
    format!(
        "t/{}/catalog/{}/idx/{}.{}.npost",
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

/// (ingest_hour_bucket, shard, writer_id, writer_epoch, writer_seq): the
/// identity a commit record must be unique under (docs/catalog-and-mvcc.md).
type EntryIdentity = (u32, u32, Vec<u8>, u64, u64);

fn entry_identity(entry: &SnapshotEntry) -> EntryIdentity {
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

/// Build a level-1 [`SnapshotEntry`] for one part of a compaction record
/// (docs/metric-index-plan.md section 7 point 2). The proto's frozen entry
/// shape has no dedicated field for an L1 part's identity, so the writer_*
/// slots -- which an L1 part has no native use for -- carry it: `writer_id`
/// holds the 32-byte `input_set_hash` and `writer_epoch` holds the
/// `part_index`. A resolve reads them back verbatim
/// (`build_segment_ref_from_entry`) to reconstruct the exact same
/// `SegmentRef` a live listing builds from the record and part directly
/// (`build_l1_segment_ref`), keyed by `reconstruct_l1_part_key`. `level` is
/// pinned to 1: the resolver treats every compaction record as an L1 part
/// regardless of its declared level, and the fold matches that.
fn build_l1_snapshot_entry(
    key: &str,
    record: &CompactionRecord,
    part: &CompactionPart,
) -> Result<SnapshotEntry, CatalogError> {
    if record.input_set_hash.len() != 32 {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "input_set_hash",
            expected: "32 bytes".to_string(),
            actual: format!("{} bytes", record.input_set_hash.len()),
        });
    }
    if part.content_hash.len() != 32 {
        return Err(CatalogError::FieldMismatch {
            key: key.to_string(),
            field: "part content_hash",
            expected: "32 bytes".to_string(),
            actual: format!("{} bytes", part.content_hash.len()),
        });
    }
    Ok(SnapshotEntry {
        level: 1,
        shard: record.shard,
        ingest_hour_bucket: record.ingest_hour_bucket,
        writer_id: record.input_set_hash.clone(),
        writer_epoch: u64::from(part.part_index),
        writer_seq: 0,
        content_hash: part.content_hash.clone(),
        object_size: part.object_size,
        min_event_ts_ns: part.min_event_ts_ns,
        max_event_ts_ns: part.max_event_ts_ns,
        sample_count: part.sample_count,
        series_count: part.series_count,
        segment_format_version: part.segment_format_version,
        created_unix_ns: record.created_unix_ns,
    })
}

/// Fold one entry into the running set, deduping by commit/part identity.
/// `is_canonical` marks whether this occurrence sits under the hour directory
/// its own embedded `ingest_hour_bucket` names. On a duplicate identity the
/// canonical occurrence is preferred and the collision is counted as layout
/// drift (docs/catalog-and-mvcc.md key layout: "layout drift must be visible,
/// not swallowed"), never fatal, since a duplicate identity is a permanent
/// property of the sealed layout and aborting would block the watermark
/// forever.
fn fold_in_entry(
    entries: &mut Vec<SnapshotEntry>,
    seen: &mut HashMap<EntryIdentity, (usize, bool)>,
    layout_drift_count: &mut u64,
    entry: SnapshotEntry,
    is_canonical: bool,
) {
    match seen.entry(entry_identity(&entry)) {
        Entry::Vacant(slot) => {
            slot.insert((entries.len(), is_canonical));
            entries.push(entry);
        }
        Entry::Occupied(mut slot) => {
            *layout_drift_count += 1;
            tracing::warn!(
                level = entry.level,
                shard = entry.shard,
                ingest_hour_bucket = entry.ingest_hour_bucket,
                writer_id = %writer_id_display(&entry.writer_id),
                writer_epoch = entry.writer_epoch,
                writer_seq = entry.writer_seq,
                "duplicate entry identity while folding, preferring the entry in its correct hour directory"
            );
            let &(existing_index, existing_canonical) = slot.get();
            if !existing_canonical && is_canonical {
                entries[existing_index] = entry;
                slot.insert((existing_index, true));
            }
        }
    }
}

/// (shard, hour) pairs newly sealed since `watermark_hour_old`, exclusive of
/// the old watermark and inclusive of the new one. Each hour's shard fan-out
/// is its own `scan_count(h)` derived from the generation history (ADR-0052
/// section 4), not one static count for the whole range, so a fold enumerates
/// exactly the shard set a live resolve would list for the same hours.
fn incremental_buckets(
    generations: &[ShardGeneration],
    watermark_hour_old: u32,
    watermark_hour_new: u32,
) -> Vec<(u32, u32)> {
    let mut buckets = Vec::new();
    for hour in (watermark_hour_old + 1)..=watermark_hour_new {
        let scan = scan_count(generations, hour, DEFAULT_SCAN_SLACK_HOURS);
        for shard in 0..scan {
            buckets.push((shard, hour));
        }
    }
    buckets
}

/// The fan-out ceiling at fold time (ADR-0052 section 5). Delegates to the one
/// shared [`crate::provisioning::shard_ceiling`] that the reader's head
/// validation (`snapshot_resolve::head_generations_acceptable`) also calls, so
/// the value a fold stamps into `SnapshotHead.shard_count` and the value a
/// reader recomputes to validate that head can never diverge (a decrease
/// followed by a fold past the slack window used to have the two disagree and
/// hard-fail every query permanently).
fn fold_shard_ceiling(generations: &[ShardGeneration], watermark_hour: u32) -> u32 {
    crate::provisioning::shard_ceiling(generations, watermark_hour)
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
    /// Compaction and retention are folded in from the same per-bucket
    /// listing this fold already performs (docs/metric-index-plan.md section
    /// 7 point 1): a bucket's `CompactionRecord`s contribute their parts as
    /// level-1 entries and supersede their named L0 inputs, and a bucket
    /// holding a `RetentionTombstone` contributes nothing. No separate
    /// discovery mechanism is needed, so `transactions` stays an unused
    /// extension point ([`Transaction`] has no public constructor).
    ///
    /// Returns `Ok` with `no_op: true` if no hour has newly sealed since the
    /// last fold. Every other failure mode that the metric index is allowed
    /// to degrade from (absent/corrupt HEAD, an unreadable previous part)
    /// falls back to a full rebuild from the commit layout rather than
    /// erroring. An unrecognized bucket-key shape or a duplicate commit
    /// identity is a permanent property of the sealed layout, not a
    /// transient fault: rather than failing every subsequent fold
    /// identically forever, these are counted in
    /// [`FoldReport::layout_drift_count`], logged at `warn!` with the
    /// offending key/identity, and skipped (see the bucket-processing loop
    /// below). Only a malformed commit record or exhausted HEAD CAS retries
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
        // The generation history for the read-side scan rule (ADR-0052 section
        // 4/5), read fresh (see `Catalog::read_scan_generations`). A fold must
        // enumerate the same per-hour shard set a resolve would, and records
        // the fan-out ceiling and generation count into the HEAD it writes.
        let generations = self.read_scan_generations(tenant, signal).await?;
        let shard_generation_count = generations.len() as u32;
        let mut counters = RequestCounters::default();
        let mut attempt: u32 = 0;
        // Fold never runs on the query path (module docs above) and keeps
        // its own `RequestCounters`; this handle exists only to satisfy the
        // shared cache/load API's new `QueryAccounting` parameter (issue
        // #421) and is discarded.
        let accounting = QueryAccounting::new();

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

            let (mut entries, buckets, rebuilt, previous_entries_len) = match &head_state {
                HeadState::Valid { head, .. } => match self
                    .load_previous_entries(head, &mut counters)
                    .await
                {
                    Ok(entries) => {
                        let buckets =
                            incremental_buckets(&generations, head.watermark_hour, watermark_hour);
                        let previous_entries_len = entries.len();
                        (entries, buckets, false, previous_entries_len)
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
                                &generations,
                                watermark_hour,
                                &mut counters,
                            )
                            .await?;
                        (Vec::new(), buckets, true, 0)
                    }
                },
                HeadState::Absent | HeadState::Corrupt { .. } => {
                    let buckets = self
                        .discover_buckets(
                            tenant,
                            signal,
                            &generations,
                            watermark_hour,
                            &mut counters,
                        )
                        .await?;
                    (Vec::new(), buckets, true, 0)
                }
            };

            // Identity -> (index in `entries`, whether that occurrence sits
            // under the hour directory its own embedded ingest_hour_bucket
            // names). Entries loaded from a previous part already survived
            // this same check in an earlier fold, so they seed the map as
            // trivially canonical.
            let mut seen: HashMap<EntryIdentity, (usize, bool)> = entries
                .iter()
                .enumerate()
                .map(|(index, entry)| (entry_identity(entry), (index, true)))
                .collect();
            let mut layout_drift_count: u64 = 0;

            for (shard, hour) in &buckets {
                let prefix = keys::commit_shard_hour_prefix(tenant, signal, *shard, *hour)?;
                let listing = list_all(self.store(), &prefix).await?;
                counters.list_requests += 1;

                // Partition the bucket's keys by shape, mirroring the
                // resolve-time listing (`Catalog::list_hour_bucket`,
                // docs/catalog-and-mvcc.md step 2) so a fold and a live
                // resolve derive identical bucket state
                // (docs/metric-index-plan.md section 7).
                let mut l0_keys: Vec<&str> = Vec::new();
                let mut compaction_keys: Vec<&str> = Vec::new();
                let mut has_tombstone = false;
                for meta in &listing {
                    match keys::partition_bucket_entry(&meta.key) {
                        Ok(BucketEntry::CommitRecord(_)) => l0_keys.push(meta.key.as_str()),
                        Ok(BucketEntry::CompactionRecord(_)) => {
                            compaction_keys.push(meta.key.as_str())
                        }
                        Ok(BucketEntry::Tombstone(_)) => has_tombstone = true,
                        Err(err) => {
                            layout_drift_count += 1;
                            tracing::warn!(
                                error = %err,
                                key = %meta.key,
                                "unrecognized bucket-key shape, skipping key"
                            );
                        }
                    }
                }

                // Retention tombstone: the bucket contributes nothing, so
                // neither its L1 parts nor its L0 records are folded in
                // (ADR-0019 section 3, docs/metric-index-plan.md section 7
                // point 2/3).
                if has_tombstone {
                    continue;
                }

                // Compaction records: fold each part as a level-1 entry and
                // collect the L0 input identities it supersedes
                // (docs/metric-index-plan.md section 7 point 2). Two records
                // with different input sets in one bucket both contribute
                // their parts (a harmless overlap the resolver alarms on but
                // still serves); the fold matches that inclusion.
                let mut excluded: HashSet<(String, u64, u64)> = HashSet::new();
                for ckey in &compaction_keys {
                    let record = self
                        .load_and_validate_compaction(tenant, signal, *shard, ckey, &accounting)
                        .await?;
                    counters.get_requests += 1;
                    for part in &record.parts {
                        let entry = build_l1_snapshot_entry(ckey, &record, part)?;
                        fold_in_entry(
                            &mut entries,
                            &mut seen,
                            &mut layout_drift_count,
                            entry,
                            true,
                        );
                    }
                    for input in &record.inputs {
                        excluded.insert((
                            input.writer_id.clone(),
                            input.writer_epoch,
                            input.writer_seq,
                        ));
                    }
                }

                // L0 commit records: fold every one not named by a compaction
                // input list above (docs/metric-index-plan.md section 7 point
                // 2). An unlisted L0 is included exactly as the resolver
                // includes it.
                for key in &l0_keys {
                    let record = self
                        .load_and_validate(tenant, signal, *shard, key, &accounting)
                        .await?;
                    counters.get_requests += 1;
                    if excluded.contains(&(
                        record.writer_id.clone(),
                        record.writer_epoch,
                        record.writer_seq,
                    )) {
                        continue;
                    }
                    let entry = build_snapshot_entry(key, &record)?;
                    let is_canonical = *hour == entry.ingest_hour_bucket;
                    fold_in_entry(
                        &mut entries,
                        &mut seen,
                        &mut layout_drift_count,
                        entry,
                        is_canonical,
                    );
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
            // The part/HEAD `shard_count` is the fan-out ceiling at fold time
            // (ADR-0052 section 5), not this process's static config value.
            let shard_ceiling = fold_shard_ceiling(&generations, watermark_hour);
            let part_bytes = snapshot_format::encode_part(
                tenant.0,
                signal_num,
                shard_ceiling,
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

            // Postings are merged forward from the previous fold's postings
            // object rather than re-decoded from every historical segment:
            // entries only ever gain a strictly greater ingest_hour_bucket
            // than the entries a valid HEAD already covers
            // (`incremental_buckets` only lists hours past the old
            // watermark), so the sort above always keeps previously-covered
            // entries at their same ordinal prefix and `decode_start` below
            // never needs to re-fetch them. On the rebuilt path (corrupt
            // HEAD, unreadable previous part), or when no usable previous
            // postings baseline exists (no postings ref on HEAD, or it fails
            // to load), decoding starts at 0 and every current entry is
            // fetched once -- a one-time full build, not a partial merge. A
            // build failure at any entry from `decode_start` onward aborts
            // the whole index, never publishes a partial one
            // (docs/metric-index-plan.md P5a).
            let postings_names = if entries.iter().all(|entry| entry.level == 0) {
                let (postings_baseline, postings_decode_start): (Vec<NamePostings>, usize) =
                    if rebuilt {
                        (Vec::new(), 0)
                    } else if let HeadState::Valid { head, .. } = &head_state {
                        match self
                            .load_previous_postings(
                                tenant,
                                head,
                                previous_entries_len as u64,
                                &mut counters,
                                &accounting,
                            )
                            .await
                        {
                            Some(names) => (names, previous_entries_len),
                            None => (Vec::new(), 0),
                        }
                    } else {
                        (Vec::new(), 0)
                    };
                self.build_postings(
                    tenant,
                    signal,
                    &entries,
                    postings_decode_start,
                    postings_baseline,
                    &mut counters,
                )
                .await
            } else {
                // A level-1 (compaction) entry is present. The forward
                // postings merge assumes append-only, hour-major L0 growth
                // with stable ordinals, which a compaction rewrite breaks, and
                // an L1 part carries no L0 segment to decode `__name__` from.
                // Publish no postings ref so a resolve considers every entry
                // exactly rather than pruning against a stale or partial index
                // (docs/metric-index-plan.md 3.1, section 7 point 6).
                None
            };
            let mut postings_built = false;
            let mut postings_size = 0u64;
            let postings_ref = match postings_names {
                Some(names) => match snapshot_format::encode_postings(
                    tenant.0,
                    signal_num,
                    &[*part_hash.as_bytes()],
                    entries.len() as u64,
                    &names,
                ) {
                    Ok(postings_bytes) => {
                        let postings_crc = crc32c::crc32c(&postings_bytes);
                        let postings_hash = blake3::hash(&postings_bytes);
                        let postings_hash16 = &postings_hash.to_hex()[..16];
                        let postings_key =
                            postings_object_key(tenant, signal, watermark_hour, postings_hash16);
                        let size = postings_bytes.len() as u64;
                        let name_count = names.len() as u32;
                        match self
                            .store()
                            .put(
                                &postings_key,
                                Bytes::from(postings_bytes),
                                PutOptions::create_if_absent()
                                    .with_checksum(UploadChecksum::Crc32c(postings_crc)),
                            )
                            .await
                        {
                            Ok(_) | Err(StoreError::AlreadyExists) => {
                                counters.put_requests += 1;
                                postings_built = true;
                                postings_size = size;
                                Some(SnapshotPostingsRef {
                                    key: postings_key,
                                    blake3: postings_hash.as_bytes().to_vec(),
                                    size,
                                    name_count,
                                    part_blake3: vec![part_hash.as_bytes().to_vec()],
                                })
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    tenant = %tenant.to_hex(),
                                    "postings PUT failed, folding without a postings ref"
                                );
                                None
                            }
                        }
                    }
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            tenant = %tenant.to_hex(),
                            "postings encode failed, folding without a postings ref"
                        );
                        None
                    }
                },
                None => None,
            };

            let new_head = SnapshotHead {
                format_version: HEAD_FORMAT_VERSION,
                tenant_hash: tenant.0.to_vec(),
                signal: signal_num,
                shard_count: shard_ceiling,
                watermark_hour,
                parts: vec![SnapshotPartRef {
                    key: part_key,
                    blake3: part_hash.as_bytes().to_vec(),
                    size: part_bytes_len,
                    entry_count: entries.len() as u64,
                    watermark_hour,
                }],
                postings: postings_ref,
                folder_id: folder_id.into_bytes().to_vec(),
                created_unix_ns: now_ns,
                shard_generation_count,
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
                        postings_built,
                        postings_bytes: postings_size,
                        layout_drift_count,
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

    /// Load and verify the previous fold's postings object before trusting
    /// it as a merge baseline (docs/metric-index-plan.md P5a): same
    /// hash/part-binding/decode checks `snapshot_resolve`'s
    /// `load_snapshot_postings` applies at query time, plus an
    /// `entry_count` check against `previous_entry_count` -- the ordinal
    /// boundary this fold's merge carries forward, not the post-fold total.
    /// Any failure (no postings ref, cache/GET miss, hash mismatch, decode
    /// error, entry_count mismatch) returns `None`, so the caller decodes
    /// every current entry from scratch instead of merging.
    async fn load_previous_postings(
        &self,
        tenant: &TenantHash,
        head: &SnapshotHead,
        previous_entry_count: u64,
        counters: &mut RequestCounters,
        accounting: &QueryAccounting,
    ) -> Option<Vec<NamePostings>> {
        let postings_ref = head.postings.as_ref()?;
        let expected_part_blake3: Vec<[u8; 32]> = head
            .parts
            .iter()
            .map(|p| <[u8; 32]>::try_from(p.blake3.as_slice()))
            .collect::<Result<_, _>>()
            .ok()?;

        if let Some(cached) = self
            .postings_cache()
            .get(tenant, &postings_ref.key, accounting)
        {
            if cached.header.entry_count == previous_entry_count {
                return Some(cached.names.clone());
            }
            tracing::warn!(
                key = %postings_ref.key,
                "cached previous postings entry_count mismatch, rebuilding postings from scratch"
            );
            return None;
        }

        let got = match self.store().get(&postings_ref.key, GetRange::Full).await {
            Ok(got) => got,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    key = %postings_ref.key,
                    tenant = %tenant.to_hex(),
                    "previous postings GET failed, rebuilding postings from scratch"
                );
                return None;
            }
        };
        counters.get_requests += 1;
        let digest = blake3::hash(&got.data);
        if digest.as_bytes().as_slice() != postings_ref.blake3.as_slice() {
            tracing::warn!(
                key = %postings_ref.key,
                "previous postings hash mismatch, rebuilding postings from scratch"
            );
            return None;
        }
        let limits = snapshot_format::PostingsLimits {
            max_postings_bytes: self.config().max_postings_bytes,
        };
        let decoded =
            match snapshot_format::decode_postings(&got.data, &limits, &expected_part_blake3) {
                Ok(decoded) => decoded,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        key = %postings_ref.key,
                        "previous postings failed to decode, rebuilding postings from scratch"
                    );
                    return None;
                }
            };
        if decoded.header.entry_count != previous_entry_count {
            tracing::warn!(
                key = %postings_ref.key,
                expected = previous_entry_count,
                actual = decoded.header.entry_count,
                "previous postings entry_count mismatch, rebuilding postings from scratch"
            );
            return None;
        }

        let decoded = Arc::new(decoded);
        self.postings_cache().insert(
            *tenant,
            postings_ref.key.clone(),
            decoded.clone(),
            got.data.len() as u64,
            self.config().postings_cache_entries,
        );
        Some(decoded.names.clone())
    }

    /// Build the name-postings index for `entries` (docs/metric-index-plan.md
    /// P5a), merging forward from `baseline` rather than re-decoding every
    /// historical segment on every fold: entries before `decode_start` keep
    /// exactly the ordinals `baseline` already recorded for them, and only
    /// entries at or past `decode_start` are fetched and decoded. Returns
    /// `None` on any failure (a segment fetch, identity check, or decode
    /// error, or a series missing `__name__`), logging the cause: a
    /// postings build is all-or-nothing over the entries it decodes, never
    /// partial.
    async fn build_postings(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        entries: &[SnapshotEntry],
        decode_start: usize,
        baseline: Vec<NamePostings>,
        counters: &mut RequestCounters,
    ) -> Option<Vec<NamePostings>> {
        let mut by_name: BTreeMap<String, Vec<u64>> = baseline
            .into_iter()
            .map(|np| (np.name, np.ordinals))
            .collect();
        for (ordinal, entry) in entries.iter().enumerate().skip(decode_start) {
            let names = match fetch_segment_names(self.store(), tenant, signal, entry).await {
                Ok(names) => {
                    counters.get_requests += 1;
                    names
                }
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        tenant = %tenant.to_hex(),
                        shard = entry.shard,
                        ingest_hour_bucket = entry.ingest_hour_bucket,
                        "postings build aborted: segment fetch/decode failed"
                    );
                    return None;
                }
            };
            let ordinal = ordinal as u64;
            for name in names {
                by_name.entry(name).or_default().push(ordinal);
            }
        }
        Some(
            by_name
                .into_iter()
                .map(|(name, ordinals)| NamePostings { name, ordinals })
                .collect(),
        )
    }

    // The per-entry `__name__` derivation the postings build consumes is the
    // free `fetch_segment_names` function (after this impl block), extracted so
    // `ravel-maintain`'s scrubber re-derives names exactly the way the fold
    // that wrote the postings did (ADR-0059 decision 3).

    /// Enumerate every (shard, hour) commit bucket at or before
    /// `watermark_hour` by listing the commit-hour directories directly,
    /// rather than trusting a previous snapshot (HEAD absent, corrupt, or
    /// unreadable). The shard range is the fold-time fan-out ceiling
    /// (ADR-0052 section 5), i.e. the union of every generation activated by
    /// the watermark, so a rebuild lists every shard any generation ever
    /// wrote (a shard a generation never used lists empty, which is cheap and
    /// which `list_shard_hours`-style listing already tolerates). Data only
    /// ever lands in hours `<= watermark` under a generation active at or
    /// before those hours, so this ceiling is complete; listing is the ground
    /// truth here, so a discovered (shard, hour) is always kept.
    async fn discover_buckets(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        generations: &[ShardGeneration],
        watermark_hour: u32,
        counters: &mut RequestCounters,
    ) -> Result<Vec<(u32, u32)>, CatalogError> {
        let scan_shards = fold_shard_ceiling(generations, watermark_hour);
        let mut buckets = Vec::new();
        for shard in 0..scan_shards {
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

/// Fetch one entry's segment and return the distinct `__name__` values among
/// its series. The segment is fetched in full (rather than the
/// footer-suffix-then-range-chase protocol `ravel-query`'s fetcher uses for
/// query-time page reads): a fold reads every newly-covered entry's catalog
/// exactly once and needs no page data, so the extra bytes of a single full
/// GET are cheaper than the extra round trips a suffix chase would add here.
///
/// This is the one authoritative derivation of a segment's true `__name__`
/// set from a [`SnapshotEntry`]: [`Catalog::build_postings`] uses it to build
/// the name-postings index, and `ravel-maintain`'s content-tier scrubber
/// (ADR-0059 decision 1) uses it to re-derive the same set at rest and diff it
/// against what a covering postings object claims for the object. Keeping a
/// single implementation is deliberate: two copies that must always agree
/// would be a maintenance hazard, since a scrub is only meaningful if it
/// derives names exactly the way the fold that wrote the postings did.
pub async fn fetch_segment_names(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    entry: &SnapshotEntry,
) -> Result<HashSet<String>, PostingsBuildError> {
    let content_hash: [u8; 32] = entry
        .content_hash
        .as_slice()
        .try_into()
        .map_err(|_| PostingsBuildError::BadContentHashLen(entry.content_hash.len()))?;
    let writer_id_bytes: [u8; 16] = entry
        .writer_id
        .as_slice()
        .try_into()
        .map_err(|_| PostingsBuildError::BadWriterIdLen(entry.writer_id.len()))?;
    let writer_id = Uuid::from_bytes(writer_id_bytes);
    let data_key = keys::data_key(
        tenant,
        signal,
        entry.shard,
        writer_id,
        entry.writer_epoch,
        entry.writer_seq,
        &content_hash,
    )?;
    let got = store.get(&data_key, GetRange::Full).await?;

    let limits = ReaderLimits::default();
    let location = ravel_segment::open_from_full(&got.data, limits)?;
    let expected = ExpectedIdentity {
        tenant_hash: tenant.0,
        shard: entry.shard,
        writer_id: writer_id.to_string(),
        writer_epoch: entry.writer_epoch,
        writer_seq: entry.writer_seq,
    };
    ravel_segment::check_identity(&location.footer, &expected)?;

    // ADR-0027: v6 is the only supported version (`open_from_full` above has
    // already rejected anything else). The chunked v5-shaped catalog
    // (unchanged by the v6 EXEMPLARS addition) spans sections, so it is
    // decoded over the whole object -- already in hand here via the
    // `GetRange::Full` GET -- and folded to the per-series `SeriesEntry` view
    // the postings build consumes.
    let series: Vec<ravel_segment::SeriesEntry> = match location.version {
        ravel_segment::VERSION_V6 => {
            ravel_segment::decode_catalog_v5(&location.footer, &got.data, limits)?
                .into_iter()
                .map(|e| e.entry)
                .collect()
        }
        other => return Err(PostingsBuildError::UnsupportedSegmentVersion(other)),
    };

    let mut names = HashSet::new();
    for s in &series {
        let name = s
            .labels
            .get(METRIC_NAME_LABEL)
            .ok_or(PostingsBuildError::MissingMetricName)?;
        names.insert(name.to_string());
    }
    Ok(names)
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
        postings_built: false,
        postings_bytes: 0,
        layout_drift_count: 0,
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
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

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

    /// Like `publish_segment`, but writes a real RSEG v1 segment (not a fake
    /// payload) so that `Catalog::build_postings` has something genuine to
    /// decode. Needed for tests that must observe postings-build behavior,
    /// since a fake payload makes `build_postings` fail unconditionally
    /// regardless of any fault injection under test.
    async fn publish_real_segment(
        store: &MemoryStore,
        shard: u32,
        writer_id: Uuid,
        seq: u64,
        ingest_hour_bucket: u32,
        created_unix_ns: i64,
        metrics: &[&str],
    ) -> CommitRecord {
        let tenant_id = TenantId::new("fold-postings-fault-test");
        let series: Vec<SeriesInput> = metrics
            .iter()
            .map(|metric| {
                let labels = LabelSet::new(vec![Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: (*metric).to_string(),
                }])
                .expect("valid labels");
                let series_id = SeriesId::compute(&tenant_id, metric, &labels).expect("series id");
                SeriesInput {
                    series_id,
                    labels,
                    samples: vec![Sample {
                        ts_ns: created_unix_ns,
                        value: 1.0,
                    }],
                }
            })
            .collect();
        let identity = SegmentIdentity {
            tenant_hash: tenant().0,
            shard,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: seq,
        };
        let min_ingest_ts_ns = created_unix_ns - 1_000;
        let max_ingest_ts_ns = created_unix_ns;
        let bounds = IngestBounds {
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        };
        let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            segment_format_version: 1,
            created_unix_ns,
            ingest_hour_bucket,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store, &data_key, written.bytes)
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

    /// Every data-object key contains `/l0/`
    /// (crates/ravel-commit/src/keys.rs `data_key`); no HEAD, part, postings,
    /// or commit-record key does. This rule fails exactly the segment fetch
    /// `Catalog::build_postings` performs mid-fold, and nothing else, so the
    /// part and HEAD writes still succeed while the postings build is the
    /// one thing interrupted.
    #[tokio::test]
    async fn segment_fetch_fault_mid_postings_build_leaves_fold_successful_without_postings() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("segment unreadable".into()),
            )
            .with_key_contains("/l0/"),
        );
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let now = now_at_seal(10);
        publish_real_segment(
            store.inner(),
            0,
            Uuid::new_v4(),
            1,
            10,
            now - NS_PER_HOUR,
            &["cpu", "mem"],
        )
        .await;

        let report = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[])
            .await
            .expect("fold succeeds despite postings-build fault");

        assert!(!report.no_op);
        assert_eq!(report.entry_count, 1);
        assert!(!report.postings_built);
        assert_eq!(report.postings_bytes, 0);
        assert!(store.fault_count(Op::Get, FaultKind::Permanent) >= 1);

        let head_key = head_object_key(&tenant(), Signal::Metrics);
        let got = store
            .inner()
            .get(&head_key, GetRange::Full)
            .await
            .expect("head readable");
        let head = snapshot_format::decode_head(&got.data).expect("head decodes");
        assert!(head.postings.is_none());
        assert_eq!(head.parts.len(), 1);
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
    async fn duplicate_commit_identity_across_buckets_skips_and_advances() {
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

        let report = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[])
            .await
            .expect("duplicate identity must not be fold-fatal");
        assert!(!report.no_op);
        assert_eq!(report.watermark_hour, Some(11));
        assert_eq!(
            report.entry_count, 1,
            "the misplaced duplicate collapses into the one entry kept in its correct hour directory"
        );
        assert_eq!(report.layout_drift_count, 1);
    }

    #[tokio::test]
    async fn unrecognized_bucket_key_shape_skips_and_advances() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = now_at_seal(5);

        // A key sitting in a well-formed commit hour directory, but whose
        // filename matches none of the three recognized shapes (commit
        // record, compaction record, retention tombstone).
        let prefix = keys::commit_shard_hour_prefix(&tenant(), Signal::Metrics, 0, 5)
            .expect("bucket prefix");
        store
            .put(
                &format!("{prefix}not-a-recognized-shape"),
                Bytes::from_static(b"layout drift"),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put unrecognized key");

        let report = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[])
            .await
            .expect("unrecognized key shape must not be fold-fatal");
        assert!(!report.no_op);
        assert!(report.rebuilt);
        assert_eq!(report.watermark_hour, Some(5));
        assert_eq!(report.entry_count, 0);
        assert_eq!(report.layout_drift_count, 1);
    }

    /// Issue #183: a fold must decode only the entries newly folded since
    /// the last successful fold, carrying forward the previous postings
    /// object as a merge baseline rather than re-fetching every
    /// historically-covered entry's segment. `get_requests` is the proof:
    /// each fold's total is bookkeeping gets (HEAD, plus on the second
    /// fold the previous part and previous postings) plus exactly one
    /// commit-record load and one segment fetch per *newly* folded entry.
    /// A fold that still rebuilt postings from every historical segment
    /// would add one extra segment fetch on the second run (re-decoding
    /// the first fold's entry), landing on 6 instead of 5.
    #[tokio::test]
    async fn incremental_fold_decodes_only_newly_folded_entries_for_postings() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let now_1 = now_at_seal(10);
        publish_real_segment(
            &store,
            0,
            Uuid::new_v4(),
            1,
            10,
            now_1 - NS_PER_HOUR,
            &["cpu"],
        )
        .await;
        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");
        assert!(!first.no_op);
        assert_eq!(first.entry_count, 1);
        assert!(first.postings_built);
        // HEAD get (absent) + 1 commit-record load + 1 segment fetch to
        // decode the sole entry's names.
        assert_eq!(first.get_requests, 3);

        let now_2 = now_at_seal(12);
        publish_real_segment(
            &store,
            0,
            Uuid::new_v4(),
            1,
            12,
            now_2 - NS_PER_HOUR,
            &["mem"],
        )
        .await;
        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[])
            .await
            .expect("second fold");
        assert!(!second.rebuilt, "a valid HEAD must fold incrementally");
        assert!(!second.no_op);
        assert_eq!(second.entry_count, 2);
        assert!(second.postings_built);
        // HEAD get + previous-part load + previous-postings load + 1 new
        // commit-record load + 1 new segment fetch = 5: the first fold's
        // "cpu" entry is neither re-loaded from the part nor re-decoded
        // from its segment.
        assert_eq!(second.get_requests, 5);
    }
}
