//! Metric index fold: catalog snapshot construction from commit records
//! (ADR-0020).
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
use futures::stream::{self, StreamExt};
use ravel_commit::keys::{self, BucketEntry, KeyError};
use ravel_commit::signal;
use ravel_object_store::{
    GetRange, ObjectMeta, ObjectStoreBackend, PutMode, PutOptions, StoreError, UploadChecksum,
    Version, list_all,
};
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartRef, SnapshotPostingsRef};
use ravel_proto::commit::v1::{CommitRecord, CompactionPart, CompactionRecord, RewriteRecord};
use ravel_segment::{ExpectedIdentity, ReaderLimits, SegmentError};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{METRIC_NAME_LABEL, Signal, TenantHash};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::config::CatalogConfig;
use crate::error::CatalogError;
use crate::provisioning::{DEFAULT_SCAN_SLACK_HOURS, ShardGeneration, scan_count};
use crate::snapshot_format::{
    self, HEAD_FORMAT_VERSION, NamePostings, PartLimits, SnapshotFormatError,
};

/// RSEG section kinds needed to decode a segment's catalog
/// (docs/segment-format.md `SectionKind`). Kinds 1/2 are v1
/// (LABEL_DICT/SERIES_TABLE); 5/6 are v2 (SERIES_IDS/SERIES_META).
/// A failure while building the name-postings index for a fold. Every
/// variant is caught by [`Catalog::build_postings`], logged, and turned into
/// "fold succeeds without a postings ref" ("Postings build failures leave the fold successful without a postings
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

/// Bounded retry budget for HEAD's CAS loop: a folder that keeps losing to concurrent folders
/// gives up with [`CatalogError::FoldCasRetriesExhausted`] rather than
/// retrying forever.
const MAX_HEAD_CAS_ATTEMPTS: u32 = 8;

/// Placeholder for future compaction/retention transactions.
/// `fold`'s entry point accepts a
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
    /// Total entries across every part of the new HEAD (previous entries
    /// plus newly folded ones).
    pub entry_count: u64,
    /// Total encoded size across every part named by the new HEAD, in bytes
    /// (sealed, carried-by-reference, and the fresh tail).
    pub part_bytes: u64,
    /// Number of parts named by the new HEAD (ADR-0063: sealed + carried +
    /// tail). `1` for a single-part fold that never crossed
    /// `snapshot_part_max_entries`.
    pub parts_total: u64,
    /// Of `parts_total`, how many were carried forward from the previous
    /// HEAD by reference (same key, same blake3) rather than re-encoded and
    /// re-PUT (ADR-0063 section 3, "Only changed parts are encoded and PUT").
    /// A fold that touches only recent hours reuses every unchanged sealed
    /// part, so no PUT is issued for it.
    pub parts_reused: u64,
    pub list_requests: u64,
    pub get_requests: u64,
    pub put_requests: u64,
    /// `true` if this fold successfully built and attached a name-postings
    /// index. `false` covers both "no
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
    /// Retention-frontier hours this fold re-listed (ADR-0020 delete-blocker,
    /// the retirement-frontier half). In addition to the fixed
    /// [`CatalogConfig::fold_reconcile_window_hours`](crate::CatalogConfig::fold_reconcile_window_hours)
    /// window behind the watermark, the reconcile pass covers snapshot-named
    /// hours at or approaching the tenant's retention frontier so a tombstone
    /// written `R` days behind the watermark is applied before the retention
    /// sweep's horizon lets it delete the objects that hour's snapshot entries
    /// still name. Zero when the tenant has no durable retention window, on a
    /// first fold, and on a rebuild (which re-derives every hour anyway).
    pub frontier_hours_reconciled: u64,
    /// Retention-frontier hours that were at or approaching the frontier this
    /// fold but exceeded
    /// [`CatalogConfig::frontier_reconcile_max_hours`](crate::CatalogConfig::frontier_reconcile_max_hours)
    /// and were carried to the next fold rather than reconciled now (a jump:
    /// a shortened retention window, or a folder that was stopped for a long
    /// time). Never a silent skip: the deferred hours are still named by the
    /// snapshot, so the next fold recomputes them as frontier candidates and
    /// drains the backlog oldest-first. Nonzero here is the signal that the
    /// frontier is behind; steady state is always zero.
    pub frontier_hours_deferred: u64,
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
    /// HEAD decoded far enough to see a `format_version` this process does
    /// not understand (ADR-0066 decision 2, "fail-closed-on-newer"). Unlike
    /// [`HeadState::Corrupt`], this HEAD is NOT rebuildable: a newer-format
    /// HEAD (e.g. an EH multi-part HEAD written by an already-upgraded peer)
    /// carries state this older process cannot see, so rebuilding a
    /// single-part HEAD and CAS-overwriting it would silently discard that
    /// state. The fold fails loudly instead
    /// ([`CatalogError::UnsupportedHeadVersion`]). Carries only
    /// `format_version`: unlike [`HeadState::Corrupt`], this state never CAS-
    /// writes, so it has no use for the store `Version`.
    UnsupportedVersion {
        format_version: u32,
    },
}

impl HeadState {
    fn watermark_hour(&self) -> Option<u32> {
        match self {
            HeadState::Valid { head, .. } => Some(head.watermark_hour),
            HeadState::Absent
            | HeadState::Corrupt { .. }
            | HeadState::UnsupportedVersion { .. } => None,
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

/// One shard's delimited-listing result during a rebuild's parallel bucket
/// discovery: `(shard, shard_prefix, common_prefixes)` (ADR-0063 section 3).
type ShardListing = (u32, String, Vec<String>);

/// One `(shard, hour)` bucket's discovered keys during the parallel bucket
/// discovery: `(shard, hour, keys)` (ADR-0063 section 3).
type BucketListing = (u32, u32, Vec<ObjectMeta>);

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

/// Build a level-1 [`SnapshotEntry`] for one part of a compaction record. The proto's frozen entry
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

/// Build a level-1 [`SnapshotEntry`] for one part of a [`RewriteRecord`]
/// (ADR-0064 decision 3 point 5: rewrite outputs fold in exactly as compaction
/// parts do). Identical entry shape to [`build_l1_snapshot_entry`]: the frozen
/// entry has no dedicated L1-identity field, so `writer_id` carries the
/// rewrite's 32-byte `input_set_hash` and `writer_epoch` carries the
/// `part_index`. A resolve reconstructs the part key from these via
/// `reconstruct_rewrite_part_key`, matching what a live listing builds
/// (`build_rewrite_l1_segment_ref`).
fn build_rewrite_l1_snapshot_entry(
    key: &str,
    record: &RewriteRecord,
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
    hour_range_buckets(generations, watermark_hour_old + 1, watermark_hour_new)
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

/// One part's span in the fully-sorted entry set: the `[start, end)` index
/// range into `entries` and the `[min_hour, watermark_hour]` ingest-hour range
/// those entries occupy (ADR-0063 section 1). Ranges are disjoint and
/// ascending across the returned spans because every whole hour lands in
/// exactly one part and parts are cut only at hour boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartSpan {
    start: usize,
    end: usize,
    min_hour: u32,
    watermark_hour: u32,
}

/// Partition the fully hour-major-sorted `entries` into hour-range parts under
/// the sealing policy (ADR-0063 section 1): whole hours accumulate into the
/// current part until adding the next hour's run would push the part's entry
/// count past `max_entries`, at which point the part seals at that hour
/// boundary and a fresh part starts. Splits are always at hour boundaries, so
/// a single hour larger than `max_entries` produces one oversized part (the
/// decode cap still bounds it) rather than being split across parts -- which
/// would break the free per-part duplicate-freedom proof, since the dedup
/// identity leads with the hour.
///
/// Always returns at least one span. An empty entry set yields a single empty
/// `[0, 0)` span with `min_hour`/`watermark_hour` 0, so an empty fold still
/// writes one (empty) part exactly as the single-part path always has. Each
/// span's `watermark_hour` here is the last ingest hour it actually contains;
/// the caller overrides the tail span's watermark with the fold watermark.
fn partition_parts(entries: &[SnapshotEntry], max_entries: usize) -> Vec<PartSpan> {
    if entries.is_empty() {
        return vec![PartSpan {
            start: 0,
            end: 0,
            min_hour: 0,
            watermark_hour: 0,
        }];
    }
    // `max_entries == 0` would seal after every hour and never make progress;
    // clamp to 1 so a part always holds at least one whole hour.
    let cap = max_entries.max(1);
    let mut spans = Vec::new();
    let mut cur_start = 0usize;
    let mut cur_count = 0usize;
    let mut cur_min_hour = entries[0].ingest_hour_bucket;
    let mut i = 0usize;
    while i < entries.len() {
        // The contiguous run of entries sharing this ingest hour (entries are
        // hour-major sorted, so equal hours are adjacent).
        let hour = entries[i].ingest_hour_bucket;
        let run_start = i;
        while i < entries.len() && entries[i].ingest_hour_bucket == hour {
            i += 1;
        }
        let run_len = i - run_start;
        // Seal the current part at this hour boundary if it already holds
        // entries and admitting this whole hour would exceed the cap.
        if cur_count > 0 && cur_count + run_len > cap {
            spans.push(PartSpan {
                start: cur_start,
                end: run_start,
                min_hour: cur_min_hour,
                watermark_hour: entries[run_start - 1].ingest_hour_bucket,
            });
            cur_start = run_start;
            cur_count = 0;
            cur_min_hour = hour;
        }
        cur_count += run_len;
    }
    spans.push(PartSpan {
        start: cur_start,
        end: entries.len(),
        min_hour: cur_min_hour,
        watermark_hour: entries[entries.len() - 1].ingest_hour_bucket,
    });
    spans
}

/// One `(shard, hour)` commit bucket's contribution to the fold: each entry it
/// should fold in paired with that entry's `is_canonical` flag (whether the
/// occurrence sits under the hour directory its own embedded
/// `ingest_hour_bucket` names). Compaction L1 parts first, then unsuperseded L0
/// commit records; empty when the bucket is tombstoned or holds nothing
/// foldable. Produced by the shared [`Catalog::classify_bucket`] so both the
/// incremental fold loop and the reconcile pass (ADR-0063 section 4) derive a
/// bucket's state one way only, never two subtly different ways.
type BucketContribution = Vec<(SnapshotEntry, bool)>;

/// The reconcile pass only needs to touch a window bucket whose commit-record
/// set could differ from what a past fold folded. The seal lemma
/// (docs/catalog-and-mvcc.md "Sealed hours") makes the L0 set of a sealed
/// bucket immutable, so a bucket holding only L0 records is guaranteed
/// unchanged and can be skipped without a GET. A compaction record, a
/// retention tombstone, or a selective-erasure rewrite record (ADR-0064
/// decision 3) can all land after an hour was sealed and folded, so those
/// three shapes are the reconcile triggers.
///
/// A rewrite record is load-bearing here, not an incidental addition: the
/// rewrite pass targets already-sealed (already-folded) buckets by
/// construction (ADR-0064 §3.1 scopes it to sealed buckets), so without this
/// trigger a rewrite would never be picked up by the incremental fold path at
/// all -- the folded snapshot would keep serving the rewrite's pre-erasure
/// inputs indefinitely for any hour outside `incremental_buckets`' range,
/// since those objects stay physically present (GET-able) until the
/// horizon-gated sweep runs, so there is no NotFound-driven re-resolve to
/// force a refresh either. This is the mechanism ADR-0064 §4's "the next
/// fold rebuilds snapshots over the rewrite outputs" claim depends on; that
/// claim is honest only within `fold_reconcile_window_hours` of the fold
/// that observes it -- see the amendment note in
/// docs/adrs/0064-selective-subject-erasure.md §4.
fn bucket_needs_reconcile(listing: &[ObjectMeta]) -> bool {
    listing.iter().any(|meta| {
        matches!(
            keys::partition_bucket_entry(&meta.key),
            Ok(BucketEntry::CompactionRecord(_))
                | Ok(BucketEntry::Tombstone(_))
                | Ok(BucketEntry::RewriteRecord(_))
        )
    })
}

/// Dedup one bucket's classified contribution by entry identity, preferring
/// the canonical occurrence, reusing [`fold_in_entry`]'s exact rule so the
/// reconcile pass and the incremental path resolve a within-bucket duplicate
/// identically. Within a single bucket a duplicate identity is near-impossible
/// (it would take two records with the same identity in one directory), so the
/// throwaway drift counter here is expected to stay zero.
fn dedup_contribution(items: Vec<(SnapshotEntry, bool)>) -> Vec<SnapshotEntry> {
    let mut out: Vec<SnapshotEntry> = Vec::new();
    let mut seen: HashMap<EntryIdentity, (usize, bool)> = HashMap::new();
    let mut drift = 0u64;
    for (entry, is_canonical) in items {
        fold_in_entry(&mut out, &mut seen, &mut drift, entry, is_canonical);
    }
    out
}

/// Whether two entry sets for one bucket are identical ignoring order: same
/// identities and, for each, byte-identical [`SnapshotEntry`] content. Used by
/// the reconcile pass to decide whether a re-listed bucket's current
/// contribution differs from what the in-progress `entries` already reflect.
/// Sorting by identity is safe: identity is unique within a deduped
/// contribution, so the sort is total and the elementwise compare is exact.
fn same_entry_set(a: &mut [SnapshotEntry], b: &mut [SnapshotEntry]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.sort_by_key(entry_identity);
    b.sort_by_key(entry_identity);
    a == b
}

/// `(shard, hour)` pairs for every hour in the inclusive range `[lo, hi]`, one
/// per shard in that hour's own `scan_count(h)` fan-out (ADR-0052 section 4).
/// `lo > hi` yields no buckets. Shared by the incremental range
/// ([`incremental_buckets`]) and the reconcile window (ADR-0063 section 4) so
/// both enumerate exactly the shard set a live resolve would list.
fn hour_range_buckets(generations: &[ShardGeneration], lo: u32, hi: u32) -> Vec<(u32, u32)> {
    let mut buckets = Vec::new();
    for hour in lo..=hi {
        let scan = scan_count(generations, hour, DEFAULT_SCAN_SLACK_HOURS);
        for shard in 0..scan {
            buckets.push((shard, hour));
        }
    }
    buckets
}

/// The newest ingest hour at or approaching the retention frontier for a
/// tenant with retention window `R` (`retention_window_ns`) and protection
/// horizon `protection_horizon_ns` at fold time `now_ns` (ADR-0020
/// delete-blocker, retirement-frontier half). Retention (ADR-0019) tombstones
/// a bucket once its newest event is older than `R`; conservatively every hour
/// whose end is at or before `now - R` may already be tombstoned, so `now - R`
/// is the frontier. The `+ protection_horizon` look-ahead extends it so hours
/// approaching the frontier are pre-listed and an appearing tombstone is caught
/// the fold it lands, a full horizon before the sweep may delete the objects
/// that hour still names. `None` when the cutoff predates the epoch (no hour is
/// near the frontier yet).
fn retirement_frontier_hour(
    now_ns: i64,
    retention_window_ns: i64,
    protection_horizon_ns: i64,
) -> Option<u32> {
    let cutoff_ns = now_ns
        .saturating_sub(retention_window_ns)
        .saturating_add(protection_horizon_ns);
    if cutoff_ns < 0 {
        return None;
    }
    u32::try_from(cutoff_ns.div_euclid(NS_PER_HOUR)).ok()
}

/// `(shard, hour)` pairs for an explicit, arbitrary set of ingest hours, one
/// per shard in each hour's own `scan_count(h)` fan-out (ADR-0052 section 4).
/// The retention-frontier reconcile (ADR-0020) needs the shard buckets for a
/// sparse, non-contiguous hour set (the snapshot-named hours at or below the
/// frontier), which [`hour_range_buckets`]' contiguous `[lo, hi]` range cannot
/// express.
fn frontier_hour_set_buckets(generations: &[ShardGeneration], hours: &[u32]) -> Vec<(u32, u32)> {
    let mut buckets = Vec::new();
    for &hour in hours {
        let scan = scan_count(generations, hour, DEFAULT_SCAN_SLACK_HOURS);
        for shard in 0..scan {
            buckets.push((shard, hour));
        }
    }
    buckets
}

/// Whether a previous HEAD's part covers any dirty reconcile hour, so it must
/// NOT be carried forward by reference this fold (ADR-0063 section 4). A dirty
/// part is always re-encoded and re-PUT through the normal path: fail toward
/// re-PUTting fresh content, never toward silently keeping a stale part, even
/// in the (impossible-by-content-addressing) case where the recomputed hash
/// still coincided with the old ref.
fn part_covers_dirty_hour(part: &SnapshotPartRef, dirty_hours: &HashSet<u32>) -> bool {
    dirty_hours
        .iter()
        .any(|&h| part.min_hour <= h && h <= part.watermark_hour)
}

impl Catalog {
    /// Fold previous snapshot parts plus newly sealed commit buckets into a
    /// new snapshot part and CAS-swap HEAD to name it.
    /// Never runs on the ingest or
    /// query path.
    ///
    /// `now_ns` and `folder_id` are always caller-supplied: this crate never
    /// reads a clock or generates randomness. `folder_id` should be a fresh
    /// UUIDv4 per folder process start (proto/ravel/catalog.proto,
    /// `SnapshotHead.folder_id`).
    ///
    /// Compaction and retention are folded in from the same per-bucket
    /// listing this fold already performs: a bucket's `CompactionRecord`s contribute their parts as
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
        // shared cache/load API's `QueryAccounting` parameter and is discarded.
        let accounting = QueryAccounting::new();

        // The tenant's durable retention window (TenantConfig.retention_ns),
        // read once per fold (loop-invariant). Bounds the retention-frontier
        // reconcile pass below (ADR-0020 delete-blocker). A tenant with no
        // per-tenant window, or a transient config-read fault, degrades to "no
        // frontier reconcile this fold": the fold's index role is a pure
        // optimization and the sweep's HEAD-reachability gate is the durable
        // delete blocker, so this never fails the fold. (A tenant retired only
        // by a deployment-wide RetentionConfig default, with no per-tenant
        // config record, is not frontier-reconciled here; its sweep still
        // blocks safely on the HEAD gate.)
        let tenant_retention_ns: Option<i64> =
            match crate::tenant_config::read_config_values(self.store(), tenant).await {
                Ok(Some(cfg)) => cfg.retention_ns,
                Ok(None) => None,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        tenant = %tenant.to_hex(),
                        "tenant config read failed; skipping retention-frontier reconcile this fold"
                    );
                    None
                }
            };

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
                // A newer-format HEAD is not rebuildable (ADR-0066 decision
                // 2): rebuilding a single-part HEAD here would CAS-overwrite
                // whatever the newer format carries. Fail loudly so the
                // operator sees this process is too old for this HEAD, rather
                // than falling into the `Corrupt`/`Absent` rebuild path.
                HeadState::UnsupportedVersion { format_version, .. } => {
                    return Err(CatalogError::UnsupportedHeadVersion {
                        format_version: *format_version,
                    });
                }
            };

            // The old watermark the reconcile pass anchors its window to,
            // present ONLY for an incremental fold from a trusted HEAD. Absent
            // (`None`) on the first fold (no previous watermark) and on any
            // rebuild (the rebuild already re-derived every hour), which
            // together skip the reconcile pass entirely (ADR-0063 section 4).
            let reconcile_watermark: Option<u32> = match &head_state {
                HeadState::Valid { head, .. } if !rebuilt => Some(head.watermark_hour),
                _ => None,
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

            // Discover each (shard, hour) bucket's keys concurrently, bounded
            // by `fold_bucket_concurrency` (ADR-0063 section 3), mirroring the
            // resolve path's `buffered` fan-out. `buffered`
            // preserves input order, so the discovered buckets are merged in
            // the same deterministic bucket order the serial loop used, keeping
            // the fold byte-for-byte reproducible (content addressing depends
            // on it). Only the LIST discovery is parallel; per-record GETs and
            // the dedup merge below stay serial in bucket order.
            let bucket_listings = self
                .discover_bucket_listings(tenant, signal, &buckets)
                .await?;
            counters.list_requests += buckets.len() as u64;

            for (shard, hour, listing) in &bucket_listings {
                // Classify the bucket into its contribution via the shared
                // classifier the reconcile pass also uses (ADR-0063 section 4,
                // ADR-0064 decision 3), then fold each contributed entry into
                // the running set. A tombstoned bucket classifies to an empty
                // contribution, so it folds nothing in (ADR-0019 section 3).
                let contribution = self
                    .classify_bucket(
                        tenant,
                        signal,
                        *shard,
                        *hour,
                        listing,
                        &accounting,
                        &mut counters,
                        &mut layout_drift_count,
                    )
                    .await?;
                for (entry, is_canonical) in contribution {
                    fold_in_entry(
                        &mut entries,
                        &mut seen,
                        &mut layout_drift_count,
                        entry,
                        is_canonical,
                    );
                }
            }

            // ---- Reconcile pass (ADR-0063 section 4) ----
            //
            // `incremental_buckets` only lists hours STRICTLY AFTER the
            // previous fold's watermark, so a compaction record or retention
            // tombstone published into an hour that was already sealed and
            // folded into a PAST fold's output is never rediscovered by any
            // later incremental fold. Re-list a bounded window of already-
            // sealed hours and diff each bucket's current contribution against
            // what the in-progress `entries` (seeded from the previous fold's
            // own output for every hour outside the incremental range) already
            // reflect. A bucket whose contribution changed updates `entries`
            // and marks its covering part dirty so it is re-encoded and re-PUT
            // rather than carried forward by reference. The findings fold into
            // THIS fold attempt's single HEAD CAS below, never a second CAS.
            //
            // Skipped on the first fold for a tenant (`Absent`: no previous
            // watermark exists) and on a rebuilt fold (`rebuilt`: the rebuild
            // already re-derives every hour from the commit layout, so a
            // reconcile pass would redo the same work over the same buckets).
            let mut dirty_hours: HashSet<u32> = HashSet::new();
            let mut frontier_hours_reconciled: u64 = 0;
            let mut frontier_hours_deferred: u64 = 0;
            if let Some(watermark_hour_old) = reconcile_watermark {
                let window = self.config().fold_reconcile_window_hours;
                let lo = watermark_hour_old.saturating_sub(window);
                // Inclusive at both ends: `watermark_hour_old` is the boundary
                // `incremental_buckets` already excludes, so the reconcile
                // window and the incremental range are adjacent, not
                // overlapping.
                let reconcile_buckets = hour_range_buckets(&generations, lo, watermark_hour_old);
                let reconcile_listings = self
                    .discover_bucket_listings(tenant, signal, &reconcile_buckets)
                    .await?;
                counters.list_requests += reconcile_buckets.len() as u64;

                for (shard, hour, listing) in &reconcile_listings {
                    self.reconcile_one_bucket(
                        tenant,
                        signal,
                        *shard,
                        *hour,
                        listing,
                        &accounting,
                        &mut entries,
                        &mut seen,
                        &mut dirty_hours,
                        &mut counters,
                        &mut layout_drift_count,
                    )
                    .await?;
                }

                // ---- Retention-frontier reconcile (ADR-0020 delete-blocker) ----
                //
                // The fixed window above reaches only `fold_reconcile_window_hours`
                // behind the watermark. A retention tombstone (ADR-0019) is
                // written at its own bucket's ingest-hour key, which is `R` (the
                // tenant's retention window) behind the watermark -- far outside
                // that window for any realistic `R`. Left unobserved, the snapshot
                // keeps naming the retired bucket's segments forever, and the
                // horizon-gated physical sweep would (absent its own HEAD gate)
                // delete objects a HEAD-referenced snapshot still names. So the
                // fold ALSO reconciles the bounded set of snapshot-named hours at
                // or approaching the tenant's retirement frontier. This is the
                // fold half of ADR-0020's "GC must treat reachability from
                // HEAD-referenced snapshots (within the protection horizon) as a
                // delete blocker": it makes the sweep's block clear on its own by
                // dropping a tombstoned bucket from the snapshot promptly, rather
                // than leaving the sweep permanently blocked on a bucket no fold
                // ever drops.
                //
                // Frontier derivation (docs/catalog-and-mvcc.md): a bucket whose
                // end is at or before `now - R` may already be tombstoned;
                // `frontier_hi` pushes that forward by the protection horizon so
                // hours *approaching* the frontier are pre-listed and an appearing
                // tombstone is caught the fold it lands, a full horizon before the
                // sweep may delete. Candidates are the snapshot-named hours at or
                // below `frontier_hi` and below the fixed window (which already
                // covers the near-watermark region), reconciled oldest-first (an
                // object is deletable `protection_horizon` after its tombstone, so
                // the oldest still-named hours are closest to deletion) and capped
                // at `frontier_reconcile_max_hours`; the remainder is carried to
                // the next fold via `frontier_hours_deferred` (the snapshot still
                // names those hours, so the next fold recomputes them), never
                // dropped. `tenant_retention_ns` is the tenant's durable
                // TenantConfig.retention_ns; a tenant with no per-tenant window
                // gets no frontier reconcile (nothing is being retired).
                if let Some(retention_window_ns) = tenant_retention_ns
                    && let Some(frontier_hi_raw) = retirement_frontier_hour(
                        now_ns,
                        retention_window_ns,
                        self.config().protection_horizon_ns,
                    )
                {
                    // Cap the band's top strictly below the fixed window so no
                    // bucket is listed by both passes.
                    let frontier_hi = frontier_hi_raw.min(lo.saturating_sub(1));
                    let mut candidate_hours: Vec<u32> = {
                        let mut hs: HashSet<u32> = HashSet::new();
                        for entry in entries.iter() {
                            if entry.ingest_hour_bucket <= frontier_hi {
                                hs.insert(entry.ingest_hour_bucket);
                            }
                        }
                        hs.into_iter().collect()
                    };
                    // Oldest-first: the hours closest to physical deletion are
                    // reconciled before the cap is spent.
                    candidate_hours.sort_unstable();
                    let cap = self.config().frontier_reconcile_max_hours as usize;
                    let take = candidate_hours.len().min(cap);
                    frontier_hours_deferred = (candidate_hours.len() - take) as u64;
                    let frontier_hours = &candidate_hours[..take];
                    if !frontier_hours.is_empty() {
                        let frontier_buckets =
                            frontier_hour_set_buckets(&generations, frontier_hours);
                        let frontier_listings = self
                            .discover_bucket_listings(tenant, signal, &frontier_buckets)
                            .await?;
                        counters.list_requests += frontier_buckets.len() as u64;
                        frontier_hours_reconciled = frontier_hours.len() as u64;
                        for (shard, hour, listing) in &frontier_listings {
                            self.reconcile_one_bucket(
                                tenant,
                                signal,
                                *shard,
                                *hour,
                                listing,
                                &accounting,
                                &mut entries,
                                &mut seen,
                                &mut dirty_hours,
                                &mut counters,
                                &mut layout_drift_count,
                            )
                            .await?;
                        }
                    }
                }
            }
            // A reconcile that changed any hour invalidates the append-only,
            // stable-ordinal assumption the forward postings merge relies on
            // (a superseded or removed entry shifts every later ordinal), so
            // postings must be rebuilt from scratch this fold, exactly as on
            // the rebuild path.
            let reconciled = !dirty_hours.is_empty();

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

            // Partition the fully-sorted entry set into hour-range parts under
            // the sealing policy (ADR-0063 section 1): whole hours accumulate
            // into the current part until adding the next hour would cross
            // `snapshot_part_max_entries`, then the part seals at that hour
            // boundary and a fresh tail starts. The concatenation of
            // range-ordered parts is exactly the globally sorted entry set, so
            // postings ordinals still index that concatenation and global
            // duplicate-freedom follows from per-part validation plus range
            // disjointness (no cross-part entry pass is needed).
            let spans = partition_parts(&entries, self.config().snapshot_part_max_entries);

            // Sealed parts unchanged since the previous HEAD are carried by
            // reference, never re-encoded or re-PUT (ADR-0063 section 3). The
            // "unchanged" signal is content addressing: a part recomputed from
            // the same entries over the same [min_hour, watermark] range
            // encodes to byte-identical output and therefore the same blake3,
            // so a hash already present in the previous HEAD names an object
            // already in the store under the same content-addressed key. Only
            // a part whose covering hours actually changed produces a new hash
            // and is PUT.
            //
            // `!rebuilt` is load-bearing: when `load_previous_entries` failed
            // above and this fold rebuilt from the commit layout instead
            // (missing or corrupt sealed part object), reusing the old HEAD's
            // refs here would recompute a byte-identical hash for a part that
            // does NOT exist in the store, skip its PUT, and leave the new
            // HEAD permanently naming a dead object -- defeating the
            // PUT-and-get-AlreadyExists self-heal ADR-0063 section 5
            // describes, which only works if the object actually exists.
            //
            // A part the reconcile pass marked dirty (its covering hours'
            // contribution changed) is excluded from this reuse map, so it can
            // never be carried forward even if its recomputed hash somehow
            // still matched the old ref: a dirty part always takes the normal
            // re-encode-and-PUT path (ADR-0063 section 4). In practice the
            // changed content already yields a different hash, so this is a
            // fail-safe, not the primary mechanism.
            let existing_by_blake3: HashMap<Vec<u8>, SnapshotPartRef> = match &head_state {
                HeadState::Valid { head, .. } if !rebuilt => head
                    .parts
                    .iter()
                    .filter(|p| !part_covers_dirty_hour(p, &dirty_hours))
                    .map(|p| (p.blake3.clone(), p.clone()))
                    .collect(),
                _ => HashMap::new(),
            };

            let single_part = spans.len() == 1;
            let mut part_refs: Vec<SnapshotPartRef> = Vec::with_capacity(spans.len());
            let mut part_hashes: Vec<[u8; 32]> = Vec::with_capacity(spans.len());
            let mut total_part_bytes: u64 = 0;
            let mut parts_reused: u64 = 0;
            for (span_index, span) in spans.iter().enumerate() {
                let is_tail = span_index + 1 == spans.len();
                // Single-part fold keeps exact v1 semantics: min_hour 0 (the
                // epoch floor) and the fold watermark, byte-identical to the
                // legacy encode so existing single-part objects and their
                // content addresses never change. Multi-part: each part carries
                // its real first hour as min_hour; sealed parts end at their
                // last contained hour, and only the tail carries the fold
                // watermark, so HEAD.watermark == max part watermark == fold
                // watermark (the `validate_head` contract).
                let (part_min_hour, part_watermark) = if single_part {
                    (0, watermark_hour)
                } else if is_tail {
                    (span.min_hour, watermark_hour)
                } else {
                    (span.min_hour, span.watermark_hour)
                };
                let part_entries = &entries[span.start..span.end];
                let part_bytes = snapshot_format::encode_part_ranged(
                    tenant.0,
                    signal_num,
                    shard_ceiling,
                    part_min_hour,
                    part_watermark,
                    part_entries,
                )?;
                let part_hash = blake3::hash(&part_bytes);
                if let Some(existing) = existing_by_blake3.get(part_hash.as_bytes().as_slice()) {
                    // Carried by reference: the previous HEAD already names an
                    // object with these exact bytes, so no PUT is issued and
                    // the existing ref (same key/size/entry_count/range) is
                    // forwarded unchanged.
                    total_part_bytes += existing.size;
                    part_hashes.push(*part_hash.as_bytes());
                    part_refs.push(existing.clone());
                    parts_reused += 1;
                    continue;
                }
                let part_bytes_len = part_bytes.len() as u64;
                let part_crc = crc32c::crc32c(&part_bytes);
                let hash16 = &part_hash.to_hex()[..16];
                let part_key = part_object_key(tenant, signal, part_watermark, hash16);
                match self
                    .store()
                    .put(
                        &part_key,
                        Bytes::from(part_bytes),
                        PutOptions::create_if_absent()
                            .with_checksum(UploadChecksum::Crc32c(part_crc)),
                    )
                    .await
                {
                    Ok(_) => {}
                    // Content-addressed key: bytes are identical by
                    // construction, so a losing folder's part is as good as
                    // its own (mirrors `publish::put_data_object`).
                    Err(StoreError::AlreadyExists) => {}
                    Err(e) => return Err(CatalogError::Store(e)),
                }
                counters.put_requests += 1;
                total_part_bytes += part_bytes_len;
                part_hashes.push(*part_hash.as_bytes());
                part_refs.push(SnapshotPartRef {
                    key: part_key,
                    blake3: part_hash.as_bytes().to_vec(),
                    size: part_bytes_len,
                    entry_count: part_entries.len() as u64,
                    watermark_hour: part_watermark,
                    min_hour: part_min_hour,
                });
            }

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
            // the whole index, never publishes a partial one.
            let postings_names = if entries.iter().all(|entry| entry.level == 0) {
                let (postings_baseline, postings_decode_start): (Vec<NamePostings>, usize) =
                    if rebuilt || reconciled {
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
                // exactly rather than pruning against a stale or partial index.
                None
            };
            let mut postings_built = false;
            let mut postings_size = 0u64;
            let postings_ref = match postings_names {
                Some(names) => match snapshot_format::encode_postings(
                    tenant.0,
                    signal_num,
                    &part_hashes,
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
                                    part_blake3: part_hashes.iter().map(|h| h.to_vec()).collect(),
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
                // Sealed parts (ascending, disjoint by construction), any
                // carried-by-reference sealed parts, and the fresh tail, in
                // range order (ADR-0063 section 1). `partition_parts` builds
                // them min_hour-ascending with disjoint ranges, exactly what
                // `validate_head` enforces for a multi-part head.
                parts: part_refs.clone(),
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
                // Unreachable in practice: the rebuild-decision match above
                // returns before we build a HEAD to PUT. Handled explicitly
                // (never a CAS write) so a future refactor cannot let a
                // newer-format HEAD reach a clobbering PUT (ADR-0066
                // decision 2).
                HeadState::UnsupportedVersion { format_version, .. } => {
                    return Err(CatalogError::UnsupportedHeadVersion {
                        format_version: *format_version,
                    });
                }
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
                        part_bytes: total_part_bytes,
                        parts_total: part_refs.len() as u64,
                        parts_reused,
                        list_requests: counters.list_requests,
                        get_requests: counters.get_requests,
                        put_requests: counters.put_requests,
                        postings_built,
                        postings_bytes: postings_size,
                        layout_drift_count,
                        frontier_hours_reconciled,
                        frontier_hours_deferred,
                    });
                }
                // Another folder's HEAD CAS won first. Re-GET HEAD next
                // iteration: if the winner's watermark already covers ours,
                // the top-of-loop no-op check stops cleanly; otherwise we
                // rebase onto the winner's parts and retry.
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
                    // A HEAD whose `format_version` this process does not
                    // understand is NOT corruption and must NOT be rebuilt
                    // (ADR-0066 decision 2, "fail-closed-on-newer"): rebuilding
                    // would CAS-clobber a newer HEAD written by an upgraded
                    // peer. Keep it a distinct state so the fold fails loudly.
                    Err(SnapshotFormatError::UnsupportedHeadVersion(format_version)) => {
                        tracing::error!(
                            key = %head_key,
                            format_version,
                            "HEAD is a newer format than this process understands; refusing to rebuild"
                        );
                        Ok(HeadState::UnsupportedVersion { format_version })
                    }
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
    /// it as a merge baseline: same
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

    /// Build the name-postings index for `entries`, merging forward from `baseline` rather than re-decoding every
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
        let tenant_c = *tenant;
        let concurrency = self.config().fold_bucket_concurrency.max(1);
        // Fan out the per-shard delimited LISTs concurrently under
        // `fold_bucket_concurrency` (ADR-0063 section 3); parse and filter
        // serially afterwards so the result stays deterministic.
        let per_shard: Vec<Result<ShardListing, CatalogError>> = stream::iter(0..scan_shards)
            .map(|shard| async move {
                let prefix = keys::commit_shard_prefix(&tenant_c, signal, shard)?;
                let listing = self.store().list_delimited(&prefix).await?;
                Ok::<_, CatalogError>((shard, prefix, listing.common_prefixes))
            })
            .buffered(concurrency)
            .collect()
            .await;
        counters.list_requests += u64::from(scan_shards);
        let mut buckets = Vec::new();
        for res in per_shard {
            let (shard, prefix, common_prefixes) = res?;
            for common_prefix in &common_prefixes {
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

    /// Reconcile one already-sealed, already-folded bucket against its current
    /// on-store state, replacing exactly this bucket's entries in the
    /// in-progress `entries` set when its contribution changed and marking the
    /// hour dirty. Shared by the fixed reconcile window and the
    /// retention-frontier reconcile (ADR-0063 section 4, ADR-0020
    /// delete-blocker) so both derive a re-listed bucket's state one way only.
    #[allow(clippy::too_many_arguments)]
    async fn reconcile_one_bucket(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        hour: u32,
        listing: &[ObjectMeta],
        accounting: &QueryAccounting,
        entries: &mut Vec<SnapshotEntry>,
        seen: &mut HashMap<EntryIdentity, (usize, bool)>,
        dirty_hours: &mut HashSet<u32>,
        counters: &mut RequestCounters,
        layout_drift_count: &mut u64,
    ) -> Result<(), CatalogError> {
        // A bucket with only immutable L0 records cannot have changed since it
        // was folded (seal lemma). Skip it with no GET; only a late compaction
        // record, rewrite record, or retention tombstone triggers real
        // reconcile work.
        if !bucket_needs_reconcile(listing) {
            return Ok(());
        }
        let contribution = self
            .classify_bucket(
                tenant,
                signal,
                shard,
                hour,
                listing,
                accounting,
                counters,
                layout_drift_count,
            )
            .await?;
        let mut desired = dedup_contribution(contribution);
        // What `entries` currently reflects for this (shard, hour): found by
        // each entry's own shard and ingest_hour_bucket.
        let mut current: Vec<SnapshotEntry> = entries
            .iter()
            .filter(|e| e.shard == shard && e.ingest_hour_bucket == hour)
            .cloned()
            .collect();
        if same_entry_set(&mut current, &mut desired) {
            return Ok(());
        }
        // The bucket's contribution changed (a late compaction superseded L0
        // inputs previously folded in directly, or a late tombstone means the
        // hour now contributes nothing): replace exactly this bucket's entries,
        // leave every other entry untouched, and mark the hour dirty.
        entries.retain(|e| !(e.shard == shard && e.ingest_hour_bucket == hour));
        // `seen`'s cached indices go stale the instant `entries` is mutated by
        // anything other than `fold_in_entry` (the `retain` above shifts every
        // later index); rebuild it from the post-retain entries, seeded as
        // trivially canonical, so the fold-in below can detect a `desired`
        // identity that already exists elsewhere in `entries` (a drift-relocated
        // commit record present under its true home bucket) instead of a blind
        // `extend` that would insert a duplicate identity and break every later
        // fold with `SnapshotFormat(DuplicateEntry)`. `fold_in_entry` resolves
        // the collision exactly as the incremental loop does: the resident
        // occurrence wins, the duplicate is dropped and counted as layout drift.
        *seen = entries
            .iter()
            .enumerate()
            .map(|(index, entry)| (entry_identity(entry), (index, true)))
            .collect();
        for entry in desired {
            let is_canonical = entry.ingest_hour_bucket == hour;
            fold_in_entry(entries, seen, layout_drift_count, entry, is_canonical);
        }
        dirty_hours.insert(hour);
        Ok(())
    }

    /// Classify one `(shard, hour)` commit bucket's listing into the entries
    /// it should contribute to the snapshot, the single shared classifier for both the incremental fold loop and
    /// the reconcile pass (ADR-0063 section 4). Partitioning the bucket's keys
    /// by shape mirrors the resolve-time listing (`Catalog::list_hour_bucket`,
    /// docs/catalog-and-mvcc.md step 2) so a fold and a live resolve derive
    /// identical bucket state.
    ///
    /// A retention tombstone makes the bucket contribute nothing: neither its
    /// L1 parts nor its L0 records are folded in (ADR-0019 section 3). Each
    /// compaction record contributes its parts as level-1 entries and
    /// supersedes its named L0 inputs; two records with different input sets in
    /// one bucket both contribute (a harmless overlap the resolver alarms on
    /// but still serves). A selective-erasure rewrite record (ADR-0064
    /// decision 3, amended) contributes its own output parts as level-1
    /// entries and supersedes either its named L0 inputs directly, or -- when
    /// it names a `superseded_record_key` instead -- whatever that
    /// compaction/rewrite predecessor's own inputs are, chased recursively via
    /// [`crate::catalog::resolve_rewrite_supersession`] (the same helper
    /// snapshot resolution uses, so a fold and a live resolve can never
    /// disagree on which records an erasure has superseded). A rewrite itself
    /// superseded by a later one (a bucket erased twice) contributes nothing.
    /// Every L0 not named by a compaction or rewrite input list is contributed
    /// exactly as the resolver includes it. Unrecognized key shapes are
    /// counted as layout drift and skipped, never fatal.
    #[allow(clippy::too_many_arguments)]
    async fn classify_bucket(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        shard: u32,
        hour: u32,
        listing: &[ObjectMeta],
        accounting: &QueryAccounting,
        counters: &mut RequestCounters,
        layout_drift_count: &mut u64,
    ) -> Result<BucketContribution, CatalogError> {
        let mut l0_keys: Vec<&str> = Vec::new();
        let mut compaction_keys: Vec<&str> = Vec::new();
        let mut rewrite_keys: Vec<&str> = Vec::new();
        let mut has_tombstone = false;
        for meta in listing {
            match keys::partition_bucket_entry(&meta.key) {
                Ok(BucketEntry::CommitRecord(_)) => l0_keys.push(meta.key.as_str()),
                Ok(BucketEntry::CompactionRecord(_)) => compaction_keys.push(meta.key.as_str()),
                // Selective-erasure rewrite record (ADR-0064 decision 3).
                // Recognized here rather than warn-skipped: a silently
                // skipped rewrite record's supersession would be ignored by
                // the fold and could resurrect an erased subject's
                // pre-rewrite records into a folded snapshot.
                Ok(BucketEntry::RewriteRecord(_)) => rewrite_keys.push(meta.key.as_str()),
                Ok(BucketEntry::Tombstone(_)) => has_tombstone = true,
                Err(err) => {
                    *layout_drift_count += 1;
                    tracing::warn!(
                        error = %err,
                        key = %meta.key,
                        "unrecognized bucket-key shape, skipping key"
                    );
                }
            }
        }

        if has_tombstone {
            return Ok(Vec::new());
        }

        let mut contributed: BucketContribution = Vec::new();
        let mut excluded: HashSet<(String, u64, u64)> = HashSet::new();
        let mut compaction_records: Vec<(String, Arc<CompactionRecord>)> = Vec::new();
        for ckey in &compaction_keys {
            let record = self
                .load_and_validate_compaction(tenant, signal, shard, ckey, accounting)
                .await?;
            counters.get_requests += 1;
            for input in &record.inputs {
                excluded.insert((
                    input.writer_id.clone(),
                    input.writer_epoch,
                    input.writer_seq,
                ));
            }
            compaction_records.push(((*ckey).to_string(), record));
        }

        // Load rewrite records (ADR-0064 decision 3), verified against their
        // own identity inside `load_and_validate_rewrite`.
        let mut rewrite_records: Vec<(String, Arc<RewriteRecord>)> = Vec::new();
        for rkey in &rewrite_keys {
            let record = self
                .load_and_validate_rewrite(tenant, signal, shard, rkey, accounting)
                .await?;
            counters.get_requests += 1;
            rewrite_records.push(((*rkey).to_string(), record));
        }

        // Resolve every rewrite's supersession into the SAME `excluded` set
        // (its effective L0 inputs) and a `superseded_records` set (keys of
        // compaction/rewrite records superseded as a whole, whose parts must
        // therefore be skipped). This is what stops an erased subject's
        // pre-rewrite records from resurfacing in the fold. Identical
        // mechanism to snapshot resolution (`process_bucket`), sharing one
        // chase helper.
        let mut superseded_records: HashSet<String> = HashSet::new();
        if !rewrite_records.is_empty() {
            let bucket_label = keys::commit_shard_hour_prefix(tenant, signal, shard, hour)?;
            let compaction_by_key: HashMap<&str, &CompactionRecord> = compaction_records
                .iter()
                .map(|(k, r)| (k.as_str(), r.as_ref()))
                .collect();
            let rewrite_by_key: HashMap<&str, &RewriteRecord> = rewrite_records
                .iter()
                .map(|(k, r)| (k.as_str(), r.as_ref()))
                .collect();
            for (rkey, record) in &rewrite_records {
                crate::catalog::resolve_rewrite_supersession(
                    rkey,
                    record,
                    &bucket_label,
                    &compaction_by_key,
                    &rewrite_by_key,
                    &mut excluded,
                    &mut superseded_records,
                )?;
            }
        }

        // Sibling-rewrite alarm, raised through the same helper snapshot
        // resolution uses. The fold is the path that matters most here: a
        // rewrite always lands in an already-sealed bucket (ADR-0064 §3.1),
        // and once that hour is folded, a query is served from the snapshot
        // without ever calling `process_bucket`, so this is the only place the
        // conflict would ever be observed for the ordinary case.
        self.check_rewrite_siblings(shard, hour, &rewrite_records, &superseded_records);

        // Compaction parts: contribute each non-superseded record's parts as
        // level-1 entries. Two records with different input sets in one
        // bucket both contribute (a harmless overlap the resolver alarms on
        // but still serves).
        for (ckey, record) in &compaction_records {
            if superseded_records.contains(ckey.as_str()) {
                continue;
            }
            for part in &record.parts {
                let entry = build_l1_snapshot_entry(ckey, record, part)?;
                contributed.push((entry, true));
            }
        }

        // Rewrite output parts: contributed as level-1 entries exactly as
        // compaction parts, unless this rewrite is itself superseded by a
        // newer one (ADR-0064 amendment).
        for (rkey, record) in &rewrite_records {
            if superseded_records.contains(rkey.as_str()) {
                continue;
            }
            for part in &record.parts {
                let entry = build_rewrite_l1_snapshot_entry(rkey, record, part)?;
                contributed.push((entry, true));
            }
        }

        // L0 commit records: contribute every one not named by a compaction
        // or rewrite input list above. An unlisted L0 is included exactly as the resolver
        // includes it.
        for key in &l0_keys {
            let record = self
                .load_and_validate(tenant, signal, shard, key, accounting)
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
            let is_canonical = hour == entry.ingest_hour_bucket;
            contributed.push((entry, is_canonical));
        }

        Ok(contributed)
    }

    /// Discover each `(shard, hour)` bucket's raw keys concurrently, bounded by
    /// `fold_bucket_concurrency` (ADR-0063 section 3). Returns one
    /// `(shard, hour, keys)` per input bucket in the SAME order as `buckets`
    /// (`buffered` preserves input order), so the caller's serial merge stays
    /// deterministic and the fold's output stays byte-for-byte reproducible.
    /// Only the LIST discovery is parallel here; the caller loads per-record
    /// GETs and merges entries serially in this order.
    async fn discover_bucket_listings(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        buckets: &[(u32, u32)],
    ) -> Result<Vec<BucketListing>, CatalogError> {
        let tenant = *tenant;
        let concurrency = self.config().fold_bucket_concurrency.max(1);
        let results: Vec<Result<BucketListing, CatalogError>> =
            stream::iter(buckets.iter().copied())
                .map(|(shard, hour)| async move {
                    let prefix = keys::commit_shard_hour_prefix(&tenant, signal, shard, hour)?;
                    let listing = list_all(self.store(), &prefix).await?;
                    Ok((shard, hour, listing))
                })
                .buffered(concurrency)
                .collect()
                .await;
        results.into_iter().collect()
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
        parts_total: 0,
        parts_reused: 0,
        list_requests: counters.list_requests,
        get_requests: counters.get_requests,
        put_requests: counters.put_requests,
        postings_built: false,
        postings_bytes: 0,
        layout_drift_count: 0,
        frontier_hours_reconciled: 0,
        frontier_hours_deferred: 0,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use prost::Message;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_proto::commit::v1::{CompactionInputIdentity, RetentionTombstone};
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

    /// A HEAD whose `format_version` is newer than this process understands
    /// (ADR-0066 decision 2) must fail the fold loudly and, critically, never
    /// attempt a CAS write: an older process racing a rolling upgrade must not
    /// rebuild a single-part HEAD over a newer multi-part one written by an
    /// upgraded peer (the single-part-over-multipart clobber bug).
    #[tokio::test]
    async fn newer_format_head_fails_loudly_and_never_clobbers() {
        let inner = MemoryStore::new();
        // Fault EVERY put. The fold must return before writing anything, so
        // this fault must never fire. Setup writes go through `inner()` and
        // bypass this counter, so a non-zero count can only mean the fold
        // itself attempted a PUT.
        let plan = FaultPlan::empty().with_rule(Rule::new(
            Op::Put,
            ScriptedFault::Permanent("no write expected on newer-format head".into()),
        ));
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        // Seed a HEAD one format version ahead of this build, written
        // directly so it bypasses `encode_head`'s own version validation. It
        // decodes as protobuf, so `decode_head` reaches the version check
        // first and returns `UnsupportedHeadVersion`.
        let head_key = head_object_key(&tenant(), Signal::Metrics);
        let newer_head = SnapshotHead {
            format_version: HEAD_FORMAT_VERSION + 1,
            ..Default::default()
        };
        let newer_bytes = newer_head.encode_to_vec();
        store
            .inner()
            .put(
                &head_key,
                Bytes::from(newer_bytes.clone()),
                PutOptions::default(),
            )
            .await
            .expect("seed newer-format head");

        let now = now_at_seal(12);
        let err = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[])
            .await
            .expect_err("fold must fail on a newer-format HEAD");
        assert!(
            matches!(
                err,
                CatalogError::UnsupportedHeadVersion { format_version }
                    if format_version == HEAD_FORMAT_VERSION + 1
            ),
            "expected UnsupportedHeadVersion, got {err:?}"
        );

        // The core safety property: the old process never attempted a PUT, so
        // it could not have clobbered the newer HEAD.
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Permanent),
            0,
            "fold must not attempt any PUT against a newer-format HEAD"
        );

        // And the newer HEAD is byte-for-byte intact in the store.
        let got = store
            .inner()
            .get(&head_key, GetRange::Full)
            .await
            .expect("newer head still present");
        assert_eq!(got.data.as_ref(), newer_bytes.as_slice());
    }

    /// The mirror image of the test above: a HEAD that decodes as protobuf and
    /// carries the CURRENT format version but fails a LATER structural check
    /// (tenant_hash length) is genuine corruption, not a newer format, and
    /// must still take the self-healing rebuild-and-CAS-overwrite path. Proves
    /// the single-part/multipart split is precise and did not block legitimate
    /// self-healing.
    #[tokio::test]
    async fn head_failing_late_validation_still_rebuilds_via_corrupt_path() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        publish_segment(
            &store,
            0,
            Uuid::new_v4(),
            1,
            10,
            now_at_seal(10) - NS_PER_HOUR,
        )
        .await;

        let head_key = head_object_key(&tenant(), Signal::Metrics);
        let corrupt_head = SnapshotHead {
            format_version: HEAD_FORMAT_VERSION,
            tenant_hash: vec![0u8; 4],
            ..Default::default()
        };
        store
            .put(
                &head_key,
                Bytes::from(corrupt_head.encode_to_vec()),
                PutOptions::default(),
            )
            .await
            .expect("seed corrupt head");

        let now_2 = now_at_seal(12);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 12, now_2 - NS_PER_HOUR).await;
        let report = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[])
            .await
            .expect("corrupt head self-heals via rebuild");
        assert!(report.rebuilt);
        assert!(
            report.put_requests >= 1,
            "rebuild must CAS-overwrite the corrupt head"
        );
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

    /// A fold must decode only the entries newly folded since
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

    // ---- ADR-0063 T2: hour-partitioned sealed/tail parts ----

    /// `partition_parts` seals only at hour boundaries: whole hours accumulate
    /// until admitting the next hour would cross the cap, and a single hour
    /// larger than the cap becomes one oversized part rather than being split.
    #[test]
    fn partition_parts_seals_at_hour_boundaries() {
        let entry = |hour: u32| SnapshotEntry {
            ingest_hour_bucket: hour,
            ..SnapshotEntry::default()
        };
        // Empty input still yields one (empty) span.
        let empty = partition_parts(&[], 100);
        assert_eq!(empty.len(), 1);
        assert_eq!((empty[0].start, empty[0].end), (0, 0));

        // One entry per hour, cap 1: every hour becomes its own part.
        let one_per_hour = vec![entry(10), entry(11), entry(12)];
        let spans = partition_parts(&one_per_hour, 1);
        assert_eq!(spans.len(), 3);
        assert_eq!(
            spans
                .iter()
                .map(|s| (s.min_hour, s.watermark_hour))
                .collect::<Vec<_>>(),
            vec![(10, 10), (11, 11), (12, 12)],
        );

        // A single hour of 3 entries with cap 2 is NOT split: one oversized
        // part, because splits are only at hour boundaries.
        let dense = vec![entry(5), entry(5), entry(5)];
        let spans = partition_parts(&dense, 2);
        assert_eq!(spans.len(), 1);
        assert_eq!((spans[0].start, spans[0].end), (0, 3));

        // Two entries in hour 7 then one in hour 8, cap 2: hour 7 fills the
        // first part, hour 8 seals it and starts the tail.
        let mixed = vec![entry(7), entry(7), entry(8)];
        let spans = partition_parts(&mixed, 2);
        assert_eq!(spans.len(), 2);
        assert_eq!((spans[0].min_hour, spans[0].watermark_hour), (7, 7));
        assert_eq!((spans[1].min_hour, spans[1].watermark_hour), (8, 8));
    }

    /// A fold whose entry count crosses
    /// `snapshot_part_max_entries` mid-run produces a multi-part HEAD that is
    /// sorted, disjoint, and self-consistent (`validate_head`); a later fold
    /// that adds no new data carries the sealed part forward by reference, so
    /// no PUT is issued for it.
    #[tokio::test]
    async fn ceiling_crossing_produces_multiple_parts() {
        let store = Arc::new(MemoryStore::new());
        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        // Two segments in two distinct sealed hours: with a cap of 1 entry the
        // fold seals hour 10 into its own part and starts a fresh tail at 11.
        let now_1 = now_at_seal(11);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 10, now_1 - 2 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 1, 11, now_1 - NS_PER_HOUR).await;

        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");
        assert!(!first.no_op);
        assert_eq!(first.entry_count, 2);
        assert_eq!(
            first.parts_total, 2,
            "crossing the cap splits into two parts"
        );
        assert_eq!(first.parts_reused, 0, "first fold writes every part fresh");

        let head_key = head_object_key(&tenant(), Signal::Metrics);
        let head = snapshot_format::decode_head(
            &store
                .get(&head_key, GetRange::Full)
                .await
                .expect("get head")
                .data,
        )
        .expect("head decodes");
        assert_eq!(head.parts.len(), 2);
        // Re-encoding re-runs validate_head: sorted, disjoint, self-consistent.
        snapshot_format::encode_head(&head).expect("multi-part head is self-consistent");
        assert!(head.parts[0].min_hour <= head.parts[0].watermark_hour);
        assert!(
            head.parts[0].watermark_hour < head.parts[1].min_hour,
            "ranges ascending and disjoint"
        );
        assert_eq!(
            head.parts[1].watermark_hour, head.watermark_hour,
            "the tail carries the fold watermark"
        );
        assert_eq!(head.watermark_hour, 11);

        let sealed_key = head.parts[0].key.clone();
        let sealed_bytes = store
            .get(&sealed_key, GetRange::Full)
            .await
            .expect("sealed part present")
            .data;

        // A second fold advances the watermark but adds no new data: the sealed
        // part is carried by reference, no PUT for it.
        let now_2 = now_at_seal(13);
        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[])
            .await
            .expect("second fold");
        assert!(!second.no_op);
        assert!(!second.rebuilt, "a valid HEAD folds incrementally");
        assert_eq!(second.parts_total, 2);
        assert!(
            second.parts_reused >= 1,
            "the unchanged sealed part is carried by reference (no PUT)"
        );

        let head2 = snapshot_format::decode_head(
            &store
                .get(&head_key, GetRange::Full)
                .await
                .expect("get head 2")
                .data,
        )
        .expect("head2 decodes");
        assert!(
            head2.parts.iter().any(|p| p.key == sealed_key),
            "sealed part still referenced by its original key"
        );
        let sealed_bytes_2 = store
            .get(&sealed_key, GetRange::Full)
            .await
            .expect("sealed part still present")
            .data;
        assert_eq!(
            sealed_bytes, sealed_bytes_2,
            "the sealed part object was not rewritten"
        );
    }

    /// Stage 4 checkpoint regression: when a sealed part object is missing
    /// and the fold falls back to a full rebuild from the commit layout
    /// (`rebuilt = true`), the rebuild recomputes byte-identical content for
    /// that span, so its blake3 matches the stale ref in the (still `Valid`)
    /// previous HEAD. Carrying that ref forward by reference -- as if the
    /// object still existed -- would skip the PUT and leave the new HEAD
    /// permanently naming a dead object. The rebuild path must always
    /// restore the sealed part, never reuse a reference across a rebuild.
    #[tokio::test]
    async fn rebuild_restores_a_deleted_sealed_part_instead_of_reusing_its_ref() {
        let store = Arc::new(MemoryStore::new());
        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        let now_1 = now_at_seal(11);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 10, now_1 - 2 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 1, 11, now_1 - NS_PER_HOUR).await;

        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");
        assert_eq!(
            first.parts_total, 2,
            "crossing the cap splits into two parts"
        );

        let head_key = head_object_key(&tenant(), Signal::Metrics);
        let head = snapshot_format::decode_head(
            &store
                .get(&head_key, GetRange::Full)
                .await
                .expect("get head")
                .data,
        )
        .expect("head decodes");
        let sealed_key = head.parts[0].key.clone();

        // Delete the sealed part object out from under the HEAD, simulating
        // an unreadable/missing part (e.g. a GC race). Any live tenant read
        // of it now fails with `NotFound`.
        store.delete(&sealed_key).await.expect("delete sealed part");

        let now_2 = now_at_seal(13);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 12, now_2 - NS_PER_HOUR).await;
        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[])
            .await
            .expect("fold falls back to rebuild and restores the missing part");
        assert!(
            second.rebuilt,
            "the missing sealed part must trigger a rebuild, not an incremental fold"
        );

        // The sealed part object must have been restored: a live GET must
        // succeed now, not still return NotFound.
        let restored = store.get(&sealed_key, GetRange::Full).await;
        assert!(
            restored.is_ok(),
            "the rebuild must have re-PUT the deleted sealed part, got {restored:?}"
        );

        let head2 = snapshot_format::decode_head(
            &store
                .get(&head_key, GetRange::Full)
                .await
                .expect("get head 2")
                .data,
        )
        .expect("head2 decodes");
        for part in &head2.parts {
            let got = store.get(&part.key, GetRange::Full).await;
            assert!(
                got.is_ok(),
                "every part the rebuilt HEAD references must actually exist: {} -> {got:?}",
                part.key
            );
        }
    }

    /// Failing the crashing fold's HEAD CAS (after some
    /// new part objects have already been PUT) must leave the previous HEAD
    /// exactly intact -- never half-updated. The orphaned new part objects are
    /// allowed garbage.
    #[tokio::test]
    async fn crash_after_partial_part_puts_leaves_old_head_intact() {
        // Fail only the SECOND HEAD PUT (the crashing fold's CAS). The first
        // fold's HEAD PUT (the 1st matching call) passes, establishing a valid
        // old head; no other PUT key contains "HEAD".
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("head cas crash".into()))
                .with_key_contains("HEAD")
                .with_occurrence(Occurrence::Nth(2)),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        let now_1 = now_at_seal(11);
        publish_segment(
            store.inner(),
            0,
            Uuid::new_v4(),
            1,
            10,
            now_1 - 2 * NS_PER_HOUR,
        )
        .await;
        publish_segment(store.inner(), 0, Uuid::new_v4(), 1, 11, now_1 - NS_PER_HOUR).await;
        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");
        assert_eq!(first.parts_total, 2);

        let head_key = head_object_key(&tenant(), Signal::Metrics);
        let old_head_bytes = store
            .inner()
            .get(&head_key, GetRange::Full)
            .await
            .expect("old head")
            .data;

        // A second fold adds new data (so new parts get PUT), then its HEAD CAS
        // is failed by the fault.
        let now_2 = now_at_seal(13);
        publish_segment(store.inner(), 0, Uuid::new_v4(), 1, 13, now_2 - NS_PER_HOUR).await;
        let err = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[])
            .await
            .expect_err("HEAD CAS crash must surface as an error");
        assert!(matches!(err, CatalogError::Store(_)), "got {err:?}");

        // The fault fired exactly once: the crashing fold's single HEAD PUT
        // attempt (a Permanent error is not retried).
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 1);

        // HEAD is byte-for-byte the old head: never half-updated.
        let head_now = store
            .inner()
            .get(&head_key, GetRange::Full)
            .await
            .expect("head still present")
            .data;
        assert_eq!(
            head_now.as_ref(),
            old_head_bytes.as_ref(),
            "HEAD unchanged after the crashing fold"
        );
        // And it still decodes to the pre-crash multi-part snapshot.
        let head = snapshot_format::decode_head(&head_now).expect("old head still valid");
        assert_eq!(head.watermark_hour, 11);
        assert_eq!(head.parts.len(), 2);
    }

    /// A resolve over a narrow hour window fetches only
    /// the parts whose range intersects it, not every part in HEAD. Proven by a
    /// GET fault on the low part's object (its `/snap/` key embeds hour 10's
    /// watermark string): a narrow window must not fire it, a wide window must.
    #[tokio::test]
    async fn range_scoped_resolve_fetches_only_intersecting_parts() {
        let low_marker = format!("snap/{}", keys::ingest_hour_string(10));
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("low part fetched".into()))
                .with_key_contains(low_marker),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        // Two far-apart hours: hour 10 seals into its own part, hour 20 is the
        // tail.
        let now = now_at_seal(20);
        publish_segment(
            store.inner(),
            0,
            Uuid::new_v4(),
            1,
            10,
            now - 11 * NS_PER_HOUR,
        )
        .await;
        publish_segment(store.inner(), 0, Uuid::new_v4(), 1, 20, now - NS_PER_HOUR).await;
        let report = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[])
            .await
            .expect("fold");
        assert_eq!(report.parts_total, 2);
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Permanent),
            0,
            "the rebuild fold GETs no /snap/ object"
        );

        // Narrow window near hour 20: the low part (hours 10..10) does not
        // intersect, so it is not fetched.
        let narrow = ravel_types::TimeRange {
            start_ns: 20 * NS_PER_HOUR,
            end_ns: 21 * NS_PER_HOUR,
        };
        catalog
            .resolve(&tenant(), Signal::Metrics, narrow, &[], now)
            .await
            .expect("narrow resolve");
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Permanent),
            0,
            "range filter must skip the non-intersecting low part"
        );

        // A window reaching hour 10 intersects the low part, so a fetch is
        // attempted (fault fires); the resolve still succeeds via listing.
        let wide = ravel_types::TimeRange {
            start_ns: 0,
            end_ns: 21 * NS_PER_HOUR,
        };
        catalog
            .resolve(&tenant(), Signal::Metrics, wide, &[], now)
            .await
            .expect("wide resolve");
        assert!(
            store.fault_count(Op::Get, FaultKind::Permanent) >= 1,
            "the intersecting low part is fetched"
        );
    }

    // ---- ADR-0063: fold reconcile pass ----

    /// Publish a compaction record into `(shard, ingest_hour_bucket)` that
    /// supersedes the given L0 `inputs` and contributes one L1 part. Only the
    /// record object is written (at its own reconstructed key): the fold's
    /// reconcile and incremental paths read the record, not the part object, so
    /// no L1 part object is needed for these tests.
    async fn publish_compaction(
        store: &MemoryStore,
        shard: u32,
        ingest_hour_bucket: u32,
        inputs: &[&CommitRecord],
        created_unix_ns: i64,
    ) -> CompactionRecord {
        let input_ids: Vec<CompactionInputIdentity> = inputs
            .iter()
            .map(|r| CompactionInputIdentity {
                writer_id: r.writer_id.clone(),
                writer_epoch: r.writer_epoch,
                writer_seq: r.writer_seq,
            })
            .collect();
        let mut hasher = blake3::Hasher::new();
        for id in &input_ids {
            hasher.update(id.writer_id.as_bytes());
            hasher.update(&id.writer_epoch.to_le_bytes());
            hasher.update(&id.writer_seq.to_le_bytes());
        }
        let input_set_hash = *hasher.finalize().as_bytes();
        let part_payload = format!("l1-{shard}-{ingest_hour_bucket}").into_bytes();
        let part_content_hash = *blake3::hash(&part_payload).as_bytes();
        let part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xffu8; 16],
            content_hash: part_content_hash.to_vec(),
            object_size: part_payload.len() as u64,
            sample_count: 1,
            series_count: 1,
            run_count: 1,
            min_event_ts_ns: created_unix_ns - 1_000,
            max_event_ts_ns: created_unix_ns,
            segment_format_version: 3,
        };
        let record = CompactionRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics).into(),
            shard,
            ingest_hour_bucket,
            level: 1,
            inputs: input_ids,
            input_set_hash: input_set_hash.to_vec(),
            parts: vec![part],
            created_unix_ns,
        };
        let key = keys::compaction_record_key_for(&record).expect("compaction key");
        store
            .put(
                &key,
                Bytes::from(record.encode_to_vec()),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put compaction record");
        record
    }

    /// Publish a retention tombstone for `(shard, ingest_hour_bucket)`. The
    /// fold classifies a bucket as tombstoned from the key shape alone and
    /// never reads the body, so a minimal valid record suffices.
    async fn publish_tombstone(store: &MemoryStore, shard: u32, ingest_hour_bucket: u32) {
        let record = RetentionTombstone {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics).into(),
            shard,
            ingest_hour_bucket,
            retired_at_ns: 0,
            retention_window_ns: 0,
            record_count_observed: 0,
        };
        let key = keys::retention_tombstone_key_for(&record).expect("tombstone key");
        store
            .put(
                &key,
                Bytes::from(record.encode_to_vec()),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put tombstone");
    }

    /// Load and concatenate every entry across all parts a HEAD names, in part
    /// order (the globally sorted entry set).
    async fn collect_head_entries(
        store: &dyn ObjectStoreBackend,
        head: &SnapshotHead,
    ) -> Vec<SnapshotEntry> {
        let mut out = Vec::new();
        for part in &head.parts {
            let got = store
                .get(&part.key, GetRange::Full)
                .await
                .expect("part present");
            let decoded = snapshot_format::decode_part(&got.data, &PartLimits::default())
                .expect("decode part");
            out.extend(decoded.entries);
        }
        out
    }

    async fn read_head(store: &dyn ObjectStoreBackend) -> SnapshotHead {
        let head_key = head_object_key(&tenant(), Signal::Metrics);
        let got = store
            .get(&head_key, GetRange::Full)
            .await
            .expect("head present");
        snapshot_format::decode_head(&got.data).expect("head decodes")
    }

    /// A compaction record published into an hour that a
    /// PREVIOUS fold already sealed and folded is discovered and applied by the
    /// reconcile pass of a later fold, and the sealed part covering that hour is
    /// actually re-PUT (its content changed) rather than carried forward by
    /// reference.
    #[tokio::test]
    async fn reconcile_applies_late_compaction_before_horizon() {
        let store = Arc::new(MemoryStore::new());
        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        // Hour 10 seals into its own part; hour 11 is the tail.
        let now_1 = now_at_seal(11);
        let writer_a = Uuid::new_v4();
        let seg_a = publish_segment(&store, 0, writer_a, 1, 10, now_1 - 2 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 1, 11, now_1 - NS_PER_HOUR).await;
        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");
        assert_eq!(first.parts_total, 2);
        assert_eq!(first.entry_count, 2);

        let head1 = read_head(store.as_ref()).await;
        let hour10_part = head1
            .parts
            .iter()
            .find(|p| p.min_hour <= 10 && 10 <= p.watermark_hour)
            .expect("a part covers hour 10")
            .clone();
        let old_blake3 = hour10_part.blake3.clone();

        // A compaction lands in the already-sealed, already-folded hour 10,
        // superseding seg_a's L0 with one L1 part.
        publish_compaction(&store, 0, 10, &[&seg_a], now_1).await;

        // A later fold: the reconcile window covers hour 10 and applies it.
        let now_2 = now_at_seal(13);
        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[])
            .await
            .expect("second fold");
        assert!(!second.no_op);
        assert!(!second.rebuilt, "an incremental fold, not a rebuild");

        let head2 = read_head(store.as_ref()).await;
        let entries = collect_head_entries(store.as_ref(), &head2).await;
        let hour10: Vec<&SnapshotEntry> = entries
            .iter()
            .filter(|e| e.ingest_hour_bucket == 10)
            .collect();
        assert_eq!(hour10.len(), 1, "hour 10 now has exactly the L1 part");
        assert_eq!(hour10[0].level, 1, "hour 10's entry is the compaction L1");
        assert!(
            !entries.iter().any(|e| e.content_hash == seg_a.content_hash),
            "the superseded L0 must be gone from the snapshot"
        );

        // The sealed part covering hour 10 was re-PUT: no part in the new HEAD
        // carries the old hour-10 part's blake3, so it was not carried forward.
        assert!(
            !head2.parts.iter().any(|p| p.blake3 == old_blake3),
            "the sealed hour-10 part must be re-PUT with new content, not reused"
        );
        assert!(
            head2
                .parts
                .iter()
                .any(|p| p.min_hour <= 10 && 10 <= p.watermark_hour),
            "a part still covers hour 10"
        );
    }

    /// The reconcile pass must never reintroduce a duplicate entry identity.
    /// A record physically stored under one hour's directory but whose own
    /// embedded `ingest_hour_bucket` names a different hour
    /// (`classify_bucket`'s drift tolerance, exercised for the incremental
    /// path by `duplicate_commit_identity_across_buckets_skips_and_advances`)
    /// is already resident in `entries` from a past fold with no colliding
    /// copy anywhere. When a LATER compaction lands in that drifted record's
    /// own physical (not embedded) bucket, the reconcile pass re-classifies
    /// that bucket and must not blindly re-append the drifted record's
    /// identity a second time -- doing so used to permanently break every
    /// later fold with `SnapshotFormat(DuplicateEntry)`.
    #[tokio::test]
    async fn reconcile_never_reintroduces_a_drifted_duplicate() {
        let store = Arc::new(MemoryStore::new());
        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        let now_1 = now_at_seal(11);

        // S: a genuine record properly placed in hour 9's directory.
        let seg_s = publish_segment(&store, 0, Uuid::new_v4(), 1, 9, now_1 - 2 * NS_PER_HOUR).await;

        // R: physically stored under hour 9's directory, but its own
        // embedded ingest_hour_bucket names hour 10 -- a drifted record with
        // no other physical copy anywhere. classify_bucket tolerates this
        // shape (validate_expected_fields never checks the embedded hour
        // against the physical directory).
        let writer_r = Uuid::new_v4();
        let record_r = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id: writer_r,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: 5,
            content_hash: *blake3::hash(b"r").as_bytes(),
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
        .expect("valid record r");
        let key_r =
            keys::commit_key(&tenant(), Signal::Metrics, 0, 9, writer_r, 1, 1).expect("key r");
        store
            .put(
                &key_r,
                record::encode(&record_r),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put r");

        publish_segment(&store, 0, Uuid::new_v4(), 1, 11, now_1 - NS_PER_HOUR).await;

        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");
        assert_eq!(
            first.entry_count, 3,
            "S, the drifted R, and hour 11's segment all fold in with no collision"
        );
        assert_eq!(first.layout_drift_count, 0, "R has no colliding copy yet");

        // A late compaction lands in hour 9's directory, covering S but not
        // the drifted R (a different writer identity entirely).
        publish_compaction(&store, 0, 9, &[&seg_s], now_1).await;

        let second = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(13),
                &[],
            )
            .await
            .expect("reconcile must not fail with DuplicateEntry");
        assert!(!second.no_op);
        assert!(!second.rebuilt, "an incremental fold, not a rebuild");

        let entries = collect_head_entries(store.as_ref(), &read_head(store.as_ref()).await).await;
        let r_bytes = writer_r.into_bytes().to_vec();
        assert_eq!(
            entries.iter().filter(|e| e.writer_id == r_bytes).count(),
            1,
            "the drifted record R must appear exactly once, never duplicated"
        );
        let hour9: Vec<&SnapshotEntry> = entries
            .iter()
            .filter(|e| e.ingest_hour_bucket == 9)
            .collect();
        assert_eq!(
            hour9.len(),
            1,
            "hour 9 now holds exactly the compacted L1 part"
        );
        assert_eq!(hour9[0].level, 1);

        // A third fold must also succeed cleanly: the bug reproduced
        // deterministically on every subsequent fold, not just the first.
        let third = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(14),
                &[],
            )
            .await
            .expect("a third fold must also succeed, not fail forever");
        assert!(!third.no_op);
    }

    /// A late record landing OUTSIDE the reconcile window (older than
    /// `watermark_hour_old - fold_reconcile_window_hours`) is correctly NOT
    /// picked up: the stated, bounded staleness this window accepts.
    #[tokio::test]
    async fn reconcile_ignores_late_record_outside_window() {
        let store = Arc::new(MemoryStore::new());
        // Default window is 26 hours.
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let writer_x = Uuid::new_v4();
        let seg_x = publish_segment(&store, 0, writer_x, 1, 5, 6 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 1, 40, 41 * NS_PER_HOUR).await;
        let first = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(40),
                &[],
            )
            .await
            .expect("first fold");
        assert_eq!(first.entry_count, 2);

        // A compaction lands in hour 5, far older than watermark 40 minus the
        // 26-hour window (floor 14).
        publish_compaction(&store, 0, 5, &[&seg_x], 6 * NS_PER_HOUR).await;

        let second = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(41),
                &[],
            )
            .await
            .expect("second fold");
        assert!(!second.no_op, "the watermark advanced 40 -> 41");
        assert_eq!(
            second.entry_count, 2,
            "the out-of-window compaction is not applied"
        );

        let entries = collect_head_entries(store.as_ref(), &read_head(store.as_ref()).await).await;
        let hour5: Vec<&SnapshotEntry> = entries
            .iter()
            .filter(|e| e.ingest_hour_bucket == 5)
            .collect();
        assert_eq!(hour5.len(), 1);
        assert_eq!(hour5[0].level, 0, "hour 5 still holds the original L0");
        assert_eq!(hour5[0].content_hash, seg_x.content_hash);
    }

    /// A late retention tombstone in an already-folded, in-window hour drops
    /// that hour's entries on the next fold.
    #[tokio::test]
    async fn reconcile_applies_late_tombstone() {
        let store = Arc::new(MemoryStore::new());
        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        let now_1 = now_at_seal(11);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 10, now_1 - 2 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 1, 11, now_1 - NS_PER_HOUR).await;
        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");
        assert_eq!(first.entry_count, 2);

        // Retire hour 10 after it was folded.
        publish_tombstone(&store, 0, 10).await;

        let second = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(13),
                &[],
            )
            .await
            .expect("second fold");
        assert!(!second.no_op);
        assert!(!second.rebuilt);

        let entries = collect_head_entries(store.as_ref(), &read_head(store.as_ref()).await).await;
        assert!(
            !entries.iter().any(|e| e.ingest_hour_bucket == 10),
            "the tombstoned hour contributes nothing after reconcile"
        );
        assert!(
            entries.iter().any(|e| e.ingest_hour_bucket == 11),
            "the untouched hour 11 is preserved"
        );
    }

    /// Write a per-tenant durable retention window (TenantConfig.retention_ns)
    /// so the fold's retention-frontier reconcile has a frontier to derive.
    async fn write_retention_config(store: &dyn ObjectStoreBackend, retention_ns: i64) {
        let cfg = crate::tenant_config::TenantConfig {
            retention_ns: Some(retention_ns),
            ..crate::tenant_config::TenantConfig::new(
                crate::tenant_config::TenantLifecycleState::Active,
            )
        };
        crate::tenant_config::set_tenant_config(store, &tenant(), &cfg, 1)
            .await
            .expect("write tenant retention config");
    }

    /// A retention tombstone written into an hour FAR behind the watermark
    /// (well outside `fold_reconcile_window_hours`) is applied by the next
    /// fold's retention-frontier reconcile, and the resulting snapshot no
    /// longer names that bucket's segments (deliverable 2, fold side; ADR-0020
    /// delete-blocker, retirement-frontier half). To watch this FAIL against
    /// pre-fix code, set `frontier_reconcile_max_hours: 0` in the config below
    /// (or `retention_ns: None`): the frontier pass then never runs, hour 5's
    /// tombstone stays outside the fixed 26h window forever, and the snapshot
    /// keeps naming hour 5.
    #[tokio::test]
    async fn frontier_reconcile_applies_out_of_window_tombstone() {
        let store = Arc::new(MemoryStore::new());
        let cfg = CatalogConfig {
            shard_count: 1,
            frontier_reconcile_max_hours: 168,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        // Retention window 200h: with the ~25h protection-horizon look-ahead,
        // the retirement frontier at fold time sits near hour 28 -- well above
        // the retired hour 5, and well below the fixed reconcile window behind
        // the hour-200 watermark ([174, 200]). So only the frontier pass can
        // reach hour 5's tombstone.
        write_retention_config(store.as_ref(), 200 * NS_PER_HOUR).await;

        let now_1 = now_at_seal(200);
        publish_segment(
            &store,
            0,
            Uuid::new_v4(),
            1,
            5,
            5 * NS_PER_HOUR + 60_000_000_000,
        )
        .await;
        publish_segment(
            &store,
            0,
            Uuid::new_v4(),
            1,
            200,
            200 * NS_PER_HOUR + 60_000_000_000,
        )
        .await;
        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");
        assert!(first.rebuilt, "first fold rebuilds from the commit layout");
        let entries0 = collect_head_entries(store.as_ref(), &read_head(store.as_ref()).await).await;
        assert!(
            entries0.iter().any(|e| e.ingest_hour_bucket == 5),
            "the first fold's snapshot names hour 5"
        );

        // Retire hour 5 long after it was folded: the tombstone lands at hour
        // 5's key, ~195 hours behind the watermark -- far outside [174, 200].
        publish_tombstone(&store, 0, 5).await;

        let second = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(202),
                &[],
            )
            .await
            .expect("second fold");
        assert!(
            !second.rebuilt,
            "the second fold is incremental, not a rebuild"
        );
        assert!(
            second.frontier_hours_reconciled >= 1,
            "the frontier pass re-listed hour 5's bucket (got {})",
            second.frontier_hours_reconciled
        );
        assert_eq!(
            second.frontier_hours_deferred, 0,
            "the single frontier hour is well under the cap"
        );

        let entries = collect_head_entries(store.as_ref(), &read_head(store.as_ref()).await).await;
        assert!(
            !entries.iter().any(|e| e.ingest_hour_bucket == 5),
            "the out-of-window tombstoned hour 5 contributes nothing after the frontier reconcile"
        );
        assert!(
            entries.iter().any(|e| e.ingest_hour_bucket == 200),
            "the untouched recent hour 200 is preserved"
        );
    }

    /// A frontier jump -- the retention window is short, so many already-sealed
    /// hours sit below the frontier at once -- does NOT make one fold re-list an
    /// unbounded number of buckets: the frontier pass reconciles at most
    /// `frontier_reconcile_max_hours` per fold and carries the remainder to the
    /// next fold (deliverable 5). The carried hours are never silently dropped:
    /// they are still named by the snapshot and counted on
    /// `frontier_hours_deferred`. To watch this FAIL, remove the cap in the
    /// fold (`let take = candidate_hours.len()`): `frontier_hours_reconciled`
    /// then jumps to the full candidate count and `frontier_hours_deferred`
    /// collapses to 0.
    #[tokio::test]
    async fn frontier_reconcile_is_bounded_and_carries_remainder() {
        let store = Arc::new(MemoryStore::new());
        let cap: u32 = 3;
        let cfg = CatalogConfig {
            shard_count: 1,
            frontier_reconcile_max_hours: cap,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        // A dramatically short window: every sealed hour below ~28 is at or
        // past the frontier at once. Hours 5..=15 (eleven hours) all sit below
        // both the frontier and the fixed window behind the hour-200 watermark.
        write_retention_config(store.as_ref(), 200 * NS_PER_HOUR).await;

        let old_hours: Vec<u32> = (5..=15).collect();
        for &h in &old_hours {
            publish_segment(
                &store,
                0,
                Uuid::new_v4(),
                1,
                h,
                i64::from(h) * NS_PER_HOUR + 60_000_000_000,
            )
            .await;
        }
        publish_segment(
            &store,
            0,
            Uuid::new_v4(),
            1,
            200,
            200 * NS_PER_HOUR + 60_000_000_000,
        )
        .await;
        catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(200),
                &[],
            )
            .await
            .expect("first fold");

        let second = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(202),
                &[],
            )
            .await
            .expect("second fold");
        assert_eq!(
            second.frontier_hours_reconciled,
            u64::from(cap),
            "the frontier pass re-lists at most the cap per fold"
        );
        assert_eq!(
            second.frontier_hours_deferred,
            old_hours.len() as u64 - u64::from(cap),
            "every frontier hour beyond the cap is carried, not dropped"
        );

        // Carried, not dropped: no tombstones exist yet, so every old hour is
        // still named by the snapshot after the capped fold -- none was
        // silently skipped out of the snapshot.
        let entries = collect_head_entries(store.as_ref(), &read_head(store.as_ref()).await).await;
        for &h in &old_hours {
            assert!(
                entries.iter().any(|e| e.ingest_hour_bucket == h),
                "hour {h} is still named (carried, never dropped without a tombstone)"
            );
        }
    }

    /// A fold whose reconcile window finds nothing changed carries every
    /// untouched sealed part forward by reference: no spurious PUT; the
    /// carry-forward optimization is not regressed by the reconcile pass.
    #[tokio::test]
    async fn reconcile_no_change_carries_sealed_parts_forward() {
        let store = Arc::new(MemoryStore::new());
        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        let now_1 = now_at_seal(11);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 10, now_1 - 2 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 1, 11, now_1 - NS_PER_HOUR).await;
        catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");

        let head1 = read_head(store.as_ref()).await;
        let sealed = head1
            .parts
            .iter()
            .find(|p| p.min_hour <= 10 && 10 <= p.watermark_hour)
            .expect("hour-10 part")
            .clone();
        let sealed_bytes = store
            .get(&sealed.key, GetRange::Full)
            .await
            .expect("sealed part")
            .data;

        // No late record: a later fold's reconcile pass finds nothing changed.
        let second = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(13),
                &[],
            )
            .await
            .expect("second fold");
        assert!(!second.rebuilt);
        assert!(
            second.parts_reused >= 1,
            "the unchanged sealed part is carried by reference"
        );

        let head2 = read_head(store.as_ref()).await;
        assert!(
            head2.parts.iter().any(|p| p.key == sealed.key),
            "sealed part still referenced by its original key"
        );
        let sealed_bytes_2 = store
            .get(&sealed.key, GetRange::Full)
            .await
            .expect("sealed part still present")
            .data;
        assert_eq!(
            sealed_bytes, sealed_bytes_2,
            "the sealed part object was not rewritten"
        );
    }

    /// Both the very first fold (`HeadState::Absent`) and a rebuilt fold
    /// (corrupt HEAD) skip the reconcile pass entirely: no in-window empty
    /// bucket is ever listed. Proven by a LIST fault on hour 5's bucket prefix
    /// (an hour that holds no data, so `discover_buckets` never lists it, but
    /// the reconcile window WOULD): the fault must never fire.
    #[tokio::test]
    async fn first_and_rebuilt_folds_skip_reconcile() {
        let hour5_prefix =
            keys::commit_shard_hour_prefix(&tenant(), Signal::Metrics, 0, 5).expect("prefix");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::List,
                ScriptedFault::Permanent("reconcile listed an in-window empty hour".into()),
            )
            .with_key_contains(hour5_prefix),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        // First fold: HEAD absent -> rebuilt, reconcile skipped.
        let now_1 = now_at_seal(10);
        publish_segment(store.inner(), 0, Uuid::new_v4(), 1, 10, now_1 - NS_PER_HOUR).await;
        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");
        assert!(first.rebuilt);
        assert_eq!(
            store.fault_count(Op::List, FaultKind::Permanent),
            0,
            "first fold must not run the reconcile pass"
        );

        // Corrupt HEAD, then fold again: rebuilt, reconcile skipped.
        let head_key = head_object_key(&tenant(), Signal::Metrics);
        store
            .inner()
            .put(
                &head_key,
                Bytes::from_static(b"not a head"),
                PutOptions::default(),
            )
            .await
            .expect("corrupt head");
        let now_2 = now_at_seal(12);
        publish_segment(store.inner(), 0, Uuid::new_v4(), 1, 12, now_2 - NS_PER_HOUR).await;
        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[])
            .await
            .expect("second fold");
        assert!(second.rebuilt);
        assert_eq!(
            store.fault_count(Op::List, FaultKind::Permanent),
            0,
            "a rebuilt fold must not run the reconcile pass"
        );
    }

    /// The single HEAD CAS invariant holds even when the reconcile pass finds
    /// and applies a change: exactly one HEAD PUT per fold attempt. Fault the
    /// THIRD HEAD PUT (fold 1 is the 1st, the reconcile-applying fold 2 is the
    /// 2nd); a spurious second HEAD PUT inside fold 2 would be the 3rd and fire
    /// the fault. It must not.
    #[tokio::test]
    async fn reconcile_preserves_single_head_cas() {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Permanent("second HEAD PUT within one fold".into()),
            )
            .with_key_contains("HEAD")
            .with_occurrence(Occurrence::Nth(3)),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        let now_1 = now_at_seal(11);
        let writer_a = Uuid::new_v4();
        let seg_a =
            publish_segment(store.inner(), 0, writer_a, 1, 10, now_1 - 2 * NS_PER_HOUR).await;
        publish_segment(store.inner(), 0, Uuid::new_v4(), 1, 11, now_1 - NS_PER_HOUR).await;
        catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
            .await
            .expect("first fold");

        publish_compaction(store.inner(), 0, 10, &[&seg_a], now_1).await;

        let second = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(13),
                &[],
            )
            .await
            .expect("second fold applies reconcile under one HEAD CAS");
        assert!(!second.no_op);

        // The reconcile change actually landed (proving the test exercised the
        // apply path, not a no-op).
        let entries = collect_head_entries(store.inner(), &read_head(store.inner()).await).await;
        assert!(
            entries
                .iter()
                .any(|e| e.ingest_hour_bucket == 10 && e.level == 1),
            "the late compaction was applied"
        );
        assert!(
            !entries.iter().any(|e| e.content_hash == seg_a.content_hash),
            "the superseded L0 is gone"
        );

        // No fold attempt issued a second HEAD PUT.
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Permanent),
            0,
            "each fold attempt performs exactly one HEAD CAS write"
        );
    }
}
