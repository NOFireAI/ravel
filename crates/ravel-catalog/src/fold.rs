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
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::{self, StreamExt};
use ravel_codec::key_idx::{KeyIdxFieldEntries, KeyType, decode_key_idx};
use ravel_commit::keys::{self, BucketEntry, KeyError};
use ravel_commit::signal;
use ravel_memory::Reservation;
use ravel_object_store::{
    GetRange, ObjectMeta, ObjectStoreBackend, PutMode, PutOptions, StoreError, UploadChecksum,
    Version, list_all,
};
use ravel_proto::catalog::v1::{
    ColumnStatsSegment, SnapshotColumnStatsPartRef, SnapshotEntry, SnapshotHead,
    SnapshotKeyIndexLeafRef, SnapshotPartRef, SnapshotPostingsRef,
};
use ravel_proto::commit::v1::{CommitRecord, CompactionPart, CompactionRecord, RewriteRecord};
use ravel_segment::{ExpectedIdentity, ReaderLimits, SegmentError};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{METRIC_NAME_LABEL, Signal, TenantHash};
use uuid::Uuid;

use crate::catalog::Catalog;
use crate::column_stats_build;
use crate::config::CatalogConfig;
use crate::declared_stats;
use crate::error::CatalogError;
use crate::provisioning::{DEFAULT_SCAN_SLACK_HOURS, ShardGeneration, scan_count};
use crate::snapshot_format::{
    self, EncodedKeyIndexLeaf, HEAD_FORMAT_VERSION_1, HEAD_FORMAT_VERSIONS,
    KeyIndexLeafEncodeError, KeyIndexLeafEntry, KeyIndexLeafInput, KeyIndexLeafLimits,
    KeyIndexLeafSummary, KeyIndexSlice, NamePostings, PartLimits, SnapshotFormatError,
    column_stats_segments_concat, encode_key_index_leaf, encoded_entry_len,
    key_index_leaf_version_accepted, select_key_index_slice, split_key_index_entries,
    verify_key_index_leaf_frames,
};
use crate::tenant_config::DeclaredTypedColumn;

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
    #[error("L1 entry writer_id (input_set_hash) must be 32 bytes, got {0}")]
    BadInputSetHashLen(usize),
    #[error("L1 entry writer_epoch (part_index) does not fit u32: {0}")]
    BadPartIndex(u64),
    #[error("segment series entry has no {METRIC_NAME_LABEL} label")]
    MissingMetricName,
    #[error("unsupported segment format version {0}")]
    UnsupportedSegmentVersion(u16),
}

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Bounded retry budget for HEAD's CAS loop: a folder that keeps losing to concurrent folders
/// gives up with [`CatalogError::FoldCasRetriesExhausted`] rather than
/// retrying forever.
pub(crate) const MAX_HEAD_CAS_ATTEMPTS: u32 = 8;

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

/// The ingest hours a caller wants this fold to re-list and reconcile, on top
/// of the fold's own fixed reconcile window and retention-frontier band
/// (ADR-0063 section 4). The receiving end of issue #526's re-fold half.
///
/// The fold cannot derive this set itself. An hour that received a late
/// compaction or rewrite record is indistinguishable, from the snapshot
/// entries alone, from an hour that did not: proving the difference needs a
/// LIST of that hour's commit buckets, and the hours that could have received
/// one are every hour the snapshot names, so a self-derived version would cost
/// a full-history LIST fan-out on every fold. The fixed window bounds that by
/// recency and the frontier band bounds it by the retention frontier; neither
/// reaches a late record in the middle of a tenant's history, which is exactly
/// the hour that leaks (the sweep's HEAD-reachability gate holds the
/// pre-rewrite inputs because the snapshot still names them).
///
/// The maintain tier's superseded-input sweep already pays for those LISTs and
/// already computes the answer: a bucket it holds on
/// `SnapshotBlock::Named` is an hour whose snapshot entries still name inputs a
/// published compaction or rewrite record superseded. Those hours are what
/// belongs here.
///
/// Requesting an hour is a hint, never a durability dependency: an unrequested
/// or dropped hour degrades to today's behavior (the hour keeps naming its
/// pre-rewrite inputs until retention drops it), and the sweep's gate remains
/// the delete blocker either way.
///
/// Three kinds of fold ignore a request entirely: a first fold, a rebuild,
/// and a no-op fold over a HEAD with no held-open hour (the watermark did not
/// advance, so the fold returns before any reconcile work). A fold over
/// held-open hours does run the targeted pass for the requested hours outside
/// the held range, even when it then reports a no-op. Nothing records the
/// ignored hours; the requester re-derives its blocked set on each sweep and
/// resubmits a still-held hour.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefoldRequest {
    /// Ascending and deduplicated, so a fold spends its cap oldest-first (the
    /// hours whose held inputs have occupied storage longest).
    hours: BTreeSet<u32>,
}

impl RefoldRequest {
    /// An empty request: the fold behaves exactly as [`Catalog::fold`] does.
    pub fn new() -> Self {
        Self::default()
    }

    /// Request a re-fold of one ingest hour.
    pub fn insert(&mut self, ingest_hour_bucket: u32) {
        self.hours.insert(ingest_hour_bucket);
    }

    /// Build a request from an hour iterator; duplicates collapse.
    pub fn from_hours(hours: impl IntoIterator<Item = u32>) -> Self {
        Self {
            hours: hours.into_iter().collect(),
        }
    }

    /// The requested hours, ascending.
    pub fn hours(&self) -> impl Iterator<Item = u32> + '_ {
        self.hours.iter().copied()
    }

    /// Number of distinct hours requested.
    pub fn len(&self) -> usize {
        self.hours.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hours.is_empty()
    }
}

/// Outcome of one [`Catalog::fold`] call.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FoldReport {
    /// HEAD's watermark_hour after this call. `None` only when no hour has
    /// ever been sealed for this (tenant, signal) yet.
    pub watermark_hour: Option<u32>,
    /// HEAD's watermark_hour before this call.
    pub previous_watermark_hour: Option<u32>,
    /// `true` if nothing was sealed beyond `previous_watermark_hour` and no
    /// hour an operator-asserted seal holds open listed a commit HEAD does
    /// not hold: HEAD was left untouched, no part was written.
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
    /// Number of PUT requests this fold issued to the object store, counted
    /// unconditionally at the point each request is issued -- an
    /// `AlreadyExists` or another store-side error still counts, since the
    /// store received the request and, on an ambiguous error, may have
    /// durably written the object despite the client-visible failure (#1598).
    /// This is a request-issued counter, not a request-succeeded one: a
    /// build/PUT failure that leaves a `*_built` field `false` still counted
    /// its attempt here.
    pub put_requests: u64,
    /// `true` if this fold successfully built and attached a name-postings
    /// index. `false` covers both "no
    /// postings work was needed" (a no-op fold) and "the build failed and
    /// the fold proceeded without a postings ref".
    pub postings_built: bool,
    /// Encoded size of the postings object, in bytes. `0` when
    /// `postings_built` is `false`.
    pub postings_bytes: u64,
    /// Number of per-part (v3, ADR-1413) column-statistics objects this fold
    /// built and PUT, one per newly written or rewritten part (never for a
    /// part carried forward by reference: its existing
    /// `SnapshotPartRef.column_stats` ref, if any, is forwarded unchanged).
    /// `0` on a no-op fold or when the tenant has no configured typed
    /// columns. A part whose statistics exceed the ceiling still gets an
    /// object here (ADR-1413 decision 4, amended): the fold degrades it by
    /// dropping dictionaries (see [`Self::column_stats_dictionaries_dropped`])
    /// rather than skipping it, and only fails the whole fold when no
    /// dictionary is left and it is still over ceiling.
    pub column_stats_part_objects_built: u64,
    /// Total (segment, column) dictionaries this fold dropped (largest
    /// encoded size first, `dictionary_present` set to `false`) across every
    /// per-part column-statistics object, to bring an over-ceiling part's
    /// body under [`crate::snapshot_format::DEFAULT_MAX_COLUMN_STATS_BYTES`]
    /// (ADR-1413 decision 4, amended). `0` when every part's statistics
    /// already fit the ceiling with every dictionary intact. min/max/count/sum
    /// are never affected: only the dictionary is dropped, never truncated
    /// (ADR-0850 decision 3's omit-never-truncate rule).
    pub column_stats_dictionaries_dropped: u64,
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
    /// Ingest hours the targeted re-fold pass reconciled this fold, from a
    /// caller-supplied [`RefoldRequest`] (issue #526, ADR-0063 amendment).
    /// Bounded by the same `frontier_reconcile_max_hours` cap the frontier
    /// band uses; a request naming more hours than the cap allows carries the
    /// remainder to whichever later fold the requester asks again, and this
    /// field counts only the hours this call actually re-listed. `0` on a
    /// [`Catalog::fold`] call (an empty [`RefoldRequest`]), on a first fold or
    /// a rebuild (both derive every hour from the commit layout, so the pass
    /// does not run), on a no-op fold over a HEAD with no held-open hours
    /// (it returns before any reconcile), and when every requested hour is
    /// one this fold lists anyway (the fixed window, the frontier band, or
    /// the held-open hours) or one no snapshot entry names. A fold that
    /// re-lists held-open hours without advancing the watermark runs the
    /// pass before deciding `no_op`, so this field can be nonzero on a no-op
    /// report. The requester resubmits a still-held hour on its next sweep.
    pub refold_hours_reconciled: usize,
    /// Stamped carriers this fold read: L0 commit records whose
    /// `declared_column_stats` (field 20) was non-empty, plus L1 compaction
    /// parts whose own stamps (field 12) were non-empty. The read half of the
    /// ADR-0873 stamp-coverage pair. Counted off the wire before the
    /// statistics validity predicate runs, so a carrier whose every entry is
    /// defective still counts here. Rewrite output parts are excluded: they
    /// carry no stamps by decision 3, so counting them would report a
    /// permanent shortfall. An exact count for this fold, not a running total.
    pub stamped_records: u64,
    /// Snapshot entries this fold built carrying at least one declared-column
    /// stamp, from either carrier: the carry half of the same pair. Tallied
    /// where the entry is built, so it counts what the fold carried forward
    /// rather than what a later step wrote: a bucket the reconcile pass
    /// re-reads and leaves unchanged, and an entry dropped as a duplicate
    /// identity, are both counted here and on the records side alike. Never
    /// above `stamped_records` in this report, since only a stamped carrier
    /// can produce a stamped entry. Below it means every entry of some stamped carrier was
    /// dropped on the way through, which is the coverage shortfall ADR-0873's
    /// deployment gate is read against.
    pub stamped_entries: u64,
    /// The operator-asserted seal-through hour this fold applied
    /// ([`Catalog::fold_with_seal_through`], ADR-2677 decision 1): `Some` only
    /// when a supplied hour was above the hour the seal margin alone seals at
    /// `now_ns`, and so set the fold's target. Set on a no-op as well, when
    /// that target was already at or below HEAD's watermark. `None` when no
    /// hour was supplied or the margin already sealed it.
    pub seal_through_hour: Option<u32>,
    /// Ranged GETs the key-index leaf build issued (ADR-2707 decision 4), for
    /// the entries of the parts this fold re-encoded: a footer suffix GET of
    /// at most [`KEY_INDEX_FOOTER_SUFFIX_LEN`] bytes, one more footer GET
    /// only when the footer is longer than that, and a GET of the KEY_IDX
    /// section when the footer names one. The total this fold issued: a
    /// part's built leaves are kept for the whole fold, so a HEAD CAS attempt
    /// after a lost one that re-encodes the same part issues none for it.
    /// Also counted in `get_requests`. For metrics the section is LABEL_DICT,
    /// and an entry whose object the postings build fetched whole and held
    /// within `key_index_build_max_pairs` is read from those bytes with no GET.
    /// Zero when
    /// [`CatalogConfig::build_key_index_leaves`](crate::CatalogConfig::build_key_index_leaves)
    /// is off, for metrics when
    /// [`CatalogConfig::metrics_label_index`](crate::CatalogConfig::metrics_label_index)
    /// is off, and for a logs tenant that declares no key-index column and
    /// leaves `index_trace_id` off.
    pub key_index_ranged_gets: u64,
    /// Bytes those GETs returned (footer suffixes plus KEY_IDX or, for
    /// metrics, LABEL_DICT sections, as transferred; a failed GET adds
    /// nothing). The total this fold read, like `key_index_ranged_gets`.
    pub key_index_bytes_read: u64,
    /// `.kidx` leaves the published HEAD names for the parts the publishing
    /// attempt re-encoded, one per (part, field, key slice): the published
    /// leaf set. Each was PUT by this fold (an `AlreadyExists` included: the
    /// content-addressed object is there either way), by the publishing
    /// attempt or by an attempt lost to a HEAD CAS that built the same part.
    /// A part carried forward by reference keeps its `key_index` refs and
    /// adds none. A leaf a build PUT and then abandoned, because a later pass
    /// found a new uncovered entry (see `key_index_build_passes`), is not
    /// named by HEAD and is not counted; `put_requests` counts its PUT and
    /// the catalog sweep removes it. Zero on a no-op fold.
    pub key_index_leaves_written: u64,
    /// Total bytes of those leaves.
    pub key_index_leaf_bytes_written: u64,
    /// For each part the publishing attempt re-encoded and each field of its
    /// key-index set, the part entries no leaf covers, summed: the entries
    /// whose `uncovered_entry_ordinals` lists them, plus every entry of a
    /// field that wrote no leaf.
    pub key_index_uncovered_entries: u64,
    /// KEY_IDX section reads the leaf build issued, one per (entry, pass)
    /// whatever the GETs each cost (`key_index_ranged_gets` counts those).
    /// An entry carried from a previous part's leaves is not read; an entry
    /// those leaves listed as uncovered is read again, and so is every entry
    /// of a previous part whose lowest leaves could not be read or verified,
    /// and every entry of a part whose build dropped the carry.
    pub key_index_sections_read: u64,
    /// Entries of the re-encoded parts whose pairs the leaf build took from
    /// the leaves of the previous part that held the same data object,
    /// instead of reading the entry's section: matched by data object key,
    /// never by ordinal, and only for an entry those leaves covered for every
    /// field.
    pub key_index_entries_carried: u64,
    /// Build passes over the re-encoded parts, summed. A part whose held
    /// pairs fit
    /// [`CatalogConfig::key_index_build_max_pairs`](crate::CatalogConfig::key_index_build_max_pairs)
    /// takes one; a part over it takes one pass per key8 range the pass rule
    /// that field documents cuts, and each pass reads every section the
    /// build needs again. A field a later pass finds a new uncovered entry
    /// for (a transient read failure, or a key8 over the body ceiling) keeps
    /// passing to the end of the key space and is then rebuilt once from its
    /// first range with the complete uncovered list, which adds its passes
    /// here.
    pub key_index_build_passes: u64,
    /// The pair budget the leaf build ran under:
    /// [`CatalogConfig::key_index_build_max_pairs`](crate::CatalogConfig::key_index_build_max_pairs),
    /// at least 1. Each field the build indexes gets `max(1, budget / F)` pairs a pass,
    /// where F is the number of fields in `key_index_field_pairs`. Zero when
    /// the fold builds no leaf for this tenant and signal: the switch is off,
    /// the signal is not indexed, a logs tenant declares no key-index field,
    /// or a metrics erasure is pending; and on a no-op fold, which publishes
    /// no leaf.
    pub key_index_pair_budget: u64,
    /// For each field the leaf build indexed, its key pairs, one per
    /// distinct (key8, entry), summed over the parts the publishing attempt
    /// re-encoded. A declared field with no pairs is present with 0, and so
    /// is a field a section named during the build. For a field rebuilt
    /// after a later pass found a new uncovered entry, the pairs of the
    /// rebuild only. Keyed by field name alone: one fold covers one signal.
    /// Empty when `key_index_pair_budget` is zero and on a fold that
    /// re-encoded no part.
    pub key_index_field_pairs: BTreeMap<String, u64>,
    /// `true` when the leaf switch
    /// ([`CatalogConfig::build_key_index_leaves`](crate::CatalogConfig::build_key_index_leaves))
    /// is off and the HEAD this fold read has a part carrying key-index leaf
    /// refs, which only a switch-on fold writes (ADR-3939 decision 4). Such a
    /// fold partitions by entry count alone, so it re-encodes every part
    /// whose span differs from the day-sealed one, and drops the leaf refs of
    /// every part it re-encodes. The fold proceeds unchanged and logs one
    /// warning naming the tenant and signal: turning the switch off on
    /// purpose looks the same. Always `false` with the switch on.
    pub key_index_switch_off_over_leaves: bool,
    /// Parts the published HEAD carries by reference, each with at least one
    /// entry, that hold no key-index leaf ref for at least one field the
    /// tenant's key-index set names at fold time: the declared logs fields,
    /// spans `trace_id`, metrics `label_value` under
    /// [`CatalogConfig::metrics_label_index`](crate::CatalogConfig::metrics_label_index),
    /// whether or not a metrics erasure request is pending. The fold never
    /// builds leaves for a part it carries (ADR-3939 decision 2), so such a
    /// part keeps its refs until `ravel-cli catalog fold --rebuild-key-index`
    /// attaches the missing ones; a metrics field over the byte cap, or a
    /// field no entry of the part indexes, stays missing after that too. Nonzero logs one warning per fold naming the
    /// tenant, signal and the missing fields. Zero with the leaf switch off
    /// and on a no-op fold.
    pub key_index_parts_missing_declared_leaves: u64,
    /// GETs of previous parts' leaves, whole objects, one per (field, slice)
    /// fetched, issued when a re-encoded part shares a data object with a
    /// previous part whose hour range overlaps it. Also counted in
    /// `get_requests`.
    pub key_index_leaf_gets: u64,
    /// The most previous-leaf bytes the incremental leaf build's carry held
    /// at once while building any one part in this fold, every source and
    /// field together: a slice is held from its GET until a pass starts past
    /// its `key8_hi`. Zero with the leaf switch off and when no previous slice
    /// was fetched; a slice fetched by a source's load that then failed on
    /// another field counts.
    pub key_index_carry_peak_held_bytes: u64,
    /// Re-encoded metrics parts the publishing attempt left with no
    /// `label_value` leaf because its leaves would have totalled more than
    /// [`CatalogConfig::metrics_label_index_max_ratio_ppm`](crate::CatalogConfig::metrics_label_index_max_ratio_ppm)
    /// of the part's entries' object bytes.
    pub metrics_label_index_parts_over_cap: u64,
    /// Re-encoded metrics parts the publishing attempt built no
    /// `label_value` leaf for because a metrics erasure request was pending
    /// (or the pending-erasure LIST failed) when the fold started.
    pub metrics_label_index_parts_erasure_gated: u64,
    /// LIST requests of the pending-erasure prefix the metrics leaf build
    /// issued: one listing per fold when
    /// [`CatalogConfig::metrics_label_index`](crate::CatalogConfig::metrics_label_index)
    /// and `build_key_index_leaves` are both on, zero otherwise. Also counted
    /// in `list_requests`.
    pub metrics_label_index_erasure_list_requests: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct RequestCounters {
    list_requests: u64,
    get_requests: u64,
    put_requests: u64,
    key_index_ranged_gets: u64,
    key_index_bytes_read: u64,
    key_index_sections_read: u64,
    key_index_entries_carried: u64,
    key_index_build_passes: u64,
    key_index_leaf_gets: u64,
    /// A maximum over the fold's part builds, not a sum.
    key_index_carry_peak_held_bytes: u64,
    metrics_label_index_erasure_list_requests: u64,
}

/// HEAD as read at the top of one fold attempt.
pub(crate) enum HeadState {
    Absent,
    /// `head` is boxed so this large variant does not inflate every
    /// `HeadState` value (`clippy::large_enum_variant`): `SnapshotHead` grew
    /// past 300 bytes when ADR-0850 added `column_stats`, while the other
    /// variants are tiny.
    Valid {
        head: Box<SnapshotHead>,
        version: Version,
    },
    /// HEAD object exists but failed to decode: logged loudly by
    /// [`Catalog::get_head`] and treated as absent for fold purposes.
    Corrupt {
        version: Version,
    },
    /// HEAD decoded far enough to see a `format_version` above every one of
    /// [`HEAD_FORMAT_VERSIONS`] (ADR-0066 decision 2, "fail-closed-on-newer");
    /// a version below them is [`HeadState::Corrupt`]. Unlike
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

/// Orders per-part (v3) column-statistics records by their join key (the
/// covered part's content hash, carried in `writer_id`) and collapses
/// repeats.
///
/// `encode_column_stats_v3` rejects a repeated key outright, and the fold
/// treats that error as "no field 7 ref for this part", so one repeat would
/// strip the part's statistics. A repeated key means two entries cover a
/// byte-identical part, so their records carry the same statistics and
/// keeping one is exact rather than a narrowing.
fn sort_and_dedup_part_segments(segments: &mut Vec<ColumnStatsSegment>) {
    segments.sort_by(|a, b| a.writer_id.cmp(&b.writer_id));
    segments.dedup_by(|a, b| a.writer_id == b.writer_id);
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

fn column_stats_object_key(
    tenant: &TenantHash,
    signal: Signal,
    watermark_hour: u32,
    hash16: &str,
) -> String {
    format!(
        "t/{}/catalog/{}/idx/{}.{}.cstat",
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

/// A decoded snapshot part's entries and the reservation that charges their
/// decoded body to the catalog's memory budget, held together until the
/// entries drop.
pub(crate) type ChargedEntries = (Vec<SnapshotEntry>, Reservation);

/// Whether `err` is `Catalog::load_part_entries` refusing a part whose header
/// names another tenant, the one load failure that is an isolation breach.
fn is_foreign_part(err: &CatalogError) -> bool {
    matches!(
        err,
        CatalogError::FieldMismatch {
            field: "tenant_hash",
            ..
        }
    )
}

fn entry_identity(entry: &SnapshotEntry) -> EntryIdentity {
    (
        entry.ingest_hour_bucket,
        entry.shard,
        entry.writer_id.clone(),
        entry.writer_epoch,
        entry.writer_seq,
    )
}

fn build_snapshot_entry(
    key: &str,
    record: &CommitRecord,
    coverage: &mut declared_stats::StampCoverage,
) -> Result<SnapshotEntry, CatalogError> {
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
        // ADR-0873 decision 4: the stamps are copied from the record the fold
        // is already reading, through the gated read that binds the row-count
        // clauses against this record's own sample_count. A defective entry is
        // dropped here rather than sealed into a part that outlives the record
        // proving it wrong.
        declared_column_stats: declared_stats::carry_commit_record(record, coverage),
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
    coverage: &mut declared_stats::StampCoverage,
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
        // The part's own stamps (CompactionPart field 12), gated against the
        // part's own row count exactly as the L0 path gates a commit record's
        // (ADR-0873 decision 4), and tallied into the same coverage pair: a
        // stamped entry built here counts as much as one built from an L0
        // record.
        declared_column_stats: declared_stats::carry_compaction_part(part, coverage),
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
        // NEVER carried for a rewrite output (ADR-0873 decision 3): the
        // rewrite dropped rows, so a stamp computed before the drop is stale
        // for the surviving rows. Every rewrite writer emits empty stamps
        // today; forcing empty here also refuses a stamped rewrite part from
        // a buggy or future writer until the recompute path lands with its
        // own tests and flips this line deliberately. Counted on neither side
        // of the stamp-coverage pair for the same reason: a carrier that can
        // never carry would report a permanent shortfall.
        declared_column_stats: Vec::new(),
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

/// The number of bytes a protobuf varint encodes `v` in: 7 payload bits per
/// byte, continuation bit set on every byte but the last. Used by the
/// per-part column-stats degrade loop to track, without re-encoding, how a
/// message's own length-delimiter width changes when its content shrinks.
fn varint_len(v: u64) -> u64 {
    let mut len = 1u64;
    let mut rest = v >> 7;
    while rest > 0 {
        len += 1;
        rest >>= 7;
    }
    len
}

// Test-only instrumentation for #1482: counts calls to
// `column_stats_segments_concat` made by the per-part column-stats degrade
// loop, so a regression test can pin the loop at measuring the body a
// bounded number of times rather than once per dictionary dropped.
#[cfg(test)]
thread_local! {
    static COLUMN_STATS_CONCAT_CALLS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn record_column_stats_concat_call_for_test() {
    COLUMN_STATS_CONCAT_CALLS.with(|c| c.set(c.get() + 1));
}

#[cfg(test)]
fn column_stats_concat_calls_for_test() -> u64 {
    COLUMN_STATS_CONCAT_CALLS.with(std::cell::Cell::get)
}

#[cfg(test)]
fn reset_column_stats_concat_calls_for_test() {
    COLUMN_STATS_CONCAT_CALLS.with(|c| c.set(0));
}

// Test-only: a key-index field whose leaf encode is refused as if over the
// body ceiling. Every encoder refusal needs input the fold never builds from
// writer-validated sections, so the refusal branch is reached through this.
#[cfg(test)]
thread_local! {
    static REFUSE_KEY_INDEX_LEAF_ENCODE: std::cell::RefCell<Option<String>> =
        const { std::cell::RefCell::new(None) };
}

#[cfg(test)]
fn refuse_key_index_leaf_encode_for_test(field: Option<&str>) {
    REFUSE_KEY_INDEX_LEAF_ENCODE.with(|f| *f.borrow_mut() = field.map(str::to_string));
}

// Test-only: a leaf body ceiling below the default, so a fold over a small
// fixture builds a field in several slices.
#[cfg(test)]
thread_local! {
    static KEY_INDEX_LEAF_BODY_CEILING: std::cell::Cell<Option<u64>> =
        const { std::cell::Cell::new(None) };
}

#[cfg(test)]
fn set_key_index_leaf_body_ceiling_for_test(ceiling: Option<u64>) {
    KEY_INDEX_LEAF_BODY_CEILING.with(|c| c.set(ceiling));
}

/// The leaf limits the fold builds with.
fn key_index_leaf_limits() -> KeyIndexLeafLimits {
    #[cfg(test)]
    if let Some(max_body_bytes) = KEY_INDEX_LEAF_BODY_CEILING.with(std::cell::Cell::get) {
        return KeyIndexLeafLimits { max_body_bytes };
    }
    KeyIndexLeafLimits::default()
}

fn encode_part_key_index_leaf(
    input: &KeyIndexLeafInput<'_>,
    limits: &KeyIndexLeafLimits,
) -> Result<EncodedKeyIndexLeaf, KeyIndexLeafEncodeError> {
    #[cfg(test)]
    if REFUSE_KEY_INDEX_LEAF_ENCODE.with(|f| f.borrow().as_deref() == Some(input.field)) {
        return Err(KeyIndexLeafEncodeError::BodyOverCeiling {
            body: u64::MAX,
            ceiling: limits.max_body_bytes,
        });
    }
    encode_key_index_leaf(input, limits)
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

/// One span's part encoding, computed once and reused by both the
/// `reused_old_part_hashes` baseline pass and the main per-span build loop,
/// so a span is encoded and hashed once per fold. `min_hour`/`watermark` are
/// this span's already-resolved
/// part bounds (single-part/tail overrides applied), not the raw `PartSpan`
/// values.
struct SpanEncoding {
    min_hour: u32,
    watermark: u32,
    bytes: Vec<u8>,
    hash: blake3::Hash,
}

/// The sealing policy [`partition_parts`] applies (ADR-0063 section 1, as
/// amended by ADR-3939 decision 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SealPolicy {
    /// The entry threshold: [`CatalogConfig::snapshot_part_max_entries`].
    max_entries: usize,
    /// Also seal at every UTC day boundary:
    /// [`CatalogConfig::build_key_index_leaves`], so a part never spans two
    /// UTC days and a sealed day keeps its key-index leaves.
    seal_utc_days: bool,
}

/// The UTC day of an ingest hour. `ingest_hour_bucket` counts whole hours
/// since the unix epoch, which is a UTC midnight, and unix time has no leap
/// seconds, so every UTC day is exactly 24 of them.
fn utc_day_of_hour(hour: u32) -> u32 {
    hour / 24
}

/// Partition the fully hour-major-sorted `entries` into hour-range parts under
/// the sealing policy (ADR-0063 section 1): whole hours accumulate into the
/// current part until adding the next hour's run would push the part's entry
/// count past `policy.max_entries`, at which point the part seals at that hour
/// boundary and a fresh part starts. With `policy.seal_utc_days` (ADR-3939
/// decision 1) the part also seals when the next hour's run falls on a later
/// UTC day than the part's first hour, so no part spans two UTC days; a day
/// with no entries produces no part, and the entry threshold still splits
/// inside a day. Splits are always at hour boundaries, so a single hour larger
/// than `max_entries` produces one oversized part (the decode cap still bounds
/// it) rather than being split across parts -- which would break the free
/// per-part duplicate-freedom proof, since the dedup identity leads with the
/// hour.
///
/// Always returns at least one span. An empty entry set yields a single empty
/// `[0, 0)` span with `min_hour`/`watermark_hour` 0, so an empty fold still
/// writes one (empty) part exactly as the single-part path always has. Each
/// span's `watermark_hour` here is the last ingest hour it actually contains;
/// the caller overrides the tail span's watermark with the fold watermark.
fn partition_parts(entries: &[SnapshotEntry], policy: SealPolicy) -> Vec<PartSpan> {
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
    let cap = policy.max_entries.max(1);
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
        // entries and admitting this whole hour would exceed the cap, or
        // would carry the part into a later UTC day.
        let over_cap = cur_count + run_len > cap;
        let next_day =
            policy.seal_utc_days && utc_day_of_hour(hour) != utc_day_of_hour(cur_min_hour);
        if cur_count > 0 && (over_cap || next_day) {
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
/// three shapes are the reconcile triggers. An hour sealed only by an
/// operator-asserted seal is not yet sealed in the lemma's sense, so the
/// reconcile pass also folds an unrecognised L0 record there (see
/// [`held_open_hours`]).
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

/// The hours a HEAD with watermark `head_watermark` covers only through an
/// operator-asserted seal at a fold whose margin hour is `margin_hour`: every
/// hour above the margin hour up to the watermark, inclusive (ADR-2677
/// decision 1). The seal lemma does not hold for these hours yet, since a
/// writer that started after the seal may still publish into them, so the
/// fold keeps re-listing them until the margin alone seals them. `None` when
/// the margin already seals the watermark, which is every HEAD no seal-through
/// hour raised.
///
/// The range never reaches below `window` hours under the watermark, the
/// reconcile window a fold already pays for. A watermark far above the margin
/// (a seal asserted from a fast clock, or a margin larger than the clock)
/// would otherwise make every fold list an unbounded range.
fn held_open_hours(
    head_watermark: u32,
    margin_hour: Option<u32>,
    window: u32,
) -> Option<(u32, u32)> {
    let floor = head_watermark.saturating_sub(window);
    match margin_hour {
        Some(margin) if margin >= head_watermark => None,
        Some(margin) => Some((margin.saturating_add(1).max(floor), head_watermark)),
        None => Some((floor, head_watermark)),
    }
}

/// Whether `listing` names a level-0 commit record whose identity
/// `is_folded` does not recognise, reading only the keys: a commit key carries
/// its full entry identity, so no record GET is needed to tell a commit
/// already in the snapshot from one published after the fold that sealed its
/// hour.
fn lists_an_unfolded_commit(
    listing: &[ObjectMeta],
    is_folded: impl Fn(&EntryIdentity) -> bool,
) -> bool {
    listing.iter().any(|meta| {
        let Ok(BucketEntry::CommitRecord(parsed)) = keys::partition_bucket_entry(&meta.key) else {
            return false;
        };
        !is_folded(&(
            parsed.ingest_hour_bucket,
            parsed.shard,
            parsed.writer_id.as_bytes().to_vec(),
            parsed.epoch,
            parsed.seq,
        ))
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
/// Both sparse reconcile passes need this shape, which
/// [`hour_range_buckets`]' contiguous `[lo, hi]` range cannot express: the
/// retention-frontier reconcile (ADR-0020) over the snapshot-named hours at or
/// below the frontier, and the targeted re-fold pass (issue #526) over the
/// hours a [`RefoldRequest`] names.
fn hour_set_buckets(generations: &[ShardGeneration], hours: &[u32]) -> Vec<(u32, u32)> {
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

/// Retention tombstones the reconcile passes found in a previous HEAD's
/// sealed, leaf-bearing parts (ADR-3939 decision 6). Such a bucket is set
/// aside instead of being dropped from `entries` on the spot, and
/// [`apply_retirements`] decides after every pass has listed whether its
/// hour is recorded as a retired hour of the carried part, drops the whole
/// part, or takes the re-encode path.
#[derive(Default)]
struct RetireTracker {
    /// `[min_hour, watermark_hour]` of each retirable previous part. Empty
    /// with the leaf switch off and on a rebuilt fold.
    ranges: Vec<(u32, u32)>,
    /// Tombstoned `(hour, shard)` buckets inside a retirable part.
    deferred: BTreeSet<(u32, u32)>,
}

impl RetireTracker {
    /// The retirable parts of `previous`: every part but the last that
    /// carries key-index leaves. `leaf_switch_on` false leaves it empty, so
    /// every tombstone takes the re-encode path.
    fn of_head(previous: Option<&SnapshotHead>, leaf_switch_on: bool) -> Self {
        let ranges = match previous {
            Some(head) if leaf_switch_on => {
                let sealed = head.parts.len().saturating_sub(1);
                head.parts[..sealed]
                    .iter()
                    .filter(|part| !part.key_index.is_empty())
                    .map(|part| (part.min_hour, part.watermark_hour))
                    .collect()
            }
            _ => Vec::new(),
        };
        Self {
            ranges,
            deferred: BTreeSet::new(),
        }
    }

    /// Records a tombstoned bucket and returns true when `hour` lies in a
    /// retirable part; false leaves the bucket to the normal reconcile.
    fn defer(&mut self, shard: u32, hour: u32, listing: &[ObjectMeta]) -> bool {
        let in_part = self
            .ranges
            .iter()
            .any(|&(min_hour, watermark)| min_hour <= hour && hour <= watermark);
        if !in_part || !lists_a_tombstone(listing) {
            return false;
        }
        self.deferred.insert((hour, shard));
        true
    }
}

fn lists_a_tombstone(listing: &[ObjectMeta]) -> bool {
    listing.iter().any(|meta| {
        matches!(
            keys::partition_bucket_entry(&meta.key),
            Ok(BucketEntry::Tombstone(_))
        )
    })
}

/// Removes every entry `drop` selects from `entries`, rebuilding `seen` and
/// marking `hours` dirty when anything was removed. The carry keys of the
/// parts holding those hours are derived first, as for any reconcile change.
#[allow(clippy::too_many_arguments)]
fn drop_entries(
    entries: &mut Vec<SnapshotEntry>,
    seen: &mut HashMap<EntryIdentity, (usize, bool)>,
    dirty_hours: &mut HashSet<u32>,
    carry_sources: &[CarrySource],
    tenant: &TenantHash,
    signal: Signal,
    hours: &[u32],
    drop: impl Fn(&SnapshotEntry) -> bool,
) {
    if !entries.iter().any(&drop) {
        return;
    }
    derive_carry_keys_before_change(carry_sources, tenant, signal, entries, hours);
    entries.retain(|entry| !drop(entry));
    *seen = entries
        .iter()
        .enumerate()
        .map(|(index, entry)| (entry_identity(entry), (index, true)))
        .collect();
    dirty_hours.extend(hours.iter().copied());
}

/// Settles the retired hours of every previous part (ADR-3939 decision 6),
/// after the reconcile passes and before the entries are partitioned.
/// Returns the new retired-hours list of each part carried by reference
/// whose list grew, keyed by its blake3; every other carried part keeps its
/// own list.
///
/// For each part, in HEAD order:
/// - With the leaf switch off, a part's retired hours lose their entries
///   now, so it re-encodes without them and the field is never written.
/// - A part that re-encodes this fold anyway (a reconcile changed one of its
///   hours) drops its retired hours and every set-aside bucket for good.
/// - So does a part with a set-aside hour that some shard holding entries
///   in it was not tombstoned in: retirement is per hour.
/// - Otherwise each fully tombstoned hour is added to the part's retired
///   hours, and a part all of whose hours are retired drops out of HEAD with
///   its entries, no PUT.
#[allow(clippy::too_many_arguments)]
fn apply_retirements(
    previous: Option<&SnapshotHead>,
    leaf_switch_on: bool,
    tracker: &RetireTracker,
    entries: &mut Vec<SnapshotEntry>,
    seen: &mut HashMap<EntryIdentity, (usize, bool)>,
    dirty_hours: &mut HashSet<u32>,
    carry_sources: &[CarrySource],
    tenant: &TenantHash,
    signal: Signal,
) -> HashMap<Vec<u8>, Vec<u32>> {
    let mut grown: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
    let Some(head) = previous else {
        return grown;
    };
    for part in &head.parts {
        let range = part.min_hour..=part.watermark_hour;
        let deferred: BTreeMap<u32, BTreeSet<u32>> = tracker
            .deferred
            .iter()
            .filter(|(hour, _)| range.contains(hour))
            .fold(BTreeMap::new(), |mut by_hour, &(hour, shard)| {
                by_hour
                    .entry(hour)
                    .or_insert_with(BTreeSet::new)
                    .insert(shard);
                by_hour
            });
        if part.retired_hours.is_empty() && deferred.is_empty() {
            continue;
        }
        let mut shards_by_hour: BTreeMap<u32, BTreeSet<u32>> = BTreeMap::new();
        for entry in entries.iter() {
            if range.contains(&entry.ingest_hour_bucket) {
                shards_by_hour
                    .entry(entry.ingest_hour_bucket)
                    .or_default()
                    .insert(entry.shard);
            }
        }
        let fully_tombstoned = |hour: &u32, shards: &BTreeSet<u32>| {
            shards_by_hour
                .get(hour)
                .is_none_or(|held| held.is_subset(shards))
        };
        let re_encodes = !leaf_switch_on
            || part_covers_dirty_hour(part, dirty_hours)
            || !deferred
                .iter()
                .all(|(hour, shards)| fully_tombstoned(hour, shards));
        if re_encodes {
            let retired: BTreeSet<u32> = part.retired_hours.iter().copied().collect();
            let hours: Vec<u32> = retired.iter().chain(deferred.keys()).copied().collect();
            drop_entries(
                entries,
                seen,
                dirty_hours,
                carry_sources,
                tenant,
                signal,
                &hours,
                |entry| {
                    retired.contains(&entry.ingest_hour_bucket)
                        || deferred
                            .get(&entry.ingest_hour_bucket)
                            .is_some_and(|shards| shards.contains(&entry.shard))
                },
            );
            continue;
        }
        let mut retired: BTreeSet<u32> = part.retired_hours.iter().copied().collect();
        retired.extend(
            deferred
                .keys()
                .copied()
                .filter(|hour| shards_by_hour.contains_key(hour)),
        );
        if retired.len() == part.retired_hours.len() {
            continue;
        }
        if shards_by_hour.keys().all(|hour| retired.contains(hour)) {
            let hours: Vec<u32> = shards_by_hour.keys().copied().collect();
            drop_entries(
                entries,
                seen,
                dirty_hours,
                carry_sources,
                tenant,
                signal,
                &hours,
                |entry| range.contains(&entry.ingest_hour_bucket),
            );
            continue;
        }
        grown.insert(part.blake3.clone(), retired.into_iter().collect());
    }
    grown
}

/// The retired hours of `previous`'s parts, as [`apply_retirements`] left
/// them in `grown`, that hold an entry of a span this fold does not carry by
/// reference. A late commit through a held-open hour, a change of the entry
/// threshold, or any other change that moves a span off its previous part's
/// bytes lands here.
fn stranded_retired_hours(
    previous: Option<&SnapshotHead>,
    grown: &HashMap<Vec<u8>, Vec<u32>>,
    spans: &[PartSpan],
    encodings: &[Result<SpanEncoding, SnapshotFormatError>],
    carried: &HashMap<Vec<u8>, SnapshotPartRef>,
    entries: &[SnapshotEntry],
) -> BTreeSet<u32> {
    let retired: HashSet<u32> = previous
        .into_iter()
        .flat_map(|head| &head.parts)
        .flat_map(|part| grown.get(&part.blake3).unwrap_or(&part.retired_hours))
        .copied()
        .collect();
    if retired.is_empty() {
        return BTreeSet::new();
    }
    spans
        .iter()
        .zip(encodings)
        .filter(|(_, encoding)| {
            encoding
                .as_ref()
                .is_ok_and(|encoding| !carried.contains_key(encoding.hash.as_bytes().as_slice()))
        })
        .flat_map(|(span, _)| &entries[span.start..span.end])
        .map(|entry| entry.ingest_hour_bucket)
        .filter(|hour| retired.contains(hour))
        .collect()
}

/// Bytes of a data object's tail the key-index leaf build reads to open its
/// footer. A footer longer than this costs one more ranged GET.
pub(crate) const KEY_INDEX_FOOTER_SUFFIX_LEN: u64 = 8 * 1024;

fn key_index_object_key(
    tenant: &TenantHash,
    signal: Signal,
    watermark_hour: u32,
    hash16: &str,
) -> String {
    format!(
        "t/{}/catalog/{}/idx/{}.{}.kidx",
        tenant.to_hex(),
        signal.key_prefix(),
        keys::ingest_hour_string(watermark_hour),
        hash16
    )
}

/// The logs key-index fields `config` declares, as the writer derives them:
/// each `key_index` column under its declared type, then `trace_id` when
/// `index_trace_id` is set. A declaration that does not resolve declares
/// nothing, and a tenant declaring nothing gets no leaves.
fn declared_logs_key_index_fields(
    config: &crate::tenant_config::TenantConfig,
    tenant: &TenantHash,
) -> Vec<(String, KeyType)> {
    use crate::tenant_config::DeclaredColumnType;
    let columns = match config.key_index_columns() {
        Ok(columns) => columns,
        Err(error) => {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                %error,
                "tenant key-index columns do not resolve; with index_trace_id off this fold builds no leaf, otherwise it indexes trace_id and the fields the sections name"
            );
            Vec::new()
        }
    };
    let mut fields: Vec<(String, KeyType)> = columns
        .into_iter()
        .filter_map(|column| {
            let key_type = match column.ty {
                DeclaredColumnType::I64 => KeyType::I64,
                DeclaredColumnType::Str => KeyType::Str,
                DeclaredColumnType::Bytes => KeyType::Bytes,
                DeclaredColumnType::Bool => return None,
            };
            Some((column.key, key_type))
        })
        .collect();
    if config.index_trace_id() {
        fields.push((
            ravel_logseg::key_index::TRACE_ID_FIELD.to_string(),
            KeyType::Id16,
        ));
    }
    fields
}

/// The key-index fields `signal`'s objects carry for a tenant whose config is
/// `config`: the declared logs fields (see [`declared_logs_key_index_fields`],
/// none without a config), `trace_id` for spans, and for metrics
/// [`METRICS_LABEL_VALUE_FIELD`] when `catalog_config` turns on
/// [`CatalogConfig::metrics_label_index`]. `None` for a signal the leaf build
/// does not index.
pub(crate) fn declared_key_index_fields(
    signal: Signal,
    config: Option<&crate::tenant_config::TenantConfig>,
    tenant: &TenantHash,
    catalog_config: &CatalogConfig,
) -> Option<Vec<(String, KeyType)>> {
    match signal {
        Signal::Logs => Some(
            config
                .map(|cfg| declared_logs_key_index_fields(cfg, tenant))
                .unwrap_or_default(),
        ),
        Signal::Spans => Some(vec![(
            ravel_rspan::TRACE_ID_FIELD.to_string(),
            KeyType::Id16,
        )]),
        Signal::Metrics if catalog_config.metrics_label_index => {
            Some(vec![(METRICS_LABEL_VALUE_FIELD.to_string(), KeyType::Str)])
        }
        _ => None,
    }
}

/// The metrics key-index field (ADR-3694 decision 2): key8 of every
/// LABEL_DICT string of an object, label names and values alike, each at
/// block ordinal 0, which stands for the whole object.
pub const METRICS_LABEL_VALUE_FIELD: &str = "label_value";

/// RSEG section kind of LABEL_DICT (docs/segment-format.md).
const RSEG_LABEL_DICT_KIND: u32 = 1;

/// One metrics object's `label_value` field from its footer and its
/// LABEL_DICT section's stored bytes: `(key8_str(s), 0)` for every string,
/// sorted and deduplicated.
fn metrics_label_value_entries(
    footer: &ravel_segment::Footer,
    label_dict_stored: &[u8],
) -> Result<Vec<KeyIdxFieldEntries>, String> {
    let strings =
        ravel_segment::label_dict_strings(footer, label_dict_stored, ReaderLimits::default())
            .map_err(|e| format!("LABEL_DICT: {e}"))?;
    let mut entries: Vec<(ravel_codec::key_idx::Key8, u32)> = strings
        .iter()
        .map(|s| (ravel_codec::key_idx::key8_str(s), 0))
        .collect();
    entries.sort_unstable();
    entries.dedup();
    Ok(vec![KeyIdxFieldEntries {
        name: METRICS_LABEL_VALUE_FIELD.to_string(),
        key_type: KeyType::Str,
        entries,
    }])
}

/// The `label_value` field of a metrics object the fold already holds whole,
/// read with no GET. The footer must name `tenant`.
fn metrics_label_value_from_object(
    tenant: &TenantHash,
    object: &[u8],
    location: &ravel_segment::FooterLocation,
) -> EntryKeyIndex {
    if location.footer.tenant_hash.as_slice() != tenant.0.as_slice() {
        return Err("footer names another tenant".to_string());
    }
    let section = location
        .footer
        .sections
        .iter()
        .find(|s| s.kind == RSEG_LABEL_DICT_KIND)
        .ok_or_else(|| "object has no LABEL_DICT section".to_string())?;
    let stored = usize::try_from(section.offset)
        .ok()
        .zip(usize::try_from(section.len).ok())
        .and_then(|(offset, len)| object.get(offset..offset.checked_add(len)?))
        .ok_or_else(|| "LABEL_DICT range is outside the object".to_string())?;
    metrics_label_value_entries(&location.footer, stored)
}

/// The metrics `label_value` fields the postings build read from objects it
/// fetched whole, keyed by the entry's ordinal in the fold's sorted entries,
/// for the leaf build to read with no GET. Only entries of a part this
/// attempt will build a leaf for are held, and the held pairs are charged
/// against `key_index_build_max_pairs`: an object whose pairs would take the
/// held total past it is not held, and the leaf build reads it by range. A
/// part's entries are released once its leaf is built.
pub(crate) struct HeldLabelDicts {
    /// Entry ordinal ranges of the parts the attempt builds a leaf for,
    /// ascending and disjoint.
    wanted: Vec<std::ops::Range<usize>>,
    budget: u64,
    pairs: u64,
    by_ordinal: BTreeMap<usize, (EntryKeyIndex, u64)>,
}

impl HeldLabelDicts {
    pub(crate) fn new(wanted: Vec<std::ops::Range<usize>>, budget: u64) -> Self {
        Self {
            wanted,
            budget,
            pairs: 0,
            by_ordinal: BTreeMap::new(),
        }
    }

    /// Whether entry `ordinal` belongs to a part the attempt builds a leaf
    /// for.
    fn wants(&self, ordinal: usize) -> bool {
        let i = self.wanted.partition_point(|range| range.end <= ordinal);
        self.wanted
            .get(i)
            .is_some_and(|range| range.contains(&ordinal))
    }

    /// Holds entry `ordinal`'s field when the entry is wanted and its pairs
    /// fit the budget; `read` runs only for a wanted entry.
    fn offer(&mut self, ordinal: usize, read: impl FnOnce() -> EntryKeyIndex) {
        if !self.wants(ordinal) {
            return;
        }
        let fields = read();
        let pairs = fields
            .as_ref()
            .map(|fields| fields.iter().map(|f| f.entries.len() as u64).sum())
            .unwrap_or(0);
        let Some(total) = self
            .pairs
            .checked_add(pairs)
            .filter(|&total| total <= self.budget)
        else {
            return;
        };
        self.pairs = total;
        self.by_ordinal.insert(ordinal, (fields, pairs));
        #[cfg(test)]
        HELD_LABEL_DICT_LOG.with(|log| {
            let mut log = log.borrow_mut();
            log.held_ordinals.push(ordinal);
            log.peak_pairs = log.peak_pairs.max(total);
        });
    }

    /// Drops every held entry in `ordinals`.
    fn release(&mut self, ordinals: std::ops::Range<usize>) {
        let held: Vec<usize> = self.by_ordinal.range(ordinals).map(|(&o, _)| o).collect();
        for ordinal in held {
            if let Some((_, pairs)) = self.by_ordinal.remove(&ordinal) {
                self.pairs -= pairs;
            }
        }
        #[cfg(test)]
        HELD_LABEL_DICT_LOG.with(|log| log.borrow_mut().pairs_after_release.push(self.pairs));
    }

    /// The held entries of the part whose first entry is at `start`.
    fn part(&self, start: usize) -> HeldPart<'_> {
        HeldPart { held: self, start }
    }
}

/// [`HeldLabelDicts`] seen from one part, by the part's own entry ordinal.
#[derive(Clone, Copy)]
pub(crate) struct HeldPart<'a> {
    held: &'a HeldLabelDicts,
    start: usize,
}

impl HeldPart<'_> {
    fn get(&self, part_ordinal: u64) -> Option<&EntryKeyIndex> {
        let ordinal = usize::try_from(part_ordinal)
            .ok()?
            .checked_add(self.start)?;
        self.held.by_ordinal.get(&ordinal).map(|(fields, _)| fields)
    }
}

// Test-only: the fold ordinals [`HeldLabelDicts`] held, the most pairs it
// held at once, and the pairs it still held after each part's release.
#[cfg(test)]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct HeldLabelDictLog {
    pub(crate) held_ordinals: Vec<usize>,
    pub(crate) peak_pairs: u64,
    pub(crate) pairs_after_release: Vec<u64>,
}

#[cfg(test)]
thread_local! {
    static HELD_LABEL_DICT_LOG: std::cell::RefCell<HeldLabelDictLog> =
        std::cell::RefCell::new(HeldLabelDictLog::default());
}

#[cfg(test)]
pub(crate) fn take_held_label_dict_log_for_test() -> HeldLabelDictLog {
    HELD_LABEL_DICT_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()))
}

/// Reads a metrics object's `label_value` field by range: a suffix GET for
/// the RSEG footer (a second footer GET only when the footer is longer than
/// [`KEY_INDEX_FOOTER_SUFFIX_LEN`]), then one GET of LABEL_DICT.
async fn read_metrics_label_value_counted(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    key: &str,
    object_size: u64,
    gets: &mut u64,
    bytes: &mut u64,
) -> Result<Vec<KeyIdxFieldEntries>, KeyIdxReadError> {
    use ravel_segment::FooterOutcome;
    if object_size == 0 {
        return Err("entry names a zero-byte object".to_string().into());
    }
    let limits = ReaderLimits::default();
    let suffix_len = object_size.min(KEY_INDEX_FOOTER_SUFFIX_LEN);
    *gets += 1;
    let tail = store
        .get(key, GetRange::Suffix(suffix_len))
        .await
        .map_err(|e| KeyIdxReadError::Get(format!("footer suffix GET: {e}")))?;
    *bytes += tail.data.len() as u64;
    let total_size = tail.total_size;
    let location = match ravel_segment::open_from_suffix(&tail.data, total_size, limits)
        .map_err(|e| format!("RSEG footer: {e}"))?
    {
        FooterOutcome::Ready(location) => location,
        FooterOutcome::NeedRange { offset, len } => {
            let end = offset
                .checked_add(len)
                .ok_or_else(|| "footer range overflows".to_string())?;
            *gets += 1;
            let got = store
                .get(key, GetRange::Range(offset, end))
                .await
                .map_err(|e| KeyIdxReadError::Get(format!("footer range GET: {e}")))?;
            *bytes += got.data.len() as u64;
            match ravel_segment::open_from_suffix(&got.data, total_size, limits)
                .map_err(|e| format!("RSEG footer: {e}"))?
            {
                FooterOutcome::Ready(location) => location,
                FooterOutcome::NeedRange { .. } => {
                    return Err("footer range GET did not cover the footer"
                        .to_string()
                        .into());
                }
            }
        }
    };
    if location.footer.tenant_hash.as_slice() != tenant.0.as_slice() {
        return Err("footer names another tenant".to_string().into());
    }
    let Some(section) = location
        .footer
        .sections
        .iter()
        .find(|s| s.kind == RSEG_LABEL_DICT_KIND)
    else {
        return Err("object has no LABEL_DICT section".to_string().into());
    };
    let end = section
        .offset
        .checked_add(section.len)
        .ok_or_else(|| "LABEL_DICT range overflows".to_string())?;
    *gets += 1;
    let stored = store
        .get(key, GetRange::Range(section.offset, end))
        .await
        .map_err(|e| KeyIdxReadError::Get(format!("LABEL_DICT section GET: {e}")))?;
    *bytes += stored.data.len() as u64;
    Ok(metrics_label_value_entries(&location.footer, &stored.data)?)
}

/// PUTs one encoded leaf at its content-addressed key under the covered
/// part's `watermark_hour` and returns its ref with `key` filled in. A leaf
/// already stored under that key holds identical bytes, so `AlreadyExists` is
/// success.
pub(crate) async fn put_key_index_leaf(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    watermark_hour: u32,
    leaf: EncodedKeyIndexLeaf,
) -> Result<SnapshotKeyIndexLeafRef, StoreError> {
    let EncodedKeyIndexLeaf {
        bytes,
        mut leaf_ref,
    } = leaf;
    let leaf_key = key_index_object_key(
        tenant,
        signal,
        watermark_hour,
        &hex_encode(&leaf_ref.blake3)[..16],
    );
    let leaf_crc = crc32c::crc32c(&bytes);
    match store
        .put(
            &leaf_key,
            Bytes::from(bytes),
            PutOptions::create_if_absent().with_checksum(UploadChecksum::Crc32c(leaf_crc)),
        )
        .await
    {
        Ok(_) | Err(StoreError::AlreadyExists) => {}
        Err(e) => return Err(e),
    }
    leaf_ref.key = leaf_key;
    Ok(leaf_ref)
}

/// One entry's tier-1 KEY_IDX fields, or why the entry is uncovered for every
/// field.
pub(crate) type EntryKeyIndex = Result<Vec<KeyIdxFieldEntries>, String>;

/// One data object's KEY_IDX read and the ranged GETs and bytes it cost.
pub(crate) struct EntryKeyIndexRead {
    pub(crate) fields: EntryKeyIndex,
    /// Whether a GET failed, as opposed to the object's bytes not yielding a
    /// section.
    pub(crate) get_failed: bool,
    pub(crate) gets: u64,
    pub(crate) bytes: u64,
}

/// Why an object's KEY_IDX read leaves its entry uncovered.
enum KeyIdxReadError {
    Get(String),
    Invalid(String),
}

impl From<String> for KeyIdxReadError {
    fn from(reason: String) -> Self {
        Self::Invalid(reason)
    }
}

/// What a data object's footer says about its KEY_IDX section.
struct KeyIdxFooter {
    tenant_hash: [u8; 16],
    block_count: u64,
    /// `(offset, len, crc32c, prefix_len)` of the section, `None` when absent.
    section: Option<(u64, u64, u32, u32)>,
}

enum KeyIdxFooterOpen {
    Ready(KeyIdxFooter),
    NeedRange { offset: u64, len: u64 },
}

fn open_key_idx_footer(
    signal: Signal,
    suffix: &[u8],
    total_size: u64,
) -> Result<KeyIdxFooterOpen, String> {
    match signal {
        Signal::Logs => {
            use ravel_logseg::footer::{SuffixOutcome, kind, open_from_suffix};
            match open_from_suffix(suffix, total_size).map_err(|e| e.to_string())? {
                SuffixOutcome::Ready(footer) => Ok(KeyIdxFooterOpen::Ready(KeyIdxFooter {
                    tenant_hash: footer.tenant_hash,
                    block_count: footer.block_count,
                    section: footer
                        .section(kind::KEY_IDX)
                        .map(|d| (d.offset, d.len, d.crc32c, d.prefix_len)),
                })),
                SuffixOutcome::NeedRange { offset, len } => {
                    Ok(KeyIdxFooterOpen::NeedRange { offset, len })
                }
            }
        }
        Signal::Spans => {
            use ravel_rspan::footer::{SuffixOutcome, kind, open_from_suffix};
            match open_from_suffix(suffix, total_size).map_err(|e| e.to_string())? {
                SuffixOutcome::Ready(footer) => Ok(KeyIdxFooterOpen::Ready(KeyIdxFooter {
                    tenant_hash: footer.tenant_hash,
                    block_count: footer.block_count,
                    section: footer
                        .section(kind::KEY_IDX)
                        .map(|d| (d.offset, d.len, d.crc32c, d.prefix_len)),
                })),
                SuffixOutcome::NeedRange { offset, len } => {
                    Ok(KeyIdxFooterOpen::NeedRange { offset, len })
                }
            }
        }
        other => Err(format!("{other:?} objects carry no KEY_IDX section")),
    }
}

/// Reads the KEY_IDX section of the data object at `key` (`object_size`
/// bytes) by range: a suffix GET for the footer (a second footer GET only
/// when the footer is longer than [`KEY_INDEX_FOOTER_SUFFIX_LEN`]), then one
/// GET of the section. Never reads rows. Every failure leaves the entry
/// uncovered rather than failing the fold. A metrics object has no KEY_IDX
/// section: its LABEL_DICT section is read the same way instead and yields
/// the one [`METRICS_LABEL_VALUE_FIELD`] field.
pub(crate) async fn read_entry_key_index(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    key: &str,
    object_size: u64,
) -> EntryKeyIndexRead {
    let mut gets = 0u64;
    let mut bytes = 0u64;
    let read = read_entry_key_index_counted(
        store,
        tenant,
        signal,
        key,
        object_size,
        &mut gets,
        &mut bytes,
    )
    .await;
    let (fields, get_failed) = match read {
        Ok(fields) => (Ok(fields), false),
        Err(KeyIdxReadError::Get(reason)) => (Err(reason), true),
        Err(KeyIdxReadError::Invalid(reason)) => (Err(reason), false),
    };
    EntryKeyIndexRead {
        fields,
        get_failed,
        gets,
        bytes,
    }
}

async fn read_entry_key_index_counted(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    key: &str,
    object_size: u64,
    gets: &mut u64,
    bytes: &mut u64,
) -> Result<Vec<KeyIdxFieldEntries>, KeyIdxReadError> {
    if signal == Signal::Metrics {
        return read_metrics_label_value_counted(store, tenant, key, object_size, gets, bytes)
            .await;
    }
    if object_size == 0 {
        return Err("entry names a zero-byte object".to_string().into());
    }
    let suffix_len = object_size.min(KEY_INDEX_FOOTER_SUFFIX_LEN);
    *gets += 1;
    let tail = store
        .get(key, GetRange::Suffix(suffix_len))
        .await
        .map_err(|e| KeyIdxReadError::Get(format!("footer suffix GET: {e}")))?;
    *bytes += tail.data.len() as u64;
    let total_size = tail.total_size;
    let footer = match open_key_idx_footer(signal, &tail.data, total_size)? {
        KeyIdxFooterOpen::Ready(footer) => footer,
        KeyIdxFooterOpen::NeedRange { offset, len } => {
            let end = offset
                .checked_add(len)
                .ok_or_else(|| "footer range overflows".to_string())?;
            *gets += 1;
            let got = store
                .get(key, GetRange::Range(offset, end))
                .await
                .map_err(|e| KeyIdxReadError::Get(format!("footer range GET: {e}")))?;
            *bytes += got.data.len() as u64;
            match open_key_idx_footer(signal, &got.data, total_size)? {
                KeyIdxFooterOpen::Ready(footer) => footer,
                KeyIdxFooterOpen::NeedRange { .. } => {
                    return Err("footer range GET did not cover the footer"
                        .to_string()
                        .into());
                }
            }
        }
    };
    if footer.tenant_hash != tenant.0 {
        return Err("footer names another tenant".to_string().into());
    }
    let Some((offset, len, crc, prefix_len)) = footer.section else {
        return Err("object has no KEY_IDX section".to_string().into());
    };
    let end = offset
        .checked_add(len)
        .ok_or_else(|| "KEY_IDX range overflows".to_string())?;
    *gets += 1;
    let section = store
        .get(key, GetRange::Range(offset, end))
        .await
        .map_err(|e| KeyIdxReadError::Get(format!("KEY_IDX section GET: {e}")))?;
    *bytes += section.data.len() as u64;
    if section.data.len() as u64 != len || crc32c::crc32c(&section.data) != crc {
        return Err("KEY_IDX section crc mismatch".to_string().into());
    }
    let fields = decode_key_idx(&section.data, prefix_len).map_err(|e| e.to_string())?;
    // A leaf answers a logs probe, so it may only carry keys the reader's
    // resolution rule produced; a version 1 RLOG section's entry is uncovered.
    if signal == Signal::Logs {
        let version = section.data.first().copied().unwrap_or_default();
        if !ravel_logseg::key_index::section_resolves_like_the_reader(version) {
            return Err(format!(
                "RLOG KEY_IDX section at version {version} does not resolve like the reader"
            )
            .into());
        }
    }
    for field in &fields {
        if let Some((_, block)) = field
            .entries
            .iter()
            .find(|(_, block)| u64::from(*block) >= footer.block_count)
        {
            return Err(format!(
                "KEY_IDX field {:?} names block {block}, object has {} blocks",
                field.name, footer.block_count
            )
            .into());
        }
    }
    if signal == Signal::Spans
        && !matches!(
            fields.as_slice(),
            [field] if field.name == ravel_rspan::TRACE_ID_FIELD && field.key_type == KeyType::Id16
        )
    {
        return Err("RSPAN KEY_IDX must name exactly trace_id of key type Id16"
            .to_string()
            .into());
    }
    Ok(fields)
}

/// One part's leaves, every `leaf_ref.key` still empty, and its uncovered
/// (entry, field) count.
pub(crate) struct PartKeyIndexLeaves {
    pub(crate) leaves: Vec<EncodedKeyIndexLeaf>,
    pub(crate) uncovered_entries: u64,
}

/// Builds one part's leaves from its entries' KEY_IDX reads (`reads[i]` is
/// part entry `i`): one leaf per (field, key slice) for every field in
/// `declared` and, when `section_named_fields`, every field a section names,
/// the declared key type winning, else the key type of the first entry naming
/// the field. An entry is
/// uncovered for a field when its read failed or its section does not name
/// the field under that key type. Every entry carrying a `key8` whose own
/// entries exceed the body ceiling is uncovered for the field too, since no
/// slice can hold that `key8`. A field with no covered entry writes no leaf.
///
/// `max_pairs` is the pair budget [`build_part_key_index`] builds under: each
/// field's pairs are cut into the passes [`key_index_pass_ends`] gives under
/// its share [`key_index_field_share`], and each pass's pairs into slices by
/// [`key_index_pass_slices`], so for the same reads this returns the leaves
/// the incremental build writes, byte for byte. `u64::MAX` is one pass per
/// field.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_part_key_index_leaves(
    tenant: &TenantHash,
    signal: Signal,
    part_blake3: [u8; 32],
    declared: &[(String, KeyType)],
    section_named_fields: bool,
    reads: &[&EntryKeyIndex],
    limits: &KeyIndexLeafLimits,
    max_pairs: u64,
) -> PartKeyIndexLeaves {
    let part_entry_count = reads.len() as u64;
    let mut field_types: BTreeMap<&str, KeyType> = BTreeMap::new();
    for (name, key_type) in declared {
        field_types.entry(name.as_str()).or_insert(*key_type);
    }
    if section_named_fields {
        for fields in reads.iter().filter_map(|read| (*read).as_ref().ok()) {
            for field in fields {
                field_types
                    .entry(field.name.as_str())
                    .or_insert(field.key_type);
            }
        }
    }
    let share = key_index_field_share(max_pairs, field_types.len());
    let mut built = PartKeyIndexLeaves {
        leaves: Vec::new(),
        uncovered_entries: 0,
    };
    for (field, key_type) in field_types {
        let mut uncovered: Vec<u64> = Vec::new();
        let mut entries: Vec<KeyIndexLeafEntry> = Vec::new();
        for (ordinal, read) in reads.iter().enumerate() {
            let ordinal = ordinal as u64;
            let named = read
                .as_ref()
                .ok()
                .and_then(|fields| fields.iter().find(|f| f.name == field))
                .filter(|f| f.key_type == key_type);
            let Some(named) = named else {
                uncovered.push(ordinal);
                continue;
            };
            let mut pairs = named.entries.clone();
            pairs.sort_unstable();
            pairs.dedup();
            for run in pairs.chunk_by(|a, b| a.0 == b.0) {
                entries.push(KeyIndexLeafEntry {
                    key8: run[0].0,
                    entry_ordinal: ordinal,
                    blocks: run.iter().map(|&(_, block)| block).collect(),
                });
            }
        }
        entries.sort_by_key(|e| (e.key8, e.entry_ordinal));
        // The passes count every pair the incremental build holds, those of
        // entries the ceiling then uncovers included.
        let pass_ends = key_index_pass_ends(&entries, share);
        let mut oversized: BTreeSet<u64> = BTreeSet::new();
        for run in entries.chunk_by(|a, b| a.key8 == b.key8) {
            let body = run
                .iter()
                .map(encoded_entry_len)
                .fold(0u64, u64::saturating_add);
            if body > limits.max_body_bytes {
                oversized.extend(run.iter().map(|e| e.entry_ordinal));
            }
        }
        if !oversized.is_empty() {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                field,
                entries = oversized.len(),
                ceiling = limits.max_body_bytes,
                "a key8's own index entries exceed the key-index leaf body ceiling; the entries carrying it are uncovered for the field"
            );
            entries.retain(|e| !oversized.contains(&e.entry_ordinal));
            uncovered.extend(oversized);
            uncovered.sort_unstable();
        }
        if uncovered.len() as u64 == part_entry_count {
            built.uncovered_entries += part_entry_count;
            continue;
        }
        let mut leaves: Vec<EncodedKeyIndexLeaf> = Vec::new();
        let mut refused = None;
        let mut slice_lo = 0u64;
        let mut start = 0usize;
        for end in pass_ends.iter().copied().map(Some).chain([None]) {
            let stop = end.map_or(entries.len(), |end| {
                start + entries[start..].partition_point(|e| u64::from_be_bytes(e.key8) < end)
            });
            let pass = &entries[start..stop];
            start = stop;
            let slices = match key_index_pass_slices(pass, slice_lo, end, limits.max_body_bytes) {
                Ok(slices) => slices,
                Err(err) => {
                    refused = Some(err);
                    break;
                }
            };
            if let Some(end) = end
                && !slices.is_empty()
            {
                slice_lo = end;
            }
            for slice in slices {
                match encode_part_key_index_leaf(
                    &KeyIndexLeafInput {
                        tenant_hash: *tenant,
                        signal,
                        part_blake3,
                        part_entry_count,
                        field,
                        key_type,
                        key8_lo: slice.key8_lo,
                        key8_hi: slice.key8_hi,
                        uncovered_entry_ordinals: &uncovered,
                        entries: &pass[slice.entries],
                    },
                    limits,
                ) {
                    Ok(leaf) => leaves.push(leaf),
                    Err(err) => {
                        refused = Some(err);
                        break;
                    }
                }
            }
            if refused.is_some() {
                break;
            }
        }
        match refused {
            None => {
                built.uncovered_entries += uncovered.len() as u64;
                built.leaves.extend(leaves);
            }
            Some(err) => {
                tracing::error!(
                    tenant = %tenant.to_hex(),
                    field,
                    error = %err,
                    "key-index leaf encode refused; the part is uncovered for the field"
                );
                built.uncovered_entries += part_entry_count;
            }
        }
    }
    built
}

/// The exclusive key8 ends of a field's passes over `entries`, sorted by
/// `(key8, entry_ordinal)`, under `share`: greedy over the key8 runs in
/// ascending order, a pass taking whole runs while their pairs fit `share`,
/// and a run over `share` a pass of its own. The budget cut of
/// [`FieldBuild::cut`] converges to these ends whatever order the pairs
/// arrive in.
fn key_index_pass_ends(entries: &[KeyIndexLeafEntry], share: u64) -> Vec<u64> {
    let mut ends = Vec::new();
    let mut held = 0u64;
    for run in entries.chunk_by(|a, b| a.key8 == b.key8) {
        let pairs = run.len() as u64;
        if held > 0 && held.saturating_add(pairs) > share {
            ends.push(u64::from_be_bytes(run[0].key8));
            held = 0;
        }
        held = held.saturating_add(pairs);
    }
    ends
}

/// The slices of one pass: `entries`, the pass's covered pairs sorted by
/// `(key8, entry_ordinal)`, split by [`split_key_index_entries`], the first
/// slice starting at `slice_lo` (where the field's last slice ended) and the
/// last ending at `end - 1`, or `u64::MAX` for the field's last pass. A pass
/// that ends before `u64::MAX` with no covered pair has no slice, and the
/// next pass's first slice starts at `slice_lo`. Every pass closes its
/// slices, so no pair outlives its pass.
fn key_index_pass_slices(
    entries: &[KeyIndexLeafEntry],
    slice_lo: u64,
    end: Option<u64>,
    max_body_bytes: u64,
) -> Result<Vec<KeyIndexSlice>, KeyIndexLeafEncodeError> {
    if entries.is_empty() && end.is_some() {
        return Ok(Vec::new());
    }
    let mut slices = split_key_index_entries(entries, max_body_bytes)?;
    if let Some(first) = slices.first_mut() {
        first.key8_lo = slice_lo;
    }
    if let (Some(last), Some(end)) = (slices.last_mut(), end) {
        last.key8_hi = end - 1;
    }
    Ok(slices)
}

static FOLD_KEY_INDEX_SECTIONS_READ: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static FOLD_KEY_INDEX_ENTRIES_CARRIED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static FOLD_KEY_INDEX_BUILD_PASSES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static FOLD_KEY_INDEX_LEAF_GETS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// The leaf build's [`FoldReport`] figures `key_index_sections_read`,
/// `key_index_entries_carried`, `key_index_build_passes` and
/// `key_index_leaf_gets`, summed over every fold that returned a report in
/// this process since it started, a no-op included (an attempt lost to a
/// HEAD CAS can build leaves before the next attempt finds nothing to do).
/// A fold that returns an error adds nothing. Monotonic and process-local: a
/// scrape takes the delta between samples.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FoldKeyIndexTotals {
    pub sections_read: u64,
    pub entries_carried: u64,
    pub build_passes: u64,
    pub leaf_gets: u64,
}

/// The process-global [`FoldKeyIndexTotals`].
pub fn fold_key_index_totals() -> FoldKeyIndexTotals {
    use std::sync::atomic::Ordering::Relaxed;
    FoldKeyIndexTotals {
        sections_read: FOLD_KEY_INDEX_SECTIONS_READ.load(Relaxed),
        entries_carried: FOLD_KEY_INDEX_ENTRIES_CARRIED.load(Relaxed),
        build_passes: FOLD_KEY_INDEX_BUILD_PASSES.load(Relaxed),
        leaf_gets: FOLD_KEY_INDEX_LEAF_GETS.load(Relaxed),
    }
}

/// Adds one fold's leaf-build figures to the process-global totals, from
/// the same counters its [`FoldReport`] reads.
fn observe_fold_key_index(counters: &RequestCounters) {
    use std::sync::atomic::Ordering::Relaxed;
    FOLD_KEY_INDEX_SECTIONS_READ.fetch_add(counters.key_index_sections_read, Relaxed);
    FOLD_KEY_INDEX_ENTRIES_CARRIED.fetch_add(counters.key_index_entries_carried, Relaxed);
    FOLD_KEY_INDEX_BUILD_PASSES.fetch_add(counters.key_index_build_passes, Relaxed);
    FOLD_KEY_INDEX_LEAF_GETS.fetch_add(counters.key_index_leaf_gets, Relaxed);
}

/// A part of the previous HEAD with key-index leaves, a carry source of the
/// incremental leaf build: its ref, a digest of its entries, and each of its
/// entries' data object key by ordinal once derived.
///
/// The keys are derived at most once per fold attempt, for a source a
/// re-encoded part's build carries from, or for one whose hours a reconcile
/// is about to change, whichever comes first. By then the attempt's entry
/// set has gained, lost and re-sorted entries, so the source's entries are
/// found again as those in its hour range, and used only when they hash to
/// the digest taken before the first change.
pub(crate) struct CarrySource {
    part: SnapshotPartRef,
    entries_digest: [u8; 32],
    /// `Some(None)` once the entries in its hours no longer match the digest.
    object_keys: std::sync::OnceLock<Option<Vec<Option<String>>>>,
}

impl CarrySource {
    /// Every part of `head` that carries a leaf ref, in HEAD order, each
    /// digesting its entries sliced from `previous_entries` (every part's
    /// entries concatenated in HEAD order) by cumulative `entry_count`. Empty
    /// when the parts' entry counts do not sum to `previous_entries.len()`.
    fn all_of_head(head: &SnapshotHead, previous_entries: &[SnapshotEntry]) -> Vec<Self> {
        let mut sources = Vec::new();
        let mut start = 0usize;
        for part in &head.parts {
            let Some(end) = usize::try_from(part.entry_count)
                .ok()
                .and_then(|count| start.checked_add(count))
                .filter(|&end| end <= previous_entries.len())
            else {
                return Vec::new();
            };
            if !part.key_index.is_empty() {
                sources.push(Self {
                    part: part.clone(),
                    entries_digest: carry_entries_digest(previous_entries[start..end].iter()),
                    object_keys: std::sync::OnceLock::new(),
                });
            }
            start = end;
        }
        if start != previous_entries.len() {
            return Vec::new();
        }
        sources
    }

    /// Whether this part's `[min_hour, watermark_hour]` overlaps the range
    /// `[min_hour, watermark_hour]` of the part being encoded.
    fn overlaps_hours(&self, min_hour: u32, watermark_hour: u32) -> bool {
        self.part.min_hour <= watermark_hour && min_hour <= self.part.watermark_hour
    }

    fn holds_hour(&self, hour: u32) -> bool {
        self.overlaps_hours(hour, hour)
    }

    /// The entries of `entries` in this part's hour range, in order.
    fn entries_in<'a>(
        &self,
        entries: &'a [SnapshotEntry],
    ) -> impl Iterator<Item = &'a SnapshotEntry> + use<'a> {
        let hours = self.part.min_hour..=self.part.watermark_hour;
        entries
            .iter()
            .filter(move |entry| hours.contains(&entry.ingest_hour_bucket))
    }

    /// This part's entries' data object keys by ordinal, derived from
    /// `entries` on the first call. `None` when the entries in its hour range
    /// are no longer the part's own, which leaves the part uncarried.
    fn derive_object_keys(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        entries: &[SnapshotEntry],
    ) -> Option<&[Option<String>]> {
        self.object_keys
            .get_or_init(|| {
                if carry_entries_digest(self.entries_in(entries)) != self.entries_digest {
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        part = %self.part.key,
                        "previous part's entries changed before its keys were derived; its leaves are not carried"
                    );
                    return None;
                }
                #[cfg(test)]
                KEY_INDEX_CARRY_KEYS_DERIVED
                    .with(|derived| derived.set(derived.get() + self.part.entry_count));
                Some(
                    self.entries_in(entries)
                        .map(|entry| {
                            column_stats_build::segment_data_key(tenant, signal, entry).ok()
                        })
                        .collect(),
                )
            })
            .as_deref()
    }

    /// The keys [`Self::derive_object_keys`] derived; empty before it ran or
    /// when it found the entries changed.
    fn object_keys(&self) -> &[Option<String>] {
        self.object_keys
            .get()
            .and_then(Option::as_deref)
            .unwrap_or(&[])
    }
}

/// Derives the keys of every source holding one of `hours`, before the
/// caller changes the entries in those hours.
fn derive_carry_keys_before_change(
    sources: &[CarrySource],
    tenant: &TenantHash,
    signal: Signal,
    entries: &[SnapshotEntry],
    hours: &[u32],
) {
    for source in sources {
        if hours.iter().any(|&hour| source.holds_hour(hour)) {
            source.derive_object_keys(tenant, signal, entries);
        }
    }
}

/// blake3 over every field [`column_stats_build::segment_data_key`] reads,
/// entry by entry, and the entry count.
fn carry_entries_digest<'a>(entries: impl Iterator<Item = &'a SnapshotEntry>) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    let mut count = 0u64;
    for entry in entries {
        count += 1;
        hasher.update(&entry.level.to_le_bytes());
        hasher.update(&entry.shard.to_le_bytes());
        hasher.update(&entry.ingest_hour_bucket.to_le_bytes());
        hasher.update(&entry.writer_epoch.to_le_bytes());
        hasher.update(&entry.writer_seq.to_le_bytes());
        hasher.update(&(entry.writer_id.len() as u64).to_le_bytes());
        hasher.update(&entry.writer_id);
        hasher.update(&(entry.content_hash.len() as u64).to_le_bytes());
        hasher.update(&entry.content_hash);
    }
    hasher.update(&count.to_le_bytes());
    *hasher.finalize().as_bytes()
}

// Test-only: entries whose data object keys a carry source derived.
#[cfg(test)]
thread_local! {
    static KEY_INDEX_CARRY_KEYS_DERIVED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(crate) fn take_key_index_carry_keys_derived_for_test() -> u64 {
    KEY_INDEX_CARRY_KEYS_DERIVED.with(|derived| derived.replace(0))
}

/// Previous-leaf bytes the carry holds now, and the most it held at once.
#[derive(Default)]
struct CarryHold {
    held: u64,
    peak: u64,
}

impl CarryHold {
    fn take(&mut self, bytes: u64) {
        self.held = self.held.saturating_add(bytes);
        self.peak = self.peak.max(self.held);
    }

    fn release(&mut self, bytes: u64) {
        self.held = self.held.saturating_sub(bytes);
    }
}

/// One slice of a source field's leaf set, planned from its ref. The lowest
/// slice's bytes are fetched when the source is loaded and a later slice's
/// when a pass's feed walks to it; either is dropped once a pass starts past
/// its `key8_hi`, and each pass that overlaps it decodes the bytes again, one
/// frame at a time.
struct CarriedSlice {
    leaf: SnapshotKeyIndexLeafRef,
    bytes: Option<Bytes>,
    /// The field build attempt that fetched it: within one attempt a
    /// field's passes ascend in key8, so a dropped slice is never needed
    /// again until a rebuild starts the field over from key8 0.
    fetched_in: Option<u32>,
}

/// One field's previous slices of one source, in key8 order.
struct CarriedField {
    slices: Vec<CarriedSlice>,
    /// The lowest slice's `uncovered_entry_ordinals`, which every slice of
    /// the set must carry.
    uncovered: Vec<u64>,
    /// Per new ordinal: whether this source's leaves covered its object.
    covered: Vec<bool>,
}

/// One previous part's leaves, mapped onto a new part by data object key.
struct CarriedSource {
    part_blake3: Vec<u8>,
    part_entry_count: u64,
    fields: BTreeMap<String, CarriedField>,
    /// Per previous ordinal: the new ordinal of the same data object.
    onto: Vec<Option<u64>>,
}

/// Every carry source a part's build loaded, and the bytes they hold.
struct CarriedKeyIndex {
    sources: Vec<CarriedSource>,
    hold: CarryHold,
}

/// GETs one previous leaf whole and verifies it: the leaf's own checks, its
/// ref, its tenant, signal, part hash and part entry count, and a
/// `format_version` the signal accepts (not a logs version-1 leaf).
#[allow(clippy::too_many_arguments)]
async fn fetch_carried_leaf(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    part_blake3: &[u8],
    part_entry_count: u64,
    leaf: &SnapshotKeyIndexLeafRef,
    limits: &KeyIndexLeafLimits,
    counters: &mut RequestCounters,
) -> Result<(KeyIndexLeafSummary, Bytes), String> {
    counters.get_requests += 1;
    counters.key_index_leaf_gets += 1;
    let got = store
        .get(&leaf.key, GetRange::Full)
        .await
        .map_err(|e| format!("leaf {} GET: {e}", leaf.key))?;
    let summary = verify_key_index_leaf_frames(&got.data, limits, drop)
        .map_err(|e| format!("leaf {}: {e}", leaf.key))?;
    let expected = SnapshotKeyIndexLeafRef {
        key: String::new(),
        ..leaf.clone()
    };
    if summary.leaf_ref() != expected {
        return Err(format!("leaf {} does not match its ref", leaf.key));
    }
    let header = &summary.header;
    if header.tenant_hash.as_slice() != tenant.0.as_slice()
        || header.signal != signal::to_proto(signal) as u32
        || header.part_blake3 != part_blake3
        || header.part_entry_count != part_entry_count
    {
        return Err(format!(
            "leaf {} binds another tenant, signal or part",
            leaf.key
        ));
    }
    if !key_index_leaf_version_accepted(signal, header.format_version) {
        return Err(format!(
            "leaf {} is format_version {}, which a {signal:?} carry does not take",
            leaf.key, header.format_version
        ));
    }
    Ok((summary, got.data))
}

/// Plans one source's leaf sets from its refs and maps its covered entries
/// onto the new part by data object key. Per field it GETs and verifies only
/// the lowest slice, whose header gives the uncovered entries of the whole
/// set; the later slices are fetched by the passes that reach them. `Err`
/// (the reason) when the leaves cannot stand in for the sections: a leaf
/// field not in `fields` under its key type, a field of `fields` with no
/// leaf, refs that do not tile the key8 space or disagree on key type,
/// `uncovered_count` or part hash, or name another part, or a lowest slice
/// whose GET fails or that does not verify ([`fetch_carried_leaf`]).
#[allow(clippy::too_many_arguments)]
async fn load_carry_source(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    source: &CarrySource,
    fields: &BTreeMap<&str, KeyType>,
    ordinal_by_key: &HashMap<&str, u64>,
    part_entry_count: usize,
    limits: &KeyIndexLeafLimits,
    hold: &mut CarryHold,
    counters: &mut RequestCounters,
) -> Result<CarriedSource, String> {
    let refs = &source.part.key_index;
    if let Some(leaf) = refs.iter().find(|leaf| {
        fields
            .get(leaf.field.as_str())
            .is_none_or(|key_type| u32::from(key_type.as_u8()) != leaf.key_type)
    }) {
        return Err(format!(
            "previous leaf field {:?} is not declared under key type {}",
            leaf.field, leaf.key_type
        ));
    }
    if let Some(name) = fields
        .keys()
        .find(|name| !refs.iter().any(|leaf| leaf.field == **name))
    {
        return Err(format!("declared field {name:?} has no previous leaf"));
    }
    if source.object_keys().len() as u64 != source.part.entry_count {
        return Err("previous part entry list does not match its ref".to_string());
    }
    for name in fields.keys() {
        select_key_index_slice(refs, name, &[0u8; 8])
            .map_err(|e| format!("field {name:?}: {e}"))?;
        if refs
            .iter()
            .any(|leaf| leaf.field == *name && leaf.part_blake3 != source.part.blake3)
        {
            return Err(format!("field {name:?}: a leaf ref names another part"));
        }
    }
    let onto: Vec<Option<u64>> = source
        .object_keys()
        .iter()
        .map(|key| {
            key.as_deref()
                .and_then(|key| ordinal_by_key.get(key).copied())
        })
        .collect();
    let mut carried = CarriedSource {
        part_blake3: source.part.blake3.clone(),
        part_entry_count: source.part.entry_count,
        fields: BTreeMap::new(),
        onto,
    };
    let mut fetched = 0u64;
    let result = async {
        for name in fields.keys() {
            let mut slices: Vec<CarriedSlice> = refs
                .iter()
                .filter(|leaf| leaf.field == *name)
                .map(|leaf| CarriedSlice {
                    leaf: leaf.clone(),
                    bytes: None,
                    fetched_in: None,
                })
                .collect();
            slices.sort_by_key(|slice| slice.leaf.key8_lo);
            let Some(lowest) = slices.first_mut() else {
                return Err(format!("declared field {name:?} has no previous leaf"));
            };
            let (summary, bytes) = fetch_carried_leaf(
                store,
                tenant,
                signal,
                &carried.part_blake3,
                carried.part_entry_count,
                &lowest.leaf,
                limits,
                counters,
            )
            .await?;
            fetched += bytes.len() as u64;
            hold.take(bytes.len() as u64);
            lowest.bytes = Some(bytes);
            lowest.fetched_in = Some(0);
            let uncovered = summary.header.uncovered_entry_ordinals;
            let uncovered_set: HashSet<u64> = uncovered.iter().copied().collect();
            let mut covered = vec![false; part_entry_count];
            for (previous, new) in carried.onto.iter().enumerate() {
                if let Some(new) = new
                    && !uncovered_set.contains(&(previous as u64))
                    && let Some(slot) = covered.get_mut(*new as usize)
                {
                    *slot = true;
                }
            }
            carried.fields.insert(
                name.to_string(),
                CarriedField {
                    slices,
                    uncovered,
                    covered,
                },
            );
        }
        Ok(())
    }
    .await;
    match result {
        Ok(()) => Ok(carried),
        Err(reason) => {
            hold.release(fetched);
            Err(reason)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FieldBuildState {
    Building,
    Done,
    /// No leaf: every entry is uncovered, the encoder refused a slice, or a
    /// rebuild found new uncovered entries again.
    Uncovered,
}

/// A held (entry, key8) pair, ordered by `(key8, entry_ordinal)` so the top
/// of a field's heap is its highest key8.
struct HeldPair(KeyIndexLeafEntry);

impl HeldPair {
    fn order_key(&self) -> ([u8; 8], u64) {
        (self.0.key8, self.0.entry_ordinal)
    }
}

impl PartialEq for HeldPair {
    fn eq(&self, other: &Self) -> bool {
        self.order_key() == other.order_key()
    }
}

impl Eq for HeldPair {}

impl PartialOrd for HeldPair {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeldPair {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.order_key().cmp(&other.order_key())
    }
}

/// One field of one part's leaf build.
struct FieldBuild {
    key_type: KeyType,
    uncovered: BTreeSet<u64>,
    /// Pairs held this pass, read and carried, inside `[lo, end)`.
    held: BinaryHeap<HeldPair>,
    /// The lowest key8 in `held`: when the top equals it, `held` is one key8.
    held_min: Option<[u8; 8]>,
    /// The most pairs `held` had at once this pass, a placed pair before its
    /// cut included.
    held_peak: usize,
    lo: u64,
    /// Exclusive end of this pass's key8 range, once the budget cut it.
    end: Option<u64>,
    /// Where the field's next slice starts: the end of the last pass that
    /// wrote one.
    slice_lo: u64,
    /// Whether a pass this attempt ended holding a pair: a new uncovered
    /// entry after that makes the attempt stale, since that pass's leaves
    /// and pass range were made without it.
    fed: bool,
    stale: bool,
    attempt: u32,
    state: FieldBuildState,
    refs: Vec<SnapshotKeyIndexLeafRef>,
    /// Pairs held by this attempt's passes, summed: each pass holds the
    /// pairs of its own key8 range, and the ranges tile the key space.
    pairs: u64,
}

impl FieldBuild {
    fn new(key_type: KeyType, uncovered: BTreeSet<u64>) -> Self {
        Self {
            key_type,
            uncovered,
            held: BinaryHeap::new(),
            held_min: None,
            held_peak: 0,
            lo: 0,
            end: None,
            slice_lo: 0,
            fed: false,
            stale: false,
            attempt: 0,
            state: FieldBuildState::Building,
            refs: Vec::new(),
            pairs: 0,
        }
    }

    fn in_range(&self, key8: [u8; 8]) -> bool {
        let key8 = u64::from_be_bytes(key8);
        key8 >= self.lo && self.end.is_none_or(|end| key8 < end)
    }

    /// Holds one pair if it falls in this pass's range, then cuts to `share`.
    fn hold(&mut self, pair: KeyIndexLeafEntry, share: u64) {
        if !self.in_range(pair.key8) {
            return;
        }
        self.held_min = Some(self.held_min.map_or(pair.key8, |min| min.min(pair.key8)));
        self.held.push(HeldPair(pair));
        self.held_peak = self.held_peak.max(self.held.len());
        self.cut(share);
    }

    /// While the field holds more than `share` pairs over more than one key8,
    /// gives up its highest key8 run, which becomes the exclusive end of the
    /// pass's range. A single key8 is never split, so one whose own pairs
    /// exceed `share` is held whole.
    fn cut(&mut self, share: u64) {
        while self.held.len() as u64 > share {
            let Some(top) = self.held.peek().map(|pair| pair.0.key8) else {
                break;
            };
            if self.held_min == Some(top) {
                break;
            }
            while self.held.peek().is_some_and(|pair| pair.0.key8 == top) {
                self.held.pop();
            }
            self.end = Some(u64::from_be_bytes(top));
        }
    }

    fn note_uncovered(&mut self, ordinal: u64) {
        if self.uncovered.insert(ordinal) && self.fed {
            self.stale = true;
        }
    }

    fn give_up(&mut self) {
        self.state = FieldBuildState::Uncovered;
        self.refs.clear();
        self.held.clear();
        self.held_min = None;
    }
}

/// Each field's share of the pair budget: `max_pairs` split evenly over the
/// `fields` the build indexes, at least 1. Fixed for the build, a finished
/// or uncovered field included, so a field's passes depend on its own pairs
/// and the field count alone, not on when other fields finish.
fn key_index_field_share(max_pairs: u64, fields: usize) -> u64 {
    (max_pairs / fields.max(1) as u64).max(1)
}

/// Drops the bytes of every slice of `carried` that the field's next pass,
/// starting at `lo`, no longer reaches: all of them when the field has
/// stopped building.
fn release_carried_slices(
    carried: &mut CarriedField,
    lo: u64,
    building: bool,
    hold: &mut CarryHold,
) {
    for slice in &mut carried.slices {
        if (!building || slice.leaf.key8_hi < lo)
            && let Some(bytes) = slice.bytes.take()
        {
            hold.release(bytes.len() as u64);
        }
    }
}

/// Feeds a field's carried pairs in its pass range into `held`, decoding
/// one source's slices that overlap it in key8 order, one frame at a time,
/// and stopping at the first slice past the range the budget has cut so
/// far. A slice the field's build attempt has not fetched yet is fetched
/// and verified here ([`fetch_carried_leaf`], and its uncovered list against
/// the lowest slice's), then kept until a pass starts past it. A rebuilt
/// field starts again from key8 0, so a slice dropped in its earlier attempt
/// is fetched again, while one still kept from that attempt stays held until
/// a pass starts past it. `Err` when a slice's GET fails, it does not
/// verify, or a kept slice no longer decodes.
#[allow(clippy::too_many_arguments)]
async fn feed_carried_key_index(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    field: &mut FieldBuild,
    source: &mut CarriedSource,
    name: &str,
    carried_all: &[bool],
    limits: &KeyIndexLeafLimits,
    share: u64,
    hold: &mut CarryHold,
    counters: &mut RequestCounters,
) -> Result<(), String> {
    let CarriedSource {
        part_blake3,
        part_entry_count,
        fields,
        onto,
    } = source;
    let Some(carried) = fields.get_mut(name) else {
        return Ok(());
    };
    for slice in &mut carried.slices {
        if slice.leaf.key8_hi < field.lo {
            continue;
        }
        if field.end.is_some_and(|end| slice.leaf.key8_lo >= end) {
            break;
        }
        let bytes = match &slice.bytes {
            Some(bytes) => bytes.clone(),
            None => {
                if slice.fetched_in == Some(field.attempt) {
                    return Err(format!(
                        "leaf {} was dropped and is needed again",
                        slice.leaf.key
                    ));
                }
                let (summary, bytes) = fetch_carried_leaf(
                    store,
                    tenant,
                    signal,
                    part_blake3,
                    *part_entry_count,
                    &slice.leaf,
                    limits,
                    counters,
                )
                .await?;
                if summary.header.uncovered_entry_ordinals != carried.uncovered {
                    return Err(format!(
                        "leaf {} carries other uncovered_entry_ordinals than its set's lowest slice",
                        slice.leaf.key
                    ));
                }
                hold.take(bytes.len() as u64);
                slice.fetched_in = Some(field.attempt);
                slice.bytes = Some(bytes.clone());
                bytes
            }
        };
        verify_key_index_leaf_frames(&bytes, limits, |frame| {
            for entry in frame {
                let Some(new) = usize::try_from(entry.entry_ordinal)
                    .ok()
                    .and_then(|previous| onto.get(previous).copied().flatten())
                else {
                    continue;
                };
                if !carried_all.get(new as usize).copied().unwrap_or(false) {
                    continue;
                }
                field.hold(
                    KeyIndexLeafEntry {
                        entry_ordinal: new,
                        ..entry
                    },
                    share,
                );
            }
        })
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

// Test-only: every pass's range and held pairs, per field, in build order.
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyIndexPassRecord {
    pub(crate) field: String,
    pub(crate) attempt: u32,
    pub(crate) lo: u64,
    pub(crate) end: Option<u64>,
    pub(crate) share: u64,
    pub(crate) held: u64,
    /// The most pairs held at once during the pass.
    pub(crate) held_peak: u64,
    /// Whether every held pair has one key8.
    pub(crate) one_key8: bool,
    /// Capacity of the held heap's buffer at the end of the pass.
    pub(crate) held_capacity: u64,
    /// Whether every held pair's blocks `Vec` has capacity equal to its length.
    pub(crate) blocks_exact: bool,
    /// Pairs the field still held once this pass had ended, measured after
    /// `end_key_index_pass` returned: pairs that would sit in the next pass
    /// beside its own. Every pass closes its slices
    /// ([`key_index_pass_slices`]) and drops its heap, so this is 0.
    pub(crate) open_slice: u64,
}

#[cfg(test)]
thread_local! {
    static KEY_INDEX_PASS_LOG: std::cell::RefCell<Vec<KeyIndexPassRecord>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(crate) fn take_key_index_pass_log_for_test() -> Vec<KeyIndexPassRecord> {
    KEY_INDEX_PASS_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()))
}

#[cfg(test)]
fn record_key_index_pass(name: &str, field: &FieldBuild, share: u64) {
    let one_key8 = field
        .held_min
        .is_some_and(|min| field.held.peek().is_some_and(|top| top.0.key8 == min));
    let record = KeyIndexPassRecord {
        field: name.to_string(),
        attempt: field.attempt,
        lo: field.lo,
        end: field.end,
        share,
        held: field.held.len() as u64,
        held_peak: field.held_peak as u64,
        one_key8,
        held_capacity: field.held.capacity() as u64,
        blocks_exact: field
            .held
            .iter()
            .all(|pair| pair.0.blocks.capacity() == pair.0.blocks.len()),
        open_slice: 0,
    };
    KEY_INDEX_PASS_LOG.with(|log| log.borrow_mut().push(record));
}

/// Completes the record [`record_key_index_pass`] just wrote for `name`:
/// the pairs the field still holds now that its pass has ended.
#[cfg(test)]
fn record_key_index_pass_end(name: &str, field: &FieldBuild) {
    KEY_INDEX_PASS_LOG.with(|log| {
        if let Some(record) = log.borrow_mut().iter_mut().rev().find(|r| r.field == name) {
            record.open_slice = field.held.len() as u64;
        }
    });
}

// Test-only: what the carry held once each pass had fed its carried pairs,
// the most it holds during that pass (slices are dropped before the feeds).
#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyIndexCarryRecord {
    /// Slices held per (source index in HEAD order of the loaded sources,
    /// field).
    pub(crate) slices_held: BTreeMap<(usize, String), usize>,
    /// Previous-leaf bytes held, every source and field together.
    pub(crate) held_bytes: u64,
}

#[cfg(test)]
thread_local! {
    static KEY_INDEX_CARRY_LOG: std::cell::RefCell<Vec<KeyIndexCarryRecord>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

#[cfg(test)]
pub(crate) fn take_key_index_carry_log_for_test() -> Vec<KeyIndexCarryRecord> {
    KEY_INDEX_CARRY_LOG.with(|log| std::mem::take(&mut *log.borrow_mut()))
}

#[cfg(test)]
fn record_key_index_carry(carried: &CarriedKeyIndex) {
    let mut slices_held = BTreeMap::new();
    for (index, source) in carried.sources.iter().enumerate() {
        for (name, field) in &source.fields {
            slices_held.insert(
                (index, name.clone()),
                field.slices.iter().filter(|s| s.bytes.is_some()).count(),
            );
        }
    }
    let record = KeyIndexCarryRecord {
        slices_held,
        held_bytes: carried.hold.held,
    };
    KEY_INDEX_CARRY_LOG.with(|log| log.borrow_mut().push(record));
}

/// What the leaf build needs beyond the part: the body ceiling, the pair
/// budget, the read concurrency and the byte cap.
#[derive(Debug, Clone, Copy)]
pub(crate) struct KeyIndexBuildLimits {
    pub(crate) leaf: KeyIndexLeafLimits,
    pub(crate) max_pairs: u64,
    pub(crate) concurrency: usize,
    /// Most bytes one field's leaves may total for the part; a field whose
    /// leaves would pass it is left uncovered with no ref. `None` is no cap.
    pub(crate) leaf_bytes_cap: Option<u64>,
}

/// The metrics `label_value` byte cap for a part whose entries' objects
/// total `part_data_bytes`: that times `ratio_ppm` parts per million,
/// rounded down.
pub(crate) fn metrics_label_index_leaf_bytes_cap(part_data_bytes: u64, ratio_ppm: u32) -> u64 {
    let cap = u128::from(part_data_bytes) * u128::from(ratio_ppm) / 1_000_000;
    u64::try_from(cap).unwrap_or(u64::MAX)
}

/// What [`Catalog::metrics_label_index_erasure_gate`] found and the requests
/// its listing issued.
pub(crate) struct MetricsErasureGate {
    pub(crate) gated: bool,
    pub(crate) list_requests: u64,
    pub(crate) get_requests: u64,
}

/// One part's leaf refs, sorted by (field, key8_lo), its uncovered
/// (entry, field) count, and whether a field was left uncovered by the
/// byte cap.
#[derive(Debug, Clone)]
pub(crate) struct BuiltPartKeyIndex {
    pub(crate) refs: Vec<SnapshotKeyIndexLeafRef>,
    pub(crate) uncovered_entries: u64,
    pub(crate) over_cap: bool,
    /// Each field the build indexed, declared or named by a section, and
    /// its pairs ([`FoldReport::key_index_field_pairs`]).
    pub(crate) field_pairs: BTreeMap<String, u64>,
}

/// Builds and PUTs one re-encoded part's leaves (ADR-2707 decision 4, the
/// incremental tail build amendment), the same leaves
/// [`build_part_key_index_leaves`] builds from every entry's section, byte
/// for byte.
///
/// Carry: `sources` are the previous HEAD's parts with leaves whose hour
/// range overlaps this part's. Each one that shares a data object with this
/// part is mapped onto it by data object key, and each entry its leaves
/// cover for every declared field is taken from them without reading its
/// section. Entries they list as uncovered and entries new to the part are
/// read. Per source and field only the lowest slice is fetched up front; a
/// pass fetches each later slice its feed walks to before the end cut by the
/// carried pairs fed so far, and drops it once a pass starts past its
/// `key8_hi`, so every slice is fetched once per field build attempt. A
/// source whose refs or lowest slices do not check out is not carried and
/// its entries are read.
/// A data object mapped from two sources drops the whole carry, and so
/// does a later slice that cannot be read or verified (the build restarts
/// with no carry) or a section naming a field the tenant does not declare,
/// since a carried entry might name it too.
///
/// Passes: every field the build indexes gets a fixed share of
/// `limits.max_pairs`, `max(1, max_pairs / fields)`, for the whole build,
/// and each pass holds, per field, the pairs read and carried in its key8
/// range. When a field's held pairs pass its share, it gives up its highest
/// key8 run, which becomes the exclusive end of the range, so a pass holds
/// the longest run of whole key8s from where the field's last pass ended
/// that fits the share, or that one key8 whole when its own pairs exceed
/// it. A pass encodes and closes every slice its pairs open before the next
/// pass starts, so no pair outlives its pass. Each later pass reads the
/// sections again and decodes the carried leaves its range overlaps. A field
/// whose later pass finds an entry uncovered after an earlier pass ended
/// holding pairs keeps passing to the end of the key space, collecting every
/// uncovered entry, and is then rebuilt once from the first range with the
/// complete list; a further finding in the rebuild leaves it with no leaf.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_part_key_index(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    part_blake3: [u8; 32],
    part_watermark: u32,
    part_entries: &[SnapshotEntry],
    declared: &[(String, KeyType)],
    sources: &[&CarrySource],
    held: HeldPart<'_>,
    limits: &KeyIndexBuildLimits,
    counters: &mut RequestCounters,
) -> Result<BuiltPartKeyIndex, StoreError> {
    let n = part_entries.len();
    let part_entry_count = n as u64;
    let max_pairs = limits.max_pairs.max(1);
    let data_keys: Vec<Result<String, String>> = part_entries
        .iter()
        .map(|entry| {
            column_stats_build::segment_data_key(tenant, signal, entry)
                .map_err(|e| format!("data key: {e}"))
        })
        .collect();
    let key_errors: BTreeSet<u64> = (0..part_entry_count)
        .filter(|&o| data_keys[o as usize].is_err())
        .collect();
    let mut ordinal_by_key: HashMap<&str, u64> = HashMap::new();
    for (ordinal, key) in data_keys.iter().enumerate() {
        if let Ok(key) = key {
            ordinal_by_key.entry(key.as_str()).or_insert(ordinal as u64);
        }
    }
    let mut declared_types: BTreeMap<&str, KeyType> = BTreeMap::new();
    for (name, key_type) in declared {
        declared_types.entry(name.as_str()).or_insert(*key_type);
    }

    // The sources that share a data object with this part. A source sharing
    // none costs no GET.
    let sharing: Vec<&CarrySource> = sources
        .iter()
        .copied()
        .filter(|source| {
            source
                .object_keys()
                .iter()
                .flatten()
                .any(|key| ordinal_by_key.contains_key(key.as_str()))
        })
        .collect();
    let mut mapped: HashSet<&str> = HashSet::new();
    let duplicate = sharing.iter().find_map(|source| {
        source
            .object_keys()
            .iter()
            .flatten()
            .filter(|key| ordinal_by_key.contains_key(key.as_str()))
            .find(|key| !mapped.insert(key.as_str()))
    });
    let mut carry = None;
    let mut peak_held = 0u64;
    if let Some(key) = duplicate {
        tracing::warn!(
            tenant = %tenant.to_hex(),
            object_key = %key,
            "a data object is listed twice in the previous parts; the leaf build carries no previous leaf and reads every entry's section"
        );
    } else if !sharing.is_empty() {
        let mut carried = CarriedKeyIndex {
            sources: Vec::new(),
            hold: CarryHold::default(),
        };
        for source in sharing {
            match load_carry_source(
                store,
                tenant,
                signal,
                source,
                &declared_types,
                &ordinal_by_key,
                n,
                &limits.leaf,
                &mut carried.hold,
                counters,
            )
            .await
            {
                Ok(loaded) => carried.sources.push(loaded),
                Err(reason) => {
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        part = %source.part.key,
                        reason = %reason,
                        "previous part leaves not carried; the leaf build reads that part's entries' sections"
                    );
                }
            }
        }
        peak_held = carried.hold.peak;
        if !carried.sources.is_empty() {
            carry = Some(carried);
        }
    }

    'build: loop {
        let mut over_cap = false;
        let mut carried = carry.take();
        let carry_active = carried.is_some();
        // An entry goes unread only when the source its object came from
        // covered it for every declared field; an entry read for one field is
        // indexed from its section for all of them.
        let mut carried_all = vec![false; n];
        if let Some(carried) = &carried {
            for source in &carried.sources {
                for new in source.onto.iter().flatten() {
                    let new = *new as usize;
                    if new < n
                        && declared_types.keys().all(|name| {
                            source
                                .fields
                                .get(*name)
                                .is_some_and(|field| field.covered[new])
                        })
                    {
                        carried_all[new] = true;
                    }
                }
            }
        }
        let mut fields: BTreeMap<String, FieldBuild> = declared_types
            .iter()
            .map(|(&name, &key_type)| {
                (
                    name.to_string(),
                    FieldBuild::new(key_type, key_errors.clone()),
                )
            })
            .collect();
        let mut to_read: Vec<u64> = (0..part_entry_count)
            .filter(|&o| data_keys[o as usize].is_ok() && !carried_all[o as usize])
            .collect();
        let mut first_pass = true;

        loop {
            counters.key_index_build_passes += 1;
            let mut share = key_index_field_share(max_pairs, fields.len());
            if let Some(carried) = &mut carried {
                // Every drop before any fetch, so the carry holds the most
                // it holds this pass once the feeds below are done.
                for source in &mut carried.sources {
                    for (name, carried_field) in &mut source.fields {
                        let (lo, building) = fields.get(name).map_or((0, false), |field| {
                            (field.lo, field.state == FieldBuildState::Building)
                        });
                        release_carried_slices(carried_field, lo, building, &mut carried.hold);
                    }
                }
                let mut failure = None;
                'feed: for (name, field) in fields.iter_mut() {
                    if field.state != FieldBuildState::Building {
                        continue;
                    }
                    for source in &mut carried.sources {
                        if let Err(reason) = feed_carried_key_index(
                            store,
                            tenant,
                            signal,
                            field,
                            source,
                            name,
                            &carried_all,
                            &limits.leaf,
                            share,
                            &mut carried.hold,
                            counters,
                        )
                        .await
                        {
                            failure = Some(reason);
                            break 'feed;
                        }
                    }
                }
                peak_held = peak_held.max(carried.hold.peak);
                if let Some(reason) = failure {
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        reason = %reason,
                        "a previous part's leaf slice cannot be read, does not verify or no longer decodes; the leaf build drops the carry and reads every entry's section"
                    );
                    continue 'build;
                }
                #[cfg(test)]
                record_key_index_carry(carried);
            }
            let targets: Vec<(u64, String, u64)> = to_read
                .iter()
                .filter_map(|&o| {
                    let key = data_keys[o as usize].as_ref().ok()?;
                    Some((o, key.clone(), part_entries[o as usize].object_size))
                })
                .collect();
            let mut failed: HashSet<u64> = HashSet::new();
            let mut carry_conflict = false;
            {
                let targets_ref = &targets;
                // Indexed rather than `iter()`, so the stream's item is not a
                // borrow: a borrowed item makes the fold future fail the
                // `Send` bound the server's spawned fold loop needs.
                let mut reads = stream::iter(0..targets.len())
                    .map(move |i| {
                        let (ordinal, key, object_size) = &targets_ref[i];
                        let held = held.get(*ordinal).cloned();
                        async move {
                            match held {
                                // Read from object bytes the fold already
                                // holds: no GET.
                                Some(fields) => EntryKeyIndexRead {
                                    fields,
                                    get_failed: false,
                                    gets: 0,
                                    bytes: 0,
                                },
                                None => {
                                    read_entry_key_index(store, tenant, signal, key, *object_size)
                                        .await
                                }
                            }
                        }
                    })
                    .buffered(limits.concurrency.max(1));
                let mut next = 0usize;
                while let Some(read) = reads.next().await {
                    let (ordinal, key, _) = &targets[next];
                    let ordinal = *ordinal;
                    next += 1;
                    counters.get_requests += read.gets;
                    counters.key_index_ranged_gets += read.gets;
                    counters.key_index_bytes_read += read.bytes;
                    counters.key_index_sections_read += 1;
                    let sections = match &read.fields {
                        Ok(sections) => sections,
                        Err(reason) => {
                            if read.get_failed {
                                tracing::warn!(
                                    tenant = %tenant.to_hex(),
                                    object_key = %key,
                                    reason = %reason,
                                    "KEY_IDX read GET failed; the entry is uncovered by this fold's key-index leaves"
                                );
                            }
                            failed.insert(ordinal);
                            for field in fields.values_mut() {
                                if field.state == FieldBuildState::Building {
                                    field.note_uncovered(ordinal);
                                }
                            }
                            continue;
                        }
                    };
                    let mut added = false;
                    for section in sections {
                        if fields.contains_key(&section.name) {
                            continue;
                        }
                        if carry_active {
                            carry_conflict = true;
                        } else if first_pass {
                            // The first entry naming an undeclared field
                            // sets its key type, and every entry before it
                            // names it under none.
                            let mut uncovered = key_errors.clone();
                            uncovered.extend(0..ordinal);
                            fields.insert(
                                section.name.clone(),
                                FieldBuild::new(section.key_type, uncovered),
                            );
                            added = true;
                        }
                    }
                    if carry_conflict {
                        break;
                    }
                    if added {
                        share = key_index_field_share(max_pairs, fields.len());
                        for field in fields.values_mut() {
                            field.cut(share);
                        }
                    }
                    for (name, field) in fields.iter_mut() {
                        if field.state != FieldBuildState::Building {
                            continue;
                        }
                        let Some(named) = sections
                            .iter()
                            .find(|s| s.name == *name)
                            .filter(|s| s.key_type == field.key_type)
                        else {
                            field.note_uncovered(ordinal);
                            continue;
                        };
                        let mut pairs = named.entries.clone();
                        pairs.sort_unstable();
                        pairs.dedup();
                        for run in pairs.chunk_by(|a, b| a.0 == b.0) {
                            field.hold(
                                KeyIndexLeafEntry {
                                    key8: run[0].0,
                                    entry_ordinal: ordinal,
                                    blocks: run.iter().map(|&(_, block)| block).collect(),
                                },
                                share,
                            );
                        }
                    }
                }
            }
            if carry_conflict {
                tracing::warn!(
                    tenant = %tenant.to_hex(),
                    "a section names a field the tenant does not declare; the leaf build drops the carry and reads every entry's section"
                );
                continue 'build;
            }
            first_pass = false;
            to_read.retain(|o| !failed.contains(o));

            let mut pending: Vec<(String, u32, EncodedKeyIndexLeaf)> = Vec::new();
            for (name, field) in fields.iter_mut() {
                if field.state == FieldBuildState::Building {
                    #[cfg(test)]
                    record_key_index_pass(name, field, share);
                    end_key_index_pass(
                        tenant,
                        signal,
                        part_blake3,
                        part_entry_count,
                        name,
                        field,
                        &limits.leaf,
                        &mut pending,
                    );
                    #[cfg(test)]
                    record_key_index_pass_end(name, field);
                }
            }
            if let Some(cap) = limits.leaf_bytes_cap {
                for (name, field) in fields.iter_mut() {
                    if field.state == FieldBuildState::Uncovered {
                        continue;
                    }
                    let put: u64 = field.refs.iter().map(|r| r.size).sum();
                    let queued: u64 = pending
                        .iter()
                        .filter(|(n, attempt, _)| n == name && *attempt == field.attempt)
                        .map(|(_, _, leaf)| leaf.bytes.len() as u64)
                        .sum();
                    if put.saturating_add(queued) > cap {
                        tracing::warn!(
                            tenant = %tenant.to_hex(),
                            field = name.as_str(),
                            leaf_bytes = put.saturating_add(queued),
                            cap,
                            "the part's key-index leaves would exceed the byte cap; the part is uncovered for the field"
                        );
                        field.give_up();
                        over_cap = true;
                    }
                }
                pending.retain(|(name, _, _)| {
                    fields
                        .get(name)
                        .is_some_and(|field| field.state != FieldBuildState::Uncovered)
                });
            }
            for (name, attempt, leaf) in pending {
                let put_result =
                    put_key_index_leaf(store, tenant, signal, part_watermark, leaf).await;
                // Request-issued, like every PUT counter here (#1598).
                counters.put_requests += 1;
                let leaf_ref = put_result?;
                if let Some(field) = fields.get_mut(&name)
                    && field.attempt == attempt
                    && field.state != FieldBuildState::Uncovered
                {
                    field.refs.push(leaf_ref);
                }
            }
            if fields
                .values()
                .all(|field| field.state != FieldBuildState::Building)
            {
                break;
            }
        }

        let carried_entries = carried_all.iter().filter(|&&carried| carried).count() as u64;
        counters.key_index_entries_carried += carried_entries;
        counters.key_index_carry_peak_held_bytes =
            counters.key_index_carry_peak_held_bytes.max(peak_held);
        let mut built = BuiltPartKeyIndex {
            refs: Vec::new(),
            uncovered_entries: 0,
            over_cap,
            field_pairs: BTreeMap::new(),
        };
        for (name, field) in fields {
            built.field_pairs.insert(name, field.pairs);
            match field.state {
                FieldBuildState::Done => {
                    built.uncovered_entries += field.uncovered.len() as u64;
                    built.refs.extend(field.refs);
                }
                FieldBuildState::Building | FieldBuildState::Uncovered => {
                    built.uncovered_entries += part_entry_count;
                }
            }
        }
        return Ok(built);
    }
}

/// Closes one pass of one field: sorts the pass's held pairs, read and
/// carried, by `(key8, entry_ordinal)` in the heap's own buffer, uncovers the
/// entries of every key8 whose own entries exceed the body ceiling, drops
/// the uncovered entries' pairs in place and, unless the attempt is stale,
/// queues the pass's slices ([`key_index_pass_slices`]) on `pending`,
/// encoded from that buffer. The buffer is freed when the pass ends, so the
/// pairs a field holds at any moment are the pairs of one pass.
#[allow(clippy::too_many_arguments)]
fn end_key_index_pass(
    tenant: &TenantHash,
    signal: Signal,
    part_blake3: [u8; 32],
    part_entry_count: u64,
    name: &str,
    field: &mut FieldBuild,
    limits: &KeyIndexLeafLimits,
    pending: &mut Vec<(String, u32, EncodedKeyIndexLeaf)>,
) {
    let mut held = key_index_sorted_pairs(std::mem::take(&mut field.held));
    field.pairs += held.len() as u64;
    field.held_min = None;
    field.held_peak = 0;
    let end = field.end.take();
    for run in held.chunk_by(|a, b| a.key8 == b.key8) {
        let body = run
            .iter()
            .map(encoded_entry_len)
            .fold(0u64, u64::saturating_add);
        if body > limits.max_body_bytes {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                field = name,
                entries = run.len(),
                ceiling = limits.max_body_bytes,
                "a key8's own index entries exceed the key-index leaf body ceiling; the entries carrying it are uncovered for the field"
            );
            for entry in run {
                field.note_uncovered(entry.entry_ordinal);
            }
        }
    }
    if field.uncovered.len() as u64 == part_entry_count {
        field.give_up();
        return;
    }
    if !field.stale {
        field.fed |= !held.is_empty();
        held.retain(|e| !field.uncovered.contains(&e.entry_ordinal));
        let uncovered: Vec<u64> = field.uncovered.iter().copied().collect();
        let encoded = key_index_pass_slices(&held, field.slice_lo, end, limits.max_body_bytes)
            .and_then(|slices| {
                slices
                    .into_iter()
                    .map(|slice| {
                        encode_part_key_index_leaf(
                            &KeyIndexLeafInput {
                                tenant_hash: *tenant,
                                signal,
                                part_blake3,
                                part_entry_count,
                                field: name,
                                key_type: field.key_type,
                                key8_lo: slice.key8_lo,
                                key8_hi: slice.key8_hi,
                                uncovered_entry_ordinals: &uncovered,
                                entries: &held[slice.entries],
                            },
                            limits,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()
            });
        drop(held);
        match encoded {
            Ok(leaves) => {
                if let Some(end) = end
                    && !leaves.is_empty()
                {
                    field.slice_lo = end;
                }
                pending.extend(
                    leaves
                        .into_iter()
                        .map(|leaf| (name.to_string(), field.attempt, leaf)),
                );
            }
            Err(err) => {
                tracing::error!(
                    tenant = %tenant.to_hex(),
                    field = name,
                    error = %err,
                    "key-index leaf encode refused; the part is uncovered for the field"
                );
                field.give_up();
                return;
            }
        }
    }
    match end {
        // A stale attempt keeps passing to the end of the key space so the
        // rebuild starts with every uncovered entry a pass can find.
        Some(end) => field.lo = end,
        None if field.stale && field.attempt == 0 => {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                field = name,
                "a later leaf-build pass found new uncovered entries; the field is rebuilt with the complete list"
            );
            field.attempt = 1;
            field.stale = false;
            field.fed = false;
            field.lo = 0;
            field.slice_lo = 0;
            field.refs.clear();
            field.pairs = 0;
        }
        None if field.stale => {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                field = name,
                "the field's rebuild found new uncovered entries again; the part is uncovered for the field"
            );
            field.give_up();
        }
        None => field.state = FieldBuildState::Done,
    }
}

/// `held`'s pairs sorted by `(key8, entry_ordinal)`, in the heap's own
/// buffer: `HeldPair` wraps one `KeyIndexLeafEntry`, so the collect reuses
/// the allocation rather than copying the pairs into a second one.
fn key_index_sorted_pairs(held: BinaryHeap<HeldPair>) -> Vec<KeyIndexLeafEntry> {
    held.into_sorted_vec()
        .into_iter()
        .map(|pair| pair.0)
        .collect()
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
    /// last fold and no hour an operator-asserted seal holds open (see
    /// [`Catalog::fold_with_seal_through`]) lists a commit HEAD does not
    /// hold. Every other failure mode that the metric index is allowed
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
    ///
    /// `default_retention_ns` is the caller-resolved deployment-level default
    /// retention window for this tenant (from the CLI-derived `RetentionConfig`
    /// in ravel-maintain, resolved by the caller via `window_for`), or `None`
    /// if no deployment default is configured. `ravel-catalog` never sees
    /// `RetentionConfig` itself: the dependency runs the other way (see
    /// [`crate::config::DEFAULT_PROTECTION_HORIZON_NS`] for the same
    /// one-directional mirroring precedent). The frontier reconcile below
    /// overlays the durable per-tenant `TenantConfig.retention_ns` on top of
    /// this default.
    pub async fn fold(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        folder_id: Uuid,
        now_ns: i64,
        transactions: &[Transaction],
        default_retention_ns: Option<i64>,
    ) -> Result<FoldReport, CatalogError> {
        self.fold_with_refold_request(
            tenant,
            signal,
            folder_id,
            now_ns,
            transactions,
            default_retention_ns,
            &RefoldRequest::new(),
        )
        .await
    }

    /// [`Catalog::fold`], plus a targeted re-fold of the ingest hours
    /// `refold_request` names (issue #526).
    ///
    /// The fixed reconcile window reaches only
    /// `fold_reconcile_window_hours` behind the previous watermark and the
    /// retention-frontier band reaches only the hours near the tenant's
    /// retirement frontier. A compaction or rewrite record that lands in an
    /// already-folded hour between those two bands is never observed by any
    /// later fold, so the snapshot keeps naming that hour's pre-rewrite
    /// inputs, the maintain tier's superseded-input sweep holds those objects
    /// on its HEAD-reachability gate (ADR-0020), and they occupy storage until
    /// retention drops the hour. This entry point is how the holder tells the
    /// fold which hours to re-list; see [`RefoldRequest`] for why the fold
    /// cannot derive that set itself.
    ///
    /// The requested hours are reconciled through exactly the same per-bucket
    /// path as the other two passes, so a re-fold can only ever make the
    /// snapshot agree with the commit layout. The pass is bounded by
    /// `frontier_reconcile_max_hours`, skips hours the other two passes already
    /// list, and skips hours the snapshot does not name (a re-fold of an hour
    /// no part covers would be pure cost). It also inherits ADR-0063 section
    /// 4's carve-outs: a first fold and a rebuild do no reconcile work at all,
    /// so a request made against either is ignored rather than adding LISTs to
    /// a fold that already derives every hour from the commit layout, and a
    /// fold that does not advance the watermark ignores a request too: it
    /// either returns before the reconcile block or reconciles only the hours
    /// an operator-asserted seal holds open (see
    /// [`Catalog::fold_with_seal_through`]). The requester resubmits a
    /// still-held hour on its next sweep.
    #[allow(clippy::too_many_arguments)]
    pub async fn fold_with_refold_request(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        folder_id: Uuid,
        now_ns: i64,
        transactions: &[Transaction],
        default_retention_ns: Option<i64>,
        refold_request: &RefoldRequest,
    ) -> Result<FoldReport, CatalogError> {
        self.fold_with_seal_through(
            tenant,
            signal,
            folder_id,
            now_ns,
            transactions,
            default_retention_ns,
            refold_request,
            None,
        )
        .await
    }

    /// [`Catalog::fold_with_refold_request`], sealing through at least
    /// `seal_through_hour` (ADR-2677 decision 1).
    ///
    /// The fold's target watermark is the greater of the hour the seal margin
    /// seals at `now_ns` and `seal_through_hour`. Supplying an hour is an
    /// operator assertion that no writer will publish a commit into that hour
    /// or any earlier one: the margin exists because a live writer may still
    /// do so. The assertion is not trusted past the margin, though: every
    /// later fold, with or without a seal-through hour, re-lists each hour
    /// above the margin hour at its own `now_ns` and at or below HEAD's
    /// watermark, one LIST per shard, and folds any level-0 commit record
    /// there that HEAD does not hold, so a commit published into such an hour
    /// is visible to queries that carry no commit token from the next fold
    /// on. Once the margin passes an hour it is sealed like any other, and a
    /// commit published into it after that stays invisible to those queries
    /// until a HEAD rebuild. `now_ns` stays the real time for everything else:
    /// HEAD's `created_unix_ns`, the retention frontier, and the outcome
    /// metrics. A target at or below HEAD's watermark is a no-op, exactly as
    /// for [`Catalog::fold`], unless that re-listing finds a commit HEAD does
    /// not hold, in which case the fold writes a HEAD at the same watermark.
    ///
    /// A `seal_through_hour` later than the hour bucket of `now_ns` is
    /// refused with [`CatalogError::InvalidConfig`] before anything is read:
    /// no writer can have published into an hour that has not begun.
    #[allow(clippy::too_many_arguments)]
    pub async fn fold_with_seal_through(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        folder_id: Uuid,
        now_ns: i64,
        transactions: &[Transaction],
        default_retention_ns: Option<i64>,
        refold_request: &RefoldRequest,
        seal_through_hour: Option<u32>,
    ) -> Result<FoldReport, CatalogError> {
        if let Some(hour) = seal_through_hour
            && i64::from(hour) > now_ns.div_euclid(NS_PER_HOUR)
        {
            return Err(CatalogError::InvalidConfig(
                "seal_through_hour is later than the hour bucket of now_ns",
            ));
        }
        let result = self
            .fold_inner(
                tenant,
                signal,
                folder_id,
                now_ns,
                transactions,
                default_retention_ns,
                refold_request,
                seal_through_hour,
            )
            .await;
        self.record_fold_outcome(
            signal,
            now_ns,
            result
                .as_ref()
                .ok()
                .map(|report| report.column_stats_dictionaries_dropped),
        );
        result
    }

    /// HEAD's watermark hour for `(tenant, signal)`, read exactly as a fold
    /// reads it. `None` when HEAD is absent or fails to decode, the two
    /// states a fold rebuilds from a full listing. A HEAD newer than this
    /// process understands is [`CatalogError::UnsupportedHeadVersion`], as it
    /// is for a fold.
    pub async fn head_watermark_hour(
        &self,
        tenant: &TenantHash,
        signal: Signal,
    ) -> Result<Option<u32>, CatalogError> {
        let mut counters = RequestCounters::default();
        match self
            .get_head(&head_object_key(tenant, signal), &mut counters)
            .await?
        {
            HeadState::UnsupportedVersion { format_version } => {
                Err(CatalogError::UnsupportedHeadVersion { format_version })
            }
            state => Ok(state.watermark_hour()),
        }
    }

    /// The last ingest-hour bucket the seal margin alone seals at `now_ns`
    /// under this catalog's config, with no seal-through hour supplied.
    /// `None` when no hour is sealed yet.
    pub fn margin_watermark_hour(&self, now_ns: i64) -> Option<u32> {
        sealed_watermark_hour(now_ns, self.config())
    }

    /// The fold body. Wrapped by [`Catalog::fold`] rather than instrumented
    /// in place because the body returns from a dozen points and through `?`,
    /// and a counter maintained at each of those is a counter that a later
    /// early return silently stops feeding.
    #[allow(clippy::too_many_arguments)]
    async fn fold_inner(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        folder_id: Uuid,
        now_ns: i64,
        _transactions: &[Transaction],
        default_retention_ns: Option<i64>,
        refold_request: &RefoldRequest,
        seal_through_hour: Option<u32>,
    ) -> Result<FoldReport, CatalogError> {
        let head_key = head_object_key(tenant, signal);
        let margin_hour = sealed_watermark_hour(now_ns, self.config());
        let applied_seal_through =
            seal_through_hour.filter(|ceiling| margin_hour.is_none_or(|hour| *ceiling > hour));
        let target_hour = applied_seal_through.or(margin_hour);
        // Fold never runs on the query path (module docs above) and keeps
        // its own `RequestCounters`; this handle exists only to satisfy the
        // shared cache/load API's `QueryAccounting` parameter and is discarded.
        // Declared before the generation read so that read's own GET (issue
        // #729: it now funnels through `guarded_get`) has a handle to charge,
        // discarded like every other fold-path GET.
        let accounting = QueryAccounting::new();
        // The generation history for the read-side scan rule (ADR-0052 section
        // 4/5), read fresh (see `Catalog::read_scan_generations`). A fold must
        // enumerate the same per-hour shard set a resolve would, and records
        // the fan-out ceiling and generation count into the HEAD it writes.
        let generations = self
            .read_scan_generations(tenant, signal, &accounting)
            .await?;
        let shard_generation_count = generations.len() as u32;
        let mut counters = RequestCounters::default();
        // Fold the generation-history read's GET(s) into the fold's own
        // counters. Under provisioning enforcement `read_scan_generations`
        // funnels one provisioning-record GET through `guarded_get` (issue
        // #729), charged only to the `accounting` handle this fold otherwise
        // discards; without counting it here `FoldReport.get_requests`
        // under-reports the store's real GETs by exactly one whenever
        // enforcement is on (issue #770). The snapshot is taken before the
        // retry loop reuses `accounting`, so it captures only this pre-loop
        // read; every in-loop guarded GET is counted explicitly at its call
        // site. With enforcement off the read issues no store request and this
        // adds zero.
        counters.get_requests += accounting.snapshot().s3_requests(AccountedOp::Get);
        let mut attempt: u32 = 0;

        // The tenant's effective retention window, read once per fold
        // (loop-invariant). Bounds the retention-frontier reconcile pass below
        // (ADR-0020 delete-blocker). Resolution mirrors the admission-limit
        // overlay in crates/ravel-ingest/src/lifecycle.rs (ADR-0078): the
        // durable per-tenant `TenantConfig.retention_ns` override wins when the
        // record exists and carries `Some`; otherwise the effective window is
        // `default_retention_ns`, the caller-resolved deployment-wide default.
        // A `TenantConfig` read failure or absent record is NOT "skip the
        // frontier reconcile": it means "fall back to the deployment default,"
        // which does not depend on that read at all. Only a tenant with neither
        // a per-tenant override nor a deployment default gets no frontier
        // reconcile (nothing is being retired). The fold's index role is a pure
        // optimization and the sweep's HEAD-reachability gate is the durable
        // delete blocker, so a config-read fault never fails the fold.
        let tenant_config = match crate::tenant_config::read_config_values(self.store(), tenant)
            .await
        {
            Ok(cfg) => cfg,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    tenant = %tenant.to_hex(),
                    "tenant config read failed; falling back to deployment-default retention and no typed attribute columns"
                );
                None
            }
        };
        let tenant_retention_ns: Option<i64> = crate::tenant_config::resolve_retention_window(
            tenant_config.as_ref(),
            default_retention_ns,
        );
        // ADR-0850: the tenant's configured typed logs attribute columns, if
        // any. `None`/absent config/a read failure all mean "no configured
        // columns" -- unlike `retention_ns`, `typed_attr_columns` has no
        // deployment-wide default threaded through `fold` (ravel-bench's own
        // consumer of this field treats absence the same way), so a fold
        // simply builds no column-stats artifact for that tenant.
        //
        // ADR-2707 decision 4: the key-index fields the tenant declares at
        // fold time. `None` builds no leaves and reads no section: the
        // switch is off, the signal is metrics and `metrics_label_index` is
        // off, or a logs tenant declares no key-index field.
        let key_index_fields: Option<Vec<(String, KeyType)>> =
            if self.config().build_key_index_leaves {
                declared_key_index_fields(signal, tenant_config.as_ref(), tenant, self.config())
                    .filter(|declared| !declared.is_empty())
            } else {
                None
            };
        let typed_attr_columns: Vec<DeclaredTypedColumn> = tenant_config
            .and_then(|cfg| cfg.typed_attr_columns)
            .unwrap_or_default();
        // Count the tenant-config record GET. `read_config_values` issues one
        // raw `store.get` per fold on every outcome (hit, absent, or error) and
        // takes neither a counters nor an accounting handle, so it is otherwise
        // invisible to the report. Read once before the retry loop, so it is one
        // GET per fold regardless of how many CAS attempts the loop makes.
        counters.get_requests += 1;

        // ADR-3694 decision 2: a metrics leaf built while an erasure request
        // is pending would store key8s of the strings being erased, so the
        // pending set is listed once per fold and any pending request (or a
        // listing that fails) builds no new metrics leaf this fold. Parts
        // carried by reference keep the refs they already have.
        let metrics_erasure_gated = if signal == Signal::Metrics && key_index_fields.is_some() {
            let gate = self.metrics_label_index_erasure_gate(tenant).await;
            counters.list_requests += gate.list_requests;
            counters.metrics_label_index_erasure_list_requests += gate.list_requests;
            counters.get_requests += gate.get_requests;
            gate.gated
        } else {
            false
        };
        // Taken before the erasure gate: a part carried while a request is
        // pending lacks a leaf the tenant declares all the same.
        let key_index_declared: Vec<String> = key_index_fields
            .iter()
            .flatten()
            .map(|(name, _)| name.clone())
            .collect();
        let key_index_fields = if metrics_erasure_gated {
            None
        } else {
            key_index_fields
        };

        // One tally cache for the WHOLE fold, CAS retries included: a lost
        // HEAD CAS restarts the loop, and a cache constructed inside it would
        // discard every successful tally and refetch the same objects on the
        // next attempt. The declared columns, tenant, and signal are fixed for
        // the fold, and a failed tally is never cached, so reuse across
        // attempts is safe by the cache's own contract.
        let mut column_stats_cache =
            column_stats_build::SegmentColumnStatsCache::new(&typed_attr_columns);
        // Each part's built leaf refs, by part hash, for the whole fold for the
        // same reason: an attempt after a lost HEAD CAS that re-encodes the
        // same part reads none of its sections or previous leaves again. The
        // leaves are content-addressed and already stored.
        let mut key_index_built: HashMap<[u8; 32], BuiltPartKeyIndex> = HashMap::new();
        let mut warned_switch_off_over_leaves = false;

        loop {
            let head_state = self.get_head(&head_key, &mut counters).await?;

            // ADR-3939 decision 4: with the switch off this fold partitions
            // by entry count alone and drops the leaf refs of every part it
            // re-encodes. Reported and warned once, never refused: turning
            // the switch off on purpose looks the same.
            let key_index_switch_off_over_leaves = !self.config().build_key_index_leaves
                && matches!(
                    &head_state,
                    HeadState::Valid { head, .. }
                        if head.parts.iter().any(|part| !part.key_index.is_empty())
                );
            if key_index_switch_off_over_leaves && !warned_switch_off_over_leaves {
                warned_switch_off_over_leaves = true;
                tracing::warn!(
                    tenant = %tenant.to_hex(),
                    signal = ?signal,
                    "the key-index leaf switch is off and the previous HEAD carries key-index leaf refs: this fold partitions by entry count and drops the refs of every part it re-encodes, and the next switch-on fold rebuilds those days' leaves; every process folding a tenant must agree on the switch"
                );
            }

            // Hours the HEAD covers only through an operator-asserted seal
            // (ADR-2677 decision 1), from its watermark and this fold's real
            // clock alone: the seal-through hour of an earlier fold is not
            // recorded anywhere, and the seal-through hour of this one says
            // nothing about what the HEAD already holds.
            let held_open = match &head_state {
                HeadState::Valid { head, .. } => held_open_hours(
                    head.watermark_hour,
                    margin_hour,
                    self.config().fold_reconcile_window_hours,
                ),
                _ => None,
            };
            let advance_to = target_hour
                .filter(|target| head_state.watermark_hour().is_none_or(|old| *target > old));
            // `held_listings` is `Some` only on a fold that does not advance
            // the watermark and runs solely to fold a late commit into a
            // held-open hour: its reconcile pass is those listings, plus a
            // targeted re-fold pass over any `refold_request` hours outside
            // the held range (see the reconcile-pass match below).
            //
            // `held_open_probe_cache` holds the parts the held-open pre-check
            // already read, keyed by part object key, so the main load below
            // reads each part once per fold. Empty on every other path.
            let mut held_open_probe_cache: HashMap<String, ChargedEntries> = HashMap::new();
            let (watermark_hour, held_listings) = match (advance_to, held_open, &head_state) {
                (Some(target), _, _) => (target, None),
                (None, Some(held), HeadState::Valid { head, .. }) => {
                    let held_buckets = hour_range_buckets(&generations, held.0, held.1);
                    let listings = self
                        .discover_bucket_listings(tenant, signal, &held_buckets)
                        .await?;
                    counters.list_requests += held_buckets.len() as u64;
                    let (has_unfolded, probe_cache) = self
                        .held_open_has_unfolded(tenant, head, held, &listings, &mut counters)
                        .await?;
                    held_open_probe_cache = probe_cache;
                    // A quiet held range returns before the full snapshot
                    // load, unless a refold request is queued: the targeted
                    // pass below needs every entry, and a fold over this HEAD
                    // takes no other path until the margin passes the
                    // watermark.
                    if !has_unfolded && refold_request.is_empty() {
                        return Ok(no_op_report(
                            Some(head.watermark_hour),
                            applied_seal_through,
                            counters,
                            0,
                            key_index_switch_off_over_leaves,
                        ));
                    }
                    (head.watermark_hour, Some(listings))
                }
                _ => {
                    return Ok(no_op_report(
                        head_state.watermark_hour(),
                        applied_seal_through,
                        counters,
                        0,
                        key_index_switch_off_over_leaves,
                    ));
                }
            };

            // `_previous_charge` holds the previous parts' decode
            // reservations for the rest of this attempt, as long as `entries`
            // holds what they charge for.
            let (mut entries, buckets, rebuilt, previous_entries_len, _previous_charge) =
                match &head_state {
                    HeadState::Valid { head, .. } => match self
                        .load_previous_entries(
                            tenant,
                            head,
                            &mut held_open_probe_cache,
                            &mut counters,
                        )
                        .await
                    {
                        Ok((entries, previous_charge)) => {
                            let buckets = incremental_buckets(
                                &generations,
                                head.watermark_hour,
                                watermark_hour,
                            );
                            let previous_entries_len = entries.len();
                            (
                                entries,
                                buckets,
                                false,
                                previous_entries_len,
                                previous_charge,
                            )
                        }
                        // A refused decode reservation fails this fold tick; the
                        // next tick retries. Rebuilding instead would re-list
                        // and re-read every hour the HEAD covers, far more than
                        // the one part that was just refused.
                        Err(CatalogError::MemoryExhausted(exhausted)) => {
                            return Err(CatalogError::MemoryExhausted(exhausted));
                        }
                        // A part naming another tenant is an isolation breach
                        // (ADR-0050 §2), which a rebuild would leave unreported,
                        // the same answer `load_previous_postings` gives.
                        Err(err) if is_foreign_part(&err) => return Err(err),
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
                            (Vec::new(), buckets, true, 0, Vec::new())
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
                        (Vec::new(), buckets, true, 0, Vec::new())
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

            // The candidate carry sources of this attempt's incremental leaf
            // build, every previous part with leaves, taken before `entries`
            // gains this fold's commits. Each part's build carries from those
            // whose hour range overlaps its own.
            let carry_sources: Vec<CarrySource> = match &head_state {
                HeadState::Valid { head, .. } if !rebuilt && key_index_fields.is_some() => {
                    CarrySource::all_of_head(head, &entries)
                }
                _ => Vec::new(),
            };

            // The previous HEAD whose parts' retired hours this fold settles
            // (ADR-3939 decision 6). None on a rebuild, whose entries come
            // from the listing, where a tombstoned hour contributes nothing.
            let retire_head: Option<&SnapshotHead> = match &head_state {
                HeadState::Valid { head, .. } if !rebuilt => Some(head),
                _ => None,
            };
            let leaf_switch_on = self.config().build_key_index_leaves;
            let mut retire = RetireTracker::of_head(retire_head, leaf_switch_on);
            // Hours already retired are not listed again by the frontier band.
            let already_retired: HashSet<u32> = retire_head
                .iter()
                .flat_map(|head| &head.parts)
                .flat_map(|part| part.retired_hours.iter().copied())
                .collect();

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
            // ADR-0873 stamp coverage for this fold attempt. Threaded through
            // the classifier like `layout_drift_count` rather than kept in a
            // thread-local: the fold is an async fn that can migrate threads
            // across any of its awaits, and a retried attempt must start from
            // zero rather than double-count the records the abandoned attempt
            // already read.
            let mut coverage = declared_stats::StampCoverage::default();

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
                        &mut coverage,
                    )
                    .await?;
                for (entry, is_canonical) in contribution {
                    derive_carry_keys_before_change(
                        &carry_sources,
                        tenant,
                        signal,
                        &entries,
                        &[entry.ingest_hour_bucket],
                    );
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
            // Hours the retention-frontier pass listed this fold, so the
            // targeted re-fold pass below does not list any of them a second
            // time (issue #526).
            let mut frontier_listed_hours: HashSet<u32> = HashSet::new();
            let mut refold_hours_reconciled: u64 = 0;
            if let (Some(listings), Some(held), Some(_)) =
                (&held_listings, held_open, reconcile_watermark)
            {
                // A fold that does not advance the watermark re-lists the
                // held-open hours, and below them only the requested hours of
                // a queued re-fold request: every other hour is reconciled by
                // the next fold that does advance it.
                for (shard, hour, listing) in listings {
                    self.reconcile_one_bucket(
                        tenant,
                        signal,
                        *shard,
                        *hour,
                        listing,
                        &accounting,
                        &mut entries,
                        &carry_sources,
                        &mut seen,
                        &mut dirty_hours,
                        &mut retire,
                        &mut counters,
                        &mut layout_drift_count,
                        &mut coverage,
                        true,
                    )
                    .await?;
                }
                // The held hours were just listed above, so the targeted
                // pass skips them.
                refold_hours_reconciled = self
                    .run_targeted_refold_pass(
                        tenant,
                        signal,
                        &generations,
                        refold_request,
                        |hour| (held.0..=held.1).contains(&hour),
                        &accounting,
                        &mut entries,
                        &carry_sources,
                        &mut seen,
                        &mut dirty_hours,
                        &mut retire,
                        &mut counters,
                        &mut layout_drift_count,
                        &mut coverage,
                    )
                    .await?;
            } else if let Some(watermark_hour_old) = reconcile_watermark {
                let window = self.config().fold_reconcile_window_hours;
                // The window always reaches the held-open hours, which sit at
                // or below the old watermark.
                let lo = held_open.map_or(watermark_hour_old.saturating_sub(window), |held| {
                    watermark_hour_old.saturating_sub(window).min(held.0)
                });
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
                        &carry_sources,
                        &mut seen,
                        &mut dirty_hours,
                        &mut retire,
                        &mut counters,
                        &mut layout_drift_count,
                        &mut coverage,
                        held_open.is_some_and(|held| (held.0..=held.1).contains(hour)),
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
                // dropped. `tenant_retention_ns` is the tenant's effective
                // retention window: the durable `TenantConfig.retention_ns`
                // override when present, else the deployment-wide default
                // (ADR-0078). A tenant with neither gets no frontier reconcile
                // (nothing is being retired).
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
                            if entry.ingest_hour_bucket <= frontier_hi
                                && !already_retired.contains(&entry.ingest_hour_bucket)
                            {
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
                        let frontier_buckets = hour_set_buckets(&generations, frontier_hours);
                        let frontier_listings = self
                            .discover_bucket_listings(tenant, signal, &frontier_buckets)
                            .await?;
                        counters.list_requests += frontier_buckets.len() as u64;
                        frontier_hours_reconciled = frontier_hours.len() as u64;
                        frontier_listed_hours.extend(frontier_hours.iter().copied());
                        for (shard, hour, listing) in &frontier_listings {
                            self.reconcile_one_bucket(
                                tenant,
                                signal,
                                *shard,
                                *hour,
                                listing,
                                &accounting,
                                &mut entries,
                                &carry_sources,
                                &mut seen,
                                &mut dirty_hours,
                                &mut retire,
                                &mut counters,
                                &mut layout_drift_count,
                                &mut coverage,
                                false,
                            )
                            .await?;
                        }
                    }
                }

                // ---- Targeted re-fold pass (issue #526) ----
                //
                // The two passes above are bounded by recency and by the
                // retention frontier. A compaction or rewrite record that
                // lands in an already-folded hour between those bands is
                // reached by neither, so the snapshot keeps naming that hour's
                // pre-rewrite inputs; the superseded-input sweep's
                // HEAD-reachability gate (ADR-0020) then holds those objects
                // instead of deleting them, and they occupy storage until
                // retention drops the hour. `refold_request` is how the holder
                // names those hours (see `RefoldRequest` for why the fold
                // cannot derive them itself). [`Self::run_targeted_refold_pass`]
                // re-lists them through the same per-bucket reconcile the
                // other two passes use, excluding whatever the fixed window
                // (`< lo`) or the frontier pass (`frontier_listed_hours`) just
                // covered, so no bucket is listed twice in one fold.
                refold_hours_reconciled = self
                    .run_targeted_refold_pass(
                        tenant,
                        signal,
                        &generations,
                        refold_request,
                        |hour| hour >= lo || frontier_listed_hours.contains(&hour),
                        &accounting,
                        &mut entries,
                        &carry_sources,
                        &mut seen,
                        &mut dirty_hours,
                        &mut retire,
                        &mut counters,
                        &mut layout_drift_count,
                        &mut coverage,
                    )
                    .await?;
            }
            if refold_hours_reconciled > 0 {
                tracing::debug!(
                    tenant = %tenant.to_hex(),
                    hours = refold_hours_reconciled,
                    "fold reconciled the ingest hours a re-fold request named"
                );
            }
            let retired_grown = apply_retirements(
                retire_head,
                leaf_switch_on,
                &retire,
                &mut entries,
                &mut seen,
                &mut dirty_hours,
                &carry_sources,
                tenant,
                signal,
            );
            // A fold run only for the held-open hours whose reconcile found
            // every listed commit already folded writes nothing. The report
            // still carries the hours the targeted pass re-listed.
            if held_listings.is_some()
                && !rebuilt
                && dirty_hours.is_empty()
                && retired_grown.is_empty()
            {
                return Ok(no_op_report(
                    Some(watermark_hour),
                    applied_seal_through,
                    counters,
                    refold_hours_reconciled,
                    key_index_switch_off_over_leaves,
                ));
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

            // Partition the fully-sorted entry set into hour-range parts under
            // the sealing policy (ADR-0063 section 1): whole hours accumulate
            // into the current part until adding the next hour would cross
            // `snapshot_part_max_entries`, or, with the leaf switch on, would
            // carry it into the next UTC day (ADR-3939 decision 1), then the
            // part seals at that hour boundary and a fresh tail starts. The concatenation of
            // range-ordered parts is exactly the globally sorted entry set, so
            // postings ordinals still index that concatenation and global
            // duplicate-freedom follows from per-part validation plus range
            // disjointness (no cross-part entry pass is needed).
            //
            // A retired hour is hidden only through the ref of the part that
            // holds it, so a span that re-encodes may not hold one: its
            // entries are dropped for good and the entries partitioned again.
            let (spans, existing_by_blake3, span_encodings) = loop {
                let spans = partition_parts(
                    &entries,
                    SealPolicy {
                        max_entries: self.config().snapshot_part_max_entries,
                        seal_utc_days: self.config().build_key_index_leaves,
                    },
                );

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

                // An old part carried forward by reference below
                // (`existing_by_blake3`) never touches `v3_content_baseline`: the
                // per-span loop skips the v3 block for it. Only an old part this
                // fold re-derives can use its prior per-segment statistics as a
                // baseline, so precompute which old part hashes the spans
                // reproduce byte-for-byte and fetch baselines for the rest only.
                //
                // Encoded once per span (kept in `span_encodings`, not discarded):
                // the per-span build loop below consumes the same bytes and hash
                // rather than re-running `encode_part_ranged` and `blake3::hash`
                // a second time over identical input.
                let single_part = spans.len() == 1;
                let span_encodings: Vec<Result<SpanEncoding, SnapshotFormatError>> = spans
                    .iter()
                    .enumerate()
                    .map(|(span_index, span)| {
                        let is_tail = span_index + 1 == spans.len();
                        let (min_hour, watermark) = if single_part {
                            (0, watermark_hour)
                        } else if is_tail {
                            (span.min_hour, watermark_hour)
                        } else {
                            (span.min_hour, span.watermark_hour)
                        };
                        let part_entries = &entries[span.start..span.end];
                        let bytes = snapshot_format::encode_part_ranged(
                            tenant.0,
                            signal_num,
                            shard_ceiling,
                            min_hour,
                            watermark,
                            part_entries,
                        )?;
                        let hash = blake3::hash(&bytes);
                        Ok(SpanEncoding {
                            min_hour,
                            watermark,
                            bytes,
                            hash,
                        })
                    })
                    .collect();
                let stranded = stranded_retired_hours(
                    retire_head,
                    &retired_grown,
                    &spans,
                    &span_encodings,
                    &existing_by_blake3,
                    &entries,
                );
                if stranded.is_empty() {
                    break (spans, existing_by_blake3, span_encodings);
                }
                let hours: Vec<u32> = stranded.iter().copied().collect();
                drop_entries(
                    &mut entries,
                    &mut seen,
                    &mut dirty_hours,
                    &carry_sources,
                    tenant,
                    signal,
                    &hours,
                    |entry| stranded.contains(&entry.ingest_hour_bucket),
                );
            };
            // A reconcile that changed any hour invalidates the append-only,
            // stable-ordinal assumption the forward postings merge relies on
            // (a superseded or removed entry shifts every later ordinal), so
            // postings must be rebuilt from scratch this fold, exactly as on
            // the rebuild path.
            let reconciled = !dirty_hours.is_empty();
            let reused_old_part_hashes: HashSet<[u8; 32]> = span_encodings
                .iter()
                .filter_map(|encoding| encoding.as_ref().ok())
                .filter_map(|encoding| {
                    let hash = *encoding.hash.as_bytes();
                    existing_by_blake3
                        .contains_key(hash.as_slice())
                        .then_some(hash)
                })
                .collect();

            // ADR-1413 decision 1 (v2 semantics): reuse the previous fold's
            // per-part v3 column-stats objects the same way the field-13 (v2)
            // baseline below reuses its own predecessor, so a part that grows
            // (e.g. an appended hour) does not re-derive every entry it
            // already covered. Keyed by content hash, exactly as the v2
            // baseline is (both key on `entry.content_hash`, the same join
            // key); merged across every old part still valid for reuse
            // (excluded: a part covering a dirty hour, whose old statistics
            // cannot be trusted forward, mirroring `existing_by_blake3`
            // above; also excluded: a part this fold's spans reproduce
            // byte-for-byte and will carry forward by reference, whose
            // baseline would never be consulted).
            let v3_content_baseline: HashMap<Vec<u8>, ColumnStatsSegment> = if typed_attr_columns
                .is_empty()
                || rebuilt
            {
                HashMap::new()
            } else if let HeadState::Valid { head, .. } = &head_state {
                let mut baseline = HashMap::new();
                for old_part in head
                    .parts
                    .iter()
                    .filter(|p| !part_covers_dirty_hour(p, &dirty_hours))
                {
                    let Some(stats_ref) = &old_part.column_stats else {
                        continue;
                    };
                    let Ok(old_part_hash) = <[u8; 32]>::try_from(old_part.blake3.as_slice()) else {
                        continue;
                    };
                    if reused_old_part_hashes.contains(&old_part_hash) {
                        continue;
                    }
                    match self.store().get(&stats_ref.key, GetRange::Full).await {
                        Ok(got) => {
                            counters.get_requests += 1;
                            match column_stats_build::decode_previous_column_stats(
                                &got.data,
                                &[old_part_hash],
                                &crate::snapshot_format::ColumnStatsLimits::default(),
                            ) {
                                Ok(segments) => {
                                    for segment in segments {
                                        baseline.insert(segment.writer_id.clone(), segment);
                                    }
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        error = %err,
                                        tenant = %tenant.to_hex(),
                                        "previous per-part column-stats object failed to decode or bind to its owning part; rebuilding every segment's statistics for parts that reuse it"
                                    );
                                }
                            }
                        }
                        // `NotFound` is reachable: a sealed part can be
                        // deleted out from under a ref a previous fold wrote,
                        // which `rebuild_restores_a_deleted_sealed_part_instead_of_reusing_its_ref`
                        // pins. It stays the quiet "nothing to reuse" case
                        // rather than being promoted, because only
                        // a non-NotFound failure (issue #1964, most commonly
                        // AccessDenied on a missing idx/* read grant) is an
                        // outage worth an operator's attention, since it
                        // recurs on every fold and silently turns reuse into
                        // a permanent full rebuild.
                        Err(StoreError::NotFound) => {
                            tracing::warn!(
                                key = %stats_ref.key,
                                tenant = %tenant.to_hex(),
                                "previous per-part column-stats object not found; rebuilding every segment's statistics for parts that reuse it"
                            );
                        }
                        Err(err) => {
                            tracing::error!(
                                error = %err,
                                key = %stats_ref.key,
                                tenant = %tenant.to_hex(),
                                "previous per-part column-stats object GET failed; rebuilding every segment's statistics for parts that reuse it"
                            );
                        }
                    }
                }
                baseline
            } else {
                HashMap::new()
            };

            // A metrics leaf build takes the postings build first, so an
            // entry it fetches whole can have its LABEL_DICT read from those
            // bytes, and the leaf build issues no GET for an entry held.
            // Other folds build the postings after the parts, as below. Only
            // the entries of parts this attempt builds a leaf for are held: a
            // part reused by reference, or one an earlier attempt already
            // built, needs none.
            let wanted_spans: Vec<std::ops::Range<usize>> = spans
                .iter()
                .zip(&span_encodings)
                .filter_map(|(span, encoding)| {
                    let hash = encoding.as_ref().ok()?.hash;
                    let reused = existing_by_blake3.contains_key(hash.as_bytes().as_slice())
                        || key_index_built.contains_key(hash.as_bytes());
                    (!reused).then_some(span.start..span.end)
                })
                .collect();
            let mut held_label_dicts =
                HeldLabelDicts::new(wanted_spans, self.config().key_index_build_max_pairs.max(1));
            let early_postings = if signal == Signal::Metrics && key_index_fields.is_some() {
                Some(
                    self.fold_postings_names(
                        tenant,
                        signal,
                        &head_state,
                        rebuilt || reconciled,
                        previous_entries_len,
                        &entries,
                        Some(&mut held_label_dicts),
                        &mut counters,
                        &accounting,
                    )
                    .await?,
                )
            } else {
                None
            };

            let mut part_refs: Vec<SnapshotPartRef> = Vec::with_capacity(spans.len());
            let mut part_hashes: Vec<[u8; 32]> = Vec::with_capacity(spans.len());
            let mut total_part_bytes: u64 = 0;
            let mut metrics_label_index_parts_over_cap: u64 = 0;
            let mut metrics_label_index_parts_erasure_gated: u64 = 0;
            let mut parts_reused: u64 = 0;
            let mut key_index_parts_missing_declared_leaves: u64 = 0;
            let mut key_index_missing_fields: BTreeSet<&str> = BTreeSet::new();
            let mut column_stats_part_objects_built: u64 = 0;
            let mut column_stats_dictionaries_dropped: u64 = 0;
            let mut key_index_uncovered_entries: u64 = 0;
            let mut key_index_leaves_written: u64 = 0;
            let mut key_index_leaf_bytes_written: u64 = 0;
            let mut key_index_field_pairs: BTreeMap<String, u64> = BTreeMap::new();
            for (span, encoding) in spans.iter().zip(span_encodings) {
                // Single-part fold keeps exact v1 semantics: min_hour 0 (the
                // epoch floor) and the fold watermark, byte-identical to the
                // legacy encode so existing single-part objects and their
                // content addresses never change. Multi-part: each part carries
                // its real first hour as min_hour; sealed parts end at their
                // last contained hour, and only the tail carries the fold
                // watermark, so HEAD.watermark == max part watermark == fold
                // watermark (the `validate_head` contract). Both already
                // resolved into `encoding` above.
                let SpanEncoding {
                    min_hour: part_min_hour,
                    watermark: part_watermark,
                    bytes: part_bytes,
                    hash: part_hash,
                } = encoding?;
                let part_entries = &entries[span.start..span.end];
                if let Some(existing) = existing_by_blake3.get(part_hash.as_bytes().as_slice()) {
                    // Carried by reference: the previous HEAD already names an
                    // object with these exact bytes, so no PUT is issued and
                    // the existing ref (same key/size/entry_count/range) is
                    // forwarded unchanged, but for the hours a tombstone
                    // retired this fold.
                    total_part_bytes += existing.size;
                    part_hashes.push(*part_hash.as_bytes());
                    let mut carried = existing.clone();
                    if let Some(retired) = retired_grown.get(&existing.blake3) {
                        carried.retired_hours.clone_from(retired);
                    }
                    part_refs.push(carried);
                    parts_reused += 1;
                    if existing.entry_count > 0 {
                        let mut missing = key_index_declared
                            .iter()
                            .filter(|field| {
                                !existing.key_index.iter().any(|leaf| &leaf.field == *field)
                            })
                            .peekable();
                        if missing.peek().is_some() {
                            key_index_parts_missing_declared_leaves += 1;
                            key_index_missing_fields.extend(missing.map(String::as_str));
                        }
                    }
                    continue;
                }
                let part_bytes_len = part_bytes.len() as u64;
                let part_crc = crc32c::crc32c(&part_bytes);
                let hash16 = &part_hash.to_hex()[..16];
                let part_key = part_object_key(tenant, signal, part_watermark, hash16);

                // ADR-1413: build (degrading dictionaries as needed to fit
                // the ceiling) the per-part (v3) column-statistics object
                // BEFORE writing the part's own `.csnap` object below. Unlike
                // the v1/v2 builds below, a failure here (no dictionary left
                // and still over ceiling) is never graceful: it must fail the
                // whole fold rather than publish a truncated object or
                // silently carry on without one (decision 4, amended).
                // Deriving it first means that refusal happens before the
                // part object is ever written, so a refused fold leaves no
                // orphaned `.csnap` behind for a part that will never gain a
                // HEAD entry.
                let column_stats = if typed_attr_columns.is_empty() {
                    None
                } else {
                    let mut v3_segments: Vec<ColumnStatsSegment> = Vec::new();
                    for entry in part_entries.iter() {
                        if let Some(existing) = v3_content_baseline.get(&entry.content_hash) {
                            v3_segments.push(existing.clone());
                            continue;
                        }
                        match column_stats_cache
                            .segment_column_stats(self.store(), tenant, signal, entry)
                            .await
                        {
                            Ok((mut segment, fetch)) => {
                                if fetch == column_stats_build::StatsFetch::Issued {
                                    counters.get_requests += 1;
                                }
                                // v2 semantics (ADR-1413): bind the record to
                                // this part's content hash, uniform for L0 and
                                // L1.
                                segment.writer_id = entry.content_hash.clone();
                                v3_segments.push(segment);
                            }
                            Err(err) => {
                                tracing::warn!(
                                    error = %err,
                                    tenant = %tenant.to_hex(),
                                    ingest_hour_bucket = entry.ingest_hour_bucket,
                                    shard = entry.shard,
                                    level = entry.level,
                                    "per-part column-stats build failed for one segment; it has no stats entry and queries over it fall back to scanning"
                                );
                            }
                        }
                    }
                    sort_and_dedup_part_segments(&mut v3_segments);

                    // ADR-1413 decision 4 (amended): degrade before refusing.
                    // While the segments' uncompressed body -- measured
                    // exactly as the encoder will measure it -- exceeds the
                    // ceiling, drop the largest remaining dictionary (by its
                    // own encoded size). min/max/count/sum are never touched:
                    // only `dictionary_present`/`dictionary` are cleared, the
                    // same omitted-dictionary shape ADR-0850 decision 3
                    // already uses for the cardinality ceiling. Only once no
                    // dictionary is left does `encode_column_stats_v3` below
                    // refuse.
                    //
                    // The body is never re-concatenated per drop: a part can
                    // need tens of thousands of drops against a
                    // multi-gigabyte body. Every (segment, column) pair's
                    // exact contribution to the body is measured once, kept
                    // in a max-heap, and a running total is adjusted by
                    // exactly the bytes each drop removes: the
                    // `DictEntry` bytes and the `dictionary_present` flag,
                    // plus any varint length-delimiter shrink on the
                    // enclosing `ColumnStat` and on the segment's own
                    // length-delimited framing (`encode_length_delimited_to_vec`,
                    // per `column_stats_segments_concat`). Popping by
                    // `(dict_size, seg_idx, col_idx)`'s natural tuple max
                    // orders the drops: largest size first, then the highest
                    // (seg_idx, col_idx).
                    let ceiling = self.column_stats_part_ceiling();
                    let mut segment_len: Vec<u64> = v3_segments
                        .iter()
                        .map(|seg| prost::Message::encoded_len(seg) as u64)
                        .collect();
                    #[cfg(test)]
                    record_column_stats_concat_call_for_test();
                    let mut running_total: u64 =
                        column_stats_segments_concat(&v3_segments).len() as u64;
                    let mut heap: BinaryHeap<(u64, usize, usize, u64)> = BinaryHeap::new();
                    for (seg_idx, seg) in v3_segments.iter().enumerate() {
                        for (col_idx, col) in seg.columns.iter().enumerate() {
                            if !col.dictionary_present {
                                continue;
                            }
                            let dict_size: u64 = col
                                .dictionary
                                .iter()
                                .map(|e| prost::Message::encoded_len(e) as u64)
                                .sum();
                            // Bytes the `ColumnStat`'s own body shrinks by:
                            // the `dictionary_present` flag (proto3 omits a
                            // false bool, so tag + varint(true) = 2 bytes
                            // recovered) plus each `DictEntry`'s own
                            // tag + length-varint + content.
                            let dict_content_shrink: u64 = 2 + col
                                .dictionary
                                .iter()
                                .map(|e| {
                                    let entry_len = prost::Message::encoded_len(e) as u64;
                                    1 + varint_len(entry_len) + entry_len
                                })
                                .sum::<u64>();
                            let col_len_before = prost::Message::encoded_len(col) as u64;
                            let col_len_after = col_len_before.saturating_sub(dict_content_shrink);
                            // The column's own embedding in `segment.columns`
                            // (tag + length-varint + content): the content
                            // shrink plus any varint-width drop in its own
                            // length prefix.
                            let seg_reduction = dict_content_shrink
                                + (varint_len(col_len_before) - varint_len(col_len_after));
                            heap.push((dict_size, seg_idx, col_idx, seg_reduction));
                        }
                    }

                    let mut dropped_this_part: u64 = 0;
                    while running_total > ceiling {
                        let Some((_, seg_idx, col_idx, seg_reduction)) = heap.pop() else {
                            break;
                        };
                        let seg_len_before = segment_len[seg_idx];
                        let seg_len_after = seg_len_before.saturating_sub(seg_reduction);
                        // Any varint-width drop in the segment's own
                        // length-delimited framing shrinks the body by that
                        // much more than the column's own contribution.
                        let body_reduction = seg_reduction
                            + (varint_len(seg_len_before) - varint_len(seg_len_after));
                        // Saturating, like the two subtractions above: the
                        // invariant makes these non-negative, and a broken
                        // invariant must reach the drift check as a typed
                        // error rather than wrap first.
                        running_total = running_total.saturating_sub(body_reduction);
                        segment_len[seg_idx] = seg_len_after;
                        let col = &mut v3_segments[seg_idx].columns[col_idx];
                        col.dictionary_present = false;
                        col.dictionary.clear();
                        dropped_this_part += 1;
                    }
                    if dropped_this_part > 0 {
                        tracing::info!(
                            tenant = %tenant.to_hex(),
                            part_key = %part_key,
                            dropped = dropped_this_part,
                            ceiling,
                            "per-part column-stats degrade dropped dictionaries to fit the ceiling"
                        );
                        column_stats_dictionaries_dropped += dropped_this_part;
                    }
                    // The incremental accounting above must agree exactly
                    // with what the encoder measures. Re-measuring once here
                    // (not on every drop) turns a drift between the two into
                    // an explicit refusal carrying both figures, rather than
                    // either stopping early on a still-over-ceiling body or
                    // silently publishing one.
                    #[cfg(test)]
                    record_column_stats_concat_call_for_test();
                    let measured_total = column_stats_segments_concat(&v3_segments).len() as u64;
                    if measured_total != running_total {
                        return Err(CatalogError::FieldMismatch {
                            key: part_key.clone(),
                            field: "column_stats_degrade_running_total",
                            expected: running_total.to_string(),
                            actual: measured_total.to_string(),
                        });
                    }
                    let stats_bytes = snapshot_format::encode_column_stats_v3(
                        tenant.0,
                        signal_num,
                        *part_hash.as_bytes(),
                        &v3_segments,
                        ceiling,
                    )
                    .map_err(|err| match err {
                        SnapshotFormatError::ColumnStatsPartOverBound { declared, ceiling } => {
                            CatalogError::ColumnStatsPartOverBound {
                                part_key: part_key.clone(),
                                declared,
                                ceiling,
                            }
                        }
                        other => CatalogError::SnapshotFormat(other),
                    })?;

                    let stats_crc = crc32c::crc32c(&stats_bytes);
                    let stats_hash = blake3::hash(&stats_bytes);
                    // Keyed by the hash of the object's OWN bytes
                    // (`stats_hash16`), the same derivation the field-13 (v2)
                    // object uses, NOT by the part's hash (`hash16`): the
                    // per-segment build has a warn-and-omit path
                    // (column_stats_cache above), so two folds over the same
                    // part can produce different statistics bytes. Keying by
                    // the part's hash would let a second fold's PUT collide
                    // with the first's under `AlreadyExists`, which this
                    // code (like the part PUT above) treats as "bytes are
                    // identical by construction" -- false here, leaving a
                    // field-7 ref whose blake3 describes bytes that were
                    // never stored. Content-addressing on the object's own
                    // bytes makes that assumption true again: identical
                    // bytes always collapse onto the same key, and differing
                    // bytes always land on a different one.
                    let stats_hash16 = &stats_hash.to_hex()[..16];
                    let stats_key =
                        column_stats_object_key(tenant, signal, part_watermark, stats_hash16);
                    let size = stats_bytes.len() as u64;
                    let segment_count = v3_segments.len() as u32;
                    let put_result = self
                        .store()
                        .put(
                            &stats_key,
                            Bytes::from(stats_bytes),
                            PutOptions::create_if_absent()
                                .with_checksum(UploadChecksum::Crc32c(stats_crc)),
                        )
                        .await;
                    // Counted unconditionally, before matching the outcome:
                    // this is a request-issued counter, not a
                    // request-succeeded one (#1598).
                    counters.put_requests += 1;
                    match put_result {
                        Ok(_) => {}
                        // Content-addressed under the object's own hash:
                        // bytes really are identical by construction here.
                        Err(StoreError::AlreadyExists) => {}
                        Err(e) => return Err(CatalogError::Store(e)),
                    }
                    column_stats_part_objects_built += 1;
                    Some(SnapshotColumnStatsPartRef {
                        key: stats_key,
                        blake3: stats_hash.as_bytes().to_vec(),
                        size,
                        segment_count,
                        part_blake3: vec![part_hash.as_bytes().to_vec()],
                    })
                };

                // ADR-2707 decision 4: this part's key-index leaves, built from
                // its entries' KEY_IDX sections by ranged GET and PUT before
                // the part object below. A failed leaf PUT fails the fold
                // before this part's PUT and before any HEAD CAS, so no HEAD
                // references a part without its leaves; parts this fold
                // already wrote are unreferenced and left to the catalog
                // sweep. The reads share the fold's bucket concurrency bound.
                let mut key_index = Vec::new();
                if metrics_erasure_gated {
                    metrics_label_index_parts_erasure_gated += 1;
                }
                if let Some(declared) = &key_index_fields {
                    let built = match key_index_built.get(part_hash.as_bytes()) {
                        Some(built) => built.clone(),
                        None => {
                            // ADR-3694 decision 2: the metrics byte cap, a
                            // ratio of the part's own data bytes.
                            let leaf_bytes_cap = (signal == Signal::Metrics).then(|| {
                                metrics_label_index_leaf_bytes_cap(
                                    part_entries.iter().map(|e| e.object_size).sum(),
                                    self.config().metrics_label_index_max_ratio_ppm,
                                )
                            });
                            let sources: Vec<&CarrySource> = carry_sources
                                .iter()
                                .filter(|source| {
                                    source.overlaps_hours(part_min_hour, part_watermark)
                                        && source
                                            .derive_object_keys(tenant, signal, &entries)
                                            .is_some()
                                })
                                .collect();
                            let built = build_part_key_index(
                                self.store(),
                                tenant,
                                signal,
                                *part_hash.as_bytes(),
                                part_watermark,
                                part_entries,
                                declared,
                                &sources,
                                held_label_dicts.part(span.start),
                                &KeyIndexBuildLimits {
                                    leaf: key_index_leaf_limits(),
                                    max_pairs: self.config().key_index_build_max_pairs,
                                    concurrency: self.config().fold_bucket_concurrency,
                                    leaf_bytes_cap,
                                },
                                &mut counters,
                            )
                            .await
                            .map_err(CatalogError::Store)?;
                            held_label_dicts.release(span.start..span.end);
                            key_index_built.insert(*part_hash.as_bytes(), built.clone());
                            built
                        }
                    };
                    if built.over_cap {
                        metrics_label_index_parts_over_cap += 1;
                    }
                    key_index_uncovered_entries += built.uncovered_entries;
                    key_index_leaves_written += built.refs.len() as u64;
                    key_index_leaf_bytes_written +=
                        built.refs.iter().map(|leaf| leaf.size).sum::<u64>();
                    for (field, pairs) in built.field_pairs {
                        *key_index_field_pairs.entry(field).or_insert(0) += pairs;
                    }
                    key_index = built.refs;
                }

                let put_result = self
                    .store()
                    .put(
                        &part_key,
                        Bytes::from(part_bytes),
                        PutOptions::create_if_absent()
                            .with_checksum(UploadChecksum::Crc32c(part_crc)),
                    )
                    .await;
                // Counted unconditionally, before matching the outcome: this
                // is a request-issued counter, not a request-succeeded one
                // (#1598).
                counters.put_requests += 1;
                match put_result {
                    Ok(_) => {}
                    // Content-addressed key: bytes are identical by
                    // construction, so a losing folder's part is as good as
                    // its own (mirrors `publish::put_data_object`).
                    Err(StoreError::AlreadyExists) => {}
                    Err(e) => return Err(CatalogError::Store(e)),
                }
                total_part_bytes += part_bytes_len;
                part_hashes.push(*part_hash.as_bytes());

                part_refs.push(SnapshotPartRef {
                    key: part_key,
                    blake3: part_hash.as_bytes().to_vec(),
                    size: part_bytes_len,
                    entry_count: part_entries.len() as u64,
                    watermark_hour: part_watermark,
                    min_hour: part_min_hour,
                    column_stats,
                    key_index,
                    retired_hours: Vec::new(),
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
            //
            // Level is not gated here: `fetch_segment_names` derives an L1
            // entry's key and identity the same way `build_segment_ref_from_entry`
            // does for a query read, so a compacted bucket's `__name__` set
            // is decoded exactly like an L0 one. The forward-merge ordinal
            // assumption a compaction rewrite breaks is guarded by
            // `reconciled` above, not by level: a bucket whose content
            // changed under an already-recorded ordinal (an L0 run superseded
            // by its L1 compaction) always lands in `dirty_hours` and forces
            // `decode_start = 0` here, while a bucket that arrives
            // already-compacted on its first fold is a plain hour-major
            // append and never disturbs a previously-recorded ordinal.
            let postings_names = match early_postings {
                Some(names) => names,
                None => {
                    self.fold_postings_names(
                        tenant,
                        signal,
                        &head_state,
                        rebuilt || reconciled,
                        previous_entries_len,
                        &entries,
                        None,
                        &mut counters,
                        &accounting,
                    )
                    .await?
                }
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
                        let put_result = self
                            .store()
                            .put(
                                &postings_key,
                                Bytes::from(postings_bytes),
                                PutOptions::create_if_absent()
                                    .with_checksum(UploadChecksum::Crc32c(postings_crc)),
                            )
                            .await;
                        // Counted unconditionally: a real (non-AlreadyExists)
                        // Err here still means the store received a PUT
                        // request, and may have durably written the object
                        // despite the client-visible error (#1598).
                        counters.put_requests += 1;
                        match put_result {
                            Ok(_) | Err(StoreError::AlreadyExists) => {
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

            let mut new_head = SnapshotHead {
                format_version: HEAD_FORMAT_VERSION_1,
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
            new_head.format_version = snapshot_format::head_format_version_for(&new_head);
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

            let head_put_result = self
                .store()
                .put(
                    &head_key,
                    Bytes::from(head_bytes),
                    PutOptions {
                        mode: put_mode,
                        checksum: Some(UploadChecksum::Crc32c(head_crc)),
                    },
                )
                .await;
            // Counted unconditionally, before matching the outcome: this is
            // a request-issued counter, not a request-succeeded one (#1598).
            counters.put_requests += 1;
            match head_put_result {
                Ok(_) => {
                    // Only a fold that reached its committed HEAD contributes
                    // to the process-global coverage totals, so an attempt
                    // abandoned to a CAS loss and retried from zero above is
                    // counted exactly once, by whichever attempt won.
                    declared_stats::observe_fold_stamp_coverage(coverage);
                    observe_fold_key_index(&counters);
                    if key_index_parts_missing_declared_leaves > 0 {
                        tracing::warn!(
                            tenant = %tenant.to_hex(),
                            signal = ?signal,
                            parts = key_index_parts_missing_declared_leaves,
                            fields = ?key_index_missing_fields,
                            "parts carried by reference lack a key-index leaf for a declared field; the fold builds no leaf for a part it carries. ravel-cli catalog fold --rebuild-key-index attaches the leaves of a day sealed under the metrics erasure gate or before the field was declared; a field over the metrics byte cap, or with no indexable entries, stays without a leaf after the repair and keeps this count nonzero"
                        );
                    }
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
                        column_stats_part_objects_built,
                        column_stats_dictionaries_dropped,
                        layout_drift_count,
                        frontier_hours_reconciled,
                        frontier_hours_deferred,
                        refold_hours_reconciled: refold_hours_reconciled as usize,
                        stamped_records: coverage.records(),
                        stamped_entries: coverage.entries(),
                        seal_through_hour: applied_seal_through,
                        key_index_ranged_gets: counters.key_index_ranged_gets,
                        key_index_bytes_read: counters.key_index_bytes_read,
                        key_index_leaves_written,
                        key_index_leaf_bytes_written,
                        key_index_uncovered_entries,
                        key_index_sections_read: counters.key_index_sections_read,
                        key_index_entries_carried: counters.key_index_entries_carried,
                        key_index_build_passes: counters.key_index_build_passes,
                        key_index_pair_budget: if key_index_fields.is_some() {
                            self.config().key_index_build_max_pairs.max(1)
                        } else {
                            0
                        },
                        key_index_field_pairs,
                        key_index_switch_off_over_leaves,
                        key_index_parts_missing_declared_leaves,
                        key_index_leaf_gets: counters.key_index_leaf_gets,
                        key_index_carry_peak_held_bytes: counters.key_index_carry_peak_held_bytes,
                        metrics_label_index_parts_over_cap,
                        metrics_label_index_parts_erasure_gated,
                        metrics_label_index_erasure_list_requests: counters
                            .metrics_label_index_erasure_list_requests,
                    });
                }
                // Another folder's HEAD CAS won first. Re-GET HEAD next
                // iteration: if the winner's watermark already covers ours,
                // the top-of-loop no-op check stops cleanly; otherwise we
                // rebase onto the winner's parts and retry.
                Err(StoreError::PreconditionFailed) | Err(StoreError::AlreadyExists) => {
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

    pub(crate) async fn get_head(
        &self,
        head_key: &str,
        counters: &mut RequestCounters,
    ) -> Result<HeadState, CatalogError> {
        match self.store().get(head_key, GetRange::Full).await {
            Ok(got) => {
                counters.get_requests += 1;
                match snapshot_format::decode_head(&got.data) {
                    Ok(head) => Ok(HeadState::Valid {
                        head: Box::new(head),
                        version: got.version,
                    }),
                    // A HEAD newer than this process understands is NOT
                    // corruption and must NOT be rebuilt (ADR-0066 decision 2,
                    // "fail-closed-on-newer"): rebuilding would CAS-clobber a
                    // newer HEAD written by an upgraded peer. Keep it a
                    // distinct state so the fold fails loudly.
                    Err(SnapshotFormatError::UnsupportedHeadVersion(format_version))
                        if snapshot_format::head_format_version_is_newer(format_version) =>
                    {
                        tracing::error!(
                            key = %head_key,
                            format_version,
                            "HEAD is a newer format than this process understands; refusing to rebuild"
                        );
                        Ok(HeadState::UnsupportedVersion { format_version })
                    }
                    // Below the supported floor (`decode_head` accepts only
                    // `HEAD_FORMAT_VERSIONS`, so the floor is the lowest of
                    // them): corrupt, or written by an older peer during a
                    // rolling upgrade. Either way it is rebuilt like any
                    // undecodable HEAD: the CAS on `got.version` only moves
                    // a HEAD forward, and an older peer refuses the result.
                    Err(SnapshotFormatError::UnsupportedHeadVersion(format_version)) => {
                        tracing::warn!(
                            key = %head_key,
                            format_version,
                            supported = ?HEAD_FORMAT_VERSIONS,
                            "HEAD format_version is below the supported version, rebuilding"
                        );
                        Ok(HeadState::Corrupt {
                            version: got.version,
                        })
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
    /// failure (GET error, hash mismatch, a part naming another tenant, decode
    /// error, refused decode reservation) is returned to the caller, which
    /// fails the fold on a foreign part or a refused reservation and falls
    /// back to a full rebuild on anything else.
    ///
    /// Returns the entries together with every part's decode reservation; the
    /// caller holds the reservations for as long as it holds the entries.
    ///
    /// `probe_cache` holds the parts a held-open fold's
    /// `held_open_has_unfolded` pre-check already read, keyed by part object
    /// key, so each part is read once per fold. Each one used here is moved
    /// out of it, reservation included. Empty on every other path.
    async fn load_previous_entries(
        &self,
        tenant: &TenantHash,
        head: &SnapshotHead,
        probe_cache: &mut HashMap<String, ChargedEntries>,
        counters: &mut RequestCounters,
    ) -> Result<(Vec<SnapshotEntry>, Vec<Reservation>), CatalogError> {
        let mut entries = Vec::new();
        let mut reservations = Vec::with_capacity(head.parts.len());
        for part_ref in &head.parts {
            let (part_entries, reservation) = match probe_cache.remove(&part_ref.key) {
                Some(cached) => cached,
                None => self.load_part_entries(tenant, part_ref, counters).await?,
            };
            entries.extend(part_entries);
            reservations.push(reservation);
        }
        Ok((entries, reservations))
    }

    /// Load one part HEAD names, verifying its bytes against the recorded
    /// blake3 before decoding them.
    ///
    /// A part whose header names a tenant other than `tenant` is an isolation
    /// breach (ADR-0050 §2): it is counted and returned as a `tenant_hash`
    /// [`CatalogError::FieldMismatch`], before anything is reserved.
    ///
    /// The decoded entry body is charged to the catalog's memory budget
    /// before the decode (ADR-1170, the catalog decode charge amendment), the
    /// same way the resolve path charges a part: the returned reservation
    /// holds `entries_uncompressed_len` until the caller drops it. A refusal
    /// is [`CatalogError::MemoryExhausted`].
    pub(crate) async fn load_part_entries(
        &self,
        tenant: &TenantHash,
        part_ref: &SnapshotPartRef,
        counters: &mut RequestCounters,
    ) -> Result<ChargedEntries, CatalogError> {
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
        let limits = PartLimits::default();
        let header = snapshot_format::decode_part_header(&got.data)?;
        // ADR-0050 §2, before the reservation below, exactly as
        // `snapshot_resolve` checks a part: the HEAD's blake3 binds the bytes
        // to the HEAD, and only the header's tenant_hash binds them to the
        // tenant. Checked any later, a budget with no room would refuse first
        // and report memory pressure where a cross-tenant part was.
        if header.tenant_hash.as_slice() != tenant.0.as_slice() {
            self.record_isolation_breach();
            return Err(CatalogError::FieldMismatch {
                key: part_ref.key.clone(),
                field: "tenant_hash",
                expected: tenant.to_hex(),
                actual: hex::encode(&header.tenant_hash),
            });
        }
        let reservation = self.reserve_decoded(
            header.entries_uncompressed_len,
            limits.max_snapshot_part_bytes,
        )?;
        let decoded = snapshot_format::decode_part(&got.data, &limits)?;
        Ok((decoded.entries, reservation))
    }

    /// Whether the held-open hours `held` (see [`held_open_hours`]) list a
    /// level-0 commit record HEAD's parts do not hold. LISTs nothing itself:
    /// `listings` is the caller's re-listing of exactly those hours. GETs only
    /// the parts whose hour range meets `held`, and none at all when the
    /// listings name no commit record. A part that cannot be read answers
    /// true, so the caller takes the full fold path, whose own load of the
    /// same part fails and rebuilds, or fails the fold when the failure is a
    /// refused decode reservation. A part naming another tenant is returned
    /// as the error instead, failing the fold here, so the breach is counted
    /// once rather than again by the main load.
    ///
    /// Also returns every part read here with its decode reservation, keyed
    /// by part object key, so `load_previous_entries` does not read it again
    /// this fold. A part that fails here is not cached; the main load reads it
    /// again and answers the same failure the same way.
    async fn held_open_has_unfolded(
        &self,
        tenant: &TenantHash,
        head: &SnapshotHead,
        held: (u32, u32),
        listings: &[BucketListing],
        counters: &mut RequestCounters,
    ) -> Result<(bool, HashMap<String, ChargedEntries>), CatalogError> {
        let names_a_commit = listings.iter().any(|(_, _, listing)| {
            listing.iter().any(|meta| {
                matches!(
                    keys::partition_bucket_entry(&meta.key),
                    Ok(BucketEntry::CommitRecord(_))
                )
            })
        });
        if !names_a_commit {
            return Ok((false, HashMap::new()));
        }
        let mut folded: HashSet<EntryIdentity> = HashSet::new();
        let mut loaded: HashMap<String, ChargedEntries> = HashMap::new();
        for part_ref in head
            .parts
            .iter()
            .filter(|part| part.min_hour <= held.1 && part.watermark_hour >= held.0)
        {
            match self.load_part_entries(tenant, part_ref, counters).await {
                Ok(charged) => {
                    folded.extend(
                        charged
                            .0
                            .iter()
                            .filter(|entry| (held.0..=held.1).contains(&entry.ingest_hour_bucket))
                            .map(entry_identity),
                    );
                    loaded.insert(part_ref.key.clone(), charged);
                }
                Err(err) if is_foreign_part(&err) => return Err(err),
                Err(_) => return Ok((true, loaded)),
            }
        }
        let has_unfolded = listings
            .iter()
            .any(|(_, _, listing)| lists_an_unfolded_commit(listing, |id| folded.contains(id)));
        Ok((has_unfolded, loaded))
    }

    /// Load and verify the previous fold's postings object before trusting
    /// it as a merge baseline: same
    /// hash/tenant/part-binding/decode checks `snapshot_resolve`'s
    /// `load_snapshot_postings` applies at query time, plus an
    /// `entry_count` check against `previous_entry_count` -- the ordinal
    /// boundary this fold's merge carries forward, not the post-fold total.
    /// Most failures (no postings ref, cache miss, a `NotFound` GET, hash
    /// mismatch, decode error, entry_count mismatch) return `Ok(None)`, so the
    /// caller decodes every current entry from scratch instead of merging.
    /// A GET failure other than `NotFound` (issue #1964, most commonly
    /// AccessDenied on a missing idx/* read grant) also returns `Ok(None)` --
    /// merging is an optimization, never a correctness gate -- but is logged
    /// at `error!`, not `warn!`, since it recurs on every fold and silently
    /// turns postings reuse into a permanent full rebuild.
    ///
    /// Two failures are errors instead, both for the same reason the resolve
    /// path treats them as errors (ADR-0050 §2, ADR-1702 decision 6 and the
    /// decode-refusal amendment): a postings object whose header names another
    /// tenant is an isolation breach, which a silent rebuild would leave
    /// unreported; and a refused decode reservation is memory pressure, which
    /// the rebuild would answer by fetching and decoding every segment's names
    /// instead of one postings object, holding more memory rather than less.
    async fn load_previous_postings(
        &self,
        tenant: &TenantHash,
        head: &SnapshotHead,
        previous_entry_count: u64,
        counters: &mut RequestCounters,
        accounting: &QueryAccounting,
    ) -> Result<Option<Vec<NamePostings>>, CatalogError> {
        let Some(postings_ref) = head.postings.as_ref() else {
            return Ok(None);
        };
        let expected_part_blake3: Vec<[u8; 32]> = match head
            .parts
            .iter()
            .map(|p| <[u8; 32]>::try_from(p.blake3.as_slice()))
            .collect::<Result<_, _>>()
        {
            Ok(hashes) => hashes,
            Err(_) => return Ok(None),
        };

        if let Some(cached) = self
            .postings_cache()
            .get(tenant, &postings_ref.key, accounting)
        {
            if cached.header.entry_count == previous_entry_count {
                return Ok(Some(cached.names.clone()));
            }
            tracing::warn!(
                key = %postings_ref.key,
                "cached previous postings entry_count mismatch, rebuilding postings from scratch"
            );
            return Ok(None);
        }

        let got = match self.store().get(&postings_ref.key, GetRange::Full).await {
            Ok(got) => got,
            // `NotFound` is reachable here too: a sealed part can be deleted
            // out from under a ref a previous fold wrote. It stays quiet for
            // the same reason as the `.cstat` read above; only a non-NotFound
            // failure (issue #1964, most commonly AccessDenied on a missing
            // idx/* read grant) is an outage worth an operator's attention,
            // since it recurs on every fold and silently turns reuse into a
            // permanent full rebuild.
            Err(StoreError::NotFound) => {
                tracing::warn!(
                    key = %postings_ref.key,
                    tenant = %tenant.to_hex(),
                    "previous postings not found, rebuilding postings from scratch"
                );
                return Ok(None);
            }
            Err(err) => {
                tracing::error!(
                    error = %err,
                    key = %postings_ref.key,
                    tenant = %tenant.to_hex(),
                    "previous postings GET failed, rebuilding postings from scratch"
                );
                return Ok(None);
            }
        };
        counters.get_requests += 1;
        let digest = blake3::hash(&got.data);
        if digest.as_bytes().as_slice() != postings_ref.blake3.as_slice() {
            tracing::warn!(
                key = %postings_ref.key,
                "previous postings hash mismatch, rebuilding postings from scratch"
            );
            return Ok(None);
        }
        let limits = snapshot_format::PostingsLimits {
            max_postings_bytes: self.config().max_postings_bytes,
        };
        // The decoded postings go into the same postings cache the resolve
        // path reads, so they carry a decode reservation the same way
        // (ADR-1702 decision 6).
        let header = match snapshot_format::decode_postings_header(&got.data) {
            Ok(header) => header,
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    key = %postings_ref.key,
                    "previous postings header unreadable, rebuilding postings from scratch"
                );
                return Ok(None);
            }
        };
        // ADR-0050 §2, read off the already hash-verified bytes and before the
        // reservation below, exactly as `load_snapshot_postings` does it on the
        // resolve path. `decode_postings` only binds the object to this HEAD's
        // part hashes, and that mismatch degrades to a rebuild, so a foreign
        // tenant_hash checked any later would be masked by the degrade; and a
        // budget with no room would otherwise refuse first, reporting memory
        // pressure where a cross-tenant object was.
        match <[u8; 16]>::try_from(header.tenant_hash.as_slice()) {
            Ok(declared) if declared != tenant.0 => {
                self.record_isolation_breach();
                return Err(CatalogError::FieldMismatch {
                    key: postings_ref.key.clone(),
                    field: "tenant_hash",
                    expected: tenant.to_hex(),
                    actual: hex::encode(declared),
                });
            }
            Ok(_) => {}
            Err(_) => {
                tracing::warn!(
                    key = %postings_ref.key,
                    "previous postings header tenant_hash malformed, rebuilding postings from scratch"
                );
                return Ok(None);
            }
        }
        // A refusal fails the fold rather than rebuilding: the rebuild fetches
        // and decodes every segment's names, holding more memory than the one
        // postings decode just refused. The resolve path answers the same
        // refusal the same way (ADR-1702, the decode-refusal amendment). A
        // refusal the eviction pass cannot clear (see
        // `Catalog::reserve_decoded`) recurs on every fold while it holds, so
        // that tenant's fold stays failed.
        let reservation =
            self.reserve_decoded(header.body_uncompressed_len, limits.max_postings_bytes)?;
        let decoded =
            match snapshot_format::decode_postings(&got.data, &limits, &expected_part_blake3) {
                Ok(decoded) => crate::charged::Charged::new(decoded, reservation),
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        key = %postings_ref.key,
                        "previous postings failed to decode, rebuilding postings from scratch"
                    );
                    return Ok(None);
                }
            };
        if decoded.header.entry_count != previous_entry_count {
            tracing::warn!(
                key = %postings_ref.key,
                expected = previous_entry_count,
                actual = decoded.header.entry_count,
                "previous postings entry_count mismatch, rebuilding postings from scratch"
            );
            return Ok(None);
        }

        let decoded = Arc::new(decoded);
        self.decoded_caches().insert_postings(
            *tenant,
            postings_ref.key.clone(),
            decoded.clone(),
            got.data.len() as u64,
            self.config().postings_cache_entries,
        );
        Ok(Some(decoded.names.clone()))
    }

    /// ADR-3694 decision 2: a metrics leaf built while an erasure request is
    /// pending would store key8s of the strings being erased. Lists the
    /// tenant's pending metrics erasure once; any pending request, or a
    /// listing that fails, gates the build.
    pub(crate) async fn metrics_label_index_erasure_gate(
        &self,
        tenant: &TenantHash,
    ) -> MetricsErasureGate {
        let accounting = QueryAccounting::new();
        let pending = self
            .list_pending_erasure(tenant, Signal::Metrics, &accounting)
            .await;
        let snapshot = accounting.snapshot();
        let gated = match pending {
            Ok(pending) => !pending.is_empty(),
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    tenant = %tenant.to_hex(),
                    "pending-erasure listing failed; no metrics label_value leaf is built"
                );
                true
            }
        };
        MetricsErasureGate {
            gated,
            list_requests: snapshot.s3_requests(AccountedOp::List),
            get_requests: snapshot.s3_requests(AccountedOp::Get),
        }
    }

    /// The fold's name postings for `entries`: merged forward from the
    /// previous HEAD's postings unless `from_scratch` or no usable baseline
    /// exists, then built by [`Catalog::build_postings`].
    #[allow(clippy::too_many_arguments)]
    async fn fold_postings_names(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        head_state: &HeadState,
        from_scratch: bool,
        previous_entries_len: usize,
        entries: &[SnapshotEntry],
        held_label_dicts: Option<&mut HeldLabelDicts>,
        counters: &mut RequestCounters,
        accounting: &QueryAccounting,
    ) -> Result<Option<Vec<NamePostings>>, CatalogError> {
        let (baseline, decode_start): (Vec<NamePostings>, usize) = if from_scratch {
            (Vec::new(), 0)
        } else if let HeadState::Valid { head, .. } = head_state {
            match self
                .load_previous_postings(
                    tenant,
                    head,
                    previous_entries_len as u64,
                    counters,
                    accounting,
                )
                .await?
            {
                Some(names) => (names, previous_entries_len),
                None => (Vec::new(), 0),
            }
        } else {
            (Vec::new(), 0)
        };
        Ok(self
            .build_postings(
                tenant,
                signal,
                entries,
                decode_start,
                baseline,
                held_label_dicts,
                counters,
            )
            .await)
    }

    /// Build the name-postings index for `entries`, merging forward from `baseline` rather than re-decoding every
    /// historical segment on every fold: entries before `decode_start` keep
    /// exactly the ordinals `baseline` already recorded for them, and only
    /// entries at or past `decode_start` are fetched and decoded. Returns
    /// `None` on any failure (a segment fetch, identity check, or decode
    /// error, or a series missing `__name__`), logging the cause: a
    /// postings build is all-or-nothing over the entries it decodes, never
    /// partial.
    ///
    /// With `held_label_dicts`, each fetched object's metrics `label_value`
    /// field is offered to it from the bytes in hand, under the entry's
    /// ordinal; an entry it holds is read by the leaf build with no GET.
    #[allow(clippy::too_many_arguments)]
    async fn build_postings(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        entries: &[SnapshotEntry],
        decode_start: usize,
        baseline: Vec<NamePostings>,
        mut held_label_dicts: Option<&mut HeldLabelDicts>,
        counters: &mut RequestCounters,
    ) -> Option<Vec<NamePostings>> {
        let mut by_name: BTreeMap<String, Vec<u64>> = baseline
            .into_iter()
            .map(|np| (np.name, np.ordinals))
            .collect();
        for (ordinal, entry) in entries.iter().enumerate().skip(decode_start) {
            let fetched = fetch_segment_object(self.store(), tenant, signal, entry).await;
            let names = match fetched.and_then(|(_, data, location)| {
                let names = segment_names_from_object(&location, &data)?;
                if let Some(held) = held_label_dicts.as_deref_mut() {
                    held.offer(ordinal, || {
                        metrics_label_value_from_object(tenant, &data, &location)
                    });
                }
                Ok(names)
            }) {
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
        carry_sources: &[CarrySource],
        seen: &mut HashMap<EntryIdentity, (usize, bool)>,
        dirty_hours: &mut HashSet<u32>,
        retire: &mut RetireTracker,
        counters: &mut RequestCounters,
        layout_drift_count: &mut u64,
        coverage: &mut declared_stats::StampCoverage,
        held_open: bool,
    ) -> Result<(), CatalogError> {
        // A bucket with only immutable L0 records cannot have changed since it
        // was folded (seal lemma). Skip it with no GET; only a late compaction
        // record, rewrite record, or retention tombstone triggers real
        // reconcile work. In a held-open hour (`held_open`, see
        // [`held_open_hours`]) the lemma does not hold yet, so a commit
        // record the snapshot does not hold triggers it too.
        if !bucket_needs_reconcile(listing)
            && !(held_open && lists_an_unfolded_commit(listing, |id| seen.contains_key(id)))
        {
            return Ok(());
        }
        // A tombstone in a sealed leaf-bearing part is settled once every
        // pass has listed, by `apply_retirements`.
        if retire.defer(shard, hour, listing) {
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
                coverage,
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
        let changed_hours: Vec<u32> = std::iter::once(hour)
            .chain(desired.iter().map(|e| e.ingest_hour_bucket))
            .collect();
        derive_carry_keys_before_change(carry_sources, tenant, signal, entries, &changed_hours);
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

    /// The targeted re-fold pass (issue #526): re-lists and reconciles the
    /// `refold_request` hours that survive `exclude` and are named by
    /// `entries`, oldest-first, capped at `frontier_reconcile_max_hours`.
    /// Both the advancing path and the held-open-only path call it.
    ///
    /// `exclude` drops the hours the caller already lists this fold (the
    /// fixed window and frontier band when advancing, the held-open range
    /// otherwise), so no bucket is listed twice. An hour no entry names
    /// cannot be blocking a delete on HEAD reachability, so it is dropped
    /// uncounted.
    ///
    /// Returns the number of hours re-listed.
    #[allow(clippy::too_many_arguments)]
    async fn run_targeted_refold_pass(
        &self,
        tenant: &TenantHash,
        signal: Signal,
        generations: &[ShardGeneration],
        refold_request: &RefoldRequest,
        exclude: impl Fn(u32) -> bool,
        accounting: &QueryAccounting,
        entries: &mut Vec<SnapshotEntry>,
        carry_sources: &[CarrySource],
        seen: &mut HashMap<EntryIdentity, (usize, bool)>,
        dirty_hours: &mut HashSet<u32>,
        retire: &mut RetireTracker,
        counters: &mut RequestCounters,
        layout_drift_count: &mut u64,
        coverage: &mut declared_stats::StampCoverage,
    ) -> Result<u64, CatalogError> {
        if refold_request.is_empty() {
            return Ok(0);
        }
        let named_hours: HashSet<u32> = entries.iter().map(|e| e.ingest_hour_bucket).collect();
        // `RefoldRequest` iterates ascending, so this is oldest-first without
        // a sort: the hours whose held inputs have occupied storage longest
        // are reconciled before the cap is spent.
        let candidates: Vec<u32> = refold_request
            .hours()
            .filter(|hour| !exclude(*hour) && named_hours.contains(hour))
            .collect();
        let cap = self.config().frontier_reconcile_max_hours as usize;
        let take = candidates.len().min(cap);
        if take < candidates.len() {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                requested = candidates.len(),
                reconciled = take,
                "targeted re-fold request exceeds the per-fold cap; the remainder is \
                 carried to whichever later fold the requester asks for it again"
            );
        }
        let refold_hours = &candidates[..take];
        if refold_hours.is_empty() {
            return Ok(0);
        }
        let refold_buckets = hour_set_buckets(generations, refold_hours);
        let refold_listings = self
            .discover_bucket_listings(tenant, signal, &refold_buckets)
            .await?;
        counters.list_requests += refold_buckets.len() as u64;
        for (shard, hour, listing) in &refold_listings {
            self.reconcile_one_bucket(
                tenant,
                signal,
                *shard,
                *hour,
                listing,
                accounting,
                entries,
                carry_sources,
                seen,
                dirty_hours,
                retire,
                counters,
                layout_drift_count,
                coverage,
                false,
            )
            .await?;
        }
        Ok(refold_hours.len() as u64)
    }

    /// Classify one `(shard, hour)` commit bucket's listing into the entries
    /// it should contribute to the snapshot, the single shared classifier for both the incremental fold loop and
    /// the reconcile pass (ADR-0063 section 4). Partitioning the bucket's keys
    /// by shape mirrors the resolve-time listing (`Catalog::process_bucket`,
    /// docs/catalog-and-mvcc.md step 2) so a fold and a live resolve derive
    /// identical bucket state.
    ///
    /// A retention tombstone makes the bucket contribute nothing: neither its
    /// L1 parts nor its L0 records are folded in (ADR-0019 section 3). Each
    /// compaction record contributes its parts as level-1 entries and
    /// supersedes its named L0 inputs; two records with DISJOINT input sets in
    /// one bucket both contribute (a harmless overlap the resolver alarms on
    /// but still serves), while two whose input sets OVERLAP resolve to one
    /// authoritative record through
    /// [`crate::catalog::select_authoritative_compaction_records`] (issue
    /// #1070), the loser's parts skipped and its uncovered inputs folded in as
    /// raw L0. The same selector skips a record a present version 2 record
    /// supersedes, and a version 2 record a live rewrite dominates is dropped
    /// before it runs
    /// ([`crate::catalog::erasure_dominated_compaction_records`]). A
    /// selective-erasure rewrite record (ADR-0064
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
        coverage: &mut declared_stats::StampCoverage,
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
        let bucket_label = keys::commit_shard_hour_prefix(tenant, signal, shard, hour)?;
        if !rewrite_records.is_empty() {
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

        // Overlapping input sets in one bucket (issue #1070): pick one
        // authoritative record per overlap component through the SAME helpers
        // snapshot resolution uses, so a folded snapshot and a live resolve
        // never disagree on which record wins: a version 2 record a live
        // rewrite dominates is dropped first, and the selector excludes every
        // record a present version 2 record supersedes. An excluded record's
        // parts are skipped below and only AUTHORITATIVE records' inputs join
        // `excluded`, so a loser input no winner names is folded in as a raw
        // L0. Disjoint records are not in conflict and both contribute.
        let dominated = crate::catalog::erasure_dominated_compaction_records(
            &compaction_records,
            &rewrite_records,
            &bucket_label,
        )?;
        let candidates: Vec<(&str, &CompactionRecord)> = compaction_records
            .iter()
            .map(|(k, r)| (k.as_str(), r.as_ref()))
            .filter(|(k, _)| !dominated.contains(k))
            .collect();
        let selection = crate::catalog::select_authoritative_compaction_records(&candidates)?;
        for (ckey, record) in &candidates {
            if selection.is_excluded(ckey) {
                continue;
            }
            for input in &record.inputs {
                excluded.insert((
                    input.writer_id.clone(),
                    input.writer_epoch,
                    input.writer_seq,
                ));
            }
        }

        // Sibling-rewrite alarm, raised through the same helper snapshot
        // resolution uses. The fold is the path that matters most here: a
        // rewrite always lands in an already-sealed bucket (ADR-0064 §3.1),
        // and once that hour is folded, a query is served from the snapshot
        // without ever calling `process_bucket`, so this is the only place the
        // conflict would ever be observed for the ordinary case.
        self.check_rewrite_siblings(shard, hour, &rewrite_records, &superseded_records);

        // Compaction parts: contribute each non-superseded, authoritative
        // record's parts as level-1 entries. Records whose input sets overlap
        // resolve to one winner per overlap component, exactly as the live
        // resolver picks it; a loser's parts are skipped and any input only
        // the loser names stays a raw level-0 entry.
        for (ckey, record) in &compaction_records {
            if superseded_records.contains(ckey.as_str())
                || dominated.contains(ckey.as_str())
                || selection.is_excluded(ckey)
            {
                continue;
            }
            for part in &record.parts {
                let entry = build_l1_snapshot_entry(ckey, record, part, coverage)?;
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
            let entry = build_snapshot_entry(key, &record, coverage)?;
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
///
/// Level-aware key and identity derivation, matching
/// `build_segment_ref_from_entry` (`snapshot_resolve.rs`) exactly: an L0
/// entry's `writer_id`/`writer_epoch`/`writer_seq` are the flush's real
/// writer identity, addressed with `keys::data_key`. An L1 entry has no
/// writer identity of its own; the fold packs its `input_set_hash` into
/// `writer_id` and its `part_index` into `writer_epoch`
/// (`build_l1_snapshot_entry`), addressed with `keys::l1_part_key`, and its
/// footer carries the nil/0 sentinel identity `ravel-query`'s fetcher checks
/// L1 segments against (`expected_identity` in `ravel-query/src/fetcher.rs`).
pub async fn fetch_segment_names(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    entry: &SnapshotEntry,
) -> Result<HashSet<String>, PostingsBuildError> {
    let (_, data, location) = fetch_segment_object(store, tenant, signal, entry).await?;
    segment_names_from_object(&location, &data)
}

/// One entry's segment, fetched whole with one GET, its footer opened and
/// its identity checked, with the data object key it was read from: the
/// fetch half of [`fetch_segment_names`].
async fn fetch_segment_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    entry: &SnapshotEntry,
) -> Result<(String, Bytes, ravel_segment::FooterLocation), PostingsBuildError> {
    let content_hash: [u8; 32] = entry
        .content_hash
        .as_slice()
        .try_into()
        .map_err(|_| PostingsBuildError::BadContentHashLen(entry.content_hash.len()))?;

    let (data_key, expected) = if entry.level == 0 {
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
        let expected = ExpectedIdentity {
            tenant_hash: tenant.0,
            shard: entry.shard,
            writer_id: writer_id.to_string(),
            writer_epoch: entry.writer_epoch,
            writer_seq: entry.writer_seq,
        };
        (data_key, expected)
    } else {
        let input_set_hash: [u8; 32] = entry
            .writer_id
            .as_slice()
            .try_into()
            .map_err(|_| PostingsBuildError::BadInputSetHashLen(entry.writer_id.len()))?;
        let part_index = u32::try_from(entry.writer_epoch)
            .map_err(|_| PostingsBuildError::BadPartIndex(entry.writer_epoch))?;
        let data_key = keys::l1_part_key(
            tenant,
            signal,
            entry.shard,
            entry.ingest_hour_bucket,
            &hex::encode(&input_set_hash[..8]),
            part_index,
            &hex::encode(&content_hash[..8]),
        )?;
        let expected = ExpectedIdentity {
            tenant_hash: tenant.0,
            shard: entry.shard,
            writer_id: Uuid::nil().to_string(),
            writer_epoch: 0,
            writer_seq: 0,
        };
        (data_key, expected)
    };
    let got = store.get(&data_key, GetRange::Full).await?;

    let location = ravel_segment::open_from_full(&got.data, ReaderLimits::default())?;
    ravel_segment::check_identity(&location.footer, &expected)?;
    Ok((data_key, got.data, location))
}

/// The distinct `__name__` values of a segment held whole: the decode half
/// of [`fetch_segment_names`].
fn segment_names_from_object(
    location: &ravel_segment::FooterLocation,
    data: &[u8],
) -> Result<HashSet<String>, PostingsBuildError> {
    let limits = ReaderLimits::default();
    // ADR-0027: v7 is the only supported version (`open_from_full` has
    // already rejected anything else, including the retired v6). The chunked
    // v5-shaped catalog (unchanged by the v6 EXEMPLARS addition and the v7
    // per-sample provenance extension, which the postings build ignores) spans
    // sections, so it is decoded over the whole object -- already in hand here
    // via the `GetRange::Full` GET -- and folded to the per-series
    // `SeriesEntry` view the postings build consumes.
    let series: Vec<ravel_segment::SeriesEntry> = match location.version {
        ravel_segment::VERSION_V7 => {
            ravel_segment::decode_catalog_v5(&location.footer, data, limits)?
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

fn no_op_report(
    watermark_hour: Option<u32>,
    seal_through_hour: Option<u32>,
    counters: RequestCounters,
    refold_hours_reconciled: u64,
    key_index_switch_off_over_leaves: bool,
) -> FoldReport {
    // The work an attempt lost to a HEAD CAS did is real work even when the
    // fold ends in a no-op, so it reaches `/metrics` too.
    observe_fold_key_index(&counters);
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
        column_stats_part_objects_built: 0,
        column_stats_dictionaries_dropped: 0,
        layout_drift_count: 0,
        frontier_hours_reconciled: 0,
        frontier_hours_deferred: 0,
        // Usually `0` (a no-op fold normally never reaches a reconcile
        // branch at all), but the held-open-only path runs the targeted
        // re-fold pass BEFORE deciding `no_op`, so the caller can tell a
        // fully-reconciled request apart from one this fold never looked at.
        refold_hours_reconciled: refold_hours_reconciled as usize,
        // A no-op fold reads no commit record and writes no entry, so it has
        // no coverage to report and contributes nothing to the process-global
        // totals either.
        stamped_records: 0,
        stamped_entries: 0,
        seal_through_hour,
        // The read and build figures are nonzero only when an earlier attempt
        // of this fold lost its HEAD CAS after building leaves; a no-op
        // publishes no leaf.
        key_index_ranged_gets: counters.key_index_ranged_gets,
        key_index_bytes_read: counters.key_index_bytes_read,
        key_index_leaves_written: 0,
        key_index_leaf_bytes_written: 0,
        key_index_uncovered_entries: 0,
        key_index_sections_read: counters.key_index_sections_read,
        key_index_entries_carried: counters.key_index_entries_carried,
        key_index_build_passes: counters.key_index_build_passes,
        key_index_pair_budget: 0,
        key_index_field_pairs: BTreeMap::new(),
        key_index_switch_off_over_leaves,
        key_index_parts_missing_declared_leaves: 0,
        key_index_leaf_gets: counters.key_index_leaf_gets,
        key_index_carry_peak_held_bytes: counters.key_index_carry_peak_held_bytes,
        metrics_label_index_parts_over_cap: 0,
        metrics_label_index_parts_erasure_gated: 0,
        metrics_label_index_erasure_list_requests: counters
            .metrics_label_index_erasure_list_requests,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
pub(crate) mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use prost::Message;
    use ravel_commit::erasure;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault, Sequence,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{InstrumentedStore, ObjectStoreBackend, PutOptions};
    use ravel_proto::commit::v1::{CompactionInputIdentity, RetentionTombstone, RewriteDrop};
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};
    use tracing_subscriber::layer::SubscriberExt;

    use super::*;
    use crate::config::{
        DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
        DEFAULT_MAX_FLUSH_LIFETIME_NS,
    };
    use crate::snapshot_format::{
        HEAD_FORMAT_VERSION_2, check_key_index_leaf_set, head_format_version_for,
    };

    const DEFAULT_MARGIN_NS: i64 = DEFAULT_MAX_FLUSH_LIFETIME_NS
        + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
        + DEFAULT_FOLD_SAFETY_MARGIN_NS;

    pub(crate) fn tenant() -> TenantHash {
        TenantHash([0xcd; 16])
    }

    pub(crate) fn config(shard_count: u32) -> CatalogConfig {
        CatalogConfig {
            shard_count,
            ..Default::default()
        }
    }

    /// `now_ns` at which ingest hour `hour` has just sealed under default
    /// margins: the exact boundary of `sealed_watermark_hour`.
    pub(crate) fn now_at_seal(hour: u32) -> i64 {
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

    fn stamp_stat(
        name: &str,
        min: i64,
        max: i64,
        null_count: u64,
    ) -> ravel_types::declared_stats::DeclaredColumnStat {
        ravel_types::declared_stats::DeclaredColumnStat::new(
            name,
            ravel_types::declared_stats::DeclaredStatType::I64,
            Some(ravel_types::declared_stats::DeclaredStatValue::I64(min)),
            Some(ravel_types::declared_stats::DeclaredStatValue::I64(max)),
            null_count,
        )
        .expect("valid i64 stat")
    }

    /// `publish_segment` with an ADR-0873 declared-column stamp: the published
    /// record carries `stats` in `declared_column_stats` (field 20) exactly as
    /// the writer that computed them would have written it.
    async fn publish_stamped_segment(
        store: &MemoryStore,
        seq: u64,
        ingest_hour_bucket: u32,
        created_unix_ns: i64,
        stats: &[ravel_types::declared_stats::DeclaredColumnStat],
    ) -> CommitRecord {
        let payload = format!("stamped-seg-{seq}").into_bytes();
        let content_hash = *blake3::hash(&payload).as_bytes();
        let mut record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id: Uuid::from_u128(u128::from(seq) + 1),
            writer_epoch: 1,
            writer_seq: seq,
            object_size: payload.len() as u64,
            content_hash,
            sample_count: 8,
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
        ravel_commit::declared_stats::stamp_commit_record(&mut record, stats);
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

    /// A MemoryStore wrapper that records the key of every whole-object `get`,
    /// so a test can assert exactly which objects a fold read in full. The
    /// key-index leaf build's ranged reads are not recorded; they are pinned by
    /// `leaf_build_issues_two_ranged_gets_per_entry_and_reads_exactly_the_sections`.
    struct RecordingStore {
        inner: MemoryStore,
        get_keys: std::sync::Mutex<Vec<String>>,
    }

    impl RecordingStore {
        fn new() -> Self {
            RecordingStore {
                inner: MemoryStore::new(),
                get_keys: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn clear_gets(&self) {
            self.get_keys.lock().expect("lock").clear();
        }

        fn count_gets_of(&self, key: &str) -> usize {
            self.get_keys
                .lock()
                .expect("lock")
                .iter()
                .filter(|k| k.as_str() == key)
                .count()
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for RecordingStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            if range == GetRange::Full {
                self.get_keys.lock().expect("lock").push(key.to_string());
            }
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// Publish one real L0 RLOG logs segment carrying an I64 `status` attribute
    /// on every row, plus its commit record, exactly as ingest would. Returns
    /// the record so the caller can reconstruct its data-object key.
    async fn publish_logs_segment(
        store: &dyn ObjectStoreBackend,
        writer_seq: u64,
        ingest_hour_bucket: u32,
        statuses: &[i64],
    ) -> CommitRecord {
        use ravel_logseg::writer::ObjectIdentity;
        use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
        use ravel_types::logstream::{AttrValue, log_stream_id};

        let writer_id = Uuid::from_u128(u128::from(writer_seq));
        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        )];
        let mut w = RlogWriter::new(
            RlogConfig::default(),
            ObjectIdentity {
                tenant_hash: tenant().0,
                shard: 0,
                writer_id: *writer_id.as_bytes(),
                writer_epoch: 1,
                writer_seq,
            },
        );
        let base_ts = i64::from(ingest_hour_bucket) * NS_PER_HOUR + 60_000_000_000;
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for (i, status) in statuses.iter().enumerate() {
            let ts = base_ts + i as i64;
            min_ts = min_ts.min(ts);
            max_ts = max_ts.max(ts);
            w.push(LogRecord {
                stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("row {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![("status".to_string(), AttrValue::I64(*status))],
            })
            .expect("push");
        }
        let bytes = w.finish().expect("finish");
        let content_hash = *blake3::hash(&bytes).as_bytes();
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: statuses.len() as u64,
            series_count: 1,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
            min_ingest_ts_ns: min_ts,
            max_ingest_ts_ns: max_ts,
            segment_format_version: 1,
            created_unix_ns: max_ts,
            ingest_hour_bucket,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store, &data_key, Bytes::from(bytes))
            .await
            .expect("put data object");
        publish::publish(store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        record
    }

    /// ADR-0873 stamp coverage, the acceptance test for both halves of the
    /// pair and for the shortfall between them.
    ///
    /// A fold over a known mix of stamped and unstamped commit records reports
    /// exactly how many stamped records it read and exactly how many stamped
    /// entries it wrote from them. The second half folds the same stamped
    /// records with the test-only strip hook installed, which is the only way
    /// to produce the state a mixed-version fleet produces in the field: new
    /// records carrying stamps, a fold of an older shape not carrying them
    /// through. The counts must then diverge, entries below records, which is
    /// what the alert in docs/guides/observability.md fires on.
    #[tokio::test]
    async fn fold_report_counts_stamped_records_and_stamped_entries_and_flags_shortfall() {
        const HOUR: u32 = 10;
        let created = i64::from(HOUR) * NS_PER_HOUR + 60_000_000_000;
        let stats = vec![stamp_stat("EventDate", -5, 19_000, 2)];

        let records_before = crate::declared_stats::fold_stamped_records_total();
        let entries_before = crate::declared_stats::fold_stamped_entries_total();

        // Three stamped records and two unstamped ones, one hour, one shard.
        let store = Arc::new(MemoryStore::new());
        for seq in 1..=3u64 {
            publish_stamped_segment(&store, seq, HOUR, created, &stats).await;
        }
        for seq in 4..=5u64 {
            publish_segment(
                &store,
                0,
                Uuid::from_u128(u128::from(seq) + 1),
                seq,
                HOUR,
                created,
            )
            .await;
        }
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let report = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(HOUR),
                &[],
                None,
            )
            .await
            .expect("fold");

        assert_eq!(
            report.entry_count, 5,
            "all five records fold in; only three of them are stamped"
        );
        assert_eq!(
            report.stamped_records, 3,
            "exactly the three records published with a stamp are counted as read with one"
        );
        assert_eq!(
            report.stamped_entries, 3,
            "each of those three carried its stamp onto its snapshot entry"
        );

        // The shortfall. Same three stamped records, a fold whose carriage does
        // not survive.
        let store = Arc::new(MemoryStore::new());
        for seq in 1..=3u64 {
            publish_stamped_segment(&store, seq, HOUR, created, &stats).await;
        }
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let stripped = {
            let _strip = crate::declared_stats::StripCarriedStampsGuard::install();
            catalog
                .fold(
                    &tenant(),
                    Signal::Metrics,
                    Uuid::new_v4(),
                    now_at_seal(HOUR),
                    &[],
                    None,
                )
                .await
                .expect("fold")
        };

        assert_eq!(
            stripped.stamped_records, 3,
            "the records are still read with their stamps on the wire"
        );
        assert_eq!(
            stripped.stamped_entries, 0,
            "none of the stamps reached an entry, which is the shortfall itself"
        );
        assert!(
            stripped.stamped_entries < stripped.stamped_records,
            "entries below records is the state the coverage alert fires on"
        );

        // The two per-fold reports also reach the process-global totals the
        // /metrics families render. A lower bound, not an equality: the totals
        // are process-global and other fold tests in this binary add to them
        // concurrently, so only this test's own contribution is pinned.
        assert!(
            crate::declared_stats::fold_stamped_records_total() >= records_before + 6,
            "both folds' six stamped records must reach the process-global total"
        );
        assert!(
            crate::declared_stats::fold_stamped_entries_total() >= entries_before + 3,
            "only the first fold's three stamped entries may reach the process-global total"
        );
    }

    /// The coverage pair counts the compaction-part carriage path, not only the
    /// L0 one.
    ///
    /// A fold whose entries come entirely from compaction parts reports exactly
    /// the stamped parts it read and exactly the stamped entries it built from
    /// them, and both reach the process-global totals the /metrics families
    /// render. Without the tally at `carry_compaction_part`, this fold reports
    /// zero on both halves while writing two stamped L1 entries, which is the
    /// state that makes the rendered entries counter mean less than it says.
    #[tokio::test]
    async fn fold_counts_stamp_coverage_carried_from_compaction_parts() {
        const HOUR: u32 = 10;

        let records_before = crate::declared_stats::fold_stamped_records_total();
        let entries_before = crate::declared_stats::fold_stamped_entries_total();

        // Three L1 parts of one compaction record; the first two are stamped.
        let store = Arc::new(MemoryStore::new());
        publish_logs_l1(
            store.as_ref(),
            0,
            HOUR,
            "stamped-compaction-parts",
            &[
                (&[200, 404, 200, 500], true),
                (&[500, 500, 200], true),
                (&[200, 200], false),
            ],
        )
        .await;

        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let report = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(HOUR),
                &[],
                None,
            )
            .await
            .expect("fold");

        assert_eq!(
            report.entry_count, 3,
            "one L1 entry per part, and no L0 record in this fixture"
        );
        assert_eq!(
            report.stamped_records, 2,
            "exactly the two parts published with a stamp are counted as read with one"
        );
        assert_eq!(
            report.stamped_entries, 2,
            "each of those two carried its stamp onto its L1 snapshot entry"
        );

        // A lower bound on the process-global totals, as above: other fold
        // tests in this binary add to them concurrently, so only this fold's
        // own contribution is pinned.
        assert!(
            crate::declared_stats::fold_stamped_records_total() >= records_before + 2,
            "the fold's two stamped parts must reach the process-global total"
        );
        assert!(
            crate::declared_stats::fold_stamped_entries_total() >= entries_before + 2,
            "the fold's two stamped L1 entries must reach the process-global total"
        );
    }

    /// Issue #850, Finding 3: an incremental fold that appends entries must
    /// REUSE the previous fold's column-statistics baseline for the segments it
    /// already covered, re-fetching only the genuinely new segment. The reuse
    /// binding is checked against the parts the baseline was actually built with
    /// (the previous HEAD's parts), not the parts this fold just encoded.
    ///
    /// Asserted by a count of OBJECTS READ: after the second fold, the two
    /// first-fold segment data objects are never GET again (reuse), while the
    /// new segment is. Against the pre-fix code (`expected_part_blake3 =
    /// part_hashes`), the append changes the tail part's hash, the baseline is
    /// rejected, and both old segments are re-fetched, so `count_gets_of` for
    /// each old data key is 1 and this test fails.
    #[tokio::test]
    async fn incremental_fold_reuses_column_stats_baseline() {
        let store = Arc::new(RecordingStore::new());

        // Declare `status` as an I64 typed logs column so the fold builds
        // column statistics.
        let cfg = crate::tenant_config::TenantConfig {
            typed_attr_columns: Some(vec![crate::tenant_config::DeclaredTypedColumn {
                key: "status".to_string(),
                ty: crate::tenant_config::DeclaredColumnType::I64,
            }]),
            ..crate::tenant_config::TenantConfig::new(
                crate::tenant_config::TenantLifecycleState::Active,
            )
        };
        crate::tenant_config::set_tenant_config(store.as_ref(), &tenant(), &cfg, 1)
            .await
            .expect("write tenant config");

        // Two segments in hour 10.
        let rec_a = publish_logs_segment(store.as_ref(), 1, 10, &[200, 404, 200]).await;
        let rec_b = publish_logs_segment(store.as_ref(), 2, 10, &[500, 200]).await;
        let key_a = keys::reconstruct_data_key(&rec_a).expect("key a");
        let key_b = keys::reconstruct_data_key(&rec_b).expect("key b");

        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let first = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("first fold");
        assert!(first.rebuilt, "first fold rebuilds from the commit layout");
        assert!(
            first.column_stats_part_objects_built > 0,
            "the first fold builds per-part column statistics over the hour-10 segments"
        );

        // A new segment in a later hour, then an incremental second fold.
        let rec_c = publish_logs_segment(store.as_ref(), 3, 11, &[200, 200]).await;
        let key_c = keys::reconstruct_data_key(&rec_c).expect("key c");

        store.clear_gets();
        let second = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(11),
                &[],
                None,
            )
            .await
            .expect("second fold");
        assert!(!second.rebuilt, "the second fold is incremental");
        assert!(
            second.column_stats_part_objects_built > 0,
            "the second fold builds a per-part statistics object for the new part"
        );

        // The reuse assertion, by objects read. Segment B is the clean
        // discriminator: on the second fold it is fetched ONLY if the
        // column-stats baseline was rejected and B recomputed. With reuse it is
        // never read; with the pre-fix `part_hashes` binding it is read once.
        //
        // Segment A is read exactly once, by the name-postings pass, which
        // aborts on the first L0 entry because a logs RLOG object is not a
        // metrics RSEG. The v3 (content-hash-keyed, per part) baseline
        // covers it without a second read.
        assert_eq!(
            store.count_gets_of(&key_a),
            1,
            "only the postings pass reads A; the v3 publish reuses its baseline record"
        );
        assert_eq!(
            store.count_gets_of(&key_b),
            0,
            "segment B's stats were reused, not recomputed"
        );
        // Issue #964: the one genuinely new segment is read ONCE.
        assert_eq!(
            store.count_gets_of(&key_c),
            1,
            "the new hour-11 segment is fetched once"
        );

        // Reuse still produced a complete artifact covering all three
        // segments. Since ADR-1413 the per-part v3 object answers this load
        // and its records are content-hash keyed, so they land in
        // `by_content_hash`, not the always-empty `segments` map.
        store.clear_gets();
        let acc = QueryAccounting::new();
        let loaded = catalog
            .load_column_stats(
                &tenant(),
                Signal::Logs,
                ravel_types::TimeRange {
                    start_ns: 0,
                    end_ns: 50 * NS_PER_HOUR,
                },
                50 * NS_PER_HOUR,
                &crate::ColumnStatsRequest::summary_only(),
                &acc,
            )
            .await
            .expect("load ok")
            .expect("stats present");
        assert_eq!(
            loaded.content_hash_segments().count(),
            3,
            "the reused baseline plus the new segment cover all three"
        );
    }

    /// Captures WARN and ERROR events with their message and fields flattened
    /// to one line each, so a test can assert which level a log line landed
    /// at without depending on `tracing`'s own formatting.
    #[derive(Clone, Default)]
    struct LevelCapture {
        lines: Arc<std::sync::Mutex<Vec<(tracing::Level, String)>>>,
    }

    impl LevelCapture {
        fn lines(&self) -> Vec<(tracing::Level, String)> {
            self.lines.lock().expect("lock").clone()
        }

        fn count_at(&self, level: tracing::Level, needle: &str) -> usize {
            self.lines
                .lock()
                .expect("lock")
                .iter()
                .filter(|(l, line)| *l == level && line.contains(needle))
                .count()
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LevelCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let level = *event.metadata().level();
            if level != tracing::Level::WARN && level != tracing::Level::ERROR {
                return;
            }
            let mut visitor = LevelCaptureVisitor::default();
            event.record(&mut visitor);
            self.lines
                .lock()
                .expect("lock")
                .push((level, visitor.finish()));
        }
    }

    #[derive(Default)]
    struct LevelCaptureVisitor {
        message: String,
        fields: Vec<String>,
    }

    impl LevelCaptureVisitor {
        fn finish(self) -> String {
            let mut line = self.message;
            for field in self.fields {
                line.push(' ');
                line.push_str(&field);
            }
            line
        }
    }

    impl tracing::field::Visit for LevelCaptureVisitor {
        fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
            self.fields.push(format!("{}={}", field.name(), value));
        }
        fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
            self.fields.push(format!("{}={}", field.name(), value));
        }
        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "message" {
                self.message = value.to_string();
            } else {
                self.fields.push(format!("{}={}", field.name(), value));
            }
        }
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.message = format!("{value:?}");
            } else {
                self.fields.push(format!("{}={:?}", field.name(), value));
            }
        }
    }

    /// Issue #1964: a GET failure on the previous fold's per-part
    /// column-stats object (`.cstat`), other than `NotFound`, must surface at
    /// `error!`, naming the key, rather than the quiet `warn!` a genuine
    /// absence gets. Either way the fold still falls back to rebuilding the
    /// part's statistics from scratch: the reuse baseline is an optimization,
    /// never a correctness gate. `ScriptedFault` has no `AccessDenied` kind (a
    /// `ravel-object-store` testing gap, out of scope here), so `Permanent`
    /// stands in as a structurally equivalent non-`NotFound` GET failure: the
    /// fixed code branches on `Err(StoreError::NotFound)` versus every other
    /// `Err`, not on which variant the "other" arm carries.
    #[tokio::test]
    async fn cstat_baseline_permanent_failure_logs_at_error() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("simulated AccessDenied on idx/*".into()),
            )
            .with_key_contains(".cstat"),
        );
        let store = Arc::new(FaultStore::new(inner, plan));
        set_status_column_config(store.inner()).await;

        publish_logs_segment(store.inner(), 1, 10, &[200, 404, 200]).await;
        publish_logs_segment(store.inner(), 2, 10, &[500, 200]).await;

        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let first = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("first fold");
        assert!(
            first.column_stats_part_objects_built > 0,
            "the first fold must build a column-stats baseline for the second fold to reuse"
        );

        publish_logs_segment(store.inner(), 3, 11, &[200, 200]).await;

        let capture = LevelCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _default = tracing::subscriber::set_default(subscriber);

        let second = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(11),
                &[],
                None,
            )
            .await
            .expect("second fold succeeds despite the faulted baseline read");
        assert!(!second.rebuilt);

        assert!(
            store.fault_count(Op::Get, FaultKind::Permanent) >= 1,
            "the injected fault must actually have fired"
        );
        assert!(
            capture.count_at(
                tracing::Level::ERROR,
                "previous per-part column-stats object GET failed"
            ) >= 1,
            "a non-NotFound GET failure on the .cstat baseline must log at error!, not warn!: {:?}",
            capture.lines()
        );
    }

    /// Counterpart to `cstat_baseline_permanent_failure_logs_at_error`: a
    /// genuine `NotFound` on the `.cstat` baseline (modeled with
    /// `NotFoundBlip`, an eventual-consistency blip rather than a permission
    /// fault) must still degrade quietly to a `warn!`, exactly as before this
    /// fix, never an `error!`.
    #[tokio::test]
    async fn cstat_baseline_not_found_blip_still_warns_quietly() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_key_contains(".cstat"));
        let store = Arc::new(FaultStore::new(inner, plan));
        set_status_column_config(store.inner()).await;

        publish_logs_segment(store.inner(), 1, 10, &[200, 404, 200]).await;
        publish_logs_segment(store.inner(), 2, 10, &[500, 200]).await;

        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let first = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("first fold");
        assert!(first.column_stats_part_objects_built > 0);

        publish_logs_segment(store.inner(), 3, 11, &[200, 200]).await;

        let capture = LevelCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _default = tracing::subscriber::set_default(subscriber);

        let second = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(11),
                &[],
                None,
            )
            .await
            .expect("second fold succeeds");
        assert!(!second.rebuilt);

        assert!(
            store.fault_count(Op::Get, FaultKind::NotFoundBlip) >= 1,
            "the injected fault must actually have fired"
        );
        assert!(
            capture.count_at(
                tracing::Level::WARN,
                "previous per-part column-stats object not found"
            ) >= 1,
            "a NotFound on the .cstat baseline must still log at warn!: {:?}",
            capture.lines()
        );
        assert_eq!(
            capture.count_at(
                tracing::Level::ERROR,
                "previous per-part column-stats object GET failed"
            ),
            0,
            "a genuine absence must never log at error!"
        );
    }

    /// Issue #1964: a GET failure on the previous fold's postings object
    /// (`.npost`), other than `NotFound`, must surface at `error!`, naming
    /// the key, rather than the quiet `warn!` a genuine absence gets. Either
    /// way the fold still falls back to rebuilding postings from scratch:
    /// reuse is an optimization, never a correctness gate. Uses real metrics
    /// segments (`publish_real_segment`), not logs: a logs RLOG object never
    /// decodes as an RSEG, so `build_postings` returns `None` before a
    /// `.npost` object is ever written, and the reuse path this test targets
    /// would never be reached.
    #[tokio::test]
    async fn npost_baseline_permanent_failure_logs_at_error() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("simulated AccessDenied on idx/*".into()),
            )
            .with_key_contains(".npost"),
        );
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let now_1 = now_at_seal(10);
        publish_real_segment(
            store.inner(),
            0,
            Uuid::new_v4(),
            1,
            10,
            now_1 - NS_PER_HOUR,
            &["cpu", "mem"],
        )
        .await;
        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
            .await
            .expect("first fold");
        assert!(
            first.postings_built,
            "the first fold must build a postings ref for the second fold's reuse baseline to exist"
        );

        let now_2 = now_at_seal(12);
        publish_real_segment(
            store.inner(),
            0,
            Uuid::new_v4(),
            2,
            12,
            now_2 - NS_PER_HOUR,
            &["disk"],
        )
        .await;

        let capture = LevelCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _default = tracing::subscriber::set_default(subscriber);

        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
            .await
            .expect("second fold succeeds despite the faulted baseline read");
        assert!(!second.rebuilt);

        assert!(
            store.fault_count(Op::Get, FaultKind::Permanent) >= 1,
            "the injected fault must actually have fired"
        );
        assert!(
            capture.count_at(tracing::Level::ERROR, "previous postings GET failed") >= 1,
            "a non-NotFound GET failure on the .npost baseline must log at error!, not warn!: {:?}",
            capture.lines()
        );
    }

    /// Counterpart to `npost_baseline_permanent_failure_logs_at_error`: a
    /// genuine `NotFound` on the `.npost` baseline (modeled with
    /// `NotFoundBlip`) must still degrade quietly to a `warn!`, exactly as
    /// before this fix, never an `error!`.
    #[tokio::test]
    async fn npost_baseline_not_found_blip_still_warns_quietly() {
        let inner = MemoryStore::new();
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_key_contains(".npost"));
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let now_1 = now_at_seal(10);
        publish_real_segment(
            store.inner(),
            0,
            Uuid::new_v4(),
            1,
            10,
            now_1 - NS_PER_HOUR,
            &["cpu", "mem"],
        )
        .await;
        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
            .await
            .expect("first fold");
        assert!(first.postings_built);

        let now_2 = now_at_seal(12);
        publish_real_segment(
            store.inner(),
            0,
            Uuid::new_v4(),
            2,
            12,
            now_2 - NS_PER_HOUR,
            &["disk"],
        )
        .await;

        let capture = LevelCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _default = tracing::subscriber::set_default(subscriber);

        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
            .await
            .expect("second fold succeeds");
        assert!(!second.rebuilt);

        assert!(
            store.fault_count(Op::Get, FaultKind::NotFoundBlip) >= 1,
            "the injected fault must actually have fired"
        );
        assert!(
            capture.count_at(tracing::Level::WARN, "previous postings not found") >= 1,
            "a NotFound on the .npost baseline must still log at warn!: {:?}",
            capture.lines()
        );
        assert_eq!(
            capture.count_at(tracing::Level::ERROR, "previous postings GET failed"),
            0,
            "a genuine absence must never log at error!"
        );
    }

    /// One fold over hour 10, then the HEAD and the `.npost` object it wrote,
    /// for the reuse-path tests below. Returns the catalog, the HEAD, and the
    /// postings object's bytes.
    async fn folded_postings_baseline(store: &Arc<MemoryStore>) -> (Catalog, SnapshotHead, Bytes) {
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now_1 = now_at_seal(10);
        publish_real_segment(
            store,
            0,
            Uuid::new_v4(),
            1,
            10,
            now_1 - NS_PER_HOUR,
            &["cpu", "mem"],
        )
        .await;
        let first = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
            .await
            .expect("first fold");
        assert!(
            first.postings_built,
            "the second fold's reuse baseline is what these tests exercise"
        );

        let head_bytes = store
            .get(&head_object_key(&tenant(), Signal::Metrics), GetRange::Full)
            .await
            .expect("HEAD present")
            .data;
        let head = snapshot_format::decode_head(&head_bytes).expect("HEAD decodes");
        let postings_key = head
            .postings
            .as_ref()
            .expect("the first fold wrote a postings ref")
            .key
            .clone();
        let postings = store
            .get(&postings_key, GetRange::Full)
            .await
            .expect("postings present")
            .data;
        (catalog, head, postings)
    }

    /// Rewrite the postings object `head` already points at, re-stamping the
    /// ref's blake3 and size, and PUT both. The blake3 gate runs before every
    /// header check, so this is what lets a reader reach the header checks at
    /// all.
    async fn repoint_head_at_postings(
        store: &Arc<MemoryStore>,
        head: &mut SnapshotHead,
        postings: Vec<u8>,
    ) {
        let key = head
            .postings
            .as_ref()
            .expect("a postings ref to re-point")
            .key
            .clone();
        let postings_ref = head.postings.as_mut().expect("a postings ref to re-point");
        postings_ref.blake3 = blake3::hash(&postings).as_bytes().to_vec();
        postings_ref.size = postings.len() as u64;
        store
            .put(&key, Bytes::from(postings), PutOptions::default())
            .await
            .expect("put the rewritten postings");
        let head_bytes = snapshot_format::encode_head(head).expect("encode head");
        store
            .put(
                &head_object_key(&tenant(), Signal::Metrics),
                Bytes::from(head_bytes),
                PutOptions::default(),
            )
            .await
            .expect("put the re-pointed HEAD");
    }

    /// Issue #2081 item 1, ADR-0050 §2: the fold's previous-postings reuse path
    /// checks the object's declared `tenant_hash` before it reuses it, exactly
    /// as `snapshot_resolve::load_snapshot_postings` does at query time, and a
    /// foreign one is an isolation breach that fails the fold rather than a
    /// quiet rebuild. The re-encoded object still binds to this HEAD's part
    /// hashes, so nothing but the tenant check can reject it.
    ///
    /// FLIP: drop the `tenant_hash` match in `load_previous_postings` and the
    /// second fold succeeds, so `expect_err` panics with "a previous postings
    /// object naming another tenant must fail the fold: FoldReport { ... }"
    /// and `isolation_breaches()` reads 0.
    #[tokio::test]
    async fn a_foreign_tenant_hash_on_the_previous_postings_fails_the_fold() {
        assert_foreign_previous_postings_fail_the_fold(false, true).await;
    }

    /// The tenant check runs before the decode reservation: under a budget
    /// with room for the previous part's charge and none at all for the
    /// postings the fold still reports the breach, not memory pressure.
    ///
    /// FLIP: move the `tenant_hash` match below the `reserve_decoded` call in
    /// `load_previous_postings` and the fold fails with `MemoryExhausted`
    /// instead.
    #[tokio::test]
    async fn a_foreign_tenant_hash_is_reported_before_the_postings_reservation() {
        assert_foreign_previous_postings_fail_the_fold(true, true).await;
    }

    /// The tenant check runs before the part-binding check: a foreign object
    /// that is also bound to other part hashes still fails the fold instead of
    /// degrading to a rebuild.
    ///
    /// FLIP: move the `tenant_hash` match below the `decode_postings` call in
    /// `load_previous_postings` and the fold rebuilds and succeeds, so
    /// `expect_err` panics.
    #[tokio::test]
    async fn a_foreign_tenant_hash_is_reported_before_the_part_binding_degrade() {
        assert_foreign_previous_postings_fail_the_fold(false, false).await;
    }

    /// Re-point the fixture HEAD at a copy of its postings naming another
    /// tenant, bound to the HEAD's own part hashes or to hashes it does not
    /// carry, then fold again and assert the fold fails with the
    /// `tenant_hash` mismatch and one counted breach. With `parts_only_budget`
    /// the fold runs under a budget of exactly the previous parts' declared
    /// lengths, which the part loads fill and the postings reservation finds
    /// no room in; otherwise the budget is unlimited.
    async fn assert_foreign_previous_postings_fail_the_fold(
        parts_only_budget: bool,
        bind_to_head_parts: bool,
    ) {
        let store = Arc::new(MemoryStore::new());
        let (catalog, mut head, postings) = folded_postings_baseline(&store).await;
        let catalog = if parts_only_budget {
            let mut parts_declared = 0;
            for part in &head.parts {
                parts_declared += part_declared_len(&store, &part.key).await;
            }
            catalog.with_memory_budget(Arc::new(ravel_memory::MemoryBudget::new(parts_declared)))
        } else {
            catalog
        };
        let part_blake3: Vec<[u8; 32]> = head
            .parts
            .iter()
            .map(|p| <[u8; 32]>::try_from(p.blake3.as_slice()).expect("32-byte part hash"))
            .collect();
        let decoded = snapshot_format::decode_postings(
            &postings,
            &snapshot_format::PostingsLimits::default(),
            &part_blake3,
        )
        .expect("the fixture postings decode");
        let bound_parts = if bind_to_head_parts {
            part_blake3.clone()
        } else {
            vec![[0xee; 32]; part_blake3.len()]
        };
        assert_eq!(
            bound_parts == part_blake3,
            bind_to_head_parts,
            "the fixture binds the foreign object as the case asks"
        );
        let foreign = snapshot_format::encode_postings(
            [0xff; 16],
            decoded.header.signal,
            &bound_parts,
            decoded.header.entry_count,
            &decoded.names,
        )
        .expect("re-encode the fixture postings for a foreign tenant");
        let postings_key = head.postings.as_ref().expect("a postings ref").key.clone();
        repoint_head_at_postings(&store, &mut head, foreign).await;

        let now_2 = now_at_seal(12);
        publish_real_segment(
            &store,
            0,
            Uuid::new_v4(),
            2,
            12,
            now_2 - NS_PER_HOUR,
            &["disk"],
        )
        .await;
        let err = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
            .await
            .expect_err("a previous postings object naming another tenant must fail the fold");
        match err {
            CatalogError::FieldMismatch { field, key, .. } => {
                assert_eq!(field, "tenant_hash");
                assert_eq!(key, postings_key);
            }
            other => panic!("expected a tenant_hash FieldMismatch, got {other:?}"),
        }
        assert_eq!(
            catalog.isolation_breaches(),
            1,
            "the breach is counted, not swallowed by a rebuild"
        );
    }

    /// Issue #2081 item 4: a refused decode reservation on the previous
    /// postings object fails the fold, the same answer
    /// `snapshot_resolve::load_snapshot_postings` gives the same refusal
    /// (pinned by `catalog::tests::postings_decode_reserves_its_output`).
    /// Rebuilding instead answers memory pressure by fetching and decoding
    /// every segment's names, which holds more memory than the one postings
    /// decode that was just refused.
    ///
    /// The previous snapshot part is charged too (issue #3320) and its
    /// reservation is still held when the postings reserve, since the fold
    /// still holds the part's entries; the budget leaves room for the part
    /// and one byte less than the postings on top of it, so the refusal
    /// names the part's charge as already reserved.
    ///
    /// FLIP: restore the `Err(err) => { warn!(...); return Ok(None) }` arm
    /// around `reserve_decoded` in `load_previous_postings` and the second fold
    /// succeeds, so `expect_err` panics. FLIP: drop `_previous_charge` from
    /// the tuple `load_previous_entries`'s result is bound to in
    /// `fold_with_refold_request` (bind it to `_` instead) and the part's
    /// reservation is released before the postings reserve, so the second
    /// fold succeeds and `expect_err` panics.
    #[tokio::test]
    async fn a_refused_postings_reservation_fails_the_fold() {
        let store = Arc::new(MemoryStore::new());
        let (catalog, head, postings) = folded_postings_baseline(&store).await;
        let declared = snapshot_format::decode_postings_header(&postings)
            .expect("postings header decodes")
            .body_uncompressed_len;
        assert!(declared > 0, "the fixture postings have a body to charge");
        assert!(
            declared <= config(1).max_postings_bytes,
            "the decoder would admit this body, so only the budget can refuse it"
        );
        assert_eq!(head.parts.len(), 1, "one previous part");
        let part_declared = part_declared_len(&store, &head.parts[0].key).await;
        drop(head);

        let budget = Arc::new(ravel_memory::MemoryBudget::new(
            part_declared + declared - 1,
        ));
        let catalog = catalog.with_memory_budget(budget.clone());
        let now_2 = now_at_seal(12);
        publish_real_segment(
            &store,
            0,
            Uuid::new_v4(),
            2,
            12,
            now_2 - NS_PER_HOUR,
            &["disk"],
        )
        .await;
        let err = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
            .await
            .expect_err("a refused postings reservation must fail the fold");
        match err {
            CatalogError::MemoryExhausted(exhausted) => {
                assert_eq!(exhausted.requested, declared);
                assert_eq!(exhausted.reserved, part_declared);
                assert_eq!(exhausted.limit, part_declared + declared - 1);
            }
            other => panic!("expected CatalogError::MemoryExhausted, got {other:?}"),
        }
        assert_eq!(
            budget.reserved(),
            0,
            "a refused reservation charges nothing"
        );
    }

    /// The declared `entries_uncompressed_len` of the snapshot part at `key`,
    /// what `Catalog::load_part_entries` reserves before decoding it. Asserts
    /// it is non-zero and within the decode ceiling, so only a budget can
    /// refuse it.
    pub(crate) async fn part_declared_len(store: &MemoryStore, key: &str) -> u64 {
        let part = store
            .get(key, GetRange::Full)
            .await
            .expect("part present")
            .data;
        let declared = snapshot_format::decode_part_header(&part)
            .expect("part header decodes")
            .entries_uncompressed_len;
        assert!(declared > 0, "the fixture part has a body to charge");
        assert!(
            declared <= PartLimits::default().max_snapshot_part_bytes,
            "the decoder would admit this body, so only the budget can refuse it"
        );
        assert!(
            declared != part.len() as u64,
            "the stored and declared lengths differ, so a charge of either is told apart"
        );
        declared
    }

    /// The metrics HEAD's stored bytes.
    async fn metrics_head_bytes(store: &MemoryStore) -> Bytes {
        store
            .get(&head_object_key(&tenant(), Signal::Metrics), GetRange::Full)
            .await
            .expect("head present")
            .data
    }

    const PREVIOUS_PART_WRITER: Uuid = Uuid::from_u128(0x3320_0001);
    const NEXT_PART_WRITER: Uuid = Uuid::from_u128(0x3320_0002);
    const FIRST_FOLDER: Uuid = Uuid::from_u128(0x3320_0011);
    const SECOND_FOLDER: Uuid = Uuid::from_u128(0x3320_0012);

    /// Fold one hour-10 segment into a one-part HEAD on `inner` through a
    /// catalog with no budget, then publish an hour-12 segment the next fold
    /// folds in incrementally from that part. Returns the part's declared
    /// entry body length. Fixed writer and folder ids, so two stores given
    /// the same calls hold byte-identical HEADs.
    async fn previous_part_baseline(inner: &Arc<MemoryStore>) -> u64 {
        let catalog = Catalog::new(inner.clone(), config(1)).expect("catalog");
        publish_segment(
            inner,
            0,
            PREVIOUS_PART_WRITER,
            1,
            10,
            now_at_seal(10) - NS_PER_HOUR,
        )
        .await;
        let first = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                FIRST_FOLDER,
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("first fold");
        assert!(first.rebuilt, "the first fold builds HEAD from the layout");
        let head = read_metrics_head(inner).await;
        assert_eq!(head.parts.len(), 1, "one previous part");
        let declared = part_declared_len(inner, &head.parts[0].key).await;
        publish_segment(
            inner,
            0,
            NEXT_PART_WRITER,
            2,
            12,
            now_at_seal(12) - NS_PER_HOUR,
        )
        .await;
        declared
    }

    /// The fold `previous_part_baseline` sets up: incremental, from its HEAD.
    async fn fold_after_previous_part(catalog: &Catalog) -> Result<FoldReport, CatalogError> {
        catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                SECOND_FOLDER,
                now_at_seal(12),
                &[],
                None,
            )
            .await
    }

    /// Issue #3320: the fold charges its decode of a previous snapshot part
    /// to the catalog's memory budget before decoding it, reserving the
    /// part's declared `entries_uncompressed_len` (not its stored length),
    /// and a refusal fails that fold tick with the typed error rather than
    /// rebuilding from the commit layout, which would re-list and re-read
    /// every hour HEAD covers. HEAD is left as it was and the store sees no
    /// LIST, which the rebuild's layout discovery would issue.
    ///
    /// FLIP: drop the `reserve_decoded` call in `Catalog::load_part_entries`
    /// and the fold succeeds, so `expect_err` panics. FLIP: delete the
    /// `Err(CatalogError::MemoryExhausted(exhausted)) => return ...` arm after
    /// `load_previous_entries` in `fold_with_refold_request` and the refusal
    /// falls into the rebuild, which succeeds, so `expect_err` panics.
    #[tokio::test]
    async fn a_refused_previous_part_reservation_fails_the_fold() {
        let inner = Arc::new(MemoryStore::new());
        let declared = previous_part_baseline(&inner).await;
        let head_before = metrics_head_bytes(&inner).await;

        let budget = Arc::new(ravel_memory::MemoryBudget::new(declared - 1));
        let store = Arc::new(InstrumentedStore::new(inner.clone()));
        let catalog = Catalog::new(store.clone(), config(1))
            .expect("catalog")
            .with_memory_budget(budget.clone());
        let err = fold_after_previous_part(&catalog)
            .await
            .expect_err("a refused previous part reservation must fail the fold");
        match err {
            CatalogError::MemoryExhausted(exhausted) => {
                assert_eq!(exhausted.requested, declared);
                assert_eq!(exhausted.reserved, 0);
                assert_eq!(exhausted.limit, declared - 1);
            }
            other => panic!("expected CatalogError::MemoryExhausted, got {other:?}"),
        }
        assert_eq!(
            budget.reserved(),
            0,
            "a refused reservation charges nothing"
        );
        assert_eq!(
            metrics_head_bytes(&inner).await,
            head_before,
            "the refused fold publishes no HEAD"
        );
        assert_eq!(list_calls(&store), 0, "no commit-layout re-listing ran");
    }

    /// Issue #3320: a refused previous-part reservation leaves nothing behind
    /// that the next fold tick trips over. Fixture: a budget with room for the
    /// part, and a reservation held by someone else that leaves one byte too
    /// few; once that reservation drops, the same catalog's next fold of the
    /// same tenant succeeds incrementally and writes the HEAD a store folded
    /// with no budget at all writes, byte for byte, and returns every byte it
    /// charged.
    ///
    /// FLIP: delete the `Err(CatalogError::MemoryExhausted(exhausted)) =>
    /// return ...` arm after `load_previous_entries` in
    /// `fold_with_refold_request` and the first fold rebuilds and succeeds, so
    /// `expect_err` panics. FLIP: drop the `reserve_decoded` call in
    /// `Catalog::load_part_entries` and the same `expect_err` panics.
    #[tokio::test]
    async fn the_next_fold_tick_retries_after_a_refused_previous_part() {
        let control = Arc::new(MemoryStore::new());
        previous_part_baseline(&control).await;
        let control_catalog = Catalog::new(control.clone(), config(1)).expect("catalog");
        let controlled = fold_after_previous_part(&control_catalog)
            .await
            .expect("the control fold");
        assert!(!controlled.rebuilt, "the control fold is incremental");

        let inner = Arc::new(MemoryStore::new());
        let declared = previous_part_baseline(&inner).await;
        let head_before = metrics_head_bytes(&inner).await;
        let limit = 64 * declared;
        let budget = Arc::new(ravel_memory::MemoryBudget::new(limit));
        let catalog = Catalog::new(inner.clone(), config(1))
            .expect("catalog")
            .with_memory_budget(budget.clone());
        let pressure = budget
            .reserve(limit - declared + 1)
            .expect("the competing reservation fits");
        let err = fold_after_previous_part(&catalog)
            .await
            .expect_err("the first tick is refused");
        match err {
            CatalogError::MemoryExhausted(exhausted) => {
                assert_eq!(exhausted.requested, declared);
                assert_eq!(exhausted.reserved, limit - declared + 1);
            }
            other => panic!("expected CatalogError::MemoryExhausted, got {other:?}"),
        }
        assert_eq!(metrics_head_bytes(&inner).await, head_before);
        drop(pressure);
        assert_eq!(budget.reserved(), 0, "the refused tick holds nothing");

        let retried = fold_after_previous_part(&catalog)
            .await
            .expect("the next tick folds");
        assert!(!retried.rebuilt, "the retry is incremental: {retried:?}");
        assert_eq!(retried.entry_count, controlled.entry_count);
        assert_eq!(
            metrics_head_bytes(&inner).await,
            metrics_head_bytes(&control).await,
            "the retried fold writes the HEAD an unrefused fold writes"
        );
        assert_eq!(
            budget.reserved(),
            0,
            "the fold returns every byte it charged"
        );
    }

    /// Issue #3320: a previous part's charge is held for as long as the fold
    /// holds the part's entries, not only across its decode. Fixture: two
    /// previous parts (`snapshot_part_max_entries = 1`, one hour each) under a
    /// budget of the two declared lengths together less one byte, so each fits
    /// alone and the pair does not. The fold reserves the parts in HEAD order,
    /// so the refusal is on the second part with exactly the first part's
    /// declared length already reserved: that `reserved` figure is what a
    /// reservation dropped right after the first decode cannot produce, since
    /// it would read 0 and the second reservation would fit.
    /// `a_refused_postings_reservation_fails_the_fold` pins the rest of the
    /// span, a part charge still held when the postings reserve later on.
    ///
    /// FLIP: in `Catalog::load_previous_entries`, drop each `reservation`
    /// instead of pushing it onto `reservations`, and the fold succeeds, so
    /// `expect_err` panics.
    #[tokio::test]
    async fn the_previous_part_charge_is_held_while_the_entries_are_resident() {
        let inner = Arc::new(MemoryStore::new());
        let cfg = CatalogConfig {
            snapshot_part_max_entries: 1,
            ..config(1)
        };
        let unbudgeted = Catalog::new(inner.clone(), cfg).expect("catalog");
        publish_segment(
            &inner,
            0,
            Uuid::new_v4(),
            1,
            10,
            now_at_seal(10) - NS_PER_HOUR,
        )
        .await;
        publish_segment(
            &inner,
            0,
            Uuid::new_v4(),
            2,
            11,
            now_at_seal(11) - NS_PER_HOUR,
        )
        .await;
        unbudgeted
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(11),
                &[],
                None,
            )
            .await
            .expect("first fold");
        let head = read_metrics_head(&inner).await;
        assert_eq!(head.parts.len(), 2, "one part per hour under a cap of 1");
        let first = part_declared_len(&inner, &head.parts[0].key).await;
        let second = part_declared_len(&inner, &head.parts[1].key).await;
        publish_segment(
            &inner,
            0,
            Uuid::new_v4(),
            3,
            13,
            now_at_seal(13) - NS_PER_HOUR,
        )
        .await;

        let budget = Arc::new(ravel_memory::MemoryBudget::new(first + second - 1));
        let catalog = Catalog::new(inner.clone(), cfg)
            .expect("catalog")
            .with_memory_budget(budget.clone());
        let err = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(13),
                &[],
                None,
            )
            .await
            .expect_err("the second part does not fit beside the first");
        match err {
            CatalogError::MemoryExhausted(exhausted) => {
                assert_eq!(exhausted.requested, second);
                assert_eq!(exhausted.reserved, first, "the first part is still charged");
                assert_eq!(exhausted.limit, first + second - 1);
            }
            other => panic!("expected CatalogError::MemoryExhausted, got {other:?}"),
        }
        assert_eq!(
            budget.reserved(),
            0,
            "the failed fold releases the first part"
        );
    }

    /// Issue #3320: the held-open pre-check (`held_open_has_unfolded`) loads
    /// a part through the same charged load; its refusal sends the fold down
    /// the full path, whose own load is refused the same way and fails the
    /// fold, rather than rebuilding. The only LISTs are the pre-check's
    /// re-listing of the three held-open hours.
    ///
    /// FLIP: delete the `Err(CatalogError::MemoryExhausted(exhausted)) =>
    /// return ...` arm after `load_previous_entries` in
    /// `fold_with_refold_request` and the fold rebuilds and succeeds, so
    /// `expect_err` panics.
    #[tokio::test]
    async fn a_refused_held_open_probe_fails_the_fold_without_a_rebuild() {
        const H: u32 = 480_000;
        let inner = Arc::new(MemoryStore::new());
        let unbudgeted = Catalog::new(inner.clone(), config(1)).expect("catalog");
        let now = now_inside(H);
        publish_segment(&inner, 0, Uuid::new_v4(), 1, H, now).await;
        let sealed = fold_sealing(&unbudgeted, now, Some(H)).await;
        assert_eq!(sealed.watermark_hour, Some(H), "{sealed:?}");
        let head = read_metrics_head(&inner).await;
        assert_eq!(head.parts.len(), 1, "one previous part");
        let declared = part_declared_len(&inner, &head.parts[0].key).await;
        let head_before = metrics_head_bytes(&inner).await;
        publish_segment(&inner, 0, Uuid::new_v4(), 2, H, now + 1).await;

        let budget = Arc::new(ravel_memory::MemoryBudget::new(declared - 1));
        let store = Arc::new(InstrumentedStore::new(inner.clone()));
        let catalog = Catalog::new(store.clone(), config(1))
            .expect("catalog")
            .with_memory_budget(budget.clone());
        let later = now + 5 * 60 * 1_000_000_000;
        assert_eq!(catalog.margin_watermark_hour(later), Some(H - 3));
        let err = catalog
            .fold_with_seal_through(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                later,
                &[],
                None,
                &RefoldRequest::new(),
                None,
            )
            .await
            .expect_err("a refused held-open part load must fail the fold");
        match err {
            CatalogError::MemoryExhausted(exhausted) => {
                assert_eq!(exhausted.requested, declared);
                assert_eq!(exhausted.reserved, 0);
                assert_eq!(exhausted.limit, declared - 1);
            }
            other => panic!("expected CatalogError::MemoryExhausted, got {other:?}"),
        }
        assert_eq!(budget.reserved(), 0);
        assert_eq!(metrics_head_bytes(&inner).await, head_before);
        assert_eq!(
            list_calls(&store),
            3,
            "only the held-open hours H-2..=H are listed, once each"
        );
    }

    const FOREIGN_TENANT: [u8; 16] = [0xff; 16];

    /// Rewrite part `index` of `signal`'s HEAD as the same entries under
    /// [`FOREIGN_TENANT`], re-stamping the part ref's blake3 and size, and PUT
    /// both, so the HEAD's blake3 check passes and only the part header's
    /// tenant binding can reject the part. Returns the part's key.
    pub(crate) async fn repoint_head_at_foreign_part(
        store: &dyn ObjectStoreBackend,
        signal: Signal,
        index: usize,
    ) -> String {
        repoint_tenant_head_at_foreign_part(store, &tenant(), signal, index).await
    }

    /// [`repoint_head_at_foreign_part`] for `owner`'s HEAD rather than the
    /// fixture tenant's.
    pub(crate) async fn repoint_tenant_head_at_foreign_part(
        store: &dyn ObjectStoreBackend,
        owner: &TenantHash,
        signal: Signal,
        index: usize,
    ) -> String {
        assert_ne!(FOREIGN_TENANT, owner.0, "the owning tenant differs");
        let head_key = head_object_key(owner, signal);
        let head_bytes = store
            .get(&head_key, GetRange::Full)
            .await
            .expect("HEAD present")
            .data;
        let mut head = snapshot_format::decode_head(&head_bytes).expect("HEAD decodes");
        let key = head.parts[index].key.clone();
        let original = store
            .get(&key, GetRange::Full)
            .await
            .expect("part present")
            .data;
        let decoded = snapshot_format::decode_part(&original, &PartLimits::default())
            .expect("the fixture part decodes");
        let header = &decoded.header;
        let foreign = snapshot_format::encode_part_ranged(
            FOREIGN_TENANT,
            header.signal,
            header.shard_count,
            header.min_hour,
            header.watermark_hour,
            &decoded.entries,
        )
        .expect("re-encode the fixture part for a foreign tenant");
        let foreign_header =
            snapshot_format::decode_part_header(&foreign).expect("foreign header decodes");
        assert_eq!(foreign_header.tenant_hash, FOREIGN_TENANT.to_vec());
        assert_eq!(
            foreign_header.entries_uncompressed_len, header.entries_uncompressed_len,
            "only the tenant differs, so the part's charge is unchanged"
        );
        let part_ref = &mut head.parts[index];
        part_ref.blake3 = blake3::hash(&foreign).as_bytes().to_vec();
        part_ref.size = foreign.len() as u64;
        let restamped = part_ref.blake3.clone();
        // A HEAD's postings ref repeats each part's blake3; the postings object
        // itself keeps the old binding, so it no longer decodes against this
        // part.
        if let Some(postings) = head.postings.as_mut() {
            postings.part_blake3[index] = restamped;
        }
        store
            .put(&key, Bytes::from(foreign), PutOptions::default())
            .await
            .expect("put the foreign part");
        let head_bytes = snapshot_format::encode_head(&head).expect("encode head");
        store
            .put(&head_key, Bytes::from(head_bytes), PutOptions::default())
            .await
            .expect("put the re-pointed HEAD");
        key
    }

    /// The hex of the tenant [`repoint_tenant_head_at_foreign_part`] stamps.
    pub(crate) fn foreign_tenant_hex() -> String {
        hex::encode(FOREIGN_TENANT)
    }

    /// Assert `err` is the `tenant_hash` mismatch `load_part_entries` returns
    /// for the foreign part at `key`, the error the resolve path returns for
    /// the same part.
    pub(crate) fn assert_foreign_part_error(err: &CatalogError, key: &str) {
        match err {
            CatalogError::FieldMismatch {
                key: got_key,
                field,
                expected,
                actual,
            } => {
                assert_eq!(*field, "tenant_hash");
                assert_eq!(got_key, key);
                assert_eq!(*expected, tenant().to_hex());
                assert_eq!(*actual, hex::encode(FOREIGN_TENANT));
            }
            other => panic!("expected a tenant_hash FieldMismatch, got {other:?}"),
        }
    }

    /// Issue #3539, ADR-0050 §2: the fold's load of a previous snapshot part
    /// checks the part header's `tenant_hash`, as the resolve path and
    /// `load_previous_postings` do, and a part naming another tenant fails the
    /// fold as a counted isolation breach rather than a rebuild from the
    /// commit layout that would leave it unreported. HEAD is left as it was
    /// and the store sees no LIST, which the rebuild's layout discovery would
    /// issue.
    ///
    /// FLIP: delete the `tenant_hash` comparison in
    /// `Catalog::load_part_entries` and the second fold succeeds, so
    /// `expect_err` panics. FLIP: delete the `Err(err) if
    /// is_foreign_part(&err) => return Err(err)` arm after
    /// `load_previous_entries` in `fold_with_refold_request` and the breach
    /// falls into the rebuild, which succeeds, so `expect_err` panics. FLIP:
    /// drop the `record_isolation_breach` call in `load_part_entries` and
    /// `isolation_breaches()` reads 0.
    #[tokio::test]
    async fn a_foreign_tenant_hash_on_a_previous_part_fails_the_fold() {
        assert_foreign_previous_part_fails_the_fold(None).await;
    }

    /// The part's tenant check runs before its decode reservation: under a
    /// budget with no room at all the fold still reports the breach, not
    /// memory pressure, and charges nothing.
    ///
    /// FLIP: move the `tenant_hash` comparison in `Catalog::load_part_entries`
    /// below its `reserve_decoded` call, or delete it, and the fold fails with
    /// `MemoryExhausted` instead, so `assert_foreign_part_error` panics.
    #[tokio::test]
    async fn a_foreign_tenant_hash_on_a_part_is_reported_before_the_part_reservation() {
        assert_foreign_previous_part_fails_the_fold(Some(0)).await;
    }

    /// `previous_part_baseline`, with its one part re-pointed at a foreign
    /// copy, folded again under a budget of `budget` bytes (unlimited when
    /// `None`): the fold fails with the part's `tenant_hash` mismatch and one
    /// counted breach, leaves HEAD as it was, LISTs nothing and holds no
    /// charge.
    async fn assert_foreign_previous_part_fails_the_fold(budget: Option<u64>) {
        let inner = Arc::new(MemoryStore::new());
        previous_part_baseline(&inner).await;
        let part_key = repoint_head_at_foreign_part(inner.as_ref(), Signal::Metrics, 0).await;
        let head_before = metrics_head_bytes(&inner).await;

        let store = Arc::new(InstrumentedStore::new(inner.clone()));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let budget = budget.map(|limit| Arc::new(ravel_memory::MemoryBudget::new(limit)));
        let catalog = match &budget {
            Some(budget) => catalog.with_memory_budget(budget.clone()),
            None => catalog,
        };
        let err = fold_after_previous_part(&catalog)
            .await
            .expect_err("a previous part naming another tenant must fail the fold");
        assert_foreign_part_error(&err, &part_key);
        assert_eq!(
            catalog.isolation_breaches(),
            1,
            "the breach is counted once, not swallowed by a rebuild"
        );
        assert_eq!(
            metrics_head_bytes(&inner).await,
            head_before,
            "the failed fold publishes no HEAD"
        );
        assert_eq!(list_calls(&store), 0, "no commit-layout re-listing ran");
        if let Some(budget) = budget {
            assert_eq!(budget.reserved(), 0, "the breach charges nothing");
        }
    }

    /// Issue #3539: the held-open pre-check (`held_open_has_unfolded`) loads
    /// a part through the same checked load, and a part naming another tenant
    /// fails the fold from there, counted once: the main load never reads the
    /// part again to count it a second time. The only LISTs are the
    /// pre-check's re-listing of the three held-open hours.
    ///
    /// FLIP: delete the `tenant_hash` comparison in
    /// `Catalog::load_part_entries` and the fold succeeds, so `expect_err`
    /// panics. FLIP: delete the `Err(err) if is_foreign_part(&err) => return
    /// Err(err)` arm in `held_open_has_unfolded` and the probe's breach sends
    /// the fold down the full path, whose load counts it again, so
    /// `isolation_breaches()` reads 2.
    #[tokio::test]
    async fn a_foreign_tenant_hash_on_a_held_open_part_fails_the_fold_once() {
        const H: u32 = 480_000;
        let inner = Arc::new(MemoryStore::new());
        let unbudgeted = Catalog::new(inner.clone(), config(1)).expect("catalog");
        let now = now_inside(H);
        publish_segment(&inner, 0, Uuid::new_v4(), 1, H, now).await;
        let sealed = fold_sealing(&unbudgeted, now, Some(H)).await;
        assert_eq!(sealed.watermark_hour, Some(H), "{sealed:?}");
        assert_eq!(read_metrics_head(&inner).await.parts.len(), 1);
        let part_key = repoint_head_at_foreign_part(inner.as_ref(), Signal::Metrics, 0).await;
        let head_before = metrics_head_bytes(&inner).await;
        publish_segment(&inner, 0, Uuid::new_v4(), 2, H, now + 1).await;

        let store = Arc::new(InstrumentedStore::new(inner.clone()));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let later = now + 5 * 60 * 1_000_000_000;
        assert_eq!(catalog.margin_watermark_hour(later), Some(H - 3));
        let err = catalog
            .fold_with_seal_through(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                later,
                &[],
                None,
                &RefoldRequest::new(),
                None,
            )
            .await
            .expect_err("a held-open part naming another tenant must fail the fold");
        assert_foreign_part_error(&err, &part_key);
        assert_eq!(
            catalog.isolation_breaches(),
            1,
            "the probe's breach is the only one counted"
        );
        assert_eq!(metrics_head_bytes(&inner).await, head_before);
        assert_eq!(
            list_calls(&store),
            3,
            "only the held-open hours H-2..=H are listed, once each"
        );
    }

    /// Issue #1482 finding 4: `v3_content_baseline` must fetch a previous
    /// per-part stats object only for an old part this fold is actually about
    /// to re-derive, never for one about to be carried forward by reference.
    /// `snapshot_part_max_entries = 1` seals every hour into its own part, so
    /// the first fold over hours 10-12 produces three parts: two sealed
    /// (hour 10, hour 11) whose `(min_hour, watermark_hour)` come from their
    /// own entries and never change again, and one tail (hour 12) whose
    /// watermark is overridden to the fold's overall watermark. The first
    /// fold seals at hour 15 (`now_at_seal(15)`) even though entries only
    /// reach hour 12, so that override tuple is `(12, 15)` -- deliberately
    /// different from what hour 12 encodes to once it seals for real, so the
    /// second fold's reseal is a genuine content change, not a coincidental
    /// hash match.
    ///
    /// The second fold appends an hour-13 entry: hours 10 and 11 stay
    /// byte-identical and are carried by reference (untouched), while hour
    /// 12 seals from tail to non-tail -- its tuple becomes `(12, 12)`, so it
    /// genuinely re-derives even though its one entry's content is
    /// unchanged, and reuses that entry's segment statistics from the first
    /// fold's baseline. Pre-fix, all three non-dirty old parts get their
    /// stats object GET regardless of reuse; post-fix, only hour 12's does.
    #[tokio::test]
    async fn incremental_fold_bounds_baseline_gets_to_parts_actually_rederived() {
        let store = Arc::new(RecordingStore::new());
        set_status_column_config(store.as_ref()).await;

        publish_logs_segment(store.as_ref(), 1, 10, &[200]).await;
        publish_logs_segment(store.as_ref(), 2, 11, &[404]).await;
        publish_logs_segment(store.as_ref(), 3, 12, &[500]).await;

        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        let first = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(15),
                &[],
                None,
            )
            .await
            .expect("first fold");
        assert!(first.rebuilt, "first fold rebuilds from the commit layout");
        assert_eq!(first.parts_total, 3, "one part per hour under a cap of 1");

        let head_1 = read_logs_head(store.as_ref()).await;
        let stats_key_of = |hour: u32| {
            head_1
                .parts
                .iter()
                .find(|p| p.min_hour == hour)
                .unwrap_or_else(|| panic!("part for hour {hour}"))
                .column_stats
                .clone()
                .unwrap_or_else(|| panic!("v3 object for hour {hour}"))
                .key
        };
        let stats_key_10 = stats_key_of(10);
        let stats_key_11 = stats_key_of(11);
        let stats_key_12 = stats_key_of(12);

        publish_logs_segment(store.as_ref(), 4, 13, &[200]).await;

        store.clear_gets();
        let second = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(16),
                &[],
                None,
            )
            .await
            .expect("second fold");
        assert!(!second.rebuilt, "the second fold is incremental");

        let head_2 = read_logs_head(store.as_ref()).await;
        let part_10 = head_2
            .parts
            .iter()
            .find(|p| p.min_hour == 10)
            .expect("hour 10 part survives");
        let part_11 = head_2
            .parts
            .iter()
            .find(|p| p.min_hour == 11)
            .expect("hour 11 part survives");
        let part_12 = head_2
            .parts
            .iter()
            .find(|p| p.min_hour == 12)
            .expect("hour 12 part survives, now sealed");
        assert_eq!(
            part_10.column_stats.as_ref().map(|r| &r.key),
            Some(&stats_key_10),
            "hour 10's v3 object is carried forward at its same key"
        );
        assert_eq!(
            part_11.column_stats.as_ref().map(|r| &r.key),
            Some(&stats_key_11),
            "hour 11's v3 object is carried forward at its same key"
        );
        assert_ne!(
            part_12.column_stats.as_ref().map(|r| &r.key),
            Some(&stats_key_12),
            "hour 12 reseals from tail to non-tail and gets a new v3 object"
        );

        // The bound this finding fixes: an untouched, carried-forward part's
        // previous stats object must never be fetched as a baseline. Against
        // the pre-fix code (baseline built from every non-dirty old part
        // unconditionally) these are each 1.
        assert_eq!(
            store.count_gets_of(&stats_key_10),
            0,
            "hour 10 is carried by reference; its old stats object must not be fetched"
        );
        assert_eq!(
            store.count_gets_of(&stats_key_11),
            0,
            "hour 11 is carried by reference; its old stats object must not be fetched"
        );
        // Hour 12 is genuinely re-derived (tail -> sealed), so its baseline
        // fetch is legitimate work, not the bug: exactly one GET reuses its
        // one entry's segment statistics instead of re-fetching the segment
        // itself.
        assert_eq!(
            store.count_gets_of(&stats_key_12),
            1,
            "hour 12 is re-derived and its prior stats object is fetched once as a baseline"
        );
    }

    /// Declare three I64 typed logs columns (`col_a`, `col_b`, `col_c`) so a
    /// fold builds column statistics with three distinct-size dictionaries per
    /// part, for the degrade-loop tests.
    async fn set_three_column_config(store: &dyn ObjectStoreBackend) {
        let cfg = crate::tenant_config::TenantConfig {
            typed_attr_columns: Some(vec![
                crate::tenant_config::DeclaredTypedColumn {
                    key: "col_a".to_string(),
                    ty: crate::tenant_config::DeclaredColumnType::I64,
                },
                crate::tenant_config::DeclaredTypedColumn {
                    key: "col_b".to_string(),
                    ty: crate::tenant_config::DeclaredColumnType::I64,
                },
                crate::tenant_config::DeclaredTypedColumn {
                    key: "col_c".to_string(),
                    ty: crate::tenant_config::DeclaredColumnType::I64,
                },
            ]),
            ..crate::tenant_config::TenantConfig::new(
                crate::tenant_config::TenantLifecycleState::Active,
            )
        };
        crate::tenant_config::set_tenant_config(store, &tenant(), &cfg, 1)
            .await
            .expect("write tenant config");
    }

    /// Publish one L0 segment of `rows` entries, each carrying all three
    /// `col_a`/`col_b`/`col_c` declared columns with deliberately distinct
    /// cardinality (`col_a` all-distinct, `col_b` 30 distinct, `col_c` 3
    /// distinct), so their per-column dictionaries encode to distinct sizes.
    async fn publish_logs_segment_wide(
        store: &dyn ObjectStoreBackend,
        writer_seq: u64,
        ingest_hour_bucket: u32,
        rows: usize,
    ) -> CommitRecord {
        use ravel_logseg::writer::ObjectIdentity;
        use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
        use ravel_types::logstream::{AttrValue, log_stream_id};

        let writer_id = Uuid::from_u128(u128::from(writer_seq));
        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        )];
        let mut w = RlogWriter::new(
            RlogConfig::default(),
            ObjectIdentity {
                tenant_hash: tenant().0,
                shard: 0,
                writer_id: *writer_id.as_bytes(),
                writer_epoch: 1,
                writer_seq,
            },
        );
        let base_ts = i64::from(ingest_hour_bucket) * NS_PER_HOUR + 60_000_000_000;
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for i in 0..rows {
            let ts = base_ts + i as i64;
            min_ts = min_ts.min(ts);
            max_ts = max_ts.max(ts);
            w.push(LogRecord {
                stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("row {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![
                    ("col_a".to_string(), AttrValue::I64(i as i64)),
                    ("col_b".to_string(), AttrValue::I64((i % 30) as i64)),
                    ("col_c".to_string(), AttrValue::I64((i % 3) as i64)),
                ],
            })
            .expect("push");
        }
        let bytes = w.finish().expect("finish");
        let content_hash = *blake3::hash(&bytes).as_bytes();
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: rows as u64,
            series_count: 1,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
            min_ingest_ts_ns: min_ts,
            max_ingest_ts_ns: max_ts,
            segment_format_version: 1,
            created_unix_ns: max_ts,
            ingest_hour_bucket,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store, &data_key, Bytes::from(bytes))
            .await
            .expect("put data object");
        publish::publish(store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        record
    }

    /// Declare `n_columns` I64 typed columns (`c00`, `c01`, ...), all names
    /// the same byte length so every column's `ColumnStat.name` field costs
    /// the same number of bytes regardless of index.
    async fn set_many_column_config(store: &dyn ObjectStoreBackend, n_columns: usize) {
        let cfg = crate::tenant_config::TenantConfig {
            typed_attr_columns: Some(
                (0..n_columns)
                    .map(|i| crate::tenant_config::DeclaredTypedColumn {
                        key: format!("c{i:02}"),
                        ty: crate::tenant_config::DeclaredColumnType::I64,
                    })
                    .collect(),
            ),
            ..crate::tenant_config::TenantConfig::new(
                crate::tenant_config::TenantLifecycleState::Active,
            )
        };
        crate::tenant_config::set_tenant_config(store, &tenant(), &cfg, 1)
            .await
            .expect("write tenant config");
    }

    /// Publish one L0 segment carrying all `n_columns` declared columns
    /// (`c00`..`c{n_columns-1}`), every column given the same two distinct
    /// values across `rows` entries, so every (segment, column) dictionary
    /// encodes to the exact same size -- the fixture
    /// `degrade_completes_a_part_needing_many_drops_with_one_concatenation`
    /// needs a large, size-tied pair set so the drop order is decided purely
    /// by the heap's `(seg_idx, col_idx)` tie-break, not by planted size
    /// differences.
    async fn publish_logs_segment_many_columns(
        store: &dyn ObjectStoreBackend,
        writer_seq: u64,
        ingest_hour_bucket: u32,
        n_columns: usize,
        rows: usize,
    ) -> CommitRecord {
        use ravel_logseg::writer::ObjectIdentity;
        use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
        use ravel_types::logstream::{AttrValue, log_stream_id};

        let writer_id = Uuid::from_u128(u128::from(writer_seq));
        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        )];
        let mut w = RlogWriter::new(
            RlogConfig::default(),
            ObjectIdentity {
                tenant_hash: tenant().0,
                shard: 0,
                writer_id: *writer_id.as_bytes(),
                writer_epoch: 1,
                writer_seq,
            },
        );
        let base_ts = i64::from(ingest_hour_bucket) * NS_PER_HOUR + 60_000_000_000;
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        for i in 0..rows {
            let ts = base_ts + i as i64;
            min_ts = min_ts.min(ts);
            max_ts = max_ts.max(ts);
            let attrs = (0..n_columns)
                .map(|c| (format!("c{c:02}"), AttrValue::I64((i % 2) as i64)))
                .collect();
            w.push(LogRecord {
                stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("row {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs,
            })
            .expect("push");
        }
        let bytes = w.finish().expect("finish");
        let content_hash = *blake3::hash(&bytes).as_bytes();
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: rows as u64,
            series_count: 1,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
            min_ingest_ts_ns: min_ts,
            max_ingest_ts_ns: max_ts,
            segment_format_version: 1,
            created_unix_ns: max_ts,
            ingest_hour_bucket,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store, &data_key, Bytes::from(bytes))
            .await
            .expect("put data object");
        publish::publish(store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        record
    }

    /// Declare `status` as an I64 typed logs column so the fold builds column
    /// statistics for a (tenant, Logs) pair.
    async fn set_status_column_config(store: &dyn ObjectStoreBackend) {
        let cfg = crate::tenant_config::TenantConfig {
            typed_attr_columns: Some(vec![crate::tenant_config::DeclaredTypedColumn {
                key: "status".to_string(),
                ty: crate::tenant_config::DeclaredColumnType::I64,
            }]),
            ..crate::tenant_config::TenantConfig::new(
                crate::tenant_config::TenantLifecycleState::Active,
            )
        };
        crate::tenant_config::set_tenant_config(store, &tenant(), &cfg, 1)
            .await
            .expect("write tenant config");
    }

    async fn read_logs_head(store: &dyn ObjectStoreBackend) -> SnapshotHead {
        let got = store
            .get(&head_object_key(&tenant(), Signal::Logs), GetRange::Full)
            .await
            .expect("head present");
        snapshot_format::decode_head(&got.data).expect("head decodes")
    }

    /// Publish a compaction record for `(shard, ingest_hour_bucket)` whose parts
    /// are REAL L1 RLOG objects, each carrying the `status` I64 column. `parts`
    /// gives, per part, its status values and whether the part carries an
    /// ADR-0873 declared stamp over them (`status` min/max, no NULLs); part `i`
    /// becomes CompactionPart `part_index = i`. Both the part objects (at their
    /// reconstructed L1 keys) and the compaction record are written, so a fold
    /// over the hour produces one L1 `SnapshotEntry` per part and ADR-0942
    /// builds part-bound stats for each. Returns the record (its
    /// `parts[i].content_hash` is the v2 join key for part `i`).
    async fn publish_logs_l1(
        store: &dyn ObjectStoreBackend,
        shard: u32,
        ingest_hour_bucket: u32,
        input_set_seed: &str,
        parts: &[(&[i64], bool)],
    ) -> CompactionRecord {
        use ravel_logseg::writer::ObjectIdentity;
        use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
        use ravel_types::logstream::{AttrValue, log_stream_id};

        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        )];
        let input_set_hash = *blake3::hash(input_set_seed.as_bytes()).as_bytes();
        let base_ts = i64::from(ingest_hour_bucket) * NS_PER_HOUR + 60_000_000_000;

        let mut compaction_parts: Vec<CompactionPart> = Vec::new();
        let mut part_bytes: Vec<Vec<u8>> = Vec::new();
        let mut max_ts_all = i64::MIN;
        for (part_index, (statuses, stamped)) in parts.iter().enumerate() {
            let mut w = RlogWriter::new(
                RlogConfig::default(),
                ObjectIdentity {
                    tenant_hash: tenant().0,
                    shard,
                    writer_id: *Uuid::nil().as_bytes(),
                    writer_epoch: 0,
                    writer_seq: 0,
                },
            );
            let mut min_ts = i64::MAX;
            let mut max_ts = i64::MIN;
            for (i, status) in statuses.iter().enumerate() {
                let ts = base_ts + (part_index as i64) * 1_000_000 + i as i64;
                min_ts = min_ts.min(ts);
                max_ts = max_ts.max(ts);
                w.push(LogRecord {
                    stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                    stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                    ts_ns: ts,
                    observed_ts_ns: ts,
                    severity_num: 9,
                    severity_text: "INFO".into(),
                    body: format!("row {part_index}-{i}"),
                    trace_id: None,
                    span_id: None,
                    flags: 0,
                    attrs: vec![("status".to_string(), AttrValue::I64(*status))],
                })
                .expect("push");
            }
            let bytes = w.finish().expect("finish");
            let content_hash = *blake3::hash(&bytes).as_bytes();
            max_ts_all = max_ts_all.max(max_ts);
            let mut compaction_part = CompactionPart {
                part_index: part_index as u32,
                first_series_id: vec![0u8; 16],
                last_series_id: vec![0xffu8; 16],
                content_hash: content_hash.to_vec(),
                object_size: bytes.len() as u64,
                sample_count: statuses.len() as u64,
                series_count: 1,
                run_count: 1,
                min_event_ts_ns: min_ts,
                max_event_ts_ns: max_ts,
                segment_format_version: 2,
                declared_column_stats: Vec::new(),
            };
            if *stamped {
                let min = *statuses.iter().min().expect("a part has rows");
                let max = *statuses.iter().max().expect("a part has rows");
                ravel_commit::declared_stats::stamp_compaction_part(
                    &mut compaction_part,
                    &[stamp_stat("status", min, max, 0)],
                );
            }
            compaction_parts.push(compaction_part);
            part_bytes.push(bytes);
        }

        let record = CompactionRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: signal::to_proto(Signal::Logs).into(),
            shard,
            ingest_hour_bucket,
            level: 1,
            inputs: Vec::new(),
            input_set_hash: input_set_hash.to_vec(),
            parts: compaction_parts,
            created_unix_ns: max_ts_all,
            superseded_record_key: String::new(),
        };
        for (part, bytes) in record.parts.iter().zip(part_bytes) {
            let key = keys::reconstruct_l1_part_key(&record, part).expect("l1 part key");
            publish::put_data_object(store, &key, Bytes::from(bytes))
                .await
                .expect("put l1 part object");
        }
        let ckey = keys::compaction_record_key_for(&record).expect("compaction key");
        store
            .put(
                &ckey,
                Bytes::from(record.encode_to_vec()),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put compaction record");
        record
    }

    /// Assert the single declared `status` I64 column of a decoded segment has
    /// exactly the given statistics.
    fn assert_status_column(
        seg: &ColumnStatsSegment,
        non_null: u64,
        null: u64,
        min: i64,
        max: i64,
        dict: &[(i64, u64)],
        sum: i64,
    ) {
        use ravel_proto::catalog::v1::ColumnValue;
        use ravel_proto::catalog::v1::column_value::Kind;
        let unwrap_i64 = |v: &ColumnValue| match &v.kind {
            Some(Kind::I64(x)) => *x,
            other => panic!("expected I64 column value, got {other:?}"),
        };
        assert_eq!(seg.columns.len(), 1, "only status is declared");
        let col = &seg.columns[0];
        assert_eq!(col.name, "status");
        assert_eq!(col.declared_type, 2, "I64 stats tag");
        assert_eq!(col.non_null_count, non_null);
        assert_eq!(col.null_count, null);
        assert_eq!(unwrap_i64(col.min.as_ref().expect("min")), min);
        assert_eq!(unwrap_i64(col.max.as_ref().expect("max")), max);
        assert!(col.dictionary_present, "cardinality under the ceiling");
        let got: Vec<(i64, u64)> = col
            .dictionary
            .iter()
            .map(|e| (unwrap_i64(e.value.as_ref().expect("value")), e.count))
            .collect();
        assert_eq!(got, dict, "exact per-value counts, ascending");
        assert_eq!(col.sum, Some(sum));
    }

    /// ADR-1413: a fold over an L1-COMPACTED fixture builds the per-part (v3,
    /// field 7) object over the L1 entries, with EXACT expected values, and
    /// two L1 parts of one (shard, hour) bucket -- the collision the old
    /// five-field tuple key could not represent, because a reconstructed L1
    /// SegmentRef carries writer_id nil / epoch 0 / seq 0 -- produce two
    /// DISTINCT records keyed by their distinct content hashes within that
    /// one part's object, neither overwriting the other.
    #[tokio::test]
    async fn l1_compacted_fold_builds_part_bound_records_with_exact_values() {
        let store = Arc::new(MemoryStore::new());
        set_status_column_config(store.as_ref()).await;

        // One compaction record, two L1 parts, both in (shard 0, hour 10).
        let record = publish_logs_l1(
            store.as_ref(),
            0,
            10,
            "input-set-seed",
            &[(&[200, 404, 200, 500], false), (&[500, 500, 200], false)],
        )
        .await;

        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let report = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("fold");
        assert!(report.rebuilt, "first fold rebuilds from the commit layout");
        assert_eq!(report.entry_count, 2, "two L1 parts folded");
        assert_eq!(
            report.column_stats_part_objects_built, 1,
            "both L1 entries seal into one output part, so one v3 object is built"
        );

        let head = read_logs_head(store.as_ref()).await;
        // Both L1 entries really are level 1 in the folded snapshot.
        let entries = collect_head_entries(store.as_ref(), &head).await;
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.level == 1), "genuinely L1 fixture");
        assert_eq!(
            head.parts.len(),
            1,
            "one output part covers both L1 entries"
        );

        let stats_ref = head.parts[0]
            .column_stats
            .clone()
            .expect("field 7 present after folding L1 parts");
        let got = store
            .get(&stats_ref.key, GetRange::Full)
            .await
            .expect("v3 object present");
        let decoded = snapshot_format::decode_column_stats(
            &got.data,
            &crate::snapshot_format::ColumnStatsLimits::default(),
        )
        .expect("v3 decodes");
        assert_eq!(
            decoded.header.format_version, 3,
            "per-part object is envelope v3"
        );
        assert_eq!(
            decoded.segments.len(),
            2,
            "two L1 parts of one bucket -> two records"
        );

        let ch0 = record.parts[0].content_hash.clone();
        let ch1 = record.parts[1].content_hash.clone();
        assert_ne!(ch0, ch1, "distinct parts have distinct content hashes");
        // A v2 record is keyed by the part content hash carried in writer_id.
        let rec0 = decoded
            .segments
            .iter()
            .find(|s| s.writer_id == ch0)
            .expect("part 0's record is present, keyed by its content hash");
        let rec1 = decoded
            .segments
            .iter()
            .find(|s| s.writer_id == ch1)
            .expect("part 1's record is present, keyed by its content hash");
        // Neither collapsed onto the other: exact, per-part values.
        assert_status_column(rec0, 4, 0, 200, 500, &[(200, 2), (404, 1), (500, 1)], 1304);
        assert_status_column(rec1, 3, 0, 200, 500, &[(200, 1), (500, 2)], 1200);
    }

    pub(crate) async fn list_all_keys(store: &dyn ObjectStoreBackend, prefix: &str) -> Vec<String> {
        let mut out = Vec::new();
        let mut page_token = None;
        loop {
            let page = store.list(prefix, page_token).await.expect("list");
            out.extend(page.objects.into_iter().map(|m| m.key));
            match page.next {
                Some(t) => page_token = Some(t),
                None => break,
            }
        }
        out
    }

    /// ADR-1413 decision 6 (#1600): a fold whose entry count crosses
    /// `snapshot_part_max_entries` produces a multi-part HEAD, and EACH newly
    /// written part gets its OWN v3 (field 7) column-statistics object, not
    /// one shared whole-tenant object -- and no whole-tenant v1/v2 object is
    /// published at all: the store holds EXACTLY one `.cstat` object per
    /// part, never a whole-tenant one, at any size.
    #[tokio::test]
    async fn fold_over_two_parts_writes_exactly_two_cstat_objects_and_no_whole_object() {
        let store = Arc::new(MemoryStore::new());
        set_status_column_config(store.as_ref()).await;

        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        // Two L0 segments in two distinct sealed hours: cap 1 seals hour 10
        // into its own part and starts a fresh tail at hour 11.
        publish_logs_segment(store.as_ref(), 0, 10, &[200, 404]).await;
        publish_logs_segment(store.as_ref(), 1, 11, &[500, 500, 200]).await;

        let report = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(11),
                &[],
                None,
            )
            .await
            .expect("fold");
        assert_eq!(
            report.parts_total, 2,
            "crossing the cap splits into two parts"
        );
        assert_eq!(
            report.column_stats_part_objects_built, 2,
            "one v3 object per newly written part"
        );

        let head = read_logs_head(store.as_ref()).await;
        assert_eq!(head.parts.len(), 2);
        assert!(
            head.parts.iter().all(|p| p.column_stats.is_some()),
            "field 7 set on both parts"
        );

        let all_keys = list_all_keys(store.as_ref(), "").await;
        let cstat_keys: Vec<&String> = all_keys.iter().filter(|k| k.ends_with(".cstat")).collect();
        assert_eq!(
            cstat_keys.len(),
            2,
            "exactly two .cstat objects in the store, one per part: {cstat_keys:?}"
        );

        for part in &head.parts {
            let part_stats = part
                .column_stats
                .clone()
                .expect("every newly written part carries its own field 7 ref");
            assert_eq!(
                part_stats.part_blake3,
                vec![part.blake3.clone()],
                "the v3 object's own part_blake3 names exactly this part"
            );
            assert_eq!(
                part_stats.segment_count, 1,
                "one entry per part in this fixture"
            );

            let got = store
                .get(&part_stats.key, GetRange::Full)
                .await
                .expect("v3 object present at its declared key");
            assert_eq!(got.data.len() as u64, part_stats.size);
            assert_eq!(
                blake3::hash(&got.data).as_bytes().to_vec(),
                part_stats.blake3
            );
            let decoded = snapshot_format::decode_column_stats(
                &got.data,
                &crate::snapshot_format::ColumnStatsLimits::default(),
            )
            .expect("v3 decodes");
            assert_eq!(
                decoded.header.format_version, 3,
                "per-part object is envelope v3"
            );
            assert_eq!(decoded.segments.len(), 1);
            assert_eq!(
                decoded.header.part_blake3,
                vec![part.blake3.clone()],
                "decoded header's part_blake3 matches the ref"
            );
        }
    }

    /// ADR-1413 decision 4 (amended): a part whose per-part column-statistics
    /// body exceeds the ceiling degrades rather than refuses -- the fold
    /// drops the largest remaining dictionary (by its own encoded size),
    /// re-measures, and repeats until the part fits. Three declared columns
    /// with deliberately distinct dictionary sizes (`col_a` all-distinct
    /// across `rows` entries, `col_b` 30 distinct, `col_c` 3 distinct) let a
    /// small injected ceiling force exactly the two largest (`col_a`,
    /// `col_b`) to drop while `col_c`'s dictionary survives; min/max/count/sum
    /// stay exact for all three regardless.
    #[tokio::test]
    async fn fold_drops_the_largest_dictionaries_until_a_part_fits_the_ceiling() {
        let rows = 300;

        // Reference fold at the real (unbounded-for-this-fixture) ceiling:
        // every dictionary intact. Used only to measure, via the same
        // `column_stats_segments_concat` the fold itself measures with, the
        // exact post-drop body size -- so the injected ceiling below is
        // derived from the encoder's own accounting rather than a guessed
        // constant.
        let reference_store = Arc::new(MemoryStore::new());
        set_three_column_config(reference_store.as_ref()).await;
        publish_logs_segment_wide(reference_store.as_ref(), 0, 10, rows).await;
        let reference_catalog = Catalog::new(reference_store.clone(), config(1)).expect("catalog");
        let reference_report = reference_catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("reference fold at the real ceiling never degrades");
        assert_eq!(
            reference_report.column_stats_dictionaries_dropped, 0,
            "the real ceiling admits this small fixture with every dictionary intact"
        );
        let reference_head = read_logs_head(reference_store.as_ref()).await;
        let reference_ref = reference_head.parts[0]
            .column_stats
            .clone()
            .expect("v3 ref");
        let reference_got = reference_store
            .get(&reference_ref.key, GetRange::Full)
            .await
            .expect("v3 object present");
        let reference_decoded = snapshot_format::decode_column_stats(
            &reference_got.data,
            &crate::snapshot_format::ColumnStatsLimits::default(),
        )
        .expect("v3 decodes");
        assert_eq!(reference_decoded.segments.len(), 1);
        let reference_columns = reference_decoded.segments[0].columns.clone();

        let mut sizes: Vec<(usize, usize)> = reference_columns
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let size: usize = c.dictionary.iter().map(|e| e.encoded_len()).sum();
                (i, size)
            })
            .collect();
        sizes.sort_by_key(|(_, size)| std::cmp::Reverse(*size));
        let largest_idx = sizes[0].0;
        let second_idx = sizes[1].0;
        let kept_idx = sizes[2].0;
        assert_eq!(
            reference_columns[largest_idx].name, "col_a",
            "col_a has the most distinct values, so the largest dictionary"
        );
        assert_eq!(
            reference_columns[second_idx].name, "col_b",
            "col_b is the second-most distinct"
        );
        assert_eq!(
            reference_columns[kept_idx].name, "col_c",
            "col_c has the fewest distinct values, so the smallest dictionary"
        );

        let mut post_drop_segments = reference_decoded.segments.clone();
        post_drop_segments[0].columns[largest_idx].dictionary_present = false;
        post_drop_segments[0].columns[largest_idx]
            .dictionary
            .clear();
        post_drop_segments[0].columns[second_idx].dictionary_present = false;
        post_drop_segments[0].columns[second_idx].dictionary.clear();
        let ceiling =
            snapshot_format::column_stats_segments_concat(&post_drop_segments).len() as u64;

        // Fresh store, byte-identical fixture, ceiling injected: dropping only
        // the single largest dictionary still leaves `col_b`'s dictionary in
        // the body, which is strictly larger than `ceiling` (built from
        // dropping both), so the degrade loop must continue to a second drop.
        let store = Arc::new(MemoryStore::new());
        set_three_column_config(store.as_ref()).await;
        publish_logs_segment_wide(store.as_ref(), 0, 10, rows).await;
        let mut catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        catalog.set_column_stats_part_ceiling_for_test(ceiling);

        let report = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("an over-ceiling part degrades rather than refusing");
        assert_eq!(
            report.column_stats_dictionaries_dropped, 2,
            "exactly the two largest dictionaries are dropped"
        );

        let head = read_logs_head(store.as_ref()).await;
        let stats_ref = head.parts[0].column_stats.clone().expect("v3 ref");
        let got = store
            .get(&stats_ref.key, GetRange::Full)
            .await
            .expect("v3 object present");
        let limits = crate::snapshot_format::ColumnStatsLimits {
            max_column_stats_bytes: ceiling,
        };
        let decoded = snapshot_format::decode_column_stats(&got.data, &limits)
            .expect("the degraded object decodes under the injected ceiling");
        let columns = &decoded.segments[0].columns;
        assert!(
            !columns[largest_idx].dictionary_present,
            "col_a's dictionary (largest) was dropped"
        );
        assert!(
            columns[largest_idx].dictionary.is_empty(),
            "a dropped dictionary is empty, not truncated"
        );
        assert!(
            !columns[second_idx].dictionary_present,
            "col_b's dictionary (second-largest) was dropped"
        );
        assert!(columns[second_idx].dictionary.is_empty());
        assert!(
            columns[kept_idx].dictionary_present,
            "col_c's dictionary (smallest) survives"
        );
        assert_eq!(
            columns[kept_idx].dictionary, reference_columns[kept_idx].dictionary,
            "the surviving dictionary is unchanged"
        );
        for i in 0..3 {
            assert_eq!(
                columns[i].non_null_count, reference_columns[i].non_null_count,
                "column {i} non_null_count stays exact regardless of dictionary drop"
            );
            assert_eq!(
                columns[i].min, reference_columns[i].min,
                "column {i} min stays exact"
            );
            assert_eq!(
                columns[i].max, reference_columns[i].max,
                "column {i} max stays exact"
            );
            assert_eq!(
                columns[i].sum, reference_columns[i].sum,
                "column {i} sum stays exact"
            );
        }
    }

    /// Issue #3244: [`Catalog::fold_column_stats_dictionaries_dropped`] is the
    /// sum of the successful folds' reported
    /// [`FoldReport::column_stats_dictionaries_dropped`], in the folded
    /// signal's own slot. The fixture is the one above: one part whose two
    /// largest dictionaries must drop to fit an injected ceiling, so a counter
    /// that added one per part or one per degraded part reads 1, not 2. A
    /// second, no-op fold reports 0 drops and must leave the counter at 2.
    /// The other folded signals were never folded and read exactly 0.
    #[tokio::test]
    async fn fold_dictionary_drop_counter_sums_the_reported_drops_per_signal() {
        let rows = 300;
        let reference_store = Arc::new(MemoryStore::new());
        set_three_column_config(reference_store.as_ref()).await;
        publish_logs_segment_wide(reference_store.as_ref(), 0, 10, rows).await;
        Catalog::new(reference_store.clone(), config(1))
            .expect("catalog")
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("reference fold");
        let reference_ref = read_logs_head(reference_store.as_ref()).await.parts[0]
            .column_stats
            .clone()
            .expect("v3 ref");
        let reference_got = reference_store
            .get(&reference_ref.key, GetRange::Full)
            .await
            .expect("v3 object present");
        let mut post_drop_segments = snapshot_format::decode_column_stats(
            &reference_got.data,
            &crate::snapshot_format::ColumnStatsLimits::default(),
        )
        .expect("v3 decodes")
        .segments;
        for column in &mut post_drop_segments[0].columns {
            if column.name == "col_a" || column.name == "col_b" {
                column.dictionary_present = false;
                column.dictionary.clear();
            }
        }
        let ceiling =
            snapshot_format::column_stats_segments_concat(&post_drop_segments).len() as u64;

        let store = Arc::new(MemoryStore::new());
        set_three_column_config(store.as_ref()).await;
        publish_logs_segment_wide(store.as_ref(), 0, 10, rows).await;
        let mut catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        catalog.set_column_stats_part_ceiling_for_test(ceiling);
        assert_eq!(
            catalog.fold_column_stats_dictionaries_dropped(Signal::Logs),
            0
        );

        let report = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("an over-ceiling part degrades rather than refusing");
        assert_eq!(report.column_stats_dictionaries_dropped, 2);
        assert_eq!(report.parts_total, 1, "one part, so drops != parts");
        assert_eq!(
            catalog.fold_column_stats_dictionaries_dropped(Signal::Logs),
            report.column_stats_dictionaries_dropped,
            "the counter equals the report's figure exactly"
        );

        let no_op = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("second fold succeeds");
        assert!(
            no_op.no_op,
            "nothing new sealed, so the second fold is a no-op"
        );
        assert_eq!(no_op.column_stats_dictionaries_dropped, 0);
        assert_eq!(catalog.fold_cycles(Signal::Logs), 2);
        assert_eq!(
            catalog.fold_column_stats_dictionaries_dropped(Signal::Logs),
            2,
            "a fold that dropped nothing adds nothing"
        );
        for untouched in [Signal::Metrics, Signal::Spans] {
            assert_eq!(
                catalog.fold_column_stats_dictionaries_dropped(untouched),
                0,
                "{untouched:?} was never folded"
            );
        }
    }

    /// ADR-1413 decision 4 (amended): the fold refuses a part ONLY once no
    /// dictionary is left to drop and the dictionary-free body (fixed
    /// fields: name, declared_type, non_null_count, null_count, min, max,
    /// sum, plus the segment/header framing) is still over the ceiling. A
    /// ceiling of 1 byte is below that fixed-field floor for any declared
    /// column, so this never depends on dictionary cardinality at all.
    #[tokio::test]
    async fn fold_refuses_only_a_part_whose_dictionary_free_stats_exceed_the_ceiling() {
        let store = Arc::new(MemoryStore::new());
        set_status_column_config(store.as_ref()).await;
        publish_logs_segment(store.as_ref(), 0, 10, &[200, 404, 200]).await;

        let mut catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        catalog.set_column_stats_part_ceiling_for_test(1);

        let err = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect_err("dictionary-free stats alone still exceed a 1-byte ceiling");
        match err {
            CatalogError::ColumnStatsPartOverBound {
                part_key,
                declared,
                ceiling,
            } => {
                assert!(
                    part_key.contains(".csnap"),
                    "error names the part object's own key, got {part_key}"
                );
                assert_eq!(ceiling, 1);
                assert!(
                    declared > ceiling,
                    "declared {declared} must exceed ceiling {ceiling} for this to be the refusal path"
                );
            }
            other => panic!("expected ColumnStatsPartOverBound, got {other:?}"),
        }

        // No column-stats object of any version was ever published: the
        // fold bailed out of the per-part loop before it could reach the
        // whole-tenant v1/v2 builds that run after it.
        let keys = list_all_keys(store.as_ref(), &format!("t/{}/", tenant().to_hex())).await;
        assert!(
            keys.iter().all(|k| !k.ends_with(".cstat")),
            "no column-stats object of any version was written, got {keys:?}"
        );
        // The ceiling check runs before the part's own `.csnap` PUT, so a
        // refused part never leaves an orphan part object behind either.
        assert!(
            keys.iter().all(|k| !k.ends_with(".csnap")),
            "no part object was written for the refused part, got {keys:?}"
        );
        // HEAD was never written either: a failed fold must not publish any
        // catalog state.
        assert!(
            store
                .get(&head_object_key(&tenant(), Signal::Logs), GetRange::Full)
                .await
                .is_err(),
            "HEAD must not exist after a fold that failed before publishing"
        );
    }

    /// #1482: the pre-fix loop re-measured the WHOLE part's uncompressed
    /// body (`column_stats_segments_concat`, a fresh multi-gigabyte `Vec`)
    /// and re-summed every dictionary's `encoded_len` on every single drop,
    /// so a part needing many drops never finished. This fixture forces
    /// 1,500 drops across 2,000 (segment, column) pairs (100 segments x 20
    /// columns each, every dictionary the same size so the drop order is
    /// decided purely by the heap's `(seg_idx, col_idx)` tie-break) and
    /// pins the fix at measuring the body exactly twice regardless of how
    /// many drops it takes: once before the drop loop, once after.
    ///
    /// Reverting the fix's initial `running_total` computation in
    /// `fold.rs` back to a per-iteration `column_stats_segments_concat`
    /// call inside the `while running_total > ceiling` loop (the shape
    /// `#1482` reports) flips this test's concatenation-count assertion
    /// back to failing, since the counter would then read 1,501 instead of
    /// 2 (1 before the loop that is no longer needed, plus 1500 in-loop,
    /// plus the 1 after).
    #[tokio::test]
    async fn degrade_completes_a_part_needing_many_drops_with_one_concatenation() {
        let n_columns = 20;
        let n_segments = 100;
        let rows = 4;

        // Reference fold at the real (unbounded-for-this-fixture) ceiling:
        // every dictionary intact. Used only to measure, via the same
        // `column_stats_segments_concat` the fold itself measures with,
        // the exact post-drop body size for an injected ceiling that
        // forces exactly 1500 drops.
        let reference_store = Arc::new(MemoryStore::new());
        set_many_column_config(reference_store.as_ref(), n_columns).await;
        for seq in 0..n_segments {
            publish_logs_segment_many_columns(
                reference_store.as_ref(),
                seq as u64,
                10,
                n_columns,
                rows,
            )
            .await;
        }
        let reference_catalog = Catalog::new(reference_store.clone(), config(1)).expect("catalog");
        let reference_report = reference_catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("reference fold at the real ceiling never degrades");
        assert_eq!(
            reference_report.column_stats_dictionaries_dropped, 0,
            "the real ceiling admits this fixture with every dictionary intact"
        );
        let reference_head = read_logs_head(reference_store.as_ref()).await;
        let reference_ref = reference_head.parts[0]
            .column_stats
            .clone()
            .expect("v3 ref");
        let reference_got = reference_store
            .get(&reference_ref.key, GetRange::Full)
            .await
            .expect("v3 object present");
        let reference_decoded = snapshot_format::decode_column_stats(
            &reference_got.data,
            &crate::snapshot_format::ColumnStatsLimits::default(),
        )
        .expect("v3 decodes");
        assert_eq!(reference_decoded.segments.len(), n_segments);
        for seg in &reference_decoded.segments {
            assert_eq!(seg.columns.len(), n_columns);
        }

        // Drop every dictionary in the LAST 75 segments (all 20 columns
        // each = 1500 pairs). Every pair's dictionary is the same size, so
        // this is exactly what the degrade loop's heap picks: with the
        // primary (size) key tied across all 2000 pairs, the heap's
        // `(size, seg_idx, col_idx, ...)` tuple order pops the highest
        // seg_idx first, then within it the highest col_idx, reproducing
        // the same "last maximum wins" tie-break the pre-fix
        // `max_by_key`-based loop used.
        let mut post_drop_segments = reference_decoded.segments.clone();
        let dropped_seg_start = n_segments - 75;
        for seg in &mut post_drop_segments[dropped_seg_start..] {
            for col in &mut seg.columns {
                col.dictionary_present = false;
                col.dictionary.clear();
            }
        }
        let ceiling =
            snapshot_format::column_stats_segments_concat(&post_drop_segments).len() as u64;

        // Fresh store, byte-identical fixture, ceiling injected.
        let store = Arc::new(MemoryStore::new());
        set_many_column_config(store.as_ref(), n_columns).await;
        for seq in 0..n_segments {
            publish_logs_segment_many_columns(store.as_ref(), seq as u64, 10, n_columns, rows)
                .await;
        }
        let mut catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        catalog.set_column_stats_part_ceiling_for_test(ceiling);

        reset_column_stats_concat_calls_for_test();
        let report = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(10),
                &[],
                None,
            )
            .await
            .expect("an over-ceiling part degrades rather than refusing");
        assert_eq!(
            report.column_stats_dictionaries_dropped, 1500,
            "exactly the 1500 pairs in the last 75 segments are dropped"
        );
        assert_eq!(
            column_stats_concat_calls_for_test(),
            2,
            "the degrade loop itself concatenates the part's body exactly twice \
             (once before the drop loop, once after) no matter how many \
             drops it takes -- not once per drop; the encoder's own passes \
             are outside this count"
        );

        let head = read_logs_head(store.as_ref()).await;
        let stats_ref = head.parts[0].column_stats.clone().expect("v3 ref");
        let got = store
            .get(&stats_ref.key, GetRange::Full)
            .await
            .expect("v3 object present");
        let limits = crate::snapshot_format::ColumnStatsLimits {
            max_column_stats_bytes: ceiling,
        };
        snapshot_format::decode_column_stats(&got.data, &limits)
            .expect("the degraded object decodes under the injected ceiling");
    }

    /// Issue #1482 finding 2: a v3 object keyed by the PART's own hash
    /// (`hash16`, the pre-fix code) rather than the hash of its OWN bytes
    /// collides across two folds that recompute the same byte-identical part
    /// but derive different statistics for it. Here `snapshot_part_max_entries
    /// = 2` seals hour 10's two segments (A, B) into a non-tail part on the
    /// very first fold, alongside a separate tail part for hour 11's segment
    /// (C); a non-tail part's `(min_hour, watermark_hour)` come from its own
    /// entries, not the fold's overall watermark, so this part's `.csnap`
    /// bytes -- and therefore its part hash -- never change no matter how far
    /// later folds advance. A permanent fault on every `/snap/` GET forces the
    /// second fold to rebuild from the commit layout (ADR-0063 section 4)
    /// rather than forward the byte-identical part by reference, so it
    /// genuinely re-derives that part's v3 statistics from the two segments a
    /// second time. A `Sequence` scripted onto segment A's own data key lets
    /// its first two reads (the first fold's v3 build, then the first fold's
    /// name-postings pass, which aborts immediately because a logs RLOG
    /// object is not a metrics RSEG) succeed normally, then fails the third
    /// read -- the second fold's v3 build -- permanently. That is the
    /// warn-and-omit path (fold.rs's per-entry v3 loop only logs and
    /// continues on a fetch failure), and it drops A from the second fold's
    /// v3 segments while B (a distinct key, never faulted) still succeeds:
    /// the second fold's stats object for this part covers only B, genuinely
    /// different bytes than the first fold's two-segment object for the exact
    /// same part.
    ///
    /// Reverting the fix (`stats_hash16` back to the part's own `hash16`)
    /// turns this from two objects at two keys into one: the second fold's
    /// PUT collides under `AlreadyExists` with the first fold's object
    /// already sitting at that part-hash-derived key, and the code (like the
    /// legitimate content-addressed case) treats that as "bytes are
    /// identical" and skips writing anything. The HEAD's field-7 ref still
    /// gets the SECOND fold's `blake3` (computed directly off the just-built,
    /// one-segment bytes, independent of where they landed), so the ref names
    /// a hash that the object actually stored at its key does not have --
    /// exactly the corruption this test's final assertion below would catch.
    #[tokio::test]
    async fn fold_keys_v3_object_by_its_own_content_hash_not_the_parts_hash() {
        let inner = MemoryStore::new();
        set_status_column_config(&inner).await;

        let rec_a = publish_logs_segment(&inner, 1, 10, &[200, 404]).await;
        let rec_b = publish_logs_segment(&inner, 2, 10, &[500]).await;
        publish_logs_segment(&inner, 3, 11, &[200]).await;
        let key_a = keys::reconstruct_data_key(&rec_a).expect("key a");

        let plan = FaultPlan::empty()
            .with_rule(
                Rule::new(Op::Get, ScriptedFault::Permanent("part unreadable".into()))
                    .with_key_contains("/snap/"),
            )
            .with_sequence(
                Sequence::new(Op::Get)
                    .with_key_contains(key_a.clone())
                    .then_passthrough() // first fold's v3 build reads A
                    .then_passthrough() // first fold's postings pass reads A, aborts (RLOG vs RSEG)
                    .then_fault(ScriptedFault::Permanent(
                        "segment A unreadable on rebuild".into(),
                    )), // second fold's v3 build: warn-and-omit
            );
        let store = Arc::new(FaultStore::new(inner, plan));

        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 2,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        let first = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(11),
                &[],
                None,
            )
            .await
            .expect("first fold");
        assert!(first.rebuilt, "first fold rebuilds from the commit layout");
        assert_eq!(
            first.parts_total, 2,
            "hour 10 seals off from hour 11's tail"
        );

        let head_1 = read_logs_head(store.as_ref()).await;
        let part_1 = head_1
            .parts
            .iter()
            .find(|p| p.entry_count == 2)
            .expect("the sealed two-entry part")
            .clone();
        let stats_ref_1 = part_1
            .column_stats
            .clone()
            .expect("first fold's v3 object for the sealed part");
        let stored_1 = store
            .get(&stats_ref_1.key, GetRange::Full)
            .await
            .expect("first fold's v3 object present");
        let decoded_1 = snapshot_format::decode_column_stats(
            &stored_1.data,
            &crate::snapshot_format::ColumnStatsLimits::default(),
        )
        .expect("first fold's v3 object decodes");
        assert_eq!(
            decoded_1.segments.len(),
            2,
            "first fold covers both A and B"
        );

        let second = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(12),
                &[],
                None,
            )
            .await
            .expect("second fold");
        assert!(
            second.rebuilt,
            "the /snap/ fault forces a rebuild from the commit layout"
        );
        assert!(
            store.fault_count(Op::Get, FaultKind::Permanent) >= 1,
            "at least one Permanent Get fault fired; this counter is aggregate \
             across the /snap/ rule and the sequence, so it does not alone prove \
             the sequence's third-read fault hit A -- decoded_2.segments below \
             (len 1, surviving segment is B) is what proves that"
        );

        let head_2 = read_logs_head(store.as_ref()).await;
        let part_2 = head_2
            .parts
            .iter()
            .find(|p| p.entry_count == 2)
            .expect("the same sealed two-entry part, rebuilt")
            .clone();
        assert_eq!(
            part_2.blake3, part_1.blake3,
            "byte-identical part: same entries, same non-tail (min_hour, watermark_hour)"
        );
        let stats_ref_2 = part_2
            .column_stats
            .clone()
            .expect("second fold's v3 object for the same part");

        // The bug this guards against: two different keys, not one collided
        // key. With the fix, content-addressing guarantees this on its own;
        // asserting it is what would catch a regression back to part-hash
        // keying, since fold1 and fold2 share the same part hash.
        assert_ne!(
            stats_ref_2.key, stats_ref_1.key,
            "different statistics bytes must land at different keys, even though \
             both folds computed this from the exact same part"
        );

        // First fold's object at its own key is untouched.
        let restored_1 = store
            .get(&stats_ref_1.key, GetRange::Full)
            .await
            .expect("first fold's object still present, unmodified");
        assert_eq!(
            restored_1.data, stored_1.data,
            "the first fold's v3 object must not be overwritten by the second"
        );

        // The core correctness property (issue #1482 finding 2): the field-7
        // ref's blake3 must match what is actually stored at its key. Under
        // the pre-fix part-hash keying this fails, because the second PUT is
        // skipped on a false `AlreadyExists` against the first fold's bytes.
        let stored_2 = store
            .get(&stats_ref_2.key, GetRange::Full)
            .await
            .expect("second fold's v3 object present at its declared key");
        assert_eq!(
            blake3::hash(&stored_2.data).as_bytes().to_vec(),
            stats_ref_2.blake3,
            "the ref's blake3 must describe the bytes actually stored at its key"
        );
        let decoded_2 = snapshot_format::decode_column_stats(
            &stored_2.data,
            &crate::snapshot_format::ColumnStatsLimits::default(),
        )
        .expect("second fold's v3 object decodes");
        assert_eq!(
            decoded_2.segments.len(),
            1,
            "A was dropped by the warn-and-omit path; only B survived"
        );
        assert_eq!(
            decoded_2.segments[0].writer_id, rec_b.content_hash,
            "the surviving segment is B's (entry.content_hash), not A's"
        );
    }

    /// ADR-0942: one bucket can hold two entries that cover a byte-identical
    /// output part, so they carry the same content hash under different
    /// identity tuples (compaction records with different input sets, plus a
    /// rewrite record's own parts). `encode_column_stats_v2` rejects a repeated
    /// key for the WHOLE artifact, and the fold's error arm only logs, so a
    /// single repeat would strip field 13 for that tenant on this and every
    /// later fold. Sorting alone does not prevent it; the repeat is collapsed.
    #[test]
    fn duplicate_part_content_hashes_are_collapsed_before_encoding() {
        let shared = vec![7u8; 32];
        let other = vec![9u8; 32];
        let seg = |hash: &Vec<u8>| ColumnStatsSegment {
            writer_id: hash.clone(),
            ..Default::default()
        };

        // What the pre-fix code produced: sorted, but still carrying the repeat.
        let mut sorted_only = vec![seg(&other), seg(&shared), seg(&shared)];
        sorted_only.sort_by(|a, b| a.writer_id.cmp(&b.writer_id));
        let err = snapshot_format::encode_column_stats_v2(
            [0u8; 16],
            signal::to_proto(Signal::Logs) as u32,
            Vec::new(),
            &sorted_only,
        )
        .expect_err("a repeated key is rejected for the whole artifact");
        assert!(
            matches!(
                err,
                crate::snapshot_format::SnapshotFormatError::ColumnStatsDuplicateSegment
            ),
            "expected ColumnStatsDuplicateSegment, got {err:?}"
        );

        // With the collapse the artifact encodes, and the distinct part survives.
        let mut segments = vec![seg(&other), seg(&shared), seg(&shared)];
        sort_and_dedup_part_segments(&mut segments);
        assert_eq!(
            segments.len(),
            2,
            "the repeat collapsed to one, the distinct part kept"
        );
        assert_eq!(segments[0].writer_id, shared, "sorted by content hash");
        assert_eq!(segments[1].writer_id, other);
        snapshot_format::encode_column_stats_v2(
            [0u8; 16],
            signal::to_proto(Signal::Logs) as u32,
            Vec::new(),
            &segments,
        )
        .expect("encodes once the repeat is collapsed");
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), 0, &[], None)
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
                None,
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
            .await
            .expect("first fold");
        assert_eq!(first.entry_count, 1);

        let now_2 = now_at_seal(12);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 12, now_2 - NS_PER_HOUR).await;
        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
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
            format_version: 3,
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
            .await
            .expect_err("fold must fail on a newer-format HEAD");
        assert!(
            matches!(
                err,
                CatalogError::UnsupportedHeadVersion { format_version }
                    if format_version == 3
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
            format_version: HEAD_FORMAT_VERSION_1,
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
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

    /// A HEAD below the supported format version (0: a writer that never set
    /// the field) is corrupt, not newer, so ADR-0066 decision 2 does not
    /// apply: the fold rebuilds it exactly as it rebuilds a HEAD of garbage
    /// bytes (`corrupt_head_falls_back_to_rebuild_and_recovers_all_entries`).
    #[tokio::test]
    async fn below_floor_head_version_rebuilds_like_corrupt_head() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now_1 = now_at_seal(10);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 10, now_1 - NS_PER_HOUR).await;
        catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
            .await
            .expect("first fold");

        let head_key = head_object_key(&tenant(), Signal::Metrics);
        let below_floor = SnapshotHead {
            format_version: 0,
            watermark_hour: 10,
            ..Default::default()
        };
        store
            .put(
                &head_key,
                Bytes::from(below_floor.encode_to_vec()),
                PutOptions::default(),
            )
            .await
            .expect("overwrite head with a version-0 head");

        let now_2 = now_at_seal(12);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 12, now_2 - NS_PER_HOUR).await;
        let report = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
            .await
            .expect("a below-floor HEAD is rebuilt, not refused as newer");
        assert!(report.rebuilt);
        assert!(!report.no_op);
        assert_eq!(report.watermark_hour, Some(12));
        assert_eq!(report.entry_count, 2);

        let got = store
            .get(&head_key, GetRange::Full)
            .await
            .expect("rebuilt head present");
        let head = snapshot_format::decode_head(&got.data).expect("rebuilt head decodes");
        assert_eq!(head.format_version, HEAD_FORMAT_VERSION_1);
        assert_eq!(head.watermark_hour, 12);
    }

    /// A HEAD one version above this build is still refused (ADR-0066
    /// decision 2) on a plain store, and its bytes are left untouched.
    #[tokio::test]
    async fn above_max_head_version_is_refused_and_not_overwritten() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now_1 = now_at_seal(10);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 10, now_1 - NS_PER_HOUR).await;
        catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
            .await
            .expect("first fold");

        let head_key = head_object_key(&tenant(), Signal::Metrics);
        let newer_bytes = SnapshotHead {
            format_version: 3,
            watermark_hour: 10,
            ..Default::default()
        }
        .encode_to_vec();
        store
            .put(
                &head_key,
                Bytes::from(newer_bytes.clone()),
                PutOptions::default(),
            )
            .await
            .expect("overwrite head with a newer-format head");

        let now_2 = now_at_seal(12);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 12, now_2 - NS_PER_HOUR).await;
        let err = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
            .await
            .expect_err("a newer-format HEAD must be refused");
        assert!(
            matches!(
                err,
                CatalogError::UnsupportedHeadVersion { format_version }
                    if format_version == 3
            ),
            "expected UnsupportedHeadVersion, got {err:?}"
        );

        let got = store
            .get(&head_key, GetRange::Full)
            .await
            .expect("newer head still present");
        assert_eq!(got.data.as_ref(), newer_bytes.as_slice());
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
            .await
            .expect("first fold");
        assert_eq!(first.entry_count, 1);

        let now_2 = now_at_seal(12);
        publish_segment(store.inner(), 0, Uuid::new_v4(), 1, 12, now_2 - NS_PER_HOUR).await;
        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
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
            catalog_a.fold(&tenant, Signal::Metrics, Uuid::new_v4(), now, &[], None),
            catalog_b.fold(&tenant, Signal::Metrics, Uuid::new_v4(), now, &[], None),
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
            .await
            .expect("first fold");
        assert!(!first.no_op);
        assert_eq!(first.entry_count, 1);
        assert!(first.postings_built);
        // HEAD get (absent) + 1 tenant-config read + 1 commit-record load +
        // 1 segment fetch to decode the sole entry's names.
        assert_eq!(first.get_requests, 4);

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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
            .await
            .expect("second fold");
        assert!(!second.rebuilt, "a valid HEAD must fold incrementally");
        assert!(!second.no_op);
        assert_eq!(second.entry_count, 2);
        assert!(second.postings_built);
        // HEAD get + tenant-config read + previous-part load + previous-postings
        // load + 1 new commit-record load + 1 new segment fetch = 6: the first
        // fold's "cpu" entry is neither re-loaded from the part nor re-decoded
        // from its segment.
        assert_eq!(second.get_requests, 6);
    }

    /// `FoldReport.get_requests` must equal the store's real observed GET count
    /// on every path (issue #770). Two fold GETs were charged to the discarded
    /// `QueryAccounting` handle or a bare `store.get` and never folded into the
    /// report: the tenant-config read (every fold) and, under provisioning
    /// enforcement, the generation-history read. The report is measured against
    /// an `InstrumentedStore`'s own GET counter rather than a hand-written
    /// constant, so the assertion tracks the code it checks.
    ///
    /// FLIP (pre-fix demonstration, either line reverted in `Catalog::fold`):
    /// dropping `counters.get_requests += accounting.snapshot()...` leaves the
    /// enforcement path reporting 4 against 5 observed; dropping
    /// `counters.get_requests += 1` after the tenant-config read leaves both
    /// paths reporting one below observed. Each makes an `assert_eq!` of report
    /// against observed fail.
    #[tokio::test]
    async fn fold_report_get_requests_equals_observed_store_gets() {
        // One rebuild fold over a fresh instrumented store holding a single
        // sealed segment. Returns (FoldReport.get_requests, the store's real GET
        // calls issued during the fold only). Publishing happens on the inner
        // store before it is wrapped, so only the fold's own requests are
        // measured.
        async fn measured(enforce: bool) -> (u64, u64) {
            let inner = MemoryStore::new();
            let now = now_at_seal(10);
            publish_real_segment(
                &inner,
                0,
                Uuid::new_v4(),
                1,
                10,
                now - NS_PER_HOUR,
                &["cpu"],
            )
            .await;
            let store = Arc::new(InstrumentedStore::new(inner));
            let catalog = {
                let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
                if enforce {
                    catalog.with_provisioning_enforcement()
                } else {
                    catalog
                }
            };

            let before = store.metrics().snapshot();
            let report = catalog
                .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
                .await
                .expect("fold");
            let after = store.metrics().snapshot();

            assert!(!report.no_op);
            assert!(report.rebuilt);
            (report.get_requests, after.get.calls - before.get.calls)
        }

        // Enforcement off: the report must equal the store's real GET count.
        // The scan set is a single shard 0 either way (implicit generation 0 at
        // shard_count 1), so the GETs are HEAD (absent) + tenant-config +
        // commit-record load + segment fetch = 4.
        let (off_report, off_observed) = measured(false).await;
        assert_eq!(
            off_report, off_observed,
            "with enforcement off the report must equal the store's observed GETs"
        );
        assert_eq!(off_observed, 4);

        // Enforcement on: `read_scan_generations` funnels exactly one extra GET
        // (the provisioning-record read, NotFound here but still one request)
        // through `guarded_get`. That is the GET issue #770 dropped; the report
        // must now equal the store's real count on this path too.
        let (on_report, on_observed) = measured(true).await;
        assert_eq!(
            on_report, on_observed,
            "with enforcement on the report must equal the store's observed GETs"
        );
        assert_eq!(on_observed, 5);
        assert_eq!(
            on_observed,
            off_observed + 1,
            "enforcement issues exactly one extra GET, the provisioning read"
        );
    }

    // ---- ADR-0063 T2: hour-partitioned sealed/tail parts ----

    /// The entry threshold alone, the policy with the leaf switch off.
    fn entry_cap(max_entries: usize) -> SealPolicy {
        SealPolicy {
            max_entries,
            seal_utc_days: false,
        }
    }

    /// ADR-3939 decision 1: with the leaf switch on, a part also seals where
    /// the next hour's run is on a later UTC day than the part's first hour,
    /// whether or not an entry sits on the boundary hour, and a day with no
    /// entry gets no part. The entry threshold still splits inside a day.
    /// With the switch off every input partitions as it did before the rule.
    #[test]
    fn partition_parts_seals_at_utc_days_with_the_leaf_switch() {
        const D: u32 = 24 * 19_583;
        let entries = |hours: &[u32]| -> Vec<SnapshotEntry> {
            hours
                .iter()
                .map(|&hour| SnapshotEntry {
                    ingest_hour_bucket: hour,
                    ..SnapshotEntry::default()
                })
                .collect()
        };
        let spans = |hours: &[u32], max_entries: usize, seal_utc_days: bool| {
            partition_parts(
                &entries(hours),
                SealPolicy {
                    max_entries,
                    seal_utc_days,
                },
            )
            .iter()
            .map(|s| (s.start, s.end, s.min_hour, s.watermark_hour))
            .collect::<Vec<_>>()
        };

        // Two days, an entry on the boundary hour D + 24.
        let two_days = [D + 22, D + 23, D + 24, D + 25];
        assert_eq!(
            spans(&two_days, 100, true),
            vec![(0, 2, D + 22, D + 23), (2, 4, D + 24, D + 25)]
        );
        assert_eq!(spans(&two_days, 100, false), vec![(0, 4, D + 22, D + 25)]);

        // Three days, no entry on day d + 1 and none on a boundary hour: the
        // last hour of day d, then the middle of day d + 2.
        let three_days = [D + 5, D + 23, D + 60, D + 61];
        assert_eq!(
            spans(&three_days, 100, true),
            vec![(0, 2, D + 5, D + 23), (2, 4, D + 60, D + 61)]
        );
        assert_eq!(spans(&three_days, 100, false), vec![(0, 4, D + 5, D + 61)]);

        // The entry threshold of 4 splits day d between D + 2 and D + 3; the
        // day seal then splits D + 3 from D + 30, which the threshold alone
        // keeps together.
        let threshold = [D + 1, D + 1, D + 2, D + 2, D + 3, D + 30];
        assert_eq!(
            spans(&threshold, 4, true),
            vec![
                (0, 4, D + 1, D + 2),
                (4, 5, D + 3, D + 3),
                (5, 6, D + 30, D + 30)
            ]
        );
        assert_eq!(
            spans(&threshold, 4, false),
            vec![(0, 4, D + 1, D + 2), (4, 6, D + 3, D + 30)]
        );

        // An empty entry set is one empty span either way.
        assert_eq!(spans(&[], 100, true), vec![(0, 0, 0, 0)]);
    }

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
        let empty = partition_parts(&[], entry_cap(100));
        assert_eq!(empty.len(), 1);
        assert_eq!((empty[0].start, empty[0].end), (0, 0));

        // One entry per hour, cap 1: every hour becomes its own part.
        let one_per_hour = vec![entry(10), entry(11), entry(12)];
        let spans = partition_parts(&one_per_hour, entry_cap(1));
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
        let spans = partition_parts(&dense, entry_cap(2));
        assert_eq!(spans.len(), 1);
        assert_eq!((spans[0].start, spans[0].end), (0, 3));

        // Two entries in hour 7 then one in hour 8, cap 2: hour 7 fills the
        // first part, hour 8 seals it and starts the tail.
        let mixed = vec![entry(7), entry(7), entry(8)];
        let spans = partition_parts(&mixed, entry_cap(2));
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
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
            declared_column_stats: Vec::new(),
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
            superseded_record_key: String::new(),
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

    /// Like `publish_real_segment`, but writes the real RSEG part as an L1
    /// compaction output (`keys::l1_part_key`, footer identity nil/0/0 per
    /// `build_l1_snapshot_entry`) with a `CompactionRecord` pointing at it,
    /// instead of a `CommitRecord`-addressed L0 object. Needed so a test can
    /// observe `Catalog::build_postings` actually GET and decode an L1
    /// entry, the same way it already does for L0 via
    /// `publish_real_segment`.
    async fn publish_real_compaction(
        store: &MemoryStore,
        shard: u32,
        ingest_hour_bucket: u32,
        created_unix_ns: i64,
        metrics: &[&str],
    ) -> CompactionRecord {
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
            writer_id: Uuid::nil().to_string(),
            writer_epoch: 0,
            writer_seq: 0,
        };
        let min_ingest_ts_ns = created_unix_ns - 1_000;
        let max_ingest_ts_ns = created_unix_ns;
        let bounds = IngestBounds {
            min_ingest_ts_ns,
            max_ingest_ts_ns,
        };
        let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
        let input_set_hash =
            *blake3::hash(format!("l1-inputs-{shard}-{ingest_hour_bucket}").as_bytes()).as_bytes();
        let part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xffu8; 16],
            content_hash: written.summary.blake3.to_vec(),
            object_size: written.bytes.len() as u64,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            run_count: 1,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 3,
            declared_column_stats: Vec::new(),
        };
        let record = CompactionRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics).into(),
            shard,
            ingest_hour_bucket,
            level: 1,
            inputs: Vec::new(),
            input_set_hash: input_set_hash.to_vec(),
            parts: vec![part],
            created_unix_ns,
            superseded_record_key: String::new(),
        };
        let data_key =
            keys::reconstruct_l1_part_key(&record, &record.parts[0]).expect("l1 part key");
        publish::put_data_object(store, &data_key, written.bytes)
            .await
            .expect("put l1 part object");
        let ckey = keys::compaction_record_key_for(&record).expect("compaction key");
        store
            .put(
                &ckey,
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
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

    /// Test debt (formal/tla/TRACEABILITY.md, catalog row 1, `DoFoldCas`
    /// fold-lifetime bound / `DoFoldRebase`): a fold that LISTs a bucket
    /// before a compaction supersedes one of its L0 inputs, and whose own
    /// publishing CAS lands only after that input's superseded-sweep horizon
    /// has passed (so a real deployment's maintenance process would already
    /// have physically deleted it), must never publish a snapshot that names
    /// the swept input. Nothing inside `Catalog::fold` samples a clock or
    /// times its own duration (module docs above: "this crate never reads a
    /// clock"); the guarantee comes from `DoFoldRebase`, the HEAD CAS retry
    /// loop's `PreconditionFailed`/`AlreadyExists` branch: a delayed fold's
    /// CAS loses to whichever fold's fresher listing published first, and the
    /// loser re-reads HEAD and takes the no-op path instead of ever
    /// publishing its own stale listing. A `FaultStore` hold on the delayed
    /// fold's HEAD PUT freezes it after its LIST and before its CAS so a
    /// second, unheld fold can win the race while the compaction and the
    /// sweep both land in between.
    #[tokio::test]
    async fn fold_that_lists_before_a_compaction_and_cas_after_the_sweep_horizon_does_not_name_a_swept_input()
     {
        let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let cfg = CatalogConfig {
            shard_count: 1,
            snapshot_part_max_entries: 1,
            ..Default::default()
        };
        let catalog = Arc::new(Catalog::new(store.clone(), cfg).expect("catalog"));

        // Hour 10 seals into its own part; hour 11 is the tail. Fold A's LIST
        // of hour 10 sees only the raw L0 segment: no compaction exists yet.
        let now_1 = now_at_seal(11);
        let writer_a = Uuid::new_v4();
        let seg_a =
            publish_segment(store.inner(), 0, writer_a, 1, 10, now_1 - 2 * NS_PER_HOUR).await;
        publish_segment(store.inner(), 0, Uuid::new_v4(), 1, 11, now_1 - NS_PER_HOUR).await;

        // Hold fold A's HEAD PUT: HEAD is absent, so this is the very first
        // (and only, until released) call that matches. Fold A's LIST has
        // already completed by the time its PUT is attempted, so the hold
        // freezes it exactly after LIST and before CAS.
        let gate = store.hold(Op::Put, Some("HEAD".to_string()), Occurrence::Nth(1));
        let cat_a = catalog.clone();
        let fold_a = tokio::spawn(async move {
            cat_a
                .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
                .await
        });
        gate.wait_until_held(1).await;
        assert_eq!(gate.held_count(), 1, "fold A is blocked on its HEAD CAS");

        // A compaction lands while fold A is paused, superseding seg_a's L0
        // with one L1 part.
        publish_compaction(store.inner(), 0, 10, &[&seg_a], now_1).await;

        // Simulate the superseded-input sweep: once `protection_horizon_ns`
        // has elapsed past the compaction's own `created_unix_ns`, a real
        // maintenance process would physically delete seg_a's commit record
        // and data object (docs/deletion-and-gc.md's superseded-input sweep
        // row; the delete blocker there is vacuous while HEAD is still
        // absent, which is exactly fold A's paused state). Do that directly
        // here (ravel-maintain is out of this task's scope) so fold B below
        // runs, and fold A resumes, against a store where the swept input is
        // already gone, not merely superseded.
        let commit_key = keys::commit_key_for_record(&seg_a).expect("commit key");
        let data_key = keys::reconstruct_data_key(&seg_a).expect("data key");
        store
            .inner()
            .delete(&commit_key)
            .await
            .expect("delete swept commit record");
        store
            .inner()
            .delete(&data_key)
            .await
            .expect("delete swept data object");

        // Fold B: its own LIST of hour 10 sees the compaction directly (no
        // reconcile pass needed, this is also a first/rebuild fold since HEAD
        // is still absent while fold A is paused). Its CAS is not gated by
        // the hold (`Occurrence::Nth(1)` matched only fold A's attempt), so
        // it wins the race and publishes HEAD naming the L1 part, well past
        // the sweep horizon.
        let now_2 = now_1 + cfg.protection_horizon_ns + NS_PER_HOUR;
        let second = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
            .await
            .expect("fold B must not fail even though the swept input is already gone");
        assert!(!second.no_op);
        assert!(second.rebuilt, "HEAD was still absent when fold B listed");

        // Release fold A: its own HEAD PUT now loses (`AlreadyExists`, HEAD
        // was created by fold B while fold A was held), so it must rebase
        // onto fold B's HEAD and take the no-op path rather than publish its
        // own stale listing that still names seg_a.
        for id in gate.held() {
            gate.release(id);
        }
        let first = fold_a
            .await
            .expect("join fold A")
            .expect("fold A must rebase, not error");
        assert!(
            first.no_op,
            "fold A must rebase onto fold B's HEAD, never publish its stale listing"
        );

        let head = read_head(store.inner()).await;
        let entries = collect_head_entries(store.inner(), &head).await;
        let hour10: Vec<&SnapshotEntry> = entries
            .iter()
            .filter(|e| e.ingest_hour_bucket == 10)
            .collect();
        assert_eq!(hour10.len(), 1, "hour 10 now has exactly the L1 part");
        assert_eq!(hour10[0].level, 1, "hour 10's entry is the compaction L1");
        assert!(
            !entries.iter().any(|e| e.content_hash == seg_a.content_hash),
            "the published fold must not name the swept input"
        );
    }

    /// Regression for issue #587: a fold used to skip postings entirely the
    /// moment any L1 (compacted) entry was present, because
    /// `fetch_segment_names` could only key and identity-check an L0 object.
    /// A bucket set mixing one real L0 segment and one real L1 compaction
    /// part must still build postings and cover both entries.
    #[tokio::test]
    async fn mixed_l0_l1_entries_still_build_postings() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let now = now_at_seal(10);
        publish_real_segment(
            &store,
            0,
            Uuid::new_v4(),
            1,
            5,
            now - 5 * NS_PER_HOUR,
            &["cpu"],
        )
        .await;
        publish_real_compaction(&store, 0, 8, now - 2 * NS_PER_HOUR, &["disk"]).await;

        let report = catalog
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
            .await
            .expect("fold");

        assert!(!report.no_op);
        assert!(report.rebuilt);
        assert_eq!(report.entry_count, 2);
        assert!(
            report.postings_built,
            "postings must be built over a mixed L0/L1 entry set, not skipped"
        );

        let head = read_head(store.as_ref()).await;
        assert!(head.postings.is_some());
        let entries = collect_head_entries(store.as_ref(), &head).await;
        let mut levels: Vec<u32> = entries.iter().map(|e| e.level).collect();
        levels.sort_unstable();
        assert_eq!(levels, vec![0, 1], "entry set is genuinely mixed L0/L1");
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
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
                None,
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
                None,
            )
            .await
            .expect("a third fold must also succeed, not fail forever");
        assert!(!third.no_op);
    }

    /// A late record landing OUTSIDE the reconcile window (older than
    /// `watermark_hour_old - fold_reconcile_window_hours`) is still NOT picked
    /// up by a fold that was not asked to look there.
    ///
    /// This is no longer an unconditional invariant, and the distinction is
    /// the whole point of issue #526's re-fold half. What the window bounds is
    /// what the fold discovers ON ITS OWN: it cannot tell which old hour
    /// received a late record without listing that hour's buckets, and the
    /// hours that could have received one are every hour the snapshot names,
    /// so a self-derived pass would cost a full-history LIST fan-out per fold.
    /// The bounded staleness this test pins is therefore the staleness of the
    /// UNREQUESTED fold, and it is the reason a caller that DOES know (the
    /// superseded-input sweep, which lists those buckets anyway and holds the
    /// hour's pre-rewrite inputs on its HEAD-reachability gate) has
    /// [`Catalog::fold_with_refold_request`] to say so.
    ///
    /// Held deliberately, not by omission: `refold_request_applies_out_of_
    /// window_compaction` below is this exact scenario plus the request, and
    /// it asserts the opposite outcome.
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
                None,
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
                None,
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

    // ---- issue #526: targeted re-fold of hours that received a late record ----

    /// The object key a snapshot entry names, derived level-aware exactly as
    /// [`fetch_segment_names`] derives it for a fetch. Lets a test assert the
    /// exact set of objects a folded snapshot still names, rather than a count.
    fn entry_object_key(entry: &SnapshotEntry) -> String {
        if entry.level == 0 {
            let writer_id: [u8; 16] = entry
                .writer_id
                .as_slice()
                .try_into()
                .expect("a level-0 entry carries a 16-byte writer id");
            let content_hash: [u8; 32] = entry
                .content_hash
                .as_slice()
                .try_into()
                .expect("an entry carries a 32-byte content hash");
            keys::data_key(
                &tenant(),
                Signal::Metrics,
                entry.shard,
                Uuid::from_bytes(writer_id),
                entry.writer_epoch,
                entry.writer_seq,
                &content_hash,
            )
            .expect("data key")
        } else {
            keys::l1_part_key(
                &tenant(),
                Signal::Metrics,
                entry.shard,
                entry.ingest_hour_bucket,
                &hex::encode(&entry.writer_id[..8]),
                u32::try_from(entry.writer_epoch).expect("part index fits u32"),
                &hex::encode(&entry.content_hash[..8]),
            )
            .expect("l1 part key")
        }
    }

    /// Every object key the current HEAD's snapshot names, as a set.
    async fn head_object_keys(store: &dyn ObjectStoreBackend) -> BTreeSet<String> {
        collect_head_entries(store, &read_head(store).await)
            .await
            .iter()
            .map(entry_object_key)
            .collect()
    }

    /// Publish a selective-erasure rewrite record (ADR-0064 decision 3) into
    /// `(shard, ingest_hour_bucket)` that supersedes the given L0 `inputs` and
    /// contributes one L1 output part. Like `publish_compaction`, only the
    /// record object is written: the fold reads the record, not the part.
    async fn publish_rewrite(
        store: &MemoryStore,
        shard: u32,
        ingest_hour_bucket: u32,
        inputs: &[&CommitRecord],
        created_unix_ns: i64,
    ) -> RewriteRecord {
        let mut input_ids: Vec<CompactionInputIdentity> = inputs
            .iter()
            .map(|r| CompactionInputIdentity {
                writer_id: r.writer_id.clone(),
                writer_epoch: r.writer_epoch,
                writer_seq: r.writer_seq,
            })
            .collect();
        // `validate_rewrite` requires the inputs in this order and recomputes
        // the input-set hash over them.
        input_ids.sort_by(|a, b| {
            (a.writer_id.as_str(), a.writer_epoch, a.writer_seq).cmp(&(
                b.writer_id.as_str(),
                b.writer_epoch,
                b.writer_seq,
            ))
        });
        let request_ids = vec![Uuid::new_v4().to_string()];
        let input_set_hash =
            erasure::compute_rewrite_input_set_hash(&input_ids, None, &request_ids).to_vec();
        let part_payload = format!("rw-{shard}-{ingest_hour_bucket}").into_bytes();
        let part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xffu8; 16],
            content_hash: blake3::hash(&part_payload).as_bytes().to_vec(),
            object_size: part_payload.len() as u64,
            sample_count: 1,
            series_count: 1,
            run_count: 1,
            min_event_ts_ns: created_unix_ns - 1_000,
            max_event_ts_ns: created_unix_ns,
            segment_format_version: 3,
            declared_column_stats: Vec::new(),
        };
        let record = RewriteRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as i32,
            shard,
            ingest_hour_bucket,
            inputs: input_ids,
            input_set_hash,
            parts: vec![part],
            drops: request_ids
                .iter()
                .map(|request_id| RewriteDrop {
                    request_id: request_id.clone(),
                    dropped_count: 1,
                })
                .collect(),
            created_unix_ns,
            superseded_record_key: String::new(),
        };
        let key = keys::rewrite_record_key_for(&record).expect("rewrite key");
        store
            .put(
                &key,
                erasure::encode_rewrite(&record),
                PutOptions::default(),
            )
            .await
            .expect("put rewrite record");
        record
    }

    /// Deliverable 1: an hour that received a late COMPACTION record outside
    /// the fixed reconcile window is re-folded when a request names it, and
    /// the resulting snapshot stops naming that hour's pre-rewrite inputs.
    /// Same setup as `reconcile_ignores_late_record_outside_window`, whose
    /// unrequested fold leaves hour 5 naming the superseded L0.
    ///
    /// To watch this FAIL against the pre-fix behaviour, empty the targeted
    /// pass's slice in `fold_inner`: `let refold_hours = &candidates[..0];`.
    /// The pass then lists nothing, hour 5 keeps naming `seg_x`, and the exact
    /// key-set assertion below fails. (`&candidates[..take]` is the shipped
    /// line itself, so quoting that changes nothing and the test still
    /// passes.)
    #[tokio::test]
    async fn refold_request_applies_out_of_window_compaction() {
        let store = Arc::new(MemoryStore::new());
        // Default window is 26 hours, so the fixed pass covers [14, 40] only.
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let seg_x = publish_segment(&store, 0, Uuid::new_v4(), 1, 5, 6 * NS_PER_HOUR).await;
        let seg_recent = publish_segment(&store, 0, Uuid::new_v4(), 1, 40, 41 * NS_PER_HOUR).await;
        let first = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(40),
                &[],
                None,
            )
            .await
            .expect("first fold");
        assert_eq!(first.entry_count, 2);

        let compaction = publish_compaction(&store, 0, 5, &[&seg_x], 6 * NS_PER_HOUR).await;

        let second = catalog
            .fold_with_refold_request(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(41),
                &[],
                None,
                &RefoldRequest::from_hours([5]),
            )
            .await
            .expect("second fold");
        assert!(!second.no_op, "the watermark advanced 40 -> 41");
        assert!(!second.rebuilt, "an incremental fold, not a rebuild");

        let expected: BTreeSet<String> = [
            keys::reconstruct_l1_part_key(&compaction, &compaction.parts[0]).expect("l1 part key"),
            keys::reconstruct_data_key(&seg_recent).expect("data key"),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            head_object_keys(store.as_ref()).await,
            expected,
            "hour 5 must now name the compaction output, and no longer its superseded L0 input"
        );
        assert_eq!(second.refold_hours_reconciled, 1);
    }

    /// Deliverable 1, the rewrite half: the same for a late selective-erasure
    /// REWRITE record (ADR-0064 decision 3). This is the shape issue #526
    /// names, because a rewrite always lands in an already-sealed bucket, so a
    /// rewrite outside the window is never picked up by any later fold at all.
    ///
    /// Flip the same line as `refold_request_applies_out_of_window_compaction`
    /// (`let refold_hours = &candidates[..0];`) to watch it fail: the snapshot
    /// keeps naming the pre-erasure L0.
    #[tokio::test]
    async fn refold_request_applies_out_of_window_rewrite() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let seg_x = publish_segment(&store, 0, Uuid::new_v4(), 1, 5, 6 * NS_PER_HOUR).await;
        let seg_recent = publish_segment(&store, 0, Uuid::new_v4(), 1, 40, 41 * NS_PER_HOUR).await;
        catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(40),
                &[],
                None,
            )
            .await
            .expect("first fold");

        let rewrite = publish_rewrite(&store, 0, 5, &[&seg_x], 6 * NS_PER_HOUR).await;

        let second = catalog
            .fold_with_refold_request(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(41),
                &[],
                None,
                &RefoldRequest::from_hours([5]),
            )
            .await
            .expect("second fold");
        assert!(!second.rebuilt, "an incremental fold, not a rebuild");

        let expected: BTreeSet<String> = [
            keys::reconstruct_rewrite_part_key(&rewrite, &rewrite.parts[0])
                .expect("rewrite part key"),
            keys::reconstruct_data_key(&seg_recent).expect("data key"),
        ]
        .into_iter()
        .collect();
        assert_eq!(
            head_object_keys(store.as_ref()).await,
            expected,
            "hour 5 must now name the rewrite output, and no longer its pre-erasure L0 input"
        );
        assert_eq!(second.refold_hours_reconciled, 1);
    }

    /// ADR-0063 section 4 carve-out 1: a FIRST fold skips the reconcile pass
    /// entirely (no previous watermark exists to anchor a window to). A
    /// re-fold request must not make it do reconcile work anyway. The
    /// requested hour is not even sealed-and-folded yet, so listing it would
    /// be work the rebuild-shaped first fold already does.
    #[tokio::test]
    async fn refold_request_is_ignored_on_a_first_fold() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let seg_x = publish_segment(&store, 0, Uuid::new_v4(), 1, 5, 6 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 1, 40, 41 * NS_PER_HOUR).await;
        publish_compaction(&store, 0, 5, &[&seg_x], 6 * NS_PER_HOUR).await;

        let first = catalog
            .fold_with_refold_request(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(40),
                &[],
                None,
                &RefoldRequest::from_hours([5]),
            )
            .await
            .expect("first fold");
        assert!(!first.no_op);
        assert_eq!(
            first.refold_hours_reconciled, 0,
            "a first fold does no reconcile work, requested or not"
        );
        assert_eq!(
            first.frontier_hours_reconciled, 0,
            "and no frontier work either, which the carve-out also covers"
        );
        // The first fold derives hour 5 from the commit layout, compaction
        // record included, so the request had nothing to add.
        assert_eq!(first.entry_count, 2);
    }

    /// #1763 part (a), the #1781 review item: a [`RefoldRequest`] submitted
    /// to a fold call that turns out to be a no-op (nothing newly sealed
    /// beyond the previous watermark) reconciles zero hours, not the count
    /// of hours the request named. The targeted pass sits inside the same
    /// reconcile branch as the fixed window and the frontier band, and that
    /// branch never runs once the top-of-loop watermark check has already
    /// returned a no-op report -- the request is never denied, it is never
    /// reached.
    ///
    /// Prove-the-test: hardcode `no_op_report`'s `refold_hours_reconciled` to
    /// `1` and the final assertion below fails.
    #[tokio::test]
    async fn refold_request_to_a_no_op_fold_reconciles_zero_hours_and_reports_it() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        publish_segment(&store, 0, Uuid::new_v4(), 1, 5, 6 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 1, 40, 41 * NS_PER_HOUR).await;
        catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(40),
                &[],
                None,
            )
            .await
            .expect("first fold");

        // Same `now_ns` as the first fold: the watermark cannot advance, so
        // this call is a no-op regardless of what the request names. Hour 5
        // is a real, snapshot-named hour a reachable targeted pass would
        // happily reconcile.
        let second = catalog
            .fold_with_refold_request(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(40),
                &[],
                None,
                &RefoldRequest::from_hours([5]),
            )
            .await
            .expect("second fold");
        assert!(second.no_op, "the watermark did not advance");
        assert_eq!(
            second.refold_hours_reconciled, 0,
            "a request against a no-op fold reconciles nothing, however many hours it names"
        );
    }

    /// ADR-0063 section 4 carve-out 2: a REBUILD skips the reconcile pass (it
    /// re-derives every hour from the commit layout already). A re-fold
    /// request must not add a second listing of hours the rebuild just read.
    #[tokio::test]
    async fn refold_request_is_ignored_on_a_rebuild() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let seg_x = publish_segment(&store, 0, Uuid::new_v4(), 1, 5, 6 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 1, 40, 41 * NS_PER_HOUR).await;
        catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(40),
                &[],
                None,
            )
            .await
            .expect("first fold");

        // Corrupt HEAD in place so the next fold rebuilds.
        store
            .put(
                &head_object_key(&tenant(), Signal::Metrics),
                Bytes::from_static(b"not a head"),
                PutOptions::default(),
            )
            .await
            .expect("overwrite head with garbage");
        publish_compaction(&store, 0, 5, &[&seg_x], 6 * NS_PER_HOUR).await;

        let second = catalog
            .fold_with_refold_request(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(41),
                &[],
                None,
                &RefoldRequest::from_hours([5]),
            )
            .await
            .expect("fold after corruption");
        assert!(second.rebuilt, "a rebuild, not an incremental fold");
        assert_eq!(
            second.refold_hours_reconciled, 0,
            "a rebuild does no reconcile work, requested or not"
        );
        // The rebuild picks the late compaction up on its own, from the same
        // listing it performs for every hour.
        let hour5: Vec<SnapshotEntry> =
            collect_head_entries(store.as_ref(), &read_head(store.as_ref()).await)
                .await
                .into_iter()
                .filter(|e| e.ingest_hour_bucket == 5)
                .collect();
        assert_eq!(hour5.len(), 1);
        assert_eq!(hour5[0].level, 1);
    }

    /// Bounded work: one late record means exactly ONE hour is re-folded, not
    /// every hour the snapshot names, and not every hour on every later tick.
    ///
    /// The LIST arithmetic is asserted exactly, because the hour count alone
    /// would not catch a pass that re-listed the whole history and then
    /// reported only the hours it changed. For the second fold below
    /// (`watermark_hour_old` 40, new watermark 41, one shard, no tenant
    /// retention window so the frontier pass does not run): 1 incremental
    /// bucket for hour 41, 27 for the fixed window `[14, 40]`, and 1 for the
    /// requested hour 3.
    /// The cap truncates oldest-first and carries the remainder, rather than
    /// dropping it or deferring it into the snapshot. The requester re-derives
    /// its blocked set each pass, so an hour the cap cut out comes back on the
    /// next request.
    ///
    /// The cap is read from the runtime config, so this case sets it to 2 and
    /// requests 4 hours rather than needing 169 of them. That is also what
    /// makes the config read testable at all: with the previous hardcoded
    /// const, reaching the branch meant building a 169-hour snapshot.
    ///
    /// Flip to watch it fail: drop the `.min(cap)` from `take`. All four
    /// requested hours are then re-folded and the count below reads 4.
    #[tokio::test]
    async fn refold_request_is_capped_oldest_first_and_carries_the_remainder() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                shard_count: 1,
                frontier_reconcile_max_hours: 2,
                ..Default::default()
            },
        )
        .expect("catalog");

        let mut old_segments = Vec::new();
        for hour in [1u32, 2, 3, 5] {
            old_segments.push(
                publish_segment(
                    &store,
                    0,
                    Uuid::new_v4(),
                    1,
                    hour,
                    (i64::from(hour) + 1) * NS_PER_HOUR,
                )
                .await,
            );
        }
        publish_segment(&store, 0, Uuid::new_v4(), 1, 40, 41 * NS_PER_HOUR).await;
        catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(40),
                &[],
                None,
            )
            .await
            .expect("first fold");

        // Every one of the four old hours receives a late record, so all four
        // are genuine candidates and the cap is what limits the pass.
        for (i, hour) in [1u32, 2, 3, 5].iter().enumerate() {
            publish_compaction(
                &store,
                0,
                *hour,
                &[&old_segments[i]],
                (i64::from(*hour) + 1) * NS_PER_HOUR,
            )
            .await;
        }

        let capped = catalog
            .fold_with_refold_request(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(41),
                &[],
                None,
                &RefoldRequest::from_hours([1, 2, 3, 5]),
            )
            .await
            .expect("capped fold");
        assert_eq!(
            capped.refold_hours_reconciled, 2,
            "the cap of 2 bounds the pass, not the four requested hours"
        );

        // The remainder is not lost: requesting it again re-folds it, which is
        // the property that lets the pass defer nothing into the snapshot.
        let remainder = catalog
            .fold_with_refold_request(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(42),
                &[],
                None,
                &RefoldRequest::from_hours([3, 5]),
            )
            .await
            .expect("remainder fold");
        assert_eq!(
            remainder.refold_hours_reconciled, 2,
            "the hours the cap cut out are re-foldable on the next request"
        );
    }

    #[tokio::test]
    async fn refold_request_re_lists_only_the_requested_hours() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        // Five snapshot-named hours, all but hour 40 far outside the window.
        let mut old_segments = Vec::new();
        for hour in [1u32, 2, 3, 5] {
            old_segments.push(
                publish_segment(
                    &store,
                    0,
                    Uuid::new_v4(),
                    1,
                    hour,
                    (i64::from(hour) + 1) * NS_PER_HOUR,
                )
                .await,
            );
        }
        publish_segment(&store, 0, Uuid::new_v4(), 1, 40, 41 * NS_PER_HOUR).await;
        let first = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(40),
                &[],
                None,
            )
            .await
            .expect("first fold");
        assert_eq!(first.entry_count, 5);

        // Exactly one hour receives a late record.
        publish_compaction(&store, 0, 3, &[&old_segments[2]], 4 * NS_PER_HOUR).await;

        let second = catalog
            .fold_with_refold_request(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(41),
                &[],
                None,
                &RefoldRequest::from_hours([3]),
            )
            .await
            .expect("second fold");
        assert_eq!(
            second.refold_hours_reconciled, 1,
            "exactly the one requested hour is re-folded"
        );
        assert_eq!(
            second.list_requests,
            1 + 27 + 1,
            "hour 41 incrementally, the 27-hour fixed window [14, 40], and hour 3"
        );

        // A later tick with nothing to request re-lists no extra hour at all:
        // the pass is driven by the request, never by the snapshot's size.
        let third = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(42),
                &[],
                None,
            )
            .await
            .expect("third fold");
        assert_eq!(third.refold_hours_reconciled, 0);
        assert_eq!(
            third.list_requests,
            1 + 27,
            "hour 42 incrementally plus the fixed window [15, 41]; no re-fold LISTs"
        );
    }

    /// The pass declines the hours it cannot usefully re-fold: one inside the
    /// fixed window (which lists it anyway), one above the old watermark
    /// (which the incremental range folds fresh), and one no snapshot entry
    /// names (which cannot be blocking a delete on HEAD reachability). A
    /// request of only such hours costs no LIST at all.
    #[tokio::test]
    async fn refold_request_skips_hours_the_other_passes_cover() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        publish_segment(&store, 0, Uuid::new_v4(), 1, 5, 6 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 1, 20, 21 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 1, 40, 41 * NS_PER_HOUR).await;
        catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(40),
                &[],
                None,
            )
            .await
            .expect("first fold");

        let second = catalog
            .fold_with_refold_request(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(41),
                &[],
                None,
                // 20: inside the fixed window [14, 40]. 41: the incremental
                // range. 4: no snapshot entry names it.
                &RefoldRequest::from_hours([4, 20, 41]),
            )
            .await
            .expect("second fold");
        assert_eq!(second.refold_hours_reconciled, 0);
        assert_eq!(
            second.list_requests,
            1 + 27,
            "hour 41 incrementally plus the fixed window [14, 40], and nothing more"
        );
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
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
                None,
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
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
                None,
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
                None,
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
                None,
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

    /// Run the out-of-window frontier scenario (hour 5 retired far behind an
    /// hour-200 watermark) with a chosen `TenantConfig.retention_ns` state and a
    /// chosen deployment default, and return the second fold's
    /// `frontier_hours_reconciled`. `tenant_config` is `None` to write no config
    /// record at all, `Some(inner)` to write a record whose `retention_ns` is
    /// `inner`. `default_retention_ns` is the value the caller (the production
    /// fold loop) resolves from `RetentionConfig::window_for` and passes into
    /// `Catalog::fold`; it is passed to both folds, as a real tick would.
    async fn frontier_reconciled_with(
        tenant_config: Option<Option<i64>>,
        default_retention_ns: Option<i64>,
    ) -> u64 {
        let store = Arc::new(MemoryStore::new());
        let cfg = CatalogConfig {
            shard_count: 1,
            frontier_reconcile_max_hours: 168,
            ..Default::default()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");

        if let Some(inner) = tenant_config {
            let tc = crate::tenant_config::TenantConfig {
                retention_ns: inner,
                ..crate::tenant_config::TenantConfig::new(
                    crate::tenant_config::TenantLifecycleState::Active,
                )
            };
            crate::tenant_config::set_tenant_config(store.as_ref(), &tenant(), &tc, 1)
                .await
                .expect("write tenant config");
        }

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
        catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(200),
                &[],
                default_retention_ns,
            )
            .await
            .expect("first fold");

        publish_tombstone(&store, 0, 5).await;

        let second = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_at_seal(202),
                &[],
                default_retention_ns,
            )
            .await
            .expect("second fold");
        second.frontier_hours_reconciled
    }

    /// The retention-window overlay (ADR-0078, deliverable 2): the frontier
    /// reconcile resolves its window as the durable `TenantConfig.retention_ns`
    /// override when present, else the caller-supplied deployment default. A
    /// window of 200h retires hour 5 (frontier ~hour 28); a window of 100_000h
    /// pushes the frontier before the epoch, so `retirement_frontier_hour`
    /// returns `None` and no frontier reconcile runs. That gap makes each
    /// precedence branch observable through `frontier_hours_reconciled`.
    ///
    /// To watch this FAIL against pre-fix code, restore the old resolution
    /// (`Ok(None) => None`, `Err(..) => None`, and `Ok(Some(cfg)) =>
    /// cfg.retention_ns` without `.or(default_retention_ns)`): the two
    /// deployment-default-only cases below then report 0 reconciled hours.
    #[tokio::test]
    async fn retention_overlay_precedence() {
        const RETIRES: i64 = 200 * NS_PER_HOUR;
        // A window so large the retirement frontier predates the epoch, so no
        // frontier pass runs even though a window is set.
        const NO_FRONTIER: i64 = 100_000 * NS_PER_HOUR;

        // 1. TenantConfig override present wins over the deployment default:
        //    override retires (200h) while the default would not (NO_FRONTIER).
        assert!(
            frontier_reconciled_with(Some(Some(RETIRES)), Some(NO_FRONTIER)).await >= 1,
            "the TenantConfig override must win over the deployment default"
        );

        // 2. No TenantConfig record at all: the deployment default applies.
        assert!(
            frontier_reconciled_with(None, Some(RETIRES)).await >= 1,
            "the deployment default must apply when no TenantConfig record exists"
        );

        // 3. TenantConfig record present but retention_ns is None: the
        //    deployment default applies (the record does not veto the default).
        assert!(
            frontier_reconciled_with(Some(None), Some(RETIRES)).await >= 1,
            "the deployment default must apply when TenantConfig.retention_ns is None"
        );

        // 4. Neither a per-tenant override nor a deployment default: no frontier
        //    reconcile, identical to pre-ADR-0078 behavior.
        assert_eq!(
            frontier_reconciled_with(None, None).await,
            0,
            "no override and no default means no frontier reconcile"
        );
        assert_eq!(
            frontier_reconciled_with(Some(None), None).await,
            0,
            "a None-valued TenantConfig record with no default still means no reconcile"
        );
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
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
                None,
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_2, &[], None)
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
            .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now_1, &[], None)
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
                None,
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

    /// The fold-liveness gauge and cycle counter (issue #1306). A successful
    /// fold stamps the caller's `now_ns` into `fold_last_success_unix_ns`
    /// exactly, and counts exactly one cycle. Exact equality, not a band: the
    /// value IS the injected clock reading the caller passed in, since this
    /// crate reads no clock of its own.
    #[tokio::test]
    async fn successful_fold_stamps_last_success_and_counts_one_cycle() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        assert_eq!(catalog.fold_cycles(Signal::Metrics), 0);
        assert_eq!(catalog.fold_failures(Signal::Metrics), 0);
        assert_eq!(
            catalog.fold_last_success_unix_ns(Signal::Metrics),
            0,
            "no fold has succeeded yet, so the gauge is the zero sentinel"
        );

        let now_ns = now_at_seal(11);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 11, now_ns - NS_PER_HOUR).await;
        let report = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_ns,
                &[],
                None,
            )
            .await
            .expect("fold succeeds");
        assert!(!report.no_op, "the fold folded the sealed hour");

        assert_eq!(catalog.fold_cycles(Signal::Metrics), 1);
        assert_eq!(catalog.fold_failures(Signal::Metrics), 0);
        assert_eq!(
            catalog.fold_last_success_unix_ns(Signal::Metrics),
            now_ns,
            "the gauge is exactly the now_ns the caller folded at"
        );

        // A second successful fold at a later clock reading advances the gauge
        // to that reading and counts a second cycle. This one is a no-op fold
        // (nothing new has sealed), which is the healthy steady state: the
        // gauge must move for it, or a quiet tenant reads as a stopped fold.
        let later_ns = now_ns + 7_000_000_000;
        let second = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                later_ns,
                &[],
                None,
            )
            .await
            .expect("second fold succeeds");
        assert!(second.no_op, "nothing newly sealed between the two folds");
        assert_eq!(catalog.fold_cycles(Signal::Metrics), 2);
        assert_eq!(catalog.fold_failures(Signal::Metrics), 0);
        assert_eq!(catalog.fold_last_success_unix_ns(Signal::Metrics), later_ns);
    }

    /// The other half of the same claim (issue #1306): a fold that returns
    /// `Err` counts exactly one failure and leaves the liveness gauge where
    /// the last SUCCESS left it. Without this half, a gauge that advanced on
    /// every call regardless of outcome would pass the success test above and
    /// still report a fold failing every cycle as healthy.
    #[tokio::test]
    async fn failing_fold_counts_a_failure_and_leaves_the_gauge_where_it_was() {
        // Fail the second HEAD PUT: the first fold establishes a real HEAD and
        // a real gauge value, the second fold's CAS write fails. No other PUT
        // key contains "HEAD".
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("head cas failure".into()))
                .with_key_contains("HEAD")
                .with_occurrence(Occurrence::Nth(2)),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let first_ns = now_at_seal(11);
        publish_segment(
            store.inner(),
            0,
            Uuid::new_v4(),
            1,
            11,
            first_ns - NS_PER_HOUR,
        )
        .await;
        catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                first_ns,
                &[],
                None,
            )
            .await
            .expect("first fold succeeds");
        assert_eq!(catalog.fold_cycles(Signal::Metrics), 1);
        assert_eq!(catalog.fold_last_success_unix_ns(Signal::Metrics), first_ns);

        let failing_ns = now_at_seal(13);
        publish_segment(
            store.inner(),
            0,
            Uuid::new_v4(),
            1,
            13,
            failing_ns - NS_PER_HOUR,
        )
        .await;
        let err = catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                failing_ns,
                &[],
                None,
            )
            .await
            .expect_err("the HEAD CAS fault must surface as an error");
        assert!(matches!(err, CatalogError::Store(_)), "got {err:?}");

        // The fault fired exactly once, so exactly one fold failed.
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 1);
        assert_eq!(catalog.fold_failures(Signal::Metrics), 1);
        assert_eq!(
            catalog.fold_cycles(Signal::Metrics),
            1,
            "the failing fold is not a cycle: the cycle count is still the one successful fold"
        );
        assert_eq!(
            catalog.fold_last_success_unix_ns(Signal::Metrics),
            first_ns,
            "a failing fold must not advance the liveness gauge, even though its now_ns is later"
        );
    }

    /// A plain `store`, not `fetch_max`: the liveness gauge holds the stamp of
    /// the last fold to FINISH, even when that stamp is older than the one the
    /// slot already carried. This pins the semantics deliberately.
    ///
    /// The two calls below model out-of-order completion, which is the way one
    /// signal's slot goes backwards with no clock anomaly at all. Each stamp
    /// is the `now_ns` read before its fold began, and folds of one signal
    /// overlap in the server (the scheduled loop for that signal re-reads the
    /// clock per tenant while an on-demand fold for another tenant of the same
    /// signal runs against the same `Catalog`), so a fold that started later
    /// and finished sooner is overwritten by the slower one completing behind
    /// it. Calling `fold(ahead)` then `fold(behind)` is that completion order.
    /// An NTP step back produces the same store.
    ///
    /// Under `fetch_max` the second store would be dropped and the gauge would
    /// stay at `ahead_ns`, which is the assertion that fails below. That is
    /// the failure mode being rejected: `fetch_max` latches a forward step and
    /// masks a later genuine stall with no bound on how long.
    #[tokio::test]
    async fn an_out_of_order_fold_completion_lowers_the_last_success_gauge() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let ahead_ns = now_at_seal(11);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 11, ahead_ns - NS_PER_HOUR).await;
        catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                ahead_ns,
                &[],
                None,
            )
            .await
            .expect("fold stamped at the later reading");
        assert_eq!(catalog.fold_last_success_unix_ns(Signal::Metrics), ahead_ns);

        let behind_ns = ahead_ns - 60_000_000_000;
        catalog
            .fold(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                behind_ns,
                &[],
                None,
            )
            .await
            .expect("fold stamped at the earlier reading finishes second");
        assert_eq!(
            catalog.fold_last_success_unix_ns(Signal::Metrics),
            behind_ns,
            "the gauge holds the stamp of the last fold to finish, even an older one"
        );
        assert_eq!(
            catalog.fold_cycles(Signal::Metrics),
            2,
            "both folds are still cycles"
        );
    }

    /// Per-signal keying (issue #1306): folding one signal moves that signal's
    /// counters and gauge and leaves every other signal's exactly where they
    /// were. Without this, the three families are process-global and one dead
    /// fold loop out of three stays invisible behind the two that still run.
    ///
    /// `Signal::Profiles` is asserted alongside the two other folded signals
    /// because the slot table covers every `Signal` variant, not just the
    /// three the server folds, and an off-by-one there would alias two of them
    /// onto one slot.
    #[tokio::test]
    async fn folding_one_signal_leaves_the_other_signals_untouched() {
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        let now_ns = now_at_seal(11);
        publish_segment(&store, 0, Uuid::new_v4(), 1, 11, now_ns - NS_PER_HOUR).await;
        catalog
            .fold(&tenant(), Signal::Logs, Uuid::new_v4(), now_ns, &[], None)
            .await
            .expect("the logs fold succeeds");

        assert_eq!(catalog.fold_cycles(Signal::Logs), 1);
        assert_eq!(catalog.fold_failures(Signal::Logs), 0);
        assert_eq!(catalog.fold_last_success_unix_ns(Signal::Logs), now_ns);

        for untouched in [Signal::Metrics, Signal::Spans, Signal::Profiles] {
            assert_eq!(
                catalog.fold_cycles(untouched),
                0,
                "{untouched:?} was never folded, so its cycle count is still 0"
            );
            assert_eq!(
                catalog.fold_failures(untouched),
                0,
                "{untouched:?} was never folded, so its failure count is still 0"
            );
            assert_eq!(
                catalog.fold_last_success_unix_ns(untouched),
                0,
                "{untouched:?} was never folded, so its gauge is still the zero sentinel"
            );
        }
    }

    /// The failure half of the per-signal claim: a fold that fails for one
    /// signal leaves every other signal's failure counter at zero, so the
    /// `RavelCatalogFoldFailing` rule attributes the failure to the signal
    /// that produced it rather than to the process.
    #[tokio::test]
    async fn a_failing_fold_charges_only_its_own_signal() {
        // Fail the first HEAD PUT. No other PUT key contains "HEAD".
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("head cas failure".into()))
                .with_key_contains("HEAD")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");

        // No segment is published: a fold over an empty prefix still advances
        // its watermark over the hours that held nothing and writes a first
        // HEAD, which is the PUT the fault below intercepts.
        let now_ns = now_at_seal(11);
        let err = catalog
            .fold(&tenant(), Signal::Spans, Uuid::new_v4(), now_ns, &[], None)
            .await
            .expect_err("the HEAD CAS fault must surface as an error");
        assert!(matches!(err, CatalogError::Store(_)), "got {err:?}");

        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 1);
        assert_eq!(catalog.fold_failures(Signal::Spans), 1);
        for untouched in [Signal::Metrics, Signal::Logs] {
            assert_eq!(
                catalog.fold_failures(untouched),
                0,
                "{untouched:?} did not fail: only the folded signal is charged"
            );
            assert_eq!(catalog.fold_cycles(untouched), 0);
        }
    }

    // ---- ADR-2677 decision 1: operator-asserted seal-through hour ----

    async fn read_metrics_head(store: &MemoryStore) -> SnapshotHead {
        let got = store
            .get(&head_object_key(&tenant(), Signal::Metrics), GetRange::Full)
            .await
            .expect("head present");
        snapshot_format::decode_head(&got.data).expect("head decodes")
    }

    async fn fold_sealing(
        catalog: &Catalog,
        now_ns: i64,
        seal_through_hour: Option<u32>,
    ) -> FoldReport {
        catalog
            .fold_with_seal_through(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now_ns,
                &[],
                None,
                &RefoldRequest::new(),
                seal_through_hour,
            )
            .await
            .expect("fold")
    }

    /// Ten minutes into hour `hour`: the seal margin alone seals nothing
    /// later than `hour - 3` there.
    fn now_inside(hour: u32) -> i64 {
        i64::from(hour) * NS_PER_HOUR + 10 * 60 * 1_000_000_000
    }

    #[tokio::test]
    async fn seal_through_hour_seals_the_open_hour_and_no_ops_at_or_below_the_watermark() {
        const H: u32 = 480_000;
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = now_inside(H);
        assert_eq!(sealed_watermark_hour(now, catalog.config()), Some(H - 3));
        publish_segment(&store, 0, Uuid::new_v4(), 1, H - 1, now - NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 2, H, now).await;

        let sealed = fold_sealing(&catalog, now, Some(H)).await;
        assert!(!sealed.no_op, "{sealed:?}");
        assert_eq!(sealed.watermark_hour, Some(H));
        assert_eq!(sealed.seal_through_hour, Some(H));
        assert_eq!(sealed.entry_count, 2, "both hours' records are folded");
        // The ceiling moves the target, not the clock: HEAD records the real
        // fold time.
        assert_eq!(read_metrics_head(&store).await.created_unix_ns, now);

        for ceiling in [H, H - 1] {
            let again = fold_sealing(&catalog, now + 60 * 1_000_000_000, Some(ceiling)).await;
            assert!(again.no_op, "seal-through {ceiling}: {again:?}");
            assert_eq!(again.watermark_hour, Some(H));
            assert_eq!(again.seal_through_hour, Some(ceiling));
        }

        // Without a ceiling the scheduled fold re-lists the asserted hours
        // until the margin alone passes them, and with nothing new there it
        // no-ops.
        for later in [now_at_seal(H) - 1, now_at_seal(H)] {
            let scheduled = fold_sealing(&catalog, later, None).await;
            assert!(scheduled.no_op, "at {later}: {scheduled:?}");
            assert_eq!(scheduled.watermark_hour, Some(H));
            assert_eq!(scheduled.seal_through_hour, None);
        }
        let natural = fold_sealing(&catalog, now_at_seal(H + 1), None).await;
        assert!(!natural.no_op, "{natural:?}");
        assert_eq!(natural.watermark_hour, Some(H + 1));
        assert_eq!(natural.seal_through_hour, None);
        assert_eq!(natural.entry_count, 2);
    }

    #[tokio::test]
    async fn seal_through_hour_below_the_margin_hour_leaves_the_margin_target() {
        const H: u32 = 480_000;
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = now_inside(H);
        let margin_hour = H - 3;
        assert_eq!(catalog.margin_watermark_hour(now), Some(margin_hour));
        publish_segment(&store, 0, Uuid::new_v4(), 1, H - 5, now - 5 * NS_PER_HOUR).await;
        publish_segment(
            &store,
            0,
            Uuid::new_v4(),
            2,
            margin_hour,
            now - 3 * NS_PER_HOUR,
        )
        .await;

        let report = fold_sealing(&catalog, now, Some(H - 5)).await;
        assert!(!report.no_op, "{report:?}");
        assert_eq!(report.watermark_hour, Some(margin_hour), "{report:?}");
        assert_eq!(report.seal_through_hour, None, "{report:?}");
        assert_eq!(report.entry_count, 2, "{report:?}");
        assert_eq!(
            read_metrics_head(&store).await.watermark_hour,
            margin_hour,
            "HEAD carries the margin hour, not the lower seal-through hour"
        );
    }

    #[tokio::test]
    async fn seal_through_hour_after_the_current_hour_is_refused_before_any_read() {
        const H: u32 = 480_000;
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = now_inside(H);
        publish_segment(&store, 0, Uuid::new_v4(), 1, H, now).await;

        let err = catalog
            .fold_with_seal_through(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                now,
                &[],
                None,
                &RefoldRequest::new(),
                Some(H + 1),
            )
            .await
            .expect_err("a seal-through hour after now's hour is refused");
        assert!(
            matches!(
                err,
                CatalogError::InvalidConfig(
                    "seal_through_hour is later than the hour bucket of now_ns"
                )
            ),
            "{err:?}"
        );
        assert!(
            store
                .get(&head_object_key(&tenant(), Signal::Metrics), GetRange::Full)
                .await
                .is_err(),
            "no HEAD was written"
        );

        // The current hour itself is accepted.
        let sealed = fold_sealing(&catalog, now, Some(H)).await;
        assert_eq!(sealed.watermark_hour, Some(H), "{sealed:?}");
    }

    #[tokio::test]
    async fn head_watermark_hour_reads_the_folded_head() {
        const H: u32 = 480_000;
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        assert_eq!(
            catalog
                .head_watermark_hour(&tenant(), Signal::Metrics)
                .await
                .expect("read"),
            None,
            "no HEAD yet"
        );
        let now = now_inside(H);
        publish_segment(&store, 0, Uuid::new_v4(), 1, H, now).await;
        fold_sealing(&catalog, now, Some(H)).await;
        assert_eq!(
            catalog
                .head_watermark_hour(&tenant(), Signal::Metrics)
                .await
                .expect("read"),
            Some(H)
        );
    }

    /// The store's LIST requests of either kind, for a fold-cost delta.
    fn list_calls(store: &InstrumentedStore<Arc<MemoryStore>>) -> u64 {
        let snapshot = store.metrics().snapshot();
        snapshot.list.calls + snapshot.list_delimited.calls
    }

    /// The `writer_seq`s a fresh catalog's non-token resolve of hour `hour`
    /// serves, sorted.
    async fn resolved_seqs(store: &Arc<MemoryStore>, shard_count: u32, hour: u32) -> Vec<u64> {
        let window = ravel_types::TimeRange {
            start_ns: i64::from(hour) * NS_PER_HOUR,
            end_ns: i64::from(hour + 1) * NS_PER_HOUR,
        };
        let query_now = i64::from(hour + 1) * NS_PER_HOUR + 1;
        let fresh = Catalog::new(store.clone(), config(shard_count)).expect("catalog");
        let resolved = fresh
            .resolve(&tenant(), Signal::Metrics, window, &[], query_now)
            .await
            .expect("resolve");
        let mut seqs: Vec<u64> = resolved.segments.iter().map(|s| s.writer_seq).collect();
        seqs.sort_unstable();
        seqs
    }

    /// The reference tenant's shard count, which every per-fold LIST figure
    /// below is a multiple of.
    const REFERENCE_SHARDS: u32 = 4;

    #[tokio::test]
    async fn late_commit_into_an_operator_sealed_hour_is_visible_after_the_next_fold() {
        const H: u32 = 480_000;
        let inner = Arc::new(MemoryStore::new());
        let store = Arc::new(InstrumentedStore::new(inner.clone()));
        let catalog = Catalog::new(store.clone(), config(REFERENCE_SHARDS)).expect("catalog");
        let now = now_inside(H);
        let on_time = publish_segment(&inner, 0, Uuid::new_v4(), 1, H, now).await;
        let sealed = fold_sealing(&catalog, now, Some(H)).await;
        assert_eq!(sealed.watermark_hour, Some(H), "{sealed:?}");

        // A writer that started after the seal publishes into hour H.
        let late = publish_segment(&inner, 1, Uuid::new_v4(), 2, H, now + 1).await;
        assert_eq!(
            resolved_seqs(&inner, REFERENCE_SHARDS, H).await,
            vec![on_time.writer_seq],
            "before the next fold the late commit is not in the snapshot"
        );

        // A plain fold five minutes later, with no seal-through hour and still
        // well inside H's natural margin, re-lists the held-open hours H-2..=H
        // (the margin hour is H-3 until twenty past): three hours, one LIST
        // per shard.
        let later = now + 5 * 60 * 1_000_000_000;
        assert!(later < now_at_seal(H));
        assert_eq!(catalog.margin_watermark_hour(later), Some(H - 3));
        let before = list_calls(&store);
        let next = fold_sealing(&catalog, later, None).await;
        let listed = list_calls(&store) - before;
        assert!(!next.no_op, "{next:?}");
        assert!(!next.rebuilt, "{next:?}");
        assert_eq!(next.watermark_hour, Some(H), "the watermark does not move");
        assert_eq!(next.previous_watermark_hour, Some(H));
        assert_eq!(next.entry_count, 2, "{next:?}");
        assert_eq!(listed, u64::from(3 * REFERENCE_SHARDS));
        assert_eq!(next.list_requests, listed);
        assert_eq!(
            resolved_seqs(&inner, REFERENCE_SHARDS, H).await,
            vec![on_time.writer_seq, late.writer_seq],
            "a query carrying no token sees the late commit after the fold"
        );

        // With nothing new published the same re-listing finds nothing and
        // the fold is a no-op.
        let before = list_calls(&store);
        let quiet = fold_sealing(&catalog, later + 60 * 1_000_000_000, None).await;
        assert!(quiet.no_op, "{quiet:?}");
        assert_eq!(quiet.watermark_hour, Some(H));
        assert_eq!(list_calls(&store) - before, u64::from(3 * REFERENCE_SHARDS));
        // HEAD and the tenant config, then each snapshot part, since the
        // listing names commit records.
        assert_eq!(quiet.get_requests, 2 + next.parts_total, "{quiet:?}");
    }

    /// A fold whose held-open range is quiet still reconciles a requested
    /// hour outside that range: every fold over this HEAD takes the
    /// held-open path until the margin passes the watermark, so a request it
    /// skipped would stay stale however often it was resubmitted. An early
    /// return on `!has_unfolded` alone leaves `refold_hours_reconciled` at 0
    /// and HEAD naming the superseded segment at `H - 5`.
    #[tokio::test]
    async fn held_open_quiet_fold_still_serves_a_refold_request_outside_the_range() {
        const H: u32 = 480_000;
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = now_inside(H);

        let stale =
            publish_segment(&store, 0, Uuid::new_v4(), 1, H - 5, now - 5 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 2, H, now).await;
        let sealed = fold_sealing(&catalog, now, Some(H)).await;
        assert_eq!(sealed.watermark_hour, Some(H), "{sealed:?}");

        // A late compaction lands in hour H - 5, well below the held-open
        // range [H - 2, H] (margin hour H - 3, default 26-hour window), after
        // the hour was already folded.
        let compaction =
            publish_compaction(&store, 0, H - 5, &[&stale], now - 4 * NS_PER_HOUR).await;

        // Still well inside H's natural margin, and nothing new published
        // into the held-open range itself: without a request this fold would
        // be the ordinary quiet no-op the test above exercises.
        let later = now + 5 * 60 * 1_000_000_000;
        assert!(later < now_at_seal(H));
        let second = catalog
            .fold_with_seal_through(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                later,
                &[],
                None,
                &RefoldRequest::from_hours([H - 5]),
                None,
            )
            .await
            .expect("second fold");

        assert!(
            !second.no_op,
            "the targeted pass found hour H-5 dirty: {second:?}"
        );
        assert!(!second.rebuilt, "an incremental fold, not a rebuild");
        assert_eq!(
            second.refold_hours_reconciled, 1,
            "the out-of-range requested hour was reconciled, not dropped"
        );

        let head_keys = head_object_keys(store.as_ref()).await;
        assert!(
            head_keys.contains(
                &keys::reconstruct_l1_part_key(&compaction, &compaction.parts[0])
                    .expect("l1 part key")
            ),
            "hour H-5 must now name the compaction output: {head_keys:?}"
        );
        assert!(
            !head_keys.contains(&keys::reconstruct_data_key(&stale).expect("data key")),
            "hour H-5 must no longer name its superseded L0 input: {head_keys:?}"
        );
    }

    /// A requested hour that is already clean still counts as reconciled on
    /// a held-open fold that stays a no-op. The server's `run_tick` dequeues
    /// a no-op fold's request only when this count is nonzero, so a pass
    /// that counted only changed hours would leave a clean hour queued.
    #[tokio::test]
    async fn held_open_quiet_fold_reconciles_a_clean_requested_hour_without_going_dirty() {
        const H: u32 = 480_000;
        let store = Arc::new(MemoryStore::new());
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        let now = now_inside(H);

        publish_segment(&store, 0, Uuid::new_v4(), 1, H - 5, now - 5 * NS_PER_HOUR).await;
        publish_segment(&store, 0, Uuid::new_v4(), 2, H, now).await;
        let sealed = fold_sealing(&catalog, now, Some(H)).await;
        assert_eq!(sealed.watermark_hour, Some(H), "{sealed:?}");

        // No late record anywhere: hour H - 5 is already exactly what the
        // snapshot holds, and the held-open range [H - 2, H] is quiet too.
        let later = now + 5 * 60 * 1_000_000_000;
        assert!(later < now_at_seal(H));
        let second = catalog
            .fold_with_seal_through(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                later,
                &[],
                None,
                &RefoldRequest::from_hours([H - 5]),
                None,
            )
            .await
            .expect("second fold");

        assert!(
            second.no_op,
            "nothing changed anywhere, in or out of the held range: {second:?}"
        );
        assert_eq!(
            second.refold_hours_reconciled, 1,
            "the clean requested hour was still reconciled, so a caller can dequeue it"
        );
    }

    /// Records every `get()` key, so a fold-cost test can tell one part read
    /// twice from two parts read once each. Every other method delegates.
    struct GetLoggingStore<S> {
        inner: S,
        get_keys: parking_lot::Mutex<Vec<String>>,
    }

    impl<S> GetLoggingStore<S> {
        fn new(inner: S) -> Self {
            GetLoggingStore {
                inner,
                get_keys: parking_lot::Mutex::new(Vec::new()),
            }
        }

        /// The keys `get()` was called with since the last call, in order.
        fn take_get_keys(&self) -> Vec<String> {
            std::mem::take(&mut *self.get_keys.lock())
        }
    }

    #[async_trait::async_trait]
    impl<S: ObjectStoreBackend> ObjectStoreBackend for GetLoggingStore<S> {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            self.get_keys.lock().push(key.to_string());
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// How many times each of `part_keys` appears in `get_keys`.
    fn part_get_counts(part_keys: &[String], get_keys: &[String]) -> Vec<usize> {
        part_keys
            .iter()
            .map(|part| get_keys.iter().filter(|key| *key == part).count())
            .collect()
    }

    /// A HEAD sealed through `H` at `now_inside(H)` with one snapshot part per
    /// hour (`snapshot_part_max_entries = 1`, one shard): hours `H - 6` and
    /// `H - 5` lie below the held-open range `[H - 2, H]` a fold five minutes
    /// later sees, hours `H - 1` and `H` inside it. Returns the catalog, the
    /// commit record published into `H - 1` before the seal, and the HEAD's
    /// part keys with how many of them cover the held range.
    async fn four_part_held_open_fixture(
        inner: &Arc<MemoryStore>,
        store: &Arc<GetLoggingStore<Arc<MemoryStore>>>,
        compact_h_minus_1: bool,
    ) -> (Catalog, Vec<String>, usize) {
        const H: u32 = 480_000;
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                shard_count: 1,
                snapshot_part_max_entries: 1,
                ..Default::default()
            },
        )
        .expect("catalog");
        let now = now_inside(H);
        publish_segment(inner, 0, Uuid::new_v4(), 1, H - 6, now - 6 * NS_PER_HOUR).await;
        publish_segment(inner, 0, Uuid::new_v4(), 2, H - 5, now - 5 * NS_PER_HOUR).await;
        let a = publish_segment(inner, 0, Uuid::new_v4(), 3, H - 1, now - NS_PER_HOUR).await;
        if compact_h_minus_1 {
            let b = publish_segment(inner, 0, Uuid::new_v4(), 4, H - 1, now - NS_PER_HOUR).await;
            publish_compaction(inner, 0, H - 1, &[&a, &b], now - 30 * 60 * 1_000_000_000).await;
        }
        publish_segment(inner, 0, Uuid::new_v4(), 5, H, now).await;
        let sealed = fold_sealing(&catalog, now, Some(H)).await;
        assert_eq!(sealed.watermark_hour, Some(H), "{sealed:?}");
        assert_eq!(sealed.parts_total, 4, "one part per hour: {sealed:?}");

        let head = read_metrics_head(inner).await;
        let held = held_open_hours(
            H,
            catalog.margin_watermark_hour(now + 5 * 60 * 1_000_000_000),
            catalog.config().fold_reconcile_window_hours,
        )
        .expect("H is held open five minutes later");
        assert_eq!(held, (H - 2, H));
        let covering = head
            .parts
            .iter()
            .filter(|part| part.min_hour <= held.1 && part.watermark_hour >= held.0)
            .count();
        assert_eq!(covering, 2, "the parts of H - 1 and H cover the held range");
        let part_keys = head.parts.iter().map(|part| part.key.clone()).collect();
        store.take_get_keys();
        (catalog, part_keys, covering)
    }

    /// A held-open fold that folds a late commit reads every snapshot part
    /// exactly once: the parts covering the held range in the pre-check, and
    /// only the other parts in the main load. Reading the covering parts
    /// again in the main load adds `covering` to the figure.
    #[tokio::test]
    async fn held_open_fold_of_a_late_commit_gets_each_snapshot_part_once() {
        const H: u32 = 480_000;
        let inner = Arc::new(MemoryStore::new());
        let store = Arc::new(GetLoggingStore::new(inner.clone()));
        let (catalog, part_keys, covering) =
            four_part_held_open_fixture(&inner, &store, false).await;

        publish_segment(&inner, 0, Uuid::new_v4(), 6, H - 1, now_inside(H) + 1).await;
        let later = now_inside(H) + 5 * 60 * 1_000_000_000;
        let next = fold_sealing(&catalog, later, None).await;
        assert!(!next.no_op, "the late commit is folded: {next:?}");
        assert!(!next.rebuilt, "{next:?}");
        assert_eq!(next.entry_count, 5, "{next:?}");

        let get_keys = store.take_get_keys();
        assert_eq!(
            part_get_counts(&part_keys, &get_keys),
            vec![1; part_keys.len()],
            "each part of the previous HEAD is read once: {get_keys:?}"
        );
        // HEAD and the tenant config, each of the previous HEAD's parts once
        // (`covering` of them in the pre-check, the rest in the main load),
        // then both commit records in the reconciled bucket H - 1.
        let parts = part_keys.len() as u64;
        assert_eq!(parts, 4);
        assert_eq!(covering, 2);
        assert_eq!(next.get_requests, 2 + parts + 2, "{next:?}");
    }

    /// The compact-then-seal order: hour `H - 1` was compacted before the
    /// seal, so its listing still names the two compacted level-0 records,
    /// which the snapshot holds only as the compaction's level-1 part. The
    /// pre-check therefore takes the full path on every fold in the window;
    /// that path still reads each snapshot part exactly once, then the
    /// bucket's compaction record and both level-0 records, and writes
    /// nothing.
    #[tokio::test]
    async fn held_open_compacted_hour_reads_each_snapshot_part_once_and_stays_a_no_op() {
        const H: u32 = 480_000;
        let inner = Arc::new(MemoryStore::new());
        let store = Arc::new(GetLoggingStore::new(inner.clone()));
        let (catalog, part_keys, covering) =
            four_part_held_open_fixture(&inner, &store, true).await;
        let head_before = read_metrics_head(&inner).await;

        let later = now_inside(H) + 5 * 60 * 1_000_000_000;
        let quiet = fold_sealing(&catalog, later, None).await;
        assert!(quiet.no_op, "nothing new in the held range: {quiet:?}");
        assert_eq!(
            read_metrics_head(&inner).await.parts,
            head_before.parts,
            "no HEAD was written"
        );

        let get_keys = store.take_get_keys();
        assert_eq!(
            part_get_counts(&part_keys, &get_keys),
            vec![1; part_keys.len()],
            "each part of the HEAD is read once: {get_keys:?}"
        );
        // HEAD and the tenant config, each part once (`covering` of them in
        // the pre-check), then the compaction record and its two level-0
        // inputs in bucket H - 1.
        let parts = part_keys.len() as u64;
        assert_eq!(parts, 4);
        assert_eq!(covering, 2);
        assert_eq!(quiet.get_requests, 2 + parts + 3, "{quiet:?}");
    }

    /// A quiet held-open fold reads only the parts covering the held range;
    /// the same fold with a refold request queued loads the whole snapshot,
    /// each part once, and still writes nothing when the requested hour is
    /// clean.
    #[tokio::test]
    async fn held_open_quiet_fold_loads_the_whole_snapshot_only_for_a_refold_request() {
        const H: u32 = 480_000;
        let inner = Arc::new(MemoryStore::new());
        let store = Arc::new(GetLoggingStore::new(inner.clone()));
        let (catalog, part_keys, covering) =
            four_part_held_open_fixture(&inner, &store, false).await;
        let later = now_inside(H) + 5 * 60 * 1_000_000_000;

        let quiet = fold_sealing(&catalog, later, None).await;
        assert!(quiet.no_op, "{quiet:?}");
        let get_keys = store.take_get_keys();
        // The parts of H - 6 and H - 5 are not read; H - 1 and H are.
        assert_eq!(part_get_counts(&part_keys, &get_keys), vec![0, 0, 1, 1]);
        // HEAD and the tenant config, then the covering parts.
        assert_eq!(quiet.get_requests, 2 + covering as u64, "{quiet:?}");

        let requested = catalog
            .fold_with_seal_through(
                &tenant(),
                Signal::Metrics,
                Uuid::new_v4(),
                later + 60 * 1_000_000_000,
                &[],
                None,
                &RefoldRequest::from_hours([H - 6]),
                None,
            )
            .await
            .expect("fold");
        assert!(
            requested.no_op,
            "the requested hour is clean: {requested:?}"
        );
        assert_eq!(requested.refold_hours_reconciled, 1, "{requested:?}");
        let get_keys = store.take_get_keys();
        assert_eq!(
            part_get_counts(&part_keys, &get_keys),
            vec![1; part_keys.len()],
            "{get_keys:?}"
        );
        // HEAD and the tenant config, then every part once. Bucket H - 6
        // holds only a level-0 record, so the targeted pass lists it and
        // reads nothing.
        assert_eq!(
            requested.get_requests,
            2 + part_keys.len() as u64,
            "{requested:?}"
        );
    }

    #[test]
    fn held_open_hours_number_three_then_two_then_one_then_none() {
        const H: u32 = 480_000;
        let catalog =
            Catalog::new(Arc::new(MemoryStore::new()), config(REFERENCE_SHARDS)).expect("catalog");
        let minute = 60 * 1_000_000_000;
        let start = i64::from(H) * NS_PER_HOUR;
        let window = catalog.config().fold_reconcile_window_hours;
        let held = |now: i64| {
            held_open_hours(H, catalog.margin_watermark_hour(now), window)
                .map_or(0, |(lo, hi)| hi - lo + 1)
        };
        assert_eq!(held(start), 3);
        assert_eq!(held(start + 20 * minute - 1), 3);
        assert_eq!(held(start + 20 * minute), 2);
        assert_eq!(held(start + 80 * minute - 1), 2);
        assert_eq!(held(start + 80 * minute), 1);
        assert_eq!(held(now_at_seal(H) - 1), 1);
        assert_eq!(held(now_at_seal(H)), 0);
        assert_eq!(held(now_at_seal(H + 5)), 0);
    }

    /// A watermark far above the margin hour, or a margin the clock cannot
    /// reach at all, holds open at most the reconcile window's hours below
    /// the watermark, never the whole gap.
    ///
    /// FLIP: drop the `.max(floor)` (or return `(0, head_watermark)` for a
    /// `None` margin) in `held_open_hours` and the range widths below become
    /// 1,000 and 480,001.
    #[test]
    fn held_open_hours_never_reach_below_the_reconcile_window() {
        const H: u32 = 480_000;
        const WINDOW: u32 = 27;
        let width = |range: Option<(u32, u32)>| range.map_or(0, |(lo, hi)| hi - lo + 1);
        assert_eq!(
            held_open_hours(H, Some(H - 1_000), WINDOW),
            Some((H - WINDOW, H))
        );
        assert_eq!(
            width(held_open_hours(H, Some(H - 1_000), WINDOW)),
            WINDOW + 1
        );
        assert_eq!(held_open_hours(H, None, WINDOW), Some((H - WINDOW, H)));
        // A margin inside the window keeps its own, narrower range.
        assert_eq!(held_open_hours(H, Some(H - 3), WINDOW), Some((H - 2, H)));
        assert_eq!(held_open_hours(H, Some(H), WINDOW), None);
    }

    #[tokio::test]
    async fn after_the_natural_seal_the_skip_applies_again() {
        const H: u32 = 480_000;
        let inner = Arc::new(MemoryStore::new());
        let store = Arc::new(InstrumentedStore::new(inner.clone()));
        let catalog = Catalog::new(store.clone(), config(REFERENCE_SHARDS)).expect("catalog");
        let now = now_inside(H);
        let on_time = publish_segment(&inner, 0, Uuid::new_v4(), 1, H, now).await;
        fold_sealing(&catalog, now, Some(H)).await;

        // From H's natural seal on, the margin alone seals the watermark, so
        // a fold that does not advance it lists nothing.
        let before = list_calls(&store);
        let at_seal = fold_sealing(&catalog, now_at_seal(H), None).await;
        assert!(at_seal.no_op, "{at_seal:?}");
        assert_eq!(list_calls(&store) - before, 0);

        let late = publish_segment(&inner, 1, Uuid::new_v4(), 2, H, now_at_seal(H)).await;
        let before = list_calls(&store);
        let after_seal =
            fold_sealing(&catalog, now_at_seal(H) + 30 * 60 * 1_000_000_000, None).await;
        assert!(after_seal.no_op, "{after_seal:?}");
        assert_eq!(list_calls(&store) - before, 0);

        // The next advancing fold lists hour H+1 and the 27-hour reconcile
        // window [H-26, H]; the window skips H's L0-only bucket as for any
        // naturally sealed hour, so the late commit stays out.
        let before = list_calls(&store);
        let advanced = fold_sealing(&catalog, now_at_seal(H + 1), None).await;
        assert!(!advanced.no_op, "{advanced:?}");
        assert_eq!(advanced.watermark_hour, Some(H + 1));
        assert_eq!(advanced.entry_count, 1, "{advanced:?}");
        assert_eq!(
            list_calls(&store) - before,
            u64::from((1 + 27) * REFERENCE_SHARDS)
        );
        assert_eq!(
            resolved_seqs(&inner, REFERENCE_SHARDS, H).await,
            vec![on_time.writer_seq],
            "a commit published after the natural seal is invisible: {late:?}"
        );
    }

    #[tokio::test]
    async fn a_plain_fold_with_no_operator_seal_lists_nothing_extra() {
        const H: u32 = 480_000;
        let inner = Arc::new(MemoryStore::new());
        let store = Arc::new(InstrumentedStore::new(inner.clone()));
        let catalog = Catalog::new(store.clone(), config(REFERENCE_SHARDS)).expect("catalog");
        let now = now_inside(H);
        publish_segment(&inner, 0, Uuid::new_v4(), 1, H - 3, now - 3 * NS_PER_HOUR).await;
        let first = fold_sealing(&catalog, now, None).await;
        assert_eq!(first.watermark_hour, Some(H - 3), "{first:?}");

        // Same margin hour: a no-op that lists nothing and GETs only HEAD and
        // the tenant config.
        let before = store.metrics().snapshot();
        let quiet = fold_sealing(&catalog, now + 5 * 60 * 1_000_000_000, None).await;
        let after = store.metrics().snapshot();
        assert!(quiet.no_op, "{quiet:?}");
        assert_eq!(
            after.list.calls + after.list_delimited.calls,
            before.list.calls + before.list_delimited.calls
        );
        assert_eq!(after.get.calls - before.get.calls, 2);

        // An advancing fold lists the new hour and the 27-hour reconcile
        // window [H-29, H-3], one LIST per shard each.
        let before = list_calls(&store);
        let advanced = fold_sealing(&catalog, now_at_seal(H - 2), None).await;
        assert!(!advanced.no_op, "{advanced:?}");
        assert_eq!(advanced.watermark_hour, Some(H - 2));
        assert_eq!(
            list_calls(&store) - before,
            u64::from((1 + 27) * REFERENCE_SHARDS)
        );
        assert_eq!(advanced.list_requests, list_calls(&store) - before);
    }

    // ---- ADR-2707 decision 4: per-part key-index leaves ----

    use crate::snapshot_format::{
        KeyIndexLeafProbe, KeyIndexLeafRequest, KeyIndexMatch, select_key_index_slice,
        verify_key_index_leaf,
    };
    use ravel_codec::key_idx::{key8_i64, key8_id16};

    pub(crate) const USER_ID: &str = "user_id";
    /// The hour the single-hour key-index fixtures publish into.
    pub(crate) const KIDX_HOUR: u32 = 470_000;

    /// Publishes one L0 RLOG object into `hour`: row `i` carries
    /// `user_id = user_ids[i]`, rows go `rows_per_block` to a level-0 block, and
    /// each body is padded with `body_bytes` hex characters derived from
    /// `(writer_seq, i)`. The KEY_IDX section indexes `user_id` as I64 when
    /// `with_key_idx`. Returns the record and the object's bytes.
    pub(crate) async fn publish_user_id_logs(
        store: &dyn ObjectStoreBackend,
        writer_seq: u64,
        hour: u32,
        user_ids: &[i64],
        rows_per_block: usize,
        body_bytes: usize,
        with_key_idx: bool,
    ) -> (CommitRecord, Bytes) {
        let key_fields: &[(&str, KeyType)] = if with_key_idx {
            &[(USER_ID, KeyType::I64)]
        } else {
            &[]
        };
        publish_keyed_logs(
            store,
            writer_seq,
            hour,
            user_ids,
            rows_per_block,
            body_bytes,
            key_fields,
        )
        .await
    }

    /// [`publish_user_id_logs`] with the KEY_IDX section indexing
    /// `key_fields`: row `i` carries each of them as an attribute, an I64
    /// field as `user_ids[i]` and a Str field as `"u{user_ids[i]}"`, and
    /// `user_id = user_ids[i]` as I64 when `key_fields` does not name it.
    pub(crate) async fn publish_keyed_logs(
        store: &dyn ObjectStoreBackend,
        writer_seq: u64,
        hour: u32,
        user_ids: &[i64],
        rows_per_block: usize,
        body_bytes: usize,
        key_fields: &[(&str, KeyType)],
    ) -> (CommitRecord, Bytes) {
        use ravel_logseg::writer::ObjectIdentity;
        use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
        use ravel_types::logstream::{AttrValue, log_stream_id};

        let writer_id = Uuid::from_u128(u128::from(writer_seq));
        let resource = vec![(
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        )];
        let mut w = RlogWriter::new(
            RlogConfig {
                block_target_records: rows_per_block,
                ..RlogConfig::default()
            },
            ObjectIdentity {
                tenant_hash: tenant().0,
                shard: 0,
                writer_id: *writer_id.as_bytes(),
                writer_epoch: 1,
                writer_seq,
            },
        );
        if !key_fields.is_empty() {
            w = w.with_key_index_fields(
                key_fields
                    .iter()
                    .map(|(name, key_type)| (name.to_string(), *key_type))
                    .collect(),
            );
        }
        let attrs_of = |user_id: i64| {
            let mut attrs: Vec<(String, AttrValue)> = key_fields
                .iter()
                .map(|(name, key_type)| {
                    let value = match key_type {
                        KeyType::Str => AttrValue::Str(format!("u{user_id}")),
                        _ => AttrValue::I64(user_id),
                    };
                    (name.to_string(), value)
                })
                .collect();
            if !key_fields.iter().any(|(name, _)| *name == USER_ID) {
                attrs.push((USER_ID.to_string(), AttrValue::I64(user_id)));
            }
            attrs.sort_by(|a, b| a.0.cmp(&b.0));
            attrs
        };
        let base_ts = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
        for (i, user_id) in user_ids.iter().enumerate() {
            let mut body = format!("row {i} ");
            let mut n = 0u32;
            while body.len() < body_bytes {
                body.push_str(
                    blake3::hash(format!("{writer_seq}-{i}-{n}").as_bytes())
                        .to_hex()
                        .as_str(),
                );
                n += 1;
            }
            let ts = base_ts + i as i64;
            w.push(LogRecord {
                stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body,
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: attrs_of(*user_id),
            })
            .expect("push");
        }
        let bytes = Bytes::from(w.finish().expect("finish"));
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: user_ids.len() as u64,
            series_count: 1,
            min_event_ts_ns: base_ts,
            max_event_ts_ns: base_ts + user_ids.len() as i64,
            min_ingest_ts_ns: base_ts,
            max_ingest_ts_ns: base_ts + user_ids.len() as i64,
            segment_format_version: 1,
            created_unix_ns: base_ts,
            ingest_hour_bucket: hour,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store, &data_key, bytes.clone())
            .await
            .expect("put data object");
        publish::publish(store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        (record, bytes)
    }

    /// Publishes one L0 RSPAN object into [`KIDX_HOUR`] with one span per
    /// trace id, all in block 0 (the default block holds 8192 spans).
    pub(crate) async fn publish_trace_spans(
        store: &dyn ObjectStoreBackend,
        writer_seq: u64,
        trace_ids: &[[u8; 16]],
    ) -> CommitRecord {
        use ravel_rspan::{ObjectIdentity, RspanConfig, RspanWriter, SpanRecord, StatusCode};

        let writer_id = Uuid::from_u128(u128::from(writer_seq));
        let mut w = RspanWriter::new(
            RspanConfig::default(),
            ObjectIdentity {
                tenant_hash: tenant().0,
                shard: 0,
                writer_id: *writer_id.as_bytes(),
                writer_epoch: 1,
                writer_seq,
            },
        );
        let base_ts = i64::from(KIDX_HOUR) * NS_PER_HOUR + 60_000_000_000;
        for (i, trace_id) in trace_ids.iter().enumerate() {
            w.push(SpanRecord {
                trace_id: *trace_id,
                span_id: [i as u8 + 1; 8],
                parent_span_id: None,
                name: "op".to_string(),
                start_ts_ns: base_ts + i as i64,
                end_ts_ns: base_ts + i as i64 + 10,
                status_code: StatusCode::Unset,
                status_message: None,
                attrs: Vec::new(),
            });
        }
        let bytes = Bytes::from(w.finish().expect("finish"));
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Spans,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: trace_ids.len() as u64,
            series_count: 1,
            min_event_ts_ns: base_ts,
            max_event_ts_ns: base_ts + 100,
            min_ingest_ts_ns: base_ts,
            max_ingest_ts_ns: base_ts + 100,
            segment_format_version: 1,
            created_unix_ns: base_ts,
            ingest_hour_bucket: KIDX_HOUR,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store, &data_key, bytes)
            .await
            .expect("put data object");
        publish::publish(store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        record
    }

    /// Declares `user_id` as an I64 typed column carrying `key_index`.
    pub(crate) async fn declare_user_id_key_index(store: &dyn ObjectStoreBackend) {
        use crate::tenant_config::{
            DeclaredColumnType, StorageLayoutWrite, TenantConfig, TenantLifecycleState,
        };
        let mut cfg = TenantConfig {
            typed_attr_columns: Some(vec![DeclaredTypedColumn {
                key: USER_ID.to_string(),
                ty: DeclaredColumnType::I64,
            }]),
            ..TenantConfig::new(TenantLifecycleState::Active)
        };
        cfg.set_key_index_columns(
            vec![USER_ID.to_string()],
            StorageLayoutWrite::ReadersRolledOut,
        )
        .expect("declare the key index");
        crate::tenant_config::set_tenant_config(store, &tenant(), &cfg, 1)
            .await
            .expect("write tenant config");
    }

    /// One shard with the key-index leaf build switched on.
    fn kidx_config() -> CatalogConfig {
        CatalogConfig {
            build_key_index_leaves: true,
            ..config(1)
        }
    }

    pub(crate) async fn fold_through(catalog: &Catalog, signal: Signal, hour: u32) -> FoldReport {
        catalog
            .fold(
                &tenant(),
                signal,
                Uuid::new_v4(),
                now_at_seal(hour),
                &[],
                None,
            )
            .await
            .expect("fold")
    }

    pub(crate) async fn read_signal_head(
        store: &dyn ObjectStoreBackend,
        signal: Signal,
    ) -> SnapshotHead {
        let got = store
            .get(&head_object_key(&tenant(), signal), GetRange::Full)
            .await
            .expect("head present");
        snapshot_format::decode_head(&got.data).expect("head decodes")
    }

    /// `writer_seq` to part entry ordinal, read off the part object.
    pub(crate) async fn part_ordinals(
        store: &dyn ObjectStoreBackend,
        part: &SnapshotPartRef,
    ) -> HashMap<u64, u64> {
        let got = store
            .get(&part.key, GetRange::Full)
            .await
            .expect("part present");
        snapshot_format::decode_part(&got.data, &PartLimits::default())
            .expect("part decodes")
            .entries
            .iter()
            .enumerate()
            .map(|(ordinal, entry)| (entry.writer_seq, ordinal as u64))
            .collect()
    }

    /// Probes `part`'s leaf for `field` through the catalog probe API.
    pub(crate) async fn probe_part(
        catalog: &Catalog,
        signal: Signal,
        part: &SnapshotPartRef,
        key_type: KeyType,
        field: &str,
        key8: [u8; 8],
    ) -> KeyIndexLeafProbe {
        let leaf = select_key_index_slice(&part.key_index, field, &key8)
            .expect("the field's slices tile")
            .expect("the part has a leaf for the field");
        catalog
            .probe_key_index_leaf(
                leaf,
                &KeyIndexLeafRequest {
                    tenant_hash: tenant(),
                    signal,
                    part_blake3: &part.blake3,
                    part_entry_count: part.entry_count,
                    field,
                    key_type,
                },
                &key8,
                &KeyIndexLeafLimits::default(),
            )
            .await
            .expect("no isolation breach")
    }

    pub(crate) fn matches(found: &[(u64, &[u32])], uncovered: &[u64]) -> KeyIndexLeafProbe {
        let mut matches: Vec<KeyIndexMatch> = found
            .iter()
            .map(|(entry_ordinal, blocks)| KeyIndexMatch {
                entry_ordinal: *entry_ordinal,
                blocks: blocks.to_vec(),
            })
            .collect();
        matches.sort_by_key(|m| m.entry_ordinal);
        KeyIndexLeafProbe::Matches {
            matches,
            uncovered_entry_ordinals: uncovered.to_vec(),
        }
    }

    /// ADR-2707 decision 4, the acceptance test: a fold builds one leaf per
    /// (part, field) from every entry's KEY_IDX section and attaches it to
    /// HEAD, and a probe through `Catalog::probe_key_index_leaf` names exactly
    /// the (entry, block set) pairs holding a key.
    ///
    /// Logs: three entries, two rows per block, `user_id` declared I64 with
    /// `key_index`. 10 sits in entry 1 block 0 and entry 2 blocks 0 and 1, 50
    /// in entry 2 block 0 only, 99 nowhere. Spans: two entries, one trace in
    /// both and one in the first only. A fold that built the leaf from only
    /// the part's first entry answers 10 with entry 1 alone and 50 as absent.
    ///
    /// The last phase fails every `.kidx` PUT: the fold fails and no part is
    /// written, which holds only while the leaves are PUT before the part. A
    /// fold that wrote the part first would leave a `.csnap` behind here.
    #[tokio::test]
    async fn leaf_probe_names_the_entries_and_blocks_holding_the_key() {
        let store = Arc::new(MemoryStore::new());
        declare_user_id_key_index(store.as_ref()).await;
        publish_user_id_logs(
            store.as_ref(),
            1,
            KIDX_HOUR,
            &[10, 10, 20, 30, 40, 40],
            2,
            0,
            true,
        )
        .await;
        publish_user_id_logs(store.as_ref(), 2, KIDX_HOUR, &[50, 10, 60, 10], 2, 0, true).await;
        publish_user_id_logs(store.as_ref(), 3, KIDX_HOUR, &[70, 80], 2, 0, true).await;
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        let report = fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
        assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
        assert_eq!(report.key_index_uncovered_entries, 0, "{report:?}");

        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(head.parts.len(), 1);
        let part = &head.parts[0];
        let ordinal = part_ordinals(store.as_ref(), part).await;
        assert_eq!(part.key_index.len(), 1, "one leaf for user_id on HEAD");
        let leaf = &part.key_index[0];
        assert_eq!(leaf.field, USER_ID);
        assert_eq!(leaf.key_type, u32::from(KeyType::I64.as_u8()));
        assert_eq!((leaf.key8_lo, leaf.key8_hi), (0, u64::MAX));
        assert_eq!(leaf.entry_count, 9, "4 + 3 + 2 distinct (key, entry) pairs");
        assert_eq!(leaf.uncovered_count, 0);
        assert_eq!(leaf.part_blake3, part.blake3);
        assert_eq!(report.key_index_leaf_bytes_written, leaf.size);
        let stored = store
            .get(&leaf.key, GetRange::Full)
            .await
            .expect("the leaf HEAD names is stored");
        assert_eq!(
            blake3::hash(&stored.data).as_bytes().as_slice(),
            leaf.blake3
        );
        assert!(
            leaf.key.starts_with(&format!(
                "t/{}/catalog/{}/idx/{}.",
                tenant().to_hex(),
                Signal::Logs.key_prefix(),
                keys::ingest_hour_string(part.watermark_hour)
            )) && leaf
                .key
                .ends_with(&format!("{}.kidx", &hex_encode(&leaf.blake3)[..16])),
            "{}",
            leaf.key
        );

        let probe = |v: i64| {
            probe_part(
                &catalog,
                Signal::Logs,
                part,
                KeyType::I64,
                USER_ID,
                key8_i64(v),
            )
        };
        assert_eq!(
            probe(10).await,
            matches(&[(ordinal[&1], &[0]), (ordinal[&2], &[0, 1])], &[])
        );
        assert_eq!(probe(50).await, matches(&[(ordinal[&2], &[0])], &[]));
        assert_eq!(
            probe(99).await,
            KeyIndexLeafProbe::Absent {
                uncovered_entry_ordinals: Vec::new()
            }
        );

        // Spans: trace_id is always indexed, with no declaration.
        let store = Arc::new(MemoryStore::new());
        let (t1, t2, absent) = ([0x11u8; 16], [0x22u8; 16], [0x44u8; 16]);
        publish_trace_spans(store.as_ref(), 1, &[t1, t2]).await;
        publish_trace_spans(store.as_ref(), 2, &[t1, [0x33u8; 16]]).await;
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        let report = fold_through(&catalog, Signal::Spans, KIDX_HOUR).await;
        assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
        let head = read_signal_head(store.as_ref(), Signal::Spans).await;
        let part = &head.parts[0];
        let ordinal = part_ordinals(store.as_ref(), part).await;
        let field = ravel_rspan::TRACE_ID_FIELD;
        assert_eq!(
            probe_part(
                &catalog,
                Signal::Spans,
                part,
                KeyType::Id16,
                field,
                key8_id16(&t1)
            )
            .await,
            matches(&[(ordinal[&1], &[0]), (ordinal[&2], &[0])], &[])
        );
        assert_eq!(
            probe_part(
                &catalog,
                Signal::Spans,
                part,
                KeyType::Id16,
                field,
                key8_id16(&t2)
            )
            .await,
            matches(&[(ordinal[&1], &[0])], &[])
        );
        assert_eq!(
            probe_part(
                &catalog,
                Signal::Spans,
                part,
                KeyType::Id16,
                field,
                key8_id16(&absent)
            )
            .await,
            KeyIndexLeafProbe::Absent {
                uncovered_entry_ordinals: Vec::new()
            }
        );

        // Ordering: a refused leaf PUT fails the fold before the part PUT.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("leaf PUT refused".into()))
                .with_key_contains(".kidx"),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        declare_user_id_key_index(store.as_ref()).await;
        publish_user_id_logs(store.as_ref(), 1, KIDX_HOUR, &[10, 20], 2, 0, true).await;
        publish_user_id_logs(store.as_ref(), 2, KIDX_HOUR, &[10, 30], 2, 0, true).await;
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        let err = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(KIDX_HOUR),
                &[],
                None,
            )
            .await
            .expect_err("a refused leaf PUT fails the fold");
        assert!(matches!(err, CatalogError::Store(_)), "{err:?}");
        assert_eq!(store.fault_count(Op::Put, FaultKind::Permanent), 1);
        let catalog_prefix = format!(
            "t/{}/catalog/{}/",
            tenant().to_hex(),
            Signal::Logs.key_prefix()
        );
        // The part's `.cstat`, also built before the part PUT, may remain; it
        // is unreferenced and swept like any other.
        let written: Vec<String> = list_all_keys(store.as_ref(), &catalog_prefix)
            .await
            .into_iter()
            .filter(|key| !key.ends_with(".cstat"))
            .collect();
        assert_eq!(
            written,
            Vec::<String>::new(),
            "no part, no leaf and no HEAD after the refusal"
        );
    }

    /// An entry whose object has no KEY_IDX section and one whose section GET
    /// fails are uncovered: the leaf is still written for the third entry,
    /// lists both as uncovered, and a probe returns them as candidates. The
    /// failed GET is logged at warn, naming the data object.
    #[tokio::test]
    async fn entries_without_a_readable_section_are_uncovered_and_the_leaf_is_still_written() {
        // Published before the fault plan wraps the store, so the plan counts
        // only the fold's GETs of entry 3's data object: the column-stats
        // build's whole-object GET is the first, the footer suffix GET the
        // second, the section GET the third.
        let inner = MemoryStore::new();
        declare_user_id_key_index(&inner).await;
        publish_user_id_logs(&inner, 1, KIDX_HOUR, &[10, 20], 2, 0, true).await;
        publish_user_id_logs(&inner, 2, KIDX_HOUR, &[10], 2, 0, false).await;
        let (failing, _) = publish_user_id_logs(&inner, 3, KIDX_HOUR, &[10, 30], 2, 0, true).await;
        let failing = keys::reconstruct_data_key(&failing).expect("data key");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("section GET refused".into()),
            )
            .with_key_contains(failing.as_str())
            .with_occurrence(Occurrence::Nth(3)),
        );
        let store = Arc::new(FaultStore::new(inner, plan));
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        let capture = LevelCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let report = {
            let _default = tracing::subscriber::set_default(subscriber);
            fold_through(&catalog, Signal::Logs, KIDX_HOUR).await
        };
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Permanent),
            1,
            "the section GET of entry 3 fired"
        );
        let warned: Vec<String> = capture
            .lines()
            .into_iter()
            .filter(|(level, line)| *level == tracing::Level::WARN && line.contains("KEY_IDX"))
            .map(|(_, line)| line)
            .collect();
        assert_eq!(warned.len(), 1, "{warned:?}");
        assert!(
            warned[0].contains(&format!("object_key={failing}"))
                && warned[0].contains("KEY_IDX section GET"),
            "{warned:?}"
        );
        assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
        assert_eq!(report.key_index_uncovered_entries, 2, "{report:?}");
        // Entry 1: suffix and section. Entry 2: suffix only (no section).
        // Entry 3: suffix and the refused section GET.
        assert_eq!(report.key_index_ranged_gets, 5, "{report:?}");

        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        let part = &head.parts[0];
        let ordinal = part_ordinals(store.as_ref(), part).await;
        assert_eq!(part.key_index.len(), 1);
        assert_eq!(part.key_index[0].uncovered_count, 2);
        let mut uncovered = vec![ordinal[&2], ordinal[&3]];
        uncovered.sort_unstable();
        assert_eq!(
            probe_part(
                &catalog,
                Signal::Logs,
                part,
                KeyType::I64,
                USER_ID,
                key8_i64(10)
            )
            .await,
            matches(&[(ordinal[&1], &[0])], &uncovered)
        );
        assert_eq!(
            probe_part(
                &catalog,
                Signal::Logs,
                part,
                KeyType::I64,
                USER_ID,
                key8_i64(30)
            )
            .await,
            KeyIndexLeafProbe::Absent {
                uncovered_entry_ordinals: uncovered.clone()
            }
        );
    }

    /// A part carried forward by reference keeps its `key_index` refs
    /// unchanged across a second fold, and the second fold reads a section
    /// only for the re-encoded tail's new entry: the tail's other entry is
    /// carried from the previous tail's leaf. With a 2-entry part ceiling the
    /// first hour's two entries seal into a part the second fold reproduces
    /// byte-for-byte.
    #[tokio::test]
    async fn a_part_carried_forward_keeps_its_key_index_refs() {
        let h1 = KIDX_HOUR;
        let store = Arc::new(MemoryStore::new());
        declare_user_id_key_index(store.as_ref()).await;
        publish_user_id_logs(store.as_ref(), 1, h1, &[10, 20], 2, 0, true).await;
        publish_user_id_logs(store.as_ref(), 2, h1, &[10, 30], 2, 0, true).await;
        publish_user_id_logs(store.as_ref(), 3, h1 + 1, &[40], 2, 0, true).await;
        let cfg = CatalogConfig {
            snapshot_part_max_entries: 2,
            ..kidx_config()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");
        fold_through(&catalog, Signal::Logs, h1 + 1).await;
        let first = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(first.parts.len(), 2);
        assert_eq!(first.parts[0].key_index.len(), 1);

        publish_user_id_logs(store.as_ref(), 4, h1 + 2, &[50], 2, 0, true).await;
        let report = fold_through(&catalog, Signal::Logs, h1 + 2).await;
        let second = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(report.parts_reused, 1, "{report:?}");
        assert_eq!(
            second.parts[0], first.parts[0],
            "the carried part's ref, key_index included, is forwarded unchanged"
        );
        assert_eq!(
            report.key_index_ranged_gets, 2,
            "two GETs for the tail's new entry; the tail's carried entry and the carried part read none"
        );
        assert_eq!(report.key_index_leaf_gets, 1, "{report:?}");
        assert_eq!(report.key_index_entries_carried, 1, "{report:?}");
        assert_eq!(second.parts[1].key_index.len(), 1);
    }

    /// A store that records every GET's key, range and returned length.
    struct RangeRecordingStore {
        inner: MemoryStore,
        gets: std::sync::Mutex<Vec<(String, GetRange, u64)>>,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for RangeRecordingStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            let got = self.inner.get(key, range).await;
            let len = got.as_ref().map_or(0, |g| g.data.len() as u64);
            self.gets
                .lock()
                .expect("lock")
                .push((key.to_string(), range, len));
            got
        }

        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// Fold cost of the leaf build: on three synthetic logs objects of 64
    /// rows, 8 rows to a block, each row padded with 512 hex characters, the
    /// fold issues exactly two ranged GETs per entry, a footer suffix of
    /// `min(8 KiB, object size)` and the KEY_IDX section's exact range, and
    /// reads exactly the sum of those lengths. No leaf-build GET reads a whole
    /// object.
    ///
    /// Fixture figures (pinned below): objects of 22,330, 22,347 and 22,330
    /// bytes (67,007 in all), KEY_IDX sections of 2,912 bytes each (8,736,
    /// 13.0% of the object bytes), 33,312 bytes read by the build (3 x 8,192
    /// suffix plus the sections). The ratio is a property of this synthetic
    /// geometry (64 distinct I64 values in 64 rows per object, so the
    /// 256-bucket section directory dominates), not of a real corpus; ADR-2707
    /// pre-registers 1 to 2% for logs on real corpora.
    #[tokio::test]
    async fn leaf_build_issues_two_ranged_gets_per_entry_and_reads_exactly_the_sections() {
        let store = Arc::new(RangeRecordingStore {
            inner: MemoryStore::new(),
            gets: std::sync::Mutex::new(Vec::new()),
        });
        declare_user_id_key_index(store.as_ref()).await;
        let mut objects = Vec::new();
        for seq in 1..=3u64 {
            let user_ids: Vec<i64> = (0..64).map(|i| (seq as i64) * 1_000 + i).collect();
            let (record, bytes) =
                publish_user_id_logs(store.as_ref(), seq, KIDX_HOUR, &user_ids, 8, 512, true).await;
            let footer = ravel_logseg::footer::open(&bytes).expect("footer");
            let section = *footer
                .section(ravel_logseg::footer::kind::KEY_IDX)
                .expect("KEY_IDX section");
            let key = keys::reconstruct_data_key(&record).expect("data key");
            objects.push((key, bytes.len() as u64, section));
        }
        store.gets.lock().expect("lock").clear();
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        let report = fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;

        let object_bytes: u64 = objects.iter().map(|(_, size, _)| size).sum();
        let section_bytes: u64 = objects.iter().map(|(_, _, s)| s.len).sum();
        let expected_bytes: u64 = objects
            .iter()
            .map(|(_, size, s)| (*size).min(KEY_INDEX_FOOTER_SUFFIX_LEN) + s.len)
            .sum();
        assert_eq!(report.key_index_ranged_gets, 6, "{report:?}");
        assert_eq!(report.key_index_bytes_read, expected_bytes, "{report:?}");

        let gets = store.gets.lock().expect("lock").clone();
        for (key, size, section) in &objects {
            let ranged: Vec<(GetRange, u64)> = gets
                .iter()
                .filter(|(k, range, _)| k == key && !matches!(range, GetRange::Full))
                .map(|(_, range, len)| (*range, *len))
                .collect();
            let suffix = (*size).min(KEY_INDEX_FOOTER_SUFFIX_LEN);
            assert_eq!(
                ranged,
                vec![
                    (GetRange::Suffix(suffix), suffix),
                    (
                        GetRange::Range(section.offset, section.offset + section.len),
                        section.len
                    ),
                ],
                "{key}"
            );
        }
        // The fixture geometry the doc comment states.
        let sizes: Vec<u64> = objects.iter().map(|(_, size, _)| *size).collect();
        let sections: Vec<u64> = objects.iter().map(|(_, _, s)| s.len).collect();
        assert_eq!(sizes, vec![22_330, 22_347, 22_330]);
        assert_eq!(sections, vec![2_912, 2_912, 2_912]);
        assert_eq!((object_bytes, section_bytes), (67_007, 8_736));
        assert_eq!(expected_bytes, 33_312);
        assert!(sizes.iter().all(|size| *size > KEY_INDEX_FOOTER_SUFFIX_LEN));
    }

    fn recording_store() -> Arc<RangeRecordingStore> {
        Arc::new(RangeRecordingStore {
            inner: MemoryStore::new(),
            gets: std::sync::Mutex::new(Vec::new()),
        })
    }

    /// Ranged GETs of data objects (every key outside the catalog prefix) the
    /// store has seen: the reads the key-index build issues, and nothing else
    /// a fold issues reads a data object by range.
    fn data_object_ranged_gets(store: &RangeRecordingStore) -> Vec<(String, GetRange)> {
        store
            .gets
            .lock()
            .expect("lock")
            .iter()
            .filter(|(key, range, _)| {
                !key.contains("/catalog/") && !matches!(range, GetRange::Full)
            })
            .map(|(key, range, _)| (key.clone(), *range))
            .collect()
    }

    pub(crate) async fn kidx_objects(
        store: &dyn ObjectStoreBackend,
        signal: Signal,
    ) -> Vec<String> {
        let prefix = format!("t/{}/catalog/{}/", tenant().to_hex(), signal.key_prefix());
        list_all_keys(store, &prefix)
            .await
            .into_iter()
            .filter(|key| key.ends_with(".kidx"))
            .collect()
    }

    /// The switch is off by default, and off the fold reads no KEY_IDX
    /// section and writes no leaf, for a logs tenant that declares a
    /// key-index column and for spans, whose objects always carry a KEY_IDX
    /// section: zero ranged GETs of any data object, no `.kidx` object, and
    /// no `key_index` ref on the part.
    #[tokio::test]
    async fn the_leaf_build_is_off_by_default_and_reads_no_section() {
        assert!(!CatalogConfig::default().build_key_index_leaves);
        let store = recording_store();
        declare_user_id_key_index(store.as_ref()).await;
        publish_user_id_logs(store.as_ref(), 1, KIDX_HOUR, &[10, 20], 2, 0, true).await;
        publish_user_id_logs(store.as_ref(), 2, KIDX_HOUR, &[10, 30], 2, 0, true).await;
        publish_trace_spans(store.as_ref(), 3, &[[0x11u8; 16], [0x22u8; 16]]).await;
        publish_trace_spans(store.as_ref(), 4, &[[0x11u8; 16]]).await;
        store.gets.lock().expect("lock").clear();
        let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
        for signal in [Signal::Logs, Signal::Spans] {
            let report = fold_through(&catalog, signal, KIDX_HOUR).await;
            assert_eq!(report.entry_count, 2, "{signal:?}: {report:?}");
            assert_eq!(report.key_index_ranged_gets, 0, "{signal:?}: {report:?}");
            assert_eq!(report.key_index_bytes_read, 0, "{signal:?}: {report:?}");
            assert_eq!(report.key_index_leaves_written, 0, "{signal:?}: {report:?}");
            assert_eq!(
                report.key_index_uncovered_entries, 0,
                "{signal:?}: {report:?}"
            );
            let head = read_signal_head(store.as_ref(), signal).await;
            assert_eq!(head.parts.len(), 1);
            assert!(head.parts[0].key_index.is_empty(), "{signal:?}");
            assert_eq!(
                kidx_objects(store.as_ref(), signal).await,
                Vec::<String>::new()
            );
        }
        assert_eq!(data_object_ranged_gets(&store), Vec::new());
    }

    /// A part the fold re-encodes with the switch off carries no `key_index`
    /// refs, even when the part it replaces had them, while a part carried
    /// forward by reference keeps its refs unchanged. The first fold runs with
    /// the switch on and seals hour 1's two entries into a part of their own;
    /// the second runs with it off, re-encodes the tail, and reads no section.
    #[tokio::test]
    async fn a_part_re_encoded_with_the_switch_off_drops_its_key_index_refs() {
        let h1 = KIDX_HOUR;
        let store = recording_store();
        declare_user_id_key_index(store.as_ref()).await;
        publish_user_id_logs(store.as_ref(), 1, h1, &[10, 20], 2, 0, true).await;
        publish_user_id_logs(store.as_ref(), 2, h1, &[10, 30], 2, 0, true).await;
        publish_user_id_logs(store.as_ref(), 3, h1 + 1, &[40], 2, 0, true).await;
        let on = Catalog::new(
            store.clone(),
            CatalogConfig {
                snapshot_part_max_entries: 2,
                ..kidx_config()
            },
        )
        .expect("catalog");
        fold_through(&on, Signal::Logs, h1 + 1).await;
        let first = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(first.parts.len(), 2);
        assert_eq!(first.parts[0].key_index.len(), 1);
        assert_eq!(first.parts[1].key_index.len(), 1, "the tail had a leaf");

        publish_user_id_logs(store.as_ref(), 4, h1 + 2, &[50], 2, 0, true).await;
        store.gets.lock().expect("lock").clear();
        let off = Catalog::new(
            store.clone(),
            CatalogConfig {
                snapshot_part_max_entries: 2,
                ..config(1)
            },
        )
        .expect("catalog");
        let report = fold_through(&off, Signal::Logs, h1 + 2).await;
        let second = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(report.parts_reused, 1, "{report:?}");
        assert_eq!(
            second.parts[0], first.parts[0],
            "the carried part keeps its key_index refs"
        );
        assert_eq!(second.parts.len(), 2);
        assert_ne!(
            second.parts[1].blake3, first.parts[1].blake3,
            "the tail was re-encoded"
        );
        assert!(
            second.parts[1].key_index.is_empty(),
            "the re-encoded tail carries no key_index ref: {:?}",
            second.parts[1].key_index
        );
        assert_eq!(report.key_index_ranged_gets, 0, "{report:?}");
        assert_eq!(report.key_index_leaves_written, 0, "{report:?}");
        assert_eq!(data_object_ranged_gets(&store), Vec::new());
    }

    /// With the switch on, a logs tenant that declares no key-index field (no
    /// tenant config at all, or typed columns none of which carries
    /// `key_index`, with `index_trace_id` off) reads no KEY_IDX section and
    /// writes no leaf, though its objects carry one.
    #[tokio::test]
    async fn a_logs_tenant_declaring_no_key_index_field_reads_no_section() {
        use crate::tenant_config::{DeclaredColumnType, TenantConfig, TenantLifecycleState};
        for declare_typed_column in [false, true] {
            let store = recording_store();
            if declare_typed_column {
                let cfg = TenantConfig {
                    typed_attr_columns: Some(vec![DeclaredTypedColumn {
                        key: USER_ID.to_string(),
                        ty: DeclaredColumnType::I64,
                    }]),
                    ..TenantConfig::new(TenantLifecycleState::Active)
                };
                assert!(!cfg.index_trace_id());
                crate::tenant_config::set_tenant_config(store.as_ref(), &tenant(), &cfg, 1)
                    .await
                    .expect("write tenant config");
            }
            publish_user_id_logs(store.as_ref(), 1, KIDX_HOUR, &[10, 20], 2, 0, true).await;
            publish_user_id_logs(store.as_ref(), 2, KIDX_HOUR, &[10, 30], 2, 0, true).await;
            store.gets.lock().expect("lock").clear();
            let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
            let report = fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
            let case = format!("typed column declared: {declare_typed_column}");
            assert_eq!(report.entry_count, 2, "{case}: {report:?}");
            assert_eq!(report.key_index_ranged_gets, 0, "{case}: {report:?}");
            assert_eq!(report.key_index_leaves_written, 0, "{case}: {report:?}");
            assert_eq!(report.key_index_uncovered_entries, 0, "{case}: {report:?}");
            let head = read_signal_head(store.as_ref(), Signal::Logs).await;
            assert!(head.parts[0].key_index.is_empty(), "{case}");
            assert_eq!(
                kidx_objects(store.as_ref(), Signal::Logs).await,
                Vec::<String>::new(),
                "{case}"
            );
            assert_eq!(data_object_ranged_gets(&store), Vec::new(), "{case}");
        }
    }

    /// A fold whose first HEAD CAS is lost reads each object's KEY_IDX section
    /// once in all: the attempt that publishes re-encodes the same part and
    /// takes the leaf refs the lost attempt built for it. Three entries: three
    /// footer suffix GETs and three section GETs, not six of each, and the
    /// leaf counters report the one leaf PUT.
    #[tokio::test]
    async fn a_lost_head_cas_reuses_the_section_reads() {
        let inner = recording_store();
        declare_user_id_key_index(inner.as_ref()).await;
        for seq in 1..=3u64 {
            publish_user_id_logs(
                inner.as_ref(),
                seq,
                KIDX_HOUR,
                &[10, 20 + seq as i64],
                2,
                0,
                true,
            )
            .await;
        }
        inner.gets.lock().expect("lock").clear();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite)
                .with_key_contains(head_object_key(&tenant(), Signal::Logs))
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = Arc::new(FaultStore::new(Arc::clone(&inner), plan));
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        let report = fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
        assert_eq!(
            store.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
            1,
            "the first HEAD write was lost"
        );
        assert_eq!(report.entry_count, 3, "{report:?}");
        let ranged = data_object_ranged_gets(&inner);
        let suffixes = ranged
            .iter()
            .filter(|(_, range)| matches!(range, GetRange::Suffix(_)))
            .count();
        assert_eq!((suffixes, ranged.len()), (3, 6), "{ranged:?}");
        assert_eq!(report.key_index_ranged_gets, 6, "{report:?}");
        assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(head.parts[0].key_index.len(), 1);
        assert_eq!(
            report.key_index_leaf_bytes_written,
            head.parts[0].key_index[0].size
        );
    }

    /// GETs of any range the recording store saw on a key containing
    /// `pattern`.
    fn gets_containing(store: &RangeRecordingStore, pattern: &str) -> usize {
        store
            .gets
            .lock()
            .expect("lock")
            .iter()
            .filter(|(key, _, _)| key.contains(pattern))
            .count()
    }

    /// `part`'s leaves as stored, in HEAD order.
    async fn stored_leaves(store: &dyn ObjectStoreBackend, part: &SnapshotPartRef) -> Vec<Vec<u8>> {
        let mut leaves = Vec::new();
        for leaf in &part.key_index {
            let got = store
                .get(&leaf.key, GetRange::Full)
                .await
                .expect("the leaf HEAD names is stored");
            leaves.push(got.data.to_vec());
        }
        leaves
    }

    /// `part`'s `user_id` leaves as [`build_part_key_index_leaves`] builds
    /// them from every entry's section, under the limits the fold builds
    /// with: the from-scratch build the incremental one must equal.
    async fn from_scratch_leaves(
        store: &dyn ObjectStoreBackend,
        part: &SnapshotPartRef,
    ) -> Vec<Vec<u8>> {
        from_scratch_leaves_declaring(store, part, &[(USER_ID.to_string(), KeyType::I64)]).await
    }

    /// `part`'s `user_id` leaves as the one-call build writes them under the
    /// pair budget `max_pairs`.
    async fn user_id_leaves_at(
        store: &dyn ObjectStoreBackend,
        part: &SnapshotPartRef,
        max_pairs: u64,
    ) -> Vec<Vec<u8>> {
        reference_leaves(
            store,
            part,
            &[(USER_ID.to_string(), KeyType::I64)],
            max_pairs,
            &[],
        )
        .await
    }

    /// [`from_scratch_leaves`] for a tenant declaring `declared`, the fields
    /// the sections name besides included, as the fold builds them.
    async fn from_scratch_leaves_declaring(
        store: &dyn ObjectStoreBackend,
        part: &SnapshotPartRef,
        declared: &[(String, KeyType)],
    ) -> Vec<Vec<u8>> {
        reference_leaves(
            store,
            part,
            declared,
            kidx_config().key_index_build_max_pairs,
            &[],
        )
        .await
    }

    /// `part`'s leaves as [`build_part_key_index_leaves`] builds them in one
    /// call under the pair budget `max_pairs`, for a tenant declaring
    /// `declared`, with the read of every entry in `failed` failed.
    async fn reference_leaves(
        store: &dyn ObjectStoreBackend,
        part: &SnapshotPartRef,
        declared: &[(String, KeyType)],
        max_pairs: u64,
        failed: &[u64],
    ) -> Vec<Vec<u8>> {
        let got = store
            .get(&part.key, GetRange::Full)
            .await
            .expect("part present");
        let entries = snapshot_format::decode_part(&got.data, &PartLimits::default())
            .expect("part decodes")
            .entries;
        let mut reads = Vec::new();
        for (ordinal, entry) in (0u64..).zip(&entries) {
            if failed.contains(&ordinal) {
                reads.push(Err("failed".to_string()));
                continue;
            }
            let key = column_stats_build::segment_data_key(&tenant(), Signal::Logs, entry)
                .expect("data key");
            reads.push(
                read_entry_key_index(store, &tenant(), Signal::Logs, &key, entry.object_size)
                    .await
                    .fields,
            );
        }
        let part_blake3: [u8; 32] = part.blake3.as_slice().try_into().expect("32-byte hash");
        build_part_key_index_leaves(
            &tenant(),
            Signal::Logs,
            part_blake3,
            declared,
            true,
            &reads.iter().collect::<Vec<_>>(),
            &key_index_leaf_limits(),
            max_pairs,
        )
        .leaves
        .into_iter()
        .map(|leaf| leaf.bytes)
        .collect()
    }

    /// Folds logs at `now_ns`, sealing through `seal_through_hour`.
    async fn fold_logs_sealing(
        catalog: &Catalog,
        now_ns: i64,
        seal_through_hour: Option<u32>,
    ) -> FoldReport {
        catalog
            .fold_with_seal_through(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_ns,
                &[],
                None,
                &RefoldRequest::new(),
                seal_through_hour,
            )
            .await
            .expect("fold")
    }

    /// The fold after [`advancing_fixture`]: five minutes after its first
    /// fold, inside [`KIDX_HOUR`]'s natural margin, so it re-lists the
    /// held-open hour and picks the late writers up.
    async fn advancing_fold(catalog: &Catalog) -> FoldReport {
        fold_logs_sealing(
            catalog,
            now_inside(KIDX_HOUR) + 5 * 60 * 1_000_000_000,
            None,
        )
        .await
    }

    /// The incremental-build fixture: an operator-sealed fold over writers
    /// 10, 20 and 30 in [`KIDX_HOUR`] leaves one part with one leaf, then
    /// writers 15 and 35 publish late into the same held-open hour. Writer 15
    /// sorts between 10 and 20, so the next fold's tail is 10, 15, 20, 30, 35
    /// and writers 20 and 30 move from ordinals 1 and 2 to 2 and 3. Every
    /// writer's values include 7. Returns the first HEAD, with the GET record
    /// cleared.
    async fn advancing_fixture(inner: &Arc<RangeRecordingStore>) -> SnapshotHead {
        declare_user_id_key_index(inner.as_ref()).await;
        for seq in [10u64, 20, 30] {
            let own = seq as i64;
            publish_user_id_logs(
                inner.as_ref(),
                seq,
                KIDX_HOUR,
                &[7, own, own + 1],
                2,
                0,
                true,
            )
            .await;
        }
        let catalog = Catalog::new(inner.clone(), kidx_config()).expect("catalog");
        fold_logs_sealing(&catalog, now_inside(KIDX_HOUR), Some(KIDX_HOUR)).await;
        let first = read_signal_head(inner.as_ref(), Signal::Logs).await;
        assert_eq!(first.parts.len(), 1);
        assert_eq!(first.parts[0].key_index.len(), 1);
        assert_eq!(first.parts[0].key_index[0].uncovered_count, 0);
        publish_user_id_logs(inner.as_ref(), 15, KIDX_HOUR, &[7, 15, 16], 2, 0, true).await;
        publish_user_id_logs(inner.as_ref(), 35, KIDX_HOUR, &[7, 35, 36], 2, 0, true).await;
        inner.gets.lock().expect("lock").clear();
        first
    }

    /// ADR-2707's incremental tail build amendment: an advancing fold GETs
    /// the previous tail's one leaf, reads the KEY_IDX sections of only the
    /// two entries that leaf does not cover (two ranged GETs each, the
    /// objects being shorter than the footer suffix), carries the other
    /// three by data object key, and writes the leaf a build from every
    /// section writes, byte for byte, in one pass. A carry by ordinal gives
    /// writer 15 writer 20's pairs (see [`advancing_fixture`]) and the leaf
    /// differs.
    #[tokio::test]
    async fn advancing_fold_reads_sections_for_new_entries_only() {
        let inner = recording_store();
        let first = advancing_fixture(&inner).await;
        let store = Arc::new(FaultStore::new(Arc::clone(&inner), FaultPlan::empty()));
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        let report = advancing_fold(&catalog).await;
        assert_eq!(report.entry_count, 5, "{report:?}");
        let second = read_signal_head(inner.as_ref(), Signal::Logs).await;
        assert_eq!(second.parts.len(), 1, "one re-encoded tail");

        let section_gets = data_object_ranged_gets(&inner);
        assert_eq!(section_gets.len(), 2 * 2, "{section_gets:?}");
        assert_eq!(report.key_index_sections_read, 2, "{report:?}");
        assert_eq!(report.key_index_ranged_gets, 4, "{report:?}");
        assert_eq!(
            gets_containing(&inner, ".kidx"),
            first.parts[0].key_index.len(),
            "one GET per previous leaf"
        );
        assert_eq!(report.key_index_leaf_gets, 1, "{report:?}");
        assert_eq!(report.key_index_build_passes, 1, "{report:?}");
        let previous_covered =
            first.parts[0].entry_count - first.parts[0].key_index[0].uncovered_count;
        assert_eq!(
            report.key_index_entries_carried, previous_covered,
            "{report:?}"
        );
        assert_eq!(previous_covered, 3);
        assert_eq!(
            stored_leaves(inner.as_ref(), &second.parts[0]).await,
            from_scratch_leaves(inner.as_ref(), &second.parts[0]).await,
            "the carried build writes the from-scratch leaf"
        );
        let ordinal = part_ordinals(inner.as_ref(), &second.parts[0]).await;
        assert_eq!(
            probe_part(
                &catalog,
                Signal::Logs,
                &second.parts[0],
                KeyType::I64,
                USER_ID,
                key8_i64(20)
            )
            .await,
            matches(&[(ordinal[&20], &[0])], &[])
        );
    }

    /// A previous leaf whose GET fails is not carried: the fold reads every
    /// entry's section, counts each under `key_index_sections_read`, carries
    /// nothing, and writes the from-scratch leaf.
    #[tokio::test]
    async fn a_failed_previous_leaf_get_falls_back_to_section_reads() {
        let inner = recording_store();
        advancing_fixture(&inner).await;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("leaf GET refused".into()))
                .with_key_contains(".kidx")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = Arc::new(FaultStore::new(Arc::clone(&inner), plan));
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        let report = advancing_fold(&catalog).await;
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Permanent),
            1,
            "the previous leaf GET was refused"
        );
        assert_eq!(report.key_index_leaf_gets, 1, "{report:?}");
        assert_eq!(report.key_index_sections_read, 5, "{report:?}");
        assert_eq!(data_object_ranged_gets(&inner).len(), 5 * 2);
        assert_eq!(report.key_index_entries_carried, 0, "{report:?}");
        let second = read_signal_head(inner.as_ref(), Signal::Logs).await;
        assert_eq!(
            stored_leaves(inner.as_ref(), &second.parts[0]).await,
            from_scratch_leaves(inner.as_ref(), &second.parts[0]).await
        );
    }

    /// A previous leaf with one byte flipped fails verification and is not
    /// carried: every entry's section is read and counted, and the fold
    /// writes the from-scratch leaf.
    #[tokio::test]
    async fn a_corrupt_previous_leaf_falls_back_to_section_reads() {
        let inner = recording_store();
        let first = advancing_fixture(&inner).await;
        let leaf_key = &first.parts[0].key_index[0].key;
        let mut bytes = inner
            .inner
            .get(leaf_key, GetRange::Full)
            .await
            .expect("leaf stored")
            .data
            .to_vec();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        inner
            .inner
            .put(leaf_key, Bytes::from(bytes), PutOptions::default())
            .await
            .expect("overwrite the leaf");
        let catalog = Catalog::new(inner.clone(), kidx_config()).expect("catalog");
        let report = advancing_fold(&catalog).await;
        assert_eq!(report.key_index_leaf_gets, 1, "{report:?}");
        assert_eq!(report.key_index_sections_read, 5, "{report:?}");
        assert_eq!(data_object_ranged_gets(&inner).len(), 5 * 2);
        assert_eq!(report.key_index_entries_carried, 0, "{report:?}");
        let second = read_signal_head(inner.as_ref(), Signal::Logs).await;
        assert_eq!(
            stored_leaves(inner.as_ref(), &second.parts[0]).await,
            from_scratch_leaves(inner.as_ref(), &second.parts[0]).await
        );
    }

    /// Folds three entries of four distinct `user_id` values each (twelve
    /// (entry, key8) pairs, every key8 in one entry, values interleaved so
    /// each key8 range spans all three entries) under `max_pairs` and a
    /// 40-byte body ceiling, which holds three 11-byte entries to a slice.
    async fn budget_fixture_fold(
        max_pairs: u64,
    ) -> (Arc<RangeRecordingStore>, FoldReport, SnapshotHead) {
        let store = recording_store();
        declare_user_id_key_index(store.as_ref()).await;
        for seq in 1..=3u64 {
            let own = seq as i64;
            publish_user_id_logs(
                store.as_ref(),
                seq,
                KIDX_HOUR,
                &[own, own + 3, own + 6, own + 9],
                2,
                0,
                true,
            )
            .await;
        }
        store.gets.lock().expect("lock").clear();
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                key_index_build_max_pairs: max_pairs,
                ..kidx_config()
            },
        )
        .expect("catalog");
        set_key_index_leaf_body_ceiling_for_test(Some(40));
        take_key_index_pass_log_for_test();
        let report = fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        (store, report, head)
    }

    /// A held (entry, key8) pair is one 40-byte `KeyIndexLeafEntry` in its
    /// field's heap buffer, the per-pair struct term of the bound
    /// `CatalogConfig::key_index_build_max_pairs` states. The buffer and
    /// blocks terms are asserted on every pass by [`pass_ranges`].
    #[test]
    fn a_held_pair_is_forty_bytes_in_the_heap_buffer() {
        assert_eq!(std::mem::size_of::<HeldPair>(), 40);
        assert_eq!(std::mem::size_of::<KeyIndexLeafEntry>(), 40);
    }

    /// The `(attempt, lo, end)` of each of `field`'s passes in `log`, after
    /// asserting the claims the pair budget rests on, pass by pass: the held
    /// pairs, read and carried, are at most the pass's share unless they are
    /// one key8, and at any moment at most the larger of the share and
    /// `max_run` (the fixture's largest key8 run) plus the one pair placed
    /// before its cut; the heap buffer is at most twice the most pairs it
    /// held (4 at least); every held pair's blocks `Vec` has capacity equal
    /// to its length. Within each attempt the ranges tile the key space: the
    /// first starts at 0, each starts where the last ended and the last runs
    /// to `u64::MAX`.
    fn pass_ranges(
        log: &[KeyIndexPassRecord],
        field: &str,
        max_run: u64,
    ) -> Vec<(u32, u64, Option<u64>)> {
        let records: Vec<&KeyIndexPassRecord> = log.iter().filter(|r| r.field == field).collect();
        assert!(!records.is_empty(), "no pass of {field}: {log:?}");
        for record in &records {
            assert!(
                record.held <= record.share || record.one_key8,
                "a pass held more than its share over several key8s: {record:?}"
            );
            assert!(
                record.held_peak <= record.share.max(max_run).saturating_add(1),
                "{record:?}"
            );
            assert!(
                record.held_capacity <= (2 * record.held_peak).max(4),
                "{record:?}"
            );
            assert!(record.blocks_exact, "{record:?}");
        }
        let mut attempts: BTreeMap<u32, Vec<&KeyIndexPassRecord>> = BTreeMap::new();
        for record in &records {
            attempts.entry(record.attempt).or_default().push(record);
        }
        for (attempt, passes) in &attempts {
            assert_eq!(passes[0].lo, 0, "attempt {attempt}: {passes:?}");
            for pair in passes.windows(2) {
                assert_eq!(
                    pair[0].end,
                    Some(pair[1].lo),
                    "attempt {attempt}: {passes:?}"
                );
            }
            assert_eq!(
                passes.last().and_then(|p| p.end),
                None,
                "attempt {attempt}: {passes:?}"
            );
        }
        records.iter().map(|r| (r.attempt, r.lo, r.end)).collect()
    }

    /// The pass rule for one field: greedy over its key8 runs in ascending
    /// order, a pass taking whole runs while their pairs fit `budget`, and a
    /// run over `budget` a pass of its own.
    fn greedy_passes(run_pairs: &[u64], budget: u64) -> u64 {
        let mut passes = 0u64;
        let mut held = 0u64;
        for &pairs in run_pairs {
            if held > 0 && held + pairs > budget {
                passes += 1;
                held = 0;
            }
            held += pairs;
        }
        passes + u64::from(held > 0)
    }

    /// The shared-key8 fixture: six logs objects in [`KIDX_HOUR`] whose
    /// `user_id` sets share key8s 7 (four entries) and 20 (four entries),
    /// 17 pairs over 11 key8 runs.
    const SHARED_KEY8_USER_IDS: [&[i64]; 6] = [
        &[1, 7, 20],
        &[2, 7, 20],
        &[3, 7, 20],
        &[4, 7, 21],
        &[5, 20, 22],
        &[6, 30],
    ];
    /// Pairs per key8 run of [`SHARED_KEY8_USER_IDS`], ascending: 1 to 6,
    /// 7, 20, 21, 22, 30.
    const SHARED_KEY8_RUN_PAIRS: [u64; 11] = [1, 1, 1, 1, 1, 1, 4, 4, 1, 1, 1];

    /// Folds [`SHARED_KEY8_USER_IDS`] under `max_pairs` and the body ceiling
    /// `ceiling` (the default when `None`), returning the store, the report,
    /// HEAD and the pass log.
    async fn shared_key8_fold(
        max_pairs: u64,
        ceiling: Option<u64>,
    ) -> (
        Arc<RangeRecordingStore>,
        FoldReport,
        SnapshotHead,
        Vec<KeyIndexPassRecord>,
    ) {
        let store = recording_store();
        declare_user_id_key_index(store.as_ref()).await;
        for (seq, user_ids) in (1u64..).zip(SHARED_KEY8_USER_IDS) {
            publish_user_id_logs(store.as_ref(), seq, KIDX_HOUR, user_ids, 2, 0, true).await;
        }
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                key_index_build_max_pairs: max_pairs,
                ..kidx_config()
            },
        )
        .expect("catalog");
        set_key_index_leaf_body_ceiling_for_test(ceiling);
        take_key_index_pass_log_for_test();
        let report = fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
        let log = take_key_index_pass_log_for_test();
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        (store, report, head, log)
    }

    /// Byte identity in every budget (ADR-3692 decision 5): under a 40-byte
    /// body ceiling key8 7 and key8 20 each carry four entries, 44 bytes, so
    /// both are over the ceiling and uncover entries 0 to 4. A small budget
    /// meets them in different passes, after pairs already reached a slice;
    /// the build scans every remaining range before its one restart, so for
    /// every budget from 1 to 20 and `u64::MAX` it stores the leaves the
    /// one-call build writes at the same budget, every one listing the five
    /// uncovered entries, and at `u64::MAX` that is one leaf. A restart on
    /// the first finding meets key8 20 again in the rebuild and writes no
    /// leaf for budgets 1 to 7.
    #[tokio::test]
    async fn a_restart_sees_every_uncovered_entry_at_every_budget() {
        for max_pairs in (1..=20).chain([u64::MAX]) {
            let (store, report, head, log) = shared_key8_fold(max_pairs, Some(40)).await;
            let part = &head.parts[0];
            assert!(!part.key_index.is_empty(), "budget {max_pairs}: {report:?}");
            assert!(
                part.key_index.iter().all(|leaf| leaf.uncovered_count == 5),
                "budget {max_pairs}: {part:?}"
            );
            assert_eq!(report.key_index_uncovered_entries, 5, "budget {max_pairs}");
            assert_eq!(
                stored_leaves(store.as_ref(), part).await,
                user_id_leaves_at(store.as_ref(), part, max_pairs).await,
                "budget {max_pairs}"
            );
            if max_pairs == u64::MAX {
                assert_eq!(part.key_index.len(), 1, "{report:?}");
            }
            pass_ranges(&log, USER_ID, 4);
        }
        set_key_index_leaf_body_ceiling_for_test(None);
    }

    /// The pass rule (ADR-3692 decision 5, the #3704 amendment): a field's
    /// passes are the greedy partition of its key8 runs under the budget, not
    /// ceil(pairs / budget) once a key8 is shared across entries. 17 pairs
    /// at budget 6 take 4 passes, [1, 7), [7, 20), [20, 30) and [30, MAX],
    /// where ceil(17 / 6) is 3; every budget from 1 to 20 matches the greedy
    /// count. A restart doubles it: under a 40-byte ceiling the first
    /// attempt's 4 passes find the uncovered entries and the rebuild's 4
    /// write the leaf, 8 in all.
    #[tokio::test]
    async fn the_pass_count_is_the_greedy_key8_partition() {
        let k = |v: i64| u64::from_be_bytes(key8_i64(v));
        let (_, report, _, log) = shared_key8_fold(6, None).await;
        assert_eq!(report.key_index_build_passes, 4, "{report:?}");
        assert_eq!(17u64.div_ceil(6), 3);
        assert_eq!(
            pass_ranges(&log, USER_ID, 4),
            vec![
                (0, 0, Some(k(7))),
                (0, k(7), Some(k(20))),
                (0, k(20), Some(k(30))),
                (0, k(30), None),
            ]
        );
        assert_eq!(report.key_index_sections_read, 6 * 4, "{report:?}");
        for max_pairs in 1..=20u64 {
            let (_, report, _, log) = shared_key8_fold(max_pairs, None).await;
            assert_eq!(
                report.key_index_build_passes,
                greedy_passes(&SHARED_KEY8_RUN_PAIRS, max_pairs),
                "budget {max_pairs}"
            );
            pass_ranges(&log, USER_ID, 4);
        }

        let (_, restarted, _, log) = shared_key8_fold(6, Some(40)).await;
        assert_eq!(restarted.key_index_build_passes, 8, "{restarted:?}");
        let ranges = pass_ranges(&log, USER_ID, 4);
        assert_eq!(ranges.iter().filter(|r| r.0 == 0).count(), 4);
        assert_eq!(ranges.iter().filter(|r| r.0 == 1).count(), 4);
        set_key_index_leaf_body_ceiling_for_test(None);
    }

    /// `key_index_leaves_written` and `key_index_leaf_bytes_written` are the
    /// published leaf set: at budget 1 under a 40-byte ceiling the first
    /// attempt PUTs slices it then abandons (the restart), and the store holds
    /// more `.kidx` objects than HEAD names, but the report counts exactly
    /// the leaf HEAD names and its bytes.
    #[tokio::test]
    async fn the_leaves_written_are_the_published_set() {
        let (store, report, head, _) = shared_key8_fold(1, Some(40)).await;
        set_key_index_leaf_body_ceiling_for_test(None);
        let part = &head.parts[0];
        let stored = kidx_objects(store.as_ref(), Signal::Logs).await;
        assert!(
            stored.len() > part.key_index.len(),
            "the first attempt PUT leaves it abandoned: {stored:?}"
        );
        assert_eq!(
            report.key_index_leaves_written,
            part.key_index.len() as u64,
            "{report:?}"
        );
        assert_eq!(
            report.key_index_leaf_bytes_written,
            part.key_index.iter().map(|leaf| leaf.size).sum::<u64>(),
            "{report:?}"
        );
    }

    /// Carried pairs count against the budget (ADR-3692 decision 5): the
    /// advancing fixture's tail holds 15 pairs, 9 carried and 6 read, key8 7
    /// in all five entries. Budget 5 takes 3 passes, [7], [10 to 20] and
    /// [21 to 36], each holding exactly 5 pairs, so no pass holds more than
    /// the budget of read and carried pairs together. The fold GETs the one
    /// previous leaf once and decodes it from memory in each pass, reads the
    /// two new sections in each pass, and writes one leaf per pass, the
    /// leaves the one-call build writes at budget 5. Counting only read
    /// pairs, the 6 read pairs fit 2 passes.
    #[tokio::test]
    async fn carried_pairs_count_against_the_pair_budget() {
        let (inner, report, log) = advancing_budget_fold(5).await;
        let k = |v: i64| u64::from_be_bytes(key8_i64(v));
        assert_eq!(
            pass_ranges(&log, USER_ID, 5),
            vec![
                (0, 0, Some(k(10))),
                (0, k(10), Some(k(21))),
                (0, k(21), None),
            ]
        );
        assert!(log.iter().all(|r| r.held == 5), "{log:?}");
        assert_eq!(report.key_index_build_passes, 3, "{report:?}");
        assert_eq!(report.key_index_entries_carried, 3, "{report:?}");
        assert_eq!(report.key_index_leaf_gets, 1, "{report:?}");
        assert_eq!(report.key_index_sections_read, 2 * 3, "{report:?}");
        // Two ranged GETs per section read, the objects being shorter than
        // the footer suffix: 2 entries x 2 GETs x 3 passes.
        assert_eq!(report.key_index_ranged_gets, 2 * 2 * 3, "{report:?}");
        assert_eq!(data_object_ranged_gets(&inner).len(), 2 * 2 * 3);
        assert_eq!(report.key_index_leaves_written, 3, "{report:?}");
        let second = read_signal_head(inner.as_ref(), Signal::Logs).await;
        assert_eq!(
            stored_leaves(inner.as_ref(), &second.parts[0]).await,
            user_id_leaves_at(inner.as_ref(), &second.parts[0], 5).await
        );
    }

    /// The advancing fold of [`advancing_fixture`] under `max_pairs`,
    /// returning the store, the report and the pass log.
    async fn advancing_budget_fold(
        max_pairs: u64,
    ) -> (
        Arc<RangeRecordingStore>,
        FoldReport,
        Vec<KeyIndexPassRecord>,
    ) {
        let inner = recording_store();
        advancing_fixture(&inner).await;
        let catalog = Catalog::new(
            inner.clone(),
            CatalogConfig {
                key_index_build_max_pairs: max_pairs,
                ..kidx_config()
            },
        )
        .expect("catalog");
        take_key_index_pass_log_for_test();
        let report = advancing_fold(&catalog).await;
        let log = take_key_index_pass_log_for_test();
        (inner, report, log)
    }

    /// Asserts the pair budget's bound on every pass in `log`: the pairs the
    /// field's heap held at its peak plus the pairs the field still held once
    /// the pass ended, measured, are at most the larger of the pass's share
    /// and `max_run`, the fixture's largest key8 run, plus the one pair
    /// placed before a cut; and the pairs still held are 0, since every pass
    /// closes its slices. Returns the number of passes checked.
    fn assert_pass_bound(log: &[KeyIndexPassRecord], max_run: u64, what: &str) -> usize {
        assert!(!log.is_empty(), "{what}: no pass recorded");
        for record in log {
            assert!(
                record.held_peak + record.open_slice <= record.share.max(max_run).saturating_add(1),
                "{what}: a pass held more than its share, open slice included: {record:?}"
            );
            assert_eq!(
                record.open_slice, 0,
                "{what}: a pass ended still holding pairs: {record:?}"
            );
        }
        log.len()
    }

    /// The pair budget bounds everything a pass holds, the open slice
    /// included (ADR-3692 decision 5, the #3704 open-slice amendment): on
    /// every pass, heap pairs at their peak plus open-slice pairs are at most
    /// `max(share, largest key8 run) + 1`. Checked from scratch on the
    /// 17-pair shared-key8 fixture (largest run 4) and the 12-pair budget
    /// fixture under its 40-byte ceiling (largest run 1, so ceiling cuts fall
    /// inside passes), and on the advancing fixture's 9 carried and 6 read
    /// pairs (largest run 5), at budgets 1, 2, 3, 5, 6 and `u64::MAX`. The
    /// open slice is measured after each pass ends, as the pairs the field
    /// still holds, so the test fails when a pass's pairs outlive it.
    #[tokio::test]
    async fn every_pass_holds_at_most_its_share_open_slice_included() {
        for max_pairs in [1, 2, 3, 5, 6, u64::MAX] {
            let (_, report, _, log) = shared_key8_fold(max_pairs, None).await;
            let passes = assert_pass_bound(&log, 4, &format!("shared key8, budget {max_pairs}"));
            assert_eq!(passes as u64, report.key_index_build_passes, "{report:?}");

            let (_, report, _) = budget_fixture_fold(max_pairs).await;
            let log = take_key_index_pass_log_for_test();
            set_key_index_leaf_body_ceiling_for_test(None);
            let passes = assert_pass_bound(&log, 1, &format!("budget fixture, budget {max_pairs}"));
            assert_eq!(passes as u64, report.key_index_build_passes, "{report:?}");

            let (_, report, log) = advancing_budget_fold(max_pairs).await;
            let passes = assert_pass_bound(&log, 5, &format!("advancing, budget {max_pairs}"));
            assert_eq!(passes as u64, report.key_index_build_passes, "{report:?}");
            if max_pairs == 1 {
                assert_eq!(passes, 11, "one pass per key8 run of the advancing tail");
            }
        }
    }

    /// The pairs a pass ends with are sorted and encoded in the heap's own
    /// buffer, not copied into a second one: the sorted `Vec` starts at the
    /// heap's buffer address.
    #[test]
    fn a_pass_sorts_its_pairs_in_the_heap_buffer() {
        let mut heap = BinaryHeap::with_capacity(8);
        for (key8, ordinal) in [(3u8, 0u64), (1, 1), (2, 0), (1, 0)] {
            heap.push(HeldPair(KeyIndexLeafEntry {
                key8: [key8; 8],
                entry_ordinal: ordinal,
                blocks: vec![0],
            }));
        }
        let buffer = heap.as_slice().as_ptr().cast::<u8>();
        let sorted = key_index_sorted_pairs(heap);
        assert_eq!(sorted.as_ptr().cast::<u8>(), buffer, "the same allocation");
        assert_eq!(
            sorted
                .iter()
                .map(|e| (e.key8[0], e.entry_ordinal))
                .collect::<Vec<_>>(),
            vec![(1, 0), (1, 1), (2, 0), (3, 0)]
        );
    }

    /// The leaves depend on the budget and the pairs read, not on how the
    /// build got them (ADR-3692 decision 5, the #3704 open-slice amendment).
    /// The advancing fixture's tail is built three ways at each budget:
    /// carrying the previous leaf; from scratch, the previous leaf's GET
    /// refused so every section is read; and through a forced restart,
    /// writer 35's section GET refused in the second pass after the first
    /// pass held pairs. The first two store the leaves the one-call build
    /// writes at the same budget. The restart stores the leaves the
    /// one-call build writes with writer 35's read failed, the same leaves
    /// as a fold whose first-pass read of writer 35 failed and needed no
    /// restart.
    #[tokio::test]
    async fn the_leaves_are_the_same_carried_from_scratch_and_after_a_restart() {
        let writer_35 = {
            let scratch = MemoryStore::new();
            let (commit, _) =
                publish_user_id_logs(&scratch, 35, KIDX_HOUR, &[7, 35, 36], 2, 0, true).await;
            keys::reconstruct_data_key(&commit).expect("data key")
        };
        // Writer 35 sorts last in the tail of 10, 15, 20, 30 and 35.
        let writer_35_ordinal = 4u64;
        let all_pairs: [u64; 11] = [5, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1];
        for max_pairs in [1, 2, 3, 5, 6, 7, 15, u64::MAX] {
            let (carried_store, carried, _) = advancing_budget_fold(max_pairs).await;
            assert_eq!(carried.key_index_leaf_gets, 1, "budget {max_pairs}");
            assert_eq!(carried.key_index_entries_carried, 3, "budget {max_pairs}");
            assert_eq!(
                carried.key_index_build_passes,
                greedy_passes(&all_pairs, max_pairs),
                "budget {max_pairs}: {carried:?}"
            );
            let part = read_signal_head(carried_store.as_ref(), Signal::Logs)
                .await
                .parts
                .remove(0);
            let carried_leaves = stored_leaves(carried_store.as_ref(), &part).await;
            assert_eq!(
                carried_leaves,
                user_id_leaves_at(carried_store.as_ref(), &part, max_pairs).await,
                "budget {max_pairs}: carried"
            );

            let fault_fold = |rule: Rule| async move {
                let inner = recording_store();
                advancing_fixture(&inner).await;
                let store = Arc::new(FaultStore::new(
                    Arc::clone(&inner),
                    FaultPlan::empty().with_rule(rule),
                ));
                let catalog = Catalog::new(
                    store.clone(),
                    CatalogConfig {
                        key_index_build_max_pairs: max_pairs,
                        ..kidx_config()
                    },
                )
                .expect("catalog");
                take_key_index_pass_log_for_test();
                let report = advancing_fold(&catalog).await;
                let log = take_key_index_pass_log_for_test();
                assert_eq!(
                    store.fault_count(Op::Get, FaultKind::Permanent),
                    1,
                    "budget {max_pairs}: the fault fired"
                );
                let part = read_signal_head(inner.as_ref(), Signal::Logs)
                    .await
                    .parts
                    .remove(0);
                let leaves = stored_leaves(inner.as_ref(), &part).await;
                (inner, part, report, log, leaves)
            };

            let (_, _, scratch, _, scratch_leaves) = fault_fold(
                Rule::new(Op::Get, ScriptedFault::Permanent("leaf GET refused".into()))
                    .with_key_contains(".kidx")
                    .with_occurrence(Occurrence::Nth(1)),
            )
            .await;
            assert_eq!(scratch.key_index_entries_carried, 0, "budget {max_pairs}");
            assert_eq!(
                scratch_leaves, carried_leaves,
                "budget {max_pairs}: from scratch"
            );

            // Writer 35's object: the column-stats GET, then a footer suffix
            // GET and a section GET per pass, so the 3rd GET is its first
            // pass's section and the 5th its second pass's.
            let section_get = |nth: u64| {
                Rule::new(
                    Op::Get,
                    ScriptedFault::Permanent("section GET refused".into()),
                )
                .with_key_contains(writer_35.as_str())
                .with_occurrence(Occurrence::Nth(nth))
            };
            let (first_store, first_part, _, first_log, first_leaves) =
                fault_fold(section_get(3)).await;
            assert!(
                first_log.iter().all(|r| r.attempt == 0),
                "budget {max_pairs}: a first-pass failure needs no restart"
            );
            assert_eq!(
                first_leaves,
                reference_leaves(
                    first_store.as_ref(),
                    &first_part,
                    &[(USER_ID.to_string(), KeyType::I64)],
                    max_pairs,
                    &[writer_35_ordinal],
                )
                .await,
                "budget {max_pairs}: writer 35 failed in the first pass"
            );
            if greedy_passes(&all_pairs, max_pairs) < 2 {
                continue;
            }
            let (_, _, restarted, restart_log, restart_leaves) = fault_fold(section_get(5)).await;
            assert!(
                restart_log.iter().any(|r| r.attempt == 1),
                "budget {max_pairs}: the build restarted: {restarted:?}"
            );
            assert_pass_bound(&restart_log, 5, &format!("restart, budget {max_pairs}"));
            assert_eq!(
                restart_leaves, first_leaves,
                "budget {max_pairs}: after a restart"
            );
        }
    }

    /// The share is fixed for the build (ADR-3692 decision 5): with two
    /// fields every pass gives each `max(1, budget / 2)` pairs, whether or
    /// not the other field has finished, so the part takes as many passes as
    /// its slowest field under that share. Eight writers index `user_id`,
    /// the first four `session` too, two values each: `user_id` holds 16
    /// pairs and `session` 8, one entry per key8. At budget 4 each field's
    /// share is 2 and the build takes 8 passes, `user_id`'s count; a share
    /// that grew to 4 once `session` finished after 4 passes would take 6.
    /// Every budget matches the slowest field's greedy count, and the leaves
    /// equal the one-call build's at the same budget.
    #[tokio::test]
    async fn a_two_field_build_takes_as_many_passes_as_its_slowest_field() {
        use crate::tenant_config::DeclaredColumnType;
        for max_pairs in [1, 2, 3, 4, 5, 8, 16, u64::MAX] {
            let store = recording_store();
            declare_key_index_columns(
                store.as_ref(),
                &[
                    (USER_ID, DeclaredColumnType::I64),
                    (SESSION, DeclaredColumnType::I64),
                ],
                1,
            )
            .await;
            for seq in 1..=8u64 {
                let own = seq as i64;
                let fields: &[(&str, KeyType)] = if seq <= 4 {
                    &[(USER_ID, KeyType::I64), (SESSION, KeyType::I64)]
                } else {
                    &[(USER_ID, KeyType::I64)]
                };
                publish_keyed_logs(
                    store.as_ref(),
                    seq,
                    KIDX_HOUR,
                    &[own, own + 10],
                    2,
                    0,
                    fields,
                )
                .await;
            }
            let catalog = Catalog::new(
                store.clone(),
                CatalogConfig {
                    key_index_build_max_pairs: max_pairs,
                    ..kidx_config()
                },
            )
            .expect("catalog");
            take_key_index_pass_log_for_test();
            let report = fold_through(&catalog, Signal::Logs, KIDX_HOUR).await;
            let log = take_key_index_pass_log_for_test();
            let share = (max_pairs / 2).max(1);
            assert!(log.iter().all(|r| r.share == share), "{log:?}");
            let user_id_passes = greedy_passes(&[1; 16], share);
            let session_passes = greedy_passes(&[1; 8], share);
            assert_eq!(
                pass_ranges(&log, USER_ID, 1).len() as u64,
                user_id_passes,
                "budget {max_pairs}"
            );
            assert_eq!(
                pass_ranges(&log, SESSION, 1).len() as u64,
                session_passes,
                "budget {max_pairs}"
            );
            assert_eq!(
                report.key_index_build_passes,
                user_id_passes.max(session_passes),
                "budget {max_pairs}: {report:?}"
            );
            if max_pairs == 4 {
                assert_eq!(report.key_index_build_passes, 8, "{report:?}");
            }
            let part = read_signal_head(store.as_ref(), Signal::Logs)
                .await
                .parts
                .remove(0);
            assert_eq!(
                stored_leaves(store.as_ref(), &part).await,
                reference_leaves(
                    store.as_ref(),
                    &part,
                    &[
                        (SESSION.to_string(), KeyType::I64),
                        (USER_ID.to_string(), KeyType::I64)
                    ],
                    max_pairs,
                    &[],
                )
                .await,
                "budget {max_pairs}"
            );
        }
    }

    /// Declares `columns` as typed columns carrying `key_index`, written at
    /// `now_ns` so a later call replaces an earlier one.
    async fn declare_key_index_columns(
        store: &dyn ObjectStoreBackend,
        columns: &[(&str, crate::tenant_config::DeclaredColumnType)],
        now_ns: i64,
    ) {
        use crate::tenant_config::{StorageLayoutWrite, TenantConfig, TenantLifecycleState};
        let mut cfg = TenantConfig {
            typed_attr_columns: Some(
                columns
                    .iter()
                    .map(|(key, ty)| DeclaredTypedColumn {
                        key: key.to_string(),
                        ty: *ty,
                    })
                    .collect(),
            ),
            ..TenantConfig::new(TenantLifecycleState::Active)
        };
        cfg.set_key_index_columns(
            columns.iter().map(|(key, _)| key.to_string()).collect(),
            StorageLayoutWrite::ReadersRolledOut,
        )
        .expect("declare the key index");
        crate::tenant_config::set_tenant_config(store, &tenant(), &cfg, now_ns)
            .await
            .expect("write tenant config");
    }

    const SESSION: &str = "session";

    /// The first half of the carry-guard fixtures: `user_id` declared I64,
    /// writers 10, 20 and 30 folded and sealed into one part with one leaf,
    /// then the late writers 15 and 35 published with `late_fields` indexed,
    /// so the next fold's tail is 10, 15, 20, 30, 35 (see
    /// [`advancing_fixture`]).
    async fn carry_guard_fixture(
        inner: &Arc<RangeRecordingStore>,
        late_fields: &[(&str, KeyType)],
    ) {
        use crate::tenant_config::DeclaredColumnType;
        declare_key_index_columns(inner.as_ref(), &[(USER_ID, DeclaredColumnType::I64)], 1).await;
        for seq in [10u64, 20, 30] {
            let own = seq as i64;
            publish_user_id_logs(inner.as_ref(), seq, KIDX_HOUR, &[7, own], 2, 0, true).await;
        }
        let catalog = Catalog::new(inner.clone(), kidx_config()).expect("catalog");
        fold_logs_sealing(&catalog, now_inside(KIDX_HOUR), Some(KIDX_HOUR)).await;
        let first = read_signal_head(inner.as_ref(), Signal::Logs).await;
        assert_eq!(first.parts[0].key_index.len(), 1);
        for seq in [15u64, 35] {
            let own = seq as i64;
            publish_keyed_logs(inner.as_ref(), seq, KIDX_HOUR, &[7, own], 2, 0, late_fields).await;
        }
        inner.gets.lock().expect("lock").clear();
    }

    /// A key-index field declared between two folds has no previous leaf, so
    /// the advancing fold carries nothing: it GETs no previous leaf, reads
    /// all five entries' sections, and stores the from-scratch leaves of both
    /// fields byte for byte, the three old entries uncovered for `session`.
    /// Without the no-previous-leaf guard the fold GETs the previous leaf
    /// (still carrying nothing, since `carried_all` runs over the declared
    /// fields); without that guard and with `carried_all` over the carried
    /// fields, the old entries go unread and the `session` leaf claims them
    /// covered.
    #[tokio::test]
    async fn a_key_index_field_declared_between_folds_reads_every_entry() {
        use crate::tenant_config::DeclaredColumnType;
        let inner = recording_store();
        carry_guard_fixture(&inner, &[(USER_ID, KeyType::I64), (SESSION, KeyType::I64)]).await;
        declare_key_index_columns(
            inner.as_ref(),
            &[
                (USER_ID, DeclaredColumnType::I64),
                (SESSION, DeclaredColumnType::I64),
            ],
            2,
        )
        .await;
        let catalog = Catalog::new(inner.clone(), kidx_config()).expect("catalog");
        let report = advancing_fold(&catalog).await;
        assert_eq!(report.key_index_leaf_gets, 0, "{report:?}");
        assert_eq!(report.key_index_entries_carried, 0, "{report:?}");
        assert_eq!(report.key_index_sections_read, 5, "{report:?}");
        assert_eq!(data_object_ranged_gets(&inner).len(), 5 * 2);
        let second = read_signal_head(inner.as_ref(), Signal::Logs).await;
        let part = &second.parts[0];
        let session: Vec<_> = part
            .key_index
            .iter()
            .filter(|l| l.field == SESSION)
            .collect();
        assert_eq!(session.len(), 1, "{part:?}");
        assert_eq!(session[0].uncovered_count, 3, "{part:?}");
        assert_eq!(
            stored_leaves(inner.as_ref(), part).await,
            from_scratch_leaves_declaring(
                inner.as_ref(),
                part,
                &[
                    (SESSION.to_string(), KeyType::I64),
                    (USER_ID.to_string(), KeyType::I64)
                ]
            )
            .await
        );
    }

    /// A field retyped between two folds (`user_id` I64, then Str) does not
    /// carry an old-type pair: the previous leaf's key type is not the
    /// declared one, so the fold GETs no previous leaf, reads every section,
    /// and stores the from-scratch Str leaf, the three I64-indexed entries
    /// uncovered. Without the key-type guard the old I64 pairs are carried
    /// into the Str leaf.
    #[tokio::test]
    async fn a_key_index_field_retyped_between_folds_carries_no_old_type_pair() {
        use crate::tenant_config::DeclaredColumnType;
        let inner = recording_store();
        carry_guard_fixture(&inner, &[(USER_ID, KeyType::Str)]).await;
        declare_key_index_columns(inner.as_ref(), &[(USER_ID, DeclaredColumnType::Str)], 2).await;
        let catalog = Catalog::new(inner.clone(), kidx_config()).expect("catalog");
        let report = advancing_fold(&catalog).await;
        assert_eq!(report.key_index_leaf_gets, 0, "{report:?}");
        assert_eq!(report.key_index_entries_carried, 0, "{report:?}");
        assert_eq!(report.key_index_sections_read, 5, "{report:?}");
        let second = read_signal_head(inner.as_ref(), Signal::Logs).await;
        let part = &second.parts[0];
        assert_eq!(part.key_index.len(), 1, "{part:?}");
        assert_eq!(
            part.key_index[0].key_type,
            u32::from(KeyType::Str.as_u8()),
            "{part:?}"
        );
        assert_eq!(part.key_index[0].uncovered_count, 3, "{part:?}");
        assert_eq!(
            stored_leaves(inner.as_ref(), part).await,
            from_scratch_leaves_declaring(
                inner.as_ref(),
                part,
                &[(USER_ID.to_string(), KeyType::Str)]
            )
            .await
        );
    }

    /// #3948: the report carries the pair budget the build ran under and
    /// every indexed field's pairs. `user_id` and `absent_key` are declared;
    /// the sections name `user_id` and `session`, so `session` joins during
    /// the build and `absent_key` holds no pair. Entry 1's ids 10, 10, 20 and
    /// entry 2's 30, 40, 40, 50 are 2 + 3 distinct (key, entry) pairs per
    /// named field, held over several passes under a budget of 6 (2 a field).
    /// With the switch off the same fold reports neither.
    #[tokio::test]
    async fn the_fold_report_carries_the_pair_budget_and_every_fields_pairs() {
        use crate::tenant_config::DeclaredColumnType;
        const ABSENT: &str = "absent_key";
        let fold_once = |build_key_index_leaves: bool| async move {
            let store = Arc::new(MemoryStore::new());
            declare_key_index_columns(
                store.as_ref(),
                &[
                    (USER_ID, DeclaredColumnType::I64),
                    (ABSENT, DeclaredColumnType::I64),
                ],
                1,
            )
            .await;
            let fields = [(USER_ID, KeyType::I64), (SESSION, KeyType::I64)];
            publish_keyed_logs(store.as_ref(), 1, KIDX_HOUR, &[10, 10, 20], 2, 0, &fields).await;
            publish_keyed_logs(
                store.as_ref(),
                2,
                KIDX_HOUR,
                &[30, 40, 40, 50],
                2,
                0,
                &fields,
            )
            .await;
            let catalog = Catalog::new(
                store.clone(),
                CatalogConfig {
                    build_key_index_leaves,
                    key_index_build_max_pairs: 6,
                    ..config(1)
                },
            )
            .expect("catalog");
            fold_through(&catalog, Signal::Logs, KIDX_HOUR).await
        };

        let on = fold_once(true).await;
        assert_eq!(on.key_index_pair_budget, 6, "{on:?}");
        assert_eq!(
            on.key_index_field_pairs,
            BTreeMap::from([
                (ABSENT.to_string(), 0),
                (SESSION.to_string(), 5),
                (USER_ID.to_string(), 5),
            ]),
            "{on:?}"
        );
        assert!(on.key_index_build_passes > 1, "{on:?}");

        let off = fold_once(false).await;
        assert_eq!(off.key_index_pair_budget, 0, "{off:?}");
        assert!(off.key_index_field_pairs.is_empty(), "{off:?}");
    }

    /// Records the key of every PUT and the key and range of every GET, so
    /// a test can attribute a fold's leaf PUTs, previous-leaf GETs and
    /// section GETs to one part.
    struct OpLogStore {
        inner: MemoryStore,
        puts: std::sync::Mutex<Vec<String>>,
        gets: std::sync::Mutex<Vec<(String, GetRange)>>,
    }

    impl OpLogStore {
        fn empty() -> Arc<Self> {
            Arc::new(OpLogStore {
                inner: MemoryStore::new(),
                puts: std::sync::Mutex::new(Vec::new()),
                gets: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn clear(&self) {
            self.puts.lock().expect("lock").clear();
            self.gets.lock().expect("lock").clear();
        }

        /// `.kidx` PUTs of the leaves of the part whose watermark is `hour`:
        /// a leaf's key names its part's watermark hour.
        fn leaf_puts_for_part_watermark(&self, hour: u32) -> usize {
            let needle = format!("/idx/{}.", keys::ingest_hour_string(hour));
            self.puts
                .lock()
                .expect("lock")
                .iter()
                .filter(|key| key.ends_with(".kidx") && key.contains(&needle))
                .count()
        }

        /// Keys of every snapshot part PUT, in order.
        fn part_puts(&self) -> Vec<String> {
            self.puts
                .lock()
                .expect("lock")
                .iter()
                .filter(|key| key.ends_with(".csnap"))
                .cloned()
                .collect()
        }

        /// Full GETs of `key`.
        fn full_gets_of(&self, key: &str) -> usize {
            self.gets
                .lock()
                .expect("lock")
                .iter()
                .filter(|(got, range)| got == key && matches!(range, GetRange::Full))
                .count()
        }

        /// `.kidx` GETs of the leaves of the part whose watermark is `hour`.
        fn leaf_gets_for_part_watermark(&self, hour: u32) -> usize {
            let needle = format!("/idx/{}.", keys::ingest_hour_string(hour));
            self.gets
                .lock()
                .expect("lock")
                .iter()
                .filter(|(key, _)| key.ends_with(".kidx") && key.contains(&needle))
                .count()
        }

        /// GETs of any of `leaves`' objects.
        fn gets_of_leaves(&self, leaves: &[SnapshotKeyIndexLeafRef]) -> usize {
            self.gets
                .lock()
                .expect("lock")
                .iter()
                .filter(|(key, _)| leaves.iter().any(|leaf| leaf.key == *key))
                .count()
        }

        /// Ranged GETs of the data objects of `records`: the key-index
        /// build's footer and section reads.
        fn section_gets_of(&self, records: &[&CommitRecord]) -> usize {
            let data_keys: Vec<String> = records
                .iter()
                .map(|record| keys::reconstruct_data_key(record).expect("data key"))
                .collect();
            self.gets
                .lock()
                .expect("lock")
                .iter()
                .filter(|(key, range)| !matches!(range, GetRange::Full) && data_keys.contains(key))
                .count()
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for OpLogStore {
        async fn put(
            &self,
            key: &str,
            data: Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, StoreError> {
            self.puts.lock().expect("lock").push(key.to_string());
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: GetRange,
        ) -> Result<ravel_object_store::GetOutcome, StoreError> {
            self.gets
                .lock()
                .expect("lock")
                .push((key.to_string(), range));
            self.inner.get(key, range).await
        }

        async fn head(&self, key: &str) -> Result<ravel_object_store::ObjectMeta, StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// The first hour of the UTC day `KIDX_HOUR` falls on, day d below.
    const DAY_D: u32 = KIDX_HOUR / 24 * 24;

    /// Publishes a logs retention tombstone for `(0, ingest_hour_bucket)`,
    /// the logs form of [`publish_tombstone`].
    async fn publish_logs_tombstone(store: &dyn ObjectStoreBackend, ingest_hour_bucket: u32) {
        let record = RetentionTombstone {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: signal::to_proto(Signal::Logs).into(),
            shard: 0,
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

    /// ADR-3939 acceptance A3 through `Catalog::fold`: with the leaf switch
    /// on, day d seals into its own part once day d + 1 holds an entry, and
    /// every later advance carries that part by reference, its leaf refs
    /// equal to the previous HEAD's, with no leaf PUT, no GET of its leaves
    /// and no read of its entries' sections.
    ///
    /// The first fold holds day d alone, so its one part is encoded with
    /// `min_hour` 0 (the single-part override). The sealing fold encodes day
    /// d with its real first hour, so its hash changes and its leaves are
    /// built once more, from the previous tail's leaf: one leaf GET, one
    /// leaf PUT, no section read. With the day seal off at the caller, the
    /// sealing fold keeps one part over both days and the first assertion on
    /// its HEAD fails.
    #[tokio::test]
    async fn a_sealed_utc_day_keeps_its_leaves_across_advancing_folds() {
        let store = OpLogStore::empty();
        declare_user_id_key_index(store.as_ref()).await;
        let mut day_d = Vec::new();
        for (seq, hour, ids) in [(1u64, DAY_D + 20, [10, 20]), (2, DAY_D + 22, [30, 40])] {
            let (record, _) =
                publish_user_id_logs(store.as_ref(), seq, hour, &ids, 2, 0, true).await;
            day_d.push(record);
        }
        let day_d: Vec<&CommitRecord> = day_d.iter().collect();
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        fold_through(&catalog, Signal::Logs, DAY_D + 22).await;
        let first = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(first.parts.len(), 1);
        assert_eq!(first.parts[0].min_hour, 0, "a lone part keeps min_hour 0");

        // The sealing fold: day d + 1's first entry.
        publish_user_id_logs(store.as_ref(), 3, DAY_D + 25, &[50], 2, 0, true).await;
        store.clear();
        let sealing = fold_through(&catalog, Signal::Logs, DAY_D + 25).await;
        let sealed = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            sealed
                .parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour, p.entry_count))
                .collect::<Vec<_>>(),
            vec![(DAY_D + 20, DAY_D + 22, 2), (DAY_D + 25, DAY_D + 25, 1)],
            "day d seals into its own part"
        );
        assert_ne!(sealed.parts[0].blake3, first.parts[0].blake3);
        assert_eq!(sealed.parts[0].key_index.len(), 1);
        assert_eq!(sealing.parts_reused, 0, "{sealing:?}");
        assert_eq!(
            (
                store.leaf_puts_for_part_watermark(DAY_D + 22),
                store.gets_of_leaves(&first.parts[0].key_index),
                store.section_gets_of(&day_d),
            ),
            (1, 1, 0),
            "the sealing fold rebuilds day d's leaf once, from the previous tail's leaf"
        );
        assert_eq!(sealing.key_index_entries_carried, 2, "{sealing:?}");

        let mut previous = sealed;
        for (seq, hour) in [(4u64, DAY_D + 26), (5, DAY_D + 27)] {
            publish_user_id_logs(store.as_ref(), seq, hour, &[seq as i64 * 10], 2, 0, true).await;
            store.clear();
            let report = fold_through(&catalog, Signal::Logs, hour).await;
            let head = read_signal_head(store.as_ref(), Signal::Logs).await;
            assert_eq!(head.parts.len(), 2, "{hour}");
            assert_eq!(
                head.parts[0], previous.parts[0],
                "{hour}: day d's ref, key_index included, is the previous HEAD's"
            );
            assert_eq!(report.parts_reused, 1, "{hour}: {report:?}");
            assert_eq!(
                (
                    store.leaf_puts_for_part_watermark(DAY_D + 22),
                    store.gets_of_leaves(&previous.parts[0].key_index),
                    store.section_gets_of(&day_d),
                ),
                (0, 0, 0),
                "{hour}: no leaf PUT, leaf GET or section read for sealed day d"
            );
            previous = head;
        }
    }

    /// ADR-3939 acceptance A4: two folds of the same entry set into two
    /// fresh stores, under the same clock, folder id and config, write
    /// byte-identical HEADs with the leaf switch on, three day parts each.
    /// With the switch off the same entries fold into the one part the entry
    /// threshold alone gives.
    #[tokio::test]
    async fn day_sealed_folds_of_one_entry_set_write_identical_heads() {
        let folder = Uuid::from_u128(7);
        let fold_into = |build_key_index_leaves: bool| async move {
            let store = Arc::new(MemoryStore::new());
            declare_user_id_key_index(store.as_ref()).await;
            for (seq, hour) in [
                (1u64, DAY_D + 3),
                (2, DAY_D + 23),
                (3, DAY_D + 24),
                (4, DAY_D + 50),
            ] {
                publish_user_id_logs(store.as_ref(), seq, hour, &[seq as i64], 2, 0, true).await;
            }
            let catalog = Catalog::new(
                store.clone(),
                CatalogConfig {
                    build_key_index_leaves,
                    ..config(1)
                },
            )
            .expect("catalog");
            catalog
                .fold(
                    &tenant(),
                    Signal::Logs,
                    folder,
                    now_at_seal(DAY_D + 50),
                    &[],
                    None,
                )
                .await
                .expect("fold");
            store
                .get(&head_object_key(&tenant(), Signal::Logs), GetRange::Full)
                .await
                .expect("head present")
                .data
        };
        let one = fold_into(true).await;
        let two = fold_into(true).await;
        assert_eq!(one, two, "byte-identical HEADs");
        let head = snapshot_format::decode_head(&one).expect("head decodes");
        assert_eq!(
            head.parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour))
                .collect::<Vec<_>>(),
            vec![
                (DAY_D + 3, DAY_D + 23),
                (DAY_D + 24, DAY_D + 24),
                (DAY_D + 50, DAY_D + 50)
            ]
        );
        assert!(head.parts.iter().all(|p| p.key_index.len() == 1));

        let off = snapshot_format::decode_head(&fold_into(false).await).expect("head decodes");
        assert_eq!(
            off.parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour, p.entry_count))
                .collect::<Vec<_>>(),
            vec![(0, DAY_D + 50, 4)],
            "the switch-off partition is the entry threshold's alone"
        );
    }

    /// ADR-3939 decision 4: a switch-off fold over a HEAD a switch-on fold
    /// wrote reports `key_index_switch_off_over_leaves` and warns once,
    /// naming the tenant and signal, and still folds. A switch-off fold over
    /// a switch-off HEAD, and every switch-on fold, reports `false`.
    #[tokio::test]
    async fn key_index_switch_off_over_leaves_is_reported_and_warned() {
        let fold_pair = |first_on: bool, second_on: bool| async move {
            let store = Arc::new(MemoryStore::new());
            declare_user_id_key_index(store.as_ref()).await;
            publish_user_id_logs(store.as_ref(), 1, KIDX_HOUR, &[10], 2, 0, true).await;
            let catalog_with = |on: bool| {
                Catalog::new(
                    store.clone(),
                    CatalogConfig {
                        build_key_index_leaves: on,
                        ..config(1)
                    },
                )
                .expect("catalog")
            };
            let first = fold_through(&catalog_with(first_on), Signal::Logs, KIDX_HOUR).await;
            publish_user_id_logs(store.as_ref(), 2, KIDX_HOUR + 1, &[20], 2, 0, true).await;
            let capture = LevelCapture::default();
            let subscriber = tracing_subscriber::registry().with(capture.clone());
            let second = {
                let _default = tracing::subscriber::set_default(subscriber);
                fold_through(&catalog_with(second_on), Signal::Logs, KIDX_HOUR + 1).await
            };
            let warned = capture.count_at(tracing::Level::WARN, "leaf switch is off");
            (first, second, warned, capture.lines())
        };

        let (first, second, warned, lines) = fold_pair(true, false).await;
        assert!(!first.key_index_switch_off_over_leaves, "{first:?}");
        assert!(second.key_index_switch_off_over_leaves, "{second:?}");
        assert!(!second.no_op, "the fold still runs: {second:?}");
        assert_eq!(second.entry_count, 2, "{second:?}");
        assert_eq!(warned, 1, "{lines:?}");
        assert!(
            lines
                .iter()
                .any(|(_, line)| line.contains("leaf switch is off")
                    && line.contains(&format!("tenant={}", tenant().to_hex()))
                    && line.contains("signal=Logs")),
            "{lines:?}"
        );

        for (first_on, second_on) in [(false, false), (false, true), (true, true)] {
            let (first, second, warned, lines) = fold_pair(first_on, second_on).await;
            assert!(
                !first.key_index_switch_off_over_leaves && !second.key_index_switch_off_over_leaves,
                "{first_on} then {second_on}: {first:?} {second:?}"
            );
            assert_eq!(warned, 0, "{first_on} then {second_on}: {lines:?}");
        }
    }

    /// Publishes day d's surviving writers 2 and 3 (hour 21) and day d + 1's
    /// writers 4 (hour 25) and 5 (hour 26) into `store`, the entry set
    /// [`a_dirty_sealed_day_carries_its_own_previous_leaves`] folds after
    /// its tombstone, and returns writers 2 and 3's records.
    async fn publish_dirty_day_survivors(
        store: &dyn ObjectStoreBackend,
    ) -> (CommitRecord, CommitRecord) {
        let (two, _) = publish_user_id_logs(store, 2, DAY_D + 21, &[10, 20], 2, 0, true).await;
        let (three, _) = publish_user_id_logs(store, 3, DAY_D + 21, &[30, 40], 2, 0, true).await;
        publish_user_id_logs(store, 4, DAY_D + 25, &[50], 2, 0, true).await;
        publish_user_id_logs(store, 5, DAY_D + 26, &[60], 2, 0, true).await;
        (two, three)
    }

    /// ADR-3939 decision 5 (task C1), acceptance W2 in miniature: a retention
    /// tombstone in one hour of a sealed day re-encodes that day's part, and
    /// the build carries the surviving entries from the day's own previous
    /// leaves, a previous part that is not the HEAD's last. Day d holds
    /// writers 1 (hour 20, ids 1 and 2) and 2 and 3 (hour 21, ids 10, 20 and
    /// 30, 40): 6 pairs, which the first fold cuts into 3 passes of 2 under a
    /// budget of 2, one leaf each. Hour 20 is tombstoned, so 2 entries
    /// survive. The re-encode GETs each of the 3 previous leaves once, reads
    /// no section of the surviving entries, and writes the 2 leaves a fold of
    /// the surviving entries into a fresh store writes, byte for byte. With
    /// the carry source the previous HEAD's last part, day d's previous
    /// leaves are never read and the survivors' sections are, 2 ranged GETs
    /// per entry per pass.
    #[tokio::test]
    async fn a_dirty_sealed_day_carries_its_own_previous_leaves() {
        let budget = CatalogConfig {
            key_index_build_max_pairs: 2,
            ..kidx_config()
        };
        let store = OpLogStore::empty();
        declare_user_id_key_index(store.as_ref()).await;
        let (one, _) =
            publish_user_id_logs(store.as_ref(), 1, DAY_D + 20, &[1, 2], 2, 0, true).await;
        let (two, _) =
            publish_user_id_logs(store.as_ref(), 2, DAY_D + 21, &[10, 20], 2, 0, true).await;
        let (three, _) =
            publish_user_id_logs(store.as_ref(), 3, DAY_D + 21, &[30, 40], 2, 0, true).await;
        publish_user_id_logs(store.as_ref(), 4, DAY_D + 25, &[50], 2, 0, true).await;
        let catalog = Catalog::new(store.clone(), budget).expect("catalog");
        fold_through(&catalog, Signal::Logs, DAY_D + 25).await;
        let first = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(first.parts.len(), 2);
        assert_eq!(first.parts[0].entry_count, 3);
        assert_eq!(
            first.parts[0].key_index.len(),
            3,
            "one leaf per pass: 6 pairs under a share of 2"
        );

        publish_logs_erasure(store.as_ref(), DAY_D + 20, &[&one]).await;
        publish_user_id_logs(store.as_ref(), 5, DAY_D + 26, &[60], 2, 0, true).await;
        store.clear();
        take_key_index_carry_log_for_test();
        let report = fold_through(&catalog, Signal::Logs, DAY_D + 26).await;
        let carry_log = take_key_index_carry_log_for_test();
        let second = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(report.parts_reused, 0, "{report:?}");
        assert_eq!(
            (
                second.parts[0].min_hour,
                second.parts[0].watermark_hour,
                second.parts[0].entry_count
            ),
            (DAY_D + 21, DAY_D + 21, 2),
            "day d is re-encoded without the tombstoned hour"
        );
        assert_eq!(
            second.parts[0].key_index.len(),
            2,
            "one leaf per pass: 4 pairs under a share of 2"
        );
        assert_eq!(
            store.section_gets_of(&[&two, &three]),
            0,
            "no section read for the surviving entries"
        );
        // Writer 1's pairs are gone, so the first pass's 2 pairs reach past
        // the first previous slice into the third: it holds all 3.
        assert_eq!(
            carry_log
                .iter()
                .flat_map(|record| record.slices_held.values().copied())
                .max(),
            Some(3),
            "{carry_log:?}"
        );
        assert_eq!(
            store.gets_of_leaves(&first.parts[0].key_index),
            first.parts[0].key_index.len(),
            "each of day d's previous leaves is fetched once"
        );
        for leaf in &first.parts[0].key_index {
            assert_eq!(store.gets_of_leaves(std::slice::from_ref(leaf)), 1);
        }

        let fresh = OpLogStore::empty();
        declare_user_id_key_index(fresh.as_ref()).await;
        publish_dirty_day_survivors(fresh.as_ref()).await;
        let scratch_catalog = Catalog::new(fresh.clone(), budget).expect("catalog");
        fold_through(&scratch_catalog, Signal::Logs, DAY_D + 26).await;
        let scratch = read_signal_head(fresh.as_ref(), Signal::Logs).await;
        assert_eq!(
            scratch.parts[0].blake3, second.parts[0].blake3,
            "the same day d part"
        );
        assert_eq!(
            stored_leaves(store.as_ref(), &second.parts[0]).await,
            stored_leaves(fresh.as_ref(), &scratch.parts[0]).await,
            "the carried leaves are the from-scratch leaves, byte for byte"
        );
    }

    /// ADR-3939 decision 5: a later slice whose GET fails restarts the build
    /// with no carry, as a slice that no longer decodes does. In the
    /// dirty-day fixture of [`a_dirty_sealed_day_carries_its_own_previous_leaves`]
    /// the first pass decodes day d's lowest previous slice, fetched at load,
    /// and reaches the second slice, whose GET is refused: 2 previous-leaf
    /// GETs issued before the restart, 1 of them served. The restart warns
    /// once, reads both surviving entries' sections in each of its 2 passes
    /// (2 ranged GETs each, 8), and writes the from-scratch leaves. The new
    /// tail's own carry adds 1 leaf GET, so the report counts 3.
    #[tokio::test]
    async fn a_later_slice_whose_get_fails_restarts_the_build_without_the_carry() {
        let budget = CatalogConfig {
            key_index_build_max_pairs: 2,
            ..kidx_config()
        };
        let inner = OpLogStore::empty();
        declare_user_id_key_index(inner.as_ref()).await;
        let (one, _) =
            publish_user_id_logs(inner.as_ref(), 1, DAY_D + 20, &[1, 2], 2, 0, true).await;
        let (two, _) =
            publish_user_id_logs(inner.as_ref(), 2, DAY_D + 21, &[10, 20], 2, 0, true).await;
        let (three, _) =
            publish_user_id_logs(inner.as_ref(), 3, DAY_D + 21, &[30, 40], 2, 0, true).await;
        publish_user_id_logs(inner.as_ref(), 4, DAY_D + 25, &[50], 2, 0, true).await;
        let catalog = Catalog::new(inner.clone(), budget).expect("catalog");
        fold_through(&catalog, Signal::Logs, DAY_D + 25).await;
        let first = read_signal_head(inner.as_ref(), Signal::Logs).await;
        let mut day_d = first.parts[0].key_index.clone();
        day_d.sort_by_key(|leaf| leaf.key8_lo);
        assert_eq!(day_d.len(), 3);

        publish_logs_erasure(inner.as_ref(), DAY_D + 20, &[&one]).await;
        publish_user_id_logs(inner.as_ref(), 5, DAY_D + 26, &[60], 2, 0, true).await;
        inner.clear();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("slice GET refused".into()),
            )
            .with_key_contains(day_d[1].key.as_str())
            .with_occurrence(Occurrence::Nth(1)),
        );
        let store = Arc::new(FaultStore::new(Arc::clone(&inner), plan));
        let faulted = Catalog::new(store.clone(), budget).expect("catalog");
        let (report, warnings) = fold_capturing_warnings(&faulted, Signal::Logs, DAY_D + 26).await;
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Permanent),
            1,
            "the second slice's GET was refused"
        );
        assert_eq!(
            warnings
                .iter()
                .filter(|line| line.contains("no longer decodes"))
                .count(),
            1,
            "{warnings:?}"
        );
        assert_eq!(
            (
                inner.gets_of_leaves(&day_d[..1]),
                inner.gets_of_leaves(&day_d[1..]),
            ),
            (1, 0),
            "the lowest slice served once; the refused GET never reached the store"
        );
        assert_eq!(report.key_index_leaf_gets, 3, "{report:?}");
        assert_eq!(
            inner.section_gets_of(&[&two, &three]),
            2 * 2 * 2,
            "2 ranged GETs x 2 surviving entries x 2 passes"
        );
        let second = read_signal_head(inner.as_ref(), Signal::Logs).await;
        assert_eq!(second.parts[0].entry_count, 2);

        let fresh = OpLogStore::empty();
        declare_user_id_key_index(fresh.as_ref()).await;
        publish_dirty_day_survivors(fresh.as_ref()).await;
        let scratch_catalog = Catalog::new(
            fresh.clone(),
            CatalogConfig {
                key_index_build_max_pairs: 2,
                ..kidx_config()
            },
        )
        .expect("catalog");
        fold_through(&scratch_catalog, Signal::Logs, DAY_D + 26).await;
        let scratch = read_signal_head(fresh.as_ref(), Signal::Logs).await;
        assert_eq!(scratch.parts[0].blake3, second.parts[0].blake3);
        assert_eq!(
            stored_leaves(inner.as_ref(), &second.parts[0]).await,
            stored_leaves(fresh.as_ref(), &scratch.parts[0]).await,
            "the restarted build writes the from-scratch leaves"
        );
    }

    /// Publishes a logs selective-erasure rewrite record into `(0, hour)`
    /// that supersedes `inputs` and contributes no part: every row of the
    /// inputs was erased.
    async fn publish_logs_erasure(
        store: &dyn ObjectStoreBackend,
        hour: u32,
        inputs: &[&CommitRecord],
    ) {
        let mut input_ids: Vec<CompactionInputIdentity> = inputs
            .iter()
            .map(|r| CompactionInputIdentity {
                writer_id: r.writer_id.clone(),
                writer_epoch: r.writer_epoch,
                writer_seq: r.writer_seq,
            })
            .collect();
        input_ids.sort_by(|a, b| {
            (a.writer_id.as_str(), a.writer_epoch, a.writer_seq).cmp(&(
                b.writer_id.as_str(),
                b.writer_epoch,
                b.writer_seq,
            ))
        });
        let request_ids = vec![Uuid::from_u128(u128::from(hour)).to_string()];
        let input_set_hash =
            erasure::compute_rewrite_input_set_hash(&input_ids, None, &request_ids).to_vec();
        let record = RewriteRecord {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: signal::to_proto(Signal::Logs) as i32,
            shard: 0,
            ingest_hour_bucket: hour,
            inputs: input_ids,
            input_set_hash,
            parts: Vec::new(),
            drops: request_ids
                .iter()
                .map(|request_id| RewriteDrop {
                    request_id: request_id.clone(),
                    dropped_count: 1,
                })
                .collect(),
            created_unix_ns: i64::from(hour + 1) * NS_PER_HOUR,
            superseded_record_key: String::new(),
        };
        let key = keys::rewrite_record_key_for(&record).expect("rewrite key");
        store
            .put(
                &key,
                erasure::encode_rewrite(&record),
                PutOptions::default(),
            )
            .await
            .expect("put rewrite record");
    }

    /// Day d holds writer 1 (hour 20, ids 1 and 2) and writers 2 and 3
    /// (hour 21, ids 10, 20 and 30, 40); day d + 1 holds writer 4 (hour 25,
    /// id 50). Returns the four records in writer order.
    async fn publish_retire_fixture(store: &dyn ObjectStoreBackend) -> Vec<CommitRecord> {
        let mut records = Vec::new();
        for (seq, hour, ids) in [
            (1u64, DAY_D + 20, &[1i64, 2][..]),
            (2, DAY_D + 21, &[10, 20]),
            (3, DAY_D + 21, &[30, 40]),
            (4, DAY_D + 25, &[50]),
        ] {
            let (record, _) = publish_user_id_logs(store, seq, hour, ids, 2, 0, true).await;
            records.push(record);
        }
        records
    }

    /// Every entry of every part `head` names, read off the part objects.
    async fn head_entries(
        store: &dyn ObjectStoreBackend,
        head: &SnapshotHead,
    ) -> Vec<SnapshotEntry> {
        let mut entries = Vec::new();
        for part in &head.parts {
            let got = store
                .get(&part.key, GetRange::Full)
                .await
                .expect("part present");
            entries.extend(
                snapshot_format::decode_part(&got.data, &PartLimits::default())
                    .expect("part decodes")
                    .entries,
            );
        }
        entries
    }

    /// Folds the retire fixture through hour 25 with the leaf switch on,
    /// tombstones hour 20, and folds once more through hour 26, where
    /// writer 5 lands. Returns the catalog and the HEADs before and after.
    async fn retire_hour_twenty(
        store: &Arc<OpLogStore>,
        config: CatalogConfig,
    ) -> (
        Catalog,
        SnapshotHead,
        SnapshotHead,
        FoldReport,
        Vec<CommitRecord>,
    ) {
        declare_user_id_key_index(store.as_ref()).await;
        let records = publish_retire_fixture(store.as_ref()).await;
        let catalog = Catalog::new(store.clone(), config).expect("catalog");
        fold_through(&catalog, Signal::Logs, DAY_D + 25).await;
        let first = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(first.parts.len(), 2);
        assert!(!first.parts[0].key_index.is_empty(), "day d has leaves");
        assert_eq!(first.format_version, HEAD_FORMAT_VERSION_1);

        publish_logs_tombstone(store.as_ref(), DAY_D + 20).await;
        publish_user_id_logs(store.as_ref(), 5, DAY_D + 26, &[60], 2, 0, true).await;
        store.clear();
        let report = fold_through(&catalog, Signal::Logs, DAY_D + 26).await;
        let second = read_signal_head(store.as_ref(), Signal::Logs).await;
        (catalog, first, second, report, records)
    }

    /// ADR-3939 decision 6: a retention tombstone for hour 20 of sealed,
    /// leaf-bearing day d is recorded in day d's ref as a retired hour. The
    /// fold PUTs no part and no leaf for day d (its one part PUT is the
    /// tail's), carries day d's ref unchanged but for `retired_hours`, and
    /// writes a version 2 HEAD.
    #[tokio::test]
    async fn a_tombstoned_hour_in_a_leaf_bearing_sealed_day_is_recorded_not_reencoded() {
        let store = OpLogStore::empty();
        let (_, first, second, report, _) = retire_hour_twenty(&store, kidx_config()).await;
        let mut expected = first.parts[0].clone();
        expected.retired_hours = vec![DAY_D + 20];
        assert_eq!(
            second.parts[0], expected,
            "day d's ref is the previous one but for its retired hour"
        );
        assert_eq!(report.parts_reused, 1, "{report:?}");
        assert_eq!(
            store.part_puts(),
            vec![second.parts[1].key.clone()],
            "the one part PUT is the tail's"
        );
        assert_eq!(store.leaf_puts_for_part_watermark(DAY_D + 21), 0);
        assert_eq!(second.format_version, HEAD_FORMAT_VERSION_2);
        assert_eq!(head_format_version_for(&second), HEAD_FORMAT_VERSION_2);
    }

    /// ADR-3939 decision 6: once every hour of day d holding entries is
    /// tombstoned, day d's part leaves HEAD with no part PUT, and HEAD goes
    /// back to version 1 since no other part has retired hours.
    #[tokio::test]
    async fn a_fully_tombstoned_day_part_is_dropped_without_a_put() {
        let store = OpLogStore::empty();
        let (catalog, _, second, _, _) = retire_hour_twenty(&store, kidx_config()).await;
        assert_eq!(second.format_version, HEAD_FORMAT_VERSION_2);

        publish_logs_tombstone(store.as_ref(), DAY_D + 21).await;
        publish_user_id_logs(store.as_ref(), 6, DAY_D + 27, &[70], 2, 0, true).await;
        store.clear();
        fold_through(&catalog, Signal::Logs, DAY_D + 27).await;
        let third = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            third
                .parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour, p.entry_count))
                .collect::<Vec<_>>(),
            vec![(0, DAY_D + 27, 3)],
            "day d's part left HEAD, and the one part left starts at 0"
        );
        assert_eq!(
            store.part_puts(),
            vec![third.parts[0].key.clone()],
            "no part PUT for day d"
        );
        assert_eq!(third.format_version, HEAD_FORMAT_VERSION_1);
    }

    /// ADR-3939 decision 6: an erasure rewrite record in hour 21 of a day
    /// with retired hour 20 re-encodes day d in the same fold. The new part
    /// holds writer 2 alone (writer 3 erased, hour 20's entries dropped for
    /// good), its ref carries no retired hours, and HEAD is version 1.
    #[tokio::test]
    async fn a_rewrite_record_in_a_retired_sealed_day_re_encodes_now() {
        let store = OpLogStore::empty();
        let (catalog, _, second, _, records) = retire_hour_twenty(&store, kidx_config()).await;
        publish_logs_erasure(store.as_ref(), DAY_D + 21, &[&records[2]]).await;
        publish_user_id_logs(store.as_ref(), 6, DAY_D + 27, &[70], 2, 0, true).await;
        store.clear();
        fold_through(&catalog, Signal::Logs, DAY_D + 27).await;
        let third = read_signal_head(store.as_ref(), Signal::Logs).await;
        let day_d = &third.parts[0];
        assert_eq!(
            (day_d.min_hour, day_d.watermark_hour, day_d.entry_count),
            (DAY_D + 21, DAY_D + 21, 1),
            "day d is re-encoded without hour 20 and without writer 3"
        );
        assert!(day_d.retired_hours.is_empty(), "{day_d:?}");
        assert_ne!(day_d.key, second.parts[0].key);
        assert_eq!(store.part_puts().first(), Some(&day_d.key));
        assert_eq!(third.format_version, HEAD_FORMAT_VERSION_1);
        let writers: Vec<u64> = head_entries(store.as_ref(), &third)
            .await
            .iter()
            .filter(|e| e.ingest_hour_bucket < DAY_D + 24)
            .map(|e| e.writer_seq)
            .collect();
        assert_eq!(writers, vec![2]);
    }

    /// ADR-3939 decision 6: a part carried by reference keeps its retired
    /// hours. The fourth fold no longer lists hour 20 (its reconcile window
    /// starts at hour 34), yet day d's ref still carries it.
    #[tokio::test]
    async fn a_part_carried_by_reference_keeps_its_retired_hours() {
        let store = OpLogStore::empty();
        let (catalog, _, second, _, _) = retire_hour_twenty(&store, kidx_config()).await;
        for (seq, hour) in [(6u64, DAY_D + 60), (7, DAY_D + 61)] {
            publish_user_id_logs(store.as_ref(), seq, hour, &[seq as i64], 2, 0, true).await;
            store.clear();
            let report = fold_through(&catalog, Signal::Logs, hour).await;
            let head = read_signal_head(store.as_ref(), Signal::Logs).await;
            assert_eq!(head.parts[0], second.parts[0], "{hour}");
            assert_eq!(head.parts[0].retired_hours, vec![DAY_D + 20], "{hour}");
            assert!(report.parts_reused >= 1, "{hour}: {report:?}");
            assert!(
                !store.part_puts().contains(&second.parts[0].key),
                "{hour}: no PUT of day d"
            );
            assert_eq!(head.format_version, HEAD_FORMAT_VERSION_2, "{hour}");
        }
    }

    /// ADR-3939 decision 6: a switch-off fold over a HEAD with retired hours
    /// drops their entries and re-encodes the part, so the field is never
    /// written with the switch off, even where the entry threshold alone
    /// would reproduce day d's part. Under a threshold of 2, day d is the
    /// first part either way; the switch-off fold runs with a 2-hour
    /// reconcile window, so it never lists hour 20's tombstone and only the
    /// retired-hours field tells it the hour is gone.
    #[tokio::test]
    async fn a_switch_off_fold_over_retired_hours_re_encodes_the_part() {
        let store = OpLogStore::empty();
        declare_user_id_key_index(store.as_ref()).await;
        for (seq, hour) in [(1u64, DAY_D + 20), (2, DAY_D + 21), (3, DAY_D + 25)] {
            publish_user_id_logs(store.as_ref(), seq, hour, &[seq as i64], 2, 0, true).await;
        }
        let on = Catalog::new(
            store.clone(),
            CatalogConfig {
                snapshot_part_max_entries: 2,
                ..kidx_config()
            },
        )
        .expect("catalog");
        fold_through(&on, Signal::Logs, DAY_D + 25).await;
        publish_logs_tombstone(store.as_ref(), DAY_D + 20).await;
        publish_user_id_logs(store.as_ref(), 4, DAY_D + 26, &[4], 2, 0, true).await;
        fold_through(&on, Signal::Logs, DAY_D + 26).await;
        let retired = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            (
                retired.parts[0].min_hour,
                retired.parts[0].watermark_hour,
                retired.parts[0].retired_hours.clone()
            ),
            (DAY_D + 20, DAY_D + 21, vec![DAY_D + 20])
        );

        let off = Catalog::new(
            store.clone(),
            CatalogConfig {
                snapshot_part_max_entries: 2,
                fold_reconcile_window_hours: 2,
                ..config(1)
            },
        )
        .expect("catalog");
        publish_user_id_logs(store.as_ref(), 5, DAY_D + 28, &[5], 2, 0, true).await;
        fold_through(&off, Signal::Logs, DAY_D + 28).await;
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert!(
            head.parts.iter().all(|p| p.retired_hours.is_empty()),
            "{head:?}"
        );
        assert_eq!(head.format_version, HEAD_FORMAT_VERSION_1);
        assert!(!head.parts.contains(&retired.parts[0]));
        let mut writers: Vec<u64> = head_entries(store.as_ref(), &head)
            .await
            .iter()
            .map(|e| e.writer_seq)
            .collect();
        writers.sort_unstable();
        assert_eq!(writers, vec![2, 3, 4, 5], "hour 20's writer 1 is gone");
    }

    /// A monotonic clock a test steps past the HEAD cache's TTL, so a
    /// reader's next resolve reads the current HEAD.
    #[derive(Default)]
    struct StepMono(std::sync::atomic::AtomicU64);

    impl StepMono {
        fn advance_past(&self, ttl_ns: i64) {
            let step = u64::try_from(ttl_ns).expect("a positive TTL") + 1;
            self.0.fetch_add(step, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl ravel_cpu_gate::MonotonicClock for StepMono {
        fn now_nanos(&self) -> u64 {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// ADR-3939 decision 6: the resolve filter reads the retired hours from
    /// the HEAD it pinned, not from the decoded part. A reader that decoded
    /// day d's part before the tombstone serves the part from its cache
    /// after it (no GET of the part), and still resolves no segment of
    /// hour 20.
    #[tokio::test]
    async fn a_decoded_part_cached_before_the_tombstone_hides_the_hour_after_it() {
        let store = OpLogStore::empty();
        declare_user_id_key_index(store.as_ref()).await;
        publish_retire_fixture(store.as_ref()).await;
        let writer = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        fold_through(&writer, Signal::Logs, DAY_D + 25).await;
        let first = read_signal_head(store.as_ref(), Signal::Logs).await;
        let mono = Arc::new(StepMono::default());
        let reader = Catalog::new(store.clone(), kidx_config())
            .expect("catalog")
            .with_monotonic_clock(mono.clone());
        let window = ravel_types::TimeRange {
            start_ns: i64::from(DAY_D) * NS_PER_HOUR,
            end_ns: i64::from(DAY_D + 48) * NS_PER_HOUR,
        };
        let hours_of = |snapshot: &crate::Snapshot| {
            let mut hours: Vec<(u32, u64)> = snapshot
                .segments
                .iter()
                .map(|s| (s.ingest_hour_bucket, s.writer_seq))
                .collect();
            hours.sort_unstable();
            hours
        };
        let before = reader
            .resolve(
                &tenant(),
                Signal::Logs,
                window,
                &[],
                now_at_seal(DAY_D + 25),
            )
            .await
            .expect("resolve");
        assert_eq!(
            hours_of(&before),
            vec![
                (DAY_D + 20, 1),
                (DAY_D + 21, 2),
                (DAY_D + 21, 3),
                (DAY_D + 25, 4)
            ]
        );
        assert_eq!(store.full_gets_of(&first.parts[0].key), 1);

        publish_logs_tombstone(store.as_ref(), DAY_D + 20).await;
        publish_user_id_logs(store.as_ref(), 5, DAY_D + 26, &[60], 2, 0, true).await;
        fold_through(&writer, Signal::Logs, DAY_D + 26).await;
        let second = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(second.parts[0].key, first.parts[0].key);
        assert_eq!(second.parts[0].retired_hours, vec![DAY_D + 20]);

        store.clear();
        mono.advance_past(reader.config().head_cache_ttl_ns);
        let after = reader
            .resolve(
                &tenant(),
                Signal::Logs,
                window,
                &[],
                now_at_seal(DAY_D + 26),
            )
            .await
            .expect("resolve");
        assert_eq!(
            store.full_gets_of(&first.parts[0].key),
            0,
            "day d's part is served from the reader's decoded cache"
        );
        assert_eq!(
            hours_of(&after),
            vec![
                (DAY_D + 21, 2),
                (DAY_D + 21, 3),
                (DAY_D + 25, 4),
                (DAY_D + 26, 5)
            ],
            "no segment of retired hour 20"
        );
    }

    /// ADR-3939 decision 6: a late commit through a held-open hour of the
    /// previous UTC day re-encodes that sealed day's part without its
    /// retired hours. Day d holds writers 1 (hour 20) and 2 (hour 21), day
    /// d + 1 writer 3 (hour 24). A seal-through fold at hour 25 holds hours
    /// 23 to 25 open (margin hour 22) and retires hour 20 of day d. Writer 6
    /// then lands in hour 23, the last hour of day d, and the fold that
    /// folds it re-encodes day d over hours 21 to 23: hour 20's entries are
    /// dropped for good and the new ref carries no retired hours.
    #[tokio::test]
    async fn a_late_commit_through_a_held_open_hour_re_encodes_the_previous_day() {
        let store = OpLogStore::empty();
        declare_user_id_key_index(store.as_ref()).await;
        for (seq, hour, ids) in [
            (1u64, DAY_D + 20, &[1i64, 2][..]),
            (2, DAY_D + 21, &[10, 20]),
            (3, DAY_D + 24, &[30]),
        ] {
            publish_user_id_logs(store.as_ref(), seq, hour, ids, 2, 0, true).await;
        }
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        let fold_sealing_logs = |now_ns: i64, seal_through_hour: Option<u32>| {
            let catalog = &catalog;
            async move {
                catalog
                    .fold_with_seal_through(
                        &tenant(),
                        Signal::Logs,
                        Uuid::new_v4(),
                        now_ns,
                        &[],
                        None,
                        &RefoldRequest::new(),
                        seal_through_hour,
                    )
                    .await
                    .expect("fold")
            }
        };
        let ten_minutes = 10 * 60 * 1_000_000_000;
        let inside = |hour: u32| i64::from(hour) * NS_PER_HOUR + ten_minutes;
        fold_sealing_logs(inside(DAY_D + 24), Some(DAY_D + 24)).await;
        publish_logs_tombstone(store.as_ref(), DAY_D + 20).await;
        publish_user_id_logs(store.as_ref(), 5, DAY_D + 25, &[60], 2, 0, true).await;
        let sealed = fold_sealing_logs(inside(DAY_D + 25), Some(DAY_D + 25)).await;
        assert_eq!(sealed.watermark_hour, Some(DAY_D + 25), "{sealed:?}");
        let retired = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            (
                retired.parts[0].min_hour,
                retired.parts[0].watermark_hour,
                retired.parts[0].retired_hours.clone()
            ),
            (DAY_D + 20, DAY_D + 21, vec![DAY_D + 20])
        );

        publish_user_id_logs(store.as_ref(), 6, DAY_D + 23, &[70], 2, 0, true).await;
        store.clear();
        let later = inside(DAY_D + 25) + 5 * 60 * 1_000_000_000;
        assert_eq!(catalog.margin_watermark_hour(later), Some(DAY_D + 22));
        let late = fold_sealing_logs(later, None).await;
        assert!(!late.no_op, "{late:?}");
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        let day_d = &head.parts[0];
        assert_eq!(
            (day_d.min_hour, day_d.watermark_hour, day_d.entry_count),
            (DAY_D + 21, DAY_D + 23, 2),
            "day d re-encoded over writers 2 and 6, without hour 20"
        );
        assert!(day_d.retired_hours.is_empty(), "{day_d:?}");
        assert_eq!(store.part_puts().first(), Some(&day_d.key));
        assert_eq!(head.format_version, HEAD_FORMAT_VERSION_1);
        assert!(
            head_entries(store.as_ref(), &head)
                .await
                .iter()
                .all(|e| e.ingest_hour_bucket != DAY_D + 20)
        );
    }

    /// ADR-3939 decision 5: the carry holds a previous slice from its GET
    /// until a pass starts past it. The previous tail indexes `user_id` and
    /// `session` (the same values) over 12 values in 3 writers, cut by a
    /// budget of 4 (a share of 2 a field) into 6 slices a field. The
    /// advancing fold adds one late writer whose value sorts above them all,
    /// so each of the first 6 passes reaches one slice beyond the one it
    /// starts in, and a 7th holds the late value. Every pass holds 1 or 2
    /// slices per (source, field), 2 at the most, each of the 12 slices is
    /// fetched once, the report's peak equals the largest total the passes
    /// held and is below the 12 slices' sum, and the leaves are the one-call
    /// build's. Fetching every slice at load holds 6 a field; fetching a
    /// slice again in each pass it overlaps issues 26 GETs.
    #[tokio::test]
    async fn the_carry_holds_at_most_two_slices_per_field_per_source() {
        use crate::tenant_config::DeclaredColumnType;
        let store = OpLogStore::empty();
        declare_key_index_columns(
            store.as_ref(),
            &[
                (USER_ID, DeclaredColumnType::I64),
                (SESSION, DeclaredColumnType::I64),
            ],
            1,
        )
        .await;
        let fields = [(USER_ID, KeyType::I64), (SESSION, KeyType::I64)];
        for (seq, ids) in [
            (10u64, [1i64, 2, 3, 4]),
            (20, [5, 6, 7, 8]),
            (30, [9, 10, 11, 12]),
        ] {
            publish_keyed_logs(store.as_ref(), seq, KIDX_HOUR, &ids, 2, 0, &fields).await;
        }
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                key_index_build_max_pairs: 4,
                ..kidx_config()
            },
        )
        .expect("catalog");
        fold_logs_sealing(&catalog, now_inside(KIDX_HOUR), Some(KIDX_HOUR)).await;
        let first = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(first.parts.len(), 1);
        let previous = &first.parts[0].key_index;
        for field in [USER_ID, SESSION] {
            assert_eq!(
                previous.iter().filter(|leaf| leaf.field == field).count(),
                6,
                "{field}: 12 pairs under a share of 2"
            );
        }
        let previous_bytes: u64 = previous.iter().map(|leaf| leaf.size).sum();

        publish_keyed_logs(store.as_ref(), 40, KIDX_HOUR, &[100], 2, 0, &fields).await;
        store.clear();
        take_key_index_carry_log_for_test();
        let report = advancing_fold(&catalog).await;
        let log = take_key_index_carry_log_for_test();
        assert_eq!(report.key_index_build_passes, 7, "{report:?}");
        assert_eq!(log.len(), 7, "one record per pass: {log:?}");
        let most = log
            .iter()
            .flat_map(|record| record.slices_held.values().copied())
            .max()
            .expect("a record");
        assert!(most <= 2, "at most two slices a field a source: {log:?}");
        assert!(
            log.iter().all(|record| record.slices_held.len() == 2
                && record.slices_held.values().all(|&held| held >= 1)),
            "{log:?}"
        );
        assert_eq!(most, 2, "{log:?}");
        assert_eq!(
            store.gets_of_leaves(previous),
            previous.len(),
            "each previous slice fetched once"
        );
        assert_eq!(report.key_index_leaf_gets, 12, "{report:?}");
        let largest = log
            .iter()
            .map(|record| record.held_bytes)
            .max()
            .expect("a record");
        assert_eq!(
            report.key_index_carry_peak_held_bytes, largest,
            "{report:?}"
        );
        assert!(
            largest > 0 && largest < previous_bytes,
            "{largest} {previous_bytes}"
        );
        assert_eq!(report.key_index_entries_carried, 3, "{report:?}");

        let part = read_signal_head(store.as_ref(), Signal::Logs)
            .await
            .parts
            .remove(0);
        assert_eq!(
            stored_leaves(store.as_ref(), &part).await,
            reference_leaves(
                store.as_ref(),
                &part,
                &[
                    (USER_ID.to_string(), KeyType::I64),
                    (SESSION.to_string(), KeyType::I64)
                ],
                4,
                &[],
            )
            .await
        );
    }

    /// A source's load that fetched one field's lowest slice and then fails
    /// on another releases the slice's bytes, but the peak keeps them: they
    /// were held.
    #[test]
    fn a_slice_released_by_a_failed_load_stays_in_the_peak() {
        let mut hold = CarryHold::default();
        hold.take(100);
        hold.release(100);
        assert_eq!((hold.held, hold.peak), (0, 100));
        hold.take(40);
        assert_eq!((hold.held, hold.peak), (40, 100));
    }

    /// A carry source's data object keys are derived only for a source a
    /// re-encoded part overlaps. Under a 2-entry part threshold, day d's
    /// writers at hours 1 to 5 fold into three parts with leaves, hours 1 and
    /// 2, 3 and 4, and 5. A writer lands at hour 6, so the advancing fold
    /// re-encodes only the tail, now hours 5 and 6, and derives the key of the
    /// tail's one previous entry, not the five of every previous part.
    #[tokio::test]
    async fn carry_keys_are_derived_only_for_overlapping_sources() {
        let store = OpLogStore::empty();
        declare_user_id_key_index(store.as_ref()).await;
        let mut writers = Vec::new();
        for seq in 1u64..=5 {
            let ids = [seq as i64 * 2 - 1, seq as i64 * 2];
            let (record, _) =
                publish_user_id_logs(store.as_ref(), seq, DAY_D + seq as u32, &ids, 2, 0, true)
                    .await;
            writers.push(record);
        }
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                snapshot_part_max_entries: 2,
                ..kidx_config()
            },
        )
        .expect("catalog");
        fold_through(&catalog, Signal::Logs, DAY_D + 5).await;
        let first = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            first
                .parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour, p.key_index.is_empty()))
                .collect::<Vec<_>>(),
            vec![
                (DAY_D + 1, DAY_D + 2, false),
                (DAY_D + 3, DAY_D + 4, false),
                (DAY_D + 5, DAY_D + 5, false)
            ]
        );

        publish_user_id_logs(store.as_ref(), 6, DAY_D + 6, &[11], 2, 0, true).await;
        store.clear();
        take_key_index_carry_keys_derived_for_test();
        let report = fold_through(&catalog, Signal::Logs, DAY_D + 6).await;
        let derived = take_key_index_carry_keys_derived_for_test();
        let second = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            second
                .parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour))
                .collect::<Vec<_>>(),
            vec![
                (DAY_D + 1, DAY_D + 2),
                (DAY_D + 3, DAY_D + 4),
                (DAY_D + 5, DAY_D + 6)
            ]
        );
        assert_eq!(report.parts_reused, 2, "{report:?}");
        assert_eq!(
            store.section_gets_of(&[&writers[4]]),
            0,
            "the tail's previous entry is carried"
        );
        assert_eq!(derived, 1, "only the tail source's one entry is keyed");
    }

    /// ADR-3939 decision 5: a part carries from every previous part it shares
    /// objects with. Under a 2-entry part threshold, day d's writers at hours
    /// 1 to 4 fold into parts A (hours 1 and 2) and B (hours 3 and 4), each
    /// with 2 leaves under a budget of 2. Hour 1 is tombstoned and a writer
    /// lands at hour 5, so the next fold's first part holds hours 2 and 3:
    /// writer 2 from A and writer 3 from B. It reads neither entry's section,
    /// carries both, and writes the one-call build's leaves. A's two slices
    /// are fetched once; B's are fetched once by each of the two re-encoded
    /// parts that carry from it. Coverage taken from the first source alone
    /// reads writer 3's section.
    #[tokio::test]
    async fn a_part_carries_from_two_previous_parts() {
        let store = OpLogStore::empty();
        declare_user_id_key_index(store.as_ref()).await;
        let mut writers = Vec::new();
        for (seq, ids) in [(1u64, [1i64, 2]), (2, [3, 4]), (3, [5, 6]), (4, [7, 8])] {
            let (record, _) =
                publish_user_id_logs(store.as_ref(), seq, DAY_D + seq as u32, &ids, 2, 0, true)
                    .await;
            writers.push(record);
        }
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                key_index_build_max_pairs: 2,
                snapshot_part_max_entries: 2,
                ..kidx_config()
            },
        )
        .expect("catalog");
        fold_through(&catalog, Signal::Logs, DAY_D + 4).await;
        let first = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            first
                .parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour, p.key_index.len()))
                .collect::<Vec<_>>(),
            vec![(DAY_D + 1, DAY_D + 2, 2), (DAY_D + 3, DAY_D + 4, 2)]
        );

        publish_logs_erasure(store.as_ref(), DAY_D + 1, &[&writers[0]]).await;
        publish_user_id_logs(store.as_ref(), 5, DAY_D + 5, &[9], 2, 0, true).await;
        store.clear();
        let report = fold_through(&catalog, Signal::Logs, DAY_D + 5).await;
        let second = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            second
                .parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour, p.entry_count))
                .collect::<Vec<_>>(),
            vec![(DAY_D + 2, DAY_D + 3, 2), (DAY_D + 4, DAY_D + 5, 2)]
        );
        assert_eq!(
            store.section_gets_of(&[&writers[1], &writers[2]]),
            0,
            "both sources cover their entries"
        );
        assert_eq!(report.key_index_entries_carried, 3, "{report:?}");
        for leaf in &first.parts[0].key_index {
            assert_eq!(store.gets_of_leaves(std::slice::from_ref(leaf)), 1);
        }
        for leaf in &first.parts[1].key_index {
            assert_eq!(store.gets_of_leaves(std::slice::from_ref(leaf)), 2);
        }
        assert_eq!(report.key_index_leaf_gets, 6, "{report:?}");
        assert_eq!(
            stored_leaves(store.as_ref(), &second.parts[0]).await,
            user_id_leaves_at(store.as_ref(), &second.parts[0], 2).await,
            "the two-source carry writes the one-call build's leaves"
        );
    }

    /// ADR-3939 decision 5: a source whose lowest slice fails drops only its
    /// own entries from the carry. In the fixture of
    /// [`a_part_carries_from_two_previous_parts`] the re-encoded part holds
    /// writer 2 from A and writer 3 from B, and the GET of A's lowest slice
    /// is refused. A is not carried, with one warning, and A's later slice is
    /// never fetched; writer 2's section is read in both of the part's 2
    /// passes (2 ranged GETs each), while B's entries, writers 3 and 4, have
    /// no section read and are carried. The leaves equal those a fold of
    /// writers 2 to 5 into a fresh store writes at the same budget, byte for
    /// byte. Dropping the whole carry when one source fails reads writers 3
    /// and 4 too.
    #[tokio::test]
    async fn a_failing_source_drops_only_its_own_entries_from_the_carry() {
        let config = CatalogConfig {
            key_index_build_max_pairs: 2,
            snapshot_part_max_entries: 2,
            ..kidx_config()
        };
        let writer_ids = [(1u64, [1i64, 2]), (2, [3, 4]), (3, [5, 6]), (4, [7, 8])];
        let inner = OpLogStore::empty();
        declare_user_id_key_index(inner.as_ref()).await;
        let mut writers = Vec::new();
        for (seq, ids) in writer_ids {
            let (record, _) =
                publish_user_id_logs(inner.as_ref(), seq, DAY_D + seq as u32, &ids, 2, 0, true)
                    .await;
            writers.push(record);
        }
        let catalog = Catalog::new(inner.clone(), config).expect("catalog");
        fold_through(&catalog, Signal::Logs, DAY_D + 4).await;
        let first = read_signal_head(inner.as_ref(), Signal::Logs).await;
        assert_eq!(first.parts.len(), 2);
        let mut a = first.parts[0].key_index.clone();
        a.sort_by_key(|leaf| leaf.key8_lo);
        assert_eq!(a.len(), 2);

        publish_logs_erasure(inner.as_ref(), DAY_D + 1, &[&writers[0]]).await;
        publish_user_id_logs(inner.as_ref(), 5, DAY_D + 5, &[9], 2, 0, true).await;
        inner.clear();
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Permanent("lowest slice GET refused".into()),
            )
            .with_key_contains(a[0].key.as_str())
            .with_occurrence(Occurrence::Nth(1)),
        );
        let store = Arc::new(FaultStore::new(Arc::clone(&inner), plan));
        let faulted = Catalog::new(store.clone(), config).expect("catalog");
        let (report, warnings) = fold_capturing_warnings(&faulted, Signal::Logs, DAY_D + 5).await;
        assert_eq!(
            store.fault_count(Op::Get, FaultKind::Permanent),
            1,
            "A's lowest slice GET was refused"
        );
        assert_eq!(
            warnings
                .iter()
                .filter(|line| line.contains("previous part leaves not carried"))
                .count(),
            1,
            "{warnings:?}"
        );
        assert_eq!(
            inner.gets_of_leaves(&a),
            0,
            "A's later slice is not fetched"
        );
        assert_eq!(
            inner.section_gets_of(&[&writers[2], &writers[3]]),
            0,
            "B's entries are carried"
        );
        assert_eq!(
            inner.section_gets_of(&[&writers[1]]),
            2 * 2,
            "2 ranged GETs x 2 passes for A's surviving entry"
        );
        assert_eq!(report.key_index_entries_carried, 2, "{report:?}");
        let second = read_signal_head(inner.as_ref(), Signal::Logs).await;
        assert_eq!(
            second
                .parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour, p.entry_count))
                .collect::<Vec<_>>(),
            vec![(DAY_D + 2, DAY_D + 3, 2), (DAY_D + 4, DAY_D + 5, 2)]
        );

        let fresh = OpLogStore::empty();
        declare_user_id_key_index(fresh.as_ref()).await;
        for (seq, ids) in [
            (2u64, &[3i64, 4][..]),
            (3, &[5, 6]),
            (4, &[7, 8]),
            (5, &[9]),
        ] {
            publish_user_id_logs(fresh.as_ref(), seq, DAY_D + seq as u32, ids, 2, 0, true).await;
        }
        let scratch_catalog = Catalog::new(fresh.clone(), config).expect("catalog");
        fold_through(&scratch_catalog, Signal::Logs, DAY_D + 5).await;
        let scratch = read_signal_head(fresh.as_ref(), Signal::Logs).await;
        assert_eq!(scratch.parts.len(), second.parts.len());
        for (scratch_part, part) in scratch.parts.iter().zip(&second.parts) {
            assert_eq!(scratch_part.blake3, part.blake3, "the same part");
            assert_eq!(
                stored_leaves(inner.as_ref(), part).await,
                stored_leaves(fresh.as_ref(), scratch_part).await,
                "the partly carried leaves are the from-scratch leaves, byte for byte"
            );
        }
    }

    /// ADR-3939 decision 5: a later slice is fed only when its
    /// `uncovered_entry_ordinals` equal its set's lowest slice's. A slice ref
    /// binds only `uncovered_count`, so a later slice listing entry 3 where
    /// the lowest lists entry 2 matches its own ref and verifies. The feed
    /// fetches it, refuses it, holds only the lowest slice's pair and keeps
    /// none of its bytes; that `Err` is what restarts the build with no
    /// carry (`a_later_slice_whose_get_fails_restarts_the_build_without_the_carry`).
    /// The same slice listing entry 2 is fed and its pair held.
    #[tokio::test]
    async fn a_later_slice_listing_other_uncovered_entries_is_refused() {
        let limits = key_index_leaf_limits();
        let part_blake3 = [7u8; 32];
        let leaf = |key8_lo: u64, key8_hi: u64, ordinal: u64, key8: u64, uncovered: &[u64]| {
            encode_key_index_leaf(
                &KeyIndexLeafInput {
                    tenant_hash: tenant(),
                    signal: Signal::Logs,
                    part_blake3,
                    part_entry_count: 4,
                    field: USER_ID,
                    key_type: KeyType::I64,
                    key8_lo,
                    key8_hi,
                    uncovered_entry_ordinals: uncovered,
                    entries: &[KeyIndexLeafEntry {
                        key8: key8.to_be_bytes(),
                        entry_ordinal: ordinal,
                        blocks: vec![0],
                    }],
                },
                &limits,
            )
            .expect("the leaf encodes")
        };
        for (later_uncovered, accepted) in [([3u64], false), ([2], true)] {
            let store = MemoryStore::new();
            let lowest = leaf(0, 99, 0, 10, &[2]);
            let lowest_bytes = Bytes::from(lowest.bytes.clone());
            let lowest_ref = put_key_index_leaf(&store, &tenant(), Signal::Logs, KIDX_HOUR, lowest)
                .await
                .expect("put");
            let later = leaf(100, u64::MAX, 1, 200, &later_uncovered);
            assert_eq!(later.leaf_ref.uncovered_count, lowest_ref.uncovered_count);
            let later_ref = put_key_index_leaf(&store, &tenant(), Signal::Logs, KIDX_HOUR, later)
                .await
                .expect("put");
            let mut source = CarriedSource {
                part_blake3: part_blake3.to_vec(),
                part_entry_count: 4,
                fields: BTreeMap::from([(
                    USER_ID.to_string(),
                    CarriedField {
                        slices: vec![
                            CarriedSlice {
                                leaf: lowest_ref,
                                bytes: Some(lowest_bytes.clone()),
                                fetched_in: Some(0),
                            },
                            CarriedSlice {
                                leaf: later_ref,
                                bytes: None,
                                fetched_in: None,
                            },
                        ],
                        uncovered: vec![2],
                        covered: vec![true, true, false, true],
                    },
                )]),
                onto: vec![Some(0), Some(1), Some(2), Some(3)],
            };
            let mut field = FieldBuild::new(KeyType::I64, BTreeSet::from([2]));
            let mut hold = CarryHold::default();
            hold.take(lowest_bytes.len() as u64);
            let mut counters = RequestCounters::default();
            let fed = feed_carried_key_index(
                &store,
                &tenant(),
                Signal::Logs,
                &mut field,
                &mut source,
                USER_ID,
                &[true, true, false, true],
                &limits,
                u64::MAX,
                &mut hold,
                &mut counters,
            )
            .await;
            assert_eq!(
                counters.key_index_leaf_gets, 1,
                "the later slice is fetched"
            );
            let later_slice = &source.fields[USER_ID].slices[1];
            if accepted {
                assert_eq!(fed, Ok(()));
                assert_eq!(field.held.len(), 2, "both slices' pairs held");
                assert!(later_slice.bytes.is_some());
            } else {
                let reason = fed.expect_err("the later slice is refused");
                assert!(
                    reason.contains("other uncovered_entry_ordinals"),
                    "{reason}"
                );
                assert_eq!(field.held.len(), 1, "only the lowest slice's pair");
                assert!(later_slice.bytes.is_none(), "no byte of it kept");
                assert_eq!(hold.held, lowest_bytes.len() as u64);
            }
        }
    }

    /// ADR-3939 decision 5: a previous part is a carry source only when it
    /// shares a data object with the part being encoded, not merely an hour.
    /// The first fold holds writer 1 alone, on day d - 1, so its one part is
    /// encoded with `min_hour` 0 and the fold watermark, hour 22 of day d. A
    /// late writer lands in held-open hour 21 of day d, so the next fold seals
    /// day d - 1 and writes a tail over hours 21 and 22: both overlap the
    /// previous part's range, and only the day d - 1 part shares its object.
    /// The fold GETs the previous leaf once, for day d - 1; a source chosen
    /// by hour overlap alone GETs it a second time for the tail.
    #[tokio::test]
    async fn an_overlapping_part_sharing_no_object_is_not_a_carry_source() {
        let store = OpLogStore::empty();
        declare_user_id_key_index(store.as_ref()).await;
        let sealed = DAY_D + 22;
        publish_user_id_logs(store.as_ref(), 1, DAY_D - 1, &[1, 2], 2, 0, true).await;
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        fold_logs_sealing(&catalog, now_inside(sealed), Some(sealed)).await;
        let first = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            first
                .parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour, p.key_index.len()))
                .collect::<Vec<_>>(),
            vec![(0, sealed, 1)]
        );

        publish_user_id_logs(store.as_ref(), 2, DAY_D + 21, &[3], 2, 0, true).await;
        store.clear();
        let report =
            fold_logs_sealing(&catalog, now_inside(sealed) + 5 * 60 * 1_000_000_000, None).await;
        let second = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            second
                .parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour, p.entry_count))
                .collect::<Vec<_>>(),
            vec![(DAY_D - 1, DAY_D - 1, 1), (DAY_D + 21, sealed, 1)]
        );
        assert_eq!(store.gets_of_leaves(&first.parts[0].key_index), 1);
        assert_eq!(report.key_index_leaf_gets, 1, "{report:?}");
        assert_eq!(report.key_index_entries_carried, 1, "{report:?}");
    }

    /// The warning a fold logs when it carries a part that lacks a declared
    /// field's leaf.
    const MISSING_LEAVES_WARNING: &str = "lack a key-index leaf for a declared field";

    /// [`fold_through`], returning the WARN lines it logged.
    async fn fold_capturing_warnings(
        catalog: &Catalog,
        signal: Signal,
        hour: u32,
    ) -> (FoldReport, Vec<String>) {
        let capture = LevelCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let report = {
            let _default = tracing::subscriber::set_default(subscriber);
            fold_through(catalog, signal, hour).await
        };
        let warnings = capture
            .lines()
            .into_iter()
            .filter(|(level, _)| *level == tracing::Level::WARN)
            .map(|(_, line)| line)
            .collect();
        (report, warnings)
    }

    /// The one missing-leaves warning in `warnings`.
    fn missing_leaves_warning(warnings: &[String]) -> &str {
        let found: Vec<&String> = warnings
            .iter()
            .filter(|line| line.contains(MISSING_LEAVES_WARNING))
            .collect();
        assert_eq!(found.len(), 1, "{warnings:?}");
        found[0]
    }

    fn leaf_fields(part: &SnapshotPartRef) -> Vec<&str> {
        let mut fields: Vec<&str> = part
            .key_index
            .iter()
            .map(|leaf| leaf.field.as_str())
            .collect();
        fields.sort_unstable();
        fields.dedup();
        fields
    }

    /// Day d's two records and its sealed part's ref, folded through day
    /// d + 1's first entry into a store whose op log is then cleared. Every
    /// entry's KEY_IDX section indexes `user_id` and `session`, both I64,
    /// while `session` is declared Str, so every entry is uncovered for it
    /// and day d seals with a `user_id` leaf and no `session` leaf.
    async fn sealed_day_missing_session(
        store: &Arc<OpLogStore>,
        catalog: &Catalog,
    ) -> (Vec<CommitRecord>, SnapshotPartRef) {
        use crate::tenant_config::DeclaredColumnType;
        declare_key_index_columns(
            store.as_ref(),
            &[
                (USER_ID, DeclaredColumnType::I64),
                (SESSION, DeclaredColumnType::Str),
            ],
            1,
        )
        .await;
        let fields = [(USER_ID, KeyType::I64), (SESSION, KeyType::I64)];
        let mut day_d = Vec::new();
        for (seq, hour, ids) in [(1u64, DAY_D + 20, [10, 20]), (2, DAY_D + 22, [30, 40])] {
            let (record, _) =
                publish_keyed_logs(store.as_ref(), seq, hour, &ids, 2, 0, &fields).await;
            day_d.push(record);
        }
        publish_keyed_logs(store.as_ref(), 3, DAY_D + 25, &[50], 2, 0, &fields).await;
        fold_through(catalog, Signal::Logs, DAY_D + 25).await;
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            head.parts
                .iter()
                .map(|p| (p.min_hour, p.watermark_hour))
                .collect::<Vec<_>>(),
            vec![(DAY_D + 20, DAY_D + 22), (DAY_D + 25, DAY_D + 25)],
            "day d is sealed"
        );
        store.clear();
        (day_d, head.parts[0].clone())
    }

    /// ADR-3939 decision 2: the fold builds no leaf for a part it carries by
    /// reference. Day d seals with a `user_id` leaf only (see
    /// [`sealed_day_missing_session`]); once `session` is declared with the
    /// type its sections index, two advancing folds each carry day d's ref
    /// byte for byte, read none of its sections, GET and PUT none of its
    /// leaves, count it in `key_index_parts_missing_declared_leaves` and warn
    /// once naming the tenant, the signal and `session` alone. The repair,
    /// `rebuild_key_index` for `session`, attaches the leaf to the same part
    /// object, and the fold after it carries the repaired ref with no GET or
    /// PUT for day d and counts 0.
    ///
    /// An attach in the fold reads day d's sections on the first fold after
    /// the declaration; a count over re-encoded parts instead of carried ones
    /// reports 0 on both.
    #[tokio::test]
    async fn a_sealed_day_missing_a_declared_fields_leaf_is_carried_unchanged_and_counted() {
        use crate::tenant_config::DeclaredColumnType;
        let store = OpLogStore::empty();
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        let (day_d, sealed) = sealed_day_missing_session(&store, &catalog).await;
        let day_d: Vec<&CommitRecord> = day_d.iter().collect();
        assert_eq!(leaf_fields(&sealed), vec![USER_ID]);

        declare_key_index_columns(
            store.as_ref(),
            &[
                (USER_ID, DeclaredColumnType::I64),
                (SESSION, DeclaredColumnType::I64),
            ],
            2,
        )
        .await;
        let fields = [(USER_ID, KeyType::I64), (SESSION, KeyType::I64)];
        for (seq, hour) in [(4u64, DAY_D + 26), (5, DAY_D + 27)] {
            publish_keyed_logs(store.as_ref(), seq, hour, &[seq as i64 * 10], 2, 0, &fields).await;
            store.clear();
            let (report, warnings) = fold_capturing_warnings(&catalog, Signal::Logs, hour).await;
            let head = read_signal_head(store.as_ref(), Signal::Logs).await;
            assert_eq!(
                head.parts[0].encode_to_vec(),
                sealed.encode_to_vec(),
                "{hour}: day d's ref is carried byte for byte"
            );
            assert_eq!(
                (
                    store.section_gets_of(&day_d),
                    store.leaf_gets_for_part_watermark(DAY_D + 22),
                    store.leaf_puts_for_part_watermark(DAY_D + 22),
                ),
                (0, 0, 0),
                "{hour}: no section GET, leaf GET or leaf PUT for day d"
            );
            assert_eq!(
                report.key_index_parts_missing_declared_leaves, 1,
                "{hour}: {report:?}"
            );
            let warning = missing_leaves_warning(&warnings);
            assert!(
                warning.contains(&format!("tenant={}", tenant().to_hex()))
                    && warning.contains("signal=Logs")
                    && warning.contains("parts=1")
                    && warning.contains("{\"session\"}")
                    && warning.contains("--rebuild-key-index"),
                "{hour}: {warning}"
            );
        }

        catalog
            .rebuild_key_index(
                &tenant(),
                Signal::Logs,
                &crate::KeyIndexRebuildOptions {
                    field: Some(SESSION.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("repair");
        let repaired = read_signal_head(store.as_ref(), Signal::Logs).await.parts[0].clone();
        assert_eq!(
            (&repaired.key, &repaired.blake3),
            (&sealed.key, &sealed.blake3)
        );
        assert_eq!(leaf_fields(&repaired), vec![SESSION, USER_ID]);

        publish_keyed_logs(store.as_ref(), 6, DAY_D + 28, &[60], 2, 0, &fields).await;
        store.clear();
        let (report, warnings) = fold_capturing_warnings(&catalog, Signal::Logs, DAY_D + 28).await;
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(head.parts[0], repaired);
        assert_eq!(
            (
                store.section_gets_of(&day_d),
                store.leaf_gets_for_part_watermark(DAY_D + 22),
                store.leaf_puts_for_part_watermark(DAY_D + 22),
            ),
            (0, 0, 0)
        );
        assert_eq!(
            report.key_index_parts_missing_declared_leaves, 0,
            "{report:?}"
        );
        assert!(
            !warnings
                .iter()
                .any(|line| line.contains(MISSING_LEAVES_WARNING)),
            "{warnings:?}"
        );
    }

    /// ADR-3694 decision 2 under the day seal: a metrics day sealed while an
    /// erasure request is pending is published with no `label_value` leaf.
    /// Once the request is gone, every fold carries the day by reference
    /// with no leaf GET or PUT for it, counts it in
    /// `key_index_parts_missing_declared_leaves` and warns naming
    /// `label_value`; the sealing fold re-encoded it, so counts 0. The
    /// metrics repair attaches the day's leaf and the next fold counts 0.
    ///
    /// A count over parts sealed by the entry cap alone misses the day part.
    #[tokio::test]
    async fn a_metrics_day_sealed_under_the_erasure_gate_is_counted_until_repaired() {
        let store = OpLogStore::empty();
        let catalog = Catalog::new(store.clone(), metrics_kidx_config()).expect("catalog");
        put_metrics_erasure_request(store.as_ref()).await;
        for (seq, hour, region) in [
            (1u64, DAY_D + 20, "east"),
            (2, DAY_D + 22, "east"),
            (3, DAY_D + 25, "west"),
        ] {
            publish_labeled_metrics(
                store.as_ref(),
                seq,
                hour,
                &region_series(region),
                1000,
                false,
            )
            .await;
        }
        let sealing = fold_through(&catalog, Signal::Metrics, DAY_D + 25).await;
        assert_eq!(
            sealing.metrics_label_index_parts_erasure_gated, 2,
            "{sealing:?}"
        );
        assert_eq!(
            sealing.key_index_parts_missing_declared_leaves, 0,
            "the sealing fold re-encodes day d: {sealing:?}"
        );
        let sealed = read_signal_head(store.as_ref(), Signal::Metrics)
            .await
            .parts[0]
            .clone();
        assert_eq!(
            (sealed.min_hour, sealed.watermark_hour),
            (DAY_D + 20, DAY_D + 22)
        );
        assert!(sealed.key_index.is_empty(), "{sealed:?}");

        let request_key =
            keys::erasure_request_key(&tenant(), Signal::Metrics, Uuid::from_u128(0xe7a5e))
                .expect("request key");
        store.delete(&request_key).await.expect("drop the request");
        for (seq, hour) in [(4u64, DAY_D + 26), (5, DAY_D + 27)] {
            publish_labeled_metrics(
                store.as_ref(),
                seq,
                hour,
                &region_series("west"),
                1000,
                false,
            )
            .await;
            store.clear();
            let (report, warnings) = fold_capturing_warnings(&catalog, Signal::Metrics, hour).await;
            let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
            assert_eq!(head.parts[0], sealed, "{hour}: day d is carried unchanged");
            assert_eq!(
                report.metrics_label_index_parts_erasure_gated, 0,
                "{hour}: {report:?}"
            );
            assert_eq!(
                report.key_index_parts_missing_declared_leaves, 1,
                "{hour}: {report:?}"
            );
            assert_eq!(
                (
                    store.leaf_gets_for_part_watermark(DAY_D + 22),
                    store.leaf_puts_for_part_watermark(DAY_D + 22),
                ),
                (0, 0),
                "{hour}: no leaf GET or PUT for day d"
            );
            let warning = missing_leaves_warning(&warnings);
            assert!(
                warning.contains("signal=Metrics")
                    && warning.contains(&format!("{{\"{METRICS_LABEL_VALUE_FIELD}\"}}")),
                "{hour}: {warning}"
            );
        }

        catalog
            .rebuild_key_index(&tenant(), Signal::Metrics, &Default::default())
            .await
            .expect("repair");
        let repaired = read_signal_head(store.as_ref(), Signal::Metrics)
            .await
            .parts[0]
            .clone();
        assert_eq!(
            (&repaired.key, &repaired.blake3),
            (&sealed.key, &sealed.blake3)
        );
        assert_eq!(leaf_fields(&repaired), vec![METRICS_LABEL_VALUE_FIELD]);

        publish_labeled_metrics(
            store.as_ref(),
            6,
            DAY_D + 28,
            &region_series("west"),
            1000,
            false,
        )
        .await;
        store.clear();
        let report = fold_through(&catalog, Signal::Metrics, DAY_D + 28).await;
        let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
        assert_eq!(head.parts[0], repaired);
        assert_eq!(
            report.key_index_parts_missing_declared_leaves, 0,
            "{report:?}"
        );
        assert_eq!(
            (
                store.leaf_gets_for_part_watermark(DAY_D + 22),
                store.leaf_puts_for_part_watermark(DAY_D + 22),
            ),
            (0, 0)
        );
    }

    /// `key_index_parts_missing_declared_leaves` is 0 with the leaf switch
    /// off. Under an entry threshold of 1, a switch-on fold seals day d's
    /// one indexed entry with a `user_id` leaf and leaves day d + 1's
    /// unindexed entry with none; the next fold re-partitions identically
    /// under either switch and carries both parts by reference. Switch on,
    /// it counts the leafless part; switch off, it counts 0 and reports
    /// `key_index_switch_off_over_leaves`.
    ///
    /// A count that reads the declared fields whatever the switch reports 1
    /// for the switch-off fold.
    #[tokio::test]
    async fn the_missing_leaves_count_is_zero_with_the_switch_off() {
        let fold_pair = |second_on: bool| async move {
            let store = Arc::new(MemoryStore::new());
            declare_user_id_key_index(store.as_ref()).await;
            publish_user_id_logs(store.as_ref(), 1, DAY_D + 20, &[10], 2, 0, true).await;
            publish_user_id_logs(store.as_ref(), 2, DAY_D + 25, &[20], 2, 0, false).await;
            let catalog_with = |build_key_index_leaves: bool| {
                Catalog::new(
                    store.clone(),
                    CatalogConfig {
                        build_key_index_leaves,
                        snapshot_part_max_entries: 1,
                        ..config(1)
                    },
                )
                .expect("catalog")
            };
            fold_through(&catalog_with(true), Signal::Logs, DAY_D + 25).await;
            let first = read_signal_head(store.as_ref(), Signal::Logs).await;
            assert_eq!(
                first
                    .parts
                    .iter()
                    .map(|p| p.key_index.len())
                    .collect::<Vec<_>>(),
                vec![1, 0]
            );
            publish_user_id_logs(store.as_ref(), 3, DAY_D + 26, &[30], 2, 0, false).await;
            let report = fold_through(&catalog_with(second_on), Signal::Logs, DAY_D + 26).await;
            let second = read_signal_head(store.as_ref(), Signal::Logs).await;
            assert_eq!(report.parts_reused, 2, "{second_on}: {report:?}");
            assert_eq!(second.parts[..2], first.parts[..], "{second_on}");
            report
        };

        let on = fold_pair(true).await;
        assert_eq!(on.key_index_parts_missing_declared_leaves, 1, "{on:?}");
        assert!(!on.key_index_switch_off_over_leaves, "{on:?}");

        let off = fold_pair(false).await;
        assert_eq!(off.key_index_parts_missing_declared_leaves, 0, "{off:?}");
        assert!(off.key_index_switch_off_over_leaves, "{off:?}");
    }

    /// `key_index_switch_off_over_leaves` reads every part of the HEAD: here
    /// only sealed day d carries a leaf ref, since day d + 1's entry indexes
    /// no field, and a switch-off fold still reports it. A check of the
    /// tail part alone reports `false`.
    #[tokio::test]
    async fn key_index_switch_off_over_leaves_reads_a_non_tail_part() {
        let store = Arc::new(MemoryStore::new());
        declare_user_id_key_index(store.as_ref()).await;
        publish_user_id_logs(store.as_ref(), 1, DAY_D + 20, &[10], 2, 0, true).await;
        publish_user_id_logs(store.as_ref(), 2, DAY_D + 25, &[20], 2, 0, false).await;
        let on = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        fold_through(&on, Signal::Logs, DAY_D + 25).await;
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(
            head.parts
                .iter()
                .map(|p| p.key_index.len())
                .collect::<Vec<_>>(),
            vec![1, 0]
        );
        publish_user_id_logs(store.as_ref(), 3, DAY_D + 26, &[30], 2, 0, false).await;
        let off = Catalog::new(
            store.clone(),
            CatalogConfig {
                build_key_index_leaves: false,
                ..config(1)
            },
        )
        .expect("catalog");
        let report = fold_through(&off, Signal::Logs, DAY_D + 26).await;
        assert!(report.key_index_switch_off_over_leaves, "{report:?}");
    }

    /// A new entry whose section names a field the tenant does not declare,
    /// read while the carry is active, drops the carry: the build restarts
    /// with every entry read and stores what the from-scratch build stores,
    /// a `session` leaf for the section-named field beside the `user_id`
    /// one, every entry but the two late ones uncovered for `session`.
    /// Writer 15 is read first (ordinal 1) and stops the carried attempt
    /// after one section, so 1 + 5 sections and 2 passes. Without the guard
    /// the carried entries 20 and 30 are never read for `session` and its
    /// leaf claims them covered.
    #[tokio::test]
    async fn a_section_naming_an_undeclared_field_drops_the_carry() {
        let inner = recording_store();
        carry_guard_fixture(&inner, &[(USER_ID, KeyType::I64), (SESSION, KeyType::I64)]).await;
        let catalog = Catalog::new(inner.clone(), kidx_config()).expect("catalog");
        let report = advancing_fold(&catalog).await;
        assert_eq!(report.key_index_leaf_gets, 1, "{report:?}");
        assert_eq!(report.key_index_entries_carried, 0, "{report:?}");
        assert_eq!(report.key_index_sections_read, 1 + 5, "{report:?}");
        assert_eq!(report.key_index_build_passes, 2, "{report:?}");
        let second = read_signal_head(inner.as_ref(), Signal::Logs).await;
        let part = &second.parts[0];
        let session: Vec<_> = part
            .key_index
            .iter()
            .filter(|l| l.field == SESSION)
            .collect();
        assert_eq!(session.len(), 1, "{part:?}");
        assert_eq!(session[0].uncovered_count, 3, "{part:?}");
        assert_eq!(
            stored_leaves(inner.as_ref(), part).await,
            from_scratch_leaves_declaring(
                inner.as_ref(),
                part,
                &[(USER_ID.to_string(), KeyType::I64)]
            )
            .await
        );
    }

    /// ADR-3692 decision 5: a (part, field) over the pair budget is built in
    /// passes over disjoint key8 ranges, each pass re-reading the sections,
    /// and each pass closes its slices, so the slices tile `[0, u64::MAX]`
    /// with no gap and no overlap and a pass end is a slice end. Twelve pairs
    /// of one entry per key8 under a budget of five: passes of five, five and
    /// two, which the 40-byte ceiling (three entries) cuts into slices of
    /// three and two, three and two, and two: 5 leaves, the leaves the
    /// one-call build writes at budget 5, where one pass writes 4 slices of
    /// three.
    #[tokio::test]
    async fn pair_budget_builds_in_passes_and_closes_a_slice_at_each_pass_end() {
        let (store, report, head) = budget_fixture_fold(5).await;
        let k = |v: i64| u64::from_be_bytes(key8_i64(v));
        assert_eq!(
            pass_ranges(&take_key_index_pass_log_for_test(), USER_ID, 1),
            vec![(0, 0, Some(k(6))), (0, k(6), Some(k(11))), (0, k(11), None)],
            "passes of five, five and two pairs"
        );
        let part = &head.parts[0];
        let mut tiles: Vec<(u64, u64)> = part
            .key_index
            .iter()
            .map(|leaf| (leaf.key8_lo, leaf.key8_hi))
            .collect();
        tiles.sort_unstable();
        assert_eq!(tiles.first().map(|t| t.0), Some(0), "{tiles:?}");
        assert_eq!(tiles.last().map(|t| t.1), Some(u64::MAX), "{tiles:?}");
        for pair in tiles.windows(2) {
            assert_eq!(
                pair[0].1.checked_add(1),
                Some(pair[1].0),
                "no gap and no overlap: {tiles:?}"
            );
        }
        let summaries: Vec<KeyIndexLeafSummary> = stored_leaves(store.as_ref(), part)
            .await
            .iter()
            .map(|bytes| verify_key_index_leaf(bytes, &key_index_leaf_limits()).expect("leaf"))
            .collect();
        check_key_index_leaf_set(&summaries).expect("the slices form one set");
        let pairs: u64 = part.key_index.iter().map(|leaf| leaf.entry_count).sum();
        assert_eq!(pairs, 12);
        assert_eq!(
            tiles,
            vec![
                (0, k(4) - 1),
                (k(4), k(6) - 1),
                (k(6), k(9) - 1),
                (k(9), k(11) - 1),
                (k(11), u64::MAX),
            ],
            "slices of three and two closed at each pass end"
        );
        assert_eq!(
            report.key_index_build_passes,
            greedy_passes(&[1; 12], 5),
            "{report:?}"
        );
        assert_eq!(report.key_index_build_passes, 3);
        assert_eq!(report.key_index_sections_read, 3 * 3, "{report:?}");
        assert_eq!(data_object_ranged_gets(&store).len(), 3 * 2 * 3);

        let (one_pass_store, one_pass, one_pass_head) = budget_fixture_fold(u64::MAX).await;
        assert_eq!(one_pass.key_index_build_passes, 1, "{one_pass:?}");
        assert_eq!(one_pass.key_index_sections_read, 3, "{one_pass:?}");
        assert_eq!(one_pass_head.parts[0].key_index.len(), 4, "slices of three");
        assert_eq!(
            stored_leaves(store.as_ref(), part).await,
            user_id_leaves_at(store.as_ref(), part, 5).await
        );
        assert_eq!(
            stored_leaves(one_pass_store.as_ref(), &one_pass_head.parts[0]).await,
            user_id_leaves_at(one_pass_store.as_ref(), &one_pass_head.parts[0], u64::MAX).await
        );
        set_key_index_leaf_body_ceiling_for_test(None);
    }

    /// A tail the next fold seals unchanged encodes to the same bytes, so the
    /// fold carries it by reference with its leaf refs and runs no build for
    /// it: zero section GETs of its entry and zero leaf GETs. The new tail
    /// shares no data object with the previous one, so no previous leaf is
    /// read for it either. A part ceiling of two seals hour 1's two entries
    /// and then hour 2's one entry beside hour 3's two.
    #[tokio::test]
    async fn sealed_tail_carries_its_leaf_refs_without_a_build() {
        let h1 = KIDX_HOUR;
        let store = recording_store();
        declare_user_id_key_index(store.as_ref()).await;
        publish_user_id_logs(store.as_ref(), 1, h1, &[10, 20], 2, 0, true).await;
        publish_user_id_logs(store.as_ref(), 2, h1, &[10, 30], 2, 0, true).await;
        let (sealed, _) = publish_user_id_logs(store.as_ref(), 3, h1 + 1, &[40], 2, 0, true).await;
        let sealed_key = keys::reconstruct_data_key(&sealed).expect("data key");
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                snapshot_part_max_entries: 2,
                ..kidx_config()
            },
        )
        .expect("catalog");
        fold_through(&catalog, Signal::Logs, h1 + 1).await;
        let first = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(first.parts.len(), 2);
        assert_eq!(first.parts[1].key_index.len(), 1, "the tail had a leaf");

        publish_user_id_logs(store.as_ref(), 4, h1 + 2, &[50], 2, 0, true).await;
        publish_user_id_logs(store.as_ref(), 5, h1 + 2, &[60], 2, 0, true).await;
        store.gets.lock().expect("lock").clear();
        let report = fold_through(&catalog, Signal::Logs, h1 + 2).await;
        let second = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(second.parts.len(), 3, "{report:?}");
        assert_eq!(report.parts_reused, 2, "{report:?}");
        assert_eq!(
            second.parts[1], first.parts[1],
            "the sealed part's ref, its leaf refs included, is the last tail's"
        );
        assert_eq!(gets_containing(&store, &sealed_key), 0);
        assert_eq!(gets_containing(&store, ".kidx"), 0);
        assert_eq!(report.key_index_leaf_gets, 0, "{report:?}");
        assert_eq!(report.key_index_entries_carried, 0, "{report:?}");
        assert_eq!(
            report.key_index_sections_read, 2,
            "the new tail's two entries"
        );
        assert_eq!(report.key_index_build_passes, 1, "{report:?}");
    }

    /// A leaf encode the encoder refuses leaves the part uncovered for the
    /// field and the fold still publishes the part: no leaf is written, every
    /// entry counts as uncovered, and HEAD names the part with no `key_index`
    /// ref for the field.
    #[tokio::test]
    async fn a_refused_leaf_encode_uncovers_the_field_and_the_part_is_published() {
        let store = Arc::new(MemoryStore::new());
        declare_user_id_key_index(store.as_ref()).await;
        for seq in 1..=3u64 {
            publish_user_id_logs(store.as_ref(), seq, KIDX_HOUR, &[10, 20], 2, 0, true).await;
        }
        let catalog = Catalog::new(store.clone(), kidx_config()).expect("catalog");
        refuse_key_index_leaf_encode_for_test(Some(USER_ID));
        let folded = catalog
            .fold(
                &tenant(),
                Signal::Logs,
                Uuid::new_v4(),
                now_at_seal(KIDX_HOUR),
                &[],
                None,
            )
            .await;
        refuse_key_index_leaf_encode_for_test(None);
        let report = folded.expect("a refused leaf encode does not fail the fold");
        assert_eq!(report.entry_count, 3, "{report:?}");
        assert_eq!(report.key_index_ranged_gets, 6, "the sections were read");
        assert_eq!(report.key_index_leaves_written, 0, "{report:?}");
        assert_eq!(report.key_index_leaf_bytes_written, 0, "{report:?}");
        assert_eq!(report.key_index_uncovered_entries, 3, "{report:?}");
        let head = read_signal_head(store.as_ref(), Signal::Logs).await;
        assert_eq!(head.parts.len(), 1);
        assert_eq!(head.parts[0].entry_count, 3);
        assert!(head.parts[0].key_index.is_empty());
        store
            .get(&head.parts[0].key, GetRange::Full)
            .await
            .expect("the part is published");
        assert_eq!(
            kidx_objects(store.as_ref(), Signal::Logs).await,
            Vec::<String>::new()
        );
    }

    fn i64_field(name: &str, pairs: &[(i64, u32)]) -> KeyIdxFieldEntries {
        KeyIdxFieldEntries {
            name: name.to_string(),
            key_type: KeyType::I64,
            entries: pairs.iter().map(|&(v, b)| (key8_i64(v), b)).collect(),
        }
    }

    /// One `key8` whose own entries exceed the body ceiling cannot be sliced:
    /// every entry carrying it is uncovered for the field, and the leaf holds
    /// the rest. Each single-block entry here encodes to 11 bytes, so under a
    /// 20-byte ceiling the run of value 1 (entries 0 and 1, 22 bytes) is over
    /// it and value 3 (entry 2 alone) is not. Entry 0's other value, 2, goes
    /// with it: an uncovered entry is uncovered for every key. Also pins the
    /// key-type rule (entry 3 names the field as Str, so it is uncovered) and
    /// a declared field no section names (no leaf, every entry uncovered).
    #[test]
    fn a_key8_over_the_ceiling_uncovers_its_entries_and_the_rest_is_indexed() {
        let reads: Vec<EntryKeyIndex> = vec![
            Ok(vec![i64_field(USER_ID, &[(1, 0), (2, 0)])]),
            Ok(vec![i64_field(USER_ID, &[(1, 4)])]),
            Ok(vec![i64_field(USER_ID, &[(3, 7)])]),
            Ok(vec![KeyIdxFieldEntries {
                name: USER_ID.to_string(),
                key_type: KeyType::Str,
                entries: vec![(key8_i64(3), 0)],
            }]),
        ];
        let limits = KeyIndexLeafLimits { max_body_bytes: 20 };
        let built = build_part_key_index_leaves(
            &tenant(),
            Signal::Logs,
            [5u8; 32],
            &[
                (USER_ID.to_string(), KeyType::I64),
                ("order_id".to_string(), KeyType::I64),
            ],
            true,
            &reads.iter().collect::<Vec<_>>(),
            &limits,
            u64::MAX,
        );
        assert_eq!(built.leaves.len(), 1, "user_id only; order_id has no leaf");
        assert_eq!(
            built.uncovered_entries,
            3 + 4,
            "user_id 0, 1, 3; order_id all four"
        );
        let summary = verify_key_index_leaf(&built.leaves[0].bytes, &limits).expect("valid leaf");
        assert_eq!(summary.header.uncovered_entry_ordinals, vec![0, 1, 3]);
        assert_eq!(summary.header.part_entry_count, 4);
        assert_eq!(
            summary.entries,
            vec![KeyIndexLeafEntry {
                key8: key8_i64(3),
                entry_ordinal: 2,
                blocks: vec![7],
            }]
        );
    }

    // ---- ADR-3694 decision 2: the metrics `label_value` leaf ----

    use ravel_codec::key_idx::key8_str;

    /// The fixture's metric name.
    pub(crate) const CPU_SECONDS: &str = "cpu_seconds";

    /// Both metrics switches on, one shard.
    pub(crate) fn metrics_kidx_config() -> CatalogConfig {
        CatalogConfig {
            build_key_index_leaves: true,
            metrics_label_index: true,
            ..config(1)
        }
    }

    /// Publishes one real RSEG object into `hour` (shard 0, writer
    /// `writer_seq`): one `cpu_seconds` series per element of `series`, which
    /// lists the series' labels besides `__name__`, each with `samples`
    /// pseudo-random samples so the object's data bytes dwarf its LABEL_DICT.
    /// `corrupt_label_dict` flips one byte inside the stored LABEL_DICT
    /// section; the record's content hash is of the bytes as stored.
    pub(crate) async fn publish_labeled_metrics(
        store: &dyn ObjectStoreBackend,
        writer_seq: u64,
        hour: u32,
        series: &[Vec<(&str, &str)>],
        samples: usize,
        corrupt_label_dict: bool,
    ) -> CommitRecord {
        let tenant_id = TenantId::new("metrics-label-index");
        let base_ts = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
        let mut state = writer_seq.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut inputs = Vec::with_capacity(series.len());
        for labels in series {
            let mut all = vec![Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: CPU_SECONDS.to_string(),
            }];
            all.extend(labels.iter().map(|(name, value)| Label {
                name: (*name).to_string(),
                value: (*value).to_string(),
            }));
            let labels = LabelSet::new(all).expect("valid labels");
            let series_id = SeriesId::compute(&tenant_id, CPU_SECONDS, &labels).expect("series id");
            let mut points = Vec::with_capacity(samples);
            for i in 0..samples {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                points.push(Sample {
                    ts_ns: base_ts + i as i64 * 1_000_000,
                    value: (state >> 11) as f64 / (1u64 << 53) as f64,
                });
            }
            inputs.push(SeriesInput {
                series_id,
                labels,
                samples: points,
            });
        }
        let writer_id = Uuid::from_u128(u128::from(writer_seq));
        let min_ingest_ts_ns = base_ts;
        let max_ingest_ts_ns = base_ts + samples as i64 * 1_000_000;
        let written = SegmentWriter::write(
            inputs,
            SegmentIdentity {
                tenant_hash: tenant().0,
                shard: 0,
                writer_id: writer_id.to_string(),
                writer_epoch: 1,
                writer_seq,
            },
            IngestBounds {
                min_ingest_ts_ns,
                max_ingest_ts_ns,
            },
        )
        .expect("write segment");
        let mut bytes = written.bytes.to_vec();
        if corrupt_label_dict {
            let (offset, len) = label_dict_range(&bytes);
            bytes[(offset + len / 2) as usize] ^= 0xff;
        }
        let record = record::build(NewCommitRecord {
            tenant_hash: tenant(),
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            segment_format_version: 1,
            created_unix_ns: base_ts,
            ingest_hour_bucket: hour,
        })
        .expect("valid record");
        let data_key = keys::reconstruct_data_key(&record).expect("data key");
        publish::put_data_object(store, &data_key, Bytes::from(bytes))
            .await
            .expect("put data object");
        publish::publish(store, &record, &RetryPolicy::default())
            .await
            .expect("publish");
        record
    }

    /// `(offset, len)` of an RSEG object's stored LABEL_DICT section.
    pub(crate) fn label_dict_range(object: &[u8]) -> (u64, u64) {
        let location = match ravel_segment::open_from_suffix(
            object,
            object.len() as u64,
            ReaderLimits::default(),
        )
        .expect("footer opens")
        {
            ravel_segment::FooterOutcome::Ready(location) => location,
            ravel_segment::FooterOutcome::NeedRange { .. } => panic!("whole object in hand"),
        };
        let section = location
            .footer
            .sections
            .iter()
            .find(|s| s.kind == RSEG_LABEL_DICT_KIND)
            .expect("LABEL_DICT section");
        (section.offset, section.len)
    }

    /// Four series per object, `instance` i0 to i3, all with `region` set to
    /// `region`.
    pub(crate) fn region_series(region: &'static str) -> Vec<Vec<(&'static str, &'static str)>> {
        ["i0", "i1", "i2", "i3"]
            .into_iter()
            .map(|instance| vec![("instance", instance), ("region", region)])
            .collect()
    }

    /// The writers of [`publish_label_fixture`] whose objects hold "east".
    pub(crate) const EAST_WRITERS: [u64; 2] = [2, 5];

    /// Six objects in [`KIDX_HOUR`], writers 1 to 6, [`region_series`] with
    /// 1000 samples per series: `region` is "east" for [`EAST_WRITERS`] and
    /// "west" for the other four.
    pub(crate) async fn publish_label_fixture(store: &dyn ObjectStoreBackend) -> Vec<CommitRecord> {
        let mut records = Vec::new();
        for seq in 1..=6u64 {
            let region = if EAST_WRITERS.contains(&seq) {
                "east"
            } else {
                "west"
            };
            records.push(
                publish_labeled_metrics(store, seq, KIDX_HOUR, &region_series(region), 1000, false)
                    .await,
            );
        }
        records
    }

    async fn probe_label(
        catalog: &Catalog,
        part: &SnapshotPartRef,
        value: &str,
    ) -> KeyIndexLeafProbe {
        probe_part(
            catalog,
            Signal::Metrics,
            part,
            KeyType::Str,
            METRICS_LABEL_VALUE_FIELD,
            key8_str(value),
        )
        .await
    }

    /// ADR-3694 decision 2, the acceptance test: with both switches on, the
    /// fold builds one `label_value` leaf for the part from every object's
    /// LABEL_DICT, and a probe names exactly the objects holding a string,
    /// each at block 0. "east" sits in two of the six objects, "north" in
    /// none, and the label name "region" in all six. The objects reach the
    /// leaf build through the postings build's whole-object GETs, so the
    /// build issues no ranged GET, and the leaf stays within 1% of the part's
    /// data bytes.
    ///
    /// A build that indexed only label names answers "east" as absent; one
    /// that read only the part's first object answers "east" as absent too.
    #[tokio::test]
    async fn metrics_label_leaf_routes_exactly_the_holders() {
        let store = recording_store();
        let records = publish_label_fixture(store.as_ref()).await;
        let catalog = Catalog::new(store.clone(), metrics_kidx_config()).expect("catalog");
        store.gets.lock().expect("lock").clear();
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        assert_eq!(report.entry_count, 6, "{report:?}");
        assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
        assert_eq!(report.key_index_uncovered_entries, 0, "{report:?}");
        assert_eq!(report.metrics_label_index_parts_over_cap, 0, "{report:?}");
        assert_eq!(report.metrics_label_index_parts_erasure_gated, 0);
        assert_eq!(report.metrics_label_index_erasure_list_requests, 1);
        assert_eq!(
            data_object_ranged_gets(&store),
            Vec::<(String, GetRange)>::new(),
            "every object is read from the postings build's bytes"
        );
        assert_eq!(report.key_index_ranged_gets, 0, "{report:?}");

        let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
        assert_eq!(head.parts.len(), 1);
        let part = &head.parts[0];
        assert_eq!(part.key_index.len(), 1);
        assert_eq!(part.key_index[0].field, METRICS_LABEL_VALUE_FIELD);
        let ordinal = part_ordinals(store.as_ref(), part).await;

        let east: Vec<(u64, &[u32])> = EAST_WRITERS
            .iter()
            .map(|seq| (ordinal[seq], &[0u32][..]))
            .collect();
        assert_eq!(
            probe_label(&catalog, part, "east").await,
            matches(&east, &[])
        );
        assert_eq!(
            probe_label(&catalog, part, "north").await,
            KeyIndexLeafProbe::Absent {
                uncovered_entry_ordinals: Vec::new()
            }
        );
        let all: Vec<(u64, &[u32])> = (1..=6u64).map(|seq| (ordinal[&seq], &[0u32][..])).collect();
        assert_eq!(
            probe_label(&catalog, part, "region").await,
            matches(&all, &[])
        );
        assert_eq!(
            probe_label(&catalog, part, CPU_SECONDS).await,
            matches(&all, &[])
        );

        let leaf_bytes = report.key_index_leaf_bytes_written;
        let data_bytes: u64 = records.iter().map(|r| r.object_size).sum();
        println!("label_value leaf bytes {leaf_bytes}, part data bytes {data_bytes}");
        assert_eq!(leaf_bytes, part.key_index[0].size);
        assert!(
            leaf_bytes * 100 <= data_bytes,
            "{leaf_bytes} leaf bytes over 1% of {data_bytes}"
        );
    }

    /// With `build_key_index_leaves` on and `metrics_label_index` off, a
    /// metrics fold builds no leaf, reads no LABEL_DICT by range and lists no
    /// pending erasure.
    #[tokio::test]
    async fn the_metrics_leaf_is_off_without_metrics_label_index() {
        let store = recording_store();
        publish_label_fixture(store.as_ref()).await;
        let cfg = CatalogConfig {
            metrics_label_index: false,
            ..metrics_kidx_config()
        };
        assert!(cfg.build_key_index_leaves);
        assert!(!CatalogConfig::default().metrics_label_index);
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        assert_eq!(report.entry_count, 6, "{report:?}");
        assert_eq!(report.key_index_leaves_written, 0, "{report:?}");
        assert_eq!(report.key_index_sections_read, 0, "{report:?}");
        assert_eq!(report.metrics_label_index_erasure_list_requests, 0);
        assert_eq!(
            data_object_ranged_gets(&store),
            Vec::<(String, GetRange)>::new()
        );
        assert_eq!(
            kidx_objects(store.as_ref(), Signal::Metrics).await,
            Vec::<String>::new()
        );
        let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
        assert!(head.parts.iter().all(|p| p.key_index.is_empty()));
    }

    /// PUTs a pending metrics erasure request for `region = east`.
    pub(crate) async fn put_metrics_erasure_request(store: &dyn ObjectStoreBackend) {
        use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
        let request_id = Uuid::from_u128(0xe7a5e);
        let request = ErasureRequest {
            format_version: 1,
            tenant_hash: tenant().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
            request_id: request_id.to_string(),
            created_unix_ns: i64::from(KIDX_HOUR) * NS_PER_HOUR,
            predicate: vec![ErasurePredicateMatcher {
                key: "region".to_string(),
                value: "east".to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: String::new(),
        };
        let key =
            keys::erasure_request_key(&tenant(), Signal::Metrics, request_id).expect("request key");
        store
            .put(
                &key,
                erasure::encode_request(&request),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put request");
    }

    /// A pending metrics erasure request gates the leaf build: the fold
    /// writes no leaf and reads no LABEL_DICT, counts the part as gated and
    /// the one LIST of the pending set. A listing that fails gates the same
    /// way, its LIST counted too.
    #[tokio::test]
    async fn a_pending_erasure_request_builds_no_metrics_leaf() {
        let store = recording_store();
        publish_label_fixture(store.as_ref()).await;
        put_metrics_erasure_request(store.as_ref()).await;
        let catalog = Catalog::new(store.clone(), metrics_kidx_config()).expect("catalog");
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        assert_eq!(report.entry_count, 6, "{report:?}");
        assert_eq!(
            report.metrics_label_index_parts_erasure_gated, 1,
            "{report:?}"
        );
        assert_eq!(report.metrics_label_index_erasure_list_requests, 1);
        assert_eq!(report.key_index_leaves_written, 0, "{report:?}");
        assert_eq!(report.key_index_sections_read, 0, "{report:?}");
        assert_eq!(
            data_object_ranged_gets(&store),
            Vec::<(String, GetRange)>::new()
        );
        assert_eq!(
            kidx_objects(store.as_ref(), Signal::Metrics).await,
            Vec::<String>::new()
        );
        let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
        assert!(head.parts.iter().all(|p| p.key_index.is_empty()));

        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::List,
                ScriptedFault::Permanent("del LIST refused".into()),
            )
            .with_key_contains("/del/"),
        );
        let inner = Arc::new(MemoryStore::new());
        publish_label_fixture(inner.as_ref()).await;
        let store = Arc::new(FaultStore::new(Arc::clone(&inner), plan));
        let catalog = Catalog::new(store.clone(), metrics_kidx_config()).expect("catalog");
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        assert_eq!(store.fault_count(Op::List, FaultKind::Permanent), 1);
        assert_eq!(
            report.metrics_label_index_parts_erasure_gated, 1,
            "{report:?}"
        );
        assert_eq!(report.metrics_label_index_erasure_list_requests, 1);
        assert_eq!(report.key_index_leaves_written, 0, "{report:?}");
        assert_eq!(
            kidx_objects(inner.as_ref(), Signal::Metrics).await,
            Vec::<String>::new()
        );
    }

    /// A part whose leaf would pass the byte cap is uncovered for
    /// `label_value`: no leaf is PUT, the part has no ref, the report counts
    /// the part over the cap and every entry uncovered, and the fold
    /// publishes. The cap is the part's data bytes times the ratio.
    #[tokio::test]
    async fn a_part_over_the_metrics_byte_cap_is_uncovered() {
        assert_eq!(
            metrics_label_index_leaf_bytes_cap(1_000_000, 10_000),
            10_000
        );
        assert_eq!(metrics_label_index_leaf_bytes_cap(199, 10_000), 1);
        assert_eq!(
            metrics_label_index_leaf_bytes_cap(u64::MAX, 1_000_000),
            u64::MAX
        );
        let store = recording_store();
        publish_label_fixture(store.as_ref()).await;
        let cfg = CatalogConfig {
            metrics_label_index_max_ratio_ppm: 1,
            ..metrics_kidx_config()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        assert_eq!(report.entry_count, 6, "{report:?}");
        assert_eq!(report.metrics_label_index_parts_over_cap, 1, "{report:?}");
        assert_eq!(report.key_index_uncovered_entries, 6, "{report:?}");
        assert_eq!(report.key_index_leaves_written, 0, "{report:?}");
        assert_eq!(
            kidx_objects(store.as_ref(), Signal::Metrics).await,
            Vec::<String>::new()
        );
        let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
        assert_eq!(head.parts.len(), 1);
        assert!(head.parts[0].key_index.is_empty());
    }

    /// A reconciled fold over a tenant whose parts are mostly reused holds
    /// LABEL_DICT pairs only for the part it builds a leaf for. One object
    /// per hour, one part per hour; a tombstone on the fourth hour makes the
    /// next fold reconcile, so its postings build fetches every entry whole,
    /// yet the parts for the first, second, third and fifth hours are reused
    /// by reference and only the new tail (ordinal 4, nine pairs) is held,
    /// and released once its leaf is built. Holding by fetch rather than by
    /// need held all five entries, 45 pairs.
    #[tokio::test]
    async fn a_reconciled_metrics_fold_holds_no_pairs_for_reused_parts() {
        let store = recording_store();
        let cfg = CatalogConfig {
            snapshot_part_max_entries: 1,
            metrics_label_index_max_ratio_ppm: 1_000_000,
            ..metrics_kidx_config()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");
        for offset in 0..5u32 {
            let region = if offset % 2 == 0 { "west" } else { "east" };
            publish_labeled_metrics(
                store.as_ref(),
                u64::from(offset) + 1,
                KIDX_HOUR + offset,
                &region_series(region),
                1000,
                false,
            )
            .await;
        }
        let first = fold_through(&catalog, Signal::Metrics, KIDX_HOUR + 4).await;
        assert_eq!(first.key_index_leaves_written, 5, "{first:?}");
        take_held_label_dict_log_for_test();

        publish_tombstone(&store.inner, 0, KIDX_HOUR + 3).await;
        publish_labeled_metrics(
            store.as_ref(),
            6,
            KIDX_HOUR + 5,
            &region_series("east"),
            1000,
            false,
        )
        .await;
        store.gets.lock().expect("lock").clear();
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR + 5).await;
        assert_eq!(report.entry_count, 5, "{report:?}");
        assert_eq!(report.parts_reused, 4, "{report:?}");
        assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
        assert_eq!(report.key_index_ranged_gets, 0, "{report:?}");
        assert_eq!(
            data_object_ranged_gets(&store),
            Vec::<(String, GetRange)>::new()
        );
        let whole: usize = store
            .gets
            .lock()
            .expect("lock")
            .iter()
            .filter(|(key, range, _)| key.ends_with(".rseg") && matches!(range, GetRange::Full))
            .count();
        assert_eq!(
            whole, 5,
            "the reconciled postings build fetches every entry"
        );
        assert_eq!(
            take_held_label_dict_log_for_test(),
            HeldLabelDictLog {
                held_ordinals: vec![4],
                peak_pairs: 9,
                pairs_after_release: vec![0],
            }
        );

        let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
        assert_eq!(head.parts.len(), 5);
        let tail = &head.parts[4];
        let ordinal = part_ordinals(store.as_ref(), tail).await;
        assert_eq!(
            probe_label(&catalog, tail, "east").await,
            matches(&[(ordinal[&6], &[0])], &[])
        );
    }

    /// With `key_index_build_max_pairs` 20, the postings build holds the
    /// first two objects' LABEL_DICT pairs (18) and no more, since a third
    /// would take the held total to 27; the other four are read by range in
    /// every pass. The held total never passes the budget, drops to zero once
    /// the part's leaf is built, and the leaf answers exactly. Holding every
    /// fetched object held all six, 54 pairs.
    #[tokio::test]
    async fn held_label_dict_pairs_stay_within_key_index_build_max_pairs() {
        let store = recording_store();
        let records = publish_label_fixture(store.as_ref()).await;
        let cfg = CatalogConfig {
            key_index_build_max_pairs: 20,
            metrics_label_index_max_ratio_ppm: 1_000_000,
            ..metrics_kidx_config()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");
        store.gets.lock().expect("lock").clear();
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        assert_eq!(report.entry_count, 6, "{report:?}");
        assert_eq!(report.key_index_uncovered_entries, 0, "{report:?}");
        assert!(report.key_index_leaves_written > 1, "{report:?}");
        assert!(report.key_index_build_passes > 1, "{report:?}");
        assert_eq!(
            take_held_label_dict_log_for_test(),
            HeldLabelDictLog {
                held_ordinals: vec![0, 1],
                peak_pairs: 18,
                pairs_after_release: vec![0],
            }
        );
        assert_eq!(
            report.key_index_ranged_gets,
            2 * 4 * report.key_index_build_passes,
            "{report:?}"
        );

        let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
        assert_eq!(head.parts.len(), 1);
        let part = &head.parts[0];
        let ordinal = part_ordinals(store.as_ref(), part).await;
        let held = [0u64, 1];
        let ranged_keys: BTreeSet<String> = data_object_ranged_gets(&store)
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        let expected: BTreeSet<String> = records
            .iter()
            .filter(|r| !held.contains(&ordinal[&r.writer_seq]))
            .map(|r| keys::reconstruct_data_key(r).expect("data key"))
            .collect();
        assert_eq!(ranged_keys, expected);
        let east: Vec<(u64, &[u32])> = EAST_WRITERS
            .iter()
            .map(|seq| (ordinal[seq], &[0u32][..]))
            .collect();
        assert_eq!(
            probe_label(&catalog, part, "east").await,
            matches(&east, &[])
        );
        let all: Vec<(u64, &[u32])> = (1..=6u64).map(|seq| (ordinal[&seq], &[0u32][..])).collect();
        assert_eq!(
            probe_label(&catalog, part, "region").await,
            matches(&all, &[])
        );
    }

    /// The byte cap counts every slice of the field, not each on its own:
    /// with `key_index_build_max_pairs` 20 the leaf is built in several
    /// slices, and a cap at least the largest slice but under their sum
    /// leaves the part uncovered. The passes before the one that crosses the
    /// cap already PUT their slices; HEAD names none of them, so they are
    /// unreferenced and left to the catalog sweep.
    #[tokio::test]
    async fn slices_each_under_the_byte_cap_but_over_it_together_are_uncovered() {
        let cfg = CatalogConfig {
            key_index_build_max_pairs: 20,
            metrics_label_index_max_ratio_ppm: 1_000_000,
            ..metrics_kidx_config()
        };
        let uncapped = recording_store();
        let records = publish_label_fixture(uncapped.as_ref()).await;
        let catalog = Catalog::new(uncapped.clone(), cfg).expect("catalog");
        fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        let head = read_signal_head(uncapped.as_ref(), Signal::Metrics).await;
        let sizes: Vec<u64> = head.parts[0].key_index.iter().map(|l| l.size).collect();
        assert!(sizes.len() > 1, "{sizes:?}");
        let largest = sizes.iter().copied().max().unwrap_or(0);
        let total: u64 = sizes.iter().sum();
        let data_bytes: u64 = records.iter().map(|r| r.object_size).sum();
        let ppm = (1..=1_000_000u32)
            .find(|&ppm| metrics_label_index_leaf_bytes_cap(data_bytes, ppm) >= largest)
            .expect("a ratio reaches the largest slice");
        let cap = metrics_label_index_leaf_bytes_cap(data_bytes, ppm);
        assert!(largest <= cap && cap < total, "{sizes:?} cap {cap}");

        let store = recording_store();
        publish_label_fixture(store.as_ref()).await;
        let catalog = Catalog::new(
            store.clone(),
            CatalogConfig {
                metrics_label_index_max_ratio_ppm: ppm,
                ..cfg
            },
        )
        .expect("catalog");
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        assert_eq!(report.entry_count, 6, "{report:?}");
        assert_eq!(report.metrics_label_index_parts_over_cap, 1, "{report:?}");
        assert_eq!(report.key_index_uncovered_entries, 6, "{report:?}");
        let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
        assert_eq!(head.parts.len(), 1);
        assert!(head.parts[0].key_index.is_empty());
        let orphans = kidx_objects(store.as_ref(), Signal::Metrics).await;
        assert!(!orphans.is_empty(), "the earlier passes PUT their slices");
        assert!(orphans.len() < sizes.len(), "{orphans:?}");
    }

    /// An L1 compaction part fetched whole by the postings build is held
    /// under its fold ordinal like an L0 object: the leaf build reads neither
    /// by range, the fold issues one whole-object GET per entry and nothing
    /// else on a data object, and the leaf names the L1 entry exactly.
    #[tokio::test]
    async fn an_l1_metrics_part_is_read_from_the_postings_bytes() {
        let store = recording_store();
        publish_real_compaction(
            &store.inner,
            0,
            KIDX_HOUR,
            i64::from(KIDX_HOUR) * NS_PER_HOUR + 60_000_000_000,
            &[CPU_SECONDS, "mem_bytes"],
        )
        .await;
        publish_labeled_metrics(
            store.as_ref(),
            7,
            KIDX_HOUR,
            &region_series("east"),
            1000,
            false,
        )
        .await;
        let cfg = CatalogConfig {
            metrics_label_index_max_ratio_ppm: 1_000_000,
            ..metrics_kidx_config()
        };
        let catalog = Catalog::new(store.clone(), cfg).expect("catalog");
        store.gets.lock().expect("lock").clear();
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        assert_eq!(report.entry_count, 2, "{report:?}");
        assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
        assert_eq!(report.key_index_uncovered_entries, 0, "{report:?}");
        assert_eq!(report.key_index_ranged_gets, 0, "{report:?}");
        let data_gets: Vec<(String, GetRange)> = store
            .gets
            .lock()
            .expect("lock")
            .iter()
            .filter(|(key, _, _)| key.ends_with(".rseg"))
            .map(|(key, range, _)| (key.clone(), *range))
            .collect();
        assert_eq!(data_gets.len(), 2, "{data_gets:?}");
        assert!(
            data_gets
                .iter()
                .all(|(_, range)| matches!(range, GetRange::Full))
        );
        assert_eq!(
            take_held_label_dict_log_for_test().held_ordinals,
            vec![0, 1]
        );

        let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
        let part = &head.parts[0];
        // The L1 entry carries writer_seq 0, the L0 object 7.
        let ordinal = part_ordinals(store.as_ref(), part).await;
        assert_eq!(
            probe_label(&catalog, part, "mem_bytes").await,
            matches(&[(ordinal[&0], &[0])], &[])
        );
        assert_eq!(
            probe_label(&catalog, part, "east").await,
            matches(&[(ordinal[&7], &[0])], &[])
        );
        assert_eq!(
            probe_label(&catalog, part, CPU_SECONDS).await,
            matches(&[(ordinal[&0], &[0]), (ordinal[&7], &[0])], &[])
        );
    }

    /// The ranged read of one metrics object: exactly a suffix GET for the
    /// footer and one range GET of LABEL_DICT, the range the footer names.
    #[tokio::test]
    async fn a_metrics_label_value_read_is_one_suffix_and_one_range_get() {
        let store = recording_store();
        let record = publish_labeled_metrics(
            store.as_ref(),
            1,
            KIDX_HOUR,
            &region_series("east"),
            1000,
            false,
        )
        .await;
        let key = keys::reconstruct_data_key(&record).expect("data key");
        let object = store
            .get(&key, GetRange::Full)
            .await
            .expect("object")
            .data
            .to_vec();
        let (offset, len) = label_dict_range(&object);
        assert!(record.object_size > KEY_INDEX_FOOTER_SUFFIX_LEN);
        store.gets.lock().expect("lock").clear();
        let (mut gets, mut bytes) = (0u64, 0u64);
        let Ok(fields) = read_metrics_label_value_counted(
            store.as_ref(),
            &tenant(),
            &key,
            record.object_size,
            &mut gets,
            &mut bytes,
        )
        .await
        else {
            panic!("the ranged read fails");
        };
        assert_eq!(
            data_object_ranged_gets(&store),
            vec![
                (key.clone(), GetRange::Suffix(KEY_INDEX_FOOTER_SUFFIX_LEN)),
                (key.clone(), GetRange::Range(offset, offset + len)),
            ]
        );
        assert_eq!(gets, 2);
        assert_eq!(bytes, KEY_INDEX_FOOTER_SUFFIX_LEN + len);
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, METRICS_LABEL_VALUE_FIELD);
        assert!(fields[0].entries.contains(&(key8_str("east"), 0)));
        assert!(fields[0].entries.iter().all(|&(_, block)| block == 0));
        assert_eq!(
            Ok(fields.clone()),
            metrics_label_value_from_object(
                &tenant(),
                &object,
                &match ravel_segment::open_from_suffix(
                    &object,
                    object.len() as u64,
                    ReaderLimits::default()
                )
                .expect("footer")
                {
                    ravel_segment::FooterOutcome::Ready(location) => location,
                    ravel_segment::FooterOutcome::NeedRange { .. } => panic!("whole object"),
                }
            ),
            "the held read and the ranged read agree"
        );
    }

    /// A fold into the next hour with the metrics leaf newly on: the three
    /// entries the earlier fold already decoded for postings are merged
    /// forward, not fetched, so the re-encoded tail reads them by range, a
    /// suffix GET and one LABEL_DICT range GET each; the new hour's object,
    /// fetched whole for postings, costs no GET.
    #[tokio::test]
    async fn entries_not_held_are_read_by_range_and_held_ones_cost_no_get() {
        let store = recording_store();
        let mut records = Vec::new();
        for seq in [10u64, 20, 30] {
            records.push(
                publish_labeled_metrics(
                    store.as_ref(),
                    seq,
                    KIDX_HOUR,
                    &region_series("west"),
                    1000,
                    false,
                )
                .await,
            );
        }
        let off = Catalog::new(
            store.clone(),
            CatalogConfig {
                metrics_label_index: false,
                ..metrics_kidx_config()
            },
        )
        .expect("catalog");
        fold_through(&off, Signal::Metrics, KIDX_HOUR).await;
        let late = publish_labeled_metrics(
            store.as_ref(),
            15,
            KIDX_HOUR + 1,
            &region_series("east"),
            1000,
            false,
        )
        .await;
        store.gets.lock().expect("lock").clear();
        let on = Catalog::new(store.clone(), metrics_kidx_config()).expect("catalog");
        let report = fold_through(&on, Signal::Metrics, KIDX_HOUR + 1).await;
        assert_eq!(report.entry_count, 4, "{report:?}");
        assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
        assert_eq!(report.key_index_ranged_gets, 2 * 3, "{report:?}");

        let gets = data_object_ranged_gets(&store);
        let late_key = keys::reconstruct_data_key(&late).expect("data key");
        assert!(gets.iter().all(|(key, _)| *key != late_key), "{gets:?}");
        for record in &records {
            let key = keys::reconstruct_data_key(record).expect("data key");
            let object = store
                .inner
                .get(&key, GetRange::Full)
                .await
                .expect("object")
                .data
                .to_vec();
            let (offset, len) = label_dict_range(&object);
            let mine: Vec<GetRange> = gets
                .iter()
                .filter(|(k, _)| *k == key)
                .map(|(_, range)| *range)
                .collect();
            assert_eq!(
                mine,
                vec![
                    GetRange::Suffix(KEY_INDEX_FOOTER_SUFFIX_LEN),
                    GetRange::Range(offset, offset + len)
                ],
                "{key}"
            );
        }

        let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
        assert_eq!(head.parts.len(), 1);
        let ordinal = part_ordinals(store.as_ref(), &head.parts[0]).await;
        assert_eq!(
            probe_label(&on, &head.parts[0], "east").await,
            matches(&[(ordinal[&15], &[0])], &[])
        );
    }

    /// An advancing fold with the metrics leaf on carries the previous tail's
    /// `label_value` pairs through its leaf, with no LABEL_DICT read for the
    /// carried entries, and writes the leaf a single fold over the same
    /// objects writes, byte for byte.
    #[tokio::test]
    async fn an_advancing_metrics_fold_carries_the_previous_leaf() {
        async fn publish_early(store: &dyn ObjectStoreBackend) {
            for seq in [10u64, 20, 30] {
                publish_labeled_metrics(store, seq, KIDX_HOUR, &region_series("west"), 1000, false)
                    .await;
            }
        }
        async fn publish_late(store: &dyn ObjectStoreBackend) {
            publish_labeled_metrics(
                store,
                15,
                KIDX_HOUR + 1,
                &region_series("east"),
                1000,
                false,
            )
            .await;
        }

        let advancing = recording_store();
        let catalog = Catalog::new(advancing.clone(), metrics_kidx_config()).expect("catalog");
        publish_early(advancing.as_ref()).await;
        fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        publish_late(advancing.as_ref()).await;
        advancing.gets.lock().expect("lock").clear();
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR + 1).await;
        assert_eq!(report.entry_count, 4, "{report:?}");
        assert_eq!(report.key_index_entries_carried, 3, "{report:?}");
        assert_eq!(report.key_index_ranged_gets, 0, "{report:?}");
        assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
        assert_eq!(data_object_ranged_gets(&advancing), Vec::new());

        let single = recording_store();
        publish_early(single.as_ref()).await;
        publish_late(single.as_ref()).await;
        let once = Catalog::new(single.clone(), metrics_kidx_config()).expect("catalog");
        fold_through(&once, Signal::Metrics, KIDX_HOUR + 1).await;

        let a = read_signal_head(advancing.as_ref(), Signal::Metrics).await;
        let b = read_signal_head(single.as_ref(), Signal::Metrics).await;
        assert_eq!(a.parts.len(), 1);
        assert_eq!(a.parts[0].blake3, b.parts[0].blake3);
        assert_eq!(a.parts[0].key_index, b.parts[0].key_index);
        let ordinal = part_ordinals(advancing.as_ref(), &a.parts[0]).await;
        assert_eq!(
            probe_label(&catalog, &a.parts[0], "west").await,
            matches(
                &[
                    (ordinal[&10], &[0]),
                    (ordinal[&20], &[0]),
                    (ordinal[&30], &[0])
                ],
                &[]
            )
        );
    }

    /// An object whose stored LABEL_DICT fails its checksum is uncovered by
    /// the part's `label_value` leaf, which is still written for the other
    /// objects, and the fold succeeds.
    #[tokio::test]
    async fn a_corrupt_label_dict_leaves_the_entry_uncovered_and_the_fold_succeeds() {
        let store = recording_store();
        for (seq, corrupt) in [(1u64, false), (2, true), (3, false)] {
            publish_labeled_metrics(
                store.as_ref(),
                seq,
                KIDX_HOUR,
                &region_series("west"),
                1000,
                corrupt,
            )
            .await;
        }
        let catalog = Catalog::new(store.clone(), metrics_kidx_config()).expect("catalog");
        let report = fold_through(&catalog, Signal::Metrics, KIDX_HOUR).await;
        assert_eq!(report.entry_count, 3, "{report:?}");
        assert_eq!(report.key_index_leaves_written, 1, "{report:?}");
        assert_eq!(report.key_index_uncovered_entries, 1, "{report:?}");
        let head = read_signal_head(store.as_ref(), Signal::Metrics).await;
        let part = &head.parts[0];
        let ordinal = part_ordinals(store.as_ref(), part).await;
        assert_eq!(
            probe_label(&catalog, part, "west").await,
            matches(&[(ordinal[&1], &[0]), (ordinal[&3], &[0])], &[ordinal[&2]])
        );
    }
}
