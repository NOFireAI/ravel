//! Resolved snapshot types (docs/catalog-and-mvcc.md "Snapshot resolution",
//! "MVCC rules").

use ravel_proto::commit::v1::ErasureRequest;
use uuid::Uuid;

/// Which storage level a [`SegmentRef`] names
/// (docs/catalog-and-mvcc.md "Snapshot resolution").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentLevel {
    /// A single flushed L0 segment referenced by its own commit record.
    /// The `SegmentRef`'s `writer_id`/`writer_epoch`/`writer_seq` are the
    /// flush's real writer identity and are verified against the segment
    /// footer (ADR-0010 §7).
    L0,
    /// One RSEG v4 part of a compacted (L1) bucket.
    /// Carries the parent
    /// compaction record's `input_set_hash` and this part's `part_index`:
    /// the two identity fields (beyond the shared shard/hour/tenant already
    /// on the `SegmentRef`) a reader needs to reconstruct the part key and
    /// verify the v4 footer against the record. A part has no writer
    /// identity of its own, so the `SegmentRef`'s `writer_*` fields are not
    /// meaningful for an L1 ref and are never used for identity or dedup.
    L1 {
        input_set_hash: [u8; 32],
        part_index: u32,
    },
}

/// One immutable segment reference in a resolved snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRef {
    /// Data-object key, reconstructed from identity fields, never a stored
    /// key string (ADR-0010 §7). For L0 this is
    /// `ravel_commit::keys::verify_object_key` over the commit record; for
    /// L1 it is `ravel_commit::keys::reconstruct_l1_part_key` over the
    /// compaction record and this part.
    pub data_object_key: String,
    pub object_size: u64,
    pub min_event_ts_ns: i64,
    pub max_event_ts_ns: i64,
    /// Ingest-hour bucket pinned at flush open (unix hours). For an L1 part
    /// this is the compacted bucket's hour.
    pub ingest_hour_bucket: u32,
    pub sample_count: u64,
    pub series_count: u64,
    pub shard: u32,
    /// For L0, the commit record's segment content hash; for L1, the part
    /// object's own blake3.
    pub content_hash: [u8; 32],
    /// L0: the flush's writer id. L1: not meaningful (a part has no writer
    /// identity); set to [`Uuid::nil`] and never used for identity/dedup.
    pub writer_id: Uuid,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    /// Wall-clock a record was created: the commit record for L0, the
    /// compaction record for L1. Used only for cross-segment
    /// duplicate-sample ordering (docs/catalog-and-mvcc.md "Cross-segment
    /// duplicate samples": `(created_unix_ns, writer_epoch, writer_seq,
    /// in-page index)`, greatest wins); never for pruning.
    pub created_unix_ns: i64,
    /// L0 vs L1 discriminator (docs/catalog-and-mvcc.md "Snapshot
    /// resolution"). Determines how a reader verifies the segment footer
    /// and how the ref sorts into the mixed-level snapshot order.
    pub level: SegmentLevel,
}

/// A pinned, immutable set of segments for one `resolve` call (MVCC).
/// Later commits or deletions never affect an already-returned snapshot:
/// this type owns its data and holds no reference back to the store or the
/// catalog's cache.
///
/// Segments are sorted by the cross-segment dedup provenance order named in
/// docs/catalog-and-mvcc.md (`created_unix_ns`, `writer_epoch`,
/// `writer_seq`), with `shard` then `writer_id` as final tiebreaks. The
/// `writer_id` tiebreak makes the key a total order over distinct segments,
/// so iteration order is deterministic even when two same-shard segments from
/// different writers tie on every provenance component.
///
/// The order stays a deterministic total order across mixed L0/L1 levels
/// (docs/catalog-and-mvcc.md "Snapshot resolution"): an L1 part uses its
/// compaction record's `created_unix_ns` in the provenance position and,
/// since it has no writer identity, `input_set_hash` then `part_index` as
/// its final tiebreaks in place of `writer_id`/`writer_epoch`/`writer_seq`.
///
/// `Eq` is deliberately not derived: [`Snapshot::pending_erasure`] holds
/// `ErasureRequest` protobuf messages, which prost derives `PartialEq` but
/// not `Eq` for. `PartialEq` is all any caller needs (assertions, dedup
/// checks); nothing keys a `HashSet`/`BTreeMap` on a whole `Snapshot`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Snapshot {
    pub segments: Vec<SegmentRef>,
    /// Snapshot-sourced segments excluded by postings-based pruning.
    /// Always 0 when the caller used
    /// [`Catalog::resolve`](crate::Catalog::resolve) or when
    /// [`Catalog::resolve_pruned`](crate::Catalog::resolve_pruned) found no
    /// name filter, no usable postings, or a listing/token fallback for the
    /// whole window; never counts listing- or `min_token`-sourced segments,
    /// which are never pruned.
    pub segments_pruned: u64,
    /// Pending selective-erasure predicates for this (tenant, signal),
    /// discovered by one LIST of `t/<th>/<sig>/del/` per resolve (ADR-0064
    /// decision 2). Every durable `.dreq` observed at resolve time is
    /// decoded, its observed key verified against its own identity fields
    /// (ADR-0010 §7), and attached here. Empty for the common case of a
    /// (tenant, signal) with no pending erasure.
    ///
    /// This task only *discovers and attaches* the predicates. The scan /
    /// materialization layer is what filters matching series, rows,
    /// and spans out of query results against these predicates; nothing here
    /// filters. The visibility bound (ADR-0064 decision 2) is delivered by
    /// this attachment being unconditional: a `.dreq` durable before a
    /// resolve is always seen by that resolve, before any physical rewrite
    /// has run.
    pub pending_erasure: Vec<ErasureRequest>,
}

/// Where one resolved segment came from (ADR-0073 decision 1). Kept as a
/// sibling of [`Snapshot`]/[`SegmentRef`] rather than a new field on either:
/// both types are constructed by full field-literal at call sites outside
/// this crate (`ravel-sql`'s executor, providers, and flight ticket), which
/// have no `..` update syntax to absorb an added field, so a new field there
/// would be a breaking change this task's scope (`ravel-catalog`/
/// `ravel-query` only) cannot land. `Snapshot`'s and `SegmentRef`'s shapes
/// are unchanged; this enum is produced alongside a `Snapshot`, not inside
/// it, by [`crate::Catalog::resolve_pruned_with_admission`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentOrigin {
    /// Extracted from a folded snapshot part at or below the fold
    /// watermark; postings-pruned, and the only origin `max_segments`
    /// counts against (ADR-0073 decision 2).
    SealedBelowWatermark,
    /// Listed live above the fold watermark; never postings-pruned, exempt
    /// from `max_segments`.
    Recent,
    /// Resolved from an explicit `min_commit_token` (read-your-write);
    /// exempt from `max_segments`.
    TokenResolved,
}

/// The per-segment origin breakdown for one resolved [`Snapshot`]
/// (ADR-0073 decision 1), in the same order as `Snapshot::segments`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SegmentOrigins {
    /// Parallel to `Snapshot::segments`: `origins[i]` is `segments[i]`'s
    /// origin.
    pub origins: Vec<SegmentOrigin>,
    /// Count of `SegmentOrigin::SealedBelowWatermark` entries: the set
    /// `max_segments` is checked against.
    pub sealed_count: u64,
    /// Count of `Recent` plus `TokenResolved` entries: exempt from
    /// `max_segments` (ADR-0073 decision 2).
    pub exempt_count: u64,
}

impl SegmentOrigins {
    /// Record one more segment's origin, keeping `sealed_count`/
    /// `exempt_count` in sync with `origins`.
    pub fn push(&mut self, origin: SegmentOrigin) {
        match origin {
            SegmentOrigin::SealedBelowWatermark => self.sealed_count += 1,
            SegmentOrigin::Recent | SegmentOrigin::TokenResolved => self.exempt_count += 1,
        }
        self.origins.push(origin);
    }
}
