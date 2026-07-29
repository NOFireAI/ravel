//! Resolved snapshot types (docs/catalog-and-mvcc.md "Snapshot resolution",
//! "MVCC rules").

use uuid::Uuid;

/// Which storage level a [`SegmentRef`] names
/// (docs/catalog-and-mvcc.md "Snapshot resolution";
/// docs/compaction-retention-plan.md §3.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentLevel {
    /// A single flushed L0 segment referenced by its own commit record.
    /// The `SegmentRef`'s `writer_id`/`writer_epoch`/`writer_seq` are the
    /// flush's real writer identity and are verified against the segment
    /// footer (ADR-0010 §7).
    L0,
    /// One RSEG v4 part of a compacted (L1) bucket
    /// (docs/compaction-retention-plan.md §3.5). Carries the parent
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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Snapshot {
    pub segments: Vec<SegmentRef>,
    /// Snapshot-sourced segments excluded by postings-based pruning
    /// (docs/metric-index-plan.md P5b). Always 0 when the caller used
    /// [`Catalog::resolve`](crate::Catalog::resolve) or when
    /// [`Catalog::resolve_pruned`](crate::Catalog::resolve_pruned) found no
    /// name filter, no usable postings, or a listing/token fallback for the
    /// whole window; never counts listing- or `min_token`-sourced segments,
    /// which are never pruned.
    pub segments_pruned: u64,
}
