//! Resolved snapshot types (docs/catalog-and-mvcc.md "Snapshot resolution",
//! "MVCC rules").

use uuid::Uuid;

/// One immutable segment reference in a resolved snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRef {
    /// Data-object key, reconstructed from the commit record's own identity
    /// fields (never the record's raw `object_key` field verbatim; see
    /// `ravel_commit::keys::verify_object_key`, ADR-0010 §7).
    pub data_object_key: String,
    pub object_size: u64,
    pub min_event_ts_ns: i64,
    pub max_event_ts_ns: i64,
    /// Ingest-hour bucket pinned at flush open (unix hours).
    pub ingest_hour_bucket: u32,
    pub sample_count: u64,
    pub series_count: u64,
    pub shard: u32,
    pub content_hash: [u8; 32],
    pub writer_id: Uuid,
    pub writer_epoch: u64,
    pub writer_seq: u64,
    /// Wall-clock the commit record was created. Used only for
    /// cross-segment duplicate-sample ordering (docs/catalog-and-mvcc.md
    /// "Cross-segment duplicate samples": `(created_unix_ns, writer_epoch,
    /// writer_seq, in-page index)`, greatest wins); never for pruning.
    pub created_unix_ns: i64,
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
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Snapshot {
    pub segments: Vec<SegmentRef>,
}
