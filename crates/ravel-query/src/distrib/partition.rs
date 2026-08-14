//! Snapshot partitioning and the cost gate for the ADR-0071 distributed read
//! fan-out.
//!
//! The coordinator resolves ONE pinned snapshot, then this module decides
//! whether the query is expensive enough to distribute at all (the cost gate)
//! and, if so, splits the snapshot's segments into slices — one dispatched unit
//! of work each.
//!
//! # Shard-major, total partitioning
//!
//! Slices are cut shard-major: a segment's ingest shard is the primary grouping
//! key, and a shard's segments are never split across two slices. This keeps a
//! worker's fetch local to a contiguous shard range, matching how ingest and
//! compaction already group by shard. When a snapshot spans more shards than
//! `max_parallel_slices`, whole shard groups are packed together so the slice
//! count never exceeds the cap; no shard is ever split to hit a target size.
//!
//! Partitioning is **total**: every segment in the snapshot lands in exactly
//! one slice, and no slice is empty. Because the coordinator's k-way merge
//! (`merge_soa_runs`) is order-insensitive over the flat pool of decoded runs
//! (it groups by series id and re-sorts under the ADR-0010 total order), the
//! specific shard-to-slice assignment never changes the merged result — only
//! how the fetch work is spread. The acceptance test
//! (`distributed_merge_equals_local_bitwise`) proves that invariance directly.

use std::collections::BTreeMap;

use ravel_catalog::{SegmentRef, Snapshot};
use ravel_types::accounting::CostEstimate;

/// Estimated store bytes at or above which a query is worth distributing
/// (256 MiB). A query cheaper than this on both axes runs fully locally,
/// exactly as it did before this module existed. ADR-0074's measured
/// crossover (16-worker byte win around ~36 MiB estimated store) confirmed
/// keeping this conservative: `should_distribute` cannot see the worker
/// count, and at 1 worker the same corpus is ~2.5x slower distributed, so no
/// single byte threshold in the 36-256 MiB band is correct. A
/// worker-count-aware gate is future work.
pub const DISTRIBUTE_MIN_STORE_BYTES: u64 = 256 * 1024 * 1024;

/// Segment count at or above which a query is worth distributing. Either axis
/// alone trips the gate. ADR-0074 raised this from ADR-0071's estimated 64 to
/// the measured value: on the reference host distributed p95 first beat local
/// p95 at 256 tiny segments (75 on the byte axis); per ADR-0074's policy of
/// taking the conservative (higher) measured crossover per axis, 256 is the
/// default. The old 64 triggered a case measured ~25% slower distributed even
/// at 16 workers.
pub const DISTRIBUTE_MIN_SEGMENTS: u64 = 256;

/// Default ceiling on concurrently dispatched slices. Bounds fan-out width so a
/// wide snapshot does not spawn an unbounded number of remote fetches.
pub const DEFAULT_MAX_PARALLEL_SLICES: usize = 8;

/// The cost gate and fan-out width for distribution. Held on the engine's
/// optional distributed context; when that context is absent the local path is
/// untouched (ADR-0071: off by default).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DistribThresholds {
    /// Distribute when the estimated store bytes reach this bound.
    pub min_store_bytes: u64,
    /// Distribute when the segment count reaches this bound.
    pub min_segments: u64,
    /// Never cut more than this many slices; must be at least 1.
    pub max_parallel_slices: usize,
}

impl Default for DistribThresholds {
    fn default() -> Self {
        DistribThresholds {
            min_store_bytes: DISTRIBUTE_MIN_STORE_BYTES,
            min_segments: DISTRIBUTE_MIN_SEGMENTS,
            max_parallel_slices: DEFAULT_MAX_PARALLEL_SLICES,
        }
    }
}

/// Whether a query with the given pre-fetch [`CostEstimate`] should be
/// distributed. Either axis at or above its threshold trips the gate; a query
/// below both stays fully local. Reads the same estimate the engine already
/// computes per query (ADR-0044), so the gate needs no extra pass over the
/// snapshot.
pub fn should_distribute(thresholds: &DistribThresholds, cost: &CostEstimate) -> bool {
    cost.estimated_store_bytes >= thresholds.min_store_bytes
        || cost.segments >= thresholds.min_segments
}

/// One dispatched unit of work: the segments a single worker fetches. Holds
/// owned [`SegmentRef`] clones so the slice outlives the borrow of the pinned
/// snapshot it was cut from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slice {
    pub segments: Vec<SegmentRef>,
}

/// Splits a resolved snapshot into at most `max_parallel_slices` shard-major
/// slices. Every segment lands in exactly one non-empty slice; a shard's
/// segments are never split across slices.
///
/// Returns an empty vec for an empty snapshot (no work to dispatch). A
/// `max_parallel_slices` of 0 is treated as 1, so a caller that misconfigures
/// it still gets a single valid slice rather than a panic or a dropped segment.
pub fn partition_snapshot(snapshot: &Snapshot, max_parallel_slices: usize) -> Vec<Slice> {
    if snapshot.segments.is_empty() {
        return Vec::new();
    }
    let cap = max_parallel_slices.max(1);

    // Group segment indices by shard, deterministically ordered by shard id.
    // A BTreeMap keeps the shard-to-slice assignment stable across runs so the
    // partition is a pure function of the snapshot (the acceptance test relies
    // on nothing here being nondeterministic).
    let mut by_shard: BTreeMap<u32, Vec<usize>> = BTreeMap::new();
    for (idx, seg) in snapshot.segments.iter().enumerate() {
        by_shard.entry(seg.shard).or_default().push(idx);
    }

    let shard_groups: Vec<Vec<usize>> = by_shard.into_values().collect();
    let slice_count = shard_groups.len().min(cap);

    // Pack whole shard groups into `slice_count` slices, contiguously: shard
    // groups 0..k go to slice 0, and so on, so each slice owns a contiguous
    // shard range. `chunk_len` rounds up so the last slice is never left empty
    // and no group is dropped.
    let groups = shard_groups.len();
    let chunk_len = groups.div_ceil(slice_count);

    let mut slices: Vec<Slice> = Vec::with_capacity(slice_count);
    for chunk in shard_groups.chunks(chunk_len) {
        let mut segments = Vec::new();
        for group in chunk {
            for &idx in group {
                segments.push(snapshot.segments[idx].clone());
            }
        }
        if !segments.is_empty() {
            slices.push(Slice { segments });
        }
    }
    slices
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_catalog::SegmentLevel;
    use uuid::Uuid;

    fn seg(shard: u32, seq: u64) -> SegmentRef {
        SegmentRef {
            data_object_key: format!("k/{shard}/{seq}"),
            object_size: 1,
            min_event_ts_ns: 0,
            max_event_ts_ns: 0,
            ingest_hour_bucket: 0,
            sample_count: 0,
            series_count: 0,
            shard,
            content_hash: [0u8; 32],
            writer_id: Uuid::nil(),
            writer_epoch: 0,
            writer_seq: seq,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
        }
    }

    fn snapshot(segs: Vec<SegmentRef>) -> Snapshot {
        Snapshot {
            segments: segs,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        }
    }

    /// The multiset of object keys across all slices equals the snapshot's,
    /// with no key appearing twice: partitioning is total and disjoint.
    fn assert_total_and_disjoint(snapshot: &Snapshot, slices: &[Slice]) {
        let mut got: Vec<&str> = slices
            .iter()
            .flat_map(|s| s.segments.iter().map(|seg| seg.data_object_key.as_str()))
            .collect();
        let mut want: Vec<&str> = snapshot
            .segments
            .iter()
            .map(|seg| seg.data_object_key.as_str())
            .collect();
        got.sort_unstable();
        want.sort_unstable();
        assert_eq!(
            got, want,
            "every segment appears exactly once across slices"
        );
    }

    #[test]
    fn empty_snapshot_yields_no_slices() {
        assert!(partition_snapshot(&snapshot(vec![]), 8).is_empty());
    }

    #[test]
    fn partition_is_total_and_disjoint_and_bounded() {
        let segs = vec![
            seg(0, 0),
            seg(0, 1),
            seg(1, 0),
            seg(2, 0),
            seg(2, 1),
            seg(3, 0),
        ];
        let snap = snapshot(segs);
        for cap in 1..=10 {
            let slices = partition_snapshot(&snap, cap);
            assert!(!slices.is_empty());
            assert!(slices.len() <= cap.max(1), "never exceeds the cap");
            assert!(
                slices.iter().all(|s| !s.segments.is_empty()),
                "no empty slice"
            );
            assert_total_and_disjoint(&snap, &slices);
        }
    }

    #[test]
    fn a_shard_is_never_split_across_slices() {
        // Four shards, cap 2: each slice must hold whole shards only.
        let snap = snapshot(vec![seg(0, 0), seg(1, 0), seg(1, 1), seg(2, 0), seg(3, 0)]);
        let slices = partition_snapshot(&snap, 2);
        assert!(slices.len() <= 2);
        for slice in &slices {
            // Every segment in a slice whose shard appears must have ALL of
            // that shard's segments in the same slice.
            for seg in &slice.segments {
                let in_slice = slice
                    .segments
                    .iter()
                    .filter(|s| s.shard == seg.shard)
                    .count();
                let in_snapshot = snap
                    .segments
                    .iter()
                    .filter(|s| s.shard == seg.shard)
                    .count();
                assert_eq!(
                    in_slice, in_snapshot,
                    "shard {} split across slices",
                    seg.shard
                );
            }
        }
    }

    #[test]
    fn zero_cap_is_treated_as_one_slice() {
        let snap = snapshot(vec![seg(0, 0), seg(1, 0)]);
        let slices = partition_snapshot(&snap, 0);
        assert_eq!(slices.len(), 1);
        assert_total_and_disjoint(&snap, &slices);
    }

    #[test]
    fn cost_gate_trips_on_either_axis() {
        let th = DistribThresholds::default();
        // Below both: local.
        assert!(!should_distribute(&th, &CostEstimate::new(0, 0, 0, 1, 1)));
        // Segment axis.
        assert!(should_distribute(
            &th,
            &CostEstimate::new(0, 0, 0, DISTRIBUTE_MIN_SEGMENTS, 1)
        ));
        // Byte axis.
        assert!(should_distribute(
            &th,
            &CostEstimate::new(0, DISTRIBUTE_MIN_STORE_BYTES, 0, 1, 1)
        ));
        // Just under the segment bound stays local.
        assert!(!should_distribute(
            &th,
            &CostEstimate::new(
                0,
                DISTRIBUTE_MIN_STORE_BYTES - 1,
                0,
                DISTRIBUTE_MIN_SEGMENTS - 1,
                1
            )
        ));
    }
}
