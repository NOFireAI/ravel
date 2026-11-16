//! The RSPAN merge cursor releases a block's raw fetched charge when that
//! block's decode fails (issue #3817).
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::{SHARD, sealed_now_ns, seed_rspan_input, span_record, spans_bucket};
use ravel_maintain::{
    CompactorConfig, FixedClock, MergeLiveBytes, MergeMemoryTracker, compact_bucket,
};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, ScriptedFault, Sequence};
use ravel_object_store::memory::MemoryStore;
use uuid::Uuid;

/// GETs on this fixture's first input that precede its first block fetch.
/// With fewer, the fault lands before any block is charged, which the
/// `peak_transient_bytes` assertion below catches.
const GETS_BEFORE_FIRST_BLOCK: usize = 2;

/// The first block fetched from one input comes back corrupted, so its decode
/// fails after the raw block was charged as fetched. The run fails and leaves
/// every live term at zero.
///
/// Shown failing on the `live_bytes` assertion against the cursor that
/// released the raw charge only after a successful decode: `fetched` reads
/// the corrupted block's 116 raw bytes.
#[tokio::test]
async fn a_failed_block_decode_releases_its_raw_charge() {
    let inner = MemoryStore::new();
    let a = vec![span_record(0, 0, 10, 20), span_record(1, 0, 5, 9)];
    let b = vec![span_record(0, 1, 15, 18), span_record(2, 0, 1, 2)];
    seed_rspan_input(&inner, Uuid::from_u128(1), 10, 1, &a).await;
    seed_rspan_input(&inner, Uuid::from_u128(2), 10, 2, &b).await;
    let data_key = format!("/l0/{SHARD:04}/{}.", Uuid::from_u128(1));
    let mut seq = Sequence::new(Op::Get).with_key_contains(&data_key);
    for _ in 0..GETS_BEFORE_FIRST_BLOCK {
        seq = seq.then_passthrough();
    }
    let store = FaultStore::new(
        inner,
        FaultPlan::empty().with_sequence(seq.then_fault(ScriptedFault::CorruptRange)),
    );
    let tracker = MergeMemoryTracker::new();
    let config = CompactorConfig {
        merge_memory_tracker: Some(tracker.clone()),
        ..CompactorConfig::default()
    };

    let run = compact_bucket(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &config,
        &spans_bucket(),
    )
    .await;
    assert!(run.is_err(), "the merge fails decoding the block: {run:?}");
    assert_eq!(store.fault_count(Op::Get, FaultKind::CorruptRange), 1);
    assert!(
        tracker.peak_transient_bytes() > 0,
        "the block's raw bytes were charged before the decode failed"
    );
    assert_eq!(tracker.peak_cursor_decoded_bytes(), 0, "no block decoded");

    assert_eq!(tracker.live_bytes(), MergeLiveBytes::default());
}
