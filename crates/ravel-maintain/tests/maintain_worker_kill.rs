//! Process-kill coverage for leased maintenance (ADR-0065): a maintain worker
//! is killed mid-compaction while a sibling runs, and the sibling must take the
//! dead worker's unit over within the liveness bound, complete the interrupted
//! compaction under the conservation gate, and leave no partial output visible.
//!
//! This is the in-process, deterministic analogue of ADR-0077 section 4's
//! scenario 2 (`scripts/chaos/kill-maintain-worker.sh`, which needs real MinIO
//! and a real `kill -9`). The takeover math (`ravel_fleet::worker_set`) was
//! previously exercised only as a pure function; here it decides ownership over
//! a real `compact_bucket` that a `FaultStore` fault interrupted, and the
//! survivor completes that same interrupted compaction. What is new over the
//! pure-function test is the real interrupted-then-completed compaction, not
//! the supervisor loop: ownership is still computed by calling `owner` against
//! the live set, and the survivor's completion is a direct second
//! `compact_bucket` call rather than its supervisor discovering and claiming
//! the unit. The chaos scenario covers that loop.
//!
//! Determinism, per the repo testing conventions:
//!   * `MemoryStore` is the durable store, with its injectable clock left at 0
//!     so every heartbeat object's modification time is 0 and liveness is a
//!     pure function of the `now_ns` each `live_set` call is given.
//!   * `FaultStore` injects the kill (a record-PUT timeout) and its counter is
//!     asserted, so the test proves the fault fired.
//!   * `FixedClock` is the compactor's injected clock; no wall-clock read, no
//!     sleep. The `3 * H` liveness window and the maintenance tick are ADR
//!     constants, evaluated as integers rather than waited out.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::{
    InputSpec, bucket, expected_samples, fetch_compaction_record, raw_series, read_record_samples,
    sealed_now_ns, seed_input,
};
use ravel_commit::keys;
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, WorkerSet, compact_bucket, owner, owns,
    unit_key,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use uuid::Uuid;

/// ADR-0065 decision 1 heartbeat interval `H` (60s), in nanoseconds.
const H_NS: i64 = 60 * 1_000_000_000;
/// The liveness window: a sibling whose heartbeat is older than `3 * H` at read
/// time is gone and its units are taken over (ADR-0065 decision 1, the same
/// `LIVENESS_FACTOR * H` `WorkerSet::with_defaults` uses).
const WINDOW_NS: i64 = 3 * H_NS;
/// The maintenance supervisor tick (ADR-0048/0065), 5 min: the survivor needs
/// one discovery cycle after membership changes to pick up the newly-owned
/// unit. The ADR-0077 takeover bound is `3 * H + one tick`.
const TICK_NS: i64 = 300 * 1_000_000_000;
/// The pinned ADR-0077 section 4 takeover bound.
const TAKEOVER_BOUND_NS: i64 = WINDOW_NS + TICK_NS;

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

/// Two compactable L0 metrics inputs whose union is the compaction's expected
/// output (mirrors the crash-matrix fixture).
fn two_inputs() -> Vec<InputSpec> {
    vec![
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![
                raw_series("m", &[("k", "a")], &[(1_000, 1.0), (2_000, 2.0)]),
                raw_series("m", &[("k", "b")], &[(1_000, 5.0)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("m", &[("k", "a")], &[(3_000, 3.0)])],
        ),
    ]
}

/// Whether the bucket has a published compaction record.
async fn compaction_record_present(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> bool {
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    list_all(store, &prefix).await.unwrap().iter().any(|m| {
        matches!(
            keys::partition_bucket_entry(&m.key),
            Ok(keys::BucketEntry::CompactionRecord(_))
        )
    })
}

/// Count the L1 part objects on the store, whatever record (if any) references
/// them. Used to prove the killed worker left partial output that is present
/// but unreferenced.
async fn l1_part_count(store: &dyn ObjectStoreBackend) -> usize {
    list_all(store, "")
        .await
        .unwrap()
        .into_iter()
        .filter(|m| keys::parse_l1_part_key(&m.key).is_ok())
        .count()
}

/// The scenario end to end: kill the owning worker mid-compaction, the sibling
/// takes over within `3 * H` + one tick, completes the compaction under the
/// conservation gate, and no partial output is ever visible.
#[tokio::test]
async fn maintain_worker_killed_mid_compaction_sibling_takes_over() {
    let inner = MemoryStore::new();

    // Seed two compactable L0 inputs so the compaction has real work.
    let specs = two_inputs();
    for s in &specs {
        seed_input(&inner, s).await;
    }
    let bucket = bucket();
    let unit = unit_key(&bucket.tenant_hash, bucket.signal, bucket.shard);

    // Two maintain-role workers under leased maintenance. Pin their ids and let
    // the rendezvous hash pick which one owns the unit; the owner is the worker
    // we kill, so the survivor is the one that must take over.
    let id1 = Uuid::from_u128(0x1111_1111_1111_1111);
    let id2 = Uuid::from_u128(0x2222_2222_2222_2222);
    let initial_owner = owner(&unit, &[id1, id2]).expect("non-empty live set has an owner");
    let (dead_id, survivor_id) = if initial_owner == id1 {
        (id1, id2)
    } else {
        (id2, id1)
    };
    let dead = WorkerSet::with_defaults(0).with_process_id(dead_id);
    let survivor = WorkerSet::with_defaults(0).with_process_id(survivor_id);

    // t = 0: both workers heartbeat and are live; the doomed worker owns the
    // unit. The store clock is 0, so both heartbeat objects carry mtime 0 and
    // staleness is decided purely by the `now_ns` each read below supplies.
    dead.write_heartbeat(&inner, 0)
        .await
        .expect("dead heartbeat");
    survivor
        .write_heartbeat(&inner, 0)
        .await
        .expect("survivor heartbeat");
    let live0 = survivor.live_set(&inner, 0).await.expect("live set at t0");
    assert!(
        live0.contains(&dead_id) && live0.contains(&survivor_id),
        "both workers live at t0"
    );
    assert_eq!(
        owner(&unit, &live0),
        Some(dead_id),
        "the worker we kill owns the unit at t0"
    );

    // The owner begins compacting and is SIGKILLed mid-publish: a timeout on the
    // record PUT models death after the L1 parts are written but before the
    // compaction record is published (the record filename is `l1.<hash>.cmt`;
    // part keys never contain the literal "l1.").
    let store = FaultStore::new(
        inner,
        FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Timeout)
                .with_key_contains("l1.")
                .with_occurrence(Occurrence::Nth(1)),
        ),
    );
    let clock = FixedClock::new(sealed_now_ns());

    let killed = compact_bucket(&store, &clock, &cfg(), &bucket).await;
    assert!(
        killed.is_err(),
        "the record-PUT fault kills the owner mid-publish"
    );
    assert_eq!(store.fault_count(Op::Put, FaultKind::Timeout), 1);

    // No partial compaction output is visible: the killed worker did write L1
    // parts, but no compaction record references them, so a reader sees nothing.
    assert!(
        l1_part_count(&store).await >= 1,
        "the killed worker left partial L1 parts on the store"
    );
    assert!(
        !compaction_record_present(&store, &bucket).await,
        "no compaction record was published: the partial output is invisible"
    );

    // The dead worker stops heartbeating. At exactly `3 * H` it is still live
    // (the window is inclusive), so membership has NOT changed and the survivor
    // has not taken over: the takeover is bounded, not immediate.
    let live_at_edge = survivor
        .live_set(&store, WINDOW_NS)
        .await
        .expect("live set at exactly 3*H");
    assert!(
        live_at_edge.contains(&dead_id),
        "at exactly 3*H the dead worker is still within the liveness window"
    );
    assert_eq!(
        owner(&unit, &live_at_edge),
        Some(dead_id),
        "no premature takeover: the unit's owner is unchanged at the window edge"
    );

    // One nanosecond past the window the dead worker drops out of the live set
    // and the survivor owns the unit. This is the takeover, and it lands within
    // the ADR-0077 `3 * H + one tick` bound.
    let takeover_ns = WINDOW_NS + 1;
    assert!(
        takeover_ns <= TAKEOVER_BOUND_NS,
        "takeover lands within 3*H + one maintenance tick"
    );
    let live_after = survivor
        .live_set(&store, takeover_ns)
        .await
        .expect("live set past 3*H");
    assert!(
        !live_after.contains(&dead_id),
        "the dead worker is excluded from the live set past 3*H"
    );
    assert_eq!(
        owner(&unit, &live_after),
        Some(survivor_id),
        "the survivor takes the dead worker's unit over"
    );
    assert!(
        owns(&unit, survivor_id, &live_after),
        "the survivor owns the unit after takeover: no unit stays orphaned"
    );

    // The survivor runs the compaction to completion on its next tick, reusing
    // the killed worker's content-addressed parts via CreateIfAbsent. This is
    // the "+ one tick" of the bound.
    let completed = compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("the survivor completes the interrupted compaction");
    assert!(matches!(completed, CompactionOutcome::Compacted { .. }));
    assert!(
        compaction_record_present(&store, &bucket).await,
        "the survivor published the compaction record"
    );

    // Conservation holds: the published output is exactly the input union, so
    // the merge dropped or invented no records. The ADR-0048 conservation gate
    // is what enforces this at publish time; a violation would have returned an
    // error from `compact_bucket` above rather than a `Compacted` outcome.
    let record = fetch_compaction_record(&store, &bucket).await;
    assert_eq!(
        read_record_samples(&store, &record).await,
        expected_samples(&specs),
        "conservation: the compacted output equals the full input union"
    );
}
