//! The RSEG merge releases every merge memory tracker charge on every exit
//! from `build_parts`, not only on success (issue #3817).
//!
//! A charge a failed merge leaves behind stays in the tracker's live terms
//! until the next run's `reset_for_run`, and every figure read from the
//! tracker in between carries it. Each test here drives one exit of the merge
//! with a tracker
//! installed and asserts every live term is zero afterwards
//! ([`MergeMemoryTracker::live_bytes`]), while the high-water figures the run
//! recorded stay readable.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use common::*;
use ravel_fleet::claim::COMPACTION_CLAIMS_PREFIX;
use ravel_maintain::claim_guard::ClaimSleeper;
use ravel_maintain::{
    Checkpoint, ClaimParticipant, ClaimedCompaction, Clock, CompactionOutcome, CompactorConfig,
    Coordination, FixedClock, MergeLiveBytes, MergeMemoryTracker, PublishOutcome, RequestLedger,
    build, compact_bucket, compact_bucket_claimed, read,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault, Sequence,
};
use ravel_object_store::memory::MemoryStore;
use uuid::Uuid;

/// Two inputs that both carry series `m{k="a"}`, so that series merges two
/// runs and is decoded on the read gate rather than copied verbatim. Returns
/// a substring only the first input's data key contains.
async fn seed_overlapping(store: &dyn ravel_object_store::ObjectStoreBackend) -> String {
    seed_input(
        store,
        &InputSpec::new(
            Uuid::from_u128(1),
            1,
            1,
            vec![
                raw_series("m", &[("k", "a")], &[(1_000, 1.0), (2_000, 2.0)]),
                raw_series("m", &[("k", "b")], &[(1_000, 5.0)]),
            ],
        ),
    )
    .await;
    seed_input(
        store,
        &InputSpec::new(
            Uuid::from_u128(2),
            1,
            2,
            vec![raw_series("m", &[("k", "a")], &[(3_000, 3.0)])],
        ),
    )
    .await;
    format!("/l0/{SHARD:04}/{}.", Uuid::from_u128(1))
}

/// A config with `tracker` and `ledger` installed.
fn tracked(tracker: &MergeMemoryTracker, ledger: &RequestLedger) -> CompactorConfig {
    CompactorConfig {
        merge_memory_tracker: Some(tracker.clone()),
        request_ledger: Some(ledger.clone()),
        ..CompactorConfig::default()
    }
}

/// Catalog-read GETs one input of [`seed_overlapping`] costs, measured on a
/// clean merge: both inputs carry the same sections, so it is half the total.
async fn catalog_gets_per_input() -> u64 {
    let store = MemoryStore::new();
    seed_overlapping(&store).await;
    let ledger = RequestLedger::new();
    let config = tracked(&MergeMemoryTracker::new(), &ledger);
    let clock = FixedClock::new(sealed_now_ns());
    compact_bucket(&store, &clock, &config, &bucket())
        .await
        .expect("clean merge");
    let total = ledger.report().catalog_read.requests;
    assert_eq!(total % 2, 0, "{total} catalog GETs over two inputs");
    total / 2
}

/// A GET plan on `key`: its catalog GETs pass and the next GET, the merge's
/// first page fetch from it, gets `fault`.
async fn first_page_get_on(key: &str, fault: ScriptedFault) -> FaultPlan {
    let mut seq = Sequence::new(Op::Get).with_key_contains(key);
    for _ in 0..catalog_gets_per_input().await {
        seq = seq.then_passthrough();
    }
    FaultPlan::empty().with_sequence(seq.then_fault(fault))
}

/// The catalogs were really charged, and so was the index built from them.
fn assert_peaks_recorded(tracker: &MergeMemoryTracker) {
    let peaks = tracker.phase_peaks();
    assert!(peaks.catalog_plan_heap_bytes > 0, "{peaks:?}");
    assert!(peaks.catalog_label_heap_bytes > 0, "{peaks:?}");
    assert!(peaks.plan_index_heap_bytes > 0, "{peaks:?}");
    assert!(tracker.peak_resident_bytes() > 0);
}

/// A page GET that fails with a transient store error mid-merge fails the run
/// and leaves every live term at zero.
///
/// Shown failing on the `live_bytes` assertion against the release-on-`Ok`-only
/// merge: `catalog_plan`, `catalog_labels` and `plan_index` read nonzero.
#[tokio::test]
async fn a_failed_page_get_releases_every_charge() {
    let inner = MemoryStore::new();
    let first = seed_overlapping(&inner).await;
    let store = FaultStore::new(
        inner,
        first_page_get_on(&first, ScriptedFault::Transient("page".into())).await,
    );
    let tracker = MergeMemoryTracker::new();
    let ledger = RequestLedger::new();
    let clock = FixedClock::new(sealed_now_ns());

    let err = compact_bucket(&store, &clock, &tracked(&tracker, &ledger), &bucket()).await;
    assert!(err.is_err(), "the merge fails on the page GET: {err:?}");
    assert_eq!(store.fault_count(Op::Get, FaultKind::Transient), 1);
    let report = ledger.report();
    assert_eq!(
        report.catalog_read.requests,
        2 * catalog_gets_per_input().await,
        "both catalogs loaded before the fault"
    );
    assert!(
        report.block_read.requests >= 1,
        "the fault hit a page fetch"
    );

    assert_eq!(tracker.live_bytes(), MergeLiveBytes::default());
    assert_peaks_recorded(&tracker);
}

/// A page whose bytes come back corrupted fails the series' decode on the read
/// gate, and that exit also leaves every live term at zero.
///
/// Shown failing on the `live_bytes` assertion against the release-on-`Ok`-only
/// merge, and against a merge that releases by hand before every return except
/// the read gate's error: in both, `fetched`, `decoded`, `catalog_plan`,
/// `catalog_labels` and `plan_index` read nonzero. The second is the only one
/// of these tests that merge fails.
#[tokio::test]
async fn a_failed_series_decode_releases_every_charge() {
    let inner = MemoryStore::new();
    let first = seed_overlapping(&inner).await;
    let store = FaultStore::new(
        inner,
        first_page_get_on(&first, ScriptedFault::CorruptRange).await,
    );
    let tracker = MergeMemoryTracker::new();
    let ledger = RequestLedger::new();
    let clock = FixedClock::new(sealed_now_ns());

    let err = compact_bucket(&store, &clock, &tracked(&tracker, &ledger), &bucket()).await;
    assert!(err.is_err(), "the merge fails decoding the page: {err:?}");
    assert_eq!(store.fault_count(Op::Get, FaultKind::CorruptRange), 1);
    let report = ledger.report();
    assert_eq!(
        report.catalog_read.requests,
        2 * catalog_gets_per_input().await
    );
    assert_eq!(
        report.part_put.requests, 0,
        "the run failed before any part was written"
    );

    assert_eq!(tracker.live_bytes(), MergeLiveBytes::default());
    assert_peaks_recorded(&tracker);
    assert!(
        tracker.peak_cursor_decoded_bytes() > 0,
        "the series' decode was charged before it failed"
    );
}

/// A claim lost at the merge-loop checkpoint cancels the run, and leaves
/// every live term at zero.
///
/// The participant's clock advances a renewal cadence per read, so every
/// checkpoint renews: claim PUT 1 is the acquisition, 2 the input-set renewal
/// and 3 the merge-loop renewal, which a scripted `FailedConditionalWrite`
/// rejects the way a steal does. Shown failing against the
/// release-on-`Ok`-only merge: `catalog_plan`, `catalog_labels` and
/// `plan_index` nonzero.
#[tokio::test]
async fn a_claim_lost_at_the_merge_loop_releases_every_charge() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite)
            .with_key_contains(COMPACTION_CLAIMS_PREFIX)
            .with_occurrence(Occurrence::Nth(3)),
    );
    let store = FaultStore::new(MemoryStore::new(), plan);
    seed_overlapping(&store).await;
    let tracker = MergeMemoryTracker::new();
    let ledger = RequestLedger::new();
    let clock = FixedClock::new(sealed_now_ns());
    let lease = Duration::from_secs(3);
    let participant = ClaimParticipant::new(
        Uuid::from_u128(1),
        Arc::new(TickingClock {
            now_ns: AtomicI64::new(sealed_now_ns()),
            step_ns: lease.as_nanos() as i64 / 3,
        }) as Arc<dyn Clock>,
    )
    .with_sleeper(Arc::new(NoWait));
    let config = CompactorConfig {
        coordination: Coordination::On,
        claim_min_input_bytes: 1,
        claim_lease_duration: lease,
        claim_participant: Some(participant),
        ..tracked(&tracker, &ledger)
    };

    let outcome = compact_bucket_claimed(&store, &clock, &config, &bucket())
        .await
        .expect("a lost claim is an outcome");
    match outcome {
        ClaimedCompaction::Cancelled { at, outcome } => {
            assert_eq!(at, Checkpoint::MergeLoop);
            assert_eq!(
                outcome,
                CompactionOutcome::Compacted {
                    parts: 0,
                    publish: PublishOutcome::Abandoned
                }
            );
        }
        other => panic!("expected a cancelled run, got {other:?}"),
    }
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
        1
    );

    assert_eq!(tracker.live_bytes(), MergeLiveBytes::default());
    assert_peaks_recorded(&tracker);
}

/// A direct `build_parts` call over catalogs that went through no
/// `admit_catalog` charges what it releases, so with a tracker installed no
/// term ends below where it started.
///
/// Shown failing against the merge that charged the catalogs in
/// `admit_catalog` and released them in `build_parts`: `catalog_plan` and
/// `catalog_labels` wrap to just under `u64::MAX`.
#[tokio::test]
async fn a_direct_build_parts_call_releases_only_what_it_charged() {
    let store = MemoryStore::new();
    seed_overlapping(&store).await;
    let tracker = MergeMemoryTracker::new();
    let config = CompactorConfig {
        merge_memory_tracker: Some(tracker.clone()),
        ..CompactorConfig::default()
    };
    let b = bucket();
    let listing = read::list_bucket(&store, &b).await.unwrap();
    let inputs = read::load_inputs(&store, &b, &listing.commit_keys, 1)
        .await
        .unwrap();
    let hash = read::input_set_hash(&inputs);
    let mut catalogs = Vec::new();
    for i in &inputs {
        catalogs.push(read::load_input_catalog(&store, &config, i).await.unwrap());
    }

    let parts = build::build_parts(
        &store,
        &config,
        &b,
        &inputs,
        catalogs,
        &HashSet::new(),
        &hash,
    )
    .await
    .unwrap();
    assert_eq!(parts.len(), 1);

    assert_eq!(tracker.live_bytes(), MergeLiveBytes::default());
    assert_peaks_recorded(&tracker);
}

/// A [`ClaimSleeper`] that returns immediately.
struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _duration: Duration) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// A clock that advances `step_ns` on every read.
struct TickingClock {
    now_ns: AtomicI64,
    step_ns: i64,
}

impl Clock for TickingClock {
    fn now_ns(&self) -> i64 {
        self.now_ns.fetch_add(self.step_ns, Ordering::SeqCst) + self.step_ns
    }
}
