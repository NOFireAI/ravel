//! Advisory compaction claims through the real compaction pipeline (ADR-1029
//! decisions 3 to 5).
//!
//! The unit-level protocol (acquire, renew, steal, complete, the renewal
//! cadence, `NotFound` as a lost claim) is pinned in `claim_guard.rs`'s own
//! tests. This file drives `compact_bucket_claimed` over real seeded buckets
//! and asserts what the pipeline does with a claim: the cost gate, a claim lost
//! mid-merge, and the property the whole design rests on -- that a claim
//! confers no publication rights, so an owner that loses its claim and finishes
//! anyway still converges on exactly one record.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use ravel_fleet::claim::{COMPACTION_CLAIMS_PREFIX, ClaimConfig};
use ravel_maintain::claim_guard::{Acquire, ClaimGuard, ClaimSleeper};
use ravel_maintain::{
    Checkpoint, ClaimParticipant, ClaimedCompaction, Clock, CompactionOutcome, CompactorConfig,
    Coordination, FixedClock, PublishOutcome, RequestLedger, compact_bucket_claimed,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_types::Signal;
use uuid::Uuid;

/// A lease long enough that a whole `MemoryStore` merge fits inside a third of
/// it, so a test that does not move the clock itself issues exactly zero
/// renewals.
const LEASE: Duration = Duration::from_secs(3);

/// A participant on `clock` whose acquisition jitter returns at once, so no
/// test in this file waits on the real timer.
fn participant(process: u128, clock: &FixedClock) -> ClaimParticipant {
    ClaimParticipant::new(
        Uuid::from_u128(process),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
    )
    .with_sleeper(Arc::new(NoWait))
}

/// A [`ClaimSleeper`] that returns immediately.
struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _duration: Duration) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// A compactor config that claims as `process`, on `clock`, over any bucket at
/// or above `min_input_bytes`, with `ledger` installed.
fn claiming_config(
    process: u128,
    clock: &FixedClock,
    min_input_bytes: u64,
    ledger: &RequestLedger,
) -> CompactorConfig {
    CompactorConfig {
        coordination: Coordination::On,
        claim_min_input_bytes: min_input_bytes,
        claim_lease_duration: LEASE,
        claim_participant: Some(participant(process, clock)),
        request_ledger: Some(ledger.clone()),
        ..CompactorConfig::default()
    }
}

/// Two metrics L0 inputs: a real, compactable bucket whose stored input bytes
/// are a few KiB, which is far below the 64 MiB shipped cost gate and far above
/// a threshold of 1.
async fn seed_two_metric_inputs(store: &dyn ObjectStoreBackend) {
    seed_input(
        store,
        &InputSpec::new(
            Uuid::from_u128(1),
            1,
            1,
            vec![raw_series("alpha", &[], &[(10, 1.0), (20, 2.0)])],
        ),
    )
    .await;
    seed_input(
        store,
        &InputSpec::new(
            Uuid::from_u128(2),
            1,
            2,
            vec![raw_series("beta", &[], &[(30, 3.0)])],
        ),
    )
    .await;
}

/// Every compaction record key in the metrics bucket.
async fn record_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    record_keys_in(store, &bucket()).await
}

/// Every compaction record key in `b`.
async fn record_keys_in(store: &dyn ObjectStoreBackend, b: &ravel_maintain::Bucket) -> Vec<String> {
    use ravel_commit::keys;
    let prefix =
        keys::commit_shard_hour_prefix(&b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket)
            .expect("prefix");
    let mut out: Vec<String> = list_all(store, &prefix)
        .await
        .expect("list bucket")
        .into_iter()
        .map(|m| m.key)
        .filter(|k| {
            matches!(
                keys::partition_bucket_entry(k),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        })
        .collect();
    out.sort();
    out
}

/// The cost gate (ADR-1029 decision 4), both sides of it, on one fixture.
///
/// Below `claim_min_input_bytes` the bucket runs unclaimed and issues EXACTLY
/// zero coordinate-phase requests: no claim object is written at all. At a
/// threshold the same bucket clears, it issues exactly two -- one acquisition
/// and one completion -- because the merge is far shorter than a third of the
/// lease, so no renewal is due.
///
/// Shown failing against a guard that ignores the gate (the `input_bytes >=
/// claim_min_input_bytes` term removed from `claim_guard::claims_bucket`): the
/// first half then fails at "a bucket below the cost gate issues no claim
/// request at all", left 2, right 0.
#[tokio::test]
async fn the_cost_gate_decides_whether_a_bucket_is_claimed_at_all() {
    // The shipped defaults this test's two halves stand on.
    assert_eq!(
        CompactorConfig::default().claim_min_input_bytes,
        64 * 1024 * 1024,
        "the shipped cost gate is 64 MiB of listed input bytes"
    );
    assert_eq!(
        CompactorConfig::default().coordination,
        Coordination::On,
        "and coordination is on by default"
    );
    assert_eq!(
        CompactorConfig::default().claim_lease_duration,
        Duration::from_secs(300),
        "and the shipped claim lease is 300 s"
    );

    // Below the gate: the shipped 64 MiB default over a few-KiB bucket.
    let store = MemoryStore::new();
    seed_two_metric_inputs(&store).await;
    let clock = FixedClock::new(sealed_now_ns());
    let ledger = RequestLedger::new();
    let config = claiming_config(1, &clock, 64 * 1024 * 1024, &ledger);

    let outcome = compact_bucket_claimed(&store, &clock, &config, &bucket())
        .await
        .expect("compact");
    assert!(
        matches!(
            outcome,
            ClaimedCompaction::Ran(CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            })
        ),
        "the bucket is compacted, just not claimed: {outcome:?}"
    );
    assert_eq!(
        ledger.report().coordinate.requests,
        0,
        "a bucket below the cost gate issues no claim request at all"
    );
    assert_eq!(
        list_all(&store, COMPACTION_CLAIMS_PREFIX)
            .await
            .expect("list claims")
            .len(),
        0,
        "and writes no claim object"
    );

    // At the gate: the same fixture, a threshold every bucket clears.
    let store = MemoryStore::new();
    seed_two_metric_inputs(&store).await;
    let ledger = RequestLedger::new();
    let config = claiming_config(1, &clock, 1, &ledger);

    let outcome = compact_bucket_claimed(&store, &clock, &config, &bucket())
        .await
        .expect("compact");
    assert!(
        matches!(
            outcome,
            ClaimedCompaction::Ran(CompactionOutcome::Compacted { .. })
        ),
        "{outcome:?}"
    );
    let report = ledger.report();
    assert_eq!(
        report.coordinate.requests, 2,
        "one acquisition and one completion; the merge is shorter than a third \
         of the lease, so no renewal is due"
    );
    assert_eq!(
        report.coordinate.wire_bytes_sent, 0,
        "claim payloads are built inside the claim primitive and are not \
         visible at this crate's seam, so the phase reports requests only"
    );
    assert_eq!(
        list_all(&store, COMPACTION_CLAIMS_PREFIX)
            .await
            .expect("list claims")
            .len(),
        1,
        "exactly one claim object, for this one bucket"
    );
    assert_eq!(
        record_keys(&store).await.len(),
        1,
        "and the claimed run published its record"
    );
}

/// Coordination off is the same code path as a below-gate bucket: the merge
/// runs, publishes, and issues no claim request (ADR-1029 decision 5's escape
/// hatch is a config value, not a second pipeline).
#[tokio::test]
async fn coordination_off_runs_the_same_pipeline_unclaimed() {
    let store = MemoryStore::new();
    seed_two_metric_inputs(&store).await;
    let clock = FixedClock::new(sealed_now_ns());
    let ledger = RequestLedger::new();
    let config = CompactorConfig {
        coordination: Coordination::Off,
        ..claiming_config(1, &clock, 1, &ledger)
    };

    let outcome = compact_bucket_claimed(&store, &clock, &config, &bucket())
        .await
        .expect("compact");
    assert!(
        matches!(
            outcome,
            ClaimedCompaction::Ran(CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            })
        ),
        "{outcome:?}"
    );
    assert_eq!(ledger.report().coordinate.requests, 0);
    assert_eq!(record_keys(&store).await.len(), 1);
}

/// [`compact_bucket`], the uncoordinated entry point, takes no claim even under
/// a config that would claim: claiming is a property of the coordinated entry
/// point, not of the config alone. Both in-tree drivers that do claim, the
/// supervisor tick and `ravel-cli`'s `compact-bucket`/`compact-tenant` (#1034),
/// go through [`compact_bucket_claimed`] instead.
#[tokio::test]
async fn the_unclaimed_entry_point_never_claims() {
    let store = MemoryStore::new();
    seed_two_metric_inputs(&store).await;
    let clock = FixedClock::new(sealed_now_ns());
    let ledger = RequestLedger::new();
    let config = claiming_config(1, &clock, 1, &ledger);

    let outcome = ravel_maintain::compact_bucket(&store, &clock, &config, &bucket())
        .await
        .expect("compact");
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{outcome:?}"
    );
    assert_eq!(ledger.report().coordinate.requests, 0);
    assert_eq!(
        list_all(&store, COMPACTION_CLAIMS_PREFIX)
            .await
            .expect("list claims")
            .len(),
        0,
        "no claim object was written"
    );
}

/// A second attempt refused the claim does NOTHING: it reports the skip with
/// the holder and a reschedule point past the holder's expiry, and publishes
/// no record and no part.
///
/// The claim is seeded by a first guard rather than by a first merge, so the
/// bucket still has no compaction record when the second attempt runs: without
/// that, the already-compacted gate would return before the claim is ever
/// consulted and the test would prove nothing about claims.
#[tokio::test]
async fn a_held_claim_skips_the_bucket_without_merging_it() {
    let store = MemoryStore::new();
    // The store's clock is the expiry base, so it is put on the same timeline
    // as the injected clock the compaction and the claim both read: the
    // bucket's sealed instant.
    let now_ns = sealed_now_ns();
    store.set_clock_ms((now_ns / 1_000_000) as u64);
    seed_two_metric_inputs(&store).await;
    let clock = FixedClock::new(now_ns);

    let holder = ClaimGuard::new(
        &bucket(),
        &participant(1, &clock),
        ClaimConfig {
            lease_duration: LEASE,
            ..ClaimConfig::default()
        },
        None,
    );
    assert!(matches!(
        holder.acquire(&store).await.expect("holder acquires"),
        Acquire::Acquired
    ));

    let ledger = RequestLedger::new();
    let config = claiming_config(2, &clock, 1, &ledger);
    let outcome = compact_bucket_claimed(&store, &clock, &config, &bucket())
        .await
        .expect("compact");

    let skip = match outcome {
        ClaimedCompaction::SkippedClaimed(skip) => skip,
        other => panic!("expected the bucket to be skipped, got {other:?}"),
    };
    assert_eq!(skip.holder_process_id, Some(Uuid::from_u128(1)));
    assert_eq!(skip.work_id_hex, holder.work_id_hex());
    assert_eq!(
        skip.expiry_unix_ms,
        now_ns / 1_000_000 + 3_000,
        "expiry is the store's own write timestamp plus the lease"
    );
    assert!(
        skip.reschedule_after_unix_ms > skip.expiry_unix_ms,
        "the retry is scheduled strictly after the holder's expiry"
    );
    let report = ledger.report();
    assert_eq!(
        report.coordinate.requests, 3,
        "the contention path is one rejected PUT, one GET and one HEAD"
    );
    assert_eq!(
        report.record_read.requests, 2,
        "the claim is taken after the input commit records are read (one GET \
         per input), because the cost gate is priced on them"
    );
    assert_eq!(
        report.catalog_read.requests + report.block_read.requests,
        0,
        "and before any catalog or block read"
    );
    assert_eq!(
        report.part_put.requests, 0,
        "the skipped attempt built and PUT no part"
    );
    assert_eq!(report.publish.requests, 0, "and published nothing at all");
    assert_eq!(record_keys(&store).await.len(), 0);
}

/// A claim lost mid-merge cancels the run at its next checkpoint, and the run
/// publishes NOTHING: exactly zero record PUTs.
///
/// The renewal is rejected by a scripted `FailedConditionalWrite` on the second
/// PUT under the claims prefix (the first is the acquisition), which is exactly
/// what a steal leaves behind for the dispossessed owner. The renewal is made
/// due by advancing the injected clock past a third of the lease at the moment
/// the first L1 part PUT lands, so the cancel site is deterministic: the part
/// boundary immediately after it.
///
/// Shown failing against a guard that acquires and never checks (the body of
/// `claim_guard::checkpoint` replaced by `Ok(())`): the run then reports
/// `Ran(Compacted { parts: 1, publish: Published })` instead of a cancelled
/// run, and publishes a record.
#[tokio::test]
async fn a_lost_renewal_cancels_at_the_part_boundary_and_publishes_nothing() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite)
            .with_key_contains(COMPACTION_CLAIMS_PREFIX)
            .with_occurrence(Occurrence::Nth(2)),
    );
    let clock = FixedClock::new(sealed_now_ns());
    let store = AdvanceClockOnFirstPartPut {
        inner: FaultStore::new(MemoryStore::new(), plan),
        clock: clock.clone(),
        // Past a third of the 3 s lease, so the very next checkpoint renews.
        advance_ns: LEASE.as_nanos() as i64,
        fired: AtomicBool::new(false),
    };
    seed_two_metric_inputs(&store).await;

    let ledger = RequestLedger::new();
    let config = claiming_config(1, &clock, 1, &ledger);
    let outcome = compact_bucket_claimed(&store, &clock, &config, &bucket())
        .await
        .expect("a lost claim is an outcome, never an error");

    match outcome {
        ClaimedCompaction::Cancelled { at, outcome } => {
            assert_eq!(
                at,
                Checkpoint::PartBoundary,
                "the run cancels at the checkpoint after the part PUT that made \
                 the renewal due"
            );
            assert_eq!(
                outcome,
                CompactionOutcome::Compacted {
                    parts: 0,
                    publish: PublishOutcome::Abandoned
                },
                "a cancelled run abandons: it publishes no part set"
            );
        }
        other => panic!("expected a cancelled run, got {other:?}"),
    }
    assert!(
        store.fired.load(Ordering::SeqCst),
        "the clock really advanced"
    );
    assert_eq!(
        store
            .inner
            .fault_count(Op::Put, FaultKind::FailedConditionalWrite),
        1,
        "the scripted renewal rejection fired exactly once"
    );
    assert_eq!(
        ledger.report().publish.requests,
        0,
        "exactly zero record PUTs: the cancelled run published nothing"
    );
    assert_eq!(record_keys(&store).await.len(), 0);
    assert_eq!(
        ledger.report().part_put.requests,
        1,
        "the one part it had already written was PUT, and is left where it is: \
         content-addressed and byte-identical to what a later run over the same \
         frozen input set republishes (PublishOutcome::Abandoned)"
    );
}

/// ADR-1029 decision 2, the property the whole design rests on: a claim confers
/// no publication rights and its absence removes none.
///
/// Owner A takes the claim and is paused mid-merge, at its first part PUT. The
/// lease expires under it; owner B steals the claim and merges the bucket to
/// completion. A then resumes and finishes anyway -- its next checkpoint does
/// not renew, because the renewal cadence has not come due on its own clock, so
/// nothing cancels it -- and its publish collides at the content-addressed part
/// keys and the record's `CreateIfAbsent`. Both converge: exactly ONE
/// compaction record exists, and the rows it serves are the inputs' exactly.
///
/// This is the case a claim bug looks like, and it costs work and nothing else.
#[tokio::test]
async fn a_paused_stale_owner_that_finishes_after_a_steal_converges_on_one_record() {
    let now_ns = sealed_now_ns();
    let store = Arc::new(PauseFirstPartPut {
        inner: MemoryStore::new(),
        a_reached_part: tokio::sync::Notify::new(),
        b_finished: tokio::sync::Notify::new(),
        paused: AtomicBool::new(false),
    });
    store.inner.set_clock_ms((now_ns / 1_000_000) as u64);
    seed_two_metric_inputs(store.as_ref()).await;

    let clock = FixedClock::new(now_ns);
    let a_ledger = RequestLedger::new();
    let a_config = claiming_config(1, &clock, 1, &a_ledger);
    let b_ledger = RequestLedger::new();

    let a_store = Arc::clone(&store);
    let b_store = Arc::clone(&store);
    let a_clock = clock.clone();
    let a = async move {
        compact_bucket_claimed(a_store.as_ref(), &a_clock, &a_config, &bucket())
            .await
            .expect("A compacts")
    };
    let b = async {
        // Wait until A is paused holding its claim and its first part written.
        b_store.a_reached_part.notified().await;
        // The lease expires on the STORE's clock, the one time base every
        // contender shares.
        let expired_ns = now_ns + 4 * 1_000_000_000;
        b_store.inner.set_clock_ms((expired_ns / 1_000_000) as u64);
        let b_clock = FixedClock::new(expired_ns);
        let b_config = claiming_config(2, &b_clock, 1, &b_ledger);
        let outcome = compact_bucket_claimed(b_store.as_ref(), &b_clock, &b_config, &bucket())
            .await
            .expect("B compacts");
        b_store.b_finished.notify_one();
        outcome
    };
    let (a_outcome, b_outcome) = tokio::join!(a, b);

    assert!(
        matches!(
            b_outcome,
            ClaimedCompaction::Ran(CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            })
        ),
        "B steals the expired claim and publishes: {b_outcome:?}"
    );
    assert_eq!(
        b_ledger.report().coordinate.requests,
        5,
        "B's claim protocol: the rejected CreateIfAbsent, the GET and HEAD that \
         observed the expired claim, the steal that took it, and the completion"
    );
    assert!(
        matches!(
            a_outcome,
            ClaimedCompaction::Ran(CompactionOutcome::Compacted {
                publish: PublishOutcome::Converged { .. },
                ..
            })
        ),
        "A finished after losing its claim and converged on B's record rather \
         than publishing a second: {a_outcome:?}"
    );

    assert_eq!(
        record_keys(store.as_ref()).await.len(),
        1,
        "exactly one compaction record, whatever happened to the claim"
    );
    let record = fetch_compaction_record(store.as_ref(), &bucket()).await;
    let expected = expected_samples(&[
        InputSpec::new(
            Uuid::from_u128(1),
            1,
            1,
            vec![raw_series("alpha", &[], &[(10, 1.0), (20, 2.0)])],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            1,
            2,
            vec![raw_series("beta", &[], &[(30, 3.0)])],
        ),
    ]);
    assert_eq!(
        read_record_samples(store.as_ref(), &record).await,
        expected,
        "and it serves exactly the inputs' rows"
    );
}

/// A bucket another attempt holds the claim on is NOT re-requested on the next
/// tick: the scan's memo holds it until the holder's lease can have expired
/// (ADR-1029 decision 1 step 2, "never poll an active claim").
///
/// The first pass observes the holder and pays the three-request contention
/// path; the second pass, on the same memo and the same clock, issues no claim
/// request at all and still reports the bucket skipped rather than compacted.
///
/// Shown failing by removing the `memo.claim_deferred` hold from
/// `scan_and_maintain_with_memo`'s bucket loop: the second pass then issues the
/// same three coordinate requests again, which is the polling loop the claim
/// protocol exists to avoid.
#[tokio::test]
async fn a_claimed_away_bucket_is_not_polled_on_the_next_tick() {
    let store = MemoryStore::new();
    let now_ns = sealed_now_ns();
    store.set_clock_ms((now_ns / 1_000_000) as u64);
    seed_two_metric_inputs(&store).await;
    let clock = FixedClock::new(now_ns);

    let holder = ClaimGuard::new(
        &bucket(),
        &participant(1, &clock),
        ClaimConfig {
            lease_duration: LEASE,
            ..ClaimConfig::default()
        },
        None,
    );
    assert!(matches!(
        holder.acquire(&store).await.expect("holder acquires"),
        Acquire::Acquired
    ));

    let mut memo = ravel_maintain::MaintainMemo::with_default_interval();
    let first_ledger = RequestLedger::new();
    let first = ravel_maintain::scan::scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &clock,
        &claiming_config(2, &clock, 1, &first_ledger),
        &ravel_maintain::RetentionConfig::default(),
        &ravel_maintain::NoLeases,
        bucket().tenant_hash,
        bucket().signal,
        bucket().shard,
    )
    .await
    .expect("first pass");
    assert_eq!(first.claim_skipped, 1, "the holder's bucket is skipped");
    assert_eq!(first.compacted, 0, "and never counted as compacted");
    assert_eq!(
        first_ledger.report().coordinate.requests,
        3,
        "the contention path is one rejected PUT, one GET and one HEAD"
    );

    let second_ledger = RequestLedger::new();
    let second = ravel_maintain::scan::scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &clock,
        &claiming_config(2, &clock, 1, &second_ledger),
        &ravel_maintain::RetentionConfig::default(),
        &ravel_maintain::NoLeases,
        bucket().tenant_hash,
        bucket().signal,
        bucket().shard,
    )
    .await
    .expect("second pass");
    assert_eq!(
        second.claim_skipped, 1,
        "the bucket is still reported skipped, from the hold alone"
    );
    assert_eq!(second.compacted, 0);
    assert_eq!(
        second_ledger.report().coordinate.requests,
        0,
        "and the second pass issues no claim request at all"
    );
    assert_eq!(record_keys(&store).await.len(), 0, "nothing was published");
}

/// A claim hold skips only the held bucket's compaction. Retention and the
/// zone split still run for it, so a held head-zone hour stays in
/// `head_tail_hours`, which is what scopes the supervisor's zoned sweep, and
/// the bucket is still not compacted.
///
/// Shown failing against the loop that `continue`d on a held bucket before
/// classifying its zone: "a held head hour still reaches head_tail_hours"
/// reads left [], right [495000].
#[tokio::test]
async fn a_held_bucket_still_reaches_the_zone_split() {
    let store = MemoryStore::new();
    let config = CompactorConfig::default();
    // Sealed, and within the hour of slack past the seal margin that keeps a
    // bucket in the head zone.
    let now_ns = bucket().end_ns() + config.seal_margin_ns() + NS_PER_HOUR / 2;
    assert_eq!(
        ravel_maintain::scan::classify_zone(HOUR, now_ns, &config, None),
        ravel_maintain::scan::Zone::Head,
        "the fixture hour must be a head hour for this test to mean anything"
    );
    store.set_clock_ms((now_ns / 1_000_000) as u64);
    seed_two_metric_inputs(&store).await;
    let clock = FixedClock::new(now_ns);

    let holder = ClaimGuard::new(
        &bucket(),
        &participant(1, &clock),
        ClaimConfig {
            lease_duration: LEASE,
            ..ClaimConfig::default()
        },
        None,
    );
    assert!(matches!(
        holder.acquire(&store).await.expect("holder acquires"),
        Acquire::Acquired
    ));

    let mut memo = ravel_maintain::MaintainMemo::with_default_interval();
    let first = ravel_maintain::scan::scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &clock,
        &claiming_config(2, &clock, 1, &RequestLedger::new()),
        &ravel_maintain::RetentionConfig::default(),
        &ravel_maintain::NoLeases,
        bucket().tenant_hash,
        bucket().signal,
        bucket().shard,
    )
    .await
    .expect("first pass");
    assert_eq!(first.claim_skipped, 1, "the first pass observes the holder");
    assert_eq!(first.head_tail_hours, vec![HOUR]);

    let second_ledger = RequestLedger::new();
    let second = ravel_maintain::scan::scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &clock,
        &claiming_config(2, &clock, 1, &second_ledger),
        &ravel_maintain::RetentionConfig::default(),
        &ravel_maintain::NoLeases,
        bucket().tenant_hash,
        bucket().signal,
        bucket().shard,
    )
    .await
    .expect("second pass");
    assert_eq!(
        second.head_tail_hours,
        vec![HOUR],
        "a held head hour still reaches head_tail_hours"
    );
    assert_eq!(second.claim_skipped, 1, "the hold still skips the bucket");
    assert_eq!(second.compacted, 0, "and it is not compacted");
    assert_eq!(
        second_ledger.report().coordinate.requests,
        0,
        "the hold issues no claim request"
    );
    assert_eq!(record_keys(&store).await.len(), 0, "nothing was published");
}

/// Every signal's merge consults the guard at its own seams, not just the RLOG
/// one the ADR's line numbers pointed at.
///
/// Shown failing against the same never-checks guard: the metrics count reads
/// 0 checkpoints instead of 4, and the logs and spans counts follow.
///
/// A claim clock that advances a renewal cadence per read makes a renewal due
/// at EVERY checkpoint, so the
/// coordinate-phase request count is the acquisition, one renewal per
/// checkpoint the run reached, and the completion. The exact figure per signal
/// is therefore a direct count of the checkpoints the pipeline consulted, and
/// removing any seam lowers it.
#[tokio::test]
async fn every_signal_consults_the_guard_at_its_own_seams() {
    // metrics: input_set + one fetch window + one part boundary + publish = 4
    // checkpoints. The two-input fixture's series fit one fetch window and one
    // part, so the merge-loop and part-boundary terms are one each.
    let store = MemoryStore::new();
    seed_two_metric_inputs(&store).await;
    assert_eq!(checkpoints_taken(&store, &bucket()).await, 4, "metrics");

    // logs: input_set + one per merged stream + one part boundary + publish
    // = 6. The fixture's two inputs carry streams {0,1} and {0,2}, so the
    // merged set is three streams and the merge-loop checkpoint fires three
    // times.
    let store = MemoryStore::new();
    let logs = seed_rlog_two_inputs(&store).await;
    assert_eq!(checkpoints_taken(&store, &logs).await, 6, "logs");

    // spans: input_set + one per merged trace + one part boundary + publish
    // = 6. The fixture's two inputs carry traces {0,1} and {0,2}; a trace is
    // RSPAN's unit of merge progress, so the checkpoint fires at each of the
    // three trace transitions.
    let store = MemoryStore::new();
    let spans = seed_rspan_two_inputs(&store).await;
    assert_eq!(checkpoints_taken(&store, &spans).await, 6, "spans");
}

/// Compact `bucket` with a claim clock that advances a full renewal cadence
/// between reads, so EVERY checkpoint renews, and return how many checkpoints
/// the run consulted: the coordinate-phase requests less the acquisition and
/// the completion.
async fn checkpoints_taken(store: &dyn ObjectStoreBackend, bucket: &ravel_maintain::Bucket) -> u64 {
    let clock = FixedClock::new(sealed_now_ns());
    let ledger = RequestLedger::new();
    let config = CompactorConfig {
        claim_participant: Some(ticking_participant()),
        ..claiming_config(1, &clock, 1, &ledger)
    };
    let outcome = compact_bucket_claimed(store, &clock, &config, bucket)
        .await
        .expect("compact");
    assert!(
        matches!(
            outcome,
            ClaimedCompaction::Ran(CompactionOutcome::Compacted { .. })
        ),
        "the bucket must really compact for its seams to be reached: {outcome:?}"
    );
    ledger.report().coordinate.requests - 2
}

/// A participant whose claim clock advances one renewal cadence per read, so
/// every checkpoint the run reaches issues exactly one renewal.
fn ticking_participant() -> ClaimParticipant {
    ClaimParticipant::new(
        Uuid::from_u128(1),
        Arc::new(TickingClock::new(
            sealed_now_ns(),
            LEASE.as_nanos() as i64 / 3,
        )) as Arc<dyn Clock>,
    )
    .with_sleeper(Arc::new(NoWait))
}

/// A clock that advances one renewal cadence on every read, so a guard
/// consulting it finds a renewal due at every checkpoint. The compaction's own
/// clock stays fixed; only the claim participant reads this one.
struct TickingClock {
    now_ns: std::sync::atomic::AtomicI64,
    step_ns: i64,
}

impl TickingClock {
    fn new(start_ns: i64, step_ns: i64) -> Self {
        TickingClock {
            now_ns: std::sync::atomic::AtomicI64::new(start_ns),
            step_ns,
        }
    }
}

impl Clock for TickingClock {
    fn now_ns(&self) -> i64 {
        self.now_ns.fetch_add(self.step_ns, Ordering::SeqCst) + self.step_ns
    }
}

/// A store that advances an injected clock the first time an L1 part is PUT.
///
/// This is how a test makes a renewal fall due at a known point in the merge
/// without a wall-clock sleep: the clock is the one the claim guard reads, and
/// the part PUT is the seam whose checkpoint immediately follows.
struct AdvanceClockOnFirstPartPut {
    inner: FaultStore<MemoryStore>,
    clock: FixedClock,
    advance_ns: i64,
    fired: AtomicBool,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for AdvanceClockOnFirstPartPut {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        let outcome = self.inner.put(key, data, opts).await;
        if key.contains("/l1/")
            && self
                .fired
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.clock.set(self.clock.now_ns() + self.advance_ns);
        }
        outcome
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// A store that pauses the first L1 part PUT until another run has finished,
/// which is how this file pauses one owner mid-merge without a sleep or a
/// race. The pause is released by `b_finished`; the part itself is written
/// before the pause, so the second run's identical content-addressed PUT sees
/// the bytes already there, exactly as two real racing merges do.
struct PauseFirstPartPut {
    inner: MemoryStore,
    a_reached_part: tokio::sync::Notify,
    b_finished: tokio::sync::Notify,
    paused: AtomicBool,
}

#[async_trait::async_trait]
impl ObjectStoreBackend for PauseFirstPartPut {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        let outcome = self.inner.put(key, data, opts).await;
        if key.contains("/l1/")
            && self
                .paused
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            self.a_reached_part.notify_one();
            self.b_finished.notified().await;
        }
        outcome
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// Marking the claim completed is advisory, so a store error on that last CAS
/// does not fail a run whose record is published: the bucket still reports
/// `Compacted`, and the supervisor's pass goes on to the unit's next bucket.
///
/// The completion is the second claim PUT of a run whose merge renews nothing
/// (acquisition, then completion), so a transient fault scripted on the
/// second PUT under the claims prefix lands on exactly the first bucket's
/// completion.
///
/// Shown failing against the pre-change driver (`guard.complete(store).await?`
/// in `compact_bucket_scoped`): the direct call panics at "a failed
/// completion does not fail a published run" with `Store(Transient(..))`, and
/// the scan half at "the pass is not aborted" with the same error.
#[tokio::test]
async fn a_failed_completion_keeps_the_published_outcome_and_the_pass_goes_on() {
    let transient_on_completion = || {
        FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Transient("completion CAS dropped".into()),
            )
            .with_key_contains(COMPACTION_CLAIMS_PREFIX)
            .with_occurrence(Occurrence::Nth(2)),
        )
    };

    // One bucket, driven directly.
    let store = FaultStore::new(MemoryStore::new(), transient_on_completion());
    seed_two_metric_inputs(&store).await;
    let clock = FixedClock::new(sealed_now_ns());
    let ledger = RequestLedger::new();
    let outcome = compact_bucket_claimed(
        &store,
        &clock,
        &claiming_config(1, &clock, 1, &ledger),
        &bucket(),
    )
    .await
    .expect("a failed completion does not fail a published run");
    assert!(
        matches!(
            outcome,
            ClaimedCompaction::Ran(CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            })
        ),
        "{outcome:?}"
    );
    assert_eq!(store.fault_count(Op::Put, FaultKind::Transient), 1);
    assert_eq!(record_keys(&store).await.len(), 1);

    // Two sealed buckets in one unit, driven by the supervisor's pass.
    let store = FaultStore::new(MemoryStore::new(), transient_on_completion());
    for hour in [HOUR, HOUR + 1] {
        for (writer, seq, name) in [(1u128, 1u64, "alpha"), (2, 2, "beta")] {
            seed_input(
                &store,
                &InputSpec::new_at(
                    hour,
                    Uuid::from_u128(writer),
                    1,
                    seq,
                    vec![raw_series(name, &[], &[(10, 1.0)])],
                ),
            )
            .await;
        }
    }
    let clock = FixedClock::new(sealed_now_ns() + NS_PER_HOUR);
    let ledger = RequestLedger::new();
    let mut memo = ravel_maintain::MaintainMemo::with_default_interval();
    let report = ravel_maintain::scan::scan_and_maintain_with_memo(
        &mut memo,
        &store,
        &clock,
        &claiming_config(1, &clock, 1, &ledger),
        &ravel_maintain::RetentionConfig::default(),
        &ravel_maintain::NoLeases,
        bucket().tenant_hash,
        bucket().signal,
        bucket().shard,
    )
    .await
    .expect("the pass is not aborted");
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::Transient),
        1,
        "the scripted completion fault fired exactly once"
    );
    assert_eq!(report.compacted, 2, "both buckets compacted in one pass");
    for hour in [HOUR, HOUR + 1] {
        assert_eq!(
            record_keys_in(&store, &bucket_at(hour)).await.len(),
            1,
            "hour {hour} published its record"
        );
    }
}

/// An unreadable claim older than one lease plus jitter does not starve its
/// bucket: the run goes ahead unclaimed and publishes, the claim object is
/// left untouched, and the claim protocol issues only the contention path
/// (no steal, no completion).
///
/// Shown failing against a guard that skips an unreadable claim whatever its
/// age: the outcome is `SkippedClaimed(.. UnreadableClaim ..)` instead.
#[tokio::test]
async fn a_stale_unreadable_claim_runs_the_bucket_unclaimed() {
    let store = MemoryStore::new();
    let now_ns = sealed_now_ns();
    let written_ms = now_ns / 1_000_000;
    store.set_clock_ms(written_ms as u64);
    seed_two_metric_inputs(&store).await;
    let holder = ClaimGuard::new(
        &bucket(),
        &participant(9, &FixedClock::new(now_ns)),
        ClaimConfig {
            lease_duration: LEASE,
            ..ClaimConfig::default()
        },
        None,
    );
    let key = format!("{COMPACTION_CLAIMS_PREFIX}{}", holder.work_id_hex());
    let garbage = Bytes::from_static(b"\xffnot a claim");
    store
        .put(&key, garbage.clone(), PutOptions::create_if_absent())
        .await
        .expect("seed an unreadable claim");

    // Well past one lease plus the largest possible jitter (10% of the lease).
    let later_ns = now_ns + 2 * LEASE.as_nanos() as i64;
    let clock = FixedClock::new(later_ns);
    let ledger = RequestLedger::new();
    let outcome = compact_bucket_claimed(
        &store,
        &clock,
        &claiming_config(2, &clock, 1, &ledger),
        &bucket(),
    )
    .await
    .expect("compact");
    assert!(
        matches!(
            outcome,
            ClaimedCompaction::Ran(CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            })
        ),
        "{outcome:?}"
    );
    assert_eq!(
        ledger.report().coordinate.requests,
        3,
        "the rejected CreateIfAbsent, one GET and one HEAD: no steal, no completion"
    );
    assert_eq!(
        store
            .get(&key, GetRange::Full)
            .await
            .expect("claim read")
            .data,
        garbage,
        "the unreadable claim is left in place"
    );
    assert_eq!(record_keys(&store).await.len(), 1);
}

/// The Publish checkpoint (rewrite.rs, immediately before the record PUT)
/// cancels a run whose claim is lost after its last part PUT: the run reports
/// `Cancelled` at `Checkpoint::Publish` and issues exactly zero record PUTs.
///
/// The claim clock advances a renewal cadence per read, so every checkpoint
/// renews. For this one-part metrics fixture the claim PUTs are the
/// acquisition, then renewals at InputSet, MergeLoop, PartBoundary and
/// Publish; the fifth claim PUT is rejected, which is the loss landing after
/// the part-boundary renewal that followed the last part PUT.
///
/// Shown failing with the Publish checkpoint removed from
/// `rewrite_and_publish_loaded`: the run then publishes and the rejected CAS
/// lands on the completion instead, and the test panics "expected a
/// cancelled run, got Ran(Compacted { parts: 1, publish: Published })".
#[tokio::test]
async fn a_claim_lost_after_the_last_part_cancels_at_publish() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite)
            .with_key_contains(COMPACTION_CLAIMS_PREFIX)
            .with_occurrence(Occurrence::Nth(5)),
    );
    let store = FaultStore::new(MemoryStore::new(), plan);
    seed_two_metric_inputs(&store).await;
    let clock = FixedClock::new(sealed_now_ns());
    let ledger = RequestLedger::new();
    let config = CompactorConfig {
        claim_participant: Some(ticking_participant()),
        ..claiming_config(1, &clock, 1, &ledger)
    };

    let outcome = compact_bucket_claimed(&store, &clock, &config, &bucket())
        .await
        .expect("a lost claim is an outcome, never an error");
    match outcome {
        ClaimedCompaction::Cancelled { at, outcome } => {
            assert_eq!(at, Checkpoint::Publish);
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
        1,
        "the scripted claim loss fired exactly once"
    );
    let report = ledger.report();
    assert_eq!(report.part_put.requests, 1, "the one part was already PUT");
    assert_eq!(report.publish.requests, 0, "exactly zero record PUTs");
    assert_eq!(record_keys(&store).await.len(), 0);
}

/// Three metrics series in two inputs. At `max_l1_part_bytes = 1` every series
/// is its own fetch window and its own part, so the merge writes three parts
/// and the first two part boundaries are the mid-loop checkpoint in
/// `build.rs`, not the tail one.
async fn seed_three_metric_series(store: &dyn ObjectStoreBackend) {
    seed_input(
        store,
        &InputSpec::new(
            Uuid::from_u128(1),
            1,
            1,
            vec![
                raw_series("alpha", &[], &[(10, 1.0)]),
                raw_series("beta", &[], &[(20, 2.0)]),
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
            vec![raw_series("gamma", &[], &[(30, 3.0)])],
        ),
    )
    .await;
}

/// Part-size knobs small enough that every metrics series and every RSPAN
/// trace closes its own part. RSEG splits on `max_l1_part_bytes`, RSPAN on
/// `l1_part_memory_target_bytes`; both are set.
fn tiny_parts(config: CompactorConfig) -> CompactorConfig {
    CompactorConfig {
        max_l1_part_bytes: 1,
        l1_part_memory_target_bytes: 1,
        ..config
    }
}

/// A claim lost after the FIRST part of a multi-part merge cancels at the
/// part-boundary checkpoint right after that part's PUT, for RSEG
/// (`build.rs`'s mid-loop flush) and RSPAN (`rspan_codec.rs`'s trace-boundary
/// flush), with exactly one part PUT.
///
/// Each fixture is first compacted unclaimed with the same knobs to prove it
/// really writes three parts, so the cancel site is not the tail checkpoint.
/// The renewal is made due by advancing the claim clock at the first L1 part
/// PUT and rejected by a scripted conditional-write failure on the second
/// claim PUT (the first is the acquisition).
///
/// Shown failing with each mid-loop checkpoint removed: the next merge-loop
/// head catches the loss instead, and the `at` assertion reads left
/// `MergeLoop`, right `PartBoundary` (rseg in `build.rs`, rspan in
/// `rspan_codec.rs`).
#[tokio::test]
async fn a_claim_lost_after_the_first_of_three_parts_cancels_at_that_boundary() {
    // metrics
    let baseline = MemoryStore::new();
    seed_three_metric_series(&baseline).await;
    assert_eq!(unclaimed_parts(&baseline, &bucket()).await, 3, "rseg parts");
    let (at, part_puts) = cancel_after_first_part(Signal::Metrics).await;
    assert_eq!(at, Checkpoint::PartBoundary, "rseg");
    assert_eq!(part_puts, 1, "rseg: exactly one part PUT");

    // spans: traces {0,1} and {0,2} merge to three traces, one part each.
    let baseline = MemoryStore::new();
    let spans = seed_rspan_two_inputs(&baseline).await;
    assert_eq!(unclaimed_parts(&baseline, &spans).await, 3, "rspan parts");
    let (at, part_puts) = cancel_after_first_part(Signal::Spans).await;
    assert_eq!(at, Checkpoint::PartBoundary, "rspan");
    assert_eq!(part_puts, 1, "rspan: exactly one part PUT");
}

/// Compact `b` unclaimed under [`tiny_parts`] and return its part count.
async fn unclaimed_parts(store: &dyn ObjectStoreBackend, b: &ravel_maintain::Bucket) -> usize {
    let clock = FixedClock::new(sealed_now_ns());
    let config = tiny_parts(CompactorConfig::default());
    match ravel_maintain::compact_bucket(store, &clock, &config, b)
        .await
        .expect("baseline compact")
    {
        CompactionOutcome::Compacted {
            parts,
            publish: PublishOutcome::Published,
        } => parts,
        other => panic!("the baseline must publish: {other:?}"),
    }
}

/// Seed the three-part fixture for `signal`, lose the claim after its first
/// part PUT, and return the cancel site and the part PUT count.
async fn cancel_after_first_part(signal: Signal) -> (Checkpoint, u64) {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite)
            .with_key_contains(COMPACTION_CLAIMS_PREFIX)
            .with_occurrence(Occurrence::Nth(2)),
    );
    let clock = FixedClock::new(sealed_now_ns());
    let store = AdvanceClockOnFirstPartPut {
        inner: FaultStore::new(MemoryStore::new(), plan),
        clock: clock.clone(),
        advance_ns: LEASE.as_nanos() as i64,
        fired: AtomicBool::new(false),
    };
    let b = match signal {
        Signal::Metrics => {
            seed_three_metric_series(&store).await;
            bucket()
        }
        Signal::Spans => seed_rspan_two_inputs(&store).await,
        other => panic!("no three-part fixture for {other:?}"),
    };
    let ledger = RequestLedger::new();
    let config = tiny_parts(claiming_config(1, &clock, 1, &ledger));
    let outcome = compact_bucket_claimed(&store, &clock, &config, &b)
        .await
        .expect("a lost claim is an outcome, never an error");
    assert!(
        store.fired.load(Ordering::SeqCst),
        "the clock really advanced"
    );
    assert_eq!(
        store
            .inner
            .fault_count(Op::Put, FaultKind::FailedConditionalWrite),
        1,
        "the scripted renewal rejection fired exactly once"
    );
    assert_eq!(ledger.report().publish.requests, 0, "no record PUT");
    assert_eq!(record_keys_in(&store, &b).await.len(), 0);
    match outcome {
        ClaimedCompaction::Cancelled { at, .. } => (at, ledger.report().part_put.requests),
        other => panic!("expected a cancelled run, got {other:?}"),
    }
}
