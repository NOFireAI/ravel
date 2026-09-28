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
use ravel_maintain::claim_guard::{Acquire, ClaimGuard};
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
use uuid::Uuid;

/// A lease long enough that a whole `MemoryStore` merge fits inside a third of
/// it, so a test that does not move the clock itself issues exactly zero
/// renewals. Short enough that the deterministic acquisition jitter (10% of the
/// lease) stays under a third of a second.
const LEASE: Duration = Duration::from_secs(3);

fn participant(process: u128, clock: &FixedClock) -> ClaimParticipant {
    ClaimParticipant::new(
        Uuid::from_u128(process),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
    )
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

/// Every compaction record key in the bucket.
async fn record_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    use ravel_commit::keys;
    let b = bucket();
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

/// [`compact_bucket`], the entry point every caller outside the supervisor
/// still uses, takes no claim even under a config that would claim: claiming is
/// a property of the coordinated entry point, not of the config alone (the CLI
/// adopts it in wave 3, #1034).
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
/// Shown failing by removing the `memo.claim_deferred` check at the top of
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
        claim_participant: Some(ClaimParticipant::new(
            Uuid::from_u128(1),
            Arc::new(TickingClock::new(
                sealed_now_ns(),
                LEASE.as_nanos() as i64 / 3,
            )) as Arc<dyn Clock>,
        )),
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
