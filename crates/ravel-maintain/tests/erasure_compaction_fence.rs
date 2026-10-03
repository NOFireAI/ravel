//! The fence between a compaction publish and an erasure rewrite publish of
//! the same bucket (ADR-1029, the 2026-10-03 amendment; issue #2199).
//!
//! A compaction record (`l1.<hash>.cmt`) and a rewrite record (`rw.<hash>.cmt`)
//! have different keys, so neither's `CreateIfAbsent` refuses the other. A
//! compaction planned from inputs that still hold an erased subject's rows and
//! published after the erasure rewrite of the same bucket serves those rows
//! again. Two checks fence it: both passes take the bucket's one claim before
//! they build and hold it through their record PUT, and both re-list the
//! bucket before that PUT and publish nothing when its record set changed.
//!
//! Every interleaving here is driven by a `FaultStore` hold gate, which parks
//! one pass at a named store call while the other runs, and by `FixedClock`s
//! the test moves. No test sleeps. A hold gate has no `FaultStore` fault
//! counter (it delays a call rather than failing it), so each test proves its
//! hold fired from the gate's own held-call registry: the op and key of the one
//! call it parked.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use ravel_commit::{keys, signal};
use ravel_fleet::claim::COMPACTION_CLAIMS_PREFIX;
use ravel_maintain::claim_guard::ClaimSleeper;
use ravel_maintain::{
    Checkpoint, ClaimAcquisition, ClaimParticipant, ClaimSkipReason, ClaimedCompaction, Clock,
    CompactionOutcome, CompactorConfig, Coordination, ErasureAbandon, ErasureRewriteOutcome,
    FixedClock, MaintainMemo, MigrateOutcome, NoLeases, PendingErasureRequest, PublishOutcome,
    RequestLedger, compact_bucket, compact_bucket_claimed, erasure_rewrite_bucket,
    migrate_bucket_format, read,
};
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_types::Signal;
use uuid::Uuid;

/// A lease long enough that no merge here comes due for a renewal unless the
/// test moves the clock by a third of it.
const LEASE: Duration = Duration::from_secs(3);

/// Past the lease, on every clock a test moves.
const PAST_LEASE_NS: i64 = 4 * 1_000_000_000;

/// The key fragment of every L1 and rewrite part object.
const PART_KEYS: &str = "/l1/";

/// The key fragment of every L0 data object.
const L0_KEYS: &str = "/l0/";

/// A [`ClaimSleeper`] that returns at once, so no jitter wait touches the real
/// timer.
struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _duration: Duration) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// A config for one pass. `process` installs a claim participant on `clock`;
/// `None` is a caller that takes no claims. The cost gate stays at its shipped
/// 64 MiB, far above these few-KiB buckets: it no longer decides whether a
/// participating pass claims.
fn config(process: Option<u128>, clock: &FixedClock, ledger: &RequestLedger) -> CompactorConfig {
    CompactorConfig {
        coordination: Coordination::On,
        claim_lease_duration: LEASE,
        claim_participant: process.map(|p| {
            ClaimParticipant::new(
                Uuid::from_u128(p),
                Arc::new(clock.clone()) as Arc<dyn Clock>,
            )
            .with_sleeper(Arc::new(NoWait))
        }),
        request_ledger: Some(ledger.clone()),
        ..CompactorConfig::default()
    }
}

/// A fault store whose own clock (the time base claim expiry is judged on)
/// reads `now_ns`, seeded with a two-input metrics bucket that holds the
/// subject `victim` beside a series that is kept.
async fn seeded_store(now_ns: i64) -> Arc<FaultStore<MemoryStore>> {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    store.inner().set_clock_ms(ms(now_ns));
    for spec in [
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![
                raw_series("keep", &[("k", "a")], &[(1_000, 1.0), (2_000, 2.0)]),
                raw_series("victim", &[("k", "b")], &[(1_000, 5.0)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("victim", &[("k", "b")], &[(3_000, 3.0)])],
        ),
    ] {
        seed_input(store.as_ref(), &spec).await;
    }
    store
}

fn ms(ns: i64) -> u64 {
    u64::try_from(ns / 1_000_000).expect("positive instant")
}

/// A windowless erasure request for every series named `victim`.
fn pending() -> Vec<PendingErasureRequest> {
    let request_id = Uuid::from_u128(0x2199);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 0,
        predicate: vec![ErasurePredicateMatcher {
            key: "__name__".to_string(),
            value: "victim".to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    vec![PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id)
            .expect("dreq key"),
        request,
    }]
}

async fn erasure(
    store: &dyn ObjectStoreBackend,
    clock: &FixedClock,
    config: &CompactorConfig,
) -> ErasureRewriteOutcome {
    let mut memo = MaintainMemo::with_default_interval();
    erasure_rewrite_bucket(
        store,
        clock,
        config,
        &NoLeases,
        &bucket(),
        &pending(),
        &mut memo,
    )
    .await
    .expect("erasure rewrite")
}

/// Wait until `gate` parks exactly one call, check it is the `op` on a key
/// containing `fragment` this test meant to hold, and return its id.
async fn parked(gate: &GateHandle, op: Op, fragment: &str) -> u64 {
    gate.wait_until_held(1).await;
    let held = gate.held_details();
    assert_eq!(held.len(), 1, "the gate parked exactly one call: {held:?}");
    let (id, held_op, key) = &held[0];
    assert_eq!(*held_op, op, "the parked call is the intended op: {key}");
    assert!(
        key.contains(fragment),
        "the parked call is on {fragment}: {key}"
    );
    *id
}

/// `(compaction records, rewrite records)` in the bucket.
async fn record_sets(store: &dyn ObjectStoreBackend) -> (usize, usize) {
    let listing = read::list_bucket(store, &bucket()).await.expect("list");
    (
        listing.compaction_record_keys.len(),
        listing.rewrite_record_keys.len(),
    )
}

/// A compaction plans, an erasure rewrite publishes on the same bucket, and
/// the compaction then fails to publish: the claim fence.
///
/// Compaction A claims the bucket and is parked at its first part PUT. The
/// lease expires; erasure E, a second process, steals the claim and publishes
/// its rewrite record. A resumes; its next checkpoint renews, the renewal finds
/// the claim stolen, and A cancels there with nothing published.
///
/// Shown failing against the pre-fix code, where E took no claim: A's renewal
/// succeeds and A publishes a compaction record next to E's rewrite record
/// (`Ran(Compacted { publish: Published })`). With only the
/// `claim_bucket(store, config, bucket, "erasure_rewrite")` call in
/// `erasure_rewrite_bucket` removed, E publishes unclaimed, A's renewal
/// succeeds, and A is stopped by its re-list instead (`Ran(RewritePresent)`),
/// which fails the `Cancelled` assertion: this test pins the claim, the
/// re-list test below pins the re-list.
#[tokio::test]
async fn an_erasure_rewrite_that_publishes_mid_compaction_cancels_the_compaction() {
    let now_ns = sealed_now_ns();
    let store = seeded_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let a_ledger = RequestLedger::new();
    let a_config = config(Some(1), &clock, &a_ledger);
    let e_ledger = RequestLedger::new();
    let e_config = config(Some(2), &clock, &e_ledger);
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));

    let a = async {
        compact_bucket_claimed(store.as_ref(), &clock, &a_config, &bucket())
            .await
            .expect("compaction")
    };
    let e = async {
        let id = parked(&gate, Op::Put, PART_KEYS).await;
        clock.set(now_ns + PAST_LEASE_NS);
        store.inner().set_clock_ms(ms(now_ns + PAST_LEASE_NS));
        let outcome = erasure(store.as_ref(), &clock, &e_config).await;
        assert!(gate.release(id), "the parked part PUT was released");
        outcome
    };
    let (a_outcome, e_outcome) = tokio::join!(a, e);

    assert!(
        matches!(
            e_outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "E steals the expired claim and publishes: {e_outcome:?}"
    );
    match a_outcome {
        ClaimedCompaction::Cancelled { at, outcome } => {
            assert_eq!(at, Checkpoint::PartBoundary);
            assert_eq!(
                outcome,
                CompactionOutcome::Compacted {
                    parts: 0,
                    publish: PublishOutcome::Abandoned
                }
            );
        }
        other => panic!("A must cancel on its stolen claim: {other:?}"),
    }
    assert_eq!(
        record_sets(store.as_ref()).await,
        (0, 1),
        "only the erasure rewrite's record set is in the bucket"
    );
    assert_eq!(a_ledger.report().publish.requests, 0, "A PUT no record");
    assert_eq!(
        e_ledger.report().coordinate.requests,
        5,
        "E took the bucket claim: the refused CreateIfAbsent, the GET and HEAD \
         that observed it expired, the steal, and the completion"
    );
}

/// The mirror order: an erasure rewrite plans, a compaction publishes on the
/// same bucket, and the erasure rewrite then fails to publish.
///
/// Erasure E claims the bucket and is parked at its first L0 data GET, after
/// the claim and before its build. The lease expires; compaction C, a second
/// process, steals the claim and publishes a compaction record. E resumes,
/// builds, and its publish checkpoint finds the claim stolen: it publishes
/// nothing, and it stopped there, before its re-list (zero LISTs on its
/// ledger).
///
/// Shown failing against the pre-fix code, where E took no claim and C ran
/// unclaimed below the cost gate: E publishes a rewrite record next to C's
/// compaction record. With only the cost-gate behaviour restored (the
/// `input_bytes >= claim_min_input_bytes` term back in
/// `claim_guard::claims_bucket`), C runs unclaimed and publishes without
/// stealing, E's renewal succeeds, and E is stopped by its re-list instead,
/// which fails the zero-LIST assertion.
#[tokio::test]
async fn a_compaction_that_publishes_mid_erasure_cancels_the_erasure_rewrite() {
    let now_ns = sealed_now_ns();
    let store = seeded_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let e_ledger = RequestLedger::new();
    let e_config = config(Some(1), &clock, &e_ledger);
    let c_ledger = RequestLedger::new();
    let c_config = config(Some(2), &clock, &c_ledger);
    let gate = store.hold(Op::Get, Some(L0_KEYS.to_string()), Occurrence::Nth(1));

    let e = erasure(store.as_ref(), &clock, &e_config);
    let c = async {
        let id = parked(&gate, Op::Get, L0_KEYS).await;
        clock.set(now_ns + PAST_LEASE_NS);
        store.inner().set_clock_ms(ms(now_ns + PAST_LEASE_NS));
        let outcome = compact_bucket_claimed(store.as_ref(), &clock, &c_config, &bucket())
            .await
            .expect("compaction");
        assert!(gate.release(id), "the parked data GET was released");
        outcome
    };
    let (e_outcome, c_outcome) = tokio::join!(e, c);

    assert!(
        matches!(
            c_outcome,
            ClaimedCompaction::Ran(CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            })
        ),
        "C steals the expired claim and publishes: {c_outcome:?}"
    );
    assert_eq!(
        e_outcome,
        ErasureRewriteOutcome::Rewritten {
            parts: 0,
            publish: PublishOutcome::Abandoned,
            abandoned: Some(ErasureAbandon::ClaimLost {
                at: Checkpoint::Publish
            }),
            claim: Some(ClaimAcquisition { stolen: false }),
        },
        "E publishes nothing, and says it lost the claim it took fresh"
    );
    assert_eq!(
        record_sets(store.as_ref()).await,
        (1, 0),
        "only the compaction's record set is in the bucket"
    );
    assert_eq!(
        e_ledger.report().list.requests,
        0,
        "E stopped at its claim checkpoint, before its re-list"
    );
    assert_eq!(
        e_ledger.report().coordinate.requests,
        2,
        "E's acquisition, then the renewal that found its claim stolen"
    );
    assert_eq!(
        c_ledger.report().coordinate.requests,
        5,
        "C took the bucket claim although the bucket is far below the cost gate"
    );
}

/// A claim held by one pass makes the other back off, in both directions,
/// with no clock movement: the holder is live.
///
/// First, compaction A holds the claim, parked at its first part PUT, and an
/// erasure rewrite backs off with nothing built (`Rewritten { parts: 0,
/// publish: Abandoned }`, its refused CreateIfAbsent plus one GET and one
/// HEAD). Then, on a fresh bucket, erasure E holds the claim, parked at its
/// first L0 data GET, and a compaction backs off (`SkippedClaimed`,
/// `HeldByAnother`). Each holder then publishes.
///
/// Shown failing against the pre-fix code: the erasure rewrite took no claim,
/// so the first half publishes a rewrite record beside A's compaction record,
/// and E holds nothing in the second half, so the compaction runs (and the
/// pre-fix cost gate would have left it unclaimed anyway). The removed lines
/// are the `claim_bucket(store, config, bucket, "erasure_rewrite")` call for
/// both halves, and for the second also the cost-gate term in
/// `claim_guard::claims_bucket`.
#[tokio::test]
async fn a_claim_held_by_one_pass_makes_the_other_back_off() {
    let now_ns = sealed_now_ns();

    // A compaction holds the claim; the erasure rewrite backs off.
    let store = seeded_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let a_ledger = RequestLedger::new();
    let a_config = config(Some(1), &clock, &a_ledger);
    let e_ledger = RequestLedger::new();
    let e_config = config(Some(2), &clock, &e_ledger);
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));
    let a = async {
        compact_bucket_claimed(store.as_ref(), &clock, &a_config, &bucket())
            .await
            .expect("compaction")
    };
    let e = async {
        let id = parked(&gate, Op::Put, PART_KEYS).await;
        let outcome = erasure(store.as_ref(), &clock, &e_config).await;
        let between = record_sets(store.as_ref()).await;
        assert!(gate.release(id));
        (outcome, between)
    };
    let (a_outcome, (e_outcome, between)) = tokio::join!(a, e);
    assert_eq!(
        e_outcome,
        ErasureRewriteOutcome::Rewritten {
            parts: 0,
            publish: PublishOutcome::Abandoned,
            abandoned: Some(ErasureAbandon::ClaimHeld {
                reason: ClaimSkipReason::HeldByAnother
            }),
            claim: None,
        },
        "the erasure rewrite backs off while the compaction holds the claim"
    );
    assert_eq!(
        e_ledger.report().coordinate.requests,
        3,
        "the refused CreateIfAbsent, then one GET and one HEAD of the live claim"
    );
    assert_eq!(e_ledger.report().part_put.requests, 0, "E built nothing");
    assert_eq!(between, (0, 0), "nothing was published while A was parked");
    assert!(
        matches!(
            a_outcome,
            ClaimedCompaction::Ran(CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            })
        ),
        "the holder publishes: {a_outcome:?}"
    );
    assert_eq!(record_sets(store.as_ref()).await, (1, 0));

    // An erasure rewrite holds the claim; the compaction backs off.
    let store = seeded_store(now_ns).await;
    let e_ledger = RequestLedger::new();
    let e_config = config(Some(3), &clock, &e_ledger);
    let c_ledger = RequestLedger::new();
    let c_config = config(Some(4), &clock, &c_ledger);
    let gate = store.hold(Op::Get, Some(L0_KEYS.to_string()), Occurrence::Nth(1));
    let e = erasure(store.as_ref(), &clock, &e_config);
    let c = async {
        let id = parked(&gate, Op::Get, L0_KEYS).await;
        let outcome = compact_bucket_claimed(store.as_ref(), &clock, &c_config, &bucket())
            .await
            .expect("compaction");
        let between = record_sets(store.as_ref()).await;
        assert!(gate.release(id));
        (outcome, between)
    };
    let (e_outcome, (c_outcome, between)) = tokio::join!(e, c);
    match c_outcome {
        ClaimedCompaction::SkippedClaimed(skip) => {
            assert_eq!(skip.reason, ClaimSkipReason::HeldByAnother);
            assert_eq!(skip.holder_process_id, Some(Uuid::from_u128(3)));
        }
        other => panic!("the compaction must back off: {other:?}"),
    }
    assert_eq!(
        c_ledger.report().catalog_read.requests,
        0,
        "C merged nothing"
    );
    assert_eq!(between, (0, 0), "nothing was published while E was parked");
    assert!(
        matches!(
            e_outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "the holder publishes: {e_outcome:?}"
    );
    assert_eq!(record_sets(store.as_ref()).await, (0, 1));
    assert_eq!(
        list_all(store.as_ref(), COMPACTION_CLAIMS_PREFIX)
            .await
            .expect("list claims")
            .len(),
        1,
        "both passes contended for the one claim object the bucket's work id names"
    );
}

/// The pre-publish re-list aborts when the record set changed, for each pass,
/// with no claim in play: neither caller installs a participant, which is the
/// case where the re-list is the whole fence.
///
/// First, a compaction through the unclaimed `compact_bucket` entry point is
/// parked at its first part PUT while an erasure rewrite publishes; on resuming
/// it re-lists, finds the rewrite record, and reports `RewritePresent` with no
/// record PUT. Then an erasure rewrite is parked at its first L0 data GET while
/// a compaction publishes; on resuming it re-lists (one LIST on its ledger),
/// finds the compaction record, and publishes nothing.
///
/// Shown failing against the pre-fix code, which had no re-list: the first
/// half publishes a compaction record beside the rewrite record
/// (`Compacted { publish: Published }`), the second a rewrite record beside the
/// compaction record. The removed lines are the `relist_changed` call in
/// `rewrite_and_publish_guarded` (first half) and the `relist_changed` call in
/// `build_and_publish_rewrite` (second half).
#[tokio::test]
async fn the_pre_publish_relist_aborts_when_the_record_set_changed() {
    let now_ns = sealed_now_ns();
    let clock = FixedClock::new(now_ns);

    // The compaction's re-list.
    let store = seeded_store(now_ns).await;
    let c_ledger = RequestLedger::new();
    let c_config = config(None, &clock, &c_ledger);
    let e_ledger = RequestLedger::new();
    let e_config = config(None, &clock, &e_ledger);
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));
    let c = async {
        compact_bucket(store.as_ref(), &clock, &c_config, &bucket())
            .await
            .expect("compaction")
    };
    let e = async {
        let id = parked(&gate, Op::Put, PART_KEYS).await;
        let outcome = erasure(store.as_ref(), &clock, &e_config).await;
        assert!(gate.release(id));
        outcome
    };
    let (c_outcome, e_outcome) = tokio::join!(c, e);
    assert!(
        matches!(
            e_outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{e_outcome:?}"
    );
    assert_eq!(
        c_outcome,
        CompactionOutcome::RewritePresent,
        "the re-list found the rewrite record and the compaction refused it"
    );
    let c_report = c_ledger.report();
    assert_eq!(
        c_report.list.requests, 2,
        "the planning LIST and the re-list"
    );
    assert_eq!(c_report.publish.requests, 0, "no record PUT");
    assert_eq!(c_report.coordinate.requests, 0, "no claim was involved");
    assert_eq!(record_sets(store.as_ref()).await, (0, 1));

    // The erasure rewrite's re-list.
    let store = seeded_store(now_ns).await;
    let e_ledger = RequestLedger::new();
    let e_config = config(None, &clock, &e_ledger);
    let c_ledger = RequestLedger::new();
    let c_config = config(None, &clock, &c_ledger);
    let gate = store.hold(Op::Get, Some(L0_KEYS.to_string()), Occurrence::Nth(1));
    let e = erasure(store.as_ref(), &clock, &e_config);
    let c = async {
        let id = parked(&gate, Op::Get, L0_KEYS).await;
        let outcome = compact_bucket(store.as_ref(), &clock, &c_config, &bucket())
            .await
            .expect("compaction");
        assert!(gate.release(id));
        outcome
    };
    let (e_outcome, c_outcome) = tokio::join!(e, c);
    assert!(
        matches!(
            c_outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{c_outcome:?}"
    );
    assert_eq!(
        e_outcome,
        ErasureRewriteOutcome::Rewritten {
            parts: 0,
            publish: PublishOutcome::Abandoned,
            abandoned: Some(ErasureAbandon::RecordSetChanged),
            claim: None,
        },
        "the re-list found the compaction record and the rewrite published nothing"
    );
    let e_report = e_ledger.report();
    assert_eq!(e_report.list.requests, 1, "the re-list ran");
    assert_eq!(e_report.coordinate.requests, 0, "no claim was involved");
    assert_eq!(record_sets(store.as_ref()).await, (1, 0));
}

/// A stale unreadable claim holds the bucket against both passes: neither runs
/// unclaimed past it, since a pass without the claim would publish unfenced.
///
/// Shown failing against the pre-fix compaction path, where
/// `Acquire::Unclaimed` ran the bucket unclaimed (`Ran(Compacted { .. })`).
/// The removed line is the `Acquire::Unclaimed { key } =>` arm of
/// `claim_guard::claim_bucket` that builds the `UnreadableClaim` skip.
#[tokio::test]
async fn a_stale_unreadable_claim_holds_the_bucket_against_both_passes() {
    use bytes::Bytes;
    use ravel_fleet::claim::{WorkIdentity, compaction_claim_key};
    use ravel_object_store::PutOptions;

    let now_ns = sealed_now_ns();
    let store = seeded_store(now_ns).await;
    let b = bucket();
    let claim_key = compaction_claim_key(
        &WorkIdentity::new(b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket).work_id(),
    );
    store
        .put(
            &claim_key,
            Bytes::from_static(b"\xff\xff not a claim"),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("seed an unreadable claim");
    // Well past one lease plus any jitter on both the store's and the passes'
    // clocks.
    let later_ns = now_ns + 10 * PAST_LEASE_NS;
    store.inner().set_clock_ms(ms(later_ns));
    let clock = FixedClock::new(later_ns);

    let c_ledger = RequestLedger::new();
    let c_config = config(Some(1), &clock, &c_ledger);
    match compact_bucket_claimed(store.as_ref(), &clock, &c_config, &bucket())
        .await
        .expect("compaction")
    {
        ClaimedCompaction::SkippedClaimed(skip) => {
            assert_eq!(skip.reason, ClaimSkipReason::UnreadableClaim);
            assert_eq!(
                skip.reschedule_after_unix_ms,
                i64::try_from(ms(later_ns)).expect("ms") + 3_000,
                "retried one lease later"
            );
        }
        other => panic!("the compaction must back off: {other:?}"),
    }

    let e_ledger = RequestLedger::new();
    let e_config = config(Some(2), &clock, &e_ledger);
    assert_eq!(
        erasure(store.as_ref(), &clock, &e_config).await,
        ErasureRewriteOutcome::Rewritten {
            parts: 0,
            publish: PublishOutcome::Abandoned,
            abandoned: Some(ErasureAbandon::ClaimHeld {
                reason: ClaimSkipReason::UnreadableClaim
            }),
            claim: None,
        }
    );
    assert_eq!(record_sets(store.as_ref()).await, (0, 0));
    assert_eq!(
        store
            .get(&claim_key, ravel_object_store::GetRange::Full)
            .await
            .expect("claim")
            .data,
        Bytes::from_static(b"\xff\xff not a claim"),
        "the unreadable claim is left in place"
    );
}

/// A run past its `max_compaction_lifetime` deadline publishes nothing and says
/// the deadline was why, not the claim: it took the claim fresh and still held
/// it.
///
/// Fails if the deadline arm of `build_and_publish_rewrite` (the
/// `then_some(ErasureAbandon::Deadline)`) is removed: `abandoned` is then
/// `None` beside an abandoned publish.
#[tokio::test]
async fn an_erasure_rewrite_past_its_deadline_reports_the_deadline() {
    let now_ns = sealed_now_ns();
    let store = seeded_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let ledger = RequestLedger::new();
    let e_config = CompactorConfig {
        max_compaction_lifetime_ns: -1,
        ..config(Some(1), &clock, &ledger)
    };

    let outcome = erasure(store.as_ref(), &clock, &e_config).await;
    match outcome {
        ErasureRewriteOutcome::Rewritten {
            parts,
            publish: PublishOutcome::Abandoned,
            abandoned: Some(ErasureAbandon::Deadline),
            claim: Some(ClaimAcquisition { stolen: false }),
        } => assert!(parts > 0, "the build ran before the deadline check"),
        other => panic!("the run must abandon at its deadline: {other:?}"),
    }
    assert_eq!(record_sets(store.as_ref()).await, (0, 0));
    assert_eq!(ledger.report().publish.requests, 0, "no record PUT");
}

/// `maintain migrate`'s per-bucket publish is fenced like a compaction's: an
/// erasure rewrite that publishes while a migration of the same bucket is
/// parked at its first part PUT makes the migration publish nothing.
///
/// Migration M claims the bucket and is parked at its first part PUT. The
/// lease expires; erasure E, a second process, steals the claim and publishes
/// its rewrite record. M resumes; its next checkpoint renews, finds the claim
/// stolen, and M cancels there.
///
/// Shown failing against the pre-fix code, where the migration took no claim
/// and did not re-list: M publishes a compaction record from the unerased
/// inputs beside E's rewrite record (`Rewritten { publish: Published }`,
/// record sets `(1, 1)`). With only the `claim_bucket(store, config, bucket,
/// "migrate")` call in `migrate_bucket_format_scoped` removed, E creates the
/// claim fresh rather than stealing it, and M is stopped by its re-list
/// instead (`RewritePresent`), which fails both the steal and the `Cancelled`
/// assertions: this test pins the claim, the next pins the re-list.
#[tokio::test]
async fn an_erasure_rewrite_that_publishes_mid_migration_cancels_the_migration() {
    let now_ns = sealed_now_ns();
    let store = seeded_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let m_ledger = RequestLedger::new();
    let m_config = config(Some(1), &clock, &m_ledger);
    let e_ledger = RequestLedger::new();
    let e_config = config(Some(2), &clock, &e_ledger);
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));

    let m = async {
        migrate_bucket_format(store.as_ref(), &clock, &m_config, &bucket(), u32::MAX)
            .await
            .expect("migration")
    };
    let e = async {
        let id = parked(&gate, Op::Put, PART_KEYS).await;
        clock.set(now_ns + PAST_LEASE_NS);
        store.inner().set_clock_ms(ms(now_ns + PAST_LEASE_NS));
        let outcome = erasure(store.as_ref(), &clock, &e_config).await;
        assert!(gate.release(id), "the parked part PUT was released");
        outcome
    };
    let (m_outcome, e_outcome) = tokio::join!(m, e);

    assert!(
        matches!(
            e_outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                claim: Some(ClaimAcquisition { stolen: true }),
                ..
            }
        ),
        "E steals the expired claim and publishes: {e_outcome:?}"
    );
    assert_eq!(
        m_outcome,
        MigrateOutcome::Cancelled {
            at: Checkpoint::PartBoundary
        },
        "M cancels on its stolen claim"
    );
    assert_eq!(
        record_sets(store.as_ref()).await,
        (0, 1),
        "exactly the erasure rewrite's record and no compaction record"
    );
    assert_eq!(m_ledger.report().publish.requests, 0, "M PUT no record");
}

/// The migration's pre-publish re-list is its whole fence when it takes no
/// claim (`ravel-cli maintain migrate --no-claim`, or no participant): parked
/// at its first part PUT while an erasure rewrite publishes, it re-lists on
/// resuming, finds the rewrite record, and reports `RewritePresent` with no
/// record PUT.
///
/// Shown failing against the pre-fix code, where `migrate_bucket_format` did
/// not re-list: M publishes a compaction record beside the rewrite record. The
/// removed line is the `Some(planned)` that `dispatch_rewrite` passes to
/// `load_then_rewrite` (pass `None` and M publishes).
#[tokio::test]
async fn the_migration_relist_aborts_when_an_erasure_rewrite_published() {
    let now_ns = sealed_now_ns();
    let store = seeded_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let m_ledger = RequestLedger::new();
    let m_config = config(None, &clock, &m_ledger);
    let e_ledger = RequestLedger::new();
    let e_config = config(None, &clock, &e_ledger);
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));

    let m = async {
        migrate_bucket_format(store.as_ref(), &clock, &m_config, &bucket(), u32::MAX)
            .await
            .expect("migration")
    };
    let e = async {
        let id = parked(&gate, Op::Put, PART_KEYS).await;
        let outcome = erasure(store.as_ref(), &clock, &e_config).await;
        assert!(gate.release(id));
        outcome
    };
    let (m_outcome, e_outcome) = tokio::join!(m, e);

    assert!(
        matches!(
            e_outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "{e_outcome:?}"
    );
    assert_eq!(
        m_outcome,
        MigrateOutcome::RewritePresent,
        "the re-list found the rewrite record and the migration refused it"
    );
    let m_report = m_ledger.report();
    assert_eq!(
        m_report.list.requests, 2,
        "the planning LIST and the re-list"
    );
    assert_eq!(m_report.publish.requests, 0, "no record PUT");
    assert_eq!(m_report.coordinate.requests, 0, "no claim was involved");
    assert_eq!(
        record_sets(store.as_ref()).await,
        (0, 1),
        "exactly the erasure rewrite's record and no compaction record"
    );
}

/// A migration backs off a bucket whose claim an erasure rewrite holds, with
/// nothing built, and the holder then publishes.
///
/// Fails if the `BucketClaim::Skipped` arm of `migrate_bucket_format_scoped`
/// is replaced by running unclaimed: the migration then builds parts and
/// publishes a compaction record while E is parked.
#[tokio::test]
async fn a_claim_held_by_an_erasure_rewrite_makes_the_migration_back_off() {
    let now_ns = sealed_now_ns();
    let store = seeded_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let e_ledger = RequestLedger::new();
    let e_config = config(Some(1), &clock, &e_ledger);
    let m_ledger = RequestLedger::new();
    let m_config = config(Some(2), &clock, &m_ledger);
    let gate = store.hold(Op::Get, Some(L0_KEYS.to_string()), Occurrence::Nth(1));

    let e = erasure(store.as_ref(), &clock, &e_config);
    let m = async {
        let id = parked(&gate, Op::Get, L0_KEYS).await;
        let outcome = migrate_bucket_format(store.as_ref(), &clock, &m_config, &bucket(), u32::MAX)
            .await
            .expect("migration");
        let between = record_sets(store.as_ref()).await;
        assert!(gate.release(id));
        (outcome, between)
    };
    let (e_outcome, (m_outcome, between)) = tokio::join!(e, m);

    assert_eq!(
        m_outcome,
        MigrateOutcome::SkippedClaimed {
            reason: ClaimSkipReason::HeldByAnother
        }
    );
    assert_eq!(m_ledger.report().part_put.requests, 0, "M built nothing");
    assert_eq!(between, (0, 0), "nothing was published while E was parked");
    assert!(
        matches!(
            e_outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "the holder publishes: {e_outcome:?}"
    );
    assert_eq!(record_sets(store.as_ref()).await, (0, 1));
}
