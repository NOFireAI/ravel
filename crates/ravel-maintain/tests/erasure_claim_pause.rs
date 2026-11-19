//! A claimed compaction paused inside its record PUT while an erasure rewrite
//! steals the expired claim and publishes the same bucket (issue #4449).
//!
//! Both passes run with claims on: one participant per process, a 300 s lease
//! and test clocks. The fence (ADR-1029, the 2026-10-03 amendment) is the
//! claim, consulted at the compaction's checkpoints, and the pre-publish
//! re-list. A pause that lands after both, between the re-list and the record
//! PUT reaching the store, is caught by neither: the compaction record and the
//! rewrite record have different create-only keys.
//!
//! Each test parks the compaction with a `FaultStore` hold gate, which blocks
//! the matching call before it reaches the inner store, and proves the hold
//! fired from the gate's held-call registry. No test sleeps.
//!
//! Contract checked on the final state, with no `.dreq` in the store (the
//! state once the request is retired): the bucket holds one live record set,
//! the catalog serves exactly the rewrite record's parts, the erased series is
//! not served, and no sample is served from two segments.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use common::*;
use prost::Message;
use ravel_commit::{keys, signal};
use ravel_maintain::claim_guard::ClaimSleeper;
use ravel_maintain::{
    Checkpoint, ClaimAcquisition, ClaimParticipant, ClaimedCompaction, Clock, CompactionOutcome,
    CompactorConfig, Coordination, ErasureRewriteOutcome, FixedClock, MaintainMemo, NoLeases,
    PendingErasureRequest, PublishOutcome, RequestLedger, compact_bucket_claimed,
    erasure_rewrite_bucket, read,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest, RewriteRecord};
use ravel_types::{Signal, TimeRange};
use uuid::Uuid;

/// The shipped default lease.
const LEASE: Duration = Duration::from_secs(300);

/// Two leases: past the lease and any steal jitter on every clock a test moves.
const PAST_LEASE_NS: i64 = 600 * 1_000_000_000;

/// The key fragment of the compaction record (`.../l1.<hash>.cmt`). Part keys
/// sit under `/l1/` and never contain it.
const RECORD_KEYS: &str = "/l1.";

/// The key fragment of every L1 part object.
const PART_KEYS: &str = "/l1/";

struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _duration: Duration) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

fn config(process: u128, clock: &FixedClock, ledger: &RequestLedger) -> CompactorConfig {
    CompactorConfig {
        coordination: Coordination::On,
        claim_lease_duration: LEASE,
        claim_participant: Some(
            ClaimParticipant::new(
                Uuid::from_u128(process),
                Arc::new(clock.clone()) as Arc<dyn Clock>,
            )
            .with_sleeper(Arc::new(NoWait)),
        ),
        request_ledger: Some(ledger.clone()),
        ..CompactorConfig::default()
    }
}

fn base_ns() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn ms(ns: i64) -> u64 {
    u64::try_from(ns / 1_000_000).expect("positive instant")
}

/// A two-input sealed metrics bucket: `keep` in the first input, the subject
/// `victim` in both.
async fn seeded_store(now_ns: i64) -> Arc<FaultStore<MemoryStore>> {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    store.inner().set_clock_ms(ms(now_ns));
    let b = base_ns();
    for spec in [
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![
                raw_series("keep", &[("k", "a")], &[(b + 1_000, 1.0), (b + 2_000, 2.0)]),
                raw_series("victim", &[("k", "b")], &[(b + 1_000, 5.0)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("victim", &[("k", "b")], &[(b + 3_000, 3.0)])],
        ),
    ] {
        seed_input(store.as_ref(), &spec).await;
    }
    store
}

/// A windowless erasure request for every series named `victim`. It is handed
/// to the pass directly and never written to the store.
fn pending() -> Vec<PendingErasureRequest> {
    let request_id = Uuid::from_u128(0x4449);
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
) -> ravel_maintain::Result<ErasureRewriteOutcome> {
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
}

/// Wait until `gate` parks exactly one call, check it is a PUT on a key
/// containing `fragment` and ending `suffix`, and return its id.
async fn parked(gate: &GateHandle, fragment: &str, suffix: &str) -> u64 {
    gate.wait_until_held(1).await;
    let held = gate.held_details();
    assert_eq!(held.len(), 1, "the gate parked exactly one call: {held:?}");
    let (id, op, key) = &held[0];
    assert_eq!(*op, Op::Put, "the parked call is a PUT: {key}");
    assert!(
        key.contains(fragment) && key.ends_with(suffix),
        "the parked call is on {fragment}..{suffix}: {key}"
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

/// Every breach of the contract in the bucket's final state, empty when it
/// holds. Each observation is printed first so a failing run shows all of
/// them, not only the first.
async fn contract_violations(store: &Arc<FaultStore<MemoryStore>>, now_ns: i64) -> Vec<String> {
    let mut violations = Vec::new();
    let listing = read::list_bucket(store.as_ref(), &bucket())
        .await
        .expect("list");
    eprintln!(
        "records: compaction={:?} rewrite={:?}",
        listing.compaction_record_keys, listing.rewrite_record_keys
    );
    let live = listing.compaction_record_keys.len() + listing.rewrite_record_keys.len();
    if live > 1 {
        violations.push(format!(
            "{live} live record sets in one bucket: compaction {:?}, rewrite {:?}",
            listing.compaction_record_keys, listing.rewrite_record_keys
        ));
    }

    let mut rewrite_parts = BTreeSet::new();
    for key in &listing.rewrite_record_keys {
        let record = RewriteRecord::decode(get_full(store.as_ref(), key).await.as_ref())
            .expect("decode rewrite record");
        for part in &record.parts {
            rewrite_parts
                .insert(keys::reconstruct_rewrite_part_key(&record, part).expect("part key"));
        }
    }

    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = ravel_catalog::Catalog::new(
        dyn_store,
        ravel_catalog::CatalogConfig {
            shard_count: SHARD + 1,
            ..Default::default()
        },
    )
    .expect("catalog");
    let snapshot = catalog
        .resolve(
            &tenant_hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: base_ns(),
                end_ns: base_ns() + NS_PER_HOUR,
            },
            &[],
            now_ns,
        )
        .await
        .expect("resolve");
    let served: BTreeSet<String> = snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect();
    eprintln!(
        "served segments ({} pruned, {} pending erasures): {served:?}",
        snapshot.segments_pruned,
        snapshot.pending_erasure.len()
    );
    assert!(
        snapshot.pending_erasure.is_empty(),
        "no .dreq is in the store, so nothing masks the served rows"
    );
    if served != rewrite_parts {
        violations.push(format!(
            "served {served:?}, not exactly the rewrite record's parts {rewrite_parts:?}"
        ));
    }

    let victim = series_id("victim", &[("k", "b")]).0;
    let keep = series_id("keep", &[("k", "a")]).0;
    let mut seen: BTreeMap<([u8; 16], i64, u64), usize> = BTreeMap::new();
    for key in &served {
        for (id, samples) in read_scalar_samples(&get_full(store.as_ref(), key).await) {
            for (ts, bits) in samples {
                *seen.entry((id, ts, bits)).or_default() += 1;
            }
        }
    }
    let name = |id: &[u8; 16]| match *id {
        x if x == victim => "victim",
        x if x == keep => "keep",
        _ => "other",
    };
    let rendered: Vec<String> = seen
        .iter()
        .map(|((id, ts, bits), n)| {
            format!(
                "{}@{} = {} x{n}",
                name(id),
                ts - base_ns(),
                f64::from_bits(*bits)
            )
        })
        .collect();
    eprintln!("served samples: {rendered:?}");
    let victim_served: Vec<&String> = seen
        .keys()
        .zip(&rendered)
        .filter(|((id, _, _), _)| *id == victim)
        .map(|(_, r)| r)
        .collect();
    if !victim_served.is_empty() {
        violations.push(format!("the erased series is served: {victim_served:?}"));
    }
    let twice: Vec<&String> = seen
        .values()
        .zip(&rendered)
        .filter(|(n, _)| **n > 1)
        .map(|(_, r)| r)
        .collect();
    if !twice.is_empty() {
        violations.push(format!("samples served more than once: {twice:?}"));
    }
    let kept: BTreeSet<i64> = seen
        .keys()
        .filter(|(id, _, _)| *id == keep)
        .map(|(_, ts, _)| ts - base_ns())
        .collect();
    if kept != BTreeSet::from([1_000, 2_000]) {
        violations.push(format!(
            "the kept series is served at {kept:?}, not [1000, 2000]"
        ));
    }
    violations
}

/// Compaction A takes the claim, builds, passes every checkpoint and its
/// re-list, and is paused inside its record PUT before the PUT reaches the
/// store. Both clocks move two leases. Erasure E, a second process, steals the
/// expired claim and publishes its rewrite record. A resumes and its PUT
/// lands under its own create-only key.
#[tokio::test]
#[ignore = "suspected defect #4449: a claimed compaction paused between its re-list and its \
            record PUT publishes beside an erasure rewrite that stole its expired claim, and the \
            catalog serves the erased series"]
async fn a_compaction_paused_inside_its_record_put_publishes_beside_the_erasure_rewrite() {
    let now_ns = sealed_now_ns();
    let store = seeded_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let a_ledger = RequestLedger::new();
    let a_config = config(1, &clock, &a_ledger);
    let e_ledger = RequestLedger::new();
    let e_config = config(2, &clock, &e_ledger);
    let gate = store.hold(Op::Put, Some(RECORD_KEYS.to_string()), Occurrence::Nth(1));

    let a = async {
        compact_bucket_claimed(store.as_ref(), &clock, &a_config, &bucket())
            .await
            .expect("compaction")
    };
    let e = async {
        let id = parked(&gate, RECORD_KEYS, ".cmt").await;
        let at_park = a_ledger.report();
        let between = record_sets(store.as_ref()).await;
        clock.set(now_ns + PAST_LEASE_NS);
        store.inner().set_clock_ms(ms(now_ns + PAST_LEASE_NS));
        let outcome = erasure(store.as_ref(), &clock, &e_config)
            .await
            .expect("erasure rewrite");
        assert!(gate.release(id), "the parked record PUT was released");
        (outcome, at_park, between)
    };
    let (a_outcome, (e_outcome, at_park, between)) = tokio::join!(a, e);

    eprintln!("A outcome: {a_outcome:?}");
    eprintln!("E outcome: {e_outcome:?}");
    eprintln!(
        "A at park: list={} publish={} coordinate={}; record sets then {between:?}",
        at_park.list.requests, at_park.publish.requests, at_park.coordinate.requests
    );
    eprintln!(
        "record sets after both: {:?}",
        record_sets(store.as_ref()).await
    );

    // The setup: A was past its re-list with no record PUT counted, held the
    // claim, and E stole it.
    assert_eq!(
        at_park.list.requests, 2,
        "the planning LIST and the re-list"
    );
    assert_eq!(at_park.publish.requests, 0, "the record PUT had not landed");
    assert!(at_park.coordinate.requests > 0, "A took the claim");
    assert_eq!(between, (0, 0), "nothing was published when A parked");
    assert!(
        matches!(
            e_outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                abandoned: None,
                claim: Some(ClaimAcquisition { stolen: true }),
                ..
            }
        ),
        "E steals the expired claim and publishes: {e_outcome:?}"
    );

    let violations = contract_violations(&store, now_ns + PAST_LEASE_NS).await;

    let rerun_clock = FixedClock::new(now_ns + 2 * PAST_LEASE_NS);
    store.inner().set_clock_ms(ms(now_ns + 2 * PAST_LEASE_NS));
    let rerun_ledger = RequestLedger::new();
    let rerun = erasure(
        store.as_ref(),
        &rerun_clock,
        &config(3, &rerun_clock, &rerun_ledger),
    )
    .await;
    eprintln!("erasure rerun: {rerun:?}");

    assert!(
        violations.is_empty(),
        "contract broken with claims on (A: {a_outcome:?}):\n{}",
        violations.join("\n")
    );
}

/// Control: A is paused at its part PUT, before its last two checkpoints and
/// its re-list, on a clock that moves with E's. Its next checkpoint renews,
/// finds the claim stolen, and A cancels with nothing published.
#[tokio::test]
async fn control_a_compaction_paused_before_its_checkpoints_cancels_on_the_stolen_claim() {
    let now_ns = sealed_now_ns();
    let store = seeded_store(now_ns).await;
    let clock = FixedClock::new(now_ns);
    let a_ledger = RequestLedger::new();
    let a_config = config(1, &clock, &a_ledger);
    let e_ledger = RequestLedger::new();
    let e_config = config(2, &clock, &e_ledger);
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));

    let a = async {
        compact_bucket_claimed(store.as_ref(), &clock, &a_config, &bucket())
            .await
            .expect("compaction")
    };
    let e = async {
        let id = parked(&gate, PART_KEYS, "").await;
        clock.set(now_ns + PAST_LEASE_NS);
        store.inner().set_clock_ms(ms(now_ns + PAST_LEASE_NS));
        let outcome = erasure(store.as_ref(), &clock, &e_config)
            .await
            .expect("erasure rewrite");
        assert!(gate.release(id), "the parked part PUT was released");
        outcome
    };
    let (a_outcome, e_outcome) = tokio::join!(a, e);
    eprintln!("A outcome: {a_outcome:?}");
    eprintln!("E outcome: {e_outcome:?}");

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
    assert_eq!(a_ledger.report().publish.requests, 0, "A PUT no record");
    assert_eq!(
        a_ledger.report().list.requests,
        1,
        "A stopped before its re-list"
    );
    let violations = contract_violations(&store, now_ns + PAST_LEASE_NS).await;
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}

/// Control for the lease/3 renewal cadence: A is paused at its part PUT, and
/// its own clock does not move, so neither of its remaining checkpoints comes
/// due for a renewal and neither makes a store request. The re-list after them
/// finds E's rewrite record and A publishes nothing.
#[tokio::test]
async fn control_a_compaction_whose_checkpoints_do_not_renew_is_stopped_by_its_relist() {
    let now_ns = sealed_now_ns();
    let store = seeded_store(now_ns).await;
    let a_clock = FixedClock::new(now_ns);
    let e_clock = FixedClock::new(now_ns);
    let a_ledger = RequestLedger::new();
    let a_config = config(1, &a_clock, &a_ledger);
    let e_ledger = RequestLedger::new();
    let e_config = config(2, &e_clock, &e_ledger);
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));

    let a = async {
        compact_bucket_claimed(store.as_ref(), &a_clock, &a_config, &bucket())
            .await
            .expect("compaction")
    };
    let e = async {
        let id = parked(&gate, PART_KEYS, "").await;
        let coordinate_at_park = a_ledger.report().coordinate.requests;
        e_clock.set(now_ns + PAST_LEASE_NS);
        store.inner().set_clock_ms(ms(now_ns + PAST_LEASE_NS));
        let outcome = erasure(store.as_ref(), &e_clock, &e_config)
            .await
            .expect("erasure rewrite");
        assert!(gate.release(id), "the parked part PUT was released");
        (outcome, coordinate_at_park)
    };
    let (a_outcome, (e_outcome, coordinate_at_park)) = tokio::join!(a, e);
    eprintln!("A outcome: {a_outcome:?}");
    eprintln!("E outcome: {e_outcome:?}");

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
    assert!(
        matches!(
            a_outcome,
            ClaimedCompaction::Ran(CompactionOutcome::RewritePresent)
        ),
        "A's re-list found the rewrite record: {a_outcome:?}"
    );
    let report = a_ledger.report();
    assert_eq!(report.list.requests, 2, "the planning LIST and the re-list");
    assert_eq!(report.publish.requests, 0, "A PUT no record");
    assert_eq!(
        report.coordinate.requests,
        coordinate_at_park + 1,
        "no checkpoint renewed after the pause; the one request is the completion CAS"
    );
    let violations = contract_violations(&store, now_ns + PAST_LEASE_NS).await;
    assert!(violations.is_empty(), "{}", violations.join("\n"));
}
