//! FaultStore crash-matrix coverage for the sweeper's crash-recovery rows, plus the convergence,
//! pinned-query-races-sweep, and horizon-boundary properties. Every fault-injecting test asserts the fault actually fired (via
//! `fault_count`) so it proves its own fault point, per the repo testing
//! conventions.
//!
//! The sweeper is signal-generic, so the shared suite runs once over an RSEG
//! (metrics) fixture, once over an RLOG (logs) fixture, and once over an RSPAN
//! (spans) fixture via [`Sig`] and one seeding helper called three times,
//! never three copies of every test (ADR-0041 phase 4
//! confirms the sweeper needs no spans-specific code). L1 "still intact" is
//! asserted signal-generically by HEADing the compaction record's parts, not by
//! decoding samples, so one assertion serves every format.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use prost::Message;
use ravel_commit::{keys, signal};
use ravel_maintain::{
    Bucket, Clock, CompactionOutcome, CompactorConfig, ErasureRewriteOutcome, FixedClock,
    MaintainMemo, NoLeases, PendingErasureRequest, PublishOutcome, compact_bucket,
    erasure_rewrite_bucket, sweep_orphans, sweep_shard, sweep_shard_zoned, sweep_superseded,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, StoreError, list_all};
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest, RetentionTombstone};
use ravel_types::Signal;
use uuid::Uuid;

/// Which signal fixture a shared test runs over.
#[derive(Debug, Clone, Copy)]
enum Sig {
    Metrics,
    Logs,
    Spans,
}

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

/// Two compactable metrics L0 inputs (the shape crash_matrix.rs uses).
fn metrics_specs() -> Vec<InputSpec> {
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

/// Seed two compactable L0 inputs for `sig` and return the bucket.
async fn seed_two(store: &dyn ObjectStoreBackend, sig: Sig) -> Bucket {
    match sig {
        Sig::Metrics => {
            for s in metrics_specs() {
                seed_input(store, &s).await;
            }
            bucket()
        }
        Sig::Logs => seed_rlog_two_inputs(store).await,
        Sig::Spans => seed_rspan_two_inputs(store).await,
    }
}

/// Seed and compact one bucket at `clock`, leaving the L0 inputs in place
/// (compaction never deletes; the sweep does). Returns the bucket.
async fn seed_and_compact(store: &dyn ObjectStoreBackend, clock: &dyn Clock, sig: Sig) -> Bucket {
    let bucket = seed_two(store, sig).await;
    let outcome = compact_bucket(store, clock, &cfg(), &bucket)
        .await
        .expect("compact");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "expected Compacted, got {outcome:?}"
    );
    bucket
}

/// A `now_ns` comfortably past the compaction record's supersession horizon,
/// given the record was created at `created_ns`.
fn past_horizon(created_ns: i64, config: &CompactorConfig) -> i64 {
    created_ns
        .saturating_add(config.protection_horizon_ns)
        .saturating_add(NS_PER_HOUR)
}

/// The L0 commit-record keys currently in a bucket.
async fn l0_commit_keys(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> Vec<String> {
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    let mut out: Vec<String> = list_all(store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .filter(|k| {
            matches!(
                keys::partition_bucket_entry(k),
                Ok(keys::BucketEntry::CommitRecord(_))
            )
        })
        .collect();
    out.sort();
    out
}

async fn l0_commit_count(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> usize {
    l0_commit_keys(store, bucket).await.len()
}

fn l0_data_prefix(bucket: &Bucket) -> String {
    format!(
        "t/{}/{}/l0/{:04}/",
        bucket.tenant_hash.to_hex(),
        bucket.signal.key_prefix(),
        bucket.shard
    )
}

async fn l0_data_count(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> usize {
    list_all(store, &l0_data_prefix(bucket))
        .await
        .unwrap()
        .len()
}

/// Assert the compaction record and every L1 part it names are still present
/// (signal-generic: HEAD, no decode).
async fn assert_l1_intact(store: &dyn ObjectStoreBackend, bucket: &Bucket) {
    let record = fetch_compaction_record(store, bucket).await;
    assert!(!record.parts.is_empty(), "compaction record has parts");
    for part in &record.parts {
        let key = keys::reconstruct_l1_part_key(&record, part).unwrap();
        store.head(&key).await.expect("L1 part still present");
    }
}

// --- Row 7: sweep with some input records already deleted -------------------

/// Row 7: a prior pass deleted a strict subset of the inputs, so the bucket
/// lists fewer L0 records than the compaction record names. The re-sweep must
/// still converge: every input is in the record's input list, so exclusion
/// never needed the L0 records themselves, and every delete is idempotent.
#[tokio::test]
async fn row7_partial_input_records_deleted_reswept_converges() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_and_compact(&store, &clock, sig).await;

        // Simulate a crash-interrupted prior sweep: one input's commit record
        // is already gone (its data object is now a record-less orphan).
        let inputs = l0_commit_keys(&store, &bucket).await;
        assert_eq!(inputs.len(), 2);
        store.delete(&inputs[0]).await.unwrap();

        clock.set(past_horizon(created, &cfg()));
        // A full shard sweep: superseded removes the surviving input record and
        // both data objects' records-then-data, orphan GC mops up the
        // pre-deleted input's record-less data.
        let report = sweep_shard(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("sweep");
        assert!(
            report.superseded_records_deleted >= 1,
            "the surviving input record is swept: {report:?}"
        );

        assert_eq!(
            l0_commit_count(&store, &bucket).await,
            0,
            "no L0 records left"
        );
        assert_eq!(l0_data_count(&store, &bucket).await, 0, "no L0 data left");
        assert_l1_intact(&store, &bucket).await;

        // Idempotent re-sweep: nothing left to delete.
        let again = sweep_shard(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("re-sweep");
        assert_eq!(
            again,
            ravel_maintain::SweepReport {
                full_pass: true,
                ..ravel_maintain::SweepReport::default()
            },
            "converged: nothing left to delete, but sweep_shard always ran a full pass"
        );
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

// --- Row 8: records deleted, data objects not (orphan GC converges) ---------

/// Row 8: a crash between the records phase and the data phase of the
/// superseded sweep leaves record-less L0 data objects. They are invisible to
/// new snapshots and collected by orphan GC on the next pass. The data-delete
/// fault (scoped to `/l0/`) fires once and aborts the first pass after the
/// records are gone.
#[tokio::test]
async fn row8_records_deleted_data_not_orphan_gc_converges() {
    async fn run(sig: Sig) {
        let inner = MemoryStore::new();
        // Fail the first L0 data-object delete (keys carry "/l0/"); the commit
        // records (under "/c/") are deleted before it, so the pass aborts with
        // record-less data left behind.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Delete, ScriptedFault::Timeout)
                .with_key_contains("/l0/")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(inner, plan);
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_and_compact(&store, &clock, sig).await;

        clock.set(past_horizon(created, &cfg()));
        // Superseded sweep: records deleted, then the data delete faults.
        let err = sweep_superseded(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await;
        assert!(err.is_err(), "the L0 data delete fault aborts the pass");
        assert_eq!(store.fault_count(Op::Delete, FaultKind::Timeout), 1);
        assert_eq!(
            l0_commit_count(&store, &bucket).await,
            0,
            "records were deleted before the fault"
        );
        assert!(
            l0_data_count(&store, &bucket).await > 0,
            "record-less data remains"
        );

        // Next pass: the fault is spent; orphan GC collects the record-less data.
        let report = sweep_shard(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("converging sweep");
        assert!(report.orphans_deleted > 0, "orphan GC ran: {report:?}");
        assert_eq!(l0_data_count(&store, &bucket).await, 0, "orphans gone");
        assert_l1_intact(&store, &bucket).await;
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

// --- Row 8b: locked commit record blocks the record-delete loop -----------

/// Row 8b: a compliance-mode Object Lock retention on a commit record
/// refuses every delete attempt against it, not just the first. The
/// superseded sweep's record-delete loop runs before its data-delete loop
/// (docs/deletion-and-gc.md, docs/object-store-contract.md "Required bucket
/// configuration"), and the record loop covers every cleared group in the
/// pass before the data loop runs at all, so a record that never stops
/// faulting aborts the pass at the record loop and the data loop never runs
/// for any chain in it, so the L0 data is still there afterwards.
///
/// This row pins the whole-pass abort only. It deliberately asserts nothing
/// about the commit-record count: the rule fires on every `/c/` delete and
/// `FaultStore::delete` returns the error without calling the inner store, so
/// "no commit record was deleted" is a restatement of the fixture and no
/// production mutation can falsify it. Row 8c below is the row that reads the
/// record loop's own behaviour, by letting the first delete through.
#[tokio::test]
async fn row8b_locked_commit_record_delete_blocks_before_data_loop() {
    async fn run(sig: Sig) {
        let inner = MemoryStore::new();
        // Always-on: every commit-record delete faults, modeling a retention
        // period that has not elapsed rather than a one-shot transient error.
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(Op::Delete, ScriptedFault::Timeout).with_key_contains("/c/"));
        let store = FaultStore::new(inner, plan);
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_and_compact(&store, &clock, sig).await;

        clock.set(past_horizon(created, &cfg()));
        let before_data = l0_data_count(&store, &bucket).await;
        assert!(before_data > 0, "L0 data exists before the sweep");
        let before_records = l0_commit_count(&store, &bucket).await;
        assert!(
            before_records > 0,
            "L0 commit records exist before the sweep"
        );

        let err = sweep_superseded(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await;
        assert!(
            err.is_err(),
            "a commit record delete that never stops faulting must abort the pass"
        );
        assert!(
            store.fault_count(Op::Delete, FaultKind::Timeout) >= 1,
            "the record-delete fault must have fired"
        );
        assert_eq!(
            l0_data_count(&store, &bucket).await,
            before_data,
            "the data loop must never run while the record loop is still failing"
        );
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

// --- Row 8c: the record loop stops at the first refusal --------------------

/// Seed three compactable L0 inputs for `sig`, one more than [`seed_two`], so
/// a fault on the second commit-record delete still leaves a third record
/// behind it for the loop to reach if it wrongly carried on.
async fn seed_three(store: &dyn ObjectStoreBackend, sig: Sig) -> Bucket {
    match sig {
        Sig::Metrics => {
            for s in metrics_specs() {
                seed_input(store, &s).await;
            }
            seed_input(
                store,
                &InputSpec::new(
                    Uuid::from_u128(3),
                    10,
                    3,
                    vec![raw_series("m", &[("k", "c")], &[(4_000, 4.0)])],
                ),
            )
            .await;
            bucket()
        }
        Sig::Logs => {
            let b = seed_rlog_two_inputs(store).await;
            seed_rlog_input(
                store,
                Uuid::from_u128(3),
                10,
                3,
                &[log_record(0, 25, "echo"), log_record(3, 30, "foxtrot")],
            )
            .await;
            b
        }
        Sig::Spans => {
            let b = seed_rspan_two_inputs(store).await;
            seed_rspan_input(
                store,
                Uuid::from_u128(3),
                10,
                3,
                &[span_record(0, 2, 25, 30), span_record(3, 0, 2, 4)],
            )
            .await;
            b
        }
    }
}

/// Row 8c: the record-delete loop must propagate the first refusal, not
/// accumulate errors and keep deleting. The fixture has three superseded L0
/// commit records and the fault fires on the *second* `/c/` delete, so the
/// first delete really lands in the store and a third record sits behind the
/// refusal. A correct pass deletes exactly one record and then returns the
/// error, leaving the second record, the third record, and all the L0 data
/// untouched.
///
/// This is the row row 8b cannot be. There the fault is always-on and
/// `FaultStore::delete` returns before touching the inner store, so a record
/// count that never moves restates the fixture. Here, a record loop rewritten
/// to collect the error and continue deletes the third record too, and the
/// `before - after == 1` assertion fails whether or not that rewrite still
/// stops short of the data loop.
#[tokio::test]
async fn row8c_record_loop_stops_at_the_first_refused_delete() {
    async fn run(sig: Sig) {
        let inner = MemoryStore::new();
        // Nth(2): the first commit-record delete succeeds, the second refuses,
        // modeling one locked record among several in the same pass.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Delete, ScriptedFault::Timeout)
                .with_key_contains("/c/")
                .with_occurrence(Occurrence::Nth(2)),
        );
        let store = FaultStore::new(inner, plan);
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_three(&store, sig).await;
        let outcome = compact_bucket(&store, &clock, &cfg(), &bucket)
            .await
            .expect("compact");
        assert!(
            matches!(outcome, CompactionOutcome::Compacted { .. }),
            "expected Compacted, got {outcome:?}"
        );

        clock.set(past_horizon(created, &cfg()));
        let before_data = l0_data_count(&store, &bucket).await;
        assert!(before_data > 0, "L0 data exists before the sweep");
        let before_records = l0_commit_count(&store, &bucket).await;
        assert_eq!(
            before_records, 3,
            "the fixture must hold three superseded L0 commit records: the \
             Nth(2) fault has to land inside the record loop with a record \
             still behind it, or a loop that carried on past the refusal would \
             have nothing left to delete and this row would restate row 8b"
        );

        let err = sweep_superseded(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await;
        assert!(
            err.is_err(),
            "a refused record delete must abort the pass, not be collected and \
             skipped"
        );
        assert_eq!(
            store.fault_count(Op::Delete, FaultKind::Timeout),
            1,
            "the Nth(2) record-delete fault must have fired"
        );

        let after_records = l0_commit_count(&store, &bucket).await;
        assert_eq!(
            before_records - after_records,
            1,
            "exactly the one record before the refusal may be gone: the loop \
             must return the error rather than carry on through the rest \
             (before {before_records}, after {after_records})"
        );
        assert_eq!(
            l0_data_count(&store, &bucket).await,
            before_data,
            "the data loop must not run after the record loop refused"
        );
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

// --- Row 8d: a lock on the chain's own record aborts the third loop --------

/// Two metrics L0 inputs carrying a "victim" series an erasure request can
/// match, so a rewrite over the compacted bucket supersedes the compaction
/// record via `superseded_record_key`. That is the only path that puts a
/// chain's own record into `chain_record_keys` (the third delete loop):
/// `gather_superseded_chain` in `crates/ravel-maintain/src/sweep.rs`.
fn erasure_metrics_specs() -> Vec<InputSpec> {
    vec![
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
    ]
}

/// A windowless metrics erasure request matching every series named `metric`,
/// acknowledged at time 0.
fn pending_erasure(seed: u128, metric: &str) -> PendingErasureRequest {
    let request_id = Uuid::from_u128(seed);
    PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id)
            .unwrap(),
        request: ErasureRequest {
            format_version: 1,
            tenant_hash: tenant_hash().0.to_vec(),
            signal: signal::to_proto(Signal::Metrics) as i32,
            request_id: request_id.to_string(),
            created_unix_ns: 0,
            predicate: vec![ErasurePredicateMatcher {
                key: "__name__".to_string(),
                value: metric.to_string(),
            }],
            window_start_ns: 0,
            window_end_ns: 0,
            reason: String::new(),
        },
    }
}

/// The single compaction record key in the metrics test bucket.
async fn compaction_record_key(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> String {
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    for m in list_all(store, &prefix).await.unwrap() {
        if matches!(
            keys::partition_bucket_entry(&m.key),
            Ok(keys::BucketEntry::CompactionRecord(_))
        ) {
            return m.key;
        }
    }
    panic!("no compaction record in the bucket");
}

/// The chain's own records: the `l1.*.cmt` compaction records and `rw.*.cmt`
/// rewrite records in the bucket. These are what the third delete loop removes.
async fn chain_record_count(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> usize {
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    list_all(store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .filter(|m| {
            matches!(
                keys::partition_bucket_entry(&m.key),
                Ok(keys::BucketEntry::CompactionRecord(_))
                    | Ok(keys::BucketEntry::RewriteRecord(_))
            )
        })
        .count()
}

/// Row 8d: a compliance lock on a chain's OWN compaction or rewrite record does
/// not hold the superseded data. `sweep_superseded` runs three delete loops in
/// order (`sweep_superseded_impl`, `crates/ravel-maintain/src/sweep.rs`): every
/// cleared group's input commit records, then every cleared group's data
/// objects, then every cleared group's own chain records, so a rewrite record
/// outlives every input it superseded. By the time a lock on a chain record
/// refuses, the pass has already deleted the input records, the L0 data, and
/// the pre-rewrite L1 parts; the refusal aborts the pass at the third loop, and
/// the chain's own record survives for the next pass to retry.
///
/// The fixture compacts two L0 inputs, then rewrites the compacted bucket so
/// the rewrite record supersedes the compaction record (the
/// `superseded_record_key` path, the only one that populates
/// `chain_record_keys`). The commit keyspace then holds two input commit
/// records (deleted in loop one), one compaction record (deleted in loop
/// three), and the live rewrite record (never deleted); the L0 data and the L1
/// parts carry no `/c/` in their keys. So a `Nth(3)` fault on `/c/` lands on
/// the first (and only) chain-record delete, after loops one and two ran to
/// completion.
///
/// Discrimination: moving the fault to loop one (`Nth(1)`) instead aborts
/// before the data loop, leaving the L0 data in place and failing the
/// `l0_data_count == 0` assertion, so this row separates a third-loop refusal
/// from a first-loop one. Metrics-only, like the rewrite fixtures in
/// `erasure_sweep.rs`: an erasure predicate matching an RLOG or RSPAN series is
/// signal-specific, and the third loop the row exercises is signal-generic.
#[tokio::test]
async fn row8d_locked_chain_record_aborts_after_inputs_and_data_gone() {
    let inner = MemoryStore::new();
    // Nth(3) on `/c/`: loop one deletes the two input commit records, loop two
    // deletes the L0 data and pre-rewrite L1 parts (neither key carries `/c/`),
    // and the third `/c/` delete is the chain's own compaction record in loop
    // three.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Delete, ScriptedFault::Timeout)
            .with_key_contains("/c/")
            .with_occurrence(Occurrence::Nth(3)),
    );
    let store = FaultStore::new(inner, plan);
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let bucket = bucket();

    for spec in erasure_metrics_specs() {
        seed_input(&store, &spec).await;
    }
    let outcome = compact_bucket(&store, &clock, &cfg(), &bucket)
        .await
        .expect("compact");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "expected Compacted, got {outcome:?}"
    );
    let comp_key = compaction_record_key(&store, &bucket).await;

    let mut memo = MaintainMemo::with_default_interval();
    let rewrite = erasure_rewrite_bucket(
        &store,
        &clock,
        &cfg(),
        &NoLeases,
        &bucket,
        &[pending_erasure(42, "victim")],
        &mut memo,
    )
    .await
    .expect("rewrite");
    assert!(
        matches!(rewrite, ErasureRewriteOutcome::Rewritten { .. }),
        "the rewrite must supersede the compaction record, got {rewrite:?}"
    );

    clock.set(past_horizon(created, &cfg()));
    let before_data = l0_data_count(&store, &bucket).await;
    assert!(before_data > 0, "L0 data exists before the sweep");
    assert_eq!(
        l0_commit_count(&store, &bucket).await,
        2,
        "the fixture must hold two superseded L0 input commit records"
    );
    let before_chain = chain_record_count(&store, &bucket).await;
    assert_eq!(
        before_chain, 2,
        "the fixture must hold the compaction record and the live rewrite \
         record; the Nth(3) /c/ fault reaches the compaction record only after \
         the two input records are gone, so the third /c/ delete is a \
         chain-record delete in loop three"
    );
    assert!(
        store.head(&comp_key).await.is_ok(),
        "the superseded compaction record is present before the sweep"
    );

    let err = sweep_superseded(
        &store,
        &clock,
        &cfg(),
        &NoLeases,
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
    )
    .await;
    assert!(
        err.is_err(),
        "a refused chain-record delete must abort the pass"
    );
    assert_eq!(
        store.fault_count(Op::Delete, FaultKind::Timeout),
        1,
        "the Nth(3) chain-record-delete fault must have fired"
    );

    // Loops one and two ran to completion before the third loop refused, so a
    // lock on the chain's own record did not hold the data behind it.
    assert_eq!(
        l0_commit_count(&store, &bucket).await,
        0,
        "the input commit records are gone: loop one completed before the \
         chain-record loop refused"
    );
    assert_eq!(
        l0_data_count(&store, &bucket).await,
        0,
        "the L0 data is gone: loop two completed before the chain-record loop \
         refused. Moving the fault to loop one (Nth(1)) leaves this data in \
         place and fails here, which is what makes the row discriminate the \
         third loop from the first"
    );
    assert_eq!(
        chain_record_count(&store, &bucket).await,
        before_chain,
        "the chain's own record count is unchanged: the refused compaction \
         record survives the retention period for the next pass to retry"
    );
    assert!(
        store.head(&comp_key).await.is_ok(),
        "the superseded compaction record survives the refused delete"
    );
}

// --- Row 9: pinned query outlives horizon, input deleted under it ----------

/// Row 9: a query resolved and pinned a snapshot referencing the L0 inputs;
/// the sweep then removes those inputs past the horizon. A live fetcher's next
/// GET of a pinned L0 object returns NotFound (which the frontend surfaces as
/// SnapshotInvalidated); a single re-resolve then serves the same data from
/// the surviving L1 parts. Modeled at the store level: this crate has no
/// dependency on ravel-query/ravel-catalog, so the NotFound (the
/// SnapshotInvalidated trigger) and the L1 survival (the re-resolve target)
/// are asserted directly.
#[tokio::test]
async fn row9_pinned_query_races_sweep_then_reresolves_against_l1() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_and_compact(&store, &clock, sig).await;

        // Pin the snapshot: capture the L0 data-object keys a resolver would
        // have referenced.
        let inputs = l0_commit_keys(&store, &bucket).await;
        let mut pinned_data_keys = Vec::new();
        for k in &inputs {
            let got = store.get(k, GetRange::Full).await.unwrap();
            let rec = ravel_commit::record::decode(&got.data).unwrap();
            pinned_data_keys.push(keys::reconstruct_data_key(&rec).unwrap());
        }
        // The pinned objects are readable before the sweep.
        for dk in &pinned_data_keys {
            store
                .head(dk)
                .await
                .expect("pinned input present pre-sweep");
        }

        // Sweep past the horizon removes the pinned inputs.
        clock.set(past_horizon(created, &cfg()));
        sweep_shard(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("sweep");

        // The pinned reader's next access sees NotFound (SnapshotInvalidated).
        for dk in &pinned_data_keys {
            assert!(
                matches!(
                    store.get(dk, GetRange::Full).await,
                    Err(StoreError::NotFound)
                ),
                "pinned input gone -> SnapshotInvalidated"
            );
        }
        // The single re-resolve serves the same logical data from L1.
        assert_l1_intact(&store, &bucket).await;
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

// --- Row 12: token GET NotFound post-sweep, served via L1 -------------------

/// Row 12: after the superseded sweep removed an input's commit record, an
/// exact-token GET on that commit key returns NotFound, and the token's writer
/// identity is still named in the compaction record's input list, so the data
/// is served via the record's L1 parts. This is exactly the input to
/// ravel-catalog's `resolve_min_token_fallback`; asserted here at the store
/// level (this crate does not depend on ravel-catalog), with a companion
/// catalog test exercising the real fallback resolve.
#[tokio::test]
async fn row12_token_get_notfound_post_sweep_found_in_input_list() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_and_compact(&store, &clock, sig).await;

        // Capture one input's token identity and its commit key before the sweep.
        let inputs = l0_commit_keys(&store, &bucket).await;
        let victim_key = inputs[0].clone();
        let got = store.get(&victim_key, GetRange::Full).await.unwrap();
        let victim = ravel_commit::record::decode(&got.data).unwrap();
        let token = ravel_commit::record::token_for(&victim).unwrap();

        clock.set(past_horizon(created, &cfg()));
        sweep_superseded(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("superseded sweep");

        // The exact-token GET now returns NotFound.
        assert!(
            matches!(
                store.get(&victim_key, GetRange::Full).await,
                Err(StoreError::NotFound)
            ),
            "swept commit record is gone"
        );

        // The compaction record still names the token in its input list, and
        // its L1 parts carry the data (the fallback's serve-via-L1 path).
        let record = fetch_compaction_record(&store, &bucket).await;
        let covered = record.inputs.iter().any(|i| {
            i.writer_id == token.writer_id.to_string()
                && i.writer_epoch == token.epoch
                && i.writer_seq == token.seq
        });
        assert!(covered, "token identity is in the input list");
        assert_l1_intact(&store, &bucket).await;
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

// --- Convergence: crash mid-sweep, re-run to fully converged ---------------

/// Crash mid-sweep (an injected delete fault on any rule) then re-run: the
/// second pass finishes the job. Here the fault hits a superseded record
/// delete; the re-run converges to a fully swept bucket with L1 intact.
#[tokio::test]
async fn convergence_crash_mid_sweep_then_reruns_clean() {
    async fn run(sig: Sig) {
        let inner = MemoryStore::new();
        // Fail the first commit-record delete (keys carry "/c/"): note the
        // compaction record also lives under "/c/", but the superseded sweep
        // only ever deletes L0 commit records, so this scopes to that phase.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Delete, ScriptedFault::Throttled { retry_after_ms: 1 })
                .with_key_contains("/c/")
                .with_occurrence(Occurrence::Nth(1)),
        );
        let store = FaultStore::new(inner, plan);
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_and_compact(&store, &clock, sig).await;

        clock.set(past_horizon(created, &cfg()));
        let first = sweep_shard(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await;
        assert!(first.is_err(), "the delete fault aborts the first pass");
        assert_eq!(store.fault_count(Op::Delete, FaultKind::Throttled), 1);

        let report = sweep_shard(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("re-run converges");
        assert_eq!(l0_commit_count(&store, &bucket).await, 0);
        assert_eq!(l0_data_count(&store, &bucket).await, 0);
        assert_l1_intact(&store, &bucket).await;
        assert!(
            report.superseded_records_deleted + report.orphans_deleted > 0,
            "the re-run did the remaining work: {report:?}"
        );
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

// --- Horizon boundary: no delete fires before its horizon ------------------

/// No superseded delete fires before `now >= created + protection_horizon`,
/// proven by stepping a fixed clock across the boundary. Before the horizon,
/// the sweep deletes nothing; at the boundary it deletes the inputs.
#[tokio::test]
async fn no_delete_before_horizon_boundary_stepped() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_and_compact(&store, &clock, sig).await;
        let record = fetch_compaction_record(&store, &bucket).await;
        let horizon = record.created_unix_ns + cfg().protection_horizon_ns;

        // Just after compaction: horizon not reached, nothing deleted.
        let before = sweep_superseded(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("sweep");
        assert_eq!(
            (before.records_deleted, before.data_deleted),
            (0, 0),
            "nothing before the horizon"
        );
        assert_eq!(l0_commit_count(&store, &bucket).await, 2);

        // One ns before the boundary: still nothing.
        clock.set(horizon - 1);
        let edge = sweep_superseded(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("sweep");
        assert_eq!(
            (edge.records_deleted, edge.data_deleted),
            (0, 0),
            "nothing one ns before the horizon"
        );
        assert_eq!(l0_commit_count(&store, &bucket).await, 2);

        // Exactly at the boundary: the inputs are swept.
        clock.set(horizon);
        let at = sweep_superseded(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("sweep");
        assert_eq!(
            at.records_deleted, 2,
            "both input records deleted at the horizon"
        );
        assert_eq!(l0_commit_count(&store, &bucket).await, 0);
        assert_l1_intact(&store, &bucket).await;
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

// --- Unreferenced-part cleanup + its age gate ------------------------------

/// An `l1/` object referenced by no compaction record in a bucket that already
/// holds one is collected once it is older than `grace + max_compaction_lifetime`,
/// and never before. Uses the store's own `last_modified` clock to age the
/// stray part precisely across the gate.
#[tokio::test]
async fn unreferenced_part_swept_only_after_age_gate() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        let bucket = seed_and_compact(&store, &clock, sig).await;

        // Plant a stray l1 object in the bucket that no compaction record
        // references, at store time 0 (a losing compactor's leftover part).
        let stray_hash16 = hex::encode([0xEEu8; 8]);
        let record = fetch_compaction_record(&store, &bucket).await;
        let input_set_hash16 = hex::encode(&record.input_set_hash[..8]);
        let stray_key = keys::l1_part_key(
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
            bucket.ingest_hour_bucket,
            &input_set_hash16,
            9,
            &stray_hash16,
        )
        .unwrap();
        store
            .put(
                &stray_key,
                bytes::Bytes::from_static(b"stray-l1-part"),
                ravel_object_store::PutOptions::default(),
            )
            .await
            .unwrap();

        let config = cfg();
        // Before the age gate: last_modified is store-time 0, so age == now.
        // Set now just under the gate; the stray part survives.
        clock.set(config.unreferenced_part_age_gate_ns());
        let n = sweep_unreferenced(&store, &clock, &config, &bucket).await;
        assert_eq!(n, 0, "stray part younger than the gate survives");
        store
            .head(&stray_key)
            .await
            .expect("stray part still present");

        // Past the gate: the stray part is collected; referenced parts stay.
        clock.set(config.unreferenced_part_age_gate_ns() + 1);
        let n = sweep_unreferenced(&store, &clock, &config, &bucket).await;
        assert_eq!(n, 1, "stray part collected past the gate");
        assert!(matches!(
            store.head(&stray_key).await,
            Err(StoreError::NotFound)
        ));
        assert_l1_intact(&store, &bucket).await;
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

async fn sweep_unreferenced(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
) -> usize {
    sweep_unreferenced_result(store, clock, config, bucket)
        .await
        .expect("unreferenced sweep")
}

/// Like [`sweep_unreferenced`] but surfaces the `Result` (fault-path tests).
async fn sweep_unreferenced_result(
    store: &dyn ObjectStoreBackend,
    clock: &dyn Clock,
    config: &CompactorConfig,
    bucket: &Bucket,
) -> ravel_maintain::Result<usize> {
    ravel_maintain::sweep_unreferenced_parts(
        store,
        clock,
        config,
        &NoLeases,
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
    )
    .await
}

// --- Abandoned-compaction L1 leak (option (b)) ------------------------------
//
// The fix collects record-less `l1/` parts ONLY in buckets that hold a
// retention tombstone (which makes any future compaction impossible), and
// never in a bucket that still lacks both a record and a tombstone (where a
// future recovery compaction may republish and name those exact
// content-addressed parts). These tests pin both halves, and the decisive
// interleaving the first attempt lost data on.

/// Seed two compactable L0 inputs, then run a compaction that builds and PUTs
/// its L1 parts but abandons past `max_compaction_lifetime` without publishing. The bucket is left with record-less `l1/` parts and no
/// compaction record. Returns the bucket.
async fn seed_and_abandon(store: &dyn ObjectStoreBackend, clock: &dyn Clock, sig: Sig) -> Bucket {
    let bucket = seed_two(store, sig).await;
    let mut config = cfg();
    // Any elapsed time (>= 0, and a FixedClock gives exactly 0) exceeds this,
    // so publish_record returns Abandoned after the parts are already PUT.
    config.max_compaction_lifetime_ns = -1;
    let outcome = compact_bucket(store, clock, &config, &bucket)
        .await
        .expect("compact");
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Abandoned,
                ..
            }
        ),
        "expected an abandoned compaction, got {outcome:?}"
    );
    bucket
}

/// Write a retention tombstone at the bucket's fixed tombstone key. The
/// sweeper classifies it by key shape alone (it never GETs the body), so a
/// minimal valid `RetentionTombstone` suffices.
async fn seed_tombstone(store: &dyn ObjectStoreBackend, bucket: &Bucket) {
    let tombstone = RetentionTombstone {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal: ravel_commit::signal::to_proto(bucket.signal) as i32,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        retired_at_ns: 0,
        retention_window_ns: 0,
        record_count_observed: 0,
    };
    let key = keys::retention_tombstone_key_for(&tombstone).expect("tombstone key");
    store
        .put(
            &key,
            tombstone.encode_to_vec().into(),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put tombstone");
}

/// The sorted `l1/` part keys currently present in a bucket.
async fn l1_part_keys(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> Vec<String> {
    let prefix = format!(
        "t/{}/{}/l1/{:04}/{}/",
        bucket.tenant_hash.to_hex(),
        bucket.signal.key_prefix(),
        bucket.shard,
        keys::ingest_hour_string(bucket.ingest_hour_bucket),
    );
    let mut out: Vec<String> = list_all(store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .collect();
    out.sort();
    out
}

/// THE decisive test. Models exactly the
/// data-loss interleaving the first attempt introduced: an abandoned run
/// leaves record-less L1 parts older than the age gate, the sweeper fires
/// while the bucket is still record-less (the worst point: mid recovery
/// build), then the recovery compaction publishes. Every part the published
/// record names MUST still exist and the bucket MUST read back correctly.
///
/// The first attempt swept the record-less parts here (record-less, old
/// enough, fresh re-verify saw no record) and then let the recovery publish a
/// record naming the now-deleted parts: permanent loss. Option (b) refuses to
/// collect in a bucket that lacks both a record and a tombstone, so the sweep
/// is a no-op and nothing is lost.
#[tokio::test]
async fn recovery_over_abandoned_parts_never_loses_a_named_part() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let clock = FixedClock::new(sealed_now_ns());

        // Run A: build parts, abandon without publishing.
        let bucket = seed_and_abandon(&store, &clock, sig).await;
        let abandoned = l1_part_keys(&store, &bucket).await;
        assert!(!abandoned.is_empty(), "the abandoned run left L1 parts");

        // The parts' store last_modified is 0, so at `sealed_now_ns()` (which
        // the recovery compaction also needs, to see the bucket as sealed) they
        // are already far past the unreferenced-part age gate: exactly the
        // precondition the first attempt keyed its (unsafe) delete on.
        let config = cfg();
        assert!(
            sealed_now_ns() > config.unreferenced_part_age_gate_ns(),
            "the abandoned parts are already past the age gate"
        );

        // The sweeper fires mid-recovery, while the bucket is record-less and
        // NOT tombstoned. It must delete nothing.
        let report = sweep_shard(
            &store,
            &clock,
            &config,
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("mid-recovery sweep");
        assert_eq!(
            report.unreferenced_parts_deleted, 0,
            "record-less, non-tombstoned parts are never swept: {report:?}"
        );
        assert_eq!(
            l1_part_keys(&store, &bucket).await,
            abandoned,
            "every abandoned part survives the sweep"
        );

        // Run B: the normal recovery path compacts the same sealed bucket and
        // publishes. Its deterministic content-addressed parts are exactly the
        // surviving ones (same input_set_hash), so the record names live parts.
        let outcome = compact_bucket(&store, &clock, &config, &bucket)
            .await
            .expect("recovery compact");
        assert!(
            matches!(
                outcome,
                CompactionOutcome::Compacted {
                    publish: PublishOutcome::Published,
                    ..
                }
            ),
            "recovery published, got {outcome:?}"
        );

        // A post-publish sweep now sees a record; it must not touch a part the
        // record references.
        let report = sweep_shard(
            &store,
            &clock,
            &config,
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("post-publish sweep");
        assert_eq!(report.unreferenced_parts_deleted, 0, "{report:?}");

        // Every part the published record names EXISTS.
        let record = fetch_compaction_record(&store, &bucket).await;
        assert!(!record.parts.is_empty());
        for part in &record.parts {
            let key = keys::reconstruct_l1_part_key(&record, part).unwrap();
            store
                .head(&key)
                .await
                .expect("a part the published record names is present");
        }
        assert_l1_intact(&store, &bucket).await;

        // And the bucket queries correctly (metrics: exact sample equivalence).
        if matches!(sig, Sig::Metrics) {
            let got = read_record_samples(&store, &record).await;
            let expected = expected_samples(&metrics_specs());
            assert_eq!(got, expected, "bucket reads back the full input set");
        }
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

/// The plain abandoned-then-retired bucket: once a retention tombstone is
/// present, the record-less parts are collectable (they can never be
/// re-referenced, since a tombstoned bucket is never compacted again). The
/// pre-delete re-verify LIST is proven by injecting a fault on the SECOND
/// commit-prefix LIST of the pass (the re-verify) and asserting it fired: the
/// store is seeded BEFORE it is wrapped, so the pass's first commit LIST is
/// the initial reference map and its second is the re-verify.
#[tokio::test]
async fn tombstoned_abandoned_parts_collected_reverify_proven() {
    async fn run(sig: Sig) {
        let mem = MemoryStore::new();
        let clock = FixedClock::new(sealed_now_ns());
        let bucket = seed_and_abandon(&mem, &clock, sig).await;
        seed_tombstone(&mem, &bucket).await;
        let abandoned = l1_part_keys(&mem, &bucket).await;
        assert!(!abandoned.is_empty());

        // Fault the second commit-prefix LIST (the pre-delete re-verify).
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::List, ScriptedFault::Timeout)
                .with_key_contains("/c/")
                .with_occurrence(Occurrence::Nth(2)),
        );
        let store = FaultStore::new(mem, plan);

        let config = cfg();
        clock.set(config.unreferenced_part_age_gate_ns() + 1);

        // The candidate passes the age gate and the initial LIST; the
        // re-verify LIST faults, aborting before any delete.
        let res = sweep_unreferenced_result(&store, &clock, &config, &bucket).await;
        assert!(res.is_err(), "the re-verify LIST fault aborts the pass");
        assert_eq!(
            store.fault_count(Op::List, FaultKind::Timeout),
            1,
            "exactly the pre-delete re-verify LIST faulted"
        );
        assert_eq!(
            l1_part_keys(&store, &bucket).await,
            abandoned,
            "nothing deleted when the re-verify faults"
        );

        // The one-shot fault is spent; the re-run collects the tombstoned
        // bucket's record-less parts.
        let n = sweep_unreferenced(&store, &clock, &config, &bucket).await;
        assert_eq!(
            n,
            abandoned.len(),
            "all record-less parts of the tombstoned bucket collected"
        );
        assert!(
            l1_part_keys(&store, &bucket).await.is_empty(),
            "tombstoned bucket's record-less parts gone"
        );
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

/// A record-less part in a tombstoned bucket younger than the age gate
/// survives; it is collected only once strictly past the gate. Proves the
/// tombstoned branch is age-gated exactly like the record-present branch.
#[tokio::test]
async fn young_tombstoned_recordless_part_survives_age_gate() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let clock = FixedClock::new(sealed_now_ns());
        let bucket = seed_and_abandon(&store, &clock, sig).await;
        seed_tombstone(&store, &bucket).await;
        let abandoned = l1_part_keys(&store, &bucket).await;
        assert!(!abandoned.is_empty());

        let config = cfg();
        // Exactly at the gate: age == gate is treated as too young.
        clock.set(config.unreferenced_part_age_gate_ns());
        let n = sweep_unreferenced(&store, &clock, &config, &bucket).await;
        assert_eq!(n, 0, "younger-than-gate part survives even when tombstoned");
        assert_eq!(l1_part_keys(&store, &bucket).await, abandoned);

        // One ns past the gate: collectable.
        clock.set(config.unreferenced_part_age_gate_ns() + 1);
        let n = sweep_unreferenced(&store, &clock, &config, &bucket).await;
        assert_eq!(n, abandoned.len(), "collected once strictly past the gate");
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

/// A record-less part in a bucket with NEITHER a compaction record NOR a
/// tombstone is never swept, no matter how old: a future recovery compaction
/// may still publish a record naming it (the leak stays open until
/// the bucket is either compacted or retired). This is the safety boundary of
/// option (b), asserted directly.
#[tokio::test]
async fn recordless_untombstoned_part_is_never_swept() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let clock = FixedClock::new(sealed_now_ns());
        let bucket = seed_and_abandon(&store, &clock, sig).await;
        let abandoned = l1_part_keys(&store, &bucket).await;
        assert!(!abandoned.is_empty());

        let config = cfg();
        // Far past the gate: age alone would satisfy every timing precondition.
        clock.set(config.unreferenced_part_age_gate_ns() + 100 * NS_PER_HOUR);
        let n = sweep_unreferenced(&store, &clock, &config, &bucket).await;
        assert_eq!(
            n, 0,
            "no record and no tombstone: the part is retained for a future publish"
        );
        assert_eq!(l1_part_keys(&store, &bucket).await, abandoned);
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

/// Orphan GC never touches a live input (one with a commit record), and a
/// record-less data object is only collected past the orphan age gate.
#[tokio::test]
async fn orphan_gc_respects_live_records_and_age_gate() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let created = sealed_now_ns();
        let clock = FixedClock::new(created);
        // Seed inputs but do NOT compact: every L0 data object has a live
        // commit record, so orphan GC must never delete one.
        let bucket = seed_two(&store, sig).await;

        clock.set(created); // huge now, so age gate is trivially satisfied
        let n = sweep_orphans(
            &store,
            &clock,
            &cfg(),
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("orphan sweep")
        .deleted;
        assert_eq!(n, 0, "live inputs are never orphaned");
        assert_eq!(l0_data_count(&store, &bucket).await, 2);

        // Now orphan one data object by deleting its commit record.
        let inputs = l0_commit_keys(&store, &bucket).await;
        store.delete(&inputs[0]).await.unwrap();

        // Under the age gate: the record-less object survives.
        let config = cfg();
        clock.set(config.orphan_age_gate_ns());
        let n = sweep_orphans(
            &store,
            &clock,
            &config,
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("orphan sweep")
        .deleted;
        assert_eq!(n, 0, "orphan younger than the gate survives");

        // Past the gate: the orphan is collected; the still-committed object
        // stays.
        clock.set(config.orphan_age_gate_ns() + 1);
        let n = sweep_orphans(
            &store,
            &clock,
            &config,
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("orphan sweep")
        .deleted;
        assert_eq!(n, 1, "orphan collected past the gate");
        assert_eq!(
            l0_data_count(&store, &bucket).await,
            1,
            "committed object stays"
        );
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

// --- Dry-run reports the eligible set but deletes nothing ---------------

#[tokio::test]
async fn dry_run_sweep_reports_eligible_set_but_deletes_nothing() {
    async fn run(sig: Sig) {
        let store = MemoryStore::new();
        let clock = FixedClock::new(sealed_now_ns());
        let bucket = seed_and_compact(&store, &clock, sig).await;

        let before_records = l0_commit_count(&store, &bucket).await;
        let before_data = l0_data_count(&store, &bucket).await;
        assert!(before_records >= 2 && before_data >= 2);

        // Past the supersession horizon, so the superseded rule is eligible.
        let record = fetch_compaction_record(&store, &bucket).await;
        let mut config = cfg();
        config.dry_run = true;
        let now = past_horizon(record.created_unix_ns, &config);
        let clock = FixedClock::new(now);

        let report = sweep_shard(
            &store,
            &clock,
            &config,
            &NoLeases,
            &bucket.tenant_hash,
            bucket.signal,
            bucket.shard,
        )
        .await
        .expect("dry-run sweep");

        // The report reflects exactly what a real run would delete...
        assert_eq!(report.superseded_records_deleted, before_records);
        assert_eq!(report.superseded_data_deleted, before_data);
        // ...but nothing was actually deleted, and the L1 output is intact.
        assert_eq!(l0_commit_count(&store, &bucket).await, before_records);
        assert_eq!(l0_data_count(&store, &bucket).await, before_data);
        assert_l1_intact(&store, &bucket).await;
    }
    run(Sig::Metrics).await;
    run(Sig::Logs).await;
    run(Sig::Spans).await;
}

// --- Zone-scoped sweep and the full-pass safety net (ADR-0065 decision 3) ---
//
// `sweep_shard_zoned` scopes the superseded-input and unreferenced-part rules
// to the caller's per-tick hour set (typically the unit scan's head+tail
// hours). An hour classified Interior this tick is excluded from that set,
// and a full-keyspace `sweep_shard` pass -- the slow safety net -- is what
// eventually covers it. These tests pin that deferral-then-guarantee at the
// sweep primitives directly (the maintain driver's cadence decision lives in
// `services/ravel-server`, out of this crate's scope).

/// The safety-net case: an hour excluded from `sweep_shard_zoned`'s scope
/// (this tick's Interior classification) keeps its superseded input past the
/// protection horizon, but a full `sweep_shard` pass -- the slow cadence's
/// safety net -- still collects it.
#[tokio::test]
async fn sweep_shard_zoned_defers_out_of_scope_hour_to_full_sweep() {
    let store = MemoryStore::new();
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let bucket = seed_and_compact(&store, &clock, Sig::Metrics).await;
    clock.set(past_horizon(created, &cfg()));

    // The bucket's hour is excluded from this tick's scope (classified
    // Interior, not head or tail).
    let zoned = sweep_shard_zoned(
        &store,
        &clock,
        &cfg(),
        &NoLeases,
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        &[],
    )
    .await
    .expect("zoned sweep");
    assert!(!zoned.full_pass);
    assert_eq!(
        zoned.superseded_records_deleted, 0,
        "an out-of-scope hour is left untouched by the zoned pass"
    );
    assert_eq!(
        l0_commit_count(&store, &bucket).await,
        2,
        "inputs still present"
    );
    assert_l1_intact(&store, &bucket).await;

    // The safety-net full pass covers it.
    let full = sweep_shard(
        &store,
        &clock,
        &cfg(),
        &NoLeases,
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
    )
    .await
    .expect("full sweep");
    assert!(full.full_pass);
    assert!(
        full.superseded_records_deleted >= 1,
        "the safety-net pass sweeps what the zoned pass deferred"
    );
    assert_eq!(l0_commit_count(&store, &bucket).await, 0);
    assert_eq!(l0_data_count(&store, &bucket).await, 0);
    assert_l1_intact(&store, &bucket).await;
}

/// Deletion promptness bound (ADR-0065 decision 3): a tombstoned interior
/// bucket's record-less `l1/` residue is not collected by the per-tick zoned
/// pass while the hour is out of scope, but it is collected no later than the
/// next full safety-net pass -- the explicit bound this test pins, rather than
/// leaving it as an unstated property of the age gate.
#[tokio::test]
async fn tombstoned_interior_bucket_swept_no_later_than_full_pass() {
    let store = MemoryStore::new();
    let created = sealed_now_ns();
    let clock = FixedClock::new(created);
    let bucket = seed_and_compact(&store, &clock, Sig::Metrics).await;
    seed_tombstone(&store, &bucket).await;

    // A record-less, unreferenced `l1/` part (a losing compactor's leftover),
    // planted at store time 0.
    let stray_hash16 = hex::encode([0xEEu8; 8]);
    let record = fetch_compaction_record(&store, &bucket).await;
    let input_set_hash16 = hex::encode(&record.input_set_hash[..8]);
    let stray_key = keys::l1_part_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
        &input_set_hash16,
        9,
        &stray_hash16,
    )
    .unwrap();
    store
        .put(
            &stray_key,
            bytes::Bytes::from_static(b"stray-l1-part"),
            PutOptions::default(),
        )
        .await
        .unwrap();

    let config = cfg();
    clock.set(config.unreferenced_part_age_gate_ns() + 1);

    // Out of this tick's scope: the stray part survives.
    let zoned = sweep_shard_zoned(
        &store,
        &clock,
        &config,
        &NoLeases,
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        &[],
    )
    .await
    .expect("zoned sweep");
    assert!(!zoned.full_pass);
    assert_eq!(zoned.unreferenced_parts_deleted, 0);
    store
        .head(&stray_key)
        .await
        .expect("stray part survives the zoned pass");

    // The full safety-net pass collects it -- the promptness bound: no later
    // than this cadence.
    let full = sweep_shard(
        &store,
        &clock,
        &config,
        &NoLeases,
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
    )
    .await
    .expect("full sweep");
    assert!(full.full_pass);
    assert_eq!(full.unreferenced_parts_deleted, 1);
    assert!(matches!(
        store.head(&stray_key).await,
        Err(StoreError::NotFound)
    ));
    assert_l1_intact(&store, &bucket).await;
}
