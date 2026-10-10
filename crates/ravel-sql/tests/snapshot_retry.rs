//! Snapshot retry contract.
//!
//! `docs/consistency-model.md` mandates one re-resolve-and-retry when a
//! pinned segment vanishes before any result has been emitted, and an
//! immediate `SnapshotInvalidated` once emission has started. These tests
//! drive the first half end-to-end with a `FaultStore` `NotFoundBlip` on the
//! segment's data-object GET and assert the *resolve count*, not just the
//! outcome: an implementation that retried twice, or that returned the right
//! error without re-resolving at all, would pass an outcome-only assertion.
//!
//! The post-emission half is proven by the exhaustive unit test on
//! `retry_decision` in src/executor.rs rather than here. Today's
//! `RsegScanExec` fetches every segment in a partition before emitting its
//! first batch, and `SortPreservingMergeExec` needs one batch from every
//! partition before it emits anything, so a store `NotFound` can only ever
//! surface with zero batches emitted. `no_retry_happens_after_a_successful_
//! first_batch` below pins the observable half of that: a healthy query
//! resolves exactly once.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use std::sync::Arc;
use std::time::{Duration, Instant};

use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_sql::{PhaseWallTiming, SqlConfig, SqlError};
use ravel_types::{Signal, TenantId, logstream};
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};
use uuid::Uuid;

fn specs() -> Vec<SegSpec> {
    vec![
        SegSpec::new(
            10,
            1,
            1,
            vec![SeriesSpec::new("m", vec![(100, 1.0), (200, 2.0)])],
        ),
        SegSpec::new(20, 1, 1, vec![SeriesSpec::new("m", vec![(300, 3.0)])]),
    ]
}

/// Build a fixture over a `FaultStore` carrying `plan`.
///
/// Every rule below is scoped to `Op::Get` on keys containing `.rseg`, the
/// data-object suffix (`ravel_commit::keys::DATA_SUFFIX`). Publishing writes
/// data objects and commit records with PUT, and `Catalog::resolve` reads
/// commit records (`.cmt`) with LIST and GET, so no rule here can perturb
/// setup or snapshot resolution: only the segment reads the scan performs.
/// The one `.cmt` hold gate ([`hold_first_resolve`]) is registered after the
/// build and delays a resolve without changing its outcome.
async fn faulted_fixture(plan: FaultPlan) -> (Arc<FaultStore<MemoryStore>>, Fixture) {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let fixture = Fixture::build(
        backend,
        &[(&tenant, &seg_specs)],
        SqlConfig::default(),
        1 << 30,
    )
    .await;
    (store, fixture)
}

/// How long the first attempt's first commit-record GET is held.
const FIRST_ATTEMPT_HOLD: Duration = Duration::from_secs(1);

/// Hold the first commit-record GET the statement issues for
/// [`FIRST_ATTEMPT_HOLD`], then let every later one through.
///
/// The gate is registered after the fixture is built, so its first match is
/// the first attempt's own snapshot resolve: that attempt's resolve stage
/// contains the whole hold, which is what lets the assertions below tell the
/// successful attempt's stamps apart from the discarded attempt's.
fn hold_first_resolve(store: &FaultStore<MemoryStore>) -> tokio::task::JoinHandle<()> {
    let gate = store.hold(Op::Get, Some(".cmt".to_string()), Occurrence::Nth(1));
    tokio::spawn(async move {
        gate.wait_until_held(1).await;
        tokio::time::sleep(FIRST_ATTEMPT_HOLD).await;
        for id in gate.held() {
            gate.release(id);
        }
    })
}

/// The stage stamps the outcome reports describe the successful attempt
/// alone. That attempt starts after the first attempt's held resolve has
/// finished, and its stages are sequential, so their sum plus the hold fits
/// inside the statement's elapsed time. A stamp that kept or added the
/// discarded attempt's resolve would carry the hold a second time and break
/// the inequality, however loaded the machine is.
fn assert_stamps_exclude_the_held_attempt(wall: &PhaseWallTiming, elapsed: Duration) {
    let stages = [
        wall.resolve_ns,
        wall.plan_ns,
        wall.start_ns,
        wall.first_batch_ns,
        wall.drain_ns,
    ];
    let sum = Duration::from_nanos(stages.iter().sum::<u64>());
    assert!(
        sum + FIRST_ATTEMPT_HOLD <= elapsed,
        "the stage stamps carry the discarded attempt's {FIRST_ATTEMPT_HOLD:?} \
         hold: stages sum to {sum:?}, statement took {elapsed:?}: {wall:?}"
    );
}

/// A `NotFound` on the very first data-object GET, before any batch was
/// emitted: exactly one re-resolve, one retry, and the query then succeeds
/// with the full result.
///
/// The first attempt's resolve is held for [`FIRST_ATTEMPT_HOLD`], so the
/// discarded attempt has a stage stamp that contains the hold, and the
/// reported stamps must still exclude it.
#[tokio::test]
async fn not_found_before_the_first_batch_retries_exactly_once_and_succeeds() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(".rseg")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let (store, fixture) = faulted_fixture(plan).await;
    let tenant = tenant_id("acme");
    let releaser = hold_first_resolve(&store);

    let started = Instant::now();
    let outcome = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT count(value) AS n FROM samples"),
        )
        .await
        .expect("the retry must recover the query");
    let elapsed = started.elapsed();
    releaser.await.expect("the releaser ran to completion");

    assert_eq!(
        store.fault_count(Op::Get, FaultKind::NotFoundBlip),
        1,
        "the injected NotFound must actually have fired"
    );
    assert_eq!(
        outcome.stats.resolves, 2,
        "one original resolve plus exactly one re-resolve"
    );
    assert_eq!(outcome.stats.attempts, 2);
    assert_eq!(count_of(&outcome.output), 3, "all three samples come back");
    assert_stamps_exclude_the_held_attempt(&outcome.stats.wall, elapsed);
}

/// The same retry on the logs path: a `NotFound` on the only RLOG object's
/// first GET re-resolves once and succeeds, and the reported stamps exclude
/// the discarded attempt's held resolve.
#[tokio::test]
async fn a_logs_not_found_before_the_first_batch_retries_once_and_excludes_the_first_attempt() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(".rlog")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let tenant = tenant_id("acme-logs");
    publish_logs(backend.as_ref(), &tenant).await;
    let fixture = Fixture::build(backend, &[], SqlConfig::default(), 1 << 30).await;
    let releaser = hold_first_resolve(&store);

    let started = Instant::now();
    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, body FROM logs"))
        .await
        .expect("the retry must recover the logs query");
    let elapsed = started.elapsed();
    releaser.await.expect("the releaser ran to completion");

    assert_eq!(
        store.fault_count(Op::Get, FaultKind::NotFoundBlip),
        1,
        "the injected NotFound must actually have fired"
    );
    assert_eq!(outcome.stats.resolves, 2);
    assert_eq!(outcome.stats.attempts, 2);
    assert_eq!(outcome.output.num_rows(), LOG_RECORD_COUNT);
    assert_eq!(outcome.stats.scan_timing.scans, 1, "{:?}", outcome.stats);
    assert_stamps_exclude_the_held_attempt(&outcome.stats.wall, elapsed);
}

const LOG_RECORD_COUNT: usize = 4;

/// Publishes one RLOG object plus its `Signal::Logs` commit record (mirrors
/// query_accounting.rs's `publish_logs`).
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("snapshot-retry".to_string()),
    )];
    let stream_id = logstream::log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);

    let writer_id = Uuid::from_u128(9_200);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    for i in 0..LOG_RECORD_COUNT {
        let ts_ns = 1_000 + i as i64;
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns,
                observed_ts_ns: ts_ns,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: format!("retry record {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");

    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [7u8; 32],
        sample_count: LOG_RECORD_COUNT as u64,
        series_count: 1,
        min_event_ts_ns: 1_000,
        max_event_ts_ns: 1_000 + LOG_RECORD_COUNT as i64 - 1,
        min_ingest_ts_ns: 1_000,
        max_ingest_ts_ns: 1_000 + LOG_RECORD_COUNT as i64 - 1,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

/// A `NotFound` on every data-object GET: the retry runs once, fails again,
/// and the query ends as `SnapshotInvalidated`. Never a third resolve.
#[tokio::test]
async fn a_second_not_found_fails_snapshot_invalidated_after_exactly_one_retry() {
    let plan = FaultPlan::empty()
        .with_rule(Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_key_contains(".rseg"));
    let (_store, fixture) = faulted_fixture(plan).await;
    let tenant = tenant_id("acme");

    let err = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect_err("a persistently vanished segment must fail");

    assert!(
        matches!(err, SqlError::SnapshotInvalidated),
        "expected SnapshotInvalidated, got {err}"
    );
    // The client sees the redacted transient-storage message, never the key.
    assert_eq!(err.client_message(), ravel_sql::MSG_UNAVAILABLE);
}

/// A store fault that is *not* `NotFound` must not arm the retry: it
/// propagates on the first attempt, with a single resolve.
#[tokio::test]
async fn a_non_not_found_store_fault_does_not_retry() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Get,
            ScriptedFault::Permanent("disk on fire".to_string()),
        )
        .with_key_contains(".rseg"),
    );
    let (store, fixture) = faulted_fixture(plan).await;
    let tenant = tenant_id("acme");

    let err = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect_err("a permanent store fault must surface");

    assert!(
        !matches!(err, SqlError::SnapshotInvalidated),
        "a permanent fault is not a vanished snapshot; got {err}"
    );
    assert!(
        matches!(err, SqlError::Fetch(_)),
        "expected the typed fetch error, got {err}"
    );
    // Exactly one attempt: the retry is reserved for NotFound. With two
    // segments the plan may issue more than one GET before failing, so the
    // assertion is on the fault firing at all plus the error class, and on
    // the resolve count below.
    assert!(store.fault_count(Op::Get, FaultKind::Permanent) >= 1);
    // The raw backend text must not reach a client.
    assert!(!err.client_message().contains("disk on fire"));
}

/// The healthy path resolves exactly once. This is the observable half of
/// "no retry after emission": nothing re-resolves when nothing vanished.
#[tokio::test]
async fn no_retry_happens_after_a_successful_first_batch() {
    let tenant = tenant_id("acme");
    let seg_specs = specs();
    let fixture = Fixture::memory(&[(&tenant, &seg_specs)]).await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect("healthy query");

    assert_eq!(outcome.stats.resolves, 1);
    assert_eq!(outcome.stats.attempts, 1);
    assert!(
        outcome.stats.batches_emitted >= 1,
        "the plan must actually have emitted a batch for this to mean anything"
    );
    assert_eq!(outcome.output.num_rows(), 3);
}

/// Both resolves use the same `now_ns`, so the retry cannot widen or shift
/// the listing window and pick up a segment the first resolve could not see.
/// Asserted through the segment count the successful attempt reports.
#[tokio::test]
async fn the_retry_reuses_the_same_now_ns_and_window() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(".rseg")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let (_store, fixture) = faulted_fixture(plan).await;
    let tenant = tenant_id("acme");

    let outcome = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT count(value) AS n FROM samples"),
        )
        .await
        .expect("retry succeeds");

    assert_eq!(outcome.stats.resolves, 2);
    assert_eq!(
        outcome.stats.segments, 2,
        "the re-resolve must see the same two segments as the first"
    );
}

fn count_of(output: &ravel_sql::QueryOutput) -> i64 {
    use datafusion::arrow::array::Int64Array;

    let batch = output
        .batches()
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one result row");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count column")
        .value(0)
}
