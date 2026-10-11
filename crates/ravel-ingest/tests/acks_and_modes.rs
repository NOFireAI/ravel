//! Strict vs buffered ack semantics, and multi-shard receipts with
//! read-your-write via `Catalog::resolve` (docs/consistency-model.md).
//! Also the submit/acks split and its pending-ack budget (ADR-2793 decision
//! 4): the permit order against the byte budget, when the permit is held and
//! released, and the router snapshot's gauge and counter.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;

use common::{TestClock, make_point, span_on_shard, tenant};
use futures::future::Either;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, IngestRouter, LogIngestRouter,
    LogWriteError, PendingAckBudget, PendingAckPermit, SpanIngestRouter, SpanWriteError,
    WriteError, WriteMode,
};
use ravel_logseg::{ColumnarLogBatch, LogRecord, stream_attrs_bytes};
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{InstrumentedStore, ObjectStoreBackend, StoreMetrics, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TimeRange};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Bound on the cooperative polls in [`until_puts`], so a flush that never
/// lands fails the test's own assertion instead of hanging it.
const YIELD_LIMIT: usize = 100_000;

fn log_resource() -> Vec<(String, AttrValue)> {
    vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )]
}

fn norm_log_record(ts_ns: i64, body: &str) -> NormalizedLogRecord {
    let resource = log_resource();
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    let stream_id = log_stream_id(&resource, "scope", "", &scope_attrs);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "", &scope_attrs);
    NormalizedLogRecord {
        stream_id,
        stream_attrs,
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn columnar_batch(ts_ns: i64, body: &str) -> ColumnarLogBatch {
    let resource = log_resource();
    let record = LogRecord {
        stream_id: log_stream_id(&resource, "scope", "", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "", &[]),
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    };
    ColumnarLogBatch::from_records(&[record])
}

fn config(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(50),
        ..IngestConfig::default()
    }
}

/// One shard whose buffer flushes as soon as anything arrives (every write
/// exceeds a 1-byte target), so a strict ack depends only on the flush's own
/// PUTs and never on the test calling `flush_all`.
fn flush_on_arrival_config() -> IngestConfig {
    IngestConfig {
        target_bytes: 1,
        ..config(1)
    }
}

fn memory_store() -> Arc<dyn ObjectStoreBackend> {
    Arc::new(MemoryStore::new())
}

/// A store whose data-object PUTs a test can hold at a gate, counted by an
/// `InstrumentedStore` on the outside so a held PUT is not counted until it
/// completes.
struct GatedStore {
    store: Arc<dyn ObjectStoreBackend>,
    gate: GateHandle,
    metrics: Arc<StoreMetrics>,
}

impl GatedStore {
    fn new(occurrence: Occurrence) -> Self {
        let fault = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
        let gate = fault.hold(Op::Put, Some("/l0/".to_string()), occurrence);
        let instrumented = InstrumentedStore::new(fault);
        let metrics = instrumented.metrics();
        GatedStore {
            store: Arc::new(instrumented),
            gate,
            metrics,
        }
    }

    fn puts(&self) -> u64 {
        self.metrics.snapshot().put.calls
    }

    /// Yields until at least `want` PUTs have completed, or the yield budget
    /// runs out; returns the count either way.
    async fn until_puts(&self, want: u64) -> u64 {
        for _ in 0..YIELD_LIMIT {
            if self.puts() >= want {
                break;
            }
            tokio::task::yield_now().await;
        }
        self.puts()
    }

    fn release_all(&self) {
        for id in self.gate.held() {
            self.gate.release(id);
        }
    }
}

/// A one-permit pending-ack budget whose only permit is already taken. Keep
/// the returned permit alive for as long as the budget must stay exhausted.
fn exhausted_pending_budget() -> (Arc<PendingAckBudget>, PendingAckPermit) {
    let budget = Arc::new(PendingAckBudget::new(1));
    let permit = budget.try_acquire().expect("the only permit");
    (budget, permit)
}

/// A byte budget that refuses every non-empty charge.
fn refusing_byte_budget() -> Arc<IngestByteBudget> {
    IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1))
}

#[tokio::test]
async fn strict_ack_only_after_data_and_commit_exist() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    );

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    // `flush_all` only flushes tenants already buffered, so it must run
    // concurrently with (not after) the write, and after (not before) the
    // write's own enqueue; joining the two futures interleaves them
    // correctly without a timing-dependent sleep.
    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        router.flush_all(),
    );
    let receipt = write_result.expect("strict write succeeds");
    assert_eq!(receipt.tokens.len(), 1);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let data_objects = objects.iter().filter(|o| o.key.contains("/l0/")).count();
    let commit_objects = objects.iter().filter(|o| o.key.contains("/c/")).count();
    assert_eq!(data_objects, 1, "data object must exist once ack fires");
    assert_eq!(commit_objects, 1, "commit record must exist once ack fires");

    router.shutdown().await;
}

#[tokio::test]
async fn buffered_mode_acks_immediately_then_flush_lands_data() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    );

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    let receipt = router
        .write(
            tenant.clone(),
            points,
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write succeeds");
    assert!(receipt.tokens.is_empty());

    let before = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        before.is_empty(),
        "buffered ack must not wait on durability"
    );

    router.flush_all().await;
    let after = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(after.iter().any(|o| o.key.contains("/l0/")));
    assert!(after.iter().any(|o| o.key.contains("/c/")));

    router.shutdown().await;
}

/// `submit` must return while the flush it triggered is still held at its
/// data-object PUT (ADR-2793 decision 4): a `submit` that folds the ack wait
/// back in can only return after that PUT, so it is still pending once the
/// gate holds the PUT and the `Pending` arm below fails it. `acks()` then
/// resolves once the gate opens, and its receipt has the shape `write` gives
/// for the same point on a fresh router over the same store, at exactly two
/// PUTs (data object and commit record) per write.
#[tokio::test]
async fn submit_then_acks_matches_write() {
    let gated = GatedStore::new(Occurrence::Nth(1));
    let clock = TestClock::new(BASE_NS);
    let router = IngestRouter::new(
        flush_on_arrival_config(),
        Arc::clone(&gated.store),
        Signal::Metrics,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    );
    let tenant = tenant("acme");
    let point = || {
        vec![make_point(
            &tenant,
            "cpu_usage",
            &[("host", "a")],
            1_000,
            1.0,
        )]
    };

    let submit = Box::pin(router.submit(
        tenant.clone(),
        point(),
        WriteMode::Strict,
        Duration::from_secs(5),
    ));
    let held = Box::pin(gated.gate.wait_until_held(1));
    let submitted = match futures::future::select(submit, held).await {
        Either::Left((submitted, _)) => submitted.expect("submit enqueues"),
        Either::Right(((), mut submit)) => match futures::poll!(&mut submit) {
            Poll::Ready(submitted) => submitted.expect("submit enqueues"),
            Poll::Pending => {
                panic!("submit is still pending with its flush's data PUT held: it awaits the ack")
            }
        },
    };
    gated.gate.wait_until_held(1).await;
    assert_eq!(gated.puts(), 0, "the held data PUT has not completed");

    let mut acks = Box::pin(submitted.acks());
    assert!(
        futures::poll!(&mut acks).is_pending(),
        "acks() cannot resolve while the data PUT is held"
    );
    gated.release_all();
    let receipt = acks.await.expect("acks resolves once the flush commits");
    assert_eq!(receipt.tokens.len(), 1);
    assert_eq!(gated.puts(), 2, "one data object and one commit record");

    let fresh = IngestRouter::new(
        flush_on_arrival_config(),
        Arc::clone(&gated.store),
        Signal::Metrics,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    );
    let written = fresh
        .write(
            tenant.clone(),
            point(),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("write succeeds");
    assert_eq!(written.tokens.len(), receipt.tokens.len());
    assert_eq!(written.tokens[0].shard, receipt.tokens[0].shard);
    assert_eq!(gated.puts(), 4, "write costs the same two PUTs");

    router.shutdown().await;
    fresh.shutdown().await;
}

/// The bound-plus-one strict `submit` must refuse once the budget's limit is
/// already held (ADR-2793 decision 4), and the router's `metrics_snapshot()`
/// must report the gauge and the shed counter from the budget it holds. A
/// buffered submit takes no permit, so it succeeds at the bound and leaves
/// the gauge alone. A resolved `acks()` frees one permit even while a second
/// `Submitted`, whose shard ack has already been sent, is never polled and
/// keeps its own. The permit order against the byte budget is pinned by the
/// `*_submit_takes_the_permit_before_the_byte_charge` tests below.
#[tokio::test]
async fn pending_ack_budget_refuses_the_bound_plus_one() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let budget = Arc::new(PendingAckBudget::new(2));
    let byte_budget = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
    let router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::clone(&budget),
    )
    .with_budget(Arc::clone(&byte_budget));
    let tenant = tenant("acme");
    let point = |host: &str| {
        vec![make_point(
            &tenant,
            "cpu_usage",
            &[("host", host)],
            1_000,
            1.0,
        )]
    };
    let gauge = || router.metrics_snapshot().pending_strict_acks;
    let shed = || router.metrics_snapshot().pending_ack_shed;

    let first = router
        .submit(
            tenant.clone(),
            point("a"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("first submit within budget");
    let second = router
        .submit(
            tenant.clone(),
            point("b"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("second submit within budget");
    assert_eq!(gauge(), 2);
    assert_eq!(shed(), 0);
    let charged_after_second = byte_budget.in_flight_bytes();

    let third = router
        .submit(
            tenant.clone(),
            point("c"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .err();
    assert!(
        matches!(third, Some(WriteError::PendingAckLimitReached)),
        "the bound-plus-one submit must refuse, got {third:?}"
    );
    assert_eq!(gauge(), 2, "a refused submit holds no permit");
    assert_eq!(shed(), 1, "the refusal is counted in the router's snapshot");
    assert_eq!(
        byte_budget.in_flight_bytes(),
        charged_after_second,
        "a submit refused for the pending-ack budget leaves the byte budget's \
         in-flight gauge where it was"
    );

    let buffered = router
        .submit(
            tenant.clone(),
            point("d"),
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("a buffered submit takes no permit, so the bound does not refuse it");
    drop(buffered);
    assert_eq!(gauge(), 2, "a buffered submit leaves the gauge unchanged");
    assert_eq!(shed(), 1);

    let (resolved, ()) = tokio::join!(first.acks(), router.flush_all());
    assert_eq!(resolved.expect("first acks resolves").tokens.len(), 1);
    assert_eq!(
        gauge(),
        1,
        "the resolved ack frees its permit; the never-polled second keeps its own"
    );

    let fourth = router
        .submit(
            tenant.clone(),
            point("e"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("the freed permit admits a fourth strict submit");
    assert_eq!(gauge(), 2);
    assert_eq!(shed(), 1);

    drop(second);
    drop(fourth);
    assert_eq!(gauge(), 0);
    router.shutdown().await;
}

/// One `PendingAckBudget` shared by `Arc` bounds all three pipelines
/// together (ADR-2793 decision 4): a permit the metrics pipeline holds
/// refuses the log and span pipelines' strict submits, and every router's
/// snapshot reports the same gauge and counter. A router built with its own
/// `PendingAckBudget::new` instead of a clone of the shared one would count
/// the metrics pipeline's permit in a budget it never reads, and its refusal
/// below would not happen.
#[tokio::test]
async fn pending_ack_budget_is_shared_across_pipelines() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let budget = Arc::new(PendingAckBudget::new(1));
    let metrics_router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::clone(&budget),
    );
    let log_router = LogIngestRouter::new(
        config(1),
        Arc::clone(&store),
        clock.clone(),
        Arc::clone(&budget),
    );
    let span_router = SpanIngestRouter::new(
        config(1),
        Arc::clone(&store),
        clock.clone(),
        Arc::clone(&budget),
    );

    let tenant = tenant("acme");
    let point = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];
    let held = metrics_router
        .submit(
            tenant.clone(),
            point,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("metrics submit takes the only permit");

    let refused = log_router
        .submit(
            tenant.clone(),
            vec![norm_log_record(1_000, "boot")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .err();
    assert!(
        matches!(refused, Some(LogWriteError::PendingAckLimitReached)),
        "the log pipeline must see the metrics pipeline's held permit, got {refused:?}"
    );
    let refused = span_router
        .submit(
            tenant.clone(),
            vec![span_on_shard(0, 1, 1_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .err();
    assert!(
        matches!(refused, Some(SpanWriteError::PendingAckLimitReached)),
        "the span pipeline must see the metrics pipeline's held permit, got {refused:?}"
    );

    let metrics_snapshot = metrics_router.metrics_snapshot();
    let log_snapshot = log_router.metrics_snapshot();
    let span_snapshot = span_router.metrics_snapshot();
    assert_eq!(
        (
            metrics_snapshot.pending_strict_acks,
            metrics_snapshot.pending_ack_shed
        ),
        (1, 2)
    );
    assert_eq!(
        (
            log_snapshot.pending_strict_acks,
            log_snapshot.pending_ack_shed
        ),
        (1, 2)
    );
    assert_eq!(
        (
            span_snapshot.pending_strict_acks,
            span_snapshot.pending_ack_shed
        ),
        (1, 2)
    );

    drop(held);
    assert_eq!(span_router.metrics_snapshot().pending_strict_acks, 0);
    metrics_router.shutdown().await;
    log_router.shutdown().await;
    span_router.shutdown().await;
}

/// Dropping a `Submitted` that was never polled releases its permit
/// (ADR-2793 decision 4) and does not cancel the write: with the flush's
/// data PUT held at a gate, the drop frees the permit for a second strict
/// submit, and once the gate releases the first write's PUT the dropped
/// write still lands its data object and commit record.
#[tokio::test]
async fn dropping_submitted_unawaited_releases_the_permit() {
    let gated = GatedStore::new(Occurrence::Always);
    let clock = TestClock::new(BASE_NS);
    let budget = Arc::new(PendingAckBudget::new(1));
    let router = IngestRouter::new(
        flush_on_arrival_config(),
        Arc::clone(&gated.store),
        Signal::Metrics,
        clock.clone(),
        Arc::clone(&budget),
    );
    let tenant = tenant("acme");
    let point = |host: &str| {
        vec![make_point(
            &tenant,
            "cpu_usage",
            &[("host", host)],
            1_000,
            1.0,
        )]
    };

    let submitted = router
        .submit(
            tenant.clone(),
            point("a"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("submit takes the only permit");
    gated.gate.wait_until_held(1).await;
    let dropped_write_put = gated.gate.held()[0];
    assert_eq!(budget.held(), 1);
    assert_eq!(gated.puts(), 0);

    drop(submitted);
    assert_eq!(
        budget.held(),
        0,
        "dropping Submitted unawaited must release the permit"
    );

    let second = router
        .submit(
            tenant.clone(),
            point("b"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("permit must be free again after the drop");
    gated.gate.wait_until_held(2).await;

    assert!(gated.gate.release(dropped_write_put));
    assert_eq!(
        gated.until_puts(2).await,
        2,
        "the dropped write's flush still lands its data object and commit record"
    );

    drop(second);
    gated.release_all();
    assert_eq!(gated.until_puts(4).await, 4);
    router.shutdown().await;
}

/// A strict submit refused by the pending-ack budget must not have touched
/// the byte budget: the permit is taken first (ADR-2793 decision 4). With a
/// byte budget that refuses everything, a submit that charged first would
/// fail with `BufferBudgetExceeded` and count a byte shed instead.
#[tokio::test]
async fn metrics_submit_takes_the_permit_before_the_byte_charge() {
    let (budget, _only_permit) = exhausted_pending_budget();
    let bytes = refusing_byte_budget();
    let router = IngestRouter::new(
        config(1),
        memory_store(),
        Signal::Metrics,
        TestClock::new(BASE_NS),
        Arc::clone(&budget),
    )
    .with_budget(Arc::clone(&bytes));
    let tenant = tenant("acme");

    let refused = router
        .submit(
            tenant.clone(),
            vec![make_point(&tenant, "cpu_usage", &[], 1_000, 1.0)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .err();
    assert!(
        matches!(refused, Some(WriteError::PendingAckLimitReached)),
        "got {refused:?}"
    );
    assert_eq!(bytes.shed_total(), 0, "the byte budget was never consulted");
    assert_eq!(budget.shed_total(), 1);
    router.shutdown().await;
}

/// The log row path's counterpart of
/// `metrics_submit_takes_the_permit_before_the_byte_charge`.
#[tokio::test]
async fn log_submit_takes_the_permit_before_the_byte_charge() {
    let (budget, _only_permit) = exhausted_pending_budget();
    let bytes = refusing_byte_budget();
    let router = LogIngestRouter::new(
        config(1),
        memory_store(),
        TestClock::new(BASE_NS),
        Arc::clone(&budget),
    )
    .with_budget(Arc::clone(&bytes));

    let refused = router
        .submit(
            tenant("acme"),
            vec![norm_log_record(1_000, "boot")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .err();
    assert!(
        matches!(refused, Some(LogWriteError::PendingAckLimitReached)),
        "got {refused:?}"
    );
    assert_eq!(bytes.shed_total(), 0, "the byte budget was never consulted");
    assert_eq!(budget.shed_total(), 1);
    router.shutdown().await;
}

/// The log columnar path's counterpart of
/// `metrics_submit_takes_the_permit_before_the_byte_charge`, through
/// `submit_columnar`, which charges the router's own byte budget.
#[tokio::test]
async fn log_columnar_submit_takes_the_permit_before_the_byte_charge() {
    let (budget, _only_permit) = exhausted_pending_budget();
    let bytes = refusing_byte_budget();
    let router = LogIngestRouter::new(
        config(1),
        memory_store(),
        TestClock::new(BASE_NS),
        Arc::clone(&budget),
    )
    .with_budget(Arc::clone(&bytes));

    let refused = router
        .submit_columnar(
            tenant("acme"),
            columnar_batch(1_000, "boot"),
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .err();
    assert!(
        matches!(refused, Some(LogWriteError::PendingAckLimitReached)),
        "got {refused:?}"
    );
    assert_eq!(bytes.shed_total(), 0, "the byte budget was never consulted");
    assert_eq!(budget.shed_total(), 1);
    router.shutdown().await;
}

/// The span path's counterpart of
/// `metrics_submit_takes_the_permit_before_the_byte_charge`.
#[tokio::test]
async fn span_submit_takes_the_permit_before_the_byte_charge() {
    let (budget, _only_permit) = exhausted_pending_budget();
    let bytes = refusing_byte_budget();
    let router = SpanIngestRouter::new(
        config(1),
        memory_store(),
        TestClock::new(BASE_NS),
        Arc::clone(&budget),
    )
    .with_budget(Arc::clone(&bytes));

    let refused = router
        .submit(
            tenant("acme"),
            vec![span_on_shard(0, 1, 1_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .err();
    assert!(
        matches!(refused, Some(SpanWriteError::PendingAckLimitReached)),
        "got {refused:?}"
    );
    assert_eq!(bytes.shed_total(), 0, "the byte budget was never consulted");
    assert_eq!(budget.shed_total(), 1);
    router.shutdown().await;
}

/// An `acks()` wait in progress holds the submit's permit, and resolving it
/// releases the permit (ADR-2793 decision 4). An `acks()` that dropped the
/// permit before awaiting the shard acks would read `held() == 0` at the
/// pending poll.
#[tokio::test]
async fn metrics_acks_holds_the_permit_until_it_resolves() {
    let budget = Arc::new(PendingAckBudget::new(1));
    let router = IngestRouter::new(
        config(1),
        memory_store(),
        Signal::Metrics,
        TestClock::new(BASE_NS),
        Arc::clone(&budget),
    );
    let tenant = tenant("acme");

    let submitted = router
        .submit(
            tenant.clone(),
            vec![make_point(&tenant, "cpu_usage", &[], 1_000, 1.0)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("submit within budget");
    let mut acks = Box::pin(submitted.acks());
    assert!(futures::poll!(&mut acks).is_pending());
    assert_eq!(budget.held(), 1, "a pending acks() holds its permit");

    let (receipt, ()) = tokio::join!(acks, router.flush_all());
    assert_eq!(receipt.expect("acks resolves").tokens.len(), 1);
    assert_eq!(budget.held(), 0, "a resolved acks() releases its permit");
    router.shutdown().await;
}

/// An `acks()` whose deadline elapses returns `AckTimeout` and releases the
/// permit, while the flush's data PUT is still held at a gate (ADR-2793
/// decision 4).
#[tokio::test]
async fn metrics_acks_releases_the_permit_on_deadline() {
    let gated = GatedStore::new(Occurrence::Always);
    let budget = Arc::new(PendingAckBudget::new(1));
    let router = IngestRouter::new(
        flush_on_arrival_config(),
        Arc::clone(&gated.store),
        Signal::Metrics,
        TestClock::new(BASE_NS),
        Arc::clone(&budget),
    );
    let tenant = tenant("acme");

    let submitted = router
        .submit(
            tenant.clone(),
            vec![make_point(&tenant, "cpu_usage", &[], 1_000, 1.0)],
            WriteMode::Strict,
            Duration::from_millis(50),
        )
        .await
        .expect("submit within budget");
    gated.gate.wait_until_held(1).await;
    let mut acks = Box::pin(submitted.acks());
    assert!(futures::poll!(&mut acks).is_pending());
    assert_eq!(budget.held(), 1, "a pending acks() holds its permit");

    let result = acks.await;
    assert!(
        matches!(result, Err(WriteError::AckTimeout)),
        "got {result:?}"
    );
    assert_eq!(budget.held(), 0, "an elapsed deadline releases the permit");

    gated.release_all();
    assert_eq!(gated.until_puts(2).await, 2);
    router.shutdown().await;
}

/// The log router's counterpart of
/// `metrics_acks_holds_the_permit_until_it_resolves`.
#[tokio::test]
async fn log_acks_holds_the_permit_until_it_resolves() {
    let budget = Arc::new(PendingAckBudget::new(1));
    let router = LogIngestRouter::new(
        config(1),
        memory_store(),
        TestClock::new(BASE_NS),
        Arc::clone(&budget),
    );

    let submitted = router
        .submit(
            tenant("acme"),
            vec![norm_log_record(1_000, "boot")],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("submit within budget");
    let mut acks = Box::pin(submitted.acks());
    assert!(futures::poll!(&mut acks).is_pending());
    assert_eq!(budget.held(), 1, "a pending acks() holds its permit");

    let (receipt, ()) = tokio::join!(acks, router.flush_all());
    assert_eq!(receipt.expect("acks resolves").tokens.len(), 1);
    assert_eq!(budget.held(), 0, "a resolved acks() releases its permit");
    router.shutdown().await;
}

/// The log router's counterpart of
/// `metrics_acks_releases_the_permit_on_deadline`.
#[tokio::test]
async fn log_acks_releases_the_permit_on_deadline() {
    let gated = GatedStore::new(Occurrence::Always);
    let budget = Arc::new(PendingAckBudget::new(1));
    let router = LogIngestRouter::new(
        flush_on_arrival_config(),
        Arc::clone(&gated.store),
        TestClock::new(BASE_NS),
        Arc::clone(&budget),
    );

    let submitted = router
        .submit(
            tenant("acme"),
            vec![norm_log_record(1_000, "boot")],
            WriteMode::Strict,
            Duration::from_millis(50),
        )
        .await
        .expect("submit within budget");
    gated.gate.wait_until_held(1).await;
    let mut acks = Box::pin(submitted.acks());
    assert!(futures::poll!(&mut acks).is_pending());
    assert_eq!(budget.held(), 1, "a pending acks() holds its permit");

    let result = acks.await;
    assert!(
        matches!(result, Err(LogWriteError::AckTimeout)),
        "got {result:?}"
    );
    assert_eq!(budget.held(), 0, "an elapsed deadline releases the permit");

    gated.release_all();
    assert_eq!(gated.until_puts(2).await, 2);
    router.shutdown().await;
}

/// The span router's counterpart of
/// `metrics_acks_holds_the_permit_until_it_resolves`.
#[tokio::test]
async fn span_acks_holds_the_permit_until_it_resolves() {
    let budget = Arc::new(PendingAckBudget::new(1));
    let router = SpanIngestRouter::new(
        config(1),
        memory_store(),
        TestClock::new(BASE_NS),
        Arc::clone(&budget),
    );

    let submitted = router
        .submit(
            tenant("acme"),
            vec![span_on_shard(0, 1, 1_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("submit within budget");
    let mut acks = Box::pin(submitted.acks());
    assert!(futures::poll!(&mut acks).is_pending());
    assert_eq!(budget.held(), 1, "a pending acks() holds its permit");

    let (receipt, ()) = tokio::join!(acks, router.flush_all());
    assert_eq!(receipt.expect("acks resolves").tokens.len(), 1);
    assert_eq!(budget.held(), 0, "a resolved acks() releases its permit");
    router.shutdown().await;
}

/// The span router's counterpart of
/// `metrics_acks_releases_the_permit_on_deadline`.
#[tokio::test]
async fn span_acks_releases_the_permit_on_deadline() {
    let gated = GatedStore::new(Occurrence::Always);
    let budget = Arc::new(PendingAckBudget::new(1));
    let router = SpanIngestRouter::new(
        flush_on_arrival_config(),
        Arc::clone(&gated.store),
        TestClock::new(BASE_NS),
        Arc::clone(&budget),
    );

    let submitted = router
        .submit(
            tenant("acme"),
            vec![span_on_shard(0, 1, 1_000)],
            WriteMode::Strict,
            Duration::from_millis(50),
        )
        .await
        .expect("submit within budget");
    gated.gate.wait_until_held(1).await;
    let mut acks = Box::pin(submitted.acks());
    assert!(futures::poll!(&mut acks).is_pending());
    assert_eq!(budget.held(), 1, "a pending acks() holds its permit");

    let result = acks.await;
    assert!(
        matches!(result, Err(SpanWriteError::AckTimeout)),
        "got {result:?}"
    );
    assert_eq!(budget.held(), 0, "an elapsed deadline releases the permit");

    gated.release_all();
    assert_eq!(gated.until_puts(2).await, 2);
    router.shutdown().await;
}

#[tokio::test]
async fn multi_shard_write_returns_one_token_per_shard_and_is_read_your_write() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let shard_count = 4;
    let router = IngestRouter::new(
        config(shard_count),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
        Arc::new(PendingAckBudget::new(0)),
    );

    let tenant = tenant("acme");
    // Enough distinct series that, with 4 shards, more than one shard is hit.
    let points: Vec<_> = (0..16)
        .map(|i| {
            make_point(
                &tenant,
                "cpu_usage",
                &[("host", &i.to_string())],
                1_000,
                i as f64,
            )
        })
        .collect();

    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        router.flush_all(),
    );
    let receipt = write_result.expect("strict write succeeds");
    assert!(
        receipt.tokens.len() > 1,
        "expected points to spread across multiple shards, got {} token(s)",
        receipt.tokens.len()
    );

    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog config valid");

    let snapshot = catalog
        .resolve(
            &tenant.hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: 0,
                end_ns: 2_000,
            },
            &receipt.tokens,
            clock.now(),
        )
        .await
        .expect("resolve succeeds");
    assert_eq!(
        snapshot.segments.len(),
        receipt.tokens.len(),
        "read-your-write must surface every flushed segment via min_tokens"
    );

    router.shutdown().await;
}
