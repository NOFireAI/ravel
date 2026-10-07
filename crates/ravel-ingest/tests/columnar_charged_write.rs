//! `LogIngestRouter::write_columnar_charged` (issue #2626, ADR-2614 decision
//! 5): a caller that already holds a charge for a batch hands it to the
//! router, which carries it to the shard buffers and the flush instead of
//! charging its own budget. These tests pin the three properties the bulk
//! loader's memory budget rests on: the charge is held until the flush that
//! carries the rows finishes, not until the write returns; it is refunded on
//! both a published and an abandoned flush, at exactly the bytes the caller
//! charged; and the batch is charged once, never again by the router.
//!
//! Every wait is a cooperative poll on a gauge or a fault gate. No wall-clock
//! sleep, no `tokio::time::timeout`, no `Instant`.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, tenant};
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, LogIngestRouter, LogWriteError,
    WriteMode,
};
use ravel_logseg::{ColumnarLogBatch, LogRecord, stream_attrs_bytes};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_types::logstream::{AttrValue, log_stream_id};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Bound on the cooperative polls below, so a refund that never comes fails
/// the test's own assertion instead of hanging it.
const YIELD_LIMIT: usize = 100_000;

/// One shard and no age flush. `target_bytes` picks between a write that
/// stays buffered until `flush_all` (large) and one whose own write triggers
/// its flush (1).
fn one_shard_router(
    store: Arc<dyn ObjectStoreBackend>,
    target_bytes: usize,
    budget: Arc<IngestByteBudget>,
) -> LogIngestRouter {
    let config = IngestConfig {
        shard_count: 1,
        target_bytes,
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        flush_tick: Duration::from_secs(3600),
        ..IngestConfig::default()
    };
    LogIngestRouter::new(config, store, TestClock::new(BASE_NS)).with_budget(budget)
}

fn unlimited() -> Arc<IngestByteBudget> {
    IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)
}

/// Twelve rows over three streams, with attributes, so the batch's measured
/// heap bytes and the router's `est_columnar_bytes` estimate differ.
fn fixture_batch() -> ColumnarLogBatch {
    let records: Vec<LogRecord> = (0..12u32)
        .map(|i| {
            let res: Vec<(String, AttrValue)> = vec![
                (
                    "service.name".to_string(),
                    AttrValue::Str("api".to_string()),
                ),
                ("host".to_string(), AttrValue::Str(format!("h{}", i % 3))),
            ];
            LogRecord {
                stream_id: log_stream_id(&res, "scope", "", &[]),
                stream_attrs: stream_attrs_bytes(&res, "scope", "", &[]),
                ts_ns: 1_000 + i64::from(i),
                observed_ts_ns: 1_000 + i64::from(i),
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: format!("line {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![
                    ("k_str".to_string(), AttrValue::Str(format!("v{i}"))),
                    ("k_int".to_string(), AttrValue::I64(i64::from(i))),
                ],
            }
        })
        .collect();
    ColumnarLogBatch::from_records(&records)
}

async fn until_in_flight(budget: &IngestByteBudget, bytes: u64) -> u64 {
    for _ in 0..YIELD_LIMIT {
        if budget.in_flight_bytes() == bytes {
            break;
        }
        tokio::task::yield_now().await;
    }
    budget.in_flight_bytes()
}

/// A Buffered charged write returns at enqueue, and its charge stays on the
/// caller's budget until the flush carrying its rows finishes: a second
/// charge that does not fit beside it waits, and is admitted only once
/// `flush_all` publishes the object.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn charged_write_holds_its_charge_until_the_flush_not_the_return() {
    let batch = fixture_batch();
    let heap = batch.heap_bytes() as u64;
    let loader = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(heap));
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let router = one_shard_router(Arc::clone(&store), 64 * 1024 * 1024, unlimited());

    let charge = loader.charge_waiting(heap);
    router
        .write_columnar_charged(
            tenant("acme"),
            batch,
            WriteMode::Buffered,
            Duration::from_secs(60),
            charge,
        )
        .await
        .expect("buffered write enqueues");
    assert_eq!(
        loader.in_flight_bytes(),
        heap,
        "the write returned but its rows are still buffered: the charge is held"
    );

    let waiter = {
        let loader = Arc::clone(&loader);
        std::thread::spawn(move || loader.charge_waiting(1).bytes())
    };
    while loader.waiting() < 1 {
        tokio::task::yield_now().await;
    }
    assert!(
        !waiter.is_finished(),
        "a charge that does not fit beside the buffered batch waits"
    );

    router.flush_all().await;
    let admitted = waiter.join().expect("waiter thread");
    assert_eq!(admitted, 1, "the waiter is admitted once the flush refunds");
    assert_eq!(
        loader.in_flight_bytes(),
        0,
        "the batch's charge was refunded by its flush and the waiter's dropped"
    );
}

/// The charge is refunded at exactly the bytes the caller charged, on a
/// flush that publishes and on one abandoned by a permanent PUT fault, and
/// while the flush is in flight the caller's budget holds the batch's
/// measured heap bytes, not the router's estimate.
#[tokio::test]
async fn charged_write_refunds_on_flush_success_and_failure() {
    // Success: hold the data PUT so the flush is observably in flight.
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let loader = unlimited();
    let router = Arc::new(one_shard_router(store, 1, unlimited()));
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Always);

    let batch = fixture_batch();
    let heap = batch.heap_bytes() as u64;
    let charge = loader.charge_waiting(heap);
    let write = {
        let router = Arc::clone(&router);
        tokio::spawn(async move {
            router
                .write_columnar_charged(
                    tenant("acme"),
                    batch,
                    WriteMode::Strict,
                    Duration::from_secs(60),
                    charge,
                )
                .await
        })
    };
    gate.wait_until_held(1).await;
    assert_eq!(gate.held_count(), 1, "the flush is held at its data PUT");
    assert_eq!(
        loader.in_flight_bytes(),
        heap,
        "an in-flight flush holds exactly the caller's measured charge"
    );
    for id in gate.held() {
        gate.release(id);
    }
    write
        .await
        .expect("join write")
        .expect("strict write commits once released");
    assert_eq!(
        until_in_flight(&loader, 0).await,
        0,
        "a published flush refunds the whole charge"
    );

    // Failure: a permanent data PUT fault abandons the flush.
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("fault: charged refund-on-failure test".into()),
        )
        .with_key_contains("/l0/"),
    );
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let router = one_shard_router(store, 1, unlimited());
    let batch = fixture_batch();
    let heap = batch.heap_bytes() as u64;
    let charge = loader.charge_waiting(heap);
    assert_eq!(loader.in_flight_bytes(), heap);
    let result = router
        .write_columnar_charged(
            tenant("acme"),
            batch,
            WriteMode::Strict,
            Duration::from_secs(60),
            charge,
        )
        .await;
    assert!(
        matches!(result, Err(LogWriteError::Abandoned(_))),
        "a permanent PUT fault abandons the flush, got {result:?}"
    );
    assert!(
        fault_store.fault_count(Op::Put, FaultKind::Permanent) >= 1,
        "the injected permanent PUT fault fired"
    );
    assert_eq!(
        until_in_flight(&loader, 0).await,
        0,
        "an abandoned flush refunds the whole charge"
    );
}

/// The router does not charge its own budget for a batch that arrives with
/// a charge: with one budget on both sides, the gauge holds the caller's
/// charge and nothing more.
#[tokio::test]
async fn charged_write_is_not_charged_twice() {
    let shared = unlimited();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let router = one_shard_router(store, 64 * 1024 * 1024, Arc::clone(&shared));

    let batch = fixture_batch();
    let charge = shared.charge_waiting(batch.heap_bytes() as u64);
    let charged = charge.bytes();
    router
        .write_columnar_charged(
            tenant("acme"),
            batch,
            WriteMode::Buffered,
            Duration::from_secs(60),
            charge,
        )
        .await
        .expect("buffered write enqueues");
    assert_eq!(
        shared.in_flight_bytes(),
        charged,
        "the buffered batch is charged once, at the caller's figure"
    );

    router.flush_all().await;
    assert_eq!(until_in_flight(&shared, 0).await, 0);
}
