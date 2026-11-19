//! ADR-4326's fault switches in ravel-ingest (issue #4496), each scoped at
//! rate 1 on the test's own thread, which runs every shard actor and flush on
//! the current-thread runtime:
//!
//! - `ingest/encode-cancelled` answers a strict metrics write
//!   `ShardUnavailable` and a strict logs write `LogWriteError::ShardUnavailable`,
//!   with and without a write gate attached, and a strict spans write
//!   `SpanWriteError::ShardUnavailable` the same two ways; no data object,
//!   commit record or durable token exists.
//! - `ingest/data-put-refused` answers a strict metrics or logs write
//!   `Abandoned` with zero data-object PUTs reaching the store, no commit record
//!   and no durable token.
//!
//! Every test first writes the same input with the fault off and asserts it
//! succeeds with exactly one data PUT and one commit record. The fault half
//! runs only where ravel-invariant compiles the switches in (`antithesis` or
//! `capture`); in the default build `inv_fault!` is the constant false.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::any::Any;
use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, span_on_shard, tenant};
use ravel_cpu_gate::{CpuGateConfig, InstantClock, WriteGate, WriteSite};
use ravel_ingest::{
    IngestConfig, IngestRouter, LogIngestRouter, LogWriteError, PendingAckBudget, SpanIngestRouter,
    SpanWriteError, WriteError, WriteMode,
};
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Sequence};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::Signal;
use ravel_types::logstream::{AttrValue, log_stream_id};

const BASE_NS: i64 = 1_700_000_000_000_000_000;
const ACK_DEADLINE: Duration = Duration::from_secs(30);
const ENCODE_CANCELLED: &str = "ingest/encode-cancelled";
const DATA_PUT_REFUSED: &str = "ingest/data-put-refused";
/// More pass-through steps than any test issues data PUTs, so the sequence's
/// progress is the exact count of data-object PUTs that reached the store.
const COUNTED_PUTS: usize = 8;

/// Sets `name`'s rate on this thread until the returned guard drops; `None`
/// where ravel-invariant does not compile the switches in. The scope call sits
/// in an assertion's condition, which is compiled exactly when the `fault`
/// module is (`antithesis` or `capture`).
fn scoped_fault(name: &'static str, rate: f64) -> Option<Box<dyn Any>> {
    let guard: RefCell<Option<Box<dyn Any>>> = RefCell::new(None);
    let _ = (name, rate);
    ravel_invariant::inv_sometimes!(
        {
            let scoped = ravel_invariant::fault::scoped(name, rate).expect("a registered fault");
            *guard.borrow_mut() = Some(Box::new(scoped));
            true
        },
        "ravel-ingest test: a fault is scoped",
        ""
    );
    let guard = guard.into_inner();
    assert_eq!(
        guard.is_some(),
        ravel_invariant::EVALUATES,
        "the fault half runs exactly where the switches are compiled in"
    );
    guard
}

fn flush_on_first() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 1,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_base_delay: Duration::from_millis(0),
        put_retry_max_delay: Duration::from_millis(0),
        ..IngestConfig::default()
    }
}

/// A store that counts every data-object PUT (keys under `/l0/`).
fn counting_store() -> Arc<FaultStore<MemoryStore>> {
    let mut puts = Sequence::new(Op::Put).with_key_contains("/l0/");
    for _ in 0..COUNTED_PUTS {
        puts = puts.then_passthrough();
    }
    Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_sequence(puts),
    ))
}

/// One permit and an inline floor of 0, so every flush encode is a gated job.
fn floor_zero_gate() -> Arc<WriteGate> {
    Arc::new(WriteGate::new(
        CpuGateConfig {
            inline_floor_bytes: 0,
            ..CpuGateConfig::with_permits(1)
        },
        Arc::new(InstantClock::new()),
    ))
}

fn gate_jobs(gate: &WriteGate, site: WriteSite) -> u64 {
    gate.snapshot()
        .sites
        .iter()
        .find(|s| s.site == site)
        .map_or(0, |s| s.jobs)
}

/// What one strict write left in the store.
#[derive(Debug, PartialEq, Eq)]
struct Stored {
    data_puts: u64,
    data_objects: usize,
    commit_records: usize,
}

async fn stored(store: &FaultStore<MemoryStore>) -> Stored {
    let keys: Vec<String> = list_all(store, "t/")
        .await
        .expect("list")
        .into_iter()
        .map(|o| o.key)
        .collect();
    Stored {
        data_puts: store.sequence_progress(0),
        data_objects: keys.iter().filter(|k| k.contains("/l0/")).count(),
        commit_records: keys.iter().filter(|k| k.contains("/c/")).count(),
    }
}

const ONE_COMMIT: Stored = Stored {
    data_puts: 1,
    data_objects: 1,
    commit_records: 1,
};
const NOTHING: Stored = Stored {
    data_puts: 0,
    data_objects: 0,
    commit_records: 0,
};

struct MetricsRun {
    result: Result<ravel_ingest::WriteReceipt, WriteError>,
    stored: Stored,
    abandoned_retry_exhausted: u64,
    flush_gate_faults: u64,
    gate_jobs: Option<u64>,
}

async fn metrics_strict_write(gate: Option<Arc<WriteGate>>) -> MetricsRun {
    let store = counting_store();
    let mut router = IngestRouter::new(
        flush_on_first(),
        store.clone() as Arc<dyn ObjectStoreBackend>,
        Signal::Metrics,
        TestClock::new(BASE_NS),
        Arc::new(PendingAckBudget::new(0)),
    );
    if let Some(gate) = &gate {
        router = router.with_write_gate(Arc::clone(gate));
    }
    let tenant = tenant("acme");
    let points = vec![make_point(&tenant, "cpu", &[("host", "a")], 1_000, 1.0)];
    let result = router
        .write(tenant, points, WriteMode::Strict, ACK_DEADLINE)
        .await;
    let snapshot = router.metrics().snapshot();
    router.shutdown().await;
    MetricsRun {
        result,
        stored: stored(&store).await,
        abandoned_retry_exhausted: snapshot.abandoned_retry_exhausted,
        flush_gate_faults: snapshot.flush_gate_faults,
        gate_jobs: gate.map(|g| gate_jobs(&g, WriteSite::MetricsFlush)),
    }
}

struct LogsRun {
    result: Result<ravel_ingest::LogWriteReceipt, LogWriteError>,
    stored: Stored,
    abandoned_retry_exhausted: u64,
    flush_gate_faults: u64,
    gate_jobs: Option<u64>,
}

fn log_record(body: &str) -> NormalizedLogRecord {
    let resource = vec![("service.name".to_owned(), AttrValue::Str("svc".to_owned()))];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&resource, "scope", "", &scope_attrs),
        stream_attrs: ravel_logseg::stream_attrs_bytes(&resource, "scope", "", &scope_attrs).into(),
        ts_ns: BASE_NS + 1_000,
        observed_ts_ns: BASE_NS + 1_000,
        severity_num: 9,
        severity_text: "INFO".to_owned(),
        body: body.to_owned(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

async fn logs_strict_write(gate: Option<Arc<WriteGate>>) -> LogsRun {
    let store = counting_store();
    let mut router = LogIngestRouter::new(
        flush_on_first(),
        store.clone() as Arc<dyn ObjectStoreBackend>,
        TestClock::new(BASE_NS),
        Arc::new(PendingAckBudget::new(0)),
    );
    if let Some(gate) = &gate {
        router = router.with_write_gate(Arc::clone(gate));
    }
    let result = router
        .write(
            tenant("acme"),
            vec![log_record("hello")],
            WriteMode::Strict,
            ACK_DEADLINE,
        )
        .await;
    let snapshot = router.metrics().snapshot();
    router.shutdown().await;
    LogsRun {
        result,
        stored: stored(&store).await,
        abandoned_retry_exhausted: snapshot.abandoned_retry_exhausted,
        flush_gate_faults: snapshot.flush_gate_faults,
        gate_jobs: gate.map(|g| gate_jobs(&g, WriteSite::LogFlush)),
    }
}

struct SpansRun {
    result: Result<ravel_ingest::SpanWriteReceipt, SpanWriteError>,
    stored: Stored,
    abandoned_retry_exhausted: u64,
    flush_gate_faults: u64,
    gate_jobs: Option<u64>,
}

async fn spans_strict_write(gate: Option<Arc<WriteGate>>) -> SpansRun {
    let store = counting_store();
    let mut router = SpanIngestRouter::new(
        flush_on_first(),
        store.clone() as Arc<dyn ObjectStoreBackend>,
        TestClock::new(BASE_NS),
        Arc::new(PendingAckBudget::new(0)),
    );
    if let Some(gate) = &gate {
        router = router.with_write_gate(Arc::clone(gate));
    }
    let result = router
        .write(
            tenant("acme"),
            vec![span_on_shard(0, 1, BASE_NS + 1_000)],
            WriteMode::Strict,
            ACK_DEADLINE,
        )
        .await;
    let snapshot = router.metrics().snapshot();
    router.shutdown().await;
    SpansRun {
        result,
        stored: stored(&store).await,
        abandoned_retry_exhausted: snapshot.abandoned_retry_exhausted,
        flush_gate_faults: snapshot.flush_gate_faults,
        gate_jobs: gate.map(|g| gate_jobs(&g, WriteSite::SpanFlush)),
    }
}

fn assert_metrics_control(run: MetricsRun) {
    let receipt = run.result.expect("with the fault off the write succeeds");
    assert_eq!(receipt.tokens.len(), 1);
    assert_eq!(run.stored, ONE_COMMIT);
}

fn assert_logs_control(run: LogsRun) {
    let receipt = run.result.expect("with the fault off the write succeeds");
    assert_eq!(receipt.tokens.len(), 1);
    assert_eq!(run.stored, ONE_COMMIT);
}

fn assert_spans_control(run: SpansRun) {
    let receipt = run.result.expect("with the fault off the write succeeds");
    assert_eq!(receipt.tokens.len(), 1);
    assert_eq!(run.stored, ONE_COMMIT);
}

fn assert_metrics_encode_cancelled(run: MetricsRun) {
    let err = run
        .result
        .expect_err("a cancelled encode must not answer the write as committed");
    assert!(
        matches!(err, WriteError::ShardUnavailable),
        "a cancelled encode is a gate fault, answered ShardUnavailable: {err:?}"
    );
    assert!(
        err.durable_tokens().is_empty(),
        "{:?}",
        err.durable_tokens()
    );
    assert_eq!(
        run.stored, NOTHING,
        "no store call follows a cancelled encode"
    );
    assert_eq!(run.flush_gate_faults, 1);
    assert_eq!(run.abandoned_retry_exhausted, 0);
}

fn assert_logs_encode_cancelled(run: LogsRun) {
    let err = run
        .result
        .expect_err("a cancelled encode must not answer the write as committed");
    assert!(
        matches!(err, LogWriteError::ShardUnavailable),
        "a cancelled encode is a gate fault, answered ShardUnavailable: {err:?}"
    );
    assert!(
        err.durable_tokens().is_empty(),
        "{:?}",
        err.durable_tokens()
    );
    assert_eq!(
        run.stored, NOTHING,
        "no store call follows a cancelled encode"
    );
    assert_eq!(run.flush_gate_faults, 1);
    assert_eq!(run.abandoned_retry_exhausted, 0);
}

#[tokio::test]
async fn encode_cancelled_answers_a_metrics_write_shard_unavailable_without_a_gate() {
    assert_metrics_control(metrics_strict_write(None).await);
    let Some(_fault) = scoped_fault(ENCODE_CANCELLED, 1.0) else {
        return;
    };
    assert_metrics_encode_cancelled(metrics_strict_write(None).await);
}

#[tokio::test]
async fn encode_cancelled_answers_a_metrics_write_shard_unavailable_on_the_gate() {
    let control = metrics_strict_write(Some(floor_zero_gate())).await;
    assert_eq!(control.gate_jobs, Some(1), "the encode ran on the gate");
    assert_metrics_control(control);
    let Some(_fault) = scoped_fault(ENCODE_CANCELLED, 1.0) else {
        return;
    };
    let run = metrics_strict_write(Some(floor_zero_gate())).await;
    assert_eq!(
        run.gate_jobs,
        Some(0),
        "the cancelled encode never ran a job"
    );
    assert_metrics_encode_cancelled(run);
}

#[tokio::test]
async fn encode_cancelled_answers_a_logs_write_shard_unavailable_without_a_gate() {
    assert_logs_control(logs_strict_write(None).await);
    let Some(_fault) = scoped_fault(ENCODE_CANCELLED, 1.0) else {
        return;
    };
    assert_logs_encode_cancelled(logs_strict_write(None).await);
}

#[tokio::test]
async fn encode_cancelled_answers_a_logs_write_shard_unavailable_on_the_gate() {
    let control = logs_strict_write(Some(floor_zero_gate())).await;
    assert_eq!(control.gate_jobs, Some(1), "the encode ran on the gate");
    assert_logs_control(control);
    let Some(_fault) = scoped_fault(ENCODE_CANCELLED, 1.0) else {
        return;
    };
    let run = logs_strict_write(Some(floor_zero_gate())).await;
    assert_eq!(
        run.gate_jobs,
        Some(0),
        "the cancelled encode never ran a job"
    );
    assert_logs_encode_cancelled(run);
}

fn assert_spans_encode_cancelled(run: SpansRun) {
    let err = run
        .result
        .expect_err("a cancelled encode must not answer the write as committed");
    assert!(
        matches!(err, SpanWriteError::ShardUnavailable),
        "a cancelled encode is a gate fault, answered ShardUnavailable: {err:?}"
    );
    assert!(
        err.durable_tokens().is_empty(),
        "{:?}",
        err.durable_tokens()
    );
    assert_eq!(
        run.stored, NOTHING,
        "no data PUT and no commit record follow a cancelled encode"
    );
    assert_eq!(run.flush_gate_faults, 1);
    assert_eq!(run.abandoned_retry_exhausted, 0);
}

#[tokio::test]
async fn encode_cancelled_answers_a_spans_write_shard_unavailable_without_a_gate() {
    assert_spans_control(spans_strict_write(None).await);
    let Some(_fault) = scoped_fault(ENCODE_CANCELLED, 1.0) else {
        return;
    };
    assert_spans_encode_cancelled(spans_strict_write(None).await);
}

#[tokio::test]
async fn encode_cancelled_answers_a_spans_write_shard_unavailable_on_the_gate() {
    let control = spans_strict_write(Some(floor_zero_gate())).await;
    assert_eq!(control.gate_jobs, Some(1), "the encode ran on the gate");
    assert_spans_control(control);
    let Some(_fault) = scoped_fault(ENCODE_CANCELLED, 1.0) else {
        return;
    };
    let run = spans_strict_write(Some(floor_zero_gate())).await;
    assert_eq!(
        run.gate_jobs,
        Some(0),
        "the cancelled encode never ran a job"
    );
    assert_spans_encode_cancelled(run);
}

#[tokio::test]
async fn data_put_refused_abandons_a_metrics_flush_with_no_put() {
    assert_metrics_control(metrics_strict_write(None).await);
    let Some(_fault) = scoped_fault(DATA_PUT_REFUSED, 1.0) else {
        return;
    };
    let run = metrics_strict_write(None).await;
    assert_eq!(
        run.stored, NOTHING,
        "a refused data put issues no PUT and publishes no commit record"
    );
    let err = run
        .result
        .expect_err("a refused data put must not answer the write as committed");
    assert!(
        matches!(&err, WriteError::Abandoned(why) if why.contains("data object put")),
        "a refused data put abandons the flush: {err:?}"
    );
    assert!(
        err.durable_tokens().is_empty(),
        "{:?}",
        err.durable_tokens()
    );
    assert_eq!(run.abandoned_retry_exhausted, 1);
    assert_eq!(run.flush_gate_faults, 0);
}

#[tokio::test]
async fn data_put_refused_abandons_a_logs_flush_with_no_put() {
    assert_logs_control(logs_strict_write(None).await);
    let Some(_fault) = scoped_fault(DATA_PUT_REFUSED, 1.0) else {
        return;
    };
    let run = logs_strict_write(None).await;
    assert_eq!(
        run.stored, NOTHING,
        "a refused data put issues no PUT and publishes no commit record"
    );
    let err = run
        .result
        .expect_err("a refused data put must not answer the write as committed");
    assert!(
        matches!(&err, LogWriteError::Abandoned(why) if why.contains("data object put")),
        "a refused data put abandons the flush: {err:?}"
    );
    assert!(
        err.durable_tokens().is_empty(),
        "{:?}",
        err.durable_tokens()
    );
    assert_eq!(run.abandoned_retry_exhausted, 1);
    assert_eq!(run.flush_gate_faults, 0);
}
