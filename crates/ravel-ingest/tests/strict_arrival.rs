//! Strict-arrival writes (ADR-2793 decision 2; docs/ingest.md "Shard actor"
//! and "Pipelined flushes") on the metrics, log, and span pipelines: the
//! write handler opens the tenant's flush when nothing is in flight, a write
//! behind an in-flight flush coalesces into the flush the reap opens, an
//! arrival-pending buffer still flushes on the age clock, and an arrival
//! flush is never refused at the queued-flush cap or the tenant share.
//!
//! The clock is injected and only advanced where a test says so, so a flush
//! that happens without an advance cannot have come from the age tick. Every
//! wait whose failure is the claim under test is bounded in yields and fails
//! with its own assertion rather than hanging.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_commit::record;
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, IngestRouter, LogIngestRouter,
    LogWriteError, PendingAckBudget, SpanIngestRouter, SpanWriteError, WriteError, WriteMode,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Past `max_flush_delay` (50 ms).
const PAST_DELAY_NS: i64 = 100_000_000;

const YIELD_LIMIT: usize = 10_000;

/// One shard, a size trigger no test reaches, and room for sixteen flushes
/// per shard and fifteen per tenant, so neither the queued-flush cap nor the
/// share refuses anything unless a test narrows them.
fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        flush_tick: Duration::from_millis(10),
        max_inflight_flushes: 16,
        max_queued_flushes: 16,
        ..IngestConfig::default()
    }
}

fn fault_store() -> (Arc<FaultStore<MemoryStore>>, Arc<dyn ObjectStoreBackend>) {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    (fault_store, store)
}

/// Every object in the store. Each ingest PUT creates one object, so on a
/// store with no injected faults this is the PUT count.
async fn object_count(store: &dyn ObjectStoreBackend) -> usize {
    list_all(store, "").await.expect("list").len()
}

/// Rows per flush, read from the commit records (`sample_count` is points,
/// log records, or spans), smallest first.
async fn commit_rows(store: &dyn ObjectStoreBackend) -> Vec<u64> {
    let mut rows = Vec::new();
    for object in list_all(store, "t/")
        .await
        .expect("list")
        .iter()
        .filter(|o| o.key.contains("/c/"))
    {
        let bytes = store
            .get(&object.key, GetRange::Full)
            .await
            .expect("get commit record")
            .data;
        rows.push(
            record::decode(&bytes)
                .expect("decode commit record")
                .sample_count,
        );
    }
    rows.sort_unstable();
    rows
}

/// Yields until `probe` is true or `YIELD_LIMIT` yields have passed, and
/// returns the probe's last answer.
async fn within_yields(mut probe: impl FnMut() -> bool) -> bool {
    for _ in 0..YIELD_LIMIT {
        if probe() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    probe()
}

/// [`within_yields`], releasing every call `gate` holds before each probe.
async fn releasing_within_yields(gate: &GateHandle, mut probe: impl FnMut() -> bool) -> bool {
    for _ in 0..YIELD_LIMIT {
        for id in gate.held() {
            gate.release(id);
        }
        if probe() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    probe()
}

/// The five tests, generated once per pipeline: `$new` builds the router,
/// `$spawn` is a single-item writer taking a write mode, and `$buffered` the
/// cumulative buffered-item counter on its metrics snapshot.
macro_rules! strict_arrival_tests {
    (
        $in_handler:ident,
        $coalesces:ident,
        $age_floor:ident,
        $never_refused:ident,
        $deferred_by_age:ident,
        $new:ident,
        $spawn:ident,
        $buffered:ident
    ) => {
        /// The arrival write flushes inside its handler: with the clock never
        /// advanced no tick can fire, so its one flush is the handler's. The
        /// same write in strict mode makes no PUT until the clock passes
        /// `max_flush_delay`, and then flushes by age.
        #[tokio::test]
        async fn $in_handler() {
            let (_fault_store, store) = fault_store();
            let clock = TestClock::new(BASE_NS);
            let router = Arc::new($new(config(), Arc::clone(&store), clock.clone()));
            let acme = tenant("acme");

            let write = $spawn(&router, &acme, 0, WriteMode::StrictArrival);
            assert!(
                within_yields(|| write.is_finished()).await,
                "a strict-arrival write acks with the clock never advanced"
            );
            let receipt = write
                .await
                .expect("write task")
                .expect("strict-arrival write acks");
            assert_eq!(receipt.tokens.len(), 1);
            assert_eq!(
                object_count(store.as_ref()).await,
                2,
                "one data object and one commit record"
            );
            let snapshot = router.metrics().snapshot();
            assert_eq!(snapshot.flushes_by_arrival, 1);
            assert_eq!(snapshot.flushes_by_age, 0);
            assert_eq!(snapshot.flushes_by_size, 0);
            assert_eq!(snapshot.arrival_coalesced, 0);
            assert_eq!(clock.now(), BASE_NS);

            let (_fault_store, store) = fault_store();
            let clock = TestClock::new(BASE_NS);
            let router = Arc::new($new(config(), Arc::clone(&store), clock.clone()));
            let write = $spawn(&router, &acme, 0, WriteMode::Strict);
            assert!(within_yields(|| router.metrics().snapshot().$buffered >= 1).await);
            assert!(
                !within_yields(|| write.is_finished()).await,
                "a strict write waits for the age tick"
            );
            assert_eq!(object_count(store.as_ref()).await, 0);
            clock.advance_ns(PAST_DELAY_NS);
            assert!(within_yields(|| write.is_finished()).await);
            let receipt = write
                .await
                .expect("write task")
                .expect("strict write acks on age");
            assert_eq!(receipt.tokens.len(), 1);
            let snapshot = router.metrics().snapshot();
            assert_eq!(snapshot.flushes_by_age, 1);
            assert_eq!(snapshot.flushes_by_arrival, 0);
        }

        /// Seven arrival writes behind one in-flight flush share the next
        /// flush, which the reap opens with the clock unadvanced. The held
        /// gate is the band's precondition: it keeps the first flush in flight
        /// while the seven merge, so exactly seven coalesce and one flush is
        /// open, and releasing it gives exactly two flushes and four PUTs.
        #[tokio::test]
        async fn $coalesces() {
            let (fault_store, store) = fault_store();
            let clock = TestClock::new(BASE_NS);
            let router = Arc::new($new(config(), Arc::clone(&store), clock.clone()));
            let acme = tenant("acme");
            let gate = fault_store.hold(Op::Put, None, Occurrence::Always);

            let mut writes = vec![$spawn(&router, &acme, 0, WriteMode::StrictArrival)];
            gate.wait_until_held(1).await;
            for i in 1..8 {
                writes.push($spawn(&router, &acme, i, WriteMode::StrictArrival));
            }
            assert!(within_yields(|| router.metrics().snapshot().$buffered >= 8).await);
            let snapshot = router.metrics().snapshot();
            assert_eq!(snapshot.flushes_by_arrival, 1);
            assert_eq!(snapshot.arrival_coalesced, 7);
            assert!(writes.iter().all(|w| !w.is_finished()));

            assert!(
                releasing_within_yields(&gate, || writes.iter().all(|w| w.is_finished())).await,
                "the reap opens the coalesced flush with the clock unadvanced"
            );
            for write in writes {
                let receipt = write
                    .await
                    .expect("write task")
                    .expect("coalesced write acks");
                assert_eq!(receipt.tokens.len(), 1);
            }
            let snapshot = router.metrics().snapshot();
            assert_eq!(snapshot.flushes_by_arrival, 2);
            assert_eq!(snapshot.arrival_coalesced, 7);
            assert_eq!(snapshot.flushes_by_age, 0);
            assert_eq!(object_count(store.as_ref()).await, 4);
            assert_eq!(commit_rows(store.as_ref()).await, vec![1, 7]);
            assert_eq!(clock.now(), BASE_NS);
        }

        /// An arrival-pending buffer keeps its age threshold: with the first
        /// flush still held, the age tick flushes the coalesced write and it
        /// acks before the in-flight flush finishes.
        #[tokio::test]
        async fn $age_floor() {
            let (fault_store, store) = fault_store();
            let clock = TestClock::new(BASE_NS);
            let router = Arc::new($new(config(), Arc::clone(&store), clock.clone()));
            let acme = tenant("acme");
            let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
            let queued = || -> u64 {
                router
                    .metrics()
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flushes_queued)
                    .sum()
            };

            let first = $spawn(&router, &acme, 0, WriteMode::StrictArrival);
            gate.wait_until_held(1).await;
            let pending = $spawn(&router, &acme, 1, WriteMode::StrictArrival);
            assert!(within_yields(|| router.metrics().snapshot().$buffered >= 2).await);
            assert_eq!(router.metrics().snapshot().arrival_coalesced, 1);

            clock.advance_ns(PAST_DELAY_NS);
            assert!(
                within_yields(|| pending.is_finished()).await,
                "the age tick flushes an arrival-pending buffer"
            );
            let receipt = pending
                .await
                .expect("write task")
                .expect("pending write acks on age");
            assert_eq!(receipt.tokens.len(), 1);
            assert!(!first.is_finished(), "the first flush is still held");
            let snapshot = router.metrics().snapshot();
            assert_eq!(snapshot.flushes_by_age, 1);
            assert_eq!(snapshot.flushes_by_arrival, 1);
            assert_eq!(snapshot.arrival_coalesced, 1);

            assert!(releasing_within_yields(&gate, || first.is_finished()).await);
            first.await.expect("write task").expect("first write acks");
            // The ack lands before the first flush is reaped; the queued-flushes
            // gauge is written after the reap's arrival scan.
            assert!(within_yields(|| queued() == 0).await);
            assert_eq!(
                router.metrics().snapshot().flushes_by_arrival,
                1,
                "the age flush took the pending buffer, so the reap opens nothing"
            );
        }

        /// With one permit and one queue slot, a second tenant's arrival
        /// flush spawns past the full queue with no deferral, and its next
        /// arrival write coalesces at its share rather than being refused.
        #[tokio::test]
        async fn $never_refused() {
            let (fault_store, store) = fault_store();
            let clock = TestClock::new(BASE_NS);
            let config = IngestConfig {
                max_inflight_flushes: 1,
                max_queued_flushes: 1,
                ..config()
            };
            let router = Arc::new($new(config, Arc::clone(&store), clock.clone()));
            let queued = || -> u64 {
                router
                    .metrics()
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flushes_queued)
                    .sum()
            };
            let deferred_triggers = || -> u64 {
                router
                    .metrics()
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flush_trigger_deferred)
                    .sum()
            };
            let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Always);
            let acme = tenant("acme");
            let globex = tenant("globex");

            let a = $spawn(&router, &acme, 0, WriteMode::StrictArrival);
            gate.wait_until_held(1).await;
            assert!(
                within_yields(|| queued() == 1).await,
                "the queue is at its cap"
            );

            let b1 = $spawn(&router, &globex, 1, WriteMode::StrictArrival);
            assert!(
                within_yields(|| queued() == 2).await,
                "the second tenant's arrival flush spawns past the full queue"
            );
            assert_eq!(deferred_triggers(), 0);
            assert_eq!(router.metrics().snapshot().flushes_by_arrival, 2);

            let b2 = $spawn(&router, &globex, 2, WriteMode::StrictArrival);
            assert!(within_yields(|| router.metrics().snapshot().$buffered >= 3).await);
            let snapshot = router.metrics().snapshot();
            assert_eq!(snapshot.arrival_coalesced, 1);
            assert_eq!(snapshot.flushes_by_arrival, 2);
            assert_eq!(deferred_triggers(), 0);

            // Release A alone and hold B1's data PUT until A is reaped, so the
            // reap scan always runs with globex's flush still in flight: a scan
            // that opened globex's pending buffer then would be refused at the
            // share and deferred.
            let a_put = gate.held();
            assert_eq!(a_put.len(), 1);
            assert!(gate.release(a_put[0]));
            assert!(
                within_yields(|| gate.held_count() == 1).await,
                "B1's PUT is held"
            );
            assert!(
                within_yields(|| a.is_finished() && queued() == 1).await,
                "A is reaped while B1 is held"
            );
            assert_eq!(deferred_triggers(), 0);
            assert_eq!(router.metrics().snapshot().flushes_by_arrival, 2);

            assert!(
                releasing_within_yields(&gate, || {
                    a.is_finished() && b1.is_finished() && b2.is_finished()
                })
                .await
            );
            for write in [a, b1, b2] {
                let receipt = write.await.expect("write task").expect("write acks");
                assert_eq!(receipt.tokens.len(), 1);
            }
            assert_eq!(router.metrics().snapshot().flushes_by_arrival, 3);
            assert_eq!(deferred_triggers(), 0);
            assert_eq!(clock.now(), BASE_NS);
        }

        /// An arrival-pending buffer whose age flush the queued-flush cap
        /// deferred loses its pending mark when it is put back: the reap after
        /// the in-flight flush opens nothing, and the next age tick flushes it
        /// as an age flush.
        #[tokio::test]
        async fn $deferred_by_age() {
            let (fault_store, store) = fault_store();
            let clock = TestClock::new(BASE_NS);
            let config = IngestConfig {
                max_queued_flushes: 1,
                ..config()
            };
            let router = Arc::new($new(config, Arc::clone(&store), clock.clone()));
            let queued = || -> u64 {
                router
                    .metrics()
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flushes_queued)
                    .sum()
            };
            let deferred_triggers = || -> u64 {
                router
                    .metrics()
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flush_trigger_deferred)
                    .sum()
            };
            let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Always);
            let acme = tenant("acme");

            let first = $spawn(&router, &acme, 0, WriteMode::StrictArrival);
            gate.wait_until_held(1).await;
            let pending = $spawn(&router, &acme, 1, WriteMode::StrictArrival);
            assert!(within_yields(|| router.metrics().snapshot().$buffered >= 2).await);
            let snapshot = router.metrics().snapshot();
            assert_eq!(snapshot.arrival_coalesced, 1);
            assert_eq!(snapshot.flushes_by_arrival, 1);

            clock.advance_ns(PAST_DELAY_NS);
            assert!(
                within_yields(|| deferred_triggers() == 1).await,
                "the age tick's flush is deferred at the full queue"
            );
            assert!(!pending.is_finished());
            assert_eq!(router.metrics().snapshot().flushes_by_age, 0);

            assert!(
                releasing_within_yields(&gate, || first.is_finished() && queued() == 0).await,
                "the first flush is reaped"
            );
            first.await.expect("write task").expect("first write acks");

            clock.advance_ns(PAST_DELAY_NS);
            assert!(
                releasing_within_yields(&gate, || pending.is_finished()).await,
                "the next age tick flushes the deferred buffer"
            );
            let receipt = pending
                .await
                .expect("write task")
                .expect("deferred write acks on age");
            assert_eq!(receipt.tokens.len(), 1);
            let snapshot = router.metrics().snapshot();
            assert_eq!(snapshot.flushes_by_age, 1);
            assert_eq!(snapshot.flushes_by_arrival, 1);
            assert_eq!(snapshot.arrival_coalesced, 1);
            assert_eq!(deferred_triggers(), 1);
        }
    };
}

strict_arrival_tests!(
    strict_arrival_flushes_in_the_handler_without_a_tick,
    strict_arrival_coalesces_behind_an_in_flight_flush,
    arrival_pending_buffer_keeps_the_age_floor,
    strict_arrival_at_the_share_or_cap_is_never_refused,
    deferred_pending_buffer_flushes_by_age_not_arrival,
    new_metrics_router,
    spawn_metric_write,
    buffered_points_total
);

mod logs {
    use super::*;

    strict_arrival_tests!(
        strict_arrival_flushes_in_the_handler_without_a_tick,
        strict_arrival_coalesces_behind_an_in_flight_flush,
        arrival_pending_buffer_keeps_the_age_floor,
        strict_arrival_at_the_share_or_cap_is_never_refused,
        deferred_pending_buffer_flushes_by_age_not_arrival,
        new_log_router,
        spawn_log_write,
        buffered_records_total
    );
}

mod spans {
    use super::*;

    strict_arrival_tests!(
        strict_arrival_flushes_in_the_handler_without_a_tick,
        strict_arrival_coalesces_behind_an_in_flight_flush,
        arrival_pending_buffer_keeps_the_age_floor,
        strict_arrival_at_the_share_or_cap_is_never_refused,
        deferred_pending_buffer_flushes_by_age_not_arrival,
        new_span_router,
        spawn_span_write,
        buffered_spans_total
    );
}

fn unlimited() -> Arc<IngestByteBudget> {
    IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)
}

fn new_metrics_router(
    config: IngestConfig,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<TestClock>,
) -> IngestRouter {
    IngestRouter::new(
        config,
        store,
        Signal::Metrics,
        clock,
        Arc::new(PendingAckBudget::new(0)),
    )
    .with_budget(unlimited())
}

fn new_log_router(
    config: IngestConfig,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<TestClock>,
) -> LogIngestRouter {
    LogIngestRouter::new(config, store, clock, Arc::new(PendingAckBudget::new(0)))
        .with_budget(unlimited())
}

fn new_span_router(
    config: IngestConfig,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<TestClock>,
) -> SpanIngestRouter {
    SpanIngestRouter::new(config, store, clock, Arc::new(PendingAckBudget::new(0)))
        .with_budget(unlimited())
}

fn spawn_metric_write(
    router: &Arc<IngestRouter>,
    tenant: &TenantId,
    i: usize,
    mode: WriteMode,
) -> tokio::task::JoinHandle<Result<ravel_ingest::WriteReceipt, WriteError>> {
    let router = Arc::clone(router);
    let tenant = tenant.clone();
    tokio::spawn(async move {
        let host = format!("h{i}");
        let point = make_point(
            &tenant,
            "cpu_usage",
            &[("host", &host)],
            1_000 + i as i64,
            1.0,
        );
        router
            .write(tenant, vec![point], mode, Duration::from_secs(60))
            .await
    })
}

fn spawn_log_write(
    router: &Arc<LogIngestRouter>,
    tenant: &TenantId,
    i: usize,
    mode: WriteMode,
) -> tokio::task::JoinHandle<Result<ravel_ingest::LogWriteReceipt, LogWriteError>> {
    let router = Arc::clone(router);
    let tenant = tenant.clone();
    tokio::spawn(async move {
        router
            .write(tenant, vec![log_record(i)], mode, Duration::from_secs(60))
            .await
    })
}

/// One record on its own log stream.
fn log_record(i: usize) -> NormalizedLogRecord {
    let res: Vec<(String, AttrValue)> = vec![
        (
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        ),
        ("host".to_string(), AttrValue::Str(format!("h{i}"))),
    ];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope", "", &scope_attrs),
        stream_attrs: stream_attrs_bytes(&res, "scope", "", &scope_attrs),
        ts_ns: 1_000 + i as i64,
        observed_ts_ns: 1_000 + i as i64,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("line {i}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn spawn_span_write(
    router: &Arc<SpanIngestRouter>,
    tenant: &TenantId,
    i: usize,
    mode: WriteMode,
) -> tokio::task::JoinHandle<Result<ravel_ingest::SpanWriteReceipt, SpanWriteError>> {
    let router = Arc::clone(router);
    let tenant = tenant.clone();
    tokio::spawn(async move {
        router
            .write(tenant, vec![span_fixture(i)], mode, Duration::from_secs(60))
            .await
    })
}

/// One span with a distinct trace id.
fn span_fixture(i: usize) -> NormalizedSpan {
    let mut trace_id = [0u8; 16];
    trace_id[..4].copy_from_slice(&(i as u32 + 1).to_be_bytes());
    let mut span_id = [0u8; 8];
    span_id[..4].copy_from_slice(&(i as u32 + 1).to_be_bytes());
    NormalizedSpan {
        trace_id,
        span_id,
        parent_span_id: None,
        name: format!("handle-{i}"),
        start_ts_ns: 1_000 + i as i64,
        end_ts_ns: 1_100 + i as i64,
        status_code: StatusCode::Unset,
        status_message: None,
        attrs: vec![("service.name".to_string(), "checkout".to_string())],
    }
}
