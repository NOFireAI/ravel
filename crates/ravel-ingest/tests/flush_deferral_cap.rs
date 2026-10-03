//! The flush deferral cap (issue #1916, ADR-1642 deferral cap amendment) on
//! the log and span shard actors and routers. The metrics actor's copies live in
//! `ravel_ingest::shard::tests`, beside the read-side slack tests they extend.
//!
//! Each pipeline repeats the deferral bookkeeping rather than sharing it with
//! `shard.rs`, so each gets its own proof that it is wired in: a shard whose
//! oldest deferred flush reaches the cap answers the deferred strict-mode
//! write with the outcome-unknown `Abandoned`, its router refuses any new
//! write to it before enqueue with the retryable `DeferralCapReached` in both
//! write modes, it accepts again once its queue drains, and it retries
//! deferred flushes oldest first.
//!
//! Every wait here is a cooperative poll on a metric or a task plus an
//! injected-clock advance. No wall-clock sleep, no `tokio::time::timeout`, no
//! `Instant`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, tenant};
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, LogIngestRouter, LogWriteError,
    SpanIngestRouter, SpanWriteError, WriteMode,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::TenantId;
use ravel_types::logstream::{AttrValue, log_stream_id};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Past `max_flush_delay` (50 ms) on every tick.
const TICK_ADVANCE_NS: i64 = 100_000_000;

/// One permit and one queue slot, so one parked flush puts the shard at its
/// queued-flush cap. A 4000 s lifetime leaves a deferral cap of 7200 s less
/// 4000 s less the 50 ms strict delay and one 10 ms tick; a jump to the cap
/// stays inside the parked flush's lifetime, so abandonment never frees the
/// slot early.
fn one_slot_config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        flush_tick: Duration::from_millis(10),
        max_inflight_flushes: 1,
        max_queued_flushes: 1,
        max_flush_lifetime: Duration::from_secs(4000),
        ..IngestConfig::default()
    }
}

fn unlimited() -> Arc<IngestByteBudget> {
    IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)
}

/// Yields until `probe` is true; a probe that never becomes true hangs, which
/// the runner reports under the test's own name.
async fn until(mut probe: impl FnMut() -> bool) {
    while !probe() {
        tokio::task::yield_now().await;
    }
}

/// The two tests, generated once per pipeline: `$router` is the router type,
/// `$err` its write error type, `$spawn` a single-item writer taking a write
/// mode, and `$buffered` the cumulative buffered-item counter on its metrics
/// snapshot.
macro_rules! deferral_cap_tests {
    ($refuse:ident, $oldest_first:ident, $router:ty, $err:ident, $spawn:ident, $buffered:ident) => {
        #[tokio::test]
        async fn $refuse() {
            let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
            let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
            let clock = TestClock::new(BASE_NS);
            let config = one_slot_config();
            let cap_ns = config.flush_deferral_cap_ns();
            assert_eq!(cap_ns, 3_199_940_000_000);
            let router = Arc::new(
                <$router>::new(config, Arc::clone(&store), clock.clone()).with_budget(unlimited()),
            );
            let acme = tenant("acme");
            let globex = tenant("globex");
            let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
            let buffered = || router.metrics().snapshot().$buffered;
            let in_flight = || -> u64 {
                router
                    .metrics()
                    .in_flight_flushes_by_shard()
                    .into_iter()
                    .map(|(_, n)| n)
                    .sum()
            };
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

            let refusals = || router.metrics().snapshot().deferral_cap_refused;

            let parked = $spawn(&router, &acme, 0, WriteMode::Strict);
            until(|| buffered() >= 1).await;
            clock.advance_ns(TICK_ADVANCE_NS);
            until(|| in_flight() == 1).await;
            gate.wait_until_held(1).await;

            let deferred = $spawn(&router, &acme, 1, WriteMode::Strict);
            until(|| buffered() >= 2).await;
            clock.advance_ns(TICK_ADVANCE_NS);
            until(|| deferred_triggers() >= 1).await;
            assert!(
                !deferred.is_finished(),
                "below the cap the write still waits"
            );

            clock.advance_ns(cap_ns);
            until(|| deferred_triggers() >= 2).await;
            let stripped = deferred.await.expect("deferred write task");
            assert!(
                matches!(&stripped, Err($err::Abandoned(msg)) if msg.contains("deferral cap")),
                "the deferred write is answered outcome-unknown at the cap, got \
                 {stripped:?}"
            );

            let before = buffered();
            let shed = $spawn(&router, &globex, 2, WriteMode::Strict)
                .await
                .expect("globex write task");
            assert!(
                matches!(shed, Err($err::DeferralCapReached)),
                "a shard at the deferral cap refuses a strict write, got {shed:?}"
            );
            let shed = $spawn(&router, &globex, 4, WriteMode::Buffered)
                .await
                .expect("globex write task");
            assert!(
                matches!(shed, Err($err::DeferralCapReached)),
                "a shard at the deferral cap refuses a buffered write, got {shed:?}"
            );
            assert!($err::DeferralCapReached.is_retryable());
            assert_eq!(buffered(), before, "a refused write buffers nothing");
            assert_eq!(refusals(), 2);

            for id in gate.held() {
                assert!(gate.release(id));
            }
            parked
                .await
                .expect("parked write task")
                .expect("parked write acks once the gate is released");
            until(|| in_flight() == 0 && queued() == 0).await;
            let flushes_before = router.metrics().snapshot().flushes_by_age;
            clock.advance_ns(TICK_ADVANCE_NS);
            until(|| {
                router.metrics().snapshot().flushes_by_age > flushes_before
                    && in_flight() == 0
                    && queued() == 0
            })
            .await;

            let receipt = $spawn(&router, &globex, 5, WriteMode::Buffered)
                .await
                .expect("buffered write task")
                .expect("the shard accepts a buffered write once its deferred flush opened");
            assert!(receipt.tokens.is_empty());
            until(|| buffered() == before + 1).await;

            let accepted = $spawn(&router, &globex, 3, WriteMode::Strict);
            until(|| accepted.is_finished() || buffered() > before + 1).await;
            clock.advance_ns(TICK_ADVANCE_NS);
            let receipt = accepted
                .await
                .expect("accepted write task")
                .expect("the shard accepts again once its deferred flush opened");
            assert_eq!(receipt.tokens.len(), 1);
            router.flush_all().await;
        }

        #[tokio::test]
        async fn $oldest_first() {
            const TENANTS: usize = 8;
            let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
            let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
            let clock = TestClock::new(BASE_NS);
            let router = Arc::new(
                <$router>::new(one_slot_config(), Arc::clone(&store), clock.clone())
                    .with_budget(unlimited()),
            );
            let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
            let buffered = || router.metrics().snapshot().$buffered;
            let idle = || -> bool {
                let in_flight: u64 = router
                    .metrics()
                    .in_flight_flushes_by_shard()
                    .into_iter()
                    .map(|(_, n)| n)
                    .sum();
                let queued: u64 = router
                    .metrics()
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flushes_queued)
                    .sum();
                in_flight == 0 && queued == 0
            };
            let deferred_triggers = || -> u64 {
                router
                    .metrics()
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flush_trigger_deferred)
                    .sum()
            };

            let parked = $spawn(&router, &tenant("parked"), 0, WriteMode::Strict);
            until(|| buffered() >= 1).await;
            clock.advance_ns(TICK_ADVANCE_NS);
            until(|| !idle()).await;
            gate.wait_until_held(1).await;

            // Tenant i is first refused on tick i, so tick i refuses i + 1
            // buffers and the deferral start times increase with i.
            let mut writes = Vec::new();
            let mut refusals = 0u64;
            for i in 0..TENANTS {
                writes.push($spawn(
                    &router,
                    &tenant(&format!("t{i}")),
                    i + 1,
                    WriteMode::Strict,
                ));
                let want = (i + 2) as u64;
                until(|| buffered() >= want).await;
                clock.advance_ns(TICK_ADVANCE_NS);
                refusals += (i + 1) as u64;
                until(|| deferred_triggers() >= refusals).await;
            }
            assert!(writes.iter().all(|w| !w.is_finished()));

            for id in gate.held() {
                assert!(gate.release(id));
            }
            parked
                .await
                .expect("parked write task")
                .expect("parked write acks once the gate is released");
            until(|| idle()).await;

            for step in 0..TENANTS {
                clock.advance_ns(TICK_ADVANCE_NS);
                until(|| writes.iter().filter(|w| w.is_finished()).count() > step).await;
                let finished: Vec<usize> =
                    (0..TENANTS).filter(|&i| writes[i].is_finished()).collect();
                assert_eq!(
                    finished,
                    (0..=step).collect::<Vec<_>>(),
                    "tick {step} after the drain must open tenant t{step}, the \
                     oldest deferral left"
                );
                until(|| idle()).await;
            }
            for (i, w) in writes.into_iter().enumerate() {
                w.await
                    .expect("write task")
                    .unwrap_or_else(|e| panic!("t{i} acks: {e}"));
            }
            router.flush_all().await;
        }
    };
}

deferral_cap_tests!(
    log_shard_at_the_deferral_cap_refuses_appends_until_it_drains,
    log_shard_retries_deferred_flushes_oldest_first,
    LogIngestRouter,
    LogWriteError,
    spawn_log_write,
    buffered_records_total
);

deferral_cap_tests!(
    span_shard_at_the_deferral_cap_refuses_appends_until_it_drains,
    span_shard_retries_deferred_flushes_oldest_first,
    SpanIngestRouter,
    SpanWriteError,
    spawn_span_write,
    buffered_spans_total
);

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
