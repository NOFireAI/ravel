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
//! deferred flushes oldest deferral first, which is not always oldest row
//! first. Below the cap a write to a shard with a deferred flush is accepted.
//! A shard whose actor died with a deferred flush answers the dead-shard
//! error rather than refusing at the cap forever; that test runs for the
//! metrics pipeline too, since its router respawns the actor.
//!
//! Every wait here is a cooperative poll on a metric or a task plus an
//! injected-clock advance. No wall-clock sleep, no `tokio::time::timeout`, no
//! `Instant`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{SplitBrainOnFirstCommit, TestClock, make_point, tenant};
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, IngestRouter, LogIngestRouter,
    LogWriteError, SpanIngestRouter, SpanWriteError, WriteError, WriteMode,
};
use ravel_logseg::{ColumnarLogBatch, LogRecord, stream_attrs_bytes};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Past `max_flush_delay` (50 ms) on every tick.
const TICK_ADVANCE_NS: i64 = 100_000_000;

/// One permit and one queue slot, so one parked flush puts the shard at its
/// queued-flush cap. A 4000 s lifetime leaves a deferral cap of 7200 s less
/// 4000 s less the 40 s idle delay and one 10 ms tick; a jump to the cap
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

/// Yields until `probe` is true or `YIELD_LIMIT` yields have passed, and
/// returns the probe's last answer. For a wait whose failure is the claim a
/// test pins, so the test fails with its own assertion instead of hanging.
async fn within_yields(mut probe: impl FnMut() -> bool) -> bool {
    for _ in 0..YIELD_LIMIT {
        if probe() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    probe()
}

const YIELD_LIMIT: usize = 10_000;

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
            assert_eq!(cap_ns, 3_159_990_000_000);
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

/// Three more tests per pipeline: a write below the cap is accepted, deferred
/// flushes reopen by deferral start rather than by row age, and a dead shard
/// past the cap gives the dead-shard answer.
macro_rules! deferral_cap_order_tests {
    ($below_cap:ident, $by_deferral:ident, $router:ty, $err:ident, $spawn:ident, $buffered:ident) => {
        /// A write that reaches a shard while one of its flushes is deferred,
        /// one tick short of the cap, is accepted in both write modes: the
        /// router and the actor refuse only at the cap, not on any deferral.
        #[tokio::test]
        async fn $below_cap() {
            let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
            let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
            let clock = TestClock::new(BASE_NS);
            let config = one_slot_config();
            let cap_ns = config.flush_deferral_cap_ns();
            let router = Arc::new(
                <$router>::new(config, Arc::clone(&store), clock.clone()).with_budget(unlimited()),
            );
            let acme = tenant("acme");
            let globex = tenant("globex");
            let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
            let buffered = || router.metrics().snapshot().$buffered;
            let refusals = || router.metrics().snapshot().deferral_cap_refused;
            let deferred_triggers = || -> u64 {
                router
                    .metrics()
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flush_trigger_deferred)
                    .sum()
            };

            let parked = $spawn(&router, &acme, 0, WriteMode::Strict);
            until(|| buffered() >= 1).await;
            clock.advance_ns(TICK_ADVANCE_NS);
            gate.wait_until_held(1).await;

            // The deferral starts at the tick after this advance.
            let deferred = $spawn(&router, &acme, 1, WriteMode::Strict);
            until(|| buffered() >= 2).await;
            clock.advance_ns(TICK_ADVANCE_NS);
            until(|| deferred_triggers() >= 1).await;
            // One tick short of the cap; the tick this advance fires refuses
            // the trigger again without reaching the cap.
            clock.advance_ns(cap_ns - TICK_ADVANCE_NS);
            until(|| deferred_triggers() >= 2).await;

            let strict = $spawn(&router, &globex, 2, WriteMode::Strict);
            let reached_the_buffer =
                within_yields(|| buffered() >= 3 || strict.is_finished()).await;
            assert!(
                reached_the_buffer && !strict.is_finished(),
                "a strict write below the cap is buffered and waits, got {:?}",
                if strict.is_finished() {
                    Some(strict.await.expect("strict write task"))
                } else {
                    None
                }
            );
            let receipt = $spawn(&router, &globex, 3, WriteMode::Buffered)
                .await
                .expect("buffered write task")
                .expect("a buffered write below the cap is accepted");
            assert!(receipt.tokens.is_empty());
            until(|| buffered() >= 4).await;
            assert_eq!(refusals(), 0, "nothing below the cap is refused");

            for id in gate.held() {
                assert!(gate.release(id));
            }
            parked
                .await
                .expect("parked write task")
                .expect("parked write acks once the gate is released");
            router.flush_all().await;
            deferred
                .await
                .expect("deferred write task")
                .expect("a write deferred short of the cap acks once its flush opens");
            strict
                .await
                .expect("strict write task")
                .expect("the strict write accepted below the cap acks");
        }

        /// Deferred flushes reopen by deferral start, not by row age. `old`'s
        /// row arrives first but, buffered with no strict waiter, it waits for
        /// the idle clock, while `young`'s strict row arrives later and is
        /// deferred first on the fast clock. The first tick after the drain
        /// must open `young`.
        #[tokio::test]
        async fn $by_deferral() {
            let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
            let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
            let clock = TestClock::new(BASE_NS);
            let config = one_slot_config();
            let idle_ns = config.max_flush_delay_idle.as_nanos() as i64;
            let router = Arc::new(
                <$router>::new(config, Arc::clone(&store), clock.clone()).with_budget(unlimited()),
            );
            let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
            let buffered = || router.metrics().snapshot().$buffered;
            let flushes = || router.metrics().snapshot().flushes_by_age;
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
            gate.wait_until_held(1).await;

            $spawn(&router, &tenant("old"), 1, WriteMode::Buffered)
                .await
                .expect("old write task")
                .expect("old buffered write accepted");
            until(|| buffered() >= 2).await;
            clock.advance_ns(TICK_ADVANCE_NS);

            let young = $spawn(&router, &tenant("young"), 2, WriteMode::Strict);
            until(|| buffered() >= 3).await;
            clock.advance_ns(TICK_ADVANCE_NS);
            until(|| deferred_triggers() >= 1).await;
            // `old` reaches the idle clock here and is deferred too, after
            // `young`, though its row is the older one.
            clock.advance_ns(idle_ns);
            until(|| deferred_triggers() >= 3).await;

            for id in gate.held() {
                assert!(gate.release(id));
            }
            parked
                .await
                .expect("parked write task")
                .expect("parked write acks once the gate is released");
            until(|| idle()).await;

            let before = flushes();
            clock.advance_ns(TICK_ADVANCE_NS);
            until(|| flushes() > before && idle()).await;
            assert!(
                within_yields(|| young.is_finished()).await,
                "the first tick after the drain must open the earliest deferral \
                 (young), not the oldest row (old)"
            );
            young
                .await
                .expect("young write task")
                .expect("young acks from its flush");
            router.flush_all().await;
        }
    };
}

deferral_cap_order_tests!(
    log_write_below_the_deferral_cap_is_accepted,
    log_shard_reopens_by_deferral_start_not_row_age,
    LogIngestRouter,
    LogWriteError,
    spawn_log_write,
    buffered_records_total
);

deferral_cap_order_tests!(
    span_write_below_the_deferral_cap_is_accepted,
    span_shard_reopens_by_deferral_start_not_row_age,
    SpanIngestRouter,
    SpanWriteError,
    spawn_span_write,
    buffered_spans_total
);

/// `write_columnar` refuses at the cap before enqueue, exactly as `write`.
#[tokio::test]
async fn log_write_columnar_at_the_deferral_cap_is_refused() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(BASE_NS);
    let config = one_slot_config();
    let cap_ns = config.flush_deferral_cap_ns();
    let router = Arc::new(
        LogIngestRouter::new(config, Arc::clone(&store), clock.clone()).with_budget(unlimited()),
    );
    let acme = tenant("acme");
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
    let buffered = || router.metrics().snapshot().buffered_records_total;
    let refusals = || router.metrics().snapshot().deferral_cap_refused;
    let deferred_triggers = || -> u64 {
        router
            .metrics()
            .shard_skew_by_shard()
            .into_iter()
            .map(|(_, s)| s.flush_trigger_deferred)
            .sum()
    };

    let parked = spawn_log_write(&router, &acme, 0, WriteMode::Strict);
    until(|| buffered() >= 1).await;
    clock.advance_ns(TICK_ADVANCE_NS);
    gate.wait_until_held(1).await;
    let deferred = spawn_log_write(&router, &acme, 1, WriteMode::Strict);
    until(|| buffered() >= 2).await;
    clock.advance_ns(TICK_ADVANCE_NS);
    until(|| deferred_triggers() >= 1).await;
    clock.advance_ns(cap_ns);
    until(|| deferred_triggers() >= 2).await;

    for mode in [WriteMode::Strict, WriteMode::Buffered] {
        let batch = ColumnarLogBatch::from_records(&[to_logrecord(&log_record(7))]);
        let shed = router
            .write_columnar(tenant("globex"), batch, mode, Duration::from_secs(60))
            .await;
        assert!(
            matches!(shed, Err(LogWriteError::DeferralCapReached)),
            "a columnar write to a shard at the cap is refused in {mode:?} mode, \
             got {shed:?}"
        );
    }
    assert_eq!(refusals(), 2);
    assert_eq!(buffered(), 2, "a refused columnar write buffers nothing");

    for id in gate.held() {
        assert!(gate.release(id));
    }
    parked
        .await
        .expect("parked write task")
        .expect("parked write acks once the gate is released");
    let stripped = deferred.await.expect("deferred write task");
    assert!(matches!(stripped, Err(LogWriteError::Abandoned(_))));
    router.flush_all().await;
}

/// A shard whose actor dies while a flush is deferred past the cap, with
/// nothing routed to it since, answers the dead-shard error, not
/// `DeferralCapReached` forever. Every write here is buffered: a strict
/// waiter's dropped ack would itself report the death, and this is the case
/// where nothing does. `$respawns` is whether the router respawns the shard
/// (metrics) or condemns it (logs, spans).
macro_rules! dead_shard_at_cap_test {
    ($name:ident, $new_router:ident, $signal:expr, $err:ident, $spawn:ident, $buffered:ident, $respawns:expr) => {
        #[tokio::test]
        async fn $name() {
            let fault_store = Arc::new(FaultStore::new(
                SplitBrainOnFirstCommit::new("/c/", $signal),
                FaultPlan::empty(),
            ));
            let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
            let clock = TestClock::new(BASE_NS);
            let config = one_slot_config();
            let cap_ns = config.flush_deferral_cap_ns();
            let idle_ns = config.max_flush_delay_idle.as_nanos() as i64;
            let router = Arc::new($new_router(config, store, clock.clone()));
            let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
            let buffered = || router.metrics().snapshot().$buffered;
            let deferred_triggers = || -> u64 {
                router
                    .metrics()
                    .shard_skew_by_shard()
                    .into_iter()
                    .map(|(_, s)| s.flush_trigger_deferred)
                    .sum()
            };

            $spawn(&router, &tenant("parked"), 0, WriteMode::Buffered)
                .await
                .expect("parked write task")
                .expect("parked buffered write accepted");
            until(|| buffered() >= 1).await;
            clock.advance_ns(idle_ns + TICK_ADVANCE_NS);
            gate.wait_until_held(1).await;
            $spawn(&router, &tenant("acme"), 1, WriteMode::Buffered)
                .await
                .expect("acme write task")
                .expect("acme buffered write accepted");
            until(|| buffered() >= 2).await;
            clock.advance_ns(idle_ns + TICK_ADVANCE_NS);
            until(|| deferred_triggers() >= 1).await;

            clock.advance_ns(cap_ns);
            until(|| deferred_triggers() >= 2).await;
            let capped = $spawn(&router, &tenant("globex"), 2, WriteMode::Buffered)
                .await
                .expect("capped write task");
            assert!(
                matches!(capped, Err($err::DeferralCapReached)),
                "the live shard is at the cap, got {capped:?}"
            );

            // The parked flush's commit meets the split-brain poison, and its
            // panic ends the actor with acme's flush deferred past the cap.
            // Nothing has routed to the shard since.
            for id in gate.held() {
                assert!(gate.release(id));
            }

            let mut answer = $spawn(&router, &tenant("globex"), 2, WriteMode::Buffered)
                .await
                .expect("probe write task");
            for _ in 0..YIELD_LIMIT {
                if !matches!(answer, Err($err::DeferralCapReached)) {
                    break;
                }
                tokio::task::yield_now().await;
                answer = $spawn(&router, &tenant("globex"), 2, WriteMode::Buffered)
                    .await
                    .expect("probe write task");
            }
            assert!(
                matches!(answer, Err($err::ShardUnavailable)),
                "a dead shard past the cap answers ShardUnavailable, got {answer:?}"
            );
            let snap = router.metrics().snapshot();
            assert_eq!(snap.shard_deaths, 1, "the death is observed once");
            if $respawns {
                assert_eq!(snap.shards_condemned, 0);
                assert!(router.ready());
                // The respawned actor has nothing deferred, and the dead one
                // cleared its deferral on exit, so the shard takes writes.
                $spawn(&router, &tenant("globex"), 3, WriteMode::Buffered)
                    .await
                    .expect("write task")
                    .expect("the respawned shard accepts a write past the old cap");
            } else {
                assert_eq!(snap.shards_condemned, 1);
                assert!(!router.ready(), "the dead shard drops readiness");
            }
        }
    };
}

dead_shard_at_cap_test!(
    metrics_shard_dead_with_a_deferred_flush_is_respawned_not_capped,
    new_metrics_router,
    Signal::Metrics,
    WriteError,
    spawn_metric_write,
    buffered_points_total,
    true
);

dead_shard_at_cap_test!(
    log_shard_dead_with_a_deferred_flush_answers_unavailable_not_capped,
    new_log_router,
    Signal::Logs,
    LogWriteError,
    spawn_log_write,
    buffered_records_total,
    false
);

dead_shard_at_cap_test!(
    span_shard_dead_with_a_deferred_flush_answers_unavailable_not_capped,
    new_span_router,
    Signal::Spans,
    SpanWriteError,
    spawn_span_write,
    buffered_spans_total,
    false
);

fn new_metrics_router(
    config: IngestConfig,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<TestClock>,
) -> IngestRouter {
    IngestRouter::new(config, store, Signal::Metrics, clock).with_budget(unlimited())
}

fn new_log_router(
    config: IngestConfig,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<TestClock>,
) -> LogIngestRouter {
    LogIngestRouter::new(config, store, clock).with_budget(unlimited())
}

fn new_span_router(
    config: IngestConfig,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<TestClock>,
) -> SpanIngestRouter {
    SpanIngestRouter::new(config, store, clock).with_budget(unlimited())
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

fn to_logrecord(r: &NormalizedLogRecord) -> LogRecord {
    LogRecord {
        stream_id: r.stream_id,
        stream_attrs: r.stream_attrs.clone(),
        ts_ns: r.ts_ns,
        observed_ts_ns: r.observed_ts_ns,
        severity_num: r.severity_num,
        severity_text: r.severity_text.clone(),
        body: r.body.clone(),
        trace_id: r.trace_id,
        span_id: r.span_id,
        flags: r.flags,
        attrs: r.attrs.clone(),
    }
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
