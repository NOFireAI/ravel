//! ADR-1702 task 9: every shard flush's encode runs on the write gate, and a
//! strict acknowledgement still waits for the flush it depends on.
//!
//! The `*_runs_through_the_write_gate` tests park the gate's only permit on an
//! unrelated job, start a strict write that flushes on its first record, and
//! wait until the flush's encode is queued behind the parked job. At that
//! point the write has not returned and no commit record exists. Releasing the
//! permit lets the encode run, the flush publish and the write return, and the
//! commit record is then in the store. The flush site's `jobs` counter moves by
//! exactly one and its `inline` counter stays at zero, since the gate's inline
//! floor is 0.
//!
//! The `*_keeps_its_charge_*` tests do the same with a buffered write and check
//! that the flush's ADR-0069 byte charge is held for the whole wait (decision
//! 6) and refunded once the flush ends. A flush whose lifetime ends while its
//! encode is still queued is abandoned there without encoding, counted as a
//! queue-deadline abandonment.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::future::Future;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use common::{TestClock, make_point, span_on_shard, tenant};
use ravel_commit::rng::SeededRng;
use ravel_cpu_gate::{CpuGateConfig, InstantClock, JobSize, WriteGate, WriteSite};
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, IngestRouter, LogIngestRouter,
    LogWriteError, SpanIngestRouter, SpanWriteError, WriteError, WriteMode,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::Signal;
use ravel_types::logstream::{AttrValue, log_stream_id};
use tokio::task::JoinHandle;

const BASE_NS: i64 = 1_700_000_000_000_000_000;
const ACK_DEADLINE: Duration = Duration::from_secs(60);

fn flush_on_first() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 1,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

/// One permit, so a parked job holds the gate, and an inline floor of 0, so
/// every flush goes through it.
fn floor_zero_gate() -> Arc<WriteGate> {
    Arc::new(WriteGate::new(
        CpuGateConfig {
            inline_floor_bytes: 0,
            ..CpuGateConfig::with_permits(1)
        },
        Arc::new(InstantClock::new()),
    ))
}

fn counts(gate: &WriteGate, site: WriteSite) -> (u64, u64) {
    gate.snapshot()
        .sites
        .iter()
        .find(|s| s.site == site)
        .map_or((0, 0), |s| (s.jobs, s.inline))
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    panic!("timed out waiting until {what}");
}

async fn commit_records(store: &dyn ObjectStoreBackend) -> usize {
    list_all(store, "t/")
        .await
        .expect("list")
        .iter()
        .filter(|o| o.key.contains("/c/"))
        .count()
}

/// Runs `write` against `store` behind a parked permit and asserts the
/// ordering and the per-site count described in the module docs. `write`
/// resolves to whether the strict write succeeded.
/// Parks the gate's only permit on an unrelated job until the returned sender
/// is used.
async fn park_the_permit(gate: &Arc<WriteGate>) -> (mpsc::Sender<()>, JoinHandle<()>) {
    let (release, parked) = mpsc::channel::<()>();
    let blocker_gate = Arc::clone(gate);
    let blocker: JoinHandle<()> = tokio::spawn(async move {
        blocker_gate
            .run(WriteSite::OtapDecode, JobSize::Bytes(0), move || {
                let _ = parked.recv();
            })
            .await
            .expect("the parked job runs");
    });
    wait_until("the parked job holds the only permit", || {
        gate.running() == 1
    })
    .await;
    (release, blocker)
}

fn unlimited_budget() -> Arc<IngestByteBudget> {
    IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)
}

async fn assert_ack_waits_for_gated_flush<F>(
    gate: Arc<WriteGate>,
    site: WriteSite,
    store: Arc<dyn ObjectStoreBackend>,
    write: F,
) where
    F: Future<Output = bool> + Send + 'static,
{
    let (release, blocker) = park_the_permit(&gate).await;
    assert_eq!(counts(&gate, site), (0, 0));

    let write = tokio::spawn(write);
    wait_until("the flush encode is queued for the permit", || {
        gate.queued() == 1
    })
    .await;
    assert!(
        !write.is_finished(),
        "a strict write must not return while its flush's encode waits for the gate"
    );
    assert_eq!(
        commit_records(store.as_ref()).await,
        0,
        "nothing is published before the encode runs"
    );

    release.send(()).expect("release the parked job");
    blocker.await.expect("blocker task");
    assert!(
        write.await.expect("write task"),
        "the strict write succeeds"
    );
    assert_eq!(
        commit_records(store.as_ref()).await,
        1,
        "the commit record exists once the strict ack is observed"
    );
    assert_eq!(
        counts(&gate, site),
        (1, 0),
        "one flush is exactly one gated job at its site, and none inline"
    );
}

/// Decision 6: a buffered write returns at enqueue and holds no charge of its
/// own, so once its flush's encode is queued behind the parked permit the
/// buffer's ADR-0069 charge is held by the flush alone. It stays held, whole,
/// until the encode runs, and is refunded at the flush's terminal outcome.
async fn assert_charge_held_while_gated_encode_waits<F>(
    gate: Arc<WriteGate>,
    budget: Arc<IngestByteBudget>,
    buffered_write: F,
) where
    F: Future<Output = ()>,
{
    let (release, blocker) = park_the_permit(&gate).await;
    buffered_write.await;
    wait_until("the flush encode is queued for the permit", || {
        gate.queued() == 1
    })
    .await;
    let held = budget.in_flight_bytes();
    assert!(
        held > 0,
        "the flush's byte charge is held while its encode waits for a permit"
    );
    assert_eq!(
        held,
        budget.peak_bytes(),
        "no part of the flush's charge was refunded before the encode ran"
    );
    release.send(()).expect("release the parked job");
    blocker.await.expect("blocker task");
    wait_until(
        "the flush's charge is refunded at its terminal outcome",
        || budget.in_flight_bytes() == 0,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_metrics_flush_keeps_its_charge_while_its_encode_waits() {
    let gate = floor_zero_gate();
    let budget = unlimited_budget();
    let router = IngestRouter::new(
        flush_on_first(),
        Arc::new(MemoryStore::new()),
        Signal::Metrics,
        TestClock::new(BASE_NS),
    )
    .with_budget(Arc::clone(&budget))
    .with_write_gate(Arc::clone(&gate));
    let tenant = tenant("acme");
    let points = vec![make_point(&tenant, "cpu", &[("host", "a")], 1_000, 1.0)];
    assert_charge_held_while_gated_encode_waits(gate, budget, async {
        router
            .write(tenant, points, WriteMode::Buffered, ACK_DEADLINE)
            .await
            .expect("a buffered write enqueues");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_flush_keeps_its_charge_while_its_encode_waits() {
    let gate = floor_zero_gate();
    let budget = unlimited_budget();
    let router = LogIngestRouter::new(
        flush_on_first(),
        Arc::new(MemoryStore::new()),
        TestClock::new(BASE_NS),
    )
    .with_budget(Arc::clone(&budget))
    .with_write_gate(Arc::clone(&gate));
    assert_charge_held_while_gated_encode_waits(gate, budget, async {
        router
            .write(
                tenant("acme"),
                vec![log_record("hello")],
                WriteMode::Buffered,
                ACK_DEADLINE,
            )
            .await
            .expect("a buffered write enqueues");
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_span_flush_keeps_its_charge_while_its_encode_waits() {
    let gate = floor_zero_gate();
    let budget = unlimited_budget();
    let router = SpanIngestRouter::new(
        flush_on_first(),
        Arc::new(MemoryStore::new()),
        TestClock::new(BASE_NS),
    )
    .with_budget(Arc::clone(&budget))
    .with_write_gate(Arc::clone(&gate));
    assert_charge_held_while_gated_encode_waits(gate, budget, async {
        router
            .write(
                tenant("acme"),
                vec![span_on_shard(0, 1, 1_000)],
                WriteMode::Buffered,
                ACK_DEADLINE,
            )
            .await
            .expect("a buffered write enqueues");
    })
    .await;
}

fn log_record(body: &str) -> NormalizedLogRecord {
    let res = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope", "", &scope_attrs),
        stream_attrs: stream_attrs_bytes(&res, "scope", "", &scope_attrs),
        ts_ns: 1_000,
        observed_ts_ns: 1_000,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: body.to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_encode_runs_through_the_write_gate() {
    let gate = floor_zero_gate();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let router = Arc::new(
        IngestRouter::new(
            flush_on_first(),
            Arc::clone(&store),
            Signal::Metrics,
            TestClock::new(BASE_NS),
        )
        .with_write_gate(Arc::clone(&gate)),
    );
    let tenant = tenant("acme");
    let points = vec![make_point(&tenant, "cpu", &[("host", "a")], 1_000, 1.0)];
    let writer = Arc::clone(&router);
    assert_ack_waits_for_gated_flush(gate, WriteSite::MetricsFlush, store, async move {
        writer
            .write(tenant, points, WriteMode::Strict, ACK_DEADLINE)
            .await
            .is_ok()
    })
    .await;
}

/// The wait for a write gate permit is bounded by the flush's lifetime, as the
/// shard-permit wait is (issue #1739). With the permit parked and the injected
/// clock moved past `max_flush_lifetime`, the strict write returns `Abandoned`
/// while the encode is still queued, the flush counts as a queue-deadline
/// abandonment, its charge is refunded, and once the permit frees the encode
/// never runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_flush_queued_past_its_lifetime_on_the_write_gate_is_abandoned_unencoded() {
    const LIFETIME: Duration = Duration::from_secs(10);
    let gate = floor_zero_gate();
    let budget = unlimited_budget();
    let clock = TestClock::new(BASE_NS);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let router = Arc::new(
        IngestRouter::new(
            IngestConfig {
                max_flush_lifetime: LIFETIME,
                ..flush_on_first()
            },
            Arc::clone(&store),
            Signal::Metrics,
            clock.clone(),
        )
        .with_budget(Arc::clone(&budget))
        .with_write_gate(Arc::clone(&gate)),
    );
    let (release, blocker) = park_the_permit(&gate).await;

    let tenant = tenant("acme");
    let points = vec![make_point(&tenant, "cpu", &[("host", "a")], 1_000, 1.0)];
    let writer = Arc::clone(&router);
    let write = tokio::spawn(async move {
        writer
            .write(tenant, points, WriteMode::Strict, ACK_DEADLINE)
            .await
    });
    wait_until("the flush encode is queued for the permit", || {
        gate.queued() == 1
    })
    .await;
    assert!(budget.in_flight_bytes() > 0);

    clock.advance_ns(i64::try_from(LIFETIME.as_nanos()).unwrap() + 1);
    let result = tokio::time::timeout(Duration::from_secs(30), write)
        .await
        .expect("the write returns while the permit is still parked")
        .expect("write task");
    match result {
        Err(WriteError::Abandoned(message)) => assert!(
            message.contains("write gate"),
            "unexpected abandonment message: {message}"
        ),
        other => panic!("expected Abandoned, got {other:?}"),
    }
    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.abandoned_queue_deadline, 1);
    assert_eq!(snapshot.abandoned_retry_exhausted, 0);
    assert_eq!(snapshot.abandoned_input_rejected, 0);
    assert_eq!(gate.queued(), 0, "the abandoned waiter left the queue");
    assert_eq!(
        budget.in_flight_bytes(),
        0,
        "the queued job's charge dropped with it"
    );

    release.send(()).expect("release the parked job");
    blocker.await.expect("blocker task");
    assert_eq!(
        counts(&gate, WriteSite::MetricsFlush),
        (0, 0),
        "the abandoned encode never ran"
    );
    assert_eq!(commit_records(store.as_ref()).await, 0);
}

const LIFETIME: Duration = Duration::from_secs(10);

fn short_lifetime() -> IngestConfig {
    IngestConfig {
        max_flush_lifetime: LIFETIME,
        ..flush_on_first()
    }
}

/// Parks the permit, starts `write`, waits for its flush's encode to queue,
/// moves `clock` past the lifetime and returns the write's error message,
/// which must arrive while the permit is still parked. Then frees the permit
/// and checks the encode never ran at `site`.
async fn abandoned_in_the_gate_queue<F, T, E>(
    gate: Arc<WriteGate>,
    site: WriteSite,
    clock: Arc<TestClock>,
    write: F,
) -> E
where
    F: Future<Output = Result<T, E>> + Send + 'static,
    T: std::fmt::Debug + Send + 'static,
    E: std::fmt::Debug + Send + 'static,
{
    let (release, blocker) = park_the_permit(&gate).await;
    let write = tokio::spawn(write);
    wait_until("the flush encode is queued for the permit", || {
        gate.queued() == 1
    })
    .await;
    clock.advance_ns(i64::try_from(LIFETIME.as_nanos()).unwrap() + 1);
    let err = tokio::time::timeout(Duration::from_secs(30), write)
        .await
        .expect("the write returns while the permit is still parked")
        .expect("write task")
        .expect_err("the flush is abandoned");
    release.send(()).expect("release the parked job");
    blocker.await.expect("blocker task");
    assert_eq!(
        counts(&gate, site),
        (0, 0),
        "the abandoned encode never ran"
    );
    err
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_flush_queued_past_its_lifetime_on_the_write_gate_is_abandoned() {
    let gate = floor_zero_gate();
    let clock = TestClock::new(BASE_NS);
    let router = Arc::new(
        LogIngestRouter::new(
            short_lifetime(),
            Arc::new(MemoryStore::new()),
            clock.clone(),
        )
        .with_write_gate(Arc::clone(&gate)),
    );
    let writer = Arc::clone(&router);
    let err = abandoned_in_the_gate_queue(gate, WriteSite::LogFlush, clock, async move {
        writer
            .write(
                tenant("acme"),
                vec![log_record("hello")],
                WriteMode::Strict,
                ACK_DEADLINE,
            )
            .await
    })
    .await;
    assert!(
        matches!(&err, LogWriteError::Abandoned(m) if m.contains("write gate")),
        "expected the gate-wait abandonment, got {err:?}"
    );
    assert_eq!(router.metrics().snapshot().abandoned_queue_deadline, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_span_flush_queued_past_its_lifetime_on_the_write_gate_is_abandoned() {
    let gate = floor_zero_gate();
    let clock = TestClock::new(BASE_NS);
    let router = Arc::new(
        SpanIngestRouter::new(
            short_lifetime(),
            Arc::new(MemoryStore::new()),
            clock.clone(),
        )
        .with_write_gate(Arc::clone(&gate)),
    );
    let writer = Arc::clone(&router);
    let err = abandoned_in_the_gate_queue(gate, WriteSite::SpanFlush, clock, async move {
        writer
            .write(
                tenant("acme"),
                vec![span_on_shard(0, 1, 1_000)],
                WriteMode::Strict,
                ACK_DEADLINE,
            )
            .await
    })
    .await;
    assert!(
        matches!(&err, SpanWriteError::Abandoned(m) if m.contains("write gate")),
        "expected the gate-wait abandonment, got {err:?}"
    );
    assert_eq!(router.metrics().snapshot().abandoned_queue_deadline, 1);
}

async fn stored_objects(store: &dyn ObjectStoreBackend) -> Vec<(String, Vec<u8>)> {
    let mut out = Vec::new();
    for meta in list_all(store, "t/").await.expect("list") {
        let bytes = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get object")
            .data;
        out.push((meta.key, bytes.to_vec()));
    }
    out
}

/// An RSEG flush encoded on the write gate stores the same bytes as one encoded
/// inline: two routers with one seed and one pinned clock write the same
/// points, only one with the gate, and every stored object (data object and
/// commit record) matches byte for byte. The gate's single `metrics_flush` job
/// shows the gated router's one flush really ran there.
#[tokio::test]
async fn gated_metrics_flush_stores_the_same_objects_as_inline() {
    let gate = floor_zero_gate();
    let tenant = tenant("acme");
    let points: Vec<_> = (0..64)
        .map(|i| {
            make_point(
                &tenant,
                "cpu",
                &[("host", &i.to_string())],
                1_000 + i,
                i as f64,
            )
        })
        .collect();
    let mut stored = Vec::new();
    for gated in [false, true] {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let config = IngestConfig {
            target_bytes: 64 * 1024 * 1024,
            ..flush_on_first()
        };
        let router = IngestRouter::with_rng(
            config,
            Arc::clone(&store),
            Signal::Metrics,
            TestClock::new(BASE_NS),
            Arc::new(SeededRng::new(0x5EED)),
        );
        let router = if gated {
            router.with_write_gate(Arc::clone(&gate))
        } else {
            router
        };
        router
            .write(
                tenant.clone(),
                points.clone(),
                WriteMode::Buffered,
                ACK_DEADLINE,
            )
            .await
            .expect("buffered write enqueues");
        router.flush_all().await;
        stored.push(stored_objects(store.as_ref()).await);
        router.shutdown().await;
    }
    assert_eq!(stored[0].len(), 2, "one data object and one commit record");
    assert_eq!(
        stored[0], stored[1],
        "the gated encode must store byte-identical objects"
    );
    assert_eq!(counts(&gate, WriteSite::MetricsFlush), (1, 0));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_flush_encode_runs_through_the_write_gate() {
    let gate = floor_zero_gate();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let router = Arc::new(
        LogIngestRouter::new(
            flush_on_first(),
            Arc::clone(&store),
            TestClock::new(BASE_NS),
        )
        .with_write_gate(Arc::clone(&gate)),
    );
    let writer = Arc::clone(&router);
    assert_ack_waits_for_gated_flush(gate, WriteSite::LogFlush, store, async move {
        writer
            .write(
                tenant("acme"),
                vec![log_record("hello")],
                WriteMode::Strict,
                ACK_DEADLINE,
            )
            .await
            .is_ok()
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn span_flush_encode_runs_through_the_write_gate() {
    let gate = floor_zero_gate();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let router = Arc::new(
        SpanIngestRouter::new(
            flush_on_first(),
            Arc::clone(&store),
            TestClock::new(BASE_NS),
        )
        .with_write_gate(Arc::clone(&gate)),
    );
    let writer = Arc::clone(&router);
    assert_ack_waits_for_gated_flush(gate, WriteSite::SpanFlush, store, async move {
        writer
            .write(
                tenant("acme"),
                vec![span_on_shard(0, 1, 1_000)],
                WriteMode::Strict,
                ACK_DEADLINE,
            )
            .await
            .is_ok()
    })
    .await;
}
