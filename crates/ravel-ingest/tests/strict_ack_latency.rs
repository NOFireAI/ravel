//! Strict-acknowledgement latency floor of the log ingest path (issue #2592,
//! experiment E1).
//!
//! The shard loop and the store run on paused tokio time: the injected
//! [`Clock`] reads tokio's clock, the flush tick sleeps on it, and every PUT is
//! held for [`PUT_HOLD`] of it through a [`FaultStore::hold`] gate. A paused
//! runtime advances time only when every task is parked, so the latencies
//! below are the protocol's floor (tick cadence, age threshold, two PUT round
//! trips) with encode and merge CPU counted as zero.
#![allow(clippy::expect_used)]

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use ravel_commit::{keys, record};
use ravel_ingest::{Clock, IngestConfig, LogIngestRouter, LogWriteError, WriteMode};
use ravel_logseg::{Predicate, RlogConfig, RlogReader, stream_attrs_bytes};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, GateHandle, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{CommitToken, Signal, TenantId};
use tokio::task::JoinHandle;
use tokio::time::Instant;

const BASE_NS: i64 = 1_700_000_000_000_000_000;
const PUT_HOLD: Duration = Duration::from_millis(20);
const WRITES: usize = 20;

/// Injected clock on tokio's (paused) time, so the age check, the flush tick
/// and the PUT holds share one timeline.
struct TokioClock {
    start: Instant,
}

impl Clock for TokioClock {
    fn now_ns(&self) -> i64 {
        BASE_NS + i64::try_from(self.start.elapsed().as_nanos()).expect("elapsed fits i64")
    }
}

type Store = InstrumentedStore<FaultStore<MemoryStore>>;

/// A store whose every PUT is held, plus the gate handle that holds them.
fn held_store() -> (Arc<Store>, GateHandle) {
    let fault = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
    let gate = fault.hold(Op::Put, None, Occurrence::Always);
    (Arc::new(InstrumentedStore::new(fault)), gate)
}

/// Releases every held call `PUT_HOLD` after it is first seen held. Parks on
/// the gate while nothing is held, so paused time can jump across the age
/// window; polls at 1 ms while a release is pending, so a seen call waits at
/// most `PUT_HOLD` plus 1 ms.
fn spawn_releaser(gate: GateHandle) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut scheduled: HashSet<u64> = HashSet::new();
        loop {
            gate.wait_until_held(1).await;
            for id in gate.held() {
                if scheduled.insert(id) {
                    let gate = gate.clone();
                    tokio::spawn(async move {
                        tokio::time::sleep(PUT_HOLD).await;
                        gate.release(id);
                    });
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
}

fn norm_record(body: &str, ts_ns: i64) -> NormalizedLogRecord {
    let res = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope", "", &scope_attrs),
        stream_attrs: stream_attrs_bytes(&res, "scope", "", &scope_attrs),
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

fn put_calls(store: &Store) -> u64 {
    store.metrics().snapshot().op(StoreOp::Put).calls
}

/// Data objects and commit records in the store.
async fn object_counts(store: &Store) -> (usize, usize) {
    let objects = list_all(store.inner().inner(), "t/").await.expect("list");
    let data = objects.iter().filter(|o| o.key.contains("/l0/")).count();
    let commits = objects.iter().filter(|o| o.key.contains("/c/")).count();
    (data, commits)
}

/// Per-arm result: sorted latencies and the PUT count each write cost.
struct Arm {
    sorted: Vec<Duration>,
    puts_per_write: Vec<u64>,
    data_objects: usize,
    commit_records: usize,
}

impl Arm {
    /// Nearest-rank median.
    fn p50(&self) -> Duration {
        self.sorted[(self.sorted.len() - 1) / 2]
    }
}

/// One shard, every PUT held `PUT_HOLD`, `WRITES` strict single-record writes
/// issued one after another, each awaiting its ack before the next is sent.
async fn measure(label: &str, config: IngestConfig) -> Arm {
    let (store, gate) = held_store();
    let releaser = spawn_releaser(gate);
    let clock: Arc<dyn Clock> = Arc::new(TokioClock {
        start: Instant::now(),
    });
    let router = LogIngestRouter::new(
        IngestConfig {
            shard_count: 1,
            ..config
        },
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        Arc::clone(&clock),
    );
    let tenant = TenantId::new("acme");

    let mut latencies = Vec::with_capacity(WRITES);
    let mut puts_per_write = Vec::with_capacity(WRITES);
    for i in 0..WRITES {
        let puts_before = put_calls(&store);
        let sent = Instant::now();
        let receipt = router
            .write(
                tenant.clone(),
                vec![norm_record(&format!("write-{i}"), clock.now_ns())],
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await
            .expect("strict write acks");
        latencies.push(sent.elapsed());
        assert_eq!(receipt.tokens.len(), 1, "one shard, one token");
        puts_per_write.push(put_calls(&store) - puts_before);
    }
    let (data_objects, commit_records) = object_counts(&store).await;
    router.shutdown().await;
    releaser.abort();

    let mut sorted = latencies;
    sorted.sort_unstable();
    let arm = Arm {
        sorted,
        puts_per_write,
        data_objects,
        commit_records,
    };
    println!(
        "[{label}] sorted ack latencies (ms): {:?}",
        arm.sorted
            .iter()
            .map(|d| d.as_secs_f64() * 1e3)
            .collect::<Vec<_>>()
    );
    println!("[{label}] p50 = {:.1} ms", arm.p50().as_secs_f64() * 1e3);
    println!(
        "[{label}] PUTs per write = {:?}; data objects = {}, commit records = {}",
        arm.puts_per_write, arm.data_objects, arm.commit_records
    );
    arm
}

fn assert_two_puts_per_write(arm: &Arm) {
    assert!(
        arm.puts_per_write.iter().all(|&n| n == 2),
        "each strict write must cost exactly 2 PUTs: {:?}",
        arm.puts_per_write
    );
    assert_eq!(
        (arm.data_objects, arm.commit_records),
        (WRITES, WRITES),
        "one data object and one commit record per write"
    );
}

/// With the default configuration a strict write at an empty buffer waits the
/// whole `max_flush_delay` age window before its flush opens, so the median
/// ack is at least that window. With `strict_waiter_flushes_immediately` on,
/// the buffer is due on the next `flush_tick`, so every ack lands within one
/// tick plus the two held PUTs. Both arms cost one data PUT and one commit PUT
/// per write.
#[tokio::test(start_paused = true)]
async fn strict_ack_latency_floor() {
    let config = IngestConfig::default();
    let max_flush_delay = config.max_flush_delay;
    let flush_tick = config.flush_tick;

    let default_arm = measure("default", config).await;
    assert!(
        default_arm.p50() >= max_flush_delay,
        "default p50 {:?} must be at least max_flush_delay {:?}",
        default_arm.p50(),
        max_flush_delay
    );
    assert_two_puts_per_write(&default_arm);

    let flag_arm = measure(
        "strict_waiter_flushes_immediately",
        IngestConfig {
            strict_waiter_flushes_immediately: true,
            ..config
        },
    )
    .await;
    // The releaser polls at 1 ms while a release is pending, so a held PUT
    // takes up to `PUT_HOLD` plus 1 ms.
    let bound = flush_tick + 2 * (PUT_HOLD + Duration::from_millis(1));
    let slowest = *flag_arm.sorted.last().expect("measured writes");
    assert!(
        slowest <= bound,
        "with the flag on every ack must land within one flush_tick plus two held PUTs \
         ({bound:?}); slowest was {slowest:?}"
    );
    assert!(flag_arm.p50() < max_flush_delay);
    assert_two_puts_per_write(&flag_arm);
}

fn flag_on() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        strict_waiter_flushes_immediately: true,
        ..IngestConfig::default()
    }
}

fn tokio_clock() -> Arc<dyn Clock> {
    Arc::new(TokioClock {
        start: Instant::now(),
    })
}

/// With the flag on, a buffered write at an otherwise idle buffer carries no
/// strict waiter, so it keeps the idle age threshold (`max_flush_delay_idle`,
/// the buffer holds far less than `min_flush_bytes`): no PUT through one tick
/// short of that threshold, then exactly one flush once it passes.
#[tokio::test(start_paused = true)]
async fn buffered_write_keeps_idle_cadence_with_flag_on() {
    let config = flag_on();
    let idle = config.max_flush_delay_idle;
    let tick = config.flush_tick;
    assert!(idle > config.max_flush_delay);
    let (store, gate) = held_store();
    let releaser = spawn_releaser(gate);
    let clock = tokio_clock();
    let router = LogIngestRouter::new(
        config,
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        Arc::clone(&clock),
    );
    let start = Instant::now();
    router
        .write(
            TenantId::new("acme"),
            vec![norm_record("buffered", clock.now_ns())],
            WriteMode::Buffered,
            Duration::from_secs(60),
        )
        .await
        .expect("buffered write enqueues");

    tokio::time::sleep_until(start + idle - tick).await;
    assert_eq!(
        put_calls(&store),
        0,
        "a buffered-only buffer must not flush before max_flush_delay_idle"
    );

    tokio::time::sleep_until(start + idle + tick + 2 * (PUT_HOLD + Duration::from_millis(1))).await;
    assert_eq!(put_calls(&store), 2, "the idle age trigger fires once");
    assert_eq!(object_counts(&store).await, (1, 1));
    assert_eq!(router.metrics().snapshot().flushes_by_age, 1);

    router.shutdown().await;
    releaser.abort();
}

/// With the flag on, the strict ack still waits for the commit record: a
/// commit-record PUT that fails permanently leaves the data object stored and
/// answers the strict waiter with an error, not a token.
#[tokio::test(start_paused = true)]
async fn strict_ack_waits_for_commit_put_with_flag_on() {
    let fault = FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("commit down".into()))
                .with_key_contains("/c/"),
        ),
    );
    let store = Arc::new(InstrumentedStore::new(fault));
    let router = LogIngestRouter::new(
        flag_on(),
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        tokio_clock(),
    );
    let result = router
        .write(
            TenantId::new("acme"),
            vec![norm_record("strict", BASE_NS)],
            WriteMode::Strict,
            Duration::from_secs(60),
        )
        .await;
    assert!(
        matches!(result, Err(LogWriteError::Abandoned(_))),
        "a failed commit PUT must fail the strict ack, got {result:?}"
    );
    assert_eq!(
        store.inner().fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the commit PUT fault must have fired"
    );
    assert_eq!(
        object_counts(&store).await,
        (1, 0),
        "data object stored, no commit record"
    );
    router.shutdown().await;
}

/// Every record a token's RLOG object holds, by body.
async fn bodies_behind(store: &Store, tenant: &TenantId, token: &CommitToken) -> Vec<String> {
    let backend = store.inner().inner();
    let commit_key =
        keys::commit_key_for_token(&tenant.hash(), Signal::Logs, token).expect("commit key");
    let commit = backend
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record");
    let rec = record::decode(&commit.data).expect("decode commit record");
    let data = backend
        .get(&rec.object_key, GetRange::Full)
        .await
        .expect("get data object");
    let reader = RlogReader::new(&data.data, &RlogConfig::default()).expect("open rlog");
    let (records, _stats) = reader
        .scan(&Predicate::And(Vec::new()))
        .expect("unfiltered scan");
    records.into_iter().map(|r| r.body).collect()
}

/// Eight concurrent strict writes to one shard, with the flag on, share
/// flushes: writers 0..4 start first and the first data PUT is held until
/// writers 4..8 have been buffered behind it, so at least two flushes run and
/// the late writers arrive while the first is in flight. Fewer flushes than
/// writes, two PUTs per flush, and every writer's token names an object that
/// holds that writer's record.
#[tokio::test(start_paused = true)]
async fn concurrent_strict_writes_share_flushes() {
    const WRITERS: usize = 8;
    let fault = FaultStore::new(MemoryStore::new(), FaultPlan::empty());
    let gate = fault.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
    let store = Arc::new(InstrumentedStore::new(fault));
    let router = Arc::new(LogIngestRouter::new(
        flag_on(),
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        tokio_clock(),
    ));
    let tenant = TenantId::new("acme");

    let spawn_writer = |i: usize| {
        let router = Arc::clone(&router);
        let tenant = tenant.clone();
        tokio::spawn(async move {
            router
                .write(
                    tenant,
                    vec![norm_record(&format!("writer-{i}"), BASE_NS)],
                    WriteMode::Strict,
                    Duration::from_secs(60),
                )
                .await
        })
    };
    let mut writers: Vec<_> = (0..WRITERS / 2).map(spawn_writer).collect();
    gate.wait_until_held(1).await;
    writers.extend((WRITERS / 2..WRITERS).map(spawn_writer));
    while router.metrics().snapshot().buffered_records_total < WRITERS as u64 {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let held = gate.held();
    assert_eq!(held.len(), 1, "only the first data PUT is held");
    gate.release(held[0]);

    for (i, writer) in writers.into_iter().enumerate() {
        let receipt = writer
            .await
            .expect("writer task")
            .expect("strict write acks");
        assert_eq!(receipt.tokens.len(), 1);
        let bodies = bodies_behind(&store, &tenant, &receipt.tokens[0]).await;
        assert!(
            bodies.contains(&format!("writer-{i}")),
            "writer {i} was acked with a token whose object does not hold its record: {bodies:?}"
        );
    }

    let (data, commits) = object_counts(&store).await;
    let puts = put_calls(&store);
    println!("coalescing: {WRITERS} strict writes, {data} flushes, {puts} PUTs");
    assert_eq!(data, commits, "one commit record per data object");
    assert!(
        (2..WRITERS).contains(&data),
        "8 strict writes must share flushes, and the late writers need a second one: \
         {data} flushes"
    );
    assert_eq!(puts, 2 * data as u64, "two PUTs per flush");

    let router = Arc::try_unwrap(router).ok().expect("writers joined");
    router.shutdown().await;
}
