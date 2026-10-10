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

use ravel_ingest::{Clock, IngestConfig, LogIngestRouter, WriteMode};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::instrument::{InstrumentedStore, StoreOp};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::TenantId;
use ravel_types::logstream::{AttrValue, log_stream_id};
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

/// With the default configuration a strict write at an empty buffer waits the
/// whole `max_flush_delay` age window before its flush opens, so the median
/// ack is at least that window, and each write costs one data PUT and one
/// commit PUT.
#[tokio::test(start_paused = true)]
async fn strict_ack_latency_floor() {
    let config = IngestConfig::default();
    let max_flush_delay = config.max_flush_delay;
    let default_arm = measure("default", config).await;
    assert!(
        default_arm.p50() >= max_flush_delay,
        "default p50 {:?} must be at least max_flush_delay {:?}",
        default_arm.p50(),
        max_flush_delay
    );
    assert!(
        default_arm.puts_per_write.iter().all(|&n| n == 2),
        "each strict write must cost exactly 2 PUTs: {:?}",
        default_arm.puts_per_write
    );
    assert_eq!(
        (default_arm.data_objects, default_arm.commit_records),
        (WRITES, WRITES),
        "one data object and one commit record per write"
    );
}
