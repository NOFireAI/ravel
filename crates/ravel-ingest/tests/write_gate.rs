//! ADR-1702 task 9: every shard flush's encode runs on the write gate, and a
//! strict acknowledgement still waits for the flush it depends on.
//!
//! Each test parks the gate's only permit on an unrelated job, starts a strict
//! write that flushes on its first record, and waits until the flush's encode
//! is queued behind the parked job. At that point the write has not returned
//! and no commit record exists. Releasing the permit lets the encode run, the
//! flush publish and the write return, and the commit record is then in the
//! store. The flush site's `jobs` counter moves by exactly one and its
//! `inline` counter stays at zero, since the gate's inline floor is 0.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::future::Future;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use common::{TestClock, make_point, span_on_shard, tenant};
use ravel_commit::rng::SeededRng;
use ravel_cpu_gate::{CpuGateConfig, InstantClock, JobSize, WriteGate, WriteSite};
use ravel_ingest::{IngestConfig, IngestRouter, LogIngestRouter, SpanIngestRouter, WriteMode};
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
async fn assert_ack_waits_for_gated_flush<F>(
    gate: Arc<WriteGate>,
    site: WriteSite,
    store: Arc<dyn ObjectStoreBackend>,
    write: F,
) where
    F: Future<Output = bool> + Send + 'static,
{
    let (release, parked) = mpsc::channel::<()>();
    let blocker_gate = Arc::clone(&gate);
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
