//! Issue #2818 trial instrumentation on the metrics flush.
//!
//! `with_flush_checksum_in_job` moves the data object's CRC32C upload checksum
//! from the flush task into the encode job. Routers with one seed and one
//! pinned clock write the same points with the switch off and on, gated and
//! ungated, through a `FaultStore` over a `MemoryStore` that records the
//! checksum of every PUT: the stored objects and the recorded checksums match
//! across every run.
//!
//! `with_flush_input_bytes` records the figure each flush encode is sized by:
//! a write gate whose inline floor is that figure dispatches the encode, and
//! one a byte higher runs it inline.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_commit::rng::SeededRng;
use ravel_cpu_gate::{CpuGateConfig, InstantClock, WriteGate, WriteSite};
use ravel_ingest::{FlushInputBytes, IngestConfig, IngestRouter, PendingAckBudget, WriteMode};
use ravel_object_store::fault::{FaultPlan, FaultStore};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, UploadChecksum, list_all};
use ravel_otlp::NormalizedPoint;
use ravel_types::Signal;

const BASE_NS: i64 = 1_700_000_000_000_000_000;
const ACK_DEADLINE: Duration = Duration::from_secs(60);

fn one_flush_config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 64 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

fn gate_with_floor(inline_floor_bytes: u64) -> Arc<WriteGate> {
    Arc::new(WriteGate::new(
        CpuGateConfig {
            inline_floor_bytes,
            ..CpuGateConfig::with_permits(1)
        },
        Arc::new(InstantClock::new()),
    ))
}

fn site_counts(gate: &WriteGate) -> (u64, u64) {
    gate.snapshot()
        .sites
        .iter()
        .find(|s| s.site == WriteSite::MetricsFlush)
        .map_or((0, 0), |s| (s.jobs, s.inline))
}

fn points() -> Vec<NormalizedPoint> {
    let tenant = tenant("acme");
    (0..64)
        .map(|i| {
            make_point(
                &tenant,
                "cpu",
                &[("host", &i.to_string())],
                1_000 + i,
                i as f64,
            )
        })
        .collect()
}

fn new_router(store: Arc<dyn ObjectStoreBackend>) -> IngestRouter {
    IngestRouter::with_rng(
        one_flush_config(),
        store,
        Signal::Metrics,
        TestClock::new(BASE_NS),
        Arc::new(SeededRng::new(0x5EED)),
        Arc::new(PendingAckBudget::new(0)),
    )
}

async fn write_and_flush(router: &IngestRouter) {
    router
        .write(tenant("acme"), points(), WriteMode::Buffered, ACK_DEADLINE)
        .await
        .expect("buffered write enqueues");
    router.flush_all().await;
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

struct Run {
    stored: Vec<(String, Vec<u8>)>,
    put_checksums: Vec<(String, Option<UploadChecksum>)>,
}

async fn run(gate: Option<Arc<WriteGate>>, checksum_in_job: bool) -> Run {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let dyn_store: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as _;
    let router = new_router(dyn_store);
    let router = match gate {
        Some(gate) => router.with_write_gate(gate),
        None => router,
    };
    let router = router.with_flush_checksum_in_job(checksum_in_job);
    write_and_flush(&router).await;
    let run = Run {
        stored: stored_objects(store.as_ref()).await,
        put_checksums: store.put_checksums(),
    };
    router.shutdown().await;
    run
}

#[tokio::test]
async fn checksum_in_job_stores_the_same_bytes_and_sends_the_same_checksum() {
    let stock = run(None, false).await;
    assert_eq!(
        stock.stored.len(),
        2,
        "one data object and one commit record"
    );
    let (data_key, data_bytes) = stock
        .stored
        .iter()
        .find(|(key, _)| !key.contains("/c/"))
        .expect("a data object");
    let data_puts: Vec<_> = stock
        .put_checksums
        .iter()
        .filter(|(key, _)| key == data_key)
        .collect();
    assert_eq!(data_puts.len(), 1, "one data-object PUT");
    assert_eq!(
        data_puts[0].1,
        Some(UploadChecksum::Crc32c(crc32c::crc32c(data_bytes))),
        "the PUT carries the CRC32C of the stored bytes"
    );

    let gated_off_gate = gate_with_floor(0);
    let gated_off = run(Some(Arc::clone(&gated_off_gate)), false).await;
    let gated_on_gate = gate_with_floor(0);
    let gated_on = run(Some(Arc::clone(&gated_on_gate)), true).await;
    let ungated_on = run(None, true).await;
    for (name, other) in [
        ("gated, switch off", &gated_off),
        ("gated, switch on", &gated_on),
        ("ungated, switch on", &ungated_on),
    ] {
        assert_eq!(
            other.stored, stock.stored,
            "{name}: stored objects match the stock run byte for byte"
        );
        assert_eq!(
            other.put_checksums, stock.put_checksums,
            "{name}: every PUT sends the stock run's key and checksum"
        );
    }
    assert_eq!(site_counts(&gated_off_gate), (1, 0));
    assert_eq!(site_counts(&gated_on_gate), (1, 0));
}

#[tokio::test]
async fn flush_input_bytes_is_the_figure_the_inline_floor_compares() {
    let recorder = FlushInputBytes::default();
    let router = new_router(Arc::new(MemoryStore::new())).with_flush_input_bytes(recorder.clone());
    write_and_flush(&router).await;
    router.shutdown().await;
    let sizes = recorder.take();
    assert_eq!(sizes.len(), 1, "one flush, one recorded size");
    let size = sizes[0];
    assert!(size > 0);
    assert!(recorder.take().is_empty(), "take empties the list");

    for (floor, expected) in [(size, (1, 0)), (size + 1, (0, 1))] {
        let gate = gate_with_floor(floor);
        let recorder = FlushInputBytes::default();
        let router = new_router(Arc::new(MemoryStore::new()))
            .with_write_gate(Arc::clone(&gate))
            .with_flush_input_bytes(recorder.clone());
        write_and_flush(&router).await;
        router.shutdown().await;
        assert_eq!(recorder.take(), vec![size]);
        assert_eq!(
            site_counts(&gate),
            expected,
            "floor {floor} against a {size}-byte flush"
        );
    }
}
