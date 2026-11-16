//! `with_flush_input_bytes` records the figure each flush encode is sized by
//! (issue #2818): a write gate whose inline floor is that figure dispatches
//! the encode, and one a byte higher runs it inline. Built only with the
//! `flush-input-bytes` feature, which `stage-timing` turns on.
#![cfg(feature = "flush-input-bytes")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_commit::rng::SeededRng;
use ravel_cpu_gate::{CpuGateConfig, InstantClock, WriteGate, WriteSite};
use ravel_ingest::{FlushInputBytes, IngestConfig, IngestRouter, PendingAckBudget, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
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
