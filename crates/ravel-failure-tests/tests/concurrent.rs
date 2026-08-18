//! Concurrent ingest and query: every query's snapshot must be internally
//! consistent -- a commit's samples become visible atomically, never as a
//! partial subset (docs/consistency-model.md "Snapshot isolation";
//! docs/catalog-and-mvcc.md).
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use common::{TestClock, catalog, expect_matrix, make_point, tenant};
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use ravel_ingest::{IngestConfig, IngestRouter, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_types::Signal;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const BASE_NS: i64 = 1_700_000_000_000_000_000;

const BATCHES: usize = 20;
const SAMPLES_PER_BATCH: i64 = 10;

fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 1,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        ..IngestConfig::default()
    }
}

fn batch_start_ns(batch: usize) -> i64 {
    BASE_NS - 30 * NS_PER_MIN + (batch as i64) * NS_PER_MIN
}

/// Every one of a batch's samples, all committed together in a single
/// `write` call (one flush, one commit, one segment), must appear together
/// or not at all to a concurrently-running query -- never as a partial
/// count strictly between zero and the full batch size.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ingest_and_query_never_sees_a_partially_visible_flush() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let router = Arc::new(IngestRouter::new(
        config(),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    ));
    let tid = tenant("acme");

    let cat = Arc::new(catalog(Arc::clone(&store), 1));
    let engine = Arc::new(QueryEngine::new(
        Arc::clone(&cat),
        Arc::clone(&store),
        EngineConfig::default(),
    ));

    let producer_router = Arc::clone(&router);
    let producer_tenant = tid.clone();
    let producer = tokio::spawn(async move {
        for batch in 0..BATCHES {
            let start = batch_start_ns(batch);
            let points = (0..SAMPLES_PER_BATCH)
                .map(|i| {
                    make_point(
                        &producer_tenant,
                        "concurrent_metric",
                        &[("batch", &batch.to_string())],
                        start + i * NS_PER_SEC,
                        i as f64,
                    )
                })
                .collect();
            producer_router
                .write(
                    producer_tenant.clone(),
                    points,
                    WriteMode::Strict,
                    Duration::from_secs(5),
                )
                .await
                .expect("batch write must succeed");
        }
    });

    let mismatches = Arc::new(AtomicUsize::new(0));
    let mut rng = StdRng::seed_from_u64(0xC0FFEE);
    let mut polls = 0usize;
    while !producer.is_finished() {
        let batch = rng.random_range(0..BATCHES);
        let count = query_batch_sample_count(&engine, &tid, batch).await;
        if count != 0 && count != SAMPLES_PER_BATCH as usize {
            mismatches.fetch_add(1, Ordering::SeqCst);
        }
        polls += 1;
        tokio::task::yield_now().await;
    }
    producer.await.expect("producer task must not panic");

    assert_eq!(
        mismatches.load(Ordering::SeqCst),
        0,
        "a query observed a partially-visible flush (some but not all of a batch's samples)"
    );
    assert!(
        polls > 0,
        "the query loop must have actually run at least once"
    );

    // Final pass: every batch's full sample set must be visible now that
    // ingest has finished.
    for batch in 0..BATCHES {
        let count = query_batch_sample_count(&engine, &tid, batch).await;
        assert_eq!(
            count, SAMPLES_PER_BATCH as usize,
            "batch {batch} must be fully visible after ingest completes"
        );
    }
}

async fn query_batch_sample_count(
    engine: &QueryEngine,
    tid: &ravel_types::TenantId,
    batch: usize,
) -> usize {
    let start = batch_start_ns(batch);
    let end = start + (SAMPLES_PER_BATCH - 1) * NS_PER_SEC;
    let query = format!("concurrent_metric{{batch=\"{batch}\"}}");
    let (value, _coverage) = engine
        .range(
            tid.hash(),
            &query,
            start / 1_000_000,
            end / 1_000_000,
            NS_PER_SEC / 1_000_000,
            &[],
            BASE_NS,
            Duration::from_secs(5),
        )
        .await
        .expect("range query must not error");
    let result = expect_matrix(value);
    result.first().map_or(0, |(_, samples)| samples.len())
}
