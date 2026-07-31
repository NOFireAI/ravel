//! Smoke tests for the `concurrent_bench` readers-and-writers +
//! cold/warm-cache path (docs/benchmarking.md "End-to-end", issue #269):
//! `readers_writers_memory_smoke` always runs against an in-process
//! `MemoryStore`; `readers_writers_minio_smoke` runs the same path against a
//! real MinIO endpoint, gated on `RAVEL_MINIO_URL` exactly like
//! `minio_ingest_read_smoke` in tests/s3_e2e_smoke.rs -- same env var names,
//! same skip-if-unset convention.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::env;
use std::sync::Arc;

use ravel_bench::concurrent::{ConcurrentConfig, run};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};

fn small_config(
    store: Arc<dyn ravel_object_store::ObjectStoreBackend>,
    label: &str,
) -> ConcurrentConfig {
    ConcurrentConfig {
        store,
        store_label: label.to_string(),
        shards: 1,
        writers: 2,
        readers: 2,
        target_series: 20,
        points_per_sec: 2_000,
        duration_secs: 2,
        batch_size: 50,
        ack_timeout_secs: 5,
        query: "bench_gauge".to_string(),
    }
}

/// Two writer tasks and two reader tasks are spawned before any of them is
/// awaited (`ravel_bench::concurrent::run`), so they run concurrently for
/// the whole `duration_secs` window, not one after another; this asserts
/// the outcome every role must reach under that real contention, per-role
/// rather than as one blended average.
///
/// `flavor = "multi_thread"` is required, not cosmetic: the plain
/// `#[tokio::test]` default is a single-threaded runtime, where the
/// router's background per-shard flush task and this test's tight reader
/// polling loop only interleave at `.await` points on one thread. That
/// starves the age-based flush tick badly enough that no data lands before
/// `duration_secs` elapses. `concurrent_bench`'s `#[tokio::main]` already
/// gets a genuine multi-threaded runtime (workspace `tokio` has
/// `rt-multi-thread` on), so this matches it instead of silently testing a
/// different concurrency model than the bin actually runs under.
#[tokio::test(flavor = "multi_thread")]
async fn readers_writers_memory_smoke() {
    let config = small_config(Arc::new(MemoryStore::new()), "memory");
    let report = run(&config).await;

    assert_eq!(report.writers.len(), 2, "must run exactly 2 writer tasks");
    assert_eq!(report.readers.len(), 2, "must run exactly 2 reader tasks");

    for writer in &report.writers {
        assert!(
            writer.accepted_points > 0,
            "writer {} must accept a non-zero point count",
            writer.writer_id
        );
    }
    for reader in &report.readers {
        assert!(
            reader.non_empty_results > 0,
            "reader {} must see a non-empty query result at least once",
            reader.reader_id
        );
    }

    assert!(
        report.cache.cold_matched_series > 0,
        "cold query must match series"
    );
    assert!(
        report.cache.warm_matched_series > 0,
        "warm query must match series"
    );
}

/// Real MinIO conformance smoke test. Gated on `RAVEL_MINIO_URL` so the
/// suite skips cleanly wherever no MinIO is reachable (e.g. this sandbox,
/// most laptops, unconfigured CI runners) -- see `minio_ingest_read_smoke`
/// in tests/s3_e2e_smoke.rs for the same gate.
///
/// Optional overrides: `RAVEL_MINIO_BUCKET` (must already exist -- this
/// crate does not create buckets), `RAVEL_MINIO_ACCESS_KEY`,
/// `RAVEL_MINIO_SECRET_KEY`, `RAVEL_MINIO_REGION`.
///
/// `flavor = "multi_thread"` for the same reason as
/// `readers_writers_memory_smoke`: a single-threaded runtime starves the
/// router's background flush task against this test's reader polling loop.
#[tokio::test(flavor = "multi_thread")]
async fn readers_writers_minio_smoke() {
    let Ok(url) = env::var("RAVEL_MINIO_URL") else {
        println!("skipping MinIO readers/writers smoke test: RAVEL_MINIO_URL not set");
        return;
    };
    let bucket =
        env::var("RAVEL_MINIO_BUCKET").unwrap_or_else(|_| "ravel-object-store-test".to_string());
    let access_key_id =
        env::var("RAVEL_MINIO_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let secret_access_key =
        env::var("RAVEL_MINIO_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let region = env::var("RAVEL_MINIO_REGION").unwrap_or_else(|_| "us-east-1".to_string());
    let allow_http = url.starts_with("http://");

    let s3_config = S3Config {
        bucket,
        region,
        endpoint: Some(url),
        access_key_id,
        secret_access_key,
        allow_http,
        force_path_style: true,
    };
    let store = S3Store::new(s3_config).expect("S3Store::new must succeed with a valid config");

    let config = small_config(Arc::new(store), "s3");
    let report = run(&config).await;

    for writer in &report.writers {
        assert!(
            writer.accepted_points > 0,
            "writer {} must accept a non-zero point count against real MinIO",
            writer.writer_id
        );
    }
    for reader in &report.readers {
        assert!(
            reader.non_empty_results > 0,
            "reader {} must see a non-empty query result at least once against real MinIO",
            reader.reader_id
        );
    }
}
