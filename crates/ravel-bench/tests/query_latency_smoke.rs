//! Smoke tests for the `query_latency_bench` end-to-end PromQL query-latency
//! path:
//! `promql_instant_memory_smoke` and `promql_range_memory_smoke` always run
//! against an in-process `MemoryStore`; `promql_instant_range_minio_smoke`
//! runs the same path against a real MinIO endpoint, gated on
//! `RAVEL_MINIO_URL` exactly like `minio_ingest_read_smoke` in
//! tests/s3_e2e_smoke.rs -- same env var names, same skip-if-unset
//! convention.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::env;
use std::sync::Arc;

use ravel_bench::query_latency::{QueryLatencyConfig, run};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};

fn small_config(
    store: Arc<dyn ravel_object_store::ObjectStoreBackend>,
    label: &str,
) -> QueryLatencyConfig {
    QueryLatencyConfig {
        store,
        store_label: label.to_string(),
        shards: 1,
        target_series: 20,
        points_per_sec: 2_000,
        duration_secs: 1,
        batch_size: 50,
        ack_timeout_secs: 5,
        query: "bench_gauge".to_string(),
        instant_query_count: 3,
        range_query_count: 3,
        range_steps: 5,
    }
}

#[tokio::test]
async fn promql_instant_memory_smoke() {
    let config = small_config(Arc::new(MemoryStore::new()), "memory");
    let report = run(&config).await;

    assert!(
        report.accepted_points > 0,
        "promql_instant_memory_smoke must ingest a non-zero point count"
    );
    assert!(
        report.instant_matched_series > 0,
        "promql_instant_memory_smoke must be able to query back at least one series it just ingested"
    );
    assert!(
        report.instant_latency_ms.count > 0,
        "promql_instant_memory_smoke must report a non-zero instant query count"
    );
}

#[tokio::test]
async fn promql_range_memory_smoke() {
    let config = small_config(Arc::new(MemoryStore::new()), "memory");
    let report = run(&config).await;

    assert!(
        report.accepted_points > 0,
        "promql_range_memory_smoke must ingest a non-zero point count"
    );
    assert!(
        report.range_matched_series > 0,
        "promql_range_memory_smoke must be able to query back at least one series it just ingested"
    );
    assert!(
        report.range_latency_ms.count > 0,
        "promql_range_memory_smoke must report a non-zero range query count"
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
#[tokio::test]
async fn promql_instant_range_minio_smoke() {
    let Ok(url) = env::var("RAVEL_MINIO_URL") else {
        println!("skipping MinIO instant/range query smoke test: RAVEL_MINIO_URL not set");
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

    let config = S3Config {
        bucket,
        region,
        endpoint: Some(url),
        access_key_id,
        secret_access_key,
        allow_http,
        force_path_style: true,
        kms_key_id: None,
        session_token: None,
        credentials_file: None,
        auth: Default::default(),
        instance_metadata_endpoint: None,
    };
    let store = S3Store::new(config).expect("S3Store::new must succeed with a valid config");

    let query_config = small_config(Arc::new(store), "s3");
    let report = run(&query_config).await;

    assert!(
        report.accepted_points > 0,
        "promql_instant_range_minio_smoke must ingest a non-zero point count against real MinIO"
    );
    assert!(
        report.instant_matched_series > 0,
        "promql_instant_range_minio_smoke must be able to query back at least one series via instant query against real MinIO"
    );
    assert!(
        report.range_matched_series > 0,
        "promql_instant_range_minio_smoke must be able to query back at least one series via range query against real MinIO"
    );
}
