//! Smoke tests for the `s3_e2e_bench` end-to-end ingest+query path:
//! `memory_smoke` always
//! runs against an in-process `MemoryStore`; `minio_ingest_read_smoke` runs
//! the same path against a real MinIO endpoint, gated on `RAVEL_MINIO_URL`
//! exactly like `minio_contract` in
//! crates/ravel-object-store/tests/contract.rs -- same env var names, same
//! skip-if-unset convention -- not the `RAVEL_S3_*` convention
//! `ravel_bench::harness` uses for the bin's own `--store s3` flag.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::env;
use std::sync::Arc;

use ravel_bench::e2e::{E2eConfig, run};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};

fn small_config(store: Arc<dyn ravel_object_store::ObjectStoreBackend>, label: &str) -> E2eConfig {
    E2eConfig {
        store,
        store_label: label.to_string(),
        shards: 1,
        target_series: 20,
        points_per_sec: 2_000,
        duration_secs: 1,
        batch_size: 50,
        ack_timeout_secs: 5,
        query: "bench_gauge".to_string(),
        query_count: 3,
    }
}

#[tokio::test]
async fn memory_smoke() {
    let config = small_config(Arc::new(MemoryStore::new()), "memory");
    let report = run(&config).await;

    assert!(
        report.accepted_points > 0,
        "memory_smoke must ingest a non-zero point count"
    );
    assert!(
        report.query_matched_series > 0,
        "memory_smoke must be able to query back at least one series it just ingested"
    );
}

/// Real MinIO conformance smoke test. Gated on `RAVEL_MINIO_URL` so the
/// suite skips cleanly wherever no MinIO is reachable (e.g. this sandbox,
/// most laptops, unconfigured CI runners) -- see `minio_contract` in
/// crates/ravel-object-store/tests/contract.rs for the same gate.
///
/// Optional overrides: `RAVEL_MINIO_BUCKET` (must already exist -- this
/// crate does not create buckets), `RAVEL_MINIO_ACCESS_KEY`,
/// `RAVEL_MINIO_SECRET_KEY`, `RAVEL_MINIO_REGION`.
#[tokio::test]
async fn minio_ingest_read_smoke() {
    let Ok(url) = env::var("RAVEL_MINIO_URL") else {
        println!("skipping MinIO ingest+read smoke test: RAVEL_MINIO_URL not set");
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
    };
    let store = S3Store::new(config).expect("S3Store::new must succeed with a valid config");

    let e2e_config = small_config(Arc::new(store), "s3");
    let report = run(&e2e_config).await;

    assert!(
        report.accepted_points > 0,
        "minio_ingest_read_smoke must ingest a non-zero point count against real MinIO"
    );
    assert!(
        report.query_matched_series > 0,
        "minio_ingest_read_smoke must be able to query back at least one series it just ingested"
    );
}
