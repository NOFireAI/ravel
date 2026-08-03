//! Regression test for issue #59 / finding a7-F02: a storage-layer failure
//! must surface to the client as a stable, typed, redacted message that
//! carries no object key, tenant hash, or raw object-store text, while the
//! full detail still reaches the server log.
//!
//! Adapted from the `#[ignore]`d a7-F02 reproducer on branch `audit/repro-a7`
//! (`store_error_body_leaks_object_key`). That reproducer asserted the defect:
//! the 503 body contained the `t/<tenant_hash>/.../*.rseg` key verbatim. This
//! version inverts the assertion (the body must contain none of those tokens)
//! and additionally captures the server's `tracing` output to prove the full
//! detail, including the object key, is preserved for the operator.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_server::{FoldTaskConfig, Mode, Running, ServerConfig};
use ravel_types::TenantId;
use tracing_subscriber::fmt::MakeWriter;

const TOKEN: &str = "testtoken";

/// A `tracing` writer that appends every emitted byte to a shared buffer so
/// the test can assert what the server logged.
#[derive(Clone)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl io::Write for CapturedLog {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .lock()
            .expect("log buffer lock")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturedLog {
    type Writer = CapturedLog;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

fn export_request(
    metric_name: &str,
    job: &str,
    value: f64,
    ts_ns: i64,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", job)],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: ts_ns as u64,
                            value: Some(NumberValue::AsDouble(value)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

async fn start_test_server() -> (Running, Arc<MemoryStore>) {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
    let config = ServerConfig {
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
    };
    let running = ravel_server::start(
        config,
        store_dyn,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    (running, store)
}

async fn ingest_one(base: &str, client: &reqwest::Client, metric: &str) -> String {
    let request = export_request(metric, "demo", 42.5, now_ns());
    let body = request.encode_to_vec();
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(body)
        .send()
        .await
        .expect("export request succeeds");
    assert_eq!(response.status(), 200, "export should succeed");
    response
        .headers()
        .get("x-ravel-commit-token")
        .expect("commit token header present")
        .to_str()
        .expect("commit token header is ascii")
        .to_string()
}

/// issue #59 / a7-F02: forcing a segment decode failure must yield a 503 whose
/// body is a stable, redacted message with no internal identifiers, while the
/// server log still carries the full detail (the object key) for diagnosis.
#[tokio::test]
async fn storage_failure_body_is_redacted_but_log_keeps_detail() {
    // Capture the server's tracing output for the duration of this test. One
    // test file is one test binary, so this global default has no contention
    // with other tests.
    let buffer = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_writer(CapturedLog(buffer.clone()))
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("install capturing subscriber");

    let (running, store) = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let commit_token = ingest_one(&base, &client, "cpu_usage").await;

    // Corrupt the committed data object so the fetch path fails to decode it.
    let objects = list_all(store.as_ref(), "t/").await.expect("list objects");
    let rseg_key = objects
        .iter()
        .map(|meta| meta.key.clone())
        .find(|key| key.ends_with(".rseg"))
        .expect("a committed .rseg data object exists");
    let tenant_hash = rseg_key
        .strip_prefix("t/")
        .and_then(|rest| rest.split('/').next())
        .expect("key has a t/<tenant_hash>/ prefix")
        .to_string();
    store
        .put(
            &rseg_key,
            Bytes::from_static(b"this is not a valid RSEG segment"),
            PutOptions::default(),
        )
        .await
        .expect("overwrite segment");

    // The min_commit_token pins the corrupt segment into the snapshot.
    let response = client
        .get(format!("{base}/api/v1/query"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", "cpu_usage"),
            ("min_commit_token", commit_token.as_str()),
        ])
        .send()
        .await
        .expect("query request completes");

    // A corrupt segment is a permanent server-side data fault, so it maps to
    // the non-retryable 500 `internal`, not the retryable 503 (issue #62,
    // a7-F05). The redaction of the body content (below) is unchanged (#59).
    assert_eq!(
        response.status(),
        500,
        "a corrupt segment yields non-retryable 500"
    );
    let body: serde_json::Value = response.json().await.expect("error body is JSON");
    let error = body["error"].as_str().expect("error is a string");

    // The client-visible message leaks no internal identifiers.
    assert!(
        !error.contains("t/"),
        "object key prefix leaked into client body: {error}"
    );
    assert!(
        !error.contains(".rseg"),
        "segment suffix leaked into client body: {error}"
    );
    assert!(
        !error.contains(&rseg_key),
        "full object key leaked into client body: {error}"
    );
    assert!(
        !error.contains(&tenant_hash),
        "tenant hash leaked into client body: {error}"
    );
    // It is a stable, typed message, not an empty or raw-passthrough string.
    assert!(
        !error.is_empty() && error.chars().all(|c| c.is_ascii() && !c.is_control()),
        "message should be a stable printable string: {error:?}"
    );

    running.shutdown().await.expect("graceful shutdown");

    // The full detail survives for the operator: the server log carries the
    // object key that was withheld from the client.
    let logged = String::from_utf8(buffer.lock().expect("log buffer lock").clone())
        .expect("log output is utf-8");
    assert!(
        logged.contains(&rseg_key),
        "server log should retain the full object key for diagnosis; log was:\n{logged}"
    );
    assert!(
        logged.contains("redacted"),
        "server log should record that the client message was redacted; log was:\n{logged}"
    );
}
