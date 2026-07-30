//! End-to-end coverage for the background catalog fold task
//! (docs/metric-index-plan.md section 4): ingest a metric into an
//! already-sealed hour, let the fold task run against a real timer, then
//! confirm HEAD was written directly against the store (bypassing the query
//! path, which is already covered by `integration.rs`).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};

const TOKEN: &str = "testtoken";

/// Sealed well past the default `max_flush_lifetime (1h) + clock_skew_allowance
/// (5m) + fold_safety_margin (15m)` bound (docs/metric-index-plan.md section 2),
/// so the fold task treats this ingest hour as immutable on its first tick.
const SEALED_AGE: Duration = Duration::from_secs(3 * 60 * 60);

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

/// HEAD object key (docs/catalog-and-mvcc.md key layout, frozen format).
/// Duplicated from `ravel_server::fold`'s private helper: this test only
/// ever reads the object, never constructs it for a real fold.
fn head_key(tenant_hash_hex: &str, signal: Signal) -> String {
    format!("t/{tenant_hash_hex}/catalog/{}/HEAD", signal.key_prefix())
}

#[tokio::test]
async fn background_fold_writes_head_for_a_sealed_hour() {
    let mut tokens = HashMap::new();
    let tenant = TenantId::new("acme");
    tokens.insert(TOKEN.to_string(), tenant.clone());
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();

    let config = ServerConfig {
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        fold_tenants: vec![tenant.hash()],
        fold: FoldTaskConfig {
            enabled: true,
            fold_interval: Duration::from_millis(200),
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
    };
    let running = ravel_server::start(config, store_dyn)
        .await
        .expect("server starts");

    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    let sealed_ts_ns = now_ns() - i64::try_from(SEALED_AGE.as_nanos()).expect("fits i64");
    let request = export_request("cpu_usage", "demo", 42.5, sealed_ts_ns);
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("export request succeeds");
    assert_eq!(response.status(), 200, "export should succeed");

    let key = head_key(&tenant.hash().to_hex(), Signal::Metrics);
    let mut head_bytes = None;
    for _ in 0..50 {
        if let Ok(got) = store.get(&key, GetRange::Full).await {
            head_bytes = Some(got.data);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let head_bytes = head_bytes.expect("fold task writes HEAD within the polling window");
    let head = ravel_catalog::decode_head(&head_bytes).expect("HEAD decodes");
    assert!(
        head.watermark_hour > 0,
        "fold should have sealed the ingested hour"
    );

    running.shutdown().await.expect("graceful shutdown");
}
