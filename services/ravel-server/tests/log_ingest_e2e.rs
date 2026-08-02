//! End-to-end coverage for OTLP log ingest: real HTTP and gRPC exports
//! against an in-process server backed by `MemoryStore`, followed by reading
//! the flushed RLOG object straight out of the object store with
//! `ravel_logseg::RlogReader`.
//!
//! There is no query surface for logs yet (phase 3 wires the fold task and
//! snapshot resolution; `services/ravel-server/src/fold.rs` still folds
//! `Signal::Metrics` only), so durability is proven the way phase 1 proved
//! the format: list the objects the write produced, GET the bytes, and decode
//! them. Nothing here reads back through an in-process handle to the router.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_logseg::{Predicate, RlogConfig, RlogReader};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;
use ravel_types::logstream::AttrValue;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

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

fn export_request(body: &str, ts_ns: i64) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "checkout")],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: ts_ns as u64,
                    observed_time_unix_nano: ts_ns as u64,
                    severity_number: 9,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue(body.to_string())),
                    }),
                    attributes: vec![string_kv("http.route", "/checkout")],
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            ..Default::default()
        }],
    }
}

/// Starts a full server and hands back the object store it writes through, so
/// a test can inspect the durable bytes rather than an in-process view of
/// them.
async fn start_test_server() -> (ravel_server::Running, Arc<MemoryStore>) {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
    };
    let running = ravel_server::start(config, store.clone())
        .await
        .expect("server starts");
    (running, store)
}

/// Lists every RLOG data object the log keyspace holds for `TENANT`, GETs
/// each one, and returns every record an unfiltered scan yields.
async fn scan_durable_log_records(store: &dyn ObjectStoreBackend) -> Vec<ravel_logseg::LogRecord> {
    let prefix = format!("t/{}/l/l0/", TenantId::new(TENANT).hash().to_hex());
    let objects = list_all(store, &prefix)
        .await
        .expect("list log data objects");
    assert!(
        !objects.is_empty(),
        "no log data object under {prefix}: the write never became durable"
    );

    let mut records = Vec::new();
    for object in objects {
        let bytes = store
            .get(&object.key, GetRange::Full)
            .await
            .expect("get log data object")
            .data;
        let reader = RlogReader::new(&bytes, &RlogConfig::default()).expect("open RLOG object");
        let (mut scanned, _stats) = reader
            .scan(&Predicate::And(Vec::new()))
            .expect("unfiltered scan");
        records.append(&mut scanned);
    }
    records
}

fn assert_is_the_exported_record(record: &ravel_logseg::LogRecord, body: &str, ts_ns: i64) {
    assert_eq!(record.body, body);
    assert_eq!(record.ts_ns, ts_ns);
    assert_eq!(record.severity_num, 9);
    assert_eq!(record.severity_text, "INFO");
    let route = record
        .attrs
        .iter()
        .find(|(key, _)| key == "http.route")
        .expect("http.route attribute survived the round trip");
    assert_eq!(route.1, AttrValue::Str("/checkout".to_string()));
}

#[tokio::test]
async fn http_export_round_trips_to_a_durable_rlog_object() {
    let (running, store) = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let ts_ns = now_ns();
    let request = export_request("checkout completed", ts_ns);
    let response = client
        .post(format!("{base}/v1/logs"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("export request succeeds");

    assert_eq!(response.status(), 200, "log export should succeed");
    assert!(
        response
            .headers()
            .get("x-ravel-commit-token")
            .is_some_and(|value| !value.is_empty()),
        "strict-mode export must return a commit token"
    );
    let body = response.bytes().await.expect("response body");
    let decoded = ExportLogsServiceResponse::decode(body.as_ref())
        .expect("response is an ExportLogsServiceResponse");
    assert!(
        decoded.partial_success.is_none(),
        "nothing should have been rejected: {:?}",
        decoded.partial_success
    );

    // The commit token acked before the response, so the object is durable
    // already; read it back out of the store, not out of the process.
    let records = scan_durable_log_records(store.as_ref()).await;
    assert_eq!(records.len(), 1, "expected exactly one durable log record");
    assert_is_the_exported_record(&records[0], "checkout completed", ts_ns);

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn grpc_export_round_trips_to_a_durable_rlog_object() {
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;

    let (running, store) = start_test_server().await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let mut client = LogsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");

    let ts_ns = now_ns();
    let mut request = tonic::Request::new(export_request("grpc log line", ts_ns));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );

    let response = client.export(request).await.expect("gRPC export succeeds");
    assert!(
        response
            .metadata()
            .get("x-ravel-commit-token")
            .is_some_and(|value| !value.is_empty()),
        "strict-mode export must return commit-token metadata"
    );
    assert!(response.get_ref().partial_success.is_none());

    let records = scan_durable_log_records(store.as_ref()).await;
    assert_eq!(records.len(), 1, "expected exactly one durable log record");
    assert_is_the_exported_record(&records[0], "grpc log line", ts_ns);

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn grpc_export_without_credentials_is_unauthenticated() {
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;

    let (running, _store) = start_test_server().await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let mut client = LogsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");

    let status = client
        .export(export_request("unauthorized", now_ns()))
        .await
        .expect_err("missing credentials must be rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn missing_credentials_yield_401() {
    let (running, _store) = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{base}/v1/logs"))
        .header("content-type", "application/x-protobuf")
        .body(export_request("unauthorized", now_ns()).encode_to_vec())
        .send()
        .await
        .expect("export request completes");
    assert_eq!(response.status(), 401);

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn malformed_protobuf_yields_400() {
    let (running, _store) = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{base}/v1/logs"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        // Field 1 tagged as a varint and then truncated: not a decodable
        // ExportLogsServiceRequest under any reading.
        .body(vec![0x08u8])
        .send()
        .await
        .expect("export request completes");
    assert_eq!(response.status(), 400);

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn buffered_mode_header_is_honored() {
    let (running, store) = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let ts_ns = now_ns();
    let response = client
        .post(format!("{base}/v1/logs"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .header("x-ravel-ingest-mode", "buffered")
        .body(export_request("buffered log line", ts_ns).encode_to_vec())
        .send()
        .await
        .expect("export request succeeds");

    assert_eq!(response.status(), 200, "buffered export should succeed");
    assert!(
        response.headers().get("x-ravel-commit-token").is_none(),
        "buffered mode acks at enqueue, before any commit exists"
    );

    // Shutdown drains the log router the same way it drains the metrics one,
    // so a buffered record is still durable afterwards.
    running.shutdown().await.expect("graceful shutdown");

    let records = scan_durable_log_records(store.as_ref()).await;
    assert_eq!(records.len(), 1, "shutdown must flush the buffered record");
    assert_is_the_exported_record(&records[0], "buffered log line", ts_ns);
}
