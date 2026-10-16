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

#[path = "support/strict_flush.rs"]
mod strict_flush;

use ravel_server::config::StrictFlushPolicy;
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use strict_flush::{PolicyServer, Signal, process_sample, release_until};

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
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
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
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: std::time::Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    let running = ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
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

/// The deepest kvlist nest the log segment format holds around a scalar
/// (`ravel_logseg::attr_value_fits_storage`: a kvlist costs two of its 31
/// levels).
const FITTING_KVLISTS: usize = 15;

/// `layers` single-entry kvlists nested around an int, as OTLP and as the
/// `AttrValue` it stores as.
fn nested_kvlist(layers: usize) -> (AnyValue, AttrValue) {
    use opentelemetry_proto::tonic::common::v1::KeyValueList;
    let mut otlp = AnyValue {
        value: Some(AnyValueVariant::IntValue(7)),
    };
    let mut stored = AttrValue::I64(7);
    for _ in 0..layers {
        otlp = AnyValue {
            value: Some(AnyValueVariant::KvlistValue(KeyValueList {
                values: vec![KeyValue {
                    key: "m".to_string(),
                    value: Some(otlp),
                    ..Default::default()
                }],
            })),
        };
        stored = AttrValue::Map(vec![("m".to_string(), stored)]);
    }
    (otlp, stored)
}

fn resource_logs(resource_attrs: Vec<KeyValue>, body: &str, ts_ns: i64) -> ResourceLogs {
    let mut request = export_request(body, ts_ns);
    let mut resource_logs = request.resource_logs.remove(0);
    resource_logs.resource = Some(Resource {
        attributes: resource_attrs,
        ..Default::default()
    });
    resource_logs
}

/// A resource attribute at the deepest nest the segment format holds is
/// admitted, flushed and read back intact; one a kvlist level deeper is
/// rejected at admission: its request reports one rejected record with the
/// depth message. Once both exports return, the durable records are the
/// fitting one and the sibling resource's from the rejected request. The two
/// requests are sent concurrently, but nothing here makes them share a flush
/// batch, so this does not show that a refused value would have failed a
/// batch.
#[tokio::test]
async fn resource_attribute_depth_is_checked_at_admission_not_at_flush() {
    let (running, store) = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    let ts_ns = now_ns();

    let (fit_otlp, fit_stored) = nested_kvlist(FITTING_KVLISTS);
    let (past_otlp, _) = nested_kvlist(FITTING_KVLISTS + 1);
    let deep_kv = |value: AnyValue| KeyValue {
        key: "deep".to_string(),
        value: Some(value),
        ..Default::default()
    };
    let fit_request = ExportLogsServiceRequest {
        resource_logs: vec![resource_logs(
            vec![string_kv("service.name", "fit"), deep_kv(fit_otlp)],
            "fit",
            ts_ns,
        )],
    };
    let past_request = ExportLogsServiceRequest {
        resource_logs: vec![
            resource_logs(
                vec![string_kv("service.name", "past"), deep_kv(past_otlp)],
                "past",
                ts_ns,
            ),
            resource_logs(vec![string_kv("service.name", "sibling")], "sibling", ts_ns),
        ],
    };

    let send = |request: ExportLogsServiceRequest| {
        client
            .post(format!("{base}/v1/logs"))
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/x-protobuf")
            .body(request.encode_to_vec())
            .send()
    };
    let (fit_response, past_response) = tokio::join!(send(fit_request), send(past_request));
    let decode = |bytes: &[u8]| {
        ExportLogsServiceResponse::decode(bytes).expect("response is an ExportLogsServiceResponse")
    };

    let fit_response = fit_response.expect("fit export completes");
    assert_eq!(fit_response.status(), 200);
    let fit_body = decode(&fit_response.bytes().await.expect("response body"));
    assert!(
        fit_body.partial_success.is_none(),
        "{:?}",
        fit_body.partial_success
    );

    let past_response = past_response.expect("past export completes");
    assert_eq!(past_response.status(), 200);
    let past_body = decode(&past_response.bytes().await.expect("response body"));
    let partial = past_body
        .partial_success
        .expect("the over-nested resource is reported");
    assert_eq!(partial.rejected_log_records, 1);
    assert!(
        partial
            .error_message
            .contains("attribute deep nests more than 31 levels deep"),
        "{}",
        partial.error_message
    );

    let records = scan_durable_log_records(store.as_ref()).await;
    let mut bodies: Vec<&str> = records.iter().map(|r| r.body.as_str()).collect();
    bodies.sort_unstable();
    assert_eq!(bodies, vec!["fit", "sibling"]);
    let fit = records
        .iter()
        .find(|r| r.body == "fit")
        .expect("fit record");
    let decoded = ravel_logseg::record::decode_stream_attrs(&fit.stream_attrs)
        .expect("the stored resource attributes decode");
    let deep = decoded
        .resource
        .iter()
        .find(|(key, _)| key == "deep")
        .expect("the fitting resource attribute survived");
    assert_eq!(deep.1, fit_stored);

    running.shutdown().await.expect("graceful shutdown");
}

/// Exports one log record to `server` with `mode` as the
/// `x-ravel-ingest-mode` header (no header for `None`), on its own task so
/// the test can read the gate while the handler waits for its strict ack.
fn spawn_export(
    server: &PolicyServer,
    mode: Option<&'static str>,
    body: &'static str,
) -> tokio::task::JoinHandle<reqwest::Response> {
    let client = server.client.clone();
    let url = format!("{}/v1/logs", server.base);
    tokio::spawn(async move {
        let mut request = client
            .post(url)
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/x-protobuf");
        if let Some(mode) = mode {
            request = request.header("x-ravel-ingest-mode", mode);
        }
        request
            .body(export_request(body, now_ns()).encode_to_vec())
            .send()
            .await
            .expect("export request completes")
    })
}

fn assert_strict_ack(response: &reqwest::Response) {
    assert_eq!(response.status(), 200, "strict export should succeed");
    assert!(
        response
            .headers()
            .get("x-ravel-commit-token")
            .is_some_and(|value| !value.is_empty()),
        "a strict export acks with a commit token"
    );
}

/// `strict-arrival` under the default `aged` policy opens the flush from the
/// write itself: the data PUT is issued while the age threshold is still
/// ahead, and the ack follows it once the gate lets it through.
#[tokio::test]
async fn strict_arrival_header_acks_without_the_age_tick() {
    let server = strict_flush::start(StrictFlushPolicy::Aged, Signal::Logs).await;

    let export = spawn_export(&server, Some("strict-arrival"), "arrival log line");
    server.wait_for_data_put().await;
    assert!(
        !export.is_finished(),
        "the strict ack waits for the held data PUT"
    );
    assert_eq!(server.flushes_by_age().await, 0, "no age flush opened");
    assert_eq!(server.flushes_by_arrival().await, 1);

    let response = release_until(&server.gate, export)
        .await
        .expect("export task");
    assert_strict_ack(&response);
    assert_eq!(server.flushes_by_arrival().await, 1);

    server.shutdown().await;
}

/// `strict`, an absent header and an unknown value mean "strict under the
/// process default": each flushes on arrival under `--strict-flush-policy
/// arrival`, and none opens a flush before the age threshold under the
/// default `aged` policy.
#[tokio::test]
async fn strict_header_follows_the_process_default() {
    let server = strict_flush::start(StrictFlushPolicy::Arrival, Signal::Logs).await;
    for (flushes, mode) in [(1, Some("strict")), (2, None), (3, Some("not-a-mode"))] {
        let export = spawn_export(&server, mode, "process default log line");
        server.wait_for_data_put().await;
        assert_eq!(
            server.flushes_by_arrival().await,
            flushes,
            "header {mode:?} under the arrival policy flushes on arrival"
        );
        let response = release_until(&server.gate, export)
            .await
            .expect("export task");
        assert_strict_ack(&response);
    }
    assert_eq!(server.flushes_by_age().await, 0);
    server.shutdown().await;

    let server = strict_flush::start(StrictFlushPolicy::Aged, Signal::Logs).await;
    let strict = spawn_export(&server, Some("strict"), "aged log line");
    let absent = spawn_export(&server, None, "aged log line");
    let unknown = spawn_export(&server, Some("not-a-mode"), "aged log line");
    server.assert_no_flush_yet(3).await;

    let (strict, absent, unknown) = release_until(&server.gate, async {
        (
            strict.await.expect("export task"),
            absent.await.expect("export task"),
            unknown.await.expect("export task"),
        )
    })
    .await;
    assert_strict_ack(&strict);
    assert_strict_ack(&absent);
    assert_strict_ack(&unknown);
    assert_eq!(
        server.flushes_by_age().await,
        1,
        "all three rode one age flush"
    );
    assert_eq!(server.flushes_by_arrival().await, 0);
    server.shutdown().await;
}

/// `strict-aged` opts one request out of `--strict-flush-policy arrival`: it
/// opens no flush before the age threshold, then acks from the age flush.
#[tokio::test]
async fn strict_aged_header_opts_out_of_an_arrival_process_default() {
    let server = strict_flush::start(StrictFlushPolicy::Arrival, Signal::Logs).await;

    let export = spawn_export(&server, Some("strict-aged"), "opted out log line");
    server.assert_no_flush_yet(1).await;

    let response = release_until(&server.gate, export)
        .await
        .expect("export task");
    assert_strict_ack(&response);
    assert_eq!(server.flushes_by_age().await, 1);
    assert_eq!(server.flushes_by_arrival().await, 0);

    server.shutdown().await;
}

/// The gRPC log handler reads the same mode from request metadata.
#[tokio::test]
async fn grpc_strict_arrival_metadata_selects_the_policy() {
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;

    let server = strict_flush::start(StrictFlushPolicy::Aged, Signal::Logs).await;
    let grpc_addr = server.running.grpc_addr.expect("gateway mode binds gRPC");
    let mut client = LogsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");

    let mut request = tonic::Request::new(export_request("grpc arrival log line", now_ns()));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );
    request.metadata_mut().insert(
        "x-ravel-ingest-mode",
        "strict-arrival".parse().expect("ascii metadata"),
    );
    let export = tokio::spawn(async move { client.export(request).await });

    server.wait_for_data_put().await;
    assert_eq!(server.flushes_by_age().await, 0, "no age flush opened");
    assert_eq!(server.flushes_by_arrival().await, 1);

    let response = release_until(&server.gate, export)
        .await
        .expect("export task")
        .expect("gRPC export succeeds");
    assert!(
        response
            .metadata()
            .get("x-ravel-commit-token")
            .is_some_and(|value| !value.is_empty()),
        "a strict export acks with commit-token metadata"
    );

    server.shutdown().await;
}

/// With the in-flight ceiling at one, a strict export returns its permit
/// once its record is enqueued, so a second strict export is admitted while
/// the first still waits on its held data PUT. Both reach the shard buffer,
/// neither is shed, and both ack with a token once the gate releases.
#[tokio::test]
async fn in_flight_permit_is_released_before_the_strict_ack() {
    let server = strict_flush::start_with(StrictFlushPolicy::Arrival, Signal::Logs, |config| {
        config.ingest_concurrency_limit = IngestConcurrencyLimit::Bounded(1);
    })
    .await;

    let mut first = spawn_export(&server, Some("strict"), "first strict log line");
    server.wait_for_buffered_while_pending(1, &mut first).await;
    let mut second = spawn_export(&server, Some("strict"), "second strict log line");
    server.wait_for_buffered_while_pending(2, &mut second).await;
    assert!(
        !first.is_finished() && !second.is_finished(),
        "both strict acks wait for the held data PUTs"
    );
    assert_eq!(
        process_sample(
            &server.scrape().await,
            "ravel_ingest_concurrency_shed_total"
        ),
        0,
        "the second export was admitted, not shed"
    );

    let (first, second) = release_until(&server.gate, async {
        (
            first.await.expect("export task"),
            second.await.expect("export task"),
        )
    })
    .await;
    assert_strict_ack(&first);
    assert_strict_ack(&second);

    server.shutdown().await;
}

/// With `--max-pending-strict-acks 1` and one strict export waiting on its
/// held data PUT, a second strict export is refused 429 with `Retry-After`
/// and the shed body, the refusal and the pending count both show on the
/// real `/metrics`, and a buffered export in the same state is still
/// accepted.
#[tokio::test]
async fn pending_strict_ack_limit_refuses_with_429() {
    let server = strict_flush::start_with(StrictFlushPolicy::Arrival, Signal::Logs, |config| {
        config.max_pending_strict_acks = 1;
    })
    .await;

    let pending = spawn_export(&server, Some("strict"), "pending strict log line");
    server.wait_for_data_put().await;

    let refused = spawn_export(&server, Some("strict"), "refused strict log line")
        .await
        .expect("export task");
    assert_eq!(refused.status(), 429, "the second strict export is shed");
    assert_eq!(
        refused
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("1")
    );
    assert!(
        refused.headers().get("x-ravel-commit-token").is_none(),
        "a refused write carries no commit token"
    );
    assert_eq!(
        refused.text().await.expect("refusal body"),
        "process pending strict-acknowledgement limit reached"
    );

    let exposition = server.scrape().await;
    assert_eq!(
        process_sample(&exposition, "ravel_ingest_pending_ack_shed_total"),
        1,
        "the refusal is counted once"
    );
    assert_eq!(
        process_sample(&exposition, "ravel_ingest_pending_strict_acks"),
        1,
        "the first export still holds the only pending-ack slot"
    );

    let buffered = spawn_export(&server, Some("buffered"), "buffered log line")
        .await
        .expect("export task");
    assert_eq!(
        buffered.status(),
        200,
        "a buffered write never takes a pending-ack slot"
    );
    assert!(
        !pending.is_finished(),
        "the first strict ack is still pending"
    );

    let pending = release_until(&server.gate, pending)
        .await
        .expect("export task");
    assert_strict_ack(&pending);
    assert_eq!(
        process_sample(&server.scrape().await, "ravel_ingest_pending_strict_acks"),
        0,
        "the ack returned the slot"
    );

    server.shutdown().await;
}

/// The gRPC twin of `pending_strict_ack_limit_refuses_with_429`: the same
/// refusal over gRPC is `RESOURCE_EXHAUSTED` with the shared shed message.
#[tokio::test]
async fn grpc_pending_strict_ack_limit_refuses_resource_exhausted() {
    use opentelemetry_proto::tonic::collector::logs::v1::logs_service_client::LogsServiceClient;

    let server = strict_flush::start_with(StrictFlushPolicy::Arrival, Signal::Logs, |config| {
        config.max_pending_strict_acks = 1;
    })
    .await;
    let grpc_addr = server.running.grpc_addr.expect("gateway mode binds gRPC");

    let pending = spawn_export(&server, Some("strict"), "pending strict log line");
    server.wait_for_data_put().await;

    let mut client = LogsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");
    let mut request = tonic::Request::new(export_request("refused grpc log line", now_ns()));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );
    let status = client
        .export(request)
        .await
        .expect_err("a strict export over the pending-ack ceiling is refused");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted, "{status:?}");
    assert_eq!(
        status.message(),
        "process pending strict-acknowledgement limit reached"
    );
    assert_eq!(
        process_sample(
            &server.scrape().await,
            "ravel_ingest_pending_ack_shed_total"
        ),
        1
    );

    let pending = release_until(&server.gate, pending)
        .await
        .expect("export task");
    assert_strict_ack(&pending);

    server.shutdown().await;
}
