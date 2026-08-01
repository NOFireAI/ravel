//! End-to-end coverage for OTLP trace ingest: real HTTP and gRPC exports
//! against an in-process server backed by `MemoryStore`, followed by reading
//! the flushed RSPAN object straight out of the object store with
//! `ravel_rspan::RspanReader`.
//!
//! There is no query surface for spans yet (ADR-0041 phase 5 adds the
//! `ravel-sql` `spans` table; `services/ravel-server/src/fold.rs` still folds
//! metrics and logs only), so durability is proven the way the logs epic proved
//! it at the same stage: list the objects the write produced under
//! `t/<tenant_hash>/s/l0/`, GET the bytes, open them with `ravel_rspan::open`,
//! and scan them unfiltered. Nothing here reads back through an in-process
//! handle to the router.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use prost::Message;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_rspan::{RspanConfig, RspanReader, SpanQuery, SpanRecord, StatusCode};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const TRACE_ID: [u8; 16] = [0xa1; 16];
const SPAN_ID: [u8; 8] = [0xb2; 8];
const PARENT_SPAN_ID: [u8; 8] = [0xc3; 8];

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

fn export_request(name: &str, start_ts_ns: i64) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "checkout")],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "tracer".to_string(),
                    version: "1.0".to_string(),
                    ..Default::default()
                }),
                spans: vec![Span {
                    trace_id: TRACE_ID.to_vec(),
                    span_id: SPAN_ID.to_vec(),
                    parent_span_id: PARENT_SPAN_ID.to_vec(),
                    name: name.to_string(),
                    kind: 2,
                    start_time_unix_nano: start_ts_ns as u64,
                    end_time_unix_nano: (start_ts_ns + 1_000) as u64,
                    attributes: vec![string_kv("http.route", "/checkout")],
                    status: Some(Status {
                        code: 2,
                        message: "upstream timeout".to_string(),
                    }),
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
    };
    let running = ravel_server::start(config, store.clone())
        .await
        .expect("server starts");
    (running, store)
}

/// Lists every RSPAN data object the span keyspace holds for `TENANT`, GETs
/// each one, checks its footer opens, and returns every span an unfiltered
/// scan yields.
async fn scan_durable_spans(store: &dyn ObjectStoreBackend) -> Vec<SpanRecord> {
    let prefix = format!("t/{}/s/l0/", TenantId::new(TENANT).hash().to_hex());
    let objects = list_all(store, &prefix)
        .await
        .expect("list span data objects");
    assert!(
        !objects.is_empty(),
        "no span data object under {prefix}: the write never became durable"
    );

    let mut spans = Vec::new();
    for object in objects {
        let bytes = store
            .get(&object.key, GetRange::Full)
            .await
            .expect("get span data object")
            .data;
        // Open the footer on its own first: a scan that returns rows proves the
        // blocks decode, but this proves the object is a well-formed RSPAN
        // object down to its trailer and section table.
        let footer = ravel_rspan::open(&bytes).expect("open RSPAN footer");
        assert!(
            !footer.sections.is_empty(),
            "an RSPAN object always carries at least the BLOCKS and SKIP_IDX sections"
        );

        let reader = RspanReader::new(&bytes, &RspanConfig::default()).expect("open RSPAN object");
        let (mut scanned, stats) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("unfiltered scan");
        assert!(stats.blocks_scanned > 0, "an unfiltered scan reads a block");
        spans.append(&mut scanned);
    }
    spans
}

fn assert_is_the_exported_span(span: &SpanRecord, name: &str, start_ts_ns: i64) {
    assert_eq!(span.trace_id, TRACE_ID);
    assert_eq!(span.span_id, SPAN_ID);
    assert_eq!(span.parent_span_id, Some(PARENT_SPAN_ID));
    assert_eq!(span.name, name);
    assert_eq!(span.start_ts_ns, start_ts_ns);
    assert_eq!(span.end_ts_ns, start_ts_ns + 1_000);
    assert_eq!(span.status_code, StatusCode::Error);
    assert_eq!(span.status_message.as_deref(), Some("upstream timeout"));

    let lookup = |key: &str| {
        span.attrs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    };
    // Span, resource, and scope attributes all merged into the one attrs map.
    assert_eq!(lookup("http.route"), Some("/checkout"));
    assert_eq!(lookup("service.name"), Some("checkout"));
    assert_eq!(lookup("otel.scope.name"), Some("tracer"));
    assert_eq!(lookup("otel.scope.version"), Some("1.0"));
    assert_eq!(lookup("_kind"), Some("server"));
}

#[tokio::test]
async fn http_export_round_trips_to_a_durable_rspan_object() {
    let (running, store) = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let start_ts_ns = now_ns();
    let request = export_request("GET /checkout", start_ts_ns);
    let response = client
        .post(format!("{base}/v1/traces"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("export request succeeds");

    assert_eq!(response.status(), 200, "trace export should succeed");
    assert!(
        response
            .headers()
            .get("x-ravel-commit-token")
            .is_some_and(|value| !value.is_empty()),
        "strict-mode export must return a commit token"
    );
    let body = response.bytes().await.expect("response body");
    let decoded = ExportTraceServiceResponse::decode(body.as_ref())
        .expect("response is an ExportTraceServiceResponse");
    assert!(
        decoded.partial_success.is_none(),
        "nothing should have been rejected: {:?}",
        decoded.partial_success
    );

    // The commit token acked before the response, so the object is durable
    // already; read it back out of the store, not out of the process.
    let spans = scan_durable_spans(store.as_ref()).await;
    assert_eq!(spans.len(), 1, "expected exactly one durable span");
    assert_is_the_exported_span(&spans[0], "GET /checkout", start_ts_ns);

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn grpc_export_round_trips_to_a_durable_rspan_object() {
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;

    let (running, store) = start_test_server().await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let mut client = TraceServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");

    let start_ts_ns = now_ns();
    let mut request = tonic::Request::new(export_request("grpc span", start_ts_ns));
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

    let spans = scan_durable_spans(store.as_ref()).await;
    assert_eq!(spans.len(), 1, "expected exactly one durable span");
    assert_is_the_exported_span(&spans[0], "grpc span", start_ts_ns);

    running.shutdown().await.expect("graceful shutdown");
}

/// The property ADR-0041's routing decision exists for, proven on the durable
/// keyspace: every span of one trace lands under one shard directory, so
/// trace-by-id assembly is a bounded scan rather than a fan-out. Exported over
/// four shards with two traces, each carrying three spans.
#[tokio::test]
async fn spans_of_one_trace_land_under_one_shard_directory() {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 4,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
    };
    let running = ravel_server::start(config, store.clone())
        .await
        .expect("server starts");
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let start_ts_ns = now_ns();
    let spans: Vec<Span> = [1u8, 2]
        .into_iter()
        .flat_map(|trace| {
            (0..3u8).map(move |i| Span {
                trace_id: [trace; 16].to_vec(),
                span_id: [i + 1; 8].to_vec(),
                name: format!("trace{trace}-span{i}"),
                start_time_unix_nano: (start_ts_ns + i64::from(i)) as u64,
                end_time_unix_nano: (start_ts_ns + i64::from(i) + 10) as u64,
                ..Default::default()
            })
        })
        .collect();
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            scope_spans: vec![ScopeSpans {
                spans,
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    let response = client
        .post(format!("{base}/v1/traces"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("export request succeeds");
    assert_eq!(response.status(), 200);

    // Each object key is t/<hash>/s/l0/<shard>/...; group the durable objects
    // by the trace ids they contain and assert each trace occupies one shard.
    let prefix = format!("t/{}/s/l0/", TenantId::new(TENANT).hash().to_hex());
    let objects = list_all(store.as_ref(), &prefix).await.expect("list");
    assert!(!objects.is_empty(), "the export became durable");

    let mut shard_of_trace: HashMap<[u8; 16], String> = HashMap::new();
    let mut spans_seen = 0usize;
    for object in &objects {
        let shard = object.key[prefix.len()..]
            .split('/')
            .next()
            .expect("shard directory in the key")
            .to_string();
        let bytes = store
            .get(&object.key, GetRange::Full)
            .await
            .expect("get")
            .data;
        let reader = RspanReader::new(&bytes, &RspanConfig::default()).expect("open");
        let (scanned, _stats) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("scan");
        spans_seen += scanned.len();
        for span in scanned {
            let previous = shard_of_trace.insert(span.trace_id, shard.clone());
            if let Some(previous) = previous {
                assert_eq!(
                    previous, shard,
                    "one trace's spans must not be split across shards"
                );
            }
        }
    }
    assert_eq!(spans_seen, 6, "every exported span is durable");
    assert_eq!(shard_of_trace.len(), 2, "both traces landed");
    let mut distinct_shards: Vec<&String> = shard_of_trace.values().collect();
    distinct_shards.sort_unstable();
    distinct_shards.dedup();
    assert_eq!(
        distinct_shards.len(),
        2,
        "the two traces must land on different shards, not just be two distinct \
         trace_ids that happen to coexist on the same one"
    );

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn grpc_export_without_credentials_is_unauthenticated() {
    use opentelemetry_proto::tonic::collector::trace::v1::trace_service_client::TraceServiceClient;

    let (running, _store) = start_test_server().await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");
    let mut client = TraceServiceClient::connect(format!("http://{grpc_addr}"))
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
        .post(format!("{base}/v1/traces"))
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
        .post(format!("{base}/v1/traces"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        // Field 1 tagged as a varint and then truncated: not a decodable
        // ExportTraceServiceRequest under any reading.
        .body(vec![0x08u8])
        .send()
        .await
        .expect("export request completes");
    assert_eq!(response.status(), 400);

    running.shutdown().await.expect("graceful shutdown");
}

/// An admission-limit rejection travels the whole transport path: the span is
/// dropped, the rest of the request still commits, and the OTLP partial-success
/// body reports the rejected-span count.
#[tokio::test]
async fn a_rejected_span_is_reported_through_partial_success() {
    let (running, store) = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let start_ts_ns = now_ns();
    let mut request = export_request("GET /checkout", start_ts_ns);
    // A span whose end precedes its start cannot be pruned soundly by the
    // interval skip index, so normalization rejects it (ADR-0041 decision 3).
    request.resource_spans[0].scope_spans[0].spans.push(Span {
        trace_id: TRACE_ID.to_vec(),
        span_id: [0xd4; 8].to_vec(),
        name: "inverted".to_string(),
        start_time_unix_nano: (start_ts_ns + 5_000) as u64,
        end_time_unix_nano: start_ts_ns as u64,
        ..Default::default()
    });

    let response = client
        .post(format!("{base}/v1/traces"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("export request succeeds");
    assert_eq!(response.status(), 200);

    let body = response.bytes().await.expect("response body");
    let decoded = ExportTraceServiceResponse::decode(body.as_ref()).expect("OTLP response");
    let partial = decoded
        .partial_success
        .expect("the inverted span is rejected");
    assert_eq!(partial.rejected_spans, 1);
    assert!(
        partial.error_message.contains("before it starts"),
        "got: {}",
        partial.error_message
    );

    let spans = scan_durable_spans(store.as_ref()).await;
    assert_eq!(spans.len(), 1, "only the admitted span is durable");
    assert_is_the_exported_span(&spans[0], "GET /checkout", start_ts_ns);

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn buffered_mode_header_is_honored() {
    let (running, store) = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let start_ts_ns = now_ns();
    let response = client
        .post(format!("{base}/v1/traces"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .header("x-ravel-ingest-mode", "buffered")
        .body(export_request("buffered span", start_ts_ns).encode_to_vec())
        .send()
        .await
        .expect("export request succeeds");

    assert_eq!(response.status(), 200, "buffered export should succeed");
    assert!(
        response.headers().get("x-ravel-commit-token").is_none(),
        "buffered mode acks at enqueue, before any commit exists"
    );

    // Shutdown drains the span router the same way it drains the metrics and
    // log ones, so a buffered span is still durable afterwards.
    running.shutdown().await.expect("graceful shutdown");

    let spans = scan_durable_spans(store.as_ref()).await;
    assert_eq!(spans.len(), 1, "shutdown must flush the buffered span");
    assert_is_the_exported_span(&spans[0], "buffered span", start_ts_ns);
}
