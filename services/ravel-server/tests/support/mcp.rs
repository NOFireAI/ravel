//! A real `ravel_server::start` server with `--mcp` on, an rmcp client for
//! it, and a fixture tenant every one of the nine read tools has data in: one
//! metrics segment (metric `m`, series `k="quad"` and `k="flat"`) and one logs
//! object, both in ingest hour 0, read through a window of the first hour.
//! Included with `#[path]` by the MCP protocol, escape-route and audit tests;
//! `mcp_e2e.rs` keeps its own copies of the same helpers.

#![allow(dead_code, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::{McpConfig, QueryBudgets};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;
use rmcp::model::{CallToolRequestParams, ProtocolVersion};
use rmcp::service::{ClientLifecycleMode, RunningService};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{ClientServiceExt, RoleClient};
use serde_json::{Value, json};

pub const TOKEN: &str = "acme-token";
pub const TENANT: &str = "acme";
pub const ALLOWED_ORIGIN: &str = "https://console.example";
pub const MAX_BODY_BYTES: u64 = 4096;
pub const NS_PER_SEC: i64 = 1_000_000_000;

/// A trace id no fixture object holds: `ravel_get_trace` answers it with an
/// empty envelope, which is all these tests read of that tool.
pub const ABSENT_TRACE_HEX: &str = "7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a";

pub type Client = RunningService<RoleClient, ()>;

pub fn expected_tenant() -> ravel_types::TenantHash {
    TenantId::new(TENANT).hash()
}

pub fn mcp_on() -> McpConfig {
    McpConfig {
        enabled: true,
        allowed_origins: vec![ALLOWED_ORIGIN.to_string()],
        max_body_bytes: MAX_BODY_BYTES,
    }
}

/// The deployment's config with `--mcp` on and the audit pipeline in
/// `required` mode, so every audited tool call's record is durable before the
/// call answers.
pub fn server_config() -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Some(ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        }),
        audit_text: ravel_maintain::AuditTextPolicy::default(),
        query_budgets: QueryBudgets {
            mcp: mcp_on(),
            ..Default::default()
        },
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::Query,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants: vec![TenantId::new(TENANT).hash()],
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        // Pushed past any test's lifetime, so no periodic store call lands
        // inside a store-count assertion.
        store_probe_interval: Duration::from_secs(3600),
        admission_reconcile_interval: Duration::from_secs(3600),
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(7 * 86_400),
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
        cpu_shed: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    }
}

pub async fn start_with(
    config: ServerConfig,
    store: Arc<dyn ObjectStoreBackend>,
    store_metrics: Arc<ravel_object_store::StoreMetrics>,
) -> ravel_server::Running {
    ravel_server::start(config, store.clone(), store, store_metrics, None)
        .await
        .expect("server starts")
}

/// A `MemoryStore` behind the instrumentation decorator, and its counters.
pub fn instrumented_memory() -> (
    Arc<dyn ObjectStoreBackend>,
    Arc<ravel_object_store::StoreMetrics>,
) {
    let metrics = Arc::new(ravel_object_store::StoreMetrics::default());
    let store = Arc::new(ravel_object_store::InstrumentedStore::with_metrics(
        MemoryStore::new(),
        Arc::clone(&metrics),
    ));
    (store, metrics)
}

pub fn mcp_url(running: &ravel_server::Running) -> String {
    format!("http://{}/mcp", running.http_addr)
}

pub fn transport(
    running: &ravel_server::Running,
) -> StreamableHttpClientTransport<reqwest::Client> {
    StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(mcp_url(running)).auth_header(TOKEN),
    )
}

/// The lifecycle a client on `revision` uses: the legacy `initialize`
/// handshake on `2025-11-25`, and `server/discover` with self-contained
/// per-request metadata on `2026-07-28`. A revision this does not know fails
/// the test, so a newly supported one cannot go unexercised.
pub fn lifecycle_for(revision: &ProtocolVersion) -> ClientLifecycleMode {
    if *revision == ProtocolVersion::V_2025_11_25 {
        ClientLifecycleMode::Initialize
    } else if *revision == ProtocolVersion::V_2026_07_28 {
        ClientLifecycleMode::Discover {
            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
        }
    } else {
        panic!("no client lifecycle for revision {revision:?}; add one here")
    }
}

/// A client on the current revision, connected to `running`.
pub async fn connect(running: &ravel_server::Running) -> Client {
    connect_on(running, &ProtocolVersion::V_2026_07_28).await
}

pub async fn connect_on(running: &ravel_server::Running, revision: &ProtocolVersion) -> Client {
    ().serve_with_lifecycle(transport(running), lifecycle_for(revision))
        .await
        .expect("the client reaches the server")
}

/// One `tools/call`, returning the result's error flag and its envelope.
pub async fn call(client: &Client, name: &str, arguments: Value) -> (Option<bool>, Value) {
    let Value::Object(arguments) = arguments else {
        panic!("tool arguments are an object");
    };
    let result = client
        .call_tool(CallToolRequestParams::new(name.to_string()).with_arguments(arguments))
        .await
        .unwrap_or_else(|err| panic!("{name} answers: {err}"));
    let envelope = result
        .structured_content
        .clone()
        .expect("the tool result carries the envelope as structured content");
    (result.is_error, envelope)
}

/// One raw POST to the MCP route carrying `TOKEN`, so a test can state
/// exactly which protocol headers the request carries.
pub async fn post_raw(
    running: &ravel_server::Running,
    headers: &[(&str, &str)],
    body: String,
) -> reqwest::Response {
    let mut request = reqwest::Client::new()
        .post(mcp_url(running))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(body);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    request
        .send()
        .await
        .expect("the request reaches the server")
}

/// A 400 raised by the adapter's per-revision header check because it does
/// not serve `revision`, told apart from one the protocol layer raises by its
/// body (the adapter answers the `errorType`-tagged JSON of the HTTP query
/// routes, rmcp a JSON-RPC error) and from the header check's other
/// refusals by its message, which names the revision.
pub async fn assert_refused_for_revision(response: reqwest::Response, revision: &str) {
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    let body: Value = response.json().await.expect("a JSON error body");
    assert_eq!(body["status"], json!("error"), "{body}");
    assert_eq!(body["errorType"], json!("bad_data"), "{body}");
    let message = body["error"].as_str().expect("an error message");
    assert!(
        message.contains("unsupported MCP protocol version") && message.contains(revision),
        "{body}"
    );
}

/// A JSON-RPC `initialize` body stating `revision`.
pub fn initialize_body(revision: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": revision,
            "capabilities": {},
            "clientInfo": { "name": "ravel-test-client", "version": "0.0.0" }
        }
    })
}

/// A JSON-RPC `tools/call` body for `ravel_capabilities` whose per-request
/// `_meta` states `revision`.
pub fn tools_call_body(revision: &str) -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "ravel_capabilities",
            "arguments": {},
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": revision,
                "io.modelcontextprotocol/clientCapabilities": {}
            }
        }
    })
}

/// The first hour, which holds every fixture row.
pub fn window() -> Value {
    json!({"start_ns": "0", "end_ns": (3_600 * NS_PER_SEC).to_string()})
}

/// One fixture call for each of the nine read tools, in catalog order. Each
/// answers `ok` over [`seed_fixture`]'s tenant.
pub fn nine_tool_calls() -> Vec<(&'static str, Value)> {
    vec![
        ("ravel_capabilities", json!({})),
        ("ravel_describe_data", json!({"signal": "metrics"})),
        (
            "ravel_find_labels",
            json!({"label_name": "__name__", "time_range": window()}),
        ),
        (
            "ravel_explain_query",
            json!({"query": "SELECT value FROM samples", "time_range": window()}),
        ),
        (
            "ravel_query_sql",
            json!({"query": "SELECT value FROM samples", "time_range": window()}),
        ),
        (
            "ravel_query_promql",
            json!({"query": "m", "evaluation_time": (100 * NS_PER_SEC).to_string()}),
        ),
        (
            "ravel_analyze_timeseries",
            json!({
                "query": r#"m{k="quad"}"#,
                "time_range": {
                    "start_ns": (10 * NS_PER_SEC).to_string(),
                    "end_ns": (50 * NS_PER_SEC).to_string(),
                },
                "step": "10s",
                "op": "summary",
            }),
        ),
        (
            "ravel_search_logs",
            json!({"time_range": window(), "has_word": "timeout"}),
        ),
        ("ravel_get_trace", json!({"trace_id": ABSENT_TRACE_HEX})),
    ]
}

/// Write the fixture tenant's metrics segment and logs object.
pub async fn seed_fixture(store: &dyn ObjectStoreBackend) {
    let quad: Vec<(i64, f64)> = [1.0, 2.0, 3.0, 4.0]
        .iter()
        .enumerate()
        .map(|(index, &value)| ((index as i64 + 1) * 10 * NS_PER_SEC, value))
        .collect();
    let flat = [(10 * NS_PER_SEC, 7.0), (20 * NS_PER_SEC, 7.0)];
    publish_series(store, 0, 1, &[(Some("quad"), &quad), (Some("flat"), &flat)]).await;
    publish_logs(
        store,
        &[
            (10 * NS_PER_SEC, "user login ok"),
            (20 * NS_PER_SEC, "disk timeout on node"),
        ],
    )
    .await;
}

/// One series for [`publish_series`]: its label `k`, if any, and its
/// `(ts_ns, value)` samples.
pub type SeriesEntry<'a> = (Option<&'a str>, &'a [(i64, f64)]);

/// Publish one metrics segment of metric `m` holding one series per entry,
/// plus its commit record, flushed in `ingest_hour` by `writer_seq`.
pub async fn publish_series(
    store: &dyn ObjectStoreBackend,
    ingest_hour: u32,
    writer_seq: u64,
    entries: &[SeriesEntry<'_>],
) {
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_object_store::PutOptions;
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal};

    let tenant = TenantId::new(TENANT);
    let tenant_hash = tenant.hash();
    let series = entries
        .iter()
        .map(|&(k, samples)| {
            let mut labels = vec![Label {
                name: "__name__".to_string(),
                value: "m".to_string(),
            }];
            if let Some(k) = k {
                labels.push(Label {
                    name: "k".to_string(),
                    value: k.to_string(),
                });
            }
            let labels = LabelSet::new(labels).expect("valid labels");
            SeriesInput {
                series_id: SeriesId::compute(&tenant, "m", &labels).expect("series id"),
                labels,
                samples: samples
                    .iter()
                    .map(|&(ts_ns, value)| Sample { ts_ns, value })
                    .collect(),
            }
        })
        .collect();
    let writer_id = uuid::Uuid::from_u128(2_000);
    let hour_start_ns = i64::from(ingest_hour) * 3_600_000_000_000;
    let written = SegmentWriter::write(
        series,
        SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq,
        },
        IngestBounds {
            min_ingest_ts_ns: hour_start_ns,
            max_ingest_ts_ns: hour_start_ns,
        },
    )
    .expect("write segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: hour_start_ns + 10,
        ingest_hour_bucket: ingest_hour,
    })
    .expect("valid commit record");
    store
        .put(
            &keys::reconstruct_data_key(&rec).expect("data key"),
            written.bytes,
            PutOptions::default(),
        )
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Publish `lines` as one RLOG object of service `api` plus its
/// `Signal::Logs` commit record in ingest hour 0, the way log ingest does.
pub async fn publish_logs(store: &dyn ObjectStoreBackend, lines: &[(i64, &str)]) {
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
    use ravel_object_store::PutOptions;
    use ravel_types::Signal;
    use ravel_types::logstream::log_stream_id;

    let tenant_hash = TenantId::new(TENANT).hash();
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let writer_id = uuid::Uuid::from_u128(3_000);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    for &(ts_ns, body) in lines {
        writer
            .push(LogRecord {
                stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns,
                observed_ts_ns: ts_ns,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: body.into(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let min_ts = lines.iter().map(|&(ts, _)| ts).min().expect("a line");
    let max_ts = lines.iter().map(|&(ts, _)| ts).max().expect("a line");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: lines.len() as u64,
        series_count: 1,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: min_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid log commit record");
    store
        .put(
            &keys::reconstruct_data_key(&rec).expect("data key"),
            bytes::Bytes::from(bytes),
            PutOptions::default(),
        )
        .await
        .expect("put log data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish log commit");
}

/// Every `kind=query` record on the fixture tenant's query-audit shard.
pub async fn query_audit_records(store: &dyn ObjectStoreBackend) -> Vec<ravel_logseg::LogRecord> {
    use ravel_commit::{keys, record};
    use ravel_logseg::{Predicate, RlogConfig, RlogReader};
    use ravel_object_store::{GetRange, list_all};

    let prefix = keys::commit_shard_prefix(
        &expected_tenant(),
        ravel_types::Signal::Audit,
        ravel_maintain::QUERY_AUDIT_SHARD,
    )
    .expect("audit commit prefix");
    let config = RlogConfig::default();
    let mut out = Vec::new();
    for meta in list_all(store, &prefix).await.expect("list audit commits") {
        let commit = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get commit");
        let commit = record::decode(&commit.data).expect("decode commit");
        let data_key = keys::reconstruct_data_key(&commit).expect("data key");
        let object = store
            .get(&data_key, GetRange::Full)
            .await
            .expect("get data");
        let reader = RlogReader::new(object.data.as_ref(), &config).expect("rlog reader");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        out.extend(
            rows.into_iter()
                .filter(|row| audit_attr(row, "kind") == Some("query")),
        );
    }
    out
}

pub fn audit_attr<'a>(row: &'a ravel_logseg::LogRecord, key: &str) -> Option<&'a str> {
    row.attrs
        .iter()
        .find(|(name, _)| name == key)
        .and_then(|(_, value)| match value {
            ravel_logseg::AttrValue::Str(text) => Some(text.as_str()),
            _ => None,
        })
}
