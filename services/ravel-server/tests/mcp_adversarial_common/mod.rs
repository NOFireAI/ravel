//! Fixtures shared by the `mcp_adversarial_*` suites (issue #1383): a real
//! [`ravel_server::start`] server over a `MemoryStore` (or a `FaultStore`)
//! that resolves two bearer tokens to two tenants, the MCP client those
//! suites drive it with, and writers that publish metrics, logs and spans for
//! either tenant the way ingest does.
//!
//! Each suite includes this file with `mod mcp_adversarial_common;`, so a
//! helper one suite does not call is dead code in that binary only.

#![allow(dead_code, clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::{McpConfig, QueryBudgets};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};
use rmcp::model::{CallToolRequestParams, ProtocolVersion};
use rmcp::transport::StreamableHttpClientTransport;
use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
use rmcp::{ClientServiceExt, service::ClientLifecycleMode};

pub const NS_PER_SEC: i64 = 1_000_000_000;
pub const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// The two tenants every suite here starts with, and the bearer tokens the
/// server's resolver maps to them.
pub const TENANT_A: &str = "acme";
pub const TOKEN_A: &str = "acme-token";
pub const TENANT_B: &str = "globex";
pub const TOKEN_B: &str = "globex-token";

pub const ALLOWED_ORIGIN: &str = "https://console.example";
/// Large enough for every request body these suites send, including the
/// hostile strings, and still a live cap.
pub const MAX_BODY_BYTES: u64 = 256 * 1024;

pub type Client = rmcp::service::RunningService<rmcp::RoleClient, ()>;

pub fn hash(tenant: &str) -> TenantHash {
    TenantId::new(tenant).hash()
}

pub fn mcp_on() -> McpConfig {
    McpConfig {
        enabled: true,
        allowed_origins: vec![ALLOWED_ORIGIN.to_string()],
        max_body_bytes: MAX_BODY_BYTES,
    }
}

/// The deployment's config with `--mcp` on and both tokens resolving, in the
/// shape `mcp_e2e.rs` starts its server with.
pub fn server_config() -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN_A.to_string(), TenantId::new(TENANT_A));
    tokens.insert(TOKEN_B.to_string(), TenantId::new(TENANT_B));
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        remote_write_mode: ravel_server::config::RemoteWriteMode::Buffered,
        audit_pipeline: None,
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
        fold_tenants: vec![hash(TENANT_A), hash(TENANT_B)],
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
        ingest_hold: Default::default(),
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

/// [`server_config`] with the query-audit pipeline on in `required` mode, so
/// every SQL-backed tool call writes its record before it answers.
pub fn audited_config() -> ServerConfig {
    let mut config = server_config();
    config.audit_pipeline = Some(ravel_maintain::AuditPipelineConfig {
        audit_mode: ravel_maintain::AuditMode::Required,
        ..Default::default()
    });
    config
}

pub fn memory_store() -> Arc<dyn ObjectStoreBackend> {
    Arc::new(MemoryStore::new())
}

pub async fn start(
    config: ServerConfig,
    store: Arc<dyn ObjectStoreBackend>,
) -> ravel_server::Running {
    let metrics = Arc::new(ravel_object_store::StoreMetrics::default());
    ravel_server::start(config, store.clone(), store, metrics, None)
        .await
        .expect("server starts")
}

/// [`start`] over `store` behind the instrumentation decorator, returning the
/// counters every store call the server makes lands in.
pub async fn start_counted(
    config: ServerConfig,
    store: Arc<dyn ObjectStoreBackend>,
) -> (ravel_server::Running, Arc<ravel_object_store::StoreMetrics>) {
    let metrics = Arc::new(ravel_object_store::StoreMetrics::default());
    let counted: Arc<dyn ObjectStoreBackend> = Arc::new(
        ravel_object_store::InstrumentedStore::with_metrics(store, Arc::clone(&metrics)),
    );
    let running = ravel_server::start(config, counted.clone(), counted, Arc::clone(&metrics), None)
        .await
        .expect("server starts");
    (running, metrics)
}

/// Completed store calls across every operation kind.
pub fn store_calls(metrics: &ravel_object_store::StoreMetrics) -> u64 {
    let snapshot = metrics.snapshot();
    snapshot.put.calls
        + snapshot.get.calls
        + snapshot.head.calls
        + snapshot.list.calls
        + snapshot.list_delimited.calls
        + snapshot.delete.calls
}

pub fn mcp_url(running: &ravel_server::Running) -> String {
    format!("http://{}/mcp", running.http_addr)
}

/// A real MCP client on the current revision, authenticating with `token`.
pub async fn connect(running: &ravel_server::Running, token: &str) -> Client {
    let transport = StreamableHttpClientTransport::from_config(
        StreamableHttpClientTransportConfig::with_uri(mcp_url(running)).auth_header(token),
    );
    ().serve_with_lifecycle(
        transport,
        ClientLifecycleMode::Discover {
            preferred_versions: vec![ProtocolVersion::V_2026_07_28],
        },
    )
    .await
    .expect("the client reaches the server")
}

/// One `tools/call`, returning the result's error flag and its envelope.
pub async fn call(
    client: &Client,
    name: &str,
    arguments: serde_json::Value,
) -> (Option<bool>, serde_json::Value) {
    let (is_error, envelope, _) = call_with_text(client, name, arguments).await;
    (is_error, envelope)
}

/// [`call`], plus the text content block the result carries beside the
/// structured envelope.
pub async fn call_with_text(
    client: &Client,
    name: &str,
    arguments: serde_json::Value,
) -> (Option<bool>, serde_json::Value, Vec<String>) {
    let serde_json::Value::Object(arguments) = arguments else {
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
    let text = result
        .content
        .iter()
        .filter_map(|block| block.as_text().map(|text| text.text.clone()))
        .collect();
    (result.is_error, envelope, text)
}

/// One `tools/call` as raw HTTP on the current revision, returning the
/// response body exactly as it came off the wire and the JSON-RPC message it
/// carries. A `text/event-stream` body carries the message on its `data:`
/// lines; anything else must be the JSON message itself.
pub async fn raw_tools_call(
    running: &ravel_server::Running,
    token: &str,
    name: &str,
    arguments: serde_json::Value,
) -> (String, serde_json::Value) {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 7,
        "method": "tools/call",
        "params": {
            "name": name,
            "arguments": arguments,
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                "io.modelcontextprotocol/clientCapabilities": {}
            }
        }
    });
    let response = reqwest::Client::new()
        .post(mcp_url(running))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", "2026-07-28")
        .header("mcp-method", "tools/call")
        .header("mcp-name", name)
        .body(body.to_string())
        .send()
        .await
        .expect("the request reaches the server");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let event_stream = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"));
    let raw = response.text().await.expect("a response body");
    let message = if event_stream {
        let data: Vec<&str> = raw
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim)
            .filter(|data| !data.is_empty())
            .collect();
        assert_eq!(data.len(), 1, "one JSON-RPC message: {raw}");
        data[0].to_string()
    } else {
        raw.clone()
    };
    let parsed = serde_json::from_str(&message)
        .unwrap_or_else(|err| panic!("the response is not valid JSON ({err}): {message}"));
    (raw, parsed)
}

pub fn cursor_of(page: &serde_json::Value) -> Option<String> {
    page["presentation"]["cursor"].as_str().map(str::to_string)
}

/// The cells of column `name` on every row of `envelope`, in row order.
pub fn column(envelope: &serde_json::Value, name: &str) -> Vec<serde_json::Value> {
    let at = envelope["data"]["columns"]
        .as_array()
        .expect("columns")
        .iter()
        .position(|column| column["name"] == name)
        .unwrap_or_else(|| panic!("no {name} column in {envelope}"));
    envelope["data"]["rows"]
        .as_array()
        .expect("rows")
        .iter()
        .map(|row| row[at].clone())
        .collect()
}

pub fn rows(envelope: &serde_json::Value) -> &Vec<serde_json::Value> {
    envelope["data"]["rows"].as_array().expect("rows")
}

pub fn wall_now_ns() -> i64 {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is past the epoch");
    i64::try_from(since_epoch.as_nanos()).expect("the epoch fits an i64")
}

/// One series [`publish_series`] writes: its labels beyond `__name__` and its
/// `(ts_ns, value)` samples.
pub struct TestSeries<'a> {
    pub labels: Vec<(&'a str, &'a str)>,
    pub samples: Vec<(i64, f64)>,
}

/// Publish one RSEG segment of metric `metric` for `tenant`, flushed in
/// ingest hour `ingest_hour` by writer sequence `writer_seq`, plus its commit
/// record, the way ingest does.
pub async fn publish_series(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
    ingest_hour: u32,
    writer_seq: u64,
    metric: &str,
    entries: &[TestSeries<'_>],
) {
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_object_store::PutOptions;
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal};

    let tenant_id = TenantId::new(tenant);
    let tenant_hash = tenant_id.hash();
    let series = entries
        .iter()
        .map(|entry| {
            let mut labels = vec![Label {
                name: "__name__".to_string(),
                value: metric.to_string(),
            }];
            labels.extend(entry.labels.iter().map(|&(name, value)| Label {
                name: name.to_string(),
                value: value.to_string(),
            }));
            let labels = LabelSet::new(labels).expect("valid labels");
            SeriesInput {
                series_id: SeriesId::compute(&tenant_id, metric, &labels).expect("series id"),
                labels,
                samples: entry
                    .samples
                    .iter()
                    .map(|&(ts_ns, value)| Sample { ts_ns, value })
                    .collect(),
            }
        })
        .collect();
    let writer_id = uuid::Uuid::from_u128(2_000);
    let hour_start_ns = i64::from(ingest_hour) * NS_PER_HOUR;
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

/// One log record [`publish_logs`] writes, observed at its `ts_ns`.
pub struct TestLog<'a> {
    pub ts_ns: i64,
    pub severity_num: u8,
    pub body: &'a str,
    pub trace_id: Option<[u8; 16]>,
    pub attrs: Vec<(&'a str, &'a str)>,
}

/// Publish `records` as one RLOG object of service `api` for `tenant`, plus
/// its `Signal::Logs` commit record in ingest hour `ingest_hour` by writer
/// sequence `writer_seq`, the way log ingest does.
pub async fn publish_logs(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
    ingest_hour: u32,
    writer_seq: u64,
    records: &[TestLog<'_>],
) {
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
    use ravel_object_store::PutOptions;
    use ravel_types::Signal;
    use ravel_types::logstream::log_stream_id;

    let tenant_hash = TenantId::new(tenant).hash();
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let writer_id = uuid::Uuid::from_u128(3_000);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq,
        },
    );
    for log in records {
        writer
            .push(LogRecord {
                stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: log.ts_ns,
                observed_ts_ns: log.ts_ns,
                severity_num: log.severity_num,
                severity_text: "INFO".into(),
                body: log.body.into(),
                trace_id: log.trace_id,
                span_id: None,
                flags: 0,
                attrs: log
                    .attrs
                    .iter()
                    .map(|&(key, value)| (key.to_string(), AttrValue::Str(value.into())))
                    .collect(),
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let min_ts = records.iter().map(|log| log.ts_ns).min().expect("a line");
    let max_ts = records.iter().map(|log| log.ts_ns).max().expect("a line");
    let hour_start_ns = i64::from(ingest_hour) * NS_PER_HOUR;
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: min_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: hour_start_ns + 10,
        ingest_hour_bucket: ingest_hour,
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

/// One span [`publish_trace`] writes.
pub struct TestSpan<'a> {
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: &'a str,
    pub start_ts_ns: i64,
    pub attrs: Vec<(&'a str, &'a str)>,
}

/// One RSPAN object of `tenant` holding `spans` of trace `trace_id`, plus its
/// commit record in ingest hour 0.
pub async fn publish_trace(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
    trace_id: [u8; 16],
    spans: &[TestSpan<'_>],
) {
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_object_store::PutOptions;
    use ravel_types::Signal;

    let tenant_hash = TenantId::new(tenant).hash();
    let writer_id = uuid::Uuid::from_u128(4_000);
    let mut writer = ravel_rspan::RspanWriter::new(
        ravel_rspan::RspanConfig::default(),
        ravel_rspan::ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    for span in spans {
        let mut attrs = vec![("service.name".to_string(), "svc".to_string())];
        attrs.extend(
            span.attrs
                .iter()
                .map(|&(key, value)| (key.to_string(), value.to_string())),
        );
        writer.push(ravel_rspan::SpanRecord {
            trace_id,
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
            name: span.name.to_string(),
            start_ts_ns: span.start_ts_ns,
            end_ts_ns: span.start_ts_ns + 1_000,
            status_code: ravel_rspan::StatusCode::Ok,
            status_message: None,
            attrs,
        });
    }
    let bytes = writer.finish().expect("finish rspan object");
    let min_ts = spans
        .iter()
        .map(|span| span.start_ts_ns)
        .min()
        .expect("a span");
    let max_ts = spans
        .iter()
        .map(|span| span.start_ts_ns + 1_000)
        .max()
        .expect("a span");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: spans.len() as u64,
        series_count: spans.len() as u64,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: min_ts,
        max_ingest_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid span commit record");
    store
        .put(
            &keys::reconstruct_data_key(&rec).expect("data key"),
            bytes::Bytes::from(bytes),
            PutOptions::default(),
        )
        .await
        .expect("put span data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish span commit");
}

/// Write `tenant`'s metric metadata record: one gauge family per name, its
/// help text `help`.
pub async fn write_families(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
    names: &[String],
    help: &str,
) {
    let entries: Vec<_> = names
        .iter()
        .map(|name| ravel_catalog::MetricMetadataEntry {
            family_name: name.clone(),
            kind: ravel_catalog::MetricKind::Gauge,
            help: help.to_string(),
            unit: String::new(),
            updated_unix_ns: 1,
        })
        .collect();
    ravel_catalog::write_metrics_meta(store, &hash(tenant), &entries, None)
        .await
        .expect("metadata record written");
}

/// Declare `keys` as `tenant`'s typed string attribute columns.
pub async fn declare_columns(store: &dyn ObjectStoreBackend, tenant: &str, keys: &[&str]) {
    let declared = keys
        .iter()
        .map(|key| ravel_catalog::DeclaredTypedColumn {
            key: (*key).to_string(),
            ty: ravel_catalog::DeclaredColumnType::Str,
        })
        .collect();
    let config = ravel_catalog::TenantConfig {
        typed_attr_columns: Some(declared),
        ..ravel_catalog::TenantConfig::new(ravel_catalog::TenantLifecycleState::Active)
    };
    ravel_catalog::set_tenant_config(store, &hash(tenant), &config, wall_now_ns())
        .await
        .expect("tenant config written");
}

/// Every `kind=query` record on `tenant`'s query-audit shard.
pub async fn query_audit_records(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
) -> Vec<ravel_logseg::LogRecord> {
    use ravel_commit::{keys, record};
    use ravel_logseg::{Predicate, RlogConfig, RlogReader};
    use ravel_object_store::{GetRange, list_all};

    let prefix = keys::commit_shard_prefix(
        &hash(tenant),
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

/// The budgets the adapter hands a tool body when the caller lowers nothing.
pub fn default_budgets() -> ravel_mcp::budget::McpEffectiveBudgets {
    ravel_mcp::budget::McpRequestBudgets::default().clamp(
        &ravel_query::EngineConfig::default(),
        &ravel_mcp::budget::McpBudgetConfig::default(),
    )
}

/// The backend the adapter hands every tool body, over `running`'s service.
pub fn backend(running: &ravel_server::Running) -> ravel_server::mcp::ServiceBackend {
    let service = running
        .query_service
        .clone()
        .expect("a query-serving mode builds one");
    ravel_server::mcp::ServiceBackend::new(service, default_budgets())
}

/// A tool call context for `tenant` at `now_ns`, `max_rows` rows a page, and a
/// 30 h protection horizon, so a cursor outlives every gap between pages a
/// test opens.
pub fn tool_context<'k>(
    tenant: &str,
    now_ns: i64,
    max_rows: u32,
    key: &'k ravel_mcp::cursor::CursorKey,
) -> ravel_mcp::tools::ToolContext<'k> {
    let mut budgets = default_budgets();
    budgets.max_rows = max_rows;
    ravel_mcp::tools::ToolContext {
        tenant_hash: hash(tenant),
        now_ns,
        deadline_ns: now_ns + 60 * NS_PER_SEC,
        protection_horizon_ns: now_ns + 30 * NS_PER_HOUR,
        budgets,
        cursor_key: key,
    }
}

pub fn cursor_key() -> ravel_mcp::cursor::CursorKey {
    ravel_mcp::cursor::CursorKey::from_process_secret([0x42; ravel_mcp::cursor::CURSOR_KEY_LEN])
}
