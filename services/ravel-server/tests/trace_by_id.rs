//! `GET /api/traces/{trace_id}` and `ravel_get_trace` on a server built by
//! [`ravel_server::start`].
//!
//! The fixture tenant holds one trace whose spans sit in two ingest hours: an
//! old hour, five hours back and folded into a catalog part, and a recent one,
//! two minutes back and still in the unsealed tail. A one-hour window ending
//! now cannot reach the old hour (its listing reaches back one hour plus the
//! two-hour ingest lag, and the folded part's event range ends five hours
//! back), and a resolve over folded parts alone cannot reach the recent one,
//! so only a lookup that resolves the whole tenant returns every span.
//!
//! The server runs on the real clock, so the fixture timestamps are read from
//! it too.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::{AttrValue, LogRecord, Predicate, RlogConfig, RlogReader};
use ravel_maintain::QUERY_AUDIT_SHARD;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_server::config::{McpConfig, QueryBudgets};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};
use serde_json::Value;
use uuid::Uuid;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const OTHER_TOKEN: &str = "globex-token";
const OTHER_TENANT: &str = "globex";
const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;

/// The trace under test, and its hex spelling on the route.
const TRACE: [u8; 16] = [0x7a; 16];
const TRACE_HEX: &str = "7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a";
/// A second trace sharing both objects, which no lookup of [`TRACE`] returns.
const OTHER_TRACE: [u8; 16] = [0x11; 16];

/// [`TRACE`]'s spans in the folded old hour.
const OLD_SPANS: [[u8; 8]; 2] = [[1; 8], [2; 8]];
/// [`TRACE`]'s span in the unsealed recent hour.
const RECENT_SPAN: [u8; 8] = [3; 8];
/// [`OTHER_TRACE`]'s spans, one per hour.
const OTHER_OLD_SPAN: [u8; 8] = [9; 8];
const OTHER_RECENT_SPAN: [u8; 8] = [8; 8];

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn now_ns() -> i64 {
    let since = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after the epoch");
    i64::try_from(since.as_nanos()).expect("now fits in i64")
}

fn server_config(mcp_enabled: bool) -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    tokens.insert(OTHER_TOKEN.to_string(), TenantId::new(OTHER_TENANT));
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Some(ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        }),
        audit_text: Default::default(),
        query_budgets: QueryBudgets {
            mcp: McpConfig {
                enabled: mcp_enabled,
                allowed_origins: vec!["https://console.example".to_string()],
                max_body_bytes: 4096,
            },
            ..Default::default()
        },
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("addr"),
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
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        scrub_period: Duration::from_secs(7 * 86_400),
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
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

/// One RSPAN object holding `spans`, committed in the ingest hour of `ts_ns`.
async fn publish_spans(
    store: &dyn ObjectStoreBackend,
    writer_id: Uuid,
    ts_ns: i64,
    spans: &[([u8; 16], [u8; 8], i64)],
) {
    let tenant = TenantId::new(TENANT);
    let mut writer = ravel_rspan::RspanWriter::new(
        ravel_rspan::RspanConfig::default(),
        ravel_rspan::ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    for (trace_id, span_id, start_ts_ns) in spans {
        writer.push(ravel_rspan::SpanRecord {
            trace_id: *trace_id,
            span_id: *span_id,
            parent_span_id: None,
            name: format!("op-{}", hex(span_id)),
            start_ts_ns: *start_ts_ns,
            end_ts_ns: start_ts_ns + 1_000,
            status_code: ravel_rspan::StatusCode::Ok,
            status_message: None,
            attrs: vec![("service.name".to_string(), "svc".to_string())],
        });
    }
    let bytes = writer.finish().expect("finish rspan object");
    let min_ts = spans.iter().map(|s| s.2).min().expect("spans");
    let max_ts = spans.iter().map(|s| s.2 + 1_000).max().expect("spans");
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
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
        min_ingest_ts_ns: ts_ns,
        max_ingest_ts_ns: ts_ns,
        segment_format_version: u32::from(ravel_ingest::SPAN_SEGMENT_FORMAT_VERSION),
        created_unix_ns: ts_ns,
        ingest_hour_bucket: u32::try_from(ts_ns / NS_PER_HOUR).expect("hour fits"),
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish commit");
}

/// The two-hour fixture. Returns the store and the `now_ns` it was built
/// against.
async fn seeded() -> (Arc<dyn ObjectStoreBackend>, i64) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let now = now_ns();
    let old = now - 5 * NS_PER_HOUR;
    let recent = now - 120 * NS_PER_SEC;
    publish_spans(
        store.as_ref(),
        Uuid::from_u128(1),
        old,
        &[
            (TRACE, OLD_SPANS[0], old),
            (TRACE, OLD_SPANS[1], old + NS_PER_SEC),
            (OTHER_TRACE, OTHER_OLD_SPAN, old),
        ],
    )
    .await;
    publish_spans(
        store.as_ref(),
        Uuid::from_u128(2),
        recent,
        &[
            (TRACE, RECENT_SPAN, recent),
            (OTHER_TRACE, OTHER_RECENT_SPAN, recent),
        ],
    )
    .await;

    // Fold at the real clock: the old hour seals, the recent one cannot.
    let folder = ravel_catalog::Catalog::new(
        store.clone(),
        ravel_catalog::CatalogConfig {
            shard_count: 1,
            ..ravel_catalog::CatalogConfig::default()
        },
    )
    .expect("folding catalog");
    let report = folder
        .fold(
            &TenantId::new(TENANT).hash(),
            Signal::Spans,
            Uuid::new_v4(),
            now_ns(),
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(
        report.entry_count, 1,
        "the old hour's object folds and the recent hour's stays in the tail: {report:?}"
    );
    (store, now)
}

async fn start(store: Arc<dyn ObjectStoreBackend>, mcp_enabled: bool) -> ravel_server::Running {
    ravel_server::start(
        server_config(mcp_enabled),
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

fn secs(ns: i64) -> String {
    format!("{}", ns as f64 / NS_PER_SEC as f64)
}

/// `GET /api/traces/{id}` as `token`, with `query` as its query string.
async fn get_trace(base: &str, token: &str, id: &str, query: &[(&str, String)]) -> (u16, Value) {
    let response = reqwest::Client::new()
        .get(format!("{base}/api/traces/{id}"))
        .header("authorization", format!("Bearer {token}"))
        .query(query)
        .send()
        .await
        .expect("trace request sent");
    let status = response.status().as_u16();
    let body = response.json().await.expect("trace response is JSON");
    (status, body)
}

/// `POST /api/v1/sql` as [`TOKEN`]; `window` is `(start, end)` in seconds.
async fn post_sql(base: &str, sql: &str, window: Option<(String, String)>) -> (u16, Value) {
    let mut body = serde_json::json!({ "query": sql });
    if let Some((start, end)) = window {
        body["start"] = Value::from(start.parse::<f64>().expect("seconds"));
        body["end"] = Value::from(end.parse::<f64>().expect("seconds"));
    }
    let response = reqwest::Client::new()
        .post(format!("{base}/api/v1/sql"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
        .expect("sql request sent");
    let status = response.status().as_u16();
    let body = response.json().await.expect("sql response is JSON");
    (status, body)
}

/// The `span_id` column of a `{columns, rows}` result, in row order.
fn span_ids(data: &Value) -> Vec<Value> {
    let columns = data["columns"].as_array().expect("columns");
    let at = columns
        .iter()
        .position(|c| c["name"] == "span_id")
        .unwrap_or_else(|| panic!("no span_id column: {data}"));
    data["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("no row array: {data}"))
        .iter()
        .map(|row| row[at].clone())
        .collect()
}

/// The JSON spelling of each span id, as the `spans` table renders it.
fn want(ids: &[[u8; 8]]) -> Vec<Value> {
    ids.iter().map(|id| Value::from(hex(id))).collect()
}

const EVERY_SPAN: [[u8; 8]; 3] = [OLD_SPANS[0], OLD_SPANS[1], RECENT_SPAN];

#[tokio::test]
async fn get_trace_by_id_without_a_window_returns_every_span() {
    let (store, now) = seeded().await;
    let running = start(store, false).await;
    let base = format!("http://{}", running.http_addr);

    let (status, body) = get_trace(&base, TOKEN, TRACE_HEX, &[]).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "success", "{body}");
    assert_eq!(span_ids(&body["data"]), want(&EVERY_SPAN), "{body}");
    assert!(
        body["stats"].is_object(),
        "the SQL envelope's stats: {body}"
    );

    // Either case spells the same trace.
    let (status, upper) = get_trace(&base, TOKEN, &TRACE_HEX.to_uppercase(), &[]).await;
    assert_eq!(status, 200, "{upper}");
    assert_eq!(span_ids(&upper["data"]), want(&EVERY_SPAN), "{upper}");

    // An explicit window over the recent hour is kept exactly.
    let window = [
        ("start", secs(now - 30 * 60 * NS_PER_SEC)),
        ("end", secs(now + 60 * NS_PER_SEC)),
    ];
    let (status, recent) = get_trace(&base, TOKEN, TRACE_HEX, &window).await;
    assert_eq!(status, 200, "{recent}");
    assert_eq!(span_ids(&recent["data"]), want(&[RECENT_SPAN]), "{recent}");

    // Explicit bounds choose the objects listed, as on the SQL endpoint, and
    // filter no rows: a start between the two old spans still lists their
    // object, so both come back.
    let (status, listed) = get_trace(&base, TOKEN, TRACE_HEX, &between_old_spans(now)).await;
    assert_eq!(status, 200, "{listed}");
    assert_eq!(span_ids(&listed["data"]), want(&EVERY_SPAN), "{listed}");

    running.shutdown().await.expect("graceful shutdown");
}

/// A window from between the two old spans to just past now.
fn between_old_spans(now: i64) -> [(&'static str, String); 2] {
    let old = now - 5 * NS_PER_HOUR;
    [
        ("start", secs(old + NS_PER_SEC / 2)),
        ("end", secs(now + 60 * NS_PER_SEC)),
    ]
}

/// The SQL endpoint over the same statement: an explicit window is kept, and
/// a spans statement with no `trace_id` equality keeps the one-hour default.
#[tokio::test]
async fn sql_twin_keeps_a_given_window_and_the_default_without_a_trace_id() {
    let (store, now) = seeded().await;
    let running = start(store, false).await;
    let base = format!("http://{}", running.http_addr);
    let lookup = ravel_server::traces::trace_statement(&TRACE);

    let (status, wide) = post_sql(&base, &lookup, Some(("0".into(), secs(now)))).await;
    assert_eq!(status, 200, "{wide}");
    let (_, by_id) = get_trace(&base, TOKEN, TRACE_HEX, &[]).await;
    assert_eq!(wide["data"]["rows"], by_id["data"]["rows"], "{wide}");
    assert_eq!(span_ids(&wide["data"]), want(&EVERY_SPAN), "{wide}");

    let recent = Some((
        secs(now - 30 * 60 * NS_PER_SEC),
        secs(now + 60 * NS_PER_SEC),
    ));
    let (status, narrow) = post_sql(&base, &lookup, recent).await;
    assert_eq!(status, 200, "{narrow}");
    assert_eq!(span_ids(&narrow["data"]), want(&[RECENT_SPAN]), "{narrow}");

    let (status, defaulted) =
        post_sql(&base, "SELECT span_id FROM spans ORDER BY span_id", None).await;
    assert_eq!(status, 200, "{defaulted}");
    assert_eq!(
        span_ids(&defaulted["data"]),
        want(&[RECENT_SPAN, OTHER_RECENT_SPAN]),
        "a statement with no trace id keeps the one-hour default: {defaulted}"
    );

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn a_malformed_id_is_400_and_an_unknown_one_is_200_with_no_rows() {
    let (store, _) = seeded().await;
    let running = start(store, false).await;
    let base = format!("http://{}", running.http_addr);

    for bad in [
        "7a7a",
        "7a7a7a7a7a7a7a7a7a7a7a7a7a7a7a7g",
        &format!("{TRACE_HEX}00"),
    ] {
        let (status, body) = get_trace(&base, TOKEN, bad, &[]).await;
        assert_eq!(status, 400, "{bad}: {body}");
        assert_eq!(body["status"], "error", "{bad}: {body}");
        assert_eq!(body["errorType"], "bad_data", "{bad}: {body}");
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|e| e.contains("32 hexadecimal")),
            "{bad}: {body}"
        );
    }

    let (status, body) = get_trace(&base, TOKEN, &hex(&[0x55; 16]), &[]).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "success", "{body}");
    assert_eq!(body["data"]["rows"], serde_json::json!([]), "{body}");

    running.shutdown().await.expect("graceful shutdown");
}

/// The route is mounted on the real surface: without a token it is the SQL
/// endpoint's 401, not a 404.
#[tokio::test]
async fn the_route_is_served_and_authenticated() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start(store, false).await;
    let response = reqwest::Client::new()
        .get(format!(
            "http://{}/api/traces/{TRACE_HEX}",
            running.http_addr
        ))
        .send()
        .await
        .expect("trace request sent");
    assert_eq!(response.status().as_u16(), 401);
    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn a_lookup_writes_one_query_audit_record() {
    let (store, _) = seeded().await;
    let running = start(store.clone(), false).await;
    let base = format!("http://{}", running.http_addr);

    let (status, body) = get_trace(&base, TOKEN, TRACE_HEX, &[]).await;
    assert_eq!(status, 200, "{body}");

    let tenant = TenantId::new(TENANT);
    let records = query_audit_records(store.as_ref(), &tenant).await;
    assert_eq!(records.len(), 1, "exactly one query-audit record");
    let row = &records[0];
    assert_eq!(attr(row, "query.language"), Some("sql"));
    assert_eq!(attr(row, "query.status"), Some("ok"));
    assert_eq!(
        attr(row, "query.tenant"),
        Some(tenant.hash().to_hex().as_str())
    );
    assert_eq!(
        attr(row, "query.text"),
        Some(ravel_server::traces::trace_statement(&TRACE).as_str())
    );
    assert_eq!(attr(row, "query.window_start_ns"), Some("0"));

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn another_tenant_reads_no_rows_of_the_trace() {
    let (store, _) = seeded().await;
    let running = start(store, false).await;
    let base = format!("http://{}", running.http_addr);

    let (status, body) = get_trace(&base, OTHER_TOKEN, TRACE_HEX, &[]).await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["data"]["rows"], serde_json::json!([]), "{body}");

    running.shutdown().await.expect("graceful shutdown");
}

/// Every `kind=query` record on `tenant`'s query-audit shard.
async fn query_audit_records(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> Vec<LogRecord> {
    let prefix = keys::commit_shard_prefix(&tenant.hash(), Signal::Audit, QUERY_AUDIT_SHARD)
        .expect("audit commit prefix");
    let cfg = RlogConfig::default();
    let mut out = Vec::new();
    for meta in list_all(store, &prefix).await.expect("list audit commits") {
        let got = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get commit");
        let commit = record::decode(&got.data).expect("decode commit");
        let data_key = keys::reconstruct_data_key(&commit).expect("data key");
        let object = store
            .get(&data_key, GetRange::Full)
            .await
            .expect("get data");
        let reader = RlogReader::new(object.data.as_ref(), &cfg).expect("rlog reader");
        let (rows, _stats) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        out.extend(
            rows.into_iter()
                .filter(|r| attr(r, "kind") == Some("query")),
        );
    }
    out
}

fn attr<'a>(row: &'a LogRecord, key: &str) -> Option<&'a str> {
    row.attrs
        .iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| match v {
            AttrValue::Str(s) => Some(s.as_str()),
            _ => None,
        })
}

#[cfg(feature = "mcp")]
mod with_mcp {
    use super::*;

    use rmcp::model::{CallToolRequestParams, ProtocolVersion};
    use rmcp::transport::StreamableHttpClientTransport;
    use rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig;
    use rmcp::{ClientServiceExt, service::ClientLifecycleMode};

    async fn call_get_trace(running: &ravel_server::Running, arguments: Value) -> Value {
        let transport = StreamableHttpClientTransport::from_config(
            StreamableHttpClientTransportConfig::with_uri(format!(
                "http://{}/mcp",
                running.http_addr
            ))
            .auth_header(TOKEN),
        );
        let client = ()
            .serve_with_lifecycle(
                transport,
                ClientLifecycleMode::Discover {
                    preferred_versions: vec![ProtocolVersion::V_2026_07_28],
                },
            )
            .await
            .expect("client connects");
        let tools = client.list_all_tools().await.expect("tools/list answers");
        let listed = tools
            .iter()
            .find(|tool| tool.name == "ravel_get_trace")
            .expect("ravel_get_trace is listed");
        let schema = serde_json::to_value(&listed.input_schema).expect("schema");
        assert_eq!(
            schema["required"],
            serde_json::json!(["trace_id"]),
            "{schema}"
        );

        let Value::Object(arguments) = arguments else {
            panic!("arguments are an object");
        };
        let result = client
            .call_tool(CallToolRequestParams::new("ravel_get_trace").with_arguments(arguments))
            .await
            .expect("ravel_get_trace answers");
        let envelope = result
            .structured_content
            .clone()
            .expect("the tool result carries the envelope");
        assert_eq!(result.is_error, Some(false), "{envelope}");
        client.cancel().await.expect("client closes");
        envelope
    }

    #[tokio::test]
    async fn get_trace_over_mcp_without_a_window_returns_every_span() {
        let (store, now) = seeded().await;
        let running = start(store, true).await;
        let base = format!("http://{}", running.http_addr);
        let (_, http) = get_trace(&base, TOKEN, TRACE_HEX, &[]).await;

        let envelope = call_get_trace(&running, serde_json::json!({ "trace_id": TRACE_HEX })).await;
        assert_eq!(envelope["status"], "ok", "{envelope}");
        assert_eq!(envelope["scope"]["table"], "spans", "{envelope}");
        assert_eq!(envelope["data"]["row_count"], 3, "{envelope}");
        assert_eq!(span_ids(&envelope["data"]), want(&EVERY_SPAN), "{envelope}");
        assert_eq!(span_ids(&envelope["data"]), span_ids(&http["data"]));

        let windowed = call_get_trace(
            &running,
            serde_json::json!({
                "trace_id": TRACE_HEX,
                "time_range": {
                    "start_ns": (now - 30 * 60 * NS_PER_SEC).to_string(),
                    "end_ns": (now + 60 * NS_PER_SEC).to_string(),
                },
            }),
        )
        .await;
        assert_eq!(windowed["status"], "ok", "{windowed}");
        assert_eq!(windowed["data"]["row_count"], 1, "{windowed}");
        assert_eq!(
            span_ids(&windowed["data"]),
            want(&[RECENT_SPAN]),
            "{windowed}"
        );

        // Unlike the HTTP route, the tool also drops the rows that start
        // outside its window.
        let old = now - 5 * NS_PER_HOUR;
        let between = call_get_trace(
            &running,
            serde_json::json!({
                "trace_id": TRACE_HEX,
                "time_range": {
                    "start_ns": (old + NS_PER_SEC / 2).to_string(),
                    "end_ns": (now + 60 * NS_PER_SEC).to_string(),
                },
            }),
        )
        .await;
        assert_eq!(between["status"], "ok", "{between}");
        assert_eq!(
            span_ids(&between["data"]),
            want(&[OLD_SPANS[1], RECENT_SPAN]),
            "{between}"
        );

        // The tool's row budget cuts the result and says so.
        let capped = call_get_trace(
            &running,
            serde_json::json!({ "trace_id": TRACE_HEX, "max_rows": 2 }),
        )
        .await;
        assert_eq!(capped["status"], "ok_bounded", "{capped}");
        assert_eq!(capped["presentation"]["row_cap_hit"], true, "{capped}");
        assert_eq!(
            span_ids(&capped["data"]),
            want(&EVERY_SPAN[..2]),
            "{capped}"
        );

        running.shutdown().await.expect("graceful shutdown");
    }
}
