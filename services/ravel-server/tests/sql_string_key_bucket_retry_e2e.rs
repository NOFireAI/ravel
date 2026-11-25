//! Reachability proof for ADR-4143 decision 3 (issue #4132): a GROUP BY on a
//! declared `str` key, sent as `POST /api/v1/sql` to a real
//! [`ravel_server::start`] server whose process memory budget refuses its
//! first attempt, retries through the hash exchange and the bucketed final
//! aggregate and answers exactly what a server with no memory limit answers.
//!
//! The rows are ingested once through `POST /v1/logs` in strict mode, so they
//! are durable in the shared store before either server queries them. The
//! first server runs with no process memory budget and reports the
//! statement's peak intermediate bytes; the second runs over the same store
//! with a budget of nine tenths of that peak, the refusal
//! `the_server_built_executor_retries_through_buckets` (issue #4075) uses for
//! fixed-width keys. The retry's INFO line is read from the server's own
//! tracing output, captured by a process-wide subscriber.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_catalog::{DeclaredColumnType, DeclaredTypedColumn, TenantConfig, TenantLifecycleState};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Rows in the fixture: enough distinct keys that each of the two final
/// partitions holds far more than the 65,536 groups one bucket answers in
/// memory, so the retry routes rows to buckets, while the unlimited run stays
/// inside [`QUERY_BYTES`].
const ROWS: i64 = 2_000_000;

/// Rows per OTLP export, so each request body stays well under the 16 MiB
/// HTTP body limit.
const ROWS_PER_EXPORT: i64 = 100_000;

/// The final-aggregate partition count, set as an operator flag so the
/// first-attempt floor of issue #4014 does not raise it.
const PARTITIONS: usize = 2;

/// The per-query SQL limit: above the unlimited run's peak, so only the
/// process memory budget refuses the first attempt.
const QUERY_BYTES: usize = 1 << 30;

/// The soft open-file limit handed to the spill inputs, as `main` passes it
/// on a host that raised its limit: Linux's default of 1024 leaves too few
/// descriptors for the bits this fixture needs.
const NOFILE_SOFT: u64 = 1 << 16;

/// The grouped statement. `null_key` tells the NULL group from the empty one.
const STATEMENT: &str = "SELECT s, s IS NULL AS null_key, count(*) AS c FROM logs GROUP BY s";

/// The message of the spill retry's one INFO line.
const RETRY_LINE: &str = "SQL statement retried with spill after a memory refusal";

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// Row `i`'s declared `str` attribute `s`: unset, so NULL, on `i % 10 = 0`;
/// the empty string on `i % 10 = 1`; `s` and `i`'s digits, at most 8 bytes,
/// which a view holds inline, on `i % 10` in 2..=4; and a key of 14 to 20
/// bytes, longer than a view's 12 inline bytes, otherwise.
fn str_key(i: i64) -> Option<String> {
    match i % 10 {
        0 => None,
        1 => Some(String::new()),
        2..=4 => Some(format!("s{i}")),
        _ => Some(format!("a-longer-key-{i}")),
    }
}

fn export_request(rows: std::ops::Range<i64>, base_ts_ns: i64) -> ExportLogsServiceRequest {
    let log_records = rows
        .map(|i| {
            let attributes = str_key(i)
                .map(|s| KeyValue {
                    key: "s".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue(s)),
                    }),
                    ..Default::default()
                })
                .into_iter()
                .collect();
            LogRecord {
                time_unix_nano: (base_ts_ns + i) as u64,
                observed_time_unix_nano: (base_ts_ns + i) as u64,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue("hit".to_string())),
                }),
                attributes,
                ..Default::default()
            }
        })
        .collect();
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue("web".to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            ..Default::default()
        }],
    }
}

/// Every byte the process-wide subscriber writes.
#[derive(Clone, Default)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl CapturedLog {
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("log buffer lock")).into_owned()
    }
}

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

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLog {
    type Writer = CapturedLog;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// A server over `store` with `process_memory_budget_bytes` as its process
/// memory budget and spill under `cache_dir`.
struct Server {
    running: ravel_server::Running,
    base: String,
    client: reqwest::Client,
}

impl Server {
    async fn start(
        store: Arc<dyn ObjectStoreBackend>,
        process_memory_budget_bytes: u64,
        cache_dir: &std::path::Path,
    ) -> Self {
        let mut tokens = HashMap::new();
        tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
        let running = ravel_server::start(
            server_config(tokens, process_memory_budget_bytes, cache_dir),
            store.clone(),
            store,
            Arc::new(ravel_object_store::StoreMetrics::default()),
            None,
        )
        .await
        .expect("server starts");
        let base = format!("http://{}", running.http_addr);
        Server {
            running,
            base,
            client: reqwest::Client::new(),
        }
    }

    /// Export `rows` as one strict-mode OTLP request.
    async fn ingest(&self, rows: std::ops::Range<i64>, base_ts_ns: i64) {
        let request = export_request(rows, base_ts_ns);
        let response = self
            .client
            .post(format!("{}/v1/logs", self.base))
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/x-protobuf")
            .body(request.encode_to_vec())
            .send()
            .await
            .expect("export sent");
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        assert_eq!(status, 200, "export accepted: {text}");
    }

    /// `POST /api/v1/sql` for `query` over `[start_ns, end_ns]`.
    async fn sql(&self, query: &str, start_ns: i64, end_ns: i64) -> serde_json::Value {
        let response = self
            .client
            .post(format!("{}/api/v1/sql", self.base))
            .header("authorization", format!("Bearer {TOKEN}"))
            .header("content-type", "application/json")
            .body(
                serde_json::json!({
                    "query": query,
                    "start": start_ns as f64 / 1e9,
                    "end": end_ns as f64 / 1e9,
                })
                .to_string(),
            )
            .send()
            .await
            .expect("sql request sent");
        let status = response.status();
        let value: serde_json::Value = response.json().await.expect("sql response is JSON");
        assert_eq!(status, 200, "{value}");
        value
    }
}

fn server_config(
    tokens: HashMap<String, TenantId>,
    process_memory_budget_bytes: u64,
    cache_dir: &std::path::Path,
) -> ServerConfig {
    let query_budgets = ravel_server::config::QueryBudgets {
        sql_partition_count: PARTITIONS,
        sql_partition_count_source: ravel_server::config::PERF_SOURCE_FLAG,
        sql_max_query_bytes: QUERY_BYTES,
        sql_tenant_max_bytes: QUERY_BYTES,
        sql_spill: ravel_server::config::SqlSpillSettings {
            nofile_soft: NOFILE_SOFT,
            ..Default::default()
        },
        ..Default::default()
    };
    ServerConfig {
        emit_declared_stat_aggregates: true,
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets,
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_millis(50),
        max_flush_delay_idle: Duration::from_millis(50),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants: vec![tenant_hash()],
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
        query_deadline: Duration::from_secs(600),
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: true,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes,
        process_memory_budget_is_fallback: false,
        cache_dir: Some(cache_dir.to_path_buf()),
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

/// The response's rows, each serialized as JSON, sorted.
fn sorted_rows(value: &serde_json::Value) -> Vec<String> {
    let mut rows: Vec<String> = value["data"]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("data.rows: {}", truncated(value)))
        .iter()
        .map(serde_json::Value::to_string)
        .collect();
    rows.sort();
    rows
}

/// [`STATEMENT`]'s answer over the fixture, computed from [`str_key`], each
/// row serialized as the response serializes it, sorted.
fn expected_rows() -> Vec<String> {
    let mut groups: BTreeMap<Option<String>, i64> = BTreeMap::new();
    for i in 0..ROWS {
        *groups.entry(str_key(i)).or_default() += 1;
    }
    let mut rows: Vec<String> = groups
        .into_iter()
        .map(|(s, c)| serde_json::json!([s, s.is_none(), c]).to_string())
        .collect();
    rows.sort();
    rows
}

/// The first 2,000 characters of `value`, so a failure message stays
/// readable when the response carries a million rows.
fn truncated(value: &serde_json::Value) -> String {
    value.to_string().chars().take(2_000).collect()
}

/// The value of ` name=` on `line`, quotes removed.
fn field<'a>(line: &'a str, name: &str) -> &'a str {
    let key = format!(" {name}=");
    let start = line
        .find(&key)
        .unwrap_or_else(|| panic!("no {name} on {line}"))
        + key.len();
    let rest = &line[start..];
    match rest.strip_prefix('"') {
        Some(quoted) => &quoted[..quoted.find('"').expect("closing quote")],
        None => rest.split(' ').next().expect("a value"),
    }
}

/// [`field`] parsed as a number.
fn number(line: &str, name: &str) -> u64 {
    field(line, name)
        .parse()
        .unwrap_or_else(|e| panic!("{name} is not a number ({e}) on {line}"))
}

/// A refused GROUP BY on a declared `str` key, with NULL, the empty string
/// and keys longer than a view's 12 inline bytes among its values, retries
/// through the bucketed final aggregate on a real server and answers over
/// HTTP exactly what the same statement answers on a server with no memory
/// limit, which is the answer computed from the ingested rows. The server
/// logs the retry's INFO line once, with `retry_plan="bucket"`,
/// `retry_plan_reason="none"`, at least one routing bit and a numeric
/// `routed_chunk_bytes` of at least the 16 KiB floor, and that line counts
/// state rows written to buckets and the same number read back.
///
/// Prove-the-test: make `admits` in ravel-sql's `bucketed_final_aggregate.rs`
/// refuse a string key with spill on and the INFO line reads
/// `retry_plan="sort_merge"` with `retry_plan_reason="not_admitted"`; give
/// the second server twice the unlimited server's peak as its budget and no
/// retry runs, so no INFO line is logged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_server_retries_a_string_keyed_group_by_through_buckets() {
    let log = CapturedLog::default();
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .with_writer(log.clone())
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .finish(),
    )
    .expect("the only subscriber in this test binary");

    let memory = Arc::new(MemoryStore::new());
    let config = TenantConfig {
        typed_attr_columns: Some(vec![DeclaredTypedColumn {
            key: "s".to_string(),
            ty: DeclaredColumnType::Str,
        }]),
        ..TenantConfig::new(TenantLifecycleState::Active)
    };
    ravel_catalog::set_tenant_config(memory.as_ref(), &tenant_hash(), &config, now_ns())
        .await
        .expect("tenant config");
    let store: Arc<dyn ObjectStoreBackend> = memory;

    let base_ts_ns = now_ns();
    let (start_ns, end_ns) = (base_ts_ns - NS_PER_HOUR, base_ts_ns + ROWS + NS_PER_HOUR);
    let expected = expected_rows();
    assert_eq!(expected.len(), ROWS as usize * 8 / 10 + 2);

    let unlimited_cache = tempfile::tempdir().expect("cache dir");
    let unlimited = Server::start(Arc::clone(&store), u64::MAX, unlimited_cache.path()).await;
    let mut first = 0;
    while first < ROWS {
        let last = (first + ROWS_PER_EXPORT).min(ROWS);
        unlimited.ingest(first..last, base_ts_ns).await;
        first = last;
    }
    let answer = unlimited.sql(STATEMENT, start_ns, end_ns).await;
    let peak = answer["stats"]["accounting"]["peakIntermediateBytes"]
        .as_u64()
        .unwrap_or_else(|| {
            panic!(
                "stats.accounting.peakIntermediateBytes: {}",
                truncated(&answer)
            )
        });
    let unlimited_rows = sorted_rows(&answer);
    assert!(
        unlimited_rows == expected,
        "the unlimited server answers the ingested rows' groups"
    );
    unlimited
        .running
        .shutdown()
        .await
        .expect("graceful shutdown");
    assert!(
        !log.text().contains(RETRY_LINE),
        "the unlimited server answers on its first attempt"
    );

    let budget = peak * 9 / 10;
    let refused_cache = tempfile::tempdir().expect("cache dir");
    let refused = Server::start(Arc::clone(&store), budget, refused_cache.path()).await;
    let answer = refused.sql(STATEMENT, start_ns, end_ns).await;
    let refused_rows = sorted_rows(&answer);
    refused.running.shutdown().await.expect("graceful shutdown");

    let text = log.text();
    let info: Vec<&str> = text
        .lines()
        .filter(|line| line.contains(RETRY_LINE))
        .collect();
    assert_eq!(
        info.len(),
        1,
        "one retry INFO line at a budget of {budget} B (peak {peak} B): {info:?}"
    );
    let line = info[0];
    assert_eq!(field(line, "outcome"), "answered", "{line}");
    assert_eq!(field(line, "retry_plan"), "bucket", "{line}");
    assert_eq!(field(line, "retry_plan_reason"), "none", "{line}");
    assert!(number(line, "bits") >= 1, "rows fan out to buckets: {line}");
    // A number, not "none", is what shows the route is byte-budgeted.
    let chunk_bytes = number(line, "routed_chunk_bytes");
    assert!(
        chunk_bytes >= ravel_sql::BUCKET_SPILL_MIN_CHUNK_BYTES as u64,
        "{line}"
    );
    let written = number(line, "bucket_spill_rows_written");
    assert!(written > 0, "rows spilled to buckets: {line}");
    assert!(
        written <= ROWS as u64,
        "each row written at most once: {line}"
    );
    assert_eq!(
        number(line, "bucket_spill_rows_read"),
        written,
        "every spilled row read back: {line}"
    );

    assert_eq!(refused_rows.len(), unlimited_rows.len());
    assert!(
        refused_rows == unlimited_rows,
        "the retried answer equals the unlimited server's, row for row"
    );
}
