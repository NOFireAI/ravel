//! Reachability proof for `POST /api/v1/esql` (issue #2893): a real
//! [`ravel_server::start`] server over a `MemoryStore`, with logs ingested
//! through `POST /v1/logs`, answers ES|QL statements end to end on both
//! listeners, refuses a malformed statement before it reads the store, and
//! says so when it lowers a statement's `LIMIT`.
//!
//! The ES|QL tests are behind `feature = "esql"`; the one test that proves the
//! route is absent without it is behind `not(feature = "esql")`, so both
//! halves of the packaging decision are covered by the builds the gate list
//! runs. The test that the default feature set leaves `esql` off reads the
//! manifest and compiles in every build.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};

const TOKEN: &str = "acme-token";
/// The mTLS listener's own credential, mapped to the same tenant so both
/// listeners read one data set.
const MTLS_TOKEN: &str = "acme-mtls-token";
const TENANT: &str = "acme";

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn server_config(mtls: bool) -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let mut mtls_tokens = HashMap::new();
    mtls_tokens.insert(MTLS_TOKEN.to_string(), TenantId::new(TENANT));
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
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
        mtls_listener: mtls.then(|| ravel_server::MtlsListenerConfig {
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            resolver: ravel_server::tenant::build_resolver(mtls_tokens, false),
        }),
        fold_tenants: vec![tenant_hash()],
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
        scrub_period: Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: true,
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

async fn start(store: Arc<dyn ObjectStoreBackend>, mtls: bool) -> ravel_server::Running {
    ravel_server::start(
        server_config(mtls),
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// The `esql` feature is off, so the route it mounts does not exist and the
/// server answers its own 404 for an unknown path.
#[cfg(not(feature = "esql"))]
#[tokio::test]
async fn feature_off_build_serves_no_esql_route() {
    let running = start(Arc::new(MemoryStore::new()), false).await;

    let response = reqwest::Client::new()
        .post(format!("http://{}/api/v1/esql", running.http_addr))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(r#"{"query":"ROW a = 1"}"#)
        .send()
        .await
        .expect("the request reaches the server");
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);

    running.shutdown().await.expect("server stops");
}

/// The chain of feature entries from one of `roots` to an entry that turns the
/// ES|QL surface on (the `esql` feature, or the `ravel-esql` dependency by any
/// spelling), following each entry that names another feature of this crate.
fn path_to_esql(features: &toml::Table, roots: &[&str]) -> Option<Vec<String>> {
    let mut queue: Vec<Vec<String>> = roots.iter().map(|root| vec![root.to_string()]).collect();
    let mut seen = std::collections::HashSet::new();
    while let Some(path) = queue.pop() {
        let entry = path.last().expect("a path is never empty").clone();
        if entry == "esql"
            || entry == "ravel-esql"
            || entry.starts_with("dep:ravel-esql")
            || entry.starts_with("ravel-esql/")
            || entry.starts_with("ravel-esql?/")
        {
            return Some(path);
        }
        if !seen.insert(entry.clone()) {
            continue;
        }
        let Some(enabled) = features.get(&entry).and_then(toml::Value::as_array) else {
            continue;
        };
        for next in enabled.iter().filter_map(toml::Value::as_str) {
            let mut longer = path.clone();
            longer.push(next.to_string());
            queue.push(longer);
        }
    }
    None
}

/// The ES|QL surface is opt-in: no feature in the crate's default set enables
/// it, directly or through another feature. Compiled in every build, so the
/// default build checks its own manifest. The walk is shown able to find the
/// surface from `esql` itself, so an empty answer is not a walk that could
/// never have found one.
#[test]
fn default_features_do_not_enable_esql() {
    let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("read the crate manifest");
    let manifest: toml::Table = manifest.parse().expect("the manifest is TOML");
    let features = manifest
        .get("features")
        .and_then(toml::Value::as_table)
        .expect("the manifest has a [features] table");
    assert!(
        path_to_esql(features, &["esql"]).is_some(),
        "the esql feature is not where this test looks for it"
    );
    assert_eq!(
        path_to_esql(features, &["default"]),
        None,
        "the default features enable the ES|QL surface"
    );
}

#[cfg(feature = "esql")]
mod with_esql {
    use super::*;

    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    use async_trait::async_trait;
    use bytes::Bytes;
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use prost::Message;
    use ravel_object_store::{
        Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
        PageToken, PutOptions, PutOutcome, StoreError,
    };
    use serde_json::{Value, json};

    const NS_PER_SECOND: i64 = 1_000_000_000;
    const BUCKET_NS: i64 = 300 * NS_PER_SECOND;
    /// More than the 100 rows the `LIMIT` statement keeps, so its count is
    /// distinguishable from a count over every row.
    const ROWS: i64 = 120;
    /// Row `i` is an error when `i % ERROR_EVERY == 0`.
    const ERROR_EVERY: i64 = 7;

    fn now_ns() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_nanos() as i64
    }

    /// Every store call under the tenant's own prefix, as `(op, key)`. The
    /// server's background tasks write outside it, so a count here is the
    /// tenant's queries and ingest alone.
    #[derive(Clone, Default)]
    struct OpLog(Arc<Mutex<Vec<(&'static str, String)>>>);

    impl OpLog {
        fn record(&self, op: &'static str, key: &str) {
            if key.starts_with(&format!("t/{}/", tenant_hash().to_hex())) {
                self.0.lock().expect("lock").push((op, key.to_string()));
            }
        }

        /// The reads a query request can issue before it reaches segment
        /// data: GETs, HEADs and LISTs under the tenant's catalog prefix and
        /// of its configuration record. A background flush or maintenance
        /// write is not one, so it cannot move this count.
        fn query_reads(&self) -> usize {
            let tenant = tenant_hash().to_hex();
            let catalog = format!("t/{tenant}/catalog/");
            let config = format!("t/{tenant}/config");
            self.0
                .lock()
                .expect("lock")
                .iter()
                .filter(|(op, key)| {
                    matches!(*op, "get" | "head" | "list")
                        && (key.starts_with(&catalog) || *key == config)
                })
                .count()
        }
    }

    struct RecordingStore {
        inner: MemoryStore,
        ops: OpLog,
    }

    #[async_trait]
    impl ObjectStoreBackend for RecordingStore {
        async fn put(&self, k: &str, d: Bytes, o: PutOptions) -> Result<PutOutcome, StoreError> {
            self.ops.record("put", k);
            self.inner.put(k, d, o).await
        }
        async fn get(&self, k: &str, r: GetRange) -> Result<GetOutcome, StoreError> {
            self.ops.record("get", k);
            self.inner.get(k, r).await
        }
        async fn put_multipart<'a>(
            &'a self,
            k: &str,
        ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
            self.ops.record("put_multipart", k);
            self.inner.put_multipart(k).await
        }
        async fn head(&self, k: &str) -> Result<ObjectMeta, StoreError> {
            self.ops.record("head", k);
            self.inner.head(k).await
        }
        async fn list(&self, p: &str, t: Option<PageToken>) -> Result<ListPage, StoreError> {
            self.ops.record("list", p);
            self.inner.list(p, t).await
        }
        async fn list_after(
            &self,
            p: &str,
            start_after: Option<&str>,
            t: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
            self.ops.record("list", p);
            self.inner.list_after(p, start_after, t).await
        }
        async fn list_delimited(&self, p: &str) -> Result<DelimitedList, StoreError> {
            self.ops.record("list", p);
            self.inner.list_delimited(p).await
        }
        async fn delete(&self, k: &str) -> Result<(), StoreError> {
            self.ops.record("delete", k);
            self.inner.delete(k).await
        }
        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }
    }

    fn str_kv(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueVariant::StringValue(value.to_string())),
            }),
            ..Default::default()
        }
    }

    fn service_of(i: i64) -> &'static str {
        if i % 2 == 0 { "api" } else { "web" }
    }

    fn is_error(i: i64) -> bool {
        i % ERROR_EVERY == 0
    }

    /// The fixture's rows: row `i` at `base + i * 10s`, its `service.name` a
    /// resource attribute and its `log.level` a record attribute.
    fn export_request(base_ns: i64) -> ExportLogsServiceRequest {
        let resource_logs = ["api", "web"]
            .into_iter()
            .map(|service| {
                let log_records = (0..ROWS)
                    .filter(|i| service_of(*i) == service)
                    .map(|i| {
                        let ts = (base_ns + i * 10 * NS_PER_SECOND) as u64;
                        LogRecord {
                            time_unix_nano: ts,
                            observed_time_unix_nano: ts,
                            severity_number: if is_error(i) { 17 } else { 9 },
                            severity_text: String::new(),
                            body: Some(AnyValue {
                                value: Some(AnyValueVariant::StringValue(format!("row {i}"))),
                            }),
                            attributes: vec![str_kv(
                                "log.level",
                                if is_error(i) { "ERROR" } else { "INFO" },
                            )],
                            ..Default::default()
                        }
                    })
                    .collect();
                ResourceLogs {
                    resource: Some(Resource {
                        attributes: vec![str_kv("service.name", service)],
                        ..Default::default()
                    }),
                    scope_logs: vec![ScopeLogs {
                        scope: None,
                        log_records,
                        schema_url: String::new(),
                    }],
                    ..Default::default()
                }
            })
            .collect();
        ExportLogsServiceRequest { resource_logs }
    }

    struct Fixture {
        ops: OpLog,
        running: ravel_server::Running,
        client: reqwest::Client,
        /// The first row's event time: a 5-minute boundary 40 to 45 minutes
        /// before the test started, so all four buckets the rows span are
        /// inside the last hour and every row is before the query clock.
        base_ns: i64,
    }

    impl Fixture {
        async fn start(mtls: bool) -> Self {
            let ops = OpLog::default();
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(RecordingStore {
                inner: MemoryStore::new(),
                ops: ops.clone(),
            });
            let running = super::start(store, mtls).await;
            let base_ns = (now_ns() - 40 * 60 * NS_PER_SECOND).div_euclid(BUCKET_NS) * BUCKET_NS;
            Fixture {
                ops,
                running,
                client: reqwest::Client::new(),
                base_ns,
            }
        }

        fn base(&self) -> String {
            format!("http://{}", self.running.http_addr)
        }

        async fn ingest(&self) {
            let response = self
                .client
                .post(format!("{}/v1/logs", self.base()))
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/x-protobuf")
                .body(export_request(self.base_ns).encode_to_vec())
                .send()
                .await
                .expect("export sent");
            assert_eq!(response.status(), 200, "export accepted");
            assert!(
                response.headers().get("x-ravel-commit-token").is_some(),
                "a strict export acks after its commit"
            );
        }

        async fn esql_at(&self, base: &str, token: &str, body: Value) -> (u16, Value) {
            let response = self
                .client
                .post(format!("{base}/api/v1/esql"))
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(body.to_string())
                .send()
                .await
                .expect("esql request sent");
            let status = response.status().as_u16();
            let value: Value = response.json().await.expect("esql response is JSON");
            (status, value)
        }

        async fn esql(&self, query: &str) -> (u16, Value) {
            self.esql_at(&self.base(), TOKEN, json!({ "query": query }))
                .await
        }
    }

    /// The `stats` a statement over the catalog reports: the six sections
    /// `/api/v1/sql` reports, and a store read and a segment counted in them.
    fn assert_catalog_stats(stats: &Value) {
        let mut keys: Vec<&str> = stats
            .as_object()
            .expect("stats is an object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            [
                "accounting",
                "estimate",
                "io",
                "phases",
                "pruning",
                "timings"
            ],
            "{stats}"
        );
        assert!(stats["phases"].is_array(), "{stats}");
        assert!(
            stats["accounting"]["s3GetRequests"]
                .as_u64()
                .is_some_and(|n| n > 0),
            "a statement over ingested logs reads the store: {stats}"
        );
        assert!(
            stats["pruning"]["segments"].as_u64().is_some_and(|n| n > 0),
            "{stats}"
        );
        assert!(stats["timings"]["auditMs"].is_number(), "{stats}");
    }

    #[tokio::test]
    async fn esql_pipeline_over_logs_through_a_started_server() {
        let fx = Fixture::start(false).await;
        fx.ingest().await;

        let (status, body) = fx
            .esql(
                "FROM logs\n\
                 | WHERE @timestamp >= NOW() - 1 hour\n\
                 | STATS errors = COUNT(*) WHERE log.level == \"ERROR\"\n  \
                   BY window = BUCKET(@timestamp, 5 minutes), service.name\n\
                 | SORT window ASC, service.name ASC",
            )
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["status"], "success");
        assert_eq!(
            body["data"]["columns"],
            json!([
                { "name": "errors", "type": "long" },
                { "name": "window", "type": "date_nanos" },
                { "name": "service.name", "type": "keyword" },
            ]),
        );
        let mut expected = Vec::new();
        for bucket in 0..4 {
            for service in ["api", "web"] {
                let errors = (bucket * 30..bucket * 30 + 30)
                    .filter(|i| service_of(*i) == service && is_error(*i))
                    .count();
                expected.push(json!([errors, fx.base_ns + bucket * BUCKET_NS, service]));
            }
        }
        assert_eq!(body["data"]["rows"], Value::Array(expected.clone()));
        // Pinned by hand too, so the derivation above cannot drift with it.
        let errors: Vec<u64> = expected
            .iter()
            .map(|row| row[0].as_u64().expect("count"))
            .collect();
        assert_eq!(errors, [3, 2, 2, 2, 2, 2, 2, 3]);
        assert!(body.get("warnings").is_none(), "{body}");
        assert_catalog_stats(&body["stats"]);

        let (status, body) = fx
            .esql(
                "FROM logs\n\
                 | SORT @timestamp DESC\n\
                 | LIMIT 100\n\
                 | STATS recent_errors = COUNT(*) WHERE log.level == \"ERROR\"",
            )
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            body["data"]["columns"],
            json!([{ "name": "recent_errors", "type": "long" }]),
        );
        let recent = (ROWS - 100..ROWS).filter(|i| is_error(*i)).count();
        let all = (0..ROWS).filter(|i| is_error(*i)).count();
        assert_eq!((recent, all), (15, 18), "the LIMIT changes the answer");
        assert_eq!(body["data"]["rows"], json!([[recent]]));
        assert_catalog_stats(&body["stats"]);

        // The HTTP reference's example statement.
        let (status, body) = fx
            .esql(
                "FROM logs | WHERE @timestamp >= NOW() - 1 hour | STATS errors = COUNT(*) \
                 WHERE log.level == \"ERROR\" BY service.name | SORT service.name",
            )
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            body["data"]["columns"],
            json!([
                { "name": "errors", "type": "long" },
                { "name": "service.name", "type": "keyword" },
            ]),
        );
        let per_service = |service: &str| {
            (0..ROWS)
                .filter(|i| service_of(*i) == service && is_error(*i))
                .count()
        };
        assert_eq!((per_service("api"), per_service("web")), (9, 9));
        assert_eq!(body["data"]["rows"], json!([[9, "api"], [9, "web"]]));
        assert_catalog_stats(&body["stats"]);

        fx.running.shutdown().await.expect("graceful shutdown");
    }

    /// A construct outside the supported set is a typed 400 naming it and
    /// where it starts.
    #[tokio::test]
    async fn an_unsupported_construct_is_a_typed_400() {
        let fx = Fixture::start(false).await;
        fx.ingest().await;

        for (query, message) in [
            (
                "FROM metrics",
                "line 1:6: source [metrics] is not supported",
            ),
            (
                "FROM logs | EVAL x = TO_UPPER(message)",
                "line 1:22: function [TO_UPPER] is not supported",
            ),
        ] {
            let (status, body) = fx.esql(query).await;
            assert_eq!(status, 400, "{query}: {body}");
            assert_eq!(body["status"], "error");
            assert_eq!(body["errorType"], "bad_data");
            assert_eq!(body["error"], message, "{query}");
        }

        fx.running.shutdown().await.expect("graceful shutdown");
    }

    /// A malformed statement is a 400 naming its line and column, and the
    /// server answers it without reading the tenant's catalog or
    /// configuration. The well-formed statement after it is the control: the
    /// same observer sees the reads a statement that parses does issue. The permit half, which a
    /// started server does not expose, is
    /// `service::tests::esql_parse_error_takes_no_permit_and_reads_no_store`.
    #[tokio::test]
    async fn parse_error_reads_no_catalog_and_takes_no_permit() {
        let fx = Fixture::start(false).await;
        fx.ingest().await;

        let before = fx.ops.query_reads();
        let (status, body) = fx.esql("FROM logs\n| WHERE").await;
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["status"], "error");
        assert_eq!(body["errorType"], "bad_data");
        let message = body["error"].as_str().expect("error message");
        assert!(message.starts_with("line 2:"), "{message}");
        assert_eq!(
            fx.ops.query_reads(),
            before,
            "a malformed statement reads nothing"
        );

        let (status, body) = fx.esql("FROM logs | LIMIT 1").await;
        assert_eq!(status, 200, "{body}");
        assert!(
            fx.ops.query_reads() > before,
            "the control statement reads the store"
        );

        fx.running.shutdown().await.expect("graceful shutdown");
    }

    #[tokio::test]
    async fn params_are_refused() {
        let fx = Fixture::start(false).await;
        let (status, body) = fx
            .esql_at(
                &fx.base(),
                TOKEN,
                json!({ "query": "ROW a = ?x", "params": [{ "x": 1 }] }),
            )
            .await;
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["errorType"], "bad_data");
        assert_eq!(
            body["error"],
            "\"params\" is not supported: write each value into the statement"
        );
        fx.running.shutdown().await.expect("graceful shutdown");
    }

    /// A `LIMIT` above the ceiling runs at the ceiling and says so.
    #[tokio::test]
    async fn a_lowered_limit_is_reported_as_a_warning() {
        let fx = Fixture::start(false).await;
        fx.ingest().await;

        let (status, body) = fx.esql("FROM logs | LIMIT 20000").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            body["warnings"],
            json!([
                "the statement's LIMIT 20000 was lowered to 10000, the most rows an ES|QL query returns"
            ]),
        );
        assert_eq!(
            body["data"]["rows"].as_array().map(Vec::len),
            Some(ROWS as usize)
        );

        let (status, body) = fx.esql("FROM logs | LIMIT 10000").await;
        assert_eq!(status, 200, "{body}");
        assert!(body.get("warnings").is_none(), "{body}");

        fx.running.shutdown().await.expect("graceful shutdown");
    }

    /// The mTLS listener serves the route too, authenticated by its own
    /// resolver: its credential works there and the public one does not.
    #[tokio::test]
    async fn the_mtls_listener_serves_the_route() {
        let fx = Fixture::start(true).await;
        fx.ingest().await;
        let mtls = format!(
            "http://{}",
            fx.running
                .mtls_addr
                .expect("the mTLS listener was configured")
        );
        let statement = json!({
            "query": "FROM logs | STATS n = COUNT(*)"
        });

        let (status, body) = fx.esql_at(&mtls, MTLS_TOKEN, statement.clone()).await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["data"]["rows"], json!([[ROWS]]));

        let (status, body) = fx.esql_at(&mtls, TOKEN, statement).await;
        assert_eq!(status, 401, "{body}");

        fx.running.shutdown().await.expect("graceful shutdown");
    }

    /// A `ROW` statement reads no catalog: its `stats` are the zero
    /// accounting and estimate alone.
    #[tokio::test]
    async fn a_row_statement_reads_nothing() {
        let fx = Fixture::start(false).await;
        let before = fx.ops.query_reads();
        let (status, body) = fx.esql("ROW a = 1, b = \"x\"").await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(
            body["data"]["columns"],
            json!([
                { "name": "a", "type": "long" },
                { "name": "b", "type": "keyword" },
            ]),
        );
        assert_eq!(body["data"]["rows"], json!([[1, "x"]]));
        let mut keys: Vec<&str> = body["stats"]
            .as_object()
            .expect("stats is an object")
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort_unstable();
        assert_eq!(keys, ["accounting", "estimate"], "{body}");
        assert_eq!(body["stats"]["accounting"]["s3GetRequests"], 0);
        assert_eq!(body["stats"]["estimate"]["estimatedRequests"], 0);
        assert_eq!(
            fx.ops.query_reads(),
            before,
            "no catalog or configuration read for a ROW statement"
        );
        fx.running.shutdown().await.expect("graceful shutdown");
    }
}
