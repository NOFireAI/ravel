//! `ipcrypt_encrypt` and `ipcrypt_prefix_encrypt` through `POST /api/v1/sql`
//! on a real [`ravel_server::start_with_ipcrypt`] server (ADR-2993 decision
//! 6, issue #3050).
//!
//! The server runs an ingest policy with a deterministic rule on `client.ip`
//! and a prefix rule on `client.net`, under a deployment key on a bucket
//! pinned to the keyed tenant-hash scheme. Each tenant exports one log record
//! carrying the same address under both keys through `POST /v1/logs`, so the
//! stored values are whatever the ingest rules wrote. A token whose tenant
//! ends in `;ddl,ipcrypt` holds the capability; a token with no suffix does
//! not.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::ipcrypt::IpcryptPolicy;
use ravel_server::tenancy::ConfiguredScheme;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_tenant_resolve::{Principal, StaticBearerTokenResolver};
use ravel_types::TenantId;

const DEPLOYMENT_KEY: [u8; 32] = [0x5a; 32];
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// RFC 5737 TEST-NET-2.
const ADDR: &str = "198.51.100.7";

const POLICY: &str = r#"
[[defaults.rules]]
key = "client.ip"
variant = "deterministic"

[[defaults.rules]]
key = "client.net"
variant = "prefix"
"#;

const ACME: &str = "acme";
const GLOBEX: &str = "globex";
/// `acme;ddl,ipcrypt`.
const ACME_IPCRYPT_TOKEN: &str = "acme-ipcrypt-token";
/// `acme`, no capability.
const ACME_PLAIN_TOKEN: &str = "acme-plain-token";
/// `globex;ddl,ipcrypt`.
const GLOBEX_IPCRYPT_TOKEN: &str = "globex-ipcrypt-token";

const MATCH_DETERMINISTIC: &str =
    "SELECT body FROM logs WHERE attrs['client.ip'] = ipcrypt_encrypt('198.51.100.7', 1)";
const MATCH_PREFIX: &str =
    "SELECT body FROM logs WHERE attrs['client.net'] = ipcrypt_prefix_encrypt('198.51.100.7', 1)";
const UNKNOWN_FUNCTION: &str =
    "SELECT body FROM logs WHERE attrs['client.ip'] = no_such_function('198.51.100.7', 1)";

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
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

fn policy() -> Arc<IpcryptPolicy> {
    let file =
        ravel_ipcrypt::policy::parse_policy_file(POLICY, ravel_server::ipcrypt::sanitize_rule_key)
            .expect("policy parses");
    Arc::new(IpcryptPolicy::new(file, Some(&DEPLOYMENT_KEY)).expect("policy loads"))
}

/// What the ingest rule on `key` stores for [`ADDR`] under `tenant`.
fn stored(policy: &IpcryptPolicy, tenant: &str, key: &str) -> String {
    let rules = policy
        .rules_for(&TenantId::new(tenant))
        .expect("the tenant resolves");
    let (value, outcome) = rules.apply(key, ADDR).expect("the key is ruled");
    assert_eq!(outcome, ravel_ipcrypt::render::RenderOutcome::Encrypted);
    assert_ne!(value, ADDR);
    value
}

/// A fresh bucket pinned to the keyed tenant-hash scheme the way `main` pins
/// it. Every test in this binary uses [`DEPLOYMENT_KEY`], so a second install
/// meets the scheme already in force.
async fn keyed_bucket() -> Arc<dyn ObjectStoreBackend> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let resolved = ravel_server::tenancy::resolve_and_pin(
        store.as_ref(),
        ConfiguredScheme::Keyed(Box::new(DEPLOYMENT_KEY)),
        now_ns(),
    )
    .await
    .expect("a fresh bucket pins the keyed scheme");
    let _already_installed = ravel_types::install_tenant_hash_scheme(resolved.scheme);
    store
}

fn principals() -> HashMap<String, Principal> {
    let mut map = HashMap::new();
    for (token, tenant, ipcrypt) in [
        (ACME_IPCRYPT_TOKEN, ACME, true),
        (ACME_PLAIN_TOKEN, ACME, false),
        (GLOBEX_IPCRYPT_TOKEN, GLOBEX, true),
    ] {
        map.insert(
            token.to_string(),
            Principal {
                tenant: TenantId::new(tenant),
                ddl: ipcrypt,
                ipcrypt,
            },
        );
    }
    map
}

struct Fixture {
    running: ravel_server::Running,
    base: String,
    client: reqwest::Client,
    base_ts_ns: i64,
}

impl Fixture {
    /// A server whose query audit is `audit_pipeline`, recording statement
    /// text under `audit_text`.
    async fn start(
        audit_pipeline: Option<ravel_maintain::AuditPipelineConfig>,
        audit_text: ravel_maintain::AuditTextPolicy,
    ) -> Self {
        let store = keyed_bucket().await;
        let running = ravel_server::start_with_ipcrypt(
            server_config(audit_pipeline, audit_text),
            store.clone(),
            store,
            Arc::new(ravel_object_store::StoreMetrics::default()),
            None,
            policy(),
        )
        .await
        .expect("server starts");
        let base = format!("http://{}", running.http_addr);
        Fixture {
            running,
            base,
            client: reqwest::Client::new(),
            base_ts_ns: now_ns(),
        }
    }

    /// One strict-mode export of a record carrying [`ADDR`] under both ruled
    /// keys, with body `body`.
    async fn ingest(&self, token: &str, body: &str) {
        let ts = self.base_ts_ns as u64;
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: vec![str_kv("service.name", "edge")],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![LogRecord {
                        time_unix_nano: ts,
                        observed_time_unix_nano: ts,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        body: Some(AnyValue {
                            value: Some(AnyValueVariant::StringValue(body.to_string())),
                        }),
                        attributes: vec![str_kv("client.ip", ADDR), str_kv("client.net", ADDR)],
                        ..Default::default()
                    }],
                    schema_url: String::new(),
                }],
                ..Default::default()
            }],
        };
        let response = self
            .client
            .post(format!("{}/v1/logs", self.base))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/x-protobuf")
            .body(request.encode_to_vec())
            .send()
            .await
            .expect("export sent");
        assert_eq!(response.status(), 200, "export accepted");
    }

    /// `POST /api/v1/sql` over a window around the exported record, returning
    /// the status and the JSON body.
    async fn sql(&self, token: &str, query: &str) -> (u16, serde_json::Value) {
        let start = (self.base_ts_ns - NS_PER_HOUR) as f64 / 1e9;
        let end = (self.base_ts_ns + NS_PER_HOUR) as f64 / 1e9;
        let response = self
            .client
            .post(format!("{}/api/v1/sql", self.base))
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/json")
            .body(serde_json::json!({ "query": query, "start": start, "end": end }).to_string())
            .send()
            .await
            .expect("sql request sent");
        let status = response.status().as_u16();
        let value: serde_json::Value = response.json().await.expect("sql response is JSON");
        (status, value)
    }

    async fn rows(&self, token: &str, query: &str) -> Vec<serde_json::Value> {
        let (status, value) = self.sql(token, query).await;
        assert_eq!(status, 200, "{query}: {value}");
        value["data"]["rows"]
            .as_array()
            .unwrap_or_else(|| panic!("rows: {value}"))
            .clone()
    }

    /// The one value `select` (a single-column, single-row statement) returns.
    async fn scalar(&self, token: &str, select: &str) -> String {
        let rows = self.rows(token, select).await;
        assert_eq!(rows.len(), 1, "{select}: {rows:?}");
        rows[0][0]
            .as_str()
            .unwrap_or_else(|| panic!("{select}: a string, got {rows:?}"))
            .to_string()
    }

    /// `query` is refused exactly as a call to a function that does not exist
    /// is: same status, same body.
    async fn assert_refused_like_unknown(&self, token: &str, query: &str) {
        let (unknown_status, unknown_body) = self.sql(token, UNKNOWN_FUNCTION).await;
        assert_eq!(unknown_status, 422, "{unknown_body}");
        assert_eq!(unknown_body["error"], ravel_sql::MSG_PLAN, "{unknown_body}");
        let (status, body) = self.sql(token, query).await;
        assert_eq!(
            (status, &body),
            (unknown_status, &unknown_body),
            "{query} must be refused like an unknown function"
        );
    }
}

fn server_config(
    audit_pipeline: Option<ravel_maintain::AuditPipelineConfig>,
    audit_text: ravel_maintain::AuditTextPolicy,
) -> ServerConfig {
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline,
        audit_text,
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
        tenant_resolver: Arc::new(StaticBearerTokenResolver::with_principals(principals())),
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
        deployment_key: Some(Box::new(DEPLOYMENT_KEY)),
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

/// With the query audit off, an ipcrypt token finds the ingested record by
/// the address it was sent with, under either variant, and gets each
/// tenant's own ciphertext; a token without the capability is refused like an
/// unknown function; a wrong variant or a wrong epoch matches nothing, one
/// comparison per epoch ORed matches across a rotation, and epoch 0 is
/// refused.
///
/// Flipped: `SessionPrincipal::registers_ipcrypt` ignoring the capability
/// lets the plain token run the call, and the refusal assertion fails.
#[tokio::test]
async fn an_ipcrypt_token_matches_the_ingested_ciphertext() {
    let fx = Fixture::start(None, ravel_maintain::AuditTextPolicy::Plaintext).await;
    fx.ingest(ACME_IPCRYPT_TOKEN, "acme-hit").await;
    fx.ingest(GLOBEX_IPCRYPT_TOKEN, "globex-hit").await;
    let policy = policy();

    // The ingest rules wrote ciphertext, not the address.
    for (token, tenant) in [(ACME_IPCRYPT_TOKEN, ACME), (GLOBEX_IPCRYPT_TOKEN, GLOBEX)] {
        let ip = fx
            .scalar(token, "SELECT attrs['client.ip'] FROM logs")
            .await;
        assert_eq!(ip, stored(&policy, tenant, "client.ip"), "{tenant}");
        let net = fx
            .scalar(token, "SELECT attrs['client.net'] FROM logs")
            .await;
        assert_eq!(net, stored(&policy, tenant, "client.net"), "{tenant}");
    }

    for (token, body) in [
        (ACME_IPCRYPT_TOKEN, "acme-hit"),
        (GLOBEX_IPCRYPT_TOKEN, "globex-hit"),
    ] {
        for query in [MATCH_DETERMINISTIC, MATCH_PREFIX] {
            assert_eq!(
                fx.rows(token, query).await,
                vec![serde_json::json!([body])],
                "{token}: {query}"
            );
        }
    }

    // Two tenants, one address, two ciphertexts.
    let encrypt = "SELECT ipcrypt_encrypt('198.51.100.7', 1) FROM logs";
    let acme = fx.scalar(ACME_IPCRYPT_TOKEN, encrypt).await;
    let globex = fx.scalar(GLOBEX_IPCRYPT_TOKEN, encrypt).await;
    assert_ne!(acme, globex);
    assert_eq!(acme, stored(&policy, ACME, "client.ip"));
    assert_eq!(globex, stored(&policy, GLOBEX, "client.ip"));

    for wrong in [
        // The deterministic function against the prefix rule's column.
        "SELECT body FROM logs WHERE attrs['client.net'] = ipcrypt_encrypt('198.51.100.7', 1)",
        // The prefix function against the deterministic rule's column.
        "SELECT body FROM logs WHERE attrs['client.ip'] = ipcrypt_prefix_encrypt('198.51.100.7', 1)",
        // An epoch the rule never wrote under.
        "SELECT body FROM logs WHERE attrs['client.ip'] = ipcrypt_encrypt('198.51.100.7', 2)",
    ] {
        assert!(
            fx.rows(ACME_IPCRYPT_TOKEN, wrong).await.is_empty(),
            "{wrong} matches nothing"
        );
    }

    // One comparison per epoch, ORed, spans a rotation.
    assert_eq!(
        fx.rows(
            ACME_IPCRYPT_TOKEN,
            "SELECT body FROM logs \
             WHERE attrs['client.ip'] = ipcrypt_encrypt('198.51.100.7', 2) \
                OR attrs['client.ip'] = ipcrypt_encrypt('198.51.100.7', 1)",
        )
        .await,
        vec![serde_json::json!(["acme-hit"])]
    );

    let (status, value) = fx
        .sql(
            ACME_IPCRYPT_TOKEN,
            "SELECT body FROM logs WHERE attrs['client.ip'] = ipcrypt_encrypt('198.51.100.7', 0)",
        )
        .await;
    assert_eq!(status, 422, "epoch 0 is refused: {value}");

    for query in [MATCH_DETERMINISTIC, MATCH_PREFIX] {
        fx.assert_refused_like_unknown(ACME_PLAIN_TOKEN, query)
            .await;
    }

    fx.running.shutdown().await.expect("graceful shutdown");
}

/// With the query audit recording statement text in plaintext, neither
/// function is registered, even for an ipcrypt token: the statement carrying
/// the address would otherwise land in the audit trail as written.
///
/// Flipped: `SessionPrincipal::registers_ipcrypt` ignoring the audit lets the
/// call run, and the refusal assertion fails.
#[tokio::test]
async fn a_plaintext_query_audit_unregisters_the_functions() {
    let fx = Fixture::start(
        Some(ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        }),
        ravel_maintain::AuditTextPolicy::Plaintext,
    )
    .await;
    fx.ingest(ACME_IPCRYPT_TOKEN, "acme-hit").await;

    for query in [MATCH_DETERMINISTIC, MATCH_PREFIX] {
        fx.assert_refused_like_unknown(ACME_IPCRYPT_TOKEN, query)
            .await;
    }

    fx.running.shutdown().await.expect("graceful shutdown");
}

/// With the query audit on but recording redacted statement text, both
/// functions stay registered: an ipcrypt token's call runs and returns the
/// ciphertext the ingest rule stored.
///
/// Flipped: withholding the functions whenever the audit is on, whatever its
/// text policy, refuses the call and the row assertions fail.
#[tokio::test]
async fn a_redacted_query_audit_keeps_the_functions() {
    let audit_text = ravel_server::config::resolve_audit_text_policy(
        ravel_server::config::AuditTextArg::Redacted,
        None,
        Some(&DEPLOYMENT_KEY),
    )
    .expect("a deployment key yields a redacting policy");
    assert!(matches!(
        audit_text,
        ravel_maintain::AuditTextPolicy::Redacted(_)
    ));
    let fx = Fixture::start(
        Some(ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        }),
        audit_text,
    )
    .await;
    fx.ingest(ACME_IPCRYPT_TOKEN, "acme-hit").await;
    let policy = policy();

    assert_eq!(
        fx.scalar(
            ACME_IPCRYPT_TOKEN,
            "SELECT ipcrypt_encrypt('198.51.100.7', 1) FROM logs"
        )
        .await,
        stored(&policy, ACME, "client.ip")
    );
    for query in [MATCH_DETERMINISTIC, MATCH_PREFIX] {
        assert_eq!(
            fx.rows(ACME_IPCRYPT_TOKEN, query).await,
            vec![serde_json::json!(["acme-hit"])],
            "{query}"
        );
    }

    fx.running.shutdown().await.expect("graceful shutdown");
}
