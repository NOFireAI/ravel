//! `GET /metrics` is served on the HTTP listener in every mode, including
//! `Mode::Maintain` (issue #423), the same regression shape as
//! `health_endpoints.rs`'s maintain-mode guard.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use ravel_object_store::StoreMetrics;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";

/// Mirrors `health_endpoints.rs`'s helper: an in-process server backed by
/// `MemoryStore`, parameterized by mode.
async fn start_test_server(mode: Mode) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        mode,
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
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
    };
    ravel_server::start(config, store, Arc::new(StoreMetrics::default()), None)
        .await
        .expect("server starts")
}

/// Regression guard for issue #423: `/metrics` must be served in every mode,
/// maintain included, where before this change only `/healthz` and `/readyz`
/// existed.
#[tokio::test]
async fn metrics_served_in_every_mode() {
    for mode in [Mode::All, Mode::Gateway, Mode::Query, Mode::Maintain] {
        let running = start_test_server(mode).await;
        let base = format!("http://{}", running.http_addr);
        let client = reqwest::Client::new();

        let response = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("metrics request completes");
        assert_eq!(
            response.status(),
            200,
            "metrics must be 200 in mode {mode:?}"
        );

        let content_type = response
            .headers()
            .get("content-type")
            .expect("content-type header present")
            .to_str()
            .expect("content-type is ASCII");
        assert!(
            content_type.starts_with("text/plain"),
            "metrics content-type should be text/plain, got {content_type}"
        );

        let body = response.text().await.expect("metrics body is text");
        assert!(
            body.contains("ravel_store_calls_total"),
            "metrics body missing store family in mode {mode:?}:\n{body}"
        );
        assert!(
            body.contains("ravel_catalog_interlock_violations_total"),
            "metrics body missing catalog family in mode {mode:?}:\n{body}"
        );

        running.shutdown().await.expect("graceful shutdown");
    }
}

/// `Mode::All` and `Mode::Gateway` build ingest routers; `Mode::Query` and
/// `Mode::Maintain` do not. The ingest metric families must appear exactly
/// where the routers exist, not be zero-padded in modes that build none.
#[tokio::test]
async fn metrics_ingest_family_present_only_in_ingest_modes() {
    for (mode, expect_ingest) in [
        (Mode::All, true),
        (Mode::Gateway, true),
        (Mode::Query, false),
        (Mode::Maintain, false),
    ] {
        let running = start_test_server(mode).await;
        let base = format!("http://{}", running.http_addr);
        let client = reqwest::Client::new();

        let body = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("metrics request completes")
            .text()
            .await
            .expect("metrics body is text");

        assert_eq!(
            body.contains("ravel_ingest_flushes_by_size_total"),
            expect_ingest,
            "mode {mode:?} ingest family presence should be {expect_ingest}:\n{body}"
        );

        running.shutdown().await.expect("graceful shutdown");
    }
}

// --- ADR-0051 section 6: the /metrics admission family (issue #573) ---

use std::time::{SystemTime, UNIX_EPOCH};

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
use ravel_ingest::{AdmissionLimits, CountLimit, RateLimit};
use ravel_server::LimitsConfig;

/// One admission scenario: a distinct tenant, its bearer token, the limit
/// override that shapes what its request hits, and the rejection reason (if
/// any) that override forces onto the `/metrics` admission family.
struct Scenario {
    tenant: &'static str,
    token: &'static str,
    limits: AdmissionLimits,
    /// Distinct series in the one request this tenant sends. More than
    /// `max_active_series` forces per-series cap rejections; a fresh-series
    /// count over the creation-rate burst forces a whole-request rejection.
    series: usize,
    /// The `reason` label this scenario must produce, or `None` for the
    /// clean-admit tenant.
    expect_reason: Option<&'static str>,
}

/// Four tenants, one per admission outcome: a clean admit plus each of the
/// three rejection reasons the admission counters distinguish (byte_rate,
/// series_rate, series_cap). Every non-overridden limit stays at this
/// service's generous shipped defaults so exactly the intended layer fires.
fn admission_scenarios() -> Vec<Scenario> {
    let tight = |per_sec, burst| RateLimit::Bounded { per_sec, burst };
    vec![
        Scenario {
            tenant: "adm-admit",
            token: "tok-admit",
            limits: AdmissionLimits::default(),
            series: 2,
            expect_reason: None,
        },
        Scenario {
            tenant: "adm-cap",
            token: "tok-cap",
            limits: AdmissionLimits {
                max_active_series: CountLimit::Bounded(1),
                ..AdmissionLimits::default()
            },
            series: 4,
            expect_reason: Some("series_cap"),
        },
        Scenario {
            tenant: "adm-byte",
            token: "tok-byte",
            limits: AdmissionLimits {
                ingest_byte_rate: tight(1, 1),
                ..AdmissionLimits::default()
            },
            series: 2,
            expect_reason: Some("byte_rate"),
        },
        Scenario {
            tenant: "adm-srate",
            token: "tok-srate",
            limits: AdmissionLimits {
                series_creation_rate: tight(1, 1),
                ..AdmissionLimits::default()
            },
            series: 4,
            expect_reason: Some("series_rate"),
        },
    ]
}

fn admission_now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// An `ExportMetricsServiceRequest` with `count` distinct series (distinct
/// `__name__`), each one gauge point, mirroring `admission_e2e.rs`.
fn export_request(count: usize, ts_ns: i64) -> ExportMetricsServiceRequest {
    let metrics = (0..count)
        .map(|i| Metric {
            name: format!("adm_series_{i}"),
            data: Some(MetricData::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: ts_ns as u64,
                    value: Some(NumberValue::AsDouble(1.0)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue("adm".to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Starts a server in `Mode::All` with every scenario's tenant token and
/// per-tenant limit override installed, and `--metrics-tenant-labels` set to
/// `tenant_labels`.
async fn start_admission_server(tenant_labels: bool) -> ravel_server::Running {
    let scenarios = admission_scenarios();
    let mut tokens = HashMap::new();
    let mut tenants = HashMap::new();
    for s in &scenarios {
        tokens.insert(s.token.to_string(), TenantId::new(s.tenant));
        tenants.insert(TenantId::new(s.tenant), s.limits);
    }
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
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
        metrics_tenant_labels: tenant_labels,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        limits: LimitsConfig {
            defaults: ravel_server::config::limits::shipped_defaults(),
            tenants,
        },
    };
    ravel_server::start(config, store, Arc::new(StoreMetrics::default()), None)
        .await
        .expect("server starts")
}

/// Drives every scenario's one export request against `base`, generating the
/// admission activity the `/metrics` family then reports.
async fn drive_admission_activity(base: &str) {
    let client = reqwest::Client::new();
    for s in admission_scenarios() {
        let request = export_request(s.series, admission_now_ns());
        client
            .post(format!("{base}/v1/metrics"))
            .header("authorization", format!("Bearer {}", s.token))
            .header("content-type", "application/x-protobuf")
            .body(request.encode_to_vec())
            .send()
            .await
            .expect("export request completes");
    }
}

/// Distinct `tenant_hash` label values across every `ravel_admission_*`
/// sample line in an exposition body.
fn admission_tenant_hashes(body: &str) -> std::collections::HashSet<String> {
    let mut out = std::collections::HashSet::new();
    for line in body.lines() {
        if !line.starts_with("ravel_admission_") {
            continue;
        }
        let Some(brace) = line.find('{') else {
            continue;
        };
        let close = line.find('}').expect("closed label block");
        for pair in line[brace + 1..close].split(',') {
            if let Some(value) = pair.strip_prefix("tenant_hash=\"") {
                out.insert(value.trim_end_matches('"').to_string());
            }
        }
    }
    out
}

/// THE ACCEPTANCE TEST for issue #573 (ADR-0051 section 6). N tenants each
/// generate admission activity (one admitted, three rejected by distinct
/// reasons). Scraped with `--metrics-tenant-labels` off, every
/// `ravel_admission_*` series collapses to `tenant_hash="other"`, so the
/// exposition's cardinality does not grow with tenant count. Scraped with it
/// on, N distinct real `tenant_hash` values appear, and all three rejection
/// reasons are present and distinguishable.
#[tokio::test]
async fn admission_family_tenant_labels_bounded() {
    let scenarios = admission_scenarios();
    let tenant_count = scenarios.len();
    let expected_hashes: std::collections::HashSet<String> = scenarios
        .iter()
        .map(|s| TenantId::new(s.tenant).hash().to_hex())
        .collect();
    let expected_reasons: Vec<&str> = scenarios.iter().filter_map(|s| s.expect_reason).collect();
    let client = reqwest::Client::new();

    // --- Flag off: everything folds to tenant_hash="other". ---
    let running = start_admission_server(false).await;
    let base = format!("http://{}", running.http_addr);
    drive_admission_activity(&base).await;
    let body_off = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text");
    running.shutdown().await.expect("graceful shutdown");

    let hashes_off = admission_tenant_hashes(&body_off);
    assert_eq!(
        hashes_off,
        std::collections::HashSet::from(["other".to_string()]),
        "with --metrics-tenant-labels off, {tenant_count} tenants must collapse to exactly \
         tenant_hash=\"other\", so /metrics cardinality never grows with tenant count:\n{body_off}"
    );
    // Even folded, the admission family itself must be present and populated.
    assert!(
        body_off.contains("ravel_admission_admitted_total{"),
        "the admission family must render even when folded:\n{body_off}"
    );

    // --- Flag on: each tenant keeps its own hash; all reasons distinguishable. ---
    let running = start_admission_server(true).await;
    let base = format!("http://{}", running.http_addr);
    drive_admission_activity(&base).await;
    let body_on = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text");
    running.shutdown().await.expect("graceful shutdown");

    let hashes_on = admission_tenant_hashes(&body_on);
    assert_eq!(
        hashes_on.len(),
        tenant_count,
        "with --metrics-tenant-labels on, exactly {tenant_count} distinct tenant hashes must \
         appear (one per active tenant), got {hashes_on:?}:\n{body_on}"
    );
    assert_eq!(
        hashes_on, expected_hashes,
        "the rendered hashes must be exactly the active tenants' real hashes, not \"other\":\n\
         {body_on}"
    );
    assert!(
        !hashes_on.contains("other"),
        "no tenant may fold to \"other\" with labels on:\n{body_on}"
    );

    // Each rejection reason is exercised and distinguishable by its own series.
    for reason in expected_reasons {
        assert!(
            body_on.contains(&format!("reason=\"{reason}\"}}"))
                && body_on
                    .lines()
                    .any(|l| l.starts_with("ravel_admission_rejected_total")
                        && l.contains(&format!("reason=\"{reason}\""))
                        && l.rsplit(' ').next().is_some_and(|v| v != "0")),
            "rejection reason {reason:?} must appear with a nonzero count:\n{body_on}"
        );
    }
}
