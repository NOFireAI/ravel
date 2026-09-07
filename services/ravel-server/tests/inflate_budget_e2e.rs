//! End-to-end coverage for issue #1297: the OTLP HTTP gzip inflate path
//! charges the process-wide ingest byte budget (`--max-ingest-buffer-bytes`,
//! ADR-0069) for the bytes it inflates, *before* it finishes inflating, so a
//! decompression that would cross the ceiling is shed with HTTP 429 rather than
//! allocated in full. Before this fix the inflate (up to
//! `MAX_DECOMPRESSED_OTLP_BODY_BYTES`, 64 MiB) sat entirely outside the budget:
//! `--max-inflight-ingest-requests` copies of it could exist at once, 64 GiB at
//! the default, and the flag that claims to bound ingest memory did not.
//!
//! Both tests drive real HTTP against a full server. The concurrency test holds
//! the first request mid-flush with a `FaultStore` hold gate (the same
//! discipline as `ingest_concurrency_e2e.rs`) so its budget charge stays held
//! while the second request is admitted or shed.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::GzEncoder;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use prost::Message;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_server::IngestByteBudgetLimit;
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// A metrics export of `points` copies of one gauge point on a single series,
/// so the encoded protobuf compresses well: the decompressed size is many times
/// the gzip size, which is what makes the inflate the thing the budget must
/// bound.
fn compressible_request(points: usize) -> ExportMetricsServiceRequest {
    let ts = now_ns() as u64;
    let data_points = (0..points)
        .map(|_| NumberDataPoint {
            time_unix_nano: ts,
            value: Some(NumberValue::AsDouble(1.0)),
            ..Default::default()
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "requests_total".to_string(),
                    data: Some(MetricData::Gauge(Gauge { data_points })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// One-member gzip of `data`.
fn gzip(data: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data).expect("gzip write");
    encoder.finish().expect("gzip finish")
}

/// Starts a full server against `store`, with the process-wide ingest buffer
/// byte budget set to `budget` and the in-flight ceiling disabled (so only the
/// byte budget can shed here). One shard, so every write lands on the same
/// shard actor and the same flush.
async fn start_test_server(
    store: Arc<dyn ObjectStoreBackend>,
    budget: IngestByteBudgetLimit,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
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
        limits: LimitsConfig::default(),
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
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        ingest_buffer_budget_limit: budget,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        ingest_concurrency_limit: IngestConcurrencyLimit::Unlimited,
    };
    ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// Scrape `/metrics` and return the `u64` value of the sample line that begins
/// with `prefix` (name plus its rendered label set), or 0 if absent.
async fn metric_value(client: &reqwest::Client, base: &str, prefix: &str) -> u64 {
    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text");
    body.lines()
        .find(|line| line.starts_with(prefix))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Poll `/metrics` until at least `want` metric points have been admitted into
/// shard buffers. A buffered point has already passed decode and the router's
/// budget charge and is parked awaiting its (held) flush, so its charge is
/// held. Returns the last value read.
async fn wait_for_buffered_items(client: &reqwest::Client, base: &str, want: u64) -> u64 {
    let mut seen = 0;
    for _ in 0..1_000 {
        let body = client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("metrics scrape completes")
            .text()
            .await
            .expect("metrics body is text");
        seen = body
            .lines()
            .find(|line| {
                line.starts_with("ravel_ingest_buffered_items_total")
                    && line.contains("signal=\"metrics\"")
            })
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        if seen >= want {
            return seen;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    seen
}

/// Releases every held call on `gate` repeatedly until every task in `tasks`
/// has finished. The held flush's PUT is only armed once the flush fires, so a
/// single release pass can race a later flush; keep draining.
async fn drain_until_done<T>(gate: &GateHandle, tasks: &[tokio::task::JoinHandle<T>]) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for id in gate.held() {
                gate.release(id);
            }
            if tasks.iter().all(|t| t.is_finished()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("held requests drain within 10s");
}

fn post_gzip(client: &reqwest::Client, base: &str, body: Vec<u8>) -> reqwest::RequestBuilder {
    client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip")
        .body(body)
}

/// A gzip body whose inflate crosses the ceiling is shed with 429 + Retry-After
/// *before* it finishes inflating, and the buffer-budget shed counter reads
/// exactly 1. The body is a zeros bomb: it inflates well past the 1 MiB ceiling
/// but is not valid OTLP, so if the charge did NOT happen during inflate the
/// bytes would inflate in full and then fail protobuf decode with 400. Asserting
/// 429 therefore proves the budget was charged as the bytes inflated, before
/// decode ran at all.
///
/// Non-vacuity: revert the per-chunk `budget.try_charge` in
/// `decompress_gzip_capped_charged` and the bomb inflates fully and fails
/// decode, so this returns 400 instead of 429.
#[tokio::test]
async fn gzip_body_is_charged_against_the_budget_before_it_inflates() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestByteBudgetLimit::Bounded(1024 * 1024)).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    // 8 MiB of zeros compresses ~1000:1 and inflates far past the 1 MiB ceiling.
    let bomb_plain = vec![0u8; 8 * 1024 * 1024];
    let compressed = gzip(&bomb_plain);
    assert!(
        compressed.len() < 1024 * 1024,
        "the compressed bomb must stay small: {}",
        compressed.len()
    );

    let response = post_gzip(&client, &base, compressed)
        .send()
        .await
        .expect("shed request still gets an HTTP response");

    assert_eq!(
        response.status(),
        429,
        "a gzip inflate over the buffer budget must be shed with 429 before it inflates"
    );
    assert_eq!(
        response
            .headers()
            .get("retry-after")
            .expect("a shed response carries Retry-After")
            .to_str()
            .expect("ascii header value"),
        "1",
    );
    assert!(
        response.headers().get("x-ravel-commit-token").is_none(),
        "a shed request buffered nothing and carries no commit token"
    );

    let shed = metric_value(
        &client,
        &base,
        "ravel_ingest_buffer_shed_total{mode=\"all\"}",
    )
    .await;
    assert_eq!(shed, 1, "exactly one request shed by the buffer budget");

    running.shutdown().await.expect("graceful shutdown");
}

/// With the first request held mid-flush, a second concurrent gzip request is
/// shed with 429 because the two requests' summed charges cross the ceiling,
/// while the first still completes 200 once released and the same second body
/// succeeds on its own afterwards (proving it only shed because of the sum).
///
/// The in-flight byte gauge is read while the first request is held and its
/// gzip decode charge is isolated exactly: the same body sent uncompressed
/// (identity) holds only the router's buffered charge, so the gzip request's
/// held bytes minus the identity request's held bytes equal the decompressed
/// body length to the byte.
///
/// Non-vacuity: revert the per-chunk `budget.try_charge` and the gzip request
/// holds no decode charge, so `gzip_held - identity_held` is 0 rather than the
/// inflated length, and the isolation assertion fails.
#[tokio::test]
async fn second_concurrent_inflate_sheds_when_the_sum_would_cross_the_ceiling() {
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), Default::default()));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    // 3 MiB ceiling: one request's charge (~2.1 MiB below) fits, two do not.
    let running = start_test_server(store, IngestByteBudgetLimit::Bounded(3 * 1024 * 1024)).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let request = compressible_request(20_000);
    let encoded = request.encode_to_vec();
    let inflated_len = encoded.len() as u64;
    let compressed = gzip(&encoded);
    assert!(
        compressed.len() < encoded.len(),
        "fixture must compress: compressed={} decompressed={inflated_len}",
        compressed.len()
    );

    let data_key_prefix = format!("t/{}/m/l0/", TenantId::new(TENANT).hash().to_hex());

    // --- Control: the same body, uncompressed. The identity path takes no
    // decode charge, so the held in-flight bytes are the router's buffered
    // charge alone. ---
    let gate = fault.hold(Op::Put, Some(data_key_prefix.clone()), Occurrence::Always);
    let identity_body = encoded.clone();
    let identity_task = {
        let client = client.clone();
        let base = base.clone();
        tokio::spawn(async move {
            client
                .post(format!("{base}/v1/metrics"))
                .header("authorization", format!("Bearer {TOKEN}"))
                .header("content-type", "application/x-protobuf")
                .body(identity_body)
                .send()
                .await
                .expect("identity export completes once released")
        })
    };
    // `buffered_items_total` is a cumulative point count; one request buffers
    // all 20_000 of its points, and the counter never decreases.
    assert!(
        wait_for_buffered_items(&client, &base, 20_000).await >= 20_000,
        "the identity request must buffer its points and hold its charge"
    );
    let identity_held =
        metric_value(&client, &base, "ravel_ingest_buffer_bytes{mode=\"all\"}").await;
    assert!(
        identity_held > 0,
        "the identity request holds a router buffered charge"
    );
    drain_until_done(&gate, std::slice::from_ref(&identity_task)).await;
    assert_eq!(
        identity_task.await.expect("task joins").status(),
        200,
        "the control request succeeds once released"
    );
    // Wait for the flush to drain so the budget returns to baseline.
    for _ in 0..1_000 {
        if metric_value(&client, &base, "ravel_ingest_buffer_bytes{mode=\"all\"}").await == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // --- Test: the gzip body, held. Its held bytes are the decode charge
    // (inflated length) plus the same router buffered charge as the control. ---
    let gate = fault.hold(Op::Put, Some(data_key_prefix), Occurrence::Always);
    let gzip_body = compressed.clone();
    let held_task = {
        let client = client.clone();
        let base = base.clone();
        tokio::spawn(async move {
            post_gzip(&client, &base, gzip_body)
                .send()
                .await
                .expect("held gzip export completes once released")
        })
    };
    // The gzip request adds its own 20_000 points on top of the control's.
    assert!(
        wait_for_buffered_items(&client, &base, 40_000).await >= 40_000,
        "the gzip request must buffer its points and hold its charge"
    );
    assert!(
        !held_task.is_finished(),
        "the first gzip request must still be in flight, blocked on the held flush"
    );
    let gzip_held = metric_value(&client, &base, "ravel_ingest_buffer_bytes{mode=\"all\"}").await;
    assert_eq!(
        gzip_held - identity_held,
        inflated_len,
        "the gzip request's held bytes exceed the identity request's by exactly the \
         decompressed body length (the transient decode charge)"
    );

    let shed_before = metric_value(
        &client,
        &base,
        "ravel_ingest_buffer_shed_total{mode=\"all\"}",
    )
    .await;

    // The second concurrent gzip request: its charge summed with the held
    // first request's crosses the 3 MiB ceiling, so it is shed.
    let second = post_gzip(&client, &base, compressed.clone())
        .send()
        .await
        .expect("second request still gets an HTTP response");
    assert_eq!(
        second.status(),
        429,
        "the second concurrent request is shed once the sum crosses the ceiling"
    );

    let shed_after = metric_value(
        &client,
        &base,
        "ravel_ingest_buffer_shed_total{mode=\"all\"}",
    )
    .await;
    assert_eq!(
        shed_after - shed_before,
        1,
        "exactly one additional request shed by the buffer budget"
    );

    // Release the first: it completes 200, proving nothing about it failed.
    drain_until_done(&gate, std::slice::from_ref(&held_task)).await;
    assert_eq!(
        held_task.await.expect("task joins").status(),
        200,
        "the first request succeeds once released"
    );
    // Drain to baseline again.
    for _ in 0..1_000 {
        if metric_value(&client, &base, "ravel_ingest_buffer_bytes{mode=\"all\"}").await == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // The same body succeeds on its own: it only shed because of the sum. The
    // hold gate is still armed (Occurrence::Always), so drive it through the
    // drain the same way, then assert it was admitted (200), not shed (429).
    let alone_body = compressed.clone();
    let alone_task = {
        let client = client.clone();
        let base = base.clone();
        tokio::spawn(async move {
            post_gzip(&client, &base, alone_body)
                .send()
                .await
                .expect("solo request completes")
        })
    };
    drain_until_done(&gate, std::slice::from_ref(&alone_task)).await;
    assert_eq!(
        alone_task.await.expect("task joins").status(),
        200,
        "the second body fits on its own, so it only shed because of the concurrent sum"
    );

    running.shutdown().await.expect("graceful shutdown");
}
