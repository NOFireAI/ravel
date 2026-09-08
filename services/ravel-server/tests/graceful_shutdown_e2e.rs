//! Graceful shutdown drains buffered ingest, marks the process not-ready before
//! listeners close, and deletes the distributed-query heartbeat record on the
//! way out (issue #1291, server half).
//!
//! These drive a real in-process server over real sockets. Automatic
//! time-based flushes are disabled (a very long `max_flush_delay`) so the only
//! thing that can flush a buffered record is the shutdown drain itself: an
//! object appearing under the tenant's metrics prefix after `shutdown()` proves
//! the drain ran, not a background timer.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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
use ravel_fleet::query_workers::QUERY_WORKERS_PREFIX;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_query::distrib::partition::DistribThresholds;
use ravel_server::config::DistribSettings;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

/// The cluster fragment key the distributed test mints capabilities under.
const FRAGMENT_KEY: [u8; 32] = [0x5au8; 32];

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock after epoch")
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

fn export_request(
    metric_name: &str,
    job: &str,
    value: f64,
    ts_ns: i64,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", job)],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: ts_ns as u64,
                            value: Some(NumberValue::AsDouble(value)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Build a server config with automatic flushes disabled, so only the shutdown
/// drain flushes buffered records. `distrib` gates the ADR-0071 heartbeat. The
/// settle interval defaults to zero (no dead time per shutdown); tests that need
/// the pre-close 503 window set it via [`start_server_configured`].
async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    mode: Mode,
    distrib: Option<DistribSettings>,
) -> ravel_server::Running {
    start_server_configured(store, mode, distrib, |_| {}).await
}

/// [`start_server`] with a hook to tweak the `ServerConfig` before the server
/// starts, for the tests that need a non-default settle interval or a shorter
/// shutdown timeout.
async fn start_server_configured(
    store: Arc<dyn ObjectStoreBackend>,
    mode: Mode,
    distrib: Option<DistribSettings>,
    tweak: impl FnOnce(&mut ServerConfig),
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let mut config = ServerConfig {
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        // Long enough that no time-based flush ever fires during a test: the
        // shutdown drain is the only thing that can flush the buffered record.
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        min_flush_bytes: 1024 * 1024 * 1024,
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
        store_probe_interval: Duration::from_secs(3600),
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
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        // Zero by default so a suite that shuts a server down on every case does
        // not pay the settle delay each time; the 503-window test overrides it.
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    tweak(&mut config);
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

fn always_distribute_settings() -> DistribSettings {
    DistribSettings {
        fragment_keys: vec![FRAGMENT_KEY],
        max_inflight_fragments: 32,
        thresholds: DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: 8,
        },
        fragment_listener: None,
    }
}

/// The tenant's level-0 metrics data prefix; an object here means a segment was
/// flushed durably.
fn metrics_l0_prefix() -> String {
    format!("t/{}/m/l0/", TenantId::new(TENANT).hash().to_hex())
}

/// A buffered-mode ingest ack is written to the shard buffer and acked before
/// any object hits the store. With time-based flushes disabled, the record is
/// still buffered at shutdown, and the shutdown drain must flush it durably: an
/// l0 metrics object exists after `shutdown()` and did not before.
#[tokio::test]
async fn sigterm_drains_buffered_records_in_gateway_mode() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server(store.clone(), Mode::Gateway, None).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let request = export_request("drained_metric", "demo", 7.0, now_ns());
    let body = request.encode_to_vec();
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        // Buffered mode: ack at enqueue, before any durable write.
        .header("x-ravel-ingest-mode", "buffered")
        .body(body)
        .send()
        .await
        .expect("buffered export request succeeds");
    assert_eq!(response.status(), 200, "buffered export should be accepted");

    // Nothing durable yet: the record is buffered, and time-based flushes are
    // disabled, so the store holds no l0 metrics object.
    let prefix = metrics_l0_prefix();
    let before = list_all(store.as_ref(), &prefix)
        .await
        .expect("list before shutdown");
    assert!(
        before.is_empty(),
        "the buffered record must not be durable before shutdown, found: {before:?}"
    );

    // The drain must flush it.
    running.shutdown().await.expect("graceful shutdown");

    let after = list_all(store.as_ref(), &prefix)
        .await
        .expect("list after shutdown");
    assert_eq!(
        after.len(),
        1,
        "the shutdown drain must flush the buffered record to exactly one l0 object, found: {after:?}"
    );
}

/// Readiness flips to 503 before the first listener closes: a probe issued
/// concurrently with `shutdown()` observes an HTTP 503 response (the listener
/// was still open and routing) rather than only 200s followed by a connection
/// error. This is what lets Kubernetes stop routing new traffic to the pod
/// before the sockets actually close.
#[tokio::test]
async fn readyz_is_503_before_the_first_listener_closes() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    // This test relies on the pre-close settle window to observe 503 over a
    // still-open listener, so it sets the interval to 500ms explicitly; the
    // other tests keep the zero default.
    let running = start_server_configured(store.clone(), Mode::All, None, |config| {
        config.drain_settle_interval = Duration::from_millis(500);
    })
    .await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    // Ready before shutdown begins.
    let ready = client
        .get(format!("{base}/readyz"))
        .send()
        .await
        .expect("readyz request completes")
        .status();
    assert_eq!(ready.as_u16(), 200, "must be ready before shutdown");

    // A prober hammering /readyz concurrently with shutdown. It records whether
    // it ever received an actual 503 HTTP response (server open, but draining),
    // as opposed to a post-close connection error.
    let stop = Arc::new(AtomicBool::new(false));
    let saw_503_response = Arc::new(AtomicBool::new(false));
    let prober = {
        let stop = stop.clone();
        let saw_503 = saw_503_response.clone();
        let base = base.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            while !stop.load(Ordering::SeqCst) {
                if let Ok(resp) = client.get(format!("{base}/readyz")).send().await
                    && resp.status().as_u16() == 503
                {
                    saw_503.store(true, Ordering::SeqCst);
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    };

    // Shutdown flips readiness to draining and waits the settle interval before
    // any listener closes, so the prober has a window to observe 503 over an
    // open listener.
    running.shutdown().await.expect("graceful shutdown");
    stop.store(true, Ordering::SeqCst);
    prober.await.expect("prober joins");

    assert!(
        saw_503_response.load(Ordering::SeqCst),
        "a probe concurrent with shutdown must observe a 503 response before the listener closes"
    );
}

/// A distributed-query process writes its `sys/query/workers/<uuid>` heartbeat
/// record; graceful shutdown must delete it so sibling coordinators drop it from
/// their live set immediately rather than dialing a stopped worker until its
/// stamp ages out.
#[tokio::test]
async fn heartbeat_worker_record_is_deleted_on_shutdown() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server(store.clone(), Mode::All, Some(always_distribute_settings())).await;

    // Wait for the heartbeat's first write to land (it writes before its first
    // sleep, so this converges in well under a second).
    let mut present = false;
    for _ in 0..200 {
        let records = list_all(store.as_ref(), QUERY_WORKERS_PREFIX)
            .await
            .expect("list worker records");
        if !records.is_empty() {
            present = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        present,
        "the distributed-query process must publish a heartbeat record before shutdown"
    );

    running.shutdown().await.expect("graceful shutdown");

    let after = list_all(store.as_ref(), QUERY_WORKERS_PREFIX)
        .await
        .expect("list worker records after shutdown");
    assert!(
        after.is_empty(),
        "shutdown must delete this process's heartbeat record, found: {after:?}"
    );
}

/// The durable flush must run BEFORE the listener join, so a connection held
/// open past `--shutdown-timeout` cannot cost buffered records: a query can run
/// to its own wall deadline, which the shipped defaults put ABOVE
/// `--shutdown-timeout`. A record is buffered, then a second request is left
/// in-flight on a raw socket so the HTTP listener cannot close; with a short
/// shutdown timeout the listener join is abandoned at its sub-budget, yet the
/// record is already durable and shutdown still returns.
///
/// Reverting the ordering (flush after the join, as before this change) fails
/// this test: the stuck connection consumes the whole budget, the drain times
/// out before the flush, and the buffered record is lost.
#[tokio::test]
async fn buffered_record_is_flushed_before_a_stuck_listener_join() {
    use tokio::io::AsyncWriteExt;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server_configured(store.clone(), Mode::Gateway, None, |config| {
        // Short enough that the abandoned listener join is quick; the stuck
        // connection below outlives it, so the join can never complete.
        config.shutdown_timeout = Duration::from_secs(2);
    })
    .await;
    let http_addr = running.http_addr;
    let base = format!("http://{http_addr}");
    let client = reqwest::Client::new();

    // Buffer a record (acked at enqueue, nothing durable yet).
    let request = export_request("drained_metric", "demo", 7.0, now_ns());
    let body = request.encode_to_vec();
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .header("x-ravel-ingest-mode", "buffered")
        .body(body)
        .send()
        .await
        .expect("buffered export request succeeds");
    assert_eq!(response.status(), 200, "buffered export should be accepted");

    let prefix = metrics_l0_prefix();
    assert!(
        list_all(store.as_ref(), &prefix)
            .await
            .expect("list before shutdown")
            .is_empty(),
        "the buffered record must not be durable before shutdown"
    );

    // Open a raw connection and send a request whose body never completes, so
    // the HTTP listener has an in-flight request and cannot close during the
    // graceful drain.
    let mut stuck = tokio::net::TcpStream::connect(http_addr)
        .await
        .expect("connect a raw socket to the http listener");
    let partial = format!(
        "POST /v1/metrics HTTP/1.1\r\nHost: {http_addr}\r\nauthorization: Bearer {TOKEN}\r\n\
         content-type: application/x-protobuf\r\nx-ravel-ingest-mode: buffered\r\n\
         content-length: 100000\r\n\r\n"
    );
    stuck
        .write_all(partial.as_bytes())
        .await
        .expect("send request headers");
    // A few body bytes, far short of the declared content-length, so the
    // handler stays parked awaiting the rest.
    stuck
        .write_all(&[0u8; 8])
        .await
        .expect("send a partial body");
    stuck.flush().await.expect("flush the partial request");

    // The flush must persist the buffered record before the listener join is
    // abandoned at its sub-budget, and shutdown must still return.
    running.shutdown().await.expect("graceful shutdown returns");

    let after = list_all(store.as_ref(), &prefix)
        .await
        .expect("list after shutdown");
    assert_eq!(
        after.len(),
        1,
        "the drain must flush the buffered record even with a listener held open past the join \
         budget, found: {after:?}"
    );

    // Hold the stuck socket open until after the assertions.
    drop(stuck);
}

/// The ADR-0071 heartbeat record must be deleted BEFORE the listeners close, so
/// a sibling coordinator drops this worker from its live set while the fragment
/// listener is still up, instead of routing a fragment to a socket about to
/// disappear mid-join. With a non-zero settle interval the delete (run
/// concurrently with the settle wait) lands while the listeners are still open:
/// a probe issued mid-shutdown observes the record already gone AND a listener
/// still serving.
#[tokio::test]
async fn heartbeat_record_is_deleted_while_a_listener_still_serves() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server_configured(
        store.clone(),
        Mode::All,
        Some(always_distribute_settings()),
        |config| config.drain_settle_interval = Duration::from_millis(500),
    )
    .await;
    let base = format!("http://{}", running.http_addr);

    // Wait for the heartbeat's first write.
    let mut present = false;
    for _ in 0..200 {
        if !list_all(store.as_ref(), QUERY_WORKERS_PREFIX)
            .await
            .expect("list worker records")
            .is_empty()
        {
            present = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(present, "the heartbeat must publish before shutdown");

    // Drive shutdown from a background task so the assertion can run mid-drain.
    let shutdown = tokio::spawn(async move { running.shutdown().await });

    // During the settle window observe the record already gone while a listener
    // (`/healthz` liveness, independent of drain) still accepts. Empty implies
    // deleted: the record was present above, and only shutdown removes it.
    let probe = reqwest::Client::new();
    let mut observed = false;
    for _ in 0..200 {
        let records = list_all(store.as_ref(), QUERY_WORKERS_PREFIX)
            .await
            .expect("list worker records mid-shutdown");
        if records.is_empty()
            && let Ok(resp) = probe.get(format!("{base}/healthz")).send().await
            && resp.status().as_u16() == 200
        {
            observed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    shutdown
        .await
        .expect("shutdown task joins")
        .expect("graceful shutdown returns");

    assert!(
        observed,
        "the heartbeat record must be deleted while a listener is still serving"
    );
}
