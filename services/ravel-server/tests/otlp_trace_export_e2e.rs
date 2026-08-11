//! Reachability proof for `--otlp-trace-endpoint` on the real `ravel-server`
//! binary (ADR-0060 deliverable 6b, decision 3). The crate-level tests in
//! `ravel-tracing-export` prove the export mechanism in isolation; this proves
//! the flag actually changes the shipping server's behavior: a started server
//! whose subscriber was installed with an OTLP endpoint exports the
//! `sql_query` request-level span (ADR-0044 section 5) that a real
//! `POST /api/v1/sql` opens.
//!
//! `sql_query` lives behind the `sql` feature, so the whole file is gated to
//! it; the `cargo test -p ravel-server --features sql` gate lane runs it. The
//! mock OTLP collector implements the same `TraceService` trait
//! `services/ravel-server/src/otlp_grpc_traces.rs` implements for ingest.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::{Cli, FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use tonic::{Request, Response, Status};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Small so `Catalog::resolve` issues few LISTs, matching `sql_endpoint.rs`.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

type Received = Arc<parking_lot::Mutex<Vec<ExportTraceServiceRequest>>>;

struct MockCollector {
    received: Received,
}

#[tonic::async_trait]
impl TraceService for MockCollector {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.received.lock().push(request.into_inner());
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

/// The mock collector on its own OS thread and runtime, so the test thread may
/// block in the exporter's synchronous flush without starving it.
struct MockHandle {
    addr: SocketAddr,
    received: Received,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl MockHandle {
    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    fn span_names(&self) -> Vec<String> {
        self.received
            .lock()
            .iter()
            .flat_map(|req| &req.resource_spans)
            .flat_map(|rs| &rs.scope_spans)
            .flat_map(|ss| &ss.spans)
            .map(|s| s.name.clone())
            .collect()
    }
}

impl Drop for MockHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn start_mock_collector() -> MockHandle {
    let received: Received = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let received_for_server = Arc::clone(&received);
    let (addr_tx, addr_rx) = std::sync::mpsc::channel::<SocketAddr>();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("mock runtime");
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind mock");
            let addr = listener.local_addr().expect("mock addr");
            addr_tx.send(addr).expect("report addr");
            tonic::transport::Server::builder()
                .add_service(TraceServiceServer::new(MockCollector {
                    received: received_for_server,
                }))
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
                .expect("serve mock");
        });
    });

    let addr = addr_rx.recv().expect("mock reports addr");
    MockHandle {
        addr,
        received,
        shutdown: Some(shutdown_tx),
        thread: Some(thread),
    }
}

fn wait_until(mut f: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    f()
}

/// One real segment plus its commit record for `TENANT`, so the query returns
/// a clean 200 (mirrors `sql_endpoint.rs::publish_segment`).
async fn publish_segment(store: &dyn ObjectStoreBackend, metric: &str, samples: &[(i64, f64)]) {
    let tenant = TenantId::new(TENANT);
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(&tenant, metric, &label_set).expect("series id"),
        labels: label_set,
        samples: samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }];
    let writer_id = Uuid::from_u128(2_000);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let written = SegmentWriter::write(
        series,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

fn server_config(tokens: HashMap<String, TenantId>) -> ServerConfig {
    ServerConfig {
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
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
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    }
}

/// The whole point of this file: with `--otlp-trace-endpoint` set, a real
/// `POST /api/v1/sql` against a started server ships its `sql_query` span to
/// the collector. Multi-thread runtime so the server tasks, the (own-thread)
/// mock, and the blocking flush all make progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_query_span_is_exported_to_the_configured_collector() {
    let mock = start_mock_collector();

    // Parse a real Cli carrying the flag, proving the CLI surface (deliverable
    // 4) reaches `cli.otlp_trace_endpoint`, then derive the export config
    // through `Cli::otlp_export_config` -- the same method `main.rs` calls,
    // not a second, independently-written derivation that could drift from
    // it (this test previously copied the expression verbatim; a change to
    // `main.rs`'s derivation could not have failed it).
    let cli = Cli::try_parse_from([
        "ravel-server",
        "--mode",
        "all",
        "--otlp-trace-endpoint",
        &mock.endpoint(),
    ])
    .expect("cli parses --otlp-trace-endpoint");
    assert_eq!(
        cli.otlp_trace_endpoint.as_deref(),
        Some(mock.endpoint().as_str())
    );

    let otlp_config = cli.otlp_export_config();
    assert!(
        otlp_config.is_some(),
        "the flag must yield an export config"
    );

    // Install the process-global subscriber before any span is emitted.
    let guard = ravel_tracing_export::init(EnvFilter::new("info"), otlp_config);

    // A started server with a metric segment to query.
    let store = Arc::new(MemoryStore::new());
    publish_segment(store.as_ref(), "m", &[(100, 1.0), (200, 2.5)]).await;
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let running = ravel_server::start(
        server_config(tokens),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");

    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{base}/api/v1/sql"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "query": "SELECT ts, value FROM samples ORDER BY ts",
                "start": 0.0,
                "end": NOW_NS as f64 / 1_000_000_000.0,
            })
            .to_string(),
        )
        .send()
        .await
        .expect("sql request sent");
    assert_eq!(response.status(), 200, "the sql query should succeed");

    // Flush the exporter (the same call `main`'s shutdown makes), off the
    // runtime, then confirm the started server's `sql_query` span arrived.
    tokio::task::spawn_blocking(move || guard.flush())
        .await
        .expect("flush join");
    assert!(
        wait_until(
            || mock.span_names().iter().any(|n| n == "sql_query"),
            Duration::from_secs(5),
        ),
        "the collector never received the sql_query span; got {:?}",
        mock.span_names()
    );

    running.shutdown().await.expect("graceful shutdown");
}
