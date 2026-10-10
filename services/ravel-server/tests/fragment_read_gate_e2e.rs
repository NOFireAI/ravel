//! ADR-1702 decision 4 reachability: the distributed fragment worker that
//! [`ravel_server::start`] mounts decodes its slices on the server's read CPU
//! gate.
//!
//! `FragmentService::with_read_gate` is optional, so the unit test in
//! `distrib.rs`, which attaches its own gate, says nothing about the shipped
//! wiring. This test starts a real server with `--distributed-query` on, sends
//! one resolve-scope fragment request over a real tonic channel, and reads the
//! read gate's `segment_section` counters back from `/metrics`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "support/fragment_tls.rs"]
mod fragment_tls;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::queryfrag::v1 as pb;
use ravel_query::distrib::partition::DistribThresholds;
use ravel_query::distrib::proto::series_fetch_client::SeriesFetchClient;
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::config::DistribSettings;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const METRIC: &str = "m";
/// The cluster fragment key the server is configured with. Unused by the
/// resolve-scope request this test drives, but `DistribSettings` requires one.
const FRAGMENT_KEY: [u8; 32] = [0x5au8; 32];
const SQL_TICKET_KEY: [u8; 32] = [0x5c; 32];

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 60 * NS_PER_MIN;

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("now fits i64")
}

/// Publish one real RSEG segment plus its commit record for `tenant`, anchored
/// at `base_ns`. Mirrors the helper in `fragment_engine_config_e2e.rs`: a
/// resolve over a window covering it opens the segment and decodes its
/// catalog.
async fn publish_segment(store: &dyn ObjectStoreBackend, tenant: &TenantId, base_ns: i64) {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, METRIC, &label_set).expect("series id"),
        labels: label_set,
        samples: vec![
            Sample {
                ts_ns: base_ns,
                value: 1.0,
            },
            Sample {
                ts_ns: base_ns + NS_PER_MIN,
                value: 2.5,
            },
        ],
    }];

    let writer_id = uuid::Uuid::from_u128(4_100);
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
        created_unix_ns: base_ns + 2 * NS_PER_MIN,
        ingest_hour_bucket: u32::try_from(base_ns / NS_PER_HOUR).expect("hour bucket fits u32"),
    })
    .expect("valid commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// `--distributed-query` settings with the dedicated fragment listener, so the
/// `SeriesFetch` service is mounted on the public gRPC listener in the
/// `PublicFederation` role and serves the resolve scope this test drives.
fn distrib_settings() -> DistribSettings {
    DistribSettings {
        fragment_keys: vec![FRAGMENT_KEY],
        sql_ticket_keys: Some(vec![SQL_TICKET_KEY]),
        max_inflight_fragments: 8,
        max_inflight_federated_resolves: 8,
        thresholds: DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: 8,
        },
        fragment_listener: fragment_tls::listener_settings(),
        advertise_endpoint: None,
    }
}

async fn start_server(store: Arc<dyn ObjectStoreBackend>) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
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
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        // No read cache, so no startup warmup decodes on the read gate and
        // the slice's catalog decode is the only one this test counts.
        disable_cache: true,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: ravel_server::config::CpuGatePermits { read: 2, write: 1 },
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: Some(distrib_settings()),
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: std::time::Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
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

/// A resolve-scope fragment request over the window holding the published
/// segment. Resolve scope takes the tenant from the presented credential, so
/// no capability minting is needed, and runs its slice through the same
/// `FragmentService::run_slice` fetcher every other fragment scope does.
fn fragment_request(
    tenant: &TenantId,
    window_start_ns: i64,
    window_end_ns: i64,
) -> pb::FetchRequest {
    pb::FetchRequest {
        protocol_version: ravel_query::distrib::codec::PROTOCOL_VERSION,
        tenant_hash: tenant.hash().0.to_vec(),
        signal: ravel_query::distrib::codec::signal_to_u32(Signal::Metrics),
        window_start_ns,
        window_end_ns,
        scope: Some(pb::fetch_request::Scope::Resolve(pb::ResolveScope {
            min_commit_token: Vec::new(),
        })),
        // `0` is the wire's "no cap" sentinel on every budget.
        budgets: Some(pb::Budgets {
            max_series: 0,
            max_samples: 0,
            max_bytes_scanned: 0,
            max_segments: 0,
        }),
        ..Default::default()
    }
}

/// Dispatch `request` to `grpc_addr`'s `SeriesFetch` surface over a real tonic
/// channel with the tenant bearer credential, and return the slice's summary
/// frame (every slice ends with exactly one).
async fn fetch_summary(grpc_addr: std::net::SocketAddr, request: pb::FetchRequest) -> pb::Summary {
    let mut client = SeriesFetchClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("connect to the fragment surface");
    let mut req = tonic::Request::new(request);
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}")
            .parse()
            .expect("valid header value"),
    );
    let mut stream = client
        .fetch(req)
        .await
        .expect("the tenant credential is accepted on the resolve scope")
        .into_inner();

    let mut summary = None;
    while let Some(frame) = stream.message().await.expect("frame arrives") {
        if let Some(pb::fetch_response::Frame::Summary(s)) = frame.frame {
            assert!(
                summary.replace(s).is_none(),
                "a slice emits exactly one summary frame"
            );
        }
    }
    summary.expect("the slice ends with a summary frame")
}

/// The read gate's `(jobs, inline)` counts for `site` on `/metrics`, each
/// required to render exactly once.
async fn read_gate_site(running: &ravel_server::Running, site: &str) -> (u64, u64) {
    let body = reqwest::Client::new()
        .get(format!("http://{}/metrics", running.http_addr))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text");
    let figure = |family: &str| {
        let prefix =
            format!("ravel_cpu_gate_{family}{{mode=\"all\",gate=\"read\",site=\"{site}\"}} ");
        let samples: Vec<&str> = body
            .lines()
            .filter_map(|line| line.strip_prefix(prefix.as_str()))
            .collect();
        assert_eq!(samples.len(), 1, "{prefix:?} must render once:\n{body}");
        samples[0].parse::<u64>().expect("counter value")
    };
    (figure("jobs_total"), figure("inline_total"))
}

/// The fragment worker `start` mounts decodes the one segment in the slice's
/// window on the server's read gate: one `segment_section` run, inline under
/// the 256 KiB byte floor since the fixture catalog is a few hundred bytes.
///
/// Mutation proof: delete `.with_read_gate(cpu_gates.read.clone())` on the
/// `FragmentService` in `services/ravel-server/src/lib.rs` and the slice still
/// returns its series, but `segment_section` reads `(0, 0)` after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn start_wires_the_read_gate_into_the_fragment_service() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    let now = now_ns();
    publish_segment(store.as_ref(), &tenant, now - 10 * NS_PER_MIN).await;

    let running = start_server(Arc::clone(&store)).await;
    assert_eq!(
        read_gate_site(&running, "segment_section").await,
        (0, 0),
        "nothing has decoded a segment catalog before the fragment request"
    );

    let grpc = running.grpc_addr.expect("gRPC listener binds in All mode");
    let summary = fetch_summary(grpc, fragment_request(&tenant, now - NS_PER_HOUR, now)).await;
    let status = summary.status.expect("summary carries a status");
    assert_eq!(
        pb::status::Code::try_from(status.code).expect("known status code"),
        pb::status::Code::Ok,
        "the slice is served (message: {})",
        status.message
    );
    assert_eq!(
        summary.series_returned, 1,
        "the published series is in scope for this window"
    );

    assert_eq!(
        read_gate_site(&running, "segment_section").await,
        (0, 1),
        "the slice's one catalog decode was counted at the server's read gate"
    );

    running.shutdown().await.expect("graceful shutdown");
}
