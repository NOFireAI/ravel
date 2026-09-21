//! Issue #1687 part A, reachability: the worker-side self-clamp is wired by the
//! shipping entry point, not just by hand-built services in unit tests.
//!
//! `FragmentService::with_engine_config` is what makes a worker refuse under its
//! OWN configured `max_bytes_scanned` instead of honouring whatever budget a
//! coordinator puts on the wire. The unit tests in `distrib.rs` call that
//! builder themselves, so they prove the clamp's logic and nothing about the
//! binary: delete the one `.with_engine_config(engine_config)` call in
//! `ravel_server::start` and every one of them still passes while the shipped
//! process falls back to `EngineConfig::default()`, whose `max_bytes_scanned` is
//! `Unlimited`.
//!
//! This test closes that gap from the outside. Two real `ravel_server::start`ed
//! servers run over the same `MemoryStore` holding one real RSEG segment, both
//! with `--distributed-query` on, differing only in their configured
//! `max_bytes_scanned`. The same fragment request, carrying a wire budget a
//! billion times larger than either, goes over a real `tonic` channel to the
//! `SeriesFetch` surface each process mounts on its gRPC listener:
//!
//! - the 1-byte server must refuse with `BudgetExceeded` and render the
//!   `TooManyBytesScanned` message against ITS OWN limit of 1, never the wire's;
//! - the `Unlimited` server must serve the same request, which proves the
//!   refusal is the configured budget and not the request, the data, or the
//!   fragment surface being broken.

#![allow(clippy::expect_used, clippy::unwrap_used)]

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
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, QueryLimits, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const METRIC: &str = "m";
/// The cluster fragment key both servers are configured with. Unused by the
/// resolve-scope request this test drives (federation authenticates with an
/// ordinary tenant credential), but `DistribSettings` requires one.
const FRAGMENT_KEY: [u8; 32] = [0x5au8; 32];

/// The budget the coordinator puts on the wire: far larger than anything the
/// segment costs, so a worker that honours the wire value serves the request.
const WIRE_BYTE_BUDGET: u64 = 1_000_000_000;
/// The budget the refusing server is configured with, smaller than any real
/// segment, so the first completed fetch already exceeds it.
const WORKER_BYTE_BUDGET: u64 = 1;

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
/// at `base_ns`. Mirrors the helper in `query_byte_budget_e2e.rs` and
/// `distributed_query_e2e.rs`: a resolve over a window covering it opens the
/// segment and issues a genuine object-store GET whose bytes are charged
/// against whichever budget the worker runs the slice under.
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

/// `--distributed-query` settings with no dedicated fragment listener, so the
/// `SeriesFetch` service is mounted on the public gRPC listener in the
/// pre-amendment `Combined` role and serves the resolve scope this test drives.
fn distrib_settings() -> DistribSettings {
    DistribSettings {
        fragment_keys: vec![FRAGMENT_KEY],
        max_inflight_fragments: 8,
        max_inflight_federated_resolves: 8,
        thresholds: DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: 8,
        },
        fragment_listener: None,
        advertise_endpoint: None,
    }
}

async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    max_bytes_scanned: ravel_query::ByteLimit,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
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
        // The one knob under test: `start` resolves this into the process
        // `EngineConfig` the fragment service must clamp every slice to.
        limits: LimitsConfig {
            query_defaults: QueryLimits { max_bytes_scanned },
            ..LimitsConfig::default()
        },
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
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
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
/// segment, carrying a wire budget far above anything the segment costs.
///
/// Resolve scope is the federation half of the fragment surface: the tenant
/// comes from the presented credential, so no capability minting is needed, and
/// it runs the same `slice_byte_limit` clamp against the worker's
/// `EngineConfig` that every other fragment scope does.
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
        // `0` is the wire's "no cap" sentinel on the count budgets; only the
        // byte budget is set, and it is deliberately enormous.
        budgets: Some(pb::Budgets {
            max_series: 0,
            max_samples: 0,
            max_bytes_scanned: WIRE_BYTE_BUDGET,
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

/// `ravel_server::start` threads the process's resolved `EngineConfig` into the
/// `FragmentService` it mounts, so a worker refuses a fragment slice under its
/// own configured `max_bytes_scanned` rather than the coordinator's wire budget.
///
/// Mutation proof: delete `.with_engine_config(engine_config)` in
/// `services/ravel-server/src/lib.rs` and the budgeted server's status is `Ok`
/// with one series returned, because the service falls back to
/// `EngineConfig::default()`'s `Unlimited` and honours `WIRE_BYTE_BUDGET`
/// verbatim.
#[tokio::test]
async fn start_wires_the_worker_engine_config_into_the_fragment_service() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    let now = now_ns();
    publish_segment(store.as_ref(), &tenant, now - 10 * NS_PER_MIN).await;
    let (window_start, window_end) = (now - NS_PER_HOUR, now);

    // The control: an identically configured process whose only difference is
    // an unlimited byte budget. It must serve the very same request.
    let unlimited = start_server(Arc::clone(&store), ravel_query::ByteLimit::Unlimited).await;
    let unlimited_grpc = unlimited
        .grpc_addr
        .expect("gRPC listener binds in All mode");
    let served = fetch_summary(
        unlimited_grpc,
        fragment_request(&tenant, window_start, window_end),
    )
    .await;
    let served_status = served.status.expect("summary carries a status");
    assert_eq!(
        pb::status::Code::try_from(served_status.code).expect("known status code"),
        pb::status::Code::Ok,
        "control: an unlimited worker serves this request, so any refusal below \
         is the configured budget and not the request, the data, or the \
         fragment surface (message: {})",
        served_status.message
    );
    assert_eq!(
        served.series_returned, 1,
        "control: the published series really is in scope for this window"
    );

    // The subject: the same request against a worker configured with a 1-byte
    // budget, a billion times smaller than the wire budget it is handed.
    let budgeted = start_server(
        Arc::clone(&store),
        ravel_query::ByteLimit::Bounded(WORKER_BYTE_BUDGET),
    )
    .await;
    let budgeted_grpc = budgeted.grpc_addr.expect("gRPC listener binds in All mode");
    let refused = fetch_summary(
        budgeted_grpc,
        fragment_request(&tenant, window_start, window_end),
    )
    .await;
    let refused_status = refused.status.expect("summary carries a status");

    assert_eq!(
        pb::status::Code::try_from(refused_status.code).expect("known status code"),
        pb::status::Code::BudgetExceeded,
        "a worker started with max_bytes_scanned={WORKER_BYTE_BUDGET} must refuse \
         the slice even though the wire budget is {WIRE_BYTE_BUDGET}; getting Ok \
         here means `start` never handed its EngineConfig to the FragmentService \
         (message: {})",
        refused_status.message
    );
    // The message names the budget actually enforced. Pinning the trailing
    // literal is what separates the worker's own limit from the wire's: a
    // worker honouring the wire value would render
    // "exceeding the budget of 1000000000".
    assert!(
        refused_status
            .message
            .ends_with(&format!("exceeding the budget of {WORKER_BYTE_BUDGET}")),
        "the refusal must be rendered against the worker's OWN limit, got: {}",
        refused_status.message
    );
    assert!(
        refused_status.message.starts_with("query scanned "),
        "the refusal is the typed TooManyBytesScanned message, got: {}",
        refused_status.message
    );
    assert_eq!(
        refused.series_returned, 0,
        "a refused slice returns no series"
    );

    budgeted.shutdown().await.expect("budgeted shuts down");
    unlimited.shutdown().await.expect("unlimited shuts down");
}
