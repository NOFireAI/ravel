//! Acceptance coverage for the ADR-0071 distributed read fan-out wiring in
//! `ravel-server` (issue #865).
//!
//! Two properties are proven against real `ravel_server::start`ed servers over
//! a shared `MemoryStore`, so the whole CLI -> config -> engine -> HTTP chain
//! runs, not just an in-memory struct:
//!
//! 1. `distributed_query_http_equals_local_http`: a query served by a
//!    `--distributed-query` process (its cost gate forced open with
//!    zero thresholds, so every query fans out) returns a byte-identical
//!    `data` payload to the same query on a local-only process, and the
//!    distributed path is observable through the `ravel_distrib_*` metrics.
//!    The routing table starts empty (the first worker heartbeat is 60s out),
//!    so the coordinator maps every slice to itself and runs it locally with
//!    no network hop -- exactly the "self-mapped slices run locally" path,
//!    which ADR-0071 requires to be byte-identical to non-distributed
//!    execution. The `slices_local_total > 0` assertion is what flips if the
//!    fan-out is silently skipped; the `data` equality is what flips if the
//!    distributed path diverges from local.
//!
//! 2. `fragment_surface_requires_token_and_flag`: the internal `SeriesFetch`
//!    gRPC surface rejects a missing or wrong bearer token with
//!    `Unauthenticated`, accepts the configured cluster token, and is absent
//!    entirely (`Unimplemented`) on a process started without
//!    `--distributed-query`.
//!
//! 3. `fragment_admits_while_client_cap_saturated_no_deadlock`: with the
//!    server's client-query cap (`QueryAdmissionController`, size 1) held by
//!    a genuinely in-flight `/api/v1/query_range` request, an inbound
//!    fragment still admits and completes, because fragment admission is a
//!    distinct workload class (`--max-inflight-fragments`) and never draws
//!    from the client cap. This is the ADR-0071 deliverable-2 no-deadlock
//!    property driven through the real wiring, not a semaphore unit test.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_proto::queryfrag::v1 as pb;
use ravel_query::distrib::partition::DistribThresholds;
use ravel_query::distrib::proto::series_fetch_client::SeriesFetchClient;
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::config::DistribSettings;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const METRIC: &str = "m";
const FRAGMENT_TOKEN: &str = "cluster-internal-secret";

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
/// at `base_ns`. Mirrors `query_byte_budget_e2e.rs`'s helper so a query over a
/// window covering it resolves the snapshot, opens the segment, and (on the
/// distributed server) produces at least one slice to fan out.
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
        // Space the two samples three minutes apart so each is the winning
        // (latest-at-or-before) sample at some 60s grid step: the earlier
        // sample wins at steps in [base, base+3min), the later one from
        // base+3min on. Samples packed within one step (the earlier bug) made
        // every grid step resolve to the later sample, so a per-sample
        // divergence in the distributed decode path was unobservable and the
        // acceptance equality could not detect it.
        samples: vec![
            Sample {
                ts_ns: base_ns,
                value: 1.0,
            },
            Sample {
                ts_ns: base_ns + 3 * NS_PER_MIN,
                value: 2.5,
            },
        ],
    }];

    let writer_id = uuid::Uuid::from_u128(4_000);
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
        created_unix_ns: base_ns + 4 * NS_PER_MIN,
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

/// Zero thresholds force the cost gate open: every query with any in-scope
/// segment distributes. `max_parallel_slices` is clamped `>= 1` by the engine.
fn always_distribute_settings() -> DistribSettings {
    DistribSettings {
        auth_token: FRAGMENT_TOKEN.to_string(),
        max_inflight_fragments: 32,
        thresholds: DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: 8,
        },
    }
}

async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    distrib: Option<DistribSettings>,
) -> ravel_server::Running {
    start_server_with_query_cap(
        store,
        distrib,
        ravel_query::QueryConcurrencyLimit::Unlimited,
    )
    .await
}

async fn start_server_with_query_cap(
    store: Arc<dyn ObjectStoreBackend>,
    distrib: Option<DistribSettings>,
    query_concurrency_limit: ravel_query::QueryConcurrencyLimit,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
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
        limits: ravel_server::LimitsConfig::default(),
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

async fn query_range(base: &str, start: i64, end: i64) -> serde_json::Value {
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{base}/api/v1/query_range"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", METRIC.to_string()),
            ("start", start.to_string()),
            ("end", end.to_string()),
            ("step", "60s".to_string()),
        ])
        .send()
        .await
        .expect("query request completes");
    assert_eq!(
        response.status(),
        200,
        "query_range must succeed on both servers"
    );
    response.json().await.expect("response body is JSON")
}

async fn scrape_metrics(base: &str) -> String {
    reqwest::Client::new()
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// A `--distributed-query` process returns a byte-identical `data` payload to a
/// local-only process, and its distributed path is visible in the metrics.
#[tokio::test]
async fn distributed_query_http_equals_local_http() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    let now = now_ns();
    publish_segment(store.as_ref(), &tenant, now - 10 * NS_PER_MIN).await;
    let (start, end) = ((now - 15 * NS_PER_MIN) / NS_PER_SEC, now / NS_PER_SEC);

    // Server A distributes every query; server B is local-only. Same store,
    // same query.
    let distributed = start_server(Arc::clone(&store), Some(always_distribute_settings())).await;
    let local = start_server(Arc::clone(&store), None).await;

    let distributed_base = format!("http://{}", distributed.http_addr);
    let local_base = format!("http://{}", local.http_addr);

    let distributed_body = query_range(&distributed_base, start, end).await;
    let local_body = query_range(&local_base, start, end).await;

    assert_eq!(
        distributed_body["status"], "success",
        "distributed query envelope: {distributed_body}"
    );
    // The core ADR-0071 guarantee: fanned-out results are byte-identical to
    // local. This equality is what flips if the distributed path diverges.
    assert_eq!(
        distributed_body["data"], local_body["data"],
        "distributed `data` must be byte-identical to local:\n  distributed={distributed_body}\n  local={local_body}"
    );
    // Sanity: the query actually returned the published series, so the equality
    // above is not the trivial equality of two empty results.
    assert!(
        !distributed_body["data"]["result"]
            .as_array()
            .expect("result is an array")
            .is_empty(),
        "the query must return the published series, not an empty result: {distributed_body}"
    );

    // The distributed path must be observable. `slices_local_total > 0` is what
    // flips if the cost gate silently declined to fan out or the fetcher was
    // never wired.
    let metrics = scrape_metrics(&distributed_base).await;
    let local_slices = metric_value(&metrics, "ravel_distrib_slices_local_total");
    assert!(
        local_slices > 0.0,
        "the distributed server must record at least one locally-run slice; \
         got {local_slices}:\n{metrics}"
    );
    assert!(
        metrics.contains("ravel_distrib_fragment_requests_total"),
        "the distrib metric family must render on a --distributed-query server:\n{metrics}"
    );

    // Every ravel_distrib_ series carries only the allowlisted {mode} label
    // (plus {le} on histogram buckets): ADR-0044 forbids per-shard, per-worker,
    // or per-tenant labels on this family.
    for line in metrics.lines() {
        if !line.starts_with("ravel_distrib_") {
            continue;
        }
        if let Some((_, rest)) = line.split_once('{') {
            let labels = rest.split_once('}').map(|(l, _)| l).unwrap_or("");
            for pair in labels.split(',').filter(|p| !p.is_empty()) {
                let key = pair.split('=').next().unwrap_or(pair);
                assert!(
                    key == "mode" || key == "le",
                    "disallowed label `{key}` on a ravel_distrib series: {line}"
                );
            }
        }
    }

    // A local-only server must not render the family at all.
    let local_metrics = scrape_metrics(&local_base).await;
    assert!(
        !local_metrics.contains("ravel_distrib_"),
        "a local-only server must not render the distrib metric family:\n{local_metrics}"
    );

    distributed.shutdown().await.expect("A shuts down");
    local.shutdown().await.expect("B shuts down");
}

/// Read a `# TYPE`-style counter value for `name` (any labels) from a metrics
/// scrape. Returns the first matching sample, or 0.0 if the name is absent.
fn metric_value(metrics: &str, name: &str) -> f64 {
    for line in metrics.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some(rest) = line.strip_prefix(name) else {
            continue;
        };
        // The next char must end the metric name: `{` (labels) or ` ` (none).
        if !rest.starts_with('{') && !rest.starts_with(' ') {
            continue;
        }
        if let Some(value) = line.rsplit(' ').next()
            && let Ok(v) = value.parse::<f64>()
        {
            return v;
        }
    }
    0.0
}

/// The internal `SeriesFetch` surface is guarded by the cluster bearer token
/// and only exists under `--distributed-query`.
#[tokio::test]
async fn fragment_surface_requires_token_and_flag() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    // Server A: --distributed-query on, guarded by FRAGMENT_TOKEN.
    let distributed = start_server(Arc::clone(&store), Some(always_distribute_settings())).await;
    let grpc_addr = distributed
        .grpc_addr
        .expect("gRPC listener binds in All mode");
    let endpoint = format!("http://{grpc_addr}");

    let fetch = |token: Option<&'static str>| {
        let endpoint = endpoint.clone();
        async move {
            let mut client = SeriesFetchClient::connect(endpoint)
                .await
                .expect("connect to fragment surface");
            let mut request = tonic::Request::new(pb::FetchRequest::default());
            if let Some(token) = token {
                request.metadata_mut().insert(
                    "authorization",
                    format!("Bearer {token}").parse().expect("valid metadata"),
                );
            }
            client.fetch(request).await
        }
    };

    // No token: rejected.
    let missing = fetch(None).await;
    assert_eq!(
        missing.expect_err("no token must be rejected").code(),
        tonic::Code::Unauthenticated,
        "a fragment request with no bearer token must be Unauthenticated"
    );

    // Wrong token: rejected.
    let wrong = fetch(Some("not-the-cluster-token")).await;
    assert_eq!(
        wrong.expect_err("wrong token must be rejected").code(),
        tonic::Code::Unauthenticated,
        "a fragment request with a wrong bearer token must be Unauthenticated"
    );

    // Correct token: accepted. An empty (scope-less) request resolves to an
    // empty result, so the call returns an OK stream rather than an auth error.
    let ok = fetch(Some(FRAGMENT_TOKEN)).await;
    assert!(
        ok.is_ok(),
        "the configured cluster token must be accepted, got: {:?}",
        ok.err()
    );

    // Server B: no --distributed-query, so the service is not registered at all.
    let local = start_server(Arc::clone(&store), None).await;
    let local_grpc = local.grpc_addr.expect("gRPC listener binds in All mode");
    let mut client = SeriesFetchClient::connect(format!("http://{local_grpc}"))
        .await
        .expect("connect to local server gRPC");
    let mut request = tonic::Request::new(pb::FetchRequest::default());
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {FRAGMENT_TOKEN}")
            .parse()
            .expect("valid metadata"),
    );
    let unregistered = client.fetch(request).await;
    assert_eq!(
        unregistered
            .expect_err("the fragment service must not exist without --distributed-query")
            .code(),
        tonic::Code::Unimplemented,
        "a server without --distributed-query must not expose the fragment surface"
    );

    distributed.shutdown().await.expect("A shuts down");
    local.shutdown().await.expect("B shuts down");
}

/// A slice that rendezvous-maps to another engine's fragment endpoint really
/// travels over the network, and the result is still byte-identical to local.
///
/// Every other test in this repo either starts with an empty routing table (so
/// the coordinator self-maps every slice and runs it locally) or points a slice
/// at an unreachable endpoint (so it falls back to local). Neither proves a
/// slice ever left the process. Here server A runs the real `SeriesFetch` gRPC
/// surface over the shared store, and the coordinator engine's routing table
/// names A as the sole live worker with a `self_id` absent from that table, so
/// every slice rendezvous-maps to A and dispatches over a real `tonic` channel.
/// The `ravel_distrib_slices_remote_total` counter proves the hop fired, and
/// the decoded result equals a local-only engine's byte for byte.
#[tokio::test]
async fn distributed_query_dispatches_a_real_remote_hop() {
    use std::sync::OnceLock;

    use parking_lot::RwLock;
    use ravel_fleet::query_workers::QueryWorkerRecord;
    use ravel_query::distrib::codec;
    use ravel_query::{EngineConfig, QueryEngine};
    use ravel_server::distrib::{
        FragmentAdmission, FragmentMetrics, FragmentService, RoutingSliceFetcher,
    };

    const CACHE_BYTES: u64 = 256 * 1024 * 1024;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    let now = now_ns();
    publish_segment(store.as_ref(), &tenant, now - 10 * NS_PER_MIN).await;
    let (start_ms, end_ms, step_ms) = (
        (now - 15 * NS_PER_MIN) / NS_PER_SEC * 1000,
        now / NS_PER_SEC * 1000,
        60_000,
    );

    // Server A: a real --distributed-query process exposing the SeriesFetch
    // fragment gRPC surface over the shared store.
    let server_a = start_server(Arc::clone(&store), Some(always_distribute_settings())).await;
    let a_grpc = server_a.grpc_addr.expect("gRPC listener binds in All mode");

    // The coordinator's slice fetcher: A is the only live worker, and self is a
    // uuid absent from the worker set, so rendezvous ownership of every unit
    // falls to A (never a self-mapped local shortcut).
    let metrics = Arc::new(FragmentMetrics::new());
    let admission = FragmentAdmission::new(8, metrics.clone());
    let local_catalog =
        ravel_server::query::build_catalog(store.clone(), 1, false, CACHE_BYTES).expect("catalog");
    let clock: Arc<dyn ravel_ingest::Clock> = Arc::new(ravel_ingest::SystemClock);
    let local_service = FragmentService::new(
        FRAGMENT_TOKEN.to_string(),
        admission,
        local_catalog,
        store.clone(),
        None,
        clock,
        metrics.clone(),
    );
    let self_cell = Arc::new(OnceLock::new());
    self_cell
        .set(uuid::Uuid::from_u128(0xF00D))
        .expect("set self id");
    let live = Arc::new(RwLock::new(Arc::new(vec![QueryWorkerRecord {
        process_id: uuid::Uuid::from_u128(0xBEEF).to_string(),
        fragment_endpoint: a_grpc.to_string(),
        protocol_version: codec::PROTOCOL_VERSION,
        started_unix_ns: 0,
    }])));
    let fetcher = Arc::new(RoutingSliceFetcher::new(
        self_cell,
        live,
        FRAGMENT_TOKEN.to_string(),
        local_service,
        metrics.clone(),
    ));
    let distributed = Arc::new(ravel_query::distrib::Distributed::new(
        fetcher,
        always_distribute_settings().thresholds,
    ));

    // The coordinator engine (distributed) and a local-only engine, both over
    // the shared store.
    let coordinator_catalog =
        ravel_server::query::build_catalog(store.clone(), 1, false, CACHE_BYTES).expect("catalog");
    let coordinator = QueryEngine::new(coordinator_catalog, store.clone(), EngineConfig::default())
        .with_distributed(distributed);
    let plain_catalog =
        ravel_server::query::build_catalog(store.clone(), 1, false, CACHE_BYTES).expect("catalog");
    let plain = QueryEngine::new(plain_catalog, store.clone(), EngineConfig::default());

    let tenant_hash = tenant.hash();
    let deadline = EngineConfig::default().deadline;
    let (remote_value, _) = coordinator
        .range_with_stats(
            tenant_hash,
            METRIC,
            start_ms,
            end_ms,
            step_ms,
            &[],
            now,
            deadline,
        )
        .await
        .expect("distributed query over the remote hop succeeds");
    let (local_value, _) = plain
        .range_with_stats(
            tenant_hash,
            METRIC,
            start_ms,
            end_ms,
            step_ms,
            &[],
            now,
            deadline,
        )
        .await
        .expect("local query succeeds");

    assert_eq!(
        remote_value, local_value,
        "a query fanned out over a real network hop must be byte-identical to local"
    );
    assert!(
        metrics.slices_remote_total() > 0,
        "at least one slice must have traveled to the remote worker; \
         remote={}, local={}, fallback={}",
        metrics.slices_remote_total(),
        metrics.slices_local_total(),
        metrics.slices_fallback_total(),
    );
    assert_eq!(
        metrics.slices_fallback_total(),
        0,
        "the remote worker is reachable, so no slice should fall back to local"
    );

    server_a.shutdown().await.expect("A shuts down");
}

/// A store wrapper that can hold non-`sys/` `get` reads shut, so a real
/// client query can be pinned in flight -- admitted, permit held -- for as
/// long as a test needs. `sys/`-prefixed keys (worker heartbeats, probes) and
/// every other operation pass straight through, so the server's background
/// tasks never wedge on the gate.
struct GatedStore {
    inner: MemoryStore,
    armed: std::sync::atomic::AtomicBool,
    blocked: std::sync::atomic::AtomicUsize,
    release: tokio::sync::Notify,
}

impl GatedStore {
    fn new() -> Self {
        Self {
            inner: MemoryStore::new(),
            armed: std::sync::atomic::AtomicBool::new(false),
            blocked: std::sync::atomic::AtomicUsize::new(0),
            release: tokio::sync::Notify::new(),
        }
    }

    fn arm(&self) {
        self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// How many `get` calls are currently parked on the gate.
    fn blocked(&self) -> usize {
        self.blocked.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn release_all(&self) {
        self.armed.store(false, std::sync::atomic::Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for GatedStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if !key.starts_with("sys/") {
            loop {
                use std::sync::atomic::Ordering;
                if !self.armed.load(Ordering::SeqCst) {
                    break;
                }
                // Register the waiter before re-checking, so a release between
                // the check and the await still wakes it.
                let notified = self.release.notified();
                if !self.armed.load(Ordering::SeqCst) {
                    break;
                }
                self.blocked.fetch_add(1, Ordering::SeqCst);
                notified.await;
                self.blocked.fetch_sub(1, Ordering::SeqCst);
            }
        }
        self.inner.get(key, range).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// The ADR-0071 deliverable-2 no-deadlock property, driven through the real
/// server wiring: with the client-query cap (`QueryAdmissionController`,
/// bounded at 1) held by a genuinely in-flight `/api/v1/query_range` request,
/// an inbound fragment still admits and completes, because fragment admission
/// (`--max-inflight-fragments`) is a distinct workload class that never draws
/// from the client cap.
///
/// The saturation is real, not simulated: Q1 admits against the server's own
/// controller and then blocks inside a store read the test holds shut, so its
/// permit stays held; a second client query is rejected 503 (proving the cap
/// is exhausted) before the fragment probe is sent.
///
/// NON-VACUITY: pre-exhaust the fragment class in the production wiring --
/// `FragmentAdmission::new` (services/ravel-server/src/distrib.rs) minting
/// `Semaphore::new(0)` instead of the clamped cap -- and the probe below
/// never admits: the 10s timeout fires and this test fails. The saturation
/// premise is separately pinned by the 503 assertion.
#[tokio::test]
async fn fragment_admits_while_client_cap_saturated_no_deadlock() {
    let gated = Arc::new(GatedStore::new());
    let store: Arc<dyn ObjectStoreBackend> = gated.clone();
    let tenant = TenantId::new(TENANT);
    let now = now_ns();
    publish_segment(store.as_ref(), &tenant, now - 10 * NS_PER_MIN).await;
    let (start, end) = ((now - 15 * NS_PER_MIN) / NS_PER_SEC, now / NS_PER_SEC);

    let server = start_server_with_query_cap(
        Arc::clone(&store),
        Some(always_distribute_settings()),
        ravel_query::QueryConcurrencyLimit::Bounded(1),
    )
    .await;
    let base = format!("http://{}", server.http_addr);
    let grpc = server.grpc_addr.expect("gRPC listener binds in All mode");

    // Q1: a real client query. It admits (taking the only permit) and then
    // parks on the gated store read, holding the permit for the rest of the
    // test.
    gated.arm();
    let q1 = tokio::spawn({
        let base = base.clone();
        async move { query_range(&base, start, end).await }
    });
    let parked = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while gated.blocked() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        parked.is_ok(),
        "the client query must reach the gated store read while holding its permit"
    );

    // Q2: the cap really is saturated -- a second client query is rejected
    // with the admission 503, not queued.
    let q2 = reqwest::Client::new()
        .get(format!("{base}/api/v1/query_range"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", METRIC.to_string()),
            ("start", start.to_string()),
            ("end", end.to_string()),
            ("step", "60s".to_string()),
        ])
        .send()
        .await
        .expect("second query request completes");
    assert_eq!(
        q2.status(),
        503,
        "with the single client permit held, a second client query must be \
         rejected by admission"
    );

    // The fragment probe: admitted against the independent fragment class and
    // served while the client cap is fully held. A shared bound would leave
    // this waiting on the permit Q1 holds.
    let probe = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut client = SeriesFetchClient::connect(format!("http://{grpc}"))
            .await
            .expect("connect to fragment gRPC");
        let mut request = tonic::Request::new(pb::FetchRequest::default());
        request.metadata_mut().insert(
            "authorization",
            format!("Bearer {FRAGMENT_TOKEN}")
                .parse()
                .expect("valid metadata"),
        );
        client.fetch(request).await
    })
    .await;
    assert!(
        probe.is_ok(),
        "a fragment must admit within its own class while the client cap is \
         saturated; a timeout here means the two admission classes share a bound"
    );
    assert!(
        probe.expect("probe completed").is_ok(),
        "the admitted fragment request must be served, not rejected"
    );

    // Release the gate: Q1 completes normally (query_range asserts its 200).
    gated.release_all();
    let body = q1.await.expect("Q1 join");
    assert_eq!(
        body["status"], "success",
        "the parked client query must complete once the store read is released"
    );

    server.shutdown().await.expect("server shuts down");
}
