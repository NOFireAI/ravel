//! Acceptance coverage for the ADR-0071 distributed read fan-out wiring in
//! `ravel-server`.
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
//! 2. `fragment_surface_requires_capability_and_flag`: the internal
//!    `SeriesFetch` gRPC surface refuses a `Pinned` fetch whose fragment
//!    capability is missing or does not verify, counts each refusal under its
//!    reason on `/metrics`, accepts a capability minted under the cluster key,
//!    and is absent entirely (`Unimplemented`) on a process started without
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

#[path = "support/fragment_tls.rs"]
mod fragment_tls;

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
/// The cluster fragment key every distributed test mints and verifies
/// capabilities under (ADR-0071 amendment, decision 2).
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
/// at `base_ns`. Mirrors `query_byte_budget_e2e.rs`'s helper so a query over a
/// window covering it resolves the snapshot, opens the segment, and (on the
/// distributed server) produces at least one slice to fan out.
/// Returns the published commit record's key.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    base_ns: i64,
) -> String {
    publish_segment_seq(store, tenant, base_ns, 1).await
}

/// [`publish_segment`] with the flush sequence number exposed, so one test can
/// land several distinct L0 segments in a single ingest-hour bucket (two
/// segments sharing a `writer_seq` would share a commit key and a data key).
async fn publish_segment_seq(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    base_ns: i64,
    writer_seq: u64,
) -> String {
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
        writer_seq,
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
        writer_seq,
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
    keys::commit_key_for_record(&rec).expect("commit record key")
}

/// Zero thresholds force the cost gate open: every query with any in-scope
/// segment distributes. `max_parallel_slices` is clamped `>= 1` by the engine.
fn always_distribute_settings() -> DistribSettings {
    DistribSettings {
        fragment_keys: vec![FRAGMENT_KEY],
        sql_ticket_keys: Some(vec![SQL_TICKET_KEY]),
        max_inflight_fragments: 32,
        max_inflight_federated_resolves: 8,
        thresholds: DistribThresholds {
            min_store_bytes: 0,
            min_segments: 0,
            max_parallel_slices: 8,
        },
        // The dedicated TLS fragment listener `--distributed-query` requires
        // (ADR-1689 decision 4), on an ephemeral loopback port.
        fragment_listener: fragment_tls::listener_settings(),
        // Loopback, ephemeral-port binds: the advertised endpoints are the
        // bound addresses, so no `--advertise-fragment-endpoint` override.
        advertise_endpoint: None,
    }
}

/// A fixed claim set (one tenant, one query) with a far-future expiry, for
/// capability-authorized fragment probes (ADR-0071 amendment, decision 2). The
/// signal is deliberately non-metrics: only metrics are distributed, so the
/// worker's `build_resolver` short-circuits to an empty resolver before any
/// catalog/store read, letting a probe prove admission without depending on
/// store availability (some probes run against a gated store). Capability
/// verification is signal-agnostic: it only requires the request's signal to
/// equal the claims', which holds by construction here.
fn capability_claims() -> ravel_query::distrib::codec::FragmentClaims {
    use ravel_query::distrib::codec;
    codec::FragmentClaims {
        capability_version: codec::CAPABILITY_VERSION,
        tenant_hash: [0u8; 16],
        signal: codec::signal_to_u32(Signal::Logs),
        query_id: [0u8; 16],
        expires_unix_ns: now_ns() + NS_PER_HOUR,
    }
}

/// A minimal `Pinned`-path fetch request whose wire tenant/signal/query match
/// `claims`, carrying `capability` (which the caller mints, tampers, or omits).
fn request_for(
    claims: &ravel_query::distrib::codec::FragmentClaims,
    capability: Vec<u8>,
) -> pb::FetchRequest {
    pb::FetchRequest {
        protocol_version: ravel_query::distrib::codec::PROTOCOL_VERSION,
        tenant_hash: claims.tenant_hash.to_vec(),
        signal: claims.signal,
        query_id: claims.query_id.to_vec(),
        deadline_unix_ns: claims.expires_unix_ns,
        fragment_capability: capability,
        ..Default::default()
    }
}

/// A fetch request carrying a valid capability minted under [`FRAGMENT_KEY`], so
/// the worker admits and serves it (an empty scope resolves to an empty result).
fn valid_capability_request() -> pb::FetchRequest {
    let claims = capability_claims();
    let capability = ravel_query::distrib::codec::mint_capability(&FRAGMENT_KEY, &claims);
    request_for(&claims, capability)
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
    start_server_with(store, distrib, query_concurrency_limit, 1).await
}

async fn start_server_with(
    store: Arc<dyn ObjectStoreBackend>,
    distrib: Option<DistribSettings>,
    query_concurrency_limit: ravel_query::QueryConcurrencyLimit,
    shard_count: u32,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count,
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
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib,
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
    //
    // `stats.phases` is excluded, and only that field. The per-phase cost split
    // (issue #935) is a diagnostic attribution, not part of the result: a
    // distributed query's segment reads happen inside a remote worker, which
    // returns one pooled `QueryAccountingSnapshot` with no phase breakdown, so
    // the coordinator charges the whole remote fetch to the `scan` phase by
    // construction (`QueryStats::phase_accounting`'s own doc comment states
    // this convention). The local path splits the same reads across `plan`,
    // `probe`, and `scan`. Every other field of `data`, including the pooled
    // `stats.accounting` totals those phases sum to, is still compared
    // byte-for-byte, so a real divergence in the fanned-out result still flips
    // this assertion.
    //
    // The pooled GET totals are part of that comparison: the worker's GET of
    // the pinned segment's own commit record is charged to neither the slice's
    // accounting nor the query's budgets (ADR-0071 pinned-record amendment), so
    // the distributed query's pooled accounting equals the local query's, that
    // one extra GET not included.
    let strip_phases = |body: &serde_json::Value| -> serde_json::Value {
        let mut data = body["data"].clone();
        data["stats"]
            .as_object_mut()
            .expect("stats is an object")
            .remove("phases")
            .expect("stats carries the per-phase cost split");
        data
    };
    assert_eq!(
        strip_phases(&distributed_body),
        strip_phases(&local_body),
        "distributed `data` must be byte-identical to local outside the per-phase cost attribution:\n  distributed={distributed_body}\n  local={local_body}"
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
    // (plus {le} on histogram buckets, and {class} on the fragment admission
    // series split into Pinned/Resolve classes by ADR-0071, issue #1722, and
    // {reason} on the capability reject counter alone, issue #2314):
    // ADR-0044 forbids per-shard, per-worker, or per-tenant labels on this
    // family.
    for line in metrics.lines() {
        if !line.starts_with("ravel_distrib_") {
            continue;
        }
        let capability_reject =
            line.starts_with("ravel_distrib_fragment_capability_rejects_total{");
        if let Some((_, rest)) = line.split_once('{') {
            let labels = rest.split_once('}').map(|(l, _)| l).unwrap_or("");
            for pair in labels.split(',').filter(|p| !p.is_empty()) {
                let key = pair.split('=').next().unwrap_or(pair);
                assert!(
                    key == "mode"
                        || key == "le"
                        || key == "class"
                        || (capability_reject && key == "reason"),
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

/// The internal `SeriesFetch` surface authorizes a `Pinned` fetch only with a
/// valid fragment capability minted under the cluster key (ADR-0071 amendment,
/// decision 2), and only exists under `--distributed-query`.
#[tokio::test]
async fn fragment_surface_requires_capability_and_flag() {
    use ravel_query::distrib::codec;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    // Server A: --distributed-query on, guarded by capabilities under FRAGMENT_KEY.
    let distributed = start_server(Arc::clone(&store), Some(always_distribute_settings())).await;
    let fragment_addr = distributed
        .fragment_addr
        .expect("the dedicated fragment listener binds");

    let fetch = |capability: Vec<u8>| async move {
        let mut client = SeriesFetchClient::new(fragment_tls::dial(fragment_addr).await);
        let claims = capability_claims();
        let request = request_for(&claims, capability);
        client.fetch(tonic::Request::new(request)).await
    };

    // No capability: rejected.
    let missing = fetch(Vec::new()).await;
    assert_eq!(
        missing.expect_err("no capability must be rejected").code(),
        tonic::Code::Unauthenticated,
        "a fragment request with no capability must be Unauthenticated"
    );

    // A capability minted under the WRONG key: its MAC does not verify, rejected.
    let wrong = codec::mint_capability(&[0u8; 32], &capability_claims());
    let wrong = fetch(wrong).await;
    assert_eq!(
        wrong
            .expect_err("wrong-key capability must be rejected")
            .code(),
        tonic::Code::Unauthenticated,
        "a capability minted under a foreign key must be Unauthenticated"
    );

    // A valid capability under the configured key: accepted. An empty
    // (scope-less) request resolves to an empty result, so the call returns an
    // OK stream rather than an auth error.
    let good = codec::mint_capability(&FRAGMENT_KEY, &capability_claims());
    let ok = fetch(good).await;
    assert!(
        ok.is_ok(),
        "a valid capability under the cluster key must be accepted, got: {:?}",
        ok.err()
    );

    // `/metrics` reads the counters the fragment service recorded into: one
    // `missing` and one `bad_mac` reject, every other reason at zero (issue
    // #2314).
    let metrics = scrape_metrics(&format!("http://{}", distributed.http_addr)).await;
    let rejects: Vec<&str> = metrics
        .lines()
        .filter(|l| l.starts_with("ravel_distrib_fragment_capability_rejects_total{"))
        .collect();
    assert_eq!(
        rejects,
        vec![
            "ravel_distrib_fragment_capability_rejects_total{mode=\"all\",reason=\"missing\"} 1",
            "ravel_distrib_fragment_capability_rejects_total{mode=\"all\",reason=\"bad_mac\"} 1",
            "ravel_distrib_fragment_capability_rejects_total{mode=\"all\",reason=\"expired\"} 0",
            "ravel_distrib_fragment_capability_rejects_total{mode=\"all\",reason=\"tenant_mismatch\"} 0",
            "ravel_distrib_fragment_capability_rejects_total{mode=\"all\",reason=\"query_mismatch\"} 0",
        ],
        "each refused capability must reach /metrics under its own reason:\n{metrics}"
    );

    // Server B: no --distributed-query, so the service is not registered at all.
    let local = start_server(Arc::clone(&store), None).await;
    let local_grpc = local.grpc_addr.expect("gRPC listener binds in All mode");
    let mut client = SeriesFetchClient::connect(format!("http://{local_grpc}"))
        .await
        .expect("connect to local server gRPC");
    let unregistered = client
        .fetch(tonic::Request::new(valid_capability_request()))
        .await;
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
        AdmissionClasses, FragmentMetrics, FragmentService, RoutingSliceFetcher,
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
    let a_fragment = server_a
        .fragment_addr
        .expect("the dedicated fragment listener binds");

    // The coordinator's slice fetcher: A is the only live worker, and self is a
    // uuid absent from the worker set, so rendezvous ownership of every unit
    // falls to A (never a self-mapped local shortcut).
    let metrics = Arc::new(FragmentMetrics::new());
    let admission = AdmissionClasses::new(8, 8, metrics.clone());
    let local_catalog = ravel_server::query::build_catalog(
        store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let clock: Arc<dyn ravel_ingest::Clock> = Arc::new(ravel_ingest::SystemClock);
    let local_service = FragmentService::new(
        Arc::new(vec![FRAGMENT_KEY]),
        Arc::new(ravel_query::http::StaticBearerTokenResolver::new(
            std::collections::HashMap::new(),
        )),
        admission,
        local_catalog,
        store.clone(),
        None,
        clock,
        metrics.clone(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
    );
    let self_cell = Arc::new(OnceLock::new());
    self_cell
        .set(uuid::Uuid::from_u128(0xF00D))
        .expect("set self id");
    let live = Arc::new(RwLock::new(Arc::new(vec![QueryWorkerRecord {
        process_id: uuid::Uuid::from_u128(0xBEEF).to_string(),
        fragment_endpoint: a_fragment.to_string(),
        protocol_version: codec::PROTOCOL_VERSION,
        started_unix_ns: 0,
    }])));
    let fetcher = Arc::new(RoutingSliceFetcher::new(
        self_cell,
        live,
        Arc::new(vec![FRAGMENT_KEY]),
        local_service,
        metrics.clone(),
        fragment_tls::client_tls(),
    ));
    let distributed = Arc::new(ravel_query::distrib::Distributed::new(
        fetcher,
        always_distribute_settings().thresholds,
    ));

    // The coordinator engine (distributed) and a local-only engine, both over
    // the shared store.
    let coordinator_catalog = ravel_server::query::build_catalog(
        store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let coordinator = QueryEngine::new(coordinator_catalog, store.clone(), EngineConfig::default())
        .with_distributed(distributed);
    let plain_catalog = ravel_server::query::build_catalog(
        store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
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
    let fragment = server
        .fragment_addr
        .expect("the dedicated fragment listener binds");

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
        let mut client = SeriesFetchClient::new(fragment_tls::dial(fragment).await);
        let request = valid_capability_request();
        client.fetch(tonic::Request::new(request)).await
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

/// The deliverable-2 no-deadlock property at the tightest fragment bound: the
/// same real-wiring proof as its sibling above, but with
/// `--max-inflight-fragments 1`. A shared bound, or a coordinator that held a
/// fragment permit across its own client work, wedges at cap 1 specifically:
/// with only one fragment permit and the single client permit already held by
/// a parked query, the inbound fragment has nowhere to draw from unless the two
/// classes are genuinely independent. The 32-permit sibling can mask an
/// off-by-one that only bites when the fragment class is saturated down to its
/// last permit.
///
/// NON-VACUITY: identical to the sibling -- mint `Semaphore::new(0)` in
/// `FragmentAdmission::new` and the probe times out. The 503 assertion
/// separately pins that the client cap really is exhausted.
#[tokio::test]
async fn fragment_admits_while_client_cap_saturated_no_deadlock_single_fragment_permit() {
    let gated = Arc::new(GatedStore::new());
    let store: Arc<dyn ObjectStoreBackend> = gated.clone();
    let tenant = TenantId::new(TENANT);
    let now = now_ns();
    publish_segment(store.as_ref(), &tenant, now - 10 * NS_PER_MIN).await;
    let (start, end) = ((now - 15 * NS_PER_MIN) / NS_PER_SEC, now / NS_PER_SEC);

    // One fragment permit, one client permit: both classes saturated to their
    // last slot, so any shared bound deadlocks.
    let mut distrib = always_distribute_settings();
    distrib.max_inflight_fragments = 1;
    let server = start_server_with_query_cap(
        Arc::clone(&store),
        Some(distrib),
        ravel_query::QueryConcurrencyLimit::Bounded(1),
    )
    .await;
    let base = format!("http://{}", server.http_addr);
    let fragment = server
        .fragment_addr
        .expect("the dedicated fragment listener binds");

    // Q1 holds the single client permit, parked on the gated store read.
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

    // The client cap is saturated: a second client query is rejected 503.
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
        "with the single client permit held, a second client query must be rejected"
    );

    // The fragment probe admits against the single-permit fragment class and is
    // served while the client cap is fully held.
    let probe = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let mut client = SeriesFetchClient::new(fragment_tls::dial(fragment).await);
        let request = valid_capability_request();
        client.fetch(tonic::Request::new(request)).await
    })
    .await;
    assert!(
        probe.is_ok(),
        "a fragment must admit within its own single-permit class while the client \
         cap is saturated; a timeout here means the two classes share a bound"
    );
    assert!(
        probe.expect("probe completed").is_ok(),
        "the admitted fragment request must be served, not rejected"
    );

    gated.release_all();
    let body = q1.await.expect("Q1 join");
    assert_eq!(body["status"], "success");

    server.shutdown().await.expect("server shuts down");
}

// ---------------------------------------------------------------------------
// Fault-matrix coverage (ADR-0071 failure semantics).
//
// A configurable in-process mock `SeriesFetch` worker lets a coordinator's
// re-dispatch and slice-atomicity behavior be driven deterministically: each
// mock counts the dispatches it receives and returns a scripted outcome
// (transport loss, an Unavailable summary, a mid-stream death, or a clean
// series), so the exact attempt sequence and what each attempt contributes to
// the merged result are both observable.
// ---------------------------------------------------------------------------

/// The boxed frame stream a mock `SeriesFetch` worker streams back.
type MockStream =
    Pin<Box<dyn futures::Stream<Item = Result<pb::FetchResponse, tonic::Status>> + Send + 'static>>;

/// What a mock worker does with a dispatched slice.
#[derive(Clone)]
enum MockBehavior {
    /// Fail at the gRPC layer, which the coordinator sees as transport loss.
    TransportError,
    /// Stream a single terminal summary carrying an `Unavailable` status.
    UnavailableSummary,
    /// Stream one series frame, then abort the stream before any summary: a
    /// mid-frame death whose partial frame the coordinator must discard whole.
    PartialThenError([u8; 16]),
    /// Stream one clean series frame plus an OK summary.
    OkSeries([u8; 16]),
    /// Stream a single terminal summary carrying a `Corrupt` status: a
    /// worker-reported corruption that must fail typed with no retry and no
    /// local fallback masking it.
    CorruptSummary,
}

/// A mock `SeriesFetch` gRPC worker that counts dispatches and returns a
/// scripted [`MockBehavior`].
struct MockWorker {
    behavior: MockBehavior,
    hits: Arc<AtomicU64>,
}

#[tonic::async_trait]
impl ravel_query::distrib::proto::series_fetch_server::SeriesFetch for MockWorker {
    type FetchStream = MockStream;

    async fn fetch(
        &self,
        _request: tonic::Request<pb::FetchRequest>,
    ) -> Result<tonic::Response<Self::FetchStream>, tonic::Status> {
        self.hits.fetch_add(1, Ordering::SeqCst);
        let frames: Vec<Result<pb::FetchResponse, tonic::Status>> = match &self.behavior {
            MockBehavior::TransportError => {
                return Err(tonic::Status::unavailable(
                    "mock worker: simulated transport loss",
                ));
            }
            MockBehavior::UnavailableSummary => {
                vec![Ok(summary_frame(pb::status::Code::Unavailable))]
            }
            MockBehavior::PartialThenError(series_id) => vec![
                Ok(series_frame(*series_id)),
                Err(tonic::Status::unavailable("mock worker: mid-stream death")),
            ],
            MockBehavior::OkSeries(series_id) => vec![
                Ok(series_frame(*series_id)),
                Ok(summary_frame(pb::status::Code::Ok)),
            ],
            MockBehavior::CorruptSummary => {
                vec![Ok(summary_frame(pb::status::Code::Corrupt))]
            }
        };
        Ok(tonic::Response::new(Box::pin(futures::stream::iter(
            frames,
        ))))
    }
}

/// One decodable scalar series frame carrying `series_id` and a single sample.
fn series_frame(series_id: [u8; 16]) -> pb::FetchResponse {
    pb::FetchResponse {
        frame: Some(pb::fetch_response::Frame::Series(pb::SeriesFrame {
            series_id: series_id.to_vec(),
            labels: vec![pb::Label {
                name: "__name__".to_string(),
                value: METRIC.to_string(),
            }],
            runs: vec![pb::Run {
                created_unix_ns: 0,
                writer_epoch: 1,
                writer_seq: 1,
                ts_delta: vec![0],
                value_bits: vec![1.0f64.to_bits()],
                ..Default::default()
            }],
        })),
    }
}

/// A terminal summary frame with the given typed status and no accounting.
fn summary_frame(code: pb::status::Code) -> pb::FetchResponse {
    pb::FetchResponse {
        frame: Some(pb::fetch_response::Frame::Summary(pb::Summary {
            accounting: None,
            series_returned: 0,
            samples_returned: 0,
            status: Some(pb::Status {
                code: code as i32,
                message: String::new(),
            }),
            raw_f64_pages: 0,
            raw_f64_bytes: 0,
        })),
    }
}

/// Bind a mock `SeriesFetch` worker on loopback, returning its `host:port`
/// endpoint, a live dispatch counter, and a shutdown trigger (kept alive by the
/// caller for the worker's lifetime).
async fn spawn_mock_worker(
    behavior: MockBehavior,
) -> (String, Arc<AtomicU64>, tokio::sync::oneshot::Sender<()>) {
    use ravel_query::distrib::proto::series_fetch_server::SeriesFetchServer;

    let hits = Arc::new(AtomicU64::new(0));
    let worker = MockWorker {
        behavior,
        hits: Arc::clone(&hits),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock worker");
    let addr = listener.local_addr().expect("mock worker local addr");
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        fragment_tls::tls_server()
            .add_service(SeriesFetchServer::new(worker))
            .serve_with_incoming_shutdown(
                tonic::transport::server::TcpIncoming::from(listener),
                async {
                    let _ = rx.await;
                },
            )
            .await
            .expect("mock worker serves");
    });
    (addr.to_string(), hits, tx)
}

/// Rank a pool of candidate worker ids for one rendezvous unit, top owner
/// first, using the same public `worker_set` mapping the router uses. Returns
/// the three highest-ranked ids: the rendezvous primary, its failover, and the
/// third-ranked worker (which a correct EXACTLY-once ladder must never reach).
fn top_three_owners(unit_key: &[u8]) -> (uuid::Uuid, uuid::Uuid, uuid::Uuid) {
    use ravel_fleet::worker_set;
    let mut ids: Vec<uuid::Uuid> = (0..16u128).map(uuid::Uuid::from_u128).collect();
    let mut ranked = Vec::new();
    while let Some(owner) = worker_set::owner(unit_key, &ids) {
        ranked.push(owner);
        ids.retain(|id| *id != owner);
    }
    (ranked[0], ranked[1], ranked[2])
}

/// ADR-0071 deliverable 1: a first remote attempt lost at transport re-dispatches
/// EXACTLY once to the next rendezvous worker; if that attempt is also
/// unavailable the slice runs coordinator-local; and if the local read fails too
/// the query fails typed. The attempt sequence is asserted precisely: mock A
/// (primary) and mock B (next) each receive exactly one dispatch, and the
/// coordinator-local read faults through a `FaultStore` whose counter proves the
/// third attempt fired.
///
/// NON-VACUITY: delete the re-dispatch block in `RoutingSliceFetcher::dispatch`
/// (`services/ravel-server/src/distrib.rs`) -- the `if let Some(Owner::Remote(next))`
/// arm that calls `try_remote` a second time -- so a lost primary falls straight
/// to local (a one-attempt-then-local behavior). Mock B then receives
/// zero dispatches and `assert_eq!(b_hits, 1)` fails.
#[tokio::test]
async fn worker_loss_redispatches_once_then_fails_typed() {
    use std::sync::OnceLock;

    use parking_lot::RwLock;
    use ravel_fleet::query_workers::QueryWorkerRecord;
    use ravel_fleet::worker_set;
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_query::distrib::codec;
    use ravel_query::{EngineConfig, QueryEngine};
    use ravel_server::distrib::{
        AdmissionClasses, FragmentMetrics, FragmentService, RoutingSliceFetcher,
    };

    const CACHE_BYTES: u64 = 256 * 1024 * 1024;

    let tenant = TenantId::new(TENANT);
    let now = now_ns();

    // Publish real data into a plain store, then wrap it so only data-object
    // (.rseg) GETs fault transiently. The coordinator's snapshot resolve (which
    // reads commit records, not .rseg) still succeeds, but the coordinator-local
    // fallback -- the third and final attempt -- fails with a typed store error.
    let mem = MemoryStore::new();
    publish_segment(&mem, &tenant, now - 10 * NS_PER_MIN).await;
    let store_fault = Arc::new(FaultStore::new(
        mem,
        FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("distributed local fallback read faulted".to_string()),
            )
            .with_key_contains(".rseg"),
        ),
    ));
    let store: Arc<dyn ObjectStoreBackend> = store_fault.clone();

    // The single slice's rendezvous unit is fixed (one tenant, metrics, shard 0).
    // Rank a candidate pool for it and make the top owner mock A (transport loss)
    // and its failover mock B (Unavailable summary).
    let tenant_hash = tenant.hash();
    let unit = worker_set::unit_key(&tenant_hash, Signal::Metrics, 0);
    let (a_id, b_id, c_id) = top_three_owners(&unit);
    let (a_endpoint, a_hits, _a_tx) = spawn_mock_worker(MockBehavior::TransportError).await;
    let (b_endpoint, b_hits, _b_tx) = spawn_mock_worker(MockBehavior::UnavailableSummary).await;
    // A third ranked worker distinguishes "re-dispatch EXACTLY once" from
    // "walk the ranked list until it is exhausted": with only two workers the
    // two behaviors are indistinguishable, and an unbounded-ladder mutation
    // survives. C must never be dialed.
    let (c_endpoint, c_hits, _c_tx) = spawn_mock_worker(MockBehavior::UnavailableSummary).await;

    let live = Arc::new(RwLock::new(Arc::new(vec![
        QueryWorkerRecord {
            process_id: a_id.to_string(),
            fragment_endpoint: a_endpoint.clone(),
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        },
        QueryWorkerRecord {
            process_id: b_id.to_string(),
            fragment_endpoint: b_endpoint.clone(),
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        },
        QueryWorkerRecord {
            process_id: c_id.to_string(),
            fragment_endpoint: c_endpoint.clone(),
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        },
    ])));

    let metrics = Arc::new(FragmentMetrics::new());
    let admission = AdmissionClasses::new(8, 8, metrics.clone());
    let local_catalog = ravel_server::query::build_catalog(
        store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let clock: Arc<dyn ravel_ingest::Clock> = Arc::new(ravel_ingest::SystemClock);
    let local_service = FragmentService::new(
        Arc::new(vec![FRAGMENT_KEY]),
        Arc::new(ravel_query::http::StaticBearerTokenResolver::new(
            std::collections::HashMap::new(),
        )),
        admission,
        local_catalog,
        store.clone(),
        None,
        clock,
        metrics.clone(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
    );
    let self_cell = Arc::new(OnceLock::new());
    // A self id absent from the worker set, so every slice maps to a remote.
    self_cell
        .set(uuid::Uuid::from_u128(0xFFFF_FFFF))
        .expect("set self id");
    let fetcher = Arc::new(RoutingSliceFetcher::new(
        self_cell,
        live,
        Arc::new(vec![FRAGMENT_KEY]),
        local_service,
        metrics.clone(),
        fragment_tls::client_tls(),
    ));
    let distributed = Arc::new(ravel_query::distrib::Distributed::new(
        fetcher,
        always_distribute_settings().thresholds,
    ));
    let coordinator_catalog = ravel_server::query::build_catalog(
        store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let coordinator = QueryEngine::new(coordinator_catalog, store.clone(), EngineConfig::default())
        .with_distributed(distributed);

    let (start_ms, end_ms, step_ms) = (
        (now - 15 * NS_PER_MIN) / NS_PER_SEC * 1000,
        now / NS_PER_SEC * 1000,
        60_000,
    );
    let result = coordinator
        .range_with_stats(
            tenant_hash,
            METRIC,
            start_ms,
            end_ms,
            step_ms,
            &[],
            now,
            std::time::Duration::from_secs(60),
        )
        .await;

    assert!(
        result.is_err(),
        "primary, next-worker, and coordinator-local all fail, so the query fails typed: {result:?}"
    );
    assert_eq!(
        a_hits.load(Ordering::SeqCst),
        1,
        "the rendezvous primary A is dispatched exactly once"
    );
    assert_eq!(
        b_hits.load(Ordering::SeqCst),
        1,
        "the next rendezvous worker B receives exactly one re-dispatch"
    );
    assert_eq!(
        c_hits.load(Ordering::SeqCst),
        0,
        "the third-ranked worker C is never dialed: the ladder is exactly \
         primary, one re-dispatch, local -- not the whole ranked list"
    );
    assert_eq!(
        metrics.slices_redispatched_total(),
        1,
        "the slice entered re-dispatch exactly once"
    );
    assert_eq!(
        metrics.slices_remote_total(),
        0,
        "neither remote attempt produced a usable result"
    );
    assert_eq!(
        metrics.slices_fallback_total(),
        1,
        "after both remotes failed, the slice fell back to coordinator-local"
    );
    assert!(
        store_fault.fault_count(Op::Get, FaultKind::Transient) >= 1,
        "the coordinator-local fallback read hit the injected store fault"
    );
}

/// ADR-0071 deliverable 4: a live worker whose
/// `protocol_version` is skewed from the coordinator's receives no slices. The
/// mismatch is caught at routing time, so the skewed worker is never dialed --
/// the query silently runs fully local and is byte-identical to a non-distributed
/// engine, and neither the remote nor the fallback counter moves.
///
/// The skewed worker points at an unreachable TEST-NET endpoint: were it not
/// filtered at routing time, the slice would dial it, time out, and fall back
/// (`slices_fallback_total == 1`). Asserting the fallback counter stays zero is
/// what proves the mismatch cost no round trip.
#[tokio::test]
async fn version_mismatch_falls_back_to_local() {
    use std::sync::OnceLock;

    use parking_lot::RwLock;
    use ravel_fleet::query_workers::QueryWorkerRecord;
    use ravel_query::distrib::codec;
    use ravel_query::{EngineConfig, QueryEngine};
    use ravel_server::distrib::{
        AdmissionClasses, FragmentMetrics, FragmentService, RoutingSliceFetcher,
    };

    const CACHE_BYTES: u64 = 256 * 1024 * 1024;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    let now = now_ns();
    publish_segment(store.as_ref(), &tenant, now - 10 * NS_PER_MIN).await;

    // One live worker, version-skewed, at an unreachable endpoint. Self is
    // absent from the set, so absent the version filter every slice would map to
    // this worker and dial it.
    let metrics = Arc::new(FragmentMetrics::new());
    let admission = AdmissionClasses::new(8, 8, metrics.clone());
    let local_catalog = ravel_server::query::build_catalog(
        store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let clock: Arc<dyn ravel_ingest::Clock> = Arc::new(ravel_ingest::SystemClock);
    let local_service = FragmentService::new(
        Arc::new(vec![FRAGMENT_KEY]),
        Arc::new(ravel_query::http::StaticBearerTokenResolver::new(
            std::collections::HashMap::new(),
        )),
        admission,
        local_catalog,
        store.clone(),
        None,
        clock,
        metrics.clone(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
    );
    let self_cell = Arc::new(OnceLock::new());
    self_cell
        .set(uuid::Uuid::from_u128(0xF00D))
        .expect("set self id");
    let live = Arc::new(RwLock::new(Arc::new(vec![QueryWorkerRecord {
        process_id: uuid::Uuid::from_u128(0xBEEF).to_string(),
        // Reserved TEST-NET address that never accepts a connection.
        fragment_endpoint: "192.0.2.1:9".to_string(),
        protocol_version: codec::PROTOCOL_VERSION + 1,
        started_unix_ns: 0,
    }])));
    let fetcher = Arc::new(RoutingSliceFetcher::new(
        self_cell,
        live,
        Arc::new(vec![FRAGMENT_KEY]),
        local_service,
        metrics.clone(),
        fragment_tls::client_tls(),
    ));
    let distributed = Arc::new(ravel_query::distrib::Distributed::new(
        fetcher,
        always_distribute_settings().thresholds,
    ));
    let coordinator_catalog = ravel_server::query::build_catalog(
        store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let coordinator = QueryEngine::new(coordinator_catalog, store.clone(), EngineConfig::default())
        .with_distributed(distributed);
    let plain_catalog = ravel_server::query::build_catalog(
        store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let plain = QueryEngine::new(plain_catalog, store.clone(), EngineConfig::default());

    let tenant_hash = tenant.hash();
    let (start_ms, end_ms, step_ms) = (
        (now - 15 * NS_PER_MIN) / NS_PER_SEC * 1000,
        now / NS_PER_SEC * 1000,
        60_000,
    );
    let deadline = EngineConfig::default().deadline;
    let (distributed_value, _) = coordinator
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
        .expect("distributed query completes fully local");
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
        .expect("local query completes");

    assert_eq!(
        distributed_value, local_value,
        "a query with only a version-skewed worker must be byte-identical to local"
    );
    assert_eq!(
        metrics.slices_remote_total(),
        0,
        "a version-skewed worker must receive no slices"
    );
    assert_eq!(
        metrics.slices_fallback_total(),
        0,
        "the skew is caught at routing time, so no slice is dialed and falls back"
    );
    assert!(
        metrics.slices_local_total() > 0,
        "every slice ran on the coordinator with no hop"
    );
}

/// ADR-0071 slice atomicity: a slice whose first attempt dies mid-frames
/// contributes nothing from that dead attempt, even when the re-dispatch then
/// succeeds. Mock A streams one series frame then aborts before any summary;
/// mock B (the failover) returns a clean, different series. The merged slice
/// result must contain only B's series -- A's partial frame is discarded whole.
///
/// NON-VACUITY: in `RoutingSliceFetcher::remote_fetch`
/// (`services/ravel-server/src/distrib.rs`), change the frame-collection loop
/// from propagating the stream error (`.map_err(...)?`) to breaking and
/// decoding whatever frames arrived. A's partial `[series frame, no summary]`
/// then decodes to a `NoSummary` error that is treated as terminal, the
/// re-dispatch to B never happens, and the assertions below (status OK, B's
/// series present) fail.
#[tokio::test]
async fn slice_atomicity_discards_partial_frames_from_failed_attempt() {
    use std::sync::OnceLock;

    use parking_lot::RwLock;
    use ravel_fleet::query_workers::QueryWorkerRecord;
    use ravel_fleet::worker_set;
    use ravel_query::distrib::client::SliceFetcher;
    use ravel_query::distrib::codec;
    use ravel_server::distrib::{
        AdmissionClasses, FragmentMetrics, FragmentService, RoutingSliceFetcher,
    };

    const CACHE_BYTES: u64 = 256 * 1024 * 1024;
    const SERIES_A: [u8; 16] = [0xAA; 16];
    const SERIES_B: [u8; 16] = [0xBB; 16];

    // A fixed rendezvous unit; rank a candidate pool for it and make its top
    // owner the mid-death worker A and its failover the clean worker B.
    let tenant_hash = [7u8; 16];
    let unit = worker_set::unit_key(&ravel_types::TenantHash(tenant_hash), Signal::Metrics, 0);
    let (a_id, b_id, _c_id) = top_three_owners(&unit);
    let (a_endpoint, a_hits, _a_tx) =
        spawn_mock_worker(MockBehavior::PartialThenError(SERIES_A)).await;
    let (b_endpoint, b_hits, _b_tx) = spawn_mock_worker(MockBehavior::OkSeries(SERIES_B)).await;

    let live = Arc::new(RwLock::new(Arc::new(vec![
        QueryWorkerRecord {
            process_id: a_id.to_string(),
            fragment_endpoint: a_endpoint,
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        },
        QueryWorkerRecord {
            process_id: b_id.to_string(),
            fragment_endpoint: b_endpoint,
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        },
    ])));

    // The local fallback is never reached (B succeeds), so an empty store is fine.
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let metrics = Arc::new(FragmentMetrics::new());
    let admission = AdmissionClasses::new(8, 8, metrics.clone());
    let local_catalog = ravel_server::query::build_catalog(
        store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let clock: Arc<dyn ravel_ingest::Clock> = Arc::new(ravel_ingest::SystemClock);
    let local_service = FragmentService::new(
        Arc::new(vec![FRAGMENT_KEY]),
        Arc::new(ravel_query::http::StaticBearerTokenResolver::new(
            std::collections::HashMap::new(),
        )),
        admission,
        local_catalog,
        store.clone(),
        None,
        clock,
        metrics.clone(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
    );
    let self_cell = Arc::new(OnceLock::new());
    self_cell
        .set(uuid::Uuid::from_u128(0xFFFF_FFFF))
        .expect("set self id");
    let fetcher = RoutingSliceFetcher::new(
        self_cell,
        live,
        Arc::new(vec![FRAGMENT_KEY]),
        local_service,
        metrics.clone(),
        fragment_tls::client_tls(),
    );

    let request = pb::FetchRequest {
        protocol_version: codec::PROTOCOL_VERSION,
        tenant_hash: tenant_hash.to_vec(),
        signal: codec::signal_to_u32(Signal::Metrics),
        scope: Some(pb::fetch_request::Scope::Pinned(pb::PinnedScope {
            segments: vec![pb::SegmentIdentity {
                shard: 0,
                content_hash: vec![0u8; 32],
                ..Default::default()
            }],
        })),
        ..Default::default()
    };

    let response = fetcher
        .fetch(request)
        .await
        .expect("the re-dispatch to B yields a clean slice");

    assert_eq!(
        response.status,
        pb::status::Code::Ok,
        "the successful retry's OK summary is the slice's terminal status"
    );
    let returned: Vec<[u8; 16]> = response.scalar.iter().map(|s| s.series_id.0).collect();
    assert!(
        returned.contains(&SERIES_B),
        "the clean series from the successful retry is present"
    );
    assert!(
        !returned.contains(&SERIES_A),
        "no partial series from the dead first attempt leaks into the result"
    );
    assert_eq!(
        response.scalar.len(),
        1,
        "only the retry contributed; the dead attempt contributed nothing"
    );
    assert_eq!(a_hits.load(Ordering::SeqCst), 1, "A dispatched once");
    assert_eq!(b_hits.load(Ordering::SeqCst), 1, "B re-dispatched once");
    assert_eq!(metrics.slices_redispatched_total(), 1);
    assert_eq!(metrics.slices_remote_total(), 1, "B's clean result counted");
    assert_eq!(
        metrics.slices_fallback_total(),
        0,
        "local was never reached"
    );
}

/// ADR-0071 deliverable 5: tearing down the coordinator's stream cancels the
/// in-flight fragment on the worker and frees its admission permit. A worker Y
/// admits a dispatched fragment (its `fragment_inflight` gauge reads 1) and then
/// parks on a gated store read while holding the permit; aborting the
/// coordinator drops the client stream, tonic cancels Y's handler, and the
/// permit's `Drop` returns the gauge to zero.
///
/// NON-VACUITY: the gauge is asserted to reach 1 first (the fragment genuinely
/// admitted), so the return-to-zero cannot pass vacuously. Were the permit not
/// released on cancellation, the gauge would stay at 1 and the poll below would
/// time out.
#[tokio::test]
async fn cancelled_distributed_query_frees_fragment_permits() {
    use std::sync::OnceLock;

    use parking_lot::RwLock;
    use ravel_fleet::query_workers::QueryWorkerRecord;
    use ravel_query::distrib::codec;
    use ravel_query::{EngineConfig, QueryEngine};
    use ravel_server::distrib::{
        AdmissionClasses, FragmentMetrics, FragmentService, RoutingSliceFetcher,
    };

    const CACHE_BYTES: u64 = 256 * 1024 * 1024;

    let tenant = TenantId::new(TENANT);
    let now = now_ns();

    // Worker Y over a gated store: an armed gate holds every non-`sys/` read
    // (the fragment's snapshot resolve and data read) shut, so Y admits the
    // fragment -- taking a permit and incrementing the in-flight gauge -- and
    // then parks with the permit held.
    let gated = Arc::new(GatedStore::new());
    let y_store: Arc<dyn ObjectStoreBackend> = gated.clone();
    publish_segment(y_store.as_ref(), &tenant, now - 10 * NS_PER_MIN).await;
    let server_y = start_server(Arc::clone(&y_store), Some(always_distribute_settings())).await;
    let y_fragment = server_y
        .fragment_addr
        .expect("the dedicated fragment listener binds");
    let y_http = format!("http://{}", server_y.http_addr);

    // Coordinator over its own ungated store (same published data), so its own
    // snapshot resolve is not gated: it resolves, dispatches the slice to Y, and
    // waits on Y's stream.
    let coord_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_segment(coord_store.as_ref(), &tenant, now - 10 * NS_PER_MIN).await;

    let metrics = Arc::new(FragmentMetrics::new());
    let admission = AdmissionClasses::new(8, 8, metrics.clone());
    let local_catalog = ravel_server::query::build_catalog(
        coord_store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let clock: Arc<dyn ravel_ingest::Clock> = Arc::new(ravel_ingest::SystemClock);
    let local_service = FragmentService::new(
        Arc::new(vec![FRAGMENT_KEY]),
        Arc::new(ravel_query::http::StaticBearerTokenResolver::new(
            std::collections::HashMap::new(),
        )),
        admission,
        local_catalog,
        coord_store.clone(),
        None,
        clock,
        metrics.clone(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
    );
    let self_cell = Arc::new(OnceLock::new());
    self_cell
        .set(uuid::Uuid::from_u128(0xF00D))
        .expect("set self id");
    let live = Arc::new(RwLock::new(Arc::new(vec![QueryWorkerRecord {
        process_id: uuid::Uuid::from_u128(0xBEEF).to_string(),
        fragment_endpoint: y_fragment.to_string(),
        protocol_version: codec::PROTOCOL_VERSION,
        started_unix_ns: 0,
    }])));
    let fetcher = Arc::new(RoutingSliceFetcher::new(
        self_cell,
        live,
        Arc::new(vec![FRAGMENT_KEY]),
        local_service,
        metrics.clone(),
        fragment_tls::client_tls(),
    ));
    let distributed = Arc::new(ravel_query::distrib::Distributed::new(
        fetcher,
        always_distribute_settings().thresholds,
    ));
    let coordinator_catalog = ravel_server::query::build_catalog(
        coord_store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let coordinator = QueryEngine::new(
        coordinator_catalog,
        coord_store.clone(),
        EngineConfig::default(),
    )
    .with_distributed(distributed);

    let tenant_hash = tenant.hash();
    let (start_ms, end_ms, step_ms) = (
        (now - 15 * NS_PER_MIN) / NS_PER_SEC * 1000,
        now / NS_PER_SEC * 1000,
        60_000,
    );

    gated.arm();
    let query = tokio::spawn(async move {
        coordinator
            .range_with_stats(
                tenant_hash,
                METRIC,
                start_ms,
                end_ms,
                step_ms,
                &[],
                now,
                std::time::Duration::from_secs(60),
            )
            .await
    });

    // The fragment admits on Y and parks: its in-flight gauge reaches 1.
    let admitted = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let m = scrape_metrics(&y_http).await;
            if metric_value(
                &m,
                "ravel_distrib_fragment_inflight{mode=\"all\",class=\"pinned\"}",
            ) >= 1.0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        admitted.is_ok(),
        "the dispatched fragment must admit on the worker and hold a permit \
         (in-flight gauge reaches 1)"
    );

    // Tear down the coordinator's stream: tonic cancels Y's handler.
    query.abort();
    let _ = query.await;

    // The permit's Drop must return the gauge to zero.
    let freed = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            let m = scrape_metrics(&y_http).await;
            if metric_value(
                &m,
                "ravel_distrib_fragment_inflight{mode=\"all\",class=\"pinned\"}",
            ) == 0.0
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        freed.is_ok(),
        "cancelling the coordinator must free the worker's fragment permit; \
         the in-flight gauge must return to zero"
    );

    gated.release_all();
    server_y.shutdown().await.expect("Y shuts down");
}

/// ADR-0071 deliverable 3: a worker-reported corruption is terminal and typed.
/// No retry reaches the failover worker, no coordinator-local fallback masks
/// the corruption behind a possibly-clean local read, and the query fails with
/// the corruption in its error.
///
/// The setup makes any masking observable: real data is published, so a wrong
/// local fallback WOULD succeed, and failover worker B would return a clean
/// series if it were wrongly re-dispatched -- either wrong path flips
/// `result.is_err()` or a counter assertion below.
///
/// NON-VACUITY: in `crates/ravel-query/src/distrib/mod.rs`, route the
/// `pb::status::Code::Corrupt` arm to the `Unavailable` handling (or in
/// `services/ravel-server/src/distrib.rs` `try_remote`, classify a Corrupt
/// summary as `Attempt::Retry`); B then receives the re-dispatch, returns a
/// clean slice, the query succeeds, and the `result.is_err()` and
/// `b_hits == 0` assertions both fail.
#[tokio::test]
async fn corrupt_worker_fails_typed_without_retry_or_fallback() {
    use std::sync::OnceLock;

    use parking_lot::RwLock;
    use ravel_fleet::query_workers::QueryWorkerRecord;
    use ravel_fleet::worker_set;
    use ravel_query::distrib::codec;
    use ravel_query::{EngineConfig, QueryEngine};
    use ravel_server::distrib::{
        AdmissionClasses, FragmentMetrics, FragmentService, RoutingSliceFetcher,
    };

    const CACHE_BYTES: u64 = 256 * 1024 * 1024;

    let tenant = TenantId::new(TENANT);
    let now = now_ns();
    let mem = MemoryStore::new();
    publish_segment(&mem, &tenant, now - 10 * NS_PER_MIN).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(mem);

    // Primary A reports Corrupt; failover B would serve a clean slice if the
    // ladder wrongly continued past the corruption.
    let tenant_hash = tenant.hash();
    let unit = worker_set::unit_key(&tenant_hash, Signal::Metrics, 0);
    let (a_id, b_id, _c_id) = top_three_owners(&unit);
    let (a_endpoint, a_hits, _a_tx) = spawn_mock_worker(MockBehavior::CorruptSummary).await;
    let (b_endpoint, b_hits, _b_tx) = spawn_mock_worker(MockBehavior::OkSeries([0xCC; 16])).await;

    let live = Arc::new(RwLock::new(Arc::new(vec![
        QueryWorkerRecord {
            process_id: a_id.to_string(),
            fragment_endpoint: a_endpoint.clone(),
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        },
        QueryWorkerRecord {
            process_id: b_id.to_string(),
            fragment_endpoint: b_endpoint.clone(),
            protocol_version: codec::PROTOCOL_VERSION,
            started_unix_ns: 0,
        },
    ])));

    let metrics = Arc::new(FragmentMetrics::new());
    let admission = AdmissionClasses::new(8, 8, metrics.clone());
    let local_catalog = ravel_server::query::build_catalog(
        store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let clock: Arc<dyn ravel_ingest::Clock> = Arc::new(ravel_ingest::SystemClock);
    let local_service = FragmentService::new(
        Arc::new(vec![FRAGMENT_KEY]),
        Arc::new(ravel_query::http::StaticBearerTokenResolver::new(
            std::collections::HashMap::new(),
        )),
        admission,
        local_catalog,
        store.clone(),
        None,
        clock,
        metrics.clone(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
    );
    let self_cell = Arc::new(OnceLock::new());
    self_cell
        .set(uuid::Uuid::from_u128(0xFFFF_FFFF))
        .expect("set self id");
    let fetcher = Arc::new(RoutingSliceFetcher::new(
        self_cell,
        live,
        Arc::new(vec![FRAGMENT_KEY]),
        local_service,
        metrics.clone(),
        fragment_tls::client_tls(),
    ));
    let distributed = Arc::new(ravel_query::distrib::Distributed::new(
        fetcher,
        always_distribute_settings().thresholds,
    ));
    let coordinator_catalog = ravel_server::query::build_catalog(
        store.clone(),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        std::time::Duration::from_secs(2),
    )
    .expect("catalog");
    let coordinator = QueryEngine::new(coordinator_catalog, store.clone(), EngineConfig::default())
        .with_distributed(distributed);

    let (start_ms, end_ms, step_ms) = (
        (now - 15 * NS_PER_MIN) / NS_PER_SEC * 1000,
        now / NS_PER_SEC * 1000,
        60_000,
    );
    let result = coordinator
        .range_with_stats(
            tenant_hash,
            METRIC,
            start_ms,
            end_ms,
            step_ms,
            &[],
            now,
            std::time::Duration::from_secs(60),
        )
        .await;

    let err = result.expect_err("a worker-reported corruption fails the query");
    assert!(
        err.to_string().to_lowercase().contains("corrupt"),
        "the typed error names the corruption, got: {err}"
    );
    assert_eq!(
        a_hits.load(Ordering::SeqCst),
        1,
        "the corrupt-reporting primary is dispatched exactly once"
    );
    assert_eq!(
        b_hits.load(Ordering::SeqCst),
        0,
        "corruption is never re-dispatched to the failover worker"
    );
    assert_eq!(
        metrics.slices_redispatched_total(),
        0,
        "the corrupt slice never enters re-dispatch"
    );
    assert_eq!(
        metrics.slices_fallback_total(),
        0,
        "no coordinator-local fallback masks the corruption"
    );
}

/// A worker-side store wrapper that splits what the worker reads into three
/// counters -- commit-record reads (a GET or HEAD of a `.cmt` key), other
/// catalog-shaped requests (any listing, and any read of something that is
/// neither a `.cmt` record nor an `.rseg` data object), and data-object
/// reads -- and can hold
/// the worker's first tenant read shut so a compaction can be committed while
/// a fragment is in flight. `sys/` keys (heartbeats, the store probe, the
/// tenancy marker) are the server's own background traffic and are counted
/// under neither, so the tenant-prefixed counters stay attributable.
struct WorkerProbeStore {
    inner: Arc<dyn ObjectStoreBackend>,
    commit_record_gets: AtomicU64,
    catalog_requests: AtomicU64,
    data_object_gets: AtomicU64,
    armed: std::sync::atomic::AtomicBool,
    blocked: std::sync::atomic::AtomicUsize,
    release: tokio::sync::Notify,
}

impl WorkerProbeStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> Self {
        Self {
            inner,
            commit_record_gets: AtomicU64::new(0),
            catalog_requests: AtomicU64::new(0),
            data_object_gets: AtomicU64::new(0),
            armed: std::sync::atomic::AtomicBool::new(false),
            blocked: std::sync::atomic::AtomicUsize::new(0),
            release: tokio::sync::Notify::new(),
        }
    }

    fn note_read(&self, key: &str) {
        if key.starts_with("sys/") {
            return;
        }
        if key.ends_with(".rseg") {
            self.data_object_gets.fetch_add(1, Ordering::SeqCst);
        } else if key.ends_with(".cmt") {
            self.commit_record_gets.fetch_add(1, Ordering::SeqCst);
        } else {
            self.catalog_requests.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn note_list(&self, prefix: &str) {
        if !prefix.starts_with("sys/") {
            self.catalog_requests.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn reset_counters(&self) {
        self.commit_record_gets.store(0, Ordering::SeqCst);
        self.catalog_requests.store(0, Ordering::SeqCst);
        self.data_object_gets.store(0, Ordering::SeqCst);
    }

    fn commit_record_gets(&self) -> u64 {
        self.commit_record_gets.load(Ordering::SeqCst)
    }

    fn catalog_requests(&self) -> u64 {
        self.catalog_requests.load(Ordering::SeqCst)
    }

    fn data_object_gets(&self) -> u64 {
        self.data_object_gets.load(Ordering::SeqCst)
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn blocked(&self) -> usize {
        self.blocked.load(Ordering::SeqCst)
    }

    fn release_all(&self) {
        self.armed.store(false, Ordering::SeqCst);
        self.release.notify_waiters();
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for WorkerProbeStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.note_read(key);
        if !key.starts_with("sys/") {
            loop {
                if !self.armed.load(Ordering::SeqCst) {
                    break;
                }
                // `enable()` registers the waiter now rather than at the first
                // poll, so a release between the re-check below and the await
                // still wakes it. This gate is released exactly once; a missed
                // wake would park the fetch forever.
                let notified = self.release.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
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
        self.note_read(key);
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.note_list(prefix);
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.note_list(prefix);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// ADR-0071's reconstruct-don't-trust rule, end to end: a compaction that
/// commits between the coordinator's resolve and the worker's fetch cannot
/// change what the worker reads, and the worker's only catalog-shaped requests
/// while serving the fragment are one GET per pinned L0 segment of that
/// segment's own commit record, at the key reconstructed from the identity.
///
/// The compaction is real: two L0 segments land in one sealed ingest-hour
/// bucket and `compact_bucket` publishes an L1 part plus its compaction record
/// over them, which is exactly the catalog change that makes a re-resolve on
/// the worker return a different segment set from the one the coordinator
/// pinned. It is committed while the fragment is genuinely in flight -- the
/// worker's store holds its first tenant read shut until the compaction record
/// is published.
///
/// NON-VACUITY: the row-equality assertion alone does not pin this. A worker
/// that re-resolves and misses its pinned segments answers
/// `SNAPSHOT_INVALIDATED`, which the coordinator folds into one re-resolve and
/// re-dispatch (`crates/ravel-query/src/distrib/mod.rs`), so the second round
/// succeeds over the newly compacted L1 part and the rows still match. The
/// assertions that flip are the exact worker request counts: exactly two
/// commit-record GETs (one per pinned L0 segment) and zero other catalog
/// requests. A per-request `catalog.resolve` on the worker path lists the
/// bucket, so the zero fails regardless of what the rows say; a worker that
/// trusted the identity without reading the record fails the two.
#[tokio::test]
async fn compaction_between_resolve_and_fetch_returns_local_rows() {
    use std::sync::OnceLock;
    use std::time::Duration;

    use parking_lot::RwLock;
    use ravel_fleet::query_workers::QueryWorkerRecord;
    use ravel_maintain::{Bucket, CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};
    use ravel_query::distrib::codec;
    use ravel_query::{EngineConfig, QueryEngine};
    use ravel_server::distrib::{
        AdmissionClasses, FragmentMetrics, FragmentService, RoutingSliceFetcher,
    };

    const CACHE_BYTES: u64 = 256 * 1024 * 1024;

    // The coordinator and the local oracle read the backing store directly; the
    // worker process reads it through the counting, holdable wrapper, so every
    // counter below is attributable to the worker alone.
    let backing: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let worker_store = Arc::new(WorkerProbeStore::new(Arc::clone(&backing)));
    let worker_backend: Arc<dyn ObjectStoreBackend> = worker_store.clone();

    let tenant = TenantId::new(TENANT);
    let tenant_hash = tenant.hash();
    let now = now_ns();
    // An hour-aligned anchor three hours back: both L0 segments land in one
    // ingest-hour bucket, and that bucket is already sealed at `now`
    // (hour end plus `max_flush_lifetime + clock_skew_allowance`), so the
    // compaction below really compacts instead of returning `NotSealed`.
    let anchor = (now - 3 * NS_PER_HOUR) / NS_PER_HOUR * NS_PER_HOUR;
    publish_segment_seq(backing.as_ref(), &tenant, anchor + 5 * NS_PER_MIN, 1).await;
    publish_segment_seq(backing.as_ref(), &tenant, anchor + 20 * NS_PER_MIN, 2).await;
    let (start_ms, end_ms, step_ms) = (
        anchor / NS_PER_SEC * 1000,
        (anchor + 40 * NS_PER_MIN) / NS_PER_SEC * 1000,
        60_000,
    );

    // The worker: a real --distributed-query process serving `SeriesFetch`.
    let server_a = start_server(
        Arc::clone(&worker_backend),
        Some(always_distribute_settings()),
    )
    .await;
    let a_fragment = server_a
        .fragment_addr
        .expect("the dedicated fragment listener binds");

    let metrics = Arc::new(FragmentMetrics::new());
    let admission = AdmissionClasses::new(8, 8, metrics.clone());
    let fallback_catalog = ravel_server::query::build_catalog(
        Arc::clone(&backing),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        Duration::from_secs(2),
    )
    .expect("catalog");
    let clock: Arc<dyn ravel_ingest::Clock> = Arc::new(ravel_ingest::SystemClock);
    let fallback_service = FragmentService::new(
        Arc::new(vec![FRAGMENT_KEY]),
        Arc::new(ravel_query::http::StaticBearerTokenResolver::new(
            HashMap::new(),
        )),
        admission,
        fallback_catalog,
        Arc::clone(&backing),
        None,
        clock,
        metrics.clone(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
    );
    let self_cell = Arc::new(OnceLock::new());
    self_cell
        .set(uuid::Uuid::from_u128(0xF00D))
        .expect("set self id");
    let live = Arc::new(RwLock::new(Arc::new(vec![QueryWorkerRecord {
        process_id: uuid::Uuid::from_u128(0xBEEF).to_string(),
        fragment_endpoint: a_fragment.to_string(),
        protocol_version: codec::PROTOCOL_VERSION,
        started_unix_ns: 0,
    }])));
    let fetcher = Arc::new(RoutingSliceFetcher::new(
        self_cell,
        live,
        Arc::new(vec![FRAGMENT_KEY]),
        fallback_service,
        metrics.clone(),
        fragment_tls::client_tls(),
    ));
    let distributed = Arc::new(ravel_query::distrib::Distributed::new(
        fetcher,
        always_distribute_settings().thresholds,
    ));
    let coordinator_catalog = ravel_server::query::build_catalog(
        Arc::clone(&backing),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        Duration::from_secs(2),
    )
    .expect("catalog");
    let coordinator = Arc::new(
        QueryEngine::new(
            coordinator_catalog,
            Arc::clone(&backing),
            EngineConfig::default(),
        )
        .with_distributed(distributed),
    );

    // Everything the worker process read while starting up (the store probe,
    // the tenancy marker) is not this test's subject: count from here on.
    worker_store.reset_counters();
    worker_store.arm();

    let deadline = EngineConfig::default().deadline;
    let query = tokio::spawn({
        let coordinator = Arc::clone(&coordinator);
        async move {
            coordinator
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
        }
    });

    let parked = tokio::time::timeout(Duration::from_secs(30), async {
        while worker_store.blocked() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        parked.is_ok(),
        "the worker must reach a held tenant read while the fragment is in flight"
    );

    // The catalog changes under the in-flight fragment: an L1 part plus its
    // compaction record now cover both pinned L0 segments.
    let hour = u32::try_from(anchor / NS_PER_HOUR).expect("hour bucket fits u32");
    let compactor = CompactorConfig {
        compactor_writer_id: uuid::Uuid::from_u128(9_000),
        ..CompactorConfig::default()
    };
    let bucket = Bucket::new(tenant_hash, Signal::Metrics, 0, hour);
    let outcome = compact_bucket(backing.as_ref(), &FixedClock::new(now), &compactor, &bucket)
        .await
        .expect("compacting the sealed bucket succeeds");
    match outcome {
        CompactionOutcome::Compacted { parts, .. } => assert_eq!(
            parts, 1,
            "the two pinned L0 segments compact into exactly one L1 part"
        ),
        other => panic!("the sealed two-segment bucket must compact, got {other:?}"),
    }

    worker_store.release_all();
    let (remote_value, _) = query
        .await
        .expect("the coordinator task joins")
        .expect("the distributed query succeeds across the compaction");

    // The oracle runs after the compaction, over the compacted catalog: the
    // distributed answer must equal local execution regardless of which
    // physical objects each side read.
    let oracle_catalog = ravel_server::query::build_catalog(
        Arc::clone(&backing),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        Duration::from_secs(2),
    )
    .expect("catalog");
    let plain = QueryEngine::new(
        oracle_catalog,
        Arc::clone(&backing),
        EngineConfig::default(),
    );
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
        .expect("the local query succeeds");

    assert_eq!(
        remote_value, local_value,
        "a compaction committed between the coordinator's resolve and the \
         worker's fetch must not change the rows the distributed query returns"
    );
    // Pin the rows themselves, so the equality above cannot pass on two empty
    // results. One series, and the 5-minute lookback carries each of the four
    // samples across the 60s grid: 1.0 from +5m, 2.5 from +8m, then the second
    // segment's 1.0 from +20m and 2.5 from +23m.
    let series = match &remote_value {
        ravel_promql::Value::Matrix(matrix) => matrix,
        other => panic!("a range query returns a matrix, got {}", other.type_name()),
    };
    assert_eq!(
        series.len(),
        1,
        "both segments carry the same single series"
    );
    let points: Vec<(i64, u64)> = series[0]
        .1
        .iter()
        .map(|sample| ((sample.ts_ns - anchor) / NS_PER_MIN, sample.value.to_bits()))
        .collect();
    let expected: Vec<(i64, u64)> = [5, 6, 7]
        .into_iter()
        .map(|m| (m, 1.0f64.to_bits()))
        .chain(
            [8, 9, 10, 11, 12]
                .into_iter()
                .map(|m| (m, 2.5f64.to_bits())),
        )
        .chain([20, 21, 22].into_iter().map(|m| (m, 1.0f64.to_bits())))
        .chain(
            [23, 24, 25, 26, 27]
                .into_iter()
                .map(|m| (m, 2.5f64.to_bits())),
        )
        .collect();
    assert_eq!(
        points, expected,
        "the distributed result is the exact grid the two pinned segments produce"
    );
    assert_eq!(
        worker_store.commit_record_gets(),
        2,
        "the worker GETs each pinned L0 segment's own commit record exactly \
         once, at the key reconstructed from the identity, to verify it"
    );
    assert_eq!(
        worker_store.catalog_requests(),
        0,
        "beyond those two commit-record GETs the worker issues no catalog \
         request (no listing, no compaction record); data-object reads were {}",
        worker_store.data_object_gets(),
    );
    assert_eq!(
        worker_store.data_object_gets(),
        2,
        "the worker reads exactly the two pinned L0 objects, once each, and \
         never the compaction's L1 part"
    );
    assert_eq!(
        metrics.slices_remote_total(),
        1,
        "the single ingest shard maps to exactly one remote slice"
    );
    assert_eq!(
        metrics.slices_local_total(),
        0,
        "no slice is self-mapped: the coordinator's id is absent from the \
         worker set"
    );
    assert_eq!(
        metrics.slices_fallback_total(),
        0,
        "the worker is reachable and answers, so nothing falls back to local"
    );

    server_a.shutdown().await.expect("the worker shuts down");
}

/// A retryable record-GET failure on a SELF-MAPPED slice must still answer the
/// query.
///
/// `RoutingSliceFetcher::dispatch` runs a self-mapped or unroutable slice
/// through `run_local` directly: no remote attempt precedes it and no local
/// fallback follows it. The coordinator, meanwhile, treats an `Unavailable`
/// summary as terminal, because on the remote path it means every attempt --
/// primary, one re-dispatch, and local -- was already spent. A worker-side
/// `Unavailable` raised by the RESOLVE phase on the self-mapped path therefore
/// had nowhere to go and failed the whole query on a single store blip, where
/// the catalog re-resolve it replaced cost one re-resolve and retry.
///
/// The blip is injected on the fragment service's OWN store only: a
/// `FaultStore` wrapping the shared backing store, so the coordinator's
/// catalog resolve and the local oracle read unfaulted objects and only the
/// pinned resolve's record GET faults. `Occurrence::Nth(1)` fires it once, so
/// the retry the fix produces reads the record successfully.
///
/// NON-VACUITY: the fault counter must read exactly 1, so the blip really
/// fired and was really survived; and the rows are pinned to the exact grid
/// the published samples produce, not merely compared against an oracle that
/// could be empty on both sides.
#[tokio::test]
async fn a_retryable_record_get_on_a_self_mapped_slice_still_answers_the_query() {
    use std::sync::OnceLock;
    use std::time::Duration;

    use parking_lot::RwLock;
    use ravel_fleet::query_workers::QueryWorkerRecord;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_query::distrib::codec;
    use ravel_query::{EngineConfig, QueryEngine};
    use ravel_server::distrib::{
        AdmissionClasses, FragmentMetrics, FragmentService, RoutingSliceFetcher,
    };

    const CACHE_BYTES: u64 = 256 * 1024 * 1024;

    let backing: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new(TENANT);
    let tenant_hash = tenant.hash();
    let now = now_ns();
    // Minute-aligned and two hours back, so every grid step lands exactly on a
    // sample offset and the whole window is in the past.
    let anchor = (now - 2 * NS_PER_HOUR) / NS_PER_MIN * NS_PER_MIN;
    publish_segment(backing.as_ref(), &tenant, anchor).await;

    // Only the worker's own record GET faults: commit records end `.cmt`, and
    // the data objects this same store serves end `.rseg`.
    let worker_store = Arc::new(FaultStore::new(
        Arc::clone(&backing),
        FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("record GET throttled".to_string()),
            )
            .with_key_contains(".cmt")
            .with_occurrence(Occurrence::Nth(1)),
        ),
    ));
    let worker_backend: Arc<dyn ObjectStoreBackend> = worker_store.clone();

    let metrics = Arc::new(FragmentMetrics::new());
    let admission = AdmissionClasses::new(8, 8, metrics.clone());
    let local_catalog = ravel_server::query::build_catalog(
        Arc::clone(&backing),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        Duration::from_secs(2),
    )
    .expect("catalog");
    let clock: Arc<dyn ravel_ingest::Clock> = Arc::new(ravel_ingest::SystemClock);
    let local_service = FragmentService::new(
        Arc::new(vec![FRAGMENT_KEY]),
        Arc::new(ravel_query::http::StaticBearerTokenResolver::new(
            HashMap::new(),
        )),
        admission,
        local_catalog,
        worker_backend,
        None,
        clock,
        metrics.clone(),
        Arc::new(ravel_query::GetLimiter::new(8).expect("nonzero permits")),
    );

    // The only live worker IS this coordinator, so the slice's rendezvous owner
    // is `Owner::SelfLocal` and `dispatch` takes the run-local arm.
    let self_id = uuid::Uuid::from_u128(0x5E1F);
    let self_cell = Arc::new(OnceLock::new());
    self_cell.set(self_id).expect("set self id");
    let live = Arc::new(RwLock::new(Arc::new(vec![QueryWorkerRecord {
        process_id: self_id.to_string(),
        // Never dialed: a self-mapped slice makes no network hop.
        fragment_endpoint: "127.0.0.1:1".to_string(),
        protocol_version: codec::PROTOCOL_VERSION,
        started_unix_ns: 0,
    }])));
    let fetcher = Arc::new(RoutingSliceFetcher::new(
        self_cell,
        live,
        Arc::new(vec![FRAGMENT_KEY]),
        local_service,
        metrics.clone(),
        fragment_tls::client_tls(),
    ));
    let distributed = Arc::new(ravel_query::distrib::Distributed::new(
        fetcher,
        always_distribute_settings().thresholds,
    ));
    let coordinator_catalog = ravel_server::query::build_catalog(
        Arc::clone(&backing),
        1,
        false,
        CACHE_BYTES,
        None,
        None,
        None,
        Duration::from_secs(2),
    )
    .expect("catalog");
    let coordinator = QueryEngine::new(
        coordinator_catalog,
        Arc::clone(&backing),
        EngineConfig::default(),
    )
    .with_distributed(distributed);

    let (start_ms, end_ms, step_ms) = (
        anchor / NS_PER_SEC * 1000,
        (anchor + 10 * NS_PER_MIN) / NS_PER_SEC * 1000,
        60_000,
    );
    let (value, _) = coordinator
        .range_with_stats(
            tenant_hash,
            METRIC,
            start_ms,
            end_ms,
            step_ms,
            &[],
            now,
            Duration::from_secs(60),
        )
        .await
        .expect("a single retryable record-GET blip must not fail the query");

    assert_eq!(
        worker_store.fault_count(Op::Get, FaultKind::Transient),
        1,
        "the record GET blip must have fired exactly once"
    );
    let series = match &value {
        ravel_promql::Value::Matrix(matrix) => matrix,
        other => panic!("a range query returns a matrix, got {}", other.type_name()),
    };
    assert_eq!(series.len(), 1, "the published segment carries one series");
    let points: Vec<(i64, u64)> = series[0]
        .1
        .iter()
        .map(|sample| ((sample.ts_ns - anchor) / NS_PER_MIN, sample.value.to_bits()))
        .collect();
    // The 5-minute lookback carries the sample at +0 across steps 0..2 and the
    // sample at +3 across steps 3..7; step 8 is five minutes past it and empty.
    let expected: Vec<(i64, u64)> = [0, 1, 2]
        .into_iter()
        .map(|m| (m, 1.0f64.to_bits()))
        .chain([3, 4, 5, 6, 7].into_iter().map(|m| (m, 2.5f64.to_bits())))
        .collect();
    assert_eq!(
        points, expected,
        "the retried query returns the exact grid the published samples produce"
    );
    assert_eq!(
        metrics.slices_remote_total(),
        0,
        "the slice is self-mapped: no remote dispatch is made"
    );
    assert_eq!(
        metrics.slices_fallback_total(),
        0,
        "a self-mapped slice never enters the remote-then-fallback ladder"
    );
}

/// ADR-1689 decision 4 (release B) grep gate: no source under `crates/` or
/// `services/` names the deleted `Combined` fragment role or the removed
/// heartbeat field, except the one ravel-fleet test that decodes a heartbeat
/// object written before the removal. The needles are assembled at run time so
/// this file does not match itself.
#[test]
fn release_b_leaves_no_combined_role_or_removed_heartbeat_field() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let needles = [
        concat!("FragmentListener", "Role::Combined"),
        concat!("flight_sql", "_endpoint"),
    ];
    let allowed_file = std::path::Path::new("crates/ravel-fleet/src/query_workers.rs");
    let allowed_test = concat!(
        "fn pre_release_b_record_with_",
        "flight_sql",
        "_endpoint_decodes_and_ignores_it()"
    );

    let mut stack = vec![root.join("crates"), root.join("services")];
    let mut scanned = 0;
    let mut hits = Vec::new();
    let mut allowed_hits = 0;
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read dir") {
            let path = entry.expect("dir entry").path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if path.is_dir() {
                if !matches!(name, "target" | "node_modules") && !name.starts_with('.') {
                    stack.push(path);
                }
                continue;
            }
            let Ok(text) = std::fs::read_to_string(&path) else {
                continue;
            };
            scanned += 1;
            let relative = path
                .strip_prefix(&root)
                .expect("under the root")
                .to_path_buf();
            // The allowed span: the decode-compatibility test, from its doc
            // comment to the first line closing a test-module item after it.
            let allowed_lines = if relative == allowed_file {
                let lines: Vec<&str> = text.lines().collect();
                let fn_line = lines
                    .iter()
                    .position(|l| l.contains(allowed_test))
                    .expect("the decode-compatibility test exists");
                let start = (0..fn_line)
                    .rev()
                    .take_while(|&i| {
                        lines[i].trim_start().starts_with("///") || lines[i].trim() == "#[test]"
                    })
                    .last()
                    .unwrap_or(fn_line);
                let end = (fn_line..lines.len())
                    .find(|&i| lines[i] == "    }")
                    .expect("the test closes");
                Some(start..=end)
            } else {
                None
            };
            for (index, line) in text.lines().enumerate() {
                for needle in needles {
                    if line.contains(needle) {
                        if allowed_lines.as_ref().is_some_and(|r| r.contains(&index)) {
                            allowed_hits += 1;
                        } else {
                            hits.push(format!("{}:{}: {line}", relative.display(), index + 1));
                        }
                    }
                }
            }
        }
    }
    assert!(
        scanned > 100,
        "the scan read the checkout ({scanned} files)"
    );
    assert!(
        allowed_hits > 0,
        "the decode-compatibility test still names the field it decodes"
    );
    assert!(hits.is_empty(), "release B leftovers:\n{}", hits.join("\n"));
}

/// ADR-1689 decision 4 (release B): SQL slices travel only to the dedicated
/// TLS fragment listener.
#[cfg(feature = "flight-sql")]
mod sql_slices {
    use std::collections::HashSet;
    use std::sync::atomic::AtomicUsize;
    use std::time::{Duration, Instant};

    use arrow_flight::Ticket;
    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, TicketStatementQuery};
    use futures::TryStreamExt;
    use prost::Message;
    use ravel_sql::{FlightTicket, SliceReject, SqlTicketKeys, TicketSurface};
    use tonic::Request;

    use super::*;

    const QUERY: &str = "SELECT ts, value FROM samples ORDER BY ts";
    const DEADLINE: Duration = Duration::from_secs(20);

    /// A store over one shared [`MemoryStore`] that counts GETs of the
    /// published data objects and, when `refuse` is set, fails them.
    struct DataStore {
        inner: Arc<MemoryStore>,
        data_keys: HashSet<String>,
        refuse: bool,
        data_gets: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for DataStore {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> Result<PutOutcome, StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
            if self.data_keys.contains(key) {
                self.data_gets.fetch_add(1, Ordering::SeqCst);
                if self.refuse {
                    return Err(StoreError::AccessDenied(format!(
                        "the coordinator may not read {key}"
                    )));
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

        async fn list(
            &self,
            prefix: &str,
            page: Option<PageToken>,
        ) -> Result<ListPage, StoreError> {
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

    /// Publish one segment of `cpu` on `shard` with `samples` and return its
    /// data key.
    async fn publish_on_shard(store: &MemoryStore, shard: u32, samples: &[(i64, f64)]) -> String {
        let tenant = TenantId::new(TENANT);
        let tenant_hash = tenant.hash();
        let label_set = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: "cpu".to_string(),
        }])
        .expect("valid labels");
        let series = vec![SeriesInput {
            series_id: SeriesId::compute(&tenant, "cpu", &label_set).expect("series id"),
            labels: label_set,
            samples: samples
                .iter()
                .map(|&(ts_ns, value)| Sample { ts_ns, value })
                .collect(),
        }];
        let writer_id = uuid::Uuid::from_u128(7_000 + u128::from(shard));
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard,
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
        let base_ns = samples[0].0;
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard,
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
        data_key
    }

    /// `message` with the tenant bearer token and the query window attached.
    fn authed<T>(message: T, start_ns: i64, end_ns: i64) -> Request<T> {
        let mut request = Request::new(message);
        let metadata = request.metadata_mut();
        metadata.insert(
            "authorization",
            format!("Bearer {TOKEN}").parse().expect("ascii"),
        );
        metadata.insert(
            "x-ravel-start",
            (start_ns / NS_PER_SEC).to_string().parse().expect("ascii"),
        );
        metadata.insert(
            "x-ravel-end",
            (end_ns / NS_PER_SEC).to_string().parse().expect("ascii"),
        );
        request
    }

    /// A client of the public gRPC listener at `grpc`, which is plaintext.
    async fn public_client(
        grpc: std::net::SocketAddr,
    ) -> FlightServiceClient<tonic::transport::Channel> {
        let channel = tonic::transport::Channel::from_shared(format!("http://{grpc}"))
            .expect("valid endpoint uri")
            .connect()
            .await
            .expect("connect to the public gRPC listener");
        FlightServiceClient::new(channel)
    }

    /// One Flight SQL statement against `grpc`, returning how many rows came
    /// back, or the status that refused it.
    async fn run_query(
        grpc: std::net::SocketAddr,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<usize, String> {
        let mut client = public_client(grpc).await;
        let command = CommandStatementQuery {
            query: QUERY.to_string(),
            transaction_id: None,
        };
        let descriptor = arrow_flight::FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
        let info = client
            .get_flight_info(authed(descriptor, start_ns, end_ns))
            .await
            .map_err(|s| format!("get_flight_info: {s}"))?
            .into_inner();
        let ticket = info.endpoint[0].ticket.clone().expect("endpoint ticket");
        let stream = client
            .do_get(authed(ticket, start_ns, end_ns))
            .await
            .map_err(|s| format!("do_get: {s}"))?
            .into_inner();
        let batches: Vec<_> = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.map_err(|s| arrow_flight::error::FlightError::Tonic(Box::new(s))),
        )
        .try_collect()
        .await
        .map_err(|e| format!("do_get stream: {e}"))?;
        Ok(batches.iter().map(|b| b.num_rows()).sum())
    }

    /// Every query-worker heartbeat object in `store`, as raw JSON.
    async fn heartbeat_objects(store: &MemoryStore) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        for meta in store
            .list(ravel_fleet::query_workers::QUERY_WORKERS_PREFIX, None)
            .await
            .expect("list query workers")
            .objects
        {
            let got = store
                .get(&meta.key, GetRange::Full)
                .await
                .expect("get worker record");
            out.push(serde_json::from_slice(got.data.as_ref()).expect("record is JSON"));
        }
        out
    }

    /// A slice capability for `TENANT` over no segments, minted under the
    /// cluster's SQL ticket key exactly as a coordinator mints one.
    fn slice_capability() -> Vec<u8> {
        let now = now_ns();
        let ticket = FlightTicket {
            tenant: TenantId::new(TENANT).hash(),
            statement: String::new(),
            segments: Vec::new(),
            min_commit_tokens: Vec::new(),
            now_ns: now,
            deadline_ns: now + 60 * NS_PER_SEC,
            slice_index: 0,
            slice_count: 2,
            pending_erasure: Vec::new(),
            declared_columns: Vec::new(),
            parquet_tables: Vec::new(),
            budgets: None,
        };
        SqlTicketKeys::from_file_key(&SQL_TICKET_KEY)
            .encode(&ticket, TicketSurface::Slice)
            .expect("encode")
    }

    /// A coordinator and a worker, both with the dedicated listener: the
    /// coordinator's SQL roster names the worker at
    /// `https://{fragment_endpoint}` and never at its public gRPC address;
    /// every slice fetch is dialed over TLS (the coordinator's
    /// `ravel_sql_slice_tls_dials_total` counts one per slice, and the
    /// coordinator, which may not read the segments, still answers, so the
    /// worker served them); the worker's heartbeat record carries the fragment
    /// endpoint and no other address; and a slice ticket presented on the
    /// worker's public listener is refused as `wrong_surface`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn sql_slices_dial_only_the_tls_fragment_listener() {
        const SLICES: u64 = 2;
        let shared = Arc::new(MemoryStore::new());
        let base = now_ns() - 10 * NS_PER_MIN;
        let data_keys: HashSet<String> = [
            publish_on_shard(&shared, 0, &[(base, 1.5), (base + NS_PER_MIN, 2.5)]).await,
            publish_on_shard(&shared, 1, &[(base, 1.5), (base + 2 * NS_PER_MIN, 3.5)]).await,
        ]
        .into_iter()
        .collect();
        let (start_ns, end_ns) = (base - 5 * NS_PER_MIN, now_ns());

        let worker_store = Arc::new(DataStore {
            inner: shared.clone(),
            data_keys: data_keys.clone(),
            refuse: false,
            data_gets: AtomicUsize::new(0),
        });
        let worker = start_server_with(
            worker_store.clone(),
            Some(always_distribute_settings()),
            ravel_query::QueryConcurrencyLimit::Unlimited,
            2,
        )
        .await;
        let worker_fragment = worker
            .fragment_addr
            .expect("the dedicated fragment listener binds");
        let worker_grpc = worker.grpc_addr.expect("gRPC binds in All mode");
        let coordinator_store = Arc::new(DataStore {
            inner: shared.clone(),
            data_keys,
            refuse: true,
            data_gets: AtomicUsize::new(0),
        });
        let coordinator = start_server_with(
            coordinator_store,
            Some(always_distribute_settings()),
            ravel_query::QueryConcurrencyLimit::Unlimited,
            2,
        )
        .await;
        let coordinator_grpc = coordinator.grpc_addr.expect("gRPC binds in All mode");

        // The dial target: the roster resolves exactly as the coordinator's
        // `DoGet` resolves it, once its heartbeat read lists the worker.
        let roster = coordinator
            .sql_slice_workers
            .clone()
            .expect("--distributed-query builds the SQL roster");
        let deadline = Instant::now() + DEADLINE;
        let locations = loop {
            let locations = roster.endpoints();
            if !locations.is_empty() {
                break locations;
            }
            assert!(
                Instant::now() < deadline,
                "the coordinator never listed the worker"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        assert_eq!(
            locations,
            vec![format!("https://{worker_fragment}")],
            "the only slice location is the worker's dedicated listener, over https"
        );

        // The heartbeat record names the dedicated listener and nothing else:
        // its fields are exactly these four, and the public gRPC address
        // appears in no record.
        let records = heartbeat_objects(&shared).await;
        assert_eq!(records.len(), 2, "one record per process: {records:?}");
        for record in &records {
            let fields: HashSet<&str> = record
                .as_object()
                .expect("a record is a JSON object")
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(
                fields,
                HashSet::from([
                    "process_id",
                    "fragment_endpoint",
                    "protocol_version",
                    "started_unix_ns"
                ]),
                "{record}"
            );
            assert!(
                !record.to_string().contains(&worker_grpc.to_string())
                    && !record.to_string().contains(&coordinator_grpc.to_string()),
                "no public gRPC address is advertised: {record}"
            );
        }
        assert!(
            records
                .iter()
                .any(|r| r["fragment_endpoint"] == worker_fragment.to_string()),
            "the worker advertises its dedicated listener: {records:?}"
        );

        let dials = coordinator
            .sql_slice_tls_dials
            .clone()
            .expect("Flight SQL is served");
        assert_eq!(dials.get(), 0, "nothing dialed before the query");
        let deadline = Instant::now() + DEADLINE;
        let mut attempts = 0;
        let rows = loop {
            attempts += 1;
            match run_query(coordinator_grpc, start_ns, end_ns).await {
                Ok(rows) => break rows,
                Err(last) if Instant::now() >= deadline => {
                    panic!("the distributed query never succeeded; last error: {last}")
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
            }
        };
        assert_eq!(rows, 3, "the cross-shard duplicate dedups");
        assert_eq!(
            dials.get(),
            SLICES * attempts,
            "every slice of every attempt was dialed over TLS"
        );
        assert!(
            worker_store.data_gets.load(Ordering::SeqCst) > 0,
            "the worker read the segments, so it served the slices"
        );
        let body = scrape_metrics(&format!("http://{}", coordinator.http_addr)).await;
        let line = format!(
            "ravel_sql_slice_tls_dials_total{{mode=\"all\"}} {}",
            dials.get()
        );
        assert_eq!(
            body.lines().filter(|l| *l == line).count(),
            1,
            "exactly one `{line}` sample on /metrics:\n{body}"
        );

        // A slice ticket on the worker's public listener is refused.
        let worker_rejects = worker.sql_slice_rejects.clone().expect("Flight SQL served");
        assert_eq!(worker_rejects.get(SliceReject::WrongSurface), 0);
        let query = TicketStatementQuery {
            statement_handle: slice_capability().into(),
        };
        let status = public_client(worker_grpc)
            .await
            .do_get(Request::new(Ticket::new(query.as_any().encode_to_vec())))
            .await
            .expect_err("the public listener refuses a slice ticket");
        assert_eq!(status.code(), tonic::Code::PermissionDenied, "{status:?}");
        assert_eq!(status.message(), "slice fetch rejected: wrong_surface");
        assert_eq!(worker_rejects.get(SliceReject::WrongSurface), 1);

        coordinator
            .shutdown()
            .await
            .expect("coordinator shuts down");
        worker.shutdown().await.expect("worker shuts down");
    }
}
