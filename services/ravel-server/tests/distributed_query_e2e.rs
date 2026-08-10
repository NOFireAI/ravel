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
        samples: vec![
            Sample {
                ts_ns: base_ns + 100_000_000,
                value: 1.0,
            },
            Sample {
                ts_ns: base_ns + 200_000_000,
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
        created_unix_ns: base_ns + 300_000_000,
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
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
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
