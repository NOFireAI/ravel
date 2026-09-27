//! Issue #1255, distributed half: with `--distributed-query` on, a PromQL
//! metrics fetch runs through `FragmentService`, both for a remote worker's
//! slice and for the coordinator's own no-hop local path, so the process-wide
//! `MemoryBudget` has to reach the `SegmentFetcher` that service builds, not
//! only the one `QueryEngine` owns.
//!
//! Every server here is a real `ravel_server::start`ed process over a
//! store of its own holding one real RSEG segment large enough (over the
//! fetcher's 512 KiB whole-object threshold) that reading it goes through the
//! budgeted range reservations in `SegmentFetcher::ensure_ranges`:
//!
//! - a PromQL query through the HTTP API of a distributed server with a 4 KiB
//!   budget is refused with 503, where the same query at an unlimited budget
//!   answers 200;
//! - a fragment slice sent over gRPC to a server with a 1-byte budget ends with
//!   a `BudgetExceeded` summary carrying the typed `FetchMemoryExhausted`
//!   message against that server's own limit;
//! - while an unlimited server's slice holds a range GET, `/metrics` reports
//!   `ravel_memory_reserved_bytes{component="fetch"}` at exactly the first
//!   reservation, the one that 1-byte refusal named, `component="sql"` at 0,
//!   and both read 0 again once the slice completes.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
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
const METRIC: &str = "big_gauge";
const FRAGMENT_KEY: [u8; 32] = [0x5au8; 32];

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;

/// 150,000 one-millisecond samples: 150 s of data ending 250 s before `now`,
/// so a query at `now - 300 s` lands inside it with margin on both sides.
const SAMPLES: u64 = 150_000;
const START_OFFSET_S: i64 = 400;
const QUERY_OFFSET_S: i64 = 300;

fn now_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("now fits i64")
}

/// SplitMix64-derived values in [1, 2), so the value column does not compress
/// and the object stays over the whole-object threshold.
fn high_entropy_value(i: u64) -> f64 {
    let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^= z >> 31;
    f64::from_bits((z & 0x000F_FFFF_FFFF_FFFF) | 0x3FF0_0000_0000_0000)
}

async fn publish_large_segment(store: &dyn ObjectStoreBackend, tenant: &TenantId, now: i64) {
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let base_ts_ns = now - START_OFFSET_S * NS_PER_SEC;
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, METRIC, &label_set).expect("series id"),
        labels: label_set,
        samples: (0..SAMPLES)
            .map(|i| Sample {
                ts_ns: base_ts_ns + i as i64 * 1_000_000,
                value: high_entropy_value(i),
            })
            .collect(),
    }];

    let writer_id = uuid::Uuid::from_u128(4_200);
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
    let object_size = written.bytes.len() as u64;
    assert!(
        object_size > 512 * 1024,
        "fixture must exceed the 512 KiB whole-object threshold to force a \
         budgeted range read, got {object_size} bytes"
    );

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: now,
        ingest_hour_bucket: u32::try_from(now / NS_PER_HOUR).expect("hour bucket fits u32"),
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

/// `--distributed-query` with zero thresholds, so every PromQL query takes the
/// fragment path; with no heartbeat yet the live worker set is empty and each
/// slice runs on the coordinator's no-hop local `FragmentService`.
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
    process_memory_budget_bytes: u64,
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
        // No read cache: the only fetch reservation is the fetcher's own.
        disable_cache: true,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        // The one knob under test.
        process_memory_budget_bytes,
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

async fn promql_status(http: std::net::SocketAddr, query_time_s: i64) -> reqwest::StatusCode {
    reqwest::Client::new()
        .get(format!("http://{http}/api/v1/query"))
        .query(&[
            ("query", METRIC.to_string()),
            ("time", query_time_s.to_string()),
        ])
        .bearer_auth(TOKEN)
        .send()
        .await
        .expect("query request")
        .status()
}

/// A resolve-scope slice over the last hour, the window holding the segment.
/// Resolve scope takes its tenant from the bearer credential, so the test needs
/// no capability minting and still runs the worker's `resolve_and_run`.
fn fragment_request(tenant: &TenantId, now: i64) -> pb::FetchRequest {
    pb::FetchRequest {
        protocol_version: ravel_query::distrib::codec::PROTOCOL_VERSION,
        tenant_hash: tenant.hash().0.to_vec(),
        signal: ravel_query::distrib::codec::signal_to_u32(Signal::Metrics),
        window_start_ns: now - NS_PER_HOUR,
        window_end_ns: now,
        scope: Some(pb::fetch_request::Scope::Resolve(pb::ResolveScope {
            min_commit_token: Vec::new(),
        })),
        ..Default::default()
    }
}

async fn fetch_summary(grpc: std::net::SocketAddr, request: pb::FetchRequest) -> pb::Summary {
    let mut client = SeriesFetchClient::connect(format!("http://{grpc}"))
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

async fn scrape(http: std::net::SocketAddr) -> String {
    reqwest::Client::new()
        .get(format!("http://{http}/metrics"))
        .send()
        .await
        .expect("scrape request")
        .text()
        .await
        .expect("scrape body")
}

/// The value of the one `ravel_memory_reserved_bytes` sample for `component`,
/// panicking unless it appears exactly once.
fn reserved_gauge(scrape: &str, component: &str) -> u64 {
    let prefix = format!("ravel_memory_reserved_bytes{{mode=\"all\",component=\"{component}\"}} ");
    let values: Vec<u64> = scrape
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix))
        .map(|v| v.parse().expect("gauge value is an integer"))
        .collect();
    assert_eq!(
        values.len(),
        1,
        "component={component} must appear exactly once:\n{scrape}"
    );
    values[0]
}

/// A fresh store holding the one large segment. Each server gets its own:
/// servers sharing a store discover each other through the query-worker
/// registry in it, and a slice would then run on whichever process ranks first
/// rather than on the one whose budget is under test.
async fn seeded_store(tenant: &TenantId, now: i64) -> Arc<FaultStore<MemoryStore>> {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    publish_large_segment(store.as_ref(), tenant, now).await;
    store
}

/// Mutation proof: remove `.with_memory_budget(process_memory_budget.clone())`
/// from the `FragmentService` construction in `ravel_server::start`, and the
/// first assertion fails with the 4 KiB server answering the PromQL query 200
/// instead of 503.
#[tokio::test]
async fn distributed_promql_fetches_reserve_against_the_process_budget() {
    let tenant = TenantId::new(TENANT);
    let now = now_ns();
    let query_time_s = (now - QUERY_OFFSET_S * NS_PER_SEC) / NS_PER_SEC;

    // A 4 KiB budget is far below the >512 KiB range this segment reserves, so
    // the coordinator's no-hop fragment fetch is refused and the refusal folds
    // to the PromQL API's 503.
    let small = start_server(seeded_store(&tenant, now).await, 4 * 1024).await;
    assert_eq!(
        promql_status(small.http_addr, query_time_s).await,
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "a distributed PromQL fetch over the process budget must be refused"
    );
    let after = scrape(small.http_addr).await;
    assert!(
        after.contains("ravel_distrib_slices_local_total{mode=\"all\"} 1\n"),
        "the refused slice ran on the coordinator's no-hop fragment path:\n{after}"
    );
    assert_eq!(reserved_gauge(&after, "fetch"), 0);
    assert_eq!(reserved_gauge(&after, "sql"), 0);

    // The worker surface: a 1-byte budget refuses the first reservation with
    // nothing held, so its message names exactly that reservation's size.
    let one_byte = start_server(seeded_store(&tenant, now).await, 1).await;
    let refused = fetch_summary(
        one_byte.grpc_addr.expect("gRPC listener binds in All mode"),
        fragment_request(&tenant, now),
    )
    .await;
    let refused_status = refused.status.expect("summary carries a status");
    assert_eq!(
        pb::status::Code::try_from(refused_status.code).expect("known status code"),
        pb::status::Code::BudgetExceeded,
        "a worker's fetch over its process budget must end BudgetExceeded \
         (message: {})",
        refused_status.message
    );
    let first_reservation: u64 = refused_status
        .message
        .strip_prefix("fetch memory exhausted: requested ")
        .and_then(|rest| rest.strip_suffix(" bytes, 0 of 1 byte budget already reserved"))
        .unwrap_or_else(|| {
            panic!(
                "the refusal must be the typed FetchMemoryExhausted message against \
                 this worker's 1-byte limit, got: {}",
                refused_status.message
            )
        })
        .parse()
        .expect("requested bytes parse");
    assert!(
        first_reservation > 1,
        "the refused reservation must exceed the 1-byte limit, got {first_reservation}"
    );
    assert_eq!(refused.series_returned, 0);

    // Control: an unlimited distributed server answers the same PromQL query.
    let fault_store = seeded_store(&tenant, now).await;
    let unlimited = start_server(fault_store.clone(), u64::MAX).await;
    assert_eq!(
        promql_status(unlimited.http_addr, query_time_s).await,
        reqwest::StatusCode::OK,
        "control: an unlimited budget admits this query"
    );

    // Held: the unlimited server's slice, with every data-object GET parked.
    // The footer read reserves nothing and is released at a 0 gauge; the first
    // GET seen with the gauge nonzero belongs to the fetcher's first range
    // reservation, the one the 1-byte worker refused, and must read it exactly.
    let gate = fault_store.hold(Op::Get, Some("/l0/".to_string()), Occurrence::Always);
    let grpc = unlimited
        .grpc_addr
        .expect("gRPC listener binds in All mode");
    let mut slice = Box::pin(fetch_summary(grpc, fragment_request(&tenant, now)));
    let mut held_fetch: Vec<u64> = Vec::new();
    let served = loop {
        tokio::select! {
            summary = &mut slice => break summary,
            () = gate.wait_until_held(1) => {
                let (id, _, _) = gate.held_details()[0];
                let held = scrape(unlimited.http_addr).await;
                let fetch = reserved_gauge(&held, "fetch");
                if fetch > 0 {
                    if held_fetch.is_empty() {
                        assert_eq!(
                            reserved_gauge(&held, "sql"),
                            0,
                            "a fetch reservation must not be counted under component=\"sql\""
                        );
                    }
                    held_fetch.push(fetch);
                }
                gate.release(id);
            }
        }
    };
    let served_status = served.status.expect("summary carries a status");
    assert_eq!(
        pb::status::Code::try_from(served_status.code).expect("known status code"),
        pb::status::Code::Ok,
        "the unlimited worker serves the slice (message: {})",
        served_status.message
    );
    assert_eq!(served.series_returned, 1);
    assert_eq!(
        held_fetch.first().copied(),
        Some(first_reservation),
        "while the range GET is held the fetch gauge must read the fetcher's \
         reservation exactly; every nonzero reading seen: {held_fetch:?}"
    );
    let done = scrape(unlimited.http_addr).await;
    assert_eq!(reserved_gauge(&done, "fetch"), 0);
    assert_eq!(reserved_gauge(&done, "sql"), 0);

    unlimited
        .shutdown()
        .await
        .expect("unlimited server shuts down");
    one_byte
        .shutdown()
        .await
        .expect("one-byte server shuts down");
    small.shutdown().await.expect("small server shuts down");
}
