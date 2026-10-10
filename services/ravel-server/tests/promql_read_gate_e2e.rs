//! ADR-1702 decisions 1 and 4 reachability: a server built by
//! [`ravel_server::start`] runs its PromQL engine's evaluation and its RSEG
//! catalog decodes on the server's read CPU gate.
//!
//! Two real RSEG segments are published: a large one whose single series
//! holds at least the gate's default evaluation floor of samples, and a small
//! one hours later. A query over each runs through the HTTP API, and the read
//! gate's `promql_eval` and `segment_section` counters are read back from
//! `/metrics` before and after. An engine built without the gate leaves both
//! sites at zero; a gate on the fetcher alone moves `segment_section` and
//! leaves `promql_eval` at zero.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_cpu_gate::DEFAULT_EVAL_FLOOR_SAMPLES;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, StoreMetrics};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use uuid::Uuid;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const NS_PER_MS: i64 = 1_000_000;
const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
/// The large segment's first sample: ten minutes into hour 1.
const BIG_START_NS: i64 = NS_PER_HOUR + 600 * NS_PER_SEC;
/// Samples in the large segment's one series, one per millisecond, so all of
/// them sit inside a 5m range ending just after the last one. Equal to the
/// default evaluation floor; the test asserts that rather than trusting it.
const BIG_SAMPLES: i64 = 100_000;
/// The small segment's first sample: hour 3, far outside the large query's
/// window, so each query's resolve selects only its own segment.
const SMALL_START_NS: i64 = 3 * NS_PER_HOUR + 600 * NS_PER_SEC;
/// Samples in the small segment's one series, one per second.
const SMALL_SAMPLES: i64 = 10;

fn labels(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

/// Publishes one real RSEG segment holding `samples` for one `metric` series,
/// returning the segment's own sample count.
async fn publish_segment(
    store: &MemoryStore,
    tenant_hash: TenantHash,
    writer_seq: u64,
    metric: &str,
    samples: Vec<Sample>,
) -> u64 {
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(&TenantId::new(TENANT), metric, &labels(metric))
            .expect("series id"),
        labels: labels(metric),
        samples,
    }];
    let writer_id = Uuid::new_v4();
    let written = SegmentWriter::write(
        series,
        SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq,
        },
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");
    let created_unix_ns = written.summary.max_event_ts_ns;
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
        created_unix_ns,
        ingest_hour_bucket: u32::try_from(created_unix_ns / NS_PER_HOUR).expect("hour bucket"),
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish commit record");
    written.summary.sample_count
}

fn samples(start_ns: i64, step_ns: i64, n: i64) -> Vec<Sample> {
    (0..n)
        .map(|i| Sample {
            ts_ns: start_ns + i * step_ns,
            value: 1.0,
        })
        .collect()
}

async fn start_query_server(store: Arc<MemoryStore>) -> ravel_server::Running {
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
        mode: Mode::Query,
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
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: ravel_server::config::CpuGatePermits { read: 2, write: 1 },
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: std::time::Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    let backend: Arc<dyn ObjectStoreBackend> = store;
    ravel_server::start(
        config,
        backend.clone(),
        backend,
        Arc::new(StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
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
            format!("ravel_cpu_gate_{family}{{mode=\"query\",gate=\"read\",site=\"{site}\"}} ");
        let samples: Vec<&str> = body
            .lines()
            .filter_map(|line| line.strip_prefix(prefix.as_str()))
            .collect();
        assert_eq!(samples.len(), 1, "{prefix:?} must render once:\n{body}");
        samples[0].parse::<u64>().expect("counter value")
    };
    (figure("jobs_total"), figure("inline_total"))
}

/// Runs an instant query at `time_ns` and returns its one result value.
async fn instant_value(running: &ravel_server::Running, query: &str, time_ns: i64) -> String {
    let time = format!(
        "{}.{:03}",
        time_ns / NS_PER_SEC,
        (time_ns % NS_PER_SEC) / NS_PER_MS
    );
    let response = reqwest::Client::new()
        .get(format!("http://{}/api/v1/query", running.http_addr))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[("query", query), ("time", time.as_str())])
        .send()
        .await
        .expect("query request completes");
    let status = response.status();
    let body: serde_json::Value = response.json().await.expect("query response is JSON");
    assert_eq!(status, 200, "{query} succeeds: {body}");
    let result = body["data"]["result"]
        .as_array()
        .unwrap_or_else(|| panic!("{query} returned no result array: {body}"));
    assert_eq!(result.len(), 1, "{query} returns one series: {body}");
    result[0]["value"][1]
        .as_str()
        .unwrap_or_else(|| panic!("{query} returned no value: {body}"))
        .to_string()
}

/// A query over at least the evaluation floor of samples evaluates on the read
/// gate as one `promql_eval` job; a query under it runs inline and counts one
/// inline `promql_eval`. Each query's resolve selects one segment, whose
/// catalog decode counts one `segment_section` run. The fixture catalogs are a
/// few hundred bytes, under the 256 KiB byte floor, so that run is inline.
///
/// Fails on an engine built without the gate (`promql_eval` and
/// `segment_section` both stay `(0, 0)`), and on a gate attached to the
/// engine's fetcher only (`segment_section` moves, `promql_eval` stays
/// `(0, 0)`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn promql_evaluation_and_catalog_decode_run_on_the_server_read_gate() {
    let tenant = TenantId::new(TENANT).hash();
    let store = Arc::new(MemoryStore::new());
    let big_count = publish_segment(
        &store,
        tenant,
        1,
        "gate_big",
        samples(BIG_START_NS, NS_PER_MS, BIG_SAMPLES),
    )
    .await;
    assert!(
        big_count >= DEFAULT_EVAL_FLOOR_SAMPLES,
        "the large segment holds {big_count} samples, below the {DEFAULT_EVAL_FLOOR_SAMPLES} \
         sample evaluation floor, so its evaluation would run inline whatever gate the \
         engine holds"
    );
    let small_count = publish_segment(
        &store,
        tenant,
        2,
        "gate_small",
        samples(SMALL_START_NS, NS_PER_SEC, SMALL_SAMPLES),
    )
    .await;
    assert!(small_count < DEFAULT_EVAL_FLOOR_SAMPLES);

    let running = start_query_server(store.clone()).await;
    assert_eq!(read_gate_site(&running, "promql_eval").await, (0, 0));
    assert_eq!(read_gate_site(&running, "segment_section").await, (0, 0));

    // The range ends just after the last large sample and its 5m reach covers
    // every one of them, so the evaluation reads all BIG_SAMPLES: the answer
    // proves the evaluated count, not only the stored one.
    let big_end_ns = BIG_START_NS + BIG_SAMPLES * NS_PER_MS;
    let big = instant_value(&running, "count_over_time(gate_big[5m])", big_end_ns).await;
    assert_eq!(big, BIG_SAMPLES.to_string());
    assert!(BIG_SAMPLES as u64 >= DEFAULT_EVAL_FLOOR_SAMPLES);
    // One segment overlaps the window and its catalog decodes once.
    assert_eq!(
        read_gate_site(&running, "segment_section").await,
        (0, 1),
        "the large segment's catalog decode was counted at the read gate"
    );
    assert_eq!(
        read_gate_site(&running, "promql_eval").await,
        (1, 0),
        "the evaluation over {BIG_SAMPLES} samples ran on the read gate, once"
    );

    // The instant selector's 5m lookback holds SMALL_SAMPLES samples, far
    // under the floor, so the evaluation runs inline.
    let small_end_ns = SMALL_START_NS + (SMALL_SAMPLES - 1) * NS_PER_SEC;
    let small = instant_value(&running, "count_over_time(gate_small[5m])", small_end_ns).await;
    assert_eq!(small, SMALL_SAMPLES.to_string());
    assert_eq!(
        read_gate_site(&running, "segment_section").await,
        (0, 2),
        "the small segment's catalog decode was counted at the read gate"
    );
    assert_eq!(
        read_gate_site(&running, "promql_eval").await,
        (1, 1),
        "the small evaluation ran inline and counted one inline run"
    );

    running.shutdown().await.expect("graceful shutdown");
}
