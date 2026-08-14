//! End-to-end reachability test for ADR-0065's ownership and merge-memory
//! metrics. This is the designated proof that the
//! gauges are actually wired and reachable through the real `/metrics`
//! surface, not merely unit-tested against `MaintenanceOwnershipMetrics`
//! directly: it spins up TWO real `Mode::Maintain` `ravel_server::start`
//! roles (each with its own real spawned maintenance supervisor and
//! `WorkerSet`) sharing one store partition, waits for them to converge on
//! `ravel_maintain_workers_live == 2` via their own real heartbeat/liveness
//! protocol, and asserts the ownership split and the new metric families all
//! surface correctly on the real `GET /metrics` endpoint of each.
//!
//! Follows the structural pattern of `tests/scrub_e2e.rs` (build `ServerConfig`
//! directly, publish a real segment via `SegmentWriter`/`record::build`, start
//! through `ravel_server::start`, poll `/metrics` with a real `reqwest`
//! client). The worker heartbeat cadence is not injectable through the
//! server's public surface
//! (`ravel_maintain::worker_set::DEFAULT_HEARTBEAT_INTERVAL`, 60s), so
//! whichever of the two servers starts first only refreshes its own
//! `workers_live` gauge on its second heartbeat tick, a real ~60s after its
//! loop began. This test accepts that real wait rather than faking
//! convergence: it is the real production cadence, not a test shortcut.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::{Mode, ServerConfig};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Shards this test partitions across the two workers. Each shard carries
/// `MAINTAINED_SIGNALS.len()` (Metrics, Logs, Spans) units, so the total unit
/// count the two workers must jointly own is `SHARD_COUNT * 3`.
const SHARD_COUNT: u32 = 8;

/// Build a `Mode::Maintain` config over `store`, with a 1-second maintenance
/// interval so discovery-cycle-driven gauges (`units_owned`,
/// `full_sweep_passes_total`) update within a few real seconds. Two servers
/// built from this helper against the same store constitute the two workers
/// under test.
fn maintain_config(tenant: &TenantId) -> ServerConfig {
    ServerConfig {
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        mode: Mode::Maintain,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: SHARD_COUNT,
        tenant_resolver: ravel_server::tenant::build_resolver(Default::default(), false),
        mtls_listener: None,
        fold_tenants: vec![tenant.hash()],
        fold: ravel_server::FoldTaskConfig::default(),
        maintain: ravel_server::MaintenanceTaskConfig {
            enabled: true,
            interval: Duration::from_secs(1),
            shard_count: SHARD_COUNT,
            ..ravel_server::MaintenanceTaskConfig::default()
        },
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
        scrub_period: Duration::from_secs(60),
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

/// Publish one real RSEG segment plus its commit record into `(tenant,
/// Metrics, shard 0)`, exactly as ingest would, so real tenant discovery
/// (`ravel_maintain::discover_tenants`) has something to find.
async fn publish_segment(store: &MemoryStore, tenant: &TenantId) {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(7);
    let created_unix_ns = 500_000 * NS_PER_HOUR;
    let ingest_hour_bucket = 500_000u32;
    let series: Vec<SeriesInput> = ["cpu", "mem"]
        .iter()
        .map(|metric| {
            let labels = LabelSet::new(vec![Label {
                name: METRIC_NAME_LABEL.to_string(),
                value: (*metric).to_string(),
            }])
            .expect("valid labels");
            let series_id = SeriesId::compute(tenant, metric, &labels).expect("series id");
            SeriesInput {
                series_id,
                labels,
                samples: vec![Sample {
                    ts_ns: created_unix_ns,
                    value: 1.0,
                }],
            }
        })
        .collect();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let min_ingest_ts_ns = created_unix_ns - 1_000;
    let max_ingest_ts_ns = created_unix_ns;
    let bounds = IngestBounds {
        min_ingest_ts_ns,
        max_ingest_ts_ns,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
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
        min_ingest_ts_ns,
        max_ingest_ts_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish commit record");
}

/// Pull the trailing integer off the one line in `body` starting with
/// `line_prefix` (a full `name{labels}` prefix). Every sample this test reads
/// is rendered by `write_sample` as `name{labels} <u64>`, one line, no other
/// line in the exposition can share the prefix once labels are included.
fn sample_value(body: &str, line_prefix: &str) -> Option<u64> {
    body.lines()
        .find(|line| line.starts_with(line_prefix))
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|v| v.parse::<u64>().ok())
}

/// ADR-0065's overall acceptance criterion: two real
/// `Mode::Maintain` roles, wired exactly as production spawns them, sharing
/// one store partition, converge their `ravel_maintain_workers_live` gauge to
/// 2 and jointly own every `(signal, shard)` unit with no double-pay, all
/// observed through the real `/metrics` HTTP surface.
#[tokio::test]
async fn two_maintain_workers_reach_and_report_full_ownership_on_real_metrics() {
    let tenant = TenantId::new("ownership-e2e");
    let store = Arc::new(MemoryStore::new());
    publish_segment(store.as_ref(), &tenant).await;

    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
    let a = ravel_server::start(
        maintain_config(&tenant),
        store_dyn.clone(),
        store_dyn.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("worker a starts");
    let b = ravel_server::start(
        maintain_config(&tenant),
        store_dyn.clone(),
        store_dyn.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("worker b starts");

    let client = reqwest::Client::new();
    let base_a = format!("http://{}", a.http_addr);
    let base_b = format!("http://{}", b.http_addr);

    async fn scrape(client: &reqwest::Client, base: &str) -> String {
        client
            .get(format!("{base}/metrics"))
            .send()
            .await
            .expect("scrape /metrics")
            .text()
            .await
            .expect("metrics body")
    }

    // Poll until BOTH workers report the full two-member live set. Whichever
    // started first (here, `a`) only picks this up on its own second
    // heartbeat tick -- a real ~60s after its loop began -- so this window
    // must comfortably exceed one `DEFAULT_HEARTBEAT_INTERVAL`.
    let mut converged = false;
    for _ in 0..600 {
        let body_a = scrape(&client, &base_a).await;
        let body_b = scrape(&client, &base_b).await;
        let live_a = sample_value(&body_a, "ravel_maintain_workers_live{mode=\"maintain\"}");
        let live_b = sample_value(&body_b, "ravel_maintain_workers_live{mode=\"maintain\"}");
        if live_a == Some(2) && live_b == Some(2) {
            converged = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        converged,
        "both workers must report ravel_maintain_workers_live == 2 within the polling window \
         (one DEFAULT_HEARTBEAT_INTERVAL plus margin)"
    );

    // A fresh discovery cycle must run under the now-converged live set
    // before `units_owned` reflects the final partition; the maintenance
    // interval is 1s, so this comfortably waits for one.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // A single scrape pair can race `run_discovery_cycle`'s
    // `set_units_owned(0)`-then-accumulate window (maintain.rs): a scrape
    // landing between one worker's reset and its tenant loop finishing sees
    // a transient undercount. Poll a short, bounded number of times rather
    // than lengthening the fixed sleep above -- the reset window is a few
    // microseconds, so this almost always succeeds on the first scrape and
    // only retries across a genuine race.
    let expected_total = u64::from(SHARD_COUNT) * 3;
    let mut owned_a = 0;
    let mut owned_b = 0;
    let mut settled = false;
    for _ in 0..25 {
        let body_a = scrape(&client, &base_a).await;
        let body_b = scrape(&client, &base_b).await;
        owned_a = sample_value(&body_a, "ravel_maintain_units_owned{mode=\"maintain\"}")
            .expect("units_owned present on a");
        owned_b = sample_value(&body_b, "ravel_maintain_units_owned{mode=\"maintain\"}")
            .expect("units_owned present on b");
        if owned_a + owned_b == expected_total {
            settled = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(
        settled,
        "the two workers must jointly own every (signal, shard) unit exactly once, no double-pay \
         (ADR-0065 decision 2), regardless of how the rendezvous hash splits them: got \
         owned_a={owned_a}, owned_b={owned_b}, expected total {expected_total}"
    );

    let body_a = scrape(&client, &base_a).await;
    let body_b = scrape(&client, &base_b).await;

    for (name, body) in [("a", &body_a), ("b", &body_b)] {
        assert!(
            body.contains("ravel_maintain_units_stalled{mode=\"maintain\"}"),
            "ravel_maintain_units_stalled must be reachable on worker {name}'s /metrics"
        );
        assert!(
            body.contains("ravel_maintain_memo_warm_start_units_total{mode=\"maintain\"}"),
            "ravel_maintain_memo_warm_start_units_total must be reachable on worker {name}'s \
             /metrics"
        );
        assert!(
            body.contains("ravel_maintain_full_sweep_passes_total{mode=\"maintain\"}"),
            "ravel_maintain_full_sweep_passes_total must be reachable on worker {name}'s /metrics"
        );
        assert!(
            body.contains(
                "ravel_maintain_rlog_merge_peak_bytes{mode=\"maintain\",kind=\"transient\"}"
            ) && body
                .contains("ravel_maintain_rlog_merge_peak_bytes{mode=\"maintain\",kind=\"total\"}"),
            "ravel_maintain_rlog_merge_peak_bytes must be reachable on worker {name}'s /metrics, \
             for both the transient and total kinds (ADR-0065 decision 4)"
        );
    }

    // A cold-started memo runs its first pass as a full (unscoped) sweep for
    // every owned unit, so this must already be nonzero on both sides, not
    // merely present.
    let full_sweeps_a = sample_value(
        &body_a,
        "ravel_maintain_full_sweep_passes_total{mode=\"maintain\"}",
    )
    .expect("full_sweep_passes_total present on a");
    let full_sweeps_b = sample_value(
        &body_b,
        "ravel_maintain_full_sweep_passes_total{mode=\"maintain\"}",
    )
    .expect("full_sweep_passes_total present on b");
    assert!(
        full_sweeps_a > 0 && full_sweeps_b > 0,
        "both workers' cold-started first tick must record at least one full sweep pass per owned \
         unit"
    );

    a.shutdown().await.expect("graceful shutdown a");
    b.shutdown().await.expect("graceful shutdown b");
}
