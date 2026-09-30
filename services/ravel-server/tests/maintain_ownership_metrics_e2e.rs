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
//! client). The worker heartbeat cadence is injectable (issue #1852) through
//! `MaintenanceTaskConfig::heartbeat_interval`, which `ravel_server::start`
//! forwards to `ravel_maintain::WorkerSet::new` in place of the real
//! production default (`ravel_maintain::worker_set::DEFAULT_HEARTBEAT_INTERVAL`,
//! 60s). Whichever of the two servers starts first only refreshes its own
//! `workers_live` gauge on its first heartbeat tick after the sibling's first
//! one (`run_loop`'s heartbeat task in `ravel_server::maintain`:
//! `tokio::time::interval`'s first tick fires immediately, before the sibling
//! has necessarily written its own heartbeat, so the live set that tick reads
//! can still be solo). With H the heartbeat interval and g the gap between
//! the two loops' first heartbeat ticks, that is at most g + H after the
//! first loop began, H after the second, so this test drives
//! `TEST_HEARTBEAT_INTERVAL` below rather than waiting out real
//! `DEFAULT_HEARTBEAT_INTERVAL`s: the convergence under test is the workers'
//! own liveness protocol, not wall time this test does not control.

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

/// Heartbeat cadence the two-worker convergence test below drives its
/// workers' shared `WorkerSet` at, via `MaintenanceTaskConfig::heartbeat_interval`
/// (issue #1852), in place of the real `DEFAULT_HEARTBEAT_INTERVAL` (60s).
///
/// `WorkerSet`'s liveness window is `DEFAULT_LIVENESS_FACTOR * H` = `3 * H`
/// (crates/ravel-fleet/src/worker_set.rs `liveness_window_ns`), and its reap
/// horizon is `REAP_WINDOW_FACTOR * window` = `6 * H` (`reap_horizon_ns`). A
/// worker whose heartbeat write is delayed past the liveness window drops
/// out of the live set the sibling reads, and past the reap horizon its
/// heartbeat key is deleted outright, so `3 * H` must stay comfortably wider
/// than ordinary scheduler delay on a loaded box. The previous value here
/// (100ms) produced a 300ms liveness window; 300ms of scheduler delay on a
/// CI box under load is ordinary noise, not a fault, so that value
/// re-created the exact flake issue #1852 exists to fix, only tighter. 1s
/// gives a 3s liveness window and a 6s reap horizon: a 10x margin over that
/// 300ms failure point, while `H` itself stays 60x below the real 60s
/// `DEFAULT_HEARTBEAT_INTERVAL`, so convergence below still costs at most
/// the inter-server start gap plus 1s of real wall time, not 60s.
const TEST_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// Build a `Mode::Maintain` config over `store`, with a 1-second maintenance
/// interval so discovery-cycle-driven gauges (`units_owned`,
/// `full_sweep_passes_total`) update within a few real seconds. Two servers
/// built from this helper against the same store constitute the two workers
/// under test.
fn maintain_config(tenant: &TenantId) -> ServerConfig {
    ServerConfig {
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
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(60),
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
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: std::time::Duration::ZERO,
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
    let mut config_a = maintain_config(&tenant);
    config_a.maintain.heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
    let mut config_b = maintain_config(&tenant);
    config_b.maintain.heartbeat_interval = TEST_HEARTBEAT_INTERVAL;
    let a = ravel_server::start(
        config_a,
        store_dyn.clone(),
        store_dyn.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("worker a starts");
    let b = ravel_server::start(
        config_b,
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

    // Poll until BOTH workers report the full two-member live set. Let tA and
    // tB be the first heartbeat ticks of the earlier and the later worker
    // (`tokio::time::interval`'s first tick fires immediately, when the
    // spawned heartbeat task is first polled), g = tB - tA the gap between
    // the two loop starts, and H = `TEST_HEARTBEAT_INTERVAL`. The later
    // worker converges at tB: its first tick lists the earlier worker's
    // heartbeat, at most H old and so inside the 3H liveness window. The
    // earlier worker read a solo set at tA and ticks again only at
    // tA + k*H, so it converges on its first tick after tB, at
    // tA + (floor(g / H) + 1) * H <= tB + H = tA + g + H. The gap g is the
    // dominant term whenever the second `ravel_server::start` runs longer
    // than H, and the test does not bound it; but the poll below begins
    // only after that call returns, and on this current-thread runtime the
    // later worker's heartbeat task has run by the poll's first yield at the
    // latest, so from the poll's start the bound is H = 1s plus scheduler
    // delay on the heartbeat ticks. 100 iterations * 200ms sleep =
    // 20s total window: a 20x margin over that bound (unlike the pre-#1852
    // version of this test, which waited on the real 60s
    // `DEFAULT_HEARTBEAT_INTERVAL` with only a 2x margin).
    let mut converged = false;
    for _ in 0..100 {
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
         (100 iterations * 200ms = 20s): convergence is bounded by tA + g + H = tB + H, the \
         later worker's first heartbeat tick plus TEST_HEARTBEAT_INTERVAL = 1s, and tB falls \
         by the poll's first yield, so the window is a 20x margin over H"
    );

    // Poll until one scrape pair shows the final partition AND a completed
    // cold first tick on both workers. The partition sum alone is not that
    // state: the earlier worker's first heartbeat tick read a solo live set,
    // and if its first discovery cycle reads the live set before its own
    // second tick republishes it, that cycle claims all 24 units while the
    // later worker, whose first cycle has not run yet, still reports 0 owned
    // and 0 full sweeps. 24 + 0 matches `expected_total` with the later
    // worker never having ticked. Requiring `full_sweeps >= owned > 0` on each
    // side rules that state out: `units_owned` is written before a cycle's
    // sweeps run and a cold memo full-sweeps every owned unit, so the
    // inequality holds only once each worker has finished a cycle.
    //
    // The state is reached within one discovery cycle after convergence: the
    // later worker converged on its first heartbeat tick, before its first
    // cycle (due `interval` = 1s to 1.1s after its loop began, jitter
    // included), and the earlier worker's next cycle starts at most 1.1s
    // after its previous one ended. 100 iterations * 200ms = 20s is a margin
    // of more than 10x over that, and a scrape landing inside a cycle's
    // `set_units_owned(0)`-then-accumulate window only costs one more
    // iteration.
    let expected_total = u64::from(SHARD_COUNT) * 3;
    let owned_line = "ravel_maintain_units_owned{mode=\"maintain\"}";
    let sweeps_line = "ravel_maintain_full_sweep_passes_total{mode=\"maintain\"}";
    let cold_tick_done = |owned: Option<u64>, sweeps: Option<u64>| match (owned, sweeps) {
        (Some(owned), Some(sweeps)) => owned > 0 && sweeps >= owned,
        _ => false,
    };
    let mut observed = None;
    let mut settled = None;
    for _ in 0..100 {
        let body_a = scrape(&client, &base_a).await;
        let body_b = scrape(&client, &base_b).await;
        let owned_a = sample_value(&body_a, owned_line);
        let owned_b = sample_value(&body_b, owned_line);
        let sweeps_a = sample_value(&body_a, sweeps_line);
        let sweeps_b = sample_value(&body_b, sweeps_line);
        observed = Some((owned_a, sweeps_a, owned_b, sweeps_b));
        let joint = owned_a.zip(owned_b).map(|(a, b)| a + b);
        if joint == Some(expected_total)
            && cold_tick_done(owned_a, sweeps_a)
            && cold_tick_done(owned_b, sweeps_b)
        {
            settled = Some((body_a, body_b));
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let Some((body_a, body_b)) = settled else {
        let (owned_a, sweeps_a, owned_b, sweeps_b) = observed.unwrap_or_default();
        panic!(
            "within 100 iterations * 200ms = 20s of convergence, the two workers must jointly own \
             every (signal, shard) unit exactly once, no double-pay (ADR-0065 decision 2), and \
             each worker's cold-started first tick must have recorded at least one full sweep \
             pass per owned unit; last observed: worker a units_owned={owned_a:?} \
             full_sweep_passes_total={sweeps_a:?}, worker b units_owned={owned_b:?} \
             full_sweep_passes_total={sweeps_b:?}, expected units_owned total {expected_total}"
        );
    };

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

    a.shutdown().await.expect("graceful shutdown a");
    b.shutdown().await.expect("graceful shutdown b");
}

/// Build a single-worker `Mode::Maintain` config over `store`, `shard_count`
/// shards, a 1-second maintenance interval, and `compactor` in place of
/// `CompactorConfig::default()`. Used by the L0-pending/deleted-objects
/// acceptance tests below, which need a non-default `CompactorConfig` and have
/// no ownership-split concern that would need a second worker.
fn single_worker_maintain_config(
    tenant: &TenantId,
    shard_count: u32,
    compactor: ravel_maintain::CompactorConfig,
) -> ServerConfig {
    let mut config = maintain_config(tenant);
    config.shard_count = shard_count;
    config.maintain.shard_count = shard_count;
    config.maintain.compactor = compactor;
    config
}

/// Publish one real RSEG L0 segment plus its commit record into `(tenant,
/// Metrics, shard)`, for the historical ingest-hour bucket `hour` (an actual
/// past unix hour, not `publish_segment`'s far-future placeholder), so the
/// compactor's real seal and zone checks treat it as ready to evaluate rather
/// than skipping it as unsealed. `writer_seq` distinguishes multiple segments
/// published into the same bucket.
async fn publish_l0_segment(
    store: &MemoryStore,
    tenant: &TenantId,
    shard: u32,
    hour: u32,
    writer_seq: u64,
) {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(7);
    let created_unix_ns = i64::from(hour) * NS_PER_HOUR + 1_000_000_000;
    let labels = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: "cpu".to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant, "cpu", &labels).expect("series id");
    let series = vec![SeriesInput {
        series_id,
        labels,
        samples: vec![Sample {
            ts_ns: created_unix_ns,
            value: writer_seq as f64,
        }],
    }];
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
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
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq,
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
        ingest_hour_bucket: hour,
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

/// Issue #1729 acceptance test: the L0-pending gauge and the deleted-objects
/// counter are actually fed from the compaction scan and `SweepReport`, with
/// exact magnitudes, not merely present or nonzero.
///
/// Two historical (already-sealed) buckets on one shard:
/// - Bucket A gets exactly one L0 segment at the start (below
///   `DEFAULT_MIN_COMPACTION_INPUTS` = 2), so the first tick's scan reports it
///   `BelowMinInputs { count: 1 }` and `ravel_maintain_l0_records_pending`
///   reads exactly 1. A second segment is then published into the same
///   bucket, crossing the threshold; the next tick compacts it, and the
///   gauge must drop back to exactly 0 -- a drop of exactly the 1 L0 record
///   that had been sitting pending.
/// - Bucket B is compacted directly through `ravel_maintain::compact_bucket`
///   BEFORE the server starts, using a `FixedClock` set to shortly after that
///   bucket's own historical hour: the resulting compaction record's
///   `created_unix_ns` is old enough, relative to the real wall clock the
///   running server's sweep uses, that rule 2's protection horizon
///   (`DEFAULT_PROTECTION_HORIZON_NS`, about 25h) has already elapsed by the
///   time the server's first tick runs. That tick's sweep therefore deletes
///   bucket B's two now-superseded L0 commit records and their two data
///   objects immediately, without the test waiting out the horizon in real
///   time, and `ravel_maintain_objects_deleted_total` must read exactly 2 for
///   both the `superseded_records_deleted` and `superseded_data_deleted`
///   kinds, and exactly 0 for the other two kinds (nothing orphaned or
///   unreferenced in this scenario).
///
/// Bucket A's own fresh compaction (mid-test) is deliberately never swept
/// within this test's window: its compaction record's `created_unix_ns` is
/// the real time the tick ran at, so rule 2's horizon has not elapsed. The
/// final assertion checks the deleted-objects counters are UNCHANGED after
/// that compaction, which would catch an implementation that (wrongly)
/// raised them on compaction rather than on an actual sweep-confirmed
/// delete.
///
/// `interior_reverify_ns` is set to 0 in this test's `CompactorConfig`: the
/// default 6-hour re-verify cadence would let the memo mark bucket A
/// terminal after the first tick and skip re-listing it, missing the second
/// segment for 6 real hours. Zero disables that skip entirely
/// (`MaintainMemo::is_fresh_terminal` is never fresh at a non-positive
/// interval) and also makes `full_sweep_due` true on every tick, so the
/// unscoped sweep (which is what reaches bucket B, outside this tick's
/// head/tail zone) runs every time rather than only on a cold memo.
#[tokio::test]
async fn one_tick_drops_pending_by_compacted_count_and_raises_deleted_by_swept_count() {
    let tenant = TenantId::new("l0-pending-deleted-e2e");
    let store = Arc::new(MemoryStore::new());

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos() as i64;
    let current_hour = (now_ns / NS_PER_HOUR) as u32;
    let bucket_a_hour = current_hour - 35;
    let bucket_b_hour = current_hour - 40;

    // Bucket A: one L0 segment, below the default min_compaction_inputs (2).
    publish_l0_segment(store.as_ref(), &tenant, 0, bucket_a_hour, 1).await;

    // Bucket B: two L0 segments, then compacted offline with a historical
    // clock so its compaction record is already outside the protection
    // horizon relative to the real clock the running server will use.
    publish_l0_segment(store.as_ref(), &tenant, 0, bucket_b_hour, 101).await;
    publish_l0_segment(store.as_ref(), &tenant, 0, bucket_b_hour, 102).await;

    let compactor = ravel_maintain::CompactorConfig {
        interior_reverify_ns: 0,
        ..ravel_maintain::CompactorConfig::default()
    };
    let bucket_b = ravel_maintain::Bucket::new(tenant.hash(), Signal::Metrics, 0, bucket_b_hour);
    let pre_compact_clock = ravel_maintain::FixedClock::new(
        bucket_b.end_ns() + compactor.seal_margin_ns() + 60_000_000_000,
    );
    let pre_compact_outcome = ravel_maintain::compact_bucket(
        store.as_ref(),
        &pre_compact_clock,
        &ravel_maintain::CompactorConfig::default(),
        &bucket_b,
    )
    .await
    .expect("pre-compact bucket B");
    assert!(
        matches!(
            pre_compact_outcome,
            ravel_maintain::CompactionOutcome::Compacted { .. }
        ),
        "bucket B must actually compact its two L0 inputs before the server ever starts: {pre_compact_outcome:?}"
    );

    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
    let server = ravel_server::start(
        single_worker_maintain_config(&tenant, 1, compactor),
        store_dyn.clone(),
        store_dyn.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");

    let client = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
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

    const PENDING_LINE: &str =
        "ravel_maintain_l0_records_pending{mode=\"maintain\",signal=\"metrics\"}";
    const RECORDS_DELETED_LINE: &str = "ravel_maintain_objects_deleted_total{mode=\"maintain\",kind=\"superseded_records_deleted\"}";
    const DATA_DELETED_LINE: &str =
        "ravel_maintain_objects_deleted_total{mode=\"maintain\",kind=\"superseded_data_deleted\"}";
    const QUARANTINE_LINE: &str =
        "ravel_maintain_objects_deleted_total{mode=\"maintain\",kind=\"quarantine_reaped\"}";
    const UNREFERENCED_PARTS_LINE: &str = "ravel_maintain_objects_deleted_total{mode=\"maintain\",kind=\"unreferenced_parts_deleted\"}";

    // Poll for the first tick's baseline: bucket A pending at exactly 1
    // (BelowMinInputs{count: 1}) and bucket B's two superseded L0 records and
    // two data objects already swept, with the other two kinds untouched.
    let mut baseline = None;
    for _ in 0..100 {
        let body = scrape(&client, &base).await;
        let pending = sample_value(&body, PENDING_LINE);
        let records_deleted = sample_value(&body, RECORDS_DELETED_LINE);
        let data_deleted = sample_value(&body, DATA_DELETED_LINE);
        if pending == Some(1) && records_deleted == Some(2) && data_deleted == Some(2) {
            baseline = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let baseline = baseline.expect(
        "within the polling window, one tick must report l0_records_pending == 1 for bucket A \
         and objects_deleted_total == 2 for both superseded kinds from bucket B",
    );
    assert_eq!(
        sample_value(&baseline, QUARANTINE_LINE),
        Some(0),
        "no orphan quarantine reaping occurs in this scenario"
    );
    assert_eq!(
        sample_value(&baseline, UNREFERENCED_PARTS_LINE),
        Some(0),
        "no unreferenced parts exist in this scenario"
    );

    // Cross bucket A's threshold: a second segment brings it to exactly
    // min_compaction_inputs (2).
    publish_l0_segment(store.as_ref(), &tenant, 0, bucket_a_hour, 2).await;

    let mut compacted = None;
    for _ in 0..100 {
        let body = scrape(&client, &base).await;
        if sample_value(&body, PENDING_LINE) == Some(0) {
            compacted = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let compacted = compacted.expect(
        "within the polling window, a later tick must compact bucket A once it reaches \
         min_compaction_inputs, dropping l0_records_pending from 1 to exactly 0",
    );

    // Bucket A's own compaction just happened on the real wall clock, so its
    // two now-superseded L0 records are still well inside the protection
    // horizon: the deleted-objects counters must be exactly unchanged.
    assert_eq!(sample_value(&compacted, RECORDS_DELETED_LINE), Some(2));
    assert_eq!(sample_value(&compacted, DATA_DELETED_LINE), Some(2));
    assert_eq!(sample_value(&compacted, QUARANTINE_LINE), Some(0));
    assert_eq!(sample_value(&compacted, UNREFERENCED_PARTS_LINE), Some(0));

    server.shutdown().await.expect("graceful shutdown");
}

/// One pending bucket of the fixture below: `(tenant, shard, hours before the
/// current hour, L0 records published into it)`.
struct PendingBucket {
    tenant: &'static str,
    shard: u32,
    hours_ago: u32,
    records: u64,
}

/// Four buckets, four different record counts, two shards, two tenants. Every
/// count stays below `MIN_COMPACTION_INPUTS` below, so no bucket ever compacts
/// and the population is constant for the whole test.
const PENDING_FIXTURE: [PendingBucket; 4] = [
    PendingBucket {
        tenant: "l0-sum-e2e-a",
        shard: 0,
        hours_ago: 35,
        records: 1,
    },
    PendingBucket {
        tenant: "l0-sum-e2e-a",
        shard: 0,
        hours_ago: 36,
        records: 2,
    },
    PendingBucket {
        tenant: "l0-sum-e2e-a",
        shard: 1,
        hours_ago: 37,
        records: 3,
    },
    PendingBucket {
        tenant: "l0-sum-e2e-b",
        shard: 1,
        hours_ago: 38,
        records: 4,
    },
];

/// `min_compaction_inputs` for the fixture above: high enough that a bucket can
/// hold 1, 2, 3 or 4 L0 records and still report `BelowMinInputs { count }`
/// with that exact count. At the default of 2 every pending bucket holds
/// exactly one record, so a per-bucket sum, "the last bucket wins" and a
/// hardcoded 1 are indistinguishable.
const MIN_COMPACTION_INPUTS: usize = 5;

/// Issue #1729 acceptance test for the gauge's AGGREGATION, which the
/// exact-magnitude test above cannot reach with its single one-record bucket.
///
/// The fixture is four buckets holding 1, 2, 3 and 4 L0 records, spread over
/// two shards of two tenants, so `ravel_maintain_l0_records_pending{signal=
/// "metrics"}` must read exactly 10 (1+2+3+4) on this process: the gauge is a
/// per-process total over every owned `(tenant, shard)`, not one unit's last
/// value. Each of the three wrong answers this fixture was built to separate
/// reads differently: "the last unit written wins" reads 1, 2, 3 or 4 depending
/// on which unit the tick visited last, a per-tenant total reads 6 or 4, and a
/// hardcoded 1 reads 1.
///
/// `interior_reverify_ns` is left at its DEFAULT (6 h), unlike the test above
/// which disables it. That is the point of this test: after the first tick the
/// memo marks every one of these below-threshold interior buckets terminal, and
/// each later tick skips it without evaluating it. The gauge must still read
/// exactly 10 on those ticks, so the hold loop below re-reads it across several
/// maintenance intervals (1 s each) and requires the exact total every time. A
/// sum built only from buckets a tick actually evaluated reads 0 there, which
/// is the sawtooth an operator would see for 71 of every 72 ticks at the
/// production defaults.
///
/// `ravel_maintain_full_sweep_passes_total` is read alongside as the proof that
/// the hold window really is on the memo cadence rather than still cold: it
/// counts one pass per owned unit on the cold first tick and then nothing until
/// `interior_reverify_ns` elapses, so a frozen counter across the window means
/// the memo's skip path is what those ticks took.
#[tokio::test]
async fn l0_pending_gauge_sums_every_owned_bucket_including_memo_skipped_ones() {
    let tenant_a = TenantId::new(PENDING_FIXTURE[0].tenant);
    let store = Arc::new(MemoryStore::new());

    // The fixture's whole point is that it spans more than one shard and more
    // than one tenant, so a per-unit or per-tenant gauge reads short of the
    // total. Pin that shape here: a later edit that collapses it to one shard
    // or one tenant fails at this line rather than silently weakening every
    // assertion below.
    let mut tenants: Vec<&str> = PENDING_FIXTURE.iter().map(|b| b.tenant).collect();
    tenants.sort_unstable();
    tenants.dedup();
    let mut shards: Vec<u32> = PENDING_FIXTURE.iter().map(|b| b.shard).collect();
    shards.sort_unstable();
    shards.dedup();
    assert_eq!(tenants.len(), 2, "the fixture must span two tenants");
    assert_eq!(shards.len(), 2, "the fixture must span two shards");

    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos() as i64;
    let current_hour = (now_ns / NS_PER_HOUR) as u32;

    // `writer_seq` is unique across the whole fixture, not just within a
    // bucket: the L0 data key is built from (tenant, signal, shard, writer id,
    // epoch, seq, content hash) with no ingest hour in it, so a repeated seq
    // would rest on the segment bodies differing to stay distinct.
    let mut writer_seq = 0u64;
    let mut expected_total = 0u64;
    for bucket in &PENDING_FIXTURE {
        let tenant = TenantId::new(bucket.tenant);
        for _ in 0..bucket.records {
            writer_seq += 1;
            publish_l0_segment(
                store.as_ref(),
                &tenant,
                bucket.shard,
                current_hour - bucket.hours_ago,
                writer_seq,
            )
            .await;
        }
        expected_total += bucket.records;
    }
    assert_eq!(
        expected_total, 10,
        "the fixture's four buckets hold 1+2+3+4 L0 records"
    );

    let compactor = ravel_maintain::CompactorConfig {
        min_compaction_inputs: MIN_COMPACTION_INPUTS,
        ..ravel_maintain::CompactorConfig::default()
    };
    let mut config = single_worker_maintain_config(&tenant_a, 2, compactor);
    // `fold_tenants` is what the server hands maintenance as the fallback
    // allow-list for tenants carrying no config record, and both of the
    // fixture's tenants are that kind. With only tenant A on the list the second
    // tenant is never maintained and the gauge reads 6 rather than 10.
    config.fold_tenants = tenants
        .iter()
        .map(|name| TenantId::new(*name).hash())
        .collect();
    let store_dyn: Arc<dyn ObjectStoreBackend> = store.clone();
    let server = ravel_server::start(
        config,
        store_dyn.clone(),
        store_dyn.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");

    let client = reqwest::Client::new();
    let base = format!("http://{}", server.http_addr);
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

    const PENDING_LINE: &str =
        "ravel_maintain_l0_records_pending{mode=\"maintain\",signal=\"metrics\"}";
    const FULL_SWEEP_LINE: &str = "ravel_maintain_full_sweep_passes_total{mode=\"maintain\"}";

    // Tenant discovery has to find both tenants and one cycle has to cover
    // every owned unit before the total is complete, so poll for the exact
    // figure rather than reading the first scrape.
    let mut first_complete = None;
    // Kept for the failure message: a gauge that settles on a wrong figure says
    // which wrong answer it is (one unit, one tenant, or a hardcoded 1), which a
    // bare "never reached 10" does not.
    let mut observed: Vec<Option<u64>> = Vec::new();
    for _ in 0..100 {
        let body = scrape(&client, &base).await;
        let value = sample_value(&body, PENDING_LINE);
        if observed.last() != Some(&value) {
            observed.push(value);
        }
        if value == Some(expected_total) {
            first_complete = Some(body);
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let first_complete = first_complete.unwrap_or_else(|| {
        panic!(
            "within the polling window one maintenance cycle must publish the whole owned \
             population, 1+2+3+4 = 10 L0 records across two shards of two tenants, as a single \
             ravel_maintain_l0_records_pending{{signal=\"metrics\"}} sample; \
             observed instead: {observed:?}"
        )
    });
    let full_sweeps_cold = sample_value(&first_complete, FULL_SWEEP_LINE)
        .expect("full_sweep_passes_total present once maintenance has ticked");

    // Hold across several more 1 s maintenance intervals. Every one of these
    // ticks skips all four buckets through the memo (`interior_reverify_ns` is
    // the default 6 h here), and the published total must be the same 10.
    for _ in 0..40 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let body = scrape(&client, &base).await;
        assert_eq!(
            sample_value(&body, PENDING_LINE),
            Some(expected_total),
            "a memo-skipped below-threshold bucket must keep contributing its last known L0 \
             record count, so the gauge stays at the full population of 10 instead of \
             sawtoothing toward 0 between re-verifies"
        );
    }

    let settled = scrape(&client, &base).await;
    assert_eq!(
        sample_value(&settled, FULL_SWEEP_LINE),
        Some(full_sweeps_cold),
        "the hold window above must sit on the memo cadence: no unit may have taken another \
         full sweep pass, which is what proves those ticks skipped the fixture's buckets \
         instead of re-evaluating them"
    );

    server.shutdown().await.expect("graceful shutdown");
}

/// Render the real `/metrics` exposition for a `Mode::Maintain` process whose
/// only populated family is the maintenance-safety one, from `safety` as the
/// handler snapshots it on every scrape.
fn render_safety_exposition(safety: &ravel_server::maintain::MaintenanceSafetyMetrics) -> String {
    use ravel_server::metrics::{
        AdmissionCountersSnapshot, CatalogCountersSnapshot, IngestBufferBudgetSnapshot,
        MaintenanceSafetySnapshot, MemoryBudgetSnapshot,
    };
    let snapshot = MaintenanceSafetySnapshot::from_metrics(safety);
    ravel_server::metrics::render(
        Mode::Maintain,
        &ravel_object_store::instrument::StoreMetricsSnapshot::default(),
        &[],
        &CatalogCountersSnapshot::default(),
        None,
        Some(&snapshot),
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        &AdmissionCountersSnapshot::default(),
        &[],
        0,
        IngestBufferBudgetSnapshot::default(),
        None,
        None,
        &[],
        None,
        ravel_server::mem_stats::AllocatorStats::Other { name: "test" },
        None,
        None,
        None,
        None,
        None,
        MemoryBudgetSnapshot::default(),
        false,
    )
}

/// The clock the fixture below starts from: half past hour 500_000, so every
/// bucket hour it names is a whole number of hours before it.
const TICK_1_NS: i64 = 500_000 * NS_PER_HOUR + NS_PER_HOUR / 2;
/// The second tick's clock: 48 h later, past rule 2's protection horizon
/// (about 25 h), rule 3's part age gate (25 h) and the tombstone's own
/// protection horizon, and still inside the quarantine horizon's reach of the
/// seeded quarantine object's timestamp (see below).
const TICK_2_NS: i64 = TICK_1_NS + 48 * NS_PER_HOUR;
/// The retention window the fixture tenant carries: 30 days.
const RETENTION_WINDOW_NS: i64 = 30 * 24 * NS_PER_HOUR;
/// L0 depth of the bucket that compacts on tick 2.
const COMPACTING_DEPTH: u64 = 3;
/// L0 depth of the bucket that stays below threshold on both ticks.
const RESIDENT_DEPTH: u64 = 1;
/// Bytes of the unreferenced L1 part rule 3 deletes on tick 2.
const UNREFERENCED_PART_LEN: usize = 1_000;
/// Bytes of the quarantined object the reaper deletes on tick 2.
const QUARANTINED_LEN: usize = 234;

/// Issue #1729 acceptance test for all four throughput and backlog families,
/// driven by two deterministic ticks of [`ravel_server::maintain::run_tick_with_clock`]
/// under a [`ravel_maintain::FixedClock`] and read back from the real
/// exposition. No wall-clock wait: each tick runs to completion before its
/// assertions, and the clock only moves between ticks.
///
/// Fixture on one tenant, `Metrics` shard 0:
/// - bucket A, 35 h before tick 1: `COMPACTING_DEPTH` (3) L0 records;
/// - bucket C, 36 h before tick 1: `RESIDENT_DEPTH` (1) L0 record;
/// - bucket B, 40 h before tick 1: two L0 records compacted offline one hour
///   before tick 1, plus one unreferenced L1 part of `UNREFERENCED_PART_LEN`
///   bytes seeded after that compaction;
/// - bucket R, 31 days before tick 1: one L0 record, past the tenant's 30-day
///   retention window;
/// - one quarantined object of `QUARANTINED_LEN` bytes, quarantined one hour
///   short of the quarantine horizon at tick 1.
///
/// Tick 1 runs at `min_compaction_inputs = 4`, so A (3) and C (1) are both
/// pending: the gauge reads exactly 4. Nothing is old enough to sweep yet, so
/// every deleted and reclaimed figure reads 0. R is tombstoned, still present,
/// and its hour's retention deadline is `(hour + 1) h + 30 d`, so the lag reads
/// exactly 23.5 h = 84600 s.
///
/// Tick 2 runs 48 h later at `min_compaction_inputs = 2`. A compacts its three
/// records; C stays below threshold. The pending gauge must drop by exactly
/// the compacted count, 4 to 1. B's compaction record is now past the
/// protection horizon, so rule 2 deletes exactly its two input records and
/// their two data objects; the part and the quarantined object are past their
/// own gates, so rule 3 and the reaper delete exactly one each. The reclaimed
/// bytes counter must rise by exactly `UNREFERENCED_PART_LEN + QUARANTINED_LEN`:
/// it sums the listed sizes of those two deletions and nothing else. R's
/// tombstone is past its horizon, the bucket is physically swept, and no
/// expired bucket is left present, so the lag gauge falls to 0.
///
/// Raising `min_compaction_inputs` between ticks is the one way to compact a
/// bucket's whole pending depth in a single tick without publishing a new
/// record into it, which would make the drop differ from the compacted count.
#[tokio::test]
async fn one_clocked_tick_moves_every_backlog_and_throughput_figure_by_the_exact_fixture_count() {
    use ravel_maintain::{
        CompactorConfig, FixedClock, MaintainMemo, RetentionConfig, RetentionPolicy, WorkerSet,
    };
    use ravel_server::maintain::{MaintenanceOwnershipMetrics, MaintenanceSafetyMetrics};

    let tenant = TenantId::new("throughput-e2e");
    let tenant_hash = tenant.hash();
    let store = Arc::new(MemoryStore::new());
    // Every seeded object's last-modified time is tick 1, so rule 3's age gate
    // on the unreferenced part is shut on tick 1 and open on tick 2.
    store.set_clock_ms((TICK_1_NS / 1_000_000) as u64);

    let tick_1_hour = (TICK_1_NS / NS_PER_HOUR) as u32;
    let hour_a = tick_1_hour - 35;
    let hour_c = tick_1_hour - 36;
    let hour_b = tick_1_hour - 40;
    let hour_r = tick_1_hour - 31 * 24;

    let mut writer_seq = 0u64;
    for (hour, depth) in [
        (hour_a, COMPACTING_DEPTH),
        (hour_c, RESIDENT_DEPTH),
        (hour_b, 2),
        (hour_r, 1),
    ] {
        for _ in 0..depth {
            writer_seq += 1;
            publish_l0_segment(store.as_ref(), &tenant, 0, hour, writer_seq).await;
        }
    }

    let bucket_b = ravel_maintain::Bucket::new(tenant_hash, Signal::Metrics, 0, hour_b);
    let offline = ravel_maintain::compact_bucket(
        store.as_ref(),
        &FixedClock::new(TICK_1_NS - NS_PER_HOUR),
        &CompactorConfig::default(),
        &bucket_b,
    )
    .await
    .expect("pre-compact bucket B");
    assert!(
        matches!(offline, ravel_maintain::CompactionOutcome::Compacted { .. }),
        "bucket B must compact its two L0 inputs before tick 1: {offline:?}"
    );

    let part_key = keys::l1_part_key(
        &tenant_hash,
        Signal::Metrics,
        0,
        hour_b,
        "00000000000000ff",
        7,
        "00000000000000ee",
    )
    .expect("part key");
    store
        .put(
            &part_key,
            vec![0u8; UNREFERENCED_PART_LEN].into(),
            PutOptions::default(),
        )
        .await
        .expect("put unreferenced part");

    let quarantined_original = keys::data_key(
        &tenant_hash,
        Signal::Metrics,
        0,
        Uuid::from_u128(99),
        1,
        1,
        &[9u8; 32],
    )
    .expect("data key");
    let quarantined_at_ns =
        TICK_1_NS - ravel_maintain::config::DEFAULT_QUARANTINE_HORIZON_NS + NS_PER_HOUR;
    store
        .put(
            &format!("quarantine/{quarantined_original}/q{quarantined_at_ns:020}"),
            vec![0u8; QUARANTINED_LEN].into(),
            PutOptions::default(),
        )
        .await
        .expect("put quarantined object");

    let tick_config = |min_compaction_inputs: usize| CompactorConfig {
        min_compaction_inputs,
        interior_reverify_ns: 0,
        ..CompactorConfig::default()
    };
    let retention = RetentionConfig::from_policy(
        RetentionPolicy {
            default: None,
            tenants: vec![(tenant.as_str().to_string(), RETENTION_WINDOW_NS)],
        },
        &CompactorConfig::default(),
        ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS,
    )
    .expect("valid retention config");

    let mut memo = MaintainMemo::new(0);
    let safety = MaintenanceSafetyMetrics::default();
    let ownership = MaintenanceOwnershipMetrics::new(3);
    let worker = WorkerSet::with_defaults(0);
    let live_set = worker.solo_live_set();

    const PENDING_LINE: &str =
        "ravel_maintain_l0_records_pending{mode=\"maintain\",signal=\"metrics\"}";
    const LAG_LINE: &str =
        "ravel_maintain_retention_lag_seconds{mode=\"maintain\",signal=\"metrics\"}";
    const BYTES_LINE: &str =
        "ravel_maintain_bytes_reclaimed_total{mode=\"maintain\",signal=\"metrics\"}";
    const DELETED_KINDS: [&str; 4] = [
        "superseded_records_deleted",
        "superseded_data_deleted",
        "unreferenced_parts_deleted",
        "quarantine_reaped",
    ];
    let deleted = |body: &str, kind: &str| {
        sample_value(
            body,
            &format!("ravel_maintain_objects_deleted_total{{mode=\"maintain\",kind=\"{kind}\"}}"),
        )
    };

    // Tick 1.
    safety.begin_scan_cycle();
    let report_1 = ravel_server::maintain::run_tick_with_clock(
        &FixedClock::new(TICK_1_NS),
        store.as_ref(),
        &tenant_hash,
        &tick_config(4),
        &retention,
        1,
        &mut memo,
        &safety,
        &ownership,
        &worker,
        &live_set,
    )
    .await;
    safety.publish_scan_cycle();
    assert_eq!(
        report_1.retired, 1,
        "tick 1 tombstones bucket R: {report_1:?}"
    );
    assert_eq!(
        report_1.compacted, 0,
        "nothing reaches threshold 4: {report_1:?}"
    );

    let body_1 = render_safety_exposition(&safety);
    let pending_1 = sample_value(&body_1, PENDING_LINE).expect("pending sample");
    assert_eq!(
        pending_1,
        COMPACTING_DEPTH + RESIDENT_DEPTH,
        "tick 1: buckets A and C are both below threshold 4"
    );
    for kind in DELETED_KINDS {
        assert_eq!(
            deleted(&body_1, kind),
            Some(0),
            "tick 1: nothing is old enough to delete ({kind})"
        );
    }
    assert_eq!(
        sample_value(&body_1, BYTES_LINE),
        Some(0),
        "tick 1: nothing reclaimed"
    );
    let deadline_r = (i64::from(hour_r) + 1) * NS_PER_HOUR + RETENTION_WINDOW_NS;
    assert_eq!(TICK_1_NS - deadline_r, 84_600_000_000_000);
    assert_eq!(
        sample_value(&body_1, LAG_LINE),
        Some(84_600),
        "tick 1: bucket R is tombstoned and still present, 23.5 h past its deadline"
    );

    // Tick 2.
    safety.begin_scan_cycle();
    let report_2 = ravel_server::maintain::run_tick_with_clock(
        &FixedClock::new(TICK_2_NS),
        store.as_ref(),
        &tenant_hash,
        &tick_config(2),
        &retention,
        1,
        &mut memo,
        &safety,
        &ownership,
        &worker,
        &live_set,
    )
    .await;
    safety.publish_scan_cycle();
    assert_eq!(
        report_2.compacted, 1,
        "tick 2 compacts bucket A: {report_2:?}"
    );

    let body_2 = render_safety_exposition(&safety);
    let pending_2 = sample_value(&body_2, PENDING_LINE).expect("pending sample");
    assert_eq!(
        pending_1.checked_sub(pending_2),
        Some(COMPACTING_DEPTH),
        "tick 2: the pending gauge drops by exactly bucket A's compacted depth"
    );
    assert_eq!(
        pending_2, RESIDENT_DEPTH,
        "bucket C is still pending at threshold 2"
    );
    for (kind, expected) in [
        ("superseded_records_deleted", 2),
        ("superseded_data_deleted", 2),
        ("unreferenced_parts_deleted", 1),
        ("quarantine_reaped", 1),
    ] {
        assert_eq!(
            deleted(&body_2, kind).and_then(|now| now.checked_sub(deleted(&body_1, kind)?)),
            Some(expected),
            "tick 2: the deleted counter for {kind} rises by exactly the swept count"
        );
    }
    assert_eq!(
        sample_value(&body_2, BYTES_LINE)
            .and_then(|now| now.checked_sub(sample_value(&body_1, BYTES_LINE)?)),
        Some((UNREFERENCED_PART_LEN + QUARANTINED_LEN) as u64),
        "tick 2: reclaimed bytes rise by exactly the listed sizes of the deleted part and the \
         reaped quarantine object"
    );
    assert_eq!(
        sample_value(&body_2, LAG_LINE),
        Some(0),
        "tick 2: bucket R is swept, so no expired bucket is left present"
    );
}
