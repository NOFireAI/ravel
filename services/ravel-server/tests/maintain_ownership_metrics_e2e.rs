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
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
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
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
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
    // before `units_owned` reflects the final partition (the maintenance
    // interval is 1s), and a single scrape pair can also race
    // `run_discovery_cycle`'s `set_units_owned(0)`-then-accumulate window
    // (maintain.rs): a scrape landing between one worker's reset and its
    // tenant loop finishing sees a transient undercount. Both are covered by
    // polling to the same total window this used to spend as a fixed 3s sleep
    // plus a shorter poll. A fixed sleep in front of a bounded poll only
    // lengthens the test: it cannot make a slow discovery cycle arrive, and
    // the poll already tolerates one.
    let expected_total = u64::from(SHARD_COUNT) * 3;
    let mut owned_a = 0;
    let mut owned_b = 0;
    let mut settled = false;
    for _ in 0..40 {
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

/// Build a single-worker `Mode::Maintain` config over `store`, one shard, a
/// 1-second maintenance interval, and `compactor` in place of
/// `CompactorConfig::default()`. Used only by the L0-pending/deleted-objects
/// acceptance test below, which needs `interior_reverify_ns` disabled (see
/// that test's own doc comment) and has no ownership-split concern that would
/// need more than one shard or worker.
fn single_worker_maintain_config(
    tenant: &TenantId,
    compactor: ravel_maintain::CompactorConfig,
) -> ServerConfig {
    ServerConfig {
        maintain: ravel_server::MaintenanceTaskConfig {
            shard_count: 1,
            compactor,
            ..maintain_config(tenant).maintain
        },
        shard_count: 1,
        ..maintain_config(tenant)
    }
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
        single_worker_maintain_config(&tenant, compactor),
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
