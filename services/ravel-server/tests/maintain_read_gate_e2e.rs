//! ADR-1702 task 10 reachability: a `Mode::Maintain` server built by
//! [`ravel_server::start`] runs compaction's decode and re-encode on its read
//! CPU gate.
//!
//! Two L0 segments land in one sealed hour, each holding the same series with
//! pseudo-random sample values, so their pages do not compress below the gate's
//! default 256 KiB inline floor. The server's maintenance loop compacts the
//! bucket on its own one-second tick. The read gate's `compaction` counters on
//! `/metrics` are read once the compaction record exists.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::{Mode, ServerConfig};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Samples per segment: enough pseudo-random doubles that the one merged
/// series' pages, and the part encoded from them, clear the inline floor.
const SAMPLES: i64 = 40_000;

fn maintain_config(tenant: &TenantId) -> ServerConfig {
    ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::Maintain,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(Default::default(), false),
        mtls_listener: None,
        fold_tenants: vec![tenant.hash()],
        fold: ravel_server::FoldTaskConfig {
            enabled: false,
            ..ravel_server::FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig {
            enabled: true,
            interval: Duration::from_secs(1),
            shard_count: 1,
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
        scrub_period: Duration::from_secs(7 * 86_400),
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
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    }
}

/// Publishes one L0 segment of the `gate_e2e` series into `hour`, returning
/// its object size.
async fn publish_segment(
    store: &MemoryStore,
    tenant: &TenantId,
    hour: u32,
    writer_seq: u64,
) -> u64 {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(7);
    let created_unix_ns = i64::from(hour) * NS_PER_HOUR + 1_000_000_000;
    let labels = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: "gate_e2e".to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant, "gate_e2e", &labels).expect("series id");
    let mut x = 0x9E37_79B9_7F4A_7C15_u64 ^ writer_seq;
    let samples = (0..SAMPLES)
        .map(|i| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            Sample {
                ts_ns: created_unix_ns + i * 2 + writer_seq as i64,
                value: f64::from_bits((x >> 12) | 0x3ff0_0000_0000_0000),
            }
        })
        .collect();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
    };
    let (min_ingest_ts_ns, max_ingest_ts_ns) = (created_unix_ns - 1_000, created_unix_ns);
    let bounds = IngestBounds {
        min_ingest_ts_ns,
        max_ingest_ts_ns,
    };
    let written = SegmentWriter::write(
        vec![SeriesInput {
            series_id,
            labels,
            samples,
        }],
        identity,
        bounds,
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
    rec.object_size
}

/// The read gate's `(jobs, inline)` counts for `site`, each required to render
/// exactly once.
fn read_gate_site(body: &str, site: &str) -> (u64, u64) {
    let figure = |family: &str| {
        let name = format!("ravel_cpu_gate_{family}{{");
        let labels = format!("gate=\"read\",site=\"{site}\"}} ");
        let samples: Vec<&str> = body
            .lines()
            .filter(|line| line.starts_with(name.as_str()))
            .filter_map(|line| line.split_once(labels.as_str()).map(|(_, value)| value))
            .collect();
        assert_eq!(
            samples.len(),
            1,
            "{name}..{labels} must render once:\n{body}"
        );
        samples[0].parse::<u64>().expect("counter value")
    };
    (figure("jobs_total"), figure("inline_total"))
}

/// The maintenance loop's compaction of the two-segment bucket runs two units
/// on the read gate: the merged series' decode, merge and re-encode, and the
/// output part's encode, each over the floor. The two input catalog decodes
/// are a few hundred bytes each, so they run inline and count as two inline
/// `compaction` units.
///
/// Fails with the gate left off the compactor config in `ravel_server::start`:
/// the site then reads `(0, 0)`, since compaction consults no gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintain_compaction_runs_on_the_servers_read_gate() {
    let tenant = TenantId::new("maintain-read-gate-e2e");
    let store = Arc::new(MemoryStore::new());
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos() as i64;
    let hour = (now_ns / NS_PER_HOUR) as u32 - 5;
    let sizes = [
        publish_segment(store.as_ref(), &tenant, hour, 1).await,
        publish_segment(store.as_ref(), &tenant, hour, 2).await,
    ];
    assert!(
        sizes.iter().sum::<u64>() >= 2 * DEFAULT_INLINE_FLOOR_BYTES,
        "the fixture's pages must clear the floor: segments of {sizes:?} bytes"
    );

    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let server = ravel_server::start(
        maintain_config(&tenant),
        backend.clone(),
        backend,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");

    let prefix = keys::commit_shard_hour_prefix(&tenant.hash(), Signal::Metrics, 0, hour)
        .expect("bucket prefix");
    let mut compacted = false;
    for _ in 0..300 {
        let listed = list_all(store.as_ref(), &prefix)
            .await
            .expect("list bucket");
        if listed.iter().any(|meta| {
            matches!(
                keys::partition_bucket_entry(&meta.key),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        }) {
            compacted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(compacted, "the maintenance loop compacts the bucket");

    let body = reqwest::Client::new()
        .get(format!("http://{}/metrics", server.http_addr))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text");
    assert_eq!(read_gate_site(&body, "compaction"), (2, 2));

    server.shutdown().await.expect("server shuts down");
}
