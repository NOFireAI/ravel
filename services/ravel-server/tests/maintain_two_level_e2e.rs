//! ADR-3853 task T5 reachability: a `Mode::Maintain` server built from parsed
//! `--maintain-*` flags runs the two-level RLOG merge on a bucket the
//! single-level merge refuses, and counts each refusal under its own reason on
//! `/metrics`.
//!
//! The bucket is 60 L0 RLOG inputs in one sealed hour, each one block of 2000
//! records over the same 20 streams, so a cursor merge decodes every block once
//! per stream: 20 rows per input record, past the limit of 16. The records
//! spread 200 attribute keys, so every input's block prices at about 7 MB
//! decoded and the 60 blocks one stream's cursors hold at once (about 436 MB)
//! outgrow the unit ceiling `--maintain-unit-concurrency 60` gives (20 GiB /
//! 60, 357913941 bytes): the single-level plan is a cursor merge. Level 1 fits
//! groups of 12 inputs beside the fixed 256 MiB run target under that ceiling,
//! and each group's run is 24000 stream-ordered records cut into blocks of a
//! few streams each, so level 2's largest stream estimate (about 295 MB) fits.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{AttrValue, LogRecord, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_maintain::CompactionRefusedReason;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_server::config::{Cli, HostProfile};
use ravel_server::{Mode, ServerConfig};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const INPUTS: u32 = 60;
const STREAMS: u32 = 20;
const RECORDS_PER_INPUT: u32 = 2_000;
/// Distinct attribute keys per input, under `RlogConfig`'s 1000 dynamic
/// columns so each key is a column of its own.
const ATTR_KEYS: u32 = 200;
const ATTRS_PER_RECORD: u32 = 3;
/// The flag that forces the two-level merge: the unit ceiling is the 20 GiB
/// cursor budget over it.
const UNIT_CONCURRENCY: &str = "60";
const SCRATCH_LOCK_FILE: &str = "ravel-scratch.lock";

/// The refusal counters are process-wide, so the tests that read them run one
/// at a time.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// The `CompactorConfig` `ravel-server` resolves from `flags` the way its
/// `main` does, on a 16-core 32 GiB host, with the ADR-3853 D0 floor off: the
/// bucket predicts 2.4 million decoded rows, under the production floor.
fn compactor(flags: &[&str]) -> ravel_maintain::CompactorConfig {
    let mut argv = vec![
        "ravel-server",
        "--mode",
        "maintain",
        "--maintain-unit-concurrency",
        UNIT_CONCURRENCY,
        "--maintain-l1-part-memory-target-bytes",
        "16777216",
    ];
    argv.extend_from_slice(flags);
    let cli = Cli::try_parse_from(argv).expect("flags parse");
    let host = HostProfile::new(16, Some(32 << 30), Some(32 << 30), None, None, None);
    let performance = cli.resolve_performance(host).expect("performance");
    let gc_runtime = cli
        .resolve_gc_runtime(performance.query_deadline)
        .expect("gc runtime");
    ravel_maintain::CompactorConfig {
        geometry_floor_decoded_rows: 0,
        ..cli
            .resolve_compactor_config(&gc_runtime, &performance)
            .expect("compactor config")
    }
}

fn maintain_config(tenant: &TenantId, compactor: ravel_maintain::CompactorConfig) -> ServerConfig {
    ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
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
            unit_concurrency: UNIT_CONCURRENCY.parse().expect("concurrency"),
            compactor,
            worker_record_keys: Some(
                ravel_maintain::worker_set::MaintainRecordKeys::from_file_keys(&[[0x5C; 32]])
                    .expect("one maintain key"),
            ),
            ..ravel_server::MaintenanceTaskConfig::default()
        },
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
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
        cpu_gate_permits: ravel_server::config::CpuGatePermits {
            read: 2,
            write: 1,
            ..Default::default()
        },
        ingest_hold: Default::default(),
        max_pending_strict_acks: 8_192,
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

/// Input `input`'s records: stream `j % STREAMS` for record `j`, each carrying
/// `ATTRS_PER_RECORD` of the `ATTR_KEYS` keys, all inside `hour`.
fn input_records(hour: u32, input: u32) -> Vec<LogRecord> {
    let base = i64::from(hour) * NS_PER_HOUR + 1_000_000_000;
    (0..RECORDS_PER_INPUT)
        .map(|j| {
            let stream = j % STREAMS;
            let resource = vec![(
                "service.name".to_string(),
                AttrValue::Str(format!("svc{stream}")),
            )];
            let ts = base + i64::from(j) * 1_000 + i64::from(input);
            LogRecord {
                stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1", &[]),
                stream_attrs: ravel_logseg::stream_attrs_bytes(&resource, "scope", "1", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("in{input} r{j}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: (0..ATTRS_PER_RECORD)
                    .map(|t| {
                        let key = (j * ATTRS_PER_RECORD + t) % ATTR_KEYS;
                        (format!("a{key}"), AttrValue::I64(i64::from(j)))
                    })
                    .collect(),
            }
        })
        .collect()
}

/// Publishes the 60 inputs into `hour`, writer `i + 1` sequence 1 each,
/// returning their summed object sizes.
async fn publish_inputs(store: &MemoryStore, tenant: &TenantId, hour: u32) -> u64 {
    let tenant_hash = tenant.hash();
    let mut stored = 0u64;
    for input in 0..INPUTS {
        let records = input_records(hour, input);
        let writer_id = Uuid::from_u128(u128::from(input) + 1);
        let identity = ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity);
        for rec in &records {
            writer.push(rec.clone()).expect("push log record");
        }
        let bytes = writer.finish().expect("finish rlog object");
        let created_unix_ns = i64::from(hour) * NS_PER_HOUR + 2_000_000_000;
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: records.len() as u64,
            series_count: u64::from(STREAMS),
            min_event_ts_ns: records.first().expect("records").ts_ns,
            max_event_ts_ns: records.last().expect("records").ts_ns,
            min_ingest_ts_ns: created_unix_ns,
            max_ingest_ts_ns: created_unix_ns,
            segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns,
            ingest_hour_bucket: hour,
        })
        .expect("valid log commit record");
        stored += rec.object_size;
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put log data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish log commit");
    }
    stored
}

/// A sealed ingest hour, five hours back on the wall clock the server reads.
fn sealed_hour() -> u32 {
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock after epoch")
        .as_nanos() as i64;
    (now_ns / NS_PER_HOUR) as u32 - 5
}

/// The compaction record keys under the bucket.
async fn compaction_records(store: &MemoryStore, tenant: &TenantId, hour: u32) -> Vec<String> {
    let prefix = keys::commit_shard_hour_prefix(&tenant.hash(), Signal::Logs, 0, hour)
        .expect("bucket prefix");
    list_all(store, &prefix)
        .await
        .expect("list bucket")
        .into_iter()
        .filter(|meta| {
            matches!(
                keys::partition_bucket_entry(&meta.key),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        })
        .map(|meta| meta.key)
        .collect()
}

async fn scrape(addr: std::net::SocketAddr) -> String {
    reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// Every reason's rendered `ravel_maintain_compaction_refused_total` sample,
/// each required to render exactly once.
fn refused(body: &str) -> Vec<(CompactionRefusedReason, u64)> {
    CompactionRefusedReason::ALL
        .iter()
        .map(|&reason| {
            let prefix = format!(
                "ravel_maintain_compaction_refused_total{{mode=\"maintain\",reason=\"{}\"}} ",
                reason.name()
            );
            let samples: Vec<u64> = body
                .lines()
                .filter_map(|line| line.strip_prefix(prefix.as_str()))
                .map(|value| value.parse().expect("integer sample"))
                .collect();
            assert_eq!(samples.len(), 1, "{prefix} must render once:\n{body}");
            (reason, samples[0])
        })
        .collect()
}

/// The process-wide refusal totals, plus one under each reason in `plus`.
fn refused_now_plus(plus: &[CompactionRefusedReason]) -> Vec<(CompactionRefusedReason, u64)> {
    CompactionRefusedReason::ALL
        .iter()
        .map(|&reason| {
            let extra = plus.iter().filter(|r| **r == reason).count() as u64;
            (
                reason,
                ravel_maintain::compaction_refused_total(reason) + extra,
            )
        })
        .collect()
}

/// Everything under `dir` but the scratch lock file.
fn scratch_entries(dir: &Path) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(dir)
        .expect("read scratch dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| !p.ends_with(SCRATCH_LOCK_FILE))
        .collect()
}

/// Starts a maintain server over `store` with `compactor`, polls until
/// `done(store, body)` holds, and returns the last `/metrics` body. Panics
/// naming `waiting_for` and the last body when it has not held after two
/// minutes.
async fn run_until<F>(
    store: &Arc<MemoryStore>,
    tenant: &TenantId,
    compactor: ravel_maintain::CompactorConfig,
    waiting_for: &str,
    mut done: F,
) -> String
where
    F: AsyncFnMut(&MemoryStore, &str) -> bool,
{
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let server = ravel_server::start(
        maintain_config(tenant, compactor),
        backend.clone(),
        backend,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    let mut body = String::new();
    let mut held = false;
    for _ in 0..1_200 {
        body = scrape(server.http_addr).await;
        if done(store.as_ref(), &body).await {
            held = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    server.shutdown().await.expect("server shuts down");
    assert!(
        held,
        "timed out after 120 s (1200 polls 100 ms apart) waiting for {waiting_for}; \
         last /metrics body:\n{body}"
    );
    body
}

/// With `--maintain-scratch-dir`, the bucket the single-level merge refuses is
/// compacted in two levels: the tick publishes one compaction record, the
/// scratch directory holds only the lock the two-level merge took (each
/// unit's runs are wiped when it ends), and no reason's refusal counter moved.
///
/// Fails against a server that parses `--maintain-scratch-dir` but never sets
/// `CompactorConfig::scratch_dir`: the bucket is then refused under
/// `reason="geometry"` and no record is published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_maintain_server_compacts_a_many_input_bucket_in_two_levels() {
    let _serial = SERIAL.lock().await;
    let tenant = TenantId::new("maintain-two-level-e2e");
    let store = Arc::new(MemoryStore::new());
    let hour = sealed_hour();
    let stored = publish_inputs(store.as_ref(), &tenant, hour).await;
    let scratch = tempfile::tempdir().expect("scratch dir");
    let scratch_dir = scratch.path().to_str().expect("utf-8 path");
    let config = compactor(&["--maintain-scratch-dir", scratch_dir]);
    let expected = refused_now_plus(&[]);

    let body = run_until(
        &store,
        &tenant,
        config,
        "a compaction record or a refusal counter to move",
        async |store, body| {
            !compaction_records(store, &tenant, hour).await.is_empty() || refused(body) != expected
        },
    )
    .await;

    let records = compaction_records(store.as_ref(), &tenant, hour).await;
    assert_eq!(
        records.len(),
        1,
        "one compaction record: {records:?} {:?}",
        refused(&body)
    );
    println!(
        "two-level e2e: {INPUTS} inputs, {stored} stored bytes, record {}",
        records[0]
    );
    assert!(
        scratch.path().join(SCRATCH_LOCK_FILE).exists(),
        "the two-level merge locked the scratch directory"
    );
    assert_eq!(
        scratch_entries(scratch.path()),
        Vec::<std::path::PathBuf>::new()
    );
    assert_eq!(refused(&body), expected);
}

/// Without a scratch directory the same bucket is refused under ADR-3853 D0:
/// `reason="geometry"` moves by one, no other reason moves, and nothing is
/// published.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_maintain_server_without_scratch_refuses_with_the_geometry_reason() {
    let _serial = SERIAL.lock().await;
    let tenant = TenantId::new("maintain-two-level-geometry-e2e");
    let store = Arc::new(MemoryStore::new());
    let hour = sealed_hour();
    publish_inputs(store.as_ref(), &tenant, hour).await;
    let config = compactor(&[]);
    assert_eq!(config.scratch_dir, None);
    let before = refused_now_plus(&[]);
    let expected = refused_now_plus(&[CompactionRefusedReason::Geometry]);

    let body = run_until(
        &store,
        &tenant,
        config,
        "a refusal counter to move",
        async |_, body| refused(body) != before,
    )
    .await;

    assert_eq!(refused(&body), expected);
    assert_eq!(
        compaction_records(store.as_ref(), &tenant, hour).await,
        Vec::<String>::new()
    );
}

/// `--maintain-scratch-max-bytes` reaches the headroom admission: a cap far
/// below 1.3 times the inputs' stored bytes refuses the two-level merge under
/// `reason="scratch_headroom"`, not under `reason="geometry"`, and nothing is
/// published.
///
/// Fails against a counter that counts every refusal under `geometry`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_maintain_server_with_a_short_scratch_cap_refuses_with_the_scratch_headroom_reason() {
    let _serial = SERIAL.lock().await;
    let tenant = TenantId::new("maintain-two-level-headroom-e2e");
    let store = Arc::new(MemoryStore::new());
    let hour = sealed_hour();
    publish_inputs(store.as_ref(), &tenant, hour).await;
    let scratch = tempfile::tempdir().expect("scratch dir");
    let scratch_dir = scratch.path().to_str().expect("utf-8 path");
    let config = compactor(&[
        "--maintain-scratch-dir",
        scratch_dir,
        "--maintain-scratch-max-bytes",
        "1024",
    ]);
    assert_eq!(config.scratch_max_bytes, Some(1024));
    let before = refused_now_plus(&[]);
    let expected = refused_now_plus(&[CompactionRefusedReason::ScratchHeadroom]);

    let body = run_until(
        &store,
        &tenant,
        config,
        "a refusal counter to move",
        async |_, body| refused(body) != before,
    )
    .await;

    assert_eq!(refused(&body), expected);
    assert_eq!(
        compaction_records(store.as_ref(), &tenant, hour).await,
        Vec::<String>::new()
    );
    assert_eq!(
        scratch_entries(scratch.path()),
        Vec::<std::path::PathBuf>::new()
    );
}
