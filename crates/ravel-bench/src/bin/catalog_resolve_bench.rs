//! Catalog resolve benchmark (ADR-0020): measures `Catalog::resolve` cost
//! before and after a fold at 10^4-10^5 commit scale, and the segment-fetch
//! gate measurement (segment-fetch share of a selective query's total request
//! cost). Report-only: never changes ravel-catalog/ravel-query behavior, only
//! measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
// Default seal margins sum to 1h20m (max_flush_lifetime 1h + clock_skew_allowance
// 5m + fold_safety_margin 15m, crates/ravel-catalog/src/config.rs). 3h clears
// that margin regardless of hour quantization (matches the same padding used
// in crates/ravel-failure-tests/tests/folder_crash_matrix.rs and
// services/ravel-cli/tests/catalog.rs).
const SEAL_PADDING_NS: i64 = 3 * NS_PER_HOUR;
// Arbitrary base far enough from hour 0 that `hour - 1` arithmetic elsewhere
// in the workspace never underflows; the timeline is entirely synthetic
// (resolve/fold take `now_ns` as an explicit argument, never the system
// clock), so the actual value carries no meaning beyond being a fixed point.
const BASE_HOUR: u32 = 100_000;

fn hour_end_ns(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR
}

fn hour_start_ns(hour: u32) -> i64 {
    i64::from(hour) * NS_PER_HOUR
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum StoreKind {
    Memory,
    S3,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Scenario {
    All,
    Resolve,
    Gate,
}

#[derive(Parser, Debug)]
#[command(about = "Catalog resolve before/after fold benchmark")]
struct Args {
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    #[arg(long, value_enum, default_value_t = Scenario::All)]
    scenario: Scenario,
    /// Target total commit count for the resolve before/after scenario,
    /// spread evenly across `resolve_shards` x `resolve_hours` buckets.
    #[arg(long, default_value_t = 10_000)]
    resolve_commits: u64,
    #[arg(long, default_value_t = 4)]
    resolve_shards: u32,
    #[arg(long, default_value_t = 25)]
    resolve_hours: u32,
    /// Number of distinct single-series segments in the segment-fetch gate's one
    /// (shard, hour) bucket. The query matches exactly one of them, so
    /// selectivity is `1 / gate_segments`.
    #[arg(long, default_value_t = 200)]
    gate_segments: u32,
}

fn s3_config_from_env() -> S3Config {
    let get = |key: &str| std::env::var(key).unwrap_or_default();
    S3Config {
        bucket: get("RAVEL_S3_BUCKET"),
        region: get("RAVEL_S3_REGION"),
        endpoint: std::env::var("RAVEL_S3_ENDPOINT").ok(),
        access_key_id: get("RAVEL_S3_ACCESS_KEY_ID"),
        secret_access_key: get("RAVEL_S3_SECRET_ACCESS_KEY"),
        allow_http: std::env::var("RAVEL_S3_ALLOW_HTTP").as_deref() == Ok("true"),
        force_path_style: std::env::var("RAVEL_S3_FORCE_PATH_STYLE").as_deref() != Ok("false"),
        kms_key_id: None,
        session_token: None,
        credentials_file: None,
        auth: Default::default(),
        instance_metadata_endpoint: None,
    }
}

/// Store requests are the metric that matters for this benchmark (wall time
/// on `MemoryStore` is dominated by scheduling noise, not real I/O); this
/// wrapper counts GET/LIST/PUT calls without changing behavior. Not the
/// same type as `ravel_bench::read_accounting::CountingBackend`: that one is
/// gated behind the `parquet-baseline` feature (pulls in `object_store`,
/// `arrow`, `parquet`) and implements a different, Parquet-reader-facing
/// trait. This benchmark stays out of that feature entirely.
#[derive(Default)]
struct Counters {
    get: AtomicU64,
    list: AtomicU64,
    put: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.get.load(Ordering::Relaxed),
            self.list.load(Ordering::Relaxed),
            self.put.load(Ordering::Relaxed),
        )
    }
}

struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    counters: Arc<Counters>,
}

impl CountingStore {
    fn wrap(inner: Arc<dyn ObjectStoreBackend>) -> (Arc<dyn ObjectStoreBackend>, Arc<Counters>) {
        let counters = Arc::new(Counters::default());
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(CountingStore {
            inner,
            counters: Arc::clone(&counters),
        });
        (store, counters)
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.counters.put.fetch_add(1, Ordering::Relaxed);
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.counters.get.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.counters.list.fetch_add(1, Ordering::Relaxed);
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.counters.list.fetch_add(1, Ordering::Relaxed);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits.
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// Publishes one commit record with a small dummy payload, bypassing
/// `SegmentWriter` (no real RSEG encoding): the resolve before/after
/// scenario only exercises catalog listing/folding, never decode, so a
/// cheap payload keeps 10^5-scale populate wall time reasonable (matches
/// `services/ravel-cli/tests/catalog.rs`'s `publish_segment`, duplicated
/// here rather than imported since it is a test-only helper in a different
/// crate).
async fn publish_lightweight(
    store: &dyn ObjectStoreBackend,
    tenant_hash: TenantHash,
    shard: u32,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
) {
    let payload = format!("bench-{shard}-{ingest_hour_bucket}-{}", Uuid::new_v4()).into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let record = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id: Uuid::new_v4(),
        writer_epoch: 1,
        writer_seq: 0,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: created_unix_ns - 1_000,
        max_event_ts_ns: created_unix_ns,
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
        // format-version-literal-ok: this payload is arbitrary bench bytes, not
        // a real RSEG object, and the catalog-resolve gate never decodes it.
        // There is no writer whose footer this should track; a real segment is
        // published by `publish_real_segment` below.
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&record).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &record, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Writes a real RSEG segment with one series and publishes its commit
/// record, for the segment-fetch gate scenario: `QueryEngine::instant` must be
/// able to decode it, unlike the lightweight commits above. Duplicated from
/// `crates/ravel-failure-tests/tests/common/mod.rs::publish_segment`
/// (test-only helper, different crate, not importable).
#[allow(clippy::too_many_arguments)]
async fn publish_real_segment(
    store: &dyn ObjectStoreBackend,
    tenant_hash: TenantHash,
    shard: u32,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
    metric: &str,
    ts_ns: i64,
) {
    let label_set = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels");
    let tenant = TenantId::new("gate-bench-tenant");
    let series_id = SeriesId::compute(&tenant, metric, &label_set).expect("series id");
    let series = vec![SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample { ts_ns, value: 1.0 }],
    }];

    let writer_id = Uuid::new_v4();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 0,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written: WrittenSegment =
        SegmentWriter::write(series, identity, bounds).expect("write segment");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 0,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(ravel_segment::VERSION_V7),
        created_unix_ns,
        ingest_hour_bucket,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

fn build_catalog(store: Arc<dyn ObjectStoreBackend>, shard_count: u32) -> Catalog {
    Catalog::new(
        store,
        CatalogConfig {
            shard_count,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog config")
}

async fn run_resolve_scenario(raw_store: Arc<dyn ObjectStoreBackend>, args: &Args) {
    let tenant_hash = TenantId::new("resolve-bench-tenant").hash();
    let shards = args.resolve_shards;
    let hours = args.resolve_hours;
    let commits_per_bucket = args
        .resolve_commits
        .div_ceil(u64::from(shards) * u64::from(hours))
        .max(1);
    let total_commits = commits_per_bucket * u64::from(shards) * u64::from(hours);

    println!("== resolve before/after fold ==");
    println!(
        "populating {total_commits} commits ({shards} shards x {hours} hours x {commits_per_bucket} per bucket)"
    );

    let populate_start = Instant::now();
    for shard in 0..shards {
        for hour_offset in 0..hours {
            let hour = BASE_HOUR + hour_offset;
            let created_unix_ns = hour_end_ns(hour) - NS_PER_SEC;
            for _ in 0..commits_per_bucket {
                publish_lightweight(
                    raw_store.as_ref(),
                    tenant_hash,
                    shard,
                    hour,
                    created_unix_ns,
                )
                .await;
            }
        }
    }
    println!("populate wall time: {:?}", populate_start.elapsed());

    let last_hour = BASE_HOUR + hours - 1;
    let now_ns = hour_end_ns(last_hour) + SEAL_PADDING_NS;
    let range = TimeRange {
        start_ns: hour_start_ns(BASE_HOUR),
        end_ns: hour_end_ns(last_hour),
    };

    let (before_store, before_counters) = CountingStore::wrap(Arc::clone(&raw_store));
    let before_catalog = build_catalog(before_store, shards);
    let before_start = Instant::now();
    let before_snapshot = before_catalog
        .resolve(&tenant_hash, Signal::Metrics, range, &[], now_ns)
        .await
        .expect("resolve before fold");
    let before_wall = before_start.elapsed();
    let (before_gets, before_lists, _) = before_counters.snapshot();

    let fold_catalog = build_catalog(Arc::clone(&raw_store), shards);
    let fold_report = fold_catalog
        .fold(
            &tenant_hash,
            Signal::Metrics,
            Uuid::new_v4(),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("fold");

    let (after_store, after_counters) = CountingStore::wrap(Arc::clone(&raw_store));
    let after_catalog = build_catalog(after_store, shards);
    let after_start = Instant::now();
    let after_snapshot = after_catalog
        .resolve(&tenant_hash, Signal::Metrics, range, &[], now_ns)
        .await
        .expect("resolve after fold");
    let after_wall = after_start.elapsed();
    let (after_gets, after_lists, _) = after_counters.snapshot();

    println!("--- fold report ---");
    println!("watermark_hour: {:?}", fold_report.watermark_hour);
    println!("buckets_folded: {}", fold_report.buckets_folded);
    println!("entry_count: {}", fold_report.entry_count);
    println!("part_bytes: {}", fold_report.part_bytes);
    println!(
        "fold requests: list={} get={} put={}",
        fold_report.list_requests, fold_report.get_requests, fold_report.put_requests
    );

    println!("--- resolve before fold ---");
    println!("segments returned: {}", before_snapshot.segments.len());
    println!("wall time: {before_wall:?}");
    println!("requests: get={before_gets} list={before_lists}");

    println!("--- resolve after fold ---");
    println!("segments returned: {}", after_snapshot.segments.len());
    println!("wall time: {after_wall:?}");
    println!("requests: get={after_gets} list={after_lists}");

    let before_requests = before_gets + before_lists;
    let after_requests = after_gets + after_lists;
    println!("--- summary ---");
    println!(
        "request reduction: {before_requests} -> {after_requests} ({:.1}x)",
        before_requests as f64 / after_requests.max(1) as f64
    );
    println!(
        "wall time reduction: {before_wall:?} -> {after_wall:?} ({:.1}x)",
        before_wall.as_secs_f64() / after_wall.as_secs_f64().max(1e-9)
    );
    assert_eq!(
        before_snapshot.segments.len(),
        after_snapshot.segments.len(),
        "fold must never change which commits resolve sees (docs/consistency-model.md \
         'Catalog snapshot staleness')"
    );
}

async fn run_gate_scenario(raw_store: Arc<dyn ObjectStoreBackend>, args: &Args) {
    let tenant = TenantId::new("gate-bench-tenant");
    let tenant_hash = tenant.hash();
    let shard = 0u32;
    let hour = BASE_HOUR + 500; // disjoint from the resolve scenario's hour range
    let ts_ns = hour_end_ns(hour) - NS_PER_SEC;

    println!("== segment-fetch gate: segment-fetch share of a selective query ==");
    println!(
        "populating {} single-series segments in one (shard, hour) bucket",
        args.gate_segments
    );
    for i in 0..args.gate_segments {
        let metric = format!("gate_metric_{i}");
        publish_real_segment(
            raw_store.as_ref(),
            tenant_hash,
            shard,
            hour,
            ts_ns,
            &metric,
            ts_ns,
        )
        .await;
    }

    let now_ns = hour_end_ns(hour) + SEAL_PADDING_NS;
    let query = "gate_metric_0";
    let t_ms = ts_ns / 1_000_000;

    // The gate question is scoped to "with snapshots on": fold this bucket
    // first so both measurements below hit the index rather than the
    // full-listing fallback. An unfolded resolve would GET every
    // commit record just to build the candidate list, swamping the signal
    // this scenario exists to isolate (the per-segment suffix-GET cost the
    // fold does NOT eliminate, since it prunes by hour, not by name).
    let fold_catalog = build_catalog(Arc::clone(&raw_store), 1);
    fold_catalog
        .fold(
            &tenant_hash,
            Signal::Metrics,
            Uuid::new_v4(),
            now_ns,
            &[],
            None,
        )
        .await
        .expect("fold gate bucket");

    let (resolve_store, resolve_counters) = CountingStore::wrap(Arc::clone(&raw_store));
    let resolve_catalog = build_catalog(resolve_store, 1);
    let range = TimeRange {
        start_ns: hour_start_ns(hour),
        end_ns: hour_end_ns(hour),
    };
    let resolve_start = Instant::now();
    let resolved = resolve_catalog
        .resolve(&tenant_hash, Signal::Metrics, range, &[], now_ns)
        .await
        .expect("resolve candidates");
    let resolve_wall = resolve_start.elapsed();
    let (resolve_gets, resolve_lists, _) = resolve_counters.snapshot();
    let resolve_requests = resolve_gets + resolve_lists;

    let (query_store, query_counters) = CountingStore::wrap(Arc::clone(&raw_store));
    let query_catalog = Arc::new(build_catalog(Arc::clone(&query_store), 1));
    let engine = QueryEngine::new(
        Arc::clone(&query_catalog),
        query_store,
        EngineConfig::default(),
    );
    let query_start = Instant::now();
    let (value, _coverage) = engine
        .instant(
            tenant_hash,
            query,
            t_ms,
            &[],
            now_ns,
            Duration::from_secs(30),
        )
        .await
        .expect("gate query");
    let query_wall = query_start.elapsed();
    let (query_gets, query_lists, _) = query_counters.snapshot();
    let query_requests = query_gets + query_lists;

    let matched = match value {
        ravel_promql::Value::Vector(v) => v.len(),
        other => panic!("expected instant vector, got {other:?}"),
    };

    println!("--- resolve only (candidate listing) ---");
    println!("candidate segments: {}", resolved.segments.len());
    println!("wall time: {resolve_wall:?}");
    println!("requests: get={resolve_gets} list={resolve_lists}");

    println!("--- full query (resolve + fetch + decode) ---");
    println!(
        "matched series: {matched} (selectivity 1/{})",
        args.gate_segments
    );
    println!("wall time: {query_wall:?}");
    println!("requests: get={query_gets} list={query_lists}");

    let fetch_requests = query_requests.saturating_sub(resolve_requests);
    let fetch_share = fetch_requests as f64 / query_requests.max(1) as f64;
    let fetch_wall = query_wall.saturating_sub(resolve_wall);
    let fetch_wall_share = fetch_wall.as_secs_f64() / query_wall.as_secs_f64().max(1e-9);

    println!("--- segment-fetch gate ---");
    println!(
        "segment-fetch requests: {fetch_requests} / {query_requests} total ({:.1}% by request count)",
        fetch_share * 100.0
    );
    println!(
        "segment-fetch wall time: {fetch_wall:?} / {query_wall:?} total ({:.1}% by wall time)",
        fetch_wall_share * 100.0
    );
    println!(
        "gate criterion: segment-fetch \
         dominant (>50% of requests) = {}",
        fetch_share > 0.5
    );
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let raw_store: Arc<dyn ObjectStoreBackend> = match args.store {
        StoreKind::Memory => Arc::new(MemoryStore::new()),
        StoreKind::S3 => {
            Arc::new(S3Store::new(s3_config_from_env()).expect("build S3Store from RAVEL_S3_* env"))
        }
    };

    match args.scenario {
        Scenario::All => {
            run_resolve_scenario(Arc::clone(&raw_store), &args).await;
            println!();
            run_gate_scenario(raw_store, &args).await;
        }
        Scenario::Resolve => run_resolve_scenario(raw_store, &args).await,
        Scenario::Gate => run_gate_scenario(raw_store, &args).await,
    }
}
