//! Compaction benchmark. Measures, on a synthetic sealed hour of small L0
//! segments:
//!
//! - object count and stored bytes: L0 objects vs L1 parts, plus the
//!   commit-record / compaction-record metadata share (the size gate: L1 total
//!   <= 80% of L0 total; a miss is reported, it does not fail here);
//! - resolve + query latency and GET/LIST counts, before vs after compaction;
//! - compaction wall time and peak RSS (VmHWM delta from /proc/self/status);
//! - an end-to-end demo: ingest -> compact -> query-equivalence -> retention
//!   -> bucket verified empty.
//!
//! Report-only: it never changes ravel-maintain / ravel-catalog / ravel-query
//! behavior, only measures it. It never invents a number: the metrics (RSEG)
//! path runs fully here; the logs (RLOG) signal-generic numbers and the S3
//! panel are reported as pending, not fabricated.
//!
//! Object count (not raw byte reduction) is the primary compaction win; the
//! stored-byte gate is a secondary target whose miss is recorded, not treated
//! as a failure.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use clap::{Parser, ValueEnum};
use prost::Message;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, NoLeases, RetentionConfig,
    RetentionOutcome, RetentionPolicy, compact_bucket, retention_sweep_bucket, sweep_shard,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_proto::commit::v1::CompactionRecord;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{
    IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V6, WrittenSegment,
};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const SEAL_PADDING_NS: i64 = 3 * NS_PER_HOUR;
const BASE_HOUR: u32 = 100_000;

fn hour_end_ns(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR
}
fn hour_start_ns(hour: u32) -> i64 {
    i64::from(hour) * NS_PER_HOUR
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Scenario {
    All,
    Sizes,
    Query,
    Demo,
}

#[derive(Parser, Debug)]
#[command(about = "Compaction benchmark (L1-vs-L0 size gate)")]
struct Args {
    #[arg(long, value_enum, default_value_t = Scenario::All)]
    scenario: Scenario,
    /// Number of single-series L0 segments in the synthetic hour.
    #[arg(long, default_value_t = 3600)]
    segments: u32,
}

/// Peak resident set size in KiB (VmHWM from /proc/self/status), or 0 if
/// unreadable. Used for the incremental-peak measurement around compaction.
fn peak_rss_kib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            return rest
                .split_whitespace()
                .next()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0);
        }
    }
    0
}

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

/// Sizes accumulated while seeding: L0 data-object bytes and commit-record
/// bytes (the two things a bucket physically stores at L0).
#[derive(Default, Clone, Copy)]
struct SeedSizes {
    l0_data_bytes: u64,
    l0_record_bytes: u64,
    objects: u64,
}

/// Publish one real single-series RSEG segment and its commit record; returns
/// the data-object and record byte sizes. Mirrors catalog_resolve_bench's
/// publish_real_segment (test-only helpers are not importable across crates).
#[allow(clippy::too_many_arguments)]
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    tenant_hash: TenantHash,
    shard: u32,
    hour: u32,
    metric: &str,
    ts_ns: i64,
    value: f64,
) -> (u64, u64) {
    let label_set = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant, metric, &label_set).expect("series id");
    let series = vec![SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample { ts_ns, value }],
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
        min_ingest_ts_ns: ts_ns,
        max_ingest_ts_ns: ts_ns,
    };
    let written: WrittenSegment =
        SegmentWriter::write(series, identity, bounds).expect("write segment");
    let data_len = written.bytes.len() as u64;

    let created_unix_ns = hour_end_ns(hour) - NS_PER_SEC;
    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: 0,
        object_size: data_len,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: u32::from(VERSION_V6),
        created_unix_ns,
        ingest_hour_bucket: hour,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let record_len = record::encode(&rec).len() as u64;
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    (data_len, record_len)
}

/// Seed `n` distinct single-series segments into one (shard, hour) bucket.
async fn seed_bucket(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    tenant_hash: TenantHash,
    shard: u32,
    hour: u32,
    n: u32,
) -> SeedSizes {
    let ts_ns = hour_end_ns(hour) - NS_PER_SEC;
    let mut sizes = SeedSizes::default();
    for i in 0..n {
        let metric = format!("bench_metric_{i}");
        let (d, r) = publish_segment(
            store,
            tenant,
            tenant_hash,
            shard,
            hour,
            &metric,
            ts_ns,
            i as f64,
        )
        .await;
        sizes.l0_data_bytes += d;
        sizes.l0_record_bytes += r;
        sizes.objects += 1;
    }
    sizes
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

async fn fetch_compaction_record(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
) -> CompactionRecord {
    let listing = ravel_maintain::read::list_bucket(store, bucket)
        .await
        .expect("list bucket");
    let key = listing
        .compaction_record_keys
        .first()
        .expect("a compaction record exists");
    let got = store.get(key, GetRange::Full).await.expect("get record");
    CompactionRecord::decode(got.data.as_ref()).expect("decode record")
}

/// Stored-byte + object-count scenario (§9 gate).
async fn run_sizes(raw: Arc<dyn ObjectStoreBackend>, args: &Args) {
    let tenant = TenantId::new("compaction-bench");
    let tenant_hash = tenant.hash();
    let shard = 0u32;
    let hour = BASE_HOUR;
    println!("== stored bytes and object count (metrics/RSEG) ==");
    println!(
        "seeding {} single-series L0 segments in one bucket",
        args.segments
    );
    let sizes = seed_bucket(
        raw.as_ref(),
        &tenant,
        tenant_hash,
        shard,
        hour,
        args.segments,
    )
    .await;

    let bucket = Bucket::new(tenant_hash, Signal::Metrics, shard, hour);
    let now = hour_end_ns(hour) + SEAL_PADDING_NS;
    let clock = FixedClock::new(now);
    let config = CompactorConfig::default();

    let outcome = compact_bucket(raw.as_ref(), &clock, &config, &bucket)
        .await
        .expect("compact");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );
    let record = fetch_compaction_record(raw.as_ref(), &bucket).await;
    let l1_data_bytes: u64 = record.parts.iter().map(|p| p.object_size).sum();
    let l1_record_bytes = record.encode_to_vec().len() as u64;

    let l0_total = sizes.l0_data_bytes + sizes.l0_record_bytes;
    let l1_total = l1_data_bytes + l1_record_bytes;
    let l0_objects = sizes.objects * 2; // one data object + one commit record each
    let l1_objects = record.parts.len() as u64 + 1; // parts + one compaction record

    println!("--- objects ---");
    println!(
        "L0 objects: {l0_objects} ({} data + {} records)",
        sizes.objects, sizes.objects
    );
    println!(
        "L1 objects: {l1_objects} ({} parts + 1 record)",
        record.parts.len()
    );
    println!(
        "object-count reduction: {l0_objects} -> {l1_objects} ({:.1}x)",
        l0_objects as f64 / l1_objects.max(1) as f64
    );
    println!("--- stored bytes ---");
    println!(
        "L0 data bytes: {} record bytes: {} total: {l0_total}",
        sizes.l0_data_bytes, sizes.l0_record_bytes
    );
    println!("L1 data bytes: {l1_data_bytes} record bytes: {l1_record_bytes} total: {l1_total}");
    let ratio = l1_total as f64 / l0_total.max(1) as f64;
    println!("--- §9 stored-byte gate ---");
    println!("L1/L0 total byte ratio: {:.3} (target <= 0.80)", ratio);
    println!(
        "gate {}: {}",
        if ratio <= 0.80 { "MET" } else { "MISSED" },
        if ratio <= 0.80 {
            "L1 is within 80% of L0".to_string()
        } else {
            "L1 exceeds 80% of L0; file an L2 (run-merging) follow-up. Object-count \
             reduction, not byte reduction, is the primary win (plan §9)."
                .to_string()
        }
    );
}

/// Resolve+query before/after compaction, plus compaction wall/RSS.
async fn run_query(raw: Arc<dyn ObjectStoreBackend>, args: &Args) {
    let tenant = TenantId::new("compaction-bench-q");
    let tenant_hash = tenant.hash();
    let shard = 0u32;
    let hour = BASE_HOUR + 10;
    println!("== resolve + query, before vs after compaction (metrics/RSEG) ==");
    println!("seeding {} single-series L0 segments", args.segments);
    seed_bucket(
        raw.as_ref(),
        &tenant,
        tenant_hash,
        shard,
        hour,
        args.segments,
    )
    .await;

    let now = hour_end_ns(hour) + SEAL_PADDING_NS;
    let range = TimeRange {
        start_ns: hour_start_ns(hour),
        end_ns: hour_end_ns(hour),
    };
    let query = "bench_metric_0";
    let t_ms = (hour_end_ns(hour) - NS_PER_SEC) / 1_000_000;

    let before = measure_query(Arc::clone(&raw), tenant_hash, range, query, t_ms, now).await;

    // Compaction wall time + incremental peak RSS.
    let bucket = Bucket::new(tenant_hash, Signal::Metrics, shard, hour);
    let clock = FixedClock::new(now);
    let config = CompactorConfig::default();
    let rss_before = peak_rss_kib();
    let t = Instant::now();
    let outcome = compact_bucket(raw.as_ref(), &clock, &config, &bucket)
        .await
        .expect("compact");
    let compact_wall = t.elapsed();
    let rss_after = peak_rss_kib();
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );

    let after = measure_query(Arc::clone(&raw), tenant_hash, range, query, t_ms, now).await;

    println!("--- resolve+query before compaction ---");
    print_query_line(&before);
    println!("--- compaction ---");
    println!("wall time: {compact_wall:?}");
    println!(
        "peak RSS (VmHWM): before={rss_before} KiB after={rss_after} KiB delta={} KiB",
        rss_after.saturating_sub(rss_before)
    );
    println!("--- resolve+query after compaction ---");
    print_query_line(&after);
    println!("--- summary ---");
    println!(
        "resolve candidates: {} -> {} ; query: {} -> {}",
        before.segments, after.segments, before.query_status, after.query_status,
    );
    println!("--- token-fallback ---");
    println!(
        "pending: the extra exact-token GET a superseded input incurs post-sweep requires a \
         constructed stale-snapshot-across-sweep scenario; not measured in this bench."
    );
}

struct QueryMeasure {
    resolve_wall: Duration,
    query_wall: Duration,
    segments: usize,
    /// Human-readable query outcome: matched-series count on success, or the
    /// engine error on failure (a 3,600-segment hour hits max_segments=1024
    /// before compaction, which is the motivating failure of plan §1).
    query_status: String,
    resolve_reqs: (u64, u64),
    query_reqs: (u64, u64),
}

async fn measure_query(
    raw: Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    range: TimeRange,
    query: &str,
    t_ms: i64,
    now: i64,
) -> QueryMeasure {
    // Resolve on its own counting store, so resolve vs query request counts
    // are separable.
    let (r_store, r_counters) = CountingStore::wrap(Arc::clone(&raw));
    let r_catalog = Arc::new(build_catalog(Arc::clone(&r_store), 1));
    let resolve_start = Instant::now();
    let snapshot = r_catalog
        .resolve(&tenant_hash, Signal::Metrics, range, &[], now)
        .await
        .expect("resolve");
    let resolve_wall = resolve_start.elapsed();
    let (rg, rl, _) = r_counters.snapshot();

    let (q_store, q_counters) = CountingStore::wrap(Arc::clone(&raw));
    let q_catalog = Arc::new(build_catalog(Arc::clone(&q_store), 1));
    let engine = QueryEngine::new(q_catalog, Arc::clone(&q_store), EngineConfig::default());
    let query_start = Instant::now();
    let result = engine
        .instant(tenant_hash, query, t_ms, &[], now, Duration::from_secs(30))
        .await;
    let query_wall = query_start.elapsed();
    let (qg, ql, _) = q_counters.snapshot();
    let query_status = match result {
        Ok((ravel_promql::Value::Vector(v), _coverage)) => {
            format!("OK ({} series matched)", v.len())
        }
        Ok((other, _coverage)) => format!("OK (non-vector: {other:?})"),
        Err(e) => format!("FAILS: {e}"),
    };
    QueryMeasure {
        resolve_wall,
        query_wall,
        segments: snapshot.segments.len(),
        query_status,
        resolve_reqs: (rg, rl),
        query_reqs: (qg, ql),
    }
}

fn print_query_line(m: &QueryMeasure) {
    println!("resolved candidate segments: {}", m.segments);
    println!(
        "resolve wall: {:?}  requests: get={} list={}",
        m.resolve_wall, m.resolve_reqs.0, m.resolve_reqs.1
    );
    println!(
        "query: {}  wall: {:?}  requests: get={} list={}",
        m.query_status, m.query_wall, m.query_reqs.0, m.query_reqs.1
    );
}

/// End-to-end demo on MemoryStore: ingest -> compact -> query-equivalence ->
/// retention -> bucket verified empty.
async fn run_demo(raw: Arc<dyn ObjectStoreBackend>) {
    let tenant = TenantId::new("compaction-bench-demo");
    let tenant_hash = tenant.hash();
    let shard = 0u32;
    let hour = BASE_HOUR + 20;
    let n = 8u32;
    println!("== end-to-end demo (metrics/RSEG, MemoryStore) ==");
    println!("1. ingest {n} L0 segments");
    seed_bucket(raw.as_ref(), &tenant, tenant_hash, shard, hour, n).await;

    let now = hour_end_ns(hour) + SEAL_PADDING_NS;
    let range = TimeRange {
        start_ns: hour_start_ns(hour),
        end_ns: hour_end_ns(hour),
    };
    let query = "bench_metric_3";
    let t_ms = (hour_end_ns(hour) - NS_PER_SEC) / 1_000_000;

    // Query over the L0 inputs.
    let pre = measure_query(Arc::clone(&raw), tenant_hash, range, query, t_ms, now).await;
    let pre_value = query_value(Arc::clone(&raw), tenant_hash, query, t_ms, now).await;

    // Compact.
    println!("2. compact");
    let bucket = Bucket::new(tenant_hash, Signal::Metrics, shard, hour);
    let clock = FixedClock::new(now);
    let config = CompactorConfig::default();
    let outcome = compact_bucket(raw.as_ref(), &clock, &config, &bucket)
        .await
        .expect("compact");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );

    // Query over the L1 output must match byte-for-byte.
    println!("3. differential query-equivalence check");
    let post_value = query_value(Arc::clone(&raw), tenant_hash, query, t_ms, now).await;
    let equivalent = query_values_equal(&pre_value, &post_value);
    println!(
        "   pre-compaction result == post-compaction result: {equivalent} \
         (pre candidate segments={}, pre query: {})",
        pre.segments, pre.query_status
    );
    assert!(equivalent, "compaction changed the query result");

    // Retention: configure a window at the floor, tombstone, then physically
    // sweep after the protection horizon, and verify the bucket is empty.
    println!("4. retention (tombstone -> horizon -> physical sweep)");
    let lag_ns = CatalogConfig::default().max_ingest_lag_ns;
    let floor = config.retention_floor_ns(lag_ns);
    let policy = RetentionPolicy {
        default: Some(floor),
        tenants: vec![],
    };
    let retention = RetentionConfig::from_policy(policy, &config, lag_ns).expect("valid policy");

    // now2 is far enough past the newest event that the bucket is expired
    // (max_event < now2 - R) and also past the seal margin.
    let now2 = hour_end_ns(hour) + floor + SEAL_PADDING_NS;
    let clock2 = FixedClock::new(now2);
    let out = retention_sweep_bucket(
        raw.as_ref(),
        &clock2,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("retention pass 1");
    println!("   pass 1 (expire + tombstone): {out:?}");
    assert!(matches!(out, RetentionOutcome::Tombstoned), "{out:?}");

    // Advance past the protection horizon so the physical sweep runs.
    let now3 = now2 + config.protection_horizon_ns + NS_PER_HOUR;
    let clock3 = FixedClock::new(now3);
    let out = retention_sweep_bucket(
        raw.as_ref(),
        &clock3,
        &config,
        &retention,
        &NoLeases,
        &bucket,
    )
    .await
    .expect("retention pass 2");
    println!("   pass 2 (physical sweep): {out:?}");
    assert!(matches!(out, RetentionOutcome::Swept), "{out:?}");

    // Also run the GC sweeper to mop up any residue (should be none).
    let _ = sweep_shard(
        raw.as_ref(),
        &clock3,
        &config,
        &NoLeases,
        &tenant_hash,
        Signal::Metrics,
        shard,
    )
    .await
    .expect("sweep");

    println!("5. verify bucket empty");
    let l0_prefix = format!("t/{}/m/l0/{:04}/", tenant_hash.to_hex(), shard);
    let c_prefix = keys::commit_shard_prefix(&tenant_hash, Signal::Metrics, shard).expect("prefix");
    let l1_prefix = format!("t/{}/m/l1/{:04}/", tenant_hash.to_hex(), shard);
    let l0 = list_all(raw.as_ref(), &l0_prefix)
        .await
        .expect("list l0")
        .len();
    let c = list_all(raw.as_ref(), &c_prefix)
        .await
        .expect("list c")
        .len();
    let l1 = list_all(raw.as_ref(), &l1_prefix)
        .await
        .expect("list l1")
        .len();
    println!("   l0 objects: {l0}  commit-prefix objects: {c}  l1 objects: {l1}");
    assert_eq!(l0 + c + l1, 0, "retention left residue");
    println!("DEMO PASS: ingest -> compact -> query-equivalent -> retention -> empty");
}

async fn query_value(
    raw: Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    query: &str,
    t_ms: i64,
    now: i64,
) -> ravel_promql::Value {
    let catalog = Arc::new(build_catalog(Arc::clone(&raw), 1));
    let engine = QueryEngine::new(catalog, raw, EngineConfig::default());
    let (value, _coverage) = engine
        .instant(tenant_hash, query, t_ms, &[], now, Duration::from_secs(30))
        .await
        .expect("query");
    value
}

/// Bit-exact comparison of two instant-vector results (value bits and labels),
/// the equivalence compaction must preserve (plan: exact semantics).
fn query_values_equal(a: &ravel_promql::Value, b: &ravel_promql::Value) -> bool {
    match (a, b) {
        (ravel_promql::Value::Vector(va), ravel_promql::Value::Vector(vb)) => {
            if va.len() != vb.len() {
                return false;
            }
            let key = |s: &ravel_promql::InstantSample| {
                // Canonical, bit-exact key: label set (debug-rendered for a
                // stable total order), event ts, and the value's bit pattern
                // (NaN and -0.0 are significant; never compare with ==).
                (format!("{:?}", s.labels), s.ts_ns, s.value.to_bits())
            };
            let mut ka: Vec<_> = va.iter().map(key).collect();
            let mut kb: Vec<_> = vb.iter().map(key).collect();
            ka.sort();
            kb.sort();
            ka == kb
        }
        _ => false,
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match args.scenario {
        Scenario::All => {
            run_sizes(Arc::new(MemoryStore::new()), &args).await;
            println!();
            run_query(Arc::new(MemoryStore::new()), &args).await;
            println!();
            run_demo(Arc::new(MemoryStore::new())).await;
        }
        Scenario::Sizes => run_sizes(Arc::new(MemoryStore::new()), &args).await,
        Scenario::Query => run_query(Arc::new(MemoryStore::new()), &args).await,
        Scenario::Demo => run_demo(Arc::new(MemoryStore::new())).await,
    }
}
