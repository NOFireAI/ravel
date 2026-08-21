//! Group-by aggregation core-count scaling benchmark core (ADR-0102 decision
//! 4).
//!
//! One fixed group-by query over one fixed cardinality, swept across two axes:
//! `target_partitions` (the knob a running server sets from core count) and
//! `SqlConfig::parallel_final_aggregation` (ADR-0094's exact-typed final
//! aggregation, off by default). The deliverable is throughput and latency at
//! every (partitions x flag) combination, so the epic can see whether the
//! parallel final aggregation actually scales with cores before flipping its
//! default. This is a different instrument from `sql_corpus`/#428's
//! `sql_latency_bench` (per-query latency across a diverse SQL corpus): this
//! one holds the query fixed and sweeps the parallelism axis.
//!
//! `target_partitions` is set through `EngineConfig::fetch_concurrency`, which
//! `ravel_sql::session::session_config` feeds straight into DataFusion's
//! `with_target_partitions`. The dataset is deliberately multi-part (one RSEG
//! object per part, disjoint series): today's scan partitioning is
//! segment-granular (`min(target_partitions, segment_count)`, ADR-0102
//! decision 1 deferred), so a single-segment tenant would pin every scan to
//! one partition regardless of the axis. A multi-part tenant lets the scan
//! fan out up to the part count, which is what "scaling versus core count"
//! needs to be visible at all.
//!
//! The fixed query groups by `series_id` (a `FixedSizeBinary(16)`, non-float)
//! with `count(*)`/`min(ts)`/`max(ts)` aggregates (all exact over non-float
//! inputs), so it is provably exact-typed under ADR-0094's classification:
//! `parallel_final_aggregation` genuinely engages when on rather than being
//! silently disqualified. Group cardinality equals the distinct series count,
//! which the config controls directly.
//!
//! Lives in the lib (not just the `groupby_scaling_bench` bin), mirroring
//! `query_latency.rs`, so `tests/groupby_scaling_smoke.rs` exercises the same
//! path the bin runs. Report-only: never changes library behavior, only
//! measures it. Gated on the `sql-latency` feature (shared with `sql_corpus`),
//! so the default build never compiles ravel-sql/datafusion or this module.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{ByteLimit, EngineConfig, LogSegmentFetcher, RequestLimit, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use serde::Serialize;
use uuid::Uuid;

use crate::generator::{CardinalityProfile, WorkloadConfig, generate_raw};

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Frozen query clock. Small on purpose, exactly as `flight_sql_egress` fixes
/// it: `Catalog::resolve` issues one LIST per (shard, ingest-hour), so a
/// wall-clock value would fan out to hundreds of thousands of LISTs. Every
/// published part lives in ingest-hour bucket 0 with event timestamps a few
/// microseconds after the epoch, well inside `[0, NOW_NS]`.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// The one query the sweep measures. Group by `series_id` (non-float key) with
/// `count(*)`/`min(ts)`/`max(ts)` (all exact over non-float inputs) so the
/// query is exact-typed under ADR-0094 and `parallel_final_aggregation`
/// actually engages when enabled.
pub const QUERY: &str = "SELECT series_id, count(*) AS rows, min(ts) AS first_ts, max(ts) AS last_ts \
     FROM samples GROUP BY series_id";

/// Inputs for one scaling run.
pub struct GroupbyScalingConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_label: String,
    /// Number of RSEG objects the tenant is split across. Should be at least
    /// the largest `target_partitions` value for the scan to fan out that far
    /// (segment-granular partitioning, decision 1 deferred).
    pub parts: usize,
    /// Total distinct series across all parts. Equals the group cardinality of
    /// the fixed group-by query.
    pub series: usize,
    /// Samples per series. Total scanned rows are `series * samples_per_series`.
    pub samples_per_series: usize,
    /// The swept parallelism axis: DataFusion `target_partitions` values, each
    /// run once with the flag off and once on.
    pub target_partitions: Vec<usize>,
    /// Timed repetitions per (partitions x flag) combination. Latency
    /// min/median/max are taken across these; the fixture is built once up
    /// front and reused for every combination.
    pub runs: usize,
}

impl GroupbyScalingConfig {
    /// A cheap fixture for the acceptance test and CI: a handful of parts, a
    /// small dataset, two partition values, two runs. Fast enough to run in a
    /// unit-test harness while still exercising both axes end to end.
    pub fn smoke(store: Arc<dyn ObjectStoreBackend>, store_label: &str) -> Self {
        GroupbyScalingConfig {
            store,
            store_label: store_label.to_string(),
            parts: 4,
            series: 40,
            samples_per_series: 20,
            target_partitions: vec![1, 2],
            runs: 2,
        }
    }
}

#[derive(Serialize)]
pub struct ReportConfig {
    pub store: String,
    pub query: String,
    pub parts: usize,
    pub series: usize,
    pub samples_per_series: usize,
    pub total_samples: u64,
    /// Distinct groups the query returned (equals distinct series). Left
    /// visible so a misconfigured cardinality is obvious in the report rather
    /// than folded away.
    pub groups: usize,
    pub target_partitions: Vec<usize>,
    pub runs: usize,
}

/// One (target_partitions x parallel_final_aggregation) measurement.
#[derive(Serialize)]
pub struct ComboResult {
    pub target_partitions: usize,
    pub parallel_final_aggregation: bool,
    /// Segments the successful attempt actually scanned, from `SqlStats`. With
    /// today's segment-granular partitioning this bounds the scan fan-out.
    pub segments_scanned: usize,
    /// Result rows (groups) the query returned; identical across combinations
    /// for the same dataset, kept per-combo as a correctness check.
    pub result_rows: usize,
    pub min_ms: f64,
    pub median_ms: f64,
    pub max_ms: f64,
    /// Scanned input rows per second, computed from `total_samples` and the
    /// median latency. The throughput axis the ADR asks for.
    pub rows_per_sec: f64,
}

#[derive(Serialize)]
pub struct Report {
    pub config: ReportConfig,
    pub combos: Vec<ComboResult>,
}

/// Nearest-rank percentile over an already-sorted slice, matching
/// `query_latency.rs`'s convention so latency numbers are comparable across
/// this crate's benches.
fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

/// Build the multi-part dataset, then sweep every (partitions x flag)
/// combination against it, returning the full report.
pub async fn run(config: &GroupbyScalingConfig) -> Report {
    let store = Arc::clone(&config.store);
    // Unique per run, like `query_latency::run`'s tenant, so consecutive local
    // runs against the same bucket never share a prefix.
    let tenant = TenantId::new(format!("bench-tenant-{}", Uuid::new_v4()));
    let tenant_hash = tenant.hash();

    let total_samples = publish_dataset(store.as_ref(), &tenant, config).await;

    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));

    let mut combos = Vec::with_capacity(config.target_partitions.len() * 2);
    let mut groups = 0usize;
    for &tp in &config.target_partitions {
        // Both flag states at every partition value: the on/off axis.
        for parallel in [false, true] {
            let result = run_combo(
                Arc::clone(&catalog),
                Arc::clone(&store),
                tenant_hash,
                tp,
                parallel,
                config.runs,
                total_samples,
            )
            .await;
            groups = groups.max(result.result_rows);
            combos.push(result);
        }
    }

    Report {
        config: ReportConfig {
            store: config.store_label.clone(),
            query: QUERY.to_string(),
            parts: config.parts,
            series: config.series,
            samples_per_series: config.samples_per_series,
            total_samples,
            groups,
            target_partitions: config.target_partitions.clone(),
            runs: config.runs,
        },
        combos,
    }
}

/// Run the fixed query at one (target_partitions, parallel) combination:
/// `runs` timed iterations plus one warm-up, returning the latency spread and
/// derived throughput.
async fn run_combo(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    tenant_hash: TenantHash,
    target_partitions: usize,
    parallel: bool,
    runs: usize,
    total_samples: u64,
) -> ComboResult {
    // `target_partitions` is set through `fetch_concurrency`; every budget is
    // lifted so the benchmark measures execution time, not a budget rejection.
    let engine = EngineConfig {
        max_series: usize::MAX,
        max_samples: usize::MAX,
        max_bytes_scanned: ByteLimit::Unlimited,
        max_s3_requests: RequestLimit::Unlimited,
        fetch_concurrency: target_partitions.max(1),
        ..EngineConfig::default()
    };
    let sql_config = SqlConfig {
        engine,
        parallel_final_aggregation: parallel,
        ..SqlConfig::default()
    };
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        sql_config,
        1 << 30,
    );

    let request = || SqlRequest {
        sql: QUERY.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(30),
    };

    // One warm run establishes the result shape and primes any lazy init
    // before the timed iterations.
    let warm = executor
        .execute(tenant_hash, &request())
        .await
        .expect("warm query");
    let result_rows = warm.output.num_rows();
    let segments_scanned = warm.stats.segments;

    let mut latencies_ns = Vec::with_capacity(runs.max(1));
    for _ in 0..runs.max(1) {
        let start = Instant::now();
        let outcome = executor
            .execute(tenant_hash, &request())
            .await
            .expect("timed query");
        latencies_ns.push(start.elapsed().as_nanos() as u64);
        assert_eq!(
            outcome.output.num_rows(),
            result_rows,
            "group-by result row count is deterministic across runs"
        );
    }
    latencies_ns.sort_unstable();

    let median_ns = percentile(&latencies_ns, 0.50);
    let min_ns = latencies_ns.first().copied().unwrap_or(0);
    let max_ns = latencies_ns.last().copied().unwrap_or(0);
    let rows_per_sec = if median_ns == 0 {
        0.0
    } else {
        total_samples as f64 / (median_ns as f64 / 1e9)
    };

    ComboResult {
        target_partitions,
        parallel_final_aggregation: parallel,
        segments_scanned,
        result_rows,
        min_ms: min_ns as f64 / 1e6,
        median_ms: median_ns as f64 / 1e6,
        max_ms: max_ns as f64 / 1e6,
        rows_per_sec,
    }
}

/// Write `config.parts` RSEG objects, each carrying a disjoint slice of the
/// tenant's series, and publish each one's commit record. Returns the total
/// sample count the query scans.
async fn publish_dataset(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    config: &GroupbyScalingConfig,
) -> u64 {
    let parts = config.parts.max(1);
    let tenant_hash = tenant.hash();
    let mut total_samples = 0u64;
    let mut offset = 0usize;
    for part in 0..parts {
        // Distribute series as evenly as possible; the first `remainder` parts
        // carry one extra so the totals sum exactly to `config.series`.
        let base = config.series / parts;
        let remainder = config.series % parts;
        let series_in_part = base + usize::from(part < remainder);
        if series_in_part == 0 {
            continue;
        }
        total_samples += publish_part(
            store,
            tenant,
            tenant_hash,
            part,
            series_in_part,
            offset,
            config.samples_per_series,
        )
        .await;
        offset += series_in_part;
    }
    total_samples
}

/// Write one part's series into a single RSEG object and publish its commit
/// record. `series_idx_offset` keeps each part's series ids disjoint from the
/// others', and a per-part writer identity keeps the data-object keys distinct.
async fn publish_part(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    tenant_hash: TenantHash,
    part: usize,
    series_count: usize,
    series_idx_offset: usize,
    samples_per_series: usize,
) -> u64 {
    let workload = WorkloadConfig {
        tenant: tenant.as_str().to_string(),
        series_count,
        samples_per_series,
        // Keep every event timestamp a few microseconds after the epoch so it
        // lands inside `[0, NOW_NS]` and ingest-hour bucket 0.
        start_ts_ns: 1_000,
        interval_ns: 1_000,
        cardinality: CardinalityProfile::many_small(series_count),
        series_idx_offset,
        ..WorkloadConfig::default()
    };
    let raw = generate_raw(&workload).expect("generate dataset");
    let part_samples: u64 = raw.iter().map(|(_, _, samples)| samples.len() as u64).sum();

    let series: Vec<SeriesInput> = raw
        .into_iter()
        .map(|(series_id, labels, samples)| SeriesInput {
            series_id,
            labels,
            samples,
        })
        .collect();

    let writer_id = Uuid::from_u128(1_000 + part as u128);
    let writer_seq = part as u64 + 1;
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
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
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
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

    part_samples
}
