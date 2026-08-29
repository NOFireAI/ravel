//! ADR-0103 (epic #64) `count_over_time` pushdown crossover benchmark core.
//!
//! Order-insensitive aggregation pushdown landed and is reachable as of `main`:
//! any eligible `count_over_time` query takes the pushdown path unconditionally,
//! gated only by the *correctness* gate `is_pushdown_eligible` (shard-generation
//! stability plus federation exclusion), never by a performance heuristic. This
//! bench measures whether engaging pushdown is worth it at every point on a
//! cardinality sweep, so a follow-up can decide if a production default needs a
//! cardinality-based engage threshold before pushdown fires or can stay
//! unconditional.
//!
//! # What it measures
//!
//! One fixed instant query, `count_over_time(bench_metric[5m])`, over a corpus
//! of `target_series` distinct series all sharing the `bench_metric` name, at
//! every value in a swept `target_series` list, under TWO conditions:
//!
//! - **Pushdown-eligible** (`eligible = true`): the corpus resolved through a
//!   non-enforcing catalog, so `Catalog::read_scan_generations` synthesizes an
//!   implicit single shard generation and `is_pushdown_eligible` is `true`. The
//!   worker's `run_slice_metrics` pushdown branch computes the partial and ships
//!   the count instead of raw runs.
//! - **Pushdown-ineligible** (`eligible = false`): THE SAME corpus and segment
//!   layout (same series count, same samples-per-series, same two segments at
//!   the same two ingest hours), but a second shard generation is recorded
//!   (`validate_or_adopt` then `append_generation`) and the engine's catalog is
//!   built with `CatalogConfig { shard_count: 1, .. }.with_provisioning_enforcement()`.
//!   The two segments straddle the generation boundary, so `is_pushdown_eligible`
//!   is `false` and the query falls back to raw fetch and coordinator merge.
//!
//! The enforcing catalog on the ineligible arm is load-bearing, not incidental:
//! `Catalog::read_scan_generations` synthesizes an implicit single generation
//! with no store read *unless provisioning enforcement is on*, so publishing a
//! second generation but querying through a default catalog would let
//! `is_pushdown_eligible` return `true` on both arms. Both would silently take
//! the pushdown path and every number here would be meaningless while looking
//! fine -- `tests/pushdown_crossover_smoke.rs` proves this by construction: it
//! asserts the observed gate matches the requested one on both arms, and a
//! mutation removing `.with_provisioning_enforcement()` turns that assertion
//! red with `gate=true` on the arm requesting `false`.
//!
//! Keeping the corpus byte-for-byte identical between arms isolates the
//! pushdown DECISION as the only variable, but at the small cardinalities this
//! bench's default sweep covers, most of the accounting columns end up
//! IDENTICAL across arms too (`s3_bytes`, `s3_get_requests`,
//! `decompressed_bytes` all matched exactly in a smoke run at 4 and 32
//! series): both arms fetch the same segment bytes from the store regardless
//! of whether the worker then reduces them to a partial or ships raw runs, so
//! the wire-shape saving pushdown is meant to prove out only shows up in what
//! the WORKER RETURNS to the coordinator, which this bench's reported
//! `QueryStats::accounting` does not separately break out from the underlying
//! segment fetch. `s3_requests` DOES differ between arms, by a constant offset
//! independent of `target_series` -- that is the enforcing catalog's extra
//! generation-history reads on the ineligible arm, a construction artifact of
//! how this bench forces ineligibility, not a signal about the pushdown
//! decision. Do not read a `s3_requests` delta as a pushdown effect. Wall-time
//! is the column this bench actually measures the pushdown effect through.
//!
//! # The distributed path is real
//!
//! Both arms run through the same real ADR-0071 distributed loopback worker
//! infrastructure `distrib_crossover.rs` uses: an in-process `tonic`
//! `SeriesFetch` worker on `127.0.0.1:0` over the resolved segments, with the
//! cost gate forced open (zeroed thresholds) so every query fans out regardless
//! of corpus size. `ravel-bench` cannot import `ravel-query`'s test-only
//! helpers, so the worker construction here re-derives the pattern
//! `crates/ravel-query/src/engine.rs`'s `spawn_metric_worker`/
//! `resolve_metric_segments` use from `ravel-query`'s public API
//! (`ravel_query::distrib::service::{SeriesFetchService, SnapshotSegmentResolver}`).
//!
//! # What the numbers do and do not say
//!
//! The store is an in-process `MemoryStore` and the worker is loopback: no
//! network round trip, no second NIC. This measures the CPU and transport
//! overhead the pushdown decision adds or saves against a zero-latency store,
//! not the object-store-latency regime where pushdown's win (shipping a count
//! instead of every raw sample) is largest. It is therefore a lower bound on
//! the benefit of engaging pushdown; an object-store-backed run (`--store s3`)
//! is the follow-up for the latency-dominated regime. `QueryStats::accounting`
//! is still reported per arm for visibility, but see the caveat above: at
//! this bench's default cardinalities most of those columns match across
//! arms and are not themselves evidence of a pushdown effect. Wall-time is.
//!
//! This bench cannot observe whether the worker's `run_slice_metrics`
//! pushdown branch actually engaged, only the final answer and the wall
//! time -- no reported column distinguishes "computed via partial" from
//! "computed via raw fetch that happened to agree." Proving pushdown itself
//! engages and produces the correct answer is T4c/T4d's own reachability and
//! regression tests' job, not this bench's; this bench assumes those hold
//! and measures cost, not correctness.
//!
//! `distrib_crossover.rs` builds its corpus through `IngestRouter` +
//! `SystemClock`, which gives no control over `ingest_hour_bucket`. This
//! bench's ineligible arm needs two segments straddling a specific shard-
//! generation boundary, so it publishes directly via `ravel_segment`/
//! `ravel_commit` instead (the same technique
//! `crates/ravel-query/src/engine.rs`'s `publish_metric_segment_multi` test
//! helper uses). The two crossover benches therefore build their corpora
//! through different routes, and their reported numbers are NOT directly
//! comparable to each other.
//!
//! # Report-only
//!
//! Lives in the lib (not just the `pushdown_crossover_bench` bin) so
//! `tests/pushdown_crossover_smoke.rs` exercises the same `run` path the bin
//! runs, mirroring `distrib_crossover.rs`. This crate never changes
//! `ravel-query`/`ravel-promql`/any other crate's behavior, it only measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use ravel_catalog::{
    AbsentPolicy, Catalog, CatalogConfig, SegmentRef, ShardGeneration, Snapshot, append_generation,
    validate_or_adopt,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::ObjectStoreBackend;
use ravel_promql::Value;
use ravel_query::distrib::Distributed;
use ravel_query::distrib::client::RemoteSliceFetcher;
use ravel_query::distrib::is_pushdown_eligible;
use ravel_query::distrib::partition::DistribThresholds;
use ravel_query::distrib::service::{SeriesFetchService, SnapshotSegmentResolver};
use ravel_query::{EngineConfig, QueryEngine, QueryStats, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, Signal, TenantHash, TenantId, TimeRange,
};
use serde::Serialize;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Server};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;

/// Corpus anchor and query instant, at hour 100: far enough from the epoch that
/// a second generation can activate 8 hours later with room on both sides.
const BASE_HOUR: u32 = 100;
const BASE_NS: i64 = BASE_HOUR as i64 * NS_PER_HOUR;

/// The two ingest-hour buckets the identical corpus is split across. The
/// ineligible arm's generation boundary ([`GEN1_ACTIVATION_HOUR`]) falls
/// between them, so the two segments land in two generations there; the eligible
/// arm's implicit single generation owns both.
const SEG_A_HOUR: u32 = BASE_HOUR;
const SEG_B_HOUR: u32 = BASE_HOUR + 11;

/// Second shard generation's activation hour on the ineligible arm. Between
/// [`SEG_A_HOUR`] and [`SEG_B_HOUR`] (accounting for scan slack) so the two
/// segments are owned by different stable generations.
const GEN1_ACTIVATION_HOUR: u32 = BASE_HOUR + 8;

/// Resolve `now_ns` and the query's `now_ns`: well past both segments and the
/// generation boundary so the ingest-hour scan covers hours 100..120 and both
/// segments resolve on both arms.
const RESOLVE_NOW_NS: i64 = (BASE_HOUR as i64 + 20) * NS_PER_HOUR;

/// The one query the sweep measures. `count_over_time` over a literal selector
/// is the pushdown candidate shape.
pub const QUERY: &str = "count_over_time(bench_metric[5m])";

/// The single metric name every corpus series shares.
pub const METRIC: &str = "bench_metric";

/// Inputs for one crossover run. The corpus knobs default small so the smoke
/// target stays fast; a recorded run passes larger values on the command line.
pub struct PushdownCrossoverConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_label: String,
    /// The swept axis: distinct series counts, all under `bench_metric`. Each
    /// value is measured once eligible and once ineligible.
    pub target_series: Vec<usize>,
    /// Samples per series *per segment* (two segments), all inside the 5m
    /// reduction window. Total in-window samples per series is `2 *` this, so it
    /// must stay below 150 for every sample to land in the window.
    pub samples_per_series: usize,
    /// Timed instant queries per (target_series, arm), for the wall-time
    /// percentiles.
    pub query_reps: usize,
    /// Per-query wall deadline, in seconds.
    pub deadline_secs: u64,
}

impl PushdownCrossoverConfig {
    /// A fast, deterministic default corpus for the smoke target: two small
    /// series counts, a handful of samples each, a few reps. Fast enough for CI
    /// while still exercising both arms end to end.
    pub fn smoke(store: Arc<dyn ObjectStoreBackend>, store_label: String) -> Self {
        PushdownCrossoverConfig {
            store,
            store_label,
            target_series: vec![4, 32],
            samples_per_series: 4,
            query_reps: 5,
            deadline_secs: 30,
        }
    }
}

/// Wall-time percentiles (ms) plus the cost counters query accounting exposes,
/// for one `(target_series, arm)` measurement.
#[derive(Serialize, Clone)]
pub struct ArmReport {
    pub target_series: usize,
    /// The requested arm: `true` = pushdown-eligible construction (single stable
    /// generation, non-enforcing catalog), `false` = ineligible (two
    /// generations, enforcing catalog).
    pub eligible: bool,
    /// OBSERVED: `is_pushdown_eligible` evaluated on the exact
    /// `(segments, generations)` this arm resolved, the same inputs the engine's
    /// gate sees. Must equal `eligible`; carried in the report (not only
    /// asserted) so a construction that failed to flip the gate is visible in
    /// the data rather than hidden behind a plausible-looking wall time.
    pub pushdown_eligible: bool,
    pub wall_ms_p50: f64,
    pub wall_ms_p90: f64,
    pub wall_ms_p99: f64,
    pub wall_ms_max: f64,
    /// Series in the query's instant vector. Identical across arms for the same
    /// `target_series`: the query answer must not depend on which path served it.
    pub matched_series: usize,
    /// Sum of the count values across the instant vector, a coarse correctness
    /// cross-check that is identical across arms.
    pub evaluated_count_sum: f64,
    /// Segments the resolved snapshot fetched (`QueryStats::segments_fetched`).
    pub segments_fetched: u64,
    /// Total store GET/LIST/HEAD requests the query issued
    /// (`accounting.total_s3_requests`).
    pub s3_requests: u64,
    /// Store GET requests alone (the fetch path's ranged reads).
    pub s3_get_requests: u64,
    /// Total bytes moved out of the store (`accounting.total_s3_bytes`): the
    /// bytes-moved figure, host-independent.
    pub s3_bytes: u64,
    /// Decoded sample footprint (`accounting.decompressed_bytes`), distinct from
    /// the raw store bytes above.
    pub decompressed_bytes: u64,
    /// Segments the arm resolved (the fan-out input size); 2 by construction.
    pub corpus_segments: usize,
}

#[derive(Serialize)]
pub struct ReportConfig {
    pub store: String,
    pub query: String,
    pub target_series: Vec<usize>,
    pub samples_per_series: usize,
    pub query_reps: usize,
}

#[derive(Serialize)]
pub struct Report {
    pub config: ReportConfig,
    /// One entry per `(target_series, arm)`: eligible then ineligible at each
    /// series count, in `target_series` order.
    pub arms: Vec<ArmReport>,
}

/// Nearest-rank percentile over pre-sorted samples, matching the convention the
/// other bench cores in this crate use.
fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

/// Zeroed cost-gate thresholds so any corpus fans out; `max_parallel_slices` is
/// 1 because a single worker holds the whole segment set (mirrors the engine
/// test's `zero_thresholds`).
fn zero_thresholds() -> DistribThresholds {
    DistribThresholds {
        min_store_bytes: 0,
        min_segments: 0,
        max_parallel_slices: 1,
    }
}

/// Publish one RSEG segment carrying `series_count` series under the `bench_metric`
/// name (distinguished by an `id` label so all match the same selector), each
/// with `samples_per_series` samples at `ts_ns = BASE_NS - (ts_offset + i) * NS_PER_SEC`.
/// The `ingest_hour_bucket` is set explicitly (independent of the event
/// timestamps) so the corpus can span two ingest hours while every sample stays
/// inside the query's 5m reduction window.
#[allow(clippy::too_many_arguments)]
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    tenant_hash: TenantHash,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    created_unix_ns: i64,
    series_count: usize,
    samples_per_series: usize,
    ts_offset: i64,
) {
    let inputs: Vec<SeriesInput> = (0..series_count)
        .map(|s| {
            let labels = LabelSet::new(vec![
                Label {
                    name: METRIC_NAME_LABEL.to_string(),
                    value: METRIC.to_string(),
                },
                Label {
                    name: "id".to_string(),
                    value: format!("s{s}"),
                },
            ])
            .expect("valid labels");
            let series_id =
                ravel_types::SeriesId::compute(tenant, METRIC, &labels).expect("series id");
            let samples: Vec<Sample> = (0..samples_per_series)
                .map(|i| Sample {
                    ts_ns: BASE_NS - (ts_offset + i as i64) * NS_PER_SEC,
                    value: 1.0,
                })
                .collect();
            SeriesInput {
                series_id,
                labels,
                samples,
            }
        })
        .collect();

    let writer_id = Uuid::from_u128(writer_seq as u128);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written: WrittenSegment =
        SegmentWriter::write(inputs, identity, bounds).expect("write segment");
    let new_record = NewCommitRecord {
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
        segment_format_version: u32::from(ravel_segment::SUPPORTED_VERSIONS.newest()),
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

/// Publish the identical two-segment corpus for one `target_series` value: one
/// segment at [`SEG_A_HOUR`], one at [`SEG_B_HOUR`], each carrying every series
/// with `samples_per_series` in-window samples at disjoint timestamps. Total
/// in-window samples per series is `2 * samples_per_series`.
async fn publish_corpus(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    tenant_hash: TenantHash,
    target_series: usize,
    samples_per_series: usize,
) {
    // Segment A: timestamps BASE_NS - 1s .. BASE_NS - samples_per_series s.
    publish_segment(
        store,
        tenant,
        tenant_hash,
        1,
        SEG_A_HOUR,
        BASE_NS,
        target_series,
        samples_per_series,
        1,
    )
    .await;
    // Segment B: timestamps continue after A's, still inside the 5m window, so
    // no (series, ts) collides with A and the count is a deterministic
    // 2 * samples_per_series per series.
    publish_segment(
        store,
        tenant,
        tenant_hash,
        2,
        SEG_B_HOUR,
        i64::from(SEG_B_HOUR) * NS_PER_HOUR,
        target_series,
        samples_per_series,
        1 + samples_per_series as i64,
    )
    .await;
}

/// The resolved inputs and catalog for one arm, everything `run` needs to build
/// the engine and everything the smoke test needs to check the gate. Public so
/// the smoke test can call the SAME public `is_pushdown_eligible` on these exact
/// inputs (a pure, cannot-pass-by-accident structural signal).
pub struct ArmSetup {
    /// The catalog the engine must use so its internal gate decision matches
    /// [`Self::generations`]: default (non-enforcing) for the eligible arm, or
    /// `shard_count: 1` enforcing for the ineligible arm.
    pub catalog: Arc<Catalog>,
    pub tenant_hash: TenantHash,
    /// The segments this arm resolved (the worker's resolver is seeded with
    /// these and the gate reads their ingest-hour buckets).
    pub segments: Vec<SegmentRef>,
    /// The shard-generation history this resolve read: a single implicit
    /// generation on the eligible arm, two generations on the ineligible arm.
    pub generations: Vec<ShardGeneration>,
}

/// Publish the identical corpus into `store` under a fresh tenant, record the
/// arm's generation history (ineligible arm only), build the arm's catalog, and
/// resolve the segment set plus generation history the engine's gate will see.
/// The eligible/ineligible construction mirrors
/// `crates/ravel-query/src/engine.rs`'s `count_over_time_pushdown_on_and_off_agree`
/// test exactly, including the enforcing catalog on the ineligible arm.
pub async fn build_arm(
    store: Arc<dyn ObjectStoreBackend>,
    target_series: usize,
    samples_per_series: usize,
    eligible: bool,
) -> ArmSetup {
    let tenant = TenantId::new(format!("bench-tenant-{}", Uuid::new_v4()));
    let tenant_hash = tenant.hash();

    publish_corpus(
        store.as_ref(),
        &tenant,
        tenant_hash,
        target_series,
        samples_per_series,
    )
    .await;

    let catalog = if eligible {
        // Non-enforcing catalog: `read_scan_generations` synthesizes an implicit
        // single generation with no store read, so the gate sees one generation
        // owning every hour.
        Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog")
    } else {
        // Record a second shard generation activating between the two segments'
        // ingest hours, then query through an enforcing `shard_count: 1` catalog
        // so `read_scan_generations` reads the real two-generation record rather
        // than synthesizing one. The two segments then straddle the boundary and
        // the gate returns false.
        validate_or_adopt(
            store.as_ref(),
            &tenant_hash,
            Signal::Metrics,
            1,
            0,
            AbsentPolicy::CreateFromConfig,
        )
        .await
        .expect("create generation 0");
        append_generation(
            store.as_ref(),
            &tenant_hash,
            Signal::Metrics,
            2,
            GEN1_ACTIVATION_HOUR,
            i64::from(GEN1_ACTIVATION_HOUR - 1) * NS_PER_HOUR,
        )
        .await
        .expect("append generation 1");
        Catalog::new(
            Arc::clone(&store),
            CatalogConfig {
                shard_count: 1,
                ..CatalogConfig::default()
            },
        )
        .expect("catalog")
        .with_provisioning_enforcement()
    };
    let catalog = Arc::new(catalog);

    let range = TimeRange {
        start_ns: BASE_NS - NS_PER_HOUR,
        end_ns: RESOLVE_NOW_NS,
    };
    let accounting = QueryAccounting::new();
    let (snapshot, _origins, generations): (Snapshot, _, Vec<ShardGeneration>) = catalog
        .resolve_pruned_with_generations(
            &tenant_hash,
            Signal::Metrics,
            range,
            &[],
            RESOLVE_NOW_NS,
            None,
            &accounting,
        )
        .await
        .expect("resolve corpus snapshot");

    ArmSetup {
        catalog,
        tenant_hash,
        segments: snapshot.segments,
        generations,
    }
}

/// Starts one `tonic` `SeriesFetch` worker on `127.0.0.1:0` over `store` and the
/// full segment set, returning a client fetcher plus the server task handle
/// (abort it to shut the worker down). Re-derives `ravel-query`'s test-only
/// `spawn_metric_worker` from the public API.
async fn spawn_worker(
    store: Arc<dyn ObjectStoreBackend>,
    segments: Vec<SegmentRef>,
) -> (RemoteSliceFetcher, JoinHandle<()>) {
    let fetcher = SegmentFetcher::new(store);
    let resolver = Arc::new(SnapshotSegmentResolver::new(segments));
    let service = SeriesFetchService::new(fetcher, resolver).into_server();

    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("addr")).expect("bind worker");
    let addr = incoming.local_addr().expect("worker local addr");
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .expect("serve worker");
    });
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("worker endpoint")
        .connect_lazy();
    (RemoteSliceFetcher::new(channel), handle)
}

/// Runs `reps` timed instant queries against `engine`, returning the wall-time
/// samples plus the query's matched-series count, summed count value, and stats.
/// Stats are deterministic across reps (the same pinned snapshot and fetch), so
/// the first rep's values stand for the panel.
async fn measure(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    t_ms: i64,
    deadline: Duration,
    reps: usize,
) -> (Vec<u64>, usize, f64, QueryStats) {
    let reps = reps.max(1);
    let mut wall_ns = Vec::with_capacity(reps);
    let mut matched = 0usize;
    let mut count_sum = 0.0f64;
    let mut stats = QueryStats::default();
    for i in 0..reps {
        let start = Instant::now();
        let (value, s) = engine
            .instant_with_stats(tenant_hash, QUERY, t_ms, &[], RESOLVE_NOW_NS, deadline)
            .await
            .expect("instant query");
        wall_ns.push(start.elapsed().as_nanos() as u64);
        if i == 0 {
            if let Value::Vector(v) = value {
                matched = v.len();
                count_sum = v.iter().map(|sample| sample.value).sum();
            }
            stats = s;
        }
    }
    (wall_ns, matched, count_sum, stats)
}

#[allow(clippy::too_many_arguments)]
fn arm_report(
    target_series: usize,
    eligible: bool,
    pushdown_eligible: bool,
    wall_ns: Vec<u64>,
    matched_series: usize,
    evaluated_count_sum: f64,
    stats: &QueryStats,
    corpus_segments: usize,
) -> ArmReport {
    let mut sorted = wall_ns;
    sorted.sort_unstable();
    let acct = &stats.accounting;
    ArmReport {
        target_series,
        eligible,
        pushdown_eligible,
        wall_ms_p50: percentile(&sorted, 0.50) as f64 / 1e6,
        wall_ms_p90: percentile(&sorted, 0.90) as f64 / 1e6,
        wall_ms_p99: percentile(&sorted, 0.99) as f64 / 1e6,
        wall_ms_max: sorted.last().copied().unwrap_or(0) as f64 / 1e6,
        matched_series,
        evaluated_count_sum,
        segments_fetched: stats.segments_fetched,
        s3_requests: acct.total_s3_requests(),
        s3_get_requests: acct.s3_requests(AccountedOp::Get),
        s3_bytes: acct.total_s3_bytes(),
        decompressed_bytes: acct.decompressed_bytes,
        corpus_segments,
    }
}

/// Build both arms at every `target_series` value and measure the fixed query
/// through the real distributed loopback worker, returning the full report.
pub async fn run(config: &PushdownCrossoverConfig) -> Report {
    let store = Arc::clone(&config.store);
    let instant_t_ms = BASE_NS / 1_000_000;
    let deadline = Duration::from_secs(config.deadline_secs.max(1));

    let mut arms = Vec::with_capacity(config.target_series.len() * 2);
    for &target_series in &config.target_series {
        for eligible in [true, false] {
            let setup = build_arm(
                Arc::clone(&store),
                target_series,
                config.samples_per_series,
                eligible,
            )
            .await;
            // The structural gate decision, computed from the exact inputs the
            // engine's own gate reads. Recorded so a broken construction (both
            // arms taking the same path) is visible in the report.
            let pushdown_eligible = is_pushdown_eligible(None, &setup.segments, &setup.generations);
            let corpus_segments = setup.segments.len();

            let (worker, handle) = spawn_worker(Arc::clone(&store), setup.segments.clone()).await;
            let distributed = Distributed::new(Arc::new(worker), zero_thresholds());
            let engine = QueryEngine::new(
                Arc::clone(&setup.catalog),
                Arc::clone(&store),
                EngineConfig::default(),
            )
            .with_distributed(Arc::new(distributed));

            let (wall_ns, matched, count_sum, stats) = measure(
                &engine,
                setup.tenant_hash,
                instant_t_ms,
                deadline,
                config.query_reps,
            )
            .await;
            handle.abort();

            arms.push(arm_report(
                target_series,
                eligible,
                pushdown_eligible,
                wall_ns,
                matched,
                count_sum,
                &stats,
                corpus_segments,
            ));
        }
    }

    Report {
        config: ReportConfig {
            store: config.store_label.clone(),
            query: QUERY.to_string(),
            target_series: config.target_series.clone(),
            samples_per_series: config.samples_per_series,
            query_reps: config.query_reps,
        },
        arms,
    }
}
