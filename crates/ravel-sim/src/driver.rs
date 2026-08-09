//! Driver (ADR-0068 deliverable 4/5): one seeded ingest -> fold -> query
//! cycle over `MemoryStore`, wired through the same `IngestRouter` ->
//! `Catalog` -> `QueryEngine` path production traffic takes, plus the
//! invariant checks from deliverable 5.
//!
//! Runs on a single-threaded, paused-clock tokio runtime
//! (`current_thread`/`start_paused`) that this module owns end to end:
//! `SimClock::sleep` delegates to the runtime's virtual timer (the
//! `Clock::sleep` default impl), so every await on it fast-forwards
//! instantly instead of costing wall-clock time, and the whole cycle stays
//! reproducible because nothing here ever reads a real clock.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use rand::RngExt;
use rand::rngs::StdRng;
use ravel_catalog::{
    Catalog, CatalogConfig, CatalogError, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_FOLD_SAFETY_MARGIN_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_ingest::{
    Clock, IngestConfig, IngestPoint, IngestRouter, IngestValue, WriteError, WriteMode,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_promql::Value;
use ravel_query::{EngineConfig, QueryEngine, QueryError};
use ravel_segment::HistogramCounts;
use ravel_types::{CommitToken, METRIC_NAME_LABEL, Signal, TenantHash, TimeRange, TypeError};
use uuid::Uuid;

use crate::digest::{Digest, DigestBuilder, mix_value};
use crate::seed::MasterSeed;
use crate::workload::{QuerySpec, SeriesSamples, Workload, WorkloadConfig};

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Knobs for [`run_cycle`]. `shard_count` applies uniformly to both
/// `IngestRouter` and `Catalog` (they must agree: the shard a series routes
/// to at write time is the shard the catalog looks it up under).
#[derive(Debug, Clone)]
pub struct CycleConfig {
    pub workload: WorkloadConfig,
    pub shard_count: u32,
    pub ack_deadline: Duration,
    pub query_deadline: Duration,
    /// Upper bound on the per-tenant scheduling jitter drawn from the
    /// `"clock"` sub-seed and slept (on the paused virtual clock) before
    /// that tenant's write, so the clock-stepping domain does something
    /// observable independent of the workload domain.
    pub max_jitter: Duration,
}

impl Default for CycleConfig {
    fn default() -> Self {
        CycleConfig {
            workload: WorkloadConfig::default(),
            shard_count: 2,
            ack_deadline: Duration::from_secs(5),
            query_deadline: Duration::from_secs(5),
            max_jitter: Duration::from_millis(250),
        }
    }
}

/// What a successful cycle produced, for the caller to log or assert on.
#[derive(Debug, Clone)]
pub struct CycleOutcome {
    pub master_seed: MasterSeed,
    pub digest: Digest,
    pub tenants_run: usize,
    pub series_generated: usize,
    pub queries_run: usize,
}

/// Every variant carries the master seed so a failure message alone is
/// enough to replay it: `RAVEL_SIM_SEED=<seed> cargo test -p ravel-sim
/// <test>`, for a test whose seed comes from
/// [`crate::seed::MasterSeed::from_env_or`] (see that method's doc for which
/// ones do).
#[derive(Debug, thiserror::Error)]
pub enum CycleError {
    #[error("seed {seed}: workload generation failed: {source}")]
    Workload { seed: u64, source: TypeError },
    #[error("seed {seed}: could not build the paused simulation runtime: {source}")]
    Runtime { seed: u64, source: std::io::Error },
    #[error("seed {seed}: catalog config rejected: {source}")]
    CatalogConfig { seed: u64, source: CatalogError },
    #[error("seed {seed}: ingest write failed: {source}")]
    Ingest { seed: u64, source: WriteError },
    #[error("seed {seed}: catalog fold failed: {source}")]
    Fold { seed: u64, source: CatalogError },
    #[error("seed {seed}: catalog resolve failed: {source}")]
    Resolve { seed: u64, source: CatalogError },
    #[error("seed {seed}: query failed: {source}")]
    Query { seed: u64, source: QueryError },
    #[error(
        "seed {seed}: read-your-write violated for tenant {tenant}: commit token {token} did \
         not resolve, or the query engine did not return the samples it acked"
    )]
    ReadYourWrite {
        seed: u64,
        tenant: String,
        token: String,
    },
    #[error(
        "seed {seed}: strict-ack-implies-durable violated for tenant {tenant}: commit token \
         {token} did not resolve after the fold, or the folded snapshot did not return the \
         acked samples"
    )]
    AckNotDurable {
        seed: u64,
        tenant: String,
        token: String,
    },
}

/// Clock the driver hands to `IngestRouter`: `now_ns` is a plain atomic
/// counter, and `sleep` uses [`Clock::sleep`]'s default implementation
/// (delegates straight to `tokio::time::sleep`), then advances the counter
/// by the slept duration once that completes. Under a paused tokio runtime
/// this fast-forwards instantly and always advances in the same order the
/// awaited sleeps were issued, so two runs with the same seed see the same
/// sequence of `now_ns` values.
struct SimClock {
    now_ns: AtomicI64,
}

impl SimClock {
    fn new(start_ns: i64) -> Self {
        SimClock {
            now_ns: AtomicI64::new(start_ns),
        }
    }

    /// Advances `now_ns` to `target_ns`, monotonically: a `target_ns` at or
    /// behind the current value is a no-op. Lets the driver track the
    /// clock to each ingest batch's own event time (F13) without ever
    /// stepping it backwards, regardless of generation order.
    fn advance_to(&self, target_ns: i64) {
        self.now_ns.fetch_max(target_ns, Ordering::SeqCst);
    }
}

impl Clock for SimClock {
    fn now_ns(&self) -> i64 {
        self.now_ns.load(Ordering::SeqCst)
    }

    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            tokio::time::sleep(dur).await;
            self.now_ns
                .fetch_add(dur.as_nanos() as i64, Ordering::SeqCst);
        })
    }
}

/// The `now_ns` a fold must be given for `end_ts_ns`'s hour to be sealed:
/// exactly `sealed_watermark_hour`'s threshold, using the same three
/// margins it sums (`docs/catalog-and-mvcc.md`). `Catalog::fold`/`resolve`
/// never read a clock themselves, so this can be handed in as a literal
/// without the simulated clock needing to live through the gap.
fn seal_now_ns(end_ts_ns: i64) -> i64 {
    let margin_ns = DEFAULT_MAX_FLUSH_LIFETIME_NS
        + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
        + DEFAULT_FOLD_SAFETY_MARGIN_NS;
    let hour = end_ts_ns.div_euclid(NS_PER_HOUR);
    (hour + 1) * NS_PER_HOUR + margin_ns
}

/// Sample count of one generated series, so the driver can batch ingest by
/// index across every series in a tenant (F13: every series in one tenant
/// shares `start_ts_ns`/`interval_ns`, so index `i` maps to the same event
/// timestamp in every series -- see `workload::generate_tenant_series`).
fn sample_count(series: &crate::workload::GeneratedSeries) -> usize {
    match &series.samples {
        SeriesSamples::Scalar(points) => points.len(),
        SeriesSamples::Histogram(points) => points.len(),
    }
}

/// The event timestamp and ingest point for `series`' sample at index `i`,
/// or `None` if that series has fewer than `i + 1` samples.
fn ingest_point_at(
    series: &crate::workload::GeneratedSeries,
    i: usize,
) -> Option<(i64, IngestPoint)> {
    match &series.samples {
        SeriesSamples::Scalar(points) => points.get(i).map(|sample| {
            (
                sample.ts_ns,
                IngestPoint {
                    series_id: series.series_id,
                    labels: series.labels.clone(),
                    value: IngestValue::Scalar(*sample),
                },
            )
        }),
        SeriesSamples::Histogram(points) => points.get(i).map(|sample| {
            (
                sample.ts_ns,
                IngestPoint {
                    series_id: series.series_id,
                    labels: series.labels.clone(),
                    value: IngestValue::Histogram(sample.clone()),
                },
            )
        }),
    }
}

/// What `check_visible` (F3) expects a query pinned at `t_ms` to return for
/// one generated series: the exact label set the query result carries (for
/// a histogram query, `histogram_count()` drops `__name__` -- ravel-promql's
/// `drop_metric_name`, confirmed by its own
/// `histogram_count_reads_count_and_drops_name` test), the sample's own
/// stored timestamp (`InstantSample::orig_sample_ts_ns`, not the query eval
/// timestamp), and its value compared by bit pattern.
struct ExpectedSample<'a> {
    query: String,
    t_ms: i64,
    orig_ts_ns: i64,
    label_pairs: Vec<(&'a str, &'a str)>,
    value_bits: u64,
}

fn label_pairs(labels: &ravel_types::LabelSet) -> Vec<(&str, &str)> {
    labels
        .iter()
        .map(|l| (l.name.as_str(), l.value.as_str()))
        .collect()
}

fn expected_sample(series: &crate::workload::GeneratedSeries) -> Option<ExpectedSample<'_>> {
    match &series.samples {
        SeriesSamples::Scalar(points) => {
            let last = points.last()?;
            Some(ExpectedSample {
                query: series.metric_name.clone(),
                t_ms: last.ts_ns / 1_000_000,
                orig_ts_ns: last.ts_ns,
                label_pairs: label_pairs(&series.labels),
                value_bits: last.value.to_bits(),
            })
        }
        SeriesSamples::Histogram(points) => {
            let last = points.last()?;
            let count = match &last.value.counts {
                HistogramCounts::Int { count, .. } => *count as f64,
                HistogramCounts::Float { count, .. } => *count,
            };
            let name_dropped_pairs = series
                .labels
                .iter()
                .filter(|l| l.name != METRIC_NAME_LABEL)
                .map(|l| (l.name.as_str(), l.value.as_str()))
                .collect();
            Some(ExpectedSample {
                query: format!("histogram_count({})", series.metric_name),
                t_ms: last.ts_ns / 1_000_000,
                orig_ts_ns: last.ts_ns,
                label_pairs: name_dropped_pairs,
                value_bits: count.to_bits(),
            })
        }
    }
}

/// Runs one full seeded cycle: generate the workload, ingest it tenant by
/// tenant through a strict-mode `IngestRouter`, check read-your-write on
/// each tenant's own acked tokens, fold the sealed window, check
/// strict-ack-implies-durable on the same tokens, run the workload's query
/// mix, and fold every result into a reproducibility digest.
pub fn run_cycle(
    master_seed: MasterSeed,
    config: &CycleConfig,
) -> Result<CycleOutcome, CycleError> {
    let seed = master_seed.0;
    let workload = crate::workload::generate(&master_seed, &config.workload)
        .map_err(|source| CycleError::Workload { seed, source })?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .map_err(|source| CycleError::Runtime { seed, source })?;

    rt.block_on(run_cycle_async(seed, master_seed, workload, config))
}

async fn run_cycle_async(
    seed: u64,
    master_seed: MasterSeed,
    workload: Workload,
    config: &CycleConfig,
) -> Result<CycleOutcome, CycleError> {
    let memory_store = Arc::new(MemoryStore::new());
    memory_store.set_clock_ms((workload.start_ts_ns / 1_000_000).max(0) as u64);
    let store: Arc<dyn ObjectStoreBackend> = memory_store;

    let sim_clock = Arc::new(SimClock::new(workload.start_ts_ns));
    let clock: Arc<dyn Clock> = Arc::clone(&sim_clock) as Arc<dyn Clock>;
    let mut clock_rng: StdRng = master_seed.rng("clock");

    let catalog_config = CatalogConfig {
        shard_count: config.shard_count,
        ..CatalogConfig::default()
    };
    let catalog = Arc::new(
        Catalog::new(Arc::clone(&store), catalog_config)
            .map_err(|source| CycleError::CatalogConfig { seed, source })?,
    );
    let engine = QueryEngine::new(
        Arc::clone(&catalog),
        Arc::clone(&store),
        EngineConfig::default(),
    );

    let fold_now_ns = seal_now_ns(workload.end_ts_ns);
    let full_range = TimeRange {
        start_ns: workload.start_ts_ns,
        end_ns: workload.end_ts_ns,
    };

    let mut digest_builder = DigestBuilder::new();
    let mut series_generated = 0usize;
    let mut queries_run = 0usize;

    for tenant_wl in &workload.tenants {
        series_generated += tenant_wl.series.len();
        let tenant_hash: TenantHash = tenant_wl.tenant.hash();

        let jitter_ms = clock_rng.random_range(0..=config.max_jitter.as_millis().max(1) as u64);
        clock.sleep(Duration::from_millis(jitter_ms)).await;

        let ingest_config = IngestConfig {
            shard_count: config.shard_count,
            ..IngestConfig::default()
        };
        let router = IngestRouter::new(
            ingest_config,
            Arc::clone(&store),
            Signal::Metrics,
            Arc::clone(&clock),
        );

        // F13: ingest in per-sample-index batches rather than one batch for
        // the whole tenant, advancing `sim_clock` to each batch's own event
        // timestamp first. `flush_open_ns` (the wall/arrival clock read
        // fresh at each strict-mode flush, `ravel-ingest`'s
        // `checked_ingest_hour_bucket`) is what a commit's
        // `ingest_hour_bucket` derives from; ingesting the whole tenant in
        // one call at a single instant left every commit under one bucket
        // that could disagree with the query window's event-time-hour once
        // `interval_ns` was wide enough to span hours, failing
        // `AckNotDurable` even though nothing was actually lost. Every
        // series in one tenant shares `start_ts_ns`/`interval_ns`
        // (`workload::generate_tenant_series`), so sample index `i` is the
        // same event timestamp across all of them.
        let max_samples = tenant_wl.series.iter().map(sample_count).max().unwrap_or(0);
        let mut tokens: Vec<CommitToken> = Vec::new();
        for i in 0..max_samples {
            let mut batch_ts_ns = None;
            let mut points = Vec::with_capacity(tenant_wl.series.len());
            for series in &tenant_wl.series {
                if let Some((ts_ns, point)) = ingest_point_at(series, i) {
                    batch_ts_ns.get_or_insert(ts_ns);
                    points.push(point);
                }
            }
            if points.is_empty() {
                continue;
            }
            sim_clock.advance_to(batch_ts_ns.unwrap_or(workload.start_ts_ns));

            let receipt = router
                .write_values(
                    tenant_wl.tenant.clone(),
                    points,
                    WriteMode::Strict,
                    config.ack_deadline,
                )
                .await
                .map_err(|source| CycleError::Ingest { seed, source })?;
            tokens.extend(receipt.tokens);
        }

        let ack_wall_ns = clock.now_ns();

        // Invariant (b), read-your-write (docs/consistency-model.md: "A
        // caller holding commit tokens sees the referenced commits by
        // passing min_commit_token to query APIs"): right after ack, every
        // token every batch returned must resolve, and a query pinned to
        // those tokens must return the samples it acked -- before any fold
        // has run. F4: every generated series is checked, not just the
        // tenant's first.
        check_visible(
            seed,
            tenant_wl.tenant.as_str(),
            &catalog,
            &engine,
            &tenant_hash,
            full_range,
            &tokens,
            ack_wall_ns,
            config.query_deadline,
            &tenant_wl.series,
            CheckKind::ReadYourWrite,
        )
        .await?;

        catalog
            .fold(
                &tenant_hash,
                Signal::Metrics,
                Uuid::new_v4(),
                fold_now_ns,
                &[],
            )
            .await
            .map_err(|source| CycleError::Fold { seed, source })?;

        // Invariant (a), strict-ack-implies-durable: after the fold, the
        // same tokens still resolve, and -- unlike the read-your-write
        // check above -- a query with NO min_tokens (relying purely on the
        // catalog's own sealed listing/snapshot, not the caller's pinned
        // tokens) must also return the acked samples. F4: every generated
        // series is checked, not just the tenant's first.
        check_visible(
            seed,
            tenant_wl.tenant.as_str(),
            &catalog,
            &engine,
            &tenant_hash,
            full_range,
            &tokens,
            fold_now_ns,
            config.query_deadline,
            &tenant_wl.series,
            CheckKind::AckDurable,
        )
        .await?;

        digest_builder.mix_str(tenant_wl.tenant.as_str());
        for query in &tenant_wl.queries {
            let value = run_query(
                &engine,
                tenant_hash,
                query,
                fold_now_ns,
                config.query_deadline,
            )
            .await
            .map_err(|source| CycleError::Query { seed, source })?;
            mix_value(&mut digest_builder, &value);
            queries_run += 1;
        }
    }

    Ok(CycleOutcome {
        master_seed,
        digest: digest_builder.finish(),
        tenants_run: workload.tenants.len(),
        series_generated,
        queries_run,
    })
}

#[derive(Debug, Clone, Copy)]
enum CheckKind {
    ReadYourWrite,
    AckDurable,
}

/// Checks both invariants for every one of `series_list`'s generated series
/// (F4): every token resolves (F12: a genuine store/decode `CatalogError`
/// propagates as [`CycleError::Resolve`], distinct from
/// [`CatalogError::UnsatisfiableToken`], the one variant that means the
/// invariant itself was violated), and a query pinned at each series' own
/// last sample returns exactly that sample back -- label set, real stored
/// timestamp, and value bit pattern all matching (F3), not just "some
/// non-empty vector".
#[allow(clippy::too_many_arguments)]
async fn check_visible(
    seed: u64,
    tenant_label: &str,
    catalog: &Catalog,
    engine: &QueryEngine,
    tenant_hash: &TenantHash,
    full_range: TimeRange,
    tokens: &[CommitToken],
    now_ns: i64,
    deadline: Duration,
    series_list: &[crate::workload::GeneratedSeries],
    kind: CheckKind,
) -> Result<(), CycleError> {
    for token in tokens {
        let min_tokens = std::slice::from_ref(token);
        match catalog
            .resolve(tenant_hash, Signal::Metrics, full_range, min_tokens, now_ns)
            .await
        {
            Ok(_) => {}
            Err(CatalogError::UnsatisfiableToken { .. }) => {
                return Err(violation(seed, tenant_label, token, kind));
            }
            Err(source) => {
                return Err(CycleError::Resolve { seed, source });
            }
        }
    }

    let query_min_tokens: &[CommitToken] = match kind {
        CheckKind::ReadYourWrite => tokens,
        CheckKind::AckDurable => &[],
    };
    let fallback_token = tokens.first().cloned().unwrap_or(CommitToken {
        shard: 0,
        writer_id: Uuid::nil(),
        epoch: 0,
        seq: 0,
        ingest_hour_bucket: 0,
    });

    for series in series_list {
        let Some(expected) = expected_sample(series) else {
            continue;
        };
        let value = engine
            .instant(
                *tenant_hash,
                &expected.query,
                expected.t_ms,
                query_min_tokens,
                now_ns,
                deadline,
            )
            .await
            .map_err(|source| CycleError::Query { seed, source })?;
        let matched = match &value {
            Value::Vector(v) => v.iter().any(|sample| {
                label_pairs(&sample.labels) == expected.label_pairs
                    && sample.orig_sample_ts_ns == expected.orig_ts_ns
                    && sample.value.to_bits() == expected.value_bits
            }),
            _ => false,
        };
        if !matched {
            return Err(violation(seed, tenant_label, &fallback_token, kind));
        }
    }
    Ok(())
}

fn violation(seed: u64, tenant_label: &str, token: &CommitToken, kind: CheckKind) -> CycleError {
    let token = token.encode();
    match kind {
        CheckKind::ReadYourWrite => CycleError::ReadYourWrite {
            seed,
            tenant: tenant_label.to_string(),
            token,
        },
        CheckKind::AckDurable => CycleError::AckNotDurable {
            seed,
            tenant: tenant_label.to_string(),
            token,
        },
    }
}

async fn run_query(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    query: &QuerySpec,
    now_ns: i64,
    deadline: Duration,
) -> Result<Value, QueryError> {
    match query {
        QuerySpec::Instant { query, t_ms } => {
            engine
                .instant(tenant_hash, query, *t_ms, &[], now_ns, deadline)
                .await
        }
        QuerySpec::Range {
            query,
            start_ms,
            end_ms,
            step_ms,
        } => {
            engine
                .range(
                    tenant_hash,
                    query,
                    *start_ms,
                    *end_ms,
                    *step_ms,
                    &[],
                    now_ns,
                    deadline,
                )
                .await
        }
    }
}
