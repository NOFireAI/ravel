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
use ravel_types::{CommitToken, Signal, TenantHash, TimeRange, TypeError};
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
/// enough to replay it: `RAVEL_SIM_SEED=<seed> cargo test -p ravel-sim`.
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

fn to_ingest_points(series: &crate::workload::GeneratedSeries) -> Vec<IngestPoint> {
    match &series.samples {
        SeriesSamples::Scalar(points) => points
            .iter()
            .map(|sample| IngestPoint {
                series_id: series.series_id,
                labels: series.labels.clone(),
                value: IngestValue::Scalar(*sample),
            })
            .collect(),
        SeriesSamples::Histogram(points) => points
            .iter()
            .map(|sample| IngestPoint {
                series_id: series.series_id,
                labels: series.labels.clone(),
                value: IngestValue::Histogram(sample.clone()),
            })
            .collect(),
    }
}

fn representative_query(series: &crate::workload::GeneratedSeries, t_ms: i64) -> (String, i64) {
    let query = match series.samples {
        SeriesSamples::Histogram(_) => format!("histogram_count({})", series.metric_name),
        SeriesSamples::Scalar(_) => series.metric_name.clone(),
    };
    (query, t_ms)
}

fn last_sample_ts_ms(series: &crate::workload::GeneratedSeries) -> Option<i64> {
    match &series.samples {
        SeriesSamples::Scalar(points) => points.last().map(|s| s.ts_ns / 1_000_000),
        SeriesSamples::Histogram(points) => points.last().map(|s| s.ts_ns / 1_000_000),
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

    let clock: Arc<dyn Clock> = Arc::new(SimClock::new(workload.start_ts_ns));
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

        let points: Vec<IngestPoint> = tenant_wl.series.iter().flat_map(to_ingest_points).collect();

        let receipt = router
            .write_values(
                tenant_wl.tenant.clone(),
                points,
                WriteMode::Strict,
                config.ack_deadline,
            )
            .await
            .map_err(|source| CycleError::Ingest { seed, source })?;
        router.flush_all().await;

        let ack_wall_ns = clock.now_ns();

        // Invariant (b), read-your-write (docs/consistency-model.md: "A
        // caller holding commit tokens sees the referenced commits by
        // passing min_commit_token to query APIs"): right after ack, every
        // token this write returned must resolve, and a query pinned to
        // those tokens must return the sample it acked -- before any fold
        // has run.
        if let Some(first_series) = tenant_wl.series.first() {
            check_visible(
                seed,
                tenant_wl.tenant.as_str(),
                &catalog,
                &engine,
                &tenant_hash,
                full_range,
                &receipt.tokens,
                ack_wall_ns,
                config.query_deadline,
                first_series,
                CheckKind::ReadYourWrite,
            )
            .await?;
        }

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
        // tokens) must also return the acked samples.
        if let Some(first_series) = tenant_wl.series.first() {
            check_visible(
                seed,
                tenant_wl.tenant.as_str(),
                &catalog,
                &engine,
                &tenant_hash,
                full_range,
                &receipt.tokens,
                fold_now_ns,
                config.query_deadline,
                first_series,
                CheckKind::AckDurable,
            )
            .await?;
        }

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

enum CheckKind {
    ReadYourWrite,
    AckDurable,
}

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
    representative: &crate::workload::GeneratedSeries,
    kind: CheckKind,
) -> Result<(), CycleError> {
    for token in tokens {
        let min_tokens = std::slice::from_ref(token);
        let resolved = catalog
            .resolve(tenant_hash, Signal::Metrics, full_range, min_tokens, now_ns)
            .await;
        let token_ok = resolved.is_ok();
        if !token_ok {
            return Err(violation(seed, tenant_label, token, kind));
        }
    }

    let Some(t_ms) = last_sample_ts_ms(representative) else {
        return Ok(());
    };
    let (query, t_ms) = representative_query(representative, t_ms);
    let query_min_tokens: &[CommitToken] = match kind {
        CheckKind::ReadYourWrite => tokens,
        CheckKind::AckDurable => &[],
    };
    let value = engine
        .instant(
            *tenant_hash,
            &query,
            t_ms,
            query_min_tokens,
            now_ns,
            deadline,
        )
        .await
        .map_err(|source| CycleError::Query { seed, source })?;
    let visible = match value {
        Value::Vector(v) => !v.is_empty(),
        _ => false,
    };
    if !visible {
        let token = tokens.first().cloned().unwrap_or(CommitToken {
            shard: 0,
            writer_id: Uuid::nil(),
            epoch: 0,
            seq: 0,
            ingest_hour_bucket: 0,
        });
        return Err(violation(seed, tenant_label, &token, kind));
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
