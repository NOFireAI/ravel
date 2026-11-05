//! Issue #3779 item 1: how often a pinned `ravel_query_sql` walk expires, and
//! why. The pre-registration is `mcp_walk_expiry.bands.toml` beside this file;
//! docs/internal/walk-expiry-aws-runbook.md runs it on one EC2 box.
//!
//! ```sh
//! cargo run --release -p ravel-bench --features walk-expiry --bin mcp_walk_expiry -- \
//!     --profile production --store s3 --out-dir /tmp/walk-expiry
//! ```
//!
//! In one process it starts two servers over the same store through
//! `ravel_server::start`: a `--mode all` server (ingest, query, scheduled
//! fold, and `POST /mcp`) and a `--mode maintain` server (compaction). The
//! flush, fold, maintenance and concurrency settings are what `ravel-server`'s
//! CLI parses with no flags; `server_config` sets the rest directly (audit
//! off, library query budgets, in-memory caches, no retention). A load task posts the
//! MetricsBench workload's series over Remote Write 1.0 to the first, one
//! scrape every interval at the wall-clock instant the scrape is stamped
//! with.
//!
//! Each probe walk runs page one of the statement through
//! `QueryService::sql_execute_at`, the call the MCP adapter makes, with the
//! `SqlRequest` `ravel_query_sql` builds (`Call::request`): the window as both
//! listing bound and row filter, `max_rows`, and the MCP budgets clamped
//! against the same engine ceiling. Later pages resume from the last row's
//! order tuple and resolve at page one's t0. A page whose segment-set digest
//! differs from page one's ends the walk as an expiry, labelled from the key
//! diff by `ravel_bench::walk_expiry::label_expiry`. The cross-check walks the
//! same S1 statement through `POST /mcp` itself.
//!
//! The process exits 0 when every pre-registered figure is present exactly
//! once and inside its band, every cell ran all its walks, and no stamp voids
//! the run; 1 otherwise, naming each failure; 2 when it could not measure.
//! `result.json` and `summary.txt` go to `--out-dir` either way.

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, bail};
use clap::{Parser, ValueEnum};
use ravel_bench::metrics_gen::{GenerationTally, Generator};
use ravel_bench::metrics_ingest::{LogicalPoint, StreamRecord, classify_line, encode_rw1_points};
use ravel_bench::metrics_workload::{Profile, WorkloadFile, load_workload};
use ravel_bench::walk_expiry::{
    self as we, BANDS_TOML, Causes, Cell, CellTally, CommitListing, KeyKind, L0Commit,
    ProfileConfig, Rates, Stamps, Statement, Transport, WalkEnd, WindowRole,
};
use ravel_bench::walk_probe::{PinnedWalk, WalkSpec};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_mcp::budget::{McpBudgetConfig, McpEffectiveBudgets, McpRequestBudgets};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::S3Store;
use ravel_object_store::{ObjectStoreBackend, StoreMetrics};
use ravel_server::config::{CpuGatePermits, McpConfig};
use ravel_server::service::QueryService;
use ravel_server::{Cli, FoldTaskConfig, MaintenanceTaskConfig, Mode, QueryBudgets, ServerConfig};
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use serde_json::{Value, json};

const DEFAULT_WORKLOAD: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../benchmarks/metrics/workload.json"
);
const TENANT: &str = "walk-expiry";
const TOKEN: &str = "walk-expiry-token";
const NS_PER_S: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_S;
const MCP_PROTOCOL: &str = "2026-07-28";
const MOVED_MESSAGE: &str = "the segments this walk reads changed";

#[derive(Clone, Copy, Debug, ValueEnum)]
enum ProfileArg {
    /// The pre-registered run: hours of load on a real store.
    Production,
    /// Seconds of load, windows of seconds, wide bands: proves the bin runs.
    Smoke,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum StoreArg {
    /// An in-process `MemoryStore`.
    Memory,
    /// S3 from the `RAVEL_S3_*` environment (`ravel_bench::harness`).
    S3,
}

#[derive(Parser, Debug)]
#[command(about = "Issue #3779: how often a pinned ravel_query_sql walk expires, and why")]
struct Args {
    /// Which section of the bands file to run. No default: it selects the
    /// whole run's shape.
    #[arg(long, value_enum)]
    profile: ProfileArg,
    /// Which store to run against. No default, for the same reason.
    #[arg(long, value_enum)]
    store: StoreArg,
    /// Where `result.json` and `summary.txt` are written.
    #[arg(long)]
    out_dir: PathBuf,
    /// The MetricsBench workload manifest the load is drawn from.
    #[arg(long, default_value = DEFAULT_WORKLOAD)]
    workload: PathBuf,
    /// A bands file to read instead of the checked-in one.
    #[arg(long)]
    bands: Option<PathBuf>,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(err) => {
            println!("RESULT: could not measure: runtime: {err}");
            return ExitCode::from(2);
        }
    };
    match runtime.block_on(run(args)) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(err) => {
            println!("RESULT: could not measure: {err:#}");
            ExitCode::from(2)
        }
    }
}

fn wall_now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

async fn sleep_until_ns(at_ns: i64) {
    let wait = at_ns - wall_now_ns();
    if wait > 0 {
        tokio::time::sleep(Duration::from_nanos(wait.unsigned_abs())).await;
    }
}

/// Everything a walk needs, shared across the pool.
struct Ctx {
    config: ProfileConfig,
    service: QueryService,
    catalog: Catalog,
    tenant_hash: TenantHash,
    budgets: BTreeMap<Statement, McpEffectiveBudgets>,
    http: reqwest::Client,
    mcp_url: String,
    /// Wo walks whose newest ingest hour was past the fold watermark at t0.
    wo_unsealed: AtomicU64,
    pages: AtomicU64,
    pages_with_pending_erasure: AtomicU64,
    unresolved_hour_keys: AtomicU64,
    wr_wo_hours: Mutex<BTreeSet<u32>>,
}

async fn run(args: Args) -> anyhow::Result<bool> {
    let bands_text = match &args.bands {
        Some(path) => {
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?
        }
        None => BANDS_TOML.to_string(),
    };
    let bands = we::load_bands(&bands_text)?;
    let (profile_name, config) = match args.profile {
        ProfileArg::Production => ("production", bands.production),
        ProfileArg::Smoke => ("smoke", bands.smoke),
    };
    if config.load.scrape_interval_ms % 1000 != 0 {
        bail!("scrape_interval_ms must be whole seconds for the MetricsBench generator");
    }
    std::fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("creating {}", args.out_dir.display()))?;
    let workload: &'static WorkloadFile = Box::leak(Box::new(
        load_workload(&args.workload).context("loading the MetricsBench workload")?,
    ));
    let load_profile: &'static Profile = Box::leak(Box::new(
        we::load_profile(workload, &config).context("the workload manifest has no ci profile")?,
    ));

    let store: Arc<dyn ObjectStoreBackend> = match args.store {
        StoreArg::Memory => Arc::new(MemoryStore::new()),
        StoreArg::S3 => Arc::new(
            S3Store::new(ravel_bench::harness::s3_config_from_env())
                .context("building the S3 store from RAVEL_S3_*")?,
        ),
    };
    let tenant = TenantId::new(TENANT);
    let tenant_hash = tenant.hash();
    let cli = Cli::parse_validated_from(["ravel-server", "--mode", "all"])
        .map_err(|e| anyhow::anyhow!("parsing ravel-server's default flags: {e}"))?;
    let all_config = server_config(&cli, Mode::All, &tenant)?;
    let engine_config = ravel_server::query::build_engine_config(
        &all_config,
        &CatalogConfig {
            shard_count: cli.shards,
            ..CatalogConfig::default()
        },
    )
    .map_err(|e| anyhow::anyhow!("engine config: {e}"))?;
    let maintain_config = server_config(&cli, Mode::Maintain, &tenant)?;
    // The window the compactor's retention sweep applies to this tenant;
    // `None` never age-deletes.
    let retention_days = maintain_config
        .maintain
        .retention
        .window_for(&tenant_hash)
        .map(|ns| u64::try_from(ns / (86_400 * NS_PER_S)).unwrap_or(0));
    let all = ravel_server::start(
        all_config,
        store.clone(),
        store.clone(),
        Arc::new(StoreMetrics::default()),
        None,
    )
    .await
    .context("starting the --mode all server")?;
    let maintain = ravel_server::start(
        maintain_config,
        store.clone(),
        store.clone(),
        Arc::new(StoreMetrics::default()),
        None,
    )
    .await
    .context("starting the --mode maintain server")?;
    let service = all
        .query_service
        .clone()
        .context("the --mode all server built no query service")?;
    let http_addr = all.http_addr;
    let budgets = Statement::ALL
        .into_iter()
        .map(|statement| {
            let budgets = McpRequestBudgets {
                max_rows: Some(config.load.max_rows(statement)),
                ..McpRequestBudgets::default()
            }
            .clamp(&engine_config, &McpBudgetConfig::default());
            (statement, budgets)
        })
        .collect();
    let ctx = Arc::new(Ctx {
        config: config.clone(),
        service,
        catalog: Catalog::new(
            store.clone(),
            CatalogConfig {
                shard_count: cli.shards,
                ..CatalogConfig::default()
            },
        )
        .context("building the probe's catalog")?,
        tenant_hash,
        budgets,
        http: reqwest::Client::builder().build()?,
        mcp_url: format!("http://{http_addr}/mcp"),
        wo_unsealed: AtomicU64::new(0),
        pages: AtomicU64::new(0),
        pages_with_pending_erasure: AtomicU64::new(0),
        unresolved_hour_keys: AtomicU64::new(0),
        wr_wo_hours: Mutex::new(BTreeSet::new()),
    });
    println!(
        "profile {profile_name}: {} active series every {} ms, {} shards, store {:?}",
        config.load.active_series, config.load.scrape_interval_ms, cli.shards, args.store
    );

    let load_start_ns = wall_now_ns();
    let stop = Arc::new(AtomicBool::new(false));
    let ledger = Arc::new(Mutex::new(LoadLedger::default()));
    let load = tokio::spawn(run_load(
        workload,
        load_profile,
        config.load.clone(),
        format!("http://{http_addr}/api/v1/write"),
        load_start_ns,
        Arc::clone(&stop),
        Arc::clone(&ledger),
    ));
    let sampler_state = Arc::new(Mutex::new(SamplerState::default()));
    let sampler = tokio::spawn(run_sampler(
        Arc::clone(&ctx),
        load_start_ns,
        Arc::clone(&stop),
        Arc::clone(&sampler_state),
    ));

    let (tallies, walk_start_ns, walk_end_ns, mut errors) =
        run_phases(Arc::clone(&ctx), load_start_ns).await;
    stop.store(true, Ordering::SeqCst);
    let load_result = load.await.context("load task")?;
    let sampler_result = sampler.await.context("sampler task")?;
    if let Err(err) = &load_result {
        errors.push(format!("load: {err:#}"));
    }
    if let Err(err) = &sampler_result {
        errors.push(format!("sampler: {err:#}"));
    }
    all.shutdown()
        .await
        .context("stopping the --mode all server")?;
    maintain
        .shutdown()
        .await
        .context("stopping the --mode maintain server")?;

    let stamps = build_stamps(
        &ctx,
        &config,
        &ledger,
        &sampler_state,
        walk_start_ns,
        walk_end_ns,
        retention_days,
    )?;
    let figures = we::compute_figures(
        &config,
        &tallies,
        ctx.unresolved_hour_keys.load(Ordering::SeqCst),
    );
    let expected = we::expected_figures(&config, stamps.rates);
    let figure_errors = we::check_figures(&figures, &expected);
    let void = we::void_reasons(&stamps, &config.stamps);
    for cell in config.probe_cells().into_iter().chain(config.mcp_cells()) {
        let ran = tallies.get(&cell).map_or(0, |t| t.walks);
        let wanted = u64::from(config.walks_for(&cell));
        if ran != wanted {
            errors.push(format!("cell {} ran {ran} of {wanted} walks", cell.label()));
        }
    }
    let pass = figure_errors.is_empty() && void.is_empty() && errors.is_empty();
    let decision = we::compute_figures(&config, &tallies, 0)
        .into_iter()
        .find(|(name, _)| *name == we::decision_name(config.grid.longest_delta_ms()))
        .map(|(_, share)| {
            if share > 0.10 {
                format!("{share:.4} > 0.10: open the L0-identity digest ticket")
            } else {
                format!("{share:.4} <= 0.10: close the checkbox with this figure")
            }
        });

    let cells: Vec<Value> = tallies
        .iter()
        .map(|(cell, tally)| json!({ "cell": cell, "label": cell.label(), "tally": tally }))
        .collect();
    let figure_rows: Vec<Value> = expected
        .iter()
        .map(|f| {
            let values: Vec<f64> = figures
                .iter()
                .filter(|(n, _)| *n == f.name)
                .map(|(_, v)| *v)
                .collect();
            json!({
                "name": f.name, "values": values, "band": f.band,
                "prediction": f.prediction, "miss_means": f.miss_means,
            })
        })
        .collect();
    let result = json!({
        "version": 1,
        "issue": 3779,
        "profile": profile_name,
        "store": format!("{:?}", args.store),
        "config": config,
        "load_start_ns": load_start_ns,
        "walk_start_ns": walk_start_ns,
        "walk_end_ns": walk_end_ns,
        "stamps": stamps,
        "cells": cells,
        "figures": figure_rows,
        "figure_errors": figure_errors,
        "void_reasons": void,
        "errors": errors,
        "decision": decision,
        "result": if pass { "PASS" } else { "FAIL" },
    });
    std::fs::write(
        args.out_dir.join("result.json"),
        serde_json::to_vec_pretty(&result)?,
    )
    .context("writing result.json")?;
    let summary = summary_text(
        profile_name,
        &expected,
        &figures,
        &figure_errors,
        &void,
        &errors,
        &stamps,
        decision.as_deref(),
        pass,
    );
    std::fs::write(args.out_dir.join("summary.txt"), &summary).context("writing summary.txt")?;
    print!("{summary}");
    Ok(pass)
}

fn server_config(cli: &Cli, mode: Mode, tenant: &TenantId) -> anyhow::Result<ServerConfig> {
    let loopback = "127.0.0.1:0".parse()?;
    let tokens: HashMap<String, TenantId> = match mode {
        Mode::All => [(TOKEN.to_string(), tenant.clone())].into(),
        _ => HashMap::new(),
    };
    let cadence = cli.resolve_flush_cadence()?;
    let concurrency = cli.resolve_flush_concurrency();
    let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
    Ok(ServerConfig {
        strict_flush_policy: cli.strict_flush_policy,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: QueryBudgets {
            mcp: McpConfig {
                enabled: matches!(mode, Mode::All),
                ..McpConfig::default()
            },
            ..QueryBudgets::default()
        },
        max_inflight_flushes: concurrency.max_inflight_flushes,
        max_inflight_flushes_per_tenant: cli.max_inflight_flushes_per_tenant,
        flush_object_backstop_multiple: cli.flush_object_backstop_multiple,
        ingest_hold: Default::default(),
        max_queued_flushes: concurrency.max_queued_flushes,
        adaptive_flush_delay: cli.adaptive_flush_delay,
        max_flush_delay: cadence.max_flush_delay,
        max_flush_delay_idle: cadence.max_flush_delay_idle,
        min_flush_bytes: cadence.min_flush_bytes,
        idle_flush_byte_floor: cli.resolve_idle_flush_byte_floor()?,
        mode,
        listen_http: loopback,
        listen_grpc: loopback,
        shard_count: cli.shards,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants: vec![tenant.hash()],
        fold: FoldTaskConfig {
            enabled: !cli.disable_fold,
            fold_interval: Duration::from_secs(cli.fold_interval_secs),
            build_key_index_leaves: cli.fold_key_index_leaves,
            key_index_build_max_pairs: cli.fold_key_index_build_max_pairs,
        },
        maintain: MaintenanceTaskConfig {
            enabled: matches!(mode, Mode::Maintain),
            interval: Duration::from_secs(cli.maintain_interval_secs),
            shard_count: cli.shards,
            unit_concurrency: cli.maintain_unit_concurrency,
            stalled_after_intervals: cli.maintain_stalled_after_intervals,
            worker_record_keys: Some(
                ravel_maintain::worker_set::MaintainRecordKeys::from_file_keys(&[[0x5C; 32]])
                    .map_err(|e| anyhow::anyhow!("{e}"))?,
            ),
            ..MaintenanceTaskConfig::default()
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
        query_concurrency_limit: cli.parse_query_concurrency_limit()?,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: cli.parse_scrub_period()?,
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
        cpu_gate_permits: CpuGatePermits::derive(cores),
        max_pending_strict_acks: cli.max_pending_strict_acks,
        ingest_buffer_budget_limit: cli.parse_ingest_buffer_budget()?,
        idle_tenant_state_ttl: cli.parse_idle_tenant_state_ttl()?,
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: ravel_server::DEFAULT_DRAIN_SETTLE_INTERVAL,
        ingest_concurrency_limit: cli.parse_ingest_concurrency_limit()?,
    })
}

// --- load ----------------------------------------------------------------

/// Points generated and accepted, one entry per scrape.
#[derive(Default)]
struct LoadLedger {
    /// `(posted_at_ns, generated, accepted)`.
    steps: Vec<(i64, u64, u64)>,
    failed_requests: u64,
}

async fn run_load(
    workload: &'static WorkloadFile,
    profile: &'static Profile,
    load: we::Load,
    url: String,
    base_ns: i64,
    stop: Arc<AtomicBool>,
    ledger: Arc<Mutex<LoadLedger>>,
) -> anyhow::Result<()> {
    let base_ms = base_ns / 1_000_000;
    let mut generator = Generator::for_profile(workload, profile, base_ms);
    let mut tally = GenerationTally::new();
    let client = reqwest::Client::builder().build()?;
    let interval_ms = i64::try_from(load.scrape_interval_ms)?;
    let mut buf = Vec::new();
    let mut step = 0u64;
    while !stop.load(Ordering::SeqCst) {
        sleep_until_ns((base_ms + i64::try_from(step)? * interval_ms) * 1_000_000).await;
        if stop.load(Ordering::SeqCst) {
            break;
        }
        buf.clear();
        generator
            .generate_steps_into(step, step + 1, &mut buf, &mut tally)
            .map_err(|e| anyhow::anyhow!("generating step {step}: {e}"))?;
        let text = std::str::from_utf8(&buf).context("generated stream is not UTF-8")?;
        let mut points = Vec::new();
        for (i, line) in text.lines().enumerate() {
            match classify_line(i as u64 + 1, line) {
                StreamRecord::Scalar(s) | StreamRecord::StaleMarker(s) => {
                    points.push(LogicalPoint::Sample(s))
                }
                StreamRecord::NativeHistogram(h) => points.push(LogicalPoint::Histogram(h)),
                StreamRecord::Malformed { line_no, reason } => {
                    bail!("generated line {line_no} is malformed: {reason:?}")
                }
            }
        }
        let generated = points.len() as u64;
        let mut accepted = 0u64;
        let mut failed = 0u64;
        for chunk in points.chunks(load.series_per_body) {
            let body = encode_rw1_points(chunk).map_err(|e| anyhow::anyhow!("{e}"))?;
            let sent = client
                .post(&url)
                .header("content-type", "application/x-protobuf")
                .header("content-encoding", "snappy")
                .header("x-prometheus-remote-write-version", "0.1.0")
                .header("authorization", format!("Bearer {TOKEN}"))
                .body(body)
                .send()
                .await;
            match sent {
                Ok(response) if response.status().is_success() => accepted += chunk.len() as u64,
                Ok(response) => {
                    failed += 1;
                    let code = response.status();
                    let text = response.text().await.unwrap_or_default();
                    println!(
                        "load: step {step}: HTTP {code}: {}",
                        text.chars().take(200).collect::<String>()
                    );
                }
                Err(err) => {
                    failed += 1;
                    println!("load: step {step}: {err}");
                }
            }
        }
        if let Ok(mut ledger) = ledger.lock() {
            ledger.steps.push((wall_now_ns(), generated, accepted));
            ledger.failed_requests += failed;
        }
        step += 1;
    }
    Ok(())
}

// --- sampler -------------------------------------------------------------

#[derive(Default)]
struct SamplerState {
    first_l1_seen_ns: BTreeMap<u32, i64>,
    sealed_at_ns: BTreeMap<u32, i64>,
    listings: Vec<CommitListing>,
}

/// Every `sample_interval_ms`: the fold watermark (seal times) and one
/// resolve over everything loaded (first L1 part per hour). Every
/// `listing_every_s`: a commit listing for c and L.
async fn run_sampler(
    ctx: Arc<Ctx>,
    load_start_ns: i64,
    stop: Arc<AtomicBool>,
    state: Arc<Mutex<SamplerState>>,
) -> anyhow::Result<()> {
    let rules = &ctx.config.stamps;
    let first_hour = u32::try_from(load_start_ns / NS_PER_HOUR)?;
    let listing_every_ns = i64::try_from(rules.listing_every_s)? * NS_PER_S;
    let listing_window_ns = i64::try_from(rules.listing_window_s)? * NS_PER_S;
    let round_ns = i64::try_from(rules.commit_round_ms)? * 1_000_000;
    let mut next_listing_ns = load_start_ns + listing_window_ns;
    let mut tick = tokio::time::interval(Duration::from_millis(rules.sample_interval_ms));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    while !stop.load(Ordering::SeqCst) {
        tick.tick().await;
        let now_ns = wall_now_ns();
        let watermark = ctx
            .catalog
            .head_watermark_hour(&ctx.tenant_hash, Signal::Metrics)
            .await
            .context("reading the fold watermark")?;
        let snapshot = ctx
            .catalog
            .resolve(
                &ctx.tenant_hash,
                Signal::Metrics,
                TimeRange {
                    start_ns: load_start_ns - NS_PER_HOUR,
                    end_ns: now_ns + NS_PER_HOUR,
                },
                &[],
                now_ns,
            )
            .await
            .context("the sampler's resolve")?;
        let Ok(mut state) = state.lock() else {
            bail!("sampler state poisoned");
        };
        if let Some(watermark) = watermark {
            for hour in first_hour..=watermark {
                state.sealed_at_ns.entry(hour).or_insert(now_ns);
            }
        }
        for segment in &snapshot.segments {
            if let KeyKind::L1 { hour } = we::key_kind(&segment.data_object_key) {
                state.first_l1_seen_ns.entry(hour).or_insert(now_ns);
            }
        }
        if now_ns >= next_listing_ns {
            let commits: Vec<L0Commit> = snapshot
                .segments
                .iter()
                .filter(|s| we::key_kind(&s.data_object_key) == KeyKind::L0)
                .map(|s| L0Commit {
                    created_ns: s.created_unix_ns,
                    min_event_ts_ns: s.min_event_ts_ns,
                })
                .collect();
            state.listings.push(we::commit_listing(
                &commits,
                now_ns,
                listing_window_ns,
                round_ns,
            ));
            next_listing_ns = now_ns + listing_every_ns;
        }
    }
    Ok(())
}

// --- walks ---------------------------------------------------------------

type Tallies = BTreeMap<Cell, CellTally>;

/// Run the three phases. Returns the tallies, the walk phase's start and end,
/// and any errors that are not figures.
async fn run_phases(ctx: Arc<Ctx>, load_start_ns: i64) -> (Tallies, i64, i64, Vec<String>) {
    let config = &ctx.config;
    let deadline_ns = load_start_ns + to_ns(config.schedule.max_run_s);
    let semaphore = Arc::new(tokio::sync::Semaphore::new(
        config.grid.max_concurrent_walks,
    ));
    let tallies = Arc::new(Mutex::new(Tallies::new()));
    let mut errors = Vec::new();
    let mut walk_start_ns = None;
    for (after_s, roles) in config.schedule.phases() {
        sleep_until_ns(load_start_ns + to_ns(after_s)).await;
        let started_ns = wall_now_ns();
        walk_start_ns.get_or_insert(started_ns);
        let cells: Vec<Cell> = config
            .probe_cells()
            .into_iter()
            .chain(config.mcp_cells())
            .filter(|cell| roles.contains(&cell.window))
            .collect();
        let rounds = cells.iter().map(|c| config.walks_for(c)).max().unwrap_or(0);
        println!(
            "phase {:?}: {} cells from {:.0} s after load start",
            roles.iter().map(|r| r.name()).collect::<Vec<_>>(),
            cells.len(),
            (started_ns - load_start_ns) as f64 / 1e9
        );
        let mut handles = Vec::new();
        for round in 0..rounds {
            for cell in cells.iter().filter(|c| round < config.walks_for(c)) {
                let Ok(permit) = Arc::clone(&semaphore).acquire_owned().await else {
                    errors.push("walk pool closed".to_string());
                    break;
                };
                if wall_now_ns() >= deadline_ns {
                    drop(permit);
                    continue;
                }
                let ctx = Arc::clone(&ctx);
                let tallies = Arc::clone(&tallies);
                let cell = *cell;
                handles.push(tokio::spawn(async move {
                    let end = match cell.transport {
                        Transport::Probe => probe_walk(&ctx, cell).await,
                        Transport::Mcp => mcp_walk(&ctx, cell).await,
                    };
                    match &end {
                        WalkEnd::Failed { page, error } => {
                            println!("{} failed at page {page}: {error}", cell.label());
                        }
                        WalkEnd::Short { pages } => {
                            println!("{} ran out of rows after {pages} pages", cell.label());
                        }
                        _ => {}
                    }
                    if let Ok(mut tallies) = tallies.lock() {
                        tallies.entry(cell).or_default().record(&end);
                    }
                    drop(permit);
                }));
            }
        }
        for handle in handles {
            if let Err(err) = handle.await {
                errors.push(format!("walk task: {err}"));
            }
        }
    }
    let walk_end_ns = wall_now_ns();
    if walk_end_ns > deadline_ns {
        errors.push("the walk phase ran past max_run_s".to_string());
    }
    let tallies = tallies.lock().map(|t| t.clone()).unwrap_or_default();
    (
        tallies,
        walk_start_ns.unwrap_or(walk_end_ns),
        walk_end_ns,
        errors,
    )
}

fn to_ns(seconds: u64) -> i64 {
    i64::try_from(seconds)
        .unwrap_or(i64::MAX / NS_PER_S)
        .saturating_mul(NS_PER_S)
}

fn window_for(ctx: &Ctx, cell: Cell, t0_ns: i64) -> TimeRange {
    let (start_ns, end_ns) = ctx.config.windows.range_ns(cell.window, t0_ns);
    TimeRange { start_ns, end_ns }
}

async fn probe_walk(ctx: &Ctx, cell: Cell) -> WalkEnd {
    match probe_walk_inner(ctx, cell).await {
        Ok(end) => end,
        Err((page, err)) => WalkEnd::Failed {
            page,
            error: format!("{err:#}"),
        },
    }
}

async fn probe_walk_inner(ctx: &Ctx, cell: Cell) -> Result<WalkEnd, (u32, anyhow::Error)> {
    let pages = ctx.config.grid.pages_per_walk;
    let sql = cell.statement.sql(&ctx.config.load.s2_family);
    let Some(budgets) = ctx.budgets.get(&cell.statement) else {
        return Err((1, anyhow::anyhow!("no budgets for {:?}", cell.statement)));
    };
    let t0_ns = wall_now_ns();
    let window = window_for(ctx, cell, t0_ns);
    let mut walk = PinnedWalk::start(WalkSpec {
        service: &ctx.service,
        catalog: &ctx.catalog,
        tenant_hash: ctx.tenant_hash,
        budgets,
        sql,
        window,
        t0_ns,
        pages,
    })
    .await?;
    let watermark = if cell.window == WindowRole::Wo {
        ctx.catalog
            .head_watermark_hour(&ctx.tenant_hash, Signal::Metrics)
            .await
            .map_err(|e| (1, e.into()))?
    } else {
        None
    };
    loop {
        let now_ns = if walk.pages_run() == 0 {
            t0_ns
        } else {
            tokio::time::sleep(Duration::from_millis(cell.delta_ms)).await;
            wall_now_ns()
        };
        let report = walk.step(now_ns).await?;
        ctx.pages.fetch_add(1, Ordering::SeqCst);
        if report.pending_erasure {
            ctx.pages_with_pending_erasure
                .fetch_add(1, Ordering::SeqCst);
        }
        ctx.unresolved_hour_keys
            .fetch_add(report.unresolved_hours, Ordering::SeqCst);
        if report.page == 1 {
            let read_hours: BTreeSet<u32> = walk
                .k0()
                .iter()
                .filter_map(|key| match we::key_kind(key) {
                    KeyKind::L1 { hour } => Some(hour),
                    _ => walk.hour_of(key),
                })
                .collect();
            if matches!(cell.window, WindowRole::Wr | WindowRole::Wo)
                && let Ok(mut set) = ctx.wr_wo_hours.lock()
            {
                set.extend(read_hours.iter().copied());
            }
            if cell.window == WindowRole::Wo {
                let newest = read_hours.iter().next_back().copied();
                let sealed = matches!((newest, watermark), (Some(h), Some(w)) if h <= w);
                if !sealed {
                    ctx.wo_unsealed.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
        if let Some(end) = report.end {
            return Ok(end);
        }
    }
}

async fn mcp_walk(ctx: &Ctx, cell: Cell) -> WalkEnd {
    match mcp_walk_inner(ctx, cell).await {
        Ok(end) => end,
        Err((page, err)) => WalkEnd::Failed {
            page,
            error: format!("{err:#}"),
        },
    }
}

/// The same walk through `POST /mcp`. The adapter mints its own t0 from its
/// own clock; the window here is fixed at this side's t0, which the request
/// carries.
async fn mcp_walk_inner(ctx: &Ctx, cell: Cell) -> Result<WalkEnd, (u32, anyhow::Error)> {
    let pages = ctx.config.grid.pages_per_walk;
    let t0_ns = wall_now_ns();
    let window = window_for(ctx, cell, t0_ns);
    let mut arguments = json!({
        "query": cell.statement.sql(&ctx.config.load.s2_family),
        "time_range": {
            "start_ns": window.start_ns.to_string(),
            "end_ns": window.end_ns.to_string(),
        },
        "max_rows": ctx.config.load.max_rows(cell.statement),
    });
    for page in 1..=pages {
        if page > 1 {
            tokio::time::sleep(Duration::from_millis(cell.delta_ms)).await;
        }
        let envelope = call_query_sql(ctx, &arguments)
            .await
            .map_err(|e| (page, e))?;
        let status = envelope["status"].as_str().unwrap_or("");
        if status == "error" {
            let class = envelope["failure"]["class"].as_str().unwrap_or("");
            let message = envelope["failure"]["message"].as_str().unwrap_or("");
            if page > 1 && class == "cursor_expired" && message.contains(MOVED_MESSAGE) {
                return Ok(WalkEnd::Expired {
                    page,
                    causes: Causes::default(),
                });
            }
            return Err((page, anyhow::anyhow!("{class}: {message}")));
        }
        ctx.pages.fetch_add(1, Ordering::SeqCst);
        if page < pages {
            let Some(cursor) = envelope["presentation"]["cursor"].as_str() else {
                return Ok(WalkEnd::Short { pages: page });
            };
            arguments["cursor"] = Value::String(cursor.to_string());
        }
    }
    Ok(WalkEnd::Finished)
}

/// One sessionless current-revision `tools/call` of `ravel_query_sql`,
/// returning the D4 envelope.
async fn call_query_sql(ctx: &Ctx, arguments: &Value) -> anyhow::Result<Value> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "ravel_query_sql",
            "arguments": arguments,
            "_meta": {
                "io.modelcontextprotocol/protocolVersion": MCP_PROTOCOL,
                "io.modelcontextprotocol/clientCapabilities": {}
            }
        }
    });
    let response = ctx
        .http
        .post(&ctx.mcp_url)
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("mcp-protocol-version", MCP_PROTOCOL)
        .header("mcp-method", "tools/call")
        .header("mcp-name", "ravel_query_sql")
        .body(body.to_string())
        .send()
        .await?;
    let code = response.status();
    let text = response.text().await?;
    if !code.is_success() {
        bail!(
            "POST /mcp: HTTP {code}: {}",
            text.chars().take(300).collect::<String>()
        );
    }
    let message = jsonrpc_message(&text)?;
    if let Some(error) = message.get("error") {
        bail!("POST /mcp: JSON-RPC error {error}");
    }
    message
        .get("result")
        .and_then(|r| r.get("structuredContent"))
        .cloned()
        .context("POST /mcp: the result carries no structuredContent")
}

/// The JSON-RPC response in a body that is either plain JSON or an SSE
/// stream whose `data:` lines carry it.
fn jsonrpc_message(text: &str) -> anyhow::Result<Value> {
    let trimmed = text.trim_start();
    if trimmed.starts_with('{') {
        return Ok(serde_json::from_str(trimmed)?);
    }
    for line in text.lines() {
        if let Some(data) = line.strip_prefix("data:") {
            let Ok(value) = serde_json::from_str::<Value>(data.trim()) else {
                continue;
            };
            if value.get("result").is_some() || value.get("error").is_some() {
                return Ok(value);
            }
        }
    }
    bail!("POST /mcp: no JSON-RPC response in the body")
}

// --- stamps and report ---------------------------------------------------

fn build_stamps(
    ctx: &Ctx,
    config: &ProfileConfig,
    ledger: &Mutex<LoadLedger>,
    sampler: &Mutex<SamplerState>,
    walk_start_ns: i64,
    walk_end_ns: i64,
    retention_days: Option<u64>,
) -> anyhow::Result<Stamps> {
    let Ok(ledger) = ledger.lock() else {
        bail!("load ledger poisoned");
    };
    let Ok(sampler) = sampler.lock() else {
        bail!("sampler state poisoned");
    };
    let window_ns = to_ns(config.stamps.rate_window_s);
    let in_walks: Vec<&(i64, u64, u64)> = ledger
        .steps
        .iter()
        .filter(|(at, _, _)| *at >= walk_start_ns && *at < walk_end_ns)
        .collect();
    let span_ns = (walk_end_ns - walk_start_ns).max(1);
    let generated: u64 = in_walks.iter().map(|(_, g, _)| *g).sum();
    let nominal = generated as f64 / (span_ns as f64 / 1e9);
    let mut windows = Vec::new();
    let mut start = walk_start_ns;
    while start + window_ns <= walk_end_ns {
        let accepted: u64 = in_walks
            .iter()
            .filter(|(at, _, _)| *at >= start && *at < start + window_ns)
            .map(|(_, _, a)| *a)
            .sum();
        windows.push(accepted as f64 / (window_ns as f64 / 1e9));
        start += window_ns;
    }
    let intervals: Vec<f64> = sampler
        .listings
        .iter()
        .flat_map(|l| l.intervals_s.iter().copied())
        .collect();
    let lag = sampler
        .listings
        .iter()
        .filter_map(|l| l.max_lag_s)
        .fold(None, |acc: Option<f64>, v| {
            Some(acc.map_or(v, |a| a.max(v)))
        });
    let since_hour_end =
        |hour: u32, at_ns: i64| (at_ns - (i64::from(hour) + 1) * NS_PER_HOUR) as f64 / 1e9;
    Ok(Stamps {
        ingest_rate_nominal_per_s: nominal,
        ingest_rate_windows_per_s: windows,
        ingest_failed_requests: ledger.failed_requests,
        commit_listings: sampler.listings.clone(),
        rates: Rates {
            commit_interval_s: we::median(&intervals),
            commit_lag_s: lag,
        },
        hour_end_to_first_l1_s: sampler
            .first_l1_seen_ns
            .iter()
            .map(|(h, at)| (*h, since_hour_end(*h, *at)))
            .collect(),
        fold_seal_after_hour_end_s: sampler
            .sealed_at_ns
            .iter()
            .map(|(h, at)| (*h, since_hour_end(*h, *at)))
            .collect(),
        wr_wo_hours: ctx
            .wr_wo_hours
            .lock()
            .map(|s| s.clone())
            .unwrap_or_default(),
        wo_unsealed_walks: ctx.wo_unsealed.load(Ordering::SeqCst),
        pages: ctx.pages.load(Ordering::SeqCst),
        pages_with_pending_erasure: ctx.pages_with_pending_erasure.load(Ordering::SeqCst),
        retention_days,
        binary_path: std::env::current_exe()
            .map(|p| p.display().to_string())
            .unwrap_or_default(),
    })
}

#[allow(clippy::too_many_arguments)]
fn summary_text(
    profile: &str,
    expected: &[we::ExpectedFigure],
    figures: &[(String, f64)],
    figure_errors: &[we::FigureError],
    void: &[String],
    errors: &[String],
    stamps: &Stamps,
    decision: Option<&str>,
    pass: bool,
) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(out, "mcp_walk_expiry profile {profile}");
    let _ = writeln!(
        out,
        "stamps: c {:?} s, L {:?} s, ingest {:.1}/s nominal over {} windows, {} pages",
        stamps.rates.commit_interval_s,
        stamps.rates.commit_lag_s,
        stamps.ingest_rate_nominal_per_s,
        stamps.ingest_rate_windows_per_s.len(),
        stamps.pages
    );
    for figure in expected {
        let values: Vec<String> = figures
            .iter()
            .filter(|(n, _)| *n == figure.name)
            .map(|(_, v)| format!("{v:.4}"))
            .collect();
        let band = figure.band.map_or("unresolved".to_string(), |(lo, hi)| {
            format!("[{lo:.4}, {hi:.4}]")
        });
        let _ = writeln!(
            out,
            "{} = {} band {band} ({})",
            figure.name,
            if values.is_empty() {
                "MISSING".to_string()
            } else {
                values.join(", ")
            },
            figure.prediction
        );
    }
    for error in figure_errors {
        let _ = writeln!(out, "FIGURE: {error}");
    }
    for reason in void {
        let _ = writeln!(out, "VOID: {reason}");
    }
    for error in errors {
        let _ = writeln!(out, "ERROR: {error}");
    }
    if let Some(decision) = decision {
        let _ = writeln!(out, "decision: {decision}");
    }
    let _ = writeln!(
        out,
        "RESULT: {}",
        if pass {
            "PASS every figure present once and in band, no stamp void"
        } else {
            "FAIL"
        }
    );
    out
}
