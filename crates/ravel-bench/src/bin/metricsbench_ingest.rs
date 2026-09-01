//! MetricsBench Remote Write 1.0 ingest lane (ADR-0927, issue #937, task M5).
//!
//! Replays one logical sample stream into the in-process Ravel path
//! (`WriteMode::Strict`, durable-on-ack, commit tokens) and, behind endpoints
//! supplied on the command line with no hardcoded hosts, the
//! Prometheus/VictoriaMetrics/object-storage-native comparators over portable
//! Remote Write 1.0. It prints one JSON report whose every row states that
//! system's acknowledgement meaning (ADR-0927 decision 3), and it fails the run
//! if the sample accounting does not close (band 4) rather than reporting a
//! silent drop.
//!
//! The stream is the deterministic MetricsBench workload
//! (`metricsbench_gen`'s generator), parsed back through the shipping encoder
//! so this lane grows no second workload generator. `--profile` has no default:
//! the profile selects which data the run touches (CLAUDE.md measurement
//! discipline).
//!
//! Reachable as `cargo run -p ravel-bench --bin metricsbench_ingest -- \
//! --profile ci --store memory --steps 10`.
#![allow(clippy::expect_used)]

use std::path::PathBuf;

use clap::Parser;
use ravel_bench::harness::{StoreKind, store_and_metrics_from_env};
use ravel_bench::metrics_ingest::{
    HttpReplayConfig, LogicalSample, MetricsIngestReport, RavelReplayConfig, parse_logical_stream,
    replay_into_ravel, replay_over_http,
};
use ravel_bench::metrics_gen::Generator;
use ravel_bench::metrics_workload::{WorkloadFile, load_workload};
use ravel_ingest::{Clock, SystemClock};
use ravel_types::TenantId;

/// Where the checked-in workload manifest lives, relative to this crate's
/// manifest dir. The same default `metricsbench_gen` uses.
const DEFAULT_WORKLOAD: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../benchmarks/metrics/workload.json"
);

#[derive(Parser, Debug)]
#[command(
    about = "Replay the MetricsBench sample stream over Remote Write 1.0 into Ravel and \
             config-supplied comparators (ADR-0927 M5)"
)]
struct Args {
    /// The workload manifest to generate the stream from.
    #[arg(long, default_value = DEFAULT_WORKLOAD)]
    workload: PathBuf,
    /// Which profile to generate. No default: the choice selects which data the
    /// run touches.
    #[arg(long)]
    profile: String,
    /// Scrapes to generate. Defaults to the profile's full
    /// `samples_per_series`.
    #[arg(long)]
    steps: Option<u64>,
    /// The object store backing the in-process Ravel path.
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    /// Ingest shard count.
    #[arg(long, default_value_t = 4)]
    shards: u32,
    /// Logical samples per write batch.
    #[arg(long, default_value_t = 500)]
    batch_size: usize,
    /// Per-batch strict-ack deadline for the Ravel path.
    #[arg(long, default_value_t = 30)]
    ack_timeout_secs: u64,
    /// Tenant to write under.
    #[arg(long, default_value = "metricsbench")]
    tenant: String,
    /// Prometheus `/api/v1/write` endpoint. Omitted, Prometheus is not
    /// replayed. No hardcoded host: this is the only way to reach it.
    #[arg(long)]
    prometheus_endpoint: Option<String>,
    /// VictoriaMetrics `/api/v1/write` endpoint. Omitted, it is not replayed.
    #[arg(long)]
    victoriametrics_endpoint: Option<String>,
    /// Object-storage-native comparator `/api/v1/write` endpoint. Omitted, it
    /// is not replayed.
    #[arg(long)]
    osn_endpoint: Option<String>,
    /// Logical samples per RW1.0 request to a comparator.
    #[arg(long, default_value_t = 500)]
    http_batch_size: usize,
    /// Retries on a 429/5xx before a comparator batch is dropped.
    #[arg(long, default_value_t = 3)]
    http_max_retries: u32,
    /// Per-request timeout for a comparator POST, bounding the complete
    /// operation including the body read (ADR-0927 decision 6).
    #[arg(long, default_value_t = 30)]
    http_timeout_secs: u64,
}

/// A band violation: the run measured something outside what was pre-registered
/// (ADR-0927 band 4/5), separate from an artifact or replay error.
#[derive(Debug, thiserror::Error)]
enum RunError {
    #[error("loading the workload manifest failed: {0}")]
    Workload(String),
    #[error("generating the stream failed: {0}")]
    Generate(String),
    #[error("replaying into Ravel failed: {0}")]
    Replay(String),
    #[error("replaying into {system} over Remote Write failed: {source}")]
    Http {
        system: String,
        source: ravel_bench::metrics_ingest::LaneError,
    },
    #[error(
        "the Ravel sample accounting does not close: {offered} offered != {accepted} accepted + \
         {rejected} rejected + {dropped} dropped. A silent drop fails the run (ADR-0927 band 4)."
    )]
    AccountingMismatch {
        offered: u64,
        accepted: u64,
        rejected: u64,
        dropped: u64,
    },
    #[error(
        "the Ravel replay accepted zero samples, so it measured nothing; a stream that ingests \
         nothing is a failed run, not an empty result"
    )]
    NothingIngested,
}

/// Steps to generate: the flag if set, else the profile's full
/// `samples_per_series`.
fn resolve_steps(args: &Args, workload: &WorkloadFile) -> Result<u64, RunError> {
    if let Some(steps) = args.steps {
        return Ok(steps);
    }
    workload
        .profile(&args.profile)
        .map(|p| p.samples_per_series)
        .ok_or_else(|| {
            RunError::Generate(format!(
                "manifest declares no profile `{}`",
                args.profile
            ))
        })
}

async fn run(args: &Args) -> Result<MetricsIngestReport, RunError> {
    let workload = load_workload(&args.workload).map_err(|e| RunError::Workload(e.to_string()))?;
    let steps = resolve_steps(args, &workload)?;

    // Anchor the newest generated sample near now, so a strict write lands
    // inside a plausible ingest window rather than in 1970.
    let scrape_ms = workload
        .profile(&args.profile)
        .map(|p| p.scrape_interval_secs as i64 * 1_000)
        .unwrap_or(15_000);
    let now_ms = SystemClock.now_ns() / 1_000_000;
    let base_ts_ms = now_ms - steps as i64 * scrape_ms;

    let (bytes, _report) = Generator::new(&workload, &args.profile, base_ts_ms)
        .map_err(|e| RunError::Generate(e.to_string()))?
        .generate_bytes(steps)
        .map_err(|e| RunError::Generate(e.to_string()))?;
    let text = String::from_utf8(bytes).map_err(|e| RunError::Generate(e.to_string()))?;
    let stream: Vec<LogicalSample> = parse_logical_stream(&text);

    // The in-process Ravel path: strict, durable-on-ack, commit tokens.
    let (store, store_metrics) = store_and_metrics_from_env(args.store);
    let backend_bills_requests = matches!(args.store, StoreKind::S3);
    let ravel_cfg = RavelReplayConfig {
        store,
        store_metrics,
        backend_bills_requests,
        shards: args.shards,
        batch_size: args.batch_size,
        ack_timeout_secs: args.ack_timeout_secs,
        tenant: TenantId::new(args.tenant.clone()),
    };
    let ravel = replay_into_ravel(&ravel_cfg, &stream)
        .await
        .map_err(|e| RunError::Replay(e.to_string()))?;

    // Band 4: the Ravel accounting must close.
    let ing = &ravel.result.ingest;
    if ing.accepted_samples + ing.rejected_samples + ing.dropped_samples != ing.offered_samples {
        return Err(RunError::AccountingMismatch {
            offered: ing.offered_samples,
            accepted: ing.accepted_samples,
            rejected: ing.rejected_samples,
            dropped: ing.dropped_samples,
        });
    }
    if ing.accepted_samples == 0 {
        return Err(RunError::NothingIngested);
    }

    let mut rows = vec![ravel.result];

    // Comparators, only those a caller supplied an endpoint for.
    for (system, endpoint) in [
        ("prometheus", args.prometheus_endpoint.as_ref()),
        ("victoriametrics", args.victoriametrics_endpoint.as_ref()),
        ("osn", args.osn_endpoint.as_ref()),
    ] {
        let Some(endpoint) = endpoint else { continue };
        let http_cfg = HttpReplayConfig {
            system: system.to_string(),
            endpoint: endpoint.clone(),
            batch_size: args.http_batch_size,
            max_retries: args.http_max_retries,
            timeout_secs: args.http_timeout_secs,
        };
        let row = replay_over_http(&http_cfg, &stream)
            .await
            .map_err(|source| RunError::Http {
                system: system.to_string(),
                source,
            })?;
        rows.push(row);
    }

    Ok(MetricsIngestReport::new(rows))
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    match run(&args).await {
        Ok(report) => {
            let json = serde_json::to_string_pretty(&report).expect("serialize report");
            println!("{json}");
            for row in &report.systems {
                eprintln!(
                    "{:>16}  ack={:<14}  accepted={} rejected={} dropped={} p99={:.3}ms",
                    row.system,
                    row.ack_semantics.label(),
                    row.ingest.accepted_samples,
                    row.ingest.rejected_samples,
                    row.ingest.dropped_samples,
                    row.ingest.ack_latency_ms.p99,
                );
            }
        }
        Err(err) => {
            eprintln!("metricsbench_ingest: {err}");
            std::process::exit(1);
        }
    }
}
