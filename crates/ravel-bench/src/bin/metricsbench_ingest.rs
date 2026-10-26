//! MetricsBench Remote Write 1.0 ingest lane (ADR-0927, issue #937, task M5).
//!
//! Replays one logical stream (scalar samples, staleness markers and native
//! histograms) into the in-process Ravel path (`WriteMode::Strict`,
//! durable-on-ack, commit tokens) and, behind endpoints supplied on the
//! command line with no hardcoded hosts, over portable Remote Write 1.0 into
//! `ravel-server` (`--ravel-endpoint`, ADR-0927 2026-10-26 amendment A),
//! Prometheus, VictoriaMetrics and Mimir. It prints one JSON report whose
//! every row states that system's acknowledgement meaning (ADR-0927 decision
//! 3), and it fails the run if the accounting does not close (band 4) rather
//! than reporting a silent drop.
//!
//! The stream is the deterministic MetricsBench workload
//! (`metricsbench_gen`'s generator), streamed one scrape step at a time
//! through the shipping encoder and classified line by line, so this lane
//! grows no second workload generator and holds one batch rather than the
//! whole history. Each comparator replays its own regeneration of the same
//! stream from the same anchor, and the run fails unless every regeneration's
//! digest matches the Ravel replay's. `--profile` has no default: the profile
//! selects which data the run touches (CLAUDE.md measurement discipline).
//! `--anchor-ms` has none either (ADR-0927, 2026-10-26 amendment, section C):
//! the stream is a pure function of the manifest, the profile, the step count
//! and the anchor, so a clock-derived anchor would make every run a different
//! stream. A missing or invalid anchor exits 64.
//!
//! Reachable as `cargo run -p ravel-bench --bin metricsbench_ingest -- \
//! --profile ci --store memory --steps 10 --anchor-ms 1700000010000`.
#![allow(clippy::expect_used)]

use std::path::PathBuf;

use clap::Parser;
use ravel_bench::harness::{
    StoreKind, backend_bills_requests, endpoint_host_from_env, store_and_metrics_from_env,
};
use ravel_bench::metrics_gen::{GenerationReport, Generator};
use ravel_bench::metrics_ingest::{
    AckSemantics, GeneratedStream, HttpReplayConfig, MetricsIngestReport, RavelReplayConfig,
    RowAccountingViolation, RunProvenance, StreamBandViolation, StreamCounts, Substrate,
    build_profile_record, check_anchor, check_row_accounting, check_stream_band,
    query_after_replay, replay_into_ravel, replay_over_http, workload_digest,
};
use ravel_bench::metrics_workload::{WorkloadFile, load_workload};
use ravel_types::TenantId;

/// The exit code for a usage error (BSD `EX_USAGE`): a missing or invalid run
/// parameter, refused before anything is generated.
const EXIT_USAGE: i32 = 64;

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
    /// Timestamp of the first generated step, in milliseconds since the
    /// epoch. Required, with no default: the stream is a function of its run
    /// parameters, and two runs with the same anchor replay the same bytes.
    /// Must be non-negative, a multiple of the profile's scrape interval in
    /// milliseconds, and put the last step at or before 9223372036854 ms.
    /// Missing or invalid, the run exits 64.
    #[arg(long, allow_negative_numbers = true)]
    anchor_ms: Option<i64>,
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
    /// `ravel-server` `/api/v1/write` endpoint, the portable Ravel row
    /// (`system: ravel`, `endpoint: <url>`). Omitted, only the in-process
    /// row is written.
    #[arg(long)]
    ravel_endpoint: Option<String>,
    /// Name of the environment variable holding the bearer token for
    /// `--ravel-endpoint`. The token is read from the environment, never
    /// from argv, and never written into the report. Omitted, no
    /// `Authorization` header is sent.
    #[arg(long)]
    ravel_bearer_token_env: Option<String>,
    /// Prometheus `/api/v1/write` endpoint. Omitted, Prometheus is not
    /// replayed. No hardcoded host: this is the only way to reach it.
    #[arg(long)]
    prometheus_endpoint: Option<String>,
    /// VictoriaMetrics `/api/v1/write` endpoint. Omitted, it is not replayed.
    #[arg(long)]
    victoriametrics_endpoint: Option<String>,
    /// Mimir push endpoint (Remote Write 1.0). Omitted, it is not replayed.
    #[arg(long)]
    mimir_endpoint: Option<String>,
    /// Logical points per RW1.0 request to an HTTP receiver.
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
    #[error(
        "--anchor-ms is required: the stream digest covers every sample's timestamp, so an \
         anchor read from the clock would make each run replay a different stream, and two \
         runs could never be shown to have replayed the same one. Pass the first step's \
         timestamp in milliseconds since the epoch, a multiple of the profile's scrape \
         interval ({interval_ms} ms for profile `{profile}`)"
    )]
    MissingAnchor { profile: String, interval_ms: u64 },
    #[error(
        "--ravel-bearer-token-env names `{var}`, which is unset, empty or not UTF-8; export the \
         token under that name (the token is never taken from argv)"
    )]
    MissingBearerToken { var: String },
    #[error(
        "--ravel-bearer-token-env needs --ravel-endpoint: there is no portable Ravel row to authenticate"
    )]
    BearerWithoutEndpoint,
    #[error(transparent)]
    Anchor(#[from] ravel_bench::metrics_ingest::AnchorError),
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
    #[error("{system}: {source}")]
    StreamBand {
        system: String,
        source: StreamBandViolation,
    },
    #[error(
        "{system} replayed a regeneration whose digest {got} differs from the Ravel replay's \
         {expected}, so the rows did not see the same stream"
    )]
    StreamDiverged {
        system: String,
        expected: String,
        got: String,
    },
    #[error("{system}: {source}")]
    AccountingMismatch {
        system: String,
        source: Box<RowAccountingViolation>,
    },
    #[error(
        "the Ravel replay accepted zero samples, so it measured nothing; a stream that ingests \
         nothing is a failed run, not an empty result"
    )]
    NothingIngested,
    #[error("the post-ingest read-your-write query phase failed: {0}")]
    Query(String),
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
            RunError::Generate(format!("manifest declares no profile `{}`", args.profile))
        })
}

/// The first step's timestamp, from the required flag, checked against the
/// profile's grid and the nanosecond range. No clock is read.
fn resolve_anchor(args: &Args, scrape_interval_secs: u64, steps: u64) -> Result<i64, RunError> {
    let Some(anchor_ms) = args.anchor_ms else {
        return Err(RunError::MissingAnchor {
            profile: args.profile.clone(),
            interval_ms: scrape_interval_secs.saturating_mul(1_000),
        });
    };
    check_anchor(anchor_ms, scrape_interval_secs, steps)?;
    Ok(anchor_ms)
}

/// The portable Ravel row's bearer token, read from the environment
/// variable `--ravel-bearer-token-env` names. Refused before anything is
/// generated when the variable is unset or empty, or when the flag is given
/// without `--ravel-endpoint`.
fn resolve_bearer_token(args: &Args) -> Result<Option<String>, RunError> {
    let Some(var) = &args.ravel_bearer_token_env else {
        return Ok(None);
    };
    if args.ravel_endpoint.is_none() {
        return Err(RunError::BearerWithoutEndpoint);
    }
    match std::env::var(var) {
        Ok(token) if !token.is_empty() => Ok(Some(token)),
        _ => Err(RunError::MissingBearerToken { var: var.clone() }),
    }
}

/// Close a drained stream and check one row against it: the classification
/// identity of band 4, then the row's offering identity and the split of its
/// offered samples.
fn close_row(
    system: &str,
    stream: GeneratedStream<'_>,
    ingest: &ravel_bench::metrics_ingest::IngestPhase,
) -> Result<(GenerationReport, StreamCounts), RunError> {
    let (report, counts) = stream
        .finish()
        .map_err(|e| RunError::Generate(e.to_string()))?;
    check_stream_band(&counts, &report).map_err(|source| RunError::StreamBand {
        system: system.to_string(),
        source,
    })?;
    check_row_accounting(ingest, &counts).map_err(|source| RunError::AccountingMismatch {
        system: system.to_string(),
        source: Box::new(source),
    })?;
    Ok((report, counts))
}

async fn run(args: &Args) -> Result<MetricsIngestReport, RunError> {
    let workload = load_workload(&args.workload).map_err(|e| RunError::Workload(e.to_string()))?;
    let steps = resolve_steps(args, &workload)?;
    let profile = workload.profile(&args.profile).ok_or_else(|| {
        RunError::Generate(format!("manifest declares no profile `{}`", args.profile))
    })?;
    let anchor_ms = resolve_anchor(args, profile.scrape_interval_secs, steps)?;
    let ravel_bearer_token = resolve_bearer_token(args)?;
    // `load_workload` read the same file; a second read for the digest keeps
    // the loader's signature unchanged.
    let manifest_bytes = std::fs::read(&args.workload).map_err(|e| {
        RunError::Workload(format!(
            "reading {} for its digest: {e}",
            args.workload.display()
        ))
    })?;
    let provenance = RunProvenance {
        workload_digest: workload_digest(&manifest_bytes),
        anchor_ms,
    };
    let stream_from_anchor = || -> Result<GeneratedStream<'_>, RunError> {
        let generator = Generator::new(&workload, &args.profile, anchor_ms)
            .map_err(|e| RunError::Generate(e.to_string()))?;
        Ok(GeneratedStream::new(generator, steps))
    };

    // The in-process Ravel path: strict, durable-on-ack, commit tokens.
    let (store, store_metrics) = store_and_metrics_from_env(args.store);
    let backend_bills_requests = backend_bills_requests(args.store);
    let substrate = Substrate {
        store_backend: args.store.to_string(),
        endpoint_host: endpoint_host_from_env(args.store),
        backend_bills_requests,
    };
    let ravel_cfg = RavelReplayConfig {
        store,
        store_metrics,
        backend_bills_requests,
        shards: args.shards,
        batch_size: args.batch_size,
        ack_timeout_secs: args.ack_timeout_secs,
        tenant: TenantId::new(args.tenant.clone()),
    };
    let mut ravel_stream = stream_from_anchor()?;
    let ravel = replay_into_ravel(&ravel_cfg, &mut ravel_stream)
        .await
        .map_err(|e| RunError::Replay(e.to_string()))?;

    // Band 4: the stream closes against the generator and the Ravel
    // accounting closes over every line.
    let (gen_report, _) = close_row("ravel (in-process)", ravel_stream, &ravel.result.ingest)?;
    if ravel.result.ingest.accepted_samples == 0 {
        return Err(RunError::NothingIngested);
    }

    // ADR-0927 decision 11: the profile's own pre-registered figures, plus the
    // generator's exact counts for the figures that are not directly declared
    // (total series created, logical input bytes) -- never derived from the
    // offered sample count.
    let profile_record = build_profile_record(
        &workload,
        profile,
        steps,
        gen_report.bytes,
        &gen_report,
        provenance,
    )
    .map_err(|e| RunError::Generate(e.to_string()))?;

    // Post-ingest read-your-write query phase (ADR-0927 decision 3): pass the
    // strict writes' commit tokens as `min_commit_token` and evaluate an instant
    // query at the replay's newest sample, so the read resolves the exact
    // committed segments deterministically. Recorded as its own phase, separate
    // from ingest (decision 9). Runs while `ravel` is still whole, before its
    // result row is moved into `rows`.
    let query_expr = ravel
        .first_metric
        .clone()
        .ok_or(RunError::NothingIngested)?;
    let query = query_after_replay(&ravel, &query_expr)
        .await
        .map_err(|e| RunError::Query(e.to_string()))?;

    let mut rows = vec![ravel.result];

    // Portable rows, only those a caller supplied an endpoint for. Their
    // `system` values are the names the query runner joins on.
    for (system, endpoint, ack_semantics, bearer_token) in [
        (
            "ravel",
            args.ravel_endpoint.as_ref(),
            AckSemantics::DurableOnAck,
            ravel_bearer_token.clone(),
        ),
        (
            "prometheus",
            args.prometheus_endpoint.as_ref(),
            AckSemantics::Buffered,
            None,
        ),
        (
            "victoriametrics",
            args.victoriametrics_endpoint.as_ref(),
            AckSemantics::Buffered,
            None,
        ),
        (
            "mimir",
            args.mimir_endpoint.as_ref(),
            AckSemantics::Buffered,
            None,
        ),
    ] {
        let Some(endpoint) = endpoint else { continue };
        let http_cfg = HttpReplayConfig {
            system: system.to_string(),
            endpoint: endpoint.clone(),
            ack_semantics,
            bearer_token,
            batch_size: args.http_batch_size,
            max_retries: args.http_max_retries,
            timeout_secs: args.http_timeout_secs,
        };
        let mut stream = stream_from_anchor()?;
        let row = replay_over_http(&http_cfg, &mut stream)
            .await
            .map_err(|source| RunError::Http {
                system: system.to_string(),
                source,
            })?;
        let (report, _) = close_row(system, stream, &row.ingest)?;
        if report.digest != gen_report.digest {
            return Err(RunError::StreamDiverged {
                system: system.to_string(),
                expected: gen_report.digest.clone(),
                got: report.digest,
            });
        }
        rows.push(row);
    }

    Ok(MetricsIngestReport::new(rows)
        .with_query(query)
        .with_profile(profile_record)
        .with_substrate(substrate))
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
                    "{:>16}  endpoint={}  ack={:<14}  samples accepted={} rejected={} \
                     dropped={}  histograms accepted={} rejected={} dropped={}  p99={:.3}ms",
                    row.system,
                    row.endpoint,
                    row.ack_semantics.label(),
                    row.ingest.accepted_samples,
                    row.ingest.rejected_samples,
                    row.ingest.dropped_samples,
                    row.ingest.accepted_histograms,
                    row.ingest.rejected_histograms,
                    row.ingest.dropped_histograms,
                    row.ingest.ack_latency_ms.p99,
                );
            }
            for q in &report.queries {
                eprintln!(
                    "{:>16}  query={:<20} matched_series={} tokens={} eval_ts_ms={}",
                    q.system,
                    q.query,
                    q.matched_series,
                    q.min_commit_tokens.len(),
                    q.eval_ts_ms,
                );
            }
        }
        Err(err) => {
            eprintln!("metricsbench_ingest: {err}");
            let code = match err {
                RunError::MissingAnchor { .. }
                | RunError::Anchor(_)
                | RunError::MissingBearerToken { .. }
                | RunError::BearerWithoutEndpoint => EXIT_USAGE,
                _ => 1,
            };
            std::process::exit(code);
        }
    }
}
