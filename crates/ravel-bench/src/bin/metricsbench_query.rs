//! MetricsBench portable query run (ADR-0927, issue #938, epic #2821 W2b).
//!
//! Runs the PromQL corpus over HTTP against every engine one ingest run
//! loaded, with Prometheus as the oracle, and writes a version-3 report
//! ([`ravel_bench::report_schema`]). The binding, the pass structure and the
//! classification live in [`ravel_bench::metrics_query`]; this file reads the
//! inputs, resolves bearer tokens from the environment, and writes the report
//! only after [`ravel_bench::report_schema::validate`] accepts it. With
//! `--diagnostics` it then writes the [`ravel_bench::metrics_query::Diagnostics`]
//! sidecar, which is not part of the report.
//!
//! With `--diagnostic-after-failed-ingest` (passed by
//! `deploy/metricsbench/run.sh` only after an ingest whose read-back failed or
//! that stalled) an ingest report with such a row binds as a diagnostic
//! ([`ravel_bench::metrics_query::bind_diagnostic`]): the report carries
//! `run_verdict: "failed"` and `failed_bands`, is checked by
//! [`ravel_bench::report_schema::validate_diagnostic`], and is refused by
//! `validate` everywhere else.
//!
//! Exit codes: 0 the report (and any sidecar) was written; 64 a usage error (an `--engine`
//! value that does not parse); 65 the inputs cannot bind a run or the run is
//! void (a digest mismatch, a band-6 or accounting failure on a portable
//! ingest row, an engine with no URL or no ingest row, a classification
//! error); 66 an input file is unreadable or malformed; 70 the assembled
//! report fails validation, in which case nothing is written; 74 the report
//! or the sidecar could not be written.

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use ravel_bench::metrics_correctness::Engine;
use ravel_bench::metrics_ingest::MetricsIngestReport;
use ravel_bench::metrics_query::{
    BindError, Diagnostics, EngineSpec, RunOptions, bind, bind_diagnostic, endpoints,
    parse_engine_spec, run, system_name,
};
use ravel_bench::metrics_workload::load_workload;
use ravel_bench::promql_corpus::load_corpus;
use ravel_bench::report_schema::{
    ConfigEntry, INSUFFICIENT_PASSES, MIN_VARIANCE_PASSES, MetricsBenchReport, Provenance,
    RunVerdict, SCHEMA_VERSION, VARIANCE_CONFIG_KEY, validate, validate_diagnostic,
};

const EX_USAGE: u8 = 64;
const EX_DATAERR: u8 = 65;
const EX_NOINPUT: u8 = 66;
const EX_SOFTWARE: u8 = 70;
const EX_IOERR: u8 = 74;

#[derive(Parser, Debug)]
#[command(about = "Run the MetricsBench PromQL corpus over HTTP against every engine (ADR-0927)")]
struct Args {
    /// The `metricsbench_ingest` report of the run being queried. It fixes
    /// the anchor, the step count, the workload digest and seed, and each
    /// engine's commit token and query URL.
    #[arg(long)]
    ingest_report: PathBuf,
    /// The workload manifest the ingest run replayed. Its digest must equal
    /// the ingest report's `run.workload_digest`.
    #[arg(long)]
    workload: PathBuf,
    /// The PromQL corpus. Its digest must equal the provenance's
    /// `corpus_digest`.
    #[arg(long)]
    corpus: PathBuf,
    /// The `metricsbench_report provenance` output stamped on the report.
    #[arg(long)]
    provenance: PathBuf,
    /// `name=<prometheus|ravel|victoriametrics|mimir>[,url=<query base>][,bearer_env=VAR]`,
    /// repeatable. Prometheus is required. `url=` is used only when the
    /// engine's ingest row records no read-back query URL.
    #[arg(long = "engine", required = true)]
    engines: Vec<String>,
    /// Untimed passes before the timed ones.
    #[arg(long, default_value_t = 1)]
    warmup_passes: u32,
    /// Timed passes.
    #[arg(long, default_value_t = 5)]
    passes: u32,
    /// Repetitions of each query per pass; the pass figure is their median.
    #[arg(long, default_value_t = 3)]
    reps_per_pass: u32,
    /// The deadline of one query, in milliseconds.
    #[arg(long, default_value_t = 120_000)]
    query_timeout_ms: u64,
    /// How many more times an oracle query that timed out or failed in
    /// transport is re-sent before the run is void. Prometheus' own row keeps
    /// the first attempt's outcome.
    #[arg(long, default_value_t = 2)]
    oracle_retries: u32,
    /// Where the report is written.
    #[arg(long)]
    out: PathBuf,
    /// Where to write the diagnostics sidecar (each non-oracle engine's
    /// largest ULP gap to the oracle per entry, and every oracle retry),
    /// after the report. Not part of the report.
    #[arg(long)]
    diagnostics: Option<PathBuf>,
    /// Query a run whose ingest verification failed, as a diagnostic: a row
    /// whose read-back failed is queried, a stalled row's engine is left out,
    /// and the report says `run_verdict: "failed"` with every failure in
    /// `failed_bands`, so `metricsbench_report validate` refuses it.
    /// Without this flag such an ingest report is refused (exit 65); with it,
    /// an ingest report with no failed row is refused.
    #[arg(long)]
    diagnostic_after_failed_ingest: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run_main(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err((code, message)) => {
            eprintln!("metricsbench_query: {message}");
            ExitCode::from(code)
        }
    }
}

type Failure = (u8, String);

fn read(path: &Path) -> Result<Vec<u8>, Failure> {
    std::fs::read(path).map_err(|e| (EX_NOINPUT, format!("{}: {e}", path.display())))
}

fn blake3_digest(bytes: &[u8]) -> String {
    format!("blake3:{}", blake3::hash(bytes).to_hex())
}

/// Replaces `key`'s entry, or appends one, so every key stays unique.
fn set_config(config: &mut Vec<ConfigEntry>, key: &str, value: String) {
    config.retain(|c| c.key != key);
    config.push(ConfigEntry {
        key: key.to_string(),
        value,
    });
}

async fn run_main(args: Args) -> Result<(), Failure> {
    let specs: Vec<EngineSpec> = args
        .engines
        .iter()
        .map(|s| parse_engine_spec(s))
        .collect::<Result<_, _>>()
        .map_err(|e| (EX_USAGE, e))?;

    let ingest_bytes = read(&args.ingest_report)?;
    let ingest: MetricsIngestReport = serde_json::from_slice(&ingest_bytes).map_err(|e| {
        (
            EX_NOINPUT,
            format!(
                "{}: not an ingest report: {e}",
                args.ingest_report.display()
            ),
        )
    })?;
    let workload_bytes = read(&args.workload)?;
    let workload = load_workload(&args.workload)
        .map_err(|e| (EX_NOINPUT, format!("{}: {e}", args.workload.display())))?;
    let corpus_bytes = read(&args.corpus)?;
    let corpus = load_corpus(&args.corpus)
        .map_err(|e| (EX_NOINPUT, format!("{}: {e}", args.corpus.display())))?;
    let provenance_bytes = read(&args.provenance)?;
    let mut provenance: Provenance = serde_json::from_slice(&provenance_bytes).map_err(|e| {
        (
            EX_NOINPUT,
            format!("{}: not a provenance block: {e}", args.provenance.display()),
        )
    })?;

    let env = |var: &str| std::env::var(var).ok();
    let (binding, failed_bands) = if args.diagnostic_after_failed_ingest {
        bind_diagnostic(&ingest, &workload_bytes, &workload, &specs, env)
    } else {
        bind(&ingest, &workload_bytes, &workload, &specs, env).map(|b| (b, Vec::new()))
    }
    .map_err(|e: BindError| (EX_DATAERR, e.to_string()))?;

    let corpus_digest = blake3_digest(&corpus_bytes);
    if provenance.corpus_digest != corpus_digest {
        return Err((
            EX_DATAERR,
            format!(
                "corpus digest {corpus_digest} does not match the provenance's corpus_digest {}",
                provenance.corpus_digest
            ),
        ));
    }
    let generator_digest = format!("blake3:{}", binding.run.workload_digest);
    if provenance.generator_digest != generator_digest {
        return Err((
            EX_DATAERR,
            format!(
                "the provenance's generator_digest {} is not the ingest run's workload digest {generator_digest}",
                provenance.generator_digest
            ),
        ));
    }

    let entries: Vec<_> = corpus
        .into_iter()
        .filter(|e| e.runs_under(&binding.run.profile))
        .collect();
    let options = RunOptions {
        warmup_passes: args.warmup_passes,
        passes: args.passes,
        reps_per_pass: args.reps_per_pass,
        deadline: Duration::from_millis(args.query_timeout_ms),
        oracle_retries: args.oracle_retries,
    };
    let mut output = run(&binding, &entries, &workload, options)
        .await
        .map_err(|e| (EX_DATAERR, e.to_string()))?;

    provenance.schema_version = SCHEMA_VERSION;
    let config = &mut provenance.config;
    set_config(config, "profile", binding.run.profile.clone());
    set_config(config, "anchor_ms", binding.run.anchor_ms.to_string());
    set_config(config, "steps", binding.run.steps.to_string());
    set_config(config, "stream_digest", binding.run.stream_digest.clone());
    set_config(config, "warmup_passes", args.warmup_passes.to_string());
    set_config(config, "passes", args.passes.to_string());
    set_config(config, "reps_per_pass", args.reps_per_pass.to_string());
    set_config(
        config,
        "query_timeout_ms",
        args.query_timeout_ms.to_string(),
    );
    set_config(config, "oracle_retries", args.oracle_retries.to_string());
    set_config(
        config,
        "oracle_retries_used",
        output.oracle_retries.len().to_string(),
    );
    let mut retried: Vec<&str> = Vec::new();
    for r in &output.oracle_retries {
        if !retried.contains(&r.entry.as_str()) {
            retried.push(&r.entry);
        }
    }
    set_config(
        config,
        "oracle_retry_entries",
        if retried.is_empty() {
            "none".to_string()
        } else {
            retried.join(",")
        },
    );
    for (system, url) in endpoints(&binding) {
        set_config(config, &format!("query_url.{system}"), url);
    }
    if (args.passes as usize) < MIN_VARIANCE_PASSES {
        set_config(config, VARIANCE_CONFIG_KEY, INSUFFICIENT_PASSES.to_string());
    } else {
        config.retain(|c| c.key != VARIANCE_CONFIG_KEY);
    }

    let report = MetricsBenchReport {
        provenance,
        measurements: std::mem::take(&mut output.measurements),
        geomean_ms: None,
        cases_expected: std::mem::take(&mut output.cases_expected),
        denominators: std::mem::take(&mut output.denominators),
        exclusions: Vec::new(),
        run_verdict: (!failed_bands.is_empty()).then_some(RunVerdict::Failed),
        failed_bands,
    };
    let checked = if report.run_verdict.is_some() {
        validate_diagnostic(&report)
    } else {
        validate(&report)
    };
    checked.map_err(|e| {
        (
            EX_SOFTWARE,
            format!("the assembled report fails validation, nothing written: {e}"),
        )
    })?;
    let json = serde_json::to_vec_pretty(&report)
        .map_err(|e| (EX_SOFTWARE, format!("cannot serialize the report: {e}")))?;
    std::fs::write(&args.out, json)
        .map_err(|e| (EX_IOERR, format!("{}: {e}", args.out.display())))?;
    if let Some(path) = &args.diagnostics {
        let diagnostics = Diagnostics {
            profile: &binding.run.profile,
            corpus_digest: &corpus_digest,
            oracle: system_name(Engine::Prometheus),
            ulp_gaps: &output.ulp_gaps,
            oracle_retries: &output.oracle_retries,
        };
        let json = serde_json::to_vec_pretty(&diagnostics).map_err(|e| {
            (
                EX_SOFTWARE,
                format!("cannot serialize the diagnostics: {e}"),
            )
        })?;
        std::fs::write(path, json).map_err(|e| (EX_IOERR, format!("{}: {e}", path.display())))?;
    }
    Ok(())
}
