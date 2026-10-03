//! The ClickBench Parquet lane's operator entry point (ADR-2040 D7, issue
//! #2055): runs the upstream suite through a running `ravel-server`, judges
//! every answer against the datafusion-cli reference, writes the stamped
//! report, and exits non-zero when the report misses D7's bar.
//!
//! The binary does not drop caches, and it runs the statements back to
//! back. q01's first try is cold only if the operator restarted the server
//! and dropped the page cache first, as the runbook in
//! docs/internal/clickbench.md describes; every later statement's first try
//! starts with whatever the statements before it left cached.
//!
//! `cargo run --release -p ravel-bench --features sql-latency --bin
//! clickbench_parquet_bench -- --server http://127.0.0.1:4318 --token-env
//! RAVEL_TOKEN --arm a --location s3://clickbench/hits/ --reference
//! benchmarks/clickbench/parquet/ref --prereg
//! benchmarks/clickbench/parquet/prereg.toml --server-log server.log --out
//! report.json --sql-max-query-bytes N --sql-tenant-max-bytes N`

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use ravel_bench::bench_env;
use ravel_bench::clickbench_parquet::concurrency::{
    self, Clock, DEFAULT_TASKS, MonotonicClock, PhaseTask,
};
use ravel_bench::clickbench_parquet::engine::{HttpEngine, SuiteEngine};
use ravel_bench::clickbench_parquet::report::{
    self, Arm, ClickBenchParquetReport, Provenance, StatementFigures, Totals,
};
use ravel_bench::clickbench_parquet::suite::{self, STATEMENT_COUNT};

/// The datafusion-cli release D7 names for the reference outputs; the
/// `VERSION` file `make-reference.sh` writes must hold exactly this.
const REFERENCE_VERSION: &str = "datafusion-cli 54.1.0";

/// Timed tries per statement. Try 1 is the cold figure; the better of
/// tries 2 and 3 is the hot one.
const TRIES: usize = 3;

#[derive(Parser, Debug)]
#[command(about = "ClickBench Parquet lane: run, judge and stamp one arm (ADR-2040 D7)")]
struct Args {
    /// The server root, for example `http://127.0.0.1:9090`.
    #[arg(long)]
    server: String,
    /// Name of the environment variable holding the bearer token. The token
    /// itself is never taken on the command line.
    #[arg(long)]
    token_env: String,
    /// `a` mounts the `hits/` prefix of 100 files; `b` the single
    /// `hits.parquet`.
    #[arg(long, value_enum)]
    arm: Arm,
    /// The `LOCATION` the table is mounted from.
    #[arg(long)]
    location: String,
    /// Directory of `qNN.json` reference outputs and their `VERSION` file,
    /// as `make-reference.sh` writes them.
    #[arg(long)]
    reference: PathBuf,
    /// The filled-in `prereg.toml`.
    #[arg(long)]
    prereg: PathBuf,
    /// The running server's log, read for the settings the report is
    /// stamped with.
    #[arg(long)]
    server_log: PathBuf,
    /// Where the JSON report is written.
    #[arg(long)]
    out: PathBuf,
    /// Length of the concurrency phase in seconds; 0 skips it. D7's phase
    /// is 600.
    #[arg(long, default_value_t = 0)]
    concurrency_seconds: u64,
    /// Connections in the concurrency phase, each with its own engine.
    #[arg(long, default_value_t = DEFAULT_TASKS)]
    concurrency_tasks: usize,
    /// The `--sql-max-query-bytes` the server was started with.
    #[arg(long)]
    sql_max_query_bytes: u64,
    /// The `--sql-tenant-max-bytes` the server was started with.
    #[arg(long)]
    sql_tenant_max_bytes: u64,
}

/// The bearer token from the environment variable named `name`, read
/// through `lookup`. Refuses a name that is not a plain variable name. No
/// error includes `name`: a hex token pasted in its place can pass the
/// name check.
fn read_token(name: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<String, String> {
    let valid = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !valid {
        return Err(
            "--token-env takes the name of an environment variable ([A-Za-z_][A-Za-z0-9_]*), \
             not the token"
                .to_string(),
        );
    }
    match lookup(name) {
        Some(token) if !token.is_empty() => Ok(token),
        _ => Err("the environment variable named by --token-env is unset or empty".to_string()),
    }
}

fn reference_path(dir: &Path, number: u32) -> PathBuf {
    dir.join(format!("q{number:02}.json"))
}

/// Refuses a reference directory not written by datafusion-cli 54.1.0, or
/// missing any statement's output.
fn check_reference_dir(dir: &Path) -> Result<String, String> {
    let version_path = dir.join("VERSION");
    let version = std::fs::read_to_string(&version_path)
        .map_err(|e| format!("cannot read {}: {e}", version_path.display()))?;
    let version = version.trim().to_string();
    if version != REFERENCE_VERSION {
        return Err(format!(
            "{} says {version:?}; D7's reference is {REFERENCE_VERSION:?}",
            version_path.display()
        ));
    }
    let missing: Vec<String> = (1..=STATEMENT_COUNT as u32)
        .map(|n| reference_path(dir, n))
        .filter(|p| !p.is_file())
        .map(|p| p.display().to_string())
        .collect();
    if !missing.is_empty() {
        return Err(format!("reference outputs missing: {}", missing.join(", ")));
    }
    Ok(version)
}

async fn run(args: Args) -> Result<bool, String> {
    let prereg = report::load_prereg(&args.prereg).map_err(|e| e.to_string())?;
    let token = read_token(&args.token_env, |name| std::env::var(name).ok())?;
    args.arm.check_location(&args.location)?;
    if args.location.contains('\'') {
        return Err(format!(
            "--location may not contain a quote: {}",
            args.location
        ));
    }
    let reference_version = check_reference_dir(&args.reference)?;
    let server_log = std::fs::read_to_string(&args.server_log)
        .map_err(|e| format!("cannot read {}: {e}", args.server_log.display()))?;
    let server_settings = report::parse_server_settings(&server_log);
    let suite = suite::load_default().map_err(|e| e.to_string())?;

    let engine = HttpEngine::new(&args.server, &token);
    engine
        .ddl("DROP TABLE IF EXISTS hits")
        .await
        .map_err(|e| e.to_string())?;
    let receipt = engine
        .ddl(&suite.table.render(&args.location))
        .await
        .map_err(|e| e.to_string())?;
    if receipt.files != Some(args.arm.expected_files()) {
        return Err(format!(
            "CREATE EXTERNAL TABLE mounted {:?} files; arm {:?} mounts {}",
            receipt.files,
            args.arm,
            args.arm.expected_files()
        ));
    }

    let mut statements = Vec::with_capacity(suite.statements.len());
    for statement in &suite.statements {
        let number = statement.number;
        let mut times = [0.0; TRIES];
        let mut first_answer = None;
        let mut error = None;
        for (try_index, time) in times.iter_mut().enumerate() {
            let started = Instant::now();
            let result = engine.query(&statement.sql).await;
            *time = started.elapsed().as_secs_f64();
            match result {
                Ok(batches) if try_index == 0 => first_answer = Some(batches),
                Ok(_) => {}
                Err(e) => {
                    error = Some(e.to_string());
                    break;
                }
            }
        }
        let figures = match (error, first_answer) {
            (Some(error), _) => StatementFigures::failed(number, error),
            (None, None) => StatementFigures::failed(number, "no answer recorded".to_string()),
            (None, Some(batches)) => {
                let comparison = std::fs::read_to_string(reference_path(&args.reference, number))
                    .map_err(|e| format!("cannot read reference: {e}"))
                    .and_then(|text| {
                        report::judge_against_reference(
                            statement,
                            suite.override_for(number),
                            &text,
                            &batches,
                        )
                    });
                StatementFigures::answered(number, times, comparison)
            }
        };
        eprintln!(
            "q{number:02} cold={:?} hot={:?} verdict={} error={}",
            figures.cold_s,
            figures.hot_s,
            figures
                .verdict
                .as_ref()
                .map_or_else(|| "none".to_string(), ToString::to_string),
            figures.error.as_deref().unwrap_or("none"),
        );
        statements.push(figures);
    }

    let concurrency = if args.concurrency_seconds == 0 {
        None
    } else {
        let clock: Arc<dyn Clock> = Arc::new(MonotonicClock::new());
        let tasks = (0..args.concurrency_tasks)
            .map(|_| PhaseTask {
                engine: Arc::new(HttpEngine::new(&args.server, &token)) as Arc<dyn SuiteEngine>,
                clock: Arc::clone(&clock),
            })
            .collect();
        let figures = concurrency::run(
            tasks,
            &suite.statements,
            Duration::from_secs(args.concurrency_seconds),
            &prereg.failures,
        )
        .await
        .map_err(|e| e.to_string())?;
        eprintln!(
            "concurrency: {} tasks, {} completed, {} errors, qps={}, error_ratio={}, \
             unregistered_error_ratio={}",
            figures.tasks,
            figures.queries_completed,
            figures.errors,
            figures.qps,
            figures.error_ratio,
            figures.unregistered_error_ratio
        );
        Some(figures)
    };

    let registered_failures_answered = report::registered_failures_answered(&statements, &prereg);
    let report = ClickBenchParquetReport {
        provenance: Provenance {
            git_sha: bench_env::git_commit(),
            binary: std::env::current_exe()
                .map_or_else(|e| format!("unknown: {e}"), |p| p.display().to_string()),
            arm: args.arm,
            location: args.location.clone(),
            server: args.server.clone(),
            server_log: args.server_log.display().to_string(),
            server_settings,
            sql_max_query_bytes: args.sql_max_query_bytes,
            sql_tenant_max_bytes: args.sql_tenant_max_bytes,
            reference_dir: args.reference.display().to_string(),
            reference_version,
            ddl_files: receipt.files,
        },
        totals: Totals::from_statements(&statements),
        statements,
        registered_failures_answered,
        concurrency,
    };
    let json = serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?;
    std::fs::write(&args.out, json)
        .map_err(|e| format!("cannot write {}: {e}", args.out.display()))?;
    eprintln!(
        "report written to {}: hot {} s, cold {} s, {} answered, failed {:?}",
        args.out.display(),
        report.totals.hot_sum_s,
        report.totals.cold_sum_s,
        report.totals.answered,
        report.totals.failed
    );
    for number in &report.registered_failures_answered {
        eprintln!("finding: pre-registered failure q{number} answered");
    }

    match report::check(&report, &prereg) {
        Ok(()) => {
            eprintln!("D7 check: no violations");
            Ok(true)
        }
        Err(violations) => {
            for violation in &violations {
                eprintln!("violation: {violation}");
            }
            eprintln!("D7 check: {} violations", violations.len());
            Ok(false)
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let args = Args::parse();
    match run(args).await {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(error) => {
            eprintln!("clickbench_parquet_bench: {error}");
            ExitCode::from(2)
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use clap::error::ErrorKind;

    use super::*;

    const REQUIRED: [&str; 20] = [
        "--server",
        "http://127.0.0.1:9090",
        "--token-env",
        "RAVEL_TOKEN",
        "--arm",
        "a",
        "--location",
        "s3://clickbench/hits/",
        "--reference",
        "ref",
        "--prereg",
        "prereg.toml",
        "--server-log",
        "server.log",
        "--out",
        "report.json",
        "--sql-max-query-bytes",
        "1",
        "--sql-tenant-max-bytes",
        "2",
    ];

    fn parse(extra: &[&str], drop_flag: Option<&str>) -> Result<Args, clap::Error> {
        let mut argv = vec!["clickbench_parquet_bench"];
        let mut i = 0;
        while i < REQUIRED.len() {
            if Some(REQUIRED[i]) != drop_flag {
                argv.extend_from_slice(&REQUIRED[i..i + 2]);
            }
            i += 2;
        }
        argv.extend_from_slice(extra);
        Args::try_parse_from(argv)
    }

    #[test]
    fn the_full_flag_set_parses_with_concurrency_off() {
        let args = parse(&[], None).expect("parses");
        assert_eq!(args.concurrency_seconds, 0);
        assert_eq!(args.concurrency_tasks, 10);
        assert_eq!(args.arm, Arm::A);
    }

    #[test]
    fn a_missing_prereg_is_refused() {
        let error = parse(&[], Some("--prereg")).expect_err("--prereg is required");
        assert_eq!(error.kind(), ErrorKind::MissingRequiredArgument);
        assert!(error.to_string().contains("--prereg"), "{error}");
    }

    #[test]
    fn a_token_passed_directly_is_refused() {
        let error = parse(&["--token", "s3cr3t"], None).expect_err("no --token flag");
        assert_eq!(error.kind(), ErrorKind::UnknownArgument);
        let error = read_token("Bearer s3cr3t", |_| Some("x".to_string()))
            .expect_err("not a variable name");
        assert!(!error.contains("s3cr3t"), "{error}");
        assert_eq!(
            read_token("RAVEL_TOKEN", |name| (name == "RAVEL_TOKEN")
                .then(|| "devtoken".to_string())),
            Ok("devtoken".to_string())
        );
        assert!(read_token("RAVEL_TOKEN", |_| None).is_err());
        assert!(read_token("RAVEL_TOKEN", |_| Some(String::new())).is_err());
    }

    #[test]
    fn a_hex_token_in_place_of_the_name_is_never_echoed() {
        // The shape `openssl rand -hex 16` prints, starting with a letter,
        // so it passes the variable-name check.
        let token = "deadbeef0123456789abcdef01234567";
        assert_eq!(token.len(), 32);
        let error = read_token(token, |_| None).expect_err("no such variable");
        assert!(!error.contains(token), "{error}");
        assert_eq!(
            error,
            "the environment variable named by --token-env is unset or empty"
        );
    }

    #[test]
    fn the_reference_directory_must_be_datafusion_cli_54_1_0_and_complete() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("VERSION"), "datafusion-cli 54.0.0\n").expect("write");
        let error = check_reference_dir(dir.path()).expect_err("wrong version");
        assert!(error.contains("54.0.0"), "{error}");
        std::fs::write(dir.path().join("VERSION"), "datafusion-cli 54.1.0\n").expect("write");
        for n in 1..=43 {
            if n != 7 {
                std::fs::write(reference_path(dir.path(), n), "[]").expect("write");
            }
        }
        let error = check_reference_dir(dir.path()).expect_err("q07 missing");
        assert!(error.contains("q07.json"), "{error}");
        std::fs::write(reference_path(dir.path(), 7), "[]").expect("write");
        assert_eq!(
            check_reference_dir(dir.path()),
            Ok("datafusion-cli 54.1.0".to_string())
        );
    }
}
