//! MetricsBench report tool (ADR-0927, issue #936).
//!
//! This is the entry point that makes the reconciled report schema
//! ([`ravel_bench::report_schema`]) reachable from a real caller, in both
//! directions:
//!
//! - `render --report <path>` reads a report artifact, runs
//!   [`ravel_bench::report_schema::validate`] over it, and only then renders the
//!   table with [`ravel_bench::report_schema::render`]. A malformed artifact
//!   fails through this binary (a non-zero exit and the specific error on
//!   stderr), not only through a direct call to the validator, so the
//!   fail-closed contract is enforced where a consumer actually runs it. The
//!   renderer derives its table entirely from the artifact; it is never
//!   hand-maintained.
//! - `provenance` gathers the reconciled provenance a producer stamps on a
//!   report: the Ravel git commit, the toolchain, the hardware, the object-store
//!   backend, and the content digests of the workload manifest and PromQL
//!   corpus. The subprocess and `/proc` reads live here, off the library's
//!   deterministic path, exactly as `bench_report` and `bench_env` do. It emits
//!   the [`ravel_bench::report_schema::Provenance`] shape, so the emit and the
//!   consume sides speak one type.
//!
//! The measurement-producing harness (ADR-0927 decision 1) is separate work;
//! this tool checks and renders the artifact it produces, and stamps the
//! provenance it carries.
#![allow(clippy::expect_used)]

use std::path::PathBuf;
use std::process::{Command, ExitCode};

use clap::{Parser, Subcommand};
use ravel_bench::report_schema::{
    Backend, Comparator, ConfigEntry, Hardware, MetricsBenchReport, Provenance, SCHEMA_VERSION,
    render, validate,
};

#[derive(Parser, Debug)]
#[command(about = "Validate and render a MetricsBench report, or stamp its provenance (ADR-0927)")]
struct Args {
    #[command(subcommand)]
    command: CommandKind,
}

#[derive(Subcommand, Debug)]
enum CommandKind {
    /// Validate a report artifact and render its table. A report that fails
    /// validation exits non-zero with the specific error, before any table or
    /// summary is produced.
    Render {
        /// The report artifact to validate and render.
        #[arg(long)]
        report: PathBuf,
    },
    /// Gather and print the reconciled provenance block a producer stamps on a
    /// report. The digests are the content digests of the two named artifacts.
    Provenance {
        /// The workload manifest whose content digest becomes `generator_digest`.
        #[arg(long)]
        workload: PathBuf,
        /// The PromQL corpus whose content digest becomes `corpus_digest`.
        #[arg(long)]
        corpus: PathBuf,
        /// The ingest/query protocol the run drove.
        #[arg(long, default_value = "remote_write_1.0")]
        protocol: String,
        /// The object-store backend name.
        #[arg(long, default_value = "memory")]
        store_backend: String,
        /// The backend region, or the `"n/a"` sentinel.
        #[arg(long, default_value = "n/a")]
        region: String,
        /// The backend endpoint, or the `"n/a"` sentinel.
        #[arg(long, default_value = "n/a")]
        endpoint: String,
        /// Whether the backend bills requests (true only for the real-S3 lane,
        /// ADR-0927 decision 10).
        #[arg(long, default_value_t = false)]
        bills_requests: bool,
        /// A comparator pin, `name=version=image_digest`, repeatable. Omitted for
        /// a Ravel-only diagnostic run.
        #[arg(long = "comparator", value_name = "NAME=VERSION=DIGEST")]
        comparators: Vec<String>,
        /// A non-default configuration entry, `key=value`, repeatable.
        #[arg(long = "config", value_name = "KEY=VALUE")]
        config: Vec<String>,
    },
}

/// Trimmed stdout of `program args`, or `None` on any failure. Provenance must
/// never be a silent wrong value, so the caller decides the fallback.
fn command_stdout(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// The commit these numbers describe. `GITHUB_SHA` (set by CI) wins so a report
/// from a detached HEAD still names the branch tip; otherwise ask git. A build
/// whose commit cannot be identified is a loud error at stamp time, never a
/// sentinel that reads like data (ADR-0927: a figure without its provenance is
/// not evidence).
fn git_commit() -> Result<String, String> {
    resolve_git_commit(
        std::env::var("GITHUB_SHA").ok(),
        command_stdout("git", &["rev-parse", "HEAD"]),
    )
}

/// The decision `git_commit` makes, split out so it is testable without a git
/// checkout: `from_git` is exactly what `command_stdout` yields, i.e. `None`
/// when the git command is absent or fails.
fn resolve_git_commit(env_sha: Option<String>, from_git: Option<String>) -> Result<String, String> {
    if let Some(sha) = env_sha
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
    {
        return Ok(sha);
    }
    from_git.ok_or_else(|| {
        "cannot determine ravel_git_commit: GITHUB_SHA is unset and `git rev-parse HEAD` produced \
         no output (run on a git checkout with git installed, or set GITHUB_SHA)"
            .to_string()
    })
}

fn toolchain() -> Result<String, String> {
    command_stdout("rustc", &["--version"]).ok_or_else(|| {
        "cannot determine toolchain: `rustc --version` produced no output (is rustc on PATH?)"
            .to_string()
    })
}

/// `uname -srm`, falling back to the compile-time OS constant. This is a genuine
/// OS identifier, not a missing-data sentinel, so it stays infallible.
fn os_string() -> String {
    command_stdout("uname", &["-srm"]).unwrap_or_else(|| std::env::consts::OS.to_string())
}

fn logical_cores() -> u32 {
    std::thread::available_parallelism()
        .map(|n| n.get() as u32)
        .unwrap_or(1)
}

/// The content digest of a file, `blake3:<hex>`, or an error naming the path.
fn file_digest(path: &std::path::Path) -> Result<String, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    Ok(format!("blake3:{}", blake3::hash(&bytes).to_hex()))
}

/// Parse a `name=version=digest` comparator pin.
fn parse_comparator(spec: &str) -> Result<Comparator, String> {
    let parts: Vec<&str> = spec.splitn(3, '=').collect();
    if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
        return Err(format!(
            "comparator `{spec}` must be name=version=image_digest with all three present"
        ));
    }
    Ok(Comparator {
        name: parts[0].to_string(),
        version: parts[1].to_string(),
        image_digest: parts[2].to_string(),
    })
}

/// Parse a `key=value` config entry.
fn parse_config(spec: &str) -> Result<ConfigEntry, String> {
    let (key, value) = spec
        .split_once('=')
        .ok_or_else(|| format!("config `{spec}` must be key=value"))?;
    if key.is_empty() {
        return Err(format!("config `{spec}` has an empty key"));
    }
    Ok(ConfigEntry {
        key: key.to_string(),
        value: value.to_string(),
    })
}

fn run() -> Result<(), String> {
    let args = Args::parse();
    match args.command {
        CommandKind::Render { report } => {
            let text = std::fs::read_to_string(&report)
                .map_err(|e| format!("read report {}: {e}", report.display()))?;
            let doc: MetricsBenchReport = serde_json::from_str(&text).map_err(|e| {
                format!(
                    "report {} is not a valid MetricsBench report: {e}",
                    report.display()
                )
            })?;
            // `render` runs `validate` first; a malformed artifact fails here,
            // through this binary, not only through a direct validator call.
            let rendered = render(&doc).map_err(|e| e.to_string())?;
            print!("{rendered}");
            Ok(())
        }
        CommandKind::Provenance {
            workload,
            corpus,
            protocol,
            store_backend,
            region,
            endpoint,
            bills_requests,
            comparators,
            config,
        } => {
            let comparators = comparators
                .iter()
                .map(|s| parse_comparator(s))
                .collect::<Result<Vec<_>, _>>()?;
            let config = config
                .iter()
                .map(|s| parse_config(s))
                .collect::<Result<Vec<_>, _>>()?;
            let provenance = Provenance {
                schema_version: SCHEMA_VERSION,
                ravel_git_commit: git_commit()?,
                toolchain: toolchain()?,
                protocol,
                hardware: Hardware {
                    os: os_string(),
                    // Best-effort and infallible: ARM and macOS name the CPU in
                    // a form other than x86's `model name`, and a host that
                    // names it in none records `"unknown"` rather than aborting
                    // the stamp (issue #976). `hardware.cpu_model` is
                    // deliberately not a checked identity field, so an
                    // `"unknown"` here is visible but not blocking.
                    cpu_model: ravel_bench::bench_env::cpu_model(),
                    logical_cores: logical_cores(),
                    instance_type: std::env::var("RAVEL_INSTANCE_TYPE")
                        .ok()
                        .filter(|t| !t.trim().is_empty()),
                },
                backend: Backend {
                    store_backend,
                    region,
                    endpoint,
                    backend_bills_requests: bills_requests,
                },
                comparators,
                generator_digest: file_digest(&workload)?,
                corpus_digest: file_digest(&corpus)?,
                config,
            };
            // Prove the stamp we emit is one a report would accept: a producer
            // that pasted this block into a report and then failed validation on
            // it would be a silent contract break.
            if let Err(err) = validate_provenance_only(&provenance) {
                return Err(format!("gathered provenance does not validate: {err}"));
            }
            println!(
                "{}",
                serde_json::to_string_pretty(&provenance)
                    .map_err(|e| format!("serialize provenance: {e}"))?
            );
            Ok(())
        }
    }
}

/// Validate just the provenance, by validating a minimal report built around it.
/// `report_schema::validate` is the whole-report entry point, so the provenance
/// stamp is checked through the same code a consumer runs, with one dummy timed
/// measurement standing in for the harness's rows.
fn validate_provenance_only(
    p: &Provenance,
) -> Result<(), ravel_bench::report_schema::ValidationError> {
    use ravel_bench::promql_corpus::CostClass;
    use ravel_bench::report_schema::{Figure, Measurement, ResultStatus};
    let probe = MetricsBenchReport {
        provenance: p.clone(),
        measurements: vec![Measurement {
            id: "provenance_probe".to_string(),
            class: CostClass::MetadataOnly,
            status: ResultStatus::Ok,
            figures: vec![
                Figure {
                    name: "min_ms".to_string(),
                    value: 0.0,
                },
                Figure {
                    name: "median_ms".to_string(),
                    value: 0.0,
                },
                Figure {
                    name: "max_ms".to_string(),
                    value: 0.0,
                },
            ],
        }],
        geomean_ms: None,
    };
    validate(&probe)
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("metricsbench_report: {err}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A git command that produces nothing (what `command_stdout` returns when
    /// git is absent or fails) is an ERROR that names the missing field, not a
    /// sentinel. `from_git = None` is exactly the failing-command case.
    #[test]
    fn a_failed_git_command_is_an_error_naming_the_field() {
        let err = resolve_git_commit(None, None)
            .expect_err("no GITHUB_SHA and a failed git command must error");
        assert!(
            err.contains("ravel_git_commit"),
            "the error must name the missing field, got: {err}"
        );
    }

    /// `GITHUB_SHA` wins over git and is trimmed.
    #[test]
    fn github_sha_is_used_and_trimmed() {
        assert_eq!(
            resolve_git_commit(Some("  abc123  ".to_string()), None).unwrap(),
            "abc123"
        );
    }

    /// A blank `GITHUB_SHA` falls through to git.
    #[test]
    fn a_blank_github_sha_falls_through_to_git() {
        assert_eq!(
            resolve_git_commit(Some("   ".to_string()), Some("deadbeef".to_string())).unwrap(),
            "deadbeef"
        );
    }

    // The CPU-model resolution moved to `ravel_bench::bench_env` (one shared
    // resolver for both provenance sites, issue #976); its per-key-form tests
    // live beside it there.
}
