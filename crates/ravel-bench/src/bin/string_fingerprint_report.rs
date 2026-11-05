//! Report-only string fingerprint measurement (issue #3710, ADR-3692 work
//! package 4, Plan row T12). See [`ravel_bench::string_fingerprint`] and
//! [`ravel_bench::string_fingerprint_report`] for the measurement itself, and
//! docs/internal/bench-reports.md for the tool-level writeup.
//!
//! ```text
//! string_fingerprint_report --subject NAME --corpus-lines CORPUS.log[.gz] --out report.json
//!     [--spec fixtures/string_fingerprint/subjects.toml] [--object-records N] [--zstd-level N]
//! string_fingerprint_report --subject NAME --corpus-parquet hits.parquet --corpus-column url \
//!     --out report.json   # parquet-baseline feature only
//! string_fingerprint_report --decide report1.json report2.json ... [--spec ...]
//! ```
//!
//! Exit codes: `0` the report was written and every figure matched its
//! pre-registered band; `1` the run failed, or the report has a problem (a
//! missing, duplicate, out-of-band, or unbanded figure); the report is still
//! written in the second case so the problem can be read. With `--decide`,
//! `0` is an overall GO and `1` anything else.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use ravel_bench::string_fingerprint::{overall, saved_decision, spec};
use ravel_bench::string_fingerprint_report::{Config, Corpus, run};

#[derive(Parser, Debug)]
#[command(
    about = "Measure a learned per-block string summary against the production bloom and contains baselines"
)]
struct Args {
    /// The pre-registration file.
    #[arg(
        long,
        default_value = "crates/ravel-bench/fixtures/string_fingerprint/subjects.toml"
    )]
    spec: PathBuf,
    /// The subject name from the pre-registration file.
    #[arg(long, required_unless_present = "decide")]
    subject: Option<String>,
    /// One value per line of this (possibly gzip-compressed) text file.
    #[arg(long, conflicts_with = "decide")]
    corpus_lines: Option<PathBuf>,
    /// One value per row of this Parquet file's named `Utf8` column.
    #[cfg(feature = "parquet-baseline")]
    #[arg(long, requires = "corpus_column", conflicts_with_all = ["corpus_lines", "decide"])]
    corpus_parquet: Option<PathBuf>,
    /// The Parquet column `--corpus-parquet` reads.
    #[cfg(feature = "parquet-baseline")]
    #[arg(long, requires = "corpus_parquet")]
    corpus_column: Option<String>,
    /// Records per RLOG object (default: one full row group of blocks).
    #[arg(long)]
    object_records: Option<usize>,
    /// Read at most this many corpus values (default: the whole corpus).
    #[arg(long)]
    max_lines: Option<u64>,
    /// The writer's zstd level.
    #[arg(long, default_value_t = 3)]
    zstd_level: i32,
    /// Where to write the JSON report.
    #[arg(long, required_unless_present = "decide")]
    out: Option<PathBuf>,
    /// Print the overall decision over these written reports and exit.
    #[arg(long, num_args = 1..)]
    decide: Vec<PathBuf>,
}

fn read_spec(path: &PathBuf) -> Result<spec::FingerprintSpec, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    spec::parse(&text)
}

fn decide_overall(spec_path: &PathBuf, reports: &[PathBuf]) -> ExitCode {
    let spec = match read_spec(spec_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("string_fingerprint_report: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut saved = Vec::new();
    for path in reports {
        let read = std::fs::read_to_string(path)
            .map_err(|e| format!("read {}: {e}", path.display()))
            .and_then(|j| saved_decision(&j).map_err(|e| format!("{}: {e}", path.display())));
        match read {
            Ok(s) => {
                println!("{}", s.decision.line);
                saved.push(s);
            }
            Err(e) => {
                eprintln!("string_fingerprint_report: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    let names: Vec<String> = spec.subject.iter().map(|s| s.name.clone()).collect();
    let d = overall(&names, &saved);
    println!("{}", d.line);
    if d.go {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn main() -> ExitCode {
    let args = Args::parse();
    if !args.decide.is_empty() {
        return decide_overall(&args.spec, &args.decide);
    }
    let (Some(subject), Some(out)) = (args.subject.clone(), args.out.clone()) else {
        eprintln!("string_fingerprint_report: --subject and --out are required");
        return ExitCode::FAILURE;
    };

    let parsed = match read_spec(&args.spec) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("string_fingerprint_report: {e}");
            return ExitCode::FAILURE;
        }
    };
    let Some(subject_spec) = parsed.subject(&subject) else {
        eprintln!(
            "string_fingerprint_report: no subject named {subject:?} in {}",
            args.spec.display()
        );
        return ExitCode::FAILURE;
    };

    #[cfg(feature = "parquet-baseline")]
    let parquet = args.corpus_parquet.clone().zip(args.corpus_column.clone());
    #[cfg(not(feature = "parquet-baseline"))]
    let parquet: Option<(PathBuf, String)> = None;

    let corpus = match (args.corpus_lines.clone(), parquet) {
        (Some(path), None) => Corpus::Lines(path),
        #[cfg(feature = "parquet-baseline")]
        (None, Some((path, column))) => Corpus::ParquetColumn { path, column },
        (None, None) => {
            eprintln!(
                "string_fingerprint_report: one of --corpus-lines or --corpus-parquet is required"
            );
            return ExitCode::FAILURE;
        }
        _ => {
            eprintln!(
                "string_fingerprint_report: --corpus-lines and --corpus-parquet are mutually exclusive"
            );
            return ExitCode::FAILURE;
        }
    };

    let object_records = args.object_records.unwrap_or_else(|| {
        subject_spec.block_records * ravel_logseg::RlogConfig::default().group_target_blocks
    });

    let cfg = Config {
        spec_path: args.spec.clone(),
        subject,
        corpus,
        object_records,
        zstd_level: args.zstd_level,
        max_lines: args.max_lines,
    };

    let run_report = match run(&cfg) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("string_fingerprint_report: {e}");
            return ExitCode::FAILURE;
        }
    };
    let json = match serde_json::to_string_pretty(&run_report.report) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("string_fingerprint_report: serialize: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = std::fs::write(&out, json + "\n") {
        eprintln!("string_fingerprint_report: write {}: {e}", out.display());
        return ExitCode::FAILURE;
    }
    for line in run_report.report.lines() {
        println!("{line}");
    }
    let problems = &run_report.report.problems;
    for p in problems {
        eprintln!("string_fingerprint_report: {p}");
    }
    if problems.is_empty() {
        eprintln!(
            "string_fingerprint_report: {} objects, {} bytes, report at {}",
            run_report.objects,
            run_report.object_bytes,
            out.display()
        );
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
