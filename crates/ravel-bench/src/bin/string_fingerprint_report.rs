//! Report-only string fingerprint measurement (issue #3710, ADR-3692 work
//! package 4, Plan row T12). See [`ravel_bench::string_fingerprint`] and
//! [`ravel_bench::string_fingerprint_report`] for the measurement itself, and
//! docs/internal/bench-reports.md for the writeup.
//!
//! ```text
//! string_fingerprint_report --subject NAME --corpus-lines CORPUS.log[.gz] --out report.json
//!     [--spec PATH] [--gram-len N] [--budget-share X] [--fallback-min-bits N]
//!     [--zstd-level N] [--block-records N] [--object-records N] [--max-lines N] [--seed N]
//! string_fingerprint_report --subject NAME --corpus-parquet hits.parquet --corpus-column URL \
//!     --out report.json   # parquet-baseline feature only
//! string_fingerprint_report --describe --corpus-lines CORPUS.log [--max-lines N]
//! string_fingerprint_report --decide report1.json report2.json ... [--spec PATH]
//! ```
//!
//! Every run parameter comes from the pre-registration file. A parameter
//! flag is a check, not a setting: a value other than the pre-registered one
//! is refused before the corpus is read.
//!
//! Exit codes: `0` the report was written and has no structural problem (a
//! missing, duplicate, unexpected or not-pre-registered figure); `1` the run
//! failed, or the report has such a problem (it is still written so the
//! problem can be read). A figure outside its band is printed as `MISS` and
//! does not change the exit code. With `--decide`, `0` is an overall GO and
//! `1` anything else.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use ravel_bench::string_fingerprint::{overall, saved_decision, spec};
use ravel_bench::string_fingerprint_report::{Config, Corpus, Overrides, describe, run};

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
    #[arg(long)]
    subject: Option<String>,
    /// One value per line of this (possibly gzip-compressed) text file.
    #[arg(long, conflicts_with = "decide")]
    corpus_lines: Option<PathBuf>,
    /// One value per row of this Parquet file's named `Utf8` or `Binary` column.
    #[cfg(feature = "parquet-baseline")]
    #[arg(long, requires = "corpus_column", conflicts_with_all = ["corpus_lines", "decide"])]
    corpus_parquet: Option<PathBuf>,
    /// The Parquet column `--corpus-parquet` reads.
    #[cfg(feature = "parquet-baseline")]
    #[arg(long, requires = "corpus_parquet")]
    corpus_column: Option<String>,
    #[arg(long)]
    gram_len: Option<usize>,
    #[arg(long)]
    budget_share: Option<f64>,
    #[arg(long)]
    fallback_min_bits: Option<usize>,
    #[arg(long)]
    zstd_level: Option<i32>,
    #[arg(long)]
    block_records: Option<usize>,
    #[arg(long)]
    object_records: Option<usize>,
    /// With `--describe`, the most values to read (default: all).
    #[arg(long)]
    max_lines: Option<u64>,
    #[arg(long)]
    seed: Option<u64>,
    /// Where to write the JSON report.
    #[arg(long)]
    out: Option<PathBuf>,
    /// Print the corpus facts a pre-registration needs and exit.
    #[arg(long, conflicts_with_all = ["decide", "subject", "out"])]
    describe: bool,
    /// Print the overall decision over these written reports and exit.
    #[arg(long, num_args = 1..)]
    decide: Vec<PathBuf>,
}

fn read_spec(path: &PathBuf) -> Result<spec::FingerprintSpec, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    spec::parse(&text)
}

fn decide_overall(spec_path: &PathBuf, reports: &[PathBuf]) -> Result<bool, String> {
    let spec = read_spec(spec_path)?;
    let mut saved = Vec::new();
    for path in reports {
        let json =
            std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
        let s = saved_decision(&json).map_err(|e| format!("{}: {e}", path.display()))?;
        println!("{}", s.decision.line);
        saved.push(s);
    }
    let names: Vec<String> = spec.subject.iter().map(|s| s.name.clone()).collect();
    let d = overall(&names, &saved);
    println!("{}", d.line);
    Ok(d.go)
}

fn corpus(args: &Args) -> Result<Corpus, String> {
    #[cfg(feature = "parquet-baseline")]
    let parquet = args.corpus_parquet.clone().zip(args.corpus_column.clone());
    #[cfg(not(feature = "parquet-baseline"))]
    let parquet: Option<(PathBuf, String)> = None;
    match (args.corpus_lines.clone(), parquet) {
        (Some(path), None) => Ok(Corpus::Lines(path)),
        #[cfg(feature = "parquet-baseline")]
        (None, Some((path, column))) => Ok(Corpus::ParquetColumn { path, column }),
        (None, None) => Err("one of --corpus-lines or --corpus-parquet is required".into()),
        _ => Err("--corpus-lines and --corpus-parquet are mutually exclusive".into()),
    }
}

fn measure(args: &Args) -> Result<bool, String> {
    let (Some(subject), Some(out)) = (args.subject.clone(), args.out.clone()) else {
        return Err("--subject and --out are required".into());
    };
    let cfg = Config {
        spec_path: args.spec.clone(),
        subject,
        corpus: corpus(args)?,
        overrides: Overrides {
            gram_len: args.gram_len,
            budget_share: args.budget_share,
            fallback_min_bits: args.fallback_min_bits,
            zstd_level: args.zstd_level,
            block_records: args.block_records,
            object_records: args.object_records,
            max_lines: args.max_lines,
            seed: args.seed,
        },
    };
    let r = run(&cfg)?;
    let json = serde_json::to_string_pretty(&r.report).map_err(|e| format!("serialize: {e}"))?;
    std::fs::write(&out, json + "\n").map_err(|e| format!("write {}: {e}", out.display()))?;
    for line in r.report.lines() {
        println!("{line}");
    }
    println!(
        "string_fingerprint objects={} object_bytes={}",
        r.objects, r.object_bytes
    );
    for p in &r.report.checked.problems {
        eprintln!("string_fingerprint_report: problem: {p}");
    }
    for m in &r.report.checked.misses {
        println!("string_fingerprint miss {m}");
    }
    Ok(r.report.checked.problems.is_empty())
}

fn main() -> ExitCode {
    let args = Args::parse();
    let result = if !args.decide.is_empty() {
        decide_overall(&args.spec, &args.decide)
    } else if args.describe {
        corpus(&args)
            .and_then(|c| describe(&c, args.max_lines.unwrap_or(u64::MAX)))
            .map(|line| {
                println!("{line}");
                true
            })
    } else {
        measure(&args)
    };
    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(e) => {
            eprintln!("string_fingerprint_report: {e}");
            ExitCode::FAILURE
        }
    }
}
