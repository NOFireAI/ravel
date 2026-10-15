//! Report-only log body measurement (issue #2977): substring needle block
//! selectivity with a per-block trigram Bloom estimate, body pages against a
//! template split, and integer pages against a FastLanes-style layout. See
//! [`ravel_bench::logbody_report`] and docs/internal/bench-reports.md.
//!
//! ```text
//! logbody_report --corpus HDFS.log[.gz] --needles needles.txt --out report.json
//!     [--max-lines N] [--block-records N] [--object-records N] [--decode-runs N]
//! ```
//!
//! Exit codes: `0` the report was written and every section holds figures,
//! `1` the run failed or the report has a problem (an empty section, a body
//! that did not rebuild byte for byte); the report is still written in the
//! second case so the problem can be read.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use ravel_bench::bench_env;
use ravel_bench::logbody_report::{Config, DEFAULT_DECODE_RUNS, run};

#[derive(Parser, Debug)]
#[command(about = "Measure log body search selectivity and page encodings over a line corpus")]
struct Args {
    /// The corpus: one log line per line, plain or gzip (`.gz`).
    #[arg(long)]
    corpus: PathBuf,
    /// The needles: one substring per line.
    #[arg(long)]
    needles: PathBuf,
    /// Where to write the JSON report.
    #[arg(long)]
    out: PathBuf,
    /// Read at most this many lines (default: the whole corpus).
    #[arg(long)]
    max_lines: Option<u64>,
    /// Records per block (default: the production writer's).
    #[arg(long)]
    block_records: Option<usize>,
    /// Records per RLOG object (default: one full row group of blocks).
    #[arg(long)]
    object_records: Option<usize>,
    /// Timed runs per decode; the median is reported.
    #[arg(long, default_value_t = DEFAULT_DECODE_RUNS)]
    decode_runs: usize,
}

/// `YYYY-MM-DD` (UTC) for days since 1970-01-01 (Howard Hinnant's
/// `civil_from_days`).
fn civil_date(days: i64) -> String {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!("{y:04}-{m:02}-{d:02}")
}

fn main() -> ExitCode {
    let args = Args::parse();
    let days = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| (d.as_secs() / 86_400) as i64)
        .unwrap_or(0);
    let mut cfg = Config::new(
        args.corpus,
        args.needles,
        civil_date(days),
        bench_env::git_commit(),
    );
    cfg.cpu_model = bench_env::cpu_model();
    cfg.max_lines = args.max_lines;
    if let Some(b) = args.block_records {
        cfg.block_records = b;
        if args.object_records.is_none() {
            cfg.object_records = b * ravel_logseg::RlogConfig::default().group_target_blocks;
        }
    }
    if let Some(o) = args.object_records {
        cfg.object_records = o;
    }
    cfg.decode_runs = args.decode_runs;

    let report = match run(&cfg) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("logbody_report: {e}");
            return ExitCode::FAILURE;
        }
    };
    let json = match serde_json::to_string_pretty(&report) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("logbody_report: serialize: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = std::fs::write(&args.out, json + "\n") {
        eprintln!("logbody_report: write {}: {e}", args.out.display());
        return ExitCode::FAILURE;
    }
    let problems = report.problems();
    for p in &problems {
        eprintln!("logbody_report: {p}");
    }
    if problems.is_empty() {
        eprintln!(
            "logbody_report: {} lines, {} blocks, report at {}",
            report.stamp.line_count,
            report.stamp.blocks,
            args.out.display()
        );
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::civil_date;

    #[test]
    fn civil_dates() {
        assert_eq!(civil_date(0), "1970-01-01");
        assert_eq!(civil_date(19_723), "2024-01-01");
        assert_eq!(civil_date(20_740), "2026-10-14");
    }
}
