//! Report-only log body measurement (issue #2977): substring needle block
//! selectivity with a per-block trigram Bloom estimate, body pages against a
//! template split, and integer pages against a FastLanes-style layout. See
//! [`ravel_bench::logbody_report`] and docs/internal/bench-reports.md.
//!
//! ```text
//! logbody_report --corpus HDFS.log[.gz] --needles needles.txt --out report.json
//!     [--max-lines N] [--block-records N] [--object-records N] [--decode-runs N]
//!     [--rei-needles rei_needles.toml --rei-corpus NAME [--rei-max-gram N]
//!      [--rei-df-threshold X] [--rei-budget-share X]]
//! logbody_report --rei-needles rei_needles.toml --rei-decide report.json ...
//! ```
//!
//! With `--rei-corpus` the report also runs the REI arm (issue #3709,
//! [`ravel_bench::logbody`]) for that pre-registered corpus, prints one line
//! per figure and the corpus's decision line on stdout, and counts every
//! figure outside its band as a problem. `--block-records` defaults to the
//! corpus's pre-registered value there and must equal it when given.
//! `--rei-decide` reads written reports and prints the overall decision line
//! across every corpus the pre-registration names.
//!
//! Exit codes: `0` the report was written and every section holds figures,
//! `1` the run failed or the report has a problem (an empty section, a body
//! that did not rebuild byte for byte, an REI figure missing, repeated or out
//! of band); the report is still written in the second case so the problem can
//! be read. With `--rei-decide`, `0` is an overall GO and `1` anything else.

use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use ravel_bench::bench_env;
use ravel_bench::logbody::grams::{
    DEFAULT_BUDGET_SHARE, DEFAULT_DF_THRESHOLD, DEFAULT_MAX_GRAM, Params,
};
use ravel_bench::logbody::{ReiRun, overall, saved_decision, spec};
use ravel_bench::logbody_report::{Config, DEFAULT_DECODE_RUNS, run};

#[derive(Parser, Debug)]
#[command(about = "Measure log body search selectivity and page encodings over a line corpus")]
struct Args {
    /// The corpus: one log line per line, plain or gzip (`.gz`).
    #[arg(long, required_unless_present = "rei_decide")]
    corpus: Option<PathBuf>,
    /// The needles: one substring per line.
    #[arg(long, required_unless_present = "rei_decide")]
    needles: Option<PathBuf>,
    /// Where to write the JSON report.
    #[arg(long, required_unless_present = "rei_decide")]
    out: Option<PathBuf>,
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
    /// The REI pre-registration file (fixtures/logbody/rei_needles.toml).
    #[arg(long)]
    rei_needles: Option<PathBuf>,
    /// Run the REI arm for this pre-registered corpus.
    #[arg(long, requires = "rei_needles", conflicts_with = "rei_decide")]
    rei_corpus: Option<String>,
    /// Longest gram the selected arm considers. Must equal the corpus's
    /// pre-registered max_gram; the run is refused otherwise.
    #[arg(long, default_value_t = DEFAULT_MAX_GRAM)]
    rei_max_gram: usize,
    /// The selected arm keeps a gram held by at most this share of blocks.
    /// Must equal the corpus's pre-registered df_threshold.
    #[arg(long, default_value_t = DEFAULT_DF_THRESHOLD)]
    rei_df_threshold: f64,
    /// The selected arm's byte cap, as a share of compressed object bytes.
    /// Must equal the corpus's pre-registered budget_share.
    #[arg(long, default_value_t = DEFAULT_BUDGET_SHARE)]
    rei_budget_share: f64,
    /// Print the overall REI decision over these written reports and exit.
    #[arg(long, requires = "rei_needles", num_args = 1..)]
    rei_decide: Vec<PathBuf>,
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

fn read_spec(path: &PathBuf) -> Result<spec::ReiSpec, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    spec::parse(&text)
}

fn decide_overall(spec_path: &PathBuf, reports: &[PathBuf]) -> ExitCode {
    let spec = match read_spec(spec_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("logbody_report: {e}");
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
                eprintln!("logbody_report: {e}");
                return ExitCode::FAILURE;
            }
        }
    }
    let names: Vec<String> = spec.corpus.iter().map(|c| c.name.clone()).collect();
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
    if !args.rei_decide.is_empty() {
        let Some(spec_path) = &args.rei_needles else {
            eprintln!("logbody_report: --rei-decide needs --rei-needles");
            return ExitCode::FAILURE;
        };
        return decide_overall(spec_path, &args.rei_decide);
    }
    let (Some(corpus), Some(needles), Some(out)) = (args.corpus, args.needles, args.out) else {
        eprintln!("logbody_report: --corpus, --needles and --out are required");
        return ExitCode::FAILURE;
    };
    let rei = match (&args.rei_needles, &args.rei_corpus) {
        (Some(path), Some(name)) => {
            let found = read_spec(path).and_then(|s| {
                s.corpus(name)
                    .cloned()
                    .ok_or_else(|| format!("{} names no corpus {name}", path.display()))
            });
            match found {
                Ok(spec) => Some(ReiRun {
                    spec,
                    params: Params {
                        max_gram: args.rei_max_gram,
                        df_threshold: args.rei_df_threshold,
                        budget_share: args.rei_budget_share,
                    },
                }),
                Err(e) => {
                    eprintln!("logbody_report: {e}");
                    return ExitCode::FAILURE;
                }
            }
        }
        (Some(_), None) => {
            eprintln!("logbody_report: --rei-needles without --rei-corpus or --rei-decide");
            return ExitCode::FAILURE;
        }
        _ => None,
    };
    let days = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| (d.as_secs() / 86_400) as i64)
        .unwrap_or(0);
    let mut cfg = Config::new(corpus, needles, civil_date(days), bench_env::git_commit());
    cfg.cpu_model = bench_env::cpu_model();
    cfg.max_lines = args.max_lines;
    let block_records = args.block_records.or_else(|| {
        let r = rei.as_ref()?;
        eprintln!(
            "logbody_report: --block-records {} (pre-registered for rei corpus {})",
            r.spec.block_records, r.spec.name
        );
        Some(r.spec.block_records)
    });
    cfg.rei = rei;
    if let Some(b) = block_records {
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
    if let Err(e) = std::fs::write(&out, json + "\n") {
        eprintln!("logbody_report: write {}: {e}", out.display());
        return ExitCode::FAILURE;
    }
    if let Some(rei) = &report.rei {
        for line in rei.lines() {
            println!("{line}");
        }
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
            out.display()
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
