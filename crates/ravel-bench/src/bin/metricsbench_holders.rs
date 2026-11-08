//! MetricsBench Stage 1 holder counts (ADR-3694, issue #3968).
//!
//! Reads every `*.rseg` under `--objects-dir` (a download of one tenant's
//! `t/<tenant>/m/l0/` prefix, made by `deploy/metricsbench/run.sh`'s
//! `stage1-holders` step) and writes `holders.json`: for each corpus statement
//! under `--profile` with a positive equality matcher, the number of objects
//! holding at least one matching series. The counting rule and its method
//! statement live in [`ravel_bench::metrics_holders`]; no router or key index
//! is involved.
//!
//! Exit codes: 0 the report was written; 65 the directory holds no `*.rseg`
//! object, so there is nothing to count; 66 the corpus or an object is
//! unreadable or does not decode, and no report is written; 74 the report
//! could not be written.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::Parser;
use ravel_bench::metrics_holders::{HoldersError, count_holders};
use ravel_bench::promql_corpus::load_corpus;

#[derive(Parser, Debug)]
#[command(about = "Count, per corpus statement, the data objects holding a matching series")]
struct Args {
    /// The PromQL corpus the query lanes ran.
    #[arg(long)]
    corpus: PathBuf,
    /// The workload profile, which selects the corpus entries counted.
    #[arg(long)]
    profile: String,
    /// The directory holding the downloaded data objects, searched
    /// recursively for `*.rseg`.
    #[arg(long)]
    objects_dir: PathBuf,
    /// Where `holders.json` is written.
    #[arg(long)]
    out: PathBuf,
}

fn rseg_files(dir: &Path, found: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            rseg_files(&path, found)?;
        } else if path.extension().is_some_and(|e| e == "rseg") {
            found.push(path);
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    let args = Args::parse();
    let corpus = match load_corpus(&args.corpus) {
        Ok(corpus) => corpus,
        Err(e) => {
            eprintln!("metricsbench_holders: {}: {e}", args.corpus.display());
            return ExitCode::from(66);
        }
    };
    let mut files = Vec::new();
    if let Err(e) = rseg_files(&args.objects_dir, &mut files) {
        eprintln!("metricsbench_holders: {}: {e}", args.objects_dir.display());
        return ExitCode::from(66);
    }
    if files.is_empty() {
        eprintln!(
            "metricsbench_holders: no *.rseg object under {}",
            args.objects_dir.display()
        );
        return ExitCode::from(65);
    }
    files.sort();
    let objects = files.iter().map(|path| {
        let key = path
            .strip_prefix(&args.objects_dir)
            .unwrap_or(path)
            .display()
            .to_string();
        fs::read(path)
            .map(|bytes| (key.clone(), bytes))
            .map_err(|e| HoldersError {
                key,
                detail: e.to_string(),
            })
    });
    let report = match count_holders(&corpus, &args.profile, objects) {
        Ok(report) => report,
        Err(e) => {
            eprintln!("metricsbench_holders: {e}");
            return ExitCode::from(66);
        }
    };
    let text = match serde_json::to_string_pretty(&report) {
        Ok(text) => text,
        Err(e) => {
            eprintln!("metricsbench_holders: {e}");
            return ExitCode::from(74);
        }
    };
    if let Err(e) = fs::write(&args.out, text + "\n") {
        eprintln!("metricsbench_holders: {}: {e}", args.out.display());
        return ExitCode::from(74);
    }
    println!(
        "metricsbench_holders: {} statements counted, {} skipped, over {} objects ({} series)",
        report.statements.len(),
        report.skipped.len(),
        report.objects_total,
        report.series_total
    );
    ExitCode::SUCCESS
}
