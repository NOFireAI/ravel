//! MetricsBench Stage 1 holder counts (ADR-3694, issue #3968).
//!
//! Reads the data objects `--objects-list` names (the JSON lines
//! `ravel-cli catalog list --json` prints for the folded snapshot, L0 and L1)
//! from `--objects-dir/<key>` (a download made by
//! `deploy/metricsbench/run.sh`'s `stage1-holders` step) and writes
//! `holders.json`: for each corpus statement under the ingest run's profile
//! with a positive equality matcher, the objects whose time range overlaps
//! the statement's fetch window on the run's query grid, and how many of
//! those hold at least one matching series. The window and counting rules
//! and the method statement live in [`ravel_bench::metrics_holders`]; no
//! router or key index is involved.
//!
//! Exit codes: 0 the report was written; 65 the list names no object, or the
//! ingest report does not bind to the workload; 66 an input is unreadable, a
//! list line does not parse, or a listed object is missing or does not
//! decode, and no report is written; 74 the report could not be written.

use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use ravel_bench::metrics_holders::{HoldersError, ListedObject, count_holders};
use ravel_bench::metrics_ingest::MetricsIngestReport;
use ravel_bench::metrics_query::run_grid;
use ravel_bench::metrics_workload::load_workload;
use ravel_bench::promql_corpus::load_corpus;

#[derive(Parser, Debug)]
#[command(about = "Count, per corpus statement, the data objects holding a matching series")]
struct Args {
    /// The PromQL corpus the query lanes ran.
    #[arg(long)]
    corpus: PathBuf,
    /// The ingest report of the run, whose profile and anchor give the query
    /// grid the statements were run on.
    #[arg(long)]
    ingest_report: PathBuf,
    /// The workload file the ingest ran.
    #[arg(long)]
    workload: PathBuf,
    /// The snapshot's data objects, one `catalog list --json` line each.
    #[arg(long)]
    objects_list: PathBuf,
    /// The directory the listed objects were downloaded into, each at its
    /// object key.
    #[arg(long)]
    objects_dir: PathBuf,
    /// Where `holders.json` is written.
    #[arg(long)]
    out: PathBuf,
}

fn fail(code: u8, message: String) -> ExitCode {
    eprintln!("metricsbench_holders: {message}");
    ExitCode::from(code)
}

fn main() -> ExitCode {
    let args = Args::parse();
    let corpus = match load_corpus(&args.corpus) {
        Ok(corpus) => corpus,
        Err(e) => return fail(66, format!("{}: {e}", args.corpus.display())),
    };
    let ingest_bytes = match fs::read(&args.ingest_report) {
        Ok(bytes) => bytes,
        Err(e) => return fail(66, format!("{}: {e}", args.ingest_report.display())),
    };
    let ingest: MetricsIngestReport = match serde_json::from_slice(&ingest_bytes) {
        Ok(ingest) => ingest,
        Err(e) => {
            return fail(
                66,
                format!(
                    "{}: not an ingest report: {e}",
                    args.ingest_report.display()
                ),
            );
        }
    };
    let workload_bytes = match fs::read(&args.workload) {
        Ok(bytes) => bytes,
        Err(e) => return fail(66, format!("{}: {e}", args.workload.display())),
    };
    let workload = match load_workload(&args.workload) {
        Ok(workload) => workload,
        Err(e) => return fail(66, format!("{}: {e}", args.workload.display())),
    };
    let (run, grid) = match run_grid(&ingest, &workload_bytes, &workload) {
        Ok(bound) => bound,
        Err(e) => return fail(65, e.to_string()),
    };
    let list_text = match fs::read_to_string(&args.objects_list) {
        Ok(text) => text,
        Err(e) => return fail(66, format!("{}: {e}", args.objects_list.display())),
    };
    let mut listed = Vec::new();
    for (n, line) in list_text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<ListedObject>(line) {
            Ok(object) => listed.push(object),
            Err(e) => {
                return fail(
                    66,
                    format!("{} line {}: {e}", args.objects_list.display(), n + 1),
                );
            }
        }
    }
    if listed.is_empty() {
        return fail(
            65,
            format!("{} names no object", args.objects_list.display()),
        );
    }
    let objects = listed.into_iter().map(|object| {
        let file = args.objects_dir.join(&object.key);
        match fs::read(&file) {
            Ok(bytes) => Ok((object, bytes)),
            Err(e) => Err(HoldersError {
                key: object.key,
                detail: format!("{}: {e}", file.display()),
            }),
        }
    });
    let report = match count_holders(&corpus, &run.profile, &grid, objects) {
        Ok(report) => report,
        Err(e) => return fail(66, e.to_string()),
    };
    let text = match serde_json::to_string_pretty(&report) {
        Ok(text) => text,
        Err(e) => return fail(74, e.to_string()),
    };
    if let Err(e) = fs::write(&args.out, text + "\n") {
        return fail(74, format!("{}: {e}", args.out.display()));
    }
    println!(
        "metricsbench_holders: {} statements counted, {} skipped, over {} L0 and {} L1 objects ({} series)",
        report.statements.len(),
        report.skipped.len(),
        report.objects_l0,
        report.objects_l1,
        report.series_total
    );
    ExitCode::SUCCESS
}
