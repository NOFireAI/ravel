//! Spans SQL scan bench: the columnar fast path against the row path over one
//! shared corpus (issue #641, epic #630, ADR-0110 decision 7). Thin wrapper
//! around `ravel_bench::spans_scan::run`: parses the corpus knobs and
//! `--store memory|s3` via `ravel_bench::harness`, gathers provenance
//! (`git rev-parse HEAD`, `rustc --version`), then prints the report as JSON
//! plus a human table.
//!
//! One projection EXCLUDES the `attrs` map column and takes the columnar path;
//! one INCLUDES it and takes the row path. The report states which path each
//! shape actually ran (via the scan's partition metrics), its rows/second and
//! decoded page bytes (`page_bytes_decoded` from `QueryAccounting`, the only
//! decoded counter recorded on both paths), and the ratio between the two
//! shapes. The corpus carries attributes and events on every span, or the two
//! shapes decode identical pages and the ratio is a vacuous 1.0.
//!
//! Report-only: a standalone measurement tool, not wired into any production
//! path. Gated on the `sql-latency` feature so the default build never compiles
//! ravel-sql/datafusion/ravel-rspan or this bin.
//!
//! Run: `cargo run -p ravel-bench --release --features sql-latency \
//!   --bin spans_scan_bench -- --store memory`
#![allow(clippy::expect_used)]

use std::process::Command;

use clap::Parser;
use ravel_bench::harness::{StoreKind, store_from_env};
use ravel_bench::spans_scan::{Report, SpansScanConfig, run};
use serde::Serialize;

#[derive(Parser, Debug)]
#[command(
    about = "Spans SQL scan: columnar (attrs-excluded) vs row (attrs-included) over one corpus"
)]
struct Args {
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    /// Total spans generated.
    #[arg(long, default_value_t = 20_000)]
    spans: usize,
    /// Real attributes per span (beyond the lifted `service.name`). Must be
    /// positive, or the columnar path has no attribute pages to skip.
    #[arg(long, default_value_t = 8)]
    attrs_per_span: usize,
    /// Events per span. Must be positive, or the columnar path has no event
    /// pages to skip.
    #[arg(long, default_value_t = 3)]
    events_per_span: usize,
    /// Spans per RSPAN object.
    #[arg(long, default_value_t = 500)]
    records_per_object: usize,
    /// Writer block target; small so each object carries several blocks.
    #[arg(long, default_value_t = 64)]
    block_target_records: usize,
    /// Timed repetitions per shape; the first is the run whose path metrics are
    /// reported.
    #[arg(long, default_value_t = 7)]
    runs: usize,
    /// Small corpus for a fast CI-sized run.
    #[arg(long, default_value_t = false)]
    smoke: bool,
}

/// The report plus its provenance header, so a committed number names the commit
/// and toolchain that produced it (ADR-0075 decision 3, as the other bench bins
/// apply it).
#[derive(Serialize)]
struct Document {
    git_commit: String,
    toolchain: String,
    host_logical_cores: usize,
    report: Report,
}

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

fn git_commit() -> String {
    if let Ok(sha) = std::env::var("GITHUB_SHA")
        && !sha.trim().is_empty()
    {
        return sha.trim().to_string();
    }
    command_stdout("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_string())
}

fn toolchain() -> String {
    command_stdout("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_string())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let args = Args::parse();
    let config = if args.smoke {
        SpansScanConfig {
            runs: args.runs,
            ..SpansScanConfig::smoke(store_from_env(args.store), &args.store.to_string())
        }
    } else {
        SpansScanConfig {
            store: store_from_env(args.store),
            store_label: args.store.to_string(),
            spans: args.spans,
            attrs_per_span: args.attrs_per_span,
            events_per_span: args.events_per_span,
            records_per_object: args.records_per_object,
            block_target_records: args.block_target_records,
            runs: args.runs,
        }
    };
    let report = run(&config).await;

    let document = Document {
        git_commit: git_commit(),
        toolchain: toolchain(),
        host_logical_cores: std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0),
        report,
    };

    println!(
        "{}",
        serde_json::to_string_pretty(&document).expect("serialize report")
    );
    print_human_table(&document);
}

fn print_human_table(doc: &Document) {
    let r = &doc.report;
    println!();
    println!("spans_scan_bench report");
    println!("  commit    : {}", doc.git_commit);
    println!("  toolchain : {}", doc.toolchain);
    println!("  store     : {}", r.config.store);
    println!("  cores     : {}", r.config.cores);
    println!("  profile   : {}", r.config.profile);
    println!("  runs/shape: {}", r.config.runs);
    println!(
        "  corpus    : {} spans, {} attrs/span, {} events/span, {} objects ({} spans/object), block_target={}",
        r.corpus.spans,
        r.corpus.attrs_per_span,
        r.corpus.events_per_span,
        r.corpus.objects,
        r.corpus.records_per_object,
        r.corpus.block_target_records,
    );
    println!();
    println!(
        "  {:<22} | {:>7} | {:>7} | {:>8} | {:>8} | {:>12} | {:>12} | {:>10} | {:>14}",
        "shape",
        "colmnr",
        "rowpath",
        "pgDec*",
        "pgSkp*",
        "pgBytesFetch",
        "pgBytesDec",
        "med (ms)",
        "rows/sec",
    );
    println!(
        "  {:-<22}-+-{:-<7}-+-{:-<7}-+-{:-<8}-+-{:-<8}-+-{:-<12}-+-{:-<12}-+-{:-<10}-+-{:-<14}",
        "", "", "", "", "", "", "", "", ""
    );
    for s in [&r.attrs_free, &r.attrs_including] {
        println!(
            "  {:<22} | {:>7} | {:>7} | {:>8} | {:>8} | {:>12} | {:>12} | {:>10.3} | {:>14.0}",
            s.shape,
            s.columnar_batches,
            s.rowpath_batches,
            s.pages_decoded,
            s.pages_skipped,
            s.page_bytes_fetched,
            s.page_bytes_decoded,
            s.median_ms,
            s.rows_per_sec,
        );
    }
    println!(
        "  (* pgDec/pgSkp are partition metrics on both paths; the row path decodes \
         every page of each block it scans and skips none)"
    );
    println!();
    println!(
        "  page_bytes_decoded ratio (row/columnar): {:.3}",
        r.page_bytes_decoded_ratio
    );
    println!(
        "  rows/sec ratio (columnar/row)          : {:.3}",
        r.rows_per_sec_ratio
    );
}
