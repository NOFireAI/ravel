//! Measures the memory-budget charge of reading one L1 metrics part's catalog
//! at several part caps (issues #4393 and #4433), on two paths, so
//! `--max-l1-part-bytes` can be chosen from a measured figure:
//!
//! - the series listing, `QueryEngine::resolve_series`, which the metrics
//!   discovery endpoints call and which skips per-sample provenance;
//! - the sample read, `SegmentFetcher::fetch_series`, the catalog decode the
//!   PromQL and SQL sample paths run, which decodes and charges it.
//!
//! The bin writes one shard bucket shaped like the MetricsBench `history`
//! profile (2,500 series at a 15 s scrape: gauges, counters, classic histogram
//! series and 10 % native histograms) as L0 flushes onto a `MemoryStore`,
//! compacts it through ravel-maintain's metrics L1 build at each cap, and
//! binary-searches, for each path, the smallest `MemoryBudget` limit under
//! which a read of the first full part with `__name__=~".+"` succeeds, to 1 %.
//! The listing runs over a store holding only that part. The library module,
//! `ravel_bench::l1_listing_charge`, documents the shape, both calls and the
//! bands.
//!
//! Per cap it reports:
//!
//! - the part's wire bytes;
//! - its catalog sections' decoded length;
//! - the wire bytes either path's GETs hold during the decode;
//! - its samples and series;
//! - the listing's peak charge (the minimal succeeding budget), that peak per
//!   sample, and the listing charge it is checked against, the held fetch
//!   bytes plus the catalog decode length;
//! - the sample read's peak charge and that peak per sample;
//! - the upper bound issue #4393 states for the sample read, `(64 + 32) *
//!   samples + catalog decode length`, how far the sample read's peak lies
//!   above it, and the sample-read bound, that formula plus the held fetch
//!   bytes it leaves out.
//!
//! The bands are listing peak per sample in [1, 16] B (no per-sample decode
//! charged; the held fetch bytes and inflated catalog sections are all of it),
//! sample-read peak per sample in [96, 130] B (the full 96 B of per-sample
//! provenance must be charged, and not doubled), each peak at or below its
//! path's bound, and each path's peak non-decreasing as samples per part grow.
//! A cap that closes fewer than two full parts is refused; raise
//! `--history-hours`.
//!
//! ```text
//! cargo run --release -p ravel-bench --bin l1_listing_charge
//! cargo run --release -p ravel-bench --bin l1_listing_charge -- --caps 1048576,4194304
//! ```
//!
//! Measurement only: nothing here changes the query path. One JSON document
//! goes to stdout, a table to stderr. Exit 1 when a band fails or a figure is
//! missing or emitted twice, 2 when the run itself fails.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;
use ravel_bench::l1_listing_charge::{
    DEFAULT_CAPS, DEFAULT_SCRAPE_SECS, DEFAULT_SERIES, RunConfig, check_report, report_json,
    report_table, run,
};

#[derive(Parser, Debug)]
#[command(about = "Listing charge of one L1 metrics part by part cap (issue #4393)")]
struct Args {
    /// Part caps in bytes, comma-separated; the default is 1, 4, 16 and 64 MiB.
    #[arg(long, value_delimiter = ',')]
    caps: Option<Vec<u64>>,
    /// Series in the shard bucket.
    #[arg(long, default_value_t = DEFAULT_SERIES)]
    series: usize,
    /// Scrape interval in seconds.
    #[arg(long, default_value_t = DEFAULT_SCRAPE_SECS)]
    scrape_secs: i64,
    /// Hours of event time every series carries.
    #[arg(long, default_value_t = 72)]
    history_hours: i64,
    /// Hours of event time one L0 flush holds for every series.
    #[arg(long, default_value_t = 2)]
    flush_hours: i64,
}

fn main() {
    let args = Args::parse();
    let caps = args.caps.unwrap_or_else(|| DEFAULT_CAPS.to_vec());
    let config = RunConfig {
        series: args.series,
        scrape_secs: args.scrape_secs,
        history_hours: args.history_hours,
        flush_hours: args.flush_hours,
        caps,
    };
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let report = match rt.block_on(run(config)) {
        Ok(report) => report,
        Err(e) => {
            eprintln!("l1_listing_charge: {e}");
            std::process::exit(2);
        }
    };
    let text = serde_json::to_string_pretty(&report_json(&report)).expect("render report");
    println!("{text}");
    eprint!("{}", report_table(&report));
    if let Err(failures) = check_report(&text, &report.config.caps) {
        for failure in failures {
            eprintln!("l1_listing_charge: BAND FAIL: {failure}");
        }
        std::process::exit(1);
    }
    eprintln!("l1_listing_charge: all bands hold");
}
