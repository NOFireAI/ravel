//! ADR-0109 (#606): load one bounded ClickBench-shaped Parquet sample through
//! the row path (`LogIngestRouter::write`) and the columnar fast path
//! (`LogIngestRouter::write_columnar`) in the same process, and report the wall
//! and CPU cost of each.
//!
//! Every figure is a LOCAL differential on a bounded synthetic sample over an
//! in-memory store. It is not the ClickBench reference figure, it does not
//! exercise ADR-0109 decision 3's dictionary path (#660), and it cannot see S3
//! latency, multi-shard scaling, or real PUT round trips. See the module docs
//! on `ravel_bench::columnar_load` for the full scope statement.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use clap::Parser;

use ravel_bench::columnar_load::{self, CorpusShape};

#[derive(Debug, Parser)]
#[command(about = "Differential load bench: row path vs columnar fast path (ADR-0109, #606)")]
struct Args {
    /// Corpus row count (bounded sample).
    #[arg(long, default_value_t = 50_000)]
    rows: usize,
    /// Number of Int64 attribute columns.
    #[arg(long, default_value_t = 60)]
    int_cols: usize,
    /// Number of Utf8 attribute columns.
    #[arg(long, default_value_t = 40)]
    str_cols: usize,
    /// Shard count. One keeps the whole corpus on a single shard so the
    /// differential is the write-path pivot and not shard fan-out.
    #[arg(long, default_value_t = 1)]
    shards: u32,
    /// Rows per Strict write (one RLOG object per batch).
    #[arg(long, default_value_t = 10_000)]
    batch_rows: usize,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let shape = CorpusShape {
        rows: args.rows,
        int_cols: args.int_cols,
        str_cols: args.str_cols,
    };

    let report = columnar_load::run(shape, args.shards, args.batch_rows)
        .await
        .expect("comparison run");

    let cpu_ms = |c: Option<std::time::Duration>| {
        c.map(|d| format!("{:.1}", d.as_secs_f64() * 1e3))
            .unwrap_or_else(|| "n/a".to_string())
    };

    println!("columnar-load differential (LOCAL, in-memory store, bounded sample)");
    println!(
        "  corpus           : {} rows, {} columns ({} attribute columns), {} bytes parquet",
        report.corpus_rows, report.columns, report.attr_columns, report.parquet_bytes
    );
    println!(
        "  config           : shards={} batch_rows={}",
        report.shards, report.batch_rows
    );
    println!(
        "  row path         : wall={:.1}ms cpu={}ms objects={} row_batches={} columnar_batches={}",
        report.row.wall.as_secs_f64() * 1e3,
        cpu_ms(report.row.cpu),
        report.row.objects_written,
        report.row.row_batches,
        report.row.columnar_batches,
    );
    println!(
        "  columnar path    : wall={:.1}ms cpu={}ms objects={} row_batches={} columnar_batches={}",
        report.columnar.wall.as_secs_f64() * 1e3,
        cpu_ms(report.columnar.cpu),
        report.columnar.objects_written,
        report.columnar.row_batches,
        report.columnar.columnar_batches,
    );
    println!("  wall speedup     : {:.2}x (row / columnar)", report.wall_speedup());
    match report.pivot_cpu_share() {
        Some(share) => println!(
            "  pivot CPU share  : {:.1}% of the row-path WRITE cpu (not of end-to-end load cpu; \
             decode is excluded and shared by both paths)",
            share * 100.0
        ),
        None => println!("  pivot CPU share  : n/a (CPU reading unavailable)"),
    }
    println!(
        "  scope            : local differential, NOT the ClickBench reference; decision-3 \
         dictionary path off (#660); S3/multi-shard/real-PUT not exercised"
    );
}
