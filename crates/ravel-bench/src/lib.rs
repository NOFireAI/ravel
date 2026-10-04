//! Benchmark harness for Ravel. Report-only: this crate never changes library
//! behavior, it only measures it.

/// The console line reporting the three-way split of abandoned flushes.
///
/// Shared by every bench binary that prints one. It lived as a verbatim copy
/// per binary, which is the drift this split exists to remove: a fourth
/// abandonment reason added to one copy and not the other makes two bench
/// reports print different splits of the same counter set.
pub fn format_abandoned_line(
    retry_exhausted: u64,
    queue_deadline: u64,
    input_rejected: u64,
) -> String {
    format!(
        "  abandoned         : retry_exhausted={retry_exhausted} \
queue_deadline={queue_deadline} input_rejected={input_rejected}"
    )
}

pub mod allocator;
pub mod bench_env;
#[cfg(feature = "sql-latency")]
pub mod clickbench_parquet;
pub mod codecs;
#[cfg(feature = "parquet-baseline")]
pub mod columnar_load;
pub mod concurrent;
pub mod cost_regression;
pub mod cost_report_source;
pub mod distrib_crossover;
pub mod e2e;
pub mod generator;
#[cfg(feature = "sql-latency")]
pub mod groupby_scaling;
pub mod harness;
pub mod ingest;
#[cfg(feature = "sql-latency")]
pub mod logs_scan_scaling;
pub mod metrics_gen;
pub mod metrics_ingest;
pub mod metrics_workload;
pub mod profiling;
pub mod promql_corpus;
pub mod pushdown_crossover;
pub mod query_latency;
#[cfg(feature = "parquet-baseline")]
pub mod read_accounting;
pub mod report;
pub mod report_schema;
pub mod section_accounting;
pub mod segment_support;
#[cfg(feature = "sql-latency")]
pub mod spans_scan;
#[cfg(feature = "sql-latency")]
pub mod sql_corpus;
#[cfg(feature = "sql-latency")]
pub mod sql_latency;
pub mod value_shapes;
