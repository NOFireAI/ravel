//! Benchmark harness for Ravel. Report-only: this crate never changes library
//! behavior, it only measures it.

pub mod bench_env;
pub mod codecs;
pub mod concurrent;
pub mod distrib_crossover;
pub mod e2e;
pub mod generator;
#[cfg(feature = "sql-latency")]
pub mod groupby_scaling;
pub mod harness;
pub mod ingest;
pub mod profiling;
pub mod query_latency;
#[cfg(feature = "parquet-baseline")]
pub mod read_accounting;
pub mod report;
pub mod section_accounting;
pub mod segment_support;
#[cfg(feature = "sql-latency")]
pub mod sql_corpus;
#[cfg(feature = "sql-latency")]
pub mod sql_latency;
pub mod value_shapes;
