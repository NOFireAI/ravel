//! Benchmark harness for Ravel. Report-only: this crate never changes library
//! behavior, it only measures it.

pub mod codecs;
pub mod concurrent;
pub mod distrib_crossover;
pub mod e2e;
pub mod generator;
pub mod harness;
pub mod ingest;
pub mod query_latency;
#[cfg(feature = "parquet-baseline")]
pub mod read_accounting;
pub mod section_accounting;
pub mod segment_support;
