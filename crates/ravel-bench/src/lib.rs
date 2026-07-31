//! Phase 1 benchmark harness for Ravel (docs/benchmarking.md). Report-only:
//! this crate never changes library behavior, it only measures it.

pub mod codecs;
pub mod concurrent;
pub mod e2e;
pub mod generator;
pub mod harness;
pub mod query_latency;
#[cfg(feature = "parquet-baseline")]
pub mod read_accounting;
pub mod section_accounting;
pub mod segment_support;
