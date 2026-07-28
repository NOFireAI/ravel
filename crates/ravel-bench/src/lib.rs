//! Phase 1 benchmark harness for Ravel (docs/benchmarking.md). Report-only:
//! this crate never changes library behavior, it only measures it.

pub mod generator;
#[cfg(feature = "parquet-baseline")]
pub mod read_accounting;
pub mod segment_support;
