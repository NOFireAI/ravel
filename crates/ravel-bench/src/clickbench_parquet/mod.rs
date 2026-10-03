//! ClickBench Parquet lane (ADR-2040 D7, issue #2055): the upstream suite
//! definition, the result comparator, the synthetic fixture, and the engines
//! that run the suite (in-process Ravel, an in-process DataFusion reference,
//! and a running server over HTTP). The end-to-end check is
//! `tests/clickbench_corpus.rs`'s `parquet_lane_runs_the_upstream_suite_verbatim`.

pub mod comparator;
pub mod engine;
pub mod fixture;
pub mod suite;
