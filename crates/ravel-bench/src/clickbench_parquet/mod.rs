//! ClickBench Parquet lane (ADR-2040 D7, issue #2055): the upstream suite
//! definition, the result comparator, the synthetic fixture, and the engines
//! that run the suite (in-process Ravel, an in-process DataFusion reference,
//! and a running server over HTTP), plus the concurrency phase and the
//! report `clickbench_parquet_bench` writes and checks against D7. The
//! end-to-end check is `tests/clickbench_corpus.rs`'s
//! `parquet_lane_runs_the_upstream_suite_verbatim`.

pub mod comparator;
pub mod concurrency;
pub mod engine;
pub mod fixture;
pub mod report;
pub mod suite;
