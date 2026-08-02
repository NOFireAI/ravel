//! Query engine: snapshot resolve, segment pruning, footer-first reads,
//! sample iterators feeding the PromQL evaluator (ADR-0006).

#[cfg(test)]
mod cache_correctness;
mod config;
mod engine;
mod error;
mod fetcher;
pub mod http;
mod log_fetcher;

pub use config::{
    DEFAULT_DEADLINE, DEFAULT_FETCH_CONCURRENCY, DEFAULT_MAX_SAMPLES, DEFAULT_MAX_SEGMENTS,
    DEFAULT_MAX_SERIES, EngineConfig,
};
pub use engine::{QueryEngine, QueryStats};
pub use error::QueryError;
pub use fetcher::{FetchError, FetchStats, FetchedSeries, FetchedSeriesSoa, SegmentFetcher};
pub use log_fetcher::{
    LogFetchError, LogFetchOutput, LogQuery, LogSegmentFetcher, StreamAttrEquals,
};
