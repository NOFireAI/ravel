//! Query engine: snapshot resolve, segment pruning, footer-first reads,
//! sample iterators feeding the PromQL evaluator (ADR-0006).

mod config;
mod engine;
mod error;
mod fetcher;
pub mod http;

pub use config::{
    DEFAULT_DEADLINE, DEFAULT_FETCH_CONCURRENCY, DEFAULT_MAX_SAMPLES, DEFAULT_MAX_SEGMENTS,
    DEFAULT_MAX_SERIES, EngineConfig,
};
pub use engine::QueryEngine;
pub use error::QueryError;
pub use fetcher::{FetchError, FetchStats, FetchedSeries, FetchedSeriesSoa, SegmentFetcher};
