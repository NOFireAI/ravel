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
mod query_admission;

pub use config::{
    ByteLimit, DEFAULT_DEADLINE, DEFAULT_FETCH_CONCURRENCY, DEFAULT_MAX_SAMPLES,
    DEFAULT_MAX_SEGMENTS, DEFAULT_MAX_SERIES, EngineConfig,
};
pub use engine::{QueryEngine, QueryStats};
pub use error::QueryError;
pub use fetcher::{
    CacheFetchError, FetchError, FetchStats, FetchedSeries, FetchedSeriesSoa, SegmentFetcher,
};
pub use log_fetcher::{
    LogFetchError, LogFetchOutput, LogQuery, LogSegmentFetcher, StreamAttrEquals,
};
pub use query_admission::{
    QueryAdmissionController, QueryConcurrencyLimit, QueryPermit, QueryRejected,
    query_admission_snapshot_key, reconcile_query_admission_once,
};
