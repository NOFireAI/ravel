//! Query engine: snapshot resolve, segment pruning, footer-first reads,
//! sample iterators feeding the PromQL evaluator (ADR-0006).

#[cfg(test)]
mod cache_correctness;
mod config;
pub mod distrib;
mod engine;
pub mod erasure;
mod error;
mod fetcher;
pub mod http;
mod log_fetcher;
mod query_admission;
mod segment_admission;
pub mod span_fetcher;

pub use config::{
    ByteLimit, DEFAULT_DEADLINE, DEFAULT_FETCH_CONCURRENCY, DEFAULT_MAX_SAMPLES,
    DEFAULT_MAX_SEGMENTS, DEFAULT_MAX_SERIES, EngineConfig, REQUEST_BUDGET_FIXED_OVERHEAD,
    RequestLimit, derive_max_s3_requests,
};
pub use engine::{Coverage, QueryEngine, QueryStats, snapshot_erasure_predicates};
pub use error::QueryError;
pub use fetcher::{
    CacheFetchError, FetchError, FetchStats, FetchedSeries, FetchedSeriesSoa, SamplePriority,
    SegmentFetcher,
};
pub use log_fetcher::{
    ColumnarBlockOutcome, LogFetchError, LogFetchOutput, LogQuery, LogSegmentFetcher,
    LogSegmentScan, StreamAttrEquals,
};
pub use query_admission::{
    QueryAdmissionController, QueryConcurrencyLimit, QueryPermit, QueryRejected,
    query_admission_snapshot_key, reconcile_query_admission_once,
};
pub use segment_admission::{SegmentAdmission, admit, request_budget_exceeded};
pub use span_fetcher::{SpanFetchError, SpanFetchOutput, SpanRow, SpanSegmentFetcher};
