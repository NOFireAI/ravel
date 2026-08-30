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
mod phase_accounting;
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
    CacheFetchError, FetchError, FetchStats, FetchedSeries, FetchedSeriesSoa, ReadCache,
    SamplePriority, SegmentFetcher,
};
pub use log_fetcher::{
    AssemblyBufferStats, BlockRangeFetcher, BlockRangeStats, BlockStatsReport, CarriedFooter,
    ColumnarBlockOutcome, DEFAULT_LOG_COALESCE_GAP, DEFAULT_LOG_COVERAGE_THRESHOLD,
    DEFAULT_LOG_MAX_CONCURRENT_GETS, DEFAULT_LOG_REQUEST_COST_BYTES, DEFAULT_LOG_SUFFIX_LEN,
    DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD, LOG_SUFFIX_FLOOR_BYTES, LOG_SUFFIX_SIZE_DIVISOR,
    LogFetchError, LogFetchOutput, LogQuery, LogSegmentFetcher, LogSegmentScan, ProbeMissCounter,
    ProbeMissCounts, ProbePhase, ReadPhases, StreamAttrEquals, WHOLE_OBJECT_REQUEST_MULTIPLE,
    derive_suffix_len,
};
pub use phase_accounting::{
    PhaseAccounting, PhaseAccountingSnapshot, PhaseWireByteCounter, PhaseWireByteCounts, QueryPhase,
};
pub use query_admission::{
    QueryAdmissionController, QueryConcurrencyLimit, QueryPermit, QueryRejected,
    query_admission_snapshot_key, reconcile_query_admission_once,
};
pub use segment_admission::{SegmentAdmission, admit, request_budget_exceeded};
pub use span_fetcher::{SpanFetchError, SpanFetchOutput, SpanRow, SpanSegmentFetcher};
