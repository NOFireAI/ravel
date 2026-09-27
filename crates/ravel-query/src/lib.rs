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
pub mod io_shape;
mod limiter;
mod log_fetcher;
pub mod log_series;
mod phase_accounting;
mod query_admission;
mod request_budgets;
mod reserved_bytes;
mod segment_admission;
pub mod span_fetcher;
#[cfg(test)]
pub(crate) mod test_tracing;

pub use config::{
    ALERT_DELIVERY_SLACK, BUDGETED_REQUESTS_PER_UNSEALED_FLUSH, ByteLimit,
    DEFAULT_BUDGET_REFERENCE_SHARDS, DEFAULT_DEADLINE, DEFAULT_FETCH_CONCURRENCY,
    DEFAULT_LOG_MAX_FETCH_RUN_BYTES, DEFAULT_MAX_SAMPLES, DEFAULT_MAX_SEGMENTS, DEFAULT_MAX_SERIES,
    EngineConfig, EngineConfigError, FOLD_STALL_ALERT_FOR, LATENCY_FIRST_MEASURED_CONCURRENCY,
    LogsFetchPolicy, MAX_REQUESTS_PER_UNSEALED_FLUSH, REFERENCE_CLOCK_SKEW_ALLOWANCE,
    REFERENCE_FOLD_SAFETY_MARGIN, REFERENCE_MAX_FLUSH_LIFETIME, REQUEST_BUDGET_FIXED_OVERHEAD,
    REQUESTS_PER_UNSEALED_FLUSH, RequestBudgetParts, RequestLimit, ResolvedLogsFetch, SealMargin,
    covered_span, derive_max_s3_requests, derive_max_s3_requests_for, healthy_tail_max,
    request_budget_parts, resolve_logs_fetch,
};
pub use engine::{
    Coverage, LiveQueryAccounting, QueryEngine, QueryStats, snapshot_erasure_predicates,
};
pub use error::{FOLD_LAST_SUCCESS_GAUGE, FoldLag, QueryError};
pub use fetcher::{
    CacheFetchError, DEFAULT_WHOLE_OBJECT_THRESHOLD, FetchError, FetchStats, FetchedSeries,
    FetchedSeriesSoa, MAX_GETS_PER_L0_SEGMENT_FETCH, MAX_PAGE_RANGE_GETS_PER_L0_SEGMENT, ReadCache,
    SamplePriority, SegmentFetcher,
};
pub use limiter::{GetLimiter, GetLimiterClosed};
pub use log_fetcher::{
    AssemblyBufferStats, BlockRangeFetcher, BlockRangeStats, BlockStatsReport, CarriedFooter,
    CarriedWholeObject, ColumnarBlockOutcome, DEFAULT_LOG_COALESCE_GAP,
    DEFAULT_LOG_COVERAGE_THRESHOLD, DEFAULT_LOG_MAX_CONCURRENT_GETS,
    DEFAULT_LOG_REQUEST_COST_BYTES, DEFAULT_LOG_SUFFIX_LEN, DEFAULT_LOG_WHOLE_OBJECT_THRESHOLD,
    LOG_SUFFIX_FLOOR_BYTES, LOG_SUFFIX_SIZE_DIVISOR, LogFetchError, LogFetchOutput, LogQuery,
    LogSegmentFetcher, LogSegmentScan, ProbeMissCounter, ProbeMissCounts, ProbePhase, ReadPhases,
    StreamAttrEquals, WHOLE_OBJECT_REQUEST_MULTIPLE, derive_suffix_len,
};
pub use phase_accounting::{
    PhaseAccounting, PhaseAccountingSnapshot, PhaseWireByteCounter, PhaseWireByteCounts, QueryPhase,
};
pub use query_admission::{
    QueryAdmissionController, QueryConcurrencyLimit, QueryPermit, QueryRejected,
    query_admission_snapshot_key, reconcile_query_admission_once,
};
pub use request_budgets::{EffectiveBudgets, RequestBudgets};
pub use segment_admission::{
    RequestBudget, SegmentAdmission, admit, request_budget_exceeded, resolved_fold_lag,
    resolved_unsealed_tail,
};
pub use span_fetcher::{SpanFetchError, SpanFetchOutput, SpanRow, SpanSegmentFetcher};
