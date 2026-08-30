//! ravel-sql: DataFusion-backed SQL execution over Ravel metric samples.
//!
//! This crate is the structural isolation boundary that keeps `datafusion`
//! (and, transitively, its own `arrow`) out of the PromQL and ingest paths
//! (ADR-0013). Nothing in the
//! ingest-critical or PromQL crates links this crate.
//!
//! The read pipeline skeleton:
//!
//! ```text
//! RsegScanExec (N partitions, each sorted by (series_id, ts, provenance))
//!   -> SortPreservingMergeExec on (series_id, ts)
//!   -> RsegDedupExec (single partition, streaming, full dedup total order)
//!   -> DataFusion operators
//! ```
//!
//! Arrow types are used exclusively through the `datafusion::arrow`
//! re-export so this crate is internally version-consistent regardless of
//! the workspace `arrow` pin.

//! The request-handling half: the read-only
//! single-statement gate (`validate`), the fresh per-query single-tenant
//! session (`session`), the resolve/plan/execute driver with the snapshot
//! retry contract (`executor`), the two wire encodings (`output`), and the
//! error-to-client redaction boundary (`error`). The HTTP surface itself
//! lives in services/ravel-server behind its `sql` feature; nothing here
//! links axum, and nothing there links datafusion.

//! The second transport, behind the `flight-sql`
//! feature: `flight` is the `FlightSqlService` implementation and
//! `flight_ticket` its snapshot-pinning ticket codec. It is a transport and
//! nothing more -- it validates, resolves, plans, and executes through the
//! same `SqlExecutor` the HTTP path uses, so the two cannot answer the same
//! query differently. Its own additions are the ones the two-RPC shape forces:
//! pinning the resolved snapshot into the ticket so `DoGet` never re-resolves
//! and checking the metadata-resolved tenant against the
//! ticket's own before redeeming it.

mod alerts_provider;
mod alerts_pushdown;
mod alerts_scan;
mod alerts_schema;
mod audit_provider;
mod audit_pushdown;
mod audit_scan;
mod audit_schema;
mod avg;
mod config;
pub mod conformance;
mod cost;
mod declared;
mod dedup;
#[cfg(feature = "flight-sql")]
pub mod distributed;
#[cfg(feature = "flight-sql")]
pub mod distributed_rlog;
mod error;
mod executor;
#[cfg(feature = "flight-sql")]
pub mod flight;
#[cfg(feature = "flight-sql")]
mod flight_ticket;
mod group_keys;
mod labels;
mod late_materialization;
mod like_udf;
mod logs_provider;
mod logs_pushdown;
mod logs_scan;
mod logs_schema;
mod logs_udf;
mod map_field_planner;
mod memory;
mod metadata_agg;
mod minmax;
mod output;
mod provider;
mod pushdown;
pub mod redact;
mod rlog_attrs;
mod scan;
mod schema;
mod session;
mod spans_fetcher;
mod spans_provider;
mod spans_pushdown;
mod spans_scan;
mod spans_schema;
mod udf;
mod validate;

pub use alerts_provider::AlertsTableProvider;
pub use alerts_pushdown::{AlertsPushdown, extract_alerts};
pub use alerts_schema::{
    ALERT_COL_ALERT_ID, ALERT_COL_ATTRS, ALERT_COL_GENERATION, ALERT_COL_RULE_ID, ALERT_COL_STATE,
    ALERT_COL_TS, alerts_schema,
};
pub use audit_provider::AuditTableProvider;
pub use audit_pushdown::{AuditPushdown, extract_audit};
pub use audit_schema::{
    AUDIT_COL_ATTRS, AUDIT_COL_BODY, AUDIT_COL_SEVERITY_TEXT, AUDIT_COL_TS, audit_schema,
};
pub use config::{
    DEFAULT_LATE_MATERIALIZATION_EXTRA_COLUMNS, DEFAULT_MAX_QUERY_BYTES, ENV_SPILL_DIR,
    ENV_SPILL_MAX_BYTES, GROUP_VALUES_CEILING_COMPENSATION, GROUP_VALUES_RESIZE_TRANSIENT_FACTOR,
    GROUP_VALUES_UNDERCOUNT_FACTOR, SpillConfig, SqlConfig, compensated_group_values_ceiling,
};
pub use declared::{DeclaredColumn, DeclaredColumnSource, DeclaredType, StaticDeclaredColumns};
#[cfg(feature = "flight-sql")]
pub use distributed::{
    DistributedFlightConfig, DistributedScanExec, FlightWorkerSliceClient, StaticWorkerEndpoints,
    WorkerEndpoints, WorkerSlice, WorkerSliceClient, distributed_samples_plan,
    plan_distributed_slices,
};
#[cfg(feature = "flight-sql")]
pub use distributed_rlog::{
    ALERTS_ORDER_COLS, AUDIT_ORDER_COLS, DistributedSliceScanExec, LOGS_ORDER_COLS,
    distributed_alerts_plan, distributed_audit_plan, distributed_logs_plan, distributed_slice_plan,
    sort_slice_fragment,
};
pub use error::{
    ErrorClass, MSG_CORRUPT, MSG_EXECUTION, MSG_INTERNAL, MSG_PLAN, MSG_SPILL_BUDGET_MARKER,
    MSG_SPILL_DISABLED_MARKER, MSG_SPILL_UNAVAILABLE, MSG_UNAVAILABLE, MSG_UNSATISFIABLE, SqlError,
};
pub use executor::{PinnedQuery, PinnedStream, SqlExecutor, SqlOutcome, SqlRequest, SqlStats};
#[cfg(feature = "flight-sql")]
pub use flight::{
    DEFAULT_GC_PROTECTION_HORIZON, FlightAuth, FlightClock, FlightSqlConfig, RavelFlightSqlService,
};
#[cfg(feature = "flight-sql")]
pub use flight_ticket::{
    FlightTicket, FlightTicketError, MAX_STATEMENT_LEN, SegmentPin, TICKET_KEY_LEN, TicketKey,
    derive_ticket_key,
};
pub use group_keys::{DICTIONARY_GROUP_KEYS_RULE, DictionaryGroupKeysAsViews};
pub use late_materialization::{
    LogsRowFetchExec, ROW_REF_COLUMN, TOPK_LATE_MATERIALIZATION_RULE, TopKLateMaterialization,
};
pub use logs_provider::LogsTableProvider;
pub use logs_pushdown::{LogsPushdown, extract_logs};
pub use logs_scan::LogsScanExec;
pub use logs_schema::{
    FIRST_DECLARED_COL, LOG_COL_ATTRS, LOG_COL_BODY, LOG_COL_FLAGS, LOG_COL_OBSERVED_TS,
    LOG_COL_SEVERITY_NUM, LOG_COL_SEVERITY_TEXT, LOG_COL_SPAN_ID, LOG_COL_TRACE_ID, LOG_COL_TS,
    logs_schema, logs_schema_with_declared,
};
pub use logs_udf::{HAS_WORD_UDF, has_word_udf};
pub use memory::{CeilingBreach, TenantDelegatingPool, TenantMemoryAccountant};
pub use metadata_agg::{METADATA_ONLY_AGGREGATE_RULE, MetadataOnlyAggregate, MetadataOnlyExec};
pub use output::QueryOutput;
pub use provider::RavelTableProvider;
pub use pushdown::Pushdown;
pub use redact::{RedactError, redact};
pub use schema::{internal_schema, public_schema};
pub use session::{
    ADMITTED_SCALARS, ADMITTED_TABLE_FUNCTIONS, ADMITTED_WINDOWS, EXCLUDED_SCALARS,
    EXCLUDED_TABLE_FUNCTIONS, EXCLUDED_WINDOWS, EmptyObjectStoreRegistry, LOGS_TABLE,
    SAMPLES_TABLE, SKIP_PARTIAL_AGGREGATION_PROBE_RATIO, SKIP_PARTIAL_AGGREGATION_PROBE_ROWS,
    SPANS_TABLE, SessionTable, build_session, session_config,
};
pub use spans_fetcher::{SpanFetchError, SpanFetchOutput, SpanRow, SpanSegmentFetcher};
pub use spans_provider::SpansTableProvider;
pub use spans_pushdown::{SpansPushdown, extract_spans};
pub use spans_scan::SpansScanExec;
pub use spans_schema::{
    SPAN_COL_ATTRS, SPAN_COL_DURATION_NS, SPAN_COL_END_TS, SPAN_COL_NAME, SPAN_COL_PARENT_SPAN_ID,
    SPAN_COL_SERVICE_NAME, SPAN_COL_SPAN_ID, SPAN_COL_START_TS, SPAN_COL_STATUS_CODE,
    SPAN_COL_STATUS_MESSAGE, SPAN_COL_TRACE_ID, spans_schema,
};
pub use udf::{label_match_udf, label_udf};
pub use validate::{ValidationError, validate};

/// The internal provenance column names, in scan-output order after the
/// four public columns. Consumed by [`dedup::RsegDedupExec`] and dropped
/// before any public operator sees them.
pub use schema::{
    COL_CREATED_UNIX_NS, COL_IN_PAGE_INDEX, COL_LABELS, COL_SERIES_ID, COL_TS, COL_VALUE,
    COL_WRITER_EPOCH, COL_WRITER_SEQ,
};
