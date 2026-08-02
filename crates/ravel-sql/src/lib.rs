//! ravel-sql: DataFusion-backed SQL execution over Ravel metric samples.
//!
//! This crate is the structural isolation boundary that keeps `datafusion`
//! (and, transitively, its own `arrow`) out of the PromQL and ingest paths
//! (ADR-0013 / docs/arrow-datafusion-plan.md section 3). Nothing in the
//! ingest-critical or PromQL crates links this crate.
//!
//! Ticket B1 (issue #20) implements the read pipeline skeleton described in
//! docs/arrow-datafusion-plan.md section 2, redesigned per the F4/F5/F6/F8/
//! F11/F12 findings in docs/reviews/2026-07-27-arrow-datafusion-plan-review.md:
//!
//! ```text
//! RsegScanExec (N partitions, each sorted by (series_id, ts, provenance))
//!   -> SortPreservingMergeExec on (series_id, ts)
//!   -> RsegDedupExec (single partition, streaming, full dedup total order)
//!   -> DataFusion operators (later tickets)
//! ```
//!
//! Arrow types are used exclusively through the `datafusion::arrow`
//! re-export so this crate is internally version-consistent regardless of
//! the workspace `arrow` pin.

//! Ticket B3 (issue #22) adds the request-handling half: the read-only
//! single-statement gate (`validate`), the fresh per-query single-tenant
//! session (`session`), the resolve/plan/execute driver with the snapshot
//! retry contract (`executor`), the two wire encodings (`output`), and the
//! error-to-client redaction boundary (`error`). The HTTP surface itself
//! lives in services/ravel-server behind its `sql` feature; nothing here
//! links axum, and nothing there links datafusion.

//! Ticket C1d (issue #152) adds the second transport behind the `flight-sql`
//! feature: `flight` is the `FlightSqlService` implementation and
//! `flight_ticket` its snapshot-pinning ticket codec. It is a transport and
//! nothing more -- it validates, resolves, plans, and executes through the
//! same `SqlExecutor` the HTTP path uses, so the two cannot answer the same
//! query differently. Its own additions are the ones the two-RPC shape forces:
//! pinning the resolved snapshot into the ticket so `DoGet` never re-resolves
//! (review F18), and checking the metadata-resolved tenant against the
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
mod dedup;
mod error;
mod executor;
#[cfg(feature = "flight-sql")]
pub mod flight;
#[cfg(feature = "flight-sql")]
mod flight_ticket;
mod labels;
mod logs_provider;
mod logs_pushdown;
mod logs_scan;
mod logs_schema;
mod logs_udf;
mod memory;
mod minmax;
mod output;
mod provider;
mod pushdown;
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
pub use config::{DEFAULT_MAX_QUERY_BYTES, SqlConfig};
pub use error::{
    ErrorClass, MSG_CORRUPT, MSG_EXECUTION, MSG_INTERNAL, MSG_PLAN, MSG_UNAVAILABLE,
    MSG_UNSATISFIABLE, SqlError,
};
pub use executor::{PinnedQuery, PinnedStream, SqlExecutor, SqlOutcome, SqlRequest, SqlStats};
#[cfg(feature = "flight-sql")]
pub use flight::{
    DEFAULT_GC_PROTECTION_HORIZON, FlightAuth, FlightClock, FlightSqlConfig, RavelFlightSqlService,
};
#[cfg(feature = "flight-sql")]
pub use flight_ticket::{
    FlightTicket, FlightTicketError, MAX_STATEMENT_LEN, SegmentPin, TICKET_KEY_LEN, TicketKey,
};
pub use logs_provider::LogsTableProvider;
pub use logs_pushdown::{LogsPushdown, extract_logs};
pub use logs_scan::LogsScanExec;
pub use logs_schema::{
    LOG_COL_ATTRS, LOG_COL_BODY, LOG_COL_FLAGS, LOG_COL_OBSERVED_TS, LOG_COL_SEVERITY_NUM,
    LOG_COL_SEVERITY_TEXT, LOG_COL_SPAN_ID, LOG_COL_TRACE_ID, LOG_COL_TS, logs_schema,
};
pub use logs_udf::{HAS_WORD_UDF, has_word_udf};
pub use memory::{CeilingBreach, TenantDelegatingPool, TenantMemoryAccountant};
pub use output::QueryOutput;
pub use provider::RavelTableProvider;
pub use pushdown::Pushdown;
pub use schema::{internal_schema, public_schema};
pub use session::{
    EmptyObjectStoreRegistry, LOGS_TABLE, SAMPLES_TABLE, SPANS_TABLE, SessionTable, build_session,
    session_config,
};
pub use spans_fetcher::{SpanFetchError, SpanFetchOutput, SpanSegmentFetcher};
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
