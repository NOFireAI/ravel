//! The D1 port trait `ravel-server` implements in #1381, mirroring
//! `services/ravel-server`'s `QueryService` operations.
//!
//! `QueryService` is a concrete struct there, not a trait, and two of the
//! operations it serves (`analytics`, `exemplars`) take request/outcome types
//! (`AnalyticsRequest`, `AnalyticsOutcome`, `ExemplarsRequest`,
//! `ExemplarsOutcome`) defined in that same crate. `ravel-mcp` cannot import
//! any of the four: #1381 makes `ravel-server` depend on `ravel-mcp`, so the
//! reverse import would cycle. [`QueryBackend`] is the port side of that
//! adapter pair instead: one trait of eleven methods, each mirroring the
//! `QueryService` operation its doc names, that #1381 implements for the
//! real `QueryService`. Seven of the eleven take the exact request type
//! `QueryService` takes, from `ravel_query` (`InstantRequest`,
//! `RangeRequest`, `MetadataRequest`) or `ravel_sql` (`SqlRequest`), both
//! existing dependencies of this crate. `analytics` and `exemplars` take
//! [`crate::envelope::AnyJson`] instead, since their concrete request shape
//! is the one unreachable pair; #1381's adapter is where that JSON becomes a
//! real `AnalyticsRequest`/`ExemplarsRequest`. `trace_lookup` and
//! `describe_data` take this crate's own [`TraceLookupRequest`] and
//! [`DescribeRequest`].
//!
//! Every method but three returns the D4 envelope directly (`Result<Envelope,
//! Failure>`) rather than each operation's native outcome type. Turning a
//! native outcome into an envelope is exactly what every tool body (#1380,
//! #1382) needs regardless of which operation answered it, so returning each
//! operation's own outcome type here would only move that same conversion to a
//! later, harder-to-name place; the port trait states the shape the tool
//! layer actually consumes. Two of the exceptions are SQL's: `sql_execute`
//! returns a [`Draft`], an envelope no cap has run on beside the inputs a D5
//! cursor pins, because only the tool can mint that cursor and so only the
//! tool can finish the page; and `sql_explain` returns an [`ExplainOutcome`],
//! the estimate and schema in this crate's own types, which the explain tool
//! renders against its D6 comparison. The third is `describe_data`, which
//! returns a [`DescribeOutcome`]: `ravel_describe_data` pages with a cursor
//! too, and what a page holds depends on whether it is the first one, which
//! only the tool knows.
//!
//! No method has a body: this module only names the shape #1381's adapter
//! must implement and #1380/#1382's tool bodies must call.

use ravel_query::erasure::ErasurePredicate;
use ravel_query::http::{InstantRequest, MetadataRequest, RangeRequest};
use ravel_types::accounting::QueryAccountingSnapshot;
use ravel_types::{Signal, TenantHash};

use crate::draft::{Draft, ExplainOutcome};
use crate::envelope::{AnyJson, Envelope, Failure};

/// One `ravel_describe_data` page request, validated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeRequest {
    /// `Metrics`, `Logs` or `Spans`; the tool refuses every other signal
    /// before calling.
    pub signal: Signal,
    /// The keyset position: families whose names sort after this one in
    /// byte order. `None` starts at the first family.
    pub after_name: Option<String>,
    /// Families per page.
    pub limit: usize,
    /// The instant the catalog resolve runs at: the call's clock on page one,
    /// the cursor's mint instant on a later page.
    pub resolve_at_ns: i64,
    /// The call's own clock, on every page: the declared-column and key-index
    /// cache lookups and the audit record use it, never the resolve instant.
    pub caller_now_ns: i64,
}

/// One column of a described table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedColumn {
    pub name: String,
    /// The Arrow type, in Arrow's display form.
    pub data_type: String,
    pub nullable: bool,
}

/// One metric family from the tenant's metadata record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedFamily {
    pub name: String,
    /// `counter`, `gauge`, `histogram`, `summary` or `unknown`.
    pub kind: String,
    pub unit: String,
    pub help: String,
}

/// What one describe call observed: one signal's shape, one page of its
/// families, and the resolve inputs a D5 cursor pins.
#[derive(Debug, Clone)]
pub struct DescribeOutcome {
    /// The SQL table the columns belong to.
    pub table: String,
    /// The table's fixed columns, in schema order.
    pub fixed_columns: Vec<DescribedColumn>,
    /// The tenant's declared typed attribute columns, in declaration order.
    pub declared_columns: Vec<DescribedColumn>,
    /// The same declared set in the form a cursor pins and
    /// [`crate::cursor::PendingRedemption::confirm`] compares.
    pub declared_column_set: Vec<ravel_sql::DeclaredColumn>,
    pub indexed_keys: Vec<String>,
    /// Where the indexed keys come from: `tenant_key_index`, `span_format`
    /// or `none`.
    pub indexed_keys_source: String,
    /// One page of families, in byte order of name.
    pub families: Vec<DescribedFamily>,
    /// Every family the metadata record held when this call read it.
    pub families_total: usize,
    /// Whether the record holds a family after this page's last one, from
    /// the same read as the page.
    pub more_families: bool,
    /// Why the family page is empty by construction (non-metrics signals).
    pub families_absent_reason: Option<String>,
    /// The greatest `ingest_hour_bucket` among the resolved segments, in unix
    /// hours; `None` when the resolve found no segment.
    pub ingest_watermark_hour: Option<u32>,
    /// The resolved segments' event-time span; `None` with no segment.
    pub coverage: Option<DescribedCoverage>,
    /// Segments the resolve returned.
    pub segments: usize,
    /// Sum of the resolved segments' commit-record row counts.
    pub committed_rows: u64,
    /// The pending erasure predicates the resolve listed for the signal.
    pub pending_erasure: Vec<ErasurePredicate>,
    /// The instant the snapshot was resolved at.
    pub resolved_at_ns: i64,
    /// The resolve's requests and bytes. The metadata record and tenant
    /// configuration reads are not in it.
    pub accounting: QueryAccountingSnapshot,
}

/// The event-time span of a resolve's segments, from their commit records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DescribedCoverage {
    pub min_event_ts_ns: i64,
    pub max_event_ts_ns: i64,
}

/// One `ravel_get_trace` lookup, validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceLookupRequest {
    pub trace_id: [u8; 16],
    /// The caller's window, kept exactly. `None` is the no-window lookup,
    /// which reads every span the tenant retains.
    pub window: Option<ravel_types::TimeRange>,
}

/// The eleven operations the tool layer calls, each mirroring the
/// `QueryService` operation its doc names.
///
/// The lint allow is intentional: this trait is a crate-internal seam with
/// exactly one production implementer (#1381's adapter over the real
/// `QueryService`), called directly and never behind `dyn`, so it needs no
/// `Send`-bounded return future -- the only thing `async_fn_in_trait` warns
/// about. `crates/ravel-maintain/src/codec.rs`'s `SegmentCodec` documents the
/// same reasoning for the same shape of trait.
#[allow(async_fn_in_trait)]
pub trait QueryBackend {
    /// Mirrors `QueryService::promql_instant`.
    async fn promql_instant(
        &self,
        tenant_hash: TenantHash,
        request: &InstantRequest,
    ) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::promql_range`.
    async fn promql_range(
        &self,
        tenant_hash: TenantHash,
        request: &RangeRequest,
    ) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::labels`. The envelope stops before the row and
    /// byte caps: the tool filters the list and then caps it.
    async fn labels(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
    ) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::label_values`. Stops before the caps, as
    /// [`QueryBackend::labels`] does.
    async fn label_values(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
        name: &str,
        include_log_metric_names: bool,
    ) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::series`.
    async fn series(
        &self,
        tenant_hash: TenantHash,
        request: &MetadataRequest,
    ) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::analytics`. Its native `AnalyticsRequest` is
    /// defined in `services/ravel-server`, unreachable from this crate (see
    /// the module doc); the caller-defined shape crosses as opaque JSON
    /// instead, for #1381's adapter to convert.
    async fn analytics(
        &self,
        tenant_hash: TenantHash,
        request: &AnyJson,
    ) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::exemplars`. Same reachability note as
    /// `analytics`.
    async fn exemplars(
        &self,
        tenant_hash: TenantHash,
        request: &AnyJson,
    ) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::sql_execute_at`, stopping before the result is
    /// capped: the tool finishes the [`Draft`] with [`Draft::finish_page`].
    ///
    /// The catalog resolve runs at `resolve_at_ns`: the call's clock on page
    /// one, the pending redemption's resolve instant on a later page. The
    /// request's `now_ns` is the call's own clock on every page: the
    /// declared-column and key-index lookups, the tenant's last-touch and the
    /// audit record use it. The draft's `resolve_at_ns` is `resolve_at_ns`.
    ///
    /// `order_columns` names the result columns a keyset position is read
    /// from. When it is not empty, the draft's `order_tuples` holds each
    /// row's typed values of those columns, read from the result's arrays
    /// with `ravel_sql::resume_value`, so a cursor never reparses a rendered
    /// cell.
    async fn sql_execute(
        &self,
        tenant_hash: TenantHash,
        request: &ravel_sql::SqlRequest,
        resolve_at_ns: i64,
        order_columns: &[String],
    ) -> Result<Draft, Failure>;

    /// Mirrors `QueryService::trace_request` followed by
    /// `QueryService::sql_execute`: the statement and window rule of
    /// `GET /api/traces/{trace_id}`, run through the same SQL execution path.
    /// It stops before the D6 caps: the envelope carries every row the
    /// executor returned, with its own row cap reported in
    /// `presentation.row_cap_hit`, and `ravel_get_trace` caps and fits it.
    async fn trace_lookup(
        &self,
        tenant_hash: TenantHash,
        request: &TraceLookupRequest,
    ) -> Result<Envelope, Failure>;

    /// Mirrors `QueryService::sql_explain`.
    async fn sql_explain(
        &self,
        tenant_hash: TenantHash,
        request: &ravel_sql::SqlRequest,
    ) -> Result<ExplainOutcome, Failure>;

    /// Mirrors `QueryService::describe_data`: one catalog resolve of the
    /// whole signal at `request.resolve_at_ns` and one page of the metadata
    /// record.
    async fn describe_data(
        &self,
        tenant_hash: TenantHash,
        request: &DescribeRequest,
    ) -> Result<DescribeOutcome, Failure>;

    /// What remains of the one deadline that bounds every call this backend
    /// makes for a tool call, or `None` when each call gets the whole
    /// deadline from its own start. A tool reads it before an optional
    /// second call, to skip that call rather than start it with nothing left.
    fn time_left(&self) -> Option<std::time::Duration> {
        None
    }
}
