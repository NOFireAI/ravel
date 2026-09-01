//! SQL-lane distributed read fan-out for the RLOG family: logs, alerts, and
//! audit (ADR-0071; #326, the SQL half of #63's T6). The signal sibling of
//! [`crate::distributed`], which owns the metrics lane.
//!
//! # What is signal-generic here, and what is not
//!
//! Everything in this module except the three per-signal total-order keys is
//! signal-neutral. [`DistributedSliceScanExec`] takes a [`SchemaRef`] and an
//! ordering column list and knows nothing about logs, alerts, or audit;
//! [`distributed_slice_plan`] assembles the fan-out around any schema and key.
//! The `spans` table (#327, T6b) reuses both verbatim by passing the spans
//! schema and the span total-order key -- it does not reimplement the scan exec,
//! the merge assembly, the byte accounting, or the schema validation. The only
//! signal-specific surface is
//! [`LOGS_ORDER_COLS`]/[`ALERTS_ORDER_COLS`]/[`AUDIT_ORDER_COLS`]/[`SPAN_ORDER_COLS`]
//! and the thin wrappers ([`distributed_logs_plan`] and siblings, plus
//! [`distributed_spans_plan`]) that bind a signal's public schema to its key.
//!
//! The slice-ticket plumbing is not reimplemented here at all: the fan-out
//! reuses [`crate::distributed`]'s [`WorkerSlice`], [`WorkerSliceClient`],
//! [`WorkerEndpoints`], [`DistributedFlightConfig`], and
//! [`plan_distributed_slices`](crate::distributed::plan_distributed_slices)
//! unchanged. Those are already signal-agnostic: a slice ticket pins a subset
//! of a resolved snapshot and carries no signal discriminator, so the same
//! minting/redeeming path serves every table.
//!
//! # The merge rule this reproduces: total order, NO dedup
//!
//! This is where the correctness rule differs from the metrics lane, and it is
//! the whole point of the split. The metrics coordinator runs
//! `SortPreservingMergeExec -> RsegDedupExec`: a `(series_id, ts)` sample can be
//! written through segments in different shards, and the greatest-wins winner
//! is only decidable once every slice's candidates meet, so the coordinator's
//! dedup is authoritative.
//!
//! Logs, alerts, audit, and spans have **no query-time dedup**
//! (`docs/consistency-model.md`, "logs and spans"; ADR-0051 section 5). A retry
//! after a lost acknowledgement produces byte-identical rows that are
//! legitimately duplicate user data and MUST stay visible. So the merge is
//! a `SortPreservingMergeExec` under a total-order key and **nothing above it**:
//! no dedup node, no `.dedup_by`, no distinct. Every row every slice returns is
//! emitted. This mirrors [`ravel_query::distrib`]'s `merge_log_records`, which
//! flattens the per-slice pool and stable-sorts under a total key without ever
//! collapsing an equal-key pair.
//!
//! Because there is no dedup, the multiset of rows is preserved under *any*
//! union of the slice partitions; the sort exists to reproduce a deterministic,
//! local-equivalent ordering (and to make the reshard-straddle differential
//! test non-vacuous: a slice-order concatenation is observably wrong once one
//! stream's segments straddle two slices, an order the sorted merge never
//! produces). The key is the orderable public columns in a fixed sequence; the
//! `attrs` `Map` column is deliberately excluded because Arrow maps are not
//! orderable, which is sound precisely because there is no dedup -- two rows
//! that tie on every orderable column are indistinguishable to SQL, so their
//! relative order never matters.
//!
//! # Reachability
//!
//! Like the metrics lane today, nothing in the shipping binary reaches this
//! yet: an RLOG distributed scan is installed on a provider via
//! [`LogsTableProvider::with_distributed_scan`](crate::LogsTableProvider) (and
//! the alerts/audit siblings) and is exercised end to end by the acceptance
//! tests (`tests/flight_distributed.rs`) driving `provider.scan(..)`. The
//! server-side coordinator that installs the context from
//! `get_flight_info_statement`, and the worker `do_get` slice-fragment branch
//! that runs [`LogsTableProvider::worker_fragment`], are later wiring (see the
//! task report).

use std::fmt;
use std::sync::Arc;

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::limit::GlobalLimitExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::StreamExt;
use ravel_query::ByteLimit;
use ravel_types::accounting::{AccountedOp, QueryAccounting};

use crate::distributed::{WorkerSlice, WorkerSliceClient};
use crate::error::SqlError;

/// The `logs` table's total-order key: the orderable fixed public columns in a
/// fixed sequence (ADR-0033 schema order). `attrs` is excluded (Arrow maps are
/// not orderable); declared typed columns are excluded because they are a
/// per-tenant tail that need not participate for the order to be deterministic
/// on the fixed columns. `ts` first mirrors the local read's natural order.
pub const LOGS_ORDER_COLS: &[&str] = &[
    "ts",
    "observed_ts",
    "severity_num",
    "severity_text",
    "body",
    "trace_id",
    "span_id",
    "flags",
];

/// The `alerts` table's total-order key: the orderable public columns in schema
/// order (ADR-0040). `attrs` excluded, as above.
pub const ALERTS_ORDER_COLS: &[&str] = &["ts_ns", "alert_id", "rule_id", "state", "generation"];

/// The `audit` table's total-order key: the orderable public columns in schema
/// order (ADR-0040). `attrs` excluded, as above.
pub const AUDIT_ORDER_COLS: &[&str] = &["ts_ns", "severity_text", "body"];

/// The `spans` table's total-order key (ADR-0041): the orderable public columns
/// in the exact field sequence `ravel_query::distrib`'s `span_cmp`/
/// `span_order_key` use -- `(trace_id, span_id, start_ts_ns, end_ts_ns,
/// parent_span_id, name, status_code, status_message, service_name)` -- mapped
/// to the public `spans` schema column names (`start_ts`/`end_ts` for the two
/// timestamp columns). The trailing `attrs` `Map` column is excluded because
/// Arrow maps are not orderable; this is sound because spans have no query-time
/// dedup, so two rows tying on every orderable column are indistinguishable to
/// SQL and their relative order never matters (the same rationale that excludes
/// `attrs` from the RLOG keys above and from the queryfrag-layer span key).
pub const SPAN_ORDER_COLS: &[&str] = &[
    "trace_id",
    "span_id",
    "start_ts",
    "end_ts",
    "parent_span_id",
    "name",
    "status_code",
    "status_message",
    "service_name",
];

/// A coordinator-side distributed RLOG scan context (ADR-0071): the worker
/// endpoints one query fans out to, and the client that fetches each slice.
/// Held in an `Option` on each RLOG provider; installed via that provider's
/// `with_distributed_scan`, absent (the default) means the local scan path.
///
/// Signal-neutral by construction -- it carries no schema or key -- so the
/// three RLOG providers (and, later, the spans provider) share it unchanged.
pub(crate) struct DistributedRlogContext {
    pub(crate) endpoints: Vec<WorkerSlice>,
    pub(crate) client: Arc<dyn WorkerSliceClient>,
}

/// Build a lexicographic ascending ordering over `cols`, resolved against
/// `schema`. `nulls_first: false` matches every RLOG scan's declared ordering
/// and the metrics lane, so a nullable promoted column (an absent `alert_id`)
/// sorts last rather than first.
fn lex_ordering(schema: &SchemaRef, cols: &[&str]) -> DFResult<LexOrdering> {
    let asc = SortOptions {
        descending: false,
        nulls_first: false,
    };
    let exprs = cols
        .iter()
        .map(|name| Ok(PhysicalSortExpr::new(col(name, schema)?, asc)))
        .collect::<DFResult<Vec<_>>>()?;
    LexOrdering::new(exprs)
        .ok_or_else(|| DataFusionError::Internal("empty distributed RLOG ordering".into()))
}

/// Wrap a signal's local (multi-partition, unordered) scan in a single
/// globally-sorted partition under `ordering_cols`: the shape a worker streams
/// back for the coordinator's [`SortPreservingMergeExec`] to k-way merge.
///
/// `SortExec` with the default `preserve_partitioning = false` sorts and
/// coalesces every input partition into one, so the result is one partition
/// sorted by the signal's total-order key -- exactly what
/// [`DistributedSliceScanExec`] declares each worker partition to be. This is
/// the RLOG analog of the metrics worker fragment's
/// `RsegScanExec -> SortPreservingMergeExec`, but a full `SortExec` rather than
/// a merge because the RLOG scans declare no leaf ordering (ADR-0087) for the
/// merge to preserve.
pub fn sort_slice_fragment(
    scan: Arc<dyn ExecutionPlan>,
    schema: &SchemaRef,
    ordering_cols: &[&str],
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let ordering = lex_ordering(schema, ordering_cols)?;
    Ok(Arc::new(SortExec::new(ordering, scan)))
}

/// The leaf of an RLOG family distributed pipeline: one DataFusion partition per
/// worker endpoint, each partition the worker's public-schema slice stream,
/// declared sorted by the signal's total-order key.
///
/// Signal-generic: it holds a [`SchemaRef`] and the ordering column names, so it
/// is the same struct for logs, alerts, audit, and (later) spans. It mirrors the
/// metrics [`crate::distributed::DistributedScanExec`] -- the same per-slice
/// byte accounting, the same declared-ordering trick that keeps the optimizer
/// from striking the merge above it -- but carries the public signal schema
/// rather than the metrics internal provenance schema, and never feeds a dedup.
pub struct DistributedSliceScanExec {
    endpoints: Vec<WorkerSlice>,
    client: Arc<dyn WorkerSliceClient>,
    /// Best-effort fetch-stop hint threaded to each worker (ADR-0071). Any exact
    /// limit is re-applied above the merge by [`distributed_slice_plan`], so this
    /// may over-fetch but never causes under-fetch.
    limit: Option<usize>,
    /// The coordinator's byte-accounting handle (ADR-0044, ADR-0061 decision 1):
    /// each worker batch is folded in by `get_array_memory_size`, exactly as the
    /// metrics lane folds its worker batches.
    accounting: QueryAccounting,
    /// The per-tenant bytes-scanned budget. `Unlimited` (the RLOG default, since
    /// these providers carry no per-query byte budget) never trips and still
    /// folds the bytes.
    max_bytes_scanned: ByteLimit,
    /// The signal's public output schema, validated against every worker batch.
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl DistributedSliceScanExec {
    /// Build a scan over one worker endpoint per slice, declaring each partition
    /// sorted by `ordering_cols` over `schema`. `endpoints` must be non-empty: a
    /// distributed plan with no worker is a caller bug, not a silently-empty
    /// result.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        endpoints: Vec<WorkerSlice>,
        client: Arc<dyn WorkerSliceClient>,
        schema: SchemaRef,
        ordering_cols: &[&str],
        limit: Option<usize>,
        accounting: QueryAccounting,
        max_bytes_scanned: ByteLimit,
    ) -> DFResult<Self> {
        if endpoints.is_empty() {
            return Err(DataFusionError::Internal(
                "DistributedSliceScanExec needs at least one worker endpoint".into(),
            ));
        }
        let properties = Arc::new(Self::compute_properties(
            &schema,
            ordering_cols,
            endpoints.len(),
        )?);
        Ok(DistributedSliceScanExec {
            endpoints,
            client,
            limit,
            accounting,
            max_bytes_scanned,
            schema,
            properties,
        })
    }

    /// Declare the signal's total-order ordering per partition. Declaring it is
    /// what lets the [`SortPreservingMergeExec`] above merge on it rather than
    /// re-sort, and (like the metrics dedup's required-ordering) keeps the
    /// optimizer from striking the merge when the plan is optimized rather than
    /// executed directly.
    fn compute_properties(
        schema: &SchemaRef,
        ordering_cols: &[&str],
        n: usize,
    ) -> DFResult<PlanProperties> {
        let ordering = lex_ordering(schema, ordering_cols)?;
        let eq = EquivalenceProperties::new_with_orderings(Arc::clone(schema), vec![ordering]);
        Ok(PlanProperties::new(
            eq,
            Partitioning::UnknownPartitioning(n),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ))
    }
}

impl fmt::Debug for DistributedSliceScanExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedSliceScanExec {{ endpoints: {} }}",
            self.endpoints.len()
        )
    }
}

impl DisplayAs for DistributedSliceScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "DistributedSliceScanExec: endpoints={}, limit={:?}",
            self.endpoints.len(),
            self.limit
        )
    }
}

impl ExecutionPlan for DistributedSliceScanExec {
    fn name(&self) -> &str {
        "DistributedSliceScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let endpoint = self.endpoints.get(partition).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "DistributedSliceScanExec: partition {partition} out of range"
            ))
        })?;
        let inner = self
            .client
            .fetch_slice(&endpoint.location, &endpoint.ticket, self.limit)?;

        // Validate every worker batch against this exec's declared schema, then
        // fold its bytes into the coordinator's accounting and enforce the
        // per-tenant budget, exactly as the metrics lane does. A version-skewed
        // or buggy worker that streams a different layout must fail typed here,
        // not feed mismatched columns into the merge's key resolution downstream.
        let expected = Arc::clone(&self.schema);
        let accounting = self.accounting.clone();
        let max_bytes_scanned = self.max_bytes_scanned;
        let folded = inner.map(move |item| {
            let batch = item?;
            validate_schema(batch.schema().as_ref(), expected.as_ref())?;
            let bytes = batch.get_array_memory_size() as u64;
            accounting.add_s3_bytes(AccountedOp::Get, bytes);
            let scanned = accounting.snapshot().total_s3_bytes();
            if max_bytes_scanned.is_exceeded_by(scanned) {
                let max = match max_bytes_scanned {
                    ByteLimit::Bounded(max) => max,
                    ByteLimit::Unlimited => scanned,
                };
                return Err(DataFusionError::from(SqlError::TooManyBytesScanned {
                    scanned,
                    max,
                }));
            }
            Ok(batch)
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            folded,
        )))
    }
}

/// Verify a worker's slice stream carries this scan's declared public schema
/// (ADR-0071). Column count, names, data types, and nullability must match
/// exactly; incidental schema-level metadata Flight encode/decode may carry is
/// not compared. A mismatch is a typed error, never a panic or a wrong result
/// downstream.
fn validate_schema(actual: &Schema, expected: &Schema) -> DFResult<()> {
    let same = actual.fields().len() == expected.fields().len()
        && actual.fields().iter().zip(expected.fields()).all(|(a, e)| {
            a.name() == e.name()
                && a.data_type() == e.data_type()
                && a.is_nullable() == e.is_nullable()
        });
    if same {
        Ok(())
    } else {
        Err(DataFusionError::Internal(format!(
            "worker slice stream schema does not match the expected RLOG scan schema: \
             expected {expected:?}, got {actual:?}"
        )))
    }
}

/// Assemble the signal-generic RLOG distributed pipeline:
///
/// ```text
/// DistributedSliceScanExec (one partition per worker slice, sorted by key)
///   -> SortPreservingMergeExec (key)   [NO dedup]
///   [-> GlobalLimitExec(limit)         when a fetch limit is present]
/// ```
///
/// There is deliberately no dedup node above the merge: logs/alerts/audit have
/// no query-time dedup, so every row every slice returns is emitted (see the
/// module docs). The `SortPreservingMergeExec` reproduces the local read's
/// deterministic total order over `ordering_cols`; when `limit` is `Some`, an
/// exact [`GlobalLimitExec`] sits above the merge so the plan returns exactly
/// `limit` rows without ever under-fetching (the per-worker hint may over-fetch).
///
/// Signal-neutral: pass any table's public schema and total-order key. The three
/// RLOG wrappers below are the only signal-aware callers today; T6b calls it
/// with the spans schema and key.
pub fn distributed_slice_plan(
    endpoints: Vec<WorkerSlice>,
    client: Arc<dyn WorkerSliceClient>,
    schema: SchemaRef,
    ordering_cols: &[&str],
    limit: Option<usize>,
    accounting: QueryAccounting,
    max_bytes_scanned: ByteLimit,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    let scan: Arc<dyn ExecutionPlan> = Arc::new(DistributedSliceScanExec::new(
        endpoints,
        client,
        Arc::clone(&schema),
        ordering_cols,
        limit,
        accounting,
        max_bytes_scanned,
    )?);
    let ordering = lex_ordering(&schema, ordering_cols)?;
    let merge: Arc<dyn ExecutionPlan> = Arc::new(SortPreservingMergeExec::new(ordering, scan));
    match limit {
        Some(fetch) => Ok(Arc::new(GlobalLimitExec::new(merge, 0, Some(fetch)))),
        None => Ok(merge),
    }
}

/// The `logs` distributed plan: [`distributed_slice_plan`] bound to the `logs`
/// public schema and [`LOGS_ORDER_COLS`]. `schema` is the provider's resolved
/// schema (`logs_schema_with_declared`), so a query with declared columns still
/// merges on the fixed key present in every row.
pub fn distributed_logs_plan(
    endpoints: Vec<WorkerSlice>,
    client: Arc<dyn WorkerSliceClient>,
    schema: SchemaRef,
    limit: Option<usize>,
    accounting: QueryAccounting,
    max_bytes_scanned: ByteLimit,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    distributed_slice_plan(
        endpoints,
        client,
        schema,
        LOGS_ORDER_COLS,
        limit,
        accounting,
        max_bytes_scanned,
    )
}

/// The `alerts` distributed plan: [`distributed_slice_plan`] bound to the
/// `alerts` public schema and [`ALERTS_ORDER_COLS`].
pub fn distributed_alerts_plan(
    endpoints: Vec<WorkerSlice>,
    client: Arc<dyn WorkerSliceClient>,
    schema: SchemaRef,
    limit: Option<usize>,
    accounting: QueryAccounting,
    max_bytes_scanned: ByteLimit,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    distributed_slice_plan(
        endpoints,
        client,
        schema,
        ALERTS_ORDER_COLS,
        limit,
        accounting,
        max_bytes_scanned,
    )
}

/// The `audit` distributed plan: [`distributed_slice_plan`] bound to the `audit`
/// public schema and [`AUDIT_ORDER_COLS`].
pub fn distributed_audit_plan(
    endpoints: Vec<WorkerSlice>,
    client: Arc<dyn WorkerSliceClient>,
    schema: SchemaRef,
    limit: Option<usize>,
    accounting: QueryAccounting,
    max_bytes_scanned: ByteLimit,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    distributed_slice_plan(
        endpoints,
        client,
        schema,
        AUDIT_ORDER_COLS,
        limit,
        accounting,
        max_bytes_scanned,
    )
}

/// The `spans` distributed plan (#327, T6b): [`distributed_slice_plan`] bound to
/// the `spans` public schema and [`SPAN_ORDER_COLS`]. Like the RLOG wrappers it
/// merges under the total-order key with NO dedup above it, because spans have
/// no query-time dedup (see the module docs).
pub fn distributed_spans_plan(
    endpoints: Vec<WorkerSlice>,
    client: Arc<dyn WorkerSliceClient>,
    schema: SchemaRef,
    limit: Option<usize>,
    accounting: QueryAccounting,
    max_bytes_scanned: ByteLimit,
) -> DFResult<Arc<dyn ExecutionPlan>> {
    distributed_slice_plan(
        endpoints,
        client,
        schema,
        SPAN_ORDER_COLS,
        limit,
        accounting,
        max_bytes_scanned,
    )
}
