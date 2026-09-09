//! `SpansTableProvider`: the `spans` table over one query's already-resolved,
//! owned `Snapshot` for `Signal::Spans` (ADR-0041, phase 5). The span-signal
//! sibling of [`crate::logs_provider::LogsTableProvider`].
//!
//! Like the logs provider, this takes an owned, already-resolved `Snapshot`
//! (resolution is the endpoint's job, a follow-up) and a [`SpanSegmentFetcher`],
//! and never resolves. `scan` extracts widen-only pushdown from the filters
//! (crate::spans_pushdown), prunes the snapshot's segments by ts overlap
//! against the extracted window, compiles the pushdown into one
//! [`ravel_rspan::SpanQuery`] (the trace fast path when a `trace_id =` equality
//! was pushed, else a bare window scan) plus the optional `service_name`/`name`
//! bloom-probe literals (ADR-0054, per-block bloom pruning), and builds a single
//! [`SpansScanExec`] leaf.
//!
//! `supports_filters_pushdown` returns `Inexact` for every filter, exactly like
//! the logs provider: DataFusion always re-applies the originals above the
//! scan, so pruning may only widen. The trace_id fast path and the ts window
//! are both re-checked exactly by the reader, so the pushed prune is sound.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use ravel_catalog::{SegmentRef, Snapshot};
#[cfg(feature = "flight-sql")]
use ravel_query::ByteLimit;
use ravel_query::erasure::{ErasurePredicate, snapshot_pending_erasure_predicates};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

#[cfg(feature = "flight-sql")]
use crate::distributed::{WorkerSlice, WorkerSliceClient};
#[cfg(feature = "flight-sql")]
use crate::distributed_rlog::{
    DistributedRlogContext, SPAN_ORDER_COLS, distributed_spans_plan, sort_slice_fragment,
};
use crate::spans_fetcher::SpanSegmentFetcher;
use crate::spans_pushdown::{SpansPushdown, extract_spans};
use crate::spans_scan::{SpansScanExec, columnar_static_eligible};
use crate::spans_schema::spans_schema;

/// The `spans` table provider for one tenant over one pinned `Signal::Spans`
/// snapshot.
pub struct SpansTableProvider {
    snapshot: Arc<Snapshot>,
    tenant_hash: TenantHash,
    fetcher: SpanSegmentFetcher,
    schema: SchemaRef,
    /// This query's accounting handle (ADR-0044), cloned into every
    /// `SpansScanExec` the provider builds so every store fetch the scan
    /// issues on this query's behalf is recorded against it. Mirrors
    /// [`crate::logs_provider::LogsTableProvider`].
    accounting: QueryAccounting,
    /// Pending selective-erasure predicates derived once from
    /// `snapshot.pending_erasure` (ADR-0064 decision 2), cloned
    /// into every `SpansScanExec` the provider builds.
    erasure: Arc<Vec<ErasurePredicate>>,
    /// The coordinator-side distributed fan-out (ADR-0071; #327, T6b). `None` --
    /// the default, and every non-Flight build -- is the local scan path
    /// unchanged. Reuses the signal-neutral [`DistributedRlogContext`], which
    /// carries no schema or key, exactly as the RLOG providers do.
    #[cfg(feature = "flight-sql")]
    distributed: Option<DistributedRlogContext>,
}

impl SpansTableProvider {
    /// Build a provider around an owned, already-resolved `Signal::Spans`
    /// snapshot. Admission and budget config live on the resolve-time seam
    ///, not on the provider, so this no longer takes a
    /// config parameter.
    pub fn new(
        snapshot: Snapshot,
        tenant_hash: TenantHash,
        fetcher: SpanSegmentFetcher,
        accounting: QueryAccounting,
    ) -> Self {
        let erasure = Arc::new(snapshot_pending_erasure_predicates(&snapshot));
        SpansTableProvider {
            snapshot: Arc::new(snapshot),
            tenant_hash,
            fetcher,
            schema: spans_schema(),
            accounting,
            erasure,
            #[cfg(feature = "flight-sql")]
            distributed: None,
        }
    }

    /// Install a coordinator-side distributed scan context (ADR-0071; #327, T6b).
    /// The span-signal sibling of
    /// [`LogsTableProvider::with_distributed_scan`](crate::LogsTableProvider):
    /// [`TableProvider::scan`] fans the `spans` scan out to the given worker
    /// endpoints -- one [`crate::distributed_rlog::DistributedSliceScanExec`]
    /// partition per slice, feeding the no-dedup `SortPreservingMergeExec` --
    /// instead of scanning the local snapshot. Without it, the provider is
    /// unchanged. Only compiled with the Flight transport, which is the only
    /// thing that can carry a slice ticket.
    #[cfg(feature = "flight-sql")]
    pub fn with_distributed_scan(
        mut self,
        endpoints: Vec<WorkerSlice>,
        client: Arc<dyn WorkerSliceClient>,
    ) -> Self {
        self.distributed = Some(DistributedRlogContext { endpoints, client });
        self
    }

    /// The worker-side fragment for a distributed `spans` scan (ADR-0071; #327,
    /// T6b): the whole-snapshot [`SpansScanExec`] (no pushdown) wrapped in a
    /// single globally-sorted partition under the `spans` total-order key
    /// ([`SPAN_ORDER_COLS`]), streamed to the coordinator's no-dedup merge. A
    /// worker executes this over its slice; the coordinator's
    /// `DistributedSliceScanExec` exposes each worker stream as one sorted
    /// partition feeding the SAME no-dedup merge. There is NO dedup, in the
    /// worker or the coordinator: spans have no query-time dedup, so every
    /// fetched span is returned.
    ///
    /// Note the local `SpansScanExec` already declares a `(trace_id, start_ts)`
    /// ordering, but the distributed total order is the full [`SPAN_ORDER_COLS`]
    /// key, so this is a full [`sort_slice_fragment`] `SortExec` (not a
    /// preserving merge over the leaf's weaker order).
    #[cfg(feature = "flight-sql")]
    pub fn worker_fragment(&self, target_partitions: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        let scan = self.build_scan(target_partitions, &SpansPushdown::default(), None)?;
        sort_slice_fragment(scan, &self.schema, SPAN_ORDER_COLS)
    }

    /// Apply projection pushdown (column selection only) above `plan` on the
    /// distributed path, where the fan-out returns the full public schema and
    /// DataFusion asked for a subset. The local path applies the same
    /// `ProjectionExec` inline in [`TableProvider::scan`].
    #[cfg(feature = "flight-sql")]
    fn apply_projection(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        projection: Option<&Vec<usize>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        match projection {
            Some(proj) => {
                let exprs = proj
                    .iter()
                    .map(|&i| {
                        let name = self.schema.field(i).name();
                        Ok((col(name, &self.schema)?, name.to_string()))
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
            }
            None => Ok(plan),
        }
    }

    /// Build the scan over every segment in the snapshot with no pushdown.
    /// Exposed (like the logs provider's `plan`) so tests can execute the scan
    /// without a SQL front-end.
    pub fn plan(&self, target_partitions: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_scan(target_partitions, &SpansPushdown::default(), None)
    }

    /// Build the scan for a set of filters, extracting the pushdown from them.
    /// Exposed so tests exercise the whole `extract_spans` -> prune -> scan
    /// path, and can downcast the result to inspect the issued [`SpanQuery`].
    ///
    /// [`SpanQuery`]: ravel_rspan::SpanQuery
    pub fn plan_filters(
        &self,
        target_partitions: usize,
        filters: &[Expr],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_scan(target_partitions, &extract_spans(filters), None)
    }

    /// Admission (the sealed-segment cap) is decided exactly once, at
    /// resolve time, by whichever endpoint resolves this table's snapshot,
    /// calling `ravel_query::admit` over the full snapshot and its
    /// `SegmentOrigins` -- the same pattern
    /// `SqlExecutor::resolve` uses for the two wired tables. No such endpoint
    /// exists yet for spans (see the module doc); `pruned_segments` below is
    /// a further, client-side, widen-only ts subset, so re-checking a count
    /// against it here would be a second, weaker check over the wrong set;
    /// it is not reimplemented.
    fn build_scan(
        &self,
        target_partitions: usize,
        pushdown: &SpansPushdown,
        projection: Option<Vec<usize>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let segments = self.pruned_segments(pushdown);
        let scan = SpansScanExec::new(
            self.tenant_hash,
            self.fetcher.clone(),
            &segments,
            target_partitions,
            pushdown.span_query(),
            pushdown.service_name.clone(),
            pushdown.name.clone(),
            pushdown.duration_window(),
            pushdown.status_mask,
            Arc::clone(&self.erasure),
            projection,
            self.accounting.clone(),
        )?;
        Ok(Arc::new(scan))
    }

    /// Segments whose event-time span overlaps the extracted ts window.
    /// Widen-only: a segment is dropped only when its whole span lies outside
    /// the proven-required window (via [`SpanSegmentFetcher::ts_range_relevant`],
    /// the same catalog-summary check `fetch` uses); with no bound, every
    /// segment is kept. Finer pruning -- by trace_id range and per-block time
    /// interval -- is the reader's skip index, at block granularity inside each
    /// object, so it is not (and need not be) reproduced here.
    fn pruned_segments(&self, pushdown: &SpansPushdown) -> Vec<SegmentRef> {
        let (ts_min, ts_max) = (pushdown.ts_min(), pushdown.ts_max());
        self.snapshot
            .segments
            .iter()
            .filter(|s| SpanSegmentFetcher::ts_range_relevant(s, ts_min, ts_max))
            .cloned()
            .collect()
    }
}

impl fmt::Debug for SpansTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("SpansTableProvider")
            .field("segments", &self.snapshot.segments.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for SpansTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Inexact for every filter: the provider prunes what it can (widen-only),
    /// but DataFusion must always re-apply the original filters above the scan.
    /// Never `Exact` (carried over from the logs/metrics providers).
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Distributed coordinator path (ADR-0071; #327, T6b): fan out to worker
        // slices instead of scanning locally, folding worker bytes into the query
        // accounting under an `Unlimited` ceiling (no per-query byte budget on the
        // spans provider). See the `logs` provider for the full rationale: the
        // scan's `_limit` is a fetch-stop hint across partitions with the exact
        // limit re-applied above the merge, and filters are re-applied above the
        // returned plan by DataFusion (the provider reports `Inexact`).
        #[cfg(feature = "flight-sql")]
        if let Some(dist) = &self.distributed {
            let plan = distributed_spans_plan(
                dist.endpoints.clone(),
                Arc::clone(&dist.client),
                Arc::clone(&self.schema),
                _limit,
                self.accounting.clone(),
                ByteLimit::Unlimited,
            )?;
            return self.apply_projection(plan, projection);
        }

        let target_partitions = state.config().target_partitions();
        let pushdown = extract_spans(filters);

        // Eligible fast path (ADR-0110 decision 4): the projection excludes the
        // `attrs` map and no pending erasure predicate applies. Push the
        // projection into the scan, which emits the projected schema directly
        // and decodes only the pages that schema needs; add NO `ProjectionExec`.
        if columnar_static_eligible(projection, &self.erasure) {
            return self.build_scan(target_partitions, &pushdown, projection.cloned());
        }

        // Ineligible path, unchanged: the scan emits the full eleven-column
        // schema and, for column selection, this `ProjectionExec` drops the
        // rest. The fetch reads the whole object regardless (an `attrs`
        // projection or a pending erasure predicate forces the row path, which
        // has no per-column page toggle), matching the logs/metrics providers.
        let plan = self.build_scan(target_partitions, &pushdown, None)?;
        match projection {
            Some(proj) => {
                let exprs = proj
                    .iter()
                    .map(|&i| {
                        let name = self.schema.field(i).name();
                        Ok((col(name, &self.schema)?, name.to_string()))
                    })
                    .collect::<DFResult<Vec<_>>>()?;
                Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
            }
            None => Ok(plan),
        }
    }
}
