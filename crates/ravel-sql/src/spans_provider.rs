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
//!
//! A `trace_id` lookup over a snapshot resolved with its parts runs the
//! tier-2 router (ADR-3692 decision 3) after the ts prune: a segment whose
//! part's key-index leaves prove the trace absent is dropped before the scan
//! is built, a segment they name blocks for reads exactly those blocks, and a
//! segment with no tier-2 answer is left to its object's own KEY_IDX.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
#[cfg(feature = "flight-sql")]
use datafusion::physical_expr::expressions::col;
use datafusion::physical_plan::ExecutionPlan;
#[cfg(feature = "flight-sql")]
use datafusion::physical_plan::projection::ProjectionExec;
use ravel_catalog::{Catalog, KeyIndexLeafLimits, SegmentRef, Snapshot, SnapshotParts};
use ravel_query::PhaseAccounting;
use ravel_query::erasure::{ErasurePredicate, snapshot_pending_erasure_predicates};
use ravel_query::key_index_route::KeySet;
use ravel_query::key_index_route::{
    BudgetRefused, LeafDecodeBudget, LeafDecodeGuard, RouteFigures, RouteInput, SegmentRoute,
    route, route_with_max_parts,
};
use ravel_query::span_fetcher::{SpanLeafRangeReader, trace_ids_key_set, trace_key_set};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantHash};

#[cfg(feature = "flight-sql")]
use crate::distributed::{WorkerSlice, WorkerSliceClient};
#[cfg(feature = "flight-sql")]
use crate::distributed_rlog::{
    DistributedRlogContext, SPAN_ORDER_COLS, distributed_spans_plan, sort_slice_fragment,
};
#[cfg(feature = "flight-sql")]
use crate::flight_ticket::SlicePushdown;
use crate::scan_budget::ScanBudget;
use crate::spans_fetcher::SpanSegmentFetcher;
use crate::spans_pushdown::{SpansPushdown, extract_spans};
use crate::spans_scan::SpansScanExec;
use crate::spans_schema::spans_schema;

/// The `spans` table provider for one tenant over one pinned `Signal::Spans`
/// snapshot.
pub struct SpansTableProvider {
    snapshot: Arc<Snapshot>,
    tenant_hash: TenantHash,
    fetcher: SpanSegmentFetcher,
    schema: SchemaRef,
    /// This query's accounting handles (ADR-0044), cloned into every
    /// `SpansScanExec` the provider builds so every store fetch the scan
    /// issues on this query's behalf is recorded against the phase that
    /// issued it. Mirrors [`crate::logs_provider::LogsTableProvider`].
    accounting: PhaseAccounting,
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
    /// The statement's byte and store-request budgets (issue #1409), checked
    /// by every `SpansScanExec` this provider builds after each store read and
    /// passed to the distributed fan-out. Unlimited by default, installed with
    /// [`Self::with_scan_budget`].
    scan_budget: ScanBudget,
    /// The tier-2 router's inputs, `None` for a snapshot resolved without its
    /// parts, which leaves every segment to its object's KEY_IDX.
    key_index_route: Option<KeyIndexRouteContext>,
}

/// What the tier-2 router needs beyond the snapshot (ADR-3692 decision 3).
#[derive(Clone)]
struct KeyIndexRouteContext {
    catalog: Arc<Catalog>,
    /// From the same resolve as the provider's snapshot.
    parts: SnapshotParts,
    /// A lowered part ceiling; `None` is the ADR's.
    max_probed_parts: Option<usize>,
}

/// The tier-2 answer for one scan: the segments to read, each one's block set
/// (`None` where tier 1 decides), and the figures for `stats.keyIndex`.
struct RoutedSegments {
    segments: Vec<SegmentRef>,
    block_sets: Vec<Option<Vec<u32>>>,
    figures: RouteFigures,
    pruned: u64,
}

/// A frame decode reservation drawn from the statement's memory pool, so a
/// leaf's decoded bound is counted against the same limit as the scan.
struct PoolLeafBudget(Arc<dyn MemoryPool>);

impl LeafDecodeBudget for PoolLeafBudget {
    fn try_reserve(&self, bytes: u64) -> Result<LeafDecodeGuard, BudgetRefused> {
        let reservation = MemoryConsumer::new("spans key-index leaf").register(&self.0);
        let size = usize::try_from(bytes).map_err(|_| BudgetRefused { bytes })?;
        reservation
            .try_grow(size)
            .map_err(|_| BudgetRefused { bytes })?;
        Ok(LeafDecodeGuard::new(reservation))
    }
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
        let accounting = PhaseAccounting::pooled_over(&accounting);
        let scan_budget = ScanBudget::unlimited(&accounting);
        SpansTableProvider {
            snapshot: Arc::new(snapshot),
            tenant_hash,
            fetcher,
            schema: spans_schema(),
            accounting,
            erasure,
            #[cfg(feature = "flight-sql")]
            distributed: None,
            scan_budget,
            key_index_route: None,
        }
    }

    /// Route `trace_id` lookups through the parts' key-index leaves before the
    /// scan (ADR-3692 decision 3). `parts` must come from the resolve that
    /// produced this provider's snapshot; a mismatch fails the lookup.
    #[must_use]
    pub fn with_key_index_route(mut self, catalog: Arc<Catalog>, parts: SnapshotParts) -> Self {
        self.key_index_route = Some(KeyIndexRouteContext {
            catalog,
            parts,
            max_probed_parts: None,
        });
        self
    }

    /// Lower the router's part ceiling to `max_parts` (never above the ADR's
    /// `P_max`). A no-op without [`Self::with_key_index_route`].
    #[must_use]
    pub fn with_key_index_max_probed_parts(mut self, max_parts: Option<usize>) -> Self {
        if let Some(ctx) = &mut self.key_index_route {
            ctx.max_probed_parts = max_parts;
        }
        self
    }

    /// Install the statement's scan budget (issue #1409): every
    /// `SpansScanExec` this provider builds refuses once the statement total
    /// passes the effective `max_bytes_scanned` or `max_s3_requests`, and the
    /// distributed fan-out carries the same byte ceiling.
    pub(crate) fn with_scan_budget(mut self, scan_budget: ScanBudget) -> Self {
        self.scan_budget = scan_budget;
        self
    }

    /// Record every scan's reads on `phases`, split by the phase that issued
    /// each, instead of on the one handle [`new`](Self::new) took.
    #[must_use]
    pub fn with_phase_accounting(mut self, phases: PhaseAccounting) -> Self {
        self.accounting = phases;
        self
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
        self.worker_fragment_with_pushdown(target_partitions, None)
    }

    /// [`Self::worker_fragment`], scanning under the coordinator's pushdown the
    /// slice ticket carried (issue #2755): its time bounds window the read, and
    /// a `trace_id` takes the ranged trace read, so the worker decodes only the
    /// blocks the predicate admits. `None`, or a logs pushdown, is the
    /// no-pushdown read. The coordinator re-applies every filter above the
    /// merge, so the pushdown only narrows what is decoded.
    #[cfg(feature = "flight-sql")]
    pub fn worker_fragment_with_pushdown(
        &self,
        target_partitions: usize,
        pushdown: Option<&SlicePushdown>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let pushdown = pushdown
            .and_then(SlicePushdown::as_spans)
            .cloned()
            .unwrap_or_default();
        let scan = self.build_scan(target_partitions, &pushdown, None)?;
        sort_slice_fragment(scan, &self.schema, SPAN_ORDER_COLS)
    }

    /// Apply projection pushdown (column selection only) above `plan` on the
    /// distributed path, where the fan-out returns the full public schema and
    /// DataFusion asked for a subset. The local path has no such fan-out: it
    /// pushes the projection straight into `SpansScanExec`, which emits the
    /// projected schema directly, so it never needs this wrapper.
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
        Ok(Arc::new(self.scan_over(
            &segments,
            target_partitions,
            pushdown,
            projection,
        )?))
    }

    fn scan_over(
        &self,
        segments: &[SegmentRef],
        target_partitions: usize,
        pushdown: &SpansPushdown,
        projection: Option<Vec<usize>>,
    ) -> DFResult<SpansScanExec> {
        SpansScanExec::new(
            self.tenant_hash,
            self.fetcher.clone(),
            segments,
            target_partitions,
            pushdown.span_query(),
            pushdown.service_name.clone(),
            pushdown.name.clone(),
            pushdown.duration_window(),
            pushdown.status_mask,
            Arc::clone(&self.erasure),
            projection,
            QueryAccounting::new(),
        )
        .map(|scan| {
            scan.with_phase_accounting(self.accounting.clone())
                .with_scan_budget(self.scan_budget.clone())
                .with_trace_set(pushdown.trace_ids.clone())
        })
    }

    /// Run the tier-2 router over the segments the ts prune kept. Pruned
    /// segments are dropped; the rest keep the snapshot's order.
    async fn route_trace(
        &self,
        ctx: &KeyIndexRouteContext,
        pool: Arc<dyn MemoryPool>,
        pushdown: &SpansPushdown,
        key_set: &KeySet,
    ) -> DFResult<RoutedSegments> {
        let segment_count = self.snapshot.segments.len();
        if ctx.parts.origin.len() != segment_count {
            return Err(DataFusionError::Internal(format!(
                "spans key-index route: {} part origins for {segment_count} segments",
                ctx.parts.origin.len()
            )));
        }
        let (ts_min, ts_max) = (pushdown.ts_min(), pushdown.ts_max());
        let (kept, origin): (Vec<SegmentRef>, Vec<_>) = self
            .snapshot
            .segments
            .iter()
            .zip(&ctx.parts.origin)
            .filter(|(s, _)| SpanSegmentFetcher::ts_range_relevant(s, ts_min, ts_max))
            .map(|(s, o)| (s.clone(), *o))
            .unzip();
        let snapshot = Snapshot {
            segments: kept,
            ..Snapshot::default()
        };
        let parts = SnapshotParts::new(ctx.parts.parts.clone(), origin);
        let reader = SpanLeafRangeReader::new(&self.fetcher, self.tenant_hash);
        let budget = PoolLeafBudget(pool);
        let input = RouteInput {
            catalog: &ctx.catalog,
            snapshot: &snapshot,
            parts: &parts,
            signal: Signal::Spans,
            tenant_hash: self.tenant_hash,
            key_set,
            accounting: &self.accounting,
            reader: &reader,
            budget: &budget,
            limits: KeyIndexLeafLimits::default(),
        };
        let (routes, figures) = match ctx.max_probed_parts {
            Some(max_parts) => route_with_max_parts(input, max_parts).await,
            None => route(input).await,
        }
        .map_err(|e| DataFusionError::External(e.into()))?;
        let mut routed = RoutedSegments {
            segments: Vec::with_capacity(routes.len()),
            block_sets: Vec::with_capacity(routes.len()),
            figures,
            pruned: 0,
        };
        for (segment, segment_route) in snapshot.segments.into_iter().zip(routes) {
            match segment_route {
                SegmentRoute::Pruned => routed.pruned += 1,
                SegmentRoute::Blocks(set) => {
                    routed.segments.push(segment);
                    routed.block_sets.push(Some(set));
                }
                SegmentRoute::Uncovered => {
                    routed.segments.push(segment);
                    routed.block_sets.push(None);
                }
            }
        }
        Ok(routed)
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
        // accounting under the statement's effective `max_bytes_scanned` (issue
        // #1409). See the `logs` provider for the full rationale: the
        // scan's `_limit` is a fetch-stop hint across partitions with the exact
        // limit re-applied above the merge, and filters are re-applied above the
        // returned plan by DataFusion (the provider reports `Inexact`). The
        // extracted trace id and time bounds ride to each worker with its slice
        // ticket, so a worker reads only the blocks they admit (#2755).
        #[cfg(feature = "flight-sql")]
        if let Some(dist) = &self.distributed {
            let plan = distributed_spans_plan(
                dist.endpoints.clone(),
                Arc::clone(&dist.client),
                Arc::clone(&self.schema),
                _limit,
                self.accounting.scan().clone(),
                self.scan_budget.max_bytes_scanned(),
                Some(SlicePushdown::spans(&extract_spans(filters))),
            )?;
            return self.apply_projection(plan, projection);
        }

        let target_partitions = state.config().target_partitions();
        let pushdown = extract_spans(filters);

        // Push the projection into the scan unconditionally: `SpansScanExec`
        // computes its own `columnar_eligible` flag from the same
        // `columnar_static_eligible` check, so passing a real projection is
        // safe whether or not it includes `attrs`/`events`/`links` or an
        // erasure predicate is pending. On the columnar fast path (ADR-0110
        // decision 4) this emits the projected schema directly and decodes
        // only the pages that schema needs. On the row path, `SpansScanExec`
        // is equally projection-aware: it only builds the arrow arrays for
        // the projected column indices, so a projection that excludes
        // `events`/`links` skips their decode there too, not only on the
        // columnar path. Either way, no wrapping `ProjectionExec` is needed.
        // A `trace_id =` routes its one id; a `trace_id IN (...)` set routes
        // as one key set (ADR-3692 decision 6).
        let key_set = match (pushdown.trace_id, pushdown.trace_ids.as_slice()) {
            (Some(trace_id), _) => Some(trace_key_set(&trace_id)),
            (None, []) => None,
            (None, ids) => Some(trace_ids_key_set(ids)),
        }
        .transpose()
        .map_err(|e| DataFusionError::External(e.into()))?;
        if let (Some(ctx), Some(key_set)) = (&self.key_index_route, key_set) {
            let routed = self
                .route_trace(
                    ctx,
                    Arc::clone(&state.runtime_env().memory_pool),
                    &pushdown,
                    &key_set,
                )
                .await?;
            let scan = self
                .scan_over(
                    &routed.segments,
                    target_partitions,
                    &pushdown,
                    projection.cloned(),
                )?
                .with_key_index_route(routed.block_sets, &routed.figures, routed.pruned)?;
            return Ok(Arc::new(scan));
        }
        self.build_scan(target_partitions, &pushdown, projection.cloned())
    }
}
