//! `RavelTableProvider`: the `samples` table over one query's already
//! resolved, owned `Snapshot`.
//!
//! Snapshot resolution is the endpoint's job (it threads `now_ns` through
//! `Catalog::resolve`, review F11); this provider takes an owned `Snapshot`
//! and never resolves. `scan` (or the test-facing [`RavelTableProvider::plan`])
//! builds the three-stage physical pipeline:
//!
//! ```text
//! RsegScanExec -> SortPreservingMergeExec (series_id, ts) -> RsegDedupExec
//! ```
//!
//! The merge is `SortPreservingMergeExec`, never `CoalescePartitionsExec`:
//! deterministic float ordering depends on it (review F12).
//!
//! Pushdown (ticket B2): `supports_filters_pushdown` returns `Inexact` for
//! every filter so DataFusion always re-applies them above the scan, and
//! `scan` extracts widen-only pruning from the filters (crate::pushdown):
//! segment skipping from ts bounds, label/series matchers into the fetcher, and
//! a `series_id` allow-set. Pruning only ever widens the read set relative to
//! the query's true need; exactness comes from the scan re-applying nothing
//! destructive plus DataFusion's residual re-application.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use ravel_catalog::{SegmentRef, Snapshot};
use ravel_promql::LabelMatcher;
use ravel_query::SegmentFetcher;
use ravel_query::erasure::{ErasurePredicate, snapshot_pending_erasure_predicates};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

use crate::config::SqlConfig;
use crate::dedup::RsegDedupExec;
use crate::error::SqlError;
use crate::pushdown::{Pushdown, extract};
use crate::scan::RsegScanExec;
use crate::schema::public_schema;

/// A coordinator-side distributed scan context (ADR-0071, issue #866): the
/// worker endpoints one query fans out to, and the client that fetches each
/// slice. Present only on a coordinator provider built via
/// [`RavelTableProvider::with_distributed_scan`]; absent (the default) means
/// the local scan path.
#[cfg(feature = "flight-sql")]
struct DistributedScanContext {
    endpoints: Vec<crate::distributed::WorkerSlice>,
    client: Arc<dyn crate::distributed::WorkerSliceClient>,
}

/// The `samples` table provider for one tenant over one pinned snapshot.
pub struct RavelTableProvider {
    snapshot: Arc<Snapshot>,
    tenant_hash: TenantHash,
    fetcher: SegmentFetcher,
    config: SqlConfig,
    schema: SchemaRef,
    /// This query's accounting handle (ADR-0044), cloned into every
    /// `RsegScanExec` the provider builds so every store fetch the scan
    /// issues on this query's behalf is recorded against it.
    accounting: QueryAccounting,
    /// Pending selective-erasure predicates derived once from
    /// `snapshot.pending_erasure` (ADR-0064 decision 2, issue #829), cloned
    /// into every `RsegScanExec` the provider builds. The distributed
    /// coordinator path never reaches `build_merge` (it fans out to workers,
    /// each of which independently resolves its own snapshot and builds its
    /// own provider), so this field alone covers the local scan and the
    /// worker-side fragment.
    erasure: Arc<Vec<ErasurePredicate>>,
    /// The coordinator-side distributed fan-out, if this provider is acting as
    /// a distributed coordinator (ADR-0071, issue #866). `None` -- the default,
    /// and every non-Flight build -- is the local scan path unchanged.
    #[cfg(feature = "flight-sql")]
    distributed: Option<DistributedScanContext>,
}

impl RavelTableProvider {
    /// Build a provider around an owned, already-resolved `Snapshot`.
    ///
    /// `config` accepts anything `Into<SqlConfig>`, so an `EngineConfig` alone
    /// works (the byte budget defaults); callers wanting a specific per-query
    /// byte budget pass a full [`SqlConfig`].
    pub fn new(
        snapshot: Snapshot,
        tenant_hash: TenantHash,
        fetcher: SegmentFetcher,
        config: impl Into<SqlConfig>,
        accounting: QueryAccounting,
    ) -> Self {
        let erasure = Arc::new(snapshot_pending_erasure_predicates(&snapshot));
        RavelTableProvider {
            snapshot: Arc::new(snapshot),
            tenant_hash,
            fetcher,
            config: config.into(),
            schema: public_schema(),
            accounting,
            erasure,
            #[cfg(feature = "flight-sql")]
            distributed: None,
        }
    }

    /// Install a coordinator-side distributed scan context (ADR-0071, issue
    /// #866). With it, [`TableProvider::scan`] fans the samples scan out to the
    /// given worker endpoints -- one `DistributedScanExec` partition per slice,
    /// feeding the same merge -> dedup pair -- instead of scanning the local
    /// snapshot. Without it, the provider is unchanged. Only compiled with the
    /// Flight transport, which is the only thing that can carry a slice ticket.
    #[cfg(feature = "flight-sql")]
    pub fn with_distributed_scan(
        mut self,
        endpoints: Vec<crate::distributed::WorkerSlice>,
        client: Arc<dyn crate::distributed::WorkerSliceClient>,
    ) -> Self {
        self.distributed = Some(DistributedScanContext { endpoints, client });
        self
    }

    /// Build the full scan -> merge -> dedup physical pipeline over every
    /// segment in the snapshot with no pushdown. Exposed directly (not only
    /// through `scan`) so B1's layer-1 scan oracle can execute the post-dedup
    /// output without a SQL front-end.
    pub fn plan(&self, target_partitions: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_pipeline(
            target_partitions,
            &self.snapshot.segments,
            Arc::new(Vec::new()),
            None,
        )
    }

    /// Build the pipeline over an explicit segment set with pushed matchers and
    /// an optional `series_id` allow-set. `segments` is already ts-pruned by
    /// the caller; the `max_segments` budget is checked against what will
    /// actually be scanned.
    fn build_pipeline(
        &self,
        target_partitions: usize,
        segments: &[SegmentRef],
        matchers: Arc<Vec<LabelMatcher>>,
        series_ids: Option<Arc<HashSet<[u8; 16]>>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let merge = self.build_merge(target_partitions, segments, matchers, series_ids)?;
        let dedup = RsegDedupExec::new(merge, self.config.engine.max_samples)?;
        Ok(Arc::new(dedup))
    }

    /// Build the pre-dedup half of the pipeline: `RsegScanExec ->
    /// SortPreservingMergeExec (series_id, ts)`, internal schema, one output
    /// partition, sorted by `(series_id, ts)`. The full pipeline wraps this in
    /// [`RsegDedupExec`]; the distributed worker fragment
    /// ([`Self::worker_fragment`]) returns it as-is, because dedup stays
    /// authoritative at the coordinator (ADR-0071, crate::distributed).
    fn build_merge(
        &self,
        target_partitions: usize,
        segments: &[SegmentRef],
        matchers: Arc<Vec<LabelMatcher>>,
        series_ids: Option<Arc<HashSet<[u8; 16]>>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if segments.len() > self.config.engine.max_segments {
            return Err(SqlError::TooManySegments {
                count: segments.len(),
                max: self.config.engine.max_segments,
            }
            .into());
        }

        let scan = Arc::new(RsegScanExec::new(
            self.tenant_hash,
            self.fetcher.clone(),
            segments,
            target_partitions,
            matchers,
            series_ids,
            self.config.engine.max_series,
            self.config.engine.max_bytes_scanned,
            Arc::clone(&self.erasure),
            self.accounting.clone(),
        )?);
        let scan_schema = scan.schema();

        let asc = datafusion::arrow::compute::SortOptions {
            descending: false,
            nulls_first: false,
        };
        let merge_exprs = ["series_id", "ts"]
            .into_iter()
            .map(|name| Ok(PhysicalSortExpr::new(col(name, &scan_schema)?, asc)))
            .collect::<DFResult<Vec<_>>>()?;
        let ordering = LexOrdering::new(merge_exprs)
            .ok_or_else(|| DataFusionError::Internal("empty merge ordering".into()))?;
        let merge: Arc<dyn ExecutionPlan> = Arc::new(SortPreservingMergeExec::new(ordering, scan));
        Ok(merge)
    }

    /// The worker-side fragment for a distributed SQL scan (ADR-0071, issue
    /// #866): the internal-schema, `(series_id, ts)`-sorted `RsegScanExec ->
    /// SortPreservingMergeExec` pipeline over `segments`, with no pushdown and
    /// NO dedup.
    ///
    /// A worker executes this over its slice and streams the result to the
    /// coordinator, whose `DistributedScanExec` exposes each worker stream as
    /// one sorted partition feeding the SAME merge -> dedup pair
    /// (crate::distributed). The provenance columns are retained and dedup is
    /// deliberately absent, because the winner for a `(series_id, ts)` sample
    /// is only decidable once every slice's candidates meet at the
    /// coordinator's authoritative dedup.
    pub fn worker_fragment(
        &self,
        target_partitions: usize,
        segments: &[SegmentRef],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_merge(target_partitions, segments, Arc::new(Vec::new()), None)
    }

    /// Apply projection pushdown (column selection only) above `plan`. Shared
    /// by the local scan path and the distributed scan path so both project
    /// identically; see the note in [`TableProvider::scan`].
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

    /// Segments overlapping the extracted ts bounds. Widen-only: a segment is
    /// dropped only when its whole event-time span lies outside a
    /// proven-required bound; with no bound, every segment is kept.
    fn pruned_segments(&self, pushdown: &Pushdown) -> Vec<SegmentRef> {
        self.snapshot
            .segments
            .iter()
            .filter(|s| pushdown.segment_in_range(s.min_event_ts_ns, s.max_event_ts_ns))
            .cloned()
            .collect()
    }
}

impl fmt::Debug for RavelTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RavelTableProvider")
            .field("segments", &self.snapshot.segments.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for RavelTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Inexact for every filter: the provider prunes what it can (widen-only),
    /// but DataFusion must always re-apply the original filters above the scan.
    /// Never `Exact`, which would let DataFusion drop the residual and trust
    /// the scan to have filtered precisely, which it never does (review F8).
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
        // Distributed coordinator path (ADR-0071, issue #866): fan out to
        // worker slices instead of scanning locally. The scan's `_limit` is
        // honored here as a fetch-stop hint across the distributed partitions,
        // with the exact limit re-applied above the dedup (over-fetch is safe,
        // under-fetch is not; crate::distributed). Filters are re-applied above
        // the returned plan by DataFusion (the provider reports `Inexact`), so
        // not pushing them to workers only widens each worker's read, never
        // changes a row. Aggregation, when the session plans it, sits above the
        // dedup's single partition, so the SQL determinism ban is untouched.
        #[cfg(feature = "flight-sql")]
        if let Some(dist) = &self.distributed {
            let plan = crate::distributed::distributed_samples_plan(
                dist.endpoints.clone(),
                Arc::clone(&dist.client),
                self.config.engine.max_samples,
                _limit,
                self.accounting.clone(),
                self.config.engine.max_bytes_scanned,
            )?;
            return self.apply_projection(plan, projection);
        }

        let target_partitions = state.config().target_partitions();

        // Extract widen-only pushdown from the filters and prune with it.
        let pushdown = extract(filters);
        let segments = self.pruned_segments(&pushdown);
        let matchers = Arc::new(pushdown.matchers);
        let series_ids = pushdown.series_ids.map(Arc::new);

        let plan = self.build_pipeline(target_partitions, &segments, matchers, series_ids)?;

        // Projection pushdown: column selection only. When `value` (or any
        // column) is not projected, DataFusion still receives every column
        // from the scan and this ProjectionExec drops the rest. We do NOT skip
        // VAL/TS page GETs for unprojected columns: the `SegmentFetcher`
        // SoA API (`fetch_soa`) fetches TS and VAL pages together and exposes
        // no per-column toggle, so skipping VAL GETs would require a
        // ravel-query API change outside this ticket's crate scope. The
        // dangerous TS-page-skip/`sample_count` optimization stays off
        // regardless (review F8): it is valid only when the post-dedup row
        // count provably equals SERIES_TABLE `sample_count` for the exact
        // pruned case, which is not proven, so correctness keeps the GETs.
        self.apply_projection(plan, projection)
    }
}
