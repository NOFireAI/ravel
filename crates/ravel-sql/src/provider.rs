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
use ravel_types::TenantHash;

use crate::config::SqlConfig;
use crate::dedup::RsegDedupExec;
use crate::error::SqlError;
use crate::pushdown::{Pushdown, extract};
use crate::scan::RsegScanExec;
use crate::schema::public_schema;

/// The `samples` table provider for one tenant over one pinned snapshot.
pub struct RavelTableProvider {
    snapshot: Arc<Snapshot>,
    tenant_hash: TenantHash,
    fetcher: SegmentFetcher,
    config: SqlConfig,
    schema: SchemaRef,
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
    ) -> Self {
        RavelTableProvider {
            snapshot: Arc::new(snapshot),
            tenant_hash,
            fetcher,
            config: config.into(),
            schema: public_schema(),
        }
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

        let dedup = RsegDedupExec::new(merge, self.config.engine.max_samples)?;
        Ok(Arc::new(dedup))
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
