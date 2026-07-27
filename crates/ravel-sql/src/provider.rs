//! `RavelTableProvider`: the `samples` table over one query's already
//! resolved, owned `Snapshot`.
//!
//! Snapshot resolution is a later ticket's job (it threads `now_ns` through
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

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::DataFusionError;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use ravel_catalog::Snapshot;
use ravel_query::{EngineConfig, SegmentFetcher};
use ravel_types::TenantHash;

use crate::dedup::RsegDedupExec;
use crate::error::SqlError;
use crate::scan::RsegScanExec;
use crate::schema::public_schema;

/// The `samples` table provider for one tenant over one pinned snapshot.
pub struct RavelTableProvider {
    snapshot: Arc<Snapshot>,
    tenant_hash: TenantHash,
    fetcher: SegmentFetcher,
    config: EngineConfig,
    schema: SchemaRef,
}

impl RavelTableProvider {
    /// Build a provider around an owned, already-resolved `Snapshot`.
    pub fn new(
        snapshot: Snapshot,
        tenant_hash: TenantHash,
        fetcher: SegmentFetcher,
        config: EngineConfig,
    ) -> Self {
        RavelTableProvider {
            snapshot: Arc::new(snapshot),
            tenant_hash,
            fetcher,
            config,
            schema: public_schema(),
        }
    }

    /// Build the full scan -> merge -> dedup physical pipeline over this
    /// provider's snapshot. Exposed directly (not only through `scan`) so
    /// B1's layer-1 scan oracle can execute the post-dedup output without a
    /// SQL front-end.
    pub fn plan(&self, target_partitions: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        if self.snapshot.segments.len() > self.config.max_segments {
            return Err(SqlError::TooManySegments {
                count: self.snapshot.segments.len(),
                max: self.config.max_segments,
            }
            .into());
        }

        let scan = Arc::new(RsegScanExec::new(
            self.tenant_hash,
            self.fetcher.clone(),
            &self.snapshot.segments,
            target_partitions,
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

        let dedup = RsegDedupExec::new(merge, self.config.max_samples)?;
        Ok(Arc::new(dedup))
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

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let target_partitions = state.config().target_partitions();
        let plan = self.plan(target_partitions)?;

        // B1 has no pushdown. Projection is honored by wrapping the public
        // pipeline output in a ProjectionExec so the returned plan's schema
        // matches what DataFusion requested.
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
