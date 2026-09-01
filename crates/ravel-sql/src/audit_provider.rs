//! `AuditTableProvider`: the `audit` table over one query's already-resolved,
//! owned `Snapshot` for `Signal::Audit` (ADR-0040). The audit-signal
//! sibling of [`crate::logs_provider::LogsTableProvider`].
//!
//! Like the `logs` provider, this takes an owned, already-resolved `Snapshot`
//! and a [`LogSegmentFetcher`], and never resolves. Which shard(s) audit records
//! live on is a resolution-time concern, not this provider's: the legal-hold
//! records shipped so far all ride one fixed shard, but this crate never names
//! that number. The resolved snapshot already carries exactly the [`SegmentRef`]s
//! for whatever shard(s) audit occupies, and each `SegmentRef` carries its own
//! `shard`; if later audit record kinds spread across more shards, only what
//! resolution puts in the snapshot changes, not a constant here. Baking a shard
//! number into `ravel-sql` would silently break the day that happens.
//!
//! `scan` extracts widen-only pushdown from the filters (a `ts_ns` range only;
//! the `audit` table has no equality fast path, crate::audit_pushdown), prunes
//! the snapshot's segments by [`LogSegmentFetcher::ts_range_relevant`], and
//! builds a single [`AuditScanExec`] leaf. `supports_filters_pushdown` returns
//! `Inexact` for every filter, so DataFusion always re-applies the originals
//! above the scan; pruning may only widen.

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
use ravel_query::LogSegmentFetcher;
use ravel_query::erasure::{ErasurePredicate, snapshot_pending_erasure_predicates};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

use crate::audit_pushdown::{AuditPushdown, extract_audit};
use crate::audit_scan::AuditScanExec;
use crate::audit_schema::audit_schema;
#[cfg(feature = "flight-sql")]
use crate::distributed::{WorkerSlice, WorkerSliceClient};
#[cfg(feature = "flight-sql")]
use crate::distributed_rlog::{
    AUDIT_ORDER_COLS, DistributedRlogContext, distributed_audit_plan, sort_slice_fragment,
};
#[cfg(feature = "flight-sql")]
use ravel_query::ByteLimit;

/// The `audit` table provider for one tenant over one pinned `Signal::Audit`
/// snapshot.
pub struct AuditTableProvider {
    snapshot: Arc<Snapshot>,
    tenant_hash: TenantHash,
    fetcher: LogSegmentFetcher,
    schema: SchemaRef,
    /// This query's accounting handle (ADR-0044), cloned into every
    /// `AuditScanExec` the provider builds.
    accounting: QueryAccounting,
    /// Pending selective-erasure predicates derived once from
    /// `snapshot.pending_erasure` (ADR-0064 decision 2), cloned
    /// into every `AuditScanExec` the provider builds.
    erasure: Arc<Vec<ErasurePredicate>>,
    /// The coordinator-side distributed fan-out (ADR-0071; #326). `None` -- the
    /// default, and every non-Flight build -- is the local scan path unchanged.
    #[cfg(feature = "flight-sql")]
    distributed: Option<DistributedRlogContext>,
}

impl AuditTableProvider {
    /// Build a provider around an owned, already-resolved `Signal::Audit`
    /// snapshot. Admission and budget config live on the resolve-time seam
    ///, not on the provider, so this no longer takes a
    /// config parameter.
    pub fn new(
        snapshot: Snapshot,
        tenant_hash: TenantHash,
        fetcher: LogSegmentFetcher,
        accounting: QueryAccounting,
    ) -> Self {
        let erasure = Arc::new(snapshot_pending_erasure_predicates(&snapshot));
        AuditTableProvider {
            snapshot: Arc::new(snapshot),
            tenant_hash,
            fetcher,
            schema: audit_schema(),
            accounting,
            erasure,
            #[cfg(feature = "flight-sql")]
            distributed: None,
        }
    }

    /// Install a coordinator-side distributed scan context (ADR-0071; #326). The
    /// audit-signal sibling of
    /// [`LogsTableProvider::with_distributed_scan`](crate::LogsTableProvider):
    /// [`TableProvider::scan`] fans the `audit` scan out to the given worker
    /// endpoints, feeding the no-dedup `SortPreservingMergeExec`, instead of
    /// scanning locally. Only compiled with the Flight transport.
    #[cfg(feature = "flight-sql")]
    pub fn with_distributed_scan(
        mut self,
        endpoints: Vec<WorkerSlice>,
        client: Arc<dyn WorkerSliceClient>,
    ) -> Self {
        self.distributed = Some(DistributedRlogContext { endpoints, client });
        self
    }

    /// The worker-side fragment for a distributed `audit` scan (ADR-0071; #326):
    /// the whole-snapshot [`AuditScanExec`] wrapped in a single globally-sorted
    /// partition under the `audit` total-order key, streamed to the coordinator's
    /// no-dedup merge. There is NO dedup: audit has no query-time dedup.
    #[cfg(feature = "flight-sql")]
    pub fn worker_fragment(&self, target_partitions: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        let scan = self.build_scan(target_partitions, &AuditPushdown::default())?;
        sort_slice_fragment(scan, &self.schema, AUDIT_ORDER_COLS)
    }

    /// Apply projection pushdown (column selection only) above `plan` on the
    /// distributed path, where the fan-out returns the full public schema.
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
    /// Exposed (like the `logs` provider's `plan`) so tests can execute the scan
    /// without a SQL front-end.
    pub fn plan(&self, target_partitions: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_scan(target_partitions, &AuditPushdown::default())
    }

    /// Build the scan for a set of filters, extracting the pushdown from them.
    /// Exposed so tests exercise the whole `extract_audit` -> prune -> scan path.
    pub fn plan_filters(
        &self,
        target_partitions: usize,
        filters: &[Expr],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_scan(target_partitions, &extract_audit(filters))
    }

    /// Admission (the sealed-segment cap) is decided exactly once, at
    /// resolve time, by whichever endpoint resolves this table's snapshot,
    /// calling `ravel_query::admit` over the full snapshot and its
    /// `SegmentOrigins` -- the same pattern
    /// `SqlExecutor::resolve` uses for the two wired tables. No such endpoint
    /// exists yet for audit; `pruned_segments` below is a further,
    /// client-side, widen-only ts subset, so re-checking a count against it
    /// here would be a second, weaker check over the wrong set; it is not
    /// reimplemented.
    fn build_scan(
        &self,
        target_partitions: usize,
        pushdown: &AuditPushdown,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let segments = self.pruned_segments(pushdown);
        let scan = AuditScanExec::new(
            self.tenant_hash,
            self.fetcher.clone(),
            &segments,
            target_partitions,
            pushdown.ts_min(),
            pushdown.ts_max(),
            Arc::clone(&self.erasure),
            self.accounting.clone(),
        )?;
        Ok(Arc::new(scan))
    }

    /// Segments whose event-time span overlaps the extracted ts bounds.
    /// Widen-only: a segment is dropped only when its whole span lies outside a
    /// proven-required bound; with no bound, every segment is kept.
    fn pruned_segments(&self, pushdown: &AuditPushdown) -> Vec<SegmentRef> {
        let (ts_min, ts_max) = (pushdown.ts_min(), pushdown.ts_max());
        self.snapshot
            .segments
            .iter()
            .filter(|s| LogSegmentFetcher::ts_range_relevant(s, ts_min, ts_max))
            .cloned()
            .collect()
    }
}

impl fmt::Debug for AuditTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AuditTableProvider")
            .field("segments", &self.snapshot.segments.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for AuditTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Inexact for every filter: the provider prunes what it can (widen-only),
    /// but DataFusion must always re-apply the original filters above the scan.
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
        // Distributed coordinator path (ADR-0071; #326): fan out to worker slices
        // instead of scanning locally, folding worker bytes into the query
        // accounting under an `Unlimited` ceiling. See the `logs` provider for the
        // full rationale.
        #[cfg(feature = "flight-sql")]
        if let Some(dist) = &self.distributed {
            let plan = distributed_audit_plan(
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
        let pushdown = extract_audit(filters);
        let plan = self.build_scan(target_partitions, &pushdown)?;

        // Projection pushdown: column selection only. The fetch reads the whole
        // object regardless, matching the `logs` provider's stance.
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
