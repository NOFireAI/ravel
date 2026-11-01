//! `AlertsTableProvider`: the `alerts` table over one query's already-resolved,
//! owned `Snapshot` for `Signal::Alerts` (ADR-0040). The alert-signal
//! sibling of [`crate::logs_provider::LogsTableProvider`].
//!
//! Like the `logs` provider, this takes an owned, already-resolved `Snapshot`
//! and a [`LogSegmentFetcher`], and never resolves. Snapshot resolution -- which
//! shard(s) the alert records live on, and which segments -- is the endpoint's
//! job (the same staged split the `logs` table used). This
//! provider therefore never names a shard number: the resolved snapshot already
//! carries exactly the [`SegmentRef`]s for whatever shard(s) alerts occupy (one
//! stream per rule, spread across however many shards the write path uses), and
//! each `SegmentRef` carries its own `shard`. Widening the alert stream across
//! more shards later changes only what resolution puts in the snapshot, not a
//! constant here.
//!
//! `scan` extracts widen-only pushdown from the filters (a `ts_ns` range plus
//! exact `alert_id`/`rule_id` equalities, crate::alerts_pushdown), prunes the
//! snapshot's segments by [`LogSegmentFetcher::ts_range_relevant`], and builds a
//! single [`AlertsScanExec`] leaf. `supports_filters_pushdown` returns `Inexact`
//! for every filter, so DataFusion always re-applies the originals above the
//! scan; pruning may only widen.

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
use ravel_query::PhaseAccounting;
use ravel_query::erasure::{ErasurePredicate, snapshot_pending_erasure_predicates};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

use crate::alerts_pushdown::{AlertsPushdown, extract_alerts};
use crate::alerts_scan::AlertsScanExec;
use crate::alerts_schema::alerts_schema;
#[cfg(feature = "flight-sql")]
use crate::distributed::{WorkerSlice, WorkerSliceClient};
#[cfg(feature = "flight-sql")]
use crate::distributed_rlog::{
    ALERTS_ORDER_COLS, DistributedRlogContext, distributed_alerts_plan, sort_slice_fragment,
};
use crate::scan_budget::ScanBudget;

/// The `alerts` table provider for one tenant over one pinned `Signal::Alerts`
/// snapshot.
pub struct AlertsTableProvider {
    snapshot: Arc<Snapshot>,
    tenant_hash: TenantHash,
    fetcher: LogSegmentFetcher,
    schema: SchemaRef,
    /// This query's accounting handle (ADR-0044), cloned into every
    /// `AlertsScanExec` the provider builds.
    accounting: QueryAccounting,
    /// Pending selective-erasure predicates derived once from
    /// `snapshot.pending_erasure` (ADR-0064 decision 2), cloned
    /// into every `AlertsScanExec` the provider builds.
    erasure: Arc<Vec<ErasurePredicate>>,
    /// The coordinator-side distributed fan-out (ADR-0071; #326). `None` -- the
    /// default, and every non-Flight build -- is the local scan path unchanged.
    #[cfg(feature = "flight-sql")]
    distributed: Option<DistributedRlogContext>,
    /// The statement's byte and store-request budgets (issue #1409), checked
    /// by every `AlertsScanExec` this provider builds after each store read and
    /// passed to the distributed fan-out. Unlimited by default, installed with
    /// [`Self::with_scan_budget`].
    scan_budget: ScanBudget,
}

impl AlertsTableProvider {
    /// Build a provider around an owned, already-resolved `Signal::Alerts`
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
        let scan_budget = ScanBudget::unlimited(&PhaseAccounting::pooled_over(&accounting));
        AlertsTableProvider {
            snapshot: Arc::new(snapshot),
            tenant_hash,
            fetcher,
            schema: alerts_schema(),
            accounting,
            erasure,
            #[cfg(feature = "flight-sql")]
            distributed: None,
            scan_budget,
        }
    }

    /// Install the statement's scan budget (issue #1409): every
    /// `AlertsScanExec` this provider builds refuses once the statement total
    /// passes the effective `max_bytes_scanned` or `max_s3_requests`, and the
    /// distributed fan-out carries the same byte ceiling.
    pub(crate) fn with_scan_budget(mut self, scan_budget: ScanBudget) -> Self {
        self.scan_budget = scan_budget;
        self
    }

    /// Install a coordinator-side distributed scan context (ADR-0071; #326). The
    /// alert-signal sibling of
    /// [`LogsTableProvider::with_distributed_scan`](crate::LogsTableProvider):
    /// [`TableProvider::scan`] fans the `alerts` scan out to the given worker
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

    /// The worker-side fragment for a distributed `alerts` scan (ADR-0071; #326):
    /// the whole-snapshot [`AlertsScanExec`] wrapped in a single globally-sorted
    /// partition under the `alerts` total-order key, streamed to the coordinator's
    /// no-dedup merge. There is NO dedup: alerts have no query-time dedup.
    #[cfg(feature = "flight-sql")]
    pub fn worker_fragment(&self, target_partitions: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        let scan = self.build_scan(target_partitions, &AlertsPushdown::default())?;
        sort_slice_fragment(scan, &self.schema, ALERTS_ORDER_COLS)
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
        self.build_scan(target_partitions, &AlertsPushdown::default())
    }

    /// Build the scan for a set of filters, extracting the pushdown from them.
    /// Exposed so tests exercise the whole `extract_alerts` -> prune -> scan path
    /// (including the `alert_id`/`rule_id` equality fast paths).
    pub fn plan_filters(
        &self,
        target_partitions: usize,
        filters: &[Expr],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_scan(target_partitions, &extract_alerts(filters))
    }

    /// Admission (the sealed-segment cap) is decided exactly once, at
    /// resolve time, by whichever endpoint resolves this table's snapshot,
    /// calling `ravel_query::admit` over the full snapshot and its
    /// `SegmentOrigins` -- the same pattern
    /// `SqlExecutor::resolve` uses for the two wired tables. No such endpoint
    /// exists yet for alerts; `pruned_segments` below is a further,
    /// client-side, widen-only ts subset, so re-checking a count against it
    /// here would be a second, weaker check over the wrong set; it is not
    /// reimplemented.
    fn build_scan(
        &self,
        target_partitions: usize,
        pushdown: &AlertsPushdown,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let segments = self.pruned_segments(pushdown);
        let scan = AlertsScanExec::new(
            self.tenant_hash,
            self.fetcher.clone(),
            &segments,
            target_partitions,
            pushdown.ts_min(),
            pushdown.ts_max(),
            Arc::new(pushdown.content.clone()),
            Arc::clone(&self.erasure),
            self.accounting.clone(),
        )?
        .with_scan_budget(self.scan_budget.clone());
        Ok(Arc::new(scan))
    }

    /// Segments whose event-time span overlaps the extracted ts bounds.
    /// Widen-only: a segment is dropped only when its whole span lies outside a
    /// proven-required bound; with no bound, every segment is kept.
    fn pruned_segments(&self, pushdown: &AlertsPushdown) -> Vec<SegmentRef> {
        let (ts_min, ts_max) = (pushdown.ts_min(), pushdown.ts_max());
        self.snapshot
            .segments
            .iter()
            .filter(|s| LogSegmentFetcher::ts_range_relevant(s, ts_min, ts_max))
            .cloned()
            .collect()
    }
}

impl fmt::Debug for AlertsTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("AlertsTableProvider")
            .field("segments", &self.snapshot.segments.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for AlertsTableProvider {
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
        // accounting under the statement's effective `max_bytes_scanned` (issue
        // #1409). See the `logs` provider for the full rationale.
        #[cfg(feature = "flight-sql")]
        if let Some(dist) = &self.distributed {
            let plan = distributed_alerts_plan(
                dist.endpoints.clone(),
                Arc::clone(&dist.client),
                Arc::clone(&self.schema),
                _limit,
                self.accounting.clone(),
                self.scan_budget.max_bytes_scanned(),
            )?;
            return self.apply_projection(plan, projection);
        }

        let target_partitions = state.config().target_partitions();
        let pushdown = extract_alerts(filters);
        let plan = self.build_scan(target_partitions, &pushdown)?;

        // Projection pushdown: column selection only. The fetch reads the whole
        // object regardless (RLOG's reader has no per-column page toggle at this
        // layer), matching the `logs` provider's stance.
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_logseg::{AttrValue, LogRecord, stream_attrs_bytes};
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::memory::MemoryStore;
    use ravel_query::EngineConfig;

    use super::*;
    use crate::SqlError;
    use crate::executor::TargetSignal;
    use crate::scan_budget::test_support::{
        BudgetRun, KeyGets, TENANT, budget_run, bytes_budget, requests_budget, statement_with,
        write_rlog,
    };

    fn alert(ts: i64) -> LogRecord {
        LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(&[], "alerts", "1", &[]),
            stream_attrs: stream_attrs_bytes(&[], "alerts", "1", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 0,
            severity_text: "firing".into(),
            body: format!("alert {ts}"),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: vec![
                ("alert_id".into(), AttrValue::Str(format!("{ts:016x}"))),
                ("rule_id".into(), AttrValue::Str("r".into())),
                ("state".into(), AttrValue::Str("firing".into())),
                ("generation".into(), AttrValue::I64(1)),
            ],
        }
    }

    /// Three four-record alert objects, `alerts/seg-{0,1,2}`.
    async fn fixture() -> (Arc<KeyGets>, Vec<SegmentRef>) {
        let memory: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut segments = Vec::new();
        for i in 0..3i64 {
            let records: Vec<LogRecord> = (0..4).map(|k| alert(i * 10 + k)).collect();
            segments.push(write_rlog(memory.as_ref(), &format!("alerts/seg-{i}"), &records).await);
        }
        (KeyGets::wrap(memory), segments)
    }

    /// Scan the fixture over one partition under `engine`, in a statement that
    /// already charged `preload` (bytes, requests) to another table.
    async fn run(engine: &EngineConfig, preload: (u64, u64)) -> (BudgetRun, Vec<SegmentRef>) {
        let (store, segments) = fixture().await;
        let statement = statement_with(preload.0, preload.1);
        let backend: Arc<dyn ObjectStoreBackend> = store.clone();
        let provider = AlertsTableProvider::new(
            Snapshot {
                segments: segments.clone(),
                segments_pruned: 0,
                pending_erasure: Vec::new(),
            },
            TenantHash(TENANT),
            LogSegmentFetcher::new(backend),
            statement.lane(TargetSignal::Alerts).scan().clone(),
        )
        .with_scan_budget(statement.scan_budget(TargetSignal::Alerts, engine));
        let plan = provider.plan(1).expect("plan");
        let run = budget_run(plan.as_ref(), &store, &segments, &statement).await;
        (run, segments)
    }

    /// Each alert object is read in one whole-object GET. A budget equal to
    /// the first object's size admits it and refuses at the second read, at
    /// exactly the first two sizes summed, with the third object never
    /// fetched.
    ///
    /// FLIP: removing `budget.check()?` from `alerts_scan::prepare_partition`
    /// returns all 12 rows. Moving it below the fetch loop refuses at all
    /// three sizes summed with `seg-2` fetched. Comparing with `>=` refuses at
    /// the first object alone.
    #[tokio::test]
    async fn lowered_byte_budget_fails_typed_on_alerts_scan() {
        let (free, segments) = run(&EngineConfig::default(), (0, 0)).await;
        assert!(free.error.is_none(), "{:?}", free.error);
        assert_eq!(free.rows, 12);
        assert_eq!(free.gets, [1, 1, 1]);
        let sizes: Vec<u64> = segments.iter().map(|s| s.object_size).collect();
        assert_eq!(free.total, (sizes.iter().sum(), 3));

        let (run, _) = run(&bytes_budget(sizes[0]), (0, 0)).await;
        let expected = sizes[0] + sizes[1];
        match run.error {
            Some(SqlError::TooManyBytesScanned { scanned, max })
                if scanned == expected && max == sizes[0] => {}
            other => panic!("expected TooManyBytesScanned at {expected}, got {other:?}"),
        }
        assert_eq!(run.rows, 0);
        assert_eq!(run.gets, [1, 1, 0]);
        assert_eq!(run.total, (expected, 2));
    }

    /// A one-request budget refuses at the second GET.
    ///
    /// FLIP: removing `budget.check()?` from `alerts_scan::prepare_partition`
    /// returns all 12 rows.
    #[tokio::test]
    async fn lowered_request_budget_fails_typed_on_alerts_scan() {
        let (run, _) = run(&requests_budget(1), (0, 0)).await;
        assert!(
            matches!(
                run.error,
                Some(SqlError::RequestBudgetExceeded {
                    requests: 2,
                    max: 1,
                    table: None,
                    ..
                })
            ),
            "{:?}",
            run.error
        );
        assert_eq!(run.gets, [1, 1, 0]);
        assert_eq!(run.total.1, 2);
    }

    /// Two requests already charged to another table leave room for one
    /// alert GET under a three-request budget: the scan refuses at the
    /// statement's fourth request, though the alerts table alone made two.
    ///
    /// FLIP: building the budget over the alerts lane alone returns all 12
    /// rows.
    #[tokio::test]
    async fn the_alerts_scan_charges_the_statement_total() {
        let (alone, _) = run(&requests_budget(3), (0, 0)).await;
        assert!(alone.error.is_none(), "{:?}", alone.error);
        let (run, _) = run(&requests_budget(3), (0, 2)).await;
        assert!(
            matches!(
                run.error,
                Some(SqlError::RequestBudgetExceeded {
                    requests: 4,
                    max: 3,
                    ..
                })
            ),
            "{:?}",
            run.error
        );
        assert_eq!(run.gets, [1, 1, 0]);
    }
}
