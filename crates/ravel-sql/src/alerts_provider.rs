//! `AlertsTableProvider`: the `alerts` table over one query's already-resolved,
//! owned `Snapshot` for `Signal::Alerts` (ADR-0040, issue #383). The alert-signal
//! sibling of [`crate::logs_provider::LogsTableProvider`].
//!
//! Like the `logs` provider, this takes an owned, already-resolved `Snapshot`
//! and a [`LogSegmentFetcher`], and never resolves. Snapshot resolution -- which
//! shard(s) the alert records live on, and which segments -- is the endpoint's
//! job (the same staged split the `logs` table used, #239 before #240). This
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

use crate::alerts_pushdown::{AlertsPushdown, extract_alerts};
use crate::alerts_scan::AlertsScanExec;
use crate::alerts_schema::alerts_schema;
use crate::config::SqlConfig;
use crate::error::SqlError;

/// The `alerts` table provider for one tenant over one pinned `Signal::Alerts`
/// snapshot.
pub struct AlertsTableProvider {
    snapshot: Arc<Snapshot>,
    fetcher: LogSegmentFetcher,
    config: SqlConfig,
    schema: SchemaRef,
}

impl AlertsTableProvider {
    /// Build a provider around an owned, already-resolved `Signal::Alerts`
    /// snapshot. `config` accepts anything `Into<SqlConfig>` (an `EngineConfig`
    /// alone works), matching the `logs` provider's constructor shape.
    pub fn new(
        snapshot: Snapshot,
        fetcher: LogSegmentFetcher,
        config: impl Into<SqlConfig>,
    ) -> Self {
        AlertsTableProvider {
            snapshot: Arc::new(snapshot),
            fetcher,
            config: config.into(),
            schema: alerts_schema(),
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

    fn build_scan(
        &self,
        target_partitions: usize,
        pushdown: &AlertsPushdown,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let segments = self.pruned_segments(pushdown);
        if segments.len() > self.config.engine.max_segments {
            return Err(SqlError::TooManySegments {
                count: segments.len(),
                max: self.config.engine.max_segments,
            }
            .into());
        }
        let scan = AlertsScanExec::new(
            self.fetcher.clone(),
            &segments,
            target_partitions,
            pushdown.ts_min(),
            pushdown.ts_max(),
            Arc::new(pushdown.content.clone()),
        )?;
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
