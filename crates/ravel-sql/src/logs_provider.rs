//! `LogsTableProvider`: the `logs` table over one query's already-resolved,
//! owned `Snapshot` for `Signal::Logs` (ADR-0033). The log-signal sibling of
//! [`crate::provider::RavelTableProvider`].
//!
//! Like the metrics provider, this takes an owned, already-resolved `Snapshot`
//! (resolution is the endpoint's job, #240) and a [`LogSegmentFetcher`], and
//! never resolves. `scan` extracts widen-only pushdown from the filters
//! (crate::logs_pushdown), prunes the snapshot's segments by
//! [`LogSegmentFetcher::ts_range_relevant`] against the extracted ts bounds,
//! and builds a single [`LogsScanExec`] leaf.
//!
//! `supports_filters_pushdown` returns `Inexact` for every filter, exactly like
//! the metrics provider: DataFusion always re-applies the originals above the
//! scan, so pruning may only widen. Attribute predicates (`attrs['k']='v'`) are
//! not pushed at all — a stream-level prune is unsound against the merged `attrs`
//! column (crate::logs_pushdown, crate::logs_scan) — so they are evaluated
//! entirely by DataFusion's residual over the merged column.

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

use crate::config::SqlConfig;
use crate::error::SqlError;
use crate::logs_pushdown::{LogsPushdown, extract_logs};
use crate::logs_scan::LogsScanExec;
use crate::logs_schema::logs_schema;

/// The `logs` table provider for one tenant over one pinned `Signal::Logs`
/// snapshot.
pub struct LogsTableProvider {
    snapshot: Arc<Snapshot>,
    fetcher: LogSegmentFetcher,
    config: SqlConfig,
    schema: SchemaRef,
}

impl LogsTableProvider {
    /// Build a provider around an owned, already-resolved `Signal::Logs`
    /// snapshot. `config` accepts anything `Into<SqlConfig>` (an `EngineConfig`
    /// alone works), matching the metrics provider's constructor shape.
    pub fn new(
        snapshot: Snapshot,
        fetcher: LogSegmentFetcher,
        config: impl Into<SqlConfig>,
    ) -> Self {
        LogsTableProvider {
            snapshot: Arc::new(snapshot),
            fetcher,
            config: config.into(),
            schema: logs_schema(),
        }
    }

    /// Build the scan over every segment in the snapshot with no pushdown.
    /// Exposed (like the metrics provider's `plan`) so tests can execute the
    /// scan without a SQL front-end.
    pub fn plan(&self, target_partitions: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_scan(target_partitions, &LogsPushdown::default())
    }

    /// Build the scan for a set of filters, extracting the pushdown from them.
    /// Exposed so tests exercise the whole `extract_logs` -> prune -> scan path.
    pub fn plan_filters(
        &self,
        target_partitions: usize,
        filters: &[Expr],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_scan(target_partitions, &extract_logs(filters))
    }

    fn build_scan(
        &self,
        target_partitions: usize,
        pushdown: &LogsPushdown,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let segments = self.pruned_segments(pushdown);
        if segments.len() > self.config.engine.max_segments {
            return Err(SqlError::TooManySegments {
                count: segments.len(),
                max: self.config.engine.max_segments,
            }
            .into());
        }
        let scan = LogsScanExec::new(
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
    /// proven-required bound (via [`LogSegmentFetcher::ts_range_relevant`], the
    /// same catalog-summary check `fetch` uses); with no bound, every segment is
    /// kept.
    fn pruned_segments(&self, pushdown: &LogsPushdown) -> Vec<SegmentRef> {
        let (ts_min, ts_max) = (pushdown.ts_min(), pushdown.ts_max());
        self.snapshot
            .segments
            .iter()
            .filter(|s| LogSegmentFetcher::ts_range_relevant(s, ts_min, ts_max))
            .cloned()
            .collect()
    }
}

impl fmt::Debug for LogsTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LogsTableProvider")
            .field("segments", &self.snapshot.segments.len())
            .finish()
    }
}

#[async_trait]
impl TableProvider for LogsTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Inexact for every filter: the provider prunes what it can (widen-only),
    /// but DataFusion must always re-apply the original filters above the scan.
    /// Never `Exact` (review F8, carried over from the metrics provider).
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
        let pushdown = extract_logs(filters);
        let plan = self.build_scan(target_partitions, &pushdown)?;

        // Projection pushdown: column selection only. When a column is not
        // projected DataFusion still receives every column from the scan and
        // this ProjectionExec drops the rest; the fetch reads the whole object
        // regardless (RLOG's reader has no per-column page toggle at this
        // layer), matching the metrics provider's stance.
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
