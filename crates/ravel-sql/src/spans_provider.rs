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
use ravel_query::erasure::{ErasurePredicate, snapshot_pending_erasure_predicates};

use crate::config::SqlConfig;
use crate::error::SqlError;
use crate::spans_fetcher::SpanSegmentFetcher;
use crate::spans_pushdown::{SpansPushdown, extract_spans};
use crate::spans_scan::SpansScanExec;
use crate::spans_schema::spans_schema;

/// The `spans` table provider for one tenant over one pinned `Signal::Spans`
/// snapshot.
pub struct SpansTableProvider {
    snapshot: Arc<Snapshot>,
    fetcher: SpanSegmentFetcher,
    config: SqlConfig,
    schema: SchemaRef,
    /// Pending selective-erasure predicates derived once from
    /// `snapshot.pending_erasure` (ADR-0064 decision 2, issue #829), cloned
    /// into every `SpansScanExec` the provider builds.
    erasure: Arc<Vec<ErasurePredicate>>,
}

impl SpansTableProvider {
    /// Build a provider around an owned, already-resolved `Signal::Spans`
    /// snapshot. `config` accepts anything `Into<SqlConfig>` (an `EngineConfig`
    /// alone works), matching the logs/metrics providers' constructor shape.
    pub fn new(
        snapshot: Snapshot,
        fetcher: SpanSegmentFetcher,
        config: impl Into<SqlConfig>,
    ) -> Self {
        let erasure = Arc::new(snapshot_pending_erasure_predicates(&snapshot));
        SpansTableProvider {
            snapshot: Arc::new(snapshot),
            fetcher,
            config: config.into(),
            schema: spans_schema(),
            erasure,
        }
    }

    /// Build the scan over every segment in the snapshot with no pushdown.
    /// Exposed (like the logs provider's `plan`) so tests can execute the scan
    /// without a SQL front-end.
    pub fn plan(&self, target_partitions: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_scan(target_partitions, &SpansPushdown::default())
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
        self.build_scan(target_partitions, &extract_spans(filters))
    }

    fn build_scan(
        &self,
        target_partitions: usize,
        pushdown: &SpansPushdown,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let segments = self.pruned_segments(pushdown);
        if segments.len() > self.config.engine.max_segments {
            return Err(SqlError::TooManySegments {
                count: segments.len(),
                max: self.config.engine.max_segments,
            }
            .into());
        }
        let scan = SpansScanExec::new(
            self.fetcher.clone(),
            &segments,
            target_partitions,
            pushdown.span_query(),
            pushdown.service_name.clone(),
            pushdown.name.clone(),
            Arc::clone(&self.erasure),
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
        let target_partitions = state.config().target_partitions();
        let pushdown = extract_spans(filters);
        let plan = self.build_scan(target_partitions, &pushdown)?;

        // Projection pushdown: column selection only. When a column is not
        // projected DataFusion still receives every column from the scan and
        // this ProjectionExec drops the rest; the fetch reads the whole object
        // regardless (RSPAN's reader has no per-column page toggle at this
        // layer), matching the logs/metrics providers' stance.
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
