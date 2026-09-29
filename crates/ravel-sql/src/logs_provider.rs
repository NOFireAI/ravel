//! `LogsTableProvider`: the `logs` table over one query's already-resolved,
//! owned `Snapshot` for `Signal::Logs` (ADR-0033). The log-signal sibling of
//! [`crate::provider::RavelTableProvider`].
//!
//! Like the metrics provider, this takes an owned, already-resolved `Snapshot`
//! (resolution is the endpoint's job) and a [`LogSegmentFetcher`], and
//! never resolves. `scan` extracts widen-only pushdown from the filters
//! (crate::logs_pushdown), prunes the snapshot's segments by
//! [`LogSegmentFetcher::ts_range_relevant`] against the extracted ts bounds
//! and then by declared-column statistics (crate::logs_stats_prune, ADR-2121
//! D1), and builds a single [`LogsScanExec`] leaf.
//!
//! `supports_filters_pushdown` returns `Inexact` for every filter except one
//! that resolves purely to a `ts` bound and/or a `has_word` call, which the
//! reader re-verifies per row and so answers `Exact` (issue #733). Everything
//! else DataFusion re-applies above the scan, so pruning may only widen. An
//! attribute predicate (`attrs['k']='v'`) is
//! pushed only into the prune-only channel ([`LogsPushdown::prune`]), which
//! drives POSTINGS block pruning inside the reader and is never evaluated per
//! row: a stream-level or per-record prune used as a filter would be unsound
//! against the merged `attrs` column (crate::logs_pushdown, crate::logs_scan).
//! The equality itself is still evaluated entirely by DataFusion's residual over
//! the merged column, so the channel changes which blocks the fetch reads and
//! never which rows the query returns.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
#[cfg(feature = "flight-sql")]
use datafusion::physical_expr::expressions::col;
use datafusion::physical_plan::ExecutionPlan;
#[cfg(feature = "flight-sql")]
use datafusion::physical_plan::projection::ProjectionExec;
use ravel_catalog::{LoadedColumnStats, SegmentRef, Snapshot};
#[cfg(feature = "flight-sql")]
use ravel_query::ByteLimit;
use ravel_query::LogSegmentFetcher;
use ravel_query::PhaseAccounting;
use ravel_query::erasure::{ErasurePredicate, snapshot_pending_erasure_predicates};
use ravel_types::TenantHash;
#[cfg(test)]
use ravel_types::accounting::QueryAccounting;

use crate::declared::DeclaredColumn;
#[cfg(feature = "flight-sql")]
use crate::distributed::{WorkerSlice, WorkerSliceClient};
#[cfg(feature = "flight-sql")]
use crate::distributed_rlog::{
    DistributedRlogContext, LOGS_ORDER_COLS, distributed_logs_plan, sort_slice_fragment,
};
use crate::logs_pushdown::{LogsPushdown, extract_logs, filter_is_exact};
use crate::logs_scan::LogsScanExec;
use crate::logs_schema::{logs_schema, logs_schema_with_declared};
use crate::logs_stats_prune::prune_segments_by_stats;

/// The `logs` table provider for one tenant over one pinned `Signal::Logs`
/// snapshot.
pub struct LogsTableProvider {
    snapshot: Arc<Snapshot>,
    tenant_hash: TenantHash,
    fetcher: LogSegmentFetcher,
    schema: SchemaRef,
    /// This query's phase-split accounting handle (ADR-0044, issue #796),
    /// cloned into every `LogsScanExec` the provider builds so every store
    /// fetch the scan issues on this query's behalf is recorded against the
    /// right phase.
    phase_accounting: PhaseAccounting,
    /// Pending selective-erasure predicates derived once from
    /// `snapshot.pending_erasure` (ADR-0064 decision 2), cloned
    /// into every `LogsScanExec` the provider builds.
    erasure: Arc<Vec<ErasurePredicate>>,
    /// The tenant's declared typed attribute columns (ADR-0090), in schema-
    /// append order. Resolved once per plan by `SqlExecutor` and installed with
    /// [`LogsTableProvider::with_declared_columns`]; empty for a
    /// zero-declaration query, which reproduces the pre-ADR-0090 provider
    /// exactly. The provider's advertised [`Self::schema`] and every
    /// `LogsScanExec` it builds are derived from this list.
    declared: Arc<Vec<DeclaredColumn>>,
    /// Exact per-segment column statistics for the tenant's declared columns
    /// (ADR-0850), resolved once per plan by `SqlExecutor` and installed with
    /// [`LogsTableProvider::with_column_stats`]. `None` -- the default -- is
    /// byte-identical to the pre-ADR-0850 provider: every metadata-only path
    /// degrades to scanning.
    column_stats: Option<Arc<LoadedColumnStats>>,
    /// The coordinator-side distributed fan-out (ADR-0071; #326), if this
    /// provider is acting as a distributed coordinator. `None` -- the default,
    /// and every non-Flight build -- is the local scan path unchanged.
    #[cfg(feature = "flight-sql")]
    distributed: Option<DistributedRlogContext>,
    /// Whether every `LogsScanExec` this provider builds publishes its
    /// per-segment scan timeline (`SqlConfig::segment_timing`). `false` by
    /// default, installed with [`Self::with_segment_timing`].
    segment_timing: bool,
    /// Whether [`Self::build_scan`] skips segments by declared-column
    /// statistics (ADR-2121 D1). Always `true` outside tests, which turn it
    /// off to compare against the unpruned scan.
    stats_pruning: bool,
}

impl LogsTableProvider {
    /// Build a provider around an owned, already-resolved `Signal::Logs`
    /// snapshot. Admission and budget config live on the resolve-time seam
    ///, not on the provider, so this no longer takes a
    /// config parameter.
    pub fn new(
        snapshot: Snapshot,
        tenant_hash: TenantHash,
        fetcher: LogSegmentFetcher,
        phase_accounting: PhaseAccounting,
    ) -> Self {
        let erasure = Arc::new(snapshot_pending_erasure_predicates(&snapshot));
        LogsTableProvider {
            snapshot: Arc::new(snapshot),
            tenant_hash,
            fetcher,
            schema: logs_schema(),
            phase_accounting,
            erasure,
            declared: Arc::new(Vec::new()),
            column_stats: None,
            #[cfg(feature = "flight-sql")]
            distributed: None,
            segment_timing: false,
            stats_pruning: true,
        }
    }

    /// Install a coordinator-side distributed scan context (ADR-0071; #326). With
    /// it, [`TableProvider::scan`] fans the `logs` scan out to the given worker
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

    /// The worker-side fragment for a distributed `logs` scan (ADR-0071; #326):
    /// the whole-snapshot [`LogsScanExec`] (all columns, no pushdown) wrapped in a
    /// single globally-sorted partition under the `logs` total-order key. A worker
    /// executes this over its slice and streams the result to the coordinator,
    /// whose `DistributedSliceScanExec` exposes each worker stream as one sorted
    /// partition feeding the SAME no-dedup merge. There is NO dedup, in the worker
    /// or the coordinator: logs have no query-time dedup, so every fetched record
    /// is returned.
    #[cfg(feature = "flight-sql")]
    pub fn worker_fragment(&self, target_partitions: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        let scan = self.build_scan(target_partitions, &LogsPushdown::default(), None)?;
        sort_slice_fragment(scan, &self.schema, LOGS_ORDER_COLS)
    }

    /// Apply projection pushdown (column selection only) above `plan`, used only
    /// on the distributed path where the fan-out returns the full public schema
    /// and DataFusion asked for a subset. The local path pushes projection into
    /// the scan instead (see [`Self::build_scan`]).
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

    /// Install the tenant's declared typed attribute columns (ADR-0090), which
    /// the caller (`SqlExecutor`) resolved once per plan and threaded down. The
    /// provider's advertised schema becomes
    /// `logs_schema_with_declared(&declared)` and every scan it builds carries
    /// the list, so a declared column projects as a native typed Arrow column.
    ///
    /// A builder method rather than a `new` parameter so `LogsTableProvider::new`
    /// stays source-compatible with existing callers and tests: the
    /// zero-declaration default is exactly the base `logs` schema.
    pub fn with_declared_columns(mut self, declared: Vec<DeclaredColumn>) -> Self {
        self.schema = logs_schema_with_declared(&declared);
        self.declared = Arc::new(declared);
        self
    }

    /// Install this plan's loaded column statistics (ADR-0850), resolved once
    /// by `SqlExecutor` via `Catalog::load_column_stats` and threaded down. A
    /// builder method for the same reason [`Self::with_declared_columns`] is
    /// one: `LogsTableProvider::new` stays source-compatible, and `None` (the
    /// default) reproduces the pre-ADR-0850 provider exactly.
    pub fn with_column_stats(mut self, column_stats: Option<Arc<LoadedColumnStats>>) -> Self {
        self.column_stats = column_stats;
        self
    }

    /// Turn on every `LogsScanExec` this provider builds publishing its
    /// per-segment scan timeline (issue #913). `false` (the default)
    /// reproduces the pre-timeline-gate provider exactly: `LogsScanExec`
    /// registers none of the `seg_*_offset` metrics and
    /// `accumulate_scan_timing` finds no per-segment rows to fold. A builder
    /// method for the same reason [`Self::with_declared_columns`] is one:
    /// `LogsTableProvider::new` stays source-compatible.
    pub fn with_segment_timing(mut self, segment_timing: bool) -> Self {
        self.segment_timing = segment_timing;
        self
    }

    /// Turn plan-time statistics pruning off, so a test can compare the rows
    /// and fetches of the same query with and without it.
    #[cfg(test)]
    pub(crate) fn with_stats_pruning(mut self, stats_pruning: bool) -> Self {
        self.stats_pruning = stats_pruning;
        self
    }

    /// Build the scan over every segment in the snapshot with no pushdown and
    /// no projection (every column). Exposed (like the metrics provider's
    /// `plan`) so tests can execute the scan without a SQL front-end.
    pub fn plan(&self, target_partitions: usize) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_scan(target_partitions, &LogsPushdown::default(), None)
    }

    /// Build the scan for a set of filters, extracting the pushdown from them.
    /// Exposed so tests exercise the whole `extract_logs` -> prune -> scan path.
    pub fn plan_filters(
        &self,
        target_partitions: usize,
        filters: &[Expr],
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.build_scan(
            target_partitions,
            &extract_logs(filters, self.declared.as_ref()),
            None,
        )
    }

    /// Admission (the sealed-segment cap) is decided exactly once, at
    /// resolve time, by `SqlExecutor::resolve` calling `ravel_query::admit`
    /// over the full, unpruned snapshot and its `SegmentOrigins`.
    /// `pruned_segments` below is a further, client-side,
    /// widen-only ts subset of that already-admitted snapshot, so
    /// re-checking a count against it here would be a second, weaker check
    /// over the wrong set (post-prune, origin-blind); it is not
    /// reimplemented. The same holds for the statistics pruning below
    /// (ADR-2121 D1): a segment it skips was still admitted.
    fn build_scan(
        &self,
        target_partitions: usize,
        pushdown: &LogsPushdown,
        projection: Option<&Vec<usize>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let segments = self.pruned_segments(pushdown);
        let (segments, pruned_by_stats) = if self.stats_pruning {
            prune_segments_by_stats(
                segments,
                &pushdown.prune,
                self.declared.as_ref(),
                self.column_stats.as_deref(),
            )
        } else {
            (segments, 0)
        };
        let scan = LogsScanExec::new(
            self.tenant_hash,
            self.fetcher.clone(),
            &segments,
            target_partitions,
            pushdown.ts_min(),
            pushdown.ts_max(),
            Arc::new(pushdown.content.clone()),
            Arc::new(pushdown.prune.clone()),
            Arc::clone(&self.erasure),
            projection,
            self.phase_accounting.clone(),
            Arc::clone(&self.schema),
            Arc::clone(&self.declared),
        )?
        .with_column_stats(self.column_stats.clone())
        .with_segment_timing(self.segment_timing)
        .with_segments_pruned_by_stats(pruned_by_stats);
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

    /// `Exact` for a filter that resolves purely to a `ts` bound and/or a
    /// `has_word` content predicate; `Inexact` for everything else (issue #733).
    ///
    /// The split follows which reader channel the extracted predicate lands in
    /// ([`crate::logs_pushdown`]):
    ///
    /// - `ts` bounds and `has_word` arms go to the exact channel
    ///   ([`LogsPushdown::ts_lo`]/[`LogsPushdown::ts_hi`],
    ///   [`LogsPushdown::content`]), which `ravel_query`'s `combined_predicate`
    ///   folds into the single `Predicate` the reader re-verifies per decoded
    ///   row: `ravel_logseg::reader`'s `eval` reads that row's own `ts` for
    ///   `Predicate::TsRange` and that row's own field text for
    ///   `Predicate::HasWord`. Nothing survives for a residual to re-check, so
    ///   the filter can be deleted from the plan. Deleting it is what lets
    ///   `LogsScanExec`'s exact leaf statistics reach DataFusion's
    ///   `AggregateStatistics` rule: a `FilterExec` above the scan would report
    ///   its own non-exact statistics instead of passing the leaf's through.
    /// - Everything else -- an `attrs['k'] = 'v'` equality, every declared typed
    ///   column predicate (ADR-0093) -- goes to the prune-only
    ///   [`LogsPushdown::prune`] channel. `open_scan` passes that channel
    ///   separately from the exact predicate and the reader uses it for block
    ///   pruning ONLY, never per row, so DataFusion's residual stays the sole
    ///   exact evaluator and the filter must stay `Inexact`.
    ///
    /// A filter the extractor recognizes only in part (a `ts` bound AND-ed with
    /// an unrecognized sub-expression in one unsplit `Expr`) is `Inexact`, not
    /// partially credited, and so is one it recognizes nothing in.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // The distributed coordinator path fans out to workers without pushing
        // any filter (see `scan` below), so every filter it plans MUST come back
        // as a residual above the fan-out. Reporting `Exact` there would delete
        // a predicate nothing re-applies.
        #[cfg(feature = "flight-sql")]
        if self.distributed.is_some() {
            return Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()]);
        }
        Ok(filters
            .iter()
            .map(|f| {
                if filter_is_exact(f, self.declared.as_ref()) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Inexact
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Distributed coordinator path (ADR-0071; #326): fan out to worker slices
        // instead of scanning locally. The scan's `_limit` is honored as a
        // fetch-stop hint across the distributed partitions, with the exact limit
        // re-applied above the merge (over-fetch is safe, under-fetch is not).
        // Filters are re-applied above the returned plan by DataFusion (with a
        // coordinator installed, `supports_filters_pushdown` reports `Inexact`
        // for every filter for exactly this reason), so not pushing them to
        // workers only widens each worker's read, never changes a row. The
        // `logs` provider carries no
        // per-query byte budget (admission is decided at resolve time), so the
        // fan-out folds bytes into the query accounting under an `Unlimited`
        // ceiling, matching the local path.
        #[cfg(feature = "flight-sql")]
        if let Some(dist) = &self.distributed {
            let plan = distributed_logs_plan(
                dist.endpoints.clone(),
                Arc::clone(&dist.client),
                Arc::clone(&self.schema),
                _limit,
                self.phase_accounting.scan().clone(),
                ByteLimit::Unlimited,
            )?;
            return self.apply_projection(plan, projection);
        }

        let target_partitions = state.config().target_partitions();
        let pushdown = extract_logs(filters, self.declared.as_ref());
        // Projection pushdown reaches the reader (ADR-0087 decision 3): the
        // scan's output schema *is* the projection, and the resolved column set
        // stops the RLOG reader decoding the pages of columns nothing reads.
        // There is no `ProjectionExec` above the scan any more; one would have
        // dropped columns the scan had already paid to decode and materialize.
        //
        // The projection DataFusion hands us already contains every column its
        // residual `FilterExec` above this scan will read: an `Inexact` filter
        // survives above the scan, so the optimizer keeps its columns in the
        // scan's projection. An `Exact` one does not survive and its columns may
        // well be projected out, which is safe because `LogsScanExec` separately
        // adds the columns its own pushed content predicates and pending erasure
        // predicates need (`logs_scan::resolve_columns`); those are not visible
        // in the projection at all.
        self.build_scan(target_partitions, &pushdown, projection)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeSet;

    use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
    use datafusion::arrow::record_batch::RecordBatch;
    use proptest::prelude::*;
    use ravel_catalog::SegmentLevel;
    use ravel_logseg::writer::ObjectIdentity;
    use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use uuid::Uuid;

    use super::*;
    use crate::config::SqlConfig;
    use crate::memory::TenantMemoryAccountant;
    use crate::session::{SessionTable, build_session};

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            // Must match the `TenantHash([7u8; 16])` every provider in these
            // tests is constructed with: the RLOG read path enforces a footer
            // tenant_hash check (`fetch_accounted_with_tenant`, which
            // `LogsScanExec` calls), so an object whose footer names a different
            // tenant than the fetch fails closed with
            // `LogFetchError::Corrupt(IdentityMismatch("tenant_hash"))`.
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    fn s(v: &str) -> AttrValue {
        AttrValue::Str(v.to_string())
    }

    /// A record on the stream identified by `resource`, carrying per-record
    /// dynamic `attrs` (which win over resource/scope attributes on a key
    /// collision in the merged `attrs` column).
    fn record(
        resource: &[(String, AttrValue)],
        attrs: &[(String, AttrValue)],
        ts: i64,
        body: &str,
    ) -> LogRecord {
        LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(resource, "scope", "1.0", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: body.into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: attrs.to_vec(),
        }
    }

    /// Write one RLOG object from `records`, put it at `key`, and return a
    /// matching L0 `SegmentRef` carrying the object's true ts span.
    async fn write_object(store: &MemoryStore, key: &str, records: &[LogRecord]) -> SegmentRef {
        write_object_with(store, key, records, RlogConfig::default(), &[]).await
    }

    /// [`write_object`] with an explicit writer config and POSTINGS indexed
    /// field list (ADR-0049 decision 3: indexing is opt-in per field, so an
    /// object written with an empty list has no POSTINGS section at all and the
    /// prune channel has nothing to probe).
    async fn write_object_with(
        store: &MemoryStore,
        key: &str,
        records: &[LogRecord],
        cfg: RlogConfig,
        indexed: &[&str],
    ) -> SegmentRef {
        let mut w = RlogWriter::new(cfg, identity())
            .with_indexed_fields(indexed.iter().map(|s| s.to_string()).collect());
        for r in records {
            w.push(r.clone()).expect("push");
        }
        let bytes = w.finish().expect("finish");
        let size = bytes.len() as u64;
        store
            .put(key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put object");

        let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
        let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
        SegmentRef {
            data_object_key: key.to_string(),
            object_size: size,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            ingest_hour_bucket: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            shard: 0,
            content_hash: [0u8; 32],
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    /// A `SessionContext` built through the real production path
    /// (`crate::session::build_session`), the same one the SQL endpoint and
    /// Flight SQL use, with `provider` registered as `logs`. This drives the
    /// planner registration this crate adds, not a bespoke test-only session.
    fn logs_session(provider: LogsTableProvider) -> DFResult<datafusion::prelude::SessionContext> {
        let config = SqlConfig::default();
        let tenant = TenantMemoryAccountant::new(1 << 30);
        let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
        build_session(
            &config,
            pool,
            SessionTable::Logs(Arc::new(provider)),
            false,
            crate::session::SpillDecision::Disabled,
        )
    }

    /// Every test here selects exactly `SELECT ts, body FROM logs WHERE ...`,
    /// so `ts` and `body` are columns 0 and 1 of the projected result, not
    /// their positions in the full public `logs` schema.
    fn rows(batches: &[RecordBatch]) -> BTreeSet<(i64, String)> {
        let mut out = BTreeSet::new();
        for batch in batches {
            let ts = batch
                .column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts col");
            let body = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("body col");
            for i in 0..batch.num_rows() {
                out.insert((ts.value(i), body.value(i).to_string()));
            }
        }
        out
    }

    /// The `LogsScanExec` leaf of a physical plan, whose DataFusion metrics
    /// carry the block counters. The plan above it is whatever the optimizer
    /// built (a `FilterExec` for the residual, a `ProjectionExec`, possibly a
    /// repartition), so the leaf is found by walking rather than by shape.
    fn find_logs_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.name() == "LogsScanExec" {
            return Some(Arc::clone(plan));
        }
        plan.children().iter().find_map(|c| find_logs_scan(c))
    }

    /// What one executed query read at block granularity.
    #[derive(Debug, PartialEq)]
    struct ScanCounts {
        total: usize,
        scanned: usize,
        pruned_by_postings: usize,
    }

    /// Run `sql` end to end through the session and return both its rows and the
    /// scan's block counters. Asserting on the counters is the point: rows alone
    /// cannot distinguish "the prune worked" from "the residual saved us".
    async fn run_counted(
        ctx: &datafusion::prelude::SessionContext,
        sql: &str,
    ) -> (BTreeSet<(i64, String)>, ScanCounts) {
        let plan = ctx
            .sql(sql)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .expect("collect");
        let metrics = find_logs_scan(&plan)
            .expect("a LogsScanExec leaf")
            .metrics()
            .expect("the scan publishes metrics");
        let count = |name: &str| {
            metrics
                .sum_by_name(name)
                .map(|v| v.as_usize())
                .unwrap_or_else(|| panic!("metric {name} missing"))
        };
        (
            rows(&batches),
            ScanCounts {
                total: count("blocks_total"),
                scanned: count("blocks_scanned"),
                pruned_by_postings: count("blocks_pruned_by_postings"),
            },
        )
    }

    /// One record per block, so block counts in the tests below are exact and
    /// legible rather than a function of the default 8192-record target.
    fn one_record_per_block() -> RlogConfig {
        RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        }
    }

    /// Twelve records on one stream, ts 1..=12, each carrying a per-record
    /// `request.id = "r<ts>"` and a per-record `other.key = "same"`. Both keys
    /// are per-record only, which is what the prune can actually act on: a key
    /// that also appears at resource level is declined on a version 1 object,
    /// and a resource-only key has no FIELD_DIR column to key a posting by
    ///.
    fn per_record_key_records() -> Vec<LogRecord> {
        let worker = vec![("service.name".to_string(), s("worker"))];
        (1..=12)
            .map(|ts| {
                record(
                    &worker,
                    &[
                        ("request.id".to_string(), s(&format!("r{ts}"))),
                        ("other.key".to_string(), s("same")),
                    ],
                    ts,
                    &format!("body {ts}"),
                )
            })
            .collect()
    }

    /// The acceptance test: the same SQL query, with and without an
    /// extractable prune arm, returns identical rows while reading a different
    /// number of blocks.
    ///
    /// The two queries are `attrs['request.id'] = 'r5'` (extracted into
    /// `LogsPushdown::prune`, so it reaches POSTINGS) and the same equality
    /// OR-ed with an equality on a key no record carries. The second shape is
    /// deliberately unextractable: `extract_logs` recognizes no disjunction, so
    /// its prune channel is empty. Its rows are the same, because
    /// `attrs['absent.key']` is NULL on every row and `FALSE OR NULL` is NULL
    /// (filtered) while `TRUE OR NULL` is TRUE (kept). So the pair differs in
    /// exactly one thing: whether the prune reached the index.
    #[tokio::test]
    async fn attrs_equality_prunes_blocks_on_the_sql_path() {
        let store = MemoryStore::new();
        let records = per_record_key_records();
        let seg = write_object_with(
            &store,
            "logs/postings.rlog",
            &records,
            one_record_per_block(),
            &["request.id"],
        )
        .await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");

        let (pruned_rows, pruned) = run_counted(
            &ctx,
            "SELECT ts, body FROM logs WHERE attrs['request.id'] = 'r5'",
        )
        .await;
        let (plain_rows, plain) = run_counted(
            &ctx,
            "SELECT ts, body FROM logs \
             WHERE attrs['request.id'] = 'r5' OR attrs['absent.key'] = 'zzz'",
        )
        .await;

        // Identical rows. This is the invariant the prune may never touch.
        let expected = BTreeSet::from([(5, "body 5".to_string())]);
        assert_eq!(pruned_rows, expected);
        assert_eq!(plain_rows, expected);
        assert_eq!(pruned_rows, plain_rows, "the prune changed no row");

        // Strictly fewer blocks read, and the difference is POSTINGS' work.
        assert_eq!(
            plain,
            ScanCounts {
                total: 12,
                scanned: 12,
                pruned_by_postings: 0,
            },
            "with no prune arm the scan decodes every block"
        );
        assert_eq!(
            pruned,
            ScanCounts {
                total: 12,
                scanned: 1,
                pruned_by_postings: 11,
            },
            "the prune arm reached POSTINGS and left one block"
        );
        assert!(
            pruned.scanned < plain.scanned,
            "the whole point: {} blocks read instead of {}",
            pruned.scanned,
            plain.scanned
        );
    }

    /// An equality on a per-record key that exists but was never named as an
    /// indexed field prunes nothing, and still returns every matching row. The
    /// probe reports "no information" for a field POSTINGS does not cover, which
    /// is widen-only (ADR-0013): the fetch reads the whole object and the
    /// residual answers, exactly as before this channel existed.
    #[tokio::test]
    async fn prune_arm_on_unindexed_field_prunes_nothing_on_the_sql_path() {
        let store = MemoryStore::new();
        let records = per_record_key_records();
        let seg = write_object_with(
            &store,
            "logs/unindexed.rlog",
            &records,
            one_record_per_block(),
            // `request.id` is indexed; `other.key` deliberately is not.
            &["request.id"],
        )
        .await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");

        let (rows_got, counts) = run_counted(
            &ctx,
            "SELECT ts, body FROM logs WHERE attrs['other.key'] = 'same'",
        )
        .await;

        let expected: BTreeSet<(i64, String)> =
            (1..=12).map(|ts| (ts, format!("body {ts}"))).collect();
        assert_eq!(rows_got, expected, "every matching record is returned");
        assert_eq!(
            counts,
            ScanCounts {
                total: 12,
                scanned: 12,
                pruned_by_postings: 0,
            },
            "an unindexed prune arm prunes nothing"
        );
    }

    /// The soundness canary with the index actually loaded: `service.name` is
    /// indexed here, and one record carries it only as a resource attribute. A
    /// version 2 POSTINGS section indexes the merged view (ADR-0049 amendment),
    /// so the prune both bites (fewer blocks) and keeps that resource-only row.
    /// If the prune ever reached the per-record layer alone, ts=4 would vanish.
    #[tokio::test]
    async fn prune_on_an_indexed_resource_level_key_keeps_the_resource_only_row() {
        let store = MemoryStore::new();
        let worker = vec![("service.name".to_string(), s("worker"))];
        let records = vec![
            // Resource `worker`, overridden per-record to `api`: record wins.
            record(
                &worker,
                &[("service.name".to_string(), s("api"))],
                1,
                "override",
            ),
            record(&worker, &[], 2, "worker only"),
            record(&worker, &[], 3, "worker only again"),
            // `api` as a genuine resource attribute, no per-record attrs at all.
            record(
                &[("service.name".to_string(), s("api"))],
                &[],
                4,
                "resource",
            ),
        ];
        let seg = write_object_with(
            &store,
            "logs/resource-level.rlog",
            &records,
            one_record_per_block(),
            &["service.name"],
        )
        .await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");

        let (rows_got, counts) = run_counted(
            &ctx,
            "SELECT ts, body FROM logs WHERE attrs['service.name'] = 'api'",
        )
        .await;

        assert_eq!(
            rows_got,
            BTreeSet::from([(1, "override".to_string()), (4, "resource".to_string())]),
            "the resource-only match (ts=4) must survive the prune"
        );
        assert_eq!(
            counts,
            ScanCounts {
                total: 4,
                scanned: 2,
                pruned_by_postings: 2,
            },
            "the merged-view index prunes the two worker-only blocks"
        );
    }

    /// The acceptance test: `attrs['k'] = 'v'` must plan (the whole
    /// point of registering `crate::map_field_planner::MapFieldAccessPlanner`)
    /// and, once planned, filter to exactly the matching records over the
    /// merged, record-wins `attrs` column (ADR-0033).
    ///
    /// Four records on one stream:
    /// - ts=1: resource `service.name = "worker"`, overridden by a per-record
    ///   `service.name = "api"` -- the record-wins collision case.
    /// - ts=2: no `service.name` anywhere; a key that exists ONLY in
    ///   per-record attrs (`request.id`).
    /// - ts=3: resource `service.name = "worker"`, no override -- must not
    ///   match `service.name = 'api'`.
    /// - ts=4: resource `service.name = "api"` genuinely (no per-record attrs
    ///   at all) -- the plain top-level case.
    #[tokio::test]
    async fn attrs_subscript_plans_and_filters_correctly() {
        let store = MemoryStore::new();

        let worker = vec![("service.name".to_string(), s("worker"))];
        let records = vec![
            record(
                &worker,
                &[("service.name".to_string(), s("api"))],
                1,
                "hello match world",
            ),
            record(
                &worker,
                &[("request.id".to_string(), s("abc123"))],
                2,
                "record only",
            ),
            record(&worker, &[], 3, "no match here"),
            record(
                &[("service.name".to_string(), s("api"))],
                &[],
                4,
                "another match example",
            ),
        ];
        let seg = write_object(&store, "logs/attrs.rlog", &records).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");

        // Planning: `attrs['service.name'] = 'api'` must not error with
        // "GetFieldAccess not supported".
        let df = ctx
            .sql("SELECT ts, body FROM logs WHERE attrs['service.name'] = 'api'")
            .await
            .expect("attrs['k'] = 'v' must plan");
        let batches = df.collect().await.expect("collect");
        assert_eq!(
            rows(&batches),
            BTreeSet::from([
                (1, "hello match world".to_string()),
                (4, "another match example".to_string()),
            ]),
            "must keep the record-wins override (ts=1) and the plain top-level \
             match (ts=4), and exclude the non-matching stream (ts=3)"
        );
    }

    /// Predicate shapes that must NOT be extracted into a fetch prune:
    /// an inequality, an `OR` across different
    /// keys, a `NOT`, and a comparison against a non-literal. `extract_logs`
    /// emits nothing for any of them (see
    /// `crate::logs_pushdown::tests::non_extractable_attribute_shapes_contribute_nothing`),
    /// so each must still return correct results purely from DataFusion's
    /// residual over the merged `attrs` column. This is the end-to-end proof
    /// that leaving them to the residual is correct.
    ///
    /// Three records on distinct streams:
    /// - ts=1: resource `service.name=api`, `region=us`.
    /// - ts=2: resource `service.name=worker`, `region=eu`.
    /// - ts=3: resource `service.name=api`, `region=eu`, per-record override
    ///   `service.name=cron` (record wins in the merged map).
    #[tokio::test]
    async fn residual_handles_non_pushed_attribute_shapes() {
        let store = MemoryStore::new();
        let records = vec![
            record(
                &[
                    ("service.name".to_string(), s("api")),
                    ("region".to_string(), s("us")),
                ],
                &[],
                1,
                "one",
            ),
            record(
                &[
                    ("service.name".to_string(), s("worker")),
                    ("region".to_string(), s("eu")),
                ],
                &[],
                2,
                "two",
            ),
            record(
                &[
                    ("service.name".to_string(), s("api")),
                    ("region".to_string(), s("eu")),
                ],
                &[("service.name".to_string(), s("cron"))],
                3,
                "three",
            ),
        ];
        let seg = write_object(&store, "logs/shapes.rlog", &records).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");

        async fn bodies(ctx: &datafusion::prelude::SessionContext, sql: &str) -> BTreeSet<String> {
            let df = ctx.sql(sql).await.expect("plan");
            let batches = df.collect().await.expect("collect");
            rows(&batches).into_iter().map(|(_, b)| b).collect()
        }

        // Inequality: merged service.name is api / worker / cron; != 'api'
        // keeps worker (ts=2) and the record-wins cron (ts=3).
        assert_eq!(
            bodies(
                &ctx,
                "SELECT ts, body FROM logs WHERE attrs['service.name'] != 'api'"
            )
            .await,
            BTreeSet::from(["two".to_string(), "three".to_string()]),
        );

        // OR across different keys: service.name='api' (ts=1) OR region='eu'
        // (ts=2, ts=3) covers all three.
        assert_eq!(
            bodies(
                &ctx,
                "SELECT ts, body FROM logs \
                 WHERE attrs['service.name'] = 'api' OR attrs['region'] = 'eu'",
            )
            .await,
            BTreeSet::from(["one".to_string(), "two".to_string(), "three".to_string()]),
        );

        // NOT an equality: everything whose merged service.name is not worker.
        assert_eq!(
            bodies(
                &ctx,
                "SELECT ts, body FROM logs WHERE NOT attrs['service.name'] = 'worker'",
            )
            .await,
            BTreeSet::from(["one".to_string(), "three".to_string()]),
        );

        // Comparison against a non-literal (attr vs attr): no record has
        // service.name equal to its region.
        assert!(
            bodies(
                &ctx,
                "SELECT ts, body FROM logs WHERE attrs['service.name'] = attrs['region']",
            )
            .await
            .is_empty(),
        );
    }

    /// A subscript on a key that exists nowhere in the merged map returns no
    /// rows, not a planning or execution error.
    #[tokio::test]
    async fn attrs_subscript_on_missing_key_returns_no_rows() {
        let store = MemoryStore::new();
        let worker = vec![("service.name".to_string(), s("worker"))];
        let records = vec![record(&worker, &[], 1, "irrelevant")];
        let seg = write_object(&store, "logs/missing.rlog", &records).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");

        let df = ctx
            .sql("SELECT ts, body FROM logs WHERE attrs['does.not.exist'] = 'v'")
            .await
            .expect("must still plan");
        let batches = df.collect().await.expect("collect");
        assert!(
            rows(&batches).is_empty(),
            "a missing key must filter out every row, not error"
        );
    }

    /// A key present only in per-record attributes (never in the resource
    /// stream attrs) is still reachable through the subscript.
    #[tokio::test]
    async fn attrs_subscript_matches_record_only_key() {
        let store = MemoryStore::new();
        let worker = vec![("service.name".to_string(), s("worker"))];
        let records = vec![
            record(
                &worker,
                &[("request.id".to_string(), s("abc123"))],
                2,
                "record only",
            ),
            record(&worker, &[], 3, "no request id"),
        ];
        let seg = write_object(&store, "logs/record-only.rlog", &records).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");

        let df = ctx
            .sql("SELECT ts, body FROM logs WHERE attrs['request.id'] = 'abc123'")
            .await
            .expect("must plan");
        let batches = df.collect().await.expect("collect");
        assert_eq!(
            rows(&batches),
            BTreeSet::from([(2, "record only".to_string())])
        );
    }

    /// `attrs['k'] = 'v'` combined with a `ts` range and `has_word` still
    /// plans and returns correct results: the new planner must not disturb
    /// the existing ts/content pushdown paths.
    #[tokio::test]
    async fn attrs_subscript_combines_with_ts_range_and_has_word() {
        let store = MemoryStore::new();
        let worker = vec![("service.name".to_string(), s("worker"))];
        let records = vec![
            record(
                &worker,
                &[("service.name".to_string(), s("api"))],
                1,
                "hello match world",
            ),
            record(&worker, &[], 3, "no match here"),
            record(
                &[("service.name".to_string(), s("api"))],
                &[],
                4,
                "another match example",
            ),
            // Outside the ts range below even though it would otherwise match.
            record(
                &[("service.name".to_string(), s("api"))],
                &[],
                100,
                "far away match",
            ),
        ];
        let seg = write_object(&store, "logs/combined.rlog", &records).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");

        let df = ctx
            .sql(
                "SELECT ts, body FROM logs \
                 WHERE ts >= TIMESTAMP '1970-01-01 00:00:00.000000001' \
                 AND ts <= TIMESTAMP '1970-01-01 00:00:00.000000004' \
                 AND attrs['service.name'] = 'api' \
                 AND has_word(body, 'match')",
            )
            .await
            .expect("must plan with ts range and has_word together");
        let batches = df.collect().await.expect("collect");
        assert_eq!(
            rows(&batches),
            BTreeSet::from([
                (1, "hello match world".to_string()),
                (4, "another match example".to_string()),
            ]),
            "ts=3 fails the attrs filter, ts=100 fails the ts range"
        );
    }

    /// (ADR-0064 decision 3): a pending selective-erasure
    /// request on the resolved snapshot excludes matching rows through the real
    /// `LogsTableProvider` scan path, the one the SQL `logs` table uses in
    /// production. This covers `LogsTableProvider::build_scan` passing the
    /// snapshot-derived predicates into `LogsScanExec` (logs_provider.rs) and
    /// `LogsScanExec` calling `LogQuery::with_erasure` before fetch
    /// (logs_scan.rs); reverting `.with_erasure((*erasure).clone())` back to a
    /// bare `LogQuery::new(ts_min, ts_max)` in logs_scan.rs makes the erased row
    /// reappear.
    #[tokio::test]
    async fn pending_erasure_excludes_matching_rows_on_the_sql_path() {
        let store = MemoryStore::new();
        let worker = vec![("service.name".to_string(), s("worker"))];
        let records = vec![
            record(&worker, &[("user_id".to_string(), s("u1"))], 1, "erase me"),
            record(&worker, &[("user_id".to_string(), s("u2"))], 2, "keep me"),
        ];
        let seg = write_object(&store, "logs/erasure.rlog", &records).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);

        let request = ravel_proto::commit::v1::ErasureRequest {
            predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
                key: "user_id".to_string(),
                value: "u1".to_string(),
            }],
            ..Default::default()
        };
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: vec![request],
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");
        let df = ctx.sql("SELECT ts, body FROM logs").await.expect("plan");
        let batches = df.collect().await.expect("collect");
        assert_eq!(
            rows(&batches),
            BTreeSet::from([(2, "keep me".to_string())]),
            "u1's row is erased; u2's row survives"
        );
    }

    /// (ADR-0064): a subject named ONLY in a RESOURCE/scope
    /// (`stream_attrs`) attribute must also be excluded. The `attrs` column
    /// materializes the merged resource + scope + record view, so `user_id` is
    /// queryable, yet the fetcher-level filter (`retain_log_records`) matches
    /// per-record attributes alone and never sees it. Before the scan-layer
    /// `retain_unerased` in `logs_scan.rs::prepare_partition`, the erased row
    /// leaked through this `SELECT`; removing that call reintroduces the leak.
    #[tokio::test]
    async fn pending_erasure_excludes_resource_attribute_rows_on_the_sql_path() {
        let store = MemoryStore::new();
        // `user_id` lives in the RESOURCE position (stream_attrs), not the
        // per-record `attrs`, on two distinct streams.
        let erased_resource = vec![
            ("service.name".to_string(), s("worker")),
            ("user_id".to_string(), s("u1")),
        ];
        let kept_resource = vec![
            ("service.name".to_string(), s("worker")),
            ("user_id".to_string(), s("u2")),
        ];
        let records = vec![
            record(&erased_resource, &[], 1, "erase me"),
            record(&kept_resource, &[], 2, "keep me"),
        ];
        let seg = write_object(&store, "logs/erasure-resource.rlog", &records).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);

        let request = ravel_proto::commit::v1::ErasureRequest {
            predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
                key: "user_id".to_string(),
                value: "u1".to_string(),
            }],
            ..Default::default()
        };
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: vec![request],
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");
        let df = ctx.sql("SELECT ts, body FROM logs").await.expect("plan");
        let batches = df.collect().await.expect("collect");
        assert_eq!(
            rows(&batches),
            BTreeSet::from([(2, "keep me".to_string())]),
            "the resource-only subject u1 is erased; u2's row survives"
        );
    }

    /// A window-scoped resource-attribute erasure: the predicate carries a
    /// half-open `[start, end)` event-time window, so only the in-window record
    /// of the matching stream is excluded and the out-of-window record on the
    /// same stream survives. Exercises the `p.ts_in_window(r.ts_ns)` arm of the
    /// scan-layer filter against the merged (resource) attribute view.
    #[tokio::test]
    async fn windowed_erasure_on_resource_attribute_excludes_only_in_window_rows() {
        let store = MemoryStore::new();
        let resource = vec![
            ("service.name".to_string(), s("worker")),
            ("user_id".to_string(), s("u1")),
        ];
        // ts=5 falls inside [2, 8); ts=10 falls outside it. Both carry the same
        // resource-level user_id=u1.
        let records = vec![
            record(&resource, &[], 5, "in window"),
            record(&resource, &[], 10, "out of window"),
        ];
        let seg = write_object(&store, "logs/erasure-window.rlog", &records).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);

        let request = ravel_proto::commit::v1::ErasureRequest {
            predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
                key: "user_id".to_string(),
                value: "u1".to_string(),
            }],
            window_start_ns: 2,
            window_end_ns: 8,
            ..Default::default()
        };
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: vec![request],
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");
        let df = ctx.sql("SELECT ts, body FROM logs").await.expect("plan");
        let batches = df.collect().await.expect("collect");
        assert_eq!(
            rows(&batches),
            BTreeSet::from([(10, "out of window".to_string())]),
            "only the in-window (ts=5) row is erased; ts=10 survives"
        );
    }

    // --- declared typed column pushdown (ADR-0093) ---------------------------

    use crate::declared::{DeclaredColumn, DeclaredType};
    use crate::logs_pushdown::extract_logs;

    /// Twelve one-record blocks on one stream, ts 1..=12, each carrying a
    /// per-record I64 `status_code = ts * 100`. The skip index folds a `NumStat`
    /// for the (status_code, I64) column with no POSTINGS indexing needed.
    fn i64_code_records() -> Vec<LogRecord> {
        let worker = vec![("service.name".to_string(), s("worker"))];
        (1..=12)
            .map(|ts| {
                record(
                    &worker,
                    &[("status_code".to_string(), AttrValue::I64(ts * 100))],
                    ts,
                    &format!("body {ts}"),
                )
            })
            .collect()
    }

    fn i64_status_code() -> Vec<DeclaredColumn> {
        vec![DeclaredColumn::new("status_code", DeclaredType::I64)]
    }

    /// TEST 1: a selective I64 comparison on a declared column reduces
    /// `blocks_scanned` through the skip index (#331), following #331's own
    /// counter-assertion pattern. `status_code >= 1100` keeps only the two
    /// blocks whose code is 1100/1200; the other ten never decode.
    #[tokio::test]
    async fn declared_i64_comparison_reduces_blocks_scanned() {
        let store = MemoryStore::new();
        let seg = write_object_with(
            &store,
            "logs/decl-i64.rlog",
            &i64_code_records(),
            one_record_per_block(),
            &[],
        )
        .await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        )
        .with_declared_columns(i64_status_code());
        let ctx = logs_session(provider).expect("build session");

        let (rows_got, counts) =
            run_counted(&ctx, "SELECT ts, body FROM logs WHERE status_code >= 1100").await;
        assert_eq!(
            rows_got,
            BTreeSet::from([(11, "body 11".to_string()), (12, "body 12".to_string())]),
        );
        assert_eq!(
            counts,
            ScanCounts {
                total: 12,
                scanned: 2,
                pruned_by_postings: 0,
            },
            "the skip index leaves only the two in-range blocks"
        );
        assert!(
            counts.scanned < counts.total,
            "the numeric prune reduced blocks_scanned"
        );
    }

    /// TEST 2: a selective declared-Str equality reduces `blocks_pruned_by_postings`
    /// via POSTINGS, matching the existing `attrs['k']='v'` test's assertion shape.
    /// `region` is indexed and only ts=5 carries `region = 'eu'`.
    #[tokio::test]
    async fn declared_str_equality_prunes_via_postings() {
        let store = MemoryStore::new();
        let worker = vec![("service.name".to_string(), s("worker"))];
        let records: Vec<LogRecord> = (1..=12)
            .map(|ts| {
                let region = if ts == 5 { "eu" } else { "us" };
                record(
                    &worker,
                    &[("region".to_string(), s(region))],
                    ts,
                    &format!("body {ts}"),
                )
            })
            .collect();
        let seg = write_object_with(
            &store,
            "logs/decl-str.rlog",
            &records,
            one_record_per_block(),
            &["region"],
        )
        .await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        )
        .with_declared_columns(vec![DeclaredColumn::new("region", DeclaredType::Str)]);
        let ctx = logs_session(provider).expect("build session");

        let (rows_got, counts) =
            run_counted(&ctx, "SELECT ts, body FROM logs WHERE region = 'eu'").await;
        assert_eq!(rows_got, BTreeSet::from([(5, "body 5".to_string())]));
        assert_eq!(
            counts,
            ScanCounts {
                total: 12,
                scanned: 1,
                pruned_by_postings: 11,
            },
            "the declared-Str equality reached POSTINGS and left one block"
        );
    }

    /// TEST 3: the same query with an extractable prune arm and without one
    /// returns identical rows while reading a different number of blocks. The
    /// pruned query is `status_code = 500`; the plain query OR-s it with a
    /// body equality no record satisfies, an unextractable disjunction across
    /// two columns, so `extract_logs` yields no prune arm and the scan decodes
    /// every block. `FALSE OR FALSE` filters those rows in the residual, so the
    /// rows are identical: the pair differs only in whether the prune bit.
    #[tokio::test]
    async fn declared_i64_prune_changes_no_row() {
        let store = MemoryStore::new();
        let seg = write_object_with(
            &store,
            "logs/decl-diff.rlog",
            &i64_code_records(),
            one_record_per_block(),
            &[],
        )
        .await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        )
        .with_declared_columns(i64_status_code());
        let ctx = logs_session(provider).expect("build session");

        let (pruned_rows, pruned) =
            run_counted(&ctx, "SELECT ts, body FROM logs WHERE status_code = 500").await;
        let (plain_rows, plain) = run_counted(
            &ctx,
            "SELECT ts, body FROM logs WHERE status_code = 500 OR body = 'zzz'",
        )
        .await;

        let expected = BTreeSet::from([(5, "body 5".to_string())]);
        assert_eq!(pruned_rows, expected);
        assert_eq!(plain_rows, expected);
        assert_eq!(pruned_rows, plain_rows, "the prune changed no row");
        assert_eq!(plain.scanned, 12, "the OR shape decodes every block");
        assert_eq!(pruned.scanned, 1, "the equality prune leaves one block");
        assert!(pruned.scanned < plain.scanned);
    }

    /// TEST 4: a predicate on a declared column absent from some objects (a
    /// tenant that added the column later) still returns correct results. Object
    /// A carries `status_code`; object B does not. The absence rule (a block with
    /// no stat is never pruned, ADR-0013) is already proven at the reader; this
    /// exercises it through this ADR's NEW extraction call site. Object B's two
    /// blocks must all be SCANNED (not pruned by a stat they lack), which the
    /// counter proves: a bug pruning no-stat objects would drop `scanned` to 2.
    #[tokio::test]
    async fn declared_column_absent_from_some_objects_returns_correct_results() {
        let store = MemoryStore::new();
        let worker = vec![("service.name".to_string(), s("worker"))];
        let seg_a = write_object_with(
            &store,
            "logs/decl-a.rlog",
            &i64_code_records(),
            one_record_per_block(),
            &[],
        )
        .await;
        // Object B predates the column: ts 13/14, no `status_code` anywhere.
        let b_records = vec![
            record(&worker, &[], 13, "body 13"),
            record(&worker, &[], 14, "body 14"),
        ];
        let seg_b = write_object_with(
            &store,
            "logs/decl-b.rlog",
            &b_records,
            one_record_per_block(),
            &[],
        )
        .await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg_a, seg_b],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        )
        .with_declared_columns(i64_status_code());
        let ctx = logs_session(provider).expect("build session");

        let (rows_got, counts) =
            run_counted(&ctx, "SELECT ts, body FROM logs WHERE status_code >= 1100").await;
        // B's rows have a NULL status_code, so the residual `>= 1100` drops them.
        assert_eq!(
            rows_got,
            BTreeSet::from([(11, "body 11".to_string()), (12, "body 12".to_string())]),
        );
        assert_eq!(
            counts,
            ScanCounts {
                total: 14,
                scanned: 4,
                pruned_by_postings: 0,
            },
            "A's ten out-of-range blocks prune; B's two no-stat blocks must NOT"
        );
    }

    /// TEST 5: an `IN (v1, v2, v3)` over a declared I64 column returns correct
    /// results even though the envelope range is coarser than the exact set. A
    /// value strictly between the IN set's min and max but not a member exists
    /// in the data. The envelope range keeps its block (the prune alone cannot
    /// exclude it), so correctness rests on the Inexact residual excluding it.
    /// Proven by the final row set.
    ///
    /// Checkpoint review found the original two-value form of this test
    /// (`IN (200, 800)`) does not exercise `declared_in_list_predicate`
    /// (`logs_pushdown.rs`) at all: DataFusion's own simplifier rewrites a
    /// two-element `IN` into `col = a OR col = b` before the scan sees it, so
    /// the test was actually driving `declared_i64_or_envelope`'s OR handling,
    /// not the `Expr::InList` arm its own doc names. A five-element `IN`
    /// survives as a real `InList` through the optimizer (confirmed by
    /// inspecting the optimized `TableScan.filters`), so this uses five
    /// values to close that gap. Mutating the InList envelope down to a
    /// single point reddens this test (5 of 6 matching rows silently
    /// dropped); the two-value form stayed green under that same mutation.
    #[tokio::test]
    async fn declared_i64_in_list_envelope_is_corrected_by_residual() {
        let store = MemoryStore::new();
        let worker = vec![("service.name".to_string(), s("worker"))];
        let codes = [100i64, 200, 300, 500, 700, 800, 900];
        let records: Vec<LogRecord> = codes
            .iter()
            .enumerate()
            .map(|(i, &code)| {
                let ts = i as i64 + 1;
                record(
                    &worker,
                    &[("status_code".to_string(), AttrValue::I64(code))],
                    ts,
                    &format!("body {ts}"),
                )
            })
            .collect();
        let seg = write_object_with(
            &store,
            "logs/decl-in.rlog",
            &records,
            one_record_per_block(),
            &[],
        )
        .await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        )
        .with_declared_columns(i64_status_code());
        let ctx = logs_session(provider).expect("build session");

        let (rows_got, counts) = run_counted(
            &ctx,
            "SELECT ts, body FROM logs WHERE status_code IN (200, 300, 700, 800)",
        )
        .await;
        // ts=4 (code 500) is inside the envelope [200, 800] but not in the set:
        // the residual must exclude it, never the prune.
        assert_eq!(
            rows_got,
            BTreeSet::from([
                (2, "body 2".to_string()),
                (3, "body 3".to_string()),
                (5, "body 5".to_string()),
                (6, "body 6".to_string()),
            ]),
            "the in-envelope non-member (code 500) is excluded by the residual"
        );
        assert_eq!(
            counts,
            ScanCounts {
                total: 7,
                scanned: 5,
                pruned_by_postings: 0,
            },
            "codes 100 and 900 prune; the envelope keeps 200/300/500/700/800's blocks"
        );
    }

    /// TEST 8 (CRITICAL): the decline of `!=` and of a type-mismatched literal
    /// must hold against the filters DataFusion's optimizer actually hands to
    /// `TableProvider::scan`, not a hand-built `Expr`. A type-coercion pass can
    /// rewrite `status_code > 2.5` into a `Cast`-wrapped comparison before the
    /// extractor sees it; the extractor must still decline (a `Cast` is not a bare
    /// `Expr::Column`, so resolution fails). Built over a real
    /// `LogsTableProvider`/session and read off the optimized `LogicalPlan`.
    #[tokio::test]
    async fn declined_shapes_decline_on_real_optimized_filters() {
        use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
        use datafusion::logical_expr::LogicalPlan;

        fn table_scan_filters(plan: &LogicalPlan) -> Vec<Expr> {
            if let LogicalPlan::TableScan(ts) = plan {
                return ts.filters.clone();
            }
            for input in plan.inputs() {
                let f = table_scan_filters(input);
                if !f.is_empty() {
                    return f;
                }
            }
            Vec::new()
        }

        fn contains_cast(e: &Expr) -> bool {
            let mut found = false;
            e.apply(|node| {
                if matches!(node, Expr::Cast(_) | Expr::TryCast(_)) {
                    found = true;
                    Ok(TreeNodeRecursion::Stop)
                } else {
                    Ok(TreeNodeRecursion::Continue)
                }
            })
            .expect("walk");
            found
        }

        let store = MemoryStore::new();
        let seg = write_object_with(
            &store,
            "logs/decl-real.rlog",
            &i64_code_records(),
            one_record_per_block(),
            &[],
        )
        .await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fetcher = LogSegmentFetcher::new(store);
        let snapshot = Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            PhaseAccounting::new(),
        )
        .with_declared_columns(i64_status_code());
        let ctx = logs_session(provider).expect("build session");
        let declared = i64_status_code();

        // `!=` on a declared column: the optimizer keeps it a `NotEq`, not in the
        // comparison allowlist, so no prune arm.
        let plan = ctx
            .sql("SELECT ts, body FROM logs WHERE status_code != 500")
            .await
            .expect("plan")
            .into_optimized_plan()
            .expect("optimize");
        let ne_filters = table_scan_filters(&plan);
        assert!(
            !ne_filters.is_empty(),
            "the != predicate must reach the scan"
        );
        assert!(
            extract_logs(&ne_filters, &declared).prune.is_empty(),
            "!= must produce no prune arm on the real optimized filter"
        );

        // Type-mismatched literal: the optimizer coerces `status_code > 2.5` by
        // casting the Int64 column to Float64. The extractor must decline on the
        // Cast-wrapped operand it actually receives.
        let plan = ctx
            .sql("SELECT ts, body FROM logs WHERE status_code > 2.5")
            .await
            .expect("plan")
            .into_optimized_plan()
            .expect("optimize");
        let cast_filters = table_scan_filters(&plan);
        assert!(
            !cast_filters.is_empty(),
            "the mismatched comparison must reach the scan"
        );
        assert!(
            cast_filters.iter().any(contains_cast),
            "DataFusion is expected to insert a Cast for the Int64-vs-Float64 comparison"
        );
        assert!(
            extract_logs(&cast_filters, &declared).prune.is_empty(),
            "a Cast-wrapped comparison must produce no prune arm"
        );
    }

    // --- exact filter pushdown (issue #733) ----------------------------------

    /// A provider over an empty snapshot: `supports_filters_pushdown` is a pure
    /// function of the filter and the declared vocabulary, so it needs no data
    /// and issues no I/O.
    fn pushdown_provider(declared: Vec<DeclaredColumn>) -> LogsTableProvider {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        LogsTableProvider::new(
            Snapshot {
                segments: Vec::new(),
                segments_pruned: 0,
                pending_erasure: Vec::new(),
            },
            TenantHash([7u8; 16]),
            LogSegmentFetcher::new(store),
            PhaseAccounting::new(),
        )
        .with_declared_columns(declared)
    }

    fn pushdown_for(provider: &LogsTableProvider, filter: &Expr) -> TableProviderFilterPushDown {
        let mut v = provider
            .supports_filters_pushdown(&[filter])
            .expect("supports_filters_pushdown");
        assert_eq!(v.len(), 1, "one verdict per filter");
        v.remove(0)
    }

    fn ts_lit(v: i64) -> Expr {
        datafusion::prelude::lit(datafusion::scalar::ScalarValue::TimestampNanosecond(
            Some(v),
            None,
        ))
    }

    /// Every filter that resolves purely to a `ts` bound and/or a `has_word`
    /// call is `Exact`: both land in the channel `ravel_logseg::reader`'s `eval`
    /// re-verifies against the row's own value, so nothing is left for a
    /// residual.
    #[test]
    fn pure_ts_bound_and_has_word_filters_are_exact() {
        use datafusion::prelude::{col, lit};

        use crate::logs_udf::has_word_udf;

        let p = pushdown_provider(Vec::new());
        let between = Expr::Between(datafusion::logical_expr::Between {
            expr: Box::new(col("ts")),
            negated: false,
            low: Box::new(ts_lit(100)),
            high: Box::new(ts_lit(200)),
        });
        let cases = [
            col("ts").gt_eq(ts_lit(100)),
            col("ts").lt(ts_lit(200)),
            col("ts").eq(ts_lit(150)),
            // A flipped operand order is the same bound.
            ts_lit(100).lt_eq(col("ts")),
            between,
            // Two ts bounds AND-ed inside ONE unsplit filter expression.
            col("ts")
                .gt_eq(ts_lit(100))
                .and(col("ts").lt_eq(ts_lit(200))),
            has_word_udf().call(vec![col("body"), lit("timeout")]),
            has_word_udf().call(vec![col("severity_text"), lit("error")]),
            // A ts bound AND a content predicate together, unsplit.
            col("ts")
                .gt_eq(ts_lit(100))
                .and(has_word_udf().call(vec![col("body"), lit("timeout")])),
        ];
        for filter in cases {
            assert_eq!(
                pushdown_for(&p, &filter),
                TableProviderFilterPushDown::Exact,
                "must be Exact: {filter:?}"
            );
        }
    }

    /// Everything the extractor routes to the prune-only channel stays
    /// `Inexact`: the reader uses a prune arm for block pruning only and never
    /// evaluates it per row, so DataFusion's residual is the sole exact
    /// evaluator. An `attrs['k'] = 'v'` map equality and a declared typed column
    /// predicate (ADR-0093) are both in that channel.
    #[test]
    fn prune_channel_filters_stay_inexact() {
        use datafusion::functions::core::expr_fn::get_field;
        use datafusion::prelude::{col, lit};

        let p = pushdown_provider(i64_status_code());
        let cases = [
            get_field(col("attrs"), "service.name").eq(lit("api")),
            col("status_code").eq(lit(500i64)),
            col("status_code").gt(lit(500i64)),
            col("status_code").in_list(vec![lit(200i64), lit(404i64)], false),
        ];
        for filter in cases {
            assert_eq!(
                pushdown_for(&p, &filter),
                TableProviderFilterPushDown::Inexact,
                "a prune-channel filter must stay Inexact: {filter:?}"
            );
        }
    }

    /// A filter the extractor recognizes only in part is `Inexact`, never
    /// partially credited. Reporting `Exact` deletes the WHOLE filter from the
    /// plan, so a `ts` bound AND-ed with anything not itself exactly verified
    /// would silently drop that other conjunct's rows.
    ///
    /// DataFusion normally splits top-level `AND`s into separate filters before
    /// pushdown, so these compound shapes are a fail-closed guard rather than a
    /// shape seen every day.
    #[test]
    fn partially_recognized_compound_filters_are_inexact() {
        use datafusion::functions::core::expr_fn::get_field;
        use datafusion::prelude::{col, lit};

        let p = pushdown_provider(i64_status_code());
        let like = Expr::Like(datafusion::logical_expr::Like {
            negated: false,
            expr: Box::new(col("body")),
            pattern: Box::new(lit("%time%")),
            escape_char: None,
            case_insensitive: false,
        });
        let cases = [
            // A ts bound AND an attrs-map equality: the equality is prune-only.
            col("ts")
                .gt_eq(ts_lit(100))
                .and(get_field(col("attrs"), "k").eq(lit("v"))),
            // A ts bound AND a declared-column predicate: likewise prune-only.
            col("ts")
                .gt_eq(ts_lit(100))
                .and(col("status_code").eq(lit(500i64))),
            // A ts bound AND a shape the extractor recognizes in NEITHER
            // channel: `LIKE` is deliberately never pushed (soundness, see
            // crate::logs_pushdown), so the residual must keep it.
            col("ts").gt_eq(ts_lit(100)).and(like),
        ];
        for filter in cases {
            assert_eq!(
                pushdown_for(&p, &filter),
                TableProviderFilterPushDown::Inexact,
                "a partially recognized filter must be Inexact: {filter:?}"
            );
        }
    }

    /// An expression the extractor recognizes nothing in keeps the unchanged
    /// `Inexact` default. It must never fall through to `Exact`.
    #[test]
    fn unrecognized_filters_stay_inexact() {
        use datafusion::prelude::{col, lit};

        use crate::logs_udf::has_word_udf;

        let p = pushdown_provider(Vec::new());
        let negated_between = Expr::Between(datafusion::logical_expr::Between {
            expr: Box::new(col("ts")),
            negated: true,
            low: Box::new(ts_lit(100)),
            high: Box::new(ts_lit(200)),
        });
        let cases = [
            // Not in the ts comparison allowlist.
            col("ts").not_eq(ts_lit(100)),
            Expr::Not(Box::new(col("ts").gt_eq(ts_lit(100)))),
            negated_between,
            // An integer literal is an ambiguous ts scale and is rejected.
            col("ts").gt_eq(lit(100i64)),
            // `has_word` over a column with no field selector.
            has_word_udf().call(vec![col("attrs"), lit("timeout")]),
            // A fixed column with no extraction path at all.
            col("severity_num").eq(lit(5i64)),
            // A disjunction of two ts bounds is not one range.
            datafusion::logical_expr::or(col("ts").gt_eq(ts_lit(100)), col("ts").lt(ts_lit(10))),
        ];
        for filter in cases {
            assert_eq!(
                pushdown_for(&p, &filter),
                TableProviderFilterPushDown::Inexact,
                "an unrecognized filter must stay Inexact: {filter:?}"
            );
        }
    }

    /// The verdicts line up with the filters positionally, so a mixed set is
    /// reported per filter rather than collapsed to one answer.
    #[test]
    fn verdicts_are_reported_per_filter_in_order() {
        use datafusion::functions::core::expr_fn::get_field;
        use datafusion::prelude::{col, lit};

        let p = pushdown_provider(Vec::new());
        let ts = col("ts").gt_eq(ts_lit(100));
        let attrs = get_field(col("attrs"), "k").eq(lit("v"));
        let hi = col("ts").lt_eq(ts_lit(200));
        let verdicts = p
            .supports_filters_pushdown(&[&ts, &attrs, &hi])
            .expect("supports_filters_pushdown");
        assert_eq!(
            verdicts,
            vec![
                TableProviderFilterPushDown::Exact,
                TableProviderFilterPushDown::Inexact,
                TableProviderFilterPushDown::Exact,
            ]
        );
    }

    // --- per-key attrs projection (issue #1768) -----------------------------
    //
    // These pin what `AttrsPerKeyProjection` buys: an `attrs['k']` query stays
    // on the columnar path (`rowpath_batches == 0`), decodes stored page bytes
    // within a tight band of the declared-column form (only that key's
    // FIELD_DIR pages plus `attrs_raw`, not every dynamic column), and moves the
    // SAME wire bytes and GETs as the declared form. `SELECT attrs`/`SELECT *`
    // are left on the row path unchanged.

    use ravel_query::PhaseAccounting;

    /// A deterministic high-entropy value for record `i`'s key `k`. Repeated
    /// padding would compress to nothing under zstd and leave every per-key
    /// column tiny; this splat-mixes `(k, i)` into 96 hex chars so each column
    /// carries real, incompressible page bytes and the decode band is a
    /// meaningful measurement.
    fn cell(k: usize, i: usize) -> String {
        let mut x = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (k as u64).wrapping_mul(0xD1B5_4A32_D192_ED03);
        let mut out = String::with_capacity(96);
        for _ in 0..6 {
            x ^= x >> 33;
            x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
            x ^= x >> 33;
            out.push_str(&format!("{x:016x}"));
        }
        out
    }

    /// `rows` records on one stream, each carrying `keys` high-cardinality Str
    /// record attributes `k0..k{keys-1}`, so each attribute's FIELD_DIR column
    /// holds real page bytes and selecting one key versus several is a
    /// measurable decode difference. All keys fit the default dynamic-column
    /// budget, so no block overflows into `attrs_raw` and the columnar path is
    /// available.
    fn wide_attr_records(rows: usize, keys: usize) -> Vec<LogRecord> {
        let resource = vec![("service.name".to_string(), s("api"))];
        (0..rows)
            .map(|i| {
                let attrs: Vec<(String, AttrValue)> = (0..keys)
                    .map(|k| (format!("k{k}"), s(&cell(k, i))))
                    .collect();
                record(&resource, &attrs, i as i64 + 1, &format!("body {i}"))
            })
            .collect()
    }

    /// What one executed `logs` query cost and returned, enough to prove which
    /// path it took and how much it decoded.
    struct PerKeyMeasured {
        /// Column index 1 as text, accepting the map form's `Utf8` and the
        /// declared form's `Dictionary(Int32, Utf8)`.
        col1: Vec<Option<String>>,
        page_bytes_decoded: u64,
        wire_bytes: u64,
        gets: u64,
        columnar_batches: usize,
        rowpath_batches: usize,
    }

    /// Column index 1 of every batch as `Option<String>`, in batch order,
    /// accepting the map form's `Utf8` and the declared form's
    /// `Dictionary(Int32, Utf8)`. Returns an empty vec when column 1 is neither
    /// (a group-by count, a `SELECT *` timestamp): callers only compare it for
    /// the shapes whose second column is the string value.
    fn col1_strings(batches: &[RecordBatch]) -> Vec<Option<String>> {
        use datafusion::arrow::array::{Array, DictionaryArray};
        use datafusion::arrow::datatypes::Int32Type;
        let mut out = Vec::new();
        for b in batches {
            if b.num_columns() < 2 {
                return Vec::new();
            }
            let col = b.column(1);
            if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
                out.extend((0..a.len()).map(|i| a.is_valid(i).then(|| a.value(i).to_string())));
            } else if let Some(d) = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
                let values = d
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 dictionary values");
                out.extend((0..d.len()).map(|i| {
                    d.is_valid(i)
                        .then(|| values.value(d.keys().value(i) as usize).to_string())
                }));
            } else {
                return Vec::new();
            }
        }
        out
    }

    /// Run `sql` against a FRESH provider/session over `seg` (so its accounting
    /// is this query's alone), returning both the plan's columnar/row-path batch
    /// counters and the phase accounting's decode/wire/request figures.
    async fn measure_logs_query(
        store: &Arc<dyn ObjectStoreBackend>,
        seg: &SegmentRef,
        declared: Vec<DeclaredColumn>,
        sql: &str,
    ) -> PerKeyMeasured {
        let fetcher = LogSegmentFetcher::new(Arc::clone(store));
        let snapshot = Snapshot {
            segments: vec![seg.clone()],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let phase = PhaseAccounting::new();
        let provider =
            LogsTableProvider::new(snapshot, TenantHash([7u8; 16]), fetcher, phase.clone())
                .with_declared_columns(declared);
        let ctx = logs_session(provider).expect("build session");
        let plan = ctx
            .sql(sql)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .expect("collect");
        let metrics = find_logs_scan(&plan)
            .expect("a LogsScanExec leaf")
            .metrics()
            .expect("the scan publishes metrics");
        let count = |name: &str| metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
        let snap = phase.pooled_snapshot();
        PerKeyMeasured {
            col1: col1_strings(&batches),
            page_bytes_decoded: snap.page_bytes_decoded,
            wire_bytes: snap.total_s3_bytes(),
            gets: snap.total_s3_requests(),
            columnar_batches: count("columnar_batches"),
            rowpath_batches: count("rowpath_batches"),
        }
    }

    /// Write the wide fixture once and return its segment.
    async fn wide_attr_segment(store: &MemoryStore) -> SegmentRef {
        write_object_with(
            store,
            "logs/per-key.rlog",
            &wide_attr_records(200, 10),
            RlogConfig::default(),
            &[],
        )
        .await
    }

    fn k3_declared() -> Vec<DeclaredColumn> {
        vec![DeclaredColumn::new("k3", DeclaredType::Str)]
    }

    /// The band the acceptance criteria pin: the map form's stored page bytes
    /// decoded stay within 1.5x of the declared form's, both directions.
    fn within_band(map: u64, declared: u64) -> bool {
        let hi = declared.saturating_mul(3) / 2;
        let lo = map.saturating_mul(3) / 2;
        map <= hi && declared <= lo
    }

    /// The equality shape: `WHERE attrs['k3'] = 'v'`. The map form takes the
    /// columnar path, decodes within 1.5x of the declared column's stored page
    /// bytes, and moves the same wire bytes and GETs.
    #[tokio::test]
    async fn attrs_equality_takes_columnar_path_within_the_decode_band() {
        let store = MemoryStore::new();
        let seg = wide_attr_segment(&store).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let value = cell(3, 50);

        let map = measure_logs_query(
            &store,
            &seg,
            k3_declared(),
            &format!("SELECT ts, attrs['k3'] AS v FROM logs WHERE attrs['k3'] = '{value}'"),
        )
        .await;
        let dec = measure_logs_query(
            &store,
            &seg,
            k3_declared(),
            &format!("SELECT ts, \"k3\" AS v FROM logs WHERE \"k3\" = '{value}'"),
        )
        .await;

        // The whole point: the map form stayed columnar.
        assert_eq!(
            map.rowpath_batches, 0,
            "attrs['k3'] equality must not fall to the row path"
        );
        assert!(
            map.columnar_batches > 0,
            "attrs['k3'] equality must build columnar batches"
        );
        // Same one matching row on both forms.
        assert_eq!(map.col1.len(), 1, "exactly one row matches");
        assert_eq!(map.col1, dec.col1, "map and declared select the same row");
        // Stored page bytes decoded within 1.5x of the declared column's.
        assert!(
            within_band(map.page_bytes_decoded, dec.page_bytes_decoded),
            "map decoded {} stored page bytes, declared {}, outside the 1.5x band",
            map.page_bytes_decoded,
            dec.page_bytes_decoded
        );
        // Wire bytes and GETs are unchanged: the fetch reads the whole object
        // either way under the stock policy.
        assert!(dec.gets > 0, "the scan issued at least one GET");
        assert_eq!(map.gets, dec.gets, "GET count must be unchanged");
        assert_eq!(
            map.wire_bytes, dec.wire_bytes,
            "wire bytes must be unchanged"
        );
    }

    /// The group-by shape: `GROUP BY attrs['k3']` stays columnar and within the
    /// decode band of the declared column.
    #[tokio::test]
    async fn attrs_group_by_takes_columnar_path_within_the_decode_band() {
        let store = MemoryStore::new();
        let seg = wide_attr_segment(&store).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

        let map = measure_logs_query(
            &store,
            &seg,
            k3_declared(),
            "SELECT attrs['k3'] AS v, COUNT(*) AS n FROM logs GROUP BY attrs['k3']",
        )
        .await;
        let dec = measure_logs_query(
            &store,
            &seg,
            k3_declared(),
            "SELECT \"k3\" AS v, COUNT(*) AS n FROM logs GROUP BY \"k3\"",
        )
        .await;

        assert_eq!(map.rowpath_batches, 0, "group-by must not fall to row path");
        assert!(
            map.columnar_batches > 0,
            "group-by must build columnar batches"
        );
        assert!(
            within_band(map.page_bytes_decoded, dec.page_bytes_decoded),
            "group-by map decoded {} stored page bytes, declared {}, outside the 1.5x band",
            map.page_bytes_decoded,
            dec.page_bytes_decoded
        );
        assert!(dec.gets > 0, "the scan issued at least one GET");
        assert_eq!(map.gets, dec.gets, "GET count must be unchanged");
        assert_eq!(
            map.wire_bytes, dec.wire_bytes,
            "wire bytes must be unchanged"
        );
    }

    /// The project-limit shape: `SELECT ts, attrs['k3'] ... ORDER BY ts LIMIT k`
    /// stays columnar and within the decode band of the declared column.
    #[tokio::test]
    async fn attrs_project_limit_takes_columnar_path_within_the_decode_band() {
        let store = MemoryStore::new();
        let seg = wide_attr_segment(&store).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

        let map = measure_logs_query(
            &store,
            &seg,
            k3_declared(),
            "SELECT ts, attrs['k3'] AS v FROM logs ORDER BY ts LIMIT 10",
        )
        .await;
        let dec = measure_logs_query(
            &store,
            &seg,
            k3_declared(),
            "SELECT ts, \"k3\" AS v FROM logs ORDER BY ts LIMIT 10",
        )
        .await;

        assert_eq!(
            map.rowpath_batches, 0,
            "project-limit must not fall to row path"
        );
        assert!(
            map.columnar_batches > 0,
            "project-limit must build columnar batches"
        );
        assert_eq!(map.col1.len(), 10, "LIMIT 10 rows");
        assert_eq!(map.col1, dec.col1, "map and declared agree row for row");
        assert!(
            within_band(map.page_bytes_decoded, dec.page_bytes_decoded),
            "project-limit map decoded {} stored page bytes, declared {}, outside band",
            map.page_bytes_decoded,
            dec.page_bytes_decoded
        );
        assert!(dec.gets > 0, "the scan issued at least one GET");
        assert_eq!(map.gets, dec.gets, "GET count must be unchanged");
        assert_eq!(
            map.wire_bytes, dec.wire_bytes,
            "wire bytes must be unchanged"
        );
    }

    /// The band is tight, not vacuous: the correct single-key form is inside it,
    /// but selecting ONE extra column (`attrs['k4']` alongside `attrs['k3']`)
    /// decodes that column's pages too and breaks the band against the same
    /// declared baseline. Deliberately over-selecting is the line that makes
    /// this fail; the extra column is `attrs['k4']`.
    #[tokio::test]
    async fn the_decode_band_fails_when_one_extra_column_is_selected() {
        let store = MemoryStore::new();
        let seg = wide_attr_segment(&store).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

        let dec = measure_logs_query(
            &store,
            &seg,
            k3_declared(),
            "SELECT ts, \"k3\" AS v FROM logs ORDER BY ts",
        )
        .await;
        let one_key = measure_logs_query(
            &store,
            &seg,
            k3_declared(),
            "SELECT ts, attrs['k3'] AS v FROM logs ORDER BY ts",
        )
        .await;
        // The deliberately over-selecting variant: one extra per-key column.
        let two_keys = measure_logs_query(
            &store,
            &seg,
            k3_declared(),
            "SELECT ts, attrs['k3'] AS v, attrs['k4'] AS w FROM logs ORDER BY ts",
        )
        .await;

        assert!(
            within_band(one_key.page_bytes_decoded, dec.page_bytes_decoded),
            "one key ({}) must be inside the band of declared ({})",
            one_key.page_bytes_decoded,
            dec.page_bytes_decoded
        );
        assert!(
            !within_band(two_keys.page_bytes_decoded, dec.page_bytes_decoded),
            "two keys ({}) must break the band of declared ({}): the band is tight",
            two_keys.page_bytes_decoded,
            dec.page_bytes_decoded
        );
    }

    /// `SELECT attrs` and `SELECT *` still need the whole map, so the rule
    /// leaves them on the row path unchanged: no columnar batches at all.
    #[tokio::test]
    async fn whole_map_projections_stay_on_the_row_path() {
        let store = MemoryStore::new();
        let seg = wide_attr_segment(&store).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

        for sql in ["SELECT attrs FROM logs", "SELECT * FROM logs"] {
            let m = measure_logs_query(&store, &seg, k3_declared(), sql).await;
            assert_eq!(
                m.columnar_batches, 0,
                "{sql} needs the whole map and must stay on the row path"
            );
            assert!(m.rowpath_batches > 0, "{sql} must build row-path batches");
        }
    }

    /// Count the scan's per-segment timeline points (`seg_open_start_offset`,
    /// `seg_open_ready_offset`, `seg_done_offset`) on the plan `sql` produces,
    /// with `segment_timing` turned on.
    async fn segment_timing_points(
        store: &Arc<dyn ObjectStoreBackend>,
        seg: &SegmentRef,
        sql: &str,
    ) -> usize {
        let snapshot = Snapshot {
            segments: vec![seg.clone()],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            LogSegmentFetcher::new(Arc::clone(store)),
            PhaseAccounting::new(),
        )
        .with_declared_columns(k3_declared())
        .with_segment_timing(true);
        let ctx = logs_session(provider).expect("build session");
        let plan = ctx
            .sql(sql)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .expect("collect");
        find_logs_scan(&plan)
            .expect("a LogsScanExec leaf")
            .metrics()
            .expect("the scan publishes metrics")
            .iter()
            .filter(|m| m.value().name().starts_with("seg_"))
            .count()
    }

    /// The per-key rewrite rebuilds the scan through `reproject_attr_keys`, and
    /// a builder field dropped there is invisible: the query still answers
    /// correctly and the instrument just reports nothing, which reads as "this
    /// shape has no per-segment cost" rather than "the instrument was dropped".
    ///
    /// Comparing against the declared form is what makes it a check rather than
    /// a guess: both plans scan the same segment, so the rewritten one must
    /// publish the same number of timeline points. Dropping
    /// `.with_segment_timing(self.segment_timing)` from `reproject_attr_keys`
    /// takes the map form to 0 and fails this.
    #[tokio::test]
    async fn the_per_key_rewrite_keeps_the_segment_timeline() {
        let store = MemoryStore::new();
        let seg = wide_attr_segment(&store).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

        let declared = segment_timing_points(
            &store,
            &seg,
            "SELECT ts, \"k3\" AS v FROM logs ORDER BY ts LIMIT 10",
        )
        .await;
        let per_key = segment_timing_points(
            &store,
            &seg,
            "SELECT ts, attrs['k3'] AS v FROM logs ORDER BY ts LIMIT 10",
        )
        .await;

        assert!(
            declared > 0,
            "the declared form must publish timeline points for this to compare against"
        );
        assert_eq!(
            per_key, declared,
            "the per-key rewrite must carry segment_timing through \
             reproject_attr_keys: got {per_key} points against {declared}"
        );
    }

    // ---- Plan-time segment skipping by declared-column statistics ----------
    // ADR-2121 D1: `build_scan` drops a segment whose exact declared-column
    // min/max proves a prune-only `NumRange` arm excludes all of its rows.

    /// A pass-through backend that counts GETs per object key, so a test can
    /// state that one specific data object was never fetched.
    struct KeyCountingStore {
        inner: Arc<dyn ObjectStoreBackend>,
        gets: std::sync::Mutex<std::collections::HashMap<String, u64>>,
    }

    impl KeyCountingStore {
        fn new(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
            Arc::new(KeyCountingStore {
                inner,
                gets: std::sync::Mutex::new(std::collections::HashMap::new()),
            })
        }

        fn gets_of(&self, key: &str) -> u64 {
            self.gets
                .lock()
                .expect("gets lock")
                .get(key)
                .copied()
                .unwrap_or(0)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for KeyCountingStore {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
            self.inner.put(key, data, opts).await
        }

        async fn get(
            &self,
            key: &str,
            range: ravel_object_store::GetRange,
        ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
            *self
                .gets
                .lock()
                .expect("gets lock")
                .entry(key.to_string())
                .or_insert(0) += 1;
            self.inner.get(key, range).await
        }

        async fn head(
            &self,
            key: &str,
        ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            ravel_object_store::Capabilities {
                multipart: false,
                ..self.inner.capabilities()
            }
        }
    }

    /// The declared I64 column the statistics tests constrain.
    const CODE: &str = "code";
    /// A second declared I64 column, carried as `1` on every record and never
    /// stamped, for the cross-column `OR` shape.
    const OTHER: &str = "other";

    fn code_declared() -> Vec<DeclaredColumn> {
        vec![
            DeclaredColumn::new(CODE, crate::declared::DeclaredType::I64),
            DeclaredColumn::new(OTHER, crate::declared::DeclaredType::I64),
        ]
    }

    /// One record per value at ts `seq * 100 + i`, with `code` set to the value
    /// (absent, so NULL, for `None`) and `other = 1`.
    fn code_records(seq: u64, values: &[Option<i64>]) -> Vec<LogRecord> {
        let resource = vec![("service.name".to_string(), s("api"))];
        values
            .iter()
            .enumerate()
            .map(|(i, v)| {
                let mut attrs = vec![(OTHER.to_string(), AttrValue::I64(1))];
                if let Some(v) = v {
                    attrs.push((CODE.to_string(), AttrValue::I64(*v)));
                }
                let ts = seq as i64 * 100 + i as i64;
                record(&resource, &attrs, ts, &format!("seg {seq} row {i}"))
            })
            .collect()
    }

    /// The exact stamp a correct writer produces for `values` on [`CODE`].
    fn code_stamp(values: &[Option<i64>]) -> ravel_types::declared_stats::DeclaredColumnStat {
        use ravel_types::declared_stats::{
            DeclaredColumnStat, DeclaredStatType, DeclaredStatValue,
        };
        let present: Vec<i64> = values.iter().flatten().copied().collect();
        DeclaredColumnStat::new(
            CODE,
            DeclaredStatType::I64,
            present.iter().min().map(|v| DeclaredStatValue::I64(*v)),
            present.iter().max().map(|v| DeclaredStatValue::I64(*v)),
            (values.len() - present.len()) as u64,
        )
        .expect("valid stamp")
    }

    /// `seg` carrying `stamps` the way resolution delivers them: through the
    /// commit-record carrier read, whose row-count clauses bind against the
    /// segment's own `sample_count`.
    fn with_stamps(
        seg: SegmentRef,
        stamps: &[ravel_types::declared_stats::DeclaredColumnStat],
    ) -> SegmentRef {
        let mut rec = ravel_proto::commit::v1::CommitRecord {
            sample_count: seg.sample_count,
            ..Default::default()
        };
        ravel_commit::declared_stats::stamp_commit_record(&mut rec, stamps);
        let validated = ravel_commit::declared_stats::read_commit_record(&rec);
        assert_eq!(
            validated.covered().len(),
            stamps.len(),
            "test setup: every stamp must pass the carrier read"
        );
        SegmentRef {
            declared_column_stats: ravel_catalog::DeclaredColumnStats::from_validated(&validated),
            ..seg
        }
    }

    /// A `.cstat` entry on [`CODE`] claiming `min`/`max` over `non_null` of
    /// `rows` rows.
    fn code_cstat(
        rows: u64,
        non_null: u64,
        min: Option<i64>,
        max: Option<i64>,
    ) -> ravel_proto::catalog::v1::ColumnStat {
        use ravel_proto::catalog::v1::column_value::Kind;
        let value = |v: i64| ravel_proto::catalog::v1::ColumnValue {
            kind: Some(Kind::I64(v)),
        };
        ravel_proto::catalog::v1::ColumnStat {
            name: CODE.to_string(),
            declared_type: 2, // ravel.sys.v1.TypedAttrColumnType::I64
            non_null_count: non_null,
            null_count: rows - non_null,
            min: min.map(value),
            max: max.map(value),
            dictionary_present: false,
            dictionary: Vec::new(),
            sum: None,
        }
    }

    /// The exact `.cstat` entry for `values`.
    fn exact_cstat(values: &[Option<i64>]) -> ravel_proto::catalog::v1::ColumnStat {
        let present: Vec<i64> = values.iter().flatten().copied().collect();
        code_cstat(
            values.len() as u64,
            present.len() as u64,
            present.iter().min().copied(),
            present.iter().max().copied(),
        )
    }

    /// Loaded column statistics carrying `entries`, keyed by content hash the
    /// way a v3 `.cstat` load keys them.
    fn loaded_cstats(
        entries: Vec<(&SegmentRef, ravel_proto::catalog::v1::ColumnStat)>,
    ) -> Arc<LoadedColumnStats> {
        let by_content_hash = entries
            .into_iter()
            .map(|(seg, stat)| {
                (
                    seg.content_hash,
                    ravel_proto::catalog::v1::ColumnStatsSegment {
                        ingest_hour_bucket: seg.ingest_hour_bucket,
                        shard: seg.shard,
                        writer_id: seg.writer_id.as_bytes().to_vec(),
                        writer_epoch: seg.writer_epoch,
                        writer_seq: seg.writer_seq,
                        columns: vec![stat],
                    },
                )
            })
            .collect();
        Arc::new(LoadedColumnStats {
            segments: std::collections::HashMap::new(),
            by_content_hash,
            part_blake3: Vec::new(),
        })
    }

    /// Write `values` as one real RLOG object at `key` and return its unstamped
    /// L0 `SegmentRef`, with a content hash and writer sequence of its own so a
    /// `.cstat` entry joins to exactly this segment.
    async fn write_code_segment(
        store: &dyn ObjectStoreBackend,
        key: &str,
        seq: u64,
        values: &[Option<i64>],
    ) -> SegmentRef {
        let records = code_records(seq, values);
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        for r in &records {
            w.push(r.clone()).expect("push");
        }
        let bytes = w.finish().expect("finish");
        let content_hash = *blake3::hash(&bytes).as_bytes();
        let size = bytes.len() as u64;
        store
            .put(key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put object");
        SegmentRef {
            data_object_key: key.to_string(),
            object_size: size,
            min_event_ts_ns: records.iter().map(|r| r.ts_ns).min().expect("nonempty"),
            max_event_ts_ns: records.iter().map(|r| r.ts_ns).max().expect("nonempty"),
            ingest_hour_bucket: 0,
            sample_count: records.len() as u64,
            series_count: 0,
            shard: 0,
            content_hash,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: seq,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Default::default(),
        }
    }

    /// What one statistics-pruning run returned and fetched.
    struct PruneRun {
        rows: BTreeSet<(i64, String)>,
        /// The scan's `segments_pruned_by_stats`, or `None` when the optimizer
        /// folded the filter to a constant `false` and planned no scan at all.
        scan_pruned_by_stats: Option<usize>,
        store: Arc<KeyCountingStore>,
    }

    impl PruneRun {
        fn pruned_by_stats(&self) -> usize {
            self.scan_pruned_by_stats
                .expect("the plan carries a LogsScanExec")
        }
    }

    /// Run `sql` through the production session over `segments` stored in
    /// `inner`, with statistics pruning on or off, counting GETs per key.
    async fn run_pruned(
        inner: &Arc<dyn ObjectStoreBackend>,
        segments: Vec<SegmentRef>,
        column_stats: Option<Arc<LoadedColumnStats>>,
        stats_pruning: bool,
        sql: &str,
    ) -> PruneRun {
        let store = KeyCountingStore::new(Arc::clone(inner));
        let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as _;
        let provider = LogsTableProvider::new(
            Snapshot {
                segments,
                segments_pruned: 0,
                pending_erasure: Vec::new(),
            },
            TenantHash([7u8; 16]),
            LogSegmentFetcher::new(backend),
            PhaseAccounting::pooled_over(&QueryAccounting::new()),
        )
        .with_declared_columns(code_declared())
        .with_column_stats(column_stats)
        .with_stats_pruning(stats_pruning);
        let ctx = logs_session(provider).expect("session");
        let plan = ctx
            .sql(sql)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let batches = datafusion::physical_plan::collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .expect("collect");
        let scan_pruned_by_stats = find_logs_scan(&plan).map(|scan| {
            scan.metrics()
                .expect("the scan publishes metrics")
                .sum_by_name("segments_pruned_by_stats")
                .expect("segments_pruned_by_stats is registered")
                .as_usize()
        });
        PruneRun {
            rows: rows(&batches),
            scan_pruned_by_stats,
            store,
        }
    }

    /// The row a test expects: `(ts, body)` for row `i` of segment `seq`.
    fn code_row(seq: u64, i: usize) -> (i64, String) {
        (seq as i64 * 100 + i as i64, format!("seg {seq} row {i}"))
    }

    /// Write `values` as a real RLOG object plus its commit record, stamped
    /// with its exact [`CODE`] statistics when `stamped`, so `SqlExecutor`'s
    /// own catalog resolve delivers the segment and its stamp.
    async fn publish_code_segment(
        store: &dyn ObjectStoreBackend,
        tenant: &ravel_types::TenantId,
        seq: u64,
        values: &[Option<i64>],
        stamped: bool,
    ) -> String {
        use ravel_commit::publish::RetryPolicy;
        use ravel_commit::record::NewCommitRecord;
        use ravel_commit::{keys, publish, record};
        let records = code_records(seq, values);
        let writer_id = Uuid::from_u128(0x2121);
        let mut w = RlogWriter::new(
            RlogConfig::default(),
            ObjectIdentity {
                tenant_hash: tenant.hash().0,
                shard: 0,
                writer_id: *writer_id.as_bytes(),
                writer_epoch: 1,
                writer_seq: seq,
            },
        );
        for r in &records {
            w.push(r.clone()).expect("push");
        }
        let bytes = w.finish().expect("finish");
        let mut rec = record::build(NewCommitRecord {
            tenant_hash: tenant.hash(),
            signal: ravel_types::Signal::Logs,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq,
            object_size: bytes.len() as u64,
            content_hash: *blake3::hash(&bytes).as_bytes(),
            sample_count: records.len() as u64,
            series_count: 1,
            min_event_ts_ns: records.iter().map(|r| r.ts_ns).min().expect("nonempty"),
            max_event_ts_ns: records.iter().map(|r| r.ts_ns).max().expect("nonempty"),
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("commit record");
        if stamped {
            ravel_commit::declared_stats::stamp_commit_record(&mut rec, &[code_stamp(values)]);
        }
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
        data_key
    }

    /// The acceptance test for ADR-2121 D1, through `SqlExecutor::execute`,
    /// the funnel the SQL endpoint uses (catalog resolve, then this provider's
    /// `scan`).
    ///
    /// Five segments, every one resolved from a real commit record. Three are
    /// stamped with `code` ranges disjoint from `code = 201` (`[100, 102]`,
    /// `[300, 302]`, and one whose `code` is NULL in every row, which the stamp
    /// proves with `null_count == sample_count`). One stamped `[200, 202]`
    /// holds a match. The fifth also holds a `201` but carries no stamp, so its
    /// column is declined and it must be read.
    ///
    /// Flipped assertions: making `prune_segments_by_stats` return its input
    /// unchanged (skips nothing) fails the zero-GET assertion on the
    /// `[100, 102]` object and the count (`0 != 3`). Making `arm_excludes`
    /// answer `true` for a declined column (`let Some(coverage) = .. else {
    /// return true }`) skips the unstamped segment and fails the row assertion,
    /// which loses its `201`, and the count (`4 != 3`).
    #[tokio::test]
    async fn a_segment_whose_stats_exclude_the_predicate_is_never_fetched() {
        const NOW_NS: i64 = 4 * 3_600_000_000_000;
        let memory: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = ravel_types::TenantId::new("stats-prune-acceptance".to_string());
        let low = publish_code_segment(
            memory.as_ref(),
            &tenant,
            1,
            &[Some(100), Some(101), Some(102)],
            true,
        )
        .await;
        let hit = publish_code_segment(
            memory.as_ref(),
            &tenant,
            2,
            &[Some(200), Some(201), Some(202)],
            true,
        )
        .await;
        let high = publish_code_segment(
            memory.as_ref(),
            &tenant,
            3,
            &[Some(300), Some(301), Some(302)],
            true,
        )
        .await;
        let all_null =
            publish_code_segment(memory.as_ref(), &tenant, 4, &[None, None, None], true).await;
        let unstamped = publish_code_segment(
            memory.as_ref(),
            &tenant,
            5,
            &[Some(150), Some(201), Some(250)],
            false,
        )
        .await;

        let store = KeyCountingStore::new(Arc::clone(&memory));
        let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as _;
        let catalog = Arc::new(
            ravel_catalog::Catalog::new(
                Arc::clone(&backend),
                ravel_catalog::CatalogConfig::default(),
            )
            .expect("catalog"),
        );
        let executor = crate::executor::SqlExecutor::new(
            catalog,
            ravel_query::SegmentFetcher::new(Arc::clone(&backend)),
            LogSegmentFetcher::new(Arc::clone(&backend)),
            crate::spans_fetcher::SpanSegmentFetcher::new(Arc::clone(&backend)),
            SqlConfig::default(),
            1 << 30,
        )
        .with_declared_column_source(Arc::new(
            crate::declared::StaticDeclaredColumns::new(code_declared()),
        ));
        let outcome = executor
            .execute(
                tenant.hash(),
                &crate::executor::SqlRequest {
                    sql: "SELECT ts, body FROM logs WHERE code = 201".to_string(),
                    window: ravel_types::TimeRange {
                        start_ns: 0,
                        end_ns: NOW_NS,
                    },
                    min_tokens: Vec::new(),
                    now_ns: NOW_NS,
                    deadline: std::time::Duration::from_secs(30),
                    row_window: false,
                    max_rows: None,
                    budgets: None,
                },
            )
            .await
            .expect("execute");

        assert_eq!(
            rows(outcome.output.batches()),
            BTreeSet::from([code_row(2, 1), code_row(5, 1)]),
            "exactly the two code = 201 rows, one of them in the unstamped segment"
        );
        assert_eq!(outcome.stats.segments, 5, "the resolve sees every segment");
        for (name, key) in [("low", &low), ("high", &high), ("all-NULL", &all_null)] {
            assert_eq!(
                store.gets_of(key),
                0,
                "the {name} segment's stats exclude code = 201, so its object is never fetched"
            );
        }
        for (name, key) in [("matching", &hit), ("unstamped", &unstamped)] {
            assert!(
                store.gets_of(key) > 0,
                "the {name} segment must still be read"
            );
        }
        assert_eq!(
            outcome.stats.segments_pruned_by_stats, 3,
            "segments_pruned_by_stats counts the three excluded segments"
        );
    }

    /// Carriers that disagree about one segment decline its column, and so do
    /// no carriers at all: neither segment is skipped, although each one's
    /// stamp alone, or its `.cstat` alone, would exclude `code = 500`. A
    /// segment covered by an exact `.cstat` entry and no stamp is skipped, which
    /// shows the `.cstat` side of the union is live in this test.
    ///
    /// Flipped assertion: ignoring a disagreement (`if !stamp.agrees_with(&cstat)`
    /// in `segment_declared_coverage` to `if false`) skips the conflicting
    /// segment on its stamp and fails the count (`2 != 1`).
    #[tokio::test]
    async fn conflicting_or_absent_carriers_never_skip_a_segment() {
        let memory: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let values = [Some(200), Some(201), Some(202)];
        let conflict = write_code_segment(memory.as_ref(), "conflict", 1, &values).await;
        let conflict = with_stamps(conflict, &[code_stamp(&values)]);
        let uncovered = write_code_segment(memory.as_ref(), "uncovered", 2, &values).await;
        let cstat_only = write_code_segment(memory.as_ref(), "cstat-only", 3, &values).await;
        // The `.cstat` claims [100, 102] for an object holding [200, 202]:
        // valid on its own, and in disagreement with the exact stamp.
        let stats = loaded_cstats(vec![
            (&conflict, code_cstat(3, 3, Some(100), Some(102))),
            (&cstat_only, exact_cstat(&values)),
        ]);

        let run = run_pruned(
            &memory,
            vec![conflict, uncovered, cstat_only],
            Some(stats),
            true,
            "SELECT ts, body FROM logs WHERE code = 500",
        )
        .await;
        assert!(run.rows.is_empty(), "no row holds code = 500");
        assert_eq!(
            run.pruned_by_stats(),
            1,
            "only the segment with one exact carrier is skipped"
        );
        assert!(
            run.store.gets_of("conflict") > 0,
            "conflicting carriers decline the column, so the segment is read"
        );
        assert!(
            run.store.gets_of("uncovered") > 0,
            "a segment with no stamp and no .cstat entry is read"
        );
        assert_eq!(run.store.gets_of("cstat-only"), 0);
    }

    /// Shapes `extract_logs` declines skip nothing, even where a naive reading
    /// of the literal would exclude the segment: `code != 5` and `NOT (code =
    /// 5)` over a segment whose every `code` is `5`, and a cross-column `OR`
    /// (`other = 1 OR code = 3`) whose `code` disjunct is disjoint from the
    /// segment while its `other` disjunct matches every row. A control statement on the same segment
    /// (`code = 6`) is skipped, so the stamps are usable here.
    ///
    /// Flipped assertion: dropping `declared_i64_or_envelope`'s same-column
    /// check turns the cross-column `OR` into a `[1, 3]` envelope on `code`,
    /// which skips the segment and loses all three rows.
    #[tokio::test]
    async fn declined_shapes_skip_nothing() {
        let memory: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let values = [Some(5), Some(5), Some(5)];
        let seg = write_code_segment(memory.as_ref(), "fives", 1, &values).await;
        let seg = with_stamps(seg, &[code_stamp(&values)]);
        let every_row = BTreeSet::from([code_row(1, 0), code_row(1, 1), code_row(1, 2)]);

        for (sql, want) in [
            ("SELECT ts, body FROM logs WHERE code != 5", BTreeSet::new()),
            (
                "SELECT ts, body FROM logs WHERE NOT (code = 5)",
                BTreeSet::new(),
            ),
            (
                "SELECT ts, body FROM logs WHERE other = 1 OR code = 3",
                every_row.clone(),
            ),
        ] {
            let run = run_pruned(&memory, vec![seg.clone()], None, true, sql).await;
            assert_eq!(run.rows, want, "rows of `{sql}`");
            assert_eq!(run.pruned_by_stats(), 0, "`{sql}` must skip nothing");
            assert!(run.store.gets_of("fives") > 0, "`{sql}` reads the segment");
        }

        let control = run_pruned(
            &memory,
            vec![seg],
            None,
            true,
            "SELECT ts, body FROM logs WHERE code = 6",
        )
        .await;
        assert!(control.rows.is_empty());
        assert_eq!(
            control.pruned_by_stats(),
            1,
            "control: the stamp excludes 6"
        );
    }

    /// The count is visible where an operator reads it: `EXPLAIN ANALYZE`
    /// prints `segments_pruned_by_stats` among the logs scan's metrics.
    ///
    /// Flipped assertion: removing the `global_counter` registration from
    /// `with_segments_pruned_by_stats` drops the metric from the output.
    #[tokio::test]
    async fn explain_analyze_reports_segments_pruned_by_stats() {
        let memory: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut segments = Vec::new();
        for (seq, base) in [(1u64, 100i64), (2, 200), (3, 300)] {
            let values = [Some(base), Some(base + 1)];
            let seg =
                write_code_segment(memory.as_ref(), &format!("seg-{seq}"), seq, &values).await;
            segments.push(with_stamps(seg, &[code_stamp(&values)]));
        }
        let backend = Arc::clone(&memory);
        let provider = LogsTableProvider::new(
            Snapshot {
                segments,
                segments_pruned: 0,
                pending_erasure: Vec::new(),
            },
            TenantHash([7u8; 16]),
            LogSegmentFetcher::new(backend),
            PhaseAccounting::pooled_over(&QueryAccounting::new()),
        )
        .with_declared_columns(code_declared());
        let ctx = logs_session(provider).expect("session");
        let batches = ctx
            .sql("EXPLAIN ANALYZE SELECT ts, body FROM logs WHERE code = 201")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("explain analyze");
        let text = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
            .expect("format")
            .to_string();
        let scan_line = text
            .lines()
            .find(|l| l.contains("LogsScanExec"))
            .unwrap_or_else(|| panic!("no LogsScanExec line in:\n{text}"));
        assert!(
            scan_line.contains("segments_pruned_by_stats=2"),
            "the scan's EXPLAIN ANALYZE metrics must report the two skipped \
             segments: {scan_line}"
        );
    }

    /// Which statistics carriers a generated segment has for [`CODE`].
    #[derive(Clone, Copy, Debug)]
    enum Carriers {
        None,
        Stamp,
        Cstat,
        StampAndCstat,
        /// An exact stamp and a `.cstat` entry that disagrees with it.
        Conflict,
    }

    /// One generated conjunct of the statement's `WHERE` clause.
    #[derive(Clone, Debug)]
    enum Conjunct {
        Eq(i64),
        Lt(i64),
        LtEq(i64),
        Gt(i64),
        GtEq(i64),
        Between(i64, i64),
        In(Vec<i64>),
        NotEq(i64),
        Not(i64),
        OrOther(i64, i64),
    }

    impl Conjunct {
        fn sql(&self) -> String {
            match self {
                Conjunct::Eq(v) => format!("code = {v}"),
                Conjunct::Lt(v) => format!("code < {v}"),
                Conjunct::LtEq(v) => format!("code <= {v}"),
                Conjunct::Gt(v) => format!("code > {v}"),
                Conjunct::GtEq(v) => format!("code >= {v}"),
                Conjunct::Between(a, b) => format!("code BETWEEN {a} AND {b}"),
                Conjunct::In(vs) => format!(
                    "code IN ({})",
                    vs.iter().map(i64::to_string).collect::<Vec<_>>().join(", ")
                ),
                Conjunct::NotEq(v) => format!("code != {v}"),
                Conjunct::Not(v) => format!("NOT (code = {v})"),
                Conjunct::OrOther(a, b) => format!("(code = {a} OR other = {b})"),
            }
        }

        /// The inclusive `[lo, hi]` arm the reader's pruning builds for this
        /// conjunct, or `None` for a shape that builds no arm. Written out from
        /// the SQL meaning, independently of `extract_logs`.
        fn arm(&self) -> Option<(Option<i64>, Option<i64>)> {
            match self {
                Conjunct::Eq(v) => Some((Some(*v), Some(*v))),
                Conjunct::Lt(v) => Some((None, Some(v - 1))),
                Conjunct::LtEq(v) => Some((None, Some(*v))),
                Conjunct::Gt(v) => Some((Some(v + 1), None)),
                Conjunct::GtEq(v) => Some((Some(*v), None)),
                Conjunct::Between(a, b) => Some((Some(*a), Some(*b))),
                Conjunct::In(vs) => Some((vs.iter().min().copied(), vs.iter().max().copied())),
                Conjunct::NotEq(_) | Conjunct::Not(_) | Conjunct::OrOther(..) => None,
            }
        }
    }

    /// Whether the D1 rule, applied by hand, skips a segment holding `values`
    /// with `carriers` under `conjuncts`: some arm is disjoint from an exact
    /// `[min, max]`, or the column is all NULL with the NULL count proven,
    /// which only a stamp does.
    fn rule_skips(values: &[Option<i64>], carriers: Carriers, conjuncts: &[Conjunct]) -> bool {
        let proven = match carriers {
            Carriers::None | Carriers::Conflict => return false,
            Carriers::Stamp | Carriers::StampAndCstat => true,
            Carriers::Cstat => false,
        };
        let present: Vec<i64> = values.iter().flatten().copied().collect();
        conjuncts.iter().filter_map(Conjunct::arm).any(|(lo, hi)| {
            match (present.iter().min(), present.iter().max()) {
                (Some(&min), Some(&max)) => {
                    hi.is_some_and(|h| h < min) || lo.is_some_and(|l| l > max)
                }
                _ => proven,
            }
        })
    }

    fn arb_conjunct() -> impl Strategy<Value = Conjunct> {
        let v = || 0i64..=8;
        prop_oneof![
            v().prop_map(Conjunct::Eq),
            v().prop_map(Conjunct::Lt),
            v().prop_map(Conjunct::LtEq),
            v().prop_map(Conjunct::Gt),
            v().prop_map(Conjunct::GtEq),
            (v(), v()).prop_map(|(a, b)| Conjunct::Between(a, b)),
            proptest::collection::vec(v(), 1..=3).prop_map(Conjunct::In),
            v().prop_map(Conjunct::NotEq),
            v().prop_map(Conjunct::Not),
            (v(), 0i64..=2).prop_map(|(a, b)| Conjunct::OrOther(a, b)),
        ]
    }

    fn arb_segment() -> impl Strategy<Value = (Vec<Option<i64>>, Carriers)> {
        let values = prop_oneof![
            4 => proptest::collection::vec(proptest::option::weighted(0.8, 0i64..=8), 1..=4),
            1 => (1usize..=3).prop_map(|n| vec![None; n]),
        ];
        let carriers = prop_oneof![
            Just(Carriers::None),
            Just(Carriers::Stamp),
            Just(Carriers::Cstat),
            Just(Carriers::StampAndCstat),
            Just(Carriers::Conflict),
        ];
        (values, carriers)
    }

    /// A `.cstat` entry that disagrees with the exact one for `values` while
    /// staying valid on its own: a widened minimum, or for an all-NULL segment
    /// a claimed non-null value.
    fn conflicting_cstat(values: &[Option<i64>]) -> ravel_proto::catalog::v1::ColumnStat {
        let present: Vec<i64> = values.iter().flatten().copied().collect();
        let rows = values.len() as u64;
        match (present.iter().min(), present.iter().max()) {
            (Some(&min), Some(&max)) => {
                code_cstat(rows, present.len() as u64, Some(min - 1), Some(max))
            }
            _ => code_cstat(rows, 1, Some(0), Some(0)),
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]

        /// Over generated segments (stamped, `.cstat`-covered, both, neither,
        /// conflicting; overlapping and disjoint ranges; all-NULL columns) and
        /// generated statements of the extracted shapes plus declined ones, the
        /// rows with statistics pruning equal the rows without it, the skipped
        /// segments are exactly the ones the D1 rule selects (by count, and by
        /// which objects saw zero GETs), and nothing is skipped with pruning
        /// off.
        ///
        /// Flipped assertions: skipping a segment only when EVERY arm is
        /// disjoint (`.any(` to `.all(` over the arms in
        /// `prune_segments_by_stats`) misses skips and fails the count; treating
        /// an arm that touches the boundary as disjoint (`q < seg_min` to
        /// `q <= seg_min` in `arm_excludes`) skips a segment holding a match and
        /// fails the row equality.
        #[test]
        fn stats_pruning_changes_no_row_and_skips_exactly_what_the_rule_selects(
            segs in proptest::collection::vec(arb_segment(), 1..=4),
            conjuncts in proptest::collection::vec(arb_conjunct(), 1..=3),
        ) {
            // At most one IN list: DataFusion's simplifier intersects IN lists
            // AND-ed on one column, which would change the pushed shape.
            prop_assume!(conjuncts.iter().filter(|c| matches!(c, Conjunct::In(_))).count() <= 1);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let memory: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
                let mut segments = Vec::new();
                let mut cstats = Vec::new();
                let mut expected_skipped = BTreeSet::new();
                for (i, (values, carriers)) in segs.iter().enumerate() {
                    let key = format!("seg-{i}");
                    let seq = i as u64 + 1;
                    let seg = write_code_segment(memory.as_ref(), &key, seq, values).await;
                    let seg = match carriers {
                        Carriers::Stamp | Carriers::StampAndCstat | Carriers::Conflict => {
                            with_stamps(seg, &[code_stamp(values)])
                        }
                        Carriers::None | Carriers::Cstat => seg,
                    };
                    match carriers {
                        Carriers::Cstat | Carriers::StampAndCstat => {
                            cstats.push((seg.clone(), exact_cstat(values)));
                        }
                        Carriers::Conflict => cstats.push((seg.clone(), conflicting_cstat(values))),
                        Carriers::None | Carriers::Stamp => {}
                    }
                    if rule_skips(values, *carriers, &conjuncts) {
                        expected_skipped.insert(key);
                    }
                    segments.push(seg);
                }
                let stats = loaded_cstats(cstats.iter().map(|(s, c)| (s, c.clone())).collect());
                let sql = format!(
                    "SELECT ts, body FROM logs WHERE {}",
                    conjuncts.iter().map(Conjunct::sql).collect::<Vec<_>>().join(" AND ")
                );

                let on = run_pruned(&memory, segments.clone(), Some(Arc::clone(&stats)), true, &sql)
                    .await;
                let off = run_pruned(&memory, segments, Some(stats), false, &sql).await;

                prop_assert_eq!(&on.rows, &off.rows, "rows differ with pruning for `{}`", sql);
                // A contradiction the optimizer folds to `false` (`code IN (0)
                // AND code = 1`) plans no scan: nothing is fetched and there is
                // nothing for the rule to decide.
                let (Some(on_pruned), Some(off_pruned)) =
                    (on.scan_pruned_by_stats, off.scan_pruned_by_stats)
                else {
                    prop_assert!(on.rows.is_empty() && off.rows.is_empty());
                    for i in 0..segs.len() {
                        prop_assert_eq!(on.store.gets_of(&format!("seg-{i}")), 0);
                    }
                    return Ok(());
                };
                prop_assert_eq!(off_pruned, 0);
                prop_assert_eq!(
                    on_pruned,
                    expected_skipped.len(),
                    "skip count for `{}` over {:?}",
                    sql,
                    segs
                );
                let unfetched: BTreeSet<String> = (0..segs.len())
                    .map(|i| format!("seg-{i}"))
                    .filter(|k| on.store.gets_of(k) == 0)
                    .collect();
                prop_assert_eq!(&unfetched, &expected_skipped, "unfetched objects for `{}`", sql);
                for i in 0..segs.len() {
                    prop_assert!(
                        off.store.gets_of(&format!("seg-{i}")) > 0,
                        "with pruning off every object is read"
                    );
                }
                Ok(())
            })?;
        }
    }
}
