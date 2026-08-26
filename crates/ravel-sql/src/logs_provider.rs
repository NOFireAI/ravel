//! `LogsTableProvider`: the `logs` table over one query's already-resolved,
//! owned `Snapshot` for `Signal::Logs` (ADR-0033). The log-signal sibling of
//! [`crate::provider::RavelTableProvider`].
//!
//! Like the metrics provider, this takes an owned, already-resolved `Snapshot`
//! (resolution is the endpoint's job) and a [`LogSegmentFetcher`], and
//! never resolves. `scan` extracts widen-only pushdown from the filters
//! (crate::logs_pushdown), prunes the snapshot's segments by
//! [`LogSegmentFetcher::ts_range_relevant`] against the extracted ts bounds,
//! and builds a single [`LogsScanExec`] leaf.
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
use ravel_catalog::{SegmentRef, Snapshot};
#[cfg(feature = "flight-sql")]
use ravel_query::ByteLimit;
use ravel_query::LogSegmentFetcher;
use ravel_query::erasure::{ErasurePredicate, snapshot_pending_erasure_predicates};
use ravel_types::TenantHash;
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

/// The `logs` table provider for one tenant over one pinned `Signal::Logs`
/// snapshot.
pub struct LogsTableProvider {
    snapshot: Arc<Snapshot>,
    tenant_hash: TenantHash,
    fetcher: LogSegmentFetcher,
    schema: SchemaRef,
    /// This query's accounting handle (ADR-0044), cloned into every
    /// `LogsScanExec` the provider builds so every store fetch the scan
    /// issues on this query's behalf is recorded against it.
    accounting: QueryAccounting,
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
    /// The coordinator-side distributed fan-out (ADR-0071; #326), if this
    /// provider is acting as a distributed coordinator. `None` -- the default,
    /// and every non-Flight build -- is the local scan path unchanged.
    #[cfg(feature = "flight-sql")]
    distributed: Option<DistributedRlogContext>,
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
        accounting: QueryAccounting,
    ) -> Self {
        let erasure = Arc::new(snapshot_pending_erasure_predicates(&snapshot));
        LogsTableProvider {
            snapshot: Arc::new(snapshot),
            tenant_hash,
            fetcher,
            schema: logs_schema(),
            accounting,
            erasure,
            declared: Arc::new(Vec::new()),
            #[cfg(feature = "flight-sql")]
            distributed: None,
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
    /// reimplemented.
    fn build_scan(
        &self,
        target_partitions: usize,
        pushdown: &LogsPushdown,
        projection: Option<&Vec<usize>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let segments = self.pruned_segments(pushdown);
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
            self.accounting.clone(),
            Arc::clone(&self.schema),
            Arc::clone(&self.declared),
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
                self.accounting.clone(),
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
        build_session(&config, pool, SessionTable::Logs(Arc::new(provider)), false)
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
            QueryAccounting::new(),
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
}
