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
//! scan, so pruning may only widen. An attribute predicate (`attrs['k']='v'`) is
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
use datafusion::physical_expr::expressions::col;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use ravel_catalog::{SegmentRef, Snapshot};
use ravel_query::LogSegmentFetcher;
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

use crate::config::SqlConfig;
use crate::error::SqlError;
use crate::logs_pushdown::{LogsPushdown, extract_logs};
use crate::logs_scan::LogsScanExec;
use crate::logs_schema::logs_schema;

/// The `logs` table provider for one tenant over one pinned `Signal::Logs`
/// snapshot.
pub struct LogsTableProvider {
    snapshot: Arc<Snapshot>,
    tenant_hash: TenantHash,
    fetcher: LogSegmentFetcher,
    config: SqlConfig,
    schema: SchemaRef,
    /// This query's accounting handle (ADR-0044), cloned into every
    /// `LogsScanExec` the provider builds so every store fetch the scan
    /// issues on this query's behalf is recorded against it.
    accounting: QueryAccounting,
}

impl LogsTableProvider {
    /// Build a provider around an owned, already-resolved `Signal::Logs`
    /// snapshot. `config` accepts anything `Into<SqlConfig>` (an `EngineConfig`
    /// alone works), matching the metrics provider's constructor shape.
    pub fn new(
        snapshot: Snapshot,
        tenant_hash: TenantHash,
        fetcher: LogSegmentFetcher,
        config: impl Into<SqlConfig>,
        accounting: QueryAccounting,
    ) -> Self {
        LogsTableProvider {
            snapshot: Arc::new(snapshot),
            tenant_hash,
            fetcher,
            config: config.into(),
            schema: logs_schema(),
            accounting,
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
            self.tenant_hash,
            self.fetcher.clone(),
            &segments,
            target_partitions,
            pushdown.ts_min(),
            pushdown.ts_max(),
            Arc::new(pushdown.content.clone()),
            Arc::new(pushdown.prune.clone()),
            self.accounting.clone(),
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
    use ravel_query::EngineConfig;
    use uuid::Uuid;

    use super::*;
    use crate::memory::TenantMemoryAccountant;
    use crate::session::{SessionTable, build_session};

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            // Must match the `TenantHash([7u8; 16])` every provider in these
            // tests is constructed with: issue #612 added a footer tenant_hash
            // check on the RLOG read path (`fetch_accounted_with_tenant`, which
            // `LogsScanExec` calls), so an object whose footer names a different
            // tenant than the fetch now fails closed with
            // `LogFetchError::Corrupt(IdentityMismatch("tenant_hash"))`. These
            // fixtures previously wrote `[1u8; 16]` and only passed because the
            // check did not exist.
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
    /// planner registration this task adds, not a bespoke test-only session.
    fn logs_session(provider: LogsTableProvider) -> DFResult<datafusion::prelude::SessionContext> {
        let config = SqlConfig::default();
        let tenant = TenantMemoryAccountant::new(1 << 30);
        let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
        build_session(&config, pool, SessionTable::Logs(Arc::new(provider)))
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
    /// (issue #552).
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

    /// Issue #544's acceptance test: the same SQL query, with and without an
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
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            EngineConfig::default(),
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
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            EngineConfig::default(),
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
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            EngineConfig::default(),
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

    /// Issue #507's acceptance test: `attrs['k'] = 'v'` must plan (the whole
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
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            EngineConfig::default(),
            QueryAccounting::new(),
        );
        let ctx = logs_session(provider).expect("build session");

        // Planning: `attrs['service.name'] = 'api'` must not error with
        // "GetFieldAccess not supported" -- the bug this task fixes.
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

    /// Predicate shapes that must NOT be extracted into a fetch prune
    /// (issue #510 deliverable 3): an inequality, an `OR` across different
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
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            EngineConfig::default(),
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
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            EngineConfig::default(),
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
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            EngineConfig::default(),
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
        };
        let provider = LogsTableProvider::new(
            snapshot,
            TenantHash([7u8; 16]),
            fetcher,
            EngineConfig::default(),
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
}
