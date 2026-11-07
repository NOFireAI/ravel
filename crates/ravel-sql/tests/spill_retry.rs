//! Issue #3899: spill is a second attempt after a memory refusal, not a plan
//! property.
//!
//! A statement runs first exactly as it would with spill not configured, and
//! only a memory refusal of an eligible statement buys it a spill-enabled
//! retry. These tests pin the first half over a real RLOG object in a
//! `MemoryStore`: with spill configured, a logs statement that fits plans,
//! loads column statistics and answers byte for byte as the same statement on
//! an executor with no spill at all, and a `min`/`max` top-k that the bounded
//! aggregate serves keeps its own sort order on the plan-time path that still
//! reads the spill configuration. The retry half lives in `spill.rs`, beside
//! the q33 fixtures that are refused.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::util::display::array_value_to_string;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, SpanSegmentFetcher, SpillConfig, SqlConfig, SqlExecutor,
    SqlOutcome, SqlRequest, StaticDeclaredColumns,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::AttrValue;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

/// The declared group-key column, an `I64` attribute.
const KEY_COL: &str = "gid";

/// Distinct group keys. Group `i` holds one row, except [`LATE_KEY`], which
/// holds two.
const KEYS: i64 = 20_000;

/// The one group with two rows, so a count-ordered top ten has one leader
/// and nine rows decided by the tiebreak.
const LATE_KEY: i64 = 7;

/// The `LIMIT` both statements ask for.
const TOP_K: usize = 10;

/// A count-ordered top ten over the declared key: exact-typed, so the first
/// attempt repartitions its final aggregate, and spill-eligible, so a spill
/// grant would plan it repartition-free.
const COUNT_TOP_SQL: &str = "SELECT gid, count(*) AS c FROM logs GROUP BY gid \
                             ORDER BY c DESC LIMIT 10";

/// A `max` top-k over the non-nullable `ts` column, which the bounded top-k
/// aggregate (issue #1402) serves.
const MAX_TOP_SQL: &str = "SELECT gid, max(ts) AS m FROM logs GROUP BY gid \
                           ORDER BY m DESC LIMIT 10";

fn tenant() -> TenantId {
    TenantId::new("spill-retry-3899".to_string())
}

fn log_record(ts: i64, gid: i64) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: "req".into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![(KEY_COL.to_string(), AttrValue::I64(gid))],
    }
}

/// [`KEYS`] groups of one row each, group `i` at `ts = i + 1`, then a second
/// [`LATE_KEY`] row at `ts = KEYS + 1`.
fn records() -> Vec<LogRecord> {
    let mut out: Vec<LogRecord> = (0..KEYS).map(|i| log_record(i + 1, i)).collect();
    out.push(log_record(KEYS + 1, LATE_KEY));
    out
}

/// Write one RLOG object holding `records` and publish its commit record.
async fn publish_logs(store: &dyn ObjectStoreBackend, records: &[LogRecord]) {
    let identity = ObjectIdentity {
        tenant_hash: tenant().hash().0,
        shard: 0,
        writer_id: [5u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for r in records {
        writer.push(r.clone()).expect("push record");
    }
    let bytes = writer.finish().expect("finish rlog");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant().hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(9_3899),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [9u8; 32],
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

/// An executor over a fresh store holding [`records`], with `gid` declared,
/// and spill configured at `spill` (`None` is the unconfigured control).
async fn executor(spill: Option<SpillConfig>) -> SqlExecutor {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_logs(store.as_ref(), &records()).await;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig {
            spill,
            ..SqlConfig::default()
        },
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(vec![
        DeclaredColumn::new(KEY_COL, DeclaredType::I64),
    ])))
}

/// A spill configuration over `dir` with a scratch quota no statement here
/// approaches.
fn spill(dir: &std::path::Path) -> Option<SpillConfig> {
    Some(SpillConfig {
        dir: dir.to_path_buf(),
        max_bytes: 512 * 1024 * 1024,
    })
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: i64::MAX,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(120),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// Every row of `outcome`, rendered as `col|col`, in result order.
fn rows(outcome: &SqlOutcome) -> Vec<String> {
    let mut out = Vec::new();
    for batch in outcome.output.batches() {
        for row in 0..batch.num_rows() {
            let cells: Vec<String> = batch
                .columns()
                .iter()
                .map(|column| array_value_to_string(column, row).expect("renderable"))
                .collect();
            out.push(cells.join("|"));
        }
    }
    out
}

/// The plan-time physical plan of `sql`: `plan_pinned`, the path Flight SQL
/// takes, which still reads the spill configuration when it plans.
async fn plan_time_plan(executor: &SqlExecutor, sql: &str) -> Arc<dyn ExecutionPlan> {
    let accounting = QueryAccounting::new();
    let declared = executor
        .resolve_declared_columns(tenant().hash(), request(sql).now_ns)
        .await;
    let (snapshot, _) = executor
        .resolve_snapshot(tenant().hash(), &request(sql), &accounting)
        .await
        .expect("snapshot resolves");
    executor
        .plan_pinned(tenant().hash(), snapshot, sql, &accounting, &declared)
        .await
        .expect("query plans")
        .create_physical_plan()
        .await
        .expect("physical plan builds")
}

/// Every `AggregateExec` limit in `plan`, in pre-order.
fn aggregate_limits(plan: &Arc<dyn ExecutionPlan>) -> Vec<usize> {
    let mut out = Vec::new();
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>()
        && let Some(options) = aggregate.limit_options()
    {
        out.push(options.limit);
    }
    for child in plan.children() {
        out.extend(aggregate_limits(child));
    }
    out
}

/// Issue #3899: with spill configured, a statement that fits runs its first
/// attempt as a deployment without spill runs it. Its physical plan (as
/// `explain` renders it) is the unconfigured executor's, repartitioned final
/// aggregate included; its plan phase makes the same store requests, which is
/// where the column-statistics load is charged; it returns the same rows; and
/// it takes no spill retry and creates no scratch directory.
///
/// Prove-the-test: bind `SpillAttempt::First` to `config.spill.as_ref()` in
/// `plan_pinned_with` and the first attempt plans the repartition-free
/// spill-enabled aggregate, so the plan text differs from the control's.
#[tokio::test]
async fn a_statement_that_fits_runs_its_first_attempt_as_if_spill_were_off() {
    let scratch = tempfile::tempdir().expect("scratch root");
    let configured = executor(spill(scratch.path())).await;
    let unconfigured = executor(None).await;

    let plan = configured
        .explain(tenant().hash(), &request(COUNT_TOP_SQL))
        .await
        .expect("explain")
        .plan_text;
    let control = unconfigured
        .explain(tenant().hash(), &request(COUNT_TOP_SQL))
        .await
        .expect("explain")
        .plan_text;
    assert_eq!(plan, control, "the first attempt's physical plan");
    assert!(
        control.contains("RepartitionExec: partitioning=Hash"),
        "the control repartitions, which a spill-enabled plan never does, so \
         equal plans mean the first attempt was not planned for spill; got:\n{control}"
    );

    let outcome = configured
        .execute(tenant().hash(), &request(COUNT_TOP_SQL))
        .await
        .expect("the statement fits");
    let control = unconfigured
        .execute(tenant().hash(), &request(COUNT_TOP_SQL))
        .await
        .expect("the statement fits");
    assert_eq!(
        outcome.phase_accounting.plan, control.phase_accounting.plan,
        "the plan phase, column-statistics load included, makes the same requests"
    );
    assert_eq!(rows(&outcome), rows(&control), "row for row");
    assert_eq!(rows(&outcome).len(), TOP_K);
    assert_eq!(rows(&outcome)[0], format!("{LATE_KEY}|2"));
    assert_eq!(
        (outcome.stats.spill_retries, outcome.stats.spill.files),
        (0, 0),
        "no spill retry and no spill"
    );
    assert!(outcome.stats.spill_retry.is_none());
    assert_eq!(
        std::fs::read_dir(scratch.path())
            .expect("scratch root")
            .count(),
        0,
        "a first attempt creates no scratch directory"
    );
}

/// Issue #3899 with #3904: a `max` top-k the bounded aggregate serves gets no
/// tie-order edit on the plan-time path either, the one path that still reads
/// the spill configuration when it plans. Its aggregates keep the `LIMIT` as
/// their bound and its sort orders by `m` alone. Without the edit its sort is
/// not total, so it is not spill-eligible: a spill-configured executor plans
/// it exactly as the unconfigured one does, and it returns the same rows.
#[tokio::test]
async fn a_bounded_top_k_gets_no_tie_order_edit_with_spill_configured() {
    let scratch = tempfile::tempdir().expect("scratch root");
    let configured = executor(spill(scratch.path())).await;
    let unconfigured = executor(None).await;

    let sort_lines = |plan: &Arc<dyn ExecutionPlan>| {
        format!("{}", displayable(plan.as_ref()).indent(false))
            .lines()
            .filter(|line| line.contains("SortExec") || line.contains("SortPreservingMergeExec"))
            .map(|line| line.trim().to_string())
            .collect::<Vec<_>>()
    };
    let plan = plan_time_plan(&configured, MAX_TOP_SQL).await;
    let control = plan_time_plan(&unconfigured, MAX_TOP_SQL).await;
    let text =
        |plan: &Arc<dyn ExecutionPlan>| format!("{}", displayable(plan.as_ref()).indent(false));
    assert_eq!(
        text(&plan),
        text(&control),
        "the plan-time plan is the unconfigured executor's"
    );
    assert_eq!(
        std::fs::read_dir(scratch.path())
            .expect("scratch root")
            .count(),
        0,
        "a sort with no total order is not spill-eligible, so planning it \
         creates no scratch directory"
    );
    let limits = aggregate_limits(&plan);
    assert!(
        !limits.is_empty() && limits.iter().all(|&limit| limit == TOP_K),
        "every aggregate is bounded by the LIMIT; got {limits:?}"
    );
    let control_limits = aggregate_limits(&control);
    assert!(
        !control_limits.is_empty() && control_limits.iter().all(|&limit| limit == TOP_K),
        "the control's aggregates are bounded the same way; got {control_limits:?}"
    );
    let sorts = sort_lines(&plan);
    assert!(!sorts.is_empty(), "the plan sorts");
    for line in &sorts {
        assert!(
            line.contains("expr=[m@1 DESC]"),
            "the sort orders by the aggregate alone, with no group-key tiebreak; \
             got {line}"
        );
    }
    assert!(
        sort_lines(&control)
            .iter()
            .all(|line| line.contains("expr=[m@1 DESC]")),
        "the control sorts the same way"
    );

    let outcome = configured
        .execute(tenant().hash(), &request(MAX_TOP_SQL))
        .await
        .expect("the statement runs");
    let control = unconfigured
        .execute(tenant().hash(), &request(MAX_TOP_SQL))
        .await
        .expect("the statement runs");
    assert_eq!(rows(&outcome), rows(&control), "row for row");
    assert_eq!(rows(&outcome).len(), TOP_K);
    assert_eq!(outcome.stats.spill_retries, 0);
}
