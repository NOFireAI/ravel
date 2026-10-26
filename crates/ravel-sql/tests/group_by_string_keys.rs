//! Issue #737: a string-keyed `GROUP BY` must not be able to panic the
//! process, and must not reach the `RowConverter` group-value path that can.
//!
//! Three properties, in the order the fix delivers them:
//!
//! - `declared_str_group_by_groups_on_views_and_returns_its_declared_type`:
//!   the plan. A `GROUP BY` over a declared `Str` column, through a real
//!   `SqlExecutor` over a `MemoryStore` tenant, groups on `Utf8View` in every
//!   aggregate stage and still hands the caller a `Dictionary(Int32, Utf8)`
//!   column with the same rows a dictionary-keyed grouping produces.
//! - `a_panicking_operator_surfaces_as_a_typed_error`: the boundary. A plan
//!   whose scan panics mid-poll yields `SqlError::OperatorPanic` instead of
//!   unwinding out of whichever task drives the stream, the stream is fused
//!   afterwards, and a `SqlExecutor` in the same process serves the next
//!   statement normally.
//! - `stress_overflowing_group_table_never_panics`: the two together at the
//!   scale that produced the original report, behind
//!   `RAVEL_SQL_GROUP_BY_STRESS=1` because it moves gigabytes. See its own
//!   doc comment.
//!
//! Issue #3271 adds where the cast back to the declared type runs:
//! `group_by_str_limit_restores_above_the_topk` (above the TopK, once),
//! `single_distinct_two_level_aggregate_keeps_views_between_levels` (no cast
//! between the two levels of a `COUNT(DISTINCT ..)`) and
//! `sort_key_computing_on_the_group_key_restores_below_the_sort` (below a sort
//! whose key computes on the group key).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, ArrayRef, DictionaryArray, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::Result as DFResult;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::TableType;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::CastExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateOutputMode};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, InvariantLevel};
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    CeilingBreach, DeclaredColumn, DeclaredType, DictionaryGroupKeysAsViews, ErrorClass,
    MSG_INTERNAL, PinnedStream, SpanSegmentFetcher, SqlConfig, SqlError, SqlExecutor, SqlRequest,
    StaticDeclaredColumns, TenantDelegatingPool, TenantMemoryAccountant,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

fn dict_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: 1_000_000,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(60),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// Every `AggregateExec` in `plan`, deepest last.
fn aggregates(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Arc<dyn ExecutionPlan>>) {
    if plan.is::<AggregateExec>() {
        out.push(Arc::clone(plan));
    }
    for child in plan.children() {
        aggregates(child, out);
    }
}

// ---------------------------------------------------------------------------
// A logs tenant with one declared `Str` column
// ---------------------------------------------------------------------------

/// The declared attribute key, and the SQL column name it takes.
const URL_KEY: &str = "url";

fn tenant() -> TenantId {
    TenantId::new("group-by-string-keys-737".to_string())
}

fn log_record(ts: i64, url: &str) -> LogRecord {
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
        attrs: vec![(URL_KEY.to_string(), AttrValue::Str(url.to_string()))],
    }
}

/// Write one RLOG object holding `records` and publish its commit record, so a
/// real `Catalog::resolve` finds it.
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId, records: &[LogRecord]) {
    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: [3u8; 16],
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
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(9_737),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [7u8; 32],
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        // The version the RlogWriter above actually stamped. A commit record
        // that names another one is refused before any GET is issued
        // (ADR-0892 decision 4), so this cannot be a placeholder.
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

fn executor_over(store: Arc<dyn ObjectStoreBackend>, declared: Vec<DeclaredColumn>) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared)))
}

/// A tenant whose records carry `URL_KEY`, with that key declared as a `Str`
/// column so it projects as `Dictionary(Int32, Utf8)` (ADR-0099 decision 5).
async fn declared_url_executor() -> SqlExecutor {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let records: Vec<LogRecord> = [
        "https://example.test/a",
        "https://example.test/b",
        "https://example.test/a",
        "https://example.test/c",
        "https://example.test/a",
    ]
    .iter()
    .enumerate()
    .map(|(i, url)| log_record(i as i64 + 1, url))
    .collect();
    publish_logs(store.as_ref(), &tenant(), &records).await;
    executor_over(store, vec![DeclaredColumn::new(URL_KEY, DeclaredType::Str)])
}

/// Deliverable 1, end to end through the real executor: the aggregate stages
/// group on `Utf8View`, and nothing a caller can observe changed.
#[tokio::test]
async fn declared_str_group_by_groups_on_views_and_returns_its_declared_type() {
    let executor = declared_url_executor().await;
    let sql = format!(
        "SELECT {URL_KEY}, count(*) AS hits FROM logs GROUP BY {URL_KEY} ORDER BY {URL_KEY}"
    );

    // The plan: every aggregate stage groups on a view, so none of them can
    // reach `GroupValuesRows`, and the plan's own output type is unchanged.
    let accounting = QueryAccounting::new();
    let declared = executor
        .resolve_declared_columns(tenant().hash(), request(&sql).now_ns)
        .await;
    let (snapshot, _) = executor
        .resolve_snapshot(tenant().hash(), &request(&sql), &accounting)
        .await
        .expect("snapshot resolves");
    let planned = executor
        .plan_pinned(tenant().hash(), snapshot, &sql, &accounting, &declared)
        .await
        .expect("query plans");
    let plan = planned
        .create_physical_plan()
        .await
        .expect("physical plan builds");
    let mut stages = Vec::new();
    aggregates(&plan, &mut stages);
    assert!(
        !stages.is_empty(),
        "the query must plan at least one aggregate"
    );
    for stage in &stages {
        assert_eq!(
            stage.schema().field(0).data_type(),
            &DataType::Utf8View,
            "an aggregate stage still groups on a dictionary key: {stage:?}"
        );
    }
    assert_eq!(plan.schema().field(0).data_type(), &dict_type());

    // The result: the declared wire type, and the rows the data implies.
    let outcome = executor
        .execute(tenant().hash(), &request(&sql))
        .await
        .expect("query runs");
    let mut rows: Vec<(String, i64)> = Vec::new();
    for batch in outcome.output.batches() {
        assert_eq!(batch.column(0).data_type(), &dict_type());
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("group column is a dictionary");
        let values = keys
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dictionary values are strings");
        let hits = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is Int64");
        for row in 0..batch.num_rows() {
            let key = values.value(keys.keys().value(row) as usize).to_string();
            rows.push((key, hits.value(row)));
        }
    }
    assert_eq!(
        rows,
        vec![
            ("https://example.test/a".to_string(), 3),
            ("https://example.test/b".to_string(), 1),
            ("https://example.test/c".to_string(), 1),
        ]
    );
}

// ---------------------------------------------------------------------------
// Where the cast back to the declared type runs (issue #3271)
// ---------------------------------------------------------------------------

/// The physical plan the real executor builds for `sql` over the declared-URL
/// tenant, with every optimizer rule `build_session` installs.
async fn executor_plan(executor: &SqlExecutor, sql: &str) -> Arc<dyn ExecutionPlan> {
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

/// The nodes from `plan` down to the first node, in pre-order, for which
/// `pred` holds, both ends included.
fn path_to(
    plan: &Arc<dyn ExecutionPlan>,
    pred: &dyn Fn(&Arc<dyn ExecutionPlan>) -> bool,
) -> Option<Vec<Arc<dyn ExecutionPlan>>> {
    if pred(plan) {
        return Some(vec![Arc::clone(plan)]);
    }
    for child in plan.children() {
        if let Some(mut path) = path_to(child, pred) {
            path.insert(0, Arc::clone(plan));
            return Some(path);
        }
    }
    None
}

/// Every node of `plan`, in pre-order.
fn nodes(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<Arc<dyn ExecutionPlan>>) {
    out.push(Arc::clone(plan));
    for child in plan.children() {
        nodes(child, out);
    }
}

/// How many `CastExpr`s to `ty` the node's own expressions hold, at any depth.
fn casts_to(node: &Arc<dyn ExecutionPlan>, ty: &DataType) -> usize {
    let mut count = 0;
    node.apply_expressions(&mut |expr| {
        expr.apply(|sub| {
            if sub
                .downcast_ref::<CastExpr>()
                .is_some_and(|cast| cast.cast_type() == ty)
            {
                count += 1;
            }
            Ok(TreeNodeRecursion::Continue)
        })
    })
    .expect("expressions walk");
    count
}

/// The type of the column named `name` in `node`'s output, if it has one.
fn column_type(node: &Arc<dyn ExecutionPlan>, name: &str) -> Option<DataType> {
    node.schema()
        .field_with_name(name)
        .ok()
        .map(|field| field.data_type().clone())
}

/// Asserts that the plan holds exactly one `CastExpr` to the declared
/// dictionary type, in a `ProjectionExec` that is an ancestor of the topmost
/// node carrying a fetch, and returns the path from the root to that node.
fn assert_restore_above_the_fetch(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut all = Vec::new();
    nodes(plan, &mut all);
    let restores: Vec<_> = all
        .iter()
        .filter(|node| casts_to(node, &dict_type()) > 0)
        .collect();
    let [restore] = restores.as_slice() else {
        panic!("expected exactly one cast to the declared type, found {restores:?}");
    };
    assert!(restore.is::<ProjectionExec>(), "{restore:?}");
    assert_eq!(casts_to(restore, &dict_type()), 1);
    let fetch = path_to(plan, &|node| node.fetch().is_some()).expect("the plan has a fetch node");
    let at = fetch
        .iter()
        .position(|node| Arc::ptr_eq(node, restore))
        .unwrap_or_else(|| {
            panic!(
                "the restore projection is not an ancestor of the topmost fetch node {:?}",
                fetch.last()
            )
        });
    assert!(
        at + 1 < fetch.len(),
        "the restore projection is the fetch node itself"
    );
    fetch
}

/// q34's shape, `GROUP BY <declared Str> ORDER BY count(*) DESC LIMIT k`:
/// the cast back to the declared type runs once, above the TopK and the merge,
/// on the rows the limit lets through, and nothing between it and the scan
/// handles the key as a dictionary after the first aggregate.
#[tokio::test]
async fn group_by_str_limit_restores_above_the_topk() {
    let executor = declared_url_executor().await;
    let sql = format!(
        "SELECT {URL_KEY}, count(*) AS hits FROM logs GROUP BY {URL_KEY} \
         ORDER BY count(*) DESC LIMIT 10"
    );
    let plan = executor_plan(&executor, &sql).await;
    assert_eq!(plan.schema().field(0).data_type(), &dict_type());
    assert_restore_above_the_fetch(&plan);

    let mut all = Vec::new();
    nodes(&plan, &mut all);
    let mut checked = 0;
    for node in &all {
        if node.is::<AggregateExec>()
            || node.is::<SortExec>()
            || node.is::<SortPreservingMergeExec>()
        {
            assert_eq!(
                column_type(node, URL_KEY).as_ref(),
                Some(&DataType::Utf8View),
                "{} still carries the key as a dictionary",
                node.name()
            );
            checked += 1;
        }
    }
    assert!(
        all.iter().any(|node| node.is::<SortExec>()),
        "the plan has a TopK sort to check"
    );
    assert!(
        checked >= 3,
        "only {checked} aggregate and sort nodes checked"
    );

    let outcome = executor
        .execute(tenant().hash(), &request(&sql))
        .await
        .expect("query runs");
    let rows = url_counts(outcome.output.batches());
    assert_eq!(
        rows.first(),
        Some(&("https://example.test/a".to_string(), 3))
    );
    let mut sorted = rows.clone();
    sorted.sort();
    assert_eq!(
        sorted,
        vec![
            ("https://example.test/a".to_string(), 3),
            ("https://example.test/b".to_string(), 1),
            ("https://example.test/c".to_string(), 1),
        ]
    );
}

/// q14's shape, `GROUP BY <declared Str>` over one `COUNT(DISTINCT ..)`,
/// which DataFusion plans as two aggregate levels: the key stays a view from
/// the inner level to the outer one, so it is cast to `Utf8View` once, below
/// the inner level's final stage, and back to the declared type once, above
/// the TopK.
#[tokio::test]
async fn single_distinct_two_level_aggregate_keeps_views_between_levels() {
    let executor = declared_url_executor().await;
    let sql = format!(
        "SELECT {URL_KEY}, COUNT(DISTINCT ts) AS u FROM logs GROUP BY {URL_KEY} \
         ORDER BY 2 DESC LIMIT 10"
    );
    let plan = executor_plan(&executor, &sql).await;
    assert_eq!(plan.schema().field(0).data_type(), &dict_type());
    assert_restore_above_the_fetch(&plan);

    // The inner level groups on the key and the distinct argument; its final
    // stage is the first aggregate producing final values with two keys.
    let inner_final = path_to(&plan, &|node| {
        node.downcast_ref::<AggregateExec>().is_some_and(|agg| {
            agg.group_expr().expr().len() == 2
                && agg.mode().output_mode() != AggregateOutputMode::Partial
        })
    })
    .expect("the statement plans an inner aggregate level");
    let outer_final = inner_final
        .iter()
        .position(|node| node.is::<AggregateExec>())
        .expect("the outer level sits above the inner one");
    assert!(
        outer_final + 1 < inner_final.len(),
        "the outer level is a different aggregate from the inner one"
    );
    for node in &inner_final[outer_final..] {
        let schema = node.schema();
        for field in schema.fields() {
            assert!(
                !matches!(field.data_type(), DataType::Dictionary(..)),
                "{} carries {} as a dictionary between the two levels",
                node.name(),
                field.name()
            );
        }
    }
    for node in &inner_final[..inner_final.len() - 1] {
        assert_eq!(
            casts_to(node, &DataType::Utf8View),
            0,
            "{} casts the key to a view again above the inner final stage",
            node.name()
        );
    }
    let mut all = Vec::new();
    nodes(&plan, &mut all);
    let to_views: Vec<_> = all
        .iter()
        .filter(|node| casts_to(node, &DataType::Utf8View) > 0)
        .collect();
    let [to_view] = to_views.as_slice() else {
        panic!("expected one node casting to Utf8View, found {to_views:?}");
    };
    assert_eq!(casts_to(to_view, &DataType::Utf8View), 1);
    let inner_final_stage = inner_final.last().expect("path is non-empty");
    assert!(
        !Arc::ptr_eq(to_view, inner_final_stage)
            && path_to(inner_final_stage, &|node| Arc::ptr_eq(node, to_view)).is_some(),
        "the cast to Utf8View is not below the inner level's final stage: {to_view:?}"
    );

    let outcome = executor
        .execute(tenant().hash(), &request(&sql))
        .await
        .expect("query runs");
    let rows = url_counts(outcome.output.batches());
    assert_eq!(
        rows.first(),
        Some(&("https://example.test/a".to_string(), 3))
    );
    assert_eq!(rows.len(), 3);
}

/// A sort key that computes on the group key (`url = '..'`, a `CASE` over
/// it) is evaluated on the sort's input, against a dictionary literal, so the
/// restore projection sits below that sort and its input holds the declared
/// type. A sort key that is the bare group column next to an aggregate value
/// does not stop the carry: the restore still runs above the TopK.
#[tokio::test]
async fn sort_key_computing_on_the_group_key_restores_below_the_sort() {
    let executor = declared_url_executor().await;
    for (order_by, order) in [
        ("url = 'https://example.test/a', hits, url", ["b", "c", "a"]),
        (
            "CASE url WHEN 'https://example.test/a' THEN 0 ELSE 1 END, hits, url",
            ["a", "b", "c"],
        ),
        // The key two levels below the root of the sort expression.
        (
            "CASE WHEN url = 'https://example.test/a' THEN 0 ELSE 1 END, hits, url",
            ["a", "b", "c"],
        ),
    ] {
        let sql = format!(
            "SELECT {URL_KEY}, count(*) AS hits FROM logs GROUP BY {URL_KEY} ORDER BY {order_by}"
        );
        let plan = executor_plan(&executor, &sql).await;
        assert_eq!(plan.schema().field(0).data_type(), &dict_type());
        let mut all = Vec::new();
        nodes(&plan, &mut all);
        let sorts: Vec<_> = all.iter().filter(|node| node.is::<SortExec>()).collect();
        let [sort] = sorts.as_slice() else {
            panic!("{sql}: expected one SortExec, found {sorts:?}");
        };
        let [input] = sort.children()[..] else {
            panic!("a SortExec has one input");
        };
        assert_eq!(
            column_type(input, URL_KEY).as_ref(),
            Some(&dict_type()),
            "{sql}: the sort's input carries the key as a view"
        );
        assert!(input.is::<ProjectionExec>(), "{sql}: {input:?}");
        assert_eq!(casts_to(input, &dict_type()), 1, "{sql}");

        let outcome = executor
            .execute(tenant().hash(), &request(&sql))
            .await
            .expect("query runs");
        assert_eq!(
            url_counts(outcome.output.batches()),
            order
                .iter()
                .map(|path| {
                    let hits = if *path == "a" { 3 } else { 1 };
                    (format!("https://example.test/{path}"), hits)
                })
                .collect::<Vec<_>>(),
            "{sql}"
        );
    }

    let sql = format!(
        "SELECT {URL_KEY}, count(*) AS hits FROM logs GROUP BY {URL_KEY} \
         ORDER BY hits DESC, {URL_KEY} ASC LIMIT 10"
    );
    let plan = executor_plan(&executor, &sql).await;
    assert_restore_above_the_fetch(&plan);
}

// ---------------------------------------------------------------------------
// A partitioned hash join over grouped inputs (issue #3280)
// ---------------------------------------------------------------------------

/// The join statements the differential below runs, each with one correct row
/// order over the `join_context` fixture.
const DIFFERENTIAL: [&str; 4] = [
    // (a) Both inputs grouped on the key: the join carries the views.
    "SELECT a.k, a.c, b.c \
     FROM (SELECT k, count(*) AS c FROM logs GROUP BY k) a \
     JOIN (SELECT k, count(*) AS c FROM logs GROUP BY k) b ON a.k = b.k \
     ORDER BY a.k",
    // (b) One input grouped, the other the scan's dictionary column.
    "SELECT a.k, a.c, b.k \
     FROM (SELECT k, count(*) AS c FROM logs GROUP BY k) a \
     JOIN (SELECT k FROM logs) b ON a.k = b.k \
     ORDER BY a.k",
    // (c) A condition on the key next to the equality.
    "SELECT a.k, a.c, b.c \
     FROM (SELECT k, count(*) AS c FROM logs GROUP BY k) a \
     JOIN (SELECT k, count(*) AS c FROM logs GROUP BY k) b ON a.k = b.k AND a.k <> 'x' \
     ORDER BY a.k",
    // (d) A join filter that computes on the key and reads both inputs, so it
    // stays on the join.
    "SELECT a.k, a.c, b.c \
     FROM (SELECT k, count(*) AS c FROM logs GROUP BY k) a \
     JOIN (SELECT k, count(*) AS c FROM logs GROUP BY k) b \
     ON a.k = b.k AND (a.k <> 'x' OR b.c > 1) \
     ORDER BY a.k",
];

/// Partitions of the `join_context` table, so every aggregate splits into a
/// partial and a final stage with a hash exchange between them.
const JOIN_PARTITIONS: usize = 4;

/// A session over a `logs` table with one `Dictionary(Int32, Utf8)` column
/// `k`, set up so a hash join plans in `Partitioned` mode, and with the rule
/// installed after the default rules when `with_rule`, as `build_session`
/// installs it.
///
/// `SqlExecutor` cannot reach that mode: its `session_config` turns
/// `repartition_joins` off, which plans every hash join `CollectLeft`. Here it
/// is on, and both `hash_join_single_partition_threshold` knobs are 0, so
/// `JoinSelection` never finds an input small enough to collect.
///
/// The keys are `a` four times, `b` three, `c` twice, `x` once and NULL twice,
/// spread over `JOIN_PARTITIONS` partitions.
fn join_context(with_rule: bool) -> SessionContext {
    use datafusion::datasource::MemTable;
    use datafusion::execution::session_state::SessionStateBuilder;

    let schema = Arc::new(Schema::new(vec![Field::new("k", dict_type(), true)]));
    let keys: [Option<&str>; 12] = [
        Some("a"),
        Some("b"),
        None,
        Some("c"),
        Some("a"),
        Some("x"),
        Some("b"),
        Some("a"),
        None,
        Some("c"),
        Some("b"),
        Some("a"),
    ];
    let partitions: Vec<Vec<RecordBatch>> = keys
        .chunks(keys.len() / JOIN_PARTITIONS)
        .map(|chunk| {
            let dict: DictionaryArray<Int32Type> = chunk.iter().copied().collect();
            vec![
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(dict) as ArrayRef])
                    .expect("key batch builds"),
            ]
        })
        .collect();
    assert_eq!(partitions.len(), JOIN_PARTITIONS);

    let mut config = SessionConfig::new()
        .with_target_partitions(JOIN_PARTITIONS)
        .with_repartition_joins(true);
    config.options_mut().optimizer.hash_join_single_partition_threshold = 0;
    config
        .options_mut()
        .optimizer
        .hash_join_single_partition_threshold_rows = 0;
    let mut builder = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features();
    if with_rule {
        builder = builder.with_physical_optimizer_rule(Arc::new(DictionaryGroupKeysAsViews));
    }
    let ctx = SessionContext::new_with_state(builder.build());
    let table = MemTable::try_new(schema, partitions).expect("mem table builds");
    ctx.register_table("logs", Arc::new(table))
        .expect("table registers");
    ctx
}

/// The physical plan of `sql` over `join_context(true)`.
async fn join_plan(sql: &str) -> Arc<dyn ExecutionPlan> {
    join_context(true)
        .sql(sql)
        .await
        .expect("query plans")
        .create_physical_plan()
        .await
        .unwrap_or_else(|err| panic!("{sql}: physical plan fails: {err}"))
}

/// The plan's one `HashJoinExec`, asserted to be in `Partitioned` mode.
fn partitioned_join(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let mut all = Vec::new();
    nodes(plan, &mut all);
    let joins: Vec<_> = all
        .into_iter()
        .filter(|node| node.is::<HashJoinExec>())
        .collect();
    let [join] = joins.as_slice() else {
        panic!("expected one HashJoinExec, found {joins:?}");
    };
    let mode = *join
        .downcast_ref::<HashJoinExec>()
        .expect("is a HashJoinExec")
        .partition_mode();
    assert_eq!(mode, PartitionMode::Partitioned, "{join:?}");
    Arc::clone(join)
}

/// Every node of `plan` passes DataFusion's executable-plan invariants, the
/// check whose failure issue #3280 reported for a partitioned join.
fn assert_invariants(plan: &Arc<dyn ExecutionPlan>) {
    let mut all = Vec::new();
    nodes(plan, &mut all);
    for node in &all {
        node.check_invariants(InvariantLevel::Executable)
            .unwrap_or_else(|err| panic!("{} fails its invariants: {err}", node.name()));
    }
}

/// Run `sql` on a fresh `join_context`, returning the schema the plan
/// promised and the rows as a table.
async fn run_join(with_rule: bool, sql: &str) -> (Schema, String) {
    let df = join_context(with_rule)
        .sql(sql)
        .await
        .expect("query plans");
    let logical = df.schema().as_arrow().clone();
    let batches = df
        .collect()
        .await
        .unwrap_or_else(|err| panic!("{sql} (rule {with_rule}): {err}"));
    for batch in &batches {
        assert_eq!(batch.schema().fields(), logical.fields(), "{sql}");
    }
    let table = datafusion::arrow::util::pretty::pretty_format_batches(&batches)
        .expect("batches format")
        .to_string();
    (logical, table)
}

/// Every join statement returns the same schema, rows and row order with the
/// rule installed as without it, and plans a partitioned join either way.
#[tokio::test]
async fn partitioned_join_results_match_the_unrewritten_plan() {
    for sql in DIFFERENTIAL {
        for with_rule in [false, true] {
            let plan = join_context(with_rule)
                .sql(sql)
                .await
                .expect("query plans")
                .create_physical_plan()
                .await
                .unwrap_or_else(|err| panic!("{sql} (rule {with_rule}): {err}"));
            partitioned_join(&plan);
        }
        let (before_schema, before) = run_join(false, sql).await;
        let (after_schema, after) = run_join(true, sql).await;
        assert_eq!(before_schema, after_schema, "{sql}");
        assert_eq!(before, after, "{sql}");
        assert_eq!(before_schema.field(0).data_type(), &dict_type(), "{sql}");
        assert!(before.lines().count() > 6, "{sql} returned too few rows");
    }
    let (_, both_grouped) = run_join(true, DIFFERENTIAL[0]).await;
    let expected = [
        "+---+---+---+",
        "| k | c | c |",
        "+---+---+---+",
        "| a | 4 | 4 |",
        "| b | 3 | 3 |",
        "| c | 2 | 2 |",
        "| x | 1 | 1 |",
        "+---+---+---+",
    ];
    assert_eq!(both_grouped.lines().collect::<Vec<_>>(), expected);
}

/// (a): both inputs carry the views on the bare key, so the join hashes and
/// matches views on both sides and the one restore projection is above it.
#[tokio::test]
async fn partitioned_join_of_two_grouped_inputs_restores_above_the_join() {
    let plan = join_plan(DIFFERENTIAL[0]).await;
    assert_invariants(&plan);
    let join = partitioned_join(&plan);
    let mut all = Vec::new();
    nodes(&plan, &mut all);
    let restores: Vec<_> = all
        .iter()
        .filter(|node| casts_to(node, &dict_type()) > 0)
        .collect();
    let [restore] = restores.as_slice() else {
        panic!("expected exactly one cast to the declared type, found {restores:?}");
    };
    assert!(restore.is::<ProjectionExec>(), "{restore:?}");
    assert!(
        !Arc::ptr_eq(restore, &join)
            && path_to(restore, &|node| Arc::ptr_eq(node, &join)).is_some(),
        "the restore projection is not an ancestor of the join: {restore:?}"
    );
    for input in join.children() {
        assert_eq!(
            column_type(input, "k").as_ref(),
            Some(&DataType::Utf8View),
            "a join input does not carry the key as a view: {input:?}"
        );
    }
}

/// (b): one input carries views and the other reads the scan's dictionary
/// column. The join does not carry; the grouped input is restored below it and
/// hash-partitioned again on the restored dictionary, so both inputs hash the
/// same type on the same key into the same number of partitions.
#[tokio::test]
async fn partitioned_join_with_one_grouped_input_keeps_both_inputs_co_partitioned() {
    let plan = join_plan(DIFFERENTIAL[1]).await;
    assert_invariants(&plan);
    let join = partitioned_join(&plan);
    let inputs = join.children();
    let [left, right] = inputs.as_slice() else {
        panic!("a HashJoinExec has two inputs");
    };
    for input in [left, right] {
        let partitioning = input.properties().output_partitioning();
        let Partitioning::Hash(exprs, count) = partitioning else {
            panic!("a join input is not hash-partitioned: {partitioning:?}");
        };
        assert_eq!(*count, JOIN_PARTITIONS, "{input:?}");
        assert_eq!(exprs.len(), 1, "{input:?}");
    }
    assert_eq!(
        column_type(left, "k"),
        column_type(right, "k"),
        "the join's inputs hash different key types"
    );
    assert_eq!(column_type(left, "k").as_ref(), Some(&dict_type()));
    let batches = join_context(true)
        .sql(DIFFERENTIAL[1])
        .await
        .expect("query plans")
        .collect()
        .await
        .expect("query runs");
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    // One row per scanned non-NULL key: each matches its own group once.
    assert_eq!(rows, 4 + 3 + 2 + 1);
}

/// The `(url, count)` rows of a result whose first column is the declared
/// dictionary type and whose second is an `Int64` count, in result order.
fn url_counts(batches: &[RecordBatch]) -> Vec<(String, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        assert_eq!(batch.column(0).data_type(), &dict_type());
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .expect("group column is a dictionary");
        let values = keys
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("dictionary values are strings");
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is Int64");
        for row in 0..batch.num_rows() {
            let key = values.value(keys.keys().value(row) as usize).to_string();
            rows.push((key, counts.value(row)));
        }
    }
    rows
}

// ---------------------------------------------------------------------------
// A scan that panics on demand
// ---------------------------------------------------------------------------

/// The message the planted panic raises, asserted on so the test cannot pass
/// on some unrelated panic.
const PLANTED_PANIC: &str = "planted operator panic (issue #737)";

/// A table whose scan panics the first time its stream is polled.
///
/// The `SqlExecutor` path cannot plant this: it registers exactly one table
/// provider of its own choosing (`build_session`, security invariant 1). The
/// test drives `PinnedStream` directly instead, which is the same boundary
/// every transport polls through.
#[derive(Debug)]
struct PanickingTable {
    schema: SchemaRef,
}

#[async_trait::async_trait]
impl TableProvider for PanickingTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PanickingExec::new(Arc::clone(&self.schema))))
    }
}

#[derive(Debug)]
struct PanickingExec {
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl PanickingExec {
    fn new(schema: SchemaRef) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        PanickingExec { schema, properties }
    }
}

impl DisplayAs for PanickingExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PanickingExec")
    }
}

impl ExecutionPlan for PanickingExec {
    fn name(&self) -> &str {
        "PanickingExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DFResult<TreeNodeRecursion>,
    ) -> DFResult<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        // Panic inside the stream's poll, which is where an arrow kernel
        // called by a real operator raises one.
        let stream = futures::stream::once(async { panic!("{PLANTED_PANIC}") });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Deliverable 2: a panic inside a DataFusion operator becomes a typed error
/// at the executor's stream boundary, the stream is fused afterwards, and the
/// process is still able to serve the next statement.
#[tokio::test]
async fn a_panicking_operator_surfaces_as_a_typed_error() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let ctx = SessionContext::new();
    ctx.register_table(
        "boom",
        Arc::new(PanickingTable {
            schema: Arc::clone(&schema),
        }),
    )
    .expect("table registers");
    let plan = ctx
        .sql("SELECT v FROM boom")
        .await
        .expect("query plans")
        .create_physical_plan()
        .await
        .expect("physical plan builds");

    // The panic surfaces as a typed error at the stream boundary, which is
    // what the assertions below check; there is nothing to read off a panic
    // hook. Leaving the global hook untouched matters: cargo test runs this
    // binary's tests on parallel threads, so swapping it here would swallow a
    // concurrent test's panic output for the duration of the poll. The default
    // hook printing the planted panic's backtrace is harmless noise.
    let mut stream =
        PinnedStream::start(ctx, plan, schema, CeilingBreach::new()).expect("stream starts");
    let first = stream.next().await;
    let second = stream.next().await;

    let err = match first {
        Some(Err(err)) => err,
        other => panic!("expected a typed error from the panicking operator, got {other:?}"),
    };
    let SqlError::OperatorPanic(detail) = &err else {
        panic!("expected SqlError::OperatorPanic, got {err:?}");
    };
    assert!(
        detail.contains(PLANTED_PANIC),
        "the planted panic's message must survive into the server-side detail: {detail}"
    );
    // ...and no part of it reaches the client.
    assert_eq!(err.client_message(), MSG_INTERNAL);
    assert_eq!(err.class(), ErrorClass::Unsupported);
    assert!(
        second.is_none(),
        "a stream whose poll unwound must be fused, not polled again: {second:?}"
    );

    // The process is intact: a real executor in the same process serves the
    // next statement.
    let executor = declared_url_executor().await;
    let outcome = executor
        .execute(tenant().hash(), &request("SELECT count(*) AS n FROM logs"))
        .await
        .expect("the next statement still runs");
    assert_eq!(outcome.output.batches().len(), 1);
    assert_eq!(
        outcome.output.batches()[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count is Int64")
            .value(0),
        5
    );
}

// ---------------------------------------------------------------------------
// The overflow, at scale
// ---------------------------------------------------------------------------

/// Distinct group keys the stress test builds.
const STRESS_DISTINCT: usize = 600_000;
/// Bytes per key. `STRESS_DISTINCT * STRESS_KEY_LEN` is about 2.46 GB, past
/// `i32::MAX`, which is the only property that matters. The original report
/// reached the same total with roughly ten million keys of 215 bytes; fewer,
/// longer keys get there with a fixture a test can actually build.
const STRESS_KEY_LEN: usize = 4096;
/// Keys per input batch, so no single source array carries more than a few
/// tens of megabytes.
const STRESS_BATCH: usize = 8_192;

/// A session over `distinct` distinct `Dictionary(Int32, Utf8)` keys of
/// `key_len` bytes each, pinned to one partition so every key lands in one
/// group table. `pool`, when given, is installed as the session's memory pool
/// so the caller can read the aggregation's high-water mark off its accounting
/// handle.
fn key_context(
    with_rule: bool,
    distinct: usize,
    key_len: usize,
    pool: Option<Arc<dyn datafusion::execution::memory_pool::MemoryPool>>,
) -> SessionContext {
    use datafusion::datasource::MemTable;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::session_state::SessionStateBuilder;

    let schema = Arc::new(Schema::new(vec![Field::new("k", dict_type(), false)]));
    let mut batches = Vec::new();
    let mut next = 0usize;
    while next < distinct {
        let end = (next + STRESS_BATCH).min(distinct);
        let values: Vec<String> = (next..end)
            .map(|i| {
                let mut s = format!("https://example.test/{i:012}/");
                assert!(
                    key_len >= s.len(),
                    "key_len ({key_len}) must be at least the built prefix length ({}) \
                     so the padding subtraction cannot underflow",
                    s.len()
                );
                s.push_str(&"p".repeat(key_len - s.len()));
                s
            })
            .collect();
        let dict: DictionaryArray<Int32Type> = values.iter().map(|s| Some(s.as_str())).collect();
        batches.push(
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(dict) as ArrayRef])
                .expect("key batch builds"),
        );
        next = end;
    }

    let mut builder = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(1))
        .with_default_features();
    if let Some(pool) = pool {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .build_arc()
            .expect("runtime builds");
        builder = builder.with_runtime_env(runtime);
    }
    if with_rule {
        builder = builder.with_physical_optimizer_rule(Arc::new(DictionaryGroupKeysAsViews));
    }
    let ctx = SessionContext::new_with_state(builder.build());
    let table = MemTable::try_new(schema, vec![batches]).expect("mem table builds");
    ctx.register_table("t", Arc::new(table))
        .expect("table registers");
    ctx
}

fn stress_context(with_rule: bool) -> SessionContext {
    key_context(with_rule, STRESS_DISTINCT, STRESS_KEY_LEN, None)
}

/// Distinct keys the peak-memory measurement uses, and their length. Sized so
/// both halves complete (`MEASURE_DISTINCT * MEASURE_KEY_LEN` is about 51 MB,
/// far under `i32::MAX`): the point here is the group table's cost, and a
/// comparison needs the un-rewritten half to finish.
const MEASURE_DISTINCT: usize = 200_000;
const MEASURE_KEY_LEN: usize = 256;

/// The peak memory-pool bytes a dictionary-keyed `GROUP BY` reaches with and
/// without the rewrite, printed rather than asserted on.
///
/// Environment-gated with the stress test: a threshold here would be a
/// threshold on DataFusion's group-table internals, which is not this repo's
/// contract to pin. What it produces is the number a report or an ADR can
/// quote, measured rather than reasoned about.
#[tokio::test]
async fn stress_report_peak_pool_bytes() {
    if std::env::var("RAVEL_SQL_GROUP_BY_STRESS").as_deref() != Ok("1") {
        eprintln!("skipped: set RAVEL_SQL_GROUP_BY_STRESS=1 to run");
        return;
    }
    for with_rule in [false, true] {
        let accounting = QueryAccounting::new();
        let pool = Arc::new(TenantDelegatingPool::new(
            8 << 30,
            TenantMemoryAccountant::new(8 << 30),
            CeilingBreach::new(),
            accounting.clone(),
        ));
        let ctx = key_context(
            with_rule,
            MEASURE_DISTINCT,
            MEASURE_KEY_LEN,
            Some(pool as Arc<dyn datafusion::execution::memory_pool::MemoryPool>),
        );
        let batches = ctx
            .sql("SELECT k, count(*) AS n FROM t GROUP BY k")
            .await
            .expect("query plans")
            .collect()
            .await
            .expect("query runs");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, MEASURE_DISTINCT);
        let peak = accounting.snapshot().peak_intermediate_bytes;
        let label = if with_rule {
            "Utf8View"
        } else {
            "RowConverter"
        };
        eprintln!(
            "peak_intermediate_bytes[{label}] = {peak} over {MEASURE_DISTINCT} keys \
             of {MEASURE_KEY_LEN} bytes"
        );
    }
}

/// The original failure, reproduced and then fixed, at the scale that produces
/// it. Environment-gated (`RAVEL_SQL_GROUP_BY_STRESS=1`): the fixture alone is
/// about 2.5 GB and the un-rewritten half allocates roughly as much again, so
/// it has no place in the default suite.
///
/// Two runs over the same query and the same data, differing only in whether
/// the rewrite is installed:
///
/// - Without it, `GROUP BY` on the dictionary key falls to `GroupValuesRows`,
///   whose emit decodes every key into one `Utf8` array and overflows its
///   `i32` offsets. That must reach the caller as `SqlError::OperatorPanic`
///   through the stream boundary, never as an escaping panic.
/// - With it, the same query returns every distinct group.
#[tokio::test]
async fn stress_overflowing_group_table_never_panics() {
    if std::env::var("RAVEL_SQL_GROUP_BY_STRESS").as_deref() != Ok("1") {
        eprintln!("skipped: set RAVEL_SQL_GROUP_BY_STRESS=1 to run");
        return;
    }
    let sql = "SELECT k, count(*) AS n FROM t GROUP BY k";

    // Without the rewrite: the RowConverter path, which cannot decode this
    // many bytes into one array.
    let ctx = stress_context(false);
    let plan = ctx
        .sql(sql)
        .await
        .expect("query plans")
        .create_physical_plan()
        .await
        .expect("physical plan builds");
    let schema = plan.schema();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let mut stream =
        PinnedStream::start(ctx, plan, schema, CeilingBreach::new()).expect("stream starts");
    let mut unrewritten_rows = 0usize;
    let mut unrewritten_error = None;
    while let Some(next) = stream.next().await {
        match next {
            Ok(batch) => unrewritten_rows += batch.num_rows(),
            Err(err) => {
                unrewritten_error = Some(err);
                break;
            }
        }
    }
    std::panic::set_hook(previous);
    match unrewritten_error {
        Some(SqlError::OperatorPanic(detail)) => {
            eprintln!("unrewritten path: typed error, detail = {detail}");
            assert!(
                detail.contains("offset overflow"),
                "the unrewritten path must fail on the i32 offset limit: {detail}"
            );
        }
        Some(other) => panic!("unexpected error from the unrewritten path: {other:?}"),
        // A future DataFusion that bounds the emit itself would land here.
        // That is a pass, not a failure: the point is that it did not panic.
        None => {
            eprintln!("unrewritten path: completed with {unrewritten_rows} rows, no overflow");
            assert_eq!(unrewritten_rows, STRESS_DISTINCT);
        }
    }

    // With the rewrite: the full result.
    let rewritten = stress_context(true)
        .sql(sql)
        .await
        .expect("query plans")
        .collect()
        .await
        .expect("the rewritten plan runs to completion");
    let rows: usize = rewritten.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, STRESS_DISTINCT);
    for batch in &rewritten {
        assert_eq!(batch.column(0).data_type(), &dict_type());
    }
}
