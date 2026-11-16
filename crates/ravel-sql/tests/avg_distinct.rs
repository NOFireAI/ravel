//! Issue #2930: `AVG(DISTINCT x)` averages the distinct values in every
//! statement shape.
//!
//! DataFusion's `SingleDistinctToGroupBy` rule rewrites `avg(DISTINCT a)` into
//! a group by only when every distinct aggregate is over the same single
//! expression, every non-distinct aggregate is `sum`, `min` or `max`, and no
//! aggregate carries a FILTER or ORDER BY. Every other shape reaches
//! `crate::avg`'s accumulator with `is_distinct` set, and that accumulator
//! must average the distinct values. These tests drive the real
//! `SqlExecutor::plan_pinned` / `execute` path over a `logs` table with
//! declared `I64` columns `a`, `b` and `g`, spread over several objects so the
//! scan has several partitions, with `parallel_final_aggregation` on and off.
//!
//! Every expected value is computed here from the fixture rows (a set of the
//! distinct values, then the same arithmetic) and compared as raw f64 bits.
//! The statements whose distinct average and plain average coincide would
//! prove nothing, so each statement's expectation is also checked to differ
//! from the plain average it would be confused with.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::physical_plan::displayable;
use ravel_catalog::{Catalog, CatalogConfig, Snapshot};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest,
    StaticDeclaredColumns,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

/// RLOG objects the fixture publishes; also the scan's fan-out at
/// `fetch_concurrency == OBJECTS`, so the plan has this many partitions.
const OBJECTS: usize = 4;

/// Log records per object.
const RECORDS_PER_OBJECT: usize = 30;

fn tenant() -> TenantId {
    TenantId::new("issue-2930-avg-distinct".to_string())
}

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("a", DeclaredType::I64),
        DeclaredColumn::new("b", DeclaredType::I64),
        DeclaredColumn::new("g", DeclaredType::I64),
    ]
}

/// One fixture row.
#[derive(Debug, Clone, Copy)]
struct Row {
    a: i64,
    b: i64,
    g: i64,
}

/// The row record `index` of object `object` carries. `a` repeats unevenly
/// (quadratic residues mod 17, so some values occur several times and others
/// once) and the rows with `g <= 0` draw from a range the rows with `g > 0`
/// never reach, so a `g > 0` filter drops values as well as repeats; `b`
/// repeats over nine values, and `g` takes the four values -1, 0, 1 and 2 in
/// every object.
fn fixture_row(object: usize, index: usize) -> Row {
    let low_g_offset = if index % 4 < 2 { 20 } else { 0 };
    Row {
        a: ((index * index + object) % 17) as i64 - 3 + low_g_offset,
        b: ((index * 5 + object * 2) % 9) as i64 * 10 + 1,
        g: (index % 4) as i64 - 1,
    }
}

fn all_rows() -> Vec<Row> {
    (0..OBJECTS)
        .flat_map(|object| (0..RECORDS_PER_OBJECT).map(move |i| fixture_row(object, i)))
        .collect()
}

fn record(ts: i64, row: Row) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("web".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: String::new(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![
            ("a".to_string(), AttrValue::I64(row.a)),
            ("b".to_string(), AttrValue::I64(row.b)),
            ("g".to_string(), AttrValue::I64(row.g)),
        ],
    }
}

async fn publish_object(store: &dyn ObjectStoreBackend, tenant: &TenantId, object: usize) {
    let base_ts = object as i64 * 100_000;
    let records: Vec<LogRecord> = (0..RECORDS_PER_OBJECT)
        .map(|i| record(base_ts + i as i64, fixture_row(object, i)))
        .collect();

    let identity = ObjectIdentity {
        tenant_hash: tenant.hash().0,
        shard: 0,
        writer_id: *Uuid::from_u128(0x2930_0000 + object as u128).as_bytes(),
        writer_epoch: 1,
        writer_seq: object as u64 + 1,
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity);
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(0x2930_0000 + object as u128),
        writer_epoch: 1,
        writer_seq: object as u64 + 1,
        object_size: bytes.len() as u64,
        content_hash: [object as u8; 32],
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

struct Fixture {
    executor: SqlExecutor,
    catalog: Arc<Catalog>,
}

impl Fixture {
    async fn build(parallel_final_aggregation: bool) -> Self {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let t = tenant();
        for object in 0..OBJECTS {
            publish_object(store.as_ref(), &t, object).await;
        }
        let catalog =
            Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
        let config = SqlConfig {
            engine: EngineConfig {
                fetch_concurrency: OBJECTS,
                ..EngineConfig::default()
            },
            parallel_final_aggregation,
            ..SqlConfig::default()
        };
        let executor = SqlExecutor::new(
            Arc::clone(&catalog),
            SegmentFetcher::new(Arc::clone(&store)),
            LogSegmentFetcher::new(Arc::clone(&store)),
            SpanSegmentFetcher::new(Arc::clone(&store)),
            config,
            1 << 30,
        )
        .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared())));
        Fixture { executor, catalog }
    }

    async fn snapshot(&self) -> Snapshot {
        self.catalog
            .resolve(
                &tenant().hash(),
                Signal::Logs,
                TimeRange {
                    start_ns: 0,
                    end_ns: 1_000_000,
                },
                &[],
                1_000_000,
            )
            .await
            .expect("resolve logs snapshot")
    }

    /// The indented physical plan for `sql` through `plan_pinned`, with each
    /// operator's output schema so the `Partial` state types are visible.
    async fn plan(&self, sql: &str) -> String {
        let accounting = QueryAccounting::new();
        let planned = self
            .executor
            .plan_pinned(
                tenant().hash(),
                self.snapshot().await,
                sql,
                &accounting,
                &declared(),
            )
            .await
            .expect("plan_pinned");
        let plan = planned
            .create_physical_plan()
            .await
            .expect("physical plan builds");
        format!(
            "{}",
            displayable(plan.as_ref())
                .set_show_schema(true)
                .indent(true)
        )
    }

    /// Every row `sql` returns, each cell rendered by [`render_column`], sorted.
    async fn rows(&self, sql: &str) -> Vec<Vec<String>> {
        let outcome = self
            .executor
            .execute(tenant().hash(), &request(sql))
            .await
            .expect("query executes");
        let mut rows = Vec::new();
        for batch in outcome.output.batches() {
            let columns: Vec<Vec<String>> = batch.columns().iter().map(render_column).collect();
            for row in 0..batch.num_rows() {
                rows.push(columns.iter().map(|column| column[row].clone()).collect());
            }
        }
        rows.sort();
        rows
    }
}

/// A Float64 cell as its raw bits, so row comparisons are bit-exact; any other
/// cell as its Utf8 cast.
fn render_column(array: &ArrayRef) -> Vec<String> {
    if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
        return (0..floats.len())
            .map(|i| {
                if floats.is_null(i) {
                    "null".to_string()
                } else {
                    float_cell(floats.value(i))
                }
            })
            .collect();
    }
    let text = cast(array, &DataType::Utf8).expect("result column casts to Utf8");
    let text = text
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a Utf8 cast yields a StringArray");
    (0..text.len())
        .map(|i| {
            if text.is_null(i) {
                "null".to_string()
            } else {
                text.value(i).to_string()
            }
        })
        .collect()
}

fn float_cell(value: f64) -> String {
    format!("f64:{:#018x}", value.to_bits())
}

fn avg_cell(value: Option<f64>) -> String {
    value.map_or_else(|| "null".to_string(), float_cell)
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
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// The average of the distinct integers in `values`: the exact sum of the set
/// converted to f64 once, divided by the set size in one IEEE division. `None`
/// for an empty set.
fn distinct_int_avg(values: impl IntoIterator<Item = i64>) -> Option<f64> {
    let set: BTreeSet<i64> = values.into_iter().collect();
    if set.is_empty() {
        return None;
    }
    let sum: i128 = set.iter().map(|&v| i128::from(v)).sum();
    Some(sum as f64 / set.len() as f64)
}

/// The plain average of `values`, the same arithmetic over the multiset.
fn plain_int_avg(values: impl IntoIterator<Item = i64>) -> Option<f64> {
    let values: Vec<i64> = values.into_iter().collect();
    if values.is_empty() {
        return None;
    }
    let sum: i128 = values.iter().map(|&v| i128::from(v)).sum();
    Some(sum as f64 / values.len() as f64)
}

/// The distinct values of `values` under bit equality, sorted by
/// `f64::total_cmp`.
fn distinct_floats(values: impl IntoIterator<Item = f64>) -> Vec<f64> {
    let mut values: Vec<f64> = values.into_iter().collect();
    values.sort_by(f64::total_cmp);
    values.dedup_by(|x, y| x.to_bits() == y.to_bits());
    values
}

/// The average of the distinct floats in `values`: a naive sequential fold of
/// the set in total order, seeded with its first value, divided once.
fn distinct_float_avg(values: impl IntoIterator<Item = f64>) -> Option<f64> {
    let set = distinct_floats(values);
    let (first, rest) = set.split_first()?;
    let sum = rest.iter().fold(*first, |acc, v| acc + v);
    Some(sum / set.len() as f64)
}

/// The plain average of `values`, the same arithmetic over the multiset: a
/// sequential fold in iteration order, seeded with its first value, divided
/// once.
fn plain_float_avg(values: impl IntoIterator<Item = f64>) -> Option<f64> {
    let values: Vec<f64> = values.into_iter().collect();
    let (first, rest) = values.split_first()?;
    let sum = rest.iter().fold(*first, |acc, v| acc + v);
    Some(sum / values.len() as f64)
}

/// Group `rows` by `g`, in key order.
fn by_g(rows: &[Row]) -> BTreeMap<i64, Vec<Row>> {
    let mut groups: BTreeMap<i64, Vec<Row>> = BTreeMap::new();
    for row in rows {
        groups.entry(row.g).or_default().push(*row);
    }
    groups
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

/// Run `sql` with `parallel_final_aggregation` on and off and require both to
/// return `expected`.
async fn assert_rows(sql: &str, expected: Vec<Vec<String>>) {
    let expected = sorted(expected);
    for parallel in [true, false] {
        let fixture = Fixture::build(parallel).await;
        assert_eq!(
            fixture.rows(sql).await,
            expected,
            "{sql}\nwith parallel_final_aggregation = {parallel}"
        );
    }
}

/// Require the distinct and the plain expectation to differ, so a statement
/// that silently averaged the plain multiset could not pass.
fn assert_not_the_plain_average(distinct: &[Vec<String>], plain: &[Vec<String>], what: &str) {
    assert_ne!(
        sorted(distinct.to_vec()),
        sorted(plain.to_vec()),
        "precondition: the fixture must tell the distinct average from the plain one for {what}"
    );
}

#[tokio::test]
async fn distinct_averages_over_two_arguments() {
    let rows = all_rows();
    let distinct = vec![vec![
        avg_cell(distinct_int_avg(rows.iter().map(|r| r.a))),
        avg_cell(distinct_int_avg(rows.iter().map(|r| r.b))),
    ]];
    let plain = vec![vec![
        avg_cell(plain_int_avg(rows.iter().map(|r| r.a))),
        avg_cell(plain_int_avg(rows.iter().map(|r| r.b))),
    ]];
    assert_not_the_plain_average(&distinct, &plain, "avg(DISTINCT a), avg(DISTINCT b)");
    assert_rows(
        "SELECT avg(DISTINCT a), avg(DISTINCT b) FROM logs",
        distinct,
    )
    .await;
}

#[tokio::test]
async fn distinct_average_beside_a_plain_average() {
    let rows = all_rows();
    let distinct = vec![vec![
        avg_cell(distinct_int_avg(rows.iter().map(|r| r.a))),
        avg_cell(plain_int_avg(rows.iter().map(|r| r.b))),
    ]];
    let plain = vec![vec![
        avg_cell(plain_int_avg(rows.iter().map(|r| r.a))),
        avg_cell(plain_int_avg(rows.iter().map(|r| r.b))),
    ]];
    assert_not_the_plain_average(&distinct, &plain, "avg(DISTINCT a), avg(b)");
    assert_rows("SELECT avg(DISTINCT a), avg(b) FROM logs", distinct).await;
}

#[tokio::test]
async fn distinct_average_with_a_filter_clause() {
    let rows = all_rows();
    let kept = |r: &&Row| r.g > 0;
    let distinct = vec![vec![avg_cell(distinct_int_avg(
        rows.iter().filter(kept).map(|r| r.a),
    ))]];
    let plain = vec![vec![avg_cell(plain_int_avg(
        rows.iter().filter(kept).map(|r| r.a),
    ))]];
    assert_not_the_plain_average(&distinct, &plain, "avg(DISTINCT a) FILTER (WHERE g > 0)");
    // The filter must drop some values the unfiltered set holds, and keep
    // repeats among the rest, or it exercises nothing a plain average would not.
    let all_set: BTreeSet<i64> = rows.iter().map(|r| r.a).collect();
    let kept_set: BTreeSet<i64> = rows.iter().filter(kept).map(|r| r.a).collect();
    let kept_count = rows.iter().filter(kept).count();
    assert!(kept_set.len() < all_set.len(), "the filter drops a value");
    assert!(kept_set.len() < kept_count, "the filter keeps repeats");
    assert_rows(
        "SELECT avg(DISTINCT a) FILTER (WHERE g > 0) FROM logs",
        distinct,
    )
    .await;
}

/// A FILTER clause on a different aggregate in the same statement, not on the
/// DISTINCT average itself: DataFusion's `SingleDistinctToGroupBy` rule
/// refuses to rewrite when ANY aggregate in the statement carries a FILTER,
/// not only when the DISTINCT aggregate does.
#[tokio::test]
async fn distinct_average_beside_a_filter_on_another_aggregate() {
    let rows = all_rows();
    let sum_b_filtered: i64 = rows.iter().filter(|r| r.g > 0).map(|r| r.b).sum();
    let distinct = vec![vec![
        avg_cell(distinct_int_avg(rows.iter().map(|r| r.a))),
        sum_b_filtered.to_string(),
    ]];
    let plain = vec![vec![
        avg_cell(plain_int_avg(rows.iter().map(|r| r.a))),
        sum_b_filtered.to_string(),
    ]];
    assert_not_the_plain_average(
        &distinct,
        &plain,
        "avg(DISTINCT a), sum(b) FILTER (WHERE g > 0)",
    );
    assert_rows(
        "SELECT avg(DISTINCT a), sum(b) FILTER (WHERE g > 0) FROM logs",
        distinct,
    )
    .await;
}

/// One expected row per `g`: the group key, then the cells `render` makes of
/// that group's fixture rows.
fn grouped(render: &dyn Fn(&[Row]) -> Vec<String>) -> Vec<Vec<String>> {
    by_g(&all_rows())
        .iter()
        .map(|(g, rows)| {
            let mut row = vec![g.to_string()];
            row.extend(render(rows));
            row
        })
        .collect()
}

#[tokio::test]
async fn grouped_distinct_averages_over_two_arguments() {
    let distinct = grouped(&|rows| {
        vec![
            avg_cell(distinct_int_avg(rows.iter().map(|r| r.a))),
            avg_cell(distinct_int_avg(rows.iter().map(|r| r.b))),
        ]
    });
    let plain = grouped(&|rows| {
        vec![
            avg_cell(plain_int_avg(rows.iter().map(|r| r.a))),
            avg_cell(plain_int_avg(rows.iter().map(|r| r.b))),
        ]
    });
    assert_not_the_plain_average(&distinct, &plain, "grouped two distinct averages");
    assert_rows(
        "SELECT g, avg(DISTINCT a), avg(DISTINCT b) FROM logs GROUP BY g",
        distinct,
    )
    .await;
}

#[tokio::test]
async fn grouped_distinct_average_beside_a_plain_average() {
    let distinct = grouped(&|rows| {
        vec![
            avg_cell(distinct_int_avg(rows.iter().map(|r| r.a))),
            avg_cell(plain_int_avg(rows.iter().map(|r| r.b))),
        ]
    });
    let plain = grouped(&|rows| {
        vec![
            avg_cell(plain_int_avg(rows.iter().map(|r| r.a))),
            avg_cell(plain_int_avg(rows.iter().map(|r| r.b))),
        ]
    });
    assert_not_the_plain_average(&distinct, &plain, "grouped distinct beside plain");
    assert_rows(
        "SELECT g, avg(DISTINCT a), avg(b) FROM logs GROUP BY g",
        distinct,
    )
    .await;
}

/// A FILTER on the grouping column: the groups it empties average to NULL.
#[tokio::test]
async fn grouped_distinct_average_with_a_filter_on_the_group_key() {
    let distinct = grouped(&|rows| {
        vec![avg_cell(distinct_int_avg(
            rows.iter().filter(|r| r.g > 0).map(|r| r.a),
        ))]
    });
    let plain = grouped(&|rows| {
        vec![avg_cell(plain_int_avg(
            rows.iter().filter(|r| r.g > 0).map(|r| r.a),
        ))]
    });
    assert_not_the_plain_average(&distinct, &plain, "grouped distinct with a filter on g");
    assert!(
        distinct.iter().any(|row| row[1] == "null"),
        "precondition: the g > 0 filter empties some group"
    );
    assert_rows(
        "SELECT g, avg(DISTINCT a) FILTER (WHERE g > 0) FROM logs GROUP BY g",
        distinct,
    )
    .await;
}

/// A FILTER on another column: every group keeps some rows, and the filter
/// drops some repeats of a value while keeping others.
#[tokio::test]
async fn grouped_distinct_average_with_a_filter_on_another_column() {
    let distinct = grouped(&|rows| {
        vec![avg_cell(distinct_int_avg(
            rows.iter().filter(|r| r.b > 30).map(|r| r.a),
        ))]
    });
    let plain = grouped(&|rows| {
        vec![avg_cell(plain_int_avg(
            rows.iter().filter(|r| r.b > 30).map(|r| r.a),
        ))]
    });
    assert_not_the_plain_average(&distinct, &plain, "grouped distinct with a filter on b");
    assert_rows(
        "SELECT g, avg(DISTINCT a) FILTER (WHERE b > 30) FROM logs GROUP BY g",
        distinct,
    )
    .await;
}

/// The float kind: `CAST(a AS DOUBLE)` makes `avg` fold Float64 values. The
/// scaled variant divides by ten so the values are not exactly representable;
/// for the `b` set, the sum depends on the order it is folded in (asserted
/// below), which is why the accumulator's fold order must be pinned.
#[tokio::test]
async fn float_distinct_averages_ungrouped() {
    let rows = all_rows();
    let as_double = |v: i64| v as f64;
    let scaled = |v: i64| v as f64 / 10.0;

    let distinct = vec![vec![
        avg_cell(distinct_float_avg(rows.iter().map(|r| as_double(r.a)))),
        avg_cell(distinct_float_avg(rows.iter().map(|r| as_double(r.b)))),
    ]];
    let plain = vec![vec![
        avg_cell(plain_float_avg(rows.iter().map(|r| as_double(r.a)))),
        avg_cell(plain_float_avg(rows.iter().map(|r| as_double(r.b)))),
    ]];
    assert_not_the_plain_average(
        &distinct,
        &plain,
        "avg(DISTINCT CAST(a AS DOUBLE)), avg(DISTINCT CAST(b AS DOUBLE))",
    );
    assert_rows(
        "SELECT avg(DISTINCT CAST(a AS DOUBLE)), avg(DISTINCT CAST(b AS DOUBLE)) FROM logs",
        distinct,
    )
    .await;

    let b_scaled_set = distinct_floats(rows.iter().map(|r| scaled(r.b)));
    let mut b_scaled_reversed = b_scaled_set.clone();
    b_scaled_reversed.reverse();
    let fold_from = |set: &[f64]| -> f64 {
        let (first, rest) = set.split_first().expect("nonempty set");
        rest.iter().fold(*first, |acc, v| acc + v)
    };
    assert_ne!(
        fold_from(&b_scaled_set).to_bits(),
        fold_from(&b_scaled_reversed).to_bits(),
        "precondition: folding the scaled distinct b set in sorted order and in \
         reversed order must give different bits"
    );

    let distinct = vec![vec![
        avg_cell(distinct_float_avg(rows.iter().map(|r| scaled(r.a)))),
        avg_cell(distinct_float_avg(rows.iter().map(|r| scaled(r.b)))),
    ]];
    let plain = vec![vec![
        avg_cell(plain_float_avg(rows.iter().map(|r| scaled(r.a)))),
        avg_cell(plain_float_avg(rows.iter().map(|r| scaled(r.b)))),
    ]];
    assert_not_the_plain_average(
        &distinct,
        &plain,
        "avg(DISTINCT CAST(a AS DOUBLE) / 10.0), avg(DISTINCT CAST(b AS DOUBLE) / 10.0)",
    );
    assert_rows(
        "SELECT avg(DISTINCT CAST(a AS DOUBLE) / 10.0), avg(DISTINCT CAST(b AS DOUBLE) / 10.0) FROM logs",
        distinct,
    )
    .await;
}

#[tokio::test]
async fn float_distinct_averages_grouped() {
    let expected = grouped(&|rows| {
        vec![
            avg_cell(distinct_float_avg(rows.iter().map(|r| r.a as f64))),
            avg_cell(distinct_float_avg(rows.iter().map(|r| r.b as f64))),
        ]
    });
    let plain = grouped(&|rows| {
        vec![
            avg_cell(plain_float_avg(rows.iter().map(|r| r.a as f64))),
            avg_cell(plain_float_avg(rows.iter().map(|r| r.b as f64))),
        ]
    });
    assert_not_the_plain_average(
        &expected,
        &plain,
        "grouped avg(DISTINCT CAST(a AS DOUBLE)), avg(DISTINCT CAST(b AS DOUBLE))",
    );
    assert_rows(
        "SELECT g, avg(DISTINCT CAST(a AS DOUBLE)), avg(DISTINCT CAST(b AS DOUBLE)) FROM logs GROUP BY g",
        expected,
    )
    .await;

    let expected = grouped(&|rows| {
        vec![
            avg_cell(distinct_float_avg(rows.iter().map(|r| r.a as f64 / 10.0))),
            avg_cell(plain_int_avg(rows.iter().map(|r| r.b))),
        ]
    });
    let plain = grouped(&|rows| {
        vec![
            avg_cell(plain_float_avg(rows.iter().map(|r| r.a as f64 / 10.0))),
            avg_cell(plain_int_avg(rows.iter().map(|r| r.b))),
        ]
    });
    assert_not_the_plain_average(
        &expected,
        &plain,
        "grouped avg(DISTINCT CAST(a AS DOUBLE) / 10.0), avg(b)",
    );
    assert_rows(
        "SELECT g, avg(DISTINCT CAST(a AS DOUBLE) / 10.0), avg(b) FROM logs GROUP BY g",
        expected,
    )
    .await;
}

/// `count(DISTINCT x)` over a Float64 is DataFusion's own distinct set, so the
/// count it returns is the set size the average's keying must agree with.
#[tokio::test]
async fn float_distinct_count_agrees_with_the_distinct_set_the_average_uses() {
    let rows = all_rows();
    let a_values = distinct_floats(rows.iter().map(|r| r.a as f64));
    let expected = vec![vec![
        a_values.len().to_string(),
        avg_cell(distinct_float_avg(rows.iter().map(|r| r.a as f64))),
        avg_cell(distinct_float_avg(rows.iter().map(|r| r.b as f64))),
    ]];
    let plain = vec![vec![
        a_values.len().to_string(),
        avg_cell(plain_float_avg(rows.iter().map(|r| r.a as f64))),
        avg_cell(plain_float_avg(rows.iter().map(|r| r.b as f64))),
    ]];
    assert_not_the_plain_average(
        &expected,
        &plain,
        "count(DISTINCT CAST(a AS DOUBLE)), avg(DISTINCT CAST(a AS DOUBLE)), \
         avg(DISTINCT CAST(b AS DOUBLE))",
    );
    assert_rows(
        "SELECT count(DISTINCT CAST(a AS DOUBLE)), avg(DISTINCT CAST(a AS DOUBLE)), \
         avg(DISTINCT CAST(b AS DOUBLE)) FROM logs",
        expected,
    )
    .await;

    // Positive and negative zero are two values to `count(DISTINCT)`; the
    // average must count them as two as well.
    let x = |g: i64| -> f64 {
        if g > 0 {
            -0.0
        } else if g == 0 {
            0.0
        } else {
            1.0
        }
    };
    let signed_zero_set = distinct_floats(rows.iter().map(|r| x(r.g)));
    assert_eq!(
        signed_zero_set.len(),
        3,
        "precondition: -0.0, 0.0 and 1.0 are three values"
    );
    let expected = vec![vec![
        signed_zero_set.len().to_string(),
        avg_cell(distinct_float_avg(rows.iter().map(|r| x(r.g)))),
        avg_cell(distinct_float_avg(rows.iter().map(|r| r.b as f64))),
    ]];
    let plain = vec![vec![
        signed_zero_set.len().to_string(),
        avg_cell(plain_float_avg(rows.iter().map(|r| x(r.g)))),
        avg_cell(plain_float_avg(rows.iter().map(|r| r.b as f64))),
    ]];
    assert_not_the_plain_average(
        &expected,
        &plain,
        "count(DISTINCT x), avg(DISTINCT x), avg(DISTINCT CAST(b AS DOUBLE))",
    );
    assert_rows(
        "SELECT count(DISTINCT x), avg(DISTINCT x), avg(DISTINCT CAST(b AS DOUBLE)) FROM \
         (SELECT b, CASE WHEN g > 0 THEN CAST(-1 AS DOUBLE) * CAST(0 AS DOUBLE) \
          WHEN g = 0 THEN CAST(0 AS DOUBLE) ELSE CAST(1 AS DOUBLE) END AS x FROM logs) AS t",
        expected,
    )
    .await;
}

/// Shapes DataFusion rewrites into a group by before the accumulator sees
/// them: they were correct before the fix and must stay correct.
#[tokio::test]
async fn rewritten_distinct_averages_stay_correct() {
    let rows = all_rows();
    let avg_a = avg_cell(distinct_int_avg(rows.iter().map(|r| r.a)));
    let distinct_a_count = rows.iter().map(|r| r.a).collect::<BTreeSet<_>>().len();
    let sum_b: i64 = rows.iter().map(|r| r.b).sum();

    assert_rows(
        "SELECT avg(DISTINCT a) FROM logs",
        vec![vec![avg_a.clone()]],
    )
    .await;
    assert_rows(
        "SELECT avg(DISTINCT a), count(DISTINCT a) FROM logs",
        vec![vec![avg_a.clone(), distinct_a_count.to_string()]],
    )
    .await;
    assert_rows(
        "SELECT avg(DISTINCT a), sum(b) FROM logs",
        vec![vec![avg_a, sum_b.to_string()]],
    )
    .await;
}

/// `min` and `max` give the same answer with or without DISTINCT, so the
/// owned total-order UDAFs need not read `is_distinct`. A non-rewritten shape
/// (two different distinct arguments beside a plain `avg`) keeps the DISTINCT
/// on the aggregate the accumulator sees, asserted below from the physical
/// plan rather than taken on faith.
#[tokio::test]
async fn distinct_min_and_max_equal_the_plain_ones() {
    let on = Fixture::build(true).await;
    let plan = on
        .plan("SELECT min(DISTINCT a), max(DISTINCT b), avg(b) FROM logs")
        .await;
    assert!(
        plan.contains("DISTINCT logs.a") && plan.contains("DISTINCT logs.b"),
        "a non-rewritten min/max shape must keep DISTINCT on the aggregate the \
         accumulator sees; got:\n{plan}"
    );

    let rows = all_rows();
    let min_a = rows.iter().map(|r| r.a).min().expect("rows");
    let max_b = rows.iter().map(|r| r.b).max().expect("rows");
    assert_rows(
        "SELECT min(DISTINCT a), max(DISTINCT b), min(a), max(b), avg(b) FROM logs",
        vec![vec![
            min_a.to_string(),
            max_b.to_string(),
            min_a.to_string(),
            max_b.to_string(),
            avg_cell(plain_int_avg(rows.iter().map(|r| r.b))),
        ]],
    )
    .await;

    let float_min = rows
        .iter()
        .map(|r| r.a as f64)
        .fold(f64::INFINITY, f64::min);
    let float_max = rows
        .iter()
        .map(|r| r.b as f64)
        .fold(f64::NEG_INFINITY, f64::max);
    assert_rows(
        "SELECT min(DISTINCT CAST(a AS DOUBLE)), max(DISTINCT CAST(b AS DOUBLE)), \
         min(CAST(a AS DOUBLE)), max(CAST(b AS DOUBLE)), avg(b) FROM logs",
        vec![vec![
            avg_cell(Some(float_min)),
            avg_cell(Some(float_max)),
            avg_cell(Some(float_min)),
            avg_cell(Some(float_max)),
            avg_cell(plain_int_avg(rows.iter().map(|r| r.b))),
        ]],
    )
    .await;
}

fn line_of(plan: &str, needle: &str) -> usize {
    plan.lines()
        .position(|l| l.contains(needle))
        .unwrap_or_else(|| panic!("expected a plan line containing {needle:?}; got:\n{plan}"))
}

/// A Float64 average is not admitted to the partitioned final aggregation,
/// distinct or not, so the float distinct average keeps a single-partition
/// `Final` and no hash repartition.
#[tokio::test]
async fn float_distinct_average_keeps_a_single_partition_final() {
    let on = Fixture::build(true).await;
    let plan = on
        .plan(
            "SELECT g, avg(DISTINCT CAST(a AS DOUBLE)), avg(DISTINCT CAST(b AS DOUBLE)) \
             FROM logs GROUP BY g",
        )
        .await;
    assert!(
        !plan.contains("AggregateExec: mode=FinalPartitioned")
            && !plan.contains("RepartitionExec: partitioning=Hash")
            && plan.contains("AggregateExec: mode=Final,"),
        "a float distinct average must keep a single-partition Final; got:\n{plan}"
    );
    assert!(
        plan.contains("[avg_distinct]"),
        "the plan must carry the distinct state; got:\n{plan}"
    );
}

/// The classifier calls an integer distinct average exact: a set-union merge
/// of the partial sets is the same set however the rows are partitioned, so
/// the final aggregation fans out over a hash repartition and still answers
/// the exact distinct averages.
#[tokio::test]
async fn integer_distinct_average_plans_final_partitioned_and_is_exact() {
    let sql = "SELECT g, avg(DISTINCT a), avg(DISTINCT b) FROM logs GROUP BY g";
    let on = Fixture::build(true).await;
    let plan = on.plan(sql).await;
    assert!(
        plan.contains("AggregateExec: mode=FinalPartitioned"),
        "an exact integer distinct average must fan its final aggregation out; got:\n{plan}"
    );
    assert!(
        plan.contains("RepartitionExec: partitioning=Hash"),
        "the fanned-out final aggregate must be fed by a hash repartition; got:\n{plan}"
    );
    assert!(
        line_of(&plan, "AggregateExec: mode=FinalPartitioned")
            < line_of(&plan, "RepartitionExec: partitioning=Hash"),
        "FinalPartitioned must sit above the hash repartition; got:\n{plan}"
    );
    let partial = plan
        .lines()
        .find(|line| line.contains("AggregateExec: mode=Partial"))
        .unwrap_or_else(|| panic!("expected a Partial AggregateExec; got:\n{plan}"));
    assert!(
        partial.contains("[avg_distinct]") && !partial.contains("[avg_sum]"),
        "a distinct average carries a set as its partial state, not a sum and a count; \
         got:\n{partial}"
    );

    let expected: Vec<Vec<String>> = by_g(&all_rows())
        .iter()
        .map(|(g, rows)| {
            vec![
                g.to_string(),
                avg_cell(distinct_int_avg(rows.iter().map(|r| r.a))),
                avg_cell(distinct_int_avg(rows.iter().map(|r| r.b))),
            ]
        })
        .collect();
    assert_rows(sql, expected).await;
}
