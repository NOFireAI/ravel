//! `ravel-cli sql explain` (ADR-2867 change 7 item 3, issue #2881), driven
//! through the library entry point `main.rs` dispatches to, against logs
//! loaded by the real `load --parquet` path into a shared `MemoryStore`.

#![cfg(feature = "sql-explain")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::load;
use ravel_cli::sql_explain::{self, ExplainOptions, UNAUDITED_REQUIRED};
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_ingest::Clock;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    SpanSegmentFetcher, SqlConfig, SqlError, SqlExecutor, SqlRequest, ValidationError,
};
use ravel_types::{TenantId, TimeRange};

const CLOCK_NS: i64 = 1_700_000_000_000_000_000; // 2023-11-14T22:13:20Z
const NOW_NS: i64 = CLOCK_NS + 1_000;
const TENANT: &str = "sql-explain-2881";

/// Ten records over four bodies: `a` four times, `b` three, `c` two, `d`
/// once. Grouping by body gives four rows; keeping groups of more than one
/// gives three.
const BODIES: [&str; 10] = ["a", "b", "c", "a", "b", "a", "d", "c", "b", "a"];
const GROUPED: &str = "SELECT body, count(*) AS n FROM logs GROUP BY body";
const GROUPED_ROWS: usize = 4;
/// A single-partition final aggregate (`avg` is not exact-typed) fanned back
/// out to the session's partitions for the `HAVING` filter, the shape the
/// `CompactSlices` rule compacts (crates/ravel-sql/tests/compact_slices.rs).
const HAVING: &str = "SELECT body, count(*) AS n, min(CAST(ts AS VARCHAR)) AS s, \
                      avg(length(body)) AS a FROM logs GROUP BY body \
                      HAVING count(*) > 1 ORDER BY 2 DESC LIMIT 10";
const HAVING_ROWS: usize = 3;

struct FixedClock(i64);
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

async fn loaded_store() -> Arc<dyn ObjectStoreBackend> {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("logs.parquet");
    let ts: ArrayRef = Arc::new(Int64Array::from(
        (0..BODIES.len() as i64)
            .map(|i| CLOCK_NS + i)
            .collect::<Vec<_>>(),
    ));
    let body: ArrayRef = Arc::new(StringArray::from(BODIES.to_vec()));
    let batch = RecordBatch::try_from_iter(vec![("ts", ts), ("body", body)]).expect("batch");
    write_parquet(&pq, &batch);
    let mapping = load::parse_mapping(
        r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"
"#,
    )
    .expect("mapping");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load::load(
        Arc::clone(&store),
        &pq,
        TENANT,
        &mapping,
        4,
        10_000,
        None,
        1,
        CLOCK_NS,
        Arc::new(FixedClock(CLOCK_NS)),
    )
    .await
    .expect("load succeeds");
    assert_eq!(report.rows_processed, BODIES.len() as u64);
    store
}

fn options(sql: &str, analyze: bool) -> ExplainOptions {
    ExplainOptions {
        tenant: TENANT.to_string(),
        sql: sql.to_string(),
        analyze,
        unaudited: true,
        start_ns: None,
        end_ns: None,
        shards: 4,
        max_ingest_lag_ns: None,
        deadline: Duration::from_secs(30),
        max_rows: None,
        sql_partitions: None,
    }
}

async fn explain(
    store: Arc<dyn ObjectStoreBackend>,
    opts: &ExplainOptions,
) -> anyhow::Result<String> {
    let mut out = Vec::new();
    sql_explain::run(
        store,
        StoreSelection::explicit(StoreKind::Memory),
        opts,
        NOW_NS,
        &mut out,
    )
    .await?;
    Ok(String::from_utf8(out).expect("utf-8 output"))
}

/// The value of `name=N` on `line`, when the line carries one.
fn metric(line: &str, name: &str) -> Option<usize> {
    let at = line.find(&format!("{name}="))? + name.len() + 1;
    let digits: String = line[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

fn operator_line<'a>(output: &'a str, operator: &str) -> &'a str {
    output
        .lines()
        .find(|line| line.trim_start().starts_with(operator))
        .unwrap_or_else(|| panic!("no {operator} line:\n{output}"))
}

/// `--analyze` prints the executed plan: the final aggregate reports the
/// groups it produced as `output_rows` and its own `elapsed_compute`, and the
/// trailing line is the exact row count.
#[tokio::test]
async fn analyze_prints_per_operator_metrics_and_the_row_count() {
    let output = explain(loaded_store().await, &options(GROUPED, true))
        .await
        .expect("sql explain --analyze");

    let aggregate = operator_line(&output, "AggregateExec: mode=Final");
    assert_eq!(
        metric(aggregate, "output_rows"),
        Some(GROUPED_ROWS),
        "{output}"
    );
    assert!(
        aggregate.contains("elapsed_compute="),
        "the aggregate reports its own elapsed_compute:\n{output}"
    );
    assert_eq!(
        output.lines().last(),
        Some(format!("rows: {GROUPED_ROWS}").as_str()),
        "{output}"
    );
}

/// A plan that holds a `CompactSlicesExec` prints that node with its own
/// metrics, and the row count is the `HAVING` result's. Four partitions make
/// the planner fan the final aggregate's output back out for the filter.
#[tokio::test]
async fn analyze_prints_compact_slices_metrics() {
    let mut opts = options(HAVING, true);
    opts.sql_partitions = Some(4);
    let output = explain(loaded_store().await, &opts)
        .await
        .expect("sql explain --analyze");

    // The node's own counters account for every batch the aggregate below
    // it emitted: each one was either compacted or passed through.
    let compact = operator_line(&output, "CompactSlicesExec");
    assert!(compact.contains("metrics=["), "{output}");
    let compacted = metric(compact, "compacted_batches").expect("compacted_batches");
    let passed = metric(compact, "passed_through_batches").expect("passed_through_batches");
    let aggregate = operator_line(&output, "AggregateExec: mode=FinalPartitioned");
    assert_eq!(
        Some(compacted + passed),
        metric(aggregate, "output_batches"),
        "{output}"
    );
    assert!(compacted > 0, "{output}");
    assert_eq!(
        output.lines().last(),
        Some(format!("rows: {HAVING_ROWS}").as_str()),
        "{output}"
    );
}

/// Without `--analyze` the command prints the plan the executor's `explain`
/// renders, which ran nothing and so carries no metrics.
#[tokio::test]
async fn explain_without_analyze_prints_the_plan_only() {
    let output = explain(loaded_store().await, &options(GROUPED, false))
        .await
        .expect("sql explain");
    operator_line(&output, "AggregateExec: mode=Final");
    assert!(!output.contains("metrics=["), "{output}");
    assert!(!output.contains("rows:"), "{output}");
}

/// The executor still refuses an `EXPLAIN` statement through `execute`, the
/// path the HTTP endpoint takes: the subcommand renders plans itself rather
/// than admitting `EXPLAIN` into the gate.
#[tokio::test]
async fn explain_statement_through_execute_is_refused() {
    let store = loaded_store().await;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    );
    let req = SqlRequest {
        sql: format!("EXPLAIN ANALYZE {GROUPED}"),
        window: TimeRange {
            start_ns: CLOCK_NS - 3_600_000_000_000,
            end_ns: NOW_NS,
        },
        min_tokens: Vec::new(),
        now_ns: NOW_NS,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    };
    let err = executor
        .execute(TenantId::new(TENANT).hash(), &req)
        .await
        .expect_err("EXPLAIN is not admitted");
    assert!(
        matches!(
            err,
            SqlError::Validation(ValidationError::NotReadOnly { .. })
        ),
        "{err:?}"
    );

    // The subcommand passes its statement through the same gate.
    let err = explain(store, &options(&req.sql, true))
        .await
        .expect_err("sql explain refuses an EXPLAIN statement");
    assert!(
        matches!(
            err.downcast_ref::<SqlError>(),
            Some(SqlError::Validation(ValidationError::NotReadOnly { .. }))
        ),
        "{err:?}"
    );
}

/// A store that throttles every operation, so any request the command makes
/// shows up on the fault counters.
fn throttling_store() -> Arc<FaultStore<MemoryStore>> {
    let throttled = ScriptedFault::Throttled { retry_after_ms: 0 };
    let plan = [Op::Put, Op::Get, Op::Head, Op::List, Op::Delete]
        .into_iter()
        .fold(FaultPlan::empty(), |plan, op| {
            plan.with_rule(Rule::new(op, throttled.clone()))
        });
    Arc::new(FaultStore::new(MemoryStore::new(), plan))
}

fn requests(store: &FaultStore<MemoryStore>) -> u64 {
    [Op::Put, Op::Get, Op::Head, Op::List, Op::Delete]
        .into_iter()
        .map(|op| store.fault_count(op, FaultKind::Throttled))
        .sum()
}

/// Without `--unaudited` the command refuses before its first store request.
/// The same store with `--unaudited` does reach it, which is what shows the
/// counter would have moved.
#[tokio::test]
async fn refuses_without_unaudited_before_any_store_request() {
    let store = throttling_store();
    let mut opts = options(GROUPED, true);
    opts.unaudited = false;
    let err = explain(store.clone(), &opts)
        .await
        .expect_err("refused without --unaudited");
    assert_eq!(err.to_string(), UNAUDITED_REQUIRED);
    assert_eq!(requests(&store), 0);

    opts.unaudited = true;
    explain(store.clone(), &opts)
        .await
        .expect_err("every request is throttled");
    assert!(requests(&store) > 0);
}

/// The compiled binary refuses before building its store: with `--store s3`
/// and no bucket configured, the refusal is still the `--unaudited` one, not
/// a store configuration error.
#[test]
fn binary_refuses_without_unaudited_before_building_the_store() {
    let output = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args([
            "--store", "s3", "sql", "explain", "--tenant", TENANT, GROUPED,
        ])
        .env_remove("RAVEL_S3_BUCKET")
        .output()
        .expect("run ravel-cli");
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(UNAUDITED_REQUIRED), "{stderr}");
}

/// The help text says no query-audit record is written.
#[test]
fn help_says_no_audit_record_is_written() {
    let output = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args(["sql", "explain", "--help"])
        .output()
        .expect("run ravel-cli");
    assert!(output.status.success());
    let help = String::from_utf8_lossy(&output.stdout);
    assert!(help.contains("No query-audit record is written"), "{help}");
    assert!(help.contains("--unaudited"), "{help}");
}
