//! End-to-end coverage for `ravel-cli load --parquet` (ADR-0089, issue #275).
//!
//! These drive the loader's real command-dispatch entry point
//! (`ravel_cli::load::load`, what `main.rs` calls) in-process against a shared
//! `MemoryStore`, then query the loaded data back through the real `ravel-sql`
//! logs path. A subprocess against `--store memory` cannot be used for the
//! round-trip: each process gets its own empty in-memory store, so a second
//! process could never see the first's writes (the same reason
//! `tests/catalog.rs` drives the library entry points in-process). The library
//! entry point is the exact function the compiled binary dispatches to.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, Date32Array, Date64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::load::{self, LoadError, Mapping};
use ravel_ingest::Clock;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, Op, ScriptedFault, Sequence};
use ravel_object_store::memory::MemoryStore;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::{CommitToken, TenantId, TimeRange};

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// A fixed, plausible (post-2020) load-time clock. The RLOG flush buckets by
/// this reading, so the query window below reaches a known hour and
/// `Catalog::resolve` fans out only a couple of LISTs (a realistic wall-clock
/// value with a window from 0 would fan out to hundreds of thousands).
const CLOCK_NS: i64 = 1_700_000_000_000_000_000; // 2023-11-14T22:13:20Z

/// A clock pinned to `CLOCK_NS`; the shard actor's flush-open reading and the
/// router's generation lookups both use it. The default real-timer `sleep` is
/// fine (the flush-age tick never needs to advance in these tests).
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

fn i64_col(vals: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(vals))
}

fn str_col(vals: Vec<&str>) -> ArrayRef {
    Arc::new(StringArray::from(vals))
}

fn mapping(text: &str) -> Mapping {
    load::parse_mapping(text).expect("valid mapping")
}

/// Run a logs SQL query in-process through a real `SqlExecutor`, over a narrow
/// window around `CLOCK_NS`, with `min_tokens` for read-your-write. Returns the
/// matched row count.
async fn logs_query_rows(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    sql: &str,
    min_tokens: &[CommitToken],
) -> usize {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        LogSegmentFetcher::new(Arc::clone(store)),
        SpanSegmentFetcher::new(Arc::clone(store)),
        SqlConfig::default(),
        1 << 30,
    );
    let req = SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: CLOCK_NS - NS_PER_HOUR,
            end_ns: CLOCK_NS + NS_PER_HOUR,
        },
        min_tokens: min_tokens.to_vec(),
        now_ns: CLOCK_NS + 1_000,
        deadline: Duration::from_secs(30),
    };
    let outcome = executor
        .execute(TenantId::new(tenant).hash(), &req)
        .await
        .expect("logs query executes");
    outcome.output.num_rows()
}

/// Reachability: a Parquet fixture loaded through the compiled loader's
/// command-dispatch entry point is queryable through the real ravel-sql logs
/// path, and typed attributes survive through `attrs['k']`.
#[tokio::test]
async fn loaded_parquet_round_trips_through_the_logs_sql_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("logs.parquet");
    // Two records, same resource (service.name=api) so they share one stream,
    // distinct typed record attributes.
    let batch = RecordBatch::try_from_iter(vec![
        ("ts".to_string(), i64_col(vec![CLOCK_NS, CLOCK_NS])),
        ("body".to_string(), str_col(vec!["hello", "world"])),
        ("svc".to_string(), str_col(vec!["api", "api"])),
        ("status".to_string(), i64_col(vec![200, 500])),
    ])
    .expect("batch");
    write_parquet(&pq, &batch);

    let m = mapping(
        r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "http.status_code"
column = "status"
type = "i64"
"#,
    );

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load::load(
        Arc::clone(&store),
        &pq,
        "acme",
        &m,
        4,
        10_000,
        None,
        CLOCK_NS,
        Arc::new(FixedClock(CLOCK_NS)),
    )
    .await
    .expect("load succeeds");
    assert_eq!(report.rows_processed, 2);
    assert_eq!(
        report.objects_written(),
        1,
        "both records share one stream, so one shard flushed"
    );

    // Both records are visible.
    assert_eq!(
        logs_query_rows(&store, "acme", "SELECT ts, body FROM logs", &report.tokens).await,
        2
    );
    // The typed i64 record attribute survives through attrs['k'] (stringified
    // in the Map(Utf8,Utf8) attrs column): each value matches exactly its row.
    assert_eq!(
        logs_query_rows(
            &store,
            "acme",
            "SELECT ts FROM logs WHERE attrs['http.status_code'] = '200'",
            &report.tokens,
        )
        .await,
        1
    );
    assert_eq!(
        logs_query_rows(
            &store,
            "acme",
            "SELECT ts FROM logs WHERE attrs['http.status_code'] = '500'",
            &report.tokens,
        )
        .await,
        1
    );
    // The resource attribute is also queryable via attrs and matches both rows.
    assert_eq!(
        logs_query_rows(
            &store,
            "acme",
            "SELECT ts FROM logs WHERE attrs['service.name'] = 'api'",
            &report.tokens,
        )
        .await,
        2
    );
    // A non-matching predicate returns nothing (not an error).
    assert_eq!(
        logs_query_rows(
            &store,
            "acme",
            "SELECT ts FROM logs WHERE attrs['http.status_code'] = '999'",
            &report.tokens,
        )
        .await,
        0
    );
}

/// A `Date32` (days since the epoch) and a `Date64` (milliseconds since the
/// epoch) column load end to end as i64 attributes in their native unit, read
/// back exactly, without being rescaled to nanoseconds or routed through the ts
/// path (ADR-0100). Before Date32/Date64 were added to `read_i64`, the loader
/// rejected the batch with "expected an integer column, found Date32", so this
/// could not load at all.
#[tokio::test]
async fn date32_and_date64_columns_load_and_read_back_exactly() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("dates.parquet");

    // 19876 days since the epoch (2024-05-16); 1_700_000_000_000 ms since the
    // epoch. Stored verbatim as i64 attribute values, not rescaled.
    let d32: i32 = 19_876;
    let d64: i64 = 1_700_000_000_000;
    let batch = RecordBatch::try_from_iter(vec![
        ("ts".to_string(), i64_col(vec![CLOCK_NS])),
        ("svc".to_string(), str_col(vec!["api"])),
        (
            "event_date".to_string(),
            Arc::new(Date32Array::from(vec![d32])) as ArrayRef,
        ),
        (
            "event_ms".to_string(),
            Arc::new(Date64Array::from(vec![d64])) as ArrayRef,
        ),
    ])
    .expect("batch");
    write_parquet(&pq, &batch);

    let m = mapping(
        r#"
ts_column = "ts"
ts_unit = "nanos"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "event_date"
column = "event_date"
type = "i64"

[[attribute]]
key = "event_ms"
column = "event_ms"
type = "i64"
"#,
    );

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load::load(
        Arc::clone(&store),
        &pq,
        "acme",
        &m,
        4,
        10_000,
        None,
        CLOCK_NS,
        Arc::new(FixedClock(CLOCK_NS)),
    )
    .await
    .expect("a date column loads as an i64 attribute");
    assert_eq!(report.rows_processed, 1);

    // The Date32 value reads back as its exact day count, the Date64 as its
    // exact millisecond count (stringified in the attrs map).
    assert_eq!(
        logs_query_rows(
            &store,
            "acme",
            &format!("SELECT ts FROM logs WHERE attrs['event_date'] = '{d32}'"),
            &report.tokens,
        )
        .await,
        1,
        "the Date32 value is stored as its native day count ({d32}), not rescaled"
    );
    assert_eq!(
        logs_query_rows(
            &store,
            "acme",
            &format!("SELECT ts FROM logs WHERE attrs['event_ms'] = '{d64}'"),
            &report.tokens,
        )
        .await,
        1,
        "the Date64 value is stored as its native millisecond count ({d64}), not rescaled"
    );
    // Not rescaled to nanoseconds: the ns-scaled value must NOT match.
    assert_eq!(
        logs_query_rows(
            &store,
            "acme",
            &format!(
                "SELECT ts FROM logs WHERE attrs['event_ms'] = '{}'",
                d64 * 1_000_000
            ),
            &report.tokens,
        )
        .await,
        0,
        "the Date64 value is not multiplied to nanoseconds"
    );
}

/// A PUT failure partway through a multi-batch load exits non-zero and reports
/// the commit tokens already durable before the failure. This fails against a
/// naive loader that swallows the error or exits 0.
#[tokio::test]
async fn put_failure_midway_reports_already_durable_tokens() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("logs.parquet");
    // Three rows, each a distinct stream (distinct service.name) so each lands
    // in its own single-record batch flush under batch_rows = 1.
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts".to_string(),
            i64_col(vec![CLOCK_NS, CLOCK_NS, CLOCK_NS]),
        ),
        ("svc".to_string(), str_col(vec!["a", "b", "c"])),
    ])
    .expect("batch");
    write_parquet(&pq, &batch);

    let m = mapping(
        r#"
ts_column = "ts"
ts_unit = "nanos"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"
"#,
    );

    // Fail the SECOND log data-object PUT and every retry of it, so the first
    // batch is durable and the second batch's flush is abandoned. The key
    // filter `/l/l0/` matches only log data objects (not the provisioning
    // record or commit records), so the first data PUT passes through.
    let fault = ScriptedFault::Transient("injected PUT failure".into());
    let mut seq = Sequence::new(Op::Put)
        .with_key_contains("/l/l0/")
        .then_passthrough();
    for _ in 0..8 {
        seq = seq.then_fault(fault.clone());
    }
    let plan = FaultPlan::empty().with_sequence(seq);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(ravel_object_store::fault::FaultStore::new(
        MemoryStore::new(),
        plan,
    ));

    let err = load::load(
        Arc::clone(&store),
        &pq,
        "acme",
        &m,
        4,
        1, // one row per Strict flush
        None,
        CLOCK_NS,
        Arc::new(FixedClock(CLOCK_NS)),
    )
    .await
    .expect_err("a PUT failure must surface as an error, not exit 0");

    assert!(
        matches!(err, LoadError::Flush { .. }),
        "expected a flush failure, got: {err}"
    );
    assert_eq!(
        err.durable_tokens().len(),
        1,
        "exactly the first batch was durable before the failure: {err}"
    );
}

/// The deliberate ADR-0089 relaxation, end to end: a 2013-era event is
/// accepted (not rejected as `TooOld`) and its RLOG object buckets by *load*
/// time, not the event time. The commit token carries the pinned ingest-hour
/// bucket, so this asserts the bucketing behavior the discoverability argument
/// depends on.
#[tokio::test]
async fn past_dated_event_is_accepted_and_buckets_by_load_time() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("old.parquet");
    let ts_2013 = 1_356_998_400_000_000_000; // 2013-01-01
    let batch = RecordBatch::try_from_iter(vec![
        ("ts".to_string(), i64_col(vec![ts_2013])),
        ("svc".to_string(), str_col(vec!["api"])),
    ])
    .expect("batch");
    write_parquet(&pq, &batch);

    let m = mapping(
        r#"
ts_column = "ts"
ts_unit = "nanos"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"
"#,
    );

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load::load(
        Arc::clone(&store),
        &pq,
        "acme",
        &m,
        4,
        10_000,
        None,
        CLOCK_NS,
        Arc::new(FixedClock(CLOCK_NS)),
    )
    .await
    .expect("a decade-old event is admitted");
    assert_eq!(report.rows_processed, 1);
    assert_eq!(report.tokens.len(), 1);

    let load_hour = u32::try_from(CLOCK_NS.div_euclid(NS_PER_HOUR)).expect("hour fits u32");
    assert_eq!(
        report.tokens[0].ingest_hour_bucket, load_hour,
        "the object must bucket by load time, not by the 2013 event time"
    );
}

/// Attribute-count overflow at the RLOG object's dynamic-column budget: a load
/// whose mapping has more distinct attribute columns than the 1000-per-object
/// budget, but each row within the loader's per-record cap, succeeds. The
/// overflow columns fold into the writer's `attrs_raw` overflow column rather
/// than being rejected. This requires no loader code (the writer's existing
/// behavior is reached), only that the loader does not itself reject.
#[tokio::test]
async fn attribute_columns_over_the_object_budget_fold_into_overflow_not_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("wide.parquet");

    // 1001 distinct attribute columns: within the loader per-record cap (1024),
    // over the RLOG object's 1000-distinct-column budget.
    let n_attrs = 1001usize;
    let mut cols: Vec<(String, ArrayRef)> = vec![
        ("ts".to_string(), i64_col(vec![CLOCK_NS])),
        ("svc".to_string(), str_col(vec!["api"])),
    ];
    let mut attr_toml = String::new();
    for i in 0..n_attrs {
        let name = format!("a{i}");
        cols.push((name.clone(), i64_col(vec![i as i64])));
        attr_toml.push_str(&format!(
            "\n[[attribute]]\nkey = \"{name}\"\ncolumn = \"{name}\"\ntype = \"i64\"\n"
        ));
    }
    let batch = RecordBatch::try_from_iter(cols).expect("wide batch");
    write_parquet(&pq, &batch);

    let m = mapping(&format!(
        "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
         [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n{attr_toml}"
    ));

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load::load(
        Arc::clone(&store),
        &pq,
        "acme",
        &m,
        4,
        10_000,
        None,
        CLOCK_NS,
        Arc::new(FixedClock(CLOCK_NS)),
    )
    .await
    .expect("over-budget attribute columns fold into attrs_raw, they are not rejected");
    assert_eq!(report.rows_processed, 1);
    assert_eq!(report.tokens.len(), 1);

    // And the record is still queryable, confirming the object was written.
    assert_eq!(
        logs_query_rows(&store, "acme", "SELECT ts FROM logs", &report.tokens).await,
        1
    );
}
