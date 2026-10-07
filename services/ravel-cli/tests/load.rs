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
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, Date32Array, Date64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::load::{self, LoadError, Mapping};
use ravel_ingest::{Clock, IngestByteBudget};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, ScriptedFault, Sequence};
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
        row_window: false,
        max_rows: None,
        budgets: None,
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
        1,
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
        1,
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
        1,
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
        1,
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
        1,
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

const BUDGET_ROWS: usize = 8_000;

const BUDGET_MAPPING: &str = r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "user"
column = "user"
type = "str"
"#;

/// `BUDGET_ROWS` rows whose strings all have the same width, so every batch
/// of the same row count measures the same heap bytes and the first batch is
/// also the largest.
fn write_budget_fixture(path: &Path) {
    let ts: Vec<i64> = (0..BUDGET_ROWS as i64).map(|i| CLOCK_NS + i).collect();
    let body: Vec<String> = (0..BUDGET_ROWS)
        .map(|i| format!("request served id={i:06}"))
        .collect();
    let svc: Vec<String> = (0..BUDGET_ROWS)
        .map(|i| format!("svc-{:02}", i % 8))
        .collect();
    let user: Vec<String> = (0..BUDGET_ROWS)
        .map(|i| format!("user-{:03}", i % 500))
        .collect();
    let batch = RecordBatch::try_from_iter(vec![
        ("ts", i64_col(ts)),
        ("body", Arc::new(StringArray::from(body)) as ArrayRef),
        ("svc", Arc::new(StringArray::from(svc)) as ArrayRef),
        ("user", Arc::new(StringArray::from(user)) as ArrayRef),
    ])
    .expect("batch");
    write_parquet(path, &batch);
}

#[allow(clippy::too_many_arguments)]
async fn load_budgeted(
    store: &Arc<dyn ObjectStoreBackend>,
    pq: &Path,
    shards: u32,
    batch_rows: usize,
    target_bytes: usize,
    max_flush_delay: Option<Duration>,
    memory: load::LoadMemoryOptions,
) -> Result<load::LoadReport, LoadError> {
    load::load_with_memory(
        Arc::clone(store),
        pq,
        "acme",
        &mapping(BUDGET_MAPPING),
        shards,
        batch_rows,
        None,
        load::DEFAULT_PIPELINE_DEPTH,
        target_bytes,
        max_flush_delay,
        memory,
        CLOCK_NS,
        Arc::new(FixedClock(CLOCK_NS)),
    )
    .await
}

/// `--load-memory-bytes bytes`.
fn flag(bytes: u64) -> load::LoadMemoryOptions {
    load::LoadMemoryOptions::new(load::LoadMemoryRequest::Flag(bytes))
}

/// [`flag`], with the load's budget handed to the returned slot as soon as
/// it exists.
fn flag_watched(
    bytes: u64,
) -> (
    load::LoadMemoryOptions,
    Arc<Mutex<Option<Arc<IngestByteBudget>>>>,
) {
    let slot: Arc<Mutex<Option<Arc<IngestByteBudget>>>> = Arc::default();
    let mut options = flag(bytes);
    let hook_slot = Arc::clone(&slot);
    options.on_budget = Some(Arc::new(move |budget| {
        *hook_slot.lock().expect("budget slot") = Some(Arc::clone(budget));
    }));
    (options, slot)
}

/// Every RLOG data object the store holds, as one hash per object in sorted
/// order. Each shard actor stamps a random writer id into its objects'
/// footers (and so into the footer checksum), and writes landing in
/// different orders get different writer sequence numbers, so the hash
/// covers every section's stored bytes plus the footer's content fields:
/// everything the object holds except who wrote it and in which order.
async fn rlog_object_hashes(store: &Arc<dyn ObjectStoreBackend>) -> Vec<[u8; 32]> {
    let mut hashes = Vec::new();
    for meta in ravel_object_store::list_all(store.as_ref(), "")
        .await
        .expect("list")
    {
        let data = store
            .get(&meta.key, ravel_object_store::GetRange::Full)
            .await
            .expect("get")
            .data;
        let Ok(footer) = ravel_logseg::footer::open(&data) else {
            continue;
        };
        let mut hasher = blake3::Hasher::new();
        for field in [
            u64::from(footer.shard),
            footer.min_ts_ns as u64,
            footer.max_ts_ns as u64,
            footer.min_observed_ts_ns as u64,
            footer.max_observed_ts_ns as u64,
            footer.record_count,
            footer.block_count,
            footer.stream_count,
            footer.sections.len() as u64,
        ] {
            hasher.update(&field.to_le_bytes());
        }
        for section in &footer.sections {
            hasher.update(&section.kind.to_le_bytes());
            hasher.update(&[section.comp]);
            hasher.update(&section.uncomp_len.to_le_bytes());
            let start = section.offset as usize;
            hasher.update(&data[start..start + section.len as usize]);
        }
        hashes.push(*hasher.finalize().as_bytes());
    }
    hashes.sort_unstable();
    hashes
}

/// `--load-memory-bytes` (issue #2626): with the first batch's first data PUT
/// held, a budget of one and a half batches cannot admit the second batch
/// beside the first, so the decoder waits on the budget; the load then
/// finishes with every object byte-identical to an unbounded load of the same
/// file, which never waited. A budget below one batch is refused at start,
/// before anything is written.
#[tokio::test]
async fn load_memory_budget_bounds_the_load_and_keeps_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("budget.parquet");
    write_budget_fixture(&pq);
    const BATCH_ROWS: usize = 500;

    let unbounded_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let unbounded = load_budgeted(
        &unbounded_store,
        &pq,
        2,
        BATCH_ROWS,
        load::DEFAULT_TARGET_BYTES,
        None,
        flag(u64::MAX),
    )
    .await
    .expect("unbounded load succeeds");
    let batch = unbounded.load_memory_max_batch_bytes;
    assert!(batch > 0, "every columnar batch is charged its heap bytes");
    assert_eq!(unbounded.rows_processed, BUDGET_ROWS as u64);
    assert_eq!(unbounded.load_memory_waits, 0, "nothing waits on u64::MAX");

    // The first batch's charge is refunded only once both of its shard
    // flushes finish, so while one of them is held the budget holds one
    // batch, and the second batch's estimate (the first's bytes per row,
    // every batch here is the same width) does not fit beside it.
    let budget = batch + batch / 2;
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let bounded_store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));
    let (options, watched) = flag_watched(budget);
    let load = tokio::spawn({
        let store = Arc::clone(&bounded_store);
        let pq = pq.clone();
        async move {
            load_budgeted(
                &store,
                &pq,
                2,
                BATCH_ROWS,
                load::DEFAULT_TARGET_BYTES,
                None,
                options,
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(60), gate.wait_until_held(1))
        .await
        .expect("the first data PUT is held");
    let watched = watched
        .lock()
        .expect("budget slot")
        .clone()
        .expect("the loader handed its budget to the hook");
    // Bounded so a decoder that never waits fails the assertion below
    // instead of hanging the test.
    let saw_waiter = tokio::time::timeout(Duration::from_secs(10), async {
        while watched.waiting() == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok();
    for id in gate.held() {
        gate.release(id);
    }
    let bounded = load
        .await
        .expect("load task")
        .expect("a load under a one-and-a-half-batch budget succeeds");
    assert!(
        saw_waiter && bounded.load_memory_waits > 0,
        "the budget never bound: waiter seen {saw_waiter}, decoder waits {}",
        bounded.load_memory_waits
    );
    assert_eq!(bounded.rows_processed, BUDGET_ROWS as u64);
    assert_eq!(bounded.load_memory.budget_bytes, budget);
    assert_eq!(bounded.load_memory.source, load::LoadMemorySource::Flag);
    assert_eq!(bounded.load_memory_max_batch_bytes, batch);
    assert!(
        bounded.load_memory_peak_bytes <= budget,
        "peak {} exceeds the budget {budget}",
        bounded.load_memory_peak_bytes
    );
    assert!(
        bounded.load_memory_peak_bytes >= batch,
        "the peak counts at least one whole batch: {} < {batch}",
        bounded.load_memory_peak_bytes
    );
    let objects = rlog_object_hashes(&bounded_store).await;
    assert_eq!(objects.len(), bounded.objects_written());
    assert_eq!(
        objects,
        rlog_object_hashes(&unbounded_store).await,
        "the budget changes when batches are built, never what is written"
    );

    let refused_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let err = load_budgeted(
        &refused_store,
        &pq,
        2,
        BATCH_ROWS,
        load::DEFAULT_TARGET_BYTES,
        None,
        flag(batch - 1),
    )
    .await
    .expect_err("a budget below one batch is refused");
    let message = err.to_string();
    assert!(matches!(err, LoadError::Setup(_)), "got {err:?}");
    for needle in [
        format!("{} bytes", batch - 1),
        format!("measured {batch} bytes"),
        format!("{BATCH_ROWS} rows"),
        "floor".to_string(),
        "Lower --batch-rows or raise --load-memory-bytes".to_string(),
    ] {
        assert!(
            message.contains(&needle),
            "{needle:?} missing from {message}"
        );
    }
    assert!(
        rlog_object_hashes(&refused_store).await.is_empty(),
        "the refusal comes before any object is written"
    );
}

/// Runs the built `ravel-cli load` against its own `--store memory` on the
/// budget fixture with `--load-memory-bytes`.
fn cli_load_budgeted(dir: &Path, load_memory_bytes: u64) -> std::process::Output {
    let pq = dir.join("budget.parquet");
    let mapping = dir.join("budget.mapping.toml");
    if !pq.exists() {
        write_budget_fixture(&pq);
        std::fs::write(&mapping, BUDGET_MAPPING).expect("write mapping");
    }
    cli_load_with_budget(&pq, &mapping, 2, load_memory_bytes)
}

/// `ravel-cli load` of `pq` against its own `--store memory`, 500-row
/// batches, `--load-memory-bytes load_memory_bytes`.
fn cli_load_with_budget(
    pq: &Path,
    mapping: &Path,
    shards: u32,
    load_memory_bytes: u64,
) -> std::process::Output {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_ravel-cli"));
    for (key, _) in std::env::vars() {
        if key.starts_with("RAVEL_") {
            cmd.env_remove(key);
        }
    }
    cmd.args([
        "--store",
        "memory",
        "--tenant-hash-unkeyed",
        "load",
        "--parquet",
    ])
    .arg(pq)
    .args(["--tenant", "acme", "--mapping"])
    .arg(mapping)
    .arg("--shards")
    .arg(shards.to_string())
    .args(["--batch-rows", "500", "--load-memory-bytes"])
    .arg(load_memory_bytes.to_string())
    .output()
    .expect("ravel-cli runs")
}

/// The `budget`, `peak` and `largest batch` figures of the summary's
/// `load memory` line, which must appear exactly once.
fn summary_load_memory(stdout: &str) -> (u64, u64, u64) {
    let lines: Vec<&str> = stdout
        .lines()
        .filter(|l| l.trim_start().starts_with("load memory"))
        .collect();
    assert_eq!(lines.len(), 1, "one load memory line in:\n{stdout}");
    let words: Vec<&str> = lines[0].split_whitespace().collect();
    let after = |label: &str| -> u64 {
        let at = words
            .iter()
            .position(|w| *w == label)
            .unwrap_or_else(|| panic!("{label:?} missing from {}", lines[0]));
        words[at + 1].parse().expect("a byte count")
    };
    let largest = words
        .windows(3)
        .find(|w| w[0] == "largest" && w[1] == "batch")
        .map(|w| w[2].parse().expect("a byte count"))
        .unwrap_or_else(|| panic!("largest batch missing from {}", lines[0]));
    (after("budget"), after("peak"), largest)
}

/// The binary takes `--load-memory-bytes`, reports the budget, its source,
/// the peak charge and the largest batch on the summary line, keeps the peak
/// within the flag, and exits non-zero with the refusal when one batch does
/// not fit.
#[test]
fn the_binary_reports_the_peak_charge_and_refuses_a_budget_below_one_batch() {
    let dir = tempfile::tempdir().expect("tempdir");

    let unbounded = cli_load_budgeted(dir.path(), u64::MAX);
    let stdout = String::from_utf8_lossy(&unbounded.stdout);
    assert!(
        unbounded.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&unbounded.stderr)
    );
    let (_, _, batch) = summary_load_memory(&stdout);
    assert!(batch > 0, "the largest batch is reported: {stdout}");

    let budget = 2 * batch + batch / 2;
    let bounded = cli_load_budgeted(dir.path(), budget);
    let stdout = String::from_utf8_lossy(&bounded.stdout);
    assert!(
        bounded.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&bounded.stderr)
    );
    assert!(
        stdout.contains(&format!("rows_written     : {BUDGET_ROWS}\n")),
        "{stdout}"
    );
    assert!(stdout.contains("(--load-memory-bytes)"), "{stdout}");
    let (reported_budget, peak, largest) = summary_load_memory(&stdout);
    assert_eq!(reported_budget, budget);
    assert_eq!(largest, batch);
    assert!(
        (batch..=budget).contains(&peak),
        "peak {peak} outside [{batch}, {budget}]"
    );

    let refused = cli_load_budgeted(dir.path(), batch - 1);
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        !refused.status.success(),
        "a budget below one batch exits non-zero"
    );
    assert!(
        stderr.contains(&format!(
            "one batch of 500 rows measured {batch} bytes, more than the loader's memory \
             budget of {} bytes",
            batch - 1
        )) && stderr.contains("Lower --batch-rows or raise --load-memory-bytes"),
        "{stderr}"
    );
    assert!(
        !String::from_utf8_lossy(&refused.stdout).contains("bulk load complete"),
        "a refused load prints no summary"
    );
}

/// Rows in [`write_widening_fixture`] before its last, wide batch.
const NARROW_ROWS: usize = 1_000;

/// [`NARROW_ROWS`] narrow rows, then one 500-row batch whose bodies are
/// 4,000 bytes each, so at `--batch-rows 500` the last batch measures many
/// times the first.
fn write_widening_fixture(path: &Path) {
    const ROWS: usize = NARROW_ROWS + 500;
    let ts: Vec<i64> = (0..ROWS as i64).map(|i| CLOCK_NS + i).collect();
    let body: Vec<String> = (0..ROWS)
        .map(|i| {
            if i < NARROW_ROWS {
                format!("narrow id={i:06}")
            } else {
                format!("{i:06}{}", "x".repeat(4_000))
            }
        })
        .collect();
    let svc: Vec<String> = (0..ROWS).map(|i| format!("svc-{:02}", i % 8)).collect();
    let user: Vec<String> = (0..ROWS).map(|i| format!("user-{:03}", i % 500)).collect();
    let batch = RecordBatch::try_from_iter(vec![
        ("ts", i64_col(ts)),
        ("body", Arc::new(StringArray::from(body)) as ArrayRef),
        ("svc", Arc::new(StringArray::from(svc)) as ArrayRef),
        ("user", Arc::new(StringArray::from(user)) as ArrayRef),
    ])
    .expect("batch");
    write_parquet(path, &batch);
}

/// A batch after the first that does not fit an explicit budget: the loader
/// lets the writes already in flight resolve, then fails with the refusal,
/// the commit tokens those earlier batches made durable, and the resume
/// figures that say how far they got.
#[test]
fn the_binary_refuses_a_later_batch_wider_than_the_budget_as_a_partial_load() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("widening.parquet");
    let mapping = dir.path().join("widening.mapping.toml");
    write_widening_fixture(&pq);
    std::fs::write(&mapping, BUDGET_MAPPING).expect("write mapping");

    let unbounded = cli_load_with_budget(&pq, &mapping, 1, u64::MAX);
    let stdout = String::from_utf8_lossy(&unbounded.stdout);
    assert!(
        unbounded.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&unbounded.stderr)
    );
    let (_, _, wide) = summary_load_memory(&stdout);

    let refused = cli_load_with_budget(&pq, &mapping, 1, wide - 1);
    let stdout = String::from_utf8_lossy(&refused.stdout);
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(!refused.status.success(), "{stdout}\n{stderr}");
    assert!(
        !stdout.contains("bulk load complete"),
        "a refused load prints no summary: {stdout}"
    );
    assert!(
        stderr.contains(&format!(
            "one batch of 500 rows measured {wide} bytes, more than the loader's memory budget \
             of {} bytes (--load-memory-bytes;",
            wide - 1
        )) && stderr.contains("Lower --batch-rows or raise --load-memory-bytes"),
        "{stderr}"
    );
    // One shard, so each of the two narrow batches made exactly one token,
    // and both are listed: their writes resolved before the refusal.
    let narrow_batches = NARROW_ROWS / 500;
    assert!(
        stdout.contains(&format!(
            "{narrow_batches} commit token(s)/segment(s) were durable before the failure (a \
             partial load, not a rollback; --skip-rows can resume it"
        )),
        "{stdout}"
    );
    let tokens = stdout
        .lines()
        .skip_while(|l| !l.contains("were durable before the failure"))
        .skip(1)
        .take_while(|l| l.starts_with("  "))
        .count();
    assert_eq!(tokens, narrow_batches, "{stdout}");
    assert!(
        stderr.contains("resume figures for this failed load:")
            && stderr.contains(&format!("rows_written     : {NARROW_ROWS}"))
            && stderr.contains(&format!("next --skip-rows : {NARROW_ROWS}")),
        "{stderr}"
    );
}

/// Small batches with a target far above what the budget can hold: buffers
/// merge several batches, and when their charges fill the budget the loader
/// flushes them early instead of waiting on a target no buffer can reach or
/// an age trigger an hour away. The timeout turns a deadlock into a failure.
#[tokio::test]
async fn small_batches_with_a_large_target_merge_without_deadlock() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("budget.parquet");
    write_budget_fixture(&pq);
    const BATCH_ROWS: usize = 250;

    let probe_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let batch = load_budgeted(
        &probe_store,
        &pq,
        1,
        BATCH_ROWS,
        load::DEFAULT_TARGET_BYTES,
        None,
        flag(u64::MAX),
    )
    .await
    .expect("probe load")
    .load_memory_max_batch_bytes;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let budget = 2 * batch + batch / 2;
    let mut options = flag(budget);
    options.stall_flush_period = Duration::from_millis(20);
    let report = tokio::time::timeout(
        Duration::from_secs(120),
        load_budgeted(
            &store,
            &pq,
            1,
            BATCH_ROWS,
            1 << 40,
            Some(Duration::from_secs(3600)),
            options,
        ),
    )
    .await
    .expect("the load finishes: a full budget flushes the buffers holding it")
    .expect("load succeeds");

    let batches = BUDGET_ROWS / BATCH_ROWS;
    assert_eq!(report.rows_processed, BUDGET_ROWS as u64);
    assert_eq!(
        report.tokens.len(),
        batches,
        "one write per batch on one shard"
    );
    assert!(report.load_memory_peak_bytes <= budget);
    assert!(report.load_memory_waits > 0, "the budget bound");
    let objects = report.objects_written();
    assert_eq!(objects, rlog_object_hashes(&store).await.len());
    // The budget holds at most `budget / batch` = 2 whole batches, and a
    // batch's charge lasts until the flush carrying it finishes, so no object
    // holds more than 2: `batches / 2` objects is the floor, reached when
    // every stall flush finds both batches buffered. The ceiling of three
    // quarters leaves room for a stall flush that catches the second batch
    // still on its way to the buffer (a 1-batch object) on up to half of the
    // objects, and still fails a load that merges nothing.
    let per_object = (budget / batch) as usize;
    assert_eq!(per_object, 2);
    assert!(
        (batches.div_ceil(per_object)..=batches * 3 / 4).contains(&objects),
        "{objects} objects for {batches} batches, outside [{}, {}]",
        batches.div_ceil(per_object),
        batches * 3 / 4
    );
}

/// Decision A of the issue #2626 fix round: a derived budget below one batch
/// (here a host whose memory does not even cover the floor, so the derived
/// budget is 0) runs the load one batch at a time and says so, naming the
/// host memory, the floor and the effective one-batch budget. The same batch
/// under an explicit `--load-memory-bytes` below it is refused.
#[tokio::test]
async fn a_derived_budget_below_one_batch_runs_one_batch_at_a_time() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("budget.parquet");
    write_budget_fixture(&pq);
    const BATCH_ROWS: usize = 500;

    let probe_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let batch = load_budgeted(
        &probe_store,
        &pq,
        2,
        BATCH_ROWS,
        load::DEFAULT_TARGET_BYTES,
        None,
        flag(u64::MAX),
    )
    .await
    .expect("probe load")
    .load_memory_max_batch_bytes;

    const HOST: u64 = 1024;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load_budgeted(
        &store,
        &pq,
        2,
        BATCH_ROWS,
        load::DEFAULT_TARGET_BYTES,
        None,
        load::LoadMemoryOptions::new(load::LoadMemoryRequest::Derived {
            host_total_bytes: Some(HOST),
        }),
    )
    .await
    .expect("a derived budget below one batch still loads");
    assert_eq!(report.rows_processed, BUDGET_ROWS as u64);
    let memory = report.load_memory;
    assert_eq!(memory.budget_bytes, 0);
    assert_eq!(
        memory.source,
        load::LoadMemorySource::Host { total_bytes: HOST }
    );
    // One read cursor (the fixture is one row group) and 2 shards times the
    // default 4 in-flight flushes: the floor is sized with the resolved
    // cursor count, not `--shards`.
    assert_eq!(
        memory.floor_bytes,
        load::LoadMemory::floor_bytes(1, 2 * u64::from(load::DEFAULT_MAX_INFLIGHT_FLUSHES))
    );
    // The peak is the larger of a batch's pre-build estimate (the previous
    // batch's bytes per row) and its measured size, never two batches.
    assert!(
        (batch..2 * batch).contains(&report.load_memory_peak_bytes),
        "one batch at a time: peak {} outside [{batch}, {})",
        report.load_memory_peak_bytes,
        2 * batch
    );
    let warning = report
        .load_memory_warning
        .as_deref()
        .expect("the one-batch fallback is reported");
    for needle in [
        format!("host memory {HOST} bytes"),
        format!("floor {} bytes", memory.floor_bytes),
        "derived memory budget of 0 bytes".to_string(),
        format!("one batch at a time, an effective budget of one batch ({batch} bytes"),
    ] {
        assert!(warning.contains(&needle), "{needle:?} missing: {warning}");
    }
    assert_eq!(
        rlog_object_hashes(&store).await,
        rlog_object_hashes(&probe_store).await
    );

    let refused_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let refused = load_budgeted(
        &refused_store,
        &pq,
        2,
        BATCH_ROWS,
        load::DEFAULT_TARGET_BYTES,
        None,
        flag(batch - 1),
    )
    .await
    .expect_err("an explicit budget below one batch is refused");
    assert!(matches!(refused, LoadError::Setup(_)), "got {refused:?}");
}

/// Decision B of the issue #2626 fix round: the shard buffers count a charged
/// write at the same estimate as an uncharged one, so with a budget far above
/// the load and a `--target-bytes` that merges several batches per object, the
/// budgeted load writes byte-identical objects to a load with the budget
/// machinery bypassed, and its shard buffers count the same buffered bytes.
#[tokio::test]
async fn a_budget_that_never_binds_lays_out_the_unbudgeted_objects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("budget.parquet");
    write_budget_fixture(&pq);
    const BATCH_ROWS: usize = 250;
    let batches = (BUDGET_ROWS / BATCH_ROWS) as u64;

    // The heap figure from a charged load, the estimate from an uncharged one.
    let mut probes = Vec::new();
    for memory in [
        flag(u64::MAX),
        load::LoadMemoryOptions::new(load::LoadMemoryRequest::Unbudgeted),
    ] {
        let probe_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let probe = load_budgeted(
            &probe_store,
            &pq,
            1,
            BATCH_ROWS,
            load::DEFAULT_TARGET_BYTES,
            None,
            memory,
        )
        .await
        .expect("probe load");
        probes.push(probe);
    }
    let heap = probes[0].load_memory_max_batch_bytes;
    let estimate = probes[1].metrics.buffered_bytes_total / batches;
    // The buffered-bytes counter is fed the same per-batch figure as the
    // memory backstop and the queued-flush cap, so it can only match across
    // the two loads below if both count a batch at its estimate.
    assert!(
        estimate.abs_diff(heap) > heap / 10,
        "estimate {estimate} too close to heap {heap} to tell them apart"
    );
    // Several batches per object, and fewer than `--pipeline-depth`'s 4
    // unacknowledged writes' worth, so every object closes on the size
    // trigger rather than the age trigger.
    let target = 2 * estimate + estimate / 2;

    let mut objects = Vec::new();
    let mut buffered = Vec::new();
    for memory in [
        load::LoadMemoryOptions::new(load::LoadMemoryRequest::Unbudgeted),
        flag(u64::MAX),
    ] {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        // A layout that merges more batches than `--pipeline-depth` admits
        // waits on the hour-long age trigger; the timeout fails it instead.
        let report = tokio::time::timeout(
            Duration::from_secs(120),
            load_budgeted(
                &store,
                &pq,
                1,
                BATCH_ROWS,
                target as usize,
                Some(Duration::from_secs(3600)),
                memory,
            ),
        )
        .await
        .expect("each object closes on the size trigger")
        .expect("load succeeds");
        assert_eq!(report.rows_processed, BUDGET_ROWS as u64);
        assert_eq!(report.load_memory_waits, 0);
        objects.push(rlog_object_hashes(&store).await);
        buffered.push(report.metrics.buffered_bytes_total);
    }
    let merged = objects[0].len() as u64;
    assert!(
        (2..batches / 2).contains(&merged),
        "{merged} objects for {batches} batches: objects should merge several batches"
    );
    assert_eq!(
        objects[0], objects[1],
        "the budget changed the stored bytes"
    );
    assert_eq!(
        buffered[0], buffered[1],
        "the shard buffers counted a charged batch at a different size"
    );
}
