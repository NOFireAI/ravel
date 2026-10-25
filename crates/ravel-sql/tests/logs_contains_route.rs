//! Issues #2865 and #3241: a content list that holds only `Contains` arms must
//! not change the route a logs scan takes.
//!
//! `col LIKE '%literal%'` on a declared Str column pushes a `Contains` content
//! arm. No index covers that arm, so it excludes no block in a plan pass; the
//! reader evaluates it per row of every block on every route. These tests pin
//! the route against the same statement with an arm-free filter
//! (`col LIKE '%literal%' ESCAPE '#'`, which the extractor declines), the
//! blocks and cells the scan reads on that route, a `has_word` arm beside the
//! `Contains` arm keeping today's rejection, and a late-materialized TopK
//! whose row refs resolve against the fast-path open's surviving rows.
//!
//! No `.cstat` object exists, so every statement scans.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array, TimestampNanosecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([37u8; 16]);

/// Objects in the fixture: twice [`PARTITIONS`], so the whole-segment fast
/// path's partition conjunct holds and only the predicate decides the route.
const OBJECTS: usize = 4;
const PARTITIONS: usize = 2;

/// Each block's `url` cells by row: one without the needle, one with it, one
/// absent, one with it. Every present value carries its `ts`, so no two cells
/// share a value.
const URLS: [Option<&str>; 4] = [Some("plain"), Some("a-needle"), None, Some("needle")];
const ROWS_PER_BLOCK: usize = URLS.len();
const BLOCKS_PER_OBJECT: usize = 4;
const ROWS_PER_OBJECT: usize = ROWS_PER_BLOCK * BLOCKS_PER_OBJECT;
const PRESENT_PER_BLOCK: usize = 3;
const MATCHES_PER_BLOCK: usize = 2;

/// `bytes` widens `SELECT *` to nine columns past `ts` and `url`, one more than
/// the late-materialization rule's default threshold of eight.
fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("url", DeclaredType::Str),
        DeclaredColumn::new("bytes", DeclaredType::I64),
    ]
}

fn record(ts: i64, url: Option<&str>) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let mut attrs = vec![("bytes".to_string(), AttrValue::I64(ts))];
    if let Some(u) = url {
        attrs.push(("url".to_string(), AttrValue::Str(format!("{u}-{ts}"))));
    }
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("row at {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

/// Write object `n`: [`ROWS_PER_BLOCK`] records per block, ts `n * 100 + 1..`.
async fn write_object(store: &dyn ObjectStoreBackend, n: usize) -> SegmentRef {
    let mut w = RlogWriter::new(
        RlogConfig {
            block_target_records: ROWS_PER_BLOCK,
            ..RlogConfig::default()
        },
        ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [5u8; 16],
            writer_epoch: 1,
            writer_seq: n as u64 + 1,
        },
    );
    let base = n as i64 * 100;
    for i in 0..ROWS_PER_OBJECT {
        w.push(record(base + i as i64 + 1, URLS[i % ROWS_PER_BLOCK]))
            .expect("push");
    }
    let bytes = w.finish().expect("finish");
    let key = format!("logs/contains-route-{n}.rlog");
    let content_hash = *blake3::hash(&bytes).as_bytes();
    let object_size = bytes.len() as u64;
    store
        .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    SegmentRef {
        data_object_key: key,
        object_size,
        min_event_ts_ns: base + 1,
        max_event_ts_ns: base + ROWS_PER_OBJECT as i64,
        ingest_hour_bucket: 0,
        sample_count: ROWS_PER_OBJECT as u64,
        series_count: 0,
        shard: 0,
        content_hash,
        writer_id: Uuid::from_u128(5),
        writer_epoch: 1,
        writer_seq: n as u64 + 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

async fn fixture() -> (Arc<MemoryStore>, Snapshot) {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for n in 0..OBJECTS {
        segments.push(write_object(store.as_ref(), n).await);
    }
    let snapshot = Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    (store, snapshot)
}

fn executor(store: &Arc<MemoryStore>) -> SqlExecutor {
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(store) as Arc<dyn ObjectStoreBackend>;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&backend)),
        LogSegmentFetcher::new(Arc::clone(&backend)),
        SpanSegmentFetcher::new(Arc::clone(&backend)),
        SqlConfig {
            engine: EngineConfig {
                fetch_concurrency: PARTITIONS,
                sql_partition_count: Some(PARTITIONS),
                ..EngineConfig::default()
            },
            ..SqlConfig::default()
        },
        1 << 30,
    )
}

fn find_scan(plan: &Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    fn walk(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
        if plan.name() == "LogsScanExec" {
            return Some(Arc::clone(plan));
        }
        plan.children().iter().find_map(|c| walk(c))
    }
    walk(plan).expect("a LogsScanExec leaf")
}

/// The scan's `name` metric summed over partitions; 0 when never created.
fn sum_metric(scan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    scan.metrics()
        .expect("metrics")
        .iter()
        .filter(|m| m.value().name() == name)
        .map(|m| m.value().as_usize())
        .sum()
}

/// The route the scan took, as its route counters publish it.
#[derive(Debug, PartialEq, Eq)]
struct Route {
    fast_path_whole_object_segments: usize,
    fast_path_ranged_segments: usize,
    rejected_block_predicate: usize,
    segments_unplanned: usize,
}

impl Route {
    fn fast_path_segments(&self) -> usize {
        self.fast_path_whole_object_segments + self.fast_path_ranged_segments
    }
}

struct Executed {
    route: Route,
    plan_init_ns: usize,
    blocks_total: usize,
    blocks_scanned: usize,
    contains_cells_tested: usize,
    rows_predicate_evaluated: usize,
    plan_text: String,
    batches: Vec<RecordBatch>,
}

async fn execute(store: &Arc<MemoryStore>, snapshot: Snapshot, sql: &str) -> Executed {
    let pinned = executor(store)
        .plan_pinned(TENANT, snapshot, sql, &QueryAccounting::new(), &declared())
        .await
        .expect("plan");
    let plan = pinned.create_physical_plan().await.expect("physical plan");
    let plan_text = displayable(plan.as_ref()).indent(true).to_string();
    let batches = collect(Arc::clone(&plan), Arc::new(TaskContext::default()))
        .await
        .expect("collect");
    let scan = find_scan(&plan);
    Executed {
        route: Route {
            fast_path_whole_object_segments: sum_metric(&scan, "fast_path_whole_object_segments"),
            fast_path_ranged_segments: sum_metric(&scan, "fast_path_ranged_segments"),
            rejected_block_predicate: sum_metric(&scan, "fast_path_rejected_block_predicate"),
            segments_unplanned: sum_metric(&scan, "segments_unplanned"),
        },
        plan_init_ns: sum_metric(&scan, "plan_init_elapsed"),
        blocks_total: sum_metric(&scan, "blocks_total"),
        blocks_scanned: sum_metric(&scan, "blocks_scanned"),
        contains_cells_tested: sum_metric(&scan, "contains_cells_tested"),
        rows_predicate_evaluated: sum_metric(&scan, "rows_predicate_evaluated"),
        plan_text,
        batches,
    }
}

fn count(batches: &[RecordBatch]) -> i64 {
    let batch = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .expect("one result row");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count(*) is Int64")
        .value(0)
}

fn ts(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let col = b
                .column_by_name("ts")
                .expect("ts column")
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ts is a nanosecond timestamp");
            (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

/// `COUNT(*) WHERE url LIKE '%needle%'` takes the whole-segment route the
/// arm-free `ESCAPE '#'` form takes, with no plan pass (zero plan-init time),
/// decodes the same 16 blocks, and evaluates the arm once per row: 64 rows
/// evaluated, 48 present cells tested (three per block), 32 rows kept (two
/// per block).
///
/// Against the parent commit, whose route check read every content arm as a
/// block predicate, the route assertion failed: zero fast-path segments and
/// two `fast_path_rejected_block_predicate` counts against four and zero.
#[tokio::test]
async fn contains_count_keeps_the_whole_segment_route() {
    let (store, snapshot) = fixture().await;

    let pushed = execute(
        &store,
        snapshot.clone(),
        "SELECT COUNT(*) FROM logs WHERE url LIKE '%needle%'",
    )
    .await;
    let plain = execute(
        &store,
        snapshot,
        "SELECT COUNT(*) FROM logs WHERE url LIKE '%needle%' ESCAPE '#'",
    )
    .await;

    assert_eq!(
        plain.route.fast_path_segments(),
        OBJECTS,
        "the arm-free statement is on the whole-segment route: {:?}",
        plain.route
    );
    assert_eq!(plain.route.rejected_block_predicate, 0);
    assert_eq!(
        pushed.route, plain.route,
        "the Contains arm alone does not change the route"
    );
    assert_eq!(plain.plan_init_ns, 0, "no plan pass on the arm-free route");
    assert_eq!(pushed.plan_init_ns, 0, "no plan pass with the Contains arm");

    let blocks = BLOCKS_PER_OBJECT * OBJECTS;
    let expected = (MATCHES_PER_BLOCK * blocks) as i64;
    assert_eq!(
        count(&pushed.batches),
        expected,
        "two needle rows per block"
    );
    assert_eq!(count(&plain.batches), expected);

    assert_eq!((plain.blocks_total, plain.blocks_scanned), (blocks, blocks));
    assert_eq!(
        (pushed.blocks_total, pushed.blocks_scanned),
        (blocks, blocks),
        "the arm drops no block, so the scan decodes what the arm-free form does"
    );

    assert_eq!(
        (
            pushed.contains_cells_tested,
            pushed.rows_predicate_evaluated
        ),
        (PRESENT_PER_BLOCK * blocks, ROWS_PER_OBJECT * OBJECTS),
        "every present cell tested once, every row evaluated"
    );
    assert_eq!(
        (plain.contains_cells_tested, plain.rows_predicate_evaluated),
        (0, 0),
        "the arm-free form pushes no arm"
    );
}

/// `url LIKE '%needle%' AND has_word(body, 'row')` carries a `HasWord` arm
/// beside the `Contains` arm, so it keeps the block-predicate rejection: the
/// route check clears only a content list of `Contains` arms alone.
///
/// Against a route check that clears the whole content list whatever its arms,
/// the rejection assertion failed: zero against two.
#[tokio::test]
async fn contains_beside_a_has_word_arm_keeps_the_rejection() {
    let (store, snapshot) = fixture().await;

    let both = execute(
        &store,
        snapshot,
        "SELECT COUNT(*) FROM logs WHERE url LIKE '%needle%' AND has_word(body, 'row')",
    )
    .await;

    assert_eq!(
        (
            both.route.rejected_block_predicate,
            both.route.fast_path_segments()
        ),
        (PARTITIONS, 0),
        "the has_word arm keeps the scan off the fast path: {:?}",
        both.route
    );
    // Every body holds the word `row`, so the count is the needle rows.
    assert_eq!(
        count(&both.batches),
        (MATCHES_PER_BLOCK * BLOCKS_PER_OBJECT * OBJECTS) as i64
    );
}

/// A late-materialized TopK over a `url LIKE '%needle%'` scan on the
/// whole-segment route returns the same rows in the same order as the
/// arm-free form, with no error. Phase 1's row refs index each block's
/// surviving rows under the arm (rows 1 and 3 of four), so phase 2 has to
/// re-apply the same arm over the same blocks to resolve them.
///
/// With phase 2's query rebuilt without the content arm, the ts assertion
/// read `[1, 2, 5, 6, 9]`: each surviving-row position resolved against the
/// block's unfiltered rows.
#[tokio::test]
async fn late_materialized_topk_over_a_contains_scan_on_the_fast_path() {
    let (store, snapshot) = fixture().await;

    let pushed = execute(
        &store,
        snapshot.clone(),
        "SELECT * FROM logs WHERE url LIKE '%needle%' ORDER BY ts LIMIT 5",
    )
    .await;
    let plain = execute(
        &store,
        snapshot,
        "SELECT * FROM logs WHERE url LIKE '%needle%' ESCAPE '#' ORDER BY ts LIMIT 5",
    )
    .await;

    assert!(
        pushed.plan_text.contains("LogsRowFetchExec"),
        "the TopK is late-materialized; plan was:\n{}",
        pushed.plan_text
    );
    assert_eq!(
        (
            pushed.route.fast_path_segments(),
            pushed.route.rejected_block_predicate
        ),
        (OBJECTS, 0),
        "phase 1 is on the whole-segment route: {:?}",
        pushed.route
    );
    // Object 0 holds ts 1..=16; its needle rows are the even ts.
    assert_eq!(ts(&pushed.batches), vec![2, 4, 6, 8, 10]);
    assert_eq!(ts(&pushed.batches), ts(&plain.batches));
}
