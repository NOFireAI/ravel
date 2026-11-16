//! Issue #3240: a prune list that holds only `NumNotEq` arms must not change
//! the route a logs scan takes.
//!
//! `col <> lit` on a declared I64 column pushes a prune-only `NumNotEq` arm.
//! The whole-segment fast path declined every prune arm, so the arm moved a
//! bare `COUNT(*) WHERE col <> 0` off the plan-free whole-object route onto the
//! planned one, which cost more wall time than the decode it saved (#3231).
//! These tests pin the route against the same statement with an arm-free
//! filter (`abs(col) <> 0`), the blocks the scan decodes on that route, and a
//! `NumRange` arm beside the `NumNotEq` arm keeping today's route.
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

const TENANT: TenantHash = TenantHash([31u8; 16]);

/// Objects in the fixture: twice [`PARTITIONS`], so the whole-segment fast
/// path's partition conjunct holds and only the predicate decides the route.
const OBJECTS: usize = 4;
const PARTITIONS: usize = 2;

/// Each object's per-block `status`, one record per block. Four blocks per
/// object hold only `0`, the block with no `status` has no stat and is kept.
const STATUSES: [Option<i64>; 8] = [
    Some(0),
    Some(0),
    Some(200),
    Some(0),
    Some(404),
    None,
    Some(0),
    Some(500),
];
const BLOCKS_PER_OBJECT: usize = STATUSES.len();
const ALL_ZERO_BLOCKS_PER_OBJECT: usize = 4;
const NONZERO_PER_OBJECT: i64 = 3;

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("status", DeclaredType::I64),
        DeclaredColumn::new("bytes", DeclaredType::I64),
    ]
}

fn record(ts: i64, status: Option<i64>) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let mut attrs = vec![("bytes".to_string(), AttrValue::I64(ts))];
    if let Some(v) = status {
        attrs.push(("status".to_string(), AttrValue::I64(v)));
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

/// Write object `n`: one record per block, ts `n * 100 + 1..`.
async fn write_object(store: &dyn ObjectStoreBackend, n: usize) -> SegmentRef {
    let mut w = RlogWriter::new(
        RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        },
        ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [3u8; 16],
            writer_epoch: 1,
            writer_seq: n as u64 + 1,
        },
    );
    let base = n as i64 * 100;
    for (i, status) in STATUSES.iter().enumerate() {
        w.push(record(base + i as i64 + 1, *status)).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let key = format!("logs/ne-route-{n}.rlog");
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
        max_event_ts_ns: base + BLOCKS_PER_OBJECT as i64,
        ingest_hour_bucket: 0,
        sample_count: BLOCKS_PER_OBJECT as u64,
        series_count: 0,
        shard: 0,
        content_hash,
        writer_id: Uuid::from_u128(3),
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

struct Executed {
    route: Route,
    plan_init_ns: usize,
    blocks_total: usize,
    blocks_scanned: usize,
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

/// `COUNT(*) WHERE status <> 0` takes the whole-segment route the arm-free
/// `abs(status) <> 0` takes, with no plan pass (zero plan-init time), and on
/// that route decodes exactly the blocks that are not all zero: of 32 blocks,
/// the 16 whose stat proves min == max == 0 are skipped.
///
/// Against the parent commit, which declined every prune arm on the fast path,
/// the route assertion failed: zero fast-path segments and two
/// `fast_path_rejected_block_predicate` counts against four and zero.
#[tokio::test]
async fn not_equal_count_keeps_the_whole_segment_route_and_skips_all_zero_blocks() {
    let (store, snapshot) = fixture().await;

    let ne = execute(
        &store,
        snapshot.clone(),
        "SELECT COUNT(*) FROM logs WHERE status <> 0",
    )
    .await;
    let plain = execute(
        &store,
        snapshot,
        "SELECT COUNT(*) FROM logs WHERE abs(status) <> 0",
    )
    .await;

    assert_eq!(
        plain.route.fast_path_whole_object_segments + plain.route.fast_path_ranged_segments,
        OBJECTS,
        "the arm-free statement is on the whole-segment route: {:?}",
        plain.route
    );
    assert_eq!(
        ne.route, plain.route,
        "the NumNotEq arm alone does not change the route"
    );
    assert_eq!(plain.plan_init_ns, 0, "no plan pass on the arm-free route");
    assert_eq!(ne.plan_init_ns, 0, "no plan pass with the NumNotEq arm");

    let expected = NONZERO_PER_OBJECT * OBJECTS as i64;
    assert_eq!(count(&ne.batches), expected, "200, 404 and 500 per object");
    assert_eq!(count(&plain.batches), expected);

    let total = BLOCKS_PER_OBJECT * OBJECTS;
    assert_eq!((plain.blocks_total, plain.blocks_scanned), (total, total));
    assert_eq!(ne.blocks_total, total);
    assert_eq!(
        ne.blocks_scanned,
        (BLOCKS_PER_OBJECT - ALL_ZERO_BLOCKS_PER_OBJECT) * OBJECTS,
        "exactly the blocks that are not all zero are decoded"
    );
}

/// `status <> 0 AND bytes > 5` carries a `NumRange` arm beside the `NumNotEq`
/// arm, so it routes exactly as `bytes > 5` alone, which pushes only the range
/// arm: off the fast path on the block predicate, onto the planned route.
#[tokio::test]
async fn not_equal_beside_a_range_arm_routes_as_the_range_arm_alone() {
    let (store, snapshot) = fixture().await;

    let both = execute(
        &store,
        snapshot.clone(),
        "SELECT COUNT(*) FROM logs WHERE status <> 0 AND bytes > 5",
    )
    .await;
    let range = execute(
        &store,
        snapshot,
        "SELECT COUNT(*) FROM logs WHERE bytes > 5",
    )
    .await;

    assert_eq!(
        range.route,
        Route {
            fast_path_whole_object_segments: 0,
            fast_path_ranged_segments: 0,
            rejected_block_predicate: PARTITIONS,
            segments_unplanned: range.route.segments_unplanned,
        },
        "the range arm keeps the scan off the fast path"
    );
    assert_eq!(both.route, range.route, "the NumNotEq arm changes nothing");
    // Object 0's ts 1..=5 fail `bytes > 5`; its ts 6..=8 hold None, 0, 500.
    assert_eq!(count(&both.batches), 1 + NONZERO_PER_OBJECT * 3);
    assert_eq!(count(&range.batches), BLOCKS_PER_OBJECT as i64 * 4 - 5);
}

/// A late-materialized TopK over a `status <> 0` scan on the whole-segment
/// route returns the same rows as the arm-free filter. Phase 1's row refs
/// index the open's surviving-block list, which the pruning arm shortens, so
/// the fast path has to carry that list rather than assume every block
/// survives.
///
/// With the survivor list left unset on the fast path the statement failed
/// with "segment survivor mismatch: whole-segment open decoded block 2 at
/// drain position 0".
#[tokio::test]
async fn late_materialized_topk_over_a_not_equal_scan_on_the_fast_path() {
    let (store, snapshot) = fixture().await;

    let ne = execute(
        &store,
        snapshot.clone(),
        "SELECT * FROM logs WHERE status <> 0 ORDER BY ts DESC LIMIT 4",
    )
    .await;
    let plain = execute(
        &store,
        snapshot,
        "SELECT * FROM logs WHERE abs(status) <> 0 ORDER BY ts DESC LIMIT 4",
    )
    .await;

    assert!(
        ne.plan_text.contains("LogsRowFetchExec"),
        "the TopK is late-materialized; plan was:\n{}",
        ne.plan_text
    );
    assert_eq!(
        ne.route.fast_path_whole_object_segments + ne.route.fast_path_ranged_segments,
        OBJECTS,
        "phase 1 is on the whole-segment route: {:?}",
        ne.route
    );
    let ts = |batches: &[RecordBatch]| -> Vec<i64> {
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
    };
    // Object 3 holds ts 301..=308; its nonzero statuses sit at 303, 305, 308.
    assert_eq!(ts(&ne.batches), vec![308, 305, 303, 208]);
    assert_eq!(ts(&ne.batches), ts(&plain.batches));
}
