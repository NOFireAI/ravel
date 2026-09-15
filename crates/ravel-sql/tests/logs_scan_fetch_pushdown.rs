//! Tests for issue #362: `LogsScanExec::fetch()`/`with_fetch()` let
//! DataFusion's `LimitPushdown` physical optimizer push a LIMIT straight into
//! the scan instead of wrapping every partition's leaf in a `LocalLimitExec`.
//!
//! A prior attempt at this issue built an internal rows-emitted stop inside
//! the scan and measured no effect, because `LimitPushdown` already inserts a
//! `LocalLimitExec` above any multi-partition leaf that does not implement
//! `fetch`, and that node already stops the scan from opening further
//! segments once it stops polling. The actual change here is removing that
//! extra plan node by making the leaf report its own fetch, not making the
//! scan stop any earlier than it already did. Three properties are pinned:
//!
//! - `limit_removes_local_limit_and_reports_fetch_on_the_scan` (the plan-shape
//!   test, and the one that actually distinguishes this change from the
//!   no-op predecessor -- it comes first): `SELECT ts FROM logs LIMIT 10`'s
//!   optimized physical plan has no `LocalLimitExec` above the scan, and the
//!   scan itself reports `fetch() == Some(10)`.
//! - `fetch_pushdown_returns_exact_counts_and_correct_rows` (soundness): a
//!   LIMIT below, at, and above the total row count returns exactly the right
//!   count every time -- never fewer -- and every returned row is a real
//!   written row, across a plain scan, `ORDER BY`, and a residual `WHERE`
//!   above the scan.
//! - `fetch_stops_opening_segments_once_the_partition_limit_is_met` (GET-count
//!   regression): a partition made to own two whole segments opens only the
//!   first once the pushed per-partition fetch is satisfied by it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::array::{StringArray, TimestampNanosecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::{SessionConfig, SessionContext};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::LogsTableProvider;
use ravel_types::TenantHash;
use uuid::Uuid;

use util::CountingStore;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [7u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Cut a block every 3 records so a several-record object still has more than
/// one block.
fn small_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    }
}

/// A record on the single-`service.name` stream `svc`.
fn record(ts: i64, body: &str) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("svc".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

async fn write_object(
    store: &dyn ObjectStoreBackend,
    key: &str,
    content_hash: [u8; 32],
    records: &[LogRecord],
) -> SegmentRef {
    let mut w = RlogWriter::new(small_blocks(), identity());
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
        content_hash,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

fn provider(snapshot: Snapshot, fetcher: LogSegmentFetcher) -> LogsTableProvider {
    LogsTableProvider::new(
        snapshot,
        TenantHash([7u8; 16]),
        fetcher,
        PhaseAccounting::new(),
    )
}

async fn collect_plan(plan: Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect")
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// The `(ts, body)` pairs in `batches` from a `SELECT ts, body FROM logs ...`
/// projection (col 0 = ts, col 1 = body).
fn batches_to_rows(batches: &[RecordBatch]) -> BTreeSet<(i64, String)> {
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

/// The `ts` values of `batches`, in emission order (col 0 of a
/// `SELECT ts, ...` projection).
fn ts_sequence(batches: &[RecordBatch]) -> Vec<i64> {
    let mut out = Vec::new();
    for batch in batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        for i in 0..batch.num_rows() {
            out.push(ts.value(i));
        }
    }
    out
}

/// The `LogsScanExec` leaf of a physical plan, found by walking (the plan
/// above it is whatever the optimizer built).
fn find_by_name(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == name {
        return Some(Arc::clone(plan));
    }
    plan.children().iter().find_map(|c| find_by_name(c, name))
}

// ---------------------------------------------------------------------------
// Plan shape
// ---------------------------------------------------------------------------

/// The assertion that distinguishes this change from its no-op predecessor:
/// with `fetch`/`with_fetch` implemented, `LimitPushdown` absorbs the LIMIT
/// into the scan instead of wrapping it in a `LocalLimitExec`, and the scan
/// reports the pushed value back.
///
/// Before this change, `LogsScanExec` used the trait's default `fetch()`
/// (`None`) and had no `with_fetch` override, so `LimitPushdown` could not
/// push into it and inserted a `LocalLimitExec` per partition instead: both
/// assertions below fail on that code (`find_by_name(&plan,
/// "LocalLimitExec")` finds one, and there is no scan-reported fetch to read
/// because `LogsScanExec` never overrode `fetch()`).
#[tokio::test]
async fn limit_removes_local_limit_and_reports_fetch_on_the_scan() {
    let store = MemoryStore::new();
    let mut segments = Vec::new();
    for s in 0..3usize {
        let recs: Vec<LogRecord> = (0..5)
            .map(|i| record((s * 10 + i) as i64, &format!("s{s}-r{i}")))
            .collect();
        let mut content_hash = [0u8; 32];
        content_hash[0] = (s + 1) as u8;
        segments
            .push(write_object(&store, &format!("logs/seg{s}.rlog"), content_hash, &recs).await);
    }
    let snapshot = Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(backend);
    let table_provider = provider(snapshot, fetcher);

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("logs", Arc::new(table_provider))
        .expect("register table");

    let plan = ctx
        .sql("SELECT ts FROM logs LIMIT 10")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");

    assert!(
        find_by_name(&plan, "LocalLimitExec").is_none(),
        "LimitPushdown must not need a LocalLimitExec once the scan implements \
         `fetch`/`with_fetch` (issue #362); plan:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
    let scan = find_by_name(&plan, "LogsScanExec").expect("a LogsScanExec leaf");
    assert_eq!(
        scan.fetch(),
        Some(10),
        "the leaf must report the pushed LIMIT via `ExecutionPlan::fetch`; plan:\n{}",
        displayable(plan.as_ref()).indent(true)
    );
}

// ---------------------------------------------------------------------------
// Soundness: exact counts, correct rows, ORDER BY, residual WHERE
// ---------------------------------------------------------------------------

const SEGS: usize = 4;
const PER_SEG: usize = 5;
const TOTAL: usize = SEGS * PER_SEG;

/// Records whose `ts` interleaves across segments (`i * SEGS + s`, not
/// `s`-then-`i`), so concatenating segments in snapshot order is not
/// ts-ascending and the `ORDER BY` case below is non-vacuous. Each body
/// carries "even"/"odd" by ts parity so a `WHERE body LIKE '%odd%'` residual
/// has a real, known-size matching subset.
fn seg_records(s: usize) -> Vec<LogRecord> {
    (0..PER_SEG)
        .map(|i| {
            let ts = (i * SEGS + s) as i64;
            let parity = if ts % 2 == 0 { "even" } else { "odd" };
            record(ts, &format!("{parity}-s{s}-r{i}"))
        })
        .collect()
}

async fn build_fixture(store: &dyn ObjectStoreBackend) -> (Snapshot, BTreeSet<(i64, String)>) {
    let mut segments = Vec::with_capacity(SEGS);
    let mut want = BTreeSet::new();
    for s in 0..SEGS {
        let recs = seg_records(s);
        for r in &recs {
            want.insert((r.ts_ns, r.body.clone()));
        }
        let mut content_hash = [0u8; 32];
        content_hash[0] = (s + 1) as u8;
        let seg = write_object(
            store,
            &format!("logs/soundness-seg{s}.rlog"),
            content_hash,
            &recs,
        )
        .await;
        segments.push(seg);
    }
    let snapshot = Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    (snapshot, want)
}

/// Per-partition fetch is a bound the partition MAY act on, never a divided
/// share: DataFusion pushes the same LIMIT value to every partition of a
/// multi-partition leaf, and the exact cross-partition cap is enforced above
/// by `CoalescePartitionsExec`'s own `fetch`. This test pins the failure mode
/// the task calls out as worst: a wrong early stop that returns fewer rows
/// than requested. Every case below asserts the exact count (never fewer,
/// never more than available) and that every returned row is one that was
/// actually written and (for the WHERE case) actually matches the predicate.
#[tokio::test]
async fn fetch_pushdown_returns_exact_counts_and_correct_rows() {
    let store = MemoryStore::new();
    let (snapshot, want) = build_fixture(&store).await;
    assert_eq!(
        want.len(),
        TOTAL,
        "fixture must have no colliding (ts, body) pairs"
    );
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = LogSegmentFetcher::new(backend);
    let table_provider = provider(snapshot, fetcher);

    let config = SessionConfig::new().with_target_partitions(4);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table("logs", Arc::new(table_provider))
        .expect("register table");

    // Case A: a plain LIMIT below, at, and above TOTAL returns exactly
    // min(k, TOTAL) rows, every one of them a real written row.
    for k in [1usize, PER_SEG, TOTAL, TOTAL + 7] {
        let batches = ctx
            .sql(&format!("SELECT ts, body FROM logs LIMIT {k}"))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let want_count = k.min(TOTAL);
        assert_eq!(
            total_rows(&batches),
            want_count,
            "LIMIT {k} over {TOTAL} written rows must return exactly {want_count}, never fewer"
        );
        let got = batches_to_rows(&batches);
        assert!(
            got.is_subset(&want),
            "LIMIT {k} must return only rows that were actually written; got {got:?}"
        );
    }

    // Case B: ORDER BY ts LIMIT k returns exactly the first k of the fully
    // sorted oracle, ts ascending.
    let mut sorted_ts: Vec<i64> = want.iter().map(|(ts, _)| *ts).collect();
    sorted_ts.sort_unstable();
    for k in [3usize, TOTAL, TOTAL + 4] {
        let batches = ctx
            .sql(&format!("SELECT ts, body FROM logs ORDER BY ts LIMIT {k}"))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let got = ts_sequence(&batches);
        let want_count = k.min(TOTAL);
        assert_eq!(
            got,
            sorted_ts[..want_count],
            "ORDER BY ts LIMIT {k} must return exactly the first {want_count} ts \
             values ascending, never fewer"
        );
    }

    // Case C: a residual WHERE above the scan (a LIKE pattern the scan does
    // not push as a content prune) plus LIMIT returns exactly the right count
    // out of the matching subset, never fewer.
    let matching: BTreeSet<(i64, String)> = want
        .iter()
        .filter(|(_, b)| b.contains("odd"))
        .cloned()
        .collect();
    assert!(
        !matching.is_empty() && matching.len() < TOTAL,
        "fixture must have a proper, nonempty odd/even split"
    );
    for k in [2usize, matching.len(), matching.len() + 3] {
        let batches = ctx
            .sql(&format!(
                "SELECT ts, body FROM logs WHERE body LIKE '%odd%' LIMIT {k}"
            ))
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let want_count = k.min(matching.len());
        assert_eq!(
            total_rows(&batches),
            want_count,
            "WHERE body LIKE '%odd%' LIMIT {k} must return exactly {want_count} \
             matching rows, never fewer"
        );
        let got = batches_to_rows(&batches);
        assert!(
            got.is_subset(&matching),
            "every WHERE + LIMIT row must satisfy the residual predicate; got {got:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// GET-count regression: the scan stops opening segments once its partition's
// pushed fetch is satisfied
// ---------------------------------------------------------------------------

const GC_SEGMENTS: usize = 8;
const GC_PARTS: usize = 4;
const GC_RECORDS_PER_SEG: usize = 3;

fn gc_seg_records(s: usize) -> Vec<LogRecord> {
    (0..GC_RECORDS_PER_SEG)
        .map(|i| record((s * 1000 + i) as i64, &format!("s{s}-r{i}")))
        .collect()
}

/// A predicate-free, full-window fixture with `GC_SEGMENTS` segments and
/// `GC_SEGMENTS >= GC_PARTS`, which is exactly `LogsScanExec::
/// whole_segment_fast_path`'s admission rule: each of the `GC_PARTS`
/// partitions owns `GC_SEGMENTS / GC_PARTS` whole segments, assigned
/// `segment j -> partition j % GC_PARTS`, opened in ascending ordinal order.
async fn build_gc_fixture(store: &dyn ObjectStoreBackend) -> Snapshot {
    let mut segments = Vec::with_capacity(GC_SEGMENTS);
    for s in 0..GC_SEGMENTS {
        let recs = gc_seg_records(s);
        let mut content_hash = [0u8; 32];
        content_hash[0] = (s + 1) as u8;
        let seg = write_object(store, &format!("logs/gc-seg{s}.rlog"), content_hash, &recs).await;
        segments.push(seg);
    }
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// The property #362 is actually for: once a partition's own emitted row
/// count reaches its pushed `fetch`, it must stop opening further segments,
/// even though (per the plan-shape test above) nothing above it in the plan
/// is left to enforce that by polling less. Each `GC_PARTS` partition here
/// owns two whole segments; a fetch of 1 is satisfied by the first segment's
/// `GC_RECORDS_PER_SEG` rows alone, so the second must never be opened.
///
/// Without the early stop in `LogScanStream::poll_inner`, every partition
/// would drain both of its owned segments and this would read `GC_SEGMENTS`
/// (8) objects instead of `GC_PARTS` (4).
#[tokio::test]
async fn fetch_stops_opening_segments_once_the_partition_limit_is_met() {
    let counting = CountingStore::new(Arc::new(MemoryStore::new()));
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&counting) as Arc<dyn ObjectStoreBackend>;
    let snapshot = build_gc_fixture(store.as_ref()).await;
    assert_eq!(
        counting.gets(),
        0,
        "building the fixture must only PUT, never GET"
    );

    let fetcher = LogSegmentFetcher::new(Arc::clone(&store));
    let table_provider = provider(snapshot, fetcher);

    let plan = table_provider.plan(GC_PARTS).expect("plan");
    assert_eq!(
        counting.gets(),
        0,
        "building the plan must do no I/O (the fast path has no plan phase)"
    );

    let with_fetch = plan
        .with_fetch(Some(1))
        .expect("LogsScanExec must implement ExecutionPlan::with_fetch (issue #362)");
    assert_eq!(with_fetch.fetch(), Some(1));

    let batches = collect_plan(with_fetch).await;
    assert!(
        !batches.is_empty(),
        "each of the {GC_PARTS} partitions must still emit its first owned segment's rows"
    );

    assert_eq!(
        counting.gets(),
        GC_PARTS as u64,
        "each of the {GC_PARTS} partitions owns {} whole segments \
         (GC_SEGMENTS / GC_PARTS); with fetch = 1, every partition's first \
         owned segment (which alone has {GC_RECORDS_PER_SEG} rows) already \
         satisfies it, so no partition opens its second segment. At most \
         `partitions` segments opened, plus zero resolve GETs (the fast path \
         skips the plan phase entirely).",
        GC_SEGMENTS / GC_PARTS,
    );
}
