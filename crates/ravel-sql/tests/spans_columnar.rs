//! Acceptance gate for the columnar fast path of the `spans` SQL scan
//! (ADR-0110 decisions 3-6, issue #639, T3 of epic #630).
//!
//! The fast path builds Arrow arrays straight from `ravel_rspan`'s
//! `ColumnarBlockView` over the surviving rows, with no `SpanRecord` and no
//! `SpanRow`, and is taken only when the query is eligible: the projection
//! excludes the `attrs` map, no pending erasure predicate applies, and no block
//! carries an `attrs_raw` overflow page. Otherwise the unchanged row path runs.
//! Because the two paths' output is identical by construction, the
//! `columnar_batches`/`rowpath_batches` partition metrics are the only way to
//! prove which one ran.
//!
//! The tests drive [`ravel_sql::SpansScanExec`] directly rather than a full
//! `SessionContext`, so a projection is chosen exactly (no planner folding) and
//! the same input can be run through both paths for a byte-for-byte comparison:
//!
//! - the fast path is a scan with no pending erasure over a corpus with no
//!   `attrs_raw` spill;
//! - the row path is the *same* projection over the *same* corpus, forced
//!   ineligible by a pending erasure predicate (so it drains the row path),
//!   with the erasure either matching nothing (leaving the output identical) or
//!   matching the erased span (the data-protection case).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::{Array, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::erasure::snapshot_pending_erasure_predicates;
use ravel_rspan::{ObjectIdentity, RspanConfig, RspanWriter, SpanQuery, SpanRecord, StatusCode};
use ravel_sql::{
    SPAN_COL_ATTRS, SPAN_COL_DURATION_NS, SPAN_COL_NAME, SPAN_COL_SERVICE_NAME, SPAN_COL_START_TS,
    SPAN_COL_TRACE_ID, SpanSegmentFetcher, SpansScanExec, spans_schema,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([1u8; 16]);

/// The exact page geometry of [`two_object_corpus`], pinned so both paths'
/// `pages_decoded`/`pages_skipped` partition metrics are asserted as figures
/// rather than as "nonzero".
///
/// Geometry: two objects of three spans each, cut every two records
/// ([`small_blocks`]), so four blocks (2 + 1 per object). Each block stages nine
/// pages, one per column that has a value in it: `trace_id`, `span_id`, `name`,
/// `start_ts`, `end_ts`, `status_code`, `status_message`, `service_name` (the
/// v3 lift of `service.name`), and the dynamic `http.method` attribute column.
/// `parent_span_id` and `attrs_raw` are `None` on every fixture span, and a
/// column with no present value stages no page at all (`write_block`'s
/// `stage_column`), so neither contributes. A full decode therefore touches
/// 4 x 9 = 36 pages.
const ROW_PAGES_DECODED: usize = 36;
/// What the attrs-free projection of
/// [`columnar_path_taken_for_attrs_free_projection_and_matches_row_path`]
/// decodes out of those 36: `trace_id`, `name`, `start_ts`, `service_name`, and
/// `end_ts` (unprojected, but decoded for `duration_ns` and the ts predicate),
/// five pages per block over four blocks.
const COLUMNAR_PAGES_DECODED: usize = 20;
/// And the pages that projection walks past: `span_id`, `status_code`,
/// `status_message`, and the dynamic attribute column, four pages per block
/// over four blocks.
const COLUMNAR_PAGES_SKIPPED: usize = ROW_PAGES_DECODED - COLUMNAR_PAGES_DECODED;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [1u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Cut a block every 2 records so a small object still has several blocks and
/// the interleave across objects has something real to merge.
fn small_blocks() -> RspanConfig {
    RspanConfig {
        block_target_records: 2,
        ..RspanConfig::default()
    }
}

/// A span carrying a real attribute set: `service.name` (lifted to the v3
/// column) plus an ordinary dynamic attribute, so the block has attribute pages
/// the fast path skips (`pages_skipped > 0`) when the query excludes `attrs`.
fn span(trace: u8, span_id: u8, start: i64, service: &str, name: &str) -> SpanRecord {
    SpanRecord {
        trace_id: [trace; 16],
        span_id: [span_id; 8],
        parent_span_id: if span_id == 0 {
            None
        } else {
            Some([span_id - 1; 8])
        },
        name: name.to_string(),
        start_ts_ns: start,
        end_ts_ns: start + 100,
        status_code: StatusCode::Ok,
        status_message: Some(format!("msg {span_id}")),
        attrs: vec![
            ("service.name".to_string(), service.to_string()),
            ("http.method".to_string(), "GET".to_string()),
        ],
    }
}

/// Write one RSPAN object from `records`, put it at `key`, and return a matching
/// L0 [`SegmentRef`] carrying the object's true event-ts span.
async fn write_object(store: &MemoryStore, key: &str, records: &[SpanRecord]) -> SegmentRef {
    let mut w = RspanWriter::new(small_blocks(), identity());
    for r in records {
        w.push(r.clone());
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put object");

    let min = records
        .iter()
        .map(|r| r.start_ts_ns)
        .min()
        .expect("nonempty");
    let max = records.iter().map(|r| r.end_ts_ns).max().expect("nonempty");
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
        segment_format_version: u32::from(ravel_rspan::footer::VERSION),
    }
}

struct ScanRun {
    batches: Vec<RecordBatch>,
    columnar_batches: usize,
    rowpath_batches: usize,
    pages_decoded: usize,
    pages_skipped: usize,
}

/// Execute a `SpansScanExec` directly over `segments` with the given projection
/// and pending-erasure requests, returning its batches and the path metrics. A
/// single partition keeps the emitted order deterministic so the two paths'
/// batches compare byte for byte.
async fn run_scan(
    store: Arc<dyn ObjectStoreBackend>,
    segments: Vec<SegmentRef>,
    projection: Option<Vec<usize>>,
    erasure_reqs: Vec<ravel_proto::commit::v1::ErasureRequest>,
) -> ScanRun {
    let erasure = snapshot_pending_erasure_predicates(&Snapshot {
        segments: segments.clone(),
        segments_pruned: 0,
        pending_erasure: erasure_reqs,
    });
    let scan = SpansScanExec::new(
        TENANT,
        SpanSegmentFetcher::new(store),
        &segments,
        1,
        SpanQuery::ts_range(i64::MIN, i64::MAX),
        None,
        None,
        None,
        None,
        Arc::new(erasure),
        projection,
        QueryAccounting::new(),
    )
    .expect("build scan");

    let mut stream = scan
        .execute(0, Arc::new(TaskContext::default()))
        .expect("execute");
    let mut batches = Vec::new();
    while let Some(next) = stream.next().await {
        batches.push(next.expect("batch"));
    }
    drop(stream);

    let metrics = scan.metrics().expect("metrics");
    let count = |name: &str| metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
    ScanRun {
        columnar_batches: count("columnar_batches"),
        rowpath_batches: count("rowpath_batches"),
        pages_decoded: count("pages_decoded"),
        pages_skipped: count("pages_skipped"),
        batches,
    }
}

/// A pending erasure request matching `key = value` on the merged attribute map.
fn erasure_req(key: &str, value: &str) -> ravel_proto::commit::v1::ErasureRequest {
    ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: key.to_string(),
            value: value.to_string(),
        }],
        ..Default::default()
    }
}

/// Two objects with interleaving trace ids, so the partition's single stable
/// `(trace_id, start_ts)` merge across objects is exercised, not just one
/// object's already-sorted rows.
async fn two_object_corpus(store: &MemoryStore) -> Vec<SegmentRef> {
    // Object A holds trace ids 0, 2, 4; object B holds 1, 3, 5. A correct merge
    // interleaves them into 0,1,2,3,4,5.
    let recs_a = vec![
        span(0, 0, 10, "checkout", "a0"),
        span(2, 0, 30, "checkout", "a2"),
        span(4, 0, 50, "payments", "a4"),
    ];
    let recs_b = vec![
        span(1, 0, 20, "payments", "b1"),
        span(3, 0, 40, "inventory", "b3"),
        span(5, 0, 60, "checkout", "b5"),
    ];
    let seg_a = write_object(store, "spans/a.rspan", &recs_a).await;
    let seg_b = write_object(store, "spans/b.rspan", &recs_b).await;
    vec![seg_a, seg_b]
}

/// The trace-id column of a batch, one 16-byte id per row.
fn trace_ids(batch: &RecordBatch, col: usize) -> Vec<[u8; 16]> {
    let arr = batch
        .column(col)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("trace_id column");
    (0..arr.len())
        .map(|i| arr.value(i).try_into().expect("16-byte trace"))
        .collect()
}

/// The acceptance test (ADR-0110 decision 3): over a two-object corpus and a
/// projection that excludes `attrs`, the scan takes the columnar fast path
/// (`columnar_batches > 0`, `rowpath_batches == 0`), and its output equals the
/// row path's exactly (same projected schema, same rows, same order). The row
/// path is the same projection forced ineligible by a no-match erasure
/// predicate. The two paths return identical rows by construction, so the
/// metric is what proves the fast path actually ran.
#[tokio::test]
async fn columnar_path_taken_for_attrs_free_projection_and_matches_row_path() {
    let store = MemoryStore::new();
    let segments = two_object_corpus(&store).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    // Projects one column of every kind the fast path builds: a fixed id
    // (trace_id), a string (name), an i64 timestamp (start_ts), the computed
    // duration_ns, and the nullable v3 service_name. `attrs` is excluded, so the
    // query is eligible.
    let projection = vec![
        SPAN_COL_TRACE_ID,
        SPAN_COL_NAME,
        SPAN_COL_START_TS,
        SPAN_COL_DURATION_NS,
        SPAN_COL_SERVICE_NAME,
    ];

    let fast = run_scan(
        Arc::clone(&store),
        segments.clone(),
        Some(projection.clone()),
        Vec::new(),
    )
    .await;
    assert!(
        fast.columnar_batches > 0,
        "the eligible attrs-free projection must take the columnar fast path"
    );
    assert_eq!(
        fast.rowpath_batches, 0,
        "the fast path must not fall back to the row path"
    );
    // The projection excludes the dynamic attribute pages, so decode skipped
    // them: the page-level proof projection reached the decode. Pinned exactly,
    // not just `> 0`, so a change to what the fast path decodes is caught here
    // rather than absorbed: over this corpus's four blocks the columnar decode
    // touches COLUMNAR_PAGES_DECODED pages and walks past
    // COLUMNAR_PAGES_SKIPPED, and their sum is the same whole-block page total
    // the row path decodes (ROW_PAGES_DECODED).
    assert_eq!(
        (fast.pages_decoded, fast.pages_skipped),
        (COLUMNAR_PAGES_DECODED, COLUMNAR_PAGES_SKIPPED),
        "the columnar path's exact page split over this corpus"
    );
    assert_eq!(
        fast.pages_decoded + fast.pages_skipped,
        ROW_PAGES_DECODED,
        "every page of every scanned block is either decoded or skipped"
    );

    // Row path: same projection, forced ineligible by a no-match erasure so it
    // drains the row path yet erases nothing (output stays identical).
    let row = run_scan(
        Arc::clone(&store),
        segments.clone(),
        Some(projection.clone()),
        vec![erasure_req("user_id", "nobody-has-this")],
    )
    .await;
    assert_eq!(
        row.columnar_batches, 0,
        "an erasure-carrying scan must not run the columnar path"
    );
    assert!(
        row.rowpath_batches > 0,
        "the row path must have run for the ineligible scan"
    );

    // Identical projected schema, rows, and order.
    assert_eq!(
        fast.batches, row.batches,
        "the fast path output must equal the row path exactly"
    );

    // The advertised order (trace_id asc, start_ts asc): trace_id must be
    // non-decreasing across the fast path's emitted rows, and the interleave of
    // the two objects must have produced the full 0..=5 sequence.
    let ids: Vec<[u8; 16]> = fast.batches.iter().flat_map(|b| trace_ids(b, 0)).collect();
    assert_eq!(ids.len(), 6, "every span is returned exactly once");
    let mut sorted = ids.clone();
    sorted.sort();
    assert_eq!(ids, sorted, "rows are emitted in trace_id ascending order");
    assert_eq!(
        ids,
        (0u8..6).map(|t| [t; 16]).collect::<Vec<_>>(),
        "the two objects were interleaved into 0,1,2,3,4,5"
    );
}

/// Issue #669: the `pages_decoded`/`pages_skipped` partition metrics mean the
/// same thing on the row path as on the columnar one. An attrs-including
/// projection is ineligible for the fast path, so this scan drains the row path,
/// and the two counters report what its decode actually did: every page of every
/// block it scanned decoded ([`ROW_PAGES_DECODED`]), none skipped.
///
/// Both figures are pinned exactly. The zero is the point as much as the nonzero
/// one: before the row arm wrote these counters, `EXPLAIN ANALYZE` on an
/// attrs-including query showed `pages_decoded=0, pages_skipped=0`, which reads
/// as "this decode touched nothing" rather than "nobody counted". With the arm
/// instrumented, `pages_decoded=36` beside `pages_skipped=0` says the true
/// thing: the row path decodes whole blocks and skips nothing.
///
/// Flip to watch it fail: delete the two `this.metrics.pages_*.add(...)` lines
/// from the `Prepared::Rows` arm of `SpanScanStream::poll_next`
/// (`spans_scan.rs`), the uninstrumented state this closes. Both assertions
/// below then report 0 for `pages_decoded` while `rowpath_batches` still proves
/// the row path ran.
#[tokio::test]
async fn row_path_reports_the_pages_it_decoded() {
    let store = MemoryStore::new();
    let segments = two_object_corpus(&store).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    // Including `attrs` is the eligibility axis: the fast path never builds the
    // merged map, so this projection drains the row path (ADR-0110 decision 3),
    // with no erasure predicate needed to force it.
    let projection = vec![SPAN_COL_TRACE_ID, SPAN_COL_NAME, SPAN_COL_ATTRS];
    let run = run_scan(
        Arc::clone(&store),
        segments.clone(),
        Some(projection),
        Vec::new(),
    )
    .await;

    assert_eq!(
        run.columnar_batches, 0,
        "an attrs-including projection must not run the columnar path"
    );
    assert!(
        run.rowpath_batches > 0,
        "the row path must have run for the attrs-including projection"
    );

    assert_eq!(
        run.pages_decoded, ROW_PAGES_DECODED,
        "the row path decodes every page of the four blocks it scanned"
    );
    assert_eq!(
        run.pages_skipped, 0,
        "and skips none: it requests every column, so nothing is walked past"
    );

    // The scan still returned the rows, projected: the metrics were not bought
    // by changing what the row path emits.
    let total: usize = run.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 6, "every span returned once");
    let ids: Vec<[u8; 16]> = run.batches.iter().flat_map(|b| trace_ids(b, 0)).collect();
    assert_eq!(
        ids,
        (0u8..6).map(|t| [t; 16]).collect::<Vec<_>>(),
        "in (trace_id, start_ts) order across both objects"
    );
}

/// The `attrs_raw` fallback's page counts (#669). An eligible query whose block
/// turns out to carry an `attrs_raw` overflow page abandons the columnar attempt
/// and re-reads the whole partition through the row path, so the partition's
/// decode really did both: the projected pages of the block the attempt reached,
/// then every page of that block again. Both show up in `pages_decoded`, and the
/// attempt's skipped pages in `pages_skipped`, which is why this partition is
/// the one case where a row-path scan reports a nonzero `pages_skipped`.
///
/// Geometry: one span carrying 1001 distinct dynamic attributes. RSPAN gives the
/// first 1000 (sorted) their own columns and spills the last into `attrs_raw`
/// (`MAX_DYNAMIC_COLUMNS` in `ravel-rspan`'s writer), so the single block holds
/// 1000 dynamic pages plus `attrs_raw`, `trace_id`, `span_id`, `name`,
/// `start_ts`, `end_ts`, `status_code`, `status_message`, and `service_name`:
/// 1009 pages. The columnar attempt decodes 4 of them (`trace_id`, `start_ts`,
/// `end_ts`, `name` -- the projection unioned with the ordering and predicate
/// columns) and skips 1005; the row path then decodes all 1009.
///
/// Flip to watch it fail: drop the abandoned attempt's counts in
/// `prepare_partition`'s `ColumnarAttempt::FellBack` arm (`pages_decoded = 0`,
/// `pages_skipped = 0`). The metrics then report `(1009, 0)`, the row decode
/// alone, and this reads as a partition that never attempted the fast path.
#[tokio::test]
async fn attrs_raw_fallback_counts_the_abandoned_attempt_and_the_row_decode() {
    let store = MemoryStore::new();
    let mut spill = span(0, 0, 10, "checkout", "spill");
    spill.attrs = std::iter::once(("service.name".to_string(), "checkout".to_string()))
        .chain((0..1001).map(|i| (format!("k{i:04}"), i.to_string())))
        .collect();
    let seg = write_object(&store, "spans/attrs-raw.rspan", &[spill]).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    // Eligible by query shape (no `attrs`, no erasure): the fallback is decided
    // per block, at decode.
    let projection = vec![SPAN_COL_TRACE_ID, SPAN_COL_NAME, SPAN_COL_START_TS];
    let run = run_scan(store, vec![seg], Some(projection), Vec::new()).await;

    assert_eq!(
        run.columnar_batches, 0,
        "an attrs_raw block must force the whole partition to the row path"
    );
    assert!(run.rowpath_batches > 0, "the row path must have run");
    assert_eq!(
        (run.pages_decoded, run.pages_skipped),
        (4 + 1009, 1005),
        "the abandoned columnar attempt's pages plus the row path's whole-block decode"
    );

    let total: usize = run.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1, "the span is still returned exactly once");
}

/// The data-protection case (ADR-0110 decision 3): with a pending erasure
/// predicate active, the scan takes the row path (`columnar_batches == 0`,
/// `rowpath_batches > 0`) and the erased span is absent from the output.
///
/// This is the clause that fails closed on purpose: `is_erased_span` matches
/// against the merged attribute map, exactly the structure the fast path never
/// builds, so a fast path that ignored the erasure clause would re-serve erased
/// spans to any query that excludes `attrs`. The two paths return identical rows
/// whenever the predicate matches nothing, so a rows-only assertion is vacuous;
/// `rowpath_batches > 0` is what proves the fallback ACTUALLY fired. Deleting
/// the `erasure.is_empty() &&` term from `columnar_static_eligible`
/// (spans_scan.rs) turns this red: the scan then runs columnar
/// (`columnar_batches > 0`, `rowpath_batches == 0`) and the erased span
/// reappears.
#[tokio::test]
async fn pending_erasure_forces_the_row_path() {
    let store = MemoryStore::new();
    // Two spans on one trace: the u1 span is erased, the u2 span survives.
    let mut erase = span(0, 0, 10, "checkout", "erase-me");
    erase.attrs = vec![("user_id".to_string(), "u1".to_string())];
    let mut keep = span(0, 1, 20, "checkout", "keep-me");
    keep.attrs = vec![("user_id".to_string(), "u2".to_string())];
    let seg = write_object(&store, "spans/erasure.rspan", &[erase, keep]).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    // A projection that WOULD be eligible (excludes attrs) but for the erasure.
    let projection = vec![SPAN_COL_TRACE_ID, SPAN_COL_NAME, SPAN_COL_START_TS];
    let run = run_scan(
        store,
        vec![seg],
        Some(projection),
        vec![erasure_req("user_id", "u1")],
    )
    .await;

    // The load-bearing assertion: the row path fired. This holds for ANY pending
    // erasure, not only one that happens to match, so it is not vacuous.
    assert_eq!(
        run.columnar_batches, 0,
        "a pending erasure predicate must forbid the fast path"
    );
    assert!(
        run.rowpath_batches > 0,
        "the row path must have run under a pending erasure"
    );

    // And correctness: the erased u1 span is gone, the u2 span survives.
    let names: Vec<String> = run
        .batches
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name column");
            (0..arr.len())
                .map(|i| arr.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(names, vec!["keep-me".to_string()], "the u1 span is erased");
}

/// Guards the projected-schema wiring: an eligible scan emits exactly the
/// projected columns in projection order, and `duration_ns` is computed from
/// start/end even though neither is otherwise projected.
#[tokio::test]
async fn eligible_scan_emits_the_projected_schema_in_order() {
    let store = MemoryStore::new();
    let segments = two_object_corpus(&store).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    // Deliberately out of schema order, and projecting duration_ns alone among
    // the ts-derived columns.
    let projection = vec![SPAN_COL_NAME, SPAN_COL_DURATION_NS];
    let run = run_scan(
        Arc::clone(&store),
        segments,
        Some(projection.clone()),
        Vec::new(),
    )
    .await;
    assert!(run.columnar_batches > 0, "must take the fast path");

    let expected = spans_schema().project(&projection).expect("project schema");
    for b in &run.batches {
        assert_eq!(b.schema().as_ref(), &expected, "projected schema, in order");
        // duration_ns == 100 for every fixture span (end = start + 100).
        let dur = b
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("duration_ns column");
        for i in 0..dur.len() {
            assert_eq!(dur.value(i), 100, "duration_ns is end - start");
        }
    }
    let total: usize = run.batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 6, "every span returned once");
    // Prove start_ts was decoded for the interleave even though it is not
    // projected: the names come out in (trace_id, start_ts) order, which for
    // this corpus is a0,b1,a2,b3,a4,b5.
    let names: Vec<String> = run
        .batches
        .iter()
        .flat_map(|b| {
            let arr = b
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name column");
            (0..arr.len())
                .map(|i| arr.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(names, vec!["a0", "b1", "a2", "b3", "a4", "b5"]);
}
