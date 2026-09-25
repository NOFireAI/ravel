//! Integration tests for [`ravel_sql::SpansTableProvider`] (ADR-0041), the `spans` SQL table over an already-resolved `Signal::Spans`
//! snapshot.
//!
//! Two properties are pinned:
//!
//! - `scan_prunes_by_ts_window_returns_exact_rows` (the acceptance test, the
//!   span sibling of the `logs`
//!   `scan_prunes_by_ts_and_word_returns_exact_rows`): a ts window returns
//!   exactly the spans that overlap it across several objects, with no false
//!   positives and no false negatives, checked against an independent oracle.
//!   An object whose whole span lies outside the window is pruned before any
//!   GET.
//! - `trace_id_query_takes_the_cheap_trace_lookup`: a `trace_id = X` filter is
//!   compiled into a [`SpanQuery::trace`] single-trace lookup (proven by
//!   inspecting the built `SpansScanExec`), and that lookup scans strictly
//!   fewer blocks than the equivalent full `ts_range` window scan over the same
//!   object (proven by the reader's [`ScanStats`]) while still returning
//!   exactly that trace's spans. This is the RSPAN trace-routing fast path
//!   ADR-0041 exists for.

#![allow(clippy::expect_used, clippy::unwrap_used)]

// The Flight SQL reachability test drives the shared in-process harness, which
// only links under the `flight-sql` feature.
#[cfg(feature = "flight-sql")]
mod util;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{
    Array, FixedSizeBinaryArray, StringArray, TimestampNanosecondArray,
};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{col, lit};
use datafusion::scalar::ScalarValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::trace::v1::span::Event as SpanEventProto;
use opentelemetry_proto::tonic::trace::v1::span::Link as SpanLinkProto;
use prost::Message as _;
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel, SegmentRef, Snapshot};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_rspan::record::{EVENTS_RAW_KEY, LINKS_RAW_KEY};
use ravel_rspan::{
    ObjectIdentity, RspanConfig, RspanWriter, ScanStats, SpanQuery, SpanRecord, StatusCode,
};
use ravel_sql::{SpanSegmentFetcher, SpansScanExec, SpansTableProvider, SqlConfig, SqlExecutor};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: [1u8; 16],
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Cut a block every 2 records so small test objects still have many blocks and
/// the skip index has something real to prune.
fn small_blocks() -> RspanConfig {
    RspanConfig {
        block_target_records: 2,
        ..RspanConfig::default()
    }
}

fn span(trace: [u8; 16], span_id: u8, start: i64, end: i64, name: &str) -> SpanRecord {
    SpanRecord {
        trace_id: trace,
        span_id: [span_id; 8],
        parent_span_id: if span_id == 0 {
            None
        } else {
            Some([span_id - 1; 8])
        },
        name: name.to_string(),
        start_ts_ns: start,
        end_ts_ns: end,
        status_code: StatusCode::Ok,
        status_message: Some(format!("msg {span_id}")),
        attrs: vec![("svc".to_string(), "api".to_string())],
    }
}

/// Write one RSPAN object from `records`, put it at `key`, and return a matching
/// L0 [`SegmentRef`] carrying the object's true event-ts span
/// (`[min start_ts, max end_ts]`, the interval the summary prunes against).
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
        declared_column_stats: Default::default(),
    }
}

fn ts_lit(v: i64) -> datafusion::logical_expr::Expr {
    lit(ScalarValue::TimestampNanosecond(Some(v), None))
}

fn trace_id_lit(bytes: [u8; 16]) -> datafusion::logical_expr::Expr {
    lit(ScalarValue::FixedSizeBinary(16, Some(bytes.to_vec())))
}

/// Reduce output batches to the set of `(trace_id, span_id, start_ts, name)`
/// rows they contain, asserting the public `spans` schema on the way.
fn batches_to_rows(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> BTreeSet<([u8; 16], [u8; 8], i64, String)> {
    let mut out = BTreeSet::new();
    for batch in batches {
        assert_eq!(
            batch.schema(),
            ravel_sql::spans_schema(),
            "public spans schema"
        );
        let trace = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("trace_id col");
        let span = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("span_id col");
        let start = batch
            .column(4)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("start_ts col");
        let name = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name col");
        for i in 0..batch.num_rows() {
            let tid: [u8; 16] = trace.value(i).try_into().expect("16-byte trace");
            let sid: [u8; 8] = span.value(i).try_into().expect("8-byte span");
            out.insert((tid, sid, start.value(i), name.value(i).to_string()));
        }
    }
    out
}

async fn collect_plan(
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Vec<datafusion::arrow::record_batch::RecordBatch> {
    collect(plan, Arc::new(TaskContext::default()))
        .await
        .expect("collect")
}

/// The epic's acceptance test: a ts window returns exactly the spans that
/// overlap it across several objects, checked against an independent oracle. The
/// filter `end_ts >= lo AND start_ts <= hi` is precisely the interval-overlap
/// predicate the reader prunes and re-checks with, so the scan leaf's output
/// already equals the oracle (no false positives, no false negatives), and the
/// out-of-window object is pruned before any GET.
#[tokio::test]
async fn scan_prunes_by_ts_window_returns_exact_rows() {
    let store = MemoryStore::new();

    let t1 = [0x11u8; 16];
    let t2 = [0x22u8; 16];
    let t3 = [0x33u8; 16];

    // Object A: trace T1, spans [100,110]..[108,118]; several overlap the window.
    let obj_a: Vec<SpanRecord> = (0..5u8)
        .map(|i| {
            let s = 100 + i64::from(i) * 2;
            span(t1, i, s, s + 10, &format!("a-{i}"))
        })
        .collect();
    // Object B: trace T2, spans [1000,1005].. entirely outside the window; must
    // be pruned before any GET.
    let obj_b: Vec<SpanRecord> = (0..4u8)
        .map(|i| {
            let s = 1000 + i64::from(i) * 5;
            span(t2, i, s, s + 5, &format!("b-{i}"))
        })
        .collect();
    // Object C: traces T1 and T3 interleaved, spans around [200,240].
    let obj_c: Vec<SpanRecord> = (0..6u8)
        .map(|i| {
            let s = 200 + i64::from(i) * 8;
            let trace = if i % 2 == 0 { t1 } else { t3 };
            span(trace, i, s, s + 6, &format!("c-{i}"))
        })
        .collect();

    let ref_a = write_object(&store, "spans/a.rspan", &obj_a).await;
    let ref_b = write_object(&store, "spans/b.rspan", &obj_b).await;
    let ref_c = write_object(&store, "spans/c.rspan", &obj_c).await;

    let snapshot = Snapshot {
        segments: vec![ref_a, ref_b, ref_c],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = SpanSegmentFetcher::new(store);
    let provider = SpansTableProvider::new(
        snapshot,
        TenantHash([1u8; 16]),
        fetcher,
        QueryAccounting::new(),
    );

    // WHERE end_ts >= 50 AND start_ts <= 250  (== overlap with the window [50,250])
    let (lo, hi) = (50i64, 250i64);
    let filters = vec![
        col("end_ts").gt_eq(ts_lit(lo)),
        col("start_ts").lt_eq(ts_lit(hi)),
    ];
    let plan = provider.plan_filters(4, &filters).expect("build plan");
    let batches = collect_plan(plan).await;
    let got = batches_to_rows(&batches);

    // Independent oracle: every source span whose interval overlaps [lo, hi].
    let mut want = BTreeSet::new();
    for records in [&obj_a, &obj_b, &obj_c] {
        for r in records {
            if r.start_ts_ns <= hi && r.end_ts_ns >= lo {
                want.insert((r.trace_id, [r.span_id[0]; 8], r.start_ts_ns, r.name.clone()));
            }
        }
    }

    // Sanity: the oracle keeps object A and C spans that overlap, drops all of B.
    assert!(!want.is_empty(), "oracle should keep some rows");
    assert!(
        want.iter().all(|(tid, _, _, _)| *tid != t2),
        "no T2 (object B) row should survive the window"
    );
    assert_eq!(
        got, want,
        "scan output must equal the overlap oracle exactly"
    );
}

/// A `trace_id = X` filter provably takes the cheap [`SpanQuery::trace`] path:
///
/// 1. The provider compiles the filter into a `SpanQuery` whose `trace_id` is
///    `Some(X)` (inspected on the built `SpansScanExec`), not a bare
///    `ts_range` scan.
/// 2. That trace lookup scans strictly fewer blocks than the full-window
///    `ts_range` scan over the same object (the reader's [`ScanStats`]), which
///    is the whole point of ADR-0041's trace_id-keyed block routing.
/// 3. It still returns exactly that trace's spans and nothing else.
#[tokio::test]
async fn trace_id_query_takes_the_cheap_trace_lookup() {
    let store = MemoryStore::new();

    // One object, ten traces, four spans each, two records per block -> ~20
    // blocks, so a single-trace lookup can prune most of them.
    let traces: Vec<[u8; 16]> = (0u8..10).map(|t| [t; 16]).collect();
    let mut records = Vec::new();
    for (t, tid) in traces.iter().enumerate() {
        for s in 0u8..4 {
            let start = i64::from(t as u8) * 1000 + i64::from(s);
            records.push(span(*tid, s, start, start + 1, &format!("t{t}-s{s}")));
        }
    }
    let seg = write_object(&store, "spans/trace.rspan", &records).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = SpanSegmentFetcher::new(Arc::clone(&store));

    let snapshot = Snapshot {
        segments: vec![seg.clone()],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };
    let provider = SpansTableProvider::new(
        snapshot,
        TenantHash([1u8; 16]),
        fetcher.clone(),
        QueryAccounting::new(),
    );

    let target = traces[5];

    // (1) The provider compiles `trace_id = target` into a SpanQuery::trace.
    let plan = provider
        .plan_filters(1, &[col("trace_id").eq(trace_id_lit(target))])
        .expect("build plan");
    // Trait-upcast to `Any` to downcast, sidestepping the `as_any` name shared
    // by the in-scope arrow `Array` trait.
    let any_ref: &dyn std::any::Any = plan.as_ref();
    let issued = any_ref
        .downcast_ref::<SpansScanExec>()
        .expect("plan_filters returns a bare SpansScanExec (no projection)")
        .query();
    assert_eq!(
        issued.trace_id,
        Some(target),
        "a trace_id = literal filter must compile to a SpanQuery::trace lookup"
    );

    // (2) The trace lookup scans strictly fewer blocks than the full ts_range
    //     scan over the same object. Spy on the reader via the fetcher's stats.
    let full: ScanStats = fetcher
        .fetch(
            &seg,
            &SpanQuery::ts_range(i64::MIN, i64::MAX),
            None,
            None,
            &[],
        )
        .await
        .expect("fetch full")
        .expect("relevant")
        .stats;
    let trace_only: ScanStats = fetcher
        .fetch(&seg, &issued, None, None, &[])
        .await
        .expect("fetch trace")
        .expect("relevant")
        .stats;
    assert_eq!(full.blocks_total, trace_only.blocks_total, "same object");
    assert!(
        trace_only.blocks_scanned < full.blocks_scanned,
        "trace lookup must scan fewer blocks ({} of {}) than the full scan ({})",
        trace_only.blocks_scanned,
        trace_only.blocks_total,
        full.blocks_scanned,
    );

    // (3) It returns exactly that trace's spans, and nothing from other traces.
    let batches = collect_plan(plan).await;
    let got = batches_to_rows(&batches);
    let mut want = BTreeSet::new();
    for s in 0u8..4 {
        let start = 5i64 * 1000 + i64::from(s);
        want.insert((target, [s; 8], start, format!("t5-s{s}")));
    }
    assert_eq!(
        got, want,
        "trace query returns exactly the target trace's spans"
    );
}

/// (ADR-0064 decision 3): the `spans` SQL table wires selective erasure --
/// `SpansTableProvider` derives predicates from `snapshot.pending_erasure`,
/// and `SpansScanExec` calls the `is_erased_span` filter `ravel-query`'s
/// erasure module documents as built for this surface. This proves:
/// a pending selective-erasure request on the resolved snapshot excludes
/// matching spans through the real `SpansTableProvider` scan path. Covers
/// `SpansTableProvider::new` computing `erasure` from
/// `snapshot_pending_erasure_predicates` and passing it into `SpansScanExec`
/// (spans_provider.rs), and `SpansScanExec::execute`/`prepare_partition`
/// calling `is_erased_span` on each decoded row before sort/build
/// (spans_scan.rs); reverting the `if !erasure.is_empty() { out.retain(...) }`
/// block in spans_scan.rs's `prepare_partition` makes the erased span
/// reappear.
#[tokio::test]
async fn pending_erasure_excludes_matching_spans() {
    let store = MemoryStore::new();

    let t1 = [0x11u8; 16];
    let mut hold = span(t1, 0, 100, 110, "erase-me");
    hold.attrs = vec![("user_id".to_string(), "u1".to_string())];
    let mut keep = span(t1, 1, 200, 210, "keep-me");
    keep.attrs = vec![("user_id".to_string(), "u2".to_string())];
    let records = vec![hold, keep.clone()];

    let seg = write_object(&store, "spans/erasure.rspan", &records).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = SpanSegmentFetcher::new(store);

    let request = ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "user_id".to_string(),
            value: "u1".to_string(),
        }],
        ..Default::default()
    };
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: vec![request],
    };
    let provider = SpansTableProvider::new(
        snapshot,
        TenantHash([1u8; 16]),
        fetcher,
        QueryAccounting::new(),
    );
    let plan = provider.plan(1).expect("build plan");
    let batches = collect_plan(plan).await;
    let got = batches_to_rows(&batches);

    let mut want = BTreeSet::new();
    want.insert((
        keep.trace_id,
        [keep.span_id[0]; 8],
        keep.start_ts_ns,
        keep.name.clone(),
    ));
    assert_eq!(got, want, "the u1 span is erased; the u2 span survives");
}

/// (ADR-0044) The spans scan path is request/byte accounted, the same way the
/// logs path is (the sibling of `tests/query_accounting.rs`'s
/// `a_logs_query_is_accounted`). A `QueryAccounting` handle is threaded through
/// `SpansTableProvider` -> `SpansScanExec` -> `SpanSegmentFetcher::fetch_accounted`,
/// so executing the scan records exactly one `Get` request and the object's
/// transferred bytes against it.
///
/// This is proven at the provider/scan level rather than end to end through
/// `SqlExecutor`, because no production `spans` caller exists yet: routing has
/// no `Spans` arm until phase 2 (ADR-0045 decision 5). The provider scan path
/// exercised here is exactly the one that caller will drive.
///
/// The counts are exact, not just non-zero: one segment yields one whole-object
/// GET (`SPAN_REQUESTS_PER_SEGMENT`), and the recorded bytes equal the object's
/// size. Reverting `fetch` in place of `fetch_accounted` in
/// `spans_scan.rs::prepare_partition` drops both counts to zero and fails this.
#[tokio::test]
async fn a_spans_scan_is_accounted() {
    let store = MemoryStore::new();

    let t1 = [0x11u8; 16];
    let records: Vec<SpanRecord> = (0u8..6)
        .map(|i| {
            let s = 100 + i64::from(i);
            span(t1, i, s, s + 1, &format!("op-{i}"))
        })
        .collect();
    let seg = write_object(&store, "spans/accounted.rspan", &records).await;
    let expected_bytes = seg.object_size;
    assert!(expected_bytes > 0, "the written object must have a size");

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let fetcher = SpanSegmentFetcher::new(store);
    let snapshot = Snapshot {
        segments: vec![seg],
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    };

    let accounting = QueryAccounting::new();
    let provider =
        SpansTableProvider::new(snapshot, TenantHash([1u8; 16]), fetcher, accounting.clone());

    // A full scan (no pushdown) over the single segment: one whole-object GET.
    let plan = provider.plan(1).expect("build plan");
    let batches = collect_plan(plan).await;
    assert!(
        !batches.is_empty() && batches.iter().map(|b| b.num_rows()).sum::<usize>() == records.len(),
        "the scan must return every span so the fetch really ran"
    );

    let snap = accounting.snapshot();
    assert_eq!(
        snap.total_s3_requests(),
        1,
        "one segment yields exactly one accounted GET"
    );
    assert_eq!(
        snap.s3_requests(AccountedOp::Get),
        1,
        "the accounted request is a Get"
    );
    assert_eq!(
        snap.s3_bytes(AccountedOp::Get),
        expected_bytes,
        "the accounted GET records the whole object's transferred bytes"
    );
}

fn tenant() -> TenantId {
    TenantId::new("trace-id-hex-literal".to_string())
}

/// Publish one RSPAN object as a real `Signal::Spans` commit record, so
/// `SqlExecutor`'s `Catalog::resolve` finds it exactly as a production caller
/// would (no hand-built `Snapshot`). Mirrors
/// `flight_reachability::publish_spans_segment` below, which cannot be reused
/// directly: that copy lives inside a `#[cfg(feature = "flight-sql")]`
/// module and this test must run under the crate's unconditional SQL surface.
async fn publish_spans_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    records: &[SpanRecord],
) {
    let tenant_hash = tenant.hash();
    let identity = ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let mut w = RspanWriter::new(RspanConfig::default(), identity);
    for r in records {
        w.push(r.clone());
    }
    let bytes = w.finish().expect("finish object");
    let min = records
        .iter()
        .map(|r| r.start_ts_ns)
        .min()
        .expect("nonempty");
    let max = records.iter().map(|r| r.end_ts_ns).max().expect("nonempty");
    let content_hash = *blake3::hash(&bytes).as_bytes();
    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: 0,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: records.len() as u64,
        series_count: 0,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: 1,
        created_unix_ns: 0,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// A `SqlExecutor` over a fresh tenant holding `records`, built exactly as
/// `SqlExecutor::new` is built in `tests/bounded_topk_aggregate.rs`.
async fn executor_with_spans(records: &[SpanRecord]) -> SqlExecutor {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_spans_segment(store.as_ref(), &tenant(), records).await;
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
}

fn sql_request(sql: &str) -> ravel_sql::SqlRequest {
    ravel_sql::SqlRequest {
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

/// The physical plan `sql` produces under `executor`, for assertions that
/// inspect operators (e.g. downcasting to `SpansScanExec`) rather than rendered
/// text. Mirrors `tests/bounded_topk_aggregate.rs::physical_plan_tree`.
async fn spans_physical_plan(executor: &SqlExecutor, sql: &str) -> Arc<dyn ExecutionPlan> {
    let accounting = QueryAccounting::new();
    let declared = executor
        .resolve_declared_columns(tenant().hash(), sql_request(sql).now_ns)
        .await;
    let (snapshot, _) = executor
        .resolve_snapshot(tenant().hash(), &sql_request(sql), &accounting)
        .await
        .expect("snapshot resolves");
    let planned = executor
        .plan_pinned(tenant().hash(), snapshot, sql, &accounting, &declared)
        .await
        .expect("query plans");
    planned
        .create_physical_plan()
        .await
        .expect("physical plan builds")
}

/// Find the `SpansScanExec` leaf anywhere in `plan`'s tree. Trait-upcast to
/// `Any` before downcasting, sidestepping the `as_any` name shared by the
/// in-scope arrow `Array` trait (same reason
/// `trace_id_query_takes_the_cheap_trace_lookup` above does it).
fn find_spans_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<SpanQuery> {
    let any_ref: &dyn std::any::Any = plan.as_ref();
    if let Some(scan) = any_ref.downcast_ref::<SpansScanExec>() {
        return Some(scan.query());
    }
    for child in plan.children() {
        if let Some(q) = find_spans_scan(child) {
            return Some(q);
        }
    }
    None
}

/// Reduce `SELECT trace_id, span_id, start_ts, name FROM spans ...` output
/// batches to the set of rows they contain, in that exact column order.
/// Unlike `batches_to_rows` above, this does not assert the full public
/// `spans` schema, since these queries select a narrower column list.
fn query_rows(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> BTreeSet<([u8; 16], [u8; 8], i64, String)> {
    let mut out = BTreeSet::new();
    for batch in batches {
        let trace = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("trace_id col");
        let span = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("span_id col");
        let start = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("start_ts col");
        let name = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name col");
        for i in 0..batch.num_rows() {
            let tid: [u8; 16] = trace.value(i).try_into().expect("16-byte trace");
            let sid: [u8; 8] = span.value(i).try_into().expect("8-byte span");
            out.insert((tid, sid, start.value(i), name.value(i).to_string()));
        }
    }
    out
}

fn to_hex(bytes: [u8; 16]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Issue #1709: `trace_id = '<32-hex>'`, sent as SQL text through the real
/// `SqlExecutor`, plans and takes the `SpanQuery::trace` fast path exactly
/// like the already-working `X'<32-hex>'` binary-literal form. See
/// `crate::trace_id_planner` module docs for why the string form needs a
/// planner at all.
///
/// Reverting the `ctx.register_expr_planner(trace_id_hex_literal_planner())?;`
/// line in `session.rs` reproduces the pre-fix `type_coercion` failure.
#[tokio::test]
async fn trace_id_hex_string_literal_plans_and_takes_the_trace_fast_path() {
    let target = [0x11u8; 16];
    let other = [0x22u8; 16];
    let records = vec![
        span(target, 0, 100, 110, "root"),
        span(target, 1, 120, 130, "child"),
        span(other, 0, 200, 210, "unrelated"),
    ];
    let executor = executor_with_spans(&records).await;
    let hex = to_hex(target);

    let string_sql =
        format!("SELECT trace_id, span_id, start_ts, name FROM spans WHERE trace_id = '{hex}'");
    let binary_sql =
        format!("SELECT trace_id, span_id, start_ts, name FROM spans WHERE trace_id = X'{hex}'");

    // Both literal forms plan to the cheap trace lookup, not a full scan.
    for sql in [&string_sql, &binary_sql] {
        let plan = spans_physical_plan(&executor, sql).await;
        let issued = find_spans_scan(&plan).expect("plan contains a SpansScanExec");
        assert_eq!(
            issued.trace_id,
            Some(target),
            "trace_id = '{hex}' (via {sql}) must compile to a SpanQuery::trace lookup, not a ts_range scan"
        );
    }

    // Both forms return exactly the same rows through the real executor.
    let string_outcome = executor
        .execute(tenant().hash(), &sql_request(&string_sql))
        .await
        .expect("string-literal query executes");
    let binary_outcome = executor
        .execute(tenant().hash(), &sql_request(&binary_sql))
        .await
        .expect("binary-literal query executes");

    let string_rows = query_rows(string_outcome.output.batches());
    let binary_rows = query_rows(binary_outcome.output.batches());

    let mut want = BTreeSet::new();
    want.insert((target, [0u8; 8], 100i64, "root".to_string()));
    want.insert((target, [1u8; 8], 120i64, "child".to_string()));

    assert_eq!(
        string_rows, want,
        "trace_id = '<hex>' must return exactly the target trace's spans"
    );
    assert_eq!(
        string_rows, binary_rows,
        "the string and binary trace_id literal forms must return identical rows"
    );
}

/// Issue #1709: `is_trace_id_column` (`trace_id_planner.rs`) checks the
/// resolved schema type, `FixedSizeBinary(16)`, not just the column name
/// `trace_id`. A `Utf8` column aliased to `trace_id` compared against a
/// 32-hex string literal must still plan as an ordinary `Utf8 = Utf8`
/// comparison and return the matching row, untouched by
/// `TraceIdHexLiteralPlanner`.
///
/// Replacing the `matches!(schema.data_type(column), ...)` check in
/// `is_trace_id_column` with an unconditional `true` (name check only)
/// leaves the six other `trace_id` tests in this file passing, but breaks
/// this query: the planner rewrites the `Utf8` literal into a
/// `FixedSizeBinary(16)` scalar, the comparison against the `Utf8` `name`
/// column can never match, and this test fails at
/// `.expect("query plans and executes")` (`type_coercion` now has no path
/// from `Utf8` to `FixedSizeBinary(16)` on the LHS-is-Utf8 side) or, if it
/// still plans, at the `rows == 1` assertion below.
#[tokio::test]
async fn trace_id_hex_string_literal_against_non_trace_id_column_is_untouched() {
    let hex = "00112233445566778899aabbccddeeff".to_string();
    assert_eq!(hex.len(), 32);
    let records = vec![
        span([0x11u8; 16], 0, 100, 110, &hex),
        span([0x22u8; 16], 0, 200, 210, "unrelated"),
    ];
    let executor = executor_with_spans(&records).await;

    let sql = format!(
        "SELECT t.trace_id FROM (SELECT name AS trace_id FROM spans) t WHERE t.trace_id = '{hex}'"
    );

    let outcome = executor
        .execute(tenant().hash(), &sql_request(&sql))
        .await
        .expect("query plans and executes");

    let mut rows = 0usize;
    for batch in outcome.output.batches() {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("trace_id column stays Utf8, not rewritten to FixedSizeBinary");
        for i in 0..batch.num_rows() {
            assert_eq!(col.value(i), hex);
            rows += 1;
        }
    }
    assert_eq!(
        rows, 1,
        "exactly the one row whose name equals the hex string"
    );
}

/// Issue #1709: a 31-character `trace_id` literal (one hex digit short) is
/// not silently matched as a truncated or padded id. It fails a typed
/// `SqlError::Plan`, the same shape DataFusion's own `type_coercion` failure
/// takes, because `TraceIdHexLiteralPlanner` leaves a non-32-character
/// literal unplanned (`PlannerResult::Original`) and DataFusion's ordinary
/// coercion then has no path from `Utf8` to `FixedSizeBinary(16)`.
#[tokio::test]
async fn trace_id_literal_wrong_length_is_a_plan_error() {
    let records = vec![span([0x11u8; 16], 0, 100, 110, "root")];
    let executor = executor_with_spans(&records).await;
    let short_hex = "1".repeat(31);
    assert_eq!(short_hex.len(), 31);
    let sql = format!("SELECT trace_id FROM spans WHERE trace_id = '{short_hex}'");

    let err = executor
        .execute(tenant().hash(), &sql_request(&sql))
        .await
        .expect_err("a 31-character trace_id literal must not plan");
    assert!(
        matches!(err, ravel_sql::SqlError::Plan(_)),
        "expected a typed SqlError::Plan, got: {err:?}"
    );
}

/// Issue #1709: `trace_id != '<32-hex>'`, sent as SQL text through the real
/// `SqlExecutor`, is the `NotEq` sibling of
/// `trace_id_hex_string_literal_plans_and_takes_the_trace_fast_path` above.
/// `TraceIdHexLiteralPlanner::plan_binary_op` matches `BinaryOperator::NotEq`
/// as well as `Eq` (see the `not_eq` local in that function), so the string
/// and `X'..'` byte-literal forms of the same `!=` comparison must record the
/// identical `SpanQuery` and return the identical rows -- the non-matching
/// trace's spans, and nothing from the target trace.
///
/// Dropping the `BinaryOperator::NotEq => true,` arm in
/// `trace_id_planner.rs`'s `plan_binary_op` (falling through to the `_ =>`
/// wildcard) reproduces the pre-fix `type_coercion` failure for the string
/// form only, since the byte-literal form never needed this planner: the
/// string-literal query then fails to plan while the byte-literal query still
/// succeeds, so `string_outcome` is an `Err` and this test fails at the
/// `.expect("string-literal query executes")` call.
#[tokio::test]
async fn trace_id_not_eq_hex_string_literal_matches_the_byte_literal_form() {
    let target = [0x11u8; 16];
    let other = [0x22u8; 16];
    let records = vec![
        span(target, 0, 100, 110, "root"),
        span(target, 1, 120, 130, "child"),
        span(other, 0, 200, 210, "unrelated"),
    ];
    let executor = executor_with_spans(&records).await;
    let hex = to_hex(target);

    let string_sql =
        format!("SELECT trace_id, span_id, start_ts, name FROM spans WHERE trace_id != '{hex}'");
    let binary_sql =
        format!("SELECT trace_id, span_id, start_ts, name FROM spans WHERE trace_id != X'{hex}'");

    // Both literal forms of the same `!=` comparison record the identical
    // SpanQuery, the NotEq sibling of the Eq case's `issued.trace_id ==
    // Some(target)` check for both forms.
    let string_plan = spans_physical_plan(&executor, &string_sql).await;
    let string_issued = find_spans_scan(&string_plan).expect("plan contains a SpansScanExec");
    let binary_plan = spans_physical_plan(&executor, &binary_sql).await;
    let binary_issued = find_spans_scan(&binary_plan).expect("plan contains a SpansScanExec");
    assert_eq!(
        string_issued, binary_issued,
        "trace_id != '{hex}' and trace_id != X'{hex}' must record the same SpanQuery"
    );

    // Both forms return exactly the same rows through the real executor: the
    // non-matching trace's spans.
    let string_outcome = executor
        .execute(tenant().hash(), &sql_request(&string_sql))
        .await
        .expect("string-literal query executes");
    let binary_outcome = executor
        .execute(tenant().hash(), &sql_request(&binary_sql))
        .await
        .expect("binary-literal query executes");

    let string_rows = query_rows(string_outcome.output.batches());
    let binary_rows = query_rows(binary_outcome.output.batches());

    let mut want = BTreeSet::new();
    want.insert((other, [0u8; 8], 200i64, "unrelated".to_string()));

    assert_eq!(
        string_rows, want,
        "trace_id != '<hex>' must return exactly the non-matching trace's spans"
    );
    assert_eq!(
        string_rows, binary_rows,
        "the string and binary trace_id NotEq literal forms must return identical rows"
    );
}

/// Issue #1709: the reversed operand order, `'<32-hex>' = trace_id`, takes the
/// same `SpanQuery::trace` fast path as the column-first form. This is the
/// `else if is_trace_id_column(&right, schema) && ...` arm of
/// `TraceIdHexLiteralPlanner::plan_binary_op` in `trace_id_planner.rs`.
///
/// Dropping that arm (so only the column-first `if is_trace_id_column(&left,
/// schema)` branch remains) leaves the reversed-operand comparison
/// unplanned: DataFusion's `type_coercion` then has no path from `Utf8` to
/// `FixedSizeBinary(16)` and the query fails to plan at all, so this test
/// fails at `spans_physical_plan`'s `.expect("query plans")` call instead of
/// reaching the `SpanQuery` assertion below.
#[tokio::test]
async fn trace_id_hex_string_literal_reversed_operand_order_takes_the_trace_fast_path() {
    let target = [0x11u8; 16];
    let other = [0x22u8; 16];
    let records = vec![
        span(target, 0, 100, 110, "root"),
        span(other, 0, 200, 210, "unrelated"),
    ];
    let executor = executor_with_spans(&records).await;
    let hex = to_hex(target);

    let sql =
        format!("SELECT trace_id, span_id, start_ts, name FROM spans WHERE '{hex}' = trace_id");
    let plan = spans_physical_plan(&executor, &sql).await;
    let issued = find_spans_scan(&plan).expect("plan contains a SpansScanExec");
    assert_eq!(
        issued.trace_id,
        Some(target),
        "'{hex}' = trace_id must compile to a SpanQuery::trace lookup, not a ts_range scan"
    );

    let outcome = executor
        .execute(tenant().hash(), &sql_request(&sql))
        .await
        .expect("reversed-operand query executes");
    let rows = query_rows(outcome.output.batches());
    let mut want = BTreeSet::new();
    want.insert((target, [0u8; 8], 100i64, "root".to_string()));
    assert_eq!(
        rows, want,
        "'<hex>' = trace_id must return exactly the target trace's spans"
    );
}

/// Issue #1709: a 32-character `trace_id` literal that contains one non-hex
/// character must not plan, the sibling of
/// `trace_id_literal_wrong_length_is_a_plan_error` above which pins only the
/// length half of the validation in `hex_16` (`spans_pushdown.rs`). A bad
/// nibble must fail the same way a bad length does: DataFusion's ordinary
/// `type_coercion` error, never a silent match and never rows.
///
/// Loosening `hex_16`'s per-nibble validation to accept any byte (`.unwrap_or(0)`
/// in place of the `?` on `hex_nibble`'s result) makes the literal plan
/// successfully -- with the invalid nibble decoded as zero -- so the query
/// executes instead of failing, and this test fails at
/// `.expect_err("a 32-character literal containing a non-hex digit must not
/// plan")`.
#[tokio::test]
async fn trace_id_literal_non_hex_character_is_a_plan_error() {
    let records = vec![span([0x11u8; 16], 0, 100, 110, "root")];
    let executor = executor_with_spans(&records).await;
    let mut bad_hex = "1".repeat(31);
    bad_hex.push('g');
    assert_eq!(bad_hex.len(), 32);
    let sql = format!("SELECT trace_id FROM spans WHERE trace_id = '{bad_hex}'");

    let err = executor
        .execute(tenant().hash(), &sql_request(&sql))
        .await
        .expect_err("a 32-character literal containing a non-hex digit must not plan");
    assert!(
        matches!(err, ravel_sql::SqlError::Plan(_)),
        "expected a typed SqlError::Plan, got: {err:?}"
    );
}

/// One `opentelemetry.proto.trace.v1.Span.Event` with string-valued
/// attributes, the shape an OTel SDK sends for a recorded exception.
fn otlp_event(ts_ns: u64, name: &str, attrs: &[(&str, &str)]) -> SpanEventProto {
    SpanEventProto {
        time_unix_nano: ts_ns,
        name: name.to_string(),
        attributes: attrs
            .iter()
            .map(|(k, v)| KeyValue {
                key: (*k).to_string(),
                value: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue((*v).to_string())),
                }),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// The `_events_raw` attribute value for `events`, built exactly as
/// `ravel_otlp::traces_normalize::encode_blob` builds it at ingest:
/// length-delimited protobuf messages concatenated, then hex-encoded.
fn events_raw(events: &[SpanEventProto]) -> String {
    let mut raw = Vec::new();
    for e in events {
        e.encode_length_delimited(&mut raw).expect("encode event");
    }
    raw.iter().map(|b| format!("{b:02x}")).collect()
}

/// A span carrying `events` as its `_events_raw` attribute. Attribute keys stay
/// strictly ascending (`_events_raw` < `svc`), which is what the RSPAN attrs
/// codec requires.
fn span_with_events(
    trace: [u8; 16],
    span_id: u8,
    start: i64,
    name: &str,
    events: &[SpanEventProto],
) -> SpanRecord {
    let mut record = span(trace, span_id, start, start + 10, name);
    let mut attrs = vec![("svc".to_string(), "api".to_string())];
    if !events.is_empty() {
        attrs.insert(0, (EVENTS_RAW_KEY.to_string(), events_raw(events)));
    }
    record.attrs = attrs;
    record
}

/// Issue #1710 part A acceptance test: the `spans` table exposes span events as
/// a structured `events` column, and a query can filter on an event's own
/// attributes.
///
/// Both halves matter and are asserted separately:
///
/// - `SELECT events FROM spans` returns the decoded structure, not a hex blob:
///   two events on the first span (an exception with two attributes and a log
///   with one), one on the second, and NULL (not an empty list) on the third,
///   which carried no events at all. The assertion goes through
///   `QueryOutput::to_json`, the encoder the HTTP `POST /api/v1/sql` surface
///   uses, so it proves the nested `List(Struct{..., Map})` type serializes
///   there rather than only inside arrow.
/// - `unnest(events)` plus a `WHERE` over an event attribute filters exactly.
///   Three event rows exist in total; the exception filter keeps two, and the
///   `exception.type = 'ValueError'` filter keeps exactly one. Counting rows
///   rather than asserting non-emptiness is the point: an `unnest` that
///   silently produced one row per span, or a subscript that matched every
///   event, would still be "> 0".
///
/// The fixture's `_events_raw` value is built from real
/// `opentelemetry.proto.trace.v1.Span.Event` messages encoded the way ingest
/// encodes them, and is then written and read back through the real RSPAN v4
/// writer, commit record, and `SqlExecutor`, so the column is proven against
/// the v4 event columns rather than against an in-memory fixture.
///
/// Against the pre-fix code this test cannot even plan: `spans` had no `events`
/// column. Against the post-fix code, flipping
/// `spans_scan.rs::columnar_static_eligible`'s new `!p.contains(&SPAN_COL_EVENTS)`
/// clause back off sends a `SELECT events` projection down the columnar fast
/// path, where `columnar_column` has no arm for it, and the query fails with
/// "spans columnar column index 11 not supported on the fast path".
#[tokio::test]
async fn events_column_returns_structured_exception_event_and_filters_on_its_attrs() {
    let t1 = [0x11u8; 16];
    let t2 = [0x22u8; 16];
    let records = vec![
        span_with_events(
            t1,
            0,
            100,
            "root",
            &[
                otlp_event(
                    150,
                    "exception",
                    &[
                        ("exception.message", "boom"),
                        ("exception.type", "ValueError"),
                    ],
                ),
                otlp_event(160, "log", &[("level", "warn")]),
            ],
        ),
        span_with_events(
            t1,
            1,
            200,
            "child",
            &[otlp_event(
                210,
                "exception",
                &[("exception.type", "KeyError")],
            )],
        ),
        span_with_events(t2, 0, 300, "unrelated", &[]),
    ];
    let executor = executor_with_spans(&records).await;

    // Half one: the structured column itself, through the JSON encoder the
    // HTTP surface uses. Ordering is the scan's advertised (trace_id, start_ts).
    let sql = "SELECT name, events FROM spans ORDER BY name";
    let outcome = executor
        .execute(tenant().hash(), &sql_request(sql))
        .await
        .expect("SELECT events executes");

    let batches = outcome.output.batches();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3, "three spans, three rows");

    // The column's arrow type is the declared one, not something a builder
    // improvised: List(Struct{ts_unix_nano, name, attrs}) with the shared
    // label map type for event attributes.
    let events_field = batches[0].schema().field(1).clone();
    assert_eq!(events_field.name(), "events");
    assert_eq!(
        events_field.data_type(),
        ravel_sql::spans_schema()
            .field(ravel_sql::SPAN_COL_EVENTS)
            .data_type(),
        "the projected events column keeps the declared table type"
    );

    let json = outcome.output.to_json().expect("events encode to JSON");
    assert_eq!(
        json["columns"],
        serde_json::json!([
            {"name": "name", "type": "Utf8"},
            {"name": "events", "type": events_field.data_type().to_string()},
        ])
    );
    assert_eq!(
        json["rows"],
        serde_json::json!([
            [
                "child",
                [{
                    "ts_unix_nano": 210,
                    "name": "exception",
                    "attrs": {"exception.type": "KeyError"},
                }],
            ],
            [
                "root",
                [
                    {
                        "ts_unix_nano": 150,
                        "name": "exception",
                        "attrs": {
                            "exception.message": "boom",
                            "exception.type": "ValueError",
                        },
                    },
                    {
                        "ts_unix_nano": 160,
                        "name": "log",
                        "attrs": {"level": "warn"},
                    },
                ],
            ],
            ["unrelated", serde_json::Value::Null],
        ]),
        "events must decode into ts/name/attrs structs, and a span with no \
         events must be NULL rather than an empty list"
    );

    // The raw attribute is still there: this column is a lossy projection of
    // it, never a replacement (spans_schema.rs module doc).
    let raw = executor
        .execute(
            tenant().hash(),
            &sql_request("SELECT count(*) FROM spans WHERE attrs['_events_raw'] IS NOT NULL"),
        )
        .await
        .expect("attrs['_events_raw'] still queryable");
    assert_eq!(scalar_count(raw.output.batches()), 2);

    // Half two: unnest plus a predicate over an event attribute.
    for (sql, want, what) in [
        (
            "SELECT count(*) FROM (SELECT unnest(events) AS e FROM spans)",
            3i64,
            "three events across all spans",
        ),
        (
            "SELECT count(*) FROM (SELECT unnest(events) AS e FROM spans) \
             WHERE e['name'] = 'exception'",
            2,
            "two of the three events are exceptions",
        ),
        (
            "SELECT count(*) FROM (SELECT unnest(events) AS e FROM spans) \
             WHERE e['attrs']['exception.type'] = 'ValueError'",
            1,
            "exactly one event carries exception.type = ValueError",
        ),
        (
            "SELECT count(*) FROM (SELECT unnest(events) AS e FROM spans) \
             WHERE e['attrs']['exception.type'] = 'NoSuchError'",
            0,
            "a non-matching event attribute value keeps nothing",
        ),
    ] {
        let outcome = executor
            .execute(tenant().hash(), &sql_request(sql))
            .await
            .unwrap_or_else(|e| panic!("{sql} must execute: {e:?}"));
        assert_eq!(
            scalar_count(outcome.output.batches()),
            want,
            "{what} ({sql})"
        );
    }

    // The shape docs/guides/traces.md shows: unnest beside ordinary columns,
    // projecting a struct field and a nested map key.
    let documented = "SELECT trace_id, span_id, e['name'] AS event_name, \
                      e['attrs']['exception.type'] AS exception_type \
                      FROM (SELECT trace_id, span_id, unnest(events) AS e FROM spans) \
                      WHERE e['name'] = 'exception'";
    let outcome = executor
        .execute(tenant().hash(), &sql_request(documented))
        .await
        .unwrap_or_else(|e| panic!("the documented unnest query must execute: {e:?}"));
    let json = outcome
        .output
        .to_json()
        .expect("the documented query encodes to JSON");
    assert_eq!(
        json["columns"],
        serde_json::json!([
            {"name": "trace_id", "type": "FixedSizeBinary(16)"},
            {"name": "span_id", "type": "FixedSizeBinary(8)"},
            {"name": "event_name", "type": "Utf8"},
            {"name": "exception_type", "type": "Utf8"},
        ]),
        "the documented query names its projected columns"
    );
    assert_eq!(
        json["rows"],
        serde_json::json!([
            [
                "11111111111111111111111111111111",
                "0000000000000000",
                "exception",
                "ValueError"
            ],
            [
                "11111111111111111111111111111111",
                "0101010101010101",
                "exception",
                "KeyError"
            ],
        ]),
        "one row per exception event, with the span it came from"
    );
}

/// One `opentelemetry.proto.trace.v1.Span.Link` with string-valued attributes,
/// the shape an OTel SDK sends for a batching or fan-in span link.
fn otlp_link(trace_id: [u8; 16], span_id: [u8; 8], trace_state: &str, attrs: &[(&str, &str)]) -> SpanLinkProto {
    SpanLinkProto {
        trace_id: trace_id.to_vec(),
        span_id: span_id.to_vec(),
        trace_state: trace_state.to_string(),
        attributes: attrs
            .iter()
            .map(|(k, v)| KeyValue {
                key: (*k).to_string(),
                value: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue((*v).to_string())),
                }),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

/// The `_links_raw` attribute value for `links`, built exactly as
/// `ravel_otlp::traces_normalize::encode_blob` builds it at ingest:
/// length-delimited protobuf messages concatenated, then hex-encoded.
fn links_raw(links: &[SpanLinkProto]) -> String {
    let mut raw = Vec::new();
    for l in links {
        l.encode_length_delimited(&mut raw).expect("encode link");
    }
    raw.iter().map(|b| format!("{b:02x}")).collect()
}

/// A span carrying `links` as its `_links_raw` attribute. Attribute keys stay
/// strictly ascending (`_links_raw` < `svc`), which is what the RSPAN attrs
/// codec requires.
fn span_with_links(
    trace: [u8; 16],
    span_id: u8,
    start: i64,
    name: &str,
    links: &[SpanLinkProto],
) -> SpanRecord {
    let mut record = span(trace, span_id, start, start + 10, name);
    let mut attrs = vec![("svc".to_string(), "api".to_string())];
    if !links.is_empty() {
        attrs.insert(0, (LINKS_RAW_KEY.to_string(), links_raw(links)));
    }
    record.attrs = attrs;
    record
}

/// Issue #1710 part B acceptance test: the `spans` table exposes span links as
/// a structured `links` column, and a query can filter on a link's own
/// attributes.
///
/// Both halves matter and are asserted separately:
///
/// - `SELECT links FROM spans` returns the decoded structure, not a hex blob:
///   two links on the first span (distinct trace ids, span ids, trace states,
///   and a link attribute each), and NULL (not an empty list) on the second
///   span, which carried no links at all. The assertion goes through
///   `QueryOutput::to_json`, same as the `events` acceptance test, so it
///   proves the nested `List(Struct{..., FixedSizeBinary, FixedSizeBinary,
///   Utf8, Map})` type serializes on the HTTP surface too.
/// - `unnest(links)` plus a `WHERE` over a link attribute filters exactly.
///
/// Against the pre-fix code this test cannot even plan: `spans` had no
/// `links` column.
#[tokio::test]
async fn links_column_returns_structured_links_and_filters_on_their_attrs() {
    let t1 = [0x11u8; 16];
    let t2 = [0x22u8; 16];
    let linked_a = [0xaau8; 16];
    let linked_b = [0xbbu8; 16];
    let records = vec![
        span_with_links(
            t1,
            0,
            100,
            "root",
            &[
                otlp_link(
                    linked_a,
                    [0xaau8; 8],
                    "congo=1",
                    &[("link.attr", "first")],
                ),
                otlp_link(linked_b, [0xbbu8; 8], "", &[("link.attr", "second")]),
            ],
        ),
        span_with_links(t2, 0, 300, "unrelated", &[]),
    ];
    let executor = executor_with_spans(&records).await;

    // Half one: the structured column itself, through the JSON encoder the
    // HTTP surface uses. Ordering is the scan's advertised (trace_id, start_ts).
    let sql = "SELECT name, links FROM spans ORDER BY name";
    let outcome = executor
        .execute(tenant().hash(), &sql_request(sql))
        .await
        .expect("SELECT links executes");

    let batches = outcome.output.batches();
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 2, "two spans, two rows");

    let links_field = batches[0].schema().field(1).clone();
    assert_eq!(links_field.name(), "links");
    assert_eq!(
        links_field.data_type(),
        ravel_sql::spans_schema()
            .field(ravel_sql::SPAN_COL_LINKS)
            .data_type(),
        "the projected links column keeps the declared table type"
    );

    let linked_a_hex: String = linked_a.iter().map(|b| format!("{b:02x}")).collect();
    let linked_b_hex: String = linked_b.iter().map(|b| format!("{b:02x}")).collect();
    let json = outcome.output.to_json().expect("links encode to JSON");
    assert_eq!(
        json["columns"],
        serde_json::json!([
            {"name": "name", "type": "Utf8"},
            {"name": "links", "type": links_field.data_type().to_string()},
        ])
    );
    assert_eq!(
        json["rows"],
        serde_json::json!([
            [
                "root",
                [
                    {
                        "trace_id": linked_a_hex,
                        "span_id": "aaaaaaaaaaaaaaaa",
                        "trace_state": "congo=1",
                        "attrs": {"link.attr": "first"},
                    },
                    {
                        "trace_id": linked_b_hex,
                        "span_id": "bbbbbbbbbbbbbbbb",
                        "trace_state": "",
                        "attrs": {"link.attr": "second"},
                    },
                ],
            ],
            ["unrelated", serde_json::Value::Null],
        ]),
        "links must decode field by field, and a span with no links must be \
         NULL rather than an empty list"
    );

    // The raw attribute is still there: this column is a lossy projection of
    // it, never a replacement.
    let raw = executor
        .execute(
            tenant().hash(),
            &sql_request("SELECT count(*) FROM spans WHERE attrs['_links_raw'] IS NOT NULL"),
        )
        .await
        .expect("attrs['_links_raw'] still queryable");
    assert_eq!(scalar_count(raw.output.batches()), 1);

    // Half two: unnest plus a predicate over a link attribute.
    for (sql, want, what) in [
        (
            "SELECT count(*) FROM (SELECT unnest(links) AS l FROM spans)",
            2i64,
            "two links across all spans",
        ),
        (
            "SELECT count(*) FROM (SELECT unnest(links) AS l FROM spans) \
             WHERE l['attrs']['link.attr'] = 'first'",
            1,
            "exactly one link carries link.attr = first",
        ),
        (
            "SELECT count(*) FROM (SELECT unnest(links) AS l FROM spans) \
             WHERE l['attrs']['link.attr'] = 'nonexistent'",
            0,
            "a non-matching link attribute value keeps nothing",
        ),
    ] {
        let outcome = executor
            .execute(tenant().hash(), &sql_request(sql))
            .await
            .unwrap_or_else(|e| panic!("{sql} must execute: {e:?}"));
        assert_eq!(
            scalar_count(outcome.output.batches()),
            want,
            "{what} ({sql})"
        );
    }
}

/// Issue #1710: a malformed `_links_raw` value (garbage bytes, not a valid
/// length-delimited protobuf framing) must not panic, and must fall back to
/// the documented NULL, exactly like a span with no links at all. The raw
/// attribute itself is untouched, so the caller can still see it.
#[tokio::test]
async fn links_column_treats_unparseable_links_raw_as_null() {
    let t1 = [0x11u8; 16];
    let mut record = span(t1, 0, 100, 110, "garbage-links");
    record.attrs = vec![
        (LINKS_RAW_KEY.to_string(), "not-hex-and-not-a-link".to_string()),
        ("svc".to_string(), "api".to_string()),
    ];
    let executor = executor_with_spans(&[record]).await;

    let outcome = executor
        .execute(tenant().hash(), &sql_request("SELECT links FROM spans"))
        .await
        .expect("query over a garbage _links_raw must not panic or error");
    let json = outcome.output.to_json().expect("encodes to JSON");
    assert_eq!(
        json["rows"],
        serde_json::json!([[serde_json::Value::Null]]),
        "an unparseable _links_raw value falls back to NULL, not an error or a panic"
    );

    let raw = executor
        .execute(
            tenant().hash(),
            &sql_request("SELECT attrs['_links_raw'] FROM spans"),
        )
        .await
        .expect("the raw attribute is still queryable");
    let raw_json = raw.output.to_json().expect("encodes to JSON");
    assert_eq!(
        raw_json["rows"],
        serde_json::json!([["not-hex-and-not-a-link"]]),
        "the raw attribute value is untouched"
    );
}

/// The single `count(*)` value in `batches`, which must hold exactly one row.
fn scalar_count(batches: &[datafusion::arrow::record_batch::RecordBatch]) -> i64 {
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 1, "a count(*) query returns exactly one row");
    let batch = batches
        .iter()
        .find(|b| b.num_rows() == 1)
        .expect("the one non-empty batch");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("count(*) is Int64")
        .value(0)
}

/// Reachability (ADR-0110 decisions 3-5): a `SELECT` over the `spans` table,
/// driven through the real Flight SQL surface end to end (`GetFlightInfo` then
/// `DoGet`), takes the columnar fast path.
///
/// The other tests in this file drive `SpansScanExec`/`SpansTableProvider`
/// directly, which proves the code works; this proves a real caller on the
/// shipping surface reaches it. The Flight round trip returns only record
/// batches, not the scan's plan metrics, so "the columnar path ran" is asserted
/// through the query's cost accounting instead: the columnar exit records
/// `page_bytes_decoded` strictly below `page_bytes_fetched` whenever it skips a
/// page (ADR-0110 decision 2), which the row path never does (it decodes every
/// page, so the two are equal). A custom `QueryCostRecorder` captures the
/// per-query snapshot the Flight service folds on stream end.
///
/// The fixture spans carry `service.name`, an extra dynamic attribute, and an
/// event, so the attrs-excluding projection has attribute and event pages to
/// skip; without them both paths decode the same pages and the metric proof is
/// vacuous. Forcing the row path (making `columnar_static_eligible` return
/// `false`, or projecting `attrs`) makes `page_bytes_decoded == page_bytes_fetched`
/// and turns the metric assertion red.
#[cfg(feature = "flight-sql")]
mod flight_reachability {
    use std::sync::{Arc, Mutex};

    use datafusion::arrow::array::{
        Array, FixedSizeBinaryArray, Int64Array, ListArray, MapArray, StringArray, StructArray,
    };
    use ravel_commit::keys;
    use ravel_commit::publish::{self, RetryPolicy};
    use ravel_commit::record::{self, NewCommitRecord};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{ObjectStoreBackend, PutOptions};
    use ravel_rspan::{ObjectIdentity, RspanConfig, RspanWriter, SpanRecord, StatusCode};
    use ravel_types::Signal;
    use ravel_types::accounting::{
        CostEstimate, QueryAccountingSnapshot, QueryCostRecorder, QueryWorkloadClass,
    };
    use ravel_types::{TenantHash, TenantId};
    use uuid::Uuid;

    use crate::util::SegSpec;
    use crate::util::flight_harness::Harness;
    use crate::util::tenant_id;

    const QUERY: &str =
        "SELECT trace_id, name, start_ts, duration_ns FROM spans ORDER BY trace_id, start_ts";

    /// A `QueryCostRecorder` that keeps every recorded snapshot, so the test can
    /// read the execution snapshot the `DoGet` stream folds on end.
    #[derive(Default)]
    struct CapturingRecorder {
        snapshots: Mutex<Vec<QueryAccountingSnapshot>>,
    }

    impl QueryCostRecorder for CapturingRecorder {
        fn record(
            &self,
            accounting: &QueryAccountingSnapshot,
            _estimate: &CostEstimate,
            _tenant_hash: TenantHash,
            _workload_class: QueryWorkloadClass,
        ) {
            self.snapshots
                .lock()
                .expect("recorder lock")
                .push(*accounting);
        }
    }

    /// Length-delimited, hex-encoded `_events_raw` value carrying one event, the
    /// grammar `ravel_rspan::record::parse_events` promotes into event columns,
    /// so the block has an event page the attrs-excluding decode skips.
    fn one_event_raw() -> String {
        // uvarint length prefix (3) then a small verbatim payload.
        let raw = [0x03u8, 0x08, 0x02, 0x01];
        raw.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn span(trace: u8, span_id: u8, start: i64, name: &str) -> SpanRecord {
        SpanRecord {
            trace_id: [trace; 16],
            span_id: [span_id; 8],
            parent_span_id: None,
            name: name.to_string(),
            start_ts_ns: start,
            end_ts_ns: start + 50,
            status_code: StatusCode::Ok,
            status_message: None,
            attrs: vec![
                ("service.name".to_string(), "checkout".to_string()),
                ("http.method".to_string(), "GET".to_string()),
                ("_events_raw".to_string(), one_event_raw()),
            ],
        }
    }

    /// Publish one RSPAN object as a real `Signal::Spans` commit record, so the
    /// executor's `Catalog::resolve` finds it exactly as a production caller
    /// would (no hand-built `Snapshot`).
    async fn publish_spans_segment(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantId,
        records: &[SpanRecord],
    ) {
        let tenant_hash = tenant.hash();
        let identity = ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        };
        let mut w = RspanWriter::new(RspanConfig::default(), identity);
        for r in records {
            w.push(r.clone());
        }
        let bytes = w.finish().expect("finish object");
        let min = records
            .iter()
            .map(|r| r.start_ts_ns)
            .min()
            .expect("nonempty");
        let max = records.iter().map(|r| r.end_ts_ns).max().expect("nonempty");
        let content_hash = *blake3::hash(&bytes).as_bytes();
        let new_record = NewCommitRecord {
            tenant_hash,
            signal: Signal::Spans,
            shard: 0,
            writer_id: Uuid::from_u128(1),
            writer_epoch: 1,
            writer_seq: 1,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: records.len() as u64,
            series_count: 0,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            min_ingest_ts_ns: min,
            max_ingest_ts_ns: max,
            segment_format_version: 1,
            created_unix_ns: 0,
            ingest_hour_bucket: 0,
        };
        let rec = record::build(new_record).expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    #[tokio::test]
    async fn flight_sql_spans_select_takes_the_columnar_path() {
        let tenant = tenant_id("acme");
        let recorder = Arc::new(CapturingRecorder::default());
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        // No metrics segments; the spans segment is published straight to the
        // store the harness's catalog resolves against.
        let no_specs: &[SegSpec] = &[];
        let harness = Harness::build_with_recorder(
            Arc::clone(&store),
            &[(&tenant, no_specs)],
            recorder.clone() as Arc<dyn QueryCostRecorder>,
        )
        .await;

        // Two traces, two spans each, all carrying attributes and an event.
        let records = vec![
            span(0, 0, 100, "a-root"),
            span(0, 1, 150, "a-child"),
            span(1, 0, 200, "b-root"),
            span(1, 1, 250, "b-child"),
        ];
        publish_spans_segment(harness.store.as_ref(), &tenant, &records).await;

        let ticket = harness
            .get_flight_info("acme", QUERY)
            .await
            .expect("flight info");
        let batches = harness.do_get("acme", &ticket).await.expect("do get");

        // The rows are correct: every span comes back, projected to
        // (trace_id, name, start_ts, duration_ns) in (trace_id, start_ts) order.
        let mut names = Vec::new();
        let mut trace_ids = Vec::new();
        for batch in &batches {
            assert_eq!(batch.num_columns(), 4, "projected to four columns");
            let trace = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("trace_id column");
            let name = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("name column");
            let duration = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("duration_ns column");
            for i in 0..batch.num_rows() {
                let tid: [u8; 16] = trace.value(i).try_into().expect("16-byte trace");
                trace_ids.push(tid);
                names.push(name.value(i).to_string());
                assert_eq!(duration.value(i), 50, "duration_ns is end - start");
            }
        }
        assert_eq!(
            names,
            vec![
                "a-root".to_string(),
                "a-child".to_string(),
                "b-root".to_string(),
                "b-child".to_string(),
            ],
            "every span is returned once, in (trace_id, start_ts) order"
        );
        assert_eq!(
            trace_ids,
            vec![[0u8; 16], [0u8; 16], [1u8; 16], [1u8; 16]],
            "rows are emitted in trace_id ascending order"
        );

        // The columnar path ran: the execution snapshot decoded strictly fewer
        // page bytes than it fetched, because the attrs-excluding projection
        // skipped the attribute and event pages. The row path decodes every
        // page, so the two would be equal. `get_flight_info` also records a
        // planning snapshot with zero page bytes; the execution snapshot is the
        // one with page bytes fetched, so pick the maximum.
        let snapshots = recorder.snapshots.lock().expect("recorder lock");
        let execution = snapshots
            .iter()
            .max_by_key(|s| s.page_bytes_fetched)
            .expect("at least one recorded query");
        assert!(
            execution.page_bytes_fetched > 0,
            "the spans scan must have decoded at least one block"
        );
        assert!(
            execution.page_bytes_decoded < execution.page_bytes_fetched,
            "the columnar fast path must skip pages: decoded {} of {} fetched",
            execution.page_bytes_decoded,
            execution.page_bytes_fetched,
        );
    }

    /// The Flight encoder carries the nested `events` type end to end. Arrow IPC
    /// encodes a `List(Struct{..., Map})` with no per-type handling in Ravel, so
    /// this pins that claim on the shipping surface rather than assuming it: the
    /// batch that comes back over `DoGet` must carry the declared column type
    /// and the decoded event, and a span with no events must arrive null.
    #[tokio::test]
    async fn flight_sql_serializes_the_nested_events_column() {
        let tenant = tenant_id("acme");
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let no_specs: &[SegSpec] = &[];
        let harness = Harness::build(Arc::clone(&store), &[(&tenant, no_specs)]).await;

        let with_event = super::span_with_events(
            [0u8; 16],
            0,
            100,
            "a-root",
            &[super::otlp_event(
                1_700_000_000_000_000_000,
                "exception",
                &[("exception.type", "ValueError")],
            )],
        );
        let without_event = super::span_with_events([1u8; 16], 0, 200, "b-root", &[]);
        publish_spans_segment(
            harness.store.as_ref(),
            &tenant,
            &[with_event, without_event],
        )
        .await;

        let ticket = harness
            .get_flight_info(
                "acme",
                "SELECT trace_id, events FROM spans ORDER BY trace_id",
            )
            .await
            .expect("flight info");
        let batches = harness.do_get("acme", &ticket).await.expect("do get");

        /// One row's decoded `events` value: null, or the single event's
        /// timestamp, name, and attribute pairs.
        type DecodedEvent = Option<(i64, String, Vec<(String, String)>)>;

        let mut rows: Vec<DecodedEvent> = Vec::new();
        for batch in &batches {
            assert_eq!(
                batch.schema().field(1).data_type(),
                ravel_sql::spans_schema()
                    .field(ravel_sql::SPAN_COL_EVENTS)
                    .data_type(),
                "the Flight stream carries the declared events type"
            );
            let events = batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("events column is a list");
            for i in 0..batch.num_rows() {
                if events.is_null(i) {
                    rows.push(None);
                    continue;
                }
                let items = events.value(i);
                let items = items
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("event items are structs");
                assert_eq!(items.len(), 1, "the fixture span carries one event");
                let ts = items
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("event ts is i64")
                    .value(0);
                let name = items
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("event name is Utf8")
                    .value(0)
                    .to_string();
                let attrs = items
                    .column(2)
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .expect("event attrs is a map");
                let entries = attrs.value(0);
                let keys = entries
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("map keys are Utf8");
                let values = entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("map values are Utf8");
                let pairs = (0..entries.len())
                    .map(|k| (keys.value(k).to_string(), values.value(k).to_string()))
                    .collect();
                rows.push(Some((ts, name, pairs)));
            }
        }

        assert_eq!(
            rows,
            vec![
                Some((
                    1_700_000_000_000_000_000,
                    "exception".to_string(),
                    vec![("exception.type".to_string(), "ValueError".to_string())],
                )),
                None,
            ],
            "the decoded event survives the Flight round trip, and an event-free span is null"
        );
    }

    /// The Flight encoder carries the nested `links` type end to end, the
    /// links sibling of `flight_sql_serializes_the_nested_events_column`
    /// above: the batch that comes back over `DoGet` must carry the declared
    /// column type and the decoded link, and a span with no links must arrive
    /// null.
    #[tokio::test]
    async fn flight_sql_serializes_the_nested_links_column() {
        let tenant = tenant_id("acme");
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let no_specs: &[SegSpec] = &[];
        let harness = Harness::build(Arc::clone(&store), &[(&tenant, no_specs)]).await;

        let linked = [0xaau8; 16];
        let with_link = super::span_with_links(
            [0u8; 16],
            0,
            100,
            "a-root",
            &[super::otlp_link(
                linked,
                [0xaau8; 8],
                "congo=1",
                &[("link.attr", "first")],
            )],
        );
        let without_link = super::span_with_links([1u8; 16], 0, 200, "b-root", &[]);
        publish_spans_segment(harness.store.as_ref(), &tenant, &[with_link, without_link]).await;

        let ticket = harness
            .get_flight_info(
                "acme",
                "SELECT trace_id, links FROM spans ORDER BY trace_id",
            )
            .await
            .expect("flight info");
        let batches = harness.do_get("acme", &ticket).await.expect("do get");

        /// One row's decoded `links` value: null, or the single link's trace
        /// id, span id, trace state, and attribute pairs.
        type DecodedLink = Option<([u8; 16], [u8; 8], String, Vec<(String, String)>)>;

        let mut rows: Vec<DecodedLink> = Vec::new();
        for batch in &batches {
            assert_eq!(
                batch.schema().field(1).data_type(),
                ravel_sql::spans_schema()
                    .field(ravel_sql::SPAN_COL_LINKS)
                    .data_type(),
                "the Flight stream carries the declared links type"
            );
            let links = batch
                .column(1)
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("links column is a list");
            for i in 0..batch.num_rows() {
                if links.is_null(i) {
                    rows.push(None);
                    continue;
                }
                let items = links.value(i);
                let items = items
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("link items are structs");
                assert_eq!(items.len(), 1, "the fixture span carries one link");
                let trace_id: [u8; 16] = items
                    .column(0)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("link trace_id is FixedSizeBinary(16)")
                    .value(0)
                    .try_into()
                    .expect("16-byte trace id");
                let span_id: [u8; 8] = items
                    .column(1)
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .expect("link span_id is FixedSizeBinary(8)")
                    .value(0)
                    .try_into()
                    .expect("8-byte span id");
                let trace_state = items
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("link trace_state is Utf8")
                    .value(0)
                    .to_string();
                let attrs = items
                    .column(3)
                    .as_any()
                    .downcast_ref::<MapArray>()
                    .expect("link attrs is a map");
                let entries = attrs.value(0);
                let keys = entries
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("map keys are Utf8");
                let values = entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("map values are Utf8");
                let pairs = (0..entries.len())
                    .map(|k| (keys.value(k).to_string(), values.value(k).to_string()))
                    .collect();
                rows.push(Some((trace_id, span_id, trace_state, pairs)));
            }
        }

        assert_eq!(
            rows,
            vec![
                Some((
                    linked,
                    [0xaau8; 8],
                    "congo=1".to_string(),
                    vec![("link.attr".to_string(), "first".to_string())],
                )),
                None,
            ],
            "the decoded link survives the Flight round trip, and a link-free span is null"
        );
    }
}
