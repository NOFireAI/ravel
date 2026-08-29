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

use datafusion::arrow::array::{
    Array, FixedSizeBinaryArray, StringArray, TimestampNanosecondArray,
};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::collect;
use datafusion::prelude::{col, lit};
use datafusion::scalar::ScalarValue;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_rspan::{
    ObjectIdentity, RspanConfig, RspanWriter, ScanStats, SpanQuery, SpanRecord, StatusCode,
};
use ravel_sql::{SpanSegmentFetcher, SpansScanExec, SpansTableProvider};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
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

    use datafusion::arrow::array::{Array, FixedSizeBinaryArray, Int64Array, StringArray};
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
}
