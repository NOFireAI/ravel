//! Acceptance gate for the columnar fast path of the `logs` SQL scan
//! (ADR-0099 decisions 2-3, issue #415).
//!
//! The fast path builds Arrow arrays straight from `ravel_logseg`'s
//! `ColumnarBlockView` over the surviving rows, with no `LogRecord` and no
//! `merged_attrs`, and is taken only when the query is eligible: the projection
//! touches only fixed and declared typed columns (no `attrs` map), no pending
//! erasure predicate applies, and the block carries no `attrs_raw` overflow
//! page. Otherwise the unchanged row path runs. Because the two paths' output is
//! identical by construction, the `columnar_batches`/`rowpath_batches` partition
//! metrics are the only way to prove which one ran.
//!
//! The tests drive [`ravel_sql::LogsScanExec`] directly rather than a full
//! `SessionContext`, so a projection is chosen exactly (no planner folding) and
//! the same input can be run through both paths for a byte-for-byte comparison:
//!
//! - the fast path is a scan with no pending erasure over a corpus with no
//!   `attrs_raw` spill;
//! - the row path is the *same* projection over the *same* corpus, forced
//!   ineligible by a pending erasure predicate that matches no record (so it
//!   drains the row path yet erases nothing, leaving the output identical).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray, UInt8Array, UInt32Array,
};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use proptest::prelude::*;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, FieldSel, LogRecord, Predicate, RlogConfig, RlogWriter, stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::LogSegmentFetcher;
use ravel_query::erasure::snapshot_pending_erasure_predicates;
use ravel_sql::{
    DeclaredColumn, DeclaredType, FIRST_DECLARED_COL, LOG_COL_ATTRS, LogsScanExec,
    logs_schema_with_declared,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const CASES: u32 = 48;

// --- vocabularies ----------------------------------------------------------

/// Resource attribute keys, disjoint from the declared keys so the declared
/// columns are genuinely record-sourced (the common case) without an accidental
/// resource/record collision confusing the comparison.
const RESOURCE_KEYS: &[&str] = &["service.name", "host"];
const VALUE_VOCAB: &[&str] = &["api", "worker", "db", "edge"];
const WORD_VOCAB: &[&str] = &["timeout", "connection", "error", "ok", "retry"];
const SEVERITY_TEXT: &[&str] = &["INFO", "WARN", "ERROR"];

/// The tenant's four declared typed attribute columns, one per declared type.
fn declared_columns() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("dur", DeclaredType::I64),
        DeclaredColumn::new("name", DeclaredType::Str),
        DeclaredColumn::new("ok", DeclaredType::Bool),
        DeclaredColumn::new("blob", DeclaredType::Bytes),
    ]
}

/// Every schema index the fast path may project: the fixed columns (0..=7) and
/// the four declared columns (9..=12), never the `attrs` map (index 8).
fn eligible_indices() -> Vec<usize> {
    let mut v: Vec<usize> = (0..LOG_COL_ATTRS).collect();
    v.extend(FIRST_DECLARED_COL..FIRST_DECLARED_COL + declared_columns().len());
    v
}

// --- corpus ----------------------------------------------------------------

#[derive(Clone, Debug)]
struct RecordSpec {
    ts: i64,
    obs_extra: i64,
    severity_num: u8,
    severity_text: String,
    body: String,
    trace_id: Option<[u8; 16]>,
    span_id: Option<[u8; 8]>,
    flags: u32,
    resource: Vec<(String, AttrValue)>,
    attrs: Vec<(String, AttrValue)>,
}

#[derive(Clone, Debug)]
struct Scenario {
    records: Vec<RecordSpec>,
    /// Which eligible schema indices to project (a subsequence, at least `ts`).
    projection: Vec<usize>,
    /// Optional `has_word(body, _)` content predicate.
    word: Option<String>,
}

fn arb_value() -> impl Strategy<Value = AttrValue> {
    prop::sample::select(VALUE_VOCAB).prop_map(|s| AttrValue::Str(s.to_string()))
}

fn arb_resource() -> impl Strategy<Value = Vec<(String, AttrValue)>> {
    proptest::sample::subsequence(RESOURCE_KEYS.to_vec(), 0..=RESOURCE_KEYS.len())
        .prop_flat_map(|sel| {
            let n = sel.len();
            (Just(sel), prop::collection::vec(arb_value(), n))
        })
        .prop_map(|(sel, vals)| {
            sel.into_iter()
                .zip(vals)
                .map(|(k, v)| (k.to_string(), v))
                .collect()
        })
}

/// A declared key is independently absent, present with the matching variant, or
/// present with a deliberately wrong variant (which must read NULL, never a
/// cast). This is what exercises ADR-0090 decision 7 through the fast path.
fn arb_declared_attrs() -> impl Strategy<Value = Vec<(String, AttrValue)>> {
    let dur = prop_oneof![
        Just(None),
        any::<i32>().prop_map(|v| Some(AttrValue::I64(i64::from(v)))),
        Just(Some(AttrValue::Str("not-an-int".into()))), // wrong variant
    ];
    let name = prop_oneof![
        Just(None),
        prop::sample::select(VALUE_VOCAB).prop_map(|s| Some(AttrValue::Str(s.to_string()))),
        Just(Some(AttrValue::I64(7))), // wrong variant
    ];
    let ok = prop_oneof![
        Just(None),
        any::<bool>().prop_map(|b| Some(AttrValue::Bool(b))),
        Just(Some(AttrValue::Str("not-a-bool".into()))), // wrong variant
    ];
    let blob = prop_oneof![
        Just(None),
        prop::collection::vec(any::<u8>(), 0..4).prop_map(|b| Some(AttrValue::Bytes(b))),
        Just(Some(AttrValue::I64(9))), // wrong variant
    ];
    (dur, name, ok, blob).prop_map(|(dur, name, ok, blob)| {
        let mut out = Vec::new();
        if let Some(v) = dur {
            out.push(("dur".to_string(), v));
        }
        if let Some(v) = name {
            out.push(("name".to_string(), v));
        }
        if let Some(v) = ok {
            out.push(("ok".to_string(), v));
        }
        if let Some(v) = blob {
            out.push(("blob".to_string(), v));
        }
        out
    })
}

fn arb_body() -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(WORD_VOCAB), 1..4).prop_map(|w| w.join(" "))
}

fn arb_record() -> impl Strategy<Value = RecordSpec> {
    (
        0i64..50,
        0i64..3,
        0u8..=24,
        prop::sample::select(SEVERITY_TEXT),
        arb_body(),
        prop::option::of(any::<u8>().prop_map(|b| [b; 16])),
        prop::option::of(any::<u8>().prop_map(|b| [b; 8])),
        any::<u32>(),
        arb_resource(),
        arb_declared_attrs(),
    )
        .prop_map(
            |(
                ts,
                obs_extra,
                severity_num,
                severity_text,
                body,
                trace_id,
                span_id,
                flags,
                resource,
                attrs,
            )| RecordSpec {
                ts,
                obs_extra,
                severity_num,
                severity_text: severity_text.to_string(),
                body,
                trace_id,
                span_id,
                flags,
                resource,
                attrs,
            },
        )
}

fn arb_projection() -> impl Strategy<Value = Vec<usize>> {
    let all = eligible_indices();
    proptest::sample::subsequence(all.clone(), 0..=all.len()).prop_map(|mut sel| {
        if sel.is_empty() {
            sel.push(0);
        }
        sel
    })
}

fn arb_scenario() -> impl Strategy<Value = Scenario> {
    (
        prop::collection::vec(arb_record(), 1..12),
        arb_projection(),
        prop::option::of(prop::sample::select(WORD_VOCAB).prop_map(|s| s.to_string())),
    )
        .prop_map(|(records, projection, word)| Scenario {
            records,
            projection,
            word,
        })
}

// --- materialize -----------------------------------------------------------

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn build_record(spec: &RecordSpec) -> LogRecord {
    LogRecord {
        stream_id: log_stream_id(&spec.resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&spec.resource, "scope", "1.0", &[]),
        ts_ns: spec.ts,
        observed_ts_ns: spec.ts + spec.obs_extra,
        severity_num: spec.severity_num,
        severity_text: spec.severity_text.clone(),
        body: spec.body.clone(),
        trace_id: spec.trace_id,
        span_id: spec.span_id,
        flags: spec.flags,
        attrs: spec.attrs.clone(),
    }
}

/// Write `records` into one RLOG object with `cfg` and return its `SegmentRef`.
async fn write_object(
    store: &MemoryStore,
    key: &str,
    records: &[LogRecord],
    cfg: RlogConfig,
) -> SegmentRef {
    let mut w = RlogWriter::new(cfg, identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
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
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

/// Small blocks so a corpus of a dozen records still spans several blocks,
/// exercising the per-block loop and the block-release accounting.
fn small_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    }
}

// --- run the scan ----------------------------------------------------------

struct ScanRun {
    batches: Vec<RecordBatch>,
    columnar_batches: usize,
    rowpath_batches: usize,
}

/// Execute a `LogsScanExec` directly over `segments` with the given projection,
/// content predicate, and pending-erasure requests, returning its batches and
/// the two path metrics.
async fn run_scan(
    store: Arc<dyn ObjectStoreBackend>,
    segments: Vec<SegmentRef>,
    declared: Vec<DeclaredColumn>,
    projection: Option<Vec<usize>>,
    content: Vec<Predicate>,
    erasure_reqs: Vec<ravel_proto::commit::v1::ErasureRequest>,
) -> ScanRun {
    let full_schema = logs_schema_with_declared(&declared);
    let erasure = snapshot_pending_erasure_predicates(&Snapshot {
        segments: segments.clone(),
        segments_pruned: 0,
        pending_erasure: erasure_reqs,
    });
    let scan = LogsScanExec::new(
        TENANT,
        LogSegmentFetcher::new(store),
        &segments,
        1,
        i64::MIN,
        i64::MAX,
        Arc::new(content),
        Arc::new(Vec::new()),
        Arc::new(erasure),
        projection.as_ref(),
        QueryAccounting::new(),
        full_schema,
        Arc::new(declared),
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
        batches,
    }
}

/// A dummy pending erasure whose key exists in no record, so it forces the row
/// path (any erasure makes a scan ineligible) yet erases nothing, leaving the
/// output identical to the fast path over the same input.
fn no_match_erasure() -> Vec<ravel_proto::commit::v1::ErasureRequest> {
    vec![ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "__does_not_exist__".to_string(),
            value: "__nope__".to_string(),
        }],
        ..Default::default()
    }]
}

// --- comparable rows -------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Cell {
    Ts(i64),
    U8(u8),
    Str(String),
    Bin(Option<Vec<u8>>),
    U32(u32),
    OptI64(Option<i64>),
    OptStr(Option<String>),
    OptBool(Option<bool>),
}

fn a<T: Array + 'static>(batch: &RecordBatch, col: usize) -> &T {
    batch
        .column(col)
        .as_any()
        .downcast_ref::<T>()
        .expect("column type")
}

/// Extract every row of `batches` as a `Vec<Cell>`, one cell per projected
/// column, so two scans over the same input compare as sorted multisets.
fn rows(
    batches: &[RecordBatch],
    projection: &[usize],
    declared: &[DeclaredColumn],
) -> Vec<Vec<Cell>> {
    let mut out = Vec::new();
    for batch in batches {
        for i in 0..batch.num_rows() {
            let mut row = Vec::with_capacity(projection.len());
            for (col, &idx) in projection.iter().enumerate() {
                row.push(cell(batch, col, idx, declared, i));
            }
            out.push(row);
        }
    }
    out.sort();
    out
}

fn cell(
    batch: &RecordBatch,
    col: usize,
    idx: usize,
    declared: &[DeclaredColumn],
    i: usize,
) -> Cell {
    match idx {
        0 | 1 => Cell::Ts(a::<TimestampNanosecondArray>(batch, col).value(i)),
        2 => Cell::U8(a::<UInt8Array>(batch, col).value(i)),
        3 | 4 => Cell::Str(a::<StringArray>(batch, col).value(i).to_string()),
        5 | 6 => {
            let arr = a::<FixedSizeBinaryArray>(batch, col);
            Cell::Bin((!arr.is_null(i)).then(|| arr.value(i).to_vec()))
        }
        7 => Cell::U32(a::<UInt32Array>(batch, col).value(i)),
        _ => {
            let dc = &declared[idx - FIRST_DECLARED_COL];
            match dc.ty {
                DeclaredType::I64 => {
                    let arr = a::<Int64Array>(batch, col);
                    Cell::OptI64((!arr.is_null(i)).then(|| arr.value(i)))
                }
                DeclaredType::Str => {
                    let arr = a::<StringArray>(batch, col);
                    Cell::OptStr((!arr.is_null(i)).then(|| arr.value(i).to_string()))
                }
                DeclaredType::Bool => {
                    let arr = a::<BooleanArray>(batch, col);
                    Cell::OptBool((!arr.is_null(i)).then(|| arr.value(i)))
                }
                DeclaredType::Bytes => {
                    let arr = a::<BinaryArray>(batch, col);
                    Cell::Bin((!arr.is_null(i)).then(|| arr.value(i).to_vec()))
                }
            }
        }
    }
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

// --- the gate --------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(CASES))]

    /// The acceptance test (issue #415): over random corpora, projections, and
    /// content predicates, the columnar fast path's batches equal the row
    /// path's exactly, and the `columnar_batches` metric proves the fast path
    /// actually ran for the eligible cases.
    #[test]
    fn columnar_fast_path_matches_row_path_over_random_corpora(scenario in arb_scenario()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let declared = declared_columns();
            let records: Vec<LogRecord> = scenario.records.iter().map(build_record).collect();
            let store = MemoryStore::new();
            let seg = write_object(&store, "logs/case.rlog", &records, small_blocks()).await;
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
            let segments = vec![seg];

            let content: Vec<Predicate> = scenario
                .word
                .clone()
                .map(|w| Predicate::HasWord { field: FieldSel::Body, word: w })
                .into_iter()
                .collect();

            // Fast path: eligible (no erasure), no attrs_raw spill in this corpus.
            let fast = run_scan(
                Arc::clone(&store),
                segments.clone(),
                declared.clone(),
                Some(scenario.projection.clone()),
                content.clone(),
                Vec::new(),
            )
            .await;

            // Row path: same projection, forced ineligible by a no-match erasure
            // that changes no row.
            let row = run_scan(
                Arc::clone(&store),
                segments.clone(),
                declared.clone(),
                Some(scenario.projection.clone()),
                content.clone(),
                no_match_erasure(),
            )
            .await;

            let n = total_rows(&fast.batches);
            prop_assert_eq!(n, total_rows(&row.batches), "row counts must match");

            // The fast path never fell back on this spill-free corpus, and it
            // ran (emitted a columnar batch) whenever there was output.
            prop_assert_eq!(fast.rowpath_batches, 0, "fast scan must not fall back");
            if n > 0 {
                prop_assert!(fast.columnar_batches > 0, "fast path must have run");
            }
            // The forced row path never touched the fast path.
            prop_assert_eq!(row.columnar_batches, 0, "row scan must not run columnar");
            if n > 0 {
                prop_assert!(row.rowpath_batches > 0, "row path must have run");
            }

            let want = rows(&row.batches, &scenario.projection, &declared);
            let got = rows(&fast.batches, &scenario.projection, &declared);
            prop_assert_eq!(got, want, "fast path output must equal the row path exactly");
            Ok(())
        })?;
    }
}

/// The data-protection case (issue #415): with a pending erasure predicate
/// active, the scan takes the row path (`columnar_batches == 0`,
/// `rowpath_batches > 0`) and the erased record is absent from the output. This
/// is the clause that fails closed on purpose (ADR-0099 decision 2): erasure
/// decides whether an erased record reaches a client.
#[tokio::test]
async fn pending_erasure_forces_the_row_path() {
    let declared = declared_columns();
    let resource = vec![("service.name".to_string(), AttrValue::Str("worker".into()))];
    let mk = |ts: i64, user: &str, body: &str| LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("user_id".to_string(), AttrValue::Str(user.into()))],
    };
    let records = vec![mk(1, "u1", "erase me"), mk(2, "u2", "keep me")];

    let store = MemoryStore::new();
    let seg = write_object(&store, "logs/erasure.rlog", &records, RlogConfig::default()).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    let erasure = vec![ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "user_id".to_string(),
            value: "u1".to_string(),
        }],
        ..Default::default()
    }];

    // Project only fixed columns, which would be eligible but for the erasure.
    let run = run_scan(
        store,
        vec![seg],
        declared.clone(),
        Some(vec![0, 4]), // ts, body
        Vec::new(),
        erasure,
    )
    .await;

    assert_eq!(run.columnar_batches, 0, "erasure must forbid the fast path");
    assert!(run.rowpath_batches > 0, "the row path must have run");

    let projection = vec![0usize, 4];
    let got = rows(&run.batches, &projection, &declared);
    // Only ts=2 "keep me" survives; the erased ts=1 "u1" row is absent.
    assert_eq!(got.len(), 1, "the erased record must be dropped");
    assert_eq!(got[0][0], Cell::Ts(2));
    assert_eq!(got[0][1], Cell::Str("keep me".to_string()));
}

/// A block WITH an `attrs_raw` spill and a query projecting a declared column
/// falls back to the row path and still returns the spilled attribute's value
/// (issue #415). A false-negative `has_attrs_raw_page` would silently drop the
/// spilled value, so this asserts the value itself, not only the path taken.
///
/// The spill is forced with `max_dynamic_columns = 1`: the alphabetically
/// earlier `filler` key consumes the single dynamic column, so the declared
/// `tags` key overflows into `attrs_raw` and is only visible after canonical
/// decode, which the fast path does not do.
#[tokio::test]
async fn attrs_raw_spill_falls_back_to_row_path_and_keeps_the_value() {
    let declared = vec![DeclaredColumn::new("tags", DeclaredType::Str)];
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let record = LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: 1,
        observed_ts_ns: 1,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: "row".into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![
            ("filler".to_string(), AttrValue::Str("f".into())),
            ("tags".to_string(), AttrValue::Str("spilled".into())),
        ],
    };
    let cfg = RlogConfig {
        max_dynamic_columns: 1,
        ..RlogConfig::default()
    };

    let store = MemoryStore::new();
    let seg = write_object(&store, "logs/spill.rlog", &[record], cfg).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    // Project ts and the declared `tags` column (schema index FIRST_DECLARED_COL).
    let projection = vec![0usize, FIRST_DECLARED_COL];
    let run = run_scan(
        store,
        vec![seg],
        declared.clone(),
        Some(projection.clone()),
        Vec::new(),
        Vec::new(),
    )
    .await;

    assert_eq!(
        run.columnar_batches, 0,
        "an attrs_raw block must fall back to the row path"
    );
    assert!(run.rowpath_batches > 0, "the row path must have run");

    let got = rows(&run.batches, &projection, &declared);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0][0], Cell::Ts(1));
    assert_eq!(
        got[0][1],
        Cell::OptStr(Some("spilled".to_string())),
        "the spilled declared value must survive the fallback"
    );
}
