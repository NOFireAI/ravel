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

/// Resource attribute keys. `name` is deliberately also a *declared* key, so a
/// generated record can set the same key at both resource and record level and
/// the differential exercises the record-wins-over-resource merge rather than
/// only the resource-absent case. With a disjoint vocabulary an implementation
/// that inverted the precedence passed the whole suite.
const RESOURCE_KEYS: &[&str] = &["service.name", "host", "name"];
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
    let bytes = encode_object(records, cfg);
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    put_object(store, key, bytes, min, max, records.len()).await
}

/// Encode `records` into one RLOG object's bytes.
fn encode_object(records: &[LogRecord], cfg: RlogConfig) -> Vec<u8> {
    let mut w = RlogWriter::new(cfg, identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
}

/// Small blocks so a corpus of a dozen records still spans several blocks,
/// exercising the per-block loop and the block-release accounting.
fn small_blocks() -> RlogConfig {
    RlogConfig {
        block_target_records: 3,
        ..RlogConfig::default()
    }
}

/// Put already-encoded RLOG bytes into `store` and describe them as a
/// `SegmentRef`. Split out of [`write_object`] because one fixture needs an
/// object the writer cannot produce (see `rewrite_single_block`).
async fn put_object(
    store: &MemoryStore,
    key: &str,
    bytes: Vec<u8>,
    min: i64,
    max: i64,
    records: usize,
) -> SegmentRef {
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        ingest_hour_bucket: 0,
        sample_count: records as u64,
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

/// A declared key set at BOTH resource and record level, which is where the
/// merge's precedence is decided (ADR-0033: the record wins) and where a wrong
/// variant must read NULL rather than falling through to the resource value
/// (ADR-0090 decision 7).
///
/// Three rows, one per case, asserted on both paths:
///
/// - ts 0: resource sets `name`, the record sets it too -> the record's value;
/// - ts 1: resource sets `name`, the record does not -> the resource's value;
/// - ts 2: resource sets `name`, the record sets it as an `I64` -> NULL, not the
///   resource value: the record does set the key, so the fallback is not
///   consulted, and the value's variant does not match the declared type.
///
/// An implementation that inverted the precedence, or that fell through to the
/// resource layer on a wrong-variant record value, produces a different answer
/// for ts 0 and ts 2 respectively.
#[tokio::test]
async fn a_declared_key_set_at_both_levels_resolves_record_over_resource() {
    let declared = declared_columns();
    // Index 1 of `declared_columns()` is the `Str`-typed `name`.
    let name_col = FIRST_DECLARED_COL + 1;
    let projection = vec![0usize, name_col];

    let resource = vec![("name".to_string(), AttrValue::Str("from-resource".into()))];
    let mk = |ts: i64, attrs: Vec<(String, AttrValue)>| LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: "b".into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    };
    let records = vec![
        mk(
            0,
            vec![("name".to_string(), AttrValue::Str("from-record".into()))],
        ),
        mk(1, Vec::new()),
        mk(2, vec![("name".to_string(), AttrValue::I64(7))]),
    ];

    let store = MemoryStore::new();
    let seg = write_object(&store, "logs/collide.rlog", &records, RlogConfig::default()).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let segments = vec![seg];

    let want = vec![
        vec![Cell::Ts(0), Cell::OptStr(Some("from-record".to_string()))],
        vec![Cell::Ts(1), Cell::OptStr(Some("from-resource".to_string()))],
        vec![Cell::Ts(2), Cell::OptStr(None)],
    ];

    let fast = run_scan(
        Arc::clone(&store),
        segments.clone(),
        declared.clone(),
        Some(projection.clone()),
        Vec::new(),
        Vec::new(),
    )
    .await;
    assert!(fast.columnar_batches > 0, "the fast path must have run");
    assert_eq!(fast.rowpath_batches, 0, "the fast path must not fall back");
    assert_eq!(rows(&fast.batches, &projection, &declared), want);

    let row = run_scan(
        Arc::clone(&store),
        segments,
        declared.clone(),
        Some(projection.clone()),
        Vec::new(),
        no_match_erasure(),
    )
    .await;
    assert_eq!(
        row.columnar_batches, 0,
        "the forced run must take the row path"
    );
    assert!(row.rowpath_batches > 0, "the row path must have run");
    assert_eq!(rows(&row.batches, &projection, &declared), want);
}

/// The `attrs_raw` fallback *mid-segment*: an earlier block is clean and a later
/// one carries an overflow page, so the fast path emits block 0, then re-opens
/// the segment on the row path and discards the blocks it already emitted.
///
/// That discard loop (`LogScanState::Rows`'s `skip`) is the one place in the
/// columnar change where a row can be emitted twice or dropped, and a fixture of
/// one block can never reach it: the fallback there fires on block 0 with
/// `skip == 0`. Here `columnar_batches == 1` and `rowpath_batches == 1` prove
/// both halves ran, and the `ts` multiset proves every row was emitted exactly
/// once across the switch.
///
/// The spill is forced with `max_dynamic_columns = 1`: `filler` (earlier in the
/// writer's `(name, type)` order) takes the single dynamic column, so `tags`
/// overflows into `attrs_raw` -- but only in the block holding the two records
/// that carry it, because a block whose records all fit their columns has no
/// `attrs_raw` page at all.
#[tokio::test]
async fn a_later_block_with_attrs_raw_falls_back_without_losing_or_repeating_rows() {
    let declared = vec![DeclaredColumn::new("tags", DeclaredType::Str)];
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let mk = |ts: i64, spilled: bool| LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: "b".into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: {
            let mut attrs = vec![("filler".to_string(), AttrValue::Str("f".into()))];
            if spilled {
                attrs.push(("tags".to_string(), AttrValue::Str(format!("spilled-{ts}"))));
            }
            attrs
        },
    };
    // Blocks of two records: block 0 (ts 0, 1) is clean, block 1 (ts 2, 3)
    // carries the `attrs_raw` page.
    let records = vec![mk(0, false), mk(1, false), mk(2, true), mk(3, true)];
    let cfg = RlogConfig {
        block_target_records: 2,
        max_dynamic_columns: 1,
        ..RlogConfig::default()
    };

    let store = MemoryStore::new();
    let seg = write_object(&store, "logs/midspill.rlog", &records, cfg).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

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
        run.columnar_batches, 1,
        "the clean first block must be emitted columnar; columnar={}, rowpath={}",
        run.columnar_batches, run.rowpath_batches
    );
    assert_eq!(
        run.rowpath_batches, 1,
        "the spilled second block must be emitted by the re-opened row path; \
         columnar={}, rowpath={}",
        run.columnar_batches, run.rowpath_batches
    );

    let got = rows(&run.batches, &projection, &declared);
    let want = vec![
        vec![Cell::Ts(0), Cell::OptStr(None)],
        vec![Cell::Ts(1), Cell::OptStr(None)],
        vec![Cell::Ts(2), Cell::OptStr(Some("spilled-2".to_string()))],
        vec![Cell::Ts(3), Cell::OptStr(Some("spilled-3".to_string()))],
    ];
    assert_eq!(
        got, want,
        "every row exactly once across the columnar/row switch, spilled values \
         included"
    );
}
