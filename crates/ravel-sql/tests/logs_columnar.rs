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
    Array, BinaryArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray, Int64Array,
    RecordBatch, StringArray, TimestampNanosecondArray, UInt8Array, UInt32Array,
};
use datafusion::arrow::datatypes::{DataType, Int32Type};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use proptest::prelude::*;
use ravel_cache::{Cache, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_codec::encoding::{Enc, encode_strings};
use ravel_logseg::block::{ColumnPlan, write_block};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{
    COMP_NONE, LogFooter, SectionDesc, kind, open as open_footer, write_footer_and_trailer,
};
use ravel_logseg::reader::read_section;
use ravel_logseg::record::{ColumnValue, ResolvedRow};
use ravel_logseg::skip_index::{Level0Entry, SkipIndex};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, FieldSel, FieldType, LogRecord, Predicate, RlogConfig, RlogWriter,
    stream_attrs_bytes,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::erasure::snapshot_pending_erasure_predicates;
use ravel_query::{CacheFetchError, LogSegmentFetcher};
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
    pages_decoded: usize,
    pages_skipped: usize,
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
        pages_decoded: count("pages_decoded"),
        pages_skipped: count("pages_skipped"),
        batches,
    }
}

/// A logs fetcher with ADR-0046's read cache wired, sized to hold any fixture
/// in this file with room to spare so the striping's repeated whole-object reads
/// coalesce rather than evict each other.
fn cached_fetcher(store: Arc<dyn ObjectStoreBackend>) -> LogSegmentFetcher {
    let cache: Cache<CacheFetchError> = Cache::new(CacheLimits::new(1 << 26, 1 << 20, 1 << 26));
    LogSegmentFetcher::new(store).with_cache(Arc::new(cache))
}

/// The result of executing a `LogsScanExec` across ALL of its declared
/// partitions (intra-segment scan partitioning, ADR-0102), for the tests that
/// need block striping to actually fan out.
struct MultiRun {
    /// Every partition's batches, concatenated. Row order across partitions is
    /// meaningless (no ordering is declared), so callers compare as a multiset.
    batches: Vec<RecordBatch>,
    columnar_batches: usize,
    rowpath_batches: usize,
    blocks_total: usize,
    blocks_scanned: usize,
    /// Partitions that emitted at least one row. The capability this item adds
    /// is that this can exceed the segment count.
    non_empty_partitions: usize,
    /// The DataFusion partition count the scan declared (`target_partitions`).
    declared_partitions: usize,
}

/// Execute `LogsScanExec` over `segments` at `target_partitions`, draining every
/// declared partition and summing the path/block metrics across all of them.
///
/// The fetcher is wired with ADR-0046's read cache, because that is the
/// precondition `LogsScanExec::new` requires before it declares more partitions
/// than there are segments (ADR-0102 decision 1): an un-cached fetcher is capped
/// at the segment count, which is what
/// `an_uncached_fetcher_caps_partitions_at_the_segment_count` pins. These tests
/// exercise the striping itself, so they take the cached side of that gate.
#[allow(clippy::too_many_arguments)]
async fn run_scan_tp(
    store: Arc<dyn ObjectStoreBackend>,
    segments: Vec<SegmentRef>,
    declared: Vec<DeclaredColumn>,
    projection: Option<Vec<usize>>,
    content: Vec<Predicate>,
    erasure_reqs: Vec<ravel_proto::commit::v1::ErasureRequest>,
    target_partitions: usize,
) -> MultiRun {
    let full_schema = logs_schema_with_declared(&declared);
    let erasure = snapshot_pending_erasure_predicates(&Snapshot {
        segments: segments.clone(),
        segments_pruned: 0,
        pending_erasure: erasure_reqs,
    });
    let scan = LogsScanExec::new(
        TENANT,
        cached_fetcher(store),
        &segments,
        target_partitions,
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

    let declared_partitions = scan.properties().output_partitioning().partition_count();
    let ctx = Arc::new(TaskContext::default());
    let mut batches = Vec::new();
    let mut non_empty_partitions = 0;
    for p in 0..declared_partitions {
        let mut stream = scan.execute(p, Arc::clone(&ctx)).expect("execute");
        let mut rows_here = 0usize;
        while let Some(next) = stream.next().await {
            let batch = next.expect("batch");
            rows_here += batch.num_rows();
            batches.push(batch);
        }
        if rows_here > 0 {
            non_empty_partitions += 1;
        }
    }

    let metrics = scan.metrics().expect("metrics");
    let count = |name: &str| metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
    MultiRun {
        batches,
        columnar_batches: count("columnar_batches"),
        rowpath_batches: count("rowpath_batches"),
        blocks_total: count("blocks_total"),
        blocks_scanned: count("blocks_scanned"),
        non_empty_partitions,
        declared_partitions,
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

/// The logical `Str` value of a declared `Dictionary(Int32, Utf8)` column at row
/// `i`: `None` for a NULL key or a key addressing a NULL (non-UTF-8) dictionary
/// value, `Some(s)` otherwise. Declared `Str` columns are dictionary-encoded
/// (ADR-0099 decision 5), so a plain `StringArray` downcast no longer applies.
fn dict_str(batch: &RecordBatch, col: usize, i: usize) -> Option<String> {
    let dict = a::<DictionaryArray<Int32Type>>(batch, col);
    if dict.is_null(i) {
        return None;
    }
    let values = dict
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("dictionary values are Utf8");
    let k = usize::try_from(dict.keys().value(i)).expect("non-negative key");
    (!values.is_null(k)).then(|| values.value(k).to_string())
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
                DeclaredType::Str => Cell::OptStr(dict_str(batch, col, i)),
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
        Arc::clone(&store),
        vec![seg.clone()],
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

    // Issue #474: pages_decoded/pages_skipped must count the abandoned
    // columnar cursor's partial decode of this same segment, not just the
    // re-opened row scan's own pass. A row-path-only run of the identical
    // segment (forced ineligible via a non-matching erasure predicate, same
    // pattern the eligibility tests above use) decodes every block exactly
    // once and is the true single-pass floor; the fallback run does strictly
    // more decode work (block 0's abandoned columnar pass, plus whatever of
    // block 1 the columnar cursor decoded before detecting the overflow
    // page), so its published pages_decoded must be strictly greater, not
    // equal to the floor.
    let baseline = run_scan(
        store,
        vec![seg],
        declared.clone(),
        Some(projection.clone()),
        Vec::new(),
        no_match_erasure(),
    )
    .await;
    assert_eq!(
        baseline.columnar_batches, 0,
        "the forced baseline must take the row path only"
    );
    assert!(
        run.pages_decoded > baseline.pages_decoded,
        "the fallback run's pages_decoded ({}) must exceed the single-pass \
         row-only baseline ({}): the abandoned columnar cursor's decode work \
         must be counted, not dropped",
        run.pages_decoded,
        baseline.pages_decoded
    );
    // The projection excludes `filler` (the dynamic column that won the one
    // available slot), so block 0's clean columnar pass skips its page; the
    // abandoned pass's skip count is subject to the same drop this fix
    // addresses.
    assert!(
        run.pages_skipped > baseline.pages_skipped,
        "the fallback run's pages_skipped ({}) must exceed the single-pass \
         row-only baseline ({}) for the same reason",
        run.pages_skipped,
        baseline.pages_skipped
    );
}

/// The strided sibling of the test above, for intra-segment scan partitioning
/// (ADR-0102, deliverable 4): the single most important test in this item,
/// because a wrong `ReopenRows` fallback drops or duplicates rows *silently* --
/// no crash, just a wrong answer.
///
/// One segment of four two-record blocks is striped across `target_partitions =
/// 2`. Blocks flatten to surviving positions 0..3 and unit `i` goes to partition
/// `i % 2`, so partition 0 owns positions {0, 2} and partition 1 owns {1, 3}.
/// The `attrs_raw` overflow is placed on positions 2 and 3 (the LATER block each
/// partition owns), so on each partition the fallback fires on a block it does
/// NOT own contiguously from the start of the segment: it has already emitted
/// its position-0/1 block columnar (`seg_columnar_blocks == 1`) and must skip
/// exactly ONE position of its OWN index list, not one block of the whole
/// segment.
///
/// The fix (ADR-0102) is that the reopened row scan is handed the SAME
/// `current_indices` this partition owns, so `skip` is a position within that
/// list. Proven by mutation: temporarily change the `LogScanState::Fallback`
/// arm in `crates/ravel-sql/src/logs_scan.rs` to reopen a contiguous
/// whole-segment range instead of `this.current_indices.clone()` -- e.g.
/// `open_segment_subset(ctx, seg, (0..=this.current_indices.iter().copied()
/// .max().unwrap_or(0)).collect())` -- and this test goes RED with duplicated
/// rows (each partition's whole-segment skip-by-count re-emits blocks the other
/// partition owns). Restoring `this.current_indices.clone()` makes it pass.
#[tokio::test]
async fn a_strided_fallback_across_partitions_neither_drops_nor_repeats_rows() {
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
    // Four blocks of two records: positions 0 (ts 0,1) and 1 (ts 2,3) are clean,
    // positions 2 (ts 4,5) and 3 (ts 6,7) carry the `attrs_raw` page. Under the
    // 2-way stride, partition 0 gets {clean pos 0, spilled pos 2} and partition 1
    // gets {clean pos 1, spilled pos 3}: each falls back on its SECOND owned
    // block, so `skip == 1` against its own two-element index list.
    let records = vec![
        mk(0, false),
        mk(1, false),
        mk(2, false),
        mk(3, false),
        mk(4, true),
        mk(5, true),
        mk(6, true),
        mk(7, true),
    ];
    let cfg = RlogConfig {
        block_target_records: 2,
        max_dynamic_columns: 1,
        ..RlogConfig::default()
    };

    let store = MemoryStore::new();
    let seg = write_object(&store, "logs/strided-midspill.rlog", &records, cfg).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    let projection = vec![0usize, FIRST_DECLARED_COL];
    let run = run_scan_tp(
        store,
        vec![seg],
        declared.clone(),
        Some(projection.clone()),
        Vec::new(),
        Vec::new(),
        2,
    )
    .await;

    // Both partitions ran, each emitting one clean block columnar and one
    // spilled block through the re-opened row path.
    assert_eq!(run.declared_partitions, 2, "target_partitions=2 declared");
    assert_eq!(
        run.non_empty_partitions, 2,
        "both partitions own blocks and emit rows"
    );
    assert_eq!(
        run.columnar_batches, 2,
        "each partition emits its clean block columnar; columnar={}, rowpath={}",
        run.columnar_batches, run.rowpath_batches
    );
    assert_eq!(
        run.rowpath_batches, 2,
        "each partition emits its spilled block via the re-opened row path; \
         columnar={}, rowpath={}",
        run.columnar_batches, run.rowpath_batches
    );

    let got = rows(&run.batches, &projection, &declared);
    let want = vec![
        vec![Cell::Ts(0), Cell::OptStr(None)],
        vec![Cell::Ts(1), Cell::OptStr(None)],
        vec![Cell::Ts(2), Cell::OptStr(None)],
        vec![Cell::Ts(3), Cell::OptStr(None)],
        vec![Cell::Ts(4), Cell::OptStr(Some("spilled-4".to_string()))],
        vec![Cell::Ts(5), Cell::OptStr(Some("spilled-5".to_string()))],
        vec![Cell::Ts(6), Cell::OptStr(Some("spilled-6".to_string()))],
        vec![Cell::Ts(7), Cell::OptStr(Some("spilled-7".to_string()))],
    ];
    assert_eq!(
        got, want,
        "every row exactly once across the strided columnar/row switch, no drops \
         and no duplicates"
    );
}

/// Differential correctness (ADR-0102): the SAME query over the SAME segments
/// yields the SAME row multiset under a single partition (blocks drained in
/// order, the closest analogue of the old whole-segment partitioning) and under
/// block-level striping across many partitions. Covers BOTH the
/// already-adequately-partitioned case (segment count >= target_partitions) and
/// the undersubscribed case (segment count < target_partitions), the latter
/// being the one this item exists to fix.
#[tokio::test]
async fn block_striping_is_row_identical_to_single_partition() {
    let declared = declared_columns();
    // Vary block counts per segment so the stride crosses segment boundaries at
    // non-trivial offsets (block_target_records = 3, see `small_blocks`).
    let seg_sizes = [7usize, 4, 11, 5];
    let mut all_records: Vec<Vec<LogRecord>> = Vec::new();
    let mut ts = 0i64;
    for &size in &seg_sizes {
        let mut recs = Vec::with_capacity(size);
        for _ in 0..size {
            let spilled = ts % 3 == 0;
            recs.push(LogRecord {
                stream_id: log_stream_id(
                    &[("service.name".to_string(), AttrValue::Str("api".into()))],
                    "scope",
                    "1.0",
                    &[],
                ),
                stream_attrs: stream_attrs_bytes(
                    &[("service.name".to_string(), AttrValue::Str("api".into()))],
                    "scope",
                    "1.0",
                    &[],
                ),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: (ts % 5) as u8,
                severity_text: "INFO".into(),
                body: format!("body-{ts}"),
                trace_id: None,
                span_id: None,
                flags: ts as u32,
                attrs: {
                    let mut a = vec![("filler".to_string(), AttrValue::Str("f".into()))];
                    if spilled {
                        // Overflow into attrs_raw for some blocks so the run also
                        // exercises the fallback path under striping.
                        a.push(("tags".to_string(), AttrValue::Str(format!("t-{ts}"))));
                    }
                    a
                },
            });
            ts += 1;
        }
        all_records.push(recs);
    }

    // max_dynamic_columns = 1 so `tags` overflows into attrs_raw wherever it is
    // set, forcing the columnar->row fallback on those blocks.
    let cfg = RlogConfig {
        block_target_records: 3,
        max_dynamic_columns: 1,
        ..RlogConfig::default()
    };

    let store = MemoryStore::new();
    let mut segments = Vec::new();
    for (i, recs) in all_records.iter().enumerate() {
        segments.push(write_object(&store, &format!("logs/diff-{i}.rlog"), recs, cfg).await);
    }
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    let projection = vec![0usize, 4usize, FIRST_DECLARED_COL + 1];

    let baseline = run_scan_tp(
        Arc::clone(&store),
        segments.clone(),
        declared.clone(),
        Some(projection.clone()),
        Vec::new(),
        Vec::new(),
        1,
    )
    .await;
    let baseline_rows = rows(&baseline.batches, &projection, &declared);

    // Adequately partitioned: 4 segments, target_partitions = 4.
    let adequate = run_scan_tp(
        Arc::clone(&store),
        segments.clone(),
        declared.clone(),
        Some(projection.clone()),
        Vec::new(),
        Vec::new(),
        4,
    )
    .await;
    assert_eq!(
        rows(&adequate.batches, &projection, &declared),
        baseline_rows,
        "block striping at target_partitions=4 (>= segment count) must be \
         row-identical to a single partition"
    );

    // Undersubscribed: 4 segments, target_partitions = 12 (the case this fixes).
    let under = run_scan_tp(
        Arc::clone(&store),
        segments.clone(),
        declared.clone(),
        Some(projection.clone()),
        Vec::new(),
        Vec::new(),
        12,
    )
    .await;
    assert_eq!(
        rows(&under.batches, &projection, &declared),
        baseline_rows,
        "block striping at target_partitions=12 (< nothing; > segment count) \
         must be row-identical to a single partition"
    );

    // The whole-segment prune totals are counted once regardless of how many
    // partitions stripe a segment: blocks_total is stable across the sweep.
    assert_eq!(
        baseline.blocks_total, adequate.blocks_total,
        "blocks_total is a whole-object figure, counted once, not per partition"
    );
    assert_eq!(
        baseline.blocks_total, under.blocks_total,
        "blocks_total stays stable under heavy striping"
    );
    // NOT partition-count-invariant once an attrs_raw fallback is involved
    // (issue #474): each partition that independently hits a spilled block
    // within its own owned subset abandons its own columnar cursor and
    // re-decodes its own blocks from the start, so finer striping can create
    // MORE independent fallback points than a single partition ever would,
    // each paying its own one-time double-decode. `blocks_scanned` now
    // counts that work honestly (record_scan publishes the abandoned
    // cursor's stats before it is dropped), so `under`, striped far finer
    // than `baseline`, can only ever report as much or more real decode
    // work, never less.
    assert!(
        under.blocks_scanned >= baseline.blocks_scanned,
        "finer striping can only add fallback-driven re-decode work, never \
         remove it: under={} baseline={}",
        under.blocks_scanned,
        baseline.blocks_scanned
    );
}

/// The capability this item adds (ADR-0102): in the undersubscribed case
/// (segment count < target_partitions) the scan now uses MORE than
/// `segment_count` non-empty partitions when the block count allows it. Asserts
/// the non-empty partition count directly, not just row correctness.
#[tokio::test]
async fn undersubscribed_uses_more_partitions_than_segments() {
    let declared = declared_columns();
    // Two segments, but 18 and 21 records at 3 records/block => 6 and 7 blocks,
    // 13 surviving blocks total. With target_partitions = 8 the stride is
    // min(8, 13) = 8, so 8 partitions each own at least one block -- four times
    // the segment count.
    let store = MemoryStore::new();
    let cfg = small_blocks();
    let mut segments = Vec::new();
    let mut ts = 0i64;
    for (i, &size) in [18usize, 21].iter().enumerate() {
        let mut recs = Vec::with_capacity(size);
        for _ in 0..size {
            recs.push(LogRecord {
                stream_id: log_stream_id(
                    &[("service.name".to_string(), AttrValue::Str("api".into()))],
                    "scope",
                    "1.0",
                    &[],
                ),
                stream_attrs: stream_attrs_bytes(
                    &[("service.name".to_string(), AttrValue::Str("api".into()))],
                    "scope",
                    "1.0",
                    &[],
                ),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 1,
                severity_text: "INFO".into(),
                body: format!("b-{ts}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![],
            });
            ts += 1;
        }
        segments.push(write_object(&store, &format!("logs/under-{i}.rlog"), &recs, cfg).await);
    }
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    let projection = vec![0usize];
    let run = run_scan_tp(
        store,
        segments,
        declared,
        Some(projection),
        Vec::new(),
        Vec::new(),
        8,
    )
    .await;

    assert_eq!(run.declared_partitions, 8, "target_partitions=8 declared");
    assert!(
        run.non_empty_partitions > 2,
        "undersubscribed (2 segments) must fan out to more than 2 non-empty \
         partitions; got {}",
        run.non_empty_partitions
    );
    assert_eq!(
        total_rows(&run.batches),
        39,
        "all 18+21 records are emitted once"
    );
}

/// The cache gate on the fan-out (ADR-0102 decision 1): striping one segment's
/// blocks across K partitions means K whole-object GETs at that segment's key,
/// because the fetch unit is the whole object (ADR-0087 decision 3, no ranged
/// block reader). ADR-0046's read cache is what makes those coalesce, so the
/// planner only declares more partitions than segments when the fetcher carries
/// one. Un-cached, the partition count falls back to the pre-ADR-0102 bound,
/// `min(target_partitions, segment_count)`.
///
/// Both halves are asserted over the SAME fixture and the SAME
/// `target_partitions`, so the only difference is the cache.
#[tokio::test]
async fn an_uncached_fetcher_caps_partitions_at_the_segment_count() {
    let declared = declared_columns();
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let mk = |ts: i64| LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("b-{ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    };

    // Two segments, 12 records each at 3 records/block: 4 blocks per segment, 8
    // surviving blocks total, so the block count is NOT what limits the fan-out
    // at target_partitions = 6. Only the cache gate is.
    const SEGMENTS: usize = 2;
    const TARGET_PARTITIONS: usize = 6;
    let store = MemoryStore::new();
    let mut segments = Vec::new();
    let mut ts = 0i64;
    for i in 0..SEGMENTS {
        let recs: Vec<LogRecord> = (0..12)
            .map(|_| {
                let r = mk(ts);
                ts += 1;
                r
            })
            .collect();
        segments.push(
            write_object(
                &store,
                &format!("logs/gate-{i}.rlog"),
                &recs,
                small_blocks(),
            )
            .await,
        );
    }
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let full_schema = logs_schema_with_declared(&declared);
    let projection = vec![0usize];

    let build = |fetcher: LogSegmentFetcher| {
        LogsScanExec::new(
            TENANT,
            fetcher,
            &segments,
            TARGET_PARTITIONS,
            i64::MIN,
            i64::MAX,
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Arc::new(Vec::new()),
            Some(&projection),
            QueryAccounting::new(),
            Arc::clone(&full_schema),
            Arc::new(declared.clone()),
        )
        .expect("build scan")
    };

    let uncached = build(LogSegmentFetcher::new(Arc::clone(&store)));
    assert_eq!(
        uncached
            .properties()
            .output_partitioning()
            .partition_count(),
        SEGMENTS,
        "an un-cached fetcher caps the partition count at the segment count \
         ({SEGMENTS}), not target_partitions ({TARGET_PARTITIONS}): nothing \
         absorbs the extra whole-object GETs"
    );

    let cached = build(cached_fetcher(Arc::clone(&store)));
    assert_eq!(
        cached.properties().output_partitioning().partition_count(),
        TARGET_PARTITIONS,
        "a cache-wired fetcher declares target_partitions ({TARGET_PARTITIONS}) \
         even though only {SEGMENTS} segments are scanned"
    );

    // The clamp must not lose rows: the capped plan still emits every record.
    let ctx = Arc::new(TaskContext::default());
    let mut rows_seen = 0usize;
    for p in 0..uncached
        .properties()
        .output_partitioning()
        .partition_count()
    {
        let mut stream = uncached.execute(p, Arc::clone(&ctx)).expect("execute");
        while let Some(next) = stream.next().await {
            rows_seen += next.expect("batch").num_rows();
        }
    }
    assert_eq!(
        rows_seen,
        SEGMENTS * 12,
        "the un-cached, segment-capped plan still emits every record"
    );
}

/// `LogsScanExec` still declares NO output ordering after the change to
/// block-level striping (ADR-0087 decision 1, ADR-0102): striping blocks rather
/// than segments only weakens any per-partition order, so declaring one would
/// be unsound. Nothing downstream in this crate depends on scan order; an
/// `ORDER BY` gets an explicit `SortExec` above this leaf.
#[tokio::test]
async fn logs_scan_declares_no_output_ordering() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let scan = LogsScanExec::new(
        TENANT,
        LogSegmentFetcher::new(store),
        &[],
        8,
        i64::MIN,
        i64::MAX,
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        Arc::new(Vec::new()),
        None,
        QueryAccounting::new(),
        logs_schema_with_declared(&[]),
        Arc::new(Vec::new()),
    )
    .expect("build scan");
    assert!(
        scan.properties().output_ordering().is_none(),
        "LogsScanExec must declare no output ordering"
    );
}

// --- a declared `Str` cell that is not UTF-8 --------------------------------

/// Rewrite one writer-produced RLOG object, replacing its single block with one
/// whose dynamic column `column_id` holds `cells[i]` verbatim at row `i`.
///
/// `RlogWriter` cannot produce the fixture this exists for: a `Str` column cell
/// that is not UTF-8. Its input is `AttrValue::Str(String)` and `resolve_value`
/// carries those bytes through unchanged, so every `Str` cell it writes is valid
/// by construction -- while a reader still has to decide what such a cell means,
/// and the two batch-building paths must decide it the same way.
///
/// Everything except the block is the writer's own output: STREAM_DIR, FIELD_DIR
/// and BLOOM are copied verbatim (bytes, `comp` and `uncomp_len` alike) and the
/// footer keeps its identity and counters, because `rows` mirrors the object's
/// records one for one. Only BLOCKS and the SKIP_IDX entry describing it are
/// rebuilt, through `write_block` and `SkipIndex` themselves, so this carries no
/// independent knowledge of any section's encoding. The stale BLOOM is harmless
/// here: it is consulted only to prune on a content predicate, and this fixture
/// pushes none.
fn rewrite_str_column_cells(
    obj: &[u8],
    cfg: RlogConfig,
    records: &[LogRecord],
    column_id: u32,
    cells: &[Option<Vec<u8>>],
) -> Vec<u8> {
    assert_eq!(records.len(), cells.len(), "one cell per record");
    let footer = open_footer(obj).expect("open the writer's footer");

    let rows: Vec<ResolvedRow> = records
        .iter()
        .zip(cells)
        .map(|(r, cell)| ResolvedRow {
            stream_ref: 0,
            ts_ns: r.ts_ns,
            observed_ts_ns: r.observed_ts_ns,
            severity_num: r.severity_num,
            severity_text: r.severity_text.clone(),
            body: r.body.clone(),
            trace_id: r.trace_id,
            span_id: r.span_id,
            flags: r.flags,
            attrs_raw: None,
            // `None` leaves the declared column absent at this row, so a
            // resource-level value shows through the merge (the fallback-append
            // branch); `Some` writes the record cell verbatim, including bytes
            // the writer would reject (a non-UTF-8 `Str`).
            columns: match cell {
                Some(bytes) => vec![(column_id, ColumnValue::Str(bytes.clone()))],
                None => Vec::new(),
            },
            indexed_terms: Vec::new(),
            stat_winners: Vec::new(),
        })
        .collect();
    let plans = vec![ColumnPlan {
        column_id,
        ty: FieldType::Str,
    }];
    let block = write_block(&rows, &plans, cfg.zstd_level).expect("write one block");
    let skip = SkipIndex::build(vec![Level0Entry {
        block_offset: 0,
        block_len: block.bytes.len() as u64,
        block_crc32c: block.crc32c,
        record_count: block.record_count,
        min_ts: block.min_ts,
        max_ts: block.max_ts,
        min_stream_ref: block.min_stream_ref,
        max_stream_ref: block.max_stream_ref,
        stats: block.stats.clone(),
    }])
    .encode();

    let mut out: Vec<u8> = Vec::new();
    let mut sections: Vec<SectionDesc> = Vec::new();
    for d in &footer.sections {
        let (bytes, comp, uncomp_len) = match d.kind {
            kind::BLOCKS => (block.bytes.clone(), COMP_NONE, block.bytes.len() as u64),
            kind::SKIP_IDX => (skip.clone(), COMP_NONE, skip.len() as u64),
            _ => {
                let start = usize::try_from(d.offset).expect("offset fits");
                let end = start + usize::try_from(d.len).expect("len fits");
                (obj[start..end].to_vec(), d.comp, d.uncomp_len)
            }
        };
        sections.push(SectionDesc {
            kind: d.kind,
            offset: out.len() as u64,
            len: bytes.len() as u64,
            crc32c: crc32c::crc32c(&bytes),
            comp,
            uncomp_len,
        });
        out.extend_from_slice(&bytes);
    }
    let patched = LogFooter { sections, ..footer };
    write_footer_and_trailer(&mut out, &patched);
    out
}

/// The `Str`-typed FIELD_DIR column id for `key` in `obj`.
fn str_column_id(obj: &[u8], cfg: RlogConfig, key: &str) -> u32 {
    let footer = open_footer(obj).expect("open footer");
    let desc = *footer
        .section(kind::FIELD_DIR)
        .expect("a FIELD_DIR section");
    let bytes = read_section(obj, &desc, &cfg).expect("read FIELD_DIR");
    let dir = FieldDir::decode(&bytes, u64::MAX).expect("decode FIELD_DIR");
    dir.column(key, FieldType::Str)
        .expect("a Str column for the key")
        .column_id
}

/// A declared `Str` cell whose bytes are not UTF-8 means the record does not set
/// the key, so the resource value shows through -- on both paths.
///
/// The row path decides this in `ravel_logseg`'s `get_attr_value`
/// (`String::from_utf8(b).ok()`): the attribute never enters the record, so the
/// merged view resolves the key to the resource's value. The fast path used
/// `String::from_utf8_lossy`, which reported the cell present (suppressing that
/// fallback) and substituted U+FFFD -- two divergences and a silent
/// approximation on a read path, from one line.
///
/// Row 0 holds `[0xff]` in the declared column while the resource sets the same
/// key; row 1 holds a valid record value, so the fixture also shows the cell is
/// still read when it is readable.
#[tokio::test]
async fn a_declared_str_cell_that_is_not_utf8_reads_as_absent_on_both_paths() {
    let cfg = RlogConfig::default();
    let declared = vec![DeclaredColumn::new("name", DeclaredType::Str)];
    let resource = vec![("name".to_string(), AttrValue::Str("from-resource".into()))];
    let mk = |ts: i64, name: &str| LogRecord {
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
        attrs: vec![("name".to_string(), AttrValue::Str(name.to_string()))],
    };
    // The placeholder at row 0 is replaced by `[0xff]` below; row 1 keeps its
    // value, so both the readable and the unreadable case are in one block.
    let records = vec![mk(0, "placeholder"), mk(1, "from-record")];

    let clean = encode_object(&records, cfg);
    let column_id = str_column_id(&clean, cfg, "name");
    let patched = rewrite_str_column_cells(
        &clean,
        cfg,
        &records,
        column_id,
        &[Some(vec![0xff]), Some(b"from-record".to_vec())],
    );

    let store = MemoryStore::new();
    let seg = put_object(&store, "logs/badutf8.rlog", patched, 0, 1, records.len()).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let segments = vec![seg];

    let projection = vec![0usize, FIRST_DECLARED_COL];
    let want = vec![
        vec![Cell::Ts(0), Cell::OptStr(Some("from-resource".to_string()))],
        vec![Cell::Ts(1), Cell::OptStr(Some("from-record".to_string()))],
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
    let got = rows(&fast.batches, &projection, &declared);
    assert_eq!(
        got, want,
        "invalid UTF-8 in a declared Str cell must read as absent, letting the \
         resource value through, not as U+FFFD"
    );

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
        "the forced run must be the row path"
    );
    assert!(row.rowpath_batches > 0, "the row path must have run");
    assert_eq!(
        rows(&row.batches, &projection, &declared),
        want,
        "the row path's answer is the reference the fast path must match"
    );
}

/// The two-paths-one-schema invariant (ADR-0099 decision 5). An erasure-pending
/// query over a declared `Str` column takes the row path, and its batches must
/// carry the SAME `Dictionary(Int32, Utf8)` column type the fast path produces:
/// DataFusion validates every batch against one schema, so a row-path batch that
/// built a plain `Utf8` array would be rejected by `RecordBatch::try_new` at
/// runtime. This asserts the type on the returned batch and that the two paths'
/// batch schemas are identical, not merely that the scan did not error.
///
/// Reverting `declared_column_array`'s `Str` arm to a `StringBuilder`
/// (crates/ravel-sql/src/logs_scan.rs, the `DeclaredType::Str` arm) makes the
/// row-path scan fail to produce batches at all, failing this test.
#[tokio::test]
async fn fallback_batches_match_the_dictionary_schema() {
    let declared = declared_columns();
    let name_col = FIRST_DECLARED_COL + 1; // the Str-typed `name`
    let projection = vec![0usize, name_col];

    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let mk = |ts: i64, name: &str| LogRecord {
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
        attrs: vec![("name".to_string(), AttrValue::Str(name.to_string()))],
    };
    let records = vec![mk(0, "a"), mk(1, "b"), mk(2, "a")];

    let store = MemoryStore::new();
    let seg = write_object(&store, "logs/schema.rlog", &records, RlogConfig::default()).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let segments = vec![seg];

    let row = run_scan(
        Arc::clone(&store),
        segments.clone(),
        declared.clone(),
        Some(projection.clone()),
        Vec::new(),
        no_match_erasure(),
    )
    .await;
    assert_eq!(row.columnar_batches, 0, "erasure must force the row path");
    assert!(row.rowpath_batches > 0, "the row path must have run");

    let fast = run_scan(
        Arc::clone(&store),
        segments,
        declared.clone(),
        Some(projection.clone()),
        Vec::new(),
        Vec::new(),
    )
    .await;
    assert!(fast.columnar_batches > 0, "the fast path must have run");

    let dict_ty = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    for b in &row.batches {
        assert_eq!(
            b.schema().field(1).data_type(),
            &dict_ty,
            "row-path declared Str column must be dictionary-typed"
        );
    }
    assert_eq!(
        row.batches[0].schema(),
        fast.batches[0].schema(),
        "the row path and fast path must agree on the batch schema"
    );
    assert_eq!(
        rows(&row.batches, &projection, &declared),
        rows(&fast.batches, &projection, &declared),
        "both paths must read the same values"
    );
}

/// A dict-encoded page and a plain page both read back correct declared `Str`
/// values through the fast path (ADR-0099 decision 5). A page whose distinct
/// values are at most half its rows is dictionary-encoded (the writer's
/// distinct-ratio heuristic) and reaches Arrow as its dictionary plus ids; an
/// all-distinct page stays plain and takes the degenerate identity-dictionary
/// branch (`str_dict` returns `None`). Both must yield the exact strings.
#[tokio::test]
async fn dict_page_and_plain_page_both_read_correct_values() {
    let declared = declared_columns();
    let name_col = FIRST_DECLARED_COL + 1;
    let projection = vec![0usize, name_col];
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let mk = |ts: i64, name: &str| LogRecord {
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
        attrs: vec![("name".to_string(), AttrValue::Str(name.to_string()))],
    };

    // One block per object so the encoding decision is per whole corpus.
    let cfg = RlogConfig {
        block_target_records: 64,
        ..RlogConfig::default()
    };

    // Dict page: 6 rows, 2 distinct values (2*2 <= 6).
    let dict_records: Vec<LogRecord> = (0..6)
        .map(|i| mk(i, if i % 2 == 0 { "api" } else { "worker" }))
        .collect();
    // Plain page: 6 rows, all distinct (6*2 > 6).
    let plain_records: Vec<LogRecord> = (0..6).map(|i| mk(i, &format!("n{i}"))).collect();

    for (key, recs) in [
        ("logs/dictpage.rlog", &dict_records),
        ("logs/plainpage.rlog", &plain_records),
    ] {
        // Assert each fixture's declared `name` value set actually produces the
        // encoding its name claims. `encode_strings` is the exact codec the
        // writer stages a dynamic string column with, so this ties the fixture
        // to the live `dict_is_worth_it` heuristic: move that heuristic and the
        // "dict page" fixture becomes a plain page, failing here rather than
        // silently exercising the identity branch twice.
        let name_vals: Vec<&[u8]> = recs
            .iter()
            .map(|r| match &r.attrs[0].1 {
                AttrValue::Str(s) => s.as_bytes(),
                _ => unreachable!(),
            })
            .collect();
        let want_enc = if key == "logs/dictpage.rlog" {
            Enc::Dict
        } else {
            Enc::Plain
        };
        assert_eq!(
            encode_strings(&name_vals).0,
            want_enc,
            "fixture {key} must actually produce {want_enc:?}"
        );

        let store = MemoryStore::new();
        let seg = write_object(&store, key, recs, cfg).await;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
        let fast = run_scan(
            store,
            vec![seg],
            declared.clone(),
            Some(projection.clone()),
            Vec::new(),
            Vec::new(),
        )
        .await;
        assert!(
            fast.columnar_batches > 0,
            "fast path must have run for {key}"
        );
        assert_eq!(
            fast.rowpath_batches, 0,
            "fast path must not fall back for {key}"
        );

        let mut want: Vec<Vec<Cell>> = recs
            .iter()
            .map(|r| {
                let name = match &r.attrs[0].1 {
                    AttrValue::Str(s) => s.clone(),
                    _ => unreachable!(),
                };
                vec![Cell::Ts(r.ts_ns), Cell::OptStr(Some(name))]
            })
            .collect();
        want.sort();
        assert_eq!(
            rows(&fast.batches, &projection, &declared),
            want,
            "declared Str values must be correct for {key}"
        );
    }
}

/// The dict-page fast path with BOTH a resource/scope fallback append and a
/// non-UTF-8 page-dictionary entry in the same block (logs_scan.rs's
/// `next_extra` append and the `str::from_utf8` NULL arm). Neither is reached by
/// the existing coverage: `a_declared_str_cell_that_is_not_utf8_reads_as_absent`
/// uses 2 rows / 2 distinct, so `dict_is_worth_it(2, 2)` is false and it takes
/// the identity (plain-page) branch, and the differential proptest's
/// `small_blocks()` admits only a single-entry page dictionary.
///
/// The block: rows 0..=3 set the declared `name` to "api", row 4 to "worker",
/// row 5 to raw `[b'b', 0xff]` (a non-UTF-8 record cell, sorting between "api"
/// and "worker" in the page dictionary so a dropped-not-nulled entry would
/// misnumber a value that is actually used), rows 6..=7 do not set it at all. The record `name` column is present on rows 0..=5 (six values, three
/// distinct), so `dict_is_worth_it(3, 6)` holds and the page is `Enc::Dict` with
/// a non-UTF-8 entry. A non-UTF-8 record cell reads as absent (matching the row
/// path's `String::from_utf8().ok()`), so row 5 and the truly-absent rows 6..=7
/// all fall through to the resource value appended past the page dictionary.
#[tokio::test]
async fn dict_page_with_non_utf8_entry_and_resource_fallback_append() {
    let cfg = RlogConfig {
        block_target_records: 64,
        ..RlogConfig::default()
    };
    let declared = vec![DeclaredColumn::new("name", DeclaredType::Str)];
    let resource = vec![("name".to_string(), AttrValue::Str("from-resource".into()))];
    let mk = |ts: i64| LogRecord {
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
        // A record-level `name` on every record so the FIELD_DIR (copied verbatim
        // by the rewrite) carries the `Str` column; per-row presence is decided
        // by the rewrite's cell list below, not here.
        attrs: vec![("name".to_string(), AttrValue::Str("seed".to_string()))],
    };
    let records: Vec<LogRecord> = (0..8).map(mk).collect();

    let clean = encode_object(&records, cfg);
    let column_id = str_column_id(&clean, cfg, "name");

    // Present rows carry the page dictionary; `None` rows leave the column absent
    // so the resource value shows through the fallback append.
    let cells: Vec<Option<Vec<u8>>> = vec![
        Some(b"api".to_vec()),
        Some(b"api".to_vec()),
        Some(b"api".to_vec()),
        Some(b"api".to_vec()),
        Some(b"worker".to_vec()),
        // A non-UTF-8 cell whose bytes sort BETWEEN "api" and "worker" in the
        // page dictionary. The `Err(_) => append_null()` arm in logs_scan.rs
        // exists to keep Arrow dictionary indices aligned with the page's ids:
        // a non-UTF-8 entry must become a NULL SLOT, not be dropped. A value
        // that sorts last (e.g. `[0xff]`) shifts no in-use id when dropped, so
        // the test could not tell "null slot" from "dropped"; `[b'b', 0xff]`
        // sorts mid-dictionary, so skipping it would renumber "worker".
        Some(vec![b'b', 0xff]),
        None,
        None,
    ];
    // The page holds only the present cells; assert it truly encodes as a dict
    // page (three distinct of six present), with the non-UTF-8 byte string among
    // its entries.
    let present: Vec<&[u8]> = cells.iter().filter_map(|c| c.as_deref()).collect();
    assert_eq!(
        encode_strings(&present).0,
        Enc::Dict,
        "the present cells must encode as a dict page"
    );

    let patched = rewrite_str_column_cells(&clean, cfg, &records, column_id, &cells);

    let store = MemoryStore::new();
    let seg = put_object(
        &store,
        "logs/dictfallback.rlog",
        patched,
        0,
        7,
        records.len(),
    )
    .await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let segments = vec![seg];

    let projection = vec![0usize, FIRST_DECLARED_COL];
    let want = vec![
        vec![Cell::Ts(0), Cell::OptStr(Some("api".to_string()))],
        vec![Cell::Ts(1), Cell::OptStr(Some("api".to_string()))],
        vec![Cell::Ts(2), Cell::OptStr(Some("api".to_string()))],
        vec![Cell::Ts(3), Cell::OptStr(Some("api".to_string()))],
        vec![Cell::Ts(4), Cell::OptStr(Some("worker".to_string()))],
        // Row 5's [0xff] cell reads as absent -> resource shows through.
        vec![Cell::Ts(5), Cell::OptStr(Some("from-resource".to_string()))],
        vec![Cell::Ts(6), Cell::OptStr(Some("from-resource".to_string()))],
        vec![Cell::Ts(7), Cell::OptStr(Some("from-resource".to_string()))],
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
    let mut got = rows(&fast.batches, &projection, &declared);
    got.sort();
    let mut want_sorted = want.clone();
    want_sorted.sort();
    assert_eq!(
        got, want_sorted,
        "dict-id rows, the non-UTF-8 cell, and the fallback-append rows must all \
         read correctly on the fast path"
    );

    // The row path over the same object is the reference: it must agree.
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
        "the forced run must be the row path"
    );
    assert!(row.rowpath_batches > 0, "the row path must have run");
    let mut row_got = rows(&row.batches, &projection, &declared);
    row_got.sort();
    assert_eq!(
        row_got, want_sorted,
        "the row path's answer is the reference the fast path must match"
    );
}
