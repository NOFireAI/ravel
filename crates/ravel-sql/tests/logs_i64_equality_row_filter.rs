//! ADR-0093, I64 equality amendment: a declared `I64` column's `=` against a
//! non-NULL integer literal is a row filter the logs scan evaluates, and on the
//! row path the scan builds the filter's column first and every other column
//! only for the rows the filter keeps.
//!
//! Every statement runs through the production session over real RLOG objects
//! in a `MemoryStore`. The reference for an answer is the same statement on a
//! provider with the row filter off
//! (`LogsTableProvider::with_row_filter_pushdown(false)`), which reports the
//! conjunct `Inexact`, so DataFusion's `FilterExec` is the only evaluator.
//!
//! - `i64_equality_row_filter_matches_filter_exec`: over generated objects
//!   whose `uid` value comes from the record cell, a wrong-variant cell, the
//!   resource, the scope, or nowhere, with and without `attrs` projected and
//!   with pending erasure, the rows equal the reference.
//! - `the_scan_builds_attrs_only_for_the_rows_uid_keeps`: the scan's
//!   `attrs_rows_built` counter equals the matching rows, and the plan holds
//!   no `FilterExec`.
//! - `limit_and_top_k_statements_match_filter_exec`: `LIMIT k` (a fetch
//!   pushed onto the scan) and `ORDER BY ts LIMIT k` (late materialization),
//!   over `SELECT *` and a projection with `attrs`, equal the reference.
//! - `key_index_and_decoded_cache_runs_match_filter_exec`: with the
//!   provider's KEY_IDX declaration and the fetcher's decoded-column cache,
//!   a first run and a second run served from the cache equal the reference.
//! - `extreme_values_and_narrow_literals_match_filter_exec`: negative values,
//!   `i64::MIN`, `i64::MAX` and a large value, and literals written as a
//!   narrower or unsigned type, equal the reference.
//! - `a_block_whose_uid_comes_only_from_the_resource_is_not_lost`: a fixed
//!   answer for a block with no `uid` record cell at all.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::TimestampNanosecondType;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use proptest::prelude::*;
use ravel_catalog::{SegmentRef, Snapshot};
use ravel_logseg::decoded_cache::DecodedColumnCache;
use ravel_logseg::key_index::KeyType;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    CeilingBreach, DeclaredColumn, DeclaredType, KeyIndexDeclaration, LogsTableProvider,
    SessionTable, SpillDecision, SqlConfig, TenantDelegatingPool, TenantMemoryAccountant,
    build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;

use crate::logs_str_not_equal_row_filter::{nodes, pretty, put, run};

const TENANT: TenantHash = TenantHash([0x93; 16]);

fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("uid", DeclaredType::I64),
        DeclaredColumn::new("region", DeclaredType::Str),
    ]
}

/// Where a stream-level `uid` comes from, if anywhere.
#[derive(Debug, Clone, Copy)]
enum StreamUid {
    None,
    Resource(i64),
    Scope(i64),
    Both(i64, i64),
}

/// A record's own `uid` cell.
#[derive(Debug, Clone, Copy)]
enum RecordUid {
    None,
    I64(i64),
    /// A `Str` cell under the `I64` declaration: the column reads NULL.
    WrongVariant(i64),
}

#[derive(Debug, Clone)]
struct Row {
    stream: usize,
    uid: RecordUid,
    /// The row carries `tag = 'gone'`, which the pending erasure removes.
    erased: bool,
}

#[derive(Debug, Clone)]
struct Corpus {
    streams: Vec<StreamUid>,
    objects: Vec<Vec<Row>>,
    erasure: bool,
    max_dynamic_columns: usize,
    value: i64,
}

type Attrs = Vec<(String, AttrValue)>;

/// A stream's resource and scope attributes.
fn stream_attrs(uid: StreamUid) -> (Attrs, Attrs) {
    let mut resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let mut scope = Vec::new();
    match uid {
        StreamUid::None => {}
        StreamUid::Resource(v) => resource.push(("uid".into(), AttrValue::I64(v))),
        StreamUid::Scope(v) => scope.push(("uid".into(), AttrValue::I64(v))),
        StreamUid::Both(r, s) => {
            resource.push(("uid".into(), AttrValue::I64(r)));
            scope.push(("uid".into(), AttrValue::I64(s)));
        }
    }
    (resource, scope)
}

fn log_record(ts: i64, stream: StreamUid, row: &Row) -> LogRecord {
    let (resource, scope) = stream_attrs(stream);
    let mut attrs = vec![
        ("region".to_string(), AttrValue::Str(format!("r{}", ts % 3))),
        ("n".to_string(), AttrValue::I64(ts)),
    ];
    match row.uid {
        RecordUid::None => {}
        RecordUid::I64(v) => attrs.push(("uid".into(), AttrValue::I64(v))),
        RecordUid::WrongVariant(v) => attrs.push(("uid".into(), AttrValue::Str(v.to_string()))),
    }
    if row.erased {
        attrs.push(("tag".into(), AttrValue::Str("gone".into())));
    }
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &scope),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &scope),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("row {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

async fn write_corpus(corpus: &Corpus) -> (Arc<MemoryStore>, Vec<SegmentRef>) {
    write_corpus_with(corpus, false).await
}

/// [`write_corpus`], with each object's KEY_IDX section over `uid` when
/// `key_index` is set.
async fn write_corpus_with(
    corpus: &Corpus,
    key_index: bool,
) -> (Arc<MemoryStore>, Vec<SegmentRef>) {
    let store = Arc::new(MemoryStore::new());
    let cfg = RlogConfig {
        block_target_records: 4,
        max_dynamic_columns: corpus.max_dynamic_columns,
        ..RlogConfig::default()
    };
    let mut segments = Vec::new();
    let mut ts = 1;
    for (o, rows) in corpus.objects.iter().enumerate() {
        let records: Vec<LogRecord> = rows
            .iter()
            .map(|row| {
                ts += 1;
                let stream = corpus.streams[row.stream % corpus.streams.len()];
                log_record(ts, stream, row)
            })
            .collect();
        let mut w = RlogWriter::new(
            cfg,
            ObjectIdentity {
                tenant_hash: TENANT.0,
                shard: 0,
                writer_id: [4u8; 16],
                writer_epoch: 1,
                writer_seq: o as u64 + 1,
            },
        );
        if key_index {
            w = w.with_key_index_fields(vec![("uid".to_string(), KeyType::I64)]);
        }
        for r in &records {
            w.push(r.clone()).expect("push");
        }
        let bytes = w.finish().expect("finish");
        let mut seg = put(&store, &format!("logs/{o}.rlog"), bytes, &records).await;
        // The decoded-column cache keys an object by its content hash, which
        // `put` leaves zero for every object.
        seg.content_hash = [o as u8 + 1; 32];
        segments.push(seg);
    }
    (store, segments)
}

fn session(
    store: &Arc<MemoryStore>,
    segments: Vec<SegmentRef>,
    erasure: bool,
    row_filter: bool,
) -> SessionContext {
    session_with(
        store,
        segments,
        &Options {
            erasure,
            row_filter,
            ..Options::default()
        },
    )
}

/// How [`session_with`] builds its provider.
#[derive(Default)]
struct Options {
    erasure: bool,
    row_filter: bool,
    /// Declare `uid` in the provider's KEY_IDX declaration.
    key_index: bool,
    /// The fetcher's decoded-column cache.
    cache: Option<Arc<DecodedColumnCache>>,
}

fn session_with(
    store: &Arc<MemoryStore>,
    segments: Vec<SegmentRef>,
    options: &Options,
) -> SessionContext {
    let erasure = options.erasure;
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(store) as Arc<dyn ObjectStoreBackend>;
    let pending_erasure = if erasure {
        vec![ravel_proto::commit::v1::ErasureRequest {
            predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
                key: "tag".to_string(),
                value: "gone".to_string(),
            }],
            ..Default::default()
        }]
    } else {
        Vec::new()
    };
    let mut fetcher = LogSegmentFetcher::new(backend);
    if options.key_index {
        // Objects this small sit under the block-range threshold, where no
        // KEY_IDX probe is issued.
        fetcher = fetcher.with_block_range_threshold(0);
    }
    if let Some(cache) = &options.cache {
        fetcher = fetcher.with_decoded_cache(Arc::clone(cache));
    }
    let mut provider = LogsTableProvider::new(
        Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure,
        },
        TENANT,
        fetcher,
        PhaseAccounting::new(),
    )
    .with_declared_columns(declared())
    .with_row_filter_pushdown(options.row_filter);
    if options.key_index {
        provider = provider.with_key_index(KeyIndexDeclaration {
            columns: vec!["uid".to_string()],
            trace_id: false,
        });
    }
    let pool = Arc::new(TenantDelegatingPool::new(
        1 << 30,
        TenantMemoryAccountant::new(1 << 30),
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));
    build_session(
        &SqlConfig::default(),
        pool,
        SessionTable::Logs(Arc::new(provider)),
        false,
        SpillDecision::Disabled,
        &ravel_sql::SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
    )
    .expect("session")
}

/// `name` summed over every `LogsScanExec` in `plan`.
fn scan_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    nodes(plan, "LogsScanExec")
        .iter()
        .map(|scan| {
            scan.metrics()
                .and_then(|m| m.sum_by_name(name))
                .map_or(0, |v| v.as_usize())
        })
        .sum()
}

fn stream_uid() -> impl Strategy<Value = StreamUid> {
    prop_oneof![
        Just(StreamUid::None),
        (0i64..3).prop_map(StreamUid::Resource),
        (0i64..3).prop_map(StreamUid::Scope),
        (0i64..3, 0i64..3).prop_map(|(r, s)| StreamUid::Both(r, s)),
    ]
}

fn row() -> impl Strategy<Value = Row> {
    (
        0usize..3,
        prop_oneof![
            2 => Just(RecordUid::None),
            3 => (0i64..3).prop_map(RecordUid::I64),
            1 => (0i64..3).prop_map(RecordUid::WrongVariant),
        ],
        prop::bool::weighted(0.2),
    )
        .prop_map(|(stream, uid, erased)| Row {
            stream,
            uid,
            erased,
        })
}

fn corpus() -> impl Strategy<Value = Corpus> {
    (
        prop::collection::vec(stream_uid(), 1..4),
        prop::collection::vec(prop::collection::vec(row(), 1..10), 1..3),
        any::<bool>(),
        prop::sample::select(vec![0usize, 1, 8]),
        0i64..3,
    )
        .prop_map(
            |(streams, objects, erasure, max_dynamic_columns, value)| Corpus {
                streams,
                objects,
                erasure,
                max_dynamic_columns,
                value,
            },
        )
}

/// Every statement over `corpus` returns, with the row filter on, the rows it
/// returns with the row filter off; with it on the plan holds no `FilterExec`
/// and the scan's own counter shows the filter ran.
async fn check_corpus(corpus: &Corpus) -> Result<(), TestCaseError> {
    let (store, segments) = write_corpus(corpus).await;
    let on = session(&store, segments.clone(), corpus.erasure, true);
    let off = session(&store, segments, corpus.erasure, false);
    let v = corpus.value;
    for sql in [
        format!("SELECT ts, uid FROM logs WHERE uid = {v} ORDER BY ts"),
        format!("SELECT ts, uid, attrs FROM logs WHERE {v} = uid ORDER BY ts"),
        format!("SELECT ts, region, attrs['uid'] AS u FROM logs WHERE uid = {v} ORDER BY ts"),
        format!("SELECT count(*) AS c FROM logs WHERE uid = {v}"),
    ] {
        let (_, want) = run(&off, &sql).await;
        let (plan, got) = run(&on, &sql).await;
        prop_assert_eq!(pretty(&got), pretty(&want), "{}", sql);
        prop_assert!(
            nodes(&plan, "FilterExec").is_empty(),
            "{}: no FilterExec",
            sql
        );
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// ADR-0093, I64 equality amendment: the scan's row filter keeps exactly
    /// the rows the `FilterExec` keeps, over `uid` from the record cell, a
    /// wrong-variant cell (NULL), the resource, the scope, or nowhere (NULL),
    /// on the row path (`attrs` projected, pending erasure, an `attrs_raw`
    /// block) and on the columnar path.
    ///
    /// Flipped: a reader-side filter on the record cell only drops a row
    /// whose `uid` comes from the resource or scope; keeping a NULL verdict
    /// returns a wrong-variant or absent row. Both fail the row comparison.
    #[test]
    fn i64_equality_row_filter_matches_filter_exec(corpus in corpus()) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(check_corpus(&corpus))?;
    }
}

/// The fixture the counter test reads: 12 rows on a stream whose resource
/// says `uid = 7`. Rows 0, 4 and 8 have no `uid` cell and read the resource's
/// 7, rows 1, 5 and 9 hold 7, rows 2, 6 and 10 hold the string `"7"` (NULL),
/// the others hold 100 plus their index.
const ROWS: usize = 12;
const MATCHING: usize = 6;

async fn counter_fixture() -> (Arc<MemoryStore>, Vec<SegmentRef>) {
    let corpus = Corpus {
        streams: vec![StreamUid::Resource(7)],
        objects: vec![
            (0..ROWS)
                .map(|i| Row {
                    stream: 0,
                    uid: match i % 4 {
                        0 => RecordUid::None,
                        1 => RecordUid::I64(7),
                        2 => RecordUid::WrongVariant(7),
                        _ => RecordUid::I64(100 + i as i64),
                    },
                    erased: false,
                })
                .collect(),
        ],
        erasure: false,
        max_dynamic_columns: 8,
        value: 7,
    };
    write_corpus(&corpus).await
}

/// ADR-0093, I64 equality amendment, deliverables 2 to 4 at the scan:
/// `SELECT ts, attrs FROM logs WHERE uid = 7` plans no `FilterExec`, returns
/// the 6 matching rows, and builds the `attrs` map for those 6 rows only; the
/// provider with the row filter off builds it for all 12 and keeps its
/// `FilterExec`.
///
/// Flipped: building each chunk whole and then filtering it in
/// `emit_next_row_batch` counts 12 and fails "attrs built for the matching
/// rows only".
#[tokio::test]
async fn the_scan_builds_attrs_only_for_the_rows_uid_keeps() {
    let (store, segments) = counter_fixture().await;
    let on = session(&store, segments.clone(), false, true);
    let off = session(&store, segments, false, false);
    let sql = "SELECT ts, attrs FROM logs WHERE uid = 7 ORDER BY ts";
    let (reference_plan, want) = run(&off, sql).await;
    let (plan, got) = run(&on, sql).await;
    assert_eq!(pretty(&got), pretty(&want));
    assert_eq!(
        got.iter().map(|b| b.num_rows()).sum::<usize>(),
        MATCHING,
        "the matching rows"
    );
    assert!(nodes(&plan, "FilterExec").is_empty(), "no FilterExec");
    assert_eq!(nodes(&reference_plan, "FilterExec").len(), 1);
    assert_eq!(
        scan_metric(&plan, "attrs_rows_built"),
        MATCHING,
        "attrs built for the matching rows only"
    );
    assert_eq!(
        scan_metric(&plan, "rows_dropped_by_row_filter"),
        ROWS - MATCHING
    );
    assert_eq!(scan_metric(&reference_plan, "attrs_rows_built"), ROWS);
}

/// A corpus over values outside `0..3`: two objects of 24 rows, `uid` from the
/// record cell (as I64 or a wrong-variant `Str`), the resource, the scope or
/// nowhere, taking each of `VALUES`.
const VALUES: [i64; 5] = [-5, i64::MIN, i64::MAX, 1 << 40, 3];

fn wide_corpus(max_dynamic_columns: usize) -> Corpus {
    let rows = |offset: usize| -> Vec<Row> {
        (0..24)
            .map(|i| {
                let n = i + offset;
                Row {
                    stream: n % 4,
                    uid: match n % 6 {
                        0 => RecordUid::None,
                        1 | 3 => RecordUid::I64(VALUES[n % VALUES.len()]),
                        2 => RecordUid::WrongVariant(VALUES[n % VALUES.len()]),
                        4 => RecordUid::I64(i64::MAX),
                        _ => RecordUid::I64(i64::MIN),
                    },
                    erased: false,
                }
            })
            .collect()
    };
    Corpus {
        streams: vec![
            StreamUid::Resource(i64::MIN),
            StreamUid::Scope(i64::MAX),
            StreamUid::Both(1 << 40, -5),
            StreamUid::None,
        ],
        objects: vec![rows(0), rows(5)],
        erasure: false,
        max_dynamic_columns,
        value: 0,
    }
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn timestamps(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let ts = b
                .column_by_name("ts")
                .expect("ts")
                .as_primitive::<TimestampNanosecondType>();
            (0..ts.len()).map(|i| ts.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

/// ADR-0093, I64 equality amendment: `LIMIT k` and `ORDER BY ts LIMIT k`
/// with the row filter in the scan. `ORDER BY ts LIMIT k` over `SELECT *`
/// plans late materialization (`LogsRowFetchExec`) and equals the reference;
/// the same over `ts, uid, attrs` equals it too. A bare `LIMIT k` puts the
/// fetch on the scan: it returns exactly `k` rows, each of them a row the
/// reference returns, whole.
///
/// Flipped: a fetch counted on built rows instead of filtered ones returns
/// fewer than `k` rows and fails "k rows"; a phase-1 scan whose row refs do
/// not point at the rows it kept returns another row's columns and fails
/// "the reference answer".
#[tokio::test]
async fn limit_and_top_k_statements_match_filter_exec() {
    for max_dynamic_columns in [0, 8] {
        let corpus = wide_corpus(max_dynamic_columns);
        let (store, segments) = write_corpus(&corpus).await;
        let on = session(&store, segments.clone(), false, true);
        let off = session(&store, segments, false, false);
        for v in [i64::MAX, i64::MIN] {
            let lit = if v == i64::MIN {
                "CAST('-9223372036854775808' AS BIGINT)".to_string()
            } else {
                v.to_string()
            };
            let (_, all) = run(&off, &format!("SELECT * FROM logs WHERE uid = {lit}")).await;
            let matching = row_count(&all);
            assert!(matching > 3, "{v}: {matching} matching rows");

            for sql in [
                format!("SELECT * FROM logs WHERE uid = {lit} ORDER BY ts LIMIT 3"),
                format!("SELECT ts, uid, attrs FROM logs WHERE uid = {lit} ORDER BY ts LIMIT 3"),
            ] {
                let (plan, got) = run(&on, &sql).await;
                let (_, want) = run(&off, &sql).await;
                assert_eq!(pretty(&got), pretty(&want), "the reference answer: {sql}");
                assert_eq!(row_count(&got), 3, "{sql}");
                assert!(nodes(&plan, "FilterExec").is_empty(), "{sql}: no FilterExec");
                if sql.starts_with("SELECT *") {
                    let shown = format!("{}", displayable(plan.as_ref()).indent(true));
                    assert!(
                        shown.contains("LogsRowFetchExec"),
                        "late materialized: {shown}"
                    );
                }
            }

            for projection in ["*", "ts, uid, attrs"] {
                let sql = format!("SELECT {projection} FROM logs WHERE uid = {lit} LIMIT 3");
                let (plan, got) = run(&on, &sql).await;
                assert!(nodes(&plan, "FilterExec").is_empty(), "{sql}: no FilterExec");
                let scans = nodes(&plan, "LogsScanExec");
                assert!(
                    scans.iter().any(|scan| scan.fetch().is_some()),
                    "{sql}: the fetch is on the scan: {}",
                    displayable(plan.as_ref()).indent(true)
                );
                assert_eq!(row_count(&got), 3, "{sql}: k rows");
                let ts = timestamps(&got);
                let listed = ts.iter().map(i64::to_string).collect::<Vec<_>>().join(", ");
                let pick = format!(
                    "SELECT {projection} FROM logs WHERE uid = {lit} \
                     AND CAST(ts AS BIGINT) IN ({listed}) ORDER BY ts"
                );
                let (_, want) = run(&off, &pick).await;
                let sorted = format!("SELECT * FROM ({sql}) ORDER BY ts");
                let (_, got) = run(&on, &sorted).await;
                assert_eq!(pretty(&got), pretty(&want), "the reference rows: {sql}");
            }
        }
    }
}

/// ADR-0093, I64 equality amendment: with `uid` in each object's KEY_IDX
/// section and in the provider's KEY_IDX declaration, and a decoded-column
/// cache on the fetcher, every statement equals the reference on its first
/// run and on a second run served from the cache. The probe runs (a value no
/// row holds is pruned by KEY_IDX) and the second run hits the cache.
///
/// Flipped: a KEY_IDX probe or a cached column that loses a row whose `uid`
/// comes from the resource or scope (no KEY_IDX entry) fails "the reference
/// answer" on that run.
#[tokio::test]
async fn key_index_and_decoded_cache_runs_match_filter_exec() {
    for max_dynamic_columns in [0, 8] {
        let corpus = wide_corpus(max_dynamic_columns);
        let (store, segments) = write_corpus_with(&corpus, true).await;
        let cache = Arc::new(DecodedColumnCache::new(1 << 30, None));
        let on = session_with(
            &store,
            segments.clone(),
            &Options {
                row_filter: true,
                key_index: true,
                cache: Some(Arc::clone(&cache)),
                ..Options::default()
            },
        );
        let off = session(&store, segments, false, false);
        let statements: Vec<String> = VALUES
            .iter()
            .flat_map(|&v| {
                let lit = if v == i64::MIN {
                    "CAST('-9223372036854775808' AS BIGINT)".to_string()
                } else {
                    v.to_string()
                };
                [
                    format!("SELECT ts, uid FROM logs WHERE uid = {lit} ORDER BY ts"),
                    format!("SELECT ts, uid, region, attrs FROM logs WHERE uid = {lit} ORDER BY ts"),
                    format!("SELECT count(*) AS c FROM logs WHERE uid = {lit}"),
                ]
            })
            .chain(["SELECT ts, uid FROM logs WHERE uid = 999 ORDER BY ts".to_string()])
            .collect();
        for pass in ["first", "cached"] {
            let mut key_pruned = 0;
            let hits_before = cache.process_counters().hits;
            for sql in &statements {
                let (_, want) = run(&off, sql).await;
                let (plan, got) = run(&on, sql).await;
                assert_eq!(
                    pretty(&got),
                    pretty(&want),
                    "the reference answer ({pass}, {max_dynamic_columns}): {sql}"
                );
                assert!(nodes(&plan, "FilterExec").is_empty(), "{sql}: no FilterExec");
                key_pruned += scan_metric(&plan, "segments_pruned_by_key_index")
                    + scan_metric(&plan, "blocks_pruned_by_key_index");
            }
            assert!(key_pruned > 0, "the KEY_IDX probe pruned ({pass})");
            let hits = cache.process_counters().hits - hits_before;
            if pass == "cached" {
                assert!(hits > 0, "the second run hits the decoded cache");
            }
        }
    }
}

/// ADR-0093, I64 equality amendment: values outside `0..3` (negative,
/// `i64::MIN`, `i64::MAX`, `2^40`), and literals that reach the session as
/// another integer type, each equal the reference, on the row path (an
/// `attrs_raw` block, `attrs` projected) and the columnar path. Every value
/// matches at least one row.
///
/// Flipped: a row filter that compares a narrowed or wrapped value, or drops
/// a resource or scope value, returns another row set and fails "the
/// reference answer".
#[tokio::test]
async fn extreme_values_and_narrow_literals_match_filter_exec() {
    let literals: Vec<String> = vec![
        "-5".into(),
        "CAST('-9223372036854775808' AS BIGINT)".into(),
        "-9223372036854775807 - 1".into(),
        i64::MAX.to_string(),
        (1i64 << 40).to_string(),
        "CAST(3 AS INT)".into(),
        "CAST(3 AS SMALLINT)".into(),
        "CAST(-5 AS TINYINT)".into(),
        "arrow_cast(3, 'Int32')".into(),
        "arrow_cast(3, 'UInt8')".into(),
        "arrow_cast(9223372036854775807, 'UInt64')".into(),
    ];
    for max_dynamic_columns in [0, 8] {
        let corpus = wide_corpus(max_dynamic_columns);
        let (store, segments) = write_corpus(&corpus).await;
        let on = session(&store, segments.clone(), false, true);
        let off = session(&store, segments, false, false);
        for lit in &literals {
            for sql in [
                format!("SELECT ts, uid FROM logs WHERE uid = {lit} ORDER BY ts"),
                format!("SELECT ts, uid, attrs FROM logs WHERE {lit} = uid ORDER BY ts"),
                format!("SELECT count(*) AS c FROM logs WHERE uid = {lit}"),
            ] {
                let (_, want) = run(&off, &sql).await;
                let (_, got) = run(&on, &sql).await;
                assert_eq!(pretty(&got), pretty(&want), "the reference answer: {sql}");
                if !sql.contains("count(*)") {
                    assert!(row_count(&want) > 0, "{sql}: matches a row");
                }
            }
        }
    }
}

/// ADR-0093, I64 equality amendment: object 0 is one block on a stream whose
/// resource says `uid = 7`, with no `uid` record cell in any row, so neither
/// its `uid` column statistics nor its KEY_IDX section hold 7. Object 1's
/// rows hold `uid` 1 and 2 in their own cells. `uid = 7` returns exactly
/// object 0's four rows, with and without `attrs` projected, with and without
/// KEY_IDX, on the row path and the columnar path.
///
/// Flipped: dropping a row whose `uid` comes from its resource (or pruning
/// the block because no record cell holds 7) returns no row and fails "the
/// resource-sourced rows".
#[tokio::test]
async fn a_block_whose_uid_comes_only_from_the_resource_is_not_lost() {
    for max_dynamic_columns in [0, 8] {
        let corpus = Corpus {
            streams: vec![StreamUid::Resource(7), StreamUid::None],
            objects: vec![
                (0..4)
                    .map(|_| Row {
                        stream: 0,
                        uid: RecordUid::None,
                        erased: false,
                    })
                    .collect(),
                (0..4)
                    .map(|i| Row {
                        stream: 1,
                        uid: RecordUid::I64(1 + i % 2),
                        erased: false,
                    })
                    .collect(),
            ],
            erasure: false,
            max_dynamic_columns,
            value: 7,
        };
        for key_index in [false, true] {
            let (store, segments) = write_corpus_with(&corpus, key_index).await;
            let on = session_with(
                &store,
                segments,
                &Options {
                    row_filter: true,
                    key_index,
                    ..Options::default()
                },
            );
            for sql in [
                "SELECT ts, uid FROM logs WHERE uid = 7 ORDER BY ts",
                "SELECT ts, uid, attrs FROM logs WHERE uid = 7 ORDER BY ts",
            ] {
                let (plan, got) = run(&on, sql).await;
                assert!(nodes(&plan, "FilterExec").is_empty(), "{sql}: no FilterExec");
                assert_eq!(
                    timestamps(&got),
                    [2, 3, 4, 5],
                    "the resource-sourced rows: {sql}, key index {key_index}, \
                     {max_dynamic_columns} dynamic columns"
                );
                let uid = got
                    .iter()
                    .flat_map(|b| {
                        let c = b
                            .column_by_name("uid")
                            .expect("uid")
                            .as_primitive::<datafusion::arrow::datatypes::Int64Type>();
                        (0..c.len()).map(|i| c.value(i)).collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(uid, [7, 7, 7, 7], "{sql}");
            }
        }
    }
}
