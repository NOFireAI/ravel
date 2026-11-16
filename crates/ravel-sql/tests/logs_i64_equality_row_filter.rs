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
//! - `limit_under_the_row_filter_matches_filter_exec`: `LIMIT k` and
//!   `ORDER BY ts LIMIT k`, for `SELECT *` and an `attrs` projection.
//! - `the_row_filter_matches_filter_exec_with_key_index_and_decoded_cache`:
//!   the KEY_IDX probe and the decoded column cache, each statement run twice.
//! - `i64_extreme_and_narrow_literals_match_filter_exec`: negative,
//!   `i64::MIN`, `i64::MAX`, large and `Int32`-fitting values, and literals
//!   typed narrower than `Int64`.
//! - `a_resource_sourced_uid_matches_by_a_fixed_answer`: a fixed expected
//!   answer for rows whose `uid` comes only from the resource.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::physical_plan::ExecutionPlan;
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
    write_corpus_keyed(corpus, false).await
}

/// `keyed` writes a KEY_IDX over `uid`.
async fn write_corpus_keyed(corpus: &Corpus, keyed: bool) -> (Arc<MemoryStore>, Vec<SegmentRef>) {
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
        if keyed {
            w = w.with_key_index_fields(vec![("uid".to_string(), KeyType::I64)]);
        }
        for r in &records {
            w.push(r.clone()).expect("push");
        }
        let bytes = w.finish().expect("finish");
        segments.push(put(&store, &format!("logs/{o}.rlog"), bytes, &records).await);
    }
    (store, segments)
}

fn session(
    store: &Arc<MemoryStore>,
    segments: Vec<SegmentRef>,
    erasure: bool,
    row_filter: bool,
) -> SessionContext {
    session_with(store, segments, erasure, row_filter, false, None)
}

/// `key_index` declares `uid`'s KEY_IDX to the provider; `cache` gives the
/// fetcher a decoded column cache.
fn session_with(
    store: &Arc<MemoryStore>,
    segments: Vec<SegmentRef>,
    erasure: bool,
    row_filter: bool,
    key_index: bool,
    cache: Option<Arc<DecodedColumnCache>>,
) -> SessionContext {
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
    if key_index {
        // The KEY_IDX probe reads only objects above the block-range threshold.
        fetcher = fetcher.with_block_range_threshold(0);
    }
    if let Some(cache) = cache {
        fetcher = fetcher.with_decoded_cache(cache);
    }
    let provider = LogsTableProvider::new(
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
    .with_row_filter_pushdown(row_filter);
    let provider = if key_index {
        provider.with_key_index(KeyIndexDeclaration {
            columns: vec!["uid".to_string()],
            trace_id: false,
        })
    } else {
        provider
    };
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

/// Each row of `batches` as one line of its cells' display text.
fn row_texts(batches: &[RecordBatch]) -> Vec<String> {
    batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(move |r| {
                b.columns()
                    .iter()
                    .map(|c| array_value_to_string(c, r).expect("display"))
                    .collect::<Vec<_>>()
                    .join(" | ")
            })
        })
        .collect()
}

/// Two objects: the first is `counter_fixture`'s 12 rows, 6 of which match
/// `uid = 7`; the second's 8 rows hold `uid` 0 or 201 to 207 on a stream with
/// no `uid`, so each of its blocks spans 7 and holds no 7. `max_dynamic_columns`
/// 0 puts every attribute in `attrs_raw`.
fn two_object_corpus(max_dynamic_columns: usize) -> Corpus {
    let first = (0..ROWS)
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
        .collect();
    let second = (0..8)
        .map(|i| Row {
            stream: 1,
            uid: RecordUid::I64(if i % 2 == 0 { 0 } else { 200 + i }),
            erased: false,
        })
        .collect();
    Corpus {
        streams: vec![StreamUid::Resource(7), StreamUid::None],
        objects: vec![first, second],
        erasure: false,
        max_dynamic_columns,
        value: 7,
    }
}

/// `LIMIT k` and `ORDER BY ts LIMIT k` under the row filter, for `SELECT *`
/// and for a projection that reads `attrs`, on the columnar and the
/// `attrs_raw` row path. With an order the rows equal the reference's; without
/// one the scan may return any k matching rows, so the test asserts the count
/// and that every row is one of the reference's matching rows.
///
/// Flipped: counting the limit before the row filter returns fewer than k
/// rows, or a non-matching row, and fails the count or the membership check.
#[tokio::test]
async fn limit_under_the_row_filter_matches_filter_exec() {
    for max_dynamic_columns in [8, 0] {
        let (store, segments) = write_corpus(&two_object_corpus(max_dynamic_columns)).await;
        let on = session(&store, segments.clone(), false, true);
        let off = session(&store, segments, false, false);
        for select in ["*", "ts, uid, attrs, attrs['n'] AS n"] {
            let (_, all) = run(
                &off,
                &format!("SELECT {select} FROM logs WHERE uid = 7 ORDER BY ts"),
            )
            .await;
            let all = row_texts(&all);
            assert_eq!(all.len(), MATCHING);
            for k in [1, 4, MATCHING, MATCHING + 3] {
                let ordered =
                    format!("SELECT {select} FROM logs WHERE uid = 7 ORDER BY ts LIMIT {k}");
                let (_, want) = run(&off, &ordered).await;
                let (plan, got) = run(&on, &ordered).await;
                assert_eq!(
                    pretty(&got),
                    pretty(&want),
                    "{ordered} ({max_dynamic_columns})"
                );
                assert!(
                    nodes(&plan, "FilterExec").is_empty(),
                    "{ordered}: no FilterExec"
                );

                let limited = format!("SELECT {select} FROM logs WHERE uid = 7 LIMIT {k}");
                let (plan, got) = run(&on, &limited).await;
                let got = row_texts(&got);
                assert_eq!(
                    got.len(),
                    k.min(MATCHING),
                    "{limited} ({max_dynamic_columns})"
                );
                for row in &got {
                    assert!(all.contains(row), "{limited}: {row} is a matching row");
                }
                let mut distinct = got.clone();
                distinct.sort();
                distinct.dedup();
                assert_eq!(distinct.len(), got.len(), "{limited}: no row twice");
                assert!(
                    nodes(&plan, "FilterExec").is_empty(),
                    "{limited}: no FilterExec"
                );
            }
        }
    }
}

/// The row filter beside the KEY_IDX probe and the fetcher's decoded column
/// cache: each statement runs twice on one provider holding both, and both runs
/// equal the reference, a provider with neither and the row filter off. The
/// probe prunes the second object and the second run is served from
/// the cache.
///
/// Flipped: a cached column served for the wrong block, or a pruned block's
/// rows kept, changes the rows and fails the comparison.
#[tokio::test]
async fn the_row_filter_matches_filter_exec_with_key_index_and_decoded_cache() {
    for max_dynamic_columns in [8, 0] {
        let (store, segments) =
            write_corpus_keyed(&two_object_corpus(max_dynamic_columns), true).await;
        let cache = Arc::new(DecodedColumnCache::new(1 << 30, None));
        let on = session_with(
            &store,
            segments.clone(),
            false,
            true,
            true,
            Some(Arc::clone(&cache)),
        );
        let off = session(&store, segments, false, false);
        let mut pruned = 0;
        for sql in [
            "SELECT ts, uid FROM logs WHERE uid = 7 ORDER BY ts",
            "SELECT ts, uid, region FROM logs WHERE uid = 7 ORDER BY ts",
            "SELECT * FROM logs WHERE uid = 7 ORDER BY ts",
            "SELECT count(*) AS c FROM logs WHERE uid = 7",
        ] {
            let (_, want) = run(&off, sql).await;
            for pass in ["first", "second"] {
                let (plan, got) = run(&on, sql).await;
                assert_eq!(
                    pretty(&got),
                    pretty(&want),
                    "{sql}, {pass} run ({max_dynamic_columns})"
                );
                assert!(
                    nodes(&plan, "FilterExec").is_empty(),
                    "{sql}: no FilterExec"
                );
                pruned += scan_metric(&plan, "segments_pruned_by_key_index")
                    + scan_metric(&plan, "blocks_pruned_by_key_index");
            }
        }
        assert!(
            pruned > 0,
            "the KEY_IDX probe pruned blocks ({max_dynamic_columns})"
        );
        if max_dynamic_columns > 0 {
            assert!(
                cache.process_counters().hits > 0,
                "the second runs read the decoded cache"
            );
        }
    }
}

/// `uid` literals across the I64 range, each against the reference. The
/// stream's resource holds `i64::MAX` and a scope holds -3, so both reach rows
/// through stream attributes as well as record cells.
///
/// Flipped: a literal narrowed or widened on the way into the scan's filter
/// (an `i64::MIN` or `i64::MAX` that wraps, an `Int32` literal compared by
/// its own width) keeps or drops rows the `FilterExec` does not.
#[tokio::test]
async fn i64_extreme_and_narrow_literals_match_filter_exec() {
    let values = [-3, i64::MIN, i64::MAX, 1 << 40, 5, 0];
    for max_dynamic_columns in [8, 0] {
        let objects = vec![
            (0..18)
                .map(|i| Row {
                    stream: i % 3,
                    uid: match i % 4 {
                        0 => RecordUid::None,
                        3 => RecordUid::WrongVariant(5),
                        _ => RecordUid::I64(values[i % values.len()]),
                    },
                    erased: false,
                })
                .collect(),
        ];
        let corpus = Corpus {
            streams: vec![
                StreamUid::Resource(i64::MAX),
                StreamUid::Scope(-3),
                StreamUid::None,
            ],
            objects,
            erasure: false,
            max_dynamic_columns,
            value: 0,
        };
        let (store, segments) = write_corpus(&corpus).await;
        let on = session(&store, segments.clone(), false, true);
        let off = session(&store, segments, false, false);
        let mut literals: Vec<String> = values
            .iter()
            .filter(|v| **v != i64::MIN)
            .map(|v| v.to_string())
            .collect();
        literals.push(i64::MIN.to_string());
        literals.push(format!("({} - 1)", i64::MIN + 1));
        literals.push("CAST(5 AS INT)".to_string());
        literals.push("arrow_cast(-3, 'Int16')".to_string());
        for literal in literals {
            for select in ["ts, uid", "ts, uid, attrs"] {
                let sql = format!("SELECT {select} FROM logs WHERE uid = {literal} ORDER BY ts");
                let (_, want) = run(&off, &sql).await;
                let (plan, got) = run(&on, &sql).await;
                assert_eq!(pretty(&got), pretty(&want), "{sql} ({max_dynamic_columns})");
                assert!(
                    nodes(&plan, "FilterExec").is_empty(),
                    "{sql}: no FilterExec"
                );
            }
        }
    }
}

/// A fixed answer, not a comparison: on a block whose `uid` comes only from
/// its stream's resource (`uid = 7`), the rows on that stream match and the
/// rows on a stream without `uid` do not, on the columnar path, the row path
/// (`attrs` projected) and the `attrs_raw` row path.
///
/// Flipped: a first pass that resolves the record cells and not the stream
/// entries drops ts 2, 4 and 6 and fails "the resource-sourced rows".
#[tokio::test]
async fn a_resource_sourced_uid_matches_by_a_fixed_answer() {
    for max_dynamic_columns in [8, 0] {
        let corpus = Corpus {
            streams: vec![StreamUid::Resource(7), StreamUid::None],
            objects: vec![
                (0..6)
                    .map(|i| Row {
                        stream: i % 2,
                        uid: RecordUid::None,
                        erased: false,
                    })
                    .collect(),
            ],
            erasure: false,
            max_dynamic_columns,
            value: 7,
        };
        let (store, segments) = write_corpus(&corpus).await;
        let on = session(&store, segments, false, true);
        for select in ["ts, uid", "ts, uid, attrs"] {
            let sql = format!("SELECT {select} FROM logs WHERE uid = 7 ORDER BY ts");
            let (plan, got) = run(&on, &sql).await;
            let ts: Vec<String> = got
                .iter()
                .flat_map(|b| {
                    let c = b.column(b.schema().index_of("ts").expect("ts"));
                    (0..b.num_rows()).map(move |r| array_value_to_string(c, r).expect("ts"))
                })
                .collect();
            let uid: Vec<String> = got
                .iter()
                .flat_map(|b| {
                    let c = b.column(b.schema().index_of("uid").expect("uid"));
                    (0..b.num_rows()).map(move |r| array_value_to_string(c, r).expect("uid"))
                })
                .collect();
            assert_eq!(
                ts,
                [
                    "1970-01-01T00:00:00.000000002",
                    "1970-01-01T00:00:00.000000004",
                    "1970-01-01T00:00:00.000000006"
                ],
                "the resource-sourced rows: {sql} ({max_dynamic_columns})"
            );
            assert_eq!(uid, ["7", "7", "7"], "{sql}");
            assert!(
                nodes(&plan, "FilterExec").is_empty(),
                "{sql}: no FilterExec"
            );
        }
    }
}
