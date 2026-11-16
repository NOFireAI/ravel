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

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use proptest::prelude::*;
use ravel_catalog::{SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    CeilingBreach, DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SpillDecision,
    SqlConfig, TenantDelegatingPool, TenantMemoryAccountant, build_session,
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

fn stream_attrs(uid: StreamUid) -> (Vec<(String, AttrValue)>, Vec<(String, AttrValue)>) {
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
    let provider = LogsTableProvider::new(
        Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure,
        },
        TENANT,
        LogSegmentFetcher::new(backend),
        PhaseAccounting::new(),
    )
    .with_declared_columns(declared())
    .with_row_filter_pushdown(row_filter);
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
        prop_assert!(nodes(&plan, "FilterExec").is_empty(), "{}: no FilterExec", sql);
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
