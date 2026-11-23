//! The metadata-only aggregate rewrite over a statement with several
//! aggregates (issue #4651): ClickBench line 3, `SUM(a), COUNT(*), AVG(w)`,
//! and line 30, ninety `SUM(w + k)`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use ravel_catalog::{EntryIdentity, LoadedColumnStats, SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue, DictEntry};
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SpillDecision, SqlConfig,
    TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
use uuid::Uuid;

use crate::util::CountingStore;

const TENANT: TenantHash = TenantHash([9u8; 16]);

/// `(ts_ns, a, w)` of the first object.
const SEG_A: &[(i64, Option<i64>, Option<i64>)] = &[
    (0, Some(0), Some(1368)),
    (10, Some(2), Some(1917)),
    (20, None, Some(1368)),
    (30, Some(0), None),
    (40, Some(5), Some(800)),
];

/// `(ts_ns, a, w)` of the second object.
const SEG_B: &[(i64, Option<i64>, Option<i64>)] = &[
    (100, Some(3), Some(1024)),
    (110, None, Some(640)),
    (120, Some(0), Some(1920)),
    (130, Some(7), Some(1368)),
];

type Row = (i64, Option<i64>, Option<i64>);

fn cols() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("a", DeclaredType::I64),
        DeclaredColumn::new("w", DeclaredType::I64),
    ]
}

fn record(ts: i64, a: Option<i64>, w: Option<i64>) -> LogRecord {
    let resource: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let mut attrs = Vec::new();
    if let Some(v) = a {
        attrs.push(("a".to_string(), AttrValue::I64(v)));
    }
    if let Some(v) = w {
        attrs.push(("w".to_string(), AttrValue::I64(v)));
    }
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("row at {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

fn i64_value(v: i64) -> ColumnValue {
    ColumnValue {
        kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(v)),
    }
}

/// The exact I64 statistics a fold produces for `values`.
fn stat_of(name: &str, values: &[Option<i64>]) -> ColumnStat {
    let mut dict: Vec<(i64, u64)> = Vec::new();
    let mut null_count = 0u64;
    for v in values {
        match v {
            Some(v) => match dict.iter_mut().find(|(dv, _)| dv == v) {
                Some(entry) => entry.1 += 1,
                None => dict.push((*v, 1)),
            },
            None => null_count += 1,
        }
    }
    dict.sort_unstable();
    let sum_wide: i128 = dict
        .iter()
        .map(|(v, c)| i128::from(*v) * i128::from(*c))
        .sum();
    ColumnStat {
        name: name.to_string(),
        declared_type: 2,
        non_null_count: dict.iter().map(|(_, c)| c).sum(),
        null_count,
        min: dict.first().map(|(v, _)| i64_value(*v)),
        max: dict.last().map(|(v, _)| i64_value(*v)),
        dictionary_present: true,
        dictionary: dict
            .iter()
            .map(|(v, c)| DictEntry {
                value: Some(i64_value(*v)),
                count: *c,
            })
            .collect(),
        sum: i64::try_from(sum_wide).ok(),
    }
}

/// Both columns' statistics for `rows`.
fn stats_of(rows: &[Row]) -> Vec<ColumnStat> {
    vec![
        stat_of("a", &rows.iter().map(|r| r.1).collect::<Vec<_>>()),
        stat_of("w", &rows.iter().map(|r| r.2).collect::<Vec<_>>()),
    ]
}

/// `seg` carrying the stamps a writer folds for every entry of `stats`.
fn stamped(seg: SegmentRef, stats: &[ColumnStat]) -> SegmentRef {
    use ravel_proto::catalog::v1::column_value::Kind;
    use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};
    let value = |v: &Option<ColumnValue>| {
        v.as_ref().map(|v| match v.kind {
            Some(Kind::I64(x)) => DeclaredStatValue::I64(x),
            ref other => panic!("fixture stamps I64 entries only, got {other:?}"),
        })
    };
    let stamps: Vec<DeclaredColumnStat> = stats
        .iter()
        .map(|stat| {
            DeclaredColumnStat::new(
                &stat.name,
                DeclaredStatType::I64,
                value(&stat.min),
                value(&stat.max),
                stat.null_count,
            )
            .expect("valid stamp")
        })
        .collect();
    let mut record = ravel_proto::commit::v1::CommitRecord {
        sample_count: seg.sample_count,
        ..Default::default()
    };
    ravel_commit::declared_stats::stamp_commit_record(&mut record, &stamps);
    let validated = ravel_commit::declared_stats::read_commit_record(&record);
    assert_eq!(
        validated.covered().len(),
        stats.len(),
        "fixture stamps valid"
    );
    SegmentRef {
        declared_column_stats: ravel_catalog::DeclaredColumnStats::from_validated(&validated),
        ..seg
    }
}

/// Write `rows` as one real RLOG object and return its stamped ref.
async fn write_segment(
    store: &Arc<CountingStore>,
    key: &str,
    seq: u64,
    rows: &[Row],
) -> SegmentRef {
    let mut w = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: seq,
        },
    );
    for (ts, a, wv) in rows {
        w.push(record(*ts, *a, *wv)).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let object_size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    let seg = SegmentRef {
        data_object_key: key.to_string(),
        object_size,
        min_event_ts_ns: rows.iter().map(|r| r.0).min().expect("nonempty"),
        max_event_ts_ns: rows.iter().map(|r| r.0).max().expect("nonempty"),
        ingest_hour_bucket: 0,
        sample_count: rows.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: seq,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    };
    stamped(seg, &stats_of(rows))
}

fn identity_of(seg: &SegmentRef) -> EntryIdentity {
    (
        seg.ingest_hour_bucket,
        seg.shard,
        *seg.writer_id.as_bytes(),
        seg.writer_epoch,
        seg.writer_seq,
    )
}

/// Loaded statistics carrying `columns` for each segment.
fn loaded(entries: Vec<(&SegmentRef, Vec<ColumnStat>)>) -> Arc<LoadedColumnStats> {
    let mut segments = HashMap::new();
    for (seg, columns) in entries {
        segments.insert(
            identity_of(seg),
            ColumnStatsSegment {
                ingest_hour_bucket: seg.ingest_hour_bucket,
                shard: seg.shard,
                writer_id: seg.writer_id.as_bytes().to_vec(),
                writer_epoch: seg.writer_epoch,
                writer_seq: seg.writer_seq,
                columns,
            },
        );
    }
    Arc::new(LoadedColumnStats::from_maps(segments, HashMap::new()))
}

/// Two real objects plus their exact statistics.
struct Corpus {
    store: Arc<CountingStore>,
    segs: Vec<SegmentRef>,
    stats: Arc<LoadedColumnStats>,
}

/// What one run of a statement produced.
struct Run {
    plan: String,
    answer: datafusion::error::Result<Vec<datafusion::arrow::record_batch::RecordBatch>>,
    gets: u64,
    /// `fetch()` of every node in the physical plan, top-down, as
    /// `(node name, fetch)`.
    fetches: Vec<(String, Option<usize>)>,
}

fn fetches_of(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<(String, Option<usize>)>) {
    out.push((plan.name().to_string(), plan.fetch()));
    for child in plan.children() {
        fetches_of(child, out);
    }
}

impl Corpus {
    async fn build(parts: &[&[Row]]) -> Corpus {
        let store = CountingStore::new(Arc::new(MemoryStore::new()));
        let mut segs = Vec::new();
        for (i, rows) in parts.iter().enumerate() {
            let seq = i as u64 + 1;
            segs.push(write_segment(&store, &format!("logs/multi-{seq}.rlog"), seq, rows).await);
        }
        let stats = loaded(
            segs.iter()
                .zip(parts)
                .map(|(seg, rows)| (seg, stats_of(rows)))
                .collect(),
        );
        Corpus { store, segs, stats }
    }

    async fn two() -> Corpus {
        Corpus::build(&[SEG_A, SEG_B]).await
    }

    async fn run(
        &self,
        sql: &str,
        stats: Option<Arc<LoadedColumnStats>>,
        erasure: Vec<ravel_proto::commit::v1::ErasureRequest>,
        exact_typed: bool,
    ) -> Run {
        let backend: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&self.store) as Arc<dyn ObjectStoreBackend>;
        let provider = LogsTableProvider::new(
            Snapshot {
                segments: self.segs.clone(),
                segments_pruned: 0,
                pending_erasure: erasure,
            },
            TENANT,
            LogSegmentFetcher::new(backend),
            PhaseAccounting::new(),
        )
        .with_declared_columns(cols())
        .with_column_stats(stats);
        let config = SqlConfig::default();
        let (pool, _breach) =
            config.query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
        let ctx = build_session(
            &config,
            pool,
            SessionTable::Logs(Arc::new(provider)),
            exact_typed,
            SpillDecision::Disabled,
            &ravel_sql::SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
        )
        .expect("session");
        let plan = ctx
            .sql(sql)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let shown = displayable(plan.as_ref()).indent(true).to_string();
        let mut fetches = Vec::new();
        fetches_of(&plan, &mut fetches);
        let before = self.store.gets();
        let answer = collect(plan, ctx.task_ctx()).await;
        Run {
            plan: shown,
            answer,
            gets: self.store.gets() - before,
            fetches,
        }
    }

    async fn with_stats(&self, sql: &str) -> Run {
        self.run(sql, Some(Arc::clone(&self.stats)), Vec::new(), false)
            .await
    }

    async fn scanned(&self, sql: &str) -> Run {
        self.run(sql, None, Vec::new(), false).await
    }
}

const LINE_3: &str = "SELECT SUM(a), COUNT(*), AVG(w) FROM logs";

fn line_30() -> String {
    let terms: Vec<String> = (0..90)
        .map(|k| {
            if k == 0 {
                "SUM(w)".to_string()
            } else {
                format!("SUM(w + {k})")
            }
        })
        .collect();
    format!("SELECT {} FROM logs", terms.join(", "))
}

/// The answer as one row of columns, failing the test on an error.
fn columns(run: &Run) -> Vec<ArrayRef> {
    let batches = run.answer.as_ref().expect("an answer");
    let batch = concat_batches(&batches[0].schema(), batches).expect("concat");
    assert_eq!(batch.num_rows(), 1, "one row");
    batch.columns().to_vec()
}

fn i64_at(column: &ArrayRef) -> Option<i64> {
    let array = column.as_any().downcast_ref::<Int64Array>().expect("Int64");
    (!array.is_null(0)).then(|| array.value(0))
}

fn f64_at(column: &ArrayRef) -> Option<f64> {
    let array = column
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64");
    (!array.is_null(0)).then(|| array.value(0))
}

fn assert_answered(run: &Run, what: &str) {
    assert!(
        run.plan
            .contains("MetadataOnlyExec: metadata_only=true, rows=1")
            && !run.plan.contains("LogsScanExec"),
        "{what}: answered from statistics; plan was:\n{}",
        run.plan
    );
    assert_eq!(run.gets, 0, "{what}: CountingStore::gets() moved");
}

fn assert_scanned(run: &Run, what: &str) {
    assert!(
        run.plan.contains("LogsScanExec") && !run.plan.contains("MetadataOnlyExec"),
        "{what}: must scan; plan was:\n{}",
        run.plan
    );
    assert!(run.gets > 0, "{what}: a scan reads data objects");
}

/// Same values, same types, NULL for NULL, and float answers equal bit for
/// bit; or the same error.
fn assert_same_answer(left: &Run, right: &Run, what: &str) {
    match (&left.answer, &right.answer) {
        (Err(l), Err(r)) => assert_eq!(l.to_string(), r.to_string(), "{what}: same error"),
        (Ok(_), Ok(_)) => {
            let (l, r) = (columns(left), columns(right));
            assert_eq!(l.len(), r.len(), "{what}: width");
            for (i, (l, r)) in l.iter().zip(&r).enumerate() {
                assert_eq!(l.data_type(), r.data_type(), "{what}: column {i} type");
                assert_eq!(l.to_data(), r.to_data(), "{what}: column {i} value");
                if let (Some(lf), Some(rf)) = (
                    l.as_any().downcast_ref::<Float64Array>(),
                    r.as_any().downcast_ref::<Float64Array>(),
                ) {
                    assert_eq!(
                        lf.values().iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                        rf.values().iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
                        "{what}: column {i} bits"
                    );
                }
            }
        }
        (l, r) => panic!(
            "{what}: one path errored: {:?} vs {:?}",
            l.as_ref().err(),
            r.as_ref().err()
        ),
    }
}

/// The fixture's figures, recomputed from the rows rather than trusted.
fn fixture_sums() -> (i64, i64, i64, i64) {
    let rows = SEG_A.iter().chain(SEG_B);
    let a: Vec<i64> = rows.clone().filter_map(|r| r.1).collect();
    let w: Vec<i64> = rows.filter_map(|r| r.2).collect();
    (
        a.iter().sum(),
        a.len() as i64,
        w.iter().sum(),
        w.len() as i64,
    )
}

/// T1, ClickBench line 3: `SUM(a), COUNT(*), AVG(w)` is one `MetadataOnlyExec`
/// with no data-object GET, and each column carries its own figure: the
/// row count 9 (not a column's non-null count, 7 or 8) for `COUNT(*)`, `a`'s
/// sum for `SUM`, `w`'s sum over `w`'s count for `AVG`. Both session kinds.
#[tokio::test]
async fn line_3_is_answered_from_statistics() {
    let (a_sum, a_count, w_sum, w_count) = fixture_sums();
    assert_eq!((a_sum, a_count, w_sum, w_count), (17, 7, 10405, 8));
    let corpus = Corpus::two().await;
    for exact_typed in [false, true] {
        let run = corpus
            .run(
                LINE_3,
                Some(Arc::clone(&corpus.stats)),
                Vec::new(),
                exact_typed,
            )
            .await;
        assert_answered(&run, "line 3");
        let cols = columns(&run);
        assert_eq!(i64_at(&cols[0]), Some(17), "SUM(a)");
        assert_eq!(i64_at(&cols[1]), Some(9), "COUNT(*)");
        assert_eq!(f64_at(&cols[2]), Some(10405.0 / 8.0), "AVG(w)");
        let scan = corpus.run(LINE_3, None, Vec::new(), exact_typed).await;
        assert_scanned(&scan, "line 3 without statistics");
        assert_same_answer(&run, &scan, "line 3");
    }
}

/// T1, ClickBench line 30: all ninety `SUM(w + k)` terms are answered from
/// statistics, `10405 + 8k` each, with no data-object GET, and equal the scan.
#[tokio::test]
async fn line_30_is_answered_from_statistics() {
    let sql = line_30();
    let corpus = Corpus::two().await;
    for exact_typed in [false, true] {
        let run = corpus
            .run(
                &sql,
                Some(Arc::clone(&corpus.stats)),
                Vec::new(),
                exact_typed,
            )
            .await;
        assert_answered(&run, "line 30");
        let cols = columns(&run);
        assert_eq!(cols.len(), 90);
        for (k, col) in cols.iter().enumerate() {
            assert_eq!(i64_at(col), Some(10405 + 8 * k as i64), "SUM(w + {k})");
        }
        let scan = corpus.run(&sql, None, Vec::new(), exact_typed).await;
        assert_scanned(&scan, "line 30 without statistics");
        assert_same_answer(&run, &scan, "line 30");
    }
}

/// `EXPLAIN` of a multi-aggregate statement keeps the one-line
/// `MetadataOnlyExec` form.
#[tokio::test]
async fn explain_shows_the_metadata_only_line_for_several_aggregates() {
    let corpus = Corpus::two().await;
    let run = corpus
        .run(
            &format!("EXPLAIN {LINE_3}"),
            Some(Arc::clone(&corpus.stats)),
            Vec::new(),
            false,
        )
        .await;
    let batches = run.answer.as_ref().expect("explain output");
    let text = pretty_format_batches(batches).expect("format").to_string();
    assert!(
        text.contains("MetadataOnlyExec: metadata_only=true, rows=1"),
        "explain was:\n{text}"
    );
}

/// T2: over a column with no non-null row `SUM` and `AVG` are NULL while
/// `COUNT(*)` stays the exact row count, beside a column that has values;
/// same values and types as the scan.
#[tokio::test]
async fn null_sums_beside_an_exact_count_match_the_scan() {
    let rows: &[Row] = &[(0, None, Some(5)), (10, None, Some(6)), (20, None, None)];
    let corpus = Corpus::build(&[rows]).await;
    let sql = "SELECT SUM(a), AVG(a), COUNT(*), SUM(w), SUM(a + 3) FROM logs";
    let run = corpus.with_stats(sql).await;
    assert_answered(&run, "all-NULL column");
    let cols = columns(&run);
    assert_eq!(i64_at(&cols[0]), None, "SUM over no non-null row");
    assert_eq!(f64_at(&cols[1]), None, "AVG over no non-null row");
    assert_eq!(i64_at(&cols[2]), Some(3), "COUNT(*)");
    assert_eq!(i64_at(&cols[3]), Some(11), "SUM(w)");
    assert_eq!(i64_at(&cols[4]), None, "SUM(a + 3) over no non-null row");
    assert_same_answer(&run, &corpus.scanned(sql).await, "all-NULL column");
}

/// T2: `SUM(x + 1)` at the 64-bit edge. Rows `i64::MAX` and `-i64::MAX` sum
/// to 0, so `SUM(a + 1)` is exactly 2 with no per-row wrap on either path;
/// rows `i64::MAX` and 0 make `SUM(a + 1)` pass `i64::MAX`, and both paths
/// fail with the same overflow error rather than wrap.
#[tokio::test]
async fn sum_plus_literal_at_the_64_bit_edge_matches_the_scan() {
    let fits: &[Row] = &[(0, Some(i64::MAX), Some(1)), (10, Some(-i64::MAX), Some(1))];
    let corpus = Corpus::build(&[fits]).await;
    let sql = "SELECT SUM(a), SUM(a + 1), COUNT(*) FROM logs";
    let run = corpus.with_stats(sql).await;
    assert_answered(&run, "edge, fits");
    let cols = columns(&run);
    assert_eq!(
        (i64_at(&cols[0]), i64_at(&cols[1]), i64_at(&cols[2])),
        (Some(0), Some(2), Some(2))
    );
    assert_same_answer(&run, &corpus.scanned(sql).await, "edge, fits");

    let over: &[Row] = &[(0, Some(i64::MAX), Some(1)), (10, Some(0), Some(1))];
    let corpus = Corpus::build(&[over]).await;
    let sql = "SELECT SUM(a + 1), COUNT(*) FROM logs";
    let run = corpus.with_stats(sql).await;
    assert!(
        run.plan.contains("MetadataOnlyExec"),
        "edge, overflows: plan was:\n{}",
        run.plan
    );
    assert!(run.answer.is_err(), "SUM(a + 1) passes i64::MAX");
    let scan = corpus.scanned(sql).await;
    assert!(scan.answer.is_err(), "the scan overflows too");
    assert_same_answer(&run, &scan, "edge, overflows");
}

/// T3: one segment without a statistics object declines the whole statement.
#[tokio::test]
async fn a_segment_without_statistics_declines_the_statement() {
    let corpus = Corpus::two().await;
    let partial = loaded(vec![(&corpus.segs[0], stats_of(SEG_A))]);
    let run = corpus.run(LINE_3, Some(partial), Vec::new(), false).await;
    assert_scanned(&run, "missing stats object");
    assert_same_answer(&run, &corpus.scanned(LINE_3).await, "missing stats object");
}

/// T3: `w` without a stored sum on one segment declines the statement that
/// reads `w` and `a` together, while a statement on `a` alone with the same
/// statistics is still answered: the check is per column.
#[tokio::test]
async fn one_column_without_a_sum_declines_the_statement() {
    let corpus = Corpus::two().await;
    let mut b = stats_of(SEG_B);
    b[1].sum = None;
    let stats = loaded(vec![
        (&corpus.segs[0], stats_of(SEG_A)),
        (&corpus.segs[1], b),
    ]);
    let run = corpus
        .run(LINE_3, Some(Arc::clone(&stats)), Vec::new(), false)
        .await;
    assert_scanned(&run, "w without a sum");
    assert_same_answer(&run, &corpus.scanned(LINE_3).await, "w without a sum");

    let a_only = "SELECT SUM(a), COUNT(*) FROM logs";
    let run = corpus.run(a_only, Some(stats), Vec::new(), false).await;
    assert_answered(&run, "a alone");
    let cols = columns(&run);
    assert_eq!((i64_at(&cols[0]), i64_at(&cols[1])), (Some(17), Some(9)));
}

/// T3: a pending erasure declines the statement; the scan applies it.
#[tokio::test]
async fn a_pending_erasure_declines_the_statement() {
    let corpus = Corpus::two().await;
    let erasure = || {
        vec![ravel_proto::commit::v1::ErasureRequest {
            predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
                key: "a".to_string(),
                value: "5".to_string(),
            }],
            ..Default::default()
        }]
    };
    let run = corpus
        .run(LINE_3, Some(Arc::clone(&corpus.stats)), erasure(), false)
        .await;
    assert_scanned(&run, "pending erasure");
    let scan = corpus.run(LINE_3, None, erasure(), false).await;
    assert_same_answer(&run, &scan, "pending erasure");
}

/// T3: one aggregate the statistics cannot answer beside answerable ones
/// declines the whole statement, and the scan's answer is the same as with no
/// statistics at all.
#[tokio::test]
async fn an_unanswerable_aggregate_declines_the_statement() {
    let corpus = Corpus::two().await;
    for sql in [
        "SELECT SUM(a), COUNT(*), SUM(DISTINCT w) FROM logs",
        "SELECT SUM(a), COUNT(*), SUM(w) FILTER (WHERE a > 0) FROM logs",
        "SELECT SUM(a), AVG(CAST(w AS DOUBLE)) FROM logs",
        "SELECT SUM(a), COUNT(*), MIN(w) FROM logs",
    ] {
        let run = corpus.with_stats(sql).await;
        assert_scanned(&run, sql);
        assert_same_answer(&run, &corpus.scanned(sql).await, sql);
    }
}

/// T3: a residual filter or a group key beside several answerable aggregates
/// declines the statement.
#[tokio::test]
async fn a_filter_or_group_key_declines_several_aggregates() {
    let corpus = Corpus::two().await;
    for sql in [
        "SELECT SUM(a), COUNT(*), AVG(w) FROM logs WHERE a <> 0",
        "SELECT w, SUM(a), COUNT(*) FROM logs GROUP BY w ORDER BY w",
    ] {
        let run = corpus.with_stats(sql).await;
        assert_scanned(&run, sql);
        let scan = corpus.scanned(sql).await;
        let (l, r) = (run.answer.expect("answer"), scan.answer.expect("answer"));
        assert_eq!(
            pretty_format_batches(&l).expect("format").to_string(),
            pretty_format_batches(&r).expect("format").to_string(),
            "{sql}"
        );
    }
}

/// `SEG_A` then `SEG_B` as one object: one scan partition, so `LimitPushdown`
/// leaves a subquery's `LIMIT 3` on the scan alone. The scan emits the first
/// three rows, `a` = 0, 2, NULL.
async fn one_object() -> Corpus {
    let rows: Vec<Row> = SEG_A.iter().chain(SEG_B).copied().collect();
    Corpus::build(&[rows.as_slice()]).await
}

/// Runs `sql` with statistics loaded and without, asserts both runs print
/// `expected` with no `MetadataOnlyExec`, and that the plan without
/// statistics carries its limit only as the scan's pushed fetch (no limit
/// node, no fetch on any other node), the shape the rule saw before it
/// declined.
async fn assert_limit_on_the_scan_is_honoured(corpus: &Corpus, sql: &str, expected: &str) {
    let run = corpus.with_stats(sql).await;
    let scan = corpus.scanned(sql).await;
    for (what, run) in [("with statistics", &run), ("scanned", &scan)] {
        let got = pretty_format_batches(run.answer.as_ref().expect("an answer"))
            .expect("format")
            .to_string();
        assert_eq!(got.trim(), expected.trim(), "{sql} {what}");
        assert!(
            !run.plan.contains("MetadataOnlyExec"),
            "{sql} {what}: a scan with a pushed fetch is not answered from statistics; plan was:\n{}",
            run.plan
        );
        if what == "scanned" {
            let limited: Vec<_> = run.fetches.iter().filter(|(_, f)| f.is_some()).collect();
            assert_eq!(
                limited,
                [&("LogsScanExec".to_string(), Some(3))],
                "{sql}: the limit must sit on the scan alone; plan was:\n{}",
                run.plan
            );
            assert!(
                !run.plan.contains("LimitExec"),
                "{sql}: no limit node; plan was:\n{}",
                run.plan
            );
        }
    }
    assert!(run.gets > 0, "{sql}: the limited rows are read");
}

/// Issue #4651 review: `SUM(a), COUNT(*)` over a `LIMIT 3` subquery whose
/// limit is the scan's pushed fetch is the sum and count of the three rows the
/// scan emits (2 and 3), not the statistics of all nine rows (17 and 9).
#[tokio::test]
async fn sum_and_count_star_over_a_limit_on_the_scan_read_the_limited_rows() {
    let corpus = one_object().await;
    assert_limit_on_the_scan_is_honoured(
        &corpus,
        "SELECT SUM(a), COUNT(*) FROM (SELECT a FROM logs LIMIT 3)",
        "
+-------------+----------+
| sum(logs.a) | count(*) |
+-------------+----------+
| 2           | 3        |
+-------------+----------+",
    )
    .await;
}

/// A single `SUM` over the same limited subquery: 2, not 17.
#[tokio::test]
async fn a_single_sum_over_a_limit_on_the_scan_reads_the_limited_rows() {
    let corpus = one_object().await;
    assert_limit_on_the_scan_is_honoured(
        &corpus,
        "SELECT SUM(a) FROM (SELECT a FROM logs LIMIT 3)",
        "
+-------------+
| sum(logs.a) |
+-------------+
| 2           |
+-------------+",
    )
    .await;
}

/// A single `AVG` over the same limited subquery: 1.0, not 17/7.
#[tokio::test]
async fn a_single_avg_over_a_limit_on_the_scan_reads_the_limited_rows() {
    let corpus = one_object().await;
    assert_limit_on_the_scan_is_honoured(
        &corpus,
        "SELECT AVG(a) FROM (SELECT a FROM logs LIMIT 3)",
        "
+-------------+
| avg(logs.a) |
+-------------+
| 1.0         |
+-------------+",
    )
    .await;
}

/// q02 over a limited subquery: `COUNT(*) WHERE a <> 0` counts the one
/// qualifying row among the three the scan emits, not the four among all
/// nine rows.
#[tokio::test]
async fn q02_over_a_limit_on_the_scan_reads_the_limited_rows() {
    let corpus = one_object().await;
    assert_limit_on_the_scan_is_honoured(
        &corpus,
        "SELECT COUNT(*) FROM (SELECT a FROM logs LIMIT 3) WHERE a <> 0",
        "
+----------+
| count(*) |
+----------+
| 1        |
+----------+",
    )
    .await;
}

/// q08 over a limited subquery: three groups of one row, not the six groups
/// of all nine rows.
#[tokio::test]
async fn q08_over_a_limit_on_the_scan_reads_the_limited_rows() {
    let corpus = one_object().await;
    assert_limit_on_the_scan_is_honoured(
        &corpus,
        "SELECT a, COUNT(*) FROM (SELECT a FROM logs LIMIT 3) GROUP BY a ORDER BY a",
        "
+---+----------+
| a | count(*) |
+---+----------+
| 0 | 1        |
| 2 | 1        |
|   | 1        |
+---+----------+",
    )
    .await;
}

/// `MIN`/`MAX` over the limited subquery are not this rule's shape and scan:
/// 0 and 2 over the three emitted rows.
#[tokio::test]
async fn min_max_over_a_limit_on_the_scan_read_the_limited_rows() {
    let corpus = one_object().await;
    assert_limit_on_the_scan_is_honoured(
        &corpus,
        "SELECT MIN(a), MAX(a) FROM (SELECT a FROM logs LIMIT 3)",
        "
+-------------+-------------+
| min(logs.a) | max(logs.a) |
+-------------+-------------+
| 0           | 2           |
+-------------+-------------+",
    )
    .await;
}

/// A lone `COUNT(*)` over the limited subquery is DataFusion's
/// `AggregateStatistics` answer, the constant 3 with no `MetadataOnlyExec` and
/// no data-object GET: that rule runs while the limit is still a node above
/// the scan, so this rule never sees the shape.
#[tokio::test]
async fn a_lone_count_star_over_a_limit_is_the_limited_row_count() {
    let corpus = one_object().await;
    let sql = "SELECT COUNT(*) FROM (SELECT a FROM logs LIMIT 3)";
    for run in [corpus.with_stats(sql).await, corpus.scanned(sql).await] {
        assert!(
            run.plan.contains("ProjectionExec: expr=[3 as count(*)]")
                && run.plan.contains("PlaceholderRowExec")
                && !run.plan.contains("MetadataOnlyExec"),
            "plan was:\n{}",
            run.plan
        );
        let cols = columns(&run);
        assert_eq!(i64_at(&cols[0]), Some(3));
        assert_eq!(run.gets, 0);
    }
}
