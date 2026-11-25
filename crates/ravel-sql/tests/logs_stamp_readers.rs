//! The stamp-first statistics readers of ADR-4628 (issue #4684, task T5a):
//! `SUM`, `AVG`, `exact_sum_count` and `COUNT(*) WHERE col <> lit` answered
//! per segment from the declared-column stamp's sum and zero count (fields 6
//! to 8), else from a `.cstat` entry the stamp grants, else scanned.
//!
//! Every object is a real RLOG written by `RlogWriter`, and every stamp is
//! built from the rows that object holds. A forced scan is the same objects
//! with fields 6 to 8 stripped from the stamps and no `.cstat`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, RecordBatch};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::physical_plan::{collect, displayable};
use ravel_catalog::{
    Catalog, CatalogConfig, DeclaredColumnStats, EntryIdentity, LoadedColumnStats, SegmentLevel,
    SegmentRef, Snapshot,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{erasure, keys, publish, signal};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue, DictEntry};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, ErasureCompletion,
    ErasurePredicateMatcher, ErasureRequest, RewriteDrop, RewriteRecord,
};
use ravel_query::{LogSegmentFetcher, PhaseAccounting, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, KeyIndexDeclaration, LogsTableProvider, SessionTable,
    SpanSegmentFetcher, SpillDecision, SqlConfig, SqlExecutor, SqlOutcome, SqlRequest,
    StaticDeclaredColumns, TenantMemoryAccountant, build_session,
};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};
use ravel_types::logstream::log_stream_id;
use ravel_types::{CommitToken, Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

use crate::util::CountingStore;

// ---------------------------------------------------------------------------
// Rows and stamps
// ---------------------------------------------------------------------------

/// One log row: its timestamp and the attributes it sets.
#[derive(Clone, Debug)]
pub(crate) struct Row {
    pub(crate) ts: i64,
    pub(crate) attrs: Vec<(&'static str, AttrValue)>,
}

/// A row setting `a` and `w` (I64) where given.
pub(crate) fn row(ts: i64, a: Option<i64>, w: Option<i64>) -> Row {
    let mut attrs = Vec::new();
    if let Some(v) = a {
        attrs.push(("a", AttrValue::I64(v)));
    }
    if let Some(v) = w {
        attrs.push(("w", AttrValue::I64(v)));
    }
    Row { ts, attrs }
}

impl Row {
    pub(crate) fn with(mut self, key: &'static str, value: AttrValue) -> Row {
        self.attrs.push((key, value));
        self
    }

    fn get(&self, key: &str) -> Option<&AttrValue> {
        self.attrs.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }
}

pub(crate) fn log_record(r: &Row) -> LogRecord {
    let resource: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: r.ts,
        observed_ts_ns: r.ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("row at {}", r.ts),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: r
            .attrs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect(),
    }
}

/// Whether a stamp carries ADR-4628's fields 6 to 8.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fields {
    With,
    Without,
}

/// The stamp a writer folds for column `col` of `declared` type over `rows`:
/// min, max and NULL count, plus the sum and zero count under
/// [`Fields::With`]. A value of another kind reads NULL.
pub(crate) fn stamp_of(
    col: &str,
    declared: DeclaredType,
    rows: &[Row],
    fields: Fields,
) -> Option<DeclaredColumnStat> {
    match declared {
        DeclaredType::I64 => {
            let values: Vec<i64> = rows
                .iter()
                .filter_map(|r| match r.get(col) {
                    Some(AttrValue::I64(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            let null_count = (rows.len() - values.len()) as u64;
            let stat = DeclaredColumnStat::new(
                col,
                DeclaredStatType::I64,
                values.iter().min().map(|v| DeclaredStatValue::I64(*v)),
                values.iter().max().map(|v| DeclaredStatValue::I64(*v)),
                null_count,
            )
            .expect("valid stamp");
            Some(match fields {
                Fields::Without => stat,
                Fields::With => stat
                    .with_sum(values.iter().map(|v| i128::from(*v)).sum())
                    .expect("valid sum")
                    .with_zero_count(values.iter().filter(|v| **v == 0).count() as u64)
                    .expect("valid zero count"),
            })
        }
        DeclaredType::Bool => {
            let values: Vec<bool> = rows
                .iter()
                .filter_map(|r| match r.get(col) {
                    Some(AttrValue::Bool(v)) => Some(*v),
                    _ => None,
                })
                .collect();
            let null_count = (rows.len() - values.len()) as u64;
            let stat = DeclaredColumnStat::new(
                col,
                DeclaredStatType::Bool,
                values.iter().min().map(|v| DeclaredStatValue::Bool(*v)),
                values.iter().max().map(|v| DeclaredStatValue::Bool(*v)),
                null_count,
            )
            .expect("valid stamp");
            Some(match fields {
                Fields::Without => stat,
                Fields::With => stat
                    .with_zero_count(values.iter().filter(|v| !**v).count() as u64)
                    .expect("valid zero count"),
            })
        }
        DeclaredType::Str | DeclaredType::Bytes => None,
    }
}

/// Every eligible column's stamp over `rows`.
pub(crate) fn stamps_of(
    cols: &[DeclaredColumn],
    rows: &[Row],
    fields: Fields,
) -> Vec<DeclaredColumnStat> {
    cols.iter()
        .filter_map(|c| stamp_of(&c.key, c.ty, rows, fields))
        .collect()
}

/// `stamps` as a commit record of `sample_count` rows would carry them,
/// validated by the real reader.
pub(crate) fn validated(stamps: &[DeclaredColumnStat], sample_count: u64) -> DeclaredColumnStats {
    let mut record = CommitRecord {
        sample_count,
        ..Default::default()
    };
    ravel_commit::declared_stats::stamp_commit_record(&mut record, stamps);
    let read = ravel_commit::declared_stats::read_commit_record(&record);
    assert_eq!(read.covered().len(), stamps.len(), "fixture stamps valid");
    DeclaredColumnStats::from_validated(&read)
}

fn i64_value(v: i64) -> ColumnValue {
    ColumnValue {
        kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(v)),
    }
}

/// The `.cstat` entry a fold builds for I64 column `col` over `rows`.
pub(crate) fn cstat_of(col: &str, rows: &[Row]) -> ColumnStat {
    let mut dict: Vec<(i64, u64)> = Vec::new();
    let mut null_count = 0u64;
    for r in rows {
        match r.get(col) {
            Some(AttrValue::I64(v)) => match dict.iter_mut().find(|(dv, _)| dv == v) {
                Some(entry) => entry.1 += 1,
                None => dict.push((*v, 1)),
            },
            _ => null_count += 1,
        }
    }
    dict.sort_unstable();
    let sum: i128 = dict
        .iter()
        .map(|(v, c)| i128::from(*v) * i128::from(*c))
        .sum();
    ColumnStat {
        name: col.to_string(),
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
        sum: i64::try_from(sum).ok(),
    }
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

/// Loaded `.cstat` carrying `columns` for each segment.
pub(crate) fn loaded(entries: Vec<(&SegmentRef, Vec<ColumnStat>)>) -> Arc<LoadedColumnStats> {
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

// ---------------------------------------------------------------------------
// Provider-level corpus
// ---------------------------------------------------------------------------

const TENANT: TenantHash = TenantHash([0x46; 16]);

/// `a`, `w` I64, `s` Str, `f` Bool.
pub(crate) fn cols() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("a", DeclaredType::I64),
        DeclaredColumn::new("w", DeclaredType::I64),
        DeclaredColumn::new("s", DeclaredType::Str),
        DeclaredColumn::new("f", DeclaredType::Bool),
    ]
}

/// Real RLOG objects plus their unstamped refs and rows.
pub(crate) struct Corpus {
    pub(crate) store: Arc<CountingStore>,
    pub(crate) bare: Vec<SegmentRef>,
    pub(crate) rows: Vec<Vec<Row>>,
}

/// What one run of a statement produced.
pub(crate) struct Run {
    pub(crate) plan: String,
    pub(crate) answer: datafusion::error::Result<Vec<RecordBatch>>,
    pub(crate) gets: u64,
}

/// How a run is set up.
pub(crate) struct Setup {
    pub(crate) segs: Vec<SegmentRef>,
    pub(crate) stats: Option<Arc<LoadedColumnStats>>,
    pub(crate) erasure: Vec<ErasureRequest>,
    pub(crate) key_index: Option<KeyIndexDeclaration>,
}

impl Corpus {
    pub(crate) async fn build(parts: &[Vec<Row>]) -> Corpus {
        let store = CountingStore::new(Arc::new(MemoryStore::new()));
        let mut bare = Vec::new();
        for (i, rows) in parts.iter().enumerate() {
            let seq = i as u64 + 1;
            let key = format!("logs/stamp-{seq}.rlog");
            let mut w = RlogWriter::new(
                RlogConfig::default(),
                ObjectIdentity {
                    tenant_hash: TENANT.0,
                    shard: 0,
                    writer_id: [4u8; 16],
                    writer_epoch: 1,
                    writer_seq: seq,
                },
            );
            for r in rows {
                w.push(log_record(r)).expect("push");
            }
            let bytes = w.finish().expect("finish");
            let object_size = bytes.len() as u64;
            store
                .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
                .await
                .expect("put");
            bare.push(SegmentRef {
                data_object_key: key,
                object_size,
                min_event_ts_ns: rows.iter().map(|r| r.ts).min().expect("nonempty"),
                max_event_ts_ns: rows.iter().map(|r| r.ts).max().expect("nonempty"),
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
            });
        }
        Corpus {
            store,
            bare,
            rows: parts.to_vec(),
        }
    }

    /// Segment `i` carrying `stamps`.
    pub(crate) fn seg_with(&self, i: usize, stamps: &[DeclaredColumnStat]) -> SegmentRef {
        SegmentRef {
            declared_column_stats: validated(stamps, self.bare[i].sample_count),
            ..self.bare[i].clone()
        }
    }

    /// Segment `i` carrying what the real decode keeps of `stamps`, with the
    /// number of entries it dropped.
    pub(crate) fn seg_read(&self, i: usize, stamps: &[DeclaredColumnStat]) -> (SegmentRef, usize) {
        let mut record = CommitRecord {
            sample_count: self.bare[i].sample_count,
            ..Default::default()
        };
        ravel_commit::declared_stats::stamp_commit_record(&mut record, stamps);
        let read = ravel_commit::declared_stats::read_commit_record(&record);
        let seg = SegmentRef {
            declared_column_stats: DeclaredColumnStats::from_validated(&read),
            ..self.bare[i].clone()
        };
        (seg, read.dropped().len())
    }

    /// Every segment stamped from its own rows.
    pub(crate) fn segs(&self, fields: Fields) -> Vec<SegmentRef> {
        (0..self.bare.len())
            .map(|i| self.seg_with(i, &stamps_of(&cols(), &self.rows[i], fields)))
            .collect()
    }

    /// `.cstat` for `a` and `w` on every segment.
    pub(crate) fn cstat(&self, segs: &[SegmentRef]) -> Arc<LoadedColumnStats> {
        loaded(
            segs.iter()
                .zip(&self.rows)
                .map(|(seg, rows)| (seg, vec![cstat_of("a", rows), cstat_of("w", rows)]))
                .collect(),
        )
    }

    pub(crate) fn setup(&self, fields: Fields) -> Setup {
        Setup {
            segs: self.segs(fields),
            stats: None,
            erasure: Vec::new(),
            key_index: None,
        }
    }

    pub(crate) async fn run(&self, sql: &str, setup: Setup) -> Run {
        let backend: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&self.store) as Arc<dyn ObjectStoreBackend>;
        let mut provider = LogsTableProvider::new(
            Snapshot {
                segments: setup.segs,
                segments_pruned: 0,
                pending_erasure: setup.erasure,
            },
            TENANT,
            LogSegmentFetcher::new(backend),
            PhaseAccounting::new(),
        )
        .with_declared_columns(cols())
        .with_column_stats(setup.stats);
        if let Some(key_index) = setup.key_index {
            provider = provider.with_key_index(key_index);
        }
        let config = SqlConfig::default();
        let (pool, _breach) =
            config.query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
        let ctx = build_session(
            &config,
            pool,
            SessionTable::Logs(Arc::new(provider)),
            false,
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
        let before = self.store.gets();
        let answer = collect(plan, ctx.task_ctx()).await;
        Run {
            plan: shown,
            answer,
            gets: self.store.gets() - before,
        }
    }

    /// The statement over stamps with fields 6 to 8 and no `.cstat`.
    pub(crate) async fn stamped(&self, sql: &str) -> Run {
        self.run(sql, self.setup(Fields::With)).await
    }

    /// The forced scan: fields 6 to 8 stripped, no `.cstat`.
    pub(crate) async fn forced(&self, sql: &str) -> Run {
        self.run(sql, self.setup(Fields::Without)).await
    }

    /// The same objects with no stamp at all, so no statistic answers: the
    /// reference for a not-equal literal that a pre-ADR stamp's extrema
    /// already decide (outside `[min, max]`, or `min == max == lit`).
    pub(crate) async fn truth(&self, sql: &str) -> Run {
        let setup = Setup {
            segs: self.bare.clone(),
            stats: None,
            erasure: Vec::new(),
            key_index: None,
        };
        self.run(sql, setup).await
    }
}

pub(crate) fn assert_answered(run: &Run, what: &str) {
    assert!(
        run.plan.contains("MetadataOnlyExec: metadata_only=true")
            && !run.plan.contains("LogsScanExec"),
        "{what}: answered from statistics; plan was:\n{}",
        run.plan
    );
    assert_eq!(run.gets, 0, "{what}: no data-object GET");
}

pub(crate) fn assert_scanned(run: &Run, what: &str) {
    assert!(
        run.plan.contains("LogsScanExec") && !run.plan.contains("MetadataOnlyExec"),
        "{what}: must scan; plan was:\n{}",
        run.plan
    );
    assert!(run.gets > 0, "{what}: a scan reads data objects");
}

/// The answer as one batch, or `None` for zero rows.
fn one_batch(batches: &[RecordBatch]) -> Option<RecordBatch> {
    let first = batches.first()?;
    Some(concat_batches(&first.schema(), batches).expect("concat"))
}

/// Same rows, same types, NULL for NULL, floats equal bit for bit; or the
/// same error.
pub(crate) fn assert_same_answer(left: &Run, right: &Run, what: &str) {
    match (&left.answer, &right.answer) {
        (Err(l), Err(r)) => assert_eq!(l.to_string(), r.to_string(), "{what}: same error"),
        (Ok(l), Ok(r)) => {
            let (l, r) = (one_batch(l), one_batch(r));
            let (Some(l), Some(r)) = (l.as_ref(), r.as_ref()) else {
                assert_eq!(
                    l.map(|b| b.num_rows()).unwrap_or(0),
                    r.map(|b| b.num_rows()).unwrap_or(0),
                    "{what}: row count"
                );
                return;
            };
            assert_eq!(l.num_rows(), r.num_rows(), "{what}: row count");
            assert_eq!(l.num_columns(), r.num_columns(), "{what}: width");
            for (i, (lc, rc)) in l.columns().iter().zip(r.columns()).enumerate() {
                assert_eq!(lc.data_type(), rc.data_type(), "{what}: column {i} type");
                assert_eq!(lc.to_data(), rc.to_data(), "{what}: column {i} value");
                if let (Some(lf), Some(rf)) = (
                    lc.as_any().downcast_ref::<Float64Array>(),
                    rc.as_any().downcast_ref::<Float64Array>(),
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

/// The answer's columns of a one-row result.
pub(crate) fn columns(run: &Run) -> Vec<ArrayRef> {
    let batch = one_batch(run.answer.as_ref().expect("an answer")).expect("a row");
    assert_eq!(batch.num_rows(), 1, "one row");
    batch.columns().to_vec()
}

pub(crate) fn pretty(run: &Run) -> String {
    pretty_format_batches(run.answer.as_ref().expect("an answer"))
        .expect("format")
        .to_string()
}

/// A pending erasure on `user = subject`.
pub(crate) fn erasure_on(subject: &str) -> Vec<ErasureRequest> {
    vec![ErasureRequest {
        predicate: vec![ErasurePredicateMatcher {
            key: "user".to_string(),
            value: subject.to_string(),
        }],
        ..Default::default()
    }]
}

/// Two objects over `a` and `w`, with zero values, NULLs, a one-valued
/// column on the second object, and a row matching the erasure subject.
pub(crate) fn two_parts() -> Vec<Vec<Row>> {
    vec![
        vec![
            row(0, Some(0), Some(1368)),
            row(10, Some(2), Some(1917)).with("user", AttrValue::Str("subject-s".into())),
            row(20, None, Some(1368)),
            row(30, Some(0), None),
            row(40, Some(5), Some(800)),
        ],
        vec![
            row(100, Some(3), Some(7)),
            row(110, None, Some(7)),
            row(120, Some(0), Some(7)),
            row(130, Some(7), None),
        ],
    ]
}

pub(crate) fn ninety_sums(col: &str) -> String {
    let terms: Vec<String> = (0..90)
        .map(|k| {
            if k == 0 {
                format!("SUM({col})")
            } else {
                format!("SUM({col} + {k})")
            }
        })
        .collect();
    format!("SELECT {} FROM logs", terms.join(", "))
}

/// The answerable shapes, each over `a` and `w`.
pub(crate) fn answerable_shapes() -> Vec<String> {
    vec![
        "SELECT COUNT(*) FROM logs WHERE a <> 0".to_string(),
        "SELECT COUNT(*) FROM logs WHERE w <> 7".to_string(),
        "SELECT SUM(a), COUNT(*), AVG(w) FROM logs".to_string(),
        "SELECT AVG(a) FROM logs".to_string(),
        "SELECT SUM(a + 3) FROM logs".to_string(),
        ninety_sums("w"),
    ]
}

// ---------------------------------------------------------------------------
// Provider-level tests
// ---------------------------------------------------------------------------

/// Every answerable shape answers from the stamps alone, with no `.cstat` and
/// no data-object GET, and equals the forced scan, which scans.
#[tokio::test]
async fn every_shape_answers_from_stamps_with_no_cstat() {
    let corpus = Corpus::build(&two_parts()).await;
    for sql in answerable_shapes() {
        let run = corpus.stamped(&sql).await;
        assert_answered(&run, &sql);
        let truth = corpus.truth(&sql).await;
        assert_scanned(&truth, &sql);
        assert_same_answer(&run, &truth, &sql);
        // `w <> 7` is decided by the extrema alone (the first object's range
        // excludes 7, the second holds only 7), so the stripped stamps answer
        // it too; every other shape needs fields 6 to 8.
        let forced = corpus.forced(&sql).await;
        if sql.contains("w <> 7") {
            assert_answered(&forced, &sql);
        } else {
            assert_scanned(&forced, &sql);
        }
        assert_same_answer(&run, &forced, &sql);
    }
    // The figures, recomputed by hand from `two_parts`.
    let run = corpus
        .stamped("SELECT COUNT(*) FROM logs WHERE a <> 0")
        .await;
    let count = columns(&run)[0]
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Int64")
        .value(0);
    assert_eq!(count, 4, "a is 2, 5, 3, 7 on four rows");
}

/// `EXPLAIN` of the stamp answer keeps the one-line `MetadataOnlyExec` form.
#[tokio::test]
async fn explain_line_is_unchanged_for_a_stamp_answer() {
    let corpus = Corpus::build(&two_parts()).await;
    let run = corpus
        .stamped("EXPLAIN SELECT SUM(a), COUNT(*), AVG(w) FROM logs")
        .await;
    let text = pretty(&run);
    assert!(
        text.contains("MetadataOnlyExec: metadata_only=true, rows=1"),
        "explain was:\n{text}"
    );
}

/// Test 2: a pending erasure, stamps with the fields, no `.cstat`: every shape
/// scans, reads data objects, and equals a forced scan under the same erasure.
#[tokio::test]
async fn a_pending_erasure_scans_every_shape() {
    let corpus = Corpus::build(&two_parts()).await;
    for sql in answerable_shapes() {
        let mut stamped = corpus.setup(Fields::With);
        stamped.erasure = erasure_on("subject-s");
        let run = corpus.run(&sql, stamped).await;
        assert_scanned(&run, &sql);
        let mut forced = corpus.setup(Fields::Without);
        forced.erasure = erasure_on("subject-s");
        let scan = corpus.run(&sql, forced).await;
        assert_same_answer(&run, &scan, &sql);
    }
    // The erased row is the one with a = 2: the scan drops it.
    let mut stamped = corpus.setup(Fields::With);
    stamped.erasure = erasure_on("subject-s");
    let run = corpus.run("SELECT SUM(a) FROM logs", stamped).await;
    let sum = columns(&run)[0]
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Int64")
        .value(0);
    assert_eq!(
        sum, 15,
        "0 + 0 + 5 + 3 + 0 + 7, the erased row's 2 left out"
    );
}

/// Test 3: stamp sums past `i64::MAX` make `SUM` fail with the scan's
/// overflow error; `SUM(w + k)` at the edge does the same; `AVG` over an
/// object whose own sum needs field 7 answers with the scan's bits.
#[tokio::test]
async fn totals_outside_the_result_type_scan_and_field_7_answers_avg() {
    let big = i64::MAX - 1;
    let over = Corpus::build(&[
        vec![
            row(0, Some(big), Some(i64::MAX)),
            row(10, Some(big), Some(0)),
        ],
        vec![row(20, Some(3), Some(1))],
    ])
    .await;
    for sql in ["SELECT SUM(a) FROM logs", "SELECT SUM(w + 1) FROM logs"] {
        let run = over.stamped(sql).await;
        let scan = over.forced(sql).await;
        let err = run.answer.as_ref().expect_err("the total overflows");
        assert!(
            err.to_string().contains("overflow"),
            "{sql}: an overflow error, got {err}"
        );
        assert_same_answer(&run, &scan, sql);
    }
    assert_scanned(&over.stamped("SELECT SUM(a) FROM logs").await, "SUM(a)");

    // The first object's own `a` sum, 2 * (i64::MAX - 1), needs field 7.
    let stamps = stamps_of(&cols(), &over.rows[0], Fields::With);
    let a = stamps.iter().find(|s| s.name() == "a").expect("a stamped");
    assert_eq!(a.sum(), Some(2 * i128::from(big)));
    assert!(i64::try_from(a.sum().expect("sum")).is_err(), "field 7");
    let sql = "SELECT AVG(a) FROM logs";
    let run = over.stamped(sql).await;
    assert_answered(&run, sql);
    let scan = over.forced(sql).await;
    assert_scanned(&scan, sql);
    assert_same_answer(&run, &scan, sql);
}

/// Both carriers (decision 5, mutant b): one segment carries the fields and a
/// `.cstat` entry `merged_view_entry` grants; the other has only the entry.
/// The answer counts each segment once, and adding both carriers for the
/// first segment would change it.
#[tokio::test]
async fn a_segment_with_both_carriers_is_counted_once() {
    let corpus = Corpus::build(&two_parts()).await;
    let first = corpus.seg_with(0, &stamps_of(&cols(), &corpus.rows[0], Fields::With));
    let second = corpus.seg_with(1, &stamps_of(&cols(), &corpus.rows[1], Fields::Without));
    let segs = vec![first, second];
    let stats = corpus.cstat(&segs);
    for sql in answerable_shapes() {
        let run = corpus
            .run(
                &sql,
                Setup {
                    segs: segs.clone(),
                    stats: Some(Arc::clone(&stats)),
                    erasure: Vec::new(),
                    key_index: None,
                },
            )
            .await;
        assert_answered(&run, &sql);
        assert_same_answer(&run, &corpus.forced(&sql).await, &sql);
    }
    // The first segment's `a` sum is 7 and its non-zero count 2, so a double
    // count would read SUM(a) = 24 and the not-equal count 6, not 17 and 4.
    let run = corpus
        .run(
            "SELECT SUM(a) FROM logs",
            Setup {
                segs,
                stats: Some(stats),
                erasure: Vec::new(),
                key_index: None,
            },
        )
        .await;
    let sum = columns(&run)[0]
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Int64")
        .value(0);
    assert_eq!(sum, 17);
}

/// A segment whose stamp lacks the fields and has no `.cstat` declines the
/// whole statement (decision 2: an absent field is no answer, never 0).
#[tokio::test]
async fn one_segment_without_the_fields_scans() {
    let corpus = Corpus::build(&two_parts()).await;
    let segs = vec![
        corpus.seg_with(0, &stamps_of(&cols(), &corpus.rows[0], Fields::With)),
        corpus.seg_with(1, &stamps_of(&cols(), &corpus.rows[1], Fields::Without)),
    ];
    for sql in [
        "SELECT SUM(a) FROM logs",
        "SELECT AVG(w) FROM logs",
        "SELECT COUNT(*) FROM logs WHERE a <> 0",
    ] {
        let run = corpus
            .run(
                sql,
                Setup {
                    segs: segs.clone(),
                    stats: None,
                    erasure: Vec::new(),
                    key_index: None,
                },
            )
            .await;
        assert_scanned(&run, sql);
        assert_same_answer(&run, &corpus.forced(sql).await, sql);
    }
}

// ---------------------------------------------------------------------------
// Executor-level harness: commit records through a real Catalog
// ---------------------------------------------------------------------------

pub(crate) const NS_PER_SEC: i64 = 1_000_000_000;
pub(crate) const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
pub(crate) const BASE_HOUR: u32 = 500_000;

/// GETs by key kind, counted below every cache.
#[derive(Default)]
pub(crate) struct KeyCountingStore {
    inner: MemoryStore,
    data_gets: AtomicU64,
    head_gets: AtomicU64,
    other_gets: AtomicU64,
    get_keys: std::sync::Mutex<Vec<String>>,
}

impl KeyCountingStore {
    pub(crate) fn data_gets(&self) -> u64 {
        self.data_gets.load(Ordering::Acquire)
    }

    pub(crate) fn take_keys(&self) -> Vec<String> {
        std::mem::take(&mut *self.get_keys.lock().expect("keys"))
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for KeyCountingStore {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        let parts: Vec<&str> = key.split('/').collect();
        let counter = if parts.len() > 3 && matches!(parts[3], "l0" | "l1" | "rw") {
            &self.data_gets
        } else if key.ends_with("/HEAD") {
            &self.head_gets
        } else {
            &self.other_gets
        };
        counter.fetch_add(1, Ordering::AcqRel);
        self.get_keys.lock().expect("keys").push(key.to_string());
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

pub(crate) fn tenant() -> TenantId {
    TenantId::new("stamp-readers-4684".to_string())
}

/// The ClickBench columns of test 1.
pub(crate) fn clickbench_cols() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("AdvEngineID", DeclaredType::I64),
        DeclaredColumn::new("ResolutionWidth", DeclaredType::I64),
        DeclaredColumn::new("UserID", DeclaredType::I64),
    ]
}

pub(crate) fn cb_row(ts: i64, adv: Option<i64>, width: Option<i64>, user: Option<i64>) -> Row {
    let mut attrs = Vec::new();
    if let Some(v) = adv {
        attrs.push(("AdvEngineID", AttrValue::I64(v)));
    }
    if let Some(v) = width {
        attrs.push(("ResolutionWidth", AttrValue::I64(v)));
    }
    if let Some(v) = user {
        attrs.push(("UserID", AttrValue::I64(v)));
    }
    Row { ts, attrs }
}

/// Three objects; the first one's `UserID` sum leaves `i64` (field 7).
pub(crate) fn clickbench_parts() -> Vec<Vec<Row>> {
    let h = i64::from(BASE_HOUR) * NS_PER_HOUR;
    let big = i64::MAX - 10;
    vec![
        vec![
            cb_row(h + 1, Some(0), Some(1368), Some(big)),
            cb_row(h + 2, Some(0), Some(1917), Some(big - 1)),
            cb_row(h + 3, Some(2), None, Some(big - 2)),
            cb_row(h + 4, None, Some(1368), Some(5)),
            cb_row(h + 5, Some(0), Some(800), None),
        ],
        vec![
            cb_row(h + 11, Some(3), Some(1024), Some(-4)),
            cb_row(h + 12, Some(0), Some(640), Some(435_090_932_899_640_449)),
            cb_row(h + 13, None, Some(1920), Some(1)),
            cb_row(h + 14, Some(7), Some(1368), None),
        ],
        vec![
            cb_row(h + 21, Some(0), None, Some(100)),
            cb_row(h + 22, Some(0), Some(1366), Some(200)),
        ],
    ]
}

/// Write `rows` as a real RLOG object and return its bytes.
pub(crate) fn rlog_bytes(tenant: &TenantId, seq: u64, rows: &[Row]) -> Vec<u8> {
    let mut w = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: [9u8; 16],
            writer_epoch: 1,
            writer_seq: seq,
        },
    );
    for r in rows {
        w.push(log_record(r)).expect("push");
    }
    w.finish().expect("finish")
}

/// The commit record of object `seq` holding `rows`, carrying `stamps`.
fn record_with_stamps(
    tenant: &TenantId,
    seq: u64,
    hour: u32,
    rows: &[Row],
    bytes: &[u8],
    stamps: &[DeclaredColumnStat],
) -> CommitRecord {
    let min = rows.iter().map(|r| r.ts).min().expect("nonempty");
    let max = rows.iter().map(|r| r.ts).max().expect("nonempty");
    let mut rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(4_684),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(bytes).as_bytes(),
        sample_count: rows.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: i64::from(hour) * NS_PER_HOUR,
        max_ingest_ts_ns: i64::from(hour) * NS_PER_HOUR,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: max,
        ingest_hour_bucket: hour,
    })
    .expect("valid logs commit record");
    ravel_commit::declared_stats::stamp_commit_record(&mut rec, stamps);
    rec
}

/// Write object `seq` and publish its stamped commit record, as a flush does.
pub(crate) async fn publish_object(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    seq: u64,
    hour: u32,
    rows: &[Row],
    cols: &[DeclaredColumn],
    fields: Fields,
) -> CommitRecord {
    publish_with_stamps(
        store,
        tenant,
        seq,
        hour,
        rows,
        &stamps_of(cols, rows, fields),
    )
    .await
}

/// [`publish_object`] with the stamps given per column.
pub(crate) async fn publish_with_stamps(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    seq: u64,
    hour: u32,
    rows: &[Row],
    stamps: &[DeclaredColumnStat],
) -> CommitRecord {
    let bytes = rlog_bytes(tenant, seq, rows);
    let rec = record_with_stamps(tenant, seq, hour, rows, &bytes, stamps);
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
    rec
}

pub(crate) fn sql_config() -> SqlConfig {
    let mut config = SqlConfig::default();
    config.engine.max_segments = 1_000_000;
    config
}

pub(crate) fn executor(store: &Arc<KeyCountingStore>, cols: Vec<DeclaredColumn>) -> SqlExecutor {
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(store) as Arc<dyn ObjectStoreBackend>;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog"));
    executor_on(catalog, &backend, cols)
}

pub(crate) fn executor_on(
    catalog: Arc<Catalog>,
    backend: &Arc<dyn ObjectStoreBackend>,
    cols: Vec<DeclaredColumn>,
) -> SqlExecutor {
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(backend)),
        LogSegmentFetcher::new(Arc::clone(backend)),
        SpanSegmentFetcher::new(Arc::clone(backend)),
        sql_config(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(cols)))
}

/// A request over `BASE_HOUR`, at an instant inside the next hour (so the
/// hour is not sealed and no fold is assumed).
pub(crate) fn request(sql: &str, tokens: Vec<CommitToken>) -> SqlRequest {
    let start = i64::from(BASE_HOUR) * NS_PER_HOUR;
    let now = start + NS_PER_HOUR + 60 * NS_PER_SEC;
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: start,
            end_ns: now,
        },
        min_tokens: tokens
            .into_iter()
            .map(|token| ravel_sql::MinCommitToken { table: None, token })
            .collect(),
        now_ns: now,
        deadline: Duration::from_secs(120),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// Per-phase GETs and wire bytes of one statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Phases {
    pub(crate) resolve: (u64, u64),
    pub(crate) plan: (u64, u64),
    pub(crate) probe: (u64, u64),
    pub(crate) scan: (u64, u64),
    pub(crate) key_index: (u64, u64),
    pub(crate) fetch: (u64, u64),
}

pub(crate) fn phases(outcome: &SqlOutcome) -> Phases {
    let pa = &outcome.phase_accounting;
    let of = |s: &ravel_types::accounting::QueryAccountingSnapshot| {
        (
            s.s3_requests(AccountedOp::Get),
            s.s3_bytes(AccountedOp::Get),
        )
    };
    Phases {
        resolve: of(&pa.resolve),
        plan: of(&pa.plan),
        probe: of(&pa.probe),
        scan: of(&pa.scan),
        key_index: of(&pa.key_index),
        fetch: of(&pa.fetch),
    }
}

/// One statement through the executor.
pub(crate) struct ExecRun {
    pub(crate) plan: String,
    pub(crate) answer: Run,
    pub(crate) phases: Phases,
    pub(crate) data_gets: u64,
    pub(crate) get_keys: Vec<String>,
    /// The executed statement's stats; `None` when it failed.
    pub(crate) stats: Option<ravel_sql::SqlStats>,
}

pub(crate) async fn exec(
    store: &Arc<KeyCountingStore>,
    executor: &SqlExecutor,
    sql: &str,
    tokens: Vec<CommitToken>,
) -> ExecRun {
    exec_request(store, executor, &tenant(), request(sql, tokens)).await
}

/// [`exec`] for `req` against `tenant`.
pub(crate) async fn exec_request(
    store: &Arc<KeyCountingStore>,
    executor: &SqlExecutor,
    tenant: &TenantId,
    req: SqlRequest,
) -> ExecRun {
    let th = tenant.hash();
    store.take_keys();
    let before = store.data_gets();
    let outcome = executor.execute(th, &req).await;
    let data_gets = store.data_gets() - before;
    let get_keys = store.take_keys();
    let (answer, phases_of, stats) = match outcome {
        Ok(outcome) => {
            let p = phases(&outcome);
            (
                Ok(outcome.output.batches().to_vec()),
                Some(p),
                Some(outcome.stats.clone()),
            )
        }
        Err(e) => (
            Err(datafusion::error::DataFusionError::Execution(e.to_string())),
            None,
            None,
        ),
    };
    let plan = executor
        .explain(th, &req)
        .await
        .expect("statement explains")
        .plan_text;
    store.take_keys();
    ExecRun {
        plan: plan.clone(),
        answer: Run {
            plan,
            answer,
            gets: data_gets,
        },
        phases: phases_of.unwrap_or(Phases {
            resolve: (0, 0),
            plan: (0, 0),
            probe: (0, 0),
            scan: (0, 0),
            key_index: (0, 0),
            fetch: (0, 0),
        }),
        data_gets,
        get_keys,
        stats,
    }
}

/// A store holding `parts` published at `BASE_HOUR`, stamped with `fields`.
pub(crate) async fn clickbench_store(fields: Fields) -> Arc<KeyCountingStore> {
    let store = Arc::new(KeyCountingStore::default());
    for (i, rows) in clickbench_parts().iter().enumerate() {
        publish_object(
            store.as_ref(),
            &tenant(),
            i as u64 + 1,
            BASE_HOUR,
            rows,
            &clickbench_cols(),
            fields,
        )
        .await;
    }
    store
}

fn clickbench_q29() -> String {
    let terms: Vec<String> = (0..90)
        .map(|k| {
            if k == 0 {
                "SUM(\"ResolutionWidth\")".to_string()
            } else {
                format!("SUM(\"ResolutionWidth\" + {k})")
            }
        })
        .collect();
    format!("SELECT {} FROM logs", terms.join(", "))
}

pub(crate) const Q1: &str = "SELECT COUNT(*) FROM logs WHERE \"AdvEngineID\" <> 0";
const Q2: &str = "SELECT SUM(\"AdvEngineID\"), COUNT(*), AVG(\"ResolutionWidth\") FROM logs";
const Q3: &str = "SELECT AVG(\"UserID\") FROM logs";

/// Test 1, one statement: answered from the stamps of unfolded commit records
/// with no data-object GET, against the forced scan of the same objects.
async fn assert_clickbench_statement(sql: &str) -> (Phases, Phases) {
    let stamped_store = clickbench_store(Fields::With).await;
    let forced_store = clickbench_store(Fields::Without).await;
    let stamped_exec = executor(&stamped_store, clickbench_cols());
    let forced_exec = executor(&forced_store, clickbench_cols());

    let run = exec(&stamped_store, &stamped_exec, sql, Vec::new()).await;
    let scan = exec(&forced_store, &forced_exec, sql, Vec::new()).await;

    assert!(
        run.plan
            .contains("MetadataOnlyExec: metadata_only=true, rows=1")
            && !run.plan.contains("LogsScanExec"),
        "{sql}: answered from the stamps; plan was:\n{}",
        run.plan
    );
    assert_eq!(run.data_gets, 0, "{sql}: no data-object GET");
    let p = run.phases;
    assert_eq!(
        (p.probe.0, p.scan.0, p.key_index.0, p.fetch.0),
        (0, 0, 0, 0),
        "{sql}: probe, scan, key_index and fetch GETs: {p:?}"
    );
    // Every segment's stamp answers every figure, so the statistics load is
    // skipped (T5b): no plan-phase GET. The resolve reads the absent catalog
    // HEAD once; every other GET is a commit record.
    let head = format!("t/{}/catalog/l/HEAD", tenant().hash().to_hex());
    let non_record: Vec<&String> = run
        .get_keys
        .iter()
        .filter(|k| !k.ends_with(".cmt"))
        .collect();
    assert_eq!(p.plan.0, 0, "{sql}: no plan-phase GET: {p:?}");
    assert_eq!(
        non_record,
        vec![&head],
        "{sql}: the resolve's HEAD read only"
    );
    assert_eq!(
        p.resolve.0,
        clickbench_parts().len() as u64 + 1,
        "{sql}: one GET per commit record plus the HEAD"
    );

    assert!(
        scan.plan.contains("LogsScanExec") && !scan.plan.contains("MetadataOnlyExec"),
        "{sql}: the forced scan scans; plan was:\n{}",
        scan.plan
    );
    assert!(scan.data_gets > 0, "{sql}: the forced scan reads objects");
    assert_eq!(
        scan.phases.plan.0, 1,
        "{sql}: the stripped stamps keep the load, the absent HEAD: {:?}",
        scan.phases
    );
    assert_eq!(
        p.resolve.0, scan.phases.resolve.0,
        "{sql}: resolve GETs equal the forced scan's"
    );
    assert_same_answer(&run.answer, &scan.answer, sql);
    (run.phases, scan.phases)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn q1_answers_from_the_zero_count_with_no_data_object_get() {
    assert_clickbench_statement(Q1).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn q2_answers_from_the_stamp_with_no_data_object_get() {
    let (stamp, scan) = assert_clickbench_statement(Q2).await;
    println!("COST q2 stamp per phase (GETs, wire bytes): {stamp:?}");
    println!("COST q2 forced scan per phase (GETs, wire bytes): {scan:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn q3_answers_from_the_wide_sum_with_no_data_object_get() {
    let parts = clickbench_parts();
    let stamps = stamps_of(&clickbench_cols(), &parts[0], Fields::With);
    let user = stamps
        .iter()
        .find(|s| s.name() == "UserID")
        .expect("stamped");
    assert!(
        i64::try_from(user.sum().expect("sum")).is_err(),
        "the first object's UserID sum needs field 7"
    );
    assert_clickbench_statement(Q3).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn q29_answers_from_the_stamp_with_no_data_object_get() {
    assert_clickbench_statement(&clickbench_q29()).await;
}

/// Test 4: a token-resolved segment lacking the fields scans; with the fields
/// it answers from its stamp; both equal a forced scan. The token's record
/// sits at an ingest hour below the listing window, so only the token's GET
/// reaches it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_token_resolved_segment_without_the_fields_scans() {
    let h = i64::from(BASE_HOUR) * NS_PER_HOUR;
    let token_rows = vec![
        cb_row(h + 31, Some(0), Some(10), Some(1)),
        cb_row(h + 32, Some(4), Some(20), None),
    ];
    let token_hour = BASE_HOUR - 400;
    let build = |token_fields: Fields, listed_fields: Fields| {
        let rows = token_rows.clone();
        async move {
            let store = Arc::new(KeyCountingStore::default());
            for (i, part) in clickbench_parts().iter().enumerate() {
                publish_object(
                    store.as_ref(),
                    &tenant(),
                    i as u64 + 1,
                    BASE_HOUR,
                    part,
                    &clickbench_cols(),
                    listed_fields,
                )
                .await;
            }
            let rec = publish_object(
                store.as_ref(),
                &tenant(),
                99,
                token_hour,
                &rows,
                &clickbench_cols(),
                token_fields,
            )
            .await;
            (store, record::token_for(&rec).expect("token"))
        }
    };
    let count_sql = "SELECT COUNT(*) FROM logs";
    for (token_fields, answers) in [(Fields::Without, false), (Fields::With, true)] {
        let (store, token) = build(token_fields, Fields::With).await;
        let (forced_store, forced_token) = build(Fields::Without, Fields::Without).await;
        let ex = executor(&store, clickbench_cols());
        let forced_ex = executor(&forced_store, clickbench_cols());

        // Only the token reaches the record: 11 listed rows, 13 with it.
        let listed = exec(&store, &ex, count_sql, Vec::new()).await;
        let with_token = exec(&store, &ex, count_sql, vec![token.clone()]).await;
        assert_eq!(pretty(&listed.answer).matches("| 11 ").count(), 1);
        assert_eq!(pretty(&with_token.answer).matches("| 13 ").count(), 1);

        for sql in [Q1, Q2, Q3] {
            let run = exec(&store, &ex, sql, vec![token.clone()]).await;
            let scan = exec(&forced_store, &forced_ex, sql, vec![forced_token.clone()]).await;
            if answers {
                assert!(
                    run.plan.contains("MetadataOnlyExec") && !run.plan.contains("LogsScanExec"),
                    "{sql}: answered from the token's stamp; plan was:\n{}",
                    run.plan
                );
                assert_eq!(run.data_gets, 0, "{sql}");
            } else {
                assert!(
                    run.plan.contains("LogsScanExec") && !run.plan.contains("MetadataOnlyExec"),
                    "{sql}: the token's segment lacks the fields; plan was:\n{}",
                    run.plan
                );
                assert!(run.data_gets > 0, "{sql}");
            }
            assert_same_answer(&run.answer, &scan.answer, sql);
        }
    }
}

// ---------------------------------------------------------------------------
// Test 6: erasure rewrite parts and a pre-rewrite input
// ---------------------------------------------------------------------------

const REQUEST_Q: u128 = 0x4684_d001;

fn erasure_tenant() -> TenantId {
    TenantId::new("stamp-readers-erasure-4684".to_string())
}

fn erasure_cols() -> Vec<DeclaredColumn> {
    vec![DeclaredColumn::new("a", DeclaredType::I64)]
}

/// Input A's rows; the second is the erased subject's.
fn input_rows() -> Vec<Row> {
    let h = i64::from(BASE_HOUR) * NS_PER_HOUR;
    vec![
        row(h + 1, Some(0), None),
        row(h + 2, Some(40), None).with("user", AttrValue::Str("subject-s".into())),
        row(h + 3, Some(5), None),
    ]
}

fn identity(record: &CommitRecord) -> CompactionInputIdentity {
    CompactionInputIdentity {
        writer_id: record.writer_id.clone(),
        writer_epoch: record.writer_epoch,
        writer_seq: record.writer_seq,
    }
}

/// The rewrite R of input A applying request Q, its one part P a real RLOG
/// object holding A's surviving rows, with an empty stamp as the erasure
/// rewrite writes it.
async fn put_rewrite(store: &dyn ObjectStoreBackend, a: &CommitRecord) -> RewriteRecord {
    let t = erasure_tenant();
    let survivors: Vec<Row> = input_rows()
        .into_iter()
        .filter(|r| r.get("user").is_none())
        .collect();
    let bytes = rlog_bytes(&t, 500, &survivors);
    let request_id = Uuid::from_u128(REQUEST_Q).to_string();
    let input_ids = vec![identity(a)];
    let start = i64::from(BASE_HOUR) * NS_PER_HOUR;
    let record = RewriteRecord {
        format_version: 1,
        tenant_hash: t.hash().0.to_vec(),
        signal: signal::to_proto(Signal::Logs) as i32,
        shard: 0,
        ingest_hour_bucket: BASE_HOUR,
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &input_ids,
            None,
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        inputs: input_ids,
        parts: vec![CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xff; 16],
            content_hash: blake3::hash(&bytes).as_bytes().to_vec(),
            object_size: bytes.len() as u64,
            sample_count: survivors.len() as u64,
            series_count: 1,
            run_count: 1,
            min_event_ts_ns: survivors.iter().map(|r| r.ts).min().expect("rows"),
            max_event_ts_ns: survivors.iter().map(|r| r.ts).max().expect("rows"),
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            declared_column_stats: Vec::new(),
        }],
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 1,
        }],
        created_unix_ns: start + 5_000,
        superseded_record_key: String::new(),
    };
    let part_key = keys::reconstruct_rewrite_part_key(&record, &record.parts[0]).expect("part key");
    store
        .put(&part_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rewrite part");
    store
        .put(
            &keys::rewrite_record_key_for(&record).expect("rewrite key"),
            erasure::encode_rewrite(&record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put rewrite record");
    record
}

async fn put_dreq(store: &dyn ObjectStoreBackend) {
    let t = erasure_tenant();
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: t.hash().0.to_vec(),
        signal: signal::to_proto(Signal::Logs) as i32,
        request_id: Uuid::from_u128(REQUEST_Q).to_string(),
        created_unix_ns: i64::from(BASE_HOUR) * NS_PER_HOUR + 4_000,
        predicate: vec![ErasurePredicateMatcher {
            key: "user".to_string(),
            value: "subject-s".to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    store
        .put(
            &keys::erasure_request_key(&t.hash(), Signal::Logs, Uuid::from_u128(REQUEST_Q))
                .expect("dreq key"),
            erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put dreq");
}

/// The erasure pass's `.done`. The `.dreq` stays: the sweep keeps it while a
/// superseded input is still held (docs/deletion-and-gc.md).
async fn put_done(store: &dyn ObjectStoreBackend) {
    let t = erasure_tenant();
    let completion = ErasureCompletion {
        format_version: 1,
        tenant_hash: t.hash().0.to_vec(),
        signal: signal::to_proto(Signal::Logs) as i32,
        request_id: Uuid::from_u128(REQUEST_Q).to_string(),
        predicate_hash: vec![0u8; 32],
        bucket_drops: Vec::new(),
        requested_unix_ns: i64::from(BASE_HOUR) * NS_PER_HOUR + 4_000,
        completed_unix_ns: i64::from(BASE_HOUR) * NS_PER_HOUR + 20_000,
        ..Default::default()
    };
    store
        .put(
            &keys::erasure_completion_key_for(&completion).expect("done key"),
            erasure::encode_completion(&completion),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put done");
}

/// The instant a fold seals `BASE_HOUR` at, and a request at it.
fn sealed_request(sql: &str, tokens: Vec<CommitToken>) -> SqlRequest {
    let cfg = CatalogConfig::default();
    let start = i64::from(BASE_HOUR) * NS_PER_HOUR;
    let now = start
        + NS_PER_HOUR
        + cfg.max_flush_lifetime_ns
        + cfg.clock_skew_allowance_ns
        + cfg.fold_safety_margin_ns
        + NS_PER_SEC;
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: start,
            end_ns: now,
        },
        min_tokens: tokens
            .into_iter()
            .map(|token| ravel_sql::MinCommitToken { table: None, token })
            .collect(),
        now_ns: now,
        deadline: Duration::from_secs(120),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// One world of test 6: input A stamped with `fields`, warmed into the query
/// node's commit-record cache by a token read, then rewritten.
pub(crate) struct ErasureWorld {
    store: Arc<KeyCountingStore>,
    executor: SqlExecutor,
    token: CommitToken,
}

async fn erasure_world(fields: Fields) -> ErasureWorld {
    let t = erasure_tenant();
    let store = Arc::new(KeyCountingStore::default());
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let a = publish_object(
        store.as_ref(),
        &t,
        1,
        BASE_HOUR,
        &input_rows(),
        &erasure_cols(),
        fields,
    )
    .await;
    let token = record::token_for(&a).expect("token");
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog"));
    let executor = executor_on(Arc::clone(&catalog), &backend, erasure_cols());
    // Warm the record cache with A through a token read.
    let warmed = executor
        .execute(
            t.hash(),
            &request("SELECT COUNT(*) FROM logs", vec![token.clone()]),
        )
        .await
        .expect("warm read");
    assert_eq!(warmed.output.num_rows(), 1);
    put_dreq(store.as_ref()).await;
    put_rewrite(store.as_ref(), &a).await;
    ErasureWorld {
        store,
        executor,
        token,
    }
}

pub(crate) async fn erasure_exec(world: &ErasureWorld, req: &SqlRequest) -> ExecRun {
    let th = erasure_tenant().hash();
    let before = world.store.data_gets();
    let outcome = world.executor.execute(th, req).await.expect("executes");
    let data_gets = world.store.data_gets() - before;
    let plan = world
        .executor
        .explain(th, req)
        .await
        .expect("explains")
        .plan_text;
    ExecRun {
        plan: plan.clone(),
        phases: phases(&outcome),
        answer: Run {
            plan,
            answer: Ok(outcome.output.batches().to_vec()),
            gets: data_gets,
        },
        data_gets,
        get_keys: Vec::new(),
        stats: Some(outcome.stats.clone()),
    }
}

/// Test 6 (decision 6): the rewrite part carries an empty stamp and the
/// statement over it scans; after the erasure completes, with a sealed part
/// and a warm record cache naming the pre-rewrite input whose stamp has the
/// fields, the request is still pending and the statement scans. Every answer
/// equals the forced scan of the same world.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rewrite_part_and_a_completed_erasure_scan() {
    let stamped = erasure_world(Fields::With).await;
    let forced = erasure_world(Fields::Without).await;
    let shapes = [
        "SELECT SUM(a) FROM logs",
        "SELECT AVG(a) FROM logs",
        "SELECT COUNT(*) FROM logs WHERE a <> 0",
    ];

    // The rewrite is listed, the request pending: P serves A's survivors.
    for sql in shapes {
        let req = request(sql, vec![stamped.token.clone()]);
        let run = erasure_exec(&stamped, &req).await;
        let scan = erasure_exec(&forced, &request(sql, vec![forced.token.clone()])).await;
        assert!(
            run.plan.contains("LogsScanExec") && !run.plan.contains("MetadataOnlyExec"),
            "{sql}: the rewrite part scans; plan was:\n{}",
            run.plan
        );
        assert!(run.data_gets > 0, "{sql}");
        assert_same_answer(&run.answer, &scan.answer, sql);
    }
    let run = erasure_exec(
        &stamped,
        &request("SELECT SUM(a) FROM logs", vec![stamped.token.clone()]),
    )
    .await;
    assert!(
        pretty(&run.answer).contains("| 5 "),
        "{}",
        pretty(&run.answer)
    );

    // Fold seals the hour onto a part naming P; the erasure completes; the
    // query node still caches A's record. The request is still pending.
    for world in [&stamped, &forced] {
        let backend: Arc<dyn ObjectStoreBackend> =
            Arc::clone(&world.store) as Arc<dyn ObjectStoreBackend>;
        let req = sealed_request("SELECT 1", Vec::new());
        Catalog::new(backend, CatalogConfig::default())
            .expect("catalog")
            .fold(
                &erasure_tenant().hash(),
                Signal::Logs,
                Uuid::from_u128(0x4684_f000),
                req.now_ns,
                &[],
                None,
            )
            .await
            .expect("fold");
        put_done(world.store.as_ref()).await;
    }
    for sql in shapes {
        let run = erasure_exec(&stamped, &sealed_request(sql, vec![stamped.token.clone()])).await;
        let scan = erasure_exec(&forced, &sealed_request(sql, vec![forced.token.clone()])).await;
        assert!(
            run.plan.contains("LogsScanExec") && !run.plan.contains("MetadataOnlyExec"),
            "{sql}: after the erasure completes the statement scans; plan was:\n{}",
            run.plan
        );
        assert!(run.data_gets > 0, "{sql}");
        assert_same_answer(&run.answer, &scan.answer, sql);
    }
    let run = erasure_exec(
        &stamped,
        &sealed_request("SELECT SUM(a) FROM logs", vec![stamped.token.clone()]),
    )
    .await;
    assert!(
        pretty(&run.answer).contains("| 5 "),
        "the erased 40 is never counted: {}",
        pretty(&run.answer)
    );
}

/// Input A alone, stamped with `fields`, unfolded, with no rewrite record;
/// the request on A's erased row is pending when `pending` is set.
pub(crate) async fn pre_rewrite_world(fields: Fields, pending: bool) -> ErasureWorld {
    let t = erasure_tenant();
    let store = Arc::new(KeyCountingStore::default());
    let a = publish_object(
        store.as_ref(),
        &t,
        1,
        BASE_HOUR,
        &input_rows(),
        &erasure_cols(),
        fields,
    )
    .await;
    if pending {
        put_dreq(store.as_ref()).await;
    }
    let executor = executor(&store, erasure_cols());
    ErasureWorld {
        store,
        executor,
        token: record::token_for(&a).expect("token"),
    }
}

const PRE_REWRITE_SHAPES: [&str; 3] = [
    "SELECT SUM(a) FROM logs",
    "SELECT AVG(a) FROM logs",
    "SELECT COUNT(*) FROM logs WHERE a <> 0",
];

/// A pre-rewrite input named while its erasure request is pending: no rewrite
/// record exists, so the snapshot names input A itself, whose stamp carries
/// the fields and counts the erased row. Listed and through A's token, every
/// shape scans, reads data objects and equals the forced scan of the same
/// world, and `SUM(a)` leaves the erased row's 40 out.
///
/// Prove-the-test: drop `self.erasure.is_empty()` from
/// `metadata_rewrite_stats_are_exact`; each shape then answers from A's
/// stamp, failing the plan assertion, and `SUM(a)` reads 45.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pre_rewrite_input_with_its_request_pending_scans() {
    let stamped = pre_rewrite_world(Fields::With, true).await;
    let forced = pre_rewrite_world(Fields::Without, true).await;
    for tokens in [false, true] {
        let pick = |w: &ErasureWorld| {
            if tokens {
                vec![w.token.clone()]
            } else {
                Vec::new()
            }
        };
        for sql in PRE_REWRITE_SHAPES {
            let run = erasure_exec(&stamped, &request(sql, pick(&stamped))).await;
            let scan = erasure_exec(&forced, &request(sql, pick(&forced))).await;
            assert!(
                run.plan.contains("LogsScanExec") && !run.plan.contains("MetadataOnlyExec"),
                "{sql} (token {tokens}): the pending request scans; plan was:\n{}",
                run.plan
            );
            assert!(run.data_gets > 0, "{sql} (token {tokens})");
            assert_same_answer(&run.answer, &scan.answer, sql);
        }
        let run = erasure_exec(
            &stamped,
            &request("SELECT SUM(a) FROM logs", pick(&stamped)),
        )
        .await;
        assert!(
            pretty(&run.answer).contains("| 5 "),
            "0 + 5, the erased 40 left out (token {tokens}): {}",
            pretty(&run.answer)
        );
    }
}

/// The same world with no request at all: the statement answers from A's
/// stamp, with no data-object GET, and equals the scan, both counting every
/// row A holds (`SUM(a)` is 0 + 40 + 5). A stamp answer over a pre-rewrite
/// input rests on its request staying pending while that input is held
/// (docs/deletion-and-gc.md, the sweep's hold rule), which this test does
/// not exercise.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pre_rewrite_input_with_no_request_pending_answers_as_the_scan_reads_it() {
    let stamped = pre_rewrite_world(Fields::With, false).await;
    let forced = pre_rewrite_world(Fields::Without, false).await;
    for tokens in [false, true] {
        let pick = |w: &ErasureWorld| {
            if tokens {
                vec![w.token.clone()]
            } else {
                Vec::new()
            }
        };
        for sql in PRE_REWRITE_SHAPES {
            let run = erasure_exec(&stamped, &request(sql, pick(&stamped))).await;
            let scan = erasure_exec(&forced, &request(sql, pick(&forced))).await;
            assert!(
                run.plan.contains("MetadataOnlyExec") && !run.plan.contains("LogsScanExec"),
                "{sql} (token {tokens}): answered from A's stamp; plan was:\n{}",
                run.plan
            );
            assert_eq!(run.data_gets, 0, "{sql} (token {tokens})");
            assert!(scan.data_gets > 0, "{sql} (token {tokens})");
            assert_same_answer(&run.answer, &scan.answer, sql);
        }
        let run = erasure_exec(
            &stamped,
            &request("SELECT SUM(a) FROM logs", pick(&stamped)),
        )
        .await;
        assert!(
            pretty(&run.answer).contains("| 45 "),
            "every row of A (token {tokens}): {}",
            pretty(&run.answer)
        );
    }
}
