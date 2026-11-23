//! The metadata-only aggregate rewrite over a statement with several
//! aggregates (issue #4651): ClickBench line 3, `SUM(a), COUNT(*), AVG(w)`,
//! and line 30, ninety `SUM(w + k)`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::physical_plan::{collect, displayable};
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
    assert_eq!(validated.covered().len(), stats.len(), "fixture stamps valid");
    SegmentRef {
        declared_column_stats: ravel_catalog::DeclaredColumnStats::from_validated(&validated),
        ..seg
    }
}

/// Write `rows` as one real RLOG object and return its stamped ref.
async fn write_segment(store: &Arc<CountingStore>, key: &str, seq: u64, rows: &[Row]) -> SegmentRef {
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
        let (pool, _breach) = config.query_pool(
            TenantMemoryAccountant::new(1 << 30),
            QueryAccounting::new(),
        );
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
        let before = self.store.gets();
        let answer = collect(plan, ctx.task_ctx()).await;
        Run {
            plan: shown,
            answer,
            gets: self.store.gets() - before,
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

#[tokio::test]
async fn dump_plans() {
    let corpus = Corpus::two().await;
    for sql in [LINE_3.to_string(), line_30()] {
        for exact in [false, true] {
            let run = corpus
                .run(&sql, Some(Arc::clone(&corpus.stats)), Vec::new(), exact)
                .await;
            println!("exact_typed={exact}\n{}", run.plan);
        }
    }
}
