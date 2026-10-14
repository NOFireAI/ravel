//! The shared fixtures of the ES|QL tests, each published to a `MemoryStore`
//! for one tenant and read by a `SqlExecutor` with two declared columns:
//! [`fixture`], six log records over three minutes in one segment, and
//! [`segmented_fixture`], records on both sides of the query clock laid out
//! as two segments or as one.

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::util::display::array_value_to_string;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use ravel_catalog::{Catalog, CatalogConfig, Snapshot};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, PhaseAccounting, SegmentFetcher};
use ravel_sql::esql::{
    self, EsqlError, EsqlOutcome, EsqlRequest, EsqlRun, PrepareContext, PreparedEsql,
};
use ravel_sql::{
    CeilingBreach, DeclaredColumn, DeclaredType, LiveAccounting, LogsTableProvider, SessionTable,
    SpanSegmentFetcher, SpillDecision, SqlConfig, SqlExecutor, StaticDeclaredColumns,
    TenantDelegatingPool, TenantMemoryAccountant, build_session,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

/// 2023-11-14T22:13:00Z, on a minute boundary.
pub const BASE: i64 = 1_699_999_980_000_000_000;
pub const SECOND: i64 = 1_000_000_000;
/// The query clock: ten minutes after [`BASE`].
pub const NOW: i64 = BASE + 600 * SECOND;

pub fn tenant() -> TenantId {
    TenantId::new("esql-lowering-2891".to_string())
}

pub fn declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("dur", DeclaredType::I64),
        DeclaredColumn::new("region", DeclaredType::Str),
    ]
}

struct Row {
    offset_s: i64,
    severity: u8,
    body: &'static str,
    service: Option<&'static str>,
    dur: Option<i64>,
}

const ROWS: [Row; 6] = [
    Row {
        offset_s: 0,
        severity: 9,
        body: "a",
        service: Some("api"),
        dur: Some(10),
    },
    Row {
        offset_s: 10,
        severity: 13,
        body: "b",
        service: Some("api"),
        dur: Some(20),
    },
    Row {
        offset_s: 70,
        severity: 17,
        body: "c",
        service: Some("web"),
        dur: None,
    },
    Row {
        offset_s: 80,
        severity: 13,
        body: "d",
        service: Some("api"),
        dur: Some(5),
    },
    Row {
        offset_s: 130,
        severity: 5,
        body: "e",
        service: Some("web"),
        dur: Some(7),
    },
    Row {
        offset_s: 140,
        severity: 21,
        body: "f",
        service: None,
        dur: Some(1),
    },
];

fn records() -> Vec<LogRecord> {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("fixture".to_string()),
    )];
    ROWS.iter()
        .enumerate()
        .map(|(i, row)| {
            let mut attrs = Vec::new();
            if let Some(service) = row.service {
                attrs.push(("service".to_string(), AttrValue::Str(service.to_string())));
            }
            if let Some(dur) = row.dur {
                attrs.push(("dur".to_string(), AttrValue::I64(dur)));
            }
            if i == 0 {
                attrs.push(("region".to_string(), AttrValue::Str("eu".to_string())));
            }
            let ts = BASE + row.offset_s * SECOND;
            LogRecord {
                stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: row.severity,
                severity_text: "INFO".into(),
                body: row.body.into(),
                trace_id: (i == 0).then_some([0xAB; 16]),
                span_id: None,
                flags: 0,
                attrs,
            }
        })
        .collect()
}

/// Publish `records` as one RLOG object under one commit record, the
/// `writer_seq`-th of the fixture's writer.
async fn publish_logs(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    records: &[LogRecord],
    writer_seq: u64,
) {
    let mut w = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: [3u8; 16],
            writer_epoch: 1,
            writer_seq,
        },
    );
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(2_891),
        writer_epoch: 1,
        writer_seq,
        object_size: bytes.len() as u64,
        content_hash: [7u8; 32],
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: min,
        max_event_ts_ns: max,
        min_ingest_ts_ns: min,
        max_ingest_ts_ns: max,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        // The catalog lists by ingest hour, so a window that does not start
        // at the epoch only finds an object stamped with its real hour.
        created_unix_ns: max,
        ingest_hour_bucket: u32::try_from(max / (3_600 * SECOND)).expect("fits u32"),
    })
    .expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

pub struct Fixture {
    pub executor: SqlExecutor,
    pub store: Arc<dyn ObjectStoreBackend>,
}

pub async fn fixture() -> Fixture {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_logs(store.as_ref(), &tenant(), &records(), 1).await;
    executor_over(store)
}

/// One record of the segmented fixture.
pub struct Event {
    pub ts: i64,
    pub severity: u8,
    pub body: &'static str,
}

/// Segment A of the segmented fixture: from 30 s before the query clock to
/// 10 s after it, one record exactly at it.
pub const SEGMENT_A: [Event; 4] = [
    Event {
        ts: NOW - 30 * SECOND,
        severity: 9,
        body: "a1",
    },
    Event {
        ts: NOW - 10 * SECOND,
        severity: 3,
        body: "a2",
    },
    Event {
        ts: NOW,
        severity: 11,
        body: "a3",
    },
    Event {
        ts: NOW + 10 * SECOND,
        severity: 7,
        body: "a4",
    },
];

/// Segment B of the segmented fixture: from 300 s to 310 s after the query
/// clock.
pub const SEGMENT_B: [Event; 3] = [
    Event {
        ts: NOW + 300 * SECOND,
        severity: 5,
        body: "b1",
    },
    Event {
        ts: NOW + 305 * SECOND,
        severity: 13,
        body: "b2",
    },
    Event {
        ts: NOW + 310 * SECOND,
        severity: 1,
        body: "b3",
    },
];

/// How the segmented fixture lays [`SEGMENT_A`] and [`SEGMENT_B`] out.
#[derive(Debug, Clone, Copy)]
pub enum Layout {
    /// Two commit records, one object each.
    Split,
    /// One commit record holding every event, as after compaction.
    Merged,
}

pub const LAYOUTS: [Layout; 2] = [Layout::Split, Layout::Merged];

fn event_records(events: &[&Event]) -> Vec<LogRecord> {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("segmented".to_string()),
    )];
    events
        .iter()
        .map(|event| LogRecord {
            stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
            stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
            ts_ns: event.ts,
            observed_ts_ns: event.ts,
            severity_num: event.severity,
            severity_text: "INFO".into(),
            body: event.body.into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        })
        .collect()
}

/// [`SEGMENT_A`] and [`SEGMENT_B`] published under `layout`, each commit
/// record stamped with the ingest hour of its own events.
pub async fn segmented_fixture(layout: Layout) -> Fixture {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let a: Vec<&Event> = SEGMENT_A.iter().collect();
    let b: Vec<&Event> = SEGMENT_B.iter().collect();
    match layout {
        Layout::Split => {
            publish_logs(store.as_ref(), &tenant(), &event_records(&a), 1).await;
            publish_logs(store.as_ref(), &tenant(), &event_records(&b), 2).await;
        }
        Layout::Merged => {
            let all: Vec<&Event> = a.into_iter().chain(b).collect();
            publish_logs(store.as_ref(), &tenant(), &event_records(&all), 1).await;
        }
    }
    executor_over(store)
}

/// The number of segments the layout publishes.
pub fn segment_count(layout: Layout) -> usize {
    match layout {
        Layout::Split => 2,
        Layout::Merged => 1,
    }
}

/// Run `source` with the request's `start` and `end`.
pub async fn run_with_range(
    fixture: &Fixture,
    source: &str,
    start: Option<i64>,
    end: Option<i64>,
) -> Result<EsqlOutcome, EsqlError> {
    let ctx = PrepareContext {
        start,
        end,
        ..context()
    };
    let prepared = esql::prepare(source, &ravel_esql::Limits::default(), &ctx)?;
    run_prepared(fixture, &prepared).await
}

/// The segments the catalog resolved for `outcome`.
pub fn segments(outcome: &EsqlOutcome) -> usize {
    match &outcome.run {
        EsqlRun::Catalog(outcome) => outcome.stats.segments,
        EsqlRun::Row(_) => panic!("a ROW statement resolves no segments"),
    }
}

fn executor_over(store: Arc<dyn ObjectStoreBackend>) -> Fixture {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared())));
    Fixture { executor, store }
}

pub fn context() -> PrepareContext {
    PrepareContext {
        declared: declared(),
        now_ns: NOW,
        start: None,
        end: None,
    }
}

pub fn prepare(source: &str) -> Result<PreparedEsql, EsqlError> {
    esql::prepare(source, &ravel_esql::Limits::default(), &context())
}

pub fn request() -> EsqlRequest {
    EsqlRequest {
        min_tokens: Vec::new(),
        deadline: Duration::from_secs(30),
        budgets: None,
        max_rows: None,
    }
}

pub async fn run(fixture: &Fixture, source: &str) -> Result<EsqlOutcome, EsqlError> {
    let prepared = prepare(source)?;
    run_prepared(fixture, &prepared).await
}

pub async fn run_prepared(
    fixture: &Fixture,
    prepared: &PreparedEsql,
) -> Result<EsqlOutcome, EsqlError> {
    run_with(fixture, prepared, &request()).await
}

pub async fn run_with(
    fixture: &Fixture,
    prepared: &PreparedEsql,
    request: &EsqlRequest,
) -> Result<EsqlOutcome, EsqlError> {
    esql::execute(
        &fixture.executor,
        tenant().hash(),
        prepared,
        request,
        &LiveAccounting::new(),
    )
    .await
}

/// Every row of `outcome`, each cell rendered as text and a null as `null`.
pub fn rows(outcome: &EsqlOutcome) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in outcome.output().batches() {
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        // A `NullArray` has no validity buffer; its nulls are logical.
                        if column
                            .logical_nulls()
                            .is_some_and(|nulls| nulls.is_null(row))
                        {
                            "null".to_string()
                        } else {
                            array_value_to_string(column, row).expect("cell")
                        }
                    })
                    .collect(),
            );
        }
    }
    rows
}

/// `(name, ES|QL type)` for each result column.
pub fn columns(outcome: &EsqlOutcome) -> Vec<(String, String)> {
    outcome
        .columns
        .iter()
        .map(|column| (column.name.clone(), column.ty.name().to_string()))
        .collect()
}

/// A session over an empty `logs` snapshot with the fixture's declared
/// columns, built as the executor builds one.
pub fn session(store: &Arc<dyn ObjectStoreBackend>) -> SessionContext {
    let provider = LogsTableProvider::new(
        Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        tenant().hash(),
        LogSegmentFetcher::new(Arc::clone(store)),
        PhaseAccounting::new(),
    )
    .with_declared_columns(declared());
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
    )
    .expect("session builds")
}

pub async fn optimized(fixture: &Fixture, source: &str) -> LogicalPlan {
    let prepared = prepare(source).expect("prepares");
    prepared
        .optimized_plan(&session(&fixture.store))
        .await
        .expect("plans")
}
