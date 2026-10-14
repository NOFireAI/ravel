//! The shared fixture of the ES|QL tests: six log records over three minutes
//! for one tenant, published to a `MemoryStore`, and a `SqlExecutor` with two
//! declared columns.

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
use ravel_sql::esql::{self, EsqlError, EsqlOutcome, EsqlRequest, PrepareContext, PreparedEsql};
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

async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId, records: &[LogRecord]) {
    let mut w = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: [3u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
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
        writer_seq: 1,
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
    publish_logs(store.as_ref(), &tenant(), &records()).await;
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
    esql::execute(
        &fixture.executor,
        tenant().hash(),
        prepared,
        &request(),
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
