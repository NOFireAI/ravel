//! ADR-2773 decision 4: `POST /api/v1/sql`'s stats report which logs build
//! path a statement took. One object of two blocks; a statement projecting
//! only fixed and declared columns builds both blocks columnar, and one
//! projecting the `attrs` map builds both on the row path.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest,
    StaticDeclaredColumns,
};
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

const ROWS_PER_BLOCK: usize = 4;
const BLOCKS: usize = 2;

fn record(ts: i64) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("row at {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("n".to_string(), AttrValue::I64(ts))],
    }
}

async fn publish_object(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let records: Vec<LogRecord> = (0..ROWS_PER_BLOCK * BLOCKS)
        .map(|i| record(i as i64 + 1))
        .collect();
    let cfg = RlogConfig {
        block_target_records: ROWS_PER_BLOCK,
        ..RlogConfig::default()
    };
    let mut w = RlogWriter::new(
        cfg,
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: [5u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    for r in &records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let rec = record::build(NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(2_789),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: records.len() as u64,
        series_count: 1,
        min_event_ts_ns: 1,
        max_event_ts_ns: records.len() as i64,
        min_ingest_ts_ns: 1,
        max_ingest_ts_ns: records.len() as i64,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
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

fn executor(store: &Arc<dyn ObjectStoreBackend>) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(store)),
        LogSegmentFetcher::new(Arc::clone(store)),
        SpanSegmentFetcher::new(Arc::clone(store)),
        SqlConfig::default(),
        1 << 30,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(vec![
        DeclaredColumn::new("n", DeclaredType::I64),
    ])))
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: 1_000_000,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// Exact `rowpathBatches` and `columnarBatches` in the stats JSON: one batch
/// per block, all on the path the projection selects, none on the other.
#[tokio::test]
async fn sql_stats_report_rowpath_and_columnar_batches() {
    let t = TenantId::new("rowpath-stats-2789".to_string());
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_object(store.as_ref(), &t).await;
    let exec = executor(&store);

    for (sql, rowpath, columnar) in [
        ("SELECT ts, n FROM logs", 0, BLOCKS),
        ("SELECT ts, attrs FROM logs", BLOCKS, 0),
    ] {
        let outcome = exec
            .execute(t.hash(), &request(sql))
            .await
            .expect("statement executes");
        assert_eq!(outcome.output.num_rows(), ROWS_PER_BLOCK * BLOCKS, "{sql}");
        let json = ravel_sql::stats_json::pruning_json(&outcome.stats);
        assert_eq!(json["rowpathBatches"], rowpath, "{sql}: {json}");
        assert_eq!(json["columnarBatches"], columnar, "{sql}: {json}");
    }
}
