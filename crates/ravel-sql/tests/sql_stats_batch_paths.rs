//! Issue #2789: `SqlStats` carries the logs scans' `columnar_batches` and
//! `rowpath_batches` counters, and the JSON stats object of `POST
//! /api/v1/sql` (`ravel_sql::stats_json::pruning_json`) renders them as
//! `columnarBatches` and `rowpathBatches`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity as LogObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::stats_json::pruning_json;
use ravel_sql::{SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

/// Records per block, and blocks in the one object.
const BLOCK_ROWS: usize = 4;
const BLOCKS: usize = 3;

/// Publishes one RLOG object of `BLOCKS` blocks of `BLOCK_ROWS` records each,
/// every record carrying one attribute that fits a FIELD_DIR column, so no
/// block has an `attrs_raw` page and a statement that does not project
/// `attrs` takes the columnar path on every block.
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant_id: &TenantId) {
    let resource = vec![("service.name".to_string(), AttrValue::Str("api".into()))];
    let tenant_hash = tenant_id.hash();
    let identity = LogObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    };
    let cfg = RlogConfig {
        block_target_records: BLOCK_ROWS,
        ..RlogConfig::default()
    };
    let mut writer = RlogWriter::new(cfg, identity);
    let rows = (BLOCK_ROWS * BLOCKS) as i64;
    for ts in 1..=rows {
        writer
            .push(LogRecord {
                stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
                stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
                ts_ns: ts,
                observed_ts_ns: ts,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("row {ts}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![("k".to_string(), AttrValue::Str(format!("v{ts}")))],
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id: Uuid::from_u128(2_789),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [7u8; 32],
        sample_count: rows as u64,
        series_count: 1,
        min_event_ts_ns: 1,
        max_event_ts_ns: rows,
        min_ingest_ts_ns: 1,
        max_ingest_ts_ns: rows,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

async fn executor() -> (SqlExecutor, ravel_types::TenantHash) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = TenantId::new("acme-batch-paths".to_string());
    publish_logs(store.as_ref(), &tid).await;
    let cat = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        cat,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        SqlConfig::default(),
        1 << 30,
    );
    (executor, tid.hash())
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: 1_000,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// A statement that does not project `attrs` builds one batch per block on
/// the columnar path; one that projects `attrs` builds one per block on the
/// row path. Each count is exact in `SqlStats` and in the JSON stats object.
#[tokio::test]
async fn sql_stats_report_rowpath_and_columnar_batches() {
    let (executor, tenant) = executor().await;
    for (sql, columnar, rowpath) in [
        ("SELECT ts, body FROM logs", BLOCKS as u64, 0),
        ("SELECT ts, attrs FROM logs", 0, BLOCKS as u64),
    ] {
        let outcome = executor.execute(tenant, &request(sql)).await.expect(sql);
        assert_eq!(outcome.output.num_rows(), BLOCK_ROWS * BLOCKS, "{sql}");
        assert_eq!(outcome.stats.columnar_batches, columnar, "{sql}");
        assert_eq!(outcome.stats.rowpath_batches, rowpath, "{sql}");
        let json = pruning_json(&outcome.stats);
        assert_eq!(
            json["columnarBatches"].as_u64(),
            Some(columnar),
            "{sql}: {json}"
        );
        assert_eq!(
            json["rowpathBatches"].as_u64(),
            Some(rowpath),
            "{sql}: {json}"
        );
    }
}
