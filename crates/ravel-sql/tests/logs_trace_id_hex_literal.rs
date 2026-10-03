//! Integration test for `TraceIdHexLiteralPlanner` (issue #1709) against the
//! `logs` table. `logs.trace_id` is `FixedSizeBinary(16)`, the same width as
//! `spans.trace_id`, so the planner registered in
//! `crate::session::build_session` applies unchanged; see
//! `crate::trace_id_planner`'s module docs for why the rewrite exists.
//! `crates/ravel-sql/tests/spans_provider.rs` carries the equivalent
//! `spans`-side coverage.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::FixedSizeBinaryArray;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{SpanSegmentFetcher, SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

fn tenant() -> TenantId {
    TenantId::new("logs-trace-id-hex-literal".to_string())
}

fn identity(tenant_hash: [u8; 16]) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash,
        shard: 0,
        writer_id: [2u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

fn record(ts: i64, body: &str, trace_id: Option<[u8; 16]>) -> LogRecord {
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
        body: body.into(),
        trace_id,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// Write one RLOG object from `records` and publish its `Signal::Logs` commit
/// record, so a real `Catalog::resolve` finds the segment. Mirrors
/// `logs_declared_columns.rs::publish_logs`.
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId, records: &[LogRecord]) {
    let writer_id = Uuid::from_u128(17_090);
    let mut w = RlogWriter::new(RlogConfig::default(), identity(tenant.hash().0));
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let min = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
    let max = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id,
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

fn executor_with(store: Arc<dyn ObjectStoreBackend>) -> SqlExecutor {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        SpanSegmentFetcher::new(Arc::clone(&store)),
        SqlConfig::default(),
        1 << 30,
    )
}

fn request(sql: &str) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window: TimeRange {
            start_ns: 0,
            end_ns: i64::MAX,
        },
        min_tokens: Vec::new(),
        now_ns: 1_000_000,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

fn to_hex(bytes: [u8; 16]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Issue #1709: `trace_id = '<32-hex>'` against `logs.trace_id`
/// (`FixedSizeBinary(16)`, same width as `spans.trace_id`) plans and returns
/// the same rows as the already-working `X'<32-hex>'` binary-literal form.
/// Three records: one on the target trace, one on another trace, one with no
/// trace id at all (NULL, since `logs.trace_id` is nullable) -- the string
/// form must match only the target trace's row, same as the byte form.
#[tokio::test]
async fn logs_trace_id_hex_string_literal_matches_the_byte_literal_form() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let t = tenant();
    let target = [0x11u8; 16];
    let other = [0x22u8; 16];
    let records = vec![
        record(1, "match", Some(target)),
        record(2, "other", Some(other)),
        record(3, "none", None),
    ];
    publish_logs(store.as_ref(), &t, &records).await;

    let hex = to_hex(target);
    let executor = executor_with(Arc::clone(&store));

    let string_sql = format!("SELECT ts, trace_id FROM logs WHERE trace_id = '{hex}'");
    let binary_sql = format!("SELECT ts, trace_id FROM logs WHERE trace_id = X'{hex}'");

    for sql in [&string_sql, &binary_sql] {
        let outcome = executor
            .execute(t.hash(), &request(sql))
            .await
            .unwrap_or_else(|e| panic!("{sql} must plan and execute: {e}"));
        let batches = outcome.output.batches();
        let mut rows = 0usize;
        for batch in batches {
            let trace = batch
                .column(1)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("trace_id col");
            for i in 0..batch.num_rows() {
                assert_eq!(
                    trace.value(i),
                    target,
                    "{sql} must return only the matching trace's row"
                );
                rows += 1;
            }
        }
        assert_eq!(rows, 1, "{sql} must return exactly the one matching row");
    }
}
