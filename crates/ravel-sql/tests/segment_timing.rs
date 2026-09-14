//! Issue #913: `SqlConfig::segment_timing` gates `LogsScanExec`'s per-segment
//! scan timeline (`seg_open_start_offset`/`seg_open_ready_offset`/
//! `seg_done_offset`, folded into `SqlStats.scan_timing.segments`).
//!
//! Off (the shipped default), a production logs query registers none of
//! those metrics: `LogScanStream::mark_segment` returns before it allocates
//! a label or touches the metric set, so `accumulate_scan_timing` finds no
//! `(partition, segment)` rows to fold, while the O(1) sums and counts next
//! to it (`open_elapsed_ns`, `decode_build_elapsed_ns`, `segments_opened`)
//! are read off the same `BlockMetrics` set built unconditionally in
//! `execute` and stay populated either way.
//!
//! On, the timeline is exactly `partitions x segments` rows for a query
//! whose predicate-free full window takes the whole-segment fast path: with
//! `fetch_concurrency` pinned to 1 (one scan partition) over three published
//! segments, every segment is opened by that one partition, so
//! `scan_timing.segments` has exactly 3 rows.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::sync::Arc;

use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::EngineConfig;
use ravel_sql::SqlConfig;
use ravel_types::{Signal, TenantId, logstream};
use util::{Fixture, request, tenant_id};
use uuid::Uuid;

const LOG_SEGMENT_COUNT: usize = 3;
const LOG_RECORDS_PER_SEGMENT: usize = 4;

/// Publishes one RLOG object plus its `Signal::Logs` commit record, keyed by
/// `seq` so `LOG_SEGMENT_COUNT` calls with distinct `seq` values publish
/// distinct segments (mirrors `query_accounting.rs`'s single-segment
/// `publish_logs`, generalized to a chosen count).
async fn publish_logs_segment(store: &dyn ObjectStoreBackend, tenant: &TenantId, seq: u64) {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("segment-timing".to_string()),
    )];
    let stream_id = logstream::log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);

    let writer_id = Uuid::from_u128(9_100);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq: seq,
        },
    );
    let base_ts = 1_000 + (seq as i64) * 10_000;
    for i in 0..LOG_RECORDS_PER_SEGMENT {
        let ts_ns = base_ts + i as i64;
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns,
                observed_ts_ns: ts_ns,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: format!("segment {seq} record {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");

    let content_hash = [seq as u8; 32];
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: LOG_RECORDS_PER_SEGMENT as u64,
        series_count: 1,
        min_event_ts_ns: base_ts,
        max_event_ts_ns: base_ts + LOG_RECORDS_PER_SEGMENT as i64 - 1,
        min_ingest_ts_ns: base_ts,
        max_ingest_ts_ns: base_ts + LOG_RECORDS_PER_SEGMENT as i64 - 1,
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

/// One scan partition (`fetch_concurrency: 1`), so every published segment is
/// opened by the same partition and the pinned row count below does not
/// depend on how DataFusion happens to stripe partitions.
fn single_partition_config(segment_timing: bool) -> SqlConfig {
    SqlConfig {
        engine: EngineConfig {
            fetch_concurrency: 1,
            ..EngineConfig::default()
        },
        segment_timing,
        ..SqlConfig::default()
    }
}

#[tokio::test]
async fn segment_timing_off_by_default_registers_no_per_segment_metrics() {
    let tenant = tenant_id("segment-timing-off");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    for seq in 0..LOG_SEGMENT_COUNT as u64 {
        publish_logs_segment(store.as_ref(), &tenant, seq).await;
    }
    let fixture = Fixture::build(
        Arc::clone(&store),
        &[],
        single_partition_config(false),
        1 << 30,
    )
    .await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, body FROM logs"))
        .await
        .expect("logs query");

    assert_eq!(
        outcome.stats.segments, LOG_SEGMENT_COUNT,
        "all published segments must resolve"
    );
    assert_eq!(
        outcome.output.num_rows(),
        LOG_SEGMENT_COUNT * LOG_RECORDS_PER_SEGMENT,
        "every published record must come back"
    );
    // This is the flag's whole contract: `mark_segment` never runs the label
    // allocation or the metric registration, so `accumulate_scan_timing` --
    // the only place that reads these three metric names out of the plan's
    // `ExecutionPlanMetricsSet` -- folds zero rows. An empty Vec here is
    // exactly what "no seg_open_start_offset/seg_open_ready_offset/
    // seg_done_offset metric was ever registered" looks like from outside
    // `logs_scan.rs`.
    assert_eq!(
        outcome.stats.scan_timing.segments,
        Vec::new(),
        "the per-segment timeline must not be registered when segment_timing is off"
    );
    // The O(1) sums and counts sit right next to the gated Vec on the same
    // struct and must NOT be gated: a test that only checked the Vec was
    // empty would still pass if the whole timing system had been broken.
    assert!(
        outcome.stats.scan_timing.open_elapsed_ns > 0,
        "open_elapsed_ns must stay populated with segment_timing off"
    );
    assert!(
        outcome.stats.scan_timing.decode_build_elapsed_ns > 0,
        "decode_build_elapsed_ns must stay populated with segment_timing off"
    );
    assert_eq!(
        outcome.stats.scan_timing.segments_opened, LOG_SEGMENT_COUNT as u64,
        "segments_opened must stay populated with segment_timing off"
    );
}

#[tokio::test]
async fn segment_timing_on_publishes_one_row_per_partition_segment_pair() {
    let tenant = tenant_id("segment-timing-on");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    for seq in 0..LOG_SEGMENT_COUNT as u64 {
        publish_logs_segment(store.as_ref(), &tenant, seq).await;
    }
    let fixture = Fixture::build(
        Arc::clone(&store),
        &[],
        single_partition_config(true),
        1 << 30,
    )
    .await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, body FROM logs"))
        .await
        .expect("logs query");

    assert_eq!(outcome.stats.segments, LOG_SEGMENT_COUNT);
    // One scan partition over three segments: every segment is opened by
    // partition 0, so the timeline has exactly one row per segment -- 3,
    // pinned, not "non-empty".
    assert_eq!(
        outcome.stats.scan_timing.segments.len(),
        LOG_SEGMENT_COUNT,
        "segment_timing on must publish exactly one row per (partition, segment) pair"
    );
    for row in &outcome.stats.scan_timing.segments {
        assert_eq!(row.partition, 0, "the single scan partition is partition 0");
        assert!(
            row.open_start_ns > 0 && row.open_ready_ns >= row.open_start_ns,
            "each row's open offsets must be real timeline points: {row:?}"
        );
        assert!(
            row.done_ns >= row.open_ready_ns,
            "each row's done offset must follow its open-ready offset: {row:?}"
        );
    }
}
