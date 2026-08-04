//! ADR-0044 acceptance tests for SQL query accounting (issue #424).
//!
//! `sql_execute_records_requests_bytes_and_peak_memory` (crate::executor's
//! own unit test module) proves one query's counters cross-check against an
//! `InstrumentedStore`. What is missing from that unit test is the property
//! operators actually care about across query shapes: that the two-part
//! `CostEstimate` (ADR-0044 "3.", amended) never falls below what the query
//! actually spent, that a `logs` query is accounted at all (previously it
//! contributed nothing, since `LogSegmentFetcher::fetch` was unaccounted),
//! and that wiring accounting through every fetch call changed no result.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{FixedSizeBinaryArray, Float64Array, TimestampNanosecondArray};
use datafusion::arrow::record_batch::RecordBatch;
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_sql::{SqlConfig, SqlOutcome};
use ravel_types::accounting::AccountedOp;
use ravel_types::{Signal, TenantId, logstream};
use util::{Fixture, SegSpec, SeriesSpec, reference_rows, request, tenant_id};
use uuid::Uuid;

/// The estimate is an upper envelope (ADR-0044 "3."): every dimension's
/// actual-over-estimated ratio must be at or below 1.0, whatever the query
/// shape. A ratio above 1.0 means the estimate under-shot, the failure mode
/// that matters.
fn assert_estimate_covers_actual(outcome: &SqlOutcome) {
    let divergence = outcome.estimate.divergence(&outcome.accounting);
    assert!(
        divergence.requests <= 1.0,
        "estimate must not undercount requests: {divergence:?}"
    );
    assert!(
        divergence.store_bytes <= 1.0,
        "estimate must not undercount store bytes: {divergence:?}"
    );
    assert!(
        divergence.decompressed_bytes <= 1.0,
        "estimate must not undercount decompressed bytes: {divergence:?}"
    );
}

#[tokio::test]
async fn estimate_never_below_actual_for_a_point_lookup() {
    let tenant = tenant_id("estimate-point");
    let specs = vec![
        SegSpec::new(
            1,
            1,
            1,
            vec![SeriesSpec::new("aaa", vec![(60_000_000_000, 1.0)])],
        ),
        SegSpec::new(
            2,
            1,
            2,
            vec![SeriesSpec::new("bbb", vec![(60_000_000_000, 2.0)])],
        ),
    ];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let outcome = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT ts, value FROM samples WHERE label(labels, '__name__') = 'aaa'"),
        )
        .await
        .expect("point lookup query");

    assert_estimate_covers_actual(&outcome);
}

#[tokio::test]
async fn estimate_never_below_actual_for_a_wide_scan() {
    let tenant = tenant_id("estimate-wide");
    const SAMPLES: i64 = 20_000;
    let specs = vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new(
            "m",
            (0..SAMPLES).map(|i| (i, i as f64)).collect(),
        )],
    )];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect("wide scan query");

    assert_estimate_covers_actual(&outcome);
}

#[tokio::test]
async fn estimate_never_below_actual_for_a_multi_segment_query() {
    let tenant = tenant_id("estimate-multi");
    let specs = vec![
        SegSpec::new(
            1,
            1,
            1,
            vec![SeriesSpec::new("a", vec![(1, 1.0), (2, 2.0)])],
        ),
        SegSpec::new(
            2,
            1,
            2,
            vec![SeriesSpec::new("b", vec![(1, 3.0), (2, 4.0)])],
        ),
        SegSpec::new(
            3,
            1,
            3,
            vec![SeriesSpec::new("c", vec![(1, 5.0), (2, 6.0)])],
        ),
    ];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect("multi-segment query");

    assert_eq!(outcome.stats.segments, 3, "all three segments must resolve");
    assert_estimate_covers_actual(&outcome);
}

const LOG_RECORD_COUNT: usize = 5;

/// Publishes one RLOG object plus its `Signal::Logs` commit record, so a
/// `logs` query has real rows to return (mirrors tests/conformance.rs's
/// `publish_logs`).
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("query-accounting".to_string()),
    )];
    let stream_id = logstream::log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);

    let writer_id = Uuid::from_u128(9_000);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    for i in 0..LOG_RECORD_COUNT {
        let ts_ns = 1_000 + i as i64;
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns,
                observed_ts_ns: ts_ns,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: format!("accounting record {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");

    let content_hash = [9u8; 32];
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: LOG_RECORD_COUNT as u64,
        series_count: 1,
        min_event_ts_ns: 1_000,
        max_event_ts_ns: 1_000 + LOG_RECORD_COUNT as i64 - 1,
        min_ingest_ts_ns: 1_000,
        max_ingest_ts_ns: 1_000 + LOG_RECORD_COUNT as i64 - 1,
        segment_format_version: 1,
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

#[tokio::test]
async fn a_logs_query_is_accounted() {
    let tenant = tenant_id("logs-accounting");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_logs(store.as_ref(), &tenant).await;
    let fixture = Fixture::build(Arc::clone(&store), &[], SqlConfig::default(), 1 << 30).await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, body FROM logs"))
        .await
        .expect("logs query");

    assert_eq!(outcome.output.num_rows(), LOG_RECORD_COUNT);
    assert!(
        outcome.accounting.total_s3_requests() > 0,
        "the logs fetch funnel must now be accounted (issue #424 deliverable 5); \
         before this, LogSegmentFetcher::fetch was unaccounted and every logs \
         query contributed nothing to per-query counters"
    );
    assert!(
        outcome.accounting.s3_bytes(AccountedOp::Get) > 0,
        "the accounted logs GET must record bytes transferred"
    );
    // The LogsScanExec block counters (#544) flow through to the outcome so the
    // /metrics prune-selectivity family can reuse them (issue #511). This query
    // has no pruning predicate, so every block the scan considered was scanned.
    assert!(
        outcome.stats.blocks_total > 0,
        "the logs scan's block counters must reach SqlStats"
    );
    assert_eq!(
        outcome.stats.blocks_scanned, outcome.stats.blocks_total,
        "with no prune predicate every block survives"
    );
    assert_eq!(
        outcome.stats.blocks_pruned_by_postings, 0,
        "nothing is pruned by postings without an indexed equality predicate"
    );
}

/// Corpus for `a_logs_query_with_an_indexed_predicate_prunes_blocks_by_postings`:
/// 48 records on one stream, in six groups of eight, each group carrying a
/// distinct value of the per-record attribute `region` (`region-0` ..
/// `region-5`). It must be a per-record attribute, not a resource one: a
/// resource-only key gets no dynamic column and so no postings at all
/// (ADR-0049, the same gap Fix 1 of this ticket documents).
const POSTINGS_PRUNE_RECORD_COUNT: usize = 48;
const POSTINGS_PRUNE_GROUP_SIZE: usize = 8;

/// `block_target_records: 4`, so the object holds exactly
/// `POSTINGS_PRUNE_RECORD_COUNT / 4 = 12` blocks, and because the group size
/// (8) is a multiple of the block size (4), each region value's eight
/// records span exactly two whole blocks with no block straddling a group
/// boundary: region `k` owns blocks `2k` and `2k+1`, nothing else.
fn postings_prune_cfg() -> RlogConfig {
    RlogConfig {
        block_target_records: 4,
        ..RlogConfig::default()
    }
}

/// Publishes one RLOG object with the per-record attribute `region` indexed
/// (see [`POSTINGS_PRUNE_RECORD_COUNT`]'s doc comment for the block layout),
/// plus its `Signal::Logs` commit record.
async fn publish_logs_with_region_postings(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("query-accounting".to_string()),
    )];
    let stream_id = logstream::log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);

    let writer_id = Uuid::from_u128(9_001);
    let mut writer = RlogWriter::new(
        postings_prune_cfg(),
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    )
    .with_indexed_fields(vec!["region".to_string()]);

    for i in 0..POSTINGS_PRUNE_RECORD_COUNT {
        let ts_ns = 1_000 + i as i64;
        let region = i / POSTINGS_PRUNE_GROUP_SIZE;
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns,
                observed_ts_ns: ts_ns,
                severity_num: 9,
                severity_text: "INFO".to_string(),
                body: format!("postings prune record {i}"),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![(
                    "region".to_string(),
                    AttrValue::Str(format!("region-{region}")),
                )],
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");

    let content_hash = [10u8; 32];
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: POSTINGS_PRUNE_RECORD_COUNT as u64,
        series_count: 1,
        min_event_ts_ns: 1_000,
        max_event_ts_ns: 1_000 + POSTINGS_PRUNE_RECORD_COUNT as i64 - 1,
        min_ingest_ts_ns: 1_000,
        max_ingest_ts_ns: 1_000 + POSTINGS_PRUNE_RECORD_COUNT as i64 - 1,
        segment_format_version: 1,
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

/// The anti-vacuity guard for issue #511 deliverable 2: `a_logs_query_is_accounted`
/// above proves `blocks_pruned_by_postings` is not spuriously nonzero (no
/// predicate, so nothing is pruned); it does not prove the counter can ever
/// be nonzero at all. A metric that is permanently zero passes every test
/// that only checks it equals zero, so this test writes a query with an
/// indexed equality predicate and requires the prune to be both real
/// (greater than zero) and partial (less than the total block count, since
/// the matching value's own blocks must still be scanned).
#[tokio::test]
async fn a_logs_query_with_an_indexed_predicate_prunes_blocks_by_postings() {
    let tenant = tenant_id("logs-postings-prune");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_logs_with_region_postings(store.as_ref(), &tenant).await;
    let fixture = Fixture::build(Arc::clone(&store), &[], SqlConfig::default(), 1 << 30).await;

    let outcome = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT ts, body FROM logs WHERE attrs['region'] = 'region-0'"),
        )
        .await
        .expect("logs query with an indexed equality predicate");

    assert_eq!(
        outcome.output.num_rows(),
        POSTINGS_PRUNE_GROUP_SIZE,
        "exactly one region's eight records must match"
    );
    assert_eq!(
        outcome.stats.blocks_total, 12,
        "48 records at 4 per block must land in 12 blocks"
    );
    assert!(
        outcome.stats.blocks_pruned_by_postings > 0,
        "an indexed equality predicate must prune at least one block: {:?}",
        outcome.stats
    );
    assert!(
        outcome.stats.blocks_pruned_by_postings < outcome.stats.blocks_total,
        "the matching region's own blocks must still be scanned, so pruning \
         cannot be total: {:?}",
        outcome.stats
    );
}

fn reduce_rows(batches: &[RecordBatch]) -> HashMap<[u8; 16], HashMap<i64, u64>> {
    let mut out: HashMap<[u8; 16], HashMap<i64, u64>> = HashMap::new();
    for batch in batches {
        let sid_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("series_id col");
        let ts_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts col");
        let value_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value col");
        for i in 0..batch.num_rows() {
            let sid = <[u8; 16]>::try_from(sid_col.value(i)).expect("16 bytes");
            out.entry(sid)
                .or_default()
                .insert(ts_col.value(i), value_col.value(i).to_bits());
        }
    }
    out
}

/// The property that matters most for this ticket: threading accounting
/// through every fetch call is counting only, never a behavior change. The
/// oracle side (`reference_rows`) fetches through the same plain,
/// unaccounted `SegmentFetcher::fetch` the pre-#424 code path used; the
/// executor side now runs fully accounted end to end. Row-for-row equality
/// between the two proves the accounting wiring changed nothing.
#[tokio::test]
async fn accounted_execution_returns_identical_rows_to_the_unaccounted_oracle() {
    let tenant = tenant_id("identical-rows");
    let specs = vec![
        SegSpec::new(
            1,
            1,
            1,
            vec![SeriesSpec::new("a", vec![(10, 1.0), (20, 2.0)])],
        ),
        SegSpec::new(
            2,
            1,
            2,
            vec![SeriesSpec::new("a", vec![(10, 9.0), (30, 3.0)])],
        ),
        SegSpec::new(3, 2, 1, vec![SeriesSpec::new("b", vec![(15, 4.0)])]),
    ];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;
    let snapshot = fixture.snapshot(&tenant).await;

    let want = reference_rows(&fixture.fetcher, tenant.hash(), &snapshot).await;
    let mut want_map: HashMap<[u8; 16], HashMap<i64, u64>> = HashMap::new();
    for row in &want {
        want_map
            .entry(row.series_id)
            .or_default()
            .insert(row.ts, row.value.to_bits());
    }

    let outcome = fixture
        .executor
        .execute(
            tenant.hash(),
            &request("SELECT series_id, ts, value FROM samples"),
        )
        .await
        .expect("accounted query");
    let got = reduce_rows(outcome.output.batches());

    assert_eq!(got.len(), want_map.len(), "series count must match");
    for (sid, want_ts) in &want_map {
        let got_ts = got
            .get(sid)
            .unwrap_or_else(|| panic!("accounted output missing series {sid:?}"));
        assert_eq!(
            got_ts, want_ts,
            "series {sid:?} rows must match the unaccounted oracle byte-for-byte"
        );
    }
}
