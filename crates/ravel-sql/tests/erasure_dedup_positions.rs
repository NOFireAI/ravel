//! The SQL samples scan resolves a duplicate the same way with a pending
//! selective-erasure request as without it, at a timestamp the request does
//! not erase (ADR-0064 decision 2, issue #2448).
//!
//! The SQL twin of `crates/ravel-query/tests/erasure_dedup_positions.rs`. The
//! scan-time mask drops a pending request's matching samples from a fetched
//! run and hands the survivors a per-sample priority column carrying each
//! one's original dedup key (ADR-0092 decision 1). The SQL scan must read that
//! column: deriving the in-page index from the masked run's offsets renumbers
//! every survivor after an erased sample.
//!
//! Two L0 segments of one series share the run-wide triple
//! `(created_unix_ns, writer_epoch, writer_seq)` (only the writer id differs,
//! which is not part of the key), so the in-run index decides the contested
//! timestamp:
//!
//! - segment A: `[EARLY, CONTESTED = A_LOW, CONTESTED = A_HIGH]`, indexes
//!   0, 1, 2 (a duplicate inside one write, kept in insertion order);
//! - segment B: `[B_EARLY, CONTESTED = B]`, indexes 0, 1.
//!
//! Without a pending request `A_HIGH` wins on index 2 over `B`'s index 1. The
//! request erases only `EARLY`. If the scan renumbered A's survivors, `A_HIGH`
//! would tie `B` at index 1 and the value tie-break (`B` is the greater bit
//! pattern) would serve `B` instead.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use datafusion::arrow::array::{Array, Float64Array, TimestampNanosecondArray};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{erasure, keys, publish, record, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_promql::Value;
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_query::{EngineConfig, LogSegmentFetcher, QueryEngine, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_sql::{SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash,
    TenantId, TimeRange,
};
use uuid::Uuid;

const NS: i64 = 1_000_000_000;
const CONTESTED: i64 = 1_000 * NS;
/// The only sample the pending request erases.
const EARLY: i64 = CONTESTED - 10 * NS;
/// Segment B's leading sample: outside the erasure window, so B's run is never
/// masked and its contested sample keeps index 1 either way.
const B_EARLY: i64 = CONTESTED - 20 * NS;
/// Read time, after every sample.
const NOW: i64 = CONTESTED + NS;
const METRIC: &str = "m";
const USER: &str = "u1";

/// Values chosen so the to_bits tie-break prefers `B` over `A_HIGH`, and the
/// index alone (not the value) decides when the positions are intact.
const EARLY_VALUE: f64 = 1.0;
const A_LOW: f64 = 1.5;
const A_HIGH: f64 = 2.0;
const B: f64 = 3.0;
const B_EARLY_VALUE: f64 = 4.0;

/// The run-wide triple both segments carry.
const CREATED_UNIX_NS: i64 = 42;
const WRITER_EPOCH: u64 = 1;
const WRITER_SEQ: u64 = 0;

fn label_set() -> LabelSet {
    LabelSet::new(vec![
        Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: METRIC.to_string(),
        },
        Label {
            name: "user_id".to_string(),
            value: USER.to_string(),
        },
    ])
    .expect("valid labels")
}

/// Commits one scalar segment of the series under `writer_id` with the
/// shared run-wide triple and returns its read-your-write token.
async fn publish_scalar(
    store: &MemoryStore,
    tenant_id: &TenantId,
    writer_id: Uuid,
    samples: &[(i64, f64)],
) -> CommitToken {
    let tenant_hash = tenant_id.hash();
    let labels = label_set();
    let written = SegmentWriter::write(
        vec![SeriesInput {
            series_id: SeriesId::compute(tenant_id, METRIC, &labels).expect("series id"),
            labels,
            samples: samples
                .iter()
                .map(|&(ts_ns, value)| Sample { ts_ns, value })
                .collect(),
        }],
        SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: WRITER_EPOCH,
            writer_seq: WRITER_SEQ,
        },
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: WRITER_EPOCH,
        writer_seq: WRITER_SEQ,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: CREATED_UNIX_NS,
        ingest_hour_bucket: 0,
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish")
}

/// A durable windowed request erasing `user_id = u1` at `EARLY` only.
async fn put_early_dreq(store: &MemoryStore, tenant_hash: TenantHash) {
    let request_id = Uuid::from_u128(0x2448);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 1,
        predicate: vec![ErasurePredicateMatcher {
            key: "user_id".to_string(),
            value: USER.to_string(),
        }],
        window_start_ns: EARLY,
        window_end_ns: EARLY + 1,
        reason: String::new(),
    };
    let key =
        keys::erasure_request_key(&tenant_hash, Signal::Metrics, request_id).expect("dreq key");
    store
        .put(
            &key,
            erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put dreq");
}

/// The series' deduplicated `(ts_ns, value_bits)` rows as the SQL samples
/// query serves them.
async fn sql_rows(
    executor: &SqlExecutor,
    tenant_hash: TenantHash,
    tokens: &[CommitToken],
) -> Vec<(i64, u64)> {
    let outcome = executor
        .execute(
            tenant_hash,
            &SqlRequest {
                sql: format!(
                    "SELECT ts, value FROM samples \
                     WHERE label(labels, '__name__') = '{METRIC}' ORDER BY ts"
                ),
                window: TimeRange {
                    start_ns: 0,
                    end_ns: NOW,
                },
                min_tokens: tokens.to_vec(),
                now_ns: NOW,
                deadline: Duration::from_secs(30),
                row_window: false,
                max_rows: None,
                budgets: None,
            },
        )
        .await
        .expect("SQL samples query");
    let mut rows = Vec::new();
    for batch in outcome.output.batches() {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .expect("ts is Timestamp(ns)");
        let value = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("value is Float64");
        for i in 0..batch.num_rows() {
            rows.push((ts.value(i), value.value(i).to_bits()));
        }
    }
    rows
}

/// The value bits an instant PromQL query serves at the contested timestamp.
async fn promql_contested_bits(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    tokens: &[CommitToken],
) -> u64 {
    let (value, _coverage) = engine
        .instant(
            tenant_hash,
            METRIC,
            CONTESTED / 1_000_000,
            tokens,
            NOW,
            Duration::from_secs(30),
        )
        .await
        .expect("instant query");
    let Value::Vector(vector) = value else {
        panic!("instant query over a selector must return a vector");
    };
    assert_eq!(vector.len(), 1, "exactly the one series: {vector:?}");
    vector[0].value.to_bits()
}

fn bits_at(rows: &[(i64, u64)], ts_ns: i64) -> Option<u64> {
    let mut hits = rows.iter().filter(|r| r.0 == ts_ns).map(|r| r.1);
    let first = hits.next();
    assert!(hits.next().is_none(), "one row per timestamp: {rows:?}");
    first
}

#[tokio::test]
async fn pending_erasure_keeps_sql_scalar_dedup_positions() {
    let tenant_id = TenantId::new("tenant-a".to_string());
    let tenant_hash = tenant_id.hash();
    let store = Arc::new(MemoryStore::new());
    let tokens = [
        publish_scalar(
            &store,
            &tenant_id,
            Uuid::from_u128(7),
            &[
                (EARLY, EARLY_VALUE),
                (CONTESTED, A_LOW),
                (CONTESTED, A_HIGH),
            ],
        )
        .await,
        publish_scalar(
            &store,
            &tenant_id,
            Uuid::from_u128(8),
            &[(B_EARLY, B_EARLY_VALUE), (CONTESTED, B)],
        )
        .await,
    ];
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine_config = EngineConfig::default();
    let engine = QueryEngine::new(Arc::clone(&catalog), backend.clone(), engine_config);
    let executor = SqlExecutor::new(
        Arc::clone(&catalog),
        SegmentFetcher::new(backend.clone()),
        LogSegmentFetcher::new(backend.clone()),
        ravel_sql::SpanSegmentFetcher::new(backend.clone()),
        SqlConfig {
            engine: engine_config,
            ..SqlConfig::default()
        },
        1 << 30,
    );

    let before = sql_rows(&executor, tenant_hash, &tokens).await;
    assert_eq!(
        before,
        vec![
            (B_EARLY, B_EARLY_VALUE.to_bits()),
            (EARLY, EARLY_VALUE.to_bits()),
            (CONTESTED, A_HIGH.to_bits()),
        ],
        "baseline: in-run index 2 beats index 1 on the shared triple"
    );

    put_early_dreq(&store, tenant_hash).await;

    let after = sql_rows(&executor, tenant_hash, &tokens).await;
    assert_eq!(
        bits_at(&after, EARLY),
        None,
        "the pending request must exclude the early sample: {after:?}"
    );
    assert_eq!(
        bits_at(&after, CONTESTED),
        bits_at(&before, CONTESTED),
        "a non-erased duplicate must resolve the same with the request pending"
    );
    assert_eq!(
        bits_at(&after, CONTESTED),
        Some(promql_contested_bits(&engine, tenant_hash, &tokens).await),
        "SQL must serve the value PromQL serves under the same pending request"
    );
    assert_eq!(
        after,
        vec![
            (B_EARLY, B_EARLY_VALUE.to_bits()),
            (CONTESTED, A_HIGH.to_bits()),
        ],
        "only the early sample is erased"
    );
}
