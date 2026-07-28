//! Selective-query timing for BENCHMARKS.md's P5b row (postings pruning,
//! docs/metric-index-plan.md 5.4): equality `__name__` query (pruned) vs.
//! regex `__name__` query (bypassed, fetches every segment) over the same
//! fold, at two segment counts. `#[ignore]`d: a wall-clock measurement, not
//! a correctness assertion, so it stays out of the crate's normal test gate.
//!
//! Reproduce: `cargo test -p ravel-query --release --test pruning_bench --
//! --ignored --nocapture`
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn series_input(tenant_id: &TenantId, metric: &str, ts_ns: i64) -> SeriesInput {
    let label_set = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, metric, &label_set).expect("series id");
    SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample { ts_ns, value: 1.0 }],
    }
}

async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant_hash: TenantHash,
    writer_seq: u64,
    hour: u32,
    inputs: Vec<SeriesInput>,
) {
    let writer_id = Uuid::new_v4();
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    let written: WrittenSegment =
        SegmentWriter::write(inputs, identity, bounds).expect("write segment");
    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 0,
        ingest_hour_bucket: hour,
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

#[tokio::test]
#[ignore]
async fn measure_selective_query_before_after() {
    for &n in &[100usize, 1_000usize] {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tid = TenantId::new("bench".to_string());
        let th = tid.hash();
        let hour = 9_000u32;
        let now = now_at_seal(hour);
        let ts = i64::from(hour) * NS_PER_HOUR + 30 * 60 * NS_PER_SEC;
        let t_ms = ts / 1_000_000;

        for i in 0..n {
            let metric = if i == 0 {
                "target_metric".to_string()
            } else {
                format!("metric_{i}")
            };
            publish_segment(
                store.as_ref(),
                th,
                1,
                hour,
                vec![series_input(&tid, &metric, ts)],
            )
            .await;
        }

        let catalog = Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog");
        catalog
            .fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[])
            .await
            .expect("fold");
        let engine = QueryEngine::new(Arc::new(catalog), store, EngineConfig::default());

        let iters = 20;
        let start = Instant::now();
        for _ in 0..iters {
            engine
                .instant_with_stats(th, "target_metric", t_ms, &[], now, Duration::from_secs(5))
                .await
                .expect("pruned query");
        }
        let pruned_avg = start.elapsed() / iters;

        let start = Instant::now();
        for _ in 0..iters {
            engine
                .instant_with_stats(
                    th,
                    "{__name__=~\".+\"}",
                    t_ms,
                    &[],
                    now,
                    Duration::from_secs(5),
                )
                .await
                .expect("bypassed query");
        }
        let bypassed_avg = start.elapsed() / iters;

        eprintln!("n={n} pruned_avg={pruned_avg:?} bypassed_avg={bypassed_avg:?}");
    }
}
