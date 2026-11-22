//! Issue #4530 (ADR-4523 decision 6, R5 step 2): PromQL range queries over
//! one `MemoryStore`, sharing one `MemoryBudget`, answer the same run
//! concurrently as run one after another, and the budget returns to zero
//! after each pass.
//!
//! The tenant is five L0 flushes of `SERIES` series: four cover a quarter of
//! the hour each, and the fifth rewrites the second quarter of every even
//! series with other values, so a series' runs merge and its duplicate
//! timestamps are won by the newer write. Each query reaches
//! `prefetch_metric_plans`, which builds the query's `ScalarFold` against the
//! engine's budget, and absorbs each object's runs into it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::keys::commit_key_for_record;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_memory::MemoryBudget;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_promql::RangeValue;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const HOUR: u32 = 500_000;
const TENANT: TenantHash = TenantHash([0x53; 16]);
const SERIES: usize = 400;
const INTERVAL_NS: i64 = 15 * NS_PER_SEC;
const SAMPLES_PER_SERIES: i64 = NS_PER_HOUR / INTERVAL_NS;
const QUARTERS: i64 = 4;
const STEP_MS: i64 = 60_000;
const QUERIES: [&str; 6] = [
    "m",
    "rate(m[5m])",
    "sum by (job) (m)",
    "max_over_time(m[10m])",
    "m{job=\"job-1\"}",
    "count(m > 100)",
];
/// Each query runs this many times in the concurrent pass.
const COPIES: usize = 3;

fn hour_start() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn span_end() -> i64 {
    hour_start() + NS_PER_HOUR
}

fn series_labels(i: usize) -> (SeriesId, LabelSet) {
    let labels = LabelSet::new(vec![
        Label {
            name: "__name__".to_string(),
            value: "m".to_string(),
        },
        Label {
            name: "instance".to_string(),
            value: format!("instance-{i}"),
        },
        Label {
            name: "job".to_string(),
            value: format!("job-{}", i % 5),
        },
    ])
    .expect("labels");
    let id = SeriesId::compute(&TenantId::new("t"), "m", &labels).expect("series id");
    (id, labels)
}

async fn put_l0(store: &MemoryStore, seq: u64, series: Vec<SeriesInput>) {
    let writer_id = Uuid::from_u128(u128::from(seq) + 1);
    let identity = SegmentIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: hour_start(),
        max_ingest_ts_ns: hour_start() + 10,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("write L0");
    let rec = record::build(NewCommitRecord {
        tenant_hash: TENANT,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: hour_start(),
        max_ingest_ts_ns: hour_start() + 10,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: hour_start() + i64::try_from(seq).expect("seq"),
        ingest_hour_bucket: HOUR,
    })
    .expect("commit record");
    store
        .put(
            &rec.object_key,
            written.bytes,
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put data object");
    let commit_key = commit_key_for_record(&rec).expect("commit key");
    store
        .put(
            &commit_key,
            record::encode(&rec),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put commit record");
}

/// One flush: samples `ks` of every series `keep` selects, valued by
/// `value(series, k)`.
fn flush(
    ks: std::ops::Range<i64>,
    keep: impl Fn(usize) -> bool,
    value: impl Fn(usize, i64) -> f64,
) -> Vec<SeriesInput> {
    (0..SERIES)
        .filter(|&i| keep(i))
        .map(|i| {
            let (series_id, labels) = series_labels(i);
            let samples = ks
                .clone()
                .map(|k| Sample {
                    ts_ns: hour_start() + k * INTERVAL_NS,
                    value: value(i, k),
                })
                .collect();
            SeriesInput {
                series_id,
                labels,
                samples,
            }
        })
        .collect()
}

async fn tenant() -> Arc<MemoryStore> {
    let store = Arc::new(MemoryStore::new());
    let quarter = SAMPLES_PER_SERIES / QUARTERS;
    for q in 0..QUARTERS {
        let inputs = flush(
            q * quarter..(q + 1) * quarter,
            |_| true,
            |i, k| (i % 7 + 1) as f64 * k as f64,
        );
        put_l0(&store, u64::try_from(q).expect("quarter") + 1, inputs).await;
    }
    let rewrite = flush(
        quarter..2 * quarter,
        |i| i % 2 == 0,
        |i, k| (i % 3) as f64 * k as f64 + 0.5,
    );
    put_l0(&store, 5, rewrite).await;
    store
}

/// A query's answer as the `Debug` text of its matrix with its series
/// sorted by labels; `Debug` prints each float in its shortest round-trip
/// form, so two equal texts hold the same values.
type Answer = String;

async fn answer(engine: &QueryEngine, query: &str) -> Answer {
    let (value, _annotations, _stats) = engine
        .range_hist_with_budgets(
            TENANT,
            query,
            hour_start() / 1_000_000,
            span_end() / 1_000_000,
            STEP_MS,
            &[],
            span_end() + NS_PER_HOUR,
            Duration::from_secs(600),
            None,
        )
        .await
        .expect("the query answers");
    let RangeValue::Matrix(matrix) = value else {
        panic!("{query} answers a matrix, got {value:?}");
    };
    assert!(!matrix.is_empty(), "{query} answers no series");
    let mut sorted: Vec<_> = matrix.iter().collect();
    sorted.sort_by(|a, b| a.0.iter().cmp(b.0.iter()));
    format!("{sorted:?}")
}

#[test]
fn concurrent_range_queries_sharing_one_budget_answer_as_sequential_ones_and_release_it() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            let store = tenant().await;
            let backend: Arc<dyn ObjectStoreBackend> = store;
            let catalog =
                Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog");
            let budget = Arc::new(MemoryBudget::new(1 << 30));
            let engine = Arc::new(
                QueryEngine::new(Arc::new(catalog), backend, EngineConfig::default())
                    .with_memory_budget(Arc::clone(&budget)),
            );

            let mut sequential = Vec::new();
            for query in QUERIES {
                sequential.push(answer(&engine, query).await);
                assert_eq!(budget.reserved(), 0, "{query} released its charge");
            }

            let tasks: Vec<_> = (0..COPIES)
                .flat_map(|_| QUERIES.iter().enumerate())
                .map(|(n, &query)| {
                    let engine = Arc::clone(&engine);
                    tokio::spawn(async move { (n, answer(&engine, query).await) })
                })
                .collect();
            let mut answered = 0;
            for task in tasks {
                let (n, got) = task.await.expect("query task");
                assert_eq!(got, sequential[n], "{} concurrently", QUERIES[n]);
                answered += 1;
            }
            assert_eq!(answered, COPIES * QUERIES.len());
            assert_eq!(budget.reserved(), 0, "the concurrent pass released the budget");
        });
}
