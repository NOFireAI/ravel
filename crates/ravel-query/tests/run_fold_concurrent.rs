//! Issue #4530 (ADR-4523 decision 6, R5 step 2): PromQL range queries over
//! one `MemoryStore`, sharing one `MemoryBudget`, answer the same run
//! concurrently as run one after another, and the budget returns to zero
//! after each pass. On a budget too tight for some of them, the rest still
//! answer the same and the refused ones fail with the budget's typed error.
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

use ravel_catalog::{Catalog, CatalogConfig, CatalogError};
use ravel_commit::keys::commit_key_for_record;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_memory::MemoryBudget;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_promql::RangeValue;
use ravel_query::{EngineConfig, FetchError, QueryEngine, QueryError};
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

async fn try_answer(engine: &QueryEngine, query: &str) -> Result<Answer, QueryError> {
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
        .await?;
    let RangeValue::Matrix(matrix) = value else {
        panic!("{query} answers a matrix, got {value:?}");
    };
    assert!(!matrix.is_empty(), "{query} answers no series");
    let mut sorted: Vec<_> = matrix.iter().collect();
    sorted.sort_by(|a, b| a.0.iter().cmp(b.0.iter()));
    Ok(format!("{sorted:?}"))
}

async fn answer(engine: &QueryEngine, query: &str) -> Answer {
    try_answer(engine, query).await.expect("the query answers")
}

/// Whether `err` is the budget's typed refusal of a query's memory.
fn is_budget_refusal(err: &QueryError) -> bool {
    matches!(
        err,
        QueryError::Fetch(FetchError::FetchMemoryExhausted { .. })
            | QueryError::Catalog(CatalogError::MemoryExhausted(_))
    )
}

fn engine_on(
    catalog: &Arc<Catalog>,
    backend: &Arc<dyn ObjectStoreBackend>,
    budget: &Arc<MemoryBudget>,
) -> Arc<QueryEngine> {
    Arc::new(
        QueryEngine::new(
            Arc::clone(catalog),
            Arc::clone(backend),
            EngineConfig::default(),
        )
        .with_memory_budget(Arc::clone(budget)),
    )
}

/// The smallest budget limit, to within 64 bytes, on which `query` answers
/// alone on a fresh engine. Each probe on a smaller limit must fail with the
/// budget's typed refusal, and every probe returns the budget to 0.
async fn threshold(
    catalog: &Arc<Catalog>,
    backend: &Arc<dyn ObjectStoreBackend>,
    query: &str,
) -> u64 {
    let answers = |limit: u64| async move {
        let budget = Arc::new(MemoryBudget::new(limit));
        let result = try_answer(&engine_on(catalog, backend, &budget), query).await;
        assert_eq!(budget.reserved(), 0, "{query} at {limit} B released");
        match result {
            Ok(_) => true,
            Err(err) => {
                assert!(is_budget_refusal(&err), "{query} at {limit} B: {err:?}");
                false
            }
        }
    };
    let (mut refused, mut answered) = (0u64, 1u64 << 30);
    assert!(answers(answered).await, "{query} answers on 1 GiB");
    while answered - refused > 64 {
        let mid = refused + (answered - refused) / 2;
        if answers(mid).await {
            answered = mid;
        } else {
            refused = mid;
        }
    }
    answered
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
            assert_eq!(
                budget.reserved(),
                0,
                "the concurrent pass released the budget"
            );
        });
}

/// The query over one job's 80 series, which answers on a budget the
/// queries over all 400 series do not.
const SMALL: &str = "m{job=\"job-1\"}";

/// The same queries, run concurrently on a budget of twice what [`SMALL`]
/// needs alone, which is less than `m` over all 400 series needs alone, so
/// some queries are refused. Every query either answers exactly as on an
/// ample budget or fails with the budget's typed refusal, and the budget
/// returns to 0 after every round. Rounds repeat, at most 100, until a query
/// has answered beside the refused ones.
#[test]
fn concurrent_range_queries_on_a_tight_budget_answer_or_refuse_typed_and_release_it() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(async {
            let store = tenant().await;
            let backend: Arc<dyn ObjectStoreBackend> = store;
            let catalog = Arc::new(
                Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog"),
            );
            let ample = Arc::new(MemoryBudget::new(1 << 30));
            let reference = engine_on(&catalog, &backend, &ample);
            let mut sequential = Vec::new();
            for query in QUERIES {
                sequential.push(answer(&reference, query).await);
            }

            let small = threshold(&catalog, &backend, SMALL).await;
            let all = threshold(&catalog, &backend, "m").await;
            let limit = 2 * small;
            println!("tight budget: {SMALL} needs {small} B, m needs {all} B, limit {limit} B");
            assert!(all > limit, "m needs {all} B, within the {limit} B limit");

            let budget = Arc::new(MemoryBudget::new(limit));
            let engine = engine_on(&catalog, &backend, &budget);
            let (mut answered, mut refused) = (0usize, 0usize);
            for round in 0..100 {
                let tasks: Vec<_> = (0..COPIES)
                    .flat_map(|_| QUERIES.iter().enumerate())
                    .map(|(n, &query)| {
                        let engine = Arc::clone(&engine);
                        tokio::spawn(async move { (n, try_answer(&engine, query).await) })
                    })
                    .collect();
                for task in tasks {
                    let (n, got) = task.await.expect("query task");
                    match got {
                        Ok(got) => {
                            assert_eq!(got, sequential[n], "{} on the tight budget", QUERIES[n]);
                            answered += 1;
                        }
                        Err(err) => {
                            assert!(is_budget_refusal(&err), "{}: {err:?}", QUERIES[n]);
                            refused += 1;
                        }
                    }
                }
                assert_eq!(budget.reserved(), 0, "round {round} released the budget");
                if answered > 0 {
                    break;
                }
            }
            println!("tight budget: {answered} answered, {refused} refused");
            assert!(answered > 0, "no query answered on the tight budget");
            assert!(refused > 0, "no query was refused on the tight budget");
        });
}
