//! End-to-end proof that a pending selective-erasure request excludes records
//! from a real query result (ADR-0064 decision 2).
//!
//! Every test here drives the whole stack: a real `.dreq` object in the store,
//! the real `Catalog::resolve` that lists `t/<th>/<sig>/del/` and attaches the
//! decoded request to `Snapshot::pending_erasure`, and the real `QueryEngine`
//! fetch funnels that map those requests into scan-time predicates. Nothing is
//! injected and no filter function is called by hand, so a call site deleted
//! from `fetch_all_samples_and_histograms` or `fetch_all_series` fails these
//! tests -- which the crate's unit tests, driving the mapping and the filter
//! separately, do not.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{erasure, keys, publish, record, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_promql::{LabelMatcher, Value};
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds, ResetHint,
    SegmentIdentity, SegmentWriter, SeriesInput, SeriesInputV3, SeriesValues,
};
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash,
    TenantId, TimeRange,
};
use uuid::Uuid;

const NS: i64 = 1_000_000_000;
/// Two sample times, far enough apart to sit in distinct erasure windows.
const TS_A: i64 = 1_000 * NS;
const TS_B: i64 = 2_000 * NS;
const METRIC: &str = "m";

fn label_set(user_id: &str) -> LabelSet {
    LabelSet::new(vec![
        Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: METRIC.to_string(),
        },
        Label {
            name: "user_id".to_string(),
            value: user_id.to_string(),
        },
    ])
    .expect("valid labels")
}

/// Publishes one RSEG segment carrying `m{user_id="u1"}` and `m{user_id="u2"}`,
/// each with the given samples, and returns its read-your-write token.
async fn publish_segment(
    store: &MemoryStore,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
    samples: &[i64],
) -> CommitToken {
    let writer_id = Uuid::from_u128(7);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 0,
    };
    let inputs: Vec<SeriesInput> = ["u1", "u2"]
        .iter()
        .map(|uid| {
            let labels = label_set(uid);
            SeriesInput {
                series_id: SeriesId::compute(tenant_id, METRIC, &labels).expect("series id"),
                labels,
                samples: samples
                    .iter()
                    .map(|ts| Sample {
                        ts_ns: *ts,
                        value: 1.0,
                    })
                    .collect(),
            }
        })
        .collect();
    let written = SegmentWriter::write(
        inputs,
        identity,
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
        writer_epoch: 1,
        writer_seq: 0,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 42,
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

/// Publishes one RSEG segment carrying native-histogram series
/// `m{user_id="u1"}` and `m{user_id="u2"}`, each with one histogram sample at
/// `TS_A`, and returns its read-your-write token. Mirrors [`publish_segment`]
/// but through `SegmentWriter::write_histograms`, so the query path decodes it
/// as native histograms and filters it via `retain_histogram_series` rather
/// than the scalar `retain_series_soa`.
async fn publish_histogram_segment(
    store: &MemoryStore,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
) -> CommitToken {
    let writer_id = Uuid::from_u128(9);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 0,
    };
    let one_bucket_histogram = || HistogramValue {
        scale: 0,
        zero_threshold: 0.0,
        sum: Some(1.0),
        custom_values: None,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: 1,
        }],
        negative_spans: Vec::new(),
        counts: HistogramCounts::Int {
            zero_count: 0,
            count: 1,
            positive: vec![1],
            negative: Vec::new(),
        },
        reset_hint: ResetHint::Unknown,
    };
    let inputs: Vec<SeriesInputV3> = ["u1", "u2"]
        .iter()
        .map(|uid| {
            let labels = label_set(uid);
            SeriesInputV3 {
                series_id: SeriesId::compute(tenant_id, METRIC, &labels).expect("series id"),
                labels,
                values: SeriesValues::Histogram(vec![HistogramSample {
                    ts_ns: TS_A,
                    value: one_bucket_histogram(),
                }]),
            }
        })
        .collect();
    let written = SegmentWriter::write_histograms(
        inputs,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write histogram segment");

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 0,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 42,
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

/// Writes a real, valid `.dreq` erasure request object for `user_id = <value>`
/// under this tenant's metrics `del/` prefix, exactly as the erasure submit
/// path does. The resolver picks it up on the next resolve.
async fn put_dreq(
    store: &MemoryStore,
    tenant_hash: TenantHash,
    value: &str,
    window_start_ns: i64,
    window_end_ns: i64,
) {
    put_dreq_matchers(
        store,
        tenant_hash,
        &[("user_id", value)],
        window_start_ns,
        window_end_ns,
    )
    .await;
}

/// The general form of [`put_dreq`]: a request whose predicate is the
/// conjunction of every `(key, value)` in `matchers`.
async fn put_dreq_matchers(
    store: &MemoryStore,
    tenant_hash: TenantHash,
    matchers: &[(&str, &str)],
    window_start_ns: i64,
    window_end_ns: i64,
) {
    let request_id = Uuid::new_v4();
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 1,
        predicate: matchers
            .iter()
            .map(|(k, v)| ErasurePredicateMatcher {
                key: (*k).to_string(),
                value: (*v).to_string(),
            })
            .collect(),
        window_start_ns,
        window_end_ns,
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

/// A store with one two-series segment, plus the engine reading it.
async fn setup(samples: &[i64]) -> (Arc<MemoryStore>, QueryEngine, CommitToken, TenantHash) {
    let tenant_id = TenantId::new("tenant-a".to_string());
    let tenant_hash = tenant_id.hash();
    let store = Arc::new(MemoryStore::new());
    let token = publish_segment(&store, &tenant_id, tenant_hash, samples).await;
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = QueryEngine::new(catalog, backend, EngineConfig::default());
    (store, engine, token, tenant_hash)
}

/// The `user_id` label values an instant query at `ts_ns` returns, sorted.
async fn queried_user_ids(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    token: &CommitToken,
    ts_ns: i64,
) -> Vec<String> {
    let (value, _coverage) = engine
        .instant(
            tenant_hash,
            METRIC,
            ts_ns / 1_000_000,
            std::slice::from_ref(token),
            TS_B + NS,
            Duration::from_secs(30),
        )
        .await
        .expect("instant query");
    let Value::Vector(vector) = value else {
        panic!("instant query over a selector must return a vector");
    };
    let mut ids: Vec<String> = vector
        .iter()
        .map(|s| {
            s.labels
                .get("user_id")
                .expect("series carries user_id")
                .to_string()
        })
        .collect();
    ids.sort();
    ids
}

/// The distinct `user_id` label values a range query over `[TS_A, TS_B]`
/// returns, sorted. The `range` entry point is a distinct materialization
/// surface from `instant`: it fetches through the same sample funnel but
/// renders a `Value::Matrix`. Driving it here means a regression that drops
/// the erasure pass from the range path (or applies it to a narrower view
/// than the matrix it returns) fails this helper's callers.
async fn range_user_ids(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    token: &CommitToken,
) -> Vec<String> {
    let (value, _coverage) = engine
        .range(
            tenant_hash,
            METRIC,
            TS_A / 1_000_000,
            TS_B / 1_000_000,
            // One step per sample time: TS_A and TS_B are 1000s apart, so a
            // step lands on each and sees its own sample regardless of the
            // evaluator's lookback.
            1_000_000,
            std::slice::from_ref(token),
            TS_B + NS,
            Duration::from_secs(30),
        )
        .await
        .expect("range query");
    let Value::Matrix(matrix) = value else {
        panic!("range query over a selector must return a matrix");
    };
    let mut ids: Vec<String> = matrix
        .iter()
        .map(|(labels, _samples)| {
            labels
                .get("user_id")
                .expect("series carries user_id")
                .to_string()
        })
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

/// The `user_id` label values the series/labels metadata path returns, sorted.
async fn enumerated_user_ids(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    token: &CommitToken,
) -> Vec<String> {
    let (series, _coverage) = engine
        .resolve_series(
            tenant_hash,
            &[LabelMatcher::equal(METRIC_NAME_LABEL, METRIC)],
            TimeRange {
                start_ns: 0,
                end_ns: TS_B + NS,
            },
            std::slice::from_ref(token),
            TS_B + NS,
            Duration::from_secs(30),
        )
        .await
        .expect("resolve series");
    let mut ids: Vec<String> = series
        .iter()
        .map(|(_, labels)| {
            labels
                .get("user_id")
                .expect("series carries user_id")
                .to_string()
        })
        .collect();
    ids.sort();
    ids
}

/// Regression baseline: with no `.dreq` in the store the snapshot's
/// `pending_erasure` is empty, and both query surfaces return every series.
#[tokio::test]
async fn no_pending_erasure_excludes_nothing() {
    let (_store, engine, token, tenant_hash) = setup(&[TS_A]).await;
    assert_eq!(
        queried_user_ids(&engine, tenant_hash, &token, TS_A).await,
        vec!["u1".to_string(), "u2".to_string()],
    );
    assert_eq!(
        enumerated_user_ids(&engine, tenant_hash, &token).await,
        vec!["u1".to_string(), "u2".to_string()],
    );
}

/// A durable windowless `.dreq` excludes the matching series from an instant
/// query's result, through catalog resolve and the engine's sample fetch
/// funnel. The non-matching series is untouched.
#[tokio::test]
async fn pending_erasure_excludes_matching_series_from_query_results() {
    let (store, engine, token, tenant_hash) = setup(&[TS_A]).await;
    assert_eq!(
        queried_user_ids(&engine, tenant_hash, &token, TS_A).await,
        vec!["u1".to_string(), "u2".to_string()],
        "baseline before the erasure request is durable",
    );

    put_dreq(&store, tenant_hash, "u1", 0, 0).await;

    assert_eq!(
        queried_user_ids(&engine, tenant_hash, &token, TS_A).await,
        vec!["u2".to_string()],
        "the erased subject's series must not reach the caller",
    );
}

/// The same windowless request also excludes the matching series from a
/// *range* query result. The range path renders a `Value::Matrix` through a
/// materialization step distinct from the instant vector one, so covering it
/// separately proves the erasure pass is on the range surface too, not only
/// the instant surface.
#[tokio::test]
async fn pending_erasure_excludes_matching_series_from_range_query() {
    let (store, engine, token, tenant_hash) = setup(&[TS_A, TS_B]).await;
    assert_eq!(
        range_user_ids(&engine, tenant_hash, &token).await,
        vec!["u1".to_string(), "u2".to_string()],
        "baseline before the erasure request is durable",
    );

    put_dreq(&store, tenant_hash, "u1", 0, 0).await;

    assert_eq!(
        range_user_ids(&engine, tenant_hash, &token).await,
        vec!["u2".to_string()],
        "the erased subject's series must not reach the caller on the range path",
    );
}

/// A native-histogram series takes a decode-and-materialize path distinct from
/// scalar samples, filtered by its own `retain_histogram_series` call site. A
/// durable windowless `.dreq` must exclude a matching histogram series from an
/// instant query exactly as it does a scalar one; this drives that separate
/// call site through the real engine entry point.
#[tokio::test]
async fn pending_erasure_excludes_matching_histogram_series_from_query() {
    let tenant_id = TenantId::new("tenant-a".to_string());
    let tenant_hash = tenant_id.hash();
    let store = Arc::new(MemoryStore::new());
    let token = publish_histogram_segment(&store, &tenant_id, tenant_hash).await;
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog =
        Arc::new(Catalog::new(backend.clone(), CatalogConfig::default()).expect("catalog"));
    let engine = QueryEngine::new(catalog, backend, EngineConfig::default());

    assert_eq!(
        queried_user_ids(&engine, tenant_hash, &token, TS_A).await,
        vec!["u1".to_string(), "u2".to_string()],
        "baseline: both native-histogram series are returned before erasure",
    );

    put_dreq(&store, tenant_hash, "u1", 0, 0).await;

    assert_eq!(
        queried_user_ids(&engine, tenant_hash, &token, TS_A).await,
        vec!["u2".to_string()],
        "the erased histogram subject's series must not reach the caller",
    );
}

/// The same request also excludes the series from the labels/series metadata
/// path, which fetches through `fetch_all_series` rather than the sample
/// funnel.
#[tokio::test]
async fn pending_erasure_excludes_matching_series_from_series_enumeration() {
    let (store, engine, token, tenant_hash) = setup(&[TS_A]).await;
    put_dreq(&store, tenant_hash, "u1", 0, 0).await;

    assert_eq!(
        enumerated_user_ids(&engine, tenant_hash, &token).await,
        vec!["u2".to_string()],
        "an erased series must not appear in series enumeration either",
    );
}

/// The request's `[window_start_ns, window_end_ns)` bounds are carried into the
/// scan predicate unchanged: a window covering only `TS_B` erases the matching
/// series' sample at `TS_B` and leaves its sample at `TS_A`. Swapped or dropped
/// bounds change which instant returns the series, so this pins the mapping,
/// not just the fact that some filtering happened.
#[tokio::test]
async fn pending_erasure_window_bounds_are_carried_through() {
    let (store, engine, token, tenant_hash) = setup(&[TS_A, TS_B]).await;
    put_dreq(&store, tenant_hash, "u1", TS_B, TS_B + NS).await;

    assert_eq!(
        queried_user_ids(&engine, tenant_hash, &token, TS_B).await,
        vec!["u2".to_string()],
        "the in-window sample of the matching series is erased",
    );
    assert_eq!(
        queried_user_ids(&engine, tenant_hash, &token, TS_A).await,
        vec!["u1".to_string(), "u2".to_string()],
        "the out-of-window sample of the same series survives",
    );
}

/// Every matcher of a request's conjunction is carried into the predicate. The
/// request names `user_id = u1 AND region = eu`; no series carries a `region`
/// label at all, so the conjunction matches nothing and every series survives.
/// Dropping a matcher anywhere in the mapping would widen the predicate to
/// `user_id = u1` and silently erase more than the operator asked for, which is
/// the unrecoverable direction for an irreversible erasure.
#[tokio::test]
async fn every_matcher_of_the_conjunction_is_carried_through() {
    let (store, engine, token, tenant_hash) = setup(&[TS_A]).await;
    put_dreq_matchers(
        &store,
        tenant_hash,
        &[("user_id", "u1"), ("region", "eu")],
        0,
        0,
    )
    .await;

    assert_eq!(
        queried_user_ids(&engine, tenant_hash, &token, TS_A).await,
        vec!["u1".to_string(), "u2".to_string()],
        "an unsatisfied conjunct must keep the predicate from matching",
    );
    assert_eq!(
        enumerated_user_ids(&engine, tenant_hash, &token).await,
        vec!["u1".to_string(), "u2".to_string()],
        "the same on the enumeration path",
    );
}

/// Two independent pending requests (two DSAR tickets at once) are each mapped
/// to their own predicate: both subjects are excluded, not just the first.
#[tokio::test]
async fn every_pending_request_produces_its_own_predicate() {
    let (store, engine, token, tenant_hash) = setup(&[TS_A]).await;
    put_dreq(&store, tenant_hash, "u1", 0, 0).await;
    put_dreq(&store, tenant_hash, "u2", 0, 0).await;

    assert!(
        queried_user_ids(&engine, tenant_hash, &token, TS_A)
            .await
            .is_empty(),
        "both pending requests must be applied, not only the first",
    );
    assert!(
        enumerated_user_ids(&engine, tenant_hash, &token)
            .await
            .is_empty(),
        "both pending requests must be applied on the enumeration path too",
    );
}
