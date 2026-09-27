//! ADR-1306 decision 6 (follow-up task 6): a request-budget refusal names the
//! catalog's unsealed tail and the fold-liveness gauge when the tail is what
//! the budget was spent on, and keeps its pre-ADR-1306 wording when it is not.
//!
//! The flipped line: `crates/ravel-query/src/engine.rs`'s `resolve_bounded`
//! computes `fold_lag` from the origins the resolve produced and returns it in
//! `ResolvedBounded`. Replace that call with `FoldLag::Healthy` (the pre-fix
//! behavior: no refusal ever named the tail) and
//! `budget_refusal_during_fold_lag_names_the_unsealed_tail` fails with
//! `left: Healthy, right: Lagging { unsealed_tail: 19800s, healthy_tail_max:
//! 8400s }`, and the three message assertions after it fail too.
//!
//! Both cases drive a real range query through `QueryEngine` against
//! `MemoryStore`, so the refusal comes from the resolve path the server runs,
//! never from the error constructor. Time is injected: every instant is
//! computed from a fixed ingest hour, and no test here reads a wall clock.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::http::QueryErrorResponse;
use ravel_query::{
    EngineConfig, FOLD_LAST_SUCCESS_GAUGE, FoldLag, QueryEngine, QueryError, RequestLimit,
    SealMargin, healthy_tail_max,
};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;

/// The ingest hour the fixtures call "now". An arbitrary but fixed hour
/// bucket, large enough that subtracting several hours stays positive.
const NOW_HOUR: u32 = 490_000;
/// Offset into [`NOW_HOUR`] at which every query below runs.
const NOW_OFFSET_NS: i64 = 30 * NS_PER_MIN;
const METRIC: &str = "m";

/// Query time: half an hour into [`NOW_HOUR`].
fn now_ns() -> i64 {
    i64::from(NOW_HOUR) * NS_PER_HOUR + NOW_OFFSET_NS
}

/// Writes one real RSEG segment carrying a single sample into `ingest_hour`
/// and publishes its commit record. Nothing folds in this file, so every
/// segment published here resolves as `SegmentOrigin::Recent` -- an unsealed
/// tail, which is exactly the state a stalled fold leaves behind.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    tenant_hash: TenantHash,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    ts_ns: i64,
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
    let label_set = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: METRIC.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, METRIC, &label_set).expect("series id");
    let input = SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample {
            ts_ns,
            value: 1.125,
        }],
    };
    let written = SegmentWriter::write(vec![input], identity, bounds).expect("write segment");

    let rec = record::build(NewCommitRecord {
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
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: 0,
        ingest_hour_bucket,
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// An engine over a fresh `MemoryStore` holding one segment per entry of
/// `ingest_hours`, with the request budget pinned at `max_s3_requests`. The
/// catalog is returned alongside so a test can fold through the same handle
/// the engine resolves through.
async fn engine_with_unsealed_hours(
    ingest_hours: &[u32],
    max_s3_requests: RequestLimit,
) -> (QueryEngine, TenantHash, Arc<Catalog>) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant_id = TenantId::new("acme".to_string());
    let tenant_hash = tenant_id.hash();
    for (i, hour) in ingest_hours.iter().enumerate() {
        publish_segment(
            store.as_ref(),
            &tenant_id,
            tenant_hash,
            i as u64 + 1,
            *hour,
            // Ten minutes into the hour, inside every window below.
            i64::from(*hour) * NS_PER_HOUR + 10 * NS_PER_MIN,
        )
        .await;
    }
    let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    let config = EngineConfig {
        max_s3_requests,
        fetch_concurrency: 1,
        ..EngineConfig::default()
    };
    (
        QueryEngine::new(Arc::clone(&catalog), store, config),
        tenant_hash,
        catalog,
    )
}

/// Runs `METRIC` as a range query over `[now - window_ns, now]` and returns
/// the refusal it must produce.
async fn refuse_range_query(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    window_ns: i64,
) -> QueryError {
    let now = now_ns();
    let result = engine
        .range(
            tenant_hash,
            METRIC,
            (now - window_ns) / 1_000_000,
            now / 1_000_000,
            60_000,
            &[],
            now,
            Duration::from_secs(30),
        )
        .await;
    match result {
        Err(err) => err,
        Ok(_) => panic!("the pinned request budget must refuse this query"),
    }
}

/// ADR-1306 follow-up task 6's acceptance test. Two refusals from the same
/// engine path, differing only in the unsealed tail the resolve listed:
///
/// - a 5 h 30 m tail, well past `healthy_tail_max` (2 h 20 m at the reference
///   seal margin): the message carries the tail's length in seconds and names
///   `ravel_catalog_fold_last_success_timestamp_seconds`;
/// - a 30 m tail: the message is byte-identical to the pre-ADR-1306 wording,
///   so a refusal the fold did not cause blames nothing.
///
/// Both still map to HTTP 422 with the `execution` error type, and both echo
/// their own text (the fold-lag clause carries a duration and a metric name,
/// no object key and no tenant identity, so the redaction boundary treats it
/// like the counts it sits beside).
#[tokio::test]
async fn budget_refusal_during_fold_lag_names_the_unsealed_tail() {
    let healthy_max = healthy_tail_max(SealMargin::REFERENCE);
    assert_eq!(
        healthy_max,
        Duration::from_secs(8_400),
        "test setup: the reference healthy tail is 2 h 20 m"
    );

    // Half 1: the oldest unsealed hour the resolve lists is five hours back,
    // so the tail runs from the start of that hour to query time.
    let lagging_hours = [NOW_HOUR - 5, NOW_HOUR];
    let (engine, tenant_hash, _catalog) =
        engine_with_unsealed_hours(&lagging_hours, RequestLimit::Bounded(1)).await;
    let lagging = refuse_range_query(&engine, tenant_hash, 5 * NS_PER_HOUR + 30 * NS_PER_MIN).await;

    let expected_tail = Duration::from_nanos(
        (now_ns() - i64::from(NOW_HOUR - 5) * NS_PER_HOUR)
            .try_into()
            .expect("a positive tail"),
    );
    assert_eq!(
        expected_tail,
        Duration::from_secs(5 * 3_600 + 30 * 60),
        "test setup: the fixture's tail is 5 h 30 m"
    );
    assert!(
        expected_tail > healthy_max,
        "test setup: {expected_tail:?} must exceed the healthy bound {healthy_max:?}"
    );

    let QueryError::RequestBudgetExceeded {
        requests,
        max,
        fold_lag,
    } = &lagging
    else {
        panic!("expected QueryError::RequestBudgetExceeded, got {lagging:?}");
    };
    assert!(
        requests > max,
        "a refusal must carry the spend that passed the budget: {requests} vs {max}"
    );
    assert_eq!(
        *fold_lag,
        FoldLag::Lagging {
            unsealed_tail: expected_tail,
            healthy_tail_max: healthy_max,
        },
        "the refusal must carry the tail the resolve listed, not an estimate"
    );
    let message = lagging.to_string();
    assert!(
        message.contains("19800 s"),
        "the message must name the tail's length: {message}"
    );
    assert!(
        message.contains("8400 s"),
        "the message must name the healthy bound it exceeded: {message}"
    );
    assert!(
        message.contains(FOLD_LAST_SUCCESS_GAUGE),
        "the message must name the fold-liveness gauge: {message}"
    );
    let rendered = QueryErrorResponse::from_query_error(lagging);
    assert_eq!(rendered.status.as_u16(), 422);
    assert_eq!(rendered.error_type, "execution");
    assert!(
        rendered.message.contains(FOLD_LAST_SUCCESS_GAUGE) && rendered.message.contains("19800 s"),
        "the 422 body must carry the fold-lag text, not a redacted class message: {}",
        rendered.message
    );

    // Half 2: same engine path, same budget, a tail of 30 minutes. The
    // refusal is the one this code shipped before ADR-1306, to the byte.
    let (engine, tenant_hash, _catalog) =
        engine_with_unsealed_hours(&[NOW_HOUR], RequestLimit::Bounded(1)).await;
    let healthy = refuse_range_query(&engine, tenant_hash, 20 * NS_PER_MIN).await;

    let QueryError::RequestBudgetExceeded {
        requests,
        max,
        fold_lag,
    } = &healthy
    else {
        panic!("expected QueryError::RequestBudgetExceeded, got {healthy:?}");
    };
    assert_eq!(
        *fold_lag,
        FoldLag::Healthy,
        "a 30 m tail is inside the healthy bound and must not be reported as lag"
    );
    let (requests, max) = (*requests, *max);
    let message = healthy.to_string();
    assert_eq!(
        message,
        format!("query issued {requests} S3 requests, exceeding the budget of {max}"),
        "a refusal with a healthy tail must keep its pre-ADR-1306 message exactly"
    );
    assert!(
        !message.contains(FOLD_LAST_SUCCESS_GAUGE),
        "a healthy tail must not send an operator to the fold: {message}"
    );
    let rendered = QueryErrorResponse::from_query_error(healthy);
    assert_eq!(rendered.status.as_u16(), 422);
    assert_eq!(rendered.error_type, "execution");
    assert_eq!(rendered.message, message);
}

/// Non-vacuity for the fixture above: the same tenant and window, with the
/// budget left unbounded, runs to a result. Without this, a fixture that was
/// broken in some other way (an unresolvable window, a missing segment) would
/// still "refuse" and the assertions above would pass on the wrong error.
#[tokio::test]
async fn the_fold_lag_fixture_is_queryable_without_the_budget() {
    let (engine, tenant_hash, _catalog) =
        engine_with_unsealed_hours(&[NOW_HOUR - 5, NOW_HOUR], RequestLimit::Unlimited).await;
    let now = now_ns();
    let (value, _coverage) = engine
        .range(
            tenant_hash,
            METRIC,
            (now - 5 * NS_PER_HOUR - 30 * NS_PER_MIN) / 1_000_000,
            now / 1_000_000,
            60_000,
            &[],
            now,
            Duration::from_secs(30),
        )
        .await
        .expect("an unbounded budget must admit the fixture's query");
    let series = match &value {
        ravel_promql::Value::Matrix(m) => m.len(),
        other => panic!("expected a range vector, got {other:?}"),
    };
    assert_eq!(series, 1, "the fixture's one metric must resolve");
}

/// The other side of the tail's lower-bound property: a catalog whose fold
/// IS running seals its old hours, so the same wide window over the same six
/// hours of data resolves a short tail and its refusal blames nothing. Only
/// the fold distinguishes this case from half 1 of the acceptance test above,
/// which is the claim decision 6 rests on -- a refusal names fold lag when
/// there is fold lag, not whenever a query is wide.
#[tokio::test]
async fn a_folded_catalog_refusal_does_not_blame_the_fold() {
    let hours: Vec<u32> = (0..6).map(|back| NOW_HOUR - back).collect();
    let (engine, tenant_hash, catalog) =
        engine_with_unsealed_hours(&hours, RequestLimit::Bounded(1)).await;
    let report = catalog
        .fold(
            &tenant_hash,
            Signal::Metrics,
            Uuid::new_v4(),
            now_ns(),
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(
        report.watermark_hour,
        Some(NOW_HOUR - 2),
        "test setup: at the reference seal margin this fold seals through NOW_HOUR - 2"
    );

    let err = refuse_range_query(&engine, tenant_hash, 5 * NS_PER_HOUR + 30 * NS_PER_MIN).await;
    let QueryError::RequestBudgetExceeded { fold_lag, .. } = &err else {
        panic!("expected QueryError::RequestBudgetExceeded, got {err:?}");
    };
    assert_eq!(
        *fold_lag,
        FoldLag::Healthy,
        "a folding catalog leaves at most a healthy tail, whatever the query's width"
    );
    assert!(!err.to_string().contains(FOLD_LAST_SUCCESS_GAUGE));
}
