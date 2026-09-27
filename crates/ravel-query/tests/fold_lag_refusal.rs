//! ADR-1306 decision 6 (follow-up task 6): a request-budget refusal names the
//! catalog's unsealed tail and the fold-liveness gauge when the tail is what
//! the budget was spent on, and keeps its pre-ADR-1306 wording when it is not.
//!
//! Three flipped lines, one per claim this file pins:
//!
//! - `crates/ravel-query/src/engine.rs`'s `resolve_bounded` computes
//!   `fold_lag` from the origins the resolve produced and returns it in
//!   `ResolvedBounded`. Replace that call with `FoldLag::Healthy` (the pre-fix
//!   behavior: no refusal ever named the tail) and
//!   `budget_refusal_during_fold_lag_names_the_unsealed_tail` fails with
//!   `left: Healthy, right: Lagging { unsealed_tail: 19800s,
//!   fold_lag_threshold: 8730s }`, and the three message assertions after it
//!   fail too.
//! - `EngineConfig::fold_lag_threshold` (`config.rs`) adds the fold interval
//!   and the HEAD cache TTL to `healthy_tail_max`. Return
//!   `healthy_tail_max(self.seal_margin)` instead (the pre-fix threshold) and
//!   `a_tail_a_keeping_up_fold_can_show_is_not_lag` fails with `left:
//!   Lagging { unsealed_tail: 8700s, fold_lag_threshold: 8400s }, right:
//!   Healthy`.
//! - `resolved_unsealed_tail` (`segment_admission.rs`) returns `None` when
//!   `resolve_read_a_snapshot_part` is false. Delete that early return (the
//!   pre-fix behavior: every `Recent` tag counted as tail whether or not a
//!   watermark was ever read) and
//!   `a_listing_fallback_refusal_does_not_blame_the_fold` fails with `left:
//!   Lagging { unsealed_tail: 27000s, fold_lag_threshold: 8730s }, right:
//!   Healthy`, on a catalog that has no fold state at all.
//!
//! Every case drives a real range query through `QueryEngine` against
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

/// Folds `tenant_hash`'s metrics through the same catalog handle the engine
/// resolves through, at an injected `fold_now`, and returns the watermark hour
/// it sealed to. Every lagging fixture below folds once: fold lag is a fold
/// that ran and then stopped, and only a resolve that reads a folded snapshot
/// part knows where the watermark is (ADR-1306 "Amendment (2026-09-27,
/// #1306)").
async fn fold_at(catalog: &Catalog, tenant_hash: TenantHash, fold_now_ns: i64) -> Option<u32> {
    catalog
        .fold(
            &tenant_hash,
            Signal::Metrics,
            Uuid::new_v4(),
            fold_now_ns,
            &[],
            None,
        )
        .await
        .expect("fold")
        .watermark_hour
}

/// Runs `METRIC` as a range query over `[now - window_ns, now]` and returns
/// the refusal it must produce.
async fn refuse_range_query(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    window_ns: i64,
) -> QueryError {
    refuse_range_query_at(engine, tenant_hash, now_ns(), window_ns).await
}

/// [`refuse_range_query`] at an explicit query time, for a fixture whose tail
/// is not a whole number of hours.
async fn refuse_range_query_at(
    engine: &QueryEngine,
    tenant_hash: TenantHash,
    now: i64,
    window_ns: i64,
) -> QueryError {
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

/// The threshold a refusal classifies its tail against: `healthy_tail_max`
/// (8,400 s at the reference seal margin) plus the 300 s fold interval plus
/// the 30 s HEAD cache TTL, 8,730 s.
///
/// Spelled as a literal rather than read back from
/// [`EngineConfig::fold_lag_threshold`]: a figure taken from the code under
/// test moves with it, so a threshold mutated back to `healthy_tail_max`
/// would still satisfy every assertion below.
fn threshold() -> Duration {
    Duration::from_secs(8_730)
}

/// ADR-1306 follow-up task 6's acceptance test. Two refusals from the same
/// engine path, differing only in the unsealed tail the resolve listed:
///
/// - a 5 h 30 m tail, well past the 8,730 s a fold that is keeping up can
///   show: the message carries the tail's length in seconds and names
///   `ravel_catalog_fold_last_success_timestamp_seconds`;
/// - a 30 m tail: the message is byte-identical to the pre-ADR-1306 wording,
///   so a refusal the fold did not cause blames nothing.
///
/// The lagging fixture folds once, at an injected instant six hours before
/// query time, and then never again. That is what fold lag is, and it is also
/// what makes the tail measurable: a resolve that read no folded snapshot part
/// has no watermark to measure a tail against
/// (`a_listing_fallback_refusal_does_not_blame_the_fold` below).
///
/// Both still map to HTTP 422 with the `execution` error type, and both echo
/// their own text (the fold-lag clause carries a duration and a metric name,
/// no object key and no tenant identity, so the redaction boundary treats it
/// like the counts it sits beside).
#[tokio::test]
async fn budget_refusal_during_fold_lag_names_the_unsealed_tail() {
    assert_eq!(
        healthy_tail_max(SealMargin::REFERENCE),
        Duration::from_secs(8_400),
        "test setup: the reference healthy tail is 2 h 20 m"
    );

    // Half 1: one segment in an hour the fold sealed, and two in hours it did
    // not. The oldest unsealed hour the resolve lists is five hours back, so
    // the tail runs from the start of that hour to query time.
    let lagging_hours = [NOW_HOUR - 7, NOW_HOUR - 5, NOW_HOUR];
    let (engine, tenant_hash, catalog) =
        engine_with_unsealed_hours(&lagging_hours, RequestLimit::Bounded(1)).await;
    // 1 h 50 m into NOW_HOUR - 5 the seal margin (4,800 s) has elapsed for
    // NOW_HOUR - 6 and not for NOW_HOUR - 5, so this fold seals to NOW_HOUR - 6
    // and the fixture's NOW_HOUR - 7 segment is below the watermark.
    let fold_now = i64::from(NOW_HOUR - 5) * NS_PER_HOUR + 110 * NS_PER_MIN;
    assert_eq!(
        fold_at(&catalog, tenant_hash, fold_now).await,
        Some(NOW_HOUR - 6),
        "test setup: the one fold that ran sealed through NOW_HOUR - 6"
    );

    // The window reaches back past the sealed segment's event time, so the
    // resolve extracts it from the snapshot and therefore knows the watermark.
    // The tail is set by the oldest RECENT hour, NOW_HOUR - 5, not by the
    // window's width.
    let lagging = refuse_range_query(
        &engine,
        tenant_hash,
        7 * NS_PER_HOUR + 30 * NS_PER_MIN - 5 * NS_PER_MIN,
    )
    .await;

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
        expected_tail > threshold(),
        "test setup: {expected_tail:?} must exceed the threshold {:?}",
        threshold()
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
            fold_lag_threshold: threshold(),
        },
        "the refusal must carry the tail the resolve listed, not an estimate"
    );
    let message = lagging.to_string();
    assert!(
        message.contains("19800 s"),
        "the message must name the tail's length: {message}"
    );
    assert!(
        message.contains("8730 s"),
        "the message must name the threshold it exceeded: {message}"
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
        "a 30 m tail is inside the threshold and must not be reported as lag"
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

    // Last, so a threshold that moved fails the assertions above on what the
    // refusal said rather than here on a figure: the 8,730 s those assertions
    // are written against is the engine's own derived threshold, 8,400 s of
    // healthy tail plus the 300 s fold interval plus the 30 s HEAD cache TTL.
    assert_eq!(EngineConfig::default().fold_lag_threshold(), threshold());
}

/// Non-vacuity for the fixture above: the same tenant, fold and window, with
/// the budget left unbounded, runs to a result. Without this, a fixture that
/// was broken in some other way (an unresolvable window, a missing segment)
/// would still "refuse" and the assertions above would pass on the wrong
/// error.
#[tokio::test]
async fn the_fold_lag_fixture_is_queryable_without_the_budget() {
    let (engine, tenant_hash, catalog) = engine_with_unsealed_hours(
        &[NOW_HOUR - 7, NOW_HOUR - 5, NOW_HOUR],
        RequestLimit::Unlimited,
    )
    .await;
    let fold_now = i64::from(NOW_HOUR - 5) * NS_PER_HOUR + 110 * NS_PER_MIN;
    assert_eq!(
        fold_at(&catalog, tenant_hash, fold_now).await,
        Some(NOW_HOUR - 6)
    );
    let now = now_ns();
    let window_ns = 7 * NS_PER_HOUR + 30 * NS_PER_MIN - 5 * NS_PER_MIN;
    let (value, _coverage) = engine
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
        .await
        .expect("an unbounded budget must admit the fixture's query");
    let series = match &value {
        ravel_promql::Value::Matrix(m) => m.len(),
        other => panic!("expected a range vector, got {other:?}"),
    };
    assert_eq!(series, 1, "the fixture's one metric must resolve");
}

/// ADR-1306 "Amendment (2026-09-27, #1306)", finding 2. The same three hours
/// of data and the same wide window as the acceptance test's half 1, with the
/// fold never having run at all. The resolve finds no snapshot to read, lists
/// the whole window live and tags every key `Recent` -- including hours a fold
/// that is keeping up would already have sealed. There is no watermark behind
/// those tags, so the refusal names nothing.
///
/// This is the false positive the amendment closes: before it, this fixture
/// reported the same `Lagging { unsealed_tail: 19800s }` the acceptance test
/// asserts, on a catalog whose fold state is simply unknown to the resolve.
#[tokio::test]
async fn a_listing_fallback_refusal_does_not_blame_the_fold() {
    let (engine, tenant_hash, _catalog) = engine_with_unsealed_hours(
        &[NOW_HOUR - 7, NOW_HOUR - 5, NOW_HOUR],
        RequestLimit::Bounded(1),
    )
    .await;
    // No fold_at: nothing has ever folded this tenant, so there is no HEAD to
    // read and the resolve falls back to listing the whole window.
    let err = refuse_range_query(
        &engine,
        tenant_hash,
        7 * NS_PER_HOUR + 30 * NS_PER_MIN - 5 * NS_PER_MIN,
    )
    .await;
    let QueryError::RequestBudgetExceeded {
        requests,
        max,
        fold_lag,
    } = &err
    else {
        panic!("expected QueryError::RequestBudgetExceeded, got {err:?}");
    };
    assert_eq!(
        *fold_lag,
        FoldLag::Healthy,
        "a resolve that read no snapshot part has no watermark to measure a tail against"
    );
    let (requests, max) = (*requests, *max);
    assert_eq!(
        err.to_string(),
        format!("query issued {requests} S3 requests, exceeding the budget of {max}"),
        "the listing-fallback refusal must keep the plain wording"
    );
    assert!(!err.to_string().contains(FOLD_LAST_SUCCESS_GAUGE));
}

/// ADR-1306 "Amendment (2026-09-27, #1306)", finding 1: the threshold is not
/// `healthy_tail_max`. A fold leaves at most 8,400 s unsealed at the instant
/// it runs, then waits one 300 s fold interval before running again, and the
/// resolve reads the resulting watermark through a 30 s HEAD cache. A tail of
/// 8,700 s is therefore one a fold that is keeping up really can show, and
/// blaming it would put a fold-lag clause on about five minutes of every hour
/// on a healthy deployment.
///
/// The fixture folds on schedule and queries 2 h 25 m past the start of the
/// oldest hour the fold has not sealed. Against the pre-fix 8,400 s bound this
/// classifies `Lagging`; against 8,730 s it is `Healthy`.
#[tokio::test]
async fn a_tail_a_keeping_up_fold_can_show_is_not_lag() {
    // 25 minutes into NOW_HOUR, not the 30 the other fixtures use: the tail
    // has to land between 8,400 s and 8,730 s, and it moves in whole hours
    // plus this offset.
    let now = i64::from(NOW_HOUR) * NS_PER_HOUR + 25 * NS_PER_MIN;
    let (engine, tenant_hash, catalog) =
        engine_with_unsealed_hours(&[NOW_HOUR - 3, NOW_HOUR - 2], RequestLimit::Bounded(1)).await;
    // 1 h 50 m into NOW_HOUR - 2: the seal margin has elapsed for NOW_HOUR - 3
    // and not for NOW_HOUR - 2.
    let fold_now = i64::from(NOW_HOUR - 2) * NS_PER_HOUR + 110 * NS_PER_MIN;
    assert_eq!(
        fold_at(&catalog, tenant_hash, fold_now).await,
        Some(NOW_HOUR - 3),
        "test setup: this fold sealed through NOW_HOUR - 3"
    );

    let expected_tail = Duration::from_nanos(
        (now - i64::from(NOW_HOUR - 2) * NS_PER_HOUR)
            .try_into()
            .expect("a positive tail"),
    );
    assert_eq!(
        expected_tail,
        Duration::from_secs(8_700),
        "test setup: the tail is 2 h 25 m"
    );
    assert!(
        expected_tail > healthy_tail_max(SealMargin::REFERENCE) && expected_tail <= threshold(),
        "test setup: {expected_tail:?} must sit between the old 8,400 s bound and 8,730 s"
    );

    // The window reaches back past the sealed segment's event time, so the
    // resolve extracts it and knows the watermark: this is a measured tail,
    // not the fallback case above.
    let window_ns = 3 * NS_PER_HOUR + 25 * NS_PER_MIN - 5 * NS_PER_MIN;
    let err = refuse_range_query_at(&engine, tenant_hash, now, window_ns).await;
    let QueryError::RequestBudgetExceeded { fold_lag, .. } = &err else {
        panic!("expected QueryError::RequestBudgetExceeded, got {err:?}");
    };
    assert_eq!(
        *fold_lag,
        FoldLag::Healthy,
        "8,700 s is inside one fold interval plus one HEAD cache TTL of the healthy tail"
    );
    assert!(!err.to_string().contains(FOLD_LAST_SUCCESS_GAUGE));
}

/// The other side of the tail's lower-bound property: a catalog whose fold is
/// still running seals its old hours, so a wide window over six hours of data
/// resolves a short tail and its refusal blames nothing. The acceptance test's
/// half 1 folds too, at an instant six hours earlier and never again; the fold
/// time is the only difference between the two fixtures, which is the claim
/// decision 6 rests on -- a refusal names fold lag when there is fold lag, not
/// whenever a query is wide.
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
