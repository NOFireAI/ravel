//! Issue #1367: the SQL executor's per-phase I/O accounting
//! (`SqlOutcome::phase_accounting`) over a real fixture with a known object
//! layout, and its JSON rendering (`ravel_sql::stats_json`).
//!
//! Each published segment is small enough (well under
//! `DEFAULT_WHOLE_OBJECT_THRESHOLD`, 512 KiB) that `SegmentFetcher` reads it
//! whole in one GET issued from inside `open_segment`, which is charged to
//! the *plan* phase (`crates/ravel-query/src/fetcher.rs`'s `open_segment`
//! takes `accounting.plan()`), not `scan`: the footer, catalog, and pages all
//! come from that one buffer, so `probe`/`scan` make no further S3 calls for
//! a segment this size. That is the concrete shape these tests pin.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::io_shape::PlanClass;
use ravel_query::phase_accounting::QueryPhase;
use ravel_query::{GetLimiter, LogSegmentFetcher, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_V7};
use ravel_sql::{SqlConfig, SqlExecutor, SqlRequest};
use ravel_types::accounting::AccountedOp;
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange,
};
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// First instant at which `hour` is sealed (mirrors `admission_parity.rs`).
fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn tenant(id: &str) -> TenantId {
    TenantId::new(id.to_string())
}

/// Writes one real RSEG v7 segment carrying a single sample and publishes its
/// commit record, returning the object's exact on-disk byte length so the
/// tests below can pin the wire-byte figure against it exactly rather than
/// against `> 0`.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant_id: &TenantId,
    writer_seq: u64,
    ingest_hour_bucket: u32,
    metric: &str,
    ts_ns: i64,
    value: f64,
) -> u64 {
    let tenant_hash = tenant_id.hash();
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
        value: metric.to_string(),
    }])
    .expect("valid labels");
    let series_id = SeriesId::compute(tenant_id, metric, &label_set).expect("series id");
    let input = SeriesInput {
        series_id,
        labels: label_set,
        samples: vec![Sample { ts_ns, value }],
    };
    let written = SegmentWriter::write(vec![input], identity, bounds).expect("write v7 segment");
    let object_len = written.bytes.len() as u64;

    let new_record = NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq,
        object_size: object_len,
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
    };
    let rec = record::build(new_record).expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, written.bytes)
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    object_len
}

fn sql_request(sql: &str, window: TimeRange, now_ns: i64) -> SqlRequest {
    SqlRequest {
        sql: sql.to_string(),
        window,
        min_tokens: Vec::new(),
        now_ns,
        deadline: Duration::from_secs(30),
        row_window: false,
        max_rows: None,
        budgets: None,
    }
}

/// Fixture: two never-folded (`Recent`, exempt from admission caps) segments
/// under one tenant and hour, one per metric name (`a`, `b`), each holding a
/// single sample. Returns the executor, the tenant hash, the two segments'
/// exact object byte lengths, and the query window/now.
async fn fixture() -> (SqlExecutor, TenantHash, u64, u64, TimeRange, i64) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let hour = 9_501u32;
    let now = now_at_seal(hour);

    let len_a = publish_segment(
        store.as_ref(),
        &tid,
        1,
        hour,
        "a",
        i64::from(hour) * NS_PER_HOUR + 10 * 60 * NS_PER_SEC,
        1.0,
    )
    .await;
    let len_b = publish_segment(
        store.as_ref(),
        &tid,
        2,
        hour,
        "b",
        i64::from(hour) * NS_PER_HOUR + 20 * 60 * NS_PER_SEC,
        2.0,
    )
    .await;

    let cat = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    cat.fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold seals both segments, building the postings index prune needs");
    let executor = SqlExecutor::new(
        cat,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        SqlConfig::default(),
        1 << 30,
    );
    let window = TimeRange {
        start_ns: i64::from(hour) * NS_PER_HOUR,
        end_ns: i64::from(hour + 1) * NS_PER_HOUR,
    };
    (executor, th, len_a, len_b, window, now)
}

/// Like `fixture()` (same two segments, same tenant/hour/window), but with
/// `sql_partition_count` overridden to `partition_count` and the metrics
/// `SegmentFetcher`'s shared `GetLimiter` overridden to `get_limiter_permits`
/// permits, so the two bounds `sql_io_shape` mins together can be made to
/// differ: `get_limiter_permits < partition_count` makes the limiter, not the
/// partition count, the one that binds `service_batches`.
async fn fixture_with_low_get_limiter(
    partition_count: usize,
    get_limiter_permits: usize,
) -> (SqlExecutor, TenantHash, TimeRange, i64) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let th = tid.hash();
    let hour = 9_501u32;
    let now = now_at_seal(hour);

    publish_segment(
        store.as_ref(),
        &tid,
        1,
        hour,
        "a",
        i64::from(hour) * NS_PER_HOUR + 10 * 60 * NS_PER_SEC,
        1.0,
    )
    .await;
    publish_segment(
        store.as_ref(),
        &tid,
        2,
        hour,
        "b",
        i64::from(hour) * NS_PER_HOUR + 20 * 60 * NS_PER_SEC,
        2.0,
    )
    .await;

    let cat = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
    cat.fold(&th, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold seals both segments, building the postings index prune needs");

    let get_limiter = Arc::new(GetLimiter::new(get_limiter_permits).expect("valid permit count"));
    let fetcher = SegmentFetcher::new(store.clone()).with_get_limiter(get_limiter);

    let mut config = SqlConfig::default();
    config.engine.sql_partition_count = Some(partition_count);

    let executor = SqlExecutor::new(
        cat,
        fetcher,
        LogSegmentFetcher::new(store.clone()),
        ravel_sql::SpanSegmentFetcher::new(store.clone()),
        config,
        1 << 30,
    );
    let window = TimeRange {
        start_ns: i64::from(hour) * NS_PER_HOUR,
        end_ns: i64::from(hour + 1) * NS_PER_HOUR,
    };
    (executor, th, window, now)
}

const PRUNED_SQL: &str = "SELECT ts, value FROM samples WHERE label(labels, '__name__') = 'a'";
const UNPRUNED_SQL: &str =
    "SELECT ts, value FROM samples WHERE label_match(labels, '__name__', 'a|b')";

/// A query matching only the `a` segment reports the plan phase's exact
/// request count (1) and exact wire-byte figure (`len_a`, the segment's real
/// on-disk size): the segment is small enough to be read whole from inside
/// `open_segment`, which is charged to `plan`, not `scan`/`probe` (both stay
/// at 0 GETs, since the whole object is already in hand by then).
#[tokio::test]
async fn plan_phase_reports_exact_figures_for_one_matched_segment() {
    let (executor, th, len_a, _len_b, window, now) = fixture().await;
    let outcome = executor
        .execute(th, &sql_request(PRUNED_SQL, window, now))
        .await
        .expect("query over the matched 'a' segment");
    assert_eq!(outcome.stats.segments, 1, "only the 'a' segment matches");

    let plan = outcome.phase_accounting.phase(QueryPhase::Plan);
    assert_eq!(plan.s3_requests(AccountedOp::Get), 1);
    assert_eq!(plan.s3_bytes(AccountedOp::Get), len_a);

    let probe = outcome.phase_accounting.phase(QueryPhase::Probe);
    let scan = outcome.phase_accounting.phase(QueryPhase::Scan);
    assert_eq!(probe.s3_requests(AccountedOp::Get), 0);
    assert_eq!(scan.s3_requests(AccountedOp::Get), 0);
}

/// The same fixture, queried with a regex matcher that admits both segments:
/// the plan phase now reports exactly 2 GETs and `len_a + len_b` wire bytes --
/// two concrete numbers, contrasted against the single-segment query above's
/// 1 GET / `len_a` bytes, not an inequality against zero.
#[tokio::test]
async fn plan_phase_prunes_to_fewer_requests_than_the_unpruned_query() {
    let (executor, th, len_a, len_b, window, now) = fixture().await;

    let pruned = executor
        .execute(th, &sql_request(PRUNED_SQL, window, now))
        .await
        .expect("pruned query");
    let unpruned = executor
        .execute(th, &sql_request(UNPRUNED_SQL, window, now))
        .await
        .expect("unpruned query");

    assert_eq!(unpruned.stats.segments, 2, "both segments match a|b");

    let pruned_plan = pruned.phase_accounting.phase(QueryPhase::Plan);
    let unpruned_plan = unpruned.phase_accounting.phase(QueryPhase::Plan);
    assert_eq!(pruned_plan.s3_requests(AccountedOp::Get), 1);
    assert_eq!(unpruned_plan.s3_requests(AccountedOp::Get), 2);
    assert_eq!(pruned_plan.s3_bytes(AccountedOp::Get), len_a);
    assert_eq!(unpruned_plan.s3_bytes(AccountedOp::Get), len_a + len_b);
}

/// Per-phase requests and bytes, summed over all four phases of a real
/// executed query, equal the pooled (`SqlOutcome::accounting`) total exactly:
/// the same invariant `stats_json`'s unit test pins against a synthetic
/// `PhaseAccounting`, now pinned against the executor's actual output.
#[tokio::test]
async fn per_phase_figures_sum_to_the_pooled_total_on_a_real_query() {
    let (executor, th, _len_a, _len_b, window, now) = fixture().await;
    let outcome = executor
        .execute(th, &sql_request(UNPRUNED_SQL, window, now))
        .await
        .expect("unpruned query");

    let summed_requests: u64 = QueryPhase::ALL
        .iter()
        .map(|p| outcome.phase_accounting.phase(*p).total_s3_requests())
        .sum();
    let summed_bytes: u64 = QueryPhase::ALL
        .iter()
        .map(|p| outcome.phase_accounting.phase(*p).total_s3_bytes())
        .sum();

    assert_eq!(summed_requests, outcome.accounting.total_s3_requests());
    assert_eq!(summed_bytes, outcome.accounting.total_s3_bytes());
}

/// The JSON rendering names each of the four phases exactly once, in
/// `resolve`/`plan`/`probe`/`scan` order, on a real executed query's
/// accounting -- not just on a synthetic `PhaseAccounting` as in
/// `stats_json`'s own unit test.
#[tokio::test]
async fn wire_rendering_names_each_phase_exactly_once_on_a_real_query() {
    let (executor, th, _len_a, _len_b, window, now) = fixture().await;
    let outcome = executor
        .execute(th, &sql_request(PRUNED_SQL, window, now))
        .await
        .expect("pruned query");

    let entries = ravel_sql::stats_json::phase_costs_json(&outcome.phase_accounting);
    let names: Vec<&str> = entries
        .iter()
        .map(|e| e["phase"].as_str().expect("phase is a string"))
        .collect();
    assert_eq!(names, vec!["resolve", "plan", "probe", "scan"]);
}

/// `SqlOutcome::io_shape` on this fixture's two queries, pinned by exact
/// value rather than `> 0`. Both segments are tiny (well under
/// `DEFAULT_WHOLE_OBJECT_THRESHOLD`), so `dependency_depth` is 1 for either
/// query. `service_batches` is `ceil(segments / min(sql_partition_count,
/// shared_get_permits))`; `SqlConfig::default()` resolves
/// `sql_partition_count()` to `DEFAULT_FETCH_CONCURRENCY` (8) and
/// `SegmentFetcher::new`'s `GetLimiter` defaults to `DEFAULT_MAX_CONCURRENT_GETS`
/// (16), so the bound is `min(8, 16) = 8` for both queries here, and
/// `ceil(1 / 8) = ceil(2 / 8) = 1` either way -- this pins the inert case,
/// where `sql_partition_count` alone already gives the same answer as the
/// min. The case where the `.min(shared_get_permits)` bound actually binds
/// (a `GetLimiter` narrower than `sql_partition_count`) is pinned separately,
/// by `service_batches_is_bound_by_shared_get_limiter_permits_not_partition_count`
/// below. `plan_class` is the pair this test exists to pin post-fix: the
/// `Metrics` target still tells a pruned fetch from a full scan via
/// `Snapshot::segments_pruned`, so the pruned single-segment query classifies
/// as `SelectiveIndexed` (one of the two window segments was excluded) and
/// the unpruned two-segment query as `ExhaustiveScan` (no segment was
/// excluded) -- neither is `Unclassified`, because that fix only changed the
/// `Logs`/`Spans`/`Alerts`/`Audit` targets, not `Metrics`.
#[tokio::test]
async fn io_shape_pins_dependency_depth_service_batches_and_plan_class() {
    let (executor, th, _len_a, _len_b, window, now) = fixture().await;

    let pruned = executor
        .execute(th, &sql_request(PRUNED_SQL, window, now))
        .await
        .expect("pruned query");
    assert_eq!(pruned.io_shape.dependency_depth, 1);
    assert_eq!(pruned.io_shape.service_batches, 1);
    assert_eq!(pruned.io_shape.plan_class, PlanClass::SelectiveIndexed);

    let unpruned = executor
        .execute(th, &sql_request(UNPRUNED_SQL, window, now))
        .await
        .expect("unpruned query");
    assert_eq!(unpruned.io_shape.dependency_depth, 1);
    assert_eq!(unpruned.io_shape.service_batches, 1);
    assert_eq!(unpruned.io_shape.plan_class, PlanClass::ExhaustiveScan);
}

/// `sql_io_shape` computes `service_batches(segments, min(sql_partition_count,
/// shared_get_permits))`. `io_shape_pins_dependency_depth_service_batches_and_plan_class`
/// above never exercises the `.min` doing any work: its fixture's
/// `sql_partition_count` (8) is already below the default `GetLimiter`'s 16
/// permits, so `min(8, 16) == 8` and the un-minned `sql_partition_count`
/// alone would report the same `service_batches` value. This test makes the
/// two bounds differ: `sql_partition_count` is 4 and the shared `GetLimiter`
/// has only 1 permit, so `min(4, 1) == 1` is the binding bound over the
/// fixture's 2 matched segments -- `ceil(2 / 1) = 2`, where
/// `sql_partition_count` alone would give `ceil(2 / 4) = 1`.
#[tokio::test]
async fn service_batches_is_bound_by_shared_get_limiter_permits_not_partition_count() {
    let (executor, th, window, now) = fixture_with_low_get_limiter(4, 1).await;

    let unpruned = executor
        .execute(th, &sql_request(UNPRUNED_SQL, window, now))
        .await
        .expect("unpruned query");
    assert_eq!(unpruned.stats.segments, 2, "both segments match a|b");
    assert_eq!(unpruned.io_shape.service_batches, 2);
}
