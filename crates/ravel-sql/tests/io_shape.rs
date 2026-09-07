//! Acceptance tests for issue #1250: a SQL statement's `stats.io_shape`,
//! computed at `SqlExecutor::resolve`'s resolve site
//! (`crates/ravel-sql/src/executor.rs`), matches PromQL's own `QueryIoShape`
//! contract exactly rather than reporting a placeholder.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::sync::Arc;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::io_shape::PlanClass;
use ravel_query::{EngineConfig, LogSegmentFetcher};
use ravel_sql::SqlConfig;
use util::{
    Fixture, SegSpec, SeriesSpec, build_read_cache, publish_logs_segments, request, tenant_id,
};

fn segment(index: i64, metric: &str) -> SegSpec {
    SegSpec::new(
        index,
        1,
        index as u64,
        vec![SeriesSpec::new(
            metric,
            vec![(1, index as f64), (2, (index * 2) as f64)],
        )],
    )
}

/// `unfolded_segments_resolved` and `plan_class` must reflect the real
/// resolve, not a placeholder: every segment published here is fresh (no
/// fold has run), so every one of them resolves through the `Recent`
/// listing path, and a scan with no predicate leaves `segments_pruned == 0`,
/// so the classification is `ExhaustiveScan`.
///
/// Flip-line proof: add a fifth entry to `specs` below (e.g. `segment(5,
/// "m4")`) without touching the two `assert_eq!(.., 4, ..)` calls below, and
/// both fail: `outcome.stats.segments` and
/// `outcome.stats.io_shape.unfolded_segments_resolved` each read 5, not the
/// pinned 4.
#[tokio::test]
async fn a_multi_segment_scan_reports_its_exact_unfolded_count_and_plan_class() {
    let tenant = tenant_id("io-shape-multi-segment");
    let specs = vec![
        segment(1, "m0"),
        segment(2, "m1"),
        segment(3, "m2"),
        segment(4, "m3"),
    ];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect("multi-segment query");

    assert_eq!(
        outcome.stats.segments, 4,
        "sanity: every published segment must resolve"
    );
    assert_eq!(
        outcome.stats.io_shape.unfolded_segments_resolved, 4,
        "every segment is fresh (no fold has run), so all of them must count \
         as unfolded"
    );
    assert_eq!(
        outcome.stats.io_shape.plan_class,
        PlanClass::ExhaustiveScan,
        "no predicate prunes any segment, so the whole listed window is scanned"
    );
}

/// `service_batches` for `ravel-sql` is a deterministic function of the
/// resolved segment count and the configured `target_partitions`
/// (`EngineConfig::sql_partition_count`), clamped to the segment count and
/// floored at 1 (`SqlExecutor::io_shape_for_resolve`): with `inner_fanout ==
/// 1` and `shared_get_permits == u64::MAX`, the model reduces to the busiest
/// partition's own segment count, `ceil(total_segments / partitions)`.
///
/// This fixture publishes 7 segments with `sql_partition_count` set to 3:
/// `partitions = min(3, 7) = 3`, so segments split 3/2/2 round-robin across
/// partitions, and the busiest partition holds `ceil(7 / 3) = 3` segments.
/// `service_batches` must therefore read exactly 3.
///
/// Flip-line proof: add three more entries to `specs` (10 segments total)
/// without touching `TARGET_PARTITIONS` or the `service_batches, 3`
/// assertion below: `ceil(10 / 3) = 4`, so the pinned `3` fails.
#[tokio::test]
async fn service_batches_matches_the_busiest_partition_s_segment_count() {
    const TARGET_PARTITIONS: usize = 3;
    let tenant = tenant_id("io-shape-service-batches");
    let specs = vec![
        segment(1, "m0"),
        segment(2, "m1"),
        segment(3, "m2"),
        segment(4, "m3"),
        segment(5, "m4"),
        segment(6, "m5"),
        segment(7, "m6"),
    ];
    let config = SqlConfig {
        engine: EngineConfig {
            sql_partition_count: Some(TARGET_PARTITIONS),
            ..EngineConfig::default()
        },
        ..SqlConfig::default()
    };
    let store: std::sync::Arc<dyn ravel_object_store::ObjectStoreBackend> =
        std::sync::Arc::new(ravel_object_store::memory::MemoryStore::new());
    let fixture = Fixture::build(store, &[(&tenant, &specs)], config, 1 << 30).await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect("multi-segment query");

    assert_eq!(outcome.stats.segments, 7, "sanity: all 7 resolve");
    assert_eq!(
        outcome.stats.io_shape.service_batches, 3,
        "7 segments over 3 partitions: the busiest partition holds ceil(7/3) = 3"
    );
}

/// `shared_get_permits` must be the real, resolved
/// `EngineConfig::store_get_concurrency` -- the size of the one `GetLimiter`
/// the server shares across every fetcher it builds -- not `u64::MAX`
/// (issue #1250 review fix, finding 1). `u64::MAX` silently drops the
/// concurrency clamp whenever the configured GET permits are fewer than the
/// configured partitions, which is exactly the case this fixture sets up:
/// `sql_partition_count = 8`, `store_get_concurrency = 3`.
///
/// This fixture publishes 8 segments with `partitions = min(8, 8) = 8`, so
/// `segments_per_plan = ceil(8/8) = 1`, one wave (`distinct_plans ==
/// outer_fanout == 8`), `active = 8`.
///
/// Under the WRONG `u64::MAX` model: `capacity = min(1 * 8, u64::MAX) = 8`,
/// `service_batches = ceil(1 * 8 / 8) = 1`.
///
/// Under the FIXED model: `capacity = min(1 * 8, 3) = 3`,
/// `service_batches = ceil(1 * 8 / 3) = 3`.
///
/// Flip-line proof: with `shared_get_permits` reverted to `u64::MAX`, this
/// assertion (`3`) fails and reads `1` instead.
#[tokio::test]
async fn shared_get_permits_reflects_store_get_concurrency_not_u64_max() {
    const TARGET_PARTITIONS: usize = 8;
    const STORE_GET_CONCURRENCY: usize = 3;
    let tenant = tenant_id("io-shape-shared-get-permits");
    let specs = vec![
        segment(1, "m0"),
        segment(2, "m1"),
        segment(3, "m2"),
        segment(4, "m3"),
        segment(5, "m4"),
        segment(6, "m5"),
        segment(7, "m6"),
        segment(8, "m7"),
    ];
    let config = SqlConfig {
        engine: EngineConfig {
            sql_partition_count: Some(TARGET_PARTITIONS),
            store_get_concurrency: Some(STORE_GET_CONCURRENCY),
            ..EngineConfig::default()
        },
        ..SqlConfig::default()
    };
    let store: std::sync::Arc<dyn ravel_object_store::ObjectStoreBackend> =
        std::sync::Arc::new(ravel_object_store::memory::MemoryStore::new());
    let fixture = Fixture::build(store, &[(&tenant, &specs)], config, 1 << 30).await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, value FROM samples"))
        .await
        .expect("multi-segment query");

    assert_eq!(outcome.stats.segments, 8, "sanity: all 8 resolve");
    assert_eq!(
        outcome.stats.io_shape.service_batches, 3,
        "8 segments, 8 partitions, but only 3 shared GET permits: \
         ceil(8/min(8,3)) = 3, not the u64::MAX model's ceil(8/8) = 1"
    );
}

/// Uncached Logs/Alerts/Audit is segment-granular, matching `RsegScanExec`
/// (issue #1250 review fix, finding 2a) -- but the old model charged only
/// the scan phase and missed `compute_plan_counts`'s own uncounted
/// `buffer_unordered(target_partitions)` plan-probe pass (issue #691), which
/// gates every scan partition and never overlaps with it.
///
/// This fixture publishes 5 log segments with `sql_partition_count = 2` and
/// the default `store_get_concurrency` (8):
///
/// Scan phase (unchanged by this fix, both models agree here):
/// `partitions = min(2, 5) = 2`, `segments_per_plan = ceil(5/2) = 3`, one
/// wave (`distinct_plans == outer_fanout == partitions == 2`), `active = 2`,
/// `capacity = min(1*2, 8) = 2`, `service_batches = ceil(3*2/2) = 3`.
///
/// Plan phase (the old model's omission): one probe per segment at
/// `buffer_unordered(plan_partitions.min(shared_get_permits)) =
/// buffer_unordered(min(2,8)=2)`, so `service_batches = ceil(5/2) = 3`.
///
/// Total, fixed model: `3 (plan) + 3 (scan) = 6`. Old (Metrics-shaped, no
/// plan-phase term) model: `3`.
///
/// Flip-line proof: with the plan-phase `saturating_add` removed (or
/// `plan_phase_batches` fixed at 0), this assertion (`6`) fails and reads `3`
/// instead.
#[tokio::test]
async fn uncached_logs_scan_adds_the_plan_phase_to_the_scan_phase() {
    const TARGET_PARTITIONS: usize = 2;
    const LOG_SEGMENTS: usize = 5;
    let tenant = tenant_id("io-shape-logs-uncached");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_logs_segments(store.as_ref(), &tenant, LOG_SEGMENTS).await;

    let config = SqlConfig {
        engine: EngineConfig {
            sql_partition_count: Some(TARGET_PARTITIONS),
            ..EngineConfig::default()
        },
        ..SqlConfig::default()
    };
    let fixture = Fixture::build(Arc::clone(&store), &[], config, 1 << 30).await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, body FROM logs"))
        .await
        .expect("logs query");

    assert_eq!(
        outcome.stats.segments, LOG_SEGMENTS,
        "sanity: all 5 resolve"
    );
    assert_eq!(
        outcome.stats.io_shape.service_batches, 6,
        "uncached logs: plan phase ceil(5/min(2,8))=3 plus scan phase \
         ceil(3*2/2)=3, total 6, not the old Metrics-shaped model's 3 \
         (scan phase alone)"
    );
}

/// Cache-wired Logs/Alerts/Audit block-strides across `min(target_partitions,
/// total_block_count)` partitions, unclamped by segment count -- the exact
/// opposite of the uncached shape above (issue #1250 review fix, finding
/// 2b). The old model applied the uncached (`RsegScanExec`-style) clamp
/// uniformly, which badly undercounts here since a segment count far below
/// the partition count no longer bounds the outer width once the cache is
/// wired in.
///
/// This fixture publishes 3 log segments with `sql_partition_count = 8` and
/// `store_get_concurrency = 50`, and wires a read cache into the
/// `LogSegmentFetcher` so `has_cache() == true`.
///
/// Fixed model: `total_block_count` is not resolvable at resolve time
/// without extra I/O (`SegmentRef` carries no block-count field), so
/// `segments_per_plan` is upper-bounded at `total_segments = 3` and
/// `partitions` at the configured `8` (both legitimate, never-exceeded
/// bounds since there are only 3 segments and DataFusion never plans more
/// than 8 partitions). Plan phase: `service_batches(3, min(8,50)=8) =
/// ceil(3/8) = 1`. Scan phase: one wave (`distinct_plans == outer_fanout ==
/// 8`), `active = 8`, `capacity = min(1*8, 50) = 8`,
/// `service_batches = ceil(3*8/8) = 3`. Total: `1 + 3 = 4`.
///
/// Old (uncached-shaped) model: `partitions = min(8, 3) = 3` (wrongly
/// clamped by segment count), `segments_per_plan = ceil(3/3) = 1`, one wave,
/// `active = 3`, `capacity = min(3,50) = 3`, `service_batches = ceil(1*3/3)
/// = 1`, no plan-phase term. Old total: `1`.
///
/// Flip-line proof: with the `has_cache()` branch removed (falling through
/// to the uncached clamp unconditionally), this assertion (`4`) fails and
/// reads `1` instead.
#[tokio::test]
async fn cached_logs_scan_is_not_clamped_by_segment_count() {
    const TARGET_PARTITIONS: usize = 8;
    const STORE_GET_CONCURRENCY: usize = 50;
    const LOG_SEGMENTS: usize = 3;
    let tenant = tenant_id("io-shape-logs-cached");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_logs_segments(store.as_ref(), &tenant, LOG_SEGMENTS).await;

    let cache = build_read_cache(64 << 20);
    let log_fetcher = LogSegmentFetcher::new(Arc::clone(&store)).with_cache(cache);
    assert!(
        log_fetcher.has_cache(),
        "this fixture must exercise the cache-wired path"
    );

    let config = SqlConfig {
        engine: EngineConfig {
            sql_partition_count: Some(TARGET_PARTITIONS),
            store_get_concurrency: Some(STORE_GET_CONCURRENCY),
            ..EngineConfig::default()
        },
        ..SqlConfig::default()
    };
    let fixture =
        Fixture::build_with_log_fetcher(Arc::clone(&store), &[], config, 1 << 30, log_fetcher)
            .await;

    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT ts, body FROM logs"))
        .await
        .expect("logs query");

    assert_eq!(
        outcome.stats.segments, LOG_SEGMENTS,
        "sanity: all 3 resolve"
    );
    assert_eq!(
        outcome.stats.io_shape.service_batches, 4,
        "cached logs: plan phase ceil(3/min(8,50))=1 plus scan phase \
         ceil(3*8/8)=3, total 4, not the old segment-clamped model's 1"
    );
}
