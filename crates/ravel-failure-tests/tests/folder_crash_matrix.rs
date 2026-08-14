//! Folder crash matrix and the fallback-not-failure guarantee: every row
//! exercises a folder/index failure end to end through `QueryEngine`, never
//! `Catalog` in isolation (that is already covered inside
//! `crates/ravel-catalog`'s own unit tests). The point of this file is the
//! caller-visible contract: a degraded or absent index changes cost, never
//! correctness, except for the one documented row (clock skew past the seal
//! margin) that names a narrow, repairable visibility gap.

#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use common::{
    NS_PER_HOUR, NS_PER_SEC, catalog, engine, expect_vector, publish_segment, series_input, tenant,
};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

/// Nanosecond timestamp one second before `hour`'s end: always inside
/// `hour`'s bucket regardless of rounding.
fn hour_end_ns(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR
}

/// A `now_ns` that safely seals `hour`: default margins sum to 1h20m, not a
/// whole number of hours, so 3h of padding is used everywhere in this suite to
/// clear the boundary regardless of which hour the margin arithmetic lands in.
fn now_sealing(hour: u32) -> i64 {
    hour_end_ns(hour) + 3 * NS_PER_HOUR
}

/// HEAD object key (docs/catalog-and-mvcc.md, frozen format). Duplicated
/// here rather than imported: `ravel-catalog` only exposes the equivalent
/// helper as `pub(crate)` (`services/ravel-cli/src/catalog.rs` duplicates it
/// the same way, for the same reason).
fn head_key(tenant_hash: &TenantHash) -> String {
    format!(
        "t/{}/catalog/{}/HEAD",
        tenant_hash.to_hex(),
        Signal::Metrics.key_prefix()
    )
}

fn folder_catalog(store: Arc<dyn ObjectStoreBackend>) -> Catalog {
    Catalog::new(
        store,
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("folder catalog")
}

/// Row: "folder down D hours" -> listed suffix widens, never loses data.
/// Three sealed hours accumulate with no folder ever running; every sample
/// resolves through pure commit-record listing. A folder then catches up in one
/// shot, and the same queries must return byte-identical results, proving
/// the index changed only the request shape, not the answer.
#[tokio::test]
async fn folder_down_for_hours_never_loses_data_only_widens_listing() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let hours = [10u32, 11, 12];
    let mut timestamps = Vec::new();
    for hour in hours {
        let ts = hour_end_ns(hour) - NS_PER_SEC;
        timestamps.push(ts);
        let series = vec![series_input(
            &tid,
            "folder_down_metric",
            &[],
            &[(ts, f64::from(hour))],
        )];
        publish_segment(
            store.as_ref(),
            tid.hash(),
            0,
            Uuid::new_v4(),
            1,
            1,
            hour,
            ts,
            series,
        )
        .await;
    }
    let now_ns = now_sealing(12);

    let before = engine(Arc::clone(&store), catalog(Arc::clone(&store), 1));
    for (hour, ts) in hours.iter().zip(&timestamps) {
        let result = expect_vector(
            before
                .instant(
                    tid.hash(),
                    "folder_down_metric",
                    ts / 1_000_000,
                    &[],
                    now_ns,
                    Duration::from_secs(5),
                )
                .await
                .expect("query with no folder ever run must still succeed via listing"),
        );
        assert_eq!(
            result.len(),
            1,
            "hour {hour} must resolve via listing fallback"
        );
        assert_eq!(result[0].value, f64::from(*hour));
    }

    let folder = folder_catalog(Arc::clone(&store));
    folder
        .fold(&tid.hash(), Signal::Metrics, Uuid::new_v4(), now_ns, &[])
        .await
        .expect("fold catches up in one shot");

    let after = engine(Arc::clone(&store), catalog(Arc::clone(&store), 1));
    for (hour, ts) in hours.iter().zip(&timestamps) {
        let result = expect_vector(
            after
                .instant(
                    tid.hash(),
                    "folder_down_metric",
                    ts / 1_000_000,
                    &[],
                    now_ns,
                    Duration::from_secs(5),
                )
                .await
                .expect("query after the folder catches up must still succeed"),
        );
        assert_eq!(
            result.len(),
            1,
            "hour {hour} must still resolve once indexed"
        );
        assert_eq!(result[0].value, f64::from(*hour));
    }
}

/// Row: "HEAD missing/corrupt" -> full commit-record listing cost, never an
/// error. A HEAD overwritten with garbage bytes must degrade
/// `resolve_snapshot_window` to `None`, not surface `CatalogError` up through
/// the query engine.
#[tokio::test]
async fn corrupt_head_falls_back_to_listing_never_to_an_error() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let hour = 10u32;
    let ts = hour_end_ns(hour) - NS_PER_SEC;
    let series = vec![series_input(
        &tid,
        "corrupt_head_metric",
        &[],
        &[(ts, 42.0)],
    )];
    publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour,
        ts,
        series,
    )
    .await;

    let now_ns = now_sealing(hour);
    folder_catalog(Arc::clone(&store))
        .fold(&tid.hash(), Signal::Metrics, Uuid::new_v4(), now_ns, &[])
        .await
        .expect("fold seals hour 10");

    store
        .put(
            &head_key(&tid.hash()),
            Bytes::from_static(b"not a valid head"),
            PutOptions::default(),
        )
        .await
        .expect("overwrite HEAD with garbage");

    let eng = engine(Arc::clone(&store), catalog(Arc::clone(&store), 1));
    let result = expect_vector(
        eng.instant(
            tid.hash(),
            "corrupt_head_metric",
            ts / 1_000_000,
            &[],
            now_ns,
            Duration::from_secs(5),
        )
        .await
        .expect("a corrupt HEAD must degrade to listing, not fail the query"),
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 42.0);
}

/// Row: "part missing/corrupt" -> full commit-record listing cost after one HEAD
/// re-read. Deleting the part HEAD names (a GC race) must trigger exactly one
/// bypass-cache HEAD re-read before falling back, never an error and never
/// missing data.
#[tokio::test]
async fn missing_snapshot_part_falls_back_to_listing_after_one_head_reread() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let hour = 10u32;
    let ts = hour_end_ns(hour) - NS_PER_SEC;
    let series = vec![series_input(&tid, "missing_part_metric", &[], &[(ts, 7.0)])];
    publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour,
        ts,
        series,
    )
    .await;

    let now_ns = now_sealing(hour);
    folder_catalog(Arc::clone(&store))
        .fold(&tid.hash(), Signal::Metrics, Uuid::new_v4(), now_ns, &[])
        .await
        .expect("fold seals hour 10");

    let head_bytes = store
        .get(&head_key(&tid.hash()), GetRange::Full)
        .await
        .expect("read head")
        .data;
    let head = ravel_catalog::decode_head(&head_bytes).expect("decode head");
    let part_key = head.parts[0].key.clone();
    store
        .delete(&part_key)
        .await
        .expect("delete the part, simulating a GC race");

    let eng = engine(Arc::clone(&store), catalog(Arc::clone(&store), 1));
    let result = expect_vector(
        eng.instant(
            tid.hash(),
            "missing_part_metric",
            ts / 1_000_000,
            &[],
            now_ns,
            Duration::from_secs(5),
        )
        .await
        .expect("a missing snapshot part must degrade to listing, not fail the query"),
    );
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 7.0);
}

/// Row: "CAS races between folders" -> none; one winner, the loser's work
/// is idempotent. `crates/ravel-catalog` already unit-tests that exactly
/// one racer advances; this asserts the caller-visible half of that
/// contract instead: post-race query results contain the entry exactly
/// once, whichever folder won.
#[tokio::test]
async fn concurrent_folders_race_head_cas_without_losing_or_duplicating_data() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let hour = 10u32;
    let ts = hour_end_ns(hour) - NS_PER_SEC;
    let series = vec![series_input(&tid, "cas_race_metric", &[], &[(ts, 5.0)])];
    publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour,
        ts,
        series,
    )
    .await;

    let now_ns = now_sealing(hour);
    let folder_a = folder_catalog(Arc::clone(&store));
    let folder_b = folder_catalog(Arc::clone(&store));
    let tenant_hash = tid.hash();
    let (result_a, result_b) = tokio::join!(
        folder_a.fold(&tenant_hash, Signal::Metrics, Uuid::new_v4(), now_ns, &[]),
        folder_b.fold(&tenant_hash, Signal::Metrics, Uuid::new_v4(), now_ns, &[]),
    );
    result_a.expect("folder a's fold");
    result_b.expect("folder b's fold");

    let eng = engine(Arc::clone(&store), catalog(Arc::clone(&store), 1));
    let result = expect_vector(
        eng.instant(
            tid.hash(),
            "cas_race_metric",
            ts / 1_000_000,
            &[],
            now_ns,
            Duration::from_secs(5),
        )
        .await
        .expect("query after a CAS race"),
    );
    assert_eq!(
        result.len(),
        1,
        "the losing folder's idempotent retry must not duplicate the entry"
    );
    assert_eq!(result[0].value, 5.0);
}

/// Row: "stale HEAD cache (TTL)" -> up to TTL of extra listed suffix,
/// never missing data. One `QueryEngine` (one `Catalog`, one warm
/// `head_cache` entry) queries before and after a second folder advances
/// the watermark; the second query's `now_ns` is deliberately kept close to
/// the first so the cache entry (default TTL 30s) is still valid, proving
/// the new hour is found via the ordinary listing suffix above the stale
/// cached watermark rather than through the cache noticing anything.
#[tokio::test]
async fn stale_head_cache_widens_listing_but_never_misses_new_data() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let hour_a = 10u32;
    let ts_a = hour_end_ns(hour_a) - NS_PER_SEC;
    let series_a = vec![series_input(
        &tid,
        "stale_cache_metric",
        &[],
        &[(ts_a, 1.0)],
    )];
    publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour_a,
        ts_a,
        series_a,
    )
    .await;

    let now_1 = now_sealing(hour_a);
    folder_catalog(Arc::clone(&store))
        .fold(&tid.hash(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
        .await
        .expect("first fold seals hour_a");

    // One engine, queried twice: its internal Catalog's head_cache entry
    // from the first query stays warm for the second.
    let eng = engine(Arc::clone(&store), catalog(Arc::clone(&store), 1));
    let first = expect_vector(
        eng.instant(
            tid.hash(),
            "stale_cache_metric",
            ts_a / 1_000_000,
            &[],
            now_1,
            Duration::from_secs(5),
        )
        .await
        .expect("first query populates the head_cache"),
    );
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].value, 1.0);

    // `now_sealing(hour_a)` seals through hour_a + 1 (the 1h20m default
    // margin means "3h past hour_a's end" already covers part of the next
    // hour too), so the first fold's watermark_hour is hour_a + 1, not
    // hour_a. hour_b has to clear that watermark, or the first snapshot
    // would already (truthfully, at fold time) claim full coverage of it
    // and this test would degenerate into the wrongly-sealed-bucket row
    // instead of exercising plain cache staleness.
    let hour_b = hour_a + 3;
    let ts_b = hour_end_ns(hour_b) - NS_PER_SEC;
    let series_b = vec![series_input(
        &tid,
        "stale_cache_metric",
        &[],
        &[(ts_b, 2.0)],
    )];
    publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour_b,
        ts_b,
        series_b,
    )
    .await;
    folder_catalog(Arc::clone(&store))
        .fold(
            &tid.hash(),
            Signal::Metrics,
            Uuid::new_v4(),
            now_sealing(hour_b),
            &[],
        )
        .await
        .expect("second fold advances the real HEAD past hour_b");

    // Reuse now_1 for the second query: the cache delta is zero, well
    // under the default 30s TTL, so `eng` still trusts its stale
    // watermark_hour = hour_a + 1. now_1 is already past hour_b's end,
    // so hour_b sits inside the ordinary listing suffix above that stale
    // watermark and must still be found.
    let second = expect_vector(
        eng.instant(
            tid.hash(),
            "stale_cache_metric",
            ts_b / 1_000_000,
            &[],
            now_1,
            Duration::from_secs(5),
        )
        .await
        .expect("second query, head_cache still warm"),
    );
    assert_eq!(
        second.len(),
        1,
        "data above a stale cached watermark must still be found via listing"
    );
    assert_eq!(second[0].value, 2.0);
}

/// Row: "folder clock fast beyond fold_safety_margin" -> commits published
/// into a wrongly-sealed bucket are invisible to non-token queries until
/// repair; the token path is unaffected. This is the same failure class as
/// writer skew beyond clock_skew_allowance: a writer's flush lands in an hour
/// the folder already folded as sealed. Repair: delete HEAD (forcing the next
/// fold onto its existing absent-HEAD full-rebuild path, the "HEAD
/// missing/corrupt" row) and fold again.
#[tokio::test]
async fn commit_in_wrongly_sealed_bucket_is_invisible_until_head_rebuild_repairs_it() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let hour = 10u32;
    let on_time_ts = hour_end_ns(hour) - 2 * NS_PER_SEC;
    let on_time_series = vec![series_input(
        &tid,
        "clock_skew_metric",
        &[],
        &[(on_time_ts, 1.0)],
    )];
    publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour,
        on_time_ts,
        on_time_series,
    )
    .await;

    let now_1 = now_sealing(hour);
    folder_catalog(Arc::clone(&store))
        .fold(&tid.hash(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
        .await
        .expect("fold seals hour 10 with only the on-time sample");

    // A late writer's flush for the same hour, arriving after the folder
    // already considered it sealed.
    let late_ts = hour_end_ns(hour) - NS_PER_SEC;
    let late_series = vec![series_input(
        &tid,
        "clock_skew_metric",
        &[],
        &[(late_ts, 2.0)],
    )];
    let (late_token, _) = publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour,
        late_ts,
        late_series,
    )
    .await;

    let eng = engine(Arc::clone(&store), catalog(Arc::clone(&store), 1));
    let without_token = expect_vector(
        eng.instant(
            tid.hash(),
            "clock_skew_metric",
            late_ts / 1_000_000,
            &[],
            now_1,
            Duration::from_secs(5),
        )
        .await
        .expect("query without the late writer's token"),
    );
    // on_time_ts and late_ts are 1s apart, well inside PromQL's default 5m
    // lookback, so a masked late sample does not show up as an empty
    // vector: `pick_sample` falls through to the on-time sample instead.
    // The value, not the length, is what proves the late sample stayed
    // invisible to the non-token query.
    assert_eq!(
        without_token.len(),
        1,
        "lookback must still find the on-time sample"
    );
    assert_eq!(
        without_token[0].value, 1.0,
        "a commit published into an already-sealed bucket must stay invisible \
         to a non-token query: only the on-time sample, never the late one, \
         may satisfy it"
    );

    let with_token = expect_vector(
        eng.instant(
            tid.hash(),
            "clock_skew_metric",
            late_ts / 1_000_000,
            &[late_token],
            now_1,
            Duration::from_secs(5),
        )
        .await
        .expect("query with the late writer's own token"),
    );
    assert_eq!(
        with_token.len(),
        1,
        "the min_token (read-your-writes) path must be unaffected"
    );
    assert_eq!(with_token[0].value, 2.0);

    // Repair: delete HEAD, then fold again. The next fold sees an absent
    // HEAD and rebuilds from the commit layout directly, picking up the
    // late writer's record.
    store
        .delete(&head_key(&tid.hash()))
        .await
        .expect("delete HEAD to force a rebuild");
    folder_catalog(Arc::clone(&store))
        .fold(&tid.hash(), Signal::Metrics, Uuid::new_v4(), now_1, &[])
        .await
        .expect("rebuild fold after HEAD deletion");

    let repaired = expect_vector(
        engine(Arc::clone(&store), catalog(Arc::clone(&store), 1))
            .instant(
                tid.hash(),
                "clock_skew_metric",
                late_ts / 1_000_000,
                &[],
                now_1,
                Duration::from_secs(5),
            )
            .await
            .expect("query after the rebuild repair"),
    );
    assert_eq!(
        repaired.len(),
        1,
        "a HEAD rebuild must recover a commit orphaned by clock-skewed sealing"
    );
    assert_eq!(repaired[0].value, 2.0);
}
