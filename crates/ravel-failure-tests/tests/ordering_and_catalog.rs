//! Out-of-order and duplicate-timestamp sample resolution (ADR-0010 §5's
//! total order: `(created_unix_ns, writer_epoch, writer_seq, in-page
//! index)`, greatest wins), and `min_commit_token` unsatisfiability
//! (docs/catalog-and-mvcc.md "Read-your-writes").
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{catalog, engine, publish_segment, series_input, tenant};
use ravel_catalog::CatalogError;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::QueryError;
use ravel_types::CommitToken;
use uuid::Uuid;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Within one segment: a duplicate timestamp resolves to the
/// later-in-the-page sample, and a sample submitted out of order (earlier
/// in the input vec, later in time) still lands at its own correct
/// timestamp once `SegmentWriter` sorts it.
#[tokio::test]
async fn within_segment_duplicate_and_out_of_order_timestamps_resolve_correctly() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let ts = BASE_NS - NS_PER_MIN;
    let later_ts = ts + 5_000_000_000;

    // Deliberately out of order and with a duplicate timestamp: the later
    // event ts is listed first, and `ts` appears twice with different
    // values.
    let series = vec![series_input(
        &tid,
        "ordering_metric",
        &[],
        &[(later_ts, 3.0), (ts, 1.0), (ts, 9.0)],
    )];
    publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        u32::try_from(BASE_NS / (3_600 * NS_PER_SEC)).expect("hour bucket"),
        BASE_NS,
        series,
    )
    .await;

    let cat = catalog(Arc::clone(&store), 1);
    let eng = engine(store, cat);

    let at_ts = eng
        .instant(
            tid.hash(),
            "ordering_metric",
            ts / 1_000_000,
            &[],
            BASE_NS,
            Duration::from_secs(5),
        )
        .await
        .expect("query at ts");
    assert_eq!(at_ts.len(), 1);
    assert_eq!(
        at_ts[0].value, 9.0,
        "later-in-page sample must win the duplicate timestamp"
    );

    let at_later_ts = eng
        .instant(
            tid.hash(),
            "ordering_metric",
            later_ts / 1_000_000,
            &[],
            BASE_NS,
            Duration::from_secs(5),
        )
        .await
        .expect("query at later_ts");
    assert_eq!(at_later_ts.len(), 1);
    assert_eq!(
        at_later_ts[0].value, 3.0,
        "out-of-order input sample must still land at its own timestamp"
    );
}

/// Across two segments, the one with the greater `created_unix_ns` wins a
/// duplicate timestamp -- regardless of which segment was published first.
/// Publishing the later-created segment *before* the earlier-created one
/// proves the outcome depends on `created_unix_ns`, not call order.
#[tokio::test]
async fn cross_segment_duplicate_timestamp_later_commit_wins_regardless_of_publish_order() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let ts = BASE_NS - NS_PER_MIN;
    let hour_bucket = u32::try_from(BASE_NS / (3_600 * NS_PER_SEC)).expect("hour bucket");

    let created_early = BASE_NS - 10_000;
    let created_late = BASE_NS;

    // Publish the later-created ("B") segment first...
    let series_b = vec![series_input(&tid, "race_metric", &[], &[(ts, 2.0)])];
    publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour_bucket,
        created_late,
        series_b,
    )
    .await;

    // ...then the earlier-created ("A") segment second.
    let series_a = vec![series_input(&tid, "race_metric", &[], &[(ts, 1.0)])];
    publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour_bucket,
        created_early,
        series_a,
    )
    .await;

    let cat = catalog(Arc::clone(&store), 1);
    let eng = engine(store, cat);
    let result = eng
        .instant(
            tid.hash(),
            "race_metric",
            ts / 1_000_000,
            &[],
            BASE_NS,
            Duration::from_secs(5),
        )
        .await
        .expect("query");
    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].value, 2.0,
        "the segment with the greater created_unix_ns must win, even though it was published first"
    );
}

/// A `min_commit_token` referencing a commit record that never existed
/// must fail with `UnsatisfiableToken`, never silently fall back to
/// whatever (possibly stale) data happens to be in the catalog already.
#[tokio::test]
async fn unsatisfiable_min_commit_token_errors_instead_of_returning_stale_data() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tid = tenant("acme");
    let ts = BASE_NS - NS_PER_MIN;
    let hour_bucket = u32::try_from(BASE_NS / (3_600 * NS_PER_SEC)).expect("hour bucket");

    // Seed real, already-visible data: if the engine were to ignore an
    // unsatisfiable token and fall back to "whatever is visible", this is
    // the stale value it would wrongly return.
    let series = vec![series_input(&tid, "seeded_metric", &[], &[(ts, 123.0)])];
    publish_segment(
        store.as_ref(),
        tid.hash(),
        0,
        Uuid::new_v4(),
        1,
        1,
        hour_bucket,
        BASE_NS,
        series,
    )
    .await;

    let cat = catalog(Arc::clone(&store), 1);
    let eng = engine(store, cat);

    let bogus_token = CommitToken {
        shard: 0,
        writer_id: Uuid::new_v4(),
        epoch: 999,
        seq: 999,
        ingest_hour_bucket: hour_bucket,
    };
    let err = eng
        .instant(
            tid.hash(),
            "seeded_metric",
            ts / 1_000_000,
            &[bogus_token],
            BASE_NS,
            Duration::from_secs(5),
        )
        .await
        .expect_err("a token for a commit that never existed must fail resolution");
    assert!(
        matches!(
            err,
            QueryError::Catalog(CatalogError::UnsatisfiableToken { .. })
        ),
        "expected UnsatisfiableToken, got {err:?}"
    );
}
