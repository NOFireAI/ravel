//! A legal hold acknowledged while a maintain pass is running (issue #4501).
//!
//! ADR-0042 decision 2 and LifecycleGC.tla `HeldObjectNeverDeleted` say a hold
//! present at a delete's own step stops that delete. The code loads the hold
//! set once per pass (`LegalHoldCheck::refresh`) and every delete in the pass
//! reads only that snapshot, so a hold acknowledged after the refresh is not
//! honoured until the next pass.
//!
//! This test pins the shape of that window with the same entry points the
//! maintain tick uses: one refresh, then `sweep_shard` with that snapshot. It
//! asserts the ADR/model contract and is ignored until #4501 decides between
//! the two contracts:
//!
//! 1. Code matches the model: a hold acknowledged before a delete batch stops
//!    it (re-read holds before each batch, or fence hold writes against
//!    running passes). This test then passes and loses its ignore.
//! 2. Model matches the code: a hold takes effect from the next pass. This
//!    test is then rewritten to assert the documented next-pass bound and the
//!    ADR, model and hold CLI say so.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, LegalHoldCheck, compact_bucket,
    shard_hold_scopes, sweep_shard, write_hold_set,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use uuid::Uuid;

fn cfg() -> CompactorConfig {
    CompactorConfig {
        max_query_duration_ns: 0,
        head_cache_ttl_ns: 0,
        clock_skew_allowance_ns: 0,
        ..CompactorConfig::default()
    }
}

async fn seed_and_compact(store: &dyn ObjectStoreBackend, clock: &FixedClock) -> Bucket {
    for spec in [
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![raw_series("m", &[("k", "a")], &[(1_000, 1.0)])],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("m", &[("k", "a")], &[(3_000, 3.0)])],
        ),
    ] {
        seed_input(store, &spec).await;
    }
    let bucket = bucket();
    let outcome = compact_bucket(store, clock, &cfg(), &bucket)
        .await
        .expect("compact");
    assert!(matches!(outcome, CompactionOutcome::Compacted { .. }));
    bucket
}

async fn l0_data_count(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> usize {
    let prefix = format!(
        "t/{}/{}/l0/{:04}/",
        bucket.tenant_hash.to_hex(),
        bucket.signal.key_prefix(),
        bucket.shard
    );
    list_all(store, &prefix).await.unwrap().len()
}

#[tokio::test]
#[ignore = "contract decision pending #4501: a hold acknowledged after the pass's hold refresh does not stop that pass's deletes"]
async fn a_hold_acknowledged_after_the_pass_refresh_stops_the_pass_deletes() {
    let created = sealed_now_ns();
    let past_horizon = created
        .saturating_add(cfg().protection_horizon_ns)
        .saturating_add(NS_PER_HOUR);

    // Control: with no hold the same pass deletes the superseded inputs, so
    // the held run below is checking a delete that would otherwise happen.
    let control = MemoryStore::new();
    let control_clock = FixedClock::new(created);
    let control_bucket = seed_and_compact(&control, &control_clock).await;
    let control_snapshot = LegalHoldCheck::refresh(&control, &control_bucket.tenant_hash)
        .await
        .expect("refresh");
    control_clock.set(past_horizon);
    let control_report = sweep_shard(
        &control,
        &control_clock,
        &cfg(),
        &control_snapshot,
        &control_bucket.tenant_hash,
        control_bucket.signal,
        control_bucket.shard,
    )
    .await
    .expect("control sweep");
    assert!(control_report.superseded_data_deleted > 0);
    assert_eq!(l0_data_count(&control, &control_bucket).await, 0);

    let store = MemoryStore::new();
    let clock = FixedClock::new(created);
    let bucket = seed_and_compact(&store, &clock).await;
    let data_before = l0_data_count(&store, &bucket).await;
    assert!(data_before > 0);

    // The pass starts: its hold refresh sees no hold.
    let snapshot = LegalHoldCheck::refresh(&store, &bucket.tenant_hash)
        .await
        .expect("refresh");
    assert!(snapshot.active_prefixes().is_empty());

    // The operator's hold over the whole shard is acknowledged (durable)
    // while the pass is still running.
    for (i, scope) in shard_hold_scopes(&bucket.tenant_hash, bucket.signal, bucket.shard)
        .expect("shard scopes")
        .iter()
        .enumerate()
    {
        write_hold_set(
            &store,
            &bucket.tenant_hash,
            Uuid::from_u128(0x4501 + i as u128),
            created,
            scope,
            "hold acknowledged mid-pass",
        )
        .await
        .expect("set hold");
    }
    assert!(
        !LegalHoldCheck::refresh(&store, &bucket.tenant_hash)
            .await
            .expect("re-read")
            .active_prefixes()
            .is_empty(),
        "the hold is durable before the delete batch runs"
    );

    // The same pass reaches its delete batch, past the protection horizon.
    clock.set(past_horizon);
    let report = sweep_shard(
        &store,
        &clock,
        &cfg(),
        &snapshot,
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
    )
    .await
    .expect("sweep");

    assert_eq!(
        l0_data_count(&store, &bucket).await,
        data_before,
        "an object under a hold acknowledged before its delete is not deleted \
         (ADR-0042 decision 2, HeldObjectNeverDeleted); sweep report: {report:?}"
    );
}
