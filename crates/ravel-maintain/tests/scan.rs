//! Sealed-bucket scan and advisory CAS cursor (plan §3.2). Covers multi-hour
//! walking, cursor advancement, stopping at the first unsealed hour, and the
//! cursor's re-scan-avoidance on a second pass.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_commit::keys;
use ravel_maintain::{CompactorConfig, FixedClock, scan_and_compact};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::Signal;
use uuid::Uuid;

fn two_at(hour: u32, base: u128) -> Vec<InputSpec> {
    vec![
        InputSpec::new_at(
            hour,
            Uuid::from_u128(base),
            1,
            1,
            vec![raw_series("m", &[("k", "a")], &[(1_000, 1.0)])],
        ),
        InputSpec::new_at(
            hour,
            Uuid::from_u128(base + 1),
            1,
            2,
            vec![raw_series("m", &[("k", "a")], &[(2_000, 2.0)])],
        ),
    ]
}

#[tokio::test]
async fn scan_compacts_all_sealed_hours_and_advances_cursor() {
    let store = MemoryStore::new();
    for s in two_at(HOUR, 10).into_iter().chain(two_at(HOUR + 1, 20)) {
        seed_input(&store, &s).await;
    }
    let clock = FixedClock::new((i64::from(HOUR + 1) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR);

    let report = scan_and_compact(
        &store,
        &clock,
        &CompactorConfig::default(),
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("scan");
    assert_eq!(report.compacted, 2);
    assert_eq!(report.cursor_advanced_to, Some(HOUR + 1));

    // Both buckets now carry a compaction record.
    let record_a = fetch_compaction_record(&store, &bucket_at(HOUR)).await;
    let record_b = fetch_compaction_record(&store, &bucket_at(HOUR + 1)).await;
    assert_eq!(record_a.inputs.len(), 2);
    assert_eq!(record_b.inputs.len(), 2);

    // The advisory cursor object exists.
    let cursor_key = keys::maint_cursor_key(&tenant_hash(), Signal::Metrics, SHARD).unwrap();
    assert!(store.get(&cursor_key, GetRange::Full).await.is_ok());

    // Second pass: cursor skips both done hours; nothing new compacted.
    let report2 = scan_and_compact(
        &store,
        &clock,
        &CompactorConfig::default(),
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("scan 2");
    assert_eq!(report2.compacted, 0);
    assert_eq!(report2.already_done, 0, "cursor skips already-done hours");
}

#[tokio::test]
async fn scan_stops_at_first_unsealed_hour() {
    let store = MemoryStore::new();
    // HOUR is sealed; a far-future hour is not.
    let future = HOUR + 5;
    for s in two_at(HOUR, 10).into_iter().chain(two_at(future, 20)) {
        seed_input(&store, &s).await;
    }
    // now seals HOUR but not `future`.
    let clock = FixedClock::new((i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR);

    let report = scan_and_compact(
        &store,
        &clock,
        &CompactorConfig::default(),
        tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("scan");
    assert_eq!(report.compacted, 1);
    assert_eq!(report.not_sealed, 1);
    assert_eq!(report.cursor_advanced_to, Some(HOUR));
    // The unsealed future bucket is untouched.
    assert_eq!(
        ravel_maintain::compact_bucket(
            &store,
            &clock,
            &CompactorConfig::default(),
            &bucket_at(future)
        )
        .await
        .expect("check"),
        ravel_maintain::CompactionOutcome::NotSealed
    );
}
