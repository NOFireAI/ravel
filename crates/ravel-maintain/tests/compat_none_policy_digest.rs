//! With no compat policy the maintenance writers write exactly what they
//! wrote before the policy existed. Each test compacts a fixed bucket per
//! signal and pins a blake3 digest over every object the store then holds,
//! keys and bytes, so any change to a part, a record or a key under a `None`
//! policy fails here. The digests were taken on the commit before the policy
//! was added and match it unchanged.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use common::*;
use ravel_maintain::{CompactionOutcome, CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use uuid::Uuid;

/// blake3 over every object in the store in key order: each key's bytes, a
/// zero byte, the object's length as u64 LE, then the object's bytes.
async fn store_digest(store: &dyn ObjectStoreBackend) -> String {
    let mut listing = list_all(store, "").await.expect("list");
    listing.sort_by(|a, b| a.key.cmp(&b.key));
    let mut hasher = blake3::Hasher::new();
    for meta in listing {
        let bytes = get_full(store, &meta.key).await;
        hasher.update(meta.key.as_bytes());
        hasher.update(&[0]);
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
    }
    hasher.finalize().to_hex().to_string()
}

async fn compact(store: &MemoryStore, target: &ravel_maintain::Bucket) {
    let outcome = compact_bucket(
        store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        target,
    )
    .await
    .expect("compacts");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn metrics_compaction_without_a_policy_matches_the_pinned_digest() {
    let store = MemoryStore::new();
    for spec in [
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![
                raw_series("keep", &[("k", "a")], &[(1_000, 1.0), (2_000, 2.0)]),
                raw_series("victim", &[("k", "b")], &[(1_000, 5.0)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("victim", &[("k", "b")], &[(3_000, 3.0)])],
        ),
    ] {
        seed_input(&store, &spec).await;
    }
    compact(&store, &bucket()).await;
    assert_eq!(store_digest(&store).await, "15b363bd2f07841b1797386b77084906dc2de9edf3fe927ef7b553f9e59c6945");
}

#[tokio::test]
async fn logs_compaction_without_a_policy_matches_the_pinned_digest() {
    let store = MemoryStore::new();
    let target = seed_rlog_two_inputs(&store).await;
    compact(&store, &target).await;
    assert_eq!(store_digest(&store).await, "597076e7830eb61b4357738723c4211fad714ccd3fcb6e61ae948b0c61330b65");
}

#[tokio::test]
async fn spans_compaction_without_a_policy_matches_the_pinned_digest() {
    let store = MemoryStore::new();
    let target = seed_rspan_two_inputs(&store).await;
    compact(&store, &target).await;
    assert_eq!(store_digest(&store).await, "68f97d7569aee22b65b65322cd88930fb4f7a84c489802bc83698c286ebf794a");
}
