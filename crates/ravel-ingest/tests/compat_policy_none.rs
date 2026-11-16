//! A metrics router with no activation policy publishes byte for byte what it
//! published before the policy existed (ADR-4233; issue #4244). The pinned
//! digest is of one flush's objects as the parent of the policy work wrote
//! them, under a seeded writer rng and a fixed clock.
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_commit::SeededRng;
use ravel_ingest::{IngestConfig, IngestRouter, PendingAckBudget, WriteMode};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_types::Signal;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// blake3 over every key and object under the tenant's metrics prefix, in
/// key order, as the parent commit (de80c40) wrote them.
const PINNED_DIGEST: &str = "54c6374659568d3431485f2749ae515880ed3fabcff7a6d9f30ebb24f8165b9c";

fn router(store: &Arc<MemoryStore>) -> IngestRouter {
    let store: Arc<dyn ObjectStoreBackend> = store.clone();
    IngestRouter::with_rng(
        IngestConfig {
            shard_count: 1,
            ..IngestConfig::default()
        },
        store,
        Signal::Metrics,
        TestClock::new(BASE_NS),
        Arc::new(SeededRng::new(4244)),
        Arc::new(PendingAckBudget::new(0)),
    )
}

async fn flush_digest(store: &MemoryStore, router: IngestRouter) -> String {
    let acme = tenant("acme");
    let points = (0..4)
        .map(|i| {
            make_point(
                &acme,
                "http_requests_total",
                &[("host", "a")],
                BASE_NS + i * 1_000_000_000,
                f64::from(i32::try_from(i).expect("small")),
            )
        })
        .collect();
    router
        .write(
            acme.clone(),
            points,
            WriteMode::StrictArrival,
            Duration::from_secs(60),
        )
        .await
        .expect("written");
    let prefix = format!(
        "t/{}/{}/",
        acme.hash().to_hex(),
        Signal::Metrics.key_prefix()
    );
    let mut keys: Vec<String> = list_all(store, &prefix)
        .await
        .expect("list")
        .into_iter()
        .map(|meta| meta.key)
        .collect();
    keys.sort();
    assert!(keys.iter().any(|key| key.contains("/l0/")), "{keys:?}");
    assert!(keys.iter().any(|key| key.contains("/c/")), "{keys:?}");
    let mut hasher = blake3::Hasher::new();
    for key in &keys {
        let bytes = store.get(key, GetRange::Full).await.expect("get").data;
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key.as_bytes());
        hasher.update(&(bytes.len() as u64).to_le_bytes());
        hasher.update(&bytes);
    }
    hasher.finalize().to_hex().to_string()
}

#[tokio::test]
async fn a_router_without_a_policy_publishes_what_the_parent_commit_published() {
    let store = Arc::new(MemoryStore::new());
    let digest = flush_digest(&store, router(&store)).await;
    assert_eq!(digest, PINNED_DIGEST);
}

#[tokio::test]
async fn a_router_given_no_policy_explicitly_publishes_what_the_parent_commit_published() {
    let store = Arc::new(MemoryStore::new());
    let router = router(&store)
        .with_compat_policy(None)
        .expect("no policy, no publish bound");
    let digest = flush_digest(&store, router).await;
    assert_eq!(digest, PINNED_DIGEST);
}
