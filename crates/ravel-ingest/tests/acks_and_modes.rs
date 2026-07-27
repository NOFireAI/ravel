//! Strict vs buffered ack semantics, and multi-shard receipts with
//! read-your-write via `Catalog::resolve` (docs/consistency-model.md).
#![allow(clippy::expect_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, commit_and_trailer_versions, make_point, tenant};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{IngestConfig, IngestRouter, SEGMENT_FORMAT_V2, WriteMode};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::{Signal, TimeRange};

fn config(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(50),
        ..IngestConfig::default()
    }
}

#[tokio::test]
async fn strict_ack_only_after_data_and_commit_exist() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    // `flush_all` only flushes tenants already buffered, so it must run
    // concurrently with (not after) the write, and after (not before) the
    // write's own enqueue; joining the two futures interleaves them
    // correctly without a timing-dependent sleep.
    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        router.flush_all(),
    );
    let receipt = write_result.expect("strict write succeeds");
    assert_eq!(receipt.tokens.len(), 1);

    let objects = list_all(store.as_ref(), "t/").await.expect("list");
    let data_objects = objects.iter().filter(|o| o.key.contains("/l0/")).count();
    let commit_objects = objects.iter().filter(|o| o.key.contains("/c/")).count();
    assert_eq!(data_objects, 1, "data object must exist once ack fires");
    assert_eq!(commit_objects, 1, "commit record must exist once ack fires");

    router.shutdown().await;
}

#[tokio::test]
async fn buffered_mode_acks_immediately_then_flush_lands_data() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let router = IngestRouter::new(
        config(1),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    let points = vec![make_point(
        &tenant,
        "cpu_usage",
        &[("host", "a")],
        1_000,
        1.0,
    )];

    let receipt = router
        .write(
            tenant.clone(),
            points,
            WriteMode::Buffered,
            Duration::from_secs(5),
        )
        .await
        .expect("buffered write succeeds");
    assert!(receipt.tokens.is_empty());

    let before = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(
        before.is_empty(),
        "buffered ack must not wait on durability"
    );

    router.flush_all().await;
    let after = list_all(store.as_ref(), "t/").await.expect("list");
    assert!(after.iter().any(|o| o.key.contains("/l0/")));
    assert!(after.iter().any(|o| o.key.contains("/c/")));

    router.shutdown().await;
}

#[tokio::test]
async fn multi_shard_write_returns_one_token_per_shard_and_is_read_your_write() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let shard_count = 4;
    let router = IngestRouter::new(
        config(shard_count),
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    // Enough distinct series that, with 4 shards, more than one shard is hit.
    let points: Vec<_> = (0..16)
        .map(|i| {
            make_point(
                &tenant,
                "cpu_usage",
                &[("host", &i.to_string())],
                1_000,
                i as f64,
            )
        })
        .collect();

    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        router.flush_all(),
    );
    let receipt = write_result.expect("strict write succeeds");
    assert!(
        receipt.tokens.len() > 1,
        "expected points to spread across multiple shards, got {} token(s)",
        receipt.tokens.len()
    );

    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog config valid");

    let snapshot = catalog
        .resolve(
            &tenant.hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: 0,
                end_ns: 2_000,
            },
            &receipt.tokens,
            clock.now(),
        )
        .await
        .expect("resolve succeeds");
    assert_eq!(
        snapshot.segments.len(),
        receipt.tokens.len(),
        "read-your-write must surface every flushed segment via min_tokens"
    );

    router.shutdown().await;
}

/// Mirror of `multi_shard_write_returns_one_token_per_shard_and_is_read_your_write`
/// with `segment_format_version` set to v2: strict-ack durability and
/// read-your-write via `min_tokens` must hold identically, and every
/// flushed object's own trailer must actually be v2 (not just the config
/// knob), proving the config reaches every shard actor, not just shard 0.
#[tokio::test]
async fn multi_shard_write_v2_is_strict_ack_durable_and_read_your_write() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(1_700_000_000_000_000_000);
    let shard_count = 4;
    let v2_config = IngestConfig {
        segment_format_version: SEGMENT_FORMAT_V2,
        ..config(shard_count)
    };
    let router = IngestRouter::new(
        v2_config,
        Arc::clone(&store),
        Signal::Metrics,
        clock.clone(),
    );

    let tenant = tenant("acme");
    // Enough distinct series that, with 4 shards, more than one shard is hit.
    let points: Vec<_> = (0..16)
        .map(|i| {
            make_point(
                &tenant,
                "cpu_usage",
                &[("host", &i.to_string())],
                1_000,
                i as f64,
            )
        })
        .collect();

    let (write_result, ()) = tokio::join!(
        router.write(
            tenant.clone(),
            points,
            WriteMode::Strict,
            Duration::from_secs(5)
        ),
        router.flush_all(),
    );
    let receipt = write_result.expect("strict write succeeds");
    assert!(
        receipt.tokens.len() > 1,
        "expected points to spread across multiple shards, got {} token(s)",
        receipt.tokens.len()
    );

    for token in &receipt.tokens {
        let (record_version, trailer_version) =
            commit_and_trailer_versions(store.as_ref(), &tenant.hash(), Signal::Metrics, token)
                .await;
        assert_eq!(record_version, u32::from(SEGMENT_FORMAT_V2));
        assert_eq!(trailer_version, SEGMENT_FORMAT_V2);
    }

    let catalog = Catalog::new(
        Arc::clone(&store),
        CatalogConfig {
            shard_count,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog config valid");

    let snapshot = catalog
        .resolve(
            &tenant.hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: 0,
                end_ns: 2_000,
            },
            &receipt.tokens,
            clock.now(),
        )
        .await
        .expect("resolve succeeds");
    assert_eq!(
        snapshot.segments.len(),
        receipt.tokens.len(),
        "read-your-write must surface every flushed v2 segment via min_tokens"
    );

    router.shutdown().await;
}
