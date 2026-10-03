//! `SpanIngestRouter` shard-death observability: a split-brain panic takes a
//! span shard actor down mid-flush, the caller sees the typed
//! `ShardUnavailable`, and because the span router never respawns, that first
//! death condemns the shard. The condemned count (which drives `/readyz` 503)
//! moves exactly once and `ready()` turns false. Mirrors the log router's own
//! `dead_shard_is_observable_and_counted_once` in `log_router.rs`, swapping
//! `NormalizedSpan`s and the span commit keyspace for the log ones.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{SplitBrainOnFirstCommit, TestClock, span_on_shard, tenant};
use ravel_ingest::{IngestConfig, SpanIngestRouter, SpanWriteError, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::Signal;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Flushes on the first span (`target_bytes: 1`) and never on age, so a strict
/// write drives one complete flush inline and returns its outcome.
fn flush_on_first(shard_count: u32) -> IngestConfig {
    IngestConfig {
        shard_count,
        target_bytes: 1,
        max_flush_delay: Duration::from_secs(3600),
        flush_tick: Duration::from_millis(20),
        put_retry_base_delay: Duration::from_millis(1),
        put_retry_max_delay: Duration::from_millis(5),
        ..IngestConfig::default()
    }
}

#[tokio::test]
async fn dead_shard_is_observable_and_condemns_on_first_death() {
    let shard_count = 4;
    // `/s/c/` is the span commit keyspace, so only a span flush trips the
    // poison; `Signal::Spans` stamps the conflicting record landed there.
    let store: Arc<dyn ObjectStoreBackend> =
        Arc::new(SplitBrainOnFirstCommit::new("/s/c/", Signal::Spans));
    let clock = TestClock::new(BASE_NS);
    let router = SpanIngestRouter::new(
        flush_on_first(shard_count),
        Arc::clone(&store),
        clock.clone(),
    );

    let tenant = tenant("acme");
    let victim = 0;
    let survivor = 1;

    // The victim flush hits the poisoned commit key and panics its actor
    // mid-flush; the waiter's ack sender dies with it, so the caller gets the
    // typed ShardUnavailable and the router counts the death.
    let err = router
        .write(
            tenant.clone(),
            vec![span_on_shard(victim, shard_count, 1_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the split-brain panic takes the shard actor down mid-flush");
    assert!(
        matches!(err, SpanWriteError::ShardUnavailable),
        "a dead shard is reported as the typed ShardUnavailable, got {err}"
    );
    assert_eq!(router.metrics().snapshot().shard_deaths, 1);
    // The span router never respawns, so the first death condemns the shard:
    // the counter that drives /readyz 503 moves on this same death, and the
    // router reports not-ready.
    assert_eq!(
        router.metrics().snapshot().shards_condemned,
        1,
        "the first shard death condemns the shard (spans never respawn)"
    );
    assert_eq!(router.metrics().condemned_shards(), 1);
    assert!(
        !router.ready(),
        "a condemned shard makes the router report not-ready"
    );

    // A survivor shard still acks durably.
    let receipt = router
        .write(
            tenant.clone(),
            vec![span_on_shard(survivor, shard_count, 2_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("surviving shards keep acking after a sibling dies");
    assert_eq!(receipt.tokens.len(), 1);

    // The dead shard never comes back; a later write to it fails at the send
    // half with the same typed error, not double-counted.
    let again = router
        .write(
            tenant.clone(),
            vec![span_on_shard(victim, shard_count, 3_000)],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect_err("the dead shard never comes back");
    assert!(matches!(again, SpanWriteError::ShardUnavailable));
    assert_eq!(
        router.metrics().snapshot().shard_deaths,
        1,
        "a permanently dead shard is counted once, not once per routed write"
    );
    assert_eq!(
        router.metrics().snapshot().shards_condemned,
        1,
        "condemnation is counted once per shard, not once per routed write"
    );
    assert!(
        !router.ready(),
        "the condemned shard keeps the router not-ready"
    );

    router.shutdown().await;
}
