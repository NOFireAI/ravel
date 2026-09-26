//! Per-shard ingest-skew metrics on the SPANS pipeline (issue #1692), driven
//! through the real `SpanIngestRouter` into a live shard actor rather than by
//! calling the recorders directly, so deleting either recording site (the
//! router's enqueue count or the actor's processed count) fails a test here.
//!
//! The actor is held deterministically: a first write's flush stalls on a
//! `SlowStore` PUT that only the injected clock releases, then a `FlushNow` is
//! enqueued ahead of further writes. The actor pulls that `FlushNow` first
//! (the channel is FIFO) and parks in it until every in-flight flush has
//! finished, so the writes behind it stay in the channel until the test
//! advances the clock.
#![allow(clippy::expect_used)]

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::{SlowStore, TestClock, span_on_shard, tenant};
use ravel_ingest::{IngestConfig, ShardSkewStats, SpanIngestRouter, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Ceiling on the spin helpers. The happy path yields a handful of times, so
/// this real-clock timeout never fires there; if the pipeline never reaches the
/// expected state the spin fails naming its condition instead of hanging.
const SPIN_TIMEOUT: Duration = Duration::from_secs(30);

const SLOW: Duration = Duration::from_secs(5);

fn skew_of(router: &SpanIngestRouter, shard: u32) -> ShardSkewStats {
    let map: HashMap<u32, ShardSkewStats> =
        router.metrics().shard_skew_by_shard().into_iter().collect();
    map.get(&shard).copied().unwrap_or_default()
}

async fn spin_until(what: &str, mut cond: impl FnMut() -> bool) {
    tokio::time::timeout(SPIN_TIMEOUT, async {
        while !cond() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what} not reached within {SPIN_TIMEOUT:?}"));
}

#[tokio::test]
async fn span_router_and_actor_record_enqueued_processed_and_queue_depth() {
    const QUEUED: u64 = 3;

    let clock = TestClock::new(BASE_NS);
    let slow_store = Arc::new(SlowStore::new(
        MemoryStore::new(),
        clock.clone(),
        // The span data-object PUT; the commit-record PUT stays fast.
        "/s/l0/",
        SLOW,
    ));
    let store: Arc<dyn ObjectStoreBackend> = slow_store.clone();
    let config = IngestConfig {
        shard_count: 1,
        target_bytes: 1,
        max_inflight_flushes: 4,
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        flush_tick: Duration::from_secs(3600),
        max_flush_lifetime: Duration::from_secs(86_400),
        ..IngestConfig::default()
    };
    let router = SpanIngestRouter::new(config, Arc::clone(&store), clock.clone());
    let t = tenant("acme");
    let write = |i: i64| {
        router.write(
            t.clone(),
            vec![span_on_shard(0, 1, BASE_NS + i)],
            WriteMode::Buffered,
            Duration::from_secs(600),
        )
    };

    // Write 1 is processed and its flush stalls on the slow PUT.
    write(0).await.expect("buffered write 1 is accepted");
    spin_until("write 1 processed and its flush PUT stalled", || {
        slow_store.hits() == 1 && skew_of(&router, 0).messages_processed == 1
    })
    .await;

    // One poll sends `FlushNow` into the channel (it has room) and then parks
    // on its done signal, so everything written after this queues behind it.
    let mut held = Box::pin(router.flush_all());
    assert!(
        futures::poll!(held.as_mut()).is_pending(),
        "FlushNow cannot complete while write 1's flush is stalled"
    );
    for i in 1..=QUEUED {
        write(i as i64).await.expect("buffered write is accepted");
    }

    let shard0 = skew_of(&router, 0);
    assert_eq!(
        shard0.messages_enqueued,
        1 + QUEUED,
        "the router counted every write it sent into the shard channel"
    );
    assert_eq!(
        shard0.messages_processed, 1,
        "the held actor has handled only write 1"
    );
    assert_eq!(
        shard0.queue_depth, QUEUED,
        "the writes queued behind the held actor are the channel depth"
    );

    // Release write 1's flush; the actor finishes FlushNow and then drains the
    // queued writes, each of which opens its own stalled flush.
    clock.advance_ns(SLOW.as_nanos() as i64);
    held.await;
    spin_until("every queued write's flush PUT stalled", || {
        slow_store.hits() == 1 + QUEUED
    })
    .await;
    clock.advance_ns(SLOW.as_nanos() as i64);
    router.flush_all().await;

    let shard0 = skew_of(&router, 0);
    assert_eq!(shard0.messages_enqueued, 1 + QUEUED);
    assert_eq!(
        shard0.messages_processed,
        1 + QUEUED,
        "the released actor handled every queued write"
    );
    assert_eq!(shard0.queue_depth, 0, "the channel is drained");
    assert_eq!(
        shard0.off_actor_ns,
        (1 + QUEUED) * SLOW.as_nanos() as u64,
        "each flush cost exactly one SLOW data PUT on the injected clock"
    );
    assert_eq!(
        shard0.on_actor_ns, 0,
        "the actor's own work advances the injected clock by nothing"
    );
}
