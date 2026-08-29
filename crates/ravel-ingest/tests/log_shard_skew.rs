//! Per-shard ingest-skew metrics on the LOGS pipeline (issues #865, #800), the
//! counterpart of `shard_skew.rs`. The bulk loader (`ravel-cli load`, ADR-0089
//! and ADR-0109) drives this pipeline and not the metrics one, so these are the
//! figures any argument about where a bulk load's wall time goes has to rest on.
//!
//! The three spans and where each starts and stops, identical to the metrics
//! pipeline's:
//!
//! 1. `on_actor_ns`: the actor pulls a write off its channel -> `handle_write`
//!    (or `handle_write_columnar`) returns, minus any permit wait nested inside.
//! 2. `flush_permit_wait_ns`: `flush_tenant` reaches the `max_inflight_flushes`
//!    acquire -> that acquire grants.
//! 3. `off_actor_ns`: the spawned flush task enters `run_flush` (permit held) ->
//!    `run_flush` returns. On this pipeline that span is the RLOG encode plus
//!    the data-object PUT plus the commit-record publish.
//!
//! Every figure below is exact rather than banded: `SlowStore` sleeps on the
//! injected `Clock`, the spans are bracketed on that same clock, and the test is
//! the only thing that advances it, so the arithmetic is deterministic and a
//! loaded machine cannot move any assertion here.
#![allow(clippy::expect_used)]

mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common::{SlowStore, TestClock, tenant};
use ravel_ingest::{IngestConfig, LogIngestRouter, ShardSkewStats, WriteMode};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::logstream::{AttrValue, log_stream_id};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// Generous ceiling on the injected-clock spin helpers. The happy path yields a
/// handful of times, so this real-clock timeout never fires there and no
/// injected time passes; if the actor never reaches the expected state the spin
/// fails naming its condition instead of hanging until the harness kills the run.
const SPIN_TIMEOUT: Duration = Duration::from_secs(30);

/// One shard, `target_bytes: 1` so every write is its own size-triggered flush
/// (exactly what the bulk loader configures), and age thresholds far past the
/// scripted timeline so the only thing that opens a flush is a write and the
/// only thing that advances the clock is the test.
fn one_shard_config(max_inflight_flushes: u32) -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 1,
        max_inflight_flushes,
        max_flush_delay: Duration::from_secs(3600),
        max_flush_delay_idle: Duration::from_secs(3600),
        flush_tick: Duration::from_secs(3600),
        max_flush_lifetime: Duration::from_secs(86_400),
        ..IngestConfig::default()
    }
}

/// A consistently-built record: `stream_id` and `stream_attrs` share the same
/// resource inputs, so `RlogWriter::finish`'s collision check passes.
fn norm_record(host: &str, ts_ns: i64) -> NormalizedLogRecord {
    let res: Vec<(String, AttrValue)> = vec![
        (
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        ),
        ("host".to_string(), AttrValue::Str(host.to_string())),
    ];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope", "", &scope_attrs),
        stream_attrs: stream_attrs_bytes(&res, "scope", "", &scope_attrs),
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: "hello".to_string(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

fn skew_of(router: &LogIngestRouter, shard: u32) -> ShardSkewStats {
    let map: HashMap<u32, ShardSkewStats> =
        router.metrics().shard_skew_by_shard().into_iter().collect();
    map.get(&shard).copied().unwrap_or_default()
}

/// Spins until `store` has entered its artificial delay `n` times, i.e. `n`
/// flushes have their data PUT genuinely in flight and stalled on the injected
/// clock.
async fn await_slow_puts(store: &SlowStore, n: u64) {
    tokio::time::timeout(SPIN_TIMEOUT, async {
        while store.hits() < n {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "store never reached {n} slow PUTs within {SPIN_TIMEOUT:?}: reached {}",
            store.hits()
        )
    });
}

/// The logs pipeline records all three spans at all, which before issue #800 it
/// did not: the accounting was wired into `router.rs`/`shard.rs` only, so every
/// figure for the pipeline the bulk loader actually drives read as absent.
///
/// One strict write, one flush, one free permit. The flush's data PUT costs
/// SLOW on the injected clock and nothing else advances it, so every span is
/// known exactly: the whole cost is off-actor, the actor itself does no
/// clock-advancing work, and nothing parks on the semaphore.
///
/// Non-vacuity (prove-the-test): reverting either recording site in
/// `log_shard.rs` (the `record_shard_processed` call in the actor loop, or the
/// `record_shard_off_actor_ns` in the spawned flush) leaves this shard absent
/// from `shard_skew_by_shard`, so `messages_processed` and `off_actor_ns` both
/// read 0 and the first two assertions fail. Confirmed by running it against
/// the pre-change tree, where `shard_skew_by_shard` does not exist on
/// `LogIngestMetrics` at all and the test does not compile.
#[tokio::test]
async fn log_flush_cost_is_recorded_off_actor_and_nothing_parks_on_a_free_permit() {
    const SLOW: Duration = Duration::from_secs(5);

    let clock = TestClock::new(BASE_NS);
    let slow_store = Arc::new(SlowStore::new(
        MemoryStore::new(),
        clock.clone(),
        // The data-object PUT; the commit-record PUT ("/c/") stays fast.
        "/l0/",
        SLOW,
    ));
    let store: Arc<dyn ObjectStoreBackend> = slow_store.clone();
    let router = LogIngestRouter::new(one_shard_config(1), Arc::clone(&store), clock.clone());
    let t = tenant("acme");

    let write = {
        let router_records = vec![norm_record("h0", BASE_NS)];
        let t = t.clone();
        let router = &router;
        async move {
            router
                .write(
                    t,
                    router_records,
                    WriteMode::Strict,
                    Duration::from_secs(600),
                )
                .await
        }
    };
    let driver = async {
        await_slow_puts(&slow_store, 1).await;
        clock.advance_ns(SLOW.as_nanos() as i64);
    };
    let (receipt, ()) = tokio::join!(write, driver);
    receipt.expect("the strict write acks durable");

    // Barrier: `off_actor_ns` is recorded after `run_flush` returns, which can
    // be after the ack the write already observed. `flush_all` joins every
    // spawned flush task, so the figure is settled before it is read.
    router.flush_all().await;

    let shard0 = skew_of(&router, 0);
    assert_eq!(
        shard0.messages_enqueued, 1,
        "the router counted its one dispatch into the shard channel"
    );
    assert_eq!(
        shard0.messages_processed, 1,
        "the actor counted the write it handled"
    );
    assert_eq!(
        shard0.off_actor_ns,
        SLOW.as_nanos() as u64,
        "the flush's whole cost is off-actor: encode plus both PUTs, of which \
         the data PUT is the injected SLOW and everything else advances the \
         injected clock by nothing"
    );
    assert_eq!(
        shard0.on_actor_ns, 0,
        "the actor's merge-and-pin work advances the injected clock by nothing, \
         so charging it any of the flush would misattribute a flush bottleneck \
         to the actor"
    );
    assert_eq!(
        shard0.flush_permit_wait_ns, 0,
        "one flush against a free permit never parks on the semaphore"
    );
    assert_eq!(
        shard0.queue_depth, 0,
        "the one enqueued message was processed"
    );
}

/// The measurement that says which of the two write-concurrency windows is
/// binding, and shows that neither alone is enough (issue #800, ADR-0807). All
/// three arms run the SAME three flushes at the SAME injected cost. They differ
/// only in the two windows: whether a second batch is offered to the shard while
/// the first is still flushing (the loader's `--pipeline-depth`), and how many
/// concurrent flushes the shard will accept (`--max-inflight-flushes`).
///
/// | arm | submission | permits | wall | `flush_permit_wait_ns` |
/// |---|---|---|---|---|
/// | A | serial | 1 | `3 * SLOW` | 0 |
/// | B | concurrent | 1 | `3 * SLOW` | `2 * SLOW` |
/// | C | concurrent | 3 | `SLOW` | 0 |
///
/// Arm A is the shipped-before loader shape (`--pipeline-depth 1`): each write
/// is awaited before the next is submitted. `flush_permit_wait_ns` is exactly 0
/// however slow the flushes are, because the shard is never asked for a second
/// concurrent flush. That zero is the finding: at depth 1 the per-shard flush
/// window (issue #807) cannot be what the load is waiting on, so raising it
/// alone cannot move the wall.
///
/// Arm B is the same three flushes submitted concurrently against ONE permit.
/// The wall is identical to arm A's, so the outer window alone buys nothing
/// either -- but the counter now reads `2 * SLOW`, each of writes 2 and 3
/// parking out exactly one prior flush. A and B are the two ways to be slow, and
/// the counter is what tells them apart: 0 means "nobody asked this window for
/// anything", nonzero means "this window refused". Only the second is a reason
/// to raise it.
///
/// Arm C raises both. The three flushes overlap, the wall drops to one flush,
/// and the permit wait is 0 again -- with `off_actor_ns` still `3 * SLOW`,
/// legitimately exceeding the wall because the spans now run concurrently.
///
/// Non-vacuity (prove-the-test): no arm's figure is vacuous, because another arm
/// moves the same counter on the same fixture. A permit-wait counter stuck at 0
/// fails arm B; one that reported any wait unconditionally fails arms A and C; a
/// wall that ignored the windows fails arm C. Confirmed by replacing the
/// `record_shard_flush_permit_wait_ns` call in `log_shard.rs::flush_tenant` with
/// a discard, which is the pre-change state of that pipeline: arm B then fails
/// with `left: 0, right: 10000000000` while arms A and C still pass, since their
/// expectation for that counter is 0 either way.
#[tokio::test]
async fn neither_write_window_alone_moves_the_wall_and_the_counters_say_which() {
    const SLOW: Duration = Duration::from_secs(5);
    const WRITES: usize = 3;

    // Arm A: serial submission, one flush permit. The depth-1 loader shape.
    {
        let clock = TestClock::new(BASE_NS);
        let slow_store = Arc::new(SlowStore::new(
            MemoryStore::new(),
            clock.clone(),
            "/l0/",
            SLOW,
        ));
        let store: Arc<dyn ObjectStoreBackend> = slow_store.clone();
        let router = LogIngestRouter::new(one_shard_config(1), Arc::clone(&store), clock.clone());
        let t = tenant("acme");

        for i in 0..WRITES {
            let write = router.write(
                t.clone(),
                vec![norm_record("h0", BASE_NS + i as i64)],
                WriteMode::Strict,
                Duration::from_secs(600),
            );
            let driver = async {
                await_slow_puts(&slow_store, i as u64 + 1).await;
                clock.advance_ns(SLOW.as_nanos() as i64);
            };
            let (receipt, ()) = tokio::join!(write, driver);
            receipt.expect("each strict write acks durable");
        }
        router.flush_all().await;

        let shard0 = skew_of(&router, 0);
        assert_eq!(shard0.messages_processed, WRITES as u64);
        assert_eq!(
            shard0.flush_permit_wait_ns, 0,
            "submitting one batch at a time never asks the shard for a second \
             concurrent flush, so the max_inflight_flushes semaphore is never \
             contended and cannot be what the load is waiting on"
        );
        assert_eq!(
            shard0.off_actor_ns,
            WRITES as u64 * SLOW.as_nanos() as u64,
            "the shard did {WRITES} flushes' worth of work"
        );
        let elapsed_ns = (clock.now() - BASE_NS) as u64;
        assert_eq!(
            elapsed_ns,
            WRITES as u64 * SLOW.as_nanos() as u64,
            "and it took {WRITES} flushes' worth of wall to do it: one flush in \
             flight at any instant, which is the serialisation, not the semaphore"
        );
    }

    // Arms B and C: concurrent submission, against one permit and against three.
    let concurrent_arm = async |permits: u32| -> (ShardSkewStats, u64) {
        let clock = TestClock::new(BASE_NS);
        let slow_store = Arc::new(SlowStore::new(
            MemoryStore::new(),
            clock.clone(),
            "/l0/",
            SLOW,
        ));
        let store: Arc<dyn ObjectStoreBackend> = slow_store.clone();
        let router = Arc::new(LogIngestRouter::new(
            one_shard_config(permits),
            Arc::clone(&store),
            clock.clone(),
        ));
        let t = tenant("acme");

        let mut writes = Vec::new();
        for i in 0..WRITES {
            let router = Arc::clone(&router);
            let t = t.clone();
            writes.push(tokio::spawn(async move {
                router
                    .write(
                        t,
                        vec![norm_record("h0", BASE_NS + i as i64)],
                        WriteMode::Strict,
                        Duration::from_secs(600),
                    )
                    .await
            }));
        }

        // Retire the flushes one permit-window at a time: wait until this
        // window's PUTs are all genuinely in flight, then advance the clock
        // exactly once to complete them together. Advancing more often than
        // that would move the clock past a flush that has already finished but
        // has not yet been polled, inflating its `off_actor_ns` by the extra
        // jump. Waiting first makes the sequencing deterministic instead: a
        // flush task records `off_actor_ns` before it releases its permit, so
        // the next window's PUTs cannot reach the store until the previous
        // window's spans are already recorded at the pre-advance reading.
        let window = permits as usize;
        let mut retired = 0usize;
        while retired < WRITES {
            retired += window.min(WRITES - retired);
            await_slow_puts(&slow_store, retired as u64).await;
            clock.advance_ns(SLOW.as_nanos() as i64);
        }
        for w in writes {
            w.await
                .expect("write task")
                .expect("each strict write acks durable");
        }
        router.flush_all().await;
        (skew_of(&router, 0), (clock.now() - BASE_NS) as u64)
    };

    let (shard0, elapsed_ns) = concurrent_arm(1).await;
    assert_eq!(shard0.messages_processed, WRITES as u64);
    assert_eq!(
        shard0.flush_permit_wait_ns,
        2 * SLOW.as_nanos() as u64,
        "writes 2 and 3 each park out exactly one prior flush: at one permit the \
         inner window IS refusing work, and it says so on its own counter rather \
         than leaving the wall unexplained"
    );
    assert_eq!(
        shard0.off_actor_ns,
        WRITES as u64 * SLOW.as_nanos() as u64,
        "the same three flushes at the same cost as arm A"
    );
    assert_eq!(
        elapsed_ns,
        WRITES as u64 * SLOW.as_nanos() as u64,
        "and the same wall as arm A: offering the shard more work without raising \
         its flush window changes where the time is attributed, not how long it takes"
    );

    let (shard0, elapsed_ns) = concurrent_arm(WRITES as u32).await;
    assert_eq!(shard0.messages_processed, WRITES as u64);
    assert_eq!(
        shard0.flush_permit_wait_ns, 0,
        "with a permit per outstanding batch nothing parks"
    );
    assert_eq!(
        shard0.off_actor_ns,
        WRITES as u64 * SLOW.as_nanos() as u64,
        "the same three flushes at the same cost as arms A and B; this span is a \
         sum over concurrent tasks, so it legitimately exceeds the wall below"
    );
    assert_eq!(
        elapsed_ns,
        SLOW.as_nanos() as u64,
        "raising BOTH windows is what collapses {WRITES} flushes' work into one \
         flush's wall; either alone left it at {WRITES} * SLOW"
    );
}
