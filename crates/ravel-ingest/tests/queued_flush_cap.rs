//! Per-shard queued-flush cap (issue #1740), the bound ADR-1642's amendment
//! adds on top of `max_inflight_flushes`.
//!
//! ADR-1642 moved the `max_inflight_flushes` acquire inside the spawned flush
//! task so the actor never parks. The exposure it recorded: the
//! spawned-but-waiting queue has no bound of its own, and under
//! `IngestByteBudgetLimit::Unlimited` neither does anything else, so a shard
//! whose PUTs are stalled spawns one more flush window per age tick forever.
//!
//! Each test here parks the store's data PUT with a `FaultStore` gate, gives
//! the shard exactly one flush permit, and drives `CAP + DEFERRED` age ticks
//! on the injected clock. The shard must stop at `CAP` spawned flushes
//! exactly, refuse the remaining `DEFERRED` triggers, and leave those rows in
//! the tenant buffer with their arrival bookkeeping intact. Releasing the gate
//! then drains the capped prefix and the next tick flushes the deferred rows
//! as one object, whose commit record carries the exact refused row count.
//!
//! Every wait here is a cooperative poll on a metric plus an injected-clock
//! advance. No wall-clock sleep, no `tokio::time::timeout`, no `Instant`.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{TestClock, make_point, tenant};
use ravel_commit::keys;
use ravel_commit::record;
use ravel_ingest::{
    IngestByteBudget, IngestByteBudgetLimit, IngestConfig, IngestRouter, LogIngestRouter,
    SpanIngestRouter, WriteMode,
};
use ravel_logseg::stream_attrs_bytes;
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_rspan::StatusCode;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{Signal, TenantId};

const BASE_NS: i64 = 1_700_000_000_000_000_000;

/// The cap under test. Deliberately not the default 8: a test that happens to
/// agree with the default cannot distinguish "the configured cap was honored"
/// from "some other constant is 8".
const CAP: usize = 3;

/// Triggers driven past the cap. Also the exact row count that must survive in
/// the buffer, since each deferred write contributes exactly one row.
const DEFERRED: usize = 2;

/// One flush permit, a huge `target_bytes` so only the age trigger can ever
/// fire, and `max_flush_delay` far below the per-tick advance below. Retry
/// delays are irrelevant here (no PUT ever fails) but kept short so a future
/// fault in this fixture fails fast rather than idling.
fn capped_config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        target_bytes: 8 * 1024 * 1024,
        max_flush_delay: Duration::from_millis(50),
        flush_tick: Duration::from_millis(10),
        max_inflight_flushes: 1,
        max_queued_flushes: CAP,
        ..IngestConfig::default()
    }
}

/// Past `max_flush_delay` (50 ms) on every tick, and far below the 3600 s
/// `max_flush_lifetime`, so no parked flush's deadline elapses over the whole
/// run and the abandonment path never competes with the cap for the evidence.
const TICK_ADVANCE_NS: i64 = 100_000_000;

/// An unlimited byte budget, stated explicitly rather than relying on the
/// router default: this is the configuration in which `try_charge` never
/// sheds, so the queued-flush cap is the only bound left.
fn unlimited() -> Arc<IngestByteBudget> {
    IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited)
}

/// Yields until `probe` is true. Every caller's `probe` reads a metric the
/// shard actor publishes, so this converts "the actor got there" into a
/// cooperative wait rather than a timed one. A probe that never becomes true
/// hangs the test, which the runner reports as a timeout with this test's own
/// name; a wall-clock bound here would instead report a slow machine as a cap
/// violation.
async fn until(mut probe: impl FnMut() -> bool) {
    while !probe() {
        tokio::task::yield_now().await;
    }
}

// ---------------------------------------------------------------------------
// Metrics shard actor
// ---------------------------------------------------------------------------

/// Acceptance test for issue #1740 on the metrics shard actor.
///
/// Unlimited byte budget, one flush permit, the data PUT parked, and
/// `CAP + DEFERRED` age ticks driven on the injected clock. Without the cap the
/// shard spawns a flush on every one of those ticks and the in-flight gauge
/// reaches `CAP + DEFERRED`; with it, the gauge stops at `CAP` exactly and the
/// `DEFERRED` refused rows are still in the tenant buffer, unacked and
/// undropped. Releasing the gate drains the capped prefix, and the next tick
/// flushes the refused rows as a single object whose commit record carries
/// exactly `DEFERRED` samples.
#[tokio::test]
async fn unlimited_budget_with_stalled_put_stops_spawning_at_the_cap() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(BASE_NS);
    let router = Arc::new(
        IngestRouter::new(
            capped_config(),
            Arc::clone(&store),
            Signal::Metrics,
            clock.clone(),
        )
        .with_budget(unlimited()),
    );

    let acme = tenant("acme");

    // Held at the first data-object PUT only. That first flush parks here
    // holding the single permit; every later flush parks on the semaphore
    // instead and never reaches a PUT at all. That parked set is the queue
    // ADR-1642 left unbounded. `Nth(1)` rather than `Always` so releasing this
    // one call lets the whole capped prefix drain: with one permit the flushes
    // serialize, and an always-armed gate would re-hold each in turn.
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

    // Drive CAP age ticks, each with its own strict write, so each tick finds a
    // non-empty buffer and spawns one flush.
    let mut capped_writes = Vec::new();
    for i in 0..CAP {
        capped_writes.push(spawn_point_write(&router, &acme, i));
        let want = (i + 1) as u64;
        until(|| router.metrics().snapshot().buffered_points_total >= want).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| in_flight(&router) == want).await;
    }
    assert_eq!(
        gate.held_count(),
        1,
        "exactly one flush holds the single permit and reaches the PUT; the \
         rest are parked on the semaphore"
    );

    // Now drive DEFERRED more ticks against a shard that is already at its cap.
    let mut deferred_writes = Vec::new();
    for j in 0..DEFERRED {
        deferred_writes.push(spawn_point_write(&router, &acme, CAP + j));
        let want = (CAP + j + 1) as u64;
        until(|| router.metrics().snapshot().buffered_points_total >= want).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        // Waits for whichever of the two outcomes the tick produces: the
        // trigger is refused (the deferred counter moves), or it is not and a
        // flush spawns anyway (the in-flight gauge moves past the cap).
        // Without the second disjunct an uncapped actor hangs here instead of
        // reaching the assertion below, and a hang is not a demonstration.
        until(|| deferred_triggers(&router) >= (j + 1) as u64 || in_flight(&router) > CAP as u64)
            .await;
    }

    // The headline assertion. `flushes_in_flight` counts spawned flush tasks
    // (the guard is taken on the actor before the spawn and dropped when the
    // task ends), so on the unmodified actor this reads CAP + DEFERRED.
    assert_eq!(
        in_flight(&router),
        CAP as u64,
        "the shard must stop spawning at max_queued_flushes: {CAP} flush \
         windows held, not {}",
        CAP + DEFERRED
    );
    assert_eq!(
        queued_gauge(&router),
        CAP as u64,
        "the flushes_queued gauge must report the same count the cap is \
         tested against"
    );
    assert_eq!(
        deferred_triggers(&router),
        DEFERRED as u64,
        "every trigger past the cap is counted as deferred, exactly once"
    );
    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_age, CAP as u64,
        "a refused trigger never reaches record_flush, so it is not counted \
         as a flush that happened"
    );
    assert_eq!(
        snapshot.flushes_by_size, 0,
        "target_bytes is never reached; only the age trigger fires here"
    );
    assert_eq!(
        snapshot.acks_err, 0,
        "a refusal is a deferral, not a shed: nobody is acked with an error"
    );

    // The refused rows are still buffered: their writers are still waiting,
    // which is only true if the rows were neither flushed nor dropped.
    for (j, write) in deferred_writes.iter().enumerate() {
        assert!(
            !write.is_finished(),
            "deferred write {j} must still be waiting on a buffered row"
        );
    }

    // Release the stalled prefix. Each completion frees the permit for the next
    // parked flush and is reaped by the actor's join arm, which brings the
    // queued count back below the cap.
    for id in gate.held() {
        assert!(gate.release(id));
    }
    for (i, write) in capped_writes.into_iter().enumerate() {
        let receipt = write
            .await
            .expect("capped write task")
            .unwrap_or_else(|e| panic!("capped write {i} acks: {e}"));
        assert_eq!(receipt.tokens.len(), 1);
    }

    // With the capped prefix reaped, the queued count is back to zero and the
    // shard is below its cap again. Nothing has advanced the clock since the
    // last refusal, so no tick has fired in between and the deferred rows are
    // still buffered.
    until(|| in_flight(&router) == 0 && queued_gauge(&router) == 0).await;
    assert_eq!(
        deferred_triggers(&router),
        DEFERRED as u64,
        "draining the prefix must not refuse anything further"
    );

    // Exactly one more tick, with no new rows written: the buffer kept
    // `oldest_arrival_ns` through every refusal, so it is still due.
    clock.advance_ns(TICK_ADVANCE_NS);
    let mut deferred_tokens = Vec::new();
    for (j, write) in deferred_writes.into_iter().enumerate() {
        let receipt = write
            .await
            .expect("deferred write task")
            .unwrap_or_else(|e| panic!("deferred write {j} acks after the cap clears: {e}"));
        assert_eq!(receipt.tokens.len(), 1);
        deferred_tokens.push(receipt.tokens[0].clone());
    }

    // All DEFERRED refused rows rode one buffer into one flush: one shared
    // token, and its commit record carries exactly DEFERRED samples.
    assert!(
        deferred_tokens.windows(2).all(|w| w[0] == w[1]),
        "the refused rows stayed in one buffer, so one flush acks them all"
    );
    let commit_key = keys::commit_key_for_token(&acme.hash(), Signal::Metrics, &deferred_tokens[0])
        .expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let decoded = record::decode(&commit_bytes).expect("decode commit record");
    assert_eq!(
        decoded.sample_count, DEFERRED as u64,
        "the deferred flush writes exactly the rows the cap refused"
    );
    assert_eq!(
        decoded.series_count, DEFERRED as u64,
        "each refused write contributed its own series, all held together"
    );

    let final_snapshot = router.metrics().snapshot();
    assert_eq!(
        final_snapshot.acks_ok,
        (CAP + DEFERRED) as u64,
        "every write is acked exactly once: the cap defers rows, it never \
         drops or double-acks them"
    );
    assert_eq!(final_snapshot.acks_err, 0);

    router.flush_all().await;
}

/// The release case: once a queued flush is acked and reaped, the very next
/// age tick spawns the flush the cap refused. Proves the refusal is a deferral
/// with a retry, not a silent drop that needs a new write to recover from.
#[tokio::test]
async fn a_deferred_trigger_spawns_on_the_next_tick_after_an_ack() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(BASE_NS);
    // A cap of 1 makes the release edge unambiguous: one flush parks at the
    // PUT, the next trigger is refused, and reaping that one flush is the only
    // thing that can let the refused trigger through.
    let config = IngestConfig {
        max_queued_flushes: 1,
        ..capped_config()
    };
    let router = Arc::new(
        IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone())
            .with_budget(unlimited()),
    );

    let acme = tenant("acme");
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

    let first = spawn_point_write(&router, &acme, 0);
    until(|| router.metrics().snapshot().buffered_points_total >= 1).await;
    clock.advance_ns(TICK_ADVANCE_NS);
    until(|| in_flight(&router) == 1).await;
    gate.wait_until_held(1).await;

    let refused = spawn_point_write(&router, &acme, 1);
    until(|| router.metrics().snapshot().buffered_points_total >= 2).await;
    clock.advance_ns(TICK_ADVANCE_NS);
    until(|| deferred_triggers(&router) >= 1 || in_flight(&router) > 1).await;
    assert_eq!(
        in_flight(&router),
        1,
        "at a cap of 1 the second trigger must not spawn a second flush"
    );
    assert!(!refused.is_finished(), "the refused row is still buffered");

    // Ack and reap the one queued flush. Nothing else changes: no new write,
    // no configuration change.
    for id in gate.held() {
        assert!(gate.release(id));
    }
    let first_receipt = first
        .await
        .expect("first write task")
        .expect("first write acks once its PUT is released");
    assert_eq!(first_receipt.tokens.len(), 1);
    until(|| in_flight(&router) == 0 && queued_gauge(&router) == 0).await;

    // Exactly one more tick, with no new rows, flushes the deferred buffer.
    clock.advance_ns(TICK_ADVANCE_NS);
    let refused_receipt = refused
        .await
        .expect("refused write task")
        .expect("the deferred trigger re-fires on the next tick after the ack");
    assert_eq!(refused_receipt.tokens.len(), 1);
    assert_ne!(
        refused_receipt.tokens[0], first_receipt.tokens[0],
        "the deferred rows flush as their own object, with their own identity"
    );

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.flushes_by_age, 2, "both triggers eventually flush");
    assert_eq!(snapshot.acks_ok, 2);
    assert_eq!(snapshot.acks_err, 0);
    assert_eq!(
        deferred_triggers(&router),
        1,
        "exactly one trigger was refused, and it was retried rather than \
         refused again"
    );

    router.flush_all().await;
}

/// `FlushTrigger::Manual` is exempt from the cap. `flush_all` is the drain the
/// shutdown and channel-close paths use, and it has no later tick to retry a
/// refusal, so a refused Manual trigger would strand acknowledged
/// buffered-mode rows. Drives more buffered tenants than the cap allows and
/// proves the drain still writes every one of them.
#[tokio::test]
async fn a_manual_drain_is_never_refused_by_the_cap() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::new(BASE_NS);
    let config = IngestConfig {
        // No trigger but Manual can fire: the buffers never reach target_bytes
        // and the clock never advances past max_flush_delay.
        max_flush_delay: Duration::from_secs(3600),
        max_queued_flushes: 1,
        ..capped_config()
    };
    let router = Arc::new(
        IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone())
            .with_budget(unlimited()),
    );

    // More tenants than the cap, all on the one shard, so a capped Manual
    // trigger would leave residue.
    let tenants: Vec<TenantId> = (0..(CAP + DEFERRED) * 2)
        .map(|i| tenant(&format!("tenant-{i}")))
        .collect();
    for (i, t) in tenants.iter().enumerate() {
        let points = vec![make_point(
            t,
            "cpu_usage",
            &[("host", "a")],
            1_000 + i as i64,
            i as f64,
        )];
        router
            .write(
                t.clone(),
                points,
                WriteMode::Buffered,
                Duration::from_secs(5),
            )
            .await
            .expect("buffered write is accepted");
    }
    until(|| router.metrics().snapshot().buffered_points_total >= tenants.len() as u64).await;

    router.flush_all().await;

    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_manual,
        tenants.len() as u64,
        "the drain flushes every buffered tenant, cap or no cap"
    );
    assert_eq!(
        snapshot.flush_all_residue_tenants, 0,
        "a Manual trigger refused by the cap would leave residue here, which \
         on the shutdown path is lost acknowledged data"
    );
    assert_eq!(
        deferred_triggers(&router),
        0,
        "no Manual trigger is ever counted as deferred"
    );
}

// ---------------------------------------------------------------------------
// The memory-backstop exemption
// ---------------------------------------------------------------------------

/// Filler labels each heavy row carries beyond `__name__` and the `h`
/// discriminator. Short names and values, which is the shape the memory
/// backstop exists for: a `Label` is two `String` headers (48 bytes) whatever
/// the strings hold, so the buffer's resident memory climbs 51 bytes per label
/// while the object estimate climbs 3.
const HEAVY_FILLER_LABELS: usize = 17;

/// Exact buffered-memory cost of one heavy row, by `TenantBuf::merge`'s rule
/// (16 per sample, plus `size_of::<Label>()` = 48 plus name and value bytes for
/// each label of a newly seen series):
/// `16 + (48 + 8 + 3) + (48 + 1 + 3) + 17 * (48 + 2 + 1)`, for `__name__=cpu`,
/// the 3-character `h` discriminator, and the fillers.
const HEAVY_ROW_EST_BYTES: usize = 16 + 59 + 52 + HEAVY_FILLER_LABELS * 51;

/// Heavy rows buffered before the backstop fires. Chosen with
/// [`BACKSTOP_CEILING_BYTES`] so the crossing is exact rather than an overshoot:
/// row `HEAVY_ROWS - 1` leaves the buffer just under the backstop and row
/// `HEAVY_ROWS` lands on it.
const HEAVY_ROWS: usize = 16;

/// The backstop this fixture arranges: `HEAVY_ROWS` rows exactly.
const BACKSTOP_BYTES: usize = HEAVY_ROWS * HEAVY_ROW_EST_BYTES;

/// The ADR-0069 ceiling that yields [`BACKSTOP_BYTES`], since the backstop is an
/// eighth of the configured ceiling (`BUFFER_MEMORY_BACKSTOP_BUDGET_DIVISOR`).
///
/// Bounded, not `Unlimited`: under `Unlimited` the backstop is a flat 64 MiB,
/// which a test would have to make genuinely resident to cross. The exempt code
/// path is the same one either way -- it reads
/// `buffer_memory_backstop_bytes(config, ceiling)`, whichever arm produced it --
/// and this ceiling is far above everything this fixture charges, so nothing
/// sheds here either (asserted on `shed_total` and `acks_err` below).
const BACKSTOP_CEILING_BYTES: u64 = (8 * BACKSTOP_BYTES) as u64;

/// Well above the object-bytes estimate of a full heavy buffer
/// (`HEAVY_ROWS * (32 + 11 + 4 + 17 * 3 + 16)` = 1,824) and well below
/// [`BACKSTOP_BYTES`], so the target half of the size trigger cannot fire and
/// the backstop half is the only thing that can.
const HEAVY_TARGET_BYTES: usize = 8 * 1024;

/// Acceptance test for the memory-backstop exemption (PR #1903 review finding
/// 1). The queued-flush cap bounds a queue of flush TASKS; the per-(shard,
/// tenant) memory backstop is what bounds the BUFFER those tasks drain. Refusing
/// a backstop-crossing trigger at the cap trades the first bound for the loss of
/// the second, and under `IngestByteBudgetLimit::Unlimited` nothing else sheds,
/// so the buffer would then grow without any bound at all.
///
/// Fixture: a shard held at its cap by `CAP` parked flushes, then heavy rows
/// buffered one write at a time. No clock advance during the heavy phase, so no
/// age tick competes: the only trigger that can fire is the size trigger, and
/// only through its backstop half. The crossing row must spawn a flush even
/// though the shard is at `CAP`, and the buffer figure at that moment must be
/// exactly [`BACKSTOP_BYTES`] -- the count of rows that rode into the flush is
/// read back from the commit record, so "the buffer never grew past the
/// backstop" is an exact figure, not a bound.
#[tokio::test]
async fn a_backstop_crossing_flush_spawns_even_at_the_queue_cap() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(BASE_NS);
    let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(BACKSTOP_CEILING_BYTES));
    let config = IngestConfig {
        target_bytes: HEAVY_TARGET_BYTES,
        ..capped_config()
    };
    let router = Arc::new(
        IngestRouter::new(config, Arc::clone(&store), Signal::Metrics, clock.clone())
            .with_budget(Arc::clone(&budget)),
    );

    let acme = tenant("acme");
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

    // Fill the queue to the cap exactly as the acceptance test above does: one
    // small write per age tick, each spawning a flush that parks.
    let mut capped_writes = Vec::new();
    for i in 0..CAP {
        capped_writes.push(spawn_point_write(&router, &acme, i));
        let want = (i + 1) as u64;
        until(|| router.metrics().snapshot().buffered_points_total >= want).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| in_flight(&router) == want).await;
    }
    assert_eq!(
        queued_gauge(&router),
        CAP as u64,
        "the shard is at its cap before the backstop is approached"
    );

    // Everything buffered from here is the heavy tenant buffer, so the
    // buffered-bytes counter's delta from this baseline is that buffer's own
    // resident figure: nothing drains it until the backstop fires.
    let baseline_bytes = router.metrics().snapshot().buffered_bytes_total;
    let baseline_points = router.metrics().snapshot().buffered_points_total;

    let mut heavy_writes = Vec::new();
    for i in 0..HEAVY_ROWS {
        heavy_writes.push(spawn_heavy_write(&router, &acme, i));
        let want = baseline_points + (i + 1) as u64;
        until(|| router.metrics().snapshot().buffered_points_total >= want).await;
        if i + 1 < HEAVY_ROWS {
            // Under the backstop, and the object estimate is nowhere near
            // target_bytes, so no trigger fires and the cap is untouched.
            assert_eq!(
                router.metrics().snapshot().buffered_bytes_total - baseline_bytes,
                ((i + 1) * HEAVY_ROW_EST_BYTES) as u64,
                "row {i} charges exactly HEAVY_ROW_EST_BYTES to the buffer"
            );
            assert_eq!(
                in_flight(&router),
                CAP as u64,
                "no trigger fires below the backstop, so the queue is still at \
                 the cap after row {i}"
            );
        }
    }

    // The crossing row's trigger fired with the shard at its cap. Either it was
    // exempted and a flush spawned, or it was refused and the deferred counter
    // moved; wait for whichever happened rather than for the outcome under test.
    until(|| in_flight(&router) > CAP as u64 || deferred_triggers(&router) >= 1).await;

    // The headline assertion. Refusing here is what converts a bounded queue of
    // flush tasks into an unbounded buffer.
    assert_eq!(
        in_flight(&router),
        (CAP + 1) as u64,
        "a backstop-crossing trigger must spawn even at max_queued_flushes: the \
         backstop is the only bound on the buffer, and under an unlimited byte \
         budget nothing else sheds"
    );
    assert_eq!(
        queued_gauge(&router),
        (CAP + 1) as u64,
        "the gauge reports the real queue depth, which the exemption lets exceed \
         max_queued_flushes under memory pressure"
    );
    assert_eq!(
        deferred_triggers(&router),
        0,
        "the exempt path never counts a deferral: nothing was refused"
    );
    let snapshot = router.metrics().snapshot();
    assert_eq!(
        snapshot.flushes_by_size, 1,
        "the backstop is the size trigger's memory half, so the flush it opens \
         is counted as a size flush"
    );
    assert_eq!(
        snapshot.flushes_by_age, CAP as u64,
        "the heavy phase advanced no clock, so no age trigger fired in it"
    );
    assert_eq!(
        snapshot.buffered_bytes_total - baseline_bytes,
        BACKSTOP_BYTES as u64,
        "the buffer crossed the backstop on its last row and was flushed there: \
         exactly {BACKSTOP_BYTES} bytes, not one row more"
    );
    assert_eq!(
        budget.shed_total(),
        0,
        "the ceiling is eight times the backstop, so nothing shed: the flush \
         above is the exemption's work, not the byte budget's"
    );
    assert_eq!(snapshot.acks_err, 0);

    // Release the parked prefix so every flush, the exempt one last, completes.
    for id in gate.held() {
        assert!(gate.release(id));
    }
    for (i, write) in capped_writes.into_iter().enumerate() {
        write
            .await
            .expect("capped write task")
            .unwrap_or_else(|e| panic!("capped write {i} acks: {e}"));
    }
    let mut heavy_tokens = Vec::new();
    for (i, write) in heavy_writes.into_iter().enumerate() {
        let receipt = write
            .await
            .expect("heavy write task")
            .unwrap_or_else(|e| panic!("heavy write {i} acks: {e}"));
        assert_eq!(receipt.tokens.len(), 1);
        heavy_tokens.push(receipt.tokens[0].clone());
    }

    // Every heavy row rode the one exempt flush: one shared token, and its
    // commit record carries exactly HEAVY_ROWS rows. That is the exact figure
    // the buffer held when it flushed, read back from the object rather than
    // from a gauge.
    assert!(
        heavy_tokens.windows(2).all(|w| w[0] == w[1]),
        "the backstop flush took the whole buffer, so one flush acks every heavy \
         row"
    );
    let commit_key = keys::commit_key_for_token(&acme.hash(), Signal::Metrics, &heavy_tokens[0])
        .expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let decoded = record::decode(&commit_bytes).expect("decode commit record");
    assert_eq!(
        decoded.sample_count, HEAVY_ROWS as u64,
        "the exempt flush wrote the buffer at exactly the backstop: \
         {HEAVY_ROWS} rows"
    );
    assert_eq!(decoded.series_count, HEAVY_ROWS as u64);

    let final_snapshot = router.metrics().snapshot();
    assert_eq!(final_snapshot.acks_ok, (CAP + HEAVY_ROWS) as u64);
    assert_eq!(final_snapshot.acks_err, 0);

    router.flush_all().await;
}

/// Spawns one strict single-point write whose series is deliberately
/// label-heavy: `HEAVY_FILLER_LABELS` short labels, so the row costs
/// [`HEAVY_ROW_EST_BYTES`] of buffered memory against 114 object bytes. The `h`
/// discriminator is zero-padded to a fixed width so every row costs the same
/// exact figure whatever `i` is.
fn spawn_heavy_write(
    router: &Arc<IngestRouter>,
    tenant: &TenantId,
    i: usize,
) -> tokio::task::JoinHandle<Result<ravel_ingest::WriteReceipt, ravel_ingest::WriteError>> {
    let router = Arc::clone(router);
    let tenant = tenant.clone();
    tokio::spawn(async move {
        let discriminator = format!("{i:03}");
        // Two-character names for every filler, so the per-row figure stays the
        // constant the fixture's arithmetic depends on.
        let filler: Vec<(String, String)> = (0..HEAVY_FILLER_LABELS)
            .map(|n| {
                let suffix = (b'a' + n as u8) as char;
                (format!("a{suffix}"), "x".to_string())
            })
            .collect();
        let mut labels: Vec<(&str, &str)> = vec![("h", discriminator.as_str())];
        labels.extend(filler.iter().map(|(n, v)| (n.as_str(), v.as_str())));
        let points = vec![make_point(
            &tenant,
            "cpu",
            &labels,
            1_000 + i as i64,
            i as f64,
        )];
        router
            .write(tenant, points, WriteMode::Strict, Duration::from_secs(60))
            .await
    })
}

/// Spawns one strict single-point write. Each call uses its own `host` label,
/// so a buffer holding `n` of these holds `n` series and `n` samples and the
/// commit record's counts are an exact row count.
fn spawn_point_write(
    router: &Arc<IngestRouter>,
    tenant: &TenantId,
    i: usize,
) -> tokio::task::JoinHandle<Result<ravel_ingest::WriteReceipt, ravel_ingest::WriteError>> {
    let router = Arc::clone(router);
    let tenant = tenant.clone();
    tokio::spawn(async move {
        let host = format!("h{i}");
        let points = vec![make_point(
            &tenant,
            "cpu_usage",
            &[("host", host.as_str())],
            1_000 + i as i64,
            i as f64,
        )];
        router
            .write(tenant, points, WriteMode::Strict, Duration::from_secs(60))
            .await
    })
}

fn in_flight(router: &IngestRouter) -> u64 {
    router
        .metrics()
        .in_flight_flushes_by_shard()
        .into_iter()
        .map(|(_, n)| n)
        .sum()
}

fn queued_gauge(router: &IngestRouter) -> u64 {
    router
        .metrics()
        .shard_skew_by_shard()
        .into_iter()
        .map(|(_, s)| s.flushes_queued)
        .sum()
}

fn deferred_triggers(router: &IngestRouter) -> u64 {
    router
        .metrics()
        .shard_skew_by_shard()
        .into_iter()
        .map(|(_, s)| s.flush_trigger_deferred)
        .sum()
}

// ---------------------------------------------------------------------------
// Log shard actor
// ---------------------------------------------------------------------------

/// The log pipeline's copy of the acceptance test above. Same fixture, same
/// bound: `log_shard.rs` repeats the cap check rather than sharing one with
/// `shard.rs`, so it needs its own proof that the check is wired in.
#[tokio::test]
async fn log_shard_stops_spawning_at_the_cap() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(BASE_NS);
    let router = Arc::new(
        LogIngestRouter::new(capped_config(), Arc::clone(&store), clock.clone())
            .with_budget(unlimited()),
    );

    let acme = tenant("acme");
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

    let buffered = |r: &LogIngestRouter| r.metrics().snapshot().buffered_records_total;
    let in_flight = |r: &LogIngestRouter| -> u64 {
        r.metrics()
            .in_flight_flushes_by_shard()
            .into_iter()
            .map(|(_, n)| n)
            .sum()
    };
    let queued = |r: &LogIngestRouter| -> u64 {
        r.metrics()
            .shard_skew_by_shard()
            .into_iter()
            .map(|(_, s)| s.flushes_queued)
            .sum()
    };
    let deferred = |r: &LogIngestRouter| -> u64 {
        r.metrics()
            .shard_skew_by_shard()
            .into_iter()
            .map(|(_, s)| s.flush_trigger_deferred)
            .sum()
    };

    let mut capped_writes = Vec::new();
    for i in 0..CAP {
        capped_writes.push(spawn_log_write(&router, &acme, i));
        let want = (i + 1) as u64;
        until(|| buffered(&router) >= want).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| in_flight(&router) == want).await;
    }

    let mut deferred_writes = Vec::new();
    for j in 0..DEFERRED {
        deferred_writes.push(spawn_log_write(&router, &acme, CAP + j));
        let want = (CAP + j + 1) as u64;
        until(|| buffered(&router) >= want).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| deferred(&router) >= (j + 1) as u64 || in_flight(&router) > CAP as u64).await;
    }

    assert_eq!(
        in_flight(&router),
        CAP as u64,
        "the log shard must stop spawning at max_queued_flushes"
    );
    assert_eq!(queued(&router), CAP as u64);
    assert_eq!(deferred(&router), DEFERRED as u64);
    assert_eq!(router.metrics().snapshot().flushes_by_age, CAP as u64);
    for (j, write) in deferred_writes.iter().enumerate() {
        assert!(
            !write.is_finished(),
            "deferred log write {j} must still be waiting on a buffered record"
        );
    }

    for id in gate.held() {
        assert!(gate.release(id));
    }
    for (i, write) in capped_writes.into_iter().enumerate() {
        let receipt = write
            .await
            .expect("capped log write task")
            .unwrap_or_else(|e| panic!("capped log write {i} acks: {e}"));
        assert_eq!(receipt.tokens.len(), 1);
    }

    until(|| in_flight(&router) == 0 && queued(&router) == 0).await;
    clock.advance_ns(TICK_ADVANCE_NS);
    let mut deferred_tokens = Vec::new();
    for (j, write) in deferred_writes.into_iter().enumerate() {
        let receipt = write
            .await
            .expect("deferred log write task")
            .unwrap_or_else(|e| panic!("deferred log write {j} acks after the cap clears: {e}"));
        assert_eq!(receipt.tokens.len(), 1);
        deferred_tokens.push(receipt.tokens[0].clone());
    }

    assert!(
        deferred_tokens.windows(2).all(|w| w[0] == w[1]),
        "the refused records stayed in one buffer, so one flush acks them all"
    );
    let commit_key = keys::commit_key_for_token(&acme.hash(), Signal::Logs, &deferred_tokens[0])
        .expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let decoded = record::decode(&commit_bytes).expect("decode commit record");
    assert_eq!(
        decoded.sample_count, DEFERRED as u64,
        "the deferred flush writes exactly the records the cap refused"
    );

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.acks_ok, (CAP + DEFERRED) as u64);
    assert_eq!(snapshot.acks_err, 0);

    router.flush_all().await;
}

fn spawn_log_write(
    router: &Arc<LogIngestRouter>,
    tenant: &TenantId,
    i: usize,
) -> tokio::task::JoinHandle<Result<ravel_ingest::LogWriteReceipt, ravel_ingest::LogWriteError>> {
    let router = Arc::clone(router);
    let tenant = tenant.clone();
    tokio::spawn(async move {
        let record = log_record(i);
        router
            .write(
                tenant,
                vec![record],
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await
    })
}

/// One record on its own log stream, so a buffer holding `n` of these holds
/// `n` records. `stream_id` and `stream_attrs` share their inputs, which is
/// what `RlogWriter::finish`'s collision check requires.
fn log_record(i: usize) -> NormalizedLogRecord {
    let host = format!("h{i}");
    let res: Vec<(String, AttrValue)> = vec![
        (
            "service.name".to_string(),
            AttrValue::Str("api".to_string()),
        ),
        ("host".to_string(), AttrValue::Str(host)),
    ];
    let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
    NormalizedLogRecord {
        stream_id: log_stream_id(&res, "scope", "", &scope_attrs),
        stream_attrs: stream_attrs_bytes(&res, "scope", "", &scope_attrs),
        ts_ns: 1_000 + i as i64,
        observed_ts_ns: 1_000 + i as i64,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("line {i}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Span shard actor
// ---------------------------------------------------------------------------

/// The span pipeline's copy of the acceptance test. `span_shard.rs` repeats the
/// cap check too, so it gets its own proof.
#[tokio::test]
async fn span_shard_stops_spawning_at_the_cap() {
    let fault_store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let clock = TestClock::new(BASE_NS);
    let router = Arc::new(
        SpanIngestRouter::new(capped_config(), Arc::clone(&store), clock.clone())
            .with_budget(unlimited()),
    );

    let acme = tenant("acme");
    let gate = fault_store.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

    let buffered = |r: &SpanIngestRouter| r.metrics().snapshot().buffered_spans_total;
    let in_flight = |r: &SpanIngestRouter| -> u64 {
        r.metrics()
            .in_flight_flushes_by_shard()
            .into_iter()
            .map(|(_, n)| n)
            .sum()
    };
    let queued = |r: &SpanIngestRouter| -> u64 {
        r.metrics()
            .shard_skew_by_shard()
            .into_iter()
            .map(|(_, s)| s.flushes_queued)
            .sum()
    };
    let deferred = |r: &SpanIngestRouter| -> u64 {
        r.metrics()
            .shard_skew_by_shard()
            .into_iter()
            .map(|(_, s)| s.flush_trigger_deferred)
            .sum()
    };

    let mut capped_writes = Vec::new();
    for i in 0..CAP {
        capped_writes.push(spawn_span_write(&router, &acme, i));
        let want = (i + 1) as u64;
        until(|| buffered(&router) >= want).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| in_flight(&router) == want).await;
    }

    let mut deferred_writes = Vec::new();
    for j in 0..DEFERRED {
        deferred_writes.push(spawn_span_write(&router, &acme, CAP + j));
        let want = (CAP + j + 1) as u64;
        until(|| buffered(&router) >= want).await;
        clock.advance_ns(TICK_ADVANCE_NS);
        until(|| deferred(&router) >= (j + 1) as u64 || in_flight(&router) > CAP as u64).await;
    }

    assert_eq!(
        in_flight(&router),
        CAP as u64,
        "the span shard must stop spawning at max_queued_flushes"
    );
    assert_eq!(queued(&router), CAP as u64);
    assert_eq!(deferred(&router), DEFERRED as u64);
    assert_eq!(router.metrics().snapshot().flushes_by_age, CAP as u64);
    for (j, write) in deferred_writes.iter().enumerate() {
        assert!(
            !write.is_finished(),
            "deferred span write {j} must still be waiting on a buffered span"
        );
    }

    for id in gate.held() {
        assert!(gate.release(id));
    }
    for (i, write) in capped_writes.into_iter().enumerate() {
        let receipt = write
            .await
            .expect("capped span write task")
            .unwrap_or_else(|e| panic!("capped span write {i} acks: {e}"));
        assert_eq!(receipt.tokens.len(), 1);
    }

    until(|| in_flight(&router) == 0 && queued(&router) == 0).await;
    clock.advance_ns(TICK_ADVANCE_NS);
    let mut deferred_tokens = Vec::new();
    for (j, write) in deferred_writes.into_iter().enumerate() {
        let receipt = write
            .await
            .expect("deferred span write task")
            .unwrap_or_else(|e| panic!("deferred span write {j} acks after the cap clears: {e}"));
        assert_eq!(receipt.tokens.len(), 1);
        deferred_tokens.push(receipt.tokens[0].clone());
    }

    assert!(
        deferred_tokens.windows(2).all(|w| w[0] == w[1]),
        "the refused spans stayed in one buffer, so one flush acks them all"
    );
    let commit_key = keys::commit_key_for_token(&acme.hash(), Signal::Spans, &deferred_tokens[0])
        .expect("commit key");
    let commit_bytes = store
        .get(&commit_key, GetRange::Full)
        .await
        .expect("get commit record")
        .data;
    let decoded = record::decode(&commit_bytes).expect("decode commit record");
    assert_eq!(
        decoded.sample_count, DEFERRED as u64,
        "the deferred flush writes exactly the spans the cap refused"
    );

    let snapshot = router.metrics().snapshot();
    assert_eq!(snapshot.acks_ok, (CAP + DEFERRED) as u64);
    assert_eq!(snapshot.acks_err, 0);

    router.flush_all().await;
}

fn spawn_span_write(
    router: &Arc<SpanIngestRouter>,
    tenant: &TenantId,
    i: usize,
) -> tokio::task::JoinHandle<Result<ravel_ingest::SpanWriteReceipt, ravel_ingest::SpanWriteError>> {
    let router = Arc::clone(router);
    let tenant = tenant.clone();
    tokio::spawn(async move {
        let span = span_fixture(i);
        router
            .write(
                tenant,
                vec![span],
                WriteMode::Strict,
                Duration::from_secs(60),
            )
            .await
    })
}

/// One span with a distinct trace id. `shard_count: 1` routes every trace id to
/// shard 0, so the ids only need to differ, not to be searched for.
fn span_fixture(i: usize) -> NormalizedSpan {
    let mut trace_id = [0u8; 16];
    trace_id[..4].copy_from_slice(&(i as u32 + 1).to_be_bytes());
    let mut span_id = [0u8; 8];
    span_id[..4].copy_from_slice(&(i as u32 + 1).to_be_bytes());
    NormalizedSpan {
        trace_id,
        span_id,
        parent_span_id: None,
        name: format!("handle-{i}"),
        start_ts_ns: 1_000 + i as i64,
        end_ts_ns: 1_100 + i as i64,
        status_code: StatusCode::Unset,
        status_message: None,
        attrs: vec![("service.name".to_string(), "checkout".to_string())],
    }
}
