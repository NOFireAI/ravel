//! Behavioral tests for the instrumentation decorator:
//! per-operation counters and byte totals over the memory oracle, error-class
//! counters over `FaultStore` with scripted faults, latency bucketing under an
//! injected clock, and capability passthrough.
//!
//! The decorator is observability only, so every assertion here is about what
//! the counters say *and* about the wrapped backend's behavior being unchanged.
//!
//! Integration-test binary, so `[lints] workspace = true` applies (including
//! `clippy::expect_used` as a hard error under `-D warnings`); the crate-level
//! allow matches `tests/contract.rs`, since every assertion uses
//! `expect`/`expect_err` for a readable failure.
#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::instrument::{
    InstantClock, LATENCY_BUCKET_BOUNDS_MICROS, LATENCY_BUCKET_COUNT, MonotonicClock,
    OpMetricsSnapshot, StoreErrorClass, StoreMetricsSnapshot, StoreOp,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    GetRange, InstrumentedStore, ObjectStoreBackend, PutOptions, StoreError, list_all,
};

/// A clock that hands out a scripted list of readings, one per call, then
/// repeats its last value. The decorator reads it exactly twice per operation
/// (before and after the inner call), so a pair of entries scripts one
/// operation's measured latency.
struct ScriptedClock {
    readings: Vec<u64>,
    next: AtomicUsize,
}

impl ScriptedClock {
    fn new(readings: Vec<u64>) -> Self {
        assert!(!readings.is_empty(), "a scripted clock needs a reading");
        ScriptedClock {
            readings,
            next: AtomicUsize::new(0),
        }
    }

    /// How many readings have been consumed, so a test can prove the decorator
    /// timed exactly the calls it made and no others.
    fn reads(&self) -> usize {
        self.next.load(Ordering::SeqCst)
    }
}

impl MonotonicClock for ScriptedClock {
    fn now_nanos(&self) -> u64 {
        let index = self.next.fetch_add(1, Ordering::SeqCst);
        let last = self.readings.len() - 1;
        self.readings[index.min(last)]
    }
}

fn instrumented_memory() -> InstrumentedStore<MemoryStore> {
    InstrumentedStore::new(MemoryStore::new())
}

/// Asserts an operation's block field by field, so an unexpected counter
/// movement (a stray call, a byte total on an op that must not move one)
/// fails rather than passing under a looser check.
fn assert_op(
    snap: &OpMetricsSnapshot,
    op: StoreOp,
    calls: u64,
    ok: u64,
    bytes: u64,
    errors: &[(StoreErrorClass, u64)],
) {
    let name = op.name();
    assert_eq!(snap.calls, calls, "{name}.calls");
    assert_eq!(snap.ok, ok, "{name}.ok");
    assert_eq!(snap.bytes, bytes, "{name}.bytes");
    for class in StoreErrorClass::ALL {
        let expected = errors
            .iter()
            .find(|(c, _)| *c == class)
            .map_or(0, |(_, n)| *n);
        assert_eq!(
            snap.error_count(class),
            expected,
            "{name}.errors[{}]",
            class.name()
        );
    }
    assert_eq!(
        snap.errors_total(),
        calls - ok,
        "{name}: every failed call must land in exactly one error class"
    );
    let histogram: u64 = snap.latency_micros_buckets.iter().sum();
    assert_eq!(
        histogram, calls,
        "{name}: every completed call must be bucketed once, successes and failures alike"
    );
}

/// A scripted sequence over the memory oracle: each operation kind advances
/// `calls` and `ok`, and the byte counters follow the payload (`put`) and the
/// data actually returned (`get`), not the object size.
#[tokio::test]
async fn every_op_kind_advances_its_counters_over_the_memory_oracle() {
    let store = instrumented_memory();
    let metrics = store.metrics();

    // 2 puts: 10 bytes then 3 bytes.
    store
        .put(
            "p/a",
            Bytes::from_static(b"0123456789"),
            PutOptions::default(),
        )
        .await
        .expect("put a");
    store
        .put("p/b", Bytes::from_static(b"xyz"), PutOptions::default())
        .await
        .expect("put b");

    // 3 gets: full (10), a 3-byte range, and a 4-byte suffix. The ranged
    // reads must count the bytes handed back, not the 10-byte object.
    let full = store.get("p/a", GetRange::Full).await.expect("get full");
    assert_eq!(
        &full.data[..],
        b"0123456789",
        "results pass through unchanged"
    );
    store
        .get("p/a", GetRange::Range(2, 5))
        .await
        .expect("get range");
    store
        .get("p/a", GetRange::Suffix(4))
        .await
        .expect("get suffix");

    // 1 head, 1 list page (2 keys fit in the oracle's default 1000-key page,
    // so `list_all` stops after one call), 1 delimited list, 1 delete.
    store.head("p/a").await.expect("head");
    let listed = list_all(&store, "p/").await.expect("list_all");
    assert_eq!(listed.len(), 2, "both keys listed");
    store.list_delimited("p/").await.expect("list_delimited");
    store.delete("p/b").await.expect("delete");

    let snap = metrics.snapshot();
    assert_op(&snap.put, StoreOp::Put, 2, 2, 13, &[]);
    assert_op(&snap.get, StoreOp::Get, 3, 3, 10 + 3 + 4, &[]);
    assert_op(&snap.head, StoreOp::Head, 1, 1, 0, &[]);
    assert_op(&snap.list, StoreOp::List, 1, 1, 0, &[]);
    assert_op(&snap.list_delimited, StoreOp::ListDelimited, 1, 1, 0, &[]);
    assert_op(&snap.delete, StoreOp::Delete, 1, 1, 0, &[]);

    // The wrapped backend really did the work: the deleted key is gone.
    assert!(matches!(
        store.inner().head("p/b").await,
        Err(StoreError::NotFound)
    ));
}

/// `list` counts pages, not keys: one call is one page, so a paginated drain
/// advances the counter once per page.
#[tokio::test]
async fn list_counts_one_call_per_page() {
    let store = InstrumentedStore::new(MemoryStore::with_page_size(2));
    let metrics = store.metrics();
    for i in 0..5 {
        store
            .put(
                &format!("k/{i}"),
                Bytes::from_static(b"v"),
                PutOptions::default(),
            )
            .await
            .expect("put");
    }
    let all = list_all(&store, "k/").await.expect("list_all");
    assert_eq!(all.len(), 5);
    // 5 keys at 2 per page: pages of 2, 2, 1, and the third page ends the
    // drain by reporting `next: None`.
    assert_eq!(metrics.snapshot().list.calls, 3, "one call per listed page");
}

/// Errors are classified per `StoreError` variant, including the protocol
/// signal `AlreadyExists`, and a failing call still lands in the histogram.
#[tokio::test]
async fn error_classes_from_the_oracle_include_protocol_signals() {
    let store = instrumented_memory();
    let metrics = store.metrics();

    store
        .put(
            "k",
            Bytes::from_static(b"first"),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("first create-if-absent");
    let err = store
        .put(
            "k",
            Bytes::from_static(b"second"),
            PutOptions::create_if_absent(),
        )
        .await
        .expect_err("second create-if-absent must fail");
    assert!(matches!(err, StoreError::AlreadyExists), "got {err:?}");

    let err = store
        .get("k", GetRange::Suffix(0))
        .await
        .expect_err("Suffix(0) must be InvalidRange");
    assert!(matches!(err, StoreError::InvalidRange(_)), "got {err:?}");
    let err = store
        .head("missing")
        .await
        .expect_err("head of a missing key must be NotFound");
    assert!(matches!(err, StoreError::NotFound), "got {err:?}");

    let snap = metrics.snapshot();
    // The rejected put's payload is still counted: 5 bytes offered and
    // accepted, 6 offered and refused.
    assert_op(
        &snap.put,
        StoreOp::Put,
        2,
        1,
        11,
        &[(StoreErrorClass::AlreadyExists, 1)],
    );
    assert_op(
        &snap.get,
        StoreOp::Get,
        1,
        0,
        0,
        &[(StoreErrorClass::InvalidRange, 1)],
    );
    assert_op(
        &snap.head,
        StoreOp::Head,
        1,
        0,
        0,
        &[(StoreErrorClass::NotFound, 1)],
    );
}

/// Scripted faults, one per class, with `FaultStore`'s own counters asserted
/// so the test proves the faults fired rather than assuming they did.
#[tokio::test]
async fn scripted_faults_advance_the_matching_error_class() {
    let plan = FaultPlan::empty()
        .with_rule(
            Rule::new(Op::Get, ScriptedFault::Throttled { retry_after_ms: 7 })
                .with_key_contains("throttled"),
        )
        .with_rule(Rule::new(Op::Head, ScriptedFault::NotFoundBlip).with_key_contains("blip"))
        .with_rule(
            Rule::new(Op::Put, ScriptedFault::Transient("dropped".into()))
                .with_key_contains("transient"),
        );
    let store = InstrumentedStore::new(FaultStore::new(MemoryStore::new(), plan));
    let metrics = store.metrics();

    // An object the throttled get would otherwise find, written under a key
    // no rule matches, so the setup put stays clean.
    store
        .put(
            "ok/k",
            Bytes::from_static(b"payload"),
            PutOptions::default(),
        )
        .await
        .expect("unfaulted put");

    let err = store
        .get("throttled/k", GetRange::Full)
        .await
        .expect_err("scripted throttle must fail the get");
    assert!(
        matches!(err, StoreError::Throttled { retry_after_ms: 7 }),
        "the decorator must forward the error unchanged, got {err:?}"
    );
    let err = store
        .head("blip/k")
        .await
        .expect_err("scripted NotFound blip must fail the head");
    assert!(matches!(err, StoreError::NotFound), "got {err:?}");
    let err = store
        .put(
            "transient/k",
            Bytes::from_static(b"1234"),
            PutOptions::default(),
        )
        .await
        .expect_err("scripted transient must fail the put");
    assert!(matches!(err, StoreError::Transient(_)), "got {err:?}");

    // The faults really fired (FaultStore's own counters, not inferred from
    // the errors above).
    let fault_store = store.inner();
    assert_eq!(
        fault_store.fault_count(Op::Get, FaultKind::Throttled),
        1,
        "throttle fault must have fired"
    );
    assert_eq!(
        fault_store.fault_count(Op::Head, FaultKind::NotFoundBlip),
        1,
        "NotFound-blip fault must have fired"
    );
    assert_eq!(
        fault_store.fault_count(Op::Put, FaultKind::Transient),
        1,
        "transient fault must have fired"
    );

    let snap = metrics.snapshot();
    // put: the clean 7-byte setup write plus the 4-byte payload the transient
    // fault refused (offered bytes are counted either way).
    assert_op(
        &snap.put,
        StoreOp::Put,
        2,
        1,
        11,
        &[(StoreErrorClass::Transient, 1)],
    );
    assert_op(
        &snap.get,
        StoreOp::Get,
        1,
        0,
        0,
        &[(StoreErrorClass::Throttled, 1)],
    );
    assert_op(
        &snap.head,
        StoreOp::Head,
        1,
        0,
        0,
        &[(StoreErrorClass::NotFound, 1)],
    );
}

/// The latency histogram follows the injected clock exactly: one bucketed
/// observation per completed call, chosen by the scripted elapsed time, with
/// boundary values landing in the lower bucket and an overflow past the last
/// bound.
#[tokio::test]
async fn latency_buckets_follow_the_injected_clock() {
    // Reading pairs, in call order: 50us, exactly 100us (bucket 0's inclusive
    // bound), 300us (bucket 1), and 6s (past the 5s bound, so overflow).
    let clock = Arc::new(ScriptedClock::new(vec![
        0,
        50_000,
        1_000_000,
        1_000_000 + 100_000,
        2_000_000,
        2_000_000 + 300_000,
        3_000_000,
        3_000_000 + 6_000_000_000,
    ]));
    let store = InstrumentedStore::with_clock(MemoryStore::new(), clock.clone());
    let metrics = store.metrics();

    for i in 0..4 {
        store
            .put(
                &format!("k{i}"),
                Bytes::from_static(b"v"),
                PutOptions::default(),
            )
            .await
            .expect("put");
    }

    assert_eq!(
        clock.reads(),
        8,
        "the decorator must read the clock exactly twice per call"
    );
    let mut expected = [0u64; LATENCY_BUCKET_COUNT];
    expected[0] = 2; // 50us and 100us
    expected[1] = 1; // 300us, inside the 500us bound
    expected[LATENCY_BUCKET_COUNT - 1] = 1; // 6s, past the last bound
    let snap = metrics.snapshot();
    assert_eq!(
        snap.put.latency_micros_buckets, expected,
        "bounds are {LATENCY_BUCKET_BOUNDS_MICROS:?} micros, upper-bound inclusive"
    );
    assert_eq!(
        snap.put.latency_nanos_total,
        50_000 + 100_000 + 300_000 + 6_000_000_000,
        "the summed latency must be the scripted elapsed times"
    );
    // No other op moved, so nothing was measured that was not called.
    for op in [
        StoreOp::Get,
        StoreOp::Head,
        StoreOp::List,
        StoreOp::ListDelimited,
        StoreOp::Delete,
    ] {
        assert_eq!(
            *snap.op(op),
            OpMetricsSnapshot::default(),
            "{} must be untouched",
            op.name()
        );
    }
}

/// The default clock is real, monotonic, and `Instant`-based: two readings
/// never go backwards, and a completed call lands in some bucket.
#[tokio::test]
async fn default_clock_is_monotonic_and_buckets_real_calls() {
    let clock = InstantClock::new();
    let first = clock.now_nanos();
    let second = clock.now_nanos();
    assert!(second >= first, "InstantClock must not go backwards");

    let store = instrumented_memory();
    let metrics = store.metrics();
    store
        .delete("never-written")
        .await
        .expect("idempotent delete");
    let snap = metrics.snapshot();
    assert_eq!(snap.delete.calls, 1);
    assert_eq!(
        snap.delete.latency_micros_buckets.iter().sum::<u64>(),
        1,
        "a real call must be bucketed exactly once"
    );
}

/// Capability passthrough: the decorator reports the wrapped backend's set
/// verbatim, flag for flag, so the server's startup gate (which runs against
/// the decorator) still sees the backend's own declaration. Asserted as an
/// exact equality, not `satisfies`, so a decorator that flattened the set to
/// `mandatory()` would fail here (`MemoryStore` reports `upload_checksum:
/// true`, which the mandatory set does not require).
#[test]
fn capabilities_pass_through_verbatim() {
    let inner = MemoryStore::new();
    let expected = inner.capabilities();
    let store = InstrumentedStore::new(inner);
    assert_eq!(store.capabilities(), expected);
    assert!(
        expected.upload_checksum,
        "precondition: MemoryStore reports more than the mandatory set, so this \
         equality is not vacuous"
    );
}

/// A store nobody called reports all-zero counters, so a nonzero counter is
/// always evidence of a call.
#[test]
fn fresh_metrics_are_zero() {
    let store = instrumented_memory();
    assert_eq!(store.metrics().snapshot(), StoreMetricsSnapshot::default());
}

/// The handle is shared, not copied: a clone taken before any call observes
/// calls made afterward.
#[tokio::test]
async fn cloned_handles_observe_the_same_counters() {
    let store = instrumented_memory();
    let early = store.metrics();
    store
        .put("k", Bytes::from_static(b"v"), PutOptions::default())
        .await
        .expect("put");
    let late = store.metrics();
    assert_eq!(early.snapshot(), late.snapshot());
    assert_eq!(early.snapshot().put.calls, 1);
    assert!(
        Arc::ptr_eq(&early, &late),
        "metrics() must clone one shared handle, not build a new one"
    );
}

/// Attempts are `None`, not zero, for a backend with no attempt-observing layer
/// (#928), and a fault injected *above* the store cannot change that.
///
/// This is the boundary of what `FaultStore` can show about retries.
/// `object_store`'s retry loop is inside its client, below the
/// `ObjectStoreBackend` trait, so a `FaultStore` rule fires in place of the
/// wrapped call and its error goes straight to the caller: three throttled gets
/// here are three failed calls the caller made, never one call retried three
/// times. The counter says so rather than reporting a zero (or a
/// call-shaped count) that would read as "no retries happened"; the retried-call
/// case is pinned over real HTTP in `tests/s3_http_faults.rs`.
#[tokio::test]
async fn attempts_are_unobserved_for_a_backend_that_reports_none() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::Throttled { retry_after_ms: 3 })
            .with_key_contains("throttled"),
    );
    let store = InstrumentedStore::new(FaultStore::new(MemoryStore::new(), plan));
    let metrics = store.metrics();

    for _ in 0..3 {
        let err = store
            .get("throttled/k", GetRange::Full)
            .await
            .expect_err("the scripted throttle must fail the get");
        assert!(matches!(err, StoreError::Throttled { retry_after_ms: 3 }));
    }

    assert_eq!(
        store.inner().fault_count(Op::Get, FaultKind::Throttled),
        3,
        "the fault must have fired on each call, not been assumed to"
    );

    let snapshot = metrics.snapshot();
    assert_eq!(
        snapshot.get.calls, 3,
        "three failed calls, each one the caller's own"
    );
    assert!(
        !snapshot.attempts_observed,
        "MemoryStore under FaultStore has no layer that sees requests"
    );
    for op in StoreOp::ALL {
        assert_eq!(
            snapshot.attempts(op),
            None,
            "{}: an unobserved attempt count must not read as a number",
            op.name()
        );
        assert_eq!(snapshot.retries(op), None, "{}: nor its retries", op.name());
    }
    assert_eq!(snapshot.attempts_total(), None);
}
