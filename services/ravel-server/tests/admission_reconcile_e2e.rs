//! Fleet-global admission reconciliation acceptance test (ADR-0057, EF-T1,
//! issue #677). The epic's proof that the capability works end to end: two
//! independent admission controllers, each fronting its own real ingest write
//! path over one shared object store, hold a single fleet-wide active-series
//! cap that neither would hold alone.
//!
//! # Why this drives the real router + `handle_export` with an injected read
//! # clock instead of two `ravel_server::start` servers over real timers
//!
//! Reconciliation is a background loop on interval `R` (default 10s) driven off
//! `SystemClock` inside `ravel_server::start`; neither `R` nor the clock is
//! injectable through the server's public surface, so crossing a reconciliation
//! boundary through two live HTTP servers would mean either real multi-second
//! sleeps (slow, and the pre-reconciliation overshoot half becomes a race) or
//! shrinking `R` so far the test measures timer jitter. That is a property of
//! the shipped lifecycle, not a gap in the mechanism.
//!
//! So this test drives the **real production ingest admission path** — the same
//! `ravel_server::ingest::handle_export` the OTLP HTTP and gRPC handlers call,
//! which runs the real `AdmissionController` layer-4 checks internally (series-
//! creation rate then the active-series cap) before a real `IngestRouter` write
//! over a real `MemoryStore` — and invokes the **real reconciliation cycle**
//! (`ravel_ingest::reconcile_once`, exactly what the server's background task
//! calls each tick) at chosen instants. Nothing calls the controller's check
//! functions directly; traffic goes through `handle_export`. The only thing
//! bypassed is the HTTP transport and the background task's `sleep` loop,
//! neither of which carries reconciliation-correctness logic.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ravel_ingest::{
    AdmissionController, AdmissionLimits, Clock, CountLimit, IngestConfig, IngestRouter, RateLimit,
    SystemClock, WriteMode, reconcile_once,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::IngestLimits;
use ravel_server::ingest::{IngestState, handle_export};
use ravel_types::{Signal, TenantId};

const TENANT: &str = "acme";
/// The single fleet-wide active-series cap. Sized so neither process alone hits
/// it under its round-1 load (60 < 100) but their combined real traffic exceeds
/// it (120 > 100), which is the exact condition ADR-0057 exists to bound.
const FLEET_CAP: u64 = 100;
/// The reconciliation interval `R` this test reconciles under. Only its value
/// (for the `2 * R` staleness window) matters here, since the cycle is driven
/// explicitly rather than on a timer; every snapshot is written and read at the
/// same injected instant, so nothing is ever stale.
const R: Duration = Duration::from_secs(10);
const ACK: Duration = Duration::from_secs(5);
/// A fixed ingest instant. The metric data points carry this exact time, so the
/// ADR-0051 event-time skew check (which runs inside `handle_export` before
/// admission) never rejects a point for skew; only the active-series cap gates.
const TS_NS: i64 = 1_000 * 3_600_000_000_000;

/// Limits with only the active-series cap bounded (creation rate and byte rate
/// unlimited), so the reachability property is exercised through the count cap
/// alone — the cap ADR-0057's Context, formula, and overshoot bound are about.
fn cap_only_limits(cap: u64) -> AdmissionLimits {
    AdmissionLimits {
        max_active_series: CountLimit::Bounded(cap),
        max_active_streams: CountLimit::Bounded(cap),
        ingest_byte_rate: RateLimit::Unlimited,
        series_creation_rate: RateLimit::Unlimited,
    }
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

/// One request carrying `count` distinct series with names `<prefix>_<i>`, each
/// one gauge point at `TS_NS`. Distinct `__name__` yields distinct series ids,
/// so each request demands `count` fresh identities from the active-series cap.
fn export_request(prefix: &str, count: usize) -> ExportMetricsServiceRequest {
    let metrics = (0..count)
        .map(|i| Metric {
            name: format!("{prefix}_{i}"),
            data: Some(MetricData::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: TS_NS as u64,
                    value: Some(NumberValue::AsDouble(1.0)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "flood")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// One process's real ingest admission path over `store`: its own
/// `AdmissionController` (fleet cap `FLEET_CAP`) and its own `IngestRouter`,
/// wired into an `IngestState` exactly as `ravel_server::start` wires the
/// gateway handlers' state.
fn build_process(store: Arc<dyn ObjectStoreBackend>) -> (Arc<AdmissionController>, IngestState) {
    let admission = Arc::new(AdmissionController::new(
        Arc::new(SystemClock),
        cap_only_limits(FLEET_CAP),
    ));
    admission.set_tenant_limits(TenantId::new(TENANT), cap_only_limits(FLEET_CAP));
    let router = Arc::new(IngestRouter::new(
        IngestConfig {
            shard_count: 1,
            ..IngestConfig::default()
        },
        store,
        Signal::Metrics,
        Arc::new(SystemClock),
    ));
    let state = IngestState {
        router,
        limits: IngestLimits::default(),
        ack_deadline: ACK,
        admission: admission.clone(),
        recovery: None,
        provisioning: None,
    };
    (admission, state)
}

/// Drive `count` fresh series named `<prefix>_*` through the real ingest path
/// and return how many were rejected by the active-series cap (the OTLP
/// partial-success count; 0 when the whole batch was admitted).
async fn drive(state: &IngestState, prefix: &str, count: usize) -> i64 {
    let outcome = handle_export(
        state,
        TenantId::new(TENANT),
        WriteMode::Buffered,
        export_request(prefix, count),
        TS_NS,
    )
    .await
    .expect("buffered ingest never fails on a cap breach (partial success)");
    outcome
        .response
        .partial_success
        .map(|p| p.rejected_data_points)
        .unwrap_or(0)
}

/// This process's current active-series count for `acme`, read from the same
/// usage snapshot `/metrics` renders.
fn active_series(admission: &AdmissionController) -> u64 {
    let hash = TenantId::new(TENANT).hash();
    admission
        .usage_snapshot()
        .iter()
        .find(|u| u.tenant_hash == hash && u.signal == Signal::Metrics)
        .map(|u| u.active_series)
        .unwrap_or(0)
}

/// THE reachability test (ADR-0057, EF-T1). Two processes share one fleet-wide
/// active-series cap of 100 over one real `MemoryStore`. Before reconciliation
/// each enforces the cap independently, so their combined real traffic
/// overshoots it (120 > 100 — the bounded overshoot ADR-0057 section 4 admits
/// exists). After one reconciliation cycle has run on both, each process's
/// effective cap has been driven down to leave room only for the fleet
/// remainder, so further fresh traffic is refused: the fleet total is bounded
/// near the configured cap, NOT the 2x (200) two independent per-process caps
/// would have allowed.
#[tokio::test]
async fn fleet_cap_holds_across_two_processes_after_reconciliation() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let (admission_a, state_a) = build_process(store.clone());
    let (admission_b, state_b) = build_process(store.clone());

    // ---- Before reconciliation: bounded overshoot is possible ----
    //
    // Each process admits 60 fresh series (well under its local cap of 100), so
    // neither rejects anything, but the fleet-wide distinct total is 120 — over
    // the configured cap of 100. This is exactly the N x quota gap ADR-0057
    // exists to close: with independent per-process enforcement the cap does not
    // hold fleet-wide.
    assert_eq!(drive(&state_a, "a", 60).await, 0, "A admits its full 60");
    assert_eq!(drive(&state_b, "b", 60).await, 0, "B admits its full 60");
    assert_eq!(active_series(&admission_a), 60);
    assert_eq!(active_series(&admission_b), 60);
    let fleet_before = active_series(&admission_a) + active_series(&admission_b);
    assert!(
        fleet_before > FLEET_CAP,
        "before reconciliation the fleet total {fleet_before} overshoots the cap {FLEET_CAP} \
         (each process enforced independently) — the overshoot ADR-0057 bounds"
    );

    // ---- Reconcile both processes ----
    //
    // A writes its snapshot (no sibling yet, so its cap stays at 100); B then
    // writes its own and reads A's (its cap becomes 60 + max(0, 100-60-60) = 60);
    // a second pass on A reads B's snapshot and drives A's cap to 60 too. Every
    // snapshot is written and read at the same instant `TS_NS`, so none is stale.
    reconcile_once(admission_a.as_ref(), store.as_ref(), R, TS_NS).await;
    reconcile_once(admission_b.as_ref(), store.as_ref(), R, TS_NS).await;
    reconcile_once(admission_a.as_ref(), store.as_ref(), R, TS_NS).await;

    // ---- After reconciliation: admission bounds to the fleet cap ----
    //
    // Both processes now see fleet_used = 120 >= cap 100, so each has a local
    // soft threshold equal to its own current usage (60): no room for new
    // series. Driving 60 MORE fresh series through each real ingest path is
    // therefore fully rejected — WITHOUT reconciliation each would have admitted
    // up to its independent cap (40 more each, fleet -> 200); WITH it, the fleet
    // stays put.
    assert_eq!(
        drive(&state_a, "a2", 60).await,
        60,
        "post-reconciliation A refuses every fresh series: its soft threshold left no room"
    );
    assert_eq!(
        drive(&state_b, "b2", 60).await,
        60,
        "post-reconciliation B refuses every fresh series too"
    );
    assert_eq!(
        active_series(&admission_a),
        60,
        "A did not grow past its reconciled threshold"
    );
    assert_eq!(
        active_series(&admission_b),
        60,
        "B did not grow past its reconciled threshold"
    );

    let fleet_after = active_series(&admission_a) + active_series(&admission_b);
    assert_eq!(
        fleet_after, fleet_before,
        "reconciliation stopped fleet-wide growth: no new series admitted on either process"
    );
    assert!(
        fleet_after < 2 * FLEET_CAP,
        "the fleet total {fleet_after} is bounded near the configured cap, not the 2x ({}) two \
         independent per-process caps would have allowed",
        2 * FLEET_CAP
    );
}

// ---------------------------------------------------------------------------
// Rate-cap reconciliation (ADR-0057 section 2, rate caps).
//
// The count-cap test above sets every rate cap to `Unlimited`, so it never
// exercised `reconcile_rate`. This section drives a real bounded
// `ingest_byte_rate` cap across two processes over multiple reconciliation
// intervals and asserts the combined admitted throughput converges to the
// fleet-wide cap -- the case whose absence let the original additive-headroom
// rate bug ship green.
//
// Byte-rate admission (ADR-0051 layer 2) is charged by the transport handlers
// on wire bytes, ahead of `handle_export`, via `AdmissionController::
// check_byte_rate`. That is the real production byte-rate hot-path function;
// this test calls it directly (the transport is what this test file already
// bypasses, exactly as the count-cap test bypasses HTTP and drives
// `handle_export`). The reconciliation cycle is the real `reconcile_once`.
// ---------------------------------------------------------------------------

/// The fleet-wide ingest byte-rate cap, in bytes/sec. Sized so two processes
/// each saturating it before reconciliation sustain 2x the cap (the overshoot),
/// and their equal shares after reconciliation sum back to exactly the cap.
const BYTE_CAP: u64 = 1000;
/// Token-bucket burst, equal to one second of the cap: small enough that a
/// per-second drain never accumulates more than one second's worth, so the
/// per-second admitted count tracks `rate_per_sec` rather than a large stored
/// burst.
const BYTE_BURST: u64 = 1000;
/// Per-request size the driver charges. Ten of these fit one second at the full
/// cap, five at the reconciled half share, so admitted-byte counts are exact.
const CHUNK: u64 = 100;
const SEC_NS: i64 = 1_000_000_000;
/// The reconciliation interval in whole seconds (matches `R`).
const R_SECS: i64 = 10;

/// An injected clock the byte-rate test drives one second at a time, so the
/// token buckets refill deterministically and reconciliation lands on exact
/// interval boundaries (`SystemClock`, used by the count-cap test, cannot do
/// this -- token refill needs controllable elapsed time).
struct TestClock(AtomicI64);
impl TestClock {
    fn new(now_ns: i64) -> Arc<Self> {
        Arc::new(TestClock(AtomicI64::new(now_ns)))
    }
    fn advance(&self, by_ns: i64) -> i64 {
        self.0.fetch_add(by_ns, Ordering::SeqCst) + by_ns
    }
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}
impl Clock for TestClock {
    fn now_ns(&self) -> i64 {
        self.now()
    }
}

/// Limits with only the ingest byte-rate bounded; counts and creation rate
/// unlimited, so this test exercises `reconcile_rate` alone.
fn byte_rate_limits() -> AdmissionLimits {
    AdmissionLimits {
        max_active_series: CountLimit::Unlimited,
        max_active_streams: CountLimit::Unlimited,
        ingest_byte_rate: RateLimit::Bounded {
            per_sec: BYTE_CAP,
            burst: BYTE_BURST,
        },
        series_creation_rate: RateLimit::Unlimited,
    }
}

/// One byte-rate-capped process on the shared `clock`: its own controller with
/// the fleet-wide `BYTE_CAP`, tenant limits installed.
fn build_byte_process(clock: Arc<TestClock>) -> Arc<AdmissionController> {
    let admission = Arc::new(AdmissionController::new(clock, byte_rate_limits()));
    admission.set_tenant_limits(TenantId::new(TENANT), byte_rate_limits());
    admission
}

/// Saturate one process's byte-rate bucket at `now_ns`: offer far more than one
/// second of the cap in `CHUNK`-sized requests and return how many bytes were
/// admitted before the bucket rejected. This is the real layer-2 hot-path
/// charge, whole-request per chunk (a rejected chunk consumes no tokens).
fn saturate_bytes(admission: &AdmissionController, now_ns: i64) -> u64 {
    let mut admitted = 0u64;
    // 100 chunks = 10 000 bytes offered, an order of magnitude over any
    // one-second refill, so the loop always drains the bucket to rejection.
    for _ in 0..100 {
        if admission
            .check_byte_rate(&TenantId::new(TENANT), Signal::Metrics, CHUNK, now_ns)
            .is_ok()
        {
            admitted += CHUNK;
        } else {
            break;
        }
    }
    admitted
}

/// One reconciliation round over two processes (A writes; B writes and reads A;
/// A reads B), leaving both with an up-to-date view of `N = 2`, mirroring the
/// count-cap test's three-call convergence.
async fn reconcile_round(
    a: &AdmissionController,
    b: &AdmissionController,
    store: &dyn ObjectStoreBackend,
    now_ns: i64,
) {
    reconcile_once(a, store, R, now_ns).await;
    reconcile_once(b, store, R, now_ns).await;
    reconcile_once(a, store, R, now_ns).await;
}

/// Fleet-wide byte-rate cap holds across two processes over multiple intervals
/// (ADR-0057 section 2, rate caps). Two processes share one `BYTE_CAP` fleet
/// byte-rate cap over one real store. Both saturate their buckets every second;
/// reconciliation runs at each interval boundary. Before the first
/// reconciliation each enforces the full cap independently, so their combined
/// throughput is the documented 2x overshoot; after it, the equal-share formula
/// pins each to `BYTE_CAP / 2` and their combined admitted throughput holds at
/// the fleet cap for every subsequent interval -- not just the first.
///
/// This is the test the original bug evaded: under the old additive-headroom
/// rate formula each process would keep reading its own current (full-cap) rate
/// as headroom-neutral and never reduce its threshold, so the combined
/// throughput would stay at 2x the cap for every interval and the
/// post-convergence assertions below would fail.
#[tokio::test]
async fn fleet_byte_rate_holds_across_two_processes_over_multiple_intervals() {
    let base = TS_NS;
    let clock = TestClock::new(base);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let admission_a = build_byte_process(clock.clone());
    let admission_b = build_byte_process(clock.clone());

    // Four intervals of R_SECS seconds each. Interval 0 runs before any
    // reconciliation (the overshoot); intervals 1..=3 run after both processes
    // have converged to their equal share.
    let mut per_interval_combined = Vec::new();
    for _ in 0..4 {
        let mut combined = 0u64;
        for _ in 0..R_SECS {
            let now = clock.advance(SEC_NS);
            combined += saturate_bytes(admission_a.as_ref(), now);
            combined += saturate_bytes(admission_b.as_ref(), now);
        }
        per_interval_combined.push(combined);
        // Reconcile at the interval boundary; snapshots are written and read at
        // the same instant, all within the 2*R staleness window.
        reconcile_round(
            admission_a.as_ref(),
            admission_b.as_ref(),
            store.as_ref(),
            clock.now(),
        )
        .await;
    }

    // ---- Interval 0: the documented pre-reconciliation overshoot ----
    let cap_per_interval = BYTE_CAP * R_SECS as u64;
    assert_eq!(
        per_interval_combined[0],
        2 * cap_per_interval,
        "before reconciliation both processes enforce the full cap independently, so combined \
         throughput is the 2x overshoot ADR-0057 section 4 bounds"
    );

    // ---- Intervals 1..=3: combined throughput holds at the fleet cap ----
    //
    // Each post-convergence interval's combined admitted bytes is at most one
    // interval's worth of the fleet cap (plus at most one bucket's burst of
    // slack, which the small burst keeps negligible), and the sum across all
    // three stays under the fleet cap's aggregate -- NOT the 2x the old formula
    // would have sustained on every interval.
    let post: u64 = per_interval_combined[1..].iter().sum();
    let post_intervals = 3u64;
    assert!(
        post <= cap_per_interval * post_intervals + BYTE_BURST,
        "combined post-reconciliation throughput {post} over {post_intervals} intervals must stay \
         within the fleet cap ({} + one burst {BYTE_BURST}); the old formula would sustain ~{}",
        cap_per_interval * post_intervals,
        2 * cap_per_interval * post_intervals,
    );
    for (i, &combined) in per_interval_combined[1..].iter().enumerate() {
        assert!(
            combined <= cap_per_interval + BYTE_BURST,
            "post-reconciliation interval {} combined throughput {combined} exceeds the fleet cap \
             per interval {cap_per_interval} (+burst {BYTE_BURST})",
            i + 1,
        );
    }
}
