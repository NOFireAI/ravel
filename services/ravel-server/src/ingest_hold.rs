//! ADR-3718 (flush-backlog hold amendment, #3756): hold ingest requests at the
//! head while the node's flush backlog is over a bound.
//!
//! A background detector (spawned in [`crate::start`] when
//! `--ingest-hold-max-wait` is above zero) samples each ingest router's queued
//! flush count and the routers' deferred-flush-trigger counters every 100 ms
//! and publishes one congested flag. `admit_ingest_request` and
//! `GrpcIngestAdmissionLayer::call` call [`IngestHoldGate::hold`] right after
//! the in-flight permit and before tenant resolution: not congested, the
//! request proceeds at the cost of one relaxed load; congested, it waits in
//! 10 ms steps, keeping its permit, until the flag clears or
//! `--ingest-hold-max-wait` runs out, and is refused then. An OTAP stream
//! holds once per `BatchArrowRecords` batch in `process_batch`, after its own
//! in-flight permit; its tenant was resolved when the stream opened.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use ravel_ingest::{Clock, LatencyHistogramSnapshot};
use ravel_object_store::instrument::{LATENCY_BUCKET_COUNT, latency_bucket};
use tokio::task::JoinHandle;

use crate::config::IngestHoldConfig;

/// One sample every 100 ms (ADR-3718 D1).
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
/// The deferred-trigger window: 1 s of samples at the 100 ms interval.
const WINDOW_SAMPLES: usize = 10;
/// Deferred triggers per full 1 s window that count as congested: the GKE
/// traces at 140k on 8 vCPU show 0 to 0.1/s healthy and about 85/s collapsed.
pub const DEFERRED_TRIGGERS_PER_WINDOW: u64 = 10;
/// A held request re-reads the congested flag this often.
const HOLD_STEP: Duration = Duration::from_millis(10);

/// How a held request left the hold, for `ravel_ingest_head_hold_total`'s
/// `outcome` label.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldOutcome {
    /// The flag cleared within the max wait and the request went on.
    Admitted,
    /// The max wait ran out and the request was refused.
    Refused,
}

impl HoldOutcome {
    pub const ALL: [HoldOutcome; 2] = [HoldOutcome::Admitted, HoldOutcome::Refused];

    pub fn name(self) -> &'static str {
        match self {
            HoldOutcome::Admitted => "admitted",
            HoldOutcome::Refused => "refused",
        }
    }
}

/// What [`IngestHoldGate::hold`] decided for one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoldVerdict {
    /// Not congested (or the gate is off): no wait, nothing counted.
    NotHeld,
    /// Held, then let through.
    Admitted,
    /// Held for the whole max wait, then refused.
    Refused,
}

/// The published congested flag, the hold itself and its counters. Every
/// atomic uses `Ordering::Relaxed`: the flag is advisory, read once per
/// request and once per hold step.
#[derive(Debug)]
pub struct IngestHoldGate {
    max_wait: Duration,
    congested: AtomicBool,
    /// Test-only override of the published flag: 0 none, 1 on, 2 off.
    #[cfg(any(test, feature = "ingest-hold-test-support"))]
    forced: std::sync::atomic::AtomicU8,
    /// Test-only: set once a detector task publishing into this gate has
    /// exited.
    #[cfg(any(test, feature = "ingest-hold-test-support"))]
    detector_exited: AtomicBool,
    admitted_total: AtomicU64,
    refused_total: AtomicU64,
    held_buckets: [AtomicU64; LATENCY_BUCKET_COUNT],
    held_sum_ns: AtomicU64,
}

impl Default for IngestHoldGate {
    /// The gate off: a zero max wait never holds.
    fn default() -> Self {
        IngestHoldGate::new(Duration::ZERO)
    }
}

impl IngestHoldGate {
    /// A gate holding for at most `max_wait`; `Duration::ZERO` turns it off.
    pub fn new(max_wait: Duration) -> Self {
        IngestHoldGate {
            max_wait,
            congested: AtomicBool::new(false),
            #[cfg(any(test, feature = "ingest-hold-test-support"))]
            forced: std::sync::atomic::AtomicU8::new(0),
            #[cfg(any(test, feature = "ingest-hold-test-support"))]
            detector_exited: AtomicBool::new(false),
            admitted_total: AtomicU64::new(0),
            refused_total: AtomicU64::new(0),
            held_buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            held_sum_ns: AtomicU64::new(0),
        }
    }

    /// The configured `--ingest-hold-max-wait`.
    pub fn max_wait(&self) -> Duration {
        self.max_wait
    }

    /// Whether the node currently reads as congested, for the
    /// `ravel_ingest_head_congested` gauge and each hold step.
    pub fn is_congested(&self) -> bool {
        #[cfg(any(test, feature = "ingest-hold-test-support"))]
        match self.forced.load(Ordering::Relaxed) {
            1 => return true,
            2 => return false,
            _ => {}
        }
        self.congested.load(Ordering::Relaxed)
    }

    /// Published by the detector task after each sample.
    pub(crate) fn publish(&self, congested: bool) {
        self.congested.store(congested, Ordering::Relaxed);
    }

    /// Whether [`Self::hold`] would wait right now: the gate is on and the
    /// node is congested. Lets a synchronous caller keep its fast path.
    pub fn should_hold(&self) -> bool {
        !self.max_wait.is_zero() && self.is_congested()
    }

    /// Holds the caller while the node is congested, re-reading the flag
    /// every 10 ms, for at most the max wait. A request that is not held
    /// returns at once and counts nothing; a held one is counted under its
    /// outcome and its held time recorded.
    pub async fn hold(&self) -> HoldVerdict {
        if !self.should_hold() {
            return HoldVerdict::NotHeld;
        }
        let started = tokio::time::Instant::now();
        loop {
            let waited = started.elapsed();
            if waited >= self.max_wait {
                self.record(HoldOutcome::Refused, waited);
                return HoldVerdict::Refused;
            }
            tokio::time::sleep(HOLD_STEP.min(self.max_wait - waited)).await;
            if !self.is_congested() {
                self.record(HoldOutcome::Admitted, started.elapsed());
                return HoldVerdict::Admitted;
            }
        }
    }

    fn record(&self, outcome: HoldOutcome, held: Duration) {
        let counter = match outcome {
            HoldOutcome::Admitted => &self.admitted_total,
            HoldOutcome::Refused => &self.refused_total,
        };
        counter.fetch_add(1, Ordering::Relaxed);
        let held_ns = u64::try_from(held.as_nanos()).unwrap_or(u64::MAX);
        self.held_buckets[latency_bucket(held_ns)].fetch_add(1, Ordering::Relaxed);
        self.held_sum_ns.fetch_add(held_ns, Ordering::Relaxed);
    }

    /// Held requests since process start that left with `outcome`.
    pub fn hold_total(&self, outcome: HoldOutcome) -> u64 {
        match outcome {
            HoldOutcome::Admitted => self.admitted_total.load(Ordering::Relaxed),
            HoldOutcome::Refused => self.refused_total.load(Ordering::Relaxed),
        }
    }

    /// The held-time histogram, one observation per held request.
    pub fn held_seconds(&self) -> LatencyHistogramSnapshot {
        LatencyHistogramSnapshot {
            buckets: std::array::from_fn(|i| self.held_buckets[i].load(Ordering::Relaxed)),
            sum_ns: self.held_sum_ns.load(Ordering::Relaxed),
        }
    }

    /// Test-only seam: `Some(c)` pins the flag to `c` over whatever the
    /// detector publishes, `None` hands it back. Gated behind `cfg(test)`
    /// and the `ingest-hold-test-support` feature, which the `tests/`
    /// binaries build this crate with.
    #[cfg(any(test, feature = "ingest-hold-test-support"))]
    pub fn force_for_test(&self, congested: Option<bool>) {
        let forced = match congested {
            None => 0,
            Some(true) => 1,
            Some(false) => 2,
        };
        self.forced.store(forced, Ordering::Relaxed);
    }

    /// Test-only seam: whether a detector task publishing into this gate has
    /// exited, so a shutdown test can see the task was stopped.
    #[cfg(any(test, feature = "ingest-hold-test-support"))]
    pub fn detector_exited_for_test(&self) -> bool {
        self.detector_exited.load(Ordering::SeqCst)
    }
}

/// A router's queued-flush cap: its shard count times its per-shard
/// `max_queued_flushes`, floored at 1 as the shard actors floor it. A shard
/// at its cap defers further triggers, so only a buffer over its memory
/// backstop takes a router's queued flushes above this.
pub fn queued_flush_cap(shard_count: u32, max_queued_flushes: u32) -> u64 {
    u64::from(shard_count) * u64::from(max_queued_flushes.max(1))
}

/// One ingest router's figures in one detector sample.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RouterQueue {
    /// The router's `flushes_queued_total`, over all its shards.
    pub queued_flushes: u64,
    /// The router's [`queued_flush_cap`], its bound unless
    /// `--ingest-hold-queued-flushes` overrides it.
    pub cap: u64,
}

/// One detector sample: each ingest router's queued flushes against its cap,
/// and the cumulative deferred-flush-trigger count summed over every router.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CongestionSample {
    /// One entry per ingest router this process built.
    pub routers: Vec<RouterQueue>,
    pub deferred_total: u64,
}

/// The congestion rule: any one router's queued flushes above its own bound
/// (`bound_override` when set, else that router's cap), or at least
/// [`DEFERRED_TRIGGERS_PER_WINDOW`] deferred triggers over a full window of
/// per-sample deltas. A window short of [`WINDOW_SAMPLES`] never counts on
/// deferrals.
fn congested(
    routers: &[RouterQueue],
    bound_override: Option<u64>,
    deferred_window: &VecDeque<u64>,
) -> bool {
    routers
        .iter()
        .any(|router| router.queued_flushes > bound_override.unwrap_or(router.cap))
        || (deferred_window.len() == WINDOW_SAMPLES
            && deferred_window.iter().sum::<u64>() >= DEFERRED_TRIGGERS_PER_WINDOW)
}

/// The congestion detector: publishes [`congested`] into the shared gate
/// once per sample.
pub struct Detector {
    bound_override: Option<u64>,
    gate: Arc<IngestHoldGate>,
}

impl Detector {
    /// `bound_override` is `--ingest-hold-queued-flushes`: `Some` replaces
    /// every router's cap as its bound.
    pub fn new(bound_override: Option<u64>, gate: Arc<IngestHoldGate>) -> Self {
        Detector {
            bound_override,
            gate,
        }
    }

    /// Runs forever, sampling every 100 ms on `clock`. `sample` reads the
    /// current figures of whichever of the metrics, log and span routers this
    /// process built; the deferred count is cumulative, and the window holds
    /// its per-sample deltas from a baseline read before the first sleep.
    pub async fn run(
        self,
        clock: Arc<dyn Clock>,
        sample: impl Fn() -> CongestionSample + Send + 'static,
    ) {
        let mut last_deferred = sample().deferred_total;
        let mut deferred_window: VecDeque<u64> = VecDeque::with_capacity(WINDOW_SAMPLES);
        loop {
            clock.sleep(SAMPLE_INTERVAL).await;
            let now = sample();
            let deferred_delta = now.deferred_total.saturating_sub(last_deferred);
            last_deferred = now.deferred_total;
            if deferred_window.len() == WINDOW_SAMPLES {
                deferred_window.pop_front();
            }
            deferred_window.push_back(deferred_delta);
            self.gate.publish(congested(
                &now.routers,
                self.bound_override,
                &deferred_window,
            ));
        }
    }
}

/// Handle to the spawned detector task, so graceful shutdown can stop it
/// before `drain_router`'s `Arc::try_unwrap` runs: the detector's `sample`
/// closure must never close over a router `Arc` (it closes over metrics
/// handles instead, the same rule `crate::ingest_health_sources` documents),
/// but a task left running past shutdown serves no purpose either, and a
/// future change that reintroduces a router clone here would otherwise
/// silently defeat the shard-actor join again.
pub struct DetectorTask(JoinHandle<()>);

impl DetectorTask {
    /// Spawns `detector.run(..)` and returns a handle that can stop it.
    pub fn spawn(
        detector: Detector,
        clock: Arc<dyn Clock>,
        sample: impl Fn() -> CongestionSample + Send + 'static,
    ) -> Self {
        #[cfg(any(test, feature = "ingest-hold-test-support"))]
        let exit_mark = DetectorExitMark(detector.gate.clone());
        DetectorTask(tokio::spawn(async move {
            #[cfg(any(test, feature = "ingest-hold-test-support"))]
            let _exit_mark = exit_mark;
            detector.run(clock, sample).await;
        }))
    }

    /// Spawns the detector for `config`, or nothing when its max wait is
    /// zero: the gate is off and nothing reads the flag.
    pub fn spawn_for(
        config: &IngestHoldConfig,
        gate: Arc<IngestHoldGate>,
        clock: Arc<dyn Clock>,
        sample: impl Fn() -> CongestionSample + Send + 'static,
    ) -> Option<Self> {
        if config.max_wait.is_zero() {
            return None;
        }
        Some(DetectorTask::spawn(
            Detector::new(config.queued_flushes, gate),
            clock,
            sample,
        ))
    }

    /// Aborts the loop and waits for the task to actually finish tearing
    /// down before returning, so a caller that joins other `Arc`s right
    /// after this is guaranteed the task's own clones are already gone.
    pub async fn stop(self) {
        self.0.abort();
        let _ = self.0.await;
    }
}

/// Test-only: marks the gate when the detector task's future is dropped,
/// whether it was aborted or the runtime tore it down.
#[cfg(any(test, feature = "ingest-hold-test-support"))]
struct DetectorExitMark(Arc<IngestHoldGate>);

#[cfg(any(test, feature = "ingest-hold-test-support"))]
impl Drop for DetectorExitMark {
    fn drop(&mut self) {
        self.0.detector_exited.store(true, Ordering::SeqCst);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use ravel_ingest::SystemClock;

    use super::*;

    /// Lets the spawned detector run the iteration a just-fired timer woke.
    async fn tick() {
        tokio::time::advance(SAMPLE_INTERVAL).await;
        tokio::task::yield_now().await;
    }

    fn window(deltas: &[u64]) -> VecDeque<u64> {
        deltas.iter().copied().collect()
    }

    fn router(queued_flushes: u64, cap: u64) -> RouterQueue {
        RouterQueue {
            queued_flushes,
            cap,
        }
    }

    /// 6e, the rule: the queued bound is exclusive, the deferred threshold
    /// inclusive, and a short window never counts on deferrals.
    #[test]
    fn congestion_rule_bounds() {
        let quiet = window(&[0; WINDOW_SAMPLES]);
        assert!(!congested(&[router(16, 16)], None, &quiet), "at the cap");
        assert!(congested(&[router(17, 16)], None, &quiet), "at cap + 1");

        let nine = window(&[1, 1, 1, 1, 1, 1, 1, 1, 1, 0]);
        assert!(
            !congested(&[router(0, 16)], None, &nine),
            "9 deferrals in a full window"
        );
        let ten = window(&[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]);
        assert!(
            congested(&[router(0, 16)], None, &ten),
            "10 deferrals in a full window"
        );

        let short = window(&[100; WINDOW_SAMPLES - 1]);
        assert!(
            !congested(&[router(0, 16)], None, &short),
            "fewer than 10 samples never counts on deferrals"
        );
    }

    /// Each router is held to its own bound: two routers each under their
    /// cap of 16 are not congested, though their queued flushes sum to 28,
    /// above 16.
    ///
    /// Non-vacuity: a rule that sums the routers' queued flushes and compares
    /// the sum against 16 reads this as congested.
    #[test]
    fn routers_each_under_their_own_bound_are_not_congested_whatever_their_sum() {
        let quiet = window(&[0; WINDOW_SAMPLES]);
        let routers = [router(14, 16), router(14, 16)];
        assert!(!congested(&routers, None, &quiet));
    }

    /// One router one flush above its own cap is congested, while the others
    /// sit idle.
    #[test]
    fn one_router_one_above_its_bound_is_congested() {
        let quiet = window(&[0; WINDOW_SAMPLES]);
        let routers = [router(0, 16), router(33, 32), router(0, 16)];
        assert!(congested(&routers, None, &quiet));
        let at_cap = [router(0, 16), router(32, 32), router(0, 16)];
        assert!(!congested(&at_cap, None, &quiet), "at the cap is not over it");
    }

    /// `--ingest-hold-queued-flushes` replaces every router's cap: 20 queued
    /// on a router capped at 32 is congested under an override of 16, and 20
    /// queued on a router capped at 16 is not under an override of 32.
    #[test]
    fn the_override_replaces_every_routers_cap() {
        let quiet = window(&[0; WINDOW_SAMPLES]);
        assert!(!congested(&[router(20, 32)], None, &quiet));
        assert!(congested(&[router(20, 32)], Some(16), &quiet));
        assert!(congested(&[router(20, 16)], None, &quiet));
        assert!(!congested(&[router(20, 16)], Some(32), &quiet));
    }

    #[test]
    fn queued_flush_cap_is_shards_times_the_per_shard_cap_floored_at_one() {
        assert_eq!(queued_flush_cap(4, 8), 32);
        assert_eq!(queued_flush_cap(2, 8), 16);
        assert_eq!(queued_flush_cap(4, 0), 4);
    }

    /// 6e, through the running detector: queued flushes alone flip the flag
    /// both ways from the first sample.
    #[tokio::test(start_paused = true)]
    async fn detector_publishes_queued_flushes_against_the_bound() {
        let gate = Arc::new(IngestHoldGate::new(Duration::from_secs(1)));
        let queued = Arc::new(AtomicU64::new(0));
        let queued_for_task = queued.clone();
        let handle = tokio::spawn(Detector::new(None, gate.clone()).run(
            Arc::new(SystemClock),
            move || CongestionSample {
                routers: vec![router(0, 4), router(queued_for_task.load(Ordering::Relaxed), 16)],
                deferred_total: 0,
            },
        ));
        tokio::task::yield_now().await;

        queued.store(16, Ordering::Relaxed);
        tick().await;
        assert!(!gate.is_congested(), "queued at the bound");
        queued.store(17, Ordering::Relaxed);
        tick().await;
        assert!(gate.is_congested(), "queued at bound + 1");
        queued.store(3, Ordering::Relaxed);
        tick().await;
        assert!(!gate.is_congested(), "cleared once the backlog drains");

        handle.abort();
    }

    /// 6e, through the running detector: deferrals count only once the
    /// window is full, and 9 per window is still clear while 10 is not.
    #[tokio::test(start_paused = true)]
    async fn detector_needs_a_full_window_of_deferrals() {
        let gate = Arc::new(IngestHoldGate::new(Duration::from_secs(1)));
        let deferred = Arc::new(AtomicU64::new(0));
        let deferred_for_task = deferred.clone();
        let handle = tokio::spawn(Detector::new(None, gate.clone()).run(
            Arc::new(SystemClock),
            move || CongestionSample {
                routers: vec![router(0, 16)],
                deferred_total: deferred_for_task.load(Ordering::Relaxed),
            },
        ));
        // The task reads its baseline before any tick moves the counter.
        tokio::task::yield_now().await;

        // 50 deferrals in the first sample: far over the threshold, but one
        // sample is not a full window.
        deferred.store(50, Ordering::Relaxed);
        tick().await;
        for _ in 1..(WINDOW_SAMPLES - 1) {
            tick().await;
            assert!(!gate.is_congested(), "short of a full window");
        }
        tick().await;
        assert!(
            gate.is_congested(),
            "the tenth sample fills the window holding 50 deferrals"
        );

        // The 50 is the window's oldest sample: the next one evicts it.
        tick().await;
        assert!(!gate.is_congested(), "the 50 left the window");

        // One deferral in each of the next nine samples: the window holds
        // one zero and nine ones.
        for _ in 0..9 {
            deferred.fetch_add(1, Ordering::Relaxed);
            tick().await;
        }
        assert!(!gate.is_congested(), "9 deferrals in a window");
        // The tenth replaces the zero: ten ones.
        deferred.fetch_add(1, Ordering::Relaxed);
        tick().await;
        assert!(gate.is_congested(), "10 deferrals in a window");

        handle.abort();
    }

    /// The deferred count is cumulative: a constant nonzero total is a zero
    /// delta every sample, never a congested window.
    #[tokio::test(start_paused = true)]
    async fn deferred_is_a_per_sample_delta_not_the_raw_cumulative_counter() {
        let gate = Arc::new(IngestHoldGate::new(Duration::from_secs(1)));
        let handle = tokio::spawn(Detector::new(None, gate.clone()).run(
            Arc::new(SystemClock),
            || CongestionSample {
                routers: vec![router(0, 16)],
                deferred_total: 1000,
            },
        ));
        for _ in 0..(WINDOW_SAMPLES + 2) {
            tick().await;
        }
        assert!(
            !gate.is_congested(),
            "an unchanging cumulative counter must contribute a zero delta"
        );
        handle.abort();
    }

    /// 6a: not congested, the hold returns without sleeping and counts
    /// nothing.
    #[tokio::test(start_paused = true)]
    async fn not_congested_proceeds_at_once_and_counts_nothing() {
        let gate = IngestHoldGate::new(Duration::from_secs(1));
        let started = tokio::time::Instant::now();
        assert_eq!(gate.hold().await, HoldVerdict::NotHeld);
        assert_eq!(started.elapsed(), Duration::ZERO, "no sleep");
        for outcome in HoldOutcome::ALL {
            assert_eq!(gate.hold_total(outcome), 0);
        }
        assert_eq!(gate.held_seconds(), LatencyHistogramSnapshot::default());
    }

    /// 6b at the gate: congested for 300 ms, then cleared.
    #[tokio::test(start_paused = true)]
    async fn congested_then_cleared_is_admitted_after_the_clear() {
        let gate = Arc::new(IngestHoldGate::new(Duration::from_secs(1)));
        gate.publish(true);
        let clearer = gate.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            clearer.publish(false);
        });
        let started = tokio::time::Instant::now();
        assert_eq!(gate.hold().await, HoldVerdict::Admitted);
        let held = started.elapsed();
        assert!(
            held >= Duration::from_millis(300) && held <= Duration::from_millis(310),
            "held {held:?}"
        );
        assert_eq!(gate.hold_total(HoldOutcome::Admitted), 1);
        assert_eq!(gate.hold_total(HoldOutcome::Refused), 0);
        assert_eq!(gate.held_seconds().count(), 1);
    }

    /// 6c at the gate: congested throughout, refused at exactly the max wait.
    #[tokio::test(start_paused = true)]
    async fn congested_throughout_is_refused_at_the_max_wait() {
        let gate = IngestHoldGate::new(Duration::from_millis(1000));
        gate.publish(true);
        let started = tokio::time::Instant::now();
        assert_eq!(gate.hold().await, HoldVerdict::Refused);
        assert_eq!(started.elapsed(), Duration::from_millis(1000));
        assert_eq!(gate.hold_total(HoldOutcome::Refused), 1);
        assert_eq!(gate.hold_total(HoldOutcome::Admitted), 0);
        assert_eq!(gate.held_seconds().count(), 1);
    }

    /// 6d: a zero max wait spawns no detector, and the gate never holds even
    /// when the flag reads congested.
    #[tokio::test(start_paused = true)]
    async fn zero_max_wait_spawns_no_detector_and_never_holds() {
        let config = IngestHoldConfig {
            queued_flushes: None,
            max_wait: Duration::ZERO,
        };
        let gate = Arc::new(IngestHoldGate::new(config.max_wait));
        let sampled = Arc::new(AtomicBool::new(false));
        let sampled_for_task = sampled.clone();
        let task =
            DetectorTask::spawn_for(&config, gate.clone(), Arc::new(SystemClock), move || {
                sampled_for_task.store(true, Ordering::Relaxed);
                CongestionSample::default()
            });
        assert!(task.is_none(), "no detector for a zero max wait");
        tick().await;
        assert!(!sampled.load(Ordering::Relaxed), "nothing sampled");

        gate.force_for_test(Some(true));
        assert!(!gate.should_hold());
        assert_eq!(gate.hold().await, HoldVerdict::NotHeld);
        assert_eq!(gate.hold_total(HoldOutcome::Refused), 0);
    }

    /// A non-zero max wait does spawn the detector, so 6d's `None` is the
    /// zero wait's doing.
    #[tokio::test(start_paused = true)]
    async fn nonzero_max_wait_spawns_the_detector() {
        let config = IngestHoldConfig {
            queued_flushes: None,
            max_wait: Duration::from_secs(1),
        };
        let gate = Arc::new(IngestHoldGate::new(config.max_wait));
        let task = DetectorTask::spawn_for(&config, gate.clone(), Arc::new(SystemClock), || {
            CongestionSample {
                routers: vec![router(17, 16)],
                deferred_total: 0,
            }
        })
        .expect("a detector for a non-zero max wait");
        tokio::task::yield_now().await;
        tick().await;
        assert!(gate.is_congested());
        assert!(!gate.detector_exited_for_test(), "running");
        task.stop().await;
        assert!(
            gate.detector_exited_for_test(),
            "stop returns once the task has exited"
        );
    }

    /// The closed-loop plant behind the #3756 decision, ported line for line
    /// from the Python reference (law_off: `p` is always 0).
    mod plant {
        use std::collections::VecDeque;

        const TICK: f64 = 0.1;
        const CORES: f64 = 8.0;
        const COST_OK: f64 = 49e-6;
        const COST_BAD: f64 = 75e-6;
        const TIP: f64 = 300_000.0;
        const RECOVER: f64 = 60_000.0;
        /// The reference reads this to fill its deferral window `def_w`, which
        /// only its shed laws read; law_off ignores it, so the port keeps
        /// neither in use.
        #[allow(dead_code)]
        const DEFER: f64 = 150_000.0;
        pub(super) const HOLD: f64 = 100_000.0;
        const HOLD_MAX_TICKS: u32 = 10;
        const CHANNEL: f64 = 400_000.0;
        const WORKERS: usize = 32;
        const BATCH: f64 = 2000.0;
        const LAT0: f64 = 0.080;
        const LAT_PER_K: f64 = 0.0009;
        const B0: f64 = 0.1;
        const BMAX: f64 = 5.0;
        const WIN: usize = 10;

        /// SplitMix64.
        struct Rng {
            s: u64,
        }

        impl Rng {
            fn f(&mut self) -> f64 {
                self.s = self.s.wrapping_add(0x9E37_79B9_7F4A_7C15);
                let mut z = self.s;
                z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
                z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
                z ^= z >> 31;
                (z >> 11) as f64 / (1u64 << 53) as f64
            }
        }

        #[derive(Default)]
        pub(super) struct Trace {
            /// Recorded as the reference does; `span` does not read it.
            #[allow(dead_code)]
            adm: Vec<f64>,
            proc: Vec<f64>,
            drop: Vec<f64>,
            cpu: Vec<f64>,
            col: Vec<bool>,
        }

        pub(super) struct Run {
            pub phases: Vec<(f64, f64)>,
            pub seconds: f64,
            pub hold: bool,
            pub hold_bound: f64,
            pub jitter: f64,
        }

        pub(super) fn run(cfg: &Run) -> Trace {
            let seed: u64 = 0x3741;
            let burst = CHANNEL;
            let mut rng = Rng { s: seed };
            let n = (cfg.seconds / TICK) as usize;
            let mut channel = burst;
            let mut free_at = [0.0f64; WORKERS];
            let mut streak = [0i32; WORKERS];
            let mut held = [false; WORKERS];
            let mut waited = [0u32; WORKERS];
            let mut backlog = 0.0f64;
            let mut collapsed = false;
            let p = 0.0f64;
            let mut cpu_w: VecDeque<f64> = VecDeque::with_capacity(WIN);
            let _def_w: VecDeque<f64> = VecDeque::with_capacity(WIN);
            let mut tr = Trace::default();
            let mut rate = 0.0f64;
            for t in 0..n {
                let now = t as f64 * TICK;
                for &(start, r) in &cfg.phases {
                    if (start - now).abs() < 1e-9 {
                        rate = r;
                    }
                }
                let gen_lines = rate * TICK;
                let drop = (channel + gen_lines - CHANNEL).max(0.0);
                channel = CHANNEL.min(channel + gen_lines);
                let mut admitted = 0.0f64;
                for w in 0..WORKERS {
                    while free_at[w] < now + TICK {
                        if !held[w] {
                            if channel < BATCH {
                                free_at[w] = now + TICK;
                                break;
                            }
                            channel -= BATCH;
                            held[w] = true;
                        }
                        if cfg.hold
                            && backlog + admitted > cfg.hold_bound
                            && waited[w] < HOLD_MAX_TICKS
                        {
                            waited[w] += 1;
                            free_at[w] = now + TICK;
                            break;
                        }
                        if (p > 0.0 && rng.f() < p) || (cfg.hold && waited[w] >= HOLD_MAX_TICKS) {
                            free_at[w] += (B0 * 2f64.powi(streak[w])).min(BMAX);
                            streak[w] += 1;
                            waited[w] = 0;
                        } else {
                            waited[w] = 0;
                            admitted += BATCH;
                            held[w] = false;
                            streak[w] = 0;
                            free_at[w] += LAT0 + LAT_PER_K * backlog / 1000.0;
                        }
                    }
                }
                let cost = if collapsed { COST_BAD } else { COST_OK };
                let cap_lines = CORES * TICK / cost * (1.0 + cfg.jitter * (2.0 * rng.f() - 1.0));
                let proc = (backlog + admitted).min(cap_lines);
                backlog = backlog + admitted - proc;
                if backlog > TIP {
                    collapsed = true;
                } else if backlog < RECOVER {
                    collapsed = false;
                }
                if cpu_w.len() == WIN {
                    cpu_w.pop_front();
                }
                cpu_w.push_back((proc * cost / (CORES * TICK)).min(1.0));
                tr.adm.push(admitted);
                tr.proc.push(proc);
                tr.drop.push(drop);
                tr.cpu.push(cpu_w.iter().sum::<f64>() / cpu_w.len() as f64);
                tr.col.push(collapsed);
            }
            tr
        }

        #[derive(Debug)]
        pub(super) struct Span {
            /// Thousand lines per second processed.
            pub stored: f64,
            /// Thousand lines per second dropped by the writer.
            pub drops: f64,
            pub cores: f64,
            pub collapsed_share: f64,
        }

        pub(super) fn span(tr: &Trace, a: f64, b: f64) -> Span {
            let range = (a / TICK) as usize..(b / TICK) as usize;
            let ticks = range.len() as f64;
            Span {
                stored: tr.proc[range.clone()].iter().sum::<f64>() / (b - a) / 1000.0,
                drops: tr.drop[range.clone()].iter().sum::<f64>() / (b - a) / 1000.0,
                cores: tr.cpu[range.clone()].iter().sum::<f64>() / ticks * CORES,
                collapsed_share: tr.col[range].iter().filter(|c| **c).count() as f64 / ticks,
            }
        }
    }

    const JITTER: f64 = 0.25;

    fn plant_span(
        phases: &[(f64, f64)],
        seconds: f64,
        hold_bound: Option<f64>,
        a: f64,
        b: f64,
    ) -> plant::Span {
        let trace = plant::run(&plant::Run {
            phases: phases.to_vec(),
            seconds,
            hold: hold_bound.is_some(),
            hold_bound: hold_bound.unwrap_or(plant::HOLD),
            jitter: JITTER,
        });
        let span = plant::span(&trace, a, b);
        println!("{phases:?} {seconds}s hold={hold_bound:?} span {a}..{b}: {span:?}");
        span
    }

    /// Case A: 160k for 80 s with the 400k start burst. The hold stores the
    /// offered rate with no collapse; with no gate, and with a bound above
    /// the 300k tip point, the burst locks the plant in its collapsed state.
    #[test]
    fn plant_case_a_hold_avoids_the_start_burst_collapse() {
        let phases = [(0.0, 160_000.0)];
        let hold = plant_span(&phases, 80.0, Some(plant::HOLD), 20.0, 80.0);
        assert!(hold.stored >= 152.0, "{hold:?}");
        assert!(hold.drops <= 3.2, "{hold:?}");
        assert_eq!(hold.collapsed_share, 0.0, "{hold:?}");

        let off = plant_span(&phases, 80.0, None, 20.0, 80.0);
        assert!(off.collapsed_share > 0.5, "{off:?}");

        let above_tip = plant_span(&phases, 80.0, Some(1_000_000.0), 20.0, 80.0);
        assert!(above_tip.collapsed_share > 0.5, "{above_tip:?}");
    }

    /// Case B: 320k, twice capacity, for 80 s. The bar is the Python
    /// reference's: 0.95 x its 163.3 capacity figure, 155.1.
    #[test]
    fn plant_case_b_hold_stores_capacity_under_double_load() {
        let hold = plant_span(&[(0.0, 320_000.0)], 80.0, Some(plant::HOLD), 20.0, 80.0);
        assert!(hold.stored >= 0.95 * 163.3, "{hold:?}");
        assert_eq!(hold.collapsed_share, 0.0, "{hold:?}");
    }

    /// Case C: 140k, below capacity, for 120 s with the burst.
    #[test]
    fn plant_case_c_hold_recovers_from_the_burst_below_capacity() {
        let phases = [(0.0, 140_000.0)];
        let hold = plant_span(&phases, 120.0, Some(plant::HOLD), 60.0, 120.0);
        assert!(hold.stored >= 139.0, "{hold:?}");
        assert_eq!(hold.drops, 0.0, "{hold:?}");
        assert_eq!(hold.collapsed_share, 0.0, "{hold:?}");

        let off = plant_span(&phases, 120.0, None, 60.0, 120.0);
        assert!(off.collapsed_share > 0.5, "{off:?}");
    }

    /// Case D: 160k, then 100k from 40 s: below capacity the hold holds
    /// nothing back and the node does not run hot.
    #[test]
    fn plant_case_d_hold_tracks_a_drop_in_load() {
        let hold = plant_span(
            &[(0.0, 160_000.0), (40.0, 100_000.0)],
            70.0,
            Some(plant::HOLD),
            45.0,
            70.0,
        );
        assert!((hold.stored - 100.0).abs() <= 1.0, "{hold:?}");
        assert!(hold.cores < 5.5, "{hold:?}");
    }
}
