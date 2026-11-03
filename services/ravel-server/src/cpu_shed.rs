//! ADR-3718: CPU-aware ingest admission.
//!
//! A background detector (spawned in [`crate::start`] when
//! `--ingest-cpu-shed-threshold` is above 0) samples the process's own CPU
//! use and the ingest routers' deferred-flush-trigger counters every 100 ms,
//! keeps 1-second rolling averages of both, and moves a shed fraction `p` per
//! [`step`] (ADR-3718 D2). `admit_ingest_request` and
//! `GrpcIngestAdmissionLayer::call` draw against `p` once at the request head,
//! before tenant resolution or body work. An OTAP stream draws once per
//! `BatchArrowRecords` batch in `process_batch`, after its own in-flight
//! permit; its tenant was resolved when the stream opened.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rand::RngExt;
use ravel_ingest::Clock;
use tokio::task::JoinHandle;

/// One sample every 100 ms (ADR-3718 D1).
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
/// 1-second rolling average over 10 samples at the 100 ms sample interval
/// (ADR-3718 D1/D2).
const WINDOW_SAMPLES: usize = 10;
/// Floor below which the wall-clock interval between two samples is treated
/// as degenerate (a backward or frozen clock step, or two ticks racing) and
/// skipped rather than divided into the CPU delta: dividing by a near-zero
/// interval spikes `cpu_frac` toward infinity on a single bad sample. Measured
/// with `tokio::time::Instant`, which is monotonic and immune to the
/// wall-clock steps `ravel_ingest::Clock::now_ns` is exposed to; this loop
/// reads no wall time at all, so a backward `SystemTime` step cannot reach it.
const MIN_WALL_DELTA: Duration = Duration::from_millis(10);

/// The deferred-trigger average the detector feeds to [`step`]: 0 until the
/// window holds its full [`WINDOW_SAMPLES`], so saturation (which needs
/// deferrals above zero) cannot trip on fewer than 1 s of samples (ADR-3718
/// D1), while the hold and decay rows still run from the first sample.
fn window_deferred_avg(window: &VecDeque<u64>) -> f64 {
    if window.len() < WINDOW_SAMPLES {
        return 0.0;
    }
    window.iter().sum::<u64>() as f64 / window.len() as f64
}

/// Moves the shed fraction `p` by one of ADR-3718 D2's three rules. `p` is
/// always in `[0, cap]`.
///
/// - **Saturated** (`cpu_avg` at or above `threshold` AND `deferred_avg`
///   above zero): `p` rises by `0.05`, capped at `cap`.
/// - **Near the ceiling but not saturated** (`cpu_avg` at or above
///   `threshold - 0.05`, the release level): `p` holds, whatever
///   `deferred_avg` is. This is the row that catches the recovery gap: CPU
///   between the release level and `threshold` with deferrals still above
///   zero is NOT saturated (CPU has not reached `threshold`), so it holds
///   here rather than falling.
/// - **Clear of the ceiling** (`cpu_avg` below the release level): `p` falls
///   by 10% of itself, snapping to `0` below `0.01`.
///
/// `threshold <= 0.0` (`--ingest-cpu-shed-threshold 0`) always returns `0.0`:
/// shedding is off, and the three rows above never run.
pub fn step(p: f64, cpu_avg: f64, deferred_avg: f64, threshold: f64, cap: f64) -> f64 {
    if threshold <= 0.0 {
        return 0.0;
    }
    let release_level = threshold - 0.05;
    let saturated = cpu_avg >= threshold && deferred_avg > 0.0;
    if saturated {
        (p + 0.05).min(cap)
    } else if cpu_avg >= release_level {
        p
    } else {
        let decayed = p * 0.9;
        if decayed < 0.01 { 0.0 } else { decayed }
    }
}

/// The shed fraction `p`, published by the detector task and read by every
/// ingest admission path, plus the cumulative refusal counter for
/// `ravel_ingest_cpu_shed_total`. Stored as `f64` bits behind one
/// `AtomicU64`: every read and write uses `Ordering::Relaxed` (ADR-3718
/// Consequences: "a shed draw is one atomic load and one random number per
/// request", never a snapshot of detector state).
#[derive(Debug, Default)]
pub struct CpuShedGauge {
    p_bits: AtomicU64,
    shed_total: AtomicU64,
}

impl CpuShedGauge {
    /// The current shed fraction, for the `/metrics`
    /// `ravel_ingest_cpu_shed_fraction` gauge and for [`Self::draw`].
    pub fn load(&self) -> f64 {
        f64::from_bits(self.p_bits.load(Ordering::Relaxed))
    }

    /// Published by the detector task after each [`step`].
    pub(crate) fn store(&self, p: f64) {
        self.p_bits.store(p.to_bits(), Ordering::Relaxed);
    }

    /// Cumulative refusals since process start, for
    /// `ravel_ingest_cpu_shed_total`.
    pub fn shed_total(&self) -> u64 {
        self.shed_total.load(Ordering::Relaxed)
    }

    /// One relaxed load of the current shed fraction plus one random draw
    /// (ADR-3718 D2/Consequences): `true` means this request is refused.
    /// `p <= 0.0` never shed, regardless of the draw, so a disabled or fully
    /// recovered detector costs nothing beyond the one atomic load. On
    /// refusal, bumps [`Self::shed_total`].
    pub fn draw(&self) -> bool {
        let p = self.load();
        let shed = p > 0.0 && rand::rng().random::<f64>() < p;
        if shed {
            self.shed_total.fetch_add(1, Ordering::Relaxed);
        }
        shed
    }

    /// Test-only seam: pins `p` to a fixed value so a reachability test can
    /// assert the admit/refuse behavior at `p = 0.0` and `p = 1.0` without
    /// driving the real detector loop or CPU load. Never a production flag
    /// (ADR-3718 does not expose one): gated behind `cfg(test)` for this
    /// crate's own unit/lib tests, and behind the `cpu-shed-test-support`
    /// feature (mirroring `gcs-test-endpoint`) for the `tests/` integration
    /// binaries, which build this crate as a dev-dependency with that
    /// feature on.
    #[cfg(any(test, feature = "cpu-shed-test-support"))]
    pub fn set_for_test(&self, p: f64) {
        self.store(p);
    }
}

/// Source of the process's own CPU time, for the saturation detector's CPU
/// signal (ADR-3718 D1).
pub trait CpuSource: Send + Sync {
    /// Process CPU time (user plus system) since process start, or `None`
    /// when unavailable: off Linux, or `/proc/self/stat` could not be read
    /// or parsed. `None` never panics; the detector logs it once and leaves
    /// `p` at `0`.
    fn cpu_time(&self) -> Option<Duration>;
}

/// Reads `/proc/self/stat` fields 14 and 15 (utime, stime) at Linux's fixed
/// 100 ticks/sec, the same file and fields
/// `crates/ravel-bench/src/columnar_load.rs`'s `process_cpu` already reads.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProcSelfStatCpuSource;

impl CpuSource for ProcSelfStatCpuSource {
    #[cfg(target_os = "linux")]
    fn cpu_time(&self) -> Option<Duration> {
        let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
        parse_proc_self_stat(&stat)
    }

    #[cfg(not(target_os = "linux"))]
    fn cpu_time(&self) -> Option<Duration> {
        None
    }
}

/// `USER_HZ`: Linux's fixed clock tick rate for `/proc/[pid]/stat`'s utime
/// and stime fields (`man 5 proc`), not the configurable `CLOCKS_PER_SEC`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
const USER_HZ: u64 = 100;

/// Parses the utime/stime fields out of a `/proc/self/stat` line. The comm
/// field (the process name in parentheses) can itself contain spaces and
/// closing parentheses, so parsing starts after the LAST `)` rather than
/// splitting on whitespace from the start, matching the kernel's own comm
/// quoting and `columnar_load.rs`'s `process_cpu`. Returns `None` -- never
/// panics -- on anything malformed or truncated: a missing `)`, or fewer
/// fields than expected.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_proc_self_stat(stat: &str) -> Option<Duration> {
    let rest = &stat[stat.rfind(')')? + 1..];
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(Duration::from_nanos(
        (utime + stime).saturating_mul(1_000_000_000 / USER_HZ),
    ))
}

/// Computes one sample's CPU fraction, or `None` when `wall_delta` is
/// degenerate (zero or below [`MIN_WALL_DELTA`]). A backward step is
/// structurally impossible on the monotonic clock [`Detector::run`] measures
/// with, but a near-zero interval from scheduler jitter is not, and dividing
/// `delta_cpu` by it would spike the result toward infinity on one sample.
fn sample_cpu_fraction(delta_cpu: Duration, wall_delta: Duration, cores: usize) -> Option<f64> {
    if wall_delta < MIN_WALL_DELTA {
        return None;
    }
    Some(delta_cpu.as_nanos() as f64 / wall_delta.as_nanos() as f64 / cores.max(1) as f64)
}

/// The saturation detector (ADR-3718 D1/D2): owns the CPU source and the
/// resolved threshold/cap, and publishes `p` into a shared [`CpuShedGauge`].
pub struct Detector {
    cpu_source: Box<dyn CpuSource>,
    cores: usize,
    threshold: f64,
    cap: f64,
    gauge: Arc<CpuShedGauge>,
}

impl Detector {
    pub fn new(
        cpu_source: Box<dyn CpuSource>,
        cores: usize,
        threshold: f64,
        cap: f64,
        gauge: Arc<CpuShedGauge>,
    ) -> Self {
        Detector {
            cpu_source,
            cores: cores.max(1),
            threshold,
            cap,
            gauge,
        }
    }

    /// Runs forever, sampling every 100 ms on `clock` (the injected
    /// `ravel_ingest::Clock`, `SystemClock` in production). `deferred_total`
    /// reads the current cumulative sum of `flush_trigger_deferred` across
    /// every signal's routers (`crate::start` closes over whichever of the
    /// metrics/log/span routers this process built).
    ///
    /// Returns (does not spawn a loop) when `threshold <= 0.0`
    /// (`--ingest-cpu-shed-threshold 0`, shedding disabled) or when the CPU
    /// source reports unavailable on the first sample (off Linux): either
    /// way `p` stays at the gauge's default `0.0` and no task runs forever
    /// for nothing.
    pub async fn run(
        self,
        clock: Arc<dyn Clock>,
        deferred_total: impl Fn() -> u64 + Send + 'static,
    ) {
        if self.threshold <= 0.0 {
            return;
        }
        let Some(mut last_cpu_time) = self.cpu_source.cpu_time() else {
            tracing::warn!(
                "CPU-aware ingest admission (ADR-3718) disabled: /proc/self/stat is \
                 unavailable on this host; the shed fraction stays at 0"
            );
            return;
        };
        let mut last_wall = tokio::time::Instant::now();
        let mut last_deferred = deferred_total();
        let mut cpu_window: VecDeque<f64> = VecDeque::with_capacity(WINDOW_SAMPLES);
        let mut deferred_window: VecDeque<u64> = VecDeque::with_capacity(WINDOW_SAMPLES);
        let mut p = 0.0;

        loop {
            clock.sleep(SAMPLE_INTERVAL).await;

            let now = tokio::time::Instant::now();
            let wall_delta = now.saturating_duration_since(last_wall);
            if wall_delta < MIN_WALL_DELTA {
                // Degenerate interval: skip this sample's contribution
                // entirely instead of dividing by it. `last_wall`,
                // `last_cpu_time` and `last_deferred` stay put, so the next
                // valid tick measures the combined interval rather than
                // losing the skipped one.
                continue;
            }

            let cpu_now = self.cpu_source.cpu_time();
            let cpu_frac = match cpu_now {
                Some(now_cpu) => {
                    let delta_cpu = now_cpu.saturating_sub(last_cpu_time);
                    sample_cpu_fraction(delta_cpu, wall_delta, self.cores).unwrap_or(0.0)
                }
                None => 0.0,
            };
            last_wall = now;
            last_cpu_time = cpu_now.unwrap_or(last_cpu_time);

            let deferred_now = deferred_total();
            let deferred_delta = deferred_now.saturating_sub(last_deferred);
            last_deferred = deferred_now;

            if cpu_window.len() == WINDOW_SAMPLES {
                cpu_window.pop_front();
            }
            cpu_window.push_back(cpu_frac);
            if deferred_window.len() == WINDOW_SAMPLES {
                deferred_window.pop_front();
            }
            deferred_window.push_back(deferred_delta);

            let cpu_avg = cpu_window.iter().sum::<f64>() / cpu_window.len() as f64;
            let deferred_avg = window_deferred_avg(&deferred_window);

            p = step(p, cpu_avg, deferred_avg, self.threshold, self.cap);
            self.gauge.store(p);
        }
    }
}

/// Handle to the spawned detector task (ADR-3718 D1), so graceful shutdown
/// can stop it before `drain_router`'s `Arc::try_unwrap` runs: the detector's
/// `deferred_total` closure must never close over a router `Arc` (it closes
/// over metrics handles instead, the same rule `crate::ingest_health_sources`
/// documents), but a task left running past shutdown serves no purpose
/// either, and a future change that reintroduces a router clone here would
/// otherwise silently defeat the shard-actor join again.
pub struct DetectorTask(JoinHandle<()>);

impl DetectorTask {
    /// Spawns `detector.run(..)` and returns a handle that can stop it.
    pub fn spawn(
        detector: Detector,
        clock: Arc<dyn Clock>,
        deferred_total: impl Fn() -> u64 + Send + 'static,
    ) -> Self {
        DetectorTask(tokio::spawn(detector.run(clock, deferred_total)))
    }

    /// Aborts the loop and waits for the task to actually finish tearing
    /// down before returning, so a caller that joins other `Arc`s right
    /// after this is guaranteed the task's own clones are already gone.
    pub async fn stop(self) {
        self.0.abort();
        let _ = self.0.await;
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::Mutex;

    use ravel_ingest::SystemClock;

    use super::*;

    const THRESHOLD: f64 = 0.95;
    const CAP: f64 = 0.9;

    #[test]
    fn saturated_rises_by_fixed_step_capped() {
        assert_eq!(step(0.5, 0.96, 1.0, THRESHOLD, CAP), 0.55);
        // Capped at `cap`, never above it.
        assert_eq!(step(0.88, 0.96, 1.0, THRESHOLD, CAP), CAP);
        assert_eq!(step(CAP, 0.96, 1.0, THRESHOLD, CAP), CAP);
    }

    #[test]
    fn not_saturated_at_cpu_exactly_at_threshold_requires_deferrals_too() {
        // At the threshold but no deferrals: not saturated (D1 needs both
        // signals), and CPU is still at or above the release level, so this
        // holds rather than rising or falling.
        assert_eq!(step(0.3, THRESHOLD, 0.0, THRESHOLD, CAP), 0.3);
    }

    /// ADR-3718 D2's recovery-gap case: CPU between the release level
    /// (0.90) and the threshold (0.95), with deferrals still above zero.
    /// This is NOT saturated (CPU has not reached the threshold), so it must
    /// hold, not fall. A wrong implementation that reads the hold row as
    /// "release level or above, AND deferred_avg == 0" instead of "release
    /// level or above, any deferred_avg" sends this to the decay branch
    /// instead: `step` would return `0.5 * 0.9 == 0.45` rather than holding
    /// at `0.5`. This assertion fails on that implementation at this exact
    /// line (`assert_eq!(result, 0.5)`).
    #[test]
    fn recovery_gap_holds_even_with_deferrals_still_active() {
        let result = step(0.5, 0.92, 5.0, THRESHOLD, CAP);
        assert_eq!(result, 0.5, "CPU in the recovery gap must hold, not fall");
    }

    #[test]
    fn below_release_level_decays_by_ten_percent_of_itself() {
        // A wrong implementation using the superseded halving rule (p *
        // 0.5) returns 0.25 here instead of 0.45. This assertion fails on
        // that implementation at this exact line (`assert_eq!(result,
        // 0.45)`).
        let result = step(0.5, 0.5, 0.0, THRESHOLD, CAP);
        assert_eq!(result, 0.45, "decay must be 10% of p, not a halving");
    }

    #[test]
    fn decay_snaps_to_zero_below_one_percent() {
        // 0.011 * 0.9 == 0.0099, under the 0.01 snap floor.
        assert_eq!(step(0.011, 0.0, 0.0, THRESHOLD, CAP), 0.0);
        // 0.02 * 0.9 == 0.018 (0.018000000000000002 in f64), at or above
        // the floor: decays, does not snap.
        assert_eq!(step(0.02, 0.0, 0.0, THRESHOLD, CAP), 0.02 * 0.9);
    }

    #[test]
    fn threshold_zero_always_returns_zero() {
        assert_eq!(step(0.8, 1.0, 10.0, 0.0, CAP), 0.0);
        assert_eq!(step(0.0, 0.0, 0.0, 0.0, CAP), 0.0);
    }

    #[test]
    fn gauge_draw_never_sheds_at_zero_and_always_sheds_at_one() {
        let gauge = CpuShedGauge::default();
        gauge.set_for_test(0.0);
        for _ in 0..64 {
            assert!(!gauge.draw());
        }
        assert_eq!(gauge.shed_total(), 0);

        gauge.set_for_test(1.0);
        for _ in 0..64 {
            assert!(gauge.draw());
        }
        assert_eq!(gauge.shed_total(), 64);
    }

    fn stat_line(comm: &str, trailing_fields: &[&str]) -> String {
        // pid (comm) state ppid pgrp session tty_nr tpgid flags minflt
        // cminflt majflt cmajflt utime stime ...
        format!(
            "1 ({comm}) R 0 0 0 0 0 0 0 0 0 0 {}",
            trailing_fields.join(" ")
        )
    }

    #[test]
    fn parses_utime_and_stime_into_cpu_time() {
        let line = stat_line("ravel-server", &["250", "150"]);
        let cpu = parse_proc_self_stat(&line).expect("well-formed line parses");
        // (250 + 150) ticks at 100 ticks/sec == 4s of CPU time.
        assert_eq!(cpu, Duration::from_secs(4));
    }

    #[test]
    fn comm_with_spaces_and_closing_paren_parses_past_the_last_paren() {
        let line = stat_line("my proc) weird)", &["100", "0"]);
        let cpu = parse_proc_self_stat(&line).expect("comm with parens still parses");
        assert_eq!(cpu, Duration::from_secs(1));
    }

    #[test]
    fn malformed_line_is_unavailable_not_a_panic() {
        assert_eq!(parse_proc_self_stat("not a stat line at all"), None);
        assert_eq!(parse_proc_self_stat("1 (comm) R 0 0"), None);
        assert_eq!(parse_proc_self_stat(""), None);
        // A non-numeric utime field.
        let bad = stat_line("ravel-server", &["not-a-number", "0"]);
        assert_eq!(parse_proc_self_stat(&bad), None);
    }

    #[test]
    fn degenerate_wall_delta_is_not_divided() {
        // A wrong implementation that divides by `wall_delta` regardless of
        // its size would return a huge fraction here (1s of CPU over 1ns).
        // This assertion fails on that implementation at this exact line.
        assert_eq!(
            sample_cpu_fraction(Duration::from_secs(1), Duration::from_nanos(1), 1),
            None,
            "a near-zero wall delta must be skipped, not divided into"
        );
        assert_eq!(
            sample_cpu_fraction(Duration::from_millis(5), Duration::from_millis(5), 1),
            None,
            "a wall delta below the 10ms floor must be skipped"
        );
        assert_eq!(
            sample_cpu_fraction(Duration::ZERO, Duration::ZERO, 1),
            None,
            "a zero wall delta must be skipped"
        );
        // At or above the floor, the sample is used normally.
        assert_eq!(
            sample_cpu_fraction(Duration::from_millis(50), Duration::from_millis(100), 1),
            Some(0.5)
        );
    }

    /// A fake [`CpuSource`] whose reading is set by the test at any point,
    /// simulating process CPU time advancing between samples.
    struct FakeCpuSource(Arc<Mutex<Duration>>);

    impl CpuSource for FakeCpuSource {
        fn cpu_time(&self) -> Option<Duration> {
            Some(*self.0.lock().expect("lock"))
        }
    }

    /// `Detector::run` never reads wall time (item 2's fix removed its only
    /// `Clock::now_ns` call), so any `Clock` whose `sleep` resolves on
    /// tokio's real timer works; `SystemClock` plus `tokio::time::pause`
    /// drives it deterministically without a dedicated test clock.
    #[tokio::test(start_paused = true)]
    async fn run_averages_over_a_sliding_ten_sample_window() {
        let cpu_time = Arc::new(Mutex::new(Duration::ZERO));
        let gauge = Arc::new(CpuShedGauge::default());
        let detector = Detector::new(
            Box::new(FakeCpuSource(cpu_time.clone())),
            1,
            THRESHOLD,
            CAP,
            gauge.clone(),
        );
        let deferred_total = Arc::new(AtomicU64::new(0));
        let deferred_for_task = deferred_total.clone();
        let handle = tokio::spawn(detector.run(Arc::new(SystemClock), move || {
            deferred_for_task.load(Ordering::Relaxed)
        }));
        // Let the task reach its pre-loop baseline capture (cpu=0, deferred=0)
        // before any tick sets fake data: without this yield, the spawned
        // task's first poll happens inside the loop's first `advance()` call
        // below, so the baseline capture races the first tick's own writes
        // and silently eats one sample off every later tick's count.
        tokio::task::yield_now().await;

        // A plain, unbounded-free oracle: a 10-wide sliding window fed the
        // exact same per-tick cpu_frac/deferred_delta sequence, run through
        // the already-trusted `step`. 10 ticks of 5.0 cpu_frac (500ms CPU per
        // 100ms wall tick) and a deferred delta of 2, then 10 idle ticks.
        let mut oracle_p = 0.0_f64;
        let mut cpu_window: VecDeque<f64> = VecDeque::new();
        let mut deferred_window: VecDeque<u64> = VecDeque::new();
        let mut push_oracle_tick = |cpu_frac: f64, deferred_delta: u64, p: &mut f64| {
            if cpu_window.len() == WINDOW_SAMPLES {
                cpu_window.pop_front();
            }
            cpu_window.push_back(cpu_frac);
            if deferred_window.len() == WINDOW_SAMPLES {
                deferred_window.pop_front();
            }
            deferred_window.push_back(deferred_delta);
            let cpu_avg = cpu_window.iter().sum::<f64>() / cpu_window.len() as f64;
            let deferred_avg = window_deferred_avg(&deferred_window);
            *p = step(*p, cpu_avg, deferred_avg, THRESHOLD, CAP);
        };

        // The trailing `yield_now()` after each `advance()` is required, not
        // decorative: `advance()` bumps the virtual clock and wakes the
        // sleeping task but does not run it to completion within the same
        // poll, so the woken iteration's `gauge.store()` only becomes
        // observable one more yield later.
        let mut cpu = Duration::ZERO;
        let mut deferred = 0u64;
        for _ in 0..10 {
            cpu += Duration::from_millis(500);
            *cpu_time.lock().expect("lock") = cpu;
            deferred += 2;
            deferred_total.store(deferred, Ordering::Relaxed);
            push_oracle_tick(5.0, 2, &mut oracle_p);
            tokio::time::advance(SAMPLE_INTERVAL).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            gauge.load(),
            oracle_p,
            "10 saturated ticks must match the windowed-average oracle"
        );

        // 10 more idle ticks: once the window (capped at 10 samples) has
        // evicted every high sample, `cpu_avg` falls below the release level
        // and `p` starts decaying. A wrong implementation averaging over an
        // unbounded/cumulative sample count instead of the fixed 10-wide
        // window would still see the high history diluted rather than
        // evicted (sum 50 / 20 ticks == 2.5, still saturated) and would
        // still read saturated here. This assertion fails on that
        // implementation at this exact line.
        for _ in 0..10 {
            push_oracle_tick(0.0, 0, &mut oracle_p);
            tokio::time::advance(SAMPLE_INTERVAL).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            gauge.load(),
            oracle_p,
            "the window must evict the old high samples and decay, matching the oracle exactly"
        );

        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_is_a_per_sample_delta_not_the_raw_cumulative_counter() {
        // `deferred_total` reports a cumulative counter (ADR-3718: the sum of
        // every router's `flush_trigger_deferred_total`), already nonzero
        // from process start. If the loop pushed that raw value into its
        // window instead of the delta since the last sample, a process that
        // accumulated deferrals long ago but has none happening right now
        // would read as permanently saturated.
        let cpu_time = Arc::new(Mutex::new(Duration::ZERO));
        let gauge = Arc::new(CpuShedGauge::default());
        let detector = Detector::new(
            Box::new(FakeCpuSource(cpu_time.clone())),
            1,
            THRESHOLD,
            CAP,
            gauge.clone(),
        );
        // CPU alone is always at/above the release level, so the branch
        // taken depends entirely on whether `deferred_avg` reads as 0 (hold)
        // or as the raw 1000 (saturated).
        let handle = tokio::spawn(detector.run(Arc::new(SystemClock), || 1000));

        // Past a full window, so saturation is reachable at all: with fewer
        // than WINDOW_SAMPLES samples the deferred average reads 0 for every
        // implementation and this test would distinguish nothing.
        let mut cpu = Duration::ZERO;
        for _ in 0..(WINDOW_SAMPLES + 2) {
            cpu += Duration::from_millis(100);
            *cpu_time.lock().expect("lock") = cpu;
            tokio::time::advance(SAMPLE_INTERVAL).await;
            tokio::task::yield_now().await;
        }

        // A wrong implementation treating `deferred_total()`'s raw 1000 as
        // the per-sample count would compute `deferred_avg == 1000 > 0`,
        // read as saturated (cpu_avg == 1.0 >= threshold), and raise `p`
        // above 0 within one tick. This assertion fails on that
        // implementation at this exact line.
        assert_eq!(
            gauge.load(),
            0.0,
            "an unchanging cumulative counter must contribute a zero delta, not its raw value"
        );

        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn first_sample_measures_only_the_delta_since_task_start_not_the_full_baseline() {
        // The CPU source already reports 5s of accumulated process CPU time
        // when the detector starts (a realistic baseline for a process that
        // has been running a while). A wrong implementation that seeds
        // `last_cpu_time` at `Duration::ZERO` instead of reading the real
        // baseline before the first sleep would see the entire 5s as new use
        // on the very first sample.
        let cpu_time = Arc::new(Mutex::new(Duration::from_secs(5)));
        let gauge = Arc::new(CpuShedGauge::default());
        let detector = Detector::new(
            Box::new(FakeCpuSource(cpu_time.clone())),
            1,
            THRESHOLD,
            CAP,
            gauge.clone(),
        );
        // Deferrals grow every tick (deferred_avg > 0 throughout), so a
        // falsely saturated first sample (cpu_avg spiked to ~50 by the 5s
        // baseline divided by one 100ms tick) would show up as `p > 0`
        // immediately; a constant deferred count would hide the spike behind
        // the "hold" branch instead (needs deferred_avg > 0 too).
        let deferred_calls = Arc::new(AtomicU64::new(0));
        let deferred_for_task = deferred_calls.clone();
        let handle = tokio::spawn(detector.run(Arc::new(SystemClock), move || {
            deferred_for_task.fetch_add(3, Ordering::Relaxed)
        }));

        // Let the task capture its 5s baseline before changing the reading.
        tokio::task::yield_now().await;
        // Only 30ms of genuine CPU use during the first 100ms sample.
        *cpu_time.lock().expect("lock") = Duration::from_secs(5) + Duration::from_millis(30);
        tokio::time::advance(SAMPLE_INTERVAL).await;
        // A single `advance()` wakes the sleeping task but does not run it to
        // completion within the same poll; without this second yield the
        // real iteration below never executes and the assertion would pass
        // vacuously regardless of which baseline the implementation seeds.
        tokio::task::yield_now().await;

        // A wrong implementation would compute cpu_avg far above the
        // threshold on this first sample and saturate, raising `p` above 0.
        // This assertion fails on that implementation at this exact line.
        assert_eq!(
            gauge.load(),
            0.0,
            "the first sample must measure only the delta since task start, not the whole baseline"
        );

        handle.abort();
    }

    #[tokio::test(start_paused = true)]
    async fn saturation_requires_a_full_window_not_one_sample() {
        // A single 100ms sample at startup, however high, must not trip
        // saturation: ADR-3718 D1/D2 average over the full 1s/10-sample
        // window, not the latest sample alone.
        let cpu_time = Arc::new(Mutex::new(Duration::ZERO));
        let gauge = Arc::new(CpuShedGauge::default());
        let detector = Detector::new(
            Box::new(FakeCpuSource(cpu_time.clone())),
            1,
            THRESHOLD,
            CAP,
            gauge.clone(),
        );
        // Deferrals must grow every tick: saturation requires deferred_avg >
        // 0 (see `step`'s `saturated` condition above), so a constant
        // deferred_total would keep deferred_avg at 0 and mask saturation
        // regardless of cpu_avg.
        let deferred_calls = Arc::new(AtomicU64::new(0));
        let deferred_for_task = deferred_calls.clone();
        let handle = tokio::spawn(detector.run(Arc::new(SystemClock), move || {
            deferred_for_task.fetch_add(5, Ordering::Relaxed)
        }));
        // Let the task capture its pre-loop baseline (cpu=0) before the CPU
        // spike below is written, or the baseline capture itself races the
        // write and the first `advance()` is consumed entirely by it,
        // leaving no real sample computed at all.
        tokio::task::yield_now().await;

        // A full 1s of CPU time within the first 100ms tick: cpu_frac == 10,
        // far past the threshold.
        *cpu_time.lock().expect("lock") = Duration::from_secs(1);
        tokio::time::advance(SAMPLE_INTERVAL).await;
        // A single `advance()` only bumps the virtual clock and wakes the
        // sleeping task; it does not run that task to completion in the same
        // poll under this runtime, so a second yield is needed before the
        // woken iteration's own `gauge.store()` is observable here.
        tokio::task::yield_now().await;

        // One saturated sample must not trip saturation: the window is not
        // yet full, so the deferred average reads 0 and `p` stays at 0.
        assert_eq!(
            gauge.load(),
            0.0,
            "a single saturated sample before the 1 s window fills must not raise p"
        );

        // Keep the CPU saturated (1 s of CPU per 100 ms tick) until the
        // window holds its 10 samples: only then does `p` take its first step.
        let mut cpu = Duration::from_secs(1);
        for _ in 1..(WINDOW_SAMPLES - 1) {
            cpu += Duration::from_secs(1);
            *cpu_time.lock().expect("lock") = cpu;
            tokio::time::advance(SAMPLE_INTERVAL).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(
            gauge.load(),
            0.0,
            "nine samples are still short of a full window"
        );
        cpu += Duration::from_secs(1);
        *cpu_time.lock().expect("lock") = cpu;
        tokio::time::advance(SAMPLE_INTERVAL).await;
        tokio::task::yield_now().await;
        assert_eq!(
            gauge.load(),
            0.05,
            "the tenth saturated sample fills the window and raises p by exactly one step"
        );

        handle.abort();
    }

    /// Collects the message of every WARN event, mirroring
    /// `crates/ravel-ingest/tests/clustering_key_warning.rs`'s pattern.
    #[derive(Clone, Default)]
    struct WarnCapture {
        messages: Arc<Mutex<Vec<String>>>,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnCapture {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() != tracing::Level::WARN {
                return;
            }
            let mut visitor = MessageVisitor::default();
            event.record(&mut visitor);
            self.messages.lock().expect("lock").push(visitor.0);
        }
    }

    #[derive(Default)]
    struct MessageVisitor(String);

    impl tracing::field::Visit for MessageVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.0 = format!("{value:?}");
            }
        }
    }

    struct AlwaysUnavailable;

    impl CpuSource for AlwaysUnavailable {
        fn cpu_time(&self) -> Option<Duration> {
            None
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unavailable_cpu_source_keeps_p_at_zero_and_warns_once() {
        use tracing_subscriber::layer::SubscriberExt;

        let capture = WarnCapture::default();
        let subscriber = tracing_subscriber::registry().with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);

        let gauge = Arc::new(CpuShedGauge::default());
        let detector = Detector::new(
            Box::new(AlwaysUnavailable),
            1,
            THRESHOLD,
            CAP,
            gauge.clone(),
        );
        // `run` loops forever; spawn it and drive several sample ticks under
        // paused time rather than awaiting it inline, which would hang.
        let handle = tokio::spawn(detector.run(Arc::new(SystemClock), || 0));

        for _ in 0..5 {
            tokio::time::advance(SAMPLE_INTERVAL).await;
        }
        // Let the spawned task actually run past each advance before reading
        // its state.
        tokio::task::yield_now().await;

        assert_eq!(
            gauge.load(),
            0.0,
            "an unavailable CPU source must leave p at 0"
        );
        let warn_count = capture
            .messages
            .lock()
            .expect("lock")
            .iter()
            .filter(|m| m.contains("CPU-aware ingest admission (ADR-3718) disabled"))
            .count();
        assert_eq!(
            warn_count, 1,
            "an unavailable CPU source must warn exactly once, not per sample attempt"
        );

        handle.abort();
    }
}
