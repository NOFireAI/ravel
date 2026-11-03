//! ADR-3718: CPU-aware ingest admission.
//!
//! A background detector (spawned once in [`crate::start`]) samples the
//! process's own CPU use and the ingest routers' deferred-flush-trigger
//! counters every 100 ms, keeps 1-second rolling averages of both, and moves
//! a shed fraction `p` per [`step`] (ADR-3718 D2). Every ingest admission
//! path -- `admit_ingest_request` and `GrpcIngestAdmissionLayer::call` --
//! draws against the published `p` once, at the request head, before tenant
//! resolution or body work.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use rand::RngExt;
use ravel_ingest::Clock;

/// One sample every 100 ms (ADR-3718 D1).
const SAMPLE_INTERVAL: Duration = Duration::from_millis(100);
/// 1-second rolling average over 10 samples at the 100 ms sample interval
/// (ADR-3718 D1/D2).
const WINDOW_SAMPLES: usize = 10;

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
        let mut last_wall_ns = clock.now_ns();
        let mut last_deferred = deferred_total();
        let mut cpu_window: VecDeque<f64> = VecDeque::with_capacity(WINDOW_SAMPLES);
        let mut deferred_window: VecDeque<u64> = VecDeque::with_capacity(WINDOW_SAMPLES);
        let mut p = 0.0;

        loop {
            clock.sleep(SAMPLE_INTERVAL).await;

            let now_ns = clock.now_ns();
            let wall_delta_ns = (now_ns.saturating_sub(last_wall_ns)).max(1) as f64;
            last_wall_ns = now_ns;

            let cpu_frac = match self.cpu_source.cpu_time() {
                Some(now) => {
                    let delta_ns = now.as_nanos().saturating_sub(last_cpu_time.as_nanos()) as f64;
                    last_cpu_time = now;
                    delta_ns / wall_delta_ns / self.cores as f64
                }
                None => 0.0,
            };

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
            let deferred_avg =
                deferred_window.iter().sum::<u64>() as f64 / deferred_window.len() as f64;

            p = step(p, cpu_avg, deferred_avg, self.threshold, self.cap);
            self.gauge.store(p);
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
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
}
