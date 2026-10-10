//! Band checks for the ADR-1702 task 11 saturation scenarios (issue #2670).
//!
//! The two bins, `liveness_under_decode` and `heartbeat_under_query_load`,
//! capture their figures into a [`FigureSet`] and hand it to
//! [`evaluate_decode_liveness`] or [`evaluate_heartbeat_under_load`]. Both are
//! pure functions over the captured set, so every band is unit tested here
//! without a server. A figure a band reads must be present exactly once: an
//! absent or duplicated figure is a miss of that band, named by the figure.
//!
//! The bands were published on the tracking issue before either scenario ran.
//! They are constants here, not flags, so a run cannot move one to fit.
//!
//! This module also holds the Prometheus text parsing the bins scrape
//! `/metrics` with, and the host stamp each run prints.

use std::fmt;
use std::time::Duration;

/// Scenario 1: the health listener is probed on this period.
pub const PROBE_INTERVAL_MS: u64 = 100;
/// Scenario 1: the decode window covers at least this many probe slots.
pub const MIN_DECODE_PROBE_SLOTS: u64 = 10;
/// Scenario 1: the shortest decode window inside the band, the window
/// [`MIN_DECODE_PROBE_SLOTS`] probe slots take.
pub const MIN_DECODE_WINDOW_MS: f64 = (MIN_DECODE_PROBE_SLOTS * PROBE_INTERVAL_MS) as f64;
/// Scenario 1: the largest probe latency inside the band is just under this.
pub const MAX_PROBE_LATENCY_MS: f64 = 250.0;
/// Scenario 2: the expected band is a heartbeat age under this.
pub const HEARTBEAT_EXPECTED_S: f64 = 2.0;
/// Scenario 2: ADR-1702 decision 9's bound, a third of the 30 s readiness
/// threshold. An age at or above it is a hard miss.
pub const HEARTBEAT_ADR_BOUND_S: f64 = 10.0;
/// Scenario 2: the query load runs at least this long.
pub const MIN_LOAD_WINDOW_MS: f64 = 60_000.0;
/// Scenario 1: the decode unit ADR-1702 task 11 names, before any scale-down.
pub const DECODE_UNIT_BYTES: u64 = 256 * 1024 * 1024;

/// The heartbeat age family scenario 2 scrapes, one sample per scrape.
pub const HEARTBEAT_FAMILY: &str = "ravel_health_heartbeat_age_seconds";

/// The metric families both bins depend on, checked by exact name before any
/// load is driven.
pub const REQUIRED_FAMILIES: [&str; 3] = [
    HEARTBEAT_FAMILY,
    "ravel_cpu_gate_jobs_total",
    "ravel_cpu_gate_inline_total",
];

/// Figure names. One constant per figure, so a band and the bin that records
/// it cannot drift apart by a typo.
pub mod fig {
    pub const WINDOW_MS: &str = "window_ms";
    pub const PROBES_ISSUED: &str = "probes_issued";
    pub const PROBE_SLOTS_COVERED: &str = "probe_slots_covered";
    pub const PROBES_ANSWERED_200: &str = "probes_answered_200";
    pub const PROBE_LATENCY_MS: &str = "probe_latency_ms";
    pub const PROBE_WAKE_LATENESS_MS: &str = "probe_wake_lateness_ms";
    pub const INLINE_JOBS: &str = "inline_jobs";
    pub const DECODES_ISSUED: &str = "decodes_issued";
    pub const DECODE_JOBS: &str = "decode_jobs";
    pub const DECODE_UNIT_BYTES: &str = "decode_unit_bytes";
    pub const DECODE_UNIT_TARGET_BYTES: &str = "decode_unit_target_bytes";
    pub const QUERIES_FAILED: &str = "queries_failed";
    pub const PROMQL_QUERIES_OK: &str = "promql_queries_ok";
    pub const SQL_QUERIES_OK: &str = "sql_queries_ok";
    pub const HEARTBEAT_AGE_S: &str = "heartbeat_age_s";
    pub const HEARTBEAT_SCRAPES_FAILED: &str = "heartbeat_scrapes_failed";
    pub const HEARTBEAT_SCRAPE_LATENCY_S: &str = "heartbeat_scrape_latency_s";
    pub const HEARTBEAT_UNANSWERED_LATENCY_S: &str = "heartbeat_unanswered_latency_s";
    pub const READYZ_PROBES_ISSUED: &str = "readyz_probes_issued";
    pub const READYZ_ANSWERED_200: &str = "readyz_answered_200";
}

/// Band names, as the bins print them and as a miss is reported.
pub mod band {
    pub const DECODE_WINDOW: &str = "decode_window";
    pub const PROBE_SLOTS_COVERED: &str = "probe_slots_covered";
    pub const PROBES_ANSWERED: &str = "probes_answered";
    pub const PROBE_LATENCY_MAX: &str = "probe_latency_max";
    pub const INLINE_JOBS: &str = "inline_jobs";
    pub const DECODE_JOBS: &str = "decode_jobs";
    pub const DECODE_UNIT_SIZE: &str = "decode_unit_size";
    pub const QUERIES_FAILED: &str = "queries_failed";
    pub const LOAD_WINDOW: &str = "load_window";
    pub const PROMQL_QUERIES: &str = "promql_queries";
    pub const SQL_QUERIES: &str = "sql_queries";
    pub const HEARTBEAT_SCRAPES: &str = "heartbeat_scrapes";
    pub const HEARTBEAT_ADR_BOUND: &str = "heartbeat_age_adr_bound";
    pub const HEARTBEAT_EXPECTED: &str = "heartbeat_age_expected";
    pub const READYZ_PROBES: &str = "readyz_probes";
    pub const READYZ_ALL_200: &str = "readyz_all_200";
}

/// The figures one run captured: scalars, and sample series a band reduces
/// (probe latencies, heartbeat ages).
#[derive(Debug, Default, Clone)]
pub struct FigureSet {
    scalars: Vec<(&'static str, f64)>,
    series: Vec<(&'static str, Vec<f64>)>,
}

/// Why a figure a band reads could not be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FigureError {
    Absent(&'static str),
    Duplicated(&'static str),
}

impl fmt::Display for FigureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FigureError::Absent(name) => write!(f, "figure {name} absent"),
            FigureError::Duplicated(name) => write!(f, "figure {name} recorded more than once"),
        }
    }
}

impl FigureSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, name: &'static str, value: f64) {
        self.scalars.push((name, value));
    }

    /// Records a sample series. An empty series is recorded as present and
    /// reduces to no value, which its band reports as absent.
    pub fn record_series(&mut self, name: &'static str, values: Vec<f64>) {
        self.series.push((name, values));
    }

    pub fn scalar(&self, name: &'static str) -> Result<f64, FigureError> {
        let mut found = self.scalars.iter().filter(|(n, _)| *n == name);
        match (found.next(), found.next()) {
            (None, _) => Err(FigureError::Absent(name)),
            (Some(_), Some(_)) => Err(FigureError::Duplicated(name)),
            (Some((_, v)), None) => Ok(*v),
        }
    }

    /// The samples of a series, which may be empty.
    pub fn series_values(&self, name: &'static str) -> Result<&[f64], FigureError> {
        let mut found = self.series.iter().filter(|(n, _)| *n == name);
        match (found.next(), found.next()) {
            (None, _) => Err(FigureError::Absent(name)),
            (Some(_), Some(_)) => Err(FigureError::Duplicated(name)),
            (Some((_, values)), None) => Ok(values),
        }
    }

    /// The largest sample of a series. Max, not mean: one slow probe among
    /// many fast ones is exactly what the latency band exists to catch.
    pub fn series_max(&self, name: &'static str) -> Result<f64, FigureError> {
        self.series_values(name)?
            .iter()
            .copied()
            .reduce(f64::max)
            .ok_or(FigureError::Absent(name))
    }
}

/// One band's verdict on one run.
#[derive(Debug, Clone, PartialEq)]
pub struct BandOutcome {
    pub band: &'static str,
    /// The rule, as printed beside the figure.
    pub rule: String,
    /// The figure the band read, or why it could not.
    pub value: Result<f64, FigureError>,
    pub inside: bool,
    /// A hard miss: outside a bound the ADR itself sets, not only outside the
    /// expected band.
    pub hard: bool,
}

impl BandOutcome {
    fn new(
        band: &'static str,
        rule: String,
        value: Result<f64, FigureError>,
        hard: bool,
        test: impl FnOnce(f64) -> bool,
    ) -> Self {
        let inside = value.map(test).unwrap_or(false);
        BandOutcome {
            band,
            rule,
            value,
            inside,
            hard,
        }
    }

    /// The line a bin prints for this band, once per run.
    pub fn line(&self) -> String {
        let verdict = match (self.inside, self.hard) {
            (true, _) => "INSIDE",
            (false, true) => "HARD MISS",
            (false, false) => "MISS",
        };
        match &self.value {
            Ok(v) => format!(
                "band {}: {} (band: {}) {verdict}",
                self.band,
                fmt_num(*v),
                self.rule
            ),
            Err(e) => format!("band {}: {e} (band: {}) {verdict}", self.band, self.rule),
        }
    }
}

fn fmt_num(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{v:.3}")
    }
}

/// The first band outside, in evaluation order: the band a bin names when it
/// exits non-zero. `None` when every band holds.
pub fn first_miss(outcomes: &[BandOutcome]) -> Option<&BandOutcome> {
    outcomes.iter().find(|o| !o.inside)
}

fn both(
    a: Result<f64, FigureError>,
    b: Result<f64, FigureError>,
) -> Result<(f64, f64), FigureError> {
    Ok((a?, b?))
}

/// Scenario 1, decode saturation versus liveness. Bands, in order:
///
/// - `decode_window`: the measured window is at least 1000 ms, so it covers
///   at least 10 probe slots. A decode too short to probe is a miss here
///   rather than a pass with nothing probed.
/// - `probes_answered`: every probe recorded for a slot due in the window
///   answered 200.
/// - `probe_latency_max`: the slowest of those probes took under 250 ms,
///   including probes that cover no slot. It is read before
///   `probe_slots_covered`, so a slow listener is named as slow.
/// - `probe_slots_covered`: exactly `floor(window_ms / 100)` slots are
///   covered, every slot due in the window. Slot `k` is due `k * 100` ms
///   after the window start, and it is covered when either
///   (a) a probe for slot `k` was issued no more than one period after its
///   due time, no later than the window's end, and answered 200, or
///   (b) a probe that covers its own slot by (a), with a latency under the
///   250 ms latency band, was issued at or before slot `k`'s due time and
///   answered after it, so the listener was being probed when slot `k` fell
///   due.
///   A probe issued more than one period after its slot's due time, or after
///   the window's end, covers nothing, so a prober that was not scheduled
///   for part of the window cannot make that stretch up with probes sent
///   later. [`slot_coverage`] applies the rule.
/// - `inline_jobs`: `ravel_cpu_gate_inline_total` moved by exactly 0 over the
///   window, summed over every gate and site.
/// - `decode_jobs`: the read gate ran exactly one catalog decode job per
///   query issued, so the load the bands are read under actually happened.
/// - `decode_unit_size`: each decode unit is at least the target size.
/// - `queries_failed`: no decode query failed.
pub fn evaluate_decode_liveness(figs: &FigureSet) -> Vec<BandOutcome> {
    let window = figs.scalar(fig::WINDOW_MS);
    let issued = figs.scalar(fig::PROBES_ISSUED);
    let expected_slots = window.map(|w| (w / PROBE_INTERVAL_MS as f64).floor());
    let slots_covered = match both(figs.scalar(fig::PROBE_SLOTS_COVERED), expected_slots) {
        Ok((covered, expected)) => BandOutcome::new(
            band::PROBE_SLOTS_COVERED,
            format!(
                "== floor(window_ms / {PROBE_INTERVAL_MS}) = {}",
                fmt_num(expected)
            ),
            Ok(covered),
            false,
            |v| v == expected,
        ),
        Err(e) => BandOutcome::new(
            band::PROBE_SLOTS_COVERED,
            format!("== floor(window_ms / {PROBE_INTERVAL_MS})"),
            Err(e),
            false,
            |_| false,
        ),
    };
    let answered = figs.scalar(fig::PROBES_ANSWERED_200);
    let probes_answered = match both(answered, issued) {
        Ok((answered, issued)) => BandOutcome::new(
            band::PROBES_ANSWERED,
            format!("== probes_issued = {}", fmt_num(issued)),
            Ok(answered),
            false,
            |v| v == issued,
        ),
        Err(e) => BandOutcome::new(
            band::PROBES_ANSWERED,
            "== probes_issued".to_string(),
            Err(e),
            false,
            |_| false,
        ),
    };
    let decodes = figs.scalar(fig::DECODES_ISSUED);
    let decode_jobs = match both(figs.scalar(fig::DECODE_JOBS), decodes) {
        Ok((jobs, decodes)) => BandOutcome::new(
            band::DECODE_JOBS,
            format!("== decodes_issued = {}", fmt_num(decodes)),
            Ok(jobs),
            false,
            |v| v == decodes,
        ),
        Err(e) => BandOutcome::new(
            band::DECODE_JOBS,
            "== decodes_issued".to_string(),
            Err(e),
            false,
            |_| false,
        ),
    };
    let unit = figs.scalar(fig::DECODE_UNIT_BYTES);
    let decode_unit = match both(unit, figs.scalar(fig::DECODE_UNIT_TARGET_BYTES)) {
        Ok((unit, target)) => BandOutcome::new(
            band::DECODE_UNIT_SIZE,
            format!(">= decode_unit_target_bytes = {}", fmt_num(target)),
            Ok(unit),
            false,
            |v| v >= target,
        ),
        Err(e) => BandOutcome::new(
            band::DECODE_UNIT_SIZE,
            ">= decode_unit_target_bytes".to_string(),
            Err(e),
            false,
            |_| false,
        ),
    };
    vec![
        BandOutcome::new(
            band::DECODE_WINDOW,
            format!(
                ">= {} ms ({MIN_DECODE_PROBE_SLOTS} probe slots)",
                fmt_num(MIN_DECODE_WINDOW_MS)
            ),
            window,
            false,
            |v| v >= MIN_DECODE_WINDOW_MS,
        ),
        probes_answered,
        BandOutcome::new(
            band::PROBE_LATENCY_MAX,
            format!("max < {} ms", fmt_num(MAX_PROBE_LATENCY_MS)),
            figs.series_max(fig::PROBE_LATENCY_MS),
            false,
            |v| v < MAX_PROBE_LATENCY_MS,
        ),
        slots_covered,
        BandOutcome::new(
            band::INLINE_JOBS,
            "== 0".to_string(),
            figs.scalar(fig::INLINE_JOBS),
            false,
            |v| v == 0.0,
        ),
        decode_jobs,
        decode_unit,
        BandOutcome::new(
            band::QUERIES_FAILED,
            "== 0".to_string(),
            figs.scalar(fig::QUERIES_FAILED),
            false,
            |v| v == 0.0,
        ),
    ]
}

/// Scenario 2, query saturation versus the heartbeat. Bands, in order:
///
/// - `load_window`: the query load ran at least 60 s.
/// - `promql_queries`, `sql_queries`: at least one query of each kind
///   completed, so both kinds of load were present.
/// - `queries_failed`: no query failed.
/// - `heartbeat_age_adr_bound`: the largest scraped
///   `ravel_health_heartbeat_age_seconds` is under 10 s. At or above is a
///   hard miss of ADR-1702 decision 9's bound. A scrape that returned no age
///   counts its own latency as an age: `/metrics` is served by the runtime
///   being measured, so an unanswered scrape is a stall of at least that
///   long that no age sample saw. It is read before `heartbeat_scrapes`, so
///   a scrape that timed out past 10 s is named as the hard miss.
/// - `heartbeat_scrapes`: no `/metrics` scrape failed, so no heartbeat
///   sample was lost.
/// - `heartbeat_age_expected`: the same maximum is under 2 s. Between 2 s and
///   10 s it is outside the expected band and inside the ADR's.
/// - `readyz_probes`: at least one `/readyz` probe was issued.
/// - `readyz_all_200`: every `/readyz` probe on the health listener answered
///   200.
pub fn evaluate_heartbeat_under_load(figs: &FigureSet) -> Vec<BandOutcome> {
    let heartbeat = heartbeat_age_max(figs);
    let readyz_issued = figs.scalar(fig::READYZ_PROBES_ISSUED);
    let readyz_all = match both(figs.scalar(fig::READYZ_ANSWERED_200), readyz_issued) {
        Ok((answered, issued)) => BandOutcome::new(
            band::READYZ_ALL_200,
            format!("== readyz_probes_issued = {}", fmt_num(issued)),
            Ok(answered),
            false,
            |v| v == issued,
        ),
        Err(e) => BandOutcome::new(
            band::READYZ_ALL_200,
            "== readyz_probes_issued".to_string(),
            Err(e),
            false,
            |_| false,
        ),
    };
    vec![
        BandOutcome::new(
            band::LOAD_WINDOW,
            format!(">= {} ms", fmt_num(MIN_LOAD_WINDOW_MS)),
            figs.scalar(fig::WINDOW_MS),
            false,
            |v| v >= MIN_LOAD_WINDOW_MS,
        ),
        BandOutcome::new(
            band::PROMQL_QUERIES,
            ">= 1".to_string(),
            figs.scalar(fig::PROMQL_QUERIES_OK),
            false,
            |v| v >= 1.0,
        ),
        BandOutcome::new(
            band::SQL_QUERIES,
            ">= 1".to_string(),
            figs.scalar(fig::SQL_QUERIES_OK),
            false,
            |v| v >= 1.0,
        ),
        BandOutcome::new(
            band::QUERIES_FAILED,
            "== 0".to_string(),
            figs.scalar(fig::QUERIES_FAILED),
            false,
            |v| v == 0.0,
        ),
        BandOutcome::new(
            band::HEARTBEAT_ADR_BOUND,
            format!("max < {} s", fmt_num(HEARTBEAT_ADR_BOUND_S)),
            heartbeat,
            true,
            |v| v < HEARTBEAT_ADR_BOUND_S,
        ),
        BandOutcome::new(
            band::HEARTBEAT_SCRAPES,
            "failed == 0".to_string(),
            figs.scalar(fig::HEARTBEAT_SCRAPES_FAILED),
            false,
            |v| v == 0.0,
        ),
        BandOutcome::new(
            band::HEARTBEAT_EXPECTED,
            format!("max < {} s", fmt_num(HEARTBEAT_EXPECTED_S)),
            heartbeat,
            false,
            |v| v < HEARTBEAT_EXPECTED_S,
        ),
        BandOutcome::new(
            band::READYZ_PROBES,
            ">= 1".to_string(),
            readyz_issued,
            false,
            |v| v >= 1.0,
        ),
        readyz_all,
    ]
}

/// The figure both heartbeat bands read: the largest scraped age, or the
/// latency of a scrape that returned no age when that is larger.
fn heartbeat_age_max(figs: &FigureSet) -> Result<f64, FigureError> {
    let age = figs.series_max(fig::HEARTBEAT_AGE_S)?;
    let unanswered = figs.series_values(fig::HEARTBEAT_UNANSWERED_LATENCY_S)?;
    Ok(unanswered.iter().copied().fold(age, f64::max))
}

/// One probe the prober issued.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Probe {
    /// The slot it was issued for: slot `k` is due `k` probe intervals after
    /// the window start, counting from 1.
    pub slot: u64,
    /// When it was issued, from the window start.
    pub issued_at: Duration,
    pub latency: Duration,
    pub status: Option<u16>,
}

/// The number of probe slots due within `window`: slots `1..=slots_due` are
/// due at or before its end. Equal to `floor(window_ms / 100)`, the count the
/// `probe_slots_covered` band expects.
pub fn slots_due(window: Duration) -> u64 {
    let period = u128::from(PROBE_INTERVAL_MS) * 1_000_000;
    u64::try_from(window.as_nanos() / period).unwrap_or(u64::MAX)
}

/// The slot the prober sends after `probe`, with slots `period` apart. Only
/// the probe's own latency, from issue to answer, measured from its slot's
/// due time, decides how many slots it skips: a slot is skipped when that
/// duration crossed the slot's due time, never because the prober woke late
/// to send it. A probe answered exactly at a later slot's due time does not
/// skip that slot.
pub fn next_probe_slot(probe: &Probe, period: Duration) -> u64 {
    let crossed = probe.latency.as_nanos().div_ceil(period.as_nanos().max(1));
    probe
        .slot
        .saturating_add(u64::try_from(crossed).unwrap_or(u64::MAX).max(1))
}

/// What the prober does next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProberStep {
    /// Sleep until this slot's due time and probe it.
    Send(u64),
    /// The window closed before the next slot's due time.
    Stop,
}

/// The prober loop's decision: the slot after `previous` (slot 1 before the
/// first probe, else [`next_probe_slot`]), or [`ProberStep::Stop`] when that
/// slot is past `last_slot`, the window's last slot (`u64::MAX` while the
/// window is open). Waking late does not move the prober forward: the
/// coverage rule ([`slot_coverage`]) is what refuses a late probe.
pub fn prober_step(previous: Option<&Probe>, period: Duration, last_slot: u64) -> ProberStep {
    let slot = previous.map_or(1, |p| next_probe_slot(p, period));
    if slot > last_slot {
        ProberStep::Stop
    } else {
        ProberStep::Send(slot)
    }
}

/// When slot `slot` is due, from the window start.
fn slot_due(slot: u64) -> Duration {
    u32::try_from(slot)
        .ok()
        .and_then(|slot| Duration::from_millis(PROBE_INTERVAL_MS).checked_mul(slot))
        .unwrap_or(Duration::MAX)
}

/// How late the prober woke to send `probe`: its issue time minus its slot's
/// due time, zero when it was on time.
pub fn wake_lateness(probe: &Probe) -> Duration {
    probe.issued_at.saturating_sub(slot_due(probe.slot))
}

/// Whether `probe` covers its own slot under rule (a) of the
/// `probe_slots_covered` band: issued no more than one period after its
/// slot's due time, no later than `window`, and answered 200.
fn covers_own_slot(probe: &Probe, window: Duration) -> bool {
    let period = Duration::from_millis(PROBE_INTERVAL_MS);
    probe.slot >= 1
        && probe.status == Some(200)
        && probe.issued_at <= window
        && probe.issued_at <= slot_due(probe.slot).saturating_add(period)
}

/// The probe slots due in a window and how many of them were covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotCoverage {
    pub due: u64,
    pub covered: u64,
}

impl SlotCoverage {
    pub fn uncovered(&self) -> u64 {
        self.due - self.covered
    }
}

/// Applies the `probe_slots_covered` rule ([`evaluate_decode_liveness`]) to
/// the probes a prober returned: slot `k` of `1..=slots_due(window)` is
/// covered when a probe for it covers its own slot, or when a probe that
/// covers its own slot, with a latency under [`MAX_PROBE_LATENCY_MS`], was
/// issued at or before slot `k`'s due time and answered after it.
pub fn slot_coverage(probes: &[Probe], window: Duration) -> SlotCoverage {
    let due = slots_due(window);
    let latency_band = Duration::from_secs_f64(MAX_PROBE_LATENCY_MS / 1000.0);
    let covering: Vec<&Probe> = probes
        .iter()
        .filter(|p| covers_own_slot(p, window))
        .collect();
    let covered = (1..=due)
        .filter(|&k| {
            let at = slot_due(k);
            covering.iter().any(|p| {
                p.slot == k
                    || (p.latency < latency_band
                        && p.issued_at <= at
                        && p.issued_at.saturating_add(p.latency) > at)
            })
        })
        .count();
    SlotCoverage {
        due,
        covered: u64::try_from(covered).unwrap_or(u64::MAX),
    }
}

/// The line a bin prints once per run with the prober's largest wake
/// lateness, read from the recorded `probe_wake_lateness_ms` series. No band
/// reads it: it describes the prober, not the server.
pub fn wake_lateness_line(figs: &FigureSet) -> String {
    match figs.series_max(fig::PROBE_WAKE_LATENESS_MS) {
        Ok(ms) => format!("prober wake lateness: max {ms:.3} ms (no band)"),
        Err(e) => format!("prober wake lateness: {e} (no band)"),
    }
}

fn record_wake_lateness(figs: &mut FigureSet, in_window: &[&Probe]) {
    figs.record_series(
        fig::PROBE_WAKE_LATENESS_MS,
        in_window
            .iter()
            .map(|p| wake_lateness(p).as_secs_f64() * 1000.0)
            .collect(),
    );
}

/// The probes whose slot was due within `window`, whenever the prober
/// actually woke to send them.
pub fn probes_in_window(probes: &[Probe], window: Duration) -> Vec<&Probe> {
    let due = slots_due(window);
    probes.iter().filter(|p| p.slot <= due).collect()
}

/// The window in milliseconds, unrounded.
pub fn window_ms(window: Duration) -> f64 {
    window.as_nanos() as f64 / 1e6
}

/// Records scenario 1's window and probe figures from the probes the prober
/// returned.
pub fn record_decode_probes(figs: &mut FigureSet, window: Duration, probes: &[Probe]) {
    let in_window = probes_in_window(probes, window);
    figs.record(fig::WINDOW_MS, window_ms(window));
    figs.record(fig::PROBES_ISSUED, in_window.len() as f64);
    figs.record(
        fig::PROBE_SLOTS_COVERED,
        slot_coverage(probes, window).covered as f64,
    );
    figs.record(
        fig::PROBES_ANSWERED_200,
        in_window.iter().filter(|p| p.status == Some(200)).count() as f64,
    );
    figs.record_series(
        fig::PROBE_LATENCY_MS,
        in_window
            .iter()
            .map(|p| p.latency.as_secs_f64() * 1000.0)
            .collect(),
    );
    record_wake_lateness(figs, &in_window);
}

/// Records scenario 2's `/readyz` probe figures.
pub fn record_readyz_probes(figs: &mut FigureSet, window: Duration, probes: &[Probe]) {
    let in_window = probes_in_window(probes, window);
    figs.record(fig::READYZ_PROBES_ISSUED, in_window.len() as f64);
    figs.record(
        fig::READYZ_ANSWERED_200,
        in_window.iter().filter(|p| p.status == Some(200)).count() as f64,
    );
    record_wake_lateness(figs, &in_window);
}

/// One heartbeat scrape: how long `/metrics` took to answer (or to fail),
/// and the age it returned, if any.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Scrape {
    pub latency: Duration,
    pub age_s: Option<f64>,
}

/// Records scenario 2's heartbeat figures: the ages returned, every scrape's
/// latency, the failed count, and the latency of each scrape that returned no
/// age, which the heartbeat bands read as an age.
pub fn record_heartbeat_scrapes(figs: &mut FigureSet, scrapes: &[Scrape]) {
    figs.record_series(
        fig::HEARTBEAT_AGE_S,
        scrapes.iter().filter_map(|s| s.age_s).collect(),
    );
    figs.record_series(
        fig::HEARTBEAT_SCRAPE_LATENCY_S,
        scrapes.iter().map(|s| s.latency.as_secs_f64()).collect(),
    );
    let unanswered: Vec<f64> = scrapes
        .iter()
        .filter(|s| s.age_s.is_none())
        .map(|s| s.latency.as_secs_f64())
        .collect();
    figs.record(fig::HEARTBEAT_SCRAPES_FAILED, unanswered.len() as f64);
    figs.record_series(fig::HEARTBEAT_UNANSWERED_LATENCY_S, unanswered);
}

/// The scrape figures a reader needs to tell a stalled heartbeat from a slow
/// scrape: the slowest scrape, and the latency of the scrape that returned
/// the largest age.
pub fn scrape_summary(scrapes: &[Scrape]) -> String {
    let slowest = scrapes
        .iter()
        .map(|s| s.latency.as_secs_f64())
        .reduce(f64::max);
    let at_max_age = scrapes
        .iter()
        .filter_map(|s| s.age_s.map(|a| (a, s.latency.as_secs_f64())))
        .reduce(|a, b| if b.0 > a.0 { b } else { a });
    let slowest = slowest.map_or("none".to_string(), |s| format!("{s:.3} s"));
    let at_max_age = at_max_age.map_or("none".to_string(), |(age, latency)| {
        format!("age {age:.3} s returned by a scrape of {latency:.3} s")
    });
    format!("heartbeat scrapes: slowest {slowest}, largest {at_max_age}")
}

/// The change in the sum of `family` (filtered as [`family_sum`] filters)
/// between two scrapes. Fails naming the family when either scrape has no
/// sample of exactly that name, so a family that vanished reads as a failure
/// rather than as 0.
pub fn family_delta(
    before: &str,
    after: &str,
    family: &'static str,
    label_filter: &[&str],
) -> Result<f64, String> {
    for (body, when) in [(before, "before"), (after, "after")] {
        if !missing_families(body, &[family]).is_empty() {
            return Err(format!(
                "metric family {family} missing from /metrics {when} the window"
            ));
        }
    }
    Ok(family_sum(after, family, label_filter) - family_sum(before, family, label_filter))
}

/// The note a scaled-down decode run carries on its RESULT line, so a result
/// at a reduced decode unit cannot be read as the published scenario's.
/// `None` at divisor 1.
pub fn scale_note(divisor: u64) -> Option<String> {
    if divisor <= 1 {
        return None;
    }
    let mib = |bytes: u64| {
        if bytes.is_multiple_of(1024 * 1024) {
            format!("{} MiB", bytes / (1024 * 1024))
        } else {
            format!("{bytes} bytes")
        }
    };
    Some(format!(
        "scaled: decode unit {}, divisor {divisor}; the published scenario is {}",
        mib(DECODE_UNIT_BYTES / divisor),
        mib(DECODE_UNIT_BYTES)
    ))
}

/// One sample line of a Prometheus text exposition: the metric name, the raw
/// label block (without braces, empty when there is none) and the value.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample<'a> {
    pub name: &'a str,
    pub labels: &'a str,
    pub value: f64,
}

/// The sample lines of `body`, skipping comments and lines that do not parse.
pub fn parse_samples(body: &str) -> Vec<Sample<'_>> {
    body.lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
        .filter_map(|line| {
            let (series, value) = line.rsplit_once(' ')?;
            let value: f64 = value.trim().parse().ok()?;
            let (name, labels) = match series.split_once('{') {
                Some((name, rest)) => (name, rest.strip_suffix('}')?),
                None => (series, ""),
            };
            Some(Sample {
                name,
                labels,
                value,
            })
        })
        .collect()
}

/// The families of `required` with no sample of exactly that name in `body`.
pub fn missing_families<'r>(body: &str, required: &[&'r str]) -> Vec<&'r str> {
    let samples = parse_samples(body);
    required
        .iter()
        .copied()
        .filter(|family| !samples.iter().any(|s| s.name == *family))
        .collect()
}

/// The start-of-run check on a `/metrics` body: every family of
/// [`REQUIRED_FAMILIES`] present by exact name, and the heartbeat family
/// carrying exactly one sample, since a scrape reads its age with
/// [`single_value`].
pub fn check_start_families(body: &str) -> Result<(), String> {
    let missing = missing_families(body, &REQUIRED_FAMILIES);
    if !missing.is_empty() {
        return Err(format!(
            "metric families missing from /metrics: {}",
            missing.join(", ")
        ));
    }
    let samples = parse_samples(body)
        .iter()
        .filter(|s| s.name == HEARTBEAT_FAMILY)
        .count();
    if samples != 1 {
        return Err(format!(
            "metric family {HEARTBEAT_FAMILY} carries {samples} samples in /metrics, expected exactly 1"
        ));
    }
    Ok(())
}

/// The sum of every sample of exactly `name` whose label block contains every
/// `label_filter` entry (each a `key="value"` pair).
pub fn family_sum(body: &str, name: &str, label_filter: &[&str]) -> f64 {
    parse_samples(body)
        .iter()
        .filter(|s| s.name == name && label_filter.iter().all(|f| s.labels.contains(f)))
        .map(|s| s.value)
        .sum()
}

/// The value of the one sample of exactly `name`, or `None` when there is
/// not exactly one.
pub fn single_value(body: &str, name: &str) -> Option<f64> {
    let samples = parse_samples(body);
    let mut found = samples.iter().filter(|s| s.name == name);
    match (found.next(), found.next()) {
        (Some(s), None) => Some(s.value),
        _ => None,
    }
}

/// The host a run measured on, printed at the start of each run.
#[derive(Debug, Clone, PartialEq)]
pub struct HostStamp {
    pub arch: &'static str,
    pub cores: usize,
    pub mem_total_kib: Option<u64>,
    pub mem_available_kib: Option<u64>,
    /// The 1, 5 and 15 minute load averages.
    pub load: Option<(f64, f64, f64)>,
}

impl HostStamp {
    /// Reads the host from `/proc`. A field `/proc` cannot answer stays
    /// `None` and prints as unknown.
    pub fn detect() -> Self {
        let meminfo = std::fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let loadavg = std::fs::read_to_string("/proc/loadavg").unwrap_or_default();
        HostStamp {
            arch: std::env::consts::ARCH,
            cores: std::thread::available_parallelism().map_or(1, |n| n.get()),
            mem_total_kib: meminfo_kib(&meminfo, "MemTotal"),
            mem_available_kib: meminfo_kib(&meminfo, "MemAvailable"),
            load: parse_loadavg(&loadavg),
        }
    }

    pub fn line(&self) -> String {
        let kib = |v: Option<u64>| v.map_or("unknown".to_string(), |k| format!("{k} kB"));
        let load = self.load.map_or("unknown".to_string(), |(a, b, c)| {
            format!("{a:.2} {b:.2} {c:.2}")
        });
        format!(
            "host: arch={} cores={} mem_total={} mem_available={} loadavg={}",
            self.arch,
            self.cores,
            kib(self.mem_total_kib),
            kib(self.mem_available_kib),
            load
        )
    }
}

/// The `key:` line of a `/proc/meminfo` body, in KiB.
pub fn meminfo_kib(meminfo: &str, key: &str) -> Option<u64> {
    meminfo.lines().find_map(|line| {
        let rest = line.strip_prefix(key)?.strip_prefix(':')?;
        rest.split_whitespace().next()?.parse().ok()
    })
}

/// The three load averages of a `/proc/loadavg` body.
pub fn parse_loadavg(loadavg: &str) -> Option<(f64, f64, f64)> {
    let mut it = loadavg.split_whitespace().map(|v| v.parse::<f64>().ok());
    Some((it.next()??, it.next()??, it.next()??))
}

/// The power-of-two divisor that scales [`DECODE_UNIT_BYTES`] down until
/// `decodes` concurrent decodes, each holding `bytes_per_unit_byte` bytes of
/// memory per byte of decode unit, fit in half of `available_bytes`. 1 when
/// the full size fits. The divisor is printed with every run.
pub fn decode_scale_divisor(decodes: u64, bytes_per_unit_byte: u64, available_bytes: u64) -> u64 {
    let budget = available_bytes / 2;
    let mut divisor = 1u64;
    while divisor < DECODE_UNIT_BYTES
        && (DECODE_UNIT_BYTES / divisor)
            .saturating_mul(decodes)
            .saturating_mul(bytes_per_unit_byte)
            > budget
    {
        divisor *= 2;
    }
    divisor
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Scenario 1 figures inside every band: a 1234 ms window, 12 probes.
    fn decode_inside() -> FigureSet {
        let mut f = FigureSet::new();
        f.record(fig::WINDOW_MS, 1234.0);
        f.record(fig::PROBES_ISSUED, 12.0);
        f.record(fig::PROBE_SLOTS_COVERED, 12.0);
        f.record(fig::PROBES_ANSWERED_200, 12.0);
        f.record_series(fig::PROBE_LATENCY_MS, vec![3.0, 249.9, 5.0]);
        f.record(fig::INLINE_JOBS, 0.0);
        f.record(fig::DECODES_ISSUED, 16.0);
        f.record(fig::DECODE_JOBS, 16.0);
        f.record(fig::DECODE_UNIT_BYTES, 300.0);
        f.record(fig::DECODE_UNIT_TARGET_BYTES, 256.0);
        f.record(fig::QUERIES_FAILED, 0.0);
        f
    }

    fn heartbeat_inside() -> FigureSet {
        let mut f = FigureSet::new();
        f.record(fig::WINDOW_MS, 60_000.0);
        f.record(fig::PROMQL_QUERIES_OK, 40.0);
        f.record(fig::SQL_QUERIES_OK, 40.0);
        f.record(fig::QUERIES_FAILED, 0.0);
        f.record(fig::HEARTBEAT_SCRAPES_FAILED, 0.0);
        f.record_series(fig::HEARTBEAT_AGE_S, vec![0.1, 1.999, 0.5]);
        f.record_series(fig::HEARTBEAT_UNANSWERED_LATENCY_S, Vec::new());
        f.record(fig::READYZ_PROBES_ISSUED, 600.0);
        f.record(fig::READYZ_ANSWERED_200, 600.0);
        f
    }

    /// `base` with `name` replaced by `value`.
    fn with(base: &FigureSet, name: &'static str, value: f64) -> FigureSet {
        let mut f = base.clone();
        f.scalars.retain(|(n, _)| *n != name);
        f.record(name, value);
        f
    }

    fn with_series(base: &FigureSet, name: &'static str, values: Vec<f64>) -> FigureSet {
        let mut f = base.clone();
        f.series.retain(|(n, _)| *n != name);
        f.record_series(name, values);
        f
    }

    fn without(base: &FigureSet, name: &'static str) -> FigureSet {
        let mut f = base.clone();
        f.scalars.retain(|(n, _)| *n != name);
        f.series.retain(|(n, _)| *n != name);
        f
    }

    fn decode_miss(f: &FigureSet) -> Option<&'static str> {
        first_miss(&evaluate_decode_liveness(f)).map(|o| o.band)
    }

    fn heartbeat_miss(f: &FigureSet) -> Option<&'static str> {
        first_miss(&evaluate_heartbeat_under_load(f)).map(|o| o.band)
    }

    #[test]
    fn figure_sets_inside_every_band_return_no_miss() {
        assert_eq!(decode_miss(&decode_inside()), None);
        assert_eq!(heartbeat_miss(&heartbeat_inside()), None);
    }

    #[test]
    fn one_slot_short_misses_probe_slots_covered() {
        let f = with(&decode_inside(), fig::PROBE_SLOTS_COVERED, 11.0);
        assert_eq!(decode_miss(&f), Some(band::PROBE_SLOTS_COVERED));
    }

    #[test]
    fn one_extra_slot_misses_probe_slots_covered() {
        let f = with(&decode_inside(), fig::PROBE_SLOTS_COVERED, 13.0);
        assert_eq!(decode_miss(&f), Some(band::PROBE_SLOTS_COVERED));
    }

    #[test]
    fn slot_count_follows_the_floor_of_the_window() {
        // 1299 ms floors to 12 slots; 1300 ms needs 13.
        let f = with(&decode_inside(), fig::WINDOW_MS, 1299.0);
        assert_eq!(decode_miss(&f), None);
        let f = with(&decode_inside(), fig::WINDOW_MS, 1300.0);
        assert_eq!(decode_miss(&f), Some(band::PROBE_SLOTS_COVERED));
    }

    /// Distinguishes `== probes_issued` from `> 0`: 11 of 12 answered is
    /// above zero and still a miss.
    #[test]
    fn one_unanswered_probe_misses_probes_answered() {
        let f = with(&decode_inside(), fig::PROBES_ANSWERED_200, 11.0);
        assert_eq!(decode_miss(&f), Some(band::PROBES_ANSWERED));
    }

    #[test]
    fn latency_of_exactly_250_ms_misses_probe_latency_max() {
        let f = with_series(&decode_inside(), fig::PROBE_LATENCY_MS, vec![250.0]);
        assert_eq!(decode_miss(&f), Some(band::PROBE_LATENCY_MAX));
    }

    /// Distinguishes max from mean: the mean of these is 70 ms, inside the
    /// band, and the one 250 ms probe is a miss.
    #[test]
    fn one_slow_probe_among_fast_ones_misses_probe_latency_max() {
        let f = with_series(
            &decode_inside(),
            fig::PROBE_LATENCY_MS,
            vec![10.0, 10.0, 10.0, 250.0],
        );
        assert_eq!(decode_miss(&f), Some(band::PROBE_LATENCY_MAX));
    }

    #[test]
    fn non_zero_inline_count_misses_inline_jobs() {
        let f = with(&decode_inside(), fig::INLINE_JOBS, 1.0);
        assert_eq!(decode_miss(&f), Some(band::INLINE_JOBS));
    }

    #[test]
    fn missing_decode_job_misses_decode_jobs() {
        let f = with(&decode_inside(), fig::DECODE_JOBS, 15.0);
        assert_eq!(decode_miss(&f), Some(band::DECODE_JOBS));
    }

    #[test]
    fn undersized_unit_misses_decode_unit_size() {
        let f = with(&decode_inside(), fig::DECODE_UNIT_BYTES, 255.0);
        assert_eq!(decode_miss(&f), Some(band::DECODE_UNIT_SIZE));
    }

    #[test]
    fn heartbeat_age_of_exactly_2_s_misses_the_expected_band_only() {
        let f = with_series(&heartbeat_inside(), fig::HEARTBEAT_AGE_S, vec![0.2, 2.0]);
        let outcomes = evaluate_heartbeat_under_load(&f);
        assert_eq!(heartbeat_miss(&f), Some(band::HEARTBEAT_EXPECTED));
        let missed: Vec<_> = outcomes.iter().filter(|o| !o.inside).collect();
        assert_eq!(missed.len(), 1);
        assert!(!missed[0].hard);
    }

    #[test]
    fn heartbeat_age_of_exactly_10_s_is_a_hard_miss_of_the_adr_bound() {
        let f = with_series(&heartbeat_inside(), fig::HEARTBEAT_AGE_S, vec![10.0]);
        let outcomes = evaluate_heartbeat_under_load(&f);
        let miss = first_miss(&outcomes).expect("a miss");
        assert_eq!(miss.band, band::HEARTBEAT_ADR_BOUND);
        assert!(miss.hard);
    }

    #[test]
    fn heartbeat_age_just_under_10_s_is_inside_the_adr_bound() {
        let f = with_series(&heartbeat_inside(), fig::HEARTBEAT_AGE_S, vec![9.999]);
        let outcomes = evaluate_heartbeat_under_load(&f);
        let adr = outcomes
            .iter()
            .find(|o| o.band == band::HEARTBEAT_ADR_BOUND)
            .expect("adr band");
        assert!(adr.inside);
        assert_eq!(heartbeat_miss(&f), Some(band::HEARTBEAT_EXPECTED));
    }

    #[test]
    fn single_readyz_503_misses_readyz_all_200() {
        let f = with(&heartbeat_inside(), fig::READYZ_ANSWERED_200, 599.0);
        assert_eq!(heartbeat_miss(&f), Some(band::READYZ_ALL_200));
    }

    #[test]
    fn short_load_window_misses_load_window() {
        let f = with(&heartbeat_inside(), fig::WINDOW_MS, 59_999.0);
        assert_eq!(heartbeat_miss(&f), Some(band::LOAD_WINDOW));
    }

    #[test]
    fn absent_figure_fails_its_band_by_name() {
        for name in [
            fig::WINDOW_MS,
            fig::PROBES_ISSUED,
            fig::PROBE_SLOTS_COVERED,
            fig::PROBES_ANSWERED_200,
            fig::PROBE_LATENCY_MS,
            fig::INLINE_JOBS,
            fig::DECODES_ISSUED,
            fig::DECODE_JOBS,
            fig::DECODE_UNIT_BYTES,
            fig::DECODE_UNIT_TARGET_BYTES,
            fig::QUERIES_FAILED,
        ] {
            let outcomes = evaluate_decode_liveness(&without(&decode_inside(), name));
            let miss = first_miss(&outcomes).expect("absent figure is a miss");
            assert_eq!(miss.value, Err(FigureError::Absent(name)), "{name}");
        }
        for name in [
            fig::WINDOW_MS,
            fig::PROMQL_QUERIES_OK,
            fig::SQL_QUERIES_OK,
            fig::QUERIES_FAILED,
            fig::HEARTBEAT_SCRAPES_FAILED,
            fig::HEARTBEAT_AGE_S,
            fig::HEARTBEAT_UNANSWERED_LATENCY_S,
            fig::READYZ_PROBES_ISSUED,
            fig::READYZ_ANSWERED_200,
        ] {
            let outcomes = evaluate_heartbeat_under_load(&without(&heartbeat_inside(), name));
            let miss = first_miss(&outcomes).expect("absent figure is a miss");
            assert_eq!(miss.value, Err(FigureError::Absent(name)), "{name}");
        }
    }

    #[test]
    fn empty_sample_series_is_absent() {
        let f = with_series(&heartbeat_inside(), fig::HEARTBEAT_AGE_S, Vec::new());
        let outcomes = evaluate_heartbeat_under_load(&f);
        let miss = first_miss(&outcomes).expect("a miss");
        assert_eq!(miss.value, Err(FigureError::Absent(fig::HEARTBEAT_AGE_S)));
    }

    #[test]
    fn duplicated_figure_fails_its_band_by_name() {
        let mut f = decode_inside();
        f.record(fig::INLINE_JOBS, 0.0);
        let outcomes = evaluate_decode_liveness(&f);
        let miss = first_miss(&outcomes).expect("a miss");
        assert_eq!(miss.band, band::INLINE_JOBS);
        assert_eq!(miss.value, Err(FigureError::Duplicated(fig::INLINE_JOBS)));
    }

    #[test]
    fn band_lines_name_the_band_value_and_verdict() {
        let f = with_series(&heartbeat_inside(), fig::HEARTBEAT_AGE_S, vec![10.0]);
        let outcomes = evaluate_heartbeat_under_load(&f);
        let adr = named(&outcomes, band::HEARTBEAT_ADR_BOUND);
        assert_eq!(
            adr.line(),
            "band heartbeat_age_adr_bound: 10 (band: max < 10 s) HARD MISS"
        );
    }

    const EXPOSITION: &str = "\
# HELP ravel_cpu_gate_inline_total Jobs.
# TYPE ravel_cpu_gate_inline_total counter
ravel_cpu_gate_inline_total{mode=\"query\",gate=\"read\",site=\"catalog_part\"} 2
ravel_cpu_gate_inline_total{mode=\"query\",gate=\"read\",site=\"promql_eval\"} 3
ravel_cpu_gate_inline_total{mode=\"query\",gate=\"write\",site=\"metrics_flush\"} 5
ravel_cpu_gate_inline_totals{mode=\"query\"} 100
ravel_health_heartbeat_age_seconds{mode=\"query\"} 0.25
";

    #[test]
    fn missing_families_matches_exact_names_only() {
        let missing = missing_families(EXPOSITION, &REQUIRED_FAMILIES);
        assert_eq!(missing, vec!["ravel_cpu_gate_jobs_total"]);
        // A HELP/TYPE header with no sample is not a present family.
        let header_only = "# TYPE ravel_cpu_gate_jobs_total counter\n";
        assert_eq!(
            missing_families(header_only, &["ravel_cpu_gate_jobs_total"]),
            vec!["ravel_cpu_gate_jobs_total"]
        );
    }

    #[test]
    fn family_sum_filters_by_name_and_labels() {
        assert_eq!(
            family_sum(EXPOSITION, "ravel_cpu_gate_inline_total", &[]),
            10.0
        );
        assert_eq!(
            family_sum(
                EXPOSITION,
                "ravel_cpu_gate_inline_total",
                &["gate=\"read\"", "site=\"promql_eval\""]
            ),
            3.0
        );
        assert_eq!(
            single_value(EXPOSITION, "ravel_health_heartbeat_age_seconds"),
            Some(0.25)
        );
        assert_eq!(
            single_value(EXPOSITION, "ravel_cpu_gate_inline_total"),
            None
        );
    }

    #[test]
    fn proc_parsers_read_the_fields() {
        let meminfo = "MemTotal:       32132612 kB\nMemFree: 1 kB\nMemAvailable:   20000000 kB\n";
        assert_eq!(meminfo_kib(meminfo, "MemTotal"), Some(32_132_612));
        assert_eq!(meminfo_kib(meminfo, "MemAvailable"), Some(20_000_000));
        assert_eq!(meminfo_kib(meminfo, "SwapTotal"), None);
        assert_eq!(
            parse_loadavg("9.88 6.16 5.32 4/832 499740\n"),
            Some((9.88, 6.16, 5.32))
        );
        assert_eq!(parse_loadavg(""), None);
    }

    #[test]
    fn scale_divisor_halves_until_the_decodes_fit() {
        let gib = 1u64 << 30;
        // 16 x 256 MiB x 4 = 16 GiB, fits in half of 32 GiB.
        assert_eq!(decode_scale_divisor(16, 4, 32 * gib), 1);
        // Half of 31 GiB does not hold 16 GiB; 8 GiB does.
        assert_eq!(decode_scale_divisor(16, 4, 31 * gib), 2);
        assert_eq!(decode_scale_divisor(16, 4, 8 * gib), 4);
    }

    /// Probe `slot`, issued `late_us` microseconds after its due time.
    fn probe(slot: u64, late_us: u64, status: Option<u16>) -> Probe {
        Probe {
            slot,
            issued_at: Duration::from_millis(slot * PROBE_INTERVAL_MS)
                + Duration::from_micros(late_us),
            latency: Duration::from_millis(2),
            status,
        }
    }

    /// Scenario 1 figures with the window and probe figures taken from
    /// `probes` the way the bin takes them.
    fn decode_with_probes(window: Duration, probes: &[Probe]) -> FigureSet {
        let mut f = decode_inside();
        for name in [
            fig::WINDOW_MS,
            fig::PROBES_ISSUED,
            fig::PROBE_SLOTS_COVERED,
            fig::PROBES_ANSWERED_200,
            fig::PROBE_LATENCY_MS,
            fig::PROBE_WAKE_LATENESS_MS,
        ] {
            f = without(&f, name);
        }
        record_decode_probes(&mut f, window, probes);
        f
    }

    fn named<'a>(outcomes: &'a [BandOutcome], band: &str) -> &'a BandOutcome {
        outcomes
            .iter()
            .find(|o| o.band == band)
            .expect("band present")
    }

    /// A 1200.7 ms window has 12 slots due. Probe 12 woke 0.6 ms late, inside
    /// the window and inside one period of its due time, so it covers slot 12.
    #[test]
    fn probe_woken_late_inside_the_window_covers_its_slot() {
        let window = Duration::from_micros(1_200_700);
        let probes: Vec<Probe> = (1..=12)
            .map(|k| probe(k, if k == 12 { 600 } else { 50 }, Some(200)))
            .collect();
        assert_eq!(slots_due(window), 12);
        let f = decode_with_probes(window, &probes);
        let outcomes = evaluate_decode_liveness(&f);
        let covered = named(&outcomes, band::PROBE_SLOTS_COVERED);
        assert_eq!(covered.value, Ok(12.0));
        assert!(covered.inside, "{}", covered.line());
        assert_eq!(first_miss(&outcomes), None);
    }

    /// The same probes against a 1200.4 ms window: probe 12 was issued
    /// 0.2 ms after the window's end, so it covers nothing.
    #[test]
    fn probe_issued_after_the_window_covers_nothing() {
        let window = Duration::from_micros(1_200_400);
        let probes: Vec<Probe> = (1..=12)
            .map(|k| probe(k, if k == 12 { 600 } else { 50 }, Some(200)))
            .collect();
        assert_eq!(
            slot_coverage(&probes, window),
            SlotCoverage {
                due: 12,
                covered: 11
            }
        );
        let f = decode_with_probes(window, &probes);
        let miss = first_miss(&evaluate_decode_liveness(&f))
            .expect("a miss")
            .clone();
        assert_eq!(miss.band, band::PROBE_SLOTS_COVERED);
        assert_eq!(miss.value, Ok(11.0));
        // Its latency is still recorded.
        assert_eq!(
            f.series_values(fig::PROBE_LATENCY_MS).map(<[f64]>::len),
            Ok(12)
        );
    }

    /// A probe for slot 5 answered in 150 ms, inside the latency band, was in
    /// flight when slot 6 fell due, so slot 6 is covered although the prober
    /// skipped it.
    #[test]
    fn probe_of_150_ms_covers_the_slot_it_was_in_flight_for() {
        let window = Duration::from_micros(1_200_400);
        let slow = timed(5, Duration::from_micros(50), Duration::from_millis(150));
        assert_eq!(next_probe_slot(&slow, PERIOD), 7);
        let probes: Vec<Probe> = (1..=12)
            .filter(|k| *k != 6)
            .map(|k| {
                if k == 5 {
                    slow
                } else {
                    probe(k, 50, Some(200))
                }
            })
            .collect();
        assert_eq!(
            slot_coverage(&probes, window),
            SlotCoverage {
                due: 12,
                covered: 12
            }
        );
        let f = decode_with_probes(window, &probes);
        let outcomes = evaluate_decode_liveness(&f);
        let covered = named(&outcomes, band::PROBE_SLOTS_COVERED);
        assert_eq!(covered.value, Ok(12.0));
        assert!(covered.inside, "{}", covered.line());
        let latency = named(&outcomes, band::PROBE_LATENCY_MAX);
        assert_eq!(latency.value, Ok(150.0));
        assert!(latency.inside, "{}", latency.line());
        assert_eq!(first_miss(&outcomes), None);
    }

    /// A 300 ms probe for slot 5 is outside the latency band: it is named
    /// first, and the slots it was in flight for, 6 and 7, stay uncovered.
    #[test]
    fn probe_of_300_ms_misses_probe_latency_max_first() {
        let window = Duration::from_micros(1_200_400);
        let slow = timed(5, Duration::from_micros(50), Duration::from_millis(300));
        assert_eq!(next_probe_slot(&slow, PERIOD), 8);
        let probes: Vec<Probe> = (1..=12)
            .filter(|k| *k != 6 && *k != 7)
            .map(|k| {
                if k == 5 {
                    slow
                } else {
                    probe(k, 50, Some(200))
                }
            })
            .collect();
        assert_eq!(
            slot_coverage(&probes, window),
            SlotCoverage {
                due: 12,
                covered: 10
            }
        );
        let f = decode_with_probes(window, &probes);
        let outcomes = evaluate_decode_liveness(&f);
        let miss = first_miss(&outcomes).expect("a miss");
        assert_eq!(miss.band, band::PROBE_LATENCY_MAX);
        assert_eq!(miss.value, Ok(300.0));
        let covered = named(&outcomes, band::PROBE_SLOTS_COVERED);
        assert_eq!(covered.value, Ok(10.0));
        assert!(!covered.inside);
    }

    /// A 25 s window: slots 1..=50 probed on time, then the prober is not
    /// scheduled until 24.8 s and sends slots 51..=250 back to back, 2 ms
    /// each, slot 152 onwards after the window's end. Every one of those 200
    /// slots is uncovered, the 101 sent inside the window included.
    fn starved_trace() -> (Duration, Vec<Probe>) {
        let window = Duration::from_secs(25);
        let mut probes: Vec<Probe> = (1..=50).map(|k| probe(k, 50, Some(200))).collect();
        let burst = Duration::from_millis(24_800);
        for k in 51..=250u64 {
            let issued_at = burst + Duration::from_millis(2) * u32::try_from(k - 51).unwrap();
            probes.push(Probe {
                slot: k,
                issued_at,
                latency: Duration::from_millis(2),
                status: Some(200),
            });
        }
        (window, probes)
    }

    #[test]
    fn starved_prober_catching_up_misses_probe_slots_covered() {
        let (window, probes) = starved_trace();
        assert_eq!(probes.len(), 250);
        assert_eq!(
            probes.iter().filter(|p| p.issued_at > window).count(),
            99,
            "slots 152..=250 sent after the window"
        );
        let coverage = slot_coverage(&probes, window);
        assert_eq!(
            coverage,
            SlotCoverage {
                due: 250,
                covered: 50
            }
        );
        assert_eq!(coverage.uncovered(), 200);
        let f = decode_with_probes(window, &probes);
        let outcomes = evaluate_decode_liveness(&f);
        let miss = first_miss(&outcomes).expect("a miss");
        assert_eq!(miss.band, band::PROBE_SLOTS_COVERED);
        assert_eq!(miss.value, Ok(50.0));
        assert_eq!(
            miss.line(),
            "band probe_slots_covered: 50 (band: == floor(window_ms / 100) = 250) MISS"
        );
    }

    #[test]
    fn every_slot_probed_on_time_passes() {
        let window = Duration::from_micros(25_000_100);
        let probes: Vec<Probe> = (1..=250).map(|k| probe(k, 50, Some(200))).collect();
        assert_eq!(
            slot_coverage(&probes, window),
            SlotCoverage {
                due: 250,
                covered: 250
            }
        );
        assert_eq!(decode_miss(&decode_with_probes(window, &probes)), None);
    }

    /// A probe issued exactly one period after its due time still covers its
    /// slot, and the next slot, whose due time it was in flight across; one
    /// nanosecond later it covers neither.
    #[test]
    fn probe_issued_one_period_late_is_the_last_that_covers() {
        let window = Duration::from_secs(2);
        let at = |late| slot_coverage(&[timed(5, late, Duration::from_millis(2))], window).covered;
        assert_eq!(at(PERIOD), 2);
        assert_eq!(at(PERIOD + Duration::from_nanos(1)), 0);
    }

    /// A probe that did not answer 200 covers neither its slot nor the slots
    /// it was in flight for.
    #[test]
    fn unanswered_probe_covers_nothing() {
        let window = Duration::from_secs(2);
        let mut slow = timed(5, Duration::ZERO, Duration::from_millis(150));
        assert_eq!(slot_coverage(&[slow], window).covered, 2);
        slow.status = Some(503);
        assert_eq!(slot_coverage(&[slow], window).covered, 0);
    }

    /// The prober loop's decisions: woken at 20 s on slot 50 of a 25 s window
    /// with a 2 ms probe, it moves to slot 51, and that probe, sent about
    /// 15 s after slot 51's due time, covers nothing. A next slot past the
    /// window's last slot stops the loop.
    #[test]
    fn prober_step_advances_one_slot_after_a_late_wake_and_stops_past_the_window() {
        assert_eq!(prober_step(None, PERIOD, u64::MAX), ProberStep::Send(1));
        let woke = Probe {
            slot: 50,
            issued_at: Duration::from_secs(20),
            latency: Duration::from_millis(2),
            status: Some(200),
        };
        assert_eq!(wake_lateness(&woke), Duration::from_secs(15));
        let ProberStep::Send(next) = prober_step(Some(&woke), PERIOD, u64::MAX) else {
            panic!("window still open");
        };
        assert_eq!(next, 51);
        let sent = Probe {
            slot: next,
            issued_at: woke.issued_at + woke.latency,
            latency: Duration::from_millis(2),
            status: Some(200),
        };
        assert_eq!(wake_lateness(&sent), Duration::from_millis(14_902));
        let window = Duration::from_secs(25);
        assert_eq!(slot_coverage(&[woke, sent], window).covered, 0);

        let last = slots_due(window);
        assert_eq!(last, 250);
        let at = |slot| timed(slot, Duration::ZERO, Duration::from_millis(2));
        assert_eq!(
            prober_step(Some(&at(249)), PERIOD, last),
            ProberStep::Send(250)
        );
        assert_eq!(prober_step(Some(&at(250)), PERIOD, last), ProberStep::Stop);
        // A probe whose duration crossed the window's last due time stops it too.
        let slow = timed(249, Duration::ZERO, Duration::from_millis(150));
        assert_eq!(prober_step(Some(&slow), PERIOD, last), ProberStep::Stop);
    }

    #[test]
    fn probe_missing_from_slot_7_misses_probe_slots_covered() {
        let window = Duration::from_micros(1_200_400);
        let probes: Vec<Probe> = (1..=12)
            .filter(|k| *k != 7)
            .map(|k| probe(k, 50, Some(200)))
            .collect();
        let f = decode_with_probes(window, &probes);
        let outcomes = evaluate_decode_liveness(&f);
        let miss = first_miss(&outcomes).expect("a miss");
        assert_eq!(miss.band, band::PROBE_SLOTS_COVERED);
        assert_eq!(miss.value, Ok(11.0));
    }

    /// A probe for slot 13, due after the 1200.4 ms window, is not counted
    /// even if it was issued.
    #[test]
    fn probe_due_after_the_window_is_not_counted() {
        let window = Duration::from_micros(1_200_400);
        let probes: Vec<Probe> = (1..=13).map(|k| probe(k, 50, Some(200))).collect();
        assert_eq!(probes_in_window(&probes, window).len(), 12);
        assert_eq!(decode_miss(&decode_with_probes(window, &probes)), None);
    }

    /// A scrape that timed out after 15 s returned no age. The ages that did
    /// come back are all small, and the ADR bound still reads as a hard miss.
    #[test]
    fn scrape_timed_out_above_10_s_is_a_hard_miss_of_the_adr_bound() {
        let mut f = without(&heartbeat_inside(), fig::HEARTBEAT_AGE_S);
        f = without(&f, fig::HEARTBEAT_UNANSWERED_LATENCY_S);
        f = without(&f, fig::HEARTBEAT_SCRAPES_FAILED);
        let scrapes = [
            Scrape {
                latency: Duration::from_millis(3),
                age_s: Some(0.1),
            },
            Scrape {
                latency: Duration::from_secs(15),
                age_s: None,
            },
            Scrape {
                latency: Duration::from_millis(4),
                age_s: Some(0.2),
            },
        ];
        record_heartbeat_scrapes(&mut f, &scrapes);
        let outcomes = evaluate_heartbeat_under_load(&f);
        let named = |band| {
            outcomes
                .iter()
                .find(|o| o.band == band)
                .expect("band present")
        };
        assert!(!named(band::HEARTBEAT_SCRAPES).inside);
        assert_eq!(heartbeat_miss(&f), Some(band::HEARTBEAT_ADR_BOUND));
        let adr = named(band::HEARTBEAT_ADR_BOUND);
        assert!(!adr.inside, "{}", adr.line());
        assert!(adr.hard);
        assert_eq!(adr.value, Ok(15.0));
        assert!(!named(band::HEARTBEAT_EXPECTED).inside);
    }

    #[test]
    fn scrape_summary_names_the_latency_beside_the_largest_age() {
        let scrapes = [
            Scrape {
                latency: Duration::from_millis(12_500),
                age_s: Some(12.0),
            },
            Scrape {
                latency: Duration::from_millis(3),
                age_s: Some(0.5),
            },
        ];
        assert_eq!(
            scrape_summary(&scrapes),
            "heartbeat scrapes: slowest 12.500 s, largest age 12.000 s returned by a scrape of 12.500 s"
        );
    }

    #[test]
    fn after_window_scrape_missing_inline_family_fails_by_name() {
        let after: String = EXPOSITION
            .lines()
            .filter(|l| !l.starts_with("ravel_cpu_gate_inline_total{"))
            .map(|l| format!("{l}\n"))
            .collect();
        let err = family_delta(EXPOSITION, &after, "ravel_cpu_gate_inline_total", &[])
            .expect_err("an absent family is a failure, not 0");
        assert_eq!(
            err,
            "metric family ravel_cpu_gate_inline_total missing from /metrics after the window"
        );
        assert_eq!(
            family_delta(EXPOSITION, EXPOSITION, "ravel_cpu_gate_inline_total", &[]),
            Ok(0.0)
        );
    }

    /// A 450 ms decode window has 4 slots due and 4 probes answered, so every
    /// probe band holds; the window band is the miss.
    #[test]
    fn decode_window_under_1000_ms_misses_decode_window() {
        let window = Duration::from_millis(450);
        let probes: Vec<Probe> = (1..=4).map(|k| probe(k, 50, Some(200))).collect();
        let f = decode_with_probes(window, &probes);
        let outcomes = evaluate_decode_liveness(&f);
        let miss = first_miss(&outcomes).expect("a miss");
        assert_eq!(miss.band, band::DECODE_WINDOW);
        assert_eq!(miss.value, Ok(450.0));
        assert_eq!(
            miss.line(),
            "band decode_window: 450 (band: >= 1000 ms (10 probe slots)) MISS"
        );
        let missed: Vec<_> = outcomes.iter().filter(|o| !o.inside).collect();
        assert_eq!(missed.len(), 1, "{missed:?}");
    }

    /// The window that leaves no slot to probe at all, the case the band
    /// exists for: 0 probes issued matches its expected 0.
    #[test]
    fn decode_window_with_no_probe_slot_misses_decode_window() {
        let f = decode_with_probes(Duration::from_millis(90), &[]);
        assert_eq!(decode_miss(&f), Some(band::DECODE_WINDOW));
    }

    /// The window ends 0.1 ms after slot 10's due time, after slot 10's probe
    /// was sent at 0.05 ms late.
    #[test]
    fn decode_window_of_1000_ms_with_10_probes_is_inside() {
        let window = Duration::from_micros(1_000_100);
        let probes: Vec<Probe> = (1..=10).map(|k| probe(k, 50, Some(200))).collect();
        let f = decode_with_probes(window, &probes);
        assert_eq!(decode_miss(&f), None);
    }

    const PERIOD: Duration = Duration::from_millis(PROBE_INTERVAL_MS);

    /// Slot 5 was due at 500 ms; the prober woke at 610 ms and the probe
    /// answered in 2 ms. The listener did not overrun slot 6, so slot 6 is
    /// next, not 7.
    /// Probe `slot`, issued `late` after its due time, answered `latency`
    /// after it was issued.
    fn timed(slot: u64, late: Duration, latency: Duration) -> Probe {
        Probe {
            slot,
            issued_at: PERIOD * u32::try_from(slot).unwrap() + late,
            latency,
            status: Some(200),
        }
    }

    #[test]
    fn late_wake_with_a_fast_probe_advances_one_slot() {
        let late = timed(5, Duration::from_millis(110), Duration::from_millis(2));
        assert_eq!(late.issued_at, Duration::from_millis(610));
        assert_eq!(next_probe_slot(&late, PERIOD), 6);
        assert_eq!(wake_lateness(&late), Duration::from_millis(110));
    }

    /// A probe for slot 5 whose own duration is 230 ms ran past the due times
    /// of slots 6 (600 ms) and 7 (700 ms), so both are skipped, whether it
    /// was sent on time or late.
    #[test]
    fn probe_whose_duration_crosses_two_slots_skips_them() {
        let slow = Duration::from_millis(230);
        assert_eq!(next_probe_slot(&timed(5, Duration::ZERO, slow), PERIOD), 8);
        assert_eq!(
            next_probe_slot(&timed(5, Duration::from_millis(110), slow), PERIOD),
            8
        );
    }

    #[test]
    fn probe_answered_exactly_at_the_next_due_time_skips_nothing() {
        let at = |latency| next_probe_slot(&timed(5, Duration::ZERO, latency), PERIOD);
        assert_eq!(at(PERIOD), 6);
        assert_eq!(at(PERIOD + Duration::from_nanos(1)), 7);
        assert_eq!(at(Duration::ZERO), 6);
    }

    #[test]
    fn wake_lateness_is_recorded_and_printed_without_a_band() {
        let window = Duration::from_micros(1_000_100);
        let probes: Vec<Probe> = (1..=10)
            .map(|k| probe(k, if k == 3 { 90_000 } else { 50 }, Some(200)))
            .collect();
        let f = decode_with_probes(window, &probes);
        assert_eq!(f.series_max(fig::PROBE_WAKE_LATENESS_MS), Ok(90.0));
        assert_eq!(decode_miss(&f), None);
        assert_eq!(
            wake_lateness_line(&f),
            "prober wake lateness: max 90.000 ms (no band)"
        );
        // The line reads the recorded figure, not the probes.
        let f = with_series(&f, fig::PROBE_WAKE_LATENESS_MS, vec![1.5, 7.25]);
        assert_eq!(
            wake_lateness_line(&f),
            "prober wake lateness: max 7.250 ms (no band)"
        );
    }

    #[test]
    fn start_of_run_check_names_a_heartbeat_family_with_two_samples() {
        let body = format!(
            "{EXPOSITION}ravel_cpu_gate_jobs_total{{gate=\"read\"}} 1\n\
             ravel_health_heartbeat_age_seconds{{mode=\"ingest\"}} 0.5\n"
        );
        assert_eq!(
            check_start_families(&body),
            Err(
                "metric family ravel_health_heartbeat_age_seconds carries 2 samples in /metrics, expected exactly 1"
                    .to_string()
            )
        );
        let one = format!("{EXPOSITION}ravel_cpu_gate_jobs_total{{gate=\"read\"}} 1\n");
        assert_eq!(check_start_families(&one), Ok(()));
        assert_eq!(
            check_start_families(EXPOSITION),
            Err("metric families missing from /metrics: ravel_cpu_gate_jobs_total".to_string())
        );
    }

    #[test]
    fn scale_note_names_the_divisor_and_the_published_size() {
        assert_eq!(scale_note(1), None);
        assert_eq!(
            scale_note(2).as_deref(),
            Some("scaled: decode unit 128 MiB, divisor 2; the published scenario is 256 MiB")
        );
    }
}
