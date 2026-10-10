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

/// Scenario 1: the health listener is probed on this period.
pub const PROBE_INTERVAL_MS: u64 = 100;
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

/// The metric families both bins depend on, checked by exact name before any
/// load is driven.
pub const REQUIRED_FAMILIES: [&str; 3] = [
    "ravel_health_heartbeat_age_seconds",
    "ravel_cpu_gate_jobs_total",
    "ravel_cpu_gate_inline_total",
];

/// Figure names. One constant per figure, so a band and the bin that records
/// it cannot drift apart by a typo.
pub mod fig {
    pub const WINDOW_MS: &str = "window_ms";
    pub const PROBES_ISSUED: &str = "probes_issued";
    pub const PROBES_ANSWERED_200: &str = "probes_answered_200";
    pub const PROBE_LATENCY_MS: &str = "probe_latency_ms";
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
    pub const READYZ_PROBES_ISSUED: &str = "readyz_probes_issued";
    pub const READYZ_ANSWERED_200: &str = "readyz_answered_200";
}

/// Band names, as the bins print them and as a miss is reported.
pub mod band {
    pub const DECODE_WINDOW: &str = "decode_window";
    pub const PROBES_ISSUED: &str = "probes_issued";
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

    /// The largest sample of a series. Max, not mean: one slow probe among
    /// many fast ones is exactly what the latency band exists to catch.
    pub fn series_max(&self, name: &'static str) -> Result<f64, FigureError> {
        let mut found = self.series.iter().filter(|(n, _)| *n == name);
        match (found.next(), found.next()) {
            (None, _) => Err(FigureError::Absent(name)),
            (Some(_), Some(_)) => Err(FigureError::Duplicated(name)),
            (Some((_, values)), None) => values
                .iter()
                .copied()
                .reduce(f64::max)
                .ok_or(FigureError::Absent(name)),
        }
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
/// - `decode_window`: the measured window is positive.
/// - `probes_issued`: exactly `floor(window_ms / 100)` probes were issued in
///   the window. A probe that overruns its 100 ms slot delays every later
///   one, so a slow listener shows here as a short count.
/// - `probes_answered`: every issued probe answered 200.
/// - `probe_latency_max`: the slowest probe took under 250 ms.
/// - `inline_jobs`: `ravel_cpu_gate_inline_total` moved by exactly 0 over the
///   window, summed over every gate and site.
/// - `decode_jobs`: the read gate ran exactly one catalog decode job per
///   query issued, so the load the bands are read under actually happened.
/// - `decode_unit_size`: each decode unit is at least the target size.
/// - `queries_failed`: no decode query failed.
pub fn evaluate_decode_liveness(figs: &FigureSet) -> Vec<BandOutcome> {
    let window = figs.scalar(fig::WINDOW_MS);
    let issued = figs.scalar(fig::PROBES_ISSUED);
    let expected_probes = window.map(|w| (w / PROBE_INTERVAL_MS as f64).floor());
    let probes_issued = match both(issued, expected_probes) {
        Ok((issued, expected)) => BandOutcome::new(
            band::PROBES_ISSUED,
            format!(
                "== floor(window_ms / {PROBE_INTERVAL_MS}) = {}",
                fmt_num(expected)
            ),
            Ok(issued),
            false,
            |v| v == expected,
        ),
        Err(e) => BandOutcome::new(
            band::PROBES_ISSUED,
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
        BandOutcome::new(band::DECODE_WINDOW, "> 0".to_string(), window, false, |v| {
            v > 0.0
        }),
        probes_issued,
        probes_answered,
        BandOutcome::new(
            band::PROBE_LATENCY_MAX,
            format!("max < {} ms", fmt_num(MAX_PROBE_LATENCY_MS)),
            figs.series_max(fig::PROBE_LATENCY_MS),
            false,
            |v| v < MAX_PROBE_LATENCY_MS,
        ),
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
/// - `heartbeat_scrapes`: no `/metrics` scrape failed, so no heartbeat
///   sample was lost.
/// - `heartbeat_age_adr_bound`: the largest scraped
///   `ravel_health_heartbeat_age_seconds` is under 10 s. At or above is a
///   hard miss of ADR-1702 decision 9's bound.
/// - `heartbeat_age_expected`: the same maximum is under 2 s. Between 2 s and
///   10 s it is outside the expected band and inside the ADR's.
/// - `readyz_probes`: at least one `/readyz` probe was issued.
/// - `readyz_all_200`: every `/readyz` probe on the health listener answered
///   200.
pub fn evaluate_heartbeat_under_load(figs: &FigureSet) -> Vec<BandOutcome> {
    let heartbeat = figs.series_max(fig::HEARTBEAT_AGE_S);
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
            band::HEARTBEAT_SCRAPES,
            "failed == 0".to_string(),
            figs.scalar(fig::HEARTBEAT_SCRAPES_FAILED),
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
    fn probe_count_one_short_misses_probes_issued() {
        let f = with(&decode_inside(), fig::PROBES_ISSUED, 11.0);
        // Answered stays at 12, so check the issued band is the one named.
        assert_eq!(decode_miss(&f), Some(band::PROBES_ISSUED));
    }

    #[test]
    fn one_extra_probe_misses_probes_issued() {
        let f = with(&decode_inside(), fig::PROBES_ISSUED, 13.0);
        assert_eq!(decode_miss(&f), Some(band::PROBES_ISSUED));
    }

    #[test]
    fn probe_count_follows_the_floor_of_the_window() {
        // 1299 ms floors to 12 probes; 1300 ms needs 13.
        let f = with(&decode_inside(), fig::WINDOW_MS, 1299.0);
        assert_eq!(decode_miss(&f), None);
        let f = with(&decode_inside(), fig::WINDOW_MS, 1300.0);
        assert_eq!(decode_miss(&f), Some(band::PROBES_ISSUED));
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
        let adr = &outcomes[5];
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
}
