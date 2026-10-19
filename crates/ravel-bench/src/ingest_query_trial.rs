//! Issue #2818 trial: query latency under a fixed Remote Write load, and the
//! flush input sizes the write gate's inline floor is compared against.
//!
//! The `ingest_query_latency_trial` bin (feature `saturation`) runs the arms
//! in [`ARMS`] at each client count in [`CLIENTS`] against one in-process
//! server each, and records every figure [`expected_figures`] names into a
//! [`TrialFigures`]. A figure absent or recorded more than once fails the run:
//! the report prints each expected figure exactly once, read from the set, so
//! a load that silently measured nothing cannot pass for a result.
//!
//! The `ingest_write_gate_overhead` bench reads [`min_median_max`] for its
//! flush input byte column.

use std::fmt;
use std::time::Duration;

use prost::Message;
use ravel_remote_write::proto::prometheus::{
    Label as PbLabel, Sample as PbSample, TimeSeries as PbTimeSeries, WriteRequest,
};

/// One server configuration the trial measures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrialArm {
    pub name: &'static str,
    /// `--cpu-gate-write-inline-floor-bytes`. `u64::MAX` runs every write job
    /// inline, which is the ungated arm: no write job reaches the gate.
    pub write_inline_floor_bytes: u64,
}

/// The arms in report order: ungated, the write gate at the 256 KiB floor it
/// shipped with before issue #2818, and at the 1 MiB floor that is now its
/// default.
pub const ARMS: [TrialArm; 3] = [
    TrialArm {
        name: "ungated",
        write_inline_floor_bytes: u64::MAX,
    },
    TrialArm {
        name: "gated_floor_256kib",
        write_inline_floor_bytes: CODEC_INLINE_FLOOR_BYTES,
    },
    TrialArm {
        name: "gated_floor_1mib",
        write_inline_floor_bytes: WRITE_INLINE_FLOOR_BYTES,
    },
];

/// The 256 KiB codec floor the read gate keeps and the write gate had before
/// issue #2818, restated because the standard build of this crate does not
/// link ravel-cpu-gate; a test pins it to
/// `ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES`.
pub const CODEC_INLINE_FLOOR_BYTES: u64 = 256 * 1024;

/// The write gate's shipped 1 MiB inline floor, restated for the same reason;
/// a test pins it to `ravel_cpu_gate::DEFAULT_WRITE_INLINE_FLOOR_BYTES`.
pub const WRITE_INLINE_FLOOR_BYTES: u64 = 1024 * 1024;

/// Concurrent Remote Write clients per arm.
pub const CLIENTS: [usize; 2] = [1, 16];

/// The compressed size of every Remote Write body.
pub const BODY_TARGET_BYTES: usize = 64 * 1024;

/// The figures one arm at one client count reports.
pub const METRICS: [&str; 11] = [
    "promql_p50_ms",
    "promql_p99_ms",
    "sql_p50_ms",
    "sql_p99_ms",
    "ingest_req_per_s",
    "ingest_mib_per_s",
    "promql_queries",
    "sql_queries",
    "ingest_requests",
    "queries_failed",
    "ingest_failed",
];

/// The name of one figure: `<arm>/c<clients>/<metric>`.
pub fn figure_name(arm: &str, clients: usize, metric: &str) -> String {
    format!("{arm}/c{clients}/{metric}")
}

/// Every figure a run over `arms` must record, in report order.
pub fn expected_figures(arms: &[TrialArm]) -> Vec<String> {
    let mut out = Vec::with_capacity(arms.len() * CLIENTS.len() * METRICS.len());
    for arm in arms {
        for clients in CLIENTS {
            for metric in METRICS {
                out.push(figure_name(arm.name, clients, metric));
            }
        }
    }
    out
}

/// Why an expected figure cannot be reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrialFigureError {
    Absent(String),
    Duplicated(String),
}

impl fmt::Display for TrialFigureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TrialFigureError::Absent(name) => write!(f, "figure {name} absent"),
            TrialFigureError::Duplicated(name) => {
                write!(f, "figure {name} recorded more than once")
            }
        }
    }
}

impl std::error::Error for TrialFigureError {}

/// The figures a trial run recorded, by name.
#[derive(Debug, Default, Clone)]
pub struct TrialFigures {
    entries: Vec<(String, f64)>,
}

impl TrialFigures {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, name: String, value: f64) {
        self.entries.push((name, value));
    }

    /// Records a percentile of `latencies` in milliseconds, or nothing when
    /// there are none, so an arm that completed no query reads as absent.
    pub fn record_percentile(&mut self, name: String, latencies: &[Duration], q: f64) {
        if let Some(p) = percentile(latencies, q) {
            self.record(name, p.as_secs_f64() * 1000.0);
        }
    }

    pub fn get(&self, name: &str) -> Result<f64, TrialFigureError> {
        let mut found = self.entries.iter().filter(|(n, _)| n == name);
        match (found.next(), found.next()) {
            (None, _) => Err(TrialFigureError::Absent(name.to_owned())),
            (Some(_), Some(_)) => Err(TrialFigureError::Duplicated(name.to_owned())),
            (Some((_, v)), None) => Ok(*v),
        }
    }

    /// One line per expected figure, `<name> <value>` or `<name> ERROR
    /// <reason>`, and every error in the order the lines name them.
    pub fn report(&self, expected: &[String]) -> (Vec<String>, Vec<TrialFigureError>) {
        let mut lines = Vec::with_capacity(expected.len());
        let mut errors = Vec::new();
        for name in expected {
            match self.get(name) {
                Ok(v) => lines.push(format!("{name} {v:.3}")),
                Err(e) => {
                    lines.push(format!("{name} ERROR {e}"));
                    errors.push(e);
                }
            }
        }
        (lines, errors)
    }
}

/// Nearest-rank percentile of unsorted `samples`; none when empty.
pub fn percentile(samples: &[Duration], q: f64) -> Option<Duration> {
    if samples.is_empty() {
        return None;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    Some(sorted[rank - 1])
}

/// Smallest, median and largest of `values`; none when empty. The median of
/// an even count is the lower of the two middle values, so it is always a
/// size some flush actually had.
pub fn min_median_max(values: &[u64]) -> Option<(u64, u64, u64)> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let (first, last) = (sorted.first()?, sorted.last()?);
    Some((*first, sorted[(sorted.len() - 1) / 2], *last))
}

/// `min/median/max over N flushes`, or `none` with no flush.
pub fn format_flush_input_bytes(values: &[u64]) -> String {
    match min_median_max(values) {
        Some((min, mid, max)) => format!("{min}/{mid}/{max} B over {} flushes", values.len()),
        None => "none (no flush)".to_owned(),
    }
}

/// Series one instance contributes to a body.
pub const SERIES_PER_INSTANCE: usize = 64;

/// Snappy Remote Write 1.0 bodies of about one compressed size.
pub struct RwBodies {
    pub bodies: Vec<Vec<u8>>,
    pub series_per_body: usize,
    /// Mean compressed size.
    pub compressed_bytes: usize,
}

fn write_request(instances: usize, variant: usize, ts_ms: i64) -> WriteRequest {
    let mut state = 0x9e37_79b9_7f4a_7c15u64 ^ (variant as u64 + 1);
    let mut timeseries = Vec::with_capacity(instances * SERIES_PER_INSTANCE);
    for i in 0..instances {
        let instance = format!("10.1.{}.{}:9100", i / 256, i % 256);
        for cpu in 0..8 {
            for mode in [
                "idle", "iowait", "irq", "nice", "softirq", "steal", "system", "user",
            ] {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let mut labels = vec![
                    PbLabel {
                        name: "__name__".to_owned(),
                        value: "node_cpu_seconds_total".to_owned(),
                    },
                    PbLabel {
                        name: "cpu".to_owned(),
                        value: cpu.to_string(),
                    },
                    PbLabel {
                        name: "instance".to_owned(),
                        value: instance.clone(),
                    },
                    PbLabel {
                        name: "job".to_owned(),
                        value: "node".to_owned(),
                    },
                    PbLabel {
                        name: "mode".to_owned(),
                        value: mode.to_owned(),
                    },
                ];
                labels.sort_by(|a, b| a.name.cmp(&b.name));
                timeseries.push(PbTimeSeries {
                    labels,
                    samples: vec![PbSample {
                        value: (state % 10_000_000) as f64 / 100.0,
                        timestamp: ts_ms,
                    }],
                    exemplars: Vec::new(),
                    histograms: Vec::new(),
                });
            }
        }
    }
    WriteRequest {
        timeseries,
        metadata: Vec::new(),
    }
}

fn snappy_body(instances: usize, variant: usize, ts_ms: i64) -> Result<Vec<u8>, snap::Error> {
    snap::raw::Encoder::new()
        .compress_vec(&write_request(instances, variant, ts_ms).encode_to_vec())
}

/// `variants` bodies whose compressed size is within 2% of `target`, each
/// with its own timestamp (`ts_ms` plus a second per variant).
pub fn rw_snappy_bodies(
    target: usize,
    variants: usize,
    ts_ms: i64,
) -> Result<RwBodies, snap::Error> {
    let mut instances = 4usize;
    for _ in 0..8 {
        let len = snappy_body(instances, 0, ts_ms)?.len();
        if len.abs_diff(target) * 50 <= target {
            break;
        }
        instances = ((instances as f64 * target as f64 / len as f64).round() as usize).max(1);
    }
    let mut bodies = Vec::with_capacity(variants);
    for variant in 0..variants {
        bodies.push(snappy_body(
            instances,
            variant,
            ts_ms + variant as i64 * 1000,
        )?);
    }
    let compressed_bytes = bodies.iter().map(Vec::len).sum::<usize>() / variants.max(1);
    Ok(RwBodies {
        bodies,
        series_per_body: instances * SERIES_PER_INSTANCE,
        compressed_bytes,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn complete(arms: &[TrialArm]) -> TrialFigures {
        let mut figs = TrialFigures::new();
        for (i, name) in expected_figures(arms).into_iter().enumerate() {
            figs.record(name, i as f64);
        }
        figs
    }

    #[test]
    fn every_expected_figure_is_named_once() {
        let names = expected_figures(&ARMS);
        assert_eq!(names.len(), 3 * 2 * 11);
        let mut unique = names.clone();
        unique.sort();
        unique.dedup();
        assert_eq!(unique.len(), names.len());
        assert_eq!(names[0], "ungated/c1/promql_p50_ms");
        assert_eq!(
            names.last().map(String::as_str),
            Some("gated_floor_1mib/c16/ingest_failed")
        );
    }

    #[test]
    fn a_complete_set_reports_every_figure_once_with_no_error() {
        let expected = expected_figures(&ARMS);
        let (lines, errors) = complete(&ARMS).report(&expected);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(lines.len(), expected.len());
        assert_eq!(lines[0], "ungated/c1/promql_p50_ms 0.000");
        assert_eq!(lines[1], "ungated/c1/promql_p99_ms 1.000");
    }

    #[test]
    fn a_missing_figure_is_absent_and_named() {
        let expected = expected_figures(&ARMS);
        let mut figs = TrialFigures::new();
        for name in expected
            .iter()
            .filter(|n| n.as_str() != "gated_floor_256kib/c16/sql_p99_ms")
        {
            figs.record(name.clone(), 1.0);
        }
        let (lines, errors) = figs.report(&expected);
        assert_eq!(
            errors,
            vec![TrialFigureError::Absent(
                "gated_floor_256kib/c16/sql_p99_ms".to_owned()
            )]
        );
        assert_eq!(lines.len(), expected.len());
        assert!(lines.contains(
            &"gated_floor_256kib/c16/sql_p99_ms ERROR figure gated_floor_256kib/c16/sql_p99_ms absent"
                .to_owned()
        ));
    }

    #[test]
    fn a_figure_recorded_twice_is_duplicated() {
        let expected = expected_figures(&ARMS);
        let mut figs = complete(&ARMS);
        figs.record("ungated/c1/ingest_req_per_s".to_owned(), 5.0);
        let (_, errors) = figs.report(&expected);
        assert_eq!(
            errors,
            vec![TrialFigureError::Duplicated(
                "ungated/c1/ingest_req_per_s".to_owned()
            )]
        );
    }

    #[test]
    fn an_arm_with_no_query_records_no_percentile() {
        let mut figs = TrialFigures::new();
        figs.record_percentile("a/c1/promql_p50_ms".to_owned(), &[], 0.5);
        assert_eq!(
            figs.get("a/c1/promql_p50_ms"),
            Err(TrialFigureError::Absent("a/c1/promql_p50_ms".to_owned()))
        );
        let ms = |n| Duration::from_millis(n);
        figs.record_percentile(
            "a/c1/promql_p99_ms".to_owned(),
            &[ms(3), ms(1), ms(2), ms(100)],
            0.99,
        );
        assert_eq!(figs.get("a/c1/promql_p99_ms"), Ok(100.0));
        assert_eq!(
            percentile(&[ms(3), ms(1), ms(2), ms(100)], 0.5),
            Some(ms(2))
        );
    }

    #[test]
    fn arms_are_ungated_and_the_two_floors() {
        let names: Vec<_> = ARMS.iter().map(|a| a.name).collect();
        assert_eq!(names, ["ungated", "gated_floor_256kib", "gated_floor_1mib"]);
        assert_eq!(ARMS[0].write_inline_floor_bytes, u64::MAX);
        assert_eq!(
            CODEC_INLINE_FLOOR_BYTES,
            ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES
        );
        assert_eq!(
            WRITE_INLINE_FLOOR_BYTES,
            ravel_cpu_gate::DEFAULT_WRITE_INLINE_FLOOR_BYTES
        );
        assert_eq!(ARMS[1].write_inline_floor_bytes, 256 * 1024);
        assert_eq!(ARMS[2].write_inline_floor_bytes, 1_048_576);
    }

    #[test]
    fn min_median_max_takes_the_lower_middle() {
        assert_eq!(min_median_max(&[]), None);
        assert_eq!(min_median_max(&[7]), Some((7, 7, 7)));
        assert_eq!(min_median_max(&[9, 1, 5]), Some((1, 5, 9)));
        assert_eq!(min_median_max(&[4, 1, 3, 2]), Some((1, 2, 4)));
        assert_eq!(format_flush_input_bytes(&[]), "none (no flush)");
        assert_eq!(
            format_flush_input_bytes(&[30, 10, 20]),
            "10/20/30 B over 3 flushes"
        );
    }

    #[test]
    fn bodies_are_the_target_size_and_decode_to_their_series() {
        let set = rw_snappy_bodies(BODY_TARGET_BYTES, 2, 1_700_000_000_000).expect("bodies");
        assert_eq!(set.bodies.len(), 2);
        for body in &set.bodies {
            assert!(
                body.len().abs_diff(BODY_TARGET_BYTES) * 50 <= BODY_TARGET_BYTES,
                "{} B against {BODY_TARGET_BYTES}",
                body.len()
            );
            let plain = snap::raw::Decoder::new()
                .decompress_vec(body)
                .expect("snappy");
            let request = WriteRequest::decode(plain.as_slice()).expect("protobuf");
            assert_eq!(request.timeseries.len(), set.series_per_body);
        }
        assert_ne!(set.bodies[0], set.bodies[1], "variants differ");
    }
}
