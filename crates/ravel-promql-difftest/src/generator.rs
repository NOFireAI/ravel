//! Seeded, deterministic dataset generator (docs/promql-evaluator-plan.md
//! section 5.2). Same seed and [`DatasetConfig`] always produce the same
//! series, labels, and samples, following the bench-generator discipline
//! (`crates/ravel-bench/src/generator.rs`): timestamps are derived from
//! `config.base_ts_ms`, never sampled from the clock.
//!
//! Constraints imposed by the Prometheus side of the harness (section 5.2):
//! every series is in strictly ascending timestamp order, no timestamp
//! repeats with a different value, and no sample accidentally carries the
//! stale-marker bit pattern ([`STALE_MARKER_BITS`]) unless the scenario is
//! explicitly about staleness.

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

/// Bit pattern Prometheus (and, since P1, Ravel) treats as "series absent at
/// this instant" rather than a live value (a10-F01, docs/adrs/0010.md
/// section 5). Distinct from a quiet NaN ([`f64::NAN`]'s bit pattern is
/// `0x7ff8_0000_0000_0000`), so ordinary NaN injections never collide with
/// it by accident.
pub const STALE_MARKER_BITS: u64 = 0x7ff0_0000_0000_0002;

pub fn stale_marker() -> f64 {
    f64::from_bits(STALE_MARKER_BITS)
}

/// One generated series: full label set (including `__name__`, always the
/// first entry) and its samples in ascending timestamp order.
#[derive(Debug, Clone)]
pub struct GeneratedSeries {
    pub labels: Vec<(String, String)>,
    /// `(ts_ms, value)`, strictly ascending by `ts_ms`.
    pub samples: Vec<(i64, f64)>,
}

impl GeneratedSeries {
    pub fn metric_name(&self) -> &str {
        self.labels
            .first()
            .map(|(_, v)| v.as_str())
            .unwrap_or_default()
    }
}

#[derive(Debug, Clone)]
pub struct Dataset {
    pub series: Vec<GeneratedSeries>,
}

/// Knobs for [`generate`]. `base_ts_ms` anchors every series' first sample;
/// corpus entries reference offsets from it rather than wall-clock time
/// (CLAUDE.md: "time is injected").
#[derive(Debug, Clone, Copy)]
pub struct DatasetConfig {
    pub seed: u64,
    pub base_ts_ms: i64,
    /// Nominal scrape interval in milliseconds.
    pub step_ms: i64,
    /// Samples per regularly-spaced series.
    pub sample_count: usize,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        DatasetConfig {
            seed: 0xD1FF_7E57,
            base_ts_ms: 1_700_000_000_000,
            step_ms: 30_000,
            sample_count: 40,
        }
    }
}

fn labels(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

fn metric(name: &str, extra: &[(&str, &str)]) -> Vec<(String, String)> {
    let mut out = vec![("__name__".to_string(), name.to_string())];
    out.extend(extra.iter().map(|(k, v)| (k.to_string(), v.to_string())));
    out
}

/// Builds the full P3 selector-corpus dataset: every shape section 5.2
/// requires, each under a distinct `diff_*` metric name so corpus entries
/// can select exactly one scenario by name.
pub fn generate(config: &DatasetConfig) -> Dataset {
    let mut rng = StdRng::seed_from_u64(config.seed);
    let mut series = vec![
        gauge_walk(&mut rng, config),
        counter_with_reset(&mut rng, config),
        counter_reset_at_boundary(config),
        sparse_jittered(&mut rng, config),
        boundary_aligned(config),
        special_values(config),
        stale_then_live(config),
    ];
    series.extend(classic_histogram_family(&mut rng, config, false));
    series.extend(classic_histogram_family(&mut rng, config, true));
    series.extend(vector_matching_shapes(&mut rng, config));

    Dataset { series }
}

fn regular_timestamps(config: &DatasetConfig) -> Vec<i64> {
    (0..config.sample_count as i64)
        .map(|i| config.base_ts_ms + i * config.step_ms)
        .collect()
}

fn gauge_walk(rng: &mut StdRng, config: &DatasetConfig) -> GeneratedSeries {
    let mut value = 0.0f64;
    let samples = regular_timestamps(config)
        .into_iter()
        .map(|ts| {
            value += rng.random_range(-1.0..1.0);
            (ts, value)
        })
        .collect();
    GeneratedSeries {
        labels: metric("diff_gauge_walk", &[("shape", "walk")]),
        samples,
    }
}

/// A monotonically increasing counter with one deliberate reset (value
/// drops back near zero) partway through, exercising the P4 rate-family
/// counter-reset path once it lands; for P3 it is exercised only as a plain
/// selector.
fn counter_with_reset(rng: &mut StdRng, config: &DatasetConfig) -> GeneratedSeries {
    let reset_at = config.sample_count / 2;
    let mut value = 0.0f64;
    let samples = regular_timestamps(config)
        .into_iter()
        .enumerate()
        .map(|(i, ts)| {
            if i == reset_at {
                value = rng.random_range(0.0..2.0);
            } else {
                value += rng.random_range(0.0..5.0);
            }
            (ts, value)
        })
        .collect();
    GeneratedSeries {
        labels: metric("diff_counter_total", &[("shape", "reset")]),
        samples,
    }
}

/// Reset lands exactly on a 5m-lookback window boundary relative to
/// `base_ts_ms`, so instant/range queries anchored on that boundary
/// exercise the left-open-interval edge deterministically.
fn counter_reset_at_boundary(config: &DatasetConfig) -> GeneratedSeries {
    let lookback_steps = (300_000 / config.step_ms.max(1)).max(1);
    let samples: Vec<(i64, f64)> = regular_timestamps(config)
        .into_iter()
        .enumerate()
        .map(|(i, ts)| {
            let idx = i as i64;
            let value = if idx < lookback_steps {
                (idx as f64) * 10.0
            } else {
                ((idx - lookback_steps) as f64) * 10.0
            };
            (ts, value)
        })
        .collect();
    GeneratedSeries {
        labels: metric("diff_counter_total", &[("shape", "reset_at_boundary")]),
        samples,
    }
}

/// Irregular, always-increasing gaps around the nominal step (scrape
/// jitter): every gap is `step_ms + jitter`, jitter drawn from `[0,
/// step_ms/2]`, so ordering is preserved by construction.
fn sparse_jittered(rng: &mut StdRng, config: &DatasetConfig) -> GeneratedSeries {
    let max_jitter = (config.step_ms / 2).max(1);
    let mut ts = config.base_ts_ms;
    let mut value = 0.0f64;
    let mut samples = Vec::with_capacity(config.sample_count);
    for _ in 0..config.sample_count {
        value += rng.random_range(-1.0..1.0);
        samples.push((ts, value));
        ts += config.step_ms + rng.random_range(0..=max_jitter);
    }
    GeneratedSeries {
        labels: metric("diff_sparse_jitter", &[("shape", "sparse")]),
        samples,
    }
}

/// One sample landing exactly on the start, middle, and end of a 5m
/// lookback window relative to `base_ts_ms`, for boundary-inclusive /
/// left-open-exclusive lookback assertions.
fn boundary_aligned(config: &DatasetConfig) -> GeneratedSeries {
    let samples = vec![
        (config.base_ts_ms, 1.0),
        (config.base_ts_ms + 300_000, 2.0),
        (config.base_ts_ms + 600_000, 3.0),
    ];
    GeneratedSeries {
        labels: metric("diff_boundary", &[("shape", "boundary")]),
        samples,
    }
}

/// +Inf / -Inf / NaN / -0.0 interleaved with ordinary values, one per
/// timestamp so a selector at each exact instant isolates one special
/// value.
fn special_values(config: &DatasetConfig) -> GeneratedSeries {
    let values = [
        1.5,
        f64::INFINITY,
        -1.5,
        f64::NEG_INFINITY,
        f64::NAN,
        -0.0,
        0.0,
    ];
    let samples = values
        .iter()
        .enumerate()
        .map(|(i, v)| (config.base_ts_ms + i as i64 * config.step_ms, *v))
        .collect();
    GeneratedSeries {
        labels: metric("diff_special_values", &[("shape", "special")]),
        samples,
    }
}

/// A live sample, then a stale marker, then another live sample: the
/// a10-F01 acceptance shape (series absent exactly at the stale instant,
/// present again after).
fn stale_then_live(config: &DatasetConfig) -> GeneratedSeries {
    let samples = vec![
        (config.base_ts_ms, 5.0),
        (config.base_ts_ms + config.step_ms, stale_marker()),
        (config.base_ts_ms + 2 * config.step_ms, 6.0),
    ];
    GeneratedSeries {
        labels: metric("diff_stale", &[("shape", "stale")]),
        samples,
    }
}

/// Classic `le`-bucketed histogram family. `bad` selects a deliberately
/// non-monotonic bucket count at the last timestamp (a lower `le` bucket
/// reports a higher count than the next one up), which is otherwise
/// well-formed (has `+Inf`).
fn classic_histogram_family(
    rng: &mut StdRng,
    config: &DatasetConfig,
    bad: bool,
) -> Vec<GeneratedSeries> {
    let buckets = ["0.1", "0.5", "1", "2.5", "+Inf"];
    let variant = if bad { "nonmonotonic" } else { "wellformed" };
    let timestamps = regular_timestamps(config);
    let mut cumulative = vec![0.0f64; buckets.len()];
    let mut per_bucket_series: Vec<GeneratedSeries> = buckets
        .iter()
        .map(|le| GeneratedSeries {
            labels: metric("diff_histogram_bucket", &[("variant", variant), ("le", le)]),
            samples: Vec::with_capacity(timestamps.len()),
        })
        .collect();

    for (t_idx, ts) in timestamps.iter().enumerate() {
        for c in cumulative.iter_mut() {
            *c += rng.random_range(0.0..3.0);
        }
        // Monotonicity fixup: running max so far keeps le-order monotonic.
        for b in 1..buckets.len() {
            if cumulative[b] < cumulative[b - 1] {
                cumulative[b] = cumulative[b - 1];
            }
        }
        let mut row = cumulative.clone();
        if bad && t_idx == timestamps.len() - 1 && row.len() >= 2 {
            let last = row.len() - 1;
            row[last - 1] = row[last] + 5.0;
        }
        for (b, series) in per_bucket_series.iter_mut().enumerate() {
            series.samples.push((*ts, row[b]));
        }
    }
    per_bucket_series
}

/// Label shapes for vector-matching (P7): a one-to-one matching pair, a
/// non-matching pair, a many-to-one (`group_left`) shape, and (added for
/// P7's binary-operator corpus) duplicate-signature shapes for
/// matching-cardinality error cases plus an extra `group_left` "one" side
/// carrying a label absent from the "many" side, for the label-copy
/// delete case.
fn vector_matching_shapes(rng: &mut StdRng, config: &DatasetConfig) -> Vec<GeneratedSeries> {
    let ts = regular_timestamps(config);
    let mut make = |name: &str, extra: &[(&str, &str)]| -> GeneratedSeries {
        let mut value = rng.random_range(0.0..10.0);
        let samples = ts
            .iter()
            .map(|t| {
                value += rng.random_range(-0.5..0.5);
                (*t, value)
            })
            .collect();
        GeneratedSeries {
            labels: metric(name, extra),
            samples,
        }
    };
    vec![
        make("diff_match_left", &[("job", "api"), ("instance", "1")]),
        make("diff_match_right", &[("job", "api"), ("instance", "1")]),
        make("diff_match_left", &[("job", "api"), ("instance", "2")]),
        make("diff_match_right", &[("job", "other"), ("instance", "2")]),
        make("diff_group_left", &[("job", "batch"), ("instance", "1")]),
        make("diff_group_left", &[("job", "batch"), ("instance", "2")]),
        make("diff_group_right", &[("job", "batch")]),
        // Two `diff_dup_left` series that agree on `job` (their on(job)
        // matching signature) but differ on `replica`: an `on(job)` match
        // against either dup side is ambiguous.
        make("diff_dup_left", &[("job", "dup"), ("replica", "a")]),
        make("diff_dup_left", &[("job", "dup"), ("replica", "b")]),
        // Symmetric duplicate on the right-hand side.
        make("diff_dup_right", &[("job", "dup"), ("region", "us")]),
        make("diff_dup_right", &[("job", "dup"), ("region", "eu")]),
        // A `group_left` "one" side carrying `version`, a label
        // `diff_group_left`'s series never had: the copied-label delete
        // case (the many side's own `version`, if any, is removed since
        // the one side is absent for it).
        make("diff_group_meta", &[("job", "batch"), ("version", "v2")]),
    ]
}

#[allow(dead_code)]
fn unused_labels_helper_silencer() {
    let _ = labels(&[]);
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_by_seed() {
        let config = DatasetConfig::default();
        let a = generate(&config);
        let b = generate(&config);
        assert_eq!(a.series.len(), b.series.len());
        for (x, y) in a.series.iter().zip(b.series.iter()) {
            assert_eq!(x.labels, y.labels);
            for ((ts_a, va), (ts_b, vb)) in x.samples.iter().zip(y.samples.iter()) {
                assert_eq!(ts_a, ts_b);
                assert_eq!(va.to_bits(), vb.to_bits());
            }
        }
    }

    #[test]
    fn different_seed_diverges_values_not_identity() {
        let mut config = DatasetConfig::default();
        let a = generate(&config);
        config.seed ^= 0xABCD;
        let b = generate(&config);
        let walk_a = a
            .series
            .iter()
            .find(|s| s.metric_name() == "diff_gauge_walk")
            .expect("gauge walk series");
        let walk_b = b
            .series
            .iter()
            .find(|s| s.metric_name() == "diff_gauge_walk")
            .expect("gauge walk series");
        assert_ne!(walk_a.samples, walk_b.samples);
    }

    #[test]
    fn every_series_is_strictly_ascending_and_dup_free() {
        let dataset = generate(&DatasetConfig::default());
        for series in &dataset.series {
            for pair in series.samples.windows(2) {
                assert!(
                    pair[0].0 < pair[1].0,
                    "series {:?} out of order at {:?}",
                    series.labels,
                    pair
                );
            }
        }
    }

    #[test]
    fn no_sample_accidentally_carries_the_stale_marker() {
        let dataset = generate(&DatasetConfig::default());
        for series in &dataset.series {
            let is_stale_scenario = series
                .labels
                .iter()
                .any(|(k, v)| k == "shape" && v == "stale");
            for (_, value) in &series.samples {
                let is_marker = value.to_bits() == STALE_MARKER_BITS;
                assert!(
                    is_stale_scenario || !is_marker,
                    "series {:?} carries an accidental stale marker",
                    series.labels
                );
            }
        }
    }

    #[test]
    fn stale_scenario_has_exactly_one_stale_sample() {
        let dataset = generate(&DatasetConfig::default());
        let stale = dataset
            .series
            .iter()
            .find(|s| s.metric_name() == "diff_stale")
            .expect("stale series present");
        let count = stale
            .samples
            .iter()
            .filter(|(_, v)| v.to_bits() == STALE_MARKER_BITS)
            .count();
        assert_eq!(count, 1);
    }

    #[test]
    fn histogram_family_has_the_inf_bucket_and_is_monotonic_when_wellformed() {
        let dataset = generate(&DatasetConfig::default());
        let inf_bucket = dataset.series.iter().find(|s| {
            s.metric_name() == "diff_histogram_bucket"
                && s.labels.iter().any(|(k, v)| k == "le" && v == "+Inf")
                && s.labels
                    .iter()
                    .any(|(k, v)| k == "variant" && v == "wellformed")
        });
        assert!(inf_bucket.is_some());

        let wellformed: Vec<&GeneratedSeries> = dataset
            .series
            .iter()
            .filter(|s| {
                s.metric_name() == "diff_histogram_bucket"
                    && s.labels
                        .iter()
                        .any(|(k, v)| k == "variant" && v == "wellformed")
            })
            .collect();
        for t_idx in 0..wellformed[0].samples.len() {
            let mut counts: Vec<f64> = wellformed.iter().map(|s| s.samples[t_idx].1).collect();
            let sorted = {
                let mut c = counts.clone();
                c.sort_by(|a, b| a.partial_cmp(b).expect("no NaN in histogram counts"));
                c
            };
            counts.sort_by(|a, b| a.partial_cmp(b).expect("no NaN"));
            assert_eq!(counts, sorted);
        }
    }

    #[test]
    fn bad_histogram_variant_is_non_monotonic_at_the_last_timestamp() {
        let dataset = generate(&DatasetConfig::default());
        let bad: Vec<&GeneratedSeries> = dataset
            .series
            .iter()
            .filter(|s| {
                s.metric_name() == "diff_histogram_bucket"
                    && s.labels
                        .iter()
                        .any(|(k, v)| k == "variant" && v == "nonmonotonic")
            })
            .collect();
        let last = bad[0].samples.len() - 1;
        let mut by_le: Vec<(&str, f64)> = bad
            .iter()
            .map(|s| {
                let le = s
                    .labels
                    .iter()
                    .find(|(k, _)| k == "le")
                    .map(|(_, v)| v.as_str())
                    .unwrap_or_default();
                (le, s.samples[last].1)
            })
            .collect();
        by_le.sort_by(|a, b| a.0.cmp(b.0));
        // Bucket order in generation is 0.1, 0.5, 1, 2.5, +Inf; assert the
        // deliberate violation exists somewhere in the last row.
        let counts: Vec<f64> = ["0.1", "0.5", "1", "2.5", "+Inf"]
            .iter()
            .map(|le| {
                bad.iter()
                    .find(|s| {
                        s.metric_name() == "diff_histogram_bucket"
                            && s.labels.iter().any(|(k, v)| k == "le" && v == le)
                    })
                    .expect("bucket")
                    .samples[last]
                    .1
            })
            .collect();
        let is_monotonic = counts.windows(2).all(|w| w[0] <= w[1]);
        assert!(!is_monotonic, "expected a deliberate non-monotonic bucket");
        let _ = by_le;
    }
}
