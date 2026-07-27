//! Deterministic-by-seed synthetic workload generator (docs/benchmarking.md
//! "Workload generator" section). Same seed and config always produce the
//! same series, labels, and samples.

use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use ravel_otlp::NormalizedPoint;
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId, TypeError};

/// Gauge or cumulative counter, matching docs/benchmarking.md's workload list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricKind {
    Gauge,
    Counter,
}

/// Label cardinality shape: a handful of big, shared label sets versus many
/// small, mostly-unique ones.
#[derive(Debug, Clone, Copy)]
pub struct CardinalityProfile {
    /// Number of distinct base label sets shared across series.
    pub distinct_sets: usize,
    /// Label pairs per base set, excluding the per-series uniquifying label.
    pub labels_per_set: usize,
}

impl CardinalityProfile {
    /// A handful of big label sets shared by most series.
    pub fn few_big(series_count: usize) -> Self {
        CardinalityProfile {
            distinct_sets: 4.min(series_count.max(1)),
            labels_per_set: 20,
        }
    }

    /// One small, mostly-unique label set per series.
    pub fn many_small(series_count: usize) -> Self {
        CardinalityProfile {
            distinct_sets: series_count.max(1),
            labels_per_set: 4,
        }
    }

    /// [`many_small`](Self::many_small) with an explicit base-label count
    /// instead of the fixed 4, for the label-count axis sweep (issue #98).
    /// The generator always appends one uniquifying `series_idx` label, so a
    /// series built from this profile carries `labels_per_set + 1` labels.
    pub fn many_small_with_labels(series_count: usize, labels_per_set: usize) -> Self {
        CardinalityProfile {
            distinct_sets: series_count.max(1),
            labels_per_set,
        }
    }
}

/// Batch sizes drawn uniformly from `[min, max]`.
#[derive(Debug, Clone, Copy)]
pub struct BatchSizeDistribution {
    pub min: usize,
    pub max: usize,
}

impl BatchSizeDistribution {
    pub fn fixed(size: usize) -> Self {
        BatchSizeDistribution {
            min: size.max(1),
            max: size.max(1),
        }
    }

    pub fn uniform(min: usize, max: usize) -> Self {
        let min = min.max(1);
        BatchSizeDistribution {
            min,
            max: max.max(min),
        }
    }

    fn sample(&self, rng: &mut StdRng) -> usize {
        if self.min >= self.max {
            self.min
        } else {
            rng.random_range(self.min..=self.max)
        }
    }
}

/// Full knob set for [`generate_raw`], [`generate_normalized`], and
/// [`generate_batches`].
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    pub seed: u64,
    pub tenant: String,
    pub series_count: usize,
    pub samples_per_series: usize,
    pub start_ts_ns: i64,
    pub interval_ns: i64,
    /// Max +/- nanoseconds of jitter applied to each sample's timestamp.
    pub jitter_ns: i64,
    /// Fraction of samples (after jitter) pushed behind the previous one.
    pub out_of_order_fraction: f64,
    /// Fraction of series whose label set changes value halfway through,
    /// which mints a second, distinct series id for the tail samples.
    pub label_churn_rate: f64,
    /// Fraction of series generated as cumulative counters (rest are gauges).
    pub counter_fraction: f64,
    pub cardinality: CardinalityProfile,
    pub batch_size: BatchSizeDistribution,
}

/// Total sample budget the axis-sweep cells hold fixed (issue #98): every
/// cell derives its series count as `AXIS_SWEEP_TOTAL_SAMPLES /
/// samples_per_series`, so encode/decode work is compared at a constant total
/// sample count while samples-per-series and label-count vary independently.
pub const AXIS_SWEEP_TOTAL_SAMPLES: usize = 200_000;

impl WorkloadConfig {
    /// One cell of the samples-per-series x labels-per-series axis sweep
    /// (issue #98). Fixes the total sample count at [`AXIS_SWEEP_TOTAL_SAMPLES`]
    /// by deriving `series_count = AXIS_SWEEP_TOTAL_SAMPLES /
    /// samples_per_series` (so the product is exactly the budget when it
    /// divides evenly, otherwise the largest whole-series total at or below
    /// it), and sets a `many_small` cardinality whose series each carry
    /// exactly `labels_per_series` labels (the profile holds
    /// `labels_per_series - 1` base labels; the generator appends one
    /// uniquifying `series_idx` label). Every other knob keeps its
    /// [`Default`] value, so this is additive: existing entry points and
    /// callers that build `WorkloadConfig` directly are untouched.
    pub fn axis_sweep(samples_per_series: usize, labels_per_series: usize) -> Self {
        let samples_per_series = samples_per_series.max(1);
        let series_count = (AXIS_SWEEP_TOTAL_SAMPLES / samples_per_series).max(1);
        // series_idx is always appended by the generator, so the base label
        // count is one less than the requested per-series total.
        let labels_per_set = labels_per_series.max(1) - 1;
        WorkloadConfig {
            series_count,
            samples_per_series,
            cardinality: CardinalityProfile::many_small_with_labels(series_count, labels_per_set),
            ..Default::default()
        }
    }
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        WorkloadConfig {
            seed: 0,
            tenant: "bench-tenant".to_string(),
            series_count: 100,
            samples_per_series: 100,
            start_ts_ns: 1_700_000_000_000_000_000,
            interval_ns: 1_000_000_000,
            jitter_ns: 0,
            out_of_order_fraction: 0.0,
            label_churn_rate: 0.0,
            counter_fraction: 0.5,
            cardinality: CardinalityProfile::many_small(100),
            batch_size: BatchSizeDistribution::fixed(64),
        }
    }
}

/// One physical series: identity, labels, kind, and its generated samples.
/// A single logical series index can produce two of these when label churn
/// splits it mid-stream, since a label-value change is a new series id.
pub struct GeneratedSeries {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub kind: MetricKind,
    pub samples: Vec<Sample>,
}

/// Raw `(SeriesId, LabelSet, Vec<Sample>)` form consumed by
/// `ravel_segment::SegmentWriter` and the segment benches.
pub fn generate_raw(
    config: &WorkloadConfig,
) -> Result<Vec<(SeriesId, LabelSet, Vec<Sample>)>, TypeError> {
    Ok(generate_series(config)?
        .into_iter()
        .map(|s| (s.series_id, s.labels, s.samples))
        .collect())
}

/// Flat `Vec<NormalizedPoint>` form, arrival-order interleaved across
/// series, reused directly by the ingest benchmark.
pub fn generate_normalized(config: &WorkloadConfig) -> Result<Vec<NormalizedPoint>, TypeError> {
    Ok(interleave(generate_series(config)?))
}

/// Same as [`generate_normalized`], chunked per `config.batch_size` to
/// simulate discrete ingest requests.
pub fn generate_batches(config: &WorkloadConfig) -> Result<Vec<Vec<NormalizedPoint>>, TypeError> {
    let mut rng = StdRng::seed_from_u64(config.seed ^ 0xBA7C_BA7C_BA7C_BA7C);
    let mut rest = generate_normalized(config)?;
    let mut batches = Vec::new();
    while !rest.is_empty() {
        let take = config.batch_size.sample(&mut rng).min(rest.len());
        batches.push(rest.drain(..take).collect());
    }
    Ok(batches)
}

fn base_label_set(base_idx: usize, labels_per_set: usize) -> Vec<Label> {
    (0..labels_per_set)
        .map(|j| Label {
            name: format!("label_{j:03}"),
            value: format!("v{base_idx}_{j}"),
        })
        .collect()
}

fn interleave(series: Vec<GeneratedSeries>) -> Vec<NormalizedPoint> {
    let max_len = series.iter().map(|s| s.samples.len()).max().unwrap_or(0);
    let total: usize = series.iter().map(|s| s.samples.len()).sum();
    let mut out = Vec::with_capacity(total);
    for i in 0..max_len {
        for s in &series {
            if let Some(sample) = s.samples.get(i) {
                out.push(NormalizedPoint {
                    series_id: s.series_id,
                    labels: s.labels.clone(),
                    sample: *sample,
                    is_monotonic_sum: matches!(s.kind, MetricKind::Counter),
                });
            }
        }
    }
    out
}

fn generate_series(config: &WorkloadConfig) -> Result<Vec<GeneratedSeries>, TypeError> {
    let tenant = TenantId::new(config.tenant.clone());
    let mut rng = StdRng::seed_from_u64(config.seed);
    let profile = config.cardinality;
    let mut out = Vec::with_capacity(config.series_count);

    for i in 0..config.series_count {
        let base_idx = i % profile.distinct_sets.max(1);
        let kind = if rng.random_bool(config.counter_fraction.clamp(0.0, 1.0)) {
            MetricKind::Counter
        } else {
            MetricKind::Gauge
        };
        let metric_name = match kind {
            MetricKind::Counter => "bench_counter_total",
            MetricKind::Gauge => "bench_gauge",
        };

        let mut base_labels = base_label_set(base_idx, profile.labels_per_set);
        base_labels.push(Label {
            name: "series_idx".to_string(),
            value: i.to_string(),
        });

        let churn_at = rng
            .random_bool(config.label_churn_rate.clamp(0.0, 1.0))
            .then_some(config.samples_per_series / 2);

        let first_labels = LabelSet::new(base_labels.clone())?;
        let first_len = churn_at.unwrap_or(config.samples_per_series);

        let mut ts = config.start_ts_ns;
        let mut value = match kind {
            MetricKind::Counter => 0.0,
            MetricKind::Gauge => rng.random_range(-100.0..100.0),
        };
        let mut samples = Vec::with_capacity(first_len);
        for _ in 0..first_len {
            let (next_ts, next_value) = next_sample(&mut rng, config, kind, ts, value);
            ts = next_ts;
            value = next_value;
            samples.push(Sample { ts_ns: ts, value });
        }
        out.push(GeneratedSeries {
            series_id: SeriesId::compute(&tenant, metric_name, &first_labels)?,
            labels: first_labels,
            kind,
            samples,
        });

        if let Some(cut) = churn_at {
            let mut churned_labels = base_labels.clone();
            match churned_labels.iter_mut().find(|l| l.name == "label_000") {
                Some(l) => l.value = format!("{}-churned", l.value),
                None => churned_labels.push(Label {
                    name: "churn".to_string(),
                    value: "1".to_string(),
                }),
            }
            let churned_labels = LabelSet::new(churned_labels)?;
            let remaining = config.samples_per_series - cut;
            let mut samples = Vec::with_capacity(remaining);
            for _ in 0..remaining {
                let (next_ts, next_value) = next_sample(&mut rng, config, kind, ts, value);
                ts = next_ts;
                value = next_value;
                samples.push(Sample { ts_ns: ts, value });
            }
            if !samples.is_empty() {
                out.push(GeneratedSeries {
                    series_id: SeriesId::compute(&tenant, metric_name, &churned_labels)?,
                    labels: churned_labels,
                    kind,
                    samples,
                });
            }
        }
    }
    Ok(out)
}

fn next_sample(
    rng: &mut StdRng,
    config: &WorkloadConfig,
    kind: MetricKind,
    prev_ts: i64,
    prev_value: f64,
) -> (i64, f64) {
    let mut ts = prev_ts + config.interval_ns;
    if config.jitter_ns > 0 {
        ts += rng.random_range(-config.jitter_ns..=config.jitter_ns);
    }
    if config.out_of_order_fraction > 0.0
        && rng.random_bool(config.out_of_order_fraction.clamp(0.0, 1.0))
    {
        ts -= 2 * config.interval_ns.max(1);
    }
    let value = match kind {
        MetricKind::Counter => prev_value + rng.random_range(0.0..5.0),
        MetricKind::Gauge => prev_value + rng.random_range(-1.0..1.0),
    };
    (ts, value)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_by_seed() {
        let config = WorkloadConfig {
            series_count: 10,
            samples_per_series: 5,
            ..Default::default()
        };
        let a = generate_raw(&config).expect("generate");
        let b = generate_raw(&config).expect("generate");
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.0, y.0);
            assert_eq!(x.2, y.2);
        }
    }

    #[test]
    fn different_seed_diverges() {
        let mut config = WorkloadConfig {
            series_count: 10,
            samples_per_series: 5,
            ..Default::default()
        };
        let a = generate_raw(&config).expect("generate");
        config.seed = 1;
        let b = generate_raw(&config).expect("generate");
        // Series identity depends only on labels, which this config assigns
        // deterministically regardless of seed; the seed instead varies the
        // sampled values.
        assert_eq!(a[0].0, b[0].0);
        assert_ne!(a[0].2, b[0].2);
    }

    #[test]
    fn label_churn_splits_series() {
        let config = WorkloadConfig {
            series_count: 20,
            samples_per_series: 10,
            label_churn_rate: 1.0,
            ..Default::default()
        };
        let series = generate_series(&config).expect("generate");
        assert!(series.len() > 20);
        for s in &series {
            assert!(!s.samples.is_empty());
        }
    }

    #[test]
    fn normalized_matches_raw_sample_count() {
        let config = WorkloadConfig {
            series_count: 8,
            samples_per_series: 6,
            ..Default::default()
        };
        let raw = generate_raw(&config).expect("generate");
        let normalized = generate_normalized(&config).expect("generate");
        let raw_total: usize = raw.iter().map(|(_, _, samples)| samples.len()).sum();
        assert_eq!(raw_total, normalized.len());
    }

    #[test]
    fn batches_cover_all_points() {
        let config = WorkloadConfig {
            series_count: 5,
            samples_per_series: 5,
            batch_size: BatchSizeDistribution::uniform(2, 4),
            ..Default::default()
        };
        let normalized = generate_normalized(&config).expect("generate");
        let batches = generate_batches(&config).expect("generate");
        let batched_total: usize = batches.iter().map(|b| b.len()).sum();
        assert_eq!(batched_total, normalized.len());
    }

    #[test]
    fn axis_sweep_honors_series_count_and_total_samples() {
        // Each cell fixes the total sample budget and derives series count.
        for &spp in &[2usize, 60, 500] {
            let config = WorkloadConfig::axis_sweep(spp, 5);
            let expected_series = AXIS_SWEEP_TOTAL_SAMPLES / spp;
            assert_eq!(
                config.series_count, expected_series,
                "series_count for spp={spp}"
            );
            assert_eq!(config.samples_per_series, spp);

            let raw = generate_raw(&config).expect("generate");
            // No churn by default, so one physical series per logical series.
            assert_eq!(raw.len(), expected_series, "raw series for spp={spp}");
            let total: usize = raw.iter().map(|(_, _, samples)| samples.len()).sum();
            assert_eq!(total, expected_series * spp, "total samples for spp={spp}");
            // The budget is honored to within one series' worth of samples
            // (exact when spp divides the budget: 2 and 500 do, 60 leaves a
            // sub-series remainder).
            assert!(
                AXIS_SWEEP_TOTAL_SAMPLES - total < spp,
                "total {total} within one series of budget for spp={spp}"
            );
        }
    }

    #[test]
    fn axis_sweep_honors_label_count() {
        for &labels in &[5usize, 15] {
            let config = WorkloadConfig::axis_sweep(60, labels);
            let raw = generate_raw(&config).expect("generate");
            assert!(!raw.is_empty());
            for (_, label_set, _) in &raw {
                assert_eq!(
                    label_set.len(),
                    labels,
                    "each series carries exactly {labels} labels"
                );
            }
        }
    }

    #[test]
    fn cardinality_profiles_differ() {
        let few = CardinalityProfile::few_big(1000);
        let many = CardinalityProfile::many_small(1000);
        assert!(few.distinct_sets < many.distinct_sets);
        assert!(few.labels_per_set > many.labels_per_set);
    }
}
