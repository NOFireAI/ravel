//! Deterministic workload generator (ADR-0068): tenants,
//! per-tenant series cardinality shapes, scalar and native-histogram points
//! over a simulated time span, and a query mix -- all fully determined by
//! the `"workload"` sub-seed derived from a [`MasterSeed`].
//!
//! Mirrors, rather than reuses, ravel-bench's `CardinalityProfile` shape
//! concept (few big label sets vs. many small ones): `ravel-bench` is not
//! in the workspace's shared `[workspace.dependencies]` table, so this
//! generator is self-contained instead of adding an unlisted dependency.

use rand::RngExt;
use rand::rngs::StdRng;
use ravel_segment::{HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, ResetHint};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId, TypeError};

use crate::seed::MasterSeed;

/// Label cardinality shape a tenant's series are drawn from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardinalityShape {
    /// A handful of big label sets shared by most series.
    FewBigLabels,
    /// One small, mostly-unique label set per series.
    ManySmallLabels,
}

impl CardinalityShape {
    fn distinct_sets(self, series_count: usize) -> usize {
        match self {
            CardinalityShape::FewBigLabels => 4.min(series_count.max(1)),
            CardinalityShape::ManySmallLabels => series_count.max(1),
        }
    }

    fn labels_per_set(self) -> usize {
        match self {
            CardinalityShape::FewBigLabels => 20,
            CardinalityShape::ManySmallLabels => 4,
        }
    }
}

/// Knobs for [`generate`]. Every field is read deterministically by the
/// sub-seeded RNG; two calls with the same [`MasterSeed`] and config always
/// produce byte-identical output.
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    pub tenant_count: usize,
    pub series_per_tenant: usize,
    pub samples_per_series: usize,
    pub cardinality: CardinalityShape,
    /// Fraction of series generated as native histograms rather than scalars.
    pub histogram_fraction: f64,
    pub start_ts_ns: i64,
    pub interval_ns: i64,
    /// Instant/range queries generated per tenant (alternating instant,
    /// range in generation order).
    pub queries_per_tenant: usize,
}

impl Default for WorkloadConfig {
    fn default() -> Self {
        WorkloadConfig {
            tenant_count: 2,
            series_per_tenant: 12,
            samples_per_series: 8,
            cardinality: CardinalityShape::ManySmallLabels,
            histogram_fraction: 0.25,
            start_ts_ns: 1_700_000_000_000_000_000,
            interval_ns: 1_000_000_000,
            queries_per_tenant: 4,
        }
    }
}

/// One generated series' samples: exactly one of scalar points or
/// native-histogram points, never both (a series' value kind never mixes,
/// the same convention `HistogramCounts::Int`/`Float` follows).
#[derive(Debug, Clone)]
pub enum SeriesSamples {
    Scalar(Vec<Sample>),
    Histogram(Vec<HistogramSample>),
}

#[derive(Debug, Clone)]
pub struct GeneratedSeries {
    pub series_id: SeriesId,
    pub labels: LabelSet,
    pub metric_name: String,
    pub samples: SeriesSamples,
}

/// One query the driver runs through `QueryEngine` after the fold.
#[derive(Debug, Clone)]
pub enum QuerySpec {
    Instant {
        query: String,
        t_ms: i64,
    },
    Range {
        query: String,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    },
}

#[derive(Debug, Clone)]
pub struct TenantWorkload {
    pub tenant: TenantId,
    pub series: Vec<GeneratedSeries>,
    pub queries: Vec<QuerySpec>,
}

#[derive(Debug, Clone)]
pub struct Workload {
    pub tenants: Vec<TenantWorkload>,
    pub start_ts_ns: i64,
    pub end_ts_ns: i64,
}

/// Generates a full workload from `master_seed`'s `"workload"` sub-seed.
/// Fully deterministic: no OS entropy, no wall clock is read, and series
/// live in `Vec`s in generation order throughout (no HashMap iteration
/// anywhere order-sensitive).
pub fn generate(master_seed: &MasterSeed, config: &WorkloadConfig) -> Result<Workload, TypeError> {
    let mut rng = master_seed.rng("workload");
    let mut tenants = Vec::with_capacity(config.tenant_count);
    let mut end_ts_ns = config.start_ts_ns;

    for t in 0..config.tenant_count {
        let tenant = TenantId::new(format!("sim-tenant-{t:03}"));
        let (series, tenant_end) = generate_tenant_series(&mut rng, &tenant, config)?;
        end_ts_ns = end_ts_ns.max(tenant_end);
        let queries = generate_queries(&mut rng, &series, config);
        tenants.push(TenantWorkload {
            tenant,
            series,
            queries,
        });
    }

    Ok(Workload {
        tenants,
        start_ts_ns: config.start_ts_ns,
        end_ts_ns,
    })
}

fn base_label_set(base_idx: usize, labels_per_set: usize) -> Vec<Label> {
    (0..labels_per_set)
        .map(|j| Label {
            name: format!("label_{j:03}"),
            value: format!("v{base_idx}_{j}"),
        })
        .collect()
}

fn generate_tenant_series(
    rng: &mut StdRng,
    tenant: &TenantId,
    config: &WorkloadConfig,
) -> Result<(Vec<GeneratedSeries>, i64), TypeError> {
    let distinct_sets = config.cardinality.distinct_sets(config.series_per_tenant);
    let labels_per_set = config.cardinality.labels_per_set();
    let mut series = Vec::with_capacity(config.series_per_tenant);
    let mut end_ts_ns = config.start_ts_ns;

    for i in 0..config.series_per_tenant {
        let base_idx = i % distinct_sets.max(1);
        let is_histogram = rng.random_bool(config.histogram_fraction.clamp(0.0, 1.0));
        let metric_name = if is_histogram {
            format!("sim_histogram_{i:04}")
        } else {
            format!("sim_scalar_{i:04}")
        };

        let mut labels = base_label_set(base_idx, labels_per_set);
        labels.push(Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: metric_name.clone(),
        });
        labels.push(Label {
            name: "series_idx".to_string(),
            value: i.to_string(),
        });
        let labels = LabelSet::new(labels)?;
        let series_id = SeriesId::compute(tenant, &metric_name, &labels)?;

        let (samples, last_ts) = if is_histogram {
            let mut points = Vec::with_capacity(config.samples_per_series);
            let mut ts = config.start_ts_ns;
            for _ in 0..config.samples_per_series {
                points.push(HistogramSample {
                    ts_ns: ts,
                    value: gen_histogram_value(rng),
                });
                ts += config.interval_ns;
            }
            let last = ts - config.interval_ns;
            (SeriesSamples::Histogram(points), last)
        } else {
            let mut points = Vec::with_capacity(config.samples_per_series);
            let mut ts = config.start_ts_ns;
            let mut value = rng.random_range(-100.0..100.0);
            for _ in 0..config.samples_per_series {
                points.push(Sample { ts_ns: ts, value });
                value += rng.random_range(-5.0..5.0);
                ts += config.interval_ns;
            }
            let last = ts - config.interval_ns;
            (SeriesSamples::Scalar(points), last)
        };
        end_ts_ns = end_ts_ns.max(last_ts);

        series.push(GeneratedSeries {
            series_id,
            labels,
            metric_name,
            samples,
        });
    }

    Ok((series, end_ts_ns))
}

/// A small, fixed-shape native-histogram value (one four-bucket positive
/// span, no negative buckets): enough to exercise the ingest/query
/// histogram path without pulling in `ravel-bench`'s larger representative
/// shape.
fn gen_histogram_value(rng: &mut StdRng) -> HistogramValue {
    let positive_spans = vec![HistogramSpan {
        offset: 0,
        length: 4,
    }];
    let positive: Vec<u64> = (0..4).map(|_| rng.random_range(1..64)).collect();
    let count = positive.iter().sum::<u64>();
    HistogramValue {
        scale: 2,
        zero_threshold: 1e-9,
        sum: Some(rng.random_range(0.0..1000.0)),
        custom_values: None,
        positive_spans,
        negative_spans: vec![],
        counts: HistogramCounts::Int {
            zero_count: 0,
            count,
            positive,
            negative: vec![],
        },
        reset_hint: ResetHint::Unknown,
    }
}

fn generate_queries(
    rng: &mut StdRng,
    series: &[GeneratedSeries],
    config: &WorkloadConfig,
) -> Vec<QuerySpec> {
    if series.is_empty() || config.samples_per_series == 0 {
        return Vec::new();
    }
    let mut queries = Vec::with_capacity(config.queries_per_tenant);
    let last_ts_ms = (config.start_ts_ns
        + (config.samples_per_series as i64 - 1) * config.interval_ns)
        / 1_000_000;
    let start_ms = config.start_ts_ns / 1_000_000;

    for q in 0..config.queries_per_tenant {
        let idx = if series.len() == 1 {
            0
        } else {
            rng.random_range(0..series.len())
        };
        let s = &series[idx];
        let selector = match s.samples {
            SeriesSamples::Histogram(_) => format!("histogram_count({})", s.metric_name),
            SeriesSamples::Scalar(_) => s.metric_name.clone(),
        };
        if q % 2 == 0 {
            queries.push(QuerySpec::Instant {
                query: selector,
                t_ms: last_ts_ms,
            });
        } else {
            queries.push(QuerySpec::Range {
                query: selector,
                start_ms,
                end_ms: last_ts_ms,
                step_ms: (config.interval_ns / 1_000_000).max(1),
            });
        }
    }
    queries
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_workload() {
        let seed = MasterSeed::new(42);
        let cfg = WorkloadConfig::default();
        let a = generate(&seed, &cfg).expect("generate a");
        let b = generate(&seed, &cfg).expect("generate b");
        assert_eq!(a.tenants.len(), b.tenants.len());
        for (ta, tb) in a.tenants.iter().zip(b.tenants.iter()) {
            let ids_a: Vec<_> = ta.series.iter().map(|s| s.series_id).collect();
            let ids_b: Vec<_> = tb.series.iter().map(|s| s.series_id).collect();
            assert_eq!(ids_a, ids_b);
        }
    }

    #[test]
    fn different_seed_different_workload() {
        let cfg = WorkloadConfig::default();
        let a = generate(&MasterSeed::new(1), &cfg).expect("generate a");
        let b = generate(&MasterSeed::new(2), &cfg).expect("generate b");
        let ids_a: Vec<_> = a.tenants[0].series.iter().map(|s| s.series_id).collect();
        let ids_b: Vec<_> = b.tenants[0].series.iter().map(|s| s.series_id).collect();
        assert_ne!(ids_a, ids_b);
    }
}
