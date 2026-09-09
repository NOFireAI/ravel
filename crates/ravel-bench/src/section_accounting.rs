//! Shared workload shapers for the RSEG v5 byte gates
//! (docs/segment-format.md; ADR-0027 retired the cross-version v1-vs-columnar
//! comparison this module used to host, so only the deterministic workload
//! generators remain).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::generator::{CardinalityProfile, WorkloadConfig, generate_raw};
use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

/// Raw `(SeriesId, LabelSet, Vec<Sample>)` workload consumed by the segment
/// builders in [`crate::segment_support`].
pub type Raw = Vec<(SeriesId, LabelSet, Vec<Sample>)>;

/// The shared generator's `many_small` shape (5 label pairs/series),
/// identical config to the `segment_encode` bench. `total_samples` is spread
/// evenly across `series` (at least one sample each). Seed is the generator
/// default (0), so the workload is fixed.
pub fn shape_many_small(series: usize, total_samples: usize) -> Raw {
    let config = WorkloadConfig {
        series_count: series,
        samples_per_series: (total_samples / series.max(1)).max(1),
        cardinality: CardinalityProfile::many_small(series),
        ..Default::default()
    };
    generate_raw(&config).expect("generate")
}

/// A 15-labels-per-series shape built inline (not via the shared
/// generator). Every series carries the same 15 label names
/// (`label_000..label_014`) with per-series-unique values plus a unique
/// `series_idx`, so all series collapse to a single columnar schema: this is
/// the production-shaped case (plan sec 1.2/3.4: 10-20 labels/series) where
/// the schema dictionary earns the most. Fully deterministic (no RNG).
pub fn shape_wide_15(series: usize, total_samples: usize) -> Raw {
    let tenant = TenantId::new("bench-tenant");
    let start_ts_ns: i64 = 1_700_000_000_000_000_000;
    let interval_ns: i64 = 1_000_000_000;
    let samples_per_series = (total_samples / series.max(1)).max(1);
    let mut out: Raw = Vec::with_capacity(series);
    for i in 0..series {
        let mut labels: Vec<Label> = (0..15)
            .map(|j| Label {
                name: format!("label_{j:03}"),
                value: format!("v{i}_{j}"),
            })
            .collect();
        labels.push(Label {
            name: "series_idx".to_string(),
            value: i.to_string(),
        });
        let labels = LabelSet::new(labels).expect("labels");
        let series_id = SeriesId::compute(&tenant, "bench_gauge", &labels).expect("compute");
        let mut ts = start_ts_ns;
        let mut samples = Vec::with_capacity(samples_per_series);
        for k in 0..samples_per_series {
            ts += interval_ns;
            samples.push(Sample {
                ts_ns: ts,
                value: i as f64 + k as f64 * 0.5,
            });
        }
        out.push((series_id, labels, samples));
    }
    out
}
