//! Allocation accounting for the OTLP metrics normalize path (#367).
//!
//! This benchmark MEASURES a claim; it changes nothing. The claim, read from
//! `normalize.rs` and not yet verified, is that `build_point` allocates on the
//! order of `2 * (labels per point) + 4` times per datapoint, and that the
//! per-metric `SeriesIdMemo` is at best neutral and at worst net-negative.
//! Every number here is produced so that claim can be confirmed or refuted.
//!
//! What is measured, and what is deliberately kept out of the measured region
//! (the earlier profiling task in this epic reported numbers dominated by its
//! own harness and had to withdraw them; this does not repeat that):
//!
//! - The `ExportMetricsServiceRequest` fixture is built ONCE per configuration,
//!   entirely outside the `stats_alloc::Region`. `normalize_metrics` consumes
//!   its request by value, so the measured call receives an owned request that
//!   was constructed before the region opened; no fixture clone, no `Vec`
//!   growth for collecting results, and no formatting happens inside the region.
//! - `SeriesId::compute` keeps a thread-local scratch buffer that grows on
//!   first use. A warmup `normalize_metrics` call runs on the same thread
//!   before every measured region so that one-time growth is not counted as a
//!   per-point cost.
//! - The only allocations left inside the region are `normalize_metrics`'s own:
//!   per-point label building plus the output point `Vec`. The output `Vec`
//!   grows O(log points) times and washes out of the per-point figure.
//!
//! Reachability: this is measurement infrastructure. Its only callers are
//! `cargo bench` and a human reading the output; there is no production caller.
//!
//! The memo hit-rate half (deliverable 3) needs the `memo-stats` cargo feature:
//!
//! ```sh
//! cargo bench -p ravel-otlp --features memo-stats --bench normalize_alloc
//! ```
//!
//! Without the feature the bench still builds and runs (so it compiles under
//! `--all-targets`); it just prints a note where the hit rate would be.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data as MetricData,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use ravel_otlp::{IngestLimits, normalize_metrics};
use ravel_types::TenantId;
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const INGEST_TS_NS: i64 = 1_700_000_000_000_000_000;
/// Metrics per request in the sweep. Allocation counts are exact and
/// deterministic, so this only needs to be large enough that the per-request
/// fixed cost (the output `Vec`) is negligible against the per-point figure.
const METRICS: usize = 8;

/// How the datapoints within one metric map to series.
#[derive(Clone, Copy, PartialEq)]
enum Shape {
    /// Every datapoint of a metric carries an identical attribute set: one
    /// series per metric. The memo hits on every point after the first.
    Grouped,
    /// Each datapoint of a metric carries a distinct attribute value, so no two
    /// consecutive points share a series. The one-entry memo never hits.
    Interleaved,
}

#[derive(Clone, Copy)]
struct Cfg {
    /// Resource labels per resource (`R`); paid per POINT if the claim holds.
    resource_labels: usize,
    /// Attributes per datapoint (`A`).
    attrs_per_point: usize,
    /// Datapoints per metric; the term that separates per-resource from
    /// per-point costs.
    points_per_metric: usize,
    /// When true, attribute names carry a `.` that `sanitize_label_name` must
    /// rewrite; when false, names are already valid label names.
    needs_sanitise: bool,
    shape: Shape,
}

impl Cfg {
    /// `L` in the `2L + 4` model: labels contributed per point excluding the
    /// synthesized `__name__`.
    fn labels_per_point(&self) -> usize {
        self.resource_labels + self.attrs_per_point
    }
}

/// One attribute key. A dirty key holds a `.`, which sanitisation rewrites to
/// `_`; a clean key is already a valid label name (sanitisation still runs and
/// still allocates, which is the point under test).
fn attr_key(i: usize, needs_sanitise: bool) -> String {
    if needs_sanitise {
        format!("attr.{i}")
    } else {
        format!("attr_{i}")
    }
}

fn string_kv(key: String, value: String) -> KeyValue {
    KeyValue {
        key,
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value)),
        }),
        ..Default::default()
    }
}

/// Limits whose allowlist admits exactly `resource_labels` resource attributes,
/// so each configured resource attribute becomes one resource label.
fn limits_for(cfg: &Cfg) -> IngestLimits {
    IngestLimits {
        resource_attribute_allowlist: (0..cfg.resource_labels)
            .map(|i| format!("res_{i}"))
            .collect(),
        ..IngestLimits::default()
    }
}

/// The attribute set for point `idx` of a metric under `cfg`.
fn attrs_for_point(cfg: &Cfg, idx: usize) -> Vec<KeyValue> {
    (0..cfg.attrs_per_point)
        .map(|a| {
            let key = attr_key(a, cfg.needs_sanitise);
            let value = match cfg.shape {
                // Vary attribute 0 by point index so every point is its own
                // series; the rest are constant.
                Shape::Interleaved if a == 0 => format!("v{idx}"),
                _ => format!("v{a}"),
            };
            string_kv(key, value)
        })
        .collect()
}

/// Build the request fixture for `cfg`, entirely outside any measured region.
/// Returns the request and its total datapoint count.
fn build_request(cfg: &Cfg) -> (ExportMetricsServiceRequest, usize) {
    let resource_attrs: Vec<KeyValue> = (0..cfg.resource_labels)
        .map(|i| string_kv(format!("res_{i}"), format!("rv{i}")))
        .collect();

    let metrics: Vec<Metric> = (0..METRICS)
        .map(|m| {
            let data_points: Vec<NumberDataPoint> = (0..cfg.points_per_metric)
                .map(|p| NumberDataPoint {
                    attributes: attrs_for_point(cfg, p),
                    time_unix_nano: (INGEST_TS_NS + p as i64 * 1_000_000) as u64,
                    value: Some(NumberValue::AsDouble(p as f64 * 0.5)),
                    ..Default::default()
                })
                .collect();
            Metric {
                name: format!("metric_{m}"),
                data: Some(MetricData::Gauge(Gauge { data_points })),
                ..Default::default()
            }
        })
        .collect();

    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: resource_attrs,
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let total_points = METRICS * cfg.points_per_metric;
    (req, total_points)
}

/// Allocations and bytes for one untimed `normalize_metrics` call on a freshly
/// built (outside the region) fixture, after a same-thread warmup that grows
/// `SeriesId::compute`'s thread-local scratch.
fn measure_alloc(cfg: &Cfg, tenant: &TenantId) -> (usize, usize, usize) {
    let limits = limits_for(cfg);

    // Warmup on a separate fixture, same thread: grows the compute scratch so
    // its one-time growth is not charged to the measured region.
    let (warm, _) = build_request(cfg);
    let warm_out = normalize_metrics(tenant, warm, &limits, INGEST_TS_NS);
    std::hint::black_box(&warm_out);
    drop(warm_out);

    let (req, total_points) = build_request(cfg);
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let out = normalize_metrics(tenant, req, &limits, INGEST_TS_NS);
    let change = region.change();
    assert_eq!(out.points.len(), total_points, "unexpected point count");
    (total_points, change.allocations, change.bytes_allocated)
}

fn sweep_configs() -> Vec<(String, Cfg)> {
    let resource_labels = [0usize, 5, 15];
    let attrs = [0usize, 5, 15];
    let points = [1usize, 100];
    let mut out = Vec::new();
    for &r in &resource_labels {
        for &a in &attrs {
            for &pmm in &points {
                for &sanitise in &[false, true] {
                    // The sanitise variant only differs when there are
                    // attributes to sanitise.
                    if sanitise && a == 0 {
                        continue;
                    }
                    let tag = if sanitise { "dirty" } else { "clean" };
                    let label = format!("R{r}_A{a}_P{pmm}_{tag}");
                    out.push((
                        label,
                        Cfg {
                            resource_labels: r,
                            attrs_per_point: a,
                            points_per_metric: pmm,
                            needs_sanitise: sanitise,
                            shape: Shape::Grouped,
                        },
                    ));
                }
            }
        }
    }
    out
}

fn report_sweep(tenant: &TenantId) {
    eprintln!("\n=== #367 OTLP normalize allocation sweep (grouped: 1 series/metric) ===");
    eprintln!(
        "{:<22} {:>8} {:>10} {:>12} {:>10} {:>10}",
        "config", "points", "allocs/pt", "bytes/pt", "2L+4", "measured-2L+4"
    );
    for (label, cfg) in sweep_configs() {
        let (points, allocs, bytes) = measure_alloc(&cfg, tenant);
        let per_pt = allocs as f64 / points as f64;
        let bytes_pt = bytes as f64 / points as f64;
        let predicted = 2.0 * cfg.labels_per_point() as f64 + 4.0;
        eprintln!(
            "{label:<22} {points:>8} {per_pt:>10.2} {bytes_pt:>12.1} {predicted:>10.1} {:>+10.2}",
            per_pt - predicted
        );
    }
}

fn report_memo(tenant: &TenantId) {
    eprintln!("\n=== #367 SeriesIdMemo hit rate and per-point cost ===");
    // Two shapes at 100 points/metric, R=5, A=5, clean names, so only the
    // series layout differs.
    let base = Cfg {
        resource_labels: 5,
        attrs_per_point: 5,
        points_per_metric: 100,
        needs_sanitise: false,
        shape: Shape::Grouped,
    };
    let grouped = base;
    let interleaved = Cfg {
        shape: Shape::Interleaved,
        ..base
    };

    let (g_points, g_allocs, _) = measure_alloc(&grouped, tenant);
    let (i_points, i_allocs, _) = measure_alloc(&interleaved, tenant);
    let g_per = g_allocs as f64 / g_points as f64;
    let i_per = i_allocs as f64 / i_points as f64;

    eprintln!("R=5 A=5, 100 points/metric, {METRICS} metrics:");
    eprintln!("  grouped     allocs/pt = {g_per:.2}");
    eprintln!("  interleaved allocs/pt = {i_per:.2}");
    eprintln!(
        "  interleaved - grouped = {:+.2} allocs/pt (the memo's store-clone paid on every miss)",
        i_per - g_per
    );

    report_memo_stats(tenant, &grouped, "grouped");
    report_memo_stats(tenant, &interleaved, "interleaved");
}

#[cfg(feature = "memo-stats")]
fn report_memo_stats(tenant: &TenantId, cfg: &Cfg, name: &str) {
    use ravel_otlp::normalize::memo_stats;
    let limits = limits_for(cfg);
    let (req, _) = build_request(cfg);
    memo_stats::reset();
    let out = normalize_metrics(tenant, req, &limits, INGEST_TS_NS);
    std::hint::black_box(&out);
    let (hits, misses) = memo_stats::snapshot();
    let total = hits + misses;
    let rate = if total == 0 {
        0.0
    } else {
        hits as f64 / total as f64 * 100.0
    };
    eprintln!("  {name:<12} hits={hits} misses={misses} hit_rate={rate:.1}%");
}

#[cfg(not(feature = "memo-stats"))]
fn report_memo_stats(_tenant: &TenantId, _cfg: &Cfg, name: &str) {
    eprintln!("  {name:<12} hit rate: rerun with --features memo-stats");
}

fn bench_normalize(c: &mut Criterion) {
    let tenant = TenantId::new("acme");

    // Allocation reporting (the deliverable), printed outside criterion's
    // measured region so its bookkeeping never enters the counts.
    report_sweep(&tenant);
    report_memo(&tenant);

    // A representative timing slice. iter_batched keeps the fixture clone in
    // the untimed setup closure, so only normalize_metrics is timed.
    let timing: [(&str, Cfg); 3] = [
        (
            "R0_A5_P100",
            Cfg {
                resource_labels: 0,
                attrs_per_point: 5,
                points_per_metric: 100,
                needs_sanitise: false,
                shape: Shape::Grouped,
            },
        ),
        (
            "R15_A5_P100",
            Cfg {
                resource_labels: 15,
                attrs_per_point: 5,
                points_per_metric: 100,
                needs_sanitise: false,
                shape: Shape::Grouped,
            },
        ),
        (
            "R5_A5_P100_interleaved",
            Cfg {
                resource_labels: 5,
                attrs_per_point: 5,
                points_per_metric: 100,
                needs_sanitise: false,
                shape: Shape::Interleaved,
            },
        ),
    ];

    let mut group = c.benchmark_group("normalize");
    for (label, cfg) in timing {
        let limits = limits_for(&cfg);
        let (req, total_points) = build_request(&cfg);
        group.throughput(Throughput::Elements(total_points as u64));
        group.bench_with_input(BenchmarkId::from_parameter(label), &req, |b, req| {
            b.iter_batched(
                || req.clone(),
                |owned| {
                    let out = normalize_metrics(&tenant, owned, &limits, INGEST_TS_NS);
                    std::hint::black_box(out.points.len())
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_normalize);
criterion_main!(benches);
