//! Allocation scaling acceptance test for the OTLP metrics normalize path
//! (#367, ADR-0098).
//!
//! This file contains EXACTLY ONE test on purpose. The measurement wraps a
//! `stats_alloc::Region` around the global allocator, so any other thread
//! allocating while the region is open lands in the count; `cargo test` and
//! nextest both run test functions in one binary concurrently, so a second
//! test here would make the figures non-deterministic. This mirrors the
//! established pattern in `ravel-sql/tests/scan_batch_allocations.rs`.
//!
//! The task (#367) named this acceptance test
//! `ravel_otlp::normalize::tests::normalize_allocations_scale_per_scope_not_per_point`,
//! i.e. a unit test in `src/normalize.rs`'s shared `mod tests`. It lives here
//! as a single-test integration binary instead, for the reason above: a unit
//! test would share the lib test binary with hundreds of other unit tests, and
//! a global-`Region` measurement under concurrent execution is exactly what
//! that neighbouring ravel-sql file documents as unusable. The test name is
//! preserved; only the module path differs.
//!
//! What it pins (ADR-0098 decision 6): with the realistic OTLP shape (one
//! series sampled over time, i.e. many points sharing one attribute set) the
//! per-point resource-label clone and `__name__`/metric-name allocation are
//! hoisted per scope by the `SeriesIdMemo`. So the allocation count must grow
//! with the number of resource/metric SCOPES and stay FLAT in the number of
//! points per scope. The assertions bound a per-point magnitude near zero and
//! a per-scope magnitude near the full label-set build cost, so a regression to
//! per-point rebuilding (the pre-ADR-0098 behaviour, ~`2R + A + 3` allocations
//! per point) blows through the per-point bound by an order of magnitude.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;

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

/// Resource labels admitted per resource (`R`). Cloned into every point's
/// label set on the pre-ADR-0098 path; hoisted per scope with the memo.
const R: usize = 5;
/// Attributes per datapoint (`A`).
const A: usize = 5;

fn string_kv(key: String, value: String) -> KeyValue {
    KeyValue {
        key,
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value)),
        }),
        ..Default::default()
    }
}

/// Limits whose allowlist admits exactly `R` resource attributes.
fn limits() -> IngestLimits {
    IngestLimits {
        resource_attribute_allowlist: (0..R).map(|i| format!("res.{i}")).collect(),
        ..IngestLimits::default()
    }
}

/// One request with `scopes` gauge metrics under a single resource, each metric
/// carrying `points_per_scope` datapoints that all share ONE attribute set.
/// Grouped shape: one series per metric, so `scopes` = distinct series =
/// resource/metric scopes, and every point after the first in a metric is a
/// memo hit. Attribute names carry a `.` so `sanitize_label_name` runs (the
/// clean path still allocates its value string, which is the point under test).
fn build_request(scopes: usize, points_per_scope: usize) -> (ExportMetricsServiceRequest, usize) {
    let resource_attrs: Vec<KeyValue> = (0..R)
        .map(|i| string_kv(format!("res.{i}"), format!("rv{i}")))
        .collect();

    let attrs: Vec<KeyValue> = (0..A)
        .map(|a| string_kv(format!("attr.{a}"), format!("v{a}")))
        .collect();

    let metrics: Vec<Metric> = (0..scopes)
        .map(|m| {
            let data_points: Vec<NumberDataPoint> = (0..points_per_scope)
                .map(|p| NumberDataPoint {
                    attributes: attrs.clone(),
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
    (req, scopes * points_per_scope)
}

/// Allocations for one untimed `normalize_metrics` call, after a same-thread
/// warmup on a freshly built (outside the region) fixture. The warmup grows
/// `SeriesId::compute`'s thread-local scratch so its one-time growth is not
/// charged to the measured region; the fixture the region measures is built
/// before the region opens, and `normalize_metrics` consumes it by value.
fn allocations(tenant: &TenantId, scopes: usize, points_per_scope: usize) -> usize {
    let limits = limits();

    let (warm, _) = build_request(scopes, points_per_scope);
    let warm_out = normalize_metrics(tenant, warm, &limits, INGEST_TS_NS);
    std::hint::black_box(&warm_out);
    drop(warm_out);

    let (req, total_points) = build_request(scopes, points_per_scope);
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let out = normalize_metrics(tenant, req, &limits, INGEST_TS_NS);
    let change = region.change();
    assert_eq!(out.points.len(), total_points, "unexpected point count");
    assert!(
        out.rejected.is_empty(),
        "unexpected rejections: {:?}",
        out.rejected
    );
    change.allocations
}

#[test]
fn normalize_allocations_scale_per_scope_not_per_point() {
    let tenant = TenantId::new("acme");

    // Same scope count, few vs many points: isolates the per-point term.
    let s = 8usize;
    let few = allocations(&tenant, s, 1);
    let many = allocations(&tenant, s, 100);

    // Double the scopes at one point each: isolates the per-scope term.
    let s2 = 16usize;
    let few_2s = allocations(&tenant, s2, 1);

    let extra_points = s * (100 - 1);
    let per_point = (many.saturating_sub(few)) as f64 / extra_points as f64;
    let per_scope = (few_2s.saturating_sub(few)) as f64 / (s2 - s) as f64;

    eprintln!(
        "normalize_allocations: few(S={s},P=1)={few} many(S={s},P=100)={many} \
         few(S={s2},P=1)={few_2s} => per_point={per_point:.4} per_scope={per_scope:.2}"
    );

    // Flat in points. A memo hit is one `Arc::clone` (a refcount bump, no heap
    // allocation), so every point after the first in a scope adds ~0
    // allocations. Measured 0.0000 (few == many == 321 at 8 scopes). The bound
    // is 1.0 rather than exact 0.0 to leave headroom for the output `Vec`'s
    // O(log points) amortized growth without going brittle. A regression to
    // per-point label rebuilding (pre-ADR-0098) costs ~`2R + A + 3` = 18
    // allocations per point here, an order of magnitude over this bound.
    assert!(
        per_point < 1.0,
        "allocations must be FLAT in points per scope: measured {per_point:.4} \
         allocations per extra point ({few} at P=1, {many} at P=100 over {extra_points} \
         extra points at {s} scopes). A per-point rebuild regression costs ~2R+A+3 \
         allocations per point.",
    );

    // Grows with scopes. Each new scope is a new series whose first (and only,
    // at P=1) point is a memo miss that builds one full `LabelSet`: the
    // `Vec<Label>`, two strings per resource label, two for `__name__`, one per
    // attribute value, plus the memo's attribute-key clone. Measured 38.00
    // allocations per scope; the floor is 20 with headroom.
    assert!(
        per_scope >= 20.0,
        "allocations must GROW with scope count: measured {per_scope:.2} allocations \
         per scope ({few} at {s} scopes, {few_2s} at {s2} scopes). A scope that paid \
         nothing would mean the per-scope build was itself elided.",
    );

    // The scaling is proportional to scope count, not a fixed cost: doubling the
    // scopes adds at least `s * 20` allocations.
    assert!(
        few_2s >= few + s * 20,
        "total allocations must grow proportionally to scope count: {few} at {s} \
         scopes but only {few_2s} at {s2} scopes (expected at least {}).",
        few + s * 20,
    );
}
