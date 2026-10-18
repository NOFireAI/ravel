//! The resolved-label bound (ADR-2708 D2) rejects an OTAP batch before any
//! resource label exists.
//!
//! This file contains EXACTLY ONE test: the measurement wraps a
//! `stats_alloc::Region` around the global allocator, and a second test running
//! concurrently in this binary would land in the count.
//!
//! The batch is attack-shaped, scaled down: many resources whose attribute
//! values all point at one dictionary string, so the wire carries the string
//! once while every built prefix would copy it. Two batches differ only in that
//! string's length. A path that builds a resource label before the bound
//! rejects allocates in proportion to `RESOURCES x VALUE_LEN`; one that counts
//! lengths allocates the same bytes for both.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;

use ravel_ipcrypt::rules::TenantRules;
use ravel_otap::encode::{
    AttrRow, AttrValue, DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder,
    ResourceAssignment,
};
use ravel_otap::normalize::{
    normalize_decoded, normalize_prepared_with_metadata, prepare_batch,
    project_resolved_label_bytes,
};
use ravel_otap::stream::{DecodedBatch, StreamConfig, StreamState};
use ravel_otlp::{IngestLimits, Rejection};
use ravel_types::{ExemplarCap, TenantId};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

const INGEST_TS_NS: i64 = 1_700_000_000_000_000_000;
const RESOURCES: usize = 256;
const SHORT: usize = 8;
const LONG: usize = 2048;

fn limits() -> IngestLimits {
    IngestLimits {
        resource_attribute_allowlist: vec!["host.name".to_string()],
        max_resolved_label_bytes_per_request: 1024,
        ..IngestLimits::default()
    }
}

/// `RESOURCES` resources, each with one gauge point, whose `service.name`,
/// `service.instance.id` and `host.name` all carry one shared string of
/// `value_len` bytes.
fn decoded(value_len: usize) -> DecodedBatch {
    let value = "v".repeat(value_len);
    let resources: Vec<Vec<AttrRow>> = (0..RESOURCES)
        .map(|_| {
            ["service.name", "service.instance.id", "host.name"]
                .iter()
                .map(|key| AttrRow {
                    key: key.to_string(),
                    value: AttrValue::Str(value.clone()),
                })
                .collect()
        })
        .collect();
    let metrics: Vec<MetricRow> = (0..RESOURCES)
        .map(|_| MetricRow {
            name: "up".to_string(),
            kind: MetricKind::Gauge,
            data_points: vec![DataPointRow {
                time_unix_nano: INGEST_TS_NS,
                value: 1.0,
                flags: 0,
                exemplars: vec![],
                attrs: vec![],
            }],
        })
        .collect();
    let units = vec![""; RESOURCES];
    let slots: Vec<Option<usize>> = (0..RESOURCES).map(Some).collect();
    let mut encoder = MetricsStreamEncoder::new("label-bound-allocations").expect("encoder");
    let raw = encoder
        .encode_batch_resources(
            0,
            &metrics,
            &units,
            &[],
            &[],
            &[],
            &[],
            &ResourceAssignment {
                resources: &resources,
                metrics: &slots,
                histograms: &[],
                summaries: &[],
            },
        )
        .expect("encode");
    StreamState::new(StreamConfig::default())
        .decode(raw)
        .expect("decode")
}

fn assert_rejected_whole(rejected: &[Rejection]) {
    assert!(
        matches!(
            rejected,
            [Rejection::ResolvedLabelBytesExceeded { count, .. }] if *count == RESOURCES
        ),
        "{rejected:?}"
    );
}

/// Bytes allocated by each step for a batch whose shared value is
/// `value_len` long: the standalone projection, the whole normalize, and the
/// gateway's prepare-then-normalize.
fn measure(value_len: usize) -> [usize; 3] {
    let tenant = TenantId::new("acme");
    let limits = limits();
    let batch = decoded(value_len);

    let region = Region::new(GLOBAL);
    let projected = project_resolved_label_bytes(&batch, &limits, &TenantRules::none());
    let project = region.change().bytes_allocated;
    assert!(projected.get() > limits.max_resolved_label_bytes_per_request);

    let region = Region::new(GLOBAL);
    let out = normalize_decoded(&tenant, &batch, &limits, &TenantRules::none(), INGEST_TS_NS);
    let normalize = region.change().bytes_allocated;
    assert_rejected_whole(&out.rejected);
    drop(out);

    let mut cap = ExemplarCap::new(limits.exemplar_cap_window_ns);
    let region = Region::new(GLOBAL);
    let prepared = prepare_batch(&batch, &limits, &TenantRules::none());
    assert!(prepared.projected_label_bytes().get() > limits.max_resolved_label_bytes_per_request);
    let (result, _) = normalize_prepared_with_metadata(
        &tenant,
        prepared,
        &limits,
        &TenantRules::none(),
        INGEST_TS_NS,
        &mut cap,
    );
    let prepare = region.change().bytes_allocated;
    assert_rejected_whole(&result.output.rejected);

    [project, normalize, prepare]
}

#[test]
fn resource_labels_are_bounded_before_any_is_built() {
    let short = measure(SHORT);
    let long = measure(LONG);
    println!("bytes allocated [project, normalize, prepare]: short={short:?} long={long:?}");
    assert_eq!(
        short, long,
        "allocation grew with the resource value length: labels were built before the bound"
    );
}
