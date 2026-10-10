//! An OTLP request the ingest byte budget sheds on its projected
//! resolved-label bytes is shed before normalization allocates those labels
//! (ADR-2708 D2, issue #2710), for metrics, logs and traces.
//!
//! This file contains EXACTLY ONE test on purpose: the measurement wraps a
//! `stats_alloc::Region` around the global allocator, so another test running
//! concurrently in this binary would land in the count. The same pattern is in
//! `crates/ravel-otlp/tests/normalize_allocations.rs`.
//!
//! A shed caused by a charge taken after normalization returns the same 429
//! and leaves every counter where a shed before it does, so neither the
//! response nor the metrics can tell the two apart. The allocator can: the
//! normalizer builds roughly the projected bytes, and the shed path builds
//! almost nothing.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::alloc::System;
use std::sync::Arc;
use std::time::Duration;

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data as MetricData,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use ravel_ingest::{
    AdmissionController, AdmissionLimits, IngestByteBudget, IngestByteBudgetLimit, IngestConfig,
    IngestRouter, LogIngestRouter, LogWriteError, SpanIngestRouter, SpanWriteError, SystemClock,
    WriteError, WriteMode,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::{IngestLimits, LogIngestLimits, SpanIngestLimits};
use ravel_server::ingest::{IngestRequestError, IngestState, handle_export};
use ravel_server::logs_ingest::{LogIngestRequestError, LogIngestState, handle_export_logs};
use ravel_server::normalize_reject_metrics::NormalizeRejectMetrics;
use ravel_server::traces_ingest::{SpanIngestRequestError, SpanIngestState, handle_export_traces};
use ravel_types::{Signal, TenantId};
use stats_alloc::{INSTRUMENTED_SYSTEM, Region, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<System> = &INSTRUMENTED_SYSTEM;

/// 2026-01-01T00:00:00Z: a fixed, post-floor ingest clock.
const INGEST_TS_NS: i64 = 1_767_225_600_000_000_000;

/// Units per request, each carrying its own copy of a 1 KiB value: about
/// 2 MB of labels or attributes per request.
const UNITS: usize = 2_000;
const VALUE_LEN: usize = 1_024;

fn string_kv(key: &str, value: String) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value)),
        }),
        ..Default::default()
    }
}

/// A resource whose one attribute every log record or span copies.
fn wide_resource() -> Resource {
    Resource {
        attributes: vec![string_kv("host", "h".repeat(VALUE_LEN))],
        ..Default::default()
    }
}

/// Gauge points with distinct 1 KiB attribute values, so the label memo
/// shares nothing and the normalizer builds one label set per point.
fn wide_gauge_request() -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "temperature".to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: (0..UNITS)
                            .map(|i| {
                                let id = format!("{i:06}");
                                NumberDataPoint {
                                    attributes: vec![string_kv(
                                        "room",
                                        format!("{id}{}", "r".repeat(VALUE_LEN - id.len())),
                                    )],
                                    time_unix_nano: INGEST_TS_NS as u64,
                                    value: Some(NumberValue::AsDouble(i as f64)),
                                    ..Default::default()
                                }
                            })
                            .collect(),
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn wide_stream_log_request() -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(wide_resource()),
            scope_logs: vec![ScopeLogs {
                log_records: (0..UNITS)
                    .map(|i| LogRecord {
                        time_unix_nano: INGEST_TS_NS as u64 + i as u64,
                        observed_time_unix_nano: INGEST_TS_NS as u64,
                        severity_number: 9,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn wide_resource_span_request() -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(wide_resource()),
            scope_spans: vec![ScopeSpans {
                spans: (0..UNITS)
                    .map(|i| Span {
                        trace_id: [0xa1; 16].to_vec(),
                        span_id: (i as u64 + 1).to_be_bytes().to_vec(),
                        name: "op".to_string(),
                        start_time_unix_nano: INGEST_TS_NS as u64 - 1_000,
                        end_time_unix_nano: INGEST_TS_NS as u64,
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn config() -> IngestConfig {
    IngestConfig {
        shard_count: 1,
        ..IngestConfig::default()
    }
}

fn admission() -> Arc<AdmissionController> {
    Arc::new(AdmissionController::new(
        Arc::new(SystemClock),
        AdmissionLimits::default(),
    ))
}

fn metrics_state(budget: Arc<IngestByteBudget>) -> IngestState {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    IngestState {
        router: Arc::new(IngestRouter::new(
            config(),
            store,
            Signal::Metrics,
            Arc::new(SystemClock),
        )),
        limits: IngestLimits::default(),
        ack_deadline: Duration::from_secs(5),
        admission: admission(),
        recovery: None,
        provisioning: None,
        metadata_sink: None,
        normalize_metrics: Arc::new(NormalizeRejectMetrics::new()),
        budget,
    }
}

fn logs_state(budget: Arc<IngestByteBudget>) -> LogIngestState {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    LogIngestState {
        router: Arc::new(LogIngestRouter::new(
            config(),
            store.clone(),
            Arc::new(SystemClock),
        )),
        limits: LogIngestLimits::default(),
        ack_deadline: Duration::from_secs(5),
        admission: admission(),
        store,
        recovery: None,
        provisioning: None,
        normalize_metrics: Arc::new(NormalizeRejectMetrics::new()),
        budget,
    }
}

fn spans_state(budget: Arc<IngestByteBudget>) -> SpanIngestState {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    SpanIngestState {
        router: Arc::new(SpanIngestRouter::new(
            config(),
            store.clone(),
            Arc::new(SystemClock),
        )),
        limits: SpanIngestLimits::default(),
        ack_deadline: Duration::from_secs(5),
        admission: admission(),
        store,
        recovery: None,
        provisioning: None,
        normalize_metrics: Arc::new(NormalizeRejectMetrics::new()),
        budget,
    }
}

/// A budget one byte short of `projected`.
fn short_budget(projected: usize) -> Arc<IngestByteBudget> {
    IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(projected as u64 - 1))
}

/// Normalization alone allocates at least half the projection, and the shed
/// path less than a sixteenth of it, so the two cannot be confused.
fn assert_separated(signal: &str, projected: usize, normalized: usize, shed: usize) {
    assert!(
        projected > UNITS * VALUE_LEN,
        "{signal}: every unit projects its own copy: {projected}"
    );
    assert!(
        normalized >= projected / 2,
        "{signal}: normalization allocated {normalized} of a {projected}-byte projection"
    );
    assert!(
        shed < projected / 16,
        "{signal}: the shed path allocated {shed} bytes, against {normalized} for \
         normalization and a {projected}-byte projection"
    );
}

#[tokio::test]
async fn a_shed_projection_allocates_nothing_near_the_label_bytes() {
    let tenant = TenantId::new("acme");

    // Metrics.
    let limits = IngestLimits::default();
    let projected = ravel_otlp::project_resolved_label_bytes(&wide_gauge_request(), &limits);
    let request = wide_gauge_request();
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let output = ravel_otlp::normalize_metrics(&tenant, request, &limits, INGEST_TS_NS);
    let normalized = region.change().bytes_allocated;
    assert_eq!(output.points.len(), UNITS);
    drop(output);
    let budget = short_budget(projected);
    let state = metrics_state(Arc::clone(&budget));
    let request = wide_gauge_request();
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let result = handle_export(
        &state,
        tenant.clone(),
        WriteMode::Buffered,
        request,
        INGEST_TS_NS,
    )
    .await;
    let shed = region.change().bytes_allocated;
    assert!(matches!(
        result,
        Err(IngestRequestError::Write(WriteError::BufferBudgetExceeded))
    ));
    assert_eq!(budget.shed_total(), 1);
    assert_separated("metrics", projected, normalized, shed);

    // Logs.
    let limits = LogIngestLimits::default();
    let projected =
        ravel_otlp::project_log_resolved_label_bytes(&wide_stream_log_request(), &limits);
    let request = wide_stream_log_request();
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let output = ravel_otlp::normalize_logs(request, &limits, INGEST_TS_NS);
    let normalized = region.change().bytes_allocated;
    assert_eq!(output.records.len(), UNITS);
    drop(output);
    let budget = short_budget(projected);
    let state = logs_state(Arc::clone(&budget));
    let request = wide_stream_log_request();
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let result = handle_export_logs(
        &state,
        tenant.clone(),
        WriteMode::Buffered,
        request,
        INGEST_TS_NS,
        None,
    )
    .await;
    let shed = region.change().bytes_allocated;
    assert!(matches!(
        result,
        Err(LogIngestRequestError::Write(
            LogWriteError::BufferBudgetExceeded
        ))
    ));
    assert_eq!(budget.shed_total(), 1);
    assert_separated("logs", projected, normalized, shed);

    // Traces.
    let limits = SpanIngestLimits::default();
    let projected =
        ravel_otlp::project_span_resolved_label_bytes(&wide_resource_span_request(), &limits);
    let request = wide_resource_span_request();
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let output = ravel_otlp::normalize_traces(request, &limits, INGEST_TS_NS);
    let normalized = region.change().bytes_allocated;
    assert_eq!(output.spans.len(), UNITS);
    drop(output);
    let budget = short_budget(projected);
    let state = spans_state(Arc::clone(&budget));
    let request = wide_resource_span_request();
    let region = Region::new(&INSTRUMENTED_SYSTEM);
    let result = handle_export_traces(
        &state,
        tenant,
        WriteMode::Buffered,
        request,
        INGEST_TS_NS,
        None,
    )
    .await;
    let shed = region.change().bytes_allocated;
    assert!(matches!(
        result,
        Err(SpanIngestRequestError::Write(
            SpanWriteError::BufferBudgetExceeded
        ))
    ));
    assert_eq!(budget.shed_total(), 1);
    assert_separated("traces", projected, normalized, shed);
}
