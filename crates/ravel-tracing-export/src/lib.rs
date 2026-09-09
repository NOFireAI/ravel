//! Subscriber construction shared by `ravel-server` and `ravel-operator`
//! (ADR-0060). One entry point, [`init`], is the single subscriber
//! construction each binary calls instead of an inline
//! `tracing_subscriber::fmt()...init()`.
//!
//! With `otlp: None` it installs exactly today's bare `fmt` subscriber: same
//! output, same behavior, no OTLP object constructed and no export attempted
//! (ADR-0060 decision 1). With `otlp: Some(_)` it installs a `Registry` gated
//! by one `EnvFilter` with two layers under it, the same `fmt` layer plus a
//! `tracing-opentelemetry` layer wired to a `BatchSpanProcessor` over an
//! OTLP/gRPC exporter (ADR-0060 decision 2). Export is best-effort and never
//! blocks the span-emitting caller (decision 6); [`ExportGuard`] flushes
//! buffered spans on a clean shutdown (decision 7).

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::trace::{SdkTracerProvider, SpanData, SpanExporter};
use tracing::Dispatch;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// Everything the OTLP trace exporter needs, all supplied by the caller so
/// this crate stays ignorant of Ravel's own types (ADR-0060 decision 5). The
/// caller renders `mode` from its own `Mode` enum; this crate only stamps the
/// two strings as resource attributes.
#[derive(Debug, Clone)]
pub struct OtlpExportConfig {
    /// The collector endpoint URL the OTLP/gRPC exporter dials.
    pub endpoint: String,
    /// The `service.name` resource attribute (`ravel-server` / `ravel-operator`).
    pub service_name: String,
    /// The `ravel.mode` resource attribute, the same label `/metrics` renders
    /// (`all`, `gateway`, `query`, `maintain`).
    pub mode: String,
}

/// The value [`init`] returns. Its [`Drop`], or an explicit [`ExportGuard::flush`],
/// shuts the tracer provider down, flushing any spans the
/// `BatchSpanProcessor` still holds (ADR-0060 decision 7). On the `otlp: None`
/// path it owns nothing and dropping it is a no-op.
#[must_use = "dropping the guard immediately flushes and tears down the OTLP exporter; \
              hold it for the process lifetime"]
pub struct ExportGuard {
    provider: Option<SdkTracerProvider>,
}

impl ExportGuard {
    /// Flush and shut the exporter down explicitly, for a binary whose
    /// shutdown sequence prefers an explicit call to relying on `Drop` order
    /// (ADR-0060 decision 7). Idempotent: a later `Drop` shuts down an
    /// already-shut provider, which the SDK treats as a no-op. Best-effort by
    /// design (decision 6); a failed flush is swallowed, never surfaced as an
    /// error to the shutdown path.
    pub fn flush(&self) {
        if let Some(provider) = &self.provider {
            let _ = provider.shutdown();
        }
    }
}

impl Drop for ExportGuard {
    fn drop(&mut self) {
        self.flush();
    }
}

/// Install the process-wide subscriber (ADR-0060 decision 1). Call once at
/// process start in place of an inline `tracing_subscriber::fmt()`.
///
/// `filter` is the single `EnvFilter` both layers share (decision 2): with
/// `otlp: Some(_)` it gates the `fmt` layer and the OTLP layer alike, so
/// whatever the log stream admits is exactly what export ships, with no
/// second verbosity knob to drift.
pub fn init(filter: EnvFilter, otlp: Option<OtlpExportConfig>) -> ExportGuard {
    let (dispatch, guard, degraded) = build(filter, otlp);
    dispatch.init();
    // A failed exporter build degrades to the fmt-only subscriber rather than
    // refusing startup: trace export is a best-effort debugging aid, not a
    // durability guarantee (ADR-0060 decision 6). Warn once, now that the
    // subscriber is installed so the line actually surfaces.
    if let Some(reason) = degraded {
        tracing::warn!(
            "OTLP trace export disabled: {reason}. Continuing with the log-only subscriber."
        );
    }
    guard
}

/// Build the subscriber as a [`Dispatch`], its [`ExportGuard`], and an
/// optional degradation reason, without installing it. [`init`] installs the
/// returned dispatch globally; tests scope it with
/// `tracing::subscriber::with_default` so several can run in one binary
/// without fighting over the single global default.
fn build(
    filter: EnvFilter,
    otlp: Option<OtlpExportConfig>,
) -> (Dispatch, ExportGuard, Option<String>) {
    let Some(config) = otlp else {
        return (fmt_only(filter), ExportGuard { provider: None }, None);
    };

    let provider = match build_provider(&config) {
        Ok(provider) => provider,
        // Degrade to the log-only subscriber rather than refuse startup; the
        // `filter` is still owned here and moves into the fmt subscriber.
        Err(err) => {
            return (
                fmt_only(filter),
                ExportGuard { provider: None },
                Some(err.to_string()),
            );
        }
    };

    let tracer = provider.tracer("ravel-tracing-export");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    // One EnvFilter as a subscriber-wide layer gates BOTH the fmt and OTLP
    // layers below it (decision 2): not two per-layer filters that could
    // drift.
    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer);
    (
        Dispatch::new(subscriber),
        ExportGuard {
            provider: Some(provider),
        },
        None,
    )
}

/// The bare `fmt` subscriber as a [`Dispatch`] (ADR-0060 decision 1).
/// `.finish()` builds the same subscriber a plain
/// `tracing_subscriber::fmt()...init()` installs, so the log path is
/// unchanged and no OTLP object is constructed.
fn fmt_only(filter: EnvFilter) -> Dispatch {
    Dispatch::new(tracing_subscriber::fmt().with_env_filter(filter).finish())
}

/// Build the OTLP/gRPC tracer provider with a `BatchSpanProcessor` (decisions
/// 1 and 6). The batch processor exports off a background task, so a span
/// emitted while the collector is unreachable never errors, panics, or adds
/// latency to the caller; the exporter dials the collector lazily, not here.
///
/// A syntactically valid endpoint whose collector is unreachable at send time
/// (connection refused, DNS failure, timeout) is not caught here: `build`
/// succeeds and the failure only surfaces when the background export task
/// first dials. [`WarnOnExportFailure`] wraps the exporter to turn that
/// otherwise-silent runtime failure into a one-shot `tracing::warn!`, distinct
/// from the build-time "OTLP trace export disabled" message [`init`] emits.
/// The wrapper only observes the export result on the same
/// background task the SDK already runs, so it adds no blocking or latency to
/// the span-emitting path or to shutdown (decision 6).
fn build_provider(
    config: &OtlpExportConfig,
) -> Result<SdkTracerProvider, opentelemetry_otlp::ExporterBuildError> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(config.endpoint.clone())
        .build()?;

    let resource = Resource::builder()
        .with_service_name(config.service_name.clone())
        .with_attribute(KeyValue::new("ravel.mode", config.mode.clone()))
        .build();

    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(WarnOnExportFailure::new(exporter))
        .with_resource(resource)
        .build())
}

/// A [`SpanExporter`] decorator that surfaces a runtime export failure as a
/// `tracing::warn!` line.
///
/// `opentelemetry_otlp::SpanExporter` dials the collector lazily, so an
/// endpoint that is syntactically valid but unreachable (connection refused,
/// DNS failure, timeout) builds without error and then fails silently on every
/// background export attempt: today nothing surfaces it, and an operator who
/// set the flag and saw no startup warning reasonably assumes export works.
/// This wrapper delegates every call to the inner exporter unchanged and, when
/// `export` returns `Err`, emits one warning. It never alters the returned
/// result, so the best-effort, non-blocking guarantee (ADR-0060 decision 6) is
/// untouched: `export` runs only on the `BatchSpanProcessor`'s background task,
/// and the span-emitting caller and the shutdown flush see exactly the same
/// behavior as before.
///
/// The warning fires at most once per process (`warned`): a broken endpoint
/// fails on every batch interval, and a line per interval forever would drown
/// the log the signal is meant to reach. The SDK itself already emits an
/// internal `error`-level event per failed export (its `internal-logs` feature
/// is on via defaults), so the recurring detail is not lost; this adds the
/// single, plainly worded operator-facing signal that event lacks.
#[derive(Debug)]
struct WarnOnExportFailure {
    inner: opentelemetry_otlp::SpanExporter,
    warned: AtomicBool,
}

impl WarnOnExportFailure {
    fn new(inner: opentelemetry_otlp::SpanExporter) -> Self {
        Self {
            inner,
            warned: AtomicBool::new(false),
        }
    }
}

impl SpanExporter for WarnOnExportFailure {
    async fn export(&self, batch: Vec<SpanData>) -> OTelSdkResult {
        let result = self.inner.export(batch).await;
        if let Err(err) = &result {
            // `swap` returns the previous value: warn only on the first failure
            // this process sees.
            if !self.warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    "OTLP trace export is failing at runtime: {err}. The exporter built \
                     successfully, so the endpoint is well-formed, but the collector could \
                     not be reached (connection refused, DNS failure, or timeout) and spans \
                     are being dropped. Export stays best-effort and non-blocking; this \
                     warns once per process."
                );
            }
        }
        result
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
}

#[cfg(test)]
mod tests;
