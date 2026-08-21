//! Acceptance tests for the export mechanism (ADR-0060 decisions 1, 2, 6),
//! standing up a minimal mock OTLP `TraceService` gRPC server in-process. The
//! mock implements the same
//! `opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::TraceService`
//! trait `services/ravel-server/src/otlp_grpc_traces.rs` implements for
//! ingest.
//!
//! These exercise the private [`build`] seam rather than the public [`init`]
//! so several can run in one test binary: [`init`] installs one process-global
//! subscriber, while `build` returns the same `Dispatch` for a test to scope
//! with `tracing::subscriber::set_default`. `init` is exactly `build` plus that
//! global install, so the subscriber under test is byte-identical to the one a
//! binary installs.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use parking_lot::Mutex;
use tonic::{Request, Response, Status};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt as _;

use super::{OtlpExportConfig, build, build_provider};

/// Every `ExportTraceServiceRequest` the mock collector received.
type Received = Arc<Mutex<Vec<ExportTraceServiceRequest>>>;

/// A mock OTLP trace collector that records each export request. The same
/// `TraceService` trait the real ingest gRPC service implements.
struct MockCollector {
    received: Received,
}

#[tonic::async_trait]
impl TraceService for MockCollector {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        self.received.lock().push(request.into_inner());
        Ok(Response::new(ExportTraceServiceResponse::default()))
    }
}

/// A running mock collector: the address to point the exporter at, the shared
/// received-requests buffer, and a shutdown handle that stops the server when
/// dropped.
struct MockHandle {
    addr: SocketAddr,
    received: Received,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl MockHandle {
    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Count of spans across every received export request.
    fn total_spans(&self) -> usize {
        self.received
            .lock()
            .iter()
            .flat_map(|req| &req.resource_spans)
            .flat_map(|rs| &rs.scope_spans)
            .map(|ss| ss.spans.len())
            .sum()
    }
}

impl Drop for MockHandle {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Start the mock collector on its OWN OS thread with its own runtime. Keeping
/// it off the test's runtime means the test thread can block in the exporter's
/// synchronous shutdown flush without starving the collector that flush is
/// waiting on.
fn start_mock_collector() -> MockHandle {
    let received: Received = Arc::new(Mutex::new(Vec::new()));
    let received_for_server = Arc::clone(&received);
    let (addr_tx, addr_rx) = std::sync::mpsc::channel::<SocketAddr>();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

    let thread = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build mock collector runtime");
        rt.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind mock collector");
            let addr = listener.local_addr().expect("mock collector addr");
            addr_tx.send(addr).expect("report mock collector addr");
            let service = TraceServiceServer::new(MockCollector {
                received: received_for_server,
            });
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming_shutdown(
                    tonic::transport::server::TcpIncoming::from(listener),
                    async {
                        let _ = shutdown_rx.await;
                    },
                )
                .await
                .expect("serve mock collector");
        });
    });

    let addr = addr_rx.recv().expect("mock collector reports its address");
    MockHandle {
        addr,
        received,
        shutdown: Some(shutdown_tx),
        thread: Some(thread),
    }
}

fn config_for(endpoint: String) -> OtlpExportConfig {
    OtlpExportConfig {
        endpoint,
        service_name: "ravel-server".to_string(),
        mode: "all".to_string(),
    }
}

/// Poll `f` until it returns true or the timeout elapses. Export is
/// asynchronous even after a synchronous shutdown returns on some platforms,
/// so a short bounded poll avoids a fixed sleep without becoming flaky.
fn wait_until(mut f: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    f()
}

/// Decision 2/5: with `otlp: Some(_)`, a span emitted through the installed
/// subscriber reaches the collector, carrying the span name and the
/// `service.name` / `ravel.mode` resource attributes.
///
/// Runs on a multi-thread runtime, the same shape the service binaries install,
/// not the single-thread default `#[tokio::test]` gives. `guard.flush()` drives
/// `SdkTracerProvider::shutdown`, a synchronous call that internally waits on
/// the batch processor's async export (with its own bounded timeout) to drain.
/// On a single-thread runtime that async work cannot make progress while the
/// blocking shutdown holds the runtime, so a collector that accepts the
/// connection but is slow to answer leaves the flush waiting forever (issue
/// #454: a 28-minute CI stall until the workflow timeout). A second worker lets
/// the export's own timeout fire, so the flush returns in about ten seconds
/// even against a collector that never responds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn some_exports_the_emitted_span_to_the_collector() {
    let mock = start_mock_collector();
    let (dispatch, guard, degraded) =
        build(EnvFilter::new("info"), Some(config_for(mock.endpoint())));
    assert!(
        degraded.is_none(),
        "building the exporter against a live endpoint must not degrade: {degraded:?}"
    );

    {
        let _default = tracing::dispatcher::set_default(&dispatch);
        // A request-level span shape (ADR-0044 section 5) emitted synchronously,
        // so it is created and closed on this thread while the default is set.
        let span = tracing::info_span!("sql_query", tenant_hash = "deadbeef");
        let _entered = span.enter();
    }

    // Force the batch processor to flush on this call, off the mock's thread.
    // The multi-thread runtime above is the fix for the #454 stall; this bound
    // is the safety net. If the flush ever blocks past 20s anyway, fail with an
    // attributable message rather than hang to the workflow timeout. A healthy
    // flush against the live mock returns in milliseconds and even a
    // never-responding collector returns in about ten seconds, so 20s never
    // fires on a healthy run yet sits far below the workflow timeout. Nested
    // results: `timeout`'s outer Ok means it returned, the inner `JoinHandle`'s
    // Ok means `flush()` did not panic.
    let flushed = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking(move || {
            guard.flush();
        }),
    )
    .await
    .expect("flush against the live mock collector must return within 20s, not hang");
    flushed.expect("flush join");

    assert!(
        wait_until(|| mock.total_spans() >= 1, Duration::from_secs(5)),
        "the collector never received the exported span"
    );

    let received = mock.received.lock();
    let span_names: Vec<&str> = received
        .iter()
        .flat_map(|req| &req.resource_spans)
        .flat_map(|rs| &rs.scope_spans)
        .flat_map(|ss| &ss.spans)
        .map(|s| s.name.as_str())
        .collect();
    assert!(
        span_names.contains(&"sql_query"),
        "expected the sql_query span, got {span_names:?}"
    );

    // Decision 5: the resource attributes distinguish a fleet of processes.
    let attrs: Vec<(String, String)> = received
        .iter()
        .flat_map(|req| &req.resource_spans)
        .flat_map(|rs| rs.resource.iter())
        .flat_map(|r| &r.attributes)
        .filter_map(|kv| {
            let value = kv.value.as_ref()?;
            match &value.value {
                Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) => {
                    Some((kv.key.clone(), s.clone()))
                }
                _ => None,
            }
        })
        .collect();
    assert!(
        attrs.contains(&("service.name".to_string(), "ravel-server".to_string())),
        "missing service.name resource attribute, got {attrs:?}"
    );
    assert!(
        attrs.contains(&("ravel.mode".to_string(), "all".to_string())),
        "missing ravel.mode resource attribute, got {attrs:?}"
    );
}

/// Decision 1: with `otlp: None`, no OTLP object is constructed and nothing is
/// exported, even though a valid, listening collector endpoint exists. The
/// `Option` API models "export not enabled" as `None`, so the endpoint is
/// never dialed. This is the counterpart to the export test above: the same
/// live mock, zero calls.
#[tokio::test]
async fn none_constructs_no_exporter_and_sends_nothing() {
    let mock = start_mock_collector();
    let (dispatch, guard, degraded) = build(EnvFilter::new("info"), None);
    assert!(degraded.is_none(), "the fmt-only path never degrades");
    assert!(
        guard.provider.is_none(),
        "otlp: None must construct no tracer provider"
    );

    {
        let _default = tracing::dispatcher::set_default(&dispatch);
        let span = tracing::info_span!("sql_query", tenant_hash = "deadbeef");
        let _entered = span.enter();
    }

    // A no-op flush, then confirm the live collector saw nothing. Give any
    // stray export the same window the positive test allows before asserting
    // zero, so this is not merely racing the send.
    tokio::task::spawn_blocking(move || guard.flush())
        .await
        .expect("flush join");
    assert!(
        !wait_until(|| mock.total_spans() >= 1, Duration::from_millis(500)),
        "otlp: None must not export to a live collector, but the mock received a span"
    );
}

/// Decision 6: export is best-effort and non-blocking. Pointed at a
/// closed/refused port, emitting a span must not error, panic, or block the
/// caller, and the shutdown flush must return rather than hang on the
/// unreachable collector.
#[tokio::test]
async fn an_unreachable_collector_never_blocks_or_errors_the_caller() {
    // Bind then drop a listener to obtain a port with nothing listening on it.
    let refused_addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind to find a free port");
        listener.local_addr().expect("addr")
    };
    let endpoint = format!("http://{refused_addr}");

    let (dispatch, guard, degraded) = build(EnvFilter::new("info"), Some(config_for(endpoint)));
    assert!(
        degraded.is_none(),
        "the exporter builds lazily; a refused endpoint is not a build failure: {degraded:?}"
    );

    let start = Instant::now();
    {
        let _default = tracing::dispatcher::set_default(&dispatch);
        // The span-emitting caller must observe no error and no latency: the
        // BatchSpanProcessor buffers off-task, so this returns immediately even
        // though nothing is listening at the endpoint.
        let span = tracing::info_span!("sql_query", tenant_hash = "deadbeef");
        let _entered = span.enter();
    }
    let emit_elapsed = start.elapsed();
    assert!(
        emit_elapsed < Duration::from_secs(1),
        "emitting a span with an unreachable collector added latency: {emit_elapsed:?}"
    );

    // The shutdown flush is best-effort: it returns even though the collector
    // is unreachable, never propagating an error to the shutdown path. Two
    // nested results to check, not one: `timeout`'s Ok means it didn't hang,
    // and the inner `JoinHandle`'s Ok means `flush()` itself didn't panic --
    // a bare `flush.is_ok()` on the outer result alone would pass even if
    // `flush()` panicked, since `spawn_blocking` turns a panic into an `Err`
    // on the *inner* result, not a timeout.
    let flush = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking(move || guard.flush()),
    )
    .await
    .expect("flushing against an unreachable collector must return, not hang");
    flush.expect("flush must not panic against an unreachable collector");
}

/// Every WARN-level event the capture layer saw, as `"target::message"`.
type CapturedWarnings = Arc<Mutex<Vec<String>>>;

/// A `tracing` layer that records each WARN event's target and message. This is
/// the event-level counterpart to the mock collector above: where the collector
/// captures exported spans, this captures the `tracing::warn!` line the runtime
/// export-failure signal emits, so the test can assert on it.
struct WarnCaptureLayer {
    warnings: CapturedWarnings,
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnCaptureLayer {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if *event.metadata().level() != tracing::Level::WARN {
            return;
        }
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        self.warnings
            .lock()
            .push(format!("{}::{}", event.metadata().target(), visitor.0));
    }
}

/// Pulls the `message` field out of a `tracing` event as a string.
struct MessageVisitor(String);

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

/// A reachable-but-wrong endpoint (built fine, refused at send time)
/// must surface a `tracing::warn!` line, not fail silently. Point the exporter at
/// a refused port, emit a span, force an export attempt, and assert the crate
/// logged its distinct runtime-failure warning.
///
/// The warning is emitted from the `BatchSpanProcessor`'s background export
/// thread, which carries no thread-local dispatcher, so it routes to the process
/// GLOBAL default subscriber (unlike the other tests here, which scope a
/// thread-local default). This test therefore installs the global default once;
/// no other test in this binary sets a global default, so the single call
/// succeeds. The subscriber mirrors the one `build` installs (an `EnvFilter`
/// over an `fmt` layer and the OTLP layer) plus a `WarnCaptureLayer` to observe
/// the warning.
#[tokio::test]
async fn a_refused_collector_logs_a_runtime_export_failure_warning() {
    // Bind then drop a listener to obtain a port with nothing listening on it,
    // the same refused-port technique the non-blocking test above uses.
    let refused_addr = {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind to find a free port");
        listener.local_addr().expect("addr")
    };
    let endpoint = format!("http://{refused_addr}");

    let provider = build_provider(&config_for(endpoint))
        .expect("a syntactically valid endpoint builds; the failure is at send time");
    let tracer = provider.tracer("ravel-tracing-export");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let warnings: CapturedWarnings = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .with(otel_layer)
        .with(WarnCaptureLayer {
            warnings: Arc::clone(&warnings),
        });
    tracing::subscriber::set_global_default(subscriber)
        .expect("this is the only test in the binary that sets a global default");

    // Emit a span. With the global default installed, it routes to the OTLP
    // layer and is buffered by the batch processor.
    {
        let span = tracing::info_span!("sql_query", tenant_hash = "deadbeef");
        let _entered = span.enter();
    }

    // Force an export attempt off the test's runtime. The export dials the
    // refused endpoint, fails, and the wrapper logs its one-shot warning on the
    // background thread, which reaches the global default installed above.
    // Bound the shutdown for the same reason the flush above is bounded (issue
    // #454): a provider shutdown that drives the batch processor to talk to a
    // network peer must never hang the test to the workflow timeout. A refused
    // port fails fast, so 20s is generous.
    tokio::time::timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking(move || {
            let _ = provider.shutdown();
        }),
    )
    .await
    .expect("shutdown against the refused collector must return within 20s, not hang")
    .expect("shutdown join");

    let matched = |w: &str| {
        w.starts_with("ravel_tracing_export::")
            && w.contains("OTLP trace export is failing at runtime")
    };
    assert!(
        wait_until(
            || warnings.lock().iter().any(|w| matched(w)),
            Duration::from_secs(10)
        ),
        "expected a runtime export-failure warning, captured: {:?}",
        warnings.lock()
    );
}
