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

use opentelemetry_proto::tonic::collector::trace::v1::trace_service_server::{
    TraceService, TraceServiceServer,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use parking_lot::Mutex;
use tonic::{Request, Response, Status};
use tracing_subscriber::EnvFilter;

use super::{OtlpExportConfig, build};

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
#[tokio::test]
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
    let flushed = tokio::task::spawn_blocking(move || {
        guard.flush();
    })
    .await;
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
    // is unreachable, never propagating an error to the shutdown path.
    let flush = tokio::time::timeout(
        Duration::from_secs(20),
        tokio::task::spawn_blocking(move || guard.flush()),
    )
    .await;
    assert!(
        flush.is_ok(),
        "flushing against an unreachable collector must return, not hang"
    );
}
