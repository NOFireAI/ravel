//! Transport-level ingest admission: the in-flight permit and the tenant
//! credential check, both decided from a request's head, before its body is
//! read or decoded.
//!
//! Every ingest handler already took an [`IngestPermit`] and resolved the
//! tenant as its first two statements, but on both transports those statements
//! ran too late to bound anything an unauthenticated caller could do. On the
//! axum surfaces the handler's `Bytes` argument is an extractor, and axum runs
//! extractors before the handler body, so the whole request body was already
//! buffered (and a gzip body inflated) before the first line of the handler
//! executed. On the tonic surfaces the handler runs only after tonic's codec
//! has read and decoded the request message. Either way the permit bounded
//! post-decode work and nothing else, and no credential was checked before an
//! anonymous caller's bytes were accepted (issue #1705).
//!
//! This module moves both decisions in front of the body on both transports:
//!
//! * [`admit_ingest_request`] is axum middleware, applied with
//!   `route_layer` to the OTLP HTTP and Remote Write ingest routes. It sees
//!   the request head with the body still unread, takes the permit, resolves
//!   the tenant, and puts both results in the request's extensions for the
//!   handler behind it.
//! * [`GrpcIngestAdmissionLayer`] is the tonic counterpart, installed on the
//!   gRPC listener's `Server::builder()`. It takes the permit when the request
//!   head arrives, before tonic reads a single body frame.
//!
//! Both leave an [`IngestPermitHeld`] marker in the request's extensions, and
//! every handler behind them takes its permit through
//! [`admit_grpc_request`] (gRPC) or reads the resolved tenant out of an
//! `Extension` (HTTP), so one request never charges the ceiling twice.
//!
//! The ceiling itself, its `0`-means-unlimited spelling, and the shed counter
//! are unchanged: this is an ordering fix, so a refusal here is the same
//! refusal, counted by the same
//! `ravel_ingest_concurrency_shed_total`.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use ravel_query::http::TenantResolver;
use tonic::Status;
use tonic::body::Body as TonicBody;
use tower::{Layer, Service};

use crate::ingest_concurrency::{IngestConcurrencyController, IngestPermit};

/// The gRPC service path prefix every unary OTLP ingest service shares
/// (`opentelemetry.proto.collector.{metrics,logs,trace}.v1`).
///
/// The gRPC listener also carries Flight SQL, the ADR-0071 fragment service,
/// and (under `--otap`) the OTAP `ArrowMetricsService`, all of which this
/// layer must leave alone. The first two are query surfaces with no ingest
/// ceiling at all. OTAP is ingest, but it is a bidirectional *stream*: it
/// takes one permit per `BatchArrowRecords` inside `otap_grpc::process_batch`
/// rather than one for the whole connection, and a stream-lifetime permit
/// taken here would pin a slot for as long as a client keeps the stream open.
/// Its package is `opentelemetry.proto.experimental.arrow.v1`, outside this
/// prefix, so the two schemes do not overlap.
const OTLP_UNARY_INGEST_PATH_PREFIX: &str = "/opentelemetry.proto.collector.";

/// Marks a request whose in-flight permit was already taken by the transport
/// layer in front of the handler. The handler behind the layer must not take
/// a second permit from the same ceiling for the same request: that would
/// halve the effective bound and double-count every shed.
///
/// Deliberately a marker rather than the permit itself: extensions require
/// `Clone`, and an `IngestPermit` moved into extensions would be released
/// wherever those extensions are dropped (tonic's `Request::into_inner`
/// drops them mid-handler), which is earlier than the work the permit is
/// meant to bound. The permit instead lives in the layer's own future, whose
/// lifetime is exactly the request's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IngestPermitHeld;

/// What the axum admission middleware needs from the state of whichever
/// ingest surface it is wrapping: the shared ceiling and the tenant resolver.
/// Implemented by `otlp_http::GatewayState` and
/// `remote_write::RemoteWriteState`, the two axum ingest surfaces.
pub trait IngestAdmissionState: Send + Sync + 'static {
    fn ingest_concurrency(&self) -> &Arc<IngestConcurrencyController>;

    fn tenant_resolver(&self) -> &Arc<dyn TenantResolver>;

    /// Called instead of returning the 401 body directly, for a surface that
    /// keeps its own rejected-request counter. The default does nothing.
    fn on_unauthorized(&self) {}
}

/// axum middleware for the ingest routes: takes the in-flight permit, then
/// authenticates the tenant, both from the request head, and only then lets
/// the body extractor behind it run.
///
/// Ordering between the two is the pre-existing handler ordering, kept
/// deliberately: the ceiling is what bounds work done on behalf of callers
/// this process has not authenticated yet, so it has to decide first. A
/// request that arrives over the ceiling is therefore shed (429) rather than
/// rejected (401) even when its credentials are also bad.
///
/// The permit is bound for the whole of `next.run(...)`, so it covers the
/// body read, the decode, and the durable write, and its RAII drop returns
/// the slot on every exit path including a client disconnect that cancels
/// this future.
pub async fn admit_ingest_request<S: IngestAdmissionState>(
    State(state): State<Arc<S>>,
    mut request: Request,
    next: Next,
) -> Response {
    let _permit = match state.ingest_concurrency().try_admit() {
        Ok(permit) => permit,
        Err(_) => return crate::otlp_http::ingest_concurrency_shed_response(),
    };

    let tenant = match state.tenant_resolver().resolve(request.headers()) {
        Ok(tenant) => tenant,
        Err(_) => {
            state.on_unauthorized();
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    let extensions = request.extensions_mut();
    extensions.insert(IngestPermitHeld);
    extensions.insert(tenant);
    next.run(request).await
}

/// Takes the in-flight permit for a gRPC ingest request, unless
/// [`GrpcIngestAdmissionLayer`] already took it for this request.
///
/// `Ok(None)` means the layer holds the permit and will release it when the
/// request completes; the handler must not take another. `Ok(Some(permit))`
/// is the direct-call path (a unit test that invokes a service method without
/// the listener's layer stack), where the handler is the only thing that can
/// take one.
pub(crate) fn admit_grpc_request<T>(
    controller: &IngestConcurrencyController,
    request: &tonic::Request<T>,
) -> Result<Option<IngestPermit>, Status> {
    if request.extensions().get::<IngestPermitHeld>().is_some() {
        return Ok(None);
    }
    controller
        .try_admit()
        .map(Some)
        .map_err(|_| crate::otlp_grpc::ingest_concurrency_shed_status())
}

/// A [`tower::Layer`] for the tonic ingest listener: takes the process-wide
/// in-flight permit when a unary OTLP export's request head arrives, so a
/// request over the ceiling is refused `RESOURCE_EXHAUSTED` without tonic
/// ever reading, decompressing, or decoding its message.
///
/// Install it on the same `Server::builder()` as
/// [`crate::wire_byte_count::WireByteCountLayer`]; it wraps whichever
/// services are added after, and passes every non-OTLP-ingest path straight
/// through.
#[derive(Clone)]
pub struct GrpcIngestAdmissionLayer {
    controller: Arc<IngestConcurrencyController>,
}

impl GrpcIngestAdmissionLayer {
    pub fn new(controller: Arc<IngestConcurrencyController>) -> Self {
        GrpcIngestAdmissionLayer { controller }
    }
}

impl<S> Layer<S> for GrpcIngestAdmissionLayer {
    type Service = GrpcIngestAdmissionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcIngestAdmissionService {
            inner,
            controller: self.controller.clone(),
        }
    }
}

#[derive(Clone)]
pub struct GrpcIngestAdmissionService<S> {
    inner: S,
    controller: Arc<IngestConcurrencyController>,
}

impl<S> Service<http::Request<TonicBody>> for GrpcIngestAdmissionService<S>
where
    S: Service<http::Request<TonicBody>, Response = http::Response<TonicBody>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<TonicBody>;
    type Error = S::Error;
    // Boxed because the admitted arm has to keep the permit alive alongside
    // the inner future and the shed arm resolves without one, so the two arms
    // are different types. One allocation per unary ingest request, against a
    // protobuf decode and a durable write.
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        if !req.uri().path().starts_with(OTLP_UNARY_INGEST_PATH_PREFIX) {
            return Box::pin(self.inner.call(req));
        }
        let permit = match self.controller.try_admit() {
            Ok(permit) => permit,
            Err(_) => {
                let response = grpc_shed_response();
                return Box::pin(async move { Ok(response) });
            }
        };
        let (mut parts, body) = req.into_parts();
        parts.extensions.insert(IngestPermitHeld);
        let future = self.inner.call(http::Request::from_parts(parts, body));
        Box::pin(async move {
            // Held for the whole inner call: the body read, tonic's decode,
            // and the handler's durable write. Dropped when this future
            // finishes or is cancelled.
            let _permit = permit;
            future.await
        })
    }
}

/// The shed refusal as a complete gRPC response: a 200 HTTP response whose
/// trailers carry `grpc-status: 8` (RESOURCE_EXHAUSTED), built from the same
/// [`crate::otlp_grpc::ingest_concurrency_shed_status`] the handlers return,
/// so a client sees one status and one message whichever layer refused it.
fn grpc_shed_response() -> http::Response<TonicBody> {
    crate::otlp_grpc::ingest_concurrency_shed_status().into_http()
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use http_body::{Body, Frame, SizeHint};

    use super::*;
    use crate::ingest_concurrency::IngestConcurrencyLimit;

    /// A request body that fails the test if anything polls it. The point of
    /// the layer is that a shed request's body is never read, and this is the
    /// only place that is directly observable: from a client, an unread body
    /// and a body read-then-discarded look identical.
    struct PoisonBody {
        polls: Arc<AtomicUsize>,
    }

    impl Body for PoisonBody {
        type Data = Bytes;
        type Error = Status;

        fn poll_frame(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Some(Err(Status::internal("body polled"))))
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::new()
        }
    }

    /// An inner service that fails the test if it is ever called: the shed
    /// arm must answer without reaching the tonic routes behind it.
    #[derive(Clone)]
    struct UnreachableInner;

    impl Service<http::Request<TonicBody>> for UnreachableInner {
        type Response = http::Response<TonicBody>;
        type Error = std::convert::Infallible;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: http::Request<TonicBody>) -> Self::Future {
            panic!("the inner service must not be reached for a shed request");
        }
    }

    /// Counts calls and echoes an empty OK response, for the admitted path.
    #[derive(Clone, Default)]
    struct CountingInner {
        calls: Arc<AtomicUsize>,
        held_marker_seen: Arc<AtomicUsize>,
    }

    impl Service<http::Request<TonicBody>> for CountingInner {
        type Response = http::Response<TonicBody>;
        type Error = std::convert::Infallible;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if req.extensions().get::<IngestPermitHeld>().is_some() {
                self.held_marker_seen.fetch_add(1, Ordering::SeqCst);
            }
            Box::pin(async move { Ok(http::Response::new(TonicBody::empty())) })
        }
    }

    fn poisoned_request(path: &str, polls: Arc<AtomicUsize>) -> http::Request<TonicBody> {
        http::Request::builder()
            .method(http::Method::POST)
            .uri(path)
            .body(TonicBody::new(PoisonBody { polls }))
            .expect("request builds")
    }

    const EXPORT_PATH: &str = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";

    /// Issue #1705: over the ceiling, the layer answers RESOURCE_EXHAUSTED
    /// without polling the request body once and without reaching the service
    /// behind it, so no part of the message is read, decompressed, or decoded.
    ///
    /// Non-vacuity: move the `try_admit` call behind `self.inner.call(req)`
    /// and `UnreachableInner` panics; make the shed arm forward the request
    /// instead of answering, and the poll counter is nonzero.
    #[tokio::test]
    async fn shed_request_answers_resource_exhausted_without_polling_the_body() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let _held = controller.try_admit().expect("the only permit");

        let layer = GrpcIngestAdmissionLayer::new(controller.clone());
        let mut service = layer.layer(UnreachableInner);

        let polls = Arc::new(AtomicUsize::new(0));
        let response = service
            .call(poisoned_request(EXPORT_PATH, polls.clone()))
            .await
            .expect("the shed arm is infallible");

        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "the shed request's body must never be polled"
        );
        assert_eq!(
            response
                .headers()
                .get("grpc-status")
                .map(|v| v.to_str().expect("ascii grpc-status")),
            Some("8"),
            "gRPC status 8 is RESOURCE_EXHAUSTED"
        );
        assert_eq!(
            controller.shed_total(),
            1,
            "the existing shed counter counts it"
        );
    }

    /// An admitted request reaches the service behind the layer carrying the
    /// `IngestPermitHeld` marker, which is what stops the handler taking a
    /// second permit from the same ceiling.
    #[tokio::test]
    async fn admitted_request_reaches_the_inner_service_marked_as_held() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let inner = CountingInner::default();
        let mut service = GrpcIngestAdmissionLayer::new(controller.clone()).layer(inner.clone());

        let polls = Arc::new(AtomicUsize::new(0));
        let response = service
            .call(poisoned_request(EXPORT_PATH, polls))
            .await
            .expect("infallible inner");

        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.held_marker_seen.load(Ordering::SeqCst), 1);
        assert_eq!(controller.shed_total(), 0);
    }

    /// The permit is released when the layer's future completes, so a serial
    /// sequence of requests under a ceiling of 1 all pass. A permit leaked
    /// into the response (or held past the future) would shed the second.
    #[tokio::test]
    async fn the_permit_is_released_when_the_request_completes() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let inner = CountingInner::default();
        let mut service = GrpcIngestAdmissionLayer::new(controller.clone()).layer(inner.clone());

        for _ in 0..3 {
            let polls = Arc::new(AtomicUsize::new(0));
            service
                .call(poisoned_request(EXPORT_PATH, polls))
                .await
                .expect("infallible inner");
        }

        assert_eq!(inner.calls.load(Ordering::SeqCst), 3);
        assert_eq!(controller.shed_total(), 0);
    }

    /// Flight SQL, the ADR-0071 fragment service, and OTAP share this
    /// listener and must pass through untouched: none of them takes a permit
    /// here, and OTAP in particular must keep taking one per batch inside its
    /// own handler rather than one for the lifetime of a stream.
    #[tokio::test]
    async fn non_otlp_paths_pass_through_without_taking_a_permit() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let _held = controller.try_admit().expect("the only permit");

        let inner = CountingInner::default();
        let mut service = GrpcIngestAdmissionLayer::new(controller.clone()).layer(inner.clone());

        for path in [
            "/arrow.flight.protocol.FlightService/DoGet",
            "/opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService/ArrowMetrics",
        ] {
            let polls = Arc::new(AtomicUsize::new(0));
            let response = service
                .call(poisoned_request(path, polls))
                .await
                .expect("infallible inner");
            assert_eq!(
                response.status(),
                http::StatusCode::OK,
                "{path} passes through"
            );
        }

        assert_eq!(inner.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            inner.held_marker_seen.load(Ordering::SeqCst),
            0,
            "a pass-through request is not marked as holding a permit"
        );
        assert_eq!(
            controller.shed_total(),
            0,
            "no non-OTLP path is charged against the ingest ceiling, even when it is full"
        );
    }

    /// `admit_grpc_request` takes a permit for a direct handler call and none
    /// for a request the layer already admitted, so one request never charges
    /// the ceiling twice.
    #[test]
    fn admit_grpc_request_does_not_double_charge_a_marked_request() {
        let controller = IngestConcurrencyController::new(IngestConcurrencyLimit::Bounded(1));

        let mut marked = tonic::Request::new(());
        marked.extensions_mut().insert(IngestPermitHeld);
        let permit = admit_grpc_request(&controller, &marked).expect("marked request is admitted");
        assert!(
            permit.is_none(),
            "a request the layer admitted must not take a second permit"
        );

        let unmarked = tonic::Request::new(());
        let permit =
            admit_grpc_request(&controller, &unmarked).expect("the direct call takes a permit");
        assert!(permit.is_some());

        let over = tonic::Request::new(());
        let status = admit_grpc_request(&controller, &over)
            .expect_err("the ceiling of 1 is now full for a direct call");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            status.message(),
            "process in-flight ingest-request limit reached"
        );
    }
}
