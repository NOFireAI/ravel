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
//!   gRPC listener's `Server::builder()`. For a unary OTLP export it takes the
//!   permit and then resolves the tenant when the request head arrives,
//!   before tonic reads a single body frame. For the OTAP stream it resolves
//!   the tenant on the stream's head and takes no permit (see
//!   [`OTAP_ARROW_METRICS_PATH`]).
//!
//! Both leave an [`IngestPermitHeld`] marker and the resolved `TenantId` in
//! the request's extensions. Every handler behind them takes its permit
//! through [`admit_grpc_request`] and its tenant through
//! [`grpc_request_tenant`] (gRPC), or reads the tenant out of an `Extension`
//! (HTTP), so one request never charges the ceiling twice.
//!
//! The ceiling itself, its `0`-means-unlimited spelling, and the shed counter
//! are unchanged: this is an ordering fix, so a refusal here is the same
//! refusal, counted by the same
//! `ravel_ingest_concurrency_shed_total`.
//!
//! Because the permit is now taken before the body arrives, it is held while
//! the body is received, so both layers also bound that wait with
//! [`INGEST_BODY_READ_TIMEOUT`]. Without the bound a client could hold every
//! permit by sending request heads and then trickling their bodies.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use ravel_query::http::TenantResolver;
use ravel_types::TenantId;
use tonic::Status;
use tonic::body::Body as TonicBody;
use tower::{Layer, Service};

use crate::ingest_concurrency::{IngestConcurrencyController, IngestPermit};

/// The gRPC service path prefix every unary OTLP ingest service shares
/// (`opentelemetry.proto.collector.{metrics,logs,trace}.v1`).
///
/// The gRPC listener also carries Flight SQL and the ADR-0071 fragment
/// service, query surfaces with no ingest ceiling that this layer passes
/// through untouched, and (under `--otap`) the OTAP stream at
/// [`OTAP_ARROW_METRICS_PATH`], outside this prefix.
const OTLP_UNARY_INGEST_PATH_PREFIX: &str = "/opentelemetry.proto.collector.";

/// The OTAP `ArrowMetricsService` stream. The layer authenticates it on the
/// stream's request head, before any `BatchArrowRecords` frame is read, but
/// takes no permit for it: the stream takes one permit per batch inside
/// `otap_grpc::process_batch`, after tonic has decoded that batch's protobuf
/// frame and before its Arrow payloads are decoded. A stream-lifetime permit
/// taken here would pin a slot for as long as a client keeps the stream open.
const OTAP_ARROW_METRICS_PATH: &str =
    "/opentelemetry.proto.experimental.arrow.v1.ArrowMetricsService/ArrowMetrics";

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

/// How long an admitted ingest request may take to deliver its whole body,
/// counted from the moment its head was admitted. A request that has not
/// finished by then is refused (408 on HTTP, `DEADLINE_EXCEEDED` on gRPC) and
/// its permit returns to the ceiling.
///
/// 30 s is Prometheus' default `remote_timeout`, the most generous default
/// deadline among the senders this ingests from (the OpenTelemetry
/// Collector's OTLP exporters default to 5 s), so a sender still inside its
/// own deadline is never cut off, and a 16 MiB body fits in it at about
/// 4.5 Mbit/s.
pub const INGEST_BODY_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// The error a [`DeadlineBody`] yields once [`INGEST_BODY_READ_TIMEOUT`]
/// passes. Callers never see it: the layer that installed the body replaces
/// whatever response the body's reader built from it.
#[derive(Debug)]
struct BodyReadTimedOut;

impl std::fmt::Display for BodyReadTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "request body not received within {}s",
            INGEST_BODY_READ_TIMEOUT.as_secs()
        )
    }
}

impl std::error::Error for BodyReadTimedOut {}

/// A request body that fails with [`BodyReadTimedOut`] if its last frame has
/// not arrived by `deadline`, and records in `expired` that it did, so the
/// layer that owns the permit can answer with the timeout status.
struct DeadlineBody<B> {
    inner: B,
    deadline: Pin<Box<tokio::time::Sleep>>,
    expired: Arc<AtomicBool>,
}

impl<B> DeadlineBody<B> {
    fn new(inner: B, expired: Arc<AtomicBool>) -> Self {
        DeadlineBody {
            inner,
            deadline: Box::pin(tokio::time::sleep(INGEST_BODY_READ_TIMEOUT)),
            expired,
        }
    }
}

impl<B> HttpBody for DeadlineBody<B>
where
    B: HttpBody<Data = Bytes> + Unpin,
    B::Error: Into<axum::BoxError>,
{
    type Data = Bytes;
    type Error = axum::BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        match Pin::new(&mut this.inner).poll_frame(cx) {
            Poll::Ready(frame) => Poll::Ready(frame.map(|frame| frame.map_err(Into::into))),
            Poll::Pending => match this.deadline.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    this.expired.store(true, Ordering::SeqCst);
                    Poll::Ready(Some(Err(Box::new(BodyReadTimedOut))))
                }
                Poll::Pending => Poll::Pending,
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

/// 408 for an HTTP ingest upload whose body did not arrive within
/// [`INGEST_BODY_READ_TIMEOUT`].
fn body_timeout_response() -> Response {
    (StatusCode::REQUEST_TIMEOUT, BodyReadTimedOut.to_string()).into_response()
}

/// `DEADLINE_EXCEEDED` for a gRPC ingest export whose message did not arrive
/// within [`INGEST_BODY_READ_TIMEOUT`].
fn body_timeout_status() -> Status {
    Status::deadline_exceeded(BodyReadTimedOut.to_string())
}

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
/// this future. The body read is bounded by [`INGEST_BODY_READ_TIMEOUT`]: a
/// body still incomplete then fails the handler's body extractor, so the
/// handler never runs, and the request is answered 408.
pub async fn admit_ingest_request<S: IngestAdmissionState>(
    State(state): State<Arc<S>>,
    request: Request,
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

    let expired = Arc::new(AtomicBool::new(false));
    let mut request = request
        .map(|body| axum::body::Body::new(DeadlineBody::new(body, expired.clone())));
    let extensions = request.extensions_mut();
    extensions.insert(IngestPermitHeld);
    extensions.insert(tenant);
    let response = next.run(request).await;
    if expired.load(Ordering::SeqCst) {
        return body_timeout_response();
    }
    response
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

/// The tenant for a gRPC ingest request: the one [`GrpcIngestAdmissionLayer`]
/// already authenticated from the request head, or, on the direct-call path
/// where no layer ran, resolved here from `headers` (the request's metadata).
pub(crate) fn grpc_request_tenant<T>(
    resolver: &dyn TenantResolver,
    request: &tonic::Request<T>,
    headers: &HeaderMap,
) -> Result<TenantId, Status> {
    if let Some(tenant) = request.extensions().get::<TenantId>() {
        return Ok(tenant.clone());
    }
    resolver
        .resolve(headers)
        .map_err(|_| crate::otlp_grpc::unauthenticated_status())
}

/// Resolves the tenant from a gRPC request head exactly as a handler resolves
/// it from the decoded request's metadata: tonic builds that metadata from
/// these same headers, and [`crate::otlp_grpc::metadata_to_headers`] is the
/// same filter the handlers apply.
fn grpc_head_tenant(
    resolver: &dyn TenantResolver,
    headers: &HeaderMap,
) -> Result<TenantId, Status> {
    let metadata = tonic::metadata::MetadataMap::from_headers(headers.clone());
    resolver
        .resolve(&crate::otlp_grpc::metadata_to_headers(&metadata))
        .map_err(|_| crate::otlp_grpc::unauthenticated_status())
}

/// A [`tower::Layer`] for the tonic ingest listener. When a unary OTLP
/// export's request head arrives it takes the process-wide in-flight permit
/// and then authenticates the tenant, so a request over the ceiling is
/// refused `RESOURCE_EXHAUSTED`, and one without valid credentials
/// `UNAUTHENTICATED`, without tonic ever reading, decompressing, or decoding
/// its message. The OTAP stream is authenticated the same way on its head,
/// without a permit.
///
/// The ordering is the HTTP middleware's: the ceiling decides first, so a
/// request over it is shed even when its credentials are also bad.
///
/// An admitted unary export whose message has not fully arrived within
/// [`INGEST_BODY_READ_TIMEOUT`] is refused `DEADLINE_EXCEEDED` and its permit
/// released. The OTAP stream is not bounded this way: it holds no permit
/// while it waits for a frame, and a stream is expected to stay open.
///
/// Install it on the same `Server::builder()` as
/// [`crate::wire_byte_count::WireByteCountLayer`]; it wraps whichever
/// services are added after, and passes every other path straight through,
/// including an ingest path whose service this listener does not register,
/// so that path still answers tonic's `UNIMPLEMENTED`.
#[derive(Clone)]
pub struct GrpcIngestAdmissionLayer {
    controller: Arc<IngestConcurrencyController>,
    resolver: Arc<dyn TenantResolver>,
    services: GrpcIngestServices,
}

/// Which ingest services the listener behind a [`GrpcIngestAdmissionLayer`]
/// registers. The layer only claims the paths of the ones that are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GrpcIngestServices {
    /// The unary OTLP metrics, logs and traces export services.
    pub otlp: bool,
    /// The OTAP `ArrowMetricsService` stream.
    pub otap: bool,
}

impl GrpcIngestAdmissionLayer {
    pub fn new(
        controller: Arc<IngestConcurrencyController>,
        resolver: Arc<dyn TenantResolver>,
        services: GrpcIngestServices,
    ) -> Self {
        GrpcIngestAdmissionLayer {
            controller,
            resolver,
            services,
        }
    }
}

impl<S> Layer<S> for GrpcIngestAdmissionLayer {
    type Service = GrpcIngestAdmissionService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcIngestAdmissionService {
            inner,
            controller: self.controller.clone(),
            resolver: self.resolver.clone(),
            services: self.services,
        }
    }
}

#[derive(Clone)]
pub struct GrpcIngestAdmissionService<S> {
    inner: S,
    controller: Arc<IngestConcurrencyController>,
    resolver: Arc<dyn TenantResolver>,
    services: GrpcIngestServices,
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
        let path = req.uri().path();
        let unary = self.services.otlp && path.starts_with(OTLP_UNARY_INGEST_PATH_PREFIX);
        let otap = self.services.otap && path == OTAP_ARROW_METRICS_PATH;
        if !unary && !otap {
            return Box::pin(self.inner.call(req));
        }
        let permit = if unary {
            match self.controller.try_admit() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    let response = grpc_shed_response();
                    return Box::pin(async move { Ok(response) });
                }
            }
        } else {
            None
        };
        let (mut parts, body) = req.into_parts();
        let tenant = match grpc_head_tenant(self.resolver.as_ref(), &parts.headers) {
            Ok(tenant) => tenant,
            Err(status) => {
                // The permit, if one was taken, is released here with the
                // unread body.
                let response = status.into_http();
                return Box::pin(async move { Ok(response) });
            }
        };
        parts.extensions.insert(tenant);
        if permit.is_none() {
            return Box::pin(self.inner.call(http::Request::from_parts(parts, body)));
        }
        parts.extensions.insert(IngestPermitHeld);
        let expired = Arc::new(AtomicBool::new(false));
        let body = TonicBody::new(DeadlineBody::new(body, expired.clone()));
        let future = self.inner.call(http::Request::from_parts(parts, body));
        Box::pin(async move {
            // Held for the whole inner call: the body read, tonic's decode,
            // and the handler's durable write. Dropped when this future
            // finishes or is cancelled.
            let _permit = permit;
            let response = future.await?;
            // An expired body failed tonic's decode, so the handler never
            // ran; answer with the timeout rather than the decode error.
            if expired.load(Ordering::SeqCst) {
                return Ok(body_timeout_status().into_http());
            }
            Ok(response)
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
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use http_body::{Body, Frame, SizeHint};
    use ravel_query::http::StaticBearerTokenResolver;

    use super::*;
    use crate::ingest_concurrency::IngestConcurrencyLimit;

    const TOKEN: &str = "t-acme";

    fn resolver() -> Arc<dyn TenantResolver> {
        Arc::new(StaticBearerTokenResolver::new(HashMap::from([(
            TOKEN.to_string(),
            TenantId::new("acme"),
        )])))
    }

    const ALL_INGEST_SERVICES: GrpcIngestServices = GrpcIngestServices {
        otlp: true,
        otap: true,
    };

    fn layer(controller: &Arc<IngestConcurrencyController>) -> GrpcIngestAdmissionLayer {
        GrpcIngestAdmissionLayer::new(controller.clone(), resolver(), ALL_INGEST_SERVICES)
    }

    /// The status a gRPC client decodes from a trailers-only refusal.
    fn grpc_status_of(response: &http::Response<TonicBody>) -> (tonic::Code, String) {
        let status =
            Status::from_header_map(response.headers()).expect("a refusal carries grpc-status");
        (status.code(), status.message().to_string())
    }

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
            panic!("the inner service must not be reached for a refused request");
        }
    }

    /// Counts calls and echoes an empty OK response, for the admitted path.
    #[derive(Clone, Default)]
    struct CountingInner {
        calls: Arc<AtomicUsize>,
        held_marker_seen: Arc<AtomicUsize>,
        tenants_seen: Arc<Mutex<Vec<TenantId>>>,
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
            if let Some(tenant) = req.extensions().get::<TenantId>() {
                self.tenants_seen
                    .lock()
                    .expect("unpoisoned")
                    .push(tenant.clone());
            }
            Box::pin(async move { Ok(http::Response::new(TonicBody::empty())) })
        }
    }

    fn poisoned_request(
        path: &str,
        polls: Arc<AtomicUsize>,
        bearer: Option<&str>,
    ) -> http::Request<TonicBody> {
        let mut builder = http::Request::builder()
            .method(http::Method::POST)
            .uri(path);
        if let Some(token) = bearer {
            builder = builder.header("authorization", format!("Bearer {token}"));
        }
        builder
            .body(TonicBody::new(PoisonBody { polls }))
            .expect("request builds")
    }

    const OTLP_EXPORT_PATHS: [&str; 3] = [
        "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export",
        "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
        "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
    ];

    const EXPORT_PATH: &str = "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export";

    /// Issue #1705: over the ceiling, the layer answers RESOURCE_EXHAUSTED
    /// without polling the request body once and without reaching the service
    /// behind it, so no part of the message is read, decompressed, or decoded.
    ///
    /// The request carries no credentials, which pins the ordering too: the
    /// ceiling decides before the credential check, as on HTTP.
    ///
    /// Non-vacuity: move the `try_admit` call behind `self.inner.call(req)`
    /// and `UnreachableInner` panics; make the shed arm forward the request
    /// instead of answering, and the poll counter is nonzero; authenticate
    /// before admitting and the status is UNAUTHENTICATED.
    #[tokio::test]
    async fn shed_request_answers_resource_exhausted_without_polling_the_body() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let _held = controller.try_admit().expect("the only permit");

        let mut service = layer(&controller).layer(UnreachableInner);

        let polls = Arc::new(AtomicUsize::new(0));
        let response = service
            .call(poisoned_request(EXPORT_PATH, polls.clone(), None))
            .await
            .expect("the shed arm is infallible");

        assert_eq!(
            polls.load(Ordering::SeqCst),
            0,
            "the shed request's body must never be polled"
        );
        assert_eq!(
            grpc_status_of(&response),
            (
                tonic::Code::ResourceExhausted,
                "process in-flight ingest-request limit reached".to_string()
            )
        );
        assert_eq!(
            controller.shed_total(),
            1,
            "the existing shed counter counts it"
        );
    }

    /// An admitted request reaches the service behind the layer carrying the
    /// `IngestPermitHeld` marker and the authenticated tenant, which is what
    /// stops the handler taking a second permit or resolving a second time.
    #[tokio::test]
    async fn admitted_request_reaches_the_inner_service_marked_as_held() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let inner = CountingInner::default();
        let mut service = layer(&controller).layer(inner.clone());

        let polls = Arc::new(AtomicUsize::new(0));
        let response = service
            .call(poisoned_request(EXPORT_PATH, polls, Some(TOKEN)))
            .await
            .expect("infallible inner");

        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(inner.held_marker_seen.load(Ordering::SeqCst), 1);
        assert_eq!(
            *inner.tenants_seen.lock().expect("unpoisoned"),
            vec![TenantId::new("acme")]
        );
        assert_eq!(controller.shed_total(), 0);
    }

    /// Issue #1705: an OTLP export without valid credentials is refused
    /// UNAUTHENTICATED, with the handlers' message, without its body being
    /// polled or the routes behind the layer being reached, and the permit it
    /// took first is released rather than held for the refused request.
    ///
    /// Non-vacuity: drop the credential check from the layer and
    /// `UnreachableInner` panics; hold the permit past the refusal and the
    /// final `try_admit` fails.
    #[tokio::test]
    async fn unauthenticated_export_is_refused_without_polling_the_body() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let mut service = layer(&controller).layer(UnreachableInner);

        for path in OTLP_EXPORT_PATHS {
            for bearer in [None, Some("not-a-token")] {
                let polls = Arc::new(AtomicUsize::new(0));
                let response = service
                    .call(poisoned_request(path, polls.clone(), bearer))
                    .await
                    .expect("the refusal arm is infallible");

                assert_eq!(
                    grpc_status_of(&response),
                    (
                        tonic::Code::Unauthenticated,
                        "invalid or missing tenant credentials".to_string()
                    ),
                    "{path} with {bearer:?}"
                );
                assert_eq!(polls.load(Ordering::SeqCst), 0, "{path} with {bearer:?}");
            }
        }

        assert_eq!(
            controller.shed_total(),
            0,
            "a credential refusal is not a shed"
        );
        let _permit = controller
            .try_admit()
            .expect("every refused request released its permit");
    }

    /// Issue #1705: the OTAP stream is authenticated on its head, before any
    /// frame of it is read, and takes no permit there even with the ceiling
    /// full: its permits are per batch, inside the handler.
    ///
    /// Non-vacuity: leave the OTAP path out of the layer and the unauthenticated
    /// stream reaches `UnreachableInner`; take a permit for it and the
    /// authenticated stream is shed against the held permit.
    #[tokio::test]
    async fn otap_stream_is_authenticated_on_its_head_without_a_permit() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let _held = controller.try_admit().expect("the only permit");

        let polls = Arc::new(AtomicUsize::new(0));
        let response = layer(&controller)
            .layer(UnreachableInner)
            .call(poisoned_request(
                OTAP_ARROW_METRICS_PATH,
                polls.clone(),
                None,
            ))
            .await
            .expect("the refusal arm is infallible");
        assert_eq!(
            grpc_status_of(&response),
            (
                tonic::Code::Unauthenticated,
                "invalid or missing tenant credentials".to_string()
            )
        );
        assert_eq!(polls.load(Ordering::SeqCst), 0);

        let inner = CountingInner::default();
        let response = layer(&controller)
            .layer(inner.clone())
            .call(poisoned_request(
                OTAP_ARROW_METRICS_PATH,
                Arc::new(AtomicUsize::new(0)),
                Some(TOKEN),
            ))
            .await
            .expect("infallible inner");
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(inner.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            inner.held_marker_seen.load(Ordering::SeqCst),
            0,
            "the stream holds no permit from the layer"
        );
        assert_eq!(
            *inner.tenants_seen.lock().expect("unpoisoned"),
            vec![TenantId::new("acme")]
        );
        assert_eq!(controller.shed_total(), 0);
    }

    /// The permit is released when the layer's future completes, so a serial
    /// sequence of requests under a ceiling of 1 all pass. A permit leaked
    /// into the response (or held past the future) would shed the second.
    #[tokio::test]
    async fn the_permit_is_released_when_the_request_completes() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let inner = CountingInner::default();
        let mut service = layer(&controller).layer(inner.clone());

        for _ in 0..3 {
            let polls = Arc::new(AtomicUsize::new(0));
            service
                .call(poisoned_request(EXPORT_PATH, polls, Some(TOKEN)))
                .await
                .expect("infallible inner");
        }

        assert_eq!(inner.calls.load(Ordering::SeqCst), 3);
        assert_eq!(controller.shed_total(), 0);
    }

    /// Flight SQL and the ADR-0071 fragment service share this listener and
    /// must pass through untouched: no permit and no credential check here,
    /// even with no credentials and the ceiling full. They authenticate in
    /// their own handlers.
    #[tokio::test]
    async fn non_ingest_paths_pass_through_untouched() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let _held = controller.try_admit().expect("the only permit");

        let inner = CountingInner::default();
        let mut service = layer(&controller).layer(inner.clone());

        for path in [
            "/arrow.flight.protocol.FlightService/DoGet",
            "/arrow.flight.protocol.FlightService/DoPut",
            "/grpc.health.v1.Health/Check",
        ] {
            let polls = Arc::new(AtomicUsize::new(0));
            let response = service
                .call(poisoned_request(path, polls, None))
                .await
                .expect("infallible inner");
            assert_eq!(
                response.status(),
                http::StatusCode::OK,
                "{path} passes through"
            );
        }

        assert_eq!(inner.calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            inner.held_marker_seen.load(Ordering::SeqCst),
            0,
            "a pass-through request is not marked as holding a permit"
        );
        assert!(
            inner.tenants_seen.lock().expect("unpoisoned").is_empty(),
            "a pass-through request carries no tenant from the layer"
        );
        assert_eq!(
            controller.shed_total(),
            0,
            "no non-ingest path is charged against the ingest ceiling, even when it is full"
        );
    }

    /// `grpc_request_tenant` reuses the tenant the layer authenticated and
    /// resolves only on the direct-call path, refusing bad credentials there
    /// with the same status the layer uses.
    #[test]
    fn grpc_request_tenant_reuses_the_layer_tenant() {
        let resolver = resolver();
        let no_credentials = HeaderMap::new();

        let mut authenticated = tonic::Request::new(());
        authenticated
            .extensions_mut()
            .insert(TenantId::new("from-layer"));
        assert_eq!(
            grpc_request_tenant(resolver.as_ref(), &authenticated, &no_credentials)
                .expect("the layer's tenant"),
            TenantId::new("from-layer")
        );

        let mut bearer = HeaderMap::new();
        bearer.insert(
            "authorization",
            format!("Bearer {TOKEN}").parse().expect("ascii header"),
        );
        let direct = tonic::Request::new(());
        assert_eq!(
            grpc_request_tenant(resolver.as_ref(), &direct, &bearer).expect("resolved here"),
            TenantId::new("acme")
        );

        let status = grpc_request_tenant(resolver.as_ref(), &direct, &no_credentials)
            .expect_err("no credentials on the direct-call path");
        assert_eq!(status.code(), tonic::Code::Unauthenticated);
        assert_eq!(status.message(), "invalid or missing tenant credentials");
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
