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
//! Both leave an [`IngestPermitHeld`] marker carrying a clone of the
//! permit's release handle, and the resolved `TenantId`, in the request's
//! extensions. Every handler behind them takes its handle through
//! [`admit_grpc_request`] and its tenant through [`grpc_request_tenant`]
//! (gRPC), or reads both out of an `Extension` (HTTP), so one request never
//! charges the ceiling twice. The handler releases the permit through its
//! handle once its write is enqueued, before it waits for the strict ack
//! (ADR-2793 decision 4).
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
//!
//! A shed is still decided from the head, before the body is read. Once
//! decided, an authenticated HTTP/1 shed request's body is read and
//! discarded, never decoded or charged, before the 429 is sent
//! ([`refuse_from_head`], issue #2926): an HTTP/1 server that answers and
//! closes with request bytes unread can reach a pooled client as a reset
//! rather than the refusal. The drain is bounded in bytes, time and
//! concurrency; a refusal it does not drain carries `Connection: close` and
//! is counted by `ravel_ingest_head_refusal_undrained_total`.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use ravel_ingest::RequestRejection;
use ravel_query::http::TenantResolver;
use ravel_types::TenantId;
use tokio::sync::Semaphore;
use tonic::Status;
use tonic::body::Body as TonicBody;
use tower::{Layer, Service};

use crate::ingest_concurrency::{IngestConcurrencyController, IngestPermitHandle};

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
/// Carries a clone of the permit's release handle, never the bare permit:
/// extensions require `Clone`, and an `IngestPermit` moved into extensions
/// would be released wherever those extensions are dropped (tonic's
/// `Request::into_inner` drops them mid-handler), which is earlier than the
/// work the permit is meant to bound. The layer holds a clone of the release
/// handle for the request's lifetime, so dropping the extensions drops a
/// clone, and the permit goes back only when the handler releases it or the
/// request ends.
#[derive(Debug, Clone)]
pub struct IngestPermitHeld(pub IngestPermitHandle);

/// How long an admitted ingest request may take to deliver its whole body,
/// counted from the moment its head was admitted. A request that has not
/// finished by then is refused (503 with `Retry-After` on HTTP,
/// `DEADLINE_EXCEEDED` on gRPC) and its permit returns to the ceiling.
///
/// 30 s is Prometheus' default `remote_timeout`, the most generous default
/// deadline among the senders this ingests from (the OpenTelemetry
/// Collector's OTLP exporters default to 5 s), so a sender still inside its
/// own deadline is never cut off, and a 16 MiB body fits in it at about
/// 4.5 Mbit/s.
///
/// This caps how long one request holds its slot, not how many slots a
/// client can hold over time: a credentialed client that reopens each slot
/// as it is refused can keep the ceiling occupied at a trickle.
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

/// Seconds a sender is told to wait before resending an upload whose body
/// timed out.
const BODY_TIMEOUT_RETRY_AFTER_SECONDS: u64 = 1;

/// 503 with `Retry-After` for an HTTP ingest upload whose body did not arrive
/// within [`INGEST_BODY_READ_TIMEOUT`]. Not 408: Prometheus remote write and
/// the OTLP/HTTP exporter retry only 429 and 5xx (OTLP: 502, 503, 504), so a
/// 408 would drop the batch instead of having it resent.
fn body_timeout_response() -> Response {
    let mut response = (
        StatusCode::SERVICE_UNAVAILABLE,
        BodyReadTimedOut.to_string(),
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(&BODY_TIMEOUT_RETRY_AFTER_SECONDS.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

/// `DEADLINE_EXCEEDED` for a gRPC ingest export whose message did not arrive
/// within [`INGEST_BODY_READ_TIMEOUT`].
fn body_timeout_status() -> Status {
    Status::deadline_exceeded(BodyReadTimedOut.to_string())
}

/// Most body bytes, as sent on the wire and before any inflate, that
/// [`refuse_from_head`] reads and discards before answering. A request
/// declaring more in its `Content-Length` is not read at all; a chunked body
/// that passes this mid-drain is abandoned.
pub const HEAD_REFUSAL_DRAIN_MAX_BYTES: u64 = 1024 * 1024;

/// How long [`refuse_from_head`] waits for the rest of a refused request's
/// body before answering without it.
pub const HEAD_REFUSAL_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Process-wide count of head-refusal drains that may run at once. A refusal
/// that finds every slot taken is answered without a drain.
pub const HEAD_REFUSAL_DRAIN_MAX_CONCURRENT: usize = 64;

/// Why a head refusal on HTTP/1 was answered with the body left unread, the
/// `reason` label of `ravel_ingest_head_refusal_undrained_total`. The
/// variants are listed in the order [`refuse_from_head`] checks them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UndrainedReason {
    /// The credential on the head did not resolve to a tenant.
    Unauthenticated,
    /// The body is larger than [`HEAD_REFUSAL_DRAIN_MAX_BYTES`], by its
    /// `Content-Length` or as it arrived.
    OverCap,
    /// All [`HEAD_REFUSAL_DRAIN_MAX_CONCURRENT`] drain slots were taken.
    Busy,
    /// The body did not finish within [`HEAD_REFUSAL_DRAIN_TIMEOUT`].
    Timeout,
}

impl UndrainedReason {
    pub const ALL: [UndrainedReason; 4] = [
        UndrainedReason::Unauthenticated,
        UndrainedReason::OverCap,
        UndrainedReason::Busy,
        UndrainedReason::Timeout,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            UndrainedReason::Unauthenticated => "unauthenticated",
            UndrainedReason::OverCap => "over_cap",
            UndrainedReason::Busy => "busy",
            UndrainedReason::Timeout => "timeout",
        }
    }

    fn index(self) -> usize {
        match self {
            UndrainedReason::Unauthenticated => 0,
            UndrainedReason::OverCap => 1,
            UndrainedReason::Busy => 2,
            UndrainedReason::Timeout => 3,
        }
    }
}

/// The process-wide state of head-refusal drains: the slots bounding how many
/// run at once, and the count of refusals answered without one. Owned by the
/// one [`IngestConcurrencyController`] a process builds, so every listener and
/// both HTTP ingest surfaces share it.
pub struct HeadRefusalDrain {
    slots: Arc<Semaphore>,
    undrained: [AtomicU64; 4],
}

impl Default for HeadRefusalDrain {
    fn default() -> Self {
        HeadRefusalDrain {
            slots: Arc::new(Semaphore::new(HEAD_REFUSAL_DRAIN_MAX_CONCURRENT)),
            undrained: Default::default(),
        }
    }
}

impl HeadRefusalDrain {
    /// Head refusals answered without a drain for `reason` since process
    /// start, for the `/metrics` counter.
    pub fn undrained_total(&self, reason: UndrainedReason) -> u64 {
        self.undrained[reason.index()].load(Ordering::Relaxed)
    }

    fn undrained(&self, reason: UndrainedReason, refusal: Response) -> Response {
        self.undrained[reason.index()].fetch_add(1, Ordering::Relaxed);
        with_connection_close(refusal)
    }
}

/// Adds `Connection: close` to a refusal whose request body stays unread, so
/// an HTTP/1.1 client knows not to reuse the connection. It does not stop the
/// reset: closing a socket with unread bytes in its receive buffer sends RST,
/// which can reach the client before it reads this response.
fn with_connection_close(mut refusal: Response) -> Response {
    refusal
        .headers_mut()
        .insert(header::CONNECTION, HeaderValue::from_static("close"));
    refusal
}

/// Why a drain stopped before the body ended.
enum DrainStopped {
    OverCap,
    BodyError,
}

/// Reads and drops `body` until it ends. Data frames are counted and never
/// kept, inflated or decoded.
async fn discard_body(mut body: axum::body::Body) -> Result<(), DrainStopped> {
    let mut read: u64 = 0;
    while let Some(frame) = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await {
        let frame = frame.map_err(|_| DrainStopped::BodyError)?;
        if let Some(data) = frame.data_ref() {
            read = read.saturating_add(data.len() as u64);
            if read > HEAD_REFUSAL_DRAIN_MAX_BYTES {
                return Err(DrainStopped::OverCap);
            }
        }
    }
    Ok(())
}

/// Answers `refusal`, a response decided from `request`'s head, after reading
/// and discarding the body the client is sending, so a pooled HTTP/1.1 client
/// reads the refusal instead of a reset on a connection closed with its
/// upload unread. The body is never inflated, decoded or charged to any
/// budget.
///
/// Holds no admission permit while it reads. A caller that took this
/// request's in-flight permit must release it before calling this; any
/// [`IngestPermitHeld`] handle still in the request's extensions is released
/// here before the first byte is read, so a slow sender cannot hold a slot of
/// the ceiling through its own refusal.
///
/// Only an HTTP/1 request whose credential `authenticated` accepts is
/// drained, and the first of these to apply decides:
///
/// 1. HTTP/2 and later: answered as is, uncounted, since a stream reset
///    leaves the connection usable and a `Connection` header is invalid.
/// 2. Not authenticated: `unauthenticated`.
/// 3. `Expect: 100-continue`: answered with `Connection: close`, uncounted.
///    The client sends nothing until it sees `100 Continue`, which reading the
///    body would solicit, so there are no unread bytes to drain.
/// 4. `Content-Length` above [`HEAD_REFUSAL_DRAIN_MAX_BYTES`], with nothing
///    read: `over_cap`.
/// 5. No free drain slot ([`HEAD_REFUSAL_DRAIN_MAX_CONCURRENT`]): `busy`.
/// 6. While draining, the body passes the cap (`over_cap`) or does not end
///    within [`HEAD_REFUSAL_DRAIN_TIMEOUT`] (`timeout`).
///
/// Cases 2 and 4 to 6 add `Connection: close` and count one under
/// `ravel_ingest_head_refusal_undrained_total`. A body that fails mid-drain
/// (the client went away) also gets `Connection: close` and is not counted.
pub(crate) async fn refuse_from_head(
    drain: &HeadRefusalDrain,
    request: Request,
    refusal: Response,
    authenticated: impl FnOnce(&HeaderMap) -> bool,
) -> Response {
    if let Some(IngestPermitHeld(handle)) = request.extensions().get::<IngestPermitHeld>() {
        handle.release();
    }
    if !matches!(
        request.version(),
        http::Version::HTTP_10 | http::Version::HTTP_11
    ) {
        return refusal;
    }
    if !authenticated(request.headers()) {
        return drain.undrained(UndrainedReason::Unauthenticated, refusal);
    }
    if expects_continue(request.headers()) {
        return with_connection_close(refusal);
    }
    let body = request.into_body();
    if body.size_hint().lower() > HEAD_REFUSAL_DRAIN_MAX_BYTES {
        return drain.undrained(UndrainedReason::OverCap, refusal);
    }
    let Ok(_slot) = drain.slots.clone().try_acquire_owned() else {
        return drain.undrained(UndrainedReason::Busy, refusal);
    };
    match tokio::time::timeout(HEAD_REFUSAL_DRAIN_TIMEOUT, discard_body(body)).await {
        Ok(Ok(())) => refusal,
        Ok(Err(DrainStopped::OverCap)) => drain.undrained(UndrainedReason::OverCap, refusal),
        Ok(Err(DrainStopped::BodyError)) => with_connection_close(refusal),
        Err(_) => drain.undrained(UndrainedReason::Timeout, refusal),
    }
}

fn expects_continue(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::EXPECT)
        .iter()
        .any(|value| value.as_bytes().eq_ignore_ascii_case(b"100-continue"))
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
/// The permit is bound for the whole of `next.run(...)` unless the handler
/// releases it first, so it covers the body read and the decode, and the
/// layer's clone of its handle returns the slot on every exit path including
/// a client disconnect that cancels this future. A handler releases it once
/// its write is enqueued, so the strict ack wait holds no slot. The body read
/// is bounded by [`INGEST_BODY_READ_TIMEOUT`]: a body still incomplete then
/// fails the handler's body extractor, so the handler never runs, and the
/// request is answered 503 with `Retry-After`.
pub async fn admit_ingest_request<S: IngestAdmissionState>(
    State(state): State<Arc<S>>,
    request: Request,
    next: Next,
) -> Response {
    let permit = match state.ingest_concurrency().try_admit() {
        Ok(permit) => IngestPermitHandle::new(permit),
        Err(_) => {
            let resolver = state.tenant_resolver();
            return refuse_from_head(
                state.ingest_concurrency().head_refusal_drain(),
                request,
                ingest_concurrency_shed_response(),
                |headers| resolver.resolve(headers).is_ok(),
            )
            .await;
        }
    };

    let tenant = match state.tenant_resolver().resolve(request.headers()) {
        Ok(tenant) => tenant,
        Err(_) => {
            state.on_unauthorized();
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    let expired = Arc::new(AtomicBool::new(false));
    let mut request =
        request.map(|body| axum::body::Body::new(DeadlineBody::new(body, expired.clone())));
    let extensions = request.extensions_mut();
    extensions.insert(IngestPermitHeld(permit.clone()));
    extensions.insert(tenant);
    let response = next.run(request).await;
    drop(permit);
    if expired.load(Ordering::SeqCst) {
        return body_timeout_response();
    }
    response
}

/// The release handle for a gRPC ingest request's in-flight permit: a clone
/// of the one [`GrpcIngestAdmissionLayer`] took for this request, or, on the
/// direct-call path (a unit test that invokes a service method without the
/// listener's layer stack), a fresh handle over a permit taken here, which
/// the handler then owns alone. Read it before `request.into_inner()`.
///
/// Either way the handler calls [`IngestPermitHandle::release`] once its
/// write is enqueued; without that the permit goes back when the request
/// ends.
pub(crate) fn admit_grpc_request<T>(
    controller: &IngestConcurrencyController,
    request: &tonic::Request<T>,
) -> Result<IngestPermitHandle, Status> {
    if let Some(IngestPermitHeld(handle)) = request.extensions().get::<IngestPermitHeld>() {
        return Ok(handle.clone());
    }
    controller
        .try_admit()
        .map(IngestPermitHandle::new)
        .map_err(|_| ingest_concurrency_shed_status())
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
        .map_err(|_| unauthenticated_status())
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
        .map_err(|_| unauthenticated_status())
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
                Ok(permit) => Some(IngestPermitHandle::new(permit)),
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
        // Only a unary export took a permit; the OTAP stream passes on here.
        let Some(permit) = permit else {
            return Box::pin(self.inner.call(http::Request::from_parts(parts, body)));
        };
        parts.extensions.insert(IngestPermitHeld(permit.clone()));
        let expired = Arc::new(AtomicBool::new(false));
        let body = TonicBody::new(DeadlineBody::new(body, expired.clone()));
        let future = self.inner.call(http::Request::from_parts(parts, body));
        Box::pin(async move {
            // This clone keeps the permit through the body read and tonic's
            // decode, and returns it when this future finishes or is
            // cancelled if the handler never released it. The handler
            // releases it through its own clone once its write is enqueued.
            let response = future.await;
            drop(permit);
            let response = response?;
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
/// [`ingest_concurrency_shed_status`] the handlers return,
/// so a client sees one status and one message whichever layer refused it.
fn grpc_shed_response() -> http::Response<TonicBody> {
    ingest_concurrency_shed_status().into_http()
}

/// Turn a layer-2/layer-4 whole-request rejection into the ADR-0051 HTTP
/// response: 429 with `Retry-After` in whole seconds (rounded up, minimum
/// 1), the reason as the body.
pub(crate) fn admission_rejection_response(rejection: RequestRejection) -> Response {
    let mut response =
        (StatusCode::TOO_MANY_REQUESTS, rejection.reason.to_string()).into_response();
    let retry_after_secs = retry_after_seconds(rejection.retry_after_ns);
    if let Ok(value) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

pub(crate) fn retry_after_seconds(retry_after_ns: i64) -> u64 {
    let ns = retry_after_ns.max(0) as u64;
    ns.div_ceil(1_000_000_000).max(1)
}

/// A fixed `Retry-After` for the process-wide in-flight shed: the
/// controller tracks no per-caller refill time the way `RequestRejection`
/// does, and a slot can free up as soon as any in-flight request completes,
/// so a short fixed wait is the right shape here (no per-error estimate is
/// available, the same situation `remote_write::RETRY_AFTER_SECONDS`
/// documents).
const INGEST_CONCURRENCY_RETRY_AFTER_SECONDS: u64 = 1;

/// 429 for a request shed by the process-wide in-flight ceiling, before
/// tenant resolution or any per-signal admission check: no shard is touched
/// and no commit token is issued.
pub(crate) fn ingest_concurrency_shed_response() -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        "process in-flight ingest-request limit reached",
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(&INGEST_CONCURRENCY_RETRY_AFTER_SECONDS.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

/// The body of a strict write refused by the process-wide pending
/// strict-acknowledgement bound (`--max-pending-strict-acks`). Shared with the
/// gRPC status message so both transports say the same thing.
pub(crate) const PENDING_ACK_SHED_MESSAGE: &str =
    "process pending strict-acknowledgement limit reached";

/// 429 for a strict write refused by the pending strict-acknowledgement bound
/// (ADR-2793 decision 4): the same status and `Retry-After` as the in-flight
/// shed, since a held acknowledgement frees as soon as any strict write
/// resolves. The refusal comes inside `submit`, after decode and before the
/// byte charge, so no shard was touched and no commit token issued.
pub(crate) fn pending_ack_shed_response() -> Response {
    let mut response = (StatusCode::TOO_MANY_REQUESTS, PENDING_ACK_SHED_MESSAGE).into_response();
    if let Ok(value) = HeaderValue::from_str(&INGEST_CONCURRENCY_RETRY_AFTER_SECONDS.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

/// A fixed `Retry-After` for the process-wide ingest buffer byte budget shed
/// (ADR-0069): like the in-flight shed above, the budget tracks no per-caller
/// refill time -- a buffer slot frees as soon as any in-flight flush completes
/// -- so a short fixed wait is the right shape.
const INGEST_BUFFER_BUDGET_RETRY_AFTER_SECONDS: u64 = 1;

/// 429 for a request shed by the process-wide ingest buffer byte budget
/// (ADR-0069 decision 1): the write was rejected before any buffering, so no
/// shard was touched and no commit token issued. Same 429 + `Retry-After`
/// shape as the byte-rate rejection and the in-flight shed.
pub(crate) fn ingest_buffer_budget_shed_response() -> Response {
    let mut response = (
        StatusCode::TOO_MANY_REQUESTS,
        "ingest buffer byte budget reached",
    )
        .into_response();
    if let Ok(value) = HeaderValue::from_str(&INGEST_BUFFER_BUDGET_RETRY_AFTER_SECONDS.to_string())
    {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

/// 429 for a write refused because a shard it routes to has a flush deferred
/// for the whole flush deferral cap: refused before any buffering, like the
/// budget shed above, and answered with the same shape. `message` is the
/// write error's own text, which names the cap.
pub(crate) fn ingest_deferral_cap_response(message: String) -> Response {
    let mut response = (StatusCode::TOO_MANY_REQUESTS, message).into_response();
    if let Ok(value) = HeaderValue::from_str(&INGEST_BUFFER_BUDGET_RETRY_AFTER_SECONDS.to_string())
    {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

/// A layer-2/layer-4 whole-request rejection (ADR-0051 section 1) as a gRPC
/// status: `RESOURCE_EXHAUSTED`, no retry-info attached (the ADR's gRPC
/// column carries no header equivalent to HTTP's `Retry-After`). Shared by
/// every gRPC ingest service.
pub(crate) fn admission_rejection_status(rejection: RequestRejection) -> Status {
    Status::resource_exhausted(rejection.reason.to_string())
}

/// A shed over the process-wide in-flight ceiling, as
/// `RESOURCE_EXHAUSTED`: before tenant resolution or any per-signal
/// admission check, so a shed request touches no shard and carries no commit
/// token. Shared by every gRPC ingest service, the same discipline
/// [`admission_rejection_status`] follows.
pub(crate) fn ingest_concurrency_shed_status() -> Status {
    Status::resource_exhausted("process in-flight ingest-request limit reached")
}

/// A strict write refused by the process-wide pending strict-acknowledgement
/// bound, as `RESOURCE_EXHAUSTED` with the HTTP body's message. Shared by
/// every gRPC ingest service.
pub(crate) fn pending_ack_shed_status() -> Status {
    Status::resource_exhausted(PENDING_ACK_SHED_MESSAGE)
}

/// A request whose tenant credential did not resolve, as `UNAUTHENTICATED`.
/// Shared by `GrpcIngestAdmissionLayer` and every gRPC ingest handler, so a
/// client sees one status and one message whichever of them refused it.
pub(crate) fn unauthenticated_status() -> Status {
    Status::unauthenticated("invalid or missing tenant credentials")
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use bytes::Bytes;
    use http_body::{Body, Frame, SizeHint};
    use ravel_ingest::{LogWriteError, SpanWriteError, WriteError};
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

    /// An ingest path whose service this listener does not register (a
    /// `--mode query` process, or OTAP without `--otap`) passes through to
    /// tonic's router, which answers UNIMPLEMENTED, rather than being shed or
    /// refused for credentials here.
    ///
    /// Non-vacuity: ignore `GrpcIngestServices` in `call` and the
    /// credential-less requests reach `UnreachableInner`'s shed or
    /// UNAUTHENTICATED arm instead of the inner service.
    #[tokio::test]
    async fn unregistered_ingest_paths_pass_through_untouched() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let _held = controller.try_admit().expect("the only permit");

        let inner = CountingInner::default();
        let mut service = GrpcIngestAdmissionLayer::new(
            controller.clone(),
            resolver(),
            GrpcIngestServices {
                otlp: false,
                otap: false,
            },
        )
        .layer(inner.clone());

        for path in OTLP_EXPORT_PATHS
            .into_iter()
            .chain([OTAP_ARROW_METRICS_PATH])
        {
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
            assert!(
                Status::from_header_map(response.headers()).is_none(),
                "{path} carries no refusal from the layer"
            );
        }

        assert_eq!(inner.calls.load(Ordering::SeqCst), 4);
        assert_eq!(inner.held_marker_seen.load(Ordering::SeqCst), 0);
        assert!(inner.tenants_seen.lock().expect("unpoisoned").is_empty());
        assert_eq!(controller.shed_total(), 0);
    }

    /// Longer than [`INGEST_BODY_READ_TIMEOUT`], so a test that waits this
    /// long without an answer has shown the bound is missing. Under paused
    /// time neither costs wall-clock time.
    const TEST_DEADLINE: Duration = Duration::from_secs(2 * INGEST_BODY_READ_TIMEOUT.as_secs());

    /// A body that delivers one chunk of a large declared length and then
    /// stalls forever: a client trickling its upload.
    struct StalledBody {
        sent_first_chunk: bool,
    }

    impl StalledBody {
        fn new() -> Self {
            StalledBody {
                sent_first_chunk: false,
            }
        }
    }

    impl Body for StalledBody {
        type Data = Bytes;
        type Error = Status;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
            if self.sent_first_chunk {
                return Poll::Pending;
            }
            self.sent_first_chunk = true;
            Poll::Ready(Some(Ok(Frame::data(Bytes::from_static(b"x")))))
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(1024 * 1024)
        }
    }

    struct HttpTestState {
        controller: Arc<IngestConcurrencyController>,
        resolver: Arc<dyn TenantResolver>,
    }

    impl IngestAdmissionState for HttpTestState {
        fn ingest_concurrency(&self) -> &Arc<IngestConcurrencyController> {
            &self.controller
        }

        fn tenant_resolver(&self) -> &Arc<dyn TenantResolver> {
            &self.resolver
        }
    }

    /// An ingest route shaped like the real ones: the handler takes the body
    /// through the `Bytes` extractor, behind the admission middleware.
    fn http_router(controller: &Arc<IngestConcurrencyController>) -> axum::Router {
        let state = Arc::new(HttpTestState {
            controller: controller.clone(),
            resolver: resolver(),
        });
        axum::Router::new()
            .route(
                "/v1/metrics",
                axum::routing::post(|body: Bytes| async move { body.len().to_string() }),
            )
            .route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                admit_ingest_request::<HttpTestState>,
            ))
            .with_state(state)
    }

    fn http_upload(body: axum::body::Body) -> Request {
        http::Request::builder()
            .method(http::Method::POST)
            .uri("/v1/metrics")
            .header("authorization", format!("Bearer {TOKEN}"))
            .body(body)
            .expect("request builds")
    }

    /// An authenticated HTTP upload that sends its head and one chunk and
    /// then stalls is answered 503 with `Retry-After` (a status both
    /// Prometheus remote write and the OTLP/HTTP exporter retry) once
    /// [`INGEST_BODY_READ_TIMEOUT`] passes,
    /// and the permit it held is released: under a ceiling of 1 the next
    /// upload is admitted and nothing is shed.
    ///
    /// Non-vacuity: without the `DeadlineBody` around the request body the
    /// stalled upload is still unanswered at `TEST_DEADLINE`.
    #[tokio::test(start_paused = true)]
    async fn stalled_http_upload_is_refused_503_and_releases_its_permit() {
        use tower::ServiceExt;

        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let app = http_router(&controller);

        let started = tokio::time::Instant::now();
        let response = tokio::time::timeout(
            TEST_DEADLINE,
            app.clone()
                .oneshot(http_upload(axum::body::Body::new(StalledBody::new()))),
        )
        .await
        .expect("the stalled upload is answered within the bound, not left hanging")
        .expect("infallible router");
        let waited = started.elapsed();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER),
            Some(&HeaderValue::from_static("1")),
            "a timed-out upload tells the sender when to resend"
        );
        // hygiene-allow: wall-clock -- `start_paused` makes this tokio's
        // virtual clock, which moves only when a timer fires.
        assert!(
            waited >= INGEST_BODY_READ_TIMEOUT && waited < TEST_DEADLINE,
            "refused at the bound, after {waited:?}"
        );

        let response = app
            .oneshot(http_upload(axum::body::Body::from("payload")))
            .await
            .expect("infallible router");
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "the stalled upload's permit was released"
        );
        assert_eq!(controller.shed_total(), 0, "nothing was shed");
    }

    /// A chunked body with no length in its head: `chunks` frames of 64 KiB,
    /// counting each frame it hands out in `polled`.
    struct ChunkedBody {
        chunks: usize,
        polled: Arc<AtomicUsize>,
    }

    impl Body for ChunkedBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
            if self.chunks == 0 {
                return Poll::Ready(None);
            }
            self.chunks -= 1;
            self.polled.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Some(Ok(Frame::data(Bytes::from(vec![0u8; 64 * 1024])))))
        }
    }

    /// The undrained-refusal counts in [`UndrainedReason::ALL`] order:
    /// unauthenticated, over_cap, busy, timeout.
    fn undrained_counts(controller: &IngestConcurrencyController) -> [u64; 4] {
        UndrainedReason::ALL.map(|reason| controller.head_refusal_drain().undrained_total(reason))
    }

    /// The shed refusal, drained or not: same status, `Retry-After` and body,
    /// with `Connection: close` only when `connection_close`.
    async fn assert_shed(response: Response, connection_close: bool) {
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers().get(header::RETRY_AFTER),
            Some(&HeaderValue::from_static("1"))
        );
        let expected = connection_close.then(|| HeaderValue::from_static("close"));
        assert_eq!(
            response.headers().get(header::CONNECTION),
            expected.as_ref()
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("refusal body reads");
        assert_eq!(&body[..], b"process in-flight ingest-request limit reached");
    }

    /// An authenticated shed whose body fits under the drain cap, sized or
    /// chunked, is read to its end and answered with the plain 429: no
    /// `Connection: close`, and no undrained reason counted.
    #[tokio::test(start_paused = true)]
    async fn a_drained_shed_is_answered_without_connection_close() {
        use tower::ServiceExt;

        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(0));
        let app = http_router(&controller);

        let response = tokio::time::timeout(
            TEST_DEADLINE,
            app.clone()
                .oneshot(http_upload(axum::body::Body::from(vec![0u8; 512 * 1024]))),
        )
        .await
        .expect("answered")
        .expect("infallible router");
        assert_shed(response, false).await;

        let polled = Arc::new(AtomicUsize::new(0));
        let response = tokio::time::timeout(
            TEST_DEADLINE,
            app.oneshot(http_upload(axum::body::Body::new(ChunkedBody {
                chunks: 8,
                polled: polled.clone(),
            }))),
        )
        .await
        .expect("answered")
        .expect("infallible router");
        assert_shed(response, false).await;
        assert_eq!(polled.load(Ordering::SeqCst), 8, "read to its end");

        assert_eq!(undrained_counts(&controller), [0, 0, 0, 0]);
        assert_eq!(controller.shed_total(), 2);
    }

    /// A chunked shed body is read only until it passes the 1 MiB cap: the
    /// 17th 64 KiB frame of 32 crosses it, nothing after that is read, and the
    /// refusal carries `Connection: close`, counted under `over_cap`.
    #[tokio::test(start_paused = true)]
    async fn a_chunked_shed_body_past_the_cap_stops_the_drain() {
        use tower::ServiceExt;

        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(0));
        let polled = Arc::new(AtomicUsize::new(0));
        let response = tokio::time::timeout(
            TEST_DEADLINE,
            http_router(&controller).oneshot(http_upload(axum::body::Body::new(ChunkedBody {
                chunks: 32,
                polled: polled.clone(),
            }))),
        )
        .await
        .expect("answered")
        .expect("infallible router");

        assert_shed(response, true).await;
        assert_eq!(
            polled.load(Ordering::SeqCst) as u64,
            HEAD_REFUSAL_DRAIN_MAX_BYTES / (64 * 1024) + 1
        );
        assert_eq!(undrained_counts(&controller), [0, 1, 0, 0]);
    }

    /// Issue #2926, case E: with every drain slot taken, an authenticated
    /// shed is answered at once with `Connection: close` and counted under
    /// `busy`; the slot is taken with `try_acquire`, so the refusal never
    /// waits for one. A body above the cap is still named `over_cap`, the
    /// earlier reason. Once the slots are free again the next shed drains.
    #[tokio::test(start_paused = true)]
    async fn a_shed_with_no_drain_slot_free_is_answered_busy() {
        use tower::ServiceExt;

        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(0));
        let app = http_router(&controller);
        let held = controller
            .head_refusal_drain()
            .slots
            .clone()
            .try_acquire_many_owned(HEAD_REFUSAL_DRAIN_MAX_CONCURRENT as u32)
            .expect("every drain slot is free");

        let response = tokio::time::timeout(
            TEST_DEADLINE,
            app.clone()
                .oneshot(http_upload(axum::body::Body::from(vec![0u8; 1024]))),
        )
        .await
        .expect("a shed never waits for a drain slot")
        .expect("infallible router");
        assert_shed(response, true).await;
        assert_eq!(undrained_counts(&controller), [0, 0, 1, 0]);

        // A body above the cap is named `over_cap`, which is checked first.
        let response = app
            .clone()
            .oneshot(http_upload(axum::body::Body::from(vec![
                0u8;
                2 * HEAD_REFUSAL_DRAIN_MAX_BYTES
                    as usize
            ])))
            .await
            .expect("infallible router");
        assert_shed(response, true).await;
        assert_eq!(undrained_counts(&controller), [0, 1, 1, 0]);

        drop(held);
        let response = app
            .oneshot(http_upload(axum::body::Body::from(vec![0u8; 1024])))
            .await
            .expect("infallible router");
        assert_shed(response, false).await;
        assert_eq!(undrained_counts(&controller), [0, 1, 1, 0]);
    }

    /// An HTTP/2 shed refuses one stream and keeps the connection, so it is
    /// answered without a drain, without a `Connection` header (which HTTP/2
    /// forbids), and is not counted. A body that would stall a drain shows
    /// none was attempted.
    #[tokio::test(start_paused = true)]
    async fn an_http2_shed_is_answered_without_a_drain() {
        use tower::ServiceExt;

        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(0));
        let mut request = http_upload(axum::body::Body::new(StalledBody::new()));
        *request.version_mut() = http::Version::HTTP_2;

        let started = tokio::time::Instant::now();
        let response =
            tokio::time::timeout(TEST_DEADLINE, http_router(&controller).oneshot(request))
                .await
                .expect("answered")
                .expect("infallible router");

        assert_shed(response, false).await;
        // hygiene-allow: wall-clock -- `start_paused` makes this tokio's
        // virtual clock, which moves only when a timer fires.
        assert!(started.elapsed() < HEAD_REFUSAL_DRAIN_TIMEOUT);
        assert_eq!(undrained_counts(&controller), [0, 0, 0, 0]);
    }

    /// A request waiting on `Expect: 100-continue` has sent no body, and hyper
    /// sends the 100 only when the body is polled, so the shed is answered
    /// without polling it, with `Connection: close`, and not counted.
    #[tokio::test(start_paused = true)]
    async fn an_expect_continue_shed_is_answered_without_a_drain() {
        use tower::ServiceExt;

        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(0));
        let mut request = http_upload(axum::body::Body::new(StalledBody::new()));
        request
            .headers_mut()
            .insert(header::EXPECT, HeaderValue::from_static("100-continue"));

        let started = tokio::time::Instant::now();
        let response =
            tokio::time::timeout(TEST_DEADLINE, http_router(&controller).oneshot(request))
                .await
                .expect("answered")
                .expect("infallible router");

        assert_shed(response, true).await;
        // hygiene-allow: wall-clock -- `start_paused` makes this tokio's
        // virtual clock, which moves only when a timer fires.
        assert!(started.elapsed() < HEAD_REFUSAL_DRAIN_TIMEOUT);
        assert_eq!(undrained_counts(&controller), [0, 0, 0, 0]);
    }

    /// The credential is checked before `Expect: 100-continue`, so an
    /// unauthenticated request waiting on it is counted as `unauthenticated`
    /// like any other unauthenticated shed.
    #[tokio::test(start_paused = true)]
    async fn an_unauthenticated_expect_continue_shed_counts_as_unauthenticated() {
        use tower::ServiceExt;

        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(0));
        let request = http::Request::builder()
            .method(http::Method::POST)
            .uri("/v1/metrics")
            .header(header::EXPECT, "100-continue")
            .body(axum::body::Body::new(StalledBody::new()))
            .expect("request builds");

        let response =
            tokio::time::timeout(TEST_DEADLINE, http_router(&controller).oneshot(request))
                .await
                .expect("answered")
                .expect("infallible router");

        assert_shed(response, true).await;
        let drain = controller.head_refusal_drain();
        assert_eq!(drain.undrained_total(UndrainedReason::Unauthenticated), 1);
        assert_eq!(undrained_counts(&controller).iter().sum::<u64>(), 1);
    }

    /// A caller that refuses a request still holding its permit has the
    /// permit released before the drain starts: under a ceiling of 1 a new
    /// request is admitted while the stalled body is still being drained,
    /// and the drain then ends at its timeout, counted under `timeout`.
    #[tokio::test(start_paused = true)]
    async fn the_drain_runs_with_no_permit_held() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let handle =
            IngestPermitHandle::new(controller.try_admit().expect("the only permit is free"));
        let mut request = http_upload(axum::body::Body::new(StalledBody::new()));
        request
            .extensions_mut()
            .insert(IngestPermitHeld(handle.clone()));

        let started = tokio::time::Instant::now();
        let refusing = tokio::spawn({
            let controller = controller.clone();
            async move {
                refuse_from_head(
                    controller.head_refusal_drain(),
                    request,
                    ingest_concurrency_shed_response(),
                    |_| true,
                )
                .await
            }
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!refusing.is_finished(), "the drain is still waiting");
        assert!(!handle.is_held());
        drop(
            controller
                .try_admit()
                .expect("the refused request's permit was released before its drain"),
        );

        let response = refusing.await.expect("refusal task completes");
        // hygiene-allow: wall-clock -- `start_paused` makes this tokio's
        // virtual clock, which moves only when a timer fires.
        assert!(started.elapsed() >= HEAD_REFUSAL_DRAIN_TIMEOUT);
        assert_shed(response, true).await;
        assert_eq!(undrained_counts(&controller), [0, 0, 0, 1]);
    }

    /// Stands in for tonic's unary codec: reads the request body to its end
    /// and answers OK, or answers the body error the way tonic does, as a
    /// status other than DEADLINE_EXCEEDED.
    #[derive(Clone)]
    struct DrainingInner;

    impl Service<http::Request<TonicBody>> for DrainingInner {
        type Response = http::Response<TonicBody>;
        type Error = std::convert::Infallible;
        type Future =
            Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'static>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
            Box::pin(async move {
                let mut body = std::pin::pin!(req.into_body());
                loop {
                    match std::future::poll_fn(|cx| body.as_mut().poll_frame(cx)).await {
                        None => return Ok(http::Response::new(TonicBody::empty())),
                        Some(Ok(_)) => {}
                        Some(Err(error)) => {
                            return Ok(Status::unknown(error.to_string()).into_http());
                        }
                    }
                }
            })
        }
    }

    fn grpc_export(body: TonicBody) -> http::Request<TonicBody> {
        http::Request::builder()
            .method(http::Method::POST)
            .uri(EXPORT_PATH)
            .header("authorization", format!("Bearer {TOKEN}"))
            .body(body)
            .expect("request builds")
    }

    /// The gRPC counterpart: an authenticated unary export whose message
    /// stalls is refused DEADLINE_EXCEEDED once [`INGEST_BODY_READ_TIMEOUT`]
    /// passes, and its permit is released for the next export.
    ///
    /// Non-vacuity: without the `DeadlineBody` the stalled export is still
    /// unanswered at `TEST_DEADLINE`; without the `expired` check the status
    /// is `DrainingInner`'s UNKNOWN.
    #[tokio::test(start_paused = true)]
    async fn stalled_grpc_export_is_refused_deadline_exceeded_and_releases_its_permit() {
        let controller = IngestConcurrencyController::shared(IngestConcurrencyLimit::Bounded(1));
        let mut service = layer(&controller).layer(DrainingInner);

        let started = tokio::time::Instant::now();
        let response = tokio::time::timeout(
            TEST_DEADLINE,
            service.call(grpc_export(TonicBody::new(StalledBody::new()))),
        )
        .await
        .expect("the stalled export is answered within the bound, not left hanging")
        .expect("infallible inner");
        let waited = started.elapsed();

        assert_eq!(
            grpc_status_of(&response),
            (
                tonic::Code::DeadlineExceeded,
                "request body not received within 30s".to_string()
            )
        );
        // hygiene-allow: wall-clock -- `start_paused` makes this tokio's
        // virtual clock, which moves only when a timer fires.
        assert!(
            waited >= INGEST_BODY_READ_TIMEOUT && waited < TEST_DEADLINE,
            "refused at the bound, after {waited:?}"
        );

        let response = service
            .call(grpc_export(TonicBody::new(axum::body::Body::from(
                "message",
            ))))
            .await
            .expect("infallible inner");
        assert!(
            Status::from_header_map(response.headers()).is_none(),
            "the stalled export's permit was released"
        );
        assert_eq!(controller.shed_total(), 0, "nothing was shed");
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
        marked
            .extensions_mut()
            .insert(IngestPermitHeld(IngestPermitHandle::default()));
        let handle = admit_grpc_request(&controller, &marked).expect("marked request is admitted");
        assert!(
            !handle.is_held(),
            "a request the layer admitted must not take a second permit"
        );

        let unmarked = tonic::Request::new(());
        let handle =
            admit_grpc_request(&controller, &unmarked).expect("the direct call takes a permit");
        assert!(handle.is_held());

        let over = tonic::Request::new(());
        let status = admit_grpc_request(&controller, &over)
            .expect_err("the ceiling of 1 is now full for a direct call");
        assert_eq!(status.code(), tonic::Code::ResourceExhausted);
        assert_eq!(
            status.message(),
            "process in-flight ingest-request limit reached"
        );
    }

    /// A write refused at the flush deferral cap is a 429 with `Retry-After`
    /// whose body is the write error's text, which names the cap.
    #[test]
    fn a_deferral_cap_refusal_is_429_with_retry_after() {
        let response = ingest_deferral_cap_response(WriteError::DeferralCapReached.to_string());
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(response.headers().contains_key(header::RETRY_AFTER));
        assert!(
            LogWriteError::DeferralCapReached
                .to_string()
                .contains("deferral cap")
        );
        assert!(
            SpanWriteError::DeferralCapReached
                .to_string()
                .contains("deferral cap")
        );
    }
}
