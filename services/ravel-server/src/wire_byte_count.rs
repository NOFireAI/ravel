//! Wire-byte counting for gRPC ingest admission (issue #803).
//!
//! Every OTLP gRPC ingest handler used to charge layer-2 byte-rate admission
//! (docs/ingest.md admission section) on `request.get_ref().encoded_len()`:
//! an `O(message)` walk of the decoded protobuf tree, on every request, after
//! tonic had already decoded it. [`WireByteCountLayer`] replaces that with a
//! tower layer wrapping the tonic service stack: it swaps the request body
//! for a [`CountingBody`] that tallies `DATA` frame bytes into a
//! [`WireByteCounter`] as tonic's decoder reads them, and places that counter
//! in the request's extensions before the inner service (and eventually the
//! handler) runs. This also aligns the charged quantity with the HTTP
//! ingest path, which has always charged wire body bytes.
//!
//! Install [`WireByteCountLayer`] once on the `tonic::transport::Server`
//! builder that serves the gRPC listener; it wraps every service added to
//! that builder, unary and streaming alike. A unary handler reads the
//! completed count with [`wire_request_bytes`]. A streaming handler (OTAP)
//! reads the running counter directly with [`wire_byte_counter`] and tracks
//! its own previous read to charge each message's delta.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use tonic::body::Body as TonicBody;
use tonic::{Request, Status};
use tower::{Layer, Service};

/// Shared, cloneable handle to one request's wire-byte count. Placed in the
/// request's extensions by [`WireByteCountLayer`]; read back by the handler
/// via [`wire_request_bytes`] or [`wire_byte_counter`].
#[derive(Clone)]
pub struct WireByteCounter(Arc<Counter>);

struct Counter {
    bytes: AtomicU64,
    complete: AtomicBool,
}

impl WireByteCounter {
    fn new() -> Self {
        WireByteCounter(Arc::new(Counter {
            bytes: AtomicU64::new(0),
            complete: AtomicBool::new(false),
        }))
    }

    fn add(&self, n: u64) {
        self.0.bytes.fetch_add(n, Ordering::Relaxed);
    }

    fn mark_complete(&self) {
        self.0.complete.store(true, Ordering::Relaxed);
    }

    /// Cumulative `DATA`-frame bytes read from the request body so far. For a
    /// unary call this is the whole request once [`is_complete`](Self::is_complete)
    /// is true. For a stream it keeps growing as more messages arrive; a
    /// per-message caller charges the delta since its own last read (see
    /// `otap_grpc.rs`).
    pub fn bytes(&self) -> u64 {
        self.0.bytes.load(Ordering::Relaxed)
    }

    fn is_complete(&self) -> bool {
        self.0.complete.load(Ordering::Relaxed)
    }
}

/// Reads the completed wire-byte count for a unary gRPC request charged by
/// [`WireByteCountLayer`]. `Err` only if the layer was never installed on the
/// listener this request arrived on -- a wiring bug, not something a client
/// can trigger.
pub fn wire_request_bytes<T>(request: &Request<T>) -> Result<u64, Status> {
    let counter = wire_byte_counter(request)?;
    // A unary request body is fully buffered (and thus fully counted) before
    // tonic decodes the single message and hands it to the handler, so the
    // count read here must already be final.
    debug_assert!(
        counter.is_complete(),
        "wire-byte counter read before its unary request body finished"
    );
    Ok(counter.bytes())
}

/// Reads the [`WireByteCounter`] itself, for a streaming call that must
/// charge per-message deltas across the stream's lifetime rather than one
/// completed total.
pub fn wire_byte_counter<T>(request: &Request<T>) -> Result<WireByteCounter, Status> {
    request
        .extensions()
        .get::<WireByteCounter>()
        .cloned()
        .ok_or_else(|| {
            Status::internal(
                "ingest admission: wire-byte counter missing (WireByteCountLayer not installed \
                 on this gRPC listener)",
            )
        })
}

/// A [`tower::Layer`] that wraps every request reaching the gRPC listener so
/// admission can charge wire bytes without re-walking the decoded protobuf
/// tree (issue #803). Apply with `Server::builder().layer(WireByteCountLayer)`;
/// it wraps whichever services are `add_service`d after, in either order.
#[derive(Clone, Copy, Default)]
pub struct WireByteCountLayer;

impl<S> Layer<S> for WireByteCountLayer {
    type Service = WireByteCountService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        WireByteCountService { inner }
    }
}

#[derive(Clone)]
pub struct WireByteCountService<S> {
    inner: S,
}

impl<S, ResBody> Service<http::Request<TonicBody>> for WireByteCountService<S>
where
    S: Service<http::Request<TonicBody>, Response = http::Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<TonicBody>) -> Self::Future {
        let counter = WireByteCounter::new();
        let (mut parts, body) = req.into_parts();
        parts.extensions.insert(counter.clone());
        let counted = CountingBody {
            inner: body,
            counter,
        };
        let req = http::Request::from_parts(parts, TonicBody::new(counted));
        self.inner.call(req)
    }
}

/// Wraps a tonic request body, adding each `DATA` frame's byte length to a
/// shared [`WireByteCounter`] as the frame is pulled off the body, and
/// marking the counter complete once the body reports end-of-stream.
/// `tonic::body::Body` is `Unpin` (it boxes its inner body), which is what
/// lets [`Pin::get_mut`] below skip a pin-projection crate.
struct CountingBody {
    inner: TonicBody,
    counter: WireByteCounter,
}

impl Body for CountingBody {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_frame(cx);
        match &poll {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(data) = frame.data_ref() {
                    this.counter.add(data.len() as u64);
                }
            }
            Poll::Ready(None) => this.counter.mark_complete(),
            _ => {}
        }
        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
