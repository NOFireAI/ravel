//! Wire-byte counting for gRPC ingest admission (issue #803).
//!
//! Every OTLP gRPC ingest handler used to charge layer-2 byte-rate admission
//! (docs/ingest.md admission section) on `request.get_ref().encoded_len()`:
//! an `O(message)` walk of the decoded protobuf tree, on every request, after
//! tonic had already decoded it. [`WireByteCountLayer`] replaces that with a
//! tower layer wrapping the tonic service stack: it swaps the request body
//! for a [`CountingBody`] that parses gRPC's length-delimited message
//! framing directly off the wire bytes as tonic's decoder reads them, and
//! places the resulting [`WireByteCounter`] in the request's extensions
//! before the inner service (and eventually the handler) runs. This also
//! aligns the charged quantity with the HTTP ingest path, which has always
//! charged wire body bytes.
//!
//! Parsing frames at the body level rather than reading a single running
//! total matters for a streaming call (OTAP): the underlying transport can
//! (and over a fast loopback connection routinely does) hand a body's
//! `poll_frame` several messages' worth of bytes in one chunk, ahead of
//! tonic's decoder actually consuming them message by message. A single
//! cumulative byte count read at arbitrary times would then attribute a
//! later message's bytes to an earlier charge. Detecting each message's
//! frame boundary (a 1-byte compression flag plus a 4-byte big-endian
//! length, per the gRPC wire format) as bytes arrive sidesteps that: each
//! completed frame's total length (header included) is queued in arrival
//! order, and a handler charges exactly one queue entry per message it
//! decodes, in the same order.
//!
//! Install [`WireByteCountLayer`] once on the `tonic::transport::Server`
//! builder that serves the gRPC listener; it wraps every service added to
//! that builder, unary and streaming alike. A unary handler reads its one
//! message's charge with [`wire_request_bytes`]. A streaming handler (OTAP)
//! reads the [`WireByteCounter`] itself with [`wire_byte_counter`] and pops
//! one entry per decoded message.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use parking_lot::Mutex;
use tonic::body::Body as TonicBody;
use tonic::{Request, Status};
use tower::{Layer, Service};

/// Shared, cloneable handle to the completed gRPC message frames read off one
/// request's (or stream's) body so far, in arrival order. Placed in the
/// request's extensions by [`WireByteCountLayer`]; read back by the handler
/// via [`wire_request_bytes`] or [`wire_byte_counter`].
#[derive(Clone)]
pub struct WireByteCounter(Arc<Mutex<VecDeque<u64>>>);

impl WireByteCounter {
    fn new() -> Self {
        WireByteCounter(Arc::new(Mutex::new(VecDeque::new())))
    }

    fn push_message(&self, total_bytes: u64) {
        self.0.lock().push_back(total_bytes);
    }

    /// Pops the next completed message's total wire length (its 5-byte gRPC
    /// frame header plus payload), in the order frames completed on the
    /// wire. `None` if no complete frame has been counted yet.
    pub fn pop_message_bytes(&self) -> Option<u64> {
        self.0.lock().pop_front()
    }
}

/// Reads the wire-byte count for a unary gRPC request's one message, charged
/// by [`WireByteCountLayer`]. `Err` if the layer was never installed on the
/// listener this request arrived on (a wiring bug, not something a client
/// can trigger), or if, contrary to the gRPC wire format's guarantee of
/// exactly one message frame per unary call, no complete frame was counted.
pub fn wire_request_bytes<T>(request: &Request<T>) -> Result<u64, Status> {
    let counter = wire_byte_counter(request)?;
    let bytes = counter.pop_message_bytes();
    // Tonic already decoded this request's one message before calling the
    // handler, which means `CountingBody` -- sitting underneath that same
    // decode, watching the same bytes -- must already have a completed frame
    // queued. A `None` here would mean the two disagree about how many bytes
    // make up this message.
    debug_assert!(
        bytes.is_some(),
        "unary gRPC request decoded but WireByteCountLayer counted no complete message frame"
    );
    bytes.ok_or_else(|| {
        Status::internal("ingest admission: wire-byte count unavailable for this request")
    })
}

/// Reads the [`WireByteCounter`] itself, for a streaming call that charges
/// one queue entry per message it decodes rather than a single completed
/// total.
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
            parser: FrameParser::new(),
        };
        let req = http::Request::from_parts(parts, TonicBody::new(counted));
        self.inner.call(req)
    }
}

/// A gRPC-over-HTTP/2 length-delimited message frame is a 1-byte compression
/// flag followed by a 4-byte big-endian payload length, then that many
/// payload bytes (<https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md>).
/// `FrameParser` walks `DATA` frame bytes through that shape without
/// buffering or decoding the payload itself, so it can tell a body wrapper
/// exactly when one message's bytes are complete regardless of how the
/// underlying transport chunked them.
struct FrameParser {
    state: FrameState,
}

enum FrameState {
    Header { buf: [u8; 5], filled: u8 },
    Payload { remaining: u32, total: u64 },
}

impl FrameParser {
    fn new() -> Self {
        FrameParser {
            state: FrameState::Header {
                buf: [0; 5],
                filled: 0,
            },
        }
    }

    /// Feeds `data` through the frame state machine, pushing each frame's
    /// total length (header plus payload) to `counter` the moment it
    /// completes -- possibly several times in one call, if `data` spans more
    /// than one message.
    fn feed(&mut self, mut data: &[u8], counter: &WireByteCounter) {
        loop {
            match &mut self.state {
                FrameState::Header { buf, filled } => {
                    if data.is_empty() {
                        return;
                    }
                    let need = 5 - usize::from(*filled);
                    let take = need.min(data.len());
                    buf[usize::from(*filled)..usize::from(*filled) + take]
                        .copy_from_slice(&data[..take]);
                    *filled += take as u8;
                    data = &data[take..];
                    if usize::from(*filled) < 5 {
                        return;
                    }
                    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
                    self.state = FrameState::Payload {
                        remaining: len,
                        total: 5 + u64::from(len),
                    };
                }
                FrameState::Payload { remaining, total } => {
                    if *remaining == 0 {
                        counter.push_message(*total);
                        self.state = FrameState::Header {
                            buf: [0; 5],
                            filled: 0,
                        };
                        continue;
                    }
                    if data.is_empty() {
                        return;
                    }
                    let take = (*remaining as usize).min(data.len());
                    *remaining -= take as u32;
                    data = &data[take..];
                }
            }
        }
    }
}

/// Wraps a tonic request body, feeding each `DATA` frame's bytes through a
/// [`FrameParser`] and pushing every completed gRPC message frame's length to
/// a shared [`WireByteCounter`]. `tonic::body::Body` is `Unpin` (it boxes its
/// inner body), which is what lets [`Pin::get_mut`] below skip a
/// pin-projection crate.
struct CountingBody {
    inner: TonicBody,
    counter: WireByteCounter,
    parser: FrameParser,
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
        if let Poll::Ready(Some(Ok(frame))) = &poll
            && let Some(data) = frame.data_ref()
        {
            this.parser.feed(data, &this.counter);
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
