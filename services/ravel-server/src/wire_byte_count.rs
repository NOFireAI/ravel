//! Wire-byte counting for gRPC ingest admission.
//!
//! Every OTLP gRPC ingest handler used to charge layer-2 byte-rate admission
//! (docs/ingest.md admission section) on `request.get_ref().encoded_len()`:
//! an `O(message)` walk of the decoded protobuf tree, on every request, after
//! tonic had already decoded it. [`WireByteCountLayer`] replaces that with a
//! tower layer wrapping the tonic service stack: it swaps the request body
//! for a [`CountingBody`] that parses gRPC's length-delimited message
//! framing directly off the wire bytes as tonic's decoder reads them, and
//! places the resulting [`WireByteCounter`] in the request's extensions
//! before the inner service (and eventually the handler) runs.
//!
//! Since ADR-0084 the charged quantity aligns with the HTTP ingest path on the
//! *decompressed* size, not the wire size. An uncompressed frame is charged its
//! wire bytes exactly as before; a frame whose 1-byte compression flag is set
//! is charged the decoded message's size instead (via [`wire_request_bytes`]'s
//! `encoded_len` of the decoded protobuf), so a tenant that gzips its gRPC
//! export and one that does not are charged the same for identical telemetry.
//! The parser already reads that flag per frame, so the basis is available
//! where the charge is made.
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
use prost::Message;
use tonic::body::Body as TonicBody;
use tonic::{Request, Status};
use tower::{Layer, Service};

/// One completed gRPC message frame's admission accounting: whether its 1-byte
/// compression flag was set, and its total wire length (the 5-byte gRPC frame
/// header plus payload). For a compressed frame the wire length is the
/// *compressed* size, which is not what the byte rate charges (ADR-0084
/// decision 4); the flag is what lets [`wire_request_bytes`] switch the charge
/// to the decoded message size instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FrameCharge {
    compressed: bool,
    wire_bytes: u64,
}

/// Shared, cloneable handle to the completed gRPC message frames read off one
/// request's (or stream's) body so far, in arrival order. Placed in the
/// request's extensions by [`WireByteCountLayer`]; read back by the handler
/// via [`wire_request_bytes`] or [`wire_byte_counter`].
#[derive(Clone)]
pub struct WireByteCounter(Arc<Mutex<VecDeque<FrameCharge>>>);

impl WireByteCounter {
    fn new() -> Self {
        WireByteCounter(Arc::new(Mutex::new(VecDeque::new())))
    }

    fn push_message(&self, compressed: bool, total_bytes: u64) {
        self.0.lock().push_back(FrameCharge {
            compressed,
            wire_bytes: total_bytes,
        });
    }

    /// Pops the next completed frame's accounting, in the order frames
    /// completed on the wire. `None` if no complete frame has been counted yet.
    fn pop_frame(&self) -> Option<FrameCharge> {
        self.0.lock().pop_front()
    }

    /// Pops the next completed message's total wire length (its 5-byte gRPC
    /// frame header plus payload), in the order frames completed on the wire.
    /// `None` if no complete frame has been counted yet. Used by the streaming
    /// (OTAP) path, which does not enable `accept_compressed` and so never sees
    /// a compressed frame; the unary OTLP path uses [`wire_request_bytes`],
    /// which honors the compression flag.
    pub fn pop_message_bytes(&self) -> Option<u64> {
        self.pop_frame().map(|frame| frame.wire_bytes)
    }
}

/// The admission accounting for a unary gRPC request's one message
/// (ADR-0084 decision 4/5): what to charge the byte rate, and the wire size the
/// tenant actually sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestCharge {
    /// Bytes to charge the byte-rate bucket: the decoded message size for a
    /// compressed frame, the wire length otherwise. Equal to `wire_bytes` for
    /// an uncompressed frame.
    pub charge_bytes: u64,
    /// The frame's total wire length (the compressed size when the frame's
    /// compression flag was set), recorded per tenant for
    /// `ravel_ingest_wire_bytes_total`.
    pub wire_bytes: u64,
}

/// The byte-rate accounting for a unary gRPC request's one message, from the
/// frame [`WireByteCountLayer`] counted (ADR-0084 decisions 4, 5).
///
/// For an uncompressed frame the charge is the wire length exactly as before.
/// For a frame whose compression flag is set, the wire length is the
/// *compressed* size, which would charge a compressing tenant less than an
/// uncompressing one for identical telemetry; so the charge is the decoded
/// message's size (`request.get_ref().encoded_len()`) instead. Tonic has
/// already decoded the message by the time the handler runs, so the decoded
/// value is in hand with no re-decode.
///
/// `Err` if the layer was never installed on the listener this request arrived
/// on (a wiring bug, not something a client can trigger), or if, contrary to
/// the gRPC wire format's guarantee of exactly one message frame per unary
/// call, no complete frame was counted.
pub fn wire_request_bytes<T: Message>(request: &Request<T>) -> Result<RequestCharge, Status> {
    let counter = wire_byte_counter(request)?;
    let frame = counter.pop_frame();
    // Tonic already decoded this request's one message before calling the
    // handler, which means `CountingBody` -- sitting underneath that same
    // decode, watching the same bytes -- must already have a completed frame
    // queued. A `None` here would mean the two disagree about how many bytes
    // make up this message.
    debug_assert!(
        frame.is_some(),
        "unary gRPC request decoded but WireByteCountLayer counted no complete message frame"
    );
    let frame = frame.ok_or_else(|| {
        Status::internal("ingest admission: wire-byte count unavailable for this request")
    })?;
    let charge_bytes = if frame.compressed {
        request.get_ref().encoded_len() as u64
    } else {
        frame.wire_bytes
    };
    Ok(RequestCharge {
        charge_bytes,
        wire_bytes: frame.wire_bytes,
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
/// tree. Apply with `Server::builder().layer(WireByteCountLayer)`;
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
    Header {
        buf: [u8; 5],
        filled: u8,
    },
    Payload {
        remaining: u32,
        total: u64,
        compressed: bool,
    },
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
                    // Byte 0 is the gRPC compression flag: nonzero means the
                    // payload is compressed with the call's Message-Encoding
                    // (ADR-0084 decision 4 charges such a frame its decoded
                    // size, not this wire length). Bytes 1..5 are the payload
                    // length, big-endian.
                    let compressed = buf[0] != 0;
                    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
                    self.state = FrameState::Payload {
                        remaining: len,
                        total: 5 + u64::from(len),
                        compressed,
                    };
                }
                FrameState::Payload {
                    remaining,
                    total,
                    compressed,
                } => {
                    if *remaining == 0 {
                        counter.push_message(*compressed, *total);
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
    use opentelemetry_proto::tonic::metrics::v1::{Gauge, Metric, NumberDataPoint, ScopeMetrics};
    use opentelemetry_proto::tonic::{
        collector::metrics::v1::ExportMetricsServiceRequest, metrics::v1::ResourceMetrics,
    };

    use super::*;

    /// Builds one gRPC length-prefixed frame: a 1-byte compression flag, a
    /// 4-byte big-endian payload length, then `payload`.
    fn frame(compressed: bool, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(5 + payload.len());
        out.push(if compressed { 1 } else { 0 });
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    /// The frame parser reads the 1-byte compression flag per frame, so a
    /// compressed frame's completed accounting carries `compressed = true` and
    /// an uncompressed one `false`, each with the full wire length.
    #[test]
    fn frame_parser_reports_compression_flag_per_frame() {
        let counter = WireByteCounter::new();
        let mut parser = FrameParser::new();
        parser.feed(&frame(true, &[0xaa; 7]), &counter);
        parser.feed(&frame(false, &[0xbb; 3]), &counter);

        assert_eq!(
            counter.pop_frame(),
            Some(FrameCharge {
                compressed: true,
                wire_bytes: 5 + 7,
            })
        );
        assert_eq!(
            counter.pop_frame(),
            Some(FrameCharge {
                compressed: false,
                wire_bytes: 5 + 3,
            })
        );
        assert_eq!(counter.pop_frame(), None);
    }

    fn sample_request() -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "requests_total".to_string(),
                        data: Some(MetricData::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1,
                                value: Some(NumberValue::AsDouble(1.0)),
                                ..Default::default()
                            }],
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// ADR-0084 decision 4: a compressed gRPC frame is charged the DECODED
    /// message size, not its (compressed) wire length, so a tenant that gzips
    /// its gRPC export and one that does not are charged the same for identical
    /// telemetry.
    ///
    /// Non-vacuity: replace `wire_request_bytes`'s `if frame.compressed {
    /// encoded_len } else { wire }` with an unconditional `frame.wire_bytes`,
    /// and this test sees the (deliberately different) wire length instead of
    /// the decoded size and fails. The frame here has its compression flag SET;
    /// a test that left it clear would pass against the unmodified layer.
    #[test]
    fn compressed_frame_is_charged_decoded_size_not_wire_size() {
        let message = sample_request();
        let decoded_len = message.encoded_len() as u64;

        // A wire length deliberately unequal to the decoded size, standing in
        // for the compressed frame's on-the-wire size.
        let wire_len = decoded_len + 4096;
        let counter = WireByteCounter::new();
        counter.push_message(true, wire_len);

        let mut request = Request::new(message);
        request.extensions_mut().insert(counter);

        let charge = wire_request_bytes(&request).expect("charge available");
        assert_eq!(
            charge.charge_bytes, decoded_len,
            "a compressed frame charges the decoded message size"
        );
        assert_ne!(
            charge.charge_bytes, wire_len,
            "a compressed frame must not charge the compressed wire length"
        );
        assert_eq!(
            charge.wire_bytes, wire_len,
            "the wire length is still reported for /metrics"
        );
    }

    /// An uncompressed frame is charged its wire length exactly as before this
    /// change, so nothing shifts for a client that does not compress.
    #[test]
    fn uncompressed_frame_is_charged_wire_size() {
        let message = sample_request();
        let wire_len = message.encoded_len() as u64 + 5;
        let counter = WireByteCounter::new();
        counter.push_message(false, wire_len);

        let mut request = Request::new(message);
        request.extensions_mut().insert(counter);

        let charge = wire_request_bytes(&request).expect("charge available");
        assert_eq!(charge.charge_bytes, wire_len);
        assert_eq!(charge.wire_bytes, wire_len);
    }
}
