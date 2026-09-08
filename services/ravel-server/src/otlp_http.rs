//! `POST /v1/metrics`, `POST /v1/logs`, and `POST /v1/traces`: OTLP
//! HTTP-protobuf ingest. All three endpoints share this file's tenant
//! resolution, write-mode header, and commit-token header handling; the
//! signal-specific logic lives in [`crate::ingest`], [`crate::logs_ingest`],
//! and [`crate::traces_ingest`].

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use bytes::{Buf, Bytes};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message;
use ravel_ingest::{
    AdmissionController, IngestByteBudget, IngestByteCharge, LogWriteError, RequestRejection,
    SpanWriteError, WriteError, WriteMode,
};
use ravel_query::http::TenantResolver;
use ravel_types::{Signal, TenantId};

use crate::ingest::{IngestRequestError, IngestState};
use crate::ingest_concurrency::IngestConcurrencyController;
use crate::logs_ingest::{LogIngestRequestError, LogIngestState};
use crate::traces_ingest::{SpanIngestRequestError, SpanIngestState};

pub const INGEST_MODE_HEADER: &str = "x-ravel-ingest-mode";
pub const COMMIT_TOKEN_HEADER: &str = "x-ravel-commit-token";
/// Opaque, caller-supplied idempotency key for logs and spans (ADR-0051
/// section 5). The same string is the HTTP header name and the gRPC metadata
/// key, the way `authorization` is reused verbatim across both transports.
/// A keyed request that a client retries after a lost ack replays the stored
/// receipt instead of re-ingesting (docs/consistency-model.md).
pub const IDEMPOTENCY_KEY_HEADER: &str = "x-ravel-idempotency-key";
/// Cap on the idempotency key length (ravel-ingest's `idempotency` module
/// doc: the key is `≤128 bytes`). A longer key is rejected (HTTP 400 / gRPC
/// `InvalidArgument`), never truncated or hashed anyway: silent truncation
/// would collapse two distinct keys into one dedup identity.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 128;

/// Nanoseconds per hour, the unit an ingest-hour bucket counts in. Mirrors
/// `ravel_ingest`'s private `config::NS_PER_HOUR`; it and the shard actors'
/// `checked_ingest_hour_bucket` are `pub(crate)` to that crate and so
/// unreachable here. See [`request_ingest_hour_bucket`].
const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Layer 1 (ADR-0051 section 2): the wire-body cap on every OTLP HTTP
/// endpoint, ahead of protobuf decode. This bounds the *compressed* body when a
/// client sends gzip; the decompressed size is bounded independently by
/// [`MAX_DECOMPRESSED_OTLP_BODY_BYTES`], exactly as Remote Write pairs its two
/// caps (ADR-0084 decision 3).
const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Cap on the decompressed size of a gzip OTLP body (ADR-0084 decision 3),
/// matching Remote Write's post-Snappy cap. Enforced *while* expanding, not
/// after, by reading the decoder through `take(cap + 1)`: a decompression bomb
/// is refused as it is being inflated rather than after Ravel has already
/// allocated it (the same discipline as `ravel-otap`'s `decompress_capped`).
const MAX_DECOMPRESSED_OTLP_BODY_BYTES: usize = 64 * 1024 * 1024;

/// Size of the staging buffer the gzip decoder reads into, and so the upper
/// bound on one retained inflate chunk. This one staging buffer is fixed,
/// stack-allocated, and reused for every read; between the read and the
/// retained copy it transiently holds one chunk of decompressed bytes, never
/// the full decompressed body. It is one of the allocations this path never
/// charges, alongside the per-chunk bookkeeping [`ChunkedBody`] documents
/// (which scales with chunk count, not with any single chunk's size) and
/// flate2's own decoder state (tens of KiB, fixed). The compressed request
/// body itself also stays resident for the whole inflate, bounded by
/// [`MAX_REQUEST_BODY_BYTES`] and already counted against
/// `--max-inflight-ingest-requests`, not left uncharged here.
const INFLATE_CHUNK_BYTES: usize = 64 * 1024;

/// The `Content-Encoding` a request declared, after RFC 9110 parsing. Only the
/// codings Ravel actually decodes are named; everything else is
/// [`ContentCoding::Unsupported`] and answered with 415 rather than being
/// guessed at, so an unknown coding never reaches prost as a compressed body
/// and produces the misleading `invalid OTLP payload` 400 ADR-0084 exists to
/// remove.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentCoding {
    /// Absent header, empty value, or the literal `identity`: the body is the
    /// payload as-is.
    Identity,
    /// `gzip` or its RFC 9110 alias `x-gzip`, compared case-insensitively.
    Gzip,
    /// Any other single coding, or any multi-coding list (`gzip, gzip`,
    /// `deflate, gzip`): Ravel does not chain decoders, so this is a 415.
    Unsupported,
}

/// Parses `Content-Encoding` per RFC 9110: case-insensitive, `x-gzip` an alias
/// for `gzip`, a single coding only. A comma-separated list is unsupported
/// because Ravel implements no chained decoding and guessing at one member
/// would be a silent approximation.
fn parse_content_encoding(headers: &HeaderMap) -> ContentCoding {
    let Some(value) = headers.get(header::CONTENT_ENCODING) else {
        return ContentCoding::Identity;
    };
    let Ok(value) = value.to_str() else {
        return ContentCoding::Unsupported;
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return ContentCoding::Identity;
    }
    // A list is always unsupported, even `gzip, gzip`: no chained decoding.
    if trimmed.contains(',') {
        return ContentCoding::Unsupported;
    }
    let coding = trimmed.to_ascii_lowercase();
    match coding.as_str() {
        "identity" => ContentCoding::Identity,
        "gzip" | "x-gzip" => ContentCoding::Gzip,
        _ => ContentCoding::Unsupported,
    }
}

/// An OTLP request body as the exact chunks the ingest byte budget was charged
/// for, read by prost as one non-contiguous [`Buf`].
///
/// The gzip path never concatenates its output. A `Vec<u8>` grown with
/// `extend_from_slice` keeps spare capacity beyond its length and, while
/// reallocating, holds the old and the new allocation at once, so a charge taken
/// on the appended length undercounts what the process is holding; `reserve_exact`
/// does not close that gap, because an allocator may return more than was asked
/// for. Retaining each inflate chunk as its own exactly-sized [`Bytes`] instead
/// makes the charge equal the retained bytes at every instant: each chunk is
/// allocated once at its final size, and no chunk is ever copied into a larger
/// one. What stays uncharged is a fixed staging-and-decoder cost plus a
/// per-chunk bookkeeping cost that scales with chunk count: the fixed
/// [`INFLATE_CHUNK_BYTES`] staging buffer, which between the read and the
/// retained copy transiently holds one chunk of decompressed bytes; flate2's
/// own decoder state (tens of KiB, fixed); and about 48 bytes per 64 KiB chunk
/// of bookkeeping -- one `Bytes` handle in this struct's `chunks` vector and
/// one [`IngestByteCharge`] guard in the separate charges vector
/// [`decompress_gzip_capped_charged`] returns to its caller, both vectors
/// growing by doubling. The compressed request body itself also stays resident
/// for the whole inflate, but it is bounded by [`MAX_REQUEST_BODY_BYTES`] and
/// already counted against `--max-inflight-ingest-requests`, not left
/// uncharged here. No uncharged allocation holds a copy of the full
/// decompressed body; the staging buffer holds only one chunk at a time.
///
/// The identity path wraps its single body chunk here unchanged, so it still
/// makes no copy and takes no charge.
#[derive(Debug)]
struct ChunkedBody {
    chunks: Vec<Bytes>,
    /// Index of the chunk [`Buf::chunk`] reads from. Every earlier chunk is
    /// fully consumed, and `chunks[cursor]` is non-empty whenever `remaining`
    /// is nonzero.
    cursor: usize,
    remaining: usize,
}

impl ChunkedBody {
    fn new(chunks: Vec<Bytes>) -> Self {
        let remaining = chunks.iter().map(Bytes::len).sum();
        Self {
            chunks,
            cursor: 0,
            remaining,
        }
    }

    /// One contiguous body (the identity path), moved in without a copy.
    fn single(body: Bytes) -> Self {
        Self::new(vec![body])
    }

    /// Byte length of every retained chunk, for the accounting assertions: the
    /// charged figure must equal this total, not merely the decompressed length.
    #[cfg(test)]
    fn chunk_lens(&self) -> Vec<usize> {
        self.chunks.iter().map(Bytes::len).collect()
    }
}

impl Buf for ChunkedBody {
    fn remaining(&self) -> usize {
        self.remaining
    }

    fn chunk(&self) -> &[u8] {
        self.chunks
            .get(self.cursor)
            .map_or(&[][..], |chunk| chunk.as_ref())
    }

    /// `cnt` is clamped to what is left rather than panicking on an over-advance:
    /// prost never advances past `remaining`, and an ingest path that mis-decoded
    /// should return a 400 from the decoder, not abort the request task.
    fn advance(&mut self, cnt: usize) {
        let mut left = cnt.min(self.remaining);
        while left > 0 {
            let Some(current) = self.chunks.get_mut(self.cursor) else {
                break;
            };
            let taken = left.min(current.len());
            current.advance(taken);
            left -= taken;
            self.remaining -= taken;
            if current.is_empty() {
                self.cursor += 1;
            }
        }
    }
}

/// Why a gzip body could not be turned into OTLP bytes, while charging the
/// process-wide ingest byte budget for the bytes it inflates.
#[derive(Debug)]
enum GzipDecodeError {
    /// Charging the decompressed bytes would push the process-wide ingest byte
    /// budget past its ceiling (ADR-0069, amended by issue #1297): HTTP 429.
    /// The inflate stops the moment the ceiling is reached, so a shed request
    /// never allocates the full expansion.
    Shed,
    /// The decompressed stream exceeded [`MAX_DECOMPRESSED_OTLP_BODY_BYTES`]:
    /// HTTP 413. Detected while expanding, before the full expansion is
    /// allocated.
    TooLarge,
    /// The bytes were not a well-formed gzip stream, or carried trailing bytes
    /// after a well-formed stream ended: HTTP 400. Truncating silently is not
    /// an option at any size.
    Invalid(String),
}

/// Decompresses a gzip `body` under a single hard cap across all members, and
/// charges each chunk it produces against `budget` *as it produces it* (ADR-0069
/// as amended by issue #1297). The inflate path used to allocate up to
/// [`MAX_DECOMPRESSED_OTLP_BODY_BYTES`] before any byte was charged, so
/// `--max-inflight-ingest-requests` copies of a 64 MiB inflate sat outside the
/// `--max-ingest-buffer-bytes` ceiling that claims to bound ingest memory. The
/// charge now happens while the body inflates, before each chunk is retained:
/// every read charges exactly the bytes that chunk will hold, so the returned
/// guards' summed charge equals the retained chunk total, which is the
/// decompressed length exactly (no over-charge and no undercount), and a body
/// whose inflate would cross the ceiling is shed mid-inflate
/// ([`GzipDecodeError::Shed`]) rather than after the process has already grown
/// by the full expansion.
///
/// The output is a list of exactly-sized chunks, never one growing `Vec<u8>`:
/// see [`ChunkedBody`] for why an amortized-growth buffer cannot be charged
/// honestly.
///
/// Each read is handled in a fixed order, and the order is load-bearing:
///
/// 1. **Cap**: the projected size (bytes produced so far plus this read) is
///    checked against `cap` first, so a body that inflates past the cap is
///    refused with [`GzipDecodeError::TooLarge`] at the first byte past it. An
///    over-cap body is therefore never charged for the crossing chunk, which is
///    what keeps it a 413 rather than a 429 the budget happens to raise first,
///    and it stops inflating instead of expanding to `cap + 1`.
/// 2. **Budget**: only a within-cap chunk is charged against `budget`, so the
///    charge for an over-cap body peaks at `cap` exactly.
/// 3. **Retain**: only a chunk that is both within the cap and charged for is
///    copied into the returned list, so no retained byte is ever uncharged and
///    no over-cap byte is ever retained.
///
/// The decoder is additionally read through `take(cap + 1)`, so even a decoder
/// that returned one huge read could not expand past `cap + 1` before step 1
/// runs (`ravel-otap`'s `decompress_capped` discipline).
///
/// Uses [`MultiGzDecoder`], not `GzDecoder`: a concatenated multi-member stream
/// is legal gzip that ordinary tooling produces, and a plain `GzDecoder` would
/// decode member one, acknowledge it, and silently drop the rest. Trailing
/// bytes after the final member surface as [`GzipDecodeError::Invalid`] (400).
///
/// The caller holds the returned [`IngestByteCharge`] guards through protobuf
/// decode and drops them once prost has copied the buffer into owned structs,
/// before the router takes its own buffered charge (issue #1297), so the
/// inflate charge and the buffered charge never coexist; on any error return
/// here every guard already taken drops, refunding the budget exactly.
fn decompress_gzip_capped_charged(
    body: &[u8],
    cap: usize,
    budget: &Arc<IngestByteBudget>,
) -> Result<(Vec<Bytes>, Vec<IngestByteCharge>), GzipDecodeError> {
    use std::io::Read;

    use flate2::read::MultiGzDecoder;

    let cap_u64 = cap as u64;
    let mut decoder = MultiGzDecoder::new(body).take(cap_u64 + 1);
    let mut chunks: Vec<Bytes> = Vec::new();
    let mut charges = Vec::new();
    let mut produced: u64 = 0;
    // A fixed staging buffer so the charge granularity is bounded and the peak
    // uncharged allocation is at most one chunk.
    let mut staging = [0u8; INFLATE_CHUNK_BYTES];
    loop {
        let read = decoder
            .read(&mut staging)
            .map_err(|err| GzipDecodeError::Invalid(err.to_string()))?;
        if read == 0 {
            break;
        }
        // Cap first, on the projected size: an over-cap body is refused at the
        // first byte past the cap, before that chunk is charged or retained, so
        // it cannot be answered 429 by a budget rejection on a chunk the cap
        // already condemns.
        if produced + read as u64 > cap_u64 {
            return Err(GzipDecodeError::TooLarge);
        }
        // Charge before the chunk is allocated: if the ceiling is crossed,
        // nothing is retained here and every guard taken so far drops on return.
        match budget.try_charge(read as u64) {
            Ok(charge) => charges.push(charge),
            Err(_) => return Err(GzipDecodeError::Shed),
        }
        // Exactly `read` bytes, allocated once at their final size: no spare
        // capacity to leave uncharged, and no reallocation that would hold two
        // copies of the inflate at the same instant.
        chunks.push(Bytes::copy_from_slice(&staging[..read]));
        produced += read as u64;
    }
    Ok((chunks, charges))
}

/// HTTP 415 for an unsupported `Content-Encoding`, naming what is supported so
/// the gap is legible rather than a mystery (ADR-0084 decision 1).
fn unsupported_encoding_response() -> Response {
    (
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        "unsupported Content-Encoding; this endpoint accepts only an absent or identity encoding, \
         or a single gzip (x-gzip) coding",
    )
        .into_response()
}

/// Applies the layer-2 byte-rate charge and returns the OTLP protobuf bytes to
/// decode, dispatching on `Content-Encoding` (ADR-0084 decisions 1, 3, 4).
///
/// - Identity: the current path byte for byte. `check_byte_rate` charges the
///   wire body length exactly as before, and the returned `Bytes` is the input
///   with no copy.
/// - gzip/x-gzip: the compressed length is pre-checked against the tenant's
///   available tokens without consuming any (`peek_byte_rate`); if even that
///   lower bound is over rate the request is rejected 429 before anything is
///   inflated. Otherwise the body is decompressed under the 64 MiB cap and the
///   real charge is made on the decompressed size.
/// - anything else: 415.
///
/// The wire (compressed) length is recorded per tenant on the admitted path so
/// `/metrics` can report it alongside the charged (decompressed) size.
///
/// On the gzip path the returned [`IngestByteCharge`] guards hold the
/// process-wide ingest byte budget charge for the retained inflate chunks
/// (ADR-0069 as amended by issue #1297); the caller keeps them alive through
/// protobuf decode and drops them once prost has copied the chunks into owned
/// structs, before the router takes its own buffered charge, so the two never
/// coexist. The identity path allocates no transient inflate buffer, so it
/// returns no guards.
fn admit_and_decode_body(
    state: &GatewayState,
    headers: &HeaderMap,
    tenant: &TenantId,
    signal: Signal,
    body: Bytes,
) -> Result<(ChunkedBody, Vec<IngestByteCharge>), Box<Response>> {
    match parse_content_encoding(headers) {
        ContentCoding::Unsupported => Err(Box::new(unsupported_encoding_response())),
        ContentCoding::Identity => {
            let wire_len = body.len() as u64;
            // Layer 2 (ADR-0051 section 2): byte rate on the wire body, before
            // decode, unchanged from today for an uncompressed client.
            if let Err(rejection) =
                state
                    .admission
                    .check_byte_rate(tenant, signal, wire_len, now_ns())
            {
                return Err(Box::new(admission_rejection_response(rejection)));
            }
            state
                .ingest_byte_metrics
                .record_wire_bytes(tenant, signal, wire_len);
            Ok((ChunkedBody::single(body), Vec::new()))
        }
        ContentCoding::Gzip => {
            let wire_len = body.len() as u64;
            // Compressed-size pre-check (ADR-0084 decision 4): the compressed
            // length is a strict lower bound on the decompressed length, so if
            // even it exceeds the available tokens the request is rejected 429
            // without inflating anything and without consuming tokens. This
            // keeps an already-over-rate tenant from billing the gateway up to
            // 64 MiB of inflate per rejected request.
            if let Err(rejection) =
                state
                    .admission
                    .peek_byte_rate(tenant, signal, wire_len, now_ns())
            {
                return Err(Box::new(admission_rejection_response(rejection)));
            }
            // Charge the process-wide ingest byte budget for the inflated bytes
            // as they are produced (ADR-0069 as amended by issue #1297), so the
            // transient decode buffer is bounded by --max-ingest-buffer-bytes,
            // not just by --max-inflight-ingest-requests. A body whose inflate
            // would cross the ceiling is shed mid-inflate (429), before the
            // process grows by the full expansion.
            let (chunks, decode_charge) = match decompress_gzip_capped_charged(
                &body,
                MAX_DECOMPRESSED_OTLP_BODY_BYTES,
                &state.budget,
            ) {
                Ok(result) => result,
                Err(GzipDecodeError::Shed) => {
                    return Err(Box::new(ingest_buffer_budget_shed_response()));
                }
                Err(GzipDecodeError::TooLarge) => {
                    return Err(Box::new(
                        (
                            StatusCode::PAYLOAD_TOO_LARGE,
                            format!(
                                "decompressed OTLP body exceeds {} bytes",
                                MAX_DECOMPRESSED_OTLP_BODY_BYTES
                            ),
                        )
                            .into_response(),
                    ));
                }
                Err(GzipDecodeError::Invalid(detail)) => {
                    return Err(Box::new(
                        (
                            StatusCode::BAD_REQUEST,
                            format!("invalid gzip body: {detail}"),
                        )
                            .into_response(),
                    ));
                }
            };
            let decompressed = ChunkedBody::new(chunks);
            let decompressed_len = decompressed.remaining() as u64;
            // The real byte-rate charge: the decompressed size (ADR-0084
            // decision 4), so a compressing tenant and an uncompressing one
            // sending the same telemetry are charged the same. On rejection the
            // `decode_charge` guards drop, refunding the budget exactly.
            if let Err(rejection) =
                state
                    .admission
                    .check_byte_rate(tenant, signal, decompressed_len, now_ns())
            {
                return Err(Box::new(admission_rejection_response(rejection)));
            }
            state
                .ingest_byte_metrics
                .record_wire_bytes(tenant, signal, wire_len);
            Ok((decompressed, decode_charge))
        }
    }
}

pub struct GatewayState {
    pub tenant_resolver: Arc<dyn TenantResolver>,
    pub ingest: IngestState,
    /// The log pipeline's counterpart of `ingest`. Separate router, separate
    /// limits: logs flush RLOG objects under the `l` keyspace, metrics flush
    /// RSEG under `m`, and nothing is shared between them but this struct.
    pub logs_ingest: LogIngestState,
    /// The span pipeline's counterpart, on the same terms: RSPAN objects under
    /// the `s` keyspace, its own router and its own limits (ADR-0041).
    pub traces_ingest: SpanIngestState,
    /// Tenant admission (ADR-0051): shared by all three signals for the
    /// layer-2 byte-rate check. On the identity path this is done on wire bytes
    /// before decode as before; on the gzip path (ADR-0084) it is done on the
    /// decompressed size after decompression, behind a compressed-size
    /// pre-check.
    pub admission: Arc<AdmissionController>,
    /// Per-tenant wire (compressed) request-body byte counter (ADR-0084
    /// decision 5). Shared with the gRPC ingest services and the `/metrics`
    /// renderer, so an operator can compare wire bytes against the charged
    /// (decompressed) bytes admission reports and tell a tenant that increased
    /// telemetry from one that turned compression off.
    pub ingest_byte_metrics: Arc<crate::ingest_byte_metrics::IngestByteMetrics>,
    /// The process-wide ingest buffer byte budget (ADR-0069 decision 1, amended
    /// by issue #1297). The same `Arc` the ingest routers hold via
    /// `with_budget`. The gzip inflate path (`admit_and_decode_body`) charges
    /// the decompressed bytes into it *while* it inflates, one charge per chunk
    /// taken before that chunk is retained, and holds the charge through decode;
    /// it releases the charge once prost has copied the chunks into owned
    /// structs, before the router takes its own buffered charge, so the inflate
    /// charge and the buffered charge never coexist and transient decode memory
    /// is bounded by `--max-ingest-buffer-bytes` the same way buffered memory
    /// is.
    pub budget: Arc<IngestByteBudget>,
    /// The process-wide in-flight ingest-request ceiling, shared
    /// with every OTLP HTTP/gRPC service and Remote Write on this listener
    /// and the mTLS listener. Checked first in every handler below, ahead of
    /// tenant resolution and the layer-2 byte-rate check, so a shed request
    /// does none of that work.
    pub ingest_concurrency: Arc<IngestConcurrencyController>,
}

pub fn router(state: Arc<GatewayState>) -> Router {
    Router::new()
        .route("/v1/metrics", post(export_metrics))
        .route("/v1/logs", post(export_logs))
        .route("/v1/traces", post(export_traces))
        .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
        .with_state(state)
}

/// Turn a layer-2/layer-4 whole-request rejection into the ADR-0051 HTTP
/// response: 429 with `Retry-After` in whole seconds (rounded up, minimum
/// 1), the reason as the body.
fn admission_rejection_response(rejection: RequestRejection) -> Response {
    let mut response =
        (StatusCode::TOO_MANY_REQUESTS, rejection.reason.to_string()).into_response();
    let retry_after_secs = retry_after_seconds(rejection.retry_after_ns);
    if let Ok(value) = HeaderValue::from_str(&retry_after_secs.to_string()) {
        response.headers_mut().insert(header::RETRY_AFTER, value);
    }
    response
}

fn retry_after_seconds(retry_after_ns: i64) -> u64 {
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
fn ingest_concurrency_shed_response() -> Response {
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

/// A fixed `Retry-After` for the process-wide ingest buffer byte budget shed
/// (ADR-0069): like the in-flight shed above, the budget tracks no per-caller
/// refill time -- a buffer slot frees as soon as any in-flight flush completes
/// -- so a short fixed wait is the right shape.
const INGEST_BUFFER_BUDGET_RETRY_AFTER_SECONDS: u64 = 1;

/// 429 for a request shed by the process-wide ingest buffer byte budget
/// (ADR-0069 decision 1): the write was rejected before any buffering, so no
/// shard was touched and no commit token issued. Same 429 + `Retry-After`
/// shape as the byte-rate rejection and the in-flight shed.
fn ingest_buffer_budget_shed_response() -> Response {
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

/// Attaches the encoded protobuf `body` as an OTLP response, plus the
/// commit-token header `commit_token` when present. Shared by all three
/// endpoints: the header name is the same for either signal, and a client
/// distinguishes them by which endpoint it called.
///
/// `commit_token` is the already-built header value, not the token list: a
/// normal write passes [`encode_commit_tokens`] of its receipt, and an
/// idempotency replay passes the receipt's stored value verbatim, so a
/// replayed response carries the byte-identical header the original did.
fn otlp_response(body: Vec<u8>, commit_token: Option<&str>) -> Response {
    let mut response = Bytes::from(body).into_response();
    response.headers_mut().insert(
        "content-type",
        HeaderValue::from_static("application/x-protobuf"),
    );
    if let Some(encoded) = commit_token
        && let Ok(value) = HeaderValue::from_str(encoded)
    {
        response.headers_mut().insert(COMMIT_TOKEN_HEADER, value);
    }
    response
}

/// Build the `x-ravel-commit-token` header value from a write receipt's
/// tokens: one `CommitToken::encode()` per shard the request flushed through,
/// comma-joined (docs/consistency-model.md). `None` when the write produced
/// no tokens (buffered mode, or nothing admitted), so no header is emitted.
///
/// This is the exact string an idempotency marker stores, so a keyed replay
/// round-trips it back out unchanged; it lives here as the single definition
/// both transports and the marker-write path share.
pub(crate) fn encode_commit_tokens(tokens: &[ravel_types::CommitToken]) -> Option<String> {
    if tokens.is_empty() {
        return None;
    }
    Some(
        tokens
            .iter()
            .map(|token| token.encode())
            .collect::<Vec<_>>()
            .join(","),
    )
}

/// Extract the opaque idempotency key from request headers/metadata (both
/// reach here as a [`HeaderMap`]; the gRPC handlers convert metadata first).
/// An absent or empty key is `None` (plain at-least-once); length validation
/// against [`MAX_IDEMPOTENCY_KEY_BYTES`] happens inside the signal handler so
/// the rejection is a typed error the transport maps to its own status code.
pub(crate) fn idempotency_key_from_headers(headers: &HeaderMap) -> Option<Vec<u8>> {
    headers
        .get(IDEMPOTENCY_KEY_HEADER)
        .map(|value| value.as_bytes().to_vec())
        .filter(|key| !key.is_empty())
}

/// The request's ingest-hour bucket, computed once from `ingest_ts_ns` and
/// used for both the marker lookup (`read_marker`'s `now` bucket) and the
/// marker write, so the two can never drift within one request.
///
/// It mirrors `ravel_ingest`'s `checked_ingest_hour_bucket` formula
/// (`div_euclid` by [`NS_PER_HOUR`]); that function and its `NS_PER_HOUR` are
/// `pub(crate)` to `ravel-ingest` and unreachable from this crate, and the
/// commit token's `ingest_hour_bucket` field (the other in-tree source) only
/// exists *after* a write, so it cannot serve the pre-write lookup. `None`
/// for a non-positive or non-representable reading: idempotency then fails
/// open to the normal at-least-once path rather than failing a request whose
/// data is (or will be) durably committed regardless.
pub(crate) fn request_ingest_hour_bucket(ingest_ts_ns: i64) -> Option<u32> {
    if ingest_ts_ns <= 0 {
        return None;
    }
    u32::try_from(ingest_ts_ns.div_euclid(NS_PER_HOUR)).ok()
}

pub(crate) fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

pub(crate) fn write_mode_from_headers(headers: &HeaderMap) -> WriteMode {
    let buffered = headers
        .get(INGEST_MODE_HEADER)
        .and_then(|value| value.to_str().ok())
        == Some("buffered");
    if buffered {
        WriteMode::Buffered
    } else {
        WriteMode::Strict
    }
}

async fn export_metrics(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let _permit = match state.ingest_concurrency.try_admit() {
        Ok(permit) => permit,
        Err(_) => return ingest_concurrency_shed_response(),
    };

    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let mode = write_mode_from_headers(&headers);

    // Layer 2 (ADR-0051 section 2) plus gzip dispatch (ADR-0084): charge the
    // byte rate and return the OTLP protobuf bytes, decompressing first when
    // the client sent gzip. Identity is unchanged from before. On the gzip path
    // `decode_charge` holds the process-wide budget charge for the retained
    // inflate chunks (empty on the identity path); it is released once prost has
    // decoded and freed them, just below (ADR-0069 as amended by issue #1297).
    let (body, decode_charge) =
        match admit_and_decode_body(&state, &headers, &tenant, Signal::Metrics, body) {
            Ok(decoded) => decoded,
            Err(response) => return *response,
        };

    // `decode` consumes the chunked body, so prost's own return frees the
    // retained inflate chunks: it copies them into owned protobuf structs and
    // nothing borrows them afterwards.
    let request = match ExportMetricsServiceRequest::decode(body) {
        Ok(request) => request,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid OTLP payload: {err}"),
            )
                .into_response();
        }
    };
    // Release the inflate charge here, with the chunks already freed, before the
    // router takes its own charge for the normalized batch: the inflate charge
    // and the router's buffered charge never coexist, so a request whose inflate
    // and batch each fit the budget is not shed for their sum (issue #1297).
    drop(decode_charge);

    match crate::ingest::handle_export(&state.ingest, tenant, mode, request, now_ns()).await {
        Ok(outcome) => otlp_response(
            outcome.response.encode_to_vec(),
            encode_commit_tokens(&outcome.tokens).as_deref(),
        ),
        Err(IngestRequestError::Admission(rejection)) => admission_rejection_response(rejection),
        // Receiver-clock floor (ADR-0051 amendment): 503, the fault is
        // the replica's and a retry against a healthy one succeeds.
        Err(err @ IngestRequestError::ClockImplausible(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
        Err(err @ IngestRequestError::Provisioning(_)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
        }
        // The buffer-budget shed (ADR-0069) is a 429, not the 503 the other
        // write failures take: it is an admission backpressure signal, not a
        // durability failure, and the client should retry with backoff.
        Err(IngestRequestError::Write(WriteError::BufferBudgetExceeded)) => {
            ingest_buffer_budget_shed_response()
        }
        // Retryable at the client: the same replica or a healthy one can
        // succeed on the identical request later.
        Err(IngestRequestError::Write(write_err)) if write_err.is_retryable() => {
            (StatusCode::SERVICE_UNAVAILABLE, write_err.to_string()).into_response()
        }
        // Not retryable: the input itself cannot be accepted (e.g. a
        // series value-kind mismatch), so 400 rather than the 503 that would
        // tell a well-behaved exporter to retry forever.
        Err(IngestRequestError::Write(write_err)) => {
            (StatusCode::BAD_REQUEST, write_err.to_string()).into_response()
        }
    }
}

/// `POST /v1/logs`. Same shape as [`export_metrics`], down to the status
/// codes: 401 for an unresolvable tenant, 400 for an undecodable body or a
/// non-retryable write the log pipeline rejected, 503 for a retryable write
/// failure.
async fn export_logs(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let _permit = match state.ingest_concurrency.try_admit() {
        Ok(permit) => permit,
        Err(_) => return ingest_concurrency_shed_response(),
    };

    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let mode = write_mode_from_headers(&headers);

    // Layer 2 (ADR-0051 section 2) plus gzip dispatch (ADR-0084): charge the
    // byte rate and return the OTLP protobuf bytes, decompressing first when
    // the client sent gzip. Identity is unchanged from before.
    // `decode_charge`: see `export_metrics`. Released once decode has consumed
    // and freed the inflate chunks, before the log write below.
    let (body, decode_charge) =
        match admit_and_decode_body(&state, &headers, &tenant, Signal::Logs, body) {
            Ok(decoded) => decoded,
            Err(response) => return *response,
        };

    let request = match ExportLogsServiceRequest::decode(body) {
        Ok(request) => request,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid OTLP payload: {err}"),
            )
                .into_response();
        }
    };
    // See `export_metrics`: decode consumed the chunks and prost owns the
    // decoded structs, so release the charge before the router charges the
    // normalized batch, so the two never coexist (issue #1297).
    drop(decode_charge);

    let idempotency_key = idempotency_key_from_headers(&headers);

    match crate::logs_ingest::handle_export_logs(
        &state.logs_ingest,
        tenant,
        mode,
        request,
        now_ns(),
        idempotency_key,
    )
    .await
    {
        Ok(outcome) => otlp_response(
            outcome.response.encode_to_vec(),
            outcome.commit_token_header().as_deref(),
        ),
        Err(LogIngestRequestError::Admission(rejection)) => admission_rejection_response(rejection),
        Err(err @ LogIngestRequestError::ClockImplausible(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
        Err(err @ LogIngestRequestError::InvalidIdempotencyKey { .. }) => {
            (StatusCode::BAD_REQUEST, err.to_string()).into_response()
        }
        Err(err @ LogIngestRequestError::Provisioning(_)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
        }
        Err(LogIngestRequestError::Write(LogWriteError::BufferBudgetExceeded)) => {
            ingest_buffer_budget_shed_response()
        }
        // Retryable at the client: the same replica or a healthy one can
        // succeed on the identical request later.
        Err(LogIngestRequestError::Write(write_err)) if write_err.is_retryable() => {
            (StatusCode::SERVICE_UNAVAILABLE, write_err.to_string()).into_response()
        }
        // Not retryable: the input itself cannot be accepted, so 400 rather
        // than the 503 that would tell a well-behaved exporter to retry
        // forever.
        Err(LogIngestRequestError::Write(write_err)) => {
            (StatusCode::BAD_REQUEST, write_err.to_string()).into_response()
        }
    }
}

/// `POST /v1/traces`. Same shape as [`export_metrics`] and [`export_logs`],
/// down to the status codes: 401 for an unresolvable tenant, 400 for an
/// undecodable body or a non-retryable write the span pipeline rejected, 503
/// for a retryable write failure.
async fn export_traces(
    State(state): State<Arc<GatewayState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let _permit = match state.ingest_concurrency.try_admit() {
        Ok(permit) => permit,
        Err(_) => return ingest_concurrency_shed_response(),
    };

    let tenant = match state.tenant_resolver.resolve(&headers) {
        Ok(tenant) => tenant,
        Err(_) => return StatusCode::UNAUTHORIZED.into_response(),
    };

    let mode = write_mode_from_headers(&headers);

    // Layer 2 (ADR-0051 section 2) plus gzip dispatch (ADR-0084): byte rate
    // applies uniformly to every signal including spans (even though spans get
    // no layer-4 admission), charged after decompression on the gzip path.
    // `decode_charge`: see `export_metrics`. Released once decode has consumed
    // and freed the inflate chunks, before the span write below.
    let (body, decode_charge) =
        match admit_and_decode_body(&state, &headers, &tenant, Signal::Spans, body) {
            Ok(decoded) => decoded,
            Err(response) => return *response,
        };

    let request = match ExportTraceServiceRequest::decode(body) {
        Ok(request) => request,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                format!("invalid OTLP payload: {err}"),
            )
                .into_response();
        }
    };
    // See `export_metrics`: decode consumed the chunks and prost owns the
    // decoded structs, so release the charge before the router charges the
    // normalized batch, so the two never coexist (issue #1297).
    drop(decode_charge);

    let idempotency_key = idempotency_key_from_headers(&headers);

    match crate::traces_ingest::handle_export_traces(
        &state.traces_ingest,
        tenant,
        mode,
        request,
        now_ns(),
        idempotency_key,
    )
    .await
    {
        Ok(outcome) => otlp_response(
            outcome.response.encode_to_vec(),
            outcome.commit_token_header().as_deref(),
        ),
        Err(err @ SpanIngestRequestError::ClockImplausible(_)) => {
            (StatusCode::SERVICE_UNAVAILABLE, err.to_string()).into_response()
        }
        Err(err @ SpanIngestRequestError::InvalidIdempotencyKey { .. }) => {
            (StatusCode::BAD_REQUEST, err.to_string()).into_response()
        }
        Err(err @ SpanIngestRequestError::Provisioning(_)) => {
            (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
        }
        Err(SpanIngestRequestError::Write(SpanWriteError::BufferBudgetExceeded)) => {
            ingest_buffer_budget_shed_response()
        }
        // Retryable at the client: the same replica or a healthy one can
        // succeed on the identical request later.
        Err(SpanIngestRequestError::Write(write_err)) if write_err.is_retryable() => {
            (StatusCode::SERVICE_UNAVAILABLE, write_err.to_string()).into_response()
        }
        // Not retryable: the input itself cannot be accepted, so 400 rather
        // than the 503 that would tell a well-behaved exporter to retry
        // forever.
        Err(SpanIngestRequestError::Write(write_err)) => {
            (StatusCode::BAD_REQUEST, write_err.to_string()).into_response()
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
pub(crate) mod tests {
    use std::io::Write as _;

    use flate2::Compression;
    use flate2::write::GzEncoder;
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
    use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
    use opentelemetry_proto::tonic::metrics::v1::{
        AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge, Metric,
        NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
    use ravel_ingest::{
        AdmissionController, AdmissionLimits, IngestByteBudget, IngestByteBudgetLimit,
        IngestConfig, IngestRouter, LogIngestRouter, RateLimit, SpanIngestRouter, SystemClock,
    };
    use ravel_object_store::ObjectStoreBackend;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::memory::MemoryStore;
    use ravel_otlp::{IngestLimits, LogIngestLimits, SpanIngestLimits};
    use ravel_query::http::AuthError;

    use super::*;
    use crate::ingest_byte_metrics::IngestByteMetrics;
    use crate::ingest_concurrency::{IngestConcurrencyController, IngestConcurrencyLimit};

    const TENANT: &str = "acme";

    /// Resolves every request to the same fixed tenant, so a handler test can
    /// find that tenant's admission usage row without threading a token.
    struct FixedTenantResolver(TenantId);

    impl TenantResolver for FixedTenantResolver {
        fn resolve(&self, _headers: &HeaderMap) -> Result<TenantId, AuthError> {
            Ok(self.0.clone())
        }
    }

    /// A `GatewayState` over `store`, whose admission controller carries
    /// `limits` as its per-tenant defaults (so the fixed tenant gets them), and
    /// whose metrics, log, and span routers all share `budget` as their ingest
    /// buffer byte budget. The routers are all real, so a handler runs the full
    /// ingest path and returns the same status a client would see.
    fn state_with_store_and_budget(
        store: Arc<dyn ObjectStoreBackend>,
        limits: AdmissionLimits,
        budget: Arc<IngestByteBudget>,
    ) -> Arc<GatewayState> {
        let metrics_router = Arc::new(
            IngestRouter::new(
                IngestConfig::default(),
                store.clone(),
                Signal::Metrics,
                Arc::new(SystemClock),
            )
            .with_budget(budget.clone()),
        );
        let log_router = Arc::new(
            LogIngestRouter::new(
                IngestConfig::default(),
                store.clone(),
                Arc::new(SystemClock),
            )
            .with_budget(budget.clone()),
        );
        let span_router = Arc::new(
            SpanIngestRouter::new(
                IngestConfig::default(),
                store.clone(),
                Arc::new(SystemClock),
            )
            .with_budget(budget.clone()),
        );
        let admission = Arc::new(AdmissionController::new(Arc::new(SystemClock), limits));
        Arc::new(GatewayState {
            tenant_resolver: Arc::new(FixedTenantResolver(TenantId::new(TENANT))),
            ingest: crate::ingest::IngestState {
                router: metrics_router,
                limits: IngestLimits::default(),
                ack_deadline: std::time::Duration::from_secs(5),
                admission: admission.clone(),
                recovery: None,
                provisioning: None,
                metadata_sink: None,
            },
            logs_ingest: crate::logs_ingest::LogIngestState {
                router: log_router,
                limits: LogIngestLimits::default(),
                ack_deadline: std::time::Duration::from_secs(5),
                admission: admission.clone(),
                store: store.clone(),
                recovery: None,
                provisioning: None,
            },
            traces_ingest: crate::traces_ingest::SpanIngestState {
                router: span_router,
                limits: SpanIngestLimits::default(),
                ack_deadline: std::time::Duration::from_secs(5),
                admission: admission.clone(),
                store: store.clone(),
                recovery: None,
                provisioning: None,
            },
            admission,
            budget,
            ingest_concurrency: IngestConcurrencyController::shared(
                IngestConcurrencyLimit::Unlimited,
            ),
            ingest_byte_metrics: Arc::new(IngestByteMetrics::new()),
        })
    }

    /// A `GatewayState` over `MemoryStore`, whose admission controller carries
    /// `limits` as its per-tenant defaults and whose routers share an
    /// unlimited ingest buffer budget (today's default).
    ///
    /// `pub(crate)`: reused by `otlp_grpc`'s test module, so the gRPC
    /// permanent-write-error status test exercises the same fixture as the
    /// HTTP one instead of a second hand-built `GatewayState`.
    pub(crate) fn state_with_limits(limits: AdmissionLimits) -> Arc<GatewayState> {
        state_with_store_and_budget(
            Arc::new(MemoryStore::new()),
            limits,
            IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited),
        )
    }

    /// A `GatewayState` over `store`, with default admission limits and an
    /// unlimited ingest buffer budget: for tests that fault-inject the store
    /// rather than the admission or budget layers.
    fn state_with_store(store: Arc<dyn ObjectStoreBackend>) -> Arc<GatewayState> {
        state_with_store_and_budget(
            store,
            AdmissionLimits::default(),
            IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited),
        )
    }

    /// A `GatewayState` over `MemoryStore`, with default admission limits and
    /// an ingest buffer budget of zero: any write charges it and is shed
    /// before touching a shard.
    fn state_with_zero_budget() -> Arc<GatewayState> {
        state_with_store_and_budget(
            Arc::new(MemoryStore::new()),
            AdmissionLimits::default(),
            IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(0)),
        )
    }

    /// A metrics export that compresses well: `points` copies of one gauge data
    /// point, so the decompressed protobuf is many times the gzip size. The
    /// distinction matters for the charging tests, which assert the byte rate is
    /// charged the decompressed length, not the compressed one.
    fn compressible_request(points: usize) -> ExportMetricsServiceRequest {
        let data_points = (0..points)
            .map(|_| NumberDataPoint {
                time_unix_nano: now_ns() as u64,
                value: Some(NumberValue::AsDouble(1.0)),
                ..Default::default()
            })
            .collect();
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "requests_total".to_string(),
                        data: Some(MetricData::Gauge(Gauge { data_points })),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// A metrics export carrying two metrics of the same name and empty
    /// attributes, so they share one `series_id`: one a scalar `Gauge` point,
    /// the other a native (`ExponentialHistogram`) point. `ingest.rs`'s
    /// `handle_export` routes both points through one
    /// `write_values_with_exemplars` call, so `shard.rs`'s `merge` sees both
    /// claims for the same series in one batch and returns
    /// `WriteError::SeriesValueKindMismatch` -- a non-retryable, permanent
    /// rejection of the input itself.
    ///
    /// `pub(crate)`: reused by `otlp_grpc`'s test module (see
    /// `state_with_limits`).
    pub(crate) fn value_kind_mismatch_request() -> ExportMetricsServiceRequest {
        let ts = now_ns() as u64;
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![
                        Metric {
                            name: "conflicting_series".to_string(),
                            data: Some(MetricData::Gauge(Gauge {
                                data_points: vec![NumberDataPoint {
                                    time_unix_nano: ts,
                                    value: Some(NumberValue::AsDouble(1.0)),
                                    ..Default::default()
                                }],
                            })),
                            ..Default::default()
                        },
                        Metric {
                            name: "conflicting_series".to_string(),
                            data: Some(MetricData::ExponentialHistogram(ExponentialHistogram {
                                data_points: vec![ExponentialHistogramDataPoint {
                                    time_unix_nano: ts,
                                    ..Default::default()
                                }],
                                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                            })),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// One minimal, valid log export: a single record, so a handler exercises
    /// the full decode-and-write path without tripping any admission or
    /// decode rejection ahead of the write itself.
    fn minimal_log_request() -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![LogRecord {
                        time_unix_nano: now_ns() as u64,
                        severity_number: 9,
                        severity_text: "INFO".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// One minimal, valid trace export: a single span, so a handler exercises
    /// the full decode-and-write path without tripping any admission or
    /// decode rejection ahead of the write itself.
    fn minimal_trace_request() -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![Span {
                        trace_id: vec![7u8; 16],
                        span_id: vec![3u8; 8],
                        name: "span".to_string(),
                        start_time_unix_nano: now_ns() as u64,
                        end_time_unix_nano: now_ns() as u64,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    /// One-member gzip of `data`.
    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).expect("gzip write");
        encoder.finish().expect("gzip finish")
    }

    /// The fixed tenant's metrics admission usage row, or `None` if untouched.
    fn metrics_usage(state: &GatewayState) -> Option<ravel_ingest::TenantUsage> {
        let want = TenantId::new(TENANT).hash();
        state
            .admission
            .usage_snapshot()
            .into_iter()
            .find(|row| row.tenant_hash == want && row.signal == Signal::Metrics)
    }

    fn gzip_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        headers
    }

    /// ADR-0084 decision 1/4: a gzip body is decoded and ingested (200), and the
    /// byte rate is charged the DECOMPRESSED length, not the compressed one.
    ///
    /// Non-vacuity: the request is chosen so `compressed_len` is far below
    /// `decompressed_len`, and the test asserts the charge equals the latter and
    /// differs from the former. Change `admit_and_decode_body`'s gzip
    /// `check_byte_rate(decompressed_len)` to charge `wire_len` and this fails.
    #[tokio::test]
    async fn gzip_body_is_accepted_and_charged_decompressed() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = compressible_request(2000).encode_to_vec();
        let compressed = gzip(&encoded);
        assert!(
            compressed.len() < encoded.len(),
            "fixture must compress: compressed={} decompressed={}",
            compressed.len(),
            encoded.len()
        );

        let response = export_metrics(
            State(state.clone()),
            gzip_headers(),
            Bytes::from(compressed.clone()),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "gzip body must be accepted"
        );

        let usage = metrics_usage(&state).expect("a metrics usage row after admitting the request");
        assert_eq!(
            usage.bytes_admitted_total,
            encoded.len() as u64,
            "the byte rate must charge the decompressed length"
        );
        assert_ne!(
            usage.bytes_admitted_total,
            compressed.len() as u64,
            "the charge must not be the compressed length"
        );

        // The wire (compressed) size is recorded separately for /metrics.
        let wire = state.ingest_byte_metrics.snapshot();
        let row = wire
            .iter()
            .find(|r| r.tenant_hash == TenantId::new(TENANT).hash() && r.signal == Signal::Metrics)
            .expect("a wire-bytes row");
        assert_eq!(row.wire_bytes_total, compressed.len() as u64);
    }

    /// ADR-0084 decision 3: a body inflating past 64 MiB is rejected 413 while
    /// expanding, without the process allocating the full expansion (the decoder
    /// is read through `take(cap + 1)`, so at most 64 MiB + 1 is ever buffered).
    ///
    /// Non-vacuity: delete `decompress_gzip_capped_charged`'s projected-size cap
    /// check (`if produced + read as u64 > cap_u64 { return Err(TooLarge) }`) and
    /// the oversized body flows on to prost as a truncated buffer, turning the
    /// 413 into a 400.
    #[tokio::test]
    async fn gzip_bomb_over_cap_is_rejected_413() {
        let state = state_with_limits(AdmissionLimits::default());
        // Zeros compress ~1000:1, so this is a small body that inflates past the
        // 64 MiB cap. It need not be valid OTLP: the cap trips before decode.
        let bomb_plain = vec![0u8; MAX_DECOMPRESSED_OTLP_BODY_BYTES + 1024];
        let compressed = gzip(&bomb_plain);
        assert!(
            compressed.len() < 1024 * 1024,
            "the compressed bomb must stay small: {}",
            compressed.len()
        );

        let response = export_metrics(State(state), gzip_headers(), Bytes::from(compressed)).await;
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "an over-cap decompression must be 413"
        );
    }

    /// Issue #1297 review finding 1: the decompressed cap is checked BEFORE the
    /// budget charge, so a body inflating one byte past the cap is answered with
    /// the documented 413 even when the budget has exactly the cap of headroom
    /// and the crossing byte would not fit. The shed counter stays at zero (the
    /// request was refused by the cap, not by backpressure) and every partial
    /// charge taken while inflating is refunded, so the in-flight gauge returns
    /// to zero.
    ///
    /// The ceiling is deliberately the cap exactly: that is the configuration in
    /// which charging the crossing byte first is observable, because the byte
    /// past the cap is also the byte past the ceiling.
    ///
    /// Non-vacuity: move the cap check back after the loop (charge and retain
    /// every chunk, then `if produced > cap_u64 { return Err(TooLarge) }`) and
    /// the crossing byte is charged before the cap is consulted, so the budget
    /// rejects it and the handler answers 429 where 413 is asserted.
    #[tokio::test]
    async fn over_cap_gzip_body_is_413_even_when_the_ceiling_equals_the_cap() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(
            MAX_DECOMPRESSED_OTLP_BODY_BYTES as u64,
        ));
        let state = state_with_store_and_budget(
            Arc::new(MemoryStore::new()),
            AdmissionLimits::default(),
            budget.clone(),
        );
        // Exactly one byte past the cap: zeros, so the compressed body stays
        // small and the cap (not the wire-body limit) is what trips.
        let one_past_cap = vec![0u8; MAX_DECOMPRESSED_OTLP_BODY_BYTES + 1];
        let compressed = gzip(&one_past_cap);

        let response = export_metrics(State(state), gzip_headers(), Bytes::from(compressed)).await;
        assert_eq!(
            response.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "an over-cap inflate is refused by the cap, so 413 and never the budget's 429"
        );
        assert_eq!(
            budget.shed_total(),
            0,
            "the cap rejection is not a budget shed, so the shed counter is untouched"
        );
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "every chunk charged while inflating is refunded on the cap rejection"
        );
    }

    /// Issue #1297 review finding 1, the accounting half: the charge taken for a
    /// body that inflates past the cap peaks at the cap EXACTLY. The budget's own
    /// gate is the sampler, since a rejected inflate returns no charge guards to
    /// read: at a ceiling of exactly `cap` the call is `TooLarge`, so no charge
    /// ever exceeded the cap, and at one byte less it sheds, so the charge did
    /// reach the cap. The two together pin the peak to `cap`.
    ///
    /// A small cap (four staging chunks) rather than the 64 MiB constant, so the
    /// crossing read is the single last byte of a `cap + 1` body.
    ///
    /// Non-vacuity: move the cap check back after the loop and the crossing byte
    /// is charged, pushing the charge to `cap + 1`; the ceiling-equals-cap case
    /// then sheds and this observes `Shed` where `TooLarge` is asserted.
    #[test]
    fn over_cap_inflate_charge_peaks_at_the_cap_exactly() {
        const CAP: usize = 4 * INFLATE_CHUNK_BYTES;
        let one_past_cap = vec![0u8; CAP + 1];
        let compressed = gzip(&one_past_cap);

        let at_cap = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(CAP as u64));
        let err = decompress_gzip_capped_charged(&compressed, CAP, &at_cap)
            .expect_err("a body inflating past the cap is refused");
        assert!(
            matches!(err, GzipDecodeError::TooLarge),
            "with the cap of headroom available the charge never crosses it, so this is \
             TooLarge, not Shed: {err:?}"
        );
        assert_eq!(at_cap.shed_total(), 0, "no shed on the cap rejection");
        assert_eq!(
            at_cap.in_flight_bytes(),
            0,
            "the cap rejection refunds every charge it took"
        );

        let one_short = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(CAP as u64 - 1));
        let err = decompress_gzip_capped_charged(&compressed, CAP, &one_short)
            .expect_err("one byte less headroom than the cap cannot hold the inflate");
        assert!(
            matches!(err, GzipDecodeError::Shed),
            "the charge reaches the cap exactly, so a ceiling one byte under it sheds: {err:?}"
        );
        assert_eq!(one_short.shed_total(), 1, "exactly one shed is counted");
        assert_eq!(
            one_short.in_flight_bytes(),
            0,
            "the shed refunds every chunk charged before it"
        );
    }

    /// ADR-0084 decision 1: a concatenated multi-member gzip stream ingests ALL
    /// members. The fixture splits one encoded request across two members, so a
    /// decoder that stops after member one (`GzDecoder`) yields a truncated,
    /// undecodable protobuf.
    ///
    /// Non-vacuity: swap `decompress_gzip_capped_charged`'s `MultiGzDecoder` for
    /// `GzDecoder` and only the first half decompresses, so prost sees a
    /// truncated message and the handler returns 400 instead of 200.
    #[tokio::test]
    async fn gzip_multi_member_stream_ingests_all_members() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = compressible_request(500).encode_to_vec();
        let mid = encoded.len() / 2;
        assert!(mid > 0 && mid < encoded.len(), "need a real two-way split");
        // Two independently-framed gzip members, concatenated. Each is a
        // complete gzip stream; MultiGzDecoder decodes both and yields the full
        // `encoded`, GzDecoder would yield only the first half.
        let mut two_member = gzip(&encoded[..mid]);
        two_member.extend_from_slice(&gzip(&encoded[mid..]));

        // Sanity: our own capped decoder reconstructs the whole thing. An
        // unlimited budget never sheds, so this exercises decode alone.
        let unlimited = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
        let (round_trip, _charges) = decompress_gzip_capped_charged(
            &two_member,
            MAX_DECOMPRESSED_OTLP_BODY_BYTES,
            &unlimited,
        )
        .expect("decodes");
        assert_eq!(round_trip.concat(), encoded, "both members must decompress");

        let response = export_metrics(
            State(state.clone()),
            gzip_headers(),
            Bytes::from(two_member),
        )
        .await;
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "both members must ingest as one request"
        );
        let usage = metrics_usage(&state).expect("a metrics usage row");
        assert_eq!(
            usage.bytes_admitted_total,
            encoded.len() as u64,
            "the charge covers the whole decompressed stream, not one member"
        );
    }

    /// ADR-0084 decision 1: trailing bytes after a well-formed gzip stream are a
    /// 400, never silently truncated.
    #[tokio::test]
    async fn gzip_trailing_garbage_is_rejected_400() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = compressible_request(10).encode_to_vec();
        let mut body = gzip(&encoded);
        body.extend_from_slice(b"trailing junk not a gzip header");

        let response = export_metrics(State(state), gzip_headers(), Bytes::from(body)).await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "trailing bytes after the stream must be 400"
        );
    }

    /// ADR-0084 decision 1: an unsupported single coding, and any multi-coding
    /// list, are 415 (never treated as identity, which would hand prost a
    /// compressed body and produce the misleading 400 this ADR removes).
    #[tokio::test]
    async fn unsupported_content_encoding_is_415() {
        for value in ["deflate", "br", "gzip, gzip", "deflate, gzip"] {
            let state = state_with_limits(AdmissionLimits::default());
            let encoded = compressible_request(10).encode_to_vec();
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_ENCODING,
                HeaderValue::from_str(value).unwrap(),
            );
            let response = export_metrics(State(state), headers, Bytes::from(gzip(&encoded))).await;
            assert_eq!(
                response.status(),
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "Content-Encoding {value:?} must be 415"
            );
        }
    }

    /// ADR-0084 decision 1/4: the identity path is byte-for-byte unchanged. An
    /// uncompressed body (absent Content-Encoding) is charged its wire length,
    /// exactly as today, and `x-gzip` is honored as a gzip alias.
    #[tokio::test]
    async fn identity_path_charge_is_wire_length() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = compressible_request(50).encode_to_vec();

        let response = export_metrics(
            State(state.clone()),
            HeaderMap::new(),
            Bytes::from(encoded.clone()),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let usage = metrics_usage(&state).expect("a metrics usage row");
        assert_eq!(
            usage.bytes_admitted_total,
            encoded.len() as u64,
            "identity charge must equal the wire body length, as before this change"
        );
    }

    /// ADR-0084 decision 1: `x-gzip` is an accepted alias for `gzip`.
    #[tokio::test]
    async fn x_gzip_alias_is_accepted() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = compressible_request(50).encode_to_vec();
        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_ENCODING, HeaderValue::from_static("X-Gzip"));
        let response = export_metrics(State(state), headers, Bytes::from(gzip(&encoded))).await;
        assert_eq!(response.status(), StatusCode::OK, "x-gzip must be accepted");
    }

    /// ADR-0084 decision 4: an already-over-rate tenant sending a compressed body
    /// is rejected 429 by the compressed-size pre-check, WITHOUT the decompressor
    /// running.
    ///
    /// The body is deliberately not valid gzip. If the pre-check runs first (as
    /// it must), the request is 429 and the bytes are never fed to the decoder;
    /// if decompression ran before the check, the invalid body would surface as
    /// a 400. Asserting 429 therefore proves the decompressor did not run.
    ///
    /// Non-vacuity: delete the `peek_byte_rate` pre-check in
    /// `admit_and_decode_body`'s gzip arm and this returns 400 (the invalid body
    /// reaches the decoder).
    #[tokio::test]
    async fn over_rate_compressed_body_pre_check_rejects_429_without_decompressing() {
        // A tight byte-rate bucket: burst 16 bytes, so any real body is over.
        let limits = AdmissionLimits {
            ingest_byte_rate: RateLimit::Bounded {
                per_sec: 1,
                burst: 16,
            },
            ..AdmissionLimits::default()
        };
        let state = state_with_limits(limits);
        // Not a gzip stream at all, and larger than the 16-byte burst.
        let not_gzip = Bytes::from(vec![0x42u8; 1024]);

        let response = export_metrics(State(state.clone()), gzip_headers(), not_gzip).await;
        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "the compressed-size pre-check must reject before decompressing"
        );
        let usage = metrics_usage(&state).expect("a metrics usage row after the rejection");
        assert_eq!(
            usage.requests_rejected_byte_rate_total, 1,
            "the pre-check rejection is counted as a byte-rate rejection"
        );
        assert_eq!(
            usage.bytes_admitted_total, 0,
            "nothing is admitted and no tokens are consumed on the pre-check rejection"
        );
    }

    /// A permanent, non-retryable write rejection (`WriteError::SeriesValueKindMismatch`)
    /// must be 400, not the 503 a retryable failure gets: a well-behaved
    /// exporter that retries only on 503/429 would otherwise retry a request
    /// that can never succeed.
    ///
    /// Non-vacuity: change `export_metrics`'s final `Err(IngestRequestError::Write(write_err))`
    /// arm back to `(StatusCode::SERVICE_UNAVAILABLE, ...)` and this fails,
    /// observing 503 where 400 is asserted.
    #[tokio::test]
    async fn series_value_kind_mismatch_returns_400_not_503() {
        let state = state_with_limits(AdmissionLimits::default());
        let encoded = value_kind_mismatch_request().encode_to_vec();

        let response = export_metrics(State(state), HeaderMap::new(), Bytes::from(encoded)).await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "a series value-kind mismatch is a permanent, non-retryable rejection"
        );
    }

    /// A retryable write failure (`WriteError::Abandoned`, from a store that
    /// permanently fails every `Put`) must still be 503: the reorder that
    /// added the 400 arm must not swallow the retryable arm ahead of it.
    ///
    /// Non-vacuity: delete the `if write_err.is_retryable()` guard's arm (or
    /// reorder it after the bare `Write(write_err)` arm) and this fails,
    /// observing 400 where 503 is asserted.
    #[tokio::test]
    async fn a_retryable_write_error_still_returns_503() {
        let plan = FaultPlan::empty().with_rule(Rule::new(
            Op::Put,
            ScriptedFault::Permanent("boom".to_string()),
        ));
        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let state = state_with_store(store);
        let encoded = compressible_request(10).encode_to_vec();

        let response = export_metrics(State(state), HeaderMap::new(), Bytes::from(encoded)).await;
        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "an abandoned flush is retryable at the client"
        );
    }

    /// The dedicated buffer-budget-shed 429 arm must survive the reorder that
    /// added the retryable-vs-permanent split below it, for all three
    /// signals: it is easy to accidentally fold into the new
    /// `is_retryable()` guard (`BufferBudgetExceeded` is itself retryable),
    /// which would still return 503 instead of the 429 backpressure signal
    /// clients are meant to see.
    ///
    /// Non-vacuity: delete a handler's dedicated
    /// `Write(*WriteError::BufferBudgetExceeded)` arm (letting it fall
    /// through to the `is_retryable()` arm) and that handler's assertion
    /// fails, observing 503 where 429 is asserted.
    #[tokio::test]
    async fn buffer_budget_shed_still_returns_429() {
        let state = state_with_zero_budget();

        let metrics_response = export_metrics(
            State(state.clone()),
            HeaderMap::new(),
            Bytes::from(compressible_request(1).encode_to_vec()),
        )
        .await;
        assert_eq!(
            metrics_response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a zero metrics buffer budget must shed as 429, not 503"
        );

        let logs_response = export_logs(
            State(state.clone()),
            HeaderMap::new(),
            Bytes::from(minimal_log_request().encode_to_vec()),
        )
        .await;
        assert_eq!(
            logs_response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a zero log buffer budget must shed as 429, not 503"
        );

        let traces_response = export_traces(
            State(state),
            HeaderMap::new(),
            Bytes::from(minimal_trace_request().encode_to_vec()),
        )
        .await;
        assert_eq!(
            traces_response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a zero span buffer budget must shed as 429, not 503"
        );
    }

    /// ADR-0069 as amended by issue #1297: the gzip inflate path charges the
    /// process-wide ingest byte budget exactly what it retains -- the summed
    /// length of the chunks it holds, which is the ACTUAL decompressed length,
    /// not the compressed length and not the 64 MiB inflate cap -- and releases
    /// it at the instant decode consumes and frees those chunks, before the
    /// router takes its own buffered charge (finding 3), so the two never
    /// coexist. Proven on `admit_and_decode_body` in isolation plus the modelled
    /// drop point below.
    ///
    /// The charge is asserted against the retained chunk total, not only against
    /// the decompressed length: those are the same figure only because the body
    /// is retained as exactly-sized chunks. A growing `Vec<u8>` would hold spare
    /// capacity (and, mid-reallocation, two allocations) that a length-based
    /// charge would miss, and this assertion is what would catch that.
    ///
    /// Non-vacuity: change `decompress_gzip_capped_charged`'s per-chunk
    /// `budget.try_charge(read as u64)` to charge the compressed `body.len()`
    /// instead and the exact-equality assertion against `encoded.len()` fails.
    #[tokio::test]
    async fn in_flight_bytes_settle_to_the_actual_inflated_length() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(256 * 1024 * 1024));
        let state = state_with_store_and_budget(
            Arc::new(MemoryStore::new()),
            AdmissionLimits::default(),
            budget.clone(),
        );
        let encoded = compressible_request(2000).encode_to_vec();
        let compressed = gzip(&encoded);
        assert!(
            compressed.len() < encoded.len(),
            "fixture must compress: compressed={} decompressed={}",
            compressed.len(),
            encoded.len()
        );
        assert_eq!(budget.in_flight_bytes(), 0);

        let (body, charges) = admit_and_decode_body(
            &state,
            &gzip_headers(),
            &TenantId::new(TENANT),
            Signal::Metrics,
            Bytes::from(compressed.clone()),
        )
        .expect("gzip body admitted under a generous budget");

        assert_eq!(
            body.remaining(),
            encoded.len(),
            "the decoded body is the full inflate"
        );
        let chunk_lens = body.chunk_lens();
        let chunk_total: u64 = chunk_lens.iter().map(|len| *len as u64).sum();
        let charged: u64 = charges.iter().map(IngestByteCharge::bytes).sum();
        assert_eq!(
            charged,
            encoded.len() as u64,
            "the summed charge equals the decompressed length exactly"
        );
        assert_eq!(
            charged, chunk_total,
            "the summed charge equals the bytes actually retained, chunk for chunk"
        );
        assert!(
            chunk_lens.iter().all(|len| *len <= INFLATE_CHUNK_BYTES),
            "every retained chunk is bounded by the staging size, so none grew by \
             reallocation: {chunk_lens:?}"
        );
        assert_ne!(
            charged,
            compressed.len() as u64,
            "the charge is not the compressed length"
        );
        assert_eq!(
            budget.in_flight_bytes(),
            chunk_total,
            "in-flight bytes settle to the retained chunk total while the charge is held"
        );

        // The handler decodes the inflate chunks into owned protobuf structs;
        // `decode` consumes the chunked body, so the chunks are freed when it
        // returns and the handler then releases this charge, before the router
        // takes its own (issue #1297). Model that point: decode, then drop the
        // charge. The settled figure returns to zero at the instant the charge is
        // released, with no router charge ever coexisting with the inflate charge.
        let request = ExportMetricsServiceRequest::decode(body)
            .expect("the inflate chunks decode into an owned request");
        drop(charges);
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "the inflate charge is released once decode has freed the chunks, before any router charge"
        );
        // The decoded request owns its bytes, so it outlives the chunks it was
        // copied from: the release above is sound precisely because nothing
        // borrows them.
        assert_eq!(
            request.resource_metrics.len(),
            1,
            "the decoded request outlives the released inflate buffer"
        );
    }

    /// ADR-0069 as amended by issue #1297: a gzip inflate whose charge would
    /// push the budget past its ceiling is shed (429) mid-inflate, before the
    /// process has allocated the full expansion, and it charges nothing net.
    /// A pre-existing held charge stands in for a concurrent request already
    /// holding budget, so this is exactly the two-request sum-crosses-the-ceiling
    /// case: the ceiling leaves one byte less headroom than the body inflates to.
    ///
    /// Non-vacuity: replace `decompress_gzip_capped_charged`'s
    /// `Err(_) => return Err(GzipDecodeError::Shed)` with an unconditional
    /// charge and this returns the inflated body instead of a 429, so both the
    /// status assertion and `shed_total() == 1` fail.
    #[tokio::test]
    async fn second_concurrent_inflate_sheds_when_the_sum_would_cross_the_ceiling() {
        let encoded = compressible_request(2000).encode_to_vec();
        let compressed = gzip(&encoded);
        let inflated_len = encoded.len() as u64;
        // The prior charge plus (inflated_len - 1) is the whole ceiling, so the
        // inflate's final byte crosses it.
        const PRIOR: u64 = 4096;
        let ceiling = PRIOR + inflated_len - 1;
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(ceiling));
        let state = state_with_store_and_budget(
            Arc::new(MemoryStore::new()),
            AdmissionLimits::default(),
            budget.clone(),
        );

        let held = budget
            .try_charge(PRIOR)
            .expect("prior charge fits under ceiling");
        assert_eq!(budget.in_flight_bytes(), PRIOR);

        let response = admit_and_decode_body(
            &state,
            &gzip_headers(),
            &TenantId::new(TENANT),
            Signal::Metrics,
            Bytes::from(compressed),
        )
        .expect_err("the inflate crosses the ceiling and is shed");
        assert_eq!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "an inflate over the remaining budget is shed as 429"
        );
        assert_eq!(budget.shed_total(), 1, "exactly one shed is counted");
        assert_eq!(
            budget.in_flight_bytes(),
            PRIOR,
            "the shed request refunded every partial chunk; only the prior charge remains"
        );

        drop(held);
        assert_eq!(budget.in_flight_bytes(), 0);
    }
}
