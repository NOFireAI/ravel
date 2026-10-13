//! Request-body decode for the ingest handlers: OTLP HTTP's `Content-Encoding`
//! dispatch and gzip inflate, and Remote Write's snappy decode, each charging
//! the process-wide ingest byte budget for the bytes it inflates.

use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::{Buf, Bytes};
use ravel_cpu_gate::{CpuGateError, JobSize, WriteGate, WriteSite};
use ravel_ingest::{IngestByteBudget, IngestByteCharge};
use ravel_remote_write::{ResolvedRequest, Rw1DecodeError, Rw2DecodeError};
use ravel_types::{Signal, TenantId};

use crate::ingest_admission::{admission_rejection_response, ingest_buffer_budget_shed_response};
use crate::otlp_http::{GatewayState, now_ns};
use crate::remote_write::{DecodeChargeError, RemoteWriteVersion};

/// Layer 1 (ADR-0051 section 2): the wire-body cap on every OTLP HTTP
/// endpoint, ahead of protobuf decode. This bounds the *compressed* body when a
/// client sends gzip; the decompressed size is bounded independently by
/// [`MAX_DECOMPRESSED_OTLP_BODY_BYTES`], exactly as Remote Write pairs its two
/// caps (ADR-0084 decision 3).
pub(crate) const MAX_REQUEST_BODY_BYTES: usize = 16 * 1024 * 1024;

/// Cap on the decompressed size of a gzip OTLP body (ADR-0084 decision 3),
/// matching Remote Write's post-Snappy cap. Enforced *while* expanding, not
/// after, by reading the decoder through `take(cap + 1)`: a decompression bomb
/// is refused as it is being inflated rather than after Ravel has already
/// allocated it (the same discipline as `ravel-otap`'s `decompress_capped`).
pub(crate) const MAX_DECOMPRESSED_OTLP_BODY_BYTES: usize = 64 * 1024 * 1024;

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
pub(crate) struct ChunkedBody {
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
pub(crate) enum GzipDecodeError {
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
pub(crate) fn decompress_gzip_capped_charged(
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
    // uncharged allocation of decompressed bytes is at most one chunk.
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

/// Deflate's maximum expansion: zlib's technical notes give 1032:1, one
/// 258-byte match per two bits once both Huffman codes are one bit long.
const DEFLATE_MAX_EXPANSION: u64 = 1032;

/// The size the write gate compares against its inline floor for the inflate
/// of a `wire_len`-byte gzip body: an upper bound on what it inflates to
/// (ADR-1702 decision 4). Gzip records a member's inflated length, modulo
/// 2^32, only in the trailer after that member's deflate data, so nothing
/// trustworthy is known before inflating. The bound comes from the format,
/// `wire_len` times deflate's maximum expansion, capped at `cap`, past which
/// the inflate refuses the body.
fn gzip_job_bytes(wire_len: u64, cap: usize) -> u64 {
    wire_len
        .saturating_mul(DEFLATE_MAX_EXPANSION)
        .min(cap as u64)
}

/// The inflated gzip body.
type GzipInflate = Result<(Vec<Bytes>, Vec<IngestByteCharge>), GzipDecodeError>;

/// Runs [`decompress_gzip_capped_charged`] for `body` on `write_gate` under the
/// `otlp_http_gzip` site, sized by [`gzip_job_bytes`] (ADR-1702), or inline
/// when no gate is attached. The job takes each chunk's charge as it inflates
/// and returns the charges with the chunks, so nothing is charged while it
/// waits for a permit, and a dropped request releases the charges only when
/// the job returns. `Err` is the gate returning no result.
async fn inflate_gzip_gated(
    write_gate: Option<&WriteGate>,
    body: Bytes,
    budget: Arc<IngestByteBudget>,
) -> Result<GzipInflate, CpuGateError> {
    let size = gzip_job_bytes(body.len() as u64, MAX_DECOMPRESSED_OTLP_BODY_BYTES);
    let inflate =
        move || decompress_gzip_capped_charged(&body, MAX_DECOMPRESSED_OTLP_BODY_BYTES, &budget);
    match write_gate {
        Some(gate) => {
            gate.run(WriteSite::OtlpHttpGzip, JobSize::Bytes(size), inflate)
                .await
        }
        None => Ok(inflate()),
    }
}

/// The write gate produced no inflate result: the job panicked, or the runtime
/// dropped it while shutting down. Nothing was decoded or written, so a retry
/// is safe; a panic is the server's fault, a cancelled job a transient one.
fn gate_failure_response(err: CpuGateError) -> Response {
    let status = match err {
        CpuGateError::Panicked => StatusCode::INTERNAL_SERVER_ERROR,
        CpuGateError::Cancelled | CpuGateError::Closed => StatusCode::SERVICE_UNAVAILABLE,
    };
    (status, format!("gzip inflate failed: {err}")).into_response()
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
pub(crate) async fn admit_and_decode_body(
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
            let inflated = match inflate_gzip_gated(
                state.write_gate.as_deref(),
                body,
                Arc::clone(&state.budget),
            )
            .await
            {
                Ok(inflated) => inflated,
                Err(err) => return Err(Box::new(gate_failure_response(err))),
            };
            let (chunks, decode_charge) = match inflated {
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

/// Snappy decompression cap for a single Remote Write request body, applied
/// before allocation (same discipline as `ravel-otap`'s
/// `max_decompressed_payload_bytes`).
const MAX_DECOMPRESSED_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

/// Why a Remote Write body's snappy inflate could not be charged against the
/// process-wide ingest byte budget.
#[derive(Debug, PartialEq, Eq)]
enum SnappyChargeError {
    /// Charging the decompressed bytes would push the budget past its ceiling
    /// (ADR-0069, amended by issue #1419): HTTP 429, the same shed response the
    /// buffered charge takes.
    Shed,
}

/// Charges the process-wide ingest byte budget for the bytes this snappy body
/// will inflate to, **before** [`ravel_remote_write`] allocates the output
/// buffer (ADR-0069 as amended by issue #1419).
///
/// Snappy block format declares its decompressed length in a varint header, so
/// unlike gzip the exact figure is known without inflating anything:
/// `snap::raw::decompress_len` reads only that header. `ravel-remote-write`'s
/// `snappy::decompress` then allocates `vec![0u8; len]` and truncates to the
/// bytes actually written, which for a well-formed body is `len` exactly. The
/// charge is therefore the actual inflated length and the actual size of the
/// allocation it pays for, taken one step ahead of it: no over-charge of a
/// well-compressing sender, no undercount, and no growing buffer whose spare
/// capacity would escape the count (the failure mode
/// [`ChunkedBody`] exists to avoid on the gzip path, where no
/// declared length is available and the charge has to be taken chunk by chunk).
///
/// The order is cap, then budget, matching the gzip path: a body whose header
/// claims more than `cap` takes no charge and is left to the decoder, which
/// rejects it with the existing typed snappy error (HTTP 400). A tight budget
/// therefore cannot turn an over-cap body into a 429, and the charge for such a
/// body is never taken at all.
///
/// A malformed header is likewise left to the decoder rather than guessed at:
/// nothing is charged and the decode call that follows returns its own typed
/// error. An empty body decompresses to nothing and takes no charge, mirroring
/// `snappy::decompress`'s own empty-input short circuit.
///
/// The caller holds the returned guard through protobuf decode and
/// normalization and drops it immediately before the router takes its own
/// buffered charge, so the two never coexist (issue #1297 finding 3); on any
/// error path the guard drops and refunds the budget exactly.
fn charge_snappy_inflate(
    body: &[u8],
    cap: usize,
    budget: &Arc<IngestByteBudget>,
) -> Result<Option<IngestByteCharge>, SnappyChargeError> {
    if body.is_empty() {
        return Ok(None);
    }
    let Ok(len) = snap::raw::decompress_len(body) else {
        return Ok(None);
    };
    if len > cap {
        return Ok(None);
    }
    match budget.try_charge(len as u64) {
        Ok(charge) => Ok(Some(charge)),
        Err(_) => Err(SnappyChargeError::Shed),
    }
}

/// The size the write gate compares against its inline floor for the decode of
/// `body`: the inflated length the snappy header declares, capped at `cap`
/// (ADR-1702 decision 4). The decoder writes into a buffer of exactly the
/// declared length and fails a body that does not fill it, so the declared
/// length bounds both the inflate and the protobuf decode after it. A header
/// over `cap` is rejected before anything is inflated, so `cap` bounds the
/// job however large the claim. A header that does not parse is rejected
/// before any inflate too, and is sized by its compressed length.
fn snappy_job_bytes(body: &[u8], cap: usize) -> u64 {
    match snap::raw::decompress_len(body) {
        Ok(declared) => declared.min(cap) as u64,
        Err(_) => body.len() as u64,
    }
}

/// Snappy-decompresses and protobuf-decodes `body` for `version`, holding a
/// process-wide ingest byte budget charge for the inflated bytes across the
/// whole call (ADR-0069 as amended by issue #1419).
///
/// Returns the decoded request alongside the charge guard, the same shape
/// [`admit_and_decode_body`] returns for the gzip path. The caller
/// keeps the guard alive through normalization and drops it before the router
/// charges the normalized batch.
///
/// With `write_gate` set, the snappy inflate and protobuf decode run on it as
/// one job under the `remote_write_snappy` site, sized by [`snappy_job_bytes`]
/// (ADR-1702). The charge moves into the job and comes back with the decoded
/// request, so it is held while the job waits for a permit and, if the request
/// is dropped meanwhile, released only when the job returns.
///
/// `Err` says which response to return: 429 for a budget shed, 400 for a body
/// the decoder rejected (unchanged from before this charge existed), 500 or 503
/// when the gate returned no result.
pub(crate) async fn decode_body_charged(
    body: &Bytes,
    version: RemoteWriteVersion,
    budget: &Arc<IngestByteBudget>,
    write_gate: Option<&WriteGate>,
) -> Result<(ResolvedRequest, Option<IngestByteCharge>), DecodeChargeError> {
    // Charge before the decoder allocates: a body whose inflate would cross the
    // ceiling is shed here, so the process never grows by the expansion.
    let charge = match charge_snappy_inflate(body, MAX_DECOMPRESSED_PAYLOAD_BYTES, budget) {
        Ok(charge) => charge,
        Err(SnappyChargeError::Shed) => return Err(DecodeChargeError::Shed),
    };
    let input_bytes = snappy_job_bytes(body, MAX_DECOMPRESSED_PAYLOAD_BYTES);
    let body = body.clone();
    let decode = move || {
        let resolved = match version {
            RemoteWriteVersion::V1 => {
                ravel_remote_write::decode_write_request(&body, MAX_DECOMPRESSED_PAYLOAD_BYTES)
                    .map_err(|err: Rw1DecodeError| err.to_string())
            }
            RemoteWriteVersion::V2 => {
                ravel_remote_write::decode_request(&body, MAX_DECOMPRESSED_PAYLOAD_BYTES)
                    .map_err(|err: Rw2DecodeError| err.to_string())
            }
        };
        (resolved, charge)
    };
    let (resolved, charge) = match write_gate {
        Some(gate) => gate
            .run(
                WriteSite::RemoteWriteSnappy,
                JobSize::Bytes(input_bytes),
                decode,
            )
            .await
            .map_err(DecodeChargeError::Gate)?,
        None => decode(),
    };
    match resolved {
        // On this path `charge` drops here, refunding the budget exactly.
        Err(message) => Err(DecodeChargeError::Decode(message)),
        Ok(resolved) => Ok((resolved, charge)),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::time::Duration;

    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use prost::Message;
    use ravel_ingest::{AdmissionLimits, IngestByteBudgetLimit};
    use ravel_object_store::memory::MemoryStore;
    use ravel_otlp::IngestLimits;
    use ravel_remote_write::normalize_resolved;

    use super::*;
    use crate::otlp_http::tests::{
        TENANT, compressible_request, gzip, gzip_headers, state_with_store_and_budget,
    };
    use crate::remote_write::tests::FIXTURE_NOW_NS;
    use crate::remote_write::{INGEST_CONCURRENCY_RETRY_AFTER_SECONDS, RETRY_AFTER_HEADER};

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
        .await
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
        .await
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

    fn gzip_gate(inline_floor_bytes: u64) -> Arc<WriteGate> {
        Arc::new(WriteGate::new(
            ravel_cpu_gate::CpuGateConfig {
                inline_floor_bytes,
                ..ravel_cpu_gate::CpuGateConfig::with_permits(1)
            },
            Arc::new(ravel_cpu_gate::InstantClock::new()),
        ))
    }

    fn gzip_site(gate: &WriteGate) -> (u64, u64) {
        gate.snapshot()
            .sites
            .iter()
            .find(|s| s.site == WriteSite::OtlpHttpGzip)
            .map_or((0, 0), |s| (s.jobs, s.inline))
    }

    /// A gzip inflate is sized by deflate's expansion bound on its compressed
    /// length, capped: a body that compresses below the gate's default floor
    /// but can inflate above it runs on the gate, and only a body too small to
    /// reach the floor at 1032:1 runs inline.
    #[tokio::test]
    async fn the_gzip_job_is_sized_by_the_deflate_expansion_bound() {
        assert_eq!(
            gzip_job_bytes(100, MAX_DECOMPRESSED_OTLP_BODY_BYTES),
            100 * DEFLATE_MAX_EXPANSION
        );
        assert_eq!(
            gzip_job_bytes(1024 * 1024, MAX_DECOMPRESSED_OTLP_BODY_BYTES),
            MAX_DECOMPRESSED_OTLP_BODY_BYTES as u64
        );

        let gate = gzip_gate(ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES);
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
        let large = Bytes::from(gzip(&vec![0u8; 1024 * 1024]));
        assert!((large.len() as u64) < ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES);
        let inflated = inflate_gzip_gated(Some(&gate), large, Arc::clone(&budget))
            .await
            .expect("gate result")
            .expect("inflate");
        assert_eq!(gzip_site(&gate), (1, 0));
        drop(inflated);

        let tiny = Bytes::from(gzip(b"hi"));
        assert!(gzip_job_bytes(tiny.len() as u64, MAX_DECOMPRESSED_OTLP_BODY_BYTES) < 256 * 1024);
        inflate_gzip_gated(Some(&gate), tiny, Arc::clone(&budget))
            .await
            .expect("gate result")
            .expect("inflate");
        assert_eq!(gzip_site(&gate), (1, 1));
    }

    /// Decision 6 for the gzip inflate: nothing is charged while the job waits
    /// for a permit, and the charges the job took come back with the chunks
    /// and stay held until the caller drops them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_gzip_charges_come_back_with_the_chunks() {
        let gate = gzip_gate(0);
        let (release, parked) = std::sync::mpsc::channel::<()>();
        let blocker_gate = Arc::clone(&gate);
        let blocker = tokio::spawn(async move {
            blocker_gate
                .run(WriteSite::OtapDecode, JobSize::Bytes(0), move || {
                    let _ = parked.recv();
                })
                .await
                .expect("the parked job runs");
        });
        while gate.running() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }

        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
        let body = Bytes::from(gzip(&vec![5u8; 100_000]));
        let inflate = tokio::spawn({
            let (gate, budget) = (Arc::clone(&gate), Arc::clone(&budget));
            async move { inflate_gzip_gated(Some(&gate), body, budget).await }
        });
        while gate.queued() == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "nothing is charged while the job waits"
        );

        release.send(()).expect("release the parked job");
        blocker.await.expect("blocker task");
        let (chunks, charges) = inflate
            .await
            .expect("inflate task")
            .expect("gate result")
            .expect("inflate");
        assert_eq!(chunks.iter().map(Bytes::len).sum::<usize>(), 100_000);
        assert_eq!(
            budget.in_flight_bytes(),
            100_000,
            "the charges come back with the chunks, still held"
        );
        drop(charges);
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    /// A gzip inflate the gate returns no result for answers 500 when the job
    /// panicked and 503 when it was cancelled.
    #[test]
    fn a_gzip_gate_failure_answers_500_for_a_panic_and_503_for_a_cancel() {
        assert_eq!(
            gate_failure_response(CpuGateError::Panicked).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            gate_failure_response(CpuGateError::Cancelled).status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            gate_failure_response(CpuGateError::Closed).status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    /// A snappy-compressed RW2 body of `points` samples on one series, and the
    /// exact length it inflates to. Repeated identical samples compress well, so
    /// the inflated length is many times the compressed length: that gap is what
    /// makes charging the wrong quantity visible.
    fn rw2_snappy_body(points: usize) -> (Bytes, u64) {
        use prost::Message as _;
        use ravel_remote_write::proto::write_v2::{
            Request as ProtoRequestV2, Sample as ProtoSampleV2, TimeSeries as ProtoTimeSeriesV2,
        };

        let ts_ms = FIXTURE_NOW_NS / 1_000_000;
        let request = ProtoRequestV2 {
            symbols: vec![
                String::new(),
                "__name__".to_string(),
                "requests_total".to_string(),
                "job".to_string(),
                "bench".to_string(),
            ],
            timeseries: (0..points)
                .map(|i| ProtoTimeSeriesV2 {
                    labels_refs: vec![1, 2, 3, 4],
                    samples: vec![ProtoSampleV2 {
                        value: 1.0,
                        timestamp: ts_ms - i as i64,
                        start_timestamp: 0,
                    }],
                    histograms: vec![],
                    exemplars: vec![],
                    metadata: None,
                })
                .collect(),
        };
        let plain = request.encode_to_vec();
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&plain)
            .expect("fixture compresses");
        assert!(
            compressed.len() < plain.len(),
            "fixture must compress: compressed={} inflated={}",
            compressed.len(),
            plain.len()
        );
        (Bytes::from(compressed), plain.len() as u64)
    }

    /// Issue #1419 requirement 3: the settled charge is the ACTUAL inflated
    /// length, not the 64 MiB cap and not the compressed wire length. Snappy
    /// declares its decompressed size in a varint header, so the figure is
    /// exact and is taken before `ravel-remote-write` allocates the buffer it
    /// pays for.
    ///
    /// Non-vacuity: charge `body.len()` (the compressed length) or
    /// `MAX_DECOMPRESSED_PAYLOAD_BYTES` (the cap) in `charge_snappy_inflate`
    /// and the first assertion fails, because the fixture pins all three
    /// quantities as distinct.
    #[test]
    fn snappy_inflate_charge_equals_the_exact_inflated_length() {
        let (body, inflated_len) = rw2_snappy_body(400);
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(64 * 1024 * 1024));

        let charge = charge_snappy_inflate(&body, MAX_DECOMPRESSED_PAYLOAD_BYTES, &budget)
            .expect("a body inside the budget is charged, not shed")
            .expect("a non-empty body takes a charge");

        assert_eq!(
            charge.bytes(),
            inflated_len,
            "the charge is the exact inflated length"
        );
        assert_ne!(
            inflated_len,
            body.len() as u64,
            "the fixture must distinguish the inflated length from the compressed length"
        );
        assert_ne!(
            inflated_len, MAX_DECOMPRESSED_PAYLOAD_BYTES as u64,
            "the fixture must distinguish the inflated length from the cap"
        );
        assert_eq!(
            budget.in_flight_bytes(),
            inflated_len,
            "the budget holds exactly the inflated length while the charge lives"
        );

        drop(charge);
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "dropping the guard refunds the budget exactly"
        );
    }

    /// Issue #1419 requirement 2: the charge is held through decode AND
    /// normalization, not released the moment the inflate finishes. Reading
    /// `in_flight_bytes` after `decode_body_charged` returns and again after
    /// `normalize_resolved` has run pins both instants.
    ///
    /// Non-vacuity: drop the charge inside `decode_body_charged` (return only
    /// the request) and the first two assertions read 0 instead of the inflated
    /// length.
    #[tokio::test]
    async fn inflate_charge_is_held_through_decode_and_normalize() {
        let tenant = TenantId::new("acme");
        let (body, inflated_len) = rw2_snappy_body(400);
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(64 * 1024 * 1024));

        let Ok((resolved, charge)) =
            decode_body_charged(&body, RemoteWriteVersion::V2, &budget, None).await
        else {
            panic!("a well-formed body inside the budget decodes");
        };
        let charge = charge.expect("a non-empty body carries its inflate charge past decode");

        assert_eq!(
            budget.in_flight_bytes(),
            inflated_len,
            "the inflate charge is still held after decode"
        );

        let normalized =
            normalize_resolved(&tenant, resolved, &IngestLimits::default(), FIXTURE_NOW_NS);
        assert!(
            !normalized.points.is_empty(),
            "the fixture normalizes to real points, so normalization did run"
        );
        assert_eq!(
            budget.in_flight_bytes(),
            inflated_len,
            "the inflate charge is still held after normalization, before the router charge"
        );

        drop(charge);
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "the charge is released before the router takes its own"
        );
    }

    /// A body whose inflate would cross the ceiling is shed before the
    /// decompressed buffer is allocated: `charge_snappy_inflate` reports
    /// [`SnappyChargeError::Shed`], the budget is left untouched, and
    /// `decode_body_charged` turns that into 429 + `Retry-After` rather than
    /// decoding the body.
    ///
    /// Non-vacuity: remove the `budget.try_charge` call and the body inflates
    /// and decodes fine, so both the error and the 429 disappear.
    #[tokio::test]
    async fn inflate_over_the_ceiling_is_shed_without_allocating() {
        let (body, inflated_len) = rw2_snappy_body(400);
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(inflated_len - 1));

        assert!(
            matches!(
                charge_snappy_inflate(&body, MAX_DECOMPRESSED_PAYLOAD_BYTES, &budget),
                Err(SnappyChargeError::Shed)
            ),
            "one byte short of the inflated length sheds"
        );
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "a shed request holds no bytes at all"
        );
        assert_eq!(budget.shed_total(), 1, "the shed is counted exactly once");

        let Err(error) = decode_body_charged(&body, RemoteWriteVersion::V2, &budget, None).await
        else {
            panic!("a shed body produces a response, not a decoded request");
        };
        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(RETRY_AFTER_HEADER)
                .expect("the shed response carries Retry-After")
                .to_str()
                .expect("ascii header value"),
            INGEST_CONCURRENCY_RETRY_AFTER_SECONDS.to_string(),
        );

        // The exact same body fits once the ceiling covers the inflated length,
        // so the shed above was the budget and nothing else.
        let roomy = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(inflated_len));
        assert!(
            charge_snappy_inflate(&body, MAX_DECOMPRESSED_PAYLOAD_BYTES, &roomy)
                .expect("a ceiling equal to the inflated length admits the body")
                .is_some()
        );
    }

    /// Cap before budget, the same order the gzip path uses: a body whose
    /// declared inflate exceeds the cap takes no charge and is left to the
    /// decoder's existing typed error (HTTP 400), so a tight budget cannot turn
    /// an over-cap body into a 429. An empty or malformed body likewise charges
    /// nothing.
    #[test]
    fn over_cap_empty_and_malformed_bodies_take_no_charge() {
        let (body, inflated_len) = rw2_snappy_body(400);
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1));

        let over_cap = charge_snappy_inflate(&body, (inflated_len - 1) as usize, &budget)
            .expect("an over-cap body is not shed");
        assert!(
            over_cap.is_none(),
            "an over-cap body takes no charge; the decoder rejects it"
        );

        let empty = charge_snappy_inflate(&Bytes::new(), MAX_DECOMPRESSED_PAYLOAD_BYTES, &budget)
            .expect("an empty body is not shed");
        assert!(empty.is_none(), "an empty body inflates to nothing");

        // A truncated snappy header has no readable decompressed length.
        let malformed = Bytes::from_static(&[0xff, 0xff, 0xff]);
        let malformed = charge_snappy_inflate(&malformed, MAX_DECOMPRESSED_PAYLOAD_BYTES, &budget)
            .expect("a malformed body is not shed");
        assert!(
            malformed.is_none(),
            "a malformed header charges nothing and is left to the decoder"
        );

        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "none of these paths charged the budget"
        );
        assert_eq!(
            budget.shed_total(),
            0,
            "none of these paths reached the budget at all"
        );
    }

    fn snappy(data: &[u8]) -> Bytes {
        Bytes::from(
            snap::raw::Encoder::new()
                .compress_vec(data)
                .expect("compress"),
        )
    }

    fn write_gate(inline_floor_bytes: u64) -> Arc<WriteGate> {
        Arc::new(WriteGate::new(
            ravel_cpu_gate::CpuGateConfig {
                inline_floor_bytes,
                ..ravel_cpu_gate::CpuGateConfig::with_permits(1)
            },
            Arc::new(ravel_cpu_gate::InstantClock::new()),
        ))
    }

    fn snappy_site(gate: &WriteGate) -> (u64, u64) {
        gate.snapshot()
            .sites
            .iter()
            .find(|s| s.site == WriteSite::RemoteWriteSnappy)
            .map_or((0, 0), |s| (s.jobs, s.inline))
    }

    /// The decode job is sized by the declared inflated length, capped, not by
    /// the compressed body: a body that compresses below the gate's default
    /// floor but inflates above it runs on the gate.
    #[tokio::test]
    async fn the_snappy_job_is_sized_by_the_declared_inflated_length() {
        let body = snappy(&vec![0u8; 1024 * 1024]);
        assert!((body.len() as u64) < ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES);
        assert_eq!(
            snappy_job_bytes(&body, MAX_DECOMPRESSED_PAYLOAD_BYTES),
            1024 * 1024
        );
        assert_eq!(snappy_job_bytes(&body, 1000), 1000, "capped");
        let malformed = [0xffu8, 0xff, 0xff];
        assert_eq!(
            snappy_job_bytes(&malformed, MAX_DECOMPRESSED_PAYLOAD_BYTES),
            3
        );

        let gate = write_gate(ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES);
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
        // A megabyte of zeros is not a valid request; the job still ran.
        let _ = decode_body_charged(&body, RemoteWriteVersion::V1, &budget, Some(&gate)).await;
        assert_eq!(snappy_site(&gate), (1, 0));
        let small = snappy(&[0u8; 100]);
        let _ = decode_body_charged(&small, RemoteWriteVersion::V1, &budget, Some(&gate)).await;
        assert_eq!(snappy_site(&gate), (1, 1));
    }

    /// Decision 6: the inflate charge is taken before submission and held
    /// while the job waits for a permit. A request dropped while it waits
    /// refunds the charge, since the job that held it never runs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_snappy_charge_is_held_while_the_job_waits() {
        let gate = write_gate(0);
        let (release, parked) = std::sync::mpsc::channel::<()>();
        let blocker_gate = Arc::clone(&gate);
        let blocker = tokio::spawn(async move {
            blocker_gate
                .run(WriteSite::OtapDecode, JobSize::Bytes(0), move || {
                    let _ = parked.recv();
                })
                .await
                .expect("the parked job runs");
        });
        while gate.running() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
        let body = snappy(&[0u8; 1000]);
        let waiter = tokio::spawn({
            let (gate, budget) = (Arc::clone(&gate), Arc::clone(&budget));
            async move {
                decode_body_charged(&body, RemoteWriteVersion::V1, &budget, Some(&gate))
                    .await
                    .is_ok()
            }
        });
        while gate.queued() == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            budget.in_flight_bytes(),
            1000,
            "the declared inflated length is charged while the job waits"
        );

        waiter.abort();
        let _ = waiter.await;
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "a request dropped before its job ran refunds the charge"
        );
        release.send(()).expect("release the parked job");
        blocker.await.expect("blocker task");
        assert_eq!(snappy_site(&gate), (0, 0), "the dropped job never ran");
    }
}
