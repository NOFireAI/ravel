//! OTAP gRPC stream state machine: per-stream Arrow IPC decode.
//!
//! Scope: this module turns `BatchArrowRecords` messages into
//! `RecordBatch`es. It does not interpret column semantics (ids, parent_id
//! joins, AnyValue unions, delta encodings) at all -- that is the columnar
//! normalizer's job. This is
//! intentional: the OTAP spec's id-column transport encodings (plain, delta,
//! quasi-delta; see otap-spec.md section 6.4) are a property of column
//! *values*, not of Arrow IPC framing, so they do not affect how we decode
//! bytes into a `RecordBatch` here.
//!
//! Per otap-spec.md section 4.4, an Arrow IPC Stream is identified within a
//! gRPC stream by the pair (`ArrowPayload.type`, `ArrowPayload.schema_id`).
//! We keep one `arrow_ipc::reader::StreamDecoder` per such pair, matching
//! the incremental, stateful nature of Arrow IPC (schema and dictionaries
//! arrive once, record batches reference them by stream state).

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::io::{self, Read, Write};
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;

use arrow::array::ArrayData;
use arrow::buffer::{Buffer as ArrowBuffer, MutableBuffer};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow_ipc::MessageHeader;
use arrow_ipc::reader::StreamDecoder;
use ravel_cpu_gate::{CpuGateError, JobSize, WriteGate, WriteSite};
use thiserror::Error;

use crate::proto::experimental::arrow::v1::{ArrowPayload, ArrowPayloadType, BatchArrowRecords};

/// Hard resource caps for one gRPC stream's decode state.
///
/// Defaults match the caps enumerated in docs/otap-ingest.md (spec section
/// 19 threat model: zstd bombs, unbounded schema/dictionary growth, and
/// oversized batches before we ever materialize Arrow arrays for them).
#[derive(Debug, Clone, Copy)]
pub struct StreamConfig {
    /// Cap on the Arrow IPC bytes a single `ArrowPayload.record` carries:
    /// checked while decompressing a zstd frame (before the buffer is allowed
    /// to grow past it), against the length of a payload with no frame, and,
    /// before arrow's reader allocates them, against the decoded size its
    /// ZSTD body-compressed buffers declare. A payload using any other IPC
    /// body compression codec is refused before decode.
    pub max_decompressed_payload_bytes: u64,
    /// Cumulative cap, across the whole gRPC stream, on the decoded body bytes
    /// of `DictionaryBatch` IPC messages (initial dictionaries and deltas):
    /// the stored length of an uncompressed body, and the declared decoded
    /// length of a ZSTD body-compressed one, not its compressed size.
    pub max_stream_dictionary_bytes: u64,
    /// Cap on the number of distinct (payload_type, schema_id) IPC streams
    /// a single gRPC stream may open.
    pub max_schemas_per_stream: usize,
    /// Cap on rows in a single Arrow `RecordBatch`, read from IPC metadata
    /// and checked before the batch is materialized.
    pub max_rows_per_batch: i64,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            max_decompressed_payload_bytes: 16 * 1024 * 1024,
            max_stream_dictionary_bytes: 64 * 1024 * 1024,
            max_schemas_per_stream: 256,
            max_rows_per_batch: 1_000_000,
        }
    }
}

/// A batch that failed to decode. The caller nacks `batch_id` and keeps
/// using the same `StreamState` -- nothing here corrupts shared state.
#[derive(Debug, Error)]
pub enum BatchError {
    #[error("arrow payload has invalid or UNKNOWN payload type (raw={raw})")]
    UnknownPayloadType { raw: i32 },
    #[error("decompressed payload exceeds cap of {limit} bytes")]
    DecompressedPayloadTooLarge { limit: u64 },
    #[error("zstd decompression failed: {0}")]
    Decompression(String),
    #[error("malformed arrow IPC framing: {0}")]
    MalformedIpc(String),
    #[error("arrow IPC body compression codec {codec} is not supported; only ZSTD is")]
    UnsupportedBodyCompression { codec: String },
    #[error("record batch row count {actual} exceeds cap of {limit}")]
    RowCountExceeded { limit: i64, actual: i64 },
    #[error("stream dictionary budget of {limit} bytes exceeded")]
    DictionaryBudgetExceeded { limit: u64 },
    #[error("stream schema budget of {limit} distinct schemas exceeded")]
    SchemaBudgetExceeded { limit: usize },
    #[error("arrow IPC decode failed: {0}")]
    IpcDecode(String),
    #[error("arrow IPC decode panicked on malformed input: {0}")]
    InternalPanic(String),
}

/// The IPC stream state for one (payload_type, schema_id) pair is corrupt.
/// Per otap-spec.md section 4.4, Arrow IPC streams are stateful: once a
/// `StreamDecoder` errors after it has already accepted a schema, its
/// internal buffering/dictionary state cannot be trusted for further feeds.
/// We treat this conservatively as fatal for the whole `StreamState`; the
/// caller must tear down and re-establish the gRPC stream.
#[derive(Debug, Error)]
pub enum StreamError {
    #[error(
        "IPC stream corrupted for payload_type={payload_type:?} schema_id={schema_id:?}: {detail}"
    )]
    Corrupted {
        payload_type: ArrowPayloadType,
        schema_id: String,
        detail: String,
    },
    #[error("stream was already torn down by a previous corruption")]
    Poisoned,
    /// The write gate returned no result for a payload's decompression. That
    /// payload never reached its decoder, but the batch's earlier payloads
    /// already fed theirs, so the stream's decode state has advanced past
    /// what a nack tells the client. A resend on this stream would feed those
    /// payloads' schema and dictionary messages to decoders that already took
    /// them, and any the failed payload carried never reach its decoder, so
    /// the stream is poisoned like a corrupted one.
    #[error("payload decode job did not complete on the CPU write gate: {0}")]
    Gate(CpuGateError),
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error(transparent)]
    Batch(#[from] BatchError),
    #[error(transparent)]
    Stream(#[from] StreamError),
}

/// Result of decoding one `BatchArrowRecords` message: every `RecordBatch`
/// materialized across all of its payloads, each tagged with its payload
/// type. Column semantics (joins, id decoding) are left to the normalizer.
#[derive(Debug)]
pub struct DecodedBatch {
    pub batch_id: i64,
    pub payloads: Vec<(ArrowPayloadType, RecordBatch)>,
}

/// One scanned Arrow IPC encapsulated message: just enough metadata to
/// enforce caps before the real decoder materializes anything.
struct ScannedMessage {
    header: MessageHeader,
    /// The bytes arrow's reader produces from this message's body: its
    /// stored length without IPC body compression, and with ZSTD body
    /// compression (the only codec [`scan_messages`] admits) the sum of the
    /// uncompressed lengths each compressed buffer's 8-byte prefix declares.
    /// The reader allocates at most that length for a ZSTD buffer before it
    /// decompresses it, and refuses output of any other length.
    decoded_body_len: u64,
    row_count: Option<i64>,
}

const CONTINUATION_MARKER: [u8; 4] = [0xff, 0xff, 0xff, 0xff];

/// Arrow IPC's prefix on a body-compressed buffer: its uncompressed length
/// as a little-endian i64, or -1 when the bytes after it are stored raw.
const COMPRESSED_BUFFER_PREFIX_BYTES: usize = 8;
const UNCOMPRESSED_BUFFER_PREFIX: i64 = -1;

/// The bytes arrow's reader produces from the buffers of `batch`, whose
/// metadata declares ZSTD IPC body compression, read from each buffer's prefix
/// in `body`. A buffer whose prefix lies outside `body` or carries a length
/// arrow rejects counts as its stored length; arrow refuses it without
/// decompressing it, with an error or a panic `StreamState` converts to one.
fn compressed_body_decoded_len(batch: arrow_ipc::RecordBatch<'_>, body: &[u8]) -> u64 {
    let Some(buffers) = batch.buffers() else {
        return 0;
    };
    buffers.iter().fold(0u64, |total, buffer| {
        let stored = buffer.length().max(0) as u64;
        let prefix = usize::try_from(buffer.offset())
            .ok()
            .and_then(|start| body.get(start..start.checked_add(COMPRESSED_BUFFER_PREFIX_BYTES)?))
            .filter(|_| stored >= COMPRESSED_BUFFER_PREFIX_BYTES as u64)
            .and_then(|prefix| <[u8; 8]>::try_from(prefix).ok())
            .map(i64::from_le_bytes);
        let decoded = match prefix {
            Some(UNCOMPRESSED_BUFFER_PREFIX) => stored - COMPRESSED_BUFFER_PREFIX_BYTES as u64,
            Some(length) if length >= 0 => length as u64,
            _ => stored,
        };
        total.saturating_add(decoded)
    })
}

/// Walk the encapsulated Arrow IPC messages in `bytes`, reading only the
/// flatbuffer message metadata (never array bodies) to report each
/// message's kind, body length, and -- for RecordBatch messages -- the row
/// count declared in its metadata. Mirrors the framing that
/// `arrow_ipc::reader::StreamDecoder` itself expects (4-byte continuation
/// marker, 4-byte little-endian metadata length, metadata, body), so it
/// cross-checks the same bytes we go on to feed to that decoder.
///
/// A RecordBatch or DictionaryBatch whose metadata declares IPC body
/// compression with any codec but ZSTD is refused here. arrow's ZSTD path
/// refuses a buffer that does not decompress to exactly its declared length,
/// which is what makes `decoded_body_len` a bound; its LZ4_FRAME path takes
/// the declared length only as a capacity hint and reads the frame to its end.
fn scan_messages(bytes: &[u8]) -> Result<Vec<ScannedMessage>, BatchError> {
    let malformed = |detail: &str| BatchError::MalformedIpc(detail.to_string());
    let mut messages = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        if offset + 4 > bytes.len() {
            return Err(malformed("truncated message length prefix"));
        }
        let marker = [
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ];
        let len_offset = if marker == CONTINUATION_MARKER {
            offset + 4
        } else {
            offset
        };
        if len_offset + 4 > bytes.len() {
            return Err(malformed("truncated message length"));
        }
        let metadata_len = i32::from_le_bytes([
            bytes[len_offset],
            bytes[len_offset + 1],
            bytes[len_offset + 2],
            bytes[len_offset + 3],
        ]);
        let metadata_start = len_offset + 4;
        if metadata_len == 0 {
            // End-of-stream marker; nothing meaningful can follow it.
            break;
        }
        if metadata_len < 0 {
            return Err(malformed("negative metadata length"));
        }
        let metadata_end = match metadata_start.checked_add(metadata_len as usize) {
            Some(end) if end <= bytes.len() => end,
            _ => return Err(malformed("message metadata length out of bounds")),
        };
        let message = arrow_ipc::root_as_message(&bytes[metadata_start..metadata_end])
            .map_err(|e| BatchError::MalformedIpc(format!("invalid message metadata: {e}")))?;
        let body_len = message.bodyLength().max(0) as u64;
        let row_count = message.header_as_record_batch().map(|rb| rb.length());
        let body_end = match metadata_end.checked_add(body_len as usize) {
            Some(end) if end <= bytes.len() => end,
            _ => return Err(malformed("message body length out of bounds")),
        };
        let batch = message.header_as_record_batch().or_else(|| {
            message
                .header_as_dictionary_batch()
                .and_then(|dictionary| dictionary.data())
        });
        let decoded_body_len = match batch {
            Some(batch) => match batch.compression().map(|c| c.codec()) {
                Some(arrow_ipc::CompressionType::ZSTD) => {
                    compressed_body_decoded_len(batch, &bytes[metadata_end..body_end])
                }
                Some(codec) => {
                    return Err(BatchError::UnsupportedBodyCompression {
                        codec: format!("{codec:?}"),
                    });
                }
                None => body_len,
            },
            None => body_len,
        };
        messages.push(ScannedMessage {
            header: message.header_type(),
            decoded_body_len,
            row_count,
        });
        offset = body_end;
    }
    Ok(messages)
}

/// A `std::io::Write` adapter over `MutableBuffer`, so zstd can decompress
/// straight into arrow's 64-byte-aligned allocation instead of a plain
/// `Vec<u8>` that would later need copying into an aligned buffer.
struct AlignedWriter<'a>(&'a mut MutableBuffer);

impl Write for AlignedWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Decompress `record` with a hard cap enforced before any large allocation:
/// the decoder is bounded with `Read::take(cap + 1)` so a spoofed or absent
/// zstd content-size header can never force growth past `cap + 1` bytes,
/// regardless of how much data the frame actually claims or contains. The
/// destination is a `MutableBuffer` (64-byte aligned base allocation, see
/// `arrow_buffer::alloc::ALIGNMENT`), so a subsequent aligned IPC body
/// buffer can be a zero-copy view rather than requiring `StreamDecoder` to
/// realign it.
///
/// A payload whose frames all declare their content size is also stopped one
/// byte past the declared total. libzstd stops such a frame early only when
/// it sized its output buffer to the declared size; with a declared size
/// above its ring buffer it decodes the whole frame and only then compares.
/// The limit here keeps the declared size an honest bound for
/// [`zstd_job_bytes`] either way.
fn decompress_capped(record: &[u8], cap: u64) -> Result<MutableBuffer, BatchError> {
    let declared = declared_content_size(record).filter(|&declared| declared < cap);
    let limit = declared.unwrap_or(cap);
    let decoder =
        zstd::Decoder::new(record).map_err(|e| BatchError::Decompression(e.to_string()))?;
    let mut limited = decoder.take(limit + 1);
    let mut out = MutableBuffer::new(0);
    io::copy(&mut limited, &mut AlignedWriter(&mut out))
        .map_err(|e| BatchError::Decompression(e.to_string()))?;
    if out.len() as u64 > limit {
        return Err(match declared {
            Some(declared) => BatchError::Decompression(format!(
                "frame output exceeds its declared content size of {declared} bytes"
            )),
            None => BatchError::DecompressedPayloadTooLarge { limit: cap },
        });
    }
    Ok(out)
}

/// The first four bytes of a zstd frame, `ZSTD_MAGICNUMBER` little-endian.
const ZSTD_FRAME_MAGIC: [u8; 4] = [0x28, 0xB5, 0x2F, 0xFD];

/// Whether `record` is a zstd frame, by its first four bytes alone. Any other
/// payload is Arrow IPC with no outer frame, which is what the collector's
/// `otelarrow` exporter sends: its `payload_compression` is IPC body
/// compression, inside the stream, and arrow's reader undoes it.
fn is_zstd_frame(record: &[u8]) -> bool {
    record.starts_with(&ZSTD_FRAME_MAGIC)
}

/// The Arrow IPC bytes `record` carries, at most `cap` of them, in the same
/// aligned buffer either way: a zstd frame goes through
/// [`decompress_capped`], and any other payload is copied as-is.
fn payload_capped(record: &[u8], cap: u64) -> Result<MutableBuffer, BatchError> {
    if is_zstd_frame(record) {
        return decompress_capped(record, cap);
    }
    if record.len() as u64 > cap {
        return Err(BatchError::DecompressedPayloadTooLarge { limit: cap });
    }
    let mut out = MutableBuffer::new(record.len());
    out.extend_from_slice(record);
    Ok(out)
}

/// The decompressed length `record`'s frame headers declare: the sum of every
/// frame's content size, or `None` when a frame declares none, a header does
/// not parse, or `record` is empty.
fn declared_content_size(record: &[u8]) -> Option<u64> {
    if record.is_empty() {
        return None;
    }
    let mut rest = record;
    let mut total: u64 = 0;
    while !rest.is_empty() {
        let frame_len = zstd::zstd_safe::find_frame_compressed_size(rest).ok()?;
        let frame = rest.get(..frame_len).filter(|frame| !frame.is_empty())?;
        let size = zstd::zstd_safe::get_frame_content_size(frame).ok()??;
        total = total.checked_add(size)?;
        rest = &rest[frame_len..];
    }
    Some(total)
}

/// zstd's largest block, `ZSTD_BLOCKSIZE_MAX`.
const ZSTD_BLOCK_MAX_BYTES: u64 = 128 * 1024;

/// zstd's maximum expansion: the densest unit is an RLE block, a 3-byte block
/// header and the one byte it repeats, which decodes to up to a whole block.
const ZSTD_MAX_EXPANSION: u64 = ZSTD_BLOCK_MAX_BYTES / 4;

/// The size the write gate compares against its inline floor for the
/// decompression of `record` (ADR-1702 decision 4): an upper bound, to within
/// the one byte past its limit that it reads to detect an overrun, on the
/// bytes [`decompress_capped`] produces, at most `cap`. That is the declared content
/// size when every frame declares one, since [`decompress_capped`] stops past
/// it, and otherwise the compressed length times zstd's maximum expansion.
fn zstd_job_bytes(record: &[u8], cap: u64) -> u64 {
    declared_content_size(record)
        .unwrap_or_else(|| (record.len() as u64).saturating_mul(ZSTD_MAX_EXPANSION))
        .min(cap)
}

/// The write gate's size for [`payload_capped`] on `record`: [`zstd_job_bytes`]
/// for a zstd frame, and otherwise the payload's own length, capped, since
/// the job only copies it.
fn payload_job_bytes(record: &[u8], cap: u64) -> u64 {
    if is_zstd_frame(record) {
        zstd_job_bytes(record, cap)
    } else {
        (record.len() as u64).min(cap)
    }
}

fn payload_type_of(payload: &ArrowPayload) -> Result<ArrowPayloadType, BatchError> {
    ArrowPayloadType::try_from(payload.r#type)
        .ok()
        .filter(|t| *t != ArrowPayloadType::Unknown)
        .ok_or(BatchError::UnknownPayloadType {
            raw: payload.r#type,
        })
}

/// Byte range `[start, end)` of one frame's aligned decompress buffer, used
/// to tell whether a decoded array's buffer is a zero-copy view into it or
/// a fresh allocation `StreamDecoder` made to realign misaligned data.
type FrameRange = (usize, usize);

fn frame_range_of(buffer: &ArrowBuffer) -> FrameRange {
    let start = buffer.as_ptr() as usize;
    (start, start + buffer.len())
}

fn buffer_in_frame(range: FrameRange, buffer: &ArrowBuffer) -> bool {
    if buffer.is_empty() {
        return true;
    }
    let start = buffer.as_ptr() as usize;
    let end = start + buffer.len();
    start >= range.0 && end <= range.1
}

/// Whether every buffer backing `data` is a zero-copy view into `range`.
/// Dictionary values are excluded: they are cached on the decoder across
/// frames (only the key buffer is decoded fresh from this frame), so
/// checking them against this frame's range would misreport a dictionary
/// established in an earlier frame as a copy in this one.
fn array_data_in_frame(range: FrameRange, data: &ArrayData) -> bool {
    if !data.buffers().iter().all(|b| buffer_in_frame(range, b)) {
        return false;
    }
    if matches!(data.data_type(), DataType::Dictionary(_, _)) {
        return true;
    }
    data.child_data()
        .iter()
        .all(|child| array_data_in_frame(range, child))
}

/// Whether decoding `batch` out of this frame's buffer required no copies:
/// every column's array data is a zero-copy view into `range`.
fn batch_is_zero_copy(range: FrameRange, batch: &RecordBatch) -> bool {
    batch
        .columns()
        .iter()
        .all(|col| array_data_in_frame(range, &col.to_data()))
}

/// Running counts of decoded IPC record-batch frames, split by whether
/// `StreamDecoder` returned a zero-copy view into our aligned decompress
/// buffer or had to allocate and copy to realign a misaligned producer
/// buffer (this is producer-dependent and must be measured, not assumed).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DecodeStats {
    pub zero_copy_frames: u64,
    pub copy_fallback_frames: u64,
}

/// Extract a human-readable message from a caught panic payload, so a decoder
/// panic converted to a typed error still carries the arrow message that
/// triggered it (e.g. "the offset of the new Buffer cannot exceed the existing
/// length"). Falls back to a fixed string for a non-string payload.
fn panic_detail(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "arrow decoder panicked on malformed input".to_string()
    }
}

type DecoderKey = (ArrowPayloadType, String);

/// Per-gRPC-stream decode state: one `StreamDecoder` per (payload_type,
/// schema_id) IPC stream, plus the resource accounting needed to enforce
/// [`StreamConfig`]'s caps.
pub struct StreamState {
    config: StreamConfig,
    decoders: HashMap<DecoderKey, StreamDecoder>,
    dictionary_bytes_used: u64,
    poisoned: bool,
    stats: DecodeStats,
    write_gate: Option<Arc<WriteGate>>,
    /// Makes the next gated decompression fail with this error without
    /// running, so a test can reach the gate-failure arm.
    #[cfg(test)]
    fail_next_gate: Option<CpuGateError>,
}

impl StreamState {
    pub fn new(config: StreamConfig) -> Self {
        Self {
            config,
            decoders: HashMap::new(),
            dictionary_bytes_used: 0,
            poisoned: false,
            stats: DecodeStats::default(),
            write_gate: None,
            #[cfg(test)]
            fail_next_gate: None,
        }
    }

    /// Runs each payload's zstd decompression, or its copy when it carries no
    /// zstd frame, in [`decode_gated`] on the ADR-1702 write gate under the
    /// `otap_decode` site, sized by an upper bound on its output length so the
    /// gate's inline floor applies. IPC body decompression stays on the
    /// calling task, in the IPC decode.
    /// `None` decompresses inline, as [`decode`] always does.
    ///
    /// [`decode`]: Self::decode
    /// [`decode_gated`]: Self::decode_gated
    #[must_use]
    pub fn with_write_gate(mut self, gate: Option<Arc<WriteGate>>) -> Self {
        self.write_gate = gate;
        self
    }

    /// True once a [`StreamError`] has torn this stream down; every future
    /// [`decode`](Self::decode) call will return [`StreamError::Poisoned`].
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// Cumulative zero-copy vs copy-fallback frame counts observed across
    /// every [`decode`](Self::decode) call on this stream so far.
    pub fn decode_stats(&self) -> DecodeStats {
        self.stats
    }

    /// Decode one `BatchArrowRecords` message into the `RecordBatch`es
    /// carried by its payloads.
    ///
    /// On [`BatchError`], nothing in `self` that matters for future calls
    /// has been left in a bad state: nack `batch_id` and keep using this
    /// `StreamState`. On [`StreamError`], `self` is poisoned (every future
    /// call returns [`StreamError::Poisoned`]) and the caller must tear
    /// down the underlying gRPC stream.
    pub fn decode(&mut self, batch: BatchArrowRecords) -> Result<DecodedBatch, DecodeError> {
        if self.poisoned {
            return Err(StreamError::Poisoned.into());
        }
        let mut payloads = Vec::with_capacity(batch.arrow_payloads.len());
        for payload in batch.arrow_payloads {
            let decoded = payload_type_of(&payload).and_then(|payload_type| {
                let raw =
                    payload_capped(&payload.record, self.config.max_decompressed_payload_bytes)?;
                Ok((payload_type, raw))
            });
            let decoded = match decoded {
                Ok((payload_type, raw)) => self.decode_raw(payload_type, payload.schema_id, raw),
                Err(e) => Err(e.into()),
            };
            self.collect_payload(decoded, &mut payloads)?;
        }
        Ok(DecodedBatch {
            batch_id: batch.batch_id,
            payloads,
        })
    }

    /// [`decode`](Self::decode), with each payload's zstd decompression (or
    /// copy) on the write gate set by [`with_write_gate`](Self::with_write_gate). The
    /// decompression job owns the payload's compressed bytes; nothing else in
    /// the decode leaves the calling task, so the stateful IPC decoders are
    /// fed in payload order as before. A gate failure is a [`StreamError::Gate`]
    /// and poisons the stream.
    pub async fn decode_gated(
        &mut self,
        batch: BatchArrowRecords,
    ) -> Result<DecodedBatch, DecodeError> {
        if self.poisoned {
            return Err(StreamError::Poisoned.into());
        }
        let mut payloads = Vec::with_capacity(batch.arrow_payloads.len());
        for payload in batch.arrow_payloads {
            let decoded = match payload_type_of(&payload) {
                Ok(payload_type) => match self.decompress_on_gate(payload.record).await {
                    Ok(raw) => self.decode_raw(payload_type, payload.schema_id, raw),
                    Err(e) => Err(e),
                },
                Err(e) => Err(e.into()),
            };
            self.collect_payload(decoded, &mut payloads)?;
        }
        Ok(DecodedBatch {
            batch_id: batch.batch_id,
            payloads,
        })
    }

    async fn decompress_on_gate(
        &mut self,
        record: bytes::Bytes,
    ) -> Result<MutableBuffer, DecodeError> {
        let cap = self.config.max_decompressed_payload_bytes;
        match &self.write_gate {
            Some(gate) => {
                #[cfg(test)]
                if let Some(err) = self.fail_next_gate.take() {
                    return Err(StreamError::Gate(err).into());
                }
                let size = JobSize::Bytes(payload_job_bytes(&record, cap));
                let decompressed = gate
                    .run(WriteSite::OtapDecode, size, move || {
                        payload_capped(&record, cap)
                    })
                    .await
                    .map_err(StreamError::Gate)?;
                Ok(decompressed?)
            }
            None => Ok(payload_capped(&record, cap)?),
        }
    }

    /// Appends one payload's batches, or poisons the stream on a
    /// [`StreamError`] and returns the error that ends this batch.
    fn collect_payload(
        &mut self,
        decoded: Result<Vec<(ArrowPayloadType, RecordBatch)>, DecodeError>,
        payloads: &mut Vec<(ArrowPayloadType, RecordBatch)>,
    ) -> Result<(), DecodeError> {
        match decoded {
            Ok(mut batches) => {
                payloads.append(&mut batches);
                Ok(())
            }
            Err(DecodeError::Stream(e)) => {
                self.poisoned = true;
                Err(e.into())
            }
            Err(e @ DecodeError::Batch(_)) => Err(e),
        }
    }

    fn decode_raw(
        &mut self,
        payload_type: ArrowPayloadType,
        schema_id: String,
        raw: MutableBuffer,
    ) -> Result<Vec<(ArrowPayloadType, RecordBatch)>, DecodeError> {
        let scanned = scan_messages(&raw)?;

        let cap = self.config.max_decompressed_payload_bytes;
        let decoded_bytes = scanned
            .iter()
            .fold(0u64, |total, m| total.saturating_add(m.decoded_body_len));
        if decoded_bytes > cap {
            return Err(BatchError::DecompressedPayloadTooLarge { limit: cap }.into());
        }

        for message in &scanned {
            if let Some(rows) = message.row_count
                && rows > self.config.max_rows_per_batch
            {
                return Err(BatchError::RowCountExceeded {
                    limit: self.config.max_rows_per_batch,
                    actual: rows,
                }
                .into());
            }
        }

        let dictionary_bytes: u64 = scanned
            .iter()
            .filter(|m| m.header == MessageHeader::DictionaryBatch)
            .map(|m| m.decoded_body_len)
            .sum();
        if self.dictionary_bytes_used.saturating_add(dictionary_bytes)
            > self.config.max_stream_dictionary_bytes
        {
            return Err(BatchError::DictionaryBudgetExceeded {
                limit: self.config.max_stream_dictionary_bytes,
            }
            .into());
        }

        let key: DecoderKey = (payload_type, schema_id.clone());
        let is_new = !self.decoders.contains_key(&key);
        if is_new && self.decoders.len() >= self.config.max_schemas_per_stream {
            return Err(BatchError::SchemaBudgetExceeded {
                limit: self.config.max_schemas_per_stream,
            }
            .into());
        }

        self.dictionary_bytes_used += dictionary_bytes;

        let entry = self.decoders.entry(key.clone());
        let had_schema_before = matches!(&entry, Entry::Occupied(e) if e.get().schema().is_some());
        let decoder = entry.or_default();

        let mut buffer = ArrowBuffer::from(raw);
        let frame_range = frame_range_of(&buffer);
        let mut batches = Vec::new();
        while !buffer.is_empty() {
            // A hostile `BatchArrowRecords` can drive arrow's `StreamDecoder`
            // to panic inside `arrow-buffer` (e.g. "the offset of the new
            // Buffer cannot exceed the existing length") on some malformed
            // inputs, rather than returning `Err` (a known footgun class in
            // the arrow decoders; see tests/fuzz_mutation.rs). Catch the
            // unwind at this boundary so a single tenant's malformed batch is
            // converted to a typed error instead of unwinding through the
            // ingest task. A panicked decoder's internal buffering state is
            // untrustworthy, so it is dropped exactly as on a returned `Err`.
            let step = match panic::catch_unwind(AssertUnwindSafe(|| decoder.decode(&mut buffer))) {
                Ok(step) => step,
                Err(panic_payload) => {
                    self.decoders.remove(&key);
                    let detail = panic_detail(panic_payload.as_ref());
                    if had_schema_before {
                        return Err(StreamError::Corrupted {
                            payload_type,
                            schema_id,
                            detail,
                        }
                        .into());
                    }
                    return Err(BatchError::InternalPanic(detail).into());
                }
            };
            match step {
                Ok(Some(record_batch)) => {
                    if batch_is_zero_copy(frame_range, &record_batch) {
                        self.stats.zero_copy_frames += 1;
                    } else {
                        self.stats.copy_fallback_frames += 1;
                    }
                    batches.push((payload_type, record_batch));
                }
                Ok(None) => {}
                Err(e) => {
                    self.decoders.remove(&key);
                    if had_schema_before {
                        return Err(StreamError::Corrupted {
                            payload_type,
                            schema_id,
                            detail: e.to_string(),
                        }
                        .into());
                    }
                    return Err(BatchError::IpcDecode(e.to_string()).into());
                }
            }
        }
        Ok(batches)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_cpu_gate::{CpuGateConfig, DEFAULT_INLINE_FLOOR_BYTES, InstantClock};

    use super::*;
    use crate::encode::{DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder};

    const CAP: u64 = 16 * 1024 * 1024;
    const MIB: usize = 1024 * 1024;

    fn declaring(data: &[u8]) -> Vec<u8> {
        zstd::bulk::compress(data, 3).expect("compress")
    }

    fn undeclaring(data: &[u8]) -> Vec<u8> {
        let mut encoder = zstd::Encoder::new(Vec::new(), 3).expect("encoder");
        encoder.include_contentsize(false).expect("no content size");
        io::Write::write_all(&mut encoder, data).expect("write");
        encoder.finish().expect("finish")
    }

    /// A payload whose frames declare their sizes is sized by the declared
    /// total, so a well-compressing payload under the floor in compressed bytes
    /// is still sized above it by what it inflates to.
    #[test]
    fn a_declared_payload_is_sized_by_its_declared_content() {
        let one = declaring(&vec![0u8; MIB]);
        assert!(
            (one.len() as u64) < DEFAULT_INLINE_FLOOR_BYTES,
            "the fixture compresses below the floor"
        );
        assert_eq!(zstd_job_bytes(&one, CAP), MIB as u64);

        let two = [declaring(&[1u8; 1000]), declaring(&[2u8; 3000])].concat();
        assert_eq!(zstd_job_bytes(&two, CAP), 4000);

        let over_cap = declaring(&vec![0u8; 17 * MIB]);
        assert_eq!(zstd_job_bytes(&over_cap, CAP), CAP);
    }

    /// A payload whose frames declare a total above the cap is refused at the
    /// cap, not returned at its declared size: the declared total only bounds
    /// the read when it is below the cap. The undeclared bomb test does not
    /// reach this branch, because `zstd::stream::encode_all` pledges no
    /// content size; `zstd::bulk::compress` does.
    #[test]
    fn a_declared_payload_over_the_cap_is_rejected_at_the_cap() {
        let over_cap = declaring(&vec![0u8; 17 * MIB]);
        assert!(matches!(
            decompress_capped(&over_cap, CAP),
            Err(BatchError::DecompressedPayloadTooLarge { limit }) if limit == CAP
        ));
        let under_cap = declaring(&vec![0u8; MIB]);
        assert_eq!(
            decompress_capped(&under_cap, CAP)
                .expect("under the cap")
                .len(),
            MIB
        );
    }

    /// With any frame undeclared, the size is the compressed length times
    /// zstd's maximum expansion, capped.
    #[test]
    fn an_undeclared_payload_is_sized_by_the_expansion_bound() {
        let bare = undeclaring(&[3u8; 1000]);
        assert_eq!(
            zstd::zstd_safe::get_frame_content_size(&bare).expect("header"),
            None
        );
        assert_eq!(
            zstd_job_bytes(&bare, CAP),
            bare.len() as u64 * ZSTD_MAX_EXPANSION
        );

        let mixed = [declaring(&[1u8; 1000]), bare].concat();
        assert_eq!(declared_content_size(&mixed), None);
        assert_eq!(
            zstd_job_bytes(&mixed, CAP),
            (mixed.len() as u64 * ZSTD_MAX_EXPANSION).min(CAP)
        );
        assert_eq!(zstd_job_bytes(&vec![0xAB; 4096], CAP), CAP);
    }

    /// A frame whose header understates its content is stopped one byte past
    /// the declared size, not at the cap. The frame is built with a 128 KiB
    /// window so it is not single-segment, and its 4-byte content size is
    /// rewritten from 1 MiB to 512 KiB: above libzstd's ring buffer for that
    /// window, so libzstd alone would decode the whole MiB before it noticed.
    #[test]
    fn a_frame_that_outgrows_its_declared_size_stops_at_it() {
        const DECLARED: u32 = 512 * 1024;
        let mut compressor = zstd::bulk::Compressor::new(3).expect("compressor");
        compressor
            .set_parameter(zstd::stream::raw::CParameter::WindowLog(17))
            .expect("window log");
        let mut frame = compressor.compress(&vec![7u8; MIB]).expect("compress");

        let descriptor = frame[4];
        let single_segment = descriptor & 0x20 != 0;
        assert!(!single_segment, "the fixture carries a window descriptor");
        assert_eq!(descriptor >> 6, 2, "a 4-byte content size field");
        assert_eq!(descriptor & 0x03, 0, "no dictionary id");
        frame[6..10].copy_from_slice(&DECLARED.to_le_bytes());
        assert_eq!(declared_content_size(&frame), Some(u64::from(DECLARED)));

        match decompress_capped(&frame, CAP) {
            Err(BatchError::Decompression(message)) => assert!(
                message.contains(&format!("declared content size of {DECLARED}")),
                "unexpected message: {message}"
            ),
            other => panic!("expected the declared-size stop, got {other:?}"),
        }
    }

    /// One-payload batch of `record`, typed as a root metrics payload; the
    /// stream layer does not interpret columns, so any schema decodes.
    fn one_payload(record: Vec<u8>) -> BatchArrowRecords {
        BatchArrowRecords {
            batch_id: 1,
            arrow_payloads: vec![ArrowPayload {
                schema_id: "s".to_string(),
                r#type: ArrowPayloadType::UnivariateMetrics as i32,
                record: record.into(),
            }],
            headers: Vec::new(),
        }
    }

    fn decode_with(
        config: StreamConfig,
        batch: BatchArrowRecords,
    ) -> Result<DecodedBatch, DecodeError> {
        StreamState::new(config).decode(batch)
    }

    /// A schema and one record batch of `rows` rows, each a zero and `text`,
    /// in one Arrow IPC stream fragment with no end-of-stream marker, as one
    /// payload carries.
    fn ipc_fragment(
        rows: usize,
        text: &str,
        compression: Option<arrow_ipc::CompressionType>,
    ) -> (Vec<u8>, RecordBatch) {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{Field, Schema};
        use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};

        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![0i64; rows])),
                Arc::new(StringArray::from(vec![text; rows])),
            ],
        )
        .expect("batch");
        let options = IpcWriteOptions::default()
            .try_with_compression(compression)
            .expect("options");
        let mut writer =
            StreamWriter::try_new_with_options(Vec::new(), &schema, options).expect("writer");
        writer.write(&batch).expect("write");
        (writer.get_ref().clone(), batch)
    }

    fn only_batch(decoded: DecodedBatch) -> RecordBatch {
        let mut payloads = decoded.payloads;
        assert_eq!(payloads.len(), 1, "one record batch");
        payloads.remove(0).1
    }

    /// Arrow IPC with no zstd frame around it, the encoder's own bytes before
    /// its zstd step, decodes to the same records as the framed payload.
    #[test]
    fn a_plain_ipc_payload_decodes_like_its_zstd_framed_form() {
        let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
        let framed = encoder
            .encode_batch(
                1,
                &[
                    metric("cpu.load", &[(1_000, 0.5), (2_000, 0.75)]),
                    metric("mem.used", &[(1_000, 42.0)]),
                ],
            )
            .expect("encode batch");
        let mut plain = framed.clone();
        for payload in &mut plain.arrow_payloads {
            assert!(is_zstd_frame(&payload.record), "the encoder frames");
            let raw = zstd::stream::decode_all(&payload.record[..]).expect("unframe");
            assert_eq!(raw[..4], CONTINUATION_MARKER, "plain Arrow IPC");
            payload.record = raw.into();
        }

        let from_framed = decode_with(StreamConfig::default(), framed).expect("framed");
        let from_plain = decode_with(StreamConfig::default(), plain).expect("plain");
        assert!(!from_framed.payloads.is_empty());
        assert_eq!(from_plain.payloads, from_framed.payloads);
    }

    /// The collector's `otelarrow` exporter's default: Arrow IPC whose buffers
    /// are zstd-compressed inside the stream, with no outer frame.
    #[test]
    fn an_ipc_body_compressed_payload_decodes_without_an_outer_frame() {
        let (compressed, batch) =
            ipc_fragment(1000, "otelarrow", Some(arrow_ipc::CompressionType::ZSTD));
        let (uncompressed, _) = ipc_fragment(1000, "otelarrow", None);
        assert!(!is_zstd_frame(&compressed));
        assert_eq!(compressed[..4], CONTINUATION_MARKER);
        assert!(
            compressed.len() < uncompressed.len(),
            "the body is compressed"
        );

        let from_compressed = only_batch(
            decode_with(StreamConfig::default(), one_payload(compressed)).expect("ipc zstd"),
        );
        let from_framed = only_batch(
            decode_with(
                StreamConfig::default(),
                one_payload(declaring(&uncompressed)),
            )
            .expect("framed"),
        );
        assert_eq!(from_compressed, batch);
        assert_eq!(from_framed, batch);
    }

    /// A payload with no zstd frame is held to the same cap as a decompressed
    /// one, by its own length.
    #[test]
    fn a_plain_payload_over_the_cap_is_rejected_at_the_cap() {
        let (plain, _) = ipc_fragment(100, "otelarrow", None);
        let cap = plain.len() as u64 - 1;
        assert!(matches!(
            payload_capped(&plain, cap),
            Err(BatchError::DecompressedPayloadTooLarge { limit }) if limit == cap
        ));
        let config = StreamConfig {
            max_decompressed_payload_bytes: cap,
            ..StreamConfig::default()
        };
        assert!(matches!(
            decode_with(config, one_payload(plain.clone())),
            Err(DecodeError::Batch(BatchError::DecompressedPayloadTooLarge { limit })) if limit == cap
        ));
        let at_cap = StreamConfig {
            max_decompressed_payload_bytes: plain.len() as u64,
            ..StreamConfig::default()
        };
        decode_with(at_cap, one_payload(plain)).expect("a payload at the cap decodes");
    }

    /// IPC body compression is bounded by the same cap: a payload well under
    /// it whose compressed buffers declare more than it is refused before
    /// arrow's reader allocates them.
    #[test]
    fn ipc_body_compression_past_the_cap_is_rejected_at_the_cap() {
        const SMALL_CAP: u64 = 64 * 1024;
        let (compressed, batch) = ipc_fragment(100_000, "", Some(arrow_ipc::CompressionType::ZSTD));
        assert!(
            (compressed.len() as u64) < SMALL_CAP,
            "the payload is under the cap, {} bytes",
            compressed.len()
        );
        assert!(
            batch.get_array_memory_size() as u64 > 10 * SMALL_CAP,
            "its buffers decode far past the cap"
        );
        let config = StreamConfig {
            max_decompressed_payload_bytes: SMALL_CAP,
            ..StreamConfig::default()
        };
        assert!(matches!(
            decode_with(config, one_payload(compressed.clone())),
            Err(DecodeError::Batch(BatchError::DecompressedPayloadTooLarge { limit })) if limit == SMALL_CAP
        ));
        let decoded = only_batch(
            decode_with(StreamConfig::default(), one_payload(compressed))
                .expect("under the default cap"),
        );
        assert_eq!(decoded, batch);
    }

    /// IPC body compression with any codec but ZSTD is refused before a decoder
    /// is created: arrow's LZ4_FRAME reader takes a buffer's declared length
    /// only as a capacity hint, so the declared lengths would bound nothing.
    #[test]
    fn an_lz4_body_compressed_payload_is_refused_before_decode() {
        let (lz4, _) = ipc_fragment(
            1000,
            "otelarrow",
            Some(arrow_ipc::CompressionType::LZ4_FRAME),
        );
        assert!(!is_zstd_frame(&lz4));
        assert_eq!(lz4[..4], CONTINUATION_MARKER);

        let mut state = StreamState::new(StreamConfig::default());
        match state.decode(one_payload(lz4)) {
            Err(DecodeError::Batch(BatchError::UnsupportedBodyCompression { codec })) => {
                assert_eq!(codec, "LZ4_FRAME");
            }
            other => panic!("expected the LZ4_FRAME refusal, got {other:?}"),
        }
        assert!(state.decoders.is_empty(), "no decoder ran");
        assert!(!state.is_poisoned());
    }

    /// The stream dictionary budget is charged with a dictionary's decoded
    /// bytes, the memory its decoder holds, not its ZSTD-compressed size: a
    /// budget the whole compressed payload fits under still refuses it.
    #[test]
    fn dictionary_budget_counts_decoded_bytes_of_a_compressed_dictionary() {
        use arrow::array::{DictionaryArray, Int32Array, StringArray};
        use arrow::datatypes::{Field, Int32Type, Schema};
        use arrow_ipc::writer::{IpcWriteOptions, StreamWriter};

        let values: Vec<String> = (0..64)
            .map(|i| format!("{i:04}{}", "x".repeat(1024)))
            .collect();
        let value_bytes: u64 = values.iter().map(|v| v.len() as u64).sum();
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from_iter_values(0..64),
            Arc::new(StringArray::from(values)),
        )
        .expect("dictionary");
        let schema = Arc::new(Schema::new(vec![Field::new(
            "d",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(dictionary)]).expect("batch");
        let options = IpcWriteOptions::default()
            .try_with_compression(Some(arrow_ipc::CompressionType::ZSTD))
            .expect("options");
        let mut writer =
            StreamWriter::try_new_with_options(Vec::new(), &schema, options).expect("writer");
        writer.write(&batch).expect("write");
        let payload = writer.get_ref().clone();

        let scanned = scan_messages(&payload).expect("scan");
        let dictionaries: Vec<u64> = scanned
            .iter()
            .filter(|m| m.header == MessageHeader::DictionaryBatch)
            .map(|m| m.decoded_body_len)
            .collect();
        assert_eq!(dictionaries.len(), 1, "one dictionary message");
        let decoded = dictionaries[0];
        assert!(decoded >= value_bytes, "the values alone are {value_bytes}");
        assert!(
            (payload.len() as u64) < decoded - 1,
            "the whole payload, {} bytes, is under the budget that refuses it",
            payload.len()
        );

        let with_budget = |limit: u64| {
            StreamState::new(StreamConfig {
                max_stream_dictionary_bytes: limit,
                ..StreamConfig::default()
            })
            .decode(one_payload(payload.clone()))
        };
        assert_eq!(
            only_batch(with_budget(decoded).expect("a budget of the decoded bytes")),
            batch
        );
        match with_budget(decoded - 1) {
            Err(DecodeError::Batch(BatchError::DictionaryBudgetExceeded { limit })) => {
                assert_eq!(limit, decoded - 1);
            }
            other => panic!("expected DictionaryBudgetExceeded, got {other:?}"),
        }
    }

    /// The gate sizes a payload with no zstd frame by its own length, capped,
    /// and a zstd frame exactly as before.
    #[test]
    fn a_plain_payload_is_sized_by_its_length() {
        let (plain, _) = ipc_fragment(100, "otelarrow", None);
        assert_eq!(payload_job_bytes(&plain, CAP), plain.len() as u64);
        assert_eq!(payload_job_bytes(&plain, 10), 10);
        assert_eq!(payload_job_bytes(&vec![0xAB; 4096], CAP), 4096);

        let framed = declaring(&vec![0u8; MIB]);
        assert_eq!(
            payload_job_bytes(&framed, CAP),
            zstd_job_bytes(&framed, CAP)
        );
        let bare = undeclaring(&[3u8; 1000]);
        assert_eq!(payload_job_bytes(&bare, CAP), zstd_job_bytes(&bare, CAP));
    }

    fn metric(name: &str, points: &[(i64, f64)]) -> MetricRow {
        MetricRow {
            name: name.to_string(),
            kind: MetricKind::Gauge,
            data_points: points
                .iter()
                .map(|&(time_unix_nano, value)| DataPointRow {
                    exemplars: vec![],
                    time_unix_nano,
                    value,
                    flags: 0,
                    attrs: vec![],
                })
                .collect(),
        }
    }

    /// A gate failure on a batch after the first poisons the stream: the
    /// first batch already fed the decoders, so the state has advanced past
    /// what the failed batch's nack tells the client, and the next decode on
    /// this state is refused.
    #[tokio::test]
    async fn a_gate_failure_poisons_the_stream() {
        for injected in [CpuGateError::Panicked, CpuGateError::Cancelled] {
            let gate = Arc::new(WriteGate::new(
                CpuGateConfig {
                    inline_floor_bytes: 0,
                    ..CpuGateConfig::with_permits(1)
                },
                Arc::new(InstantClock::new()),
            ));
            let mut encoder = MetricsStreamEncoder::new("v1").expect("encoder");
            let mut batch = |id: i64| {
                encoder
                    .encode_batch(id, &[metric("cpu.load", &[(id * 1_000, 0.5)])])
                    .expect("encode batch")
            };
            let (first, second, third) = (batch(1), batch(2), batch(3));
            let mut state = StreamState::new(StreamConfig::default()).with_write_gate(Some(gate));
            state.decode_gated(first).await.expect("first batch");

            state.fail_next_gate = Some(injected);
            match state.decode_gated(second).await {
                Err(DecodeError::Stream(StreamError::Gate(err))) => assert_eq!(err, injected),
                other => panic!("expected a stream-ending gate error, got {other:?}"),
            }
            assert!(state.is_poisoned());
            assert!(matches!(
                state.decode_gated(third).await,
                Err(DecodeError::Stream(StreamError::Poisoned))
            ));
        }
    }
}
