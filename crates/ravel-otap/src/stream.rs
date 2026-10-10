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
    /// Cap on decompressed bytes for a single `ArrowPayload.record`, checked
    /// while decompressing (before the buffer is allowed to grow past it).
    pub max_decompressed_payload_bytes: u64,
    /// Cumulative cap, across the whole gRPC stream, on bytes attributed to
    /// `DictionaryBatch` IPC messages (initial dictionaries and deltas).
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
    #[error("zstd decompression did not complete on the CPU write gate: {0}")]
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
    body_len: u64,
    row_count: Option<i64>,
}

const CONTINUATION_MARKER: [u8; 4] = [0xff, 0xff, 0xff, 0xff];

/// Walk the encapsulated Arrow IPC messages in `bytes`, reading only the
/// flatbuffer message metadata (never array bodies) to report each
/// message's kind, body length, and -- for RecordBatch messages -- the row
/// count declared in its metadata. Mirrors the framing that
/// `arrow_ipc::reader::StreamDecoder` itself expects (4-byte continuation
/// marker, 4-byte little-endian metadata length, metadata, body), so it
/// cross-checks the same bytes we go on to feed to that decoder.
fn scan_messages(bytes: &[u8]) -> Result<Vec<ScannedMessage>, String> {
    let mut messages = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        if offset + 4 > bytes.len() {
            return Err("truncated message length prefix".to_string());
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
            return Err("truncated message length".to_string());
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
            return Err("negative metadata length".to_string());
        }
        let metadata_end = match metadata_start.checked_add(metadata_len as usize) {
            Some(end) if end <= bytes.len() => end,
            _ => return Err("message metadata length out of bounds".to_string()),
        };
        let message = arrow_ipc::root_as_message(&bytes[metadata_start..metadata_end])
            .map_err(|e| format!("invalid message metadata: {e}"))?;
        let body_len = message.bodyLength().max(0) as u64;
        let row_count = message.header_as_record_batch().map(|rb| rb.length());
        let body_end = match metadata_end.checked_add(body_len as usize) {
            Some(end) if end <= bytes.len() => end,
            _ => return Err("message body length out of bounds".to_string()),
        };
        messages.push(ScannedMessage {
            header: message.header_type(),
            body_len,
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

    /// Runs each payload's zstd decompression in [`decode_gated`] on the
    /// ADR-1702 write gate under the `otap_decode` site, sized by an upper
    /// bound on its decompressed length so the gate's inline floor applies.
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
                    decompress_capped(&payload.record, self.config.max_decompressed_payload_bytes)?;
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

    /// [`decode`](Self::decode), with each payload's zstd decompression on the
    /// write gate set by [`with_write_gate`](Self::with_write_gate). The
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
                let size = JobSize::Bytes(zstd_job_bytes(&record, cap));
                let decompressed = gate
                    .run(WriteSite::OtapDecode, size, move || {
                        decompress_capped(&record, cap)
                    })
                    .await
                    .map_err(StreamError::Gate)?;
                Ok(decompressed?)
            }
            None => Ok(decompress_capped(&record, cap)?),
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
        let scanned = scan_messages(&raw).map_err(BatchError::MalformedIpc)?;

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
            .map(|m| m.body_len)
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
