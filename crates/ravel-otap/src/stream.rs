//! OTAP gRPC stream state machine: per-stream Arrow IPC decode.
//!
//! Scope (Part 1 of the `ravel-otap` decoder, issue #12): this module turns
//! `BatchArrowRecords` messages into `RecordBatch`es. It does not interpret
//! column semantics (ids, parent_id joins, AnyValue unions, delta encodings)
//! at all -- that is the columnar normalizer's job (a later task). This is
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
use std::io::Read;

use arrow::buffer::Buffer as ArrowBuffer;
use arrow::record_batch::RecordBatch;
use arrow_ipc::MessageHeader;
use arrow_ipc::reader::StreamDecoder;
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

/// Decompress `record` with a hard cap enforced before any large allocation:
/// the decoder is bounded with `Read::take(cap + 1)` so a spoofed or absent
/// zstd content-size header can never force growth past `cap + 1` bytes,
/// regardless of how much data the frame actually claims or contains.
fn decompress_capped(record: &[u8], cap: u64) -> Result<Vec<u8>, BatchError> {
    let decoder =
        zstd::Decoder::new(record).map_err(|e| BatchError::Decompression(e.to_string()))?;
    let mut limited = decoder.take(cap + 1);
    let mut out = Vec::new();
    limited
        .read_to_end(&mut out)
        .map_err(|e| BatchError::Decompression(e.to_string()))?;
    if out.len() as u64 > cap {
        return Err(BatchError::DecompressedPayloadTooLarge { limit: cap });
    }
    Ok(out)
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
}

impl StreamState {
    pub fn new(config: StreamConfig) -> Self {
        Self {
            config,
            decoders: HashMap::new(),
            dictionary_bytes_used: 0,
            poisoned: false,
        }
    }

    /// True once a [`StreamError`] has torn this stream down; every future
    /// [`decode`](Self::decode) call will return [`StreamError::Poisoned`].
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
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
            match self.decode_payload(payload) {
                Ok(mut batches) => payloads.append(&mut batches),
                Err(DecodeError::Stream(e)) => {
                    self.poisoned = true;
                    return Err(e.into());
                }
                Err(e @ DecodeError::Batch(_)) => return Err(e),
            }
        }
        Ok(DecodedBatch {
            batch_id: batch.batch_id,
            payloads,
        })
    }

    fn decode_payload(
        &mut self,
        payload: ArrowPayload,
    ) -> Result<Vec<(ArrowPayloadType, RecordBatch)>, DecodeError> {
        let payload_type = ArrowPayloadType::try_from(payload.r#type)
            .ok()
            .filter(|t| *t != ArrowPayloadType::Unknown)
            .ok_or(BatchError::UnknownPayloadType {
                raw: payload.r#type,
            })?;

        let raw = decompress_capped(&payload.record, self.config.max_decompressed_payload_bytes)?;
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

        let key: DecoderKey = (payload_type, payload.schema_id.clone());
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
        let mut batches = Vec::new();
        while !buffer.is_empty() {
            match decoder.decode(&mut buffer) {
                Ok(Some(record_batch)) => batches.push((payload_type, record_batch)),
                Ok(None) => {}
                Err(e) => {
                    self.decoders.remove(&key);
                    if had_schema_before {
                        return Err(StreamError::Corrupted {
                            payload_type,
                            schema_id: payload.schema_id,
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
