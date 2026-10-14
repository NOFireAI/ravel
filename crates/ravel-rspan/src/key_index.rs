//! The RSPAN KEY_IDX section (trailer v5, ADR-2707 decision 2): `ravel-codec`'s
//! `key_idx` grammar with RSPAN's fixed parameters, one field named
//! [`TRACE_ID_FIELD`] of key type `Id16`, written at bucket_bits 8. Each entry
//! maps a trace's `key8` (the first 8 bytes of its `trace_id`) to the ordinal
//! of a SKIP_IDX block holding it.
//!
//! A probe returns the blocks holding every trace whose `key8` equals the
//! probed one, so two traces sharing their first 8 bytes share their answer;
//! the caller's row filter on the full `trace_id` stays authoritative.

use std::ops::Range;

use ravel_codec::key_idx::{
    KEY_IDX_ENTRY_LEN, KeyIdxBucket, KeyIdxFieldInput, KeyIdxPrefix, encode_key_idx,
    key_idx_prefix_range, key8_id16, verify_key_idx,
};

use crate::error::SpanSegError;

pub use ravel_codec::key_idx::{EncodedKeyIdx, KeyIdxFieldSummary, KeyIdxSummary, KeyType};

/// The one field an RSPAN KEY_IDX section indexes.
pub const TRACE_ID_FIELD: &str = "trace_id";

/// Encodes the KEY_IDX section for `(trace_id, block ordinal)` pairs, in any
/// order with duplicates allowed.
pub fn encode_trace_key_idx(
    pairs: impl IntoIterator<Item = ([u8; 16], u32)>,
) -> Result<EncodedKeyIdx, SpanSegError> {
    let entries = pairs
        .into_iter()
        .map(|(trace_id, block)| (key8_id16(&trace_id), block))
        .collect();
    Ok(encode_key_idx(&[KeyIdxFieldInput {
        name: TRACE_ID_FIELD.to_string(),
        key_type: KeyType::Id16,
        entries,
    }])?)
}

/// RSPAN's parameter rules over a parsed prefix: exactly one field, named
/// [`TRACE_ID_FIELD`], of key type `Id16`. Anything else is `Corrupted`.
fn check_parameters(prefix: &KeyIdxPrefix<'_>) -> Result<(), SpanSegError> {
    match prefix.fields() {
        [field] if field.name() == TRACE_ID_FIELD && field.key_type() == KeyType::Id16 => Ok(()),
        [field] => Err(SpanSegError::Corrupted(format!(
            "KEY_IDX field is {:?} of key type {:?}, RSPAN requires {TRACE_ID_FIELD:?} of Id16",
            field.name(),
            field.key_type()
        ))),
        fields => Err(SpanSegError::Corrupted(format!(
            "KEY_IDX names {} fields, RSPAN requires exactly one",
            fields.len()
        ))),
    }
}

/// Whole-object open check on the section's stored bytes (crc already
/// verified by the caller): parses `[0, prefix_len)` and applies the RSPAN
/// parameter rules.
pub(crate) fn check_section(section: &[u8], prefix_len: u32) -> Result<(), SpanSegError> {
    let range = key_idx_prefix_range(prefix_len, section.len() as u64)?;
    let prefix_bytes = section
        .get(range.start as usize..range.end as usize)
        .ok_or_else(|| SpanSegError::Corrupted("KEY_IDX prefix past the section".into()))?;
    let prefix = KeyIdxPrefix::parse(prefix_bytes, prefix_len, section.len() as u64)?;
    check_parameters(&prefix)
}

/// Verifies every frame of a whole section and applies the RSPAN parameter
/// rules, for inspection.
pub(crate) fn summarize(section: &[u8], prefix_len: u32) -> Result<KeyIdxSummary, SpanSegError> {
    check_section(section, prefix_len)?;
    Ok(verify_key_idx(section, prefix_len)?)
}

/// Probes for `trace_id` through `read`, which returns the section bytes for a
/// section-relative range. Reads exactly `[0, prefix_len)`, then the one bucket
/// the trace's `key8` mixes to (no second read when that bucket is empty).
/// Every ordinal in the bucket read must be below `block_count`, whichever key
/// it belongs to. `Ok(None)` only when the section declines the probe, which
/// the parameter rules make unreachable for a section that passed them.
pub(crate) fn probe<B, F>(
    section_len: u64,
    prefix_len: u32,
    block_count: usize,
    trace_id: &[u8; 16],
    mut read: F,
) -> Result<Option<Vec<u32>>, SpanSegError>
where
    B: AsRef<[u8]>,
    F: FnMut(Range<u64>) -> Result<B, SpanSegError>,
{
    let prefix_range = key_idx_prefix_range(prefix_len, section_len)?;
    let prefix_bytes = read(prefix_range)?;
    let prefix = KeyIdxPrefix::parse(prefix_bytes.as_ref(), prefix_len, section_len)?;
    check_parameters(&prefix)?;
    let Some(bucket) = prefix.locate(TRACE_ID_FIELD, KeyType::Id16, &key8_id16(trace_id))? else {
        return Ok(None);
    };
    if bucket.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let bucket_bytes = read(bucket.range())?;
    blocks_in_range(&bucket, bucket_bytes.as_ref(), block_count).map(Some)
}

/// [`KeyIdxBucket::blocks`] plus the block-count check over every entry of
/// the frame, not only the probed key's.
fn blocks_in_range(
    bucket: &KeyIdxBucket,
    frame: &[u8],
    block_count: usize,
) -> Result<Vec<u32>, SpanSegError> {
    // Verifies the frame's crc, entry count, size, order and bucket membership,
    // so the decode below runs on a well-formed frame.
    let blocks = bucket.blocks(frame)?;
    let count = frame
        .get(..4)
        .map(|b| u32::from_le_bytes([b[0], b[1], b[2], b[3]]) as usize)
        .ok_or_else(|| SpanSegError::Corrupted("KEY_IDX bucket frame truncated".into()))?;
    let payload = frame
        .get(8..)
        .ok_or_else(|| SpanSegError::Corrupted("KEY_IDX bucket frame truncated".into()))?;
    let raw = zstd::bulk::decompress(payload, count * KEY_IDX_ENTRY_LEN)
        .map_err(|e| SpanSegError::Corrupted(format!("KEY_IDX bucket frame zstd: {e}")))?;
    for entry in raw.chunks_exact(KEY_IDX_ENTRY_LEN) {
        let block = u32::from_le_bytes([entry[8], entry[9], entry[10], entry[11]]);
        if block as usize >= block_count {
            return Err(SpanSegError::Corrupted(format!(
                "KEY_IDX bucket {} names block {block}, object has {block_count} blocks",
                bucket.bucket()
            )));
        }
    }
    Ok(blocks)
}
