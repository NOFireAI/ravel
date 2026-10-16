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
    KeyIdxBucket, KeyIdxFieldInput, KeyIdxPrefix, encode_key_idx, key_idx_prefix_range, key8_id16,
    verify_key_idx,
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

/// What a KEY_IDX probe for one trace says about one object (ADR-2707
/// decision 5, tier 1). The three arms are opposite pruning decisions, so a
/// caller matches on all three.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TraceKeyProbe {
    /// No entry of the section has the trace's `key8`: no block of the object
    /// holds the trace, and the caller drops the object without a block read.
    Absent,
    /// The ascending, non-empty SKIP_IDX block ordinals holding every trace
    /// whose `key8` equals the probed one. Only these blocks can hold the
    /// trace; a prefix collision can add a block that does not.
    Present(Vec<u32>),
    /// The section does not index `trace_id` under `Id16`, so it says nothing
    /// and every block stays a candidate. RSPAN's parameter rules refuse such
    /// a section as `Corrupted` before the lookup, so a section that passed
    /// them never declines; the arm exists so a caller cannot read "declined"
    /// as "absent".
    Declined,
}

/// The first step of a probe over `[0, prefix_len)`: either the answer, or the
/// one bucket left to read.
#[derive(Debug, Clone)]
pub(crate) enum Located {
    Done(TraceKeyProbe),
    Bucket(KeyIdxBucket),
}

/// Parses the section's `[0, prefix_len)` bytes (`prefix_bytes`), applies the
/// RSPAN parameter rules, and locates the bucket `trace_id`'s `key8` mixes to.
/// An empty bucket is [`TraceKeyProbe::Absent`] with no second read.
pub(crate) fn locate(
    section_len: u64,
    prefix_len: u32,
    trace_id: &[u8; 16],
    prefix_bytes: &[u8],
) -> Result<Located, SpanSegError> {
    let prefix = KeyIdxPrefix::parse(prefix_bytes, prefix_len, section_len)?;
    check_parameters(&prefix)?;
    let Some(bucket) = prefix.locate(TRACE_ID_FIELD, KeyType::Id16, &key8_id16(trace_id))? else {
        return Ok(Located::Done(TraceKeyProbe::Declined));
    };
    if bucket.is_empty() {
        return Ok(Located::Done(TraceKeyProbe::Absent));
    }
    Ok(Located::Bucket(bucket))
}

/// The second step: verifies and decodes the bucket's bytes once and answers
/// for the probed key. Every entry of the bucket, whichever key it belongs to,
/// must name a block below `block_count`, so one bad ordinal fails the whole
/// bucket as `Corrupted` rather than trusting the rest of a frame that is
/// already known to be wrong.
pub(crate) fn finish(
    bucket: &KeyIdxBucket,
    frame: &[u8],
    block_count: usize,
) -> Result<TraceKeyProbe, SpanSegError> {
    let entries = bucket.entries(frame)?;
    if let Some(&(_, block)) = entries.iter().find(|(_, b)| *b as usize >= block_count) {
        return Err(SpanSegError::Corrupted(format!(
            "KEY_IDX bucket {} names block {block}, object has {block_count} blocks",
            bucket.bucket()
        )));
    }
    let blocks = bucket.matching(&entries);
    Ok(if blocks.is_empty() {
        TraceKeyProbe::Absent
    } else {
        TraceKeyProbe::Present(blocks)
    })
}

/// Probes for `trace_id` through `read`, which returns the section bytes for a
/// section-relative range. Reads exactly `[0, prefix_len)`, then the one bucket
/// the trace's `key8` mixes to (no second read when that bucket is empty).
pub(crate) fn probe<B, F>(
    section_len: u64,
    prefix_len: u32,
    block_count: usize,
    trace_id: &[u8; 16],
    mut read: F,
) -> Result<TraceKeyProbe, SpanSegError>
where
    B: AsRef<[u8]>,
    F: FnMut(Range<u64>) -> Result<B, SpanSegError>,
{
    let prefix_range = key_idx_prefix_range(prefix_len, section_len)?;
    let prefix_bytes = read(prefix_range)?;
    match locate(section_len, prefix_len, trace_id, prefix_bytes.as_ref())? {
        Located::Done(answer) => Ok(answer),
        Located::Bucket(bucket) => {
            let frame = read(bucket.range())?;
            finish(&bucket, frame.as_ref(), block_count)
        }
    }
}
