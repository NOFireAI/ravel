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
    KEY_IDX_WRITER_BUCKET_BITS, Key8, KeyIdxBucket, KeyIdxFieldInput, KeyIdxPrefix, bucket_index,
    encode_key_idx_with_bucket_bits, key_idx_prefix_range, key8_id16, verify_key_idx,
};

use crate::error::SpanSegError;

pub use ravel_codec::key_idx::{
    EncodedKeyIdx, KEY_IDX_BUCKET_MAX_ENTRIES, KeyIdxFieldSummary, KeyIdxSummary, KeyType,
};

/// The one field an RSPAN KEY_IDX section indexes.
pub const TRACE_ID_FIELD: &str = "trace_id";

/// The `bucket_bits` an RSPAN writer frames its KEY_IDX section at. The
/// writer's overflow check and [`trace_key_idx_bucket`] both bucket with it,
/// so a caller sizing an object by bucket cannot disagree with the section
/// that gets written.
pub const TRACE_KEY_IDX_BUCKET_BITS: u8 = KEY_IDX_WRITER_BUCKET_BITS;

/// The number of buckets in an RSPAN KEY_IDX section.
pub const TRACE_KEY_IDX_BUCKETS: usize = 1 << TRACE_KEY_IDX_BUCKET_BITS;

/// The KEY_IDX bucket, in `0..TRACE_KEY_IDX_BUCKETS`, that `trace_id`'s
/// entries land in when an RSPAN writer frames the section.
pub fn trace_key_idx_bucket(trace_id: &[u8; 16]) -> Result<u32, SpanSegError> {
    Ok(bucket_index(
        &key8_id16(trace_id),
        TRACE_KEY_IDX_BUCKET_BITS,
    )?)
}

/// Encodes the KEY_IDX section for `(trace_id, block ordinal)` pairs, in any
/// order with duplicates allowed. A bucket holding more than
/// [`KEY_IDX_BUCKET_MAX_ENTRIES`] distinct `(key8, block)` entries is
/// [`SpanSegError::KeyIdxBucketOverflow`].
pub fn encode_trace_key_idx(
    pairs: impl IntoIterator<Item = ([u8; 16], u32)>,
) -> Result<EncodedKeyIdx, SpanSegError> {
    encode_trace_key_idx_capped(pairs, KEY_IDX_BUCKET_MAX_ENTRIES)
}

/// [`encode_trace_key_idx`] with the per-bucket cap as a parameter, so a test
/// can reach the overflow without 2^20 entries. The lowest overflowing bucket
/// is the one reported.
pub(crate) fn encode_trace_key_idx_capped(
    pairs: impl IntoIterator<Item = ([u8; 16], u32)>,
    cap: u32,
) -> Result<EncodedKeyIdx, SpanSegError> {
    let mut entries: Vec<(Key8, u32)> = pairs
        .into_iter()
        .map(|(trace_id, block)| (key8_id16(&trace_id), block))
        .collect();
    entries.sort_unstable();
    entries.dedup();
    let mut counts = vec![0u64; TRACE_KEY_IDX_BUCKETS];
    for (key8, _) in &entries {
        let bucket = bucket_index(key8, TRACE_KEY_IDX_BUCKET_BITS)?;
        let slot = counts.get_mut(bucket as usize).ok_or_else(|| {
            SpanSegError::Corrupted(format!(
                "KEY_IDX bucket {bucket} outside {TRACE_KEY_IDX_BUCKETS} buckets"
            ))
        })?;
        *slot += 1;
    }
    if let Some((bucket, &n)) = counts
        .iter()
        .enumerate()
        .find(|&(_, &n)| n > u64::from(cap))
    {
        return Err(SpanSegError::KeyIdxBucketOverflow {
            bucket: bucket as u32,
            entries: n,
            cap,
        });
    }
    Ok(encode_key_idx_with_bucket_bits(
        &[KeyIdxFieldInput {
            name: TRACE_ID_FIELD.to_string(),
            key_type: KeyType::Id16,
            entries,
        }],
        TRACE_KEY_IDX_BUCKET_BITS,
    )?)
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

/// A trace id whose `key8` lands in `bucket` at [`TRACE_KEY_IDX_BUCKET_BITS`],
/// distinct for distinct `j` below `2^(64 - bits)`. The bucket mix multiplies
/// by an odd constant, so it inverts mod 2^64 and any bucket can be filled on
/// purpose; this is the adversarial input of issue #2985.
#[cfg(test)]
pub(crate) fn trace_id_in_bucket(bucket: u32, j: u64) -> [u8; 16] {
    const MIX: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut inv = MIX;
    for _ in 0..5 {
        inv = inv.wrapping_mul(2u64.wrapping_sub(MIX.wrapping_mul(inv)));
    }
    let shift = 64 - u32::from(TRACE_KEY_IDX_BUCKET_BITS);
    let key8 = ((u64::from(bucket) << shift) | j).wrapping_mul(inv);
    let mut id = [0u8; 16];
    id[..8].copy_from_slice(&key8.to_be_bytes());
    id
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn trace_id_in_bucket_lands_in_its_bucket() {
        for bucket in [0u32, 17, 255] {
            for j in 0..64 {
                assert_eq!(
                    trace_key_idx_bucket(&trace_id_in_bucket(bucket, j)).expect("bucket"),
                    bucket
                );
            }
        }
    }

    /// At the real cap: a bucket of exactly `KEY_IDX_BUCKET_MAX_ENTRIES`
    /// distinct entries encodes and verifies, one entry more is the typed
    /// overflow naming the bucket and its count.
    ///
    /// FLIP: before #2985 the 2^20 + 1 case surfaced as the codec's
    /// `Corrupted("... over KEY_IDX_BUCKET_MAX_ENTRIES ...")` string, so the
    /// `KeyIdxBucketOverflow` match fails. Rules out a check with `>=` in
    /// place of `>` (the at-cap case is refused) and a count of pairs before
    /// deduplication (the duplicated pair below pushes the at-cap bucket over).
    #[test]
    fn the_trace_key_idx_cap_agrees_with_the_codec_at_the_real_cap() {
        let max = u64::from(KEY_IDX_BUCKET_MAX_ENTRIES);
        let at_cap = || (0..max).map(|j| (trace_id_in_bucket(9, j), 0u32));

        let encoded = encode_trace_key_idx(at_cap().chain([(trace_id_in_bucket(9, 0), 0)]))
            .expect("a bucket of exactly the cap encodes");
        let summary = verify_key_idx(&encoded.bytes, encoded.prefix_len).expect("verify");
        assert_eq!(summary.bucket_bits, TRACE_KEY_IDX_BUCKET_BITS);
        assert_eq!(summary.fields[0].bucket_entry_counts[9], KEY_IDX_BUCKET_MAX_ENTRIES);

        match encode_trace_key_idx(at_cap().chain([(trace_id_in_bucket(9, max), 0)])) {
            Err(SpanSegError::KeyIdxBucketOverflow {
                bucket,
                entries,
                cap,
            }) => {
                assert_eq!(bucket, 9);
                assert_eq!(entries, max + 1);
                assert_eq!(cap, KEY_IDX_BUCKET_MAX_ENTRIES);
            }
            other => panic!("expected KeyIdxBucketOverflow, got {other:?}"),
        }
    }

    /// The encoder counts distinct `(key8, block)` entries per bucket: one
    /// trace in two blocks is two entries, the same trace twice in one block
    /// is one, and the lowest overflowing bucket is reported.
    ///
    /// FLIP: counting keys instead of entries passes the first case (cap 2,
    /// one key in three blocks); counting rows refuses the second.
    #[test]
    fn the_capped_encoder_counts_distinct_entries_per_bucket() {
        let t = trace_id_in_bucket(3, 1);
        match encode_trace_key_idx_capped([(t, 0), (t, 1), (t, 2)], 2) {
            Err(SpanSegError::KeyIdxBucketOverflow {
                bucket: 3,
                entries: 3,
                cap: 2,
            }) => {}
            other => panic!("expected bucket 3 overflow, got {other:?}"),
        }
        encode_trace_key_idx_capped([(t, 0), (t, 0), (t, 0), (t, 1)], 2)
            .expect("two distinct entries fit a cap of two");

        let pairs = (0..3).flat_map(|j| {
            [
                (trace_id_in_bucket(200, j), 0),
                (trace_id_in_bucket(40, j), 0),
            ]
        });
        match encode_trace_key_idx_capped(pairs, 2) {
            Err(SpanSegError::KeyIdxBucketOverflow { bucket: 40, .. }) => {}
            other => panic!("expected the lowest bucket, 40, got {other:?}"),
        }
    }
}
