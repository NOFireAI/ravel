//! The RLOG KEY_IDX section (kind 9, ADR-2707 decision 3): `ravel-codec`'s
//! `key_idx` grammar over the tenant's declared exact-key fields
//! (docs/log-segment-format.md, "KEY_IDX"). Each entry maps a field value's
//! `key8` to the ordinal of a SKIP_IDX level-0 block holding a row whose
//! merged attribute view carries that value under the field's declared type.
//!
//! The section is optional: a writer given no fields emits none, and an
//! object without one reads unchanged. A probe returns the blocks holding
//! every value whose `key8` equals the probed one, so two values sharing a
//! `key8` share their answer; the caller's row filter stays authoritative.
//!
//! This writer marks its sections [`RLOG_KEY_IDX_VERSION`]: every declared
//! value was resolved by the SQL reader's merged-view rule. A section an
//! earlier writer left at version 1 still reads and verifies, but its keys
//! may disagree with the reader, so [`section_resolves_like_the_reader`] is
//! false for it and a probe declines on it.

use std::collections::HashMap;
use std::ops::Range;

use ravel_codec::key_idx::{
    KEY_IDX_MAX_NAME_LEN, KEY_IDX_VERSION_2, KEY_IDX_WRITER_BUCKET_BITS, KeyIdxBucket,
    KeyIdxFieldInput, KeyIdxPrefix, bucket_index, encode_key_idx_with, key_idx_prefix_range,
    verify_key_idx,
};

use crate::error::LogSegError;
use crate::footer::{LogFooter, SectionDesc, kind};
use crate::record::{ColumnValue, FieldType};
use crate::source::ByteSource;

pub use ravel_codec::key_idx::{
    EncodedKeyIdx, KEY_IDX_BUCKET_MAX_ENTRIES, KEY_IDX_MAX_FIELDS, KEY_IDX_VERSION_1,
    KEY_IDX_VERSIONS, Key8, KeyIdxFieldSummary, KeyIdxSummary, KeyType, key8_bytes, key8_i64,
    key8_id16, key8_str,
};

/// The KEY_IDX version byte the RLOG writer stamps: the section grammar of
/// [`KEY_IDX_VERSION_1`], with every attribute field's values resolved as the
/// SQL reader resolves the declared column (docs/log-segment-format.md,
/// "KEY_IDX", "Entries and keys"). RSPAN keeps writing version 1.
pub const RLOG_KEY_IDX_VERSION: u8 = KEY_IDX_VERSION_2;

/// Whether an RLOG KEY_IDX section of header `version` was written by the
/// reader's resolution rule, so a probe miss on it proves no row's declared
/// column holds the value. False for a version 1 section: an earlier RLOG
/// writer let a top-level `Map` or `List` resource or scope entry claim a
/// field's name, so a scalar behind it was never keyed, and under a `Bytes`
/// field it keyed that nested entry's canonical bytes, a value the reader
/// drops. [`probe_ranged`] and [`probe_source`] decline on such a section.
pub fn section_resolves_like_the_reader(version: u8) -> bool {
    version == RLOG_KEY_IDX_VERSION
}

/// The fixed `trace_id` column's field name, the only field of key type
/// [`KeyType::Id16`].
pub const TRACE_ID_FIELD: &str = "trace_id";

/// The [`FieldType`] byte an attribute field of `key_type` is stored under,
/// `None` for [`KeyType::Id16`], which only the fixed `trace_id` column takes.
fn attr_field_type(key_type: KeyType) -> Option<FieldType> {
    match key_type {
        KeyType::Id16 => None,
        KeyType::I64 => Some(FieldType::I64),
        KeyType::Str => Some(FieldType::Str),
        KeyType::Bytes => Some(FieldType::Bytes),
    }
}

/// `key8` of a merged-view value, for a value already known to be of the
/// field's declared type.
pub(crate) fn key8_of(value: &ColumnValue) -> Option<Key8> {
    match value {
        ColumnValue::I64(v) => Some(key8_i64(*v)),
        ColumnValue::Str(b) | ColumnValue::Bytes(b) => Some(key8_bytes(b)),
        ColumnValue::F64(_) | ColumnValue::Bool(_) => None,
    }
}

/// The validated field set one object's KEY_IDX is built for, and the
/// `(key8, block)` pairs accumulated per field while the blocks are written.
pub(crate) struct KeyIndexPlan {
    /// Ascending by name bytes, unique: the order the section names them in.
    fields: Vec<(String, KeyType)>,
    /// Attribute field name to `(field index, FieldType byte)`.
    attrs: HashMap<String, (u32, u8)>,
    /// Field index of [`TRACE_ID_FIELD`], when declared.
    trace: Option<u32>,
    /// Per field, every `(key8, block)` pair sealed so far.
    entries: Vec<Vec<(Key8, u32)>>,
    /// The current block's `(field index, key8)` pairs, duplicates allowed.
    block: Vec<(u32, Key8)>,
}

impl KeyIndexPlan {
    /// Validates `fields` and returns `None` when it is empty (no section is
    /// written). Refused with [`LogSegError::LimitExceeded`]: more than 64
    /// fields, a name empty or over 128 bytes, a repeated name,
    /// [`TRACE_ID_FIELD`] under any key type but `Id16`, and `Id16` under any
    /// other name.
    pub(crate) fn new(fields: &[(String, KeyType)]) -> Result<Option<Self>, LogSegError> {
        if fields.is_empty() {
            return Ok(None);
        }
        if fields.len() > KEY_IDX_MAX_FIELDS {
            return Err(LogSegError::LimitExceeded(format!(
                "{} key index fields, at most {KEY_IDX_MAX_FIELDS}",
                fields.len()
            )));
        }
        let mut sorted = fields.to_vec();
        sorted.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        for pair in sorted.windows(2) {
            if pair[0].0 == pair[1].0 {
                return Err(LogSegError::LimitExceeded(format!(
                    "key index field {:?} named twice",
                    pair[0].0
                )));
            }
        }
        let mut attrs = HashMap::new();
        let mut trace = None;
        for (idx, (name, key_type)) in sorted.iter().enumerate() {
            if name.is_empty() || name.len() > KEY_IDX_MAX_NAME_LEN {
                return Err(LogSegError::LimitExceeded(format!(
                    "key index field name of {} bytes, outside 1..={KEY_IDX_MAX_NAME_LEN}",
                    name.len()
                )));
            }
            let idx = idx as u32;
            match (name.as_str() == TRACE_ID_FIELD, attr_field_type(*key_type)) {
                (true, None) => trace = Some(idx),
                (false, Some(ty)) => {
                    attrs.insert(name.clone(), (idx, ty.to_u8()));
                }
                (true, Some(_)) | (false, None) => {
                    return Err(LogSegError::LimitExceeded(format!(
                        "key index field {name:?} of key type {key_type:?}: \
                         {TRACE_ID_FIELD:?} is indexed as Id16 and only it is"
                    )));
                }
            }
        }
        let entries = vec![Vec::new(); sorted.len()];
        Ok(Some(Self {
            fields: sorted,
            attrs,
            trace,
            entries,
            block: Vec::new(),
        }))
    }

    /// `(field index, FieldType byte)` of an attribute field.
    pub(crate) fn attr_field(&self, name: &str) -> Option<(u32, u8)> {
        self.attrs.get(name).copied()
    }

    /// Adds one row's `(field index, key8)` pair to the current block.
    pub(crate) fn push(&mut self, field: u32, key8: Key8) {
        self.block.push((field, key8));
    }

    /// Adds one row's `trace_id` to the current block when it is indexed.
    pub(crate) fn push_trace(&mut self, trace_id: Option<&[u8]>) {
        let (Some(field), Some(id)) = (self.trace, trace_id) else {
            return;
        };
        if let Ok(id) = <&[u8; 16]>::try_from(id) {
            self.block.push((field, key8_id16(id)));
        }
    }

    /// Closes the current block as level-0 ordinal `block`: one entry per
    /// distinct `(field, key8)` pair it collected.
    pub(crate) fn seal_block(&mut self, block: u32) {
        self.block.sort_unstable();
        self.block.dedup();
        for &(field, key8) in &self.block {
            if let Some(entries) = self.entries.get_mut(field as usize) {
                entries.push((key8, block));
            }
        }
        self.block.clear();
    }

    /// Encodes the section. Every declared field is named, with
    /// `entry_count = 0` when no row matched it, except a field with a bucket
    /// over `bucket_cap` entries: the codec refuses such a bucket, so the field
    /// is left out of this object's header (a probe on it declines) and a
    /// warning names it. `None` when every field was left out, so no section
    /// is written.
    pub(crate) fn encode(self, bucket_cap: u32) -> Result<Option<EncodedKeyIdx>, LogSegError> {
        let mut inputs = Vec::with_capacity(self.fields.len());
        for ((name, key_type), entries) in self.fields.into_iter().zip(self.entries) {
            if let Some((bucket, count)) = overfull_bucket(&entries, bucket_cap)? {
                tracing::warn!(
                    field = %name,
                    key_type = ?key_type,
                    bucket,
                    entries = count,
                    bucket_cap,
                    "KEY_IDX field left out of this object: one bucket holds more entries \
                     than the cap, so the object reads as unindexed for the field"
                );
                continue;
            }
            inputs.push(KeyIdxFieldInput {
                name,
                key_type,
                entries,
            });
        }
        if inputs.is_empty() {
            return Ok(None);
        }
        Ok(Some(encode_key_idx_with(
            &inputs,
            RLOG_KEY_IDX_VERSION,
            KEY_IDX_WRITER_BUCKET_BITS,
        )?))
    }
}

/// The first bucket, at the writer's bucket bits, holding more than `cap` of
/// `entries`, with its entry count. `entries` is already unique: a block is
/// sealed once and deduplicates its own pairs, so the count is the one the
/// codec checks.
fn overfull_bucket(entries: &[(Key8, u32)], cap: u32) -> Result<Option<(u32, u64)>, LogSegError> {
    if entries.len() as u64 <= u64::from(cap) {
        return Ok(None);
    }
    let mut counts = vec![0u64; 1usize << KEY_IDX_WRITER_BUCKET_BITS];
    for (key8, _) in entries {
        let bucket = bucket_index(key8, KEY_IDX_WRITER_BUCKET_BITS)?;
        if let Some(count) = counts.get_mut(bucket as usize) {
            *count += 1;
        }
    }
    Ok(counts
        .iter()
        .enumerate()
        .find(|&(_, &count)| count > u64::from(cap))
        .map(|(bucket, &count)| (bucket as u32, count)))
}

/// Checks a field set by the rules [`crate::RlogWriter::with_key_index_fields`]
/// enforces at finish, so a caller can refuse a set up front instead of failing
/// the write.
pub fn validate_fields(fields: &[(String, KeyType)]) -> Result<(), LogSegError> {
    KeyIndexPlan::new(fields).map(|_| ())
}

/// The object's KEY_IDX footer entry, `None` when it has none.
pub fn key_index_section(footer: &LogFooter) -> Option<&SectionDesc> {
    footer.section(kind::KEY_IDX)
}

/// The object-absolute range of a section-relative `range` of `desc`.
fn absolute(desc: &SectionDesc, range: Range<u64>) -> Result<Range<u64>, LogSegError> {
    let start = desc
        .offset
        .checked_add(range.start)
        .ok_or_else(|| LogSegError::Corrupted("KEY_IDX range overflow".into()))?;
    let end = desc
        .offset
        .checked_add(range.end)
        .ok_or_else(|| LogSegError::Corrupted("KEY_IDX range overflow".into()))?;
    Ok(start..end)
}

/// The fields the object's KEY_IDX header names, with their key types, in
/// section order (ascending by name), read through `read`, which returns the
/// object's bytes for an object-absolute range. Reads exactly the section's
/// `[0, prefix_len)` and verifies the header and directory crcs. Empty when
/// the object has no KEY_IDX.
pub fn key_index_fields<B, F>(
    footer: &LogFooter,
    mut read: F,
) -> Result<Vec<(String, KeyType)>, LogSegError>
where
    B: AsRef<[u8]>,
    F: FnMut(Range<u64>) -> Result<B, LogSegError>,
{
    let Some(desc) = key_index_section(footer) else {
        return Ok(Vec::new());
    };
    let prefix_range = key_idx_prefix_range(desc.prefix_len, desc.len)?;
    let prefix_bytes = read(absolute(desc, prefix_range)?)?;
    let prefix = KeyIdxPrefix::parse(prefix_bytes.as_ref(), desc.prefix_len, desc.len)?;
    Ok(prefix
        .fields()
        .iter()
        .map(|f| (f.name().to_string(), f.key_type()))
        .collect())
}

/// The object's KEY_IDX header `version` byte, read through `read` as
/// [`key_index_fields`] reads it (the section's `[0, prefix_len)`, header and
/// directory crcs verified). `None` when the object has no KEY_IDX.
pub fn key_index_version<B, F>(footer: &LogFooter, mut read: F) -> Result<Option<u8>, LogSegError>
where
    B: AsRef<[u8]>,
    F: FnMut(Range<u64>) -> Result<B, LogSegError>,
{
    let Some(desc) = key_index_section(footer) else {
        return Ok(None);
    };
    let prefix_range = key_idx_prefix_range(desc.prefix_len, desc.len)?;
    let prefix_bytes = read(absolute(desc, prefix_range)?)?;
    let prefix = KeyIdxPrefix::parse(prefix_bytes.as_ref(), desc.prefix_len, desc.len)?;
    Ok(Some(prefix.version()))
}

/// Parses a probe's prefix bytes, `None` (the probe declines) when the
/// section's version is one [`section_resolves_like_the_reader`] rejects.
fn probe_prefix<'a>(
    desc: &SectionDesc,
    prefix_bytes: &'a [u8],
) -> Result<Option<KeyIdxPrefix<'a>>, LogSegError> {
    let prefix = KeyIdxPrefix::parse(prefix_bytes, desc.prefix_len, desc.len)?;
    Ok(section_resolves_like_the_reader(prefix.version()).then_some(prefix))
}

/// Probes the object's KEY_IDX for `key8` of `field` under `key_type`,
/// through `read`, which returns the object's bytes for an object-absolute
/// range. Reads the section's `[0, prefix_len)`, then the one bucket the key
/// mixes to, skipped when that bucket is empty.
///
/// `Ok(None)` declines (no prune): the object has no KEY_IDX, or its header
/// is at a version [`section_resolves_like_the_reader`] rejects (a version 1
/// RLOG section, read after its prefix only), or does not name `field`, or
/// names it under another key type. A caller cannot tell a declined version 1
/// section from a field it does not name, and does not need to: neither
/// proves a key absent. `Ok(Some(blocks))` is the ascending level-0 block ordinals holding the
/// key, empty when the key is absent from the object. An ordinal at or past
/// the footer's `block_count`, or any grammar violation, is
/// [`LogSegError::Corrupted`].
pub fn probe_ranged<B, F>(
    footer: &LogFooter,
    field: &str,
    key_type: KeyType,
    key8: &Key8,
    mut read: F,
) -> Result<Option<Vec<u32>>, LogSegError>
where
    B: AsRef<[u8]>,
    F: FnMut(Range<u64>) -> Result<B, LogSegError>,
{
    let Some(desc) = key_index_section(footer) else {
        return Ok(None);
    };
    let prefix_range = key_idx_prefix_range(desc.prefix_len, desc.len)?;
    let prefix_bytes = read(absolute(desc, prefix_range)?)?;
    let Some(prefix) = probe_prefix(desc, prefix_bytes.as_ref())? else {
        return Ok(None);
    };
    let Some(bucket) = prefix.locate(field, key_type, key8)? else {
        return Ok(None);
    };
    if bucket.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let bucket_bytes = read(absolute(desc, bucket.range())?)?;
    let blocks = bucket.blocks(bucket_bytes.as_ref())?;
    if let Some(&past) = blocks.iter().find(|&&b| u64::from(b) >= footer.block_count) {
        return Err(LogSegError::Corrupted(format!(
            "KEY_IDX field {field:?} names block {past}, object has {} blocks",
            footer.block_count
        )));
    }
    Ok(Some(blocks))
}

/// What a KEY_IDX probe for one key says about one object (ADR-2707 decision
/// 5, tier 1). The three arms are opposite pruning decisions, so a caller
/// matches on all three.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyProbe {
    /// No entry of the field has the key's `key8`: no block of the object
    /// holds the value under the declared type, and the caller drops the
    /// object without a block read.
    Absent,
    /// The ascending, non-empty level-0 block ordinals holding every value
    /// whose `key8` equals the probed one. Only these blocks can hold the
    /// value; a `key8` collision can add a block that does not.
    Present(Vec<u32>),
    /// The object has no KEY_IDX, or its header is at a version
    /// [`section_resolves_like_the_reader`] rejects, or does not name the
    /// field, or names it under another key type: the section says nothing
    /// and every block stays a candidate.
    Declined,
}

/// The first step of a ranged probe: the object-absolute range of the
/// section's `[0, prefix_len)`, `None` when the object has no KEY_IDX (the
/// probe declines with no read).
pub fn key_probe_prefix_range(footer: &LogFooter) -> Result<Option<Range<u64>>, LogSegError> {
    let Some(desc) = key_index_section(footer) else {
        return Ok(None);
    };
    let prefix_range = key_idx_prefix_range(desc.prefix_len, desc.len)?;
    Ok(Some(absolute(desc, prefix_range)?))
}

/// The second step of a ranged probe, over the bytes of
/// [`key_probe_prefix_range`]: either the answer, or the one bucket left to
/// read.
#[derive(Debug, Clone)]
pub enum KeyProbeStep {
    Done(KeyProbe),
    Bucket(KeyProbeBucket),
}

/// The one bucket a ranged probe still has to read.
#[derive(Debug, Clone)]
pub struct KeyProbeBucket {
    bucket: KeyIdxBucket,
    range: Range<u64>,
    block_count: u64,
}

impl KeyProbeBucket {
    /// The bucket's object-absolute byte range: the probe's second read.
    pub fn range(&self) -> Range<u64> {
        self.range.clone()
    }

    /// The third step: verifies and decodes the bucket's bytes (exactly
    /// [`KeyProbeBucket::range`]) and answers for the probed key. An entry of
    /// the bucket naming a block at or past the object's level-0 block count,
    /// whichever key it belongs to, is [`LogSegError::Corrupted`].
    pub fn finish(&self, frame: &[u8]) -> Result<KeyProbe, LogSegError> {
        let entries = self.bucket.entries(frame)?;
        if let Some(&(_, past)) = entries
            .iter()
            .find(|(_, b)| u64::from(*b) >= self.block_count)
        {
            return Err(LogSegError::Corrupted(format!(
                "KEY_IDX bucket {} names block {past}, object has {} blocks",
                self.bucket.bucket(),
                self.block_count
            )));
        }
        let blocks = self.bucket.matching(&entries);
        Ok(if blocks.is_empty() {
            KeyProbe::Absent
        } else {
            KeyProbe::Present(blocks)
        })
    }
}

/// Parses the prefix bytes [`key_probe_prefix_range`] named and locates the
/// bucket `key8` of `field` under `key_type` mixes to. It declines where
/// [`probe_ranged`] does: on a header at a version
/// [`section_resolves_like_the_reader`] rejects, or one that does not name the
/// field under that key type. An empty bucket is [`KeyProbe::Absent`] with no
/// second read.
pub fn locate_key(
    footer: &LogFooter,
    field: &str,
    key_type: KeyType,
    key8: &Key8,
    prefix_bytes: &[u8],
) -> Result<KeyProbeStep, LogSegError> {
    let Some(desc) = key_index_section(footer) else {
        return Ok(KeyProbeStep::Done(KeyProbe::Declined));
    };
    let Some(prefix) = probe_prefix(desc, prefix_bytes)? else {
        return Ok(KeyProbeStep::Done(KeyProbe::Declined));
    };
    let Some(bucket) = prefix.locate(field, key_type, key8)? else {
        return Ok(KeyProbeStep::Done(KeyProbe::Declined));
    };
    if bucket.is_empty() {
        return Ok(KeyProbeStep::Done(KeyProbe::Absent));
    }
    let range = absolute(desc, bucket.range())?;
    Ok(KeyProbeStep::Bucket(KeyProbeBucket {
        bucket,
        range,
        block_count: footer.block_count,
    }))
}

/// [`probe_ranged`] over a [`ByteSource`]: the whole object, or a
/// [`crate::SparseObject`] a ranged read placed the KEY_IDX prefix and bucket
/// into.
pub fn probe_source<S: ByteSource + ?Sized>(
    footer: &LogFooter,
    source: &S,
    field: &str,
    key_type: KeyType,
    key8: &Key8,
) -> Result<Option<Vec<u32>>, LogSegError> {
    probe_ranged(footer, field, key_type, key8, |r| {
        source.read(r.start, r.end.saturating_sub(r.start))
    })
}

/// Rewrites a whole `.rlog` object's KEY_IDX section under header `version`,
/// each field's decoded entries passed through `entries`, moves every later
/// section by the length change, and rewrites the footer and trailer. For
/// tests that need an object an earlier writer produced: a version 1 section,
/// or one holding keys the reader's rule would not.
#[cfg(any(test, feature = "test-support"))]
pub fn rewrite_key_index<F>(
    object: &[u8],
    version: u8,
    mut entries: F,
) -> Result<Vec<u8>, LogSegError>
where
    F: FnMut(&str, KeyType, Vec<(Key8, u32)>) -> Vec<(Key8, u32)>,
{
    use crate::footer::{TRAILER_LEN, open, write_footer_and_trailer};
    use ravel_codec::key_idx::decode_key_idx;

    let corrupt = |what: &str| LogSegError::Corrupted(format!("rewrite_key_index: {what}"));
    let mut footer = open(object)?;
    let desc = *key_index_section(&footer).ok_or_else(|| corrupt("no KEY_IDX section"))?;
    let start = usize::try_from(desc.offset).map_err(|_| corrupt("offset"))?;
    let end = usize::try_from(desc.offset + desc.len).map_err(|_| corrupt("len"))?;
    let section = object
        .get(start..end)
        .ok_or_else(|| corrupt("section past object"))?;
    let inputs: Vec<KeyIdxFieldInput> = decode_key_idx(section, desc.prefix_len)?
        .into_iter()
        .map(|f| KeyIdxFieldInput {
            entries: entries(&f.name, f.key_type, f.entries),
            name: f.name,
            key_type: f.key_type,
        })
        .collect();
    let encoded = encode_key_idx_with(&inputs, version, KEY_IDX_WRITER_BUCKET_BITS)?;
    let trailer_at = object
        .len()
        .checked_sub(TRAILER_LEN)
        .ok_or_else(|| corrupt("object smaller than trailer"))?;
    let footer_len = object
        .get(trailer_at..trailer_at + 4)
        .and_then(|b| <[u8; 4]>::try_from(b).ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| corrupt("trailer"))?;
    let footer_at = trailer_at
        .checked_sub(footer_len as usize)
        .ok_or_else(|| corrupt("footer_len past object"))?;
    let new_len = encoded.bytes.len() as u64;
    let mut out = object[..start].to_vec();
    out.extend_from_slice(&encoded.bytes);
    out.extend_from_slice(
        object
            .get(end..footer_at)
            .ok_or_else(|| corrupt("sections"))?,
    );
    for s in &mut footer.sections {
        if s.kind == kind::KEY_IDX {
            s.len = new_len;
            s.uncomp_len = new_len;
            s.crc32c = crc32c::crc32c(&encoded.bytes);
            s.prefix_len = encoded.prefix_len;
        } else if s.offset > desc.offset {
            s.offset = s.offset - desc.len + new_len;
        }
    }
    write_footer_and_trailer(&mut out, &footer);
    Ok(out)
}

/// Verifies the section crc and every crc and frame of the object's whole
/// KEY_IDX section, given its stored bytes, and returns its shape, for
/// inspection. `Ok(None)` when the object has no KEY_IDX.
pub fn summarize(footer: &LogFooter, section: &[u8]) -> Result<Option<KeyIdxSummary>, LogSegError> {
    let Some(desc) = key_index_section(footer) else {
        return Ok(None);
    };
    if section.len() as u64 != desc.len || crc32c::crc32c(section) != desc.crc32c {
        return Err(LogSegError::Corrupted(
            "KEY_IDX section crc mismatch".into(),
        ));
    }
    Ok(Some(verify_key_idx(section, desc.prefix_len)?))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::footer::open;
    use crate::source::SparseObject;
    use crate::writer::{ObjectIdentity, RlogConfig, RlogWriter};
    use ravel_types::logstream::{AttrValue, LogStreamId};

    use crate::record::LogRecord;

    /// Twelve rows, three per block: `user_id = i / 3`, so value `v` sits in
    /// block `v` alone.
    fn object() -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 3,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: [1; 16],
            shard: 0,
            writer_id: [2; 16],
            writer_epoch: 1,
            writer_seq: 1,
        };
        let mut w = RlogWriter::new(cfg, identity)
            .with_key_index_fields(vec![("user_id".into(), KeyType::I64)]);
        for i in 0..12i64 {
            w.push(LogRecord {
                stream_id: LogStreamId([3; 16]),
                stream_attrs: crate::record::stream_attrs_bytes(&[], "", "", &[]),
                ts_ns: i,
                observed_ts_ns: i,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: "b".into(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: vec![("user_id".into(), AttrValue::I64(i / 3))],
            })
            .expect("push");
        }
        w.finish().expect("finish")
    }

    /// A ranged reader holding only the KEY_IDX prefix and the probed bucket
    /// answers the probe: nothing else of the object is read.
    #[test]
    fn a_probe_reads_the_prefix_and_one_bucket_only() {
        let obj = object();
        let footer = open(&obj).expect("open");
        assert_eq!(footer.block_count, 4);
        let mut reads = Vec::new();
        let blocks = probe_ranged(&footer, "user_id", KeyType::I64, &key8_i64(2), |r| {
            reads.push(r.clone());
            Ok::<_, LogSegError>(obj[r.start as usize..r.end as usize].to_vec())
        })
        .expect("probe");
        assert_eq!(blocks, Some(vec![2]));
        assert_eq!(reads.len(), 2, "prefix, then one bucket");

        let mut sparse = SparseObject::new(obj.len() as u64);
        for r in &reads {
            sparse
                .place(
                    r.start,
                    bytes::Bytes::copy_from_slice(&obj[r.start as usize..r.end as usize]),
                )
                .expect("place");
        }
        assert_eq!(
            probe_source(&footer, &sparse, "user_id", KeyType::I64, &key8_i64(2))
                .expect("sparse probe"),
            Some(vec![2])
        );
        assert_eq!(
            key_index_fields(&footer, |r| sparse.read(r.start, r.end - r.start)).expect("fields"),
            vec![("user_id".to_string(), KeyType::I64)]
        );
    }

    /// A block ordinal at or past the object's level-0 block count is
    /// `Corrupted`, never returned.
    #[test]
    fn an_ordinal_past_the_block_count_is_corrupted() {
        let obj = object();
        let mut footer = open(&obj).expect("open");
        assert_eq!(
            probe_source(&footer, &obj[..], "user_id", KeyType::I64, &key8_i64(3)).expect("probe"),
            Some(vec![3])
        );
        footer.block_count = 3;
        let err = probe_source(&footer, &obj[..], "user_id", KeyType::I64, &key8_i64(3))
            .expect_err("block 3 of 3");
        assert!(
            matches!(&err, LogSegError::Corrupted(m) if m.contains("names block 3, object has 3 blocks")),
            "{err:?}"
        );
        // A block inside the lowered count still answers.
        assert_eq!(
            probe_source(&footer, &obj[..], "user_id", KeyType::I64, &key8_i64(2)).expect("probe"),
            Some(vec![2])
        );
    }

    /// A flipped byte in the prefix or in a bucket fails its own crc as
    /// `Corrupted`, and `summarize` checks the section crc.
    #[test]
    fn a_corrupt_section_is_corrupted_not_a_wrong_answer() {
        let obj = object();
        let footer = open(&obj).expect("open");
        let desc = *key_index_section(&footer).expect("KEY_IDX");
        for at in [desc.offset + 2, desc.offset + desc.len - 1] {
            let mut bad = obj.clone();
            bad[at as usize] ^= 0x40;
            let out = (0..4)
                .map(|v| probe_source(&footer, &bad[..], "user_id", KeyType::I64, &key8_i64(v)));
            assert!(
                out.into_iter()
                    .any(|r| matches!(r, Err(LogSegError::Corrupted(_)))),
                "flip at {at}"
            );
            let section = &bad[desc.offset as usize..(desc.offset + desc.len) as usize];
            assert!(matches!(
                summarize(&footer, section),
                Err(LogSegError::Corrupted(_))
            ));
        }
        let section = &obj[desc.offset as usize..(desc.offset + desc.len) as usize];
        let summary = summarize(&footer, section)
            .expect("summarize")
            .expect("present");
        assert_eq!(summary.fields.len(), 1);
        assert_eq!(summary.fields[0].entry_count, 4);
    }

    fn section_of<'a>(obj: &'a [u8], footer: &LogFooter) -> &'a [u8] {
        let desc = key_index_section(footer).expect("KEY_IDX");
        &obj[desc.offset as usize..(desc.offset + desc.len) as usize]
    }

    fn whole<'a>(obj: &'a [u8]) -> impl FnMut(Range<u64>) -> Result<&'a [u8], LogSegError> {
        move |r| Ok(&obj[r.start as usize..r.end as usize])
    }

    /// The writer stamps [`RLOG_KEY_IDX_VERSION`]; the same entries under
    /// version 1 read, verify and name their fields the same, the predicate
    /// tells the two apart, and a probe declines on version 1.
    #[test]
    fn both_versions_read_alike_and_only_version_two_resolves_like_the_reader() {
        let v2 = object();
        let v1 = rewrite_key_index(&v2, KEY_IDX_VERSION_1, |_, _, e| e).expect("rewrite");
        assert_eq!(v1.len(), v2.len());
        let (f1, f2) = (open(&v1).expect("open v1"), open(&v2).expect("open v2"));
        assert_eq!(
            key_index_version(&f2, whole(&v2)).expect("v2"),
            Some(RLOG_KEY_IDX_VERSION)
        );
        assert_eq!(key_index_version(&f1, whole(&v1)).expect("v1"), Some(1));
        assert_eq!(RLOG_KEY_IDX_VERSION, 2);
        assert!(section_resolves_like_the_reader(RLOG_KEY_IDX_VERSION));
        assert!(!section_resolves_like_the_reader(KEY_IDX_VERSION_1));
        for v in KEY_IDX_VERSIONS {
            assert_eq!(section_resolves_like_the_reader(v), v == 2, "version {v}");
        }

        let s1 = summarize(&f1, section_of(&v1, &f1))
            .expect("v1")
            .expect("present");
        let s2 = summarize(&f2, section_of(&v2, &f2))
            .expect("v2")
            .expect("present");
        assert_eq!((s1.version, s2.version), (1, 2));
        assert_eq!(s1.fields, s2.fields);
        assert_eq!(
            key_index_fields(&f1, whole(&v1)).expect("v1"),
            key_index_fields(&f2, whole(&v2)).expect("v2")
        );
        let decoded = |obj: &[u8], footer: &LogFooter| {
            ravel_codec::key_idx::decode_key_idx(
                section_of(obj, footer),
                key_index_section(footer).expect("KEY_IDX").prefix_len,
            )
            .expect("decode")
        };
        assert_eq!(decoded(&v1, &f1), decoded(&v2, &f2));
        // Version 1 holds every key version 2 does, and still declines after
        // reading its prefix alone; version 2 answers.
        let user_entries = &decoded(&v1, &f1)[0].entries;
        for v in 0..5 {
            assert_eq!(
                user_entries.contains(&(key8_i64(v), v as u32)),
                v < 4,
                "value {v}"
            );
            let mut reads = Vec::new();
            let declined = probe_ranged(&f1, "user_id", KeyType::I64, &key8_i64(v), |r| {
                reads.push(r.clone());
                Ok::<_, LogSegError>(&v1[r.start as usize..r.end as usize])
            })
            .expect("probe v1");
            assert_eq!(declined, None, "version 1, value {v}");
            assert_eq!(reads.len(), 1, "version 1 reads the prefix only");
            assert_eq!(
                probe_source(&f1, &v1[..], "user_id", KeyType::I64, &key8_i64(v)).expect("v1"),
                None
            );
            let want = if v < 4 { vec![v as u32] } else { Vec::new() };
            assert_eq!(
                probe_source(&f2, &v2[..], "user_id", KeyType::I64, &key8_i64(v)).expect("v2"),
                Some(want),
                "version 2, value {v}"
            );
        }
        // Everything outside the section's version byte, header crc and footer
        // section crc is unchanged.
        let desc = *key_index_section(&f2).expect("KEY_IDX");
        assert_eq!(v1[..desc.offset as usize], v2[..desc.offset as usize]);
        assert_eq!(v1[desc.offset as usize], 1);
        assert_eq!(v2[desc.offset as usize], 2);
        let mut f1_as_v2 = f1.clone();
        if let Some(d) = f1_as_v2
            .sections
            .iter_mut()
            .find(|d| d.kind == kind::KEY_IDX)
        {
            d.crc32c = desc.crc32c;
        }
        assert_eq!(f1_as_v2, f2);
    }

    /// One block per record, each a resolution case for field `k` declared
    /// under `key_type`: `scalar(n)` builds a value of that type, `wrong` a
    /// scalar of another type. Returns the object and, per block, the value
    /// the reader's merged view resolves `k` to (`None`: no key).
    fn resolution_object(
        key_type: KeyType,
        scalar: impl Fn(i64) -> AttrValue,
        wrong: AttrValue,
    ) -> (Vec<u8>, Vec<Option<AttrValue>>) {
        let nested_list = AttrValue::List(vec![scalar(90)]);
        let nested_map = AttrValue::Map(vec![("k".into(), scalar(91))]);
        let k = |v: AttrValue| vec![("k".to_string(), v)];
        // (resource, scope, record, resolved)
        let cases: Vec<(Vec<_>, Vec<_>, Vec<_>, Option<AttrValue>)> = vec![
            // nested resource entry, then a scope scalar: the scalar.
            (
                k(nested_list.clone()),
                k(scalar(1)),
                vec![],
                Some(scalar(1)),
            ),
            // resource scalar, then a nested scope entry: the scalar.
            (k(scalar(2)), k(nested_map.clone()), vec![], Some(scalar(2))),
            // nested only: no key.
            (k(nested_map.clone()), k(nested_list.clone()), vec![], None),
            // scalar only.
            (vec![], k(scalar(4)), vec![], Some(scalar(4))),
            // record-level value over a nested and a scalar stream entry.
            (
                k(nested_list.clone()),
                k(scalar(5)),
                k(scalar(55)),
                Some(scalar(55)),
            ),
            // a wrong-type scalar first: no key, and the later scalar shadowed.
            (k(wrong.clone()), k(scalar(6)), vec![], None),
            // nested, then a wrong-type scalar, then nothing of the type.
            (k(nested_list.clone()), k(wrong.clone()), vec![], None),
            // record-level value over a stream layer holding only nested.
            (k(nested_map.clone()), vec![], k(scalar(8)), Some(scalar(8))),
        ];
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: [1; 16],
            shard: 0,
            writer_id: [2; 16],
            writer_epoch: 1,
            writer_seq: 1,
        };
        let mut w =
            RlogWriter::new(cfg, identity).with_key_index_fields(vec![("k".into(), key_type)]);
        let mut resolved = Vec::new();
        for (i, (resource, scope, attrs, want)) in cases.into_iter().enumerate() {
            let scope_name = format!("s{i}");
            w.push(LogRecord {
                stream_id: ravel_types::logstream::log_stream_id(
                    &resource,
                    &scope_name,
                    "1",
                    &scope,
                ),
                stream_attrs: crate::record::stream_attrs_bytes(
                    &resource,
                    &scope_name,
                    "1",
                    &scope,
                ),
                ts_ns: i as i64,
                observed_ts_ns: i as i64,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: "b".into(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs,
            })
            .expect("push");
            resolved.push(want);
        }
        (w.finish().expect("finish"), resolved)
    }

    /// The writer's keys for a declared field are exactly the values the
    /// reader's merged view resolves: nested resource and scope entries are
    /// skipped, the first remaining entry wins with resource before scope, a
    /// record-level value wins over both, and a wrong-type scalar first yields
    /// no key and shadows later entries. Pinned for I64, Str and Bytes fields.
    #[test]
    fn declared_keys_follow_the_readers_merged_view() {
        use ravel_codec::key_idx::decode_key_idx;
        let i64_case = (
            KeyType::I64,
            Box::new(AttrValue::I64) as Box<dyn Fn(i64) -> AttrValue>,
            AttrValue::Str("7".into()),
        );
        let str_case = (
            KeyType::Str,
            Box::new(|n: i64| AttrValue::Str(format!("v{n}"))) as Box<dyn Fn(i64) -> AttrValue>,
            AttrValue::I64(7),
        );
        let bytes_case = (
            KeyType::Bytes,
            Box::new(|n: i64| AttrValue::Bytes(n.to_le_bytes().to_vec()))
                as Box<dyn Fn(i64) -> AttrValue>,
            AttrValue::Str("7".into()),
        );
        for (key_type, scalar, wrong) in [i64_case, str_case, bytes_case] {
            let (obj, resolved) = resolution_object(key_type, &scalar, wrong);
            let footer = open(&obj).expect("open");
            assert_eq!(footer.block_count, resolved.len() as u64);
            let desc = key_index_section(&footer).expect("KEY_IDX");
            let got: Vec<(Key8, u32)> = decode_key_idx(section_of(&obj, &footer), desc.prefix_len)
                .expect("decode")
                .remove(0)
                .entries;
            // Blocks are laid out in stream order, not push order, and every
            // resolved value is distinct, so the keys name the cases.
            let mut want: Vec<Key8> = resolved
                .iter()
                .flatten()
                .map(|v| match v {
                    AttrValue::I64(x) => key8_i64(*x),
                    AttrValue::Str(s) => key8_str(s),
                    AttrValue::Bytes(b) => key8_bytes(b),
                    other => panic!("unexpected resolved value {other:?}"),
                })
                .collect();
            want.sort_unstable();
            let mut keys: Vec<Key8> = got.iter().map(|(k, _)| *k).collect();
            keys.sort_unstable();
            assert_eq!(keys, want, "{key_type:?}");
            let blocks: std::collections::BTreeSet<u32> = got.iter().map(|(_, b)| *b).collect();
            assert_eq!(blocks.len(), got.len(), "one key per block, {key_type:?}");
            assert_eq!(
                key_index_version(&footer, whole(&obj)).expect("version"),
                Some(RLOG_KEY_IDX_VERSION)
            );
        }
    }

    /// The step API reads the prefix, then at most one bucket, and answers
    /// each of the three cases the way `probe_ranged` does.
    #[test]
    fn the_step_api_matches_probe_ranged() {
        let obj = object();
        let footer = open(&obj).expect("open");
        let read = |r: Range<u64>| &obj[r.start as usize..r.end as usize];
        let prefix = key_probe_prefix_range(&footer)
            .expect("range")
            .expect("KEY_IDX");
        let step = |field: &str, key_type: KeyType, key8: &Key8| match locate_key(
            &footer,
            field,
            key_type,
            key8,
            read(prefix.clone()),
        )
        .expect("locate")
        {
            KeyProbeStep::Done(answer) => (answer, 1),
            KeyProbeStep::Bucket(bucket) => {
                (bucket.finish(read(bucket.range())).expect("finish"), 2)
            }
        };
        for v in 0..4 {
            assert_eq!(
                step("user_id", KeyType::I64, &key8_i64(v)),
                (KeyProbe::Present(vec![v as u32]), 2)
            );
        }
        let (absent, _) = step("user_id", KeyType::I64, &key8_i64(99));
        assert_eq!(absent, KeyProbe::Absent);
        assert_eq!(
            probe_source(&footer, &obj[..], "user_id", KeyType::I64, &key8_i64(99)).expect("probe"),
            Some(Vec::new())
        );
        assert_eq!(
            step("other", KeyType::I64, &key8_i64(1)),
            (KeyProbe::Declined, 1)
        );
        assert_eq!(
            step("user_id", KeyType::Str, &key8_str("1")),
            (KeyProbe::Declined, 1)
        );
        // `key8_str` is `key8_bytes` of the UTF-8 bytes.
        assert_eq!(key8_str("abc"), key8_bytes(b"abc"));
    }
}
