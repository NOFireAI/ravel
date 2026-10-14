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

use std::collections::HashMap;
use std::ops::Range;

use ravel_codec::key_idx::{
    KEY_IDX_MAX_NAME_LEN, KEY_IDX_WRITER_BUCKET_BITS, KeyIdxFieldInput, KeyIdxPrefix, bucket_index,
    encode_key_idx, key_idx_prefix_range, verify_key_idx,
};

use crate::error::LogSegError;
use crate::footer::{LogFooter, SectionDesc, kind};
use crate::record::{ColumnValue, FieldType};
use crate::source::ByteSource;

pub use ravel_codec::key_idx::{
    EncodedKeyIdx, KEY_IDX_BUCKET_MAX_ENTRIES, KEY_IDX_MAX_FIELDS, Key8, KeyIdxFieldSummary,
    KeyIdxSummary, KeyType, key8_bytes, key8_i64, key8_id16, key8_str,
};

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
        Ok(Some(encode_key_idx(&inputs)?))
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

/// Probes the object's KEY_IDX for `key8` of `field` under `key_type`,
/// through `read`, which returns the object's bytes for an object-absolute
/// range. Reads the section's `[0, prefix_len)`, then the one bucket the key
/// mixes to, skipped when that bucket is empty.
///
/// `Ok(None)` declines (no prune): the object has no KEY_IDX, or its header
/// does not name `field`, or names it under another key type.
/// `Ok(Some(blocks))` is the ascending level-0 block ordinals holding the
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
    let prefix = KeyIdxPrefix::parse(prefix_bytes.as_ref(), desc.prefix_len, desc.len)?;
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
}
