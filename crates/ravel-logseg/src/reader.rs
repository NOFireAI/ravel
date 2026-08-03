//! Pruned scan reader (docs/log-segment-format.md "Pruning soundness").
//!
//! [`RlogReader::scan`] resolves a predicate to coarse ts/stream bounds, prunes
//! blocks through the skip index and per-block blooms, then reads, verifies, and
//! re-evaluates the survivors exactly. Pruning is proof-based: a block is
//! dropped only when the skip index proves its bounds disjoint or a bloom proves
//! a required word absent. A corrupt BLOOM section degrades to no bloom pruning
//! (scan the skip survivors) and surfaces a counter; corrupt BLOCKS data is a
//! loud `Corrupted` error.

use ravel_types::logstream::AttrValue;

use crate::block::{ColumnPlan, DecodedBlock, read_block};
use crate::bloom_section::BloomSection;
use crate::error::LogSegError;
use crate::field_dir::FieldDir;
use crate::footer::{COMP_ZSTD, LogFooter, SectionDesc, kind, open};
use crate::page::DEFAULT_MAX_UNCOMP;
use crate::postings::{PostingsSection, term_key};
use crate::record::{
    COL_BODY, COL_FLAGS, COL_OBSERVED_TS, COL_SEVERITY_NUM, COL_SEVERITY_TEXT, COL_SPAN_ID,
    COL_STREAM_REF, COL_TRACE_ID, COL_TS, FieldSel, FieldType, LogRecord, Predicate, resolve_value,
};
use crate::skip_index::SkipIndex;
use crate::stream_dir::StreamDir;
use crate::tokenizer::tokens;
use crate::writer::RlogConfig;

pub(crate) const MAX_STREAMS: u64 = 1 << 24;
pub(crate) const MAX_FIELDS: u64 = 1 << 20;
pub(crate) const MAX_BLOCKS: u64 = 1 << 24;

/// Counters describing how much a scan pruned (docs/log-segment-format.md).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanStats {
    pub blocks_total: u32,
    pub blocks_after_skip: u32,
    pub blocks_after_postings: u32,
    pub blocks_after_bloom: u32,
    pub blocks_scanned: u32,
    /// Set when the BLOOM section could not be parsed and bloom pruning was
    /// skipped (the scan still returns correct results).
    pub bloom_degraded: bool,
    /// Set when the POSTINGS section (or a probed entry within it) could not
    /// be parsed and postings pruning was skipped for that arm (the scan
    /// still returns correct results via bloom + exact scan).
    pub postings_degraded: bool,
}

/// An opened RLOG object ready to scan.
pub struct RlogReader<'a> {
    bytes: &'a [u8],
    stream_dir: StreamDir,
    field_dir: FieldDir,
    skip: SkipIndex,
    blocks_offset: u64,
    bloom: SectionDesc,
    /// Absent when the object was written with no indexed fields
    /// (docs/log-segment-format.md: POSTINGS is an optional section).
    postings: Option<SectionDesc>,
}

impl<'a> RlogReader<'a> {
    /// Opens and validates the object, decoding the directories and the skip
    /// index. The skip index carries the block framing, so a corrupt SKIP_IDX
    /// (unlike a corrupt BLOOM) is a loud `Corrupted` error rather than a
    /// degrade: without it no block can be located.
    pub fn new(bytes: &'a [u8], cfg: &RlogConfig) -> Result<Self, LogSegError> {
        let footer = open(bytes)?;
        let stream_raw = read_section(bytes, section(&footer, kind::STREAM_DIR)?, cfg)?;
        let stream_dir = StreamDir::decode(&stream_raw, MAX_STREAMS)?;
        let field_raw = read_section(bytes, section(&footer, kind::FIELD_DIR)?, cfg)?;
        let field_dir = FieldDir::decode(&field_raw, MAX_FIELDS)?;
        let skip_raw = read_section(bytes, section(&footer, kind::SKIP_IDX)?, cfg)?;
        let skip = SkipIndex::decode(&skip_raw, MAX_BLOCKS)?;
        let blocks = *section(&footer, kind::BLOCKS)?;
        let bloom = *section(&footer, kind::BLOOM)?;
        let postings = footer.section(kind::POSTINGS).copied();
        Ok(RlogReader {
            bytes,
            stream_dir,
            field_dir,
            skip,
            blocks_offset: blocks.offset,
            bloom,
            postings,
        })
    }

    /// The column plans for every dynamic column, for block decode.
    fn plans(&self) -> Vec<ColumnPlan> {
        column_plans(&self.field_dir)
    }

    /// Scans the object for records matching `pred`.
    pub fn scan(&self, pred: &Predicate) -> Result<(Vec<LogRecord>, ScanStats), LogSegError> {
        let mut stats = ScanStats {
            blocks_total: self.skip.l0.len() as u32,
            ..ScanStats::default()
        };

        // Collect the And-flattened arms.
        let mut arms: Vec<&Predicate> = Vec::new();
        flatten(pred, &mut arms);

        // Coarse ts range: intersect every TsRange arm.
        let mut ts_min = i64::MIN;
        let mut ts_max = i64::MAX;
        for a in &arms {
            if let Predicate::TsRange { min_ns, max_ns } = a {
                ts_min = ts_min.max(*min_ns);
                ts_max = ts_max.min(*max_ns);
            }
        }
        if ts_min > ts_max {
            return Ok((Vec::new(), stats));
        }

        // Stream filter: intersect every StreamIn arm's resolved refs.
        let mut stream_refs: Option<Vec<u32>> = None;
        for a in &arms {
            if let Predicate::StreamIn(ids) = a {
                let mut refs: Vec<u32> = ids
                    .iter()
                    .filter_map(|id| self.stream_dir.stream_ref(id))
                    .collect();
                refs.sort_unstable();
                refs.dedup();
                stream_refs = Some(match stream_refs {
                    None => refs,
                    Some(prev) => prev.into_iter().filter(|r| refs.contains(r)).collect(),
                });
            }
        }
        if stream_refs.as_ref().is_some_and(|r| r.is_empty()) {
            return Ok((Vec::new(), stats));
        }

        // Skip-index pruning.
        let mut candidates = self
            .skip
            .candidate_blocks(ts_min, ts_max, stream_refs.as_deref());
        stats.blocks_after_skip = candidates.len() as u32;

        // Postings pruning. Exact (not probabilistic): a probed term's block
        // list is the whole truth for that field, so it can prune down to
        // zero (docs/log-segment-format.md "Pruning soundness"). An arm whose
        // field is unindexed or capped returns `Ok(None)` and is skipped
        // (falls through to bloom + exact scan); a corrupt section or entry
        // degrades the same way and sets `postings_degraded`.
        if let Some(desc) = &self.postings {
            let postings_arms = self.postings_arms(&arms);
            if !postings_arms.is_empty() {
                match self.postings_section_verified(desc).and_then(PostingsSection::parse) {
                    Ok(section) => {
                        for (cid, term) in &postings_arms {
                            match section.probe(*cid, term) {
                                Ok(Some(blocks)) => {
                                    let allowed: std::collections::HashSet<usize> =
                                        blocks.iter().map(|&b| b as usize).collect();
                                    candidates.retain(|b| allowed.contains(b));
                                }
                                Ok(None) => {}
                                Err(_) => stats.postings_degraded = true,
                            }
                        }
                    }
                    Err(_) => stats.postings_degraded = true,
                }
            }
        }
        stats.blocks_after_postings = candidates.len() as u32;

        // Bloom pruning. A parse failure degrades to no bloom pruning.
        let bloom_bytes = self.section_stored(&self.bloom)?;
        let bloom_section = match BloomSection::parse(bloom_bytes) {
            Ok(s) => Some(s),
            Err(_) => {
                stats.bloom_degraded = true;
                None
            }
        };
        let bloom_arms = self.bloom_arms(&arms);

        let mut survivors: Vec<usize> = Vec::new();
        for &b in &candidates {
            if let Some(section) = &bloom_section
                && self.block_pruned_by_bloom(section, b, &bloom_arms)
            {
                continue;
            }
            survivors.push(b);
        }
        stats.blocks_after_bloom = survivors.len() as u32;

        // Read, verify, decode, and re-evaluate the survivors exactly.
        let plans = self.plans();
        let mut out = Vec::new();
        for &b in &survivors {
            let entry = &self.skip.l0[b];
            let block_bytes = self.block_bytes(entry.block_offset, entry.block_len)?;
            let decoded = read_block(block_bytes, entry.block_crc32c, &plans, DEFAULT_MAX_UNCOMP)?;
            for row in 0..decoded.record_count() {
                if self.eval(pred, &decoded, row)? {
                    out.push(self.rebuild_record(&decoded, row)?);
                }
            }
        }
        stats.blocks_scanned = survivors.len() as u32;
        Ok((out, stats))
    }

    /// The bloom-eligible arms: HasWord on any field, and Equals on a short
    /// string field. Each yields `(column_id, key tokens)` where all tokens must
    /// probe positive for a block to survive. An arm that cannot map to a bloom
    /// column (e.g. HasWord over a name that is not a string column) is omitted
    /// so it never prunes.
    fn bloom_arms(&self, arms: &[&Predicate]) -> Vec<(u32, Vec<Vec<u8>>)> {
        let mut out = Vec::new();
        for a in arms {
            match a {
                Predicate::HasWord { field, word } => {
                    if let Some(cid) = self.word_column(field) {
                        let toks = tokens(word);
                        if !toks.is_empty() {
                            out.push((cid, toks));
                        }
                    }
                }
                Predicate::Equals {
                    field,
                    value: AttrValue::Str(s),
                } if s.len() <= 64 => {
                    if let Some(cid) = self.word_column(field) {
                        out.push((cid, vec![s.clone().into_bytes()]));
                    }
                }
                _ => {}
            }
        }
        out
    }

    /// The postings-eligible arms: `Equals` on an attribute field that has a
    /// dynamic column, paired with its term-key bytes. A field with no such
    /// column (unindexed, overflowed, or not yet seen by FIELD_DIR) is
    /// omitted; [`PostingsSection::probe`] separately reports "not indexed or
    /// capped" for a column that has no POSTINGS entry, so both cases fall
    /// through to bloom + exact scan without narrowing results.
    fn postings_arms(&self, arms: &[&Predicate]) -> Vec<(u32, Vec<u8>)> {
        let mut out = Vec::new();
        for a in arms {
            if let Predicate::Equals {
                field: FieldSel::Attr(name),
                value,
            } = a
            {
                let (ty, cv) = resolve_value(value);
                if let Some(entry) = self.field_dir.column(name, ty) {
                    out.push((entry.column_id, term_key(&cv)));
                }
            }
        }
        out
    }

    /// The bloom column id for a string field selector, if one exists.
    fn word_column(&self, field: &FieldSel) -> Option<u32> {
        match field {
            FieldSel::Body => Some(COL_BODY),
            FieldSel::SeverityText => Some(COL_SEVERITY_TEXT),
            FieldSel::Attr(name) => self
                .field_dir
                .column(name, FieldType::Str)
                .map(|e| e.column_id),
        }
    }

    /// True if `block`'s bloom proves some required arm's key absent.
    fn block_pruned_by_bloom(
        &self,
        section: &BloomSection<'_>,
        block: usize,
        arms: &[(u32, Vec<Vec<u8>>)],
    ) -> bool {
        if arms.is_empty() {
            return false;
        }
        let view = match section.entry(block) {
            Ok(v) => v,
            // A corrupt entry cannot prune: scan the block instead.
            Err(_) => return false,
        };
        for (cid, toks) in arms {
            if !toks.iter().all(|t| view.may_contain(*cid, t)) {
                return true;
            }
        }
        false
    }

    /// Slices and crc-verifies the POSTINGS section's stored bytes before
    /// [`PostingsSection::parse`] sees them. Unlike BLOOM and BLOCKS, whose
    /// per-entry/per-block crc is the only checksum ever consulted (a
    /// selective scan never reads them whole), the POSTINGS sparse-index
    /// header sits in front of every probe and is otherwise unchecksummed on
    /// this access path: `desc.crc32c` is computed and stored by the writer
    /// over the whole section (same as STREAM_DIR/FIELD_DIR/SKIP_IDX) but was
    /// never consulted here, so a single corrupted header byte that
    /// redirects a probe to a different, still crc-valid term block passed
    /// silently. Checking it costs nothing extra: the whole object is
    /// already resident. [`PostingsSection::probe`]'s own structural check
    /// (a decoded block's first term must match the sparse entry that
    /// pointed at it) is the complementary guard that still holds under a
    /// future ranged reader that fetches less than the whole section.
    fn postings_section_verified(&self, desc: &SectionDesc) -> Result<&'a [u8], LogSegError> {
        let stored = self.section_stored(desc)?;
        if crc32c::crc32c(stored) != desc.crc32c {
            return Err(LogSegError::Corrupted(
                "postings section crc mismatch".into(),
            ));
        }
        Ok(stored)
    }

    /// Absolute slice of a section's stored bytes.
    fn section_stored(&self, desc: &SectionDesc) -> Result<&'a [u8], LogSegError> {
        let start = usize::try_from(desc.offset)
            .map_err(|_| LogSegError::Corrupted("section offset range".into()))?;
        let len = usize::try_from(desc.len)
            .map_err(|_| LogSegError::Corrupted("section len range".into()))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| LogSegError::Corrupted("section range overflow".into()))?;
        self.bytes
            .get(start..end)
            .ok_or_else(|| LogSegError::Corrupted("section out of bounds".into()))
    }

    /// Slice of one block's stored bytes (offset is relative to BLOCKS).
    fn block_bytes(&self, block_offset: u64, block_len: u64) -> Result<&'a [u8], LogSegError> {
        let abs = self
            .blocks_offset
            .checked_add(block_offset)
            .ok_or_else(|| LogSegError::Corrupted("block offset overflow".into()))?;
        let start = usize::try_from(abs)
            .map_err(|_| LogSegError::Corrupted("block offset range".into()))?;
        let len = usize::try_from(block_len)
            .map_err(|_| LogSegError::Corrupted("block len range".into()))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| LogSegError::Corrupted("block range overflow".into()))?;
        self.bytes
            .get(start..end)
            .ok_or_else(|| LogSegError::Corrupted("block out of bounds".into()))
    }

    // --- exact evaluation ---------------------------------------------------

    fn eval(
        &self,
        pred: &Predicate,
        block: &DecodedBlock,
        row: usize,
    ) -> Result<bool, LogSegError> {
        Ok(match pred {
            Predicate::And(v) => {
                for p in v {
                    if !self.eval(p, block, row)? {
                        return Ok(false);
                    }
                }
                true
            }
            Predicate::TsRange { min_ns, max_ns } => {
                let ts = i64_at(block, COL_TS, row)?;
                ts >= *min_ns && ts <= *max_ns
            }
            Predicate::StreamIn(ids) => {
                let r = u32::try_from(i64_at(block, COL_STREAM_REF, row)?)
                    .map_err(|_| LogSegError::Corrupted("stream_ref range".into()))?;
                match self.stream_dir.stream_id(r) {
                    Some(sid) => ids.contains(sid),
                    None => false,
                }
            }
            Predicate::HasWord { field, word } => {
                let value = self.field_text(field, block, row)?;
                match value {
                    Some(bytes) => phrase_match(&bytes, word),
                    None => false,
                }
            }
            Predicate::Equals { field, value } => self.equals(field, value, block, row)?,
        })
    }

    /// The tokenizable text of a field for a row, if present.
    fn field_text(
        &self,
        field: &FieldSel,
        block: &DecodedBlock,
        row: usize,
    ) -> Result<Option<Vec<u8>>, LogSegError> {
        Ok(match field {
            FieldSel::Body => str_at(block, COL_BODY, row),
            FieldSel::SeverityText => str_at(block, COL_SEVERITY_TEXT, row),
            FieldSel::Attr(name) => {
                match self.field_dir.column(name, FieldType::Str) {
                    Some(e) => str_at(block, e.column_id, row),
                    // Fall back to an overflow attr of type Str.
                    None => self.overflow_attr(block, row, name)?.and_then(|v| match v {
                        AttrValue::Str(s) => Some(s.into_bytes()),
                        _ => None,
                    }),
                }
            }
        })
    }

    fn equals(
        &self,
        field: &FieldSel,
        value: &AttrValue,
        block: &DecodedBlock,
        row: usize,
    ) -> Result<bool, LogSegError> {
        match field {
            FieldSel::Body | FieldSel::SeverityText => {
                let col = if matches!(field, FieldSel::Body) {
                    COL_BODY
                } else {
                    COL_SEVERITY_TEXT
                };
                let stored = str_at(block, col, row);
                Ok(match (value, stored) {
                    (AttrValue::Str(s), Some(b)) => s.as_bytes() == b.as_slice(),
                    _ => false,
                })
            }
            FieldSel::Attr(name) => {
                let (ty, _) = resolve_value(value);
                if let Some(entry) = self.field_dir.column(name, ty) {
                    Ok(attr_equals(block, entry.column_id, ty, value, row))
                } else {
                    // Overflow: compare against the decoded attrs_raw value.
                    Ok(self
                        .overflow_attr(block, row, name)?
                        .as_ref()
                        .is_some_and(|v| attr_value_eq(v, value)))
                }
            }
        }
    }

    /// Looks up an attribute by name in a row's decoded `attrs_raw`, if any.
    fn overflow_attr(
        &self,
        block: &DecodedBlock,
        row: usize,
        name: &str,
    ) -> Result<Option<AttrValue>, LogSegError> {
        let raw = match block.str_col(crate::record::COL_ATTRS_RAW) {
            Some(col) => match col.get(row).and_then(|v| v.as_ref()) {
                Some(bytes) => bytes.clone(),
                None => return Ok(None),
            },
            None => return Ok(None),
        };
        let attrs = decode_canonical_attrs(&raw)?;
        Ok(attrs.into_iter().find(|(k, _)| k == name).map(|(_, v)| v))
    }

    /// Rebuilds a full [`LogRecord`] from a decoded row.
    fn rebuild_record(&self, block: &DecodedBlock, row: usize) -> Result<LogRecord, LogSegError> {
        rebuild_record(&self.stream_dir, &self.field_dir, block, row)
    }
}

/// The column plans for every dynamic column, for block decode. Shared by the
/// whole-object [`RlogReader`] and the ranged [`crate::ranged::RlogRangeReader`]
/// so both decode blocks through one column-plan derivation.
pub(crate) fn column_plans(field_dir: &FieldDir) -> Vec<ColumnPlan> {
    field_dir
        .entries()
        .iter()
        .map(|e| ColumnPlan {
            column_id: e.column_id,
            ty: e.ty,
        })
        .collect()
}

/// Rebuilds a full [`LogRecord`] from a decoded row, given the object's
/// STREAM_DIR and FIELD_DIR. Shared by the whole-object [`RlogReader`] and the
/// ranged [`crate::ranged::RlogRangeReader`], so a record decoded through a
/// selective block fetch is byte-for-byte the record the whole-object reader
/// would produce.
pub(crate) fn rebuild_record(
    stream_dir: &StreamDir,
    field_dir: &FieldDir,
    block: &DecodedBlock,
    row: usize,
) -> Result<LogRecord, LogSegError> {
    let sref = u32::try_from(i64_at(block, COL_STREAM_REF, row)?)
        .map_err(|_| LogSegError::Corrupted("stream_ref range".into()))?;
    // The STREAM_DIR entry carries both the stream id and the canonical
    // resource+scope blob it was derived from, so a rebuilt record is a
    // faithful round-trip of what the writer was handed.
    let stream_entry = stream_dir
        .entries()
        .get(sref as usize)
        .ok_or_else(|| LogSegError::Corrupted("stream_ref out of range".into()))?;
    let stream_id = stream_entry.stream_id;
    let stream_attrs = stream_entry.blob.clone();
    let severity_text = str_at(block, COL_SEVERITY_TEXT, row)
        .map(string_from_bytes)
        .transpose()?
        .unwrap_or_default();
    let body = str_at(block, COL_BODY, row)
        .map(string_from_bytes)
        .transpose()?
        .unwrap_or_default();
    let trace_id = fixed_at(block, COL_TRACE_ID, row)
        .map(|v| <[u8; 16]>::try_from(v.as_slice()))
        .transpose()
        .map_err(|_| LogSegError::Corrupted("trace_id width".into()))?;
    let span_id = fixed_at(block, COL_SPAN_ID, row)
        .map(|v| <[u8; 8]>::try_from(v.as_slice()))
        .transpose()
        .map_err(|_| LogSegError::Corrupted("span_id width".into()))?;

    let mut attrs = Vec::new();
    for e in field_dir.entries() {
        if let Some(v) = get_attr_value(block, e.column_id, e.ty, row) {
            attrs.push((e.name.clone(), v));
        }
    }
    if let Some(col) = block.str_col(crate::record::COL_ATTRS_RAW)
        && let Some(Some(raw)) = col.get(row)
    {
        attrs.extend(decode_canonical_attrs(raw)?);
    }

    Ok(LogRecord {
        stream_id,
        stream_attrs,
        ts_ns: i64_at(block, COL_TS, row)?,
        observed_ts_ns: i64_at(block, COL_OBSERVED_TS, row)?,
        severity_num: i64_at(block, COL_SEVERITY_NUM, row)? as u8,
        severity_text,
        body,
        trace_id,
        span_id,
        flags: i64_at(block, COL_FLAGS, row)? as u32,
        attrs,
    })
}

fn section(footer: &LogFooter, k: u32) -> Result<&SectionDesc, LogSegError> {
    footer
        .section(k)
        .ok_or_else(|| LogSegError::Corrupted(format!("missing section {k}")))
}

/// Reads and decompresses a whole-read section, verifying its crc first.
///
/// This is the section-access path for the whole-read sections STREAM_DIR,
/// FIELD_DIR, and SKIP_IDX (docs/log-segment-format.md): slice the section's
/// stored bytes from `desc`, verify `crc32c` against `desc.crc32c`, reject an
/// `uncomp_len` over `cfg.max_uncomp_section`, then zstd-decompress or pass the
/// raw bytes through, checking the result is exactly `desc.uncomp_len` long.
/// BLOCKS and BLOOM are not whole-read sections and have their own per-block or
/// per-entry access paths; do not route them through here.
///
/// Exposed so tools (the `ravel-cli` inspector) can reconstruct a section from
/// its public [`SectionDesc`] without reimplementing the crc-and-decompress
/// discipline. [`RlogReader::new`] is the only in-crate caller.
pub fn read_section(
    bytes: &[u8],
    desc: &SectionDesc,
    cfg: &RlogConfig,
) -> Result<Vec<u8>, LogSegError> {
    let start = usize::try_from(desc.offset)
        .map_err(|_| LogSegError::Corrupted("section offset range".into()))?;
    let len = usize::try_from(desc.len)
        .map_err(|_| LogSegError::Corrupted("section len range".into()))?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| LogSegError::Corrupted("section range overflow".into()))?;
    let stored = bytes
        .get(start..end)
        .ok_or_else(|| LogSegError::Corrupted("section out of bounds".into()))?;
    decode_section(stored, desc, cfg)
}

/// The crc-and-decompress half of [`read_section`], taking a section's stored
/// bytes directly (offset 0) rather than slicing them out of a whole object.
///
/// This is what a ranged reader ([`crate::ranged::RlogRangeReader`]) uses: it
/// fetches exactly `[desc.offset, desc.offset + desc.len)` with a ranged GET,
/// so the fetched buffer *is* the stored section, and passes it here. `stored`
/// MUST be exactly `desc.len` bytes. The crc is verified against `desc.crc32c`
/// before any decompression, the `uncomp_len` is rejected above
/// `cfg.max_uncomp_section` before allocating, and the decompressed length must
/// equal `desc.uncomp_len` exactly (the same discipline [`read_section`]
/// applies to a whole-object slice).
pub fn decode_section(
    stored: &[u8],
    desc: &SectionDesc,
    cfg: &RlogConfig,
) -> Result<Vec<u8>, LogSegError> {
    if stored.len() as u64 != desc.len {
        return Err(LogSegError::Corrupted(
            "section stored length != desc.len".into(),
        ));
    }
    if crc32c::crc32c(stored) != desc.crc32c {
        return Err(LogSegError::Corrupted("section crc mismatch".into()));
    }
    if desc.uncomp_len > cfg.max_uncomp_section {
        return Err(LogSegError::Corrupted("section uncomp_len over cap".into()));
    }
    if desc.comp == COMP_ZSTD {
        let raw = zstd::bulk::decompress(stored, desc.uncomp_len as usize)
            .map_err(|e| LogSegError::Corrupted(format!("section zstd: {e}")))?;
        if raw.len() as u64 != desc.uncomp_len {
            return Err(LogSegError::Corrupted("section decompressed length".into()));
        }
        Ok(raw)
    } else {
        if stored.len() as u64 != desc.uncomp_len {
            return Err(LogSegError::Corrupted(
                "raw section length != uncomp_len".into(),
            ));
        }
        Ok(stored.to_vec())
    }
}

fn flatten<'p>(pred: &'p Predicate, out: &mut Vec<&'p Predicate>) {
    match pred {
        Predicate::And(v) => {
            for p in v {
                flatten(p, out);
            }
        }
        other => out.push(other),
    }
}

pub(crate) fn i64_at(block: &DecodedBlock, col: u32, row: usize) -> Result<i64, LogSegError> {
    block
        .i64_col(col)
        .and_then(|c| c.get(row).copied())
        .flatten()
        .ok_or_else(|| LogSegError::Corrupted(format!("missing i64 col {col}")))
}

fn str_at(block: &DecodedBlock, col: u32, row: usize) -> Option<Vec<u8>> {
    block
        .str_col(col)
        .and_then(|c| c.get(row).cloned())
        .flatten()
}

fn fixed_at(block: &DecodedBlock, col: u32, row: usize) -> Option<Vec<u8>> {
    block
        .fixed_col(col)
        .and_then(|c| c.get(row).cloned())
        .flatten()
}

fn string_from_bytes(bytes: Vec<u8>) -> Result<String, LogSegError> {
    String::from_utf8(bytes).map_err(|_| LogSegError::Corrupted("value not utf-8".into()))
}

/// The `AttrValue` for a dynamic column at a row, if present.
fn get_attr_value(block: &DecodedBlock, cid: u32, ty: FieldType, row: usize) -> Option<AttrValue> {
    match ty {
        FieldType::Str => str_at(block, cid, row)
            .and_then(|b| String::from_utf8(b).ok())
            .map(AttrValue::Str),
        FieldType::Bytes => str_at(block, cid, row).map(AttrValue::Bytes),
        FieldType::I64 => block
            .i64_col(cid)
            .and_then(|c| c.get(row).copied())
            .flatten()
            .map(AttrValue::I64),
        FieldType::F64 => block
            .f64_col(cid)
            .and_then(|c| c.get(row).copied())
            .flatten()
            .map(|bits| AttrValue::F64(f64::from_bits(bits))),
        FieldType::Bool => block
            .bool_col(cid)
            .and_then(|c| c.get(row).copied())
            .flatten()
            .map(AttrValue::Bool),
    }
}

/// Compares a stored column value at a row against a query value (bit-exact for
/// f64).
fn attr_equals(
    block: &DecodedBlock,
    cid: u32,
    ty: FieldType,
    value: &AttrValue,
    row: usize,
) -> bool {
    match get_attr_value(block, cid, ty, row) {
        Some(stored) => attr_value_eq(&stored, value),
        None => false,
    }
}

/// Value equality that treats f64 by bit pattern (so -0.0 != 0.0 and NaN
/// payloads compare exactly), and canonicalizes list/map on both sides.
fn attr_value_eq(a: &AttrValue, b: &AttrValue) -> bool {
    match (a, b) {
        (AttrValue::F64(x), AttrValue::F64(y)) => x.to_bits() == y.to_bits(),
        // A Bytes column stores a canonicalized list/map; compare a query
        // list/map by its canonical bytes too.
        (AttrValue::Bytes(x), other @ (AttrValue::List(_) | AttrValue::Map(_))) => {
            *x == crate::record::canonical_value_bytes(other)
        }
        (other @ (AttrValue::List(_) | AttrValue::Map(_)), AttrValue::Bytes(y)) => {
            crate::record::canonical_value_bytes(other) == *y
        }
        _ => a == b,
    }
}

/// Phrase/word match: `word` tokenizes to one or more query tokens; a single
/// token requires containment, multiple tokens require an in-order contiguous
/// run in the tokenized value (docs/log-segment-format.md "Tokenizer").
fn phrase_match(value: &[u8], word: &str) -> bool {
    let query = tokens(word);
    if query.is_empty() {
        return true;
    }
    let text = match std::str::from_utf8(value) {
        Ok(s) => s,
        Err(_) => return false,
    };
    let toks = tokens(text);
    toks.windows(query.len()).any(|w| w == query.as_slice())
}

/// Decodes canonical attribute bytes (the write-side [`canonical_attr_bytes`])
/// back into attributes. Used for `attrs_raw` overflow. Depth-bounded against
/// hostile nesting.
fn decode_canonical_attrs(bytes: &[u8]) -> Result<Vec<(String, AttrValue)>, LogSegError> {
    let mut pos = 0usize;
    let out = decode_attr_set(bytes, &mut pos, 0)?;
    if pos != bytes.len() {
        return Err(LogSegError::Corrupted("attrs_raw trailing bytes".into()));
    }
    Ok(out)
}

const MAX_ATTR_DEPTH: u32 = 32;

fn decode_attr_set(
    bytes: &[u8],
    pos: &mut usize,
    depth: u32,
) -> Result<Vec<(String, AttrValue)>, LogSegError> {
    if depth > MAX_ATTR_DEPTH {
        return Err(LogSegError::Corrupted("attrs_raw too deep".into()));
    }
    use crate::varint::get_uvarint;
    let count = get_uvarint(bytes, pos)?;
    if count > (1 << 20) {
        return Err(LogSegError::Corrupted("attrs_raw count over cap".into()));
    }
    let mut out = Vec::with_capacity((count as usize).min(1 << 12));
    for _ in 0..count {
        let klen = usize::try_from(get_uvarint(bytes, pos)?)
            .map_err(|_| LogSegError::Corrupted("attr key len".into()))?;
        let kend = pos
            .checked_add(klen)
            .ok_or_else(|| LogSegError::Corrupted("attr key overflow".into()))?;
        let kbytes = bytes
            .get(*pos..kend)
            .ok_or_else(|| LogSegError::Corrupted("attr key truncated".into()))?;
        let key = std::str::from_utf8(kbytes)
            .map_err(|_| LogSegError::Corrupted("attr key not utf-8".into()))?
            .to_string();
        *pos = kend;
        let value = decode_attr_value(bytes, pos, depth)?;
        out.push((key, value));
    }
    Ok(out)
}

fn decode_attr_value(bytes: &[u8], pos: &mut usize, depth: u32) -> Result<AttrValue, LogSegError> {
    use crate::varint::{get_uvarint, zigzag_decode};
    let tag = *bytes
        .get(*pos)
        .ok_or_else(|| LogSegError::Corrupted("attr tag truncated".into()))?;
    *pos += 1;
    Ok(match tag {
        1 => {
            let len = usize::try_from(get_uvarint(bytes, pos)?)
                .map_err(|_| LogSegError::Corrupted("attr str len".into()))?;
            let end = pos
                .checked_add(len)
                .ok_or_else(|| LogSegError::Corrupted("attr str overflow".into()))?;
            let s = bytes
                .get(*pos..end)
                .ok_or_else(|| LogSegError::Corrupted("attr str truncated".into()))?;
            let v = std::str::from_utf8(s)
                .map_err(|_| LogSegError::Corrupted("attr str not utf-8".into()))?
                .to_string();
            *pos = end;
            AttrValue::Str(v)
        }
        2 => AttrValue::I64(zigzag_decode(get_uvarint(bytes, pos)?)),
        3 => {
            let s = bytes
                .get(*pos..*pos + 8)
                .ok_or_else(|| LogSegError::Corrupted("attr f64 truncated".into()))?;
            let mut a = [0u8; 8];
            a.copy_from_slice(s);
            *pos += 8;
            AttrValue::F64(f64::from_bits(u64::from_le_bytes(a)))
        }
        4 => {
            let b = *bytes
                .get(*pos)
                .ok_or_else(|| LogSegError::Corrupted("attr bool truncated".into()))?;
            *pos += 1;
            AttrValue::Bool(b != 0)
        }
        5 => {
            let len = usize::try_from(get_uvarint(bytes, pos)?)
                .map_err(|_| LogSegError::Corrupted("attr bytes len".into()))?;
            let end = pos
                .checked_add(len)
                .ok_or_else(|| LogSegError::Corrupted("attr bytes overflow".into()))?;
            let b = bytes
                .get(*pos..end)
                .ok_or_else(|| LogSegError::Corrupted("attr bytes truncated".into()))?
                .to_vec();
            *pos = end;
            AttrValue::Bytes(b)
        }
        6 => {
            let n = get_uvarint(bytes, pos)?;
            if n > (1 << 20) {
                return Err(LogSegError::Corrupted("attr list over cap".into()));
            }
            let mut items = Vec::with_capacity((n as usize).min(1 << 12));
            for _ in 0..n {
                items.push(decode_attr_value(bytes, pos, depth + 1)?);
            }
            AttrValue::List(items)
        }
        7 => AttrValue::Map(decode_attr_set(bytes, pos, depth + 1)?),
        other => {
            return Err(LogSegError::Corrupted(format!("attr tag {other}")));
        }
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::record::LogRecord;
    use crate::writer::{ObjectIdentity, RlogWriter};
    use ravel_types::logstream::{AttrValue, LogStreamId};

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [0u8; 16],
            shard: 0,
            writer_id: [0u8; 16],
            writer_epoch: 0,
            writer_seq: 0,
        }
    }

    fn sid(n: u8) -> LogStreamId {
        let mut a = [0u8; 16];
        a[0] = n;
        LogStreamId(a)
    }

    fn rec(stream: u8, ts: i64, body: &str) -> LogRecord {
        LogRecord {
            stream_id: sid(stream),
            stream_attrs: crate::record::stream_attrs_bytes(
                &[(
                    "service.name".into(),
                    AttrValue::Str(format!("svc{stream}")),
                )],
                "scope",
                "1",
                &[],
            ),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: body.into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        }
    }

    fn build(cfg: RlogConfig, recs: Vec<LogRecord>) -> Vec<u8> {
        let mut w = RlogWriter::new(cfg, identity());
        for r in recs {
            w.push(r).expect("push");
        }
        w.finish().expect("finish")
    }

    #[test]
    fn read_section_standalone_valid_and_crc_mismatch() {
        use crate::footer::{COMP_NONE, SectionDesc, kind};

        // A hand-built raw (uncompressed) section: stored bytes are the section
        // payload verbatim, uncomp_len equals its length, crc32c covers it.
        let payload = b"hand-built section bytes".to_vec();
        let cfg = RlogConfig::default();
        let good = SectionDesc {
            kind: kind::STREAM_DIR,
            offset: 0,
            len: payload.len() as u64,
            crc32c: crc32c::crc32c(&payload),
            comp: COMP_NONE,
            uncomp_len: payload.len() as u64,
        };
        // Usable without an RlogReader: it takes bytes + a descriptor directly.
        let got = read_section(&payload, &good, &cfg).expect("valid section reads");
        assert_eq!(got, payload);

        // A crc that does not match the stored bytes is a loud Corrupted error,
        // before any decompression or grammar parse.
        let bad = SectionDesc {
            crc32c: good.crc32c ^ 0xFFFF_FFFF,
            ..good
        };
        let err = read_section(&payload, &bad, &cfg).expect_err("crc mismatch rejected");
        assert!(matches!(err, LogSegError::Corrupted(_)), "got {err:?}");
    }

    #[test]
    fn ts_range_prunes_to_three_blocks() {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        // 100 single-record blocks, ts 0..100.
        let recs: Vec<LogRecord> = (0..100).map(|i| rec(0, i, "msg")).collect();
        let obj = build(cfg, recs);
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let (rows, stats) = reader
            .scan(&Predicate::TsRange {
                min_ns: 40,
                max_ns: 42,
            })
            .expect("scan");
        assert_eq!(stats.blocks_total, 100);
        assert_eq!(stats.blocks_after_skip, 3);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn absent_word_prunes_all_blocks() {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let recs: Vec<LogRecord> = (0..20).map(|i| rec(0, i, "connection refused")).collect();
        let obj = build(cfg, recs);
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let (rows, stats) = reader
            .scan(&Predicate::HasWord {
                field: FieldSel::Body,
                word: "timeout".into(),
            })
            .expect("scan");
        assert_eq!(rows.len(), 0);
        assert_eq!(stats.blocks_after_bloom, 0);
        assert_eq!(stats.blocks_scanned, 0);
    }

    #[test]
    fn word_present_in_one_block() {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let mut recs: Vec<LogRecord> = (0..20).map(|i| rec(0, i, "all good")).collect();
        recs[7].body = "request timeout here".into();
        let obj = build(cfg, recs);
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let (rows, stats) = reader
            .scan(&Predicate::HasWord {
                field: FieldSel::Body,
                word: "timeout".into(),
            })
            .expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts_ns, 7);
        // Bloom leaves at most a few blocks (target FPR ~1%).
        assert!(stats.blocks_scanned <= 2);
    }

    #[test]
    fn phrase_requires_order() {
        let cfg = RlogConfig::default();
        let mut a = rec(0, 1, "connection timeout occurred");
        a.attrs.clear();
        let b = rec(0, 2, "timeout on connection");
        let obj = build(cfg, vec![a, b]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let (rows, _) = reader
            .scan(&Predicate::HasWord {
                field: FieldSel::Body,
                word: "connection timeout".into(),
            })
            .expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts_ns, 1);
    }

    #[test]
    fn stream_selection() {
        let cfg = RlogConfig::default();
        let recs = vec![
            rec(1, 1, "a"),
            rec(2, 2, "b"),
            rec(3, 3, "c"),
            rec(2, 4, "d"),
        ];
        let obj = build(cfg, recs);
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let (rows, _) = reader
            .scan(&Predicate::StreamIn(vec![sid(2)]))
            .expect("scan");
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().all(|r| r.stream_id == sid(2)));
    }

    #[test]
    fn f64_equality_is_bit_exact() {
        let cfg = RlogConfig::default();
        let mut neg = rec(0, 1, "x");
        neg.attrs.push(("v".into(), AttrValue::F64(-0.0)));
        let mut pos = rec(0, 2, "x");
        pos.attrs.push(("v".into(), AttrValue::F64(0.0)));
        let mut nan = rec(0, 3, "x");
        nan.attrs.push((
            "v".into(),
            AttrValue::F64(f64::from_bits(f64::NAN.to_bits() | 0x7)),
        ));
        let obj = build(cfg, vec![neg, pos, nan]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let (rows, _) = reader
            .scan(&Predicate::Equals {
                field: FieldSel::Attr("v".into()),
                value: AttrValue::F64(-0.0),
            })
            .expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts_ns, 1);

        let (rows, _) = reader
            .scan(&Predicate::Equals {
                field: FieldSel::Attr("v".into()),
                value: AttrValue::F64(f64::from_bits(f64::NAN.to_bits() | 0x7)),
            })
            .expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts_ns, 3);
    }

    #[test]
    fn canonical_attrs_roundtrip() {
        use ravel_types::logstream::canonical_attr_bytes;
        let attrs = vec![
            ("s".to_string(), AttrValue::Str("hi".into())),
            ("n".to_string(), AttrValue::I64(-9)),
            ("f".to_string(), AttrValue::F64(-0.0)),
            ("b".to_string(), AttrValue::Bool(true)),
            ("y".to_string(), AttrValue::Bytes(vec![1, 2, 3])),
        ];
        let bytes = canonical_attr_bytes(&attrs);
        let got = decode_canonical_attrs(&bytes).expect("decode");
        // canonical_attr_bytes sorts by (key, value); compare as sets.
        let mut a = attrs;
        let mut b = got;
        a.sort_by(|x, y| x.0.cmp(&y.0));
        b.sort_by(|x, y| x.0.cmp(&y.0));
        for ((ka, va), (kb, vb)) in a.iter().zip(b.iter()) {
            assert_eq!(ka, kb);
            assert!(attr_value_eq(va, vb));
        }
    }

    fn rec_with_svc(ts: i64, svc: &str) -> LogRecord {
        let mut r = rec(0, ts, "msg");
        r.attrs
            .push(("svc".to_string(), AttrValue::Str(svc.to_string())));
        r
    }

    fn build_indexed(cfg: RlogConfig, recs: Vec<LogRecord>, fields: &[&str]) -> Vec<u8> {
        let mut w = RlogWriter::new(cfg, identity())
            .with_indexed_fields(fields.iter().map(|s| s.to_string()).collect());
        for r in recs {
            w.push(r).expect("push");
        }
        w.finish().expect("finish")
    }

    /// The exact-postings acceptance test (issue #508): every block containing
    /// a matching record survives postings pruning (soundness), and blocks
    /// proven not to contain the term are pruned before bloom or exact scan
    /// (pruning). 12 blocks of 5 records each, `svc` constant within a block
    /// and cycling through 3 values across blocks, so a probe for one value
    /// prunes to exactly the 4 blocks that carry it.
    #[test]
    fn postings_prune_exactly_and_absent_is_legal() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        let mut recs = Vec::new();
        for i in 0..60i64 {
            let block = i / 5;
            recs.push(rec_with_svc(i, &format!("s{}", block % 3)));
        }
        let obj = build_indexed(cfg, recs, &["svc"]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        // Pruning: "s0" only appears in blocks 0,3,6,9 (4 of 12); postings
        // proves the other 8 absent without touching bloom or BLOCKS.
        let (rows, stats) = reader
            .scan(&Predicate::Equals {
                field: FieldSel::Attr("svc".into()),
                value: AttrValue::Str("s0".into()),
            })
            .expect("scan");
        assert_eq!(stats.blocks_total, 12);
        assert_eq!(stats.blocks_after_postings, 4);
        assert_eq!(stats.blocks_scanned, 4);
        // Soundness: every one of the 20 matching records is present, i.e. no
        // block that actually contains a match was pruned.
        assert_eq!(rows.len(), 20);
        assert!(rows.iter().all(|r| (r.ts_ns / 5) % 3 == 0));

        // A term proven absent everywhere prunes to zero blocks outright.
        let (rows, stats) = reader
            .scan(&Predicate::Equals {
                field: FieldSel::Attr("svc".into()),
                value: AttrValue::Str("nope".into()),
            })
            .expect("scan");
        assert_eq!(stats.blocks_after_postings, 0);
        assert_eq!(rows.len(), 0);
        assert!(!stats.postings_degraded);
    }

    #[test]
    fn no_postings_section_scans_correctly() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        let recs: Vec<LogRecord> = (0..20).map(|i| rec_with_svc(i, "s0")).collect();
        // No with_indexed_fields: object has no POSTINGS section at all.
        let obj = build(cfg, recs);
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let (rows, stats) = reader
            .scan(&Predicate::Equals {
                field: FieldSel::Attr("svc".into()),
                value: AttrValue::Str("s0".into()),
            })
            .expect("scan");
        assert_eq!(rows.len(), 20);
        assert!(!stats.postings_degraded);
        assert_eq!(stats.blocks_after_postings, stats.blocks_after_skip);
    }

    #[test]
    fn corrupt_postings_section_degrades_to_exact_scan() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        let mut recs = Vec::new();
        for i in 0..60i64 {
            let block = i / 5;
            recs.push(rec_with_svc(i, &format!("s{}", block % 3)));
        }
        let mut obj = build_indexed(cfg, recs, &["svc"]);

        let footer = crate::footer::open(&obj).expect("open footer");
        let desc = *footer
            .section(crate::footer::kind::POSTINGS)
            .expect("postings section present");
        let at = desc.offset as usize;
        obj[at] ^= 0xFF; // corrupt the POSTINGS version byte

        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let (rows, stats) = reader
            .scan(&Predicate::Equals {
                field: FieldSel::Attr("svc".into()),
                value: AttrValue::Str("s0".into()),
            })
            .expect("scan degrades, not errors");
        assert!(stats.postings_degraded);
        // Falls back to bloom + exact scan: still the exact right answer.
        assert_eq!(rows.len(), 20);
        assert!(rows.iter().all(|r| (r.ts_ns / 5) % 3 == 0));
    }

    /// End-to-end reproduction (fix-task on epic #479, issue #508 follow-up):
    /// a one-byte flip of a POSTINGS sparse-index `first_term`, corrupting no
    /// term block, used to reach `scan` as `postings_degraded == false` and a
    /// silently narrowed (wrong) result. Four terms `aa, bz, ca, cz` with
    /// `postings_stride: 2` (one per its own physical block, so a probe hit
    /// is exactly one row) split into term blocks `B0 = [aa, bz]` and
    /// `B1 = [ca, cz]`; flipping `B1`'s declared `first_term` from `"ca"` to
    /// `"ba"` preserves ascending order and every term-block crc, so before
    /// this fix `RlogReader::new` and `PostingsSection::parse` both accepted
    /// it and a probe for `"bz"` landed on `B1`, missed, and reported the
    /// term absent -- baseline 1 row, mutated 0 rows, no error, no counter.
    /// The whole-section `crc32c` this fix now verifies before `parse`
    /// catches the flip regardless of which term is probed, degrading to
    /// bloom + exact scan instead.
    #[test]
    fn corrupted_first_term_header_byte_degrades_instead_of_narrowing() {
        let cfg = RlogConfig {
            block_target_records: 1,
            postings_stride: 2,
            ..RlogConfig::default()
        };
        let recs = vec![
            rec_with_svc(0, "aa"),
            rec_with_svc(1, "bz"),
            rec_with_svc(2, "ca"),
            rec_with_svc(3, "cz"),
        ];
        let mut obj = build_indexed(cfg, recs, &["svc"]);

        let pred = Predicate::Equals {
            field: FieldSel::Attr("svc".into()),
            value: AttrValue::Str("bz".into()),
        };

        let baseline_reader = RlogReader::new(&obj, &cfg).expect("open");
        let (rows, stats) = baseline_reader.scan(&pred).expect("baseline scan");
        assert_eq!(rows.len(), 1, "baseline: exactly the one \"bz\" row");
        assert!(!stats.postings_degraded);

        let footer = crate::footer::open(&obj).expect("open footer");
        let desc = *footer
            .section(crate::footer::kind::POSTINGS)
            .expect("postings section present");
        // Same header-layout offset as postings::tests:
        // corrupted_first_term_header_byte_is_caught_not_silently_narrowed --
        // B1's declared first_term "ca" at [42, 44) relative to the section.
        let corrupt_at = desc.offset as usize + 42;
        assert_eq!(&obj[corrupt_at..corrupt_at + 2], b"ca");
        obj[corrupt_at] = b'b';

        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let (rows, stats) = reader.scan(&pred).expect("scan degrades, not errors");
        assert!(
            stats.postings_degraded,
            "the whole-section crc must catch the corrupted header"
        );
        assert_eq!(
            rows.len(),
            1,
            "degraded pruning must fall back to bloom + exact scan, never silently drop the row"
        );
    }
}
