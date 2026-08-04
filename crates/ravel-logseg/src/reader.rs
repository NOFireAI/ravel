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
use crate::postings::{POSTINGS_VERSION_V1, PostingsSection, term_key};
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

    /// Scans the object for records matching `pred`, evaluated exactly per row.
    ///
    /// Equivalent to [`RlogReader::scan_pruned`] with an empty prune channel.
    pub fn scan(&self, pred: &Predicate) -> Result<(Vec<LogRecord>, ScanStats), LogSegError> {
        self.scan_pruned(pred, &[])
    }

    /// Scans for records matching `content` exactly, with `prune` an additional
    /// prune-only predicate channel.
    ///
    /// `content` is the sole exact filter: every surviving block's rows are
    /// re-evaluated against it, exactly as [`RlogReader::scan`] does, and only
    /// matches are returned. `prune` never contributes to that per-row filter.
    /// Its arms drive POSTINGS block pruning and nothing else: an `Equals` on an
    /// indexed attribute field intersects the candidate blocks with that term's
    /// exact block list, and an arm whose field the POSTINGS index does not
    /// cover contributes no pruning at all (the `Ok(None)` path). Postings are
    /// exact sets, never sketches, so this channel only ever drops blocks proven
    /// to hold no record carrying the term; it is widen-only by construction
    /// (docs/adrs/0013, docs/adrs/0049 decision 7).
    ///
    /// For a version 1 object one more arm is declined: a key that also appears
    /// at resource or scope level anywhere in this object. A v1 POSTINGS list
    /// indexes the per-record layer only, so an exact index over one layer
    /// cannot prune a merged-view query that spans two. A version 2 object
    /// indexes the merged view directly, so no arm is declined. See
    /// [`RlogReader::prune_postings_arms`].
    ///
    /// The separation is deliberate and load-bearing. A `prune` `Equals` on
    /// `FieldSel::Attr` resolves against a record's own dynamic column and
    /// `attrs_raw` overflow only (see [`RlogReader::equals`]), never against
    /// resource or scope stream attributes, so the per-record equality is a
    /// strict subset of a merged-view SQL equality (`ravel_sql::rlog_attrs`).
    /// Evaluating it per row would silently drop a record whose match lives only
    /// in its resource or scope attributes; driving only block pruning does not.
    /// The exact residual over the merged view stays the SQL layer's job.
    pub fn scan_pruned(
        &self,
        content: &Predicate,
        prune: &[Predicate],
    ) -> Result<(Vec<LogRecord>, ScanStats), LogSegError> {
        let mut stats = ScanStats {
            blocks_total: self.skip.l0.len() as u32,
            ..ScanStats::default()
        };

        // Collect the And-flattened arms of the exact `content` predicate.
        let mut arms: Vec<&Predicate> = Vec::new();
        flatten(content, &mut arms);

        // Prune-only arms: flattened. They feed POSTINGS pruning below and
        // nothing else -- never the ts/stream/bloom bounds, never per-row eval.
        let mut prune_arms: Vec<&Predicate> = Vec::new();
        for p in prune {
            flatten(p, &mut prune_arms);
        }

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
            // Both the exact `content` Equals arms and the prune-only arms drive
            // postings pruning; an unindexed or capped field on either falls
            // through to bloom + exact scan via the `Ok(None)` path below.
            //
            // The prune-only arms need the object's POSTINGS version to know
            // whether the stream-attribute exclusion applies (version 1) or not
            // (version 2, whose lists already index the merged view). Building
            // the version-2 arm set (no exclusion, hence a superset of the
            // version-1 set) first tells us whether any postings work exists at
            // all, so a query with no eligible arm still skips the section
            // exactly as before.
            let content_arms = self.postings_arms(&arms);
            let prune_arms_no_exclusion = self.prune_postings_arms(&prune_arms, false);
            if !content_arms.is_empty() || !prune_arms_no_exclusion.is_empty() {
                match self
                    .postings_section_verified(desc)
                    .and_then(PostingsSection::parse)
                {
                    Ok(section) => {
                        let prune_arms = if section.version() == POSTINGS_VERSION_V1 {
                            self.prune_postings_arms(&prune_arms, true)
                        } else {
                            prune_arms_no_exclusion
                        };
                        let mut postings_arms = content_arms;
                        postings_arms.extend(prune_arms);
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
                if self.eval(content, &decoded, row)? {
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

    /// Prune-only postings arms for a merged-view query.
    ///
    /// The soundness question is whether an exact posting list can prune a
    /// query over the merged attribute view (`ravel_sql::rlog_attrs`: the union
    /// of a record's resource, scope, and per-record attributes, the record
    /// winning on a key collision). The answer depends on what the object's
    /// POSTINGS lists index, which its grammar version records:
    ///
    /// - `apply_stream_exclusion == false` (a version 2 object): the lists
    ///   index the merged view already, so probing a term is sound for every
    ///   arm. No arm is dropped.
    /// - `apply_stream_exclusion == true` (a version 1 object): the lists index
    ///   the per-record layer only. `field_dir` is object-wide, so one record
    ///   carrying a key per-record makes it an indexed column for the whole
    ///   object, including records whose value for that key lives in their
    ///   resource blob; those records are in no posting list, so probing the
    ///   term would prune their block away. An exact index over one layer
    ///   cannot prune a union over two, so when the key appears at stream level
    ///   anywhere in this object this declines to prune it. Declining is
    ///   widen-only (ADR-0013), which pruning wrongly is not.
    ///
    /// Only the prune channel needs the exclusion. A `content` arm is the
    /// caller's own exact per-record predicate ([`RlogReader::postings_arms`]),
    /// so pruning it to the records that carry the term per-record is precisely
    /// right regardless of version.
    fn prune_postings_arms(
        &self,
        arms: &[&Predicate],
        apply_stream_exclusion: bool,
    ) -> Vec<(u32, Vec<u8>)> {
        let mut out = Vec::new();
        for a in arms {
            if let Predicate::Equals {
                field: FieldSel::Attr(name),
                value,
            } = a
            {
                if apply_stream_exclusion && self.name_in_stream_attrs(name) {
                    continue;
                }
                let (ty, cv) = resolve_value(value);
                if let Some(entry) = self.field_dir.column(name, ty) {
                    out.push((entry.column_id, term_key(&cv)));
                }
            }
        }
        out
    }

    /// Whether `name` appears as a resource or scope attribute of any stream in
    /// this object. A blob that fails to decode counts as "present", so a
    /// corrupt STREAM_DIR declines to prune rather than pruning on a partial
    /// read.
    fn name_in_stream_attrs(&self, name: &str) -> bool {
        self.stream_dir
            .entries()
            .iter()
            .any(|e| match stream_attr_names(&e.blob) {
                Ok(names) => names.iter().any(|n| n == name),
                Err(_) => true,
            })
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
/// The resource and scope attribute `(name, value)` pairs a STREAM_DIR blob
/// carries, in blob order (the resource set first, then the scope set). The
/// blob layout is `canonical_attr_bytes(resource) || len+scope_name ||
/// len+scope_version || canonical_attr_bytes(scope)` (see
/// [`crate::record::stream_attrs_bytes`]); the two length-prefixed scope
/// strings are positional, not key-value entries, so they are skipped over.
///
/// Shared by [`stream_attr_names`] (which drops the values) and the writer's
/// merged-view POSTINGS accumulation (which keeps them, to index the union of
/// a record's stream and per-record attributes). Kept in this crate so the
/// merge does not depend on `ravel-sql`
/// (docs/adrs/0049-rlog-postings.md amendment 2026-08-03).
pub(crate) fn stream_attr_pairs(blob: &[u8]) -> Result<Vec<(String, AttrValue)>, LogSegError> {
    use crate::varint::get_uvarint;
    let mut pos = 0usize;
    let mut pairs = decode_attr_set(blob, &mut pos, 0)?;
    for _ in 0..2 {
        let len = get_uvarint(blob, &mut pos)?;
        let len = usize::try_from(len)
            .map_err(|_| LogSegError::Corrupted("stream_attrs scope string len".into()))?;
        pos = pos
            .checked_add(len)
            .filter(|p| *p <= blob.len())
            .ok_or_else(|| LogSegError::Corrupted("stream_attrs scope string".into()))?;
    }
    pairs.extend(decode_attr_set(blob, &mut pos, 0)?);
    Ok(pairs)
}

/// The attribute names a STREAM_DIR blob carries, at resource and scope level.
fn stream_attr_names(blob: &[u8]) -> Result<Vec<String>, LogSegError> {
    Ok(stream_attr_pairs(blob)?
        .into_iter()
        .map(|(k, _)| k)
        .collect())
}

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

    /// Soundness (issue #538): a prune equality whose per-record resolution does
    /// NOT match, on a record whose match lives only in its resource/scope
    /// stream attributes, must still return that record. The prune-only channel
    /// drives block pruning alone, so a `service.name` that is a resource
    /// attribute (not a per-record column) resolves to no postings arm and
    /// prunes nothing. Wiring the same equality into `content` -- the exact
    /// per-row filter -- drops the record, because `equals(FieldSel::Attr(..))`
    /// reads only the per-record column plus `attrs_raw`, never the
    /// resource/scope blob. This test proves the two channels diverge there.
    #[test]
    fn prune_channel_keeps_resource_only_match_that_content_would_drop() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        // 20 records on stream 0: resource `service.name = "svc0"` for every
        // record (see `rec`), each also carrying an indexed per-record `svc`.
        let recs: Vec<LogRecord> = (0..20).map(|i| rec_with_svc(i, "s0")).collect();
        let obj = build_indexed(cfg, recs, &["svc"]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        // service.name lives only in the resource stream attrs; it is not a
        // per-record column, so the prune arm resolves to no postings entry and
        // prunes nothing. content is match-all, so every record is returned.
        let prune = [Predicate::Equals {
            field: FieldSel::Attr("service.name".into()),
            value: AttrValue::Str("svc0".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        assert_eq!(rows.len(), 20, "resource-only match must survive the prune");
        assert_eq!(stats.blocks_after_postings, stats.blocks_after_skip);

        // The same equality wired into `content` drops every record: `equals`
        // never reads the resource blob. This is the unsound coupling the
        // prune-only channel exists to avoid.
        let (wrong, _) = reader
            .scan(&Predicate::Equals {
                field: FieldSel::Attr("service.name".into()),
                value: AttrValue::Str("svc0".into()),
            })
            .expect("scan");
        assert_eq!(
            wrong.len(),
            0,
            "content wiring drops the resource-only match (the bug #538 fixes)"
        );
    }

    /// A key that is an indexed per-record column for SOME records and a
    /// resource attribute for OTHERS must not let the prune drop a record the
    /// query needs.
    ///
    /// `field_dir` is object-wide, so one record carrying `service.name` as a
    /// per-record attribute makes it an indexed column for the whole object.
    /// A record whose `service.name` comes from its resource blob has no value
    /// in that column, so it is in no posting list, so probing the term would
    /// prune its block. The merged SQL view
    /// (`ravel_sql::rlog_attrs::merged_attrs`) is the union of both layers and
    /// needs that record. This is the same union-over-two-layers problem #510
    /// refused to ship, relocated into the prune channel.
    ///
    /// Under version 2 the writer indexes the merged view, so the resource-only
    /// records ARE in the `"svc0"` posting list and the prune keeps their block
    /// rather than dropping it: `prune_postings_arms` no longer needs the
    /// stream-attribute exclusion (that is now version-1-only). Every record the
    /// merged view matches is returned. See
    /// [`super::tests::version_1_object_declines_to_prune_key_at_stream_level`]
    /// for the conservative version-1 behaviour this replaces.
    #[test]
    fn prune_must_not_drop_a_resource_only_match_when_the_key_is_also_a_column() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        let mut recs = Vec::new();
        // Block 0: stream 0, resource `service.name = "svc0"`, no per-record
        // `service.name`. These are the records the merged view must return.
        for i in 0..5i64 {
            recs.push(rec(0, i, "resource-only"));
        }
        // Block 1: stream 1, resource `service.name = "svc1"`, but each record
        // carries a per-record `service.name = "svc0"`. This is what makes
        // `service.name` an indexed column for the whole object.
        for i in 5..10i64 {
            let mut r = rec(1, i, "per-record");
            r.attrs.push((
                "service.name".to_string(),
                AttrValue::Str("svc0".to_string()),
            ));
            recs.push(r);
        }
        let obj = build_indexed(cfg, recs, &["service.name"]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let prune = [Predicate::Equals {
            field: FieldSel::Attr("service.name".into()),
            value: AttrValue::Str("svc0".into()),
        }];
        let (rows, _stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");

        // Ten records match `attrs['service.name'] = 'svc0'` on the merged
        // view: five by resource attribute, five by per-record attribute.
        assert_eq!(
            rows.len(),
            10,
            "the prune dropped the block whose match lives only in the resource blob"
        );
    }

    /// The prune-only channel actually skips blocks. 12 blocks of 5 records, an
    /// indexed per-record `svc` cycling through 3 values, so a prune for one
    /// value proves the other 8 blocks absent through POSTINGS. Asserted on
    /// ScanStats, not on the rows, so the test proves pruning happened rather
    /// than merely that the answer is right.
    #[test]
    fn prune_channel_skips_blocks_via_postings() {
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

        let prune = [Predicate::Equals {
            field: FieldSel::Attr("svc".into()),
            value: AttrValue::Str("s0".into()),
        }];
        let (_rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        // "s0" occupies blocks 0,3,6,9 -- 4 of 12; postings proves the other 8
        // absent before bloom or BLOCKS. A 3:1 block-prune ratio.
        assert_eq!(stats.blocks_total, 12);
        assert_eq!(stats.blocks_after_skip, 12);
        assert_eq!(stats.blocks_after_postings, 4);
        assert_eq!(stats.blocks_scanned, 4);
        assert!(!stats.postings_degraded);
    }

    /// An attribute the POSTINGS index does not cover prunes nothing. `region`
    /// is a real per-record column but is not in the indexed-field list, so it
    /// has no POSTINGS entry: the probe reports "not indexed" (`Ok(None)`) and
    /// no block is dropped, so every matching record is returned.
    #[test]
    fn prune_channel_uncovered_field_prunes_nothing() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        let mut recs = Vec::new();
        for i in 0..20i64 {
            let mut r = rec(0, i, "msg");
            r.attrs.push(("region".into(), AttrValue::Str("us".into())));
            recs.push(r);
        }
        // Index "svc" (absent here), never "region": region has a column but no
        // postings entry, so a probe on it prunes nothing.
        let obj = build_indexed(cfg, recs, &["svc"]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let prune = [Predicate::Equals {
            field: FieldSel::Attr("region".into()),
            value: AttrValue::Str("us".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        assert_eq!(
            rows.len(),
            20,
            "an uncovered field must not prune any match"
        );
        assert_eq!(stats.blocks_after_postings, stats.blocks_after_skip);
        assert!(!stats.postings_degraded);
    }

    /// A record on stream `stream` whose resource `service.name` is
    /// `resource_svc` (overriding [`rec`]'s default), for building objects where
    /// a key lives at resource level on one stream and per-record on another.
    fn rec_res(stream: u8, ts: i64, resource_svc: &str) -> LogRecord {
        let mut r = rec(stream, ts, "msg");
        r.stream_attrs = crate::record::stream_attrs_bytes(
            &[("service.name".into(), AttrValue::Str(resource_svc.into()))],
            "scope",
            "1",
            &[],
        );
        r
    }

    /// Like [`rec_res`] but with an explicit scope name, so two records can
    /// share a resource `service.name` yet land in distinct streams (distinct
    /// `stream_attrs` blobs, hence distinct blocks). Used to place a
    /// resource-level-only key's matching records in more than one block.
    fn rec_res_scope(stream: u8, ts: i64, resource_svc: &str, scope: &str) -> LogRecord {
        let mut r = rec(stream, ts, "msg");
        r.stream_attrs = crate::record::stream_attrs_bytes(
            &[("service.name".into(), AttrValue::Str(resource_svc.into()))],
            scope,
            "1",
            &[],
        );
        r
    }

    /// Rewrites a freshly written (version 2) object's POSTINGS section version
    /// byte to 1 and fixes both the section crc and the footer crc, producing a
    /// structurally valid version-1 object for the reader to exercise the
    /// conservative path against. Used to build a v1 fixture without a v1
    /// writer, exactly as the format-change task specifies (pin the version byte
    /// in a fixture).
    fn downgrade_postings_to_v1(obj: &[u8]) -> Vec<u8> {
        use crate::footer::{TRAILER_LEN, kind, open, write_footer_and_trailer};
        let footer = open(obj).expect("open");
        let postings = *footer
            .section(kind::POSTINGS)
            .expect("object has a POSTINGS section");
        let n = obj.len();
        let footer_len =
            u32::from_le_bytes([obj[n - 16], obj[n - 15], obj[n - 14], obj[n - 13]]) as usize;
        let footer_start = n - TRAILER_LEN - footer_len;
        let mut body = obj[..footer_start].to_vec();

        let voff = postings.offset as usize;
        assert_eq!(
            body[voff],
            crate::postings::POSTINGS_VERSION,
            "fixture must start as a version 2 object"
        );
        body[voff] = crate::postings::POSTINGS_VERSION_V1;
        let new_crc = crc32c::crc32c(&body[voff..voff + postings.len as usize]);

        let mut footer = footer;
        for s in &mut footer.sections {
            if s.kind == kind::POSTINGS {
                s.crc32c = new_crc;
            }
        }
        write_footer_and_trailer(&mut body, &footer);
        body
    }

    /// The soundness case (issue #547): a key that is a resource attribute on
    /// one stream and a per-record attribute on another. The version 2 writer
    /// indexes the merged view, so every record the merged view matches is
    /// returned AND a block holding no match is actually pruned. Asserted on
    /// `ScanStats`, not only on the rows.
    ///
    /// Three streams, one block each: block 0 has `service.name = "svc0"` as a
    /// resource attribute only; block 1 has resource `service.name = "other"`
    /// but each record overrides it per-record to `"svc0"`; block 2 has resource
    /// `service.name = "svc2"` only. A merged-view prune for `"svc0"` must keep
    /// blocks 0 and 1 (ten records) and drop block 2.
    #[test]
    fn merged_view_prune_returns_every_match_and_skips_nonmatching_blocks() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        let mut recs = Vec::new();
        // Block 0: stream 0, resource service.name = "svc0", no per-record.
        for i in 0..5i64 {
            recs.push(rec_res(0, i, "svc0"));
        }
        // Block 1: stream 1, resource service.name = "other", per-record
        // service.name = "svc0" (this is what creates the object-wide column).
        for i in 5..10i64 {
            let mut r = rec_res(1, i, "other");
            r.attrs.push((
                "service.name".to_string(),
                AttrValue::Str("svc0".to_string()),
            ));
            recs.push(r);
        }
        // Block 2: stream 2, resource service.name = "svc2", no per-record.
        for i in 10..15i64 {
            recs.push(rec_res(2, i, "svc2"));
        }
        let obj = build_indexed(cfg, recs, &["service.name"]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let prune = [Predicate::Equals {
            field: FieldSel::Attr("service.name".into()),
            value: AttrValue::Str("svc0".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");

        // Soundness: all ten merged-view matches returned (five resource-only,
        // five per-record).
        assert_eq!(rows.len(), 10);
        // Pruning: block 2 (svc2) is proven absent and skipped, not scanned.
        assert_eq!(stats.blocks_total, 3);
        assert_eq!(stats.blocks_after_skip, 3);
        assert_eq!(stats.blocks_after_postings, 2);
        assert_eq!(stats.blocks_scanned, 2);
        assert!(!stats.postings_degraded);
    }

    /// Record-wins precedence (issue #547): a key present at resource level and
    /// per-record on the same record indexes the record's value. A probe for the
    /// record's value matches the block; a probe for the (overridden) resource
    /// value prunes it away.
    #[test]
    fn record_value_wins_over_resource_value_in_postings() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        // One stream, one block: resource service.name = "svcR", every record
        // overrides it per-record to "svcP".
        let mut recs = Vec::new();
        for i in 0..5i64 {
            let mut r = rec_res(0, i, "svcR");
            r.attrs.push((
                "service.name".to_string(),
                AttrValue::Str("svcP".to_string()),
            ));
            recs.push(r);
        }
        let obj = build_indexed(cfg, recs, &["service.name"]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        // The record's own value is indexed: its block survives.
        let prune_record = [Predicate::Equals {
            field: FieldSel::Attr("service.name".into()),
            value: AttrValue::Str("svcP".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune_record)
            .expect("scan");
        assert_eq!(rows.len(), 5);
        assert_eq!(stats.blocks_after_postings, stats.blocks_after_skip);

        // The overridden resource value is NOT indexed for these records: the
        // term is absent, so the exact posting list prunes the block to zero.
        let prune_resource = [Predicate::Equals {
            field: FieldSel::Attr("service.name".into()),
            value: AttrValue::Str("svcR".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune_resource)
            .expect("scan");
        assert_eq!(
            rows.len(),
            0,
            "the overridden resource value must not match"
        );
        assert_eq!(stats.blocks_after_postings, 0);
        assert!(!stats.postings_degraded);
    }

    /// A version 1 object still reads and still prunes: for a key that is NOT at
    /// stream level, the conservative rule does not fire, so the per-record
    /// index prunes exactly as it did before the amendment. Built by pinning the
    /// version byte in a fixture.
    #[test]
    fn version_1_object_prunes_key_not_at_stream_level() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        // `svc` is per-record only (never a resource/scope attribute), 12 blocks
        // cycling 3 values, so a probe for one value prunes to 4 blocks. For
        // this key the v1 and v2 posting lists are identical, so downgrading the
        // version byte faithfully models a real v1 object.
        let mut recs = Vec::new();
        for i in 0..60i64 {
            let block = i / 5;
            recs.push(rec_with_svc(i, &format!("s{}", block % 3)));
        }
        let obj = downgrade_postings_to_v1(&build_indexed(cfg, recs, &["svc"]));
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let prune = [Predicate::Equals {
            field: FieldSel::Attr("svc".into()),
            value: AttrValue::Str("s0".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        assert_eq!(stats.blocks_total, 12);
        assert_eq!(stats.blocks_after_postings, 4);
        assert_eq!(stats.blocks_scanned, 4);
        assert!(!stats.postings_degraded);
        // Reads back the pruned rows correctly.
        assert!(rows.iter().all(|r| (r.ts_ns / 5) % 3 == 0));
    }

    /// A version 1 object declines to prune a key that appears at stream level:
    /// its posting list indexes the per-record layer only, so an exact index
    /// over one layer cannot prune a merged-view query over two. Every record
    /// the merged view matches is returned, and no block is pruned. This is the
    /// conservative behaviour version 2 replaces
    /// (see [`prune_must_not_drop_a_resource_only_match_when_the_key_is_also_a_column`]).
    #[test]
    fn version_1_object_declines_to_prune_key_at_stream_level() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        let mut recs = Vec::new();
        // Block 0: resource service.name = "svc0", no per-record.
        for i in 0..5i64 {
            recs.push(rec_res(0, i, "svc0"));
        }
        // Block 1: resource service.name = "other", per-record svc0 (creates the
        // object-wide column).
        for i in 5..10i64 {
            let mut r = rec_res(1, i, "other");
            r.attrs.push((
                "service.name".to_string(),
                AttrValue::Str("svc0".to_string()),
            ));
            recs.push(r);
        }
        let obj = downgrade_postings_to_v1(&build_indexed(cfg, recs, &["service.name"]));
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let prune = [Predicate::Equals {
            field: FieldSel::Attr("service.name".into()),
            value: AttrValue::Str("svc0".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        // Conservative rule: the key is at stream level, so v1 declines to prune.
        assert_eq!(rows.len(), 10, "every merged-view match must survive");
        assert_eq!(stats.blocks_after_postings, stats.blocks_after_skip);
        assert!(!stats.postings_degraded);
    }

    /// The per-field distinct-value cap now counts merged values
    /// (docs/adrs/0049-rlog-postings.md amendment, decision 4). Five streams,
    /// each with a distinct resource `service.name`, and a cap of 2: the field
    /// is dropped and the counter fires. Under the old per-record-only counting
    /// only one distinct value (the single per-record `service.name` that
    /// creates the column) would have been seen, well under the cap, so this
    /// object is exactly the one the amendment changes.
    #[test]
    fn distinct_value_cap_counts_merged_values() {
        let cfg = RlogConfig {
            block_target_records: 1,
            postings_max_distinct: 2,
            ..RlogConfig::default()
        };
        let mut recs = Vec::new();
        for i in 0..5u8 {
            // rec(i, ..) sets resource service.name = "svc{i}", five distinct.
            let mut r = rec(i, i64::from(i), "msg");
            if i == 0 {
                // One per-record service.name so the (service.name, Str) column
                // exists; its value equals the resource value, so the merged
                // distinct count is driven entirely by the resource attributes.
                r.attrs.push((
                    "service.name".to_string(),
                    AttrValue::Str("svc0".to_string()),
                ));
            }
            recs.push(r);
        }
        let mut w =
            RlogWriter::new(cfg, identity()).with_indexed_fields(vec!["service.name".to_string()]);
        for r in recs {
            w.push(r).expect("push");
        }
        let (obj, stats) = w.finish_with_stats().expect("finish");
        assert_eq!(
            stats.postings_capped_fields, 1,
            "five distinct merged values must exceed the cap of 2"
        );

        // The capped field reads back as not-indexed (probe None), never a
        // narrowed result.
        let footer = crate::footer::open(&obj).expect("open");
        let fd_desc = footer
            .section(crate::footer::kind::FIELD_DIR)
            .expect("field_dir");
        let start = fd_desc.offset as usize;
        let end = start + fd_desc.len as usize;
        let raw = zstd::bulk::decompress(&obj[start..end], fd_desc.uncomp_len as usize)
            .expect("decompress");
        let fd = FieldDir::decode(&raw, 1 << 20).expect("decode");
        let cid = fd
            .column("service.name", FieldType::Str)
            .expect("column exists")
            .column_id;
        let pd_desc = footer
            .section(crate::footer::kind::POSTINGS)
            .expect("postings present");
        let pd_bytes = &obj[pd_desc.offset as usize..(pd_desc.offset + pd_desc.len) as usize];
        let section = crate::postings::PostingsSection::parse(pd_bytes).expect("parse");
        assert_eq!(section.probe(cid, b"svc0").expect("probe"), None);
    }

    /// Issue #552: a key that is resource-level on EVERY record and per-record
    /// on none. Before this change it had no FIELD_DIR column, so no posting
    /// list, so a merged-view prune for it skipped nothing -- the ordinary
    /// single/few-service OTLP deployment the ADR-0049 amendment was written
    /// for. Now the writer gives it a stream-level-only column, so the prune
    /// proves non-matching blocks absent AND every matching record survives.
    ///
    /// Three streams, one block each: resource `service.name` = svc0 (block 0),
    /// svc1 (block 1), svc0 again on a different stream (block 2). A prune for
    /// svc0 must keep blocks 0 and 2 (ten records, in two non-adjacent blocks,
    /// so completeness is non-trivial) and drop block 1. No record carries
    /// `service.name` per-record.
    #[test]
    fn resource_only_key_prunes_blocks_and_returns_every_match() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        let mut recs = Vec::new();
        for i in 0..5i64 {
            recs.push(rec_res_scope(0, i, "svc0", "sa"));
        }
        for i in 5..10i64 {
            recs.push(rec_res_scope(1, i, "svc1", "sb"));
        }
        for i in 10..15i64 {
            recs.push(rec_res_scope(2, i, "svc0", "sc"));
        }
        let obj = build_indexed(cfg, recs, &["service.name"]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let prune = [Predicate::Equals {
            field: FieldSel::Attr("service.name".into()),
            value: AttrValue::Str("svc0".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        // Pruning: block 1 (svc1) is proven absent through POSTINGS and skipped.
        assert_eq!(stats.blocks_total, 3);
        assert_eq!(stats.blocks_after_skip, 3);
        assert_eq!(stats.blocks_after_postings, 2);
        assert_eq!(stats.blocks_scanned, 2);
        // Soundness: every merged-view svc0 match returned, across both blocks.
        assert_eq!(rows.len(), 10);
        assert!(!stats.postings_degraded);
    }

    /// Issue #552 soundness: adding a stream-level-only column must NOT turn
    /// `equals` (the exact per-record channel) into a merged-view match. A
    /// record whose `service.name` comes solely from its resource blob must not
    /// match a `content` equals for it, because the column carries no per-record
    /// value (it is a POSTINGS key, all-null in every block). This is the exact
    /// mistake the spec warns against: if the writer materialized the resource
    /// value into every row, this would match and the prune/exact channels would
    /// no longer be a sound subset relationship.
    #[test]
    fn equals_on_resource_only_key_stays_a_per_record_predicate() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        // Every record: resource service.name = svc0, no per-record attribute.
        let recs: Vec<LogRecord> = (0..10).map(|i| rec_res_scope(0, i, "svc0", "sa")).collect();
        let obj = build_indexed(cfg, recs, &["service.name"]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        // The stream-level column now exists, so a prune would match; but
        // `content` reads the per-record layer only, which is empty here.
        let (rows, _stats) = reader
            .scan(&Predicate::Equals {
                field: FieldSel::Attr("service.name".into()),
                value: AttrValue::Str("svc0".into()),
            })
            .expect("scan");
        assert_eq!(
            rows.len(),
            0,
            "a resource-only value must not match a content equals: equals is per-record"
        );

        // The rebuilt records must not have gained a phantom per-record
        // service.name attribute from the all-null column either.
        let (all, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan all");
        assert_eq!(all.len(), 10);
        assert!(
            all.iter()
                .all(|r| !r.attrs.iter().any(|(k, _)| k == "service.name")),
            "the stream-level column must not materialize a per-record attribute"
        );
    }

    /// Issue #552 under version 1: a resource-level-only key now has a column
    /// and a posting list, but a v1 posting list indexes the per-record layer
    /// only, so the conservative rule must decline to prune it (the key is at
    /// stream level). Every merged-view match still reads back; no block is
    /// pruned. Built by pinning the version byte in a fixture.
    #[test]
    fn version_1_object_declines_to_prune_resource_only_key() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        let mut recs = Vec::new();
        for i in 0..5i64 {
            recs.push(rec_res_scope(0, i, "svc0", "sa"));
        }
        for i in 5..10i64 {
            recs.push(rec_res_scope(1, i, "svc1", "sb"));
        }
        for i in 10..15i64 {
            recs.push(rec_res_scope(2, i, "svc0", "sc"));
        }
        let obj = downgrade_postings_to_v1(&build_indexed(cfg, recs, &["service.name"]));
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let prune = [Predicate::Equals {
            field: FieldSel::Attr("service.name".into()),
            value: AttrValue::Str("svc0".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        // service.name is at stream level, so v1 declines to prune: all blocks
        // survive and every record reads back.
        assert_eq!(rows.len(), 15, "v1 conservative rule keeps every record");
        assert_eq!(stats.blocks_after_postings, stats.blocks_after_skip);
        assert!(!stats.postings_degraded);
    }

    /// A truncated POSTINGS section on an object whose only indexed field is
    /// resource-level (the issue #552 shape) still degrades to a typed error
    /// path, never a panic: the scan falls back to bloom + exact scan and sets
    /// `postings_degraded`.
    #[test]
    fn corrupt_postings_on_resource_only_key_degrades_not_panics() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        let mut recs = Vec::new();
        for i in 0..5i64 {
            recs.push(rec_res_scope(0, i, "svc0", "sa"));
        }
        for i in 5..10i64 {
            recs.push(rec_res_scope(1, i, "svc1", "sb"));
        }
        let mut obj = build_indexed(cfg, recs, &["service.name"]);
        let footer = crate::footer::open(&obj).expect("open footer");
        let desc = *footer
            .section(crate::footer::kind::POSTINGS)
            .expect("postings present");
        // Flip the version byte: the whole-section crc no longer matches, so the
        // reader degrades this arm instead of pruning on corrupt bytes.
        obj[desc.offset as usize] ^= 0xFF;

        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let prune = [Predicate::Equals {
            field: FieldSel::Attr("service.name".into()),
            value: AttrValue::Str("svc0".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan degrades, not errors");
        assert!(stats.postings_degraded);
        // No pruning applied, so every record survives.
        assert_eq!(rows.len(), 10);
    }
}
