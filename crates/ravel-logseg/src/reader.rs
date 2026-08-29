//! Pruned scan reader (docs/log-segment-format.md "Pruning soundness").
//!
//! [`RlogReader::scan`] resolves a predicate to coarse ts/stream bounds, prunes
//! blocks through the skip index and per-block blooms, then reads, verifies, and
//! re-evaluates the survivors exactly. Pruning is proof-based: a block is
//! dropped only when the skip index proves its bounds disjoint or a bloom proves
//! a required word absent. A corrupt BLOOM section degrades to no bloom pruning
//! (scan the skip survivors) and surfaces a counter; corrupt BLOCKS data is a
//! loud `Corrupted` error.

use std::sync::Arc;

use ravel_types::logstream::AttrValue;

use crate::block::{ColumnIdSet, ColumnPlan, DecodedBlock, PageCounters, read_block_pages};
use crate::bloom_section::BloomSection;
use crate::columnar::ColumnarBlockView;
use crate::columns::ColumnSelection;
use crate::error::LogSegError;
use crate::field_dir::FieldDir;
use crate::footer::{COMP_ZSTD, LogFooter, SectionDesc, kind, open};
use crate::page::{DEFAULT_MAX_UNCOMP, read_page};
use crate::page_dir::{PageDir, PageLoc};
use crate::postings::{POSTINGS_VERSION_V1, PostingsSection, term_key};
use crate::record::{
    COL_BODY, COL_FLAGS, COL_OBSERVED_TS, COL_SEVERITY_NUM, COL_SEVERITY_TEXT, COL_SPAN_ID,
    COL_STREAM_REF, COL_TRACE_ID, COL_TS, FieldSel, FieldType, LogRecord, Predicate, resolve_value,
};
use crate::skip_index::{NumRangeArm, SkipIndex};
use crate::stream_dir::StreamDir;
use crate::tokenizer::tokens;
use crate::writer::RlogConfig;

pub(crate) const MAX_STREAMS: u64 = 1 << 24;
pub(crate) const MAX_FIELDS: u64 = 1 << 20;
/// Upper bound on an object's block count (untrusted-input guard). PAGE_DIR
/// derives its group and block caps from this (ADR-0699 decision 2).
pub const MAX_BLOCKS: u64 = 1 << 24;

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
    /// Block pages this scan decompressed and decoded. Grows as blocks are
    /// decoded, so a partially-drained [`BlockScan`] reports what it has read
    /// so far, not what it will read.
    pub pages_decoded: u64,
    /// Block pages this scan skipped because the [`ColumnSelection`] excluded
    /// their column. Zero for an all-columns scan; the observable proof that
    /// column projection reached the page level (ADR-0087 decision 3).
    pub pages_skipped: u64,
    /// Stored bytes of pages present in the blocks this scan decoded,
    /// regardless of the [`ColumnSelection`]. A decode-time column-filtering
    /// measurement, distinct from any wire-byte count. Grows as blocks are
    /// decoded (ADR-0107 decision 4).
    pub page_bytes_fetched: u64,
    /// Stored bytes of the pages this scan actually decoded after column
    /// filtering. Equal to `page_bytes_fetched` for an all-columns scan; the
    /// gap to it is the column-filtering waste (ADR-0107 decision 4).
    pub page_bytes_decoded: u64,
}

/// An opened RLOG object ready to scan.
pub struct RlogReader<'a> {
    bytes: &'a [u8],
    stream_dir: StreamDir,
    field_dir: FieldDir,
    skip: SkipIndex,
    blocks_offset: u64,
    /// The object's decoded PAGE_DIR (ADR-0699 decision 2), through which
    /// every block's pages are located.
    ///
    /// Behind an [`Arc`] so a [`BlockScan`] shares this one decoded copy and
    /// resolves each surviving block's pages at decode time, rather than
    /// materializing a `Vec<PageLoc>` per surviving block when the scan is
    /// pruned: the survivor list then stays proportional to the surviving-block
    /// count with one shared copy of the page metadata, not proportional to
    /// surviving blocks times pages-per-block (issue #760).
    page_dir: Arc<PageDir>,
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
        // PAGE_DIR is mandatory (ADR-0699 decision 2): a block's pages are
        // located only through it. Like SKIP_IDX and unlike BLOOM it is not
        // optional and a corrupt one never degrades, and an object missing it
        // is refused rather than read under a guessed layout, which would
        // decode another block's bytes as this one's.
        let page_dir = {
            let desc = footer
                .section(kind::PAGE_DIR)
                .ok_or_else(|| LogSegError::Corrupted("missing PAGE_DIR section".into()))?;
            let raw = read_section(bytes, desc, cfg)?;
            let dir = PageDir::decode(&raw)?;
            dir.validate_extents(blocks.len)?;
            if dir.block_count() != skip.l0.len() as u64 {
                return Err(LogSegError::Corrupted(format!(
                    "page_dir covers {} blocks but skip index has {}",
                    dir.block_count(),
                    skip.l0.len()
                )));
            }
            Arc::new(dir)
        };
        Ok(RlogReader {
            bytes,
            stream_dir,
            field_dir,
            skip,
            blocks_offset: blocks.offset,
            page_dir,
            bloom,
            postings,
        })
    }

    /// The byte extent in BLOCKS of one `(row group, column)` column chunk,
    /// absolute in the object: `(offset, length)`, covering exactly the pages
    /// PAGE_DIR lists for that column in that group, contiguous and in order.
    ///
    /// This is the seam ADR-0699 decision 5's fetcher reads: one ranged GET per
    /// surviving `(row group, projected column)` replaces one per block. It
    /// returns `None` for a group index past the last, and for a column the
    /// group carries no page for.
    pub fn column_chunk_range(&self, group: usize, column_id: u32) -> Option<(u64, u64)> {
        let (offset, len) = self.page_dir.chunk_range(group, column_id)?;
        Some((self.blocks_offset.checked_add(offset)?, len))
    }

    /// The number of row groups in the object.
    pub fn row_group_count(&self) -> usize {
        self.page_dir.groups.len()
    }

    /// The column plans for every dynamic column, for block decode.
    fn plans(&self) -> Vec<ColumnPlan> {
        column_plans(&self.field_dir)
    }

    /// This object's FIELD_DIR. Test-only: production resolution of a
    /// [`ColumnSelection`] happens inside [`RlogReader::scan_blocks`], which
    /// reaches the directory directly; `columns.rs`'s unit tests need a real
    /// writer-produced FIELD_DIR to resolve against.
    #[cfg(test)]
    pub(crate) fn field_dir(&self) -> &FieldDir {
        &self.field_dir
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
    /// A version 1 object declines POSTINGS pruning outright: its lists index
    /// the per-record column layer only, and a record-level duplicate key (which
    /// the v1 format records nothing about) can put the merged-view winner in a
    /// value no posting indexes, so an exact index over one layer cannot prune a
    /// merged-view query. A version 2 object indexes the merged view directly and
    /// prunes normally. See [`RlogReader::scan_blocks`] and
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
        let mut cursor = self.scan_blocks(content, prune, &ColumnSelection::all())?;
        let mut out = Vec::new();
        while let Some(rows) = cursor.next_block(self.bytes)? {
            out.extend(rows);
        }
        Ok((out, cursor.stats()))
    }

    /// The block-at-a-time form of [`RlogReader::scan_pruned`], plus column
    /// projection (ADR-0087).
    ///
    /// Pruning is identical: the same skip-index, POSTINGS, and bloom steps run
    /// here, once, and the returned [`BlockScan`] names exactly the surviving
    /// blocks. What differs is that nothing is decoded yet. The caller drives
    /// [`BlockScan::next_block`] one block at a time and can release each
    /// block's records before asking for the next, so peak resident decoded
    /// memory is one block rather than one object.
    ///
    /// `columns` narrows what each block decodes.
    /// [`ColumnSelection::all`] reproduces `scan_pruned` exactly, which is how
    /// `scan_pruned` itself is implemented; a narrower selection leaves the
    /// unselected columns undecoded, so the rebuilt records carry a *partial*
    /// view (an unselected fixed column reads as its zero value, an unselected
    /// attribute is simply absent from `attrs`). A caller that narrows the
    /// selection is responsible for having included every column it, or
    /// anything downstream of it, reads.
    pub fn scan_blocks(
        &self,
        content: &Predicate,
        prune: &[Predicate],
        columns: &ColumnSelection,
    ) -> Result<BlockScan, LogSegError> {
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
            return Ok(self.empty_scan(stats));
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
            return Ok(self.empty_scan(stats));
        }

        // Numeric-range prune arms (ADR-0095 decision 6): resolved from the
        // prune-only channel, they drive block pruning through the skip index
        // and nothing else. An arm whose field does not resolve to a dynamic
        // column of that exact type contributes zero pruning (the `Ok(None)`
        // shape of the unindexed POSTINGS path), never an error.
        let numeric = self.numeric_range_arms(&prune_arms);

        // Skip-index pruning.
        let mut candidates =
            self.skip
                .candidate_blocks(ts_min, ts_max, stream_refs.as_deref(), &numeric);
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
            // Both channels resolve to equality arms; building them first tells
            // us whether any postings work exists at all, so a query with no
            // eligible arm still skips the section exactly as before.
            let content_arms = self.postings_arms(&arms);
            let prune_arms = self.prune_postings_arms(&prune_arms);
            if !content_arms.is_empty() || !prune_arms.is_empty() {
                match self
                    .postings_section_verified(desc)
                    .and_then(PostingsSection::parse)
                {
                    Ok(section) => {
                        // A version 1 POSTINGS list indexes the per-record
                        // column layer's first occurrence per type only. A
                        // record that carries one indexed key more than once
                        // (any type) has a merged-view winner the list can omit
                        // -- a same-type duplicate's later value lands in
                        // `attrs_raw`, which no posting indexes -- and a v1
                        // object records nothing that distinguishes "had such a
                        // duplicate" from "did not". So a v1 object declines ALL
                        // equality pruning, on both channels, widen-only
                        // (ADR-0013): it costs the optimization on legacy
                        // objects, never correctness (docs/adrs/0049 amendment
                        // 2026-08-20, issue #333). A v2 list indexes the merged
                        // view and prunes both channels. This subsumes the old
                        // version-1 resource/scope exclusion, which only covered
                        // the stream-level hazard.
                        if section.version() != POSTINGS_VERSION_V1 {
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

        // Locate the survivors. Nothing is read or decoded here: the caller
        // drives the decode one block at a time through `BlockScan::next_block`.
        // A survivor is named by its whole-object block index only; its pages
        // are resolved through the shared PAGE_DIR at decode time
        // (`BlockScan::decode_block`), so the survivor list holds one fixed-size
        // handle per surviving block rather than a `Vec<PageLoc>` of that
        // block's pages, keeping it O(surviving blocks) not O(surviving blocks
        // times pages-per-block) (issue #760).
        let mut blocks = Vec::with_capacity(survivors.len());
        for &b in &survivors {
            let entry =
                self.skip.l0.get(b).ok_or_else(|| {
                    LogSegError::Corrupted("skip block index out of range".into())
                })?;
            let block_index =
                u32::try_from(b).map_err(|_| LogSegError::Corrupted("block index range".into()))?;
            blocks.push(BlockLoc {
                record_count: entry.record_count as usize,
                crc32c: entry.block_crc32c,
                block_index,
            });
        }

        Ok(BlockScan {
            stream_dir: self.stream_dir.clone(),
            field_dir: self.field_dir.clone(),
            plans: self.plans(),
            columns: columns
                .resolve(&self.field_dir)
                .map(|s| s.into_iter().collect()),
            content: content.clone(),
            blocks,
            page_dir: self.page_dir.clone(),
            blocks_offset: self.blocks_offset,
            next: 0,
            stats,
            current: None,
            surviving: Vec::new(),
        })
    }

    /// Like [`RlogReader::scan_blocks`], but the returned cursor drains only the
    /// surviving blocks at the positions named in `indices`, in the order given,
    /// rather than every surviving block (intra-segment scan partitioning,
    /// ADR-0102).
    ///
    /// `indices` are positions into the full surviving-block list
    /// [`scan_blocks`](Self::scan_blocks) would produce for the same
    /// `content`/`prune`: the same skip-index, POSTINGS, and bloom pruning runs
    /// here, once, and produces the same ordered survivor list; this then keeps
    /// only the entries the caller named. Callers derive `indices` from a prior
    /// `scan_blocks` (or an equivalent count) over the SAME immutable object, so
    /// the two survivor lists are identical by construction and an index at or
    /// past the survivor count can only mean corruption -- it is a typed
    /// `Corrupted` error, never a panic.
    ///
    /// It is deliberately an explicit index list, not a contiguous range: the
    /// SQL logs scan hands one segment's blocks to several partitions on a
    /// `pos % n` stride, so a partition owns a non-contiguous subset (0, n, 2n,
    /// ...) that a range cannot express.
    ///
    /// The returned cursor's [`ScanStats`] totals (`blocks_total`,
    /// `blocks_after_skip`/`_postings`/`_bloom`) describe the WHOLE segment's
    /// pruning, exactly as `scan_blocks` reports them, because the same pruning
    /// ran; only `blocks_scanned`/`pages_*` grow as this subset drains. A caller
    /// striping one segment across partitions must therefore attribute the
    /// whole-segment totals to a single partition to avoid counting them once
    /// per partition (see `ravel_sql::logs_scan`).
    ///
    /// [`scan_blocks`](Self::scan_blocks) itself, and every other caller, is
    /// unchanged: this adds a way to restrict which of the already-pruned blocks
    /// a cursor drains, and changes nothing about what is read from the object.
    pub fn scan_blocks_subset(
        &self,
        content: &Predicate,
        prune: &[Predicate],
        columns: &ColumnSelection,
        indices: &[usize],
    ) -> Result<BlockScan, LogSegError> {
        let mut scan = self.scan_blocks(content, prune, columns)?;
        let mut subset = Vec::with_capacity(indices.len());
        for &i in indices {
            let loc =
                scan.blocks.get(i).cloned().ok_or_else(|| {
                    LogSegError::Corrupted("subset block index out of range".into())
                })?;
            subset.push(loc);
        }
        scan.blocks = subset;
        scan.next = 0;
        Ok(scan)
    }

    /// A cursor over zero surviving blocks, for the two pre-decode short
    /// circuits (an empty ts range, an empty resolved stream set). Draining it
    /// yields nothing, which is what those paths returned before.
    fn empty_scan(&self, stats: ScanStats) -> BlockScan {
        BlockScan {
            stream_dir: self.stream_dir.clone(),
            field_dir: self.field_dir.clone(),
            plans: Vec::new(),
            columns: None,
            content: Predicate::And(Vec::new()),
            blocks: Vec::new(),
            page_dir: Arc::new(PageDir::default()),
            blocks_offset: 0,
            next: 0,
            stats,
            current: None,
            surviving: Vec::new(),
        }
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
    /// A `prune` arm is the SQL merged-view equality pushed down over the `attrs`
    /// column, which DataFusion re-applies exactly above the scan, so this may
    /// only ever widen the fetch (ADR-0013). It answers, per arm, "which blocks
    /// can hold a record whose merged `attrs[name]` equals this value", by
    /// probing the `(name, type)` column the literal resolves to. Version
    /// handling (a version 1 object declines pruning entirely) lives in
    /// [`RlogReader::scan_blocks`], which knows the grammar version after parse.
    ///
    /// One arm is declined here regardless of version: a `Str` literal on a name
    /// that also has a non-`Str` column. SQL's `attrs` is `Map(Utf8, Utf8)`, so
    /// `attrs['k'] = 'v'` arrives as a `Str` literal, but a value of another
    /// type that stringifies to the same text is a merged-view match too and
    /// lives in a different column's postings. POSTINGS terms are bit-exact per
    /// type, never stringified, so probing only the `(name, Str)` column would
    /// prune away a block whose match is, say, an `I64` value with that text.
    /// Declining is widen-only; the exact SQL residual still filters. This is a
    /// distinct hazard from the duplicate-occurrence fold order (issue #333):
    /// the term written is right, the column probed is wrong.
    fn prune_postings_arms(&self, arms: &[&Predicate]) -> Vec<(u32, Vec<u8>)> {
        let mut out = Vec::new();
        for a in arms {
            if let Predicate::Equals {
                field: FieldSel::Attr(name),
                value,
            } = a
            {
                let (ty, cv) = resolve_value(value);
                if ty == FieldType::Str && self.name_has_non_str_column(name) {
                    continue;
                }
                if let Some(entry) = self.field_dir.column(name, ty) {
                    out.push((entry.column_id, term_key(&cv)));
                }
            }
        }
        out
    }

    /// Resolves the prune-only `NumRange` arms to [`NumRangeArm`] inputs for
    /// [`SkipIndex::candidate_blocks`].
    ///
    /// Each arm names an attribute and its exact column type; it resolves to the
    /// dynamic column [`FieldDir::column`] returns for that `(name, type)`. An
    /// arm whose field does not resolve to such a column -- an unknown name, a
    /// name stored only under other types, or a non-attribute selector -- yields
    /// no [`NumRangeArm`] and so prunes nothing, matching the degrade-safe
    /// fallthrough the unindexed POSTINGS field takes. The bounds pass straight
    /// through as bit patterns; the range is prune-only, so the exact residual
    /// stays the caller's (SQL layer's) job (ADR-0095 decision 6, ADR-0013).
    fn numeric_range_arms(&self, arms: &[&Predicate]) -> Vec<NumRangeArm> {
        self.field_dir.numeric_range_arms(arms)
    }

    /// Whether `name` has any dynamic column of a type other than `Str` in this
    /// object. A `Str`-literal merged-view prune on such a name is unsound
    /// (a non-`Str` value can stringify to the same text yet sit in a different
    /// column's postings), so [`RlogReader::prune_postings_arms`] declines it.
    fn name_has_non_str_column(&self, name: &str) -> bool {
        self.field_dir
            .entries()
            .iter()
            .any(|e| e.name == name && e.ty != FieldType::Str)
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
}

/// Which surviving block to decode: its whole-object block index, resolved to
/// the block's pages through the [`BlockScan`]'s shared PAGE_DIR at decode time
/// rather than carried here (issue #760), plus what the skip index says about
/// it.
///
/// `crc32c` is the block crc, defined over the concatenation of the block's
/// pages in `column_id` order, so it is verifiable only by a reader that took
/// all of them; a page-subset read verifies each page's own crc instead.
///
/// `Copy` is load-bearing: it is what proves a survivor entry cannot own a
/// `Vec<PageLoc>`, so the pruned survivor list holds one fixed-size handle per
/// surviving block, not that block's page list. Reintroducing a per-block page
/// vector here (the pre-#760 shape) fails to compile against this derive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BlockLoc {
    record_count: usize,
    crc32c: u32,
    block_index: u32,
}

/// A pruned scan that has not decoded anything yet: the surviving block list,
/// the directories needed to rebuild records, and the resolved column filter.
///
/// [`RlogReader::scan_pruned`] is this type drained in a loop. The point of
/// exposing it is that a caller can drain it *incrementally* and drop each
/// block's records before asking for the next, which is what bounds the SQL
/// logs scan's peak memory to one block rather than one partition (ADR-0087).
///
/// It owns everything it needs (the two directories are cloned out of the
/// reader), so it does not borrow the object bytes. The bytes are supplied per
/// call instead, which lets the caller hold them however it likes -- an owned
/// `Bytes` from a cache hit, say -- without a self-referential struct.
pub struct BlockScan {
    stream_dir: StreamDir,
    field_dir: FieldDir,
    plans: Vec<ColumnPlan>,
    /// `None` decodes every column (see [`ColumnSelection::resolve`]). Column
    /// ids are self-generated and dense (docs on [`ColumnIdSet`]), so this
    /// re-collects `resolve`'s std `HashSet<u32>` into the crate's faster set
    /// rather than paying SipHash on every page of every block decoded.
    columns: Option<ColumnIdSet>,
    /// The exact per-row filter, re-evaluated against every decoded row.
    content: Predicate,
    blocks: Vec<BlockLoc>,
    /// The object's shared decoded PAGE_DIR. One [`Arc`] copy per scan, cloned
    /// from the reader, through which [`Self::decode_block`] resolves each
    /// survivor's pages at decode time. This is what keeps the pruned survivor
    /// list proportional to the surviving-block count rather than to blocks
    /// times pages-per-block (issue #760).
    page_dir: Arc<PageDir>,
    /// Absolute offset of the BLOCKS section, added to the PAGE_DIR-relative
    /// page offsets when a block is resolved at decode time.
    blocks_offset: u64,
    next: usize,
    stats: ScanStats,
    /// The block [`Self::decode_block`] most recently decoded, held only for as
    /// long as one of the two exits is reading it. Dropped at the start of the
    /// next decode, and dropped by [`Self::next_block`] before it returns, so
    /// the row path holds exactly one block's decoded columns at a time as it
    /// did before the columnar exit existed (ADR-0087 decision 2).
    current: Option<DecodedBlock>,
    /// Row positions of [`Self::current`] that matched `content`, ascending.
    surviving: Vec<usize>,
}

impl BlockScan {
    /// Pruning counters. `blocks_total`/`blocks_after_*` are final from
    /// construction; `blocks_scanned`, `pages_decoded`, and `pages_skipped`
    /// grow as blocks are drained, so read this after the last
    /// [`Self::next_block`] to get the whole scan's figures.
    pub fn stats(&self) -> ScanStats {
        self.stats
    }

    /// Surviving blocks not yet decoded.
    pub fn remaining_blocks(&self) -> usize {
        self.blocks.len().saturating_sub(self.next)
    }

    /// Decode the next surviving block and return the rows of it that match the
    /// exact filter, or `None` once every surviving block has been decoded.
    ///
    /// `object_bytes` MUST be the whole RLOG object the [`RlogReader`] this
    /// cursor came from was opened on: block extents are absolute offsets into
    /// it. A block whose extent falls outside the buffer is a typed
    /// `Corrupted` error, never a panic.
    ///
    /// Returning `Some(vec![])` is normal and distinct from `None`: a block can
    /// survive pruning and still have no row matching the exact filter. Only
    /// `None` means the scan is finished.
    pub fn next_block(
        &mut self,
        object_bytes: &[u8],
    ) -> Result<Option<Vec<LogRecord>>, LogSegError> {
        if !self.decode_block(object_bytes)? {
            return Ok(None);
        }
        let Some(decoded) = self.current.take() else {
            return Err(LogSegError::Corrupted("block not decoded".into()));
        };
        let strict = self.columns.is_none();
        let mut out = Vec::with_capacity(self.surviving.len());
        for &row in &self.surviving {
            out.push(rebuild_record_projected(
                &self.stream_dir,
                &self.field_dir,
                &decoded,
                row,
                strict,
            )?);
        }
        self.surviving.clear();
        Ok(Some(out))
    }

    /// Decode the next surviving block and return a borrowed columnar view of
    /// the rows of it that match the exact filter, or `None` once every
    /// surviving block has been decoded (ADR-0099 decision 1).
    ///
    /// This is the second exit next to [`Self::next_block`], over the same
    /// decode primitive: the view's surviving rows are exactly the rows
    /// `next_block` would have returned records for, in the same order. What
    /// differs is that nothing is rebuilt -- no `String`, no cloned STREAM_DIR
    /// blob, no per-attribute key clone -- so a caller that is about to build
    /// columnar output does not pay for a row form first.
    ///
    /// `object_bytes` has the same contract as [`Self::next_block`]'s, and the
    /// same malformed-block cases are the same typed `Corrupted` errors, never
    /// panics. Two of them are checked here rather than while rebuilding a
    /// record: every surviving row must have a `ts` and a `stream_ref` that
    /// resolves to a STREAM_DIR entry. The row path rejects both while building
    /// each record; this exit builds nothing, so it checks them up front rather
    /// than handing out a view whose `ts` and stream identity read as absent.
    ///
    /// The returned view borrows this cursor, so it must be dropped before the
    /// next call. The decoded block is released on the next call to either
    /// exit.
    pub fn next_block_columnar(
        &mut self,
        object_bytes: &[u8],
    ) -> Result<Option<ColumnarBlockView<'_>>, LogSegError> {
        if !self.decode_block(object_bytes)? {
            return Ok(None);
        }
        let Some(decoded) = self.current.as_ref() else {
            return Err(LogSegError::Corrupted("block not decoded".into()));
        };
        // Resolve stream_ref and ts once for the whole block, not once per
        // surviving row (#875): the per-row body then only indexes the resolved
        // slices, and the error text stays identical to the per-cell `i64_at`.
        let stream_refs = decoded.i64_col(COL_STREAM_REF);
        let ts = decoded.i64_col(COL_TS);
        let entries = self.stream_dir.entries();
        for &row in &self.surviving {
            let raw = stream_refs
                .and_then(|c| c.get(row).copied())
                .flatten()
                .ok_or_else(|| {
                    LogSegError::Corrupted(format!("missing i64 col {COL_STREAM_REF}"))
                })?;
            let sref = u32::try_from(raw)
                .map_err(|_| LogSegError::Corrupted("stream_ref range".into()))?;
            if entries.get(sref as usize).is_none() {
                return Err(LogSegError::Corrupted("stream_ref out of range".into()));
            }
            ts.and_then(|c| c.get(row).copied())
                .flatten()
                .ok_or_else(|| LogSegError::Corrupted(format!("missing i64 col {COL_TS}")))?;
        }
        Ok(Some(ColumnarBlockView::new(
            &self.stream_dir,
            &self.field_dir,
            decoded,
            &self.surviving,
        )))
    }

    /// The decode primitive both exits run on: locate the next surviving block,
    /// decode its selected columns, account for its pages, and evaluate the
    /// exact content predicate into [`Self::surviving`].
    ///
    /// `Ok(false)` means the cursor is exhausted and nothing was decoded.
    /// `Ok(true)` leaves the decoded block in [`Self::current`] and its
    /// surviving row positions, ascending, in [`Self::surviving`].
    ///
    /// Having one primitive is what keeps the two exits from drifting: the
    /// surviving row set is computed once, by one predicate evaluation, so a
    /// columnar view cannot disagree with the records `next_block` would have
    /// produced for the same block.
    fn decode_block(&mut self, object_bytes: &[u8]) -> Result<bool, LogSegError> {
        // Release the previous block before decoding the next, so peak resident
        // decoded memory stays one block rather than two.
        self.current = None;
        self.surviving.clear();

        let Some(loc) = self.blocks.get(self.next).cloned() else {
            return Ok(false);
        };
        self.next += 1;
        // Resolve the block's pages now, from the shared PAGE_DIR, rather than
        // from a per-block list built at scan time (issue #760). `block_pages`
        // returns offsets relative to the BLOCKS section; shift them to
        // absolute object offsets, exactly what the scan-time build produced,
        // so the decode is byte-identical.
        let mut pages = self
            .page_dir
            .block_pages(loc.block_index)
            .ok_or_else(|| LogSegError::Corrupted("block not in page_dir".into()))?;
        for p in &mut pages {
            p.offset = self
                .blocks_offset
                .checked_add(p.offset)
                .ok_or_else(|| LogSegError::Corrupted("page offset overflow".into()))?;
        }
        let decoded = decode_v4_block(
            object_bytes,
            0,
            loc.record_count,
            loc.crc32c,
            &pages,
            &self.plans,
            self.columns.as_ref(),
        )?;
        self.stats.blocks_scanned += 1;
        self.stats.pages_decoded += decoded.pages_decoded() as u64;
        self.stats.pages_skipped += decoded.pages_skipped() as u64;
        self.stats.page_bytes_fetched = self
            .stats
            .page_bytes_fetched
            .saturating_add(decoded.page_bytes_fetched());
        self.stats.page_bytes_decoded = self
            .stats
            .page_bytes_decoded
            .saturating_add(decoded.page_bytes_decoded());

        for row in 0..decoded.record_count() {
            if eval(
                &self.stream_dir,
                &self.field_dir,
                &self.content,
                &decoded,
                row,
            )? {
                self.surviving.push(row);
            }
        }
        self.current = Some(decoded);
        Ok(true)
    }
}

/// Reads and decodes one version-4 block, given its pages located through
/// PAGE_DIR (ADR-0699 decisions 1 and 2).
///
/// Every page whose column the projection keeps is checksum-verified against
/// its own PAGE_DIR `crc32c` before it is decompressed, so a page-subset read
/// verifies every byte it goes on to interpret without touching the rest of the
/// block. A page whose column the projection drops is never read at all: under
/// version 3 its stored bytes were inside the block extent already fetched and
/// were merely walked past, here they are simply not addressed.
///
/// When the projection keeps everything, the block's SKIP_IDX level-0 crc is
/// verified too, over the concatenation of the pages in `column_id` order,
/// which is what a whole-block read assembles. That is a check on PAGE_DIR's
/// placement claim, not a duplicate of the per-page checks: it fails if the
/// directory pointed at the right bytes in the wrong order, or at another
/// block's pages.
/// `bytes` need not be the whole object: `base` is the absolute offset its
/// first byte sits at, so a caller holding one fetched range passes that
/// range's start and a whole-object caller passes 0.
pub(crate) fn decode_v4_block(
    bytes: &[u8],
    base: u64,
    record_count: usize,
    block_crc32c: u32,
    pages: &[PageLoc],
    plans: &[ColumnPlan],
    columns: Option<&ColumnIdSet>,
) -> Result<DecodedBlock, LogSegError> {
    let wanted = |cid: u32| match columns {
        None => true,
        Some(set) => set.contains(&cid),
    };
    let mut descs: Vec<crate::page::PageDesc> = Vec::with_capacity(pages.len());
    let mut page_bytes: Vec<Option<Vec<u8>>> = Vec::with_capacity(pages.len());
    let mut counters = PageCounters::default();
    let mut block_crc = 0u32;
    let mut all_read = true;
    for p in pages {
        counters.bytes_fetched = counters.bytes_fetched.saturating_add(p.desc.len);
        descs.push(p.desc);
        if !wanted(p.desc.column_id) {
            counters.skipped += 1;
            all_read = false;
            page_bytes.push(None);
            continue;
        }
        let rel = p
            .offset
            .checked_sub(base)
            .ok_or_else(|| LogSegError::Corrupted("page before fetched range".into()))?;
        let start =
            usize::try_from(rel).map_err(|_| LogSegError::Corrupted("page offset range".into()))?;
        let len = usize::try_from(p.desc.len)
            .map_err(|_| LogSegError::Corrupted("page len range".into()))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| LogSegError::Corrupted("page range overflow".into()))?;
        let stored = bytes
            .get(start..end)
            .ok_or_else(|| LogSegError::Corrupted("page out of bounds".into()))?;
        if crc32c::crc32c(stored) != p.crc32c {
            return Err(LogSegError::Corrupted(format!(
                "page crc mismatch for column {}",
                p.desc.column_id
            )));
        }
        block_crc = crc32c::crc32c_append(block_crc, stored);
        page_bytes.push(Some(read_page(stored, &p.desc, DEFAULT_MAX_UNCOMP)?));
        counters.decoded += 1;
        counters.bytes_decoded = counters.bytes_decoded.saturating_add(p.desc.len);
    }
    if all_read && block_crc != block_crc32c {
        return Err(LogSegError::Corrupted("block crc mismatch".into()));
    }
    read_block_pages(record_count, &descs, &page_bytes, plans, counters)
}

// --- exact evaluation -------------------------------------------------------

/// Evaluate the exact filter against one decoded row.
///
/// Free rather than a [`RlogReader`] method because [`BlockScan`] evaluates the
/// same predicate without holding a reader: both need only the two directories.
fn eval(
    stream_dir: &StreamDir,
    field_dir: &FieldDir,
    pred: &Predicate,
    block: &DecodedBlock,
    row: usize,
) -> Result<bool, LogSegError> {
    Ok(match pred {
        Predicate::And(v) => {
            for p in v {
                if !eval(stream_dir, field_dir, p, block, row)? {
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
            match stream_dir.stream_id(r) {
                Some(sid) => ids.contains(sid),
                None => false,
            }
        }
        Predicate::HasWord { field, word } => {
            let value = field_text(field_dir, field, block, row)?;
            match value {
                Some(bytes) => phrase_match(&bytes, word),
                None => false,
            }
        }
        Predicate::Equals { field, value } => equals(field_dir, field, value, block, row)?,
        // `NumRange` is prune-only (ADR-0095 decision 6): it drives block
        // pruning through `scan_blocks`'s `prune` channel and is never an exact
        // per-row filter. Reaching exact evaluation means the caller wired it
        // into `content`; it matches every row (a no-op) rather than filtering,
        // and the caller re-evaluates the real, exactly-typed range above the
        // scan.
        Predicate::NumRange { .. } => true,
    })
}

/// The tokenizable text of a field for a row, if present.
fn field_text(
    field_dir: &FieldDir,
    field: &FieldSel,
    block: &DecodedBlock,
    row: usize,
) -> Result<Option<Vec<u8>>, LogSegError> {
    Ok(match field {
        FieldSel::Body => str_at(block, COL_BODY, row),
        FieldSel::SeverityText => str_at(block, COL_SEVERITY_TEXT, row),
        FieldSel::Attr(name) => {
            match field_dir.column(name, FieldType::Str) {
                Some(e) => str_at(block, e.column_id, row),
                // Fall back to an overflow attr of type Str.
                None => overflow_attr(block, row, name)?.and_then(|v| match v {
                    AttrValue::Str(s) => Some(s.into_bytes()),
                    _ => None,
                }),
            }
        }
    })
}

fn equals(
    field_dir: &FieldDir,
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
            if let Some(entry) = field_dir.column(name, ty) {
                Ok(attr_equals(block, entry.column_id, ty, value, row))
            } else {
                // Overflow: compare against the decoded attrs_raw value.
                Ok(overflow_attr(block, row, name)?
                    .as_ref()
                    .is_some_and(|v| attr_value_eq(v, value)))
            }
        }
    }
}

/// Looks up an attribute by name in a row's decoded `attrs_raw`, if any.
fn overflow_attr(
    block: &DecodedBlock,
    row: usize,
    name: &str,
) -> Result<Option<AttrValue>, LogSegError> {
    let raw = match block.str_at(crate::record::COL_ATTRS_RAW, row) {
        Some(bytes) => bytes,
        None => return Ok(None),
    };
    let attrs = decode_canonical_attrs(raw)?;
    Ok(attrs.into_iter().find(|(k, _)| k == name).map(|(_, v)| v))
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
    rebuild_record_projected(stream_dir, field_dir, block, row, true)
}

/// [`rebuild_record`] over a possibly column-projected block.
///
/// `strict` is the all-columns contract: every fixed numeric column must be
/// present, and its absence is a `Corrupted` error, because for an unprojected
/// decode absence really does mean the block is malformed. Under a projection
/// (`strict == false`) an undecoded fixed numeric column is expected, so it
/// reads as `0` rather than erroring -- the caller asked not to decode it and
/// must not read the field. `ts` and `stream_ref` are exempt: a
/// [`ColumnSelection`](crate::ColumnSelection) always keeps them, so their
/// absence is a real corruption under either mode and still errors.
///
/// String and fixed-width columns need no mode: they are `Option` at the block
/// level already, so an undecoded one reads as empty/`None` exactly as an
/// absent one does.
pub(crate) fn rebuild_record_projected(
    stream_dir: &StreamDir,
    field_dir: &FieldDir,
    block: &DecodedBlock,
    row: usize,
    strict: bool,
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
    if let Some(raw) = block.str_at(crate::record::COL_ATTRS_RAW, row) {
        attrs.extend(decode_canonical_attrs(raw)?);
    }

    Ok(LogRecord {
        stream_id,
        stream_attrs,
        ts_ns: i64_at(block, COL_TS, row)?,
        observed_ts_ns: i64_projected(block, COL_OBSERVED_TS, row, strict)?,
        severity_num: i64_projected(block, COL_SEVERITY_NUM, row, strict)? as u8,
        severity_text,
        body,
        trace_id,
        span_id,
        flags: i64_projected(block, COL_FLAGS, row, strict)? as u32,
        attrs,
    })
}

/// A fixed numeric column's value, or `0` when the column was projected away.
/// Under `strict` (an all-columns decode) an absent column is corruption and
/// errors, exactly as before column projection existed.
fn i64_projected(
    block: &DecodedBlock,
    col: u32,
    row: usize,
    strict: bool,
) -> Result<i64, LogSegError> {
    match block.i64_col(col) {
        Some(_) => i64_at(block, col, row),
        None if strict => i64_at(block, col, row),
        None => Ok(0),
    }
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
    block.str_at(col, row).map(<[u8]>::to_vec)
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
///
/// `pub` so `ravel-sql`'s `has_word` UDF calls this directly instead of
/// keeping its own copy: the pushed `Predicate::HasWord` filter here and the
/// UDF re-applied above the scan must agree on every row for the pushdown to
/// stay sound, and two independently maintained implementations can drift
/// without either crate's own tests noticing.
pub fn phrase_match(value: &[u8], word: &str) -> bool {
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
/// Used by the writer's merged-view POSTINGS accumulation (to index the union
/// of a record's stream and per-record attributes). Kept in this crate so the
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

    /// A record carrying `cols` int columns and `cols` string columns, all
    /// present in every row, so each is its own dynamic column and contributes
    /// at least one value page per block: the object is wide enough that a
    /// block's page count dwarfs 1.
    fn wide_rec(ts: i64, cols: usize) -> LogRecord {
        let mut attrs = Vec::with_capacity(2 * cols);
        for c in 0..cols as i64 {
            attrs.push((format!("i{c}"), AttrValue::I64(ts * 31 + c)));
            attrs.push((format!("s{c}"), AttrValue::Str(format!("v{c}-{ts}"))));
        }
        LogRecord {
            stream_id: sid(0),
            stream_attrs: crate::record::stream_attrs_bytes(
                &[("service.name".into(), AttrValue::Str("svc".into()))],
                "scope",
                "1",
                &[],
            ),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: format!("body {ts}"),
            trace_id: Some([(ts % 251) as u8; 16]),
            span_id: Some([(ts % 251) as u8; 8]),
            flags: ts as u32 & 0xf,
            attrs,
        }
    }

    /// The number of resident page-location handles the pruned survivor list
    /// holds: one whole-object block index per surviving block, never one per
    /// page. `BlockLoc` is `Copy` (asserted below), so an entry cannot own a
    /// `Vec<PageLoc>`, and the sum is the survivor count.
    fn resident_page_locations(scan: &BlockScan) -> usize {
        scan.blocks.len()
    }

    /// The version-4 survivor list holds one shared copy of the page metadata
    /// and resolves each block's pages at decode time, so the pruning stage
    /// retains one fixed-size handle per surviving block, not that block's page
    /// list: the resident page-location count is the surviving-block count, not
    /// blocks times pages-per-block (issue #760).
    #[test]
    fn v4_scan_survivor_list_is_o_surviving_blocks_not_blocks_times_pages() {
        // A survivor entry is a fixed-size Copy handle. `Vec<PageLoc>` is not
        // `Copy`, so the pre-#760 shape (a per-block page vector in `BlockLoc`)
        // would not compile here: this pins the O(1)-per-survivor structure at
        // compile time, so the resident count below cannot silently regress to
        // one PageLoc per page.
        fn assert_copy<T: Copy>() {}
        assert_copy::<BlockLoc>();

        // A wide (50 dynamic columns => >=50 pages/block), long (8 blocks over
        // 2 row groups) version-4 object.
        const COLS: usize = 25; // 25 int + 25 str = 50 dynamic columns
        const RECS_PER_BLOCK: usize = 4;
        const BLOCKS: usize = 8;
        const GROUP: usize = 4; // 8 blocks / 4-block groups = 2 groups
        let cfg = RlogConfig {
            block_target_records: RECS_PER_BLOCK,
            group_target_blocks: GROUP,
            ..RlogConfig::default()
        };
        let recs: Vec<LogRecord> = (0..(BLOCKS * RECS_PER_BLOCK) as i64)
            .map(|ts| wide_rec(ts, COLS))
            .collect();
        let obj = build(cfg, recs);

        let reader = RlogReader::new(&obj, &cfg).expect("open reader");
        let dir = reader.page_dir.clone();
        assert_eq!(dir.block_count(), BLOCKS as u64, "8 blocks as configured");
        assert_eq!(
            dir.groups.len(),
            2,
            "8 blocks at a 4-block group is 2 groups"
        );

        // Pages per block: the wide corpus makes this comfortably >= 50, so
        // blocks x pages is an order of magnitude above the block count.
        let pages_per_block: Vec<usize> = (0..BLOCKS as u32)
            .map(|b| dir.block_pages(b).expect("block in dir").len())
            .collect();
        let min_p = *pages_per_block.iter().min().expect("blocks");
        assert!(
            min_p >= 50,
            "fixture must be wide: pages-per-block {pages_per_block:?} (min {min_p}) must be >= 50"
        );
        let total_pages: usize = pages_per_block.iter().sum();

        // Shape 1: all blocks survive (predicate-free scan).
        let all = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan all");
        assert_eq!(all.blocks.len(), BLOCKS, "all 8 blocks survive");
        // One shared copy of the page metadata, not one per survivor.
        assert!(
            Arc::ptr_eq(&all.page_dir, &reader.page_dir),
            "the scan shares the reader's one decoded PAGE_DIR"
        );
        let all_resident = resident_page_locations(&all);
        println!(
            "all-survive: survivors={} resident_page_locations={all_resident} \
             (pre-#760 would retain total_pages={total_pages} across 8 blocks x {min_p}+ pages)",
            all.blocks.len()
        );
        assert_eq!(
            all_resident, BLOCKS,
            "resident page-location handles must equal the surviving-block count"
        );
        assert_ne!(
            all_resident, total_pages,
            "the survivor list must not hold one handle per page (blocks x pages)"
        );

        // Shape 2: all blocks pruned but one. Each block holds 4 consecutive
        // ts; [0,3] is exactly block 0, so the ts prune drops the other 7.
        let one = reader
            .scan_blocks(
                &Predicate::TsRange {
                    min_ns: 0,
                    max_ns: 3,
                },
                &[],
                &ColumnSelection::all(),
            )
            .expect("scan one");
        assert_eq!(one.blocks.len(), 1, "ts range [0,3] leaves exactly block 0");
        let one_resident = resident_page_locations(&one);
        println!(
            "one-survivor: survivors={} resident_page_locations={one_resident} \
             (pre-#760 would retain pages_per_block[0]={} for the surviving block)",
            one.blocks.len(),
            pages_per_block[0]
        );
        assert_eq!(
            one_resident, 1,
            "one surviving block retains exactly one resident page-location handle"
        );
        assert_ne!(
            one_resident, pages_per_block[0],
            "the surviving block must not hold its whole page list resident"
        );
    }

    /// Resolving each survivor's pages lazily, out of the shared PAGE_DIR at
    /// decode time, does not depend on how those pages were placed: the same
    /// corpus written across several row groups and written as one decodes to
    /// the same records, in the same order, over a multi-block, multi-page,
    /// wide-column segment.
    ///
    /// The two objects differ in exactly where every page landed, so a
    /// resolution that mixed up two blocks' pages, or shifted a page offset,
    /// diverges here. The exact `ts` sequence is pinned alongside, so a decode
    /// that agreed with itself while dropping or reordering rows also fails.
    #[test]
    fn lazy_page_resolution_does_not_depend_on_the_row_group_size() {
        const COLS: usize = 25;
        const RECS: i64 = 32;
        let many_groups = RlogConfig {
            block_target_records: 4,
            group_target_blocks: 4,
            ..RlogConfig::default()
        };
        let one_group = RlogConfig {
            group_target_blocks: usize::MAX,
            ..many_groups
        };
        let recs: Vec<LogRecord> = (0..RECS).map(|ts| wide_rec(ts, COLS)).collect();

        let many = build(many_groups, recs.clone());
        let one = build(one_group, recs);
        assert_ne!(many, one, "the two placements must be different bytes");

        let cfg = RlogConfig::default();
        let reader_many = RlogReader::new(&many, &cfg).expect("open many-group");
        let reader_one = RlogReader::new(&one, &cfg).expect("open one-group");
        assert_eq!(
            (reader_many.row_group_count(), reader_one.row_group_count()),
            (2, 1),
            "32 records at 4 per block is 8 blocks: 2 groups at 4 per group, 1 when ungrouped"
        );

        let pred = Predicate::And(Vec::new());
        let (rows_many, _) = reader_many.scan(&pred).expect("scan many-group");
        let (rows_one, _) = reader_one.scan(&pred).expect("scan one-group");

        let ts: Vec<i64> = rows_many.iter().map(|r| r.ts_ns).collect();
        assert_eq!(
            ts,
            (0..RECS).collect::<Vec<_>>(),
            "every record comes back exactly once, in stored order"
        );
        assert_eq!(
            rows_many, rows_one,
            "lazy page resolution decodes identically whatever the row group size"
        );
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

    /// #693 part 2 premise: the write-time `LogFooter.block_count` counter
    /// equals the read-time `ScanStats.blocks_total` and the unpruned survivor
    /// count for a well-formed object, across a single block, a handful, and
    /// enough blocks to span multiple skip-index level-1 groups (FANOUT = 64).
    /// The predicate-free plan fast path substitutes the footer count for the
    /// read-time count, so the two must agree on every object the writer emits.
    ///
    /// They are structurally tied: the writer stamps `block_count` from
    /// `block_spans.len()` and pushes exactly one `Level0Entry` per span, and
    /// `SkipIndex::encode`/`decode` round-trip that entry count, so
    /// `blocks_total` (`skip.l0.len()`) reads back the same counter.
    #[test]
    fn footer_block_count_matches_unpruned_blocks_total() {
        for n in [1usize, 5, 130] {
            let cfg = RlogConfig {
                block_target_records: 1,
                ..RlogConfig::default()
            };
            // block_target_records=1 => one block per record, so n records span
            // n blocks (n=130 > FANOUT spans 3 level-1 groups).
            let recs: Vec<LogRecord> = (0..n as i64).map(|i| rec(0, i, "msg")).collect();
            let obj = build(cfg, recs);

            let footer = open(&obj).expect("open footer");
            assert_eq!(
                footer.block_count, n as u64,
                "n={n}: writer stamps one block per record at block_target_records=1"
            );

            let reader = RlogReader::new(&obj, &cfg).expect("open reader");
            let scan = reader
                .scan_blocks(
                    &Predicate::TsRange {
                        min_ns: i64::MIN,
                        max_ns: i64::MAX,
                    },
                    &[],
                    &ColumnSelection::all(),
                )
                .expect("scan_blocks");
            assert_eq!(
                scan.stats().blocks_total,
                footer.block_count as u32,
                "n={n}: blocks_total == footer.block_count"
            );
            assert_eq!(
                scan.remaining_blocks(),
                footer.block_count as usize,
                "n={n}: unpruned survivor count == footer.block_count"
            );
        }
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

    /// Every block containing a matching record survives postings pruning
    /// (soundness), and blocks
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

    /// A one-byte flip of a POSTINGS sparse-index `first_term`, corrupting no
    /// term block, must degrade to bloom + exact scan rather than reach `scan`
    /// as `postings_degraded == false` with a silently narrowed (wrong)
    /// result. Four terms `aa, bz, ca, cz` with `postings_stride: 2` (one per
    /// its own physical block, so a probe hit is exactly one row) split into
    /// term blocks `B0 = [aa, bz]` and `B1 = [ca, cz]`; flipping `B1`'s
    /// declared `first_term` from `"ca"` to `"ba"` preserves ascending order
    /// and every term-block crc, so without a whole-section check
    /// `RlogReader::new` and `PostingsSection::parse` would both accept it and
    /// a probe for `"bz"` would land on `B1`, miss, and report the term absent
    /// -- baseline 1 row, mutated 0 rows, no error, no counter. The
    /// whole-section `crc32c` verified before `parse` catches the flip
    /// regardless of which term is probed, degrading to bloom + exact scan
    /// instead.
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

    /// Soundness: a prune equality whose per-record resolution does
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
            "content wiring drops the resource-only match"
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
    /// needs that record. This is the same union-over-two-layers problem,
    /// relocated into the prune channel.
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

    /// The soundness case: a key that is a resource attribute on
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

    /// Record-wins precedence: a key present at resource level and
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

    /// A version 1 object declines POSTINGS pruning outright, even for a key
    /// that is NOT at stream level and carries no duplicates: the v1 grammar
    /// records nothing that could prove a record-level duplicate was absent, so
    /// the reader cannot know its per-record-layer index is complete, and
    /// declines rather than risk dropping a merged-view match (widen-only,
    /// ADR-0013; docs/adrs/0049 amendment 2026-08-20). It still reads back every
    /// record; the identical v2 object below prunes, proving the decline is a
    /// version choice, not a missing index. Built by pinning the version byte in
    /// a fixture.
    #[test]
    fn version_1_object_declines_all_equality_pruning() {
        let cfg = RlogConfig {
            block_target_records: 5,
            ..RlogConfig::default()
        };
        // `svc` is per-record only, 12 blocks cycling 3 values.
        let mut recs = Vec::new();
        for i in 0..60i64 {
            let block = i / 5;
            recs.push(rec_with_svc(i, &format!("s{}", block % 3)));
        }
        let v2 = build_indexed(cfg, recs, &["svc"]);
        let prune = [Predicate::Equals {
            field: FieldSel::Attr("svc".into()),
            value: AttrValue::Str("s0".into()),
        }];

        // v2 prunes to the 4 blocks that carry "s0".
        let reader = RlogReader::new(&v2, &cfg).expect("open v2");
        let (_rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        assert_eq!(stats.blocks_after_postings, 4, "v2 prunes");

        // The same object read as v1 declines: no block is pruned, and every
        // record still reads back (content is match-all here).
        let v1 = downgrade_postings_to_v1(&v2);
        let reader = RlogReader::new(&v1, &cfg).expect("open v1");
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        assert_eq!(stats.blocks_total, 12);
        assert_eq!(
            stats.blocks_after_postings, stats.blocks_after_skip,
            "v1 declines to prune"
        );
        assert_eq!(rows.len(), 60, "every record survives");
        assert!(!stats.postings_degraded);
    }

    /// Step-4 regression for issue #333: a v1-grammar object carrying a
    /// cross-type duplicate-key record must decline to prune, returning the row
    /// a wrong v1 prune would drop. The record has `dur = I64(5)` and
    /// `dur = Str("x")`; its merged-view winner is the reconstruction-order last
    /// occurrence, `I64(5)` (`Str` type byte < `I64`, so the `(dur, Str)` column
    /// sorts first and `(dur, I64)` last). A v2 posting therefore lists this
    /// block only under `(dur, I64) = 5`. Downgrading the version byte models a
    /// v1 object; under v1 the reader declines all equality pruning, so a prune
    /// on `Str("x")` -- which resolves to the `(dur, Str)` column whose posting
    /// is empty and would otherwise drop the block -- keeps it.
    #[test]
    fn version_1_object_with_cross_type_duplicate_declines_to_prune() {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let mut r = rec(0, 0, "msg");
        r.attrs.push(("dur".into(), AttrValue::I64(5)));
        r.attrs.push(("dur".into(), AttrValue::Str("x".into())));
        let obj = downgrade_postings_to_v1(&build_indexed(cfg, vec![r], &["dur"]));
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        // A prune whose literal resolves to the empty (dur, Str) posting would
        // drop the only block if v1 pruned; declining keeps it.
        let prune = [Predicate::Equals {
            field: FieldSel::Attr("dur".into()),
            value: AttrValue::Str("x".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        assert_eq!(rows.len(), 1, "v1 declines: the row survives");
        assert_eq!(stats.blocks_after_postings, stats.blocks_after_skip);
        assert!(!stats.postings_degraded);
    }

    /// The `attrs` merged view a query sees for one record, duplicating
    /// `ravel_sql::rlog_attrs::merged_attrs` inline (a test dependency on
    /// ravel-sql would be a dependency cycle): its decoded stream (resource +
    /// scope) attributes, then its own attributes folded last-wins by name.
    /// Operates on a record already reconstructed by `rebuild_record` (i.e. one
    /// returned by `scan`), so its `attrs` are in on-disk reconstruction order.
    fn merged_attrs_local(r: &LogRecord) -> Vec<(String, AttrValue)> {
        let mut merged = stream_attr_pairs(&r.stream_attrs).expect("decode stream_attrs");
        for (k, v) in &r.attrs {
            if let Some(slot) = merged.iter_mut().find(|(mk, _)| mk == k) {
                slot.1 = v.clone();
            } else {
                merged.push((k.clone(), v.clone()));
            }
        }
        merged
    }

    /// Decodes an object's FIELD_DIR.
    fn field_dir_of(obj: &[u8]) -> FieldDir {
        let cfg = RlogConfig::default();
        let footer = open(obj).expect("open");
        let desc = footer.section(kind::FIELD_DIR).expect("field_dir");
        let raw = read_section(obj, desc, &cfg).expect("read field_dir");
        FieldDir::decode(&raw, 1 << 20).expect("decode field_dir")
    }

    /// The POSTINGS section bytes of an object, for a `PostingsSection::parse`.
    fn postings_bytes_of(obj: &[u8]) -> Vec<u8> {
        let footer = open(obj).expect("open");
        let desc = footer.section(kind::POSTINGS).expect("postings");
        obj[desc.offset as usize..(desc.offset + desc.len) as usize].to_vec()
    }

    /// Issue #333 repro 1 (cross-type duplicate). A record carrying `dur = I64(5)`
    /// then `dur = Str("x")`, with `dur` indexed. The read-side merged view is
    /// the reconstruction-order last occurrence: `Str` type byte (1) sorts before
    /// `I64` (2), so `rebuild_record` lays out `(dur, Str) = "x"` then
    /// `(dur, I64) = 5`, and `merged_attrs` folds to `I64(5)`. Before the fix the
    /// writer folded over write-time order and chose `Str("x")`, leaving the
    /// `(dur, I64)` posting empty, so a prune on `I64(5)` dropped the block. It
    /// must now keep it.
    #[test]
    fn issue_333_cross_type_duplicate_prune_keeps_row() {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let mut r = rec(0, 0, "msg");
        r.attrs.push(("dur".into(), AttrValue::I64(5)));
        r.attrs.push(("dur".into(), AttrValue::Str("x".into())));
        let obj = build_indexed(cfg, vec![r], &["dur"]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let prune = [Predicate::Equals {
            field: FieldSel::Attr("dur".into()),
            value: AttrValue::I64(5),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        assert_eq!(
            rows.len(),
            1,
            "the merged view returns dur=I64(5); the prune must not drop its block"
        );
        assert!(!stats.postings_degraded);

        // Directly: the (dur, I64) posting lists this block for term 5.
        let fd = field_dir_of(&obj);
        let cid = fd
            .column("dur", FieldType::I64)
            .expect("(dur, I64) column")
            .column_id;
        let pd = postings_bytes_of(&obj);
        let section = PostingsSection::parse(&pd).expect("parse postings");
        assert_eq!(
            section
                .probe(cid, &term_key(&resolve_value(&AttrValue::I64(5)).1))
                .expect("probe"),
            Some(vec![0]),
            "the winning I64(5) value must be the indexed term"
        );
    }

    /// Issue #333 repro 2 (cross-type stringification). Two single-record blocks,
    /// `dur = I64(5)` and `dur = Str("q")`, `dur` indexed. This is not about
    /// duplicates: it is the reader resolving a `Str` prune literal only against
    /// the `(dur, Str)` column. A prune on the I64 value keeps the I64 block; a
    /// prune on `Str("5")` must NOT drop the I64 block, because `I64(5)`
    /// stringifies to `"5"` in the merged view and would match. The fix declines
    /// a `Str`-literal prune on a name that also has a non-`Str` column.
    #[test]
    fn issue_333_cross_type_stringification_prune_does_not_overprune() {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let mut r0 = rec(0, 0, "msg");
        r0.attrs.push(("dur".into(), AttrValue::I64(5)));
        let mut r1 = rec(0, 1, "msg");
        r1.attrs.push(("dur".into(), AttrValue::Str("q".into())));
        let obj = build_indexed(cfg, vec![r0, r1], &["dur"]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        // A prune on the I64 value keeps the I64 record's block.
        let prune = [Predicate::Equals {
            field: FieldSel::Attr("dur".into()),
            value: AttrValue::I64(5),
        }];
        let (rows, _stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        assert!(
            rows.iter().any(|r| r.ts_ns == 0),
            "an I64 prune must keep the I64 record's block"
        );

        // A Str("5") prune must not drop the I64 block: I64(5) stringifies to
        // "5" and is a merged-view match. The reader declines the arm.
        let prune = [Predicate::Equals {
            field: FieldSel::Attr("dur".into()),
            value: AttrValue::Str("5".into()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        assert!(
            rows.iter().any(|r| r.ts_ns == 0),
            "a Str('5') prune must not drop the block holding I64(5), which stringifies to '5'"
        );
        assert_eq!(
            stats.blocks_after_postings, stats.blocks_after_skip,
            "the cross-type Str arm is declined, not pruned on"
        );
        assert!(!stats.postings_degraded);
    }

    /// The general differential test for issue #333: for varied duplicate-key
    /// shapes, the POSTINGS term the writer chose for an indexed name must equal
    /// what a full write-then-read round trip through `rebuild_record` (via
    /// `scan`) and `merged_attrs` (via [`merged_attrs_local`]) reports. Each
    /// shape is written as its own one-record object, so the block index is
    /// trivially 0 and the probe assertion is exact. This proves the fix in
    /// general, not just for the two known repros, and checks write against an
    /// independent read path rather than against the writer's own algorithm.
    #[test]
    fn issue_333_write_side_winner_matches_read_side_merged_view() {
        // Each shape: the record's per-record attrs (dur duplicated variously),
        // plus an optional resource-level dur to exercise layer precedence.
        struct Shape {
            name: &'static str,
            record_attrs: Vec<AttrValue>,
            resource_dur: Option<AttrValue>,
        }
        let shapes = vec![
            Shape {
                name: "two same type",
                record_attrs: vec![AttrValue::I64(5), AttrValue::I64(6)],
                resource_dur: None,
            },
            Shape {
                name: "three same type, encoded order != write order",
                record_attrs: vec![AttrValue::I64(3), AttrValue::I64(10), AttrValue::I64(7)],
                resource_dur: None,
            },
            Shape {
                name: "two different types",
                record_attrs: vec![AttrValue::I64(5), AttrValue::Str("x".into())],
                resource_dur: None,
            },
            Shape {
                name: "three different types",
                record_attrs: vec![
                    AttrValue::Str("s".into()),
                    AttrValue::Bool(true),
                    AttrValue::I64(42),
                ],
                resource_dur: None,
            },
            Shape {
                name: "duplicate plus resource-level same name",
                record_attrs: vec![AttrValue::I64(5), AttrValue::I64(9)],
                resource_dur: Some(AttrValue::Str("R".into())),
            },
        ];

        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        for shape in &shapes {
            let mut r = rec(0, 0, "msg");
            if let Some(rd) = &shape.resource_dur {
                r.stream_attrs = crate::record::stream_attrs_bytes(
                    &[
                        ("service.name".into(), AttrValue::Str("svc0".into())),
                        ("dur".into(), rd.clone()),
                    ],
                    "scope",
                    "1",
                    &[],
                );
            }
            for v in &shape.record_attrs {
                r.attrs.push(("dur".into(), v.clone()));
            }
            let obj = build_indexed(cfg, vec![r], &["dur"]);
            let reader = RlogReader::new(&obj, &cfg).expect("open");

            // Read side: reconstruct the record and compute the merged value.
            let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
            assert_eq!(rows.len(), 1, "{}", shape.name);
            let merged = merged_attrs_local(&rows[0]);
            let value = merged
                .iter()
                .find(|(k, _)| k == "dur")
                .map(|(_, v)| v.clone())
                .unwrap_or_else(|| panic!("{}: merged view has dur", shape.name));
            let (ty, cv) = resolve_value(&value);

            // Write side: the POSTINGS term for dur must be exactly that value.
            let fd = field_dir_of(&obj);
            let cid = fd
                .column("dur", ty)
                .unwrap_or_else(|| panic!("{}: (dur, {ty:?}) column exists", shape.name))
                .column_id;
            let pd = postings_bytes_of(&obj);
            let section = PostingsSection::parse(&pd).expect("parse postings");
            assert_eq!(
                section.probe(cid, &term_key(&cv)).expect("probe"),
                Some(vec![0]),
                "{}: write-side POSTINGS term must equal the read-side merged winner {value:?}",
                shape.name
            );
        }
    }

    /// A version 1 object declines to prune (here for a key that also appears at
    /// stream level, one of the hazards): a v1 posting list indexes the
    /// per-record layer only, so an exact index over one layer cannot prune a
    /// merged-view query over two. Every record the merged view matches is
    /// returned, and no block is pruned. This is the conservative behaviour
    /// version 2 replaces
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

    /// A key that is resource-level on EVERY record and per-record on none.
    /// Without a FIELD_DIR column it would have no posting list, so a
    /// merged-view prune for it would skip nothing -- the ordinary
    /// single/few-service OTLP deployment the ADR-0049 amendment was written
    /// for. The writer gives it a stream-level-only column, so the prune
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

    /// Soundness: adding a stream-level-only column must NOT turn
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

    /// Under version 1: a resource-level-only key has a column
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
    /// resource-level still degrades to a typed error
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

    /// A record carrying an i64 attribute `code`, in `stream`, at `ts`.
    fn rec_code(stream: u8, ts: i64, code: i64) -> LogRecord {
        let mut r = rec(stream, ts, "msg");
        r.attrs.push(("code".into(), AttrValue::I64(code)));
        r
    }

    /// Decodes an object's level-0 skip index.
    fn skip_of(obj: &[u8]) -> SkipIndex {
        let cfg = RlogConfig::default();
        let footer = open(obj).expect("open");
        let raw = read_section(obj, footer.section(kind::SKIP_IDX).expect("skip"), &cfg)
            .expect("read skip");
        SkipIndex::decode(&raw, MAX_BLOCKS).expect("decode skip")
    }

    /// Reachability (ADR-0095 decision 6): a `NumRange` prune arm, driven
    /// through the real public `scan_pruned` entry point, excludes the block
    /// whose merged-view winner for the column falls outside the range and keeps
    /// the block whose winner falls inside it.
    ///
    /// Two single-record blocks: block 0 resolves `code = 200`, block 1 resolves
    /// `code = 999`. A prune for `code IN [900, 1000]` must drop block 0 and keep
    /// block 1. Asserted on `ScanStats` (the block actually left the candidate
    /// set) and on the returned rows, not on either alone.
    #[test]
    fn num_range_prune_excludes_out_of_range_block_keeps_in_range() {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let obj = build(cfg, vec![rec_code(0, 0, 200), rec_code(0, 1, 999)]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let prune = [Predicate::NumRange {
            field: FieldSel::Attr("code".into()),
            ty: FieldType::I64,
            min: Some(900i64 as u64),
            max: Some(1000i64 as u64),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");

        assert_eq!(stats.blocks_total, 2);
        assert_eq!(
            stats.blocks_after_skip, 1,
            "the out-of-range block (code=200) is pruned by the numeric arm"
        );
        assert_eq!(rows.len(), 1, "only the in-range block's record survives");
        assert_eq!(rows[0].ts_ns, 1);
        assert_eq!(
            rows[0]
                .attrs
                .iter()
                .find(|(k, _)| k == "code")
                .map(|(_, v)| v),
            Some(&AttrValue::I64(999))
        );
    }

    /// Soundness (ADR-0095 decision 6, ADR-0013): a block that resolves no value
    /// for the arm's column carries no stat for it, and such a block is NEVER
    /// pruned by the arm -- even though a naive "the block holds no matching
    /// value" reading would drop it. Getting this backwards silently drops
    /// correct results.
    ///
    /// Three single-record blocks: block 0 resolves `code = 15`, block 1 carries
    /// no `code` at all (and its resource has none), block 2 resolves
    /// `code = 999`. A prune for `code IN [10, 20]` keeps block 0 (in range),
    /// prunes block 2 (out of range), and MUST keep block 1 (no stat). The test
    /// first proves the premise off the decoded index -- block 1 genuinely has
    /// no `code` stat -- so it is testing the absent-stat path, not a stat that
    /// merely happens to overlap.
    #[test]
    fn num_range_prune_keeps_block_with_no_stat_for_the_column() {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        // Block 1 (ts 1) carries no `code`; `rec` gives it only a resource
        // `service.name`, so no row of block 1 resolves the `code` column.
        let obj = build(
            cfg,
            vec![
                rec_code(0, 0, 15),
                rec(0, 1, "no-code"),
                rec_code(0, 2, 999),
            ],
        );

        // Premise: the (code, i64) column exists object-wide, and block 1 has no
        // stat for it. Blocks are one record each, sorted by ts, so l0[1] is the
        // no-code block.
        let fd = field_dir_of(&obj);
        let cid = fd
            .column("code", FieldType::I64)
            .expect("(code, i64) column exists")
            .column_id;
        let skip = skip_of(&obj);
        assert_eq!(skip.l0.len(), 3, "one record per block");
        assert!(
            !skip.l0[1].stats.iter().any(|s| s.column_id == cid),
            "block 1 must genuinely carry no stat for the code column"
        );
        assert!(
            skip.l0[0].stats.iter().any(|s| s.column_id == cid)
                && skip.l0[2].stats.iter().any(|s| s.column_id == cid),
            "blocks 0 and 2 do carry a code stat"
        );

        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let prune = [Predicate::NumRange {
            field: FieldSel::Attr("code".into()),
            ty: FieldType::I64,
            min: Some(10i64 as u64),
            max: Some(20i64 as u64),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");

        // Block 2 (code=999) pruned; blocks 0 and 1 survive.
        assert_eq!(stats.blocks_total, 3);
        assert_eq!(stats.blocks_after_skip, 2);
        let by_ts: Vec<i64> = rows.iter().map(|r| r.ts_ns).collect();
        assert!(
            by_ts.contains(&1),
            "the no-stat block must survive: absence is no information, never no match"
        );
        assert!(
            !by_ts.contains(&2),
            "the out-of-range block (code=999) is pruned"
        );
        assert!(by_ts.contains(&0), "the in-range block (code=15) survives");
    }

    /// A `NumRange` arm whose field does not resolve to a dynamic column of the
    /// named type prunes nothing (degrade-safe fallthrough), the same way an
    /// unindexed POSTINGS field does. Here `code` exists as i64 but the arm asks
    /// for it as f64, which resolves to no column, so every block survives.
    #[test]
    fn num_range_prune_unresolved_column_prunes_nothing() {
        let cfg = RlogConfig {
            block_target_records: 1,
            ..RlogConfig::default()
        };
        let obj = build(cfg, vec![rec_code(0, 0, 200), rec_code(0, 1, 999)]);
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let prune = [Predicate::NumRange {
            field: FieldSel::Attr("code".into()),
            ty: FieldType::F64,
            min: Some(0.0f64.to_bits()),
            max: Some(1.0f64.to_bits()),
        }];
        let (rows, stats) = reader
            .scan_pruned(&Predicate::And(Vec::new()), &prune)
            .expect("scan");
        assert_eq!(
            stats.blocks_after_skip, stats.blocks_total,
            "a NumRange over an unresolved (name, type) must not prune"
        );
        assert_eq!(rows.len(), 2);
    }

    // --- columnar block view (ADR-0099 decision 1) ---------------------------

    /// The attribute key that overflows the dynamic-column budget in
    /// [`columnar_corpus`] and therefore lands in `attrs_raw`.
    const OVERFLOW_KEY: &str = "z_over";

    /// The dynamic-column budget [`columnar_corpus`] is built against: exactly
    /// enough for its five columned attribute names, so the sixth
    /// ([`OVERFLOW_KEY`], last in the writer's `(name, type)` order) spills into
    /// `attrs_raw`.
    const COLUMNAR_MAX_COLUMNS: usize = 5;

    fn rec_attrs(stream: u8, ts: i64, body: &str, attrs: Vec<(String, AttrValue)>) -> LogRecord {
        let mut r = rec(stream, ts, body);
        r.attrs = attrs;
        r
    }

    fn attr(k: &str, v: AttrValue) -> (String, AttrValue) {
        (k.to_string(), v)
    }

    /// A corpus for the columnar view, deliberately awkward: two streams, five
    /// columned attribute types plus one that overflows into `attrs_raw`, rows
    /// with no attributes at all (so every dynamic column is partially present),
    /// an empty `body`, an empty string attribute, a negative and an extreme
    /// integer, a negative zero, and rows with and without trace/span ids.
    ///
    /// Bodies are worded so `HasWord(body, "keep")` keeps a non-contiguous
    /// subset of every block's rows: an accessor that read a surviving-row index
    /// as a raw block row position would return another row's data.
    fn columnar_corpus() -> Vec<LogRecord> {
        vec![
            rec_attrs(
                0,
                100,
                "keep alpha",
                vec![
                    attr("a_str", AttrValue::Str("x".into())),
                    attr("b_int", AttrValue::I64(7)),
                    attr("c_f64", AttrValue::F64(1.5)),
                    attr("d_bool", AttrValue::Bool(true)),
                    attr("e_bytes", AttrValue::Bytes(vec![1, 2])),
                    attr(OVERFLOW_KEY, AttrValue::Str("ov".into())),
                ],
            ),
            rec_attrs(0, 101, "drop beta", Vec::new()),
            rec_attrs(
                0,
                102,
                "keep gamma",
                vec![attr("a_str", AttrValue::Str("y".into()))],
            ),
            rec_attrs(
                0,
                103,
                "",
                vec![
                    attr("b_int", AttrValue::I64(-3)),
                    attr("c_f64", AttrValue::F64(2.5)),
                ],
            ),
            rec_attrs(
                0,
                104,
                "keep delta",
                vec![
                    attr("a_str", AttrValue::Str(String::new())),
                    attr("d_bool", AttrValue::Bool(false)),
                ],
            ),
            rec_attrs(
                1,
                105,
                "drop epsilon",
                vec![attr("e_bytes", AttrValue::Bytes(vec![9]))],
            ),
            rec_attrs(
                1,
                106,
                "keep zeta",
                vec![attr(OVERFLOW_KEY, AttrValue::Str("ov2".into()))],
            ),
            rec_attrs(1, 107, "drop eta", Vec::new()),
            rec_attrs(
                1,
                108,
                "keep theta",
                vec![attr("b_int", AttrValue::I64(i64::MIN))],
            ),
            {
                let mut r = rec_attrs(1, 109, "", Vec::new());
                r.trace_id = Some([3u8; 16]);
                r.span_id = Some([4u8; 8]);
                r
            },
            rec_attrs(
                1,
                110,
                "keep iota",
                vec![attr("c_f64", AttrValue::F64(-0.0))],
            ),
            {
                let mut r = rec_attrs(
                    1,
                    111,
                    "drop kappa",
                    vec![
                        attr("a_str", AttrValue::Str("z".into())),
                        attr("d_bool", AttrValue::Bool(true)),
                    ],
                );
                r.trace_id = Some([5u8; 16]);
                r
            },
        ]
    }

    /// Which columns a [`ColumnSelection`] under test keeps, so the comparison
    /// knows when a `None` from the view is the projection working rather than a
    /// disagreement with the row path.
    struct Kept {
        observed_ts: bool,
        severity_num: bool,
        flags: bool,
    }

    /// Reads every field of surviving row `i` through the view and asserts it
    /// equals the corresponding field of `r`, the record the row exit produced
    /// for the same block at the same position.
    fn assert_row_matches(view: &ColumnarBlockView<'_>, i: usize, r: &LogRecord, kept: &Kept) {
        assert_eq!(view.ts(i), Some(r.ts_ns), "ts at surviving row {i}");
        assert_eq!(
            view.stream_id(i),
            Some(&r.stream_id),
            "stream_id at surviving row {i}"
        );
        assert_eq!(
            view.stream_attrs(i),
            Some(r.stream_attrs.as_slice()),
            "stream_attrs at surviving row {i}"
        );
        // The blob is borrowed from STREAM_DIR, so two rows of the same stream
        // read the identical slice rather than two clones.
        assert_eq!(
            view.stream_attrs(i),
            view.stream_ref(i).and_then(|s| view.stream_attrs_of(s)),
            "row and stream_ref forms of the blob must agree"
        );

        assert_eq!(
            view.observed_ts(i),
            kept.observed_ts.then_some(r.observed_ts_ns),
            "observed_ts at surviving row {i}"
        );
        assert_eq!(
            view.severity_num(i),
            kept.severity_num.then_some(i64::from(r.severity_num)),
            "severity_num at surviving row {i}"
        );
        assert_eq!(
            view.flags(i),
            kept.flags.then_some(i64::from(r.flags)),
            "flags at surviving row {i}"
        );

        // `severity_text` and `body` are always-present columns whose value can
        // be empty; the row path reads an absent one as `""` and so does this.
        assert_eq!(
            view.severity_text(i).unwrap_or(b""),
            r.severity_text.as_bytes(),
            "severity_text at surviving row {i}"
        );
        assert_eq!(
            view.body(i).unwrap_or(b""),
            r.body.as_bytes(),
            "body at surviving row {i}"
        );
        assert_eq!(
            view.trace_id(i).map(<[u8]>::to_vec),
            r.trace_id.map(|t| t.to_vec()),
            "trace_id at surviving row {i}"
        );
        assert_eq!(
            view.span_id(i).map(<[u8]>::to_vec),
            r.span_id.map(|s| s.to_vec()),
            "span_id at surviving row {i}"
        );

        // Attributes reachable through FIELD_DIR columns, resolved once for the
        // block and read per row. The row path pushes these in FIELD_DIR order
        // and then appends whatever `attrs_raw` decoded, so the view-derived
        // list must be a prefix of the record's and the remainder must be
        // overflow keys only.
        let mut got: Vec<(String, AttrValue)> = Vec::new();
        for (name, col) in view.attr_columns() {
            let v = match col.ty {
                FieldType::Str => view
                    .bytes_at(col.column_id, i)
                    .and_then(|b| String::from_utf8(b.to_vec()).ok())
                    .map(AttrValue::Str),
                FieldType::Bytes => view
                    .bytes_at(col.column_id, i)
                    .map(|b| AttrValue::Bytes(b.to_vec())),
                FieldType::I64 => view.i64_at(col.column_id, i).map(AttrValue::I64),
                FieldType::F64 => view
                    .f64_bits_at(col.column_id, i)
                    .map(|bits| AttrValue::F64(f64::from_bits(bits))),
                FieldType::Bool => view.bool_at(col.column_id, i).map(AttrValue::Bool),
            };
            if let Some(v) = v {
                // Resolution by key must land on the same column.
                assert_eq!(
                    view.resolve_attr(name, col.ty),
                    Some(col),
                    "resolve_attr({name}) must match the enumerated column"
                );
                got.push((name.to_string(), v));
            }
        }
        assert!(
            got.len() <= r.attrs.len(),
            "view produced {} attrs for surviving row {i}, record has {}",
            got.len(),
            r.attrs.len()
        );
        let (prefix, tail) = r.attrs.split_at(got.len());
        for (g, w) in got.iter().zip(prefix) {
            assert_eq!(g.0, w.0, "attr key at surviving row {i}");
            assert!(
                attr_value_eq(&g.1, &w.1),
                "attr {} at surviving row {i}: view {:?} vs record {:?}",
                g.0,
                g.1,
                w.1
            );
        }
        for (k, _) in tail {
            assert_eq!(
                k, OVERFLOW_KEY,
                "record attr {k} at surviving row {i} is not reachable through a \
                 FIELD_DIR column and is not the known overflow key"
            );
        }
        assert!(
            view.resolve_attr(OVERFLOW_KEY, FieldType::Str).is_none(),
            "the overflow key must have no FIELD_DIR column, or the corpus no \
             longer exercises attrs_raw"
        );
    }

    /// Drives the row exit and the columnar exit over the same object with the
    /// same predicate and column selection, block for block, and asserts they
    /// agree on every field of every surviving row. Returns each block's
    /// `attrs_raw` page flag paired with whether that block's records actually
    /// carried an overflow attribute.
    fn compare_exits(
        reader: &RlogReader<'_>,
        obj: &[u8],
        pred: &Predicate,
        columns: &ColumnSelection,
        kept: &Kept,
        case: &str,
    ) -> Vec<(bool, bool)> {
        let mut rows_cursor = reader
            .scan_blocks(pred, &[], columns)
            .expect("open row cursor");
        let mut col_cursor = reader
            .scan_blocks(pred, &[], columns)
            .expect("open columnar cursor");
        let mut flags = Vec::new();
        loop {
            let records = rows_cursor.next_block(obj).expect("row exit");
            let view = col_cursor.next_block_columnar(obj).expect("columnar exit");
            match (records, view) {
                (None, None) => break,
                (Some(records), Some(view)) => {
                    assert_eq!(
                        view.surviving_count(),
                        records.len(),
                        "{case}: surviving row count must equal the row exit's record count"
                    );
                    assert!(
                        view.record_count() >= view.surviving_count(),
                        "{case}: surviving rows cannot outnumber the block's rows"
                    );
                    for (i, r) in records.iter().enumerate() {
                        assert_row_matches(&view, i, r, kept);
                    }
                    // Out-of-range reads are `None`, never another row's data.
                    assert_eq!(view.ts(view.surviving_count()), None, "{case}");

                    // The gather iterators walk the same cells in the same order
                    // as the per-row readers.
                    let ts_iter: Vec<Option<i64>> = view.iter_ts().collect();
                    let ts_rows: Vec<Option<i64>> =
                        (0..view.surviving_count()).map(|i| view.ts(i)).collect();
                    assert_eq!(ts_iter, ts_rows, "{case}: iter_ts");
                    let body_iter: Vec<Option<&[u8]>> = view.iter_body().collect();
                    let body_rows: Vec<Option<&[u8]>> =
                        (0..view.surviving_count()).map(|i| view.body(i)).collect();
                    assert_eq!(body_iter, body_rows, "{case}: iter_body");
                    assert_eq!(
                        view.iter_stream_ref().collect::<Vec<_>>(),
                        (0..view.surviving_count())
                            .map(|i| view.stream_ref(i))
                            .collect::<Vec<_>>(),
                        "{case}: iter_stream_ref"
                    );

                    if columns.is_all() {
                        assert_eq!(
                            view.pages_skipped(),
                            0,
                            "{case}: an all-columns decode skips no page"
                        );
                    }
                    assert!(
                        view.pages_decoded() > 0,
                        "{case}: a surviving block decodes at least one page"
                    );

                    let record_has_overflow = records
                        .iter()
                        .any(|r| r.attrs.iter().any(|(k, _)| k == OVERFLOW_KEY));
                    flags.push((view.has_attrs_raw_page(), record_has_overflow));
                }
                (records, view) => panic!(
                    "{case}: exits disagree on exhaustion (row exit {}, columnar exit {})",
                    records.is_some(),
                    view.is_some()
                ),
            }
        }
        assert_eq!(
            rows_cursor.stats(),
            col_cursor.stats(),
            "{case}: both exits must account for the same blocks and pages"
        );
        flags
    }

    /// Acceptance test for the columnar block view (issue #413): every field of
    /// every surviving row, read through the view, equals the corresponding
    /// field of the `LogRecord` the row exit produces for the same block at the
    /// same position -- across an all-columns and a projected decode, and a
    /// match-everything and a selective predicate.
    ///
    /// The selective predicate is the load-bearing half: it keeps a
    /// non-contiguous subset of each block's rows, so an accessor that read its
    /// index as a raw block row position would disagree here.
    #[test]
    fn columnar_view_matches_rebuilt_records() {
        let cfg = RlogConfig {
            block_target_records: 3,
            max_dynamic_columns: COLUMNAR_MAX_COLUMNS,
            ..RlogConfig::default()
        };
        let obj = build(cfg, columnar_corpus());
        let reader = RlogReader::new(&obj, &cfg).expect("open object");

        let all_rows = Predicate::And(Vec::new());
        let selective = Predicate::HasWord {
            field: FieldSel::Body,
            word: "keep".into(),
        };
        // Projected: no observed_ts, severity_num, flags, trace_id or span_id,
        // and only one of the five dynamic columns.
        let projected = ColumnSelection::fixed_only()
            .with_body()
            .with_severity_text()
            .with_attr("a_str");
        let all_kept = Kept {
            observed_ts: true,
            severity_num: true,
            flags: true,
        };
        let projected_kept = Kept {
            observed_ts: false,
            severity_num: false,
            flags: false,
        };

        let flags = compare_exits(
            &reader,
            &obj,
            &all_rows,
            &ColumnSelection::all(),
            &all_kept,
            "match-all / all-columns",
        );
        // With a match-everything predicate every row survives, so the row
        // exit's records cover the whole block and the `attrs_raw` page flag is
        // exactly "some record here spilled".
        assert!(flags.len() > 1, "corpus must produce several blocks");
        for (has_page, record_has_overflow) in &flags {
            assert_eq!(
                has_page, record_has_overflow,
                "attrs_raw page presence must match the block's spilled records"
            );
        }
        assert!(
            flags.iter().any(|(p, _)| *p),
            "corpus must include a block whose records spill to attrs_raw"
        );
        assert!(
            flags.iter().any(|(p, _)| !*p),
            "corpus must include a block with no attrs_raw spill"
        );

        compare_exits(
            &reader,
            &obj,
            &selective,
            &ColumnSelection::all(),
            &all_kept,
            "selective / all-columns",
        );
        compare_exits(
            &reader,
            &obj,
            &all_rows,
            &projected,
            &projected_kept,
            "match-all / projected",
        );
        let selective_projected = compare_exits(
            &reader,
            &obj,
            &selective,
            &projected,
            &projected_kept,
            "selective / projected",
        );
        // The projected decode leaves `attrs_raw` decodable (naming an attribute
        // key keeps it, since the key may have spilled), so the page flag is
        // still answered from descriptors and still varies across blocks.
        assert!(selective_projected.iter().any(|(p, _)| *p));
        assert!(selective_projected.iter().any(|(p, _)| !*p));

        // A projected decode really did skip pages, so the comparison above was
        // over a partial decode rather than a silently-full one.
        let mut cursor = reader
            .scan_blocks(&all_rows, &[], &projected)
            .expect("open cursor");
        while cursor
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .is_some()
        {}
        assert!(
            cursor.stats().pages_skipped > 0,
            "the projected selection must leave pages undecoded"
        );
    }

    /// `ColumnarBlockView::str_dict` hands out a dictionary-encoded string
    /// column intact, and fusing it back through the dictionary yields exactly
    /// the per-row bytes the byte accessor returns for the same surviving rows
    /// (ADR-0099 decision 4, deliverable 5). Presence is carried by the id
    /// vector, so a surviving row whose column value is absent reads `None`
    /// through both, an empty-string value survives as an empty dictionary
    /// entry, and a plain-page column exposes no dictionary at all.
    ///
    /// The selective predicate drops a middle row, so the surviving rows are
    /// non-contiguous: a dictionary that addressed ids by raw block row rather
    /// than by surviving-row index would disagree here.
    #[test]
    fn columnar_str_dict_fuses_to_byte_accessor() {
        let cfg = RlogConfig {
            block_target_records: 8,
            max_dynamic_columns: COLUMNAR_MAX_COLUMNS,
            ..RlogConfig::default()
        };
        // "svc" is low cardinality (3 distinct across 6 present of 8 rows -> a
        // dictionary page), carries an empty-string value, and is absent from
        // two rows. Bodies are all distinct -> a plain page, and worded so
        // `keep` drops one row to make the survivors non-contiguous.
        let recs = vec![
            rec_attrs(
                0,
                100,
                "keep a",
                vec![attr("svc", AttrValue::Str("api".into()))],
            ),
            rec_attrs(
                0,
                101,
                "keep b",
                vec![attr("svc", AttrValue::Str("".into()))],
            ),
            rec_attrs(0, 102, "drop c", Vec::new()),
            rec_attrs(
                0,
                103,
                "keep d",
                vec![attr("svc", AttrValue::Str("api".into()))],
            ),
            rec_attrs(
                0,
                104,
                "keep e",
                vec![attr("svc", AttrValue::Str("db".into()))],
            ),
            rec_attrs(0, 105, "keep f", Vec::new()),
            rec_attrs(
                0,
                106,
                "keep g",
                vec![attr("svc", AttrValue::Str("db".into()))],
            ),
            rec_attrs(
                0,
                107,
                "keep h",
                vec![attr("svc", AttrValue::Str("api".into()))],
            ),
        ];
        let obj = build(cfg, recs);
        let reader = RlogReader::new(&obj, &cfg).expect("open object");
        let pred = Predicate::HasWord {
            field: FieldSel::Body,
            word: "keep".into(),
        };

        let mut cursor = reader
            .scan_blocks(&pred, &[], &ColumnSelection::all())
            .expect("open cursor");
        let mut saw_dict = false;
        let mut saw_absent = false;
        let mut saw_empty = false;
        while let Some(view) = cursor.next_block_columnar(&obj).expect("columnar exit") {
            // A plain-page column (all-distinct bodies) exposes no dictionary.
            assert!(
                view.str_dict(COL_BODY).is_none(),
                "a plain page must not be forced into a dictionary"
            );

            let svc = view
                .resolve_attr("svc", FieldType::Str)
                .expect("svc has a FIELD_DIR column");
            let dict = view
                .str_dict(svc.column_id)
                .expect("the svc column is a dictionary page");
            saw_dict = true;
            assert_eq!(
                dict.len(),
                view.surviving_count(),
                "the dictionary column spans exactly the surviving rows"
            );

            for i in 0..view.surviving_count() {
                let via_bytes = view.bytes_at(svc.column_id, i);
                // Fusing the dictionary form equals the byte accessor cell.
                assert_eq!(dict.value_at(i), via_bytes, "surviving row {i}");
                match dict.id_at(i) {
                    Some(id) => {
                        let entry = dict.dict()[id as usize].as_slice();
                        assert_eq!(Some(entry), via_bytes, "id maps through dict() at {i}");
                        if entry.is_empty() {
                            saw_empty = true;
                        }
                    }
                    None => {
                        saw_absent = true;
                        assert_eq!(via_bytes, None, "an absent id must read absent bytes");
                    }
                }
            }
            // The gather form agrees with the per-row form.
            let ids_iter: Vec<Option<u32>> = dict.iter_ids().collect();
            let ids_rows: Vec<Option<u32>> =
                (0..view.surviving_count()).map(|i| dict.id_at(i)).collect();
            assert_eq!(ids_iter, ids_rows, "iter_ids");
        }
        assert!(
            saw_dict,
            "the corpus must produce a dictionary-encoded svc page"
        );
        assert!(saw_absent, "a surviving row must carry an absent svc value");
        assert!(
            saw_empty,
            "a surviving row must carry an empty-string svc value"
        );
    }

    /// A four-row single-block fixture whose rows all match `keep`, carrying one
    /// column of every declared attribute type. Used by the cursor tests below.
    fn cursor_fixture() -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 8,
            ..RlogConfig::default()
        };
        let recs = vec![
            rec_attrs(
                0,
                100,
                "keep a",
                vec![
                    attr("b_int", AttrValue::I64(1)),
                    attr("c_f64", AttrValue::F64(1.0)),
                    attr("d_bool", AttrValue::Bool(true)),
                    attr("a_str", AttrValue::Str("p".into())),
                    attr("e_bytes", AttrValue::Bytes(vec![1])),
                ],
            ),
            rec_attrs(0, 101, "keep b", vec![attr("b_int", AttrValue::I64(2))]),
            rec_attrs(0, 102, "keep c", vec![attr("c_f64", AttrValue::F64(3.0))]),
            // Row 3 sets no attribute at all: every dynamic column has a null
            // cell here.
            rec_attrs(0, 103, "keep d", Vec::new()),
        ];
        build(cfg, recs)
    }

    fn cursor_reader_cfg() -> RlogConfig {
        RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 8,
            ..RlogConfig::default()
        }
    }

    /// Deliverable 1 (#875): a cursor resolves its column once per block, so the
    /// scan does O(columns) resolutions, never O(rows x columns). Pins the exact
    /// count for a known 4-row, single-block fixture. The per-cell accessor --
    /// the pre-change path -- is shown resolving the column on every cell (R per
    /// column), so the exact-count assertion here fails against it: flipping the
    /// cursor loop's `cur.at(i)` back to `view.i64_at(col, i)` turns 6 into
    /// 6 * R.
    #[test]
    fn cursor_resolves_each_column_once_per_block() {
        let obj = cursor_fixture();
        let cfg = cursor_reader_cfg();
        let reader = RlogReader::new(&obj, &cfg).expect("open object");
        let pred = Predicate::And(Vec::new());
        let mut cursor = reader
            .scan_blocks(&pred, &[], &ColumnSelection::all())
            .expect("open cursor");
        let view = cursor
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let rows = view.surviving_count();
        assert_eq!(rows, 4, "match-all keeps every row of the single block");

        // Resolving these does not touch the block's column maps (FIELD_DIR
        // lookups), so they do not move the counter.
        let b_int = view.resolve_attr("b_int", FieldType::I64).expect("b_int");
        let c_f64 = view.resolve_attr("c_f64", FieldType::F64).expect("c_f64");
        let d_bool = view
            .resolve_attr("d_bool", FieldType::Bool)
            .expect("d_bool");
        let a_str = view.resolve_attr("a_str", FieldType::Str).expect("a_str");
        let e_bytes = view
            .resolve_attr("e_bytes", FieldType::Bytes)
            .expect("e_bytes");

        let base = view.column_lookups();
        // ts plus one column of every declared type: six distinct columns.
        let cur_ts = view.ts_cursor();
        let cur_bi = view.i64_cursor(b_int.column_id);
        let cur_cf = view.f64_bits_cursor(c_f64.column_id);
        let cur_db = view.bool_cursor(d_bool.column_id);
        let cur_as = view.bytes_cursor(a_str.column_id);
        let cur_eb = view.bytes_cursor(e_bytes.column_id);
        const COLUMNS: u64 = 6;
        // Walking every surviving row through the cursors adds no resolution.
        for i in 0..rows {
            let _ = (
                cur_ts.at(i),
                cur_bi.at(i),
                cur_cf.at(i),
                cur_db.at(i),
                cur_as.at(i),
                cur_eb.at(i),
            );
        }
        let cursor_lookups = view.column_lookups() - base;
        assert_eq!(
            cursor_lookups, COLUMNS,
            "one resolution per column, independent of the {rows} rows"
        );

        // The pre-change shape: the per-cell accessor resolves the column on
        // every cell. Over the two i64 columns (ts, b_int) that is exactly 2 per
        // row -- O(rows x columns), the cost this change deletes.
        let before = view.column_lookups();
        for i in 0..rows {
            let _ = view.i64_at(COL_TS, i);
            let _ = view.i64_at(b_int.column_id, i);
        }
        let per_cell = view.column_lookups() - before;
        assert_eq!(
            per_cell,
            2 * rows as u64,
            "per-cell accessor resolves once per cell"
        );
        assert!(
            per_cell > COLUMNS,
            "the pre-change per-cell path fails the O(columns) bound"
        );
    }

    /// Deliverable 1 correctness bar (#875): a cursor keeps an absent column and
    /// a present-but-null cell distinguishable, both directions. Both read as
    /// `None` at the value level -- byte-identical to the per-cell accessor the
    /// scan used before -- while `is_column_present` tells the two apart.
    #[test]
    fn cursor_distinguishes_absent_column_from_null_cell() {
        let obj = cursor_fixture();
        let cfg = cursor_reader_cfg();
        let reader = RlogReader::new(&obj, &cfg).expect("open object");
        let pred = Predicate::And(Vec::new());
        let mut cursor = reader
            .scan_blocks(&pred, &[], &ColumnSelection::all())
            .expect("open cursor");
        let view = cursor
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let b_int = view.resolve_attr("b_int", FieldType::I64).expect("b_int");

        // Present column, null cell: b_int is decoded but row 3 sets no value.
        let bi = view.i64_cursor(b_int.column_id);
        assert!(bi.is_column_present(), "b_int is a decoded column");
        assert_eq!(bi.at(0), Some(1), "row 0 sets b_int");
        assert_eq!(bi.at(3), None, "row 3 has a null b_int cell");

        // Absent column: b_int has no f64 storage, so an f64 cursor over its id
        // resolves to no column at all.
        let bi_as_f64 = view.f64_bits_cursor(b_int.column_id);
        assert!(
            !bi_as_f64.is_column_present(),
            "there is no f64 column for b_int"
        );
        assert_eq!(bi_as_f64.at(0), None, "an absent column reads None");

        // Both directions read None as a value, and the two are still
        // distinguishable through `is_column_present`.
        assert_eq!(bi.at(3), None, "null cell reads None");
        assert_eq!(bi_as_f64.at(0), None, "absent column reads None");
        assert_ne!(
            bi.is_column_present(),
            bi_as_f64.is_column_present(),
            "null cell and absent column stay distinguishable"
        );
    }

    /// Deliverable correctness bar (#875): the f64 cursor round-trips the exact
    /// stored bit pattern, so `-0.0` stays distinct from `+0.0` and a NaN payload
    /// survives. `f64_bits_at`/the cursor return bits for exactly this reason.
    #[test]
    fn f64_cursor_preserves_exact_bit_pattern() {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 8,
            ..RlogConfig::default()
        };
        let nan_payload = f64::from_bits(f64::NAN.to_bits() | 0x7);
        let recs = vec![
            rec_attrs(0, 100, "keep a", vec![attr("v", AttrValue::F64(-0.0))]),
            rec_attrs(0, 101, "keep b", vec![attr("v", AttrValue::F64(0.0))]),
            rec_attrs(
                0,
                102,
                "keep c",
                vec![attr("v", AttrValue::F64(nan_payload))],
            ),
            rec_attrs(0, 103, "keep d", vec![attr("v", AttrValue::F64(1.5))]),
        ];
        let obj = build(cfg, recs);
        let reader = RlogReader::new(&obj, &cfg).expect("open object");
        let pred = Predicate::And(Vec::new());
        let mut cursor = reader
            .scan_blocks(&pred, &[], &ColumnSelection::all())
            .expect("open cursor");
        let view = cursor
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let v = view.resolve_attr("v", FieldType::F64).expect("v col");
        let cur = view.f64_bits_cursor(v.column_id);
        assert_eq!(cur.at(0), Some((-0.0f64).to_bits()), "-0.0 bit pattern");
        assert_eq!(cur.at(1), Some(0.0f64.to_bits()), "+0.0 bit pattern");
        assert_ne!(
            cur.at(0),
            cur.at(1),
            "-0.0 and +0.0 are distinct bit patterns"
        );
        assert_eq!(
            cur.at(2),
            Some(nan_payload.to_bits()),
            "NaN payload preserved"
        );
        assert_eq!(cur.at(3), Some(1.5f64.to_bits()));
    }

    /// Both exits reject a corrupt block with the same typed error, and neither
    /// panics. The flipped byte is inside block 0's stored extent, so its
    /// crc32c no longer matches its SKIP_IDX entry.
    #[test]
    fn corrupt_block_is_a_typed_error_through_both_exits() {
        let cfg = RlogConfig {
            block_target_records: 3,
            ..RlogConfig::default()
        };
        let good = build(cfg, columnar_corpus());
        let blocks_offset = open(&good)
            .expect("open footer")
            .section(kind::BLOCKS)
            .expect("BLOCKS section")
            .offset;
        let skip = skip_of(&good);
        let entry = skip.l0.first().expect("a first block");
        // The block's first byte. Under version 4 `block_len` spans the whole
        // row group (the block's pages are interleaved with its neighbours'),
        // so a midpoint offset would land in another block; `block_offset` is
        // the first block's own first page either way (ADR-0699 decision 1).
        let at = usize::try_from(blocks_offset + entry.block_offset).expect("offset fits");
        let mut obj = good.clone();
        obj[at] ^= 0xff;

        let pred = Predicate::And(Vec::new());
        let reader = RlogReader::new(&obj, &cfg).expect("footer and sections are intact");
        let err = reader
            .scan_blocks(&pred, &[], &ColumnSelection::all())
            .expect("open cursor")
            .next_block(&obj)
            .expect_err("row exit rejects the corrupt block");
        assert!(matches!(err, LogSegError::Corrupted(_)), "{err:?}");
        let err = reader
            .scan_blocks(&pred, &[], &ColumnSelection::all())
            .expect("open cursor")
            .next_block_columnar(&obj)
            .expect_err("columnar exit rejects the corrupt block");
        assert!(matches!(err, LogSegError::Corrupted(_)), "{err:?}");

        // Same object, uncorrupted: both exits succeed, so the assertions above
        // are about the flipped byte and not about the fixture.
        let reader = RlogReader::new(&good, &cfg).expect("open");
        assert!(
            reader
                .scan_blocks(&pred, &[], &ColumnSelection::all())
                .expect("open cursor")
                .next_block(&good)
                .expect("row exit")
                .is_some()
        );
        assert!(
            reader
                .scan_blocks(&pred, &[], &ColumnSelection::all())
                .expect("open cursor")
                .next_block_columnar(&good)
                .expect("columnar exit")
                .is_some()
        );
    }

    /// `ColumnarBlockView::decoded_bytes` reports the block's real resident
    /// footprint: it counts every string cell's own allocation, not just the
    /// column spines, and it tracks what the column selection actually decoded.
    ///
    /// The bodies are 4 KiB each and all distinct, so the body column is a plain
    /// page and its per-cell term (8 x 4096 = 32768 bytes) dwarfs the spines a
    /// `record_count`-based estimate would see. Distinct is deliberate: an
    /// identical-body column would dictionary-encode to one 4 KiB entry plus
    /// eight narrow ids (ADR-0099 decision 4), which is the case
    /// `dict_decoded_bytes_reports_dictionary_footprint` pins. A figure that
    /// counted spines only would land under the cell total and fail the first
    /// assertion; a figure that ignored the column filter would fail the last
    /// two.
    #[test]
    fn decoded_bytes_counts_cells_and_follows_the_column_selection() {
        const ROWS: i64 = 8;
        const BODY: usize = 4096;
        let cfg = RlogConfig {
            block_target_records: 64,
            ..RlogConfig::default()
        };
        // Distinct bodies (a per-row suffix) keep the column a plain page, so the
        // per-cell accounting under test is exercised rather than dictionary
        // deduplication.
        let recs: Vec<LogRecord> = (0..ROWS)
            .map(|i| {
                let body = format!("{}{i:08}", "b".repeat(BODY - 8));
                rec_attrs(0, i, &body, Vec::new())
            })
            .collect();
        let obj = build(cfg, recs);
        let pred = Predicate::And(Vec::new());
        let reader = RlogReader::new(&obj, &cfg).expect("open");

        let bytes_with = |sel: &ColumnSelection| -> usize {
            let mut cursor = reader.scan_blocks(&pred, &[], sel).expect("open cursor");
            let view = cursor
                .next_block_columnar(&obj)
                .expect("columnar exit")
                .expect("one block");
            assert_eq!(view.surviving_count(), ROWS as usize);
            view.decoded_bytes()
        };

        let with_body = bytes_with(&ColumnSelection::fixed_only().with_body());
        let cells = ROWS as usize * BODY;
        assert!(
            with_body > cells,
            "the body column's cells alone are {cells} bytes; got {with_body}"
        );

        let fixed_only = bytes_with(&ColumnSelection::fixed_only());
        assert!(
            fixed_only < cells,
            "a decode that skipped the body column must not carry its \
             {cells} bytes of cells; got {fixed_only}"
        );
        assert!(
            with_body - fixed_only >= cells,
            "adding the body column must add at least its cells \
             ({cells}); with_body={with_body}, fixed_only={fixed_only}"
        );
    }
}
