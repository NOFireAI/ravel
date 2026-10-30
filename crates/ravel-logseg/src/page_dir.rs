//! PAGE_DIR section (docs/log-segment-format.md "PAGE_DIR", ADR-0699
//! decision 2).
//!
//! Version 4 stores a row group's pages column-major, so a block is no longer a
//! contiguous byte range and its pages are no longer described by a header
//! sitting in front of them. PAGE_DIR is where those descriptors live instead:
//! per row group, per column chunk, per page, the descriptor a version-3 block
//! header carried plus the page's absolute offset (derived: a chunk's pages are
//! contiguous from the chunk's `offset` in listed order) and a per-page crc32c.
//!
//! The per-page crc is what makes a page-subset read legal under ADR-0010
//! section 4: a reader that fetches two of a hundred columns cannot verify the
//! block crc without fetching the block, so every byte it interprets is covered
//! by a checksum on its own access path instead. The section as a whole is
//! covered by its `Section.crc32c`, so the `enc`/`comp` tags and every offset
//! and length here are verified before anything is located through them.
//!
//! Every decode path treats the stored bytes as untrusted: counts are checked
//! against caps derived from the block-level [`crate::block::MAX_PAGES`] and
//! [`crate::reader::MAX_BLOCKS`], the group/chunk/page ordering the writer
//! guarantees is re-checked rather than assumed, and every violation is
//! [`LogSegError::Corrupted`], never a panic and never a silent misread.

use std::collections::{HashMap, HashSet};

use crate::block::MAX_PAGES;
use crate::encoding::Enc;
use crate::error::LogSegError;
use crate::field_dir::FieldDir;
use crate::page::PageDesc;
use crate::reader::MAX_BLOCKS;
use crate::record::{COL_ATTRS_RAW, COL_BODY, COL_SEVERITY_TEXT, FieldType};
use crate::varint::{get_uvarint, put_uvarint};

/// Upper bound on the number of pages one block may contribute to one column
/// chunk: a presence bitmap page and a value page
/// (docs/log-segment-format.md "BLOCKS").
const MAX_PAGES_PER_BLOCK_PER_COLUMN: u64 = 2;

/// One page of one column chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageEntry {
    /// Block index within the group, ascending. Two entries may share a block:
    /// a partially present column contributes its presence bitmap page and its
    /// value page, in that order.
    pub block: u32,
    pub enc: Enc,
    pub comp: u8,
    /// Stored page bytes.
    pub len: u64,
    /// Page bytes before compression.
    pub uncomp_len: u64,
    /// crc32c over the page's stored bytes, verified before decompressing it.
    pub crc32c: u32,
}

impl PageEntry {
    /// The page descriptor a version-3 block header would have carried for this
    /// page, for `column_id`. The stored bytes are identical either way: only
    /// where the descriptor lives changed.
    pub fn desc(&self, column_id: u32) -> PageDesc {
        PageDesc {
            column_id,
            enc: self.enc,
            comp: self.comp,
            len: self.len,
            uncomp_len: self.uncomp_len,
        }
    }

    /// Appends the entry's PAGE_DIR encoding: `block` varint, `enc`, `comp`,
    /// `len` varint, `uncomp_len` varint, then the crc32c little-endian.
    fn encode_into(&self, out: &mut Vec<u8>) {
        put_uvarint(out, u64::from(self.block));
        out.push(self.enc.to_u8());
        out.push(self.comp);
        put_uvarint(out, self.len);
        put_uvarint(out, self.uncomp_len);
        out.extend_from_slice(&self.crc32c.to_le_bytes());
    }

    /// Bytes this entry takes in the uncompressed PAGE_DIR section.
    pub fn encoded_len(&self) -> u64 {
        let mut out = Vec::with_capacity(32);
        self.encode_into(&mut out);
        out.len() as u64
    }
}

/// One column chunk: every page one column has in one row group, contiguous.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkEntry {
    pub column_id: u32,
    /// Absolute offset of the chunk's first page into the BLOCKS section.
    pub offset: u64,
    /// The chunk's pages, contiguous from `offset` in this order.
    pub pages: Vec<PageEntry>,
}

impl ChunkEntry {
    /// The chunk's byte extent in BLOCKS: `(offset, total stored length)`.
    /// `None` when the lengths overflow, which decode already rejects.
    pub fn extent(&self) -> Option<(u64, u64)> {
        let mut len = 0u64;
        for p in &self.pages {
            len = len.checked_add(p.len)?;
        }
        Some((self.offset, len))
    }

    /// The chunk's row group dictionary page (tag 12), if it has one. Decode
    /// admits one only as the first page, which sits at the chunk's `offset`.
    pub fn dict_page(&self) -> Option<&PageEntry> {
        self.pages.first().filter(|p| p.enc == Enc::DictPage)
    }

    /// The absolute offset of each page, derived by running `offset` forward
    /// over the preceding pages' stored lengths.
    pub fn page_offsets(&self) -> Option<Vec<u64>> {
        let mut out = Vec::with_capacity(self.pages.len());
        let mut at = self.offset;
        for p in &self.pages {
            out.push(at);
            at = at.checked_add(p.len)?;
        }
        Some(out)
    }
}

/// One row group: a run of consecutive blocks whose pages are stored
/// column-major (ADR-0699 decision 1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupEntry {
    /// Index of the group's first block, in whole-object block numbering.
    pub first_block: u32,
    pub block_count: u32,
    /// The group's column chunks, in strictly ascending `column_id` order.
    pub chunks: Vec<ChunkEntry>,
}

/// The decoded PAGE_DIR section.
///
/// Carries an in-memory index of where each block's pages start in each column
/// chunk ([`BlockIndex`]), so [`Self::block_pages`] and
/// [`Self::block_dict_pages`] slice a chunk instead of scanning it. The index
/// is built by [`Self::new`] and [`Self::decode`]. A chunk whose group's block
/// or chunk count, or whose own page count, no longer matches the index is
/// scanned instead ([`BlockIndex::runs`],
/// `index_tests::a_stale_index_falls_back_to_the_scan`); an in-place edit of
/// `groups` that keeps every count must be followed by [`Self::reindex`].
#[derive(Clone, Default)]
pub struct PageDir {
    pub groups: Vec<GroupEntry>,
    index: BlockIndex,
}

/// Equality and debug output are over `groups` only: the index is derived from
/// them and is not part of the section.
impl PartialEq for PageDir {
    fn eq(&self, other: &Self) -> bool {
        self.groups == other.groups
    }
}

impl Eq for PageDir {}

impl std::fmt::Debug for PageDir {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PageDir")
            .field("groups", &self.groups)
            .finish()
    }
}

/// Per group, per column chunk, where each block's run of pages starts.
///
/// Memory: one `(u32, u32)` per (column chunk, block) plus one more per chunk,
/// that is `8 * (block_count + 1)` bytes for each chunk of a group, plus a
/// `Vec` header per group and a page count and box per chunk. A chunk the
/// index cannot describe (see [`ChunkRuns::build`]) keeps no runs and is
/// scanned.
#[derive(Clone, Default)]
struct BlockIndex {
    groups: Vec<GroupIndex>,
}

#[derive(Clone)]
struct GroupIndex {
    block_count: u32,
    chunks: Vec<ChunkIndex>,
}

#[derive(Clone)]
struct ChunkIndex {
    /// The chunk's page count when the index was built.
    pages: usize,
    runs: Option<ChunkRuns>,
}

/// `starts[b]` is `(index of block b's first page, that page's offset relative
/// to the chunk's offset)`; `starts[block_count]` is `(page count, chunk
/// length)`, so block `b`'s pages are `starts[b].0..starts[b + 1].0`.
#[derive(Clone)]
struct ChunkRuns {
    starts: Box<[(u32, u32)]>,
}

impl ChunkRuns {
    /// The runs of a chunk in the shape decode admits: an optional dictionary
    /// page first, then no other dictionary page, every page's block inside
    /// the group and the blocks non-decreasing. `None` for any other chunk,
    /// or one whose page count, length, or absolute end does not fit, and the
    /// lookups scan that chunk exactly as they did before the index.
    fn build(chunk: &ChunkEntry, block_count: u32) -> Option<Self> {
        let lead = usize::from(chunk.dict_page().is_some());
        let pages = chunk.pages.get(lead..)?;
        let mut total = 0u64;
        for p in &chunk.pages {
            total = total.checked_add(p.len)?;
        }
        chunk.offset.checked_add(total)?;
        u32::try_from(total).ok()?;
        u32::try_from(chunk.pages.len()).ok()?;
        let mut starts = Vec::with_capacity(block_count as usize + 1);
        let mut i = lead;
        let mut rel = chunk.pages.get(..lead)?.iter().map(|p| p.len).sum::<u64>();
        let mut prev = 0u32;
        for p in pages {
            if p.enc == Enc::DictPage || p.block >= block_count || p.block < prev {
                return None;
            }
            prev = p.block;
        }
        for b in 0..=block_count {
            starts.push((u32::try_from(i).ok()?, u32::try_from(rel).ok()?));
            while let Some(p) = chunk.pages.get(i).filter(|p| p.block == b) {
                rel += p.len;
                i += 1;
            }
        }
        Some(ChunkRuns {
            starts: starts.into_boxed_slice(),
        })
    }

    /// Block `within`'s pages in `chunk`: `(first, end, offset of first
    /// relative to the chunk)`. Every page of the run is checked to name
    /// `within`, and its neighbours on either side to name another block, so
    /// a run read from an index that no longer matches the chunk is `None`
    /// rather than a wrong slice; the caller then scans the chunk.
    fn run(&self, chunk: &ChunkEntry, within: u32) -> Option<(usize, usize, u64)> {
        let b = within as usize;
        let (s, rel) = *self.starts.get(b)?;
        let (e, _) = *self.starts.get(b + 1)?;
        let (s, e) = (s as usize, e as usize);
        let lead = self.starts.first()?.0 as usize;
        let run = chunk.pages.get(s..e)?;
        visit(run.len());
        if run
            .iter()
            .any(|p| p.block != within || p.enc == Enc::DictPage)
        {
            return None;
        }
        if s > lead {
            visit(1);
            if chunk.pages.get(s - 1)?.block >= within {
                return None;
            }
        }
        if let Some(next) = chunk.pages.get(e) {
            visit(1);
            if next.block <= within {
                return None;
            }
        }
        Some((s, e, u64::from(rel)))
    }
}

impl BlockIndex {
    fn build(groups: &[GroupEntry]) -> Self {
        BlockIndex {
            groups: groups
                .iter()
                .map(|g| GroupIndex {
                    block_count: g.block_count,
                    chunks: g
                        .chunks
                        .iter()
                        .map(|c| ChunkIndex {
                            pages: c.pages.len(),
                            runs: ChunkRuns::build(c, g.block_count),
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    /// The runs of chunk `ci` of group `gi`, when the index still describes
    /// `group` (same block count, chunk count, and the chunk's page count).
    fn runs(&self, gi: usize, group: &GroupEntry, ci: usize) -> Option<&ChunkRuns> {
        let g = self.groups.get(gi).filter(|g| {
            g.block_count == group.block_count && g.chunks.len() == group.chunks.len()
        })?;
        let c = g.chunks.get(ci)?;
        let chunk = group.chunks.get(ci)?;
        (c.pages == chunk.pages.len()).then_some(())?;
        c.runs.as_ref()
    }
}

#[cfg(test)]
thread_local! {
    /// Pages a lookup looked at, for the cost tests.
    static PAGES_VISITED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn visit(n: usize) {
    PAGES_VISITED.with(|v| v.set(v.get() + n as u64));
}

#[cfg(not(test))]
fn visit(_: usize) {}

/// Block `within`'s pages in `chunk` by walking every page of it, the lookup
/// for a chunk [`BlockIndex`] does not describe. `None` when an offset
/// overflows.
fn scan_chunk_pages(chunk: &ChunkEntry, within: u32, out: &mut Vec<PageLoc>) -> Option<()> {
    visit(chunk.pages.len());
    let mut at = chunk.offset;
    for p in &chunk.pages {
        if p.block == within && p.enc != Enc::DictPage {
            out.push(PageLoc {
                desc: p.desc(chunk.column_id),
                crc32c: p.crc32c,
                offset: at,
            });
        }
        at = at.checked_add(p.len)?;
    }
    Some(())
}

/// One located page: its descriptor, its checksum, and where its stored bytes
/// begin in the BLOCKS section.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageLoc {
    pub desc: PageDesc,
    pub crc32c: u32,
    pub offset: u64,
}

impl PageDir {
    /// A directory over `groups`, with its block index built.
    pub fn new(groups: Vec<GroupEntry>) -> Self {
        let index = BlockIndex::build(&groups);
        PageDir { groups, index }
    }

    /// Rebuilds the block index after `groups` was edited in place.
    pub fn reindex(&mut self) {
        self.index = BlockIndex::build(&self.groups);
    }

    /// The group holding whole-object block index `block`, and the block's
    /// index within that group.
    pub fn locate_block(&self, block: u32) -> Option<(&GroupEntry, u32)> {
        let (gi, within) = self.locate_group(block)?;
        Some((self.groups.get(gi)?, within))
    }

    /// [`Self::locate_block`] by group position.
    fn locate_group(&self, block: u32) -> Option<(usize, u32)> {
        // `decode` proves the groups are consecutive runs from block 0, so
        // the candidate is the last group whose first block is at or before
        // `block`; the scan calls this once per block, so it must not walk
        // every group.
        let idx = self
            .groups
            .partition_point(|g| g.first_block <= block)
            .checked_sub(1)?;
        let g = self.groups.get(idx)?;
        (u64::from(block) < u64::from(g.first_block) + u64::from(g.block_count))
            .then_some((idx, block - g.first_block))
    }

    /// Every page of whole-object block index `block`, in `column_id` order,
    /// which is the order the block's SKIP_IDX level-0 crc32c covers them in
    /// (ADR-0699 decision 2). Returns `None` when `block` is outside the
    /// directory or a page offset overflows.
    ///
    /// A row group dictionary page is never one of them: it belongs to the
    /// chunk, not to any block, and [`Self::block_dict_pages`] returns it.
    ///
    /// This is on the scan's per-block path, so a chunk is sliced through
    /// [`BlockIndex`] to the block's run of pages rather than walked: the cost
    /// is the pages returned plus at most two per chunk
    /// (`index_tests::block_pages_visits_only_the_blocks_run`), and the result is
    /// the full scan's, page for page (`index_tests::index_matches_the_scan`).
    pub fn block_pages(&self, block: u32) -> Option<Vec<PageLoc>> {
        let (gi, within) = self.locate_group(block)?;
        let group = self.groups.get(gi)?;
        let mut out = Vec::new();
        for (ci, chunk) in group.chunks.iter().enumerate() {
            let run = self
                .index
                .runs(gi, group, ci)
                .and_then(|r| r.run(chunk, within));
            let Some((s, e, rel)) = run else {
                scan_chunk_pages(chunk, within, &mut out)?;
                continue;
            };
            // `ChunkRuns::build` proved the chunk's absolute end fits, so no
            // offset inside it overflows.
            let mut at = chunk.offset.checked_add(rel)?;
            for p in chunk.pages.get(s..e)? {
                out.push(PageLoc {
                    desc: p.desc(chunk.column_id),
                    crc32c: p.crc32c,
                    offset: at,
                });
                at = at.checked_add(p.len)?;
            }
        }
        Some(out)
    }

    /// The row group dictionary pages whole-object block index `block` needs,
    /// in `column_id` order: one per column chunk of its group that has a
    /// dictionary page and a page for `block`. Offsets are relative to BLOCKS,
    /// as [`Self::block_pages`] returns them. `None` when `block` is outside
    /// the directory.
    pub fn block_dict_pages(&self, block: u32) -> Option<Vec<PageLoc>> {
        let (gi, within) = self.locate_group(block)?;
        let group = self.groups.get(gi)?;
        let mut out = Vec::new();
        for (ci, chunk) in group.chunks.iter().enumerate() {
            let Some(d) = chunk.dict_page() else {
                continue;
            };
            let has_page = match self
                .index
                .runs(gi, group, ci)
                .and_then(|r| r.run(chunk, within))
            {
                Some((s, e, _)) => s < e,
                None => {
                    visit(chunk.pages.len());
                    chunk
                        .pages
                        .iter()
                        .any(|p| p.block == within && p.enc != Enc::DictPage)
                }
            };
            if has_page {
                out.push(PageLoc {
                    desc: d.desc(chunk.column_id),
                    crc32c: d.crc32c,
                    offset: chunk.offset,
                });
            }
        }
        Some(out)
    }

    /// The byte extent in BLOCKS of one `(row group, column)` column chunk:
    /// `(offset, total stored length)`, covering exactly the pages PAGE_DIR
    /// lists for it, contiguous and in order.
    ///
    /// This is the seam ADR-0699 decision 5's fetcher calls: given a
    /// [`crate::ColumnSelection`] it turns each surviving `(group, projected
    /// column)` into one ranged GET instead of one per block. Nothing in this
    /// crate needs it; it exists so the fetcher does not have to re-derive the
    /// layout rule.
    pub fn chunk_range(&self, group: usize, column_id: u32) -> Option<(u64, u64)> {
        self.groups
            .get(group)?
            .chunks
            .iter()
            .find(|c| c.column_id == column_id)?
            .extent()
    }

    /// The byte extents in BLOCKS, `(offset, stored length)`, of exactly the
    /// pages row group `group` holds for the blocks named in `blocks` (indices
    /// *within* the group, strictly ascending) and the columns `columns` keeps
    /// (`None` keeps every column).
    ///
    /// This is what ADR-0699 decision 5's fetcher ranges over. A projection of
    /// `k` columns over a group whose blocks all survive is `k` runs of
    /// contiguous pages, one per column chunk; a group in which only some
    /// blocks survive contributes only those blocks' pages, and the pruned
    /// blocks' pages are the gaps the fetcher's coalescing rule decides whether
    /// to read through or split around. One extent per page, ascending within a
    /// chunk and with the chunks in `column_id` order, which is ascending
    /// overall because a group's chunks are contiguous in that order.
    ///
    /// `None` when `group` is past the last group or a page offset overflows
    /// (which [`Self::decode`] already rejects).
    pub fn projected_page_ranges(
        &self,
        group: usize,
        blocks: &[u32],
        columns: Option<&HashSet<u32>>,
    ) -> Option<Vec<(u64, u64)>> {
        let g = self.groups.get(group)?;
        let mut out = Vec::new();
        for chunk in &g.chunks {
            let keep = match columns {
                None => true,
                Some(set) => set.contains(&chunk.column_id),
            };
            let chunk_start = out.len();
            let mut at = chunk.offset;
            for p in &chunk.pages {
                let start = at;
                at = at.checked_add(p.len)?;
                // `blocks` is ascending, so this stays a binary search rather
                // than a linear scan: a default-sized group has 32 blocks and a
                // wide object has one chunk per column, so a linear membership
                // test here would be quadratic in the group.
                if keep && p.enc != Enc::DictPage && blocks.binary_search(&p.block).is_ok() {
                    out.push((start, p.len));
                }
            }
            // Every kept block's id pages resolve through the chunk's
            // dictionary, so it is fetched once, ahead of them, whenever any
            // of them is.
            if out.len() > chunk_start
                && let Some(d) = chunk.dict_page()
            {
                out.insert(chunk_start, (chunk.offset, d.len));
            }
        }
        Some(out)
    }

    /// Total blocks the directory covers.
    pub fn block_count(&self) -> u64 {
        self.groups.iter().map(|g| u64::from(g.block_count)).sum()
    }

    /// Decode a PAGE_DIR and cross-check it against the block framing every
    /// reader shares (ADR-0699 decision 2): its chunk extents must fit a BLOCKS
    /// section of `blocks_len` bytes, and it must cover exactly the `l0_len`
    /// blocks the skip index frames. A mismatch is `Corrupted`, since a
    /// directory that disagrees with the framing would locate one block's pages
    /// in another block's bytes.
    ///
    /// The two readers ([`crate::reader::RlogReader`] and
    /// [`crate::ranged::RlogRangeReader`]) obtain the decompressed PAGE_DIR
    /// bytes differently -- one reads the section from the whole object, the
    /// other takes it by range -- but decode and validate them identically, so
    /// that shared tail lives here rather than duplicated in each opener.
    pub(crate) fn decode_validated(
        raw: &[u8],
        blocks_len: u64,
        l0_len: usize,
        field_dir: &FieldDir,
    ) -> Result<Self, LogSegError> {
        let dir = PageDir::decode(raw)?;
        dir.validate_framing(blocks_len, l0_len, field_dir)?;
        Ok(dir)
    }

    /// The cross-checks [`Self::decode_validated`] runs after decoding, for a
    /// directory some other caller already decoded.
    pub(crate) fn validate_framing(
        &self,
        blocks_len: u64,
        l0_len: usize,
        field_dir: &FieldDir,
    ) -> Result<(), LogSegError> {
        self.validate_extents(blocks_len)?;
        if self.block_count() != l0_len as u64 {
            return Err(LogSegError::Corrupted(format!(
                "page_dir covers {} blocks but skip index has {}",
                self.block_count(),
                l0_len
            )));
        }
        self.validate_dict_columns(field_dir)
    }

    /// Rejects a tag 12 or tag 13 page in a chunk whose column is not a string
    /// column: `severity_text`, `body`, `attrs_raw`, or a FIELD_DIR column of
    /// type `Str` or `Bytes`.
    pub fn validate_dict_columns(&self, field_dir: &FieldDir) -> Result<(), LogSegError> {
        // Each column id's type as `FieldDir::by_column_id` would find it (the
        // first entry naming it), built on the first dictionary chunk.
        let mut types: Option<HashMap<u32, FieldType>> = None;
        for g in &self.groups {
            for c in &g.chunks {
                if !c
                    .pages
                    .iter()
                    .any(|p| matches!(p.enc, Enc::DictPage | Enc::DictIds))
                {
                    continue;
                }
                let types = types.get_or_insert_with(|| {
                    let mut m = HashMap::with_capacity(field_dir.entries().len());
                    for e in field_dir.entries() {
                        m.entry(e.column_id).or_insert(e.ty);
                    }
                    m
                });
                let string = matches!(c.column_id, COL_SEVERITY_TEXT | COL_BODY | COL_ATTRS_RAW)
                    || types
                        .get(&c.column_id)
                        .is_some_and(|ty| matches!(ty, FieldType::Str | FieldType::Bytes));
                if !string {
                    return Err(LogSegError::Corrupted(format!(
                        "page_dir column {} carries a dictionary page but is not a string column",
                        c.column_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Rejects a directory whose page extents fall outside a BLOCKS section of
    /// `blocks_len` bytes. Decode alone cannot check this: the section length
    /// is not in these bytes.
    pub fn validate_extents(&self, blocks_len: u64) -> Result<(), LogSegError> {
        for g in &self.groups {
            for c in &g.chunks {
                let (offset, len) = c.extent().ok_or_else(|| {
                    LogSegError::Corrupted("page_dir chunk length overflow".into())
                })?;
                let end = offset.checked_add(len).ok_or_else(|| {
                    LogSegError::Corrupted("page_dir chunk extent overflow".into())
                })?;
                if end > blocks_len {
                    return Err(LogSegError::Corrupted(format!(
                        "page_dir chunk for column {} ends at {end} past BLOCKS length {blocks_len}",
                        c.column_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Serializes the section in its uncompressed form.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        put_uvarint(&mut out, self.groups.len() as u64);
        for g in &self.groups {
            put_uvarint(&mut out, u64::from(g.first_block));
            put_uvarint(&mut out, u64::from(g.block_count));
            put_uvarint(&mut out, g.chunks.len() as u64);
            for c in &g.chunks {
                put_uvarint(&mut out, u64::from(c.column_id));
                put_uvarint(&mut out, c.offset);
                put_uvarint(&mut out, c.pages.len() as u64);
                for p in &c.pages {
                    p.encode_into(&mut out);
                }
            }
        }
        out
    }

    /// Decodes the uncompressed section form.
    ///
    /// Rejects, as `Corrupted`: a group count over [`MAX_BLOCKS`] (a group
    /// holds at least one block); a group whose `first_block` does not continue
    /// the previous group (the groups partition the object's blocks into
    /// consecutive runs from block 0); an empty group; a chunk count above what
    /// the group's blocks could carry pages for; chunks not in strictly
    /// ascending `column_id` order; a page count above two per block (a
    /// presence page and a value page); a page naming a block outside the group
    /// or going backwards; an unknown `enc` tag; an overflowing chunk length;
    /// truncation; and trailing bytes.
    ///
    /// A row group dictionary page (tag 12) is the one exception to three of
    /// those rules, and only as its chunk's first page: its block index is the
    /// group's `block_count`, it precedes block 0, and it may take the chunk's
    /// page count one past two per block. A tag 12 page anywhere else, a
    /// second one, a tag 13 page in a chunk with none, and a tag 12 page with
    /// no tag 13 page after it are all `Corrupted`. Which columns may carry
    /// them needs FIELD_DIR, so [`Self::decode_validated`] checks that.
    pub fn decode(bytes: &[u8]) -> Result<Self, LogSegError> {
        let mut pos = 0usize;
        let group_count = get_uvarint(bytes, &mut pos)?;
        if group_count > MAX_BLOCKS {
            return Err(LogSegError::Corrupted(format!(
                "page_dir group count {group_count} over cap {MAX_BLOCKS}"
            )));
        }
        let mut groups = Vec::with_capacity(cap(group_count));
        let mut next_block: u64 = 0;
        for _ in 0..group_count {
            let first_block = get_uvarint(bytes, &mut pos)?;
            if first_block != next_block {
                return Err(LogSegError::Corrupted(format!(
                    "page_dir group first_block {first_block} does not continue at {next_block}"
                )));
            }
            let block_count = get_uvarint(bytes, &mut pos)?;
            if block_count == 0 {
                return Err(LogSegError::Corrupted(
                    "page_dir group has no blocks".into(),
                ));
            }
            next_block = next_block
                .checked_add(block_count)
                .filter(|n| *n <= MAX_BLOCKS)
                .ok_or_else(|| {
                    LogSegError::Corrupted(format!("page_dir block count over cap {MAX_BLOCKS}"))
                })?;
            let chunk_count = get_uvarint(bytes, &mut pos)?;
            // A column chunk exists only for a column some block of the group
            // carries a page for, and no block may carry more than MAX_PAGES
            // pages, so the group cannot have more distinct columns than that.
            let max_chunks = block_count.saturating_mul(MAX_PAGES);
            if chunk_count == 0 || chunk_count > max_chunks {
                return Err(LogSegError::Corrupted(format!(
                    "page_dir chunk count {chunk_count} outside 1..={max_chunks}"
                )));
            }
            let mut chunks = Vec::with_capacity(cap(chunk_count));
            let mut prev_column: Option<u32> = None;
            for _ in 0..chunk_count {
                let column_id = read_u32_varint(bytes, &mut pos)?;
                if let Some(prev) = prev_column
                    && column_id <= prev
                {
                    return Err(LogSegError::Corrupted(format!(
                        "page_dir column ids not ascending: {column_id} after {prev}"
                    )));
                }
                prev_column = Some(column_id);
                let offset = get_uvarint(bytes, &mut pos)?;
                let page_count = get_uvarint(bytes, &mut pos)?;
                // One more than two pages per block is only for a chunk whose
                // first page is its row group dictionary, checked below once
                // the first page has been read.
                let max_pages = block_count.saturating_mul(MAX_PAGES_PER_BLOCK_PER_COLUMN);
                let max_with_dict = max_pages.saturating_add(1);
                if page_count == 0 || page_count > max_with_dict {
                    return Err(LogSegError::Corrupted(format!(
                        "page_dir page count {page_count} outside 1..={max_with_dict} for a \
                         chunk of {block_count} blocks that may carry a dictionary page"
                    )));
                }
                let mut pages = Vec::with_capacity(cap(page_count));
                let mut chunk_len = 0u64;
                let mut prev_block: Option<u32> = None;
                // Pages of one block within a chunk are a consecutive run; the
                // chunk-wide cap above bounds their total, this bounds the run
                // so no block can carry more than the presence-plus-value pair.
                let mut run = 0u64;
                let mut has_dict = false;
                let mut has_ids = false;
                for i in 0..page_count {
                    let block = read_u32_varint(bytes, &mut pos)?;
                    let enc = Enc::from_u8(read_u8(bytes, &mut pos)?)?;
                    // A row group dictionary page (ADR-2135 decision 6) is the
                    // chunk's first page and names no block: its index is one
                    // past the group's last. Every other page keeps every rule.
                    if enc == Enc::DictPage {
                        if i != 0 {
                            return Err(LogSegError::Corrupted(format!(
                                "page_dir column {column_id} dictionary page is not the \
                                 chunk's first page"
                            )));
                        }
                        if u64::from(block) != block_count {
                            return Err(LogSegError::Corrupted(format!(
                                "page_dir column {column_id} dictionary page names block \
                                 {block}, not {block_count}"
                            )));
                        }
                        has_dict = true;
                    } else if enc == Enc::DictIds && !has_dict {
                        return Err(LogSegError::Corrupted(format!(
                            "page_dir column {column_id} has a dictionary id page but no \
                             dictionary page"
                        )));
                    }
                    has_ids |= enc == Enc::DictIds;
                    if enc != Enc::DictPage && u64::from(block) >= block_count {
                        return Err(LogSegError::Corrupted(format!(
                            "page_dir page block {block} outside group of {block_count}"
                        )));
                    }
                    if enc != Enc::DictPage {
                        if let Some(prev) = prev_block
                            && block < prev
                        {
                            return Err(LogSegError::Corrupted(format!(
                                "page_dir page blocks not ascending: {block} after {prev}"
                            )));
                        }
                        run = if prev_block == Some(block) {
                            run + 1
                        } else {
                            1
                        };
                        if run > MAX_PAGES_PER_BLOCK_PER_COLUMN {
                            return Err(LogSegError::Corrupted(format!(
                                "page_dir block {block} carries more than \
                                 {MAX_PAGES_PER_BLOCK_PER_COLUMN} pages in one column"
                            )));
                        }
                        prev_block = Some(block);
                    }
                    let comp = read_u8(bytes, &mut pos)?;
                    let len = get_uvarint(bytes, &mut pos)?;
                    let uncomp_len = get_uvarint(bytes, &mut pos)?;
                    let crc32c = read_u32(bytes, &mut pos)?;
                    chunk_len = chunk_len.checked_add(len).ok_or_else(|| {
                        LogSegError::Corrupted("page_dir chunk length overflow".into())
                    })?;
                    pages.push(PageEntry {
                        block,
                        enc,
                        comp,
                        len,
                        uncomp_len,
                        crc32c,
                    });
                }
                // The run cap above already refuses this while blocks ascend
                // below `block_count`; kept so the chunk-wide bound does not
                // rest on that.
                if page_count > max_pages && !has_dict {
                    return Err(LogSegError::Corrupted(format!(
                        "page_dir page count {page_count} outside 1..={max_pages} for a \
                         chunk of {block_count} blocks without a dictionary page"
                    )));
                }
                if has_dict && !has_ids {
                    return Err(LogSegError::Corrupted(format!(
                        "page_dir column {column_id} dictionary page has no id page"
                    )));
                }
                offset.checked_add(chunk_len).ok_or_else(|| {
                    LogSegError::Corrupted("page_dir chunk extent overflow".into())
                })?;
                chunks.push(ChunkEntry {
                    column_id,
                    offset,
                    pages,
                });
            }
            groups.push(GroupEntry {
                first_block: u32::try_from(first_block).map_err(|_| {
                    LogSegError::Corrupted("page_dir first_block out of range".into())
                })?,
                block_count: u32::try_from(block_count).map_err(|_| {
                    LogSegError::Corrupted("page_dir block_count out of range".into())
                })?,
                chunks,
            });
        }
        if pos != bytes.len() {
            return Err(LogSegError::Corrupted("page_dir trailing bytes".into()));
        }
        Ok(PageDir::new(groups))
    }
}

/// Capped preallocation: trust the validated count for the common case but
/// never reserve an unbounded amount from one untrusted field.
fn cap(count: u64) -> usize {
    usize::try_from(count.min(1 << 16)).unwrap_or(0)
}

fn read_u8(bytes: &[u8], pos: &mut usize) -> Result<u8, LogSegError> {
    let b = *bytes
        .get(*pos)
        .ok_or_else(|| LogSegError::Corrupted("page_dir truncated".into()))?;
    *pos += 1;
    Ok(b)
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> Result<u32, LogSegError> {
    let s = bytes
        .get(*pos..*pos + 4)
        .ok_or_else(|| LogSegError::Corrupted("page_dir truncated u32".into()))?;
    *pos += 4;
    Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn read_u32_varint(bytes: &[u8], pos: &mut usize) -> Result<u32, LogSegError> {
    u32::try_from(get_uvarint(bytes, pos)?)
        .map_err(|_| LogSegError::Corrupted("page_dir u32 varint range".into()))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn page(block: u32, len: u64) -> PageEntry {
        PageEntry {
            block,
            enc: Enc::Plain,
            comp: 0,
            len,
            uncomp_len: len,
            crc32c: block.wrapping_mul(7),
        }
    }

    /// Two groups of two blocks, three columns each; column 5 is partially
    /// present in block 0 so it carries a presence page and a value page there.
    fn sample() -> PageDir {
        PageDir::new(vec![
            GroupEntry {
                first_block: 0,
                block_count: 2,
                chunks: vec![
                    ChunkEntry {
                        column_id: 0,
                        offset: 0,
                        pages: vec![page(0, 10), page(1, 12)],
                    },
                    ChunkEntry {
                        column_id: 5,
                        offset: 22,
                        pages: vec![page(0, 3), page(0, 7), page(1, 9)],
                    },
                ],
            },
            GroupEntry {
                first_block: 2,
                block_count: 1,
                chunks: vec![ChunkEntry {
                    column_id: 0,
                    offset: 41,
                    pages: vec![page(0, 4)],
                }],
            },
        ])
    }

    fn is_corrupted<T>(r: Result<T, LogSegError>) -> bool {
        matches!(r, Err(LogSegError::Corrupted(_)))
    }

    #[test]
    fn roundtrips() {
        let dir = sample();
        let got = PageDir::decode(&dir.encode()).expect("decode");
        assert_eq!(got, dir);
    }

    #[test]
    fn chunk_range_covers_exactly_its_pages() {
        let dir = sample();
        assert_eq!(dir.chunk_range(0, 0), Some((0, 22)));
        assert_eq!(dir.chunk_range(0, 5), Some((22, 19)));
        assert_eq!(dir.chunk_range(1, 0), Some((41, 4)));
        assert_eq!(dir.chunk_range(1, 5), None);
        assert_eq!(dir.chunk_range(2, 0), None);
    }

    #[test]
    fn block_pages_are_in_column_order_with_derived_offsets() {
        let dir = sample();
        let b0 = dir.block_pages(0).expect("block 0");
        assert_eq!(
            b0.iter().map(|p| p.desc.column_id).collect::<Vec<_>>(),
            vec![0, 5, 5]
        );
        assert_eq!(
            b0.iter()
                .map(|p| (p.offset, p.desc.len))
                .collect::<Vec<_>>(),
            vec![(0, 10), (22, 3), (25, 7)]
        );
        let b2 = dir.block_pages(2).expect("block 2");
        assert_eq!(b2.len(), 1);
        assert_eq!(b2[0].offset, 41);
        assert!(dir.block_pages(3).is_none());
    }

    #[test]
    fn projected_page_ranges_keeps_only_the_named_blocks_and_columns() {
        let dir = sample();
        // Every column, every block of group 0: the whole group, contiguous.
        let all = dir
            .projected_page_ranges(0, &[0, 1], None)
            .expect("group 0");
        assert_eq!(all, vec![(0, 10), (10, 12), (22, 3), (25, 7), (32, 9)]);

        // One block of the group: block 1's page in each chunk, with block 0's
        // pages left as the holes between them.
        let b1 = dir.projected_page_ranges(0, &[1], None).expect("group 0");
        assert_eq!(b1, vec![(10, 12), (32, 9)]);

        // One column: that chunk's pages only. Column 5 carries a presence page
        // and a value page for block 0, and both come back.
        let c5 = dir
            .projected_page_ranges(0, &[0, 1], Some(&HashSet::from([5])))
            .expect("group 0");
        assert_eq!(c5, vec![(22, 3), (25, 7), (32, 9)]);

        // One block and one column: exactly the intersection.
        let one = dir
            .projected_page_ranges(0, &[0], Some(&HashSet::from([0])))
            .expect("group 0");
        assert_eq!(one, vec![(0, 10)]);

        // A column the group does not carry contributes nothing, and a block
        // index outside the group contributes nothing, neither being an error:
        // the caller's selection is not a claim about this object's layout.
        assert_eq!(
            dir.projected_page_ranges(0, &[0, 1], Some(&HashSet::from([9])))
                .expect("group 0"),
            Vec::new()
        );
        assert_eq!(
            dir.projected_page_ranges(0, &[7], None).expect("group 0"),
            Vec::new()
        );
        // A group past the last is `None`, not an empty list.
        assert_eq!(dir.projected_page_ranges(2, &[0], None), None);
    }

    /// The extents of every block and every column of a group are exactly the
    /// group's chunk ranges laid end to end: a projection that keeps everything
    /// reads the group's whole span, which is what lets the fetcher's coalescing
    /// collapse it into one GET with no separate whole-group case.
    #[test]
    fn projected_page_ranges_over_everything_tiles_the_group() {
        let dir = sample();
        let group = &dir.groups[0];
        let blocks: Vec<u32> = (0..group.block_count).collect();
        let ranges = dir
            .projected_page_ranges(0, &blocks, None)
            .expect("group 0");
        let mut at = group.chunks[0].offset;
        for (offset, len) in &ranges {
            assert_eq!(*offset, at, "the ranges are contiguous");
            at += len;
        }
        let total: u64 = group
            .chunks
            .iter()
            .map(|c| c.extent().expect("extent").1)
            .sum();
        assert_eq!(at - group.chunks[0].offset, total);
    }

    #[test]
    fn block_count_sums_the_groups() {
        assert_eq!(sample().block_count(), 3);
    }

    #[test]
    fn validate_extents_rejects_offsets_past_blocks() {
        let dir = sample();
        assert!(dir.validate_extents(45).is_ok());
        assert!(is_corrupted(dir.validate_extents(44)));
    }

    #[test]
    fn rejects_truncation_at_every_prefix() {
        let bytes = sample().encode();
        for cut in 0..bytes.len() {
            assert!(
                is_corrupted(PageDir::decode(&bytes[..cut])),
                "prefix of {cut} bytes must be Corrupted"
            );
        }
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut bytes = sample().encode();
        bytes.push(0);
        assert!(is_corrupted(PageDir::decode(&bytes)));
    }

    #[test]
    fn rejects_non_consecutive_groups() {
        let mut dir = sample();
        dir.groups[1].first_block = 3;
        assert!(is_corrupted(PageDir::decode(&dir.encode())));
    }

    #[test]
    fn rejects_descending_column_ids() {
        let mut dir = sample();
        dir.groups[0].chunks[1].column_id = 0;
        assert!(is_corrupted(PageDir::decode(&dir.encode())));
    }

    #[test]
    fn rejects_page_block_outside_group() {
        let mut dir = sample();
        dir.groups[0].chunks[0].pages[1].block = 2;
        assert!(is_corrupted(PageDir::decode(&dir.encode())));
    }

    #[test]
    fn rejects_descending_page_blocks() {
        let mut dir = sample();
        dir.groups[0].chunks[0].pages = vec![page(1, 12), page(0, 10)];
        assert!(is_corrupted(PageDir::decode(&dir.encode())));
    }

    #[test]
    fn rejects_empty_group_and_empty_chunk_list() {
        let mut dir = sample();
        dir.groups[1].block_count = 0;
        assert!(is_corrupted(PageDir::decode(&dir.encode())));
        let mut dir = sample();
        dir.groups[1].chunks.clear();
        assert!(is_corrupted(PageDir::decode(&dir.encode())));
    }

    #[test]
    fn rejects_page_count_over_the_two_per_block_cap() {
        // A hand-built section claiming more pages for one column of a
        // one-block group than the presence-plus-value pair allows.
        let mut bytes = Vec::new();
        put_uvarint(&mut bytes, 1); // group_count
        put_uvarint(&mut bytes, 0); // first_block
        put_uvarint(&mut bytes, 1); // block_count
        put_uvarint(&mut bytes, 1); // chunk_count
        put_uvarint(&mut bytes, 0); // column_id
        put_uvarint(&mut bytes, 0); // offset
        put_uvarint(&mut bytes, 3); // page_count: over 2 * block_count
        let err = PageDir::decode(&bytes);
        assert!(is_corrupted(err));
    }

    #[test]
    fn rejects_more_pages_for_one_block_than_the_per_block_cap() {
        // Two blocks allow four pages in the chunk, but all four on block 0
        // exceed the per-block presence-plus-value pair; the decoder must
        // refuse at the third page rather than let block_pages hand back
        // four pages for one block and column.
        let mut bytes = Vec::new();
        put_uvarint(&mut bytes, 1); // group_count
        put_uvarint(&mut bytes, 0); // first_block
        put_uvarint(&mut bytes, 2); // block_count
        put_uvarint(&mut bytes, 1); // chunk_count
        put_uvarint(&mut bytes, 0); // column_id
        put_uvarint(&mut bytes, 0); // offset
        put_uvarint(&mut bytes, 4); // page_count: within 2 * block_count
        for _ in 0..3 {
            put_uvarint(&mut bytes, 0); // block 0, three times
            bytes.push(Enc::Plain.to_u8());
            bytes.push(0); // comp
            put_uvarint(&mut bytes, 1); // len
            put_uvarint(&mut bytes, 1); // uncomp_len
            bytes.extend_from_slice(&0u32.to_le_bytes()); // crc32c
        }
        let err = PageDir::decode(&bytes).expect_err("third page of block 0 must be refused");
        assert!(
            matches!(&err, LogSegError::Corrupted(msg) if msg.contains("more than")),
            "expected the per-block cap error, got {err:?}"
        );
    }

    #[test]
    fn rejects_chunk_count_over_the_max_pages_cap() {
        let mut bytes = Vec::new();
        put_uvarint(&mut bytes, 1);
        put_uvarint(&mut bytes, 0);
        put_uvarint(&mut bytes, 1); // block_count 1 => at most MAX_PAGES chunks
        put_uvarint(&mut bytes, MAX_PAGES + 1);
        assert!(is_corrupted(PageDir::decode(&bytes)));
    }

    #[test]
    fn rejects_group_count_over_the_block_cap() {
        let mut bytes = Vec::new();
        put_uvarint(&mut bytes, MAX_BLOCKS + 1);
        assert!(is_corrupted(PageDir::decode(&bytes)));
    }

    #[test]
    fn rejects_unknown_enc_tag() {
        let dir = sample();
        let mut bytes = dir.encode();
        // The first page's enc byte: after group_count, first_block,
        // block_count, chunk_count, column_id, offset, page_count, block --
        // eight single-byte varints in this sample.
        assert_eq!(bytes[8], Enc::Plain.to_u8());
        bytes[8] = 0;
        assert!(is_corrupted(PageDir::decode(&bytes)));
    }

    #[test]
    fn accepts_the_rlog_integer_codec_tags() {
        for enc in [Enc::GcdI64, Enc::ColumnRef] {
            let mut dir = sample();
            dir.groups[0].chunks[0].pages[0].enc = enc;
            assert_eq!(PageDir::decode(&dir.encode()).expect("decodes"), dir);
        }
    }

    /// The first page of the sample sits at block 0, so a dictionary page
    /// there is out of place and an id page there has no dictionary page.
    #[test]
    fn rejects_misplaced_dictionary_tags() {
        for tag in 12u8..=13 {
            let mut bytes = sample().encode();
            assert_eq!(bytes[8], Enc::Plain.to_u8());
            bytes[8] = tag;
            let err = PageDir::decode(&bytes).expect_err("misplaced tag must be refused");
            assert!(
                matches!(&err, LogSegError::Corrupted(msg) if msg.contains("dictionary")),
                "tag {tag}: {err:?}"
            );
        }
    }

    #[test]
    fn a_leading_dictionary_page_round_trips() {
        let mut dir = sample();
        let chunk = &mut dir.groups[0].chunks[1];
        chunk.pages[1].enc = Enc::DictIds;
        chunk.pages[2].enc = Enc::DictIds;
        let mut dict = page(2, 5);
        dict.enc = Enc::DictPage;
        chunk.pages.insert(0, dict);
        let decoded = PageDir::decode(&dir.encode()).expect("decodes");
        assert_eq!(decoded, dir);
        let chunk = &decoded.groups[0].chunks[1];
        assert_eq!(chunk.dict_page(), Some(&dict));
        for block in 0..2 {
            let pages = decoded.block_pages(block).expect("block in range");
            assert!(pages.iter().all(|p| p.desc.enc != Enc::DictPage));
        }
    }

    #[test]
    fn rejects_overflowing_chunk_length() {
        let mut dir = sample();
        dir.groups[0].chunks[0].pages[0].len = u64::MAX;
        dir.groups[0].chunks[0].pages[1].len = u64::MAX;
        assert!(is_corrupted(PageDir::decode(&dir.encode())));
    }

    /// A tag 12 page and its tag 13 pages decode on `severity_text`, `body`,
    /// `attrs_raw` and a FIELD_DIR `Str` or `Bytes` column, and `decode_validated`
    /// refuses them on `ts`, `severity_num`, a FIELD_DIR `I64` column and a
    /// column FIELD_DIR does not name. The same chunk with plain pages decodes
    /// on every one of them.
    ///
    /// Wrong implementations this rules out, each shown failing: no column check
    /// in `decode_validated` (every refused case decodes); a check that treats a
    /// column FIELD_DIR does not name as a string column (`ts` decodes).
    #[test]
    fn dictionary_pages_only_on_string_columns() {
        use crate::field_dir::FieldEntry;
        use crate::record::{
            COL_ATTRS_RAW, COL_BODY, COL_SEVERITY_NUM, COL_SEVERITY_TEXT, COL_TS, FieldType,
        };
        let entry = |name: &str, ty, column_id| FieldEntry {
            name: name.into(),
            ty,
            column_id,
            present_blocks: 2,
            null_count: 0,
        };
        let fields = FieldDir::new(vec![
            entry("b", FieldType::Bytes, 12),
            entry("n", FieldType::I64, 10),
            entry("s", FieldType::Str, 11),
        ]);
        let chunk = |column_id, pages| {
            PageDir::new(vec![GroupEntry {
                first_block: 0,
                block_count: 2,
                chunks: vec![ChunkEntry {
                    column_id,
                    offset: 0,
                    pages,
                }],
            }])
        };
        let dict = |block, enc| PageEntry {
            enc,
            ..page(block, 4)
        };
        let dict_pages = || {
            vec![
                dict(2, Enc::DictPage),
                dict(0, Enc::DictIds),
                dict(1, Enc::DictIds),
            ]
        };
        let open = |dir: PageDir| PageDir::decode_validated(&dir.encode(), 100, 2, &fields);

        for column_id in [COL_SEVERITY_TEXT, COL_BODY, COL_ATTRS_RAW, 11, 12] {
            let dir = chunk(column_id, dict_pages());
            assert_eq!(open(dir.clone()).expect("a string column"), dir);
        }
        for column_id in [COL_TS, COL_SEVERITY_NUM, 10, 13] {
            match open(chunk(column_id, dict_pages())) {
                Err(LogSegError::Corrupted(m)) => assert_eq!(
                    m,
                    format!(
                        "page_dir column {column_id} carries a dictionary page but is not a \
                         string column"
                    )
                ),
                other => panic!("column {column_id}: {other:?}"),
            }
            open(chunk(column_id, vec![page(0, 4), page(1, 4)])).expect("plain pages");
        }
    }
}

/// Any well-formed directory round-trips exactly, and no byte string whatsoever
/// makes the decoder panic or return anything but a typed error.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn arb_enc() -> impl Strategy<Value = Enc> {
        prop::sample::select(vec![
            Enc::Plain,
            Enc::Constant,
            Enc::Rle,
            Enc::DeltaZigzag,
            Enc::DoubleDelta,
            Enc::ForBitpack,
            Enc::Dict,
            Enc::Bitmap,
            Enc::FixedWidth,
            Enc::GcdI64,
            Enc::ColumnRef,
        ])
    }

    /// A directory in exactly the shape the writer produces: groups covering
    /// consecutive blocks from 0, chunks in ascending column order, pages in
    /// ascending block order with derived contiguous offsets. A chunk drawn
    /// with `dict` set takes the dictionary form instead: a tag 12 page first,
    /// naming block `block_count`, then one tag 13 page per block it covers.
    fn arb_page_dir() -> impl Strategy<Value = PageDir> {
        proptest::collection::vec(
            (
                1u32..5,
                proptest::collection::vec(
                    (
                        0u32..40,
                        any::<bool>(),
                        proptest::collection::vec(
                            (arb_enc(), any::<u8>(), 0u64..64, any::<u32>()),
                            1..5,
                        ),
                    ),
                    1..4,
                ),
            ),
            0..4,
        )
        .prop_map(|groups| {
            let mut out = Vec::new();
            let mut first_block = 0u32;
            let mut offset = 0u64;
            for (block_count, raw_chunks) in groups {
                // Distinct, ascending column ids.
                let mut ids: Vec<u32> = raw_chunks.iter().map(|(c, _, _)| *c).collect();
                ids.sort_unstable();
                ids.dedup();
                let mut chunks = Vec::new();
                for (i, column_id) in ids.into_iter().enumerate() {
                    let (_, dict, raw_pages) = &raw_chunks[i];
                    // Ascending block indices inside the group, at most two per
                    // block (presence page plus value page), so a chunk never
                    // exceeds the `2 * block_count` pages decode allows. A
                    // dictionary chunk holds one id page per block.
                    let per_block = if *dict { 1 } else { 2 };
                    let keep = raw_pages.len().min(per_block * block_count as usize);
                    let mut pages = Vec::new();
                    for (j, (enc, comp, len, crc)) in raw_pages[..keep].iter().enumerate() {
                        pages.push(PageEntry {
                            block: j as u32 / per_block as u32,
                            enc: if *dict { Enc::DictIds } else { *enc },
                            comp: *comp,
                            len: *len,
                            uncomp_len: *len,
                            crc32c: *crc,
                        });
                    }
                    pages.sort_by_key(|p| p.block);
                    if *dict {
                        let (_, comp, len, crc) = raw_pages[0];
                        pages.insert(
                            0,
                            PageEntry {
                                block: block_count,
                                enc: Enc::DictPage,
                                comp,
                                len: len + 1,
                                uncomp_len: len + 1,
                                crc32c: crc.rotate_left(7),
                            },
                        );
                    }
                    let total: u64 = pages.iter().map(|p| p.len).sum();
                    chunks.push(ChunkEntry {
                        column_id,
                        offset,
                        pages,
                    });
                    offset += total;
                }
                out.push(GroupEntry {
                    first_block,
                    block_count,
                    chunks,
                });
                first_block += block_count;
            }
            PageDir::new(out)
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn roundtrips_and_consumes_exactly(dir in arb_page_dir()) {
            let bytes = dir.encode();
            let got = PageDir::decode(&bytes).expect("decode");
            prop_assert_eq!(&got, &dir);
            prop_assert_eq!(got.encode(), bytes);
        }

        /// Every chunk range covers exactly its pages, contiguous and in order.
        #[test]
        fn chunk_ranges_cover_exactly_their_pages(dir in arb_page_dir()) {
            for (gi, g) in dir.groups.iter().enumerate() {
                for c in &g.chunks {
                    let (offset, len) = dir.chunk_range(gi, c.column_id).expect("range");
                    prop_assert_eq!(offset, c.offset);
                    prop_assert_eq!(len, c.pages.iter().map(|p| p.len).sum::<u64>());
                    let offsets = c.page_offsets().expect("offsets");
                    let mut at = offset;
                    for (p, o) in c.pages.iter().zip(&offsets) {
                        prop_assert_eq!(*o, at);
                        at += p.len;
                    }
                    prop_assert_eq!(at, offset + len);
                }
            }
        }

        /// A dictionary page is never one of a block's pages, and is the one
        /// dictionary page, at its chunk's offset, of every block its chunk
        /// carries an id page for.
        #[test]
        fn dictionary_pages_belong_to_their_chunk(dir in arb_page_dir()) {
            let dir = PageDir::decode(&dir.encode()).expect("decode");
            for g in &dir.groups {
                for b in g.first_block..g.first_block + g.block_count {
                    let pages = dir.block_pages(b).expect("block pages");
                    prop_assert!(pages.iter().all(|p| p.desc.enc != Enc::DictPage));
                    let dicts = dir.block_dict_pages(b).expect("dict pages");
                    let want: Vec<(u32, u64)> = g
                        .chunks
                        .iter()
                        .filter(|c| {
                            c.dict_page().is_some()
                                && c.pages.iter().any(|p| {
                                    p.block == b - g.first_block && p.enc == Enc::DictIds
                                })
                        })
                        .map(|c| (c.column_id, c.offset))
                        .collect();
                    let got: Vec<(u32, u64)> =
                        dicts.iter().map(|d| (d.desc.column_id, d.offset)).collect();
                    prop_assert_eq!(got, want);
                    prop_assert!(dicts.iter().all(|d| d.desc.enc == Enc::DictPage));
                }
            }
        }

        /// Truncating anywhere is always a typed error, never a partial decode.
        #[test]
        fn truncation_is_always_corrupted(dir in arb_page_dir(), cut in any::<usize>()) {
            let bytes = dir.encode();
            prop_assume!(!bytes.is_empty());
            let cut = cut % bytes.len();
            prop_assert!(matches!(
                PageDir::decode(&bytes[..cut]),
                Err(LogSegError::Corrupted(_))
            ));
        }

        /// A flipped byte either decodes to something (the section crc is what
        /// catches that in a real object) or is a typed error, never a panic.
        #[test]
        fn single_byte_flip_never_panics(dir in arb_page_dir(), at in any::<usize>(), xor in any::<u8>()) {
            let mut bytes = dir.encode();
            prop_assume!(!bytes.is_empty());
            let at = at % bytes.len();
            bytes[at] ^= xor | 1;
            match PageDir::decode(&bytes) {
                Ok(_) | Err(LogSegError::Corrupted(_)) => {}
                Err(other) => prop_assert!(false, "unexpected error: {other:?}"),
            }
        }

        /// Arbitrary bytes never panic and never yield anything but Corrupted.
        #[test]
        fn arbitrary_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            match PageDir::decode(&bytes) {
                Ok(_) | Err(LogSegError::Corrupted(_)) => {}
                Err(other) => prop_assert!(false, "unexpected error: {other:?}"),
            }
        }
    }
}

/// The block index against the full scan it replaces.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod index_tests {
    use super::*;
    use proptest::prelude::*;

    /// [`PageDir::block_pages`] as it was before the index: every page of
    /// every chunk of the block's group, each counted as visited.
    fn scan_block_pages(dir: &PageDir, block: u32) -> Option<Vec<PageLoc>> {
        let (group, within) = dir.locate_block(block)?;
        let mut out = Vec::new();
        for chunk in &group.chunks {
            let mut at = chunk.offset;
            for p in &chunk.pages {
                visit(1);
                if p.block == within && p.enc != Enc::DictPage {
                    out.push(PageLoc {
                        desc: p.desc(chunk.column_id),
                        crc32c: p.crc32c,
                        offset: at,
                    });
                }
                at = at.checked_add(p.len)?;
            }
        }
        Some(out)
    }

    /// [`PageDir::block_dict_pages`] as it was before the index.
    fn scan_block_dict_pages(dir: &PageDir, block: u32) -> Option<Vec<PageLoc>> {
        let (group, within) = dir.locate_block(block)?;
        let mut out = Vec::new();
        for chunk in &group.chunks {
            let Some(d) = chunk.dict_page() else {
                continue;
            };
            if chunk
                .pages
                .iter()
                .any(|p| p.block == within && p.enc != Enc::DictPage)
            {
                out.push(PageLoc {
                    desc: d.desc(chunk.column_id),
                    crc32c: d.crc32c,
                    offset: chunk.offset,
                });
            }
        }
        Some(out)
    }

    fn visited<T>(f: impl FnOnce() -> T) -> (T, u64) {
        PAGES_VISITED.with(|v| v.set(0));
        let out = f();
        (out, PAGES_VISITED.with(|v| v.get()))
    }

    /// Both lookups agree with the scan for every block of the directory and
    /// the two past its end.
    fn assert_matches_scan(dir: &PageDir) -> Result<(), TestCaseError> {
        let end = u32::try_from(dir.block_count()).expect("block count");
        for b in 0..end + 2 {
            prop_assert_eq!(dir.block_pages(b), scan_block_pages(dir, b), "block {}", b);
            prop_assert_eq!(
                dir.block_dict_pages(b),
                scan_block_dict_pages(dir, b),
                "dict pages of block {}",
                b
            );
        }
        Ok(())
    }

    fn entry(block: u32, enc: Enc, len: u64, crc32c: u32) -> PageEntry {
        PageEntry {
            block,
            enc,
            comp: 0,
            len,
            uncomp_len: len,
            crc32c,
        }
    }

    /// One chunk as drawn: whether it leads with a dictionary page, then the
    /// page count of each block (0, 1 or 2; at most 1 under a dictionary), and
    /// a seed for the page lengths. A chunk drawn with no pages at all stays
    /// empty, which decode refuses but [`PageDir::new`] accepts.
    type RawChunk = (bool, Vec<u8>, u64);

    fn build_dir(groups: Vec<(u32, Vec<RawChunk>)>) -> PageDir {
        let mut out = Vec::new();
        let mut first_block = 0u32;
        let mut offset = 0u64;
        for (block_count, raw) in groups {
            let mut chunks = Vec::new();
            for (ci, (dict, counts, seed)) in raw.into_iter().enumerate() {
                let mut pages = Vec::new();
                if dict {
                    pages.push(entry(block_count, Enc::DictPage, seed % 9 + 1, 1));
                }
                for b in 0..block_count {
                    let n = counts.get(b as usize).copied().unwrap_or(0);
                    let n = if dict { n.min(1) } else { n };
                    for k in 0..n {
                        let i = pages.len() as u64;
                        let enc = if dict {
                            Enc::DictIds
                        } else if n == 2 && k == 0 {
                            Enc::Bitmap
                        } else {
                            Enc::Plain
                        };
                        let crc = (b << 8) | (ci as u32) << 1 | u32::from(k);
                        pages.push(entry(b, enc, (seed + i * 31) % 50, crc));
                    }
                }
                let total: u64 = pages.iter().map(|p| p.len).sum();
                chunks.push(ChunkEntry {
                    column_id: ci as u32 * 3,
                    offset,
                    pages,
                });
                offset += total;
            }
            out.push(GroupEntry {
                first_block,
                block_count,
                chunks,
            });
            first_block += block_count;
        }
        PageDir::new(out)
    }

    /// Several row groups of up to 80 blocks, up to six chunks each, every
    /// block carrying 0 to 2 pages per chunk, with and without dictionaries.
    fn arb_wide_dir() -> impl Strategy<Value = PageDir> {
        proptest::collection::vec(
            (
                1u32..80,
                proptest::collection::vec(
                    (
                        any::<bool>(),
                        proptest::collection::vec(0u8..3, 0..80),
                        any::<u64>().prop_map(|s| s % 1000),
                    ),
                    0..6,
                ),
            ),
            1..4,
        )
        .prop_map(build_dir)
    }

    /// One edit that takes a directory outside the shape decode admits.
    #[derive(Clone, Debug)]
    enum Damage {
        /// Two pages of one chunk change places.
        Swap(usize, usize),
        /// A page names a block past its group.
        PastGroup(usize),
        /// A dictionary page lands after the chunk's first page.
        LateDict(usize),
        /// The chunk sits so close to the end of the offset space that its
        /// running offset overflows.
        Overflow,
    }

    fn arb_damage() -> impl Strategy<Value = Damage> {
        prop_oneof![
            (any::<usize>(), any::<usize>()).prop_map(|(a, b)| Damage::Swap(a, b)),
            any::<usize>().prop_map(Damage::PastGroup),
            any::<usize>().prop_map(Damage::LateDict),
            Just(Damage::Overflow),
        ]
    }

    /// Applies `damage` to one non-empty chunk picked by `pick`, in `groups`.
    /// `false` when the directory has no page to damage.
    fn apply(groups: &mut [GroupEntry], pick: usize, damage: &Damage) -> bool {
        let mut chunks: Vec<(usize, usize)> = Vec::new();
        for (gi, g) in groups.iter().enumerate() {
            for (ci, c) in g.chunks.iter().enumerate() {
                if !c.pages.is_empty() {
                    chunks.push((gi, ci));
                }
            }
        }
        let Some(&(gi, ci)) = chunks.get(pick % chunks.len().max(1)) else {
            return false;
        };
        let block_count = groups[gi].block_count;
        let chunk = &mut groups[gi].chunks[ci];
        let n = chunk.pages.len();
        match *damage {
            Damage::Swap(a, b) => chunk.pages.swap(a % n, b % n),
            Damage::PastGroup(a) => chunk.pages[a % n].block = block_count + (a % 3) as u32,
            Damage::LateDict(a) => {
                let at = 1 + a % n;
                chunk.pages.insert(at, entry(0, Enc::DictPage, 3, 9));
            }
            Damage::Overflow => {
                let len: u64 = chunk.pages.iter().map(|p| p.len).sum();
                chunk.offset = u64::MAX - len / 2;
                if len < 2 {
                    chunk.pages[0].len = 4;
                }
            }
        }
        true
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// The index returns exactly what the full scan returned, page for page
        /// and in order, for every block: `None` outside the directory, the
        /// same pages with the same offsets inside it, and the same dictionary
        /// pages. A directory that round-trips through decode is checked again
        /// as decode builds it.
        #[test]
        fn index_matches_the_scan(dir in arb_wide_dir()) {
            assert_matches_scan(&dir)?;
            if let Ok(decoded) = PageDir::decode(&dir.encode()) {
                prop_assert_eq!(&decoded, &dir);
                assert_matches_scan(&decoded)?;
            }
        }

        /// A directory outside the shape decode admits (pages out of block
        /// order, a page past its group, a dictionary page after the first,
        /// an overflowing offset), built directly, still gets the scan's
        /// answer for every block; decode either refuses its bytes as
        /// `Corrupted` or admits them and the decoded directory matches too.
        #[test]
        fn malformed_directories_match_the_scan(
            dir in arb_wide_dir(),
            pick in any::<usize>(),
            damage in arb_damage(),
        ) {
            let mut groups = dir.groups.clone();
            prop_assume!(apply(&mut groups, pick, &damage));
            let damaged = PageDir::new(groups);
            assert_matches_scan(&damaged)?;
            // A swap inside one block's run can leave the shape admissible.
            match PageDir::decode(&damaged.encode()) {
                Ok(decoded) => assert_matches_scan(&decoded)?,
                Err(LogSegError::Corrupted(_)) => {}
                Err(other) => prop_assert!(false, "unexpected error: {other:?}"),
            }
        }

        /// The same damage applied in place after the index was built, then
        /// `reindex`: the lookups follow the edited groups.
        #[test]
        fn reindex_follows_in_place_edits(
            dir in arb_wide_dir(),
            pick in any::<usize>(),
            damage in arb_damage(),
        ) {
            let mut dir = dir;
            prop_assume!(apply(&mut dir.groups, pick, &damage));
            dir.reindex();
            assert_matches_scan(&dir)?;
        }
    }

    /// One group of 64 blocks and eight chunks: two dense ones with presence
    /// and value pages, two dictionary chunks, sparse ones, and an empty one.
    fn wide_group() -> PageDir {
        let counts = |f: fn(u32) -> u8| (0..64).map(f).collect::<Vec<u8>>();
        build_dir(vec![(
            64,
            vec![
                (false, counts(|_| 2), 7),
                (false, counts(|_| 1), 11),
                (true, counts(|_| 1), 13),
                (true, counts(|b| u8::from(b % 3 == 0)), 17),
                (false, counts(|b| (b % 3) as u8), 19),
                (false, counts(|b| u8::from(b % 7 == 0)), 23),
                (false, Vec::new(), 29),
                (false, counts(|b| if b < 32 { 2 } else { 0 }), 31),
            ],
        )])
    }

    /// Each lookup looks at the block's own pages plus at most the page on
    /// either side of its run in each chunk, where the scan it replaces looks
    /// at every page of the group for every block.
    #[test]
    fn block_pages_visits_only_the_blocks_run() {
        let dir = wide_group();
        let chunks = dir.groups[0].chunks.len() as u64;
        let group_pages: u64 = dir.groups[0]
            .chunks
            .iter()
            .map(|c| c.pages.len() as u64)
            .sum();
        assert!(group_pages > 300, "group holds {group_pages} pages");
        for b in 0..64 {
            let (pages, cost) = visited(|| dir.block_pages(b).expect("block"));
            let bound = pages.len() as u64 + 2 * chunks;
            assert!(cost <= bound, "block {b}: visited {cost}, bound {bound}");

            let (dicts, cost) = visited(|| dir.block_dict_pages(b).expect("block"));
            // A dictionary chunk's run is at most one id page.
            assert!(cost <= 3 * 2, "block {b}: dict lookup visited {cost}");
            assert_eq!(Some(dicts), scan_block_dict_pages(&dir, b));

            let (scanned, scan_cost) = visited(|| scan_block_pages(&dir, b).expect("block"));
            assert_eq!(pages, scanned);
            assert_eq!(scan_cost, group_pages);
            assert!(
                scan_cost > bound,
                "block {b}: the scan visits {scan_cost}, over the bound {bound}"
            );
        }
    }

    /// Hand-built cases of each damage, at a block boundary: none loses a page
    /// the scan returns.
    #[test]
    fn unordered_pages_are_never_dropped() {
        let chunk = |pages| {
            PageDir::new(vec![GroupEntry {
                first_block: 0,
                block_count: 3,
                chunks: vec![ChunkEntry {
                    column_id: 1,
                    offset: 0,
                    pages,
                }],
            }])
        };
        let plain = |b| entry(b, Enc::Plain, 5, b);
        // Block 1's second page sits after block 2's.
        let dir = chunk(vec![plain(0), plain(1), plain(2), plain(1)]);
        assert_eq!(dir.block_pages(1).expect("block 1").len(), 2);
        // A page past the group, mid-chunk.
        let dir = chunk(vec![plain(0), plain(7), plain(1), plain(2)]);
        // A dictionary page that is not the first.
        let late = chunk(vec![
            plain(0),
            entry(1, Enc::DictPage, 5, 0),
            plain(1),
            plain(2),
        ]);
        // An offset that overflows on the chunk's last page.
        let mut over = chunk(vec![plain(0), plain(1), plain(2)]);
        over.groups[0].chunks[0].offset = u64::MAX - 12;
        over.reindex();
        for d in [&dir, &late, &over] {
            for b in 0..4 {
                assert_eq!(d.block_pages(b), scan_block_pages(d, b), "block {b}");
                assert_eq!(d.block_dict_pages(b), scan_block_dict_pages(d, b));
            }
            assert!(matches!(
                PageDir::decode(&d.encode()),
                Err(LogSegError::Corrupted(_))
            ));
        }
        assert_eq!(over.block_pages(0), None);
    }

    /// An index left behind by an edit that changes a group's or chunk's shape
    /// (the writer appends groups to a directory it built) is not used for
    /// what it no longer describes.
    #[test]
    fn a_stale_index_falls_back_to_the_scan() {
        let mut dir = wide_group();
        let extra = wide_group().groups.remove(0);
        dir.groups.push(GroupEntry {
            first_block: 64,
            ..extra
        });
        dir.groups[0].chunks[1].pages.pop();
        dir.groups[0].chunks.swap(2, 3);
        dir.groups[0].chunks.pop();
        for b in 0..130 {
            assert_eq!(dir.block_pages(b), scan_block_pages(&dir, b), "block {b}");
            assert_eq!(dir.block_dict_pages(b), scan_block_dict_pages(&dir, b));
        }
    }
}
