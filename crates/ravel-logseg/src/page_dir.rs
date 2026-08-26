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

use crate::block::MAX_PAGES;
use crate::encoding::Enc;
use crate::error::LogSegError;
use crate::page::PageDesc;
use crate::reader::MAX_BLOCKS;
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
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct PageDir {
    pub groups: Vec<GroupEntry>,
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
    /// The group holding whole-object block index `block`, and the block's
    /// index within that group.
    pub fn locate_block(&self, block: u32) -> Option<(&GroupEntry, u32)> {
        // `decode` proves the groups are consecutive runs from block 0, so
        // the candidate is the last group whose first block is at or before
        // `block`; the scan calls this once per block, so it must not walk
        // every group.
        let idx = self.groups.partition_point(|g| g.first_block <= block);
        let g = self.groups.get(idx.checked_sub(1)?)?;
        (u64::from(block) < u64::from(g.first_block) + u64::from(g.block_count))
            .then_some((g, block - g.first_block))
    }

    /// Every page of whole-object block index `block`, in `column_id` order,
    /// which is the order the block's SKIP_IDX level-0 crc32c covers them in
    /// (ADR-0699 decision 2). Returns `None` when `block` is outside the
    /// directory or a page offset overflows.
    pub fn block_pages(&self, block: u32) -> Option<Vec<PageLoc>> {
        let (group, within) = self.locate_block(block)?;
        // The per-page offset is derived by running the chunk's offset forward,
        // inline rather than through `page_offsets`: this is on the scan's
        // per-block path and a wide object has one chunk per column, so a
        // per-chunk allocation here would scale with the column count.
        let mut out = Vec::new();
        for chunk in &group.chunks {
            let mut at = chunk.offset;
            for p in &chunk.pages {
                if p.block == within {
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

    /// Total blocks the directory covers.
    pub fn block_count(&self) -> u64 {
        self.groups.iter().map(|g| u64::from(g.block_count)).sum()
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
                    put_uvarint(&mut out, u64::from(p.block));
                    out.push(p.enc.to_u8());
                    out.push(p.comp);
                    put_uvarint(&mut out, p.len);
                    put_uvarint(&mut out, p.uncomp_len);
                    out.extend_from_slice(&p.crc32c.to_le_bytes());
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
                let max_pages = block_count.saturating_mul(MAX_PAGES_PER_BLOCK_PER_COLUMN);
                if page_count == 0 || page_count > max_pages {
                    return Err(LogSegError::Corrupted(format!(
                        "page_dir page count {page_count} outside 1..={max_pages}"
                    )));
                }
                let mut pages = Vec::with_capacity(cap(page_count));
                let mut chunk_len = 0u64;
                let mut prev_block: Option<u32> = None;
                // Pages of one block within a chunk are a consecutive run; the
                // chunk-wide cap above bounds their total, this bounds the run
                // so no block can carry more than the presence-plus-value pair.
                let mut run = 0u64;
                for _ in 0..page_count {
                    let block = read_u32_varint(bytes, &mut pos)?;
                    if u64::from(block) >= block_count {
                        return Err(LogSegError::Corrupted(format!(
                            "page_dir page block {block} outside group of {block_count}"
                        )));
                    }
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
                    let enc = Enc::from_u8(read_u8(bytes, &mut pos)?)?;
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
        Ok(PageDir { groups })
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
        PageDir {
            groups: vec![
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
            ],
        }
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
    fn rejects_overflowing_chunk_length() {
        let mut dir = sample();
        dir.groups[0].chunks[0].pages[0].len = u64::MAX;
        dir.groups[0].chunks[0].pages[1].len = u64::MAX;
        assert!(is_corrupted(PageDir::decode(&dir.encode())));
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
        ])
    }

    /// A directory in exactly the shape the writer produces: groups covering
    /// consecutive blocks from 0, chunks in ascending column order, pages in
    /// ascending block order with derived contiguous offsets.
    fn arb_page_dir() -> impl Strategy<Value = PageDir> {
        proptest::collection::vec(
            (
                1u32..5,
                proptest::collection::vec(
                    (
                        0u32..40,
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
                let mut ids: Vec<u32> = raw_chunks.iter().map(|(c, _)| *c).collect();
                ids.sort_unstable();
                ids.dedup();
                let mut chunks = Vec::new();
                for (i, column_id) in ids.into_iter().enumerate() {
                    let (_, raw_pages) = &raw_chunks[i];
                    // Ascending block indices inside the group, at most two per
                    // block (presence page plus value page), so a chunk never
                    // exceeds the `2 * block_count` pages decode allows.
                    let keep = raw_pages.len().min(2 * block_count as usize);
                    let mut pages = Vec::new();
                    for (j, (enc, comp, len, crc)) in raw_pages[..keep].iter().enumerate() {
                        let block = j as u32 / 2;
                        pages.push(PageEntry {
                            block,
                            enc: *enc,
                            comp: *comp,
                            len: *len,
                            uncomp_len: *len,
                            crc32c: *crc,
                        });
                    }
                    pages.sort_by_key(|p| p.block);
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
            PageDir { groups: out }
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
