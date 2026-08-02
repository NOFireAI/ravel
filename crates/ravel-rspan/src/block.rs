//! Columnar row blocks (docs/span-segment-format.md "BLOCKS").
//!
//! A block holds a run of spans column by column: a header of page descriptors,
//! then the pages. Each column is one value page, preceded by a presence bitmap
//! page (`enc = Bitmap`) when the column is nullable and not present in every
//! row of the block. RSPAN v1 uses plain columnar codecs under a per-page zstd
//! envelope rather than RLOG's measured per-column encodings (ADR-0041
//! leanness): the zstd envelope carries the bulk of the compression, and the
//! `enc` tag is still stored per page so a later version can add codecs without
//! a reader rewrite. The block's crc32c lives in its SKIP_IDX entry, not inline;
//! [`read_block`] verifies it before decoding anything.

use std::collections::HashMap;

use crate::error::SpanSegError;
use crate::record::{
    COL_ATTRS, COL_END_TS, COL_NAME, COL_PARENT_SPAN_ID, COL_SPAN_ID, COL_START_TS,
    COL_STATUS_CODE, COL_STATUS_MESSAGE, COL_TRACE_ID, SPAN_ID_WIDTH, SpanRecord, StatusCode,
    TRACE_ID_WIDTH, encode_attrs,
};
use crate::skip_index::{STATUS_BIT_ERROR, STATUS_BIT_OK, STATUS_BIT_UNSET};
use crate::varint::{get_ivarint, get_uvarint, put_ivarint, put_uvarint};

/// Upper bound on a block's decoded record count (untrusted-input guard).
const MAX_RECORDS: u64 = 1 << 24;
/// Upper bound on a block's page count. The fixed nine columns, each up to two
/// pages, stays far under this.
const MAX_PAGES: u64 = 4096;
/// Page compression floor: pages under this many encoded bytes stay raw.
const COMPRESSION_FLOOR: usize = 512;
/// Default per-page decompressed-size cap (zstd bomb guard).
pub const DEFAULT_MAX_UNCOMP: u64 = 64 << 20;

/// `comp` tag: stored raw.
const COMP_NONE: u8 = 0;
/// `comp` tag: zstd frame.
const COMP_ZSTD: u8 = 2;

/// Per-page value encoding tag (frozen contract, docs/span-segment-format.md).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(u8)]
pub enum Enc {
    /// Integers as ivarints, strings as `uvarint(len)`-prefixed blobs.
    Plain = 1,
    /// LSB-first bit set: a bool value page, or a presence bitmap.
    Bitmap = 2,
    /// Fixed-width values concatenated with no framing.
    FixedWidth = 3,
}

impl Enc {
    fn from_u8(v: u8) -> Result<Enc, SpanSegError> {
        Ok(match v {
            1 => Enc::Plain,
            2 => Enc::Bitmap,
            3 => Enc::FixedWidth,
            other => return Err(SpanSegError::Corrupted(format!("unknown enc tag {other}"))),
        })
    }

    fn to_u8(self) -> u8 {
        self as u8
    }
}

/// The output of encoding one block, plus the bounds its SKIP_IDX entry needs.
#[derive(Clone, Debug)]
pub struct BlockWriteOut {
    pub bytes: Vec<u8>,
    pub crc32c: u32,
    pub record_count: u32,
    pub min_start_ts: i64,
    pub max_end_ts: i64,
    pub min_trace_id: [u8; 16],
    pub max_trace_id: [u8; 16],
    /// `min(end_ts_ns - start_ts_ns)` over the block's rows (ADR-0045).
    pub min_duration_ns: i64,
    /// `max(end_ts_ns - start_ts_ns)` over the block's rows (ADR-0045).
    pub max_duration_ns: i64,
    /// Bits 0/1/2 set when any row has status Unset/Ok/Error (ADR-0045).
    pub status_mask: u8,
}

/// One page's descriptor, stored in the block header.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct PageDesc {
    column_id: u32,
    enc: Enc,
    comp: u8,
    len: u64,
    uncomp_len: u64,
}

/// One page staged for a column: its column id, encoding tag, and value bytes.
struct StagedPage {
    column_id: u32,
    enc: Enc,
    bytes: Vec<u8>,
}

/// Encodes `rows` into one block. `rows` must be non-empty and already sorted by
/// `(trace_id, start_ts)`. Columns are emitted in ascending column-id order for
/// byte-deterministic output.
pub fn write_block(rows: &[SpanRecord], zstd_level: i32) -> Result<BlockWriteOut, SpanSegError> {
    if rows.is_empty() {
        return Err(SpanSegError::Corrupted("empty block".into()));
    }
    let n = rows.len();
    let all_present = vec![true; n];
    let mut pages: Vec<StagedPage> = Vec::new();

    // COL_TRACE_ID: fixed 16, always present.
    let trace_vals: Vec<&[u8]> = rows.iter().map(|r| r.trace_id.as_slice()).collect();
    stage_column(
        &mut pages,
        COL_TRACE_ID,
        &all_present,
        Enc::FixedWidth,
        encode_fixed(&trace_vals),
    );
    // COL_SPAN_ID: fixed 8, always present.
    let span_vals: Vec<&[u8]> = rows.iter().map(|r| r.span_id.as_slice()).collect();
    stage_column(
        &mut pages,
        COL_SPAN_ID,
        &all_present,
        Enc::FixedWidth,
        encode_fixed(&span_vals),
    );
    // COL_PARENT_SPAN_ID: fixed 8, nullable (root spans have none).
    let parent_present: Vec<bool> = rows.iter().map(|r| r.parent_span_id.is_some()).collect();
    let parent_vals: Vec<&[u8]> = rows
        .iter()
        .filter_map(|r| r.parent_span_id.as_ref().map(|a| a.as_slice()))
        .collect();
    stage_column(
        &mut pages,
        COL_PARENT_SPAN_ID,
        &parent_present,
        Enc::FixedWidth,
        encode_fixed(&parent_vals),
    );
    // COL_NAME: str, always present.
    let name_vals: Vec<&[u8]> = rows.iter().map(|r| r.name.as_bytes()).collect();
    stage_column(
        &mut pages,
        COL_NAME,
        &all_present,
        Enc::Plain,
        encode_strings(&name_vals),
    );
    // COL_START_TS / COL_END_TS: i64, always present.
    let start: Vec<i64> = rows.iter().map(|r| r.start_ts_ns).collect();
    stage_column(
        &mut pages,
        COL_START_TS,
        &all_present,
        Enc::Plain,
        encode_i64(&start),
    );
    let end: Vec<i64> = rows.iter().map(|r| r.end_ts_ns).collect();
    stage_column(
        &mut pages,
        COL_END_TS,
        &all_present,
        Enc::Plain,
        encode_i64(&end),
    );
    // COL_STATUS_CODE: small int, always present.
    let status: Vec<i64> = rows
        .iter()
        .map(|r| i64::from(r.status_code.to_u8()))
        .collect();
    stage_column(
        &mut pages,
        COL_STATUS_CODE,
        &all_present,
        Enc::Plain,
        encode_i64(&status),
    );
    // COL_STATUS_MESSAGE: str, nullable.
    let msg_present: Vec<bool> = rows.iter().map(|r| r.status_message.is_some()).collect();
    let msg_vals: Vec<&[u8]> = rows
        .iter()
        .filter_map(|r| r.status_message.as_deref().map(str::as_bytes))
        .collect();
    stage_column(
        &mut pages,
        COL_STATUS_MESSAGE,
        &msg_present,
        Enc::Plain,
        encode_strings(&msg_vals),
    );
    // COL_ATTRS: canonical merged-map blob per row, str column, always present.
    let attr_blobs: Vec<Vec<u8>> = rows.iter().map(|r| encode_attrs(&r.attrs)).collect();
    let attr_vals: Vec<&[u8]> = attr_blobs.iter().map(Vec::as_slice).collect();
    stage_column(
        &mut pages,
        COL_ATTRS,
        &all_present,
        Enc::Plain,
        encode_strings(&attr_vals),
    );

    // Compress pages into a payload buffer, collecting descriptors.
    let mut payload = Vec::new();
    let mut descs: Vec<PageDesc> = Vec::with_capacity(pages.len());
    for p in &pages {
        descs.push(write_page(
            &mut payload,
            p.column_id,
            p.enc,
            &p.bytes,
            zstd_level,
        ));
    }

    // Header, then payload.
    let mut block = Vec::new();
    put_uvarint(&mut block, n as u64);
    put_uvarint(&mut block, descs.len() as u64);
    for d in &descs {
        put_uvarint(&mut block, u64::from(d.column_id));
        block.push(d.enc.to_u8());
        block.push(d.comp);
        put_uvarint(&mut block, d.len);
        put_uvarint(&mut block, d.uncomp_len);
    }
    block.extend_from_slice(&payload);

    let crc = crc32c::crc32c(&block);
    // Rows are sorted by (trace_id, start_ts), but end_ts is not ordered, so the
    // block's time interval bound scans both endpoints explicitly.
    let min_start_ts = start.iter().copied().min().unwrap_or(0);
    let max_end_ts = end.iter().copied().max().unwrap_or(0);
    let min_trace_id = rows[0].trace_id;
    let max_trace_id = rows[n - 1].trace_id;

    // Duration and status_mask (ADR-0045 decision 2): derived from the same
    // start/end/status vectors already scanned above, not stored per row. A
    // negative duration (end precedes start) is a valid ivarint value and is
    // kept as-is; only true i64 overflow of the subtraction is rejected.
    let mut min_duration_ns = i64::MAX;
    let mut max_duration_ns = i64::MIN;
    let mut status_mask = 0u8;
    for i in 0..n {
        let duration = end[i]
            .checked_sub(start[i])
            .ok_or_else(|| SpanSegError::Corrupted("span duration overflow".into()))?;
        min_duration_ns = min_duration_ns.min(duration);
        max_duration_ns = max_duration_ns.max(duration);
        status_mask |= match StatusCode::from_u8(status[i] as u8) {
            Some(StatusCode::Unset) => STATUS_BIT_UNSET,
            Some(StatusCode::Ok) => STATUS_BIT_OK,
            Some(StatusCode::Error) => STATUS_BIT_ERROR,
            None => {
                return Err(SpanSegError::Corrupted(format!(
                    "unknown status code {}",
                    status[i]
                )));
            }
        };
    }

    Ok(BlockWriteOut {
        bytes: block,
        crc32c: crc,
        record_count: n as u32,
        min_start_ts,
        max_end_ts,
        min_trace_id,
        max_trace_id,
        min_duration_ns,
        max_duration_ns,
        status_mask,
    })
}

/// Stages a column's pages: a presence bitmap page (only when the column is
/// partially present) followed by the value page. A wholly-absent nullable
/// column stages nothing.
fn stage_column(
    pages: &mut Vec<StagedPage>,
    column_id: u32,
    present: &[bool],
    value_enc: Enc,
    value_bytes: Vec<u8>,
) {
    let any = present.iter().any(|&p| p);
    let all = present.iter().all(|&p| p);
    if !any {
        return;
    }
    if !all {
        pages.push(StagedPage {
            column_id,
            enc: Enc::Bitmap,
            bytes: encode_bitmap(present),
        });
    }
    pages.push(StagedPage {
        column_id,
        enc: value_enc,
        bytes: value_bytes,
    });
}

/// A decoded block: per-column, per-row values. Every column vector has length
/// `record_count`; `None` marks a row where a nullable column has no value.
pub struct DecodedBlock {
    record_count: usize,
    i64_cols: HashMap<u32, Vec<Option<i64>>>,
    str_cols: HashMap<u32, Vec<Option<Vec<u8>>>>,
    fixed_cols: HashMap<u32, Vec<Option<Vec<u8>>>>,
}

impl DecodedBlock {
    pub fn record_count(&self) -> usize {
        self.record_count
    }

    fn i64_at(&self, col: u32, row: usize) -> Result<i64, SpanSegError> {
        self.i64_cols
            .get(&col)
            .and_then(|c| c.get(row).copied())
            .flatten()
            .ok_or_else(|| SpanSegError::Corrupted(format!("missing i64 col {col} row {row}")))
    }

    fn fixed_at(&self, col: u32, row: usize) -> Option<Vec<u8>> {
        self.fixed_cols
            .get(&col)
            .and_then(|c| c.get(row).cloned())
            .flatten()
    }

    fn str_at(&self, col: u32, row: usize) -> Option<Vec<u8>> {
        self.str_cols
            .get(&col)
            .and_then(|c| c.get(row).cloned())
            .flatten()
    }

    /// Rebuilds the [`SpanRecord`] at `row`, a faithful round-trip of the record
    /// the writer was handed.
    pub fn record(&self, row: usize) -> Result<SpanRecord, SpanSegError> {
        let trace_id = self
            .fixed_at(COL_TRACE_ID, row)
            .ok_or_else(|| SpanSegError::Corrupted("missing trace_id".into()))?;
        let trace_id = <[u8; 16]>::try_from(trace_id.as_slice())
            .map_err(|_| SpanSegError::Corrupted("trace_id width".into()))?;
        let span_id = self
            .fixed_at(COL_SPAN_ID, row)
            .ok_or_else(|| SpanSegError::Corrupted("missing span_id".into()))?;
        let span_id = <[u8; 8]>::try_from(span_id.as_slice())
            .map_err(|_| SpanSegError::Corrupted("span_id width".into()))?;
        let parent_span_id = match self.fixed_at(COL_PARENT_SPAN_ID, row) {
            Some(bytes) => Some(
                <[u8; 8]>::try_from(bytes.as_slice())
                    .map_err(|_| SpanSegError::Corrupted("parent_span_id width".into()))?,
            ),
            None => None,
        };
        let name = string_from(
            self.str_at(COL_NAME, row)
                .ok_or_else(|| SpanSegError::Corrupted("missing name".into()))?,
        )?;
        let start_ts_ns = self.i64_at(COL_START_TS, row)?;
        let end_ts_ns = self.i64_at(COL_END_TS, row)?;
        let code_byte = u8::try_from(self.i64_at(COL_STATUS_CODE, row)?)
            .map_err(|_| SpanSegError::Corrupted("status_code range".into()))?;
        let status_code = StatusCode::from_u8(code_byte)
            .ok_or_else(|| SpanSegError::Corrupted(format!("unknown status_code {code_byte}")))?;
        let status_message = match self.str_at(COL_STATUS_MESSAGE, row) {
            Some(bytes) => Some(string_from(bytes)?),
            None => None,
        };
        let attrs = crate::record::decode_attrs(
            &self
                .str_at(COL_ATTRS, row)
                .ok_or_else(|| SpanSegError::Corrupted("missing attrs".into()))?,
        )?;
        Ok(SpanRecord {
            trace_id,
            span_id,
            parent_span_id,
            name,
            start_ts_ns,
            end_ts_ns,
            status_code,
            status_message,
            attrs,
        })
    }
}

fn string_from(bytes: Vec<u8>) -> Result<String, SpanSegError> {
    String::from_utf8(bytes).map_err(|_| SpanSegError::Corrupted("value not utf-8".into()))
}

/// How a column decodes, resolved from its (fixed) id.
enum ColKind {
    I64,
    Str,
    Fixed(usize),
}

fn column_kind(column_id: u32) -> Result<ColKind, SpanSegError> {
    Ok(match column_id {
        COL_TRACE_ID => ColKind::Fixed(TRACE_ID_WIDTH),
        COL_SPAN_ID | COL_PARENT_SPAN_ID => ColKind::Fixed(SPAN_ID_WIDTH),
        COL_NAME | COL_STATUS_MESSAGE | COL_ATTRS => ColKind::Str,
        COL_START_TS | COL_END_TS | COL_STATUS_CODE => ColKind::I64,
        other => {
            return Err(SpanSegError::Corrupted(format!(
                "unknown column id {other}"
            )));
        }
    })
}

/// Decodes a block, verifying its crc32c first.
pub fn read_block(
    bytes: &[u8],
    expected_crc: u32,
    max_uncomp: u64,
) -> Result<DecodedBlock, SpanSegError> {
    if crc32c::crc32c(bytes) != expected_crc {
        return Err(SpanSegError::Corrupted("block crc mismatch".into()));
    }
    let mut pos = 0usize;
    let record_count = get_uvarint(bytes, &mut pos)?;
    if record_count > MAX_RECORDS {
        return Err(SpanSegError::Corrupted(format!(
            "record_count {record_count} over cap"
        )));
    }
    let record_count = record_count as usize;
    let page_count = get_uvarint(bytes, &mut pos)?;
    if page_count > MAX_PAGES {
        return Err(SpanSegError::Corrupted(format!(
            "page_count {page_count} over cap"
        )));
    }
    let page_count = page_count as usize;

    let mut descs: Vec<PageDesc> = Vec::with_capacity(page_count.min(MAX_PAGES as usize));
    for _ in 0..page_count {
        let column_id = u32::try_from(get_uvarint(bytes, &mut pos)?)
            .map_err(|_| SpanSegError::Corrupted("column id out of range".into()))?;
        let enc = Enc::from_u8(
            *bytes
                .get(pos)
                .ok_or_else(|| SpanSegError::Corrupted("block truncated at enc".into()))?,
        )?;
        pos += 1;
        let comp = *bytes
            .get(pos)
            .ok_or_else(|| SpanSegError::Corrupted("block truncated at comp".into()))?;
        pos += 1;
        let len = get_uvarint(bytes, &mut pos)?;
        let uncomp_len = get_uvarint(bytes, &mut pos)?;
        descs.push(PageDesc {
            column_id,
            enc,
            comp,
            len,
            uncomp_len,
        });
    }

    // Slice and decompress each page to its encoded bytes.
    let mut page_bytes: Vec<Vec<u8>> = Vec::with_capacity(descs.len());
    for d in &descs {
        let len = usize::try_from(d.len)
            .map_err(|_| SpanSegError::Corrupted("page len out of range".into()))?;
        let end = pos
            .checked_add(len)
            .ok_or_else(|| SpanSegError::Corrupted("page range overflow".into()))?;
        let stored = bytes
            .get(pos..end)
            .ok_or_else(|| SpanSegError::Corrupted("page range out of bounds".into()))?;
        page_bytes.push(read_page(stored, d, max_uncomp)?);
        pos = end;
    }
    if pos != bytes.len() {
        return Err(SpanSegError::Corrupted(
            "trailing bytes after block pages".into(),
        ));
    }

    // Group descriptor indices by column id, preserving order.
    let mut order: Vec<u32> = Vec::new();
    let mut groups: HashMap<u32, Vec<usize>> = HashMap::new();
    for (i, d) in descs.iter().enumerate() {
        groups.entry(d.column_id).or_insert_with(|| {
            order.push(d.column_id);
            Vec::new()
        });
        if let Some(g) = groups.get_mut(&d.column_id) {
            g.push(i);
        }
    }

    let mut out = DecodedBlock {
        record_count,
        i64_cols: HashMap::new(),
        str_cols: HashMap::new(),
        fixed_cols: HashMap::new(),
    };

    for column_id in order {
        let idxs = &groups[&column_id];
        let (present, value_idx): (Vec<bool>, usize) = match idxs.as_slice() {
            [v] => (vec![true; record_count], *v),
            [p, v] => {
                if descs[*p].enc != Enc::Bitmap {
                    return Err(SpanSegError::Corrupted("presence page not a bitmap".into()));
                }
                (decode_bitmap(&page_bytes[*p], record_count)?, *v)
            }
            _ => {
                return Err(SpanSegError::Corrupted(format!(
                    "column {column_id} has {} pages",
                    idxs.len()
                )));
            }
        };
        let present_count = present.iter().filter(|&&b| b).count();
        let enc = descs[value_idx].enc;
        let encoded = &page_bytes[value_idx];
        match column_kind(column_id)? {
            ColKind::I64 => {
                let vals = decode_i64(enc, encoded, present_count)?;
                out.i64_cols.insert(column_id, scatter(&present, vals)?);
            }
            ColKind::Str => {
                let vals = decode_strings(enc, encoded, present_count)?;
                out.str_cols.insert(column_id, scatter(&present, vals)?);
            }
            ColKind::Fixed(width) => {
                let vals = decode_fixed(enc, encoded, present_count, width)?;
                out.fixed_cols.insert(column_id, scatter(&present, vals)?);
            }
        }
    }

    Ok(out)
}

/// Scatters the present values into a per-row vector. `values` holds only the
/// present entries, in row order.
fn scatter<T: Clone>(present: &[bool], values: Vec<T>) -> Result<Vec<Option<T>>, SpanSegError> {
    let mut out = Vec::with_capacity(present.len());
    let mut it = values.into_iter();
    for &p in present {
        if p {
            let v = it
                .next()
                .ok_or_else(|| SpanSegError::Corrupted("presence/value count mismatch".into()))?;
            out.push(Some(v));
        } else {
            out.push(None);
        }
    }
    if it.next().is_some() {
        return Err(SpanSegError::Corrupted(
            "value page longer than presence".into(),
        ));
    }
    Ok(out)
}

// --- page compression envelope ---------------------------------------------

/// Appends one page's stored bytes to `out` and returns its descriptor.
/// Compresses with zstd only when `encoded.len() >= COMPRESSION_FLOOR` and the
/// compressed form is strictly smaller; otherwise stores the encoded bytes raw.
fn write_page(
    out: &mut Vec<u8>,
    column_id: u32,
    enc: Enc,
    encoded: &[u8],
    zstd_level: i32,
) -> PageDesc {
    let uncomp_len = encoded.len() as u64;
    let (comp, stored): (u8, std::borrow::Cow<'_, [u8]>) = if encoded.len() >= COMPRESSION_FLOOR {
        match zstd::bulk::compress(encoded, zstd_level) {
            Ok(z) if z.len() < encoded.len() => (COMP_ZSTD, std::borrow::Cow::Owned(z)),
            _ => (COMP_NONE, std::borrow::Cow::Borrowed(encoded)),
        }
    } else {
        (COMP_NONE, std::borrow::Cow::Borrowed(encoded))
    };
    let len = stored.len() as u64;
    out.extend_from_slice(&stored);
    PageDesc {
        column_id,
        enc,
        comp,
        len,
        uncomp_len,
    }
}

/// Decodes one page's stored bytes back to its encoded codec bytes. `bytes` is
/// exactly the page's stored payload (`desc.len` bytes).
fn read_page(bytes: &[u8], desc: &PageDesc, max_uncomp: u64) -> Result<Vec<u8>, SpanSegError> {
    if bytes.len() as u64 != desc.len {
        return Err(SpanSegError::Corrupted(format!(
            "page stored length {} != descriptor {}",
            bytes.len(),
            desc.len
        )));
    }
    if desc.uncomp_len > max_uncomp {
        return Err(SpanSegError::Corrupted(format!(
            "page uncomp_len {} exceeds cap {max_uncomp}",
            desc.uncomp_len
        )));
    }
    match desc.comp {
        COMP_NONE => {
            if bytes.len() as u64 != desc.uncomp_len {
                return Err(SpanSegError::Corrupted(format!(
                    "raw page length {} != uncomp_len {}",
                    bytes.len(),
                    desc.uncomp_len
                )));
            }
            Ok(bytes.to_vec())
        }
        COMP_ZSTD => {
            let decoded = zstd::bulk::decompress(bytes, desc.uncomp_len as usize)
                .map_err(|e| SpanSegError::Corrupted(format!("zstd decompress: {e}")))?;
            if decoded.len() as u64 != desc.uncomp_len {
                return Err(SpanSegError::Corrupted(format!(
                    "decompressed length {} != uncomp_len {}",
                    decoded.len(),
                    desc.uncomp_len
                )));
            }
            Ok(decoded)
        }
        other => Err(SpanSegError::Corrupted(format!("unknown comp tag {other}"))),
    }
}

// --- plain columnar codecs -------------------------------------------------

fn encode_i64(values: &[i64]) -> Vec<u8> {
    let mut out = Vec::new();
    for &v in values {
        put_ivarint(&mut out, v);
    }
    out
}

fn decode_i64(enc: Enc, bytes: &[u8], count: usize) -> Result<Vec<i64>, SpanSegError> {
    if enc != Enc::Plain {
        return Err(SpanSegError::Corrupted("i64 page not Plain".into()));
    }
    let mut pos = 0usize;
    let mut out = Vec::with_capacity(count.min(1 << 16));
    for _ in 0..count {
        out.push(get_ivarint(bytes, &mut pos)?);
    }
    expect_consumed(pos, bytes.len())?;
    Ok(out)
}

fn encode_strings(values: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in values {
        put_uvarint(&mut out, v.len() as u64);
    }
    for v in values {
        out.extend_from_slice(v);
    }
    out
}

fn decode_strings(enc: Enc, bytes: &[u8], count: usize) -> Result<Vec<Vec<u8>>, SpanSegError> {
    if enc != Enc::Plain {
        return Err(SpanSegError::Corrupted("string page not Plain".into()));
    }
    let mut pos = 0usize;
    let mut lens = Vec::with_capacity(count.min(1 << 16));
    let mut total: u64 = 0;
    for _ in 0..count {
        let len = get_uvarint(bytes, &mut pos)?;
        total = total
            .checked_add(len)
            .ok_or_else(|| SpanSegError::Corrupted("string length overflow".into()))?;
        lens.push(len);
    }
    let blob = &bytes[pos..];
    if blob.len() as u64 != total {
        return Err(SpanSegError::Corrupted(format!(
            "string blob {} != declared {total}",
            blob.len()
        )));
    }
    let mut out = Vec::with_capacity(count.min(1 << 16));
    let mut off = 0usize;
    for len in lens {
        let len = len as usize;
        out.push(blob[off..off + len].to_vec());
        off += len;
    }
    Ok(out)
}

fn encode_fixed(values: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    for v in values {
        out.extend_from_slice(v);
    }
    out
}

fn decode_fixed(
    enc: Enc,
    bytes: &[u8],
    count: usize,
    width: usize,
) -> Result<Vec<Vec<u8>>, SpanSegError> {
    if enc != Enc::FixedWidth {
        return Err(SpanSegError::Corrupted("fixed page not FixedWidth".into()));
    }
    let need = count
        .checked_mul(width)
        .ok_or_else(|| SpanSegError::Corrupted("fixed length overflow".into()))?;
    if bytes.len() != need {
        return Err(SpanSegError::Corrupted(format!(
            "fixed length {} != {need}",
            bytes.len()
        )));
    }
    let mut out = Vec::with_capacity(count.min(1 << 16));
    for i in 0..count {
        out.push(bytes[i * width..i * width + width].to_vec());
    }
    Ok(out)
}

fn encode_bitmap(bits: &[bool]) -> Vec<u8> {
    let mut out = vec![0u8; bits.len().div_ceil(8)];
    for (i, &b) in bits.iter().enumerate() {
        if b {
            out[i / 8] |= 1u8 << ((i % 8) as u32);
        }
    }
    out
}

fn decode_bitmap(bytes: &[u8], count: usize) -> Result<Vec<bool>, SpanSegError> {
    if bytes.len() != count.div_ceil(8) {
        return Err(SpanSegError::Corrupted(format!(
            "bitmap length {} != {}",
            bytes.len(),
            count.div_ceil(8)
        )));
    }
    let mut out = Vec::with_capacity(count.min(1 << 16));
    for i in 0..count {
        out.push(bytes[i / 8] & (1u8 << ((i % 8) as u32)) != 0);
    }
    Ok(out)
}

fn expect_consumed(pos: usize, len: usize) -> Result<(), SpanSegError> {
    if pos == len {
        Ok(())
    } else {
        Err(SpanSegError::Corrupted(format!(
            "codec left {} of {len} bytes unconsumed",
            len.saturating_sub(pos)
        )))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn span(trace: u8, span: u8, start: i64, end: i64) -> SpanRecord {
        SpanRecord {
            trace_id: [trace; 16],
            span_id: [span; 8],
            parent_span_id: None,
            name: format!("op-{span}"),
            start_ts_ns: start,
            end_ts_ns: end,
            status_code: StatusCode::Unset,
            status_message: None,
            attrs: Vec::new(),
        }
    }

    #[test]
    fn roundtrip_with_nullable_columns() {
        let mut rows = Vec::new();
        for i in 0..100i64 {
            let mut r = span(1, i as u8, 1000 + i, 1000 + i + 5);
            if i % 2 == 0 {
                r.parent_span_id = Some([9u8; 8]);
            }
            if i % 3 == 0 {
                r.status_code = StatusCode::Error;
                r.status_message = Some(format!("err {i}"));
            }
            r.attrs = vec![("k".into(), format!("v{i}"))];
            rows.push(r);
        }
        let out = write_block(&rows, 3).expect("write");
        assert_eq!(out.record_count, 100);
        let dec = read_block(&out.bytes, out.crc32c, DEFAULT_MAX_UNCOMP).expect("read");
        assert_eq!(dec.record_count(), 100);
        for (i, want) in rows.iter().enumerate() {
            assert_eq!(&dec.record(i).expect("record"), want);
        }
    }

    #[test]
    fn crc_mismatch_is_corrupted() {
        let rows = vec![span(1, 0, 5, 6), span(1, 1, 6, 7)];
        let out = write_block(&rows, 3).expect("write");
        let mut bad = out.bytes.clone();
        bad[0] ^= 0xff;
        assert!(matches!(
            read_block(&bad, out.crc32c, DEFAULT_MAX_UNCOMP),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    #[test]
    fn bounds_track_interval_and_trace_ids() {
        let rows = vec![
            span(1, 0, 100, 200),
            span(1, 1, 50, 150),
            span(3, 2, 120, 500),
        ];
        let out = write_block(&rows, 3).expect("write");
        assert_eq!(out.min_start_ts, 50);
        assert_eq!(out.max_end_ts, 500);
        assert_eq!(out.min_trace_id, [1u8; 16]);
        assert_eq!(out.max_trace_id, [3u8; 16]);
    }

    #[test]
    fn duration_and_status_mask_track_rows() {
        let mut ok = span(1, 0, 100, 150); // duration 50, Ok
        ok.status_code = StatusCode::Ok;
        let mut err = span(1, 1, 100, 105); // duration 5, Error
        err.status_code = StatusCode::Error;
        let unset = span(1, 2, 100, 1100); // duration 1000, Unset
        let out = write_block(&[ok, err, unset], 3).expect("write");
        assert_eq!(out.min_duration_ns, 5);
        assert_eq!(out.max_duration_ns, 1000);
        assert_eq!(
            out.status_mask,
            STATUS_BIT_UNSET | STATUS_BIT_OK | STATUS_BIT_ERROR
        );
    }

    #[test]
    fn zero_length_span_round_trips() {
        // start == end: a zero-length span is not rejected or clamped, it
        // round-trips with duration 0.
        let rows = vec![span(1, 0, 500, 500)];
        let out = write_block(&rows, 3).expect("write");
        assert_eq!(out.min_duration_ns, 0);
        assert_eq!(out.max_duration_ns, 0);
        let dec = read_block(&out.bytes, out.crc32c, DEFAULT_MAX_UNCOMP).expect("read");
        assert_eq!(dec.record(0).expect("record"), rows[0]);
    }

    #[test]
    fn end_before_start_yields_negative_duration_not_an_error() {
        // The writer does not reject or clamp end < start: end - start is a
        // valid (negative) i64 that ivarint encodes natively.
        let rows = vec![span(1, 0, 500, 400)];
        let out = write_block(&rows, 3).expect("write");
        assert_eq!(out.min_duration_ns, -100);
        assert_eq!(out.max_duration_ns, -100);
    }

    #[test]
    fn duration_overflow_is_corrupted() {
        let rows = vec![span(1, 0, i64::MIN, i64::MAX)];
        assert!(matches!(
            write_block(&rows, 3),
            Err(SpanSegError::Corrupted(_))
        ));
    }
}
