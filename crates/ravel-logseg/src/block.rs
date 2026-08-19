//! Columnar row blocks (docs/log-segment-format.md "BLOCKS").
//!
//! A block holds a run of records column by column: a header of page
//! descriptors, then the pages. A column present in only some rows of the
//! block carries a presence bitmap page (`enc = Bitmap`) immediately before its
//! value page, so decode restores exact row alignment; a column absent from
//! every row of the block occupies zero bytes. The block's crc32c lives in its
//! SKIP_IDX level-0 entry, not inline; [`read_block`] verifies it before
//! decoding anything.

use std::collections::{HashMap, HashSet};

use crate::encoding::{
    Enc, decode_bitmap, decode_f64, decode_fixed, decode_i64, decode_strings, encode_bitmap,
    encode_f64, encode_fixed, encode_i64, encode_strings,
};
use crate::error::LogSegError;
use crate::page::{PageDesc, read_page, write_page};
use crate::record::{
    COL_ATTRS_RAW, COL_BODY, COL_FLAGS, COL_OBSERVED_TS, COL_SEVERITY_NUM, COL_SEVERITY_TEXT,
    COL_SPAN_ID, COL_STREAM_REF, COL_TRACE_ID, COL_TS, ColumnValue, FieldType, ResolvedRow,
    SPAN_ID_WIDTH, TRACE_ID_WIDTH,
};
use crate::varint::{get_uvarint, put_uvarint};

/// Upper bound on a block's decoded record count (untrusted-input guard). A
/// real block never approaches this; it caps allocation from a hostile header.
const MAX_RECORDS: u64 = 1 << 24;
/// Upper bound on a block's page count. Fixed columns plus the 1000-column
/// dynamic budget, each up to two pages, stays well under this.
const MAX_PAGES: u64 = 4096;

/// The type of a dynamic attribute column, as resolved from FIELD_DIR. Fixed
/// columns (ids 0..=9) carry no plan; their types are implicit.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct ColumnPlan {
    pub column_id: u32,
    pub ty: FieldType,
}

/// Per-numeric-column statistics stored in the SKIP_IDX level-0 entry
/// (docs/log-segment-format.md SKIP_IDX). `min_bits`/`max_bits` hold an i64 as
/// its two's-complement `u64`, or an f64 as its `to_bits` pattern; f64 min/max
/// use `total_cmp` order over non-NaN values, and NaN values are counted in
/// `has_nan` and excluded from min/max.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct NumStat {
    pub column_id: u32,
    pub ty: FieldType,
    pub min_bits: u64,
    pub max_bits: u64,
    pub null_count: u32,
    pub has_nan: bool,
}

/// The output of encoding one block.
#[derive(Clone, Debug)]
pub struct BlockWriteOut {
    pub bytes: Vec<u8>,
    pub crc32c: u32,
    pub record_count: u32,
    pub stats: Vec<NumStat>,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_stream_ref: u32,
    pub max_stream_ref: u32,
}

/// One page staged for a column: its encoding tag and the encoded value bytes.
struct StagedPage {
    column_id: u32,
    enc: Enc,
    bytes: Vec<u8>,
}

/// Stages a column's pages: a presence bitmap page (only when the column is
/// partially present) followed by the value page. A wholly-absent column
/// stages nothing.
fn stage_column(
    pages: &mut Vec<StagedPage>,
    column_id: u32,
    present: &[bool],
    value_enc: Enc,
    value_bytes: Vec<u8>,
) {
    let mut any = false;
    let mut all = true;
    for &p in present {
        any |= p;
        all &= p;
    }
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

/// i64 min/max stat over the present values (two's-complement bits).
fn i64_stat(column_id: u32, vals: &[Option<i64>]) -> NumStat {
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    let mut nulls = 0u32;
    let mut any = false;
    for v in vals {
        match v {
            Some(x) => {
                any = true;
                min = min.min(*x);
                max = max.max(*x);
            }
            None => nulls = nulls.saturating_add(1),
        }
    }
    if !any {
        min = 0;
        max = 0;
    }
    NumStat {
        column_id,
        ty: FieldType::I64,
        min_bits: min as u64,
        max_bits: max as u64,
        null_count: nulls,
        has_nan: false,
    }
}

/// f64 min/max stat over non-NaN present values; NaN presence is flagged and
/// those values are excluded from min/max.
fn f64_stat(column_id: u32, vals: &[Option<u64>]) -> NumStat {
    let mut min: Option<f64> = None;
    let mut max: Option<f64> = None;
    let mut nulls = 0u32;
    let mut has_nan = false;
    for v in vals {
        match v {
            Some(bits) => {
                let f = f64::from_bits(*bits);
                if f.is_nan() {
                    has_nan = true;
                    continue;
                }
                min = Some(match min {
                    Some(m) if m.total_cmp(&f).is_le() => m,
                    _ => f,
                });
                max = Some(match max {
                    Some(m) if m.total_cmp(&f).is_ge() => m,
                    _ => f,
                });
            }
            None => nulls = nulls.saturating_add(1),
        }
    }
    NumStat {
        column_id,
        ty: FieldType::F64,
        min_bits: min.map(f64::to_bits).unwrap_or(0),
        max_bits: max.map(f64::to_bits).unwrap_or(0),
        null_count: nulls,
        has_nan,
    }
}

/// bool min/max stat (0/1 bits) over present values.
fn bool_stat(column_id: u32, vals: &[Option<bool>]) -> NumStat {
    let mut min = 1u64;
    let mut max = 0u64;
    let mut nulls = 0u32;
    let mut any = false;
    for v in vals {
        match v {
            Some(b) => {
                any = true;
                let bit = u64::from(*b);
                min = min.min(bit);
                max = max.max(bit);
            }
            None => nulls = nulls.saturating_add(1),
        }
    }
    if !any {
        min = 0;
        max = 0;
    }
    NumStat {
        column_id,
        ty: FieldType::Bool,
        min_bits: min,
        max_bits: max,
        null_count: nulls,
        has_nan: false,
    }
}

/// Encodes `rows` into one block per the plans for its dynamic columns.
///
/// `rows` must be non-empty. Columns are emitted in ascending column-id order
/// (fixed 0..=9, then dynamic in `plans` order) for byte-deterministic output.
pub fn write_block(
    rows: &[ResolvedRow],
    plans: &[ColumnPlan],
    zstd_level: i32,
) -> Result<BlockWriteOut, LogSegError> {
    if rows.is_empty() {
        return Err(LogSegError::Corrupted("empty block".into()));
    }
    let n = rows.len();
    let mut pages: Vec<StagedPage> = Vec::new();
    let mut stats: Vec<NumStat> = Vec::new();

    // Fixed always-present integer columns.
    let all_present = vec![true; n];
    let ts: Vec<i64> = rows.iter().map(|r| r.ts_ns).collect();
    let (enc, b) = encode_i64(&ts);
    stage_column(&mut pages, COL_TS, &all_present, enc, b);
    let obs: Vec<i64> = rows.iter().map(|r| r.observed_ts_ns).collect();
    let (enc, b) = encode_i64(&obs);
    stage_column(&mut pages, COL_OBSERVED_TS, &all_present, enc, b);
    let sref: Vec<i64> = rows.iter().map(|r| i64::from(r.stream_ref)).collect();
    let (enc, b) = encode_i64(&sref);
    stage_column(&mut pages, COL_STREAM_REF, &all_present, enc, b);
    let sev: Vec<i64> = rows.iter().map(|r| i64::from(r.severity_num)).collect();
    let (enc, b) = encode_i64(&sev);
    stage_column(&mut pages, COL_SEVERITY_NUM, &all_present, enc, b);
    let flags: Vec<i64> = rows.iter().map(|r| i64::from(r.flags)).collect();
    let (enc, b) = encode_i64(&flags);
    stage_column(&mut pages, COL_FLAGS, &all_present, enc, b);

    // Fixed always-present string columns.
    let sev_text: Vec<&[u8]> = rows.iter().map(|r| r.severity_text.as_bytes()).collect();
    let (enc, b) = encode_strings(&sev_text);
    stage_column(&mut pages, COL_SEVERITY_TEXT, &all_present, enc, b);
    let body: Vec<&[u8]> = rows.iter().map(|r| r.body.as_bytes()).collect();
    let (enc, b) = encode_strings(&body);
    stage_column(&mut pages, COL_BODY, &all_present, enc, b);

    // Fixed optional fixed-width id columns.
    let trace_present: Vec<bool> = rows.iter().map(|r| r.trace_id.is_some()).collect();
    let trace_vals: Vec<&[u8]> = rows
        .iter()
        .filter_map(|r| r.trace_id.as_ref().map(|a| a.as_slice()))
        .collect();
    let (enc, b) = encode_fixed(&trace_vals, TRACE_ID_WIDTH);
    stage_column(&mut pages, COL_TRACE_ID, &trace_present, enc, b);
    let span_present: Vec<bool> = rows.iter().map(|r| r.span_id.is_some()).collect();
    let span_vals: Vec<&[u8]> = rows
        .iter()
        .filter_map(|r| r.span_id.as_ref().map(|a| a.as_slice()))
        .collect();
    let (enc, b) = encode_fixed(&span_vals, SPAN_ID_WIDTH);
    stage_column(&mut pages, COL_SPAN_ID, &span_present, enc, b);

    // Fixed optional attrs_raw column (canonical overflow bytes per row).
    let raw_present: Vec<bool> = rows.iter().map(|r| r.attrs_raw.is_some()).collect();
    let raw_vals: Vec<&[u8]> = rows.iter().filter_map(|r| r.attrs_raw.as_deref()).collect();
    let (enc, b) = encode_strings(&raw_vals);
    stage_column(&mut pages, COL_ATTRS_RAW, &raw_present, enc, b);

    // Dynamic columns, in plan order.
    for plan in plans {
        let cid = plan.column_id;
        let present: Vec<bool> = rows.iter().map(|r| row_column(r, cid).is_some()).collect();
        match plan.ty {
            FieldType::I64 => {
                let vals: Vec<Option<i64>> = rows
                    .iter()
                    .map(|r| match row_column(r, cid) {
                        Some(ColumnValue::I64(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                let present_vals: Vec<i64> = vals.iter().flatten().copied().collect();
                let (enc, b) = encode_i64(&present_vals);
                stats.push(i64_stat(cid, &vals));
                stage_column(&mut pages, cid, &present, enc, b);
            }
            FieldType::F64 => {
                let vals: Vec<Option<u64>> = rows
                    .iter()
                    .map(|r| match row_column(r, cid) {
                        Some(ColumnValue::F64(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                let present_vals: Vec<u64> = vals.iter().flatten().copied().collect();
                let (enc, b) = encode_f64(&present_vals);
                stats.push(f64_stat(cid, &vals));
                stage_column(&mut pages, cid, &present, enc, b);
            }
            FieldType::Bool => {
                let vals: Vec<Option<bool>> = rows
                    .iter()
                    .map(|r| match row_column(r, cid) {
                        Some(ColumnValue::Bool(v)) => Some(*v),
                        _ => None,
                    })
                    .collect();
                let present_vals: Vec<bool> = vals.iter().flatten().copied().collect();
                stats.push(bool_stat(cid, &vals));
                stage_column(
                    &mut pages,
                    cid,
                    &present,
                    Enc::Bitmap,
                    encode_bitmap(&present_vals),
                );
            }
            FieldType::Str | FieldType::Bytes => {
                let vals: Vec<&[u8]> = rows
                    .iter()
                    .filter_map(|r| match row_column(r, cid) {
                        Some(ColumnValue::Str(v)) | Some(ColumnValue::Bytes(v)) => {
                            Some(v.as_slice())
                        }
                        _ => None,
                    })
                    .collect();
                let (enc, b) = encode_strings(&vals);
                stage_column(&mut pages, cid, &present, enc, b);
            }
        }
    }

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

    // Header, then the payload.
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
    let min_ts = ts.iter().copied().min().unwrap_or(0);
    let max_ts = ts.iter().copied().max().unwrap_or(0);
    let min_stream_ref = rows.iter().map(|r| r.stream_ref).min().unwrap_or(0);
    let max_stream_ref = rows.iter().map(|r| r.stream_ref).max().unwrap_or(0);

    Ok(BlockWriteOut {
        bytes: block,
        crc32c: crc,
        record_count: n as u32,
        stats,
        min_ts,
        max_ts,
        min_stream_ref,
        max_stream_ref,
    })
}

fn row_column(row: &ResolvedRow, column_id: u32) -> Option<&ColumnValue> {
    row.columns
        .iter()
        .find(|(cid, _)| *cid == column_id)
        .map(|(_, v)| v)
}

/// A decoded block: per-column, per-row values. Every column vector has length
/// `record_count`; `None` marks a row where the column has no value.
///
/// A block decoded through [`read_block_columns`] with a column filter holds
/// only the filtered columns. A column the filter excluded is indistinguishable
/// from a column the block never carried: its accessor returns `None`. Callers
/// that project must therefore know which columns they asked for; nothing here
/// can tell them apart, by design (an absent page and a skipped page decode to
/// the same nothing).
pub struct DecodedBlock {
    record_count: usize,
    i64_cols: HashMap<u32, Vec<Option<i64>>>,
    f64_cols: HashMap<u32, Vec<Option<u64>>>,
    bool_cols: HashMap<u32, Vec<Option<bool>>>,
    str_cols: HashMap<u32, Vec<Option<Vec<u8>>>>,
    fixed_cols: HashMap<u32, Vec<Option<Vec<u8>>>>,
    pages_decoded: usize,
    pages_skipped: usize,
}

impl DecodedBlock {
    pub fn record_count(&self) -> usize {
        self.record_count
    }

    /// Pages this decode decompressed and decoded (value pages and presence
    /// bitmaps alike).
    pub fn pages_decoded(&self) -> usize {
        self.pages_decoded
    }

    /// Pages this decode skipped because the column filter excluded them. Zero
    /// for an all-columns decode. This is the observable proof that column
    /// projection reached the page level rather than being applied after decode.
    pub fn pages_skipped(&self) -> usize {
        self.pages_skipped
    }
    pub fn i64_col(&self, column_id: u32) -> Option<&[Option<i64>]> {
        self.i64_cols.get(&column_id).map(Vec::as_slice)
    }
    pub fn f64_col(&self, column_id: u32) -> Option<&[Option<u64>]> {
        self.f64_cols.get(&column_id).map(Vec::as_slice)
    }
    pub fn bool_col(&self, column_id: u32) -> Option<&[Option<bool>]> {
        self.bool_cols.get(&column_id).map(Vec::as_slice)
    }
    pub fn str_col(&self, column_id: u32) -> Option<&[Option<Vec<u8>>]> {
        self.str_cols.get(&column_id).map(Vec::as_slice)
    }
    pub fn fixed_col(&self, column_id: u32) -> Option<&[Option<Vec<u8>>]> {
        self.fixed_cols.get(&column_id).map(Vec::as_slice)
    }
}

/// How a column decodes, resolved from its id (fixed) or its plan (dynamic).
enum ColKind {
    I64,
    F64,
    Bool,
    Str,
    Fixed(usize),
}

fn column_kind(column_id: u32, plans: &[ColumnPlan]) -> Result<ColKind, LogSegError> {
    Ok(match column_id {
        COL_TS | COL_OBSERVED_TS | COL_STREAM_REF | COL_SEVERITY_NUM | COL_FLAGS => ColKind::I64,
        COL_SEVERITY_TEXT | COL_BODY | COL_ATTRS_RAW => ColKind::Str,
        COL_TRACE_ID => ColKind::Fixed(TRACE_ID_WIDTH),
        COL_SPAN_ID => ColKind::Fixed(SPAN_ID_WIDTH),
        _ => {
            let plan = plans
                .iter()
                .find(|p| p.column_id == column_id)
                .ok_or_else(|| LogSegError::Corrupted(format!("unknown column id {column_id}")))?;
            match plan.ty {
                FieldType::I64 => ColKind::I64,
                FieldType::F64 => ColKind::F64,
                FieldType::Bool => ColKind::Bool,
                FieldType::Str | FieldType::Bytes => ColKind::Str,
            }
        }
    })
}

/// The decoded bytes of a page that the column filter kept. Only ever called
/// for a page whose column was grouped, i.e. one the filter kept, so a `None`
/// here would be an internal inconsistency rather than bad input; it is still
/// reported as a typed error rather than unwrapped.
fn page(pages: &[Option<Vec<u8>>], idx: usize) -> Result<&[u8], LogSegError> {
    pages
        .get(idx)
        .and_then(|p| p.as_deref())
        .ok_or_else(|| LogSegError::Corrupted("page not decoded".into()))
}

/// Scatters `present`-length-`record_count` values into a per-row vector.
/// `values` holds only the present entries, in row order.
fn scatter<T: Clone>(present: &[bool], values: Vec<T>) -> Result<Vec<Option<T>>, LogSegError> {
    let mut out = Vec::with_capacity(present.len());
    let mut it = values.into_iter();
    for &p in present {
        if p {
            let v = it
                .next()
                .ok_or_else(|| LogSegError::Corrupted("presence/value count mismatch".into()))?;
            out.push(Some(v));
        } else {
            out.push(None);
        }
    }
    if it.next().is_some() {
        return Err(LogSegError::Corrupted(
            "value page longer than presence".into(),
        ));
    }
    Ok(out)
}

/// Decodes every column of a block, verifying its crc32c first.
///
/// This is the all-columns case, and it is what
/// [`crate::ranged::RlogRangeReader`] (the compaction merge's reader) and
/// [`crate::RlogReader::scan_pruned`] use: identical decoded output to before
/// column projection existed. Use [`read_block_columns`] to decode a subset.
pub fn read_block(
    bytes: &[u8],
    expected_crc: u32,
    plans: &[ColumnPlan],
    max_uncomp: u64,
) -> Result<DecodedBlock, LogSegError> {
    read_block_columns(bytes, expected_crc, plans, max_uncomp, None)
}

/// Decodes a block, verifying its crc32c first, decompressing and decoding only
/// the pages of the columns `columns` names (ADR-0087 decision 3).
///
/// `columns == None` is the all-columns case ([`read_block`]). Otherwise every
/// page whose `column_id` is not in the set is walked past without a
/// `read_page` call, so its stored bytes are never decompressed and its values
/// are never materialized; the resulting [`DecodedBlock`] reports that column as
/// absent. Both a column's presence bitmap page and its value page are governed
/// by the same decision, so a projected column is never left half-decoded.
///
/// The block's crc32c covers the whole block and is verified in full regardless
/// of the filter: projection changes what is decoded, never what is checked.
/// Every page descriptor is still parsed and every page's stored extent is still
/// walked, so a truncated or over-long block is rejected exactly as it is
/// without a filter.
pub fn read_block_columns(
    bytes: &[u8],
    expected_crc: u32,
    plans: &[ColumnPlan],
    max_uncomp: u64,
    columns: Option<&HashSet<u32>>,
) -> Result<DecodedBlock, LogSegError> {
    if crc32c::crc32c(bytes) != expected_crc {
        return Err(LogSegError::Corrupted("block crc mismatch".into()));
    }
    let mut pos = 0usize;
    let record_count = get_uvarint(bytes, &mut pos)?;
    if record_count > MAX_RECORDS {
        return Err(LogSegError::Corrupted(format!(
            "record_count {record_count} over cap"
        )));
    }
    let record_count = record_count as usize;
    let page_count = get_uvarint(bytes, &mut pos)?;
    if page_count > MAX_PAGES {
        return Err(LogSegError::Corrupted(format!(
            "page_count {page_count} over cap"
        )));
    }
    let page_count = page_count as usize;

    let mut descs: Vec<PageDesc> = Vec::with_capacity(page_count.min(MAX_PAGES as usize));
    for _ in 0..page_count {
        let column_id = get_uvarint(bytes, &mut pos)?;
        let column_id = u32::try_from(column_id)
            .map_err(|_| LogSegError::Corrupted("column id out of range".into()))?;
        let enc = Enc::from_u8(
            *bytes
                .get(pos)
                .ok_or_else(|| LogSegError::Corrupted("block truncated at enc".into()))?,
        )?;
        pos += 1;
        let comp = *bytes
            .get(pos)
            .ok_or_else(|| LogSegError::Corrupted("block truncated at comp".into()))?;
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

    // Whether a column's pages are decoded at all. `None` selects everything.
    let wanted = |cid: u32| match columns {
        None => true,
        Some(set) => set.contains(&cid),
    };

    // Slice each page's stored bytes and decompress to encoded bytes. A page
    // belonging to an unwanted column is walked past: its extent is still
    // validated (so truncation is still caught) but `read_page` is not called,
    // so its bytes are never decompressed.
    let mut page_bytes: Vec<Option<Vec<u8>>> = Vec::with_capacity(descs.len());
    let mut pages_decoded = 0usize;
    let mut pages_skipped = 0usize;
    for d in &descs {
        let len = usize::try_from(d.len)
            .map_err(|_| LogSegError::Corrupted("page len out of range".into()))?;
        let end = pos
            .checked_add(len)
            .ok_or_else(|| LogSegError::Corrupted("page range overflow".into()))?;
        let stored = bytes
            .get(pos..end)
            .ok_or_else(|| LogSegError::Corrupted("page range out of bounds".into()))?;
        if wanted(d.column_id) {
            page_bytes.push(Some(read_page(stored, d, max_uncomp)?));
            pages_decoded += 1;
        } else {
            page_bytes.push(None);
            pages_skipped += 1;
        }
        pos = end;
    }
    if pos != bytes.len() {
        return Err(LogSegError::Corrupted(
            "trailing bytes after block pages".into(),
        ));
    }

    // Group descriptor indices by column id, preserving order. Unwanted columns
    // are not grouped at all, so nothing below ever looks at their pages.
    let mut order: Vec<u32> = Vec::new();
    let mut groups: HashMap<u32, Vec<usize>> = HashMap::new();
    for (i, d) in descs.iter().enumerate() {
        if !wanted(d.column_id) {
            continue;
        }
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
        f64_cols: HashMap::new(),
        bool_cols: HashMap::new(),
        str_cols: HashMap::new(),
        fixed_cols: HashMap::new(),
        pages_decoded,
        pages_skipped,
    };

    for column_id in order {
        let idxs = &groups[&column_id];
        let (present, value_idx): (Vec<bool>, usize) = match idxs.as_slice() {
            [v] => (vec![true; record_count], *v),
            [p, v] => {
                if descs[*p].enc != Enc::Bitmap {
                    return Err(LogSegError::Corrupted("presence page not a bitmap".into()));
                }
                (decode_bitmap(page(&page_bytes, *p)?, record_count)?, *v)
            }
            _ => {
                return Err(LogSegError::Corrupted(format!(
                    "column {column_id} has {} pages",
                    idxs.len()
                )));
            }
        };
        let present_count = present.iter().filter(|&&b| b).count();
        let enc = descs[value_idx].enc;
        let encoded = page(&page_bytes, value_idx)?;
        match column_kind(column_id, plans)? {
            ColKind::I64 => {
                let vals = decode_i64(enc, encoded, present_count)?;
                out.i64_cols.insert(column_id, scatter(&present, vals)?);
            }
            ColKind::F64 => {
                let vals = decode_f64(enc, encoded, present_count)?;
                out.f64_cols.insert(column_id, scatter(&present, vals)?);
            }
            ColKind::Bool => {
                if enc != Enc::Bitmap {
                    return Err(LogSegError::Corrupted(
                        "bool value page not a bitmap".into(),
                    ));
                }
                let vals = decode_bitmap(encoded, present_count)?;
                out.bool_cols.insert(column_id, scatter(&present, vals)?);
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn row(stream_ref: u32, ts: i64) -> ResolvedRow {
        ResolvedRow {
            stream_ref,
            ts_ns: ts,
            observed_ts_ns: ts + 1,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: format!("body {ts}"),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs_raw: None,
            columns: Vec::new(),
            indexed_terms: Vec::new(),
        }
    }

    #[test]
    fn roundtrip_with_mixed_presence() {
        let plans = vec![
            ColumnPlan {
                column_id: 10,
                ty: FieldType::I64,
            },
            ColumnPlan {
                column_id: 11,
                ty: FieldType::Str,
            },
        ];
        let mut rows = Vec::new();
        for i in 0..100i64 {
            let mut r = row(0, 1000 + i);
            // Column 10 present in even rows only.
            if i % 2 == 0 {
                r.columns.push((10, ColumnValue::I64(i * 3)));
            }
            // Column 11 present in first half only.
            if i < 50 {
                r.columns
                    .push((11, ColumnValue::Str(format!("s{i}").into_bytes())));
            }
            if i % 3 == 0 {
                r.trace_id = Some([i as u8; 16]);
            }
            rows.push(r);
        }
        let out = write_block(&rows, &plans, 3).expect("write");
        assert_eq!(out.record_count, 100);
        let dec = read_block(&out.bytes, out.crc32c, &plans, 1 << 30).expect("read");
        assert_eq!(dec.record_count(), 100);

        // Fixed columns.
        let ts = dec.i64_col(COL_TS).expect("ts");
        for (i, v) in ts.iter().enumerate() {
            assert_eq!(*v, Some(1000 + i as i64));
        }
        let body = dec.str_col(COL_BODY).expect("body");
        assert_eq!(body[7], Some(b"body 1007".to_vec()));

        // Dynamic presence.
        let c10 = dec.i64_col(10).expect("c10");
        for (i, v) in c10.iter().enumerate() {
            if i % 2 == 0 {
                assert_eq!(*v, Some(i as i64 * 3));
            } else {
                assert_eq!(*v, None);
            }
        }
        let c11 = dec.str_col(11).expect("c11");
        assert_eq!(c11[10], Some(b"s10".to_vec()));
        assert_eq!(c11[60], None);

        // trace_id presence.
        let tr = dec.fixed_col(COL_TRACE_ID).expect("trace");
        assert_eq!(tr[0], Some(vec![0u8; 16]));
        assert_eq!(tr[1], None);
    }

    #[test]
    fn crc_mismatch_is_corrupted() {
        let rows = vec![row(0, 5), row(0, 6)];
        let out = write_block(&rows, &[], 3).expect("write");
        let mut bad = out.bytes.clone();
        bad[0] ^= 0xff;
        assert!(matches!(
            read_block(&bad, out.crc32c, &[], 1 << 30),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn stats_min_max_null_and_nan() {
        let plans = vec![
            ColumnPlan {
                column_id: 10,
                ty: FieldType::I64,
            },
            ColumnPlan {
                column_id: 11,
                ty: FieldType::F64,
            },
        ];
        let mut rows = Vec::new();
        for i in 0..10i64 {
            let mut r = row(0, i);
            if i < 6 {
                r.columns.push((10, ColumnValue::I64(i - 2)));
            }
            rows.push(r);
        }
        // f64 column: values including a NaN payload; NaN excluded from min/max.
        rows[0]
            .columns
            .push((11, ColumnValue::F64(1.5f64.to_bits())));
        rows[1]
            .columns
            .push((11, ColumnValue::F64((-3.0f64).to_bits())));
        rows[2]
            .columns
            .push((11, ColumnValue::F64((f64::NAN.to_bits()) | 0x7)));
        let out = write_block(&rows, &plans, 3).expect("write");

        let s10 = out.stats.iter().find(|s| s.column_id == 10).expect("s10");
        assert_eq!(s10.min_bits, (-2i64) as u64);
        assert_eq!(s10.max_bits, 3i64 as u64);
        assert_eq!(s10.null_count, 4);
        assert!(!s10.has_nan);

        let s11 = out.stats.iter().find(|s| s.column_id == 11).expect("s11");
        assert!(s11.has_nan);
        assert_eq!(f64::from_bits(s11.min_bits), -3.0);
        assert_eq!(f64::from_bits(s11.max_bits), 1.5);
        assert_eq!(s11.null_count, 7);
    }

    /// A column filter decodes only the named columns' pages, leaves every
    /// other column absent, and reports how many pages it walked past. The
    /// values it does decode are identical to the all-columns decode's, so
    /// projection changes cost, never content.
    #[test]
    fn column_filter_decodes_only_the_named_columns() {
        let plans: Vec<ColumnPlan> = (10..30)
            .map(|column_id| ColumnPlan {
                column_id,
                ty: FieldType::Str,
            })
            .collect();
        let mut rows = Vec::new();
        for i in 0..64i64 {
            let mut r = row(0, 1000 + i);
            for p in &plans {
                r.columns.push((
                    p.column_id,
                    ColumnValue::Str(format!("c{}r{i}", p.column_id).into_bytes()),
                ));
            }
            rows.push(r);
        }
        let out = write_block(&rows, &plans, 3).expect("write");

        let all = read_block(&out.bytes, out.crc32c, &plans, 1 << 30).expect("read all");
        assert_eq!(all.pages_skipped(), 0, "no filter skips nothing");

        let keep: HashSet<u32> = HashSet::from([COL_TS, COL_STREAM_REF, 11, 17]);
        let projected =
            read_block_columns(&out.bytes, out.crc32c, &plans, 1 << 30, Some(&keep)).expect("read");

        assert_eq!(projected.record_count(), 64);
        // Exactly the four requested columns decoded, one page each (every
        // column here is present in every row, so no presence bitmaps).
        assert_eq!(projected.pages_decoded(), 4);
        assert_eq!(
            projected.pages_decoded() + projected.pages_skipped(),
            all.pages_decoded(),
            "every page is either decoded or skipped, none invented"
        );
        assert!(
            projected.pages_skipped() >= 18,
            "the 18 unreferenced dynamic columns must be skipped, got {}",
            projected.pages_skipped()
        );

        // Kept columns decode identically to the unfiltered decode.
        assert_eq!(projected.i64_col(COL_TS), all.i64_col(COL_TS));
        assert_eq!(projected.str_col(11), all.str_col(11));
        assert_eq!(projected.str_col(17), all.str_col(17));
        // Excluded columns are absent, including fixed ones.
        assert!(projected.str_col(12).is_none());
        assert!(projected.str_col(COL_BODY).is_none());
        assert!(all.str_col(COL_BODY).is_some());
    }

    /// A filter never weakens the integrity checks: the block crc still covers
    /// the whole block, so a corrupted byte inside a *skipped* column's page is
    /// still caught.
    #[test]
    fn column_filter_still_verifies_the_whole_block_crc() {
        let plans = vec![ColumnPlan {
            column_id: 10,
            ty: FieldType::Str,
        }];
        let mut rows = vec![row(0, 1), row(0, 2)];
        for r in &mut rows {
            r.columns.push((10, ColumnValue::Str(b"payload".to_vec())));
        }
        let out = write_block(&rows, &plans, 3).expect("write");
        let keep: HashSet<u32> = HashSet::from([COL_TS, COL_STREAM_REF]);
        // Corrupt the tail, which belongs to a column the filter skips.
        let mut bad = out.bytes.clone();
        let last = bad.len() - 1;
        bad[last] ^= 0xff;
        assert!(matches!(
            read_block_columns(&bad, out.crc32c, &plans, 1 << 30, Some(&keep)),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn absent_column_occupies_no_page() {
        // Column 12 never present -> no page, decodes as absent everywhere.
        let plans = vec![ColumnPlan {
            column_id: 12,
            ty: FieldType::Bool,
        }];
        let rows = vec![row(0, 1), row(0, 2)];
        let out = write_block(&rows, &plans, 3).expect("write");
        let dec = read_block(&out.bytes, out.crc32c, &plans, 1 << 30).expect("read");
        assert!(dec.bool_col(12).is_none());
    }
}
