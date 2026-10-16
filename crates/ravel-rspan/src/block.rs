//! Columnar row blocks (docs/span-segment-format.md "BLOCKS").
//!
//! A block holds a run of spans column by column: a header of page descriptors,
//! then a self-describing directory naming the block's dynamic attribute
//! columns, then the pages. Each column is one value page, preceded by a
//! presence bitmap page (`enc = Bitmap`) when the column is nullable and not
//! present in every row of the block.
//!
//! v4 (ADR-0045 decision 3) ports RLOG's per-key column design onto RSPAN: the
//! merged attribute map is split into one string column per key, overflow keys
//! past the 1000-column budget fold into a canonical [`record::COL_ATTRS_RAW`]
//! blob, and span events become nested columns
//! ([`record::COL_EVENT_TS`]/[`record::COL_EVENT_NAME`]/[`record::COL_EVENT_ATTRS_BLOB`]
//! flattened one entry per event, with a per-row [`record::COL_EVENT_COUNT`]).
//! `service.name` stays in its own column (id 9, v3). Value codecs are
//! `ravel-codec`'s (the crate extracted from ravel-logseg), shared with RLOG.
//!
//! Unlike RLOG, RSPAN has no object-wide FIELD_DIR section: each block embeds
//! the `(column_id, name)` directory for the dynamic columns it carries, so a
//! block decodes with no external directory. This keeps the whole-object and
//! ranged decode paths (`read_block`, `RspanRangeReader`) self-contained, and
//! is consistent with RSPAN's existing "no STREAM_DIR/FIELD_DIR" leanness.
//! RSPAN's attribute map is `Map<Utf8, Utf8>`, so every dynamic column is a
//! string column and there is no per-type split (a named simplification from
//! RLOG's typed FIELD_DIR).
//!
//! The block's crc32c lives in its SKIP_IDX entry, not inline; [`read_block`]
//! verifies it before decoding anything. Every offset, length, and count read
//! from the block is untrusted: bounds-checked, overflow-checked, and turned
//! into a typed `Corrupted` error, never a panic.

use std::collections::{BTreeMap, HashMap, HashSet};

use ravel_codec::encoding::{
    Enc, decode_bitmap, decode_fixed, decode_i64, decode_strings, encode_bitmap, encode_fixed,
    encode_i64, encode_strings,
};

use crate::error::SpanSegError;
use crate::record::{
    COL_ATTRS_RAW, COL_END_TS, COL_EVENT_ATTRS_BLOB, COL_EVENT_COUNT, COL_EVENT_NAME, COL_EVENT_TS,
    COL_NAME, COL_PARENT_SPAN_ID, COL_SERVICE_NAME, COL_SPAN_ID, COL_START_TS, COL_STATUS_CODE,
    COL_STATUS_MESSAGE, COL_TRACE_ID, EVENTS_RAW_KEY, FIRST_DYNAMIC_COL, ResolvedSpanRow,
    SERVICE_NAME_KEY, SPAN_ID_WIDTH, SpanEvent, SpanRecord, StatusCode, TRACE_ID_WIDTH,
    decode_attrs, reconstruct_events_raw,
};
use crate::skip_index::{STATUS_BIT_ERROR, STATUS_BIT_OK, STATUS_BIT_UNSET};
use crate::varint::{get_uvarint, put_uvarint};

/// Upper bound on a block's decoded record count (untrusted-input guard).
const MAX_RECORDS: u64 = 1 << 24;
/// Upper bound on a block's total event count across all rows.
const MAX_EVENTS: u64 = 1 << 24;
/// Upper bound on a block's page count. The fixed columns plus the
/// 1000-dynamic-column budget, each up to two pages, plus the flattened event
/// columns, stays well under this.
const MAX_PAGES: u64 = 4096;
/// Default per-page decompressed-size cap (zstd bomb guard).
pub const DEFAULT_MAX_UNCOMP: u64 = 64 << 20;

/// `comp` tag: stored raw.
const COMP_NONE: u8 = 0;
/// `comp` tag: zstd frame.
const COMP_ZSTD: u8 = 2;

/// Page compression floor: pages under this many encoded bytes stay raw.
const COMPRESSION_FLOOR: usize = 512;

/// The name and id of one dynamic attribute column, object-wide. The writer
/// assigns these; [`write_block`] embeds the ids that appear in a block into
/// that block's directory so the block decodes without an external directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnPlan {
    pub column_id: u32,
    pub name: String,
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

fn row_column(row: &ResolvedSpanRow, column_id: u32) -> Option<&str> {
    row.columns
        .iter()
        .find(|(cid, _)| *cid == column_id)
        .map(|(_, v)| v.as_str())
}

/// Encodes `rows` into one block. `rows` must be non-empty and already sorted by
/// `(trace_id, start_ts)`. `plans` names every object-wide dynamic column; the
/// ones present in this block are staged and their `(id, name)` embedded in the
/// block directory. Columns are emitted in ascending column-id order for
/// byte-deterministic output.
pub fn write_block(
    rows: &[ResolvedSpanRow],
    plans: &[ColumnPlan],
    zstd_level: i32,
) -> Result<BlockWriteOut, SpanSegError> {
    if rows.is_empty() {
        return Err(SpanSegError::Corrupted("empty block".into()));
    }
    let n = rows.len();
    let all_present = vec![true; n];
    let mut pages: Vec<StagedPage> = Vec::new();
    // Dynamic column ids present in this block, with their names, ascending.
    let mut dyn_dir: Vec<(u32, String)> = Vec::new();

    // COL_TRACE_ID: fixed 16, always present.
    let trace_vals: Vec<&[u8]> = rows.iter().map(|r| r.trace_id.as_slice()).collect();
    let (enc, b) = encode_fixed(&trace_vals, TRACE_ID_WIDTH);
    stage_column(&mut pages, COL_TRACE_ID, &all_present, enc, b);
    // COL_SPAN_ID: fixed 8, always present.
    let span_vals: Vec<&[u8]> = rows.iter().map(|r| r.span_id.as_slice()).collect();
    let (enc, b) = encode_fixed(&span_vals, SPAN_ID_WIDTH);
    stage_column(&mut pages, COL_SPAN_ID, &all_present, enc, b);
    // COL_PARENT_SPAN_ID: fixed 8, nullable (root spans have none).
    let parent_present: Vec<bool> = rows.iter().map(|r| r.parent_span_id.is_some()).collect();
    let parent_vals: Vec<&[u8]> = rows
        .iter()
        .filter_map(|r| r.parent_span_id.as_ref().map(|a| a.as_slice()))
        .collect();
    let (enc, b) = encode_fixed(&parent_vals, SPAN_ID_WIDTH);
    stage_column(&mut pages, COL_PARENT_SPAN_ID, &parent_present, enc, b);
    // COL_NAME: str, always present.
    let name_vals: Vec<&[u8]> = rows.iter().map(|r| r.name.as_bytes()).collect();
    let (enc, b) = encode_strings(&name_vals);
    stage_column(&mut pages, COL_NAME, &all_present, enc, b);
    // COL_START_TS / COL_END_TS / COL_STATUS_CODE: i64, always present.
    let start: Vec<i64> = rows.iter().map(|r| r.start_ts_ns).collect();
    let (enc, b) = encode_i64(&start);
    stage_column(&mut pages, COL_START_TS, &all_present, enc, b);
    let end: Vec<i64> = rows.iter().map(|r| r.end_ts_ns).collect();
    let (enc, b) = encode_i64(&end);
    stage_column(&mut pages, COL_END_TS, &all_present, enc, b);
    let status: Vec<i64> = rows
        .iter()
        .map(|r| i64::from(r.status_code.to_u8()))
        .collect();
    let (enc, b) = encode_i64(&status);
    stage_column(&mut pages, COL_STATUS_CODE, &all_present, enc, b);
    // COL_STATUS_MESSAGE: str, nullable.
    let msg_present: Vec<bool> = rows.iter().map(|r| r.status_message.is_some()).collect();
    let msg_vals: Vec<&[u8]> = rows
        .iter()
        .filter_map(|r| r.status_message.as_deref().map(str::as_bytes))
        .collect();
    let (enc, b) = encode_strings(&msg_vals);
    stage_column(&mut pages, COL_STATUS_MESSAGE, &msg_present, enc, b);
    // COL_ATTRS_RAW: canonical overflow blob per row, str column, nullable.
    let raw_present: Vec<bool> = rows.iter().map(|r| r.attrs_raw.is_some()).collect();
    let raw_vals: Vec<&[u8]> = rows.iter().filter_map(|r| r.attrs_raw.as_deref()).collect();
    let (enc, b) = encode_strings(&raw_vals);
    stage_column(&mut pages, COL_ATTRS_RAW, &raw_present, enc, b);
    // COL_SERVICE_NAME: the `service.name` value lifted out of attrs (v3),
    // string column, nullable.
    let service_present: Vec<bool> = rows.iter().map(|r| r.service_name.is_some()).collect();
    let service_vals: Vec<&[u8]> = rows
        .iter()
        .filter_map(|r| r.service_name.as_deref().map(str::as_bytes))
        .collect();
    let (enc, b) = encode_strings(&service_vals);
    stage_column(&mut pages, COL_SERVICE_NAME, &service_present, enc, b);

    // Event columns (v4). event_count is one value per row; event_ts/name/blob
    // are flattened, one entry per event across the whole block. Staged only
    // when the block carries at least one event.
    let counts: Vec<i64> = rows.iter().map(|r| r.events.len() as i64).collect();
    let total_events: usize = rows.iter().map(|r| r.events.len()).sum();
    if total_events > 0 {
        let (enc, b) = encode_i64(&counts);
        stage_column(&mut pages, COL_EVENT_COUNT, &all_present, enc, b);
        let ev_ts: Vec<i64> = rows
            .iter()
            .flat_map(|r| r.events.iter().map(|e| e.ts_ns))
            .collect();
        let (enc, b) = encode_i64(&ev_ts);
        pages.push(StagedPage {
            column_id: COL_EVENT_TS,
            enc,
            bytes: b,
        });
        let ev_name: Vec<&[u8]> = rows
            .iter()
            .flat_map(|r| r.events.iter().map(|e| e.name.as_bytes()))
            .collect();
        let (enc, b) = encode_strings(&ev_name);
        pages.push(StagedPage {
            column_id: COL_EVENT_NAME,
            enc,
            bytes: b,
        });
        let ev_blob: Vec<&[u8]> = rows
            .iter()
            .flat_map(|r| r.events.iter().map(|e| e.attrs_blob.as_slice()))
            .collect();
        let (enc, b) = encode_strings(&ev_blob);
        pages.push(StagedPage {
            column_id: COL_EVENT_ATTRS_BLOB,
            enc,
            bytes: b,
        });
    }

    // Dynamic per-key string columns, in ascending column-id (== plan) order.
    for plan in plans {
        let cid = plan.column_id;
        let present: Vec<bool> = rows.iter().map(|r| row_column(r, cid).is_some()).collect();
        if !present.iter().any(|&p| p) {
            continue;
        }
        let vals: Vec<&[u8]> = rows
            .iter()
            .filter_map(|r| row_column(r, cid).map(str::as_bytes))
            .collect();
        let (enc, b) = encode_strings(&vals);
        stage_column(&mut pages, cid, &present, enc, b);
        dyn_dir.push((cid, plan.name.clone()));
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

    // Header, dynamic-column directory, then payload.
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
    put_uvarint(&mut block, dyn_dir.len() as u64);
    for (cid, name) in &dyn_dir {
        put_uvarint(&mut block, u64::from(*cid));
        put_uvarint(&mut block, name.len() as u64);
        block.extend_from_slice(name.as_bytes());
    }
    block.extend_from_slice(&payload);

    let crc = crc32c::crc32c(&block);
    let min_start_ts = start.iter().copied().min().unwrap_or(0);
    let max_end_ts = end.iter().copied().max().unwrap_or(0);
    let min_trace_id = rows[0].trace_id;
    let max_trace_id = rows[n - 1].trace_id;

    // Duration and status_mask (ADR-0045 decision 2), derived from the same
    // start/end/status vectors already scanned above, never stored per row.
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

/// Which columns a decode was asked to materialize (docs/adrs/0110 decision 1).
/// [`read_block`] uses [`ColumnProjection::All`]; [`read_block_projected`] uses
/// [`ColumnProjection::Only`] with the caller's set. The distinction is what
/// lets the columnar view tell a column that is absent from the block (a
/// legitimate `NULL` for every row) from one that was never requested (a caller
/// bug, a typed [`SpanSegError::ColumnNotRequested`]).
enum ColumnProjection {
    /// Decode every page, exactly as the reader did before projection existed.
    All,
    /// Decode only the pages for these column ids (plus the event columns as a
    /// group when any event column is named).
    Only(HashSet<u32>),
}

impl ColumnProjection {
    /// Whether column `col` was requested by the caller, verbatim. Event columns
    /// are not group-expanded here: this reflects the caller's literal request,
    /// so an accessor for a column the caller did not name fails loudly.
    fn is_requested(&self, col: u32) -> bool {
        match self {
            ColumnProjection::All => true,
            ColumnProjection::Only(set) => set.contains(&col),
        }
    }

    /// Whether the flattened nested event columns should be decoded and each
    /// row's events reconstructed. `true` for a full decode; for a projected
    /// decode, `true` only when the caller named at least one event column, so
    /// `decode_events` never runs for a projection that excludes them all.
    fn wants_events(&self) -> bool {
        match self {
            ColumnProjection::All => true,
            ColumnProjection::Only(set) => {
                set.contains(&COL_EVENT_COUNT)
                    || set.contains(&COL_EVENT_TS)
                    || set.contains(&COL_EVENT_NAME)
                    || set.contains(&COL_EVENT_ATTRS_BLOB)
            }
        }
    }

    /// Whether the page(s) for column `col` must be decompressed and decoded.
    /// The four event columns decode as a group (all or none), because
    /// reconstructing a row's events needs `event_count` and all three value
    /// columns together; every other column decodes iff it was requested.
    fn page_needed(&self, col: u32) -> bool {
        if matches!(
            col,
            COL_EVENT_COUNT | COL_EVENT_TS | COL_EVENT_NAME | COL_EVENT_ATTRS_BLOB
        ) {
            self.wants_events()
        } else {
            self.is_requested(col)
        }
    }
}

/// A decoded block: per-column, per-row values plus the reconstructed nested
/// events. Every fixed/dynamic column vector has length `record_count`; `None`
/// marks a row where a nullable column has no value. A projected decode
/// ([`read_block_projected`]) holds only the requested columns; the rest are
/// absent from these maps and reachable only as a typed error through the view.
pub struct DecodedBlock {
    record_count: usize,
    i64_cols: HashMap<u32, Vec<Option<i64>>>,
    str_cols: HashMap<u32, Vec<Option<Vec<u8>>>>,
    fixed_cols: HashMap<u32, Vec<Option<Vec<u8>>>>,
    /// Dynamic attribute column id -> attribute name, from the block directory.
    dyn_names: BTreeMap<u32, String>,
    /// Per-row events, length `record_count`; empty vec for a row with none.
    events: Vec<Vec<SpanEvent>>,
    /// The projection this block was decoded under, for the view's
    /// requested-vs-absent distinction.
    projection: ColumnProjection,
    /// Whether the block's header carried a [`COL_ATTRS_RAW`] page. Read from
    /// the page descriptors, so it holds even when `attrs_raw` was projected
    /// away and never decoded.
    has_attrs_raw_page: bool,
    /// Pages this decode decompressed and decoded.
    pages_decoded: usize,
    /// Pages this decode skipped because the projection excluded their column.
    pages_skipped: usize,
}

impl DecodedBlock {
    pub fn record_count(&self) -> usize {
        self.record_count
    }

    /// Pages this decode decompressed and decoded. A projected decode reports
    /// strictly fewer than a full decode of the same block whenever the block
    /// carries a page the projection excluded (docs/adrs/0110 decision 1).
    pub fn pages_decoded(&self) -> usize {
        self.pages_decoded
    }

    /// Pages this decode skipped because the projection excluded their column.
    /// Zero for a full decode.
    pub fn pages_skipped(&self) -> usize {
        self.pages_skipped
    }

    /// Whether this block carries a [`COL_ATTRS_RAW`] overflow page, answered
    /// from the block header's page descriptors without decoding it
    /// (docs/adrs/0110 decision 1, the SQL eligibility rule in T3). A block
    /// whose records all fit their per-key columns has no `attrs_raw` page at
    /// all, so this is a metadata read; decoding the page to discover it is
    /// empty would pay exactly the cost the projection removes.
    pub fn has_attrs_raw_page(&self) -> bool {
        self.has_attrs_raw_page
    }

    /// A borrowed columnar view over this block's decoded columns
    /// (docs/adrs/0110 decision 1). The view exposes per-column typed accessors
    /// and gather iterators over caller-supplied surviving row indices; it never
    /// hands out a storage vector, so a later change to how string columns are
    /// stored lands without touching a caller.
    pub fn view(&self) -> crate::columnar::ColumnarBlockView<'_> {
        crate::columnar::ColumnarBlockView::new(self)
    }

    /// Whether `col` was requested by the decode that produced this block.
    /// `true` for every column of a full decode.
    pub(crate) fn is_requested(&self, col: u32) -> bool {
        self.projection.is_requested(col)
    }

    /// The decoded i64 column `col`, or `None` when the column is absent from
    /// the block. Length `record_count`. Crate-internal: the columnar view is
    /// the only public reader, and it never leaks this storage type.
    pub(crate) fn i64_col(&self, col: u32) -> Option<&[Option<i64>]> {
        self.i64_cols.get(&col).map(Vec::as_slice)
    }

    /// The decoded string column `col`, or `None` when it is absent from the
    /// block. Length `record_count`.
    pub(crate) fn str_col(&self, col: u32) -> Option<&[Option<Vec<u8>>]> {
        self.str_cols.get(&col).map(Vec::as_slice)
    }

    /// The decoded fixed-width column `col`, or `None` when it is absent from
    /// the block. Length `record_count`.
    pub(crate) fn fixed_col(&self, col: u32) -> Option<&[Option<Vec<u8>>]> {
        self.fixed_cols.get(&col).map(Vec::as_slice)
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

    /// The decoded `service_name` value for `row` (v3, ADR-0054), or `None`
    /// when the row's merged attrs carried no `service.name`. Reads the
    /// dedicated column directly, without going through the attribute map.
    pub fn service_name(&self, row: usize) -> Option<Vec<u8>> {
        self.str_at(COL_SERVICE_NAME, row)
    }

    /// Rebuilds the [`SpanRecord`] at `row`, a faithful round-trip of the record
    /// the writer was handed: fixed fields, then the attribute map reassembled
    /// from the per-key columns, the `attrs_raw` overflow, the lifted
    /// `service.name`, and the `_events_raw` blob reconstructed from the nested
    /// event columns.
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

        // Reassemble the attribute map from the per-key columns, the overflow
        // blob, the lifted service.name, and the reconstructed events. A
        // BTreeMap keeps the canonical sorted, unique-key form the writer's
        // input canonicalizes to, so the round trip is exact.
        let mut attrs: BTreeMap<String, String> = BTreeMap::new();
        for (&cid, dyn_name) in &self.dyn_names {
            if let Some(bytes) = self.str_at(cid, row) {
                attrs.insert(dyn_name.clone(), string_from(bytes)?);
            }
        }
        if let Some(bytes) = self.str_at(COL_ATTRS_RAW, row) {
            for (k, v) in decode_attrs(&bytes)? {
                attrs.insert(k, v);
            }
        }
        if let Some(bytes) = self.service_name(row) {
            attrs.insert(SERVICE_NAME_KEY.to_string(), string_from(bytes)?);
        }
        let events = self
            .events
            .get(row)
            .ok_or_else(|| SpanSegError::Corrupted("event row out of range".into()))?;
        if !events.is_empty() {
            attrs.insert(EVENTS_RAW_KEY.to_string(), reconstruct_events_raw(events));
        }

        Ok(SpanRecord {
            trace_id,
            span_id,
            parent_span_id,
            name,
            start_ts_ns,
            end_ts_ns,
            status_code,
            status_message,
            attrs: attrs.into_iter().collect(),
        })
    }
}

fn string_from(bytes: Vec<u8>) -> Result<String, SpanSegError> {
    String::from_utf8(bytes).map_err(|_| SpanSegError::Corrupted("value not utf-8".into()))
}

/// How a column decodes, resolved from its (fixed) id or its dynamic plan.
enum ColKind {
    I64,
    Str,
    Fixed(usize),
}

fn column_kind(column_id: u32, dyn_names: &BTreeMap<u32, String>) -> Result<ColKind, SpanSegError> {
    Ok(match column_id {
        COL_TRACE_ID => ColKind::Fixed(TRACE_ID_WIDTH),
        COL_SPAN_ID | COL_PARENT_SPAN_ID => ColKind::Fixed(SPAN_ID_WIDTH),
        COL_NAME | COL_STATUS_MESSAGE | COL_ATTRS_RAW | COL_SERVICE_NAME => ColKind::Str,
        COL_START_TS | COL_END_TS | COL_STATUS_CODE | COL_EVENT_COUNT => ColKind::I64,
        _ if column_id >= FIRST_DYNAMIC_COL && dyn_names.contains_key(&column_id) => ColKind::Str,
        other => {
            return Err(SpanSegError::Corrupted(format!(
                "unknown column id {other}"
            )));
        }
    })
}

/// Decodes a block, verifying its crc32c first. Every page is decoded, exactly
/// as before column projection existed. This is [`read_block_projected`] with
/// the full column set: the whole-object reader, the ranged reader, and
/// compaction all take this path and stay byte-identical.
pub fn read_block(
    bytes: &[u8],
    expected_crc: u32,
    max_uncomp: u64,
) -> Result<DecodedBlock, SpanSegError> {
    read_block_inner(bytes, expected_crc, max_uncomp, ColumnProjection::All)
}

/// Decodes only the pages for `columns`, verifying the block's crc32c first
/// (docs/adrs/0110 decision 1). A page whose column id is not in `columns` is
/// neither decompressed nor allocated; in particular the nested event columns
/// are decoded, and each row's events reconstructed, only when `columns` names
/// at least one event column ([`COL_EVENT_COUNT`]/[`COL_EVENT_TS`]/
/// [`COL_EVENT_NAME`]/[`COL_EVENT_ATTRS_BLOB`]).
///
/// The returned [`DecodedBlock`] holds only the requested columns. Reading a
/// column that was not requested through the block's columnar view is a typed
/// [`SpanSegError::ColumnNotRequested`], never a silent column of nulls; a
/// column that was requested but is genuinely absent from this block reads as a
/// legitimate `NULL` for every row.
pub fn read_block_projected(
    bytes: &[u8],
    expected_crc: u32,
    max_uncomp: u64,
    columns: &[u32],
) -> Result<DecodedBlock, SpanSegError> {
    let set: HashSet<u32> = columns.iter().copied().collect();
    read_block_inner(bytes, expected_crc, max_uncomp, ColumnProjection::Only(set))
}

fn read_block_inner(
    bytes: &[u8],
    expected_crc: u32,
    max_uncomp: u64,
    projection: ColumnProjection,
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

    // Dynamic-column directory: (column_id, name) for the block's dynamic
    // columns, ascending and each id in the dynamic range. Untrusted.
    let dir_count = get_uvarint(bytes, &mut pos)?;
    if dir_count > MAX_PAGES {
        return Err(SpanSegError::Corrupted(format!(
            "dyn dir count {dir_count} over cap"
        )));
    }
    let mut dyn_names: BTreeMap<u32, String> = BTreeMap::new();
    let mut prev_dir: Option<u32> = None;
    for _ in 0..dir_count {
        let cid = u32::try_from(get_uvarint(bytes, &mut pos)?)
            .map_err(|_| SpanSegError::Corrupted("dyn dir column id range".into()))?;
        if cid < FIRST_DYNAMIC_COL {
            return Err(SpanSegError::Corrupted(format!(
                "dyn dir column id {cid} below dynamic range"
            )));
        }
        if prev_dir.is_some_and(|p| cid <= p) {
            return Err(SpanSegError::Corrupted("dyn dir not ascending".into()));
        }
        prev_dir = Some(cid);
        let name_len = usize::try_from(get_uvarint(bytes, &mut pos)?)
            .map_err(|_| SpanSegError::Corrupted("dyn dir name len range".into()))?;
        let end = pos
            .checked_add(name_len)
            .ok_or_else(|| SpanSegError::Corrupted("dyn dir name overflow".into()))?;
        let name_bytes = bytes
            .get(pos..end)
            .ok_or_else(|| SpanSegError::Corrupted("dyn dir name truncated".into()))?;
        let name = std::str::from_utf8(name_bytes)
            .map_err(|_| SpanSegError::Corrupted("dyn dir name not utf-8".into()))?
            .to_string();
        pos = end;
        dyn_names.insert(cid, name);
    }

    // Whether the block carries an attrs_raw overflow page, from the
    // descriptors alone (docs/adrs/0110 decision 1): a metadata read that holds
    // even when the projection excludes the column and never decodes it.
    let has_attrs_raw_page = descs.iter().any(|d| d.column_id == COL_ATTRS_RAW);

    // Slice each page, decompressing only the ones the projection needs. A page
    // whose column is not requested keeps its bytes untouched (no decompress,
    // no allocation): its slot holds an empty placeholder and is never read.
    // The byte range is still walked so `pos` advances and the trailing-bytes
    // check below stays exact.
    let mut page_bytes: Vec<Vec<u8>> = Vec::with_capacity(descs.len());
    let mut pages_decoded = 0usize;
    let mut pages_skipped = 0usize;
    for d in &descs {
        let len = usize::try_from(d.len)
            .map_err(|_| SpanSegError::Corrupted("page len out of range".into()))?;
        let end = pos
            .checked_add(len)
            .ok_or_else(|| SpanSegError::Corrupted("page range overflow".into()))?;
        let stored = bytes
            .get(pos..end)
            .ok_or_else(|| SpanSegError::Corrupted("page range out of bounds".into()))?;
        if projection.page_needed(d.column_id) {
            page_bytes.push(read_page(stored, d, max_uncomp)?);
            pages_decoded += 1;
        } else {
            page_bytes.push(Vec::new());
            pages_skipped += 1;
        }
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
        dyn_names,
        events: vec![Vec::new(); record_count],
        projection,
        has_attrs_raw_page,
        pages_decoded,
        pages_skipped,
    };

    // Decode fixed and dynamic columns via the presence/scatter model. The
    // flattened event value columns (event_ts/name/blob) have `total_events`
    // entries, not `record_count`, so they are decoded separately below.
    for &column_id in &order {
        if matches!(
            column_id,
            COL_EVENT_TS | COL_EVENT_NAME | COL_EVENT_ATTRS_BLOB
        ) {
            continue;
        }
        // Skip a column whose page the projection did not decode; it stays
        // absent from the decoded maps and is reachable only as a typed error
        // through the view.
        if !out.projection.page_needed(column_id) {
            continue;
        }
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
        match column_kind(column_id, &out.dyn_names)? {
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

    // Reconstruct the nested events only when the projection asked for them; a
    // decode that named no event column never touches their pages (they were
    // left undecoded above) and leaves every row's events empty.
    if out.projection.wants_events() {
        decode_events(&mut out, &descs, &page_bytes, &groups)?;
    }

    Ok(out)
}

/// Decodes the nested event columns and reconstructs each row's events.
///
/// `event_count` is a normal per-row i64 column decoded above; its sum is the
/// number of entries in each of the three flattened value columns. If
/// `event_count` is absent, the block carries no events and none of the value
/// columns may be present; if it is present, all three value columns must be
/// present, each a single value page (no presence bitmap). Every count and
/// index is bounds- and overflow-checked.
fn decode_events(
    out: &mut DecodedBlock,
    descs: &[PageDesc],
    page_bytes: &[Vec<u8>],
    groups: &HashMap<u32, Vec<usize>>,
) -> Result<(), SpanSegError> {
    let value_cols_present = groups.contains_key(&COL_EVENT_TS)
        || groups.contains_key(&COL_EVENT_NAME)
        || groups.contains_key(&COL_EVENT_ATTRS_BLOB);
    let counts = match out.i64_cols.get(&COL_EVENT_COUNT) {
        None => {
            if value_cols_present {
                return Err(SpanSegError::Corrupted(
                    "event value columns present without event_count".into(),
                ));
            }
            return Ok(());
        }
        Some(c) => c,
    };

    // Every row's count is present (event_count is an all-present column) and
    // non-negative; the total is the flattened value-column length.
    let mut total: u64 = 0;
    let mut per_row: Vec<usize> = Vec::with_capacity(counts.len());
    for c in counts {
        let c = c.ok_or_else(|| SpanSegError::Corrupted("event_count has a null row".into()))?;
        if c < 0 {
            return Err(SpanSegError::Corrupted("negative event_count".into()));
        }
        total = total
            .checked_add(c as u64)
            .ok_or_else(|| SpanSegError::Corrupted("event_count sum overflow".into()))?;
        per_row.push(c as usize);
    }
    if total > MAX_EVENTS {
        return Err(SpanSegError::Corrupted(format!(
            "event total {total} over cap"
        )));
    }
    let total = total as usize;

    let ev_ts = decode_event_i64(COL_EVENT_TS, descs, page_bytes, groups, total)?;
    let ev_name = decode_event_strings(COL_EVENT_NAME, descs, page_bytes, groups, total)?;
    let ev_blob = decode_event_strings(COL_EVENT_ATTRS_BLOB, descs, page_bytes, groups, total)?;

    let mut k = 0usize;
    for (row, &c) in per_row.iter().enumerate() {
        let mut evs = Vec::with_capacity(c);
        for _ in 0..c {
            let name = string_from(ev_name[k].clone())?;
            evs.push(SpanEvent {
                ts_ns: ev_ts[k],
                name,
                attrs_blob: ev_blob[k].clone(),
            });
            k += 1;
        }
        out.events[row] = evs;
    }
    Ok(())
}

/// Resolves the single value page of a flattened event column (no presence
/// bitmap; every entry is present).
fn event_value_page<'a>(
    column_id: u32,
    descs: &[PageDesc],
    page_bytes: &'a [Vec<u8>],
    groups: &HashMap<u32, Vec<usize>>,
) -> Result<(Enc, &'a [u8]), SpanSegError> {
    match groups.get(&column_id).map(Vec::as_slice) {
        Some([v]) => Ok((descs[*v].enc, page_bytes[*v].as_slice())),
        _ => Err(SpanSegError::Corrupted(format!(
            "event column {column_id} must be a single value page"
        ))),
    }
}

fn decode_event_i64(
    column_id: u32,
    descs: &[PageDesc],
    page_bytes: &[Vec<u8>],
    groups: &HashMap<u32, Vec<usize>>,
    count: usize,
) -> Result<Vec<i64>, SpanSegError> {
    let (enc, encoded) = event_value_page(column_id, descs, page_bytes, groups)?;
    Ok(decode_i64(enc, encoded, count)?)
}

fn decode_event_strings(
    column_id: u32,
    descs: &[PageDesc],
    page_bytes: &[Vec<u8>],
    groups: &HashMap<u32, Vec<usize>>,
    count: usize,
) -> Result<Vec<Vec<u8>>, SpanSegError> {
    let (enc, encoded) = event_value_page(column_id, descs, page_bytes, groups)?;
    Ok(decode_strings(enc, encoded, count)?)
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::writer::{ObjectIdentity, RspanConfig, RspanWriter};

    /// A resolved row with no attributes or events, for the fixed-column tests.
    fn row(trace: u8, span: u8, start: i64, end: i64) -> ResolvedSpanRow {
        ResolvedSpanRow {
            trace_id: [trace; 16],
            span_id: [span; 8],
            parent_span_id: None,
            name: format!("op-{span}"),
            start_ts_ns: start,
            end_ts_ns: end,
            status_code: StatusCode::Unset,
            status_message: None,
            service_name: None,
            columns: Vec::new(),
            attrs_raw: None,
            events: Vec::new(),
        }
    }

    #[test]
    fn fixed_columns_round_trip() {
        let mut rows = Vec::new();
        for i in 0..100i64 {
            let mut r = row(1, i as u8, 1000 + i, 1000 + i + 5);
            if i % 2 == 0 {
                r.parent_span_id = Some([9u8; 8]);
            }
            if i % 3 == 0 {
                r.status_code = StatusCode::Error;
                r.status_message = Some(format!("err {i}"));
            }
            rows.push(r);
        }
        let out = write_block(&rows, &[], 3).expect("write");
        assert_eq!(out.record_count, 100);
        let dec = read_block(&out.bytes, out.crc32c, DEFAULT_MAX_UNCOMP).expect("read");
        assert_eq!(dec.record_count(), 100);
        for (i, r) in rows.iter().enumerate() {
            let rec = dec.record(i).expect("record");
            assert_eq!(rec.trace_id, r.trace_id);
            assert_eq!(rec.span_id, r.span_id);
            assert_eq!(rec.parent_span_id, r.parent_span_id);
            assert_eq!(rec.name, r.name);
            assert_eq!(rec.status_code, r.status_code);
            assert_eq!(rec.status_message, r.status_message);
            assert!(rec.attrs.is_empty());
        }
    }

    #[test]
    fn crc_mismatch_is_corrupted() {
        let rows = vec![row(1, 0, 5, 6), row(1, 1, 6, 7)];
        let out = write_block(&rows, &[], 3).expect("write");
        let mut bad = out.bytes.clone();
        bad[0] ^= 0xff;
        assert!(matches!(
            read_block(&bad, out.crc32c, DEFAULT_MAX_UNCOMP),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    #[test]
    fn duration_and_status_mask_track_rows() {
        let mut ok = row(1, 0, 100, 150); // duration 50, Ok
        ok.status_code = StatusCode::Ok;
        let mut err = row(1, 1, 100, 105); // duration 5, Error
        err.status_code = StatusCode::Error;
        let unset = row(1, 2, 100, 1100); // duration 1000, Unset
        let out = write_block(&[ok, err, unset], &[], 3).expect("write");
        assert_eq!(out.min_duration_ns, 5);
        assert_eq!(out.max_duration_ns, 1000);
        assert_eq!(
            out.status_mask,
            STATUS_BIT_UNSET | STATUS_BIT_OK | STATUS_BIT_ERROR
        );
    }

    #[test]
    fn duration_overflow_is_corrupted() {
        let rows = vec![row(1, 0, i64::MIN, i64::MAX)];
        assert!(matches!(
            write_block(&rows, &[], 3),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    /// Direct block round trip of dynamic per-key columns, attrs_raw overflow,
    /// service_name, and nested events, exercised through the resolved-row form.
    #[test]
    fn dynamic_columns_events_and_overflow_round_trip_at_block_level() {
        let plans = vec![
            ColumnPlan {
                column_id: FIRST_DYNAMIC_COL,
                name: "http.method".into(),
            },
            ColumnPlan {
                column_id: FIRST_DYNAMIC_COL + 1,
                name: "http.route".into(),
            },
        ];
        let mut r = row(1, 0, 100, 200);
        r.service_name = Some("checkout".into());
        r.columns = vec![
            (FIRST_DYNAMIC_COL, "GET".into()),
            (FIRST_DYNAMIC_COL + 1, "/cart".into()),
        ];
        r.attrs_raw = Some(crate::record::encode_attrs(&[(
            "overflow.key".to_string(),
            "v".to_string(),
        )]));
        r.events = vec![
            SpanEvent {
                ts_ns: 150,
                name: "exception".into(),
                attrs_blob: vec![1, 2, 3, 4, 5],
            },
            SpanEvent {
                ts_ns: 175,
                name: "log".into(),
                attrs_blob: vec![],
            },
        ];
        // A second row with no dynamic columns and no events, to exercise mixed
        // presence and the flattened event layout across rows.
        let r2 = row(1, 1, 210, 220);
        let rows = vec![r.clone(), r2];
        let out = write_block(&rows, &plans, 3).expect("write");
        let dec = read_block(&out.bytes, out.crc32c, DEFAULT_MAX_UNCOMP).expect("read");

        let rec0 = dec.record(0).expect("record 0");
        let get = |k: &str| {
            rec0.attrs
                .iter()
                .find(|(kk, _)| kk == k)
                .map(|(_, v)| v.as_str())
        };
        assert_eq!(get("http.method"), Some("GET"));
        assert_eq!(get("http.route"), Some("/cart"));
        assert_eq!(get("overflow.key"), Some("v"));
        assert_eq!(get("service.name"), Some("checkout"));
        // Events reconstruct into the _events_raw value, byte-identical to what
        // reconstruct_events_raw produces from the same events.
        assert_eq!(
            get("_events_raw"),
            Some(reconstruct_events_raw(&r.events).as_str())
        );

        let rec1 = dec.record(1).expect("record 1");
        assert!(rec1.attrs.is_empty(), "second row carries no attributes");
    }

    /// The ADR-0045 T6 acceptance test: a span with more than 1000 distinct
    /// dynamic attribute keys (so some overflow into attrs_raw), plus at least
    /// one event with a non-trivial event_attrs_blob, round-tripped through the
    /// whole writer and reader, asserting value-for-value equality of the
    /// attribute map (including that the overflow keys survive) and of the
    /// events, and that the split between typed columns and attrs_raw is what
    /// the 1000-column budget dictates.
    #[test]
    fn v4_attribute_columns_roundtrip_with_overflow_and_events() {
        use crate::footer::{DEFAULT_MAX_SECTION_UNCOMP, kind, open, read_section};
        use crate::reader::{RspanReader, SpanQuery};
        use crate::skip_index::SkipIndex;

        // Build one span whose merged attrs carry 1500 distinct dynamic keys
        // (named so their sort order is stable), a service.name, and a valid
        // _events_raw blob with one event whose payload is non-trivial.
        let mut attrs: Vec<(String, String)> = Vec::new();
        for i in 0..1500 {
            attrs.push((format!("attr.{i:04}"), format!("val{i}")));
        }
        attrs.push(("service.name".to_string(), "checkout".to_string()));
        // A synthetic but validly-framed events blob: one OTLP-shaped event with
        // time_unix_nano (field 1, fixed64), name (field 2), and an attributes
        // field (field 3) that stands in for a non-trivial event_attrs_blob.
        let event_chunk = {
            let mut e = Vec::new();
            e.push(0x09); // field 1, wire type 1 (fixed64)
            e.extend_from_slice(&1_650_000_000_000i64.to_le_bytes());
            e.push(0x12); // field 2, wire type 2 (string)
            e.push(9);
            e.extend_from_slice(b"exception");
            e.push(0x1a); // field 3, wire type 2 (attributes stand-in)
            e.push(4);
            e.extend_from_slice(b"boom");
            e
        };
        let events_blob = {
            let mut raw = Vec::new();
            crate::varint::put_uvarint(&mut raw, event_chunk.len() as u64);
            raw.extend_from_slice(&event_chunk);
            // lowercase hex, matching the ingest encoding.
            let mut s = String::new();
            for b in &raw {
                s.push_str(&format!("{b:02x}"));
            }
            s
        };
        attrs.push(("_events_raw".to_string(), events_blob.clone()));

        let rec = SpanRecord {
            trace_id: [7u8; 16],
            span_id: [1u8; 8],
            parent_span_id: None,
            name: "GET /cart".into(),
            start_ts_ns: 1_000,
            end_ts_ns: 2_000,
            status_code: StatusCode::Ok,
            status_message: None,
            attrs: attrs.clone(),
        };

        let cfg = RspanConfig::default();
        let mut w = RspanWriter::new(
            cfg,
            ObjectIdentity {
                tenant_hash: [1u8; 16],
                shard: 0,
                writer_id: [2u8; 16],
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        w.push(rec.clone());
        let obj = w.finish().expect("finish");

        // Whole-object round trip: the reconstructed record equals the
        // canonicalized input (sorted, unique keys).
        let reader = RspanReader::new(&obj, &cfg).expect("reader");
        let (got, _stats) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("scan");
        assert_eq!(got.len(), 1);
        let mut want: BTreeMap<String, String> = BTreeMap::new();
        for (k, v) in &attrs {
            want.insert(k.clone(), v.clone());
        }
        let want: Vec<(String, String)> = want.into_iter().collect();
        assert_eq!(got[0].attrs, want, "attribute map must round-trip exactly");
        // The events blob survives byte-for-byte through the event columns.
        assert_eq!(
            got[0]
                .attrs
                .iter()
                .find(|(k, _)| k == "_events_raw")
                .map(|(_, v)| v.as_str()),
            Some(events_blob.as_str())
        );

        // Column-vs-overflow split: decode the single block directly and check
        // exactly 1000 keys became dynamic columns and the remaining 500 landed
        // in attrs_raw. service.name and _events_raw are lifted out separately
        // and never counted as dynamic columns.
        let footer = open(&obj).expect("open");
        assert_eq!(footer.block_count, 1);
        let skip_desc = footer.section(kind::SKIP_IDX).expect("skip");
        let skip_raw = read_section(&obj, skip_desc, DEFAULT_MAX_SECTION_UNCOMP).expect("skip raw");
        let skip = SkipIndex::decode(&skip_raw, 1 << 20).expect("skip decode");
        let entry = &skip.blocks[0];
        let blocks_desc = footer.section(kind::BLOCKS).expect("blocks");
        let start = blocks_desc.offset as usize;
        let block_bytes = &obj[start + entry.block_offset as usize
            ..start + entry.block_offset as usize + entry.block_len as usize];
        let dec = read_block(block_bytes, entry.block_crc32c, DEFAULT_MAX_UNCOMP).expect("read");
        assert_eq!(
            dec.dyn_names.len(),
            1000,
            "the 1000-column budget must be exactly filled"
        );
        // The 1000 columns are the lexicographically-first 1000 keys
        // (attr.0000..attr.0999); attr.1000.. overflowed into attrs_raw.
        let names: std::collections::BTreeSet<&str> =
            dec.dyn_names.values().map(String::as_str).collect();
        assert!(names.contains("attr.0000"));
        assert!(names.contains("attr.0999"));
        assert!(!names.contains("attr.1000"), "attr.1000 must overflow");
        assert!(
            !names.contains("service.name"),
            "service.name is lifted, not dynamic"
        );
        assert!(
            !names.contains("_events_raw"),
            "_events_raw is lifted, not dynamic"
        );
        let raw = dec
            .str_at(COL_ATTRS_RAW, 0)
            .expect("attrs_raw present for the overflow row");
        let overflow = decode_attrs(&raw).expect("decode overflow");
        assert_eq!(overflow.len(), 500, "500 keys overflow into attrs_raw");
        assert!(overflow.iter().any(|(k, _)| k == "attr.1000"));
    }

    #[test]
    fn single_row_events_round_trip() {
        let mut r = row(1, 0, 0, 1);
        r.events = vec![SpanEvent {
            ts_ns: 5,
            name: "x".into(),
            attrs_blob: vec![9, 9, 9],
        }];
        let out = write_block(std::slice::from_ref(&r), &[], 3).expect("write");
        let dec = read_block(&out.bytes, out.crc32c, DEFAULT_MAX_UNCOMP).expect("read");
        assert_eq!(dec.events[0], r.events);
    }

    /// A block carrying attribute and event pages built once, for the projected
    /// decode tests. Two spans: dynamic per-key columns, an attrs_raw overflow,
    /// nested events, service_name and (partly) status_message present.
    fn attr_and_event_block() -> (Vec<u8>, u32) {
        let plans = vec![
            ColumnPlan {
                column_id: FIRST_DYNAMIC_COL,
                name: "http.method".into(),
            },
            ColumnPlan {
                column_id: FIRST_DYNAMIC_COL + 1,
                name: "http.route".into(),
            },
        ];
        let mut r0 = row(1, 0, 100, 200);
        r0.service_name = Some("checkout".into());
        r0.status_message = Some("ok".into());
        r0.columns = vec![
            (FIRST_DYNAMIC_COL, "GET".into()),
            (FIRST_DYNAMIC_COL + 1, "/cart".into()),
        ];
        r0.attrs_raw = Some(crate::record::encode_attrs(&[(
            "overflow.key".to_string(),
            "v".to_string(),
        )]));
        r0.events = vec![SpanEvent {
            ts_ns: 150,
            name: "exception".into(),
            attrs_blob: vec![1, 2, 3],
        }];
        let mut r1 = row(1, 1, 210, 220);
        r1.service_name = Some("payments".into());
        r1.columns = vec![(FIRST_DYNAMIC_COL, "POST".into())];
        r1.events = vec![SpanEvent {
            ts_ns: 215,
            name: "log".into(),
            attrs_blob: vec![],
        }];
        let out = write_block(&[r0, r1], &plans, 3).expect("write");
        (out.bytes, out.crc32c)
    }

    /// Projecting a set of columns decodes those pages and no others, and the
    /// values it yields match a full decode row for row.
    #[test]
    fn projected_decode_skips_unrequested_pages_and_matches_full_decode() {
        // The fixture MUST carry attribute and event pages. Over spans with no
        // attributes and no events there are no dynamic-column, attrs_raw, or
        // COL_EVENT_* pages to skip: both decodes would touch the same pages and
        // the "strictly lower" assertion below would be false for a *correct*
        // implementation. If that assertion ever fails, fix the fixture, never
        // weaken the assertion -- weakening it makes the test vacuous.
        let (bytes, crc) = attr_and_event_block();
        let full = read_block(&bytes, crc, DEFAULT_MAX_UNCOMP).expect("full read");

        // The projected span-table columns: the fixed identity/timing/status
        // columns and service_name, but no dynamic attribute column, no
        // attrs_raw overflow, and no event column.
        let requested = [
            COL_TRACE_ID,
            COL_SPAN_ID,
            COL_PARENT_SPAN_ID,
            COL_NAME,
            COL_START_TS,
            COL_END_TS,
            COL_STATUS_CODE,
            COL_STATUS_MESSAGE,
            COL_SERVICE_NAME,
        ];
        let projected =
            read_block_projected(&bytes, crc, DEFAULT_MAX_UNCOMP, &requested).expect("projected");

        assert_eq!(projected.record_count(), full.record_count());
        let n = full.record_count();
        let fv = full.view();
        let pv = projected.view();

        // Equivalence: every requested column reads value-for-value equal to the
        // full decode, row for row, including null placement (COL_PARENT_SPAN_ID
        // is absent from the block, so it is None on both).
        for &col in &[COL_START_TS, COL_END_TS, COL_STATUS_CODE] {
            let fc = fv.i64_column(col).expect("full i64");
            let pc = pv.i64_column(col).expect("proj i64");
            for r in 0..n {
                assert_eq!(pc.value_at(r), fc.value_at(r), "i64 col {col} row {r}");
            }
        }
        for &col in &[COL_TRACE_ID, COL_SPAN_ID, COL_PARENT_SPAN_ID] {
            let fc = fv.fixed_column(col).expect("full fixed");
            let pc = pv.fixed_column(col).expect("proj fixed");
            for r in 0..n {
                assert_eq!(pc.value_at(r), fc.value_at(r), "fixed col {col} row {r}");
            }
        }
        for &col in &[COL_NAME, COL_STATUS_MESSAGE, COL_SERVICE_NAME] {
            let fc = fv.str_column(col).expect("full str");
            let pc = pv.str_column(col).expect("proj str");
            for r in 0..n {
                assert_eq!(pc.value_at(r), fc.value_at(r), "str col {col} row {r}");
            }
        }

        // The gather iterator yields one cell per surviving row index, in order.
        let survivors = [1usize];
        let start = pv.i64_column(COL_START_TS).expect("start col");
        assert_eq!(
            start.gather(&survivors).collect::<Vec<_>>(),
            vec![Some(210)]
        );

        // The win, asserted as a magnitude not a boolean: the projected decode
        // skips the two dynamic-column pages, the attrs_raw pages, and the four
        // event pages, so it decodes strictly fewer pages than the full decode.
        assert!(
            projected.pages_decoded() < full.pages_decoded(),
            "projected decoded {} pages, full decoded {}",
            projected.pages_decoded(),
            full.pages_decoded()
        );
        assert!(projected.pages_skipped() > 0);
        assert_eq!(full.pages_skipped(), 0);
    }

    /// Reading a column the projection excluded is a typed error, while a column
    /// that was requested but is genuinely absent from the block reads as nulls.
    /// If both returned the same thing, the requested-vs-absent distinction
    /// (docs/adrs/0110 decision 1) would not exist.
    #[test]
    fn unrequested_column_access_is_typed_error_not_null() {
        // Two spans with service_name present (so COL_SERVICE_NAME has a page)
        // and status_message absent on every row (so COL_STATUS_MESSAGE has no
        // page at all).
        let mut r0 = row(1, 0, 10, 20);
        r0.service_name = Some("checkout".into());
        let mut r1 = row(1, 1, 30, 40);
        r1.service_name = Some("payments".into());
        let out = write_block(&[r0, r1], &[], 3).expect("write");

        // Request status_message (absent from the block) but NOT service_name
        // (present in the block).
        let requested = [
            COL_TRACE_ID,
            COL_SPAN_ID,
            COL_NAME,
            COL_START_TS,
            COL_END_TS,
            COL_STATUS_CODE,
            COL_STATUS_MESSAGE,
        ];
        let dec = read_block_projected(&out.bytes, out.crc32c, DEFAULT_MAX_UNCOMP, &requested)
            .expect("projected");
        let view = dec.view();

        // Case (b), caller bug: a column the projection excluded is a typed
        // error, never a silent column of nulls.
        match view.str_column(COL_SERVICE_NAME) {
            Err(SpanSegError::ColumnNotRequested(col)) => assert_eq!(col, COL_SERVICE_NAME),
            Err(other) => panic!("expected ColumnNotRequested, got {other:?}"),
            Ok(_) => panic!("expected ColumnNotRequested, got a column"),
        }

        // Case (a), legitimate null: a requested column genuinely absent from
        // the block reads None for every row, not an error.
        let msg = view
            .str_column(COL_STATUS_MESSAGE)
            .expect("status_message was requested");
        for r in 0..dec.record_count() {
            assert_eq!(msg.value_at(r), None, "row {r}");
        }
    }

    /// Truncating a v4 block that carries dynamic columns and events -- at every
    /// prefix length -- must always be a typed `Corrupted` error (recomputing
    /// the crc so the truncation itself, not a stale checksum, is what the
    /// parser must catch), never a panic.
    #[test]
    fn truncated_block_is_corrupted_not_panic() {
        let plans = vec![ColumnPlan {
            column_id: FIRST_DYNAMIC_COL,
            name: "http.method".into(),
        }];
        let mut r = row(1, 0, 0, 1);
        r.columns = vec![(FIRST_DYNAMIC_COL, "GET".into())];
        r.events = vec![SpanEvent {
            ts_ns: 7,
            name: "exception".into(),
            attrs_blob: vec![1, 2, 3],
        }];
        let out = write_block(std::slice::from_ref(&r), &plans, 3).expect("write");
        for cut in 0..out.bytes.len() {
            let prefix = &out.bytes[..cut];
            let crc = crc32c::crc32c(prefix);
            // Either a parse error or a (recomputed) crc that still mismatches
            // the truncation: both are Corrupted, and neither panics.
            match read_block(prefix, crc, DEFAULT_MAX_UNCOMP) {
                Err(SpanSegError::Corrupted(_)) => {}
                Ok(_) => panic!("truncation at {cut} decoded as a valid block"),
                Err(other) => panic!("unexpected error at {cut}: {other:?}"),
            }
        }
    }

    /// The dynamic-column directory is untrusted: a non-ascending sequence or an
    /// id below the dynamic range is a typed `Corrupted` error. Hand-built
    /// (page_count 0) so the directory parse is what fails, with a matching crc
    /// so the guard, not a checksum, is exercised.
    #[test]
    fn corrupt_dyn_directory_is_rejected() {
        let make = |entries: &[(u32, &str)]| -> Vec<u8> {
            let mut b = Vec::new();
            put_uvarint(&mut b, 1); // record_count
            put_uvarint(&mut b, 0); // page_count
            put_uvarint(&mut b, entries.len() as u64);
            for (cid, name) in entries {
                put_uvarint(&mut b, u64::from(*cid));
                put_uvarint(&mut b, name.len() as u64);
                b.extend_from_slice(name.as_bytes());
            }
            b
        };
        // Non-ascending column ids.
        let bad = make(&[(FIRST_DYNAMIC_COL + 1, "b"), (FIRST_DYNAMIC_COL, "a")]);
        let crc = crc32c::crc32c(&bad);
        assert!(matches!(
            read_block(&bad, crc, DEFAULT_MAX_UNCOMP),
            Err(SpanSegError::Corrupted(_))
        ));
        // Id below the dynamic range.
        let bad = make(&[(5, "a")]);
        let crc = crc32c::crc32c(&bad);
        assert!(matches!(
            read_block(&bad, crc, DEFAULT_MAX_UNCOMP),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    proptest::proptest! {
        /// read_block never panics on arbitrary bytes. The crc is set to match
        /// the input so the parser is exercised past the checksum gate on every
        /// case, walking the header, the dynamic directory, the pages, and the
        /// event columns over hostile input.
        #[test]
        fn read_block_never_panics(bytes in proptest::collection::vec(proptest::prelude::any::<u8>(), 0..512)) {
            let crc = crc32c::crc32c(&bytes);
            let _ = read_block(&bytes, crc, DEFAULT_MAX_UNCOMP);
        }
    }
}
