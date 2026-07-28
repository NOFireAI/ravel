//! Object writer: records in, a whole RLOG object out
//! (docs/log-segment-format.md).
//!
//! [`RlogWriter::finish`] runs the full pipeline: collect and sort the stream
//! directory, assign dense stream refs, sort records by `(stream_ref, ts)`,
//! split dynamic attributes into per-type columns under the 1000-column budget
//! (overflow keys fold into `attrs_raw`), chunk into blocks, build per-block
//! token blooms and the skip index, compress the whole-read sections, and emit
//! the footer and trailer. Identical input yields byte-identical output.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use ravel_types::logstream::{LogStreamId, canonical_attr_bytes};

use crate::block::{ColumnPlan, write_block};
use crate::bloom::BloomBuilder;
use crate::bloom_section::encode_bloom_section;
use crate::error::LogSegError;
use crate::field_dir::{FieldDir, FieldEntry};
use crate::footer::{COMP_NONE, COMP_ZSTD, LogFooter, SectionDesc, kind, write_footer_and_trailer};
use crate::record::{
    COL_BODY, COL_SEVERITY_TEXT, ColumnValue, FIRST_DYNAMIC_COL, FieldType, LogRecord, ResolvedRow,
    resolve_value,
};
use crate::skip_index::{Level0Entry, SkipIndex};
use crate::stream_dir::{StreamDir, StreamEntry};
use crate::tokenizer::tokens;

/// Writer configuration and format constants (docs/log-segment-format.md).
#[derive(Clone, Copy, Debug)]
pub struct RlogConfig {
    pub block_target_records: usize,
    pub block_max_bytes: usize,
    pub max_dynamic_columns: usize,
    pub zstd_level: i32,
    pub bloom_seed: u64,
    pub max_uncomp_section: u64,
}

impl Default for RlogConfig {
    fn default() -> Self {
        RlogConfig {
            block_target_records: 8192,
            block_max_bytes: 8 << 20,
            max_dynamic_columns: 1000,
            zstd_level: 3,
            bloom_seed: 0,
            max_uncomp_section: 1 << 30,
        }
    }
}

/// The object identity written into the footer (ADR-0010 §7).
#[derive(Clone, Copy, Debug)]
pub struct ObjectIdentity {
    pub tenant_hash: [u8; 16],
    pub shard: u32,
    pub writer_id: [u8; 16],
    pub writer_epoch: u64,
    pub writer_seq: u64,
}

/// Buffers records and emits one RLOG object.
pub struct RlogWriter {
    cfg: RlogConfig,
    identity: ObjectIdentity,
    records: Vec<LogRecord>,
}

/// The maximum byte length of a string value inserted into the bloom by exact
/// value (docs/log-segment-format.md "BLOOM"): longer values are tokenized only.
const EXACT_BLOOM_MAX: usize = 64;

impl RlogWriter {
    pub fn new(cfg: RlogConfig, identity: ObjectIdentity) -> Self {
        RlogWriter {
            cfg,
            identity,
            records: Vec::new(),
        }
    }

    /// Buffers one record.
    pub fn push(&mut self, rec: LogRecord) -> Result<(), LogSegError> {
        self.records.push(rec);
        Ok(())
    }

    /// Produces the whole object. Empty input is rejected: the flush layer never
    /// writes empty objects (matches RSEG's zero-sample rule).
    pub fn finish(self) -> Result<Vec<u8>, LogSegError> {
        if self.records.is_empty() {
            return Err(LogSegError::LimitExceeded("empty object".into()));
        }

        // Stream directory: distinct ids, sorted; the dense ref is the ordinal.
        let mut ids: BTreeSet<LogStreamId> = BTreeSet::new();
        for r in &self.records {
            ids.insert(r.stream_id);
        }
        let sorted_ids: Vec<LogStreamId> = ids.into_iter().collect();
        let mut ref_of: HashMap<LogStreamId, u32> = HashMap::with_capacity(sorted_ids.len());
        for (i, id) in sorted_ids.iter().enumerate() {
            ref_of.insert(*id, i as u32);
        }

        // Dynamic column assignment: distinct (name, type) sorted by
        // (name bytes, type), the first `max_dynamic_columns` get columns.
        let mut distinct: BTreeSet<(String, u8)> = BTreeSet::new();
        for r in &self.records {
            for (k, v) in &r.attrs {
                let (ty, _) = resolve_value(v);
                distinct.insert((k.clone(), ty.to_u8()));
            }
        }
        let mut column_of: HashMap<(String, u8), u32> = HashMap::new();
        let mut columns: Vec<(String, FieldType, u32)> = Vec::new();
        for (idx, (name, ty_byte)) in distinct.into_iter().enumerate() {
            if idx >= self.cfg.max_dynamic_columns {
                break;
            }
            let column_id = FIRST_DYNAMIC_COL + idx as u32;
            let ty = FieldType::from_u8(ty_byte).unwrap_or(FieldType::Bytes);
            column_of.insert((name.clone(), ty_byte), column_id);
            columns.push((name, ty, column_id));
        }
        let ty_of_column: HashMap<u32, FieldType> =
            columns.iter().map(|(_, ty, id)| (*id, *ty)).collect();

        // Resolve every record into storage form, then sort by (stream_ref, ts).
        let mut rows: Vec<ResolvedRow> = self
            .records
            .iter()
            .map(|r| resolve_row(r, &ref_of, &column_of))
            .collect();
        rows.sort_by(|a, b| {
            a.stream_ref
                .cmp(&b.stream_ref)
                .then_with(|| a.ts_ns.cmp(&b.ts_ns))
        });

        // Chunk into blocks by record target and an estimated byte cap.
        let block_spans = chunk_blocks(&rows, &self.cfg);

        // Per-column present/blocks accounting for FIELD_DIR.
        let mut col_present: HashMap<u32, u64> = HashMap::new();
        let mut col_blocks: HashMap<u32, u32> = HashMap::new();
        // Per-stream block range.
        let mut first_blk: HashMap<u32, u32> = HashMap::new();
        let mut last_blk: HashMap<u32, u32> = HashMap::new();

        let mut blocks_bytes: Vec<u8> = Vec::new();
        let mut l0: Vec<Level0Entry> = Vec::new();
        let mut bloom_entries: Vec<Vec<u8>> = Vec::new();

        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        let mut min_obs = i64::MAX;
        let mut max_obs = i64::MIN;

        for (blk_idx, span) in block_spans.iter().enumerate() {
            let block_rows = &rows[span.clone()];
            let blk_idx_u32 = blk_idx as u32;

            // Columns present in this block, ascending by id.
            let mut present_cols: BTreeSet<u32> = BTreeSet::new();
            for row in block_rows {
                for (cid, _) in &row.columns {
                    present_cols.insert(*cid);
                }
            }
            let plans: Vec<ColumnPlan> = present_cols
                .iter()
                .map(|cid| ColumnPlan {
                    column_id: *cid,
                    ty: ty_of_column[cid],
                })
                .collect();

            let out = write_block(block_rows, &plans, self.cfg.zstd_level)?;

            // Bloom over body, severity_text, and string columns.
            let mut builder = BloomBuilder::new(self.cfg.bloom_seed);
            for row in block_rows {
                insert_text(&mut builder, COL_BODY, row.body.as_bytes());
                insert_text(
                    &mut builder,
                    COL_SEVERITY_TEXT,
                    row.severity_text.as_bytes(),
                );
                for (cid, v) in &row.columns {
                    if let ColumnValue::Str(bytes) = v {
                        insert_text(&mut builder, *cid, bytes);
                    }
                }
            }
            bloom_entries.push(builder.finish());

            // Accounting.
            for row in block_rows {
                min_ts = min_ts.min(row.ts_ns);
                max_ts = max_ts.max(row.ts_ns);
                min_obs = min_obs.min(row.observed_ts_ns);
                max_obs = max_obs.max(row.observed_ts_ns);
                first_blk.entry(row.stream_ref).or_insert(blk_idx_u32);
                last_blk.insert(row.stream_ref, blk_idx_u32);
            }
            for cid in &present_cols {
                *col_blocks.entry(*cid).or_insert(0) += 1;
            }
            for row in block_rows {
                for (cid, _) in &row.columns {
                    *col_present.entry(*cid).or_insert(0) += 1;
                }
            }

            let block_offset = blocks_bytes.len() as u64;
            blocks_bytes.extend_from_slice(&out.bytes);
            l0.push(Level0Entry {
                block_offset,
                block_len: out.bytes.len() as u64,
                block_crc32c: out.crc32c,
                record_count: out.record_count,
                min_ts: out.min_ts,
                max_ts: out.max_ts,
                min_stream_ref: out.min_stream_ref,
                max_stream_ref: out.max_stream_ref,
                stats: out.stats,
            });
        }

        // STREAM_DIR.
        let total_blocks = block_spans.len() as u32;
        let stream_entries: Vec<StreamEntry> = sorted_ids
            .iter()
            .enumerate()
            .map(|(i, id)| {
                let r = i as u32;
                StreamEntry {
                    stream_id: *id,
                    blob: Vec::new(),
                    first_blk: first_blk.get(&r).copied().unwrap_or(0),
                    last_blk: last_blk
                        .get(&r)
                        .copied()
                        .unwrap_or(total_blocks.saturating_sub(1)),
                }
            })
            .collect();
        let stream_dir = StreamDir::new(stream_entries);

        // FIELD_DIR: one entry per in-budget column, sorted by (name, type).
        let field_entries: Vec<FieldEntry> = columns
            .iter()
            .map(|(name, ty, cid)| {
                let present = col_present.get(cid).copied().unwrap_or(0);
                let null_count = (rows.len() as u64).saturating_sub(present);
                FieldEntry {
                    name: name.clone(),
                    ty: *ty,
                    column_id: *cid,
                    present_blocks: col_blocks.get(cid).copied().unwrap_or(0),
                    null_count,
                }
            })
            .collect();
        let field_dir = FieldDir::new(field_entries);

        let skip = SkipIndex::build(l0);

        // Assemble sections in kind order.
        let mut object = Vec::new();
        let mut sections: Vec<SectionDesc> = Vec::new();

        push_section(
            &mut object,
            &mut sections,
            kind::STREAM_DIR,
            &compress(&stream_dir.encode(), self.cfg.zstd_level)?,
        );
        push_section(
            &mut object,
            &mut sections,
            kind::FIELD_DIR,
            &compress(&field_dir.encode(), self.cfg.zstd_level)?,
        );
        push_section(
            &mut object,
            &mut sections,
            kind::BLOCKS,
            &Stored::raw(blocks_bytes),
        );
        push_section(
            &mut object,
            &mut sections,
            kind::SKIP_IDX,
            &compress(&skip.encode(), self.cfg.zstd_level)?,
        );
        push_section(
            &mut object,
            &mut sections,
            kind::BLOOM,
            &Stored::raw(encode_bloom_section(&bloom_entries)),
        );

        let footer = LogFooter {
            tenant_hash: self.identity.tenant_hash,
            shard: self.identity.shard,
            writer_id: self.identity.writer_id,
            writer_epoch: self.identity.writer_epoch,
            writer_seq: self.identity.writer_seq,
            min_ts_ns: min_ts,
            max_ts_ns: max_ts,
            min_observed_ts_ns: min_obs,
            max_observed_ts_ns: max_obs,
            record_count: rows.len() as u64,
            block_count: u64::from(total_blocks),
            stream_count: sorted_ids.len() as u64,
            sections,
        };
        write_footer_and_trailer(&mut object, &footer);
        Ok(object)
    }
}

/// Resolves one record into storage form: dense stream ref, dynamic columns
/// split by type, and overflow attributes canonicalized into `attrs_raw`.
fn resolve_row(
    r: &LogRecord,
    ref_of: &HashMap<LogStreamId, u32>,
    column_of: &HashMap<(String, u8), u32>,
) -> ResolvedRow {
    let stream_ref = ref_of.get(&r.stream_id).copied().unwrap_or(0);
    let mut cols: BTreeMap<u32, ColumnValue> = BTreeMap::new();
    let mut overflow: Vec<(String, ravel_types::logstream::AttrValue)> = Vec::new();
    for (k, v) in &r.attrs {
        let (ty, cv) = resolve_value(v);
        match column_of.get(&(k.clone(), ty.to_u8())) {
            Some(cid) if !cols.contains_key(cid) => {
                cols.insert(*cid, cv);
            }
            // Overflow column, or a duplicate (name,type) already columnar this
            // row: fold into attrs_raw so no value is lost.
            _ => overflow.push((k.clone(), v.clone())),
        }
    }
    let attrs_raw = if overflow.is_empty() {
        None
    } else {
        Some(canonical_attr_bytes(&overflow))
    };
    ResolvedRow {
        stream_ref,
        ts_ns: r.ts_ns,
        observed_ts_ns: r.observed_ts_ns,
        severity_num: r.severity_num,
        severity_text: r.severity_text.clone(),
        body: r.body.clone(),
        trace_id: r.trace_id,
        span_id: r.span_id,
        flags: r.flags,
        attrs_raw,
        columns: cols.into_iter().collect(),
    }
}

/// Splits row indices into block spans by record target and an estimated
/// uncompressed byte cap.
fn chunk_blocks(rows: &[ResolvedRow], cfg: &RlogConfig) -> Vec<std::ops::Range<usize>> {
    let mut spans = Vec::new();
    let mut start = 0usize;
    let mut bytes = 0usize;
    for (i, row) in rows.iter().enumerate() {
        let est = row_estimate(row);
        let count = i - start;
        if count > 0 && (count >= cfg.block_target_records || bytes + est > cfg.block_max_bytes) {
            spans.push(start..i);
            start = i;
            bytes = 0;
        }
        bytes += est;
    }
    if start < rows.len() {
        spans.push(start..rows.len());
    }
    spans
}

/// A rough uncompressed byte estimate for one row, for the block byte cap.
fn row_estimate(row: &ResolvedRow) -> usize {
    let mut est = 40; // fixed-field overhead
    est += row.body.len();
    est += row.severity_text.len();
    if let Some(raw) = &row.attrs_raw {
        est += raw.len();
    }
    for (_, v) in &row.columns {
        est += match v {
            ColumnValue::Str(b) | ColumnValue::Bytes(b) => b.len() + 2,
            _ => 8,
        };
    }
    est
}

/// Inserts a text value's word tokens (and its exact bytes when short) into the
/// bloom, all scoped to `column_id`.
fn insert_text(builder: &mut BloomBuilder, column_id: u32, bytes: &[u8]) {
    if let Ok(s) = std::str::from_utf8(bytes) {
        for tok in tokens(s) {
            builder.insert(column_id, &tok);
        }
    }
    if bytes.len() <= EXACT_BLOOM_MAX {
        builder.insert(column_id, bytes);
    }
}

/// A section's stored bytes plus its compression descriptor fields.
struct Stored {
    bytes: Vec<u8>,
    comp: u8,
    uncomp_len: u64,
}

impl Stored {
    fn raw(bytes: Vec<u8>) -> Self {
        let uncomp_len = bytes.len() as u64;
        Stored {
            bytes,
            comp: COMP_NONE,
            uncomp_len,
        }
    }
}

/// Whole-section zstd compression (STREAM_DIR, FIELD_DIR, SKIP_IDX).
fn compress(raw: &[u8], level: i32) -> Result<Stored, LogSegError> {
    let compressed = zstd::bulk::compress(raw, level)
        .map_err(|e| LogSegError::Corrupted(format!("zstd compress: {e}")))?;
    Ok(Stored {
        bytes: compressed,
        comp: COMP_ZSTD,
        uncomp_len: raw.len() as u64,
    })
}

/// Appends a section's stored bytes and records its descriptor.
fn push_section(object: &mut Vec<u8>, sections: &mut Vec<SectionDesc>, kind: u32, stored: &Stored) {
    let offset = object.len() as u64;
    object.extend_from_slice(&stored.bytes);
    sections.push(SectionDesc {
        kind,
        offset,
        len: stored.bytes.len() as u64,
        crc32c: crc32c::crc32c(&stored.bytes),
        comp: stored.comp,
        uncomp_len: stored.uncomp_len,
    });
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::footer::open;
    use ravel_types::logstream::AttrValue;

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [1u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    fn id(n: u8) -> LogStreamId {
        let mut a = [0u8; 16];
        a[0] = n;
        LogStreamId(a)
    }

    fn base_record(stream: u8, ts: i64) -> LogRecord {
        LogRecord {
            stream_id: id(stream),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "hello world".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        }
    }

    #[test]
    fn empty_input_rejected() {
        let w = RlogWriter::new(RlogConfig::default(), identity());
        assert!(matches!(w.finish(), Err(LogSegError::LimitExceeded(_))));
    }

    #[test]
    fn end_to_end_and_deterministic() {
        let cfg = RlogConfig {
            block_target_records: 1000,
            ..RlogConfig::default()
        };
        let build = || {
            let mut w = RlogWriter::new(cfg, identity());
            for i in 0..10_000i64 {
                let stream = (i % 5) as u8;
                let mut r = base_record(stream, i);
                // 30 attr keys spread across records.
                let k = format!("k{}", i % 30);
                r.attrs.push((k, AttrValue::I64(i)));
                r.attrs
                    .push(("svc".into(), AttrValue::Str(format!("s{stream}"))));
                w.push(r).expect("push");
            }
            w.finish().expect("finish")
        };
        let a = build();
        let b = build();
        assert_eq!(a, b, "identical input must be byte-identical");

        let footer = open(&a).expect("open");
        assert_eq!(footer.stream_count, 5);
        assert_eq!(footer.record_count, 10_000);
        assert_eq!(footer.block_count, 10);
    }

    #[test]
    fn field_dir_splits_types() {
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        for i in 0..10i64 {
            let mut r = base_record(0, i);
            // Same key pushed as both Str and I64 -> two columns.
            if i % 2 == 0 {
                r.attrs.push(("x".into(), AttrValue::Str("a".into())));
            } else {
                r.attrs.push(("x".into(), AttrValue::I64(i)));
            }
            w.push(r).expect("push");
        }
        let obj = w.finish().expect("finish");
        let footer = open(&obj).expect("open");
        let fd_desc = footer.section(kind::FIELD_DIR).expect("fd");
        let start = fd_desc.offset as usize;
        let end = start + fd_desc.len as usize;
        let raw = zstd::bulk::decompress(&obj[start..end], fd_desc.uncomp_len as usize)
            .expect("decompress");
        let fd = FieldDir::decode(&raw, 10_000).expect("decode");
        assert_eq!(fd.len(), 2);
        assert!(fd.column("x", FieldType::Str).is_some());
        assert!(fd.column("x", FieldType::I64).is_some());
    }

    #[test]
    fn overflow_folds_into_attrs_raw() {
        // 1500 distinct keys -> only 1000 dynamic columns; the rest overflow.
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        let mut r = base_record(0, 0);
        for i in 0..1500 {
            r.attrs.push((format!("key{i:04}"), AttrValue::I64(i)));
        }
        w.push(r).expect("push");
        let obj = w.finish().expect("finish");
        let footer = open(&obj).expect("open");
        let fd_desc = footer.section(kind::FIELD_DIR).expect("fd");
        let start = fd_desc.offset as usize;
        let end = start + fd_desc.len as usize;
        let raw = zstd::bulk::decompress(&obj[start..end], fd_desc.uncomp_len as usize)
            .expect("decompress");
        let fd = FieldDir::decode(&raw, 10_000).expect("decode");
        assert_eq!(fd.len(), 1000);
    }
}
