//! Object writer: span records in, a whole RSPAN object out
//! (docs/span-segment-format.md).
//!
//! [`RspanWriter::finish`] runs the full pipeline: sort records by
//! `(trace_id, start_ts)`, resolve each into storage form (lift `service.name`,
//! split the remaining attributes into per-key string columns under the
//! 1000-column budget with overflow folded into `attrs_raw`, and decode the
//! `_events_raw` blob into nested event columns), chunk into blocks, build each
//! block and its interval-aware SKIP_IDX entry plus its token bloom, then emit
//! the BLOCKS, SKIP_IDX, and BLOOM sections, the footer, and the trailer.
//! Identical input yields byte-identical output.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use ravel_codec::bloom::BloomBuilder;
use ravel_codec::bloom_section::encode_bloom_section;
use ravel_codec::tokenizer::tokens;

use crate::block::{BlockWriteOut, ColumnPlan, write_block};
use crate::error::SpanSegError;
use crate::footer::{
    COMP_NONE, COMP_ZSTD, SectionDesc, SpanFooter, kind, write_footer_and_trailer,
};
use crate::record::{
    COL_NAME, COL_SERVICE_NAME, EVENTS_RAW_KEY, FIRST_DYNAMIC_COL, ResolvedSpanRow,
    SERVICE_NAME_KEY, SpanEvent, SpanRecord, encode_attrs, parse_events,
};
use crate::skip_index::{BlockEntry, SkipIndex};

/// Seed for every RSPAN block bloom (ADR-0054). Fixed, not configurable: the
/// seed is stored inside each serialized bloom entry, so the reader recovers it
/// from the bytes and a query never needs to know it. A constant keeps writer
/// output byte-deterministic for identical input.
const BLOOM_SEED: u64 = 0;

/// The most dynamic per-key attribute columns one object may carry (v4, ports
/// RLOG's cap). Object-wide: the distinct attribute keys are sorted and the
/// first this-many get columns; keys past it fold into `attrs_raw` per row. A
/// single record with more than this many distinct keys therefore overflows.
const MAX_DYNAMIC_COLUMNS: usize = 1000;

/// Writer configuration and format constants (docs/span-segment-format.md).
#[derive(Clone, Copy, Debug)]
pub struct RspanConfig {
    pub block_target_records: usize,
    pub block_max_bytes: usize,
    pub zstd_level: i32,
    pub max_uncomp_section: u64,
}

impl Default for RspanConfig {
    fn default() -> Self {
        RspanConfig {
            block_target_records: 8192,
            block_max_bytes: 8 << 20,
            zstd_level: 3,
            max_uncomp_section: 1 << 30,
        }
    }
}

/// The object identity written into the footer.
#[derive(Clone, Copy, Debug)]
pub struct ObjectIdentity {
    pub tenant_hash: [u8; 16],
    pub shard: u32,
    pub writer_id: [u8; 16],
    pub writer_epoch: u64,
    pub writer_seq: u64,
}

/// Buffers span records and emits one RSPAN object.
pub struct RspanWriter {
    cfg: RspanConfig,
    identity: ObjectIdentity,
    records: Vec<SpanRecord>,
}

impl RspanWriter {
    pub fn new(cfg: RspanConfig, identity: ObjectIdentity) -> Self {
        RspanWriter {
            cfg,
            identity,
            records: Vec::new(),
        }
    }

    /// Buffers one span record.
    pub fn push(&mut self, rec: SpanRecord) {
        self.records.push(rec);
    }

    /// Produces the whole object as an L0 flush object: the compaction-identity
    /// fields are stamped at their L0 sentinels (`level = 0`, empty
    /// `input_set_hash`, `part_index = 0`). Empty input is rejected: the flush
    /// layer never writes empty objects.
    pub fn finish(self) -> Result<Vec<u8>, SpanSegError> {
        self.build_object(0, Vec::new(), 0)
    }

    /// Produces the whole object as an L1 compacted part, stamping the caller's
    /// compaction identity into the footer instead of the L0 sentinels. Every
    /// other byte is produced by the same pipeline as [`RspanWriter::finish`], so
    /// identical records plus identical identity yield byte-identical output
    /// regardless of entry point.
    pub fn finish_compacted(
        self,
        level: u32,
        input_set_hash: Vec<u8>,
        part_index: u32,
    ) -> Result<Vec<u8>, SpanSegError> {
        self.build_object(level, input_set_hash, part_index)
    }

    fn build_object(
        mut self,
        level: u32,
        input_set_hash: Vec<u8>,
        part_index: u32,
    ) -> Result<Vec<u8>, SpanSegError> {
        if self.records.is_empty() {
            return Err(SpanSegError::LimitExceeded("empty object".into()));
        }

        // Sort by (trace_id, start_ts). A stable sort keeps input order for
        // records that tie on both keys, so output stays deterministic.
        self.records.sort_by(|a, b| {
            a.trace_id
                .cmp(&b.trace_id)
                .then_with(|| a.start_ts_ns.cmp(&b.start_ts_ns))
        });

        let min_trace_id = self.records[0].trace_id;
        let max_trace_id = self.records[self.records.len() - 1].trace_id;
        let record_count = self.records.len() as u64;

        // Pass 1: canonicalize each record's attrs and split off service.name,
        // events, and the remaining ("other") attribute pairs.
        let pre: Vec<PreResolved> = self.records.iter().map(pre_resolve).collect();

        // Object-wide dynamic column assignment: distinct "other" attribute
        // keys, sorted, the first MAX_DYNAMIC_COLUMNS get columns.
        let mut distinct: BTreeSet<&str> = BTreeSet::new();
        for p in &pre {
            for (k, _) in &p.other {
                distinct.insert(k.as_str());
            }
        }
        let mut column_of: HashMap<String, u32> = HashMap::new();
        let mut plans: Vec<ColumnPlan> = Vec::new();
        for (idx, name) in distinct.into_iter().enumerate() {
            if idx >= MAX_DYNAMIC_COLUMNS {
                break;
            }
            let column_id = FIRST_DYNAMIC_COL + idx as u32;
            column_of.insert(name.to_string(), column_id);
            plans.push(ColumnPlan {
                column_id,
                name: name.to_string(),
            });
        }

        // Pass 2: resolve each row, folding out-of-budget keys into attrs_raw.
        let rows: Vec<ResolvedSpanRow> = pre
            .into_iter()
            .map(|p| resolve_row(p, &column_of))
            .collect();

        let spans = chunk_blocks(&rows, &self.cfg);

        let mut blocks_bytes: Vec<u8> = Vec::new();
        let mut entries: Vec<BlockEntry> = Vec::with_capacity(spans.len());
        let mut bloom_entries: Vec<Vec<u8>> = Vec::with_capacity(spans.len());
        let mut min_start = i64::MAX;
        let mut max_end = i64::MIN;

        for span in &spans {
            let block_rows = &rows[span.clone()];
            // Dynamic columns present in this block, in ascending id order.
            let present: BTreeSet<u32> = block_rows
                .iter()
                .flat_map(|r| r.columns.iter().map(|(cid, _)| *cid))
                .collect();
            let block_plans: Vec<ColumnPlan> = plans
                .iter()
                .filter(|p| present.contains(&p.column_id))
                .cloned()
                .collect();
            let out: BlockWriteOut = write_block(block_rows, &block_plans, self.cfg.zstd_level)?;
            bloom_entries.push(build_block_bloom(block_rows));
            min_start = min_start.min(out.min_start_ts);
            max_end = max_end.max(out.max_end_ts);
            let block_offset = blocks_bytes.len() as u64;
            blocks_bytes.extend_from_slice(&out.bytes);
            entries.push(BlockEntry {
                block_offset,
                block_len: out.bytes.len() as u64,
                block_crc32c: out.crc32c,
                record_count: out.record_count,
                min_trace_id: out.min_trace_id,
                max_trace_id: out.max_trace_id,
                min_start_ts: out.min_start_ts,
                max_end_ts: out.max_end_ts,
                min_duration_ns: out.min_duration_ns,
                max_duration_ns: out.max_duration_ns,
                status_mask: out.status_mask,
            });
        }

        let skip = SkipIndex::new(entries);

        // Assemble sections in kind order: BLOCKS (raw), SKIP_IDX (zstd),
        // BLOOM (raw).
        let mut object = Vec::new();
        let mut sections: Vec<SectionDesc> = Vec::new();
        push_section(
            &mut object,
            &mut sections,
            kind::BLOCKS,
            Stored::raw(blocks_bytes),
        );
        let skip_stored = compress(&skip.encode(), self.cfg.zstd_level)?;
        push_section(&mut object, &mut sections, kind::SKIP_IDX, skip_stored);
        push_section(
            &mut object,
            &mut sections,
            kind::BLOOM,
            Stored::raw(encode_bloom_section(&bloom_entries)),
        );

        let footer = SpanFooter {
            tenant_hash: self.identity.tenant_hash,
            shard: self.identity.shard,
            writer_id: self.identity.writer_id,
            writer_epoch: self.identity.writer_epoch,
            writer_seq: self.identity.writer_seq,
            min_start_ts_ns: min_start,
            max_end_ts_ns: max_end,
            record_count,
            block_count: spans.len() as u64,
            min_trace_id,
            max_trace_id,
            sections,
            level,
            input_set_hash,
            part_index,
        };
        write_footer_and_trailer(&mut object, &footer);
        Ok(object)
    }
}

/// A record after canonicalizing its attrs and splitting off the lifted
/// `service.name`, the decoded events, and the remaining attribute pairs
/// (sorted, unique keys) that become dynamic columns or attrs_raw overflow.
struct PreResolved {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    name: String,
    start_ts_ns: i64,
    end_ts_ns: i64,
    status_code: crate::record::StatusCode,
    status_message: Option<String>,
    service_name: Option<String>,
    events: Vec<SpanEvent>,
    other: Vec<(String, String)>,
}

/// Pass 1: canonicalize `rec.attrs` (sorted, duplicate keys collapsed last-wins,
/// exactly [`encode_attrs`]'s canonical form) and split off `service.name` and
/// a parseable `_events_raw`. An unparseable `_events_raw` stays in `other` and
/// round-trips as an ordinary attribute.
fn pre_resolve(rec: &SpanRecord) -> PreResolved {
    let mut map: BTreeMap<String, String> = BTreeMap::new();
    for (k, v) in &rec.attrs {
        map.insert(k.clone(), v.clone());
    }
    let service_name = map.remove(SERVICE_NAME_KEY);
    let mut events = Vec::new();
    if let Some(v) = map.get(EVENTS_RAW_KEY)
        && let Some(evs) = parse_events(v)
    {
        events = evs;
        map.remove(EVENTS_RAW_KEY);
    }
    let other: Vec<(String, String)> = map.into_iter().collect();
    PreResolved {
        trace_id: rec.trace_id,
        span_id: rec.span_id,
        parent_span_id: rec.parent_span_id,
        name: rec.name.clone(),
        start_ts_ns: rec.start_ts_ns,
        end_ts_ns: rec.end_ts_ns,
        status_code: rec.status_code,
        status_message: rec.status_message.clone(),
        service_name,
        events,
        other,
    }
}

/// Pass 2: split the "other" attributes into in-budget columns and overflow.
fn resolve_row(p: PreResolved, column_of: &HashMap<String, u32>) -> ResolvedSpanRow {
    let mut columns: Vec<(u32, String)> = Vec::new();
    let mut overflow: Vec<(String, String)> = Vec::new();
    for (k, v) in p.other {
        match column_of.get(&k) {
            Some(&cid) => columns.push((cid, v)),
            None => overflow.push((k, v)),
        }
    }
    columns.sort_by_key(|(cid, _)| *cid);
    let attrs_raw = if overflow.is_empty() {
        None
    } else {
        Some(encode_attrs(&overflow))
    };
    ResolvedSpanRow {
        trace_id: p.trace_id,
        span_id: p.span_id,
        parent_span_id: p.parent_span_id,
        name: p.name,
        start_ts_ns: p.start_ts_ns,
        end_ts_ns: p.end_ts_ns,
        status_code: p.status_code,
        status_message: p.status_message,
        service_name: p.service_name,
        columns,
        attrs_raw,
        events: p.events,
    }
}

/// Builds one block's serialized bloom entry (ADR-0054): span-name tokens
/// scoped to field [`COL_NAME`], and `service.name` tokens scoped to field
/// [`COL_SERVICE_NAME`], so a `name = <lit>` probe and a `service_name = <lit>`
/// probe read the same bloom through disjoint field ids. Token rule is
/// `ravel-codec`'s normative tokenizer, shared with RLOG.
fn build_block_bloom(rows: &[ResolvedSpanRow]) -> Vec<u8> {
    let mut builder = BloomBuilder::new(BLOOM_SEED);
    for r in rows {
        for tok in tokens(&r.name) {
            builder.insert(COL_NAME, &tok);
        }
        if let Some(service) = &r.service_name {
            for tok in tokens(service) {
                builder.insert(COL_SERVICE_NAME, &tok);
            }
        }
    }
    builder.finish()
}

/// Splits row indices into block spans by record target and an estimated
/// uncompressed byte cap.
fn chunk_blocks(rows: &[ResolvedSpanRow], cfg: &RspanConfig) -> Vec<std::ops::Range<usize>> {
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

/// A rough uncompressed byte estimate for one resolved span, for the block byte
/// cap.
fn row_estimate(row: &ResolvedSpanRow) -> usize {
    let mut est = 48; // fixed-field overhead (ids, timestamps, status)
    est += row.name.len();
    if let Some(m) = &row.status_message {
        est += m.len();
    }
    if let Some(s) = &row.service_name {
        est += s.len();
    }
    if let Some(raw) = &row.attrs_raw {
        est += raw.len();
    }
    for (_, v) in &row.columns {
        est += v.len() + 4;
    }
    for ev in &row.events {
        est += 12 + ev.name.len() + ev.attrs_blob.len();
    }
    est
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

/// Whole-section zstd compression (SKIP_IDX).
fn compress(raw: &[u8], level: i32) -> Result<Stored, SpanSegError> {
    let compressed = zstd::bulk::compress(raw, level)
        .map_err(|e| SpanSegError::Corrupted(format!("zstd compress: {e}")))?;
    Ok(Stored {
        bytes: compressed,
        comp: COMP_ZSTD,
        uncomp_len: raw.len() as u64,
    })
}

/// Appends a section's stored bytes and records its descriptor.
fn push_section(object: &mut Vec<u8>, sections: &mut Vec<SectionDesc>, kind: u32, stored: Stored) {
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
    use crate::record::StatusCode;

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [1u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

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
    fn empty_input_rejected() {
        let w = RspanWriter::new(RspanConfig::default(), identity());
        assert!(matches!(w.finish(), Err(SpanSegError::LimitExceeded(_))));
    }

    #[test]
    fn end_to_end_and_deterministic() {
        let cfg = RspanConfig {
            block_target_records: 100,
            ..RspanConfig::default()
        };
        let build = || {
            let mut w = RspanWriter::new(cfg, identity());
            for i in 0..1000i64 {
                let trace = (i % 4) as u8;
                let mut r = span(trace, i as u8, i, i + 10);
                r.attrs = vec![
                    ("service.name".into(), format!("s{trace}")),
                    ("http.method".into(), "GET".into()),
                ];
                w.push(r);
            }
            w.finish().expect("finish")
        };
        let a = build();
        let b = build();
        assert_eq!(a, b, "identical input must be byte-identical");

        let footer = open(&a).expect("open");
        assert_eq!(footer.record_count, 1000);
        assert_eq!(footer.block_count, 10);
        assert_eq!(footer.min_start_ts_ns, 0);
        assert_eq!(footer.max_end_ts_ns, 1009);
        assert_eq!(footer.min_trace_id, [0u8; 16]);
        assert_eq!(footer.max_trace_id, [3u8; 16]);
    }

    /// `finish_compacted` stamps the caller's compaction identity, shares its
    /// section bodies byte-for-byte with a plain `finish` of the same records,
    /// and reproduces byte-identical whole-object bytes on an independent
    /// rebuild (the crash-recovery convergence property).
    #[test]
    fn finish_compacted_stamps_identity_shares_body_and_is_rebuild_stable() {
        let build = |records: &[SpanRecord]| {
            let mut w = RspanWriter::new(RspanConfig::default(), identity());
            for r in records {
                w.push(r.clone());
            }
            w
        };
        let records: Vec<SpanRecord> = (0..50i64)
            .map(|i| {
                let mut r = span((i % 3) as u8, i as u8, i, i + 5);
                r.attrs = vec![("k".into(), format!("v{i}"))];
                r
            })
            .collect();

        let l0 = build(&records).finish().expect("finish");
        let hash = vec![0xaa, 0xbb, 0xcc];
        let l1 = build(&records)
            .finish_compacted(1, hash.clone(), 2)
            .expect("finish_compacted");

        let f0 = open(&l0).expect("open l0");
        let f1 = open(&l1).expect("open l1");
        assert_eq!(f0.level, 0);
        assert!(f0.input_set_hash.is_empty());
        assert_eq!(f0.part_index, 0);
        assert_eq!(f1.level, 1);
        assert_eq!(f1.input_set_hash, hash);
        assert_eq!(f1.part_index, 2);
        assert_eq!(f0.sections, f1.sections);
        for s in &f0.sections {
            let range = |o: u64, l: u64| (o as usize)..((o + l) as usize);
            assert_eq!(&l0[range(s.offset, s.len)], &l1[range(s.offset, s.len)]);
        }

        let l1_rebuild = build(&records)
            .finish_compacted(1, hash.clone(), 2)
            .expect("finish_compacted rebuild");
        assert_eq!(
            l1, l1_rebuild,
            "finish_compacted must reproduce byte-identical whole-object bytes on rebuild"
        );
    }
}
