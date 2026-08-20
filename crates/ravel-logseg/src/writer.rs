//! Object writer: records in, a whole RLOG object out
//! (docs/log-segment-format.md).
//!
//! [`RlogWriter::finish`] runs the full pipeline: collect and sort the stream
//! directory, assign dense stream refs, sort records by `(stream_ref, ts)`,
//! split dynamic attributes into per-type columns under the 1000-column budget
//! (overflow keys fold into `attrs_raw`), chunk into blocks, build per-block
//! token blooms and the skip index, compress the whole-read sections, and emit
//! the footer and trailer. Identical input yields byte-identical output.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use ravel_types::logstream::{LogStreamId, canonical_attr_bytes};

use crate::block::{ColumnPlan, write_block};
use crate::bloom::BloomBuilder;
use crate::bloom_section::encode_bloom_section;
use crate::error::LogSegError;
use crate::field_dir::{FieldDir, FieldEntry};
use crate::footer::{COMP_NONE, COMP_ZSTD, LogFooter, SectionDesc, kind, write_footer_and_trailer};
use crate::postings::{DEFAULT_STRIDE, FieldTerms, encode_postings_section, term_key};
use crate::reader::stream_attr_pairs;
use crate::record::{
    COL_BODY, COL_SEVERITY_TEXT, ColumnValue, FIRST_DYNAMIC_COL, FieldType, LogRecord, ResolvedRow,
    canonical_value_bytes, resolve_value,
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
    /// Per-field cap on distinct values a POSTINGS term dictionary may carry
    /// for one object. A field named via [`RlogWriter::with_indexed_fields`]
    /// that exceeds this in one object has its postings dropped for that
    /// object and `WriteStats::postings_capped_fields` incremented; the field
    /// stays queryable through BLOOM plus the exact scan
    /// (docs/adrs/0049-rlog-postings.md decision 4).
    pub postings_max_distinct: usize,
    /// Terms per POSTINGS term block (docs/adrs/0049-rlog-postings.md
    /// decision 2). Must be nonzero.
    pub postings_stride: u32,
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
            postings_max_distinct: 10_000,
            postings_stride: DEFAULT_STRIDE,
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
    indexed_fields: Vec<String>,
}

/// Counters describing one write beyond what the object bytes themselves
/// already record.
///
/// The POSTINGS counters here are the write-side half of the ADR-0049
/// metrics. They are deliberately shaped so a `/metrics` renderer can
/// aggregate them without a per-field label, which the ADR-0044 label
/// allowlist forbids: `postings_distinct_total` over `postings_indexed_fields`
/// yields a mean distinct-per-field, and `postings_distinct_max` the tail,
/// with no field name ever leaving this struct.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WriteStats {
    /// Indexed fields dropped from POSTINGS in this object for exceeding
    /// `RlogConfig::postings_max_distinct`
    /// (docs/adrs/0049-rlog-postings.md decision 4).
    pub postings_capped_fields: u32,
    /// Encoded byte length of this object's POSTINGS section, or 0 when the
    /// object carries none (no indexed field resolved to a dynamic column).
    pub postings_bytes: u64,
    /// Number of indexed fields that emitted a (non-capped) posting list in
    /// this object. The denominator for a per-field distinct-value average; a
    /// capped field is excluded (its term dictionary was dropped).
    pub postings_indexed_fields: u32,
    /// Sum of distinct values across every non-capped indexed field's term
    /// dictionary in this object.
    pub postings_distinct_total: u64,
    /// Largest distinct-value count of any single non-capped indexed field in
    /// this object, or 0 when no field emitted a posting list.
    pub postings_distinct_max: u32,
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
            indexed_fields: Vec::new(),
        }
    }

    /// Configures which dynamic attribute names get a POSTINGS entry
    /// (docs/adrs/0049-rlog-postings.md decision 3: opt-in per field, never
    /// automatic). A name with no matching dynamic column in this object
    /// (never seen, or folded into `attrs_raw` overflow past the
    /// `max_dynamic_columns` budget) simply has no postings -- always-legal
    /// degradation, not an error. A separate `[Type Str]` and `[Type I64]`
    /// column sharing this name (see `field_dir_splits_types`) both get
    /// postings, since they are distinct columns.
    pub fn with_indexed_fields(mut self, fields: Vec<String>) -> Self {
        self.indexed_fields = fields;
        self
    }

    /// Buffers one record.
    pub fn push(&mut self, rec: LogRecord) -> Result<(), LogSegError> {
        self.records.push(rec);
        Ok(())
    }

    /// Produces the whole object as an L0 flush object: the compaction-identity
    /// fields are stamped at their L0 sentinels (`level = 0`, empty
    /// `input_set_hash`, `part_index = 0`). Empty input is rejected: the flush
    /// layer never writes empty objects (matches RSEG's zero-sample rule).
    pub fn finish(self) -> Result<Vec<u8>, LogSegError> {
        self.finish_with_stats().map(|(bytes, _)| bytes)
    }

    /// Like [`RlogWriter::finish`], but also returns counters
    /// ([`WriteStats`]) not otherwise recoverable from the object bytes.
    pub fn finish_with_stats(self) -> Result<(Vec<u8>, WriteStats), LogSegError> {
        self.build_object(0, Vec::new(), 0)
    }

    /// Produces the whole object as an L1 compacted part, stamping the caller's
    /// compaction identity (`level`, `input_set_hash`, `part_index`) into the
    /// footer instead of the L0 sentinels (ADR-0032). Every other byte is
    /// produced by the exact same pipeline as [`RlogWriter::finish`]: the two
    /// share [`RlogWriter::build_object`], so the STREAM_DIR / FIELD_DIR /
    /// BLOCKS / SKIP_IDX / BLOOM encoding (including the dynamic-column cap and
    /// `attrs_raw` overflow rule) cannot drift between an L0 write and an L1
    /// merge. The compactor (`ravel-maintain`) is the only caller.
    ///
    /// `input_set_hash` is the compaction's canonical input-set hash (the same
    /// bytes the [`ravel_proto::logseg::v1::LogFooter`] and the
    /// `CompactionRecord` carry); `level` is 1 for an L1 part and `part_index`
    /// is the part's ordinal within one compaction output. Empty input is
    /// rejected exactly as [`RlogWriter::finish`] rejects it.
    pub fn finish_compacted(
        self,
        level: u32,
        input_set_hash: Vec<u8>,
        part_index: u32,
    ) -> Result<Vec<u8>, LogSegError> {
        self.finish_compacted_with_stats(level, input_set_hash, part_index)
            .map(|(bytes, _)| bytes)
    }

    /// Like [`RlogWriter::finish_compacted`], but also returns counters
    /// ([`WriteStats`]) not otherwise recoverable from the object bytes.
    pub fn finish_compacted_with_stats(
        self,
        level: u32,
        input_set_hash: Vec<u8>,
        part_index: u32,
    ) -> Result<(Vec<u8>, WriteStats), LogSegError> {
        self.build_object(level, input_set_hash, part_index)
    }

    /// The shared object-building pipeline behind [`RlogWriter::finish`] and
    /// [`RlogWriter::finish_compacted`]. The only inputs that vary between the
    /// two are the footer's compaction-identity fields; every section is built
    /// identically, so identical records plus identical identity yield
    /// byte-identical output regardless of which entry point was used.
    fn build_object(
        self,
        level: u32,
        input_set_hash: Vec<u8>,
        part_index: u32,
    ) -> Result<(Vec<u8>, WriteStats), LogSegError> {
        if self.records.is_empty() {
            return Err(LogSegError::LimitExceeded("empty object".into()));
        }

        // Stream directory: distinct ids, sorted, each with the canonical
        // resource+scope blob from the first record seen for it. The dense ref
        // is the ordinal. A second record claiming the same id with different
        // `stream_attrs` bytes has no truthful blob, so the whole object is
        // refused rather than silently keeping the first.
        let mut streams: BTreeMap<LogStreamId, &[u8]> = BTreeMap::new();
        for r in &self.records {
            match streams.entry(r.stream_id) {
                Entry::Vacant(slot) => {
                    slot.insert(r.stream_attrs.as_slice());
                }
                Entry::Occupied(slot) => {
                    if *slot.get() != r.stream_attrs.as_slice() {
                        return Err(LogSegError::InconsistentStreamAttrs(format!(
                            "stream {} carries two different stream_attrs blobs ({} and {} bytes)",
                            r.stream_id.to_hex(),
                            slot.get().len(),
                            r.stream_attrs.len(),
                        )));
                    }
                }
            }
        }
        let sorted_ids: Vec<LogStreamId> = streams.keys().copied().collect();
        let mut ref_of: HashMap<LogStreamId, u32> = HashMap::with_capacity(sorted_ids.len());
        for (i, id) in sorted_ids.iter().enumerate() {
            ref_of.insert(*id, i as u32);
        }

        // The caller's indexed-field list (docs/adrs/0049-rlog-postings.md
        // decision 3: opt-in per field). Resolved before column assignment
        // because an indexed field that appears only at stream (resource/scope)
        // level still gets a dynamic column below, so its
        // merged-view postings have a column_id to key by.
        let indexed_names: std::collections::HashSet<&str> =
            self.indexed_fields.iter().map(String::as_str).collect();

        // Dynamic column assignment: distinct (name, type) sorted by
        // (name bytes, type), the first `max_dynamic_columns` get columns.
        let mut distinct: BTreeSet<(String, u8)> = BTreeSet::new();
        for r in &self.records {
            for (k, v) in &r.attrs {
                let (ty, _) = resolve_value(v);
                distinct.insert((k.clone(), ty.to_u8()));
            }
        }
        // Stream-level-only indexed columns (ADR-0049 amendment). A key that
        // is resource- or scope-level across the whole object and per-record
        // on no record gets no column from the per-record loop above, yet
        // `service.name` on the resource is the ordinary OTLP shape the
        // amendment exists for. Give each indexed stream-level (name, type) a
        // column so `merged_indexed_terms` can key its merged-view postings
        // by it.
        //
        // The column is a POSTINGS KEY, not a materialized per-record value: no
        // row writes a value to it (`resolve_row` populates `columns` from
        // `r.attrs` only), so it stays all-null in every block. The reader's
        // `equals` on a `FieldSel::Attr` therefore still reads only the
        // per-record layer and returns false for a value that lives solely in
        // the resource blob -- the exact channel stays a strict per-record
        // predicate, distinct from the prune channel that probes these postings
        // (docs/log-segment-format.md "FIELD_DIR"). These count against the same
        // `max_dynamic_columns` budget as any dynamic column: they occupy a real
        // FIELD_DIR entry, and the indexed list is small and opt-in, so the cost
        // is a handful of columns; one that cannot fit degrades to bloom + exact
        // scan, always legal (decision 5). Decoding `stream_attrs` can fail on a
        // corrupt blob; that is propagated rather than silently under-indexing.
        if !indexed_names.is_empty() {
            for blob in streams.values() {
                for (k, v) in stream_attr_pairs(blob)? {
                    if indexed_names.contains(k.as_str()) {
                        let (ty, _) = resolve_value(&v);
                        distinct.insert((k, ty.to_u8()));
                    }
                }
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

        // Columns whose name is in the caller's indexed-field list get postings.
        // A name past the dynamic-column budget above has no matching column_id
        // here, so it has no postings -- legal degradation, not an error.
        let indexed_column_ids: BTreeSet<u32> = columns
            .iter()
            .filter(|(name, _, _)| indexed_names.contains(name.as_str()))
            .map(|(_, _, cid)| *cid)
            .collect();

        // Resolve every record into storage form, then sort by (stream_ref, ts).
        // `resolve_row` also computes each row's merged-view POSTINGS terms,
        // which decodes the record's `stream_attrs` blob and so can fail on a
        // corrupt blob; that failure is propagated rather than silently
        // under-indexing (an under-populated posting list would prune a block a
        // merged-view query needs).
        let mut rows: Vec<ResolvedRow> = self
            .records
            .iter()
            .map(|r| resolve_row(r, &ref_of, &column_of, &indexed_names))
            .collect::<Result<Vec<_>, _>>()?;
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

        // POSTINGS accumulation: per indexed column, term -> sorted block
        // indices. `BTreeMap`/`BTreeSet` throughout, never `HashMap`, so
        // output stays byte-identical for identical input (the same
        // determinism rule as everything else in this pipeline).
        let mut postings_terms: BTreeMap<u32, BTreeMap<Vec<u8>, BTreeSet<u32>>> = BTreeMap::new();
        let mut postings_capped: BTreeSet<u32> = BTreeSet::new();

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
                // POSTINGS: index each row's merged-view values (resource +
                // scope + per-record, the record winning on a key collision),
                // precomputed in `resolve_row` as `indexed_terms`. That view is
                // what SQL's `attrs` column exposes, so a v2 posting list
                // answers the merged-view query directly
                // (docs/adrs/0049-rlog-postings.md amendment 2026-08-03).
                // `indexed_terms` only ever names indexed columns, so no
                // `indexed_column_ids` check is needed here; the per-field
                // distinct-value cap (decision 4) now counts merged values.
                for (cid, v) in &row.indexed_terms {
                    if postings_capped.contains(cid) {
                        continue;
                    }
                    let field_map = postings_terms.entry(*cid).or_default();
                    field_map
                        .entry(term_key(v))
                        .or_default()
                        .insert(blk_idx_u32);
                    if field_map.len() > self.cfg.postings_max_distinct {
                        postings_terms.remove(cid);
                        postings_capped.insert(*cid);
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
        let stream_entries: Vec<StreamEntry> = streams
            .iter()
            .enumerate()
            .map(|(i, (id, blob))| {
                let r = i as u32;
                StreamEntry {
                    stream_id: *id,
                    blob: blob.to_vec(),
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

        // Final per-field postings verdict: capped fields get `Capped`
        // (their term map was already dropped above), everything else in
        // `indexed_column_ids` gets its accumulated map (empty if the field
        // never appeared in this object -- also always-legal).
        let postings_capped_fields = postings_capped.len() as u32;
        let mut postings_fields: BTreeMap<u32, FieldTerms> = BTreeMap::new();
        // Per-field distinct-value accounting for WriteStats: a
        // non-capped field's distinct count is the size of its term
        // dictionary. Summed and maxed here, never labelled by field name (the
        // ADR-0044 allowlist forbids a `field` label), so the `/metrics`
        // renderer can only ever expose a mean and a tail, not a per-field
        // series.
        let mut postings_indexed_fields: u32 = 0;
        let mut postings_distinct_total: u64 = 0;
        let mut postings_distinct_max: u32 = 0;
        for &cid in &indexed_column_ids {
            if postings_capped.contains(&cid) {
                postings_fields.insert(cid, FieldTerms::Capped);
            } else {
                let map = postings_terms.remove(&cid).unwrap_or_default();
                let distinct = map.len() as u32;
                postings_indexed_fields += 1;
                postings_distinct_total += u64::from(distinct);
                postings_distinct_max = postings_distinct_max.max(distinct);
                postings_fields.insert(cid, FieldTerms::Terms(map));
            }
        }

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
        let mut postings_bytes_len: u64 = 0;
        if !indexed_column_ids.is_empty() {
            let postings_bytes = encode_postings_section(
                &postings_fields,
                self.cfg.postings_stride,
                self.cfg.zstd_level,
            )?;
            postings_bytes_len = postings_bytes.len() as u64;
            push_section(
                &mut object,
                &mut sections,
                kind::POSTINGS,
                &Stored::raw(postings_bytes),
            );
        }

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
            // Compaction identity (ADR-0032). `finish` passes the L0 sentinels
            // (level 0, empty hash, part 0); `finish_compacted` passes the
            // compactor's real values. Stamped verbatim, checked nowhere here:
            // the footer round-trips whatever the caller set.
            level,
            input_set_hash,
            part_index,
        };
        write_footer_and_trailer(&mut object, &footer);
        Ok((
            object,
            WriteStats {
                postings_capped_fields,
                postings_bytes: postings_bytes_len,
                postings_indexed_fields,
                postings_distinct_total,
                postings_distinct_max,
            },
        ))
    }
}

/// Resolves one record into storage form: dense stream ref, dynamic columns
/// split by type, overflow attributes canonicalized into `attrs_raw`, and the
/// merged-view POSTINGS terms this record contributes ([`merged_indexed_terms`]).
fn resolve_row(
    r: &LogRecord,
    ref_of: &HashMap<LogStreamId, u32>,
    column_of: &HashMap<(String, u8), u32>,
    indexed_names: &std::collections::HashSet<&str>,
) -> Result<ResolvedRow, LogSegError> {
    let stream_ref = ref_of.get(&r.stream_id).copied().unwrap_or(0);
    let mut cols: BTreeMap<u32, ColumnValue> = BTreeMap::new();
    let mut overflow: Vec<(String, ravel_types::logstream::AttrValue)> = Vec::new();
    // For each indexed name this record carries, its own occurrences split the
    // same way the read side reconstructs them: the columnar ones (those that
    // took a fresh dynamic-column slot, one per distinct type, each with its
    // type byte kept alongside its value) and the overflow ones (any type,
    // duplicates and budget overflow alike). `merged_indexed_terms` reduces
    // these to the single occurrence `rebuild_record` + `merged_attrs` report
    // for the name (docs/adrs/0049-rlog-postings.md amendment 2026-08-20).
    let mut idx_cols: BTreeMap<String, Vec<(u8, ColumnValue)>> = BTreeMap::new();
    let mut idx_overflow: BTreeMap<String, Vec<ravel_types::logstream::AttrValue>> =
        BTreeMap::new();
    for (k, v) in &r.attrs {
        let (ty, cv) = resolve_value(v);
        let indexed = indexed_names.contains(k.as_str());
        match column_of.get(&(k.clone(), ty.to_u8())) {
            Some(cid) if !cols.contains_key(cid) => {
                if indexed {
                    idx_cols
                        .entry(k.clone())
                        .or_default()
                        .push((ty.to_u8(), cv.clone()));
                }
                cols.insert(*cid, cv);
            }
            // Overflow column, or a duplicate (name,type) already columnar this
            // row: fold into attrs_raw so no value is lost.
            _ => {
                if indexed {
                    idx_overflow.entry(k.clone()).or_default().push(v.clone());
                }
                overflow.push((k.clone(), v.clone()));
            }
        }
    }
    let attrs_raw = if overflow.is_empty() {
        None
    } else {
        Some(canonical_attr_bytes(&overflow))
    };
    let indexed_terms =
        merged_indexed_terms(r, indexed_names, column_of, &idx_cols, &idx_overflow)?;
    Ok(ResolvedRow {
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
        indexed_terms,
    })
}

/// The merged-view POSTINGS terms one record contributes.
///
/// For each indexed field the record resolves to after merging its resource,
/// scope, and per-record attributes (the record winning on a key collision),
/// this returns `(column_id, value)` under the dynamic column that value's type
/// resolves to. That precedence is exactly what
/// `ravel_sql::rlog_attrs::merged_attrs` computes for SQL's `attrs` column,
/// reproduced here so the writer does not depend on ravel-sql
/// (docs/adrs/0049-rlog-postings.md amendment 2026-08-03).
///
/// When a record carries one indexed key more than once (any type mix), the
/// winning occurrence is the one `rebuild_record` + `merged_attrs` will report,
/// not a re-derived guess: see [`record_level_winners`] for the exact two-tier
/// order (docs/adrs/0049-rlog-postings.md amendment 2026-08-20, closing issue
/// #333). The record layer then wins over the stream (resource/scope) layer as
/// before.
///
/// A field with no matching dynamic column contributes nothing. The writer
/// gives an indexed field a column even when it appears only at stream
/// level, so a resource-only indexed key normally does have a column to
/// key a posting by; a key stays column-less only when it overflowed the
/// dynamic-column budget or is not in the indexed-field list. Absence is legal;
/// no posting for a field means no pruning on it, never a wrong prune. The
/// decode of `stream_attrs` can fail on a corrupt blob, which is propagated
/// (the caller fails the write) rather than silently dropping stream-level
/// terms, since an under-populated posting list would prune a needed block.
fn merged_indexed_terms(
    r: &LogRecord,
    indexed_names: &std::collections::HashSet<&str>,
    column_of: &HashMap<(String, u8), u32>,
    idx_cols: &BTreeMap<String, Vec<(u8, ColumnValue)>>,
    idx_overflow: &BTreeMap<String, Vec<ravel_types::logstream::AttrValue>>,
) -> Result<Vec<(u32, ColumnValue)>, LogSegError> {
    if indexed_names.is_empty() {
        return Ok(Vec::new());
    }
    // Merge, restricted to indexed keys, record-wins: resource/scope pairs
    // first, then each indexed key's record-level winner overrides in place or
    // appends. This mirrors `merged_attrs` (resource/scope, then record attrs
    // win). Each entry carries the type byte its value resolves to, so a
    // record-level winner of a different type than the stream value keys its
    // posting by the right column.
    let mut merged: Vec<(String, (u8, ColumnValue))> = Vec::new();
    for (k, v) in stream_attr_pairs(&r.stream_attrs)? {
        if indexed_names.contains(k.as_str()) {
            let (ty, cv) = resolve_value(&v);
            merged.push((k, (ty.to_u8(), cv)));
        }
    }
    for (name, winner) in record_level_winners(idx_cols, idx_overflow) {
        if let Some(slot) = merged.iter_mut().find(|(mk, _)| *mk == name) {
            slot.1 = winner;
        } else {
            merged.push((name, winner));
        }
    }
    let mut out = Vec::with_capacity(merged.len());
    for (name, (ty_byte, cv)) in merged {
        if let Some(&cid) = column_of.get(&(name, ty_byte)) {
            out.push((cid, cv));
        }
    }
    Ok(out)
}

/// The record-level winning occurrence of each indexed key this record carries,
/// keyed by name and carrying the winner's `(type byte, value)`.
///
/// The winner is defined as whatever the read side already, deterministically
/// produces: `rebuild_record` lays a record's own attributes out as its
/// columnar entries in FIELD_DIR `(name bytes, type)`-ascending order, followed
/// by its `attrs_raw` overflow entries in `encode_attrs`'s canonical
/// `(key bytes, encoded value bytes)`-ascending order, and
/// `rlog_attrs::merged_attrs` then folds that list last-wins by name. Restricted
/// to one name, that combined order is: the columnar occurrences ascending by
/// [`FieldType::to_u8`], then the overflow occurrences ascending by the frozen
/// canonical encoding of the value ([`canonical_value_bytes`], which shares the
/// exact comparator `encode_attrs` uses so the two cannot drift). The last entry
/// of that order wins. Making the write side predict this, rather than
/// independently pick a winner over the original write-time occurrence order the
/// on-disk format does not preserve, is the issue #333 fix
/// (docs/adrs/0049-rlog-postings.md amendment 2026-08-20).
fn record_level_winners(
    idx_cols: &BTreeMap<String, Vec<(u8, ColumnValue)>>,
    idx_overflow: &BTreeMap<String, Vec<ravel_types::logstream::AttrValue>>,
) -> BTreeMap<String, (u8, ColumnValue)> {
    let mut names: BTreeSet<&String> = BTreeSet::new();
    names.extend(idx_cols.keys());
    names.extend(idx_overflow.keys());

    let mut out: BTreeMap<String, (u8, ColumnValue)> = BTreeMap::new();
    for name in names {
        let mut combined: Vec<(u8, ColumnValue)> = Vec::new();
        // Columnar occurrences first, ascending by type byte (FIELD_DIR's
        // (name, type) sort key restricted to this one name).
        if let Some(cols) = idx_cols.get(name) {
            let mut cols = cols.clone();
            cols.sort_by_key(|(ty_byte, _)| *ty_byte);
            combined.extend(cols);
        }
        // Overflow occurrences next, ascending by the canonical encoding of a
        // one-entry value set. They all share this name, so this orders by
        // encoded value bytes exactly as `attrs_raw` stores (and the reader
        // decodes) them.
        if let Some(over) = idx_overflow.get(name) {
            let mut keyed: Vec<(Vec<u8>, (u8, ColumnValue))> = over
                .iter()
                .map(|v| {
                    let (ty, cv) = resolve_value(v);
                    (canonical_value_bytes(v), (ty.to_u8(), cv))
                })
                .collect();
            keyed.sort_by(|a, b| a.0.cmp(&b.0));
            combined.extend(keyed.into_iter().map(|(_, entry)| entry));
        }
        if let Some(winner) = combined.pop() {
            out.insert(name.clone(), winner);
        }
    }
    out
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
    use crate::reader::{RlogReader, read_section};
    use crate::record::{Predicate, stream_attrs_bytes};
    use ravel_types::logstream::{AttrValue, log_stream_id};

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

    /// The canonical resource+scope blob for the synthetic stream `n`: distinct
    /// per stream, so a wrong blob-to-stream mapping cannot pass unnoticed.
    fn attrs_blob(n: u8) -> Vec<u8> {
        stream_attrs_bytes(
            &[("service.name".into(), AttrValue::Str(format!("svc{n}")))],
            "scope",
            "1.0",
            &[("lib".into(), AttrValue::I64(i64::from(n)))],
        )
    }

    fn base_record(stream: u8, ts: i64) -> LogRecord {
        LogRecord {
            stream_id: id(stream),
            stream_attrs: attrs_blob(stream),
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
    fn finish_compacted_stamps_identity_and_shares_body_with_finish() {
        // Two writers over identical records: one L0 via finish(), one L1 via
        // finish_compacted(). Every section byte is identical; only the footer's
        // compaction-identity fields differ. This is the guarantee that lets the
        // compactor reuse the writer without the L0 and L1 encoders drifting.
        let build = |records: &[LogRecord]| {
            let mut w = RlogWriter::new(RlogConfig::default(), identity());
            for r in records {
                w.push(r.clone()).expect("push");
            }
            w
        };
        let records: Vec<LogRecord> = (0..50i64)
            .map(|i| {
                let mut r = base_record((i % 3) as u8, i);
                r.attrs.push(("k".into(), AttrValue::I64(i)));
                r
            })
            .collect();

        let l0 = build(&records).finish().expect("finish");
        let hash = vec![0xaa, 0xbb, 0xcc, 0xdd];
        let l1 = build(&records)
            .finish_compacted(1, hash.clone(), 3)
            .expect("finish_compacted");

        let f0 = open(&l0).expect("open l0");
        let f1 = open(&l1).expect("open l1");
        // L0 sentinels unchanged.
        assert_eq!(f0.level, 0);
        assert!(f0.input_set_hash.is_empty());
        assert_eq!(f0.part_index, 0);
        // L1 identity stamped.
        assert_eq!(f1.level, 1);
        assert_eq!(f1.input_set_hash, hash);
        assert_eq!(f1.part_index, 3);
        // Every section is byte-identical between the two objects: the sections
        // live before the footer, so equal section tables plus equal section
        // bytes prove the body did not change.
        assert_eq!(f0.sections, f1.sections, "section tables identical");
        for s in &f0.sections {
            let range = |o: u64, l: u64| (o as usize)..((o + l) as usize);
            assert_eq!(
                &l0[range(s.offset, s.len)],
                &l1[range(s.offset, s.len)],
                "section kind {} bytes identical",
                s.kind
            );
        }
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

    /// Decodes the STREAM_DIR of a written object.
    fn read_stream_dir(obj: &[u8]) -> StreamDir {
        let cfg = RlogConfig::default();
        let footer = open(obj).expect("open");
        let desc = footer.section(kind::STREAM_DIR).expect("stream_dir");
        let raw = read_section(obj, desc, &cfg).expect("read section");
        StreamDir::decode(&raw, 1 << 24).expect("decode")
    }

    #[test]
    fn stream_dir_blobs_carry_stream_attrs() {
        // Three real streams: ids and blobs both come from the same
        // resource+scope, so the object records true identity, not placeholders.
        struct Spec {
            resource: Vec<(String, AttrValue)>,
            scope: &'static str,
            version: &'static str,
        }
        let streams = [
            Spec {
                resource: vec![("service.name".into(), AttrValue::Str("api".into()))],
                scope: "scope.a",
                version: "1.0",
            },
            Spec {
                resource: vec![("service.name".into(), AttrValue::Str("worker".into()))],
                scope: "scope.b",
                version: "2.3",
            },
            Spec {
                resource: vec![],
                scope: "",
                version: "",
            },
        ];
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        let mut want: Vec<(LogStreamId, Vec<u8>)> = Vec::new();
        for (i, spec) in streams.iter().enumerate() {
            let (res, scope, ver) = (&spec.resource, spec.scope, spec.version);
            let sid = log_stream_id(res, scope, ver, &[]);
            let blob = stream_attrs_bytes(res, scope, ver, &[]);
            want.push((sid, blob.clone()));
            // Two records per stream so the first-record blob is reused, not
            // re-derived per record.
            for ts in 0..2i64 {
                let mut r = base_record(0, ts);
                r.stream_id = sid;
                r.stream_attrs = blob.clone();
                r.body = format!("stream {i} record {ts}");
                w.push(r).expect("push");
            }
        }
        let obj = w.finish().expect("finish");

        let dir = read_stream_dir(&obj);
        assert_eq!(dir.len(), 3);
        want.sort_by_key(|(sid, _)| *sid);
        for (entry, (sid, blob)) in dir.entries().iter().zip(want.iter()) {
            assert_eq!(entry.stream_id, *sid);
            assert!(!entry.blob.is_empty(), "blob must not be empty");
            assert_eq!(&entry.blob, blob, "blob must be the record's stream_attrs");
            // The blob is the hash preimage: it reproduces the stream id.
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"ravel-logstream-v1");
            hasher.update(&entry.blob);
            assert_eq!(&hasher.finalize().as_bytes()[..16], &sid.0);
        }

        // The same blobs come back through the reader on every record.
        let cfg = RlogConfig::default();
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        assert_eq!(rows.len(), 6);
        for row in &rows {
            let expected = want
                .iter()
                .find(|(sid, _)| *sid == row.stream_id)
                .map(|(_, blob)| blob)
                .expect("known stream");
            assert_eq!(&row.stream_attrs, expected);
        }
    }

    #[test]
    fn same_stream_same_attrs_succeeds() {
        // The common case: many records per stream, all agreeing.
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        for ts in 0..3i64 {
            w.push(base_record(4, ts)).expect("push");
        }
        let obj = w.finish().expect("identical stream_attrs must not collide");
        let dir = read_stream_dir(&obj);
        assert_eq!(dir.len(), 1);
        assert_eq!(dir.entries()[0].blob, attrs_blob(4));
    }

    #[test]
    fn same_stream_different_attrs_rejected() {
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        w.push(base_record(4, 0)).expect("push");
        let mut clash = base_record(4, 1);
        clash.stream_attrs = attrs_blob(9); // same stream_id, other blob
        w.push(clash).expect("push");
        let err = w.finish().expect_err("colliding stream_attrs must fail");
        match err {
            LogSegError::InconsistentStreamAttrs(msg) => {
                assert!(
                    msg.contains(&id(4).to_hex()),
                    "message must name the stream: {msg}"
                );
            }
            other => panic!("expected InconsistentStreamAttrs, got {other:?}"),
        }
    }

    #[test]
    fn empty_stream_attrs_is_a_valid_blob() {
        // An empty resource+scope still has a non-empty canonical blob (two
        // zero counts and two zero lengths), so no valid object has an empty
        // STREAM_DIR blob.
        let blob = stream_attrs_bytes(&[], "", "", &[]);
        assert_eq!(blob, vec![0u8, 0, 0, 0]);
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

    #[test]
    fn no_indexed_fields_omits_postings_section() {
        // No `with_indexed_fields` call: absence is always legal
        // (docs/adrs/0049-rlog-postings.md decision 5), so the section is
        // omitted entirely rather than written empty.
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        w.push(base_record(0, 0)).expect("push");
        let obj = w.finish().expect("finish");
        let footer = open(&obj).expect("open");
        assert!(footer.section(kind::POSTINGS).is_none());
    }

    /// Decodes FIELD_DIR from a written object.
    fn read_field_dir(obj: &[u8], footer: &LogFooter) -> FieldDir {
        let fd_desc = footer.section(kind::FIELD_DIR).expect("field_dir");
        let start = fd_desc.offset as usize;
        let end = start + fd_desc.len as usize;
        let raw = zstd::bulk::decompress(&obj[start..end], fd_desc.uncomp_len as usize)
            .expect("decompress");
        FieldDir::decode(&raw, 1 << 20).expect("decode")
    }

    #[test]
    fn indexed_field_postings_round_trip() {
        // 4 records per block, 5 distinct "svc" values cycling every record:
        // record i lands in block i / 4 and carries value "s{i % 5}".
        let cfg = RlogConfig {
            block_target_records: 4,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(cfg, identity()).with_indexed_fields(vec!["svc".to_string()]);
        for i in 0..20i64 {
            let mut r = base_record(0, i);
            r.attrs
                .push(("svc".into(), AttrValue::Str(format!("s{}", i % 5))));
            w.push(r).expect("push");
        }
        let obj = w.finish().expect("finish");
        let footer = open(&obj).expect("open");

        let fd = read_field_dir(&obj, &footer);
        let cid = fd
            .column("svc", FieldType::Str)
            .expect("svc column present")
            .column_id;

        let pd_desc = footer
            .section(kind::POSTINGS)
            .expect("postings section present");
        let pd_bytes = &obj[pd_desc.offset as usize..(pd_desc.offset + pd_desc.len) as usize];
        let section = crate::postings::PostingsSection::parse(pd_bytes).expect("parse postings");

        for v in 0..5i64 {
            let term = format!("s{v}").into_bytes();
            let blocks: BTreeSet<u32> = section
                .probe(cid, &term)
                .expect("probe")
                .expect("field is indexed")
                .into_iter()
                .collect();
            let expected: BTreeSet<u32> = (0..20i64)
                .filter(|i| i % 5 == v)
                .map(|i| (i / 4) as u32)
                .collect();
            assert_eq!(blocks, expected, "value s{v}");
        }
    }

    #[test]
    fn write_stats_report_postings_bytes_and_distinct_counts() {
        // Two indexed fields, "svc" with 5 distinct values and "env" with 2.
        // The write-side POSTINGS metrics must report both fields,
        // the summed distinct count (7), the per-field maximum (5), and a
        // non-zero section byte length.
        let cfg = RlogConfig {
            block_target_records: 4,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(cfg, identity())
            .with_indexed_fields(vec!["svc".to_string(), "env".to_string()]);
        for i in 0..20i64 {
            let mut r = base_record(0, i);
            r.attrs
                .push(("svc".into(), AttrValue::Str(format!("s{}", i % 5))));
            r.attrs
                .push(("env".into(), AttrValue::Str(format!("e{}", i % 2))));
            w.push(r).expect("push");
        }
        let (_obj, stats) = w.finish_with_stats().expect("finish");
        assert_eq!(stats.postings_capped_fields, 0);
        assert_eq!(stats.postings_indexed_fields, 2, "svc and env both indexed");
        assert_eq!(stats.postings_distinct_total, 7, "5 (svc) + 2 (env)");
        assert_eq!(stats.postings_distinct_max, 5, "svc is the wider field");
        assert!(
            stats.postings_bytes > 0,
            "an object carrying postings reports a non-zero section length"
        );
    }

    #[test]
    fn write_stats_report_no_postings_when_no_field_configured() {
        // No indexed fields: no POSTINGS section, so every write-side POSTINGS
        // counter reports zero (absence is always legal, decision 5).
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        w.push(base_record(0, 0)).expect("push");
        let (_obj, stats) = w.finish_with_stats().expect("finish");
        assert_eq!(stats, WriteStats::default());
    }

    #[test]
    fn postings_cap_drops_field_and_raises_counter() {
        // 50 distinct values, cap of 2: the field must be dropped, the
        // counter must fire, and the field must read back as "not indexed"
        // (Ok(None)), never as a narrowed or wrong result
        // (docs/adrs/0049-rlog-postings.md decision 4 and 5).
        let cfg = RlogConfig {
            block_target_records: 4,
            postings_max_distinct: 2,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(cfg, identity()).with_indexed_fields(vec!["svc".to_string()]);
        for i in 0..50i64 {
            let mut r = base_record(0, i);
            r.attrs
                .push(("svc".into(), AttrValue::Str(format!("s{i}"))));
            w.push(r).expect("push");
        }
        let (obj, stats) = w.finish_with_stats().expect("finish");
        assert_eq!(stats.postings_capped_fields, 1);
        // A capped field contributes no term dictionary, so it is excluded from
        // the distinct-value accounting entirely.
        assert_eq!(stats.postings_indexed_fields, 0);
        assert_eq!(stats.postings_distinct_total, 0);
        assert_eq!(stats.postings_distinct_max, 0);

        let footer = open(&obj).expect("open");
        let fd = read_field_dir(&obj, &footer);
        let cid = fd
            .column("svc", FieldType::Str)
            .expect("svc column present")
            .column_id;
        let pd_desc = footer
            .section(kind::POSTINGS)
            .expect("postings section still present (other fields could exist)");
        let pd_bytes = &obj[pd_desc.offset as usize..(pd_desc.offset + pd_desc.len) as usize];
        let section = crate::postings::PostingsSection::parse(pd_bytes).expect("parse postings");
        assert_eq!(
            section.probe(cid, b"s0").expect("probe"),
            None,
            "capped field must read back as not-indexed, not as an empty/wrong result"
        );

        // The field is still fully queryable via the exact scan: every
        // record with "svc" = "s7" is still found, capped postings or not.
        let reader = RlogReader::new(&obj, &cfg).expect("open reader");
        let (rows, _) = reader
            .scan(&Predicate::Equals {
                field: crate::record::FieldSel::Attr("svc".into()),
                value: AttrValue::Str("s7".into()),
            })
            .expect("scan");
        assert_eq!(rows.len(), 1);
    }
}
