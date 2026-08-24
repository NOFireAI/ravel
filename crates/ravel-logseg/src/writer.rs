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

use ravel_types::logstream::{AttrValue, LogStreamId, canonical_attr_bytes};

use crate::block::{ColumnPlan, ColumnarBlockInput, write_block, write_block_columnar};
use crate::bloom::BloomBuilder;
use crate::bloom_section::encode_bloom_section;
use crate::columnar_batch::ColumnarLogBatch;
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
    /// Accumulated columnar batches (ADR-0109). A writer is row-major or
    /// columnar for its lifetime, never both (decision 5): `push` and
    /// `push_columnar` each refuse if the other has already been used.
    batches: Vec<ColumnarLogBatch>,
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
    /// Distinct `(name, type)` pairs that received a real dynamic column in
    /// this object (docs/log-segment-format.md "FIELD_DIR"). Shaped like the
    /// POSTINGS counters above: aggregate-only, no per-field label (the
    /// ADR-0044 allowlist forbids one), so a `/metrics` renderer sees a used
    /// count and, paired with `dynamic_columns_overflowed`, budget pressure
    /// without a column name ever leaving this struct.
    pub dynamic_columns_used: u32,
    /// Distinct `(name, type)` pairs that found the `max_dynamic_columns`
    /// budget full and folded into the `attrs_raw` overflow column instead of
    /// getting their own column. Nonzero exactly when a load crossed the
    /// budget, which is otherwise silent (ADR-0100 decision 1). Same
    /// aggregate-only, no-per-field-label shaping as the fields above.
    pub dynamic_columns_overflowed: u32,
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
            batches: Vec::new(),
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
        if !self.batches.is_empty() {
            return Err(LogSegError::LimitExceeded(
                "row-major push into a columnar writer".into(),
            ));
        }
        self.records.push(rec);
        Ok(())
    }

    /// Buffers one columnar batch (ADR-0109). Appending more than one batch into
    /// one object is supported: the distinct dynamic-column union and the
    /// STREAM_DIR merge span every appended batch, exactly as the row path's
    /// span every `push`. A writer that has already taken a row-major `push`
    /// refuses this (decision 5: a buffer is columnar or row, never both).
    pub fn push_columnar(&mut self, batch: ColumnarLogBatch) -> Result<(), LogSegError> {
        if !self.records.is_empty() {
            return Err(LogSegError::LimitExceeded(
                "columnar push into a row-major writer".into(),
            ));
        }
        if !batch.is_empty() {
            self.batches.push(batch);
        }
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
        if !self.batches.is_empty() {
            return self.build_object_columnar(0, Vec::new(), 0);
        }
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
        if !self.batches.is_empty() {
            return self.build_object_columnar(level, input_set_hash, part_index);
        }
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
        // Stream-level-only columns. A key that is resource- or scope-level
        // across the whole object and per-record on no record gets no column
        // from the per-record loop above, yet `service.name` on the resource is
        // the ordinary OTLP shape. Two kinds of key get one anyway:
        //
        // - Indexed keys (ADR-0049 amendment), so `indexed_term_columns` can
        //   key their merged-view postings by a column.
        // - Numeric keys (I64, F64, Bool), so `stat_winner_columns` has a
        //   column to key their NumStat by. Without it a name only ever
        //   resolved off the stream layer gets no column, drops out of
        //   `numstat_names`, and so gets no stat anywhere -- and a query
        //   ranging on the declared column then scans every block instead of
        //   pruning on bounds the writer could have written. Restricting this
        //   to numeric types is what keeps it cheap: a string resource key
        //   feeds no stat, so it still only takes a column when it is indexed.
        //
        // The column is a POSTINGS/stat KEY, not a materialized per-record
        // value: no row writes a value to it (`resolve_row` populates `columns`
        // from `r.attrs` only), so it stays all-null in every value page --
        // `stage_column` in fact emits no page at all for a wholly absent
        // column, so it costs zero BLOCKS bytes. The reader's `equals` on a
        // `FieldSel::Attr` therefore still reads only the per-record layer and
        // returns false for a value that lives solely in the resource blob --
        // the exact channel stays a strict per-record predicate, distinct from
        // the prune channel that probes these postings
        // (docs/log-segment-format.md "FIELD_DIR"). These count against the same
        // `max_dynamic_columns` budget as any dynamic column: they occupy a real
        // FIELD_DIR entry. A key that cannot fit degrades to bloom + exact scan
        // for postings, and to no stat for NumStat -- an absent stat is "no
        // information" and prunes nothing, always legal (decision 5). Decoding
        // `stream_attrs` can fail on a corrupt blob; that is propagated rather
        // than silently under-indexing.
        for blob in streams.values() {
            for (k, v) in stream_attr_pairs(blob)? {
                let (ty, _) = resolve_value(&v);
                let eligible = indexed_names.contains(k.as_str())
                    || matches!(ty, FieldType::I64 | FieldType::F64 | FieldType::Bool);
                if eligible {
                    distinct.insert((k, ty.to_u8()));
                }
            }
        }
        // Both budget numbers are known here without a second pass over the
        // records: `distinct` holds every distinct `(name, type)` pair, and the
        // loop below gives the first `max_dynamic_columns` of them a column. The
        // used count is what `columns` ends with; the overflow is the rest, the
        // pairs that fold into `attrs_raw` (docs/adrs/0100 decision 1).
        let distinct_total = distinct.len();
        let mut column_of: ColumnIndex = HashMap::new();
        let mut columns: Vec<(String, FieldType, u32)> = Vec::new();
        for (idx, (name, ty_byte)) in distinct.into_iter().enumerate() {
            if idx >= self.cfg.max_dynamic_columns {
                break;
            }
            let column_id = FIRST_DYNAMIC_COL + idx as u32;
            let ty = FieldType::from_u8(ty_byte).unwrap_or(FieldType::Bytes);
            column_of
                .entry(ty_byte)
                .or_default()
                .insert(name.clone(), column_id);
            columns.push((name, ty, column_id));
        }
        let dynamic_columns_used = columns.len() as u32;
        let dynamic_columns_overflowed = distinct_total.saturating_sub(columns.len()) as u32;
        let ty_of_column: HashMap<u32, FieldType> =
            columns.iter().map(|(_, ty, id)| (*id, *ty)).collect();

        // Attribute names carrying at least one NumStat-eligible column
        // (I64/F64/Bool: the types `write_block` writes a stat for). Every row's
        // merged-view value for these names has to be resolved so a block's
        // stats bound the merged view rather than the raw columnar occurrences
        // (ADR-0095 decision 1). Scoped to these names, not every attribute in
        // the record: the winner maps are built per row in the hot
        // `resolve_row` loop, and this set is small (one name per numeric
        // column, typically a handful) where "every attribute" is unbounded.
        //
        // Derived from `columns`, so a name in this set always has a column
        // already. That includes a name that appears *only* at stream level:
        // the allocation loop above gives every numeric-typed stream-level key
        // a column precisely so it lands here and gets a stat, the same way an
        // indexed one gets a column to key its postings by. A name still drops
        // out when it overflows `max_dynamic_columns`; then no stat is written
        // for it anywhere, and an absent stat is "no information" (no prune),
        // not an under-bound.
        let numstat_names: std::collections::HashSet<&str> = columns
            .iter()
            .filter(|(_, ty, _)| matches!(ty, FieldType::I64 | FieldType::F64 | FieldType::Bool))
            .map(|(name, _, _)| name.as_str())
            .collect();

        // Columns whose name is in the caller's indexed-field list get postings.
        // A name past the dynamic-column budget above has no matching column_id
        // here, so it has no postings -- legal degradation, not an error.
        let indexed_column_ids: BTreeSet<u32> = columns
            .iter()
            .filter(|(name, _, _)| indexed_names.contains(name.as_str()))
            .map(|(_, _, cid)| *cid)
            .collect();

        // Each stream's resource and scope pairs, restricted to the tracked names
        // (`indexed_names` union `numstat_names`) and resolved to
        // `(type byte, value)`. This is the seed of every record's merged-view
        // resolution ([`resolved_tracked_values`]), which both POSTINGS and the
        // SKIP_IDX NumStats are projections of. Decoded once per stream, not once
        // per record: a stream's blob is byte-identical across its records (the
        // check above refuses an object where it is not), so the per-record decode
        // would repeat the same work. Skipped entirely when nothing is tracked.
        // A corrupt blob fails the write rather than silently dropping
        // stream-level values, since an under-populated posting list or an
        // under-bounded stat would prune a block a merged-view query needs.
        let mut stream_tracked: HashMap<LogStreamId, TrackedValues> = HashMap::new();
        if !indexed_names.is_empty() || !numstat_names.is_empty() {
            for (id, blob) in &streams {
                let mut pairs: TrackedValues = Vec::new();
                for (k, v) in stream_attr_pairs(blob)? {
                    if indexed_names.contains(k.as_str()) || numstat_names.contains(k.as_str()) {
                        let (ty, cv) = resolve_value(&v);
                        pairs.push((k, (ty.to_u8(), cv)));
                    }
                }
                stream_tracked.insert(*id, pairs);
            }
        }

        // Resolve every record into storage form, then sort by (stream_ref, ts).
        // `resolve_row` also computes each row's merged-view POSTINGS terms and
        // its per-name NumStat winners, both read off the one resolved merged
        // view seeded from `stream_tracked`.
        let mut rows: Vec<ResolvedRow> = self
            .records
            .iter()
            .map(|r| {
                resolve_row(
                    r,
                    &ref_of,
                    &column_of,
                    &indexed_names,
                    &numstat_names,
                    &stream_tracked,
                )
            })
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
            // Columns this block gets a plan for, which is a superset of
            // `present_cols`: every column some row of this block resolves a
            // NumStat winner for is planned too, even when no row of the block
            // carries a per-record occurrence of it (a name every row here
            // resolves off its resource or scope). Without that, such a block
            // gets no stat at all for the column, and `merge_stats` folds only
            // children that carry one -- so the level-1 group summary would
            // bound the group over its other blocks alone and silently drop
            // this block's real resolved values from min/max/null_count
            // (ADR-0095 decision 2: a stat bounds what a reader materializes,
            // and level 1 has to bound the whole group, not the part of it that
            // happened to have record-level occurrences).
            //
            // A stat-only column adds no bytes to the block: `write_block`
            // stages an all-absent column's pages through `stage_column`, which
            // returns before pushing anything when no row is present. It only
            // adds the (null-only or stream-resolved) NumStat the plan asks
            // for. The FIELD_DIR accounting below deliberately stays on the
            // narrower `present_cols`: a column with no value page must not
            // count as a block the column appears in.
            let mut plan_cols: BTreeSet<u32> = present_cols.clone();
            for row in block_rows {
                for (cid, _) in &row.stat_winners {
                    plan_cols.insert(*cid);
                }
            }
            let plans: Vec<ColumnPlan> = plan_cols
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
                dynamic_columns_used,
                dynamic_columns_overflowed,
            },
        ))
    }

    /// The columnar counterpart of [`RlogWriter::build_object`] (ADR-0109). It
    /// consumes the accumulated [`ColumnarLogBatch`]es and produces the exact
    /// same object bytes and [`WriteStats`] the row path produces for the same
    /// records, with two structural differences the ADR requires:
    ///
    /// - Each dynamic attribute column is resolved to its column id **once**
    ///   (per `(name, type)`, not per row): the batch already groups values by
    ///   column, so the hot per-attribute `column_of` probe of the row path is
    ///   gone.
    /// - The block value pages are staged from contiguous per-column value
    ///   arrays through [`write_block_columnar`], so the quadratic `row_column`
    ///   gather the ADR exists to delete never runs.
    ///
    /// Every ordering-, budget-, overflow-, merged-view-, and stats-affecting
    /// rule is reproduced from the same helpers the row path uses
    /// (`record_level_winners`, `resolved_tracked_values`,
    /// `indexed_term_columns`, `stat_winner_columns`, `canonical_attr_bytes`),
    /// which is what makes the output byte-identical (decision 7). The
    /// merged-view and `attrs_raw` derivations stay per row exactly as the row
    /// path's do; only the value gather and the column-id probe change shape.
    fn build_object_columnar(
        self,
        level: u32,
        input_set_hash: Vec<u8>,
        part_index: u32,
    ) -> Result<(Vec<u8>, WriteStats), LogSegError> {
        let batches = &self.batches;
        let total_rows: usize = batches.iter().map(|b| b.num_rows).sum();
        if total_rows == 0 {
            return Err(LogSegError::LimitExceeded("empty object".into()));
        }
        let mut bases = Vec::with_capacity(batches.len());
        {
            let mut acc = 0usize;
            for b in batches {
                bases.push(acc);
                acc += b.num_rows;
            }
        }

        // Stream directory: distinct ids across every batch, id-sorted, each
        // with the blob from the first batch that carried it; a second batch
        // claiming the same id with different bytes fails the whole object.
        let mut streams: BTreeMap<LogStreamId, &[u8]> = BTreeMap::new();
        for b in batches {
            for (id, blob) in b.stream_ids.iter().zip(b.stream_attrs.iter()) {
                match streams.entry(*id) {
                    Entry::Vacant(slot) => {
                        slot.insert(blob.as_slice());
                    }
                    Entry::Occupied(slot) => {
                        if *slot.get() != blob.as_slice() {
                            return Err(LogSegError::InconsistentStreamAttrs(format!(
                                "stream {} carries two different stream_attrs blobs ({} and {} bytes)",
                                id.to_hex(),
                                slot.get().len(),
                                blob.len(),
                            )));
                        }
                    }
                }
            }
        }
        let sorted_ids: Vec<LogStreamId> = streams.keys().copied().collect();
        let mut ref_of: HashMap<LogStreamId, u32> = HashMap::with_capacity(sorted_ids.len());
        for (i, id) in sorted_ids.iter().enumerate() {
            ref_of.insert(*id, i as u32);
        }

        let indexed_names: std::collections::HashSet<&str> =
            self.indexed_fields.iter().map(String::as_str).collect();

        // Dynamic column assignment: distinct (name, type) over every batch's
        // dynamic columns (each is one (name, type) pair), plus the
        // stream-level-only columns build_object grants, sorted by
        // (name bytes, type) and truncated at max_dynamic_columns.
        let mut distinct: BTreeSet<(String, u8)> = BTreeSet::new();
        for b in batches {
            for c in &b.dyn_columns {
                distinct.insert((c.name.clone(), c.field_type.to_u8()));
            }
        }
        for blob in streams.values() {
            for (k, v) in stream_attr_pairs(blob)? {
                let (ty, _) = resolve_value(&v);
                let eligible = indexed_names.contains(k.as_str())
                    || matches!(ty, FieldType::I64 | FieldType::F64 | FieldType::Bool);
                if eligible {
                    distinct.insert((k, ty.to_u8()));
                }
            }
        }
        let distinct_total = distinct.len();
        let mut column_of: ColumnIndex = HashMap::new();
        let mut columns: Vec<(String, FieldType, u32)> = Vec::new();
        for (idx, (name, ty_byte)) in distinct.into_iter().enumerate() {
            if idx >= self.cfg.max_dynamic_columns {
                break;
            }
            let column_id = FIRST_DYNAMIC_COL + idx as u32;
            let ty = FieldType::from_u8(ty_byte).unwrap_or(FieldType::Bytes);
            column_of
                .entry(ty_byte)
                .or_default()
                .insert(name.clone(), column_id);
            columns.push((name, ty, column_id));
        }
        let dynamic_columns_used = columns.len() as u32;
        let dynamic_columns_overflowed = distinct_total.saturating_sub(columns.len()) as u32;
        let ty_of_column: HashMap<u32, FieldType> =
            columns.iter().map(|(_, ty, id)| (*id, *ty)).collect();
        let plan_of_cid: HashMap<u32, usize> = columns
            .iter()
            .enumerate()
            .map(|(i, (_, _, cid))| (*cid, i))
            .collect();
        let num_plans = columns.len();

        let numstat_names: std::collections::HashSet<&str> = columns
            .iter()
            .filter(|(_, ty, _)| matches!(ty, FieldType::I64 | FieldType::F64 | FieldType::Bool))
            .map(|(name, _, _)| name.as_str())
            .collect();
        let indexed_column_ids: BTreeSet<u32> = columns
            .iter()
            .filter(|(name, _, _)| indexed_names.contains(name.as_str()))
            .map(|(_, _, cid)| *cid)
            .collect();

        let mut stream_tracked: HashMap<LogStreamId, TrackedValues> = HashMap::new();
        if !indexed_names.is_empty() || !numstat_names.is_empty() {
            for (id, blob) in &streams {
                let mut pairs: TrackedValues = Vec::new();
                for (k, v) in stream_attr_pairs(blob)? {
                    if indexed_names.contains(k.as_str()) || numstat_names.contains(k.as_str()) {
                        let (ty, cv) = resolve_value(&v);
                        pairs.push((k, (ty.to_u8(), cv)));
                    }
                }
                stream_tracked.insert(*id, pairs);
            }
        }

        // Per-global-row fixed columns and derived data, built column by column
        // (no per-row struct, no per-attribute column_of probe).
        let mut g_ts: Vec<i64> = Vec::with_capacity(total_rows);
        let mut g_obs: Vec<i64> = Vec::with_capacity(total_rows);
        let mut g_sev: Vec<u8> = Vec::with_capacity(total_rows);
        let mut g_flags: Vec<u32> = Vec::with_capacity(total_rows);
        let mut g_sevtext: Vec<&[u8]> = Vec::with_capacity(total_rows);
        let mut g_body: Vec<&[u8]> = Vec::with_capacity(total_rows);
        let mut g_trace: Vec<Option<&[u8]>> = Vec::with_capacity(total_rows);
        let mut g_span: Vec<Option<&[u8]>> = Vec::with_capacity(total_rows);
        let mut g_stream_id: Vec<LogStreamId> = Vec::with_capacity(total_rows);
        let mut g_stream_ref: Vec<u32> = Vec::with_capacity(total_rows);

        for b in batches {
            let mut trace_slot = 0usize;
            let mut span_slot = 0usize;
            for row in 0..b.num_rows {
                g_ts.push(b.ts_ns[row]);
                g_obs.push(b.observed_ts_ns[row]);
                g_sev.push(b.severity_num[row]);
                g_flags.push(b.flags[row]);
                g_sevtext.push(b.severity_text.get(row));
                g_body.push(b.body.get(row));
                if b.trace_id_validity.get(row) {
                    g_trace.push(Some(b.trace_id_at(trace_slot)));
                    trace_slot += 1;
                } else {
                    g_trace.push(None);
                }
                if b.span_id_validity.get(row) {
                    g_span.push(Some(b.span_id_at(span_slot)));
                    span_slot += 1;
                } else {
                    g_span.push(None);
                }
                let sid = b.stream_ids[b.stream_refs[row] as usize];
                g_stream_id.push(sid);
                g_stream_ref.push(ref_of[&sid]);
            }
        }

        // Per-plan, per-row value and stat arrays: contiguous per column, so a
        // block's value page is an O(1) index per row, never a gather.
        let mut col_values: Vec<Vec<Option<ColumnValue>>> = vec![vec![None; total_rows]; num_plans];
        let mut col_stat: Vec<Vec<Option<ColumnValue>>> = vec![vec![None; total_rows]; num_plans];

        // Per-row in-budget columnar occurrences (row.columns), overflow
        // attributes (attrs_raw source), and the tracked-name occurrences the
        // merged view is resolved from.
        let mut g_cols: Vec<Vec<(u32, ColumnValue)>> = vec![Vec::new(); total_rows];
        let mut g_overflow: Vec<Vec<(String, AttrValue)>> = vec![Vec::new(); total_rows];
        let mut tracked_cols: Vec<BTreeMap<String, Vec<(u8, ColumnValue)>>> =
            vec![BTreeMap::new(); total_rows];
        let mut tracked_overflow: Vec<BTreeMap<String, Vec<AttrValue>>> =
            vec![BTreeMap::new(); total_rows];

        for (bi, b) in batches.iter().enumerate() {
            let base = bases[bi];
            for c in &b.dyn_columns {
                let ty_byte = c.field_type.to_u8();
                let cid_opt = column_lookup(&column_of, &c.name, ty_byte);
                let tracked = indexed_names.contains(c.name.as_str())
                    || numstat_names.contains(c.name.as_str());
                let mut slot = 0usize;
                for row in 0..b.num_rows {
                    if !c.validity.get(row) {
                        continue;
                    }
                    let cell = &c.cells[slot];
                    slot += 1;
                    let grow = base + row;
                    let (ty, cv) = resolve_value(cell);
                    match cid_opt {
                        Some(cid) => {
                            let plan_idx = plan_of_cid[&cid];
                            col_values[plan_idx][grow] = Some(cv.clone());
                            g_cols[grow].push((cid, cv.clone()));
                            if tracked {
                                tracked_cols[grow]
                                    .entry(c.name.clone())
                                    .or_default()
                                    .push((ty.to_u8(), cv));
                            }
                        }
                        None => {
                            g_overflow[grow].push((c.name.clone(), cell.clone()));
                            if tracked {
                                tracked_overflow[grow]
                                    .entry(c.name.clone())
                                    .or_default()
                                    .push(cell.clone());
                            }
                        }
                    }
                }
            }
            for (row, extras) in b.residual_attrs.iter().enumerate() {
                let grow = base + row;
                for (k, v) in extras {
                    g_overflow[grow].push((k.clone(), v.clone()));
                    let tracked =
                        indexed_names.contains(k.as_str()) || numstat_names.contains(k.as_str());
                    if tracked {
                        tracked_overflow[grow]
                            .entry(k.clone())
                            .or_default()
                            .push(v.clone());
                    }
                }
            }
        }

        // Finish each row: sort its columnar occurrences by column id (matching
        // the row path's BTreeMap collection), resolve the merged view, and
        // project it into POSTINGS terms, NumStat winners, and attrs_raw.
        let mut g_attrs_raw: Vec<Option<Vec<u8>>> = vec![None; total_rows];
        let mut g_indexed_terms: Vec<Vec<(u32, ColumnValue)>> = vec![Vec::new(); total_rows];
        let mut g_stat_winners: Vec<Vec<(u32, ColumnValue)>> = vec![Vec::new(); total_rows];
        for grow in 0..total_rows {
            g_cols[grow].sort_by_key(|(cid, _)| *cid);
            if !g_overflow[grow].is_empty() {
                g_attrs_raw[grow] = Some(canonical_attr_bytes(&g_overflow[grow]));
            }
            let winners = record_level_winners(&tracked_cols[grow], &tracked_overflow[grow]);
            let seed = stream_tracked
                .get(&g_stream_id[grow])
                .map_or(&[][..], Vec::as_slice);
            let resolved = resolved_tracked_values(seed, &winners);
            g_indexed_terms[grow] = indexed_term_columns(&resolved, &indexed_names, &column_of);
            let stat_winners = stat_winner_columns(&resolved, &numstat_names, &column_of);
            for (cid, cv) in &stat_winners {
                col_stat[plan_of_cid[cid]][grow] = Some(cv.clone());
            }
            g_stat_winners[grow] = stat_winners;
        }

        // Sort rows by (stream_ref, ts) exactly as the row path sorts
        // ResolvedRows; `sort_by` is stable, so ties keep the appended order.
        let mut perm: Vec<usize> = (0..total_rows).collect();
        perm.sort_by(|&a, &b| {
            g_stream_ref[a]
                .cmp(&g_stream_ref[b])
                .then_with(|| g_ts[a].cmp(&g_ts[b]))
        });

        // Chunk into blocks, reproducing chunk_blocks/row_estimate column-wise.
        let row_estimate = |grow: usize| -> usize {
            let mut est = 40usize;
            est += g_body[grow].len();
            est += g_sevtext[grow].len();
            if let Some(raw) = &g_attrs_raw[grow] {
                est += raw.len();
            }
            for (_, v) in &g_cols[grow] {
                est += match v {
                    ColumnValue::Str(b) | ColumnValue::Bytes(b) => b.len() + 2,
                    _ => 8,
                };
            }
            est
        };
        let mut spans: Vec<std::ops::Range<usize>> = Vec::new();
        {
            let mut start = 0usize;
            let mut bytes = 0usize;
            for (i, &grow) in perm.iter().enumerate() {
                let est = row_estimate(grow);
                let count = i - start;
                if count > 0
                    && (count >= self.cfg.block_target_records
                        || bytes + est > self.cfg.block_max_bytes)
                {
                    spans.push(start..i);
                    start = i;
                    bytes = 0;
                }
                bytes += est;
            }
            if start < perm.len() {
                spans.push(start..perm.len());
            }
        }

        // Per-column present/blocks accounting for FIELD_DIR, per-stream block
        // range, and the streamed section buffers.
        let mut col_present: HashMap<u32, u64> = HashMap::new();
        let mut col_blocks: HashMap<u32, u32> = HashMap::new();
        let mut first_blk: HashMap<u32, u32> = HashMap::new();
        let mut last_blk: HashMap<u32, u32> = HashMap::new();
        let mut blocks_bytes: Vec<u8> = Vec::new();
        let mut l0: Vec<Level0Entry> = Vec::new();
        let mut bloom_entries: Vec<Vec<u8>> = Vec::new();
        let mut postings_terms: BTreeMap<u32, BTreeMap<Vec<u8>, BTreeSet<u32>>> = BTreeMap::new();
        let mut postings_capped: BTreeSet<u32> = BTreeSet::new();
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        let mut min_obs = i64::MAX;
        let mut max_obs = i64::MIN;

        for (blk_idx, span) in spans.iter().enumerate() {
            let block_rows = &perm[span.clone()];
            let blk_idx_u32 = blk_idx as u32;

            let mut present_cols: BTreeSet<u32> = BTreeSet::new();
            for &g in block_rows {
                for (cid, _) in &g_cols[g] {
                    present_cols.insert(*cid);
                }
            }
            let mut plan_cols: BTreeSet<u32> = present_cols.clone();
            for &g in block_rows {
                for (cid, _) in &g_stat_winners[g] {
                    plan_cols.insert(*cid);
                }
            }
            let plans: Vec<ColumnPlan> = plan_cols
                .iter()
                .map(|cid| ColumnPlan {
                    column_id: *cid,
                    ty: ty_of_column[cid],
                })
                .collect();

            // Column-major block inputs, gathered by the sort permutation.
            let ts_v: Vec<i64> = block_rows.iter().map(|&g| g_ts[g]).collect();
            let obs_v: Vec<i64> = block_rows.iter().map(|&g| g_obs[g]).collect();
            let sref_v: Vec<u32> = block_rows.iter().map(|&g| g_stream_ref[g]).collect();
            let sev_v: Vec<u8> = block_rows.iter().map(|&g| g_sev[g]).collect();
            let flags_v: Vec<u32> = block_rows.iter().map(|&g| g_flags[g]).collect();
            let sevtext_v: Vec<&[u8]> = block_rows.iter().map(|&g| g_sevtext[g]).collect();
            let body_v: Vec<&[u8]> = block_rows.iter().map(|&g| g_body[g]).collect();
            let trace_present_v: Vec<bool> =
                block_rows.iter().map(|&g| g_trace[g].is_some()).collect();
            let trace_vals_v: Vec<&[u8]> = block_rows.iter().filter_map(|&g| g_trace[g]).collect();
            let span_present_v: Vec<bool> =
                block_rows.iter().map(|&g| g_span[g].is_some()).collect();
            let span_vals_v: Vec<&[u8]> = block_rows.iter().filter_map(|&g| g_span[g]).collect();
            let raw_present_v: Vec<bool> = block_rows
                .iter()
                .map(|&g| g_attrs_raw[g].is_some())
                .collect();
            let raw_vals_v: Vec<&[u8]> = block_rows
                .iter()
                .filter_map(|&g| g_attrs_raw[g].as_deref())
                .collect();
            let values_v: Vec<Vec<Option<ColumnValue>>> = plans
                .iter()
                .map(|p| {
                    let plan_idx = plan_of_cid[&p.column_id];
                    block_rows
                        .iter()
                        .map(|&g| col_values[plan_idx][g].clone())
                        .collect()
                })
                .collect();
            let stat_v: Vec<Vec<Option<ColumnValue>>> = plans
                .iter()
                .map(|p| {
                    let plan_idx = plan_of_cid[&p.column_id];
                    block_rows
                        .iter()
                        .map(|&g| col_stat[plan_idx][g].clone())
                        .collect()
                })
                .collect();

            let input = ColumnarBlockInput {
                n: block_rows.len(),
                ts: &ts_v,
                observed_ts: &obs_v,
                stream_ref: &sref_v,
                severity_num: &sev_v,
                flags: &flags_v,
                severity_text: &sevtext_v,
                body: &body_v,
                trace_present: &trace_present_v,
                trace_vals: &trace_vals_v,
                span_present: &span_present_v,
                span_vals: &span_vals_v,
                attrs_raw_present: &raw_present_v,
                attrs_raw_vals: &raw_vals_v,
                plans: &plans,
                values: &values_v,
                stat_values: &stat_v,
            };
            let out = write_block_columnar(&input, self.cfg.zstd_level)?;

            // Bloom over body, severity_text, and string columns; POSTINGS over
            // each row's merged-view indexed terms.
            let mut builder = BloomBuilder::new(self.cfg.bloom_seed);
            for &g in block_rows {
                insert_text(&mut builder, COL_BODY, g_body[g]);
                insert_text(&mut builder, COL_SEVERITY_TEXT, g_sevtext[g]);
                for (cid, v) in &g_cols[g] {
                    if let ColumnValue::Str(bytes) = v {
                        insert_text(&mut builder, *cid, bytes);
                    }
                }
                for (cid, v) in &g_indexed_terms[g] {
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

            for &g in block_rows {
                min_ts = min_ts.min(g_ts[g]);
                max_ts = max_ts.max(g_ts[g]);
                min_obs = min_obs.min(g_obs[g]);
                max_obs = max_obs.max(g_obs[g]);
                first_blk.entry(g_stream_ref[g]).or_insert(blk_idx_u32);
                last_blk.insert(g_stream_ref[g], blk_idx_u32);
            }
            for cid in &present_cols {
                *col_blocks.entry(*cid).or_insert(0) += 1;
            }
            for &g in block_rows {
                for (cid, _) in &g_cols[g] {
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
        let total_blocks = spans.len() as u32;
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

        // FIELD_DIR.
        let field_entries: Vec<FieldEntry> = columns
            .iter()
            .map(|(name, ty, cid)| {
                let present = col_present.get(cid).copied().unwrap_or(0);
                let null_count = (total_rows as u64).saturating_sub(present);
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

        let postings_capped_fields = postings_capped.len() as u32;
        let mut postings_fields: BTreeMap<u32, FieldTerms> = BTreeMap::new();
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
            record_count: total_rows as u64,
            block_count: u64::from(total_blocks),
            stream_count: sorted_ids.len() as u64,
            sections,
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
                dynamic_columns_used,
                dynamic_columns_overflowed,
            },
        ))
    }
}

/// Tracked attribute values as `(name, (type byte, value))`, in the order a
/// reader merges them. Used for one stream's contribution and for one record's
/// resolved view of it ([`resolved_tracked_values`]).
type TrackedValues = Vec<(String, (u8, ColumnValue))>;

/// Dynamic-column index: type byte to (attribute name to column id). Nesting the
/// map this way (rather than keying one map on an owned `(String, u8)`) lets the
/// hot per-attribute probe in [`resolve_row`] borrow the name as `&str` through
/// `String: Borrow<str>`, so a lookup allocates nothing and hashes the name once.
/// Keying a single `(String, u8)` map instead forces an owned key -- a String
/// clone and heap allocation -- on every probe, which a ClickBench load
/// (105 attributes per row over 2M rows, 200M+ probes) spent ~8.3% of all CPU on
/// (issue #570). The outer map has at most one entry per [`FieldType`] variant,
/// so its lookup is effectively free.
type ColumnIndex = HashMap<u8, HashMap<String, u32>>;

/// Resolves one `(name, type byte)` pair to its dynamic column id, borrowing the
/// name rather than cloning it. `None` when the pair took no in-budget column
/// (overflowed, or never given one). This is the read half of [`ColumnIndex`];
/// keep every probe going through it so no call site reconstructs an owned key.
fn column_lookup(column_of: &ColumnIndex, name: &str, ty_byte: u8) -> Option<u32> {
    column_of.get(&ty_byte).and_then(|m| m.get(name)).copied()
}

/// Resolves one record into storage form: dense stream ref, dynamic columns
/// split by type, overflow attributes canonicalized into `attrs_raw`, the
/// merged-view POSTINGS terms this record contributes
/// ([`indexed_term_columns`]), and its per-name NumStat winners
/// ([`stat_winner_columns`]).
///
/// The last two are two projections of one merged view
/// ([`resolved_tracked_values`]), not two independently derived answers: they
/// must not disagree about which value a reader resolves for a tracked name
/// (ADR-0095 decision 1).
fn resolve_row(
    r: &LogRecord,
    ref_of: &HashMap<LogStreamId, u32>,
    column_of: &ColumnIndex,
    indexed_names: &std::collections::HashSet<&str>,
    numstat_names: &std::collections::HashSet<&str>,
    stream_tracked: &HashMap<LogStreamId, TrackedValues>,
) -> ResolvedRow {
    let stream_ref = ref_of.get(&r.stream_id).copied().unwrap_or(0);
    let mut cols: BTreeMap<u32, ColumnValue> = BTreeMap::new();
    let mut overflow: Vec<(String, ravel_types::logstream::AttrValue)> = Vec::new();
    // For each *tracked* name this record carries -- indexed (POSTINGS) or
    // NumStat-eligible (ADR-0095 decision 1: `indexed_names` union
    // `numstat_names`) -- its own occurrences split the same way the read side
    // reconstructs them: the columnar ones (those that took a fresh
    // dynamic-column slot, one per distinct type, each with its type byte kept
    // alongside its value) and the overflow ones (any type, duplicates and
    // budget overflow alike). [`record_level_winners`] reduces these to the
    // single occurrence `rebuild_record` + `merged_attrs` report for the name
    // (docs/adrs/0049-rlog-postings.md amendment 2026-08-20). That is the record
    // layer only; [`resolved_tracked_values`] then lays it over the stream layer,
    // and both consumers below read that one merged answer -- there is no second
    // resolution rule.
    let mut tracked_cols: BTreeMap<String, Vec<(u8, ColumnValue)>> = BTreeMap::new();
    let mut tracked_overflow: BTreeMap<String, Vec<ravel_types::logstream::AttrValue>> =
        BTreeMap::new();
    for (k, v) in &r.attrs {
        let (ty, cv) = resolve_value(v);
        let tracked = indexed_names.contains(k.as_str()) || numstat_names.contains(k.as_str());
        match column_lookup(column_of, k, ty.to_u8()) {
            Some(cid) if !cols.contains_key(&cid) => {
                if tracked {
                    tracked_cols
                        .entry(k.clone())
                        .or_default()
                        .push((ty.to_u8(), cv.clone()));
                }
                cols.insert(cid, cv);
            }
            // Overflow column, or a duplicate (name,type) already columnar this
            // row: fold into attrs_raw so no value is lost.
            _ => {
                if tracked {
                    tracked_overflow
                        .entry(k.clone())
                        .or_default()
                        .push(v.clone());
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
    let winners = record_level_winners(&tracked_cols, &tracked_overflow);
    let resolved = resolved_tracked_values(
        stream_tracked
            .get(&r.stream_id)
            .map_or(&[][..], Vec::as_slice),
        &winners,
    );
    let indexed_terms = indexed_term_columns(&resolved, indexed_names, column_of);
    let stat_winners = stat_winner_columns(&resolved, numstat_names, column_of);
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
        indexed_terms,
        stat_winners,
    }
}

/// One record's merged view of every tracked name (`indexed_names` union
/// `numstat_names`, ADR-0095 decision 1), in the order and with the precedence
/// `ravel_sql::rlog_attrs::merged_attrs` produces: the stream layer first
/// (`stream_seed`, resource pairs then scope pairs, already restricted to the
/// tracked names by the caller), then each name's record-level winner
/// overriding in place or appending. Each entry carries the type byte its value
/// resolves to, so a record-level winner of a different type than the stream
/// value is keyed by the right column downstream.
///
/// This is the single resolution step both POSTINGS ([`indexed_term_columns`])
/// and the SKIP_IDX NumStats ([`stat_winner_columns`]) are projections of. They
/// used to derive their own answers, and diverged: the stats saw only the
/// record layer, so a name a record carried only on its resource or scope
/// contributed nothing to the stat even though a reader resolves the
/// stream-level value for it, and the stat then under-bounded its own column
/// (ADR-0095 decision 1, corrected). Adding a value here reaches both.
///
/// Precedence per layer, mirroring the read side exactly: a name absent from
/// the record keeps its stream-level value rather than being dropped, and the
/// record wins a collision. Duplicate names *within* the stream layer (the same
/// key on both the resource and the scope) are kept as the blob carries them,
/// exactly as `merged_attrs` keeps them; `find_attr` reads the first, which is
/// what [`stat_winner_columns`] resolves to.
///
/// A record-level winner is the occurrence `rebuild_record` + `merged_attrs`
/// will report, not a re-derived guess: see [`record_level_winners`] for the
/// exact two-tier order (docs/adrs/0049-rlog-postings.md amendment 2026-08-20,
/// closing issue #333).
fn resolved_tracked_values(
    stream_seed: &[(String, (u8, ColumnValue))],
    winners: &BTreeMap<String, (u8, ColumnValue)>,
) -> TrackedValues {
    let mut merged: TrackedValues = stream_seed.to_vec();
    for (name, winner) in winners {
        if let Some(slot) = merged.iter_mut().find(|(mk, _)| mk == name) {
            slot.1 = winner.clone();
        } else {
            merged.push((name.clone(), winner.clone()));
        }
    }
    merged
}

/// The NumStat contribution of one record: each NumStat-eligible name's
/// resolved merged-view value, keyed by the dynamic column that value's type
/// resolves to (ADR-0095 decision 2).
///
/// Read off [`resolved_tracked_values`], so a name the record carries only on
/// its resource or scope contributes the stream-level value a reader resolves
/// for it, and a record-level occurrence overrides that. Only the first entry
/// per name counts, matching `rlog_attrs::find_attr`, which is how a declared
/// typed column resolves the name.
///
/// A resolved value of a non-numeric type (a string or bytes occurrence that
/// outranked the numeric one) yields no entry at all, so the name's numeric
/// column gets none either and the row counts as a null there -- which is
/// exactly what a reader materializing that typed column produces for the row.
/// A value whose own type has no in-budget column likewise yields no entry,
/// again matching the read side (no column, no value).
fn stat_winner_columns(
    resolved: &[(String, (u8, ColumnValue))],
    numstat_names: &std::collections::HashSet<&str>,
    column_of: &ColumnIndex,
) -> Vec<(u32, ColumnValue)> {
    let mut out: Vec<(u32, ColumnValue)> = Vec::new();
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for (name, (ty_byte, cv)) in resolved {
        if !numstat_names.contains(name.as_str()) || !seen.insert(name.as_str()) {
            continue;
        }
        if !matches!(
            FieldType::from_u8(*ty_byte),
            Some(FieldType::I64 | FieldType::F64 | FieldType::Bool)
        ) {
            continue;
        }
        if let Some(cid) = column_lookup(column_of, name, *ty_byte) {
            out.push((cid, cv.clone()));
        }
    }
    out
}

/// The merged-view POSTINGS terms one record contributes: each indexed field's
/// resolved merged-view value(s) from [`resolved_tracked_values`], keyed by the
/// dynamic column that value's type resolves to. That view is exactly what
/// `ravel_sql::rlog_attrs::merged_attrs` computes for SQL's `attrs` column,
/// reproduced in this crate so the writer does not depend on ravel-sql
/// (docs/adrs/0049-rlog-postings.md amendment 2026-08-03).
///
/// `resolved` covers every tracked name (indexed and NumStat-eligible alike);
/// only the indexed ones may key a posting, so the rest are skipped here.
/// Letting a NumStat-only name through would write postings for a column the
/// caller never asked to index, which the accumulation loop in `build_object`
/// (no `indexed_column_ids` re-check) relies on not happening.
///
/// Every entry is emitted, including a name the resource and the scope both
/// carry: the term set a posting list holds may be a superset of what
/// `find_attr` resolves, which costs precision and never soundness (an extra
/// term keeps a block, it cannot drop one).
///
/// A field with no matching dynamic column contributes nothing. The writer
/// gives an indexed field a column even when it appears only at stream
/// level, so a resource-only indexed key normally does have a column to
/// key a posting by; a key stays column-less only when it overflowed the
/// dynamic-column budget or is not in the indexed-field list. Absence is legal;
/// no posting for a field means no pruning on it, never a wrong prune.
fn indexed_term_columns(
    resolved: &[(String, (u8, ColumnValue))],
    indexed_names: &std::collections::HashSet<&str>,
    column_of: &ColumnIndex,
) -> Vec<(u32, ColumnValue)> {
    let mut out = Vec::with_capacity(resolved.len());
    for (name, (ty_byte, cv)) in resolved {
        if !indexed_names.contains(name.as_str()) {
            continue;
        }
        if let Some(cid) = column_lookup(column_of, name, *ty_byte) {
            out.push((cid, cv.clone()));
        }
    }
    out
}

/// The record-level winning occurrence of each tracked key this record carries,
/// keyed by name and carrying the winner's `(type byte, value)`.
///
/// "Tracked" is `indexed_names` union `numstat_names` (ADR-0095 decision 1):
/// POSTINGS and the SKIP_IDX NumStats consume the same answer from this one
/// function, so the two sections cannot disagree about which occurrence of a
/// duplicated key a reader will see.
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
    tracked_cols: &BTreeMap<String, Vec<(u8, ColumnValue)>>,
    tracked_overflow: &BTreeMap<String, Vec<ravel_types::logstream::AttrValue>>,
) -> BTreeMap<String, (u8, ColumnValue)> {
    let mut names: BTreeSet<&String> = BTreeSet::new();
    names.extend(tracked_cols.keys());
    names.extend(tracked_overflow.keys());

    let mut out: BTreeMap<String, (u8, ColumnValue)> = BTreeMap::new();
    for name in names {
        let mut combined: Vec<(u8, ColumnValue)> = Vec::new();
        // Columnar occurrences first, ascending by type byte (FIELD_DIR's
        // (name, type) sort key restricted to this one name).
        if let Some(cols) = tracked_cols.get(name) {
            let mut cols = cols.clone();
            cols.sort_by_key(|(ty_byte, _)| *ty_byte);
            combined.extend(cols);
        }
        // Overflow occurrences next, ascending by the canonical encoding of a
        // one-entry value set. They all share this name, so this orders by
        // encoded value bytes exactly as `attrs_raw` stores (and the reader
        // decodes) them.
        if let Some(over) = tracked_overflow.get(name) {
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
        assert!(fd.column("x", FieldType::Str).is_some());
        assert!(fd.column("x", FieldType::I64).is_some());
        // Plus one: `base_record`'s stream blob carries `lib` as a scope-level
        // I64, and a numeric stream-level name takes a column of its own so its
        // NumStat has an id to be keyed by (ADR-0095). No record writes a value
        // to it.
        assert!(fd.column("lib", FieldType::I64).is_some());
        assert_eq!(fd.len(), 3);
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

    /// Decodes the SKIP_IDX of a written object through the reader's own
    /// section path.
    fn read_skip_index(obj: &[u8]) -> SkipIndex {
        let cfg = RlogConfig::default();
        let footer = open(obj).expect("open");
        let desc = footer.section(kind::SKIP_IDX).expect("skip_idx");
        let raw = read_section(obj, desc, &cfg).expect("read section");
        SkipIndex::decode(&raw, 1 << 20).expect("decode")
    }

    /// The whole-object form of `block::tests::numstat_reflects_cross_type_winner`
    /// (ADR-0095): the writer, not a hand-built `ResolvedRow`, must compute the
    /// winners the stats fold in.
    ///
    /// Record A carries `dur` three times: `I64(5)` and `Bool(true)` take the
    /// two columnar slots for `(dur, i64)` and `(dur, bool)`, and the duplicate
    /// `Bool(false)` spills into `attrs_raw`. The read side resolves that to one
    /// value per name -- columnar occurrences by ascending type byte (i64 = 2,
    /// bool = 4), then overflow, last wins -- so A's `dur` is `Bool(false)`.
    /// Record B carries `dur` once, as `I64(70)`.
    ///
    /// So the i64 stat must bound `{70}` and count A as a null (its winner is a
    /// bool, and a reader materializing a declared `dur: i64` column produces
    /// NULL for A), and the bool stat must bound `{false}` -- the *overflow*
    /// occurrence, not the `true` sitting in the bool value page -- and count B
    /// as a null.
    #[test]
    fn numstat_reflects_cross_type_winner_end_to_end() {
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        let mut a = base_record(0, 1);
        a.attrs = vec![
            ("dur".into(), AttrValue::I64(5)),
            ("dur".into(), AttrValue::Bool(true)),
            ("dur".into(), AttrValue::Bool(false)),
        ];
        let mut b = base_record(0, 2);
        b.attrs = vec![("dur".into(), AttrValue::I64(70))];
        w.push(a).expect("push");
        w.push(b).expect("push");
        let obj = w.finish().expect("finish");

        let footer = open(&obj).expect("open");
        let fd = read_field_dir(&obj, &footer);
        let i64_cid = fd
            .column("dur", FieldType::I64)
            .expect("dur i64 column")
            .column_id;
        let bool_cid = fd
            .column("dur", FieldType::Bool)
            .expect("dur bool column")
            .column_id;

        let skip = read_skip_index(&obj);
        assert_eq!(skip.l0.len(), 1, "both records land in one block");
        let stat = |cid: u32| {
            *skip.l0[0]
                .stats
                .iter()
                .find(|s| s.column_id == cid)
                .unwrap_or_else(|| panic!("stat for column {cid}"))
        };

        let i = stat(i64_cid);
        assert_eq!(
            i.min_bits, 70i64 as u64,
            "record A's losing 5 must not count"
        );
        assert_eq!(i.max_bits, 70i64 as u64);
        assert_eq!(i.null_count, 1, "record A's i64 occurrence lost the merge");

        let b_stat = stat(bool_cid);
        assert_eq!(
            (b_stat.min_bits, b_stat.max_bits),
            (0, 0),
            "the winning bool is the overflow `false`, not the columnar `true`"
        );
        assert_eq!(b_stat.null_count, 1, "record B carries no bool winner");

        // The winner the stats claim is the one the reader actually resolves:
        // fold each rebuilt record's attributes last-wins by name, exactly as
        // `ravel_sql::rlog_attrs::merged_attrs` does over the same list.
        let reader = RlogReader::new(&obj, &RlogConfig::default()).expect("open reader");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        assert_eq!(rows.len(), 2);
        for row in &rows {
            let mut winner: Option<AttrValue> = None;
            for (k, v) in &row.attrs {
                if k == "dur" {
                    winner = Some(v.clone());
                }
            }
            let expected = if row.ts_ns == 1 {
                AttrValue::Bool(false)
            } else {
                AttrValue::I64(70)
            };
            assert_eq!(
                winner,
                Some(expected),
                "reader's merged winner for ts {}",
                row.ts_ns
            );
        }
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
        // counter reports zero (absence is always legal, decision 5). The
        // dynamic-column counters are not POSTINGS counters: `base_record`'s
        // stream blob carries `lib` as a scope-level i64, a numeric
        // stream-level key, which draws one dynamic column even with no
        // per-record attribute (docs/log-segment-format.md "FIELD_DIR"), so
        // `dynamic_columns_used` is 1 and nothing overflows.
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        w.push(base_record(0, 0)).expect("push");
        let (_obj, stats) = w.finish_with_stats().expect("finish");
        assert_eq!(
            stats,
            WriteStats {
                dynamic_columns_used: 1,
                ..WriteStats::default()
            }
        );
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

    /// Bit-pattern attribute equality (CLAUDE.md: float compares use
    /// `f64::to_bits`, never `==`, so -0.0 and a NaN payload are significant).
    fn attr_eq(a: &AttrValue, b: &AttrValue) -> bool {
        match (a, b) {
            (AttrValue::F64(x), AttrValue::F64(y)) => x.to_bits() == y.to_bits(),
            _ => a == b,
        }
    }

    /// The empty resource+scope blob: no stream-level key, so the dynamic-column
    /// budget below is spent purely on per-record attributes and `lib`/
    /// `service.name` from `attrs_blob` do not perturb the count.
    fn empty_stream_blob() -> Vec<u8> {
        stream_attrs_bytes(&[], "", "", &[])
    }

    #[test]
    fn dynamic_column_budget_reports_used_and_overflowed() {
        // Budget of 3 over 5 distinct (name, type) pairs. The records ARRIVE in
        // reverse-lexicographic order (e, d, c, b, a), so an arrival-order
        // selection would give columns to e, d, c; the writer selects
        // lexicographically, so a, b, c win and d, e overflow. The counts alone
        // (used 3, overflowed 2) hold under either rule -- the FIELD_DIR
        // membership check below is what pins the order.
        let cfg = RlogConfig {
            max_dynamic_columns: 3,
            ..RlogConfig::default()
        };
        let blob = empty_stream_blob();
        let mut w = RlogWriter::new(cfg, identity());
        for (i, name) in ["e", "d", "c", "b", "a"].iter().enumerate() {
            let mut r = base_record(0, i as i64);
            r.stream_attrs = blob.clone();
            r.attrs = vec![((*name).to_string(), AttrValue::Str(format!("v_{name}")))];
            w.push(r).expect("push");
        }
        let (obj, stats) = w.finish_with_stats().expect("finish");

        assert_eq!(stats.dynamic_columns_used, 3, "first 3 pairs get a column");
        assert_eq!(stats.dynamic_columns_overflowed, 2, "the other 2 overflow");

        let footer = open(&obj).expect("open");
        let fd = read_field_dir(&obj, &footer);
        assert_eq!(fd.len(), 3, "exactly the budget of columns");
        for name in ["a", "b", "c"] {
            assert!(
                fd.column(name, FieldType::Str).is_some(),
                "lexicographic winner {name} must have a column, not the arrival-order pick"
            );
        }
        for name in ["d", "e"] {
            assert!(
                fd.column(name, FieldType::Str).is_none(),
                "overflow key {name} must have no column"
            );
        }
    }

    #[test]
    fn budget_overflow_folds_to_attrs_raw_without_value_loss() {
        // Budget of 4 over 6 distinct names, mixed types including f64(-0.0)
        // (columnar) and f64(1.5)/f64(NaN payload) (overflow), so both the
        // columnar and the attrs_raw paths carry a float whose bits must
        // survive. Lexicographic order: a,b,c,d get columns; e,f overflow.
        let cfg = RlogConfig {
            max_dynamic_columns: 4,
            ..RlogConfig::default()
        };
        let blob = empty_stream_blob();
        // Three records, each carrying all six attributes with per-record
        // values, so every column holds a real per-row value and every record
        // has two overflow attributes.
        let nan = f64::from_bits(0x7ff8_0000_0000_0abc);
        let attrs_for = |rec: i64| -> Vec<(String, AttrValue)> {
            vec![
                ("a".into(), AttrValue::I64(rec)),
                ("b".into(), AttrValue::Str(format!("s{rec}"))),
                ("c".into(), AttrValue::Bool(rec % 2 == 0)),
                (
                    "d".into(),
                    AttrValue::F64(if rec == 0 { -0.0 } else { rec as f64 }),
                ),
                (
                    "e".into(),
                    AttrValue::F64(if rec == 2 { nan } else { 1.5 * rec as f64 }),
                ),
                (
                    "f".into(),
                    AttrValue::Bytes(vec![rec as u8, 0xff, rec as u8]),
                ),
            ]
        };

        let mut w = RlogWriter::new(cfg, identity());
        for rec in 0..3i64 {
            let mut r = base_record(0, rec);
            r.stream_attrs = blob.clone();
            r.attrs = attrs_for(rec);
            w.push(r).expect("push");
        }
        let (obj, stats) = w.finish_with_stats().expect("finish");
        assert_eq!(stats.dynamic_columns_used, 4);
        assert_eq!(stats.dynamic_columns_overflowed, 2, "e and f overflow");

        // e and f are not columnar; they can only be read back through attrs_raw.
        let footer = open(&obj).expect("open");
        let fd = read_field_dir(&obj, &footer);
        assert!(fd.column("e", FieldType::F64).is_none());
        assert!(fd.column("f", FieldType::Bytes).is_none());

        // Every attribute of every record reads back with its exact value and
        // type, columnar and overflowed alike -- nothing dropped or corrupted
        // at or across the budget boundary.
        let reader = RlogReader::new(&obj, &RlogConfig::default()).expect("open reader");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        assert_eq!(rows.len(), 3);
        for rec in 0..3i64 {
            let row = rows
                .iter()
                .find(|r| r.ts_ns == rec)
                .unwrap_or_else(|| panic!("record ts {rec} present"));
            let got: HashMap<&str, &AttrValue> =
                row.attrs.iter().map(|(k, v)| (k.as_str(), v)).collect();
            let want = attrs_for(rec);
            assert_eq!(
                got.len(),
                want.len(),
                "record {rec}: no attribute added or dropped"
            );
            for (name, value) in &want {
                let read = got
                    .get(name.as_str())
                    .unwrap_or_else(|| panic!("record {rec} attribute {name} read back"));
                assert!(
                    attr_eq(read, value),
                    "record {rec} attribute {name}: read {read:?} != written {value:?}"
                );
            }
        }
    }

    /// Differential/content-stability guard for the `column_lookup` refactor
    /// (issue #570): every per-record attribute is routed to its dynamic column
    /// purely by the lookup, so a wrong probe result would misfile or drop a
    /// value. Building a multi-row object over five distinct `(name, type)`
    /// columns and reading every value back exactly -- plus asserting the build
    /// is byte-stable -- proves the change is a pure performance change, not a
    /// behavior change. Reverting `column_lookup` to ignore the type byte (e.g.
    /// `column_of.values().find_map(|m| m.get(name)).copied()`) misroutes a
    /// value to another type's column and fails the read-back below.
    #[test]
    fn lookup_refactor_preserves_column_content() {
        let blob = empty_stream_blob();
        let nan = f64::from_bits(0x7ff8_0000_0000_0abc);
        // Five distinct names, one per type, each carried by every record so
        // each column holds a real per-row value. Read-back stays unambiguous
        // because no record carries a name twice.
        let attrs_for = |rec: i64| -> Vec<(String, AttrValue)> {
            vec![
                ("a_str".into(), AttrValue::Str(format!("s{rec}"))),
                ("b_i64".into(), AttrValue::I64(rec * 7 - 3)),
                ("c_bool".into(), AttrValue::Bool(rec % 2 == 0)),
                (
                    "d_f64".into(),
                    AttrValue::F64(if rec == 4 { nan } else { rec as f64 - 0.5 }),
                ),
                (
                    "e_bytes".into(),
                    AttrValue::Bytes(vec![rec as u8, 0x00, 0xff]),
                ),
            ]
        };
        let build = || {
            let mut w = RlogWriter::new(RlogConfig::default(), identity());
            for rec in 0..8i64 {
                let mut r = base_record(0, rec);
                r.stream_attrs = blob.clone();
                r.attrs = attrs_for(rec);
                w.push(r).expect("push");
            }
            w.finish().expect("finish")
        };
        let obj = build();
        assert_eq!(obj, build(), "identical input must stay byte-identical");

        let footer = open(&obj).expect("open");
        let fd = read_field_dir(&obj, &footer);
        assert_eq!(fd.len(), 5, "one column per distinct (name, type)");
        for (name, ty) in [
            ("a_str", FieldType::Str),
            ("b_i64", FieldType::I64),
            ("c_bool", FieldType::Bool),
            ("d_f64", FieldType::F64),
            ("e_bytes", FieldType::Bytes),
        ] {
            let col = fd
                .column(name, ty)
                .unwrap_or_else(|| panic!("{name} column"));
            // Every record populates every column: a value that failed to route
            // to its column would fold into `attrs_raw` and leave the column
            // all-null, so a null here catches a misrouting lookup even though
            // the reader's merged view would still surface the overflowed value.
            assert_eq!(col.null_count, 0, "{name} column populated by every row");
        }

        let reader = RlogReader::new(&obj, &RlogConfig::default()).expect("open reader");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        assert_eq!(rows.len(), 8);
        for rec in 0..8i64 {
            let row = rows
                .iter()
                .find(|r| r.ts_ns == rec)
                .unwrap_or_else(|| panic!("record ts {rec}"));
            let got: HashMap<&str, &AttrValue> =
                row.attrs.iter().map(|(k, v)| (k.as_str(), v)).collect();
            for (name, value) in &attrs_for(rec) {
                let read = got
                    .get(name.as_str())
                    .unwrap_or_else(|| panic!("record {rec} attribute {name} read back"));
                assert!(
                    attr_eq(read, value),
                    "record {rec} attribute {name}: read {read:?} != written {value:?}"
                );
            }
        }
    }

    /// The "new column first seen mid-object" path (issue #570): a `(name, type)`
    /// pair introduced only by a LATER row must get its own column, never alias
    /// onto an earlier column with a similar name or the same name under a
    /// different type.
    ///
    /// Rows 0..5 carry `("col", Str)`. Rows 5..10 carry `("col", I64)` (same name,
    /// new type, first seen late) and `("cola", Str)` (adjacent name, first seen
    /// late). All three must be distinct columns, and every row must read back
    /// the value it actually wrote. If `column_lookup` aliased `("col", I64)`
    /// onto `("col", Str)` or `("cola", Str)`, the late I64 rows would misroute
    /// and the read-back fails.
    #[test]
    fn later_row_first_seen_column_gets_fresh_index() {
        let blob = empty_stream_blob();
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        for rec in 0..10i64 {
            let mut r = base_record(0, rec);
            r.stream_attrs = blob.clone();
            if rec < 5 {
                r.attrs = vec![("col".into(), AttrValue::Str(format!("early{rec}")))];
            } else {
                r.attrs = vec![
                    ("col".into(), AttrValue::I64(rec * 11)),
                    ("cola".into(), AttrValue::Str(format!("late{rec}"))),
                ];
            }
            w.push(r).expect("push");
        }
        let obj = w.finish().expect("finish");

        let footer = open(&obj).expect("open");
        let fd = read_field_dir(&obj, &footer);
        let col_str = fd
            .column("col", FieldType::Str)
            .expect("(col, Str)")
            .column_id;
        let col_i64 = fd
            .column("col", FieldType::I64)
            .expect("(col, I64)")
            .column_id;
        let cola_str = fd
            .column("cola", FieldType::Str)
            .expect("(cola, Str)")
            .column_id;
        assert_eq!(fd.len(), 3, "three distinct columns");
        assert_ne!(
            col_str, col_i64,
            "same name, different type: distinct columns"
        );
        assert_ne!(col_str, cola_str, "adjacent names: distinct columns");
        assert_ne!(col_i64, cola_str);

        let reader = RlogReader::new(&obj, &RlogConfig::default()).expect("open reader");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        assert_eq!(rows.len(), 10);
        for rec in 0..10i64 {
            let row = rows
                .iter()
                .find(|r| r.ts_ns == rec)
                .unwrap_or_else(|| panic!("record ts {rec}"));
            let got: HashMap<&str, &AttrValue> =
                row.attrs.iter().map(|(k, v)| (k.as_str(), v)).collect();
            if rec < 5 {
                assert_eq!(
                    got.get("col"),
                    Some(&&AttrValue::Str(format!("early{rec}"))),
                    "early row {rec} keeps its Str col"
                );
                assert!(!got.contains_key("cola"), "cola absent from early rows");
            } else {
                assert_eq!(
                    got.get("col"),
                    Some(&&AttrValue::I64(rec * 11)),
                    "late row {rec} col is the newly-columned I64"
                );
                assert_eq!(
                    got.get("cola"),
                    Some(&&AttrValue::Str(format!("late{rec}"))),
                    "late row {rec} cola read back"
                );
            }
        }
    }

    /// No two distinct `(name, type)` pairs ever collapse onto one column under
    /// the nested-map lookup (issue #570), even for adjacent short names and for
    /// one name carried under three different type tags. Ten `("kN", Str)`
    /// columns plus `("k0", I64)` and `("k0", Bool)` must be twelve distinct
    /// columns, each reading back its own value.
    #[test]
    fn distinct_names_and_types_never_alias_to_one_column() {
        let blob = empty_stream_blob();
        let mut w = RlogWriter::new(RlogConfig::default(), identity());
        // Row r carries k0..k9 as Str; rows also carry k0 as I64 (even rows) and
        // k0 as Bool (odd rows), so k0 splits into three columns across rows.
        for rec in 0..6i64 {
            let mut r = base_record(0, rec);
            r.stream_attrs = blob.clone();
            let mut attrs: Vec<(String, AttrValue)> = (0..10)
                .map(|n| (format!("k{n}"), AttrValue::Str(format!("k{n}_v{rec}"))))
                .collect();
            if rec % 2 == 0 {
                attrs.push(("k0".into(), AttrValue::I64(rec)));
            } else {
                attrs.push(("k0".into(), AttrValue::Bool(true)));
            }
            r.attrs = attrs;
            w.push(r).expect("push");
        }
        let obj = w.finish().expect("finish");

        let footer = open(&obj).expect("open");
        let fd = read_field_dir(&obj, &footer);
        // 10 Str columns (k0..k9) + (k0, I64) + (k0, Bool) = 12.
        assert_eq!(
            fd.len(),
            12,
            "every distinct (name, type) gets its own column"
        );
        let mut ids: BTreeSet<u32> = BTreeSet::new();
        for n in 0..10 {
            let name = format!("k{n}");
            ids.insert(
                fd.column(&name, FieldType::Str)
                    .unwrap_or_else(|| panic!("({name}, Str)"))
                    .column_id,
            );
        }
        ids.insert(
            fd.column("k0", FieldType::I64)
                .expect("(k0, I64)")
                .column_id,
        );
        ids.insert(
            fd.column("k0", FieldType::Bool)
                .expect("(k0, Bool)")
                .column_id,
        );
        assert_eq!(ids.len(), 12, "no two columns share a column_id");

        // Every Str value reads back on every row: proves no adjacent-name probe
        // aliased onto a neighbour.
        let reader = RlogReader::new(&obj, &RlogConfig::default()).expect("open reader");
        let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        assert_eq!(rows.len(), 6);
        for rec in 0..6i64 {
            let row = rows
                .iter()
                .find(|r| r.ts_ns == rec)
                .unwrap_or_else(|| panic!("record ts {rec}"));
            for n in 0..10 {
                let name = format!("k{n}");
                let want = AttrValue::Str(format!("k{n}_v{rec}"));
                let found = row.attrs.iter().any(|(k, v)| k == &name && v == &want);
                assert!(
                    found,
                    "record {rec} attribute {name} read back as its own value"
                );
            }
        }
    }

    /// The ADR-0109 decision 7 acceptance anchor: the columnar build path
    /// (`push_columnar` + `finish_with_stats`) produces byte-identical object
    /// bytes and field-identical `WriteStats` to the row path
    /// (`push` + `finish_with_stats`) for the same records.
    mod columnar_differential {
        use super::*;
        use crate::columnar_batch::ColumnarLogBatch;
        use proptest::prelude::*;

        fn arb_attr_value() -> impl Strategy<Value = AttrValue> {
            let leaf = prop_oneof![
                any::<i64>().prop_map(AttrValue::I64),
                any::<f64>().prop_map(AttrValue::F64),
                any::<bool>().prop_map(AttrValue::Bool),
                "[a-z]{0,4}".prop_map(AttrValue::Str),
                proptest::collection::vec(any::<u8>(), 0..4).prop_map(AttrValue::Bytes),
            ];
            leaf.prop_recursive(2, 6, 3, |inner| {
                prop_oneof![
                    proptest::collection::vec(inner.clone(), 0..3).prop_map(AttrValue::List),
                    proptest::collection::vec(("[a-z]{1,3}", inner), 0..3).prop_map(AttrValue::Map),
                ]
            })
        }

        fn arb_name() -> impl Strategy<Value = String> {
            // A small pool so duplicate keys within a record and the same key
            // with two value types across records both arise; `http.status` and
            // `a` are also indexed below.
            prop::sample::select(vec!["a", "b", "c", "dup", "http.status", "svc.k"])
                .prop_map(String::from)
        }

        fn arb_record() -> impl Strategy<Value = LogRecord> {
            (
                0u8..3,
                0i64..40,
                0u8..30,
                prop::sample::select(vec!["", "INFO", "ERROR"]),
                prop::sample::select(vec!["", "hello", "a b c"]),
                prop_oneof![Just(None), any::<[u8; 16]>().prop_map(Some)],
                prop_oneof![Just(None), any::<[u8; 8]>().prop_map(Some)],
                any::<u32>(),
                proptest::collection::vec((arb_name(), arb_attr_value()), 0..6),
            )
                .prop_map(|(s, ts, sev, sevt, body, trace, span, flags, attrs)| {
                    LogRecord {
                        stream_id: id(s),
                        stream_attrs: attrs_blob(s),
                        ts_ns: ts,
                        observed_ts_ns: ts,
                        severity_num: sev,
                        severity_text: sevt.into(),
                        body: body.into(),
                        trace_id: trace,
                        span_id: span,
                        flags,
                        attrs,
                    }
                })
        }

        fn split(recs: &[LogRecord], n: usize) -> Vec<&[LogRecord]> {
            if n <= 1 || recs.is_empty() {
                return vec![recs];
            }
            let chunk = recs.len().div_ceil(n).max(1);
            recs.chunks(chunk).collect()
        }

        fn indexed() -> Vec<String> {
            // `service.name` lives only on the resource (stream-level-only,
            // indexed); `http.status`/`a` are per-record indexed keys.
            vec![
                "service.name".to_string(),
                "http.status".to_string(),
                "a".to_string(),
            ]
        }

        fn row_object(cfg: RlogConfig, recs: &[LogRecord]) -> (Vec<u8>, WriteStats) {
            let mut w = RlogWriter::new(cfg, identity()).with_indexed_fields(indexed());
            for r in recs {
                w.push(r.clone()).expect("row push");
            }
            w.finish_with_stats().expect("row finish")
        }

        fn columnar_object(
            cfg: RlogConfig,
            recs: &[LogRecord],
            nbatches: usize,
        ) -> (Vec<u8>, WriteStats) {
            let mut w = RlogWriter::new(cfg, identity()).with_indexed_fields(indexed());
            for chunk in split(recs, nbatches) {
                if chunk.is_empty() {
                    continue;
                }
                w.push_columnar(ColumnarLogBatch::from_records(chunk))
                    .expect("columnar push");
            }
            w.finish_with_stats().expect("columnar finish")
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// Byte-for-byte and field-for-field equality of the two paths over
            /// a corpus reaching nulls in every optional column, `List`/`Map`
            /// values, a key with two types across records, a duplicate key in
            /// one record, all-null attribute columns, dynamic-column budget
            /// overflow (`max_dyn` driven low), a resource-only indexed field,
            /// and more than one batch merged into one object.
            #[test]
            fn columnar_build_matches_row_build_byte_for_byte(
                records in proptest::collection::vec(arb_record(), 1..25),
                max_dyn in 1usize..8,
                nbatches in 1usize..4,
            ) {
                let cfg = RlogConfig {
                    max_dynamic_columns: max_dyn,
                    block_target_records: 5,
                    block_max_bytes: 8192,
                    ..RlogConfig::default()
                };
                let (rb, rs) = row_object(cfg, &records);
                let (cb, cs) = columnar_object(cfg, &records, nbatches);
                prop_assert_eq!(rs, cs);
                prop_assert!(rb == cb, "object bytes differ: row {} vs col {}", rb.len(), cb.len());
            }
        }

        /// `InconsistentStreamAttrs` fires on the columnar path for the same
        /// input that triggers it on the row path. A `ColumnarLogBatch` keys its
        /// `stream_attrs` by stream id, so one batch cannot carry two blobs for
        /// one id; the conflict is expressed across two batches, and the
        /// writer's cross-batch STREAM_DIR merge rejects it exactly as the row
        /// path's cross-record merge does.
        #[test]
        fn inconsistent_stream_attrs_fires_on_columnar_path() {
            let mut r1 = base_record(1, 0);
            let mut r2 = base_record(1, 1);
            r1.stream_attrs = attrs_blob(1);
            r2.stream_attrs = attrs_blob(2); // same id(1), different blob

            let cfg = RlogConfig::default();
            let mut w = RlogWriter::new(cfg, identity());
            w.push(r1.clone()).expect("row push r1");
            w.push(r2.clone()).expect("row push r2");
            assert!(matches!(
                w.finish(),
                Err(LogSegError::InconsistentStreamAttrs(_))
            ));

            let mut w2 = RlogWriter::new(cfg, identity());
            w2.push_columnar(ColumnarLogBatch::from_records(std::slice::from_ref(&r1)))
                .expect("columnar push r1");
            w2.push_columnar(ColumnarLogBatch::from_records(std::slice::from_ref(&r2)))
                .expect("columnar push r2");
            assert!(matches!(
                w2.finish(),
                Err(LogSegError::InconsistentStreamAttrs(_))
            ));
        }

        /// Both paths cut the same blocks: identical block count and identical
        /// per-block record counts, asserted exactly (never `> 0`).
        #[test]
        fn columnar_cuts_identical_blocks() {
            let cfg = RlogConfig {
                block_target_records: 4,
                block_max_bytes: 8192,
                ..RlogConfig::default()
            };
            let mut records = Vec::new();
            for i in 0..20i64 {
                let mut r = base_record((i % 2) as u8, i);
                r.attrs = vec![("a".into(), AttrValue::I64(i))];
                records.push(r);
            }
            let (rb, _) = row_object(cfg, &records);
            let (cb, _) = columnar_object(cfg, &records, 3);
            assert_eq!(rb, cb, "object bytes must match");

            let row_l0 = read_skip_index(&rb).l0;
            let col_l0 = read_skip_index(&cb).l0;
            let row_counts: Vec<u32> = row_l0.iter().map(|e| e.record_count).collect();
            let col_counts: Vec<u32> = col_l0.iter().map(|e| e.record_count).collect();
            assert!(row_counts.len() > 1, "expected multiple blocks");
            assert_eq!(row_counts, col_counts);
        }
    }
}
