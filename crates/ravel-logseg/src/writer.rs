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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use ravel_types::logstream::{AttrValue, LogStreamId, canonical_attr_bytes};

use crate::block::{
    BlockStrDict, BlockWriteOut, ColumnPlan, ColumnarBlockInput, write_block, write_block_columnar,
};
use crate::bloom::BloomBuilder;
use crate::bloom_section::encode_bloom_section;
use crate::columnar_batch::ColumnarLogBatch;
use crate::error::LogSegError;
use crate::field_dir::{FieldDir, FieldEntry};
use crate::footer::{COMP_NONE, COMP_ZSTD, LogFooter, SectionDesc, kind, write_footer_and_trailer};
use crate::page_dir::{ChunkEntry, GroupEntry, PageDir, PageEntry};
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
    /// Blocks per row group (docs/adrs/0699-rlog-row-groups-and-page-directory.md
    /// decision 4). A row group is this many consecutive blocks whose pages
    /// BLOCKS stores column-major, so a projection of `k` columns over a
    /// group is `k` contiguous ranges instead of one range per block. The
    /// block itself is unchanged: `block_target_records`, `block_max_bytes`,
    /// and every block-keyed prune keep their granularity. A zero value is
    /// treated as one block per group; an object with fewer blocks than this
    /// has one short row group.
    pub group_target_blocks: usize,
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
            group_target_blocks: 32,
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

/// The largest [`RlogConfig::max_dynamic_columns`] an object can carry and
/// still be read back. The writer assigns dynamic column ids
/// `FIRST_DYNAMIC_COL + idx` for `idx` in `0..max_dynamic_columns`, so the
/// largest id a config can assign is `FIRST_DYNAMIC_COL + max_dynamic_columns`
/// minus one; `decode_v4_block` refuses any page whose `column_id` exceeds
/// [`crate::block::MAX_COLUMN_ID`]. Derived from that one shared cap so the
/// writer and decoder bounds cannot drift apart.
pub(crate) const MAX_DYNAMIC_COLUMNS: u64 =
    crate::block::MAX_COLUMN_ID - FIRST_DYNAMIC_COL as u64 + 1;

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
        let layout = self.layout();
        self.build(0, Vec::new(), 0, layout)
    }

    /// The row-group layout this writer's configuration asks for.
    fn layout(&self) -> Layout {
        Layout {
            group_target_blocks: self.cfg.group_target_blocks,
        }
    }

    /// Routes to the row or columnar build pipeline. A writer is one or the
    /// other for its lifetime (ADR-0109 decision 5), and both produce the same
    /// bytes for the same records.
    fn build(
        self,
        level: u32,
        input_set_hash: Vec<u8>,
        part_index: u32,
        layout: Layout,
    ) -> Result<(Vec<u8>, WriteStats), LogSegError> {
        // A config whose dynamic-column budget could assign an id past the
        // decoder's cap writes objects that cannot be read back. Refuse it here,
        // before any column is assigned, against the same shared cap the decoder
        // enforces ([`MAX_DYNAMIC_COLUMNS`]), rather than letting the mismatch
        // surface at read time as an object nothing can decode.
        if self.cfg.max_dynamic_columns as u64 > MAX_DYNAMIC_COLUMNS {
            return Err(LogSegError::LimitExceeded(format!(
                "max_dynamic_columns {} exceeds {MAX_DYNAMIC_COLUMNS}, the most the decoder can read back",
                self.cfg.max_dynamic_columns
            )));
        }
        if !self.batches.is_empty() {
            return self.build_object_columnar(level, input_set_hash, part_index, layout);
        }
        self.build_object(level, input_set_hash, part_index, layout)
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
        let layout = self.layout();
        self.build(level, input_set_hash, part_index, layout)
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
        layout: Layout,
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
        // - Indexed keys (ADR-0049 amendment), so the stamp can key their
        //   merged-view postings by a column.
        // - Numeric keys (I64, F64, Bool), so the stamp has a column to key
        //   their NumStat by. Without it a name only ever resolved off the
        //   stream layer gets no column, drops out of
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
                if stream_level_column_eligible(k.as_str(), ty, &indexed_names) {
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

        // Tracked names (indexed union numstat), interned once into a flat table
        // so a record's tracked occurrences key by a small slot index instead of
        // a fresh `String` and `BTreeMap` node per cell (#682, #1135). The
        // columnar path interns the same set the same way, so both stamp through
        // one implementation.
        let (tracked_names, tracked_slot) = intern_tracked_names(&indexed_names, &numstat_names);
        let stamp_index =
            StampIndex::build(&tracked_names, &indexed_names, &numstat_names, &column_of);

        // Each stream's resource and scope pairs, restricted to the tracked names
        // (`indexed_names` union `numstat_names`) and resolved to
        // `(type byte, value)`. This is the seed of every record's merged-view
        // resolution ([`StampScratch::finish`]), which both POSTINGS and the
        // SKIP_IDX NumStats are projections of. Decoded once per stream, not once
        // per record: a stream's blob is byte-identical across its records (the
        // check above refuses an object where it is not), so the per-record decode
        // would repeat the same work. Skipped entirely when nothing is tracked.
        // A corrupt blob fails the write rather than silently dropping
        // stream-level values, since an under-populated posting list or an
        // under-bounded stat would prune a block a merged-view query needs.
        let mut stream_seeds: HashMap<LogStreamId, StreamSeed> = HashMap::new();
        if !tracked_names.is_empty() {
            for (id, blob) in &streams {
                let seed =
                    StreamSeed::build(stream_attr_pairs(blob)?, &tracked_slot, stamp_index.slots());
                stream_seeds.insert(*id, seed);
            }
        }

        // Resolve every record into storage form, then sort by (stream_ref, ts).
        // `resolve_row` also computes each row's merged-view POSTINGS terms and
        // its per-name NumStat winners, both read off the one resolved merged
        // view seeded from `stream_seeds`. One scratch serves every record: it is
        // cleared per record, never rebuilt (#1135).
        let mut stamp = StampScratch::default();
        stamp.prepare(stamp_index.slots());
        let mut rows: Vec<ResolvedRow> = Vec::with_capacity(self.records.len());
        for r in &self.records {
            rows.push(resolve_row(
                r,
                &ref_of,
                &column_of,
                &tracked_slot,
                &stamp_index,
                &stream_seeds,
                &mut stamp,
            ));
        }
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

        let mut blocks = BlocksBuilder::new(layout);
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

            blocks.push(out);
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

        let (blocks_bytes, l0, page_dir) = blocks.finish();
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
        // PAGE_DIR is mandatory (ADR-0699 decision 2). Compressed as a whole
        // section under the section crc, like SKIP_IDX: it is read whole on
        // every open.
        push_section(
            &mut object,
            &mut sections,
            kind::PAGE_DIR,
            &compress(&page_dir.encode(), self.cfg.zstd_level)?,
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
    /// ([`StampScratch::finish`], `canonical_attr_bytes`), which is what makes
    /// the output byte-identical (decision 7). The
    /// merged-view and `attrs_raw` derivations stay per row exactly as the row
    /// path's do; only the value gather and the column-id probe change shape.
    fn build_object_columnar(
        self,
        level: u32,
        input_set_hash: Vec<u8>,
        part_index: u32,
        layout: Layout,
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
                if stream_level_column_eligible(k.as_str(), ty, &indexed_names) {
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

        // Tracked names (indexed union numstat), interned once into a flat table
        // so a row's tracked occurrences key by a small slot index instead of a
        // fresh `String` and `BTreeMap` node per cell (#682). The table is
        // name-sorted, so ascending slot order is ascending name order, which is
        // the order the merged view appends record-level winners in. The row
        // path interns the same set the same way and stamps through the same
        // scratch, so the two paths cannot disagree (#1135).
        let (tracked_names, tracked_slot) = intern_tracked_names(&indexed_names, &numstat_names);
        let stamp_index =
            StampIndex::build(&tracked_names, &indexed_names, &numstat_names, &column_of);

        let mut stream_seeds: HashMap<LogStreamId, StreamSeed> = HashMap::new();
        if !tracked_names.is_empty() {
            for (id, blob) in &streams {
                let seed =
                    StreamSeed::build(stream_attr_pairs(blob)?, &tracked_slot, stamp_index.slots());
                stream_seeds.insert(*id, seed);
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
        // Source batch of each global row, so a block can map a scattered `grow`
        // back to its source cells when it materializes its rows (#682).
        let mut g_batch: Vec<u32> = Vec::with_capacity(total_rows);

        for (bi, b) in batches.iter().enumerate() {
            let mut trace_slot = 0usize;
            let mut span_slot = 0usize;
            for row in 0..b.num_rows {
                g_batch.push(bi as u32);
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

        // Per (batch, dyn column) constants the per-row loop below would
        // otherwise re-derive per cell: the in-budget dynamic column the
        // column's own `(name, type)` takes, and the tracked slot of its name.
        // Both are properties of the column, not of the row, and each cost a
        // SipHash of the name per cell before (#1135).
        let col_meta: Vec<Vec<DynColMeta>> = batches
            .iter()
            .map(|b| {
                b.dyn_columns
                    .iter()
                    .map(|c| DynColMeta {
                        column_id: column_lookup(&column_of, &c.name, c.field_type.to_u8()),
                        slot: tracked_slot.get(c.name.as_str()).copied(),
                    })
                    .collect()
            })
            .collect();

        // Per (batch, column) word-level popcount prefix over the presence
        // bitmap. A block materializes its rows from source (below), and this
        // lets it find one (column, row)'s dense-cell slot in O(1) instead of
        // holding every resolved value for the whole batch. At one u32 per 64
        // rows per column it is a tiny fraction of one value copy; it is the
        // only whole-batch structure the per-block path adds where the old path
        // held `col_values`/`col_stat`/`g_cols` for the whole batch (#682).
        let col_rank: Vec<Vec<Vec<u32>>> = batches
            .iter()
            .map(|b| {
                b.dyn_columns
                    .iter()
                    .map(|c| presence_word_prefix(c.validity.bytes()))
                    .collect()
            })
            .collect();

        // `attrs_raw` (whole batch, overflow only) and the value part of each
        // row's `row_estimate`. Both are needed before the block cut, so they
        // are computed here in one source pass. Overflow attributes are gathered
        // in the same order (dyn-column order, then residual order) the row path
        // folds them, so `canonical_attr_bytes` reproduces it byte for byte; the
        // estimate reads source cell sizes without copying any value.
        let mut g_overflow: Vec<Vec<(String, AttrValue)>> = vec![Vec::new(); total_rows];
        let mut g_est_dyn: Vec<usize> = vec![0; total_rows];
        for (bi, b) in batches.iter().enumerate() {
            let base = bases[bi];
            for c in &b.dyn_columns {
                let in_budget = column_lookup(&column_of, &c.name, c.field_type.to_u8()).is_some();
                let mut slot = 0usize;
                for row in 0..b.num_rows {
                    if !c.validity.get(row) {
                        continue;
                    }
                    let cell = &c.cells[slot];
                    slot += 1;
                    let grow = base + row;
                    if in_budget {
                        g_est_dyn[grow] += columnar_estimate(cell);
                    } else {
                        g_overflow[grow].push((c.name.clone(), cell.clone()));
                    }
                }
            }
            for (row, extras) in b.residual_attrs.iter().enumerate() {
                let grow = base + row;
                for (k, v) in extras {
                    g_overflow[grow].push((k.clone(), v.clone()));
                }
            }
        }
        let mut g_attrs_raw: Vec<Option<Vec<u8>>> = vec![None; total_rows];
        for grow in 0..total_rows {
            if !g_overflow[grow].is_empty() {
                g_attrs_raw[grow] = Some(canonical_attr_bytes(&g_overflow[grow]));
            }
        }
        drop(g_overflow);

        // Dictionary fast path (ADR-0109 decision 3): for each Str/Bytes plan
        // column whose every contributing batch column arrived dictionary-
        // shaped, build one per-object distinct table and a per-global-row
        // index into it. The block string encode and the token bloom then run
        // per distinct value, not per row. A plan column with any plain
        // contributor falls back to the per-block materialized value page, so
        // byte-identity with the row path is preserved either way.
        let mut plan_uses_dict = vec![false; num_plans];
        let mut global_dict: Vec<Vec<Vec<u8>>> = vec![Vec::new(); num_plans];
        let mut col_dict_ids: Vec<Vec<Option<u32>>> = vec![Vec::new(); num_plans];
        {
            let mut interners: Vec<HashMap<Vec<u8>, u32>> = vec![HashMap::new(); num_plans];
            for (pi, (_, ty, _)) in columns.iter().enumerate() {
                if matches!(ty, FieldType::Str | FieldType::Bytes) {
                    plan_uses_dict[pi] = true;
                    col_dict_ids[pi] = vec![None; total_rows];
                }
            }
            for (bi, b) in batches.iter().enumerate() {
                let base = bases[bi];
                for (ci, c) in b.dyn_columns.iter().enumerate() {
                    if !matches!(c.field_type, FieldType::Str | FieldType::Bytes) {
                        continue;
                    }
                    let Some(cid) = column_lookup(&column_of, &c.name, c.field_type.to_u8()) else {
                        continue; // overflowed the budget: folds into attrs_raw
                    };
                    let pi = plan_of_cid[&cid];
                    if !plan_uses_dict[pi] {
                        continue; // already poisoned by a plain contributor
                    }
                    let Some(d) = b.dyn_col_dicts.get(ci).and_then(Option::as_ref) else {
                        plan_uses_dict[pi] = false; // plain contributor: fall back
                        continue;
                    };
                    let mut slot = 0usize;
                    for row in 0..b.num_rows {
                        if !c.validity.get(row) {
                            continue;
                        }
                        let val = &d.distinct[d.ids[slot] as usize];
                        slot += 1;
                        let next = global_dict[pi].len() as u32;
                        let gid = *interners[pi].entry(val.clone()).or_insert_with(|| {
                            global_dict[pi].push(val.clone());
                            next
                        });
                        col_dict_ids[pi][base + row] = Some(gid);
                    }
                }
            }
        }
        // Sort rows by (stream_ref, ts) exactly as the row path sorts
        // ResolvedRows; `sort_by` is stable, so ties keep the appended order.
        let mut perm: Vec<usize> = (0..total_rows).collect();
        perm.sort_by(|&a, &b| {
            g_stream_ref[a]
                .cmp(&g_stream_ref[b])
                .then_with(|| g_ts[a].cmp(&g_ts[b]))
        });

        // Chunk into blocks, reproducing chunk_blocks/row_estimate. The dynamic
        // part is precomputed in `g_est_dyn`; the rest is byte-identical.
        let row_estimate = |grow: usize| -> usize {
            let mut est = 40usize;
            est += g_body[grow].len();
            est += g_sevtext[grow].len();
            if let Some(raw) = &g_attrs_raw[grow] {
                est += raw.len();
            }
            est += g_est_dyn[grow];
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
        let mut blocks = BlocksBuilder::new(layout);
        let mut bloom_entries: Vec<Vec<u8>> = Vec::new();
        let mut postings_terms: BTreeMap<u32, BTreeMap<Vec<u8>, BTreeSet<u32>>> = BTreeMap::new();
        let mut postings_capped: BTreeSet<u32> = BTreeSet::new();
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;
        let mut min_obs = i64::MAX;
        let mut max_obs = i64::MIN;

        // The per-record stamp and its outputs, hoisted out of both loops: one
        // scratch and one pair of flat per-block arrays serve every record, each
        // cleared rather than rebuilt, so the stamp path allocates nothing in
        // steady state (#1135). `blk_indexed_ends[li]` / `blk_stat_ends[li]` are
        // the end offsets of block row `li`'s entries in the flat arrays.
        let mut stamp = StampScratch::default();
        stamp.prepare(stamp_index.slots());
        let mut blk_indexed: Vec<(u32, ColumnValue)> = Vec::new();
        let mut blk_indexed_ends: Vec<u32> = Vec::new();
        let mut blk_stat: Vec<(u32, ColumnValue)> = Vec::new();
        let mut blk_stat_ends: Vec<u32> = Vec::new();

        for (blk_idx, span) in spans.iter().enumerate() {
            let block_rows = &perm[span.clone()];
            let blk_idx_u32 = blk_idx as u32;

            // Materialize only this block's rows from source: each row's
            // in-budget columnar occurrences (cid-sorted) and its merged-view
            // POSTINGS terms and NumStat winners. `attrs_raw` is already whole
            // batch. This is the one copy of one block's cells that bounds the
            // peak (#682); nothing built here is retained past the block.
            let n = block_rows.len();
            let mut blk_cols: Vec<Vec<(u32, ColumnValue)>> = Vec::with_capacity(n);
            blk_indexed.clear();
            blk_indexed_ends.clear();
            blk_stat.clear();
            blk_stat_ends.clear();
            for &grow in block_rows {
                let bi = g_batch[grow] as usize;
                let b = &batches[bi];
                let local = grow - bases[bi];
                let mut cols: Vec<(u32, ColumnValue)> = Vec::new();
                stamp.begin();
                for (ci, c) in b.dyn_columns.iter().enumerate() {
                    if !c.validity.get(local) {
                        continue;
                    }
                    let dense = presence_rank(&col_rank[bi][ci], c.validity.bytes(), local);
                    let cell = &c.cells[dense];
                    let meta = &col_meta[bi][ci];
                    match meta.column_id {
                        Some(cid) => {
                            let (ty, cv) = resolve_value(cell);
                            if let Some(slot) = meta.slot {
                                stamp.push_columnar(slot, ty.to_u8(), cv.clone());
                            }
                            cols.push((cid, cv));
                        }
                        None => {
                            if let Some(slot) = meta.slot {
                                stamp.push_overflow(slot, cell.clone());
                            }
                        }
                    }
                }
                for (k, v) in &b.residual_attrs[local] {
                    if let Some(&slot) = tracked_slot.get(k.as_str()) {
                        stamp.push_overflow(slot, v.clone());
                    }
                }
                cols.sort_by_key(|(cid, _)| *cid);
                stamp.finish(
                    &stamp_index,
                    stream_seeds
                        .get(&g_stream_id[grow])
                        .unwrap_or(&EMPTY_STREAM_SEED),
                    &mut StampOut {
                        indexed: &mut blk_indexed,
                        stat: &mut blk_stat,
                    },
                );
                blk_indexed_ends.push(blk_indexed.len() as u32);
                blk_stat_ends.push(blk_stat.len() as u32);
                blk_cols.push(cols);
            }

            // Present columns and their FIELD_DIR occurrence counts, read from
            // the cids before the values are moved into the page below.
            let mut present_cols: BTreeSet<u32> = BTreeSet::new();
            for cols in &blk_cols {
                for (cid, _) in cols {
                    present_cols.insert(*cid);
                    *col_present.entry(*cid).or_insert(0) += 1;
                }
            }
            let mut plan_cols: BTreeSet<u32> = present_cols.clone();
            for (cid, _) in &blk_stat {
                plan_cols.insert(*cid);
            }
            let plans: Vec<ColumnPlan> = plan_cols
                .iter()
                .map(|cid| ColumnPlan {
                    column_id: *cid,
                    ty: ty_of_column[cid],
                })
                .collect();
            let plan_pos: HashMap<u32, usize> = plans
                .iter()
                .enumerate()
                .map(|(i, p)| (p.column_id, i))
                .collect();

            // Column-major fixed inputs, gathered by the sort permutation.
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

            // Per-plan value pages, filled by moving each occurrence's value out
            // of the materialized list (consumed here, never cloned): the bytes
            // live in exactly one place, the page. Dict-path plans carry their
            // page below instead, so their moved value is simply dropped.
            let mut values_v: Vec<Vec<Option<ColumnValue>>> = plans
                .iter()
                .map(|p| {
                    if plan_uses_dict[plan_of_cid[&p.column_id]] {
                        Vec::new()
                    } else {
                        vec![None; n]
                    }
                })
                .collect();
            for (li, cols) in blk_cols.into_iter().enumerate() {
                for (cid, cv) in cols {
                    if !plan_uses_dict[plan_of_cid[&cid]] {
                        values_v[plan_pos[&cid]][li] = Some(cv);
                    }
                }
            }
            let mut stat_v: Vec<Vec<Option<ColumnValue>>> =
                plans.iter().map(|_| vec![None; n]).collect();
            {
                // Drained rather than consumed: the flat array is reused by the
                // next block, so its capacity outlives this one.
                let mut drain = blk_stat.drain(..);
                let mut start = 0usize;
                for (li, &end) in blk_stat_ends.iter().enumerate() {
                    for _ in start..end as usize {
                        let Some((cid, cv)) = drain.next() else { break };
                        stat_v[plan_pos[&cid]][li] = Some(cv);
                    }
                    start = end as usize;
                }
            }
            // Owned per-block dict ids (one Option<index> per block row) for
            // every dict-path plan, referenced by `str_dicts_v` below.
            let str_dict_ids_v: Vec<Option<Vec<Option<u32>>>> = plans
                .iter()
                .map(|p| {
                    let plan_idx = plan_of_cid[&p.column_id];
                    plan_uses_dict[plan_idx].then(|| {
                        block_rows
                            .iter()
                            .map(|&g| col_dict_ids[plan_idx][g])
                            .collect()
                    })
                })
                .collect();
            let str_dicts_v: Vec<Option<BlockStrDict>> = plans
                .iter()
                .enumerate()
                .map(|(i, p)| {
                    let plan_idx = plan_of_cid[&p.column_id];
                    str_dict_ids_v[i].as_ref().map(|ids| BlockStrDict {
                        dict: &global_dict[plan_idx],
                        ids,
                    })
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
                str_dicts: &str_dicts_v,
            };
            let out = write_block_columnar(&input, self.cfg.zstd_level)?;

            // Bloom over body, severity_text, and string columns; POSTINGS over
            // each row's merged-view indexed terms.
            let mut builder = BloomBuilder::new(self.cfg.bloom_seed);
            let mut terms_start = 0usize;
            for (li, &g) in block_rows.iter().enumerate() {
                insert_text(&mut builder, COL_BODY, g_body[g]);
                insert_text(&mut builder, COL_SEVERITY_TEXT, g_sevtext[g]);
                let terms_end = blk_indexed_ends
                    .get(li)
                    .map_or(terms_start, |end| *end as usize);
                let terms = blk_indexed.get(terms_start..terms_end).unwrap_or(&[]);
                terms_start = terms_end;
                for (cid, v) in terms {
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
            // String-column bloom from the value pages. Bloom bit setting is
            // idempotent, so plan-major insertion sets the same bits the per-row
            // path did. Dict-path Str columns are tokenized per distinct value
            // below instead (their page is empty here).
            for (pos, p) in plans.iter().enumerate() {
                if plan_uses_dict[plan_of_cid[&p.column_id]] || !matches!(p.ty, FieldType::Str) {
                    continue;
                }
                for cell in &values_v[pos] {
                    if let Some(ColumnValue::Str(bytes)) = cell {
                        insert_text(&mut builder, p.column_id, bytes);
                    }
                }
            }
            // Dict-path Str columns: tokenize each distinct value present in the
            // block exactly once. Bloom bit setting is idempotent, so these set
            // the same bits the per-row path would, at per-distinct cost.
            for p in &plans {
                let plan_idx = plan_of_cid[&p.column_id];
                if !(plan_uses_dict[plan_idx] && matches!(p.ty, FieldType::Str)) {
                    continue;
                }
                let mut seen: HashSet<u32> = HashSet::new();
                for &g in block_rows {
                    if let Some(gid) = col_dict_ids[plan_idx][g]
                        && seen.insert(gid)
                    {
                        insert_text(
                            &mut builder,
                            p.column_id,
                            &global_dict[plan_idx][gid as usize],
                        );
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

            blocks.push(out);
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

        let (blocks_bytes, l0, page_dir) = blocks.finish();
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
        // PAGE_DIR is mandatory (ADR-0699 decision 2). Compressed as a whole
        // section under the section crc, like SKIP_IDX: it is read whole on
        // every open.
        push_section(
            &mut object,
            &mut sections,
            kind::PAGE_DIR,
            &compress(&page_dir.encode(), self.cfg.zstd_level)?,
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

/// Everything the columnar per-row loop needs about one batch column, resolved
/// once per `(batch, column)` instead of once per cell: the in-budget dynamic
/// column its `(name, type)` takes (`None` when it overflowed the budget, so
/// its cells fold into `attrs_raw`), and the tracked slot of its name (`None`
/// when the name is neither indexed nor NumStat-eligible).
struct DynColMeta {
    column_id: Option<u32>,
    slot: Option<u32>,
}

/// Resolves one `(name, type byte)` pair to its dynamic column id, borrowing the
/// name rather than cloning it. `None` when the pair took no in-budget column
/// (overflowed, or never given one). This is the read half of [`ColumnIndex`];
/// keep every probe going through it so no call site reconstructs an owned key.
fn column_lookup(column_of: &ColumnIndex, name: &str, ty_byte: u8) -> Option<u32> {
    column_of.get(&ty_byte).and_then(|m| m.get(name)).copied()
}

/// Whether a stream-level-only attribute `(name, ty)` earns a dynamic column
/// even though no record carries it per-record. Two kinds qualify:
///
/// - an indexed name (ADR-0049 amendment), so [`StampScratch::finish`] can key
///   its merged-view postings by a column, and
/// - a numeric type (I64/F64/Bool), so the same pass has a column to key its
///   NumStat by.
///
/// The row path ([`RlogWriter::build_object`]) and the columnar path
/// ([`RlogWriter::build_object_columnar`]) both grant these columns, and the
/// rule has to be identical on both or the two would assign different columns
/// for the same records and break the byte-identity guarantee (ADR-0109
/// decision 7). It lives here, called from both, so it cannot drift.
fn stream_level_column_eligible(
    name: &str,
    ty: FieldType,
    indexed_names: &std::collections::HashSet<&str>,
) -> bool {
    indexed_names.contains(name) || matches!(ty, FieldType::I64 | FieldType::F64 | FieldType::Bool)
}

/// Resolves one record into storage form: dense stream ref, dynamic columns
/// split by type, overflow attributes canonicalized into `attrs_raw`, the
/// merged-view POSTINGS terms this record contributes, and its per-name NumStat
/// winners.
///
/// The last two are two projections of one merged view
/// ([`StampScratch::finish`]), not two independently derived answers: they must
/// not disagree about which value a reader resolves for a tracked name
/// (ADR-0095 decision 1). The columnar path stamps through the same scratch, so
/// the two paths cannot drift.
fn resolve_row(
    r: &LogRecord,
    ref_of: &HashMap<LogStreamId, u32>,
    column_of: &ColumnIndex,
    slot_of: &HashMap<&str, u32>,
    index: &StampIndex,
    stream_seeds: &HashMap<LogStreamId, StreamSeed>,
    stamp: &mut StampScratch,
) -> ResolvedRow {
    let stream_ref = ref_of.get(&r.stream_id).copied().unwrap_or(0);
    let mut cols: BTreeMap<u32, ColumnValue> = BTreeMap::new();
    let mut overflow: Vec<(String, ravel_types::logstream::AttrValue)> = Vec::new();
    // Each *tracked* name this record carries -- indexed (POSTINGS) or
    // NumStat-eligible (ADR-0095 decision 1: `indexed_names` union
    // `numstat_names`), which is exactly the set `slot_of` keys -- has its own
    // occurrences pushed into the stamp scratch the same way the read side
    // reconstructs them: the columnar ones (those that took a fresh
    // dynamic-column slot, one per distinct type, each with its type byte kept
    // alongside its value) and the overflow ones (any type, duplicates and
    // budget overflow alike).
    stamp.begin();
    for (k, v) in &r.attrs {
        let (ty, cv) = resolve_value(v);
        let slot = slot_of.get(k.as_str()).copied();
        match column_lookup(column_of, k, ty.to_u8()) {
            Some(cid) if !cols.contains_key(&cid) => {
                if let Some(slot) = slot {
                    stamp.push_columnar(slot, ty.to_u8(), cv.clone());
                }
                cols.insert(cid, cv);
            }
            // Overflow column, or a duplicate (name,type) already columnar this
            // row: fold into attrs_raw so no value is lost.
            _ => {
                if let Some(slot) = slot {
                    stamp.push_overflow(slot, v.clone());
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
    let mut indexed_terms: Vec<(u32, ColumnValue)> = Vec::new();
    let mut stat_winners: Vec<(u32, ColumnValue)> = Vec::new();
    stamp.finish(
        index,
        stream_seeds.get(&r.stream_id).unwrap_or(&EMPTY_STREAM_SEED),
        &mut StampOut {
            indexed: &mut indexed_terms,
            stat: &mut stat_winners,
        },
    );
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

/// The number of distinct [`FieldType`] byte values (1..=5), the stride of
/// [`StampIndex::column_of_slot`].
const FIELD_TYPE_SPAN: usize = 5;

/// Sentinel for "no entry" in the slot-indexed tables below. A real index is a
/// position in a per-record occurrence list, so it never reaches this value.
const NO_ENTRY: u32 = u32::MAX;

/// Interns the tracked names (`indexed_names` union `numstat_names`, ADR-0095
/// decision 1) into slots, returning the name-ascending slot table and the
/// name-to-slot map.
///
/// Ascending slot order is ascending name order, which is the order the
/// merged view appends record-level winners in.
fn intern_tracked_names<'a>(
    indexed_names: &std::collections::HashSet<&'a str>,
    numstat_names: &std::collections::HashSet<&'a str>,
) -> (Vec<&'a str>, HashMap<&'a str, u32>) {
    let mut names: Vec<&str> = indexed_names
        .iter()
        .chain(numstat_names.iter())
        .copied()
        .collect();
    names.sort_unstable();
    names.dedup();
    let slot_of = names
        .iter()
        .enumerate()
        .map(|(i, n)| (*n, i as u32))
        .collect();
    (names, slot_of)
}

/// Everything the per-record stamp needs about a tracked name, precomputed per
/// object and read by slot.
///
/// Resolving a name to its dynamic column is a two-level hash probe over owned
/// `String` keys ([`column_lookup`]); done per record it costs one SipHash of
/// the name per resolved value. Here the whole `(slot, type byte)` cross
/// product is resolved once, at the column-index level, so the hot path indexes
/// a flat table instead (issue #1135).
struct StampIndex {
    /// Slot-indexed: the name may key a posting.
    indexed: Vec<bool>,
    /// Slot-indexed: the name may key a NumStat.
    numstat: Vec<bool>,
    /// `slot * FIELD_TYPE_SPAN + (type byte - 1)` to the dynamic column that
    /// pair takes, exactly as [`column_lookup`] resolves it. `None` when the
    /// pair took no in-budget column.
    column_of_slot: Vec<Option<u32>>,
}

impl StampIndex {
    fn build(
        names: &[&str],
        indexed_names: &std::collections::HashSet<&str>,
        numstat_names: &std::collections::HashSet<&str>,
        column_of: &ColumnIndex,
    ) -> Self {
        let mut indexed = Vec::with_capacity(names.len());
        let mut numstat = Vec::with_capacity(names.len());
        let mut column_of_slot = vec![None; names.len() * FIELD_TYPE_SPAN];
        for (slot, name) in names.iter().enumerate() {
            indexed.push(indexed_names.contains(name));
            numstat.push(numstat_names.contains(name));
            for ty in 0..FIELD_TYPE_SPAN {
                column_of_slot[slot * FIELD_TYPE_SPAN + ty] =
                    column_lookup(column_of, name, ty as u8 + 1);
            }
        }
        Self {
            indexed,
            numstat,
            column_of_slot,
        }
    }

    fn slots(&self) -> usize {
        self.indexed.len()
    }

    fn is_indexed(&self, slot: u32) -> bool {
        self.indexed.get(slot as usize).copied().unwrap_or(false)
    }

    fn is_numstat(&self, slot: u32) -> bool {
        self.numstat.get(slot as usize).copied().unwrap_or(false)
    }

    fn column(&self, slot: u32, ty_byte: u8) -> Option<u32> {
        let ty = usize::from(ty_byte).checked_sub(1)?;
        if ty >= FIELD_TYPE_SPAN {
            return None;
        }
        self.column_of_slot
            .get(slot as usize * FIELD_TYPE_SPAN + ty)
            .copied()
            .flatten()
    }
}

/// One stream's tracked resource and scope pairs, in blob order, resolved to
/// `(type byte, value)` and keyed by tracked slot. This is the stream layer
/// every record's merged view is seeded from, decoded once per stream.
struct StreamSeed {
    entries: Vec<SeedEntry>,
    /// Slot-indexed: `entries` carries this slot at all. A record-level winner
    /// for a slot that is absent here appends to the merged view instead of
    /// overriding an entry.
    has_slot: Vec<bool>,
}

/// One resolved stream-level pair.
struct SeedEntry {
    slot: u32,
    ty_byte: u8,
    value: ColumnValue,
    /// The first entry for this slot. Duplicate names *within* the stream layer
    /// (the same key on both the resource and the scope) are kept as the blob
    /// carries them, exactly as `merged_attrs` keeps them, and a record-level
    /// winner overrides only the first of them.
    first_of_slot: bool,
}

/// The seed of a stream with no tracked pairs, and of a record whose stream is
/// absent from the seed map entirely.
static EMPTY_STREAM_SEED: StreamSeed = StreamSeed {
    entries: Vec::new(),
    has_slot: Vec::new(),
};

impl StreamSeed {
    fn build(
        pairs: impl IntoIterator<Item = (String, ravel_types::logstream::AttrValue)>,
        slot_of: &HashMap<&str, u32>,
        slots: usize,
    ) -> Self {
        let mut entries = Vec::new();
        let mut has_slot = vec![false; slots];
        for (k, v) in pairs {
            let Some(&slot) = slot_of.get(k.as_str()) else {
                continue;
            };
            let (ty, cv) = resolve_value(&v);
            let first_of_slot = !has_slot[slot as usize];
            has_slot[slot as usize] = true;
            entries.push(SeedEntry {
                slot,
                ty_byte: ty.to_u8(),
                value: cv,
                first_of_slot,
            });
        }
        Self { entries, has_slot }
    }

    fn carries(&self, slot: u32) -> bool {
        self.has_slot.get(slot as usize).copied().unwrap_or(false)
    }
}

/// The per-record state of one tracked slot, held in a slot-indexed table on
/// [`StampScratch`] and reset through its `touched` list, so a record pays for
/// the slots it carries rather than for the object's whole slot table.
#[derive(Clone, Copy)]
struct SlotPick {
    /// Index into [`StampScratch::cols`] of the winning columnar occurrence.
    cols: u32,
    /// Type byte of the occurrence `cols` points at.
    cols_ty: u8,
    /// Index into [`StampScratch::over`] of the winning overflow occurrence.
    over: u32,
    /// Index into [`StampScratch::winners`] once the winner is materialized.
    winner: u32,
    /// A merged-view entry for this slot already reached the NumStat output.
    /// Only the first entry per name counts, matching `rlog_attrs::find_attr`.
    stat_seen: bool,
}

impl SlotPick {
    const EMPTY: SlotPick = SlotPick {
        cols: NO_ENTRY,
        cols_ty: 0,
        over: NO_ENTRY,
        winner: NO_ENTRY,
        stat_seen: false,
    };

    /// Untouched by this record, so not yet on the `touched` reset list.
    fn is_empty(&self) -> bool {
        self.cols == NO_ENTRY && self.over == NO_ENTRY && self.winner == NO_ENTRY && !self.stat_seen
    }
}

/// The two per-record outputs of the stamp. Both are appended to, never
/// cleared here: the columnar path points them at one block's flat arrays, the
/// row path at the pair its [`ResolvedRow`] takes ownership of.
struct StampOut<'a> {
    indexed: &'a mut Vec<(u32, ColumnValue)>,
    stat: &'a mut Vec<(u32, ColumnValue)>,
}

/// The per-record declared-column stat stamp, and the buffers it reuses.
///
/// One record's tracked occurrences are pushed in
/// ([`StampScratch::push_columnar`] / [`StampScratch::push_overflow`], slot-
/// keyed, no name cloned), then [`StampScratch::finish`] reduces them to the
/// merged view and projects that view onto the POSTINGS terms and the SKIP_IDX
/// NumStat winners. Every buffer lives here and is cleared per record rather
/// than rebuilt, so the steady-state path allocates nothing (issue #1135).
#[derive(Default)]
struct StampScratch {
    /// This record's tracked columnar occurrences, `(slot, type byte, value)`,
    /// in encounter order.
    cols: Vec<(u32, u8, ColumnValue)>,
    /// This record's tracked overflow occurrences, `(slot, value)`, in
    /// encounter order.
    over: Vec<(u32, ravel_types::logstream::AttrValue)>,
    /// Slot-indexed pick table, sized once per object by
    /// [`StampScratch::prepare`].
    pick: Vec<SlotPick>,
    /// The slots this record touched, the only entries of `pick` that need
    /// resetting.
    touched: Vec<u32>,
    /// This record's winners, `(slot, type byte, value)`, ascending by slot.
    winners: Vec<(u32, u8, ColumnValue)>,
}

impl StampScratch {
    /// Sizes the slot-indexed table for an object's tracked-name count. Called
    /// once per object, before the first record.
    fn prepare(&mut self, slots: usize) {
        self.pick.clear();
        self.pick.resize(slots, SlotPick::EMPTY);
    }

    /// Starts a record's occurrence list.
    fn begin(&mut self) {
        self.cols.clear();
        self.over.clear();
    }

    /// One tracked occurrence that took an in-budget dynamic column.
    fn push_columnar(&mut self, slot: u32, ty_byte: u8, value: ColumnValue) {
        self.cols.push((slot, ty_byte, value));
    }

    /// One tracked occurrence that folded into `attrs_raw`: an overflow column,
    /// or a duplicate `(name, type)` already columnar on this record.
    fn push_overflow(&mut self, slot: u32, value: ravel_types::logstream::AttrValue) {
        self.over.push((slot, value));
    }

    /// Reduces this record's occurrences to its merged view and appends the two
    /// projections of that view to `out`.
    ///
    /// The record-level winner of a slot is whatever the read side already,
    /// deterministically produces: `rebuild_record` lays a record's own
    /// attributes out as its columnar entries in FIELD_DIR `(name bytes,
    /// type)`-ascending order, followed by its `attrs_raw` overflow entries in
    /// `encode_attrs`'s canonical `(key bytes, encoded value bytes)`-ascending
    /// order, and `rlog_attrs::merged_attrs` then folds that list last-wins by
    /// name. Restricted to one name, that combined order is: the columnar
    /// occurrences ascending by [`FieldType::to_u8`], then the overflow
    /// occurrences ascending by the frozen canonical encoding of the value
    /// ([`canonical_value_bytes`], which shares the exact comparator
    /// `encode_attrs` uses so the two cannot drift). The last entry of that
    /// order wins, so an overflow occurrence beats every columnar one, and
    /// within a tier the last occurrence of the maximum key wins. Making the
    /// write side predict this, rather than independently pick a winner over
    /// the original write-time occurrence order the on-disk format does not
    /// preserve, is the issue #333 fix (docs/adrs/0049-rlog-postings.md
    /// amendment 2026-08-20).
    ///
    /// The merged view then lays those winners over the stream layer with the
    /// precedence `ravel_sql::rlog_attrs::merged_attrs` produces: the stream
    /// seed first, in blob order, each slot's first entry replaced by the
    /// record-level winner when the record carries one, then the winners of
    /// slots the seed does not carry, appended ascending by slot (which is
    /// ascending by name). A name absent from the record keeps its stream-level
    /// value rather than being dropped, and the record wins a collision.
    ///
    /// Both consumers read that one merged answer. They used to derive their
    /// own, and diverged: the stats saw only the record layer, so a name a
    /// record carried only on its resource or scope contributed nothing to the
    /// stat even though a reader resolves the stream-level value for it, and
    /// the stat then under-bounded its own column (ADR-0095 decision 1,
    /// corrected). Adding a value here reaches both.
    fn finish(&mut self, index: &StampIndex, seed: &StreamSeed, out: &mut StampOut<'_>) {
        let Self {
            cols,
            over,
            pick,
            touched,
            winners,
        } = self;

        // Group the columnar occurrences by slot in one pass. Keeping the last
        // occurrence whose type byte is greater than or equal to the incumbent
        // is exactly what a stable sort by type byte followed by taking the
        // last entry produced.
        for (i, (slot, ty_byte, _)) in cols.iter().enumerate() {
            let Some(p) = pick.get_mut(*slot as usize) else {
                continue;
            };
            if p.cols == NO_ENTRY {
                touched.push(*slot);
                p.cols = i as u32;
                p.cols_ty = *ty_byte;
            } else if *ty_byte >= p.cols_ty {
                p.cols = i as u32;
                p.cols_ty = *ty_byte;
            }
        }

        // Same pass over the overflow occurrences, ordered by the canonical
        // encoding of a one-entry value set. Only a slot carrying two of them
        // (a key duplicated in `attrs_raw`) encodes anything at all: with one
        // occurrence there is nothing to order it against.
        for (i, (slot, value)) in over.iter().enumerate() {
            let Some(p) = pick.get_mut(*slot as usize) else {
                continue;
            };
            if p.cols == NO_ENTRY && p.over == NO_ENTRY {
                touched.push(*slot);
            }
            match over.get(p.over as usize) {
                None => p.over = i as u32,
                Some((_, incumbent)) => {
                    if canonical_value_bytes(value) >= canonical_value_bytes(incumbent) {
                        p.over = i as u32;
                    }
                }
            }
        }

        // Materialize each touched slot's winner, ascending by slot: the
        // overflow tier wins outright when the record has one.
        touched.sort_unstable();
        for &slot in touched.iter() {
            let Some(p) = pick.get_mut(slot as usize) else {
                continue;
            };
            let winner = if let Some((_, value)) = over.get(p.over as usize) {
                let (ty, cv) = resolve_value(value);
                (slot, ty.to_u8(), cv)
            } else if let Some((_, ty_byte, cv)) = cols.get(p.cols as usize) {
                (slot, *ty_byte, cv.clone())
            } else {
                continue;
            };
            p.winner = winners.len() as u32;
            winners.push(winner);
        }

        // The merged view, emitted in order: the stream layer with winners
        // overriding in place, then the record-only winners.
        for entry in &seed.entries {
            let winner = if entry.first_of_slot {
                pick.get(entry.slot as usize)
                    .map(|p| p.winner)
                    .filter(|w| *w != NO_ENTRY)
                    .and_then(|w| winners.get(w as usize))
            } else {
                None
            };
            match winner {
                Some((_, ty_byte, value)) => {
                    emit_merged(index, pick, touched, entry.slot, *ty_byte, value, out);
                }
                None => emit_merged(
                    index,
                    pick,
                    touched,
                    entry.slot,
                    entry.ty_byte,
                    &entry.value,
                    out,
                ),
            }
        }
        for (slot, ty_byte, value) in winners.iter() {
            if seed.carries(*slot) {
                continue;
            }
            emit_merged(index, pick, touched, *slot, *ty_byte, value, out);
        }

        for &slot in touched.iter() {
            if let Some(p) = pick.get_mut(slot as usize) {
                *p = SlotPick::EMPTY;
            }
        }
        touched.clear();
        winners.clear();
        cols.clear();
        over.clear();
    }
}

/// Projects one merged-view entry onto the two per-record outputs.
///
/// POSTINGS takes every indexed entry, including a name the resource and the
/// scope both carry: the term set a posting list holds may be a superset of
/// what `find_attr` resolves, which costs precision and never soundness (an
/// extra term keeps a block, it cannot drop one). A field with no matching
/// dynamic column contributes nothing; absence is legal, no posting for a field
/// means no pruning on it, never a wrong prune.
///
/// The NumStat takes only the first entry per name, matching
/// `rlog_attrs::find_attr`, which is how a declared typed column resolves the
/// name. A resolved value of a non-numeric type (a string or bytes occurrence
/// that outranked the numeric one) yields no entry at all, so the name's
/// numeric column gets none either and the row counts as a null there -- which
/// is exactly what a reader materializing that typed column produces for the
/// row. A value whose own type has no in-budget column likewise yields no
/// entry, again matching the read side (no column, no value). The
/// first-per-name mark is taken before those two checks, so a first entry that
/// yields nothing still shadows any later entry for the name.
fn emit_merged(
    index: &StampIndex,
    pick: &mut [SlotPick],
    touched: &mut Vec<u32>,
    slot: u32,
    ty_byte: u8,
    value: &ColumnValue,
    out: &mut StampOut<'_>,
) {
    if index.is_indexed(slot)
        && let Some(cid) = index.column(slot, ty_byte)
    {
        out.indexed.push((cid, value.clone()));
    }
    if !index.is_numstat(slot) {
        return;
    }
    let Some(p) = pick.get_mut(slot as usize) else {
        return;
    };
    if p.stat_seen {
        return;
    }
    if p.is_empty() {
        // A slot the record does not carry, reached through the stream layer
        // alone: it needs resetting too.
        touched.push(slot);
    }
    p.stat_seen = true;
    if !matches!(
        FieldType::from_u8(ty_byte),
        Some(FieldType::I64 | FieldType::F64 | FieldType::Bool)
    ) {
        return;
    }
    if let Some(cid) = index.column(slot, ty_byte) {
        out.stat.push((cid, value.clone()));
    }
}

/// A test- and bench-only handle on the per-record declared-column stat stamp,
/// used by the allocation pin (`tests/stamp_alloc_pin.rs`) and the stamp
/// benchmark (`benches/stamp_declared.rs`). Both have to drive the stamp alone:
/// the record encode around it allocates per row by construction and would
/// swamp either figure (issue #1135).
///
/// Not part of the crate's supported API.
#[doc(hidden)]
pub mod stamp_probe {
    use std::collections::{HashMap, HashSet};

    use ravel_types::logstream::AttrValue;

    use crate::record::{ColumnValue, FieldType, resolve_value};

    use super::{
        ColumnIndex, StampIndex, StampOut, StampScratch, StreamSeed, column_lookup,
        intern_tracked_names,
    };

    /// A stamp's two projections: POSTINGS terms and NumStat winners.
    pub type StampOutputs<'a> = (&'a [(u32, ColumnValue)], &'a [(u32, ColumnValue)]);

    /// One record's tracked occurrences, split into the two tiers the winner
    /// rule orders: those that took an in-budget dynamic column, and those that
    /// folded into `attrs_raw` (an overflow column, or a duplicate
    /// `(name, type)` already columnar on the record).
    pub struct ProbeRecord {
        cols: Vec<(u32, u8, ColumnValue)>,
        over: Vec<(u32, AttrValue)>,
    }

    impl ProbeRecord {
        pub fn columnar(&self) -> &[(u32, u8, ColumnValue)] {
            &self.cols
        }

        pub fn overflow(&self) -> &[(u32, AttrValue)] {
            &self.over
        }
    }

    /// One object's stamp state plus a pre-split record corpus, so a caller can
    /// time or instrument [`StampProbe::stamp`] on its own.
    pub struct StampProbe {
        index: StampIndex,
        scratch: StampScratch,
        seed: StreamSeed,
        names: Vec<String>,
        records: Vec<ProbeRecord>,
        indexed: Vec<(u32, ColumnValue)>,
        stat: Vec<(u32, ColumnValue)>,
    }

    impl StampProbe {
        /// Builds the probe over one object's dynamic-column assignment
        /// (`columns`, as the writer assigns it), the caller's indexed-field
        /// list, one stream's resource and scope pairs, and the records to
        /// stamp. Every per-record occurrence list is split here, once, so
        /// `stamp` measures the stamp and nothing else.
        pub fn new(
            columns: &[(String, FieldType, u32)],
            indexed_fields: &[String],
            stream_attrs: &[(String, AttrValue)],
            records: &[Vec<(String, AttrValue)>],
        ) -> Self {
            let mut column_of: ColumnIndex = HashMap::new();
            for (name, ty, cid) in columns {
                column_of
                    .entry(ty.to_u8())
                    .or_default()
                    .insert(name.clone(), *cid);
            }
            let indexed_names: HashSet<&str> = indexed_fields.iter().map(String::as_str).collect();
            let numstat_names: HashSet<&str> = columns
                .iter()
                .filter(|(_, ty, _)| {
                    matches!(ty, FieldType::I64 | FieldType::F64 | FieldType::Bool)
                })
                .map(|(name, _, _)| name.as_str())
                .collect();
            let (names, slot_of) = intern_tracked_names(&indexed_names, &numstat_names);
            let index = StampIndex::build(&names, &indexed_names, &numstat_names, &column_of);
            let seed = StreamSeed::build(stream_attrs.iter().cloned(), &slot_of, index.slots());
            let records = records
                .iter()
                .map(|attrs| split_occurrences(attrs, &column_of, &slot_of))
                .collect();
            let mut scratch = StampScratch::default();
            scratch.prepare(index.slots());
            Self {
                index,
                scratch,
                seed,
                names: names.iter().map(|n| (*n).to_string()).collect(),
                records,
                indexed: Vec::new(),
                stat: Vec::new(),
            }
        }

        /// Slot to tracked name, name-ascending.
        pub fn tracked_names(&self) -> &[String] {
            &self.names
        }

        pub fn len(&self) -> usize {
            self.records.len()
        }

        pub fn is_empty(&self) -> bool {
            self.records.is_empty()
        }

        pub fn record(&self, i: usize) -> Option<&ProbeRecord> {
            self.records.get(i)
        }

        /// Stamps record `i`, leaving its POSTINGS terms and NumStat winners in
        /// [`StampProbe::outputs`]. Out of range is a no-op.
        pub fn stamp(&mut self, i: usize) {
            let Self {
                index,
                scratch,
                seed,
                records,
                indexed,
                stat,
                ..
            } = self;
            let Some(rec) = records.get(i) else {
                return;
            };
            scratch.begin();
            for (slot, ty_byte, value) in &rec.cols {
                scratch.push_columnar(*slot, *ty_byte, value.clone());
            }
            for (slot, value) in &rec.over {
                scratch.push_overflow(*slot, value.clone());
            }
            indexed.clear();
            stat.clear();
            scratch.finish(index, seed, &mut StampOut { indexed, stat });
        }

        /// The last [`StampProbe::stamp`]'s `(POSTINGS terms, NumStat winners)`.
        pub fn outputs(&self) -> StampOutputs<'_> {
            (&self.indexed, &self.stat)
        }
    }

    /// Splits one record's attributes into the two occurrence tiers, by the
    /// same rule `resolve_row` applies: the first occurrence of a
    /// `(name, type)` with an in-budget column is columnar, every later or
    /// column-less one folds into `attrs_raw`.
    fn split_occurrences(
        attrs: &[(String, AttrValue)],
        column_of: &ColumnIndex,
        slot_of: &HashMap<&str, u32>,
    ) -> ProbeRecord {
        let mut used: HashSet<u32> = HashSet::new();
        let mut cols = Vec::new();
        let mut over = Vec::new();
        for (k, v) in attrs {
            let (ty, cv) = resolve_value(v);
            let slot = slot_of.get(k.as_str()).copied();
            match column_lookup(column_of, k, ty.to_u8()) {
                Some(cid) if used.insert(cid) => {
                    if let Some(slot) = slot {
                        cols.push((slot, ty.to_u8(), cv));
                    }
                }
                _ => {
                    if let Some(slot) = slot {
                        over.push((slot, v.clone()));
                    }
                }
            }
        }
        ProbeRecord { cols, over }
    }
}

/// A per-64-row popcount prefix over a presence bitmap's bytes: `out[w]` is the
/// number of set bits in the first `w` 8-byte words. Paired with
/// [`presence_rank`] it turns a `(column, row)` lookup into the column's dense
/// cell slot into an O(1) probe, so the columnar writer materializes a block's
/// values from source instead of holding every value for the whole batch (#682).
fn presence_word_prefix(bytes: &[u8]) -> Vec<u32> {
    let num_words = bytes.len().div_ceil(8);
    let mut prefix = Vec::with_capacity(num_words + 1);
    let mut acc = 0u32;
    prefix.push(0);
    for w in 0..num_words {
        let start = w * 8;
        let end = (start + 8).min(bytes.len());
        for &byte in &bytes[start..end] {
            acc += byte.count_ones();
        }
        prefix.push(acc);
    }
    prefix
}

/// The dense-cell slot of present row `local`: the count of set presence bits
/// before it, which is the number of present rows the column stored ahead of it.
/// `prefix` is [`presence_word_prefix`] of the same `bytes`. The caller must have
/// checked `local` is present.
fn presence_rank(prefix: &[u32], bytes: &[u8], local: usize) -> usize {
    let w = local / 64;
    let mut rank = prefix[w] as usize;
    let byte_start = w * 8;
    let full_bytes = (local % 64) / 8;
    for k in 0..full_bytes {
        rank += bytes[byte_start + k].count_ones() as usize;
    }
    let rem = local % 8;
    if rem != 0 {
        rank += (bytes[byte_start + full_bytes] & ((1u8 << rem) - 1)).count_ones() as usize;
    }
    rank
}

/// The `row_estimate` contribution of one in-budget columnar cell, read from the
/// source attribute without resolving (copying) its value. Matches the value
/// sizing [`row_estimate`] applies to a [`ColumnValue`]: string/bytes columns
/// count their byte length plus two, every numeric column a flat eight, and a
/// `List`/`Map` its canonical `Bytes` encoding (the only case that must encode).
fn columnar_estimate(cell: &ravel_types::logstream::AttrValue) -> usize {
    use ravel_types::logstream::AttrValue;
    match cell {
        AttrValue::Str(s) => s.len() + 2,
        AttrValue::Bytes(b) => b.len() + 2,
        AttrValue::I64(_) | AttrValue::F64(_) | AttrValue::Bool(_) => 8,
        other => canonical_value_bytes(other).len() + 2,
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

/// The BLOCKS layout a build emits (ADR-0699 decision 1): row groups of
/// `group_target_blocks` consecutive blocks, pages column-major inside a group.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Layout {
    group_target_blocks: usize,
}

/// Places encoded blocks into the BLOCKS section and builds the PAGE_DIR that
/// locates their pages.
///
/// One row group's blocks are buffered before any of them is placed: within a
/// group the pages are stored grouped by column then by block, so a column's
/// pages for the whole group are contiguous (a *column chunk*) and the chunks
/// follow each other in `column_id` order (ADR-0699 decision 1). That buffer is
/// the writer's entire extra working set over the one-block-of-cells peak #682
/// established, and `group_target_blocks` bounds it: at most that many blocks'
/// *encoded, already-compressed* pages are resident at once, plus the blocks'
/// stats.
///
/// `block_offset`/`block_len` in a SKIP_IDX level-0 entry describe the block's
/// page span -- from its first page's offset to the end of its last -- which is
/// a superset range containing every one of its pages, not an exact extent.
/// Nothing locates a page through it; PAGE_DIR does that.
pub struct BlocksBuilder {
    layout: Layout,
    /// The BLOCKS section bytes built so far. Offsets recorded in PAGE_DIR and
    /// in SKIP_IDX are relative to its start.
    bytes: Vec<u8>,
    dir: PageDir,
    /// The current row group's encoded blocks, not yet placed.
    pending: Vec<BlockWriteOut>,
    /// Level-0 entries in block order, complete once the block's group flushed.
    l0: Vec<Level0Entry>,
}

impl BlocksBuilder {
    /// A layout builder at the given row group size, for a caller assembling
    /// BLOCKS out of blocks it encoded itself.
    ///
    /// The writer uses this internally. It is public because a hand-built
    /// object (a test fixture, a tool) has to produce BLOCKS bytes, SKIP_IDX
    /// level-0 entries, and a PAGE_DIR that agree with each other, and
    /// re-deriving the placement rule outside this module is exactly how the
    /// three drift apart.
    pub fn version_4(group_target_blocks: usize) -> Self {
        BlocksBuilder::new(Layout {
            group_target_blocks,
        })
    }

    fn new(layout: Layout) -> Self {
        BlocksBuilder {
            layout,
            bytes: Vec::new(),
            dir: PageDir::default(),
            pending: Vec::new(),
            l0: Vec::new(),
        }
    }

    /// Adds one encoded block. The block is buffered and placed when its row
    /// group fills.
    pub fn push(&mut self, out: BlockWriteOut) {
        self.pending.push(out);
        if self.pending.len() >= self.layout.group_target_blocks.max(1) {
            self.flush_group();
        }
    }

    /// Places the buffered row group column-major and records its PAGE_DIR
    /// entry. A group shorter than `group_target_blocks` (the object's last, or
    /// its only) is laid out identically, so a small flush object pays nothing
    /// for the level.
    fn flush_group(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        if pending.is_empty() {
            return;
        }
        let first_block = self.l0.len() as u32;
        let block_count = pending.len();

        // Per block, the byte offset of each of its pages inside its own
        // payload, so a page can be sliced out of it while placing the chunk.
        let payload_offsets: Vec<Vec<u64>> = pending
            .iter()
            .map(|out| {
                let mut at = 0u64;
                out.descs
                    .iter()
                    .map(|d| {
                        let start = at;
                        at += d.len;
                        start
                    })
                    .collect()
            })
            .collect();

        // Column chunks, in ascending column_id order. `BTreeMap` (never
        // `HashMap`) so identical input yields byte-identical output, the same
        // determinism rule the rest of this pipeline follows.
        let mut by_column: BTreeMap<u32, Vec<(usize, usize)>> = BTreeMap::new();
        for (bi, out) in pending.iter().enumerate() {
            for (di, d) in out.descs.iter().enumerate() {
                by_column.entry(d.column_id).or_default().push((bi, di));
            }
        }

        // Every block's page span, for its SKIP_IDX level-0 byte range, and its
        // version-4 crc, accumulated as its pages are placed. Chunks are
        // visited in ascending column_id order and a chunk's pages in ascending
        // block order, so each block's pages are folded in exactly the order
        // PAGE_DIR lists them, which is the order ADR-0699 decision 2 defines
        // the block crc over. That is NOT the order `BlockWriteOut::payload`
        // holds them in: the fixed columns are staged in field order, so
        // `flags` (column 8) is staged before `severity_text` (column 4).
        let mut span_start = vec![u64::MAX; block_count];
        let mut span_end = vec![0u64; block_count];
        let mut block_crc = vec![0u32; block_count];

        let mut chunks = Vec::with_capacity(by_column.len());
        for (column_id, entries) in by_column {
            let offset = self.bytes.len() as u64;
            let mut pages = Vec::with_capacity(entries.len());
            for (bi, di) in entries {
                let out = &pending[bi];
                let desc = out.descs[di];
                let at = self.bytes.len() as u64;
                let from = payload_offsets[bi][di] as usize;
                let to = from + desc.len as usize;
                let stored = &out.payload[from..to];
                self.bytes.extend_from_slice(stored);
                span_start[bi] = span_start[bi].min(at);
                span_end[bi] = span_end[bi].max(at + desc.len);
                block_crc[bi] = crc32c::crc32c_append(block_crc[bi], stored);
                pages.push(PageEntry {
                    block: bi as u32,
                    enc: desc.enc,
                    comp: desc.comp,
                    len: desc.len,
                    uncomp_len: desc.uncomp_len,
                    crc32c: crc32c::crc32c(stored),
                });
            }
            chunks.push(ChunkEntry {
                column_id,
                offset,
                pages,
            });
        }

        for (bi, out) in pending.iter().enumerate() {
            // A block always carries at least its ts page, so its span is
            // always real; guard anyway rather than underflow on an empty one.
            let (start, end) = if span_start[bi] == u64::MAX {
                (self.bytes.len() as u64, self.bytes.len() as u64)
            } else {
                (span_start[bi], span_end[bi])
            };
            self.l0.push(level0(out, start, end - start, block_crc[bi]));
        }

        self.dir.groups.push(GroupEntry {
            first_block,
            block_count: block_count as u32,
            chunks,
        });
    }

    /// The finished BLOCKS bytes, the level-0 entries, and the PAGE_DIR (empty
    /// under version 3, where no such section is written).
    pub fn finish(mut self) -> (Vec<u8>, Vec<Level0Entry>, PageDir) {
        self.flush_group();
        (self.bytes, self.l0, self.dir)
    }
}

/// The SKIP_IDX level-0 entry for one encoded block.
fn level0(
    out: &BlockWriteOut,
    block_offset: u64,
    block_len: u64,
    block_crc32c: u32,
) -> Level0Entry {
    Level0Entry {
        block_offset,
        block_len,
        block_crc32c,
        record_count: out.record_count,
        min_ts: out.min_ts,
        max_ts: out.max_ts,
        min_stream_ref: out.min_stream_ref,
        max_stream_ref: out.max_stream_ref,
        stats: out.stats.clone(),
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

/// The row-group buffer's block bound (ADR-0699 consequences), pinned directly
/// on the buffer rather than inferred from the object.
/// `tests/rlog_v4_row_group_working_set.rs` pins the byte size that bound
/// implies.
#[cfg(test)]
#[allow(clippy::expect_used)]
mod row_group_buffer {
    use super::*;
    use crate::page::PageDesc;

    fn block(payload_len: usize, columns: &[u32]) -> BlockWriteOut {
        let each = payload_len / columns.len().max(1);
        BlockWriteOut {
            descs: columns
                .iter()
                .map(|c| PageDesc {
                    column_id: *c,
                    enc: crate::encoding::Enc::Plain,
                    comp: 0,
                    len: each as u64,
                    uncomp_len: each as u64,
                })
                .collect(),
            payload: vec![7u8; each * columns.len()],
            record_count: 8,
            stats: Vec::new(),
            min_ts: 0,
            max_ts: 1,
            min_stream_ref: 0,
            max_stream_ref: 0,
        }
    }

    #[test]
    fn never_holds_more_than_group_target_blocks() {
        for group in [1usize, 3, 32] {
            let mut builder = BlocksBuilder::new(Layout {
                group_target_blocks: group,
            });
            for _ in 0..(3 * group + 1) {
                builder.push(block(64, &[0, 4, 9]));
                assert!(
                    builder.pending.len() < group,
                    "buffer held {} blocks at a {group}-block group target",
                    builder.pending.len()
                );
            }
            let (_, l0, dir) = builder.finish();
            assert_eq!(l0.len(), 3 * group + 1);
            assert_eq!(dir.block_count(), l0.len() as u64);
            for g in &dir.groups {
                assert!(
                    g.block_count as usize <= group,
                    "row group of {} blocks over the {group}-block target",
                    g.block_count
                );
            }
        }
    }

    /// A zero group target is treated as one block per group rather than
    /// buffering the whole object: the config is a target, and a nonsense value
    /// must not turn the bounded working set into an unbounded one.
    #[test]
    fn zero_group_target_does_not_buffer_the_object() {
        let mut builder = BlocksBuilder::new(Layout {
            group_target_blocks: 0,
        });
        for _ in 0..5 {
            builder.push(block(64, &[0]));
            assert!(builder.pending.is_empty());
        }
        let (_, l0, dir) = builder.finish();
        assert_eq!(l0.len(), 5);
        assert_eq!(dir.groups.len(), 5);
    }
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
    fn dynamic_column_budget_past_decoder_cap_is_refused() {
        // The exact boundary: a config carrying `MAX_DYNAMIC_COLUMNS` dynamic
        // columns can assign at most FIRST_DYNAMIC_COL + MAX_DYNAMIC_COLUMNS - 1,
        // which is exactly `block::MAX_COLUMN_ID`, the largest id
        // `decode_v4_block` accepts. One past it would assign an id the decoder
        // rejects, so the object could not be read back.
        let at_cap = MAX_DYNAMIC_COLUMNS as usize;
        assert_eq!(
            FIRST_DYNAMIC_COL as u64 + at_cap as u64 - 1,
            crate::block::MAX_COLUMN_ID,
            "at-cap budget assigns exactly the largest id the decoder accepts"
        );

        // A one-record object assigns only one dynamic column, so neither config
        // actually reaches a high id; the refusal is a property of the config
        // and fires before any column is assigned. A config at the cap is
        // accepted, one past it is refused with the typed limit error.
        let record = || {
            let mut r = base_record(0, 0);
            r.attrs.push(("k".into(), AttrValue::I64(1)));
            r
        };

        let mut ok = RlogWriter::new(
            RlogConfig {
                max_dynamic_columns: at_cap,
                ..RlogConfig::default()
            },
            identity(),
        );
        ok.push(record()).expect("push");
        ok.finish()
            .expect("a config at the decoder cap is accepted");

        let mut over = RlogWriter::new(
            RlogConfig {
                max_dynamic_columns: at_cap + 1,
                ..RlogConfig::default()
            },
            identity(),
        );
        over.push(record()).expect("push");
        let err = over
            .finish()
            .expect_err("a config one past the decoder cap is refused");
        assert!(
            matches!(err, LogSegError::LimitExceeded(_)),
            "expected LimitExceeded, got {err:?}"
        );
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

    /// Both build paths grant the identical set of stream-level-only dynamic
    /// columns. The grant rule (`stream_level_column_eligible`) is shared, so
    /// the row path and the columnar path cannot diverge on which
    /// resource/scope-only key earns a column. The fixture puts four keys only
    /// on the stream blob -- no record carries them per-record, so every
    /// FIELD_DIR entry is a stream-level grant -- covering each arm of the rule:
    ///
    /// - `idx.str`   Str, indexed     -> granted (indexed, non-grantable type)
    /// - `both.i64`  I64, indexed     -> granted (indexed and numeric)
    /// - `num.i64`   I64, not indexed -> granted (grantable type, not indexed)
    /// - `plain.str` Str, not indexed -> denied (neither)
    ///
    /// so the granted set is exactly {idx.str/Str, both.i64/I64, num.i64/I64}
    /// on both paths. Flip either call site back to a diverging inline rule
    /// (drop the `indexed_names` clause on one, say) and the two sets stop
    /// matching.
    #[test]
    fn both_build_paths_grant_same_stream_level_columns() {
        let blob = stream_attrs_bytes(
            &[
                ("idx.str".into(), AttrValue::Str("x".into())),
                ("both.i64".into(), AttrValue::I64(7)),
                ("num.i64".into(), AttrValue::I64(3)),
                ("plain.str".into(), AttrValue::Str("y".into())),
            ],
            "scope",
            "1.0",
            &[],
        );
        let indexed = vec!["idx.str".to_string(), "both.i64".to_string()];
        // Every record carries the same stream blob and no per-record attrs, so
        // the only columns the object can grant are the stream-level ones.
        let records: Vec<LogRecord> = (0..6i64)
            .map(|i| {
                let mut r = base_record(0, i);
                r.stream_attrs = blob.clone();
                r
            })
            .collect();

        let granted = |obj: &[u8]| -> Vec<(String, u8)> {
            let footer = open(obj).expect("open");
            let fd = read_field_dir(obj, &footer);
            let mut set: Vec<(String, u8)> = fd
                .entries()
                .iter()
                .map(|e| (e.name.clone(), e.ty.to_u8()))
                .collect();
            set.sort();
            set
        };

        let mut rw =
            RlogWriter::new(RlogConfig::default(), identity()).with_indexed_fields(indexed.clone());
        for r in &records {
            rw.push(r.clone()).expect("row push");
        }
        let row_obj = rw.finish().expect("row finish");

        let mut cw =
            RlogWriter::new(RlogConfig::default(), identity()).with_indexed_fields(indexed);
        cw.push_columnar(ColumnarLogBatch::from_records(&records))
            .expect("columnar push");
        let col_obj = cw.finish().expect("columnar finish");

        let mut expected: Vec<(String, u8)> = vec![
            ("idx.str".to_string(), FieldType::Str.to_u8()),
            ("both.i64".to_string(), FieldType::I64.to_u8()),
            ("num.i64".to_string(), FieldType::I64.to_u8()),
        ];
        expected.sort();
        assert_eq!(granted(&row_obj), expected, "row path granted set");
        assert_eq!(granted(&col_obj), expected, "columnar path granted set");
        assert_eq!(
            granted(&row_obj),
            granted(&col_obj),
            "both paths grant the identical stream-level column set"
        );
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

        /// Like [`columnar_object`], but attaches the dictionary shape to every
        /// batch ([`ColumnarLogBatch::with_dictionaries`]) so the writer takes
        /// the per-distinct dictionary encode and bloom path (ADR-0109
        /// decision 3, #603).
        fn columnar_object_dict(
            cfg: RlogConfig,
            recs: &[LogRecord],
            nbatches: usize,
        ) -> (Vec<u8>, WriteStats) {
            let mut w = RlogWriter::new(cfg, identity()).with_indexed_fields(indexed());
            for chunk in split(recs, nbatches) {
                if chunk.is_empty() {
                    continue;
                }
                w.push_columnar(ColumnarLogBatch::from_records(chunk).with_dictionaries())
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

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// The #603 anchor: pushing the same records through the row path and
            /// through the columnar path with dictionary-shaped string columns
            /// yields byte-identical objects and field-identical `WriteStats`.
            ///
            /// Each record gets three controlled string columns so the encoder's
            /// dict-versus-plain threshold (`dict_is_worth_it`) is straddled
            /// within a block: `same` is one identical value across every row
            /// (distinct = 1, dictionary wins), `uniq` is a distinct value per
            /// row (dictionary loses, plain wins), and `lc` draws from a 1-2
            /// value pool (dictionary wins). `bin` is a low-cardinality `Bytes`
            /// column (dictionary-encoded but never bloomed). The `arb_record`
            /// attributes still bring `List`/`Map`-derived `Bytes` columns,
            /// duplicate keys, per-type splitting, and overflow.
            #[test]
            fn dictionary_columns_match_row_build_byte_for_byte(
                mut records in proptest::collection::vec(arb_record(), 1..25),
                max_dyn in 6usize..16,
                nbatches in 1usize..4,
                lc_pool in proptest::collection::vec("[a-z]{1,3}", 1..3),
            ) {
                for (i, r) in records.iter_mut().enumerate() {
                    r.attrs.push(("same".into(), AttrValue::Str("K".into())));
                    r.attrs.push(("uniq".into(), AttrValue::Str(format!("u{i}"))));
                    r.attrs
                        .push(("lc".into(), AttrValue::Str(lc_pool[i % lc_pool.len()].clone())));
                    r.attrs
                        .push(("bin".into(), AttrValue::Bytes(vec![(i % 2) as u8])));
                }
                let cfg = RlogConfig {
                    max_dynamic_columns: max_dyn,
                    block_target_records: 5,
                    block_max_bytes: 8192,
                    ..RlogConfig::default()
                };
                let (rb, rs) = row_object(cfg, &records);
                let (cb, cs) = columnar_object_dict(cfg, &records, nbatches);
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

    /// The issue #1135 anchor: the one-pass slot-keyed stamp
    /// ([`StampScratch::finish`]) against a reference copy of the name-keyed
    /// path it replaced, over random records and over the 104-declared-column
    /// shape the issue profiled.
    ///
    /// The reference below is the pre-#1135 code from `writer.rs` at 7c0c337,
    /// verbatim. It is a frozen copy, not code to maintain: tidying it, or
    /// letting it drift toward the new implementation, is what would make this
    /// test stop meaning anything. (`benches/common/stamp_shape.rs` holds the
    /// same copy for the benchmark and the allocation pin, which compile
    /// against the crate from outside and cannot reach this module.)
    mod stamp_differential {
        use super::*;
        use proptest::prelude::*;
        use stamp_probe::{ProbeRecord, StampProbe};

        type TrackedValues = Vec<(String, (u8, ColumnValue))>;
        /// A stamp's two projections: POSTINGS terms and NumStat winners.
        type StampOutputs = (Vec<(u32, ColumnValue)>, Vec<(u32, ColumnValue)>);

        // ---- reference copy of the pre-#1135 path (frozen) ----------------

        fn record_level_winners_slot(
            cols: &[(u32, u8, ColumnValue)],
            overflow: &[(u32, AttrValue)],
            names: &[&str],
        ) -> BTreeMap<String, (u8, ColumnValue)> {
            let mut slots: BTreeSet<u32> = BTreeSet::new();
            for (slot, _, _) in cols {
                slots.insert(*slot);
            }
            for (slot, _) in overflow {
                slots.insert(*slot);
            }

            let mut out: BTreeMap<String, (u8, ColumnValue)> = BTreeMap::new();
            for slot in slots {
                let mut combined: Vec<(u8, ColumnValue)> = Vec::new();
                let mut cs: Vec<(u8, ColumnValue)> = cols
                    .iter()
                    .filter(|(s, _, _)| *s == slot)
                    .map(|(_, ty, cv)| (*ty, cv.clone()))
                    .collect();
                cs.sort_by_key(|(ty, _)| *ty);
                combined.extend(cs);
                let mut keyed: Vec<(Vec<u8>, (u8, ColumnValue))> = overflow
                    .iter()
                    .filter(|(s, _)| *s == slot)
                    .map(|(_, v)| {
                        let (ty, cv) = resolve_value(v);
                        (canonical_value_bytes(v), (ty.to_u8(), cv))
                    })
                    .collect();
                keyed.sort_by(|a, b| a.0.cmp(&b.0));
                combined.extend(keyed.into_iter().map(|(_, entry)| entry));
                if let Some(winner) = combined.pop() {
                    out.insert(names[slot as usize].to_string(), winner);
                }
            }
            out
        }

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

        fn stat_winner_columns(
            resolved: &[(String, (u8, ColumnValue))],
            numstat_names: &HashSet<&str>,
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

        fn indexed_term_columns(
            resolved: &[(String, (u8, ColumnValue))],
            indexed_names: &HashSet<&str>,
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

        // ---- driving both paths over the same inputs ----------------------

        /// One object's stamp inputs.
        struct Setup {
            columns: Vec<(String, FieldType, u32)>,
            indexed_fields: Vec<String>,
            stream_attrs: Vec<(String, AttrValue)>,
            records: Vec<Vec<(String, AttrValue)>>,
        }

        /// The per-object state the old path built once per object.
        struct Reference<'a> {
            names: Vec<&'a str>,
            seed: TrackedValues,
            indexed_names: HashSet<&'a str>,
            numstat_names: HashSet<&'a str>,
            column_of: ColumnIndex,
        }

        impl<'a> Reference<'a> {
            fn new(setup: &'a Setup) -> Self {
                let mut column_of: ColumnIndex = HashMap::new();
                for (name, ty, cid) in &setup.columns {
                    column_of
                        .entry(ty.to_u8())
                        .or_default()
                        .insert(name.clone(), *cid);
                }
                let indexed_names: HashSet<&str> =
                    setup.indexed_fields.iter().map(String::as_str).collect();
                let numstat_names: HashSet<&str> = setup
                    .columns
                    .iter()
                    .filter(|(_, ty, _)| {
                        matches!(ty, FieldType::I64 | FieldType::F64 | FieldType::Bool)
                    })
                    .map(|(name, _, _)| name.as_str())
                    .collect();
                let mut names: Vec<&str> = indexed_names
                    .iter()
                    .copied()
                    .chain(numstat_names.iter().copied())
                    .collect();
                names.sort_unstable();
                names.dedup();

                let mut seed: TrackedValues = Vec::new();
                for (k, v) in &setup.stream_attrs {
                    if indexed_names.contains(k.as_str()) || numstat_names.contains(k.as_str()) {
                        let (ty, cv) = resolve_value(v);
                        seed.push((k.clone(), (ty.to_u8(), cv)));
                    }
                }
                Reference {
                    names,
                    seed,
                    indexed_names,
                    numstat_names,
                    column_of,
                }
            }

            fn stamp(&self, rec: &ProbeRecord) -> StampOutputs {
                let winners =
                    record_level_winners_slot(rec.columnar(), rec.overflow(), &self.names);
                let resolved = resolved_tracked_values(&self.seed, &winners);
                (
                    indexed_term_columns(&resolved, &self.indexed_names, &self.column_of),
                    stat_winner_columns(&resolved, &self.numstat_names, &self.column_of),
                )
            }
        }

        /// Every record's `(old, new)` stamp outputs.
        fn old_and_new(setup: &Setup) -> Vec<(StampOutputs, StampOutputs)> {
            let reference = Reference::new(setup);
            let mut probe = StampProbe::new(
                &setup.columns,
                &setup.indexed_fields,
                &setup.stream_attrs,
                &setup.records,
            );
            let mut out = Vec::with_capacity(probe.len());
            for i in 0..probe.len() {
                let old = reference.stamp(probe.record(i).expect("record in range"));
                probe.stamp(i);
                let (indexed, stat) = probe.outputs();
                out.push((old, (indexed.to_vec(), stat.to_vec())));
            }
            out
        }

        /// The per-column min and max the block's NumStat accumulator folds a
        /// column's winners into, over every record at once. Equal winners
        /// imply equal stamps, but the issue asks for the stamps themselves, so
        /// they are folded here rather than left implied.
        fn numstat_min_max(
            per_record: impl Iterator<Item = Vec<(u32, ColumnValue)>>,
        ) -> BTreeMap<u32, (ColumnValue, ColumnValue)> {
            let mut out: BTreeMap<u32, (ColumnValue, ColumnValue)> = BTreeMap::new();
            for stat in per_record {
                for (cid, v) in stat {
                    match out.entry(cid) {
                        Entry::Vacant(e) => {
                            e.insert((v.clone(), v));
                        }
                        Entry::Occupied(mut e) => {
                            let (lo, hi) = e.get_mut();
                            if numeric_lt(&v, lo) {
                                *lo = v.clone();
                            }
                            if numeric_lt(hi, &v) {
                                *hi = v;
                            }
                        }
                    }
                }
            }
            out
        }

        /// Numeric ordering of two NumStat-eligible values of the same column,
        /// so the fold above is total (`f64::total_cmp` orders NaN too).
        fn numeric_lt(a: &ColumnValue, b: &ColumnValue) -> bool {
            match (a, b) {
                (ColumnValue::I64(x), ColumnValue::I64(y)) => x < y,
                (ColumnValue::F64(x), ColumnValue::F64(y)) => {
                    f64::from_bits(*x).total_cmp(&f64::from_bits(*y)).is_lt()
                }
                (ColumnValue::Bool(x), ColumnValue::Bool(y)) => !x & y,
                _ => false,
            }
        }

        // ---- the 104-declared-column shape --------------------------------

        const DECLARED_COLUMNS: usize = 104;
        /// The names a record carries twice: once columnar, once in
        /// `attrs_raw`, so the overflow tier decides the winner.
        const DUPLICATED: [usize; 3] = [7, 23, 61];

        fn column_name(i: usize) -> String {
            format!("col_{i:03}")
        }

        fn column_type(i: usize) -> FieldType {
            match i % 8 {
                5 => FieldType::F64,
                6 => FieldType::Bool,
                _ => FieldType::I64,
            }
        }

        fn declared_value(r: usize, i: usize) -> AttrValue {
            match column_type(i) {
                FieldType::F64 => AttrValue::F64(r as f64 + i as f64 / 128.0),
                FieldType::Bool => AttrValue::Bool((r + i).is_multiple_of(2)),
                _ => AttrValue::I64((r * DECLARED_COLUMNS + i) as i64),
            }
        }

        /// The ClickBench-like shape the issue profiled: about a hundred
        /// declared columns per record, almost all numeric, a quarter of them
        /// also indexed, three names duplicated on the record, one occurrence
        /// whose type has no column, and a stream layer carrying two names of
        /// its own plus a duplicate entry.
        fn wide_setup(n_records: usize) -> Setup {
            let mut columns: Vec<(String, FieldType, u32)> = (0..DECLARED_COLUMNS)
                .map(|i| (column_name(i), column_type(i), i as u32))
                .collect();
            columns.push((
                "stream_only_0".to_string(),
                FieldType::I64,
                DECLARED_COLUMNS as u32,
            ));
            columns.push((
                "stream_only_1".to_string(),
                FieldType::I64,
                DECLARED_COLUMNS as u32 + 1,
            ));

            let stream_attrs = vec![
                ("service.name".to_string(), AttrValue::Str("bench".into())),
                ("stream_only_0".to_string(), AttrValue::I64(-1)),
                ("stream_only_1".to_string(), AttrValue::I64(-2)),
                (column_name(0), AttrValue::I64(-3)),
                ("stream_only_0".to_string(), AttrValue::I64(-4)),
            ];

            let records = (0..n_records)
                .map(|r| {
                    let mut attrs = Vec::with_capacity(DECLARED_COLUMNS + 4);
                    for i in 0..DECLARED_COLUMNS {
                        attrs.push((column_name(i), declared_value(r, i)));
                    }
                    for i in DUPLICATED {
                        attrs.push((column_name(i), declared_value(r + 1, i)));
                    }
                    attrs.push((column_name(42), AttrValue::F64(r as f64 + 0.5)));
                    attrs
                })
                .collect();

            Setup {
                columns,
                indexed_fields: (0..DECLARED_COLUMNS).step_by(4).map(column_name).collect(),
                stream_attrs,
                records,
            }
        }

        /// Exact expected winners on the 104-column shape, stated here rather
        /// than read off either implementation.
        #[test]
        fn wide_shape_pins_exact_winners() {
            let setup = wide_setup(1);
            let mut probe = StampProbe::new(
                &setup.columns,
                &setup.indexed_fields,
                &setup.stream_attrs,
                &setup.records,
            );
            assert_eq!(probe.tracked_names().len(), DECLARED_COLUMNS + 2);
            probe.stamp(0);
            let (indexed, stat) = probe.outputs();

            // The record's winner for a name it carries once is that
            // occurrence; for a duplicated name it is the `attrs_raw`
            // occurrence, which outranks every columnar one.
            let want = |i: usize| {
                let r = usize::from(DUPLICATED.contains(&i));
                resolve_value(&declared_value(r, i)).1
            };

            // Stream seed order first (`stream_only_0`, `stream_only_1`,
            // `col_000`), then the record-only winners ascending by slot, which
            // is ascending by name. `col_042`'s winner is its `attrs_raw` F64
            // occurrence, and the name has no F64 column, so it contributes no
            // NumStat entry at all. The duplicate `stream_only_0` seed entry
            // contributes none either: NumStat takes the first entry per name.
            let mut want_stat: Vec<(u32, ColumnValue)> = vec![
                (DECLARED_COLUMNS as u32, ColumnValue::I64(-1)),
                (DECLARED_COLUMNS as u32 + 1, ColumnValue::I64(-2)),
                (0, want(0)),
            ];
            want_stat.extend(
                (1..DECLARED_COLUMNS)
                    .filter(|i| *i != 42)
                    .map(|i| (i as u32, want(i))),
            );
            assert_eq!(stat, want_stat.as_slice());
            assert_eq!(stat.len(), DECLARED_COLUMNS + 1);

            // Every fourth column is indexed; `col_000` is stamped from its
            // seed position, the rest in slot order.
            let want_indexed: Vec<(u32, ColumnValue)> = (0..DECLARED_COLUMNS)
                .step_by(4)
                .map(|i| (i as u32, want(i)))
                .collect();
            assert_eq!(indexed, want_indexed.as_slice());
            assert_eq!(indexed.len(), 26);

            // The seed value for `col_000` (-3) lost to the record, and the
            // duplicated names took their `attrs_raw` value.
            assert_eq!(stat[2], (0, ColumnValue::I64(0)));
            assert_eq!(stat[9], (7, ColumnValue::I64(DECLARED_COLUMNS as i64 + 7)));

            // And the old path agrees, on the shape and on both projections.
            let reference = Reference::new(&setup);
            let (old_indexed, old_stat) = reference.stamp(probe.record(0).expect("record 0"));
            assert_eq!(old_indexed, want_indexed);
            assert_eq!(old_stat, want_stat);
        }

        #[test]
        fn wide_shape_matches_reference_over_many_records() {
            let setup = wide_setup(64);
            let pairs = old_and_new(&setup);
            assert_eq!(pairs.len(), 64);
            for (i, (old, new)) in pairs.iter().enumerate() {
                assert_eq!(old.0, new.0, "postings terms, record {i}");
                assert_eq!(old.1, new.1, "numstat winners, record {i}");
            }
            let old_stats = numstat_min_max(pairs.iter().map(|(old, _)| old.1.clone()));
            let new_stats = numstat_min_max(pairs.iter().map(|(_, new)| new.1.clone()));
            assert_eq!(old_stats, new_stats);
            assert_eq!(old_stats.len(), DECLARED_COLUMNS + 1);
        }

        // ---- the differential property ------------------------------------

        fn arb_value() -> impl Strategy<Value = AttrValue> {
            let leaf = prop_oneof![
                any::<i64>().prop_map(AttrValue::I64),
                any::<f64>().prop_map(AttrValue::F64),
                any::<bool>().prop_map(AttrValue::Bool),
                "[a-z]{0,3}".prop_map(AttrValue::Str),
                proptest::collection::vec(any::<u8>(), 0..3).prop_map(AttrValue::Bytes),
            ];
            leaf.prop_recursive(2, 4, 2, |inner| {
                prop_oneof![
                    proptest::collection::vec(inner.clone(), 0..2).prop_map(AttrValue::List),
                    proptest::collection::vec(("[a-z]{1,2}", inner), 0..2).prop_map(AttrValue::Map),
                ]
            })
        }

        /// A name index, drawn either from the whole pool (so wide records
        /// reach many slots) or from its first few entries (so one record
        /// carries the same name several times).
        fn arb_name_idx() -> impl Strategy<Value = usize> {
            prop_oneof![0usize..128, 0usize..5]
        }

        fn arb_attrs(max: usize) -> impl Strategy<Value = Vec<(usize, AttrValue)>> {
            proptest::collection::vec((arb_name_idx(), arb_value()), 0..max)
        }

        fn name_of(idx: usize, pool: usize) -> String {
            format!("n{:03}", idx % pool)
        }

        /// Assigns dynamic columns the way the writer does: one column per
        /// distinct `(name, type)` in `(name, type)`-ascending order, until the
        /// budget runs out. Everything past the budget has no column, so its
        /// occurrences fold into `attrs_raw`.
        fn assign_columns(
            stream_attrs: &[(String, AttrValue)],
            records: &[Vec<(String, AttrValue)>],
            max_dyn: usize,
        ) -> Vec<(String, FieldType, u32)> {
            let mut distinct: BTreeSet<(String, u8)> = BTreeSet::new();
            for (k, v) in stream_attrs.iter().chain(records.iter().flatten()) {
                distinct.insert((k.clone(), resolve_value(v).0.to_u8()));
            }
            distinct
                .into_iter()
                .take(max_dyn)
                .enumerate()
                .filter_map(|(cid, (name, ty_byte))| {
                    FieldType::from_u8(ty_byte).map(|ty| (name, ty, cid as u32))
                })
                .collect()
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// The one-pass slot-keyed stamp and the name-keyed path it
            /// replaced produce the same POSTINGS terms, the same NumStat
            /// winners, and the same folded min/max, over records reaching up
            /// to 128 tracked slots, names duplicated within a record, values
            /// of every kind (including `List`/`Map`, which canonicalize into
            /// `Bytes`), occurrences with no column at all, and a stream layer
            /// that seeds names the records do not carry, carries a name they
            /// do, and repeats one of its own.
            #[test]
            fn slot_keyed_stamp_matches_name_keyed_reference(
                pool in 1usize..=128,
                max_dyn in 1usize..200,
                stream in arb_attrs(6),
                indexed_idx in proptest::collection::vec(arb_name_idx(), 0..8),
                raw_records in proptest::collection::vec(arb_attrs(12), 1..8),
            ) {
                let stream_attrs: Vec<(String, AttrValue)> = stream
                    .into_iter()
                    .map(|(idx, v)| (name_of(idx, pool), v))
                    .collect();
                let records: Vec<Vec<(String, AttrValue)>> = raw_records
                    .into_iter()
                    .map(|attrs| attrs.into_iter().map(|(idx, v)| (name_of(idx, pool), v)).collect())
                    .collect();
                let columns = assign_columns(&stream_attrs, &records, max_dyn);
                let mut indexed_fields: Vec<String> = indexed_idx
                    .into_iter()
                    .map(|idx| name_of(idx, pool))
                    .collect();
                indexed_fields.sort();
                indexed_fields.dedup();

                let setup = Setup { columns, indexed_fields, stream_attrs, records };
                let pairs = old_and_new(&setup);
                for (i, (old, new)) in pairs.iter().enumerate() {
                    prop_assert_eq!(&old.0, &new.0, "postings terms, record {}", i);
                    prop_assert_eq!(&old.1, &new.1, "numstat winners, record {}", i);
                }
                prop_assert_eq!(
                    numstat_min_max(pairs.iter().map(|(old, _)| old.1.clone())),
                    numstat_min_max(pairs.iter().map(|(_, new)| new.1.clone()))
                );
            }
        }
    }
}
