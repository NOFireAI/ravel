//! ADR-0873 wave 5a, ingest half: the log shard actor accumulates the exact
//! whole-object min/max and null count of each stamp-eligible declared column
//! (I64 and BOOL only, decision 2) as records land in a tenant's flush buffer,
//! then stamps them onto that flush's `CommitRecord` beside `sample_count`.
//!
//! The extrema are captured in-stream, O(1) per record per attribute, never by
//! a second pass over the buffered records at flush time: each buffered write
//! folds its records' eligible attributes into [`DeclaredStatAccum`] exactly
//! where the write already visits them, the same shape `sample_count` and the
//! `#983` flush-trigger counts are accumulated. Deriving the stamp from the
//! records rather than from `RlogWriter`'s per-block statistics (the ADR
//! decision 3 sketch) keeps this half inside `ravel-ingest`/`ravel-commit` and
//! makes the whole-object extremum exact regardless of how the writer laid the
//! column out, at the cost of one running triple per eligible attribute the
//! buffer has seen.
//!
//! Null semantics match the stamp reader's contract exactly
//! ([`ravel_commit::declared_stats`], ADR-0873 clause 4/5 and decision 3): a
//! row reads NULL for a declared column when the attribute is absent **or its
//! stored value is not of the declared type**. So the count is kept per
//! `(name, value kind)`: a declared I64 column's non-null rows are exactly the
//! rows whose RESOLVED value under that name is an I64, and a row that resolves
//! the same name to a BOOL (or does not resolve it at all) is a NULL for the
//! I64 column.
//! `null_count = sample_count - non_null` is derived at stamp time, so the
//! stamp is always internally consistent with the record's own row count and
//! its own reader never drops it (decision 3, "a flush must never emit a stamp
//! its own reader would drop").
//!
//! "The attribute" above is the one the reader resolves, which is the MERGED
//! view, not the record's own attribute set: a declared column's value for a
//! row is the record's attribute when the record sets that key (at any value
//! kind), and otherwise the row's stream-level resource or scope attribute of
//! the same name (`ravel_sql`'s `logs_scan::merged_value`). A fold over record
//! attributes alone stamps `null_count == sample_count` for a column whose
//! values live on the resource or scope attributes, and that affirmative
//! all-NULL statement is what the metadata-only aggregate path answers `NULL`
//! from (issue #1057). So each stream's attributes are decoded once, per
//! stream, and a row that does not set a declared key contributes its stream's
//! value instead of a NULL.
//!
//! Within the record layer, a record that carries one name more than once
//! resolves it to a single value, and which occurrence wins is fixed by
//! docs/log-segment-format.md ("Within the record layer"), NOT by the order the
//! record was written in. [`record_winner`] is this module's implementation of
//! that rule; the OTLP normalizer does not deduplicate attribute keys, so the
//! shape is reachable from malformed input (issue #1182).

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

use ravel_catalog::DeclaredColumnType;
use ravel_logseg::{ColumnarLogBatch, stream_attr_pairs};
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::declared_stats::{
    DeclaredColumnStat, DeclaredStatType, DeclaredStatValue, TYPED_ATTR_COLUMN_TYPE_BOOL,
    TYPED_ATTR_COLUMN_TYPE_BYTES, TYPED_ATTR_COLUMN_TYPE_I64, TYPED_ATTR_COLUMN_TYPE_STR,
};
use ravel_types::logstream::{AttrValue, LogStreamId};

/// The `ravel.sys.v1.TypedAttrColumnType` tag a declared column's type is
/// stored as, so a declared column reaches [`DeclaredStatAccum::build_stamps`]
/// as `(name, tag)` and the eligibility gate is the single-sourced allowlist in
/// [`ravel_types::declared_stats`] (decision 2), not a second copy of it. `F64`
/// has no [`DeclaredColumnType`] variant (it is unshipped, ADR-0090/0101), so a
/// declared F64 column cannot reach here from config at all; the gate refuses
/// its tag anyway, which is what the write-side float-gate test drives.
pub(crate) fn declared_type_tag(ty: DeclaredColumnType) -> u32 {
    match ty {
        DeclaredColumnType::Str => TYPED_ATTR_COLUMN_TYPE_STR,
        DeclaredColumnType::I64 => TYPED_ATTR_COLUMN_TYPE_I64,
        DeclaredColumnType::Bool => TYPED_ATTR_COLUMN_TYPE_BOOL,
        DeclaredColumnType::Bytes => TYPED_ATTR_COLUMN_TYPE_BYTES,
    }
}

/// Running min/max and non-null row count for one `(column, kind)` over the
/// non-null values seen so far. `min <= max` holds by construction (both start
/// at the first value and only widen), so a stamp built from it never trips the
/// reader's `min <= max` clause.
#[derive(Clone, Copy, Debug)]
struct Running<T> {
    min: T,
    max: T,
    non_null: u64,
}

impl<T: Ord + Copy> Running<T> {
    fn start(v: T) -> Self {
        Running {
            min: v,
            max: v,
            non_null: 1,
        }
    }

    fn observe(&mut self, v: T) {
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
        self.non_null += 1;
    }

    /// Merge a whole batch's `(min, max, count)` computed in one pass, for the
    /// columnar path where a dynamic column's cells are folded once rather than
    /// entry-by-entry. `count == 0` is never passed (an absent column produces
    /// no dynamic column at all).
    fn merge(&mut self, min: T, max: T, count: u64) {
        if min < self.min {
            self.min = min;
        }
        if max > self.max {
            self.max = max;
        }
        self.non_null += count;
    }
}

/// Per-column running extrema, split by observed value kind so the non-null
/// count of a declared column of one type is not inflated by same-named values
/// of the other type (which read NULL for that declaration).
///
/// A record contributes at most one value to at most one of the two, because
/// its record layer resolves the name to a single winning occurrence
/// ([`record_winner`]) before anything is folded.
#[derive(Default, Clone, Copy, Debug)]
struct ColumnAccum {
    i64: Option<Running<i64>>,
    boolean: Option<Running<bool>>,
}

/// A stream-level attribute value of a stamp-eligible kind, as the fallback a
/// row of that stream reads for a declared column it does not set itself.
#[derive(Clone, Copy, Debug, PartialEq)]
enum StreamValue {
    I64(i64),
    Bool(bool),
}

/// One stream-level attribute that can serve as a fallback, with the count of
/// this buffer's rows in that stream which override it by setting the same key.
#[derive(Clone, Copy, Debug)]
struct StreamAttrState {
    value: StreamValue,
    /// Rows of this stream in the buffer that set this key themselves, at any
    /// value kind. Those rows read the record's value (or NULL, if its kind
    /// does not match the declaration), never this fallback.
    overrides: u64,
}

/// One stream's contribution to the merged view: how many of the buffer's rows
/// belong to it, and the stream-level attributes those rows fall back to.
#[derive(Default)]
struct StreamState {
    rows: u64,
    /// Only the names whose first non-List/Map blob occurrence (resource set
    /// before scope set, the order the reader's `find_attr` resolves) is I64
    /// or BOOL. A name whose first non-List/Map occurrence is of any other
    /// kind reads NULL for a declaration either way, so it is dropped at
    /// decode time and costs nothing per record.
    attrs: HashMap<String, StreamAttrState>,
}

/// Decode one STREAM_DIR blob into the fallbacks its rows read, or `None` if
/// the blob does not decode.
fn stream_state(blob: &[u8]) -> Option<StreamState> {
    let pairs = stream_attr_pairs(blob).ok()?;
    let mut first: HashMap<String, Option<StreamValue>> = HashMap::with_capacity(pairs.len());
    for (name, value) in pairs {
        // The reader's decoder (`ravel_sql::rlog_attrs::decode_value`) never
        // pushes a List or Map entry into the pairs it resolves over, so such
        // an occurrence must not claim the key here either: skip it and let a
        // later occurrence of the same name compete.
        if matches!(value, AttrValue::List(_) | AttrValue::Map(_)) {
            continue;
        }
        // First occurrence whose value is not a List or Map wins, matching
        // the reader: an ineligible (but decoded) first occurrence shadows a
        // same-named eligible one behind it, so it must be recorded as
        // "claimed, no fallback".
        first.entry(name).or_insert(match value {
            AttrValue::I64(v) => Some(StreamValue::I64(v)),
            AttrValue::Bool(b) => Some(StreamValue::Bool(b)),
            _ => None,
        });
    }
    let attrs = first
        .into_iter()
        .filter_map(|(name, value)| {
            value.map(|value| {
                (
                    name,
                    StreamAttrState {
                        value,
                        overrides: 0,
                    },
                )
            })
        })
        .collect();
    Some(StreamState { rows: 0, attrs })
}

/// The log shard actor's per-buffer accumulator of stamp-eligible declared
/// column extrema (ADR-0873 wave 5a). One lives in each `LogTenantBuf`; each
/// buffered write folds its records into it, and the flush drains it through
/// [`DeclaredStatAccum::build_stamps`].
#[derive(Default)]
pub(crate) struct DeclaredStatAccum {
    /// Keyed by attribute name. Bounded by the buffer's distinct I64/BOOL
    /// attribute keys, strictly fewer than the attribute occurrences the buffer
    /// already holds.
    cols: HashMap<String, ColumnAccum>,
    /// The stream-level half of the merged view, keyed by stream id so each
    /// stream's blob is decoded once per buffer rather than once per record.
    /// The declared set is unknown until flush time, so this holds every
    /// eligible stream attribute the buffer's streams carry; on the common
    /// shape (resource attributes are service identity strings) it holds none.
    streams: HashMap<LogStreamId, StreamState>,
    /// Set when a stream's attribute blob did not decode, which leaves the
    /// merged view unknown for that stream's rows. The buffer then stamps
    /// nothing: an affirmative statement over a view this fold could not
    /// resolve is exactly the wrong answer decision 3 forbids.
    stream_attrs_undecodable: bool,
}

/// Fold one record's winning I64 value for `name`.
fn observe_i64(cols: &mut HashMap<String, ColumnAccum>, name: &str, v: i64) {
    match cols.get_mut(name) {
        Some(col) => match &mut col.i64 {
            Some(run) => run.observe(v),
            slot @ None => *slot = Some(Running::start(v)),
        },
        None => {
            cols.insert(
                name.to_string(),
                ColumnAccum {
                    i64: Some(Running::start(v)),
                    boolean: None,
                },
            );
        }
    }
}

/// Fold one record's winning BOOL value for `name`.
fn observe_bool(cols: &mut HashMap<String, ColumnAccum>, name: &str, b: bool) {
    match cols.get_mut(name) {
        Some(col) => match &mut col.boolean {
            Some(run) => run.observe(b),
            slot @ None => *slot = Some(Running::start(b)),
        },
        None => {
            cols.insert(
                name.to_string(),
                ColumnAccum {
                    i64: None,
                    boolean: Some(Running::start(b)),
                },
            );
        }
    }
}

/// The batch's attribute names whose per-row winner is not simply "the column's
/// cell": a name split across two typed dynamic columns, or one some row
/// repeated so its later occurrences went to `residual_attrs` (which the writer
/// folds into `attrs_raw`). Every other name has exactly one occurrence per
/// record and folds column-at-a-time.
fn contested_names(batch: &ColumnarLogBatch) -> HashSet<&str> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(batch.dyn_columns.len());
    let mut contested: HashSet<&str> = HashSet::new();
    for col in &batch.dyn_columns {
        if !seen.insert(col.name.as_str()) {
            contested.insert(col.name.as_str());
        }
    }
    // Empty for almost every row, so this is a walk over `num_rows` with no
    // inner work on the shape a well-formed producer emits.
    for extra in &batch.residual_attrs {
        for (name, _) in extra {
            contested.insert(name.as_str());
        }
    }
    contested
}

/// The record layer's winning occurrence of `name` among a record's own
/// attributes, or `None` when the record does not carry the name.
///
/// docs/log-segment-format.md ("Within the record layer") fixes which
/// occurrence a reader resolves when a record carries one name more than once,
/// and it is NOT the order the record was written in: the on-disk format does
/// not preserve that order. It is the order `rebuild_record` reconstructs, which
/// restricted to one name is the record's columnar occurrences ascending by
/// FIELD_DIR type byte, then its `attrs_raw` overflow occurrences ascending by
/// canonical encoded value bytes, last entry wins. So the overflow tier beats
/// every columnar occurrence, and within a tier the last occurrence of the
/// maximum key wins. This is the same reduction `RlogWriter`'s `StampScratch`
/// performs for POSTINGS and the SKIP_IDX numeric stats.
///
/// The tier split is the writer's: the first occurrence of each distinct
/// `(name, type)` takes the dynamic column slot and every later one folds into
/// `attrs_raw`. The one thing this fold cannot know is the writer's
/// dynamic-column budget, which the flush resolves later: a name that overflows
/// the 1000-column cap has ALL its occurrences in `attrs_raw`, and this reads
/// its first occurrence per type as columnar. That case needs a thousand
/// distinct dynamic columns in one flush AND a repeated key on the overflowing
/// name; every in-budget object, which is every object in practice, resolves
/// exactly as the reader does.
fn record_winner<'a>(attrs: &'a [(String, AttrValue)], name: &str) -> Option<&'a AttrValue> {
    // Type bytes that have already taken a columnar slot on this record.
    let mut columnar_types: u32 = 0;
    let mut columnar: Option<(u8, &'a AttrValue)> = None;
    let mut overflow: Option<(Vec<u8>, &'a AttrValue)> = None;
    for (k, value) in attrs {
        if k != name {
            continue;
        }
        let ty = ravel_logseg::record::resolve_value(value).0.to_u8();
        let bit = 1u32 << ty;
        if columnar_types & bit == 0 {
            columnar_types |= bit;
            if columnar.is_none_or(|(incumbent, _)| ty >= incumbent) {
                columnar = Some((ty, value));
            }
        } else {
            let encoded = ravel_logseg::record::canonical_value_bytes(value);
            if overflow
                .as_ref()
                .is_none_or(|(incumbent, _)| encoded >= *incumbent)
            {
                overflow = Some((encoded, value));
            }
        }
    }
    overflow
        .map(|(_, value)| value)
        .or(columnar.map(|(_, value)| value))
}

impl DeclaredStatAccum {
    fn merge_i64(&mut self, name: &str, min: i64, max: i64, count: u64) {
        match self.cols.get_mut(name) {
            Some(col) => match &mut col.i64 {
                Some(run) => run.merge(min, max, count),
                slot @ None => {
                    *slot = Some(Running {
                        min,
                        max,
                        non_null: count,
                    })
                }
            },
            None => {
                self.cols.insert(
                    name.to_string(),
                    ColumnAccum {
                        i64: Some(Running {
                            min,
                            max,
                            non_null: count,
                        }),
                        boolean: None,
                    },
                );
            }
        }
    }

    fn merge_bool(&mut self, name: &str, min: bool, max: bool, count: u64) {
        match self.cols.get_mut(name) {
            Some(col) => match &mut col.boolean {
                Some(run) => run.merge(min, max, count),
                slot @ None => {
                    *slot = Some(Running {
                        min,
                        max,
                        non_null: count,
                    })
                }
            },
            None => {
                self.cols.insert(
                    name.to_string(),
                    ColumnAccum {
                        i64: None,
                        boolean: Some(Running {
                            min,
                            max,
                            non_null: count,
                        }),
                    },
                );
            }
        }
    }

    /// Fold a row-major write's records into the accumulator. Each record
    /// contributes at most one non-null row per name: its record layer resolves
    /// the name to ONE winning occurrence ([`record_winner`]), and that winner
    /// is non-null for a declaration only when its kind matches. So no column's
    /// non-null count can exceed the buffer's row count and the derived
    /// `null_count` can never go negative.
    ///
    /// The record's own attributes are folded here; the stream-level half of
    /// the merged view is folded in [`Self::build_stamps`], from the per-stream
    /// row count and the count of rows that set the key themselves, which is
    /// what this records in [`StreamAttrState::overrides`]. Deferring it is
    /// what lets the fold stay O(1) per attribute without knowing the declared
    /// set, which the flush only resolves from the tenant config later.
    ///
    /// Cost is one hash entry per record attribute plus one pass over the
    /// record's distinct names. A record carrying a name twice pays one extra
    /// scan of its own attribute list per repeated name, which is the shape the
    /// OTLP normalizer only produces from malformed input.
    pub(crate) fn observe_records(&mut self, records: &[NormalizedLogRecord]) {
        let Self {
            cols,
            streams,
            stream_attrs_undecodable,
        } = self;
        // The record being folded, reduced to one entry per distinct name.
        // Declared outside the loop so a buffered write pays one allocation
        // rather than one per record; `repeated` names the entries whose value
        // here is provisional because the record carries the name more than
        // once.
        let mut distinct: HashMap<&str, &AttrValue> = HashMap::new();
        let mut repeated: Vec<&str> = Vec::new();
        for rec in records {
            let stream = match streams.entry(rec.stream_id) {
                Entry::Occupied(slot) => slot.into_mut(),
                Entry::Vacant(slot) => match stream_state(&rec.stream_attrs) {
                    Some(state) => slot.insert(state),
                    None => {
                        *stream_attrs_undecodable = true;
                        continue;
                    }
                },
            };
            stream.rows += 1;
            distinct.clear();
            repeated.clear();
            for (name, value) in &rec.attrs {
                match distinct.entry(name.as_str()) {
                    Entry::Vacant(slot) => {
                        slot.insert(value);
                    }
                    Entry::Occupied(_) => {
                        if !repeated.contains(&name.as_str()) {
                            repeated.push(name.as_str());
                        }
                    }
                }
            }
            for (name, value) in distinct.iter() {
                // A record that sets the key at ANY value kind overrides its
                // stream's attribute of the same name: a record value whose kind
                // does not match the declaration reads NULL for it rather than
                // falling back.
                if let Some(attr) = stream.attrs.get_mut(*name) {
                    attr.overrides += 1;
                }
                let winner = if repeated.contains(name) {
                    match record_winner(&rec.attrs, name) {
                        Some(winner) => winner,
                        None => continue,
                    }
                } else {
                    *value
                };
                match winner {
                    AttrValue::I64(v) => observe_i64(cols, name, *v),
                    AttrValue::Bool(b) => observe_bool(cols, name, *b),
                    _ => {}
                }
            }
        }
    }

    /// Fold one columnar batch into the accumulator.
    ///
    /// An UNCONTESTED name -- one the batch carries in exactly one dynamic
    /// column and in no row's `residual_attrs` -- has one occurrence per record
    /// by construction, so that column's cells are already its records' winning
    /// values and the whole column folds in a single pass.
    ///
    /// A CONTESTED name (a name split across two typed columns, or repeated
    /// within a record so its later occurrences landed in `residual_attrs`,
    /// which the writer folds into `attrs_raw`) has to be resolved per row under
    /// the record layer's last-occurrence-wins order, exactly as
    /// [`record_winner`] does for the row-major path. That is
    /// [`Self::observe_batch_contested`].
    pub(crate) fn observe_batch(&mut self, batch: &ColumnarLogBatch) {
        self.observe_batch_streams(batch);
        let contested = contested_names(batch);
        for col in &batch.dyn_columns {
            if contested.contains(col.name.as_str()) {
                continue;
            }
            match col.cells.first() {
                Some(AttrValue::I64(_)) => {
                    let mut min = i64::MAX;
                    let mut max = i64::MIN;
                    let mut count = 0u64;
                    for cell in &col.cells {
                        if let AttrValue::I64(v) = cell {
                            if *v < min {
                                min = *v;
                            }
                            if *v > max {
                                max = *v;
                            }
                            count += 1;
                        }
                    }
                    if count > 0 {
                        self.merge_i64(&col.name, min, max, count);
                    }
                }
                Some(AttrValue::Bool(_)) => {
                    let mut min = true;
                    let mut max = false;
                    let mut count = 0u64;
                    for cell in &col.cells {
                        if let AttrValue::Bool(b) = cell {
                            if !*b {
                                min = false;
                            }
                            if *b {
                                max = true;
                            }
                            count += 1;
                        }
                    }
                    if count > 0 {
                        self.merge_bool(&col.name, min, max, count);
                    }
                }
                _ => {}
            }
        }
        self.observe_batch_contested(batch, &contested);
        self.observe_batch_overrides(batch);
    }

    /// Fold the batch's contested names (see [`Self::observe_batch`]) row by
    /// row, each row contributing only its record layer's winning occurrence.
    ///
    /// The order is the one docs/log-segment-format.md pins: a row's
    /// `residual_attrs` entries are the occurrences the writer folds into
    /// `attrs_raw`, so they beat every column of the same name, and the greatest
    /// canonical encoding wins among them; otherwise the present column with the
    /// highest [`FieldType`] byte wins.
    fn observe_batch_contested(&mut self, batch: &ColumnarLogBatch, contested: &HashSet<&str>) {
        for name in contested {
            let cols: Vec<&ravel_logseg::DynColumn> = batch
                .dyn_columns
                .iter()
                .filter(|c| c.name == *name)
                .collect();
            // How many present cells of each column have been passed, so row
            // `row`'s cell is `cells[ranks[i]]` when the column is present there.
            let mut ranks = vec![0usize; cols.len()];
            let mut i64_run: Option<Running<i64>> = None;
            let mut bool_run: Option<Running<bool>> = None;
            for row in 0..batch.num_rows {
                let mut best: Option<(u8, &AttrValue)> = None;
                for (i, col) in cols.iter().enumerate() {
                    if !col.validity.get(row) {
                        continue;
                    }
                    let cell = col.cells.get(ranks[i]);
                    ranks[i] += 1;
                    let Some(cell) = cell else {
                        continue;
                    };
                    let ty = col.field_type.to_u8();
                    if best.is_none_or(|(incumbent, _)| ty >= incumbent) {
                        best = Some((ty, cell));
                    }
                }
                let mut overflow: Option<(Vec<u8>, &AttrValue)> = None;
                if let Some(extra) = batch.residual_attrs.get(row) {
                    for (k, value) in extra {
                        if k != name {
                            continue;
                        }
                        let encoded = ravel_logseg::record::canonical_value_bytes(value);
                        if overflow
                            .as_ref()
                            .is_none_or(|(incumbent, _)| encoded >= *incumbent)
                        {
                            overflow = Some((encoded, value));
                        }
                    }
                }
                let winner = match overflow.as_ref().map(|(_, value)| *value) {
                    Some(value) => Some(value),
                    None => best.map(|(_, value)| value),
                };
                match winner {
                    Some(AttrValue::I64(v)) => match &mut i64_run {
                        Some(run) => run.observe(*v),
                        slot @ None => *slot = Some(Running::start(*v)),
                    },
                    Some(AttrValue::Bool(b)) => match &mut bool_run {
                        Some(run) => run.observe(*b),
                        slot @ None => *slot = Some(Running::start(*b)),
                    },
                    _ => {}
                }
            }
            if let Some(run) = i64_run {
                self.merge_i64(name, run.min, run.max, run.non_null);
            }
            if let Some(run) = bool_run {
                self.merge_bool(name, run.min, run.max, run.non_null);
            }
        }
    }

    /// The stream-level half of a columnar batch: each distinct stream's blob
    /// decoded once, and the batch's rows charged to the stream they belong to.
    fn observe_batch_streams(&mut self, batch: &ColumnarLogBatch) {
        let mut rows_per_ref = vec![0u64; batch.stream_ids.len()];
        for stream_ref in &batch.stream_refs {
            match rows_per_ref.get_mut(*stream_ref as usize) {
                Some(rows) => *rows += 1,
                // A stream_ref with no STREAM_DIR entry is a malformed batch,
                // not something to stamp an affirmative statement over.
                None => self.stream_attrs_undecodable = true,
            }
        }
        for (i, stream_id) in batch.stream_ids.iter().enumerate() {
            let rows = rows_per_ref.get(i).copied().unwrap_or(0);
            let stream = match self.streams.entry(*stream_id) {
                Entry::Occupied(slot) => slot.into_mut(),
                Entry::Vacant(slot) => {
                    match batch.stream_attrs.get(i).and_then(|b| stream_state(b)) {
                        Some(state) => slot.insert(state),
                        None => {
                            self.stream_attrs_undecodable = true;
                            continue;
                        }
                    }
                }
            };
            stream.rows += rows;
        }
    }

    /// Count, per stream attribute that can serve as a fallback, the batch rows
    /// that set the same key themselves and therefore override it.
    ///
    /// The scan is skipped entirely unless one of the batch's streams carries an
    /// eligible attribute, which is the shape a service-identity resource set
    /// produces: the columnar path then costs exactly what it did before.
    fn observe_batch_overrides(&mut self, batch: &ColumnarLogBatch) {
        let mut names: Vec<String> = Vec::new();
        for stream_id in &batch.stream_ids {
            if let Some(stream) = self.streams.get(stream_id) {
                for name in stream.attrs.keys() {
                    if !names.iter().any(|n| n == name) {
                        names.push(name.clone());
                    }
                }
            }
        }
        for name in &names {
            for row in 0..batch.num_rows {
                let set_here = batch
                    .dyn_columns
                    .iter()
                    .any(|col| col.name == *name && col.validity.get(row))
                    || batch
                        .residual_attrs
                        .get(row)
                        .is_some_and(|extra| extra.iter().any(|(k, _)| k == name));
                if !set_here {
                    continue;
                }
                let Some(stream_ref) = batch.stream_refs.get(row) else {
                    continue;
                };
                let Some(stream_id) = batch.stream_ids.get(*stream_ref as usize) else {
                    continue;
                };
                if let Some(attr) = self
                    .streams
                    .get_mut(stream_id)
                    .and_then(|stream| stream.attrs.get_mut(name))
                {
                    attr.overrides += 1;
                }
            }
        }
    }

    /// Build the stamps for one flush from the accumulated extrema and the
    /// flush's declared columns (`(name, TypedAttrColumnType tag)`), against the
    /// object's `sample_count`.
    ///
    /// Only I64 and BOOL declared columns produce a stamp; every other tag
    /// (STR, BYTES, the unshipped F64, or UNSPECIFIED) is refused by
    /// [`DeclaredStatType::from_tag`] and never stamped, which is the write-side
    /// eligibility gate. A declared eligible column that the buffer never saw a
    /// matching-typed value for stamps absent extrema with
    /// `null_count == sample_count` (the exact "zero non-null values"
    /// statement the wave-4 all-NULL reader consumes). Every returned stat is
    /// valid by construction: `min <= max` from [`Running`], and
    /// `null_count = sample_count - non_null <= sample_count` with presence
    /// agreeing with the null count, so the flush's own reader drops none of
    /// them.
    pub(crate) fn build_stamps(
        &self,
        declared: &[(String, u32)],
        sample_count: u64,
    ) -> Vec<DeclaredColumnStat> {
        if self.stream_attrs_undecodable {
            return Vec::new();
        }
        let mut out = Vec::new();
        for (name, tag) in declared {
            let stat_type = match DeclaredStatType::from_tag(*tag) {
                Ok(ty) => ty,
                Err(_) => continue,
            };
            let col = self.cols.get(name);
            let stat = match stat_type {
                DeclaredStatType::I64 => {
                    let run =
                        self.fold_fallbacks(name, col.and_then(|c| c.i64), |value| match value {
                            StreamValue::I64(v) => Some(v),
                            StreamValue::Bool(_) => None,
                        });
                    build_one(
                        name,
                        DeclaredStatType::I64,
                        run.map(|r| {
                            (
                                DeclaredStatValue::I64(r.min),
                                DeclaredStatValue::I64(r.max),
                                r.non_null,
                            )
                        }),
                        sample_count,
                    )
                }
                DeclaredStatType::Bool => {
                    let run =
                        self.fold_fallbacks(
                            name,
                            col.and_then(|c| c.boolean),
                            |value| match value {
                                StreamValue::Bool(b) => Some(b),
                                StreamValue::I64(_) => None,
                            },
                        );
                    build_one(
                        name,
                        DeclaredStatType::Bool,
                        run.map(|r| {
                            (
                                DeclaredStatValue::Bool(r.min),
                                DeclaredStatValue::Bool(r.max),
                                r.non_null,
                            )
                        }),
                        sample_count,
                    )
                }
            };
            if let Some(stat) = stat {
                out.push(stat);
            }
        }
        out
    }

    /// Add the stream-level half of the merged view for one declared column to
    /// the running extrema its records produced.
    ///
    /// Every row of a stream whose attributes supply a matching-typed value for
    /// `name`, other than the rows that set `name` themselves, reads that value.
    /// Those rows are non-null for the declaration and carry a single value, so
    /// they fold in as one `(value, value, count)` merge per stream rather than
    /// row by row.
    fn fold_fallbacks<T: Ord + Copy>(
        &self,
        name: &str,
        mut run: Option<Running<T>>,
        typed: impl Fn(StreamValue) -> Option<T>,
    ) -> Option<Running<T>> {
        for stream in self.streams.values() {
            let Some(attr) = stream.attrs.get(name) else {
                continue;
            };
            let Some(value) = typed(attr.value) else {
                continue;
            };
            let count = stream.rows.saturating_sub(attr.overrides);
            if count == 0 {
                continue;
            }
            match &mut run {
                Some(r) => r.merge(value, value, count),
                slot @ None => {
                    *slot = Some(Running {
                        min: value,
                        max: value,
                        non_null: count,
                    })
                }
            }
        }
        run
    }
}

/// Build one declared-column stamp, or `None` when the accumulated non-null
/// count exceeds `sample_count` (impossible under [`DeclaredStatAccum`]'s
/// per-record dedup, but a `None` rather than a stamp its own reader would drop
/// keeps decision 3's invariant true even if that ever changes) or the typed
/// constructor refuses the triple.
fn build_one(
    name: &str,
    ty: DeclaredStatType,
    observed: Option<(DeclaredStatValue, DeclaredStatValue, u64)>,
    sample_count: u64,
) -> Option<DeclaredColumnStat> {
    let (min, max, null_count) = match observed {
        Some((min, max, non_null)) => {
            let null_count = sample_count.checked_sub(non_null)?;
            (Some(min), Some(max), null_count)
        }
        None => (None, None, sample_count),
    };
    DeclaredColumnStat::new(name, ty, min, max, null_count).ok()
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_types::declared_stats::TYPED_ATTR_COLUMN_TYPE_F64;
    use ravel_types::logstream::LogStreamId;

    fn i64_col(name: &str) -> (String, u32) {
        (name.to_string(), TYPED_ATTR_COLUMN_TYPE_I64)
    }

    fn bool_col(name: &str) -> (String, u32) {
        (name.to_string(), TYPED_ATTR_COLUMN_TYPE_BOOL)
    }

    /// A STREAM_DIR blob carrying `res` as the resource set and `scope` as the
    /// scope set, in the frozen layout the reader's fallback resolves over.
    fn blob(res: &[(&str, AttrValue)], scope: &[(&str, AttrValue)]) -> Vec<u8> {
        let owned = |pairs: &[(&str, AttrValue)]| -> Vec<(String, AttrValue)> {
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect()
        };
        ravel_logseg::stream_attrs_bytes(&owned(res), "scope", "", &owned(scope))
    }

    /// A record on the stream identified by `stream` (any distinct byte per
    /// distinct blob) whose stream-level attributes are `stream_attrs`.
    fn rec_in(
        stream: u8,
        stream_attrs: &[u8],
        attrs: Vec<(&str, AttrValue)>,
    ) -> NormalizedLogRecord {
        NormalizedLogRecord {
            stream_id: LogStreamId([stream; 16]),
            stream_attrs: stream_attrs.to_vec(),
            ts_ns: 0,
            observed_ts_ns: 0,
            severity_num: 0,
            severity_text: String::new(),
            body: String::new(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        }
    }

    /// A record on a stream whose resource and scope attribute sets are empty.
    fn rec(attrs: Vec<(&str, AttrValue)>) -> NormalizedLogRecord {
        rec_in(0, &blob(&[], &[]), attrs)
    }

    fn stat<'a>(stamps: &'a [DeclaredColumnStat], name: &str) -> Option<&'a DeclaredColumnStat> {
        stamps.iter().find(|s| s.name() == name)
    }

    #[test]
    fn i64_extrema_span_negative_and_i64_min_max() {
        let mut acc = DeclaredStatAccum::default();
        let records = vec![
            rec(vec![("EventDate", AttrValue::I64(-5))]),
            rec(vec![("EventDate", AttrValue::I64(i64::MAX))]),
            rec(vec![("EventDate", AttrValue::I64(i64::MIN))]),
            rec(vec![("EventDate", AttrValue::I64(0))]),
        ];
        acc.observe_records(&records);
        let stamps = acc.build_stamps(&[i64_col("EventDate")], 4);
        let s = stat(&stamps, "EventDate").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(i64::MIN)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(i64::MAX)));
        assert_eq!(s.null_count(), 0);
    }

    #[test]
    fn bool_extrema_false_below_true() {
        let mut acc = DeclaredStatAccum::default();
        // All false: max is false, not true.
        let all_false = vec![
            rec(vec![("IsRefresh", AttrValue::Bool(false))]),
            rec(vec![("IsRefresh", AttrValue::Bool(false))]),
        ];
        acc.observe_records(&all_false);
        let stamps = acc.build_stamps(&[bool_col("IsRefresh")], 2);
        let s = stat(&stamps, "IsRefresh").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::Bool(false)));
        assert_eq!(s.max(), Some(DeclaredStatValue::Bool(false)));
        assert_eq!(s.null_count(), 0);

        // Add a true: max climbs to true, min stays false.
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&all_false);
        acc.observe_records(&[rec(vec![("IsRefresh", AttrValue::Bool(true))])]);
        let stamps = acc.build_stamps(&[bool_col("IsRefresh")], 3);
        let s = stat(&stamps, "IsRefresh").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::Bool(false)));
        assert_eq!(s.max(), Some(DeclaredStatValue::Bool(true)));
    }

    #[test]
    fn absent_in_some_records_counts_toward_null_count() {
        let mut acc = DeclaredStatAccum::default();
        let records = vec![
            rec(vec![("EventDate", AttrValue::I64(10))]),
            rec(vec![("other", AttrValue::Str("x".to_string()))]),
            rec(vec![("EventDate", AttrValue::I64(20))]),
            rec(vec![]),
            rec(vec![("EventDate", AttrValue::I64(15))]),
        ];
        acc.observe_records(&records);
        let stamps = acc.build_stamps(&[i64_col("EventDate")], 5);
        let s = stat(&stamps, "EventDate").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(10)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(20)));
        // Two of five rows carry no EventDate: exact null_count 2.
        assert_eq!(s.null_count(), 2);
    }

    #[test]
    fn all_null_declared_column_stamps_absent_extrema() {
        let mut acc = DeclaredStatAccum::default();
        // Three records, none carrying the declared column.
        let records = vec![
            rec(vec![("other", AttrValue::I64(1))]),
            rec(vec![]),
            rec(vec![("other", AttrValue::I64(2))]),
        ];
        acc.observe_records(&records);
        let stamps = acc.build_stamps(&[i64_col("EventDate")], 3);
        let s = stat(&stamps, "EventDate").expect("all-null column still stamps");
        assert_eq!(s.min(), None);
        assert_eq!(s.max(), None);
        assert_eq!(s.null_count(), 3, "null_count == sample_count");
    }

    #[test]
    fn wrong_typed_value_reads_null_for_the_declaration() {
        // "flag" declared BOOL, but one record carries it as I64: that row is a
        // NULL for the BOOL column, not a non-null.
        let mut acc = DeclaredStatAccum::default();
        let records = vec![
            rec(vec![("flag", AttrValue::Bool(true))]),
            rec(vec![("flag", AttrValue::I64(9))]),
            rec(vec![("flag", AttrValue::Bool(false))]),
        ];
        acc.observe_records(&records);
        let stamps = acc.build_stamps(&[bool_col("flag")], 3);
        let s = stat(&stamps, "flag").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::Bool(false)));
        assert_eq!(s.max(), Some(DeclaredStatValue::Bool(true)));
        // Two BOOL rows, one I64 row: the I64 row is NULL for the BOOL column.
        assert_eq!(s.null_count(), 1);
    }

    #[test]
    fn f64_declared_column_is_never_stamped() {
        // The write-side float gate: a declared column carrying the ADR-0101
        // F64 tag is refused by the allowlist, so no stamp is emitted for it
        // even though the buffer carries no F64 tracking to begin with.
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[rec(vec![("Ratio", AttrValue::F64(1.5))])]);
        let declared = vec![("Ratio".to_string(), TYPED_ATTR_COLUMN_TYPE_F64)];
        let stamps = acc.build_stamps(&declared, 1);
        assert!(
            stat(&stamps, "Ratio").is_none(),
            "F64 declared column must carry no stamp"
        );
        assert!(stamps.is_empty());
    }

    #[test]
    fn str_and_bytes_declared_columns_are_never_stamped() {
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[rec(vec![
            ("URL", AttrValue::Str("a".to_string())),
            ("Blob", AttrValue::Bytes(vec![1, 2])),
        ])]);
        let declared = vec![
            ("URL".to_string(), TYPED_ATTR_COLUMN_TYPE_STR),
            ("Blob".to_string(), TYPED_ATTR_COLUMN_TYPE_BYTES),
        ];
        let stamps = acc.build_stamps(&declared, 1);
        assert!(stamps.is_empty(), "STR and BYTES never stamp");
    }

    #[test]
    fn repeated_attribute_in_one_record_counts_once() {
        // A record repeating the same I64 attribute must not push non_null past
        // the row count (which would make null_count go negative and the stamp
        // self-drop).
        //
        // Which of the two the row counts is the reader's answer, not the
        // record's write order: the first `(EventDate, i64)` occurrence takes
        // the dynamic column slot and the second folds into `attrs_raw`, and the
        // `attrs_raw` tier beats every columnar occurrence, so `rebuild_record`
        // plus `merged_attrs` resolve 99 (issue #1182).
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[rec(vec![
            ("EventDate", AttrValue::I64(7)),
            ("EventDate", AttrValue::I64(99)),
        ])]);
        let stamps = acc.build_stamps(&[i64_col("EventDate")], 1);
        let s = stat(&stamps, "EventDate").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(99)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(99)));
        assert_eq!(s.null_count(), 0);
    }

    #[test]
    fn undeclared_eligible_column_is_not_stamped() {
        // Tracking is buffer-wide but stamping is gated on the declared set:
        // an I64 attribute nobody declared produces no stamp.
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[rec(vec![("user_id", AttrValue::I64(1))])]);
        let stamps = acc.build_stamps(&[i64_col("EventDate")], 1);
        assert!(stat(&stamps, "user_id").is_none());
        // EventDate was declared but never seen: it stamps all-null.
        let s = stat(&stamps, "EventDate").expect("declared, all-null");
        assert_eq!(s.null_count(), 1);
    }

    /// Prove-the-test: drop the `fold_fallbacks` call from `build_stamps`'s I64
    /// arm and this stamps `min: None, max: None, null_count: 3`, the
    /// affirmative all-NULL statement issue #1057 is about.
    #[test]
    fn resource_attribute_supplies_the_value_no_record_sets() {
        let stream = blob(&[("k", AttrValue::I64(7))], &[]);
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[
            rec_in(1, &stream, vec![("other", AttrValue::I64(1))]),
            rec_in(1, &stream, vec![]),
            rec_in(1, &stream, vec![("other", AttrValue::I64(2))]),
        ]);
        let stamps = acc.build_stamps(&[i64_col("k")], 3);
        let s = stat(&stamps, "k").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(7)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(7)));
        assert_eq!(s.null_count(), 0);
    }

    /// Prove-the-test: stop counting `overrides` in `observe_records` and the
    /// overriding row is counted twice (non_null 4 over 3 rows), which
    /// `build_one`'s `checked_sub` turns into no stamp at all; keep the count
    /// but drop the fallback fold and `min` reads 2 with `null_count` 2.
    #[test]
    fn record_value_overrides_the_resource_attribute_for_its_row_only() {
        let stream = blob(&[("k", AttrValue::I64(7))], &[]);
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[
            rec_in(1, &stream, vec![("k", AttrValue::I64(2))]),
            rec_in(1, &stream, vec![]),
            rec_in(1, &stream, vec![("other", AttrValue::Bool(true))]),
        ]);
        let stamps = acc.build_stamps(&[i64_col("k")], 3);
        let s = stat(&stamps, "k").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(2)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(7)));
        assert_eq!(s.null_count(), 0);
    }

    /// Prove-the-test: skip the scope set when decoding the blob (parse only
    /// the resource set) and this stamps all-NULL over 3 rows.
    #[test]
    fn scope_attribute_supplies_the_value_for_a_declared_bool_column() {
        let stream = blob(&[], &[("flag", AttrValue::Bool(true))]);
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[
            rec_in(2, &stream, vec![]),
            rec_in(2, &stream, vec![("other", AttrValue::I64(1))]),
            rec_in(2, &stream, vec![]),
        ]);
        let stamps = acc.build_stamps(&[bool_col("flag")], 3);
        let s = stat(&stamps, "flag").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::Bool(true)));
        assert_eq!(s.max(), Some(DeclaredStatValue::Bool(true)));
        assert_eq!(s.null_count(), 0);
    }

    #[test]
    fn wrong_typed_record_value_suppresses_the_stream_fallback() {
        // The reader resolves the record's own value whenever the record sets
        // the key at ANY kind, so a row carrying `k` as a Str reads NULL for the
        // I64 declaration rather than falling back to the resource attribute.
        let stream = blob(&[("k", AttrValue::I64(7))], &[]);
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[
            rec_in(1, &stream, vec![("k", AttrValue::Str("x".to_string()))]),
            rec_in(1, &stream, vec![]),
        ]);
        let stamps = acc.build_stamps(&[i64_col("k")], 2);
        let s = stat(&stamps, "k").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(7)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(7)));
        assert_eq!(s.null_count(), 1);
    }

    #[test]
    fn stream_attribute_of_an_ineligible_kind_is_not_a_fallback() {
        // A resource attribute of a kind the declaration cannot read is a NULL
        // for every row of the stream, exactly as an absent one is.
        let stream = blob(&[("k", AttrValue::Str("seven".to_string()))], &[]);
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[rec_in(1, &stream, vec![]), rec_in(1, &stream, vec![])]);
        let stamps = acc.build_stamps(&[i64_col("k")], 2);
        let s = stat(&stamps, "k").expect("stamped");
        assert_eq!(s.min(), None);
        assert_eq!(s.max(), None);
        assert_eq!(s.null_count(), 2);
    }

    #[test]
    fn each_stream_contributes_its_own_fallback() {
        let a = blob(&[("k", AttrValue::I64(7))], &[]);
        let b = blob(&[("k", AttrValue::I64(-3))], &[]);
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[
            rec_in(1, &a, vec![]),
            rec_in(2, &b, vec![]),
            rec_in(2, &b, vec![("k", AttrValue::I64(100))]),
        ]);
        let stamps = acc.build_stamps(&[i64_col("k")], 3);
        let s = stat(&stamps, "k").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(-3)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(100)));
        assert_eq!(s.null_count(), 0);
    }

    #[test]
    fn an_undecodable_stream_blob_stamps_nothing() {
        // The merged view is unresolvable for that stream's rows, so the flush
        // makes no statement at all rather than an affirmative wrong one.
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[rec_in(1, &[0xff, 0xff], vec![("k", AttrValue::I64(1))])]);
        assert!(acc.build_stamps(&[i64_col("k")], 1).is_empty());
    }

    #[test]
    fn columnar_batch_folds_the_same_merged_view_as_the_row_path() {
        use ravel_logseg::LogRecord;

        let stream = blob(&[("k", AttrValue::I64(7))], &[]);
        let to_logrecord = |r: &NormalizedLogRecord| LogRecord {
            stream_id: r.stream_id,
            stream_attrs: r.stream_attrs.clone(),
            ts_ns: r.ts_ns,
            observed_ts_ns: r.observed_ts_ns,
            severity_num: r.severity_num,
            severity_text: r.severity_text.clone(),
            body: r.body.clone(),
            trace_id: None,
            span_id: None,
            flags: r.flags,
            attrs: r.attrs.clone(),
        };
        let records = vec![
            rec_in(1, &stream, vec![("k", AttrValue::I64(2))]),
            rec_in(1, &stream, vec![]),
            rec_in(1, &stream, vec![("other", AttrValue::I64(5))]),
        ];
        let batch =
            ColumnarLogBatch::from_records(&records.iter().map(to_logrecord).collect::<Vec<_>>());

        let mut columnar = DeclaredStatAccum::default();
        columnar.observe_batch(&batch);
        let columnar = columnar.build_stamps(&[i64_col("k")], 3);
        let s = stat(&columnar, "k").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(2)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(7)));
        assert_eq!(s.null_count(), 0);

        let mut row = DeclaredStatAccum::default();
        row.observe_records(&records);
        let row = row.build_stamps(&[i64_col("k")], 3);
        assert_eq!(row.len(), 1);
        assert_eq!(columnar.len(), 1);
        assert_eq!(row[0].min(), s.min());
        assert_eq!(row[0].max(), s.max());
        assert_eq!(row[0].null_count(), s.null_count());
    }

    /// Issue #1182, consumer 3: this fold must resolve a record that carries one
    /// attribute name more than once to the SAME value the readers do.
    ///
    /// The record table and the resolved values are the ones
    /// `ravel_sql::logs_scan::columnar_lookup_tests::a_duplicate_key_resolves_last_occurrence_wins_on_both_reader_paths`
    /// pins for the two reader paths, and
    /// `ravel_maintain::rlog::tests::duplicate_key_stamp_follows_the_readers_rule`
    /// pins for the compaction recompute:
    ///
    /// | record's own attributes             | resolved `k`  |
    /// |-------------------------------------|---------------|
    /// | `I64(5)` then `Str("x")`            | `I64(5)`      |
    /// | `Str("x")` then `I64(5)`            | `I64(5)`      |
    /// | `Bool(true)` then `I64(9)`          | `Bool(true)`  |
    /// | `I64(1)` then `Bytes([0xab])`       | `Bytes(..)`   |
    /// | `Str("y")`                          | `Str("y")`    |
    ///
    /// So a declared I64 `k` is non-null on exactly the first two rows and a
    /// declared BOOL `k` on exactly the third, whatever order each record wrote
    /// its occurrences in. Both fold entry points are driven, because a batch
    /// ingested column-major must not stamp a different statement than the same
    /// records ingested row-major.
    ///
    /// Prove-the-test, both halves separately. Folding every occurrence's own
    /// value in `observe_records` (the base's first-occurrence-per-(name, kind)
    /// rule: iterate `rec.attrs` instead of the distinct names and drop the
    /// `record_winner` lookup) makes the row-major I64 stamp read min 1, max 9,
    /// null_count 1 -- rows 3 and 4 claim the 9 and the 1 that the Bool and the
    /// Bytes occurrence shadow -- and the first assertion fails with
    /// `left: Some(I64(1))`. Dropping the `contested_names` skip and the
    /// `observe_batch_contested` call from `observe_batch` fails the columnar
    /// half the same way.
    #[test]
    fn duplicate_key_stamp_follows_the_readers_rule() {
        use ravel_logseg::LogRecord;

        let stream = blob(&[], &[]);
        let records = vec![
            rec_in(
                1,
                &stream,
                vec![
                    ("k", AttrValue::I64(5)),
                    ("k", AttrValue::Str("x".to_string())),
                ],
            ),
            rec_in(
                1,
                &stream,
                vec![
                    ("k", AttrValue::Str("x".to_string())),
                    ("k", AttrValue::I64(5)),
                ],
            ),
            rec_in(
                1,
                &stream,
                vec![("k", AttrValue::Bool(true)), ("k", AttrValue::I64(9))],
            ),
            rec_in(
                1,
                &stream,
                vec![
                    ("k", AttrValue::I64(1)),
                    ("k", AttrValue::Bytes(vec![0xab])),
                ],
            ),
            rec_in(1, &stream, vec![("k", AttrValue::Str("y".to_string()))]),
        ];
        let declared = vec![i64_col("k"), bool_col("k")];

        let check = |stamps: &[DeclaredColumnStat], what: &str| {
            // `build_stamps` emits one stat per declared entry, so the two
            // declarations of `k` are told apart by their stat type.
            let i64_stat = stamps
                .iter()
                .find(|s| s.declared_type() == DeclaredStatType::I64)
                .unwrap_or_else(|| panic!("{what}: I64 stamp"));
            assert_eq!(
                i64_stat.min(),
                Some(DeclaredStatValue::I64(5)),
                "{what}: I64 min is the two rows resolving to 5, not the shadowed 1"
            );
            assert_eq!(
                i64_stat.max(),
                Some(DeclaredStatValue::I64(5)),
                "{what}: I64 max is the two rows resolving to 5, not the shadowed 9"
            );
            assert_eq!(
                i64_stat.null_count(),
                3,
                "{what}: the Bool, Bytes and Str winners are NULL for a declared I64"
            );
            let bool_stat = stamps
                .iter()
                .find(|s| s.declared_type() == DeclaredStatType::Bool)
                .unwrap_or_else(|| panic!("{what}: BOOL stamp"));
            assert_eq!(bool_stat.min(), Some(DeclaredStatValue::Bool(true)));
            assert_eq!(bool_stat.max(), Some(DeclaredStatValue::Bool(true)));
            assert_eq!(
                bool_stat.null_count(),
                4,
                "{what}: only the record whose winner is a Bool is non-null"
            );
        };

        let mut row = DeclaredStatAccum::default();
        row.observe_records(&records);
        check(&row.build_stamps(&declared, 5), "row-major fold");

        let to_logrecord = |r: &NormalizedLogRecord| LogRecord {
            stream_id: r.stream_id,
            stream_attrs: r.stream_attrs.clone(),
            ts_ns: r.ts_ns,
            observed_ts_ns: r.observed_ts_ns,
            severity_num: r.severity_num,
            severity_text: r.severity_text.clone(),
            body: r.body.clone(),
            trace_id: None,
            span_id: None,
            flags: r.flags,
            attrs: r.attrs.clone(),
        };
        let batch =
            ColumnarLogBatch::from_records(&records.iter().map(to_logrecord).collect::<Vec<_>>());
        let mut columnar = DeclaredStatAccum::default();
        columnar.observe_batch(&batch);
        check(&columnar.build_stamps(&declared, 5), "columnar fold");
    }

    /// The `attrs_raw` tier of the same rule: a record repeating one name at ONE
    /// type puts every occurrence past the first into `attrs_raw`, and that tier
    /// beats the columnar occurrence outright, with the greatest canonical
    /// encoding winning inside it. So the value the row resolves is neither the
    /// first occurrence nor the largest.
    ///
    /// Prove-the-test: return `columnar` before consulting `overflow` in
    /// `record_winner` and the stamp reads 7 (the first occurrence, the base's
    /// answer) instead of 2.
    #[test]
    fn an_overflowing_duplicate_beats_the_columnar_occurrence() {
        // `I64(7)` takes the `(k, i64)` column and `I64(-1)`, `I64(2)` fold into
        // `attrs_raw`. The frozen canonical encoding of an i64 is the tag byte 2
        // followed by the LEB128 zigzag, so `I64(-1)` encodes as `.. 02 01` and
        // `I64(2)` as `.. 02 04`: 2 is the greater canonical key of the two
        // overflow occurrences and wins the row. Neither the first occurrence
        // nor the largest integer.
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[rec(vec![
            ("k", AttrValue::I64(7)),
            ("k", AttrValue::I64(-1)),
            ("k", AttrValue::I64(2)),
        ])]);
        let stamps = acc.build_stamps(&[i64_col("k")], 1);
        let s = stat(&stamps, "k").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(2)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(2)));
        assert_eq!(s.null_count(), 0, "one record, one non-null row");
    }

    /// Issue #1057 finding 1: a stream whose RESOURCE attributes carry the
    /// declared key as a List and whose SCOPE attributes carry it as a
    /// matching-typed I64 must fall back to the scope value, exactly what the
    /// reader's decoder resolves (it never decodes a List entry at all, so it
    /// moves straight to the next occurrence of the key).
    ///
    /// Prove-the-test: drop the `AttrValue::List(_) | AttrValue::Map(_)` skip
    /// from `stream_state` and the List's first occurrence claims `k` with no
    /// fallback, stamping min 2, max 2, null_count 2 over these same records
    /// (the base's wrong answer) instead of min 2, max 7, null_count 0.
    #[test]
    fn list_resource_attribute_is_skipped_for_a_matching_scope_fallback() {
        let stream = blob(
            &[("k", AttrValue::List(vec![AttrValue::I64(1)]))],
            &[("k", AttrValue::I64(7))],
        );
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[
            rec_in(1, &stream, vec![("k", AttrValue::I64(2))]),
            rec_in(1, &stream, vec![]),
            rec_in(1, &stream, vec![]),
        ]);
        let stamps = acc.build_stamps(&[i64_col("k")], 3);
        let s = stat(&stamps, "k").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(2)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(7)));
        assert_eq!(s.null_count(), 0);
    }

    /// Same stream shape as above, but no record sets `k` itself: every row
    /// reads the scope fallback the List resource attribute must not shadow.
    #[test]
    fn list_resource_attribute_is_skipped_when_no_record_overrides() {
        let stream = blob(
            &[("k", AttrValue::List(vec![AttrValue::I64(1)]))],
            &[("k", AttrValue::I64(7))],
        );
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[
            rec_in(1, &stream, vec![]),
            rec_in(1, &stream, vec![]),
            rec_in(1, &stream, vec![]),
        ]);
        let stamps = acc.build_stamps(&[i64_col("k")], 3);
        let s = stat(&stamps, "k").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(7)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(7)));
        assert_eq!(s.null_count(), 0);
    }
}
