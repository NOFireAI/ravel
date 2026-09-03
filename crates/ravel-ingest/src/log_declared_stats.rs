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
//! rows carrying an I64 value under that name, and a row carrying the same name
//! as a BOOL (or the row not carrying it at all) is a NULL for the I64 column.
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

use std::collections::HashMap;
use std::collections::hash_map::Entry;

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
/// The two epochs implement first-occurrence-wins within one record: a record
/// is folded under a fresh epoch, and a kind that has already taken its value
/// from that record carries the record's epoch. This replaces a per-record
/// `seen` list scanned once per attribute, so a wide record costs one hash
/// lookup per attribute rather than a scan over its own attributes.
#[derive(Default, Clone, Copy, Debug)]
struct ColumnAccum {
    i64: Option<Running<i64>>,
    boolean: Option<Running<bool>>,
    /// Epoch of the record this column's I64 running value last took a value
    /// from, or 0 before any (epochs start at 1).
    i64_epoch: u64,
    /// The same for the BOOL running value.
    bool_epoch: u64,
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
    /// Epoch of the record this override count last counted, so a record
    /// repeating the key counts once.
    epoch: u64,
}

/// One stream's contribution to the merged view: how many of the buffer's rows
/// belong to it, and the stream-level attributes those rows fall back to.
#[derive(Default)]
struct StreamState {
    rows: u64,
    /// Only the names whose first blob occurrence (resource set before scope
    /// set, the order the reader's `find_attr` resolves) is I64 or BOOL. A name
    /// whose first occurrence is of any other kind reads NULL for a declaration
    /// either way, so it is dropped at decode time and costs nothing per record.
    attrs: HashMap<String, StreamAttrState>,
}

/// Decode one STREAM_DIR blob into the fallbacks its rows read, or `None` if
/// the blob does not decode.
fn stream_state(blob: &[u8]) -> Option<StreamState> {
    let pairs = stream_attr_pairs(blob).ok()?;
    let mut first: HashMap<String, Option<StreamValue>> = HashMap::with_capacity(pairs.len());
    for (name, value) in pairs {
        // First occurrence wins whatever its kind, matching the reader: an
        // ineligible first occurrence shadows a same-named eligible one behind
        // it, so it must be recorded as "claimed, no fallback".
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
                        epoch: 0,
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
    /// Monotonic per-record counter driving the first-occurrence-wins epochs.
    /// Starts at 0 and is incremented before each record is folded, so a live
    /// epoch is never 0.
    epoch: u64,
    /// Set when a stream's attribute blob did not decode, which leaves the
    /// merged view unknown for that stream's rows. The buffer then stamps
    /// nothing: an affirmative statement over a view this fold could not
    /// resolve is exactly the wrong answer decision 3 forbids.
    stream_attrs_undecodable: bool,
}

/// Fold one I64 attribute value of the record being folded under `epoch`.
fn observe_i64(cols: &mut HashMap<String, ColumnAccum>, epoch: u64, name: &str, v: i64) {
    match cols.get_mut(name) {
        Some(col) => {
            if col.i64_epoch == epoch {
                return;
            }
            col.i64_epoch = epoch;
            match &mut col.i64 {
                Some(run) => run.observe(v),
                slot @ None => *slot = Some(Running::start(v)),
            }
        }
        None => {
            cols.insert(
                name.to_string(),
                ColumnAccum {
                    i64: Some(Running::start(v)),
                    boolean: None,
                    i64_epoch: epoch,
                    bool_epoch: 0,
                },
            );
        }
    }
}

/// Fold one BOOL attribute value of the record being folded under `epoch`.
fn observe_bool(cols: &mut HashMap<String, ColumnAccum>, epoch: u64, name: &str, b: bool) {
    match cols.get_mut(name) {
        Some(col) => {
            if col.bool_epoch == epoch {
                return;
            }
            col.bool_epoch = epoch;
            match &mut col.boolean {
                Some(run) => run.observe(b),
                slot @ None => *slot = Some(Running::start(b)),
            }
        }
        None => {
            cols.insert(
                name.to_string(),
                ColumnAccum {
                    i64: None,
                    boolean: Some(Running::start(b)),
                    i64_epoch: 0,
                    bool_epoch: epoch,
                },
            );
        }
    }
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
                        i64_epoch: 0,
                        bool_epoch: 0,
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
                        i64_epoch: 0,
                        bool_epoch: 0,
                    },
                );
            }
        }
    }

    /// Fold a row-major write's records into the accumulator. Each record
    /// contributes at most one non-null row per `(name, kind)`: a record that
    /// repeats an attribute key with the same value kind counts once (the first
    /// occurrence, matching the writer's first-occurrence-wins column slot), so
    /// no column's non-null count can exceed the buffer's row count and the
    /// derived `null_count` can never go negative.
    ///
    /// The record's own attributes are folded here; the stream-level half of
    /// the merged view is folded in [`Self::build_stamps`], from the per-stream
    /// row count and the count of rows that set the key themselves, which is
    /// what this records in [`StreamAttrState::overrides`]. Deferring it is
    /// what lets the fold stay O(1) per attribute without knowing the declared
    /// set, which the flush only resolves from the tenant config later.
    pub(crate) fn observe_records(&mut self, records: &[NormalizedLogRecord]) {
        let Self {
            cols,
            streams,
            epoch,
            stream_attrs_undecodable,
        } = self;
        for rec in records {
            *epoch += 1;
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
            for (name, value) in &rec.attrs {
                // A record that sets the key at ANY value kind overrides its
                // stream's attribute of the same name, the reader's
                // `record_sets_key` rule: a wrong-typed record value reads NULL
                // for the declaration rather than falling back.
                if let Some(attr) = stream.attrs.get_mut(name.as_str())
                    && attr.epoch != *epoch
                {
                    attr.epoch = *epoch;
                    attr.overrides += 1;
                }
                match value {
                    AttrValue::I64(v) => observe_i64(cols, *epoch, name, *v),
                    AttrValue::Bool(b) => observe_bool(cols, *epoch, name, *b),
                    _ => {}
                }
            }
        }
    }

    /// Fold one columnar batch into the accumulator. A dynamic column holds one
    /// dense value per present row (the batch already applied the row path's
    /// first-occurrence-wins rule, so its `cells.len()` is the column's non-null
    /// row count), which lets each column be folded in a single pass.
    pub(crate) fn observe_batch(&mut self, batch: &ColumnarLogBatch) {
        self.observe_batch_streams(batch);
        for col in &batch.dyn_columns {
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
        self.observe_batch_overrides(batch);
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
        // self-drop). First occurrence wins the value.
        let mut acc = DeclaredStatAccum::default();
        acc.observe_records(&[rec(vec![
            ("EventDate", AttrValue::I64(7)),
            ("EventDate", AttrValue::I64(99)),
        ])]);
        let stamps = acc.build_stamps(&[i64_col("EventDate")], 1);
        let s = stat(&stamps, "EventDate").expect("stamped");
        assert_eq!(s.min(), Some(DeclaredStatValue::I64(7)));
        assert_eq!(s.max(), Some(DeclaredStatValue::I64(7)));
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
}
