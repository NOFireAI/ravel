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

use std::collections::HashMap;

use ravel_catalog::DeclaredColumnType;
use ravel_logseg::ColumnarLogBatch;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_types::declared_stats::{
    DeclaredColumnStat, DeclaredStatType, DeclaredStatValue, TYPED_ATTR_COLUMN_TYPE_BOOL,
    TYPED_ATTR_COLUMN_TYPE_BYTES, TYPED_ATTR_COLUMN_TYPE_I64, TYPED_ATTR_COLUMN_TYPE_STR,
};
use ravel_types::logstream::AttrValue;

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
#[derive(Default, Clone, Copy, Debug)]
struct ColumnAccum {
    i64: Option<Running<i64>>,
    boolean: Option<Running<bool>>,
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
}

impl DeclaredStatAccum {
    fn observe_i64(&mut self, name: &str, v: i64) {
        match self.cols.get_mut(name) {
            Some(col) => match &mut col.i64 {
                Some(run) => run.observe(v),
                slot @ None => *slot = Some(Running::start(v)),
            },
            None => {
                self.cols.insert(
                    name.to_string(),
                    ColumnAccum {
                        i64: Some(Running::start(v)),
                        boolean: None,
                    },
                );
            }
        }
    }

    fn observe_bool(&mut self, name: &str, b: bool) {
        match self.cols.get_mut(name) {
            Some(col) => match &mut col.boolean {
                Some(run) => run.observe(b),
                slot @ None => *slot = Some(Running::start(b)),
            },
            None => {
                self.cols.insert(
                    name.to_string(),
                    ColumnAccum {
                        i64: None,
                        boolean: Some(Running::start(b)),
                    },
                );
            }
        }
    }

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
    /// contributes at most one non-null row per `(name, kind)`: a record that
    /// repeats an attribute key with the same value kind counts once (the first
    /// occurrence, matching the writer's first-occurrence-wins column slot), so
    /// no column's non-null count can exceed the buffer's row count and the
    /// derived `null_count` can never go negative.
    pub(crate) fn observe_records(&mut self, records: &[NormalizedLogRecord]) {
        // Reused across records so the per-record dedup allocates once, not per
        // record. `(name, kind-tag)` where the tag distinguishes I64 from BOOL.
        let mut seen: Vec<(&str, u8)> = Vec::new();
        for rec in records {
            seen.clear();
            for (name, value) in &rec.attrs {
                let (kind, is_i64, iv, bv) = match value {
                    AttrValue::I64(v) => (0u8, true, *v, false),
                    AttrValue::Bool(b) => (1u8, false, 0, *b),
                    _ => continue,
                };
                if seen.contains(&(name.as_str(), kind)) {
                    continue;
                }
                seen.push((name.as_str(), kind));
                if is_i64 {
                    self.observe_i64(name, iv);
                } else {
                    self.observe_bool(name, bv);
                }
            }
        }
    }

    /// Fold one columnar batch into the accumulator. A dynamic column holds one
    /// dense value per present row (the batch already applied the row path's
    /// first-occurrence-wins rule, so its `cells.len()` is the column's non-null
    /// row count), which lets each column be folded in a single pass.
    pub(crate) fn observe_batch(&mut self, batch: &ColumnarLogBatch) {
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
        let mut out = Vec::new();
        for (name, tag) in declared {
            let stat_type = match DeclaredStatType::from_tag(*tag) {
                Ok(ty) => ty,
                Err(_) => continue,
            };
            let col = self.cols.get(name);
            let stat = match stat_type {
                DeclaredStatType::I64 => {
                    let run = col.and_then(|c| c.i64);
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
                    let run = col.and_then(|c| c.boolean);
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

    fn rec(attrs: Vec<(&str, AttrValue)>) -> NormalizedLogRecord {
        NormalizedLogRecord {
            stream_id: LogStreamId([0u8; 16]),
            stream_attrs: Vec::new(),
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
}
