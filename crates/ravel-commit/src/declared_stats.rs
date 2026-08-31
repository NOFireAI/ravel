//! Per-declared-column min/max/null-count stamps for commit and compaction
//! records (ADR-0873). This is the wire boundary only (ADR-0873 wave 1): the
//! typed constructor that refuses a declared type outside the allowlist, and
//! the decoder that drops (and counts) any entry an older or buggy writer
//! left malformed. Stamping at ingest and compaction, the `SegmentRef` copy,
//! and the SQL planner wiring are later waves.
//!
//! The eligibility allowlist ({I64, BOOL}) is EXPLICIT, not an incidental
//! consequence of which declared types exist today (ADR-0873 decision 2):
//! ADR-0101 (Accepted) adds `TYPED_ATTR_COLUMN_TYPE_F64 = 5` unshipped, and
//! it is deliberately absent here so no writer stamps it and no reader
//! accepts it. STR and BYTES are excluded because a truncated extremum is a
//! bound, not an exact value, and an untruncated one puts arbitrary user data
//! on the hot resolve path.

use ravel_proto::commit::v1::{
    DeclaredColumnMinMax, DeclaredColumnStatValue, declared_column_stat_value::Kind,
};
use ravel_proto::sys::v1::TypedAttrColumnType;

/// A stamp-eligible declared column type (ADR-0873 decision 2 allowlist).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EligibleType {
    I64,
    Bool,
}

impl EligibleType {
    /// Map a raw `ravel.sys.v1.TypedAttrColumnType` discriminant to its
    /// eligible variant, or `None` for any ineligible or unknown type. This
    /// is the single allowlist gate: `STR`, `BYTES`, `UNSPECIFIED`, the
    /// unshipped `F64`, and any future or corrupt discriminant all map to
    /// `None`.
    pub fn from_declared_type(declared_type: i32) -> Option<Self> {
        // Matched against the generated enum discriminants, not literals, so a
        // renumber (which the frozen contract forbids anyway) cannot silently
        // shift the allowlist.
        if declared_type == TypedAttrColumnType::I64 as i32 {
            Some(Self::I64)
        } else if declared_type == TypedAttrColumnType::Bool as i32 {
            Some(Self::Bool)
        } else {
            None
        }
    }

    /// The `TypedAttrColumnType` discriminant this eligible type stores in a
    /// `DeclaredColumnMinMax.declared_type` field.
    pub fn as_declared_type(self) -> u32 {
        match self {
            Self::I64 => TypedAttrColumnType::I64 as u32,
            Self::Bool => TypedAttrColumnType::Bool as u32,
        }
    }
}

/// One extremum value, typed so its kind cannot disagree with its column's
/// declared type. Mirrors the `DeclaredColumnStatValue` oneof arms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatValue {
    I64(i64),
    Bool(bool),
}

impl StatValue {
    fn eligible_type(self) -> EligibleType {
        match self {
            Self::I64(_) => EligibleType::I64,
            Self::Bool(_) => EligibleType::Bool,
        }
    }

    fn to_proto(self) -> DeclaredColumnStatValue {
        let kind = match self {
            Self::I64(v) => Kind::I64(v),
            Self::Bool(v) => Kind::B(v),
        };
        DeclaredColumnStatValue { kind: Some(kind) }
    }

    /// Extract a typed value from a wire `DeclaredColumnStatValue`, requiring
    /// the oneof arm to match the entry's declared type. A present message
    /// with no arm set (`kind == None`) is malformed and yields `Err`.
    fn from_proto(value: &DeclaredColumnStatValue, expected: EligibleType) -> Result<Self, ()> {
        match (value.kind.as_ref(), expected) {
            (Some(Kind::I64(v)), EligibleType::I64) => Ok(Self::I64(*v)),
            (Some(Kind::B(v)), EligibleType::Bool) => Ok(Self::Bool(*v)),
            _ => Err(()),
        }
    }
}

/// A validated per-declared-column stamp: an exact whole-object min/max and an
/// exact null count for one eligible declared column. Both `min` and `max`
/// `None` means the column had zero non-null values in the object, which is
/// still an exact statement (ADR-0873 decision 1). Construct through
/// [`DeclaredColumnStat::stamp`], the typed boundary that refuses an
/// ineligible declared type; the struct's invariant (every present value's
/// kind matches `declared_type`) then holds by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredColumnStat {
    pub name: String,
    pub declared_type: EligibleType,
    pub min: Option<StatValue>,
    pub max: Option<StatValue>,
    pub null_count: u64,
}

/// Why a stamp was refused at the typed boundary (ADR-0873 decision 2).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum StampError {
    #[error(
        "declared column type {0} is not stamp-eligible; the allowlist is {{I64, BOOL}} (ADR-0873)"
    )]
    IneligibleType(i32),
    #[error("stat value kind does not match declared column type {declared_type:?}")]
    KindMismatch { declared_type: EligibleType },
}

impl DeclaredColumnStat {
    /// The typed boundary (ADR-0873 decision 2). Refuse any `declared_type`
    /// outside the allowlist, and refuse a `min`/`max` value whose kind does
    /// not match the declared type. An ineligible column cannot be turned
    /// into a wire stamp: it is refused here, never silently encoded.
    pub fn stamp(
        name: impl Into<String>,
        declared_type: i32,
        min: Option<StatValue>,
        max: Option<StatValue>,
        null_count: u64,
    ) -> Result<Self, StampError> {
        let eligible = EligibleType::from_declared_type(declared_type)
            .ok_or(StampError::IneligibleType(declared_type))?;
        for value in [min, max].into_iter().flatten() {
            if value.eligible_type() != eligible {
                return Err(StampError::KindMismatch {
                    declared_type: eligible,
                });
            }
        }
        Ok(Self {
            name: name.into(),
            declared_type: eligible,
            min,
            max,
            null_count,
        })
    }

    /// The whole-object non-null row count, given the record's total row
    /// count. Saturates at zero: a `null_count` exceeding `row_count` is a
    /// corrupt entry that a decoder already drops, so this never underflows in
    /// a validated stamp.
    pub fn non_null_count(&self, row_count: u64) -> u64 {
        row_count.saturating_sub(self.null_count)
    }

    /// Encode to the wire message. Infallible: the invariant is already held.
    pub fn to_proto(&self) -> DeclaredColumnMinMax {
        DeclaredColumnMinMax {
            name: self.name.clone(),
            declared_type: self.declared_type.as_declared_type(),
            min: self.min.map(StatValue::to_proto),
            max: self.max.map(StatValue::to_proto),
            null_count: self.null_count,
        }
    }
}

/// The result of decoding a record's raw `declared_column_stats` list: the
/// entries that validated, plus a count of those dropped. `dropped` feeds the
/// defect metric (ADR-0873 decision 2); it is never a decode failure for the
/// record. An absent field decodes as `stats` empty and `dropped` zero, which
/// is a permanently legal state (a pre-stamp record), distinct from a present
/// entry.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DecodedDeclaredStats {
    pub stats: Vec<DeclaredColumnStat>,
    /// Entries dropped as malformed: an ineligible or unknown `declared_type`,
    /// a `min`/`max` oneof arm that disagrees with `declared_type`, or a
    /// present value message with no arm set.
    pub dropped: u64,
}

/// Validate and decode a record's raw `declared_column_stats`. Every entry is
/// checked (ADR-0873 decision 2): a violating entry is dropped and counted,
/// never trusted and never a decode failure for the record. Corrupt or
/// truncated bytes are rejected earlier, by `CommitRecord::decode`; by the
/// time an entry reaches here it is a structurally valid `DeclaredColumnMinMax`
/// whose *semantics* this function validates.
pub fn decode_stats(raw: &[DeclaredColumnMinMax]) -> DecodedDeclaredStats {
    let mut stats = Vec::with_capacity(raw.len());
    let mut dropped = 0u64;
    for entry in raw {
        match decode_entry(entry) {
            Some(stat) => stats.push(stat),
            None => dropped = dropped.saturating_add(1),
        }
    }
    DecodedDeclaredStats { stats, dropped }
}

fn decode_entry(entry: &DeclaredColumnMinMax) -> Option<DeclaredColumnStat> {
    let eligible = EligibleType::from_declared_type(entry.declared_type as i32)?;
    let min = decode_value(entry.min.as_ref(), eligible)?;
    let max = decode_value(entry.max.as_ref(), eligible)?;
    Some(DeclaredColumnStat {
        name: entry.name.clone(),
        declared_type: eligible,
        min,
        max,
        null_count: entry.null_count,
    })
}

/// `None` outer = absent value (legal). `Some(None)` cannot happen: a present
/// message with a mismatched or empty arm makes the whole entry invalid, so
/// this returns `None` (drop the entry) in that case, `Some(Some(v))` for a
/// valid present value, and `Some(None)` for a legitimately absent value.
fn decode_value(
    value: Option<&DeclaredColumnStatValue>,
    expected: EligibleType,
) -> Option<Option<StatValue>> {
    match value {
        None => Some(None),
        Some(v) => StatValue::from_proto(v, expected).ok().map(Some),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn allowlist_admits_only_i64_and_bool() {
        assert_eq!(
            EligibleType::from_declared_type(TypedAttrColumnType::I64 as i32),
            Some(EligibleType::I64)
        );
        assert_eq!(
            EligibleType::from_declared_type(TypedAttrColumnType::Bool as i32),
            Some(EligibleType::Bool)
        );
        // STR, BYTES, UNSPECIFIED: ineligible today.
        assert_eq!(
            EligibleType::from_declared_type(TypedAttrColumnType::Str as i32),
            None
        );
        assert_eq!(
            EligibleType::from_declared_type(TypedAttrColumnType::Bytes as i32),
            None
        );
        assert_eq!(
            EligibleType::from_declared_type(TypedAttrColumnType::Unspecified as i32),
            None
        );
        // ADR-0101's F64 = 5 is scheduled but deliberately absent from the
        // allowlist. Its discriminant is not yet in the generated enum, so it
        // is exercised as the raw value the shipped ADR-0101 writer will use.
        assert_eq!(EligibleType::from_declared_type(5), None);
        // An arbitrary future/corrupt discriminant is refused, not accepted.
        assert_eq!(EligibleType::from_declared_type(9999), None);
        assert_eq!(EligibleType::from_declared_type(-1), None);
    }

    #[test]
    fn stamp_refuses_ineligible_type_at_the_boundary() {
        let err = DeclaredColumnStat::stamp("msg", TypedAttrColumnType::Str as i32, None, None, 0)
            .expect_err("STR is not stamp-eligible");
        assert_eq!(
            err,
            StampError::IneligibleType(TypedAttrColumnType::Str as i32)
        );
        // F64 (ADR-0101, raw 5) is refused the same way.
        let err = DeclaredColumnStat::stamp("f", 5, None, None, 0)
            .expect_err("F64 is not in the allowlist");
        assert_eq!(err, StampError::IneligibleType(5));
    }

    #[test]
    fn stamp_refuses_value_kind_mismatch() {
        // Declared I64 but the min value is a Bool: refused at the boundary.
        let err = DeclaredColumnStat::stamp(
            "n",
            TypedAttrColumnType::I64 as i32,
            Some(StatValue::Bool(true)),
            None,
            0,
        )
        .expect_err("bool value under an I64 declaration is a kind mismatch");
        assert_eq!(
            err,
            StampError::KindMismatch {
                declared_type: EligibleType::I64
            }
        );
    }

    #[test]
    fn stamp_round_trips_through_wire() {
        let stat = DeclaredColumnStat::stamp(
            "EventDate",
            TypedAttrColumnType::I64 as i32,
            Some(StatValue::I64(-5)),
            Some(StatValue::I64(42)),
            7,
        )
        .expect("eligible I64 stamp");
        let decoded = decode_stats(&[stat.to_proto()]);
        assert_eq!(decoded.dropped, 0);
        assert_eq!(decoded.stats.len(), 1);
        assert_eq!(decoded.stats[0], stat);
        // Exact figures, not bare presence.
        assert_eq!(decoded.stats[0].min, Some(StatValue::I64(-5)));
        assert_eq!(decoded.stats[0].max, Some(StatValue::I64(42)));
        assert_eq!(decoded.stats[0].null_count, 7);
        assert_eq!(decoded.stats[0].non_null_count(10), 3);
    }

    #[test]
    fn zero_non_null_column_stamps_both_extrema_absent() {
        // A column present in the declared set but with no non-null value in
        // this object: both extrema absent, an exact statement, distinct from
        // "not stamped at all".
        let stat = DeclaredColumnStat::stamp("b", TypedAttrColumnType::Bool as i32, None, None, 4)
            .expect("eligible bool stamp");
        let proto = stat.to_proto();
        assert!(proto.min.is_none(), "min absent on the wire");
        assert!(proto.max.is_none(), "max absent on the wire");
        let decoded = decode_stats(&[proto]);
        assert_eq!(decoded.dropped, 0);
        assert_eq!(decoded.stats[0].min, None);
        assert_eq!(decoded.stats[0].max, None);
        assert_eq!(decoded.stats[0].null_count, 4);
    }

    #[test]
    fn decode_drops_ineligible_declared_type_and_counts_it() {
        // A hand-built wire entry an eligible writer would never produce: a
        // STR declared_type. The decoder drops it, counts one defect, and does
        // not fail the record.
        let bad = DeclaredColumnMinMax {
            name: "s".to_string(),
            declared_type: TypedAttrColumnType::Str as u32,
            min: None,
            max: None,
            null_count: 0,
        };
        let decoded = decode_stats(&[bad]);
        assert_eq!(decoded.dropped, 1);
        assert!(decoded.stats.is_empty());
    }

    #[test]
    fn decode_drops_kind_mismatched_value_and_counts_it() {
        // declared_type I64 but the min arm carries a bool: mismatch, dropped.
        let i64_with_bool_min = DeclaredColumnMinMax {
            name: "m".to_string(),
            declared_type: TypedAttrColumnType::I64 as u32,
            min: Some(DeclaredColumnStatValue {
                kind: Some(Kind::B(true)),
            }),
            max: None,
            null_count: 0,
        };
        // The reverse direction too: declared_type Bool but the max arm is I64.
        let bool_with_i64_max = DeclaredColumnMinMax {
            name: "m2".to_string(),
            declared_type: TypedAttrColumnType::Bool as u32,
            min: None,
            max: Some(DeclaredColumnStatValue {
                kind: Some(Kind::I64(1)),
            }),
            null_count: 0,
        };
        let decoded = decode_stats(&[i64_with_bool_min, bool_with_i64_max]);
        assert_eq!(decoded.dropped, 2);
        assert!(decoded.stats.is_empty());
    }

    #[test]
    fn decode_drops_present_value_with_empty_oneof() {
        // A present min message with no arm set is malformed: dropped.
        let bad = DeclaredColumnMinMax {
            name: "e".to_string(),
            declared_type: TypedAttrColumnType::I64 as u32,
            min: Some(DeclaredColumnStatValue { kind: None }),
            max: None,
            null_count: 0,
        };
        let decoded = decode_stats(&[bad]);
        assert_eq!(decoded.dropped, 1);
        assert!(decoded.stats.is_empty());
    }

    #[test]
    fn decode_partitions_valid_and_invalid_entries() {
        let good = DeclaredColumnStat::stamp(
            "ok",
            TypedAttrColumnType::I64 as i32,
            Some(StatValue::I64(1)),
            Some(StatValue::I64(9)),
            2,
        )
        .expect("valid")
        .to_proto();
        let bad = DeclaredColumnMinMax {
            name: "no".to_string(),
            declared_type: TypedAttrColumnType::Bytes as u32,
            min: None,
            max: None,
            null_count: 0,
        };
        let decoded = decode_stats(&[good, bad]);
        assert_eq!(decoded.dropped, 1);
        assert_eq!(decoded.stats.len(), 1);
        assert_eq!(decoded.stats[0].name, "ok");
    }

    /// True when a present value's oneof arm matches `expected`; vacuously
    /// true for an absent value. Mirrors the decoder's kind check, in the test
    /// so the property can predict a drop.
    fn value_kind_matches(value: Option<&DeclaredColumnStatValue>, expected: EligibleType) -> bool {
        match value.and_then(|v| v.kind.as_ref()) {
            None => value.is_none(), // absent value: fine; present-but-empty: not
            Some(Kind::I64(_)) => expected == EligibleType::I64,
            Some(Kind::B(_)) => expected == EligibleType::Bool,
        }
    }

    /// An arbitrary wire value: absent, a present-but-empty message (malformed),
    /// an I64 arm, or a Bool arm.
    fn arbitrary_wire_value() -> impl Strategy<Value = Option<DeclaredColumnStatValue>> {
        prop_oneof![
            Just(None),
            Just(Some(DeclaredColumnStatValue { kind: None })),
            any::<i64>().prop_map(|v| Some(DeclaredColumnStatValue {
                kind: Some(Kind::I64(v))
            })),
            any::<bool>().prop_map(|v| Some(DeclaredColumnStatValue {
                kind: Some(Kind::B(v))
            })),
        ]
    }

    fn eligible_stat_strategy() -> impl Strategy<Value = DeclaredColumnStat> {
        let i64_stat = (
            ".*",
            proptest::option::of(any::<i64>()),
            proptest::option::of(any::<i64>()),
            any::<u64>(),
        )
            .prop_map(|(name, min, max, null_count)| {
                DeclaredColumnStat::stamp(
                    name,
                    TypedAttrColumnType::I64 as i32,
                    min.map(StatValue::I64),
                    max.map(StatValue::I64),
                    null_count,
                )
                .expect("I64 stamp is always eligible")
            });
        let bool_stat = (
            ".*",
            proptest::option::of(any::<bool>()),
            proptest::option::of(any::<bool>()),
            any::<u64>(),
        )
            .prop_map(|(name, min, max, null_count)| {
                DeclaredColumnStat::stamp(
                    name,
                    TypedAttrColumnType::Bool as i32,
                    min.map(StatValue::Bool),
                    max.map(StatValue::Bool),
                    null_count,
                )
                .expect("Bool stamp is always eligible")
            });
        prop_oneof![i64_stat, bool_stat]
    }

    proptest! {
        // Every eligible stamp survives to_proto/decode_stats byte-for-byte,
        // over the full i64/bool/name/null-count domain, with zero drops.
        #[test]
        fn eligible_stamps_round_trip(stats in prop::collection::vec(eligible_stat_strategy(), 0..8)) {
            let wire: Vec<DeclaredColumnMinMax> = stats.iter().map(DeclaredColumnStat::to_proto).collect();
            let decoded = decode_stats(&wire);
            prop_assert_eq!(decoded.dropped, 0);
            prop_assert_eq!(decoded.stats, stats);
        }

        // A wire entry whose declared_type is drawn to hit the eligible values
        // ({2, 3}) far more often than chance, with independently-kinded value
        // arms, never panics and never yields a stamp whose value kind
        // disagrees with its declared type. Every malformed combination is
        // dropped, every surviving one is internally consistent.
        #[test]
        fn arbitrary_wire_entries_never_panic_and_stay_consistent(
            declared_type in prop_oneof![
                Just(TypedAttrColumnType::I64 as u32),
                Just(TypedAttrColumnType::Bool as u32),
                Just(TypedAttrColumnType::Str as u32),
                any::<u32>(),
            ],
            name in ".*",
            null_count in any::<u64>(),
            min in arbitrary_wire_value(),
            max in arbitrary_wire_value(),
        ) {
            let expected_eligible = EligibleType::from_declared_type(declared_type as i32);
            let entry = DeclaredColumnMinMax {
                name,
                declared_type,
                min: min.clone(),
                max: max.clone(),
                null_count,
            };
            let decoded = decode_stats(std::slice::from_ref(&entry));
            prop_assert_eq!(decoded.stats.len() + decoded.dropped as usize, 1);
            match decoded.stats.first() {
                None => {
                    // Dropped: either the type was ineligible, or a present
                    // value's kind disagreed with the eligible declared type.
                    let kind_ok = expected_eligible.is_some_and(|e| {
                        value_kind_matches(min.as_ref(), e) && value_kind_matches(max.as_ref(), e)
                    });
                    prop_assert!(!kind_ok, "a fully-consistent eligible entry must not be dropped");
                }
                Some(stat) => {
                    prop_assert_eq!(Some(stat.declared_type), expected_eligible);
                    if let Some(v) = stat.min {
                        prop_assert_eq!(v.eligible_type(), stat.declared_type);
                    }
                    if let Some(v) = stat.max {
                        prop_assert_eq!(v.eligible_type(), stat.declared_type);
                    }
                }
            }
        }
    }
}
