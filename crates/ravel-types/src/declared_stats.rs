//! Stamp-eligible declared-column statistics (ADR-0873): the exact
//! whole-object min/max and null count a writer records for one declared
//! typed attribute column (ADR-0090).
//!
//! This module owns the eligibility allowlist, single-sourced here the way
//! format version constants are (ADR-0066 decision 1) so writers, the
//! commit/compaction wire layer, the fold, and the query planner cannot
//! disagree about which declared types may carry a stamp. The allowlist is
//! explicit rather than a consequence of the declarable-type vocabulary:
//! ADR-0101 is Accepted and adds `TYPED_ATTR_COLUMN_TYPE_F64 = 5`, whose
//! comparator, NaN rule, and `-0.0` rule are undecided, so it must be
//! refused by name and not by absence.
//!
//! [`DeclaredColumnStat`] makes an ineligible or internally inconsistent stat
//! unrepresentable: [`DeclaredColumnStat::new`] is the only constructor, and
//! it refuses an ineligible declared type, a value whose kind disagrees with
//! the declared type, a half-present min/max pair, and a min above its max.

use std::fmt;

/// `ravel.sys.v1.TypedAttrColumnType` discriminants, as the `uint32` tag the
/// commit-family and catalog protobufs carry (this repository has no
/// cross-file proto imports, so the enum travels as its tag).
///
/// All five are named, including the two ineligible declarable types and
/// ADR-0101's unshipped `F64`, so [`DeclaredStatType::from_tag`] refuses each
/// deliberately.
pub const TYPED_ATTR_COLUMN_TYPE_UNSPECIFIED: u32 = 0;
/// UTF-8 string column (ADR-0090). Not stamp-eligible.
pub const TYPED_ATTR_COLUMN_TYPE_STR: u32 = 1;
/// Signed 64-bit integer column (ADR-0090). Stamp-eligible.
pub const TYPED_ATTR_COLUMN_TYPE_I64: u32 = 2;
/// Boolean column (ADR-0090). Stamp-eligible.
pub const TYPED_ATTR_COLUMN_TYPE_BOOL: u32 = 3;
/// Opaque bytes column (ADR-0090). Not stamp-eligible.
pub const TYPED_ATTR_COLUMN_TYPE_BYTES: u32 = 4;
/// Declarable `f64` column (ADR-0101, Accepted, writer release unshipped).
/// Not stamp-eligible: admitting it requires a decided total order, NaN rule,
/// and `-0.0` rule that provably agree with the scan-path aggregate
/// (ADR-0873 decision 2).
pub const TYPED_ATTR_COLUMN_TYPE_F64: u32 = 5;

/// A declared typed attribute column type that may carry a stamped exact
/// min/max: the ADR-0873 decision 2 allowlist, in full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeclaredStatType {
    /// Signed 64-bit integer column.
    I64,
    /// Boolean column, ordered `false` before `true`.
    Bool,
}

impl DeclaredStatType {
    /// The `ravel.sys.v1.TypedAttrColumnType` tag this type is stored as.
    pub const fn tag(self) -> u32 {
        match self {
            DeclaredStatType::I64 => TYPED_ATTR_COLUMN_TYPE_I64,
            DeclaredStatType::Bool => TYPED_ATTR_COLUMN_TYPE_BOOL,
        }
    }

    /// Resolve a stored `TypedAttrColumnType` tag against the allowlist.
    ///
    /// Every ineligible declarable type is refused by name, so a future
    /// eligible type is a deliberate edit here rather than something that
    /// starts working the moment its tag exists.
    pub fn from_tag(tag: u32) -> Result<Self, DeclaredStatError> {
        match tag {
            TYPED_ATTR_COLUMN_TYPE_I64 => Ok(DeclaredStatType::I64),
            TYPED_ATTR_COLUMN_TYPE_BOOL => Ok(DeclaredStatType::Bool),
            TYPED_ATTR_COLUMN_TYPE_UNSPECIFIED
            | TYPED_ATTR_COLUMN_TYPE_STR
            | TYPED_ATTR_COLUMN_TYPE_BYTES
            | TYPED_ATTR_COLUMN_TYPE_F64 => Err(DeclaredStatError::IneligibleType { tag }),
            _ => Err(DeclaredStatError::UnknownType { tag }),
        }
    }
}

impl fmt::Display for DeclaredStatType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeclaredStatType::I64 => f.write_str("I64"),
            DeclaredStatType::Bool => f.write_str("BOOL"),
        }
    }
}

/// One extremum of a stamp-eligible declared column. Every variant has a
/// total order with no NaN-shaped hazard, which is what eligibility means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeclaredStatValue {
    /// A signed 64-bit integer extremum.
    I64(i64),
    /// A boolean extremum (`false` orders before `true`).
    Bool(bool),
}

impl DeclaredStatValue {
    /// The declared type this value belongs to.
    pub const fn stat_type(self) -> DeclaredStatType {
        match self {
            DeclaredStatValue::I64(_) => DeclaredStatType::I64,
            DeclaredStatValue::Bool(_) => DeclaredStatType::Bool,
        }
    }

    /// Total-order rank within one declared type. Comparing ranks across
    /// types is meaningless, so callers compare only after establishing that
    /// both values carry the same [`DeclaredStatType`].
    const fn order_rank(self) -> i64 {
        match self {
            DeclaredStatValue::I64(v) => v,
            DeclaredStatValue::Bool(false) => 0,
            DeclaredStatValue::Bool(true) => 1,
        }
    }
}

/// Why a declared-column stat was refused.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum DeclaredStatError {
    #[error(
        "declared column type tag {tag} is not stamp-eligible: only I64 (2) and BOOL (3) may carry an exact stamped min/max (ADR-0873 decision 2)"
    )]
    IneligibleType { tag: u32 },
    #[error("unknown declared column type tag {tag}")]
    UnknownType { tag: u32 },
    #[error("empty declared column name: a stat always names the column it describes")]
    EmptyName,
    #[error(
        "declared column {name:?}: min is {min_state} but max is {max_state}; both are present, or both are absent (zero non-null values)"
    )]
    PresenceMismatch {
        name: String,
        min_state: &'static str,
        max_state: &'static str,
    },
    #[error("declared column {name:?} is declared {declared} but carries a {actual} {which} value")]
    ValueTypeMismatch {
        name: String,
        declared: DeclaredStatType,
        actual: DeclaredStatType,
        which: &'static str,
    },
    #[error("declared column {name:?}: min {min:?} is greater than max {max:?}")]
    MinAboveMax {
        name: String,
        min: DeclaredStatValue,
        max: DeclaredStatValue,
    },
}

/// Exact whole-object statistics for one declared typed attribute column: the
/// min and max of its non-null values plus an exact null count, as computed by
/// the writer that encoded the object (ADR-0873).
///
/// `min` and `max` are both absent exactly when the column had zero non-null
/// values in the object. That is an exact statement, not missing data: absent
/// extrema with `null_count` equal to the object's row count says the column
/// read NULL for every row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclaredColumnStat {
    name: String,
    declared_type: DeclaredStatType,
    min: Option<DeclaredStatValue>,
    max: Option<DeclaredStatValue>,
    null_count: u64,
}

impl DeclaredColumnStat {
    /// Build a stat, refusing everything the wire format can express but the
    /// contract does not admit: an empty name, a value whose kind disagrees
    /// with `declared_type`, one extremum present without the other, and a
    /// min above its max.
    pub fn new(
        name: impl Into<String>,
        declared_type: DeclaredStatType,
        min: Option<DeclaredStatValue>,
        max: Option<DeclaredStatValue>,
        null_count: u64,
    ) -> Result<Self, DeclaredStatError> {
        let name = name.into();
        if name.is_empty() {
            return Err(DeclaredStatError::EmptyName);
        }
        for (value, which) in [(min, "min"), (max, "max")] {
            if let Some(value) = value
                && value.stat_type() != declared_type
            {
                return Err(DeclaredStatError::ValueTypeMismatch {
                    name: name.clone(),
                    declared: declared_type,
                    actual: value.stat_type(),
                    which,
                });
            }
        }
        match (min, max) {
            (Some(min), Some(max)) => {
                if min.order_rank() > max.order_rank() {
                    return Err(DeclaredStatError::MinAboveMax { name, min, max });
                }
            }
            (None, None) => {}
            (min, max) => {
                let state =
                    |v: Option<DeclaredStatValue>| if v.is_some() { "present" } else { "absent" };
                return Err(DeclaredStatError::PresenceMismatch {
                    min_state: state(min),
                    max_state: state(max),
                    name,
                });
            }
        }
        Ok(DeclaredColumnStat {
            name,
            declared_type,
            min,
            max,
            null_count,
        })
    }

    /// The declared column's name: the attribute key verbatim, which is also
    /// its SQL column name (ADR-0090).
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The declared type, always one of the allowlisted ones.
    pub const fn declared_type(&self) -> DeclaredStatType {
        self.declared_type
    }

    /// The exact minimum of the column's non-null values, or `None` when the
    /// object held no non-null value for it.
    pub const fn min(&self) -> Option<DeclaredStatValue> {
        self.min
    }

    /// The exact maximum of the column's non-null values, or `None` when the
    /// object held no non-null value for it.
    pub const fn max(&self) -> Option<DeclaredStatValue> {
        self.max
    }

    /// Exact count of rows in the object where the column reads NULL (the
    /// attribute is absent, or its stored variant mismatches the declared
    /// type).
    pub const fn null_count(&self) -> u64 {
        self.null_count
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn allowlist_admits_exactly_i64_and_bool() {
        assert_eq!(
            DeclaredStatType::from_tag(TYPED_ATTR_COLUMN_TYPE_I64),
            Ok(DeclaredStatType::I64)
        );
        assert_eq!(
            DeclaredStatType::from_tag(TYPED_ATTR_COLUMN_TYPE_BOOL),
            Ok(DeclaredStatType::Bool)
        );
        for tag in [
            TYPED_ATTR_COLUMN_TYPE_UNSPECIFIED,
            TYPED_ATTR_COLUMN_TYPE_STR,
            TYPED_ATTR_COLUMN_TYPE_BYTES,
            TYPED_ATTR_COLUMN_TYPE_F64,
        ] {
            assert_eq!(
                DeclaredStatType::from_tag(tag),
                Err(DeclaredStatError::IneligibleType { tag }),
                "tag {tag} must be refused by name"
            );
        }
        assert_eq!(
            DeclaredStatType::from_tag(6),
            Err(DeclaredStatError::UnknownType { tag: 6 })
        );
    }

    #[test]
    fn tag_round_trips_for_every_eligible_type() {
        for ty in [DeclaredStatType::I64, DeclaredStatType::Bool] {
            assert_eq!(DeclaredStatType::from_tag(ty.tag()), Ok(ty));
        }
        assert_eq!(DeclaredStatType::I64.tag(), 2);
        assert_eq!(DeclaredStatType::Bool.tag(), 3);
    }

    #[test]
    fn new_accepts_absent_extrema_as_an_exact_all_null_statement() {
        let stat = DeclaredColumnStat::new("EventDate", DeclaredStatType::I64, None, None, 7)
            .expect("all-null column is exact");
        assert_eq!(stat.min(), None);
        assert_eq!(stat.max(), None);
        assert_eq!(stat.null_count(), 7);
    }

    #[test]
    fn new_rejects_value_type_mismatch() {
        let err = DeclaredColumnStat::new(
            "flag",
            DeclaredStatType::Bool,
            Some(DeclaredStatValue::I64(1)),
            Some(DeclaredStatValue::Bool(true)),
            0,
        )
        .expect_err("an I64 min under a BOOL declaration is refused");
        assert_eq!(
            err,
            DeclaredStatError::ValueTypeMismatch {
                name: "flag".to_string(),
                declared: DeclaredStatType::Bool,
                actual: DeclaredStatType::I64,
                which: "min",
            }
        );
    }

    #[test]
    fn new_rejects_half_present_extrema() {
        let err = DeclaredColumnStat::new(
            "EventDate",
            DeclaredStatType::I64,
            Some(DeclaredStatValue::I64(1)),
            None,
            0,
        )
        .expect_err("min without max is refused");
        assert_eq!(
            err,
            DeclaredStatError::PresenceMismatch {
                name: "EventDate".to_string(),
                min_state: "present",
                max_state: "absent",
            }
        );
    }

    #[test]
    fn new_rejects_min_above_max() {
        let err = DeclaredColumnStat::new(
            "EventDate",
            DeclaredStatType::I64,
            Some(DeclaredStatValue::I64(9)),
            Some(DeclaredStatValue::I64(8)),
            0,
        )
        .expect_err("min above max is refused");
        assert_eq!(
            err,
            DeclaredStatError::MinAboveMax {
                name: "EventDate".to_string(),
                min: DeclaredStatValue::I64(9),
                max: DeclaredStatValue::I64(8),
            }
        );
        // false < true: a BOOL column ordered the other way is refused too.
        let err = DeclaredColumnStat::new(
            "flag",
            DeclaredStatType::Bool,
            Some(DeclaredStatValue::Bool(true)),
            Some(DeclaredStatValue::Bool(false)),
            0,
        )
        .expect_err("true..false is refused");
        assert_eq!(
            err,
            DeclaredStatError::MinAboveMax {
                name: "flag".to_string(),
                min: DeclaredStatValue::Bool(true),
                max: DeclaredStatValue::Bool(false),
            }
        );
    }

    #[test]
    fn new_rejects_empty_name() {
        assert_eq!(
            DeclaredColumnStat::new("", DeclaredStatType::I64, None, None, 0),
            Err(DeclaredStatError::EmptyName)
        );
    }

    #[test]
    fn extremes_are_representable() {
        let stat = DeclaredColumnStat::new(
            "EventDate",
            DeclaredStatType::I64,
            Some(DeclaredStatValue::I64(i64::MIN)),
            Some(DeclaredStatValue::I64(i64::MAX)),
            u64::MAX,
        )
        .expect("full i64 span is valid");
        assert_eq!(stat.min(), Some(DeclaredStatValue::I64(i64::MIN)));
        assert_eq!(stat.max(), Some(DeclaredStatValue::I64(i64::MAX)));
        assert_eq!(stat.null_count(), u64::MAX);
    }
}
