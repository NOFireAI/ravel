//! Plan-time segment skipping by declared-column statistics (ADR-2121 D1).
//!
//! [`crate::logs_provider::LogsTableProvider`] drops a segment before
//! `LogsScanExec` is built when one prune-only [`Predicate::NumRange`] arm
//! that [`crate::logs_pushdown::extract_logs`] produced is provably disjoint
//! from the segment's exact `[min, max]` for that column. The arms are the
//! reader's own block-pruning arms (declared `I64`/`Bool` comparisons and
//! `BETWEEN`, the `I64` `IN` envelope), and they are intersected exactly as the
//! reader intersects them, so one disjoint arm is enough. The `ts` window is
//! not an arm here: the provider already drops a ts-irrelevant segment by its
//! catalog event-time span before this runs.
//!
//! The segment's min/max is the one its `SegmentRef` stamp carries, read
//! through [`crate::logs_scan::segment_declared_coverage`] so a `.cstat` entry
//! that conflicts with the stamp declines the column, the same conflict rule
//! `LogsScanExec::partition_statistics` applies. A `.cstat` entry alone never
//! skips a segment: it tallies only the record-level cells, while SQL returns
//! the merged value (the record cell, or the resource or scope value a row
//! falls back to) and the stamp is folded from that merged value, so a row
//! whose value lives only in the resource or scope attributes can match
//! outside the `.cstat` `[min, max]`. A segment with no stamp for the column,
//! a conflicting `.cstat` entry, or a `Str` column is never skipped by that
//! column's arm; another column's arm or the `ts` window can still drop it. A
//! segment whose column holds no non-null value is skippable for any arm,
//! because NULL satisfies no comparison, and the stamp proves its NULL count
//! equal to the segment's row count.
//!
//! Skipping only ever removes rows no arm admits, and every arm is implied by
//! a filter DataFusion still evaluates above the scan, so a skipped segment
//! could not have contributed a row.

use datafusion::scalar::ScalarValue;
use ravel_catalog::{LoadedColumnStats, SegmentRef};
use ravel_logseg::{FieldSel, FieldType, Predicate};
use ravel_proto::catalog::v1::ColumnStatsSegment;

use crate::declared::{DeclaredColumn, DeclaredType};
use crate::logs_scan::{segment_column_stats, segment_declared_coverage, stamp_coverage};

/// One `NumRange` arm resolved to the declared column it constrains, with its
/// inclusive bounds decoded back to i64 space (a `Bool` as `0`/`1`).
struct StatsArm<'a> {
    column: &'a DeclaredColumn,
    min: Option<i64>,
    max: Option<i64>,
}

/// The `NumRange` arms in `prune` that name a declared column of the arm's
/// exact type. An arm on an undeclared name or of another type resolves to
/// nothing and so skips nothing, matching the reader's resolution of an arm to
/// one `(name, type)` column.
fn resolve_arms<'a>(prune: &[Predicate], declared: &'a [DeclaredColumn]) -> Vec<StatsArm<'a>> {
    prune
        .iter()
        .filter_map(|p| {
            let Predicate::NumRange {
                field: FieldSel::Attr(name),
                ty,
                min,
                max,
            } = p
            else {
                return None;
            };
            let column = declared.iter().find(|d| &d.key == name)?;
            let type_matches = matches!(
                (column.ty, ty),
                (DeclaredType::I64, FieldType::I64) | (DeclaredType::Bool, FieldType::Bool)
            );
            type_matches.then(|| StatsArm {
                column,
                // The bit-pattern encoding `extract_logs` builds: an i64 as its
                // two's-complement u64, a bool as 0/1, so the cast back is exact.
                min: min.map(|b| b as i64),
                max: max.map(|b| b as i64),
            })
        })
        .collect()
}

/// A stamped extremum in the i64 space the arms are in.
fn scalar_i64(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Boolean(Some(b)) => Some(i64::from(*b)),
        _ => None,
    }
}

/// Whether `arm` proves `seg` holds no row it admits.
fn arm_excludes(
    arm: &StatsArm<'_>,
    seg: &SegmentRef,
    seg_stats: Option<&ColumnStatsSegment>,
) -> bool {
    if stamp_coverage(arm.column, &seg.declared_column_stats).is_none() {
        return false;
    }
    // With a stamp present this is the stamp's triple, or `None` when a
    // `.cstat` entry disagrees with it.
    let Some(coverage) = segment_declared_coverage(arm.column, seg, seg_stats) else {
        return false;
    };
    match (coverage.min.as_ref(), coverage.max.as_ref()) {
        (Some(min), Some(max)) => {
            let (Some(seg_min), Some(seg_max)) = (scalar_i64(min), scalar_i64(max)) else {
                return false;
            };
            arm.max.is_some_and(|q| q < seg_min) || arm.min.is_some_and(|q| q > seg_max)
        }
        (None, None) => coverage.null_count_proven && coverage.null_count == seg.sample_count,
        _ => false,
    }
}

/// Split `segments` into the ones some arm in `prune` does not exclude, in
/// their original order, and the count of those it excludes.
pub(crate) fn prune_segments_by_stats(
    segments: Vec<SegmentRef>,
    prune: &[Predicate],
    declared: &[DeclaredColumn],
    column_stats: Option<&LoadedColumnStats>,
) -> (Vec<SegmentRef>, usize) {
    let arms = resolve_arms(prune, declared);
    if arms.is_empty() {
        return (segments, 0);
    }
    let before = segments.len();
    let kept: Vec<SegmentRef> = segments
        .into_iter()
        .filter(|seg| {
            let seg_stats = segment_column_stats(column_stats, seg);
            !arms.iter().any(|arm| arm_excludes(arm, seg, seg_stats))
        })
        .collect();
    let pruned = before - kept.len();
    (kept, pruned)
}
