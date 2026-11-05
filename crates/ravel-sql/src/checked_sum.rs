//! Checked integer SUM (ADR-0022, integer-sum amendment; issue #3838).
//!
//! DataFusion 55.2.0's built-in `sum` adds integers with `add_wrapping`, so a
//! group whose total leaves its result type returns a wrapped value with no
//! error: `i64::MAX + 1` sums to `i64::MIN`, `u64::MAX + 1` to 0, at any
//! partition count. This module replaces the built-in `sum` with a UDAF that
//! owns the two integer result types the built-in's coercion produces:
//! `Int64` (from `Int8`-`Int64` arguments) and `UInt64` (from `UInt8`-`UInt64`
//! arguments). Every other argument type (Float64, Decimal, Duration,
//! Interval) delegates to the wrapped built-in unchanged; float sum's
//! reduction order is ADR-0024's subject.
//!
//! # Exact accumulation, one range check
//!
//! Every owned accumulator sums in `i128`, which holds any `i64` or `u64`
//! value, and checks the total against the result type once, when the result
//! is produced. The result is therefore a function of the input multiset
//! alone: an order or a partition split whose running `i64` total would leave
//! the range on the way to an in-range total (`i64::MAX`, then `1`, then `-1`)
//! returns the exact total, and a total outside the range fails the query
//! with [`SqlError::IntegerSumOverflow`] at every partition count. The `i128`
//! additions themselves are checked too: overflowing `i128` takes more than
//! 2^63 rows in one group, and if it happened the query would fail with the
//! same error rather than wrap.
//!
//! Partial state is one nullable `Decimal128(38, 0)` holding the exact `i128`
//! sum (NULL for a group or partition that saw no non-null row), so a merge
//! across partitions adds exact values. Packing into `Decimal128(38, 0)` is
//! range-checked; a partial beyond 38 digits fails the same way.
//!
//! # Paths
//!
//! * Ungrouped: [`CheckedSumAccumulator`].
//! * Grouped: [`CheckedSumGroupsAccumulator`], a real `GroupsAccumulator`
//!   with a flat `Vec<i128>` of sums. A batch with no nulls and no filter runs
//!   one loop of overflowing `i128` adds that ORs each overflow bit into one
//!   flag tested after the loop, so the per-row work beyond the built-in's is
//!   the carry into the high word and the flag. Group presence is tracked only
//!   once a NULL or a filter is seen, as the built-in's `NullState` does.
//!   `convert_to_state` keeps the skip-partial-aggregation path.
//! * `DISTINCT`: [`CheckedDistinctSumAccumulator`], a set of the distinct
//!   values merged by union, summed exactly when evaluated. It has no
//!   `GroupsAccumulator`, so grouped execution runs it behind
//!   `GroupsAccumulatorAdapter`, as the built-in's distinct sum does.
//! * `FILTER (WHERE ...)`: the grouped path skips filtered rows itself; the
//!   ungrouped path receives rows DataFusion has already filtered.
//! * Window frames: a frame that starts at UNBOUNDED PRECEDING runs
//!   [`CheckedSumAccumulator`]; a moving frame runs
//!   [`CheckedSlidingSumAccumulator`] (or
//!   [`CheckedSlidingDistinctSumAccumulator`] for `DISTINCT`), which adds and
//!   retracts exactly in `i128` and range-checks each emitted frame total.
//!
//! The overflow check is a plain `Result`, never a `debug_assert`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, Decimal128Array, ListArray,
    PrimitiveArray,
};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowPrimitiveType, DataType, Decimal128Type, Field, FieldRef, Int64Type, UInt64Type,
};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::logical_expr::expr::AggregateFunction;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, Expr, GroupsAccumulator,
    Operator, ReversedUDAF, SetMonotonicity, Signature, StatisticsArgs,
};
use datafusion::scalar::ScalarValue;

use crate::error::SqlError;

/// The checked SUM UDAF, registered under `sum` to displace DataFusion's
/// built-in, whose integer addition wraps.
pub fn checked_sum_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(CheckedSum::new())
}

/// The largest unscaled magnitude `Decimal128(38, 0)` holds: `10^38 - 1`.
const DECIMAL128_MAX_UNSCALED: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

/// Which integer result type an owned call produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum IntKind {
    /// `Int64`, from a signed argument.
    Signed,
    /// `UInt64`, from an unsigned argument.
    Unsigned,
}

impl IntKind {
    /// The owned kind of a resolved return type, or `None` to delegate.
    fn of(return_type: &DataType) -> Option<Self> {
        match return_type {
            DataType::Int64 => Some(IntKind::Signed),
            DataType::UInt64 => Some(IntKind::Unsigned),
            _ => None,
        }
    }

    fn data_type(self) -> DataType {
        match self {
            IntKind::Signed => DataType::Int64,
            IntKind::Unsigned => DataType::UInt64,
        }
    }
}

/// What an owned accumulator needs to report an overflow: the result type and
/// the aggregate expression's display name.
#[derive(Debug, Clone)]
struct SumTarget {
    kind: IntKind,
    aggregate: String,
}

impl SumTarget {
    fn new(kind: IntKind, aggregate: &str) -> Self {
        Self {
            kind,
            aggregate: aggregate.to_string(),
        }
    }

    fn overflow(&self) -> DataFusionError {
        SqlError::IntegerSumOverflow {
            aggregate: self.aggregate.clone(),
            result_type: self.kind.data_type().to_string(),
        }
        .into()
    }

    /// The exact `sum` in the result type, or the overflow error.
    fn scalar(&self, sum: Option<i128>) -> DFResult<ScalarValue> {
        let Some(sum) = sum else {
            return Ok(match self.kind {
                IntKind::Signed => ScalarValue::Int64(None),
                IntKind::Unsigned => ScalarValue::UInt64(None),
            });
        };
        match self.kind {
            IntKind::Signed => i64::try_from(sum)
                .map(|value| ScalarValue::Int64(Some(value)))
                .map_err(|_| self.overflow()),
            IntKind::Unsigned => u64::try_from(sum)
                .map(|value| ScalarValue::UInt64(Some(value)))
                .map_err(|_| self.overflow()),
        }
    }

    /// `sum` as a `Decimal128(38, 0)` partial state value, or the overflow
    /// error when it has more than 38 digits.
    fn decimal_state(&self, sum: i128) -> DFResult<i128> {
        if (-DECIMAL128_MAX_UNSCALED..=DECIMAL128_MAX_UNSCALED).contains(&sum) {
            Ok(sum)
        } else {
            Err(self.overflow())
        }
    }

    fn checked_add(&self, acc: i128, value: i128) -> DFResult<i128> {
        acc.checked_add(value).ok_or_else(|| self.overflow())
    }
}

/// A SUM UDAF that owns integer results and delegates everything else to the
/// wrapped built-in.
///
/// Name, aliases, signature, return type, coercion, reversal, order
/// sensitivity, monotonicity, documentation and the `SUM(x + c)` rewrite all
/// forward to `inner`, so planning is unchanged. State fields and every
/// accumulator constructor diverge for owned results; `value_from_stats`
/// declines for them, so an integer sum is always computed by the checked
/// accumulators rather than read from a statistic.
#[derive(Debug)]
struct CheckedSum {
    inner: Arc<dyn AggregateUDFImpl>,
}

impl CheckedSum {
    fn new() -> Self {
        Self {
            inner: sum_udaf().inner().clone(),
        }
    }
}

// `AggregateUDFImpl` requires `DynEq`/`DynHash`; the impl carries no state of
// its own (`inner` is always the built-in `sum`), so all instances are equal.
impl PartialEq for CheckedSum {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for CheckedSum {}

impl std::hash::Hash for CheckedSum {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

impl AggregateUDFImpl for CheckedSum {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.inner.return_type(arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        match IntKind::of(args.return_type()) {
            Some(kind) if args.is_distinct => Ok(vec![Arc::new(Field::new_list(
                format!("{}[checked_sum_distinct]", args.name),
                Field::new_list_field(kind.data_type(), true),
                false,
            ))]),
            Some(_) => Ok(vec![Arc::new(Field::new(
                format!("{}{CHECKED_SUM_STATE_SUFFIX}", args.name),
                DataType::Decimal128(38, 0),
                true,
            ))]),
            None => self.inner.state_fields(args),
        }
    }

    fn accumulator(&self, args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        match owned_kind(&args) {
            Some(kind) if args.is_distinct => Ok(Box::new(CheckedDistinctSumAccumulator::new(
                SumTarget::new(kind, args.name),
            ))),
            Some(kind) => Ok(Box::new(CheckedSumAccumulator::new(SumTarget::new(
                kind, args.name,
            )))),
            None => self.inner.accumulator(args),
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        match owned_kind(&args) {
            Some(_) => !args.is_distinct,
            None => self.inner.groups_accumulator_supported(args),
        }
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        match owned_kind(&args) {
            Some(_) if args.is_distinct => Err(DataFusionError::Internal(
                "checked sum: no GroupsAccumulator exists for a DISTINCT call; \
                 groups_accumulator_supported returns false for it"
                    .to_string(),
            )),
            Some(kind) => Ok(Box::new(CheckedSumGroupsAccumulator::new(SumTarget::new(
                kind, args.name,
            )))),
            None => self.inner.create_groups_accumulator(args),
        }
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        match owned_kind(&args) {
            Some(kind) if args.is_distinct => Ok(Box::new(
                CheckedSlidingDistinctSumAccumulator::new(SumTarget::new(kind, args.name)),
            )),
            Some(kind) => Ok(Box::new(CheckedSlidingSumAccumulator::new(SumTarget::new(
                kind, args.name,
            )))),
            None => self.inner.create_sliding_accumulator(args),
        }
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_expr()
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.set_monotonicity(data_type)
    }

    fn simplify_expr_op_literal(
        &self,
        agg_function: &AggregateFunction,
        arg: &Expr,
        op: Operator,
        lit: &Expr,
        arg_is_left: bool,
    ) -> DFResult<Option<Expr>> {
        self.inner
            .simplify_expr_op_literal(agg_function, arg, op, lit, arg_is_left)
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        if IntKind::of(statistics_args.return_type).is_some() {
            return None;
        }
        self.inner.value_from_stats(statistics_args)
    }
}

/// The suffix of the non-distinct owned state field's name. `crate::memory`
/// reads it to recognise this accumulator's per-group layout.
pub(crate) const CHECKED_SUM_STATE_SUFFIX: &str = "[checked_sum]";

fn owned_kind(args: &AccumulatorArgs) -> Option<IntKind> {
    IntKind::of(args.return_field.data_type())
}

/// `array` as a `PrimitiveArray<T>`, cast when it arrives as another type.
/// After the built-in's coercion an owned argument is already `Int64` or
/// `UInt64`, so the cast is a guard, not a path.
fn primitive<T: ArrowPrimitiveType>(array: &ArrayRef) -> DFResult<PrimitiveArray<T>> {
    let array = if array.data_type() == &T::DATA_TYPE {
        Arc::clone(array)
    } else {
        cast(array, &T::DATA_TYPE)?
    };
    array.as_primitive_opt::<T>().cloned().ok_or_else(|| {
        DataFusionError::Internal(format!(
            "checked sum: argument did not cast to {}",
            T::DATA_TYPE
        ))
    })
}

fn decimal_state(array: &ArrayRef) -> DFResult<&Decimal128Array> {
    array.as_primitive_opt::<Decimal128Type>().ok_or_else(|| {
        DataFusionError::Internal("checked sum: partial state is not Decimal128".to_string())
    })
}

/// Call `visit` with every non-null value of `array` widened to `i128`.
fn for_each_value(
    kind: IntKind,
    array: &ArrayRef,
    mut visit: impl FnMut(i128) -> DFResult<()>,
) -> DFResult<()> {
    match kind {
        IntKind::Signed => {
            for value in primitive::<Int64Type>(array)?.iter().flatten() {
                visit(i128::from(value))?;
            }
        }
        IntKind::Unsigned => {
            for value in primitive::<UInt64Type>(array)?.iter().flatten() {
                visit(i128::from(value))?;
            }
        }
    }
    Ok(())
}

/// Ungrouped SUM over an owned integer argument, and the accumulator of a
/// window frame that starts at UNBOUNDED PRECEDING.
#[derive(Debug)]
struct CheckedSumAccumulator {
    target: SumTarget,
    /// The exact sum, `None` until the first non-null value or non-null
    /// partial state.
    sum: Option<i128>,
}

impl CheckedSumAccumulator {
    fn new(target: SumTarget) -> Self {
        Self { target, sum: None }
    }

    fn add(&mut self, value: i128) -> DFResult<()> {
        self.sum = Some(self.target.checked_add(self.sum.unwrap_or(0), value)?);
        Ok(())
    }
}

impl Accumulator for CheckedSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let kind = self.target.kind;
        for_each_value(kind, &values[0], |value| self.add(value))
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        for partial in decimal_state(&states[0])?.iter().flatten() {
            self.add(partial)?;
        }
        Ok(())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let sum = self
            .sum
            .map(|sum| self.target.decimal_state(sum))
            .transpose()?;
        Ok(vec![ScalarValue::Decimal128(sum, 38, 0)])
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        self.target.scalar(self.sum)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.target.aggregate.capacity()
    }
}

/// Which groups have seen a value. `All` holds until a batch carries a NULL or
/// a filter: until then every group index the accumulator has been told about
/// came from a row with a value, so no per-group flag is needed. Tracked
/// presence is one bit per group, so the per-group footprint stays within
/// `crate::memory`'s growth-step tolerance of the 16-byte sum.
#[derive(Debug)]
enum Seen {
    All,
    Tracked(BooleanBufferBuilder),
}

/// Grouped SUM over an owned integer argument: one exact `i128` per group.
#[derive(Debug)]
struct CheckedSumGroupsAccumulator {
    target: SumTarget,
    sums: Vec<i128>,
    seen: Seen,
}

impl CheckedSumGroupsAccumulator {
    fn new(target: SumTarget) -> Self {
        Self {
            target,
            sums: Vec::new(),
            seen: Seen::All,
        }
    }

    /// Grow to `total_num_groups`, switching from `All` to tracked presence
    /// when `track` is set. Every group known before the switch has seen a
    /// value.
    fn resize(&mut self, total_num_groups: usize, track: bool) {
        let known = self.sums.len();
        self.sums.resize(total_num_groups, 0);
        if track && matches!(self.seen, Seen::All) {
            let mut seen = BooleanBufferBuilder::new(total_num_groups);
            seen.append_n(known, true);
            self.seen = Seen::Tracked(seen);
        }
        if let Seen::Tracked(seen) = &mut self.seen
            && seen.len() < total_num_groups
        {
            seen.append_n(total_num_groups - seen.len(), false);
        }
    }

    /// Add `values[row]` into `sums[group_indices[row]]` for every row `nulls`
    /// (the union of the input's nulls and the filter) admits. Each add is an
    /// overflowing `i128` add whose overflow bit is ORed into one flag tested
    /// after the loop, so the loop carries no branch on the check.
    fn accumulate<V: Copy + Into<i128>>(
        &mut self,
        group_indices: &[usize],
        values: &[V],
        nulls: Option<&NullBuffer>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups, nulls.is_some());
        let mut overflow = false;
        let sums = &mut self.sums;
        match (&mut self.seen, nulls) {
            (Seen::Tracked(seen), Some(nulls)) => {
                for row in nulls.valid_indices() {
                    let group = group_indices[row];
                    let (sum, carry) = sums[group].overflowing_add(values[row].into());
                    sums[group] = sum;
                    seen.set_bit(group, true);
                    overflow |= carry;
                }
            }
            (Seen::Tracked(seen), None) => {
                for (&group, &value) in group_indices.iter().zip(values) {
                    let (sum, carry) = sums[group].overflowing_add(value.into());
                    sums[group] = sum;
                    seen.set_bit(group, true);
                    overflow |= carry;
                }
            }
            (Seen::All, Some(_)) => {
                return Err(DataFusionError::Internal(
                    "checked sum: a batch with NULL or filtered rows reached untracked presence"
                        .to_string(),
                ));
            }
            (Seen::All, None) => {
                for (&group, &value) in group_indices.iter().zip(values) {
                    let (sum, carry) = sums[group].overflowing_add(value.into());
                    sums[group] = sum;
                    overflow |= carry;
                }
            }
        }
        if overflow {
            return Err(self.target.overflow());
        }
        Ok(())
    }

    fn update_typed<T: ArrowPrimitiveType>(
        &mut self,
        array: &ArrayRef,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()>
    where
        T::Native: Into<i128>,
    {
        let array = primitive::<T>(array)?;
        let nulls = admitted_rows(array.nulls(), opt_filter);
        self.accumulate(
            group_indices,
            array.values(),
            nulls.as_ref(),
            total_num_groups,
        )
    }

    /// Take the groups `emit_to` selects: their sums and, when tracked, their
    /// presence bits.
    fn take(&mut self, emit_to: EmitTo) -> (Vec<i128>, Option<BooleanBuffer>) {
        let sums = emit_to.take_needed(&mut self.sums);
        let seen = match &mut self.seen {
            Seen::All => None,
            Seen::Tracked(seen) => {
                let all = seen.finish();
                Some(match emit_to {
                    EmitTo::All => all,
                    EmitTo::First(n) => {
                        seen.append_buffer(&all.slice(n, all.len() - n));
                        all.slice(0, n)
                    }
                })
            }
        };
        (sums, seen)
    }
}

/// The rows a batch admits, as a null buffer: the input's own nulls, plus
/// every row the filter does not select (a NULL filter slot selects nothing).
/// `None` when every row is admitted.
fn admitted_rows(nulls: Option<&NullBuffer>, opt_filter: Option<&BooleanArray>) -> Option<NullBuffer> {
    let filter = opt_filter.map(|filter| {
        let selected = match filter.nulls() {
            Some(filter_nulls) => filter_nulls.inner() & filter.values(),
            None => filter.values().clone(),
        };
        NullBuffer::from(selected)
    });
    NullBuffer::union(nulls, filter.as_ref())
}

impl GroupsAccumulator for CheckedSumGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        match self.target.kind {
            IntKind::Signed => self.update_typed::<Int64Type>(
                &values[0],
                group_indices,
                opt_filter,
                total_num_groups,
            ),
            IntKind::Unsigned => self.update_typed::<UInt64Type>(
                &values[0],
                group_indices,
                opt_filter,
                total_num_groups,
            ),
        }
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DFResult<()> {
        let partials = decimal_state(&values[0])?;
        self.accumulate(
            group_indices,
            partials.values(),
            partials.nulls(),
            total_num_groups,
        )
    }

    /// The sums narrowed to the result type, NULL for a group that saw no
    /// value (its sum is 0, which always fits).
    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let (sums, seen) = self.take(emit_to);
        let nulls = seen.map(NullBuffer::new);
        Ok(match self.target.kind {
            IntKind::Signed => {
                let values = narrow::<i64>(&sums).ok_or_else(|| self.target.overflow())?;
                Arc::new(PrimitiveArray::<Int64Type>::try_new(values.into(), nulls)?)
            }
            IntKind::Unsigned => {
                let values = narrow::<u64>(&sums).ok_or_else(|| self.target.overflow())?;
                Arc::new(PrimitiveArray::<UInt64Type>::try_new(values.into(), nulls)?)
            }
        })
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let (sums, seen) = self.take(emit_to);
        if sums
            .iter()
            .any(|sum| !(-DECIMAL128_MAX_UNSCALED..=DECIMAL128_MAX_UNSCALED).contains(sum))
        {
            return Err(self.target.overflow());
        }
        let state = Decimal128Array::try_new(sums.into(), seen.map(NullBuffer::new))?
            .with_precision_and_scale(38, 0)?;
        Ok(vec![Arc::new(state)])
    }

    /// One state row per input row, for a partial aggregation that passes rows
    /// through (`skip_partial_aggregation`): each admitted value widened to
    /// `i128`, NULL for a NULL or filtered row, which `merge_batch` skips.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> DFResult<Vec<ArrayRef>> {
        let state: Decimal128Array = match self.target.kind {
            IntKind::Signed => widen::<Int64Type>(&values[0], opt_filter)?,
            IntKind::Unsigned => widen::<UInt64Type>(&values[0], opt_filter)?,
        };
        Ok(vec![Arc::new(state.with_precision_and_scale(38, 0)?)])
    }

    fn size(&self) -> usize {
        let seen = match &self.seen {
            Seen::All => 0,
            Seen::Tracked(seen) => seen.capacity() / 8,
        };
        self.sums.capacity() * std::mem::size_of::<i128>() + seen
    }
}

/// Every sum narrowed to `N`, or `None` when any does not fit.
fn narrow<N: TryFrom<i128> + Default>(sums: &[i128]) -> Option<Vec<N>> {
    let mut fits = true;
    let narrowed = sums
        .iter()
        .map(|&sum| {
            N::try_from(sum).unwrap_or_else(|_| {
                fits = false;
                N::default()
            })
        })
        .collect();
    fits.then_some(narrowed)
}

/// `array` widened to `i128` state values, with the filter's unselected rows
/// set NULL.
fn widen<T: ArrowPrimitiveType>(
    array: &ArrayRef,
    opt_filter: Option<&BooleanArray>,
) -> DFResult<Decimal128Array>
where
    T::Native: Into<i128>,
{
    let array = primitive::<T>(array)?;
    let nulls = admitted_rows(array.nulls(), opt_filter);
    let values: Vec<i128> = array.values().iter().map(|&value| value.into()).collect();
    Ok(Decimal128Array::new(values.into(), nulls))
}

/// `SUM(DISTINCT x)` over an owned integer argument. The state is the set of
/// distinct values; partial sets merge by union and the set is summed exactly
/// when evaluated.
#[derive(Debug)]
struct CheckedDistinctSumAccumulator {
    target: SumTarget,
    values: HashSet<i128>,
}

impl CheckedDistinctSumAccumulator {
    fn new(target: SumTarget) -> Self {
        Self {
            target,
            values: HashSet::new(),
        }
    }

    fn insert(&mut self, array: &ArrayRef) -> DFResult<()> {
        let values = &mut self.values;
        for_each_value(self.target.kind, array, |value| {
            values.insert(value);
            Ok(())
        })
    }
}

impl Accumulator for CheckedDistinctSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.insert(&values[0])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        visit_distinct_lists(&states[0], |list| self.insert(list))
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        distinct_state(&self.target, self.values.iter().copied())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.values.is_empty() {
            return self.target.scalar(None);
        }
        let mut sum = 0i128;
        for &value in &self.values {
            sum = self.target.checked_add(sum, value)?;
        }
        self.target.scalar(Some(sum))
    }

    fn size(&self) -> usize {
        // One control byte per hash table slot on top of the value.
        std::mem::size_of_val(self)
            + self.target.aggregate.capacity()
            + self.values.capacity() * (std::mem::size_of::<i128>() + 1)
    }
}

/// The distinct state: one list of the values in the result type, which every
/// distinct value fits because it is one input value.
fn distinct_state(
    target: &SumTarget,
    values: impl Iterator<Item = i128>,
) -> DFResult<Vec<ScalarValue>> {
    let scalars = values
        .map(|value| target.scalar(Some(value)))
        .collect::<DFResult<Vec<_>>>()?;
    Ok(vec![ScalarValue::List(ScalarValue::new_list_nullable(
        &scalars,
        &target.kind.data_type(),
    ))])
}

fn visit_distinct_lists(
    state: &ArrayRef,
    mut visit: impl FnMut(&ArrayRef) -> DFResult<()>,
) -> DFResult<()> {
    let lists = state.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        DataFusionError::Internal("checked sum: distinct state is not a ListArray".to_string())
    })?;
    for row in 0..lists.len() {
        if lists.is_valid(row) {
            visit(&lists.value(row))?;
        }
    }
    Ok(())
}

/// SUM over a moving window frame: values enter with `update_batch` and leave
/// with `retract_batch`, both exact in `i128`; each frame total is
/// range-checked when emitted.
#[derive(Debug)]
struct CheckedSlidingSumAccumulator {
    target: SumTarget,
    sum: i128,
    /// Non-null values currently in the frame; zero means the frame sum is
    /// NULL.
    count: u64,
}

impl CheckedSlidingSumAccumulator {
    fn new(target: SumTarget) -> Self {
        Self {
            target,
            sum: 0,
            count: 0,
        }
    }
}

impl Accumulator for CheckedSlidingSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let target = &self.target;
        let (sum, count) = (&mut self.sum, &mut self.count);
        for_each_value(target.kind, &values[0], |value| {
            *sum = target.checked_add(*sum, value)?;
            *count += 1;
            Ok(())
        })
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let target = &self.target;
        let (sum, count) = (&mut self.sum, &mut self.count);
        for_each_value(target.kind, &values[0], |value| {
            *sum = target.checked_add(*sum, -value)?;
            *count = count.saturating_sub(1);
            Ok(())
        })
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        for partial in decimal_state(&states[0])?.iter().flatten() {
            self.sum = self.target.checked_add(self.sum, partial)?;
            self.count += 1;
        }
        Ok(())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let sum = (self.count > 0)
            .then(|| self.target.decimal_state(self.sum))
            .transpose()?;
        Ok(vec![ScalarValue::Decimal128(sum, 38, 0)])
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        self.target.scalar((self.count > 0).then_some(self.sum))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.target.aggregate.capacity()
    }
}

/// `SUM(DISTINCT x)` over a moving window frame: a count per distinct value
/// in the frame and the exact sum of the distinct values present.
#[derive(Debug)]
struct CheckedSlidingDistinctSumAccumulator {
    target: SumTarget,
    counts: HashMap<i128, u64>,
    sum: i128,
}

impl CheckedSlidingDistinctSumAccumulator {
    fn new(target: SumTarget) -> Self {
        Self {
            target,
            counts: HashMap::new(),
            sum: 0,
        }
    }

    fn enter(&mut self, value: i128) -> DFResult<()> {
        let count = self.counts.entry(value).or_insert(0);
        if *count == 0 {
            self.sum = self.target.checked_add(self.sum, value)?;
        }
        *count += 1;
        Ok(())
    }

    fn leave(&mut self, value: i128) -> DFResult<()> {
        if let Some(count) = self.counts.get_mut(&value) {
            *count -= 1;
            if *count == 0 {
                self.counts.remove(&value);
                self.sum = self.target.checked_add(self.sum, -value)?;
            }
        }
        Ok(())
    }

    fn values_of(&self, array: &ArrayRef) -> DFResult<Vec<i128>> {
        let mut values = Vec::with_capacity(array.len());
        for_each_value(self.target.kind, array, |value| {
            values.push(value);
            Ok(())
        })?;
        Ok(values)
    }
}

impl Accumulator for CheckedSlidingDistinctSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        for value in self.values_of(&values[0])? {
            self.enter(value)?;
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        for value in self.values_of(&values[0])? {
            self.leave(value)?;
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let mut merged = Vec::new();
        visit_distinct_lists(&states[0], |list| {
            merged.extend(self.values_of(list)?);
            Ok(())
        })?;
        for value in merged {
            self.enter(value)?;
        }
        Ok(())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        distinct_state(&self.target, self.counts.keys().copied())
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        self.target
            .scalar((!self.counts.is_empty()).then_some(self.sum))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.target.aggregate.capacity()
            + self.counts.capacity() * (std::mem::size_of::<(i128, u64)>() + 1)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch, UInt64Array};
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::displayable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;

    const I64_MAX: i128 = i64::MAX as i128;
    const U64_MAX: i128 = u64::MAX as i128;

    /// One row of `t`: its order key `o`, its group key `g` and its value `v`.
    type Row = (i64, i64, Option<i128>);

    /// `rows` in value order `0..n`, each its own order key, all in group 1.
    fn ungrouped(values: &[Option<i128>]) -> Vec<Row> {
        values
            .iter()
            .enumerate()
            .map(|(o, &v)| (o as i64, 1, v))
            .collect()
    }

    /// `t(o Int64, g Int64, v <value_type>)` with row `i` in partition
    /// `i % partitions`; a partition that gets no row holds an empty batch.
    fn table(value_type: &DataType, rows: &[Row], partitions: usize) -> MemTable {
        let schema = Arc::new(Schema::new(vec![
            Field::new("o", DataType::Int64, false),
            Field::new("g", DataType::Int64, false),
            Field::new("v", value_type.clone(), true),
        ]));
        let batches = (0..partitions)
            .map(|p| {
                let part: Vec<&Row> = rows.iter().skip(p).step_by(partitions).collect();
                let o = Int64Array::from_iter_values(part.iter().map(|r| r.0));
                let g = Int64Array::from_iter_values(part.iter().map(|r| r.1));
                let v: ArrayRef = match value_type {
                    DataType::Int64 => Arc::new(Int64Array::from_iter(
                        part.iter().map(|r| r.2.map(|v| i64::try_from(v).expect("i64"))),
                    )),
                    DataType::UInt64 => Arc::new(UInt64Array::from_iter(
                        part.iter().map(|r| r.2.map(|v| u64::try_from(v).expect("u64"))),
                    )),
                    DataType::Float64 => Arc::new(Float64Array::from_iter(
                        part.iter().map(|r| r.2.map(|v| v as f64)),
                    )),
                    other => panic!("no fixture for {other}"),
                };
                let batch =
                    RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(o), Arc::new(g), v])
                        .expect("batch");
                vec![batch]
            })
            .collect();
        MemTable::try_new(schema, batches).expect("memtable")
    }

    fn context(value_type: &DataType, rows: &[Row], partitions: usize) -> SessionContext {
        let ctx = SessionContext::new_with_config(
            SessionConfig::new().with_target_partitions(partitions),
        );
        // Displace the built-in `sum`, exactly as `build_session` does.
        ctx.register_udaf(checked_sum_udaf());
        ctx.register_table("t", Arc::new(table(value_type, rows, partitions)))
            .expect("register");
        ctx
    }

    /// The first column of `sql`'s result over `t`, row by row.
    async fn query(
        sql: &str,
        value_type: &DataType,
        rows: &[Row],
        partitions: usize,
    ) -> DFResult<Vec<ScalarValue>> {
        let batches = context(value_type, rows, partitions)
            .sql(sql)
            .await?
            .collect()
            .await?;
        let mut out = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                out.push(ScalarValue::try_from_array(batch.column(0), row)?);
            }
        }
        Ok(out)
    }

    /// The physical plan of `sql`, indented.
    async fn plan(sql: &str, value_type: &DataType, rows: &[Row], partitions: usize) -> String {
        let plan = context(value_type, rows, partitions)
            .sql(sql)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        displayable(plan.as_ref()).indent(true).to_string()
    }

    fn sql_error(err: &DataFusionError) -> Option<&SqlError> {
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(err);
        while let Some(current) = cause {
            if let Some(sql) = current.downcast_ref::<SqlError>() {
                return Some(sql);
            }
            cause = current.source();
        }
        None
    }

    /// `result` failed with the typed overflow error for `aggregate` and
    /// `result_type`, and returned no value.
    fn assert_overflow(result: DFResult<Vec<ScalarValue>>, aggregate: &str, result_type: &str) {
        let err = match result {
            Ok(values) => panic!("expected an integer sum overflow, got {values:?}"),
            Err(err) => err,
        };
        match sql_error(&err) {
            Some(SqlError::IntegerSumOverflow {
                aggregate: got_aggregate,
                result_type: got_type,
            }) => {
                assert_eq!(got_aggregate, aggregate, "{err}");
                assert_eq!(got_type, result_type, "{err}");
            }
            other => panic!("expected SqlError::IntegerSumOverflow, got {other:?} from {err}"),
        }
        let text = err.to_string();
        assert!(
            text.contains(&format!(
                "integer sum overflow in {aggregate}: the sum overflowed its result type \
                 {result_type}"
            )),
            "{text}"
        );
    }

    const UNGROUPED: &str = "SELECT sum(v) FROM t";
    const GROUPED: &str = "SELECT sum(v) FROM t GROUP BY g ORDER BY g";
    const SUM_V: &str = "sum(t.v)";

    fn i64s(values: &[i64]) -> Vec<Option<i128>> {
        values.iter().map(|&v| Some(i128::from(v))).collect()
    }

    /// Two groups: group 1 holds `values`, group 2 holds a single 7, so the
    /// grouped accumulator carries a group that does not overflow beside one
    /// that does.
    fn grouped(values: &[Option<i128>]) -> Vec<Row> {
        let mut rows: Vec<Row> = values
            .iter()
            .enumerate()
            .map(|(o, &v)| (o as i64, 1, v))
            .collect();
        rows.push((values.len() as i64, 2, Some(7)));
        rows
    }

    #[tokio::test]
    async fn signed_max_plus_one_fails_ungrouped_and_grouped_at_1_and_8_partitions() {
        let values = i64s(&[i64::MAX, 1]);
        for partitions in [1, 8] {
            let result = query(UNGROUPED, &DataType::Int64, &ungrouped(&values), partitions).await;
            assert_overflow(result, SUM_V, "Int64");
            let result = query(GROUPED, &DataType::Int64, &grouped(&values), partitions).await;
            assert_overflow(result, SUM_V, "Int64");
        }
    }

    #[tokio::test]
    async fn unsigned_max_plus_one_fails_ungrouped_and_grouped_at_1_and_8_partitions() {
        let values = vec![Some(U64_MAX), Some(1)];
        for partitions in [1, 8] {
            let result = query(UNGROUPED, &DataType::UInt64, &ungrouped(&values), partitions).await;
            assert_overflow(result, SUM_V, "UInt64");
            let result = query(GROUPED, &DataType::UInt64, &grouped(&values), partitions).await;
            assert_overflow(result, SUM_V, "UInt64");
        }
    }

    #[tokio::test]
    async fn signed_min_minus_one_fails_ungrouped_and_grouped_at_1_and_8_partitions() {
        let values = i64s(&[i64::MIN, -1]);
        for partitions in [1, 8] {
            let result = query(UNGROUPED, &DataType::Int64, &ungrouped(&values), partitions).await;
            assert_overflow(result, SUM_V, "Int64");
            let result = query(GROUPED, &DataType::Int64, &grouped(&values), partitions).await;
            assert_overflow(result, SUM_V, "Int64");
        }
    }

    /// The 8-partition fixture runs a partial aggregation per partition and a
    /// final one that merges their states, so the overflow tests above cover
    /// `merge_batch`; at 1 partition the aggregate runs in one phase.
    #[tokio::test]
    async fn eight_partitions_merge_partial_states_and_one_does_not() {
        let rows = grouped(&i64s(&[i64::MAX, 1]));
        for sql in [UNGROUPED, GROUPED] {
            let eight = plan(sql, &DataType::Int64, &rows, 8).await;
            assert!(eight.contains("mode=Partial"), "{eight}");
            let one = plan(sql, &DataType::Int64, &rows, 1).await;
            assert!(!one.contains("mode=Partial"), "{one}");
        }
    }

    async fn assert_sum(sql: &str, value_type: &DataType, rows: &[Row], expected: &[ScalarValue]) {
        for partitions in [1, 8] {
            let got = query(sql, value_type, rows, partitions)
                .await
                .unwrap_or_else(|err| panic!("{sql} at {partitions} partitions: {err}"));
            assert_eq!(got, expected, "{sql} at {partitions} partitions");
        }
    }

    fn int64(value: Option<i64>) -> ScalarValue {
        ScalarValue::Int64(value)
    }

    #[tokio::test]
    async fn sums_that_fit_are_unchanged() {
        // The maximum alone.
        let rows = ungrouped(&i64s(&[i64::MAX]));
        assert_sum(UNGROUPED, &DataType::Int64, &rows, &[int64(Some(i64::MAX))]).await;
        // Orders whose every running total stays in range.
        for values in [
            [i64::MAX, -1, 1],
            [-1, i64::MAX, 1],
            [-1, 1, i64::MAX],
        ] {
            let rows = ungrouped(&i64s(&values));
            assert_sum(UNGROUPED, &DataType::Int64, &rows, &[int64(Some(i64::MAX))]).await;
            let rows = grouped(&i64s(&values));
            let expected = [int64(Some(i64::MAX)), int64(Some(7))];
            assert_sum(GROUPED, &DataType::Int64, &rows, &expected).await;
        }
        let rows = ungrouped(&[Some(U64_MAX)]);
        assert_sum(
            UNGROUPED,
            &DataType::UInt64,
            &rows,
            &[ScalarValue::UInt64(Some(u64::MAX))],
        )
        .await;
        let rows = ungrouped(&i64s(&[i64::MIN]));
        assert_sum(UNGROUPED, &DataType::Int64, &rows, &[int64(Some(i64::MIN))]).await;
    }

    /// `i64::MAX, 1, -1` in row order: the running total after the second row
    /// is `i64::MAX + 1`, outside `Int64`, but the total is `i64::MAX`. The sum
    /// is exact in `i128` and only the total is range-checked, so the query
    /// returns `i64::MAX` at every partition count, whichever rows meet in
    /// which partial.
    #[tokio::test]
    async fn an_intermediate_total_outside_the_range_with_a_total_inside_it_returns_the_total() {
        for values in [[i64::MAX, 1, -1], [1, i64::MAX, -1], [i64::MIN, -1, 1]] {
            let expected = values.iter().map(|&v| i128::from(v)).sum::<i128>();
            let expected = i64::try_from(expected).expect("total fits");
            let rows = ungrouped(&i64s(&values));
            assert_sum(UNGROUPED, &DataType::Int64, &rows, &[int64(Some(expected))]).await;
            let rows = grouped(&i64s(&values));
            let expected = [int64(Some(expected)), int64(Some(7))];
            assert_sum(GROUPED, &DataType::Int64, &rows, &expected).await;
        }
    }

    #[tokio::test]
    async fn nulls_are_skipped_and_an_all_null_or_empty_group_is_null() {
        let rows = ungrouped(&[None, Some(I64_MAX), None]);
        assert_sum(UNGROUPED, &DataType::Int64, &rows, &[int64(Some(i64::MAX))]).await;
        let rows = ungrouped(&[None, None]);
        assert_sum(UNGROUPED, &DataType::Int64, &rows, &[int64(None)]).await;
        assert_sum(UNGROUPED, &DataType::Int64, &[], &[int64(None)]).await;
        assert_sum(
            UNGROUPED,
            &DataType::UInt64,
            &[],
            &[ScalarValue::UInt64(None)],
        )
        .await;
        // Group 1 is all NULL, group 2 holds 7.
        let rows = grouped(&[None, None]);
        assert_sum(GROUPED, &DataType::Int64, &rows, &[int64(None), int64(Some(7))]).await;
        // A NULL beside an overflow is still an overflow.
        let rows = grouped(&[Some(I64_MAX), None, Some(1)]);
        assert_overflow(query(GROUPED, &DataType::Int64, &rows, 8).await, SUM_V, "Int64");
    }

    #[tokio::test]
    async fn distinct_sum_is_checked() {
        let sql = "SELECT sum(DISTINCT v) FROM t";
        let grouped_sql = "SELECT sum(DISTINCT v) FROM t GROUP BY g ORDER BY g";
        // Duplicates of the maximum count once.
        let values = i64s(&[i64::MAX, i64::MAX, 0]);
        assert_sum(sql, &DataType::Int64, &ungrouped(&values), &[int64(Some(i64::MAX))]).await;
        assert_sum(
            grouped_sql,
            &DataType::Int64,
            &grouped(&values),
            &[int64(Some(i64::MAX)), int64(Some(7))],
        )
        .await;
        // A lone DISTINCT aggregate is rewritten by DataFusion's
        // `SingleDistinctToGroupBy` into a plain sum over a GROUP BY of the
        // value, aliased `alias1`, so the checked plain sum raises the error
        // under that name.
        let values = i64s(&[i64::MAX, i64::MAX, 1]);
        for partitions in [1, 8] {
            let result = query(sql, &DataType::Int64, &ungrouped(&values), partitions).await;
            assert_overflow(result, "sum(alias1)", "Int64");
            let result = query(grouped_sql, &DataType::Int64, &grouped(&values), partitions).await;
            assert_overflow(result, "sum(alias1)", "Int64");
        }
    }

    /// Two DISTINCT aggregates keep DataFusion from rewriting the distinct sum
    /// into a GROUP BY, so the distinct accumulator itself runs.
    #[tokio::test]
    async fn distinct_sum_beside_another_distinct_aggregate_is_checked() {
        let sql = "SELECT sum(DISTINCT v), count(DISTINCT o) FROM t";
        let values = [Some(U64_MAX), Some(U64_MAX), Some(1)];
        for partitions in [1, 8] {
            let result = query(sql, &DataType::UInt64, &ungrouped(&values), partitions).await;
            assert_overflow(result, "sum(DISTINCT t.v)", "UInt64");
        }
        let values = [Some(U64_MAX), Some(U64_MAX), Some(0), None];
        assert_sum(
            sql,
            &DataType::UInt64,
            &ungrouped(&values),
            &[ScalarValue::UInt64(Some(u64::MAX))],
        )
        .await;
    }

    #[tokio::test]
    async fn filter_is_applied_before_the_check() {
        let values = i64s(&[i64::MAX, 1, -5]);
        let kept = "SELECT sum(v) FILTER (WHERE v <> 1) FROM t";
        let kept_grouped = "SELECT sum(v) FILTER (WHERE v <> 1) FROM t GROUP BY g ORDER BY g";
        assert_sum(kept, &DataType::Int64, &ungrouped(&values), &[int64(Some(i64::MAX - 5))])
            .await;
        assert_sum(
            kept_grouped,
            &DataType::Int64,
            &grouped(&values),
            &[int64(Some(i64::MAX - 5)), int64(Some(7))],
        )
        .await;
        // A filter that selects no row of a group leaves it NULL.
        let none = "SELECT sum(v) FILTER (WHERE v > 100) FROM t GROUP BY g ORDER BY g";
        let rows = grouped(&i64s(&[1, 2]));
        assert_sum(none, &DataType::Int64, &rows, &[int64(None), int64(None)]).await;
        let overflow = "SELECT sum(v) FILTER (WHERE v > 0) FROM t";
        let overflow_grouped = "SELECT sum(v) FILTER (WHERE v > 0) FROM t GROUP BY g ORDER BY g";
        let aggregate = "sum(t.v) FILTER (WHERE t.v > Int64(0))";
        for partitions in [1, 8] {
            let result = query(overflow, &DataType::Int64, &ungrouped(&values), partitions).await;
            assert_overflow(result, aggregate, "Int64");
            let result =
                query(overflow_grouped, &DataType::Int64, &grouped(&values), partitions).await;
            assert_overflow(result, aggregate, "Int64");
        }
    }

    #[tokio::test]
    async fn window_sums_are_checked() {
        let values = i64s(&[1, i64::MAX, -1, 1]);
        let rows = ungrouped(&values);
        // UNBOUNDED PRECEDING: the running total after row 1 overflows.
        let running = "SELECT sum(v) OVER (ORDER BY o) FROM t ORDER BY o";
        // A moving frame: rows 0-1 total i64::MAX + 1.
        let moving =
            "SELECT sum(v) OVER (ORDER BY o ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t \
             ORDER BY o";
        let moving_distinct = "SELECT sum(DISTINCT v) OVER (ORDER BY o ROWS BETWEEN 1 PRECEDING \
                               AND CURRENT ROW) FROM t ORDER BY o";
        for sql in [running, moving, moving_distinct] {
            let err = query(sql, &DataType::Int64, &rows, 1)
                .await
                .expect_err("overflowing frame must fail");
            assert!(
                matches!(sql_error(&err), Some(SqlError::IntegerSumOverflow { .. })),
                "{sql}: {err}"
            );
        }
        // Frames that fit, including one reached by retracting a value that
        // put an earlier frame at the limit.
        let rows = ungrouped(&i64s(&[i64::MAX, -1, 1, 1]));
        assert_sum(
            moving,
            &DataType::Int64,
            &rows,
            &[
                int64(Some(i64::MAX)),
                int64(Some(i64::MAX - 1)),
                int64(Some(0)),
                int64(Some(2)),
            ],
        )
        .await;
        assert_sum(
            moving_distinct,
            &DataType::Int64,
            &rows,
            &[
                int64(Some(i64::MAX)),
                int64(Some(i64::MAX - 1)),
                int64(Some(0)),
                int64(Some(1)),
            ],
        )
        .await;
        let rows = ungrouped(&[Some(U64_MAX), Some(1)]);
        let err = query(moving, &DataType::UInt64, &rows, 1)
            .await
            .expect_err("overflowing unsigned frame must fail");
        assert!(
            matches!(sql_error(&err), Some(SqlError::IntegerSumOverflow { .. })),
            "{err}"
        );
    }

    #[tokio::test]
    async fn float_sum_delegates_to_the_built_in() {
        let rows = ungrouped(&[Some(1), Some(2), None]);
        for sql in [UNGROUPED, GROUPED] {
            assert_sum(sql, &DataType::Float64, &rows, &[ScalarValue::Float64(Some(3.0))]).await;
        }
        let float_state = signed_state_fields(DataType::Float64);
        assert_eq!(float_state, vec![DataType::Float64]);
    }

    /// The state field types `sum` declares for an argument of `arg_type`.
    fn signed_state_fields(arg_type: DataType) -> Vec<DataType> {
        let udaf = checked_sum_udaf();
        let return_type = udaf.return_type(&[arg_type.clone()]).expect("type");
        let input = [Arc::new(Field::new("v", arg_type, true))];
        let fields = udaf
            .state_fields(StateFieldsArgs {
                name: "sum(t.v)",
                input_fields: &input,
                return_field: Arc::new(Field::new("sum(t.v)", return_type, true)),
                ordering_fields: &[],
                is_distinct: false,
            })
            .expect("state fields");
        fields.iter().map(|f| f.data_type().clone()).collect()
    }

    /// Every integer width coerces to one of the two owned result types, so
    /// every integer sum is checked.
    #[tokio::test]
    async fn every_integer_width_sums_into_an_owned_result_type() {
        let rows = ungrouped(&i64s(&[1, 2]));
        for (cast, result_type) in [
            ("TINYINT", "Int64"),
            ("SMALLINT", "Int64"),
            ("INT", "Int64"),
            ("BIGINT", "Int64"),
            ("TINYINT UNSIGNED", "UInt64"),
            ("SMALLINT UNSIGNED", "UInt64"),
            ("INT UNSIGNED", "UInt64"),
            ("BIGINT UNSIGNED", "UInt64"),
        ] {
            let sql = format!("SELECT arrow_typeof(sum(CAST(v AS {cast}))) FROM t");
            let got = query(&sql, &DataType::Int64, &rows, 1).await.expect("query");
            assert_eq!(got, vec![ScalarValue::Utf8(Some(result_type.to_string()))], "{cast}");
        }
        assert_eq!(signed_state_fields(DataType::Int64), vec![DataType::Decimal128(38, 0)]);
        assert_eq!(signed_state_fields(DataType::UInt64), vec![DataType::Decimal128(38, 0)]);
    }

    fn signed_groups() -> CheckedSumGroupsAccumulator {
        CheckedSumGroupsAccumulator::new(SumTarget::new(IntKind::Signed, SUM_V))
    }

    fn evaluated(acc: &mut CheckedSumGroupsAccumulator, emit_to: EmitTo) -> Vec<Option<i64>> {
        let out = acc.evaluate(emit_to).expect("evaluate");
        out.as_primitive::<Int64Type>().iter().collect()
    }

    /// `convert_to_state` (the skip-partial-aggregation path) produces states
    /// `merge_batch` folds to the same sums, NULL and filtered rows included,
    /// and an overflowing merge fails.
    #[test]
    fn converted_states_merge_to_the_same_sums() {
        let acc = signed_groups();
        let values: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(i64::MAX),
            None,
            Some(-3),
            Some(5),
        ]));
        let filter = BooleanArray::from(vec![Some(true), Some(true), None, Some(true)]);
        let state = acc
            .convert_to_state(std::slice::from_ref(&values), Some(&filter))
            .expect("state");
        let mut merged = signed_groups();
        merged
            .merge_batch(&state, &[0, 1, 2, 3], 4)
            .expect("merge");
        assert_eq!(
            evaluated(&mut merged, EmitTo::All),
            vec![Some(i64::MAX), None, None, Some(5)]
        );
        let mut overflow = signed_groups();
        overflow
            .merge_batch(&state, &[0, 0, 0, 0], 1)
            .expect("i128 merge does not overflow");
        let err = overflow.evaluate(EmitTo::All).expect_err("total overflows");
        assert!(
            matches!(sql_error(&err), Some(SqlError::IntegerSumOverflow { .. })),
            "{err}"
        );
    }

    /// `EmitTo::First` hands out the first groups and keeps the rest, sums and
    /// presence bits aligned, on both the untracked and the tracked path.
    #[test]
    fn emit_first_keeps_the_remaining_groups_aligned() {
        let mut acc = signed_groups();
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        acc.update_batch(std::slice::from_ref(&values), &[0, 1, 2], None, 3)
            .expect("update");
        assert_eq!(evaluated(&mut acc, EmitTo::First(1)), vec![Some(1)]);
        let nulls: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(10)]));
        acc.update_batch(std::slice::from_ref(&nulls), &[2, 3], None, 4)
            .expect("update with nulls");
        assert_eq!(evaluated(&mut acc, EmitTo::First(2)), vec![Some(2), Some(3)]);
        // Group 2 saw only a NULL.
        assert_eq!(evaluated(&mut acc, EmitTo::All), vec![None, Some(10)]);
        let empty: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>]));
        acc.update_batch(std::slice::from_ref(&empty), &[0], None, 1)
            .expect("update all-null");
        assert_eq!(evaluated(&mut acc, EmitTo::All), vec![None]);
    }

    /// A partial state beyond `Decimal128(38, 0)` cannot be produced from
    /// fewer than about 10^19 rows; packing one fails with the overflow error
    /// rather than writing a value the decimal type cannot hold.
    #[test]
    fn a_partial_beyond_decimal128_fails_to_pack() {
        let mut acc = signed_groups();
        acc.sums = vec![DECIMAL128_MAX_UNSCALED + 1];
        let err = acc.state(EmitTo::All).expect_err("state out of range");
        assert!(
            matches!(sql_error(&err), Some(SqlError::IntegerSumOverflow { .. })),
            "{err}"
        );
        let mut plain = CheckedSumAccumulator::new(SumTarget::new(IntKind::Signed, SUM_V));
        plain.sum = Some(-DECIMAL128_MAX_UNSCALED - 1);
        assert!(plain.state().is_err());
    }

    #[test]
    fn an_i128_overflow_is_an_error_not_a_wrap() {
        let mut acc = signed_groups();
        acc.sums = vec![i128::MAX];
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let err = acc
            .update_batch(std::slice::from_ref(&values), &[0], None, 1)
            .expect_err("i128 overflow");
        assert!(
            matches!(sql_error(&err), Some(SqlError::IntegerSumOverflow { .. })),
            "{err}"
        );
        let mut plain = CheckedSumAccumulator::new(SumTarget::new(IntKind::Signed, SUM_V));
        plain.sum = Some(i128::MIN);
        assert!(plain.update_batch(&[Arc::new(Int64Array::from(vec![-1])) as ArrayRef]).is_err());
    }
}
