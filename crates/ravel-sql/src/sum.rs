//! Checked integer SUM (issue #3838, ADR-0022's integer sum amendment).
//!
//! DataFusion 55.2.0's built-in `sum` adds integers with wrapping arithmetic
//! (`add_wrapping` in its `PrimitiveGroupsAccumulator`, `SumAccumulator` and
//! `SlidingSumAccumulator`), so a total past `i64::MAX` or `u64::MAX` comes
//! back as a wrong value with no error. This module replaces the built-in
//! with a UDAF registered under the same name. For a call whose result type
//! is `Int64` (signed integer input) or `UInt64` (unsigned integer input) it
//! accumulates exactly in `i128` and converts the total to the result type
//! once, at evaluation; a total that does not fit fails the query with
//! [`SqlError::IntegerSumOverflow`]. A wrapped value is never returned. Every
//! other result type (float, decimal, duration, interval) delegates to the
//! built-in unchanged; ADR-0024 owns float `sum`.
//!
//! Partial results are never range-checked against the result type, only the
//! final total. Integer addition in `i128` is associative and exact, so the
//! result, or the error, is a function of the input multiset alone: the same
//! rows give the same answer at any partition count, in any arrival or merge
//! order. A check on each partial would make `i64::MAX, 1, -1` fail when `1`
//! and `i64::MAX` land in one partition and succeed when `-1` does, which
//! would break the partition independence ADR-0094 relies on when it lets a
//! `sum` over non-float input repartition its final aggregation.
//!
//! The partial state is one nullable `Decimal128(38, 0)` holding the exact
//! partial sum (null when the partition or group saw no non-null row). A
//! partial sum outside `Decimal128(38, 0)`, or an `i128` overflow while
//! merging, also fails with [`SqlError::IntegerSumOverflow`]; neither is
//! reachable without about `10^19` input rows, and the total would not fit a
//! 64-bit result type either.
//!
//! Grouped execution runs [`CheckedSumGroupsAccumulator`], a flat per-group
//! accumulator in the shape of DataFusion's own primitive one: one `i64` or
//! `u64` and one validity bit per group, updated from the whole input array
//! and the batch's group indices in one loop with a checked add per value,
//! with no per-group `Accumulator` object and no `ScalarValue` per row. A
//! batch with no nulls and no filter takes a loop over the raw value slice.
//! An add that would leave the 64-bit type promotes its group: the group's
//! running sum moves into an `i128` carry held in a side table
//! ([`NarrowSums`]) and the narrow sum restarts at the value, so the exact
//! total is carry plus narrow sum, the same `i128` total as above.
//!
//! `sum(DISTINCT x)` sums the set of distinct values exactly and applies the
//! same check. A window `sum` runs [`CheckedSumAccumulator`] for an
//! unbounded-preceding frame and the same accumulator with retraction for a
//! moving frame, so every frame's total is checked the same way.
//!
//! The built-in's `SUM(x + c)` to `SUM(x) + c * COUNT(x)` rewrite is not
//! applied for an integer literal: the outer `+` and `*` are DataFusion
//! scalar arithmetic, which wraps, so the rewritten form could return a
//! wrapped total where the checked `sum` would have failed. `crate::sum_plus`
//! rewrites that shape instead, into an exact sum and count of `x` and a
//! checked `sum + c * count`.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Decimal128Array, ListArray, PrimitiveArray,
};
use datafusion::arrow::array::builder::BooleanBufferBuilder;
use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{
    ArrowNativeType, DataType, Field, FieldRef, Int64Type, UInt64Type,
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
/// built-in.
pub fn checked_sum_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(CheckedSum::new())
}

/// Suffix of the partial-state field of a non-distinct integer call. Read by
/// `crate::memory`'s aggregate layout to tell this accumulator from the
/// built-in's.
pub(crate) const EXACT_SUM_STATE_SUFFIX: &str = "[sum_exact]";

/// Per-group `Vec` bytes of [`CheckedSumGroupsAccumulator`]: an `i64` or
/// `u64` sum. Its validity bitmap (one bit per group) and the carry table of
/// the groups whose sum left 64 bits are not `Vec`s of group width, as the
/// built-in's bitmap is not.
pub(crate) const EXACT_SUM_GROUP_WIDTHS: [usize; 1] = [8];

#[derive(Debug)]
struct CheckedSum {
    /// The wrapped built-in, used for every non-integer result type and for
    /// the planning methods this UDAF does not change.
    inner: Arc<dyn AggregateUDFImpl>,
}

impl CheckedSum {
    fn new() -> Self {
        Self {
            inner: sum_udaf().inner().clone(),
        }
    }
}

// `AggregateUDFImpl` requires `DynEq`/`DynHash`; every instance is the same
// function.
impl PartialEq for CheckedSum {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for CheckedSum {}

impl std::hash::Hash for CheckedSum {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

/// Which checked accumulator an integer call gets, from its resolved result
/// type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SumKind {
    /// Signed integer input, coerced to `Int64`; returns `Int64`.
    Signed,
    /// Unsigned integer input, coerced to `UInt64`; returns `UInt64`.
    Unsigned,
}

/// The checked kind of a call returning `return_type`; `None` for every other
/// result type, which the built-in runs unchanged.
pub(crate) fn sum_kind(return_type: &DataType) -> Option<SumKind> {
    match return_type {
        DataType::Int64 => Some(SumKind::Signed),
        DataType::UInt64 => Some(SumKind::Unsigned),
        _ => None,
    }
}

impl SumKind {
    pub(crate) fn result_type(self) -> DataType {
        match self {
            SumKind::Unsigned => DataType::UInt64,
            SumKind::Signed => DataType::Int64,
        }
    }

    fn result_type_name(self) -> &'static str {
        match self {
            SumKind::Unsigned => "UInt64",
            SumKind::Signed => "Int64",
        }
    }

    /// The overflow error for this kind, as the `DataFusionError` an
    /// accumulator returns. The executor recovers the `SqlError` from it.
    pub(crate) fn overflow(self) -> DataFusionError {
        DataFusionError::External(Box::new(SqlError::IntegerSumOverflow {
            result_type: self.result_type_name(),
        }))
    }

    /// `total` as a result-type scalar, or the overflow error when it does
    /// not fit. `None` is the NULL of an empty or all-NULL input.
    fn output(self, total: Option<i128>) -> DFResult<ScalarValue> {
        Ok(match self {
            SumKind::Unsigned => ScalarValue::UInt64(
                total
                    .map(|total| u64::try_from(total).map_err(|_| self.overflow()))
                    .transpose()?,
            ),
            SumKind::Signed => ScalarValue::Int64(
                total
                    .map(|total| i64::try_from(total).map_err(|_| self.overflow()))
                    .transpose()?,
            ),
        })
    }

    pub(crate) fn checked_add(self, acc: i128, value: i128) -> DFResult<i128> {
        acc.checked_add(value).ok_or_else(|| self.overflow())
    }

    /// `sum` as a `Decimal128(38, 0)` state value, or the overflow error when
    /// it is outside that type's range.
    pub(crate) fn state_value(self, sum: i128) -> DFResult<i128> {
        if (-DECIMAL128_MAX_UNSCALED..=DECIMAL128_MAX_UNSCALED).contains(&sum) {
            Ok(sum)
        } else {
            Err(self.overflow())
        }
    }

    /// Visit every non-null value of `array` (the call's coerced input) as an
    /// `i128`, with its row index.
    pub(crate) fn for_each_value(
        self,
        array: &ArrayRef,
        mut visit: impl FnMut(usize, i128) -> DFResult<()>,
    ) -> DFResult<()> {
        match self {
            SumKind::Unsigned => {
                let values = as_primitive::<UInt64Type>(array)?;
                for (row, value) in values.iter().enumerate() {
                    if let Some(value) = value {
                        visit(row, i128::from(value))?;
                    }
                }
            }
            SumKind::Signed => {
                let values = as_primitive::<Int64Type>(array)?;
                for (row, value) in values.iter().enumerate() {
                    if let Some(value) = value {
                        visit(row, i128::from(value))?;
                    }
                }
            }
        }
        Ok(())
    }

    fn scalar(self, value: i128) -> ScalarValue {
        match self {
            // Only values read from this kind's own input reach here, so the
            // conversions cannot fail; a failure would be a typed NULL, not a
            // wrapped value.
            SumKind::Unsigned => ScalarValue::UInt64(u64::try_from(value).ok()),
            SumKind::Signed => ScalarValue::Int64(i64::try_from(value).ok()),
        }
    }
}

/// The largest unscaled `Decimal128(38, 0)` value, 38 nines.
const DECIMAL128_MAX_UNSCALED: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

/// `array` as a `PrimitiveArray<T>`, cast first when its type differs.
pub(crate) fn as_primitive<T: ArrowPrimitiveType>(array: &ArrayRef) -> DFResult<PrimitiveArray<T>> {
    let array = if array.data_type() == &T::DATA_TYPE {
        Arc::clone(array)
    } else {
        cast(array, &T::DATA_TYPE)?
    };
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "checked sum: input is not a {} array",
                T::DATA_TYPE
            ))
        })
}

pub(crate) fn as_decimal128(array: &ArrayRef) -> DFResult<&Decimal128Array> {
    array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "checked sum: merge state is not a Decimal128Array".to_string(),
            )
        })
}

/// Whether `opt_filter[row]` excludes `row`. A null filter slot excludes the
/// row, as DataFusion does.
pub(crate) fn row_is_filtered(opt_filter: Option<&BooleanArray>, row: usize) -> bool {
    match opt_filter {
        None => false,
        Some(filter) => filter.is_null(row) || !filter.value(row),
    }
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
        let Some(kind) = sum_kind(args.return_type()) else {
            return self.inner.state_fields(args);
        };
        if args.is_distinct {
            // Matches [`DistinctSumAccumulator`]'s `state`/`merge_batch`.
            Ok(vec![Arc::new(Field::new_list(
                format!("{}[sum_distinct]", args.name),
                Field::new_list_field(kind.result_type(), true),
                false,
            ))])
        } else {
            // Matches [`CheckedSumAccumulator`]/[`CheckedSumGroupsAccumulator`]'s
            // `state`/`merge_batch`.
            Ok(vec![Arc::new(Field::new(
                format!("{}{EXACT_SUM_STATE_SUFFIX}", args.name),
                DataType::Decimal128(38, 0),
                true,
            ))])
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let Some(kind) = sum_kind(acc_args.return_field.data_type()) else {
            return self.inner.accumulator(acc_args);
        };
        if acc_args.is_distinct {
            Ok(Box::new(DistinctSumAccumulator::new(kind)))
        } else {
            Ok(Box::new(CheckedSumAccumulator::new(kind)))
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        match sum_kind(args.return_field.data_type()) {
            None => self.inner.groups_accumulator_supported(args),
            // DataFusion runs one `DistinctSumAccumulator` per group behind
            // its adapter, as it does for the built-in's distinct call.
            Some(_) => !args.is_distinct,
        }
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        let Some(kind) = sum_kind(args.return_field.data_type()) else {
            return self.inner.create_groups_accumulator(args);
        };
        if args.is_distinct {
            Err(DataFusionError::Internal(
                "checked sum: no GroupsAccumulator exists for a DISTINCT integer call; \
                 groups_accumulator_supported returns false for it"
                    .to_string(),
            ))
        } else {
            Ok(match kind {
                SumKind::Signed => Box::new(CheckedSumGroupsAccumulator::<Int64Type>::new(kind)),
                SumKind::Unsigned => {
                    Box::new(CheckedSumGroupsAccumulator::<UInt64Type>::new(kind))
                }
            })
        }
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let Some(kind) = sum_kind(args.return_field.data_type()) else {
            return self.inner.create_sliding_accumulator(args);
        };
        if args.is_distinct {
            Ok(Box::new(DistinctSumAccumulator::new(kind)))
        } else {
            Ok(Box::new(CheckedSumAccumulator::new(kind)))
        }
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_expr()
    }

    fn simplify_expr_op_literal(
        &self,
        agg_function: &AggregateFunction,
        arg: &Expr,
        op: Operator,
        lit: &Expr,
        arg_is_left: bool,
    ) -> DFResult<Option<Expr>> {
        if let Expr::Literal(value, _) = lit
            && value.data_type().is_integer()
        {
            return Ok(None);
        }
        self.inner
            .simplify_expr_op_literal(agg_function, arg, op, lit, arg_is_left)
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        // A statistics sum was not computed by this accumulator and carries
        // no overflow check, so an integer call always reads its rows.
        match sum_kind(statistics_args.return_type) {
            None => self.inner.value_from_stats(statistics_args),
            Some(_) => None,
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.set_monotonicity(data_type)
    }
}

/// The plain and sliding accumulator for an integer call: the exact `i128`
/// sum of the values seen, and how many contributed. A merged partial state
/// counts as one contribution; only emptiness is read from `count`.
#[derive(Debug)]
struct CheckedSumAccumulator {
    kind: SumKind,
    sum: i128,
    count: u64,
}

impl CheckedSumAccumulator {
    fn new(kind: SumKind) -> Self {
        Self {
            kind,
            sum: 0,
            count: 0,
        }
    }

    fn total(&self) -> Option<i128> {
        (self.count > 0).then_some(self.sum)
    }
}

impl Accumulator for CheckedSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let kind = self.kind;
        let (mut sum, mut count) = (self.sum, self.count);
        kind.for_each_value(&values[0], |_, value| {
            sum = kind.checked_add(sum, value)?;
            count += 1;
            Ok(())
        })?;
        (self.sum, self.count) = (sum, count);
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let kind = self.kind;
        let (mut sum, mut count) = (self.sum, self.count);
        kind.for_each_value(&values[0], |_, value| {
            sum = sum.checked_sub(value).ok_or_else(|| kind.overflow())?;
            count = count.saturating_sub(1);
            Ok(())
        })?;
        (self.sum, self.count) = (sum, count);
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let sums = as_decimal128(&states[0])?;
        for partial in sums.iter().flatten() {
            self.sum = self.kind.checked_add(self.sum, partial)?;
            self.count += 1;
        }
        Ok(())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let sum = self
            .total()
            .map(|sum| self.kind.state_value(sum))
            .transpose()?;
        Ok(vec![ScalarValue::Decimal128(sum, 38, 0)])
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        self.kind.output(self.total())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// The native type of a checked kind's narrow per-group sum: `i64` for
/// [`SumKind::Signed`], `u64` for [`SumKind::Unsigned`].
pub(crate) trait NarrowSum: ArrowNativeType + Into<i128> + TryFrom<i128> {
    fn add_checked(self, value: Self) -> Option<Self>;
}

impl NarrowSum for i64 {
    #[inline]
    fn add_checked(self, value: Self) -> Option<Self> {
        self.checked_add(value)
    }
}

impl NarrowSum for u64 {
    #[inline]
    fn add_checked(self, value: Self) -> Option<Self> {
        self.checked_add(value)
    }
}

/// Exact per-group sums held in the input's own 64-bit type. A group's exact
/// sum is `narrow[g]` plus its entry in `carries`, which only a group whose
/// running sum left the narrow type has: the add that would overflow moves
/// the narrow sum into the `i128` carry and restarts the narrow sum at the
/// value. Every value is counted exactly once, so the total is the same
/// `i128` sum as a wide accumulator's, in any arrival order.
#[derive(Debug)]
pub(crate) struct NarrowSums<N> {
    kind: SumKind,
    narrow: Vec<N>,
    carries: HashMap<usize, i128>,
}

impl<N: NarrowSum> NarrowSums<N> {
    pub(crate) fn new(kind: SumKind) -> Self {
        Self {
            kind,
            narrow: Vec::new(),
            carries: HashMap::new(),
        }
    }

    pub(crate) fn kind(&self) -> SumKind {
        self.kind
    }

    pub(crate) fn resize(&mut self, total_num_groups: usize) {
        self.narrow.resize(total_num_groups, N::default());
    }

    #[inline]
    pub(crate) fn add(&mut self, group: usize, value: N) -> DFResult<()> {
        let sum = &mut self.narrow[group];
        match sum.add_checked(value) {
            Some(total) => {
                *sum = total;
                Ok(())
            }
            None => {
                let spilled = std::mem::replace(sum, value);
                self.carry(group, spilled.into())
            }
        }
    }

    /// Add `value`, a partial sum that may be outside the narrow type.
    pub(crate) fn add_wide(&mut self, group: usize, value: i128) -> DFResult<()> {
        match N::try_from(value) {
            Ok(value) => self.add(group, value),
            Err(_) => self.carry(group, value),
        }
    }

    fn carry(&mut self, group: usize, value: i128) -> DFResult<()> {
        let carry = self.carries.entry(group).or_insert(0);
        *carry = self.kind.checked_add(*carry, value)?;
        Ok(())
    }

    /// Remove the carries of the groups `emit_to` names, renumbering the
    /// rest as `EmitTo::take_needed` renumbers the narrow sums.
    fn take_carries(&mut self, emit_to: EmitTo) -> Vec<(usize, i128)> {
        if self.carries.is_empty() {
            return Vec::new();
        }
        let carries = std::mem::take(&mut self.carries);
        match emit_to {
            EmitTo::All => carries.into_iter().collect(),
            EmitTo::First(n) => {
                let mut taken = Vec::new();
                for (group, carry) in carries {
                    if group < n {
                        taken.push((group, carry));
                    } else {
                        self.carries.insert(group - n, carry);
                    }
                }
                taken
            }
        }
    }

    /// Take the groups `emit_to` names as result-type totals, or the overflow
    /// error when one does not fit.
    pub(crate) fn take_narrow(&mut self, emit_to: EmitTo) -> DFResult<Vec<N>> {
        let kind = self.kind;
        let mut sums = emit_to.take_needed(&mut self.narrow);
        for (group, carry) in self.take_carries(emit_to) {
            let total = kind.checked_add(carry, sums[group].into())?;
            sums[group] = N::try_from(total).map_err(|_| kind.overflow())?;
        }
        Ok(sums)
    }

    /// Take the groups `emit_to` names as exact `i128` totals.
    pub(crate) fn take_wide(&mut self, emit_to: EmitTo) -> DFResult<Vec<i128>> {
        let kind = self.kind;
        let mut sums: Vec<i128> = emit_to
            .take_needed(&mut self.narrow)
            .into_iter()
            .map(Into::into)
            .collect();
        for (group, carry) in self.take_carries(emit_to) {
            sums[group] = kind.checked_add(carry, sums[group])?;
        }
        Ok(sums)
    }

    /// The narrow `Vec` by capacity, and the carry table by capacity with
    /// hashbrown's one control byte per slot.
    pub(crate) fn size(&self) -> usize {
        self.narrow.capacity() * std::mem::size_of::<N>()
            + self.carries.capacity() * (std::mem::size_of::<(usize, i128)>() + 1)
    }

    #[cfg(test)]
    pub(crate) fn carried_groups(&self) -> usize {
        self.carries.len()
    }
}

/// The flat per-group accumulator for an integer call, over `T`, the call's
/// coerced input type (`Int64` or `UInt64`). `sums` holds group `g`'s exact
/// running sum and `seen` whether any non-null value reached it; an unseen
/// group evaluates to NULL.
#[derive(Debug)]
struct CheckedSumGroupsAccumulator<T: ArrowPrimitiveType> {
    sums: NarrowSums<T::Native>,
    seen: BooleanBufferBuilder,
}

impl<T> CheckedSumGroupsAccumulator<T>
where
    T: ArrowPrimitiveType,
    T::Native: NarrowSum,
{
    fn new(kind: SumKind) -> Self {
        Self {
            sums: NarrowSums::new(kind),
            seen: BooleanBufferBuilder::new(0),
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.sums.resize(total_num_groups);
        if total_num_groups > self.seen.len() {
            self.seen.append_n(total_num_groups - self.seen.len(), false);
        }
    }

    /// Take the validity of the groups `emit_to` names.
    fn take_seen(&mut self, emit_to: EmitTo) -> NullBuffer {
        let seen = self.seen.finish();
        match emit_to {
            EmitTo::All => NullBuffer::new(seen),
            EmitTo::First(n) => {
                self.seen.append_buffer(&seen.slice(n, seen.len() - n));
                NullBuffer::new(seen.slice(0, n))
            }
        }
    }
}

impl<T> GroupsAccumulator for CheckedSumGroupsAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: NarrowSum,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups);
        let values = as_primitive::<T>(&values[0])?;
        if values.null_count() == 0 && opt_filter.is_none() {
            for (&group_index, &value) in group_indices.iter().zip(values.values().iter()) {
                self.sums.add(group_index, value)?;
                self.seen.set_bit(group_index, true);
            }
            return Ok(());
        }
        for (row, &group_index) in group_indices.iter().enumerate() {
            if row_is_filtered(opt_filter, row) || values.is_null(row) {
                continue;
            }
            self.sums.add(group_index, values.value(row))?;
            self.seen.set_bit(group_index, true);
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups);
        let partials = as_decimal128(&values[0])?;
        for (row, &group_index) in group_indices.iter().enumerate() {
            if partials.is_valid(row) {
                self.sums.add_wide(group_index, partials.value(row))?;
                self.seen.set_bit(group_index, true);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let sums = self.sums.take_narrow(emit_to)?;
        let nulls = self.take_seen(emit_to);
        Ok(Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::from(sums),
            Some(nulls),
        )))
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let sums = self.sums.take_wide(emit_to)?;
        let nulls = self.take_seen(emit_to);
        for &sum in &sums {
            self.sums.kind.state_value(sum)?;
        }
        let state = Decimal128Array::new(ScalarBuffer::from(sums), Some(nulls))
            .with_precision_and_scale(38, 0)?;
        Ok(vec![Arc::new(state)])
    }

    /// One state row per input row, so a partial aggregation whose groups are
    /// nearly all distinct can pass rows through (`skip_partial_aggregation`).
    /// A filtered or NULL row becomes a null state, which `merge_batch` skips.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> DFResult<Vec<ArrayRef>> {
        let rows = values[0].len();
        let mut sums = vec![0i128; rows];
        let mut valid = vec![false; rows];
        self.sums.kind.for_each_value(&values[0], |row, value| {
            if !row_is_filtered(opt_filter, row) {
                sums[row] = value;
                valid[row] = true;
            }
            Ok(())
        })?;
        let state = Decimal128Array::new(ScalarBuffer::from(sums), Some(NullBuffer::from(valid)))
            .with_precision_and_scale(38, 0)?;
        Ok(vec![Arc::new(state)])
    }

    fn size(&self) -> usize {
        // The `EXACT_SUM_GROUP_WIDTHS` bytes per group the memory layout reads, by
        // capacity, plus the validity bitmap and the carry table, which it does
        // not; like the built-in's, the fixed struct is not counted anywhere.
        self.sums.size() + self.seen.capacity() / 8
    }
}

/// `sum(DISTINCT x)` over integer input, plain and sliding. `values` counts
/// the rows holding each distinct value in the current input or frame, so a
/// moving window can retract; the result is the checked sum of its keys.
/// Partial states merge by union, so the result is a function of the set of
/// values alone.
#[derive(Debug)]
struct DistinctSumAccumulator {
    kind: SumKind,
    values: HashMap<i128, u64>,
}

impl DistinctSumAccumulator {
    fn new(kind: SumKind) -> Self {
        Self {
            kind,
            values: HashMap::new(),
        }
    }

    fn insert_values(&mut self, array: &ArrayRef) -> DFResult<()> {
        let values = &mut self.values;
        self.kind.for_each_value(array, |_, value| {
            *values.entry(value).or_insert(0) += 1;
            Ok(())
        })
    }
}

impl Accumulator for DistinctSumAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.insert_values(&values[0])
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let distinct = &mut self.values;
        self.kind.for_each_value(&values[0], |_, value| {
            if let Some(rows) = distinct.get_mut(&value) {
                *rows -= 1;
                if *rows == 0 {
                    distinct.remove(&value);
                }
            }
            Ok(())
        })
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let lists = states[0]
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "checked sum: distinct merge state is not a ListArray".to_string(),
                )
            })?;
        for row in 0..lists.len() {
            if lists.is_valid(row) {
                self.insert_values(&lists.value(row))?;
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let scalars: Vec<ScalarValue> = self
            .values
            .keys()
            .map(|&value| self.kind.scalar(value))
            .collect();
        Ok(vec![ScalarValue::List(ScalarValue::new_list_nullable(
            &scalars,
            &self.kind.result_type(),
        ))])
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        if self.values.is_empty() {
            return self.kind.output(None);
        }
        let kind = self.kind;
        let total = self
            .values
            .keys()
            .try_fold(0i128, |acc, &value| kind.checked_add(acc, value))?;
        kind.output(Some(total))
    }

    fn size(&self) -> usize {
        // One control byte per hash table slot on top of the entry.
        std::mem::size_of_val(self)
            + self.values.capacity()
                * (std::mem::size_of::<i128>() + std::mem::size_of::<u64>() + 1)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int8Array, Int64Array, UInt8Array, UInt64Array};
    use datafusion::arrow::datatypes::{Schema, SchemaRef};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::{SessionConfig, SessionContext};

    const MAX: i64 = i64::MAX;
    const MIN: i64 = i64::MIN;

    /// The test table: `o` (global row order), `k` (group key) and `v`, split
    /// into the given partitions, one batch per non-empty partition.
    struct Table {
        schema: SchemaRef,
        partitions: Vec<Vec<RecordBatch>>,
    }

    fn table<T, A>(
        value_type: DataType,
        parts: &[Vec<(i64, Option<T>)>],
        array: impl Fn(Vec<Option<T>>) -> A,
    ) -> Table
    where
        T: Copy,
        A: Array + 'static,
    {
        let schema = Arc::new(Schema::new(vec![
            Field::new("o", DataType::Int64, false),
            Field::new("k", DataType::Int64, false),
            Field::new("v", value_type, true),
        ]));
        let mut order = 0i64;
        let partitions = parts
            .iter()
            .map(|rows| {
                if rows.is_empty() {
                    return Vec::new();
                }
                let o: Vec<i64> = rows
                    .iter()
                    .map(|_| {
                        order += 1;
                        order
                    })
                    .collect();
                let k: Vec<i64> = rows.iter().map(|(k, _)| *k).collect();
                let v: Vec<Option<T>> = rows.iter().map(|(_, v)| *v).collect();
                vec![
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            Arc::new(Int64Array::from(o)),
                            Arc::new(Int64Array::from(k)),
                            Arc::new(array(v)),
                        ],
                    )
                    .expect("batch"),
                ]
            })
            .collect();
        Table { schema, partitions }
    }

    fn signed(parts: &[Vec<(i64, Option<i64>)>]) -> Table {
        table(DataType::Int64, parts, Int64Array::from)
    }

    fn unsigned(parts: &[Vec<(i64, Option<u64>)>]) -> Table {
        table(DataType::UInt64, parts, UInt64Array::from)
    }

    /// `rows` spread round-robin over `partitions` partitions.
    fn spread<T: Copy>(rows: &[(i64, Option<T>)], partitions: usize) -> Vec<Vec<(i64, Option<T>)>> {
        let mut parts = vec![Vec::new(); partitions];
        for (i, row) in rows.iter().enumerate() {
            parts[i % partitions].push(*row);
        }
        parts
    }

    fn ungrouped<T: Copy>(values: &[T]) -> Vec<(i64, Option<T>)> {
        values.iter().map(|v| (1, Some(*v))).collect()
    }

    /// Plan and run `sql` over `table` with the checked `sum` registered,
    /// at `target_partitions` with repartitioned aggregation on. Returns the
    /// physical plan's text and the result.
    fn run(
        table: Table,
        target_partitions: usize,
        sql: &str,
    ) -> (String, DFResult<Vec<RecordBatch>>) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let ctx = SessionContext::new_with_config(
                SessionConfig::new()
                    .with_target_partitions(target_partitions)
                    .with_repartition_aggregations(true),
            );
            ctx.register_udaf(checked_sum_udaf());
            let mem = MemTable::try_new(table.schema, table.partitions).expect("memtable");
            ctx.register_table("t", Arc::new(mem)).expect("register");
            let plan = ctx
                .sql(sql)
                .await
                .expect("plan")
                .create_physical_plan()
                .await
                .expect("physical plan");
            let text = displayable(plan.as_ref()).indent(true).to_string();
            let result = collect(plan, ctx.task_ctx()).await;
            (text, result)
        })
    }

    /// The first column of `batches` as `Int64` cells.
    fn i64_cells(batches: &[RecordBatch]) -> Vec<Option<i64>> {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Int64 result");
                column.iter().collect::<Vec<_>>()
            })
            .collect()
    }

    fn u64_cells(batches: &[RecordBatch]) -> Vec<Option<u64>> {
        batches
            .iter()
            .flat_map(|batch| {
                let column = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("UInt64 result");
                column.iter().collect::<Vec<_>>()
            })
            .collect()
    }

    fn assert_overflow(result: DFResult<Vec<RecordBatch>>, result_type: &str, case: &str) {
        let err = match result {
            Ok(batches) => panic!("{case}: an overflowing sum returned {batches:?}"),
            Err(err) => err,
        };
        match crate::executor::take_sql_error(err) {
            Ok(SqlError::IntegerSumOverflow { result_type: got }) => {
                assert_eq!(got, result_type, "{case}")
            }
            other => panic!("{case}: expected IntegerSumOverflow, got {other:?}"),
        }
    }

    /// The plan at 8 partitions really merges partial states, and at one
    /// partition really does not, so each overflow test covers the stage it
    /// names.
    fn assert_stages(plan: &str, partitions: usize) {
        if partitions == 1 {
            assert!(plan.contains("mode=Single"), "{plan}");
        } else {
            assert!(plan.contains("mode=Partial"), "{plan}");
            assert!(plan.contains("mode=Final"), "{plan}");
        }
    }

    const UNGROUPED: &str = "SELECT sum(v) FROM t";
    /// Group 1 holds the case's values; group 2 holds a small sum that fits.
    const GROUPED: &str = "SELECT sum(v) FROM t GROUP BY k ORDER BY k";

    fn with_fitting_group<T: Copy>(
        rows: Vec<(i64, Option<T>)>,
        filler: T,
    ) -> Vec<(i64, Option<T>)> {
        let mut rows = rows;
        rows.extend([(2, Some(filler)), (2, Some(filler))]);
        rows
    }

    #[test]
    fn signed_overflow_fails_ungrouped_and_grouped_at_one_and_eight_partitions() {
        for (case, values) in [("i64::MAX + 1", [MAX, 1]), ("i64::MIN + -1", [MIN, -1])] {
            for partitions in [1, 8] {
                let rows = ungrouped(&values);
                let (plan, result) = run(signed(&spread(&rows, partitions)), partitions, UNGROUPED);
                assert_stages(&plan, partitions);
                assert_overflow(
                    result,
                    "Int64",
                    &format!("ungrouped {case} at {partitions}"),
                );

                let rows = with_fitting_group(rows, 3);
                let (plan, result) = run(signed(&spread(&rows, partitions)), partitions, GROUPED);
                assert_stages(&plan, partitions);
                assert_overflow(result, "Int64", &format!("grouped {case} at {partitions}"));
            }
        }
    }

    #[test]
    fn unsigned_overflow_fails_ungrouped_and_grouped_at_one_and_eight_partitions() {
        for partitions in [1, 8] {
            let rows = ungrouped(&[u64::MAX, 1]);
            let (plan, result) = run(unsigned(&spread(&rows, partitions)), partitions, UNGROUPED);
            assert_stages(&plan, partitions);
            assert_overflow(result, "UInt64", &format!("ungrouped at {partitions}"));

            let rows = with_fitting_group(rows, 3);
            let (plan, result) = run(unsigned(&spread(&rows, partitions)), partitions, GROUPED);
            assert_stages(&plan, partitions);
            assert_overflow(result, "UInt64", &format!("grouped at {partitions}"));
        }
    }

    /// Two partitions that each fit their result type and whose merged total
    /// does not: the overflow is only visible once the final stage merges.
    #[test]
    fn overflow_that_only_the_merge_sees_fails() {
        let parts = vec![vec![(1, Some(MAX))], vec![(1, Some(1))]];
        let (plan, result) = run(signed(&parts), 8, UNGROUPED);
        assert_stages(&plan, 8);
        assert_overflow(result, "Int64", "ungrouped merge");
        let (plan, result) = run(signed(&parts), 8, GROUPED);
        assert_stages(&plan, 8);
        assert_overflow(result, "Int64", "grouped merge");
    }

    #[test]
    fn totals_at_the_limits_are_returned_exactly() {
        let (_, result) = run(signed(&[ungrouped(&[MAX])]), 1, UNGROUPED);
        assert_eq!(i64_cells(&result.expect("MAX alone")), vec![Some(MAX)]);
        let (_, result) = run(signed(&[ungrouped(&[MIN])]), 1, UNGROUPED);
        assert_eq!(i64_cells(&result.expect("MIN alone")), vec![Some(MIN)]);
        let (_, result) = run(unsigned(&[ungrouped(&[u64::MAX])]), 1, UNGROUPED);
        assert_eq!(
            u64_cells(&result.expect("u64::MAX alone")),
            vec![Some(u64::MAX)]
        );

        // Orders whose running total never leaves Int64, one partition and
        // one value per partition.
        for order in [[MAX, -1, 1], [-1, MAX, 1], [-1, 1, MAX], [1, -1, MAX]] {
            for partitions in [1, 8] {
                let rows = ungrouped(&order);
                let (_, result) = run(signed(&spread(&rows, partitions)), partitions, UNGROUPED);
                assert_eq!(
                    i64_cells(&result.expect("in range")),
                    vec![Some(MAX)],
                    "{order:?} at {partitions}"
                );
            }
        }
    }

    /// The sum is exact in `i128` and only the final total is checked, so an
    /// order whose running total passes `i64::MAX` on the way still returns
    /// the total when it fits, in one partition and when the out-of-range
    /// partial is a whole partition's state.
    #[test]
    fn an_out_of_range_partial_whose_total_fits_returns_the_total() {
        for order in [[MAX, 1, -1], [1, MAX, -1]] {
            let (plan, result) = run(signed(&[ungrouped(&order)]), 1, UNGROUPED);
            assert_stages(&plan, 1);
            assert_eq!(
                i64_cells(&result.expect("fits")),
                vec![Some(MAX)],
                "{order:?}"
            );
        }
        let parts = vec![vec![(1, Some(MAX)), (1, Some(1))], vec![(1, Some(-1))]];
        for sql in [UNGROUPED, GROUPED] {
            let (plan, result) = run(signed(&parts), 8, sql);
            assert_stages(&plan, 8);
            assert_eq!(i64_cells(&result.expect("fits")), vec![Some(MAX)], "{sql}");
        }
        let parts = vec![vec![(1, Some(MIN)), (1, Some(-1))], vec![(1, Some(1))]];
        let (_, result) = run(signed(&parts), 8, UNGROUPED);
        assert_eq!(i64_cells(&result.expect("fits")), vec![Some(MIN)]);
        let parts = vec![vec![(1, Some(u64::MAX)), (1, Some(1))], vec![]];
        let (_, result) = run(unsigned(&parts), 8, UNGROUPED);
        assert_overflow(result, "UInt64", "unsigned partial past the limit");
    }

    #[test]
    fn nulls_are_skipped_and_an_empty_or_all_null_input_is_null() {
        let (_, result) = run(signed(&[vec![(1, Some(MAX)), (1, None)]]), 1, UNGROUPED);
        assert_eq!(i64_cells(&result.expect("null skipped")), vec![Some(MAX)]);
        let (_, result) = run(signed(&[vec![(1, None), (1, None)]]), 1, UNGROUPED);
        assert_eq!(i64_cells(&result.expect("all null")), vec![None]);
        let (_, result) = run(signed(&[vec![]]), 1, UNGROUPED);
        assert_eq!(i64_cells(&result.expect("empty")), vec![None]);
        let (_, result) = run(unsigned(&[vec![]]), 1, UNGROUPED);
        assert_eq!(u64_cells(&result.expect("empty")), vec![None]);

        let rows = vec![(1, None), (2, Some(MAX)), (2, None), (1, None)];
        for partitions in [1, 8] {
            let (_, result) = run(signed(&spread(&rows, partitions)), partitions, GROUPED);
            assert_eq!(
                i64_cells(&result.expect("grouped nulls")),
                vec![None, Some(MAX)],
                "at {partitions}"
            );
        }
    }

    #[test]
    fn distinct_sums_the_set_with_the_same_check() {
        for partitions in [1, 8] {
            let rows = ungrouped(&[MAX, MAX, MAX]);
            let (_, result) = run(
                signed(&spread(&rows, partitions)),
                partitions,
                "SELECT sum(DISTINCT v) FROM t",
            );
            assert_eq!(i64_cells(&result.expect("set {MAX}")), vec![Some(MAX)]);

            let rows = ungrouped(&[MAX, MAX, 1, 1]);
            let (_, result) = run(
                signed(&spread(&rows, partitions)),
                partitions,
                "SELECT sum(DISTINCT v) FROM t",
            );
            assert_overflow(result, "Int64", &format!("distinct at {partitions}"));
            let (_, result) = run(
                signed(&spread(&with_fitting_group(rows, 3), partitions)),
                partitions,
                "SELECT sum(DISTINCT v) FROM t GROUP BY k ORDER BY k",
            );
            assert_overflow(
                result,
                "Int64",
                &format!("grouped distinct at {partitions}"),
            );
            // Two DISTINCT aggregates keep the distinct accumulator itself
            // (no single-distinct rewrite to a GROUP BY).
            let (_, result) = run(
                signed(&spread(&ungrouped(&[MAX, 1, 1]), partitions)),
                partitions,
                "SELECT sum(DISTINCT v), count(DISTINCT o) FROM t",
            );
            assert_overflow(result, "Int64", &format!("two distincts at {partitions}"));
        }
    }

    #[test]
    fn filter_is_applied_before_the_check() {
        // Group 1 holds MAX and 1; group 2 holds -1 and 1.
        let rows = vec![(1, Some(MAX)), (1, Some(1)), (2, Some(-1)), (2, Some(1))];
        for partitions in [1, 8] {
            let parts = spread(&rows, partitions);
            let (_, result) = run(
                signed(&parts),
                partitions,
                "SELECT sum(v) FILTER (WHERE v > 0) FROM t",
            );
            assert_overflow(
                result,
                "Int64",
                &format!("filter keeps both at {partitions}"),
            );
            let (_, result) = run(
                signed(&parts),
                partitions,
                "SELECT sum(v) FILTER (WHERE v <> 1) FROM t",
            );
            assert_eq!(
                i64_cells(&result.expect("filter drops the 1s")),
                vec![Some(MAX - 1)]
            );
            let (_, result) = run(
                signed(&parts),
                partitions,
                "SELECT sum(v) FILTER (WHERE v > 0) FROM t GROUP BY k ORDER BY k",
            );
            assert_overflow(result, "Int64", &format!("grouped filter at {partitions}"));
            let (_, result) = run(
                signed(&parts),
                partitions,
                "SELECT sum(v) FILTER (WHERE o <> 2) FROM t GROUP BY k ORDER BY k",
            );
            assert_eq!(
                i64_cells(&result.expect("grouped filter drops the 1")),
                vec![Some(MAX), Some(0)]
            );
        }
    }

    #[test]
    fn window_sums_are_checked_for_growing_and_moving_frames() {
        let growing = "SELECT sum(v) OVER (ORDER BY o) FROM t";
        let moving =
            "SELECT sum(v) OVER (ORDER BY o ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t";
        let (_, result) = run(signed(&[ungrouped(&[MAX, 1])]), 1, growing);
        assert_overflow(result, "Int64", "growing frame");
        let (_, result) = run(signed(&[ungrouped(&[1, MAX, -5])]), 1, moving);
        assert_overflow(result, "Int64", "moving frame");
        // Every moving frame fits, although the whole input does not.
        let (_, result) = run(signed(&[ungrouped(&[MAX, -1, 1, -5, MAX])]), 1, moving);
        assert_eq!(
            i64_cells(&result.expect("every frame fits")),
            vec![Some(MAX), Some(MAX - 1), Some(0), Some(-4), Some(MAX - 5)]
        );
    }

    #[test]
    fn non_integer_types_delegate_and_small_integers_widen() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f", DataType::Float64, false),
            Field::new("i8", DataType::Int8, false),
            Field::new("u8", DataType::UInt8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![f64::MAX, f64::MAX])),
                Arc::new(Int8Array::from(vec![i8::MAX, i8::MAX])),
                Arc::new(UInt8Array::from(vec![u8::MAX, u8::MAX])),
            ],
        )
        .expect("batch");
        let table = Table {
            schema,
            partitions: vec![vec![batch]],
        };
        let (_, result) = run(table, 1, "SELECT sum(f), sum(i8), sum(u8) FROM t");
        let batches = result.expect("delegated and widened sums");
        let batch = &batches[0];
        let f = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64 sum");
        assert_eq!(f.value(0), f64::INFINITY);
        let i = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("Int64 sum of Int8");
        assert_eq!(i.value(0), 254);
        let u = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("UInt64 sum of UInt8");
        assert_eq!(u.value(0), 510);
    }

    /// `SUM(v + 1)` is not rewritten to `SUM(v) + COUNT(v)`, whose outer `+`
    /// would wrap: here the per-row values are `MAX` and `1`.
    #[test]
    fn sum_of_column_plus_integer_literal_is_checked() {
        let (plan, result) = run(
            signed(&[ungrouped(&[MAX - 1, 0])]),
            1,
            "SELECT sum(v + 1) FROM t",
        );
        assert!(!plan.contains("count("), "{plan}");
        assert_overflow(result, "Int64", "sum(v + 1)");
    }

    /// The groups accumulator's pass-through path (`convert_to_state`, used
    /// when a partial aggregation skips grouping) drops filtered and NULL
    /// rows and keeps the check at evaluation.
    #[test]
    fn convert_to_state_then_merge_is_checked() {
        let mut accumulator = SignedGroups::new(SumKind::Signed);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(MAX), None, Some(1), Some(7)]));
        let filter = BooleanArray::from(vec![Some(true), Some(true), Some(true), None]);
        let state = accumulator
            .convert_to_state(&[Arc::clone(&values)], Some(&filter))
            .expect("state");
        let partials = as_decimal128(&state[0]).expect("decimal");
        assert_eq!(
            partials.iter().collect::<Vec<_>>(),
            vec![Some(i128::from(MAX)), None, Some(1), None]
        );
        accumulator
            .merge_batch(&state, &[0, 0, 0, 1], 2)
            .expect("merge");
        let err = accumulator.evaluate(EmitTo::All).expect_err("overflow");
        assert!(matches!(
            crate::executor::take_sql_error(err),
            Ok(SqlError::IntegerSumOverflow {
                result_type: "Int64"
            })
        ));

        // The no-null, no-filter loop and the general loop agree.
        let values: ArrayRef = Arc::new(Int64Array::from(vec![5, -3, MAX, 2]));
        let groups = [0, 1, 0, 1];
        let mut fast = SignedGroups::new(SumKind::Signed);
        fast.update_batch(&[Arc::clone(&values)], &groups, None, 3)
            .expect("fast");
        let mut general = SignedGroups::new(SumKind::Signed);
        let admit_all = BooleanArray::from(vec![true; 4]);
        general
            .update_batch(&[values], &groups, Some(&admit_all), 3)
            .expect("general");
        assert_eq!(fast.totals(), vec![i128::from(MAX) + 5, -1, 0]);
        assert_eq!(fast.totals(), general.totals());
        assert_eq!(fast.seen_flags(), vec![true, true, false]);
        assert_eq!(fast.seen_flags(), general.seen_flags());
        let err = fast.evaluate(EmitTo::All).expect_err("group 0 overflows");
        assert!(matches!(
            crate::executor::take_sql_error(err),
            Ok(SqlError::IntegerSumOverflow { .. })
        ));
    }

    type SignedGroups = CheckedSumGroupsAccumulator<Int64Type>;

    impl<T> CheckedSumGroupsAccumulator<T>
    where
        T: ArrowPrimitiveType,
        T::Native: NarrowSum,
    {
        /// Each group's exact running sum, narrow sum plus carry.
        fn totals(&self) -> Vec<i128> {
            let mut totals: Vec<i128> = self.sums.narrow.iter().map(|&n| n.into()).collect();
            for (&group, &carry) in &self.sums.carries {
                totals[group] += carry;
            }
            totals
        }

        fn seen_flags(&self) -> Vec<bool> {
            (0..self.seen.len()).map(|g| self.seen.get_bit(g)).collect()
        }
    }

    /// `GROUPS` distinct groups, one row each, in 8192-row batches, as a
    /// partial aggregation over all-distinct keys grows them.
    const GROUPS: usize = 1 << 17;

    fn grown_to_groups() -> SignedGroups {
        let mut accumulator = SignedGroups::new(SumKind::Signed);
        for start in (0..GROUPS).step_by(8192) {
            let values: ArrayRef = Arc::new(Int64Array::from(vec![1i64; 8192]));
            let groups: Vec<usize> = (start..start + 8192).collect();
            accumulator
                .update_batch(&[values], &groups, None, start + 8192)
                .expect("update");
        }
        accumulator
    }

    /// The grouped state is an `i64` and a validity bit per group. Over
    /// 131,072 groups grown batch by batch it reports 8 x 131,072 + 131,072 / 8
    /// = 1,064,960 bytes, 8.125 per group, within 9 per group plus a 64-byte
    /// allowance (arrow's bitmap allocation granule). The `i128` and `bool`
    /// layout this replaced reported 17 x 131,072 = 2,228,224.
    #[test]
    fn grouped_state_is_at_most_nine_bytes_per_group() {
        let accumulator = grown_to_groups();
        let size = accumulator.size();
        assert!(size <= 9 * GROUPS + 64, "{size} bytes for {GROUPS} groups");
        assert_eq!(size, 1_064_960);
        assert_eq!(accumulator.sums.carried_groups(), 0);
    }

    /// A promoted group's carry is reported: 1,000 groups pushed past
    /// `i64::MAX` grow `size()` by at least their 1,000 carry entries.
    #[test]
    fn promoted_groups_are_counted_in_size() {
        let mut accumulator = grown_to_groups();
        let before = accumulator.size();
        let groups: Vec<usize> = (0..1000).collect();
        let values: ArrayRef = Arc::new(Int64Array::from(vec![MAX; 1000]));
        accumulator
            .update_batch(&[values], &groups, None, GROUPS)
            .expect("update");
        assert_eq!(accumulator.sums.carried_groups(), 1000);
        let after = accumulator.size();
        let entry = std::mem::size_of::<(usize, i128)>() + 1;
        assert!(
            after >= before + 1000 * entry,
            "{after} bytes after promoting 1,000 groups, {before} before"
        );
        assert_eq!(accumulator.totals()[0], i128::from(MAX) + 1);
    }

    /// A group whose running sum passes `i64::MAX` (or `i64::MIN`) and comes
    /// back returns the exact total, beside groups that never leave `i64`,
    /// grouped at one and eight partitions.
    #[test]
    fn a_group_that_leaves_int64_and_comes_back_returns_the_exact_total() {
        let rows = vec![
            (1, Some(MAX)),
            (1, Some(1)),
            (1, Some(-2)),
            (2, Some(5)),
            (2, Some(6)),
            (3, Some(MAX)),
            (3, Some(MAX)),
            (3, Some(MIN)),
            (3, Some(MIN)),
            (3, Some(MAX)),
            (4, Some(MIN)),
            (4, Some(-1)),
            (4, Some(1)),
            (5, Some(-3)),
        ];
        let expected = vec![
            Some(MAX - 1),
            Some(11),
            Some(MAX - 2),
            Some(MIN),
            Some(-3),
        ];
        let (plan, result) = run(signed(std::slice::from_ref(&rows)), 1, GROUPED);
        assert_stages(&plan, 1);
        assert_eq!(i64_cells(&result.expect("one partition")), expected);
        // Round-robin over eight partitions puts no two rows of one group in
        // the same partition, so every partial fits and only the merge
        // leaves `i64`.
        let parts = spread(&rows, 8);
        for part in &parts {
            let mut keys: Vec<i64> = part.iter().map(|(k, _)| *k).collect();
            keys.sort_unstable();
            keys.dedup();
            assert_eq!(keys.len(), part.len(), "{part:?}");
        }
        let (plan, result) = run(signed(&parts), 8, GROUPED);
        assert_stages(&plan, 8);
        assert_eq!(i64_cells(&result.expect("eight partitions")), expected);
    }

    /// A group that leaves the result type and stays out fails, at one and
    /// eight partitions, signed and unsigned, while its neighbours fit.
    #[test]
    fn a_group_that_leaves_and_stays_out_fails() {
        let rows = vec![(1, Some(MAX)), (1, Some(1)), (1, Some(1)), (2, Some(3))];
        let wide = vec![(1, Some(u64::MAX)), (1, Some(5)), (2, Some(3))];
        for partitions in [1, 8] {
            let (_, result) = run(signed(&spread(&rows, partitions)), partitions, GROUPED);
            assert_overflow(result, "Int64", &format!("signed at {partitions}"));
            let (_, result) = run(unsigned(&spread(&wide, partitions)), partitions, GROUPED);
            assert_overflow(result, "UInt64", &format!("unsigned at {partitions}"));
        }
    }

    /// The merge promotes on overflow too, including a partial outside `i64`,
    /// and `EmitTo::First` renumbers the carries of the groups it leaves.
    #[test]
    fn merge_promotes_and_emit_first_keeps_the_carries() {
        let mut accumulator = SignedGroups::new(SumKind::Signed);
        let max = i128::from(MAX);
        let min = i128::from(MIN);
        let partials = Decimal128Array::from(vec![
            Some(max),
            Some(max),
            Some(min),
            Some(-1),
            Some(2 * max),
            Some(min),
            Some(min),
            Some(5),
            None,
            Some(7),
            Some(max),
            Some(1),
        ])
        .with_precision_and_scale(38, 0)
        .expect("decimal");
        let state: ArrayRef = Arc::new(partials);
        accumulator
            .merge_batch(&[state], &[0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 3, 3], 5)
            .expect("merge");
        assert_eq!(
            accumulator.totals(),
            vec![max - 2, 3, 7, max + 1, 0],
            "exact sums"
        );
        assert_eq!(accumulator.sums.carried_groups(), 3);

        let first = accumulator.evaluate(EmitTo::First(3)).expect("first three");
        let first = first.as_any().downcast_ref::<Int64Array>().expect("Int64");
        assert_eq!(
            first.iter().collect::<Vec<_>>(),
            vec![Some(MAX - 2), Some(3), Some(7)]
        );
        assert_eq!(accumulator.sums.carried_groups(), 1);
        assert_eq!(accumulator.totals(), vec![max + 1, 0]);
        assert_eq!(accumulator.seen_flags(), vec![true, false]);
        // Group 3, now group 0, still holds its carry and overflows.
        let state = accumulator.state(EmitTo::All).expect("state");
        let state = as_decimal128(&state[0]).expect("decimal");
        assert_eq!(state.iter().collect::<Vec<_>>(), vec![Some(max + 1), None]);
    }

    #[test]
    #[ignore = "scratch timing harness"]
    fn zz_scratch_timing_grouped_sum() {
        let rows = 10_000_000usize;
        let groups = 1_000_000usize;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let mut partitions = vec![Vec::new(); 8];
        let mut start = 0usize;
        let mut batch_no = 0usize;
        while start < rows {
            let end = (start + 8192).min(rows);
            let k: Vec<i64> = (start..end)
                .map(|i| ((i.wrapping_mul(7919)) % groups) as i64)
                .collect();
            let v: Vec<i64> = (start..end).map(|i| (i % 1000) as i64).collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(k)), Arc::new(Int64Array::from(v))],
            )
            .expect("batch");
            partitions[batch_no % 8].push(batch);
            batch_no += 1;
            start = end;
        }
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(8)
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
            ctx.register_udaf(checked_sum_udaf());
            let mem = MemTable::try_new(schema, partitions).expect("memtable");
            ctx.register_table("t", Arc::new(mem)).expect("register");
            for round in 0..2 {
                let t0 = std::time::Instant::now();
                let batches = ctx
                    .sql("SELECT k, sum(v) FROM t GROUP BY k")
                    .await
                    .expect("plan")
                    .collect()
                    .await
                    .expect("run");
                let n: usize = batches.iter().map(|b| b.num_rows()).sum();
                assert_eq!(n, groups);
                println!("TIMING round={round} ms={}", t0.elapsed().as_millis());
            }
        });
    }

    #[test]
    fn overflow_error_text_names_the_aggregate_and_result_type() {
        let err = SqlError::IntegerSumOverflow {
            result_type: "Int64",
        };
        assert_eq!(err.class(), crate::error::ErrorClass::Unsupported);
        assert_eq!(err.client_message(), err.to_string());
        assert!(
            err.client_message()
                .starts_with("aggregate sum overflowed its Int64 result type"),
            "{err}"
        );
    }
}
