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
//! accumulator in the shape of DataFusion's own primitive one: one `i128` and
//! one seen flag per group, updated from the whole input array and the
//! batch's group indices in one loop, with no per-group `Accumulator` object
//! and no `ScalarValue` per row. A batch with no nulls and no filter takes a
//! loop over the raw value slice.
//!
//! `sum(DISTINCT x)` sums the set of distinct values exactly and applies the
//! same check. A window `sum` runs [`CheckedSumAccumulator`] for an
//! unbounded-preceding frame and the same accumulator with retraction for a
//! moving frame, so every frame's total is checked the same way.
//!
//! The built-in's `SUM(x + c)` to `SUM(x) + c * COUNT(x)` rewrite is not
//! applied for an integer literal: the outer `+` and `*` are DataFusion
//! scalar arithmetic, which wraps, so the rewritten form could return a
//! wrapped total where the checked `sum` would have failed.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanArray, Decimal128Array, Int64Array, ListArray,
    PrimitiveArray, UInt64Array,
};
use datafusion::arrow::buffer::{NullBuffer, ScalarBuffer};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type, UInt64Type};
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

/// Per-group bytes of [`CheckedSumGroupsAccumulator`]: an `i128` sum and a
/// `bool` seen flag.
pub(crate) const EXACT_SUM_GROUP_WIDTHS: [usize; 2] = [16, 1];

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SumKind {
    /// Signed integer input, coerced to `Int64`; returns `Int64`.
    Signed,
    /// Unsigned integer input, coerced to `UInt64`; returns `UInt64`.
    Unsigned,
}

/// The checked kind of a call returning `return_type`; `None` for every other
/// result type, which the built-in runs unchanged.
fn sum_kind(return_type: &DataType) -> Option<SumKind> {
    match return_type {
        DataType::Int64 => Some(SumKind::Signed),
        DataType::UInt64 => Some(SumKind::Unsigned),
        _ => None,
    }
}

impl SumKind {
    fn result_type(self) -> DataType {
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
    fn overflow(self) -> DataFusionError {
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

    fn checked_add(self, acc: i128, value: i128) -> DFResult<i128> {
        acc.checked_add(value).ok_or_else(|| self.overflow())
    }

    /// `sum` as a `Decimal128(38, 0)` state value, or the overflow error when
    /// it is outside that type's range.
    fn state_value(self, sum: i128) -> DFResult<i128> {
        if (-DECIMAL128_MAX_UNSCALED..=DECIMAL128_MAX_UNSCALED).contains(&sum) {
            Ok(sum)
        } else {
            Err(self.overflow())
        }
    }

    /// Visit every non-null value of `array` (the call's coerced input) as an
    /// `i128`, with its row index.
    fn for_each_value(
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
fn as_primitive<T: ArrowPrimitiveType>(array: &ArrayRef) -> DFResult<PrimitiveArray<T>> {
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

fn as_decimal128(array: &ArrayRef) -> DFResult<&Decimal128Array> {
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
fn row_is_filtered(opt_filter: Option<&BooleanArray>, row: usize) -> bool {
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
            Ok(Box::new(CheckedSumGroupsAccumulator::new(kind)))
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

/// The flat per-group accumulator for an integer call. `sums[g]` is group
/// `g`'s exact running sum and `seen[g]` whether any non-null value reached
/// it; an unseen group evaluates to NULL.
#[derive(Debug)]
struct CheckedSumGroupsAccumulator {
    kind: SumKind,
    sums: Vec<i128>,
    seen: Vec<bool>,
}

impl CheckedSumGroupsAccumulator {
    fn new(kind: SumKind) -> Self {
        Self {
            kind,
            sums: Vec::new(),
            seen: Vec::new(),
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.sums.resize(total_num_groups, 0);
        self.seen.resize(total_num_groups, false);
    }

    fn update_primitive<T>(
        &mut self,
        values: &PrimitiveArray<T>,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
    ) -> DFResult<()>
    where
        T: ArrowPrimitiveType,
        T::Native: Into<i128>,
    {
        let kind = self.kind;
        if values.null_count() == 0 && opt_filter.is_none() {
            for (&group_index, &value) in group_indices.iter().zip(values.values().iter()) {
                let sum = &mut self.sums[group_index];
                *sum = kind.checked_add(*sum, value.into())?;
                self.seen[group_index] = true;
            }
            return Ok(());
        }
        for (row, &group_index) in group_indices.iter().enumerate() {
            if row_is_filtered(opt_filter, row) || values.is_null(row) {
                continue;
            }
            let sum = &mut self.sums[group_index];
            *sum = kind.checked_add(*sum, values.value(row).into())?;
            self.seen[group_index] = true;
        }
        Ok(())
    }

    /// Take the groups `emit_to` names, as their sums and a validity buffer
    /// that is false for each unseen group.
    fn take(&mut self, emit_to: EmitTo) -> (Vec<i128>, NullBuffer) {
        let sums = emit_to.take_needed(&mut self.sums);
        let seen = emit_to.take_needed(&mut self.seen);
        (sums, NullBuffer::from(seen))
    }
}

impl GroupsAccumulator for CheckedSumGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups);
        match self.kind {
            SumKind::Unsigned => {
                let values = as_primitive::<UInt64Type>(&values[0])?;
                self.update_primitive(&values, group_indices, opt_filter)
            }
            SumKind::Signed => {
                let values = as_primitive::<Int64Type>(&values[0])?;
                self.update_primitive(&values, group_indices, opt_filter)
            }
        }
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups);
        let kind = self.kind;
        let partials = as_decimal128(&values[0])?;
        for (row, &group_index) in group_indices.iter().enumerate() {
            if partials.is_valid(row) {
                let sum = &mut self.sums[group_index];
                *sum = kind.checked_add(*sum, partials.value(row))?;
                self.seen[group_index] = true;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let kind = self.kind;
        let (sums, nulls) = self.take(emit_to);
        // An unseen group's sum is zero, so it always converts; its slot is
        // masked by `nulls`.
        Ok(match kind {
            SumKind::Unsigned => {
                let values = sums
                    .into_iter()
                    .map(|sum| u64::try_from(sum).map_err(|_| kind.overflow()))
                    .collect::<DFResult<Vec<u64>>>()?;
                Arc::new(UInt64Array::new(ScalarBuffer::from(values), Some(nulls)))
            }
            SumKind::Signed => {
                let values = sums
                    .into_iter()
                    .map(|sum| i64::try_from(sum).map_err(|_| kind.overflow()))
                    .collect::<DFResult<Vec<i64>>>()?;
                Arc::new(Int64Array::new(ScalarBuffer::from(values), Some(nulls)))
            }
        })
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let kind = self.kind;
        let (sums, nulls) = self.take(emit_to);
        for &sum in &sums {
            kind.state_value(sum)?;
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
        self.kind.for_each_value(&values[0], |row, value| {
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
        // capacity; like the built-in's, the fixed struct is not counted anywhere.
        self.sums.capacity() * std::mem::size_of::<i128>()
            + self.seen.capacity() * std::mem::size_of::<bool>()
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
    use datafusion::arrow::array::{Float64Array, Int8Array, UInt8Array};
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
        let mut accumulator = CheckedSumGroupsAccumulator::new(SumKind::Signed);
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
        let mut fast = CheckedSumGroupsAccumulator::new(SumKind::Signed);
        fast.update_batch(&[Arc::clone(&values)], &groups, None, 3)
            .expect("fast");
        let mut general = CheckedSumGroupsAccumulator::new(SumKind::Signed);
        let admit_all = BooleanArray::from(vec![true; 4]);
        general
            .update_batch(&[values], &groups, Some(&admit_all), 3)
            .expect("general");
        assert_eq!(fast.sums, vec![i128::from(MAX) + 5, -1, 0]);
        assert_eq!(fast.sums, general.sums);
        assert_eq!(fast.seen, vec![true, true, false]);
        assert_eq!(fast.seen, general.seen);
        let err = fast.evaluate(EmitTo::All).expect_err("group 0 overflows");
        assert!(matches!(
            crate::executor::take_sql_error(err),
            Ok(SqlError::IntegerSumOverflow { .. })
        ));
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
