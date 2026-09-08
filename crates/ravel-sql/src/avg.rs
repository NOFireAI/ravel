//! Grouped-aware AVG/MEAN with two owned numerator kinds (ADR-0022 decisions
//! 3, 4; ADR-0825 decisions 1, 2).
//!
//! DataFusion 54.1.0's built-in `avg` (`AvgAccumulator`, `average.rs`) sums
//! each input batch with arrow's `compute::sum` kernel, which reduces
//! lane-parallel partial accumulators whose lane count is
//! architecture-dependent (4 on aarch64, 8 with AVX) and then folds the
//! per-batch partials with `+=`. Float addition is not associative, so no
//! portable sequential reference can be bit-identical to it. That is the same
//! root cause behind the ungrouped `sum` restriction the differential gate
//! already documents (crate::minmax has the analogous story for min/max).
//!
//! This module replaces the built-in `avg`/`mean` with a UDAF that owns two
//! numerator kinds, selected by the coerced argument type ([`avg_kind`]):
//!
//! * **Float64 input**: a naive sequential IEEE f64 fold over the non-null
//!   input values, seeded with the first value rather than a zero seed (so a
//!   group of all `-0.0` values folds to `-0.0`; a `0.0` seed would flip it to
//!   `+0.0`). Grouped execution ([`SequentialAvgGroupsAccumulator`]) folds
//!   each group in the same row order a plain per-group accumulator would,
//!   so the two paths are bit-identical. The differential gate compares
//!   against a reference running this identical algorithm; the summation is
//!   naive, not Kahan (ADR-0022 decision 3), because compensation buys no
//!   exactness against that reference.
//! * **Admitted integer input** (`Int8`-`Int64`, `UInt8`-`UInt32`, coerced to
//!   `Int64` by [`SequentialAvg::coerce_types`] rather than delegating up to
//!   Float64): exact `i128` accumulation with checked addition, so the result
//!   is a function of the input multiset only, never of partitioning or merge
//!   order (integer addition is associative and exact; no fold-order
//!   dependence exists to pin). Partial state is `(Decimal128(38, 0) sum,
//!   Int64 count)`; packing the `i128` sum into `Decimal128(38, 0)` is
//!   checked, because `i128`'s range exceeds what 38 unscaled digits can
//!   represent. Evaluation converts the exact sum to `f64` once and performs
//!   one IEEE division by the count. Return type stays Float64 either way
//!   (ADR-0825).
//!
//! Any other input type (Decimal, Duration; unreachable on the v1 `samples`
//! surface, whose only numeric column is Float64) delegates entirely to the
//! wrapped built-in: metadata (name, aliases, return type) always delegates,
//! and coercion delegates for every argument type this module does not own.
//!
//! Both owned kinds implement a real [`GroupsAccumulator`]
//! ([`SequentialAvgGroupsAccumulator`], [`ExactIntegerAvgGroupsAccumulator`]):
//! [`SequentialAvg::groups_accumulator_supported`] returns true for them, so
//! grouped execution no longer runs behind `GroupsAccumulatorAdapter`. Each
//! stores flat per-group state (a sums vector and a counts vector indexed by
//! group index) and folds every group's values in the row order the plan
//! delivers them, so the grouped and ungrouped paths compute the identical
//! fold for identical input. State is `(sum, count)` for both kinds; a merge
//! of partial states adds sums (plain IEEE addition for Float64, checked
//! `i128` addition for the integer kind) and adds counts, so a two-phase plan
//! (one partial state per group) reconstitutes the same result as a
//! single-phase one -- bit-identical for Float64, exact for the integer kind
//! regardless of how partitioning or merge order differ. See ADR-0022 and
//! ADR-0825.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator,
    ReversedUDAF, Signature,
};
use datafusion::scalar::ScalarValue;

/// The sequential-fold AVG UDAF, registered under `avg` (and its `mean`
/// alias) to displace DataFusion's built-in whose lane-reduced batch sum is
/// unpinnable (ADR-0022 decision 4) and whose integer path always widens to
/// Float64 before summing (ADR-0825).
pub fn sequential_avg_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(SequentialAvg::new())
}

/// An AVG/MEAN UDAF that owns Float64 and admitted-integer numerator
/// semantics and delegates everything else to the wrapped built-in.
///
/// Metadata methods (name, aliases, return type) forward to `inner`. Coercion
/// diverges for admitted integer arguments (kept as `Int64` instead of widened
/// to Float64); everything else delegates to the built-in's own coercion via
/// [`fields_with_udf`]. [`Self::state_fields`], [`Self::accumulator`],
/// [`Self::groups_accumulator_supported`], and
/// [`Self::create_groups_accumulator`] all dispatch on [`avg_kind`] of the
/// (coerced) argument type.
#[derive(Debug)]
struct SequentialAvg {
    /// The wrapped built-in `avg` (`avg_udaf()`), used for metadata
    /// delegation and for non-owned input types.
    inner: Arc<dyn AggregateUDFImpl>,
    /// The wrapped built-in as an `AggregateUDF`, used to run its own
    /// Coercible-signature resolution via [`fields_with_udf`] for delegated
    /// argument types. Calling `inner.coerce_types` directly is not an
    /// option: the built-in signature is `Coercible`, and DataFusion never
    /// calls an `AggregateUDFImpl::coerce_types` for a `Coercible` signature,
    /// so the built-in never implements that method itself.
    inner_udf: AggregateUDF,
    /// A `TypeSignature::UserDefined` signature so DataFusion actually calls
    /// [`Self::coerce_types`] (it does not for `Coercible`, which is what the
    /// built-in uses).
    signature: Signature,
}

impl SequentialAvg {
    fn new() -> Self {
        let inner_udf: AggregateUDF = (*avg_udaf()).clone();
        let volatility = inner_udf.signature().volatility;
        Self {
            inner: inner_udf.inner().clone(),
            inner_udf,
            signature: Signature::user_defined(volatility),
        }
    }
}

// `AggregateUDFImpl` requires `DynEq`/`DynHash`; this impl carries no state of
// its own (`inner`/`inner_udf`/`signature` are a pure function of the
// built-in `avg`), so all instances are equal.
impl PartialEq for SequentialAvg {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SequentialAvg {}

impl std::hash::Hash for SequentialAvg {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

/// The resolved argument types this UDAF coerces an admitted integer
/// argument to `Int64` from, instead of delegating to the built-in's
/// Float64-widening coercion (ADR-0825 decision 2). `UInt64` is excluded: it
/// does not fit `Int64` without narrowing, so it keeps the built-in's
/// Float64-widening behavior.
fn is_admitted_integer(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
    )
}

/// Which numerator kind owns a (coerced) argument type. Only ever inspected
/// after coercion, so the integer case is always `Int64` (the target
/// [`SequentialAvg::coerce_types`] admits integer arguments to).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AvgKind {
    Float,
    Integer,
    Delegate,
}

fn avg_kind(data_type: &DataType) -> AvgKind {
    match data_type {
        DataType::Float64 => AvgKind::Float,
        DataType::Int64 => AvgKind::Integer,
        _ => AvgKind::Delegate,
    }
}

impl AggregateUDFImpl for SequentialAvg {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn aliases(&self) -> &[String] {
        self.inner.aliases()
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        // The built-in's return type falls through to `Float64` for any
        // argument type it does not special-case (Decimal, Duration), which
        // includes `Int64`: admitting an integer argument to `Int64` in
        // `coerce_types` does not change the return type (ADR-0825: "return
        // type stays Float64").
        self.inner.return_type(arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        if arg_types.len() == 1 && is_admitted_integer(&arg_types[0]) {
            return Ok(vec![DataType::Int64]);
        }
        let fields: Vec<FieldRef> = arg_types
            .iter()
            .map(|data_type| Arc::new(Field::new("avg_arg", data_type.clone(), true)) as FieldRef)
            .collect();
        let coerced = fields_with_udf(&fields, &self.inner_udf)?;
        Ok(coerced
            .iter()
            .map(|field| field.data_type().clone())
            .collect())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        let kind = args
            .input_fields
            .first()
            .map(|field| avg_kind(field.data_type()))
            .unwrap_or(AvgKind::Delegate);
        match kind {
            // Matches [`SequentialAvgAccumulator`]/[`SequentialAvgGroupsAccumulator`]'s
            // `state`/`merge_batch`: a nullable Float64 running sum (null
            // until the first row) and an Int64 non-null count. Names are
            // namespaced by the aggregate expression name so a two-phase
            // plan's partial and final state schemas line up.
            AvgKind::Float => Ok(vec![
                Arc::new(Field::new(
                    format!("{}[avg_sum]", args.name),
                    DataType::Float64,
                    true,
                )),
                Arc::new(Field::new(
                    format!("{}[avg_count]", args.name),
                    DataType::Int64,
                    true,
                )),
            ]),
            // Matches [`ExactIntegerAvgAccumulator`]/[`ExactIntegerAvgGroupsAccumulator`]'s
            // `state`/`merge_batch`: a nullable Decimal128(38, 0) exact sum
            // (null when the group/partition saw no rows) and an Int64
            // non-null count.
            AvgKind::Integer => Ok(vec![
                Arc::new(Field::new(
                    format!("{}[avg_sum]", args.name),
                    DataType::Decimal128(38, 0),
                    true,
                )),
                Arc::new(Field::new(
                    format!("{}[avg_count]", args.name),
                    DataType::Int64,
                    true,
                )),
            ]),
            AvgKind::Delegate => self.inner.state_fields(args),
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        let kind = acc_args
            .expr_fields
            .first()
            .map(|field| avg_kind(field.data_type()))
            .unwrap_or(AvgKind::Delegate);
        match kind {
            AvgKind::Float => Ok(Box::new(SequentialAvgAccumulator::new())),
            AvgKind::Integer => Ok(Box::new(ExactIntegerAvgAccumulator::new())),
            AvgKind::Delegate => self.inner.accumulator(acc_args),
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        let kind = args
            .expr_fields
            .first()
            .map(|field| avg_kind(field.data_type()))
            .unwrap_or(AvgKind::Delegate);
        match kind {
            // Both owned kinds have a real `GroupsAccumulator` (see below):
            // no need to fall back to `GroupsAccumulatorAdapter`.
            AvgKind::Float | AvgKind::Integer => true,
            AvgKind::Delegate => self.inner.groups_accumulator_supported(args),
        }
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        let kind = args
            .expr_fields
            .first()
            .map(|field| avg_kind(field.data_type()))
            .unwrap_or(AvgKind::Delegate);
        match kind {
            AvgKind::Float => Ok(Box::new(SequentialAvgGroupsAccumulator::new())),
            AvgKind::Integer => Ok(Box::new(ExactIntegerAvgGroupsAccumulator::new())),
            AvgKind::Delegate => self.inner.create_groups_accumulator(args),
        }
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_expr()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

/// The plain per-group accumulator for Float64 input. `sum` is the running
/// numerator, `None` until the first non-null value is seen; `count` is the
/// number of non-null values folded. The result is `sum / count` in one IEEE
/// division, or NULL when `count` is zero.
#[derive(Debug)]
struct SequentialAvgAccumulator {
    /// The running numerator, or `None` until the first non-null value.
    sum: Option<f64>,
    /// The number of non-null values folded. Bounded far below 2^53 by the
    /// row materialization cap, so it is exact as f64 for the division
    /// (ADR-0022 decision 4).
    count: i64,
}

impl SequentialAvgAccumulator {
    fn new() -> Self {
        Self {
            sum: None,
            count: 0,
        }
    }

    /// Fold one value into the running numerator with plain IEEE addition,
    /// seeding with the first value rather than a zero seed so all-`-0.0`
    /// groups stay `-0.0`.
    fn fold(&mut self, value: f64) {
        self.sum = Some(match self.sum {
            None => value,
            Some(acc) => acc + value,
        });
    }

    /// Fold every non-null value of `array` after widening it to Float64.
    /// The input is already Float64 on the v1 surface; the cast is a no-op
    /// clone there.
    fn fold_values(&mut self, array: &ArrayRef) -> DFResult<()> {
        let float = as_float64(array)?;
        for i in 0..float.len() {
            if float.is_valid(i) {
                self.fold(float.value(i));
                self.count += 1;
            }
        }
        Ok(())
    }

    /// The average as a Float64 `ScalarValue`: `sum / count`, or NULL when no
    /// non-null rows were seen. A zero count yields NULL, never NaN or
    /// infinity (ADR-0022 decision 4).
    fn output(&self) -> ScalarValue {
        match self.sum {
            Some(sum) if self.count > 0 => ScalarValue::Float64(Some(sum / self.count as f64)),
            _ => ScalarValue::Float64(None),
        }
    }
}

/// Widen `array` to a `Float64Array`, cloning when it already is one.
fn as_float64(array: &ArrayRef) -> DFResult<Float64Array> {
    let float = if array.data_type() == &DataType::Float64 {
        Arc::clone(array)
    } else {
        cast(array, &DataType::Float64)?
    };
    float
        .as_any()
        .downcast_ref::<Float64Array>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "sequential avg: cast to Float64 did not yield a Float64Array".to_string(),
            )
        })
}

impl Accumulator for SequentialAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.fold_values(&values[0])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        // states[0] is the Float64 partial-sum column, states[1] the Int64
        // partial-count column (see `state_fields`). A partial state with no
        // rows carries a null sum and a zero count and contributes nothing.
        // Adding partial sums with the same plain IEEE addition keeps a
        // two-phase single-partition plan (one partial state per group)
        // bit-identical to the single-phase fold.
        let sums = as_float64(&states[0])?;
        let counts = states[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "sequential avg: merge count state is not an Int64Array".to_string(),
                )
            })?;
        for i in 0..sums.len() {
            if sums.is_valid(i) {
                self.fold(sums.value(i));
            }
            if counts.is_valid(i) {
                self.count += counts.value(i);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Float64(self.sum),
            ScalarValue::Int64(Some(self.count)),
        ])
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(self.output())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// Whether `opt_filter[row]` excludes `row` (absent filter admits every row;
/// a null filter slot means "unknown", which DataFusion treats as excluded,
/// the same as `false`).
fn row_is_filtered(opt_filter: Option<&BooleanArray>, row: usize) -> bool {
    match opt_filter {
        None => false,
        Some(filter) => filter.is_null(row) || !filter.value(row),
    }
}

/// The flat per-group `GroupsAccumulator` for Float64 input (ADR-0825
/// decision 1). `sums[group_index]` is `None` until that group's first
/// non-null value; `counts[group_index]` is its non-null row count. Folds in
/// the same row order [`SequentialAvgAccumulator`] would see for the same
/// group, so the grouped and ungrouped paths are bit-identical.
#[derive(Debug)]
struct SequentialAvgGroupsAccumulator {
    sums: Vec<Option<f64>>,
    counts: Vec<i64>,
}

impl SequentialAvgGroupsAccumulator {
    fn new() -> Self {
        Self {
            sums: Vec::new(),
            counts: Vec::new(),
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.sums.resize(total_num_groups, None);
        self.counts.resize(total_num_groups, 0);
    }

    fn fold(&mut self, group_index: usize, value: f64) {
        self.sums[group_index] = Some(match self.sums[group_index] {
            None => value,
            Some(acc) => acc + value,
        });
    }
}

impl GroupsAccumulator for SequentialAvgGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups);
        let float = as_float64(&values[0])?;
        for (row, &group_index) in group_indices.iter().enumerate() {
            if row_is_filtered(opt_filter, row) {
                continue;
            }
            if float.is_valid(row) {
                self.fold(group_index, float.value(row));
                self.counts[group_index] += 1;
            }
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups);
        let sums = as_float64(&values[0])?;
        let counts = values[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "sequential avg: merge count state is not an Int64Array".to_string(),
                )
            })?;
        for (row, &group_index) in group_indices.iter().enumerate() {
            if row_is_filtered(opt_filter, row) {
                continue;
            }
            if sums.is_valid(row) {
                self.fold(group_index, sums.value(row));
            }
            if counts.is_valid(row) {
                self.counts[group_index] += counts.value(row);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        let values: Float64Array = sums
            .into_iter()
            .zip(counts)
            .map(|(sum, count)| match sum {
                Some(sum) if count > 0 => Some(sum / count as f64),
                _ => None,
            })
            .collect();
        Ok(Arc::new(values))
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        let sum_array: Float64Array = sums.into_iter().collect();
        let count_array: Int64Array = counts.into_iter().map(Some).collect();
        Ok(vec![Arc::new(sum_array), Arc::new(count_array)])
    }

    fn size(&self) -> usize {
        self.sums.capacity() * std::mem::size_of::<Option<f64>>()
            + self.counts.capacity() * std::mem::size_of::<i64>()
    }
}

/// The largest unscaled magnitude `Decimal128(38, 0)` can represent: 38
/// nines (`10^38 - 1`). `i128::MAX` (~1.7014e38) exceeds this, so packing an
/// exact-integer-avg sum into `Decimal128(38, 0)` needs a checked bounds
/// test, not just a checked `i128` add.
const DECIMAL128_MAX_UNSCALED: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

/// Check that `sum` fits in `Decimal128(38, 0)`'s unscaled range, returning a
/// typed internal error (never a silent wrap or a panic) if it does not.
fn checked_decimal128_value(sum: i128) -> DFResult<i128> {
    if !(-DECIMAL128_MAX_UNSCALED..=DECIMAL128_MAX_UNSCALED).contains(&sum) {
        return Err(DataFusionError::Internal(format!(
            "exact integer avg: partial sum {sum} overflows Decimal128(38, 0)"
        )));
    }
    Ok(sum)
}

/// Add `value` to `acc` with checked `i128` addition, returning a typed
/// internal error (never a silent wrap or a panic) on overflow.
fn checked_add_i128(acc: i128, value: i128) -> DFResult<i128> {
    acc.checked_add(value).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "exact integer avg: sum overflowed i128 accumulating {value} onto {acc}"
        ))
    })
}

/// Widen `array` to an `Int64Array`, cloning when it already is one. The
/// input is already `Int64` on the v1 surface (admitted integer input is
/// coerced up by [`SequentialAvg::coerce_types`] before it reaches the
/// accumulator).
fn as_int64(array: &ArrayRef) -> DFResult<Int64Array> {
    let ints = if array.data_type() == &DataType::Int64 {
        Arc::clone(array)
    } else {
        cast(array, &DataType::Int64)?
    };
    ints.as_any()
        .downcast_ref::<Int64Array>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "exact integer avg: cast to Int64 did not yield an Int64Array".to_string(),
            )
        })
}

fn as_decimal128(array: &ArrayRef) -> DFResult<Decimal128Array> {
    array
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "exact integer avg: merge sum state is not a Decimal128Array".to_string(),
            )
        })
}

/// The plain per-group accumulator for admitted-integer input (ADR-0825
/// decision 2). `sum` accumulates exactly in `i128` with checked addition, so
/// it is a function of the input multiset only; integer addition is
/// associative, so unlike the Float64 kind there is no fold-order dependence
/// to pin. `count` is the number of non-null values folded. Evaluation
/// converts the exact sum to `f64` once and performs one IEEE division by
/// `count`.
#[derive(Debug)]
struct ExactIntegerAvgAccumulator {
    sum: i128,
    count: i64,
}

impl ExactIntegerAvgAccumulator {
    fn new() -> Self {
        Self { sum: 0, count: 0 }
    }

    fn fold(&mut self, value: i64) -> DFResult<()> {
        self.sum = checked_add_i128(self.sum, i128::from(value))?;
        self.count += 1;
        Ok(())
    }

    fn fold_values(&mut self, array: &ArrayRef) -> DFResult<()> {
        let ints = as_int64(array)?;
        for i in 0..ints.len() {
            if ints.is_valid(i) {
                self.fold(ints.value(i))?;
            }
        }
        Ok(())
    }

    /// The average as a Float64 `ScalarValue`: the exact `i128` sum widened
    /// to `f64` once, divided by `count` in one IEEE division. NULL when no
    /// non-null rows were seen.
    fn output(&self) -> ScalarValue {
        if self.count > 0 {
            ScalarValue::Float64(Some(self.sum as f64 / self.count as f64))
        } else {
            ScalarValue::Float64(None)
        }
    }
}

impl Accumulator for ExactIntegerAvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.fold_values(&values[0])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        // states[0] is the Decimal128(38, 0) partial-sum column, states[1]
        // the Int64 partial-count column (see `state_fields`). A partial
        // state with no rows carries a null sum and a zero count.
        let sums = as_decimal128(&states[0])?;
        let counts = states[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "exact integer avg: merge count state is not an Int64Array".to_string(),
                )
            })?;
        for i in 0..sums.len() {
            if sums.is_valid(i) {
                self.sum = checked_add_i128(self.sum, sums.value(i))?;
            }
            if counts.is_valid(i) {
                self.count += counts.value(i);
            }
        }
        Ok(())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        let sum = if self.count > 0 {
            ScalarValue::Decimal128(Some(checked_decimal128_value(self.sum)?), 38, 0)
        } else {
            ScalarValue::Decimal128(None, 38, 0)
        };
        Ok(vec![sum, ScalarValue::Int64(Some(self.count))])
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        Ok(self.output())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// The flat per-group `GroupsAccumulator` for admitted-integer input
/// (ADR-0825 decision 2). `sums[group_index]` is the exact running `i128`
/// numerator (starts at zero; integer addition is associative and exact, so
/// unlike the Float64 kind a zero seed is not observable in the result).
/// `counts[group_index]` is its non-null row count.
#[derive(Debug)]
struct ExactIntegerAvgGroupsAccumulator {
    sums: Vec<i128>,
    counts: Vec<i64>,
}

impl ExactIntegerAvgGroupsAccumulator {
    fn new() -> Self {
        Self {
            sums: Vec::new(),
            counts: Vec::new(),
        }
    }

    fn resize(&mut self, total_num_groups: usize) {
        self.sums.resize(total_num_groups, 0);
        self.counts.resize(total_num_groups, 0);
    }
}

impl GroupsAccumulator for ExactIntegerAvgGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups);
        let ints = as_int64(&values[0])?;
        for (row, &group_index) in group_indices.iter().enumerate() {
            if row_is_filtered(opt_filter, row) {
                continue;
            }
            if ints.is_valid(row) {
                self.sums[group_index] =
                    checked_add_i128(self.sums[group_index], i128::from(ints.value(row)))?;
                self.counts[group_index] += 1;
            }
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups);
        let sums = as_decimal128(&values[0])?;
        let counts = values[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "exact integer avg: merge count state is not an Int64Array".to_string(),
                )
            })?;
        for (row, &group_index) in group_indices.iter().enumerate() {
            if row_is_filtered(opt_filter, row) {
                continue;
            }
            if sums.is_valid(row) {
                self.sums[group_index] = checked_add_i128(self.sums[group_index], sums.value(row))?;
            }
            if counts.is_valid(row) {
                self.counts[group_index] += counts.value(row);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        let values: Float64Array = sums
            .into_iter()
            .zip(counts)
            .map(|(sum, count)| {
                if count > 0 {
                    Some(sum as f64 / count as f64)
                } else {
                    None
                }
            })
            .collect();
        Ok(Arc::new(values))
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        let mut sum_values: Vec<Option<i128>> = Vec::with_capacity(sums.len());
        for (sum, count) in sums.iter().zip(counts.iter()) {
            if *count > 0 {
                sum_values.push(Some(checked_decimal128_value(*sum)?));
            } else {
                sum_values.push(None);
            }
        }
        let sum_array: Decimal128Array = sum_values
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(38, 0)?;
        let count_array: Int64Array = counts.into_iter().map(Some).collect();
        Ok(vec![Arc::new(sum_array), Arc::new(count_array)])
    }

    fn size(&self) -> usize {
        self.sums.capacity() * std::mem::size_of::<i128>()
            + self.counts.capacity() * std::mem::size_of::<i64>()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// A quiet NaN with a chosen payload, so a bit-identity test can carry
    /// distinguishable NaN inputs through instead of a single canonical NaN.
    fn nan_with_payload(payload: u64) -> f64 {
        f64::from_bits(0x7ff8_0000_0000_0000 | (payload & 0x000f_ffff_ffff_ffff))
    }

    fn float64_bits(sv: ScalarValue) -> Option<u64> {
        match sv {
            ScalarValue::Float64(Some(v)) => Some(v.to_bits()),
            ScalarValue::Float64(None) => None,
            other => panic!("expected a Float64 scalar, got {other:?}"),
        }
    }

    /// `SequentialAvgAccumulator` (the plain, ungrouped path) and
    /// `SequentialAvgGroupsAccumulator` (a single group folded through the
    /// `GroupsAccumulator` path) must fold identical input in identical row
    /// order, so they must produce bit-identical output for every case,
    /// including NaN payloads and signed zero -- neither of which `==`
    /// distinguishes, which is why every comparison here goes through
    /// `to_bits()` (ADR-0022 decision 4, ADR-0825 decision 1).
    #[test]
    fn float_avg_grouped_matches_sequential_bit_for_bit() {
        let cases: Vec<Vec<Option<f64>>> = vec![
            vec![Some(1.0), Some(2.0), None, Some(3.5)],
            vec![Some(-0.0), Some(-0.0), Some(-0.0)],
            vec![Some(0.0), Some(-0.0)],
            vec![Some(-0.0), Some(0.0)],
            vec![
                Some(nan_with_payload(1)),
                Some(2.0),
                Some(nan_with_payload(0xdead)),
            ],
            vec![Some(f64::INFINITY), Some(f64::NEG_INFINITY)],
            vec![None, None],
            vec![],
        ];

        for values in cases {
            let array: ArrayRef = Arc::new(Float64Array::from(values.clone()));

            let mut plain = SequentialAvgAccumulator::new();
            plain.fold_values(&array).expect("fold must not fail");
            let plain_bits = float64_bits(plain.output());

            let mut grouped = SequentialAvgGroupsAccumulator::new();
            let group_indices = vec![0usize; values.len()];
            grouped
                .update_batch(&[Arc::clone(&array)], &group_indices, None, 1)
                .expect("update_batch must not fail");
            let grouped_array = grouped
                .evaluate(EmitTo::All)
                .expect("evaluate must not fail");
            let grouped_array = grouped_array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("evaluate must return a Float64Array");
            let grouped_bits = if grouped_array.is_valid(0) {
                Some(grouped_array.value(0).to_bits())
            } else {
                None
            };

            assert_eq!(
                plain_bits, grouped_bits,
                "plain and grouped float avg must be bit-identical for input {values:?}"
            );
        }
    }

    /// Three `-0.0` values folded with a zero seed would flip to `+0.0`
    /// (`0.0 + -0.0 == 0.0` under IEEE 754); seeding with the first value
    /// instead of zero keeps the running sum `-0.0`, and the division that
    /// follows keeps the result `-0.0`.
    #[test]
    fn float_avg_seeds_with_first_value_not_zero() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, -0.0, -0.0]));
        let mut acc = SequentialAvgAccumulator::new();
        acc.fold_values(&array).expect("fold must not fail");
        assert_eq!(
            float64_bits(acc.output()),
            Some((-0.0_f64).to_bits()),
            "a zero seed would have flipped an all -0.0 group to +0.0"
        );
    }

    #[test]
    fn checked_add_i128_overflow_is_a_typed_error_not_a_panic_or_wrap() {
        let err = checked_add_i128(i128::MAX, 1).expect_err("i128::MAX + 1 must overflow");
        assert!(
            matches!(err, DataFusionError::Internal(_)),
            "expected a typed internal error, got {err:?}"
        );
        let err = checked_add_i128(i128::MIN, -1)
            .expect_err("i128::MIN + -1 must overflow the other way");
        assert!(matches!(err, DataFusionError::Internal(_)));
    }

    #[test]
    fn checked_decimal128_value_overflow_is_a_typed_error_not_a_silent_wrap() {
        let err = checked_decimal128_value(DECIMAL128_MAX_UNSCALED + 1)
            .expect_err("one past the max unscaled magnitude must overflow");
        assert!(
            matches!(err, DataFusionError::Internal(_)),
            "expected a typed internal error, got {err:?}"
        );
        let err = checked_decimal128_value(-DECIMAL128_MAX_UNSCALED - 1)
            .expect_err("one past the min unscaled magnitude must overflow");
        assert!(matches!(err, DataFusionError::Internal(_)));

        checked_decimal128_value(DECIMAL128_MAX_UNSCALED).expect("the max magnitude itself fits");
        checked_decimal128_value(-DECIMAL128_MAX_UNSCALED).expect("the min magnitude itself fits");
    }

    /// `ExactIntegerAvgAccumulator::fold` surfaces the same typed overflow
    /// error `checked_add_i128` does, rather than swallowing it: an
    /// accumulator already holding a sum near `i128::MAX` (only reachable in
    /// practice via an adversarial number of merged partitions, but exercised
    /// directly here) must fail closed instead of wrapping.
    #[test]
    fn exact_integer_avg_accumulator_overflow_surfaces_typed_error() {
        let mut acc = ExactIntegerAvgAccumulator::new();
        acc.sum = i128::MAX;
        acc.count = 1;
        let err = acc
            .fold(1)
            .expect_err("folding onto i128::MAX must overflow");
        assert!(matches!(err, DataFusionError::Internal(_)));
    }

    /// The group and partition counts the grouped proptest fans over. Three
    /// partitions is the smallest count that distinguishes merge order from
    /// merge pairing; four groups keeps some groups empty in some partitions,
    /// which is the case that exercises the null partial-sum branch.
    const GROUPED_PROPTEST_GROUPS: usize = 4;
    const GROUPED_PROPTEST_PARTITIONS: usize = 3;

    /// Drain a grouped integer accumulator and return each group's result as
    /// raw bits, so the comparison distinguishes values `==` would not.
    fn grouped_integer_bits(acc: &mut ExactIntegerAvgGroupsAccumulator) -> Vec<Option<u64>> {
        let array = acc.evaluate(EmitTo::All).expect("evaluate must not fail");
        let array = array
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("evaluate must return a Float64Array");
        (0..array.len())
            .map(|index| {
                if array.is_valid(index) {
                    Some(array.value(index).to_bits())
                } else {
                    None
                }
            })
            .collect()
    }

    fn merge_state_into(acc: &mut ExactIntegerAvgAccumulator, state: &[ScalarValue]) {
        let arrays: Vec<ArrayRef> = state
            .iter()
            .map(|s| s.to_array().expect("state scalar must convert to an array"))
            .collect();
        acc.merge_batch(&arrays).expect("merge_batch must not fail");
    }

    proptest! {
        /// Exact-integer avg's result is a function of the input multiset
        /// only (ADR-0825 decision 2): splitting the same values across a
        /// different number of partitions, and merging those partitions'
        /// partial states in a different order, must never change the
        /// result, down to the bit, because `i128` addition is associative
        /// and exact and the partial-state pack/unpack is lossless within
        /// `Decimal128(38, 0)`'s range. This is the property the sibling
        /// `f64_partial_sum_merge_is_order_dependent` integration test shows
        /// Float64-argument avg does NOT have.
        #[test]
        fn integer_avg_partition_and_merge_order_never_changes_the_result(
            values in prop::collection::vec(-1_000_000_000_000i64..1_000_000_000_000i64, 0..64),
            assignment in prop::collection::vec(0usize..3, 0..64),
        ) {
            let mut assignment = assignment;
            assignment.resize(values.len(), 0);

            let mut reference = ExactIntegerAvgAccumulator::new();
            let all: ArrayRef = Arc::new(Int64Array::from(values.clone()));
            reference.fold_values(&all).expect("fold must not fail");
            let reference_bits = float64_bits(reference.output());

            let mut partitions: [Vec<i64>; 3] = [Vec::new(), Vec::new(), Vec::new()];
            for (&value, &part) in values.iter().zip(assignment.iter()) {
                partitions[part].push(value);
            }

            let mut states = Vec::with_capacity(3);
            for part in &partitions {
                let mut acc = ExactIntegerAvgAccumulator::new();
                let array: ArrayRef = Arc::new(Int64Array::from(part.clone()));
                acc.fold_values(&array).expect("fold must not fail");
                states.push(acc.state().expect("state must not fail"));
            }

            for order in [[0usize, 1, 2], [2, 1, 0], [1, 0, 2], [0, 2, 1]] {
                let mut merged = ExactIntegerAvgAccumulator::new();
                for idx in order {
                    merge_state_into(&mut merged, &states[idx]);
                }
                prop_assert_eq!(
                    reference_bits,
                    float64_bits(merged.output()),
                    "merge order {:?} produced a different result than the single-partition fold",
                    order
                );
            }
        }

        /// The same invariance, on the accumulator a real `GROUP BY` plan
        /// actually runs. `ExactIntegerAvgGroupsAccumulator` does not share
        /// `ExactIntegerAvgAccumulator`'s state code: its partial sums round
        /// trip through a `Decimal128(38, 0)` array rather than staying an
        /// `i128` in a scalar, and its merge reads them back per group. A
        /// pack, unpack, or group-alignment defect in that path would change
        /// the answer with the number of partitions while the ungrouped
        /// sibling above stayed green.
        #[test]
        fn integer_avg_grouped_partition_and_merge_order_never_changes_the_result(
            rows in prop::collection::vec(
                (-1_000_000_000_000i64..1_000_000_000_000i64, 0usize..GROUPED_PROPTEST_GROUPS, 0usize..GROUPED_PROPTEST_PARTITIONS),
                0..96,
            ),
        ) {
            let mut reference = ExactIntegerAvgGroupsAccumulator::new();
            let values: ArrayRef = Arc::new(Int64Array::from(
                rows.iter().map(|&(value, _, _)| value).collect::<Vec<_>>(),
            ));
            let group_indices: Vec<usize> = rows.iter().map(|&(_, group, _)| group).collect();
            reference
                .update_batch(&[values], &group_indices, None, GROUPED_PROPTEST_GROUPS)
                .expect("update_batch must not fail");
            let reference_bits = grouped_integer_bits(&mut reference);

            let mut states = Vec::with_capacity(GROUPED_PROPTEST_PARTITIONS);
            for partition in 0..GROUPED_PROPTEST_PARTITIONS {
                let mut acc = ExactIntegerAvgGroupsAccumulator::new();
                let taken: Vec<(i64, usize)> = rows
                    .iter()
                    .filter(|&&(_, _, part)| part == partition)
                    .map(|&(value, group, _)| (value, group))
                    .collect();
                let array: ArrayRef = Arc::new(Int64Array::from(
                    taken.iter().map(|&(value, _)| value).collect::<Vec<_>>(),
                ));
                let indices: Vec<usize> = taken.iter().map(|&(_, group)| group).collect();
                acc.update_batch(&[array], &indices, None, GROUPED_PROPTEST_GROUPS)
                    .expect("update_batch must not fail");
                states.push(acc.state(EmitTo::All).expect("state must not fail"));
            }

            let merge_indices: Vec<usize> = (0..GROUPED_PROPTEST_GROUPS).collect();
            for order in [[0usize, 1, 2], [2, 1, 0], [1, 0, 2], [0, 2, 1]] {
                let mut merged = ExactIntegerAvgGroupsAccumulator::new();
                for idx in order {
                    merged
                        .merge_batch(&states[idx], &merge_indices, None, GROUPED_PROPTEST_GROUPS)
                        .expect("merge_batch must not fail");
                }
                prop_assert_eq!(
                    reference_bits.clone(),
                    grouped_integer_bits(&mut merged),
                    "merge order {:?} produced different per-group results than the single-partition fold",
                    order
                );
            }
        }
    }

    /// `EmitTo::First` must drain exactly the first `n` groups and leave the
    /// remainder addressable at their shifted indices. An emit that returned
    /// the right values but left `sums` and `counts` misaligned would
    /// silently attribute later rows to the wrong group, which no
    /// whole-batch test can see.
    #[test]
    fn exact_integer_grouped_emit_first_drains_and_shifts() {
        let mut acc = ExactIntegerAvgGroupsAccumulator::new();
        let values: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20, 30, 40, 100, 200]));
        let group_indices = vec![0usize, 0, 1, 1, 2, 2];
        acc.update_batch(&[values], &group_indices, None, 3)
            .expect("update_batch must not fail");

        let emitted = acc
            .evaluate(EmitTo::First(2))
            .expect("evaluate must not fail");
        let emitted = emitted
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("evaluate must return a Float64Array");
        assert_eq!(
            emitted.len(),
            2,
            "EmitTo::First(2) must emit exactly 2 groups"
        );
        assert_eq!(emitted.value(0), 15.0);
        assert_eq!(emitted.value(1), 35.0);

        let rest = acc.evaluate(EmitTo::All).expect("evaluate must not fail");
        let rest = rest
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("evaluate must return a Float64Array");
        assert_eq!(rest.len(), 1, "one group must remain after draining two");
        assert_eq!(
            rest.value(0),
            150.0,
            "the surviving group's state must shift down, not be re-read at its old index"
        );
    }
}
