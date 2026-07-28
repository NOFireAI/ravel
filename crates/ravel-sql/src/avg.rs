//! Sequential-fold AVG/MEAN (ADR-0022 decisions 3, 4).
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
//! This module replaces the built-in `avg`/`mean` with a UDAF whose numerator
//! is a plain, sequential IEEE f64 fold over its own non-null input values, in
//! the deterministic `(series_id, ts)` order v1 guarantees by executing
//! aggregation single-partitioned above the sort-preserving merge. The fold is
//! seeded with the first value rather than a zero seed, so a group of all
//! `-0.0` values folds to `-0.0` (a `0.0` seed would flip it to `+0.0`). The
//! numerator is divided by the non-null row count in one correctly rounded
//! IEEE division; empty or all-NULL input yields NULL, never NaN or infinity.
//!
//! The summation is naive, not Kahan (ADR-0022 decision 3): the differential
//! gate compares against a reference running this identical algorithm, so
//! compensation buys no exactness, and this fold is internal to this UDAF --
//! it does not touch, replace, or reuse the public `sum` aggregate, whose bits
//! stay exactly as DataFusion's built-in produces them (ADR-0024, undecided,
//! tracks whether `sum` should ever change).
//!
//! Only the `avg`/`mean` names and only Float64 output are owned here. Metadata
//! (name, aliases, signature, return type, coercion) delegates to the wrapped
//! built-in so integer input still coerces to Float64 exactly as before, and
//! any non-Float64 return type (the decimal and duration paths, unreachable on
//! the v1 `samples` surface, whose only numeric column is Float64) delegates
//! its accumulator to the built-in untouched.
//!
//! The accumulator is a plain [`Accumulator`], never a `GroupsAccumulator`:
//! [`SequentialAvg::groups_accumulator_supported`] returns false, so grouped
//! execution runs one accumulator per group behind `GroupsAccumulatorAdapter`,
//! folding each group's values in row order. State is `(sum, count)`; a merge
//! of partial states adds sums with the same plain IEEE addition and adds
//! counts, so a two-phase single-partition plan (one partial state per group)
//! reconstitutes the same bits as a single-phase one. See ADR-0022.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, ReversedUDAF, Signature,
};
use datafusion::scalar::ScalarValue;

/// The sequential-fold AVG UDAF, registered under `avg` (and its `mean` alias)
/// to displace DataFusion's built-in whose lane-reduced batch sum is
/// unpinnable (ADR-0022 decision 4).
pub fn sequential_avg_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(SequentialAvg::new())
}

/// An AVG/MEAN UDAF that owns Float64 numerator semantics (a naive sequential
/// f64 fold divided by the non-null count) and delegates everything else to
/// the wrapped built-in.
///
/// Metadata methods forward to `inner` so the replacement is behaviourally
/// identical outside of the Float64 numerator computation. Only
/// [`Self::accumulator`] and [`Self::state_fields`] diverge, and only for
/// Float64 output; [`Self::groups_accumulator_supported`] returns false so the
/// plain accumulator runs per group.
#[derive(Debug)]
struct SequentialAvg {
    /// The wrapped built-in `avg` (`avg_udaf()`), used for metadata delegation
    /// and for non-Float64 output types.
    inner: Arc<dyn AggregateUDFImpl>,
}

impl SequentialAvg {
    fn new() -> Self {
        Self {
            inner: avg_udaf().inner().clone(),
        }
    }
}

// `AggregateUDFImpl` requires `DynEq`/`DynHash`; this impl carries no state of
// its own (the wrapped `inner` is a pure function of the built-in `avg`), so
// all instances are equal.
impl PartialEq for SequentialAvg {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for SequentialAvg {}

impl std::hash::Hash for SequentialAvg {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

/// Whether `data_type` gets the sequential-fold accumulator. Only Float64
/// output is owned here; decimal and duration outputs (unreachable on the v1
/// `samples` surface) delegate to the built-in.
fn is_owned(data_type: &DataType) -> bool {
    data_type == &DataType::Float64
}

impl AggregateUDFImpl for SequentialAvg {
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
        if is_owned(args.return_field.data_type()) {
            // Matches [`SequentialAvgAccumulator`]'s `state`/`merge_batch`: a
            // nullable Float64 running sum (null until the first row) and an
            // Int64 non-null count. Names are namespaced by the aggregate
            // expression name so a two-phase plan's partial and final state
            // schemas line up.
            Ok(vec![
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
            ])
        } else {
            self.inner.state_fields(args)
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        if is_owned(acc_args.return_field.data_type()) {
            Ok(Box::new(SequentialAvgAccumulator::new()))
        } else {
            self.inner.accumulator(acc_args)
        }
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        // Run one plain accumulator per group behind `GroupsAccumulatorAdapter`
        // so every group folds sequentially in row order; there is no
        // sequential vectorized groups accumulator for v1 (ADR-0022 decision
        // 3). The built-in's own groups accumulator is exactly the
        // lane-parallel path this UDAF exists to replace.
        false
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_expr()
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

/// The plain per-group accumulator. `sum` is the running numerator, `None`
/// until the first non-null value is seen; `count` is the number of non-null
/// values folded. The result is `sum / count` in one IEEE division, or NULL
/// when `count` is zero.
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
    /// The input is already Float64 on the v1 surface (integer input is
    /// coerced up before it reaches the accumulator); the cast is a no-op
    /// clone there and exact for the integer case regardless.
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
