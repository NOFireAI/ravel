//! Total-order MIN/MAX (ADR-0023).
//!
//! DataFusion 54.1.0 ships two disagreeing MIN/MAX implementations for
//! floating point. The ungrouped path (`MinAccumulator`/`MaxAccumulator` over
//! arrow's min/max kernels) compares with `f64::total_cmp`, a true total
//! order. The grouped path (`PrimitiveGroupsAccumulator`) folds `partial_cmp`
//! from an `f64::MAX`/`f64::MIN` seed, which is not a total order and is wrong
//! three ways: a NaN in a group poisons every later comparison, `-0.0`/`0.0`
//! compare `Equal` so arrival order decides the winner, and an all-infinite
//! group never displaces the seed so MIN/MAX returns `f64::MAX`/`f64::MIN`
//! instead of the actual value.
//!
//! This module replaces the built-in `min`/`max` with a UDAF pair that uses
//! `f64::total_cmp` for floating-point input, grouped and ungrouped alike, so
//! the result is a function of the input multiset and never of arrival order.
//! Non-float input types delegate entirely to the wrapped built-in
//! (`min_udaf()`/`max_udaf()`): `partial_cmp` is already total there and the
//! seed cannot surface a wrong extreme, so upstream's vectorized grouped path
//! is kept (for example `min(ts)` over a Timestamp column).
//!
//! The accumulator is a plain [`Accumulator`], not a custom
//! `GroupsAccumulator`: [`TotalOrderMinMax::groups_accumulator_supported`]
//! returns false for float input, so grouped execution runs one accumulator
//! per group behind `GroupsAccumulatorAdapter`. State is `Option<f64>`;
//! absence means "no rows seen", so there is no seed value to leak and the
//! all-infinite-group bug is impossible by construction. The fold is
//! associative and commutative, so partial/final aggregation splits stay
//! exact. See ADR-0023 decisions 1-4.

use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, GroupsAccumulator, ReversedUDAF,
    SetMonotonicity, Signature, StatisticsArgs,
};
use datafusion::scalar::ScalarValue;

/// The total-order MIN UDAF, registered under the name `min` to displace
/// DataFusion's built-in.
pub fn total_order_min_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(TotalOrderMinMax::new(Extreme::Min))
}

/// The total-order MAX UDAF, registered under the name `max` to displace
/// DataFusion's built-in.
pub fn total_order_max_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(TotalOrderMinMax::new(Extreme::Max))
}

/// Which extreme this UDAF computes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, std::hash::Hash)]
enum Extreme {
    Min,
    Max,
}

impl Extreme {
    /// The ordering that makes `candidate` replace the running extreme: `Less`
    /// for MIN, `Greater` for MAX. A tie keeps the incumbent, matching the
    /// ungrouped reference fold (`reduce` keeps its accumulator on non-strict
    /// comparisons).
    fn replaces(self, candidate: f64, current: f64) -> bool {
        let want = match self {
            Extreme::Min => Ordering::Less,
            Extreme::Max => Ordering::Greater,
        };
        candidate.total_cmp(&current) == want
    }
}

/// A MIN/MAX UDAF that owns floating-point extreme semantics and delegates
/// everything else to the wrapped built-in.
///
/// Every `AggregateUDFImpl` method the built-in `Min`/`Max` overrides is
/// forwarded to `inner` (name, signature, return type, coercion, state
/// fields, statistics shortcuts, and so on) so the replacement is
/// behaviourally identical outside of float extreme computation. Only
/// [`Self::accumulator`], [`Self::groups_accumulator_supported`], and
/// [`Self::create_groups_accumulator`] diverge, and only for float input.
#[derive(Debug)]
struct TotalOrderMinMax {
    /// The wrapped built-in (`min_udaf()` or `max_udaf()`), used for
    /// delegation and for non-float input types.
    inner: Arc<dyn AggregateUDFImpl>,
    extreme: Extreme,
}

impl TotalOrderMinMax {
    fn new(extreme: Extreme) -> Self {
        let inner = match extreme {
            Extreme::Min => min_udaf().inner().clone(),
            Extreme::Max => max_udaf().inner().clone(),
        };
        Self { inner, extreme }
    }
}

// `AggregateUDFImpl` requires `DynEq`/`DynHash`; identity is fully determined
// by which extreme this instance computes (the wrapped `inner` is a pure
// function of `extreme`).
impl PartialEq for TotalOrderMinMax {
    fn eq(&self, other: &Self) -> bool {
        self.extreme == other.extreme
    }
}

impl Eq for TotalOrderMinMax {}

impl std::hash::Hash for TotalOrderMinMax {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.extreme.hash(state);
    }
}

/// Whether `data_type` gets the total-order accumulator. Only floating-point
/// types can disagree between the total order and `partial_cmp`; every other
/// type delegates.
///
/// `pub(crate)` so `executor.rs`'s ADR-0094 exact-typed classification reuses
/// the exact float predicate that drives this UDAF's accumulator choice: the
/// same resolved-type test decides both "needs the total-order accumulator"
/// and "must not repartition its final aggregation".
pub(crate) fn is_float(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

impl AggregateUDFImpl for TotalOrderMinMax {
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
        self.inner.state_fields(args)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        #[cfg(test)]
        routing_probe::note_accumulator();
        let return_type = acc_args.return_field.data_type();
        if is_float(return_type) {
            Ok(Box::new(TotalOrderAccumulator::new(
                self.extreme,
                return_type.clone(),
            )))
        } else {
            self.inner.accumulator(acc_args)
        }
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        if is_float(args.return_field.data_type()) {
            // Run one plain accumulator per group behind
            // `GroupsAccumulatorAdapter`; there is no total-order vectorized
            // groups accumulator for v1 (ADR-0023 decision 3).
            false
        } else {
            self.inner.groups_accumulator_supported(args)
        }
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        // Only reached for non-float input, where
        // `groups_accumulator_supported` delegated `true`.
        self.inner.create_groups_accumulator(args)
    }

    // Delegate the moving-window (sliding) accumulator to the wrapped built-in
    // with no float guard, unlike `accumulator` above. Upstream's
    // `MovingMin`/`MovingMax` order candidates through `ScalarValue`'s
    // `PartialOrd`, which is `f64::total_cmp` for float types -- the same total
    // order ADR-0023 mandates -- so the sliding path is total-order by
    // construction for float input and needs no replacement here.
    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        #[cfg(test)]
        routing_probe::note_sliding();
        self.inner.create_sliding_accumulator(args)
    }

    fn is_descending(&self) -> Option<bool> {
        self.inner.is_descending()
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        self.inner.order_sensitivity()
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        self.inner.reverse_expr()
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        self.inner.value_from_stats(statistics_args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }

    fn set_monotonicity(&self, data_type: &DataType) -> SetMonotonicity {
        self.inner.set_monotonicity(data_type)
    }
}

/// The plain per-group accumulator. Internal state is `Option<f64>`; the
/// winning extreme is emitted in the aggregate's return type (Float64,
/// Float32, or Float16) on `evaluate`/`state`.
#[derive(Debug)]
struct TotalOrderAccumulator {
    extreme: Extreme,
    return_type: DataType,
    /// The running extreme, or `None` until the first value is seen.
    value: Option<f64>,
}

impl TotalOrderAccumulator {
    fn new(extreme: Extreme, return_type: DataType) -> Self {
        Self {
            extreme,
            return_type,
            value: None,
        }
    }

    /// Fold one candidate into the running extreme under the total order.
    fn fold(&mut self, candidate: f64) {
        self.value = Some(match self.value {
            None => candidate,
            Some(current) if self.extreme.replaces(candidate, current) => candidate,
            Some(current) => current,
        });
    }

    /// Fold every non-null value of `array` after widening it to Float64.
    /// Widening f32/f16 to f64 is exact and order-preserving, so the total
    /// order is unchanged; the winning value round-trips back to its own type
    /// exactly on output.
    fn fold_array(&mut self, array: &ArrayRef) -> DFResult<()> {
        let float = if array.data_type() == &DataType::Float64 {
            Arc::clone(array)
        } else {
            cast(array, &DataType::Float64)?
        };
        let float = float
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "total-order min/max: cast to Float64 did not yield a Float64Array".to_string(),
                )
            })?;
        for i in 0..float.len() {
            if float.is_valid(i) {
                self.fold(float.value(i));
            }
        }
        Ok(())
    }

    /// The running extreme as a `ScalarValue` in the aggregate's return type.
    /// Float64 is emitted directly so the NaN payload bits are preserved
    /// verbatim; f32/f16 go through `cast_to`, which round-trips the value
    /// exactly because it originated in that narrower type.
    fn output(&self) -> DFResult<ScalarValue> {
        let scalar = ScalarValue::Float64(self.value);
        if self.return_type == DataType::Float64 {
            Ok(scalar)
        } else {
            scalar.cast_to(&self.return_type)
        }
    }
}

impl Accumulator for TotalOrderAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        self.fold_array(&values[0])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        self.fold_array(&states[0])
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![self.output()?])
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        self.output()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// Test-only instrumentation that records which accumulator constructor
/// DataFusion calls for a given window frame shape, so the sliding-path
/// float-safety guarantee rests on a measured routing fact rather than an
/// assumption about DataFusion internals. Counters are thread-local: the
/// routing test drives its plan on a current-thread runtime, so every
/// constructor call lands on the test thread and parallel tests on other
/// threads cannot pollute the counts.
#[cfg(test)]
mod routing_probe {
    use std::cell::Cell;

    thread_local! {
        static ACCUMULATOR_CALLS: Cell<usize> = const { Cell::new(0) };
        static SLIDING_CALLS: Cell<usize> = const { Cell::new(0) };
    }

    pub(super) fn note_accumulator() {
        ACCUMULATOR_CALLS.with(|c| c.set(c.get() + 1));
    }

    pub(super) fn note_sliding() {
        SLIDING_CALLS.with(|c| c.set(c.get() + 1));
    }

    pub(super) fn reset() {
        ACCUMULATOR_CALLS.with(|c| c.set(0));
        SLIDING_CALLS.with(|c| c.set(0));
    }

    pub(super) fn accumulator_calls() -> usize {
        ACCUMULATOR_CALLS.with(Cell::get)
    }

    pub(super) fn sliding_calls() -> usize {
        SLIDING_CALLS.with(Cell::get)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{Float64Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::*;

    /// Establish, rather than assume, which window frame shapes DataFusion
    /// routes to `create_sliding_accumulator` versus `accumulator`. This is the
    /// premise the acceptance test
    /// (`tests/sliding_frame_total_order.rs`) rests on: a *moving* frame runs
    /// the sliding accumulator (upstream `MovingMin`/`MovingMax`), while an
    /// UNBOUNDED PRECEDING frame runs Ravel's own `TotalOrderAccumulator`, so a
    /// test over only the unbounded shape would never exercise the delegation
    /// under test.
    ///
    /// The routing is observed directly by counting each constructor call on the
    /// registered total-order `min` UDAF (`routing_probe`), on a current-thread
    /// runtime so the counts are deterministic.
    #[test]
    fn moving_frame_routes_to_the_sliding_accumulator() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("t", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
            ],
        )
        .expect("batch");

        // A current-thread runtime keeps all plan execution -- and thus every
        // accumulator construction -- on this thread, where the thread-local
        // probe counters live.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");

        let drain = |sql: &str| {
            let ctx =
                SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
            // Displace the built-in `min` with the total-order UDAF under test,
            // exactly as `build_session` does.
            ctx.register_udaf(total_order_min_udaf());
            let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch.clone()]])
                .expect("memtable");
            ctx.register_table("x", Arc::new(table)).expect("register");
            let batches = rt.block_on(async {
                ctx.sql(sql)
                    .await
                    .expect("plan window query")
                    .collect()
                    .await
                    .expect("execute window query")
            });
            // Force full execution.
            assert!(batches.iter().map(|b| b.num_rows()).sum::<usize>() > 0);
        };

        // An UNBOUNDED PRECEDING frame runs Ravel's own accumulator, never the
        // sliding one.
        routing_probe::reset();
        drain("SELECT min(v) OVER (ORDER BY t) AS m FROM x");
        assert!(
            routing_probe::accumulator_calls() > 0,
            "the unbounded frame must construct Ravel's own accumulator"
        );
        assert_eq!(
            routing_probe::sliding_calls(),
            0,
            "the unbounded frame must NOT construct the sliding accumulator"
        );

        // A moving `ROWS BETWEEN n PRECEDING AND CURRENT ROW` frame routes to
        // the sliding accumulator (upstream `MovingMin`), the path the
        // acceptance test's float-safety guarantee depends on.
        routing_probe::reset();
        drain(
            "SELECT min(v) OVER (ORDER BY t ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS m FROM x",
        );
        assert!(
            routing_probe::sliding_calls() > 0,
            "a moving frame must construct the sliding accumulator"
        );
    }
}
