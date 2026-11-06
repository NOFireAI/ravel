//! Exact `SUM(x + c)` for an integer `x` and an integer literal `c` (issue
//! #3845, ADR-0022's integer sum amendment).
//!
//! The total of `x + c` over a group's non-null rows is `sum(x) + c *
//! count(x)`. [`ExactSumPlusLiteral`] rewrites each such `sum` call of an
//! `Aggregate` into an `exact_sum_count` aggregate over `x`, which keeps the
//! exact `i128` sum and the non-null count of `x`, and a projection above the
//! aggregate that evaluates `exact_sum_plus` on it: `sum + c * count` in
//! `i128`, converted to the call's result type once. A total that does not fit
//! fails with [`SqlError::IntegerSumOverflow`], and a group with no non-null
//! row is NULL, as for the checked `sum` in [`crate::sum`]. A row whose own
//! `x + c` would leave the column type does not matter; only the total does.
//! This is the computation `crate::metadata_agg` answers `SUM(col + k)` with
//! from statistics, so the scan and the statistics answer agree.
//!
//! Every call over the same `x` with the same `FILTER` shares one
//! `exact_sum_count`, so ClickBench q30's ninety `SUM(x + k)` terms run one
//! accumulator. A plain `SUM(x)` beside such a call joins it with `c = 0`.
//! `SUM(x - c)` takes the same form with `-c`, and `SUM(c + x)` the same as
//! `SUM(x + c)`; a per-call `FILTER` is kept on the shared aggregate.
//!
//! `SUM(DISTINCT x + c)`, `SUM(c - x)` and a window `SUM(x + c) OVER (...)`
//! are not rewritten. They keep the checked `sum` over per-row `x + c`, which
//! is DataFusion's wrapping scalar arithmetic (#3847). DataFusion's own
//! rewrite to `SUM(x) + c * COUNT(x)`, whose outer `+` and `*` also wrap, is
//! never applied to an integer literal (`crate::sum`).

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Decimal128Array, Int64Array, PrimitiveArray,
    StructArray,
};
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Int64Type, UInt64Type};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DFSchema};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::expr::{AggregateFunction, AggregateFunctionParams};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, Aggregate, AggregateUDF, AggregateUDFImpl, BinaryExpr, ColumnarValue, EmitTo,
    Expr, ExprSchemable, GroupsAccumulator, LogicalPlan, Operator, Projection, ScalarFunctionArgs,
    ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::optimizer::optimizer::{ApplyOrder, Optimizer};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::scalar::ScalarValue;

#[cfg(doc)]
use crate::error::SqlError;
use crate::sum::{SumKind, as_decimal128, as_primitive, row_is_filtered, sum_kind};

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const EXACT_SUM_PLUS_RULE: &str = "exact_sum_plus_literal";

/// The name of the shared sum-and-count aggregate. `crate::metadata_agg`
/// answers it from statistics and `crate::memory` sizes its groups by it.
pub(crate) const EXACT_SUM_COUNT: &str = "exact_sum_count";

/// Per-group bytes of the `exact_sum_count` groups accumulator: an `i128` sum
/// and an `i64` count.
pub(crate) const EXACT_SUM_COUNT_GROUP_WIDTHS: [usize; 2] = [16, 8];

/// The output type of `exact_sum_count`: the exact sum as `Decimal128(38, 0)`
/// and the non-null count.
pub(crate) fn sum_count_type() -> DataType {
    DataType::Struct(Fields::from(vec![
        Field::new("sum", DataType::Decimal128(38, 0), false),
        Field::new("count", DataType::Int64, false),
    ]))
}

/// The name of DataFusion's common subexpression rule.
const COMMON_SUBEXPR_ELIMINATE: &str = "common_sub_expression_eliminate";

/// DataFusion's default logical optimizer rules with [`ExactSumPlusLiteral`]
/// inserted before common subexpression elimination, which would otherwise
/// move an `x + c` that two calls share into a projected column the rule
/// cannot see through.
pub(crate) fn optimizer_rules() -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let mut rules = Optimizer::new().rules;
    let at = rules
        .iter()
        .position(|rule| rule.name() == COMMON_SUBEXPR_ELIMINATE)
        .unwrap_or(rules.len());
    rules.insert(at, Arc::new(ExactSumPlusLiteral));
    rules
}

/// Rewrites `SUM(x + c)` into the shared exact form; see the module docs.
#[derive(Debug, Default)]
pub struct ExactSumPlusLiteral;

impl OptimizerRule for ExactSumPlusLiteral {
    fn name(&self) -> &str {
        EXACT_SUM_PLUS_RULE
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(aggregate) = plan else {
            return Ok(Transformed::no(plan));
        };
        match rewrite_aggregate(&aggregate)? {
            Some(rewritten) => Ok(Transformed::yes(rewritten)),
            None => Ok(Transformed::no(LogicalPlan::Aggregate(aggregate))),
        }
    }
}

/// A `sum` call the rewrite can express over `exact_sum_count`.
struct SumTerm {
    /// `exact_sum_count(x)` with the call's filter.
    call: Expr,
    /// `c`, negated for `x - c`; zero for a plain `SUM(x)`.
    addend: i128,
    kind: SumKind,
    /// Whether the call is `SUM(x ± c)` rather than a plain `SUM(x)`.
    linear: bool,
}

/// `aggregate` as a projection of `exact_sum_plus` over an aggregate whose
/// `exact_sum_count` calls are shared, or `None` when it holds no
/// `SUM(x ± c)` call.
fn rewrite_aggregate(aggregate: &Aggregate) -> DFResult<Option<LogicalPlan>> {
    let input = aggregate.input.schema();
    let terms = aggregate
        .aggr_expr
        .iter()
        .map(|expr| sum_term(expr, input))
        .collect::<DFResult<Vec<_>>>()?;
    let linear_calls: Vec<&Expr> = terms
        .iter()
        .flatten()
        .filter(|term| term.linear)
        .map(|term| &term.call)
        .collect();
    if linear_calls.is_empty() {
        return Ok(None);
    }

    // Each original call's position in the deduplicated aggregate, and the
    // addend and kind of the `exact_sum_plus` that reads it, if any.
    let mut aggr_expr: Vec<Expr> = Vec::new();
    let mut slots: Vec<(usize, Option<(i128, SumKind)>)> = Vec::new();
    for (expr, term) in aggregate.aggr_expr.iter().zip(&terms) {
        let (call, sum) = match term {
            Some(term) if term.linear || linear_calls.contains(&&term.call) => {
                (&term.call, Some((term.addend, term.kind)))
            }
            _ => (expr, None),
        };
        let index = match aggr_expr.iter().position(|existing| existing == call) {
            Some(index) => index,
            None => {
                aggr_expr.push(call.clone());
                aggr_expr.len() - 1
            }
        };
        slots.push((index, sum));
    }

    let rewritten = Aggregate::try_new(
        Arc::clone(&aggregate.input),
        aggregate.group_expr.clone(),
        aggr_expr,
    )?;
    let group_fields = aggregate.schema.fields().len() - aggregate.aggr_expr.len();
    let rewritten_schema = Arc::clone(&rewritten.schema);
    let column = |index: usize| {
        let (qualifier, field) = rewritten_schema.qualified_field(index);
        Expr::Column(Column::new(qualifier.cloned(), field.name()))
    };
    let mut exprs: Vec<Expr> = (0..group_fields).map(column).collect();
    for (position, (index, sum)) in slots.into_iter().enumerate() {
        let value = column(group_fields + index);
        let value = match sum {
            Some((addend, kind)) => {
                exact_sum_plus_udf(kind).call(vec![value, addend_literal(addend)])
            }
            None => value,
        };
        let (qualifier, field) = aggregate.schema.qualified_field(group_fields + position);
        exprs.push(value.alias_qualified(qualifier.cloned(), field.name()));
    }
    Projection::try_new_with_schema(
        exprs,
        Arc::new(LogicalPlan::Aggregate(rewritten)),
        Arc::clone(&aggregate.schema),
    )
    .map(LogicalPlan::Projection)
    .map(Some)
}

/// `expr`, one `aggr_expr` entry, as a [`SumTerm`]: a non-distinct, unordered
/// integer `sum` over `x`, `x + c`, `c + x` or `x - c`, where `x` is a
/// non-volatile expression of the call's own integer argument type and `c` a
/// non-null integer literal.
fn sum_term(expr: &Expr, input: &DFSchema) -> DFResult<Option<SumTerm>> {
    let inner = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    let Expr::AggregateFunction(function) = inner else {
        return Ok(None);
    };
    let AggregateFunctionParams {
        args,
        distinct,
        filter,
        order_by,
        null_treatment,
    } = &function.params;
    if !function.func.name().eq_ignore_ascii_case("sum")
        || *distinct
        || !order_by.is_empty()
        || null_treatment.is_some()
    {
        return Ok(None);
    }
    let [arg] = args.as_slice() else {
        return Ok(None);
    };
    let Some(kind) = sum_kind(&inner.get_type(input)?) else {
        return Ok(None);
    };
    let arg_type = arg.get_type(input)?;
    if integer_kind(&arg_type) != Some(kind) {
        return Ok(None);
    }
    let (operand, addend, linear) = match arg {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (operand, literal, negate) = match (left.as_ref(), op, right.as_ref()) {
                (operand, Operator::Plus, Expr::Literal(literal, _))
                | (Expr::Literal(literal, _), Operator::Plus, operand) => (operand, literal, false),
                (operand, Operator::Minus, Expr::Literal(literal, _)) => (operand, literal, true),
                _ => return Ok(None),
            };
            let Some(c) = integer_value(literal) else {
                return Ok(None);
            };
            let operand_type = operand.get_type(input)?;
            if value_type(&operand_type) != &arg_type {
                return Ok(None);
            }
            (operand, if negate { -c } else { c }, true)
        }
        operand => (operand, 0, false),
    };
    if operand.is_volatile() {
        return Ok(None);
    }
    let call = Expr::AggregateFunction(AggregateFunction::new_udf(
        Arc::new(exact_sum_count_udaf()),
        vec![operand.clone()],
        false,
        filter.clone(),
        Vec::new(),
        None,
    ));
    Ok(Some(SumTerm {
        call,
        addend,
        kind,
        linear,
    }))
}

/// The value type of a dictionary, or `data_type` itself.
fn value_type(data_type: &DataType) -> &DataType {
    match data_type {
        DataType::Dictionary(_, value) => value,
        other => other,
    }
}

/// The checked sum kind of an integer (or integer dictionary) input type.
fn integer_kind(data_type: &DataType) -> Option<SumKind> {
    match value_type(data_type) {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            Some(SumKind::Signed)
        }
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            Some(SumKind::Unsigned)
        }
        _ => None,
    }
}

/// A non-null integer scalar's value, also the `Decimal128(_, 0)` form
/// [`addend_literal`] writes for an addend outside `i64`.
fn integer_value(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Int8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(i128::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(i128::from(*v)),
        _ => None,
    }
}

/// `addend` as a literal: `Int64` when it fits, else `Decimal128(38, 0)`.
fn addend_literal(addend: i128) -> Expr {
    let value = match i64::try_from(addend) {
        Ok(addend) => ScalarValue::Int64(Some(addend)),
        Err(_) => ScalarValue::Decimal128(Some(addend), 38, 0),
    };
    Expr::Literal(value, None)
}

fn addend_value(value: &ScalarValue) -> Option<i128> {
    match value {
        ScalarValue::Decimal128(Some(v), _, 0) => Some(*v),
        other => integer_value(other),
    }
}

fn exact_sum_count_udaf() -> AggregateUDF {
    AggregateUDF::new_from_impl(ExactSumCount::new())
}

fn exact_sum_plus_udf(kind: SumKind) -> Arc<ScalarUDF> {
    Arc::new(ScalarUDF::new_from_impl(ExactSumPlus::new(kind)))
}

/// The input kind of an `exact_sum_count` call, from its argument field.
fn input_kind(fields: &[FieldRef]) -> DFResult<SumKind> {
    fields
        .first()
        .and_then(|field| integer_kind(field.data_type()))
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "{EXACT_SUM_COUNT}: the argument is not an integer column"
            ))
        })
}

/// The exact `i128` sum and the non-null count of an integer argument.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ExactSumCount {
    signature: Signature,
}

impl ExactSumCount {
    fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for ExactSumCount {
    fn name(&self) -> &str {
        EXACT_SUM_COUNT
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(sum_count_type())
    }

    fn state_fields(&self, args: StateFieldsArgs) -> DFResult<Vec<FieldRef>> {
        Ok(vec![
            Arc::new(Field::new(
                format!("{}[exact_sum]", args.name),
                DataType::Decimal128(38, 0),
                false,
            )),
            Arc::new(Field::new(
                format!("{}[exact_count]", args.name),
                DataType::Int64,
                false,
            )),
        ])
    }

    fn accumulator(&self, args: AccumulatorArgs) -> DFResult<Box<dyn Accumulator>> {
        Ok(Box::new(SumCountAccumulator {
            kind: input_kind(args.expr_fields)?,
            sum: 0,
            count: 0,
        }))
    }

    fn groups_accumulator_supported(&self, _args: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> DFResult<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(SumCountGroupsAccumulator {
            kind: input_kind(args.expr_fields)?,
            sums: Vec::new(),
            counts: Vec::new(),
        }))
    }
}

fn count_overflow() -> DataFusionError {
    DataFusionError::Internal(format!("{EXACT_SUM_COUNT}: the row count overflowed i64"))
}

/// Add every non-null value of `array` to `sum`, checked, and return how many
/// there were.
fn add_values(kind: SumKind, array: &ArrayRef, sum: &mut i128) -> DFResult<i64> {
    fn add<T>(kind: SumKind, values: &PrimitiveArray<T>, sum: &mut i128) -> DFResult<i64>
    where
        T: datafusion::arrow::datatypes::ArrowPrimitiveType,
        T::Native: Into<i128>,
    {
        let mut total = *sum;
        if values.null_count() == 0 {
            for &value in values.values().iter() {
                total = kind.checked_add(total, value.into())?;
            }
        } else {
            for value in values.iter().flatten() {
                total = kind.checked_add(total, value.into())?;
            }
        }
        *sum = total;
        i64::try_from(values.len() - values.null_count()).map_err(|_| count_overflow())
    }
    match kind {
        SumKind::Signed => add(kind, &as_primitive::<Int64Type>(array)?, sum),
        SumKind::Unsigned => add(kind, &as_primitive::<UInt64Type>(array)?, sum),
    }
}

fn as_count(array: &ArrayRef) -> DFResult<&Int64Array> {
    array.as_primitive_opt::<Int64Type>().ok_or_else(|| {
        DataFusionError::Internal(format!("{EXACT_SUM_COUNT}: the count is not Int64"))
    })
}

/// `sums` and `counts` as the `exact_sum_count` output, or the overflow error
/// when a sum is outside `Decimal128(38, 0)`.
fn sum_count_array(kind: SumKind, sums: Vec<i128>, counts: Vec<i64>) -> DFResult<StructArray> {
    for &sum in &sums {
        kind.state_value(sum)?;
    }
    let DataType::Struct(fields) = sum_count_type() else {
        return Err(DataFusionError::Internal(format!(
            "{EXACT_SUM_COUNT}: the output type is not a struct"
        )));
    };
    let sums =
        Decimal128Array::new(ScalarBuffer::from(sums), None).with_precision_and_scale(38, 0)?;
    let counts = Int64Array::new(ScalarBuffer::from(counts), None);
    Ok(StructArray::try_new(
        fields,
        vec![Arc::new(sums) as ArrayRef, Arc::new(counts)],
        None,
    )?)
}

/// The plain accumulator of `exact_sum_count`.
#[derive(Debug)]
struct SumCountAccumulator {
    kind: SumKind,
    sum: i128,
    count: i64,
}

impl Accumulator for SumCountAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DFResult<()> {
        let added = add_values(self.kind, &values[0], &mut self.sum)?;
        self.count = self.count.checked_add(added).ok_or_else(count_overflow)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DFResult<()> {
        let sums = as_decimal128(&states[0])?;
        let counts = as_count(&states[1])?;
        for row in 0..sums.len() {
            self.sum = self.kind.checked_add(self.sum, sums.value(row))?;
            self.count = self
                .count
                .checked_add(counts.value(row))
                .ok_or_else(count_overflow)?;
        }
        Ok(())
    }

    fn state(&mut self) -> DFResult<Vec<ScalarValue>> {
        Ok(vec![
            ScalarValue::Decimal128(Some(self.kind.state_value(self.sum)?), 38, 0),
            ScalarValue::Int64(Some(self.count)),
        ])
    }

    fn evaluate(&mut self) -> DFResult<ScalarValue> {
        let array = sum_count_array(self.kind, vec![self.sum], vec![self.count])?;
        Ok(ScalarValue::Struct(Arc::new(array)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// The flat per-group accumulator of `exact_sum_count`: `sums[g]` and
/// `counts[g]` are group `g`'s exact sum and non-null count.
#[derive(Debug)]
struct SumCountGroupsAccumulator {
    kind: SumKind,
    sums: Vec<i128>,
    counts: Vec<i64>,
}

impl SumCountGroupsAccumulator {
    fn resize(&mut self, total_num_groups: usize) {
        self.sums.resize(total_num_groups, 0);
        self.counts.resize(total_num_groups, 0);
    }

    fn add(&mut self, group: usize, value: i128) -> DFResult<()> {
        let sum = &mut self.sums[group];
        *sum = self.kind.checked_add(*sum, value)?;
        let count = &mut self.counts[group];
        *count = count.checked_add(1).ok_or_else(count_overflow)?;
        Ok(())
    }

    fn update_primitive<T>(
        &mut self,
        values: &PrimitiveArray<T>,
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
    ) -> DFResult<()>
    where
        T: datafusion::arrow::datatypes::ArrowPrimitiveType,
        T::Native: Into<i128>,
    {
        if values.null_count() == 0 && opt_filter.is_none() {
            for (&group, &value) in group_indices.iter().zip(values.values().iter()) {
                self.add(group, value.into())?;
            }
            return Ok(());
        }
        for (row, &group) in group_indices.iter().enumerate() {
            if row_is_filtered(opt_filter, row) || values.is_null(row) {
                continue;
            }
            self.add(group, values.value(row).into())?;
        }
        Ok(())
    }
}

impl GroupsAccumulator for SumCountGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> DFResult<()> {
        self.resize(total_num_groups);
        match self.kind {
            SumKind::Signed => {
                let values = as_primitive::<Int64Type>(&values[0])?;
                self.update_primitive(&values, group_indices, opt_filter)
            }
            SumKind::Unsigned => {
                let values = as_primitive::<UInt64Type>(&values[0])?;
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
        let sums = as_decimal128(&values[0])?;
        let counts = as_count(&values[1])?;
        for (row, &group) in group_indices.iter().enumerate() {
            let sum = &mut self.sums[group];
            *sum = self.kind.checked_add(*sum, sums.value(row))?;
            let count = &mut self.counts[group];
            *count = count
                .checked_add(counts.value(row))
                .ok_or_else(count_overflow)?;
        }
        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> DFResult<ArrayRef> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        Ok(Arc::new(sum_count_array(self.kind, sums, counts)?))
    }

    fn state(&mut self, emit_to: EmitTo) -> DFResult<Vec<ArrayRef>> {
        let sums = emit_to.take_needed(&mut self.sums);
        let counts = emit_to.take_needed(&mut self.counts);
        let state = sum_count_array(self.kind, sums, counts)?;
        Ok(state.columns().to_vec())
    }

    /// One state row per input row; a filtered or NULL row is a zero sum and
    /// a zero count.
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> DFResult<Vec<ArrayRef>> {
        let rows = values[0].len();
        let mut sums = vec![0i128; rows];
        let mut counts = vec![0i64; rows];
        self.kind.for_each_value(&values[0], |row, value| {
            if !row_is_filtered(opt_filter, row) {
                sums[row] = value;
                counts[row] = 1;
            }
            Ok(())
        })?;
        Ok(sum_count_array(self.kind, sums, counts)?.columns().to_vec())
    }

    fn size(&self) -> usize {
        self.sums.capacity() * std::mem::size_of::<i128>()
            + self.counts.capacity() * std::mem::size_of::<i64>()
    }
}

/// `sum + addend * count` of an `exact_sum_count` value, converted to the
/// `sum` call's result type, or the overflow error when it does not fit; NULL
/// when the count is zero.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ExactSumPlus {
    kind: SumKind,
    signature: Signature,
}

impl ExactSumPlus {
    fn new(kind: SumKind) -> Self {
        Self {
            kind,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }

    /// The total of one group, or `None` for a group with no non-null row.
    fn total(&self, sum: i128, count: i64, addend: i128) -> DFResult<Option<i128>> {
        if count == 0 {
            return Ok(None);
        }
        addend
            .checked_mul(i128::from(count))
            .and_then(|term| sum.checked_add(term))
            .map(Some)
            .ok_or_else(|| self.kind.overflow())
    }
}

impl ScalarUDFImpl for ExactSumPlus {
    fn name(&self) -> &str {
        "exact_sum_plus"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(self.kind.result_type())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let internal = |what: &str| DataFusionError::Internal(format!("exact_sum_plus: {what}"));
        let [value, ColumnarValue::Scalar(addend)] = args.args.as_slice() else {
            return Err(internal("expects a sum-and-count and a literal addend"));
        };
        let addend =
            addend_value(addend).ok_or_else(|| internal("the addend is not an integer"))?;
        let is_scalar = matches!(value, ColumnarValue::Scalar(_));
        let array = value.to_array(args.number_rows)?;
        let groups = array
            .as_struct_opt()
            .ok_or_else(|| internal("the sum-and-count is not a struct"))?;
        let sums = as_decimal128(groups.column(0))?;
        let counts = as_count(groups.column(1))?;
        let totals = (0..groups.len()).map(|row| {
            if groups.is_null(row) {
                return Ok(None);
            }
            self.total(sums.value(row), counts.value(row), addend)
        });
        let kind = self.kind;
        let result: ArrayRef = match kind {
            SumKind::Signed => Arc::new(
                totals
                    .map(|total| {
                        total?
                            .map(|total| i64::try_from(total).map_err(|_| kind.overflow()))
                            .transpose()
                    })
                    .collect::<DFResult<PrimitiveArray<Int64Type>>>()?,
            ),
            SumKind::Unsigned => Arc::new(
                totals
                    .map(|total| {
                        total?
                            .map(|total| u64::try_from(total).map_err(|_| kind.overflow()))
                            .transpose()
                    })
                    .collect::<DFResult<PrimitiveArray<UInt64Type>>>()?,
            ),
        };
        if is_scalar {
            ScalarValue::try_from_array(&result, 0).map(ColumnarValue::Scalar)
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        DictionaryArray, Int8Array, Int32Array, UInt32Array, UInt64Array,
    };
    use datafusion::arrow::compute::take;
    use datafusion::arrow::datatypes::{Int32Type, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::physical_plan::aggregates::AggregateExec;
    use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
    use datafusion::prelude::{SessionConfig, SessionContext};

    use crate::error::SqlError;
    use crate::sum::checked_sum_udaf;

    const MAX: i64 = i64::MAX;
    const MIN: i64 = i64::MIN;

    /// The table `t(k, v)`: group key `k` and the values `v`, spread
    /// round-robin over `partitions` partitions.
    fn table(keys: &[i64], values: ArrayRef, partitions: usize) -> MemTable {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", values.data_type().clone(), true),
        ]));
        let keys: ArrayRef = Arc::new(Int64Array::from(keys.to_vec()));
        let parts = (0..partitions)
            .map(|part| {
                let rows: Vec<u32> = (0..values.len())
                    .filter(|row| row % partitions == part)
                    .map(|row| u32::try_from(row).expect("row"))
                    .collect();
                if rows.is_empty() {
                    return Vec::new();
                }
                let rows = UInt32Array::from(rows);
                vec![
                    RecordBatch::try_new(
                        Arc::clone(&schema),
                        vec![
                            take(&keys, &rows, None).expect("take"),
                            take(&values, &rows, None).expect("take"),
                        ],
                    )
                    .expect("batch"),
                ]
            })
            .collect();
        MemTable::try_new(schema, parts).expect("memtable")
    }

    fn signed(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn unsigned(values: &[Option<u64>]) -> ArrayRef {
        Arc::new(UInt64Array::from(values.to_vec()))
    }

    /// Every row in group 1.
    fn ones(values: &ArrayRef) -> Vec<i64> {
        vec![1; values.len()]
    }

    struct Run {
        plan: Arc<dyn ExecutionPlan>,
        result: DFResult<Vec<RecordBatch>>,
    }

    impl Run {
        fn text(&self) -> String {
            displayable(self.plan.as_ref()).indent(true).to_string()
        }

        /// The aggregate expression count of every `AggregateExec` in the
        /// plan, top down.
        fn aggregate_widths(&self) -> Vec<usize> {
            fn walk(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<usize>) {
                if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
                    out.push(aggregate.aggr_expr().len());
                }
                for child in plan.children() {
                    walk(child, out);
                }
            }
            let mut out = Vec::new();
            walk(&self.plan, &mut out);
            out
        }

        fn batches(self, case: &str) -> Vec<RecordBatch> {
            match self.result {
                Ok(batches) => batches,
                Err(err) => panic!("{case}: {err}"),
            }
        }
    }

    /// Plan and run `sql` over `t` with the checked `sum` registered, at
    /// `partitions` target partitions with repartitioned aggregation, with or
    /// without [`ExactSumPlusLiteral`].
    fn run_with(t: MemTable, partitions: usize, sql: &str, rewrite: bool) -> Run {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let config = SessionConfig::new()
                .with_target_partitions(partitions)
                .with_repartition_aggregations(true);
            let mut builder = SessionStateBuilder::new()
                .with_config(config)
                .with_default_features();
            if rewrite {
                builder = builder.with_optimizer_rules(optimizer_rules());
            }
            let ctx = SessionContext::new_with_state(builder.build());
            ctx.register_udaf(checked_sum_udaf());
            ctx.register_table("t", Arc::new(t)).expect("register");
            let plan = ctx
                .sql(sql)
                .await
                .expect("plan")
                .create_physical_plan()
                .await
                .expect("physical plan");
            let result = collect(Arc::clone(&plan), ctx.task_ctx()).await;
            Run { plan, result }
        })
    }

    fn query(t: MemTable, partitions: usize, sql: &str) -> Run {
        run_with(t, partitions, sql, true)
    }

    /// Every cell of `batches`, row by row, as `i128` (NULL as `None`).
    fn cells(batches: &[RecordBatch]) -> Vec<Vec<Option<i128>>> {
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                rows.push(
                    batch
                        .columns()
                        .iter()
                        .map(|column| match column.data_type() {
                            DataType::Int64 => {
                                let column = column.as_primitive::<Int64Type>();
                                column.is_valid(row).then(|| i128::from(column.value(row)))
                            }
                            DataType::UInt64 => {
                                let column = column.as_primitive::<UInt64Type>();
                                column.is_valid(row).then(|| i128::from(column.value(row)))
                            }
                            other => panic!("unexpected result column {other}"),
                        })
                        .collect(),
                );
            }
        }
        rows
    }

    fn assert_overflow(run: Run, result_type: &str, case: &str) {
        let err = match run.result {
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

    fn assert_rewritten(run: &Run, case: &str) {
        let plan = run.text();
        assert!(plan.contains("exact_sum_count("), "{case}: {plan}");
        assert!(plan.contains("exact_sum_plus("), "{case}: {plan}");
    }

    fn assert_not_rewritten(run: &Run, case: &str) {
        let plan = run.text();
        assert!(!plan.contains("exact_sum_count("), "{case}: {plan}");
    }

    /// The pin the statistics path shares: rows `i64::MAX` and `-i64::MAX`,
    /// where each row's `v + 1` leaves `i64` but the total, 2, fits. The other
    /// cases each move the sum of `v` itself, or the addend, outside `i64`
    /// while the total fits.
    #[test]
    fn exact_total_where_rows_leave_the_type() {
        let cases: [(&str, Vec<Option<i64>>, i128); 6] = [
            ("sum(v + 1)", vec![Some(MAX), Some(-MAX)], 2),
            ("sum(1 + v)", vec![Some(MAX), Some(-MAX)], 2),
            ("sum(v - 1)", vec![Some(MIN), Some(MAX)], -3),
            (
                "sum(v - 9223372036854775807)",
                vec![Some(MAX), Some(MAX)],
                0,
            ),
            (
                "sum(v + 9223372036854775807)",
                vec![Some(MIN + 1), Some(MIN + 1)],
                0,
            ),
            ("sum(v + 2)", vec![Some(MAX), None, Some(-MAX - 1)], 3),
        ];
        for (sum, values, expected) in cases {
            let values = signed(&values);
            for partitions in [1, 8] {
                for (shape, sql) in [
                    ("ungrouped", format!("SELECT {sum} FROM t")),
                    ("grouped", format!("SELECT {sum} FROM t GROUP BY k")),
                ] {
                    let case = format!("{shape} {sum} at {partitions}");
                    let run = query(
                        table(&ones(&values), Arc::clone(&values), partitions),
                        partitions,
                        &sql,
                    );
                    assert_rewritten(&run, &case);
                    assert_eq!(
                        cells(&run.batches(&case)),
                        vec![vec![Some(expected)]],
                        "{case}"
                    );
                }
            }
        }
    }

    /// A total outside `Int64` fails with the integer sum overflow, grouped
    /// and ungrouped, at one and eight partitions; a fitting second group does
    /// not mask it.
    #[test]
    fn signed_total_outside_int64_overflows() {
        let cases: [(&str, Vec<Option<i64>>); 3] = [
            ("sum(v + 1)", vec![Some(MAX - 1), Some(1)]),
            ("sum(v - 1)", vec![Some(MIN + 1), Some(-1)]),
            ("sum(v + 9223372036854775807)", vec![Some(1), Some(0)]),
        ];
        for (sum, values) in cases {
            for partitions in [1, 8] {
                let ungrouped = signed(&values);
                let case = format!("ungrouped {sum} at {partitions}");
                let run = query(
                    table(&ones(&ungrouped), ungrouped, partitions),
                    partitions,
                    &format!("SELECT {sum} FROM t"),
                );
                assert_rewritten(&run, &case);
                assert_overflow(run, "Int64", &case);

                let mut keys = vec![1; values.len()];
                keys.extend([2, 2]);
                let mut grouped = values.clone();
                grouped.extend([Some(3), Some(4)]);
                let grouped = signed(&grouped);
                let case = format!("grouped {sum} at {partitions}");
                let run = query(
                    table(&keys, grouped, partitions),
                    partitions,
                    &format!("SELECT k, {sum} FROM t GROUP BY k"),
                );
                assert_rewritten(&run, &case);
                assert_overflow(run, "Int64", &case);
            }
        }
    }

    /// The unsigned form: exact where a row's `v - 1` would leave `UInt64`,
    /// and the overflow against `UInt64` where the total does.
    #[test]
    fn unsigned_total_is_exact_and_checked_against_uint64() {
        let max = u64::MAX;
        for partitions in [1, 8] {
            for (shape, group_by) in [("ungrouped", ""), ("grouped", " GROUP BY k")] {
                let values = unsigned(&[Some(max), Some(0), Some(2)]);
                let case = format!("{shape} exact at {partitions}");
                let run = query(
                    table(&ones(&values), Arc::clone(&values), partitions),
                    partitions,
                    &format!("SELECT sum(v - CAST(1 AS BIGINT UNSIGNED)) FROM t{group_by}"),
                );
                assert_rewritten(&run, &case);
                assert_eq!(
                    cells(&run.batches(&case)),
                    vec![vec![Some(i128::from(max) - 1)]],
                    "{case}"
                );

                let values = unsigned(&[Some(max - 1), Some(1)]);
                let case = format!("{shape} overflow at {partitions}");
                let run = query(
                    table(&ones(&values), Arc::clone(&values), partitions),
                    partitions,
                    &format!("SELECT sum(v + CAST(1 AS BIGINT UNSIGNED)) FROM t{group_by}"),
                );
                assert_rewritten(&run, &case);
                assert_overflow(run, "UInt64", &case);
            }
        }
    }

    /// NULL rows add nothing and are not counted; a group or input with no
    /// non-null row is NULL, as for `sum`.
    #[test]
    fn null_and_empty_groups_keep_sum_semantics() {
        let values = signed(&[Some(1), None, Some(2), None, None]);
        let keys = [1, 1, 1, 2, 2];
        for partitions in [1, 8] {
            let case = format!("grouped at {partitions}");
            let run = query(
                table(&keys, Arc::clone(&values), partitions),
                partitions,
                "SELECT k, sum(v + 10) FROM t GROUP BY k ORDER BY k",
            );
            assert_rewritten(&run, &case);
            assert_eq!(
                cells(&run.batches(&case)),
                vec![vec![Some(1), Some(23)], vec![Some(2), None]],
                "{case}"
            );

            let case = format!("empty at {partitions}");
            let run = query(
                table(&keys, Arc::clone(&values), partitions),
                partitions,
                "SELECT sum(v + 10) FROM t WHERE k > 5",
            );
            assert_rewritten(&run, &case);
            assert_eq!(cells(&run.batches(&case)), vec![vec![None]], "{case}");
        }
    }

    /// Narrow signed inputs and dictionaries of them take the exact form.
    #[test]
    fn narrow_and_dictionary_inputs_are_exact() {
        let narrow: ArrayRef = Arc::new(Int8Array::from(vec![127, 127, -128]));
        let run_narrow = query(
            table(&ones(&narrow), narrow, 1),
            1,
            "SELECT sum(v + 1) FROM t",
        );
        assert_rewritten(&run_narrow, "Int8");
        assert_eq!(cells(&run_narrow.batches("Int8")), vec![vec![Some(129)]]);

        let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![0, 1, 1]),
            Arc::new(Int64Array::from(vec![MAX, -MAX])),
        ));
        for partitions in [1, 8] {
            let case = format!("dictionary at {partitions}");
            let run = query(
                table(&ones(&dictionary), Arc::clone(&dictionary), partitions),
                partitions,
                "SELECT sum(v + 1) FROM t",
            );
            assert_rewritten(&run, &case);
            assert_eq!(
                cells(&run.batches(&case)),
                vec![vec![Some(i128::from(-MAX) + 3)]]
            );
        }
    }

    fn q30_sql(terms: usize) -> String {
        let sums: Vec<String> = std::iter::once("sum(v)".to_string())
            .chain((1..terms).map(|k| format!("sum(v + {k})")))
            .collect();
        format!("SELECT {} FROM t", sums.join(", "))
    }

    /// ClickBench q30's shape, `SUM(x)` and `SUM(x + 1)` to `SUM(x + 89)`,
    /// runs one shared aggregate over `x` and returns the exact totals.
    /// Without the rule it runs one aggregate per term.
    #[test]
    fn q30_shape_runs_one_aggregate() {
        let values = signed(&(1..=100).map(Some).collect::<Vec<_>>());
        for partitions in [1, 8] {
            let case = format!("q30 at {partitions}");
            let run = query(
                table(&ones(&values), Arc::clone(&values), partitions),
                partitions,
                &q30_sql(90),
            );
            assert_rewritten(&run, &case);
            let widths = run.aggregate_widths();
            assert!(!widths.is_empty(), "{case}");
            assert!(widths.iter().all(|&width| width == 1), "{case}: {widths:?}");
            let expected: Vec<Option<i128>> = (0..90).map(|k| Some(5050 + 100 * k)).collect();
            assert_eq!(cells(&run.batches(&case)), vec![expected], "{case}");

            let before = run_with(
                table(&ones(&values), Arc::clone(&values), partitions),
                partitions,
                &q30_sql(90),
                false,
            );
            let widths = before.aggregate_widths();
            assert!(
                widths.iter().all(|&width| width == 90),
                "{case} before: {widths:?}"
            );
        }
    }

    /// A `FILTER` takes the exact form, kept on the shared aggregate; calls
    /// with different filters do not share one.
    #[test]
    fn filter_takes_the_exact_form() {
        let values = signed(&[Some(MAX), Some(-MAX), Some(5)]);
        let keys = [1, 1, 2];
        for partitions in [1, 8] {
            let case = format!("filter at {partitions}");
            let run = query(
                table(&keys, Arc::clone(&values), partitions),
                partitions,
                "SELECT sum(v + 1) FILTER (WHERE k = 1), sum(v + 2) FILTER (WHERE k = 1), \
                 sum(v + 1) FILTER (WHERE k = 2) FROM t",
            );
            assert_rewritten(&run, &case);
            let widths = run.aggregate_widths();
            assert!(
                widths.iter().all(|&width| width == 2),
                "{case}: {widths:?} {}",
                run.text()
            );
            assert_eq!(
                cells(&run.batches(&case)),
                vec![vec![Some(2), Some(4), Some(6)]],
                "{case}"
            );
        }
    }

    /// `SUM(DISTINCT x + c)` keeps the checked per-row path: here each row's
    /// wrapped `v + 1` reaches the checked sum, whose total leaves `Int64`.
    #[test]
    fn distinct_keeps_the_checked_per_row_path() {
        let values = signed(&[Some(MAX), Some(-MAX)]);
        let run = query(
            table(&ones(&values), values, 1),
            1,
            "SELECT sum(DISTINCT v + 1) FROM t",
        );
        assert_not_rewritten(&run, "distinct");
        assert_overflow(run, "Int64", "distinct");
    }

    /// A window `SUM(x + c) OVER (...)` keeps the checked per-row path.
    #[test]
    fn window_keeps_the_checked_per_row_path() {
        let values = signed(&[Some(MAX), Some(-MAX)]);
        let run = query(
            table(&ones(&values), values, 1),
            1,
            "SELECT sum(v + 1) OVER () FROM t",
        );
        assert_not_rewritten(&run, "window");
        assert_overflow(run, "Int64", "window");
    }

    /// The rewrite keeps the output names and types, and leaves an aggregate
    /// with no `SUM(x ± c)` alone.
    #[test]
    fn output_schema_is_unchanged() {
        let values = signed(&[Some(1), Some(2)]);
        let sql = "SELECT k, sum(v), sum(v + 1), sum(v - 3) AS s, count(v) FROM t GROUP BY k";
        let after = query(table(&ones(&values), Arc::clone(&values), 4), 4, sql);
        let before = run_with(table(&ones(&values), Arc::clone(&values), 4), 4, sql, false);
        assert_rewritten(&after, "schema");
        assert_eq!(after.plan.schema(), before.plan.schema());
        assert_eq!(
            cells(&after.batches("after")),
            cells(&before.batches("before"))
        );

        let plain = query(table(&ones(&values), values, 1), 1, "SELECT sum(v) FROM t");
        assert_not_rewritten(&plain, "plain sum");
    }
}
