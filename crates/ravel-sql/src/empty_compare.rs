//! Issue #4660: a comparison of a string or binary expression with the empty
//! literal, `x = ''` or `x <> ''` in either operand order, evaluated from
//! value lengths instead of value bytes.
//!
//! Arrow's comparison kernel answers `x <> ''` by comparing each value's
//! bytes with the literal's. When a value is itself empty, that is a
//! zero-length `memcmp` whose second pointer is the literal's empty buffer,
//! a dangling, unmapped address; a vectorised `memcmp` that reads through
//! both pointers under a mask whatever the length then costs far more than
//! the length test it amounts to. [`EmptyCompareExpr`] computes the same
//! answer without reading a byte:
//!
//! - an offsets layout (`Utf8`, `LargeUtf8`, `Binary`, `LargeBinary`): the
//!   value is empty when its two offsets are equal, read through the array's
//!   own (sliced) offsets;
//! - a view layout (`Utf8View`, `BinaryView`): the view's length field is
//!   zero;
//! - a dictionary: the test runs once over the dictionary's values and the
//!   verdicts are taken through the keys.
//!
//! The operand's validity carries over, so a NULL operand gives NULL, as the
//! kernel's does. A declared `Str` column reaches a filter as
//! `Dictionary(Int32, Utf8)`, so its `<>` runs the test once per dictionary
//! entry; a plain page's dictionary has one entry per row.
//!
//! [`EmptyStringCompare`] installs the expression in the two places a filter
//! runs in a `logs` plan: a `FilterExec` predicate, and the row filter a
//! [`LogsScanExec`] evaluates on each batch it builds (ADR-0093, exact
//! not-equal amendment). It replaces only `=` and `<>` between such an
//! operand and a non-NULL empty literal, anywhere inside the predicate (under
//! `NOT`, `OR` or `AND` too), and leaves every other comparison as it was.
//! Comparisons in a projection, an aggregate argument or a join filter are
//! not rewritten.
//!
//! The rule runs after every rule that reads a predicate's shape (see
//! `crate::session`), so they all still see the comparison: the provider
//! decides scan pushdown from the logical plan, before any physical rule,
//! and [`crate::metadata_agg::MetadataOnlyAggregate`] matches its `<>`
//! before this rule rewrites it.

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, BooleanArray, OffsetSizeTrait, RecordBatch};
use datafusion::arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{ColumnarValue, Operator};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Literal};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::scalar::ScalarValue;

use crate::logs_scan::LogsScanExec;

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const EMPTY_STRING_COMPARE_RULE: &str = "empty_string_compare";

/// Replaces `x = ''` and `x <> ''` (either operand order) over a string or
/// binary `x` with an [`EmptyCompareExpr`], in `FilterExec` predicates and in
/// a [`LogsScanExec`]'s row filters. See the module docs.
#[derive(Debug, Default)]
pub struct EmptyStringCompare;

impl PhysicalOptimizerRule for EmptyStringCompare {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        plan.transform_up(rewrite_node).data()
    }

    fn name(&self) -> &str {
        EMPTY_STRING_COMPARE_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn rewrite_node(node: Arc<dyn ExecutionPlan>) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(filter) = node.downcast_ref::<FilterExec>() {
        let schema = filter.input().schema();
        let predicate = rewrite_expr(Arc::clone(filter.predicate()), &schema)?;
        if !predicate.transformed {
            return Ok(Transformed::no(node));
        }
        let rebuilt = FilterExecBuilder::from(filter)
            .with_predicate(predicate.data)
            .build()?;
        return Ok(Transformed::yes(Arc::new(rebuilt)));
    }
    if let Some(scan) = node.downcast_ref::<LogsScanExec>()
        && let Some(rebuilt) = scan.with_row_filters_rewritten(rewrite_expr)?
    {
        return Ok(Transformed::yes(Arc::new(rebuilt)));
    }
    Ok(Transformed::no(node))
}

/// `expr` with every `x = ''` and `x <> ''` over a string or binary `x`
/// (typed against `schema`) replaced by an [`EmptyCompareExpr`].
pub(crate) fn rewrite_expr(
    expr: Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> DFResult<Transformed<Arc<dyn PhysicalExpr>>> {
    expr.transform_up(|node| {
        let Some(binary) = node.downcast_ref::<BinaryExpr>() else {
            return Ok(Transformed::no(node));
        };
        let negated = match binary.op() {
            Operator::Eq => false,
            Operator::NotEq => true,
            _ => return Ok(Transformed::no(node)),
        };
        let operand = if is_empty_literal(binary.right()) {
            binary.left()
        } else if is_empty_literal(binary.left()) {
            binary.right()
        } else {
            return Ok(Transformed::no(node));
        };
        match operand.data_type(schema) {
            Ok(ty) if is_byte_type(&ty) => Ok(Transformed::yes(Arc::new(EmptyCompareExpr::new(
                Arc::clone(operand),
                negated,
            ))
                as Arc<dyn PhysicalExpr>)),
            _ => Ok(Transformed::no(node)),
        }
    })
}

/// Whether `expr` is a non-NULL empty string or binary literal, possibly
/// dictionary-typed.
fn is_empty_literal(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.downcast_ref::<Literal>()
        .is_some_and(|lit| is_empty_scalar(lit.value()))
}

fn is_empty_scalar(value: &ScalarValue) -> bool {
    match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.is_empty(),
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b)) => b.is_empty(),
        ScalarValue::Dictionary(_, inner) => is_empty_scalar(inner),
        _ => false,
    }
}

/// The types [`empty_compare`] evaluates.
fn is_byte_type(ty: &DataType) -> bool {
    match ty {
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView => true,
        DataType::Dictionary(_, values) => is_byte_type(values),
        _ => false,
    }
}

/// `arg = ''` (or `arg <> ''` when `negated`) for a string or binary `arg`,
/// evaluated from value lengths. Displays in `EXPLAIN` as `empty_eq(arg)` or
/// `empty_ne(arg)`.
#[derive(Debug, Eq)]
pub struct EmptyCompareExpr {
    arg: Arc<dyn PhysicalExpr>,
    negated: bool,
}

impl EmptyCompareExpr {
    /// `arg = ''`, or `arg <> ''` when `negated`.
    pub fn new(arg: Arc<dyn PhysicalExpr>, negated: bool) -> Self {
        EmptyCompareExpr { arg, negated }
    }

    fn name(&self) -> &'static str {
        if self.negated { "empty_ne" } else { "empty_eq" }
    }
}

impl PartialEq for EmptyCompareExpr {
    fn eq(&self, other: &Self) -> bool {
        self.negated == other.negated && self.arg.eq(&other.arg)
    }
}

impl Hash for EmptyCompareExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.arg.hash(state);
        self.negated.hash(state);
    }
}

impl fmt::Display for EmptyCompareExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.name(), self.arg)
    }
}

impl PhysicalExpr for EmptyCompareExpr {
    fn data_type(&self, _input_schema: &Schema) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, input_schema: &Schema) -> DFResult<bool> {
        self.arg.nullable(input_schema)
    }

    fn return_field(&self, input_schema: &Schema) -> DFResult<FieldRef> {
        Ok(Arc::new(Field::new(
            "",
            DataType::Boolean,
            self.nullable(input_schema)?,
        )))
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        match self.arg.evaluate(batch)? {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(Arc::new(empty_compare(
                array.as_ref(),
                self.negated,
            )?))),
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array_of_size(1)?;
                let verdict = empty_compare(array.as_ref(), self.negated)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &verdict, 0,
                )?))
            }
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        let [arg] = <[_; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Internal(format!(
                "{} takes one child, got {}",
                self.name(),
                children.len()
            ))
        })?;
        Ok(Arc::new(EmptyCompareExpr::new(arg, self.negated)))
    }

    fn fmt_sql(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name())?;
        self.arg.fmt_sql(f)?;
        write!(f, ")")
    }
}

/// Per row of `array`, whether the value is empty (or, when `negated`, not
/// empty), NULL where the value is NULL. `array` is a string or binary array
/// in an offsets or view layout, or a dictionary over one.
pub fn empty_compare(array: &dyn Array, negated: bool) -> DFResult<BooleanArray> {
    match array.data_type() {
        DataType::Utf8 => {
            let a = array.as_string::<i32>();
            Ok(by_offsets(a.offsets(), a.nulls(), negated))
        }
        DataType::LargeUtf8 => {
            let a = array.as_string::<i64>();
            Ok(by_offsets(a.offsets(), a.nulls(), negated))
        }
        DataType::Binary => {
            let a = array.as_binary::<i32>();
            Ok(by_offsets(a.offsets(), a.nulls(), negated))
        }
        DataType::LargeBinary => {
            let a = array.as_binary::<i64>();
            Ok(by_offsets(a.offsets(), a.nulls(), negated))
        }
        DataType::Utf8View => {
            let a = array.as_string_view();
            Ok(by_views(a.views(), a.nulls(), negated))
        }
        DataType::BinaryView => {
            let a = array.as_binary_view();
            Ok(by_views(a.views(), a.nulls(), negated))
        }
        DataType::Dictionary(_, _) => {
            let dict = array.as_any_dictionary();
            let per_value = empty_compare(dict.values().as_ref(), negated)?;
            let per_row = take(&per_value, dict.keys(), None)?;
            Ok(per_row.as_boolean().clone())
        }
        other => Err(DataFusionError::Internal(format!(
            "empty string comparison over {other}"
        ))),
    }
}

/// The verdict of each value of an offsets layout. `offsets` is the array's
/// own, already sliced to its rows.
fn by_offsets<O: OffsetSizeTrait>(
    offsets: &OffsetBuffer<O>,
    nulls: Option<&NullBuffer>,
    negated: bool,
) -> BooleanArray {
    let values: BooleanBuffer = offsets
        .windows(2)
        .map(|w| (w[0] == w[1]) != negated)
        .collect();
    BooleanArray::new(values, nulls.cloned())
}

/// The verdict of each value of a view layout: the low 32 bits of a view
/// hold the value's length.
fn by_views(views: &ScalarBuffer<u128>, nulls: Option<&NullBuffer>, negated: bool) -> BooleanArray {
    let values: BooleanBuffer = views
        .iter()
        .map(|&v| ((v as u32) == 0) != negated)
        .collect();
    BooleanArray::new(values, nulls.cloned())
}
