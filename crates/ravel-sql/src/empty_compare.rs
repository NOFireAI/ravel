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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use datafusion::arrow::array::{
        ArrayRef, BinaryArray, BinaryViewArray, DictionaryArray, Int32Array, LargeBinaryArray,
        LargeStringArray, Scalar, StringArray, StringViewArray,
    };
    use datafusion::arrow::compute::cast;
    use datafusion::arrow::compute::kernels::cmp::{eq, neq};
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion::physical_expr::expressions::{Column, NotExpr};
    use proptest::prelude::*;

    use super::*;

    /// Empty values, non-empty values and NULLs, with an empty value first so
    /// a slice starting at row 1 begins on an empty value whose start offset
    /// is not 0.
    const ROWS: [Option<&str>; 8] = [
        Some("a"),
        Some(""),
        None,
        Some("bc"),
        Some(""),
        Some("longer than twelve bytes"),
        None,
        Some(""),
    ];

    /// No empty value, so a dictionary over these rows has no `''` entry.
    const NO_EMPTY: [Option<&str>; 6] = [Some("a"), None, Some("bc"), Some("a"), Some("x"), None];

    /// `rows` as a `Dictionary(Int32, Utf8)` over `values`, each present row
    /// keyed to its value's position there and each NULL row keyed NULL.
    fn dictionary(rows: &[Option<&str>], values: &[&str]) -> ArrayRef {
        let keys: Int32Array = rows
            .iter()
            .map(|r| r.map(|v| values.iter().position(|x| *x == v).expect("a value") as i32))
            .collect();
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                keys,
                Arc::new(StringArray::from(values.to_vec())),
            )
            .expect("dictionary"),
        )
    }

    /// Every layout of `rows`: offsets (`Utf8`, `LargeUtf8`, `Binary`,
    /// `LargeBinary`), views (`Utf8View`, `BinaryView`), and a dictionary over
    /// values that hold `''` exactly when `rows` does, with neither `''` nor
    /// the first row's value at key 0.
    fn layouts(rows: &[Option<&str>]) -> Vec<(&'static str, ArrayRef)> {
        let bytes: Vec<Option<&[u8]>> = rows.iter().map(|r| r.map(str::as_bytes)).collect();
        let mut values = vec!["unused"];
        for v in rows.iter().flatten() {
            if !values.contains(v) {
                values.push(v);
            }
        }
        vec![
            (
                "Utf8",
                Arc::new(StringArray::from(rows.to_vec())) as ArrayRef,
            ),
            ("LargeUtf8", Arc::new(LargeStringArray::from(rows.to_vec()))),
            ("Utf8View", Arc::new(StringViewArray::from(rows.to_vec()))),
            ("Binary", Arc::new(BinaryArray::from(bytes.clone()))),
            (
                "LargeBinary",
                Arc::new(LargeBinaryArray::from(bytes.clone())),
            ),
            ("BinaryView", Arc::new(BinaryViewArray::from(bytes))),
            ("Dictionary", dictionary(rows, &values)),
        ]
    }

    /// A dictionary whose values array is itself sliced (its first offset is
    /// not 0) and holds a NULL entry, keyed through NULL keys too: rows `''`,
    /// `''`, NULL (entry), NULL (key), `'z'`, `''`.
    fn sliced_values_dictionary() -> (ArrayRef, Vec<Option<&'static str>>) {
        let values = StringArray::from(vec![Some("q"), Some(""), None, Some("z"), Some("")]);
        let values = values.slice(1, 4);
        let keys = Int32Array::from(vec![Some(3), Some(0), Some(1), None, Some(2), Some(0)]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).expect("dict");
        let rows = vec![Some(""), Some(""), None, None, Some("z"), Some("")];
        (Arc::new(dict), rows)
    }

    /// The verdicts SQL gives for `rows`: NULL for a NULL row, else whether
    /// the value is empty (or not, when `negated`).
    fn expected(rows: &[Option<&str>], negated: bool) -> Vec<Option<bool>> {
        rows.iter()
            .map(|r| r.map(|v| v.is_empty() != negated))
            .collect()
    }

    fn verdicts(array: &BooleanArray) -> Vec<Option<bool>> {
        array.iter().collect()
    }

    /// Arrow's own `=`/`<>` kernel against an empty scalar of `array`'s type,
    /// the evaluation the rule replaces.
    fn kernel(array: &ArrayRef, negated: bool) -> BooleanArray {
        let empty = cast(&StringArray::from(vec![""]), array.data_type()).expect("cast");
        let scalar = Scalar::new(empty);
        if negated {
            neq(array, &scalar).expect("neq")
        } else {
            eq(array, &scalar).expect("eq")
        }
    }

    /// T3: every layout, unsliced and sliced at a non-zero offset, with NULL
    /// rows inside the slice, gives SQL's verdict for each row and arrow's
    /// kernel's answer, for `=` and `<>`.
    ///
    /// Flipped: dropping the validity (`BooleanArray::new(values, None)`)
    /// fails at the NULL rows of every slice; reading lengths from a start
    /// offset of 0 instead of the slice's first offset fails at row 0 of the
    /// slice from row 1, which is `''`; testing the dictionary's keys instead
    /// of its values fails at the first row keyed to an entry other than 0.
    #[test]
    fn every_layout_matches_the_rows_when_sliced_and_null() {
        for rows in [&ROWS[..], &NO_EMPTY[..]] {
            for (name, array) in layouts(rows) {
                for (offset, len) in [(0, rows.len()), (1, rows.len() - 1), (2, 3), (3, 0)] {
                    let sliced = array.slice(offset, len);
                    let want_rows = &rows[offset..offset + len];
                    for negated in [false, true] {
                        let got = empty_compare(sliced.as_ref(), negated).expect("evaluates");
                        let at = format!("{name} rows {offset}..+{len} negated={negated}");
                        assert_eq!(verdicts(&got), expected(want_rows, negated), "{at}");
                        assert_eq!(
                            verdicts(&got),
                            verdicts(&kernel(&sliced, negated)),
                            "{at}: arrow's kernel"
                        );
                    }
                }
            }
        }
    }

    /// T3: a dictionary over a sliced values array with a NULL entry: the
    /// verdict is taken through each key, NULL for a NULL key and for a key
    /// naming the NULL entry, for the whole array and a slice of it.
    #[test]
    fn a_dictionary_over_sliced_values_takes_each_verdict_through_its_key() {
        let (dict, rows) = sliced_values_dictionary();
        for (offset, len) in [(0, rows.len()), (1, 4)] {
            let sliced = dict.slice(offset, len);
            for negated in [false, true] {
                let got = empty_compare(sliced.as_ref(), negated).expect("evaluates");
                let at = format!("rows {offset}..+{len} negated={negated}");
                assert_eq!(
                    verdicts(&got),
                    expected(&rows[offset..offset + len], negated),
                    "{at}"
                );
                assert_eq!(
                    verdicts(&got),
                    verdicts(&kernel(&sliced, negated)),
                    "{at}: arrow's kernel"
                );
            }
        }
    }

    /// A scalar operand gives a scalar verdict, NULL for a NULL scalar.
    #[test]
    fn a_scalar_operand_gives_a_scalar_verdict() {
        let schema = Schema::empty();
        let batch = RecordBatch::new_empty(Arc::new(schema));
        for (value, negated, want) in [
            (ScalarValue::Utf8(Some(String::new())), false, Some(true)),
            (ScalarValue::Utf8(Some("a".into())), false, Some(false)),
            (ScalarValue::Utf8(Some("a".into())), true, Some(true)),
            (ScalarValue::Utf8(None), true, None),
        ] {
            let expr = EmptyCompareExpr::new(Arc::new(Literal::new(value.clone())), negated);
            match expr.evaluate(&batch).expect("evaluates") {
                ColumnarValue::Scalar(ScalarValue::Boolean(got)) => {
                    assert_eq!(got, want, "{value:?} negated={negated}")
                }
                other => panic!("{value:?}: not a boolean scalar: {other:?}"),
            }
        }
    }

    fn column_schema() -> Schema {
        Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("t", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
            Field::new(
                "d",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("b", DataType::Binary, true),
        ])
    }

    fn col(name: &str) -> Arc<dyn PhysicalExpr> {
        let index = column_schema().index_of(name).expect("column");
        Arc::new(Column::new(name, index))
    }

    fn lit(value: ScalarValue) -> Arc<dyn PhysicalExpr> {
        Arc::new(Literal::new(value))
    }

    fn bin(
        l: Arc<dyn PhysicalExpr>,
        op: Operator,
        r: Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        Arc::new(BinaryExpr::new(l, op, r))
    }

    fn empty_utf8() -> ScalarValue {
        ScalarValue::Utf8(Some(String::new()))
    }

    fn rewritten(expr: Arc<dyn PhysicalExpr>) -> (bool, String) {
        let t = rewrite_expr(expr, &column_schema()).expect("rewrite");
        (t.transformed, t.data.to_string())
    }

    /// T1 at the expression: `=` and `<>` with the empty literal on either
    /// side, typed `Utf8`, dictionary or `Binary`, become `empty_eq(..)` and
    /// `empty_ne(..)`, also inside `NOT` and `OR`. A non-empty literal, two
    /// columns, a NULL literal, another operator and a non-string operand are
    /// left as they were.
    #[test]
    fn rewrites_only_the_empty_literal_comparison() {
        let dict_empty = ScalarValue::Dictionary(Box::new(DataType::Int32), Box::new(empty_utf8()));
        let rewrites = [
            (
                bin(col("s"), Operator::NotEq, lit(empty_utf8())),
                "empty_ne(s@0)",
            ),
            (
                bin(col("s"), Operator::Eq, lit(empty_utf8())),
                "empty_eq(s@0)",
            ),
            (
                bin(lit(empty_utf8()), Operator::NotEq, col("s")),
                "empty_ne(s@0)",
            ),
            (
                bin(lit(empty_utf8()), Operator::Eq, col("s")),
                "empty_eq(s@0)",
            ),
            (
                bin(col("d"), Operator::NotEq, lit(dict_empty.clone())),
                "empty_ne(d@3)",
            ),
            (
                bin(lit(dict_empty), Operator::Eq, col("d")),
                "empty_eq(d@3)",
            ),
            (
                bin(
                    col("b"),
                    Operator::NotEq,
                    lit(ScalarValue::Binary(Some(Vec::new()))),
                ),
                "empty_ne(b@4)",
            ),
            (
                Arc::new(NotExpr::new(bin(col("s"), Operator::Eq, lit(empty_utf8())))),
                "NOT empty_eq(s@0)",
            ),
            (
                bin(
                    bin(col("s"), Operator::NotEq, lit(empty_utf8())),
                    Operator::Or,
                    bin(col("n"), Operator::Eq, lit(ScalarValue::Int64(Some(1)))),
                ),
                "empty_ne(s@0) OR n@2 = 1",
            ),
        ];
        for (expr, want) in rewrites {
            let before = expr.to_string();
            assert_eq!(rewritten(expr), (true, want.to_string()), "{before}");
        }
        let untouched = [
            bin(
                col("s"),
                Operator::NotEq,
                lit(ScalarValue::Utf8(Some("x".into()))),
            ),
            bin(col("s"), Operator::Eq, col("t")),
            bin(col("s"), Operator::NotEq, lit(ScalarValue::Utf8(None))),
            bin(col("s"), Operator::Lt, lit(empty_utf8())),
            bin(col("s"), Operator::IsDistinctFrom, lit(empty_utf8())),
            bin(col("n"), Operator::Eq, lit(empty_utf8())),
            bin(col("n"), Operator::NotEq, lit(ScalarValue::Int64(Some(0)))),
        ];
        for expr in untouched {
            let before = expr.to_string();
            assert_eq!(rewritten(expr), (false, before.clone()), "{before}");
        }
    }

    /// The scan's EXPLAIN form: `fmt_sql` names the operand's column.
    #[test]
    fn displays_by_name() {
        let expr = EmptyCompareExpr::new(col("s"), true);
        assert_eq!(expr.to_string(), "empty_ne(s@0)");
        assert_eq!(
            datafusion::physical_expr_common::physical_expr::fmt_sql(&expr).to_string(),
            "empty_ne(s)"
        );
        let expr = EmptyCompareExpr::new(col("s"), false);
        assert_eq!(
            datafusion::physical_expr_common::physical_expr::fmt_sql(&expr).to_string(),
            "empty_eq(s)"
        );
    }

    /// One generated case: rows, a layout, a slice and an operator.
    #[derive(Debug, Clone)]
    struct Case {
        rows: Vec<Option<String>>,
        layout: usize,
        /// For the dictionary layouts: which NULL rows are keyed to a NULL
        /// entry rather than given a NULL key.
        null_entry: Vec<bool>,
        offset: usize,
        len: usize,
        negated: bool,
    }

    const LAYOUTS: usize = 9;

    fn case() -> impl Strategy<Value = Case> {
        let value = prop_oneof![
            3 => Just(String::new()),
            1 => Just("a".to_string()),
            1 => Just("bc".to_string()),
            1 => Just("longer than twelve bytes".to_string()),
        ];
        (
            proptest::collection::vec(proptest::option::weighted(0.8, value), 0..40),
            0..LAYOUTS,
            any::<bool>(),
            any::<u64>(),
            any::<u64>(),
        )
            .prop_flat_map(|(rows, layout, negated, a, b)| {
                let n = rows.len();
                let offset = (a % (n as u64 + 1)) as usize;
                let len = (b % ((n - offset) as u64 + 1)) as usize;
                proptest::collection::vec(any::<bool>(), n).prop_map(move |null_entry| Case {
                    rows: rows.clone(),
                    layout,
                    null_entry,
                    offset,
                    len,
                    negated,
                })
            })
    }

    /// The case's rows in its layout, unsliced. Layouts 7 and 8 are
    /// dictionaries with a NULL entry; 8's values array is itself sliced.
    fn build(case: &Case) -> ArrayRef {
        let rows: Vec<Option<&str>> = case.rows.iter().map(|r| r.as_deref()).collect();
        if case.layout < 7 {
            return Arc::clone(&layouts(&rows)[case.layout].1);
        }
        let mut values: Vec<Option<&str>> = vec![Some("pre"), Some("unused"), None];
        for v in rows.iter().flatten() {
            if !values.contains(&Some(*v)) {
                values.push(Some(*v));
            }
        }
        let keys: Int32Array = rows
            .iter()
            .zip(&case.null_entry)
            .map(|(r, to_entry)| match r {
                Some(v) => Some(values.iter().position(|x| *x == Some(*v)).expect("value") as i32),
                None => to_entry.then_some(2),
            })
            .collect();
        let values = StringArray::from(values);
        let (keys, values) = if case.layout == 8 {
            let keys: Int32Array = keys.iter().map(|k| k.map(|k| k - 1)).collect();
            (keys, values.slice(1, values.len() - 1))
        } else {
            (keys, values)
        };
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).expect("dict"))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// T4: random rows in a random layout, sliced at random, with NULLs
        /// (and NULL dictionary entries): the expression equals arrow's `eq`
        /// or `neq` kernel against an empty scalar, row for row.
        #[test]
        fn prop_matches_arrows_kernel(case in case()) {
            let sliced = build(&case).slice(case.offset, case.len);
            let got = empty_compare(sliced.as_ref(), case.negated).expect("evaluates");
            prop_assert_eq!(verdicts(&got), verdicts(&kernel(&sliced, case.negated)));
        }
    }
}
