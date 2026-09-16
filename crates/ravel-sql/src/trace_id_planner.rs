//! Make `trace_id = '<32-hex>'` plannable against the `spans` table's
//! `FixedSizeBinary(16)` `trace_id` column.
//!
//! DataFusion's `type_coercion` analyzer rule has no common type between
//! `FixedSizeBinary(16)` and `Utf8`/`LargeUtf8`/`Utf8View` for a comparison,
//! so `WHERE trace_id = '00112233445566778899aabbccddeeff'` fails to plan at
//! all (`Cannot infer common argument type for comparison operation
//! FixedSizeBinary(16) = Utf8`), before [`crate::spans_pushdown`] ever sees
//! the predicate. A `X'..'` binary literal does not hit this: it lowers to
//! `ScalarValue::Binary`, and DataFusion's built-in coercion already unifies
//! `Binary`/`FixedSizeBinary` pairs.
//!
//! [`TraceIdHexLiteralPlanner`] closes the gap for the string form by
//! rewriting an `Eq`/`NotEq` comparison between the schema-verified
//! `trace_id` column and a 32-character hex string literal into a native
//! `FixedSizeBinary(16)` literal, during SQL-text-to-`Expr` lowering, before
//! `type_coercion` runs. A wrong-length or non-hex literal is left
//! unplanned (`PlannerResult::Original`) so DataFusion's ordinary
//! `type_coercion` error still surfaces; this planner never silently
//! matches a malformed literal.
//!
//! `IN` lists are out of reach for this mechanism: `datafusion-sql`'s
//! `sql_in_list_to_expr` builds `Expr::InList` directly from independently
//! lowered sub-expressions and never consults a registered `ExprPlanner`.
//! Only `Eq`/`NotEq` are rewritten here.
//!
//! The column match checks the resolved schema type, not just the column
//! name `trace_id`: this planner registers once per session and runs over
//! every query against every table, not only `spans`.

use std::sync::Arc;

use datafusion::common::{DFSchema, ExprSchema};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion::logical_expr::{Expr, lit};
use datafusion::scalar::ScalarValue;
use datafusion::sql::sqlparser::ast::BinaryOperator;

use crate::spans_pushdown::hex_trace_id_literal;
use crate::spans_schema::TRACE_ID_WIDTH;

/// Rewrites `trace_id = '<32-hex>'` / `trace_id != '<32-hex>'` into the
/// column's native `FixedSizeBinary(16)` comparison. See the module docs.
#[derive(Debug, Default)]
pub struct TraceIdHexLiteralPlanner;

impl ExprPlanner for TraceIdHexLiteralPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        schema: &DFSchema,
    ) -> DFResult<PlannerResult<RawBinaryExpr>> {
        let RawBinaryExpr { op, left, right } = expr;
        let not_eq = match op {
            BinaryOperator::Eq => false,
            BinaryOperator::NotEq => true,
            _ => return Ok(PlannerResult::Original(RawBinaryExpr { op, left, right })),
        };

        if is_trace_id_column(&left, schema) {
            if let Some(bytes) = hex_trace_id_literal(&right) {
                let fixed = trace_id_scalar(bytes);
                return Ok(PlannerResult::Planned(if not_eq {
                    left.not_eq(fixed)
                } else {
                    left.eq(fixed)
                }));
            }
        } else if is_trace_id_column(&right, schema)
            && let Some(bytes) = hex_trace_id_literal(&left)
        {
            let fixed = trace_id_scalar(bytes);
            return Ok(PlannerResult::Planned(if not_eq {
                fixed.not_eq(right)
            } else {
                fixed.eq(right)
            }));
        }

        let op = if not_eq {
            BinaryOperator::NotEq
        } else {
            BinaryOperator::Eq
        };
        Ok(PlannerResult::Original(RawBinaryExpr { op, left, right }))
    }
}

/// Whether `e` is a column resolving in `schema` to `spans.trace_id`'s exact
/// native type, `FixedSizeBinary(16)`. Checking the resolved type (not just
/// the name `trace_id`) matters because this planner runs over every query,
/// not only ones against `spans`.
fn is_trace_id_column(e: &Expr, schema: &DFSchema) -> bool {
    let Expr::Column(column) = e else {
        return false;
    };
    if column.name != "trace_id" {
        return false;
    }
    matches!(
        schema.data_type(column),
        Ok(datafusion::arrow::datatypes::DataType::FixedSizeBinary(w)) if *w == TRACE_ID_WIDTH
    )
}

fn trace_id_scalar(bytes: [u8; 16]) -> Expr {
    lit(ScalarValue::FixedSizeBinary(
        TRACE_ID_WIDTH,
        Some(bytes.to_vec()),
    ))
}

/// Convenience constructor for [`crate::session::build_session`].
pub fn trace_id_hex_literal_planner() -> Arc<dyn ExprPlanner> {
    Arc::new(TraceIdHexLiteralPlanner)
}
