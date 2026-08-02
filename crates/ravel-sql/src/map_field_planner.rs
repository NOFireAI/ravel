//! Issue #507: make `col['key']` plannable without widening the DataFusion
//! feature surface.
//!
//! `crates/ravel-sql/Cargo.toml` builds `datafusion` with
//! `default-features = false, features = ["sql"]` deliberately (see that
//! file's comments): no parquet/csv/avro/json, no compression codecs, no
//! nested/regex/crypto expression packs. With that feature set, DataFusion
//! registers no `ExprPlanner` implementing `plan_field_access`, so SQL text
//! containing a subscript such as `attrs['service.name']` fails query
//! *planning* itself, before any pruning or scan logic runs, with
//! `GetFieldAccess not supported`. ADR-0033 and ADR-0049 both name this gap
//! and ADR-0049 (RLOG POSTINGS) is blocked on it, since a predicate has to
//! plan before it can be pushed down or pruned.
//!
//! The only built-in fix is the `nested_expressions` Cargo feature, which
//! pulls in the whole `datafusion-functions-nested` crate (array/map/struct
//! builder and accessor functions, `flatten`, `array_agg` helpers, and so
//! on) to get its `NestedFunctionPlanner`. That is not needed here: for a
//! quoted-string subscript, DataFusion's SQL planner (`datafusion-sql`,
//! `expr/mod.rs`) always lowers `col['key']` to
//! `GetFieldAccess::NamedStructField { name }`, never to `ListIndex`, and
//! resolving that variant only requires calling
//! `get_field(col, key)` -- a scalar function in `datafusion-functions`'
//! `core` module, which is *not* feature-gated (it is a mandatory,
//! always-compiled dependency of the `datafusion` crate regardless of which
//! optional feature packs are enabled) and already handles both `Struct` and
//! `Map` columns at runtime, including returning `NULL` for a key absent
//! from a map.
//!
//! [`MapFieldAccessPlanner`] is therefore a few-line, zero-new-dependency,
//! zero-new-feature `ExprPlanner` that only implements `plan_field_access`
//! for the `NamedStructField` case and delegates to that always-available
//! function. `ListIndex`/`ListRange` (non-string-literal subscripts, slices)
//! are left unplanned (`PlannerResult::Original`): the `logs.attrs` and
//! `samples.labels` columns are string-keyed maps reached only through the
//! quoted-string subscript form, so those variants have no caller in this
//! crate's supported SQL surface today.
//!
//! Precedence: this planner only rewrites `col['key']` into
//! `get_field(col, 'key')` over whatever `Expr` the column reference already
//! resolves to. For `logs.attrs` that expression is the fully merged,
//! record-wins map `LogsScanExec::build_batch` already produces via
//! [`crate::rlog_attrs::merged_attrs`]; the planner runs one layer up (SQL
//! text to logical plan) and has no visibility into resource/scope/record
//! layering, so it neither observes nor can violate that precedence -- it is
//! inherited automatically because there is only ever one merged `attrs`
//! column by the time this planner sees the query.

use std::sync::Arc;

use datafusion::common::DFSchema;
use datafusion::error::Result as DFResult;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::GetFieldAccess;
use datafusion::logical_expr::planner::{ExprPlanner, PlannerResult, RawFieldAccessExpr};

/// Plans `col['key']` (`GetFieldAccess::NamedStructField`) into
/// `get_field(col, 'key')`. See the module docs for why this hand-written
/// planner is preferred over the `nested_expressions` feature.
#[derive(Debug, Default)]
pub struct MapFieldAccessPlanner;

impl ExprPlanner for MapFieldAccessPlanner {
    fn plan_field_access(
        &self,
        expr: RawFieldAccessExpr,
        _schema: &DFSchema,
    ) -> DFResult<PlannerResult<RawFieldAccessExpr>> {
        let RawFieldAccessExpr { expr, field_access } = expr;
        match field_access {
            GetFieldAccess::NamedStructField { name } => {
                Ok(PlannerResult::Planned(get_field(expr, name)))
            }
            other => Ok(PlannerResult::Original(RawFieldAccessExpr {
                expr,
                field_access: other,
            })),
        }
    }
}

/// Convenience constructor for [`crate::session::build_session`].
pub fn map_field_access_planner() -> Arc<dyn ExprPlanner> {
    Arc::new(MapFieldAccessPlanner)
}
