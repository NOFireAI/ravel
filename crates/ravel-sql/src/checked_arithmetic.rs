//! Checked integer `+`, `-` and `*` for SQL, off by default (issue #3847, a
//! spike).
//!
//! DataFusion evaluates an integer `+`, `-` or `*` with Arrow's wrapping
//! kernels unless the physical `BinaryExpr` carries `fail_on_overflow`, in
//! which case it uses the checked kernels and fails the batch with
//! `ArrowError::ArithmeticOverflow`. `/` and `%` are checked already.
//! [`CheckedIntegerArithmetic`] is a physical optimizer rule that, when
//! [`SqlConfig::checked_integer_arithmetic`] is on, rebuilds every such
//! `BinaryExpr` whose two operands are integers with the flag set, at any
//! depth of the expressions of:
//!
//! - `ProjectionExec`: every projected expression.
//! - `FilterExec`: the predicate, which is also where `HAVING` lands.
//! - `AggregateExec`: the group keys, every aggregate's arguments and
//!   `ORDER BY` within the call, and every aggregate's `FILTER`.
//! - `SortExec`: the sort keys.
//! - `BoundedWindowAggExec` and `WindowAggExec`: each window function's
//!   `PARTITION BY` and `ORDER BY`, and the arguments of an aggregate window
//!   function.
//!
//! Floating point, decimal and temporal arithmetic are left as they are. The
//! logical plan, the simplifier and every rule that matches a `BinaryExpr`
//! by shape (`crate::metadata_agg`, `crate::sum_plus`) see the plan they see
//! without the option, because the flag exists only in the physical plan.
//! The rule runs before DataFusion's own physical rules, so every operator
//! those rules build from these expressions (a pushed-down projection, a
//! TopK's dynamic filter) carries the checked form.
//!
//! With the option off the rule returns its input unchanged, so the plan is
//! the one built without it (this module's tests compare the displayed and
//! `Debug` forms of the plans). With it on, [`crate::PinnedStream`] maps an arithmetic
//! overflow to [`SqlError::IntegerArithmeticOverflow`]; with it off the
//! mapping is not applied and every error keeps today's form.
//!
//! Not covered, each leaving the wrapping form in place:
//!
//! - A `SortPreservingMergeExec`'s keys. It merges the output of a
//!   `SortExec` that evaluated the same keys checked first.
//! - A non-aggregate window function's arguments (`LAG(a + 1)`):
//!   `StandardWindowExpr` has no way to rebuild its function with new
//!   arguments.
//! - An aggregate window function with a `FILTER`: neither window
//!   aggregate type exposes its filter. A cumulative one is left whole; a
//!   sliding one is checked everywhere but in the filter.
//! - An aggregate whose evaluation order DataFusion reversed: rebuilding it
//!   would drop the reversal.
//! - Join keys and join filters, and any expression a table provider
//!   evaluates inside its own scan.
//! - Constant folding: the simplifier folds `9223372036854775807 + 1` with
//!   the wrapping kernel before any physical rule runs.
//! - Unary minus and `abs`, which are not `BinaryExpr`s: `-a` on `i64::MIN`
//!   wraps to `i64::MIN`, and `abs(a)` fails with arrow's own overflow error,
//!   reported as a generic execution error, with the option on or off.
//!
//! ES|QL statements keep null-on-overflow: the executor turns the option off
//! for a lowered statement.

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::ArrowError;
use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::window::{
    PlainAggregateWindowExpr, SlidingAggregateWindowExpr, StandardWindowExpr, WindowExpr,
};
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, PhysicalGroupBy};
use datafusion::physical_plan::filter::{FilterExec, FilterExecBuilder};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion::prelude::SessionConfig;

#[cfg(doc)]
use crate::{SqlConfig, SqlError};

/// The rule's name, as it appears in DataFusion's optimizer diagnostics.
pub const CHECKED_INTEGER_ARITHMETIC_RULE: &str = "checked_integer_arithmetic";

/// The physical optimizer rule; see the module docs.
#[derive(Debug, Clone, Copy)]
pub struct CheckedIntegerArithmetic {
    enabled: bool,
}

impl CheckedIntegerArithmetic {
    /// The rule, rewriting only when `enabled`.
    pub fn new(enabled: bool) -> Self {
        CheckedIntegerArithmetic { enabled }
    }
}

impl PhysicalOptimizerRule for CheckedIntegerArithmetic {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !self.enabled {
            return Ok(plan);
        }
        plan.transform_up(rewrite_node).map(|t| t.data)
    }

    fn name(&self) -> &str {
        CHECKED_INTEGER_ARITHMETIC_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// The session config extension that marks a session built with the option
/// on, read back by [`session_is_checked`].
#[derive(Debug)]
struct CheckedIntegerArithmeticOn;

/// `config`, marked as running checked integer arithmetic.
pub(crate) fn mark_checked(config: SessionConfig) -> SessionConfig {
    config.with_extension(Arc::new(CheckedIntegerArithmeticOn))
}

/// Whether the session behind `config` was built with the option on.
pub(crate) fn session_is_checked(config: &SessionConfig) -> bool {
    config
        .get_extension::<CheckedIntegerArithmeticOn>()
        .is_some()
}

/// Whether `err` carries Arrow's arithmetic overflow at any wrapper depth.
pub(crate) fn is_arithmetic_overflow(err: &DataFusionError) -> bool {
    match err {
        DataFusionError::ArrowError(arrow, _) => arrow_is_overflow(arrow),
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            is_arithmetic_overflow(inner)
        }
        DataFusionError::Shared(inner) => is_arithmetic_overflow(inner),
        DataFusionError::External(boxed) => boxed
            .downcast_ref::<DataFusionError>()
            .is_some_and(is_arithmetic_overflow),
        DataFusionError::Collection(errors) => errors.iter().any(is_arithmetic_overflow),
        _ => false,
    }
}

fn arrow_is_overflow(err: &ArrowError) -> bool {
    match err {
        ArrowError::ArithmeticOverflow(_) => true,
        ArrowError::ExternalError(boxed) => boxed
            .downcast_ref::<DataFusionError>()
            .is_some_and(is_arithmetic_overflow),
        _ => false,
    }
}

/// `node` with every covered expression in checked form, or `node` itself
/// when nothing changed.
fn rewrite_node(node: Arc<dyn ExecutionPlan>) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
    let rebuilt: Option<Arc<dyn ExecutionPlan>> =
        if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
            rewrite_projection(projection)?
        } else if let Some(filter) = node.downcast_ref::<FilterExec>() {
            rewrite_filter(filter)?
        } else if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
            rewrite_aggregate(aggregate)?
        } else if let Some(sort) = node.downcast_ref::<SortExec>() {
            rewrite_sort(sort)?
        } else if let Some(window) = node.downcast_ref::<BoundedWindowAggExec>() {
            rewrite_window_exprs(window.window_expr(), window.input())?
                .map(|exprs| {
                    BoundedWindowAggExec::try_new(
                        exprs,
                        Arc::clone(window.input()),
                        window.input_order_mode.clone(),
                        !window.partition_keys().is_empty(),
                    )
                })
                .transpose()?
                .map(|exec| Arc::new(exec) as _)
        } else if let Some(window) = node.downcast_ref::<WindowAggExec>() {
            rewrite_window_exprs(window.window_expr(), window.input())?
                .map(|exprs| {
                    WindowAggExec::try_new(
                        exprs,
                        Arc::clone(window.input()),
                        !window.partition_keys().is_empty(),
                    )
                })
                .transpose()?
                .map(|exec| Arc::new(exec) as _)
        } else {
            None
        };
    Ok(match rebuilt {
        Some(plan) => Transformed::yes(plan),
        None => Transformed::no(node),
    })
}

/// `expr` with every integer `+`, `-` and `*` in it checked, evaluated
/// against `schema`.
fn checked(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> DFResult<Transformed<Arc<dyn PhysicalExpr>>> {
    Arc::clone(expr).transform_up(|node| {
        let Some(binary) = node.downcast_ref::<BinaryExpr>() else {
            return Ok(Transformed::no(node));
        };
        if !matches!(
            binary.op(),
            Operator::Plus | Operator::Minus | Operator::Multiply
        ) || !binary.left().data_type(schema)?.is_integer()
            || !binary.right().data_type(schema)?.is_integer()
        {
            return Ok(Transformed::no(node));
        }
        let rebuilt = BinaryExpr::new(
            Arc::clone(binary.left()),
            *binary.op(),
            Arc::clone(binary.right()),
        )
        .with_fail_on_overflow(true);
        // `BinaryExpr`'s equality includes the flag.
        if *binary == rebuilt {
            return Ok(Transformed::no(node));
        }
        Ok(Transformed::yes(Arc::new(rebuilt)))
    })
}

/// [`checked`] over a list, with whether any of them changed.
fn checked_all(
    exprs: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
) -> DFResult<(Vec<Arc<dyn PhysicalExpr>>, bool)> {
    let mut changed = false;
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let t = checked(expr, schema)?;
        changed |= t.transformed;
        out.push(t.data);
    }
    Ok((out, changed))
}

/// [`checked`] over sort keys, with whether any of them changed.
fn checked_sort_exprs(
    exprs: &[PhysicalSortExpr],
    schema: &Schema,
) -> DFResult<(Vec<PhysicalSortExpr>, bool)> {
    let mut changed = false;
    let mut out = Vec::with_capacity(exprs.len());
    for sort in exprs {
        let t = checked(&sort.expr, schema)?;
        changed |= t.transformed;
        out.push(PhysicalSortExpr {
            expr: t.data,
            options: sort.options,
        });
    }
    Ok((out, changed))
}

fn rewrite_projection(projection: &ProjectionExec) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    let schema = projection.input().schema();
    let mut changed = false;
    let mut exprs = Vec::with_capacity(projection.expr().len());
    for projected in projection.expr() {
        let t = checked(&projected.expr, &schema)?;
        changed |= t.transformed;
        exprs.push(ProjectionExpr {
            expr: t.data,
            alias: projected.alias.clone(),
        });
    }
    if !changed {
        return Ok(None);
    }
    Ok(Some(Arc::new(
        ProjectionExec::try_new_with_schema_metadata(
            exprs,
            Arc::clone(projection.input()),
            &projection.schema(),
        )?,
    )))
}

fn rewrite_filter(filter: &FilterExec) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    let t = checked(filter.predicate(), &filter.input().schema())?;
    if !t.transformed {
        return Ok(None);
    }
    Ok(Some(Arc::new(
        FilterExecBuilder::from(filter)
            .with_predicate(t.data)
            .build()?,
    )))
}

fn rewrite_aggregate(aggregate: &AggregateExec) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    let schema = aggregate.input_schema();
    let group_by = aggregate.group_expr();
    let mut changed = false;
    let mut group_exprs = Vec::with_capacity(group_by.expr().len());
    for (expr, name) in group_by.expr() {
        let t = checked(expr, &schema)?;
        changed |= t.transformed;
        group_exprs.push((t.data, name.clone()));
    }
    let mut aggr_exprs = Vec::with_capacity(aggregate.aggr_expr().len());
    for aggr in aggregate.aggr_expr() {
        match checked_aggregate(aggr, &schema)? {
            Some(rebuilt) => {
                changed = true;
                aggr_exprs.push(rebuilt);
            }
            None => aggr_exprs.push(Arc::clone(aggr)),
        }
    }
    let mut filters = Vec::with_capacity(aggregate.filter_expr().len());
    for filter in aggregate.filter_expr() {
        filters.push(match filter {
            Some(filter) => {
                let t = checked(filter, &schema)?;
                changed |= t.transformed;
                Some(t.data)
            }
            None => None,
        });
    }
    if !changed {
        return Ok(None);
    }
    let group_by = PhysicalGroupBy::new(
        group_exprs,
        group_by.null_expr().to_vec(),
        group_by.groups().to_vec(),
        group_by.has_grouping_set(),
    );
    let rebuilt = AggregateExec::try_new(
        *aggregate.mode(),
        group_by,
        aggr_exprs,
        filters,
        Arc::clone(aggregate.input()),
        schema,
    )?
    .with_limit_options(aggregate.limit_options());
    Ok(Some(Arc::new(rebuilt)))
}

/// `aggr` with its arguments and `ORDER BY` checked, or `None` when nothing
/// in them changed or the function cannot be rebuilt.
fn checked_aggregate(
    aggr: &AggregateFunctionExpr,
    schema: &Schema,
) -> DFResult<Option<Arc<AggregateFunctionExpr>>> {
    let (args, args_changed) = checked_all(&aggr.expressions(), schema)?;
    let (order_bys, order_changed) = checked_sort_exprs(aggr.order_bys(), schema)?;
    // `with_new_expressions` resets a reversed aggregate, so one is left alone.
    if (!args_changed && !order_changed) || aggr.is_reversed() {
        return Ok(None);
    }
    let order_bys = order_bys.into_iter().map(|sort| sort.expr).collect();
    Ok(aggr.with_new_expressions(args, order_bys).map(Arc::new))
}

fn rewrite_sort(sort: &SortExec) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    let (keys, changed) = checked_sort_exprs(sort.expr(), &sort.input().schema())?;
    if !changed {
        return Ok(None);
    }
    let Some(keys) = LexOrdering::new(keys) else {
        return Ok(None);
    };
    Ok(Some(Arc::new(
        SortExec::new(keys, Arc::clone(sort.input()))
            .with_preserve_partitioning(sort.preserve_partitioning())
            .with_fetch(sort.fetch()),
    )))
}

/// The window functions of a window exec over `input`, checked, or `None`
/// when nothing in them changed.
fn rewrite_window_exprs(
    exprs: &[Arc<dyn WindowExpr>],
    input: &Arc<dyn ExecutionPlan>,
) -> DFResult<Option<Vec<Arc<dyn WindowExpr>>>> {
    let schema = input.schema();
    let mut changed = false;
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        match checked_window_expr(expr, &schema)? {
            Some(rebuilt) => {
                changed = true;
                out.push(rebuilt);
            }
            None => out.push(Arc::clone(expr)),
        }
    }
    Ok(changed.then_some(out))
}

/// One window function, checked, or `None` when nothing in it changed or it
/// is of a kind this rule cannot rebuild.
fn checked_window_expr(
    expr: &Arc<dyn WindowExpr>,
    schema: &Schema,
) -> DFResult<Option<Arc<dyn WindowExpr>>> {
    let (partition_by, partition_changed) = checked_all(expr.partition_by(), schema)?;
    let (order_by, order_changed) = checked_sort_exprs(expr.order_by(), schema)?;
    if let Some(plain) = expr.as_any().downcast_ref::<PlainAggregateWindowExpr>() {
        let original = plain.get_aggregate_expr();
        let aggregate = checked_aggregate(original, schema)?;
        if aggregate.is_none() && !partition_changed && !order_changed {
            return Ok(None);
        }
        // The type has no public accessor for its FILTER, so it is rebuilt
        // only when it provably has none: rebuilding it unchanged without a
        // filter must reproduce it.
        let unfiltered = PlainAggregateWindowExpr::new(
            Arc::new(original.clone()),
            plain.partition_by(),
            plain.order_by(),
            Arc::clone(plain.get_window_frame()),
            None,
        );
        if format!("{unfiltered:?}") != format!("{plain:?}") {
            return Ok(None);
        }
        return Ok(Some(Arc::new(PlainAggregateWindowExpr::new(
            aggregate.unwrap_or_else(|| Arc::new(original.clone())),
            &partition_by,
            &order_by,
            Arc::clone(plain.get_window_frame()),
            None,
        ))));
    }
    if let Some(sliding) = expr.as_any().downcast_ref::<SlidingAggregateWindowExpr>() {
        // `with_new_expressions` keeps the FILTER and resets a reversed
        // aggregate, so a reversed one is left alone.
        if sliding.get_aggregate_expr().is_reversed() {
            return Ok(None);
        }
        let (args, args_changed) = checked_all(&expr.expressions(), schema)?;
        if !args_changed && !partition_changed && !order_changed {
            return Ok(None);
        }
        let order_by = order_by.into_iter().map(|sort| sort.expr).collect();
        return Ok(expr.with_new_expressions(args, partition_by, order_by));
    }
    if let Some(standard) = expr.as_any().downcast_ref::<StandardWindowExpr>() {
        if !partition_changed && !order_changed {
            return Ok(None);
        }
        return Ok(Some(Arc::new(StandardWindowExpr::new(
            Arc::clone(standard.get_standard_func_expr()),
            &partition_by,
            &order_by,
            Arc::clone(standard.get_window_frame()),
        ))));
    }
    Ok(None)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::tree_node::TreeNodeRecursion;
    use datafusion::datasource::MemTable;
    use datafusion::execution::session_state::SessionStateBuilder;
    use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::SessionContext;

    /// The rule's install modes: absent, installed and off, installed and on.
    #[derive(Clone, Copy, Debug)]
    enum Rule {
        Absent,
        Off,
        On,
    }

    /// One statement per exec kind the rule rewrites, a nested expression,
    /// a TopK, and a float expression the rule must leave alone.
    const STATEMENTS: &[&str] = &[
        "SELECT a + 1 FROM t",
        "SELECT a FROM t WHERE a + 1 > b",
        "SELECT SUM(a * 2) FROM t",
        "SELECT a + 1, COUNT(*) FROM t GROUP BY a + 1",
        "SELECT a FROM t ORDER BY a - b",
        "SELECT a FROM t ORDER BY a - b LIMIT 2",
        "SELECT SUM(a) OVER (ORDER BY a + 1) FROM t",
        "SELECT SUM(a) OVER (PARTITION BY b * 2 ORDER BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        "SELECT b, SUM(a) FROM t GROUP BY b HAVING SUM(a) + 1 > 0",
        "SELECT COUNT(*) FILTER (WHERE a + 1 > 0) FROM t",
        "SELECT (a + 1) * 2 FROM t",
        "SELECT f + 1.5 FROM t",
    ];

    fn table(a: Vec<i64>, b: Vec<i64>) -> MemTable {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("f", DataType::Float64, false),
        ]));
        let f = a.iter().map(|&v| v as f64).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(a)),
                Arc::new(Int64Array::from(b)),
                Arc::new(Float64Array::from(f)),
            ],
        )
        .expect("batch");
        MemTable::try_new(schema, vec![vec![batch]]).expect("table")
    }

    fn context(rule: Rule, t: MemTable) -> SessionContext {
        let mut builder = SessionStateBuilder::new().with_default_features();
        let enabled = match rule {
            Rule::Absent => None,
            Rule::Off => Some(false),
            Rule::On => Some(true),
        };
        if let Some(enabled) = enabled {
            let rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> =
                std::iter::once(Arc::new(CheckedIntegerArithmetic::new(enabled))
                    as Arc<dyn PhysicalOptimizerRule + Send + Sync>)
                .chain(PhysicalOptimizer::new().rules)
                .collect();
            builder = builder.with_physical_optimizer_rules(rules);
        }
        let ctx = SessionContext::new_with_state(builder.build());
        ctx.register_table("t", Arc::new(t)).expect("register");
        ctx
    }

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime")
    }

    async fn plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql)
            .await
            .expect("logical plan")
            .create_physical_plan()
            .await
            .expect("physical plan")
    }

    /// The expressions each exec kind the rule rewrites evaluates, with the
    /// schema they are evaluated against.
    fn node_exprs(plan: &dyn ExecutionPlan) -> Vec<(Arc<dyn PhysicalExpr>, Arc<Schema>)> {
        let Some(input) = plan.children().first().map(|child| child.schema()) else {
            return Vec::new();
        };
        let mut exprs: Vec<Arc<dyn PhysicalExpr>> = Vec::new();
        if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
            exprs.extend(projection.expr().iter().map(|e| Arc::clone(&e.expr)));
        } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
            exprs.push(Arc::clone(filter.predicate()));
        } else if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
            exprs.extend(
                aggregate
                    .group_expr()
                    .expr()
                    .iter()
                    .map(|(e, _)| Arc::clone(e)),
            );
            for aggr in aggregate.aggr_expr() {
                exprs.extend(aggr.expressions());
            }
            exprs.extend(aggregate.filter_expr().iter().flatten().cloned());
        } else if let Some(sort) = plan.downcast_ref::<SortExec>() {
            exprs.extend(sort.expr().iter().map(|s| Arc::clone(&s.expr)));
        } else {
            let window_exprs = if let Some(w) = plan.downcast_ref::<BoundedWindowAggExec>() {
                w.window_expr()
            } else if let Some(w) = plan.downcast_ref::<WindowAggExec>() {
                w.window_expr()
            } else {
                &[]
            };
            for w in window_exprs {
                exprs.extend(w.expressions());
                exprs.extend(w.partition_by().iter().cloned());
                exprs.extend(w.order_by().iter().map(|s| Arc::clone(&s.expr)));
            }
        }
        exprs.into_iter().map(|e| (e, Arc::clone(&input))).collect()
    }

    /// Integer `+`, `-`, `*` in the rewritten execs of `plan`, as (checked,
    /// unchecked). `BinaryExpr` has no getter for its flag; its equality
    /// includes it.
    fn arithmetic(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize) {
        let mut counts = (0, 0);
        plan.apply(|node| {
            for (expr, schema) in node_exprs(node.as_ref()) {
                expr.apply(|e| {
                    if let Some(b) = e.downcast_ref::<BinaryExpr>()
                        && matches!(
                            b.op(),
                            Operator::Plus | Operator::Minus | Operator::Multiply
                        )
                        && e.data_type(&schema)?.is_integer()
                    {
                        let flagged =
                            BinaryExpr::new(Arc::clone(b.left()), *b.op(), Arc::clone(b.right()))
                                .with_fail_on_overflow(true);
                        if *b == flagged {
                            counts.0 += 1;
                        } else {
                            counts.1 += 1;
                        }
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("walk");
        counts
    }

    fn shown(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    /// `plan`'s `Debug` form without the dynamic filters' process-wide
    /// expression ids, which differ between any two plannings.
    fn debug_form(plan: &Arc<dyn ExecutionPlan>) -> String {
        let text = format!("{plan:?}");
        let mut out = String::with_capacity(text.len());
        let mut rest = text.as_str();
        while let Some(at) = rest.find("expression_id: ") {
            let (head, tail) = rest.split_at(at + "expression_id: ".len());
            out.push_str(head);
            rest = tail.trim_start_matches(|c: char| c.is_ascii_digit());
        }
        out.push_str(rest);
        out
    }

    /// With the option off, the rule leaves every plan as DataFusion built it:
    /// the displayed plan and the full `Debug` form are byte-identical to the
    /// plan built without the rule installed, and no expression is checked.
    #[test]
    fn off_plans_are_byte_identical_to_plans_without_the_rule() {
        runtime().block_on(async {
            for sql in STATEMENTS {
                let absent = plan(&context(Rule::Absent, table(vec![1], vec![2])), sql).await;
                let off = plan(&context(Rule::Off, table(vec![1], vec![2])), sql).await;
                assert_eq!(shown(&absent), shown(&off), "{sql}");
                assert_eq!(debug_form(&absent), debug_form(&off), "{sql}");
                assert_eq!(arithmetic(&off).0, 0, "{sql}");
            }
        });
    }

    /// With the option on, the displayed plan is unchanged, the `Debug` form
    /// differs only in the flag, and every integer `+`, `-`, `*` in the
    /// rewritten execs carries the flag; the float statement has none.
    #[test]
    fn on_checks_every_integer_operator_and_keeps_the_plan_shape() {
        runtime().block_on(async {
            for sql in STATEMENTS {
                let off = plan(&context(Rule::Off, table(vec![1], vec![2])), sql).await;
                let on = plan(&context(Rule::On, table(vec![1], vec![2])), sql).await;
                assert_eq!(shown(&off), shown(&on), "{sql}");
                assert_eq!(
                    debug_form(&on).replace("fail_on_overflow: true", "fail_on_overflow: false"),
                    debug_form(&off),
                    "{sql}"
                );
                let (checked, unchecked) = arithmetic(&on);
                assert_eq!(unchecked, 0, "{sql}");
                let (_, before) = arithmetic(&off);
                assert_eq!(checked, before, "{sql}");
                if sql.contains("f + 1.5") {
                    assert_eq!(checked, 0, "{sql}");
                } else {
                    assert!(checked > 0, "{sql}");
                }
            }
        });
    }

    /// The rule is idempotent: a second pass over a checked plan changes
    /// nothing.
    #[test]
    fn a_second_pass_is_a_no_op() {
        runtime().block_on(async {
            let ctx = context(Rule::On, table(vec![1], vec![2]));
            for sql in STATEMENTS {
                let once = plan(&ctx, sql).await;
                let twice = CheckedIntegerArithmetic::new(true)
                    .optimize(Arc::clone(&once), &ConfigOptions::default())
                    .expect("second pass");
                assert_eq!(format!("{once:?}"), format!("{twice:?}"), "{sql}");
            }
        });
    }

    /// Execution: on fails with an arithmetic overflow that
    /// [`is_arithmetic_overflow`] recognises; off returns the wrapped value.
    #[test]
    fn on_fails_where_off_wraps() {
        runtime().block_on(async {
            let sql = "SELECT (a + 1) - b FROM t";
            let off_ctx = context(Rule::Off, table(vec![i64::MAX], vec![-1]));
            let batches = collect(plan(&off_ctx, sql).await, off_ctx.task_ctx())
                .await
                .expect("off wraps");
            let column = batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64");
            assert_eq!(column.value(0), i64::MIN + 1);

            let on_ctx = context(Rule::On, table(vec![i64::MAX], vec![-1]));
            let err = collect(plan(&on_ctx, sql).await, on_ctx.task_ctx())
                .await
                .expect_err("on fails");
            assert!(is_arithmetic_overflow(&err), "{err}");
        });
    }
}
