//! Checked integer `+`, `-` and `*` for SQL (issue #3847), behind
//! [`crate::SqlConfig::checked_integer_arithmetic`], off by default.
//!
//! DataFusion evaluates an integer `BinaryExpr` with Arrow's wrapping kernels
//! unless the expression carries `fail_on_overflow`, in which case it uses the
//! checked kernels and fails the batch with `ArrowError::ArithmeticOverflow`.
//! [`CheckedIntegerArithmetic`] is a physical optimizer rule that rebuilds
//! every `+`, `-` and `*` `BinaryExpr` whose operands are both integers with
//! that flag set, in the expressions these operators evaluate:
//!
//! - `ProjectionExec`: every output expression.
//! - `FilterExec`: the predicate, which is also where `HAVING` runs.
//! - `AggregateExec`: group keys, aggregate arguments and ordering
//!   expressions, and `FILTER` clauses, in every mode, so a partial and its
//!   final aggregate still compare equal.
//! - `SortExec`: the sort keys.
//! - `WindowAggExec` and `BoundedWindowAggExec`: partition and order keys,
//!   and the arguments and `FILTER` clause of an aggregate window function.
//!
//! The rule runs first, on the physical planner's output, so every later
//! rule (sort enforcement, filter pushdown into a scan, Ravel's own
//! rewrites) carries the flagged expressions forward: `BinaryExpr` keeps the
//! flag through `with_new_children`. With the option off the rule is still
//! installed and returns its input unchanged.
//!
//! The executor maps an `ArithmeticOverflow` to
//! [`crate::SqlError::IntegerArithmeticOverflow`] only when the option is on
//! (the session carries [`CheckedIntegerArithmeticEnabled`]).
//!
//! Not covered: arithmetic the logical optimizer folds to a constant before
//! the physical plan exists (it wraps), the arguments of a non-aggregate
//! window function such as `lag(a + 1)` (refused at planning, since the
//! expression cannot be rebuilt with its `IGNORE NULLS` setting intact), join
//! conditions, and `SortPreservingMergeExec` keys, which the sort below it
//! already evaluated. `SUM(x + c)` is rewritten to an exact per-group sum
//! before physical planning (`crate::sum_plus`), so its per-row `x + c` is
//! never evaluated and cannot fail; only the total can overflow, with
//! [`crate::SqlError::IntegerSumOverflow`].

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::BinaryExpr;
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
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

/// The rule's name, as `PhysicalOptimizerRule::name` reports it.
pub const CHECKED_INTEGER_ARITHMETIC_RULE: &str = "checked_integer_arithmetic";

/// Session config extension present when the option is on; read by the
/// executor to decide whether an Arrow overflow is this mode's error.
#[derive(Debug, Default)]
pub struct CheckedIntegerArithmeticEnabled;

/// Physical optimizer rule; see the module docs.
#[derive(Debug)]
pub struct CheckedIntegerArithmetic {
    enabled: bool,
}

impl CheckedIntegerArithmetic {
    pub fn new(enabled: bool) -> Self {
        CheckedIntegerArithmetic { enabled }
    }
}

impl PhysicalOptimizerRule for CheckedIntegerArithmetic {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !self.enabled {
            return Ok(plan);
        }
        // The physical planner's own choice for every window exec it builds.
        let can_repartition_windows =
            config.execution.target_partitions > 1 && config.optimizer.repartition_windows;
        plan.transform_up(|node| rewrite_node(node, can_repartition_windows))
            .data()
    }

    fn name(&self) -> &str {
        CHECKED_INTEGER_ARITHMETIC_RULE
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn rewrite_node(
    node: Arc<dyn ExecutionPlan>,
    can_repartition_windows: bool,
) -> DFResult<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(projection) = node.downcast_ref::<ProjectionExec>() {
        let schema = projection.input().schema();
        let mut changed = false;
        let exprs = projection
            .expr()
            .iter()
            .map(|p| {
                let expr = check(&p.expr, &schema, &mut changed)?;
                Ok(ProjectionExpr::new(expr, p.alias.clone()))
            })
            .collect::<DFResult<Vec<_>>>()?;
        if !changed {
            return Ok(Transformed::no(node));
        }
        let rebuilt = ProjectionExec::try_new(exprs, Arc::clone(projection.input()))?;
        return Ok(Transformed::yes(Arc::new(rebuilt)));
    }
    if let Some(filter) = node.downcast_ref::<FilterExec>() {
        let mut changed = false;
        let predicate = check(filter.predicate(), &filter.input().schema(), &mut changed)?;
        if !changed {
            return Ok(Transformed::no(node));
        }
        let rebuilt = FilterExecBuilder::from(filter)
            .with_predicate(predicate)
            .build()?;
        return Ok(Transformed::yes(Arc::new(rebuilt)));
    }
    if let Some(aggregate) = node.downcast_ref::<AggregateExec>() {
        return rewrite_aggregate(aggregate).map(|rebuilt| match rebuilt {
            Some(rebuilt) => Transformed::yes(rebuilt),
            None => Transformed::no(node),
        });
    }
    if let Some(sort) = node.downcast_ref::<SortExec>() {
        let mut changed = false;
        let ordering = check_ordering(sort.expr(), &sort.input().schema(), &mut changed)?;
        if !changed {
            return Ok(Transformed::no(node));
        }
        let ordering = LexOrdering::new(ordering)
            .ok_or_else(|| DataFusionError::Internal("a sort with no keys".to_string()))?;
        let rebuilt = SortExec::new(ordering, Arc::clone(sort.input()))
            .with_preserve_partitioning(sort.preserve_partitioning())
            .with_fetch(sort.fetch());
        return Ok(Transformed::yes(Arc::new(rebuilt)));
    }
    if let Some(window) = node.downcast_ref::<WindowAggExec>() {
        let Some(exprs) = rewrite_window_exprs(window.window_expr(), &window.input().schema())?
        else {
            return Ok(Transformed::no(node));
        };
        let rebuilt =
            WindowAggExec::try_new(exprs, Arc::clone(window.input()), can_repartition_windows)?;
        return Ok(Transformed::yes(Arc::new(rebuilt)));
    }
    if let Some(window) = node.downcast_ref::<BoundedWindowAggExec>() {
        let Some(exprs) = rewrite_window_exprs(window.window_expr(), &window.input().schema())?
        else {
            return Ok(Transformed::no(node));
        };
        let rebuilt = BoundedWindowAggExec::try_new(
            exprs,
            Arc::clone(window.input()),
            window.input_order_mode.clone(),
            can_repartition_windows,
        )?;
        return Ok(Transformed::yes(Arc::new(rebuilt)));
    }
    Ok(Transformed::no(node))
}

fn rewrite_aggregate(aggregate: &AggregateExec) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
    let mut changed = false;
    // Group keys evaluate against the exec's actual input; arguments and
    // filters against `input_schema`, which a final aggregate keeps as the
    // partial's input so the two compare equal.
    let input = aggregate.input().schema();
    let input_schema = aggregate.input_schema();
    let group_by = aggregate.group_expr();
    let mut check_group = |exprs: &[(Arc<dyn PhysicalExpr>, String)]| {
        exprs
            .iter()
            .map(|(expr, name)| Ok((check(expr, &input, &mut changed)?, name.clone())))
            .collect::<DFResult<Vec<_>>>()
    };
    let group_exprs = check_group(group_by.expr())?;
    let null_exprs = check_group(group_by.null_expr())?;
    let group_by = PhysicalGroupBy::new(
        group_exprs,
        null_exprs,
        group_by.groups().to_vec(),
        group_by.has_grouping_set(),
    );
    let mut aggr_exprs = Vec::with_capacity(aggregate.aggr_expr().len());
    for aggr in aggregate.aggr_expr() {
        let all = aggr.all_expressions();
        let mut this_changed = false;
        let args = check_all(&all.args, &input_schema, &mut this_changed)?;
        let order_by = check_all(&all.order_by_exprs, &input_schema, &mut this_changed)?;
        if this_changed {
            let rebuilt = aggr
                .with_new_expressions(args, order_by)
                .ok_or_else(|| cannot_rewrite(aggr.name()))?;
            aggr_exprs.push(Arc::new(rebuilt));
            changed = true;
        } else {
            aggr_exprs.push(Arc::clone(aggr));
        }
    }
    let filter_exprs = aggregate
        .filter_expr()
        .iter()
        .map(|filter| {
            filter
                .as_ref()
                .map(|filter| check(filter, &input_schema, &mut changed))
                .transpose()
        })
        .collect::<DFResult<Vec<_>>>()?;
    if !changed {
        return Ok(None);
    }
    // `try_new` is the physical planner's own constructor; the rule runs
    // before any rule sets a limit, so `with_limit_options` restores nothing
    // today and keeps the rewrite safe if it ever runs later.
    let rebuilt = AggregateExec::try_new(
        *aggregate.mode(),
        group_by,
        aggr_exprs,
        filter_exprs,
        Arc::clone(aggregate.input()),
        input_schema,
    )?
    .with_limit_options(aggregate.limit_options());
    Ok(Some(Arc::new(rebuilt)))
}

/// The window expressions with their integer arithmetic flagged, or `None`
/// when none of them holds any.
fn rewrite_window_exprs(
    exprs: &[Arc<dyn WindowExpr>],
    schema: &Schema,
) -> DFResult<Option<Vec<Arc<dyn WindowExpr>>>> {
    let mut any = false;
    let mut out = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let mut changed = false;
        let partition_by = check_all(expr.partition_by(), schema, &mut changed)?;
        let order_by = check_ordering(expr.order_by(), schema, &mut changed)?;
        let rebuilt: Arc<dyn WindowExpr> =
            if expr.as_any().is::<SlidingAggregateWindowExpr>() {
                let args = check_all(&expr.expressions(), schema, &mut changed)?;
                if !changed {
                    out.push(Arc::clone(expr));
                    continue;
                }
                // Keeps the window `FILTER` clause as it was.
                let order_by = order_by.into_iter().map(|sort| sort.expr).collect();
                expr.with_new_expressions(args, partition_by, order_by)
                    .ok_or_else(|| cannot_rewrite(expr.name()))?
            } else if let Some(plain) = expr.as_any().downcast_ref::<PlainAggregateWindowExpr>() {
                let aggregate = plain.get_aggregate_expr();
                let args = check_all(&aggregate.expressions(), schema, &mut changed)?;
                if !changed {
                    out.push(Arc::clone(expr));
                    continue;
                }
                // DataFusion 55 exposes no rewrite and no getter for a plain
                // aggregate window's `FILTER` clause, so one is rebuilt only
                // when its Debug form shows it has none.
                if !format!("{plain:?}").ends_with("filter: None }") {
                    return Err(cannot_rewrite(expr.name()));
                }
                let aggregate = rebuild_window_aggregate(aggregate, args)?;
                Arc::new(PlainAggregateWindowExpr::new(
                    aggregate,
                    &partition_by,
                    &order_by,
                    Arc::clone(expr.get_window_frame()),
                    None,
                ))
            } else if let Some(standard) = expr.as_any().downcast_ref::<StandardWindowExpr>() {
                let mut args_changed = false;
                check_all(&expr.expressions(), schema, &mut args_changed)?;
                if args_changed {
                    // `WindowUDFExpr` has no getter for its `IGNORE NULLS`
                    // setting, so its arguments cannot be rebuilt faithfully.
                    return Err(cannot_rewrite(expr.name()));
                }
                if !changed {
                    out.push(Arc::clone(expr));
                    continue;
                }
                Arc::new(StandardWindowExpr::new(
                    Arc::clone(standard.get_standard_func_expr()),
                    &partition_by,
                    &order_by,
                    Arc::clone(expr.get_window_frame()),
                ))
            } else {
                let mut args_changed = false;
                check_all(&expr.expressions(), schema, &mut args_changed)?;
                if args_changed || changed {
                    return Err(cannot_rewrite(expr.name()));
                }
                out.push(Arc::clone(expr));
                continue;
            };
        any = true;
        out.push(rebuilt);
    }
    Ok(any.then_some(out))
}

fn rebuild_window_aggregate(
    aggregate: &AggregateFunctionExpr,
    args: Vec<Arc<dyn PhysicalExpr>>,
) -> DFResult<Arc<AggregateFunctionExpr>> {
    let order_by = aggregate.all_expressions().order_by_exprs;
    aggregate
        .with_new_expressions(args, order_by)
        .map(Arc::new)
        .ok_or_else(|| cannot_rewrite(aggregate.name()))
}

/// The planning error for an expression holding integer arithmetic the rule
/// cannot flag: refused rather than left wrapping.
fn cannot_rewrite(name: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "checked integer arithmetic cannot rewrite the integer +, - or * in {name}"
    ))
}

fn check_ordering(
    ordering: &[PhysicalSortExpr],
    schema: &Schema,
    changed: &mut bool,
) -> DFResult<Vec<PhysicalSortExpr>> {
    ordering
        .iter()
        .map(|sort| Ok(PhysicalSortExpr::new(check(&sort.expr, schema, changed)?, sort.options)))
        .collect()
}

fn check_all(
    exprs: &[Arc<dyn PhysicalExpr>],
    schema: &Schema,
    changed: &mut bool,
) -> DFResult<Vec<Arc<dyn PhysicalExpr>>> {
    exprs.iter().map(|expr| check(expr, schema, changed)).collect()
}

/// `expr` with every integer `+`, `-` and `*` at any depth rebuilt with
/// `fail_on_overflow`; sets `changed` when it rebuilt one.
fn check(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
    changed: &mut bool,
) -> DFResult<Arc<dyn PhysicalExpr>> {
    let rewritten = Arc::clone(expr).transform_up(|node| {
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
        let flagged = BinaryExpr::new(
            Arc::clone(binary.left()),
            *binary.op(),
            Arc::clone(binary.right()),
        )
        .with_fail_on_overflow(true);
        Ok(Transformed::yes(Arc::new(flagged) as Arc<dyn PhysicalExpr>))
    })?;
    *changed |= rewritten.transformed;
    Ok(rewritten.data)
}
