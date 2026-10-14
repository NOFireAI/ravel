//! A bound [`Pipeline`] to a logical plan, against one [`LoweringContext`].
//!
//! Every function call carries the context's own `Arc` for the name the
//! pipeline recorded, which is what plan admission checks.

use datafusion::common::{Column, ScalarValue};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::{AggregateFunction, Case, InList, ScalarFunction};
use datafusion::logical_expr::{
    Cast, Expr, LogicalPlan, LogicalPlanBuilder, SortExpr, TryCast, binary_expr, lit,
};

use super::ir::{BAggregate, BExpr, Pipeline, Source, Stage};
use crate::session::LOGS_TABLE;
use crate::statement::LoweringContext;

pub(crate) fn lower(
    pipeline: &Pipeline,
    ctx: &LoweringContext,
) -> Result<LogicalPlan, DataFusionError> {
    let mut builder = match pipeline.source {
        Source::Logs => ctx.scan(LOGS_TABLE)?,
        Source::Row => LogicalPlanBuilder::values(vec![vec![lit(ScalarValue::Null)]])?,
    };
    for stage in &pipeline.stages {
        builder = match stage {
            Stage::Project(columns) => builder.project(
                columns
                    .iter()
                    .map(|(e, name)| Ok(named(expr(e, ctx)?, name)))
                    .collect::<Result<Vec<_>, DataFusionError>>()?,
            )?,
            Stage::Filter(condition) => builder.filter(expr(condition, ctx)?)?,
            Stage::Aggregate { groups, aggregates } => builder.aggregate(
                groups
                    .iter()
                    .map(|(e, name)| Ok(expr(e, ctx)?.alias(name)))
                    .collect::<Result<Vec<_>, DataFusionError>>()?,
                aggregates
                    .iter()
                    .map(|(call, name)| Ok(aggregate(call, ctx)?.alias(name)))
                    .collect::<Result<Vec<_>, DataFusionError>>()?,
            )?,
            Stage::Sort(keys) => builder.sort(
                keys.iter()
                    .map(|key| {
                        Ok(SortExpr::new(
                            expr(&key.expr, ctx)?,
                            key.asc,
                            key.nulls_first,
                        ))
                    })
                    .collect::<Result<Vec<_>, DataFusionError>>()?,
            )?,
            Stage::Limit(count) => {
                builder.limit(0, Some(usize::try_from(*count).unwrap_or(usize::MAX)))?
            }
        };
    }
    builder
        .project(
            pipeline
                .output
                .iter()
                .map(|(internal, name)| Expr::Column(Column::from_name(internal)).alias(name)),
        )?
        .build()
}

/// `expr` under `name`, leaving a column that already has that name bare:
/// DataFusion's leaf-expression pushdown fails on a projection that aliases
/// a column to its own name.
fn named(expr: Expr, name: &str) -> Expr {
    match &expr {
        Expr::Column(column) if column.relation.is_none() && column.name == name => expr,
        _ => expr.alias(name),
    }
}

fn aggregate(call: &BAggregate, ctx: &LoweringContext) -> Result<Expr, DataFusionError> {
    let args = call
        .args
        .iter()
        .map(|arg| expr(arg, ctx))
        .collect::<Result<Vec<_>, _>>()?;
    let filter = match &call.filter {
        Some(filter) => Some(Box::new(expr(filter, ctx)?)),
        None => None,
    };
    Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
        ctx.aggregate(call.function)?,
        args,
        false,
        filter,
        vec![],
        None,
    )))
}

fn expr(e: &BExpr, ctx: &LoweringContext) -> Result<Expr, DataFusionError> {
    let boxed = |e: &BExpr| expr(e, ctx).map(Box::new);
    Ok(match e {
        BExpr::Column(name) => Expr::Column(Column::from_name(name)),
        BExpr::Literal(value) => lit(value.clone()),
        BExpr::Scalar(name, args) => Expr::ScalarFunction(ScalarFunction::new_udf(
            ctx.scalar(name)?,
            args.iter()
                .map(|arg| expr(arg, ctx))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        BExpr::Cast(inner, to) => Expr::Cast(Cast::new(boxed(inner)?, to.clone())),
        BExpr::TryCast(inner, to) => Expr::TryCast(TryCast::new(boxed(inner)?, to.clone())),
        BExpr::Binary(left, op, right) => binary_expr(expr(left, ctx)?, *op, expr(right, ctx)?),
        BExpr::Not(inner) => Expr::Not(boxed(inner)?),
        BExpr::Negative(inner) => Expr::Negative(boxed(inner)?),
        BExpr::IsNull(inner, false) => Expr::IsNull(boxed(inner)?),
        BExpr::IsNull(inner, true) => Expr::IsNotNull(boxed(inner)?),
        BExpr::InList(value, list, negated) => Expr::InList(InList::new(
            boxed(value)?,
            list.iter()
                .map(|item| expr(item, ctx))
                .collect::<Result<Vec<_>, _>>()?,
            *negated,
        )),
        BExpr::Case(arms, otherwise) => Expr::Case(Case::new(
            None,
            arms.iter()
                .map(|(when, then)| Ok((boxed(when)?, boxed(then)?)))
                .collect::<Result<Vec<_>, DataFusionError>>()?,
            match otherwise {
                Some(otherwise) => Some(boxed(otherwise)?),
                None => None,
            },
        )),
    })
}
