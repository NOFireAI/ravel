//! ES|QL over the `logs` table (ADR-2887): bind a statement parsed by
//! [`ravel_esql::parse`] and lower it to a DataFusion logical plan that
//! [`execute`] runs through the [`SqlExecutor`](crate::SqlExecutor).
//!
//! No HTTP handler calls [`prepare`] yet.
//!
//! [`prepare`] parses and binds once, before any session exists, and fixes the
//! statement's output columns and their ES|QL types and the time window read
//! from its `@timestamp` bounds. Every statement carries the same
//! [`DialectPolicy`], the fixed function list below (ADR-2887 D7); `prepare`
//! refuses a bound pipeline calling anything off it with an internal error.
//! [`PreparedEsql`] then lowers the same bound pipeline against every session
//! the executor builds for it.
//!
//! A `FROM logs` statement reads its window from the conjunctive `@timestamp`
//! bounds of the `WHERE` stages ahead of the first `LIMIT`, `STATS` or stage
//! that redefines `@timestamp`, intersected with the request's own start and
//! end. With no upper bound from either, it ends at the query clock; with no
//! lower bound from either, it starts at the epoch. The window selects the
//! segments the catalog resolves and is also applied as a row filter, so the
//! bounds it fills in are filters: see the `window` module for what that
//! excludes. Every `WHERE` stays a filter as well.
//!
//! Integer overflow, an integer division or remainder by zero, and a double
//! result that is not finite all read null, as in ES|QL. ES|QL also attaches
//! a warning to the response for each; this lowering has no warning channel,
//! so those nulls arrive without one.
//!
//! A statement ending in its own `LIMIT` returns at most [`MAX_LIMIT`] rows,
//! and one that does not returns at most [`IMPLICIT_LIMIT`]. A statement that
//! binds to more than [`MAX_BOUND_NODES`] expression nodes, to an expression
//! deeper than [`MAX_EXPRESSION_DEPTH`] (the conditions of every `WHERE` up to
//! a `LIMIT` counted as one), or to more than [`MAX_PLAN_STAGES`] stages of its
//! own commands is refused with a verification error naming the command and
//! the limit, so every statement the parser's default limits admit plans and
//! runs within a worker thread's stack. A lowered statement DataFusion still
//! cannot plan is refused with [`EsqlError::Plan`].

mod binder;
mod error;
mod ir;
mod lower;
mod run;
mod types;
mod window;

use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use ravel_types::TimeRange;

pub use error::EsqlError;
pub use run::{EsqlOutcome, EsqlRequest, EsqlRun, execute};
pub use types::{EsqlColumn, EsqlType};

pub use binder::{
    IMPLICIT_LIMIT, MAX_BOUND_NODES, MAX_EXPRESSION_DEPTH, MAX_LIMIT, MAX_PLAN_STAGES,
};

use crate::declared::DeclaredColumn;
use crate::executor::{StatementTables, TargetSignal};
use crate::session::LOGS_TABLE;
use crate::statement::{
    DialectPolicy, LoweredStatement, LoweringContext, StatementTarget, admit_lowered_plan,
};
use ir::{Pipeline, Source};

/// What binding reads from the request, besides the statement text.
#[derive(Debug, Clone, Default)]
pub struct PrepareContext {
    /// The tenant's declared log columns, in table order.
    pub declared: Vec<DeclaredColumn>,
    /// The query clock: `NOW()`, and the end of a window with no upper bound.
    pub now_ns: i64,
    /// The request's inclusive start, in nanoseconds.
    pub start: Option<i64>,
    /// The request's exclusive end, in nanoseconds.
    pub end: Option<i64>,
}

/// A parsed and bound ES|QL statement, ready to run.
///
/// It runs only through [`execute`], which routes on its
/// [`window`](Self::window). Its [`target`](LoweredStatement::target) names
/// the `logs` signal for a `ROW` statement too, while
/// [`needs_catalog`](Self::needs_catalog) is false for one: [`execute`] runs
/// a `ROW` statement in a session that registers no table, and it reads
/// neither [`EsqlRequest::budgets`] nor [`EsqlRequest::max_rows`].
#[derive(Debug, Clone)]
pub struct PreparedEsql {
    text: String,
    pipeline: Pipeline,
    policy: DialectPolicy,
    window: Option<TimeRange>,
    columns: Vec<EsqlColumn>,
    now_ns: i64,
    clamped_limit: Option<u64>,
}

/// The scalar functions an ES|QL lowering may call, by registered name.
const SCALARS: [&str; 4] = ["get_field", "encode", "date_bin", "nullif"];

/// The aggregate functions an ES|QL lowering may call, by registered name.
const AGGREGATES: [&str; 5] = ["count", "sum", "min", "max", "avg"];

/// Refuse a bound pipeline calling a function off [`SCALARS`] or
/// [`AGGREGATES`]. The binder only produces listed names, so a refusal here
/// is a binder defect, reported as an internal error.
fn check_functions(pipeline: &Pipeline) -> Result<(), EsqlError> {
    let (scalars, aggregates) = pipeline.functions();
    let off = scalars
        .iter()
        .find(|name| !SCALARS.contains(name))
        .map(|name| ("scalar", *name))
        .or_else(|| {
            aggregates
                .iter()
                .find(|name| !AGGREGATES.contains(name))
                .map(|name| ("aggregate", *name))
        });
    match off {
        Some((kind, name)) => Err(EsqlError::Execution(crate::SqlError::Internal(format!(
            "the ES|QL binder produced the {kind} function {name}, which the ES|QL function list does not hold"
        )))),
        None => Ok(()),
    }
}

/// Parse and bind `source`.
pub fn prepare(
    source: &str,
    limits: &ravel_esql::Limits,
    ctx: &PrepareContext,
) -> Result<PreparedEsql, EsqlError> {
    let query = ravel_esql::parse(source, limits).map_err(EsqlError::Parse)?;
    let bound = binder::bind(
        &query,
        &binder::BindInput {
            source,
            declared: &ctx.declared,
            now_ns: ctx.now_ns,
            start: ctx.start,
            end: ctx.end,
        },
    )?;
    check_functions(&bound.pipeline)?;
    let policy = DialectPolicy::new(SCALARS, AGGREGATES, std::iter::empty::<String>());
    let window = bound
        .window
        .map(|window| window.resolve(ctx.now_ns, ctx.start, ctx.end));
    Ok(PreparedEsql {
        text: source.to_string(),
        pipeline: bound.pipeline,
        policy,
        window,
        columns: bound.columns,
        now_ns: ctx.now_ns,
        clamped_limit: bound.clamped_limit,
    })
}

impl PreparedEsql {
    /// The half-open window `[start_ns, end_ns)` the catalog resolves and the
    /// rows are filtered to, or `None` for a `ROW` statement.
    pub fn window(&self) -> Option<TimeRange> {
        self.window
    }

    /// Whether the statement reads the `logs` table, and so needs a resolved
    /// catalog snapshot. A `ROW` statement does not.
    pub fn needs_catalog(&self) -> bool {
        self.pipeline.source == Source::Logs
    }

    /// The result columns, in order, with their ES|QL types.
    pub fn columns(&self) -> &[EsqlColumn] {
        &self.columns
    }

    /// The statement's own final `LIMIT`, when it was above [`MAX_LIMIT`]
    /// and the statement returns at most [`MAX_LIMIT`] rows instead.
    pub fn clamped_limit(&self) -> Option<u64> {
        self.clamped_limit
    }

    /// The statement's logical plan in `session`, lowered and admitted as the
    /// executor would, then optimized by the session. `session` must hold the
    /// `logs` table for a `FROM` statement.
    pub async fn optimized_plan(&self, session: &SessionContext) -> Result<LogicalPlan, EsqlError> {
        let providers = if self.needs_catalog() {
            let provider = session
                .table_provider(LOGS_TABLE)
                .await
                .map_err(|e| EsqlError::Plan(e.to_string()))?;
            vec![(LOGS_TABLE.to_string(), provider)]
        } else {
            Vec::new()
        };
        let tables = self.tables();
        let context = LoweringContext::snapshot(session, providers, &tables);
        let plan =
            lower::lower(&self.pipeline, &context).map_err(|e| EsqlError::Plan(e.to_string()))?;
        admit_lowered_plan(&plan, &context, &tables, &self.policy)
            .map_err(EsqlError::from_executor)?;
        session
            .state()
            .optimize(&plan)
            .map_err(|e| EsqlError::Plan(e.to_string()))
    }

    fn tables(&self) -> StatementTables {
        StatementTables {
            signal: self.needs_catalog().then_some(TargetSignal::Logs),
            others: Default::default(),
        }
    }
}

impl crate::statement::sealed::Sealed for PreparedEsql {}

#[async_trait]
impl LoweredStatement for PreparedEsql {
    /// The `logs` signal, for a `ROW` statement as well, which reads no
    /// catalog; [`execute`] does not run a `ROW` statement over this target.
    fn target(&self) -> StatementTarget {
        StatementTarget::Signal(TargetSignal::Logs)
    }

    fn text(&self) -> &str {
        &self.text
    }

    fn policy(&self) -> &DialectPolicy {
        &self.policy
    }

    async fn lower(&self, ctx: &LoweringContext) -> Result<LogicalPlan, DataFusionError> {
        lower::lower(&self.pipeline, ctx)
    }
}

#[cfg(test)]
#[allow(clippy::panic)]
mod tests {
    use datafusion::common::ScalarValue;

    use super::*;
    use ir::{BAggregate, BExpr, Stage};

    fn pipeline(stages: Vec<Stage>) -> Pipeline {
        Pipeline {
            source: Source::Row,
            stages,
            output: Vec::new(),
        }
    }

    fn internal(result: Result<(), EsqlError>) -> String {
        match result {
            Err(EsqlError::Execution(crate::SqlError::Internal(message))) => message,
            other => panic!("expected an internal error, got {other:?}"),
        }
    }

    #[test]
    fn a_pipeline_calling_a_function_off_the_list_is_refused() {
        let literal = BExpr::Literal(ScalarValue::Utf8(Some("a".to_string())));
        let scalar = pipeline(vec![Stage::Project(vec![(
            BExpr::Scalar("upper", vec![literal.clone()]),
            "_e1".to_string(),
        )])]);
        assert_eq!(
            internal(check_functions(&scalar)),
            "the ES|QL binder produced the scalar function upper, which the ES|QL function list does not hold"
        );

        let aggregate = pipeline(vec![Stage::Aggregate {
            groups: Vec::new(),
            aggregates: vec![(
                BAggregate {
                    function: "median",
                    args: vec![literal.clone()],
                    filter: None,
                },
                "_e1".to_string(),
            )],
        }]);
        assert_eq!(
            internal(check_functions(&aggregate)),
            "the ES|QL binder produced the aggregate function median, which the ES|QL function list does not hold"
        );

        let listed = pipeline(vec![Stage::Filter(BExpr::Scalar(
            "nullif",
            vec![literal.clone(), literal],
        ))]);
        check_functions(&listed).unwrap_or_else(|e| panic!("listed functions pass: {e}"));
    }
}
