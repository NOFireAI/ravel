//! ES|QL over the `logs` table (ADR-2887): bind a statement parsed by
//! [`ravel_esql::parse`] and lower it to a DataFusion logical plan that runs
//! through [`SqlExecutor::execute_statement_accounted`].
//!
//! No HTTP handler calls [`prepare`] yet.
//!
//! [`prepare`] parses and binds once, before any session exists, and fixes the
//! statement's output columns and their ES|QL types, the [`DialectPolicy`]
//! (exactly the functions the bound pipeline calls) and the time window read
//! from its `@timestamp` bounds. [`PreparedEsql`] then lowers the same bound
//! pipeline against every session the executor builds for it.
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

pub use binder::IMPLICIT_LIMIT;

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
#[derive(Debug, Clone)]
pub struct PreparedEsql {
    text: String,
    pipeline: Pipeline,
    policy: DialectPolicy,
    window: Option<TimeRange>,
    columns: Vec<EsqlColumn>,
    now_ns: i64,
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
    let (scalars, aggregates) = bound.pipeline.functions();
    let policy = DialectPolicy::new(scalars, aggregates, std::iter::empty::<String>());
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

    /// The statement's logical plan in `session`, lowered and admitted as the
    /// executor would, then optimized by the session. `session` must hold the
    /// `logs` table for a `FROM` statement.
    pub async fn optimized_plan(&self, session: &SessionContext) -> Result<LogicalPlan, EsqlError> {
        let providers = if self.needs_catalog() {
            let provider = session
                .table_provider(LOGS_TABLE)
                .await
                .map_err(|e| EsqlError::Execution(crate::SqlError::Plan(e.to_string())))?;
            vec![(LOGS_TABLE.to_string(), provider)]
        } else {
            Vec::new()
        };
        let tables = self.tables();
        let context = LoweringContext::snapshot(session, providers, &tables);
        let plan = lower::lower(&self.pipeline, &context)
            .map_err(|e| EsqlError::Execution(crate::SqlError::Plan(e.to_string())))?;
        admit_lowered_plan(&plan, &context, &tables, &self.policy).map_err(EsqlError::Execution)?;
        session
            .state()
            .optimize(&plan)
            .map_err(|e| EsqlError::Execution(crate::SqlError::Plan(e.to_string())))
    }

    fn tables(&self) -> StatementTables {
        StatementTables {
            signal: self.needs_catalog().then_some(TargetSignal::Logs),
            others: Default::default(),
        }
    }
}

#[async_trait]
impl LoweredStatement for PreparedEsql {
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
