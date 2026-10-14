//! The statement the executor runs: SQL text, or a plan another front end
//! lowers onto the session the executor builds.
//!
//! The executor builds more than one session per request: the classification
//! session the spill and exact-typed decisions read, the session that
//! executes, and a rebuilt one when the snapshot retry or the spill fallback
//! fires. A statement is therefore not a plan but a lowering the executor can
//! call once per session, each call binding the plan to that session's tables
//! and functions. SQL text is one such lowering; [`LoweredStatement`] is the
//! other. No production caller constructs a non-SQL statement yet.
//!
//! SQL passes the read-only gate and the complexity guard before any of this
//! runs, and the binder can only name what the session registered. A lowered
//! plan has passed through neither. A [`LoweredStatement`] is handed a
//! [`LoweringContext`], a read-only snapshot of the session taken before the
//! lowering starts, never the session itself, so it cannot change what the
//! session registers, its tables or its configuration. Every plan it returns
//! is then checked by [`admit_lowered_plan`] against that same snapshot
//! before anything else reads it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::{provider_as_source, source_as_provider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    AggregateUDF, Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF, WindowFunctionDefinition,
    WindowUDF,
};
use datafusion::prelude::SessionContext;

use crate::error::SqlError;
use crate::executor::{SqlExecutor, StatementTables, TargetSignal};
use crate::parquet::MAX_STATEMENT_TABLE_NAMES;
use crate::session::{
    ALERTS_TABLE, AUDIT_TABLE, LOGS_TABLE, SAMPLES_TABLE, SPANS_TABLE, SessionTable,
};

/// The tables a [`LoweredStatement`] reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementTarget {
    /// One signal table. [`TargetSignal::Parquet`] names no table and is
    /// refused; use [`StatementTarget::Parquet`].
    Signal(TargetSignal),
    /// One or more of the tenant's Parquet tables, by name.
    Parquet(BTreeSet<String>),
}

/// The function names a lowered plan may call, by kind.
///
/// A call is admitted only when it is the instance the session registered
/// under one of these names (see [`admit_lowered_plan`]); a name alone admits
/// nothing.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DialectPolicy {
    scalars: BTreeSet<String>,
    aggregates: BTreeSet<String>,
    windows: BTreeSet<String>,
}

impl DialectPolicy {
    pub fn new<S, A, W>(scalars: S, aggregates: A, windows: W) -> Self
    where
        S: IntoIterator,
        S::Item: Into<String>,
        A: IntoIterator,
        A::Item: Into<String>,
        W: IntoIterator,
        W::Item: Into<String>,
    {
        Self {
            scalars: scalars.into_iter().map(Into::into).collect(),
            aggregates: aggregates.into_iter().map(Into::into).collect(),
            windows: windows.into_iter().map(Into::into).collect(),
        }
    }
}

/// What a [`LoweredStatement`] lowers against: the functions one session
/// registered and the providers of the statement's target tables, read
/// before the lowering starts.
///
/// It holds the registry's function `Arc`s and the executor's provider `Arc`s
/// and nothing else, so a lowering has no handle on the session, its state,
/// its catalog, its configuration or its rule lists: it cannot register,
/// replace or deregister a function or a table there. What it reaches is
/// those instances, through their own traits. [`admit_lowered_plan`] checks
/// the plan against this same snapshot.
pub struct LoweringContext {
    scalars: HashMap<String, Arc<ScalarUDF>>,
    aggregates: HashMap<String, Arc<AggregateUDF>>,
    windows: HashMap<String, Arc<WindowUDF>>,
    tables: BTreeMap<String, Arc<dyn TableProvider>>,
}

impl Debug for LoweringContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LoweringContext")
            .field("tables", &self.tables.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

impl LoweringContext {
    /// Snapshot `ctx`'s function registries, and keep from `providers` (the
    /// providers the executor built for `ctx`) those of the tables in
    /// `tables`.
    fn snapshot(
        ctx: &SessionContext,
        providers: Vec<(String, Arc<dyn TableProvider>)>,
        tables: &StatementTables,
    ) -> Self {
        let state = ctx.state();
        let target = target_table_names(tables);
        Self {
            scalars: state.scalar_functions().clone(),
            aggregates: state.aggregate_functions().clone(),
            windows: state.window_functions().clone(),
            tables: providers
                .into_iter()
                .filter(|(name, _)| target.contains(name.as_str()))
                .collect(),
        }
    }

    /// The scalar function the session registered under `name` (a name or an
    /// alias), as the registry's own `Arc`.
    pub fn scalar(&self, name: &str) -> Result<Arc<ScalarUDF>, DataFusionError> {
        registered("scalar", &self.scalars, name)
    }

    /// The aggregate function the session registered under `name`.
    pub fn aggregate(&self, name: &str) -> Result<Arc<AggregateUDF>, DataFusionError> {
        registered("aggregate", &self.aggregates, name)
    }

    /// The window function the session registered under `name`.
    pub fn window(&self, name: &str) -> Result<Arc<WindowUDF>, DataFusionError> {
        registered("window", &self.windows, name)
    }

    /// A plan builder over an unprojected, unfiltered scan of `table`, one of
    /// the statement's target tables, through the provider the executor
    /// registered for it.
    pub fn scan(&self, table: &str) -> Result<LogicalPlanBuilder, DataFusionError> {
        LogicalPlanBuilder::scan(table, provider_as_source(self.provider(table)?), None)
    }

    /// The schema of `table`, one of the statement's target tables.
    pub fn schema(&self, table: &str) -> Result<SchemaRef, DataFusionError> {
        Ok(self.provider(table)?.schema())
    }

    fn provider(&self, table: &str) -> Result<Arc<dyn TableProvider>, DataFusionError> {
        self.tables.get(table).map(Arc::clone).ok_or_else(|| {
            DataFusionError::Plan(format!("{table} is not a table in the statement's target"))
        })
    }
}

fn registered<F: ?Sized>(
    kind: &str,
    registry: &HashMap<String, Arc<F>>,
    name: &str,
) -> Result<Arc<F>, DataFusionError> {
    registry
        .get(name)
        .map(Arc::clone)
        .ok_or_else(|| DataFusionError::Plan(format!("no {kind} function named {name}")))
}

/// A statement that is not SQL text: a front end that lowers itself onto a
/// session the executor built.
///
/// [`Self::lower`] is called once for every session the request builds, so
/// it must be repeatable: it reads its tables and functions from the
/// [`LoweringContext`] it is handed (its functions as the `Arc` the context
/// returns, placed in the expression unchanged), never from a plan or handle
/// kept from an earlier call. It returns the unoptimized plan; the executor
/// admits it, then runs the analyzer and the optimizer itself.
#[async_trait]
pub trait LoweredStatement: Send + Sync + Debug {
    /// The tables the statement reads. Fixed for the statement's lifetime.
    fn target(&self) -> StatementTarget;
    /// The statement as the caller wrote it, for the audit record and for
    /// error detail.
    fn text(&self) -> &str;
    /// The functions the statement's dialect may call.
    fn policy(&self) -> &DialectPolicy;
    /// Lower the statement against `ctx`.
    async fn lower(&self, ctx: &LoweringContext) -> Result<LogicalPlan, DataFusionError>;
}

/// Which session a statement is being lowered for. SQL plans each with the
/// call it always used there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoweringSite {
    /// The throwaway empty-snapshot session the spill and exact-typed
    /// classification reads.
    Classification,
    /// A session that executes: the first, a snapshot retry's, or the spill
    /// fallback's rebuild.
    Executed,
}

/// Why [`Statement::lower`] produced no plan.
#[derive(Debug)]
pub(crate) enum LowerError {
    /// Planning failed: SQL that does not plan, or a lowering that returned
    /// an error.
    Plan(DataFusionError),
    /// A lowered plan failed [`admit_lowered_plan`].
    NotAdmitted(SqlError),
}

/// The statement one request runs.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Statement<'a> {
    Sql(&'a str),
    Lowered(&'a dyn LoweredStatement),
}

impl<'a> Statement<'a> {
    /// The base tables the statement names. For SQL, read off the text.
    pub(crate) fn tables(self) -> Result<StatementTables, SqlError> {
        match self {
            Statement::Sql(sql) => SqlExecutor::statement_tables(sql),
            Statement::Lowered(statement) => lowered_tables(statement.target()),
        }
    }

    /// The text to audit and to carry in errors.
    pub(crate) fn text(self) -> &'a str {
        match self {
            Statement::Sql(sql) => sql,
            Statement::Lowered(statement) => statement.text(),
        }
    }

    /// The SQL text, for the steps that read the statement's syntax tree.
    pub(crate) fn sql(self) -> Option<&'a str> {
        match self {
            Statement::Sql(sql) => Some(sql),
            Statement::Lowered(_) => None,
        }
    }

    /// The unoptimized plan for `ctx`, admitted when the statement is a
    /// [`LoweredStatement`].
    ///
    /// `providers` are the providers the executor built `ctx` over, taken
    /// from its [`SessionTable`] ([`session_table_providers`]) and not looked
    /// up in `ctx`. A lowered statement is handed a [`LoweringContext`]
    /// snapshot of `ctx` and `providers`, and its plan is admitted against
    /// that snapshot. SQL plans on `ctx` itself and ignores `providers`.
    pub(crate) async fn lower(
        self,
        ctx: &SessionContext,
        providers: Vec<(String, Arc<dyn TableProvider>)>,
        tables: &StatementTables,
        site: LoweringSite,
    ) -> Result<LogicalPlan, LowerError> {
        match (self, site) {
            (Statement::Sql(sql), LoweringSite::Executed) => Ok(ctx
                .sql(sql)
                .await
                .map_err(LowerError::Plan)?
                .into_unoptimized_plan()),
            (Statement::Sql(sql), LoweringSite::Classification) => ctx
                .state()
                .create_logical_plan(sql)
                .await
                .map_err(LowerError::Plan),
            (Statement::Lowered(statement), _) => {
                let context = LoweringContext::snapshot(ctx, providers, tables);
                let plan = statement.lower(&context).await.map_err(LowerError::Plan)?;
                admit_lowered_plan(&plan, &context, tables, statement.policy())
                    .map_err(LowerError::NotAdmitted)?;
                Ok(plan)
            }
        }
    }
}

/// The table providers `table` holds, under the names
/// [`crate::session::build_session`] registers them.
pub(crate) fn session_table_providers(
    table: &SessionTable,
) -> Vec<(String, Arc<dyn TableProvider>)> {
    let one = |name: &str, provider: Arc<dyn TableProvider>| vec![(name.to_string(), provider)];
    match table {
        SessionTable::Metrics(provider) => one(SAMPLES_TABLE, Arc::clone(provider) as _),
        SessionTable::Logs(provider) => one(LOGS_TABLE, Arc::clone(provider) as _),
        SessionTable::Spans(provider) => one(SPANS_TABLE, Arc::clone(provider) as _),
        SessionTable::Alerts(provider) => one(ALERTS_TABLE, Arc::clone(provider) as _),
        SessionTable::Audit(provider) => one(AUDIT_TABLE, Arc::clone(provider) as _),
        SessionTable::Parquet(session) => session.tables.clone(),
    }
}

fn lowered_tables(target: StatementTarget) -> Result<StatementTables, SqlError> {
    match target {
        StatementTarget::Signal(TargetSignal::Parquet) => Err(SqlError::PlanNotAdmitted(
            "a signal target named the Parquet target".to_string(),
        )),
        StatementTarget::Signal(signal) => Ok(StatementTables {
            signal: Some(signal),
            others: BTreeSet::new(),
        }),
        StatementTarget::Parquet(names) => {
            if names.is_empty() {
                return Err(SqlError::PlanNotAdmitted(
                    "a Parquet target named no table".to_string(),
                ));
            }
            if let Some(name) = names.iter().find(|name| signal_table(name).is_some()) {
                return Err(SqlError::PlanNotAdmitted(format!(
                    "a Parquet target named the signal table {name}"
                )));
            }
            if names.len() > MAX_STATEMENT_TABLE_NAMES {
                return Err(SqlError::TooManyTables {
                    count: names.len(),
                    max: MAX_STATEMENT_TABLE_NAMES,
                });
            }
            Ok(StatementTables {
                signal: None,
                others: names,
            })
        }
    }
}

fn signal_table(name: &str) -> Option<TargetSignal> {
    match name {
        SAMPLES_TABLE => Some(TargetSignal::Metrics),
        LOGS_TABLE => Some(TargetSignal::Logs),
        SPANS_TABLE => Some(TargetSignal::Spans),
        ALERTS_TABLE => Some(TargetSignal::Alerts),
        AUDIT_TABLE => Some(TargetSignal::Audit),
        _ => None,
    }
}

fn signal_table_name(signal: TargetSignal) -> Option<&'static str> {
    match signal {
        TargetSignal::Metrics => Some(SAMPLES_TABLE),
        TargetSignal::Logs => Some(LOGS_TABLE),
        TargetSignal::Spans => Some(SPANS_TABLE),
        TargetSignal::Alerts => Some(ALERTS_TABLE),
        TargetSignal::Audit => Some(AUDIT_TABLE),
        TargetSignal::Parquet => None,
    }
}

/// The table names `tables` admits a scan of.
fn target_table_names(tables: &StatementTables) -> BTreeSet<&str> {
    let mut names: BTreeSet<&str> = tables.others.iter().map(String::as_str).collect();
    if let Some(name) = tables.signal.and_then(signal_table_name) {
        names.insert(name);
    }
    names
}

/// Whether `node`'s kind may appear in a lowered plan.
fn admitted_node(node: &LogicalPlan) -> bool {
    matches!(
        node,
        LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::EmptyRelation(_)
    )
}

/// Refuse a lowered plan unless every node, every table it scans and every
/// function it calls is one `context` admits, with
/// [`SqlError::PlanNotAdmitted`].
///
/// The walk covers every node and every expression, subquery plans included
/// (a subquery expression is visited as its `Subquery` node, which is itself
/// not admitted). It runs on the plan exactly as lowered, before the
/// executor's own shape check and before any analyzer or optimizer pass, so
/// no rewrite can add or hide anything ahead of it.
///
/// - Every node kind is one of Projection, Filter, Aggregate, Sort, Limit,
///   TableScan, SubqueryAlias, Values and EmptyRelation.
/// - Every `TableScan` names a table in `tables`, and its source is the
///   provider the executor built the session over for that name: a scan
///   built over any other provider under an admitted name is refused. It
///   carries no filters and no fetch, the shape SQL's planner produces before
///   the optimizer pushes either into a scan; a lowering that set them would
///   be asking the provider to apply what it never agreed to.
/// - Every scalar, aggregate and window function is the instance the session
///   registered under some name on `policy`.
///
/// Instance identity is pointer identity, `Arc::ptr_eq` between the call's
/// `Arc<ScalarUDF>` / `Arc<AggregateUDF>` / `Arc<WindowUDF>` (or the scan's
/// `Arc<dyn TableProvider>`) and the one `context` holds under a listed name.
/// Registering a function stores that one allocation under its name and
/// every alias, so a call carrying the `Arc` the context returned passes
/// whichever of those names the policy lists. A function the session
/// replaced under a DataFusion name (the total-order `min` and `max`, the
/// sequential-fold `avg`, a scalar whose dictionary result the session
/// re-types) is a different allocation from DataFusion's own handle for that
/// name, so the stock handle is refused however its name, signature or
/// behaviour compare. A function the session did not replace is registered
/// as DataFusion's own shared instance, so DataFusion's handle for it is the
/// session's instance and is admitted under a listed name. A copy of the
/// session's instance is refused, and that is what the expression helpers
/// `ScalarUDF::call` and `AggregateUDF::call` build: a lowering puts the
/// context's `Arc` in the expression itself. There is no fallback to
/// comparing names.
pub(crate) fn admit_lowered_plan(
    plan: &LogicalPlan,
    context: &LoweringContext,
    tables: &StatementTables,
    policy: &DialectPolicy,
) -> Result<(), SqlError> {
    let admitted_tables = target_table_names(tables);
    let mut refusal: Option<String> = None;
    plan.apply_with_subqueries(|node| {
        if !admitted_node(node) {
            refusal = Some(format!(
                "a lowered plan holds an unadmitted {} node",
                node_kind(node)
            ));
            return Ok(TreeNodeRecursion::Stop);
        }
        if let LogicalPlan::TableScan(scan) = node {
            let name = &scan.table_name;
            let reason = if !admitted_tables.contains(name.table()) {
                Some(format!(
                    "a lowered plan scans {name}, which is not in its target"
                ))
            } else if !scan_is_registered(context, name.table(), &scan.source) {
                Some(format!(
                    "a lowered plan scans {name} through a provider the session did not register"
                ))
            } else if !scan.filters.is_empty() {
                Some(format!("a lowered plan scans {name} with filters"))
            } else if scan.fetch.is_some() {
                Some(format!("a lowered plan scans {name} with a fetch"))
            } else {
                None
            };
            if reason.is_some() {
                refusal = reason;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                Ok(match refused_function(expr, context, policy) {
                    Some(reason) => {
                        refusal = Some(reason);
                        TreeNodeRecursion::Stop
                    }
                    None => TreeNodeRecursion::Continue,
                })
            })
        })
    })
    .map_err(|err| SqlError::PlanNotAdmitted(format!("the admission walk failed: {err}")))?;
    match refusal {
        Some(reason) => Err(SqlError::PlanNotAdmitted(reason)),
        None => Ok(()),
    }
}

fn scan_is_registered(
    context: &LoweringContext,
    table: &str,
    source: &Arc<dyn datafusion::logical_expr::TableSource>,
) -> bool {
    let Ok(scanned) = source_as_provider(source) else {
        return false;
    };
    context
        .tables
        .get(table)
        .is_some_and(|registered| Arc::ptr_eq(&scanned, registered))
}

/// Why `expr`'s function is refused, or `None` when it is not a function
/// call or is admitted.
fn refused_function(
    expr: &Expr,
    context: &LoweringContext,
    policy: &DialectPolicy,
) -> Option<String> {
    match expr {
        Expr::ScalarFunction(call) => refused_instance(
            "scalar",
            call.func.name(),
            call.func.aliases(),
            &policy.scalars,
            &context.scalars,
            &call.func,
        ),
        Expr::AggregateFunction(call) => refused_instance(
            "aggregate",
            call.func.name(),
            call.func.aliases(),
            &policy.aggregates,
            &context.aggregates,
            &call.func,
        ),
        Expr::WindowFunction(call) => match &call.fun {
            WindowFunctionDefinition::AggregateUDF(func) => refused_instance(
                "aggregate",
                func.name(),
                func.aliases(),
                &policy.aggregates,
                &context.aggregates,
                func,
            ),
            WindowFunctionDefinition::WindowUDF(func) => refused_instance(
                "window",
                func.name(),
                func.aliases(),
                &policy.windows,
                &context.windows,
                func,
            ),
        },
        _ => None,
    }
}

/// Admits `called` when some name on `listed` resolves in the session's
/// `registry` to that very allocation. `name` and `aliases` are what the call
/// claims to be, used only to word the refusal.
fn refused_instance<F: ?Sized>(
    kind: &str,
    name: &str,
    aliases: &[String],
    listed: &BTreeSet<String>,
    registry: &HashMap<String, Arc<F>>,
    called: &Arc<F>,
) -> Option<String> {
    let registered = listed
        .iter()
        .filter_map(|listed_name| registry.get(listed_name))
        .any(|instance| Arc::ptr_eq(instance, called));
    if registered {
        return None;
    }
    let claims_listed_name =
        listed.contains(name) || aliases.iter().any(|alias| listed.contains(alias));
    if claims_listed_name {
        Some(format!(
            "a lowered plan calls the {kind} function {name}, which is not the session's instance"
        ))
    } else {
        Some(format!(
            "a lowered plan calls the {kind} function {name}, which its dialect does not list"
        ))
    }
}

fn node_kind(node: &LogicalPlan) -> String {
    let display = node.display().to_string();
    display
        .split(|c: char| c == ':' || c.is_whitespace())
        .next()
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The lowering context's whole field set, by type. Destructured
    /// exhaustively, so a field added to [`LoweringContext`] (a session, its
    /// state, its catalog or its configuration among the candidates) does not
    /// compile until this test names it.
    #[test]
    fn the_lowering_context_holds_only_functions_and_providers() {
        let context = LoweringContext {
            scalars: HashMap::new(),
            aggregates: HashMap::new(),
            windows: HashMap::new(),
            tables: BTreeMap::new(),
        };
        let LoweringContext {
            scalars,
            aggregates,
            windows,
            tables,
        } = context;
        let _: HashMap<String, Arc<ScalarUDF>> = scalars;
        let _: HashMap<String, Arc<AggregateUDF>> = aggregates;
        let _: HashMap<String, Arc<WindowUDF>> = windows;
        let _: BTreeMap<String, Arc<dyn TableProvider>> = tables;
    }
}
