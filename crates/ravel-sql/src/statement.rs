//! The statement the executor runs: SQL text, or a plan another front end
//! lowers onto the session the executor builds.
//!
//! The executor builds more than one session per request: the classification
//! session the spill and exact-typed decisions read, the session that
//! executes, and a rebuilt one when the snapshot retry or the spill fallback
//! fires. A statement is therefore not a plan but a lowering the executor can
//! call once per session, each call binding the plan to that session's tables
//! and functions. SQL text is one such lowering; [`LoweredStatement`] is the
//! other, sealed so that only this crate's front ends implement it. No
//! production caller constructs a non-SQL statement yet.
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
use datafusion::common::TableReference;
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
    pub(crate) fn snapshot(
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
    ///
    /// `table` is the name exactly as the target holds it. The scan names it
    /// as a bare [`TableReference`], never one parsed from the string, so a
    /// Parquet table named with capitals or a dot is scanned, and admitted,
    /// under that same name rather than a lowercased or schema-qualified one.
    pub fn scan(&self, table: &str) -> Result<LogicalPlanBuilder, DataFusionError> {
        LogicalPlanBuilder::scan(
            TableReference::bare(table),
            provider_as_source(self.provider(table)?),
            None,
        )
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
///
/// The trait is sealed: its supertrait lives in a module private to this
/// crate, so only this crate's front ends implement it, and nothing outside
/// the crate runs a lowered statement. A complete implementation written
/// outside the crate is refused for the missing supertrait alone:
///
/// ```compile_fail
/// use datafusion::error::DataFusionError;
/// use datafusion::logical_expr::LogicalPlan;
/// use ravel_sql::{DialectPolicy, LoweredStatement, LoweringContext, StatementTarget, TargetSignal};
///
/// #[derive(Debug)]
/// struct Outside(DialectPolicy);
///
/// #[async_trait::async_trait]
/// impl LoweredStatement for Outside {
///     fn target(&self) -> StatementTarget {
///         StatementTarget::Signal(TargetSignal::Logs)
///     }
///     fn text(&self) -> &str {
///         "outside"
///     }
///     fn policy(&self) -> &DialectPolicy {
///         &self.0
///     }
///     async fn lower(&self, ctx: &LoweringContext) -> Result<LogicalPlan, DataFusionError> {
///         ctx.scan("logs")?.build()
///     }
/// }
/// ```
///
/// The second doctest below implements `Sealed` by its path and fails because
/// the `sealed` module is private to this crate (E0603). That privacy held
/// before the trait was sealed, so this doctest proves module privacy, not the
/// seal; the first doctest is the one that proves the seal:
///
/// ```compile_fail
/// #[derive(Debug)]
/// struct Outside;
///
/// impl ravel_sql::statement::sealed::Sealed for Outside {}
/// ```
#[async_trait]
pub trait LoweredStatement: sealed::Sealed + Send + Sync + Debug {
    /// The tables the statement reads. Fixed for the statement's lifetime.
    fn target(&self) -> StatementTarget;
    /// The statement as the caller wrote it. The executor does not read it:
    /// the caller that runs the statement audits it.
    fn text(&self) -> &str;
    /// The functions the statement's dialect may call.
    fn policy(&self) -> &DialectPolicy;
    /// Lower the statement against `ctx`.
    async fn lower(&self, ctx: &LoweringContext) -> Result<LogicalPlan, DataFusionError>;
}

pub(crate) mod sealed {
    /// The supertrait of [`super::LoweredStatement`]. Public in a module
    /// private to the crate, so no other crate can name or implement it.
    pub trait Sealed {
        /// Called with the site before every lowering.
        #[cfg(test)]
        #[allow(private_interfaces)]
        fn lowering_at(&self, _site: super::LoweringSite) {}
    }
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
                #[cfg(test)]
                statement.lowering_at(site);
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
        StatementTarget::Signal(signal) => Ok(StatementTables::one(Some(signal))),
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
                signals: Vec::new(),
                others: names,
            })
        }
    }
}

pub(crate) fn signal_table(name: &str) -> Option<TargetSignal> {
    match name {
        SAMPLES_TABLE => Some(TargetSignal::Metrics),
        LOGS_TABLE => Some(TargetSignal::Logs),
        SPANS_TABLE => Some(TargetSignal::Spans),
        ALERTS_TABLE => Some(TargetSignal::Alerts),
        AUDIT_TABLE => Some(TargetSignal::Audit),
        _ => None,
    }
}

pub(crate) fn signal_table_name(signal: TargetSignal) -> Option<&'static str> {
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
    names.extend(
        tables
            .signals
            .iter()
            .filter_map(|signal| signal_table_name(*signal)),
    );
    names
}

/// Whether `node`'s kind may appear in a lowered plan. `Window` stays out
/// until window operators have a decision of their own (ADR-2887 D13).
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
/// - Every `TableScan` names a table in `tables` by a bare reference (the
///   form [`LoweringContext::scan`] builds), and its source is the
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
            let bare = match name {
                TableReference::Bare { table } => Some(table.as_ref()),
                _ => None,
            };
            let reason = if let Some(table) = bare.filter(|table| admitted_tables.contains(table))
            {
                if !scan_is_registered(context, table, &scan.source) {
                    Some(format!(
                        "a lowered plan scans {name} through a provider the session did not register"
                    ))
                } else if !scan.filters.is_empty() {
                    Some(format!("a lowered plan scans {name} with filters"))
                } else if scan.fetch.is_some() {
                    Some(format!("a lowered plan scans {name} with a fetch"))
                } else {
                    None
                }
            } else {
                Some(format!(
                    "a lowered plan scans {name}, which is not in its target"
                ))
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

/// The name of `node`'s [`LogicalPlan`] variant. Matched without a wildcard,
/// so a variant DataFusion adds does not compile until it is named here.
fn node_kind(node: &LogicalPlan) -> &'static str {
    match node {
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Window(_) => "Window",
        LogicalPlan::Aggregate(_) => "Aggregate",
        LogicalPlan::Sort(_) => "Sort",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Repartition(_) => "Repartition",
        LogicalPlan::Union(_) => "Union",
        LogicalPlan::TableScan(_) => "TableScan",
        LogicalPlan::EmptyRelation(_) => "EmptyRelation",
        LogicalPlan::Subquery(_) => "Subquery",
        LogicalPlan::SubqueryAlias(_) => "SubqueryAlias",
        LogicalPlan::Limit(_) => "Limit",
        LogicalPlan::Statement(_) => "Statement",
        LogicalPlan::Values(_) => "Values",
        LogicalPlan::Explain(_) => "Explain",
        LogicalPlan::Analyze(_) => "Analyze",
        LogicalPlan::Extension(_) => "Extension",
        LogicalPlan::Distinct(_) => "Distinct",
        LogicalPlan::Dml(_) => "Dml",
        LogicalPlan::Ddl(_) => "Ddl",
        LogicalPlan::Copy(_) => "Copy",
        LogicalPlan::DescribeTable(_) => "DescribeTable",
        LogicalPlan::Unnest(_) => "Unnest",
        LogicalPlan::RecursiveQuery(_) => "RecursiveQuery",
    }
}

#[cfg(test)]
pub(crate) mod plan_source;

#[cfg(test)]
#[allow(clippy::expect_used, clippy::panic)]
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

    #[derive(Debug)]
    struct Outside(DialectPolicy);

    impl sealed::Sealed for Outside {}

    #[async_trait]
    impl LoweredStatement for Outside {
        fn target(&self) -> StatementTarget {
            StatementTarget::Signal(TargetSignal::Logs)
        }
        fn text(&self) -> &str {
            "outside"
        }
        fn policy(&self) -> &DialectPolicy {
            &self.0
        }
        async fn lower(&self, ctx: &LoweringContext) -> Result<LogicalPlan, DataFusionError> {
            ctx.scan("logs")?.build()
        }
    }

    /// Outside the crate the trait is unimplementable: the `compile_fail`
    /// doctests on [`LoweredStatement`] are a complete implementation that
    /// fails to compile for the missing [`sealed::Sealed`] (rustc reports
    /// E0277), and an implementation of `Sealed` that fails because its module
    /// is private (E0603). The fences carry no error code, which stable rustdoc
    /// does not check. `Outside`
    /// here is the first doctest's implementation plus the one `Sealed` line
    /// it cannot write there, so it compiles and its plan is admitted: the
    /// doctest fails for the supertrait and for nothing else.
    #[tokio::test]
    async fn lowered_statement_cannot_be_implemented_outside_the_crate() {
        let statement = Outside(DialectPolicy::default());
        let tables = lowered_tables(statement.target()).expect("a logs target");
        let context = context_over(&[LOGS_TABLE]);
        let plan = statement.lower(&context).await.expect("lowers");
        admit_lowered_plan(&plan, &context, &tables, statement.policy())
            .expect("the plan is admitted");
        assert!(matches!(plan, LogicalPlan::TableScan(_)), "{plan:?}");
    }

    fn empty_provider() -> Arc<dyn TableProvider> {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        Arc::new(datafusion::datasource::empty::EmptyTable::new(Arc::new(
            Schema::new(vec![Field::new("v", DataType::Int64, true)]),
        )))
    }

    fn context_over(names: &[&str]) -> LoweringContext {
        let state = SessionContext::new().state();
        LoweringContext {
            scalars: state.scalar_functions().clone(),
            aggregates: state.aggregate_functions().clone(),
            windows: state.window_functions().clone(),
            tables: names
                .iter()
                .map(|name| ((*name).to_string(), empty_provider()))
                .collect(),
        }
    }

    fn parquet_tables(names: &[&str]) -> StatementTables {
        StatementTables {
            signals: Vec::new(),
            others: names.iter().map(|name| (*name).to_string()).collect(),
        }
    }

    /// A Parquet table named with capitals or a dot is scanned under exactly
    /// that name and admitted. Parsing the name instead would lowercase the
    /// first and split the second into a schema and a table, and admission
    /// would then find neither in the target.
    #[test]
    fn a_scan_names_its_table_as_the_target_holds_it() {
        let names = ["Mixed.Case", "UPPER"];
        let context = context_over(&names);
        let tables = parquet_tables(&names);
        for name in names {
            let plan = context
                .scan(name)
                .and_then(LogicalPlanBuilder::build)
                .expect("scan");
            let LogicalPlan::TableScan(scan) = &plan else {
                panic!("expected a TableScan, got {plan:?}");
            };
            assert_eq!(scan.table_name, TableReference::bare(name));
            admit_lowered_plan(&plan, &context, &tables, &DialectPolicy::default())
                .expect("a scan built by the context is admitted");
        }

        // The same provider under the parsed reference is not the target's
        // table.
        let source = provider_as_source(Arc::clone(&context.tables["Mixed.Case"]));
        let parsed = LogicalPlanBuilder::scan("Mixed.Case", source, None)
            .and_then(LogicalPlanBuilder::build)
            .expect("scan");
        let err = admit_lowered_plan(&parsed, &context, &tables, &DialectPolicy::default())
            .expect_err("a parsed reference is refused");
        assert!(
            err.to_string().contains("which is not in its target"),
            "{err}"
        );
        assert!(context.scan("mixed.case").is_err());
    }

    /// A `Window` node is refused even when its function is the session's
    /// instance under a name the policy lists (ADR-2887 D13).
    #[test]
    fn a_window_node_is_refused_under_a_listed_function() {
        use datafusion::logical_expr::expr::WindowFunction;

        let context = context_over(&["t"]);
        let tables = parquet_tables(&["t"]);
        let sum = context.aggregate("sum").expect("sum");
        let call = Expr::WindowFunction(Box::new(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(sum),
            vec![datafusion::prelude::col("v")],
        )))
        .alias("s");
        let plan = context
            .scan("t")
            .and_then(|builder| builder.window(vec![call]))
            .and_then(LogicalPlanBuilder::build)
            .expect("window plan");
        assert!(matches!(plan, LogicalPlan::Window(_)), "{plan:?}");

        let listed = DialectPolicy::new(Vec::<String>::new(), ["sum"], Vec::<String>::new());
        let err = admit_lowered_plan(&plan, &context, &tables, &listed)
            .expect_err("a window node is refused");
        assert!(
            err.to_string()
                .contains("a lowered plan holds an unadmitted Window node"),
            "{err}"
        );
    }

    /// The refusal names the plan variant. A join displays as `Inner Join:`
    /// or `Cross Join:`, whose first token is not the node's kind.
    #[test]
    fn an_unadmitted_node_is_named_by_its_plan_variant() {
        let context = context_over(&["t", "u"]);
        let tables = parquet_tables(&["t", "u"]);
        let right = context
            .scan("u")
            .and_then(LogicalPlanBuilder::build)
            .expect("scan");
        let plan = context
            .scan("t")
            .and_then(|builder| builder.cross_join(right))
            .and_then(LogicalPlanBuilder::build)
            .expect("join plan");
        let err = admit_lowered_plan(&plan, &context, &tables, &DialectPolicy::default())
            .expect_err("a join is refused");
        assert!(
            err.to_string()
                .contains("a lowered plan holds an unadmitted Join node"),
            "{err}"
        );
    }
}
