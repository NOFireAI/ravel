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
//! plan has passed through neither, so every plan a [`LoweredStatement`]
//! returns is checked by [`admit_lowered_plan`] before anything else reads
//! it.

use std::collections::{BTreeSet, HashMap};
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::common::TableReference;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::datasource::source_as_provider;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, LogicalPlan, WindowFunctionDefinition};
use datafusion::prelude::SessionContext;

use crate::error::SqlError;
use crate::executor::{SqlExecutor, StatementTables, TargetSignal};
use crate::parquet::MAX_STATEMENT_TABLE_NAMES;
use crate::session::{ALERTS_TABLE, AUDIT_TABLE, LOGS_TABLE, SAMPLES_TABLE, SPANS_TABLE};

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

/// A statement that is not SQL text: a front end that lowers itself onto a
/// session the executor built.
///
/// [`Self::lower`] is called once for every session the request builds, so
/// it must be repeatable and must bind against the `ctx` it is handed (its
/// tables via `ctx.table`, its functions as the `Arc` the session's registry
/// returns, placed in the expression unchanged), never
/// against a plan or handle kept from an earlier call. It returns the
/// unoptimized plan; the executor admits it, then runs the analyzer and the
/// optimizer itself.
#[async_trait]
pub trait LoweredStatement: Send + Sync + Debug {
    /// The tables the statement reads. Fixed for the statement's lifetime.
    fn target(&self) -> StatementTarget;
    /// The statement as the caller wrote it, for the audit record and for
    /// error detail.
    fn text(&self) -> &str;
    /// The functions the statement's dialect may call.
    fn policy(&self) -> &DialectPolicy;
    /// Lower the statement onto `ctx`.
    async fn lower(&self, ctx: &SessionContext) -> Result<LogicalPlan, DataFusionError>;
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

    /// The dialect policy a lowered plan is admitted against. SQL has none:
    /// its function names are governed by the session's allowlists.
    pub(crate) fn policy(self) -> Option<&'a DialectPolicy> {
        match self {
            Statement::Sql(_) => None,
            Statement::Lowered(statement) => Some(statement.policy()),
        }
    }

    /// The unoptimized plan for `ctx`.
    pub(crate) async fn lower(
        self,
        ctx: &SessionContext,
        site: LoweringSite,
    ) -> Result<LogicalPlan, DataFusionError> {
        match (self, site) {
            (Statement::Sql(sql), LoweringSite::Executed) => {
                Ok(ctx.sql(sql).await?.into_unoptimized_plan())
            }
            (Statement::Sql(sql), LoweringSite::Classification) => {
                ctx.state().create_logical_plan(sql).await
            }
            (Statement::Lowered(statement), _) => statement.lower(ctx).await,
        }
    }

    /// Admit a plan [`Self::lower`] returned for `ctx`. A no-op for SQL.
    pub(crate) async fn admit(
        self,
        plan: &LogicalPlan,
        ctx: &SessionContext,
        tables: &StatementTables,
    ) -> Result<(), SqlError> {
        match self.policy() {
            None => Ok(()),
            Some(policy) => admit_lowered_plan(plan, ctx, tables, policy).await,
        }
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
/// function it calls is one this session admits, with
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
///   provider `ctx` holds under that name: a scan built over any other
///   provider under an admitted name is refused.
/// - Every scalar, aggregate and window function is the instance `ctx`
///   registered under some name on `policy`.
///
/// Instance identity is pointer identity, `Arc::ptr_eq` between the call's
/// `Arc<ScalarUDF>` / `Arc<AggregateUDF>` / `Arc<WindowUDF>` (or the scan's
/// `Arc<dyn TableProvider>`) and the one the session's registry holds under a
/// listed name. Registering a function stores that one allocation under its
/// name and every alias, so a call carrying the `Arc` the registry returned
/// passes whichever of those names the policy lists. A handle obtained
/// anywhere else, DataFusion's own `min`, `max` or `avg` among them, is a
/// different allocation from the session's replacement registered under the
/// same name, and is refused however its name, signature or behaviour
/// compare. So is a copy of the session's own instance, which is what the
/// expression helpers `ScalarUDF::call` and `AggregateUDF::call` build: a
/// lowering puts the registry's `Arc` in the expression itself. There is no
/// fallback to comparing names.
pub(crate) async fn admit_lowered_plan(
    plan: &LogicalPlan,
    ctx: &SessionContext,
    tables: &StatementTables,
    policy: &DialectPolicy,
) -> Result<(), SqlError> {
    let mut admitted_tables: BTreeSet<&str> = tables.others.iter().map(String::as_str).collect();
    if let Some(name) = tables.signal.and_then(signal_table_name) {
        admitted_tables.insert(name);
    }
    let state = ctx.state();
    let mut refusal: Option<String> = None;
    let mut scans = Vec::new();
    plan.apply_with_subqueries(|node| {
        if !admitted_node(node) {
            refusal = Some(format!(
                "a lowered plan holds an unadmitted {} node",
                node_kind(node)
            ));
            return Ok(TreeNodeRecursion::Stop);
        }
        if let LogicalPlan::TableScan(scan) = node {
            if !admitted_tables.contains(scan.table_name.table()) {
                refusal = Some(format!(
                    "a lowered plan scans {}, which is not in its target",
                    scan.table_name
                ));
                return Ok(TreeNodeRecursion::Stop);
            }
            scans.push((scan.table_name.clone(), Arc::clone(&scan.source)));
        }
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                Ok(match refused_function(expr, &state, policy) {
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
    if let Some(reason) = refusal {
        return Err(SqlError::PlanNotAdmitted(reason));
    }
    for (name, source) in scans {
        if !scan_is_registered(ctx, &name, source).await {
            return Err(SqlError::PlanNotAdmitted(format!(
                "a lowered plan scans {name} through a provider the session did not register"
            )));
        }
    }
    Ok(())
}

async fn scan_is_registered(
    ctx: &SessionContext,
    name: &TableReference,
    source: Arc<dyn datafusion::logical_expr::TableSource>,
) -> bool {
    let Ok(scanned) = source_as_provider(&source) else {
        return false;
    };
    match ctx.table_provider(name.clone()).await {
        Ok(registered) => Arc::ptr_eq(&scanned, &registered),
        Err(_) => false,
    }
}

/// Why `expr`'s function is refused, or `None` when it is not a function
/// call or is admitted.
fn refused_function(expr: &Expr, state: &SessionState, policy: &DialectPolicy) -> Option<String> {
    match expr {
        Expr::ScalarFunction(call) => refused_instance(
            "scalar",
            call.func.name(),
            call.func.aliases(),
            &policy.scalars,
            state.scalar_functions(),
            &call.func,
        ),
        Expr::AggregateFunction(call) => refused_instance(
            "aggregate",
            call.func.name(),
            call.func.aliases(),
            &policy.aggregates,
            state.aggregate_functions(),
            &call.func,
        ),
        Expr::WindowFunction(call) => match &call.fun {
            WindowFunctionDefinition::AggregateUDF(func) => refused_instance(
                "aggregate",
                func.name(),
                func.aliases(),
                &policy.aggregates,
                state.aggregate_functions(),
                func,
            ),
            WindowFunctionDefinition::WindowUDF(func) => refused_instance(
                "window",
                func.name(),
                func.aliases(),
                &policy.windows,
                state.window_functions(),
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
