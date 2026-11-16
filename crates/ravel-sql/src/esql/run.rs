//! Running a [`PreparedEsql`].

use std::sync::Arc;
use std::time::Duration;

use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool};
use ravel_query::RequestBudgets;
use ravel_types::accounting::QueryAccounting;
use ravel_types::{CommitToken, TenantHash};

use super::{EsqlColumn, EsqlError, PreparedEsql};
use crate::error::SqlError;
use crate::executor::{LiveAccounting, SqlExecutor, SqlOutcome, StatementRequest, StatementTables};
use crate::output::QueryOutput;
use crate::parquet::ParquetSession;
use crate::session::{SessionTable, SpillDecision, build_session};
use crate::statement::{LoweredStatement, LoweringContext, admit_lowered_plan};

/// The request inputs [`execute`] reads besides the statement.
#[derive(Debug, Clone)]
pub struct EsqlRequest {
    /// Commit tokens the resolved snapshot must include.
    pub min_tokens: Vec<CommitToken>,
    /// The whole request's deadline.
    pub deadline: Duration,
    /// The request's resource budgets, which bound a `FROM logs` statement's
    /// run through the executor as they bound a SQL request's
    /// ([`SqlRequest::budgets`](crate::executor::SqlRequest::budgets)). A
    /// `ROW` statement scans no segment and issues no store request, so it
    /// does not read them; it runs under the tenant's memory budget alone.
    pub budgets: Option<RequestBudgets>,
    /// The caller's row cap: a `FROM logs` statement returning more rows
    /// reports [`SqlStats::row_cap_hit`](crate::executor::SqlStats::row_cap_hit),
    /// as a SQL request does under
    /// [`SqlRequest::max_rows`](crate::executor::SqlRequest::max_rows).
    /// A `ROW` statement returns one row and does not read it.
    pub max_rows: Option<usize>,
}

/// A statement's result: the rows, and each column's ES|QL type.
#[derive(Debug)]
pub struct EsqlOutcome {
    /// The result columns in order, matching the output schema's fields.
    pub columns: Vec<EsqlColumn>,
    /// How the statement ran.
    pub run: EsqlRun,
    /// The statement's own final `LIMIT`, when it was above
    /// [`MAX_LIMIT`](super::MAX_LIMIT) and lowered to it.
    pub clamped_limit: Option<u64>,
}

/// How a statement ran.
#[derive(Debug)]
pub enum EsqlRun {
    /// Through the executor, over a resolved `logs` snapshot.
    Catalog(Box<SqlOutcome>),
    /// A `ROW` statement, with no catalog.
    Row(QueryOutput),
}

impl EsqlOutcome {
    /// The result rows.
    pub fn output(&self) -> &QueryOutput {
        match &self.run {
            EsqlRun::Catalog(outcome) => &outcome.output,
            EsqlRun::Row(output) => output,
        }
    }
}

/// Run `prepared` for `tenant`.
///
/// The statement is routed on its [`PreparedEsql::window`]. A `FROM logs`
/// statement runs through the executor's lowered-statement path over that
/// window, which selects the segments the catalog lists and also filters the
/// rows, so no row outside the window is returned whatever the segment
/// layout; it reads every [`EsqlRequest`] field.
///
/// A `ROW` statement has no window and reads no table, so it resolves no
/// snapshot, although its [`target`](LoweredStatement::target) names the
/// `logs` signal: it runs as a `Values` plan in a session that registers no
/// table, built from the executor's configuration with a memory pool that
/// charges `tenant`'s budget under the per-query ceiling, and it is lowered,
/// admitted, executed and collected under `request.deadline`. It reads no
/// other `request` field, reports no [`SqlOutcome`] and moves nothing through
/// `live`.
pub async fn execute(
    executor: &SqlExecutor,
    tenant: TenantHash,
    prepared: &PreparedEsql,
    request: &EsqlRequest,
    live: &LiveAccounting,
) -> Result<EsqlOutcome, EsqlError> {
    let columns = prepared.columns().to_vec();
    let run = match prepared.window() {
        Some(window) => {
            let statement_request = StatementRequest {
                window,
                min_tokens: request.min_tokens.clone(),
                now_ns: prepared.now_ns,
                deadline: request.deadline,
                row_window: true,
                max_rows: request.max_rows,
                budgets: request.budgets,
            };
            let outcome = executor
                .execute_statement_accounted(tenant, prepared, &statement_request, live)
                .await
                .map_err(EsqlError::from_executor)?;
            EsqlRun::Catalog(Box::new(outcome))
        }
        None => EsqlRun::Row(
            run_row(executor, tenant, prepared, request.deadline)
                .await
                .map_err(EsqlError::from_executor)?,
        ),
    };
    Ok(EsqlOutcome {
        columns,
        run,
        clamped_limit: prepared.clamped_limit(),
    })
}

async fn run_row(
    executor: &SqlExecutor,
    tenant: TenantHash,
    prepared: &PreparedEsql,
    deadline: Duration,
) -> Result<QueryOutput, SqlError> {
    let millis = u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX);
    tokio::time::timeout(deadline, run_row_inner(executor, tenant, prepared))
        .await
        .unwrap_or(Err(SqlError::DeadlineExceeded { millis }))
}

async fn run_row_inner(
    executor: &SqlExecutor,
    tenant: TenantHash,
    prepared: &PreparedEsql,
) -> Result<QueryOutput, SqlError> {
    let config = executor.config();
    let (pool, breach) = config.query_pool(executor.tenant_budget(tenant), QueryAccounting::new());
    let session = build_session(
        config,
        Arc::clone(&pool),
        SessionTable::Parquet(ParquetSession {
            tables: Vec::new(),
            store: Arc::new(ravel_parquet::TenantParquetStore::new(tenant)),
        }),
        false,
        SpillDecision::Disabled,
        // ES|QL has its own function set; the SQL ipcrypt UDFs never apply.
        &crate::SessionPrincipal::without_capabilities(tenant),
    )
    .map_err(|e| SqlError::Plan(e.to_string()))?;
    let tables = StatementTables::one(None);
    let context = LoweringContext::snapshot(&session, Vec::new(), &tables);
    let plan = prepared
        .lower(&context)
        .await
        .map_err(|e| SqlError::Plan(e.to_string()))?;
    admit_lowered_plan(&plan, &context, &tables, prepared.policy())?;
    let frame = session
        .execute_logical_plan(plan)
        .await
        .map_err(|e| SqlError::Plan(e.to_string()))?;
    let schema = Arc::clone(frame.schema().inner());
    let batches = frame
        .collect()
        .await
        .map_err(|e| execution_error(e, &pool))?;
    if let Some(message) = breach.message() {
        return Err(SqlError::ResourcesExhausted(message.to_string()));
    }
    Ok(QueryOutput::new(schema, batches))
}

fn execution_error(err: DataFusionError, pool: &Arc<dyn MemoryPool>) -> SqlError {
    match err.find_root() {
        DataFusionError::ResourcesExhausted(message) => {
            let limit = match pool.memory_limit() {
                MemoryLimit::Finite(limit) => limit,
                MemoryLimit::Infinite | MemoryLimit::Unknown => usize::MAX,
            };
            SqlError::resources_exhausted_reattributed(message, pool.reserved(), limit)
        }
        _ => SqlError::Execution(err.to_string()),
    }
}
