//! `ravel-cli sql explain` (ADR-2867 change 7 item 3, issue #2881): print a
//! statement's physical plan, or with `--analyze` run it and print the plan
//! the run executed, every operator carrying its own metrics.
//!
//! The statement is the plain `SELECT` the HTTP endpoint would accept, and it
//! passes the same `validate_query` gate. The word `EXPLAIN` is never part of
//! it: the plan is rendered here, from the executor's own plan, so the
//! endpoint's refusal of `EXPLAIN` statements is untouched.
//!
//! Nothing here writes a query-audit record. That is why the subcommand is
//! compiled only under the off-by-default `sql-explain` feature and refuses to
//! run without `--unaudited`, checked before the store is built.

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use ravel_catalog::{Catalog, CatalogConfig, DeclaredColumnType, read_config_values};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{
    DeclaredColumn, DeclaredType, ParquetPlan, PinnedPlanInputs, PinnedResolve, SpanSegmentFetcher,
    SqlConfig, SqlError, SqlExecutor, SqlRequest, StaticDeclaredColumns, validate_query,
};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{TenantHash, TenantId, TimeRange};

use crate::store::{StoreSelection, require_tenant_data_present};

/// The window the HTTP endpoint applies when a request names none: the hour
/// ending now.
const DEFAULT_WINDOW_NS: i64 = 3_600_000_000_000;

/// The refusal printed when `--unaudited` is missing.
pub const UNAUDITED_REQUIRED: &str = "sql explain runs SQL against tenant data without writing \
     a query-audit record; pass --unaudited to acknowledge that this run is not audited";

/// What `sql explain` was asked to do.
#[derive(Debug, Clone)]
pub struct ExplainOptions {
    pub tenant: String,
    pub sql: String,
    /// Run the statement and print the executed plan with its metrics, not
    /// only the plan.
    pub analyze: bool,
    /// The operator's acknowledgement that no query-audit record is written.
    pub unaudited: bool,
    /// Event-time window; `None` takes the HTTP endpoint's default, the hour
    /// ending at the command's clock reading.
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub shards: u32,
    pub max_ingest_lag_ns: Option<i64>,
    /// Wall deadline for the resolve, plan and (with `analyze`) the run.
    pub deadline: Duration,
    /// Stop an `--analyze` run after this many rows plus one.
    pub max_rows: Option<usize>,
    /// The engine's partition count; `None` keeps the `SqlConfig` default.
    pub sql_partitions: Option<usize>,
}

/// Refuse unless the operator passed `--unaudited`. Runs before anything
/// touches the store.
pub fn require_unaudited(unaudited: bool) -> anyhow::Result<()> {
    if unaudited {
        Ok(())
    } else {
        Err(anyhow::anyhow!(UNAUDITED_REQUIRED))
    }
}

/// Explain (and with `analyze`, run) `opts.sql` for `opts.tenant`, writing the
/// report to `out`.
pub async fn run(
    store: Arc<dyn ObjectStoreBackend>,
    selection: StoreSelection,
    opts: &ExplainOptions,
    now_ns: i64,
    out: &mut dyn Write,
) -> anyhow::Result<()> {
    require_unaudited(opts.unaudited)?;
    validate_query(&opts.sql).map_err(SqlError::from)?;
    let end_ns = opts.end_ns.unwrap_or(now_ns);
    let start_ns = opts
        .start_ns
        .unwrap_or_else(|| end_ns.saturating_sub(DEFAULT_WINDOW_NS));
    if start_ns > end_ns {
        anyhow::bail!("--start must not be after --end");
    }
    let config = sql_config(opts)?;

    let tenant_hash = TenantId::new(&opts.tenant).hash();
    require_tenant_data_present(
        selection,
        store.as_ref(),
        "sql explain",
        &opts.tenant,
        &tenant_hash,
    )
    .await?;
    let declared = durable_declared_columns(store.as_ref(), &tenant_hash).await?;
    let executor = executor(&store, opts, config, declared)?;
    let req = SqlRequest {
        sql: opts.sql.clone(),
        window: TimeRange { start_ns, end_ns },
        min_tokens: Vec::new(),
        now_ns,
        deadline: opts.deadline,
        row_window: false,
        max_rows: opts.max_rows,
        budgets: None,
    };

    if !opts.analyze {
        let report = executor.explain(tenant_hash, &req).await?;
        writeln!(out, "{}", report.plan_text.trim_end())?;
        return Ok(());
    }

    let millis = u64::try_from(req.deadline.as_millis()).unwrap_or(u64::MAX);
    let run = tokio::time::timeout(req.deadline, analyze(&executor, tenant_hash, &req))
        .await
        .unwrap_or(Err(SqlError::DeadlineExceeded { millis }))?;
    writeln!(out, "{}", run.plan_with_metrics.trim_end())?;
    match (run.row_cap_hit, req.max_rows) {
        (true, Some(cap)) => writeln!(out, "rows: {cap}+ (stopped at --max-rows)")?,
        _ => writeln!(out, "rows: {}", run.rows)?,
    }
    Ok(())
}

/// Resolve, plan and run `req` through the pinned path, the same resolve and
/// plan a Flight SQL query takes, keeping the executed plan.
async fn analyze(
    executor: &SqlExecutor,
    tenant_hash: TenantHash,
    req: &SqlRequest,
) -> Result<ravel_sql::AnalyzedRun, SqlError> {
    let accounting = QueryAccounting::new();
    let PinnedResolve {
        snapshot, parquet, ..
    } = executor
        .resolve_pinned(tenant_hash, req, &accounting)
        .await?;
    let declared = executor
        .resolve_declared_columns(tenant_hash, req.now_ns)
        .await;
    executor
        .plan_pinned_with_inputs(
            tenant_hash,
            snapshot,
            &req.sql,
            &accounting,
            PinnedPlanInputs {
                declared,
                parquet: ParquetPlan::Resolved(parquet),
                budgets: None,
            },
        )
        .await?
        .with_row_cap(req.max_rows)
        .execute_analyzed()
        .await
}

/// The engine configuration with `--sql-partitions` applied, refused by the
/// same `EngineConfig::validate` the server applies to its own.
fn sql_config(opts: &ExplainOptions) -> anyhow::Result<SqlConfig> {
    let mut config = SqlConfig::default();
    if let Some(partitions) = opts.sql_partitions {
        config.engine.sql_partition_count = Some(partitions);
    }
    config
        .engine
        .validate()
        .context("--sql-partitions is not a valid engine setting")?;
    Ok(config)
}

fn executor(
    store: &Arc<dyn ObjectStoreBackend>,
    opts: &ExplainOptions,
    config: SqlConfig,
    declared: Vec<DeclaredColumn>,
) -> anyhow::Result<SqlExecutor> {
    let mut catalog_config = CatalogConfig {
        shard_count: opts.shards,
        ..CatalogConfig::default()
    };
    if let Some(ns) = opts.max_ingest_lag_ns {
        catalog_config.max_ingest_lag_ns = ns;
    }
    let catalog = Catalog::new(Arc::clone(store), catalog_config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?
        .with_provisioning_enforcement();
    let max_tenant_bytes = config.max_query_bytes;
    Ok(SqlExecutor::new(
        Arc::new(catalog),
        SegmentFetcher::new(Arc::clone(store)),
        LogSegmentFetcher::new(Arc::clone(store)),
        SpanSegmentFetcher::new(Arc::clone(store)),
        config,
        max_tenant_bytes,
    )
    .with_declared_column_source(Arc::new(StaticDeclaredColumns::new(declared))))
}

/// The tenant's durable typed attribute columns (ADR-0090). A server's
/// `--typed-attr-columns` base is not visible from here, so a tenant with no
/// durable declaration plans against the zero-declaration `logs` schema.
async fn durable_declared_columns(
    store: &dyn ObjectStoreBackend,
    tenant_hash: &TenantHash,
) -> anyhow::Result<Vec<DeclaredColumn>> {
    let config = read_config_values(store, tenant_hash)
        .await
        .context("failed to read the tenant config record")?;
    let columns = config
        .and_then(|config| config.typed_attr_columns)
        .unwrap_or_default();
    Ok(columns
        .iter()
        .map(|column| {
            DeclaredColumn::new(
                column.key.clone(),
                match column.ty {
                    DeclaredColumnType::Str => DeclaredType::Str,
                    DeclaredColumnType::I64 => DeclaredType::I64,
                    DeclaredColumnType::Bool => DeclaredType::Bool,
                    DeclaredColumnType::Bytes => DeclaredType::Bytes,
                },
            )
        })
        .collect())
}
