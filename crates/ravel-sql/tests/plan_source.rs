//! A statement that is not SQL text, run through `SqlExecutor`.
//!
//! The statements here are built with DataFusion's `LogicalPlanBuilder` and
//! expression API over the `LoweringContext` the executor hands the lowering,
//! which is what a non-SQL front end does. Each one counts its lowerings and
//! records the provider it scanned, so a test can tell the classification
//! session, the executed session and a retry's rebuilt session apart.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::{MemTable, TableProvider, provider_as_source, source_as_provider};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction};
use datafusion::logical_expr::{
    AggregateUDF, Expr, LogicalPlan, LogicalPlanBuilder, ScalarUDF, cast, col, exists, lit,
};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{RequestBudgets, RequestLimit};
use ravel_sql::{
    DialectPolicy, ErrorClass, LiveAccounting, LoweredStatement, LoweringContext, MSG_INTERNAL,
    SpillConfig, SqlConfig, SqlError, SqlOutcome, SqlRequest, StatementRequest, StatementTarget,
    TargetSignal, ValidationError,
};
use ravel_types::{Signal, TenantId, logstream};
use util::{Fixture, request, tenant_id};
use uuid::Uuid;

/// `(ts_ns, severity_num, severity_text, body)` for each published record.
const RECORDS: [(i64, u8, &str, &str); 6] = [
    (1_000, 9, "INFO", "alpha"),
    (1_001, 13, "WARN", "bravo"),
    (1_002, 9, "INFO", "charlie"),
    (1_003, 5, "DEBUG", "delta"),
    (1_004, 13, "WARN", "echo"),
    (1_005, 9, "INFO", "foxtrot"),
];

/// The SQL the default programmatic statement is the equivalent of.
const GROUPED_SQL: &str = "SELECT severity_text, count(*) AS n FROM logs \
     WHERE severity_num >= 9 GROUP BY severity_text ORDER BY severity_text";

/// Lowers against the context it is handed; the second argument is the
/// 1-based number of this call.
type Lowering =
    dyn Fn(&LoweringContext, usize) -> Result<LogicalPlan, DataFusionError> + Send + Sync;

/// A programmatic statement over a fixed target: `build` lowers it against the
/// context it is handed.
struct Programmatic {
    target: StatementTarget,
    policy: DialectPolicy,
    build: Box<Lowering>,
    lowerings: AtomicUsize,
    /// The `logs` provider each lowering's context held, kept alive so two
    /// sessions' providers cannot share an address.
    providers: Mutex<Vec<Arc<dyn TableProvider>>>,
}

impl std::fmt::Debug for Programmatic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Programmatic")
            .field("target", &self.target)
            .field("lowerings", &self.lowerings)
            .finish_non_exhaustive()
    }
}

impl Programmatic {
    fn over_logs<F>(policy: DialectPolicy, build: F) -> Self
    where
        F: Fn(&LoweringContext, usize) -> Result<LogicalPlan, DataFusionError>
            + Send
            + Sync
            + 'static,
    {
        Programmatic {
            target: StatementTarget::Signal(TargetSignal::Logs),
            policy,
            build: Box::new(build),
            lowerings: AtomicUsize::new(0),
            providers: Mutex::new(Vec::new()),
        }
    }

    fn lowerings(&self) -> usize {
        self.lowerings.load(Ordering::SeqCst)
    }

    /// How many different `logs` providers the lowerings were handed: one
    /// per session, since every session is built over its own provider.
    fn distinct_sessions(&self) -> usize {
        let providers = self.providers.lock().unwrap();
        let mut distinct: Vec<&Arc<dyn TableProvider>> = Vec::new();
        for provider in providers.iter() {
            if !distinct.iter().any(|seen| Arc::ptr_eq(seen, provider)) {
                distinct.push(provider);
            }
        }
        distinct.len()
    }
}

#[async_trait]
impl LoweredStatement for Programmatic {
    fn target(&self) -> StatementTarget {
        self.target.clone()
    }

    fn text(&self) -> &str {
        "programmatic statement"
    }

    fn policy(&self) -> &DialectPolicy {
        &self.policy
    }

    async fn lower(&self, ctx: &LoweringContext) -> Result<LogicalPlan, DataFusionError> {
        let call = self.lowerings.fetch_add(1, Ordering::SeqCst) + 1;
        let scan = logs_scan_node(ctx)?;
        self.providers
            .lock()
            .unwrap()
            .push(source_as_provider(&scan.source)?);
        (self.build)(ctx, call)
    }
}

/// The bare `TableScan` of `logs` the context builds.
fn logs_scan_node(
    ctx: &LoweringContext,
) -> Result<datafusion::logical_expr::TableScan, DataFusionError> {
    match ctx.scan("logs")?.build()? {
        LogicalPlan::TableScan(scan) => Ok(scan),
        other => Err(DataFusionError::Internal(format!(
            "the context built {other:?} for a scan"
        ))),
    }
}

/// A call carrying `func` itself. `AggregateUDF::call` would wrap a copy,
/// which admission refuses as a foreign instance.
fn aggregate_call(func: Arc<AggregateUDF>, args: Vec<Expr>) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        func,
        args,
        false,
        None,
        Vec::new(),
        None,
    ))
}

fn scalar_call(func: Arc<ScalarUDF>, args: Vec<Expr>) -> Expr {
    Expr::ScalarFunction(ScalarFunction::new_udf(func, args))
}

fn count_policy() -> DialectPolicy {
    DialectPolicy::new(Vec::<String>::new(), ["count"], Vec::<String>::new())
}

/// The programmatic equivalent of [`GROUPED_SQL`], calling the session's own
/// `count`.
fn grouped_statement() -> Programmatic {
    Programmatic::over_logs(count_policy(), |ctx, _call| {
        let count = ctx.aggregate("count")?;
        ctx.scan("logs")?
            .filter(col("severity_num").gt_eq(lit(9u8)))?
            .aggregate(
                vec![col("severity_text")],
                vec![aggregate_call(count, vec![lit(1i64)]).alias("n")],
            )?
            .sort(vec![col("severity_text").sort(true, false)])?
            .build()
    })
}

/// The grouped count with no `ORDER BY`, the shape the spill grant is decided
/// for.
const UNORDERED_GROUPED_SQL: &str =
    "SELECT severity_text, count(*) AS n FROM logs GROUP BY severity_text";

fn unordered_grouped_statement() -> Programmatic {
    Programmatic::over_logs(count_policy(), |ctx, _call| {
        let count = ctx.aggregate("count")?;
        ctx.scan("logs")?
            .aggregate(
                vec![col("severity_text")],
                vec![aggregate_call(count, vec![lit(1i64)]).alias("n")],
            )?
            .build()
    })
}

fn statement_request() -> StatementRequest {
    let req = request("");
    StatementRequest {
        window: req.window,
        min_tokens: req.min_tokens,
        now_ns: req.now_ns,
        deadline: req.deadline,
        row_window: req.row_window,
        max_rows: req.max_rows,
        budgets: req.budgets,
    }
}

async fn run_statement(
    fixture: &Fixture,
    tenant: &TenantId,
    statement: &Programmatic,
    req: &StatementRequest,
) -> Result<SqlOutcome, SqlError> {
    fixture
        .executor
        .execute_statement_accounted(tenant.hash(), statement, req, &LiveAccounting::new())
        .await
}

async fn run_sql(
    fixture: &Fixture,
    tenant: &TenantId,
    req: &SqlRequest,
) -> Result<SqlOutcome, SqlError> {
    fixture
        .executor
        .execute_accounted(tenant.hash(), req, &LiveAccounting::new())
        .await
}

/// A fixture over a fresh `FaultStore` carrying `plan`, with the logs records
/// published for `tenant`.
async fn logs_fixture(
    plan: FaultPlan,
    tenant: &TenantId,
    config: SqlConfig,
) -> (Arc<FaultStore<MemoryStore>>, Fixture) {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    publish_logs(backend.as_ref(), tenant).await;
    let fixture = Fixture::build(backend, &[], config, 1 << 30).await;
    (store, fixture)
}

/// Every cell of every row, rendered with Arrow's display formatter.
fn rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut out = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            out.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| array_value_to_string(column, row).expect("displayable"))
                    .collect(),
            );
        }
    }
    out
}

fn owned(rows: &[&[&str]]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| row.iter().map(|cell| (*cell).to_string()).collect())
        .collect()
}

fn assert_not_admitted(err: &SqlError, needle: &str) {
    assert!(
        matches!(err, SqlError::PlanNotAdmitted(reason) if reason.contains(needle)),
        "expected PlanNotAdmitted naming {needle:?}, got {err:?}"
    );
    assert_eq!(err.class(), ErrorClass::Internal);
    assert_eq!(err.client_message(), MSG_INTERNAL);
}

/// The programmatic grouped count returns the rows worked out by hand from
/// [`RECORDS`], reports the same per-phase accounting as the SQL statement it
/// is the equivalent of on an identical fixture, and is refused by a request
/// budget with the same error SQL gets under that budget.
#[tokio::test]
async fn programmatic_statement_runs_under_admission_budgets_and_accounting() {
    let tenant = tenant_id("acme-logs");

    // severity_num >= 9 keeps INFO x3 (alpha, charlie, foxtrot) and WARN x2
    // (bravo, echo); DEBUG (delta) is filtered out.
    let expected = owned(&[&["INFO", "3"], &["WARN", "2"]]);

    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = grouped_statement();
    let lowered = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect("the programmatic statement runs");
    assert_eq!(rows(lowered.output.batches()), expected);
    assert_eq!(lowered.stats.attempts, 1);

    let (_store, sql_fixture) =
        logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let sql = run_sql(&sql_fixture, &tenant, &request(GROUPED_SQL))
        .await
        .expect("the SQL equivalent runs");
    assert_eq!(rows(sql.output.batches()), expected);
    assert!(
        lowered.phase_accounting.scan.total_s3_requests() > 0,
        "the comparison below is vacuous without scan requests: {:?}",
        lowered.phase_accounting
    );
    assert_eq!(lowered.phase_accounting, sql.phase_accounting);
    assert_eq!(lowered.accounting, sql.accounting);

    let budgets = Some(RequestBudgets {
        max_store_requests: Some(RequestLimit::Bounded(1)),
        ..RequestBudgets::default()
    });
    let lowered_err = run_statement(
        &fixture,
        &tenant,
        &grouped_statement(),
        &StatementRequest {
            budgets,
            ..statement_request()
        },
    )
    .await
    .expect_err("a one-request budget refuses the programmatic statement");
    let sql_err = run_sql(
        &sql_fixture,
        &tenant,
        &SqlRequest {
            budgets,
            ..request(GROUPED_SQL)
        },
    )
    .await
    .expect_err("a one-request budget refuses the SQL equivalent");
    assert!(
        matches!(lowered_err, SqlError::RequestBudgetExceeded { max: 1, .. }),
        "got {lowered_err:?}"
    );
    assert!(
        matches!(sql_err, SqlError::RequestBudgetExceeded { max: 1, .. }),
        "got {sql_err:?}"
    );
    assert_eq!(lowered_err.class(), sql_err.class());
}

/// A segment that vanishes before the first row re-resolves and retries, and
/// the retry lowers the statement again against its own rebuilt session
/// rather than reusing the first attempt's plan.
#[tokio::test]
async fn programmatic_statement_is_lowered_again_on_snapshot_retry() {
    let tenant = tenant_id("acme-logs");
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(".rseg")
            .with_occurrence(Occurrence::Nth(1)),
    );
    // No spill and no parallel final aggregation: the executor builds no
    // classification session for a logs statement without declared columns,
    // so every lowering below is on the executed path.
    let config = SqlConfig {
        parallel_final_aggregation: false,
        ..SqlConfig::default()
    };
    let (store, fixture) = logs_fixture(plan, &tenant, config).await;
    let statement = grouped_statement();

    let outcome = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect("the retry recovers the programmatic statement");

    assert_eq!(
        store.fault_count(Op::Get, FaultKind::NotFoundBlip),
        1,
        "the injected NotFound must have fired exactly once"
    );
    assert_eq!(outcome.stats.attempts, 2);
    // One executed-path lowering per attempt, each against its own session.
    assert_eq!(statement.lowerings(), 2);
    assert_eq!(statement.distinct_sessions(), 2);
    assert_eq!(
        rows(outcome.output.batches()),
        owned(&[&["INFO", "3"], &["WARN", "2"]])
    );
}

/// The `ravel-spill-*` scratch subdirectories under `root`. The executor
/// creates one when it grants a statement spill and removes it when the
/// statement ends.
fn spill_subdirs(root: &Path) -> usize {
    std::fs::read_dir(root)
        .expect("scratch root is readable")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("ravel-spill-"))
        })
        .count()
}

fn spill_enabled_config(dir: &Path) -> SqlConfig {
    SqlConfig {
        spill: Some(SpillConfig {
            dir: dir.to_path_buf(),
            max_bytes: 64 * 1024 * 1024,
        }),
        ..SqlConfig::default()
    }
}

/// Runs `run` with the first data-object GET held, and returns how many spill
/// scratch subdirectories existed while it was held: the statement's spill
/// grant, observed before any row is read.
async fn spill_grant_while_scanning<F>(
    store: &FaultStore<MemoryStore>,
    scratch: &Path,
    run: F,
) -> (usize, SqlOutcome)
where
    F: std::future::Future<Output = Result<SqlOutcome, SqlError>>,
{
    let gate = store.hold(Op::Get, Some(".rseg".to_string()), Occurrence::Nth(1));
    let observe = async {
        gate.wait_until_held(1).await;
        let granted = spill_subdirs(scratch);
        for id in gate.held() {
            gate.release(id);
        }
        granted
    };
    let mut run = std::pin::pin!(run);
    let granted = tokio::select! {
        granted = observe => granted,
        outcome = &mut run => panic!("the statement ended before its first scan GET: {outcome:?}"),
    };
    (granted, run.await.expect("the statement runs"))
}

/// With spill configured the executor builds a classification session, and
/// the programmatic statement is lowered for it as well as for the executed
/// session. The spill grant read off that classification plan matches the SQL
/// equivalent's.
#[tokio::test]
async fn classification_plan_is_built_for_a_programmatic_statement() {
    let tenant = tenant_id("acme-logs");
    let expected = owned(&[&["DEBUG", "1"], &["INFO", "3"], &["WARN", "2"]]);

    let sql_scratch = tempfile::tempdir().expect("scratch root");
    let (sql_store, sql_fixture) = logs_fixture(
        FaultPlan::empty(),
        &tenant,
        spill_enabled_config(sql_scratch.path()),
    )
    .await;
    let sql_req = request(UNORDERED_GROUPED_SQL);
    let (sql_granted, sql) = spill_grant_while_scanning(
        &sql_store,
        sql_scratch.path(),
        run_sql(&sql_fixture, &tenant, &sql_req),
    )
    .await;
    assert_eq!(
        sql_granted, 1,
        "the SQL equivalent must be granted spill, else the comparison is vacuous"
    );

    let scratch = tempfile::tempdir().expect("scratch root");
    let (store, fixture) = logs_fixture(
        FaultPlan::empty(),
        &tenant,
        spill_enabled_config(scratch.path()),
    )
    .await;
    let statement = unordered_grouped_statement();
    let req = statement_request();
    let (granted, lowered) = spill_grant_while_scanning(
        &store,
        scratch.path(),
        run_statement(&fixture, &tenant, &statement, &req),
    )
    .await;

    assert_eq!(
        granted, sql_granted,
        "the programmatic aggregate must get the SQL equivalent's spill decision"
    );
    assert_eq!(statement.lowerings(), 2, "one classification, one executed");
    assert_eq!(statement.distinct_sessions(), 2);

    let mut sql_rows = rows(sql.output.batches());
    sql_rows.sort();
    let mut lowered_rows = rows(lowered.output.batches());
    lowered_rows.sort();
    assert_eq!(sql_rows, expected);
    assert_eq!(lowered_rows, expected);
}

/// A lowered `length()` taken from the session by its registered name is the
/// session's own instance, so admission lets it through.
#[tokio::test]
async fn lowered_length_by_its_registered_name_is_admitted() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(
        DialectPolicy::new(["length"], Vec::<String>::new(), Vec::<String>::new()),
        |ctx, _call| {
            let length = ctx.scalar("length")?;
            ctx.scan("logs")?
                .project(vec![scalar_call(length, vec![col("body")]).alias("len")])?
                .build()
        },
    );

    let outcome = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect("the session's own length() is admitted");
    let mut lengths = rows(outcome.output.batches());
    lengths.sort();
    assert_eq!(
        lengths,
        owned(&[&["4"], &["5"], &["5"], &["5"], &["7"], &["7"]])
    );
}

#[tokio::test]
async fn lowered_plan_with_an_unlisted_function_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(count_policy(), |ctx, _call| {
        let upper = ctx.scalar("upper")?;
        ctx.scan("logs")?
            .project(vec![scalar_call(upper, vec![col("body")])])?
            .build()
    });

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("a function the policy does not list is refused");
    assert_not_admitted(&err, "does not list");
}

/// DataFusion's own `min` under the admitted name `min`: the session
/// registered a replacement under that name, so the call carries a different
/// instance and is refused although its name is listed.
#[tokio::test]
async fn lowered_plan_with_a_foreign_function_instance_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(
        DialectPolicy::new(Vec::<String>::new(), ["min"], Vec::<String>::new()),
        |ctx, _call| {
            let foreign = datafusion::functions_aggregate::min_max::min_udaf();
            assert!(
                !Arc::ptr_eq(&foreign, &ctx.aggregate("min")?),
                "the session must register its own min for this case to mean anything"
            );
            assert_eq!(foreign.name(), "min");
            ctx.scan("logs")?
                .aggregate(
                    Vec::<Expr>::new(),
                    vec![aggregate_call(foreign, vec![col("ts")])],
                )?
                .build()
        },
    );

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("a same-named foreign instance is refused");
    assert_not_admitted(&err, "not the session's instance");
}

/// A scan outside the target is refused by name, and a scan of the target
/// table through a provider the session did not register is refused by
/// identity.
#[tokio::test]
async fn lowered_plan_scanning_another_table_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;

    fn foreign_table() -> Arc<MemTable> {
        let schema = Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, false)]));
        Arc::new(MemTable::try_new(schema, vec![vec![]]).expect("empty table"))
    }

    let elsewhere = Programmatic::over_logs(DialectPolicy::default(), |_ctx, _call| {
        LogicalPlanBuilder::scan("samples", provider_as_source(foreign_table()), None)?.build()
    });
    let err = run_statement(&fixture, &tenant, &elsewhere, &statement_request())
        .await
        .expect_err("a scan outside the target is refused");
    assert_not_admitted(&err, "not in its target");

    let impostor = Programmatic::over_logs(DialectPolicy::default(), |_ctx, _call| {
        LogicalPlanBuilder::scan("logs", provider_as_source(foreign_table()), None)?.build()
    });
    let err = run_statement(&fixture, &tenant, &impostor, &statement_request())
        .await
        .expect_err("a foreign provider under the target's name is refused");
    assert_not_admitted(&err, "did not register");
}

#[tokio::test]
async fn lowered_plan_with_an_unadmitted_node_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(DialectPolicy::default(), |ctx, _call| {
        let right = ctx.scan("logs")?.alias("b")?.build()?;
        ctx.scan("logs")?.alias("a")?.cross_join(right)?.build()
    });

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("a join is refused");
    assert_not_admitted(&err, "unadmitted Cross");
}

/// A subquery expression is walked as its own `Subquery` node, which is not
/// admitted even when the plan inside it would be.
#[tokio::test]
async fn lowered_plan_with_a_subquery_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(DialectPolicy::default(), |ctx, _call| {
        let inner = ctx.scan("logs")?.project(vec![col("ts")])?.build()?;
        ctx.scan("logs")?
            .filter(exists(Arc::new(inner)))?
            .project(vec![col("body")])?
            .build()
    });

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("a subquery is refused");
    assert_not_admitted(&err, "unadmitted Subquery");
}

/// `AggregateUDF::call` wraps a copy of the session's instance in a new
/// allocation, and the copy is refused like any foreign instance.
#[tokio::test]
async fn lowered_plan_with_a_copy_of_the_session_instance_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(count_policy(), |ctx, _call| {
        let registered = ctx.aggregate("count")?;
        let copied = registered.call(vec![lit(1)]);
        match &copied {
            Expr::AggregateFunction(call) => assert!(
                !Arc::ptr_eq(&call.func, &registered),
                "the helper must copy the instance for this case to mean anything"
            ),
            other => panic!("count built {other:?}"),
        }
        ctx.scan("logs")?
            .aggregate(Vec::<Expr>::new(), vec![copied])?
            .build()
    });

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("a copy of the session's count is refused");
    assert_not_admitted(&err, "not the session's instance");
}

/// The Parquet signal names no table, so it is not a statement target.
#[tokio::test]
async fn parquet_signal_target_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let mut statement = grouped_statement();
    statement.target = StatementTarget::Signal(TargetSignal::Parquet);

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("the Parquet signal target is refused");
    assert_not_admitted(&err, "Parquet target");
    assert_eq!(statement.lowerings(), 0, "refused before any lowering");
}

/// SQL through the routed path: exact rows, or the exact error variant, for
/// each statement shape the routing touched.
#[tokio::test]
async fn sql_behaviour_is_unchanged() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let ok = |sql: &'static str| {
        let fixture = &fixture;
        let tenant = &tenant;
        async move {
            let outcome = run_sql(fixture, tenant, &request(sql))
                .await
                .unwrap_or_else(|err| panic!("{sql}: {err:?}"));
            rows(outcome.output.batches())
        }
    };

    assert_eq!(
        ok("SELECT body FROM logs WHERE severity_num = 13 ORDER BY ts").await,
        owned(&[&["bravo"], &["echo"]])
    );
    assert_eq!(
        ok(GROUPED_SQL).await,
        owned(&[&["INFO", "3"], &["WARN", "2"]])
    );
    assert_eq!(
        ok("SELECT body FROM logs ORDER BY ts DESC LIMIT 2").await,
        owned(&[&["foxtrot"], &["echo"]])
    );
    assert_eq!(
        ok(
            "SELECT DISTINCT ON (severity_text) severity_text, body FROM logs \
            ORDER BY severity_text, body DESC"
        )
        .await,
        owned(&[&["DEBUG", "delta"], &["INFO", "foxtrot"], &["WARN", "echo"]])
    );

    let err = run_sql(
        &fixture,
        &tenant,
        &request("SELECT l.body FROM logs l JOIN samples s ON l.ts = s.ts"),
    )
    .await
    .expect_err("a cross-signal statement is rejected");
    assert!(matches!(err, SqlError::CrossSignalQuery), "got {err:?}");

    let err = run_sql(&fixture, &tenant, &request("SELECT uuid() FROM logs"))
        .await
        .expect_err("an excluded function is rejected");
    assert!(
        matches!(
            err,
            SqlError::Validation(ValidationError::ExcludedScalar { ref name }) if name == "uuid"
        ),
        "got {err:?}"
    );
}

/// Publishes one RLOG object holding [`RECORDS`] plus its `Signal::Logs`
/// commit record.
async fn publish_logs(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("plan-source".to_string()),
    )];
    let stream_id = logstream::log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);

    let writer_id = Uuid::from_u128(9_300);
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant.hash().0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    for (ts_ns, severity_num, severity_text, body) in RECORDS {
        writer
            .push(LogRecord {
                stream_id,
                stream_attrs: stream_attrs.clone(),
                ts_ns,
                observed_ts_ns: ts_ns,
                severity_num,
                severity_text: severity_text.to_string(),
                body: body.to_string(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: Vec::new(),
            })
            .expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");

    let first_ts = RECORDS[0].0;
    let last_ts = RECORDS[RECORDS.len() - 1].0;
    let new_record = NewCommitRecord {
        tenant_hash: tenant.hash(),
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: [7u8; 32],
        sample_count: RECORDS.len() as u64,
        series_count: 1,
        min_event_ts_ns: first_ts,
        max_event_ts_ns: last_ts,
        min_ingest_ts_ns: first_ts,
        max_ingest_ts_ns: last_ts,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    };
    let rec = record::build(new_record).expect("valid logs commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("logs data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put rlog object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish logs commit record");
}

/// A lowering that scans `logs` and takes DataFusion's own `min` under the
/// listed name `min`, where the session registered its total-order `min`.
fn stock_min_statement() -> Programmatic {
    Programmatic::over_logs(min_policy(), |ctx, _call| stock_min_plan(ctx))
}

fn min_policy() -> DialectPolicy {
    DialectPolicy::new(Vec::<String>::new(), ["min"], Vec::<String>::new())
}

fn stock_min_plan(ctx: &LoweringContext) -> Result<LogicalPlan, DataFusionError> {
    let stock = datafusion::functions_aggregate::min_max::min_udaf();
    assert!(
        !Arc::ptr_eq(&stock, &ctx.aggregate("min")?),
        "the session must register its own min for this case to mean anything"
    );
    ctx.scan("logs")?
        .aggregate(
            Vec::<Expr>::new(),
            vec![aggregate_call(stock, vec![col("body")])],
        )?
        .build()
}

/// [`grouped_statement`]'s plan for a context.
fn grouped_plan(ctx: &LoweringContext) -> Result<LogicalPlan, DataFusionError> {
    let count = ctx.aggregate("count")?;
    ctx.scan("logs")?
        .filter(col("severity_num").gt_eq(lit(9u8)))?
        .aggregate(
            vec![col("severity_text")],
            vec![aggregate_call(count, vec![lit(1i64)]).alias("n")],
        )?
        .sort(vec![col("severity_text").sort(true, false)])?
        .build()
}

/// No spill and no parallel final aggregation: a logs statement without
/// declared columns then builds no classification session, so the executed
/// session is the only one that lowers and admits.
fn executed_only_config() -> SqlConfig {
    SqlConfig {
        parallel_final_aggregation: false,
        ..SqlConfig::default()
    }
}

/// DataFusion's stock `character_length` under the listed name `length`. The
/// session replaced the function registered under that name, so the stock
/// handle is a different instance and is refused.
#[tokio::test]
async fn stock_scalar_under_a_name_the_session_replaced_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(
        DialectPolicy::new(["length"], Vec::<String>::new(), Vec::<String>::new()),
        |ctx, _call| {
            let stock = datafusion::functions::unicode::character_length();
            assert!(
                stock.aliases().iter().any(|alias| alias == "length"),
                "the stock function must answer to length for this case to mean anything"
            );
            assert!(
                !Arc::ptr_eq(&stock, &ctx.scalar("length")?),
                "the session must replace length for this case to mean anything"
            );
            ctx.scan("logs")?
                .project(vec![scalar_call(stock, vec![col("body")]).alias("len")])?
                .build()
        },
    );

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("the stock character_length under length is refused");
    assert_not_admitted(&err, "not the session's instance");
}

/// The session did not replace `count`, so DataFusion's own handle is the
/// instance the session registered, and admission lets it through.
#[tokio::test]
async fn stock_count_is_the_session_instance_and_is_admitted() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(count_policy(), |ctx, _call| {
        let stock = datafusion::functions_aggregate::count::count_udaf();
        assert!(
            Arc::ptr_eq(&stock, &ctx.aggregate("count")?),
            "the session registers DataFusion's own count"
        );
        ctx.scan("logs")?
            .aggregate(
                Vec::<Expr>::new(),
                vec![aggregate_call(stock, vec![lit(1i64)]).alias("n")],
            )?
            .build()
    });

    let outcome = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect("the stock count is the session's count");
    assert_eq!(rows(outcome.output.batches()), owned(&[&["6"]]));
}

/// A policy that lists only the alias `length` admits the instance fetched by
/// its canonical name `character_length`: both names hold one allocation.
#[tokio::test]
async fn policy_listing_an_alias_admits_the_instance_fetched_by_its_canonical_name() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(
        DialectPolicy::new(["length"], Vec::<String>::new(), Vec::<String>::new()),
        |ctx, _call| {
            let canonical = ctx.scalar("character_length")?;
            assert_eq!(canonical.name(), "character_length");
            assert!(Arc::ptr_eq(&canonical, &ctx.scalar("length")?));
            ctx.scan("logs")?
                .project(vec![scalar_call(canonical, vec![col("body")]).alias("len")])?
                .build()
        },
    );

    let outcome = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect("the alias admits the canonical name's instance");
    let mut lengths = rows(outcome.output.batches());
    lengths.sort();
    assert_eq!(
        lengths,
        owned(&[&["4"], &["5"], &["5"], &["5"], &["7"], &["7"]])
    );
}

/// The executed session refuses a foreign instance on its own: no
/// classification session is built, so the one lowering is the executed one.
#[tokio::test]
async fn foreign_function_is_refused_at_the_executed_site() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, executed_only_config()).await;
    let statement = stock_min_statement();

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("the executed site refuses a foreign min");
    assert_not_admitted(&err, "not the session's instance");
    assert_eq!(statement.lowerings(), 1, "only the executed site lowered");
}

/// Clean for the classification session, foreign for the executed one: the
/// executed site's own admission is what refuses it.
#[tokio::test]
async fn lowering_refused_only_on_its_second_call_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(
        DialectPolicy::new(Vec::<String>::new(), ["count", "min"], Vec::<String>::new()),
        |ctx, call| match call {
            1 => grouped_plan(ctx),
            _ => stock_min_plan(ctx),
        },
    );

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("the second lowering is refused");
    assert_not_admitted(&err, "not the session's instance");
    assert_eq!(statement.lowerings(), 2, "one classification, one executed");
}

/// The snapshot retry's executed lowering is admitted on its own: the first
/// attempt's lowering is clean, the retry's is foreign.
#[tokio::test]
async fn retry_lowering_is_admitted_again() {
    let tenant = tenant_id("acme-logs");
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Get, ScriptedFault::NotFoundBlip)
            .with_key_contains(".rseg")
            .with_occurrence(Occurrence::Nth(1)),
    );
    let (store, fixture) = logs_fixture(plan, &tenant, executed_only_config()).await;
    let statement = Programmatic::over_logs(
        DialectPolicy::new(Vec::<String>::new(), ["count", "min"], Vec::<String>::new()),
        |ctx, call| match call {
            1 => grouped_plan(ctx),
            _ => stock_min_plan(ctx),
        },
    );

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("the retry's lowering is refused");
    assert_eq!(
        store.fault_count(Op::Get, FaultKind::NotFoundBlip),
        1,
        "the injected NotFound must have fired, else there was no retry"
    );
    assert_not_admitted(&err, "not the session's instance");
    assert_eq!(statement.lowerings(), 2, "one per attempt");
    assert_eq!(statement.distinct_sessions(), 2);
}

/// A scan whose `filters` are set, the shape only the optimizer's pushdown
/// produces, is refused.
#[tokio::test]
async fn lowered_scan_with_filters_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(DialectPolicy::default(), |ctx, _call| {
        let mut scan = logs_scan_node(ctx)?;
        scan.filters = vec![col("severity_num").gt_eq(lit(9u8))];
        Ok(LogicalPlan::TableScan(scan))
    });

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("a scan carrying filters is refused");
    assert_not_admitted(&err, "with filters");
}

/// A scan whose `fetch` is set is refused.
#[tokio::test]
async fn lowered_scan_with_a_fetch_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(DialectPolicy::default(), |ctx, _call| {
        let mut scan = logs_scan_node(ctx)?;
        scan.fetch = Some(1);
        Ok(LogicalPlan::TableScan(scan))
    });

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("a scan carrying a fetch is refused");
    assert_not_admitted(&err, "with a fetch");
}

/// The classification lowering sums an integer column, which is exact-typed;
/// the executed lowering sums it cast to `Float64`, which is not. The
/// exact-typed decision the classification took does not hold for the plan
/// that would run, so the statement is refused.
#[tokio::test]
async fn lowering_that_changes_an_aggregate_input_type_is_refused() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(
        DialectPolicy::new(Vec::<String>::new(), ["sum"], Vec::<String>::new()),
        |ctx, call| {
            let sum = ctx.aggregate("sum")?;
            let input = match call {
                1 => col("severity_num"),
                _ => cast(col("severity_num"), DataType::Float64),
            };
            ctx.scan("logs")?
                .aggregate(
                    vec![col("severity_text")],
                    vec![aggregate_call(sum, vec![input]).alias("s")],
                )?
                .build()
        },
    );

    let err = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect_err("a lowering whose aggregate changes type is refused");
    assert_not_admitted(&err, "not repeatable");
    assert_eq!(statement.lowerings(), 2, "one classification, one executed");
}

/// The control for the case above: the integer sum on both calls runs.
#[tokio::test]
async fn lowering_with_the_same_aggregate_on_both_calls_runs() {
    let tenant = tenant_id("acme-logs");
    let (_store, fixture) = logs_fixture(FaultPlan::empty(), &tenant, SqlConfig::default()).await;
    let statement = Programmatic::over_logs(
        DialectPolicy::new(Vec::<String>::new(), ["sum"], Vec::<String>::new()),
        |ctx, _call| {
            let sum = ctx.aggregate("sum")?;
            ctx.scan("logs")?
                .aggregate(
                    vec![col("severity_text")],
                    vec![aggregate_call(sum, vec![col("severity_num")]).alias("s")],
                )?
                .sort(vec![col("severity_text").sort(true, false)])?
                .build()
        },
    );

    let outcome = run_statement(&fixture, &tenant, &statement, &statement_request())
        .await
        .expect("a repeatable lowering runs");
    assert_eq!(
        rows(outcome.output.batches()),
        owned(&[&["DEBUG", "5"], &["INFO", "27"], &["WARN", "26"]])
    );
    assert_eq!(statement.lowerings(), 2);
}
