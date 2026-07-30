//! The per-query SQL execution driver: validate, resolve, plan, execute.
//!
//! Request handling order is fixed by docs/arrow-datafusion-plan.md section 2
//! and is not an implementation detail:
//!
//! 1. Parse and validate (security invariant 1, crate::validate) -- before
//!    anything else, so a rejected statement costs no catalog LIST and
//!    builds no plan.
//! 2. Resolve the snapshot exactly once, through
//!    `catalog.resolve(&tenant_hash, signal, window, min_tokens, now_ns)`,
//!    with `now_ns` threaded in from the caller's injected clock (review F11;
//!    no `SystemTime::now()` in library logic). `signal` is chosen from the
//!    query's own `FROM` clause ([`SqlExecutor::target_signal`], ADR-0033):
//!    `Signal::Logs` when the query references the `logs` table, otherwise
//!    `Signal::Metrics`. A query referencing both tables is rejected here,
//!    before the LIST, because v1 admits one signal per query (decision C).
//! 3. Build the fresh single-tenant `SessionContext` around the owned
//!    `Snapshot`, registering the one table the query targets (security
//!    invariant 2, crate::session).
//! 4. Plan, then execute, draining the stream under the wall deadline.
//!
//! # Snapshot retry contract (plan section 2, review F9)
//!
//! `docs/consistency-model.md` mandates re-resolve-and-retry-once when a
//! pinned segment vanishes under a concurrent GC or compaction. On this path
//! that means:
//!
//! - A store `NotFound` raised **before the first batch has been emitted**
//!   re-resolves with the *same* `now_ns` and the *same* `min_tokens`,
//!   rebuilds the whole session (new pool, new provider, new context), and
//!   re-executes the query exactly once. A second `NotFound` fails
//!   [`SqlError::SnapshotInvalidated`].
//! - A store `NotFound` raised **after** at least one batch has been emitted
//!   fails [`SqlError::SnapshotInvalidated`] immediately, with zero retries:
//!   a streaming plan cannot be re-run after partial emission, and silently
//!   restarting it would duplicate already-emitted rows.
//!
//! "Emitted" means emitted by the plan's stream, which is the earliest point
//! at which a batch could have been handed to a client; buffering the result
//! before encoding does not move that line, and treating it as if it did
//! would make the second rule unreachable.
//!
//! # The pinned surface (ticket C1d)
//!
//! [`SqlExecutor::resolve_snapshot`] and [`SqlExecutor::plan_pinned`] split
//! the two halves above apart for a transport whose resolve and execute land
//! in different RPCs (Flight SQL, crate::flight). They are not a second
//! implementation: [`SqlExecutor::execute`] runs through `plan_pinned` too, so
//! there is exactly one place that builds a query's pool, provider, and
//! session, and exactly one `Catalog::resolve` call site. A transport that
//! reimplemented either would be free to drift on tenant accounting, on the
//! per-query session invariant, or on the retry contract; going through these
//! two methods is what makes that impossible rather than merely unlikely.
//!
//! # Deadline
//!
//! The wall deadline wraps the whole call, retry included, so a query cannot
//! double its budget by tripping the retry path. On expiry the stream is
//! dropped, which frees every `MemoryReservation` and returns the tenant's
//! reserved bytes (crate::memory, review F13); partial state is discarded,
//! never returned (docs/query-engine.md "Budgets").

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use futures::{Stream, StreamExt};
use ravel_catalog::{Catalog, Snapshot};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_types::{CommitToken, Signal, TenantHash, TimeRange};

use crate::config::SqlConfig;
use crate::error::SqlError;
use crate::logs_provider::LogsTableProvider;
use crate::memory::{CeilingBreach, TenantMemoryAccountant};
use crate::output::QueryOutput;
use crate::provider::RavelTableProvider;
use crate::session::{LOGS_TABLE, SAMPLES_TABLE, SessionTable, build_session};
use crate::validate::{referenced_base_tables, validate};

/// Which of the two v1 tables (and thus which `Signal`) a query targets.
/// A closed two-variant enum rather than `Signal` directly: `Signal` carries
/// variants (`Spans`, `Profiles`) the SQL surface has no table for, and the
/// executor must never resolve or register those.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TargetSignal {
    /// The `samples` table, resolved against `Signal::Metrics`.
    Metrics,
    /// The `logs` table, resolved against `Signal::Logs`.
    Logs,
}

impl TargetSignal {
    fn signal(self) -> Signal {
        match self {
            TargetSignal::Metrics => Signal::Metrics,
            TargetSignal::Logs => Signal::Logs,
        }
    }
}

/// One SQL request, fully resolved from its transport.
#[derive(Debug, Clone)]
pub struct SqlRequest {
    /// The raw statement text. Validated before use.
    pub sql: String,
    /// Event-time window handed to `Catalog::resolve`. The endpoint derives
    /// it from request parameters; the provider re-applies every predicate
    /// above the scan, so this only bounds which segments are listed.
    pub window: TimeRange,
    /// Read-your-write tokens (docs/catalog-and-mvcc.md step 4).
    pub min_tokens: Vec<CommitToken>,
    /// The injected clock reading that bounds the listing window. The same
    /// value is reused on the retry so both resolves see the same window.
    pub now_ns: i64,
    /// Wall deadline for the whole call, retry included.
    pub deadline: Duration,
}

/// What the executor actually did, for tests and operator metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SqlStats {
    /// `Catalog::resolve` calls. 1 normally, 2 when the retry contract fired.
    pub resolves: u32,
    /// Plan-and-execute attempts. Always equal to `resolves` on this path.
    pub attempts: u32,
    /// Batches the plan emitted across all attempts.
    pub batches_emitted: usize,
    /// Segments in the snapshot the successful attempt used.
    pub segments: usize,
}

/// A completed query.
#[derive(Debug, Clone)]
pub struct SqlOutcome {
    pub output: QueryOutput,
    pub stats: SqlStats,
}

/// Executes SQL for any tenant against one catalog and object store.
///
/// The executor holds no DataFusion state: it is the per-tenant memory
/// accountants and the immutable catalog/fetcher handles, nothing else.
/// Every query builds and drops its own `SessionContext` (security
/// invariant 2).
pub struct SqlExecutor {
    catalog: Arc<Catalog>,
    fetcher: SegmentFetcher,
    /// The RLOG/logs sibling of `fetcher` (ADR-0033). Used only when a query
    /// targets the `logs` table; a metrics-only query never touches it.
    log_fetcher: LogSegmentFetcher,
    config: SqlConfig,
    max_tenant_bytes: usize,
    /// Per-tenant byte accountants, created on first use and shared across
    /// that tenant's concurrent queries. This map is the one piece of state
    /// that intentionally outlives a query, and it holds no query, plan, or
    /// catalog data -- only a byte counter per tenant, so it cannot carry
    /// data across the tenant boundary.
    tenants: Mutex<HashMap<TenantHash, Arc<TenantMemoryAccountant>>>,
}

impl SqlExecutor {
    /// Build an executor. `max_tenant_bytes` is the ceiling each tenant's
    /// accountant enforces across that tenant's concurrent queries; the
    /// per-query ceiling comes from `config.max_query_bytes`.
    pub fn new(
        catalog: Arc<Catalog>,
        fetcher: SegmentFetcher,
        log_fetcher: LogSegmentFetcher,
        config: SqlConfig,
        max_tenant_bytes: usize,
    ) -> Self {
        SqlExecutor {
            catalog,
            fetcher,
            log_fetcher,
            config,
            max_tenant_bytes,
            tenants: Mutex::new(HashMap::new()),
        }
    }

    pub fn config(&self) -> &SqlConfig {
        &self.config
    }

    /// The accountant for `tenant`, creating it on first use.
    ///
    /// Exposed so the endpoint and the tenancy tests can read a tenant's
    /// reserved bytes without running a query through it.
    pub fn tenant_budget(&self, tenant: TenantHash) -> Arc<TenantMemoryAccountant> {
        let mut tenants = match self.tenants.lock() {
            Ok(guard) => guard,
            // A poisoned lock means another thread panicked while holding
            // it. The map is a plain HashMap of Arc counters with no
            // torn-state hazard, so recovering is safe and strictly better
            // than failing every subsequent query for the process's life.
            Err(poisoned) => poisoned.into_inner(),
        };
        Arc::clone(
            tenants
                .entry(tenant)
                .or_insert_with(|| TenantMemoryAccountant::new(self.max_tenant_bytes)),
        )
    }

    /// Validate, resolve, plan, and execute `req` for `tenant_hash`.
    pub async fn execute(
        &self,
        tenant_hash: TenantHash,
        req: &SqlRequest,
    ) -> Result<SqlOutcome, SqlError> {
        // Step 1: the security gate runs before any catalog or plan work.
        validate(&req.sql)?;

        let millis = u64::try_from(req.deadline.as_millis()).unwrap_or(u64::MAX);
        tokio::time::timeout(req.deadline, self.run(tenant_hash, req))
            .await
            .unwrap_or(Err(SqlError::DeadlineExceeded { millis }))
    }

    /// The resolve/plan/execute loop, minus validation and the deadline.
    async fn run(&self, tenant_hash: TenantHash, req: &SqlRequest) -> Result<SqlOutcome, SqlError> {
        let mut stats = SqlStats::default();

        // At most two passes: the original and the one retry the
        // consistency model allows.
        for attempt in 0..2u32 {
            let snapshot = self.resolve(tenant_hash, req).await?;
            stats.resolves += 1;
            stats.attempts += 1;
            stats.segments = snapshot.segments.len();

            let (result, emitted) = self.attempt(tenant_hash, req, snapshot).await;
            stats.batches_emitted += emitted;

            match result {
                Ok(output) => return Ok(SqlOutcome { output, stats }),
                Err(err) => match retry_decision(err.is_segment_not_found(), emitted, attempt) {
                    RetryDecision::RetryOnce => continue,
                    RetryDecision::FailInvalidated => return Err(SqlError::SnapshotInvalidated),
                    RetryDecision::Propagate => return Err(err),
                },
            }
        }

        // Unreachable: the loop either returns or `continue`s exactly once,
        // and the second pass always returns. Kept as a typed error rather
        // than `unreachable!()` because panicking in a query path is never
        // an acceptable failure mode.
        Err(SqlError::Internal(
            "snapshot retry loop exited without a result".to_string(),
        ))
    }

    /// One `Catalog::resolve` plus the `max_segments` budget check, exposed
    /// for a transport that resolves and executes in two separate RPCs.
    ///
    /// Flight SQL resolves at `GetFlightInfo`, pins the resulting segment set
    /// into its ticket, and executes against that pin at `DoGet` (review F18).
    /// It must reach `Catalog::resolve` through this call rather than its own,
    /// so both transports share one signature, one budget check, and one
    /// injected-clock discipline. Validation is *not* performed here: the
    /// caller runs [`crate::validate`] first, exactly as [`Self::execute`]
    /// does, so a rejected statement still costs no catalog LIST.
    pub async fn resolve_snapshot(
        &self,
        tenant_hash: TenantHash,
        req: &SqlRequest,
    ) -> Result<Snapshot, SqlError> {
        self.resolve(tenant_hash, req).await
    }

    /// Build the fresh per-query, single-tenant session over an already
    /// resolved `snapshot` and plan `sql` against it, without executing.
    ///
    /// This is the one construction path for a query's session: its own
    /// `TenantDelegatingPool` over the tenant's accountant, its own
    /// `RavelTableProvider` over the owned snapshot, its own `SessionContext`
    /// (security invariant 2, crate::session). Both [`Self::execute`] and the
    /// Flight SQL `DoGet` path go through it, which is what makes the two
    /// transports share the memory-accounting and cancellation behaviour
    /// rather than merely resemble it (review F13): the pool the returned
    /// query owns is dropped with it, and every `MemoryReservation` the plan
    /// took shrinks back through it into the tenant accountant.
    pub async fn plan_pinned(
        &self,
        tenant_hash: TenantHash,
        snapshot: Snapshot,
        sql: &str,
    ) -> Result<PinnedQuery, SqlError> {
        let (pool, breach) = self.config.query_pool(self.tenant_budget(tenant_hash));
        // Build the one table the query targets over the snapshot resolved for
        // its signal. `resolve` already resolved `snapshot` against exactly
        // this signal, so the provider and the snapshot always agree.
        let table = match Self::target_signal(sql)? {
            TargetSignal::Metrics => SessionTable::Metrics(Arc::new(RavelTableProvider::new(
                snapshot,
                tenant_hash,
                self.fetcher.clone(),
                self.config,
            ))),
            TargetSignal::Logs => SessionTable::Logs(Arc::new(LogsTableProvider::new(
                snapshot,
                self.log_fetcher.clone(),
                self.config,
            ))),
        };

        let ctx = build_session(&self.config, pool, table).map_err(plan_error)?;
        let frame = ctx.sql(sql).await.map_err(plan_error)?;
        let schema = frame.schema().inner().clone();
        Ok(PinnedQuery {
            ctx,
            frame,
            schema,
            breach,
        })
    }

    /// One `Catalog::resolve` plus the `max_segments` budget check. Resolves
    /// the signal the query's `FROM` clause targets ([`Self::target_signal`]),
    /// so a metrics-only query never lists the logs keyspace and vice versa.
    async fn resolve(
        &self,
        tenant_hash: TenantHash,
        req: &SqlRequest,
    ) -> Result<Snapshot, SqlError> {
        let signal = Self::target_signal(&req.sql)?.signal();
        let snapshot = self
            .catalog
            .resolve(
                &tenant_hash,
                signal,
                req.window,
                &req.min_tokens,
                req.now_ns,
            )
            .await?;
        if snapshot.segments.len() > self.config.engine.max_segments {
            return Err(SqlError::TooManySegments {
                count: snapshot.segments.len(),
                max: self.config.engine.max_segments,
            });
        }
        Ok(snapshot)
    }

    /// The table (and thus the signal) a query resolves against, decided from
    /// its `FROM` clause before any planning (ADR-0033 "one SQL endpoint, two
    /// tables").
    ///
    /// The referenced table names come from the same `DFParser` front end the
    /// validation gate uses ([`referenced_base_tables`]), never a raw-text
    /// scan. The mapping:
    ///
    /// - references `logs` (and not `samples`) -> [`TargetSignal::Logs`].
    /// - references `samples`, or references neither table ->
    ///   [`TargetSignal::Metrics`].
    /// - references both -> [`SqlError::CrossSignalQuery`], rejected before the
    ///   catalog LIST (decision C: v1 admits one signal per query).
    ///
    /// The "neither" case (a constant query such as `SELECT 1`, or one whose
    /// only source is a CTE with no base table) defaults to `Metrics`: it
    /// preserves the pre-ADR-0033 behavior exactly -- such a query resolved a
    /// metrics snapshot and never touched it -- and `crate::validate` already
    /// rules out anything that would need a data source it cannot reach. Only
    /// the both-tables case is genuinely unsupported, so only it is an error.
    fn target_signal(sql: &str) -> Result<TargetSignal, SqlError> {
        let tables = referenced_base_tables(sql)?;
        let has_samples = tables.contains(SAMPLES_TABLE);
        let has_logs = tables.contains(LOGS_TABLE);
        match (has_samples, has_logs) {
            (true, true) => Err(SqlError::CrossSignalQuery),
            (_, true) => Ok(TargetSignal::Logs),
            (_, false) => Ok(TargetSignal::Metrics),
        }
    }

    /// Build a session over `snapshot`, plan, and drain the stream.
    ///
    /// Returns the batch count alongside the result because the retry
    /// contract turns on whether anything was emitted before the failure.
    async fn attempt(
        &self,
        tenant_hash: TenantHash,
        req: &SqlRequest,
        snapshot: Snapshot,
    ) -> (Result<QueryOutput, SqlError>, usize) {
        let planned = match self.plan_pinned(tenant_hash, snapshot, &req.sql).await {
            Ok(planned) => planned,
            Err(e) => return (Err(e), 0),
        };
        let schema = planned.schema();

        let mut stream = match planned.execute().await {
            Ok(stream) => stream,
            Err(e) => return (Err(e), 0),
        };

        let mut batches = Vec::new();
        let mut emitted = 0usize;
        while let Some(next) = stream.next().await {
            match next {
                Ok(batch) => {
                    emitted += 1;
                    batches.push(batch);
                }
                Err(e) => return (Err(e), emitted),
            }
        }

        (Ok(QueryOutput::new(schema, batches)), emitted)
    }
}

/// A query planned against one pinned snapshot, not yet executing.
///
/// Owns the throwaway `SessionContext` built by
/// [`SqlExecutor::plan_pinned`], so dropping it drops the session, its
/// `RuntimeEnv`, and its memory pool. Exposing the planned schema before
/// execution is what lets Flight SQL's `GetFlightInfo` answer with the result
/// schema without reading a single segment.
pub struct PinnedQuery {
    ctx: SessionContext,
    frame: DataFrame,
    schema: SchemaRef,
    /// The best-effort memory ceiling's abort flag, tripped by the pool's
    /// `grow` and moved into the [`PinnedStream`] on execute (issue #163).
    breach: Arc<CeilingBreach>,
}

impl PinnedQuery {
    /// The planned result schema.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Start the plan's stream. The session stays alive inside the returned
    /// [`PinnedStream`] for as long as the stream does.
    pub async fn execute(self) -> Result<PinnedStream, SqlError> {
        let PinnedQuery {
            ctx,
            frame,
            schema,
            breach,
        } = self;
        let inner = frame.execute_stream().await.map_err(plan_error)?;
        Ok(PinnedStream {
            _ctx: ctx,
            inner,
            schema,
            breach,
        })
    }
}

/// A running plan's `RecordBatch` stream, with its session attached.
///
/// Dropping this mid-stream is the cancellation path: the plan's operators
/// and their `MemoryReservation`s drop with it, each reservation's `Drop`
/// calls `MemoryPool::shrink`, and `TenantDelegatingPool` forwards that to
/// the tenant accountant (crate::memory, review F13). No transport needs an
/// explicit release step, and adding one would double-count.
pub struct PinnedStream {
    _ctx: SessionContext,
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    /// The best-effort memory ceiling's abort flag (issue #163). Checked
    /// before every delegated poll; once the pool's `grow` has tripped it,
    /// the stream fails with [`SqlError::ResourcesExhausted`] instead of
    /// running the over-budget plan to completion.
    breach: Arc<CeilingBreach>,
}

impl PinnedStream {
    /// The stream's schema, identical to the planned schema.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for PinnedStream {
    type Item = Result<RecordBatch, SqlError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // The best-effort memory ceiling's hard-abort seam (issue #163). The
        // pool's `grow` cannot decline a reservation, so a join that overruns
        // a ceiling reserves the bytes and then trips the breach. This is the
        // one place every batch from every transport (HTTP SQL and Flight SQL,
        // crate::flight) passes through, so checking here aborts both.
        //
        // The abort fires on the poll *after* the `grow` that tripped it: the
        // batch already in flight during that `grow` is not retroactively
        // suppressed. That is intentional, not a gap. `grow` runs synchronously
        // inside an operator's own `poll`, deep under `inner.poll_next_unpin`;
        // the earliest seam that sees the tripped flag without reaching into
        // DataFusion's operators is the next poll of this outer stream. Bounding
        // the overshoot to one more in-flight batch is the whole point of the
        // ticket -- it stops the query short of running to completion over
        // budget, which is what happened before (#156).
        if let Some(message) = self.breach.message() {
            return Poll::Ready(Some(Err(SqlError::ResourcesExhausted(message.to_string()))));
        }
        self.inner
            .poll_next_unpin(cx)
            .map(|next| next.map(|batch| batch.map_err(execution_error)))
    }
}

/// What to do after a failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryDecision {
    /// Re-resolve with the same `now_ns` and `min_tokens` and re-execute.
    RetryOnce,
    /// Fail with [`SqlError::SnapshotInvalidated`].
    FailInvalidated,
    /// Not a snapshot problem: return the original error unchanged.
    Propagate,
}

/// The whole retry contract, as a pure function of three facts.
///
/// Isolated from the I/O so every combination is testable, including the
/// post-emission branch. That branch is *not* reachable end-to-end against
/// today's `RsegScanExec`: a partition fetches every one of its segments
/// before it emits its first batch, and `SortPreservingMergeExec` needs one
/// batch from every partition before it emits anything, so a store
/// `NotFound` always surfaces with `emitted == 0`. The branch exists because
/// the consistency model requires the behavior, and it becomes reachable the
/// moment the scan fetches lazily per segment (or Flight SQL streams to a
/// slow consumer, Phase C / review F18). Deleting it as "dead" would silently
/// convert a future lazy scan into one that re-runs a partially-emitted
/// query and duplicates rows.
pub(crate) fn retry_decision(retryable: bool, emitted: usize, attempt: u32) -> RetryDecision {
    if !retryable {
        return RetryDecision::Propagate;
    }
    if emitted > 0 {
        // Partial emission already happened: re-running the plan would
        // duplicate rows. Fail immediately, zero retries.
        return RetryDecision::FailInvalidated;
    }
    if attempt == 0 {
        RetryDecision::RetryOnce
    } else {
        // Second NotFound: the snapshot is genuinely gone.
        RetryDecision::FailInvalidated
    }
}

/// Map a planning-phase `DataFusionError` into a `SqlError`, recovering a
/// ravel error carried across the boundary when there is one.
fn plan_error(err: DataFusionError) -> SqlError {
    match take_sql_error(err) {
        Ok(sql) => sql,
        Err(other) => match other {
            DataFusionError::ResourcesExhausted(msg) => SqlError::ResourcesExhausted(msg),
            other => SqlError::Plan(other.to_string()),
        },
    }
}

/// Same, for the execution phase. Kept distinct so the client-visible
/// message can distinguish "this query cannot be planned" from "this query
/// failed while running", which is the only diagnostic signal that survives
/// redaction.
fn execution_error(err: DataFusionError) -> SqlError {
    match take_sql_error(err) {
        Ok(sql) => sql,
        Err(other) => match other {
            DataFusionError::ResourcesExhausted(msg) => SqlError::ResourcesExhausted(msg),
            other => SqlError::Execution(other.to_string()),
        },
    }
}

/// Recover an owned [`SqlError`] from a `DataFusionError`, unwrapping the
/// wrappers DataFusion adds on the way up: `Context`, `Diagnostic`,
/// `Shared`, `Collection`, and the arrow round trip
/// (`ArrowError::ExternalError`) that operators such as
/// `SortPreservingMergeExec` introduce when an error crosses a
/// `RecordBatchStream`.
///
/// Returns the original error unchanged when there is no ravel error inside,
/// so no detail is lost on the way to the log.
fn take_sql_error(err: DataFusionError) -> Result<SqlError, DataFusionError> {
    match err {
        DataFusionError::External(boxed) => match boxed.downcast::<SqlError>() {
            Ok(sql) => Ok(*sql),
            Err(boxed) => match boxed.downcast::<DataFusionError>() {
                Ok(inner) => take_sql_error(*inner),
                Err(boxed) => Err(DataFusionError::External(boxed)),
            },
        },
        DataFusionError::Context(msg, inner) => match take_sql_error(*inner) {
            Ok(sql) => Ok(sql),
            Err(inner) => Err(DataFusionError::Context(msg, Box::new(inner))),
        },
        DataFusionError::Diagnostic(diag, inner) => match take_sql_error(*inner) {
            Ok(sql) => Ok(sql),
            Err(inner) => Err(DataFusionError::Diagnostic(diag, Box::new(inner))),
        },
        DataFusionError::ArrowError(arrow, backtrace) => match *arrow {
            ArrowError::ExternalError(boxed) => match boxed.downcast::<DataFusionError>() {
                Ok(inner) => take_sql_error(*inner),
                Err(boxed) => match boxed.downcast::<SqlError>() {
                    Ok(sql) => Ok(*sql),
                    Err(boxed) => Err(DataFusionError::ArrowError(
                        Box::new(ArrowError::ExternalError(boxed)),
                        backtrace,
                    )),
                },
            },
            other => Err(DataFusionError::ArrowError(Box::new(other), backtrace)),
        },
        // `Shared` holds an `Arc`, so the inner error cannot be moved out.
        // Classify from the shared value and rebuild an equivalent owned
        // error rather than losing the classification: the only thing lost
        // is the exact original allocation, not the detail.
        DataFusionError::Shared(shared) => match classify_shared(shared.as_ref()) {
            Some(sql) => Ok(sql),
            None => Err(DataFusionError::Shared(shared)),
        },
        DataFusionError::Collection(errors) => {
            let mut remaining = Vec::with_capacity(errors.len());
            for error in errors {
                match take_sql_error(error) {
                    Ok(sql) => return Ok(sql),
                    Err(other) => remaining.push(other),
                }
            }
            Err(DataFusionError::Collection(remaining))
        }
        // A native budget exhaustion is a signal the client must see, so
        // recover it here rather than let a wrapper above collapse it into a
        // generic execution message. Because every wrapper arm recurses
        // through `take_sql_error`, a `ResourcesExhausted` at any depth
        // surfaces as `SqlError::ResourcesExhausted` (with its byte counts
        // intact), whichever `Context`/`Diagnostic`/`Collection` wrapper an
        // operator such as the external sort or hash aggregate carried it in.
        // This mirrors the nested-`SqlError` recovery the other arms already
        // do, and matches the `Shared`-wrapped case classify_shared handles.
        DataFusionError::ResourcesExhausted(msg) => Ok(SqlError::ResourcesExhausted(msg)),
        other => Err(other),
    }
}

/// Best-effort classification of a `DataFusionError` we can only borrow.
/// The retry contract needs the not-found case reconstructed exactly (it
/// drives `retry_decision`); everything else is captured through its own
/// `class()`/`client_message()` rather than collapsed into a generic
/// execution failure, so a budget error that happens to cross a `Shared`
/// boundary keeps its own HTTP class and text (checkpoint review finding).
fn classify_shared(err: &DataFusionError) -> Option<SqlError> {
    match err {
        DataFusionError::External(boxed) => {
            let sql = boxed.downcast_ref::<SqlError>()?;
            Some(if sql.is_segment_not_found() {
                SqlError::Fetch(ravel_query::FetchError::Store {
                    key: String::new(),
                    source: ravel_object_store::StoreError::NotFound,
                })
            } else {
                SqlError::Shared {
                    class: sql.class(),
                    message: sql.client_message(),
                }
            })
        }
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            classify_shared(inner)
        }
        DataFusionError::Shared(inner) => classify_shared(inner),
        // A budget exhaustion behind an `Arc` keeps its own type and message
        // for the client, same as it does through the owned wrappers above.
        DataFusionError::ResourcesExhausted(msg) => Some(SqlError::ResourcesExhausted(msg.clone())),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_object_store::StoreError;
    use ravel_query::FetchError;

    use super::*;

    fn not_found() -> SqlError {
        SqlError::Fetch(FetchError::Store {
            key: "t/hash/metrics/l0/0/w.1.2.abc.rseg".to_string(),
            source: StoreError::NotFound,
        })
    }

    #[test]
    fn a_bare_external_sql_error_is_recovered() {
        let df: DataFusionError = not_found().into();
        let recovered = take_sql_error(df).expect("recovered");
        assert!(recovered.is_segment_not_found());
    }

    /// The scan's error crosses `SortPreservingMergeExec`, which round-trips
    /// it through arrow. If that wrapper were not unwrapped the retry
    /// contract would never fire in a real plan.
    #[test]
    fn an_arrow_wrapped_error_is_recovered() {
        let df: DataFusionError = not_found().into();
        let wrapped =
            DataFusionError::ArrowError(Box::new(ArrowError::ExternalError(Box::new(df))), None);
        let recovered = take_sql_error(wrapped).expect("recovered");
        assert!(recovered.is_segment_not_found());
    }

    #[test]
    fn a_context_wrapped_error_is_recovered() {
        let df: DataFusionError = not_found().into();
        let wrapped = DataFusionError::Context("while scanning".to_string(), Box::new(df));
        let recovered = take_sql_error(wrapped).expect("recovered");
        assert!(recovered.is_segment_not_found());
    }

    #[test]
    fn a_shared_error_keeps_its_retry_classification() {
        let df: DataFusionError = not_found().into();
        let shared = DataFusionError::Shared(Arc::new(df));
        let recovered = take_sql_error(shared).expect("recovered");
        assert!(recovered.is_segment_not_found());
    }

    /// Checkpoint review finding: a budget error crossing a `Shared`
    /// boundary must keep its own class and message, not collapse into a
    /// generic execution failure that loses the count/limit text and
    /// reports the wrong HTTP status.
    #[test]
    fn a_shared_budget_error_keeps_its_own_class_and_message() {
        let inner = SqlError::TooManySamples { count: 20, max: 10 };
        let want_class = inner.class();
        let want_message = inner.client_message();
        let df: DataFusionError = inner.into();
        let shared = DataFusionError::Shared(Arc::new(df));

        let recovered = take_sql_error(shared).expect("recovered");
        assert_eq!(recovered.class(), want_class);
        assert_eq!(recovered.client_message(), want_message);
        assert!(recovered.client_message().contains("20"));
        assert!(recovered.client_message().contains("10"));
    }

    #[test]
    fn a_plain_datafusion_error_is_returned_untouched_and_redacted() {
        let df = DataFusionError::Plan("No field named samples.nope".to_string());
        let err = plan_error(df);
        assert!(matches!(err, SqlError::Plan(_)));
        assert!(
            !err.client_message().contains("samples.nope"),
            "plan detail must not reach the client"
        );
        assert!(
            err.to_string().contains("samples.nope"),
            "plan detail must survive for the log"
        );
    }

    /// Every combination of the retry contract's three inputs, including
    /// the post-emission branch that today's eager scan cannot reach
    /// end-to-end (see [`retry_decision`]'s docs).
    #[test]
    fn the_retry_contract_covers_every_combination() {
        // Not a vanished segment: never a snapshot problem.
        for emitted in [0usize, 1] {
            for attempt in [0u32, 1] {
                assert_eq!(
                    retry_decision(false, emitted, attempt),
                    RetryDecision::Propagate,
                    "emitted={emitted} attempt={attempt}"
                );
            }
        }

        // Vanished segment, nothing emitted, first attempt: retry exactly
        // once.
        assert_eq!(retry_decision(true, 0, 0), RetryDecision::RetryOnce);
        // Vanished segment, nothing emitted, already retried: give up.
        assert_eq!(retry_decision(true, 0, 1), RetryDecision::FailInvalidated);
        // Vanished segment after emission: give up immediately, on either
        // attempt. Never a retry, because the plan already handed rows out.
        assert_eq!(retry_decision(true, 1, 0), RetryDecision::FailInvalidated);
        assert_eq!(retry_decision(true, 7, 1), RetryDecision::FailInvalidated);
    }

    #[test]
    fn target_signal_maps_the_from_clause_to_a_signal() {
        // samples -> metrics; logs -> logs.
        assert_eq!(
            SqlExecutor::target_signal("SELECT ts FROM samples").expect("ok"),
            TargetSignal::Metrics
        );
        assert_eq!(
            SqlExecutor::target_signal("SELECT ts FROM logs").expect("ok"),
            TargetSignal::Logs
        );
        // A tableless constant query defaults to metrics, matching the
        // pre-ADR-0033 behavior (it resolved a metrics snapshot it never read).
        assert_eq!(
            SqlExecutor::target_signal("SELECT 1").expect("ok"),
            TargetSignal::Metrics
        );
        // A string literal naming the other table does not change the signal.
        assert_eq!(
            SqlExecutor::target_signal("SELECT body FROM logs WHERE body = 'samples'").expect("ok"),
            TargetSignal::Logs
        );
    }

    #[test]
    fn target_signal_rejects_a_query_touching_both_tables() {
        let err =
            SqlExecutor::target_signal("SELECT * FROM samples JOIN logs ON samples.ts = logs.ts")
                .expect_err("both tables rejected");
        assert!(matches!(err, SqlError::CrossSignalQuery));
    }

    #[test]
    fn resources_exhausted_keeps_its_own_counts() {
        let df = DataFusionError::ResourcesExhausted("needs 40 bytes, limit 8".to_string());
        let err = execution_error(df);
        assert!(matches!(err, SqlError::ResourcesExhausted(_)));
        assert!(err.client_message().contains("limit 8"));
    }
}
