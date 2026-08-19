//! The per-query SQL execution driver: validate, resolve, plan, execute.
//!
//! Request handling order is fixed and is not an implementation detail:
//!
//! 1. Parse and validate (security invariant 1, crate::validate) -- before
//!    anything else, so a rejected statement costs no catalog LIST and
//!    builds no plan.
//! 2. Resolve the snapshot exactly once, through
//!    `catalog.resolve_pruned(&tenant_hash, signal, window, min_tokens,
//!    now_ns, name_filter)`, with `now_ns` threaded in from the caller's
//!    injected clock (no `SystemTime::now()` in library logic).
//!    `signal` is chosen from the query's own `FROM` clause
//!    ([`SqlExecutor::target_signal`], ADR-0033, extended to `spans` by
//!    ADR-0045 decision 5): `Signal::Logs` for the `logs` table,
//!    `Signal::Spans` for the `spans` table, otherwise `Signal::Metrics`. A
//!    query referencing two of the three tables is rejected here, before the
//!    LIST, because v1 admits one signal per query (decision C). `name_filter` is the equality
//!    `__name__` value derived from the query's pushed-down predicates
//!    ([`SqlExecutor::pushed_down_name_filter`]), so a metrics query
//!    naming one metric prunes by postings exactly as PromQL does; a logs
//!    query or one with no such predicate resolves unpruned, identical to the
//!    former plain `resolve`.
//! 3. Build the fresh single-tenant `SessionContext` around the owned
//!    `Snapshot`, registering the one table the query targets (security
//!    invariant 2, crate::session).
//! 4. Plan, then execute, draining the stream under the wall deadline.
//!
//! # Snapshot retry contract
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
//! reserved bytes (crate::memory); partial state is discarded,
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
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::prelude::SessionContext;
use futures::{Stream, StreamExt};
use ravel_catalog::{Catalog, Snapshot};
use ravel_promql::{LabelMatcher, MatchOp};
use ravel_query::{LogSegmentFetcher, QueryError, SegmentFetcher, admit};
use ravel_types::accounting::{CostEstimate, QueryAccounting, QueryAccountingSnapshot};
use ravel_types::{CommitToken, METRIC_NAME_LABEL, Signal, TenantHash, TimeRange};

use crate::config::SqlConfig;
use crate::cost::{estimate_logs_cost, estimate_metrics_cost, estimate_spans_cost};
use crate::declared::{DeclaredColumn, DeclaredColumnSource, default_declared_source};
use crate::error::SqlError;
use crate::logs_provider::LogsTableProvider;
use crate::memory::{CeilingBreach, TenantMemoryAccountant};
use crate::output::QueryOutput;
use crate::provider::RavelTableProvider;
use crate::pushdown::extract;
use crate::session::{LOGS_TABLE, SAMPLES_TABLE, SPANS_TABLE, SessionTable, build_session};
use crate::spans_fetcher::SpanSegmentFetcher;
use crate::spans_provider::SpansTableProvider;
use crate::udf::{label_match_udf, label_udf};
use crate::validate::{referenced_base_tables, validate};

/// Which of the three v1 tables (and thus which `Signal`) a query targets.
/// A closed enum rather than `Signal` directly: `Signal` carries a `Profiles`
/// variant the SQL surface has no table for, and the executor must never
/// resolve or register that.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum TargetSignal {
    /// The `samples` table, resolved against `Signal::Metrics`.
    Metrics,
    /// The `logs` table, resolved against `Signal::Logs`.
    Logs,
    /// The `spans` table, resolved against `Signal::Spans` (ADR-0045 decision 5).
    Spans,
}

impl TargetSignal {
    fn signal(self) -> Signal {
        match self {
            TargetSignal::Metrics => Signal::Metrics,
            TargetSignal::Logs => Signal::Logs,
            TargetSignal::Spans => Signal::Spans,
        }
    }
}

/// The coordinator-side distributed samples scan for one query: the minted
/// worker slices and the client that fetches each (ADR-0071).
/// Cloneable so the pinned retry loop can re-plan without re-minting (the slice
/// `Vec` and the `Arc` client both clone cheaply).
#[cfg(feature = "flight-sql")]
pub(crate) type DistributedScan = (
    Vec<crate::distributed::WorkerSlice>,
    Arc<dyn crate::distributed::WorkerSliceClient>,
);

/// Optional per-query plan inputs threaded into [`SqlExecutor::plan_pinned_with`]
/// on top of the snapshot and SQL. Kept as a struct (rather than more
/// positional arguments) so the field set can grow, and so it compiles to a
/// zero-field value when `flight-sql` is off -- the local-only build carries no
/// distributed machinery.
#[derive(Default)]
struct PlanExtras {
    /// The tenant's declared typed attribute columns (ADR-0090), resolved once
    /// per plan at the entry point and threaded down here. Empty for a
    /// zero-declaration query, and irrelevant to a metrics- or spans-target
    /// query (only the `logs` provider consumes it).
    declared: Vec<DeclaredColumn>,
    /// The coordinator-side distributed samples scan to install for this query,
    /// or `None` to run the samples scan locally.
    #[cfg(feature = "flight-sql")]
    distributed: Option<DistributedScan>,
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
    /// Blocks the successful attempt's `LogsScanExec` saw, read straight off
    /// its DataFusion counters after the stream drained (reused rather
    /// than recounted). Zero for a metrics query, and for a logs query whose
    /// plan carries no scan node. `blocks_scanned` over `blocks_total` is the
    /// prune selectivity ADR-0049 measures.
    pub blocks_total: u64,
    pub blocks_scanned: u64,
    pub blocks_pruned_by_postings: u64,
}

/// The `LogsScanExec` block counters, summed over a plan tree.
#[derive(Clone, Copy, Default)]
struct BlockCounts {
    total: u64,
    scanned: u64,
    pruned_by_postings: u64,
}

/// Sum the `blocks_total` / `blocks_scanned` / `blocks_pruned_by_postings`
/// DataFusion counters over `plan` and its descendants. Only `LogsScanExec`
/// publishes these names (crate::logs_scan), so the sum is that scan's totals
/// however the optimizer nested it, and a plan with no logs scan contributes
/// nothing. Reads the counters the scan already maintains rather than counting
/// blocks a second time.
fn accumulate_block_counts(plan: &Arc<dyn ExecutionPlan>, counts: &mut BlockCounts) {
    if let Some(metrics) = plan.metrics() {
        let sum = |name: &str| metrics.sum_by_name(name).map_or(0, |v| v.as_usize() as u64);
        counts.total += sum("blocks_total");
        counts.scanned += sum("blocks_scanned");
        counts.pruned_by_postings += sum("blocks_pruned_by_postings");
    }
    for child in plan.children() {
        accumulate_block_counts(child, counts);
    }
}

/// A completed query.
#[derive(Debug, Clone)]
pub struct SqlOutcome {
    pub output: QueryOutput,
    pub stats: SqlStats,
    /// This query's accounting counters (ADR-0044 "1. A per-request
    /// accounting handle"), from the attempt that succeeded. A retried
    /// attempt's discarded counters never bleed into this one: `run` builds
    /// a fresh [`QueryAccounting`] per attempt.
    pub accounting: QueryAccountingSnapshot,
    /// The pre-execution cost estimate (ADR-0044 "3."), from the same
    /// successful attempt's resolve.
    pub estimate: CostEstimate,
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
    /// The RSPAN/spans sibling of `fetcher` (ADR-0045 decision 5). Used only
    /// when a query targets the `spans` table; a metrics- or logs-only query
    /// never touches it. Its `fetch_accounted` path is tenant-checked (fails
    /// closed on a footer tenant_hash mismatch) and records every span GET
    /// against the query's `QueryAccounting`.
    span_fetcher: SpanSegmentFetcher,
    config: SqlConfig,
    max_tenant_bytes: usize,
    /// Per-tenant byte accountants, created on first use and shared across
    /// that tenant's concurrent queries. This map is the one piece of state
    /// that intentionally outlives a query, and it holds no query, plan, or
    /// catalog data -- only a byte counter per tenant, so it cannot carry
    /// data across the tenant boundary.
    ///
    /// Each entry also carries the tenant's last-touch (`now_ns` of its most
    /// recent query resolve), so the server's idle-tenant sweep can evict the
    /// accountants of tenants idle past a threshold (ADR-0069 decision 2). Eviction is re-derivable: a `TenantMemoryAccountant` is pure
    /// process-local byte-counter state, rebuilt on the tenant's next query, so
    /// dropping an idle one with no outstanding reservations changes no result.
    tenants: Mutex<HashMap<TenantHash, TenantAccountantEntry>>,
    /// The source of each tenant's declared typed attribute columns for the
    /// `logs` table (ADR-0090 decision 2). Resolved once per plan at the entry
    /// point (`run` for HTTP, Flight's `get_flight_info`) and threaded down as a
    /// plain parameter; the planning functions never call this themselves.
    /// [`SqlExecutor::new`] defaults it to an empty
    /// [`crate::StaticDeclaredColumns`] so the constructor and every existing
    /// call site stay source-compatible; a caller installs the real cache-aside
    /// overlay (#302) with [`Self::with_declared_column_source`].
    declared_source: Arc<dyn DeclaredColumnSource>,
}

/// One tenant's memory accountant plus the last-touch stamp idle-tenant
/// eviction reads (ADR-0069 decision 2). `last_touch_ns` is the injected
/// `now_ns` of the tenant's most recent resolve; no clock is read here.
struct TenantAccountantEntry {
    accountant: Arc<TenantMemoryAccountant>,
    last_touch_ns: i64,
}

impl SqlExecutor {
    /// Build an executor. `max_tenant_bytes` is the ceiling each tenant's
    /// accountant enforces across that tenant's concurrent queries; the
    /// per-query ceiling comes from `config.max_query_bytes`.
    pub fn new(
        catalog: Arc<Catalog>,
        fetcher: SegmentFetcher,
        log_fetcher: LogSegmentFetcher,
        span_fetcher: SpanSegmentFetcher,
        config: SqlConfig,
        max_tenant_bytes: usize,
    ) -> Self {
        SqlExecutor {
            catalog,
            fetcher,
            log_fetcher,
            span_fetcher,
            config,
            max_tenant_bytes,
            tenants: Mutex::new(HashMap::new()),
            declared_source: default_declared_source(),
        }
    }

    /// Install the source of per-tenant declared typed attribute columns for the
    /// `logs` table (ADR-0090 decision 2), replacing the empty default this
    /// executor was built with. This is the seam the server's real cache-aside,
    /// `TenantConfig`-backed overlay (#302) attaches through, without changing
    /// [`Self::new`]'s signature or any existing call site.
    pub fn with_declared_column_source(mut self, source: Arc<dyn DeclaredColumnSource>) -> Self {
        self.declared_source = source;
        self
    }

    /// Resolve the declared typed attribute columns for `tenant` as of
    /// `now_ns` (ADR-0090 decision 2). This is the one call into the injected
    /// [`DeclaredColumnSource`]; it happens exactly once per plan, at the entry
    /// point that carries both the tenant and the query's injected clock
    /// (`SqlExecutor::run` for HTTP, Flight's `get_flight_info`), and the
    /// resolved list is threaded down as a plain parameter thereafter.
    pub async fn resolve_declared_columns(
        &self,
        tenant: TenantHash,
        now_ns: i64,
    ) -> Vec<DeclaredColumn> {
        self.declared_source.declared_columns(tenant, now_ns).await
    }

    pub fn config(&self) -> &SqlConfig {
        &self.config
    }

    /// The per-tenant memory ceiling each tenant's accountant enforces across
    /// that tenant's concurrent queries (the `max_tenant_bytes` passed to
    /// [`SqlExecutor::new`]). Distinct from the per-query ceiling in
    /// `config().max_query_bytes`. Exposed so a caller wiring the server's
    /// `--sql-tenant-max-bytes` flag can assert the configured ceiling actually
    /// reached the executor rather than a compiled-in default.
    pub fn max_tenant_bytes(&self) -> usize {
        self.max_tenant_bytes
    }

    /// Lock the tenant map, recovering a poisoned guard. A poisoned lock means
    /// another thread panicked while holding it; the map is a plain HashMap of
    /// Arc counters with no torn-state hazard, so recovering is safe and
    /// strictly better than failing every subsequent query for the process's
    /// life.
    fn lock_tenants(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<TenantHash, TenantAccountantEntry>> {
        match self.tenants.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// The accountant for `tenant`, creating it on first use.
    ///
    /// Exposed so the endpoint and the tenancy tests can read a tenant's
    /// reserved bytes without running a query through it. A first-use creation
    /// here stamps the entry's last-touch at [`i64::MIN`] so an accountant
    /// created only to read a budget (never touched by a resolve) is a
    /// candidate for the idle sweep as soon as it holds no reservations; a
    /// query path always stamps a real `now_ns` first via [`Self::touch_tenant`].
    pub fn tenant_budget(&self, tenant: TenantHash) -> Arc<TenantMemoryAccountant> {
        let mut tenants = self.lock_tenants();
        Arc::clone(
            &tenants
                .entry(tenant)
                .or_insert_with(|| TenantAccountantEntry {
                    accountant: TenantMemoryAccountant::new(self.max_tenant_bytes),
                    last_touch_ns: i64::MIN,
                })
                .accountant,
        )
    }

    /// Stamp `tenant`'s last-touch at `now_ns`, creating its accountant on
    /// first use (ADR-0069 decision 2). Called from [`Self::resolve`], the one
    /// funnel every query's snapshot resolve passes through (HTTP SQL and
    /// Flight SQL alike), so a tenant running any query is never a candidate
    /// for the idle sweep. `now_ns` is the request's injected clock reading;
    /// this reads no clock itself.
    fn touch_tenant(&self, tenant: TenantHash, now_ns: i64) {
        let mut tenants = self.lock_tenants();
        let entry = tenants
            .entry(tenant)
            .or_insert_with(|| TenantAccountantEntry {
                accountant: TenantMemoryAccountant::new(self.max_tenant_bytes),
                last_touch_ns: now_ns,
            });
        entry.last_touch_ns = now_ns;
    }

    /// Evict the memory accountant of every tenant last touched before
    /// `now_ns - ttl_ns` that also holds zero outstanding reservations
    /// (ADR-0069 decision 2). Returns the number evicted.
    ///
    /// The zero-reservation guard is load-bearing: an accountant with live
    /// reservations is backing an in-flight query's memory budget, and dropping
    /// the map entry would let a concurrent query for the same tenant build a
    /// second accountant, so the tenant's ceiling would stop being shared
    /// across its concurrent queries. A tenant reserves bytes only for the
    /// duration of a query, so a zero reservation means no query is currently
    /// accounting against it, and a re-created accountant on the next query is
    /// byte-for-byte equivalent (pure process-local counter state). Both
    /// `now_ns` and `ttl_ns` are caller-supplied (the sweep loop); this reads
    /// no clock.
    pub fn evict_idle_accountants(&self, now_ns: i64, ttl_ns: i64) -> usize {
        let mut tenants = self.lock_tenants();
        let before = tenants.len();
        tenants.retain(|_, entry| {
            entry.accountant.reserved() > 0 || now_ns.saturating_sub(entry.last_touch_ns) <= ttl_ns
        });
        before - tenants.len()
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

        // Resolve the tenant's declared typed attribute columns once for the
        // whole request (ADR-0090 decision 2), before the retry loop, so the
        // same declared schema is used across the original attempt and the one
        // retry the consistency model allows -- never re-resolved per attempt
        // with a source that might have refreshed in between. `req.now_ns` is
        // the request's injected clock reading, the same one that bounds the
        // resolve window, so the declared schema and the snapshot are pinned to
        // one instant together.
        let declared = self.resolve_declared_columns(tenant_hash, req.now_ns).await;

        // At most two passes: the original and the one retry the
        // consistency model allows. Each pass gets its own QueryAccounting
        // (ADR-0044): a discarded first attempt's counts must never bleed
        // into the retry's.
        for attempt in 0..2u32 {
            let accounting = QueryAccounting::new();
            let (snapshot, estimate) = self.resolve(tenant_hash, req, &accounting).await?;
            stats.resolves += 1;
            stats.attempts += 1;
            stats.segments = snapshot.segments.len();

            let (result, emitted, blocks) = self
                .attempt(tenant_hash, req, snapshot, &accounting, &declared)
                .await;
            stats.batches_emitted += emitted;

            match result {
                Ok(output) => {
                    stats.blocks_total = blocks.total;
                    stats.blocks_scanned = blocks.scanned;
                    stats.blocks_pruned_by_postings = blocks.pruned_by_postings;
                    return Ok(SqlOutcome {
                        output,
                        stats,
                        accounting: accounting.snapshot(),
                        estimate,
                    });
                }
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
    /// into its ticket, and executes against that pin at `DoGet`.
    /// It must reach `Catalog::resolve` through this call rather than its own,
    /// so both transports share one signature, one budget check, and one
    /// injected-clock discipline. Validation is *not* performed here: the
    /// caller runs [`crate::validate`] first, exactly as [`Self::execute`]
    /// does, so a rejected statement still costs no catalog LIST.
    ///
    /// `accounting` receives this resolve's store counters; the returned
    /// [`CostEstimate`] is the two-part estimate for the query this snapshot
    /// will be planned against.
    pub async fn resolve_snapshot(
        &self,
        tenant_hash: TenantHash,
        req: &SqlRequest,
        accounting: &QueryAccounting,
    ) -> Result<(Snapshot, CostEstimate), SqlError> {
        self.resolve(tenant_hash, req, accounting).await
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
    /// rather than merely resemble it: the pool the returned
    /// query owns is dropped with it, and every `MemoryReservation` the plan
    /// took shrinks back through it into the tenant accountant.
    ///
    /// `accounting` is this query's [`QueryAccounting`] handle: it is cloned
    /// into the query's memory pool (peak intermediate bytes) and into
    /// whichever table provider the query targets (every store fetch the
    /// scan issues).
    pub async fn plan_pinned(
        &self,
        tenant_hash: TenantHash,
        snapshot: Snapshot,
        sql: &str,
        accounting: &QueryAccounting,
        declared: &[DeclaredColumn],
    ) -> Result<PinnedQuery, SqlError> {
        self.plan_pinned_with(
            tenant_hash,
            snapshot,
            sql,
            accounting,
            PlanExtras {
                declared: declared.to_vec(),
                // Explicit per-field so this compiles clean whether or not the
                // `flight-sql` feature adds `distributed`; a `..default()` would
                // be a needless update on the single-field local build.
                #[cfg(feature = "flight-sql")]
                distributed: None,
            },
        )
        .await
    }

    /// [`Self::plan_pinned`] with a coordinator-side distributed scan installed
    /// on the metrics provider for THIS query only (ADR-0071).
    ///
    /// `distributed`, when `Some`, carries the minted worker slices and the
    /// production [`WorkerSliceClient`](crate::distributed::WorkerSliceClient)
    /// the provider fans the samples scan out over. `None` is byte-identical to
    /// [`Self::plan_pinned`]. The distribution decision itself is made by the
    /// caller ([`Self::plan_distributed_slices_for`]); this method only installs
    /// an already-made decision, so the local and distributed plans share this
    /// one construction path and cannot drift.
    #[cfg(feature = "flight-sql")]
    pub async fn plan_pinned_distributed(
        &self,
        tenant_hash: TenantHash,
        snapshot: Snapshot,
        sql: &str,
        accounting: &QueryAccounting,
        distributed: Option<DistributedScan>,
        declared: &[DeclaredColumn],
    ) -> Result<PinnedQuery, SqlError> {
        self.plan_pinned_with(
            tenant_hash,
            snapshot,
            sql,
            accounting,
            PlanExtras {
                declared: declared.to_vec(),
                distributed,
            },
        )
        .await
    }

    /// The one body behind [`Self::plan_pinned`] and
    /// [`Self::plan_pinned_distributed`]. `extras` carries the optional
    /// coordinator-side distributed scan; with the `flight-sql` feature off it
    /// is a zero-field struct and the metrics provider is always the local one.
    async fn plan_pinned_with(
        &self,
        tenant_hash: TenantHash,
        snapshot: Snapshot,
        sql: &str,
        accounting: &QueryAccounting,
        extras: PlanExtras,
    ) -> Result<PinnedQuery, SqlError> {
        let (pool, breach) = self
            .config
            .query_pool(self.tenant_budget(tenant_hash), accounting.clone());
        // Build the one table the query targets over the snapshot resolved for
        // its signal. `resolve` already resolved `snapshot` against exactly
        // this signal, so the provider and the snapshot always agree.
        let table = match Self::target_signal(sql)? {
            TargetSignal::Metrics => {
                #[cfg_attr(not(feature = "flight-sql"), allow(unused_mut))]
                let mut provider = RavelTableProvider::new(
                    snapshot,
                    tenant_hash,
                    self.fetcher.clone(),
                    self.config,
                    accounting.clone(),
                );
                // Install the distributed samples scan for this query only, when
                // the coordinator decided to fan out. `None`/feature-off leaves
                // the provider byte-identical to the local path.
                #[cfg(feature = "flight-sql")]
                if let Some((endpoints, client)) = extras.distributed {
                    provider = provider.with_distributed_scan(endpoints, client);
                }
                SessionTable::Metrics(Arc::new(provider))
            }
            // The declared typed attribute columns (ADR-0090) resolved once per
            // plan are installed on the logs provider here; they widen the
            // table's schema and are consumed only by this provider. A metrics-
            // or spans-target query leaves `extras.declared` unused.
            TargetSignal::Logs => SessionTable::Logs(Arc::new(
                LogsTableProvider::new(
                    snapshot,
                    tenant_hash,
                    self.log_fetcher.clone(),
                    accounting.clone(),
                )
                .with_declared_columns(extras.declared),
            )),
            // The spans provider drives `SpanSegmentFetcher::fetch_accounted`
            // for every scanned segment: `accounting` is cloned in so each
            // span GET is recorded against this query, and the fetch is
            // tenant-checked (fails closed on a footer tenant_hash mismatch).
            TargetSignal::Spans => SessionTable::Spans(Arc::new(SpansTableProvider::new(
                snapshot,
                tenant_hash,
                self.span_fetcher.clone(),
                accounting.clone(),
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

    /// Decide whether THIS pinned statement should distribute its samples scan,
    /// and if so mint the per-worker slice tickets (ADR-0071).
    ///
    /// Returns `Some(slices)` only for a metrics-target query whose pinned
    /// snapshot clears the cost gate, advertises more than one worker slice, and
    /// has workers to serve them (see
    /// [`plan_distributed_slices`](crate::distributed::plan_distributed_slices)).
    /// Logs have no distributed path, so a logs-target query is always `None`.
    ///
    /// The gate re-derives the cost estimate from the ticket-pinned snapshot
    /// with `catalog_requests = 0`. [`should_distribute`] reads only the
    /// snapshot-derived store-bytes and segment terms of the estimate, never the
    /// catalog term, so this recomputed estimate makes the identical
    /// distribute/local decision `get_flight_info` made when it minted the
    /// ticket -- the coordinator does not re-run `Catalog::resolve` at `DoGet`.
    #[cfg(feature = "flight-sql")]
    pub fn plan_distributed_slices_for(
        &self,
        snapshot: &Snapshot,
        sql: &str,
        config: &crate::distributed::DistributedFlightConfig,
        template: &crate::flight_ticket::FlightTicket,
    ) -> Option<Vec<crate::distributed::WorkerSlice>> {
        if !matches!(Self::target_signal(sql).ok()?, TargetSignal::Metrics) {
            return None;
        }
        let estimate = estimate_metrics_cost(snapshot, 0);
        crate::distributed::plan_distributed_slices(snapshot, &estimate, config, template)
    }

    /// Execute the worker-side scan fragment for one distributed slice
    /// (ADR-0071), returning its internal-schema, `(series_id, ts)`-sorted
    /// stream.
    ///
    /// This is the worker half of the SQL distributed lane: a coordinator's
    /// `do_get` receives a slice ticket (`slice_count > 1`) and serves this
    /// fragment over the pinned slice rather than planning the statement. There
    /// is deliberately NO SQL text, no aggregation, and no dedup here
    /// ([`RavelTableProvider::worker_fragment`]): the provenance columns are
    /// retained and the authoritative cross-slice dedup stays at the coordinator
    /// (crate::distributed). The result reuses [`PinnedStream`] so the same
    /// memory ceiling and drop-cancellation apply as on the local path.
    ///
    /// `target_partitions` follows the segment count so the fragment's internal
    /// merge fans out over the slice's segments exactly as the local scan does;
    /// the coordinator re-merges and deduplicates above the union of slices.
    #[cfg(feature = "flight-sql")]
    pub async fn worker_fragment_stream(
        &self,
        tenant_hash: TenantHash,
        snapshot: Snapshot,
        accounting: &QueryAccounting,
    ) -> Result<PinnedStream, SqlError> {
        let (pool, breach) = self
            .config
            .query_pool(self.tenant_budget(tenant_hash), accounting.clone());
        let segments = snapshot.segments.clone();
        let target_partitions = segments.len().max(1);
        let provider = Arc::new(RavelTableProvider::new(
            snapshot,
            tenant_hash,
            self.fetcher.clone(),
            self.config,
            accounting.clone(),
        ));
        let plan = provider
            .worker_fragment(target_partitions, &segments)
            .map_err(plan_error)?;
        let ctx = build_session(
            &self.config,
            pool,
            SessionTable::Metrics(Arc::clone(&provider)),
        )
        .map_err(plan_error)?;
        let schema = plan.schema();
        let inner = execute_stream(Arc::clone(&plan), ctx.task_ctx()).map_err(plan_error)?;
        Ok(PinnedStream {
            _ctx: ctx,
            inner,
            schema,
            breach,
            plan,
        })
    }

    /// One `Catalog::resolve` plus the `max_segments` budget check. Resolves
    /// the signal the query's `FROM` clause targets ([`Self::target_signal`]),
    /// so a metrics-only query never lists the logs keyspace and vice versa.
    ///
    /// Also computes the two-part cost estimate (ADR-0044 "3.", amended): the
    /// catalog term from `shard_count` and the window's hour-bucket count,
    /// computed here *before* `resolve_pruned_with_accounting` runs (resolve
    /// itself is not free, and an estimate computed only after resolve
    /// structurally cannot bound resolve's own spend), and the segment term
    /// from the pinned snapshot's `SegmentRef`s, computed after.
    async fn resolve(
        &self,
        tenant_hash: TenantHash,
        req: &SqlRequest,
        accounting: &QueryAccounting,
    ) -> Result<(Snapshot, CostEstimate), SqlError> {
        // Idle-tenant eviction last-touch (ADR-0069 decision 2): stamp this
        // tenant's activity with the request's injected clock before resolving.
        // This is the one funnel both the HTTP (`execute`/`run`) and Flight SQL
        // (`resolve_snapshot`) paths pass through, so any tenant running a query
        // is kept out of the idle sweep.
        self.touch_tenant(tenant_hash, req.now_ns);
        let target = Self::target_signal(&req.sql)?;
        // Postings pruning by the equality `__name__` predicate pushed down
        // from the query's WHERE clause. Without this the SQL
        // path called plain `Catalog::resolve`, so the measured 5.9-40.9x
        // postings pruning was structurally unreachable from SQL even for a
        // query whose `WHERE label(labels,'__name__') = '...'` names one
        // metric. Only a metrics query has a `__name__` postings index; a
        // logs query never prunes by it. Derivation is best-effort: any
        // planning hiccup yields no filter and the resolve simply does not
        // prune, exactly as before, and pruning itself already degrades
        // safely when postings are absent or unusable.
        let name_filter = match target {
            TargetSignal::Metrics => self.pushed_down_name_filter(tenant_hash, &req.sql).await,
            // Only a metrics query has a `__name__` postings index; neither a
            // logs nor a spans query prunes by it.
            TargetSignal::Logs | TargetSignal::Spans => None,
        };
        let catalog_requests = self
            .catalog
            .estimated_catalog_requests(req.window, req.now_ns);
        let (snapshot, origins) = self
            .catalog
            .resolve_pruned_with_admission(
                &tenant_hash,
                target.signal(),
                req.window,
                &req.min_tokens,
                req.now_ns,
                name_filter.as_deref(),
                accounting,
            )
            .await?;
        // Sealed, below-watermark segments count against `max_segments`;
        // recent and token-resolved segments are exempt (ADR-0073 decision
        // 2), the same seam `ravel_query::engine::resolve_bounded` uses for
        // PromQL. Their cost is bounded separately by the
        // request budget checked incrementally during fetch (see scan.rs).
        admit(&snapshot, &origins, &self.config.engine).map_err(admission_error_to_sql)?;
        let estimate = match target {
            TargetSignal::Metrics => estimate_metrics_cost(&snapshot, catalog_requests),
            TargetSignal::Logs => estimate_logs_cost(&snapshot, catalog_requests),
            TargetSignal::Spans => estimate_spans_cost(&snapshot, catalog_requests),
        };
        Ok((snapshot, estimate))
    }

    /// The equality `__name__` value a metrics query's pushed-down predicates
    /// pin, or `None` if none can be soundly used for postings pruning.
    ///
    /// This plans `sql` to a logical plan over a schema-only, empty-snapshot
    /// `samples` table (no storage I/O, no execution) purely to recover its
    /// WHERE predicates, then runs them through the same widen-only
    /// [`crate::pushdown`] extractor the scan uses and keeps a lone equality
    /// `__name__` matcher. The extractor already contributes nothing for an
    /// `OR`, a regex, a negation, or a `__name__` matcher that is not a lone
    /// `=`, so the returned name is always a predicate the query genuinely
    /// requires at the top level: pruning to it drops only segments whose
    /// rows the residual filter would drop anyway. Any planning error yields
    /// `None` (no prune); the real plan surfaces such errors later through
    /// [`Self::plan_pinned`].
    async fn pushed_down_name_filter(&self, tenant_hash: TenantHash, sql: &str) -> Option<String> {
        let ctx = SessionContext::new();
        ctx.register_udf(label_udf());
        ctx.register_udf(label_match_udf());
        let provider = RavelTableProvider::new(
            Snapshot {
                segments: Vec::new(),
                segments_pruned: 0,
                pending_erasure: Vec::new(),
            },
            tenant_hash,
            self.fetcher.clone(),
            self.config,
            // Throwaway handle: this plans over an empty snapshot purely to
            // recover predicates, so no store fetch is ever issued through
            // it and nothing would ever be recorded here.
            QueryAccounting::new(),
        );
        ctx.register_table(SAMPLES_TABLE, Arc::new(provider)).ok()?;
        let plan = ctx.state().create_logical_plan(sql).await.ok()?;
        let mut predicates = Vec::new();
        collect_filter_predicates(&plan, &mut predicates);
        equality_name_filter(&extract(&predicates).matchers)
    }

    /// The table (and thus the signal) a query resolves against, decided from
    /// its `FROM` clause before any planning (ADR-0033 "one SQL endpoint, two
    /// tables").
    ///
    /// The referenced table names come from the same `DFParser` front end the
    /// validation gate uses ([`referenced_base_tables`]), never a raw-text
    /// scan. The mapping (ADR-0045 decision 5 extends the ADR-0033 two-table
    /// rule to a third arm):
    ///
    /// - references `logs` only -> [`TargetSignal::Logs`].
    /// - references `spans` only -> [`TargetSignal::Spans`].
    /// - references `samples` only, or references no real table ->
    ///   [`TargetSignal::Metrics`].
    /// - references two or more of {`samples`, `logs`, `spans`} ->
    ///   [`SqlError::CrossSignalQuery`], rejected before the catalog LIST
    ///   (decision C: v1 admits one signal per query).
    ///
    /// The "no real table" case (a constant query such as `SELECT 1`, or one
    /// whose only source is a CTE with no base table) defaults to `Metrics`: it
    /// preserves the pre-ADR-0033 behavior exactly -- such a query resolved a
    /// metrics snapshot and never touched it -- and `crate::validate` already
    /// rules out anything that would need a data source it cannot reach. Only
    /// the multiple-table case is genuinely unsupported, so only it is an error.
    fn target_signal(sql: &str) -> Result<TargetSignal, SqlError> {
        let tables = referenced_base_tables(sql)?;
        let has_samples = tables.contains(SAMPLES_TABLE);
        let has_logs = tables.contains(LOGS_TABLE);
        let has_spans = tables.contains(SPANS_TABLE);
        // Naming two of the three real tables crosses signals: v1 resolves one
        // snapshot per query, so this is rejected before any catalog listing.
        let named = u8::from(has_samples) + u8::from(has_logs) + u8::from(has_spans);
        if named > 1 {
            return Err(SqlError::CrossSignalQuery);
        }
        if has_logs {
            Ok(TargetSignal::Logs)
        } else if has_spans {
            Ok(TargetSignal::Spans)
        } else {
            // `samples` only, or no real table at all: metrics by default.
            Ok(TargetSignal::Metrics)
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
        accounting: &QueryAccounting,
        declared: &[DeclaredColumn],
    ) -> (Result<QueryOutput, SqlError>, usize, BlockCounts) {
        let planned = match self
            .plan_pinned(tenant_hash, snapshot, &req.sql, accounting, declared)
            .await
        {
            Ok(planned) => planned,
            Err(e) => return (Err(e), 0, BlockCounts::default()),
        };
        let schema = planned.schema();

        let mut stream = match planned.execute().await {
            Ok(stream) => stream,
            Err(e) => return (Err(e), 0, BlockCounts::default()),
        };

        let mut batches = Vec::new();
        let mut emitted = 0usize;
        while let Some(next) = stream.next().await {
            match next {
                Ok(batch) => {
                    emitted += 1;
                    batches.push(batch);
                }
                // The scan's block counters are final only after a clean drain,
                // so a mid-stream error reports none: a partial prune ratio
                // would misattribute.
                Err(e) => return (Err(e), emitted, BlockCounts::default()),
            }
        }

        // The stream drained cleanly: the plan's LogsScanExec counters are now
        // final, so read them off the plan we kept.
        let blocks = stream.block_counts();
        (Ok(QueryOutput::new(schema, batches)), emitted, blocks)
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
    /// `grow` and moved into the [`PinnedStream`] on execute.
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
        // Build the physical plan explicitly rather than through
        // `frame.execute_stream()` (which does the same two steps internally)
        // so the plan handle survives the stream and its `LogsScanExec`
        // DataFusion counters can be read after the drain. The two
        // are equivalent: `execute_stream` is `create_physical_plan` then
        // `execute_stream(plan, task_ctx)`.
        let plan = frame.create_physical_plan().await.map_err(plan_error)?;
        let inner = execute_stream(Arc::clone(&plan), ctx.task_ctx()).map_err(plan_error)?;
        Ok(PinnedStream {
            _ctx: ctx,
            inner,
            schema,
            breach,
            plan,
        })
    }
}

/// A running plan's `RecordBatch` stream, with its session attached.
///
/// Dropping this mid-stream is the cancellation path: the plan's operators
/// and their `MemoryReservation`s drop with it, each reservation's `Drop`
/// calls `MemoryPool::shrink`, and `TenantDelegatingPool` forwards that to
/// the tenant accountant (crate::memory). No transport needs an
/// explicit release step, and adding one would double-count.
pub struct PinnedStream {
    _ctx: SessionContext,
    inner: SendableRecordBatchStream,
    schema: SchemaRef,
    /// The best-effort memory ceiling's abort flag. Checked
    /// before every delegated poll; once the pool's `grow` has tripped it,
    /// the stream fails with [`SqlError::ResourcesExhausted`] instead of
    /// running the over-budget plan to completion.
    breach: Arc<CeilingBreach>,
    /// The physical plan behind `inner`, kept so its `LogsScanExec` block
    /// counters can be read once the stream has drained. Holding
    /// it changes nothing about execution: the operators live in `inner`, and
    /// this is the same `Arc` handle.
    plan: Arc<dyn ExecutionPlan>,
}

impl PinnedStream {
    /// The stream's schema, identical to the planned schema.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// The `(blocks_total, blocks_scanned, blocks_pruned_by_postings)` the
    /// plan's logs scan recorded. Meaningful only once the stream has drained;
    /// zero on a plan with no logs scan. Reads the existing DataFusion counters
    ///.
    fn block_counts(&self) -> BlockCounts {
        let mut counts = BlockCounts::default();
        accumulate_block_counts(&self.plan, &mut counts);
        counts
    }
}

impl Stream for PinnedStream {
    type Item = Result<RecordBatch, SqlError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // The best-effort memory ceiling's hard-abort seam. The
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
        // budget, which is what happened before.
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
/// slow consumer). Deleting it as "dead" would silently
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

/// Maps [`admit`]'s error into a [`SqlError`]. `admit`
/// has exactly one failure variant
/// (`QueryError::TooManySegments`); the wildcard arm exists only because
/// `QueryError` is not restricted to that variant at the type level, and is
/// unreachable in practice.
fn admission_error_to_sql(err: QueryError) -> SqlError {
    match err {
        QueryError::TooManySegments { count, max } => SqlError::TooManySegments { count, max },
        other => SqlError::Internal(format!("unexpected admission error: {other}")),
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

/// Collect every `WHERE`/`HAVING` predicate in `plan` as a top-level AND
/// conjunct for [`crate::pushdown::extract`]. Recurses through
/// the plan's inputs so a predicate under a projection or aggregate is still
/// seen; the extractor treats each collected expression as an implicit
/// top-level AND conjunct and splits nested `AND`s itself.
fn collect_filter_predicates(plan: &LogicalPlan, out: &mut Vec<Expr>) {
    if let LogicalPlan::Filter(filter) = plan {
        out.push(filter.predicate.clone());
    }
    for input in plan.inputs() {
        collect_filter_predicates(input, out);
    }
}

/// Leading sentinel marking a `name_filter` as a literal-prefix range key
/// rather than an exact `__name__` value (ADR-0061 decision 3).
///
/// This MUST equal `ravel_catalog`'s
/// `snapshot_resolve::PREFIX_FILTER_SENTINEL`, the byte the catalog strips to
/// decide the prefix-vs-exact postings lookup. The value is duplicated inline
/// here (matching this codebase's language-specific-enforcement precedent for
/// name filters, which already duplicates `equality_name_filter` across
/// ravel-query and ravel-sql) rather than shared across the crate boundary; the
/// catalog pins the value with a test and the postings-pruning oracles round-
/// trip it end to end, so a silent drift cannot pass.
const PREFIX_FILTER_SENTINEL: char = '\u{1}';

/// The literal prefix of a fully-anchored `__name__` regex of the exact shape
/// `^literal.*$`, or `None` for every other shape (ADR-0061 decision 3).
///
/// SQL's `label_match(labels, '__name__', 'pattern')` UDF lowers to the same
/// fully-anchored `ravel_promql` regex matcher PromQL selectors use (the raw
/// pattern in `LabelMatcher.value`, evaluated as `^(?:value)$`), so this is the
/// byte-for-byte twin of the PromQL engine's own detector. It accepts ONLY the
/// prefix shape and rejects everything else so a misclassification can never
/// prune a segment the query could match:
///
/// - one optional explicit leading `^` and trailing `$` are tolerated;
/// - the remainder MUST end with an unanchored `.*` wildcard tail;
/// - the literal before that tail MUST be non-empty and consist solely of
///   plain metric-name bytes (`[A-Za-z0-9_:]`).
///
/// Infix wildcards, alternations, character classes, non-`.*` tails, and ANY
/// backslash escape are rejected; the caller falls back to the pre-existing
/// unpruned resolve.
fn literal_prefix_from_anchored_regex(pattern: &str) -> Option<String> {
    let mut p = pattern;
    p = p.strip_prefix('^').unwrap_or(p);
    p = p.strip_suffix('$').unwrap_or(p);
    let literal = p.strip_suffix(".*")?;
    if literal.is_empty() {
        return None;
    }
    if literal.bytes().all(is_literal_prefix_byte) {
        Some(literal.to_string())
    } else {
        None
    }
}

/// A byte that is unambiguously a literal in a Prometheus-anchored regex and a
/// valid metric-name character. Conservative on purpose: any byte outside this
/// set (including every regex metacharacter and every escape) forces a bypass.
fn is_literal_prefix_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_' || b == b':'
}

/// The postings pruning key a single `__name__` matcher yields, or `None` if
/// it cannot be soundly used to prune: an exact value verbatim, a prefix-
/// anchored regex (`^foo.*$`) as its sentinel-encoded literal prefix, and a
/// negation / non-prefix regex as `None`.
fn name_pruning_key(m: &LabelMatcher) -> Option<String> {
    match &m.op {
        MatchOp::Eq => Some(m.value.clone()),
        MatchOp::Re(_) => literal_prefix_from_anchored_regex(&m.value)
            .map(|prefix| format!("{PREFIX_FILTER_SENTINEL}{prefix}")),
        MatchOp::Ne | MatchOp::Nre(_) => None,
    }
}

/// The lone `__name__` pruning key in `matchers`, or `None` if none can be
/// soundly used to prune (extended by ADR-0061 decision 3).
/// Mirrors the PromQL engine's `equality_name_filter`: a single `__name__`
/// matcher that is either an exact `=` or a literal-prefix-anchored regex
/// yields its (possibly sentinel-encoded) key; a second `__name__` matcher of
/// any kind, a negation, or a non-prefix regex takes the conservative bypass
/// so pruning never drops a segment the query could still match.
fn equality_name_filter(matchers: &[LabelMatcher]) -> Option<String> {
    let mut found: Option<String> = None;
    for m in matchers {
        if m.name != METRIC_NAME_LABEL {
            continue;
        }
        // A second `__name__` matcher of any kind: the pruning key is no longer
        // well defined, so bypass (unchanged from the equality-only behaviour).
        if found.is_some() {
            return None;
        }
        found = Some(name_pruning_key(m)?);
    }
    found
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod name_filter_tests {
    use super::*;

    fn re(pattern: &str) -> LabelMatcher {
        LabelMatcher::regex(METRIC_NAME_LABEL, pattern).expect("compilable regex")
    }

    fn encoded(prefix: &str) -> String {
        format!("{PREFIX_FILTER_SENTINEL}{prefix}")
    }

    /// The SQL detector is the byte-for-byte twin of the PromQL engine's, so it
    /// accepts exactly the same literal-prefix shapes and rejects everything
    /// else (including escapes, which it declines rather than mis-parses).
    #[test]
    fn detector_accepts_and_rejects_the_same_shapes_as_promql() {
        for (pattern, prefix) in [
            ("foo.*", "foo"),
            ("^foo.*$", "foo"),
            ("^foo.*", "foo"),
            ("foo.*$", "foo"),
            ("a1_b:c.*", "a1_b:c"),
        ] {
            assert_eq!(
                literal_prefix_from_anchored_regex(pattern).as_deref(),
                Some(prefix),
                "{pattern:?} must yield {prefix:?}"
            );
        }
        for pattern in [
            "foo",
            "^foo$",
            ".*",
            "^.*$",
            ".*foo.*",
            "foo.*bar.*",
            "foo.*bar",
            "fo.o.*",
            "foo*",
            "foo.+",
            "foo|bar",
            "(foo).*",
            "[a-z].*",
            r"a\.b.*",
            r"^a\.b.*$",
            "^^foo.*$",
        ] {
            assert_eq!(
                literal_prefix_from_anchored_regex(pattern),
                None,
                "{pattern:?} must be rejected (unpruned bypass)"
            );
        }
    }

    #[test]
    fn equality_name_filter_routes_prefix_and_preserves_bypass() {
        // Exact case unchanged.
        assert_eq!(
            equality_name_filter(&[LabelMatcher::equal(METRIC_NAME_LABEL, "foo")]),
            Some("foo".to_string())
        );
        // Prefix regex now prunes, via the sentinel encoding.
        assert_eq!(equality_name_filter(&[re("^foo.*$")]), Some(encoded("foo")));
        // Non-prefix regex and negations bypass.
        assert_eq!(equality_name_filter(&[re(".*foo.*")]), None);
        assert_eq!(equality_name_filter(&[re("foo|bar")]), None);
        assert_eq!(
            equality_name_filter(&[LabelMatcher::not_equal(METRIC_NAME_LABEL, "foo")]),
            None
        );
        // Two `__name__` matchers bypass, even when each alone would prune.
        assert_eq!(
            equality_name_filter(&[LabelMatcher::equal(METRIC_NAME_LABEL, "foo"), re("^foo.*$"),]),
            None
        );
        // No `__name__` matcher bypasses.
        assert_eq!(
            equality_name_filter(&[LabelMatcher::equal("job", "api")]),
            None
        );
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
        // spans -> spans (ADR-0045 decision 5, the third arm).
        assert_eq!(
            SqlExecutor::target_signal("SELECT trace_id FROM spans").expect("ok"),
            TargetSignal::Spans
        );
    }

    #[test]
    fn target_signal_rejects_a_query_touching_both_tables() {
        let err =
            SqlExecutor::target_signal("SELECT * FROM samples JOIN logs ON samples.ts = logs.ts")
                .expect_err("both tables rejected");
        assert!(matches!(err, SqlError::CrossSignalQuery));
    }

    /// ADR-0045 decision 5: the one-signal-per-query rule now spans three
    /// tables, so a query naming any two of {samples, logs, spans} -- or all
    /// three -- is rejected as `CrossSignalQuery`, exactly as samples+logs was.
    #[test]
    fn target_signal_rejects_two_of_the_three_tables() {
        for sql in [
            "SELECT * FROM samples JOIN spans ON samples.ts = spans.start_ts",
            "SELECT * FROM logs JOIN spans ON logs.ts = spans.start_ts",
            "SELECT * FROM samples JOIN logs ON samples.ts = logs.ts \
             JOIN spans ON spans.start_ts = logs.ts",
        ] {
            let err = SqlExecutor::target_signal(sql)
                .expect_err("a query naming two of the three tables is rejected");
            assert!(
                matches!(err, SqlError::CrossSignalQuery),
                "expected CrossSignalQuery for {sql:?}, got {err:?}"
            );
        }
    }

    /// A CTE named after the other table is query-local, not a base-table
    /// reference, so it must not flip the signal or trip the cross-signal
    /// rejection (ADR-0033 amendment; regression for the wiring wave, which
    /// collected CTE names as if they were real tables).
    #[test]
    fn target_signal_does_not_treat_a_cte_name_as_the_real_table() {
        // A CTE named `logs` reading only `samples` is a metrics query.
        assert_eq!(
            SqlExecutor::target_signal(
                "WITH logs AS (SELECT value FROM samples) SELECT count(*) FROM logs"
            )
            .expect("cte named logs over samples is metrics-only"),
            TargetSignal::Metrics
        );
        // A CTE named `samples` reading only `logs` is a logs query.
        assert_eq!(
            SqlExecutor::target_signal(
                "WITH samples AS (SELECT body FROM logs) SELECT count(*) FROM samples"
            )
            .expect("cte named samples over logs is logs-only"),
            TargetSignal::Logs
        );
    }

    /// ADR-0045 decision 5 / ADR-0033 decision C: a cross-signal query is
    /// rejected before any catalog work, so it costs zero store requests. Runs
    /// a `spans` + `samples` join through `execute` against an
    /// `InstrumentedStore` and asserts both that the error is `CrossSignalQuery`
    /// and that the store saw no LIST/GET/HEAD at all -- proving the rejection
    /// lands in `target_signal` ahead of `resolve`'s catalog LIST, not after it.
    #[tokio::test]
    async fn cross_signal_query_rejected_before_any_catalog_listing() {
        use ravel_catalog::CatalogConfig;
        use ravel_object_store::instrument::InstrumentedStore;
        use ravel_object_store::memory::MemoryStore;

        let store = Arc::new(InstrumentedStore::new(MemoryStore::new()));
        let catalog =
            Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
        let executor = SqlExecutor::new(
            catalog,
            SegmentFetcher::new(store.clone()),
            LogSegmentFetcher::new(store.clone()),
            SpanSegmentFetcher::new(store.clone()),
            SqlConfig::default(),
            1 << 30,
        );

        let request = SqlRequest {
            sql: "SELECT * FROM spans JOIN samples ON spans.start_ts = samples.ts".to_string(),
            window: TimeRange {
                start_ns: 0,
                end_ns: 2_000,
            },
            min_tokens: Vec::new(),
            now_ns: 2_000,
            deadline: Duration::from_secs(30),
        };

        let before = store.metrics().snapshot();
        let err = executor
            .execute(TenantHash([7u8; 16]), &request)
            .await
            .expect_err("a spans+samples cross-signal query is rejected");
        let after = store.metrics().snapshot();

        assert!(
            matches!(err, SqlError::CrossSignalQuery),
            "expected CrossSignalQuery, got {err:?}"
        );
        assert_eq!(
            after.list.calls, before.list.calls,
            "a cross-signal query must be rejected before any catalog LIST"
        );
        assert_eq!(
            after.get.calls, before.get.calls,
            "a cross-signal query must issue no GET"
        );
        assert_eq!(
            after.head.calls, before.head.calls,
            "a cross-signal query must issue no HEAD"
        );
    }

    #[test]
    fn resources_exhausted_keeps_its_own_counts() {
        let df = DataFusionError::ResourcesExhausted("needs 40 bytes, limit 8".to_string());
        let err = execution_error(df);
        assert!(matches!(err, SqlError::ResourcesExhausted(_)));
        assert!(err.client_message().contains("limit 8"));
    }

    /// An executor over an empty store, for the idle-accountant eviction tests.
    fn eviction_test_executor() -> SqlExecutor {
        use ravel_catalog::{Catalog, CatalogConfig};
        use ravel_object_store::memory::MemoryStore;

        let store = Arc::new(MemoryStore::new());
        let catalog = Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("cat"));
        let fetcher = SegmentFetcher::new(store.clone());
        let log_fetcher = LogSegmentFetcher::new(store.clone());
        let span_fetcher = SpanSegmentFetcher::new(store.clone());
        SqlExecutor::new(
            catalog,
            fetcher,
            log_fetcher,
            span_fetcher,
            SqlConfig::default(),
            1 << 30,
        )
    }

    /// ADR-0069 decision 2: a tenant idle past the TTL has its
    /// memory accountant evicted, a subsequent access re-derives a fresh one,
    /// and a tenant still running queries (recently touched) survives the same
    /// sweep. Deterministic via injected `now_ns` (touch_tenant, the resolve
    /// funnel's stamp, is driven directly here so no store I/O is needed).
    #[test]
    fn idle_sql_accountant_evicted_and_rederived() {
        const NS_PER_HOUR: i64 = 3_600_000_000_000;
        let exec = eviction_test_executor();
        let idle = TenantHash([1; 16]);
        let active = TenantHash([2; 16]);
        let ttl_ns = 100 * NS_PER_HOUR;

        let t0 = 1_000 * NS_PER_HOUR;
        exec.touch_tenant(idle, t0);
        exec.touch_tenant(active, t0);

        // The active tenant runs another query right before the sweep.
        let sweep_ns = t0 + ttl_ns + 1;
        exec.touch_tenant(active, sweep_ns);

        // Both accountants hold zero reservations, so only last-touch decides:
        // the idle one is evicted, the active one survives.
        let evicted = exec.evict_idle_accountants(sweep_ns, ttl_ns);
        assert_eq!(evicted, 1, "only the idle tenant's accountant is evicted");

        // Re-derivation: the evicted tenant's next access rebuilds a fresh
        // accountant with zero reserved bytes, at the configured ceiling.
        let rebuilt = exec.tenant_budget(idle);
        assert_eq!(rebuilt.reserved(), 0);
        assert_eq!(rebuilt.limit(), 1 << 30);
    }

    /// The zero-reservation guard: an accountant with outstanding reservations
    /// is never evicted, even when idle past the TTL, because a live query is
    /// still accounting against it (ADR-0069 decision 2). Dropping it would let
    /// a concurrent query build a second accountant and stop sharing the
    /// tenant ceiling.
    #[test]
    fn sql_accountant_with_outstanding_reservation_survives_sweep() {
        use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};

        use crate::memory::{CeilingBreach, TenantDelegatingPool};

        const NS_PER_HOUR: i64 = 3_600_000_000_000;
        let exec = eviction_test_executor();
        let tenant = TenantHash([7; 16]);
        let ttl_ns = 100 * NS_PER_HOUR;

        // Create the accountant (last-touch i64::MIN, so idle by any clock) and
        // hold an outstanding reservation against it through a query pool.
        let accountant = exec.tenant_budget(tenant);
        let pool: Arc<dyn MemoryPool> = Arc::new(TenantDelegatingPool::new(
            1 << 30,
            Arc::clone(&accountant),
            CeilingBreach::new(),
            QueryAccounting::new(),
        ));
        let reservation = MemoryConsumer::new("live-query").register(&pool);
        reservation.grow(4096);
        assert!(accountant.reserved() > 0);

        let evicted = exec.evict_idle_accountants(10 * NS_PER_HOUR, ttl_ns);
        assert_eq!(
            evicted, 0,
            "an accountant with outstanding reservations is never evicted"
        );

        // Once the reservation drops and the tenant is idle, the sweep reclaims it.
        drop(reservation);
        assert_eq!(accountant.reserved(), 0);
        assert_eq!(exec.evict_idle_accountants(10 * NS_PER_HOUR, ttl_ns), 1);
    }

    /// ADR-0044 acceptance test: one `execute` call, checked
    /// against an `InstrumentedStore`'s own before/after deltas, the same
    /// cross-check `Catalog::resolve_with_accounting`'s own test uses
    /// (crates/ravel-catalog/src/catalog.rs). Proves the SQL path now
    /// contributes to per-query accounting end to end, including the
    /// `peak_intermediate_bytes` high-water mark deliverable 2 adds.
    #[tokio::test]
    async fn sql_execute_records_requests_bytes_and_peak_memory() {
        use ravel_catalog::CatalogConfig;
        use ravel_commit::publish::RetryPolicy;
        use ravel_commit::record::NewCommitRecord;
        use ravel_commit::{keys, publish, record};
        use ravel_object_store::instrument::InstrumentedStore;
        use ravel_object_store::memory::MemoryStore;
        use ravel_object_store::{ObjectStoreBackend, PutOptions};
        use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
        use ravel_types::accounting::AccountedOp;
        use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};
        use uuid::Uuid;

        let tenant = TenantId::new("acceptance-424".to_string());
        let tenant_hash = tenant.hash();
        let labels = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: "m".to_string(),
        }])
        .expect("valid labels");
        let series_id = SeriesId::compute(&tenant, "m", &labels).expect("series id");
        let samples: Vec<Sample> = (0..1_000)
            .map(|i| Sample {
                ts_ns: i,
                value: i as f64,
            })
            .collect();

        let writer_id = Uuid::from_u128(4_240);
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let bounds = IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        };
        let written = SegmentWriter::write(
            vec![SeriesInput {
                series_id,
                labels,
                samples,
            }],
            identity,
            bounds,
        )
        .expect("write segment");

        let new_record = NewCommitRecord {
            tenant_hash,
            signal: Signal::Metrics,
            shard: 0,
            writer_id,
            writer_epoch: 1,
            writer_seq: 1,
            object_size: written.bytes.len() as u64,
            content_hash: written.summary.blake3,
            sample_count: written.summary.sample_count,
            series_count: written.summary.series_count,
            min_event_ts_ns: written.summary.min_event_ts_ns,
            max_event_ts_ns: written.summary.max_event_ts_ns,
            min_ingest_ts_ns: written.summary.min_event_ts_ns,
            max_ingest_ts_ns: written.summary.max_event_ts_ns,
            segment_format_version: 1,
            created_unix_ns: 1,
            ingest_hour_bucket: 0,
        };
        let rec = record::build(new_record).expect("valid commit record");
        let data_key = keys::reconstruct_data_key(&rec).expect("data key");

        let inner = MemoryStore::new();
        inner
            .put(&data_key, written.bytes, PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(&inner, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
        let store = Arc::new(InstrumentedStore::new(inner));

        let catalog =
            Arc::new(Catalog::new(store.clone(), CatalogConfig::default()).expect("catalog"));
        let fetcher = SegmentFetcher::new(store.clone());
        let log_fetcher = LogSegmentFetcher::new(store.clone());
        let span_fetcher = SpanSegmentFetcher::new(store.clone());
        let executor = SqlExecutor::new(
            catalog,
            fetcher,
            log_fetcher,
            span_fetcher,
            SqlConfig::default(),
            1 << 30,
        );

        let request = SqlRequest {
            sql: "SELECT ts, value FROM samples".to_string(),
            window: TimeRange {
                start_ns: 0,
                end_ns: 2_000,
            },
            min_tokens: Vec::new(),
            now_ns: 2_000,
            deadline: Duration::from_secs(30),
        };

        let before = store.metrics().snapshot();
        let outcome = executor
            .execute(tenant_hash, &request)
            .await
            .expect("accounted execute");
        let after = store.metrics().snapshot();

        let get_calls_diff = after.get.calls - before.get.calls;
        let list_calls_diff = after.list.calls - before.list.calls;
        let head_calls_diff = after.head.calls - before.head.calls;
        let get_bytes_diff = after.get.bytes - before.get.bytes;

        assert_eq!(head_calls_diff, 0, "the SQL path issues no HEAD request");

        let acc = outcome.accounting;
        assert_eq!(acc.s3_requests(AccountedOp::Get), get_calls_diff);
        assert_eq!(acc.s3_requests(AccountedOp::List), list_calls_diff);
        assert_eq!(acc.s3_requests(AccountedOp::Head), 0);
        assert_eq!(acc.s3_bytes(AccountedOp::Get), get_bytes_diff);
        assert_eq!(
            acc.total_s3_requests(),
            get_calls_diff + list_calls_diff + head_calls_diff
        );
        assert!(
            acc.peak_intermediate_bytes > 0,
            "the tenant accountant's reserved high-water mark must feed \
             observe_intermediate_bytes, so this \
             is not always zero"
        );
    }
}
