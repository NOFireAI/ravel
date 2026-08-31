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

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::error::ArrowError;
use datafusion::common::DFSchema;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::disk_manager::DiskManager;
use datafusion::execution::memory_pool::{MemoryLimit, MemoryPool, UnboundedMemoryPool};
use datafusion::logical_expr::{Aggregate, Distinct, Expr, ExprSchemable, LogicalPlan};
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
use crate::logs_pushdown::extract_logs;
use crate::memory::{CeilingBreach, TenantMemoryAccountant};
use crate::output::QueryOutput;
use crate::provider::RavelTableProvider;
use crate::pushdown::extract;
use crate::session::{
    LOGS_TABLE, SAMPLES_TABLE, SPANS_TABLE, SessionTable, SpillDecision, build_session,
};
use crate::spans_fetcher::SpanSegmentFetcher;
use crate::spans_provider::SpansTableProvider;
use crate::spill::{OperatorSpill, SpillCounts, SpillScratch, accumulate_spill_counts};
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
    /// This query's spill totals (ADR-0954), read off the executed plan's own
    /// DataFusion counters after the stream stopped, the same way the block
    /// counters above are. All zero on the default configuration, where the
    /// disk manager is disabled and no query can spill at all.
    ///
    /// The totals live here because they are operator metrics; the
    /// per-operator attribution that makes a spill traceable to the operator
    /// that wrote it lives on [`SqlOutcome::spill_by_operator`], which does not
    /// have to stay `Copy`.
    pub spill: SpillCounts,
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
    /// Which operator wrote this query's spill, and how much (ADR-0954). Empty
    /// for a query that did not spill, which is every query on the default
    /// configuration. The totals are on [`SqlStats::spill`]; this is the
    /// attribution, because "the aggregate spilled 4 files" and "the exchange
    /// spilled 4 files" are different findings that a pooled total cannot tell
    /// apart.
    pub spill_by_operator: Vec<OperatorSpill>,
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

            let (result, emitted, blocks, spill, spill_by_operator) = self
                .attempt(tenant_hash, req, snapshot, &accounting, &declared)
                .await;
            stats.batches_emitted += emitted;
            // Recorded for the failing attempt too: a spill that happened
            // before the failure is a fact about this query, and a retried
            // attempt overwrites it with its own, matching how the block
            // counters and `QueryAccounting` treat a discarded attempt.
            stats.spill = spill;

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
                        spill_by_operator,
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
        // ADR-0094 decision 1/2: classify the query's aggregates and GROUP BY
        // keys before the real session is built, right here at the one call site
        // that funnels into `build_session`. The result flips
        // `repartition_aggregations` on only for a query proven exact-typed.
        // Skipped entirely when the process-wide flag is off (decision 2): an
        // unclassified query already gets `false`, so the extra plan+analyze
        // pass costs nothing when the feature is disabled. Classified against
        // `extras.declared` (the same declared columns the real logs provider
        // installs), before it is moved into the table below.
        //
        // ONE analyzed plan serves both consumers below. Each of them used to
        // build its own throwaway session and analyze the same SQL, so a logs
        // query with declared columns planned three times before executing
        // once. The build happens only when a consumer will read it.
        let wants_stats_gate = matches!(Self::target_signal(sql), Ok(TargetSignal::Logs))
            && !extras.declared.is_empty();
        // ADR-0954: the spill eligibility predicate reads the same analyzed
        // plan, so a configured-spill deployment is a third consumer of it
        // rather than a second analyze pass.
        let wants_spill_gate = self.config.spill.is_some();
        let analyzed =
            if self.config.parallel_final_aggregation || wants_stats_gate || wants_spill_gate {
                self.analyzed_classification_plan(tenant_hash, sql, &extras.declared)
                    .await
            } else {
                None
            };
        // Fail CLOSED, unchanged: an unbuildable plan is not exact-typed.
        let exact_typed_aggregates = self.config.parallel_final_aggregation
            && analyzed.as_ref().is_some_and(plan_is_exact_typed);
        // ADR-0954: spill needs BOTH an operator-configured scratch area and a
        // plan whose every aggregate is exact under a changed folding order.
        // Fail closed the same way: an unbuildable plan is not eligible, so it
        // gets the disabled disk manager and today's typed refusal.
        //
        // The scratch directory is created here, before planning, so a missing
        // or unwritable spill area is a typed `SpillUnavailable` raised with
        // nothing written and no operator started, rather than an opaque IO
        // failure from inside a spilling operator half way through a query.
        let scratch = match &self.config.spill {
            Some(spill) if analyzed.as_ref().is_some_and(plan_is_spill_eligible) => {
                Some((SpillScratch::create(spill)?, spill.max_bytes))
            }
            _ => None,
        };
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
                    self.config.clone(),
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
            TargetSignal::Logs => {
                // ADR-0850: resolved once per plan from the current folded
                // catalog HEAD, independently of `snapshot` (a different
                // object, not versioned against the pinned snapshot). `None`
                // -- nothing folded yet, no configured typed columns, or the
                // last fold's build/PUT failed -- reproduces the
                // pre-ADR-0850 provider exactly; every metadata-only path
                // keyed off it degrades to scanning on a `None`.
                let column_stats = if extras.declared.is_empty() {
                    // No configured typed columns: no metadata-only column path
                    // can ever apply, so skip the HEAD GET load_column_stats
                    // would issue. This keeps a predicate-free COUNT(*) on a
                    // tenant with no declared columns reading zero objects, and
                    // reproduces the pre-ADR-0850 provider exactly.
                    None
                } else if !self.logs_column_stats_eligible(
                    &snapshot,
                    analyzed.as_ref(),
                    &extras.declared,
                ) {
                    // Issue #888: no metadata-only column path can fire for this
                    // plan (decided from the plan and the resolved snapshot
                    // alone, ahead of the load), so skip the two GETs
                    // load_column_stats would issue. A `None` here reproduces
                    // the pre-ADR-0850 provider exactly, the same as an absent
                    // stats object would, so every failing-open behavior is
                    // preserved.
                    None
                } else {
                    self.catalog
                        .load_column_stats(&tenant_hash, Signal::Logs, accounting)
                        .await?
                };
                SessionTable::Logs(Arc::new(
                    LogsTableProvider::new(
                        snapshot,
                        tenant_hash,
                        self.log_fetcher.clone(),
                        accounting.clone(),
                    )
                    .with_declared_columns(extras.declared)
                    .with_column_stats(column_stats),
                ))
            }
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

        let decision = match &scratch {
            Some((scratch, max_bytes)) => SpillDecision::Enabled {
                dir: scratch.dir(),
                max_bytes: *max_bytes,
            },
            None => SpillDecision::Disabled,
        };
        let ctx = build_session(&self.config, pool, table, exact_typed_aggregates, decision)
            .map_err(plan_error)?;
        let frame = ctx.sql(sql).await.map_err(plan_error)?;
        let schema = frame.schema().inner().clone();
        Ok(PinnedQuery {
            ctx,
            frame,
            schema,
            breach,
            scratch: scratch.map(|(scratch, _)| scratch),
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
            self.config.clone(),
            accounting.clone(),
        ));
        let plan = provider
            .worker_fragment(target_partitions, &segments)
            .map_err(plan_error)?;
        // ADR-0094 decision 2: the worker fragment plans no new SQL and runs no
        // aggregation of its own, so it never repartitions -- always `false`,
        // never through the classification check.
        // The worker fragment plans no SQL and runs no aggregation, so it never
        // spills: the disk manager stays disabled regardless of the deployment's
        // spill configuration.
        let ctx = build_session(
            &self.config,
            pool,
            SessionTable::Metrics(Arc::clone(&provider)),
            false,
            SpillDecision::Disabled,
        )
        .map_err(plan_error)?;
        let schema = plan.schema();
        PinnedStream::start(ctx, plan, schema, breach)
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
        // Route through `build_session`, the same construction
        // `analyzed_classification_plan` uses, rather than a bare
        // `SessionContext::new()` (ADR-0097 decision 7). This carries the
        // `EmptyObjectStoreRegistry` (ADR-0013 security invariant 1) and the
        // per-registry allowlist deregistrations plus the UDAF replacements,
        // so the two throwaway-session sites converge on one path. The
        // per-table `label`/`label_match` UDFs a metrics session needs are
        // registered by `build_session` itself. This is a metrics-only site
        // (its sole caller resolves the `__name__` postings filter).
        let table = self.empty_snapshot_table(TargetSignal::Metrics, tenant_hash, &[]);
        // A private, unbounded pool: this session only logical-plans and never
        // executes, so nothing is reserved against it and it never touches the
        // tenant accountant.
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        // Plan-inspection only: this throwaway session never executes, so it
        // never spills whatever the query's own session was granted.
        let ctx = build_session(
            &self.config,
            pool,
            table,
            false,
            crate::session::SpillDecision::Disabled,
        )
        .ok()?;
        let plan = ctx.state().create_logical_plan(sql).await.ok()?;
        let mut predicates = Vec::new();
        collect_filter_predicates(&plan, &mut predicates);
        equality_name_filter(&extract(&predicates).matchers)
    }

    /// ADR-0094 decision 1: whether every aggregate expression and GROUP BY key
    /// in `sql`, once fully type-coerced, is provably order/partition-independent
    /// -- the precondition for fanning the final aggregation across partitions.
    ///
    /// Reuses `pushed_down_name_filter`'s throwaway-session shape (an empty
    /// snapshot, no I/O) but with one deliberate difference: the plan is run
    /// through DataFusion's `Analyzer` (type coercion) before classification, so
    /// a `sum`/`min`/`max` argument or a group key is classified by its
    /// *resolved* Arrow type, not its syntactic operands (ADR-0094 decision 1;
    /// `create_logical_plan` alone does not coerce). The throwaway session
    /// registers everything a real query needs to plan -- the table's scalar
    /// UDFs, the map-field `ExprPlanner`, and the tenant's ADR-0090 declared
    /// columns -- via `build_session`, so a legitimate query does not silently
    /// lose the optimization by failing to plan here.
    ///
    /// Fail-closed, the opposite polarity from `pushed_down_name_filter`'s
    /// fail-open `None`: any error building or analyzing the throwaway plan
    /// classifies the query as NOT exact, because wrongly admitting an
    /// unclassifiable query could repartition an aggregate this check never
    /// verified.
    // Test-only since the shared-plan change: the production path composes the
    // same two pieces inline (`analyzed_classification_plan` then
    // `plan_is_exact_typed`) against the plan it shares with the column-stats
    // gate, rather than building a second throwaway session. This composition
    // is identical to prod's, so the ADR-0094 cases below still exercise the
    // shipped classification.
    #[cfg(test)]
    async fn classify_exact_typed(
        &self,
        tenant_hash: TenantHash,
        sql: &str,
        declared: &[DeclaredColumn],
    ) -> bool {
        match self
            .analyzed_classification_plan(tenant_hash, sql, declared)
            .await
        {
            Some(plan) => plan_is_exact_typed(&plan),
            None => false,
        }
    }

    /// Build the throwaway empty-snapshot session for `sql`'s target signal,
    /// logical-plan `sql`, and run DataFusion's analyzer (type coercion) over
    /// the result. `None` on any error (the fail-closed source for
    /// [`Self::classify_exact_typed`]).
    ///
    /// The analyzer step is the `execute_and_check` pass DataFusion's own
    /// physical planning applies before optimization; running it here is what
    /// resolves, for example, `avg`'s argument to `Float64` and an integer
    /// `sum`'s argument to `Int64` before the walk inspects their types.
    async fn analyzed_classification_plan(
        &self,
        tenant_hash: TenantHash,
        sql: &str,
        declared: &[DeclaredColumn],
    ) -> Option<LogicalPlan> {
        let target = Self::target_signal(sql).ok()?;
        let table = self.empty_snapshot_table(target, tenant_hash, declared);
        // A private, unbounded pool: this session never executes, so nothing is
        // ever reserved against it and it never touches the tenant accountant.
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        // `exact_typed_aggregates` is irrelevant to a plan we only inspect; the
        // classification decides the real session's value, not this throwaway.
        // Plan-inspection only: this throwaway session never executes, so it
        // never spills whatever the query's own session was granted.
        let ctx = build_session(
            &self.config,
            pool,
            table,
            false,
            crate::session::SpillDecision::Disabled,
        )
        .ok()?;
        let state = ctx.state();
        let plan = state.create_logical_plan(sql).await.ok()?;
        let config_options = Arc::clone(state.config_options());
        state
            .analyzer()
            .execute_and_check(plan, &config_options, |_, _| {})
            .ok()
    }

    /// Whether any ADR-0850 metadata-only column-statistics path could fire for
    /// this logs plan, decided from the plan and the resolved snapshot alone,
    /// BEFORE the two-GET `load_column_stats` (issue #888). `false` means every
    /// path is provably out of reach, so the load is pure waste and is skipped;
    /// the query then answers exactly as an absent stats object would (the
    /// pre-ADR-0850 provider).
    ///
    /// Every branch fails OPEN (returns `true`, keeping the load) on any
    /// uncertainty, because a false `false` would turn ADR-0850 off for a plan
    /// that could have used it. The three metadata-only paths -- q07
    /// `MIN`/`MAX(declared)`, q02 `COUNT(*)` with a residual `declared <> lit`
    /// filter, q08 `COUNT(*) GROUP BY declared` -- all gate downstream on
    /// [`crate::logs_scan`]'s `stats_are_exact` (no pending erasure, no content
    /// or prune predicate, and a ts bound that clips no touched segment) and
    /// all are aggregates referencing a declared column. This mirrors exactly
    /// those necessary conditions; anything they cannot prove is left to
    /// decline downstream and still loads here.
    fn logs_column_stats_eligible(
        &self,
        snapshot: &Snapshot,
        analyzed: Option<&LogicalPlan>,
        declared: &[DeclaredColumn],
    ) -> bool {
        // Pending selective erasure (ADR-0064) rejects the ENTIRE query for
        // every metadata-only path (safety lemma): a pending erasure removes
        // rows the precomputed counts/extrema still include. Snapshot-only and
        // certain, so it is checked first and without building a plan.
        if !snapshot.pending_erasure.is_empty() {
            return false;
        }
        // The analyzed logical plan carries both the aggregate shape and the
        // filter conjuncts this check needs. If it cannot be built, fall back
        // to loading (the pre-hoist behavior always loaded).
        // Fail OPEN, unchanged: no plan means keep the load. The caller shares
        // one analyzed plan with the exact-typed classification rather than
        // building a second throwaway session for the same SQL.
        let Some(plan) = analyzed else {
            return true;
        };
        // Every path is an aggregate over a declared column. A non-aggregate
        // (a raw select, a top-N) or an aggregate touching no declared column
        // (a predicate-free `COUNT(*)`, answered from `sample_count` with no
        // column stats) can never consume the loaded object.
        if !plan_has_aggregate(plan) || !plan_references_declared(plan, declared) {
            return false;
        }
        // Re-derive the reader pushdown from the plan's filter conjuncts with
        // the SAME extractor the scan uses, so the content/prune/ts decisions
        // match `stats_are_exact` exactly. A content or prune predicate makes
        // every path decline; the analyzed (pre-optimizer) plan can only carry
        // fewer such predicates than the scan ultimately sees, so a match here
        // is real and skipping is safe, while a miss merely loads.
        let mut filters = Vec::new();
        collect_filter_predicates(plan, &mut filters);
        let pushdown = extract_logs(&filters, declared);
        if !pushdown.content.is_empty() || !pushdown.prune.is_empty() {
            return false;
        }
        // A ts bound that clips even one touched segment makes `stats_are_exact`
        // fail closed. The touched set is the ts-overlapping subset the provider
        // resolves (`pruned_segments`); a segment fully outside the bound is
        // pruned away and never reaches the containment check, so it must not
        // count as a clip here either.
        let (ts_min, ts_max) = (pushdown.ts_min(), pushdown.ts_max());
        let clips = snapshot
            .segments
            .iter()
            .filter(|s| LogSegmentFetcher::ts_range_relevant(s, ts_min, ts_max))
            .any(|s| !(ts_min <= s.min_event_ts_ns && s.max_event_ts_ns <= ts_max));
        if clips {
            return false;
        }
        true
    }

    /// A throwaway [`SessionTable`] over an empty snapshot for `target`,
    /// carrying the same table-specific surface a real query of that signal
    /// would (the logs provider's ADR-0090 declared columns included), so the
    /// classification plan resolves identically to the real one. Issues no I/O:
    /// the snapshot has no segments and the session is discarded unexecuted.
    fn empty_snapshot_table(
        &self,
        target: TargetSignal,
        tenant_hash: TenantHash,
        declared: &[DeclaredColumn],
    ) -> SessionTable {
        let empty_snapshot = || Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        match target {
            TargetSignal::Metrics => SessionTable::Metrics(Arc::new(RavelTableProvider::new(
                empty_snapshot(),
                tenant_hash,
                self.fetcher.clone(),
                self.config.clone(),
                QueryAccounting::new(),
            ))),
            TargetSignal::Logs => SessionTable::Logs(Arc::new(
                LogsTableProvider::new(
                    empty_snapshot(),
                    tenant_hash,
                    self.log_fetcher.clone(),
                    QueryAccounting::new(),
                )
                .with_declared_columns(declared.to_vec()),
            )),
            TargetSignal::Spans => SessionTable::Spans(Arc::new(SpansTableProvider::new(
                empty_snapshot(),
                tenant_hash,
                self.span_fetcher.clone(),
                QueryAccounting::new(),
            ))),
        }
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
    ) -> (
        Result<QueryOutput, SqlError>,
        usize,
        BlockCounts,
        SpillCounts,
        Vec<OperatorSpill>,
    ) {
        let planned = match self
            .plan_pinned(tenant_hash, snapshot, &req.sql, accounting, declared)
            .await
        {
            Ok(planned) => planned,
            Err(e) => {
                return (
                    Err(e),
                    0,
                    BlockCounts::default(),
                    SpillCounts::default(),
                    Vec::new(),
                );
            }
        };
        let schema = planned.schema();

        let mut stream = match planned.execute().await {
            Ok(stream) => stream,
            Err(e) => {
                return (
                    Err(e),
                    0,
                    BlockCounts::default(),
                    SpillCounts::default(),
                    Vec::new(),
                );
            }
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
                // would misattribute. The spill counters are different: they
                // record what was written, and what was written before a
                // failure was still written, so they are read on both paths.
                Err(e) => {
                    let (spill, by_operator) = stream.spill_counts();
                    return (Err(e), emitted, BlockCounts::default(), spill, by_operator);
                }
            }
        }

        // The stream drained cleanly: the plan's LogsScanExec counters are now
        // final, so read them off the plan we kept.
        let blocks = stream.block_counts();
        let (spill, by_operator) = stream.spill_counts();
        (
            Ok(QueryOutput::new(schema, batches)),
            emitted,
            blocks,
            spill,
            by_operator,
        )
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
    /// This query's spill scratch directory (ADR-0954), present only when
    /// spill is configured AND this plan passed the eligibility predicate.
    /// Declared after `ctx` so it drops after the session that owns the files
    /// inside it, and moved into the [`PinnedStream`] on execute so a query
    /// abandoned mid-stream still removes its scratch.
    scratch: Option<SpillScratch>,
}

impl PinnedQuery {
    /// The planned result schema.
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Build the physical plan for this query without consuming it or starting
    /// a stream, for plan-shape inspection (ADR-0094 decision 5's EXPLAIN-shape
    /// tests assert whether a `RepartitionExec` fans the `Final` `AggregateExec`
    /// out). This is the same first step [`Self::execute`] takes, run over the
    /// frame's logical plan by reference so it can be called on a borrow.
    pub async fn create_physical_plan(&self) -> Result<Arc<dyn ExecutionPlan>, SqlError> {
        self.ctx
            .state()
            .create_physical_plan(self.frame.logical_plan())
            .await
            .map_err(plan_error)
    }

    /// Start the plan's stream. The session stays alive inside the returned
    /// [`PinnedStream`] for as long as the stream does.
    pub async fn execute(self) -> Result<PinnedStream, SqlError> {
        let PinnedQuery {
            ctx,
            frame,
            schema,
            breach,
            scratch,
        } = self;
        // Build the physical plan explicitly rather than through
        // `frame.execute_stream()` (which does the same two steps internally)
        // so the plan handle survives the stream and its `LogsScanExec`
        // DataFusion counters can be read after the drain. The two
        // are equivalent: `execute_stream` is `create_physical_plan` then
        // `execute_stream(plan, task_ctx)`.
        let plan = frame.create_physical_plan().await.map_err(plan_error)?;
        PinnedStream::start_with_scratch(ctx, plan, schema, breach, scratch)
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
    /// Set once an operator has panicked under [`Stream::poll_next`]. A stream
    /// whose poll unwound is left in whatever state the panic interrupted, so
    /// it is never polled again; this fuses it instead.
    panicked: bool,
    /// The pool installed on `ctx`'s `RuntimeEnv` (issue #740). Read at every
    /// execution-error mapping so a `ResourcesExhausted` DataFusion raises
    /// against a spill-capable operator (which names the operator holding the
    /// reservation, not the consumer that filled it) can be re-attributed from
    /// this pool's own `used`/`limit`, the same pool `try_grow` refused
    /// against. Derived from `ctx` in [`Self::start`] rather than threaded in
    /// separately, so no caller of `start` changes.
    pool: Arc<dyn MemoryPool>,
    /// Spill bookkeeping for this query (ADR-0954), `None` whenever the
    /// session's disk manager is disabled -- which is every query on the
    /// default configuration, so the default path pays nothing for this.
    spill: Option<SpillState>,
    /// This query's scratch directory. Declared LAST so it drops after `_ctx`:
    /// the session's `RuntimeEnv` owns the spill files inside it and must
    /// release them first. Removing the directory here is what makes cleanup
    /// hold on the completion, error, AND cancellation paths alike -- all
    /// three end with this value dropping.
    _scratch: Option<SpillScratch>,
}

/// The live spill bookkeeping [`PinnedStream`] keeps while a query with an
/// enabled disk manager runs.
struct SpillState {
    /// The session's disk manager, for its `spilling_progress()` gauge: the
    /// bytes currently written across this query's spill files, and how many
    /// of those files are open.
    disk_manager: Arc<DiskManager>,
    /// The configured per-query scratch ceiling, echoed in
    /// [`SqlError::spill_budget_exhausted`]. Read from the disk manager rather
    /// than threaded in, so it is the ceiling actually installed.
    quota: u64,
    /// Set while the last poll observed at least one open spill file; `None`
    /// otherwise. See [`SpillCounts::duration`] for exactly what the resulting
    /// figure measures.
    active_since: Option<Instant>,
    /// Accumulated spill window across every open/close cycle so far.
    elapsed: Duration,
}

impl PinnedStream {
    /// Execute `plan` under `ctx` and wrap its stream in this type's ceiling
    /// and panic boundaries.
    ///
    /// This is the constructor [`PinnedQuery::execute`] uses. It is public so
    /// a test can drive a plan the `SqlExecutor` path cannot produce: that
    /// path registers exactly one table provider of its own choosing
    /// (`crate::session::build_session`, security invariant 1), so an operator
    /// that panics on demand has no way in through it.
    pub fn start(
        ctx: SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        breach: Arc<CeilingBreach>,
    ) -> Result<Self, SqlError> {
        PinnedStream::start_with_scratch(ctx, plan, schema, breach, None)
    }

    /// [`Self::start`] with this query's spill scratch directory attached
    /// (ADR-0954), so the directory outlives the stream and is removed when the
    /// stream drops. `None` is byte-identical to [`Self::start`].
    pub fn start_with_scratch(
        ctx: SessionContext,
        plan: Arc<dyn ExecutionPlan>,
        schema: SchemaRef,
        breach: Arc<CeilingBreach>,
        scratch: Option<SpillScratch>,
    ) -> Result<Self, SqlError> {
        // The same pool `build_session` installed on `ctx`'s `RuntimeEnv`
        // (`crate::session`), read back here rather than passed in: every
        // `ResourcesExhausted` this stream maps needs it, and deriving it from
        // `ctx` keeps every `start` caller (the local path, the Flight SQL
        // worker fragment, and the panic-boundary test) unchanged.
        let pool = Arc::clone(&ctx.runtime_env().memory_pool);
        // Same derivation for the disk manager: whether this query can spill
        // at all is a property of the session `build_session` built, not
        // something a caller of this constructor should be able to disagree
        // with. `tmp_files_enabled()` is false for every `SpillDecision::
        // Disabled` session, which is every session on the default config.
        let disk_manager = Arc::clone(&ctx.runtime_env().disk_manager);
        let spill = disk_manager.tmp_files_enabled().then(|| SpillState {
            quota: disk_manager.max_temp_directory_size(),
            disk_manager,
            active_since: None,
            elapsed: Duration::ZERO,
        });
        let inner = execute_stream(Arc::clone(&plan), ctx.task_ctx()).map_err(plan_error)?;
        Ok(PinnedStream {
            _ctx: ctx,
            inner,
            schema,
            breach,
            plan,
            panicked: false,
            pool,
            spill,
            _scratch: scratch,
        })
    }

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

    /// This query's spill totals and their per-operator attribution
    /// (ADR-0954), read off the plan's own DataFusion counters the same way
    /// [`Self::block_counts`] reads the scan's.
    ///
    /// Meaningful once the stream has stopped producing, whether it drained or
    /// failed: a spill that happened before a failure is still a fact about the
    /// query. All zero, with no operators listed, for a query that did not
    /// spill.
    fn spill_counts(&self) -> (SpillCounts, Vec<OperatorSpill>) {
        let mut totals = SpillCounts::default();
        let mut by_operator = Vec::new();
        accumulate_spill_counts(&self.plan, &mut totals, &mut by_operator);
        totals.duration = self
            .spill
            .as_ref()
            .map_or(Duration::ZERO, SpillState::window);
        (totals, by_operator)
    }

    /// Sample the disk manager's open-spill-file gauge and fold the interval
    /// since the previous sample into the spill window. Called on every poll
    /// of a spill-enabled query and never on any other, so the default path
    /// runs none of it.
    fn sample_spill_window(&mut self) {
        let Some(spill) = self.spill.as_mut() else {
            return;
        };
        let active = spill.disk_manager.spilling_progress().active_files_count > 0;
        match (active, spill.active_since) {
            (true, None) => spill.active_since = Some(Instant::now()),
            (false, Some(since)) => {
                spill.elapsed = spill.elapsed.saturating_add(since.elapsed());
                spill.active_since = None;
            }
            _ => {}
        }
    }

    /// Map an execution error, classifying the two spill-specific failures
    /// first (ADR-0954). Kept here rather than inside [`execution_error`]
    /// because only the stream holds the disk manager the figures come from.
    ///
    /// Runs only for a query whose disk manager is enabled, so a query on the
    /// default configuration reaches `execution_error` by exactly the path it
    /// did before spill existed.
    fn map_execution_error(&self, err: DataFusionError) -> SqlError {
        if let Some(spill) = self.spill.as_ref()
            && let Some(failure) = spill_failure_kind(&err)
        {
            return match failure {
                // DataFusion raises the scratch-quota trip as a plain
                // `ResourcesExhausted`, the same variant a memory-pool refusal
                // uses, with text telling the caller to raise a DataFusion
                // option no Ravel client can reach. The memory and scratch
                // budgets are independently enforced, so they stay
                // independently reportable: re-typed here, with the figures
                // restated from the disk manager's own gauge.
                SpillFailure::Quota => SqlError::spill_budget_exhausted(
                    spill.disk_manager.used_disk_space(),
                    spill.quota,
                ),
                // A filesystem error reaching this stream while spill is
                // enabled is a scratch failure: every read of durable data
                // goes through this crate's own fetchers, which surface as
                // `SqlError::Fetch`/`LogFetch`/`SpanFetch`, never as a bare
                // DataFusion IO error. The commonest cause is the scratch
                // volume filling mid-write.
                SpillFailure::Io(detail) => SqlError::SpillUnavailable(format!(
                    "spill file write failed while the query held \
                     {} of {} scratch bytes: {detail}",
                    spill.disk_manager.used_disk_space(),
                    spill.quota
                )),
            };
        }
        execution_error(err, &self.pool)
    }
}

/// The two spill-specific failures, told apart from every other execution
/// error.
enum SpillFailure {
    /// The per-query scratch quota was exceeded.
    Quota,
    /// The scratch area could not be written; the payload is the IO detail,
    /// logged server-side only.
    Io(String),
}

/// Find a spill failure at any wrapper depth, mirroring
/// [`take_sql_error`]'s unwrapping but by reference: the error is still needed
/// intact if this returns `None`.
fn spill_failure_kind(err: &DataFusionError) -> Option<SpillFailure> {
    match err {
        DataFusionError::ResourcesExhausted(msg)
            if msg.contains(crate::error::MSG_SPILL_QUOTA_MARKER) =>
        {
            Some(SpillFailure::Quota)
        }
        DataFusionError::IoError(io) => Some(SpillFailure::Io(io.to_string())),
        DataFusionError::Context(_, inner) | DataFusionError::Diagnostic(_, inner) => {
            spill_failure_kind(inner)
        }
        DataFusionError::Shared(inner) => spill_failure_kind(inner),
        DataFusionError::External(boxed) => boxed
            .downcast_ref::<DataFusionError>()
            .and_then(spill_failure_kind),
        DataFusionError::ArrowError(arrow, _) => match arrow.as_ref() {
            ArrowError::ExternalError(boxed) => boxed
                .downcast_ref::<DataFusionError>()
                .and_then(spill_failure_kind),
            _ => None,
        },
        DataFusionError::Collection(errors) => errors.iter().find_map(spill_failure_kind),
        _ => None,
    }
}

impl SpillState {
    /// The spill window so far, including an interval still open at the moment
    /// of the call.
    fn window(&self) -> Duration {
        match self.active_since {
            Some(since) => self.elapsed.saturating_add(since.elapsed()),
            None => self.elapsed,
        }
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
        if self.panicked {
            return Poll::Ready(None);
        }
        // The panic boundary (issue #737). A panic raised inside a DataFusion
        // operator, or inside an arrow kernel it calls, unwinds through this
        // poll: an `i32` offset overflow while a group-by table is decoded is
        // the case that put it here, but nothing about the boundary is
        // specific to that one. Unwinding past here kills whichever task is
        // driving the query -- the HTTP handler or the Flight `DoGet` -- and
        // the client sees a dropped connection rather than an error.
        //
        // `AssertUnwindSafe` is the honest annotation and not a shortcut: the
        // borrows crossing the boundary really can be left inconsistent by a
        // panic, which is exactly why `panicked` fuses the stream instead of
        // resuming it. The default panic hook still runs, so the operator's
        // own message and backtrace reach stderr before this returns.
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.inner.poll_next_unpin(cx)
        }));
        match polled {
            Ok(next) => {
                // ADR-0954's spill window, sampled once per poll and only for
                // a query whose disk manager is enabled. See
                // `SpillCounts::duration` for what the resulting figure is and
                // is not.
                self.sample_spill_window();
                match next {
                    Poll::Ready(Some(Err(err))) => {
                        Poll::Ready(Some(Err(self.map_execution_error(err))))
                    }
                    Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
                    Poll::Ready(None) => Poll::Ready(None),
                    Poll::Pending => Poll::Pending,
                }
            }
            Err(payload) => {
                self.panicked = true;
                // `payload.as_ref()`, not `&payload`: coercing the `Box`
                // itself to `&dyn Any` would downcast the box rather than
                // what it holds, and every arm would miss.
                let message = panic_message(payload.as_ref());
                Poll::Ready(Some(Err(SqlError::OperatorPanic(message))))
            }
        }
    }
}

/// The message carried by a caught panic, for the server-side log.
///
/// `panic!` payloads are `&'static str` for a literal and `String` for a
/// formatted message; anything else came from `panic_any` and has no text to
/// recover.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "panic with a non-string payload".to_string()
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
///
/// `pool` is the stream's own `MemoryPool` (issue #740): a `ResourcesExhausted`
/// reaching here can be a spill-capable operator's refusal (a `RepartitionExec`
/// exchange or an external sort, named by DataFusion's own message, with no
/// byte figures) rather than this pool's own `try_grow` text (which already
/// names its budget and figures). [`SqlError::resources_exhausted_reattributed`]
/// tells the two apart and rewrites only the former, from `pool`'s occupancy at
/// this moment; the caller does not need to classify the message itself.
///
/// `take_sql_error` already unwraps a `ResourcesExhausted` at any wrapper depth
/// into `Ok(SqlError::ResourcesExhausted(msg))` (its own doc comment above),
/// so the re-attribution runs after that recovery, on the recovered
/// `SqlError::ResourcesExhausted` itself, not in the `Err` branch below (which
/// a bare or wrapped `ResourcesExhausted` never reaches).
fn execution_error(err: DataFusionError, pool: &Arc<dyn MemoryPool>) -> SqlError {
    let sql = match take_sql_error(err) {
        Ok(sql) => sql,
        Err(other) => match other {
            DataFusionError::ResourcesExhausted(msg) => SqlError::ResourcesExhausted(msg),
            other => return SqlError::Execution(other.to_string()),
        },
    };
    match sql {
        SqlError::ResourcesExhausted(msg) => {
            let limit = match pool.memory_limit() {
                MemoryLimit::Finite(limit) => limit,
                MemoryLimit::Infinite | MemoryLimit::Unknown => usize::MAX,
            };
            SqlError::resources_exhausted_reattributed(&msg, pool.reserved(), limit)
        }
        other => other,
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

/// Collect every grouping node reachable in `plan` for ADR-0094 decision 1's
/// exact-typed classification: [`LogicalPlan::Aggregate`] nodes into
/// `aggregates`, and [`LogicalPlan::Distinct`] nodes into `distincts`. A
/// `SELECT DISTINCT` lowers to a `GROUP BY` on its distinct keys only in the
/// optimizer; the analyzer this walk runs over leaves it as a `Distinct` node,
/// so it is captured here and classified for its keys (the
/// `SELECT DISTINCT float_col` case). Sibling to [`collect_filter_predicates`],
/// with two reach rules:
///
/// - `plan.inputs()` recursion, for grouping nodes in the direct operator tree.
/// - a scan of every visited node's own `expressions()` for an embedded
///   subquery plan (`Expr::ScalarSubquery`/`InSubquery`/`Exists`), each of
///   which wraps a `LogicalPlan` that `plan.inputs()` never descends into. Every
///   embedded plan is walked with this same recursion, so a disqualifying
///   aggregate hidden inside a scalar subquery is found the same as one at the
///   top level.
///
/// Nodes are cloned out (both variants hold an `Arc`-backed input and their own
/// expression vectors, so a clone is cheap) rather than borrowed, because the
/// subquery plans live inside the owned `Vec<Expr>` that `expressions()` returns
/// and would not outlive the walk.
fn collect_aggregate_exprs(
    plan: &LogicalPlan,
    aggregates: &mut Vec<Aggregate>,
    distincts: &mut Vec<Distinct>,
) {
    match plan {
        LogicalPlan::Aggregate(aggregate) => aggregates.push(aggregate.clone()),
        LogicalPlan::Distinct(distinct) => distincts.push(distinct.clone()),
        _ => {}
    }
    for input in plan.inputs() {
        collect_aggregate_exprs(input, aggregates, distincts);
    }
    for expr in plan.expressions() {
        // Never errors: the closure only recurses and returns `Continue`.
        let _ = expr.apply(|node| {
            match node {
                Expr::ScalarSubquery(subquery) => {
                    collect_aggregate_exprs(&subquery.subquery, aggregates, distincts);
                }
                Expr::InSubquery(in_subquery) => {
                    collect_aggregate_exprs(&in_subquery.subquery.subquery, aggregates, distincts);
                }
                Expr::Exists(exists) => {
                    collect_aggregate_exprs(&exists.subquery.subquery, aggregates, distincts);
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        });
    }
}

/// Whether every grouping node in `plan` (analyzed/type-coerced) is
/// order/partition-independent (ADR-0094 decision 1). A single disqualifying
/// aggregate, GROUP BY key, or DISTINCT key anywhere, including inside a
/// subquery, makes the whole query not exact -- `repartition_aggregations` is
/// one session-wide knob, not a per-node choice.
fn plan_is_exact_typed(plan: &LogicalPlan) -> bool {
    let mut aggregates = Vec::new();
    let mut distincts = Vec::new();
    collect_aggregate_exprs(plan, &mut aggregates, &mut distincts);
    aggregates.iter().all(aggregate_node_is_exact) && distincts.iter().all(distinct_node_is_exact)
}

/// Whether `plan` (analyzed/type-coerced) may spill (ADR-0954).
///
/// This is a predicate over the plan's shape and its aggregate expressions'
/// resolved types, deliberately NOT an operator-name allowlist: what is at
/// stake is exactness, and "this operator can spill" says nothing about
/// whether spilling changes its answer. Spilling a grouped aggregation makes
/// DataFusion emit partial group state, sort it, write it, and re-merge it, so
/// the aggregate's folding order changes. An aggregate whose merge is exact
/// under any order is unaffected; one whose merge is order-dependent is not,
/// and this repo's exactness invariant is the thing the spill would trade
/// away.
///
/// A plan is eligible when ALL of the following hold:
///
/// - it contains at least one [`LogicalPlan::Aggregate`]. With no aggregate
///   there is nothing whose exactness this predicate has reasoned about, so
///   enabling spill could only benefit an operator it never classified.
/// - every node is one of the shapes below. This is an allowlist, so a
///   DataFusion release that adds a `LogicalPlan` variant makes plans using it
///   ineligible rather than silently spillable. `Sort` is deliberately outside
///   it: an external merge sort returns the same rows, but this ADR has no
///   proof its tie order equals the in-memory sort's, and row order is part of
///   an `ORDER BY` result. `Join` and `Window` are outside it for the same
///   reason, one level up: nothing here has classified their spill behavior.
/// - every aggregate expression is exactness-preserving under spill
///   ([`aggregate_expr_is_spill_exact`]) and no GROUP BY or DISTINCT key is a
///   float ([`aggregate_node_is_spill_exact`], [`distinct_node_is_exact`]).
///
/// Anything this predicate cannot classify -- an unresolvable expression type,
/// an aggregate that is not an `AggregateFunction`, an unknown plan node -- is
/// ineligible. Fail closed: the cost of a false negative is today's typed
/// refusal, and the cost of a false positive is a silently wrong answer.
fn plan_is_spill_eligible(plan: &LogicalPlan) -> bool {
    if !plan_nodes_are_spill_classifiable(plan) {
        return false;
    }
    let mut aggregates = Vec::new();
    let mut distincts = Vec::new();
    collect_aggregate_exprs(plan, &mut aggregates, &mut distincts);
    !aggregates.is_empty()
        && aggregates.iter().all(aggregate_node_is_spill_exact)
        && distincts.iter().all(distinct_node_is_exact)
}

/// Whether every node in `plan` is a shape [`plan_is_spill_eligible`] has
/// classified. Walks inputs and embedded subquery plans with the same reach
/// [`collect_aggregate_exprs`] uses, so a `Sort` hidden inside a scalar
/// subquery disqualifies the query exactly as a top-level one does.
fn plan_nodes_are_spill_classifiable(plan: &LogicalPlan) -> bool {
    let classifiable = matches!(
        plan,
        LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Values(_)
    );
    if !classifiable {
        return false;
    }
    if !plan
        .inputs()
        .iter()
        .all(|input| plan_nodes_are_spill_classifiable(input))
    {
        return false;
    }
    for expr in plan.expressions() {
        let mut ok = true;
        // Never errors: the closure only inspects and returns a recursion verb.
        let _ = expr.apply(|node| {
            let nested = match node {
                Expr::ScalarSubquery(subquery) => Some(&subquery.subquery),
                Expr::InSubquery(in_subquery) => Some(&in_subquery.subquery.subquery),
                Expr::Exists(exists) => Some(&exists.subquery.subquery),
                _ => None,
            };
            if let Some(nested) = nested
                && !plan_nodes_are_spill_classifiable(nested)
            {
                ok = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if !ok {
            return false;
        }
    }
    true
}

/// Classify one [`Aggregate`] node for spill (ADR-0954): every GROUP BY key
/// non-float, every aggregate expression exact under a changed folding order.
///
/// A float GROUP BY key disqualifies for the same reason it disqualifies
/// ADR-0094's repartition classification: `-0.0` and NaN payloads are
/// significant here, and nothing proves the sort DataFusion applies to the
/// spilled group state picks a merge-order-stable representative bit pattern.
fn aggregate_node_is_spill_exact(aggregate: &Aggregate) -> bool {
    let schema = aggregate.input.schema().as_ref();
    for key in &aggregate.group_expr {
        match key.get_type(schema) {
            Ok(ty) if crate::minmax::is_float(&ty) => return false,
            Ok(_) => {}
            // Fail closed: a key whose type will not resolve is not admitted.
            Err(_) => return false,
        }
    }
    aggregate
        .aggr_expr
        .iter()
        .all(|expr| aggregate_expr_is_spill_exact(expr, schema))
}

/// Whether one aggregate expression keeps its exact answer when the spill path
/// changes the order its partial states are folded in (ADR-0954):
///
/// - `count(...)` / `count(DISTINCT ...)`: eligible. Its accumulator is an
///   integer and its merge is integer addition (a distinct count merges sets),
///   neither of which depends on order, whatever the counted expression's type
///   is.
/// - `sum` over a resolved integer input: eligible. Integer addition is
///   associative, so partial sums merged in any order give the same total.
/// - `avg`/`mean` over a resolved `Int64` input: eligible. That is
///   `crate::avg`'s exact integer path, an `i128` numerator with checked
///   addition and an integer count (ADR-0825 decision 2); the analyzer coerces
///   every admitted integer width to `Int64`, so a resolved `Int64` argument is
///   the whole of that path.
///
/// Everything else is ineligible, including `sum` over a float (order-dependent
/// IEEE addition), `avg` over a float (the same fold), `min`/`max` of any type,
/// `sum` over a Decimal, and any expression that is not an aggregate function
/// call. `min`/`max` are excluded because ADR-0954's core cut enumerates three
/// eligible families and this predicate implements exactly those; admitting
/// more is a widening that needs its own evidence, and the cost of leaving them
/// out is today's typed refusal, not a wrong answer.
fn aggregate_expr_is_spill_exact(expr: &Expr, schema: &DFSchema) -> bool {
    // `aggr_expr` entries are either an `AggregateFunction` or an `Alias`
    // wrapping one; unwrap a single alias layer.
    let inner = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    let Expr::AggregateFunction(aggregate_function) = inner else {
        // Not an aggregate function where one is required: fail closed.
        return false;
    };
    let arg_type =
        |wanted: fn(&datafusion::arrow::datatypes::DataType) -> bool| match aggregate_function
            .params
            .args
            .first()
        {
            Some(arg) => match arg.get_type(schema) {
                Ok(ty) => wanted(&ty),
                Err(_) => false,
            },
            None => false,
        };
    match aggregate_function.func.name().to_ascii_lowercase().as_str() {
        "count" => true,
        "sum" => arg_type(is_exact_integer),
        "avg" | "mean" => {
            arg_type(|ty| matches!(ty, datafusion::arrow::datatypes::DataType::Int64))
        }
        _ => false,
    }
}

/// The integer types whose addition is exact and associative, so a partial sum
/// merged in any order is the same total. Deliberately narrower than "not a
/// float": `Decimal128`/`Decimal256` addition is exact too but can overflow
/// into a different error depending on where the split falls, and neither is
/// reachable on the v1 surface, so both fail closed here.
fn is_exact_integer(ty: &datafusion::arrow::datatypes::DataType) -> bool {
    use datafusion::arrow::datatypes::DataType;
    matches!(
        ty,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

/// Whether `plan` (analyzed) has any [`LogicalPlan::Aggregate`] node (issue
/// #888). Every ADR-0850 metadata-only path replaces an `AggregateExec`, so a
/// plan with no aggregate can never consume column statistics. Reuses
/// [`collect_aggregate_exprs`]'s reach (inputs plus embedded subquery plans),
/// so it fails open: an aggregate anywhere counts, even one the metadata rule
/// would not ultimately match.
fn plan_has_aggregate(plan: &LogicalPlan) -> bool {
    let mut aggregates = Vec::new();
    let mut distincts = Vec::new();
    collect_aggregate_exprs(plan, &mut aggregates, &mut distincts);
    !aggregates.is_empty()
}

/// Whether `plan` references at least one of `declared`'s columns by name
/// (issue #888). Every ADR-0850 metadata-only path names a declared column: a
/// `MIN`/`MAX` argument (q07), a `GROUP BY` key (q08), or the `col <> lit`
/// filter column (q02). A plan naming none can never consume column
/// statistics -- a predicate-free `COUNT(*)`, for instance, is answered from
/// `sample_count` with no column stats at all. Recurses inputs the same way
/// [`collect_filter_predicates`] does; matching by unqualified column name is
/// intentionally broad (fail open), since a false negative would turn ADR-0850
/// off for a plan that could use it.
fn plan_references_declared(plan: &LogicalPlan, declared: &[DeclaredColumn]) -> bool {
    let names: HashSet<&str> = declared.iter().map(|d| d.key.as_str()).collect();
    plan_references_names(plan, &names)
}

fn plan_references_names(plan: &LogicalPlan, names: &HashSet<&str>) -> bool {
    for expr in plan.expressions() {
        let mut found = false;
        // Never errors: the closure only inspects and returns a recursion verb.
        let _ = expr.apply(|node| {
            if let Expr::Column(column) = node
                && names.contains(column.name.as_str())
            {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            // A subquery's plan hangs off the expression, not off
            // `plan.inputs()`, so the input recursion below never reaches it.
            // `collect_aggregate_exprs` descends these three for the same
            // reason; without the matching descent here the two walks disagree,
            // and `SELECT (SELECT MIN(status) FROM logs)` would be judged to
            // name no declared column while being judged to hold an aggregate.
            let nested = match node {
                Expr::ScalarSubquery(subquery) => Some(&subquery.subquery),
                Expr::InSubquery(in_subquery) => Some(&in_subquery.subquery.subquery),
                Expr::Exists(exists) => Some(&exists.subquery.subquery),
                _ => None,
            };
            if let Some(plan) = nested
                && plan_references_names(plan, names)
            {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        if found {
            return true;
        }
    }
    plan.inputs()
        .iter()
        .any(|input| plan_references_names(input, names))
}

/// Classify a DISTINCT node's implicit group keys (ADR-0094 decision 1): a
/// float key disqualifies exactly as a float `GROUP BY` key does, since a
/// DISTINCT lowers to a `GROUP BY` on these keys. Plain `DISTINCT` keys on
/// every output column; `DISTINCT ON` keys on its `on_expr` list.
fn distinct_node_is_exact(distinct: &Distinct) -> bool {
    match distinct {
        Distinct::All(input) => input
            .schema()
            .fields()
            .iter()
            .all(|field| !crate::minmax::is_float(field.data_type())),
        Distinct::On(distinct_on) => {
            let schema = distinct_on.input.schema().as_ref();
            distinct_on
                .on_expr
                .iter()
                .all(|key| match key.get_type(schema) {
                    Ok(ty) => !crate::minmax::is_float(&ty),
                    Err(_) => false,
                })
        }
    }
}

/// Classify one [`Aggregate`] node (ADR-0094 decision 1): every GROUP BY key
/// must be non-float, and every aggregate expression must be exact-eligible.
/// Types are resolved against the node's *input* schema, over which both group
/// and aggregate-argument expressions are evaluated.
fn aggregate_node_is_exact(aggregate: &Aggregate) -> bool {
    let schema = aggregate.input.schema().as_ref();
    // A float GROUP BY key disqualifies the query even when every aggregate is
    // exact (`-0.0`/NaN bit-significance, ADR-0013): this ADR has no proof
    // DataFusion picks a merge-order-stable representative bit pattern for a
    // float group key. This also covers `SELECT DISTINCT float_col`, a
    // zero-aggregate `Aggregate` whose group key is the DISTINCT column.
    for key in &aggregate.group_expr {
        match key.get_type(schema) {
            Ok(ty) if crate::minmax::is_float(&ty) => return false,
            Ok(_) => {}
            // Fail closed: a key whose type will not resolve is not admitted.
            Err(_) => return false,
        }
    }
    aggregate
        .aggr_expr
        .iter()
        .all(|expr| aggregate_expr_is_exact(expr, schema))
}

/// Whether one aggregate expression is exact-eligible (ADR-0094 decision 1):
///
/// - `count(...)`/`count(DISTINCT ...)`: always exact, any input type.
/// - `sum`/`min`/`max` over a resolved non-float input: exact.
/// - `avg`/`mean` over any input, `sum`/`min`/`max` over a float input, and
///   anything unexpected: never exact (fail closed).
fn aggregate_expr_is_exact(expr: &Expr, schema: &DFSchema) -> bool {
    // `aggr_expr` entries are either an `AggregateFunction` or an `Alias`
    // wrapping one; unwrap a single alias layer.
    let inner = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    let Expr::AggregateFunction(aggregate_function) = inner else {
        // Not an aggregate function where one is required: fail closed.
        return false;
    };
    match aggregate_function.func.name().to_ascii_lowercase().as_str() {
        // Counting presence is order-independent regardless of the counted
        // value's type; a partial-count merge is exact integer addition, and
        // distinctness is a per-row property merge order cannot change.
        "count" => true,
        // Exact only over a resolved non-float input.
        "sum" | "min" | "max" => match aggregate_function.params.args.first() {
            Some(arg) => match arg.get_type(schema) {
                Ok(ty) => !crate::minmax::is_float(&ty),
                Err(_) => false,
            },
            // A sum/min/max with no argument should not occur; fail closed.
            None => false,
        },
        // `avg`/`mean` over a resolved integer input runs exact i128
        // accumulation with checked addition (crate::avg, ADR-0825 decision
        // 2): the analyzer coerces the admitted integer types (Int8-Int64,
        // UInt8-UInt32) to Int64, so a resolved Int64 argument's partial sum
        // is exact regardless of partitioning or merge order. A Float64
        // argument still runs the plain IEEE f64 fold (ADR-0094's original
        // amendment for issue #771) and stays never exact: that partial sum
        // is order-dependent. Any other resolved type (Decimal, Duration) is
        // unreachable on the v1 `samples` surface and fails closed.
        "avg" | "mean" => match aggregate_function.params.args.first() {
            Some(arg) => matches!(
                arg.get_type(schema),
                Ok(datafusion::arrow::datatypes::DataType::Int64)
            ),
            None => false,
        },
        // Any other name is outside the admitted aggregate set and fails
        // closed.
        _ => false,
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
        // A message without MSG_SPILL_DISABLED_MARKER is not DataFusion's
        // spill-refusal shape (issue #740); `execution_error` must pass it
        // through unchanged rather than substituting the pool's own figures,
        // so the pool here is a throwaway the reattribution never reads.
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let df = DataFusionError::ResourcesExhausted("needs 40 bytes, limit 8".to_string());
        let err = execution_error(df, &pool);
        assert!(matches!(err, SqlError::ResourcesExhausted(_)));
        assert!(err.client_message().contains("limit 8"));
    }

    /// Issue #740, finding 1: a `RepartitionExec` output channel's spill
    /// refusal (DataFusion 54's `"... SpillPool (DiskManager is disabled)"`,
    /// carrying no byte figures) is re-attributed from the stream's own pool
    /// occupancy at the moment of refusal, not passed through with the
    /// exchange's name and no figures. Exercises the real seam
    /// [`execution_error`] runs at (issue #740's named caller,
    /// [`PinnedStream::poll_next`]'s `batch.map_err`), with a real
    /// `TenantDelegatingPool` reserved to a known figure rather than a bare
    /// `resources_exhausted_reattributed` unit call.
    #[test]
    fn a_spill_refusal_from_the_streams_own_pool_names_its_occupancy() {
        use datafusion::execution::memory_pool::MemoryConsumer;

        let tenant = TenantMemoryAccountant::new(1 << 30);
        let pool: Arc<dyn MemoryPool> = Arc::new(crate::memory::TenantDelegatingPool::new(
            8000,
            tenant,
            CeilingBreach::new(),
            QueryAccounting::new(),
        ));
        let consumer = MemoryConsumer::new("GroupedHashAggregateStream[0]").register(&pool);
        consumer.try_grow(6144).expect("within the query ceiling");

        let df = DataFusionError::ResourcesExhausted(
            "Memory Exhausted while SpillPool (DiskManager is disabled)".to_string(),
        );
        let err = execution_error(df, &pool);
        let SqlError::ResourcesExhausted(message) = &err else {
            panic!("still a typed ResourcesExhausted; got {err:?}");
        };
        assert!(
            message.contains("6144") && message.contains("8000"),
            "message must name the pool's reserved bytes and limit; got {message:?}"
        );
        assert!(
            !message.contains("SpillPool"),
            "the exchange's own name must not survive re-attribution; got {message:?}"
        );
    }

    /// ADR-0094 report sanity check: the analyzer step
    /// ([`SqlExecutor::analyzed_classification_plan`]) actually resolves `avg`'s
    /// argument to `Float64` for an integer input -- the whole reason
    /// classification runs DataFusion's analyzer and not `create_logical_plan`
    /// alone. Proven by inspecting the coerced argument's type, not just that the
    /// code compiles.
    #[tokio::test]
    async fn analyzer_preserves_avg_integer_argument_as_int64() {
        use datafusion::arrow::datatypes::DataType;

        let exec = eviction_test_executor();
        let plan = exec
            .analyzed_classification_plan(
                TenantHash([9u8; 16]),
                "SELECT avg(CAST(value AS BIGINT)) AS a FROM samples",
                &[],
            )
            .await
            .expect("throwaway avg plan analyzes");

        let mut aggregates = Vec::new();
        let mut distincts = Vec::new();
        collect_aggregate_exprs(&plan, &mut aggregates, &mut distincts);
        let aggregate = aggregates.first().expect("one aggregate node");
        let schema = aggregate.input.schema().as_ref();
        let expr = aggregate.aggr_expr.first().expect("one aggregate expr");
        let inner = match expr {
            Expr::Alias(alias) => alias.expr.as_ref(),
            other => other,
        };
        let Expr::AggregateFunction(af) = inner else {
            panic!("aggregate expression is not an AggregateFunction: {inner:?}");
        };
        assert_eq!(af.func.name().to_ascii_lowercase(), "avg");
        let arg_type = af
            .params
            .args
            .first()
            .expect("avg has an argument")
            .get_type(schema)
            .expect("argument type resolves");
        assert_eq!(
            arg_type,
            DataType::Int64,
            "an admitted integer argument to avg must stay Int64, not widen to Float64 (ADR-0825 decision 2)"
        );
        // And avg over that resolved Int64 argument is exact.
        assert!(
            aggregate_expr_is_exact(expr, schema),
            "avg over a resolved integer input is exact-eligible (ADR-0094 amendment, ADR-0825 decision 3)"
        );
    }

    #[tokio::test]
    async fn analyzer_still_coerces_avg_float_argument_to_float64() {
        use datafusion::arrow::datatypes::DataType;

        let exec = eviction_test_executor();
        let plan = exec
            .analyzed_classification_plan(
                TenantHash([9u8; 16]),
                "SELECT avg(value) AS a FROM samples",
                &[],
            )
            .await
            .expect("throwaway avg plan analyzes");

        let mut aggregates = Vec::new();
        let mut distincts = Vec::new();
        collect_aggregate_exprs(&plan, &mut aggregates, &mut distincts);
        let aggregate = aggregates.first().expect("one aggregate node");
        let schema = aggregate.input.schema().as_ref();
        let expr = aggregate.aggr_expr.first().expect("one aggregate expr");
        let inner = match expr {
            Expr::Alias(alias) => alias.expr.as_ref(),
            other => other,
        };
        let Expr::AggregateFunction(af) = inner else {
            panic!("aggregate expression is not an AggregateFunction: {inner:?}");
        };
        let arg_type = af
            .params
            .args
            .first()
            .expect("avg has an argument")
            .get_type(schema)
            .expect("argument type resolves");
        assert_eq!(arg_type, DataType::Float64);
        // Float avg is never exact.
        assert!(
            !aggregate_expr_is_exact(expr, schema),
            "avg over a Float64 input is never exact-eligible (ADR-0094 decision 1)"
        );
    }

    /// ADR-0094 decision 1: the classifier admits exactly the exact-typed
    /// shapes and rejects the rest, exercised through the real throwaway-session
    /// plan+analyze path (not the pure helpers in isolation).
    #[tokio::test]
    async fn classify_exact_typed_admits_and_rejects_per_adr_0094() {
        let exec = eviction_test_executor();
        let t = TenantHash([3u8; 16]);

        // Integer sum + string group key + count: exact.
        assert!(
            exec.classify_exact_typed(
                t,
                "SELECT label(labels,'__name__') AS m, \
                 sum(CAST(value AS BIGINT)) AS s, count(*) AS c FROM samples GROUP BY m",
                &[],
            )
            .await
        );
        // count(DISTINCT float): count is always exact.
        assert!(
            exec.classify_exact_typed(t, "SELECT count(DISTINCT value) FROM samples", &[])
                .await
        );
        // No aggregate at all: trivially exact.
        assert!(
            exec.classify_exact_typed(t, "SELECT ts, value FROM samples", &[])
                .await
        );
        // Float sum: not exact.
        assert!(
            !exec
                .classify_exact_typed(t, "SELECT sum(value) FROM samples", &[])
                .await
        );
        // Float GROUP BY key (aggregate itself exact): not exact.
        assert!(
            !exec
                .classify_exact_typed(t, "SELECT value, count(*) FROM samples GROUP BY value", &[])
                .await
        );
        // SELECT DISTINCT on a float column (zero-aggregate float group key):
        // not exact.
        assert!(
            !exec
                .classify_exact_typed(t, "SELECT DISTINCT value FROM samples", &[])
                .await
        );
        // avg over a resolved integer input: exact (ADR-0825 decision 3).
        assert!(
            exec.classify_exact_typed(t, "SELECT avg(CAST(value AS BIGINT)) FROM samples", &[])
                .await
        );
        // avg over a float input: still not exact.
        assert!(
            !exec
                .classify_exact_typed(t, "SELECT avg(value) FROM samples", &[])
                .await
        );
        // A disqualifying float avg hidden inside a scalar subquery: not exact,
        // proving the expression-embedded subquery walk (decision 1).
        assert!(
            !exec
                .classify_exact_typed(
                    t,
                    "SELECT count(*) FROM samples \
                     WHERE value > (SELECT avg(value) FROM samples)",
                    &[],
                )
                .await
        );
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
