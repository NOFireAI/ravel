//! The real, `TenantConfig`-backed [`DeclaredColumnSource`] for the `logs` SQL
//! table (ADR-0090 decision 2), a cache-aside overlay over the CLI-derived
//! declaration.
//!
//! `ravel-sql` owns the query-side contract ([`DeclaredColumnSource`],
//! [`DeclaredColumn`]) and ships only a constant `StaticDeclaredColumns` test
//! implementation, so nothing in this deployment resolved a durable
//! `TenantConfig.typed_attr_columns` override before this module: every query
//! ran with zero declared columns whatever the durable record said.
//! [`TenantConfigDeclaredColumns`] is that reader.
//!
//! It wraps the CLI-derived [`TypedAttrColumnConfig`] base unchanged plus a
//! per-tenant cache, mirroring `ravel_ingest::IndexedFieldsOverlay`
//! (ADR-0079, `crates/ravel-ingest/src/indexed_fields.rs`) shape for shape: a
//! synchronous fast path ([`TenantConfigDeclaredColumns::columns_cached`])
//! serving a cached entry inside a staleness horizon, an async
//! caller-driven refresh on a stale or missing entry
//! ([`TenantConfigDeclaredColumns::refresh`]), a failed-read backoff so a
//! degraded store costs at most one GET per tenant per window, and idle
//! eviction of a cache entry no query has touched. Unlike the ingest overlay,
//! the two steps are joined inside one async trait method here, because the
//! trait `ravel-sql` defines is async and the caller (`SqlExecutor`'s
//! once-per-plan resolution) has nowhere to put a two-step dance.
//!
//! # Present-but-empty durable override
//!
//! A durable `typed_attr_columns = Some(list)` replaces the CLI base for that
//! tenant outright, INCLUDING `Some(vec![])`: a present-but-empty list is a
//! deliberate "this tenant has zero declared columns", distinct from "no
//! override yet". Only `None` (no record at all, or a record without the
//! field) falls through to the CLI base. This mirrors `IndexedFieldsOverlay`'s
//! existing semantics for the same durable-record shape exactly -- see
//! `indexed_fields.rs:249` (`Some(list) => self.install_fresh(tenant, list,
//! now_ns)`, taken for a possibly-empty list, with `None` the only arm that
//! reads the base at `indexed_fields.rs:252-255`), its doc comment at
//! `indexed_fields.rs:211-215` ("a durable `indexed_fields = Some(list)` (a
//! possibly-empty list, a valid explicit opt-out) replaces the base's resolved
//! list outright"), and its deliverable test
//! `empty_override_indexes_nothing_absent_leaves_base` at
//! `indexed_fields.rs:428`. ADR-0090 decision 2 states the same rule for this
//! feature ("a durable `Some([])` is deliberately zero declared columns for
//! that tenant, distinct from 'no override yet'"), and
//! `ravel-catalog`'s own `TenantConfig::typed_attr_columns` doc records the
//! three-way distinction on the durable side.
//!
//! # Failure discipline, and why it is NOT ADR-0079's argument
//!
//! A failed `TenantConfig` read, a missing record, an absent field, or a
//! durable list that fails validation never blocks and never fails a query:
//! the overlay serves the last successfully-resolved declaration for that
//! tenant, or the CLI base if the tenant has never resolved. ADR-0079 could
//! justify serving stale trivially ("the query path never consults it"); that
//! argument does not transfer, because this overlay's output *is* part of what
//! a query result is computed from. A stale entry can mean a query fails with
//! an unknown-column error, or that two replicas answer the same query with
//! different schemas, for up to the staleness horizon (unboundedly longer
//! under a sustained read outage). The trade is deliberate and is ADR-0090
//! decision 2's: failing a query closed on an unreadable config object would
//! turn a config-store blip into a total query outage, while a stale schema
//! degrades one tenant's newest declaration. What makes it safe to ship is
//! that it is never silent: every query served from a stale entry, a
//! backoff-suppressed read, a failed read, or a malformed durable list
//! increments [`crate::typed_attr_metrics`]'s
//! `ravel_typed_attr_columns_stale_fallback_total`, mirroring both ADR-0079's
//! own observability requirement and `active_set`'s grace-extended-stale
//! counter, so a permanently unreadable override is visible on `/metrics`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use ravel_catalog::{
    DeclaredColumnType, DeclaredTypedColumn, read_config_values, validate_typed_attr_columns,
};
use ravel_ingest::DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS;
use ravel_object_store::ObjectStoreBackend;
use ravel_sql::{DeclaredColumn, DeclaredColumnSource, DeclaredType};
use ravel_types::TenantHash;

use crate::typed_attr_config::TypedAttrColumnConfig;

/// The staleness horizon for a resolved declaration: a cached entry younger
/// than this is served by the synchronous fast path with no durable read.
///
/// 60s, ADR-0090 decision 2's explicit choice, equal to
/// `IndexedFieldsOverlay`'s own horizon
/// ([`DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS`]) but stated here rather than
/// inherited implicitly: this horizon is also the window in which two query
/// replicas can disagree about a tenant's `logs` schema after an operator
/// writes a new declaration, which is a stronger consequence than the ingest
/// overlay's (a wrong pruning hint), so the number is named at the query
/// path's own site.
pub const DEFAULT_DECLARED_COLUMNS_HORIZON_NS: i64 = DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS;

/// Backoff after a failed durable read before this tenant's `TenantConfig` GET
/// is attempted again. One second, the same magnitude and reasoning as
/// `ravel_ingest::DEFAULT_INDEXED_FIELDS_REREAD_BACKOFF_NS` and
/// `lifecycle_refresh.rs`'s `DEFAULT_ON_MISS_INTERVAL_NS`: an order of
/// magnitude below the staleness horizon, so a degraded object store adds at
/// most one failed GET per tenant per second rather than one per query.
pub const DEFAULT_DECLARED_COLUMNS_BACKOFF_NS: i64 = 1_000_000_000;

/// The backoff must stay well inside the horizon, or a single failed read would
/// park a tenant on the base declaration for a whole horizon at a time.
const _: () = assert!(DEFAULT_DECLARED_COLUMNS_BACKOFF_NS < DEFAULT_DECLARED_COLUMNS_HORIZON_NS);

/// Sentinel `refreshed_at_ns`/`attempted_at_ns` meaning "no such event yet".
/// `now_ns.saturating_sub(i64::MIN)` saturates to `i64::MAX`, so a sentinel
/// entry is neither fresh nor in backoff without a separate branch (the same
/// trick `indexed_fields.rs`'s `NEVER` uses).
const NEVER: i64 = i64::MIN;

/// One tenant's cached resolved declaration plus the three timestamps the fast
/// path, the backoff, and idle eviction key off, each with the same job as its
/// `indexed_fields.rs` counterpart:
/// - `refreshed_at_ns` advances only on a successful durable resolution; the
///   fast path compares it against the horizon. [`NEVER`] for an entry that
///   only ever recorded a failed read, so the fast path never trusts it.
/// - `attempted_at_ns` stamps the last *failed* read, and is what the backoff
///   compares. Reset to [`NEVER`] on success so a recovered tenant is not held
///   in backoff.
/// - `last_touched_ns` advances on *any* access and is the only stamp idle
///   eviction reads (a base-only failure entry has `refreshed_at_ns == NEVER`,
///   so eviction cannot key off that without reaping live entries).
#[derive(Debug, Clone)]
struct CacheEntry {
    columns: Vec<DeclaredColumn>,
    refreshed_at_ns: i64,
    attempted_at_ns: i64,
    last_touched_ns: i64,
}

/// Per-tenant cache behind a `Mutex` (a lock, not `ArcSwap`: the read side
/// already tolerates one, and a lock lets the miss path install a freshly-read
/// value without a lost-update race).
#[derive(Default)]
struct Inner {
    cache: HashMap<TenantHash, CacheEntry>,
}

/// How a resolution was reached: from a fresh durable read (or a genuine
/// absence, itself a valid state), or from a stale/backoff/failed-read/
/// validation-failure fallback. The stale-fallback metric counts exactly the
/// [`RefreshOutcome::Fallback`] arm, mirroring how `run_flush` counts
/// `IndexedFieldsOverlay`'s.
enum RefreshOutcome {
    Fresh(Vec<DeclaredColumn>),
    Fallback(Vec<DeclaredColumn>),
}

impl RefreshOutcome {
    fn into_columns(self) -> Vec<DeclaredColumn> {
        match self {
            RefreshOutcome::Fresh(columns) | RefreshOutcome::Fallback(columns) => columns,
        }
    }

    fn is_fallback(&self) -> bool {
        matches!(self, RefreshOutcome::Fallback(_))
    }
}

/// Map one durable/CLI declared column onto the query-side
/// [`DeclaredColumn`] `ravel-sql` plans against. The two types carry the same
/// information; they are distinct because `ravel-catalog` must not depend on
/// `ravel-sql` (nor the reverse), so this crate, which depends on both, owns
/// the adapter.
fn to_query_column(column: &DeclaredTypedColumn) -> DeclaredColumn {
    DeclaredColumn::new(
        column.key.clone(),
        match column.ty {
            DeclaredColumnType::Str => DeclaredType::Str,
            DeclaredColumnType::I64 => DeclaredType::I64,
            DeclaredColumnType::Bool => DeclaredType::Bool,
            DeclaredColumnType::Bytes => DeclaredType::Bytes,
        },
    )
}

/// Map a whole declaration, preserving order verbatim (ADR-0090 decision 3:
/// declaration order is the schema-append order).
fn to_query_columns(columns: &[DeclaredTypedColumn]) -> Vec<DeclaredColumn> {
    columns.iter().map(to_query_column).collect()
}

/// The cache-aside [`DeclaredColumnSource`] a query-serving process installs on
/// its `SqlExecutor` (ADR-0090 decision 2). One instance per process, shared by
/// `Arc` across the HTTP and Flight SQL surfaces, because both resolve through
/// the same executor.
pub struct TenantConfigDeclaredColumns {
    /// The CLI-derived declaration, resolved unchanged for a tenant with no
    /// durable override (or whose durable read has never yet succeeded).
    base: TypedAttrColumnConfig,
    /// The store the tenant config records live in. The same store the
    /// executor reads segments from.
    store: Arc<dyn ObjectStoreBackend>,
    inner: Mutex<Inner>,
    horizon_ns: i64,
    backoff_ns: i64,
    /// This instance's own stale-fallback count, incremented alongside the
    /// process-global [`crate::typed_attr_metrics`] counter `/metrics` renders.
    /// Both exist on purpose: the process-global is the operator-facing metric
    /// (one source, no per-instance labels, the
    /// [`crate::query_postings_metrics`] convention), and this one is what a
    /// test can assert an exact delta on, since several tests in one binary
    /// share the process-global and would race on it.
    stale_fallbacks: std::sync::atomic::AtomicU64,
}

impl TenantConfigDeclaredColumns {
    /// Wrap the CLI-derived `base` with the production horizon and backoff
    /// ([`DEFAULT_DECLARED_COLUMNS_HORIZON_NS`],
    /// [`DEFAULT_DECLARED_COLUMNS_BACKOFF_NS`]).
    pub fn new(base: TypedAttrColumnConfig, store: Arc<dyn ObjectStoreBackend>) -> Self {
        Self::with_intervals(
            base,
            store,
            DEFAULT_DECLARED_COLUMNS_HORIZON_NS,
            DEFAULT_DECLARED_COLUMNS_BACKOFF_NS,
        )
    }

    /// Wrap `base` with an explicit horizon and backoff. Both are clamped to at
    /// least 1 ns, so a zero (mis)configuration can neither make every entry
    /// instantly stale nor disable the backoff. For tests that need a small,
    /// deterministic horizon (the shape
    /// `IndexedFieldsOverlay::with_intervals` exists for); production uses
    /// [`TenantConfigDeclaredColumns::new`].
    pub fn with_intervals(
        base: TypedAttrColumnConfig,
        store: Arc<dyn ObjectStoreBackend>,
        horizon_ns: i64,
        backoff_ns: i64,
    ) -> Self {
        TenantConfigDeclaredColumns {
            base,
            store,
            inner: Mutex::new(Inner::default()),
            horizon_ns: horizon_ns.max(1),
            backoff_ns: backoff_ns.max(1),
            stale_fallbacks: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Lock the inner state, recovering a poisoned guard rather than
    /// propagating a panic: the cache is plain per-tenant state with no
    /// cross-entry invariant a panic mid-update could half-break, and declared
    /// column resolution must never take a query down.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The CLI-only resolution for a tenant, in query-side form.
    fn base_columns(&self, tenant: &TenantHash) -> Vec<DeclaredColumn> {
        to_query_columns(self.base.columns_for(tenant))
    }

    /// Synchronous fast path: the cached declaration for `tenant` if a
    /// successful refresh is still inside the staleness horizon
    /// (`now_ns - refreshed_at_ns <= horizon`, the same inclusive-fresh
    /// boundary `IndexedFieldsOverlay::fields_for_cached` uses), else `None` so
    /// the caller drives [`TenantConfigDeclaredColumns::refresh`]. A hit stamps
    /// `last_touched_ns` so idle eviction never reaps an entry still serving
    /// queries. `now_ns` is the query's injected timestamp; this type reads no
    /// clock.
    fn columns_cached(&self, tenant: &TenantHash, now_ns: i64) -> Option<Vec<DeclaredColumn>> {
        let mut inner = self.lock();
        let entry = inner.cache.get_mut(tenant)?;
        if now_ns.saturating_sub(entry.refreshed_at_ns) <= self.horizon_ns {
            entry.last_touched_ns = now_ns;
            Some(entry.columns.clone())
        } else {
            None
        }
    }

    /// Async refresh on a miss or a stale entry: read this one tenant's
    /// `TenantConfig`, validate a present durable declaration, overlay it onto
    /// the base, install the result, and return it. Reads the store at most
    /// once per call, and not at all while a prior failed read for this tenant
    /// is still inside the backoff window (it serves the cached value instead).
    /// A single plain GET, never retried here, so a degraded store costs a
    /// query one round trip at most.
    ///
    /// Overlay semantics: a durable `typed_attr_columns = Some(list)` (a
    /// possibly-empty list, a valid explicit "no declared columns") replaces
    /// the base outright once validated; `None` (no record, or a record without
    /// the field) leaves the base's own `columns_for(tenant)` resolution
    /// unchanged. A list that fails validation is treated exactly like a failed
    /// read -- never applied, because a malformed declaration is how a query
    /// resolves a column to a type nobody declared.
    async fn refresh(&self, tenant: &TenantHash, now_ns: i64) -> RefreshOutcome {
        // Backoff short-circuit: a recent failed read means the config store is
        // degraded; serve the cached value without hitting it again this window.
        {
            let mut inner = self.lock();
            if let Some(entry) = inner.cache.get_mut(tenant)
                && now_ns.saturating_sub(entry.attempted_at_ns) < self.backoff_ns
            {
                entry.last_touched_ns = now_ns;
                return RefreshOutcome::Fallback(entry.columns.clone());
            }
        }

        match read_config_values(self.store.as_ref(), tenant).await {
            Ok(config) => match config.and_then(|c| c.typed_attr_columns) {
                // A present durable declaration: validate on the same rules the
                // CLI flags and any durable write are held to. A malformed
                // record is treated exactly like a failed read.
                Some(list) if validate_typed_attr_columns(&list).is_err() => {
                    self.serve_on_failure(tenant, now_ns)
                }
                Some(list) => self.install_fresh(tenant, to_query_columns(&list), now_ns),
                // No override: the base's own tenant-override-or-default
                // resolution stands unchanged.
                None => {
                    let base = self.base_columns(tenant);
                    self.install_fresh(tenant, base, now_ns)
                }
            },
            Err(_) => self.serve_on_failure(tenant, now_ns),
        }
    }

    /// Install a freshly-resolved declaration, stamping the refresh time and
    /// clearing any pending failed-read backoff.
    fn install_fresh(
        &self,
        tenant: &TenantHash,
        columns: Vec<DeclaredColumn>,
        now_ns: i64,
    ) -> RefreshOutcome {
        let mut inner = self.lock();
        inner.cache.insert(
            *tenant,
            CacheEntry {
                columns: columns.clone(),
                refreshed_at_ns: now_ns,
                attempted_at_ns: NEVER,
                last_touched_ns: now_ns,
            },
        );
        RefreshOutcome::Fresh(columns)
    }

    /// The failed-read/validation-failure path: serve the last successfully
    /// resolved declaration for this tenant if one exists, else the CLI base,
    /// and stamp `attempted_at_ns` so the backoff bites. Never fails closed and
    /// never has a horizon cutoff. A base-captured entry keeps
    /// `refreshed_at_ns == NEVER`, so the fast path never trusts it and every
    /// query re-enters `refresh` (where the backoff makes the re-GET cheap)
    /// until a read finally succeeds.
    fn serve_on_failure(&self, tenant: &TenantHash, now_ns: i64) -> RefreshOutcome {
        let mut inner = self.lock();
        if let Some(entry) = inner.cache.get_mut(tenant)
            && entry.refreshed_at_ns != NEVER
        {
            entry.attempted_at_ns = now_ns;
            entry.last_touched_ns = now_ns;
            return RefreshOutcome::Fallback(entry.columns.clone());
        }
        let base = self.base_columns(tenant);
        inner.cache.insert(
            *tenant,
            CacheEntry {
                columns: base.clone(),
                refreshed_at_ns: NEVER,
                attempted_at_ns: now_ns,
                last_touched_ns: now_ns,
            },
        );
        RefreshOutcome::Fallback(base)
    }

    /// Evict every cached entry last touched before `now_ns - ttl_ns`
    /// (ADR-0069 decision 2's idle-tenant sweep, the same sweep
    /// `IndexedFieldsOverlay::evict_idle` feeds). Returns the number dropped.
    /// An evicted entry is re-derivable: the tenant's next query is a miss that
    /// re-reads its config. `now_ns`/`ttl_ns` are injected, so eviction is
    /// deterministic under test.
    pub fn evict_idle(&self, now_ns: i64, ttl_ns: i64) -> usize {
        let mut inner = self.lock();
        let before = inner.cache.len();
        inner
            .cache
            .retain(|_, entry| now_ns.saturating_sub(entry.last_touched_ns) <= ttl_ns);
        before - inner.cache.len()
    }

    /// The number of tenants currently cached, for tests and introspection.
    pub fn cached_tenants(&self) -> usize {
        self.lock().cache.len()
    }

    /// This instance's stale-fallback count: resolutions served from a stale
    /// cache entry, a backoff-suppressed read, a failed read, or a malformed
    /// durable declaration. The same events the process-global
    /// `ravel_typed_attr_columns_stale_fallback_total` counts.
    pub fn stale_fallbacks(&self) -> u64 {
        self.stale_fallbacks
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[async_trait]
impl DeclaredColumnSource for TenantConfigDeclaredColumns {
    /// Resolve `tenant`'s declaration as of `now_ns`: the synchronous fast path
    /// when a fresh entry exists, else one durable read. Every resolution that
    /// falls back (a backoff-suppressed read, a failed read, or a malformed
    /// durable list) counts
    /// `ravel_typed_attr_columns_stale_fallback_total` before returning, so a
    /// permanently unreadable override shows up on `/metrics` instead of
    /// silently pinning a tenant to the CLI base.
    async fn declared_columns(&self, tenant: TenantHash, now_ns: i64) -> Vec<DeclaredColumn> {
        if let Some(columns) = self.columns_cached(&tenant, now_ns) {
            return columns;
        }
        let outcome = self.refresh(&tenant, now_ns).await;
        if outcome.is_fallback() {
            self.stale_fallbacks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            crate::typed_attr_metrics::record_stale_fallback();
        }
        outcome.into_columns()
    }
}

impl crate::idle_tenant_state::IdleTenantEvictor for TenantConfigDeclaredColumns {
    fn evict_idle(&self, now_ns: i64, ttl_ns: i64) -> usize {
        TenantConfigDeclaredColumns::evict_idle(self, now_ns, ttl_ns)
    }
    fn label(&self) -> &'static str {
        "declared-typed-attr-columns"
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::typed_attr_config::{TypedAttrColumnConfig, TypedAttrColumnPolicy};
    use prost::Message;
    use ravel_catalog::{TenantConfig, TenantLifecycleState, config_key, set_tenant_config};
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault, Sequence,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_types::TenantId;

    fn col(key: &str, ty: DeclaredColumnType) -> DeclaredTypedColumn {
        DeclaredTypedColumn {
            key: key.to_string(),
            ty,
        }
    }

    /// A CLI base declaring one column for every tenant, so a test can tell
    /// "durable override to empty" (declare nothing) apart from "no durable
    /// override" (fall through to this declaration).
    fn base_declaring(key: &str) -> TypedAttrColumnConfig {
        TypedAttrColumnConfig::from_policy(TypedAttrColumnPolicy {
            default: vec![col(key, DeclaredColumnType::I64)],
            tenants: Vec::new(),
        })
        .expect("valid base declaration")
    }

    /// A tiny horizon and backoff so the staleness/backoff boundaries are easy
    /// to straddle with an injected `now_ns`.
    fn overlay_over(
        base: TypedAttrColumnConfig,
        store: Arc<dyn ObjectStoreBackend>,
    ) -> TenantConfigDeclaredColumns {
        TenantConfigDeclaredColumns::with_intervals(base, store, 1_000, 1_000)
    }

    async fn write_override(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        typed_attr_columns: Option<Vec<DeclaredTypedColumn>>,
        now_ns: i64,
    ) {
        set_tenant_config(
            store,
            tenant,
            &TenantConfig {
                typed_attr_columns,
                ..TenantConfig::new(TenantLifecycleState::Active)
            },
            now_ns,
        )
        .await
        .expect("write tenant config");
    }

    fn keys(columns: &[DeclaredColumn]) -> Vec<&str> {
        columns.iter().map(|c| c.key.as_str()).collect()
    }

    /// A present durable override replaces the CLI base, and the fast path then
    /// serves it from cache inside the horizon with NO second store read.
    ///
    /// Proven by scripting the config-key GETs: step 0 is
    /// `set_tenant_config`'s own read, step 1 the overlay's one miss-driven
    /// read, and step 2 fails. A fast path that re-read inside the horizon
    /// would consume step 2, so the sequence progress pins the read count, and
    /// the returned value would drop to the CLI base as well. The last
    /// assertion then straddles the horizon to prove step 2 really does fire,
    /// so "progress == 2" is a fact about the fast path and not about an
    /// unreachable step.
    #[tokio::test]
    async fn fresh_entry_is_served_from_cache_without_a_second_read() {
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains("/config")
                .then_passthrough()
                .then_passthrough()
                .then_fault(ScriptedFault::Transient("a third config GET".into())),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        let tenant = TenantId::new("acme").hash();
        write_override(
            store.as_ref(),
            &tenant,
            Some(vec![col("dur", DeclaredColumnType::I64)]),
            1,
        )
        .await;
        assert_eq!(
            fault.sequence_progress(0),
            1,
            "the durable write consumed exactly the first scripted config GET"
        );

        let overlay = overlay_over(base_declaring("base.col"), store);
        let first = overlay.declared_columns(tenant, 10_000).await;
        assert_eq!(
            keys(&first),
            ["dur"],
            "the durable override replaces the CLI base"
        );
        assert_eq!(
            fault.sequence_progress(0),
            2,
            "a cache miss issues exactly one config GET"
        );

        // Inside the horizon (10_000 + 500 <= 10_000 + 1_000): the fast path
        // serves it with no further read.
        let second = overlay.declared_columns(tenant, 10_500).await;
        assert_eq!(keys(&second), ["dur"]);
        assert_eq!(
            fault.sequence_progress(0),
            2,
            "a fresh cache entry must be served without a re-read"
        );

        // Past the horizon the read does happen, consuming the failing step:
        // the scripted fault is reachable, so the assertions above are about
        // the fast path rather than about a step nothing could ever hit.
        let stale = overlay.declared_columns(tenant, 20_000).await;
        assert_eq!(fault.sequence_progress(0), 3);
        assert_eq!(
            fault.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the stale resolution really did attempt the scripted third GET"
        );
        assert_eq!(
            keys(&stale),
            ["dur"],
            "and its failure serves the last known good declaration"
        );
    }

    /// Past the horizon the entry is stale, so the next resolution re-reads and
    /// picks up a changed durable declaration.
    #[tokio::test]
    async fn a_stale_entry_triggers_a_refresh_that_sees_the_new_declaration() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("acme").hash();
        write_override(
            store.as_ref(),
            &tenant,
            Some(vec![col("a", DeclaredColumnType::I64)]),
            1,
        )
        .await;
        let overlay = overlay_over(base_declaring("base.col"), store.clone());
        assert_eq!(keys(&overlay.declared_columns(tenant, 10_000).await), ["a"]);

        write_override(
            store.as_ref(),
            &tenant,
            Some(vec![col("b", DeclaredColumnType::Str)]),
            2,
        )
        .await;
        // Still inside the horizon: the cached (now outdated) value is served.
        assert_eq!(keys(&overlay.declared_columns(tenant, 10_500).await), ["a"]);
        // Past the horizon: the refresh reads through and sees the change.
        assert_eq!(
            keys(&overlay.declared_columns(tenant, 20_000).await),
            ["b"],
            "a stale entry must trigger a refresh, not keep serving forever"
        );
    }

    /// ADR-0090 decision 2 / `indexed_fields.rs:428`'s distinction: a
    /// present-but-empty durable override declares nothing (it does NOT fall
    /// back to the CLI base), while an absent override leaves the base
    /// resolution untouched.
    #[tokio::test]
    async fn empty_durable_override_declares_nothing_absent_leaves_the_base() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let overlay = overlay_over(base_declaring("base.col"), store.clone());

        let empty_tenant = TenantId::new("empty").hash();
        write_override(store.as_ref(), &empty_tenant, Some(vec![]), 1).await;
        assert!(
            overlay
                .declared_columns(empty_tenant, 10_000)
                .await
                .is_empty(),
            "a present-but-empty durable override is an explicit zero declared columns, \
             not a fall-through to the CLI base"
        );

        // No record at all: the CLI base stands.
        let absent_tenant = TenantId::new("absent").hash();
        assert_eq!(
            keys(&overlay.declared_columns(absent_tenant, 10_000).await),
            ["base.col"],
            "an absent override must leave the CLI base resolution untouched"
        );

        // A record that exists but carries no typed_attr_columns field is the
        // same "absent" case, not an empty override.
        let no_field_tenant = TenantId::new("nofield").hash();
        write_override(store.as_ref(), &no_field_tenant, None, 1).await;
        assert_eq!(
            keys(&overlay.declared_columns(no_field_tenant, 10_000).await),
            ["base.col"],
            "a record without the field is absent, not an empty override"
        );
    }

    /// A failed durable read serves the CLI base (nothing better is known yet)
    /// and counts the stale-fallback metric; a second resolution inside the
    /// backoff window issues NO second GET, proven by the FaultStore's fired
    /// count.
    #[tokio::test]
    async fn failed_read_serves_the_base_counts_the_metric_and_backs_off() {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("simulated config-store outage".into()),
            )
            .with_occurrence(Occurrence::Always),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        let overlay = overlay_over(base_declaring("base.col"), store);
        let tenant = TenantId::new("acme").hash();
        let global_before = crate::typed_attr_metrics::stale_fallbacks();

        let first = overlay.declared_columns(tenant, 10_000).await;
        assert_eq!(
            keys(&first),
            ["base.col"],
            "a failed read serves the CLI base when nothing has ever resolved"
        );
        assert_eq!(
            fault.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the first failed resolution issues exactly one GET"
        );
        assert_eq!(
            overlay.stale_fallbacks(),
            1,
            "a failed-read fallback increments the stale-fallback metric"
        );
        assert!(
            crate::typed_attr_metrics::stale_fallbacks() > global_before,
            "and the process-global counter /metrics renders moves too (a delta \
             rather than an exact count: other tests in this binary share it)"
        );

        // Inside the backoff window (10_000 + 500 < 10_000 + 1_000): served
        // from the captured base with no second GET.
        let second = overlay.declared_columns(tenant, 10_500).await;
        assert_eq!(keys(&second), ["base.col"]);
        assert_eq!(
            fault.fault_count(Op::Get, FaultKind::Transient),
            1,
            "a resolution inside the backoff window must not issue a second GET"
        );
        assert_eq!(
            overlay.stale_fallbacks(),
            2,
            "a backoff-suppressed resolution is still a fallback and is still counted"
        );

        // Past the backoff window a re-GET is attempted again (still failing).
        let third = overlay.declared_columns(tenant, 11_001).await;
        assert_eq!(keys(&third), ["base.col"]);
        assert_eq!(
            fault.fault_count(Op::Get, FaultKind::Transient),
            2,
            "past the backoff window a second GET is attempted"
        );
    }

    /// A malformed durable declaration is treated exactly like a failed read:
    /// the overlay never applies it, serves the fallback, and counts it.
    ///
    /// The record is forged with prost rather than written through
    /// `set_tenant_config`, because that function correctly refuses an invalid
    /// declaration (asserted here too). The case under test is a record that is
    /// already durable, whatever wrote it: an older writer, a hand-edited
    /// object, or a future writer with a rule this build does not share.
    #[tokio::test]
    async fn a_malformed_durable_declaration_falls_back() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("acme").hash();

        // The durable write path refuses an invalid declaration outright, so a
        // malformed record cannot come from it.
        let refused = set_tenant_config(
            store.as_ref(),
            &tenant,
            &TenantConfig {
                typed_attr_columns: Some(vec![col("body", DeclaredColumnType::Str)]),
                ..TenantConfig::new(TenantLifecycleState::Active)
            },
            1,
        )
        .await;
        assert!(
            refused.is_err(),
            "set_tenant_config must refuse a fixed-column collision"
        );

        // Forge the record instead: a duplicate declared key, which
        // `validate_typed_attr_columns` rejects.
        let record = ravel_proto::sys::v1::TenantConfigRecord {
            format_version: 1,
            tenant_hash: tenant.0.to_vec(),
            lifecycle_state: ravel_proto::sys::v1::TenantLifecycleState::Active as i32,
            typed_attr_columns: Some(ravel_proto::sys::v1::TypedAttrColumnConfig {
                columns: vec![
                    ravel_proto::sys::v1::TypedAttrColumn {
                        key: "dup".to_string(),
                        r#type: ravel_proto::sys::v1::TypedAttrColumnType::I64 as i32,
                    },
                    ravel_proto::sys::v1::TypedAttrColumn {
                        key: "dup".to_string(),
                        r#type: ravel_proto::sys::v1::TypedAttrColumnType::I64 as i32,
                    },
                ],
            }),
            created_unix_ns: 1,
            updated_unix_ns: 1,
            ..Default::default()
        };
        store
            .put(
                &config_key(&tenant),
                record.encode_to_vec().into(),
                PutOptions::default(),
            )
            .await
            .expect("forge a malformed config record");

        let overlay = overlay_over(base_declaring("base.col"), store);
        let resolved = overlay.declared_columns(tenant, 10_000).await;
        assert_eq!(
            keys(&resolved),
            ["base.col"],
            "a malformed durable declaration must fall back to the CLI base, \
             never be applied"
        );
        assert_eq!(
            overlay.stale_fallbacks(),
            1,
            "a malformed durable declaration is counted like a failed read"
        );
    }

    /// Once a read has succeeded, a later failed re-read serves that last known
    /// good declaration rather than dropping back to the CLI base, and counts
    /// the fallback.
    #[tokio::test]
    async fn a_later_failed_reread_serves_the_last_known_good_declaration() {
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Get)
                .with_key_contains("/config")
                // The durable write's own read, then the overlay's first
                // (successful) resolution, then a failing re-read.
                .then_passthrough()
                .then_passthrough()
                .then_fault(ScriptedFault::Transient("simulated later outage".into())),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        let tenant = TenantId::new("acme").hash();
        write_override(
            store.as_ref(),
            &tenant,
            Some(vec![col("dur", DeclaredColumnType::I64)]),
            1,
        )
        .await;
        let overlay = overlay_over(base_declaring("base.col"), store);

        assert_eq!(
            keys(&overlay.declared_columns(tenant, 10_000).await),
            ["dur"],
            "the first resolution succeeds and caches the durable declaration"
        );
        assert_eq!(overlay.stale_fallbacks(), 0);

        // Past the horizon, so this re-reads -- and the read now fails.
        assert_eq!(
            keys(&overlay.declared_columns(tenant, 20_000).await),
            ["dur"],
            "a failed re-read serves the last successfully resolved declaration, \
             not the CLI base"
        );
        assert_eq!(
            fault.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the re-read really was attempted and really did fail"
        );
        assert_eq!(
            overlay.stale_fallbacks(),
            1,
            "serving a stale value on a failed re-read is counted"
        );
    }

    /// Idle eviction: an entry untouched past the TTL is dropped, one still
    /// serving queries survives, and an evicted entry re-derives on the next
    /// resolution.
    #[tokio::test]
    async fn an_idle_entry_is_evicted_and_an_active_one_survives() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let idle = TenantId::new("idle").hash();
        let active = TenantId::new("active").hash();
        write_override(
            store.as_ref(),
            &idle,
            Some(vec![col("x", DeclaredColumnType::Str)]),
            1,
        )
        .await;
        write_override(
            store.as_ref(),
            &active,
            Some(vec![col("y", DeclaredColumnType::Str)]),
            1,
        )
        .await;
        let overlay = overlay_over(base_declaring("base.col"), store);

        let t0 = 1_000;
        overlay.declared_columns(idle, t0).await;
        overlay.declared_columns(active, t0).await;
        assert_eq!(overlay.cached_tenants(), 2);

        let ttl_ns = 10_000;
        let sweep_ns = t0 + ttl_ns + 1;
        // The active tenant resolves again right before the sweep, advancing
        // its last-touch stamp.
        overlay.declared_columns(active, sweep_ns).await;
        assert_eq!(
            overlay.evict_idle(sweep_ns, ttl_ns),
            1,
            "only the idle tenant's entry is evicted"
        );
        assert_eq!(overlay.cached_tenants(), 1);
        // Re-derivable: the evicted tenant's next resolution reads through.
        assert_eq!(keys(&overlay.declared_columns(idle, sweep_ns).await), ["x"]);
    }

    /// The declaration order the operator wrote is the order the query side
    /// sees, both from the CLI base and from a durable override (ADR-0090
    /// decision 3: this is the schema-append order).
    #[tokio::test]
    async fn declaration_order_survives_resolution() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("acme").hash();
        write_override(
            store.as_ref(),
            &tenant,
            Some(vec![
                col("z", DeclaredColumnType::Bytes),
                col("a", DeclaredColumnType::Bool),
                col("m", DeclaredColumnType::I64),
            ]),
            1,
        )
        .await;
        let overlay = overlay_over(TypedAttrColumnConfig::default(), store);
        let resolved = overlay.declared_columns(tenant, 10_000).await;
        assert_eq!(keys(&resolved), ["z", "a", "m"]);
        assert_eq!(
            resolved.iter().map(|c| c.ty).collect::<Vec<_>>(),
            [DeclaredType::Bytes, DeclaredType::Bool, DeclaredType::I64],
            "each declared type maps onto its query-side counterpart"
        );
    }
}
