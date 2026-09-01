//! Durable per-tenant indexed-field override, applied to the log flush path via
//! a cache-aside overlay (ADR-0079).
//!
//! [`crate::LogIndexedFields`] resolves the CLI-derived base/override
//! (`--indexed-field-default` / `--indexed-field-tenant`), the only thing that
//! decided which attribute fields a log object indexed before this module. ADR-0066
//! decision 6 promised `TenantConfig.indexed_fields` (the durable per-tenant
//! record at `t/<tenant_hash>/config`) as a no-restart runtime override of that
//! list, but nothing read it. [`IndexedFieldsOverlay`] is that reader.
//!
//! It wraps the existing `Arc<dyn LogIndexedFields>` base unchanged (the public
//! trait is deliberately not reshaped, ADR-0079 rejected alternatives) plus a
//! per-tenant cache, mirroring [`crate::GenerationSwitch`]'s cache-aside shape: a
//! synchronous fast path ([`IndexedFieldsOverlay::fields_for_cached`]) and an
//! async, caller-driven refresh on a stale/missing entry
//! ([`IndexedFieldsOverlay::refresh`]), populated lazily the first time a tenant
//! flushes. [`crate::log_router::LogIngestRouter`]'s `active_set` is the existing
//! precedent for the two-step dance; `LogFlushCtx::run_flush` follows it.
//!
//! **Failure discipline (ADR-0079 Safety), deliberately unlike
//! `GenerationSwitch::try_grace_extend`.** A failed `TenantConfig` read or a
//! validation failure never blocks or fails the flush and never fails closed:
//! the overlay serves the last successfully-resolved value for that tenant, or
//! the wrapped base's CLI-only resolution if the tenant has never refreshed. This
//! is safe because indexed-field config is an advisory, per-object, self-describing
//! write-time pruning hint the query side never consults, so stale or CLI-only
//! resolution can only cost extra scan work, never a wrong query result. A failed
//! read stamps a backoff so a sustained store outage costs at most one failed GET
//! per tenant per backoff window, and the read never borrows from the flush's own
//! `deadline_ns` PUT budget. Every flush served from a stale value or a
//! failed-read/validation fallback increments a metric (the caller's job, mirroring
//! `active_set`'s grace-extended-stale counter), so a permanently unreadable or
//! malformed override is never silent.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use ravel_catalog::{DeclaredTypedColumn, read_config_values};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::TenantHash;

use crate::lifecycle::DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS;
use crate::log_router::LogIndexedFields;

/// Backoff after a failed durable re-read before the overlay will attempt that
/// tenant's `TenantConfig` GET again (ADR-0079 deliverable 5). Mirrors the shape
/// and magnitude of `lifecycle_refresh.rs`'s `DEFAULT_ON_MISS_INTERVAL_NS`: one
/// second, an order of magnitude below the staleness horizon, so a degraded
/// object store adds at most one failed GET per tenant per second, never one per
/// flush (which at default ingest settings is several per second per tenant).
pub const DEFAULT_INDEXED_FIELDS_REREAD_BACKOFF_NS: i64 = 1_000_000_000;

/// Sentinel `refreshed_at_ns`/`attempted_at_ns` value meaning "no such event
/// yet". `now_ns.saturating_sub(i64::MIN)` saturates to `i64::MAX`, so a sentinel
/// entry is neither fresh (age exceeds any horizon) nor in backoff (age exceeds
/// any window) without a separate branch.
const NEVER: i64 = i64::MIN;

/// One tenant's cached resolved indexed-field list plus the timestamps the fast
/// path, the backoff, and idle eviction key off.
///
/// The three ADR-0079 deliverable-2 timestamps have distinct jobs:
/// - `refreshed_at_ns` advances only on a successful durable resolution; the fast
///   path compares it against the staleness horizon. [`NEVER`] for an entry that
///   only ever recorded a failed read (so the fast path never trusts it and each
///   flush re-enters `refresh`, where the backoff short-circuits).
/// - `attempted_at_ns` stamps the last *failed* read; the backoff compares it so a
///   sustained outage does not re-GET on every flush. Reset to [`NEVER`] on a
///   success so a recovered tenant is not held in backoff.
/// - `last_touched_ns` advances on *any* access (fast-path hit, refresh, or
///   failure-serve), and is the only stamp idle eviction reads. It exists for the
///   same reason [`crate::generation`]'s `TenantView` carries one: a base-only
///   failure entry has `refreshed_at_ns == NEVER`, so eviction cannot key off that
///   without evicting live entries every sweep.
#[derive(Debug, Clone)]
struct CacheEntry {
    fields: Vec<String>,
    /// The tenant's durable declared typed columns (ADR-0090), read from the
    /// same `TenantConfig` GET that resolves `fields` (ADR-0873 wave 5a: "the
    /// same durable-config bounded-staleness read that already supplies indexed
    /// fields"). Empty when the config declares none, or on a base-only entry
    /// captured before any successful read. Advisory like `fields`: a stale or
    /// empty list only costs a segment its stamp coverage (it scans), never a
    /// wrong answer, so it follows the identical failure discipline.
    typed_columns: Vec<DeclaredTypedColumn>,
    refreshed_at_ns: i64,
    attempted_at_ns: i64,
    last_touched_ns: i64,
}

/// Per-tenant cache behind a `Mutex`, mirroring [`crate::generation`]'s `Inner`
/// (a lock, not `ArcSwap`/`RwLock`: the read side already tolerates a lock, and a
/// lock lets the miss path install a freshly-read value without a lost-update
/// race).
#[derive(Default)]
struct Inner {
    cache: HashMap<TenantHash, CacheEntry>,
}

/// The outcome of an [`IndexedFieldsOverlay::refresh`]: the resolved field list
/// plus whether it came from a fresh durable read (or genuine absence) or from a
/// stale/failed-read/validation fallback. The caller (`run_flush`) counts the
/// stale-fallback metric on [`RefreshOutcome::Fallback`] and leaves it untouched
/// on [`RefreshOutcome::Fresh`], exactly as `active_set` counts its
/// grace-extended-stale metric only on the degraded arm.
pub(crate) enum RefreshOutcome {
    /// A durable read (or a genuine absence, itself a valid state) resolved the
    /// list this tick.
    Fresh(Vec<String>),
    /// The durable read failed, was skipped for backoff, or produced a list that
    /// failed validation; the overlay served the last-known-good value or the
    /// base's CLI-only resolution.
    Fallback(Vec<String>),
}

impl RefreshOutcome {
    /// The resolved field list, regardless of how it was resolved.
    pub(crate) fn into_fields(self) -> Vec<String> {
        match self {
            RefreshOutcome::Fresh(fields) | RefreshOutcome::Fallback(fields) => fields,
        }
    }

    /// Whether the caller should count the stale-fallback metric for this flush.
    pub(crate) fn is_fallback(&self) -> bool {
        matches!(self, RefreshOutcome::Fallback(_))
    }
}

/// A concrete cache-aside overlay over a base [`LogIndexedFields`] resolver
/// (ADR-0079). `LogFlushCtx` holds one directly, shared across shards by `Arc`;
/// it is not a trait object and the `LogIndexedFields` trait is untouched.
pub struct IndexedFieldsOverlay {
    /// The CLI-derived base/override, resolved unchanged when a tenant has no
    /// durable override (or its durable read has never yet succeeded).
    base: Arc<dyn LogIndexedFields>,
    inner: Mutex<Inner>,
    /// The staleness horizon: a cached entry newer than this is served by the
    /// sync fast path without a durable read. Reuses the lifecycle horizon rather
    /// than minting a new constant.
    horizon_ns: i64,
    /// The failed-read backoff window (see [`DEFAULT_INDEXED_FIELDS_REREAD_BACKOFF_NS`]).
    backoff_ns: i64,
}

impl IndexedFieldsOverlay {
    /// Wrap `base` with the default staleness horizon
    /// ([`DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS`], the same `C` every other
    /// bounded-staleness overlay in this crate uses) and the default failed-read
    /// backoff.
    pub fn new(base: Arc<dyn LogIndexedFields>) -> Self {
        Self::with_intervals(
            base,
            DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS,
            DEFAULT_INDEXED_FIELDS_REREAD_BACKOFF_NS,
        )
    }

    /// Wrap `base` with an explicit horizon and backoff. Both are clamped to at
    /// least 1 ns so a zero (mis)configuration can never make every entry
    /// instantly stale or disable the backoff. For tests that need a small,
    /// deterministic horizon; production uses [`IndexedFieldsOverlay::new`].
    pub fn with_intervals(
        base: Arc<dyn LogIndexedFields>,
        horizon_ns: i64,
        backoff_ns: i64,
    ) -> Self {
        IndexedFieldsOverlay {
            base,
            inner: Mutex::new(Inner::default()),
            horizon_ns: horizon_ns.max(1),
            backoff_ns: backoff_ns.max(1),
        }
    }

    /// Lock the inner state, recovering a poisoned guard rather than propagating a
    /// panic: the cache is plain per-tenant state with no cross-entry invariant a
    /// panic mid-update could half-break, and indexed-field resolution must never
    /// take a flush down (ADR-0079 Safety). Mirrors [`crate::generation`]'s `lock`.
    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Synchronous fast path: the cached resolved list for `tenant` if a
    /// successful refresh is still within the staleness horizon
    /// (`now_ns - refreshed_at_ns <= horizon`, the inclusive-fresh boundary
    /// [`crate::generation::GenerationSwitch::route_cached`] uses), else `None` so
    /// the caller drives [`IndexedFieldsOverlay::refresh`]. A cache hit stamps
    /// `last_touched_ns` so idle eviction never reaps an entry still serving
    /// flushes, even one whose `refreshed_at_ns` has not moved since its last
    /// re-read. `now_ns` is the flush's pinned `flush_open_ns`; this type reads no
    /// clock.
    pub(crate) fn fields_for_cached(
        &self,
        tenant: &TenantHash,
        now_ns: i64,
    ) -> Option<Vec<String>> {
        let mut inner = self.lock();
        let entry = inner.cache.get_mut(tenant)?;
        if now_ns.saturating_sub(entry.refreshed_at_ns) <= self.horizon_ns {
            entry.last_touched_ns = now_ns;
            Some(entry.fields.clone())
        } else {
            None
        }
    }

    /// The tenant's declared typed columns as last resolved into the cache
    /// (ADR-0873 wave 5a). Read after the flush has already resolved this
    /// tenant's indexed fields for the same tick (fast path or `refresh`), so
    /// the entry exists and its `typed_columns` came from the same durable read;
    /// returns an empty list when no entry exists yet or the config declares
    /// none. Stamps `last_touched_ns` so reading declared columns keeps an entry
    /// live exactly as a field read does. `now_ns` is the flush's pinned clock.
    pub(crate) fn typed_columns_cached(
        &self,
        tenant: &TenantHash,
        now_ns: i64,
    ) -> Vec<DeclaredTypedColumn> {
        let mut inner = self.lock();
        match inner.cache.get_mut(tenant) {
            Some(entry) => {
                entry.last_touched_ns = now_ns;
                entry.typed_columns.clone()
            }
            None => Vec::new(),
        }
    }

    /// Async refresh on a miss or stale entry: read this one tenant's
    /// `TenantConfig`, validate a present durable list, overlay it onto the base,
    /// install the result, and return it. Reads the store at most once per call,
    /// and never at all while a prior failed read for this tenant is still inside
    /// the backoff window (it serves the cached value instead). The read is a
    /// single plain GET, never wrapped in the flush's `deadline_ns` race and never
    /// retried here, so it cannot borrow from the flush's PUT budget (ADR-0079
    /// deliverable 5); a store error returns at once to the fallback path.
    ///
    /// Overlay semantics (ADR-0079): a durable `indexed_fields = Some(list)` (a
    /// possibly-empty list, a valid explicit opt-out) replaces the base's resolved
    /// list outright once validated; `None` (no record, or a record without the
    /// field) leaves the base's own `fields_for(tenant)` resolution unchanged. A
    /// list that fails validation is treated exactly like a failed read.
    ///
    /// `now_ns` is the flush's pinned `flush_open_ns`.
    pub(crate) async fn refresh(
        &self,
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
        now_ns: i64,
    ) -> RefreshOutcome {
        // Backoff short-circuit: a recent failed read means the store is degraded;
        // serve the cached value (last-known-good, or the base captured at failure
        // time) without hitting the store again this window.
        {
            let mut inner = self.lock();
            if let Some(entry) = inner.cache.get_mut(tenant)
                && now_ns.saturating_sub(entry.attempted_at_ns) < self.backoff_ns
            {
                entry.last_touched_ns = now_ns;
                return RefreshOutcome::Fallback(entry.fields.clone());
            }
        }

        // A single durable GET. Success (present or genuinely absent) is the only
        // path that installs a fresh value and clears the backoff.
        match read_config_values(store, tenant).await {
            Ok(config) => {
                // The declared typed columns ride the same read (ADR-0873 wave
                // 5a); an absent list is the ordinary "no declared columns"
                // state, never an error.
                let typed_columns = config
                    .as_ref()
                    .and_then(|c| c.typed_attr_columns.clone())
                    .unwrap_or_default();
                let durable = config.and_then(|c| c.indexed_fields);
                match durable {
                    // A present durable override: validate before it can replace
                    // the resolved list, exactly as the CLI path validates its
                    // parsed policy. A malformed record is treated as a failed read.
                    Some(list) if validate_indexed_list(&list).is_err() => {
                        self.serve_on_failure(tenant, now_ns)
                    }
                    Some(list) => self.install_fresh(tenant, list, typed_columns, now_ns),
                    // No override: the base's own tenant-override-or-default
                    // resolution stands unchanged.
                    None => {
                        let base_fields = self.base.fields_for(tenant);
                        self.install_fresh(tenant, base_fields, typed_columns, now_ns)
                    }
                }
            }
            Err(_) => self.serve_on_failure(tenant, now_ns),
        }
    }

    /// Install a freshly-resolved list for `tenant`, stamping the refresh time and
    /// clearing any pending failed-read backoff.
    fn install_fresh(
        &self,
        tenant: &TenantHash,
        fields: Vec<String>,
        typed_columns: Vec<DeclaredTypedColumn>,
        now_ns: i64,
    ) -> RefreshOutcome {
        let mut inner = self.lock();
        inner.cache.insert(
            *tenant,
            CacheEntry {
                fields: fields.clone(),
                typed_columns,
                refreshed_at_ns: now_ns,
                attempted_at_ns: NEVER,
                last_touched_ns: now_ns,
            },
        );
        RefreshOutcome::Fresh(fields)
    }

    /// The failed-read/validation-failure path (ADR-0079 Safety): serve the last
    /// successfully-resolved value for this tenant if one exists, else the base's
    /// CLI-only resolution, and stamp `attempted_at_ns` so the backoff bites. Never
    /// fails closed and never has a horizon cutoff. A base-captured entry keeps
    /// `refreshed_at_ns == NEVER` so the fast path never trusts it and every flush
    /// re-enters `refresh` (where the backoff makes the re-GET cheap) until a read
    /// finally succeeds.
    fn serve_on_failure(&self, tenant: &TenantHash, now_ns: i64) -> RefreshOutcome {
        let mut inner = self.lock();
        if let Some(entry) = inner.cache.get_mut(tenant)
            && entry.refreshed_at_ns != NEVER
        {
            // A last-known-good value exists: keep serving it, only stamp the
            // failed attempt.
            entry.attempted_at_ns = now_ns;
            entry.last_touched_ns = now_ns;
            return RefreshOutcome::Fallback(entry.fields.clone());
        }
        // Never successfully refreshed: capture the base resolution so a
        // stale-serve inside the backoff window is cheap, and stamp the attempt.
        let base_fields = self.base.fields_for(tenant);
        inner.cache.insert(
            *tenant,
            CacheEntry {
                fields: base_fields.clone(),
                // No config ever read for this tenant: no declared columns are
                // known, so no segment it flushes is stamped until a read
                // succeeds. Fail-closed to uncovered, never to a wrong stamp.
                typed_columns: Vec::new(),
                refreshed_at_ns: NEVER,
                attempted_at_ns: now_ns,
                last_touched_ns: now_ns,
            },
        );
        RefreshOutcome::Fallback(base_fields)
    }

    /// Evict every cached entry last touched before `now_ns - ttl_ns` (ADR-0069
    /// decision 2, the same idle-tenant sweep [`crate::generation::GenerationSwitch::evict_idle`]
    /// feeds). Returns the number of entries dropped. An evicted entry is
    /// re-derivable: the tenant's next flush is a miss that re-reads its config.
    /// `now_ns`/`ttl_ns` are injected; this type reads no clock, so eviction is
    /// deterministic under test.
    pub(crate) fn evict_idle(&self, now_ns: i64, ttl_ns: i64) -> usize {
        let mut inner = self.lock();
        let before = inner.cache.len();
        inner
            .cache
            .retain(|_, entry| now_ns.saturating_sub(entry.last_touched_ns) <= ttl_ns);
        before - inner.cache.len()
    }
}

/// Reject an empty or duplicate field name in a durable indexed-field list, the
/// SAME check `services/ravel-server`'s `postings_config::validate_list` applies
/// to the CLI-parsed policy (empty name, duplicate name). Reimplemented here
/// rather than shared because `ravel-ingest` cannot depend on the server crate; a
/// durable list that fails this is treated exactly like a failed read, so a
/// malformed record never produces a writer that indexes the wrong set or panics
/// on an empty name. Returns `Ok(())` for a valid list, including the empty list
/// (a valid explicit opt-out).
fn validate_indexed_list(list: &[String]) -> Result<(), ()> {
    let mut seen: HashSet<&str> = HashSet::with_capacity(list.len());
    for field in list {
        if field.is_empty() {
            return Err(());
        }
        if !seen.insert(field.as_str()) {
            return Err(());
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_catalog::{TenantConfig, TenantLifecycleState, set_tenant_config};
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
    };
    use ravel_object_store::memory::MemoryStore;

    use super::*;

    fn tenant(byte: u8) -> TenantHash {
        TenantHash([byte; 16])
    }

    /// A base resolver returning a fixed non-empty list for every tenant, so a
    /// test can tell "override to empty" (index nothing) apart from "no override"
    /// (fall through to this list) -- [`crate::NoIndexedFields`] returns empty for
    /// both, which cannot distinguish them.
    struct FixedFields(Vec<String>);
    impl LogIndexedFields for FixedFields {
        fn fields_for(&self, _tenant: &TenantHash) -> Vec<String> {
            self.0.clone()
        }
    }

    fn overlay_over(base: Arc<dyn LogIndexedFields>) -> IndexedFieldsOverlay {
        // A tiny horizon and backoff so the staleness/backoff boundaries are easy
        // to straddle with injected `now_ns`.
        IndexedFieldsOverlay::with_intervals(base, 1_000, 1_000)
    }

    async fn write_override(
        store: &dyn ObjectStoreBackend,
        t: &TenantHash,
        indexed_fields: Option<Vec<String>>,
    ) {
        set_tenant_config(
            store,
            t,
            &TenantConfig {
                indexed_fields,
                ..TenantConfig::new(TenantLifecycleState::Active)
            },
            1,
        )
        .await
        .expect("write config");
    }

    /// A present durable override replaces the base's resolved list outright; the
    /// fast path then serves it from cache within the horizon.
    #[tokio::test]
    async fn present_override_replaces_base_and_is_cached() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let overlay = overlay_over(Arc::new(FixedFields(vec!["base.field".into()])));
        let t = tenant(1);
        write_override(store.as_ref(), &t, Some(vec!["http.status_code".into()])).await;

        // Miss -> refresh reads the durable override.
        let out = overlay.refresh(store.as_ref(), &t, 10_000).await;
        assert!(matches!(out, RefreshOutcome::Fresh(_)));
        assert_eq!(out.into_fields(), vec!["http.status_code".to_string()]);

        // Within the horizon the fast path serves the same list with no read.
        assert_eq!(
            overlay.fields_for_cached(&t, 10_500),
            Some(vec!["http.status_code".to_string()])
        );
    }

    /// ADR-0079 deliverable-test: a present-but-empty override ("index nothing")
    /// is distinct from an absent override ("leave the base"). The empty list wins
    /// outright; the absent tenant falls through to the base's non-empty list.
    #[tokio::test]
    async fn empty_override_indexes_nothing_absent_leaves_base() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let overlay = overlay_over(Arc::new(FixedFields(vec!["base.field".into()])));

        // Present-but-empty override: resolves to an empty list, not the base.
        let empty_tenant = tenant(1);
        write_override(store.as_ref(), &empty_tenant, Some(vec![])).await;
        let out = overlay.refresh(store.as_ref(), &empty_tenant, 10_000).await;
        assert!(
            matches!(out, RefreshOutcome::Fresh(_)),
            "a valid empty override is a fresh resolution, not a fallback"
        );
        assert!(
            out.into_fields().is_empty(),
            "present-but-empty override must index nothing, not fall back to the base"
        );

        // Absent override (no record at all): the base's own resolution stands.
        let absent_tenant = tenant(2);
        let out = overlay
            .refresh(store.as_ref(), &absent_tenant, 10_000)
            .await;
        assert!(matches!(out, RefreshOutcome::Fresh(_)));
        assert_eq!(
            out.into_fields(),
            vec!["base.field".to_string()],
            "an absent override must leave the base resolution untouched"
        );
    }

    /// ADR-0079 deliverable-5 test: under a sustained store outage, a second
    /// cache-miss refresh inside the backoff window must NOT issue a second
    /// `read_config_values` GET. Proven with a `FaultStore` that fails every GET
    /// and asserting its fired-fault count is exactly one across two refresh calls.
    #[tokio::test]
    async fn failed_read_backs_off_and_issues_one_get_per_window() {
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Get,
                ScriptedFault::Transient("simulated sustained store latency".into()),
            )
            .with_occurrence(Occurrence::Always),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        // Backoff of 1_000 ns; the base indexes nothing, so a first-touch failure
        // serves the (empty) base resolution.
        let overlay =
            IndexedFieldsOverlay::with_intervals(Arc::new(crate::NoIndexedFields), 1_000, 1_000);
        let t = tenant(7);

        // First miss: the read fails, the overlay serves the base and stamps the
        // backoff. Exactly one GET has fired.
        let out = overlay.refresh(store.as_ref(), &t, 10_000).await;
        assert!(matches!(out, RefreshOutcome::Fallback(_)));
        assert_eq!(
            fault.fault_count(Op::Get, FaultKind::Transient),
            1,
            "the first failed refresh issues exactly one GET"
        );

        // Second miss inside the backoff window (10_000 + 500 < 10_000 + 1_000):
        // served from cache, NO second GET issued.
        let out = overlay.refresh(store.as_ref(), &t, 10_500).await;
        assert!(matches!(out, RefreshOutcome::Fallback(_)));
        assert_eq!(
            fault.fault_count(Op::Get, FaultKind::Transient),
            1,
            "a refresh inside the backoff window must not issue a second GET"
        );

        // Once the backoff has elapsed, a re-GET is allowed again (still failing).
        let out = overlay.refresh(store.as_ref(), &t, 11_001).await;
        assert!(matches!(out, RefreshOutcome::Fallback(_)));
        assert_eq!(
            fault.fault_count(Op::Get, FaultKind::Transient),
            2,
            "past the backoff window a second GET is attempted"
        );
    }

    /// A read that succeeds installs a fresh value and clears the backoff, so the
    /// very next stale flush re-reads (picks up a change) instead of staying
    /// parked in a stale backoff.
    #[tokio::test]
    async fn success_clears_backoff() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let overlay = overlay_over(Arc::new(FixedFields(vec!["base.field".into()])));
        let t = tenant(3);
        write_override(store.as_ref(), &t, Some(vec!["a".into()])).await;

        overlay.refresh(store.as_ref(), &t, 10_000).await;
        // Change the durable override, then refresh past the horizon: a fresh read
        // must pick it up (backoff cleared by the prior success).
        write_override(store.as_ref(), &t, Some(vec!["b".into()])).await;
        let out = overlay.refresh(store.as_ref(), &t, 20_000).await;
        assert_eq!(out.into_fields(), vec!["b".to_string()]);
    }

    /// A malformed durable list (a duplicate field name) is treated exactly like a
    /// failed read: the overlay falls back rather than passing the bad list to the
    /// writer, and the outcome is a fallback the caller counts. The empty-name case
    /// is covered by `validate_indexed_list`'s unit assertions below.
    #[tokio::test]
    async fn malformed_durable_list_falls_back() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let overlay = overlay_over(Arc::new(FixedFields(vec!["base.field".into()])));
        let t = tenant(4);
        // A duplicate field name: invalid.
        write_override(store.as_ref(), &t, Some(vec!["dup".into(), "dup".into()])).await;

        let out = overlay.refresh(store.as_ref(), &t, 10_000).await;
        assert!(
            matches!(out, RefreshOutcome::Fallback(_)),
            "a malformed durable list must fall back, not resolve fresh"
        );
        assert_eq!(
            out.into_fields(),
            vec!["base.field".to_string()],
            "the fallback serves the base resolution, never the malformed list"
        );
    }

    #[test]
    fn validate_indexed_list_matches_the_cli_rules() {
        assert!(
            validate_indexed_list(&[]).is_ok(),
            "empty list is a valid opt-out"
        );
        assert!(validate_indexed_list(&["a".into(), "b".into()]).is_ok());
        assert!(
            validate_indexed_list(&["".into()]).is_err(),
            "an empty field name is rejected"
        );
        assert!(
            validate_indexed_list(&["a".into(), "a".into()]).is_err(),
            "a duplicate field name is rejected"
        );
    }

    /// Idle eviction (ADR-0069 decision 2): an entry untouched past the TTL is
    /// dropped, one still being touched survives, and an evicted entry re-derives
    /// on the next refresh.
    #[tokio::test]
    async fn idle_entry_evicted_active_entry_survives() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let overlay = overlay_over(Arc::new(FixedFields(vec!["base.field".into()])));
        let idle = tenant(1);
        let active = tenant(2);
        write_override(store.as_ref(), &idle, Some(vec!["x".into()])).await;
        write_override(store.as_ref(), &active, Some(vec!["y".into()])).await;

        let t0 = 1_000;
        overlay.refresh(store.as_ref(), &idle, t0).await;
        overlay.refresh(store.as_ref(), &active, t0).await;

        let ttl_ns = 10_000;
        // The active tenant is touched (a fast-path hit) just before the sweep, so
        // its last-touch advances even though its refresh time does not.
        let sweep_ns = t0 + ttl_ns + 1;
        assert!(overlay.fields_for_cached(&active, sweep_ns).is_none());
        // Its horizon lapsed, so re-establish a fresh, touched entry right before
        // the sweep.
        overlay.refresh(store.as_ref(), &active, sweep_ns).await;

        let evicted = overlay.evict_idle(sweep_ns, ttl_ns);
        assert_eq!(evicted, 1, "only the idle tenant's entry is evicted");
        assert!(
            overlay.fields_for_cached(&active, sweep_ns).is_some(),
            "the active tenant's entry survives the sweep"
        );
    }
}
