//! Background maintenance task (docs/compaction-retention-plan.md P8, issue
//! #115; storage-derived tenant set is ADR-0048 decision 3, issue #504).
//! Periodically runs age-based retention, L0->L1 compaction, and the GC
//! sweeper over every `(signal, shard)` of every tenant storage holds data
//! for.
//!
//! Unlike fold (a pure query-cost optimization), this task deletes and rewrites
//! durable objects, but it changes nothing about *what* any sweep, retention,
//! or compaction rule decides: it is only the driver that calls
//! [`scan_and_maintain_with_memo`] (retention-before-compaction over every
//! sealed bucket) and [`ravel_maintain::sweep_shard`] (the three GC rules) once
//! per tenant per tick. Both are idempotent, so a missed or crashed tick is
//! recovered on the next one. The clock is the real [`SystemClock`], matching
//! everything else in this crate.
//!
//! [`spawn`] runs one supervisor task, not one task per tenant: at the start
//! of every tick it re-enumerates tenants from storage
//! ([`ravel_maintain::discover_tenants`] via
//! [`crate::tenant_discovery::discover_and_restrict`]), optionally narrows
//! that set to the configured `--tenant-token`/`--maintain-tenant`
//! restriction, then runs [`run_tick`] for each tenant in the result. A
//! tenant that first writes data mid-run is picked up on the next cycle with
//! no restart, and a tenant removed from the restriction (but still holding
//! data) is counted as excluded rather than silently dropped. Discovery
//! failure (the LIST errors) skips the whole cycle -- no tenant's tick runs
//! -- with a logged warning and a failure counter; it never falls back to an
//! empty set, because that would be indistinguishable from healthy idleness,
//! the exact silence findings S2-17/S5-09 describe.
//!
//! [`LegalHoldCheck::refresh`] is called once per tenant per tick, before
//! either pass, and its snapshot is the [`LeaseCheck`] threaded through every
//! `(signal, shard)` of that tick (ADR-0048 decision 1). A refresh failure
//! never falls back to [`NoLeases`]: the tenant's whole tick is skipped and
//! retried next tick, so a transient store fault can never turn into an
//! unprotected delete pass.
//!
//! One [`MaintainMemo`] is held across every tick and every tenant until
//! shutdown (issue #280, #330). The memo records buckets already known
//! terminal so a steady-state tick skips re-listing and re-reading them,
//! until a periodic full re-verify forces a fresh evaluation. It is ephemeral
//! and never correctness-bearing: a fresh (cold) memo on the first tick after
//! a worker start does exactly one full rescan identical to the pre-memo
//! behavior, and a wrong or lost entry only defers work by at most the
//! re-verify interval. The memo key is `(tenant, signal, shard, hour)`, so one
//! process-wide memo safely spans every tenant this supervisor discovers,
//! across ticks.

use std::sync::Arc;
use std::time::Duration;

use rand::RngExt as _;
use ravel_ingest::{Clock as _, SystemClock};
use ravel_maintain::scan::{MaintainMemo, MaintainReport, scan_and_maintain_with_memo};
use ravel_maintain::{Clock, CompactorConfig, LegalHoldCheck, RetentionConfig, sweep_shard};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::tenant_discovery::{TenantDiscoveryMetrics, discover_and_restrict};

/// Default `maintain_interval`: 5 minutes.
pub const DEFAULT_MAINTAIN_INTERVAL: Duration = Duration::from_secs(5 * 60);

/// The service-layer wall clock for [`ravel_maintain::Clock`]
/// (`ravel_maintain`'s clock doc defers the wall-clock impl to phase 8, here).
/// Delegates to `ravel-ingest`'s [`SystemClock`], the one blessed wall clock in
/// this process, so no maintenance code path ever reads `SystemTime::now()`
/// directly.
struct WallClock;

impl Clock for WallClock {
    fn now_ns(&self) -> i64 {
        SystemClock.now_ns()
    }
}

/// The signals this server ingests, and therefore maintains, today. Metrics
/// (RSEG) and logs (RLOG) both flow through the same signal-generic
/// compaction/retention/sweep code (ADR-0032).
const MAINTAINED_SIGNALS: [Signal; 3] = [Signal::Metrics, Signal::Logs, Signal::Spans];

/// Everything the maintenance task needs beyond the store and the tenant list.
#[derive(Debug, Clone)]
pub struct MaintenanceTaskConfig {
    pub enabled: bool,
    pub interval: Duration,
    pub shard_count: u32,
    /// Compactor knobs (seal margin, part cap, grace, protection horizon).
    /// `dry_run` is always false for the running service; only the CLI's
    /// `--dry-run` sets it.
    pub compactor: CompactorConfig,
    /// Validated per-tenant retention windows (ADR-0019). `RetentionConfig`'s
    /// default is "no retention", so with no `--retention-*` flags this task
    /// compacts and sweeps but never age-deletes.
    pub retention: RetentionConfig,
}

impl Default for MaintenanceTaskConfig {
    fn default() -> Self {
        MaintenanceTaskConfig {
            enabled: false,
            interval: DEFAULT_MAINTAIN_INTERVAL,
            shard_count: 4,
            compactor: CompactorConfig::default(),
            retention: RetentionConfig::default(),
        }
    }
}

/// Handle to every spawned maintenance task, for clean shutdown (mirrors
/// [`crate::fold::FoldTasks`]).
pub struct MaintenanceTasks {
    shutdown: Vec<oneshot::Sender<()>>,
    handles: Vec<JoinHandle<()>>,
}

impl MaintenanceTasks {
    pub fn none() -> Self {
        MaintenanceTasks {
            shutdown: Vec::new(),
            handles: Vec::new(),
        }
    }

    pub async fn shutdown(self) {
        for tx in self.shutdown {
            let _ = tx.send(());
        }
        for handle in self.handles {
            let _ = handle.await;
        }
    }
}

/// Spawn one supervisor task that re-discovers the tenant set from storage
/// every tick (ADR-0048 decision 3). `restrict` is the merged
/// `--tenant-token`/`--maintain-tenant` set: empty means unconfigured (every
/// discovered tenant is maintained), non-empty narrows the discovered set to
/// exactly those tenants. Returns immediately; the task runs until
/// [`MaintenanceTasks::shutdown`].
pub fn spawn(
    store: Arc<dyn ObjectStoreBackend>,
    restrict: Vec<TenantHash>,
    config: MaintenanceTaskConfig,
    metrics: Arc<TenantDiscoveryMetrics>,
) -> MaintenanceTasks {
    if !config.enabled {
        return MaintenanceTasks::none();
    }

    // One compactor writer_id per process start, shared across every tenant
    // this supervisor maintains (recorded in each L1 part's footer;
    // informational, never dedup-priority).
    let mut compactor = config.compactor.clone();
    compactor.compactor_writer_id = Uuid::new_v4();
    let compactor = Arc::new(compactor);
    let retention = Arc::new(config.retention.clone());
    let restrict = if restrict.is_empty() {
        None
    } else {
        Some(restrict)
    };

    let (tx, rx) = oneshot::channel();
    let interval = config.interval;
    let shard_count = config.shard_count;
    let handle = tokio::spawn(async move {
        run_loop(
            store,
            restrict,
            compactor,
            retention,
            shard_count,
            interval,
            metrics,
            rx,
        )
        .await;
    });
    MaintenanceTasks {
        shutdown: vec![tx],
        handles: vec![handle],
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    store: Arc<dyn ObjectStoreBackend>,
    restrict: Option<Vec<TenantHash>>,
    compactor: Arc<CompactorConfig>,
    retention: Arc<RetentionConfig>,
    shard_count: u32,
    interval: Duration,
    metrics: Arc<TenantDiscoveryMetrics>,
    mut shutdown: oneshot::Receiver<()>,
) {
    // One memo for the whole process, held across every tick and every
    // discovered tenant until shutdown (issue #280, #330). Its key includes
    // the tenant and signal, so this single instance safely spans every
    // tenant this supervisor discovers. Cold on the first tick, so that tick
    // is a full rescan identical to the pre-memo behavior.
    let mut memo = MaintainMemo::with_default_interval();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval)) => {}
            _ = &mut shutdown => return,
        }
        run_discovery_cycle(
            store.as_ref(),
            restrict.as_deref(),
            &compactor,
            &retention,
            shard_count,
            &mut memo,
            metrics.as_ref(),
        )
        .await;
    }
}

/// One discovery cycle: re-enumerate tenants from storage, narrow to
/// `restrict` when configured, then run [`run_tick`] for each tenant in the
/// result (ADR-0048 decision 3, issue #504). `metrics` records the discovered
/// and maintained gauges on success; a discovery failure -- the LIST itself
/// erroring -- skips the whole cycle (no tenant's tick runs) and only bumps
/// the failure counter, never falling back to an empty set. Falling back
/// would render identically to "storage has no tenants," the exact silent
/// failure findings S2-17/S5-09 describe.
#[allow(clippy::too_many_arguments)]
pub async fn run_discovery_cycle(
    store: &dyn ObjectStoreBackend,
    restrict: Option<&[TenantHash]>,
    compactor: &CompactorConfig,
    retention: &RetentionConfig,
    shard_count: u32,
    memo: &mut MaintainMemo,
    metrics: &TenantDiscoveryMetrics,
) -> MaintainReport {
    let outcome = match discover_and_restrict(store, restrict).await {
        Ok(outcome) => outcome,
        Err(err) => {
            tracing::error!(
                error = %err,
                "maintenance: tenant discovery failed; skipping this cycle entirely, retried next cycle"
            );
            metrics.record_discovery_failure();
            return MaintainReport::default();
        }
    };

    metrics.record_discovery(outcome.discovered.len(), outcome.maintained.len());
    if outcome.excluded > 0 {
        tracing::info!(
            excluded = outcome.excluded,
            "maintenance: flag restriction excluded discovered tenants holding data"
        );
    }

    let mut total = MaintainReport::default();
    for tenant in &outcome.maintained {
        let report = run_tick(store, tenant, compactor, retention, shard_count, memo).await;
        total.retired += report.retired;
        total.compacted += report.compacted;
        total.already_done += report.already_done;
        total.not_sealed += report.not_sealed;
        total.skipped_terminal += report.skipped_terminal;
    }
    total
}

/// One maintenance pass over every `(signal, shard)` of one tenant: a legal
/// hold refresh, then retention before compaction (via
/// [`scan_and_maintain_with_memo`]), then the GC sweeper (via
/// [`sweep_shard`]). Every scan/sweep error is logged and retried next tick;
/// nothing here affects query correctness. Split out from [`run_discovery_cycle`]
/// so a test can drive a single deterministic tenant tick without discovery or
/// the timer.
///
/// The legal hold refresh (ADR-0048 decision 1) runs once, before either
/// pass, and its snapshot gates every `(signal, shard)` of this tick. If the
/// refresh fails, the entire tenant tick is skipped -- no signal, no shard,
/// no pass runs -- and an empty [`MaintainReport`] is returned; the driver
/// never falls back to [`ravel_maintain::NoLeases`], because that would
/// convert a transient store fault into an unprotected delete pass. The
/// failure is logged at error level so it is visible to an operator, and the
/// tick is retried next interval.
///
/// `memo` is the caller's per-worker [`MaintainMemo`], threaded through every
/// `(signal, shard)` and mutated in place: buckets it already knows terminal
/// are skipped without a per-bucket LIST or GET (issue #280, #330). The
/// returned [`MaintainReport`] sums the per-`(signal, shard)` reports of the
/// retention-and-compaction passes (the sweep pass is logged, not summed);
/// `skipped_terminal` is the count of buckets the memo let this tick skip.
pub async fn run_tick(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    compactor: &CompactorConfig,
    retention: &RetentionConfig,
    shard_count: u32,
    memo: &mut MaintainMemo,
) -> MaintainReport {
    let clock = WallClock;

    let hold = match LegalHoldCheck::refresh(store, tenant).await {
        Ok(hold) => hold,
        Err(err) => {
            tracing::error!(
                tenant = %tenant.to_hex(),
                error = %err,
                "maintenance: legal hold refresh failed; skipping this tenant's tick entirely, retried next tick"
            );
            return MaintainReport::default();
        }
    };

    let mut total = MaintainReport::default();
    for signal in MAINTAINED_SIGNALS {
        for shard in 0..shard_count {
            match scan_and_maintain_with_memo(
                memo, store, &clock, compactor, retention, &hold, *tenant, signal, shard,
            )
            .await
            {
                Ok(report) => {
                    tracing::info!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        retired = report.retired,
                        compacted = report.compacted,
                        already_done = report.already_done,
                        not_sealed = report.not_sealed,
                        skipped_terminal = report.skipped_terminal,
                        "maintenance: retention + compaction pass complete"
                    );
                    total.retired += report.retired;
                    total.compacted += report.compacted;
                    total.already_done += report.already_done;
                    total.not_sealed += report.not_sealed;
                    total.skipped_terminal += report.skipped_terminal;
                }
                Err(err) => {
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        error = %err,
                        "maintenance: retention/compaction pass failed; retried next tick"
                    );
                }
            }

            match sweep_shard(store, &clock, compactor, &hold, tenant, signal, shard).await {
                Ok(report) => {
                    tracing::info!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        orphans = report.orphans_deleted,
                        superseded_records = report.superseded_records_deleted,
                        superseded_data = report.superseded_data_deleted,
                        unreferenced_parts = report.unreferenced_parts_deleted,
                        "maintenance: sweep pass complete"
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        tenant = %tenant.to_hex(),
                        signal = ?signal,
                        shard,
                        error = %err,
                        "maintenance: sweep pass failed; retried next tick"
                    );
                }
            }
        }
    }
    total
}

/// Up to 10% jitter over `base`, so co-started replicas' maintenance ticks do
/// not run in lockstep (same rationale as the fold task's jitter).
fn jittered(base: Duration) -> Duration {
    let jitter_bound_ms = u64::try_from(base.as_millis() / 10).unwrap_or(u64::MAX);
    if jitter_bound_ms == 0 {
        return base;
    }
    let extra_ms = rand::rng().random_range(0..=jitter_bound_ms);
    base + Duration::from_millis(extra_ms)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_maintain::{AUDIT_HOLD_SHARD, RetentionPolicy, shard_hold_scopes, write_hold_set};
    use ravel_object_store::PutOptions;
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
    use ravel_object_store::instrument::{InstrumentedStore, StoreMetricsSnapshot};
    use ravel_object_store::list_all;
    use ravel_object_store::memory::MemoryStore;
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

    /// The retention floor is validated against the catalog's max_ingest_lag,
    /// which must equal ravel-maintain's own DEFAULT_MAX_INGEST_LAG_NS: the two
    /// crates duplicate the constant behind a sync-contract comment (no
    /// ravel-maintain -> ravel-catalog dependency), so if either ever drifts
    /// this test fails and the retention floor would silently be validated
    /// against a different lag than the catalog resolves with.
    #[test]
    fn catalog_and_maintain_ingest_lag_agree() {
        assert_eq!(
            ravel_catalog::CatalogConfig::default().max_ingest_lag_ns,
            ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS,
            "catalog and maintain max_ingest_lag drifted; the retention floor \
             would be validated against a different lag than the catalog uses"
        );
    }

    /// One maintenance tick over an empty store touches every (signal, shard)
    /// and returns cleanly: it proves the driver actually walks both signals
    /// and all shards and calls scan_and_maintain + sweep_shard for each,
    /// without needing seeded segments (the underlying functions are tested in
    /// ravel-maintain).
    #[tokio::test]
    async fn run_tick_over_empty_store_is_clean() {
        let store = MemoryStore::new();
        let tenant = TenantId::new("acme").hash();
        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let report = run_tick(&store, &tenant, &compactor, &retention, 4, &mut memo).await;
        // Nothing to maintain, nothing memoized: a subsequent tick would still
        // find nothing to skip.
        assert_eq!(report, MaintainReport::default());
        assert!(memo.is_empty());
    }

    /// Publish one real sealed segment plus its commit record into a past ingest
    /// hour of `(tenant, Metrics, shard 0)`. One input is below the default
    /// `min_compaction_inputs`, and with no retention policy the bucket stays
    /// live, so maintenance classifies it terminal (below-threshold): exactly
    /// the steady state the memo is meant to skip. The ingest hour is 0 (1970)
    /// so the real [`WallClock`] `run_tick` uses always sees it as sealed.
    async fn publish_terminal_bucket(store: &dyn ObjectStoreBackend, tenant: &TenantId) {
        let tenant_hash = tenant.hash();
        let metric = "up";
        let label_set = LabelSet::new(vec![Label {
            name: "__name__".to_string(),
            value: metric.to_string(),
        }])
        .expect("valid labels");
        let series = vec![SeriesInput {
            series_id: SeriesId::compute(tenant, metric, &label_set).expect("series id"),
            labels: label_set,
            samples: vec![Sample {
                ts_ns: 1_000,
                value: 1.0,
            }],
        }];
        let writer_id = Uuid::from_u128(7_000);
        let identity = SegmentIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: writer_id.to_string(),
            writer_epoch: 1,
            writer_seq: 1,
        };
        let written = SegmentWriter::write(
            series,
            identity,
            IngestBounds {
                min_ingest_ts_ns: 0,
                max_ingest_ts_ns: 0,
            },
        )
        .expect("write segment");

        let rec = record::build(NewCommitRecord {
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
            created_unix_ns: 10,
            ingest_hour_bucket: 0,
        })
        .expect("valid commit record");

        let data_key = keys::reconstruct_data_key(&rec).expect("data key");
        store
            .put(&data_key, written.bytes, PutOptions::default())
            .await
            .expect("put data object");
        publish::publish(store, &rec, &RetryPolicy::default())
            .await
            .expect("publish");
    }

    /// A second `run_tick` with the same memo (a second tick) skips the buckets
    /// the first tick proved terminal: `skipped_terminal` rises from 0 to the
    /// bucket count, and the second tick issues strictly fewer GETs because the
    /// skipped bucket's per-bucket LIST/GET reads are elided (issue #280, #330).
    #[tokio::test]
    async fn second_tick_with_shared_memo_skips_terminal_buckets() {
        let store = InstrumentedStore::new(MemoryStore::new());
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_terminal_bucket(&store, &tenant_id).await;

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();

        // Per-bucket object reads: the memo elides the per-bucket LIST and GET
        // reads, so this is what shrinks between the cold and warm ticks. The
        // shard-level `list_delimited` runs on every tick and is excluded.
        let per_bucket_reads = |s: &StoreMetricsSnapshot| -> u64 { s.list.calls + s.get.calls };

        // Tick 1 (cold memo): full evaluation, nothing skipped. The single-input
        // bucket is below the compaction threshold, so it is already-done and
        // gets memoized as terminal.
        let before_first = store.metrics().snapshot();
        let first = run_tick(&store, &tenant, &compactor, &retention, 1, &mut memo).await;
        let first_reads =
            per_bucket_reads(&store.metrics().snapshot()) - per_bucket_reads(&before_first);
        assert_eq!(first.skipped_terminal, 0, "cold memo skips nothing");
        assert_eq!(first.already_done, 1, "the below-threshold bucket is done");
        assert_eq!(memo.len(), 1, "the terminal bucket is memoized");
        assert!(first_reads > 0, "cold tick did per-bucket reads");

        // Tick 2 (warm memo): the bucket is skipped straight from the memo.
        let before_second = store.metrics().snapshot();
        let second = run_tick(&store, &tenant, &compactor, &retention, 1, &mut memo).await;
        let second_reads =
            per_bucket_reads(&store.metrics().snapshot()) - per_bucket_reads(&before_second);
        assert_eq!(second.skipped_terminal, 1, "warm memo skips the bucket");
        assert_eq!(second.already_done, 0, "no per-bucket work redone");
        assert!(
            second_reads < first_reads,
            "the skipped tick reads fewer objects (first={first_reads}, second={second_reads})"
        );
    }

    /// ADR-0048 decision 1 / ADR-0042: a legal hold covering a bucket stops
    /// the real driver's retention path from ever physically deleting it.
    ///
    /// Retention's physical delete is horizon-gated (`retention_sweep_bucket`
    /// only sweeps once `now >= tombstone.retired_at_ns + protection_horizon_ns`),
    /// and a tombstone write itself is not lease-gated (only the physical
    /// delete is), so reaching the delete path this driver actually guards
    /// takes two ticks even with a zero horizon: the first tombstones the
    /// already-expired bucket, the second attempts the now horizon-elapsed
    /// physical sweep. The hold must block every delete in that second tick.
    #[tokio::test]
    async fn held_bucket_survives_retention_tick() {
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_terminal_bucket(&store, &tenant_id).await;

        // Cover all three prefixes a shard-level hold must (L0 data, commit
        // records, L1 parts), exactly as the CLI's --signal/--shard
        // convenience form does, so nothing in this bucket is left
        // unprotected.
        for scope in shard_hold_scopes(&tenant, Signal::Metrics, 0).expect("valid hold scopes") {
            write_hold_set(
                &store,
                &tenant,
                Uuid::new_v4(),
                SystemClock.now_ns(),
                &scope,
                "held for held_bucket_survives_retention_tick",
            )
            .await
            .expect("write hold set");
        }

        let compactor = CompactorConfig {
            protection_horizon_ns: 0,
            ..CompactorConfig::default()
        };
        let max_ingest_lag_ns = ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
        let floor_ns = compactor.retention_floor_ns(max_ingest_lag_ns);
        let retention = RetentionConfig::from_policy(
            RetentionPolicy {
                default: Some(floor_ns),
                tenants: Vec::new(),
            },
            &compactor,
            max_ingest_lag_ns,
        )
        .expect("valid retention policy");
        let mut memo = MaintainMemo::with_default_interval();

        // Tick 1: the bucket's one sample is from ingest hour 0 (1970), so any
        // valid retention window is already expired against the real wall
        // clock. Not memoized terminal (Tombstoned isn't a terminal state),
        // so tick 2 re-evaluates it for real rather than skipping it.
        let first = run_tick(&store, &tenant, &compactor, &retention, 1, &mut memo).await;
        assert_eq!(first.retired, 1, "the expired bucket is tombstoned");

        // Tick 2: the tombstone's horizon has elapsed (zero protection
        // horizon), so this tick attempts the physical sweep. The hold must
        // block it entirely.
        let second = run_tick(&store, &tenant, &compactor, &retention, 1, &mut memo).await;
        assert_eq!(
            second.retired, 1,
            "still counted retired (tombstoned), never actually swept"
        );

        let l0_prefix = &shard_hold_scopes(&tenant, Signal::Metrics, 0).expect("scopes")[0];
        let surviving = list_all(&store, l0_prefix)
            .await
            .expect("list held l0 prefix");
        assert!(
            !surviving.is_empty(),
            "held bucket's L0 data object must still exist after a retention tick"
        );

        let commit_prefix = keys::commit_shard_prefix(&tenant, Signal::Metrics, 0)
            .expect("valid commit shard prefix");
        let surviving_commit = list_all(&store, &commit_prefix)
            .await
            .expect("list held commit prefix");
        assert!(
            surviving_commit
                .iter()
                .any(|meta| keys::partition_bucket_entry(&meta.key)
                    .is_ok_and(|entry| matches!(entry, keys::BucketEntry::CommitRecord(_)))),
            "held bucket's commit record must still exist after a retention tick"
        );
    }

    /// ADR-0048 decision 1: a legal-hold refresh failure must never fall back
    /// to `NoLeases`. It must skip the whole tenant tick -- no bucket
    /// touched, nothing deleted -- and the failure must be visible (not
    /// swallowed). Uses `FaultStore` to fail the one LIST `LegalHoldCheck::refresh`
    /// issues (the audit hold shard's commit prefix) and asserts its fault
    /// counter to prove the fault actually fired, not just that nothing
    /// happened to be due for deletion.
    #[tokio::test]
    async fn hold_refresh_failure_skips_tenant_tick_and_deletes_nothing() {
        let inner = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();
        publish_terminal_bucket(&inner, &tenant_id).await;

        let hold_prefix = keys::commit_shard_prefix(&tenant, Signal::Audit, AUDIT_HOLD_SHARD)
            .expect("valid audit hold shard prefix");
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::List,
                ScriptedFault::Transient("hold shard unavailable".into()),
            )
            .with_key_contains(hold_prefix),
        );
        let store = FaultStore::new(inner, plan);

        let compactor = CompactorConfig {
            protection_horizon_ns: 0,
            ..CompactorConfig::default()
        };
        let max_ingest_lag_ns = ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
        let floor_ns = compactor.retention_floor_ns(max_ingest_lag_ns);
        let retention = RetentionConfig::from_policy(
            RetentionPolicy {
                default: Some(floor_ns),
                tenants: Vec::new(),
            },
            &compactor,
            max_ingest_lag_ns,
        )
        .expect("valid retention policy");
        let mut memo = MaintainMemo::with_default_interval();

        let report = run_tick(&store, &tenant, &compactor, &retention, 1, &mut memo).await;

        assert_eq!(
            report,
            MaintainReport::default(),
            "a refresh failure must skip the whole tick, not just gate deletes"
        );
        assert_eq!(
            store.fault_count(Op::List, ravel_object_store::fault::FaultKind::Transient),
            1,
            "the injected refresh fault must actually have fired"
        );
        assert!(memo.is_empty(), "a skipped tick memoizes nothing");

        let commit_prefix = keys::commit_shard_prefix(&tenant, Signal::Metrics, 0)
            .expect("valid commit shard prefix");
        let surviving = list_all(store.inner(), &commit_prefix)
            .await
            .expect("list commit prefix");
        assert!(
            !surviving.is_empty(),
            "the tenant's bucket must be untouched when the hold refresh fails"
        );
    }

    /// The test the task spec requires by name: a tenant known only to
    /// storage (no `--tenant-token`, no `--maintain-tenant`, i.e. `restrict =
    /// None`) is discovered and maintained by the real driver wiring
    /// (ADR-0048 decision 3, issue #504, experiment L2). This is exactly the
    /// OIDC/mTLS-authenticated-tenant scenario findings S2-17/S5-09
    /// describe: the flag-derived set used to be empty and nothing ran.
    #[tokio::test]
    async fn storage_discovered_tenant_is_maintained_without_flags() {
        let store = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        publish_terminal_bucket(&store, &tenant_id).await;

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let metrics = TenantDiscoveryMetrics::default();

        let report =
            run_discovery_cycle(&store, None, &compactor, &retention, 1, &mut memo, &metrics).await;

        assert_eq!(
            report.already_done, 1,
            "the storage-discovered tenant's bucket must actually be evaluated"
        );
        assert_eq!(metrics.tenants_discovered(), 1);
        assert_eq!(metrics.tenants_maintained(), 1);
        assert_eq!(metrics.discovery_failures(), 0);
    }

    /// A flag restriction narrows the discovered set: a discovered tenant not
    /// named by `--tenant-token`/`--maintain-tenant` is excluded from the
    /// cycle (never maintained) and counted, rather than either running
    /// unconditionally or being indistinguishable from "storage didn't report
    /// it."
    #[tokio::test]
    async fn flag_restriction_excludes_a_discovered_tenant_and_counts_it() {
        let store = MemoryStore::new();
        let acme = TenantId::new("acme");
        let globex = TenantId::new("globex");
        publish_terminal_bucket(&store, &acme).await;
        publish_terminal_bucket(&store, &globex).await;

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let metrics = TenantDiscoveryMetrics::default();
        let restrict = [acme.hash()];

        let report = run_discovery_cycle(
            &store,
            Some(&restrict),
            &compactor,
            &retention,
            1,
            &mut memo,
            &metrics,
        )
        .await;

        assert_eq!(
            report.already_done, 1,
            "only the restricted-in tenant's bucket is evaluated"
        );
        assert_eq!(metrics.tenants_discovered(), 2, "both tenants hold data");
        assert_eq!(
            metrics.tenants_maintained(),
            1,
            "only the restriction-named tenant is maintained"
        );
        assert_eq!(
            memo.len(),
            1,
            "only the maintained tenant's bucket is memoized"
        );
    }

    /// ADR-0048 decision 3: a tenant discovery failure (the `list_delimited("t/")`
    /// LIST erroring) must skip the entire cycle -- no tenant's tick runs --
    /// and must never fall back to an empty tenant set and report success.
    /// Uses `FaultStore` to fail the discovery LIST specifically and asserts
    /// its fault counter to prove the fault actually fired, not just that
    /// there happened to be nothing to do.
    #[tokio::test]
    async fn discovery_failure_skips_cycle_without_running_empty_set() {
        let inner = MemoryStore::new();
        let tenant_id = TenantId::new("acme");
        publish_terminal_bucket(&inner, &tenant_id).await;

        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::List,
                ScriptedFault::Transient("tenant discovery unavailable".into()),
            )
            .with_key_contains("t/"),
        );
        let store = FaultStore::new(inner, plan);

        let compactor = CompactorConfig::default();
        let retention = RetentionConfig::default();
        let mut memo = MaintainMemo::with_default_interval();
        let metrics = TenantDiscoveryMetrics::default();

        let report =
            run_discovery_cycle(&store, None, &compactor, &retention, 1, &mut memo, &metrics).await;

        assert_eq!(
            report,
            MaintainReport::default(),
            "a discovery failure must skip the whole cycle, never run an empty set"
        );
        assert_eq!(
            store.fault_count(Op::List, ravel_object_store::fault::FaultKind::Transient),
            1,
            "the injected discovery fault must actually have fired"
        );
        assert_eq!(metrics.discovery_failures(), 1);
        assert_eq!(
            metrics.tenants_discovered(),
            0,
            "gauges stay at their last known-good value, never reporting this failed cycle"
        );
        assert!(memo.is_empty(), "a skipped cycle memoizes nothing");
    }
}
