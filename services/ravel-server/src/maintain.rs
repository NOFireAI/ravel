//! Per-tenant background maintenance task (docs/compaction-retention-plan.md
//! P8, issue #115). Periodically runs age-based retention, L0->L1 compaction,
//! and the GC sweeper over every `(signal, shard)` of each tenant, mirroring
//! the shape of the fold task (`crate::fold`): one loop per tenant from the
//! static tenant-token config, a config struct, and a handle with clean
//! shutdown.
//!
//! Unlike fold (a pure query-cost optimization), this task deletes and rewrites
//! durable objects, but it changes nothing about *what* any sweep, retention,
//! or compaction rule decides: it is only the driver that calls
//! [`scan_and_maintain_with_memo`] (retention-before-compaction over every
//! sealed bucket) and [`ravel_maintain::sweep_shard`] (the three GC rules) once
//! per tick. Both are idempotent, so a missed or crashed tick is recovered on
//! the next one. The clock is the real [`SystemClock`], matching everything
//! else in this crate, and the only [`LeaseCheck`] that exists ([`NoLeases`])
//! is used.
//!
//! [`run_loop`] holds one [`MaintainMemo`] across every tick until shutdown
//! (issue #280, #330). The memo records buckets already known terminal so a
//! steady-state tick skips re-listing and re-reading them, until a periodic
//! full re-verify forces a fresh evaluation. It is ephemeral and never
//! correctness-bearing: a fresh (cold) memo on the first tick after a worker
//! start does exactly one full rescan identical to the pre-memo behavior, and a
//! wrong or lost entry only defers work by at most the re-verify interval. The
//! memo key is `(tenant, signal, shard, hour)`, so one memo per worker spans
//! every `(signal, shard)` of the one tenant this loop maintains.

use std::sync::Arc;
use std::time::Duration;

use rand::RngExt as _;
use ravel_ingest::{Clock as _, SystemClock};
use ravel_maintain::scan::{MaintainMemo, MaintainReport, scan_and_maintain_with_memo};
use ravel_maintain::{Clock, CompactorConfig, NoLeases, RetentionConfig, sweep_shard};
use ravel_object_store::ObjectStoreBackend;
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

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

/// Spawn one maintenance loop per tenant. Tenants come from the static
/// tenant-token config, the same list [`crate::fold::spawn`] uses. Returns
/// immediately; tasks run until [`MaintenanceTasks::shutdown`].
pub fn spawn(
    store: Arc<dyn ObjectStoreBackend>,
    tenants: &[TenantHash],
    config: MaintenanceTaskConfig,
) -> MaintenanceTasks {
    if !config.enabled || tenants.is_empty() {
        return MaintenanceTasks::none();
    }

    // One compactor writer_id per process start, shared by every tenant task
    // (recorded in each L1 part's footer; informational, never dedup-priority).
    let mut compactor = config.compactor.clone();
    compactor.compactor_writer_id = Uuid::new_v4();
    let compactor = Arc::new(compactor);
    let retention = Arc::new(config.retention.clone());

    let mut shutdown = Vec::new();
    let mut handles = Vec::new();
    for &tenant in tenants {
        let (tx, rx) = oneshot::channel();
        let store = store.clone();
        let compactor = compactor.clone();
        let retention = retention.clone();
        let interval = config.interval;
        let shard_count = config.shard_count;
        let handle = tokio::spawn(async move {
            run_loop(
                store,
                tenant,
                compactor,
                retention,
                shard_count,
                interval,
                rx,
            )
            .await;
        });
        shutdown.push(tx);
        handles.push(handle);
    }
    MaintenanceTasks { shutdown, handles }
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: TenantHash,
    compactor: Arc<CompactorConfig>,
    retention: Arc<RetentionConfig>,
    shard_count: u32,
    interval: Duration,
    mut shutdown: oneshot::Receiver<()>,
) {
    // One memo per worker, held across every tick until shutdown (issue #280,
    // #330). Its key includes the tenant and signal, so this single instance
    // safely spans every (signal, shard) this loop maintains. Cold on the first
    // tick, so that tick is a full rescan identical to the pre-memo behavior.
    let mut memo = MaintainMemo::with_default_interval();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval)) => {}
            _ = &mut shutdown => return,
        }
        run_tick(
            store.as_ref(),
            &tenant,
            &compactor,
            &retention,
            shard_count,
            &mut memo,
        )
        .await;
    }
}

/// One maintenance pass over every `(signal, shard)` of one tenant: retention
/// before compaction (via [`scan_and_maintain_with_memo`]) then the GC sweeper
/// (via [`sweep_shard`]). Every error is logged and retried next tick; nothing
/// here affects query correctness. Split out from [`run_loop`] so a test can
/// drive a single deterministic tick without the timer.
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
    let mut total = MaintainReport::default();
    for signal in MAINTAINED_SIGNALS {
        for shard in 0..shard_count {
            match scan_and_maintain_with_memo(
                memo, store, &clock, compactor, retention, &NoLeases, *tenant, signal, shard,
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

            match sweep_shard(store, &clock, compactor, &NoLeases, tenant, signal, shard).await {
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
    use ravel_object_store::PutOptions;
    use ravel_object_store::instrument::{InstrumentedStore, StoreMetricsSnapshot};
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
}
