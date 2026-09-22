//! Per-signal background catalog fold task (ADR-0020; storage-derived tenant
//! set is ADR-0048 decision 3). Periodically calls [`Catalog::fold`] so query resolve can
//! serve sealed history from snapshots instead of full listing.
//!
//! Never runs on the ingest or query path, and never affects correctness:
//! every failure here is logged and retried on the next tick. Disabling this
//! task (`--disable-fold`) only changes query cost, never query results.
//!
//! One loop per [`FOLD_SIGNALS`] entry, not one per tenant: each tick
//! re-enumerates tenants from storage
//! ([`crate::tenant_discovery::discover_and_restrict_by_lifecycle`]) and folds
//! every tenant the cycle discovers, narrowed by each tenant's durable
//! lifecycle state and the flag fallback (ADR-0066 decision 6: a config record
//! keeps a tenant in the set unconditionally regardless of its token, and no
//! flag can exclude a config-recorded tenant). A tenant onboarded mid-run is
//! folded starting the next tick with no restart.
//! A discovery failure skips that signal's whole cycle -- no tenant is folded
//! -- and is retried next tick; it never falls back to an empty set, since
//! fold is best-effort and a quiet failure here would look identical to
//! "nothing new to fold."

use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{Catalog, FoldReport, RefoldRequest};
use ravel_commit::rng::{RngSource, SystemRng};
use ravel_ingest::{Clock, SystemClock};
use ravel_maintain::{CompactorConfig, NoLeases, RetentionConfig};
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_types::{Signal, TenantHash};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::tenant_discovery::discover_and_restrict_by_lifecycle;

/// The tick's own instant, as the [`ravel_maintain::Clock`] the sweep-side
/// derivation takes. Every fold entry point already receives `now_ns` as a
/// parameter (CLAUDE.md testing patterns: time is injected), so the derivation
/// sees that instant rather than reading the wall clock a second time.
struct TickClock(i64);

impl ravel_maintain::Clock for TickClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

/// Default `fold_interval`: 5 minutes.
pub const DEFAULT_FOLD_INTERVAL: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Copy)]
pub struct FoldTaskConfig {
    pub enabled: bool,
    pub fold_interval: Duration,
}

impl Default for FoldTaskConfig {
    fn default() -> Self {
        FoldTaskConfig {
            enabled: true,
            fold_interval: DEFAULT_FOLD_INTERVAL,
        }
    }
}

/// Handle to every spawned fold task, so shutdown can stop them cleanly
/// (mirrors [`crate::Running`]'s listener shutdown handles).
pub struct FoldTasks {
    shutdown: Vec<oneshot::Sender<()>>,
    handles: Vec<JoinHandle<()>>,
}

impl FoldTasks {
    pub fn none() -> Self {
        FoldTasks {
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

/// Every signal `ravel-server` folds. One fold loop is spawned per
/// (tenant, signal) pair, so adding a signal here adds one loop per tenant
/// without changing any loop's shape. `FoldTaskConfig` (enabled,
/// fold_interval) is shared across signals for v1: all currently want the
/// same 5-minute cadence (ADR-0033); a per-signal interval is a config-shape
/// follow-up if that changes.
///
/// [`Signal::Spans`] is added once span compaction exists (ADR-0041 phase 3):
/// span buckets now produce L1 `.rspan` parts the resolver serves from
/// snapshots, exactly as logs do. Spans fold through the same
/// [`Catalog::fold`] path as logs and share the same accepted no-op-postings
/// cost described below (an `.rspan` object carries signal=3, so the
/// RSEG-specific postings build fails to decode it and skips writing a
/// postings ref, same as logs' signal=2).
///
/// `pub(crate)` so `crate::metrics` renders exactly one fold-liveness series
/// per entry: the series set at `/metrics` is then the set of loops that are
/// supposed to be alive, and a loop that dies leaves its own series standing
/// and going stale rather than hiding behind its siblings.
pub(crate) const FOLD_SIGNALS: [Signal; 3] = [Signal::Metrics, Signal::Logs, Signal::Spans];

/// Spawns one fold loop per signal in [`FOLD_SIGNALS`], not one per tenant:
/// each tick re-derives the tenant set from storage. [`run_loop`] is
/// signal-generic; a new signal is added by extending that array, not by
/// restructuring this function (ADR-0033 gap 1; folding is per
/// (tenant, signal) throughout).
///
/// [`Signal::Logs`] folds through the same [`Catalog::fold`] path as metrics
/// and produces `catalog/l/HEAD` plus snapshot parts, but no name-postings
/// object: `Catalog::fold` always attempts the RSEG-specific postings build
/// (`build_postings`/`fetch_entry_names`), which for a log entry issues one
/// full-object GET and then fails to decode the bytes as RSEG (an RLOG object
/// carries signal=2, RSEG expects signal=1), so `build_postings` returns
/// `None` and the fold skips writing a postings ref without failing. That
/// wasted GET-plus-failed-decode recurs every fold cycle for each log entry
/// newly covered since the last fold: real I/O and CPU proportional to new
/// log volume. It is accepted for v1 (ADR-0033), not a bug; fixing it would
/// mean a signal-aware short-circuit inside `ravel-catalog`, deliberately out
/// of scope here.
///
/// `fallback_allow` is the merged static tenant set (`--tenant-token` or
/// `--tenant-token-file`, plus `--maintain-tenant`):
/// empty means unconfigured, and it otherwise governs only tenants with no
/// durable config record (ADR-0048 decision 3, ADR-0066 decision 6). A tenant
/// carrying a config record is maintained unconditionally, so no flag can
/// exclude it. Returns immediately; tasks run in the background until
/// [`FoldTasks::shutdown`].
///
/// `retention` is the same CLI-derived [`RetentionConfig`] the Maintain-mode
/// physical sweep uses (`main.rs`, threaded into `MaintenanceTaskConfig`).
/// Each tick resolves `retention.window_for(tenant)` per tenant and passes it
/// into [`Catalog::fold`] as the deployment-default retention window, so the
/// fold's retention-frontier reconcile runs for a tenant configured only by
/// CLI flags with no durable `TenantConfig.retention_ns` record (ADR-0078).
/// An unconfigured `RetentionConfig` resolves to `None` for every tenant, so
/// this is inert when no retention flag is set.
///
/// `compactor` is the same [`CompactorConfig`] the Maintain-mode sweep runs
/// under. Each tick derives this tenant's blocked ingest hours from the store
/// with it ([`derive_refold_request`]) and passes them to
/// [`Catalog::fold_with_refold_request`], so the targeted reconcile pass
/// re-reads hours that sit outside the fixed reconcile window.
pub fn spawn(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    fallback_allow: &[TenantHash],
    config: FoldTaskConfig,
    retention: Arc<RetentionConfig>,
    compactor: Arc<CompactorConfig>,
) -> FoldTasks {
    if !config.enabled {
        return FoldTasks::none();
    }

    // Production OS-entropy randomness (ADR-0068 decision 2): the folder id
    // and the per-tick loop jitter both draw from this one source instead of
    // `Uuid::new_v4()` / `rand::rng()` directly. The server always uses the
    // OS-entropy default; only the simulation harness injects a seeded source,
    // and it does not drive this loop.
    let rng: Arc<dyn RngSource> = Arc::new(SystemRng);
    // One folder_id per process start (proto/ravel/catalog.proto,
    // `SnapshotHead.folder_id`), shared by every signal loop in this process.
    let folder_id = rng.new_uuid();
    let fallback_allow = if fallback_allow.is_empty() {
        None
    } else {
        Some(fallback_allow.to_vec())
    };
    let mut shutdown = Vec::new();
    let mut handles = Vec::new();
    for signal in FOLD_SIGNALS {
        let (tx, rx) = oneshot::channel();
        let catalog = catalog.clone();
        let store = store.clone();
        let fallback_allow = fallback_allow.clone();
        let interval = config.fold_interval;
        let rng = Arc::clone(&rng);
        let retention = Arc::clone(&retention);
        let compactor = Arc::clone(&compactor);
        let handle = tokio::spawn(async move {
            run_loop(
                catalog,
                store,
                signal,
                fallback_allow,
                folder_id,
                interval,
                rng,
                retention,
                compactor,
                rx,
            )
            .await;
        });
        shutdown.push(tx);
        handles.push(handle);
    }
    FoldTasks { shutdown, handles }
}

#[allow(clippy::too_many_arguments)]
async fn run_loop(
    catalog: Arc<Catalog>,
    store: Arc<dyn ObjectStoreBackend>,
    signal: Signal,
    fallback_allow: Option<Vec<TenantHash>>,
    folder_id: Uuid,
    interval: Duration,
    rng: Arc<dyn RngSource>,
    retention: Arc<RetentionConfig>,
    compactor: Arc<CompactorConfig>,
    mut shutdown: oneshot::Receiver<()>,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(jittered(interval, rng.as_ref())) => {}
            _ = &mut shutdown => return,
        }

        let outcome = match discover_and_restrict_by_lifecycle(
            store.as_ref(),
            fallback_allow.as_deref(),
        )
        .await
        {
            Ok(outcome) => outcome,
            Err(err) => {
                tracing::error!(
                    signal = ?signal,
                    error = %err,
                    "catalog fold: tenant discovery failed; skipping this cycle entirely, retried next tick"
                );
                continue;
            }
        };
        if outcome.excluded > 0 {
            tracing::debug!(
                signal = ?signal,
                excluded = outcome.excluded,
                "catalog fold: flag restriction excluded discovered tenants"
            );
        }

        for tenant in outcome.maintained {
            // The deployment-default retention window for this tenant, resolved
            // per tick from the CLI-derived RetentionConfig (ADR-0078). The fold
            // overlays the durable TenantConfig.retention_ns on top of it.
            let default_retention_ns = retention.window_for(&tenant);
            run_tenant_tick(
                catalog.as_ref(),
                store.as_ref(),
                compactor.as_ref(),
                &tenant,
                signal,
                folder_id,
                interval,
                SystemClock.now_ns(),
                default_retention_ns,
            )
            .await;
        }
    }
}

/// The ingest hours this tenant's fold should re-list: the hours whose live
/// catalog HEAD snapshot still names inputs a published compaction or rewrite
/// record superseded, so the ADR-0020 delete blocker holds those inputs
/// ([`ravel_maintain::SnapshotBlock::Named`]).
///
/// Derived here, from the store, at the fold's own tick. Nothing hands this
/// set to the fold: in every shipped topology a process runs EITHER the
/// maintain loop or the fold loop (`Mode::Maintain` in `crate::lib` spawns one
/// and passes the other's `none()` handle), so an in-process hand-off from the
/// sweep would never fire.
///
/// [`ravel_maintain::blocked_named_hours`] is the derivation, and it is the
/// same pass and the same `SnapshotBlock::Named` gate arm that fills
/// `SweepReport::blocked_named_hours` on the maintain side. The two cannot
/// drift on what "blocked" means because there is one function.
///
/// Cost, per tenant per fold tick: one LIST of the signal's commit prefix to
/// enumerate shards, then one LIST plus the record GETs per shard, with a
/// single HEAD read shared across all of them. It is paid only after the HEAD
/// freshness peek has decided this tick folds at all.
///
/// Best-effort, like every other part of this task: a derivation failure logs
/// and yields an empty request rather than skipping the fold. The hours stay
/// blocked in the store and the next tick derives them again.
async fn derive_refold_request(
    store: &dyn ObjectStoreBackend,
    compactor: &CompactorConfig,
    tenant: &TenantHash,
    signal: Signal,
    now_ns: i64,
) -> RefoldRequest {
    // `NoLeases`: this process holds no reader lease or legal hold of its own.
    // A group a real hold protects can therefore be derived here and not by
    // the sweep, which only ever adds an hour to re-list; see
    // `ravel_maintain::blocked_named_hours`.
    match ravel_maintain::blocked_named_hours(
        store,
        &TickClock(now_ns),
        compactor,
        &NoLeases,
        tenant,
        signal,
    )
    .await
    {
        Ok(hours) => RefoldRequest::from_hours(hours),
        Err(err) => {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                error = %err,
                "catalog fold: blocked-hour derivation failed; folding without a targeted \
                 re-fold pass this tick"
            );
            RefoldRequest::new()
        }
    }
}

/// One fold attempt for one tenant: the HEAD freshness peek, the blocked-hour
/// derivation, then [`Catalog::fold_with_refold_request`]. Split out from
/// [`run_loop`] so discovery and the per-tenant fold logic stay independently
/// readable.
///
/// The derivation runs after the freshness peek, never before: a HEAD another
/// replica folded within the last interval has already run its own targeted
/// pass over the same store state, and the LISTs would be spent on a fold that
/// does not happen.
///
/// `now_ns` is the caller's clock reading, taken once per tenant by
/// [`run_loop`] from [`SystemClock`]. Taking it as a parameter is what lets a
/// test fold at a determined instant (CLAUDE.md testing patterns: time is
/// injected) instead of at whatever the host's wall clock says, which for a
/// fixture built on low ingest hours decides whether anything is sealed at
/// all.
///
/// Returns the fold's own [`FoldReport`], or `None` when the freshness peek
/// skipped this tick or the fold itself failed. [`run_loop`] logs and
/// discards it; the re-fold tests assert
/// [`FoldReport::refold_hours_reconciled`] on it.
///
/// `pub(crate)` so those tests fold through this exact function, deriving
/// their own blocked hours, rather than calling the catalog directly.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_tenant_tick(
    catalog: &Catalog,
    store: &dyn ObjectStoreBackend,
    compactor: &CompactorConfig,
    tenant: &TenantHash,
    signal: Signal,
    folder_id: Uuid,
    interval: Duration,
    now_ns: i64,
    default_retention_ns: Option<i64>,
) -> Option<FoldReport> {
    if head_fresh_enough(store, tenant, signal, interval, now_ns).await {
        tracing::debug!(
            tenant = %tenant.to_hex(),
            signal = ?signal,
            "catalog fold: HEAD already fresh, skipping this tick"
        );
        return None;
    }

    let refold_request = derive_refold_request(store, compactor, tenant, signal, now_ns).await;

    match catalog
        .fold_with_refold_request(
            tenant,
            signal,
            folder_id,
            now_ns,
            &[],
            default_retention_ns,
            &refold_request,
        )
        .await
    {
        Ok(report) => {
            tracing::info!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                no_op = report.no_op,
                rebuilt = report.rebuilt,
                watermark_hour = ?report.watermark_hour,
                previous_watermark_hour = ?report.previous_watermark_hour,
                buckets_folded = report.buckets_folded,
                entry_count = report.entry_count,
                part_bytes = report.part_bytes,
                list_requests = report.list_requests,
                get_requests = report.get_requests,
                put_requests = report.put_requests,
                refold_hours_requested = refold_request.len(),
                refold_hours_reconciled = report.refold_hours_reconciled,
                "catalog fold complete"
            );
            Some(report)
        }
        Err(err) => {
            tracing::warn!(
                tenant = %tenant.to_hex(),
                signal = ?signal,
                error = %err,
                "catalog fold failed; the index degrades to listing until a later fold succeeds"
            );
            None
        }
    }
}

/// Adds up to 10% jitter on top of `base`, so multiple replicas' fold tasks
/// (started at roughly the same time) don't tick in lockstep forever. Shared
/// with the store-reachability probe ([`crate::store_probe`]), which reuses this
/// single helper rather than growing a second copy of the same jitter rule.
pub(crate) fn jittered(base: Duration, rng: &dyn RngSource) -> Duration {
    let jitter_bound_ms = u64::try_from(base.as_millis() / 10).unwrap_or(u64::MAX);
    if jitter_bound_ms == 0 {
        return base;
    }
    let extra_ms = rng.jitter_ms(jitter_bound_ms);
    base + Duration::from_millis(extra_ms)
}

/// Cheap duplicate-work avoidance across replicas: peeks at HEAD directly
/// (bypassing the
/// catalog's own HEAD cache and fold logic) and skips this tick if another
/// replica folded within the last `interval`. Correctness never depends on
/// this: `catalog.fold` performs its own authoritative HEAD read and no-ops
/// safely if there is nothing new to fold, so a wrong (or racing) answer here
/// only costs a redundant fold attempt, never a missed one.
async fn head_fresh_enough(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantHash,
    signal: Signal,
    interval: Duration,
    now_ns: i64,
) -> bool {
    let key = head_key(tenant, signal);
    let Ok(got) = store.get(&key, GetRange::Full).await else {
        return false;
    };
    let Ok(head) = ravel_catalog::decode_head(&got.data) else {
        return false;
    };
    head_is_fresh(head.created_unix_ns, now_ns, interval)
}

/// Whether a HEAD published at `created_unix_ns` is younger than `interval` as
/// of `now_ns`. The freshness predicate behind [`head_fresh_enough`], factored
/// out so the on-demand route ([`crate::fold_on_demand`]) applies the same rule
/// to a HEAD it has already read rather than growing a second freshness notion.
/// A zero `interval` is never fresh (age is always `>= 0`), which is how a
/// caller opts out of the gate.
pub(crate) fn head_is_fresh(created_unix_ns: i64, now_ns: i64, interval: Duration) -> bool {
    let age_ns = now_ns.saturating_sub(created_unix_ns);
    age_ns >= 0 && (age_ns as u128) < interval.as_nanos()
}

/// HEAD object key (docs/catalog-and-mvcc.md key layout, frozen format):
/// `t/<tenant_hash>/catalog/<signal>/HEAD`. Reconstructed here rather than
/// imported because it names a `pub(crate)` helper inside `ravel-catalog`;
/// this task only ever uses it for the freshness peek above, never to
/// mutate the object, and so does
/// [`crate::fold_on_demand`], which shares this one spelling of the key
/// rather than growing a second copy.
pub(crate) fn head_key(tenant: &TenantHash, signal: Signal) -> String {
    format!("t/{}/catalog/{}/HEAD", tenant.to_hex(), signal.key_prefix())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::collections::BTreeSet;

    use ravel_commit::publish::RetryPolicy;
    use ravel_commit::record::NewCommitRecord;
    use ravel_commit::{keys, publish, record};
    use ravel_maintain::FixedClock;
    use ravel_object_store::PutOptions;
    use ravel_object_store::memory::MemoryStore;
    use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
    use ravel_types::{Label, LabelSet, Sample, SeriesId, TenantId};

    use super::*;
    use crate::maintain::MaintenanceSafetyMetrics;
    use crate::metrics::{
        AdmissionCountersSnapshot, CatalogCountersSnapshot, IngestBufferBudgetSnapshot,
        MaintenanceSafetySnapshot, MemoryBudgetSnapshot, render,
    };

    /// Real wall-clock nanoseconds per hour, the unit every ingest-hour-bucket
    /// computation in this crate shares.
    const TEST_NS_PER_HOUR: i64 = 3_600_000_000_000;

    /// The ingest hour whose superseded inputs the live snapshot still names,
    /// because its compaction record is published after the fold that built
    /// that snapshot. Rule 2 holds it on
    /// [`ravel_maintain::SnapshotBlock::Named`], and it is the only hour the
    /// derivation may report.
    const REFOLD_BLOCKED_HOUR: u32 = 5;
    /// An ingest hour the same pass also looks at and builds a
    /// superseded-input group in, whose compaction record was published BEFORE
    /// the fold: the snapshot names the L1 output, the gate clears, and the
    /// inputs are collectable. It must never appear in the derived set.
    const REFOLD_CLEARED_HOUR: u32 = 6;
    /// A later ingest hour with a single L0 input and no compaction, which is
    /// what the first fold's watermark lands on.
    const REFOLD_RECENT_HOUR: u32 = 40;

    /// `now_ns` at which ingest hour `hour` has just sealed under the default
    /// catalog margins, matching `ravel_catalog`'s own `now_at_seal` test
    /// helper.
    fn refold_now_at_seal(hour: u32) -> i64 {
        (i64::from(hour) + 1) * TEST_NS_PER_HOUR
            + ravel_catalog::DEFAULT_MAX_FLUSH_LIFETIME_NS
            + ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
            + ravel_catalog::DEFAULT_FOLD_SAFETY_MARGIN_NS
    }

    fn subject_labels() -> LabelSet {
        LabelSet::new(vec![
            Label {
                name: "__name__".to_string(),
                value: "http_requests".to_string(),
            },
            Label {
                name: "user_id".to_string(),
                value: "u999".to_string(),
            },
        ])
        .expect("valid labels")
    }

    /// Publish one single-series L0 segment and its commit record into
    /// `(shard 0, ingest_hour_bucket)`, timestamped inside that hour.
    async fn publish_refold_segment(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantId,
        writer_id: Uuid,
        ingest_hour_bucket: u32,
    ) -> ravel_proto::commit::v1::CommitRecord {
        let tenant_hash = tenant.hash();
        let ts_ns = i64::from(ingest_hour_bucket) * TEST_NS_PER_HOUR + 1_000;
        let labels = subject_labels();
        let series = vec![SeriesInput {
            series_id: SeriesId::compute(tenant, "http_requests", &labels).expect("series id"),
            labels,
            samples: vec![Sample { ts_ns, value: 1.0 }],
        }];
        let written = SegmentWriter::write(
            series,
            SegmentIdentity {
                tenant_hash: tenant_hash.0,
                shard: 0,
                writer_id: writer_id.to_string(),
                writer_epoch: 1,
                writer_seq: 1,
            },
            IngestBounds {
                min_ingest_ts_ns: ts_ns,
                max_ingest_ts_ns: ts_ns,
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
            min_ingest_ts_ns: ts_ns,
            max_ingest_ts_ns: ts_ns,
            segment_format_version: 1,
            created_unix_ns: ts_ns,
            ingest_hour_bucket,
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
        rec
    }

    /// Publish a level-1 compaction record over `inputs` into
    /// `(shard 0, ingest_hour_bucket)`, plus its single output part object.
    /// The part object is written because rule 3 keeps every part a live
    /// compaction record references, so its presence is what a later fold's
    /// snapshot entry points at.
    async fn publish_refold_compaction(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantId,
        ingest_hour_bucket: u32,
        inputs: &[&ravel_proto::commit::v1::CommitRecord],
    ) -> ravel_proto::commit::v1::CompactionRecord {
        use prost::Message as _;
        use ravel_proto::commit::v1::{CompactionInputIdentity, CompactionPart, CompactionRecord};

        let created_unix_ns = i64::from(ingest_hour_bucket) * TEST_NS_PER_HOUR + 2_000;
        let input_ids: Vec<CompactionInputIdentity> = inputs
            .iter()
            .map(|r| CompactionInputIdentity {
                writer_id: r.writer_id.clone(),
                writer_epoch: r.writer_epoch,
                writer_seq: r.writer_seq,
            })
            .collect();
        let mut hasher = blake3::Hasher::new();
        for id in &input_ids {
            hasher.update(id.writer_id.as_bytes());
            hasher.update(&id.writer_epoch.to_le_bytes());
            hasher.update(&id.writer_seq.to_le_bytes());
        }
        let input_set_hash = *hasher.finalize().as_bytes();
        let part_payload = format!("l1-{ingest_hour_bucket}").into_bytes();
        let part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0u8; 16],
            last_series_id: vec![0xffu8; 16],
            content_hash: blake3::hash(&part_payload).as_bytes().to_vec(),
            object_size: part_payload.len() as u64,
            sample_count: 1,
            series_count: 1,
            run_count: 1,
            min_event_ts_ns: created_unix_ns - 1_000,
            max_event_ts_ns: created_unix_ns,
            segment_format_version: 3,
            declared_column_stats: Vec::new(),
        };
        let record = CompactionRecord {
            format_version: 1,
            tenant_hash: tenant.hash().0.to_vec(),
            signal: ravel_commit::signal::to_proto(Signal::Metrics).into(),
            shard: 0,
            ingest_hour_bucket,
            level: 1,
            inputs: input_ids,
            input_set_hash: input_set_hash.to_vec(),
            parts: vec![part],
            created_unix_ns,
        };

        let part_key =
            keys::reconstruct_l1_part_key(&record, &record.parts[0]).expect("l1 part key");
        store
            .put(
                &part_key,
                bytes::Bytes::from(part_payload),
                PutOptions::default(),
            )
            .await
            .expect("put l1 part object");
        store
            .put(
                &keys::compaction_record_key_for(&record).expect("compaction record key"),
                bytes::Bytes::from(record.encode_to_vec()),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put compaction record");
        record
    }

    /// The exact set of data-object keys the live catalog HEAD snapshot names,
    /// reconstructed from each snapshot entry's own identity fields.
    async fn refold_head_object_keys(
        store: &dyn ObjectStoreBackend,
        tenant: &TenantHash,
    ) -> BTreeSet<String> {
        let got = store
            .get(&head_key(tenant, Signal::Metrics), GetRange::Full)
            .await
            .expect("head present");
        let head = ravel_catalog::decode_head(&got.data).expect("head decodes");
        let mut out = BTreeSet::new();
        for part_ref in &head.parts {
            let got = store
                .get(&part_ref.key, GetRange::Full)
                .await
                .expect("snapshot part present");
            let decoded =
                ravel_catalog::decode_part(&got.data, &ravel_catalog::PartLimits::default())
                    .expect("snapshot part decodes");
            for entry in decoded.entries {
                out.insert(if entry.level == 0 {
                    let writer_id: [u8; 16] = entry
                        .writer_id
                        .as_slice()
                        .try_into()
                        .expect("a level-0 entry carries a 16-byte writer id");
                    let content_hash: [u8; 32] = entry
                        .content_hash
                        .as_slice()
                        .try_into()
                        .expect("an entry carries a 32-byte content hash");
                    keys::data_key(
                        tenant,
                        Signal::Metrics,
                        entry.shard,
                        Uuid::from_bytes(writer_id),
                        entry.writer_epoch,
                        entry.writer_seq,
                        &content_hash,
                    )
                    .expect("data key")
                } else {
                    keys::l1_part_key(
                        tenant,
                        Signal::Metrics,
                        entry.shard,
                        entry.ingest_hour_bucket,
                        &hex::encode(&entry.writer_id[..8]),
                        u32::try_from(entry.writer_epoch).expect("part index fits u32"),
                        &hex::encode(&entry.content_hash[..8]),
                    )
                    .expect("l1 part key")
                });
            }
        }
        out
    }

    /// What every test below starts from: one folded catalog HEAD, one hour
    /// whose compaction landed after that fold (so the snapshot still names
    /// its raw inputs) and one hour whose compaction landed before it (so the
    /// snapshot names the L1 output instead).
    ///
    /// The tenant is quiet after the first fold: nothing is ingested past
    /// [`REFOLD_RECENT_HOUR`], which is what the acceptance test needs.
    struct RefoldFixture {
        store: Arc<MemoryStore>,
        tenant: TenantHash,
        catalog: Catalog,
        /// The L0 input of [`REFOLD_BLOCKED_HOUR`], still named by the
        /// snapshot the derivation gates against.
        blocked_input: ravel_proto::commit::v1::CommitRecord,
        /// The late compaction over `blocked_input`, published after the fold.
        blocked_compaction: ravel_proto::commit::v1::CompactionRecord,
        /// The compaction of [`REFOLD_CLEARED_HOUR`], published before the
        /// fold, whose L1 output the snapshot already names.
        cleared_compaction: ravel_proto::commit::v1::CompactionRecord,
        /// The lone uncompacted L0 input of [`REFOLD_RECENT_HOUR`].
        recent_input: ravel_proto::commit::v1::CommitRecord,
    }

    impl RefoldFixture {
        /// The snapshot key set a correct targeted re-fold leaves behind: the
        /// late compaction's L1 part in place of the input it superseded, and
        /// the two hours the re-fold does not touch, unchanged.
        fn expected_keys_after_refold(&self) -> BTreeSet<String> {
            BTreeSet::from([
                keys::reconstruct_l1_part_key(
                    &self.blocked_compaction,
                    &self.blocked_compaction.parts[0],
                )
                .expect("blocked hour l1 part key"),
                keys::reconstruct_l1_part_key(
                    &self.cleared_compaction,
                    &self.cleared_compaction.parts[0],
                )
                .expect("cleared hour l1 part key"),
                keys::reconstruct_data_key(&self.recent_input).expect("recent data key"),
            ])
        }
    }

    async fn refold_fixture() -> RefoldFixture {
        let store = Arc::new(MemoryStore::new());
        let tenant_id = TenantId::new("acme");
        let tenant = tenant_id.hash();

        let blocked_input = publish_refold_segment(
            store.as_ref(),
            &tenant_id,
            Uuid::from_u128(0x5100),
            REFOLD_BLOCKED_HOUR,
        )
        .await;
        let cleared_input = publish_refold_segment(
            store.as_ref(),
            &tenant_id,
            Uuid::from_u128(0x5200),
            REFOLD_CLEARED_HOUR,
        )
        .await;
        let cleared_compaction = publish_refold_compaction(
            store.as_ref(),
            &tenant_id,
            REFOLD_CLEARED_HOUR,
            &[&cleared_input],
        )
        .await;
        let recent_input = publish_refold_segment(
            store.as_ref(),
            &tenant_id,
            Uuid::from_u128(0x5300),
            REFOLD_RECENT_HOUR,
        )
        .await;

        let catalog = Catalog::new(
            store.clone(),
            ravel_catalog::CatalogConfig {
                shard_count: 1,
                ..Default::default()
            },
        )
        .expect("catalog");
        let first = catalog
            .fold(
                &tenant,
                Signal::Metrics,
                Uuid::new_v4(),
                refold_now_at_seal(REFOLD_RECENT_HOUR),
                &[],
                None,
            )
            .await
            .expect("first fold");
        assert_eq!(
            first.entry_count, 3,
            "the first fold names the blocked hour's raw L0 input, the cleared hour's \
             L1 part, and the recent hour's raw L0 input"
        );
        assert_eq!(
            first.watermark_hour,
            Some(REFOLD_RECENT_HOUR),
            "the first fold's watermark is the last hour that had sealed by then"
        );

        // Published only now: the snapshot above cannot name its output, so
        // rule 2 must hold this hour's inputs on `SnapshotBlock::Named`.
        let blocked_compaction = publish_refold_compaction(
            store.as_ref(),
            &tenant_id,
            REFOLD_BLOCKED_HOUR,
            &[&blocked_input],
        )
        .await;

        RefoldFixture {
            store,
            tenant,
            catalog,
            blocked_input,
            blocked_compaction,
            cleared_compaction,
            recent_input,
        }
    }

    /// The acceptance test for issue #1763: a tenant whose last write is in
    /// [`REFOLD_RECENT_HOUR`] and which ingests nothing afterwards still gets
    /// the hour a late compaction record staled reconciled.
    ///
    /// The second fold runs at the SAME instant as the fixture's first one, so
    /// `sealed_watermark_hour` returns the watermark already on HEAD and the
    /// fold's incremental range is empty. That is the quiet-tenant trap: the
    /// fold used to take the `no_op` early return on an unadvanced watermark
    /// before ever reaching the targeted re-fold pass, and reported
    /// `refold_hours_reconciled == 0` on every tick forever.
    ///
    /// This is the distinguishing test for "the fold derives the set but the
    /// early return still wins": restoring the unconditional
    /// `return Ok(no_op_report(...))` on `watermark_hour_old >= sealed_hour`
    /// in `crates/ravel-catalog/src/fold.rs` fails every assertion below.
    /// Both the count and the exact key set are asserted, because either alone
    /// passes a near neighbour: the count alone would pass a fold that counted
    /// the hour without rewriting anything, and the key set alone would pass a
    /// fold that rewrote hour 5 for some other reason. Nothing else can:
    /// hour 5 is outside the 26-hour reconcile window below watermark 40.
    #[tokio::test]
    async fn a_quiet_tenants_blocked_hour_is_refolded_without_a_watermark_advance() {
        let fixture = refold_fixture().await;
        let compactor = CompactorConfig::default();
        let before = refold_head_object_keys(fixture.store.as_ref(), &fixture.tenant).await;
        assert!(
            before.contains(&keys::reconstruct_data_key(&fixture.blocked_input).expect("key")),
            "the pre-fold snapshot names the input the late compaction superseded"
        );

        // The same instant the fixture folded at: nothing has sealed since,
        // and nothing has been ingested since either.
        let now_ns = refold_now_at_seal(REFOLD_RECENT_HOUR);
        let report = run_tenant_tick(
            &fixture.catalog,
            fixture.store.as_ref(),
            &compactor,
            &fixture.tenant,
            Signal::Metrics,
            Uuid::new_v4(),
            // A zero interval opts out of the HEAD freshness peek, which would
            // otherwise skip a tick this close behind the fixture's own fold.
            Duration::ZERO,
            now_ns,
            None,
        )
        .await
        .expect("the fold runs and returns a report");

        assert!(
            !report.no_op,
            "a derived non-empty blocked set keeps the fold going past an unadvanced watermark"
        );
        assert_eq!(
            report.refold_hours_reconciled, 1,
            "the targeted pass reconciled the one derived hour"
        );
        assert_eq!(
            (report.previous_watermark_hour, report.watermark_hour),
            (Some(REFOLD_RECENT_HOUR), Some(REFOLD_RECENT_HOUR)),
            "the watermark is republished unchanged, never regressed and never advanced"
        );
        assert_eq!(
            refold_head_object_keys(fixture.store.as_ref(), &fixture.tenant).await,
            fixture.expected_keys_after_refold(),
            "after the re-fold the snapshot names the late compaction's output and no \
             longer names the input it superseded"
        );
    }

    /// The derivation is the sweep's condition, not "every hour in the
    /// unsealed region": [`derive_refold_request`] returns EXACTLY the hours
    /// whose live snapshot entry still names a superseded input.
    ///
    /// This is the distinguishing test for "the derivation is not the sweep's
    /// condition". The fixture holds two other hours the same pass reaches:
    /// [`REFOLD_CLEARED_HOUR`], whose compaction predates the fold so its gate
    /// clears, and [`REFOLD_RECENT_HOUR`], which has no compaction at all.
    /// Re-folding every hour below the watermark, or every hour the pass
    /// touched, puts one of them in the set and fails the exact-set assertion.
    ///
    /// A real deleting `sweep_shard` is asserted to report the same set, which
    /// is the anti-drift claim `ravel_maintain::blocked_named_hours` exists to
    /// make. Its deletes are asserted too, so "hour 6 is absent" cannot pass by
    /// the pass never reaching hour 6 at all.
    #[tokio::test]
    async fn the_derived_set_is_exactly_the_hours_the_live_snapshot_still_names() {
        let fixture = refold_fixture().await;
        let compactor = CompactorConfig::default();
        let now_ns = refold_now_at_seal(REFOLD_RECENT_HOUR + 1);

        let derived = derive_refold_request(
            fixture.store.as_ref(),
            &compactor,
            &fixture.tenant,
            Signal::Metrics,
            now_ns,
        )
        .await;

        assert_eq!(
            derived.hours().collect::<BTreeSet<u32>>(),
            BTreeSet::from([REFOLD_BLOCKED_HOUR]),
            "only the hour whose snapshot entry still names its superseded inputs is the \
             fold's work"
        );
        assert!(
            !derived.hours().any(|h| h == REFOLD_CLEARED_HOUR),
            "the hour whose compaction predates the fold cleared the gate, so a re-fold of \
             it would change nothing"
        );
        assert!(
            !derived.hours().any(|h| h == REFOLD_RECENT_HOUR),
            "an hour with no compaction record at all has no superseded input to name"
        );

        // The same condition, from the other caller.
        let report = ravel_maintain::sweep_shard(
            fixture.store.as_ref(),
            &FixedClock::new(now_ns),
            &compactor,
            &NoLeases,
            &fixture.tenant,
            Signal::Metrics,
            0,
        )
        .await
        .expect("sweep");
        assert_eq!(
            report.blocked_named_hours,
            derived.hours().collect::<BTreeSet<u32>>(),
            "the sweep's reporting field and the fold's work list come from one function"
        );
        assert_eq!(
            (
                report.superseded_records_deleted,
                report.superseded_data_deleted
            ),
            (1, 1),
            "the cleared hour's one superseded input record and its one data object went \
             this pass; the blocked hour's did not"
        );
    }

    /// The same reconciliation on the ordinary path, where the tenant is still
    /// ingesting and the watermark does advance, closing the loop: the hour the
    /// first sweep had to hold is collectable by the next one.
    #[tokio::test]
    async fn a_late_compaction_record_is_refolded_when_the_watermark_advances() {
        let fixture = refold_fixture().await;
        let compactor = CompactorConfig::default();
        let now_ns = refold_now_at_seal(REFOLD_RECENT_HOUR + 1);

        let report = run_tenant_tick(
            &fixture.catalog,
            fixture.store.as_ref(),
            &compactor,
            &fixture.tenant,
            Signal::Metrics,
            Uuid::new_v4(),
            DEFAULT_FOLD_INTERVAL,
            now_ns,
            None,
        )
        .await
        .expect("the HEAD is older than the fold interval, so the fold runs");

        assert_eq!(
            report.refold_hours_reconciled, 1,
            "the targeted pass reconciled the one derived hour"
        );
        assert_eq!(
            report.watermark_hour,
            Some(REFOLD_RECENT_HOUR + 1),
            "an hour sealed since the last fold advances the watermark"
        );
        assert_eq!(
            refold_head_object_keys(fixture.store.as_ref(), &fixture.tenant).await,
            fixture.expected_keys_after_refold(),
            "after the re-fold the snapshot names the late compaction's output"
        );

        let second = ravel_maintain::sweep_shard(
            fixture.store.as_ref(),
            &FixedClock::new(now_ns),
            &compactor,
            &NoLeases,
            &fixture.tenant,
            Signal::Metrics,
            0,
        )
        .await
        .expect("second sweep");
        assert!(
            second.blocked_named_hours.is_empty(),
            "nothing is held on a named snapshot once the fold has reconciled the hour"
        );
        assert_eq!(
            (
                second.superseded_records_deleted,
                second.superseded_data_deleted
            ),
            (2, 2),
            "no sweep ran before the fold in this test, so this one collects both hours: \
             the cleared hour's input and the re-folded hour's"
        );
        assert!(
            fixture
                .store
                .get(
                    &keys::reconstruct_data_key(&fixture.blocked_input).expect("data key"),
                    GetRange::Full
                )
                .await
                .is_err(),
            "the input the late compaction superseded is gone once the fold stopped naming it"
        );
    }

    /// `SweepReport::blocked_named_hours` is a reporting field with no
    /// Prometheus family of its own; what an operator watches is the sweep's
    /// deleted-object counter, and a reconciled blocked hour is exactly what
    /// makes it move. This pins that family on the rendered `/metrics` body
    /// with both values: one delete before the re-fold (the cleared hour's
    /// input) and two after it (the previously blocked hour's as well).
    #[tokio::test]
    async fn the_reconciled_blocked_hour_moves_the_rendered_delete_counter() {
        let fixture = refold_fixture().await;
        let compactor = CompactorConfig::default();
        let now_ns = refold_now_at_seal(REFOLD_RECENT_HOUR + 1);
        let clock = FixedClock::new(now_ns);
        let safety = MaintenanceSafetyMetrics::default();

        let first = ravel_maintain::sweep_shard(
            fixture.store.as_ref(),
            &clock,
            &compactor,
            &NoLeases,
            &fixture.tenant,
            Signal::Metrics,
            0,
        )
        .await
        .expect("first sweep");
        assert_eq!(
            first.blocked_named_hours,
            BTreeSet::from([REFOLD_BLOCKED_HOUR]),
            "the first sweep holds the blocked hour's data object back"
        );
        safety.record_sweep(Signal::Metrics, &first);
        assert!(
            render_body(&safety).contains(
                "ravel_maintain_objects_deleted_total{mode=\"maintain\",\
                 kind=\"superseded_data_deleted\"} 1"
            ),
            "only the cleared hour's data object has gone while the fold has not run"
        );

        run_tenant_tick(
            &fixture.catalog,
            fixture.store.as_ref(),
            &compactor,
            &fixture.tenant,
            Signal::Metrics,
            Uuid::new_v4(),
            DEFAULT_FOLD_INTERVAL,
            now_ns,
            None,
        )
        .await
        .expect("the fold runs");

        let second = ravel_maintain::sweep_shard(
            fixture.store.as_ref(),
            &clock,
            &compactor,
            &NoLeases,
            &fixture.tenant,
            Signal::Metrics,
            0,
        )
        .await
        .expect("second sweep");
        safety.record_sweep(Signal::Metrics, &second);

        let body = render_body(&safety);
        assert!(
            body.contains(
                "ravel_maintain_objects_deleted_total{mode=\"maintain\",\
                 kind=\"superseded_data_deleted\"} 2"
            ),
            "the re-folded hour's data object is collected and counted:\n{body}"
        );
        assert!(
            body.contains("# TYPE ravel_maintain_objects_deleted_total counter"),
            "the family still carries its counter TYPE header:\n{body}"
        );
    }

    /// The `/metrics` body a maintain-mode process renders with nothing set but
    /// the maintenance safety counters.
    fn render_body(safety: &MaintenanceSafetyMetrics) -> String {
        let snapshot = MaintenanceSafetySnapshot::from_metrics(safety);
        render(
            crate::config::Mode::Maintain,
            &ravel_object_store::instrument::StoreMetricsSnapshot::default(),
            &[],
            &CatalogCountersSnapshot::default(),
            None,
            Some(&snapshot),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            &AdmissionCountersSnapshot::default(),
            &[],
            0,
            IngestBufferBudgetSnapshot::default(),
            None,
            None,
            &[],
            None,
            crate::mem_stats::AllocatorStats::Other { name: "test" },
            None,
            None,
            None,
            None,
            None,
            MemoryBudgetSnapshot::default(),
            false,
        )
    }
}
