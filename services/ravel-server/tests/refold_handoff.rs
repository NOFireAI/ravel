//! ADR-0063 section 4: the maintain-role sweep hands the hours it found held by
//! a named snapshot to the next scheduled fold of the same `(tenant, signal)`
//! pair, through the process's one in-process `RefoldQueue`.
//!
//! Every test drives the two loops through their public tick functions,
//! `maintain::run_tick_with_refold` and `fold::run_tick`, over one
//! `MemoryStore` on an injected clock, never the catalog's re-fold entry point
//! directly. The fixture is a late compaction record: two L0 inputs in an old
//! hour are folded into the snapshot, the hour is compacted afterwards, and
//! the fold's fixed reconcile window has already moved past it, so nothing but
//! a re-fold request makes the snapshot stop naming the superseded inputs.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, DEFAULT_MAX_SNAPSHOT_PART_BYTES, PartLimits, decode_head,
    decode_part,
};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_maintain::scan::MaintainMemo;
use ravel_maintain::worker_set::{DEFAULT_LIVENESS_FACTOR, DEFAULT_UNIT_CONCURRENCY};
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, RetentionConfig, WorkerSet,
    compact_bucket,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::fold::{self, FOLD_UNIT_SHARD, FoldTickReport, RefoldQueue};
use ravel_server::maintain::{
    self, MaintenanceOwnershipMetrics, MaintenanceSafetyMetrics, SupersededHeldReason,
};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// Seal margin under default catalog config: an ingest hour `H` is sealed once
/// `now >= end(H) + max_flush_lifetime + clock_skew_allowance +
/// fold_safety_margin`.
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// The hour that receives the late compaction record.
const OLD_HOUR: u32 = 1_000;

/// The first fold's watermark: 40 hours past [`OLD_HOUR`], so every later
/// fold's fixed reconcile window (26 hours below the previous watermark) starts
/// above it.
const FIRST_WATERMARK_HOUR: u32 = OLD_HOUR + 40;

/// A pair's fold interval. A HEAD younger than this is skipped as fresh.
const FOLD_INTERVAL: Duration = Duration::from_secs(300);

const HEARTBEAT: Duration = Duration::from_secs(60);

const PROCESS_A: Uuid = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_00a1);
const PROCESS_B: Uuid = Uuid::from_u128(0x0000_0000_0000_0000_0000_0000_0000_00b2);

/// `now_ns` at which `hour` has exactly sealed.
fn seal_now(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

/// The default compactor, full-sweep cadence included.
fn compactor_config() -> CompactorConfig {
    CompactorConfig::default()
}

/// Publish one real L0 metrics segment and its commit record into
/// `(tenant, Metrics, shard, hour)`.
async fn publish_l0(store: &MemoryStore, tenant: &TenantId, shard: u32, hour: u32, seq: u64) {
    let tenant_hash = tenant.hash();
    let writer_id = Uuid::from_u128(0x1763);
    let created_unix_ns = i64::from(hour) * NS_PER_HOUR + 1_000_000_000;
    let labels = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: "cpu".to_string(),
    }])
    .expect("valid labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, "cpu", &labels).expect("series id"),
        labels,
        samples: vec![Sample {
            ts_ns: created_unix_ns + seq as i64,
            value: seq as f64,
        }],
    }];
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: seq,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
    };
    let written = SegmentWriter::write(series, identity, bounds).expect("write segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket: hour,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish commit record");
}

/// The metrics one maintain tick recorded.
struct TickMetrics {
    safety: MaintenanceSafetyMetrics,
    ownership: MaintenanceOwnershipMetrics,
}

/// One maintain-role process over the shared store: its catalog, its worker
/// identity, and the memo its maintain loop keeps across ticks. `fold_store`
/// is what the catalog and the fold tick read and write; `store` is the
/// memory store underneath it, for seeding and inspection.
struct Process {
    store: Arc<MemoryStore>,
    fold_store: Arc<dyn ObjectStoreBackend>,
    catalog: Catalog,
    worker: WorkerSet,
    memo: MaintainMemo,
    shard_count: u32,
    tenant: TenantId,
}

impl Process {
    fn new(
        store: &Arc<MemoryStore>,
        tenant: &TenantId,
        process_id: Uuid,
        shard_count: u32,
    ) -> Self {
        Self::with_fold_store(store, store.clone(), tenant, process_id, shard_count)
    }

    fn with_fold_store(
        store: &Arc<MemoryStore>,
        fold_store: Arc<dyn ObjectStoreBackend>,
        tenant: &TenantId,
        process_id: Uuid,
        shard_count: u32,
    ) -> Self {
        Self::with_catalog_config(
            store,
            fold_store,
            tenant,
            process_id,
            CatalogConfig {
                shard_count,
                ..CatalogConfig::default()
            },
        )
    }

    fn with_catalog_config(
        store: &Arc<MemoryStore>,
        fold_store: Arc<dyn ObjectStoreBackend>,
        tenant: &TenantId,
        process_id: Uuid,
        config: CatalogConfig,
    ) -> Self {
        let shard_count = config.shard_count;
        let catalog = Catalog::new(fold_store.clone(), config).expect("catalog builds");
        let worker = WorkerSet::new(
            seal_now(FIRST_WATERMARK_HOUR),
            HEARTBEAT,
            DEFAULT_LIVENESS_FACTOR,
            DEFAULT_UNIT_CONCURRENCY,
        )
        .with_process_id(process_id);
        Process {
            store: store.clone(),
            fold_store,
            catalog,
            worker,
            memo: MaintainMemo::new(compactor_config().interior_reverify_ns),
            shard_count,
            tenant: tenant.clone(),
        }
    }

    /// One maintain tenant tick at `now_ns`, sending to `refold`.
    async fn maintain_tick(
        &mut self,
        now_ns: i64,
        live_set: &[Uuid],
        refold: &RefoldQueue,
    ) -> TickMetrics {
        let store = self.store.clone();
        self.maintain_tick_on(store.as_ref(), now_ns, live_set, refold, 3)
            .await
    }

    /// [`Self::maintain_tick`] over `store` instead of the process's own
    /// memory store, with a unit counted stalled after `stalled_after` failed
    /// ticks.
    async fn maintain_tick_on(
        &mut self,
        store: &dyn ObjectStoreBackend,
        now_ns: i64,
        live_set: &[Uuid],
        refold: &RefoldQueue,
        stalled_after: u32,
    ) -> TickMetrics {
        let safety = MaintenanceSafetyMetrics::default();
        let ownership = MaintenanceOwnershipMetrics::new(stalled_after);
        safety.begin_scan_cycle();
        maintain::run_tick_with_refold(
            &FixedClock::new(now_ns),
            store,
            &self.tenant.hash(),
            &compactor_config(),
            &RetentionConfig::default(),
            self.shard_count,
            &mut self.memo,
            &safety,
            &ownership,
            &self.worker,
            live_set,
            Some(refold),
        )
        .await;
        safety.publish_scan_cycle();
        TickMetrics { safety, ownership }
    }

    /// One scheduled metrics fold tick at `now_ns`, taking from `refold`.
    async fn fold_tick(
        &self,
        now_ns: i64,
        live_set: &[Uuid],
        refold: &RefoldQueue,
    ) -> FoldTickReport {
        fold::run_tick(
            &self.catalog,
            self.fold_store.as_ref(),
            Signal::Metrics,
            None,
            self.worker.process_id(),
            FOLD_INTERVAL,
            &RetentionConfig::default(),
            &self.worker,
            live_set,
            &FixedClock::new(now_ns),
            refold,
        )
        .await
        .expect("tenant discovery succeeds")
    }
}

/// The snapshot levels HEAD names for `hour`, as `(level 0 count, level 1
/// count)`.
async fn head_levels(store: &MemoryStore, tenant: &TenantId, hour: u32) -> (usize, usize) {
    let head_key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant.hash().to_hex(),
        Signal::Metrics.key_prefix()
    );
    let head = store
        .get(&head_key, GetRange::Full)
        .await
        .expect("HEAD present");
    let head = decode_head(&head.data).expect("HEAD decodes");
    let limits = PartLimits {
        max_snapshot_part_bytes: DEFAULT_MAX_SNAPSHOT_PART_BYTES,
    };
    let (mut l0, mut l1) = (0, 0);
    for part_ref in &head.parts {
        let part = store
            .get(&part_ref.key, GetRange::Full)
            .await
            .expect("part present");
        let part = decode_part(&part.data, &limits).expect("part decodes");
        for entry in part.entries.iter().filter(|e| e.ingest_hour_bucket == hour) {
            match entry.level {
                0 => l0 += 1,
                _ => l1 += 1,
            }
        }
    }
    (l0, l1)
}

/// Seeds two L0 inputs into `(tenant, shard, OLD_HOUR)`, folds them into the
/// snapshot as `folder` at the first watermark, then compacts the hour. The
/// compaction record is created at that same instant, after the fold, so the
/// snapshot names its superseded inputs. Returns the compaction's part count.
async fn seed_late_compaction(folder: &Process, shard: u32, live_set: &[Uuid]) -> usize {
    publish_l0(&folder.store, &folder.tenant, shard, OLD_HOUR, 1).await;
    publish_l0(&folder.store, &folder.tenant, shard, OLD_HOUR, 2).await;
    let first = folder
        .fold_tick(
            seal_now(FIRST_WATERMARK_HOUR),
            live_set,
            &RefoldQueue::default(),
        )
        .await;
    assert_eq!(
        first.folded,
        vec![folder.tenant.hash()],
        "the first fold ran"
    );
    assert_eq!(
        head_levels(&folder.store, &folder.tenant, OLD_HOUR).await,
        (2, 0),
        "the snapshot names the two L0 inputs"
    );
    let outcome = compact_bucket(
        folder.store.as_ref(),
        &FixedClock::new(seal_now(FIRST_WATERMARK_HOUR)),
        &compactor_config(),
        &Bucket::new(folder.tenant.hash(), Signal::Metrics, shard, OLD_HOUR),
    )
    .await
    .expect("compaction runs");
    match outcome {
        CompactionOutcome::Compacted { parts, .. } => parts,
        other => panic!("the old hour must compact, got {other:?}"),
    }
}

/// The instant the late compaction record has passed its protection horizon,
/// so the superseded-input sweep gates its inputs on HEAD reachability.
fn past_horizon_ns() -> i64 {
    seal_now(FIRST_WATERMARK_HOUR) + compactor_config().protection_horizon_ns + NS_PER_HOUR
}

/// The whole hand-off, end to end. The late compaction record sits in an hour
/// below the fold's fixed reconcile window, no retention frontier is
/// configured, and a snapshot still names the superseded inputs. One maintain
/// tick finds the hour held as Named and queues it; one fold tick, its clock
/// past the next seal so it is not a no-op, re-lists exactly that hour, and the
/// snapshot then names the compaction's output and no L0 input.
///
/// Flip either line to watch it fail:
/// - in `maintain::run_tick_with_refold`, delete `queue.send(*tenant, signal,
///   blocked_named_hours);`: the queue stays empty and `pending_len` is 0, not
///   1;
/// - in `fold::run_tenant_tick`, call `catalog.fold(tenant, signal, folder_id,
///   now_ns, &[], default_retention_ns)` instead of
///   `fold_with_refold_request`: `refold_hours_reconciled` is 0, not 1.
#[tokio::test]
async fn late_compaction_record_is_refolded_after_one_sweep_and_one_fold() {
    let store = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("refold-late-compaction");
    let mut process = Process::new(&store, &tenant, PROCESS_A, 1);
    let solo = process.worker.solo_live_set();
    let parts = seed_late_compaction(&process, 0, &solo).await;

    let queue = RefoldQueue::default();
    let now = past_horizon_ns();
    process.maintain_tick(now, &solo, &queue).await;
    assert_eq!(queue.pending_len(), 1, "the sweep queued one entry");
    assert_eq!(queue.dropped_requests(), 0);

    let report = process.fold_tick(now, &solo, &queue).await;
    assert_eq!(report.folded, vec![tenant.hash()]);
    assert_eq!(report.refold_hours_reconciled, 1, "{report:?}");
    assert_eq!(queue.pending_len(), 0, "the fold took the entry");
    assert_eq!(
        head_levels(&store, &tenant, OLD_HOUR).await,
        (0, parts),
        "the snapshot names the compaction's parts and none of its inputs"
    );
}

const NS_PER_MINUTE: i64 = 60_000_000_000;

/// The hours `queue` holds for the tenant's metrics pair, ascending.
fn pending_hours(queue: &RefoldQueue, tenant: &TenantId) -> Vec<u32> {
    queue
        .peek(&tenant.hash(), Signal::Metrics, usize::MAX)
        .hours()
        .collect()
}

/// The first instant at which the seeded record is past its protection horizon
/// and a fold publishes a HEAD that advances the watermark: a quiet tenant's
/// hourly fold, with the queue still empty.
async fn hourly_fold(process: &Process, live_set: &[Uuid], queue: &RefoldQueue) -> i64 {
    let hourly = seal_now(FIRST_WATERMARK_HOUR + 30);
    assert!(
        hourly >= past_horizon_ns(),
        "the record is past its horizon"
    );
    let report = process.fold_tick(hourly, live_set, queue).await;
    assert_eq!(report.folded, vec![process.tenant.hash()]);
    assert_eq!(report.no_op, Vec::new(), "the hourly fold advanced");
    assert_eq!(report.refold_hours_reconciled, 0);
    hourly
}

/// A tenant that stopped ingesting is not stranded by the no-op carve-out, and
/// the hand-off needs no second sweep. One maintain tick, under the default
/// full-sweep cadence, queues the held hour. The fold ticks that follow inside
/// the same watermark hour find HEAD stale but nothing newly sealed, so each is
/// a no-op, reconciles nothing, and leaves the request queued. The first fold
/// tick past the next seal reconciles exactly that hour.
///
/// Flip to watch it fail: in `fold::run_tick`, call
/// `refold.remove_hours(&tenant, signal, &refold_request);` right after
/// `refold.peek` (remove before the fold, as the old drain did). The first
/// no-op tick then leaves `pending_len` 0, not 1, and without that assertion
/// the last fold's `refold_hours_reconciled` is 0, not 1.
#[tokio::test]
async fn a_quiet_tenants_blocked_hour_is_refolded_on_the_next_hourly_fold() {
    let store = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("refold-quiet-tenant");
    let mut process = Process::new(&store, &tenant, PROCESS_A, 1);
    let solo = process.worker.solo_live_set();
    let parts = seed_late_compaction(&process, 0, &solo).await;
    let queue = RefoldQueue::default();
    let hourly = hourly_fold(&process, &solo, &queue).await;

    // The one maintain tick of this test.
    let sweep_at = hourly + 10 * NS_PER_MINUTE;
    process.maintain_tick(sweep_at, &solo, &queue).await;
    assert_eq!(queue.pending_len(), 1, "the sweep queued one pair");
    assert_eq!(pending_hours(&queue, &tenant), vec![OLD_HOUR]);

    for minutes in [10, 30] {
        let report = process
            .fold_tick(hourly + minutes * NS_PER_MINUTE, &solo, &queue)
            .await;
        assert_eq!(report.folded, vec![tenant.hash()], "HEAD was not fresh");
        assert_eq!(report.no_op, vec![tenant.hash()], "nothing newly sealed");
        assert_eq!(
            report.refold_hours_reconciled, 0,
            "a no-op fold reconciles nothing"
        );
        assert_eq!(queue.pending_len(), 1, "a no-op fold leaves the request");
        assert_eq!(pending_hours(&queue, &tenant), vec![OLD_HOUR]);
    }
    assert_eq!(head_levels(&store, &tenant, OLD_HOUR).await, (2, 0));

    // Past the next seal, with no sweep in between.
    let next_seal = seal_now(FIRST_WATERMARK_HOUR + 31);
    let report = process.fold_tick(next_seal, &solo, &queue).await;
    assert_eq!(report.no_op, Vec::new());
    assert_eq!(report.refold_hours_reconciled, 1, "{report:?}");
    assert_eq!(queue.pending_len(), 0);
    assert_eq!(queue.dropped_requests(), 0);
    assert_eq!(head_levels(&store, &tenant, OLD_HOUR).await, (0, parts));
}

/// A fold tick that skips the tenant because its HEAD is fresh does not fold
/// it, so it leaves the request for a later tick, which reconciles it.
///
/// Flip to watch it fail: in `fold::run_tick`, in the
/// `TenantTickOutcome::SkippedFresh` arm, call
/// `refold.remove_hours(&tenant, signal, &refold_request);`. `pending_len`
/// after the skipped tick is then 0, not 1.
#[tokio::test]
async fn a_fresh_skipped_tenant_keeps_its_request() {
    let store = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("refold-fresh-skip");
    let mut process = Process::new(&store, &tenant, PROCESS_A, 1);
    let solo = process.worker.solo_live_set();
    seed_late_compaction(&process, 0, &solo).await;
    let queue = RefoldQueue::default();
    let hourly = hourly_fold(&process, &solo, &queue).await;

    process.maintain_tick(hourly, &solo, &queue).await;
    assert_eq!(queue.pending_len(), 1);

    let report = process
        .fold_tick(hourly + NS_PER_MINUTE, &solo, &queue)
        .await;
    assert_eq!(report.skipped_fresh, vec![tenant.hash()], "{report:?}");
    assert_eq!(report.folded, Vec::new());
    assert_eq!(queue.pending_len(), 1, "a fresh skip leaves the request");
    assert_eq!(pending_hours(&queue, &tenant), vec![OLD_HOUR]);

    let report = process
        .fold_tick(seal_now(FIRST_WATERMARK_HOUR + 31), &solo, &queue)
        .await;
    assert_eq!(report.refold_hours_reconciled, 1, "{report:?}");
    assert_eq!(queue.pending_len(), 0);
}

/// A fold that fails leaves the request, and the next fold that is not a no-op
/// reconciles it. The failure is one refused PUT under the catalog prefix,
/// counted by the fault store so the test proves it fired.
///
/// Flip to watch it fail: in `fold::run_tick`, in the
/// `TenantTickOutcome::Failed` arm, call
/// `refold.remove_hours(&tenant, signal, &refold_request);`. `pending_len`
/// after the failed tick is then 0, not 1.
#[tokio::test]
async fn a_failed_fold_keeps_its_request() {
    let store = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("refold-failed-fold");
    let mut seeder = Process::new(&store, &tenant, PROCESS_A, 1);
    let solo = seeder.worker.solo_live_set();
    seed_late_compaction(&seeder, 0, &solo).await;
    let queue = RefoldQueue::default();
    let hourly = hourly_fold(&seeder, &solo, &queue).await;
    seeder.maintain_tick(hourly, &solo, &queue).await;
    assert_eq!(queue.pending_len(), 1);

    let faults = Arc::new(FaultStore::new(
        store.clone(),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Permanent("refused".to_string()))
                .with_key_contains("/catalog/")
                .with_occurrence(Occurrence::Nth(1)),
        ),
    ));
    let folder = Process::with_fold_store(&store, faults.clone(), &tenant, PROCESS_A, 1);
    let next_seal = seal_now(FIRST_WATERMARK_HOUR + 31);

    let report = folder.fold_tick(next_seal, &solo, &queue).await;
    assert_eq!(
        faults.fault_count(Op::Put, FaultKind::Permanent),
        1,
        "the fault fired"
    );
    assert_eq!(report.failed, vec![tenant.hash()], "{report:?}");
    assert_eq!(report.folded, Vec::new());
    assert_eq!(report.refold_hours_reconciled, 0);
    assert_eq!(queue.pending_len(), 1, "a failed fold leaves the request");
    assert_eq!(pending_hours(&queue, &tenant), vec![OLD_HOUR]);

    let report = folder.fold_tick(next_seal, &solo, &queue).await;
    assert_eq!(report.folded, vec![tenant.hash()], "{report:?}");
    assert_eq!(report.no_op, Vec::new());
    assert_eq!(report.refold_hours_reconciled, 1, "{report:?}");
    assert_eq!(queue.pending_len(), 0);
    assert_eq!(faults.fault_count(Op::Put, FaultKind::Permanent), 1);
}

/// A fold takes at most the catalog's per-fold cap of a pair's oldest pending
/// hours, so a request naming more than the cap keeps its remainder queued
/// for the pair's next fold that advances the watermark. The cap is lowered to
/// 3 in the catalog config; the request names 8 hours starting at the held
/// one, and one fold that is not a no-op reconciles the held hour and leaves
/// exactly the 5 largest.
///
/// Flip to watch it fail: in `fold::run_tick`, pass `usize::MAX` to
/// `refold.peek` instead of `refold_take` (peek everything). The remainder is
/// then empty.
#[tokio::test]
async fn a_request_past_the_per_fold_cap_keeps_its_remainder() {
    const CAP: u32 = 3;
    let store = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("refold-past-the-cap");
    let process = Process::with_catalog_config(
        &store,
        store.clone(),
        &tenant,
        PROCESS_A,
        CatalogConfig {
            shard_count: 1,
            frontier_reconcile_max_hours: CAP,
            ..CatalogConfig::default()
        },
    );
    let solo = process.worker.solo_live_set();
    let parts = seed_late_compaction(&process, 0, &solo).await;

    let queue = RefoldQueue::default();
    queue.send(
        tenant.hash(),
        Signal::Metrics,
        (OLD_HOUR..OLD_HOUR + CAP + 5).collect(),
    );
    let report = process
        .fold_tick(seal_now(FIRST_WATERMARK_HOUR + 30), &solo, &queue)
        .await;
    assert_eq!(report.folded, vec![tenant.hash()], "{report:?}");
    assert_eq!(report.no_op, Vec::new(), "the fold advanced");
    assert_eq!(report.refold_hours_reconciled, 1, "{report:?}");
    assert_eq!(head_levels(&store, &tenant, OLD_HOUR).await, (0, parts));
    assert_eq!(
        pending_hours(&queue, &tenant),
        (OLD_HOUR + CAP..OLD_HOUR + CAP + 5).collect::<Vec<u32>>(),
        "the hours past the cap stay queued"
    );
    assert_eq!(queue.pending_len(), 1);
    assert_eq!(queue.dropped_requests(), 0);
}

/// With more than one maintain process, a process that owns a shard of a pair
/// but not shard 0 of it neither folds the pair nor sweeps any of it (issue
/// #2606): it runs no sweep pass on the shard it owns, so it finds no hold
/// there and queues nothing. It counts nothing as dropped either. The same
/// tick with this process alone in the live set sweeps the shard, finds the
/// hold and queues the hour, so the hold is real.
///
/// Flip to watch it fail: in `maintain::run_tick_with_refold`, replace
/// `(owned || sweeps_pair).then_some((shard, owned))` with
/// `owned.then_some((shard, owned))` and the `if !sweeps_pair` early return in
/// the unit future with `if !owned`, so each process sweeps the shards it owns
/// again. A then holds the hour's inputs as Named, 4 of them, not 0.
#[tokio::test]
async fn a_process_that_does_not_fold_the_pair_sweeps_none_of_it() {
    const SHARDS: u32 = 8;
    let live_ab = vec![PROCESS_A, PROCESS_B];
    let store = Arc::new(MemoryStore::new());
    // The first tenant (in a fixed list) for which, under {A, B}, B owns shard
    // 0 and A owns some other shard. Asserted rather than assumed.
    let (tenant, shard) = split_tenant(&live_ab, SHARDS);

    let mut a = Process::new(&store, &tenant, PROCESS_A, SHARDS);
    let solo_a = a.worker.solo_live_set();
    seed_late_compaction(&a, shard, &solo_a).await;

    let queue = RefoldQueue::default();
    let metrics = a.maintain_tick(past_horizon_ns(), &live_ab, &queue).await;
    assert_eq!(
        metrics
            .safety
            .superseded_inputs_held(Signal::Metrics, SupersededHeldReason::Named),
        0,
        "A swept none of the pair's superseded state, so it found no hold"
    );
    assert_eq!(
        metrics.ownership.full_sweep_passes_total(),
        cold_full_sweep_passes(&a, &live_ab),
        "A swept only the pairs whose shard 0 it owns, every shard of each"
    );
    assert_eq!(queue.pending_len(), 0, "A does not fold the pair");
    assert_eq!(queue.dropped_requests(), 0, "and drops nothing");

    // Control: A alone owns shard 0 too, and the same hold is found and queued.
    let mut alone = Process::new(&store, &tenant, PROCESS_A, SHARDS);
    let control = RefoldQueue::default();
    let metrics = alone
        .maintain_tick(past_horizon_ns(), &solo_a, &control)
        .await;
    assert_eq!(
        metrics
            .safety
            .superseded_inputs_held(Signal::Metrics, SupersededHeldReason::Named),
        4,
        "two commit records and two data objects are held"
    );
    assert_eq!(control.pending_len(), 1, "the hold exists and is queued");
}

/// Issue #2606: a named-snapshot hold whose hour lives on a shard the folder
/// does not own still reaches the folder's fold, in the folder's own process.
/// Under {A, B}, B owns shard 0 of the pair and so folds it; A owns shard
/// `shard` and B does not. B's maintain tick sweeps `shard` anyway, because
/// the pair's whole sweep is B's, finds the hour held because the snapshot
/// still names its superseded inputs, and queues it. B's next fold tick takes
/// it, and the snapshot then names the compaction's parts and no input.
///
/// Flip to watch it fail: in `maintain::run_tick_with_refold`, replace
/// `(owned || sweeps_pair).then_some((shard, owned))` with
/// `owned.then_some((shard, owned))` and the `if !sweeps_pair` early return in
/// the unit future with `if !owned` (each process sweeps the shards it owns, as
/// before this fix). B then never sweeps `shard` and holds 0 inputs, not 4,
/// so its queue stays empty and its fold reconciles nothing; the hold is found
/// by A instead, which does not fold the pair.
#[tokio::test]
async fn a_hold_on_a_shard_the_folder_does_not_own_reaches_its_fold() {
    const SHARDS: u32 = 8;
    let live_ab = vec![PROCESS_A, PROCESS_B];
    let store = Arc::new(MemoryStore::new());
    let (tenant, shard) = split_tenant(&live_ab, SHARDS);
    let mut a = Process::new(&store, &tenant, PROCESS_A, SHARDS);
    let mut b = Process::new(&store, &tenant, PROCESS_B, SHARDS);
    let pair = tenant.hash();
    assert!(
        b.worker
            .owns_unit(&live_ab, &pair, Signal::Metrics, FOLD_UNIT_SHARD),
        "B folds the pair"
    );
    assert!(
        !b.worker.owns_unit(&live_ab, &pair, Signal::Metrics, shard),
        "and does not own the shard the hold is on"
    );
    let solo_a = a.worker.solo_live_set();
    let parts = seed_late_compaction(&a, shard, &solo_a).await;
    let now = past_horizon_ns();

    // A owns the shard and runs its retention and compaction, but no sweep.
    let a_queue = RefoldQueue::default();
    a.maintain_tick(now, &live_ab, &a_queue).await;

    let b_queue = RefoldQueue::default();
    let metrics = b.maintain_tick(now, &live_ab, &b_queue).await;
    assert_eq!(
        metrics
            .safety
            .superseded_inputs_held(Signal::Metrics, SupersededHeldReason::Named),
        4,
        "B swept the shard it does not own and held the hour's inputs"
    );
    assert_eq!(
        metrics.ownership.full_sweep_passes_total(),
        cold_full_sweep_passes(&b, &live_ab),
        "a cold first tick runs a full sweep pass of every shard of each pair B folds"
    );
    assert_eq!(
        pending_hours(&b_queue, &tenant),
        vec![OLD_HOUR],
        "the hold reached the folder's queue"
    );

    let report = b.fold_tick(now, &live_ab, &b_queue).await;
    assert_eq!(report.owned, vec![pair]);
    assert_eq!(report.folded, vec![pair], "{report:?}");
    assert_eq!(report.refold_hours_reconciled, 1, "{report:?}");
    assert_eq!(b_queue.pending_len(), 0, "the fold took the entry");
    assert_eq!(
        head_levels(&store, &tenant, OLD_HOUR).await,
        (0, parts),
        "the snapshot names the compaction's parts and none of its inputs"
    );
    assert_eq!(
        a_queue.pending_len(),
        0,
        "A, which does not fold the pair, found no hold to queue"
    );
}

/// The tenant's durable retention window in the zoned test below: 48 hours,
/// so at [`past_horizon_ns`] [`OLD_HOUR`] has expired (its end is 1001 h, the
/// tick about 1068 h) and is still inside one protection horizon of its
/// expiry, which `classify_zone` calls its tail.
const ZONED_RETENTION_NS: i64 = 48 * NS_PER_HOUR;

/// Issue #2606, zoned cadence: on a tick where the full-sweep pass is not due,
/// the folder's sweep of a shard it does not own, and so does not scan, takes
/// its hours from the shard's own listing classified against the tenant's
/// retention window, and still finds a hold in a tail hour and queues it. B's
/// memo is primed with a full sweep of that shard at the tick's own instant
/// (`record_full_sweep`, as a previous tick's full pass would leave it), so
/// that shard takes the zoned pass while every other shard B sweeps, cold,
/// takes a full one; the full-sweep counter is one short of the cold count,
/// which is the proof the hold was found by the zoned pass.
///
/// The tail hour is also inside the fold's retirement-frontier band, so the
/// fold lists it in that pass and not in the targeted re-fold pass;
/// `refold_hours_reconciled` is 0, and the fold still takes the entry and
/// the snapshot stops naming the inputs.
///
/// Flip to watch it fail: in `unscanned_head_tail_hours`, change
/// `!= Zone::Interior` to `== Zone::Interior`, or pass `None` for the
/// retention window to `classify_zone`. The zoned pass then lists no hour of
/// the shard and B holds 0 inputs, not 4.
#[tokio::test]
async fn a_zoned_sweep_of_a_shard_the_folder_does_not_scan_finds_its_hold() {
    const SHARDS: u32 = 8;
    let live_ab = vec![PROCESS_A, PROCESS_B];
    let store = Arc::new(MemoryStore::new());
    let (tenant, shard) = split_tenant(&live_ab, SHARDS);
    let a = Process::new(&store, &tenant, PROCESS_A, SHARDS);
    let mut b = Process::new(&store, &tenant, PROCESS_B, SHARDS);
    let pair = tenant.hash();
    let solo_a = a.worker.solo_live_set();
    let parts = seed_late_compaction(&a, shard, &solo_a).await;
    let config = ravel_catalog::TenantConfig {
        retention_ns: Some(ZONED_RETENTION_NS),
        ..ravel_catalog::TenantConfig::new(ravel_catalog::TenantLifecycleState::Active)
    };
    ravel_catalog::set_tenant_config(store.as_ref(), &pair, &config, 1)
        .await
        .expect("write the tenant's retention window");
    let now = past_horizon_ns();
    assert_eq!(
        ravel_maintain::scan::classify_zone(
            OLD_HOUR,
            now,
            &compactor_config(),
            Some(ZONED_RETENTION_NS)
        ),
        ravel_maintain::scan::Zone::Tail,
        "the held hour is in the tail at the tick"
    );

    b.memo.record_full_sweep(pair, Signal::Metrics, shard, now);
    let b_queue = RefoldQueue::default();
    let metrics = b.maintain_tick(now, &live_ab, &b_queue).await;
    assert_eq!(
        metrics.ownership.full_sweep_passes_total(),
        cold_full_sweep_passes(&b, &live_ab) - 1,
        "every shard B sweeps took a full pass but the primed one"
    );
    assert_eq!(
        metrics
            .safety
            .superseded_inputs_held(Signal::Metrics, SupersededHeldReason::Named),
        4,
        "the zoned pass of the shard B does not scan held the hour's inputs"
    );
    assert_eq!(
        pending_hours(&b_queue, &tenant),
        vec![OLD_HOUR],
        "the hold reached the folder's queue"
    );

    let report = b.fold_tick(now, &live_ab, &b_queue).await;
    assert_eq!(report.owned, vec![pair]);
    assert_eq!(report.folded, vec![pair], "{report:?}");
    assert_eq!(
        report.refold_hours_reconciled, 0,
        "the frontier pass listed the tail hour first: {report:?}"
    );
    assert_eq!(b_queue.pending_len(), 0, "the fold took the entry");
    assert_eq!(
        head_levels(&store, &tenant, OLD_HOUR).await,
        (0, parts),
        "the snapshot names the compaction's parts and none of its inputs"
    );
}

/// A sweep of a shard the folder does not own that keeps failing reaches the
/// stalled-unit gauge on the folder, through the pair's FOLD_UNIT_SHARD unit,
/// although no unit of that shard is the folder's. The fault refuses every
/// LIST under the shard's commit prefix, which only the sweep issues on B, and
/// is counted so the test proves it fired.
///
/// Flip to watch it fail: in `maintain::run_tick_with_refold`, drop
/// `&& (shard != FOLD_UNIT_SHARD || unscanned_sweeps_ok)` from `sweeps_ok`.
/// `units_stalled` is then 0, not 1.
#[tokio::test]
async fn a_failing_sweep_of_an_unowned_shard_stalls_the_fold_unit() {
    const SHARDS: u32 = 8;
    let live_ab = vec![PROCESS_A, PROCESS_B];
    let store = Arc::new(MemoryStore::new());
    let (tenant, shard) = split_tenant(&live_ab, SHARDS);
    publish_l0(&store, &tenant, shard, OLD_HOUR, 1).await;
    let prefix =
        keys::commit_shard_prefix(&tenant.hash(), Signal::Metrics, shard).expect("shard prefix");
    let faults = FaultStore::new(
        store.clone(),
        FaultPlan::empty().with_rule(
            Rule::new(Op::List, ScriptedFault::Permanent("refused".to_string()))
                .with_key_contains(prefix),
        ),
    );
    let mut b = Process::new(&store, &tenant, PROCESS_B, SHARDS);

    let metrics = b
        .maintain_tick_on(
            &faults,
            past_horizon_ns(),
            &live_ab,
            &RefoldQueue::default(),
            1,
        )
        .await;
    assert_eq!(
        faults.fault_count(Op::List, FaultKind::Permanent),
        1,
        "the fault fired, on the shard sweep's first LIST"
    );
    assert_eq!(
        metrics.ownership.full_sweep_passes_total(),
        cold_full_sweep_passes(&b, &live_ab) - 1,
        "every other shard's sweep pass completed"
    );
    assert_eq!(
        metrics.ownership.units_stalled(),
        1,
        "the failed sweep stalls B's fold unit"
    );
}

/// The full sweep passes a cold first maintain tick of `process` runs under
/// `live_set`: every shard of each maintained signal whose shard 0 it owns,
/// and nothing for any other signal.
fn cold_full_sweep_passes(process: &Process, live_set: &[Uuid]) -> u64 {
    let swept_pairs = [Signal::Metrics, Signal::Logs, Signal::Spans]
        .into_iter()
        .filter(|signal| {
            process
                .worker
                .owns_unit(live_set, &process.tenant.hash(), *signal, FOLD_UNIT_SHARD)
        })
        .count() as u64;
    swept_pairs * u64::from(process.shard_count)
}

/// The first tenant (in a fixed list) for which, under `live_ab`, B owns shard
/// 0 of the metrics pair and A owns some other shard, with that shard.
/// Asserted rather than assumed.
fn split_tenant(live_ab: &[Uuid], shards: u32) -> (TenantId, u32) {
    let probe = WorkerSet::with_defaults(0).with_process_id(PROCESS_A);
    (0..64)
        .map(|n| TenantId::new(format!("refold-split-{n}")))
        .find_map(|tenant| {
            let hash = tenant.hash();
            if probe.owns_unit(live_ab, &hash, Signal::Metrics, FOLD_UNIT_SHARD) {
                return None;
            }
            (1..shards)
                .find(|s| probe.owns_unit(live_ab, &hash, Signal::Metrics, *s))
                .map(|s| (tenant, s))
        })
        .expect("some tenant splits shard 0 and another shard across A and B")
}

/// An entry for a pair whose shard 0 another live process owns is removed at
/// the start of this process's fold tick for the signal, and not counted as
/// dropped, so a pair whose ownership moved away does not pin a queue slot.
/// The same entry under this process alone, for a tenant it maintains and
/// skips as fresh, stays queued, so the removal follows ownership.
///
/// Flip to watch it fail: in `fold::run_tick`, delete the
/// `refold.remove_unless(...)` loop. `pending_len` after the tick under
/// `{A, B}` is then 1, not 0.
#[tokio::test]
async fn an_entry_for_a_pair_this_process_does_not_fold_is_removed_uncounted() {
    const SHARDS: u32 = 8;
    let live_ab = vec![PROCESS_A, PROCESS_B];
    let (tenant, _) = split_tenant(&live_ab, SHARDS);
    let store = Arc::new(MemoryStore::new());
    let a = Process::new(&store, &tenant, PROCESS_A, SHARDS);
    let solo_a = a.worker.solo_live_set();
    let now = past_horizon_ns();

    // A first fold, so the tick below finds the tenant maintained and its
    // HEAD fresh, and neither folds nor removes the entry.
    publish_l0(&store, &tenant, 0, OLD_HOUR, 1).await;
    let queue = RefoldQueue::default();
    a.fold_tick(now, &solo_a, &queue).await;
    queue.send(tenant.hash(), Signal::Metrics, BTreeSet::from([OLD_HOUR]));
    let report = a.fold_tick(now, &solo_a, &queue).await;
    assert_eq!(report.skipped_fresh, vec![tenant.hash()], "{report:?}");
    assert_eq!(queue.pending_len(), 1, "A alone owns the pair and keeps it");

    let report = a.fold_tick(now, &live_ab, &queue).await;
    assert_eq!(report.owned, Vec::new());
    assert_eq!(queue.pending_len(), 0, "B owns shard 0, so A removed it");
    assert_eq!(queue.dropped_requests(), 0, "a removal is not a drop");
}

/// An entry for a tenant the tick does not maintain, here one deleted after
/// the send so discovery no longer finds it, is removed at the fold tick for
/// the signal and not counted as dropped, although this process alone owns
/// shard 0 of the pair. Its slot is freed rather than pinned until eviction.
///
/// Flip to watch it fail: in `fold::run_tick`, pass
/// `|tenant| worker.owns_unit(live_set, tenant, signal, FOLD_UNIT_SHARD)` to
/// `refold.remove_unless` instead of the `maintained_set` membership test.
/// `pending_len` is then 1, not 0.
#[tokio::test]
async fn an_entry_for_a_tenant_the_tick_no_longer_maintains_is_removed_uncounted() {
    let store = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("refold-deleted-tenant");
    let a = Process::new(&store, &tenant, PROCESS_A, 1);
    let solo_a = a.worker.solo_live_set();
    assert!(
        a.worker
            .owns_unit(&solo_a, &tenant.hash(), Signal::Metrics, FOLD_UNIT_SHARD),
        "ownership alone would keep the entry"
    );

    let queue = RefoldQueue::default();
    queue.send(tenant.hash(), Signal::Metrics, BTreeSet::from([OLD_HOUR]));
    let report = a.fold_tick(past_horizon_ns(), &solo_a, &queue).await;
    assert_eq!(report.discovered, 0, "{report:?}");
    assert_eq!(report.maintained, 0);
    assert_eq!(
        queue.pending_len(),
        0,
        "the tick does not maintain the tenant"
    );
    assert_eq!(queue.dropped_requests(), 0, "a removal is not a drop");
}
