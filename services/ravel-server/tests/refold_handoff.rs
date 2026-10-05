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
use ravel_maintain::MaintainReport;
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
use ravel_server::maintain::{self, MaintenanceOwnershipMetrics, MaintenanceSafetyMetrics};
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
    ) -> MaintainReport {
        let safety = MaintenanceSafetyMetrics::default();
        safety.begin_scan_cycle();
        let report = maintain::run_tick_with_refold(
            &FixedClock::new(now_ns),
            self.store.as_ref(),
            &self.tenant.hash(),
            &compactor_config(),
            &RetentionConfig::default(),
            self.shard_count,
            &mut self.memo,
            &safety,
            &MaintenanceOwnershipMetrics::new(3),
            &self.worker,
            live_set,
            Some(refold),
        )
        .await;
        safety.publish_scan_cycle();
        report
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
/// - in `maintain::send_refold_hours`, delete `queue.send(*tenant, signal,
///   hours);`: the queue stays empty and `pending_len` is 0, not 1;
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
        .peek(
            &tenant.hash(),
            Signal::Metrics,
            fold::REFOLD_ENTRY_HOURS_MAX,
        )
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

/// The catalog reconciles at most `frontier_reconcile_max_hours` of a
/// request's hours per fold, oldest first, so a fold tick passes only that
/// many of the pair's oldest pending hours and removes only those. A request
/// five hours past the cap keeps exactly its five largest hours queued after a
/// fold that advanced the watermark. The oldest passed hour is the held one,
/// and the fold reconciles it.
///
/// Flip to watch it fail: in `fold::run_tick`, pass `usize::MAX` to
/// `refold.peek` instead of `refold_limit` (peek everything). The remainder is
/// then empty, not the five largest hours.
#[tokio::test]
async fn a_request_past_the_per_fold_cap_keeps_its_remainder() {
    const CAP: u32 = 4;
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
    assert_eq!(report.no_op, Vec::new(), "the fold advanced the watermark");
    assert_eq!(report.refold_hours_reconciled, 1, "{report:?}");
    assert_eq!(
        pending_hours(&queue, &tenant),
        (OLD_HOUR + CAP..OLD_HOUR + CAP + 5).collect::<Vec<u32>>(),
        "the hours past the cap stay queued"
    );
    assert_eq!(queue.pending_len(), 1);
    assert_eq!(queue.dropped_requests(), 0);
    assert_eq!(head_levels(&store, &tenant, OLD_HOUR).await, (0, parts));
}

/// With more than one maintain process, a process that sweeps a shard of a
/// pair but does not own shard 0 of it does not fold the pair, so it queues
/// nothing for it: no fold in that process would take the entry, and its fold
/// tick would only remove it. It counts nothing as dropped either. The same tick with this
/// process alone in the live set does queue the hour, so the hold is real.
///
/// Flip to watch it fail: in `maintain::send_refold_hours`, delete the
/// `!worker.owns_unit(live_set, tenant, signal, FOLD_UNIT_SHARD)` early return.
/// The first tick then queues one entry.
#[tokio::test]
async fn a_process_that_does_not_fold_the_pair_sends_nothing() {
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
    a.maintain_tick(past_horizon_ns(), &live_ab, &queue).await;
    assert_eq!(queue.pending_len(), 0, "A does not fold the pair");
    assert_eq!(queue.dropped_requests(), 0, "and drops nothing");

    // Control: A alone owns shard 0 too, and the same hold is queued.
    let mut alone = Process::new(&store, &tenant, PROCESS_A, SHARDS);
    let control = RefoldQueue::default();
    alone
        .maintain_tick(past_horizon_ns(), &solo_a, &control)
        .await;
    assert_eq!(control.pending_len(), 1, "the hold exists and is queued");
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
/// The same entry under this process alone stays queued, so the removal
/// follows ownership.
///
/// Flip to watch it fail: in `fold::run_tick`, delete the
/// `refold.remove_unless(...)` loop at the top. `pending_len` after the tick
/// under `{A, B}` is then 1, not 0.
#[tokio::test]
async fn an_entry_for_a_pair_this_process_does_not_fold_is_removed_uncounted() {
    const SHARDS: u32 = 8;
    let live_ab = vec![PROCESS_A, PROCESS_B];
    let (tenant, _) = split_tenant(&live_ab, SHARDS);
    let store = Arc::new(MemoryStore::new());
    let a = Process::new(&store, &tenant, PROCESS_A, SHARDS);
    let solo_a = a.worker.solo_live_set();
    let now = past_horizon_ns();

    let queue = RefoldQueue::default();
    queue.send(tenant.hash(), Signal::Metrics, BTreeSet::from([OLD_HOUR]));
    a.fold_tick(now, &solo_a, &queue).await;
    assert_eq!(queue.pending_len(), 1, "A alone owns the pair and keeps it");

    let report = a.fold_tick(now, &live_ab, &queue).await;
    assert_eq!(report.owned, Vec::new());
    assert_eq!(queue.pending_len(), 0, "B owns shard 0, so A removed it");
    assert_eq!(queue.dropped_requests(), 0, "a removal is not a drop");
}
