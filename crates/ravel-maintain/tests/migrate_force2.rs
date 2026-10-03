//! ADR-0066 force 2 wired into `migrate` (issue #2093 task T6), and what
//! `migrate` reports about the buckets it did not migrate (issue #2406 items 1
//! and 2).
//!
//! Every compacted bucket here is a real compaction: L0 inputs seeded through
//! the production writer and compacted by `compact_bucket`. Its record's parts
//! are then made below-target the way `force2_reencode.rs` does it: the bytes
//! stay at the current version and the record says one less, because only one
//! RSEG version is writable today. The record is overwritten in place to do
//! that, which only a fixture may do.
//!
//! Every test drives `migrate_family` end to end. A refusal test runs over a
//! `FaultStore` that fails every PUT, so it proves it wrote nothing by the fault
//! counter staying at 0. Interleavings are driven by `FaultStore` hold gates and
//! `FixedClock`s; nothing sleeps. Each test's doc comment names the line whose
//! removal fails it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use ravel_commit::{erasure, keys, record, signal};
use ravel_fleet::claim::ClaimConfig;
use ravel_maintain::claim_guard::{Acquire, ClaimGuard, ClaimSleeper};
use ravel_maintain::migrate::{
    MigrationPath, NotMigratedBucket, NotMigratedReason, ReencodeBlockedBucket,
    ReencodeBlockedReason,
};
use ravel_maintain::{
    ClaimParticipant, ClaimSkipReason, Clock, CompactionOutcome, CompactorConfig, Coordination,
    ErasureRewriteOutcome, FamilyMigrateReport, FixedClock, MaintainMemo, MigrateBudget, NoLeases,
    PendingErasureRequest, PublishOutcome, Verification, compact_bucket, erasure_rewrite_bucket,
    migrate_family, read, sweep_superseded,
};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, GateHandle, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{CompactionRecord, ErasurePredicateMatcher, ErasureRequest};
use ravel_types::Signal;
use uuid::Uuid;

/// The format family the metrics floor is recorded under.
const FAMILY: &str = "rseg";

/// Shards the walk scans: enough to reach [`SHARD`].
const SHARDS: u32 = SHARD + 1;

/// The key fragment of every L1 and rewrite part object.
const PART_KEYS: &str = "/l1/";

/// A lease no run here outlives.
const LEASE: Duration = Duration::from_secs(3);

/// The version the current build writes metrics parts at, which is the target
/// every compacted-bucket test migrates toward.
fn target() -> u32 {
    ravel_maintain::build::OUTPUT_FORMAT_VERSION
}

/// A [`ClaimSleeper`] that returns at once, so no jitter wait touches the real
/// timer.
struct NoWait;

impl ClaimSleeper for NoWait {
    fn sleep(&self, _duration: Duration) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(std::future::ready(()))
    }
}

/// The writer switch on, no claims.
fn reencode_cfg() -> CompactorConfig {
    CompactorConfig {
        reencode_writer_enabled: true,
        ..CompactorConfig::default()
    }
}

fn participant(process: u128, clock: &FixedClock) -> ClaimParticipant {
    ClaimParticipant::new(
        Uuid::from_u128(process),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
    )
    .with_sleeper(Arc::new(NoWait))
}

/// A config that takes claims as `process` on `clock`, with the writer switch
/// on.
fn claiming_cfg(process: u128, clock: &FixedClock) -> CompactorConfig {
    CompactorConfig {
        coordination: Coordination::On,
        claim_lease_duration: LEASE,
        claim_participant: Some(participant(process, clock)),
        reencode_writer_enabled: true,
        ..CompactorConfig::default()
    }
}

/// Record the tenant's provisioning, which the floor raise appends to.
async fn provision(store: &dyn ObjectStoreBackend) {
    ravel_catalog::validate_or_adopt(
        store,
        &tenant_hash(),
        Signal::Metrics,
        SHARDS,
        0,
        ravel_catalog::AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision tenant/signal");
}

/// Two metrics L0 inputs in [`bucket`], both at the current version. The
/// series `victim` is what the erasure test drops.
async fn seed_l0(store: &dyn ObjectStoreBackend) {
    for spec in [
        InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![
                raw_series("keep", &[("k", "a")], &[(1_000, 1.0)]),
                raw_series("victim", &[("k", "b")], &[(1_000, 5.0)]),
            ],
        ),
        InputSpec::new(
            Uuid::from_u128(2),
            10,
            2,
            vec![raw_series("keep", &[("k", "a")], &[(2_000, 2.0)])],
        ),
    ] {
        seed_input(store, &spec).await;
    }
}

/// Seed the two L0 inputs and compact them into one record with one part.
async fn seed_compacted(store: &dyn ObjectStoreBackend) {
    seed_l0(store).await;
    let outcome = compact_bucket(
        store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        &bucket(),
    )
    .await
    .expect("compact");
    assert!(
        matches!(
            outcome,
            CompactionOutcome::Compacted {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "the fixture compacts: {outcome:?}"
    );
}

/// The bucket's compaction records, decoded, by key.
async fn compaction_records(store: &dyn ObjectStoreBackend) -> Vec<(String, CompactionRecord)> {
    let listing = read::list_bucket(store, &bucket()).await.expect("list");
    let mut out = Vec::new();
    for key in listing.compaction_record_keys {
        let rec = record::decode_compaction(&get_full(store, &key).await).expect("decode");
        out.push((key, rec));
    }
    out
}

/// Overwrite the bucket's one compaction record with every part recorded one
/// version below the target, and check the fixture's shape: two inputs, one
/// part. Returns the record and its key.
async fn stamp_parts_below_target(store: &dyn ObjectStoreBackend) -> (String, CompactionRecord) {
    let mut records = compaction_records(store).await;
    assert_eq!(records.len(), 1, "one compaction record to stamp");
    let (key, mut rec) = records.remove(0);
    assert_eq!(rec.inputs.len(), 2, "the record names both inputs");
    assert_eq!(rec.parts.len(), 1, "the record has one part");
    for part in &mut rec.parts {
        part.segment_format_version = target() - 1;
    }
    store
        .put(&key, record::encode_compaction(&rec), PutOptions::default())
        .await
        .expect("overwrite the fixture record");
    (key, rec)
}

/// A version 1 copy of `rec` naming only `inputs`, PUT at its canonical key.
async fn put_record_over(
    store: &dyn ObjectStoreBackend,
    rec: &CompactionRecord,
    inputs: Vec<ravel_proto::commit::v1::CompactionInputIdentity>,
) {
    let copy = CompactionRecord {
        input_set_hash: erasure::compute_compaction_input_set_hash(&inputs).to_vec(),
        inputs,
        ..rec.clone()
    };
    store
        .put(
            &keys::compaction_record_key_for(&copy).expect("key"),
            record::encode_compaction(&copy),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put the record");
}

/// A store that fails every PUT made through it. Seeding and provisioning go
/// through the inner store, under the fault layer.
fn put_refusing_store() -> FaultStore<MemoryStore> {
    FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(Rule::new(
            Op::Put,
            ScriptedFault::Permanent("this run must not write".to_string()),
        )),
    )
}

fn puts_attempted(store: &FaultStore<MemoryStore>) -> u64 {
    store.fault_count(Op::Put, FaultKind::Permanent)
}

async fn all_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    list_all(store, "")
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect()
}

/// One unbudgeted `migrate` over the metrics family toward `target_version`,
/// at the sealed instant.
async fn migrate(
    store: &dyn ObjectStoreBackend,
    config: &CompactorConfig,
    target_version: u32,
) -> FamilyMigrateReport {
    migrate_family(
        store,
        &FixedClock::new(sealed_now_ns()),
        config,
        tenant_hash(),
        Signal::Metrics,
        FAMILY,
        target_version,
        SHARDS,
        MigrateBudget::unlimited(),
        "migrate_force2 test",
    )
    .await
    .expect("migrate")
}

async fn floor(store: &dyn ObjectStoreBackend) -> Option<u32> {
    ravel_catalog::current_floor_from_store(store, &tenant_hash(), Signal::Metrics, FAMILY)
        .await
        .expect("read floor")
}

fn stragglers(l0: usize, l1: usize) -> Option<Verification> {
    Some(Verification::Stragglers {
        l0,
        l1,
        rewrite_parts: 0,
        blocked: Vec::new(),
    })
}

fn reencode_blocked(reason: ReencodeBlockedReason) -> Vec<ReencodeBlockedBucket> {
    vec![ReencodeBlockedBucket {
        shard: SHARD,
        ingest_hour: HOUR,
        reason,
    }]
}

fn not_migrated(path: MigrationPath, reason: NotMigratedReason) -> Vec<NotMigratedBucket> {
    vec![NotMigratedBucket {
        shard: SHARD,
        ingest_hour: HOUR,
        path,
        reason,
    }]
}

/// With the switch on, a bucket held below the target only by its one
/// compaction record's parts is re-encoded and counted as migrated. The
/// predecessor stays listed, so this run's re-audit still counts its one part
/// and leaves the floor unraised; once `sweep` reclaims the predecessor, the
/// next run's fresh re-audit raises the floor.
///
/// Removing the `reencode_compaction_parts(store, clock, config, &bucket)` call
/// from the walk's `Force2::Reencode` arm (recording nothing instead) leaves
/// the bucket unmigrated: `buckets_migrated` is 0, not 1, and the second run
/// still finds the part below the target.
#[tokio::test]
async fn the_switch_on_reencodes_and_a_later_reaudit_raises_the_floor() {
    let store = MemoryStore::new();
    provision(&store).await;
    seed_compacted(&store).await;
    let (pred_key, _) = stamp_parts_below_target(&store).await;

    let first = migrate(&store, &reencode_cfg(), target()).await;

    assert_eq!(first.buckets_examined, 1);
    assert_eq!(first.buckets_migrated, 1);
    assert_eq!(first.records_migrated, 2, "the record's two inputs");
    assert_eq!(first.reencode_blocked, Vec::new());
    assert_eq!(first.not_migrated, Vec::new());
    assert_eq!(first.blocked_buckets, Vec::new());
    assert_eq!(
        first.verification,
        stragglers(0, 1),
        "the predecessor's one part is listed until the sweep"
    );
    assert_eq!(floor(&store).await, None);
    let records = compaction_records(&store).await;
    assert_eq!(records.len(), 2, "the predecessor and its version 2 record");
    let (v2_key, v2) = records
        .iter()
        .find(|(key, _)| *key != pred_key)
        .expect("a successor");
    assert_eq!(v2.format_version, 2);
    assert_eq!(v2.superseded_record_key, pred_key);
    assert_eq!(
        v2.parts
            .iter()
            .filter(|p| p.segment_format_version < target())
            .count(),
        0,
        "every part of the version 2 record is at the target"
    );

    let past_horizon = sealed_now_ns() + CompactorConfig::default().protection_horizon_ns + 1;
    sweep_superseded(
        &store,
        &FixedClock::new(past_horizon),
        &CompactorConfig::default(),
        &NoLeases,
        &tenant_hash(),
        Signal::Metrics,
        SHARD,
    )
    .await
    .expect("sweep");
    let records = compaction_records(&store).await;
    assert_eq!(records.len(), 1, "the sweep reclaimed the predecessor");
    assert_eq!(records[0].0, *v2_key);

    let second = migrate(&store, &reencode_cfg(), target()).await;

    assert_eq!(second.buckets_examined, 1);
    assert_eq!(second.buckets_migrated, 0, "nothing left to re-encode");
    assert_eq!(second.records_migrated, 0);
    assert_eq!(second.reencode_blocked, Vec::new());
    assert_eq!(second.not_migrated, Vec::new());
    assert_eq!(
        second.verification,
        Some(Verification::FloorRaised {
            floor_version: target()
        })
    );
    assert_eq!(floor(&store).await, Some(target()));
}

/// With the switch off (the default) the bucket force 2 would re-encode is
/// named blocked with the switch reason and its below-target part count, and
/// nothing is written.
///
/// Replacing the `ReencodeOutcome::WriterDisabled` arm of `record_reencode`
/// with one that records nothing leaves `reencode_blocked` empty.
#[tokio::test]
async fn the_switch_off_names_the_bucket_and_writes_nothing() {
    let store = put_refusing_store();
    provision(store.inner()).await;
    seed_compacted(store.inner()).await;
    stamp_parts_below_target(store.inner()).await;
    assert!(!CompactorConfig::default().reencode_writer_enabled);
    let before = all_keys(store.inner()).await;

    let report = migrate(&store, &CompactorConfig::default(), target()).await;

    assert_eq!(
        report.reencode_blocked,
        reencode_blocked(ReencodeBlockedReason::WriterDisabled { below_target: 1 })
    );
    assert_eq!(report.buckets_examined, 1);
    assert_eq!(report.buckets_migrated, 0);
    assert_eq!(report.records_migrated, 0);
    assert_eq!(report.not_migrated, Vec::new());
    assert_eq!(report.blocked_buckets, Vec::new());
    assert_eq!(report.verification, stragglers(0, 1));
    assert_eq!(puts_attempted(&store), 0, "no PUT was made");
    assert_eq!(all_keys(store.inner()).await, before);
    assert_eq!(floor(store.inner()).await, None);
}

/// A bucket whose overlap component holds two compaction records is named with
/// item 4's reason, with the switch off and on, and nothing is written. The
/// second record names one of the first record's two inputs, so the two share
/// a component; both carry the stamped below-target part.
///
/// Removing the `selection.largest_component() > 1` return in `force2_case`
/// treats the winner as the bucket's one record: with the switch off the
/// bucket is named `WriterDisabled`, not `ContestedOverlap`.
#[tokio::test]
async fn a_contested_overlap_is_named_and_writes_nothing() {
    for switch in [false, true] {
        let store = put_refusing_store();
        provision(store.inner()).await;
        seed_compacted(store.inner()).await;
        let (_, first) = stamp_parts_below_target(store.inner()).await;
        put_record_over(store.inner(), &first, vec![first.inputs[0].clone()]).await;
        let before = all_keys(store.inner()).await;
        let config = CompactorConfig {
            reencode_writer_enabled: switch,
            ..CompactorConfig::default()
        };

        let report = migrate(&store, &config, target()).await;

        assert_eq!(
            report.reencode_blocked,
            reencode_blocked(ReencodeBlockedReason::ContestedOverlap {
                largest_component: 2
            }),
            "switch {switch}"
        );
        assert_eq!(report.buckets_migrated, 0, "switch {switch}");
        assert_eq!(report.records_migrated, 0, "switch {switch}");
        assert_eq!(report.not_migrated, Vec::new(), "switch {switch}");
        assert_eq!(report.blocked_buckets, Vec::new(), "switch {switch}");
        assert_eq!(
            report.verification,
            stragglers(0, 2),
            "both records' parts, switch {switch}"
        );
        assert_eq!(puts_attempted(&store), 0, "no PUT, switch {switch}");
        assert_eq!(all_keys(store.inner()).await, before, "switch {switch}");
    }
}

/// A bucket holding two compaction records over disjoint inputs, each alone in
/// its component, is named with its own reason, with the switch off and on,
/// and nothing is written. The re-encode rewrites a bucket's one record only.
///
/// Removing the `live.len() > 1` return in `force2_case` leaves the bucket
/// unnamed: `reencode_blocked` is empty with the switch off.
#[tokio::test]
async fn two_surviving_records_are_named_and_write_nothing() {
    for switch in [false, true] {
        let store = put_refusing_store();
        provision(store.inner()).await;
        seed_compacted(store.inner()).await;
        let (pred_key, pred) = stamp_parts_below_target(store.inner()).await;
        put_record_over(store.inner(), &pred, vec![pred.inputs[0].clone()]).await;
        put_record_over(store.inner(), &pred, vec![pred.inputs[1].clone()]).await;
        store
            .inner()
            .delete(&pred_key)
            .await
            .expect("drop the record over both inputs");
        assert_eq!(compaction_records(store.inner()).await.len(), 2);
        let before = all_keys(store.inner()).await;
        let config = CompactorConfig {
            reencode_writer_enabled: switch,
            ..CompactorConfig::default()
        };

        let report = migrate(&store, &config, target()).await;

        assert_eq!(
            report.reencode_blocked,
            reencode_blocked(ReencodeBlockedReason::MultipleRecords { records: 2 }),
            "switch {switch}"
        );
        assert_eq!(report.buckets_migrated, 0, "switch {switch}");
        assert_eq!(report.records_migrated, 0, "switch {switch}");
        assert_eq!(report.not_migrated, Vec::new(), "switch {switch}");
        assert_eq!(report.blocked_buckets, Vec::new(), "switch {switch}");
        assert_eq!(report.verification, stragglers(0, 2), "switch {switch}");
        assert_eq!(puts_attempted(&store), 0, "no PUT, switch {switch}");
        assert_eq!(all_keys(store.inner()).await, before, "switch {switch}");
    }
}

/// Hold the bucket's claim as process 1 on `clock`.
async fn hold_claim(store: &dyn ObjectStoreBackend, clock: &FixedClock) -> ClaimGuard {
    let holder = ClaimGuard::new(
        &bucket(),
        &participant(1, clock),
        ClaimConfig {
            lease_duration: LEASE,
            ..ClaimConfig::default()
        },
        None,
    );
    assert!(matches!(
        holder.acquire(store).await.expect("holder acquires"),
        Acquire::Acquired
    ));
    holder
}

/// A re-encode backs off a bucket whose claim another process holds: the
/// bucket is named with the claim reason on the re-encode path, it is not
/// migrated, and the floor stays unraised.
///
/// Replacing the `ReencodeOutcome::SkippedClaimed` arm of `record_reencode`
/// with one that records nothing leaves `not_migrated` empty.
#[tokio::test]
async fn a_claim_held_elsewhere_skips_the_reencode_and_says_so() {
    let store = MemoryStore::new();
    store.set_clock_ms(u64::try_from(sealed_now_ns() / 1_000_000).expect("positive"));
    let clock = FixedClock::new(sealed_now_ns());
    provision(&store).await;
    seed_compacted(&store).await;
    stamp_parts_below_target(&store).await;
    let _holder = hold_claim(&store, &clock).await;

    let report = migrate(&store, &claiming_cfg(2, &clock), target()).await;

    assert_eq!(
        report.not_migrated,
        not_migrated(
            MigrationPath::Reencode,
            NotMigratedReason::ClaimSkipped {
                reason: ClaimSkipReason::HeldByAnother
            }
        )
    );
    assert_eq!(report.buckets_migrated, 0);
    assert_eq!(report.records_migrated, 0);
    assert_eq!(report.reencode_blocked, Vec::new());
    assert_eq!(report.verification, stragglers(0, 1));
    assert_eq!(floor(&store).await, None);
    assert_eq!(
        compaction_records(&store).await.len(),
        1,
        "nothing published"
    );
}

/// The L0 migration backs off a bucket whose claim another process holds, and
/// the report names it on the L0 path with the claim reason. The target is one
/// above the current version, so both current-version inputs are below it.
///
/// Replacing the walk's `MigrateOutcome::SkippedClaimed { reason }` arm with
/// one that records nothing leaves `not_migrated` empty.
#[tokio::test]
async fn a_claim_held_elsewhere_skips_the_l0_migration_and_says_so() {
    let store = MemoryStore::new();
    store.set_clock_ms(u64::try_from(sealed_now_ns() / 1_000_000).expect("positive"));
    let clock = FixedClock::new(sealed_now_ns());
    provision(&store).await;
    seed_l0(&store).await;
    let _holder = hold_claim(&store, &clock).await;

    let report = migrate(&store, &claiming_cfg(2, &clock), target() + 1).await;

    assert_eq!(
        report.not_migrated,
        not_migrated(
            MigrationPath::L0Migration,
            NotMigratedReason::ClaimSkipped {
                reason: ClaimSkipReason::HeldByAnother
            }
        )
    );
    assert_eq!(report.buckets_migrated, 0);
    assert_eq!(report.records_migrated, 0);
    assert_eq!(report.verification, stragglers(2, 0));
    assert_eq!(floor(&store).await, None);
    assert_eq!(
        compaction_records(&store).await.len(),
        0,
        "nothing published"
    );
}

/// An L0 migration past its deadline builds its parts and abandons its publish.
/// It is not counted as migrated, and the report names it with the reason.
///
/// Removing the walk's `MigrateOutcome::Rewritten { publish:
/// PublishOutcome::Abandoned, .. }` arm lets the `Rewritten { .. }` arm count
/// it: `buckets_migrated` is 1 and `records_migrated` 2.
#[tokio::test]
async fn an_abandoned_l0_migration_is_not_counted() {
    let store = MemoryStore::new();
    provision(&store).await;
    seed_l0(&store).await;
    let config = CompactorConfig {
        max_compaction_lifetime_ns: -1,
        ..CompactorConfig::default()
    };

    let report = migrate(&store, &config, target() + 1).await;

    assert_eq!(report.buckets_migrated, 0);
    assert_eq!(report.records_migrated, 0);
    assert_eq!(
        report.not_migrated,
        not_migrated(
            MigrationPath::L0Migration,
            NotMigratedReason::PublishAbandoned
        )
    );
    assert_eq!(report.verification, stragglers(2, 0));
    assert_eq!(
        compaction_records(&store).await.len(),
        0,
        "nothing published"
    );
}

/// A re-encode past its deadline builds its parts and abandons its publish. It
/// is not counted as migrated, and the report names it with the reason.
///
/// Widening `record_reencode`'s `publish: PublishOutcome::Published` pattern
/// to any publish counts it: `buckets_migrated` is 1 and `records_migrated` 2.
#[tokio::test]
async fn an_abandoned_reencode_is_not_counted() {
    let store = MemoryStore::new();
    provision(&store).await;
    seed_compacted(&store).await;
    stamp_parts_below_target(&store).await;
    let config = CompactorConfig {
        max_compaction_lifetime_ns: -1,
        ..reencode_cfg()
    };

    let report = migrate(&store, &config, target()).await;

    assert_eq!(report.buckets_migrated, 0);
    assert_eq!(report.records_migrated, 0);
    assert_eq!(
        report.not_migrated,
        not_migrated(MigrationPath::Reencode, NotMigratedReason::PublishAbandoned)
    );
    assert_eq!(report.reencode_blocked, Vec::new());
    assert_eq!(report.verification, stragglers(0, 1));
    assert_eq!(
        compaction_records(&store).await.len(),
        1,
        "nothing published"
    );
}

/// A windowless erasure request for every series named `victim`.
fn pending() -> Vec<PendingErasureRequest> {
    let request_id = Uuid::from_u128(0x2406);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: 0,
        predicate: vec![ErasurePredicateMatcher {
            key: "__name__".to_string(),
            value: "victim".to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    vec![PendingErasureRequest {
        request_key: keys::erasure_request_key(&tenant_hash(), Signal::Metrics, request_id)
            .expect("dreq key"),
        request,
    }]
}

/// Wait until `gate` parks exactly one call, check it is a PUT on a part key,
/// and return its id.
async fn parked_part_put(gate: &GateHandle) -> u64 {
    gate.wait_until_held(1).await;
    let held = gate.held_details();
    assert_eq!(held.len(), 1, "the gate parked exactly one call: {held:?}");
    let (id, op, key) = &held[0];
    assert_eq!(*op, Op::Put, "the parked call is a PUT: {key}");
    assert!(key.contains(PART_KEYS), "the parked call is a part: {key}");
    *id
}

/// The re-encode is parked at its first part PUT while an erasure rewrite
/// publishes over the same bucket. The re-encode's pre-publish re-list finds
/// the rewrite record, so it publishes nothing, and the report names the
/// bucket on the re-encode path with the changed record set.
///
/// Replacing the `ReencodeOutcome::RecordSetChanged` arm of `record_reencode`
/// with one that records nothing leaves `not_migrated` empty.
#[tokio::test]
async fn a_record_set_changed_under_the_reencode_is_named() {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    provision(store.inner()).await;
    seed_compacted(store.inner()).await;
    stamp_parts_below_target(store.inner()).await;
    let clock = FixedClock::new(sealed_now_ns());
    let gate = store.hold(Op::Put, Some(PART_KEYS.to_string()), Occurrence::Nth(1));

    let config = reencode_cfg();
    let m = migrate(store.as_ref(), &config, target());
    let e = async {
        let id = parked_part_put(&gate).await;
        let mut memo = MaintainMemo::with_default_interval();
        let outcome = erasure_rewrite_bucket(
            store.as_ref(),
            &clock,
            &CompactorConfig::default(),
            &NoLeases,
            &bucket(),
            &pending(),
            &mut memo,
        )
        .await
        .expect("erasure rewrite");
        assert!(gate.release(id), "the parked part PUT was released");
        outcome
    };
    let (report, e_outcome) = tokio::join!(m, e);

    assert!(
        matches!(
            e_outcome,
            ErasureRewriteOutcome::Rewritten {
                publish: PublishOutcome::Published,
                ..
            }
        ),
        "the erasure rewrite publishes: {e_outcome:?}"
    );
    assert_eq!(
        report.not_migrated,
        not_migrated(MigrationPath::Reencode, NotMigratedReason::RecordSetChanged)
    );
    assert_eq!(report.buckets_migrated, 0);
    assert_eq!(report.records_migrated, 0);
    assert_eq!(report.reencode_blocked, Vec::new());
    assert_eq!(
        report.verification,
        stragglers(0, 1),
        "the predecessor's part, listed until the sweep; the rewrite's parts are current"
    );
    let listing = read::list_bucket(store.as_ref(), &bucket())
        .await
        .expect("list");
    assert_eq!(
        (
            listing.compaction_record_keys.len(),
            listing.rewrite_record_keys.len()
        ),
        (1, 1),
        "the predecessor and the rewrite record, no version 2 record"
    );
}
