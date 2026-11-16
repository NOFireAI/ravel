//! The `sys/compat` write policy on the maintenance writers that write parts
//! (ADR-4233 decisions 3, 4 and 6): compaction, erasure rewrite and the
//! force 2 re-encode.
//!
//! `sys/compat` is written with the catalog's own functions and read through
//! a real `CompatView` on a manual clock. Without the `format-drill` feature
//! this build writes one RSEG version, so a test that needs a second writable
//! version gives the policy a two-version RSEG table through
//! `MaintainCompatPolicy::with_family`: {6, 7} with baseline 6, the shape the
//! drill gives. Every refusal is checked against a listing of the whole store
//! taken before and after the run. Interleavings are driven by `FaultStore`
//! hold gates; nothing sleeps.
//!
//! The refusal counter is process-wide, so each test that asserts it refuses
//! under a `(family, reason)` pair no other test in this binary uses.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use common::*;
use prost::Message;
use ravel_catalog::compat::{
    COMPAT_FORMAT_VERSION, COMPAT_VIEW_HORIZON_NS, CompatClock, CompatKeys, CompatRefusal,
    CompatView, FAMILY_COMPACTION_RECORD, FAMILY_REWRITE_RECORD, FAMILY_RSEG, activate_family,
    create_compat,
};
use ravel_catalog::{AbsentPolicy, provisioning_key, raise_compat_fence, validate_or_adopt};
use ravel_commit::{erasure, keys, record, signal};
use ravel_maintain::rewrite::{ReencodeOutcome, reencode_compaction_parts};
use ravel_maintain::{
    CompactionOutcome, CompactorConfig, CompatRunRefusal, FamilyVersions, FixedClock,
    MaintainCompatPolicy, MaintainError, MaintainMemo, NoLeases, RequestLedger, RetentionConfig,
    RetentionOutcome, buckets_held_unknown_section_total, compact_bucket,
    compat_publish_refused_total, erasure_rewrite_bucket, maintain_bucket,
    pending_erasure_requests, read,
};
use ravel_maintain::{MigrateBudget, MigrateHeldBucket, MigrateHoldReason, migrate_family};
use ravel_object_store::fault::{
    FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{ErasurePredicateMatcher, ErasureRequest};
use ravel_proto::sys::v1 as sysproto;
use ravel_segment::{ReaderLimits, VERSION_V7, open_from_full};
use uuid::Uuid;

const T0: i64 = 1_800_000_000_000_000_000;
const HOUR_NS: i64 = 3_600_000_000_000;
/// How long a test waits for a held call before failing instead of hanging.
const HOLD_WAIT: Duration = Duration::from_secs(30);

#[derive(Debug)]
struct ManualClock(AtomicI64);

impl ManualClock {
    fn at(ns: i64) -> Arc<Self> {
        Arc::new(ManualClock(AtomicI64::new(ns)))
    }

    fn advance(&self, ns: i64) {
        self.0.fetch_add(ns, Ordering::SeqCst);
    }
}

impl CompatClock for ManualClock {
    fn now_unix_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn compat_keys() -> CompatKeys {
    CompatKeys::from_file_keys(&[[7; 32]]).expect("one key")
}

fn family(name: &str, version: u32) -> sysproto::CompatFamily {
    sysproto::CompatFamily {
        family: name.to_string(),
        active_write_version: version,
        ..Default::default()
    }
}

async fn write_compat(store: &dyn ObjectStoreBackend, families: Vec<sysproto::CompatFamily>) {
    create_compat(
        store,
        &compat_keys(),
        sysproto::CompatRecord {
            format_version: COMPAT_FORMAT_VERSION,
            baseline_release_major: 0,
            baseline_release_minor: 34,
            families,
            max_publish_lifetime_ns: HOUR_NS,
            ..Default::default()
        },
    )
    .await
    .expect("create sys/compat");
}

async fn activate(store: &dyn ObjectStoreBackend, family: &str, version: u32) {
    activate_family(store, &compat_keys(), family, version, "test", T0)
        .await
        .expect("activate");
}

async fn view_of(store: Arc<dyn ObjectStoreBackend>, clock: Arc<ManualClock>) -> CompatView {
    let view = CompatView::new(store, compat_keys(), clock);
    view.refresh().await.expect("refresh the view");
    view
}

async fn policy_over(
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<ManualClock>,
) -> MaintainCompatPolicy {
    let view = view_of(store, clock.clone()).await;
    MaintainCompatPolicy::new(view, clock)
}

/// The RSEG table of a build that writes 6 and 7, baseline 6.
fn two_rseg() -> FamilyVersions {
    FamilyVersions {
        writable: vec![6, 7],
        baseline: 6,
    }
}

fn config_with(policy: MaintainCompatPolicy) -> CompactorConfig {
    CompactorConfig {
        compat_policy: Some(policy),
        ..CompactorConfig::default()
    }
}

/// The common two-input metrics bucket.
async fn seed_two_inputs(store: &dyn ObjectStoreBackend) {
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
            vec![raw_series("victim", &[("k", "b")], &[(3_000, 3.0)])],
        ),
    ] {
        seed_input(store, &spec).await;
    }
}

async fn provision(store: &dyn ObjectStoreBackend) {
    validate_or_adopt(
        store,
        &tenant_hash(),
        ravel_types::Signal::Metrics,
        8,
        1,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
}

async fn provisioned_compat_epoch(store: &dyn ObjectStoreBackend) -> u64 {
    let key = provisioning_key(&tenant_hash(), ravel_types::Signal::Metrics);
    let got = store.get(&key, GetRange::Full).await.expect("provisioning");
    sysproto::ProvisioningRecord::decode(got.data.as_ref())
        .expect("decode provisioning")
        .compat_epoch
}

/// Every object in the store, with its size and bytes' hash.
async fn snapshot(store: &dyn ObjectStoreBackend) -> Vec<(String, [u8; 32])> {
    let mut out = Vec::new();
    for meta in list_all(store, "").await.expect("list") {
        let bytes = get_full(store, &meta.key).await;
        out.push((meta.key, *blake3::hash(&bytes).as_bytes()));
    }
    out
}

fn l1_keys(listing: &[(String, [u8; 32])]) -> Vec<&str> {
    listing
        .iter()
        .map(|(k, _)| k.as_str())
        .filter(|k| k.contains("/l1/"))
        .collect()
}

fn trailer_version(part: &[u8]) -> u16 {
    open_from_full(part, ReaderLimits::default())
        .expect("open part")
        .version
}

/// The run refused under `family` for `refusal`'s reason.
fn refused_under(err: &MaintainError, family: &str) -> CompatRunRefusal {
    match err {
        MaintainError::CompatRefused {
            family: got,
            refusal,
            ..
        } if got == family => refusal.clone(),
        other => panic!("expected a compat refusal under {family}, got {other:?}"),
    }
}

/// ADR-4233 decision 3: an active RSEG version this build cannot write
/// refuses the compaction before its claim, its catalog reads and every PUT,
/// counted once. The one provisioning read is the run start's.
#[tokio::test]
async fn compaction_refuses_an_unwritable_active_version_before_any_claim_or_put() {
    let store = Arc::new(MemoryStore::new());
    seed_two_inputs(store.as_ref()).await;
    // 9: above the window with or without `format-drill`.
    write_compat(store.as_ref(), vec![family(FAMILY_RSEG, 9)]).await;
    let policy = policy_over(store.clone(), ManualClock::at(T0)).await;
    let ledger = RequestLedger::new();
    let config = CompactorConfig {
        request_ledger: Some(ledger.clone()),
        ..config_with(policy)
    };
    let before = snapshot(store.as_ref()).await;
    let refused = compat_publish_refused_total(FAMILY_RSEG, "active_version_not_writable");

    let err = compact_bucket(
        store.as_ref(),
        &FixedClock::new(sealed_now_ns()),
        &config,
        &bucket(),
    )
    .await
    .expect_err("refused");

    assert!(
        matches!(
            refused_under(&err, FAMILY_RSEG),
            CompatRunRefusal::Compat(CompatRefusal::ActiveVersionNotWritable { active: 9, .. })
        ),
        "{err:?}"
    );
    assert_eq!(snapshot(store.as_ref()).await, before, "nothing is written");
    assert_eq!(
        compat_publish_refused_total(FAMILY_RSEG, "active_version_not_writable"),
        refused + 1
    );
    let report = ledger.report();
    assert_eq!(report.floor_read.requests, 1, "the run start's one read");
    assert_eq!(report.coordinate.requests, 0, "no claim was taken");
    assert_eq!(report.catalog_read.requests, 0, "no catalog was read");
}

/// A refused compaction is no compaction: the maintenance pass carries on
/// and the retention evaluation stands. The refusal here is an absent
/// `sys/compat` while the bucket's provisioning record carries a fence.
#[tokio::test]
async fn a_refused_compaction_counts_as_no_run_and_the_pass_continues() {
    let store = Arc::new(MemoryStore::new());
    seed_two_inputs(store.as_ref()).await;
    provision(store.as_ref()).await;
    raise_compat_fence(
        store.as_ref(),
        &tenant_hash(),
        ravel_types::Signal::Metrics,
        1,
        T0,
    )
    .await
    .expect("fence");
    let policy = policy_over(store.clone(), ManualClock::at(T0)).await;
    let config = config_with(policy);
    let before = snapshot(store.as_ref()).await;
    let refused = compat_publish_refused_total(FAMILY_RSEG, "compat_record_missing");

    let (retention, compaction) = maintain_bucket(
        store.as_ref(),
        &FixedClock::new(sealed_now_ns()),
        &config,
        &RetentionConfig::default(),
        &NoLeases,
        &bucket(),
    )
    .await
    .expect("the pass carries on past the refusal");

    assert_eq!(retention, RetentionOutcome::NoPolicy);
    assert!(compaction.is_none(), "{compaction:?}");
    assert_eq!(snapshot(store.as_ref()).await, before);
    assert_eq!(
        compat_publish_refused_total(FAMILY_RSEG, "compat_record_missing"),
        refused + 1
    );
}

/// An erasure rewrite whose rewrite-record version is not writable is
/// refused before its claim, its build and every PUT, and its request stays
/// pending with no `.done`.
#[tokio::test]
async fn erasure_refuses_an_unwritable_active_version_and_leaves_the_request_pending() {
    let store = Arc::new(MemoryStore::new());
    seed_two_inputs(store.as_ref()).await;
    write_compat(store.as_ref(), vec![family(FAMILY_REWRITE_RECORD, 2)]).await;
    let request_id = Uuid::from_u128(0x4245);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant_hash().0.to_vec(),
        signal: signal::to_proto(ravel_types::Signal::Metrics) as i32,
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
    let request_key =
        keys::erasure_request_key(&tenant_hash(), ravel_types::Signal::Metrics, request_id)
            .expect("dreq key");
    store
        .put(
            &request_key,
            erasure::encode_request(&request),
            PutOptions::default(),
        )
        .await
        .expect("put the request");
    let pending =
        pending_erasure_requests(store.as_ref(), &tenant_hash(), ravel_types::Signal::Metrics)
            .await
            .expect("list pending");
    assert_eq!(pending.len(), 1);
    let policy = policy_over(store.clone(), ManualClock::at(T0)).await;
    let config = config_with(policy);
    let mut memo = MaintainMemo::with_default_interval();
    let before = snapshot(store.as_ref()).await;
    let refused =
        compat_publish_refused_total(FAMILY_REWRITE_RECORD, "active_version_not_writable");

    let err = erasure_rewrite_bucket(
        store.as_ref(),
        &FixedClock::new(sealed_now_ns()),
        &config,
        &NoLeases,
        &bucket(),
        &pending,
        &mut memo,
    )
    .await
    .expect_err("refused");

    assert!(
        matches!(
            refused_under(&err, FAMILY_REWRITE_RECORD),
            CompatRunRefusal::Compat(CompatRefusal::ActiveVersionNotWritable { active: 2, .. })
        ),
        "{err:?}"
    );
    assert_eq!(snapshot(store.as_ref()).await, before, "nothing is written");
    assert_eq!(
        compat_publish_refused_total(FAMILY_REWRITE_RECORD, "active_version_not_writable"),
        refused + 1
    );
    let still =
        pending_erasure_requests(store.as_ref(), &tenant_hash(), ravel_types::Signal::Metrics)
            .await
            .expect("list pending");
    assert_eq!(still.len(), 1, "the request is still pending");
    let done_key =
        keys::erasure_completion_key(&tenant_hash(), ravel_types::Signal::Metrics, request_id)
            .expect("done key");
    assert!(store.head(&done_key).await.is_err(), "no .done is written");
}

/// A view that goes stale between the run start and the part PUT abandons
/// the run without the PUT. The run is held on its first input read, which
/// comes after the run start (the ledger shows the start's provisioning
/// read), and the compat clock is moved past the horizon while it waits. A
/// check made only at the run start passes this run through.
#[tokio::test]
async fn a_view_going_stale_mid_run_abandons_the_compaction_before_its_put() {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    seed_two_inputs(store.inner()).await;
    write_compat(store.inner(), vec![family(FAMILY_RSEG, 7)]).await;
    let clock = ManualClock::at(T0);
    let policy = policy_over(store.clone(), clock.clone()).await;
    let ledger = RequestLedger::new();
    let config = CompactorConfig {
        request_ledger: Some(ledger.clone()),
        ..config_with(policy)
    };
    let gate = store.hold(Op::Get, Some("/l0/".to_string()), Occurrence::Nth(1));
    let before = snapshot(store.inner()).await;
    let refused = compat_publish_refused_total(FAMILY_RSEG, "view_stale");
    let maintain_clock = FixedClock::new(sealed_now_ns());
    let target = bucket();

    let (outcome, ()) = tokio::join!(
        compact_bucket(store.as_ref(), &maintain_clock, &config, &target),
        async {
            tokio::time::timeout(HOLD_WAIT, gate.wait_until_held(1))
                .await
                .expect("the run reads an input");
            assert_eq!(
                ledger.report().floor_read.requests,
                1,
                "the run started before the held read"
            );
            clock.advance(COMPAT_VIEW_HORIZON_NS);
            for id in gate.held() {
                gate.release(id);
            }
        }
    );

    let err = outcome.expect_err("abandoned");
    assert!(
        matches!(
            refused_under(&err, FAMILY_RSEG),
            CompatRunRefusal::Compat(CompatRefusal::ViewStale { .. })
        ),
        "{err:?}"
    );
    assert_eq!(snapshot(store.inner()).await, before, "nothing is written");
    assert_eq!(
        compat_publish_refused_total(FAMILY_RSEG, "view_stale"),
        refused + 1
    );
}

/// The store, compat clock and config of a bucket whose RSEG family was
/// activated from 6 to 7 after the bucket was provisioned, under a policy
/// that writes {6, 7}: the bucket's first compaction at 7 owes the fence.
async fn activated_bucket(
    plan: FaultPlan,
) -> (
    Arc<FaultStore<MemoryStore>>,
    Arc<ManualClock>,
    CompactorConfig,
) {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    seed_two_inputs(store.inner()).await;
    write_compat(store.inner(), vec![family(FAMILY_RSEG, 6)]).await;
    provision(store.inner()).await;
    activate(store.inner(), FAMILY_RSEG, 7).await;
    assert_eq!(provisioned_compat_epoch(store.inner()).await, 0);
    let clock = ManualClock::at(T0);
    let policy = policy_over(store.clone(), clock.clone())
        .await
        .with_family(FAMILY_RSEG, two_rseg());
    (store, clock, config_with(policy))
}

/// ADR-4233 decision 4: the bucket's first publish above the baseline raises
/// its provisioning record's fence to the activation epoch before the part
/// PUT. The part PUT is held, and the fence is read while it waits. A fence
/// raised after the first PUT, or only before the record PUT, fails here.
#[tokio::test]
async fn the_first_activated_publish_raises_the_fence_before_the_part_put() {
    let (store, _clock, config) = activated_bucket(FaultPlan::empty()).await;
    let gate = store.hold(Op::Put, Some("/l1/".to_string()), Occurrence::Nth(1));
    let maintain_clock = FixedClock::new(sealed_now_ns());
    let target = bucket();

    let (outcome, epoch_at_put) = tokio::join!(
        compact_bucket(store.as_ref(), &maintain_clock, &config, &target),
        async {
            tokio::time::timeout(HOLD_WAIT, gate.wait_until_held(1))
                .await
                .expect("the run PUTs a part");
            let epoch = provisioned_compat_epoch(store.inner()).await;
            for id in gate.held() {
                gate.release(id);
            }
            epoch
        }
    );

    let outcome = outcome.expect("compacts");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );
    assert_eq!(epoch_at_put, 1, "the fence was up before the part PUT");
    assert_eq!(provisioned_compat_epoch(store.inner()).await, 1);
    let record = fetch_compaction_record(store.inner(), &bucket()).await;
    for part in &record.parts {
        assert_eq!(part.segment_format_version, u32::from(VERSION_V7));
        let key = keys::reconstruct_l1_part_key(&record, part).expect("part key");
        assert_eq!(
            trailer_version(&get_full(store.inner(), &key).await),
            VERSION_V7
        );
    }
}

/// A fence CAS the store rejects refuses the run, counted, and nothing is
/// written: no part, no record, and the fence stays where it was.
#[tokio::test]
async fn a_failed_fence_cas_leaves_nothing_written() {
    let fence_key = provisioning_key(&tenant_hash(), ravel_types::Signal::Metrics);
    let plan = FaultPlan::empty().with_rule(
        Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains(fence_key),
    );
    let (store, _clock, config) = activated_bucket(plan).await;
    let before = snapshot(store.inner()).await;
    let refused = compat_publish_refused_total(FAMILY_RSEG, "fence_failed");

    let err = compact_bucket(
        store.as_ref(),
        &FixedClock::new(sealed_now_ns()),
        &config,
        &bucket(),
    )
    .await
    .expect_err("refused");

    assert!(
        matches!(
            refused_under(&err, FAMILY_RSEG),
            CompatRunRefusal::FenceFailed {
                compat_epoch: 1,
                ..
            }
        ),
        "{err:?}"
    );
    assert_eq!(
        store.fault_count(Op::Put, FaultKind::FailedConditionalWrite),
        1,
        "the fence CAS was attempted and rejected"
    );
    assert_eq!(snapshot(store.inner()).await, before, "nothing is written");
    assert_eq!(provisioned_compat_epoch(store.inner()).await, 0);
    assert_eq!(
        compat_publish_refused_total(FAMILY_RSEG, "fence_failed"),
        refused + 1
    );
}

/// The version is fixed per run, not per PUT. The run starts with RSEG
/// active at 7; while it is held on its first input read, RSEG is activated
/// to 8, which this policy cannot write, and the view is refreshed. The run
/// still writes its part and record at 7. A writer that asks the view again
/// at the build or at the PUT refuses or fails here.
#[tokio::test]
async fn the_version_is_chosen_once_per_run_not_per_put() {
    let store = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    seed_two_inputs(store.inner()).await;
    write_compat(store.inner(), vec![family(FAMILY_RSEG, 6)]).await;
    activate(store.inner(), FAMILY_RSEG, 7).await;
    let clock = ManualClock::at(T0);
    let policy = policy_over(store.clone(), clock.clone())
        .await
        .with_family(FAMILY_RSEG, two_rseg());
    let view = policy.view().clone();
    let config = config_with(policy);
    let gate = store.hold(Op::Get, Some("/l0/".to_string()), Occurrence::Nth(1));
    let maintain_clock = FixedClock::new(sealed_now_ns());
    let target = bucket();

    let (outcome, ()) = tokio::join!(
        compact_bucket(store.as_ref(), &maintain_clock, &config, &target),
        async {
            tokio::time::timeout(HOLD_WAIT, gate.wait_until_held(1))
                .await
                .expect("the run reads an input");
            activate(store.inner(), FAMILY_RSEG, 8).await;
            view.refresh().await.expect("refresh");
            for id in gate.held() {
                gate.release(id);
            }
        }
    );

    assert_eq!(
        view.seen_family(FAMILY_RSEG)
            .map(|f| f.active_write_version),
        Some(8),
        "the view saw the activation mid-run"
    );
    let outcome = outcome.expect("the run keeps its start's version");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );
    let record = fetch_compaction_record(store.inner(), &bucket()).await;
    for part in &record.parts {
        assert_eq!(part.segment_format_version, 7);
        let key = keys::reconstruct_l1_part_key(&record, part).expect("part key");
        assert_eq!(trailer_version(&get_full(store.inner(), &key).await), 7);
    }
}

/// Seed the bucket's second input with an unknown section kind spliced in,
/// its commit record naming the spliced bytes and stamped at
/// `recorded_version`. Returns the data key.
async fn seed_input_with_unknown_section(
    store: &dyn ObjectStoreBackend,
    recorded_version: u32,
) -> String {
    let spec = InputSpec::new(
        Uuid::from_u128(2),
        10,
        2,
        vec![raw_series("victim", &[("k", "b")], &[(3_000, 3.0)])],
    );
    let written = build_v5_l0(
        &spec.series,
        spec.hour,
        (spec.created_unix_ns, spec.epoch, spec.seq),
    );
    let bytes = with_unknown_section(&written.bytes);
    assert_eq!(
        open_from_full(&bytes, ReaderLimits::default())
            .expect("open spliced")
            .unknown_section_kinds,
        vec![UNKNOWN_KIND]
    );
    let content_hash = *blake3::hash(&bytes).as_bytes();
    let data_key = keys::data_key(
        &tenant_hash(),
        ravel_types::Signal::Metrics,
        SHARD,
        spec.writer_id,
        spec.epoch,
        spec.seq,
        &content_hash,
    )
    .expect("data key");
    let rec = record::build(record::NewCommitRecord {
        tenant_hash: tenant_hash(),
        signal: ravel_types::Signal::Metrics,
        shard: SHARD,
        writer_id: spec.writer_id,
        writer_epoch: spec.epoch,
        writer_seq: spec.seq,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: spec.created_unix_ns,
        max_ingest_ts_ns: spec.created_unix_ns,
        segment_format_version: recorded_version,
        created_unix_ns: spec.created_unix_ns,
        ingest_hour_bucket: spec.hour,
    })
    .expect("build commit record");
    store
        .put(&data_key, bytes.into(), PutOptions::default())
        .await
        .expect("put data object");
    store
        .put(
            &keys::commit_key_for_record(&rec).expect("commit key"),
            record::encode(&rec),
            PutOptions::default(),
        )
        .await
        .expect("put commit record");
    data_key
}

/// Hazard H6 (ADR-4233 decision 4): an input whose reader skipped a section
/// kind this build does not know holds its bucket, counted, because the
/// re-encode would drop the section. No part and no record is written. A
/// refusal raised after the part PUT, or a hold that lets the merge go on,
/// fails here.
#[tokio::test]
async fn an_unknown_section_kind_holds_the_bucket_in_compaction() {
    let store = MemoryStore::new();
    seed_input(
        &store,
        &InputSpec::new(
            Uuid::from_u128(1),
            10,
            1,
            vec![raw_series("keep", &[("k", "a")], &[(1_000, 1.0)])],
        ),
    )
    .await;
    seed_input_with_unknown_section(&store, u32::from(VERSION_V7)).await;
    let before = snapshot(&store).await;
    let held = buckets_held_unknown_section_total();

    let outcome = compact_bucket(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        &bucket(),
    )
    .await
    .expect("a hold is an outcome");

    assert_eq!(
        outcome,
        CompactionOutcome::HeldUnknownSection {
            version: VERSION_V7,
            kinds: vec![UNKNOWN_KIND],
        }
    );
    assert_eq!(buckets_held_unknown_section_total(), held + 1);
    let after = snapshot(&store).await;
    assert_eq!(l1_keys(&after), Vec::<&str>::new(), "no part is written");
    assert_eq!(after, before, "nothing is written");
}

/// H6 in `migrate`: the walk holds a bucket whose below-target input carries
/// an unknown section kind, writes nothing for it, and raises no floor. The
/// commit record says one version below the object's trailer, and the hold
/// names the trailer's version, the one the object was written at.
#[tokio::test]
async fn an_unknown_section_kind_holds_the_bucket_in_migrate() {
    let store = MemoryStore::new();
    provision(&store).await;
    let data_key = seed_input_with_unknown_section(&store, u32::from(VERSION_V7) - 1).await;
    let before = snapshot(&store).await;

    let err = migrate_family(
        &store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        tenant_hash(),
        ravel_types::Signal::Metrics,
        FAMILY_RSEG,
        u32::from(VERSION_V7),
        8,
        MigrateBudget::unlimited(),
        "test",
    )
    .await
    .expect_err("held");

    let MaintainError::MigrateHeld { held, report } = err else {
        panic!("expected MigrateHeld, got {err:?}");
    };
    assert_eq!(
        held,
        vec![MigrateHeldBucket {
            shard: SHARD,
            ingest_hour: HOUR,
            reason: MigrateHoldReason::UnknownSection {
                object_key: data_key,
                version: VERSION_V7,
                kinds: vec![UNKNOWN_KIND],
            },
        }]
    );
    assert_eq!(report.buckets_migrated, 0, "{report:?}");
    let after = snapshot(&store).await;
    assert_eq!(l1_keys(&after), Vec::<&str>::new(), "no part is written");
    let floor = ravel_catalog::current_floor_from_store(
        &store,
        &tenant_hash(),
        ravel_types::Signal::Metrics,
        FAMILY_RSEG,
    )
    .await
    .expect("floor");
    assert_eq!(floor, None, "a held walk raises no floor");
    let written: Vec<_> = after.iter().filter(|o| !before.contains(o)).collect();
    assert!(
        written.iter().all(|(k, _)| k.contains("/sys/")),
        "only the walk's own cursor state may be written: {written:?}"
    );
}

/// A bucket compacted without a policy whose one record is then stamped one
/// version below the current part version, so a force 2 re-encode has work.
async fn reencodable_bucket(store: &dyn ObjectStoreBackend) {
    seed_two_inputs(store).await;
    let compacted = compact_bucket(
        store,
        &FixedClock::new(sealed_now_ns()),
        &CompactorConfig::default(),
        &bucket(),
    )
    .await
    .expect("compact without a policy");
    assert!(
        matches!(compacted, CompactionOutcome::Compacted { .. }),
        "{compacted:?}"
    );
    let listing = read::list_bucket(store, &bucket()).await.expect("list");
    let key = &listing.compaction_record_keys[0];
    let mut rec = record::decode_compaction(&get_full(store, key).await).expect("decode record");
    for part in &mut rec.parts {
        part.segment_format_version = u32::from(VERSION_V7) - 1;
    }
    store
        .put(key, record::encode_compaction(&rec), PutOptions::default())
        .await
        .expect("stamp the record below target");
}

/// Force 2 writes a version 2 compaction record only when
/// `compaction-record` is active at 2. At the baseline it is refused with a
/// typed error before its claim and every PUT; once activated the same
/// bucket re-encodes and its new record is version 2.
#[tokio::test]
async fn force2_needs_compaction_record_active_at_2() {
    let store = Arc::new(MemoryStore::new());
    reencodable_bucket(store.as_ref()).await;
    write_compat(store.as_ref(), vec![family(FAMILY_COMPACTION_RECORD, 1)]).await;
    let clock = ManualClock::at(T0);
    let config = CompactorConfig {
        reencode_writer_enabled: true,
        ..config_with(policy_over(store.clone(), clock.clone()).await)
    };
    let before = snapshot(store.as_ref()).await;
    let refused =
        compat_publish_refused_total(FAMILY_COMPACTION_RECORD, "compaction_record_not_active");
    let maintain_clock = FixedClock::new(sealed_now_ns());

    let err = reencode_compaction_parts(store.as_ref(), &maintain_clock, &config, &bucket())
        .await
        .expect_err("refused at the baseline");

    assert_eq!(
        refused_under(&err, FAMILY_COMPACTION_RECORD),
        CompatRunRefusal::CompactionRecordNotActive { active: 1 }
    );
    assert_eq!(snapshot(store.as_ref()).await, before, "nothing is written");
    assert_eq!(
        compat_publish_refused_total(FAMILY_COMPACTION_RECORD, "compaction_record_not_active"),
        refused + 1
    );

    activate(store.as_ref(), FAMILY_COMPACTION_RECORD, 2).await;
    let config = CompactorConfig {
        reencode_writer_enabled: true,
        ..config_with(policy_over(store.clone(), clock).await)
    };
    let outcome = reencode_compaction_parts(store.as_ref(), &maintain_clock, &config, &bucket())
        .await
        .expect("re-encodes once active");
    let ReencodeOutcome::Reencoded {
        superseded_record_key,
        ..
    } = outcome
    else {
        panic!("expected Reencoded, got {outcome:?}");
    };
    let listing = read::list_bucket(store.as_ref(), &bucket())
        .await
        .expect("list");
    let new_key = listing
        .compaction_record_keys
        .iter()
        .find(|k| **k != superseded_record_key)
        .expect("a new record");
    let new_record =
        record::decode_compaction(&get_full(store.as_ref(), new_key).await).expect("decode");
    assert_eq!(
        new_record.format_version,
        record::COMPACTION_SUPERSEDING_FORMAT_VERSION
    );
}

/// `compaction-record` is a ceiling on a plain compaction: an active version
/// this build cannot write refuses the run before its claim and every PUT,
/// counted under `compaction-record`, even though the run would write
/// version 1.
#[tokio::test]
async fn compaction_refuses_an_unwritable_compaction_record_version_before_any_claim() {
    let store = Arc::new(MemoryStore::new());
    seed_two_inputs(store.as_ref()).await;
    write_compat(
        store.as_ref(),
        vec![
            family(FAMILY_RSEG, u32::from(VERSION_V7)),
            family(FAMILY_COMPACTION_RECORD, 9),
        ],
    )
    .await;
    let ledger = RequestLedger::new();
    let config = CompactorConfig {
        request_ledger: Some(ledger.clone()),
        ..config_with(policy_over(store.clone(), ManualClock::at(T0)).await)
    };
    let before = snapshot(store.as_ref()).await;
    let refused =
        compat_publish_refused_total(FAMILY_COMPACTION_RECORD, "active_version_not_writable");

    let err = compact_bucket(
        store.as_ref(),
        &FixedClock::new(sealed_now_ns()),
        &config,
        &bucket(),
    )
    .await
    .expect_err("refused");

    assert!(
        matches!(
            refused_under(&err, FAMILY_COMPACTION_RECORD),
            CompatRunRefusal::Compat(CompatRefusal::ActiveVersionNotWritable { active: 9, .. })
        ),
        "{err:?}"
    );
    assert_eq!(snapshot(store.as_ref()).await, before, "nothing is written");
    assert_eq!(
        compat_publish_refused_total(FAMILY_COMPACTION_RECORD, "active_version_not_writable"),
        refused + 1
    );
    assert_eq!(ledger.report().coordinate.requests, 0, "no claim was taken");
}

/// A plain compaction under `compaction-record` active at 2 still writes a
/// version 1 record, whose content needs nothing newer, and so owes the
/// bucket no fence: the activation of a ceiling family raises none.
#[tokio::test]
async fn compaction_record_active_at_2_still_writes_version_1_and_owes_no_fence() {
    let store = Arc::new(MemoryStore::new());
    seed_two_inputs(store.as_ref()).await;
    write_compat(
        store.as_ref(),
        vec![
            family(FAMILY_RSEG, u32::from(VERSION_V7)),
            family(FAMILY_COMPACTION_RECORD, 1),
        ],
    )
    .await;
    provision(store.as_ref()).await;
    activate(store.as_ref(), FAMILY_COMPACTION_RECORD, 2).await;
    let config = config_with(policy_over(store.clone(), ManualClock::at(T0)).await);

    let outcome = compact_bucket(
        store.as_ref(),
        &FixedClock::new(sealed_now_ns()),
        &config,
        &bucket(),
    )
    .await
    .expect("compacts");

    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );
    let written = fetch_compaction_record(store.as_ref(), &bucket()).await;
    assert_eq!(written.format_version, record::COMPACTION_FORMAT_VERSION);
    assert_eq!(
        provisioned_compat_epoch(store.as_ref()).await,
        0,
        "no fence"
    );
}
