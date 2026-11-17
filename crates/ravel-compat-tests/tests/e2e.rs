//! End-to-end reachability for the retained-data contract (ADR-4233 decision
//! 9, "End-to-end reachability").
//!
//! One test drives the whole sequence through the real compat library
//! functions on ONE store with an injected clock: `init` (clean census adopts
//! the baseline), `activate` (durable, monotone, with the operator attestation
//! and a completed fence walk), `migrate` (re-encode an N object to the active
//! N+1 with exact conservation and the old object superseded), `certify` (after
//! the time gate, a strict census and trailer check writes the certificate),
//! and the late-write pass (`compat verify`) finding an injected late N write
//! and setting `contradicted`. It asserts the exact reads before and after and
//! the documented refusal paths (the time gate before it opens; the contradicted
//! certificate after a late write).
//!
//! Scope note: the ADR's end-to-end test also drives these through a real
//! `ravel-server` over HTTP and Flight SQL and through the `ravel-cli compat`
//! commands. On this HEAD there is no `ravel-cli compat` subcommand and the
//! server has no compat CLI surface (the verbs exist only as these library
//! functions, which the CLI handlers would call), and the attestation ships as
//! the `writer_roles_can_read_compat` field rather than the amendment's
//! `--pre-baseline-processes-terminated` flag. This test therefore drives the
//! same library entry points the shipping handlers wrap; the HTTP/Flight/CLI
//! reachability is reported as blocked on that missing product surface.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use ravel_catalog::compat::{CompatClock, CompatKeys, CompatView, FAMILY_RSEG, read_compat};
use ravel_catalog::{
    AbsentPolicy, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS, validate_or_adopt,
};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_maintain::{
    ActivateRequest, CertifyRefusal, CertifyReport, CertifyTimings, CompactorConfig, FixedClock,
    InitConfig, InitOutcome, LateWrite, MaintainCompatPolicy, MigrateBudget, WritableVersions,
    activate, certify, init_compat, late_write_pass, migrate_family, read,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::sys::v1::CompatFamily;
use ravel_segment::{
    IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_DRILL, VERSION_V7,
};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

const SECOND_NS: i64 = 1_000_000_000;
const HOUR_NS: i64 = 3_600 * SECOND_NS;
const NS_PER_HOUR: i64 = HOUR_NS;
const T_REQ_NS: i64 = 60 * SECOND_NS;
/// The hour the seed object lands in; far enough back that it is sealed under
/// the migrate clock.
const HOUR: u32 = 495_000;
const SHARD: u32 = 0;

#[derive(Debug)]
struct ManualClock(AtomicI64);

impl ManualClock {
    fn new(ns: i64) -> Self {
        ManualClock(AtomicI64::new(ns))
    }
    fn set(&self, ns: i64) {
        self.0.store(ns, Ordering::SeqCst);
    }
}

impl CompatClock for ManualClock {
    fn now_unix_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn compat_keys() -> CompatKeys {
    CompatKeys::from_file_keys(&[[5; 32]]).expect("one key")
}

fn tenant() -> TenantId {
    TenantId::new("acme")
}

fn hash() -> TenantHash {
    tenant().hash()
}

fn drill() -> u32 {
    u32::from(VERSION_DRILL)
}

/// The instant the seed object's single sample carries, inside [`HOUR`].
fn event_ns() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR + SECOND_NS
}

/// A now well past the seal margin for [`HOUR`], where migrate will process it.
fn sealed_now_ns() -> i64 {
    (i64::from(HOUR) + 1) * NS_PER_HOUR + 3 * HOUR_NS
}

struct Seed {
    data_key: String,
    record_key: String,
    sample_count: u64,
    series_count: u64,
}

/// Write one real RSEG v7 metrics object and its commit record into the bucket,
/// as an ingest flush would. Returns the keys and the conserved counts.
async fn seed_v7_metrics(store: &MemoryStore) -> Seed {
    let writer_id = Uuid::from_u128(0xE2E);
    let (epoch, seq) = (1u64, 1u64);
    let labels = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: "e2e_gauge".to_string(),
    }])
    .expect("labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(&tenant(), "e2e_gauge", &labels).expect("series id"),
        labels,
        samples: vec![Sample {
            ts_ns: event_ns(),
            value: 42.0,
        }],
    }];
    let written = SegmentWriter::write(
        series,
        SegmentIdentity {
            tenant_hash: hash().0,
            shard: SHARD,
            writer_id: writer_id.to_string(),
            writer_epoch: epoch,
            writer_seq: seq,
        },
        IngestBounds {
            min_ingest_ts_ns: event_ns(),
            max_ingest_ts_ns: event_ns(),
        },
    )
    .expect("write RSEG v7");
    assert_eq!(
        &written.bytes[written.bytes.len() - 4..],
        b"RSG1",
        "an RSEG object"
    );
    let data_key = keys::data_key(
        &hash(),
        Signal::Metrics,
        SHARD,
        writer_id,
        epoch,
        seq,
        &written.summary.blake3,
    )
    .expect("data key");
    let commit = record::build(NewCommitRecord {
        tenant_hash: hash(),
        signal: Signal::Metrics,
        shard: SHARD,
        writer_id,
        writer_epoch: epoch,
        writer_seq: seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: event_ns(),
        max_ingest_ts_ns: event_ns(),
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: event_ns(),
        ingest_hour_bucket: u32::try_from(event_ns().div_euclid(HOUR_NS)).expect("hour"),
    })
    .expect("build commit record");
    let record_key = keys::commit_key_for_record(&commit).expect("commit key");
    store
        .put(&data_key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put data object");
    store
        .put(&record_key, record::encode(&commit), PutOptions::default())
        .await
        .expect("put commit record");
    Seed {
        data_key,
        record_key,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
    }
}

async fn policy(
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<ManualClock>,
) -> MaintainCompatPolicy {
    let view = CompatView::new(store, compat_keys(), clock.clone());
    view.refresh().await.expect("refresh the view");
    MaintainCompatPolicy::new(view, clock)
}

fn timings() -> CertifyTimings {
    CertifyTimings {
        t_req_ns: T_REQ_NS,
        max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
        clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    }
}

async fn rseg_entry(store: &dyn ObjectStoreBackend) -> CompatFamily {
    read_compat(store, &compat_keys())
        .await
        .expect("read sys/compat")
        .expect("present")
        .0
        .families
        .into_iter()
        .find(|f| f.family == FAMILY_RSEG)
        .expect("rseg entry")
}

/// The first instant certify's time gate admits, read from its refusal.
async fn past_time_gate(store: &dyn ObjectStoreBackend, probe_ns: i64) -> i64 {
    match certify(
        store,
        &compat_keys(),
        probe_ns,
        FAMILY_RSEG,
        drill(),
        timings(),
        None,
    )
    .await
    .expect("certify probe")
    {
        CertifyReport::Refused(CertifyRefusal::TimeGate { not_before_ns, .. }) => {
            not_before_ns + SECOND_NS
        }
        other => panic!("expected a time-gate refusal at the probe, got {other:?}"),
    }
}

/// init -> activate -> migrate -> certify -> verify, end to end.
#[tokio::test]
async fn compat_init_activate_migrate_certify_verify() {
    let store = Arc::new(MemoryStore::with_page_size(2));
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let activation_ns = sealed_now_ns();
    let clock = Arc::new(ManualClock::new(activation_ns));

    // Stage 1: a v0.33/baseline store holding one real RSEG 7 object. init runs
    // a clean census and adopts it at the baseline.
    let seed = seed_v7_metrics(store.as_ref()).await;
    validate_or_adopt(
        store.as_ref(),
        &hash(),
        Signal::Metrics,
        SHARD + 1,
        event_ns(),
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");

    let init = init_compat(
        store.as_ref(),
        Some(&compat_keys()),
        clock.as_ref(),
        &InitConfig {
            baseline_release_major: 0,
            baseline_release_minor: 34,
            max_publish_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
        },
    )
    .await
    .expect("init");
    assert_eq!(init.outcome, InitOutcome::Created);
    // The RSEG 7 object is in the adopted histogram.
    let adopted_rseg = init
        .census
        .as_ref()
        .expect("census")
        .adopted
        .get(&(FAMILY_RSEG.to_string(), u32::from(VERSION_V7)))
        .copied()
        .unwrap_or(0);
    assert!(
        adopted_rseg >= 1,
        "the v7 object is adopted: {:?}",
        init.census
    );

    // Stage 2: activate RSEG to the drill version N+1 with the operator
    // attestation. The fence walk completes and raises the bucket's fence.
    let report = activate(
        store.as_ref(),
        Some(&compat_keys()),
        clock.as_ref(),
        &WritableVersions::current().with_family(FAMILY_RSEG, vec![u32::from(VERSION_V7), drill()]),
        &ActivateRequest {
            family: FAMILY_RSEG.to_string(),
            version: drill(),
            activated_by: "e2e".to_string(),
            writer_roles_can_read_compat: true,
            alert_rule_tenants: Vec::new(),
            clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
            resume_from: None,
        },
    )
    .await
    .expect("activate");
    assert_eq!(report.active_write_version, drill());
    assert!(report.walk.complete, "{:?}", report.walk);

    // Stage 3: migrate the v7 object to the active version N+1. Exact
    // conservation: the superseding compaction record's parts are at N+1 and
    // carry the same sample and series counts.
    let config = CompactorConfig {
        compat_policy: Some(policy(dyn_store.clone(), clock.clone()).await),
        ..CompactorConfig::default()
    };
    let migrate = migrate_family(
        store.as_ref(),
        &FixedClock::new(sealed_now_ns()),
        &config,
        hash(),
        Signal::Metrics,
        FAMILY_RSEG,
        drill(),
        SHARD + 1,
        MigrateBudget::unlimited(),
        "e2e migrate",
    )
    .await
    .expect("migrate");
    assert_eq!(migrate.buckets_migrated, 1, "{migrate:?}");
    assert!(migrate.walk_complete, "{migrate:?}");

    // Read the superseding compaction record and assert conservation + version.
    let listing = read::list_bucket(
        store.as_ref(),
        &ravel_maintain::Bucket::new(hash(), Signal::Metrics, SHARD, HOUR),
    )
    .await
    .expect("list bucket");
    let mut migrated_sample_count = 0u64;
    let mut migrated_series_count = 0u64;
    let mut found = false;
    for key in &listing.compaction_record_keys {
        let bytes = store
            .get(key, ravel_object_store::GetRange::Full)
            .await
            .expect("get record")
            .data;
        let rec = record::decode_compaction(&bytes).expect("decode");
        for part in &rec.parts {
            assert_eq!(
                part.segment_format_version,
                drill(),
                "part at N+1: {part:?}"
            );
            migrated_sample_count += part.sample_count;
            migrated_series_count += part.series_count;
            found = true;
        }
    }
    assert!(found, "a superseding compaction record exists");
    assert_eq!(
        migrated_sample_count, seed.sample_count,
        "samples conserved across migrate"
    );
    assert_eq!(
        migrated_series_count, seed.series_count,
        "series conserved across migrate"
    );

    // The old v7 object is superseded; drop it and its record (GC past the
    // protection horizon) so the live set is the N+1 parts only, as it would be
    // by the time certify runs.
    store.delete(&seed.data_key).await.expect("gc old data");
    store.delete(&seed.record_key).await.expect("gc old record");

    // Stage 4: certify. Before the time gate opens it refuses; after, a strict
    // census and trailer check writes the certificate at N+1.
    let now = past_time_gate(store.as_ref(), activation_ns + 2 * HOUR_NS).await;
    clock.set(now);
    let report = certify(
        store.as_ref(),
        &compat_keys(),
        now,
        FAMILY_RSEG,
        drill(),
        timings(),
        None,
    )
    .await
    .expect("certify");
    assert!(
        matches!(
            report,
            CertifyReport::Certified { version, .. } if version == drill()
        ),
        "certify writes the certificate at N+1: {report:?}"
    );
    assert!(
        rseg_entry(store.as_ref()).await.certificate.is_some(),
        "the certificate is recorded"
    );
    assert!(!rseg_entry(store.as_ref()).await.contradicted);

    // Stage 5: a late N write after the certificate. The store clock is
    // advanced first so the late record's LastModified lands after the
    // watermark; compat verify (the late-write pass) then finds it by
    // LastModified and sets contradicted.
    store.set_clock_ms(u64::try_from((now + 2 * HOUR_NS) / 1_000_000).unwrap());
    let late = seed_v7_metrics(store.as_ref()).await;
    let verify = late_write_pass(
        store.as_ref(),
        &compat_keys(),
        now + 3 * HOUR_NS,
        FAMILY_RSEG,
        DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    )
    .await
    .expect("late-write pass");
    assert!(
        verify.late_writes.contains(&LateWrite {
            record_key: late.record_key.clone(),
            version: u32::from(VERSION_V7),
        }),
        "the late v7 write is found: {verify:?}"
    );
    assert!(
        rseg_entry(store.as_ref()).await.contradicted,
        "the certificate is contradicted after the late write"
    );
}
