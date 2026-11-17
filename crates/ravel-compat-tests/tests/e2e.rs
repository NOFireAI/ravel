//! End-to-end reachability for the retained-data contract (ADR-4233 decision
//! 9, "End-to-end reachability"), through the compat library functions on ONE
//! store with an injected clock:
//!
//!   - `init` adopts the one RSEG 7 object at the baseline;
//!   - `activate` moves RSEG to the drill version N+1 with the attestation and a
//!     completed fence walk;
//!   - `migrate` re-encodes the object at N+1. The query snapshot over the hour
//!     (`Catalog::resolve`) then names only the N+1 part, which decodes to the
//!     seed's exact series, `SeriesId`, timestamp and value bits, while the v7
//!     object is still on the store;
//!   - `certify` refuses with the time gate up to `not_before_ns`, which the
//!     test computes from the ADR's terms, and returns `Contradicted` at the v7
//!     member one nanosecond later;
//!   - the superseded-input sweep (`sweep_superseded`) deletes the v7 object and
//!     its record once its horizon and pinned window pass;
//!   - `certify` then writes an uncontradicted certificate at N+1;
//!   - the late-write pass finds a later v7 write and sets `contradicted`.
//!
//! The ADR's test also drives these through a running `ravel-server` (HTTP and
//! Flight SQL) and the `ravel-cli compat` commands. This tree has no `ravel-cli
//! compat` subcommand, so this test calls the library entry points directly.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use prost::Message;
use ravel_catalog::compat::{
    COMPAT_VIEW_HORIZON_NS, CompatClock, CompatKeys, CompatView, FAMILY_RSEG, read_compat,
};
use ravel_catalog::{
    AbsentPolicy, Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, provisioning_key, validate_or_adopt,
};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_maintain::{
    ActivateRequest, CertifyRefusal, CertifyReport, CertifyTimings, CompactorConfig, FixedClock,
    InitConfig, InitOutcome, LateWrite, MaintainCompatPolicy, MigrateBudget, NoLeases,
    WritableVersions, activate, certify, init_compat, late_write_pass, migrate_family, read,
    sweep_superseded,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_proto::sys::v1::{self as sysproto, CompatFamily};
use ravel_segment::{
    IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInput, VERSION_DRILL,
    VERSION_V7, open_from_full,
};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange,
};
use uuid::Uuid;

use common::DecodedSeries;

const SECOND_NS: i64 = 1_000_000_000;
const HOUR_NS: i64 = 3_600 * SECOND_NS;
const NS_PER_HOUR: i64 = HOUR_NS;
const T_REQ_NS: i64 = 60 * SECOND_NS;
/// The hour the seed object lands in; far enough back that it is sealed under
/// the migrate clock.
const HOUR: u32 = 495_000;
const SHARD: u32 = 0;
const SEED_VALUE: f64 = 42.0;

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
    series_id: SeriesId,
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
    let series_id = SeriesId::compute(&tenant(), "e2e_gauge", &labels).expect("series id");
    let series = vec![SeriesInput {
        series_id,
        labels,
        samples: vec![Sample {
            ts_ns: event_ns(),
            value: SEED_VALUE,
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
        series_id,
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

async fn certify_at(store: &dyn ObjectStoreBackend, now_ns: i64) -> CertifyReport {
    certify(
        store,
        &compat_keys(),
        now_ns,
        FAMILY_RSEG,
        drill(),
        timings(),
        None,
    )
    .await
    .expect("certify")
}

/// The data objects a query over [`HOUR`] reads: the catalog snapshot the
/// query engine resolves.
async fn query_snapshot_keys(store: Arc<dyn ObjectStoreBackend>, now_ns: i64) -> Vec<String> {
    let catalog = Catalog::new(
        store,
        CatalogConfig {
            shard_count: SHARD + 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let start = i64::from(HOUR) * NS_PER_HOUR;
    let snapshot = catalog
        .resolve(
            &hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: start,
                end_ns: start + NS_PER_HOUR,
            },
            &[],
            now_ns,
        )
        .await
        .expect("resolve the query snapshot");
    snapshot
        .segments
        .into_iter()
        .map(|s| s.data_object_key)
        .collect()
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
    let adopted_rseg = init
        .census
        .as_ref()
        .expect("census")
        .adopted
        .get(&(FAMILY_RSEG.to_string(), u32::from(VERSION_V7)))
        .copied()
        .unwrap_or(0);
    assert_eq!(
        adopted_rseg, 1,
        "the one v7 object is adopted: {:?}",
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
            .get(key, GetRange::Full)
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

    // The read path after migrate: the query snapshot names exactly the N+1
    // part, never the v7 object, which is still on the store. The part decodes
    // to the seed's series and sample exactly.
    let live = query_snapshot_keys(dyn_store.clone(), sealed_now_ns()).await;
    assert_eq!(live.len(), 1, "one live object: {live:?}");
    assert_ne!(live[0], seed.data_key, "the v7 object is not read");
    assert!(
        store.head(&seed.data_key).await.is_ok(),
        "the v7 object is unreachable while still stored"
    );
    let part = store.get(&live[0], GetRange::Full).await.expect("get").data;
    assert_eq!(
        open_from_full(&part, ReaderLimits::default())
            .expect("open part")
            .version,
        VERSION_DRILL,
        "the live part's trailer is N+1"
    );
    assert_eq!(
        common::decode_rseg(&part).expect("decode the part"),
        vec![DecodedSeries {
            labels: vec![(METRIC_NAME_LABEL.to_string(), "e2e_gauge".to_string())],
            series_id: seed.series_id.0,
            samples: vec![(event_ns(), SEED_VALUE.to_bits())],
        }],
        "the N+1 part holds 42.0 at event_ns() bit for bit"
    );

    // Stage 4: the time gate. `not_before_ns` from the ADR's terms (decision
    // 7): the later of activation + H + P + T_req and the bucket's fence time +
    // G_pre, where G_pre = 3 h + 2 x max_flush_lifetime + P + T_req, plus the
    // clock-skew allowance.
    let entry = rseg_entry(store.as_ref()).await;
    assert_eq!(entry.activated_unix_ns, activation_ns);
    let prov = sysproto::ProvisioningRecord::decode(
        store
            .get(&provisioning_key(&hash(), Signal::Metrics), GetRange::Full)
            .await
            .expect("get provisioning")
            .data
            .as_ref(),
    )
    .expect("decode provisioning");
    assert_eq!(
        prov.compat_fenced_unix_ns, activation_ns,
        "the walk fenced the bucket at activation"
    );
    let p_ns = DEFAULT_MAX_FLUSH_LIFETIME_NS;
    let activation_term = activation_ns + COMPAT_VIEW_HORIZON_NS + p_ns + T_REQ_NS;
    let g_pre = 3 * HOUR_NS + 2 * DEFAULT_MAX_FLUSH_LIFETIME_NS + p_ns + T_REQ_NS;
    let not_before =
        activation_term.max(prov.compat_fenced_unix_ns + g_pre) + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
    assert_eq!(
        certify_at(store.as_ref(), not_before).await,
        CertifyReport::Refused(CertifyRefusal::TimeGate {
            now_ns: not_before,
            not_before_ns: not_before,
        }),
        "certify refuses at exactly not_before_ns"
    );

    // One nanosecond past the gate, with the v7 object still stored, the census
    // finds it: contradicted, and no certificate.
    let gate_open = not_before + 1;
    match certify_at(store.as_ref(), gate_open).await {
        CertifyReport::Contradicted {
            member_key,
            found_version,
        } => {
            assert_eq!(member_key, seed.data_key, "the v7 member");
            assert_eq!(found_version, u32::from(VERSION_V7));
        }
        other => panic!("certify over the v7 object must be Contradicted: {other:?}"),
    }
    assert!(rseg_entry(store.as_ref()).await.certificate.is_none());

    // GC through the superseded-input sweep: the first pass after the
    // protection horizon stamps the pinned-window marker and holds the record
    // and object; the pass once the window has aged deletes both.
    let t1 = gate_open.max(sealed_now_ns() + config.protection_horizon_ns + 1);
    let sweep = |now: i64| {
        let store = store.clone();
        let config = config.clone();
        async move {
            sweep_superseded(
                store.as_ref(),
                &FixedClock::new(now),
                &config,
                &NoLeases,
                &hash(),
                Signal::Metrics,
                SHARD,
            )
            .await
            .expect("sweep superseded")
        }
    };
    let held = sweep(t1).await;
    assert_eq!(
        (held.records_deleted, held.data_deleted),
        (0, 0),
        "{held:?}"
    );
    assert_eq!(held.held_by_pinned_window, 2, "{held:?}");
    let window = config.max_query_duration_ns
        + config.head_cache_ttl_ns
        + 4 * config.clock_skew_allowance_ns;
    let now = t1 + window;
    let swept = sweep(now).await;
    assert_eq!(
        (swept.records_deleted, swept.data_deleted),
        (1, 1),
        "{swept:?}"
    );
    assert!(store.head(&seed.data_key).await.is_err(), "v7 object gone");
    assert!(
        store.head(&seed.record_key).await.is_err(),
        "v7 record gone"
    );

    // Stage 5: certify writes the certificate at N+1.
    clock.set(now);
    let report = certify_at(store.as_ref(), now).await;
    assert!(
        matches!(
            report,
            CertifyReport::Certified { version, member_count: 1, .. } if version == drill()
        ),
        "certify writes the certificate at N+1 over the one part: {report:?}"
    );
    let entry = rseg_entry(store.as_ref()).await;
    let certificate = entry.certificate.as_ref().expect("certificate recorded");
    assert_eq!(certificate.version, drill());
    assert_eq!(certificate.certified_unix_ns, now);
    assert!(!entry.contradicted);

    // Stage 6: a late N write after the certificate. The store clock is
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
