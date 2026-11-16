//! The part writers under `format-drill` (ADR-4233 decision 3): RSEG writes 7
//! and the drill version 8, baseline 7, while `SegmentVersion::CURRENT` stays
//! 7. A run's version checks (the format floor, the newer-than-writable input
//! check, force 2's below-target count) compare against the version the run
//! writes, which here differs from `CURRENT`. Each test names the comparison
//! whose return to `CURRENT` fails it.
//!
//! `sys/compat` is written with the catalog's own functions and read through a
//! real `CompatView`. An input at 8 is a production-written object with its
//! trailer version raised and its footer crc recomputed, which is what the
//! drill writer emits.
#![cfg(feature = "format-drill")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use common::*;
use ravel_catalog::compat::{
    COMPAT_FORMAT_VERSION, CompatClock, CompatKeys, CompatView, FAMILY_COMPACTION_RECORD,
    FAMILY_RSEG, activate_family, create_compat,
};
use ravel_catalog::{AbsentPolicy, FloorBasis, raise_format_floor, validate_or_adopt};
use ravel_commit::{keys, record};
use ravel_ingest::{FormatFamily, FormatFloorMetrics, FormatFloorPolicy};
use ravel_maintain::{
    CompactionOutcome, CompactorConfig, FixedClock, MaintainCompatPolicy, MaintainError,
    MigrateBudget, compact_bucket, migrate_family, read,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::sys::v1 as sysproto;
use ravel_segment::{ReaderLimits, TRAILER_LEN, VERSION_DRILL, VERSION_V7, open_from_full};
use ravel_types::Signal;
use uuid::Uuid;

const T0: i64 = 1_800_000_000_000_000_000;
const HOUR_NS: i64 = 3_600_000_000_000;

/// Shards of the provisioning record: wide enough for [`SHARD`].
const SHARDS: u32 = SHARD + 1;

fn drill() -> u32 {
    u32::from(VERSION_DRILL)
}

fn v7() -> u32 {
    u32::from(VERSION_V7)
}

#[derive(Debug)]
struct ManualClock(AtomicI64);

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

/// The build's own policy, with no family table overridden: under the drill
/// RSEG writes {7, 8}.
async fn policy_over(store: Arc<dyn ObjectStoreBackend>) -> MaintainCompatPolicy {
    let clock = Arc::new(ManualClock(AtomicI64::new(T0)));
    let view = CompatView::new(store, compat_keys(), clock.clone());
    view.refresh().await.expect("refresh the view");
    MaintainCompatPolicy::new(view, clock)
}

async fn config_over(store: Arc<dyn ObjectStoreBackend>) -> CompactorConfig {
    CompactorConfig {
        compat_policy: Some(policy_over(store).await),
        ..CompactorConfig::default()
    }
}

async fn provision(store: &dyn ObjectStoreBackend) {
    validate_or_adopt(
        store,
        &tenant_hash(),
        Signal::Metrics,
        SHARDS,
        1,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
}

/// `obj` with its trailer version set to `version` and the footer crc
/// recomputed over it.
fn restamp(obj: &[u8], version: u16) -> Vec<u8> {
    let mut out = obj.to_vec();
    let t = out.len() - TRAILER_LEN as usize;
    let footer_len = u32::from_le_bytes(out[t..t + 4].try_into().expect("4 bytes"));
    let footer = &out[t - footer_len as usize..t];
    let mut crc = crc32c::crc32c(footer);
    crc = crc32c::crc32c_append(crc, &footer_len.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &version.to_le_bytes());
    crc = crc32c::crc32c_append(crc, &out[t + 10..]);
    out[t + 4..t + 8].copy_from_slice(&crc.to_le_bytes());
    out[t + 8..t + 10].copy_from_slice(&version.to_le_bytes());
    out
}

fn trailer_version(obj: &[u8]) -> u16 {
    open_from_full(obj, ReaderLimits::default())
        .expect("open")
        .version
}

/// Seed one L0 input whose object trailer and commit record both carry
/// `version`.
async fn seed_input_at(store: &dyn ObjectStoreBackend, spec: &InputSpec, version: u16) {
    let written = build_v5_l0(
        &spec.series,
        spec.hour,
        (spec.created_unix_ns, spec.epoch, spec.seq),
    );
    let bytes = restamp(&written.bytes, version);
    assert_eq!(trailer_version(&bytes), version, "the restamp opens");
    let content_hash = *blake3::hash(&bytes).as_bytes();
    let data_key = keys::data_key(
        &tenant_hash(),
        Signal::Metrics,
        SHARD,
        spec.writer_id,
        spec.epoch,
        spec.seq,
        &content_hash,
    )
    .expect("data key");
    let rec = record::build(record::NewCommitRecord {
        tenant_hash: tenant_hash(),
        signal: Signal::Metrics,
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
        segment_format_version: u32::from(version),
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
}

/// The common two-input metrics bucket, its inputs at `versions`.
async fn seed_two_inputs_at(store: &dyn ObjectStoreBackend, versions: [u16; 2]) {
    let specs = [
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
    ];
    for (spec, version) in specs.iter().zip(versions) {
        seed_input_at(store, spec, version).await;
    }
}

/// Every object in the store, by key and content hash.
async fn snapshot(store: &dyn ObjectStoreBackend) -> Vec<(String, [u8; 32])> {
    let mut out = Vec::new();
    for meta in list_all(store, "").await.expect("list") {
        let bytes = get_full(store, &meta.key).await;
        out.push((meta.key, *blake3::hash(&bytes).as_bytes()));
    }
    out
}

/// Every part of `rec` is recorded at `version` and its object's trailer
/// carries it.
async fn assert_parts_at(
    store: &dyn ObjectStoreBackend,
    rec: &ravel_proto::commit::v1::CompactionRecord,
    version: u32,
) {
    assert!(!rec.parts.is_empty());
    for part in &rec.parts {
        assert_eq!(part.segment_format_version, version);
        let key = keys::reconstruct_l1_part_key(rec, part).expect("part key");
        assert_eq!(
            u32::from(trailer_version(&get_full(store, &key).await)),
            version
        );
    }
}

/// A bucket activated to 8 after it was provisioned, inputs at `inputs`.
async fn activated_to_drill(store: &dyn ObjectStoreBackend, inputs: [u16; 2]) {
    seed_two_inputs_at(store, inputs).await;
    write_compat(store, vec![family(FAMILY_RSEG, v7())]).await;
    provision(store).await;
    activate(store, FAMILY_RSEG, drill()).await;
}

async fn compact(store: &dyn ObjectStoreBackend, config: &CompactorConfig) -> CompactionOutcome {
    compact_bucket(store, &FixedClock::new(sealed_now_ns()), config, &bucket())
        .await
        .expect("compact")
}

/// A floor at 8 admits a compaction whose run writes 8, and its parts are
/// written and recorded at 8.
///
/// Checking the floor against `current_part_version` (7) again, instead of
/// the run's version, refuses this run with `BelowFormatFloor`.
#[tokio::test]
async fn compaction_checks_the_floor_against_the_runs_version() {
    let store = Arc::new(MemoryStore::new());
    activated_to_drill(store.as_ref(), [VERSION_V7, VERSION_V7]).await;
    raise_format_floor(
        store.as_ref(),
        &tenant_hash(),
        Signal::Metrics,
        FormatFamily::Rseg.as_str(),
        drill(),
        FloorBasis {
            observed_entries: 0,
            observed_newest_created_unix_ns: 0,
            observed_shards: 1,
        },
        "test",
        T0,
    )
    .await
    .expect("raise the floor to 8");
    let metrics = Arc::new(FormatFloorMetrics::default());
    let config = CompactorConfig {
        format_floor: Some(FormatFloorPolicy::new(false, Arc::clone(&metrics))),
        ..config_over(store.clone()).await
    };

    let outcome = compact(store.as_ref(), &config).await;

    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );
    let rec = fetch_compaction_record(store.as_ref(), &bucket()).await;
    assert_parts_at(store.as_ref(), &rec, drill()).await;
}

/// A run writing 8 carries an input recorded at 8 forward with one at 7.
///
/// Comparing the inputs against `OUTPUT_FORMAT_VERSION` (7) again, instead of
/// the run's version, refuses the input at 8 with `NewerThanWritable`.
#[tokio::test]
async fn a_run_writing_8_compacts_an_input_recorded_at_8() {
    let store = Arc::new(MemoryStore::new());
    activated_to_drill(store.as_ref(), [VERSION_V7, VERSION_DRILL]).await;
    let config = config_over(store.clone()).await;

    let outcome = compact(store.as_ref(), &config).await;

    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );
    let rec = fetch_compaction_record(store.as_ref(), &bucket()).await;
    assert_eq!(rec.inputs.len(), 2, "both inputs are compacted");
    assert_parts_at(store.as_ref(), &rec, drill()).await;
}

/// A run writing 7 refuses the same input at 8, which this build reads, and
/// writes nothing: the check is the run's version, not the reader window's
/// newest.
#[tokio::test]
async fn a_run_writing_7_refuses_an_input_recorded_at_8() {
    let store = Arc::new(MemoryStore::new());
    seed_two_inputs_at(store.as_ref(), [VERSION_V7, VERSION_DRILL]).await;
    write_compat(store.as_ref(), vec![family(FAMILY_RSEG, v7())]).await;
    provision(store.as_ref()).await;
    let config = config_over(store.clone()).await;
    let before = snapshot(store.as_ref()).await;

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
            err,
            MaintainError::NewerThanWritable {
                version: 8,
                writable: 7,
                ..
            }
        ),
        "{err:?}"
    );
    assert_eq!(snapshot(store.as_ref()).await, before, "nothing is written");
}

/// Force 2 under a run writing 8 re-encodes a compacted bucket whose parts are
/// at 7, and its version 2 record's parts are at 8. Nothing is restamped: the
/// parts really are 7.
///
/// Counting parts below `current_part_version` (7) again, instead of below the
/// run's version, finds nothing to lift, and the bucket is not migrated.
#[tokio::test]
async fn force2_lifts_parts_to_the_runs_version() {
    let store = Arc::new(MemoryStore::new());
    seed_two_inputs_at(store.as_ref(), [VERSION_V7, VERSION_V7]).await;
    write_compat(
        store.as_ref(),
        vec![
            family(FAMILY_RSEG, v7()),
            family(FAMILY_COMPACTION_RECORD, 1),
        ],
    )
    .await;
    provision(store.as_ref()).await;
    let compacted = compact(store.as_ref(), &CompactorConfig::default()).await;
    assert!(
        matches!(compacted, CompactionOutcome::Compacted { .. }),
        "{compacted:?}"
    );
    let predecessor = fetch_compaction_record(store.as_ref(), &bucket()).await;
    assert_parts_at(store.as_ref(), &predecessor, v7()).await;
    activate(store.as_ref(), FAMILY_RSEG, drill()).await;
    activate(store.as_ref(), FAMILY_COMPACTION_RECORD, 2).await;
    let config = CompactorConfig {
        reencode_writer_enabled: true,
        ..config_over(store.clone()).await
    };

    let report = migrate_family(
        store.as_ref(),
        &FixedClock::new(sealed_now_ns()),
        &config,
        tenant_hash(),
        Signal::Metrics,
        FormatFamily::Rseg.as_str(),
        drill(),
        SHARDS,
        MigrateBudget::unlimited(),
        "format_drill test",
    )
    .await
    .expect("migrate");

    assert_eq!(report.reencode_blocked, Vec::new());
    assert_eq!(report.buckets_migrated, 1, "{report:?}");
    let listing = read::list_bucket(store.as_ref(), &bucket())
        .await
        .expect("list");
    let mut successors = Vec::new();
    for key in &listing.compaction_record_keys {
        let rec = record::decode_compaction(&get_full(store.as_ref(), key).await).expect("decode");
        if rec.format_version == record::COMPACTION_SUPERSEDING_FORMAT_VERSION {
            successors.push(rec);
        }
    }
    assert_eq!(successors.len(), 1, "one version 2 record");
    assert_parts_at(store.as_ref(), &successors[0], drill()).await;
}
