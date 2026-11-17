//! Main's `compat certify` census and late-write pass against the teardown
//! flush a v0.33.0 server writes into a fenced metrics bucket (issue #4243,
//! ADR-4233 decision 7).
//!
//! The released-binary drill sees v0.33.0 flush one RSEG 7 `l0` object and
//! its commit record at graceful stop, after the fence. Certify's time gate
//! opens hours after the fence, past any drill run, so this test replays
//! that pair on a `MemoryStore` with an injected clock instead: `sys/compat`
//! activates RSEG at the drill version (8) with the fence walk complete, the
//! bucket is provisioned and fenced, and the pair is written by the current
//! RSEG 7 writer under the keys v0.33.0's flush uses.

#![allow(clippy::expect_used)]

use ravel_catalog::compat::{
    COMPAT_FORMAT_VERSION, CompatKeys, FAMILY_RSEG, create_compat, read_compat,
};
use ravel_catalog::{
    AbsentPolicy, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS,
    raise_compat_fence, validate_or_adopt,
};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_maintain::{
    CertifyRefusal, CertifyReport, CertifyTimings, LateWrite, certify, late_write_pass,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::sys::v1::{CompatFamily, CompatRecord};
use ravel_segment::{
    IngestBounds, SegmentIdentity, SegmentVersion, SegmentWriter, SeriesInputV3, SeriesValues,
    VERSION_DRILL,
};
use ravel_types::{
    Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantHash, TenantId,
};
use uuid::Uuid;

const SECOND_NS: i64 = 1_000_000_000;
const HOUR_NS: i64 = 3_600 * SECOND_NS;
/// The second run's fence, 2026-11-16T23:00:20Z: RSEG activated at the drill
/// version and every bucket fenced then.
const FENCED_NS: i64 = 1_794_870_020 * SECOND_NS;
/// The RSEG version v0.33.0 writes.
const RELEASE_RSEG: u32 = 7;
/// v0.33.0's S3 request timeout is 20 s; the drill's margin is three of them.
const T_REQ_NS: i64 = 60 * SECOND_NS;

fn compat_keys() -> CompatKeys {
    CompatKeys::from_file_keys(&[[9; 32]]).expect("one key")
}

fn timings() -> CertifyTimings {
    CertifyTimings {
        t_req_ns: T_REQ_NS,
        max_flush_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
        clock_skew_allowance_ns: DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    }
}

fn tenant() -> TenantId {
    TenantId::new("acme")
}

fn ms(ns: i64) -> u64 {
    u64::try_from(ns / 1_000_000).expect("non-negative time")
}

/// A two-key listing page, so every census and late-write listing pages.
fn new_store() -> MemoryStore {
    MemoryStore::with_page_size(2)
}

/// `sys/compat` with RSEG active at the drill version, activated and with
/// its fence walk complete at the fence, and metrics provisioned at one
/// shard and fenced to the activation epoch.
async fn drill_bucket(store: &dyn ObjectStoreBackend) -> TenantHash {
    create_compat(
        store,
        &compat_keys(),
        CompatRecord {
            format_version: COMPAT_FORMAT_VERSION,
            baseline_release_major: 0,
            baseline_release_minor: 33,
            families: vec![CompatFamily {
                family: FAMILY_RSEG.to_string(),
                active_write_version: u32::from(VERSION_DRILL),
                activated_epoch: 1,
                activated_unix_ns: FENCED_NS,
                activated_by: "certify_flags_teardown_pair".to_string(),
                certificate: None,
                fence_complete_unix_ns: FENCED_NS,
                late_writes_verified_through_unix_ns: 0,
                contradicted: false,
            }],
            compat_epoch: 1,
            max_publish_lifetime_ns: DEFAULT_MAX_FLUSH_LIFETIME_NS,
            adopted: Vec::new(),
            mac: Vec::new(),
        },
    )
    .await
    .expect("create sys/compat");
    let hash = tenant().hash();
    validate_or_adopt(
        store,
        &hash,
        Signal::Metrics,
        1,
        FENCED_NS - HOUR_NS,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision metrics");
    raise_compat_fence(store, &hash, Signal::Metrics, 1, FENCED_NS)
        .await
        .expect("fence metrics");
    hash
}

/// The keys of one teardown flush.
struct Pair {
    data_key: String,
    record_key: String,
}

/// One v0.33.0 metrics flush at `flushed_ns`: the RSEG 7 data object, then
/// (when `with_record`) its commit record, in the order v0.33.0 PUTs them
/// (ravel-ingest shard.rs), both with store `LastModified` at `flushed_ns`.
/// Returns the keys whether or not the record was written.
async fn write_teardown_flush(
    store: &MemoryStore,
    hash: TenantHash,
    flushed_ns: i64,
    with_record: bool,
) -> Pair {
    let writer_id = Uuid::from_u128(0x0033);
    let (epoch, seq, shard) = (1, 7, 0);
    let event_ns = FENCED_NS - 10 * SECOND_NS;
    let labels = LabelSet::new(vec![Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: "drill_rw_gauge".to_string(),
    }])
    .expect("labels");
    let series = vec![SeriesInputV3 {
        series_id: SeriesId::compute(&tenant(), "drill_rw_gauge", &labels).expect("series id"),
        labels,
        values: SeriesValues::Scalar(vec![Sample {
            ts_ns: event_ns,
            value: 1.0,
        }]),
    }];
    let written = SegmentWriter::write_histograms_with_exemplars_at(
        SegmentVersion::V7,
        series,
        SegmentIdentity {
            tenant_hash: hash.0,
            shard,
            writer_id: writer_id.to_string(),
            writer_epoch: epoch,
            writer_seq: seq,
        },
        IngestBounds {
            min_ingest_ts_ns: event_ns,
            max_ingest_ts_ns: event_ns,
        },
        Vec::new(),
    )
    .expect("write RSEG 7");
    let data_key = keys::data_key(
        &hash,
        Signal::Metrics,
        shard,
        writer_id,
        epoch,
        seq,
        &written.summary.blake3,
    )
    .expect("data key");
    let commit = record::build(NewCommitRecord {
        tenant_hash: hash,
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: epoch,
        writer_seq: seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: event_ns,
        max_ingest_ts_ns: event_ns,
        segment_format_version: RELEASE_RSEG,
        created_unix_ns: event_ns,
        ingest_hour_bucket: u32::try_from(event_ns.div_euclid(HOUR_NS)).expect("hour"),
    })
    .expect("build commit record");
    let record_key = keys::commit_key_for_record(&commit).expect("commit key");

    let h = hash.to_hex();
    assert!(
        data_key.starts_with(&format!("t/{h}/m/l0/0000/")) && data_key.ends_with(".rseg"),
        "{data_key}"
    );
    assert!(
        record_key.starts_with(&format!("t/{h}/m/c/0000/20261116T23/"))
            && record_key.ends_with(".cmt"),
        "{record_key}"
    );
    assert_eq!(
        &written.bytes[written.bytes.len() - 4..],
        b"RSG1",
        "an RSEG object"
    );
    assert_eq!(
        u32::from(u16::from_le_bytes([
            written.bytes[written.bytes.len() - 8],
            written.bytes[written.bytes.len() - 7],
        ])),
        RELEASE_RSEG,
        "the trailer carries the release's version"
    );

    store.set_clock_ms(ms(flushed_ns));
    store
        .put(&data_key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put data object");
    if with_record {
        store
            .put(&record_key, record::encode(&commit), PutOptions::default())
            .await
            .expect("put commit record");
    }
    Pair {
        data_key,
        record_key,
    }
}

async fn certify_drill(store: &dyn ObjectStoreBackend, now_ns: i64) -> CertifyReport {
    certify(
        store,
        &compat_keys(),
        now_ns,
        FAMILY_RSEG,
        u32::from(VERSION_DRILL),
        timings(),
        None,
    )
    .await
    .expect("certify")
}

/// The first instant certify's time gate admits, read from its refusal at
/// two hours after the fence (inside every drill run).
async fn past_time_gate(store: &dyn ObjectStoreBackend) -> i64 {
    match certify_drill(store, FENCED_NS + 2 * HOUR_NS).await {
        CertifyReport::Refused(CertifyRefusal::TimeGate { not_before_ns, .. }) => {
            assert!(
                not_before_ns > FENCED_NS + 4 * HOUR_NS,
                "the gate opens after any drill run: {not_before_ns}"
            );
            not_before_ns + SECOND_NS
        }
        other => panic!("certify before the time gate: {other:?}"),
    }
}

async fn rseg_entry(store: &dyn ObjectStoreBackend) -> CompatFamily {
    read_compat(store, &compat_keys())
        .await
        .expect("read sys/compat")
        .expect("sys/compat present")
        .0
        .families
        .into_iter()
        .find(|f| f.family == FAMILY_RSEG)
        .expect("rseg entry")
}

/// (a) With the teardown pair in the bucket, certify at the drill version
/// does not certify: the census reports the pair's RSEG 7 as Contradicted
/// and writes no certificate.
#[tokio::test]
async fn certify_does_not_certify_over_a_teardown_pair() {
    let store = new_store();
    let hash = drill_bucket(&store).await;
    let pair = write_teardown_flush(&store, hash, FENCED_NS + 3 * HOUR_NS, true).await;
    let now = past_time_gate(&store).await;

    let report = certify_drill(&store, now).await;
    assert!(
        report
            == CertifyReport::Contradicted {
                member_key: pair.record_key.clone(),
                found_version: RELEASE_RSEG,
            }
            || report
                == CertifyReport::Contradicted {
                    member_key: pair.data_key.clone(),
                    found_version: RELEASE_RSEG,
                },
        "{report:?}"
    );
    let entry = rseg_entry(&store).await;
    assert!(entry.certificate.is_none(), "{entry:?}");
}

/// (a) The same when only the data object landed (the flush stopped before
/// its record PUT): the census reads the unrecorded `l0` object's trailer.
#[tokio::test]
async fn certify_does_not_certify_over_an_unrecorded_teardown_object() {
    let store = new_store();
    let hash = drill_bucket(&store).await;
    let pair = write_teardown_flush(&store, hash, FENCED_NS + 3 * HOUR_NS, false).await;
    let now = past_time_gate(&store).await;

    assert_eq!(
        certify_drill(&store, now).await,
        CertifyReport::Contradicted {
            member_key: pair.data_key,
            found_version: RELEASE_RSEG,
        }
    );
    assert!(rseg_entry(&store).await.certificate.is_none());
}

/// Control for (a): the same bucket without the pair certifies at the same
/// instant, so the pair is what blocks it.
#[tokio::test]
async fn certify_certifies_the_bucket_without_the_pair() {
    let store = new_store();
    drill_bucket(&store).await;
    let now = past_time_gate(&store).await;

    let report = certify_drill(&store, now).await;
    assert!(
        matches!(
            report,
            CertifyReport::Certified {
                version,
                member_count: 0,
                ..
            } if version == u32::from(VERSION_DRILL)
        ),
        "{report:?}"
    );
}

/// (b) A teardown pair written after the certificate is a late write: the
/// late-write pass names its commit record at RSEG 7 and sets `contradicted`.
#[tokio::test]
async fn a_teardown_pair_after_the_certificate_sets_contradicted() {
    let store = new_store();
    let hash = drill_bucket(&store).await;
    let now = past_time_gate(&store).await;
    let certified = certify_drill(&store, now).await;
    assert!(
        matches!(certified, CertifyReport::Certified { .. }),
        "{certified:?}"
    );
    assert!(!rseg_entry(&store).await.contradicted);

    let pair = write_teardown_flush(&store, hash, now + HOUR_NS, true).await;
    let report = late_write_pass(
        &store,
        &compat_keys(),
        now + 2 * HOUR_NS,
        FAMILY_RSEG,
        DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    )
    .await
    .expect("late-write pass");
    assert!(report.certified, "{report:?}");
    assert_eq!(
        report.late_writes,
        vec![LateWrite {
            record_key: pair.record_key,
            version: RELEASE_RSEG,
        }],
        "{report:?}"
    );
    assert!(rseg_entry(&store).await.contradicted);
}
