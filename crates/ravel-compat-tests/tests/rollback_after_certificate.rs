//! Rollback "F certified, retired-reader release B+2 installed, roll back to
//! B+1" (ADR-4233 decision 5: safe, B+1 reads N+1). This crate builds with the
//! drill window {N, N+1}, so it is B+1: its reader admits both versions and
//! its writers carry the table {N, N+1} with baseline N. B+2 is the same store
//! code under the writer table {N+1} with baseline N+1; its reader retires N,
//! which the drill build cannot compile out, so B+2's reads are not exercised
//! here and its startup gate is `retired_reader_refuses_without_current_certificate`.
//!
//! After migrate, the sweeps and certify, the family carries a certificate at
//! N+1 and a late-write pass has moved the watermark past it, the state B+2's
//! gate starts on. B+2 ingests and compacts at N+1. Rolled back, B+1 writes
//! the active version N+1, never its baseline N; reads every sample B and B+2
//! published, each from an object at N+1 inside its window; and its writes
//! leave the certificate standing: a late-write pass over them finds no late
//! write and does not contradict it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
use ravel_catalog::compat::{CompatView, FAMILY_RSEG, read_compat};
use ravel_maintain::{
    Bucket, CertifyReport, CompactionOutcome, CompactorConfig, FamilyVersions, FixedClock,
    MaintainCompatPolicy, activate, compact_bucket, late_write_pass,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::sys::v1::CompatFamily;
use ravel_segment::{ReaderLimits, SegmentVersion, open_from_full};
use ravel_types::Signal;

use common::pipeline::{
    Flush, HOUR, HOUR_NS, ManualClock, SECOND_NS, activate_request, certify_at, compactor_config,
    compat_keys, drill, hash, init, migrate, provision, query_hours, sealed_now_ns, sweep,
    sweep_times, v7, writable, write_flush,
};

const B1_WRITES: [u16; 2] = [7, 8];
const B2_WRITES: [u16; 1] = [8];

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

/// A release's view of `sys/compat`, read at `now`, and its compactor
/// configuration under the RSEG writer table `writes` with `baseline`.
async fn release(
    store: Arc<MemoryStore>,
    now: i64,
    writes: &[u16],
    baseline: u16,
) -> (CompatView, CompactorConfig) {
    let clock = Arc::new(ManualClock::new(now));
    let view = CompatView::new(store, compat_keys(), clock.clone());
    view.refresh().await.expect("refresh the view");
    let policy = MaintainCompatPolicy::new(view.clone(), clock).with_family(
        FAMILY_RSEG,
        FamilyVersions {
            writable: writes.to_vec(),
            baseline,
        },
    );
    let config = CompactorConfig {
        compat_policy: Some(policy),
        ..CompactorConfig::default()
    };
    (view, config)
}

/// Publish one flush at the version `view` grants a release writing `writes`.
async fn publish(
    store: &MemoryStore,
    view: &CompatView,
    now: i64,
    writes: &[u16],
    baseline: u16,
    flush: Flush,
) {
    let granted = view
        .write_policy(now, FAMILY_RSEG, writes, baseline, None)
        .expect("the release writes the family");
    assert_eq!(
        u32::from(granted),
        drill(),
        "a release writing {writes:?} with baseline {baseline} writes the active N+1"
    );
    write_flush(
        store,
        &Flush {
            version: SegmentVersion::Drill,
            ..flush
        },
    )
    .await;
}

/// A flush into `hour`, `seq` seconds after its start.
fn flush_in(hour: u32, writer: u128, seq: u64, value: f64) -> Flush {
    Flush {
        ts_ns: i64::from(hour) * HOUR_NS + i64::try_from(seq).expect("seq") * SECOND_NS,
        ..Flush::v7(0, writer, seq, value)
    }
}

async fn compact(store: &MemoryStore, config: &CompactorConfig, hour: u32, now: i64) {
    let outcome = compact_bucket(
        store,
        &FixedClock::new(now),
        config,
        &Bucket::new(hash(), Signal::Metrics, 0, hour),
    )
    .await
    .expect("compact");
    assert!(
        matches!(outcome, CompactionOutcome::Compacted { .. }),
        "{outcome:?}"
    );
}

/// Every object a query over the hour resolves, by trailer version, and every
/// sample it reads.
async fn read_hour(store: &Arc<MemoryStore>, now: i64) -> (Vec<u32>, Vec<(i64, u64)>) {
    let mut versions = Vec::new();
    let mut samples = Vec::new();
    for (key, series) in query_hours(store.clone(), 1, HOUR, 3, now).await {
        let bytes = store
            .get(&key, GetRange::Full)
            .await
            .expect("get a resolved object")
            .data;
        let version = open_from_full(&bytes, ReaderLimits::default())
            .expect("B+1's reader opens the object")
            .version;
        assert!(
            SegmentVersion::WINDOW.iter().any(|v| v.number() == version),
            "{key} at {version} is inside B+1's window"
        );
        versions.push(u32::from(version));
        samples.extend(series.into_iter().flat_map(|s| s.samples));
    }
    samples.sort_unstable();
    (versions, samples)
}

fn sample(flush: &Flush) -> (i64, u64) {
    (flush.ts_ns, flush.value.to_bits())
}

#[tokio::test]
async fn b_plus_1_after_a_rollback_from_b_plus_2_reads_and_writes_n_plus_1() {
    let store = Arc::new(MemoryStore::with_page_size(2));
    let clock = Arc::new(ManualClock::new(sealed_now_ns()));
    let a = Flush::v7(0, 0xA0, 1, 1.5);
    let b = Flush::v7(0, 0xA1, 2, 2.5);
    write_flush(store.as_ref(), &a).await;
    write_flush(store.as_ref(), &b).await;
    provision(store.as_ref(), 1).await;
    init(store.as_ref(), &clock).await;
    let activated = activate(
        store.as_ref(),
        Some(&compat_keys()),
        clock.as_ref(),
        &writable(),
        &activate_request(None),
    )
    .await
    .expect("activate");
    assert!(activated.walk.complete, "{:?}", activated.walk);
    let config = compactor_config(store.clone(), clock.clone()).await;
    migrate(store.as_ref(), &config, 1).await.expect("migrate");
    let (mark, delete) = sweep_times(&config);
    for now in [mark, delete] {
        sweep(store.as_ref(), &config, now, 0).await.expect("sweep");
    }
    let certified = certify_at(store.as_ref(), delete, None)
        .await
        .expect("certify");
    assert!(
        matches!(certified, CertifyReport::Certified { version, .. } if version == drill()),
        "{certified:?}"
    );

    // The state B+2's startup gate requires: a certificate at N+1, not
    // contradicted, and a watermark past the certification time.
    let gate_pass = delete + HOUR_NS;
    store.set_clock_ms(u64::try_from(gate_pass / 1_000_000).expect("ms"));
    let pass = late_write_pass(
        store.as_ref(),
        &compat_keys(),
        gate_pass,
        FAMILY_RSEG,
        DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    )
    .await
    .expect("late-write pass");
    assert!(pass.late_writes.is_empty(), "{pass:?}");
    let entry = rseg_entry(store.as_ref()).await;
    let certificate = entry.certificate.clone().expect("a certificate");
    assert_eq!(certificate.version, drill());
    assert!(!entry.contradicted);
    assert!(entry.late_writes_verified_through_unix_ns > certificate.certified_unix_ns);

    // B+2 installed: it ingests and compacts at N+1.
    let b2_at = gate_pass + HOUR_NS;
    store.set_clock_ms(u64::try_from(b2_at / 1_000_000).expect("ms"));
    let (b2_view, b2_config) = release(store.clone(), b2_at, &B2_WRITES, 8).await;
    let c = flush_in(HOUR + 1, 0xB2, 3, 3.5);
    let d = flush_in(HOUR + 1, 0xB3, 4, 4.5);
    for flush in [&c, &d] {
        publish(
            store.as_ref(),
            &b2_view,
            b2_at,
            &B2_WRITES,
            8,
            flush.clone(),
        )
        .await;
    }
    compact(store.as_ref(), &b2_config, HOUR + 1, b2_at).await;

    // Rolled back to B+1: its writer table is {N, N+1} with baseline N, and it
    // writes N+1.
    let b1_at = b2_at + HOUR_NS;
    store.set_clock_ms(u64::try_from(b1_at / 1_000_000).expect("ms"));
    let (b1_view, b1_config) = release(store.clone(), b1_at, &B1_WRITES, 7).await;
    let e = flush_in(HOUR + 2, 0xC1, 5, 5.5);
    let f = flush_in(HOUR + 2, 0xC2, 6, 6.5);
    for flush in [&e, &f] {
        publish(
            store.as_ref(),
            &b1_view,
            b1_at,
            &B1_WRITES,
            7,
            flush.clone(),
        )
        .await;
    }
    compact(store.as_ref(), &b1_config, HOUR + 2, b1_at).await;

    let (versions, samples) = read_hour(&store, b1_at).await;
    assert!(!versions.is_empty());
    assert!(
        versions.iter().all(|v| *v == drill()),
        "every object B+1 reads is at N+1: {versions:?}"
    );
    let mut expected: Vec<(i64, u64)> = [&a, &b, &c, &d, &e, &f].map(sample).to_vec();
    expected.sort_unstable();
    assert_eq!(
        samples, expected,
        "B+1 reads every sample B, B+1 and B+2 published, exactly once"
    );
    for key in common::data_keys(store.as_ref()).await {
        let bytes = store.get(&key, GetRange::Full).await.expect("get").data;
        let version = open_from_full(&bytes, ReaderLimits::default())
            .expect("open")
            .version;
        assert_ne!(
            u32::from(version),
            v7(),
            "{key}: no N object after the rollback"
        );
    }

    let pass = late_write_pass(
        store.as_ref(),
        &compat_keys(),
        b1_at + HOUR_NS,
        FAMILY_RSEG,
        DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    )
    .await
    .expect("late-write pass");
    assert_eq!(
        pass.examined, 6,
        "the pass examines the four flush records and two compaction records B+2 and B+1 \
         wrote: {pass:?}"
    );
    assert!(
        pass.late_writes.is_empty(),
        "N+1 writes are not late writes: {pass:?}"
    );
    let entry = rseg_entry(store.as_ref()).await;
    assert!(!entry.contradicted, "the certificate stands");
    assert_eq!(entry.certificate, Some(certificate));
}
