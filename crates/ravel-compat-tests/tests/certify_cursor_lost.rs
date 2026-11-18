//! Certify cursor lost mid-run (ADR-4233 decision 9, "Certify cursor lost
//! mid-run"): the cursor `sys/maintain/certify/<family>/cursor` is deleted
//! between two budgeted certify passes. The census must restart, with its
//! start `S` the restarting run's own `now`, and certify must never write a
//! certificate from the units an earlier, forgotten census covered.
//!
//! The store holds two census units, shards 0 and 1 of one metrics bucket, each
//! migrated to N+1 with its v7 input swept, so a full census certifies. A pass
//! with a budget of one unit censuses shard 0 and stops.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use ravel_catalog::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
use ravel_catalog::compat::{FAMILY_RSEG, read_compat};
use ravel_maintain::{CertifyReport, LateWrite, activate, late_write_pass};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_proto::sys::v1::CompatFamily;

use common::pipeline::{
    Flush, HOUR_NS, ManualClock, activate_request, certify_at, compactor_config, compat_keys,
    drill, init, migrate, provision, sealed_now_ns, sweep, sweep_times, v7, writable, write_flush,
};

const SHARD_COUNT: u32 = 2;
const CURSOR_KEY: &str = "sys/maintain/certify/rseg/cursor";

/// A store whose two units certify at N+1, and the first instant certify may
/// run at.
async fn certifiable() -> (Arc<MemoryStore>, i64) {
    let store = Arc::new(MemoryStore::with_page_size(2));
    let clock = Arc::new(ManualClock::new(sealed_now_ns()));
    write_flush(store.as_ref(), &Flush::v7(0, 0xA0, 1, 1.5)).await;
    write_flush(store.as_ref(), &Flush::v7(1, 0xA1, 2, 2.5)).await;
    provision(store.as_ref(), SHARD_COUNT).await;
    init(store.as_ref(), &clock).await;
    let report = activate(
        store.as_ref(),
        Some(&compat_keys()),
        clock.as_ref(),
        &writable(),
        &activate_request(None),
    )
    .await
    .expect("activate");
    assert!(report.walk.complete, "{:?}", report.walk);
    let config = compactor_config(store.clone(), clock.clone()).await;
    let migrated = migrate(store.as_ref(), &config, SHARD_COUNT)
        .await
        .expect("migrate");
    assert_eq!(migrated.buckets_migrated, 2, "{migrated:?}");
    let (mark, delete) = sweep_times(&config);
    for now in [mark, delete] {
        for shard in 0..SHARD_COUNT {
            sweep(store.as_ref(), &config, now, shard)
                .await
                .expect("sweep");
        }
    }
    assert_eq!(
        common::data_keys(store.as_ref())
            .await
            .iter()
            .filter(|k| k.contains("/l0/"))
            .count(),
        0,
        "every v7 input is swept"
    );
    (store, delete)
}

/// The census start `S` the stored cursor carries (bytes 13..21, after the
/// tag, the target and the activation epoch).
async fn cursor_start(store: &dyn ObjectStoreBackend) -> i64 {
    let bytes = store
        .get(CURSOR_KEY, GetRange::Full)
        .await
        .expect("the certify cursor is stored")
        .data;
    assert_eq!(bytes[0], 2, "a certify cursor payload");
    i64::from_le_bytes(bytes[13..21].try_into().expect("eight bytes"))
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

async fn certify(
    store: &dyn ObjectStoreBackend,
    now_ns: i64,
    budget: Option<usize>,
) -> CertifyReport {
    certify_at(store, now_ns, budget).await.expect("certify")
}

/// Census shard 0 only, at `now`, and check the cursor it leaves.
async fn census_first_unit(store: &dyn ObjectStoreBackend, now: i64) {
    assert_eq!(
        certify(store, now, Some(1)).await,
        CertifyReport::Incomplete { units_examined: 1 }
    );
    assert_eq!(
        cursor_start(store).await,
        now,
        "S is the census's own start"
    );
}

/// A v7 write that lands in shard 0 after its census, with a store clock past
/// that census's start so a late-write pass can find it by `LastModified`.
async fn late_v7_in_shard_0(store: &MemoryStore, after_ns: i64) -> common::pipeline::Flushed {
    store.set_clock_ms(u64::try_from((after_ns + HOUR_NS) / 1_000_000).expect("ms"));
    write_flush(store, &Flush::v7(0, 0xB0, 3, 9.0)).await
}

/// With the cursor lost, the next run re-censuses shard 0 and finds the v7
/// write that landed there after the first census: no certificate. With the
/// cursor kept (the control), the run resumes at shard 1 and certifies with
/// the first census's `S` as its watermark, and the late-write pass is what
/// finds the write.
#[tokio::test]
async fn a_lost_cursor_restarts_the_census_and_rescans_the_censused_unit() {
    let (store, s1) = certifiable().await;
    census_first_unit(store.as_ref(), s1).await;
    let late = late_v7_in_shard_0(store.as_ref(), s1).await;
    store.delete(CURSOR_KEY).await.expect("lose the cursor");

    let s2 = s1 + HOUR_NS;
    match certify(store.as_ref(), s2, None).await {
        CertifyReport::Contradicted {
            member_key,
            found_version,
        } => {
            assert_eq!(member_key, late.data_key, "the late v7 member in shard 0");
            assert_eq!(found_version, v7());
        }
        other => panic!("the restarted census must rescan shard 0: {other:?}"),
    }
    assert!(rseg_entry(store.as_ref()).await.certificate.is_none());
    assert_eq!(
        cursor_start(store.as_ref()).await,
        s2,
        "the restarted census starts at its own now"
    );

    // Control: the same history with the cursor kept.
    let (store, s1) = certifiable().await;
    census_first_unit(store.as_ref(), s1).await;
    let late = late_v7_in_shard_0(store.as_ref(), s1).await;
    let s2 = s1 + HOUR_NS;
    let report = certify(store.as_ref(), s2, None).await;
    assert!(
        matches!(report, CertifyReport::Certified { version, member_count: 2, .. } if version == drill()),
        "the resumed census does not revisit shard 0: {report:?}"
    );
    let entry = rseg_entry(store.as_ref()).await;
    assert_eq!(entry.late_writes_verified_through_unix_ns, s1);
    let verify = late_write_pass(
        store.as_ref(),
        &compat_keys(),
        s2 + HOUR_NS,
        FAMILY_RSEG,
        DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    )
    .await
    .expect("late-write pass");
    assert!(
        verify.late_writes.contains(&LateWrite {
            record_key: late.record_key,
            version: v7(),
        }),
        "{verify:?}"
    );
    assert!(rseg_entry(store.as_ref()).await.contradicted);
}

/// With nothing to find, a lost cursor still costs the census its progress: the
/// next budgeted run restarts at shard 0 with `S` its own now, and reports
/// `Incomplete` rather than certifying on shard 1 alone. The certificate the
/// finished census writes carries the restarted census's `S` as its
/// watermark and counts both units.
#[tokio::test]
async fn a_lost_cursor_never_certifies_a_partial_census() {
    let (store, s1) = certifiable().await;
    census_first_unit(store.as_ref(), s1).await;
    store.delete(CURSOR_KEY).await.expect("lose the cursor");

    let s2 = s1 + HOUR_NS;
    census_first_unit(store.as_ref(), s2).await;
    assert!(rseg_entry(store.as_ref()).await.certificate.is_none());

    let s3 = s2 + HOUR_NS;
    let report = certify(store.as_ref(), s3, None).await;
    assert!(
        matches!(report, CertifyReport::Certified { version, member_count: 2, .. } if version == drill()),
        "{report:?}"
    );
    let entry = rseg_entry(store.as_ref()).await;
    let certificate = entry.certificate.as_ref().expect("certificate");
    assert_eq!(certificate.member_count, 2, "both units counted");
    assert_eq!(certificate.certified_unix_ns, s3);
    assert_eq!(
        entry.late_writes_verified_through_unix_ns, s2,
        "the watermark is the restarted census's S, not the lost one's"
    );
    assert!(
        store.head(CURSOR_KEY).await.is_err(),
        "the finished census drops its cursor"
    );
}
