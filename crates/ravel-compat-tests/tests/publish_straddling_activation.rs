//! A publish straddling activation, and a permit granted after `activated + H`
//! (ADR-4233 decision 9). Ingest fixes a flush's version when its permit is
//! granted (`IngestCompatPolicy::write_version`). A view read just before
//! activation answers N inside `H`, so a flush opened before activation and
//! granted inside `H` encodes at N and may land up to `P` later; the same view
//! past `H` refuses as stale, and once refreshed answers N+1. `certify` refuses
//! to start before `activated + H + P + T_req` and, after it, the census
//! counts the straddling N object.
//!
//! The tenant is new: its bucket is provisioned after activation, born fenced,
//! so no bucket's `G_pre` term outlasts the activation term and the time gate
//! is the activation term plus the clock-skew allowance.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::num::NonZeroU64;
use std::sync::Arc;

use ravel_catalog::compat::{
    COMPAT_VIEW_HORIZON_NS, CompatRefusal, CompatView, FAMILY_RSEG, read_compat,
};
use ravel_catalog::{
    AbsentPolicy, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS,
    validate_or_adopt_fenced,
};
use ravel_ingest::{FormatFamily, IngestCompatPolicy};
use ravel_maintain::{CertifyRefusal, CertifyReport, activate};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend};
use ravel_segment::{ReaderLimits, SegmentVersion, open_from_full};
use ravel_types::Signal;

use common::pipeline::{
    Flush, ManualClock, SECOND_NS, T_REQ_NS, activate_request, certify_at, compat_keys, drill,
    hash, init, sealed_now_ns, v7, writable, write_flush,
};

const H_NS: i64 = COMPAT_VIEW_HORIZON_NS;
const P_NS: i64 = DEFAULT_MAX_FLUSH_LIFETIME_NS;

fn segment_version(granted: u16) -> SegmentVersion {
    match u32::from(granted) {
        v if v == v7() => SegmentVersion::V7,
        v if v == drill() => SegmentVersion::Drill,
        other => panic!("granted a version outside the window: {other}"),
    }
}

/// The trailer version of the object at `key`.
async fn trailer_version(store: &dyn ObjectStoreBackend, key: &str) -> u32 {
    let bytes = store
        .get(key, GetRange::Full)
        .await
        .expect("get the published object")
        .data;
    u32::from(
        open_from_full(&bytes, ReaderLimits::default())
            .expect("open the published object")
            .version,
    )
}

#[tokio::test]
async fn a_straddling_publish_lands_at_n_and_a_grant_past_h_encodes_n_plus_one() {
    let activation = sealed_now_ns();
    let store = Arc::new(MemoryStore::with_page_size(2));
    let clock = Arc::new(ManualClock::new(activation - 60 * SECOND_NS));
    init(store.as_ref(), &clock).await;
    let view = CompatView::new(store.clone(), compat_keys(), clock.clone());
    view.refresh().await.expect("the ingest view's last read");
    let policy = IngestCompatPolicy::new(view.clone(), clock.clone());
    let mut writes = policy.writable(FormatFamily::Rseg).to_vec();
    writes.sort_unstable();
    assert_eq!(writes, [7, 8], "this build writes N and N+1");

    clock.set(activation);
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
    let entry = read_compat(store.as_ref(), &compat_keys())
        .await
        .expect("read sys/compat")
        .expect("present")
        .0
        .families
        .into_iter()
        .find(|f| f.family == FAMILY_RSEG)
        .expect("rseg entry");
    assert_eq!(entry.activated_unix_ns, activation);
    validate_or_adopt_fenced(
        store.as_ref(),
        &hash(),
        Signal::Metrics,
        1,
        activation,
        AbsentPolicy::CreateFromConfig,
        Some(NonZeroU64::new(entry.activated_epoch).expect("an activation epoch")),
    )
    .await
    .expect("the new tenant's bucket, born fenced");

    // Opened just before activation, granted inside H: the view read before
    // activation still names N.
    clock.set(activation + H_NS - 61 * SECOND_NS);
    let granted = policy
        .write_version(FormatFamily::Rseg, None)
        .expect("a grant inside H");
    assert_eq!(u32::from(granted), v7(), "granted inside H, encodes at N");
    let straddling = Flush {
        version: segment_version(granted),
        ts_ns: activation - SECOND_NS,
        ..Flush::v7(0, 0xA0, 1, 1.5)
    };
    store.set_clock_ms(u64::try_from((activation + P_NS - SECOND_NS) / 1_000_000).expect("ms"));
    let straddling = write_flush(store.as_ref(), &straddling).await;
    assert_eq!(
        trailer_version(store.as_ref(), &straddling.data_key).await,
        v7(),
        "the straddling publish lands at N"
    );

    // Opened before activation, granted at activated + H: the same view is
    // stale and refuses; refreshed, it names N+1.
    clock.set(activation + H_NS);
    let refusal = policy
        .write_version(FormatFamily::Rseg, None)
        .expect_err("a view read before activation is stale at activated + H");
    assert!(
        matches!(refusal, CompatRefusal::ViewStale { .. }),
        "{refusal:?}"
    );
    view.refresh().await.expect("refresh");
    let granted = policy
        .write_version(FormatFamily::Rseg, None)
        .expect("a grant after activated + H");
    assert_eq!(
        u32::from(granted),
        drill(),
        "granted after activated + H, encodes at N+1"
    );
    let late_grant = Flush {
        version: segment_version(granted),
        ts_ns: activation - SECOND_NS / 2,
        ..Flush::v7(0, 0xA1, 2, 2.5)
    };
    let late_grant = write_flush(store.as_ref(), &late_grant).await;
    assert_eq!(
        trailer_version(store.as_ref(), &late_grant.data_key).await,
        drill()
    );

    let activation_term = activation + H_NS + P_NS + T_REQ_NS;
    let not_before = activation_term + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS;
    for now in [activation_term, not_before] {
        assert_eq!(
            certify_at(store.as_ref(), now, None)
                .await
                .expect("certify"),
            CertifyReport::Refused(CertifyRefusal::TimeGate {
                now_ns: now,
                not_before_ns: not_before,
            }),
            "certify does not start before activated + H + P + T_req"
        );
    }
    assert_eq!(
        certify_at(store.as_ref(), not_before + 1, None)
            .await
            .expect("certify"),
        CertifyReport::Contradicted {
            member_key: straddling.data_key,
            found_version: v7(),
        },
        "past the gate the census counts the straddling N object"
    );
}
