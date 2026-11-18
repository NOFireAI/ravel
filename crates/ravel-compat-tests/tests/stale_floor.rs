//! Stale floor (ADR-4233 decision 9, "stale floor does not certify"): a format
//! floor is a pointer to evidence, never the evidence (ADR-1746 decision 2).
//! `migrate` raises the RSEG floor to N+1 on an audit that counts live records
//! only, so the v7 inputs its compaction superseded are not counted, though
//! they stay in the store until the sweep deletes them. A record at N+1 created
//! after the floor's basis then leaves the floor `Stale`. `certify` censuses
//! every member and refuses while a superseded v7 input remains; once the
//! sweep has deleted them it certifies, and the floor history is left as
//! `migrate` wrote it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::sync::Arc;

use ravel_catalog::compat::{FAMILY_RSEG, read_compat};
use ravel_catalog::{
    FloorEvidence, FloorObservation, FormatFloor, classify_floor, read_floors_from_store,
};
use ravel_maintain::{CertifyReport, Verification, activate};
use ravel_object_store::memory::MemoryStore;
use ravel_segment::SegmentVersion;
use ravel_types::Signal;

use common::pipeline::{
    Flush, ManualClock, SECOND_NS, activate_request, certify_at, compactor_config, compat_keys,
    drill, hash, init, migrate, provision, sealed_now_ns, sweep, sweep_times, v7, writable,
    write_flush,
};

async fn floors(store: &MemoryStore) -> Vec<FormatFloor> {
    read_floors_from_store(store, &hash(), Signal::Metrics)
        .await
        .expect("read floors")
        .expect("a provisioning record")
}

async fn has_certificate(store: &MemoryStore) -> bool {
    read_compat(store, &compat_keys())
        .await
        .expect("read sys/compat")
        .expect("present")
        .0
        .families
        .into_iter()
        .find(|f| f.family == FAMILY_RSEG)
        .expect("rseg entry")
        .certificate
        .is_some()
}

#[tokio::test]
async fn a_stale_floor_does_not_certify_while_superseded_n_inputs_remain() {
    let store = Arc::new(MemoryStore::with_page_size(2));
    let clock = Arc::new(ManualClock::new(sealed_now_ns()));
    let a = write_flush(store.as_ref(), &Flush::v7(0, 0xA0, 1, 1.5)).await;
    let b = write_flush(store.as_ref(), &Flush::v7(0, 0xA1, 2, 2.5)).await;
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

    let migrated = migrate(store.as_ref(), &config, 1).await.expect("migrate");
    assert_eq!(
        migrated.verification,
        Some(Verification::FloorRaised {
            floor_version: drill()
        }),
        "{migrated:?}"
    );
    let raised = floors(store.as_ref()).await;
    let [floor] = raised.as_slice() else {
        panic!("one floor entry: {raised:?}");
    };
    assert_eq!(
        (floor.family.as_str(), floor.floor_version),
        (FAMILY_RSEG, drill())
    );
    let basis = floor.basis.expect("migrate raises the floor with a basis");
    assert_eq!(
        basis.observed_entries, 1,
        "the basis counts the compaction part, not the superseded v7 inputs"
    );

    let newer = Flush {
        version: SegmentVersion::Drill,
        ts_ns: basis.observed_newest_created_unix_ns + SECOND_NS,
        ..Flush::v7(0, 0xC0, 4, 4.5)
    };
    write_flush(store.as_ref(), &newer).await;
    // `classify_floor` is fed a hand-built `FloorObservation` here, not one
    // the store audit produced; the `certify_at` assertion below is what
    // carries this row against the real store.
    assert_eq!(
        classify_floor(
            floor,
            &FloorObservation {
                live_below_floor: 0,
                newest_created_unix_ns: Some(newer.ts_ns),
                scan_shards: 1,
            }
        ),
        FloorEvidence::Stale,
        "a record newer than the basis and none live below the floor"
    );

    let (mark, delete) = sweep_times(&config);
    let report = certify_at(store.as_ref(), mark, None)
        .await
        .expect("certify");
    assert_eq!(
        report,
        CertifyReport::Contradicted {
            member_key: a.data_key.clone().min(b.data_key.clone()),
            found_version: v7(),
        },
        "the census meets the superseded v7 inputs the floor does not count"
    );
    assert!(!has_certificate(store.as_ref()).await);
    assert_eq!(
        floors(store.as_ref()).await,
        raised,
        "certify moves no floor"
    );

    for now in [mark, delete] {
        sweep(store.as_ref(), &config, now, 0).await.expect("sweep");
    }
    let report = certify_at(store.as_ref(), delete, None)
        .await
        .expect("certify");
    assert!(
        matches!(
            report,
            CertifyReport::Certified { version, member_count: 2, .. } if version == drill()
        ),
        "the compaction part and the newer N+1 record, counted by the census: {report:?}"
    );
    assert!(has_certificate(store.as_ref()).await);
    assert_eq!(
        floors(store.as_ref()).await,
        raised,
        "the floor history is the one migrate wrote"
    );
}
