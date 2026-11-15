//! `Catalog::resolve_ingest_hours` (issue #4257): selects the segments filed
//! under an ingest-hour range, whatever their event times, through the
//! listing path and through a folded snapshot alike, while `resolve` keeps
//! filtering by event time.
//!
//! Every fixture segment's event time is in hour 10 and its ingest hour is
//! near 1000, so an event-time window anchored at the ingest hours matches
//! none of them.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NS_PER_MIN: i64 = 60_000_000_000;
const SEAL_MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;
/// The hour every fixture segment's events fall in.
const EVENT_HOUR: i64 = 10;
/// The ingest hour `now` falls in.
const NOW_HOUR: u32 = 1003;

fn tenant() -> TenantHash {
    TenantHash([0x42; 16])
}

fn config() -> CatalogConfig {
    CatalogConfig {
        shard_count: 1,
        ..Default::default()
    }
}

fn now_ns() -> i64 {
    i64::from(NOW_HOUR) * NS_PER_HOUR + 40 * NS_PER_MIN
}

/// Publishes one segment filed under `ingest_hour` whose events are in
/// [`EVENT_HOUR`], and returns its data object key.
async fn publish_filed(store: &dyn ObjectStoreBackend, ingest_hour: u32) -> String {
    let payload = format!("seg-{ingest_hour}").into_bytes();
    let event_ts_ns = EVENT_HOUR * NS_PER_HOUR + i64::from(ingest_hour) * NS_PER_MIN / 100;
    let created_unix_ns = i64::from(ingest_hour) * NS_PER_HOUR + 5 * NS_PER_MIN;
    let record = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard: 0,
        writer_id: Uuid::new_v4(),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: payload.len() as u64,
        content_hash: *blake3::hash(&payload).as_bytes(),
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: event_ts_ns,
        max_event_ts_ns: event_ts_ns,
        min_ingest_ts_ns: created_unix_ns,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket: ingest_hour,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&record).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &record, &RetryPolicy::default())
        .await
        .expect("publish");
    data_key
}

async fn ingest_hours(catalog: &Catalog, start_hour: u32, end_hour: u32) -> BTreeSet<String> {
    ingest_hours_accounted(catalog, start_hour, end_hour, &QueryAccounting::new()).await
}

async fn ingest_hours_accounted(
    catalog: &Catalog,
    start_hour: u32,
    end_hour: u32,
    accounting: &QueryAccounting,
) -> BTreeSet<String> {
    catalog
        .resolve_ingest_hours(
            &tenant(),
            Signal::Metrics,
            start_hour,
            end_hour,
            now_ns(),
            accounting,
        )
        .await
        .expect("resolve_ingest_hours")
        .segments
        .into_iter()
        .map(|segment| segment.data_object_key)
        .collect()
}

/// Listing path: exactly the segments filed in the requested hours, an
/// `end_hour` past `now` clamped to the hour of `now` plus the clock-skew
/// allowance (a segment filed in the hour after that is left out), an
/// inverted range empty, and `resolve` over the same hours in event time
/// still empty.
#[tokio::test]
async fn listing_selects_by_ingest_hour_not_event_time() {
    // `now` is 40 minutes into NOW_HOUR, so `now` plus the 5-minute skew
    // allowance is still in NOW_HOUR, and all of NOW_HOUR + 1 is past it.
    const FUTURE_HOUR: u32 = NOW_HOUR + 1;
    assert!(i64::from(FUTURE_HOUR) * NS_PER_HOUR > now_ns() + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS);
    let store = Arc::new(MemoryStore::new());
    let mut by_hour = Vec::new();
    for hour in 1000..=FUTURE_HOUR {
        by_hour.push((hour, publish_filed(store.as_ref(), hour).await));
    }
    let keys_in = |lo: u32, hi: u32| -> BTreeSet<String> {
        by_hour
            .iter()
            .filter(|(hour, _)| (lo..=hi).contains(hour))
            .map(|(_, key)| key.clone())
            .collect()
    };
    let catalog = Catalog::new(store.clone(), config()).expect("catalog");

    assert_eq!(
        ingest_hours(&catalog, 1001, 1002).await,
        keys_in(1001, 1002)
    );
    assert_eq!(
        ingest_hours(&catalog, 1002, u32::MAX).await,
        keys_in(1002, NOW_HOUR),
        "an end hour past now is clamped to now's hour, leaving out {FUTURE_HOUR}"
    );
    assert_eq!(
        ingest_hours(&catalog, FUTURE_HOUR, FUTURE_HOUR).await,
        BTreeSet::new(),
        "a range wholly past now plus the skew allowance is empty"
    );
    assert_eq!(ingest_hours(&catalog, 1002, 1001).await, BTreeSet::new());

    let event_window = TimeRange {
        start_ns: 1001 * NS_PER_HOUR,
        end_ns: 1003 * NS_PER_HOUR,
    };
    let snapshot = catalog
        .resolve(&tenant(), Signal::Metrics, event_window, &[], now_ns())
        .await
        .expect("resolve");
    assert_eq!(
        snapshot.segments.len(),
        0,
        "resolve still filters by event time"
    );
}

/// Snapshot path: with hours 999 and 1000 folded (watermark 1000), a range
/// starting at the watermark takes hour 1000 from the snapshot and 1001..1002
/// from the listing, and leaves hour 999 and hour 1003 out. The GET count
/// shows the snapshot served hour 1000: its commit record is never fetched.
#[tokio::test]
async fn a_folded_snapshot_selects_by_ingest_hour_not_event_time() {
    let store = Arc::new(MemoryStore::new());
    let sealed_999 = publish_filed(store.as_ref(), 999).await;
    let sealed_1000 = publish_filed(store.as_ref(), 1000).await;
    let report = Catalog::new(store.clone(), config())
        .expect("catalog")
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            (1000 + 1) * NS_PER_HOUR + SEAL_MARGIN_NS,
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(
        report.watermark_hour,
        Some(1000),
        "the fold seals hour 1000"
    );
    let recent_1001 = publish_filed(store.as_ref(), 1001).await;
    let recent_1002 = publish_filed(store.as_ref(), 1002).await;
    let recent_1003 = publish_filed(store.as_ref(), 1003).await;

    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let accounting = QueryAccounting::new();
    let got = ingest_hours_accounted(&catalog, 1000, 1002, &accounting).await;
    assert_eq!(
        got,
        BTreeSet::from([sealed_1000, recent_1001, recent_1002]),
        "hour 999 ({sealed_999}) and hour 1003 ({recent_1003}) are outside the range"
    );
    // Hour 1000 came from the snapshot: the resolve GETs the commit records of
    // hours 1001 and 1002 only. A resolve that listed hour 1000 instead GETs
    // its commit record too, for 5 GETs.
    let cost = accounting.snapshot();
    assert_eq!(
        cost.s3_requests(AccountedOp::Get),
        4,
        "the snapshot HEAD, its one part, and the commit records of hours 1001 and 1002"
    );
    assert_eq!(cost.s3_requests(AccountedOp::List), 2);
    assert_eq!(cost.s3_requests(AccountedOp::Head), 0);
}
