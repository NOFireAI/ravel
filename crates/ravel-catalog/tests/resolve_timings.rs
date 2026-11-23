//! `ResolveTimings` (issue #4634): the four parts of one resolve are disjoint
//! intervals that sum to the resolve's whole duration, `other` included.
//!
//! The tenant is folded, so the resolve loads a snapshot part before its LIST
//! wave. The part GET (counted in `other`) and the `del/` LIST (counted in
//! `list_wave`) are each held for [`DELAY`], and the resolve call is timed
//! around the outside: `total()` must account for that elapsed time, which it
//! can do only when `other` carries the held part GET.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS,
};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const SEAL_MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// How long each gated request is held.
const DELAY: Duration = Duration::from_millis(200);

/// How far the resolve's elapsed time may sit above the parts' sum: the work
/// between the call and the catalog's first and last clock reads.
const OUTSIDE_TOLERANCE: Duration = Duration::from_millis(5);

fn tenant() -> TenantHash {
    TenantHash([0x46; 16])
}

async fn publish_at(store: &dyn ObjectStoreBackend, writer_seq: u64, hour: u32) {
    let event_ts_ns = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
    let payload = format!("seg-{writer_seq}-{hour}").into_bytes();
    let record = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard: 0,
        writer_id: Uuid::from_u128(0x4634),
        writer_epoch: 1,
        writer_seq,
        object_size: payload.len() as u64,
        content_hash: *blake3::hash(&payload).as_bytes(),
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: event_ts_ns,
        max_event_ts_ns: event_ts_ns,
        min_ingest_ts_ns: event_ts_ns,
        max_ingest_ts_ns: event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: event_ts_ns,
        ingest_hour_bucket: hour,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&record).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &record, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// Releases each held call once it has been held for [`DELAY`], and records
/// the op of every call it released. The store's gates share one held set, so
/// one releaser serves both.
fn delay_each(gate: GateHandle) -> (tokio::task::JoinHandle<()>, Arc<Mutex<Vec<Op>>>) {
    let released = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&released);
    let task = tokio::spawn(async move {
        loop {
            gate.wait_until_held(1).await;
            tokio::time::sleep(DELAY).await;
            for (id, op, _key) in gate.held_details() {
                if gate.release(id) {
                    log.lock().unwrap().push(op);
                }
            }
        }
    });
    (task, released)
}

#[tokio::test]
async fn the_four_parts_sum_to_the_resolves_elapsed_time() {
    let faulty = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store: Arc<dyn ObjectStoreBackend> = Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>;
    let sealed_hour = 1000u32;
    publish_at(store.as_ref(), 1, sealed_hour).await;
    let folded = Catalog::new(Arc::clone(&store), CatalogConfig::default())
        .expect("catalog")
        .fold(
            &tenant(),
            Signal::Metrics,
            Uuid::new_v4(),
            (i64::from(sealed_hour) + 1) * NS_PER_HOUR + SEAL_MARGIN_NS,
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(folded.watermark_hour, Some(sealed_hour));
    publish_at(store.as_ref(), 2, sealed_hour + 1).await;

    let now_ns = i64::from(sealed_hour + 1) * NS_PER_HOUR + 40 * 60_000_000_000;
    let range = TimeRange {
        start_ns: i64::from(sealed_hour) * NS_PER_HOUR,
        end_ns: now_ns,
    };
    let catalog = Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog");
    faulty.hold(Op::Get, Some(".csnap".to_string()), Occurrence::Always);
    let (releaser, released) =
        delay_each(faulty.hold(Op::List, Some("/del/".to_string()), Occurrence::Always));

    let started = Instant::now();
    let (snapshot, _origins, timings) = catalog
        .resolve_pruned_with_admission_timed(
            &tenant(),
            Signal::Metrics,
            range,
            &[],
            now_ns,
            None,
            &QueryAccounting::new(),
        )
        .await
        .expect("resolve");
    let elapsed = started.elapsed();
    releaser.abort();

    assert_eq!(
        snapshot.segments.len(),
        2,
        "the sealed and the recent segment"
    );
    assert_eq!(
        *released.lock().unwrap(),
        vec![Op::Get, Op::List],
        "one part GET, then one del/ LIST"
    );
    // hygiene-allow: wall-clock -- the parts are real monotonic durations by
    // design and the subject is how they relate to the call's own wall time;
    // each held part has a lower bound only.
    assert!(timings.list_wave >= DELAY, "{timings:?}");
    assert!(timings.other >= DELAY, "{timings:?}");
    assert!(timings.total() >= DELAY, "{timings:?}");
    assert!(timings.total() <= elapsed, "{timings:?} over {elapsed:?}");
    assert!(
        elapsed - timings.total() < OUTSIDE_TOLERANCE,
        "{timings:?} leave {:?} of {elapsed:?} unaccounted",
        elapsed - timings.total()
    );
}
