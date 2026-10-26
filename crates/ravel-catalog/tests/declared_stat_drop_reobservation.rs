//! The declared-stat drop metric counts reads, not records (ADR-0873 decision
//! 2), and that holds on a warm resolve whose L0 refs come from a stored
//! derivation (issue #3231): each resolve that includes a record with a dropped
//! stamp observes that drop once, whether it derived the ref or reused the
//! derivation the cache kept.
//!
//! This lives in its own integration binary on purpose.
//! `ravel_commit::declared_stats::declared_stat_drops_observed` is a
//! process-wide monotonic tally, and the crate's `--lib` binary has other tests
//! reading defective commit-record stamps at the same time, so an exact delta
//! is only meaningful where every reader in the process is known. Here that set
//! is this file, and the lock below serialises it.

#![allow(clippy::expect_used)]

use std::sync::Arc;

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::declared_stats::{
    StatCarrier, declared_stat_drops_observed, stamp_commit_record,
};
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

/// Serialises every test in this file, so a delta on the process-wide counter
/// is attributable to exactly the resolves this test ran.
static COUNTER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const HOUR: u32 = 500_000;
const SIGNAL: Signal = Signal::Logs;
const SAMPLE_COUNT: u64 = 100_000;

fn tenant() -> TenantHash {
    TenantHash([0xd7; 16])
}

fn config() -> CatalogConfig {
    CatalogConfig {
        shard_count: 1,
        ..Default::default()
    }
}

fn i64_stat(name: &str, min: i64, max: i64) -> DeclaredColumnStat {
    DeclaredColumnStat::new(
        name,
        DeclaredStatType::I64,
        Some(DeclaredStatValue::I64(min)),
        Some(DeclaredStatValue::I64(max)),
        0,
    )
    .expect("valid stat")
}

/// One L0 commit record carrying two valid stamps and one whose null count
/// exceeds the record's row count, which every read drops.
fn record_with_one_dropped_stamp() -> ravel_proto::commit::v1::CommitRecord {
    let start = i64::from(HOUR) * NS_PER_HOUR;
    let mut record = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: SIGNAL,
        shard: 0,
        writer_id: Uuid::from_u128(0x2000),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: 1_000_000,
        content_hash: *blake3::hash(b"drop-reobservation").as_bytes(),
        sample_count: SAMPLE_COUNT,
        series_count: 1,
        min_event_ts_ns: start + 1_000,
        max_event_ts_ns: start + 1_500,
        min_ingest_ts_ns: start + 1_000,
        max_ingest_ts_ns: start + 1_500,
        segment_format_version: 4,
        created_unix_ns: start + 1_000,
        ingest_hour_bucket: HOUR,
    })
    .expect("valid record");
    stamp_commit_record(
        &mut record,
        &[
            i64_stat("EventDate", 15_887, 15_900),
            i64_stat("CounterID", 1, 230_000),
        ],
    );
    let mut dropped = record.declared_column_stats[0].clone();
    dropped.name = "Dropped".to_string();
    dropped.null_count = SAMPLE_COUNT + 1;
    record.declared_column_stats.push(dropped);
    record
}

/// One cold resolve and three warm ones observe exactly one drop each: the
/// warm resolves reuse the stored derivation and still report the drop it
/// recorded. A catalog that caches nothing observes the same one drop per
/// resolve, and every resolve carries the same two kept stamps.
///
/// FLIP: delete the `observe_declared_stat_drops` call on `l0_segment_ref`'s
/// stored-derivation branch; the delta after the warm resolves is 1, not 4.
#[tokio::test]
async fn each_resolve_observes_a_stored_derivations_drop_once() {
    let _guard = COUNTER.lock().await;
    let store = Arc::new(MemoryStore::new());
    publish::publish(
        store.as_ref(),
        &record_with_one_dropped_stamp(),
        &RetryPolicy::default(),
    )
    .await
    .expect("publish");
    let range = TimeRange {
        start_ns: i64::from(HOUR) * NS_PER_HOUR,
        end_ns: i64::from(HOUR + 1) * NS_PER_HOUR,
    };
    let now = i64::from(HOUR + 1) * NS_PER_HOUR;

    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let before = declared_stat_drops_observed(StatCarrier::CommitRecord);
    let cold = catalog
        .resolve(&tenant(), SIGNAL, range, &[], now)
        .await
        .expect("cold resolve");
    assert_eq!(
        declared_stat_drops_observed(StatCarrier::CommitRecord) - before,
        1,
        "the cold resolve observes the one dropped stamp"
    );
    assert_eq!(cold.segments.len(), 1);
    let kept: Vec<&str> = cold.segments[0]
        .declared_column_stats
        .as_slice()
        .iter()
        .map(|stat| stat.name())
        .collect();
    assert_eq!(kept, ["EventDate", "CounterID"]);

    for pass in 0..3 {
        let warm = catalog
            .resolve(&tenant(), SIGNAL, range, &[], now)
            .await
            .expect("warm resolve");
        assert_eq!(
            warm.segments[0].declared_column_stats, cold.segments[0].declared_column_stats,
            "warm pass {pass}"
        );
    }
    assert_eq!(
        declared_stat_drops_observed(StatCarrier::CommitRecord) - before,
        4,
        "each of the three warm resolves observes the drop once"
    );

    let uncached = Catalog::new(
        store,
        CatalogConfig {
            cache_capacity_per_tenant: 0,
            ..config()
        },
    )
    .expect("uncached catalog");
    let fresh = uncached
        .resolve(&tenant(), SIGNAL, range, &[], now)
        .await
        .expect("uncached resolve");
    assert_eq!(
        fresh.segments[0].declared_column_stats,
        cold.segments[0].declared_column_stats
    );
    assert_eq!(
        declared_stat_drops_observed(StatCarrier::CommitRecord) - before,
        5,
        "an uncached resolve observes the drop once"
    );
}
