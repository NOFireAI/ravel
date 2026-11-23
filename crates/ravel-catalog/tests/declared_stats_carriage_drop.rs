//! ADR-4628: a stamp that fails a decision-2 validity clause on its source
//! commit record is never carried onto the sealed snapshot entry, and the drop
//! is counted exactly once, under the carrier label the fold's read uses.
//!
//! This lives in its own integration binary on purpose, like
//! `declared_stat_drop_reobservation.rs`:
//! `ravel_commit::declared_stats::declared_stat_drops_observed` is a
//! process-wide monotonic tally, and the crate's other declared-stat tests read
//! defective stamps concurrently, so an exact delta is only meaningful where
//! every reader in the process is known. Here that set is this file, and the
//! lock below serialises it.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, PartLimits, decode_part,
};
use ravel_commit::declared_stats::{
    StatCarrier, declared_stat_drops_observed, declared_stat_drops_observed_total,
    stamp_commit_record,
};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::SnapshotEntry;
use ravel_segment::VERSION_V7;
use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

/// Serialises every test in this file, so a delta on the process-wide counter
/// is attributable to exactly the folds this test ran.
static COUNTER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;
const SIGNAL: Signal = Signal::Logs;

fn tenant() -> TenantHash {
    TenantHash([0x4d; 16])
}

fn config() -> CatalogConfig {
    CatalogConfig {
        shard_count: 1,
        ..Default::default()
    }
}

fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn i64_sum(name: &str, min: i64, max: i64, null_count: u64, sum: i128) -> DeclaredColumnStat {
    DeclaredColumnStat::new(
        name,
        DeclaredStatType::I64,
        Some(DeclaredStatValue::I64(min)),
        Some(DeclaredStatValue::I64(max)),
        null_count,
    )
    .expect("valid stat")
    .with_sum(sum)
    .expect("sum attaches (bounds are checked at decode, not here)")
}

async fn put_l0(store: &dyn ObjectStoreBackend, hour: u32, stats: &[DeclaredColumnStat]) {
    let event = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
    let mut rec = record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: SIGNAL,
        shard: 0,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: 100,
        content_hash: [0x5a; 32],
        sample_count: 100,
        series_count: 1,
        min_event_ts_ns: event,
        max_event_ts_ns: event + 100,
        min_ingest_ts_ns: event,
        max_ingest_ts_ns: event + 100,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: event,
        ingest_hour_bucket: hour,
    })
    .expect("valid record");
    stamp_commit_record(&mut rec, stats);
    let key = keys::commit_key_for_record(&rec).expect("commit key");
    store
        .put(&key, record::encode(&rec), PutOptions::create_if_absent())
        .await
        .expect("put commit record");
}

async fn fold_and_read_entries(
    store: &Arc<MemoryStore>,
    catalog: &Catalog,
    now_ns: i64,
) -> Vec<SnapshotEntry> {
    let report = catalog
        .fold(&tenant(), SIGNAL, Uuid::new_v4(), now_ns, &[], None)
        .await
        .expect("fold");
    assert!(!report.no_op);
    let prefix = format!(
        "t/{}/catalog/{}/snap/",
        tenant().to_hex(),
        SIGNAL.key_prefix()
    );
    let page = store.list(&prefix, None).await.expect("list parts");
    assert_eq!(page.objects.len(), 1);
    let bytes = store
        .get(&page.objects[0].key, GetRange::Full)
        .await
        .expect("get part")
        .data;
    decode_part(&bytes, &PartLimits::default())
        .expect("part decodes")
        .entries
}

/// A commit record carrying one valid stamp and one whose sum lies outside
/// `[non_null * min, non_null * max]` (ADR-4628 decision 2): the fold carries
/// the valid one onto the sealed entry and drops the invalid one, counting the
/// drop exactly once. The drop lands under the `commit-record` label, not
/// `snapshot-entry`, because the fold reads the source record through
/// `read_commit_record`: `read_snapshot_entry`'s twin read is where a
/// snapshot-entry-labelled drop would come from, and it is not on this path.
#[tokio::test]
async fn a_clause_failing_sum_is_not_carried_and_the_drop_is_counted_once() {
    let _guard = COUNTER.lock().await;
    let store = Arc::new(MemoryStore::new());
    let hour = 500_700u32;
    let now = now_at_seal(hour);

    // 100 non-null rows between 0 and 10 allow a sum in [0, 1000]; "BadSum"'s
    // 5000 is outside it, "Keep"'s 500 is inside.
    let keep = i64_sum("Keep", 0, 10, 0, 500);
    let bad = i64_sum("BadSum", 0, 10, 0, 5000);
    put_l0(store.as_ref(), hour, &[keep.clone(), bad]).await;

    let before_total = declared_stat_drops_observed_total();
    let before_cr = declared_stat_drops_observed(StatCarrier::CommitRecord);
    let before_se = declared_stat_drops_observed(StatCarrier::SnapshotEntry);

    let catalog = Catalog::new(store.clone(), config()).expect("catalog");
    let entries = fold_and_read_entries(&store, &catalog, now).await;

    // The sealed entry carries "Keep" alone: the invalid column has no entry at
    // all on the immutable part.
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].declared_column_stats.len(), 1);
    assert_eq!(entries[0].declared_column_stats[0].name, "Keep");

    assert_eq!(
        declared_stat_drops_observed_total() - before_total,
        1,
        "exactly one stamp was dropped folding this record"
    );
    assert_eq!(
        declared_stat_drops_observed(StatCarrier::CommitRecord) - before_cr,
        1,
        "the drop is counted under the carrier the fold read: the commit record"
    );
    assert_eq!(
        declared_stat_drops_observed(StatCarrier::SnapshotEntry) - before_se,
        0,
        "no snapshot-entry-labelled drop: the fold never read the entry back here"
    );

    // The sealed resolve agrees: one covered column, "BadSum" uncovered.
    let range = TimeRange {
        start_ns: i64::from(hour) * NS_PER_HOUR,
        end_ns: now,
    };
    let segments = catalog
        .resolve(&tenant(), SIGNAL, range, &[], now)
        .await
        .expect("resolve")
        .segments;
    assert_eq!(segments.len(), 1);
    let stats = &segments[0].declared_column_stats;
    assert_eq!(stats.as_slice(), [keep].as_slice());
    assert_eq!(stats.column("BadSum"), None);
}
