//! Resolve-time conflict resolution for overlapping compaction records
//! (issue #1070). When one sealed bucket holds two compaction records whose
//! input sets overlap, query-time dedup by `(series_id, ts)` collapses the
//! overlap for metrics, but logs and spans have no query-time dedup and would
//! return the overlapping records twice. The resolver picks one authoritative
//! record per overlap component (smallest `input_set_hash`), serves its parts,
//! and serves any input the winner does not name as a raw L0 segment.
//!
//! Every test drives a resolve through the real `Catalog` against a
//! `MemoryStore`, the same reachability the fix has (the resolver runs on
//! every query). No segment bytes are needed: resolution reads only records.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use prost::Message;
use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{keys, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord,
};
use ravel_segment::VERSION_V7;
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const HOUR: u32 = 500_000;

fn tenant() -> TenantHash {
    TenantHash([0xab; 16])
}

fn config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        shard_count,
        ..Default::default()
    }
}

/// A self-consistent L0 commit record for `signal`. No segment bytes:
/// resolution reads only the record.
fn l0_record(signal: Signal, writer_id: Uuid, seq: u64, created_unix_ns: i64) -> CommitRecord {
    let start = i64::from(HOUR) * NS_PER_HOUR;
    record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: 100,
        content_hash: [seq as u8 ^ 0x5a; 32],
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: start,
        max_event_ts_ns: start + 100,
        min_ingest_ts_ns: start,
        max_ingest_ts_ns: start + 100,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns,
        ingest_hour_bucket: HOUR,
    })
    .expect("valid record")
}

async fn put_l0(store: &dyn ObjectStoreBackend, record: &CommitRecord) {
    let key = keys::commit_key_for_record(record).expect("commit key");
    store
        .put(&key, record::encode(record), PutOptions::create_if_absent())
        .await
        .expect("put commit record");
}

fn part(part_index: u32, seed: u8) -> CompactionPart {
    let start = i64::from(HOUR) * NS_PER_HOUR;
    CompactionPart {
        part_index,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xff; 16],
        content_hash: vec![seed; 32],
        object_size: 4096,
        sample_count: 10,
        series_count: 2,
        run_count: 3,
        min_event_ts_ns: start,
        max_event_ts_ns: start + 100,
        segment_format_version: u32::from(VERSION_V7),
        declared_column_stats: Vec::new(),
    }
}

/// Build and PUT a compaction record naming `inputs` for `signal`. `seed`
/// drives the record's `input_set_hash` (and thus its key and the resolver's
/// tie-break) independently of `inputs`, so a test can pin which of two
/// overlapping records wins.
async fn put_compaction_record(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    inputs: &[&CommitRecord],
    parts: Vec<CompactionPart>,
    created_unix_ns: i64,
    seed: &[u8],
) {
    let input_set_hash = *blake3::hash(seed).as_bytes();
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        level: 1,
        inputs: inputs
            .iter()
            .map(|r| CompactionInputIdentity {
                writer_id: r.writer_id.clone(),
                writer_epoch: r.writer_epoch,
                writer_seq: r.writer_seq,
            })
            .collect(),
        input_set_hash: input_set_hash.to_vec(),
        parts,
        created_unix_ns,
    };
    let hash16: String = input_set_hash[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let key = keys::compaction_record_key(&tenant(), signal, 0, HOUR, &hash16).expect("record key");
    store
        .put(
            &key,
            record.encode_to_vec().into(),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put compaction record");
}

fn hour_range_and_now() -> (TimeRange, i64) {
    let start = i64::from(HOUR) * NS_PER_HOUR;
    let now = start + 30 * 60_000_000_000;
    (
        TimeRange {
            start_ns: start,
            end_ns: start + NS_PER_HOUR,
        },
        now,
    )
}

fn l1_keys(snapshot: &ravel_catalog::Snapshot) -> Vec<String> {
    snapshot
        .segments
        .iter()
        .filter(|s| matches!(s.level, SegmentLevel::L1 { .. }))
        .map(|s| s.data_object_key.clone())
        .collect()
}

fn l0_keys(snapshot: &ravel_catalog::Snapshot) -> Vec<String> {
    snapshot
        .segments
        .iter()
        .filter(|s| matches!(s.level, SegmentLevel::L0))
        .map(|s| s.data_object_key.clone())
        .collect()
}

/// Order two seeds by their `input_set_hash`, returning
/// `(smaller_hash_seed, larger_hash_seed)`. The resolver's tie-break keeps the
/// smaller-hash record, so a test hands the winning role the smaller seed.
fn order_by_hash<'a>(x: &'a [u8], y: &'a [u8]) -> (&'a [u8], &'a [u8]) {
    if blake3::hash(x).as_bytes() <= blake3::hash(y).as_bytes() {
        (x, y)
    } else {
        (y, x)
    }
}

/// Two overlapping records in one logs bucket resolve to the ONE authoritative
/// record's parts, not both. The winner names the superset `[a, b]`, so the
/// loser adds no uncovered input and the snapshot is exactly one L1 part.
///
/// Flipped to watch it fail before the fix: the `losing_compaction_records`
/// skip in `process_bucket`'s part-inclusion loop (catalog.rs). Without it the
/// resolver includes both records' parts, so `l1_keys().len()` is 2 and the
/// `== 1` assertion below fails.
#[tokio::test]
async fn overlapping_logs_records_resolve_to_one_authoritative() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let a = l0_record(Signal::Logs, Uuid::new_v4(), 1, now);
    let b = l0_record(Signal::Logs, Uuid::new_v4(), 2, now);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;

    // Winner names [a, b] (superset); loser names [a] (overlap on a). The
    // winner takes the smaller-hash seed so the tie-break is pinned.
    let (win_seed, lose_seed) = order_by_hash(b"set-1", b"set-2");
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a, &b],
        vec![part(0, 1)],
        now,
        win_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a],
        vec![part(0, 2)],
        now,
        lose_seed,
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Logs, range, &[], now)
        .await
        .expect("resolve");

    assert_eq!(
        l1_keys(&snapshot).len(),
        1,
        "one authoritative L1 part, not both"
    );
    assert_eq!(snapshot.segments.len(), 1, "no uncovered L0 leftover");
    assert_eq!(catalog.compaction_input_set_conflicts(), 1);
}

/// The same for spans: overlapping records resolve to one authoritative part.
///
/// Flipped to watch it fail before the fix: same skip as above; without it
/// `l1_keys().len()` is 2.
#[tokio::test]
async fn overlapping_spans_records_resolve_to_one_authoritative() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let a = l0_record(Signal::Spans, Uuid::new_v4(), 1, now);
    let b = l0_record(Signal::Spans, Uuid::new_v4(), 2, now);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;

    let (win_seed, lose_seed) = order_by_hash(b"set-1", b"set-2");
    put_compaction_record(
        store.as_ref(),
        Signal::Spans,
        &[&a, &b],
        vec![part(0, 1)],
        now,
        win_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Spans,
        &[&a],
        vec![part(0, 2)],
        now,
        lose_seed,
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Spans, range, &[], now)
        .await
        .expect("resolve");

    assert_eq!(
        l1_keys(&snapshot).len(),
        1,
        "one authoritative L1 part, not both"
    );
    assert_eq!(snapshot.segments.len(), 1, "no uncovered L0 leftover");
    assert_eq!(catalog.compaction_input_set_conflicts(), 1);
}

/// An L0 named only by the LOSING record (not by the winner) is not lost: it
/// falls through to the raw-L0 pass and is served as an L0 segment. The winner
/// names `[a]`; the loser names `[a, c]`, overlapping on `a` but adding `c`.
///
/// Flipped to watch it fail before the fix: the change that builds `excluded`
/// from WINNING records only (catalog.rs). Before the fix `excluded` unions
/// every record's inputs, so `c` is excluded and served (double-counted)
/// inside the loser's part instead: `l0_keys().len()` is 0 and the `== 1`
/// assertion below fails.
#[tokio::test]
async fn losing_records_uncovered_l0_input_is_served_as_raw_l0() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let a = l0_record(Signal::Logs, Uuid::new_v4(), 1, now);
    let c = l0_record(Signal::Logs, Uuid::new_v4(), 2, now);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &c).await;

    // Winner names [a] (smaller seed); loser names [a, c] (overlap on a, extra
    // c). c is named by no winner, so it must be served as a raw L0.
    let (win_seed, lose_seed) = order_by_hash(b"set-1", b"set-2");
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a],
        vec![part(0, 1)],
        now,
        win_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a, &c],
        vec![part(0, 2)],
        now,
        lose_seed,
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Logs, range, &[], now)
        .await
        .expect("resolve");

    assert_eq!(l1_keys(&snapshot).len(), 1, "only the winner's part");
    let served_l0 = l0_keys(&snapshot);
    assert_eq!(
        served_l0.len(),
        1,
        "the loser's uncovered input c survives as L0"
    );
    let c_key = keys::reconstruct_data_key(&c).expect("c data key");
    assert_eq!(served_l0[0], c_key, "the survivor is exactly c");
    assert_eq!(snapshot.segments.len(), 2, "one L1 part plus raw L0 c");
    assert_eq!(catalog.compaction_input_set_conflicts(), 1);
}

/// Two records with DISJOINT input sets are not in conflict: both still
/// resolve, exactly as before the fix. This pins that the conflict rule fires
/// only on overlap.
///
/// Flipped to watch it fail (the assertion is load-bearing): make
/// `select_authoritative_compaction_records` treat any two records as
/// conflicting (drop the per-component `members.len() < 2` guard and pick one
/// winner across the whole bucket). Then one disjoint record is wrongly
/// dropped, `l1_keys().len()` is 1, and the `== 2` assertion below fails.
#[tokio::test]
async fn disjoint_records_both_resolve() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let a = l0_record(Signal::Logs, Uuid::new_v4(), 1, now);
    let b = l0_record(Signal::Logs, Uuid::new_v4(), 2, now);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;

    // Disjoint: [a] and [b] share no input identity.
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a],
        vec![part(0, 1)],
        now,
        b"set-1",
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&b],
        vec![part(0, 2)],
        now,
        b"set-2",
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Logs, range, &[], now)
        .await
        .expect("resolve");

    assert_eq!(
        l1_keys(&snapshot).len(),
        2,
        "disjoint records both serve their parts"
    );
    assert_eq!(
        snapshot.segments.len(),
        2,
        "both inputs excluded, no L0 leftover"
    );
    assert_eq!(catalog.compaction_input_set_conflicts(), 1);
}

/// The conflicts counter increments exactly once for the overlapping case (not
/// zero, not per-record). Paired with the `l1_keys().len() == 1` assertion so
/// the test also pins that the overlap was actually resolved to one record.
///
/// Flipped to watch the counter assertion fail: change the counter site's
/// `fetch_add(1, ...)` to `fetch_add(2, ...)` in `process_bucket`
/// (catalog.rs); the `== 1` below then fails. The `l1_keys().len() == 1`
/// assertion additionally fails before the fix, as in the tests above.
#[tokio::test]
async fn overlapping_conflicts_counter_increments_exactly_once() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let a = l0_record(Signal::Logs, Uuid::new_v4(), 1, now);
    let b = l0_record(Signal::Logs, Uuid::new_v4(), 2, now);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;

    // Overlap on b; winner names the superset [a, b].
    let (win_seed, lose_seed) = order_by_hash(b"set-1", b"set-2");
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a, &b],
        vec![part(0, 1)],
        now,
        win_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&b],
        vec![part(0, 2)],
        now,
        lose_seed,
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Logs, range, &[], now)
        .await
        .expect("resolve");

    assert_eq!(
        catalog.compaction_input_set_conflicts(),
        1,
        "once per conflicted bucket"
    );
    assert_eq!(
        l1_keys(&snapshot).len(),
        1,
        "resolved to one authoritative record"
    );
    assert_eq!(snapshot.segments.len(), 1);
}
