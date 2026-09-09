//! Resolve-time conflict resolution for overlapping compaction records
//! (issue #1070). When one sealed bucket holds two compaction records whose
//! input sets overlap, query-time dedup by `(series_id, ts)` collapses the
//! overlap for metrics, but logs and spans have no query-time dedup and would
//! return the overlapping records twice. The resolver picks one authoritative
//! record per overlap component (largest input set, then smallest
//! `input_set_hash`, then the record key), serves its parts, and serves any
//! input the winner does not name as a raw L0 segment.
//!
//! Every test drives a resolve or a fold through the real `Catalog` against a
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
    part_with_rows(part_index, seed, 10)
}

/// A part carrying an exact row count, for the tests that assert the resolved
/// snapshot's rows equal the union of the inputs' rows. A compaction of logs
/// or spans concatenates its inputs, so a part's `sample_count` is the sum of
/// the rows of the inputs the record names.
fn part_with_rows(part_index: u32, seed: u8, sample_count: u64) -> CompactionPart {
    let start = i64::from(HOUR) * NS_PER_HOUR;
    CompactionPart {
        part_index,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xff; 16],
        content_hash: vec![seed; 32],
        object_size: 4096,
        sample_count,
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
/// hash tie-break) independently of `inputs`, so a test can pin which of two
/// equal-cardinality overlapping records wins. Returns the record so a test
/// can reconstruct its part keys.
async fn put_compaction_record(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    inputs: &[&CommitRecord],
    parts: Vec<CompactionPart>,
    created_unix_ns: i64,
    seed: &[u8],
) -> CompactionRecord {
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
    record
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
/// `(smaller_hash_seed, larger_hash_seed)`. Between records of EQUAL input-set
/// cardinality the resolver keeps the smaller-hash one, so such a test hands
/// the winning role the smaller seed. Where the cardinalities differ the hash
/// does not decide, and a test pins the winner by input count instead.
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
/// falls through to the raw-L0 pass and is served as an L0 segment. The two
/// records have equal input-set cardinality and neither set contains the
/// other, so the hash decides: the winner names `[a, b]` and the loser names
/// `[a, c]`, overlapping on `a`.
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
    let b = l0_record(Signal::Logs, Uuid::new_v4(), 2, now);
    let c = l0_record(Signal::Logs, Uuid::new_v4(), 3, now);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;
    put_l0(store.as_ref(), &c).await;

    // Winner names [a, b] (smaller seed); loser names [a, c] (overlap on a,
    // extra c). Same cardinality, so the hash decides. c is named by no
    // winner, so it must be served as a raw L0.
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

/// Rows a resolve serves, summed over its segments. Logs and spans have no
/// query-time dedup (docs/consistency-model.md): the distributed merge
/// concatenates the per-slice results and sorts them, so the rows a query
/// returns are exactly the rows of the resolved segments.
fn served_rows(snapshot: &ravel_catalog::Snapshot) -> u64 {
    snapshot.segments.iter().map(|s| s.sample_count).sum()
}

/// The three seeds ordered by their `input_set_hash`, ascending. A test that
/// needs the maximal-cardinality record to lose the hash comparison hands it
/// the last one.
fn seeds_by_hash(seeds: [&[u8]; 3]) -> [&[u8]; 3] {
    let mut ordered = seeds;
    ordered.sort_by_key(|s| *blake3::hash(s).as_bytes());
    ordered
}

fn sorted(mut keys: Vec<String>) -> Vec<String> {
    keys.sort();
    keys
}

/// A logs bucket with two overlapping records serves the UNION of the inputs'
/// rows, not their sum. `a`, `b` and `c` carry one row each; the winner's part
/// carries the two rows of `[a, b]` and the loser's the two rows of `[a, c]`.
/// The union is three rows: two inside the winner's part plus the one row of
/// the uncovered input `c`, served raw.
///
/// Flipped to watch it fail: make `select_authoritative_compaction_records`
/// return an empty set (catalog.rs), which is how the resolver behaved before
/// issue #1070. Both parts are then served and `a`'s row is counted twice, so
/// `served_rows` is 4 and the `== 3` assertion below fails.
#[tokio::test]
async fn overlapping_logs_rows_equal_the_union_not_the_sum() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let a = l0_record(Signal::Logs, Uuid::new_v4(), 1, now);
    let b = l0_record(Signal::Logs, Uuid::new_v4(), 2, now);
    let c = l0_record(Signal::Logs, Uuid::new_v4(), 3, now);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;
    put_l0(store.as_ref(), &c).await;

    let (win_seed, lose_seed) = order_by_hash(b"rows-1", b"rows-2");
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a, &b],
        vec![part_with_rows(0, 1, 2)],
        now,
        win_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a, &c],
        vec![part_with_rows(0, 2, 2)],
        now,
        lose_seed,
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Logs, range, &[], now)
        .await
        .expect("resolve");

    assert_eq!(
        served_rows(&snapshot),
        3,
        "the union of a, b and c, with a served once"
    );
    assert_eq!(l1_keys(&snapshot).len(), 1, "only the winner's part");
    assert_eq!(l0_keys(&snapshot).len(), 1, "the uncovered input c, raw");
}

/// The same for spans, which have no query-time dedup either.
///
/// Flipped to watch it fail: the same empty-set flip as above; `served_rows`
/// becomes 4.
#[tokio::test]
async fn overlapping_spans_rows_equal_the_union_not_the_sum() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let a = l0_record(Signal::Spans, Uuid::new_v4(), 1, now);
    let b = l0_record(Signal::Spans, Uuid::new_v4(), 2, now);
    let c = l0_record(Signal::Spans, Uuid::new_v4(), 3, now);
    put_l0(store.as_ref(), &a).await;
    put_l0(store.as_ref(), &b).await;
    put_l0(store.as_ref(), &c).await;

    let (win_seed, lose_seed) = order_by_hash(b"rows-1", b"rows-2");
    put_compaction_record(
        store.as_ref(),
        Signal::Spans,
        &[&a, &b],
        vec![part_with_rows(0, 1, 2)],
        now,
        win_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Spans,
        &[&a, &c],
        vec![part_with_rows(0, 2, 2)],
        now,
        lose_seed,
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Spans, range, &[], now)
        .await
        .expect("resolve");

    assert_eq!(
        served_rows(&snapshot),
        3,
        "the union of a, b and c, with a served once"
    );
    assert_eq!(l1_keys(&snapshot).len(), 1, "only the winner's part");
    assert_eq!(l0_keys(&snapshot).len(), 1, "the uncovered input c, raw");
}

/// The folded index names exactly what a live resolve serves: the winner's L1
/// entry and, as a raw L0 entry, the input only the loser names. The winner
/// names the three-input set `[a, b, c]` and carries the LARGER hash, so the
/// maximal-set tie-break is what selects it.
///
/// Fails on the branch before the tie-break change: the smaller-hash record
/// `[a, d]` wins there, so the folded snapshot names that record's part and
/// carries `b` and `c` as raw L0 entries. The `l1_keys` equality below fails
/// (it names the loser's part key), and so does the `l0_keys` equality (two
/// entries, `b` and `c`, instead of the one entry `d`).
#[tokio::test]
async fn fold_of_overlapping_records_names_the_winner_and_the_uncovered_input() {
    let store = Arc::new(MemoryStore::new());
    let hour_start = i64::from(HOUR) * NS_PER_HOUR;
    let created = hour_start + 60_000_000_000;
    // Six hours past the bucket, so the fold's watermark seals it.
    let now = hour_start + 6 * NS_PER_HOUR;
    let range = TimeRange {
        start_ns: hour_start,
        end_ns: now,
    };

    let a = l0_record(Signal::Logs, Uuid::new_v4(), 1, created);
    let b = l0_record(Signal::Logs, Uuid::new_v4(), 2, created);
    let c = l0_record(Signal::Logs, Uuid::new_v4(), 3, created);
    let d = l0_record(Signal::Logs, Uuid::new_v4(), 4, created);
    for record in [&a, &b, &c, &d] {
        put_l0(store.as_ref(), record).await;
    }

    // The maximal set takes the LARGER hash, so only cardinality can select
    // it. The two records overlap on a.
    let (lose_seed, win_seed) = order_by_hash(b"fold-1", b"fold-2");
    let winner = put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a, &b, &c],
        vec![part_with_rows(0, 1, 3)],
        created,
        win_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a, &d],
        vec![part_with_rows(0, 2, 2)],
        created,
        lose_seed,
    )
    .await;

    let winner_part_key =
        keys::reconstruct_l1_part_key(&winner, &winner.parts[0]).expect("winner part key");
    let d_key = keys::reconstruct_data_key(&d).expect("d data key");

    // Resolve from the direct listing first, then fold and resolve again: the
    // index fold and the resolver must derive the same bucket state.
    let before_catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let before = before_catalog
        .resolve(&tenant(), Signal::Logs, range, &[], now)
        .await
        .expect("resolve before fold");

    let fold_catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let report = fold_catalog
        .fold(&tenant(), Signal::Logs, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");
    assert!(!report.no_op, "the fold sealed the bucket");

    let after_catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let after = after_catalog
        .resolve(&tenant(), Signal::Logs, range, &[], now)
        .await
        .expect("resolve after fold");

    assert_eq!(
        l1_keys(&after),
        vec![winner_part_key.clone()],
        "the folded snapshot names only the winner's L1 entry"
    );
    assert_eq!(
        l0_keys(&after),
        vec![d_key.clone()],
        "and the loser's uncovered input as a raw L0 entry"
    );
    assert_eq!(after.segments.len(), 2);
    assert_eq!(
        before, after,
        "a live resolve returns the identical segment set"
    );
}

/// A transitive component: A overlaps B on `b`, B overlaps C on `d`, and A and
/// C are disjoint. All three are one component, and the maximal record B wins
/// even though it carries the largest hash. The inputs only A and C name (`a`
/// and `e`) are served raw; the inputs B names (`b`, `c`, `d`) are served
/// inside B's part, once each.
///
/// Fails on the branch before the tie-break change: the smallest-hash record A
/// wins there, so `l1_keys` names A's part and `l0_keys` is `[c, d, e]`. Both
/// equalities below fail.
#[tokio::test]
async fn transitive_overlap_component_resolves_to_the_maximal_record() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let a = l0_record(Signal::Logs, Uuid::new_v4(), 1, now);
    let b = l0_record(Signal::Logs, Uuid::new_v4(), 2, now);
    let c = l0_record(Signal::Logs, Uuid::new_v4(), 3, now);
    let d = l0_record(Signal::Logs, Uuid::new_v4(), 4, now);
    let e = l0_record(Signal::Logs, Uuid::new_v4(), 5, now);
    for record in [&a, &b, &c, &d, &e] {
        put_l0(store.as_ref(), record).await;
    }

    // A takes the smallest hash and B the largest, so only cardinality can
    // select B.
    let [a_seed, c_seed, b_seed] = seeds_by_hash([b"tri-1", b"tri-2", b"tri-3"]);
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a, &b],
        vec![part_with_rows(0, 1, 2)],
        now,
        a_seed,
    )
    .await;
    let winner = put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&b, &c, &d],
        vec![part_with_rows(0, 2, 3)],
        now,
        b_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&d, &e],
        vec![part_with_rows(0, 3, 2)],
        now,
        c_seed,
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Logs, range, &[], now)
        .await
        .expect("resolve");

    let winner_part_key =
        keys::reconstruct_l1_part_key(&winner, &winner.parts[0]).expect("winner part key");
    let a_key = keys::reconstruct_data_key(&a).expect("a data key");
    let e_key = keys::reconstruct_data_key(&e).expect("e data key");

    assert_eq!(
        l1_keys(&snapshot),
        vec![winner_part_key],
        "the maximal record of the component serves its part"
    );
    assert_eq!(
        sorted(l0_keys(&snapshot)),
        sorted(vec![a_key, e_key]),
        "the inputs no winner names are served raw"
    );
    assert_eq!(snapshot.segments.len(), 3);
    assert_eq!(
        served_rows(&snapshot),
        5,
        "each of the five inputs' rows served once"
    );
    assert_eq!(catalog.compaction_input_set_conflicts(), 1);
}

/// A strict superset wins over a smaller-hash subset, and leaves nothing
/// outside the winner: every input of the component is named by the winner, so
/// the snapshot is exactly one L1 part.
///
/// Fails on the branch before the tie-break change: the smaller-hash subset
/// `[a, b]` wins there, `c` is served as a raw L0, and the `l0_keys().is_empty
/// ()` and `segments.len() == 1` assertions below fail (one raw L0, two
/// segments).
#[tokio::test]
async fn strict_superset_input_set_wins_over_the_smaller_hash() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let a = l0_record(Signal::Logs, Uuid::new_v4(), 1, now);
    let b = l0_record(Signal::Logs, Uuid::new_v4(), 2, now);
    let c = l0_record(Signal::Logs, Uuid::new_v4(), 3, now);
    for record in [&a, &b, &c] {
        put_l0(store.as_ref(), record).await;
    }

    let (lose_seed, win_seed) = order_by_hash(b"sup-1", b"sup-2");
    let winner = put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a, &b, &c],
        vec![part_with_rows(0, 1, 3)],
        now,
        win_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&a, &b],
        vec![part_with_rows(0, 2, 2)],
        now,
        lose_seed,
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Logs, range, &[], now)
        .await
        .expect("resolve");

    let winner_part_key =
        keys::reconstruct_l1_part_key(&winner, &winner.parts[0]).expect("winner part key");
    assert_eq!(
        l1_keys(&snapshot),
        vec![winner_part_key],
        "the superset record serves its part"
    );
    assert_eq!(
        l0_keys(&snapshot),
        Vec::<String>::new(),
        "the superset leaves no input outside itself"
    );
    assert_eq!(snapshot.segments.len(), 1);
    assert_eq!(served_rows(&snapshot), 3);
}

/// Issue #1171: a `min_token` covered by BOTH of two overlapping compaction
/// records is satisfied through the WINNING record's parts only, never the
/// loser's. `resolve_min_token_fallback` is reached only when the token's
/// exact commit-record GET misses (the record was compacted away and swept),
/// so `target` is never PUT as an L0 here -- that is exactly the fallback's
/// intended scenario, not an oversight.
///
/// The winner names `[target, extra]` (the strict superset), so it wins on
/// cardinality regardless of hash; the loser names `[target]` only. Both
/// cover the token. The loser's seed is chosen so its key sorts first in the
/// bucket LIST (`BTreeMap` order in `MemoryStore`), which is what makes the
/// pre-fix bug deterministic: the old loop returns on the FIRST record whose
/// inputs cover the token, so it would return the loser's part.
///
/// Flipped to watch it fail: the
/// `losing_compaction_records.contains(ckey.as_str())` skip added to
/// `resolve_min_token_fallback`'s covers-test loop (catalog.rs). Without it
/// the loop matches the loser (listed first) and adds its part on top of the
/// winner's part the bucket listing already resolved: `l1_keys(&snapshot)`
/// carries BOTH keys instead of just the winner's, and the `assert_eq!`
/// below fails (observed: a two-element vec with the loser's key first and
/// the winner's second, against the expected one-element `[winner_part_key]`).
#[tokio::test]
async fn min_token_resolves_through_the_winning_record_only() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let target = l0_record(Signal::Logs, Uuid::new_v4(), 1, now);
    let extra = l0_record(Signal::Logs, Uuid::new_v4(), 2, now);
    let token = record::token_for(&target).expect("token");

    let (lose_seed, win_seed) = order_by_hash(b"min-token-set-1", b"min-token-set-2");
    let winner = put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&target, &extra],
        vec![part(0, 1)],
        now,
        win_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&target],
        vec![part(0, 2)],
        now,
        lose_seed,
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Logs, range, &[token], now)
        .await
        .expect("resolve");

    let winner_part_key =
        keys::reconstruct_l1_part_key(&winner, &winner.parts[0]).expect("winner part key");
    assert_eq!(
        l1_keys(&snapshot),
        vec![winner_part_key],
        "the token resolves through the winning record's part only"
    );
    assert_eq!(snapshot.segments.len(), 1, "no part from the losing record");
    assert_eq!(catalog.compaction_input_set_conflicts(), 1);
}

/// Reachability check for issue #1171 at the query-serving boundary
/// (`Catalog::resolve`, what every query endpoint calls through): a
/// `min_token` query over a logs bucket with two overlapping compaction
/// records returns exactly the winner's N rows, never `2N` from also serving
/// the loser's equally-sized overlapping part. Logs have no query-time
/// dedup by `(series_id, ts)` the way metrics do, so a duplicated part is a
/// duplicated result row, not a harmless re-count.
///
/// Winner and loser parts are given the SAME row count `N` on purpose: if the
/// fallback serves both (winner via the normal bucket listing, which already
/// resolves overlap correctly per issue #1070, and the loser via this
/// fallback's bug), the two parts carry different data keys and both are
/// counted, so the total is exactly `2N` rather than some other wrong number.
///
/// Flipped to watch it fail: the same
/// `losing_compaction_records.contains(ckey.as_str())` skip in
/// `resolve_min_token_fallback` (catalog.rs). Without it, `served_rows`
/// is `4` (`2N`), not `2` (`N`), and the `assert_eq!` below fails.
#[tokio::test]
async fn min_token_query_over_logs_returns_exact_rows_not_double() {
    let store = Arc::new(MemoryStore::new());
    let catalog = Catalog::new(store.clone(), config(1)).expect("catalog");
    let (range, now) = hour_range_and_now();

    let target = l0_record(Signal::Logs, Uuid::new_v4(), 1, now);
    let extra = l0_record(Signal::Logs, Uuid::new_v4(), 2, now);
    let token = record::token_for(&target).expect("token");

    const N: u64 = 2;
    let (lose_seed, win_seed) = order_by_hash(b"min-token-rows-1", b"min-token-rows-2");
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&target, &extra],
        vec![part_with_rows(0, 1, N)],
        now,
        win_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        Signal::Logs,
        &[&target],
        vec![part_with_rows(0, 2, N)],
        now,
        lose_seed,
    )
    .await;

    let snapshot = catalog
        .resolve(&tenant(), Signal::Logs, range, &[token], now)
        .await
        .expect("resolve");

    assert_eq!(
        served_rows(&snapshot),
        N,
        "exactly the winner's N rows, not 2N from also serving the loser's part"
    );
    assert_eq!(snapshot.segments.len(), 1);
}
