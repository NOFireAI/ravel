//! The token step never adds a level-0 input beside the record that replaced
//! it (issue #4418).
//!
//! A write acked with token T lands as L0 object L. A compaction record C
//! names L and publishes part P; until the sweep deletes L's commit record, a
//! resolve with T finds that record present. Logs and spans have no
//! query-time dedup, so a snapshot holding both P and L counts L's records
//! twice. The step now LISTs the token's bucket when the snapshot already
//! holds a level-1 part of it (or an erasure is pending) and skips L when a
//! record naming L has an output part in the snapshot, or when a live rewrite
//! names L.
//!
//! Rows are counted as the sum of `sample_count` over the snapshot: a logs
//! or spans compaction concatenates its inputs, so a part carries the sum of
//! its inputs' rows, and a snapshot that holds a part and one of its inputs
//! sums to more than the rows written. Read counts are per resolve, on a
//! fresh catalog, and asserted exactly.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, Snapshot,
};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{erasure, keys, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord,
    ErasurePredicateMatcher, ErasureRequest, RewriteDrop, RewriteRecord,
};
use ravel_segment::VERSION_V7;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{CommitToken, Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const HOUR: u32 = 500_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// Rows written by the token's flush L and by its sibling M.
const L_ROWS: u64 = 3;
const M_ROWS: u64 = 4;

fn tenant() -> TenantHash {
    TenantHash([0xab; 16])
}

fn catalog(store: &Arc<MemoryStore>) -> Catalog {
    Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count: 1,
            ..Default::default()
        },
    )
    .expect("catalog")
}

fn hour_start(hour: u32) -> i64 {
    i64::from(hour) * NS_PER_HOUR
}

/// A query over `HOUR`, before it seals: the bucket is listed.
fn live_window() -> (TimeRange, i64) {
    let start = hour_start(HOUR);
    (
        TimeRange {
            start_ns: start,
            end_ns: start + NS_PER_HOUR,
        },
        start + 30 * 60_000_000_000,
    )
}

/// A query over `HOUR` at the `now` that seals it: after a fold, folded
/// HEAD serves the hour.
fn sealed_window() -> (TimeRange, i64) {
    let start = hour_start(HOUR);
    (
        TimeRange {
            start_ns: start,
            end_ns: start + NS_PER_HOUR,
        },
        i64::from(HOUR + 1) * NS_PER_HOUR + MARGIN_NS + 1,
    )
}

/// An L0 commit record in `hour` whose events start at `event_start`.
fn l0(signal: Signal, seq: u64, hour: u32, event_start: i64, rows: u64) -> CommitRecord {
    record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal,
        shard: 0,
        writer_id: Uuid::from_u128(0x4418_0000 + u128::from(seq)),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: 100,
        content_hash: [seq as u8 ^ 0x5a; 32],
        sample_count: rows,
        series_count: 1,
        min_event_ts_ns: event_start,
        max_event_ts_ns: event_start + 100,
        min_ingest_ts_ns: hour_start(hour),
        max_ingest_ts_ns: hour_start(hour) + 100,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: hour_start(hour) + 1_000,
        ingest_hour_bucket: hour,
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

fn identity(record: &CommitRecord) -> CompactionInputIdentity {
    CompactionInputIdentity {
        writer_id: record.writer_id.clone(),
        writer_epoch: record.writer_epoch,
        writer_seq: record.writer_seq,
    }
}

/// One part inside `HOUR` carrying `rows` rows; `seed` makes its key distinct.
fn part(seed: u8, rows: u64) -> CompactionPart {
    let start = hour_start(HOUR);
    CompactionPart {
        part_index: 0,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xff; 16],
        content_hash: vec![seed; 32],
        object_size: 4096,
        sample_count: rows,
        series_count: 2,
        run_count: 1,
        min_event_ts_ns: start,
        max_event_ts_ns: start + 100,
        segment_format_version: u32::from(VERSION_V7),
        declared_column_stats: Vec::new(),
    }
}

/// A version 1 compaction record over `inputs` with `input_set_hash`
/// `hash_byte` repeated (version 1 decoding does not recompute it).
fn compaction(
    signal: Signal,
    inputs: &[&CommitRecord],
    hash_byte: u8,
    seed: u8,
    rows: u64,
) -> CompactionRecord {
    CompactionRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        level: 1,
        inputs: inputs.iter().map(|r| identity(r)).collect(),
        input_set_hash: vec![hash_byte; 32],
        parts: vec![part(seed, rows)],
        created_unix_ns: hour_start(HOUR) + 2_000,
        superseded_record_key: String::new(),
    }
}

/// A version 2 record re-encoding `predecessor`, at `predecessor_key`.
fn version_2(predecessor: &CompactionRecord, predecessor_key: &str, seed: u8) -> CompactionRecord {
    CompactionRecord {
        format_version: 2,
        input_set_hash: erasure::compute_superseding_compaction_input_set_hash(
            &predecessor.inputs,
            predecessor_key,
        )
        .to_vec(),
        parts: vec![part(seed, predecessor.parts[0].sample_count)],
        created_unix_ns: predecessor.created_unix_ns + 1_000,
        superseded_record_key: predecessor_key.to_string(),
        ..predecessor.clone()
    }
}

async fn put_compaction(store: &dyn ObjectStoreBackend, record: &CompactionRecord) -> String {
    let key = keys::compaction_record_key_for(record).expect("record key");
    store
        .put(
            &key,
            record::encode_compaction(record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put compaction record");
    key
}

/// A live rewrite over raw L0 `inputs` applying `request_id`, with one part
/// of `rows` rows whose events start at `event_start`.
async fn put_rewrite(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    inputs: &[&CommitRecord],
    request_id: Uuid,
    event_start: i64,
    rows: u64,
) -> RewriteRecord {
    let request_id = request_id.to_string();
    let input_ids: Vec<CompactionInputIdentity> = inputs.iter().map(|r| identity(r)).collect();
    let mut rewrite_part = part(0xe0, rows);
    rewrite_part.min_event_ts_ns = event_start;
    rewrite_part.max_event_ts_ns = event_start + 100;
    let record = RewriteRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &input_ids,
            None,
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        inputs: input_ids,
        parts: vec![rewrite_part],
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 1,
        }],
        created_unix_ns: hour_start(HOUR) + 5_000,
        superseded_record_key: String::new(),
    };
    let key = keys::rewrite_record_key_for(&record).expect("rewrite key");
    store
        .put(
            &key,
            erasure::encode_rewrite(&record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put rewrite record");
    record
}

/// The pending `.dreq` the rewrite applied: it outlives the rewrite's inputs.
async fn put_dreq(store: &dyn ObjectStoreBackend, signal: Signal, request_id: Uuid) {
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: hour_start(HOUR) + 4_000,
        predicate: vec![ErasurePredicateMatcher {
            key: "user".to_string(),
            value: "u-4418".to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    let key = keys::erasure_request_key(&tenant(), signal, request_id).expect("dreq key");
    store
        .put(
            &key,
            erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put dreq");
}

fn token(record: &CommitRecord) -> CommitToken {
    record::token_for(record).expect("token")
}

fn l0_key(record: &CommitRecord) -> String {
    keys::reconstruct_data_key(record).expect("data key")
}

fn part_key(record: &CompactionRecord) -> String {
    keys::reconstruct_l1_part_key(record, &record.parts[0]).expect("part key")
}

fn rewrite_part_key(record: &RewriteRecord) -> String {
    keys::reconstruct_rewrite_part_key(record, &record.parts[0]).expect("rewrite part key")
}

fn sorted_keys(snapshot: &Snapshot) -> Vec<String> {
    let mut out: Vec<String> = snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect();
    out.sort();
    out
}

fn rows(snapshot: &Snapshot) -> u64 {
    snapshot.segments.iter().map(|s| s.sample_count).sum()
}

/// Requests one resolve issued, split by operation.
#[derive(Debug, PartialEq, Eq)]
struct Reads {
    lists: u64,
    gets: u64,
    total: u64,
}

/// Resolve on a fresh catalog (cold caches, as on a query node that has not
/// seen the bucket) and count its store requests.
async fn resolve_counted(
    store: &Arc<MemoryStore>,
    signal: Signal,
    window: (TimeRange, i64),
    tokens: &[CommitToken],
) -> (Snapshot, Reads) {
    let accounting = QueryAccounting::new();
    let snapshot = catalog(store)
        .resolve_with_accounting(&tenant(), signal, window.0, tokens, window.1, &accounting)
        .await
        .expect("resolve");
    let counts = accounting.snapshot();
    let reads = Reads {
        lists: counts.s3_requests(AccountedOp::List),
        gets: counts.s3_requests(AccountedOp::Get),
        total: counts.total_s3_requests(),
    };
    (snapshot, reads)
}

/// The issue's bucket: L (the token's flush) and M, both still present, and C
/// over both with part P carrying their rows.
async fn compacted_bucket(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
) -> (CommitRecord, CompactionRecord) {
    let start = hour_start(HOUR);
    let l = l0(signal, 1, HOUR, start, L_ROWS);
    let m = l0(signal, 2, HOUR, start, M_ROWS);
    put_l0(store, &l).await;
    put_l0(store, &m).await;
    let c = compaction(signal, &[&l, &m], 0x11, 0xc1, L_ROWS + M_ROWS);
    put_compaction(store, &c).await;
    (l, c)
}

async fn listed_token_after_compaction_serves_the_part_once(signal: Signal) {
    let store = Arc::new(MemoryStore::new());
    let (l, c) = compacted_bucket(store.as_ref(), signal).await;

    let (plain, plain_reads) = resolve_counted(&store, signal, live_window(), &[]).await;
    assert_eq!(sorted_keys(&plain), vec![part_key(&c)]);

    let (snapshot, reads) = resolve_counted(&store, signal, live_window(), &[token(&l)]).await;
    assert_eq!(
        sorted_keys(&snapshot),
        vec![part_key(&c)],
        "the token's rows come from P alone, never from L beside it"
    );
    assert_eq!(rows(&snapshot), L_ROWS + M_ROWS, "each record counted once");
    assert!(
        !snapshot
            .segments
            .iter()
            .any(|s| s.data_object_key == l0_key(&l)),
        "L is not in the snapshot"
    );
    // The listing cached L's commit record and C, so the token adds only the
    // one LIST of its bucket.
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists + 1,
            gets: plain_reads.gets,
            total: plain_reads.total + 1,
        }
    );
}

#[tokio::test]
async fn logs_listed_token_after_compaction_serves_the_part_once() {
    listed_token_after_compaction_serves_the_part_once(Signal::Logs).await;
}

#[tokio::test]
async fn spans_listed_token_after_compaction_serves_the_part_once() {
    listed_token_after_compaction_serves_the_part_once(Signal::Spans).await;
}

async fn folded_token_after_compaction_serves_the_part_once(signal: Signal) {
    let store = Arc::new(MemoryStore::new());
    let (l, c) = compacted_bucket(store.as_ref(), signal).await;
    let report = catalog(&store)
        .fold(&tenant(), signal, Uuid::new_v4(), sealed_window().1, &[], None)
        .await
        .expect("fold");
    assert_eq!(report.watermark_hour, Some(HOUR));

    let (plain, plain_reads) = resolve_counted(&store, signal, sealed_window(), &[]).await;
    assert_eq!(sorted_keys(&plain), vec![part_key(&c)]);

    let (snapshot, reads) = resolve_counted(&store, signal, sealed_window(), &[token(&l)]).await;
    assert_eq!(
        sorted_keys(&snapshot),
        vec![part_key(&c)],
        "folded HEAD serves P; the token must not add L beside it"
    );
    assert_eq!(rows(&snapshot), L_ROWS + M_ROWS, "each record counted once");
    // The commit-record GET, one LIST of the bucket and one GET of C, which
    // no listing loaded on this path.
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists + 1,
            gets: plain_reads.gets + 2,
            total: plain_reads.total + 3,
        }
    );
}

#[tokio::test]
async fn logs_folded_token_after_compaction_serves_the_part_once() {
    folded_token_after_compaction_serves_the_part_once(Signal::Logs).await;
}

#[tokio::test]
async fn spans_folded_token_after_compaction_serves_the_part_once() {
    folded_token_after_compaction_serves_the_part_once(Signal::Spans).await;
}

/// Metrics dedup on (series_id, ts) at query time, so the step is unchanged
/// there: L is still added beside P, from the commit record the listing
/// cached, at no extra read.
#[tokio::test]
async fn metrics_token_after_compaction_is_unchanged() {
    let store = Arc::new(MemoryStore::new());
    let (l, c) = compacted_bucket(store.as_ref(), Signal::Metrics).await;

    let (_, plain_reads) = resolve_counted(&store, Signal::Metrics, live_window(), &[]).await;
    let (snapshot, reads) =
        resolve_counted(&store, Signal::Metrics, live_window(), &[token(&l)]).await;
    let mut expected = vec![part_key(&c), l0_key(&l)];
    expected.sort();
    assert_eq!(sorted_keys(&snapshot), expected);
    assert_eq!(reads, plain_reads);
}

/// An uncompacted token in a bucket the snapshot holds no level-1 part of,
/// with no erasure pending: the step adds it without listing, as before.
async fn uncompacted_token_newer_than_head_is_added(signal: Signal) {
    let store = Arc::new(MemoryStore::new());
    let (_, c) = compacted_bucket(store.as_ref(), signal).await;
    let report = catalog(&store)
        .fold(&tenant(), signal, Uuid::new_v4(), sealed_window().1, &[], None)
        .await
        .expect("fold");
    assert_eq!(report.watermark_hour, Some(HOUR));

    // A flush acked after the fold into the next hour, with event time
    // outside the query range so the listing caches its record but leaves
    // it out, and only the token adds it.
    let n = l0(signal, 3, HOUR + 1, hour_start(HOUR + 3), 5);
    put_l0(store.as_ref(), &n).await;

    let (_, plain_reads) = resolve_counted(&store, signal, sealed_window(), &[]).await;
    let (snapshot, reads) = resolve_counted(&store, signal, sealed_window(), &[token(&n)]).await;
    let mut expected = vec![part_key(&c), l0_key(&n)];
    expected.sort();
    assert_eq!(sorted_keys(&snapshot), expected, "read-your-write holds");
    assert_eq!(reads, plain_reads);
}

#[tokio::test]
async fn logs_uncompacted_token_newer_than_head_is_added() {
    uncompacted_token_newer_than_head_is_added(Signal::Logs).await;
}

#[tokio::test]
async fn spans_uncompacted_token_newer_than_head_is_added() {
    uncompacted_token_newer_than_head_is_added(Signal::Spans).await;
}

/// An uncompacted token in a bucket whose compaction part the snapshot
/// holds: the bucket is listed, no record names the token, and it is added.
#[tokio::test]
async fn uncompacted_token_beside_a_compacted_sibling_is_added() {
    let store = Arc::new(MemoryStore::new());
    let (_, c) = compacted_bucket(store.as_ref(), Signal::Logs).await;
    // Same bucket, not named by C; its event time lies outside the query
    // range, so the listing leaves it out and only the token adds it.
    let n = l0(Signal::Logs, 3, HOUR, hour_start(HOUR + 3), 5);
    put_l0(store.as_ref(), &n).await;

    let (plain, plain_reads) = resolve_counted(&store, Signal::Logs, live_window(), &[]).await;
    assert_eq!(sorted_keys(&plain), vec![part_key(&c)]);
    let (snapshot, reads) =
        resolve_counted(&store, Signal::Logs, live_window(), &[token(&n)]).await;
    let mut expected = vec![part_key(&c), l0_key(&n)];
    expected.sort();
    assert_eq!(sorted_keys(&snapshot), expected, "read-your-write holds");
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists + 1,
            gets: plain_reads.gets,
            total: plain_reads.total + 1,
        }
    );
}

/// A live rewrite replaced L and its `.dreq` is pending. The token must not
/// bring L back beside the rewrite's part.
async fn token_after_rewrite_is_not_brought_back(signal: Signal) {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l = l0(signal, 1, HOUR, start, L_ROWS);
    put_l0(store.as_ref(), &l).await;
    let request_id = Uuid::from_u128(0xd4418);
    put_dreq(store.as_ref(), signal, request_id).await;
    let rw = put_rewrite(store.as_ref(), signal, &[&l], request_id, start, L_ROWS - 1).await;

    let (plain, plain_reads) = resolve_counted(&store, signal, live_window(), &[]).await;
    assert_eq!(sorted_keys(&plain), vec![rewrite_part_key(&rw)]);
    let (snapshot, reads) = resolve_counted(&store, signal, live_window(), &[token(&l)]).await;
    assert_eq!(
        sorted_keys(&snapshot),
        vec![rewrite_part_key(&rw)],
        "the rewrite's part serves L's surviving rows; L is not added back"
    );
    assert_eq!(rows(&snapshot), L_ROWS - 1);
    assert_eq!(snapshot.pending_erasure.len(), 1);
    // One LIST and one GET of the rewrite, which no cache holds.
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists + 1,
            gets: plain_reads.gets + 1,
            total: plain_reads.total + 2,
        }
    );
}

#[tokio::test]
async fn logs_token_after_rewrite_is_not_brought_back() {
    token_after_rewrite_is_not_brought_back(Signal::Logs).await;
}

#[tokio::test]
async fn spans_token_after_rewrite_is_not_brought_back() {
    token_after_rewrite_is_not_brought_back(Signal::Spans).await;
}

/// The rewrite's part lies outside the query range, so the snapshot holds no
/// level-1 part of the bucket; the pending `.dreq` alone sends the step to
/// the bucket, and the live rewrite keeps L out.
#[tokio::test]
async fn token_after_rewrite_out_of_range_is_not_brought_back() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    put_l0(store.as_ref(), &l).await;
    let request_id = Uuid::from_u128(0xd4418);
    put_dreq(store.as_ref(), Signal::Logs, request_id).await;
    put_rewrite(
        store.as_ref(),
        Signal::Logs,
        &[&l],
        request_id,
        start,
        L_ROWS - 1,
    )
    .await;
    let (_, now) = live_window();
    let range = TimeRange {
        start_ns: start + 1_000_000,
        end_ns: start + NS_PER_HOUR,
    };

    let (plain, plain_reads) = resolve_counted(&store, Signal::Logs, (range, now), &[]).await;
    assert!(plain.segments.is_empty());
    let (snapshot, reads) =
        resolve_counted(&store, Signal::Logs, (range, now), &[token(&l)]).await;
    assert!(
        snapshot.segments.is_empty(),
        "a live rewrite's input is never added back"
    );
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists + 1,
            gets: plain_reads.gets + 1,
            total: plain_reads.total + 2,
        }
    );
}

/// C1 over L and M, re-encoded by a version 2 C2. The listing serves C2's
/// part, and the token is decided by C2: C1, which lists first, names L too
/// but its part is not in the snapshot.
#[tokio::test]
async fn superseded_compaction_is_decided_by_the_authoritative_record() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    let m = l0(Signal::Logs, 2, HOUR, start, M_ROWS);
    put_l0(store.as_ref(), &l).await;
    put_l0(store.as_ref(), &m).await;
    let c1 = compaction(Signal::Logs, &[&l, &m], 0x00, 0xc1, L_ROWS + M_ROWS);
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    let c2 = version_2(&c1, &c1_key, 0xc2);
    let c2_key = put_compaction(store.as_ref(), &c2).await;
    assert!(c1_key < c2_key, "C1 lists first");

    let (snapshot, _) = resolve_counted(&store, Signal::Logs, live_window(), &[token(&l)]).await;
    assert_eq!(sorted_keys(&snapshot), vec![part_key(&c2)]);
    assert_eq!(rows(&snapshot), L_ROWS + M_ROWS, "each record counted once");
}

/// HEAD folded C1's part before C2 was written. The snapshot still holds C1's
/// part, and C1, superseded now, decides: its part already serves L.
#[tokio::test]
async fn superseded_compaction_part_in_a_stale_head_keeps_the_input_out() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    let m = l0(Signal::Logs, 2, HOUR, start, M_ROWS);
    put_l0(store.as_ref(), &l).await;
    put_l0(store.as_ref(), &m).await;
    let c1 = compaction(Signal::Logs, &[&l, &m], 0x00, 0xc1, L_ROWS + M_ROWS);
    let c1_key = put_compaction(store.as_ref(), &c1).await;
    catalog(&store)
        .fold(
            &tenant(),
            Signal::Logs,
            Uuid::new_v4(),
            sealed_window().1,
            &[],
            None,
        )
        .await
        .expect("fold");
    let c2 = version_2(&c1, &c1_key, 0xc2);
    put_compaction(store.as_ref(), &c2).await;

    let (plain, _) = resolve_counted(&store, Signal::Logs, sealed_window(), &[]).await;
    assert_eq!(sorted_keys(&plain), vec![part_key(&c1)], "HEAD is stale");
    let (snapshot, _) =
        resolve_counted(&store, Signal::Logs, sealed_window(), &[token(&l)]).await;
    assert_eq!(sorted_keys(&snapshot), vec![part_key(&c1)]);
    assert_eq!(rows(&snapshot), L_ROWS + M_ROWS, "each record counted once");
}
