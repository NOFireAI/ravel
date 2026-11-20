//! The token step never adds a level-0 input beside the record that replaced
//! it (issue #4418).
//!
//! A write acked with token T lands as L0 object L. A compaction record C
//! names L and publishes part P; until the sweep deletes L's commit record, a
//! resolve with T finds that record present. Logs and spans have no
//! query-time dedup, so a snapshot holding both P and L counts L's records
//! twice. The step now LISTs the token's bucket when the snapshot already
//! holds a level-1 part of it (or an erasure is pending) and leaves L out
//! when the bucket is tombstoned, when a record naming L has an output part
//! in the snapshot, or when a live rewrite names L and the snapshot holds
//! nothing else that rewrite replaced, whose parts it adds instead. Tokens
//! whose commit record is absent run first, so the answer does not depend
//! on token order.
//!
//! Rows are counted as the sum of `sample_count` over the snapshot: a logs
//! or spans compaction concatenates its inputs, so a part carries the sum of
//! its inputs' rows, and a snapshot that holds a part and one of its inputs
//! sums to more than the rows written. Read counts are per resolve, on a
//! fresh catalog, and asserted exactly.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use ravel_catalog::{
    Catalog, CatalogConfig, CatalogError, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_FOLD_SAFETY_MARGIN_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS, Snapshot,
};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{erasure, keys, signal};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreError};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord,
    ErasurePredicateMatcher, ErasureRequest, RetentionTombstone, RewriteDrop, RewriteRecord,
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
    catalog_on(store.clone(), 1)
}

fn catalog_on(store: Arc<dyn ObjectStoreBackend>, shard_count: u32) -> Catalog {
    Catalog::new(
        store,
        CatalogConfig {
            shard_count,
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
    l0_on(signal, 0, seq, hour, event_start, rows)
}

fn l0_on(
    signal: Signal,
    shard: u32,
    seq: u64,
    hour: u32,
    event_start: i64,
    rows: u64,
) -> CommitRecord {
    record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal,
        shard,
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
    resolve_counted_on(&catalog(store), signal, window, tokens).await
}

async fn resolve_counted_on(
    catalog: &Catalog,
    signal: Signal,
    window: (TimeRange, i64),
    tokens: &[CommitToken],
) -> (Snapshot, Reads) {
    let accounting = QueryAccounting::new();
    let snapshot = catalog
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
        .fold(
            &tenant(),
            signal,
            Uuid::new_v4(),
            sealed_window().1,
            &[],
            None,
        )
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
        .fold(
            &tenant(),
            signal,
            Uuid::new_v4(),
            sealed_window().1,
            &[],
            None,
        )
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

/// A query range that starts after every event the helpers write by
/// default, so `part()` and `l0(.., hour_start(HOUR), ..)` lie outside it.
fn late_window() -> (TimeRange, i64) {
    let start = hour_start(HOUR);
    (
        TimeRange {
            start_ns: start + 1_000_000,
            end_ns: start + NS_PER_HOUR,
        },
        live_window().1,
    )
}

/// The rewrite's part lies outside the query range, so the snapshot holds no
/// level-1 part of the bucket; the pending `.dreq` alone sends the step to
/// the bucket. The live rewrite keeps L out and its part is added in L's
/// place, the same answer the token gets once the sweep has deleted L's
/// commit record.
#[tokio::test]
async fn token_after_rewrite_out_of_range_is_served_by_the_rewrite() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    put_l0(store.as_ref(), &l).await;
    let request_id = Uuid::from_u128(0xd4418);
    put_dreq(store.as_ref(), Signal::Logs, request_id).await;
    let rw = put_rewrite(
        store.as_ref(),
        Signal::Logs,
        &[&l],
        request_id,
        start,
        L_ROWS - 1,
    )
    .await;

    let (plain, plain_reads) = resolve_counted(&store, Signal::Logs, late_window(), &[]).await;
    assert!(plain.segments.is_empty());
    let (snapshot, reads) =
        resolve_counted(&store, Signal::Logs, late_window(), &[token(&l)]).await;
    assert_eq!(
        sorted_keys(&snapshot),
        vec![rewrite_part_key(&rw)],
        "a live rewrite's input is never added back; its part serves the token"
    );
    assert_eq!(rows(&snapshot), L_ROWS - 1);
    // One LIST and one GET of the rewrite, which no cache holds.
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists + 1,
            gets: plain_reads.gets + 1,
            total: plain_reads.total + 2,
        }
    );

    store
        .delete(&keys::commit_key_for_record(&l).expect("commit key"))
        .await
        .expect("sweep L's commit record");
    let (_, swept_plain_reads) = resolve_counted(&store, Signal::Logs, late_window(), &[]).await;
    let (swept, swept_reads) =
        resolve_counted(&store, Signal::Logs, late_window(), &[token(&l)]).await;
    assert_eq!(sorted_keys(&swept), sorted_keys(&snapshot));
    // Two NotFound probes of L's commit record, then the fallback's LIST and
    // rewrite GET.
    assert_eq!(
        swept_reads,
        Reads {
            lists: swept_plain_reads.lists + 1,
            gets: swept_plain_reads.gets + 3,
            total: swept_plain_reads.total + 4,
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
    let (snapshot, _) = resolve_counted(&store, Signal::Logs, sealed_window(), &[token(&l)]).await;
    assert_eq!(sorted_keys(&snapshot), vec![part_key(&c1)]);
    assert_eq!(rows(&snapshot), L_ROWS + M_ROWS, "each record counted once");
}

async fn fold_hour(store: &Arc<MemoryStore>, signal: Signal) {
    let report = catalog(store)
        .fold(
            &tenant(),
            signal,
            Uuid::new_v4(),
            sealed_window().1,
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(report.watermark_hour, Some(HOUR));
}

async fn put_tombstone(store: &dyn ObjectStoreBackend, signal: Signal, shard: u32, hour: u32) {
    let tombstone = RetentionTombstone {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        shard,
        ingest_hour_bucket: hour,
        retired_at_ns: 0,
        retention_window_ns: 0,
        record_count_observed: 0,
    };
    let key = keys::retention_tombstone_key_for(&tombstone).expect("tombstone key");
    store
        .put(
            &key,
            record::encode_tombstone(&tombstone),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put tombstone");
}

/// A live rewrite that supersedes the record at `predecessor_key` as a
/// whole, applying `request_id`, with one part of `rows` rows.
async fn put_superseding_rewrite(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    predecessor_key: &str,
    request_id: Uuid,
    rows: u64,
) -> RewriteRecord {
    let request_id = request_id.to_string();
    let record = RewriteRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &[],
            Some(predecessor_key),
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        inputs: Vec::new(),
        parts: vec![part(0xe1, rows)],
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 1,
        }],
        created_unix_ns: hour_start(HOUR) + 9_000,
        superseded_record_key: predecessor_key.to_string(),
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

/// HEAD still names a bucket retention has tombstoned, and L's commit record
/// survives until the sweep deletes the bucket. A token whose bucket is
/// tombstoned is satisfied with zero segments (docs/deletion-and-gc.md), so
/// it adds nothing beside the part HEAD serves.
#[tokio::test]
async fn token_in_a_tombstoned_bucket_head_still_names_adds_nothing() {
    let store = Arc::new(MemoryStore::new());
    let (l, c) = compacted_bucket(store.as_ref(), Signal::Logs).await;
    fold_hour(&store, Signal::Logs).await;
    put_tombstone(store.as_ref(), Signal::Logs, 0, HOUR).await;

    let (plain, _) = resolve_counted(&store, Signal::Logs, sealed_window(), &[]).await;
    assert_eq!(
        sorted_keys(&plain),
        vec![part_key(&c)],
        "HEAD still names the bucket"
    );
    let (snapshot, _) = resolve_counted(&store, Signal::Logs, sealed_window(), &[token(&l)]).await;
    assert_eq!(
        sorted_keys(&snapshot),
        vec![part_key(&c)],
        "L is not added beside P"
    );
    assert_eq!(rows(&snapshot), L_ROWS + M_ROWS, "each record counted once");
}

/// Two tokens of one bucket: T1's commit record is swept, T2's is not. C
/// names both inputs and its part lies outside the query range, so only
/// T1's fallback adds it. In either token order the set holds C's part
/// once and never T2's input beside it.
#[tokio::test]
async fn swept_and_present_tokens_of_one_bucket_answer_alike_in_either_order() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l1 = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    let l2 = l0(Signal::Logs, 2, HOUR, start, M_ROWS);
    put_l0(store.as_ref(), &l1).await;
    put_l0(store.as_ref(), &l2).await;
    let c = compaction(Signal::Logs, &[&l1, &l2], 0x11, 0xc1, L_ROWS + M_ROWS);
    put_compaction(store.as_ref(), &c).await;
    store
        .delete(&keys::commit_key_for_record(&l1).expect("commit key"))
        .await
        .expect("sweep L1's commit record");

    let (plain, _) = resolve_counted(&store, Signal::Logs, late_window(), &[]).await;
    assert!(plain.segments.is_empty());
    for (order, tokens) in [
        ("present first", [token(&l2), token(&l1)]),
        ("swept first", [token(&l1), token(&l2)]),
    ] {
        let (snapshot, _) = resolve_counted(&store, Signal::Logs, late_window(), &tokens).await;
        assert_eq!(sorted_keys(&snapshot), vec![part_key(&c)], "{order}");
        assert_eq!(rows(&snapshot), L_ROWS + M_ROWS, "{order}: L2 counted once");
    }
}

/// C names L, but C's part lies outside the query range; a sibling record
/// over M has its part in range, so the step lists the bucket. A record
/// naming L keeps L out only when one of that record's own parts is in the
/// set, so L is added and read-your-write holds.
#[tokio::test]
async fn token_named_by_a_record_whose_part_is_not_in_the_set_is_added() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    let m = l0(Signal::Logs, 2, HOUR, start, M_ROWS);
    put_l0(store.as_ref(), &l).await;
    put_l0(store.as_ref(), &m).await;
    let c = compaction(Signal::Logs, &[&l], 0x11, 0xc1, L_ROWS);
    put_compaction(store.as_ref(), &c).await;
    let mut sibling = compaction(Signal::Logs, &[&m], 0x22, 0xc2, M_ROWS);
    sibling.parts[0].min_event_ts_ns = start + 2_000_000;
    sibling.parts[0].max_event_ts_ns = start + 2_000_100;
    put_compaction(store.as_ref(), &sibling).await;

    let (plain, plain_reads) = resolve_counted(&store, Signal::Logs, late_window(), &[]).await;
    assert_eq!(sorted_keys(&plain), vec![part_key(&sibling)]);
    let (snapshot, reads) =
        resolve_counted(&store, Signal::Logs, late_window(), &[token(&l)]).await;
    let mut expected = vec![part_key(&sibling), l0_key(&l)];
    expected.sort();
    assert_eq!(sorted_keys(&snapshot), expected, "read-your-write holds");
    assert_eq!(rows(&snapshot), L_ROWS + M_ROWS);
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists + 1,
            gets: plain_reads.gets,
            total: plain_reads.total + 1,
        }
    );
}

/// HEAD folded W1's part; a later rewrite W2 superseded W1 for a second
/// erasure request. W1's part already serves L's rows, so the token adds
/// neither L nor W2's part beside it.
#[tokio::test]
async fn superseded_rewrite_part_in_a_stale_head_keeps_the_input_out() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    put_l0(store.as_ref(), &l).await;
    let first = Uuid::from_u128(0xd4418);
    put_dreq(store.as_ref(), Signal::Logs, first).await;
    let w1 = put_rewrite(
        store.as_ref(),
        Signal::Logs,
        &[&l],
        first,
        start,
        L_ROWS - 1,
    )
    .await;
    fold_hour(&store, Signal::Logs).await;
    let second = Uuid::from_u128(0xd4419);
    put_dreq(store.as_ref(), Signal::Logs, second).await;
    let w1_key = keys::rewrite_record_key_for(&w1).expect("rewrite key");
    put_superseding_rewrite(store.as_ref(), Signal::Logs, &w1_key, second, L_ROWS - 2).await;

    let (plain, _) = resolve_counted(&store, Signal::Logs, sealed_window(), &[]).await;
    assert_eq!(
        sorted_keys(&plain),
        vec![rewrite_part_key(&w1)],
        "HEAD is stale"
    );
    let (snapshot, _) = resolve_counted(&store, Signal::Logs, sealed_window(), &[token(&l)]).await;
    assert_eq!(sorted_keys(&snapshot), vec![rewrite_part_key(&w1)]);
    assert_eq!(rows(&snapshot), L_ROWS - 1);
}

/// The issue's write 9: one compacted token on each of two shards. Each
/// token lists its own bucket once and adds nothing beside the parts.
#[tokio::test]
async fn tokens_on_two_shards_each_serve_their_part_once() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let mut parts = Vec::new();
    let mut tokens = Vec::new();
    for shard in 0..2u32 {
        let seq = 10 * u64::from(shard);
        let l = l0_on(Signal::Logs, shard, seq + 1, HOUR, start, L_ROWS);
        let m = l0_on(Signal::Logs, shard, seq + 2, HOUR, start, M_ROWS);
        put_l0(store.as_ref(), &l).await;
        put_l0(store.as_ref(), &m).await;
        let c = CompactionRecord {
            shard,
            ..compaction(
                Signal::Logs,
                &[&l, &m],
                0x11,
                0xc1 + shard as u8,
                L_ROWS + M_ROWS,
            )
        };
        put_compaction(store.as_ref(), &c).await;
        parts.push(part_key(&c));
        tokens.push(token(&l));
    }
    parts.sort();

    let (plain, plain_reads) = resolve_counted_on(
        &catalog_on(store.clone(), 2),
        Signal::Logs,
        live_window(),
        &[],
    )
    .await;
    assert_eq!(sorted_keys(&plain), parts);
    let (snapshot, reads) = resolve_counted_on(
        &catalog_on(store.clone(), 2),
        Signal::Logs,
        live_window(),
        &tokens,
    )
    .await;
    assert_eq!(sorted_keys(&snapshot), parts);
    assert_eq!(rows(&snapshot), 2 * (L_ROWS + M_ROWS));
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists + 2,
            gets: plain_reads.gets,
            total: plain_reads.total + 2,
        }
    );
}

/// Both of C's inputs carry a token. The check runs per token, so the
/// bucket is listed once for each.
#[tokio::test]
async fn two_tokens_of_one_bucket_list_it_once_each() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    let m = l0(Signal::Logs, 2, HOUR, start, M_ROWS);
    put_l0(store.as_ref(), &l).await;
    put_l0(store.as_ref(), &m).await;
    let c = compaction(Signal::Logs, &[&l, &m], 0x11, 0xc1, L_ROWS + M_ROWS);
    put_compaction(store.as_ref(), &c).await;

    let (_, plain_reads) = resolve_counted(&store, Signal::Logs, live_window(), &[]).await;
    let (snapshot, reads) =
        resolve_counted(&store, Signal::Logs, live_window(), &[token(&l), token(&m)]).await;
    assert_eq!(sorted_keys(&snapshot), vec![part_key(&c)]);
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists + 2,
            gets: plain_reads.gets,
            total: plain_reads.total + 2,
        }
    );
}

/// The issue's bucket folded into HEAD, read through a FaultStore whose rule
/// (built from the bucket prefix and C's key) matches only a read of the
/// token step: the resolve without the token passes and fires nothing, the
/// resolve with it fails with the returned error, never by leaving L out or
/// adding it.
async fn token_step_error(
    rule: impl FnOnce(&str, &str) -> Rule,
    op: Op,
    kind: FaultKind,
) -> CatalogError {
    let inner = Arc::new(MemoryStore::new());
    let (l, c) = compacted_bucket(inner.as_ref(), Signal::Logs).await;
    fold_hour(&inner, Signal::Logs).await;
    let prefix = keys::commit_shard_hour_prefix(&tenant(), Signal::Logs, 0, HOUR).expect("prefix");
    let ckey = keys::compaction_record_key_for(&c).expect("record key");
    let store = Arc::new(FaultStore::new(
        inner,
        FaultPlan::empty().with_rule(rule(&prefix, &ckey)),
    ));
    let catalog = catalog_on(store.clone(), 1);
    let (range, now) = sealed_window();

    let plain = catalog
        .resolve(&tenant(), Signal::Logs, range, &[], now)
        .await
        .expect("resolve without the token");
    assert_eq!(sorted_keys(&plain), vec![part_key(&c)]);
    assert_eq!(
        store.fault_count(op, kind),
        0,
        "only the token step reads it"
    );
    let err = catalog
        .resolve(&tenant(), Signal::Logs, range, &[token(&l)], now)
        .await
        .expect_err("the token step's failed read fails the resolve");
    assert_eq!(store.fault_count(op, kind), 1);
    err
}

#[tokio::test]
async fn token_step_bucket_list_error_fails_the_resolve() {
    let err = token_step_error(
        |prefix, _| {
            Rule::new(Op::List, ScriptedFault::Permanent("list".into())).with_key_contains(prefix)
        },
        Op::List,
        FaultKind::Permanent,
    )
    .await;
    assert!(
        matches!(err, CatalogError::Store(StoreError::Permanent(_))),
        "{err:?}"
    );
}

#[tokio::test]
async fn token_step_compaction_get_error_fails_the_resolve() {
    let err = token_step_error(
        |_, ckey| {
            Rule::new(Op::Get, ScriptedFault::Permanent("get".into())).with_key_contains(ckey)
        },
        Op::Get,
        FaultKind::Permanent,
    )
    .await;
    assert!(
        matches!(err, CatalogError::Store(StoreError::Permanent(_))),
        "{err:?}"
    );
}

/// C is gone between the bucket LIST and its GET.
#[tokio::test]
async fn token_step_compaction_record_gone_after_list_fails_the_resolve() {
    let err = token_step_error(
        |_, ckey| Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_key_contains(ckey),
        Op::Get,
        FaultKind::NotFoundBlip,
    )
    .await;
    assert!(
        matches!(err, CatalogError::Store(StoreError::NotFound)),
        "{err:?}"
    );
}

/// A live rewrite's GET fails on the token step: the resolve fails rather
/// than adding L or leaving it out.
#[tokio::test]
async fn token_step_rewrite_get_error_fails_the_resolve() {
    let inner = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    put_l0(inner.as_ref(), &l).await;
    let request_id = Uuid::from_u128(0xd4418);
    put_dreq(inner.as_ref(), Signal::Logs, request_id).await;
    let rw = put_rewrite(
        inner.as_ref(),
        Signal::Logs,
        &[&l],
        request_id,
        start,
        L_ROWS - 1,
    )
    .await;
    fold_hour(&inner, Signal::Logs).await;
    let rkey = keys::rewrite_record_key_for(&rw).expect("rewrite key");
    let store = Arc::new(FaultStore::new(
        inner,
        FaultPlan::empty().with_rule(
            Rule::new(Op::Get, ScriptedFault::Permanent("get".into())).with_key_contains(rkey),
        ),
    ));
    let catalog = catalog_on(store.clone(), 1);
    let (range, now) = sealed_window();

    let plain = catalog
        .resolve(&tenant(), Signal::Logs, range, &[], now)
        .await
        .expect("resolve without the token");
    assert_eq!(sorted_keys(&plain), vec![rewrite_part_key(&rw)]);
    assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 0);
    let err = catalog
        .resolve(&tenant(), Signal::Logs, range, &[token(&l)], now)
        .await
        .expect_err("the token step's failed read fails the resolve");
    assert_eq!(store.fault_count(Op::Get, FaultKind::Permanent), 1);
    assert!(
        matches!(err, CatalogError::Store(StoreError::Permanent(_))),
        "{err:?}"
    );
}

/// While an erasure is pending, every present token whose input is not in
/// the set pays the bucket LIST and a GET per rewrite record, even in a
/// bucket nothing compacted and with inputs no rewrite names; nothing is
/// shared between the two tokens.
#[tokio::test]
async fn pending_erasure_charges_each_token_a_list_and_the_rewrite_gets() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    let m = l0(Signal::Logs, 2, HOUR, start, M_ROWS);
    let n = l0(Signal::Logs, 3, HOUR, start, 5);
    for record in [&l, &m, &n] {
        put_l0(store.as_ref(), record).await;
    }
    let request_id = Uuid::from_u128(0xd4418);
    put_dreq(store.as_ref(), Signal::Logs, request_id).await;
    put_rewrite(
        store.as_ref(),
        Signal::Logs,
        &[&m],
        request_id,
        start,
        M_ROWS - 1,
    )
    .await;

    let (plain, plain_reads) = resolve_counted(&store, Signal::Logs, late_window(), &[]).await;
    assert!(plain.segments.is_empty());
    let (snapshot, reads) =
        resolve_counted(&store, Signal::Logs, late_window(), &[token(&l), token(&n)]).await;
    let mut expected = vec![l0_key(&l), l0_key(&n)];
    expected.sort();
    assert_eq!(sorted_keys(&snapshot), expected, "read-your-write holds");
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists + 2,
            gets: plain_reads.gets + 2,
            total: plain_reads.total + 4,
        }
    );
}

/// HEAD folded the bucket before an erasure rewrite R landed over M's object
/// and P's, so HEAD still names an input R replaced and the `.dreq` stays
/// pending. The query covers M's events but not P's and carries P's token:
/// the set holds `held` (M's raw object, or the part of a compaction of M)
/// but not P's input. R's part also carries M's surviving rows, so it is not
/// added beside `held`; P's own input is, and the pending erasure filters
/// its erased rows at scan time.
async fn token_beside_an_input_its_live_rewrite_replaced_adds_its_own_input(compacted_m: bool) {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let p = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    let m = l0(Signal::Logs, 2, HOUR, start + 2_000_000, M_ROWS);
    put_l0(store.as_ref(), &p).await;
    put_l0(store.as_ref(), &m).await;
    let held = if compacted_m {
        let mut c = compaction(Signal::Logs, &[&m], 0x11, 0xc1, M_ROWS);
        c.parts[0].min_event_ts_ns = m.min_event_ts_ns;
        c.parts[0].max_event_ts_ns = m.max_event_ts_ns;
        put_compaction(store.as_ref(), &c).await;
        part_key(&c)
    } else {
        l0_key(&m)
    };
    fold_hour(&store, Signal::Logs).await;
    let request_id = Uuid::from_u128(0xd4418);
    put_dreq(store.as_ref(), Signal::Logs, request_id).await;
    let rw = put_rewrite(
        store.as_ref(),
        Signal::Logs,
        &[&p, &m],
        request_id,
        start,
        L_ROWS + M_ROWS - 1,
    )
    .await;

    let window = (late_window().0, sealed_window().1);
    let (plain, plain_reads) = resolve_counted(&store, Signal::Logs, window, &[]).await;
    assert_eq!(sorted_keys(&plain), vec![held.clone()], "HEAD is stale");
    let (snapshot, reads) = resolve_counted(&store, Signal::Logs, window, &[token(&p)]).await;
    let mut expected = vec![held, l0_key(&p)];
    expected.sort();
    assert_eq!(
        sorted_keys(&snapshot),
        expected,
        "R's part would serve M's rows a second time beside the held object"
    );
    assert!(!sorted_keys(&snapshot).contains(&rewrite_part_key(&rw)));
    assert_eq!(rows(&snapshot), L_ROWS + M_ROWS, "each record counted once");
    assert_eq!(
        snapshot.pending_erasure.len(),
        1,
        "P's erased rows filtered"
    );
    // The commit-record GET, one LIST of the bucket, one GET of the rewrite,
    // which no cache holds, and one of the compaction, if any, which no
    // listing loaded on this path.
    let compaction_gets = u64::from(compacted_m);
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists + 1,
            gets: plain_reads.gets + 2 + compaction_gets,
            total: plain_reads.total + 3 + compaction_gets,
        }
    );
}

#[tokio::test]
async fn token_beside_a_raw_input_its_live_rewrite_replaced_adds_its_own_input() {
    token_beside_an_input_its_live_rewrite_replaced_adds_its_own_input(false).await;
}

#[tokio::test]
async fn token_beside_a_compaction_its_live_rewrite_replaced_adds_its_own_input() {
    token_beside_an_input_its_live_rewrite_replaced_adds_its_own_input(true).await;
}

/// Two live rewrites, neither superseding the other, both name L. A present
/// token checks the siblings and picks the rewrite exactly as a swept one
/// does: the alarm is raised and the first rewrite in listing order serves L.
#[tokio::test]
async fn present_token_raises_the_sibling_alarm_and_serves_the_first_live_rewrite() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let l = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    put_l0(store.as_ref(), &l).await;
    fold_hour(&store, Signal::Logs).await;
    let mut siblings = Vec::new();
    for request in [0xd4418u128, 0xd4419] {
        let request_id = Uuid::from_u128(request);
        put_dreq(store.as_ref(), Signal::Logs, request_id).await;
        let rw = put_rewrite(
            store.as_ref(),
            Signal::Logs,
            &[&l],
            request_id,
            start,
            L_ROWS - 1,
        )
        .await;
        siblings.push((keys::rewrite_record_key_for(&rw).expect("key"), rw));
    }
    siblings.sort_by(|a, b| a.0.cmp(&b.0));

    let window = (late_window().0, sealed_window().1);
    let catalog = catalog(&store);
    let (plain, _) = resolve_counted_on(&catalog, Signal::Logs, window, &[]).await;
    assert!(plain.segments.is_empty());
    assert_eq!(
        catalog.rewrite_sibling_conflicts(),
        0,
        "HEAD serves the hour"
    );
    let (snapshot, _) = resolve_counted_on(&catalog, Signal::Logs, window, &[token(&l)]).await;
    assert_eq!(catalog.rewrite_sibling_conflicts(), 1);
    assert_eq!(
        sorted_keys(&snapshot),
        vec![rewrite_part_key(&siblings[0].1)]
    );

    store
        .delete(&keys::commit_key_for_record(&l).expect("commit key"))
        .await
        .expect("sweep L's commit record");
    let swept_catalog = catalog_on(store.clone(), 1);
    let (swept, _) = resolve_counted_on(&swept_catalog, Signal::Logs, window, &[token(&l)]).await;
    assert_eq!(swept_catalog.rewrite_sibling_conflicts(), 1);
    assert_eq!(sorted_keys(&swept), sorted_keys(&snapshot));
}

/// The bucket is tombstoned, but the set holds no level-1 part of it and no
/// erasure is pending, so the step does not check the bucket: L is added as
/// before, at the commit-record GET alone, and is the only object serving
/// its rows.
#[tokio::test]
async fn token_in_a_tombstoned_bucket_without_the_check_is_added() {
    let store = Arc::new(MemoryStore::new());
    let l = l0(Signal::Logs, 1, HOUR, hour_start(HOUR), L_ROWS);
    put_l0(store.as_ref(), &l).await;
    fold_hour(&store, Signal::Logs).await;
    put_tombstone(store.as_ref(), Signal::Logs, 0, HOUR).await;

    let window = (late_window().0, sealed_window().1);
    let (plain, plain_reads) = resolve_counted(&store, Signal::Logs, window, &[]).await;
    assert!(plain.segments.is_empty());
    let (snapshot, reads) = resolve_counted(&store, Signal::Logs, window, &[token(&l)]).await;
    assert_eq!(sorted_keys(&snapshot), vec![l0_key(&l)]);
    assert_eq!(rows(&snapshot), L_ROWS);
    assert_eq!(
        reads,
        Reads {
            lists: plain_reads.lists,
            gets: plain_reads.gets + 1,
            total: plain_reads.total + 1,
        }
    );
}

/// The swept-record twin of
/// `token_beside_an_input_its_live_rewrite_replaced_adds_its_own_input`
/// (issue #4428). HEAD folded before rewrite R landed, so HEAD still names
/// `held` (M's raw object, or a compaction part of M), an input R replaced.
/// The test then deletes P's commit record and data object directly. The
/// fold also named P, and the superseded-input sweep holds an object HEAD
/// names, so whether a real sweep can reach this state is open (#4428); the
/// test pins what the resolver does if it is reached. The token's rows now
/// live only in R's part, which also carries M's surviving rows, so a
/// snapshot that adds R's part beside `held` serves M's rows twice.
///
/// A correct answer serves every surviving row exactly once: R's part
/// without `held`, or a refusal. It never holds both.
async fn swept_token_beside_an_input_its_live_rewrite_replaced(compacted_m: bool) {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let p = l0(Signal::Logs, 1, HOUR, start, L_ROWS);
    let m = l0(Signal::Logs, 2, HOUR, start + 2_000_000, M_ROWS);
    put_l0(store.as_ref(), &p).await;
    put_l0(store.as_ref(), &m).await;
    let held = if compacted_m {
        let mut c = compaction(Signal::Logs, &[&m], 0x11, 0xc1, M_ROWS);
        c.parts[0].min_event_ts_ns = m.min_event_ts_ns;
        c.parts[0].max_event_ts_ns = m.max_event_ts_ns;
        put_compaction(store.as_ref(), &c).await;
        part_key(&c)
    } else {
        l0_key(&m)
    };
    fold_hour(&store, Signal::Logs).await;
    let request_id = Uuid::from_u128(0xd4428);
    put_dreq(store.as_ref(), Signal::Logs, request_id).await;
    let rw = put_rewrite(
        store.as_ref(),
        Signal::Logs,
        &[&p, &m],
        request_id,
        start,
        L_ROWS + M_ROWS - 1,
    )
    .await;
    store
        .delete(&keys::commit_key_for_record(&p).expect("commit key"))
        .await
        .expect("delete P's commit record");
    store
        .delete(&l0_key(&p))
        .await
        .expect("delete P's data object");

    let window = (late_window().0, sealed_window().1);
    let (plain, _) = resolve_counted(&store, Signal::Logs, window, &[]).await;
    assert_eq!(sorted_keys(&plain), vec![held.clone()], "HEAD is stale");

    let answer = catalog(&store)
        .resolve_with_accounting(
            &tenant(),
            Signal::Logs,
            window.0,
            &[token(&p)],
            window.1,
            &QueryAccounting::new(),
        )
        .await;
    let snapshot = match answer {
        Ok(snapshot) => snapshot,
        // Refusing P's token is a correct answer to a state the resolver
        // cannot serve exactly once. Any other error is a broken fixture.
        Err(CatalogError::UnsatisfiableToken { writer_id, seq, .. }) => {
            assert_eq!(
                (writer_id, seq),
                (Uuid::from_u128(0x4418_0001).to_string(), 1)
            );
            return;
        }
        Err(other) => panic!("resolve failed for another reason: {other}"),
    };
    let keys = sorted_keys(&snapshot);
    assert!(
        !(keys.contains(&held) && keys.contains(&rewrite_part_key(&rw))),
        "R's part beside an input R replaced serves M's rows twice: {keys:?}, rows {}",
        rows(&snapshot)
    );
    assert!(
        keys.contains(&rewrite_part_key(&rw)),
        "the token's surviving rows live only in R's part once P is swept: {keys:?}"
    );
    assert_eq!(
        rows(&snapshot),
        L_ROWS + M_ROWS - 1,
        "every surviving row counted once"
    );
}

#[tokio::test]
#[ignore = "suspected defect #4428: the swept-token fallback adds a live rewrite's part beside an input it replaced"]
async fn swept_token_beside_a_raw_input_its_live_rewrite_replaced() {
    swept_token_beside_an_input_its_live_rewrite_replaced(false).await;
}

#[tokio::test]
#[ignore = "suspected defect #4428: the swept-token fallback adds a live rewrite's part beside an input it replaced"]
async fn swept_token_beside_a_compaction_its_live_rewrite_replaced() {
    swept_token_beside_an_input_its_live_rewrite_replaced(true).await;
}

const REPLACED_OBJECT_PROPERTY: &str = "commit: a swept-token fallback never adds a rewrite's parts beside an object the rewrite replaced";

/// `(condition, details)` of every captured evaluation of the swept-token
/// replaced-object property whose details mention `token`, or `None`
/// outside the invariants lane. This crate forwards no
/// `ravel-invariant/capture` feature, so the read sits inside an assertion
/// macro, whose condition runs only when ravel-invariant records
/// evaluations.
fn replaced_object_evaluations(token: &CommitToken) -> Option<Vec<(bool, String)>> {
    let found = std::cell::RefCell::new(None);
    let needle = token.encode();
    let _ = &needle;
    ravel_invariant::inv_sometimes!(
        {
            *found.borrow_mut() =
                ravel_invariant::test_support::evaluations(REPLACED_OBJECT_PROPERTY, &needle);
            true
        },
        "ravel-catalog test: evaluations are read",
        ""
    );
    found.into_inner()
}

/// Asserts `evaluations` are exactly one evaluation, judged `expected`,
/// whose details name the token's tenant, signal, shard and ingest hour and
/// a judged set of `set_len` objects: the set as it stood before the
/// rewrite's parts were added.
fn assert_one_replaced_object_evaluation(
    evaluations: &[(bool, String)],
    expected: bool,
    set_len: usize,
) {
    assert_eq!(evaluations.len(), 1, "{evaluations:?}");
    let (condition, details) = &evaluations[0];
    assert_eq!(*condition, expected, "{details}");
    for field in [
        format!("\"tenant\": \"{}\"", tenant().to_hex()),
        "\"signal\": \"Logs\"".to_string(),
        "\"shard\": \"0\"".to_string(),
        format!("\"ingest_hour\": \"{HOUR}\""),
        format!("\"set_len\": \"{set_len}\""),
    ] {
        assert!(details.contains(&field), "{field} in {details}");
    }
}

/// The state the ignored `swept_token_beside_*` tests build, on writer
/// sequences `p_seq` and `p_seq + 1` so each test reads only its own
/// evaluations: HEAD names `held` (M's raw object, or the part of a
/// compaction of M), the live rewrite R over P and M landed after the fold,
/// and P's commit record and data object are deleted directly. Resolving
/// P's token reaches the swept-token fallback's live-rewrite branch with the
/// set holding `held` alone, an object R replaced. The property is judged on
/// that one-object set, before R's part joins it; a raw-input-only judge
/// misses the compacted variant's `held`.
async fn swept_token_beside_a_replaced_object_is_evaluated_false(compacted_m: bool, p_seq: u64) {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let p = l0(Signal::Logs, p_seq, HOUR, start, L_ROWS);
    if replaced_object_evaluations(&token(&p)).is_none() {
        return;
    }
    let m = l0(Signal::Logs, p_seq + 1, HOUR, start + 2_000_000, M_ROWS);
    put_l0(store.as_ref(), &p).await;
    put_l0(store.as_ref(), &m).await;
    let held = if compacted_m {
        let mut c = compaction(Signal::Logs, &[&m], 0x11, 0xc1, M_ROWS);
        c.parts[0].min_event_ts_ns = m.min_event_ts_ns;
        c.parts[0].max_event_ts_ns = m.max_event_ts_ns;
        put_compaction(store.as_ref(), &c).await;
        part_key(&c)
    } else {
        l0_key(&m)
    };
    fold_hour(&store, Signal::Logs).await;
    let request_id = Uuid::from_u128(0xd4428);
    put_dreq(store.as_ref(), Signal::Logs, request_id).await;
    let rw = put_rewrite(
        store.as_ref(),
        Signal::Logs,
        &[&p, &m],
        request_id,
        start,
        L_ROWS + M_ROWS - 1,
    )
    .await;
    store
        .delete(&keys::commit_key_for_record(&p).expect("commit key"))
        .await
        .expect("delete P's commit record");
    store
        .delete(&l0_key(&p))
        .await
        .expect("delete P's data object");

    let window = (late_window().0, sealed_window().1);
    let (plain, _) = resolve_counted(&store, Signal::Logs, window, &[]).await;
    assert_eq!(sorted_keys(&plain), vec![held.clone()], "HEAD is stale");
    let (snapshot, _) = resolve_counted(&store, Signal::Logs, window, &[token(&p)]).await;
    let mut expected = vec![held, rewrite_part_key(&rw)];
    expected.sort();
    assert_eq!(
        sorted_keys(&snapshot),
        expected,
        "the branch adds R's parts whatever the verdict"
    );

    let evaluations = replaced_object_evaluations(&token(&p)).expect("capturing");
    assert_one_replaced_object_evaluation(&evaluations, false, 1);
}

#[tokio::test]
async fn swept_token_beside_a_raw_input_its_live_rewrite_replaced_is_evaluated_false() {
    swept_token_beside_a_replaced_object_is_evaluated_false(false, 41).await;
}

#[tokio::test]
async fn swept_token_beside_a_compaction_its_live_rewrite_replaced_is_evaluated_false() {
    swept_token_beside_a_replaced_object_is_evaluated_false(true, 43).await;
}

/// HEAD folded after the rewrite R over P and M landed, so it names R's own
/// part and no object R replaced. P's commit record and data object are
/// deleted, and resolving P's token reaches the swept-token fallback's
/// live-rewrite branch with R's part already in the set: the property holds
/// and the snapshot serves R's part once.
#[tokio::test]
async fn swept_token_with_its_rewrite_part_in_head_is_evaluated_true() {
    let store = Arc::new(MemoryStore::new());
    let start = hour_start(HOUR);
    let p = l0(Signal::Logs, 45, HOUR, start, L_ROWS);
    if replaced_object_evaluations(&token(&p)).is_none() {
        return;
    }
    let m = l0(Signal::Logs, 46, HOUR, start + 2_000_000, M_ROWS);
    put_l0(store.as_ref(), &p).await;
    put_l0(store.as_ref(), &m).await;
    let request_id = Uuid::from_u128(0xd4428);
    put_dreq(store.as_ref(), Signal::Logs, request_id).await;
    let rw = put_rewrite(
        store.as_ref(),
        Signal::Logs,
        &[&p, &m],
        request_id,
        start,
        L_ROWS + M_ROWS - 1,
    )
    .await;
    fold_hour(&store, Signal::Logs).await;
    store
        .delete(&keys::commit_key_for_record(&p).expect("commit key"))
        .await
        .expect("delete P's commit record");
    store
        .delete(&l0_key(&p))
        .await
        .expect("delete P's data object");

    let (plain, _) = resolve_counted(&store, Signal::Logs, sealed_window(), &[]).await;
    assert_eq!(sorted_keys(&plain), vec![rewrite_part_key(&rw)]);
    let (snapshot, _) = resolve_counted(&store, Signal::Logs, sealed_window(), &[token(&p)]).await;
    assert_eq!(sorted_keys(&snapshot), vec![rewrite_part_key(&rw)]);
    assert_eq!(
        rows(&snapshot),
        L_ROWS + M_ROWS - 1,
        "each row counted once"
    );

    let evaluations = replaced_object_evaluations(&token(&p)).expect("capturing");
    assert_one_replaced_object_evaluation(&evaluations, true, 1);
}
