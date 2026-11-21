//! Token reads on a node whose caches predate a compaction or an erasure
//! rewrite (issue #4570).
//!
//! A resolve with a min commit token reaches the token's level-0 input one of
//! three ways: a hit in the node-local commit-record cache (no store read), a
//! GET of the present commit record, or, when the record is gone, the
//! missing-record fallback that LISTs the token's bucket and adds a live
//! replacement's parts. The commit-record cache has no TTL, and a HEAD served
//! from the HEAD cache can name an object a newer HEAD no longer names. Each
//! test below asserts the correct answer: every acknowledged row is served
//! once and an erased row is never served.
//!
//! The "warm" catalog is one instance kept across the steps; its HEAD cache
//! is aged out with an injected monotonic clock before each resolve that must
//! see the newest HEAD, so only the commit-record cache carries state. The
//! "cold-record" catalog keeps a HEAD for an hour on a clock that never
//! moves, so it serves the HEAD it first read while its commit-record cache
//! stays empty for inputs it only saw folded.
//!
//! The catalog never reads data objects, so none are written; a "sweep" here
//! deletes commit and rewrite records only.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_HEAD_CACHE_TTL_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS, Snapshot,
};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{erasure, keys, signal};
use ravel_cpu_gate::MonotonicClock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord, ErasureCompletion,
    ErasurePredicateMatcher, ErasureRequest, RewriteDrop, RewriteRecord,
};
use ravel_segment::VERSION_V7;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::{CommitToken, Signal, TenantHash, TimeRange};
use uuid::Uuid;

const SIGNAL: Signal = Signal::Logs;
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const HOUR: u32 = 500_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

/// Rows of the token's flush A, and of the other writer's flush B.
const A_ROWS: u64 = 3;
const B_ROWS: u64 = 4;

const WRITER_A: u128 = 0x4570_000a;
const WRITER_B: u128 = 0x4570_000b;
const REQUEST_Q: u128 = 0x4570_d001;
const REQUEST_Q2: u128 = 0x4570_d002;

/// A monotonic clock that moves only when a test advances it.
#[derive(Default)]
struct ManualClock(AtomicU64);

impl ManualClock {
    fn advance_ns(&self, ns: i64) {
        self.0
            .fetch_add(u64::try_from(ns).expect("non-negative"), Ordering::SeqCst);
    }
}

impl MonotonicClock for ManualClock {
    fn now_nanos(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn tenant() -> TenantHash {
    TenantHash([0x45; 16])
}

fn hour_start(hour: u32) -> i64 {
    i64::from(hour) * NS_PER_HOUR
}

/// A query over `HOUR` before it seals: the bucket is listed.
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

/// A query over `HOUR` at the `now` of fold `step`, which seals `HOUR + step`:
/// HEAD serves the hour. Each later fold advances the watermark one hour, as
/// a fold must to re-list an hour it already sealed.
fn sealed_window(step: u32) -> (TimeRange, i64) {
    let start = hour_start(HOUR);
    (
        TimeRange {
            start_ns: start,
            end_ns: start + NS_PER_HOUR,
        },
        i64::from(HOUR + 1 + step) * NS_PER_HOUR + MARGIN_NS + 1,
    )
}

fn catalog_with(
    store: &Arc<MemoryStore>,
    head_cache_ttl_ns: i64,
    clock: &Arc<ManualClock>,
) -> Catalog {
    Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count: 1,
            head_cache_ttl_ns,
            ..Default::default()
        },
    )
    .expect("catalog")
    .with_monotonic_clock(clock.clone())
}

/// The warm node: default HEAD TTL, aged out by `age_out_head` before each
/// resolve that must read the newest HEAD.
fn warm_node(store: &Arc<MemoryStore>) -> (Catalog, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::default());
    (
        catalog_with(store, DEFAULT_HEAD_CACHE_TTL_NS, &clock),
        clock,
    )
}

fn age_out_head(clock: &ManualClock) {
    clock.advance_ns(DEFAULT_HEAD_CACHE_TTL_NS + 1);
}

/// The cold-record node: a one-hour HEAD TTL on a clock that never moves.
fn cold_record_node(store: &Arc<MemoryStore>) -> Catalog {
    catalog_with(store, NS_PER_HOUR, &Arc::new(ManualClock::default()))
}

fn l0_record(writer: u128, epoch: u64, seq: u64, rows: u64, content: u8) -> CommitRecord {
    record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: SIGNAL,
        shard: 0,
        writer_id: Uuid::from_u128(writer),
        writer_epoch: epoch,
        writer_seq: seq,
        object_size: 100,
        content_hash: [content; 32],
        sample_count: rows,
        series_count: 1,
        min_event_ts_ns: hour_start(HOUR),
        max_event_ts_ns: hour_start(HOUR) + 100,
        min_ingest_ts_ns: hour_start(HOUR),
        max_ingest_ts_ns: hour_start(HOUR) + 100,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: hour_start(HOUR) + 1_000 + i64::from(content),
        ingest_hour_bucket: HOUR,
    })
    .expect("valid record")
}

/// The token's flush: writer A, epoch 2, seq 7.
fn record_a() -> CommitRecord {
    l0_record(WRITER_A, 2, 7, A_ROWS, 0xa0)
}

async fn put_l0(store: &dyn ObjectStoreBackend, record: &CommitRecord) {
    store
        .put(
            &keys::commit_key_for_record(record).expect("commit key"),
            record::encode(record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put commit record");
}

async fn delete_commit_record(store: &dyn ObjectStoreBackend, record: &CommitRecord) {
    store
        .delete(&keys::commit_key_for_record(record).expect("commit key"))
        .await
        .expect("sweep commit record");
}

fn identity(record: &CommitRecord) -> CompactionInputIdentity {
    CompactionInputIdentity {
        writer_id: record.writer_id.clone(),
        writer_epoch: record.writer_epoch,
        writer_seq: record.writer_seq,
    }
}

fn part(seed: u8, rows: u64) -> CompactionPart {
    let start = hour_start(HOUR);
    CompactionPart {
        part_index: 0,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xff; 16],
        content_hash: vec![seed; 32],
        object_size: 4096,
        sample_count: rows,
        series_count: 1,
        run_count: 1,
        min_event_ts_ns: start,
        max_event_ts_ns: start + 100,
        segment_format_version: u32::from(VERSION_V7),
        declared_column_stats: Vec::new(),
    }
}

/// A version 1 compaction record over `inputs` with one part.
async fn put_compaction(
    store: &dyn ObjectStoreBackend,
    inputs: &[&CommitRecord],
    rows: u64,
) -> CompactionRecord {
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(SIGNAL) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        level: 1,
        inputs: inputs.iter().map(|r| identity(r)).collect(),
        input_set_hash: vec![0x45; 32],
        parts: vec![part(0xc1, rows)],
        created_unix_ns: hour_start(HOUR) + 2_000,
        superseded_record_key: String::new(),
    };
    store
        .put(
            &keys::compaction_record_key_for(&record).expect("record key"),
            record::encode_compaction(&record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put compaction record");
    record
}

async fn put_rewrite_record(store: &dyn ObjectStoreBackend, record: &RewriteRecord) -> String {
    let key = keys::rewrite_record_key_for(record).expect("rewrite key");
    store
        .put(
            &key,
            erasure::encode_rewrite(record),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put rewrite record");
    key
}

/// A rewrite over raw L0 `inputs` applying `request_id`, with one part.
async fn put_rewrite(
    store: &dyn ObjectStoreBackend,
    inputs: &[&CommitRecord],
    request_id: Uuid,
    rows: u64,
) -> (String, RewriteRecord) {
    let request_id = request_id.to_string();
    let input_ids: Vec<CompactionInputIdentity> = inputs.iter().map(|r| identity(r)).collect();
    let record = RewriteRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(SIGNAL) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &input_ids,
            None,
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        inputs: input_ids,
        parts: vec![part(0xe0, rows)],
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 1,
        }],
        created_unix_ns: hour_start(HOUR) + 5_000,
        superseded_record_key: String::new(),
    };
    (put_rewrite_record(store, &record).await, record)
}

/// A rewrite superseding the record at `predecessor_key` as a whole, the
/// shape `ravel-maintain` writes for a bucket already rewritten
/// (`RewriteSupersession::Existing`).
async fn put_superseding_rewrite(
    store: &dyn ObjectStoreBackend,
    predecessor_key: &str,
    request_id: Uuid,
    rows: u64,
) -> (String, RewriteRecord) {
    let request_id = request_id.to_string();
    let record = RewriteRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(SIGNAL) as i32,
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
    (put_rewrite_record(store, &record).await, record)
}

async fn put_dreq(store: &dyn ObjectStoreBackend, request_id: Uuid, subject: &str) {
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(SIGNAL) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: hour_start(HOUR) + 4_000,
        predicate: vec![ErasurePredicateMatcher {
            key: "user".to_string(),
            value: subject.to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    store
        .put(
            &keys::erasure_request_key(&tenant(), SIGNAL, request_id).expect("dreq key"),
            erasure::encode_request(&request),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put dreq");
}

/// The erasure pass's completion: write the `.done`, then the request sweep
/// deletes the `.dreq`.
async fn complete_erasure(store: &dyn ObjectStoreBackend, request_id: Uuid) {
    let completion = ErasureCompletion {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(SIGNAL) as i32,
        request_id: request_id.to_string(),
        predicate_hash: vec![0u8; 32],
        bucket_drops: Vec::new(),
        requested_unix_ns: hour_start(HOUR) + 4_000,
        completed_unix_ns: hour_start(HOUR) + 20_000,
        ..Default::default()
    };
    store
        .put(
            &keys::erasure_completion_key_for(&completion).expect("done key"),
            erasure::encode_completion(&completion),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put done");
    store
        .delete(&keys::erasure_request_key(&tenant(), SIGNAL, request_id).expect("dreq key"))
        .await
        .expect("sweep dreq");
}

async fn fold_hour(store: &Arc<MemoryStore>, step: u32) {
    let report = Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count: 1,
            ..Default::default()
        },
    )
    .expect("catalog")
    .fold(
        &tenant(),
        SIGNAL,
        Uuid::from_u128(0x4570_f000 + u128::from(step)),
        sealed_window(step).1,
        &[],
        None,
    )
    .await
    .expect("fold");
    assert_eq!(report.watermark_hour, Some(HOUR + step));
}

/// The HEAD a fresh reader serves for the sealed hour after fold `step`.
async fn head_keys(store: &Arc<MemoryStore>, step: u32) -> Vec<String> {
    let (snapshot, _) = resolve_on(&cold_record_node(store), sealed_window(step), &[]).await;
    sorted_keys(&snapshot)
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

fn holds(snapshot: &Snapshot, key: &str) -> bool {
    snapshot.segments.iter().any(|s| s.data_object_key == key)
}

fn rows(snapshot: &Snapshot) -> u64 {
    snapshot.segments.iter().map(|s| s.sample_count).sum()
}

/// Store requests one resolve issued.
#[derive(Debug, PartialEq, Eq)]
struct Reads {
    lists: u64,
    gets: u64,
}

async fn resolve_on(
    catalog: &Catalog,
    window: (TimeRange, i64),
    tokens: &[CommitToken],
) -> (Snapshot, Reads) {
    let accounting = QueryAccounting::new();
    let snapshot = catalog
        .resolve_with_accounting(&tenant(), SIGNAL, window.0, tokens, window.1, &accounting)
        .await
        .expect("resolve");
    let counts = accounting.snapshot();
    let reads = Reads {
        lists: counts.s3_requests(AccountedOp::List),
        gets: counts.s3_requests(AccountedOp::Get),
    };
    (snapshot, reads)
}

/// E1's state: A written and warmed into the warm node's commit-record
/// cache, then erased by rewrite R (part P), folded, swept and completed.
async fn erased_after_warm_read() -> (
    Arc<MemoryStore>,
    Catalog,
    Arc<ManualClock>,
    CommitRecord,
    (String, RewriteRecord),
) {
    let store = Arc::new(MemoryStore::new());
    let (warm, clock) = warm_node(&store);
    let a = record_a();
    put_l0(store.as_ref(), &a).await;
    let (warmed, _) = resolve_on(&warm, live_window(), &[token(&a)]).await;
    assert_eq!(sorted_keys(&warmed), vec![l0_key(&a)]);

    let q = Uuid::from_u128(REQUEST_Q);
    put_dreq(store.as_ref(), q, "subject-s").await;
    let (r_key, r) = put_rewrite(store.as_ref(), &[&a], q, A_ROWS - 1).await;
    fold_hour(&store, 0).await;
    assert_eq!(head_keys(&store, 0).await, vec![rewrite_part_key(&r)]);
    delete_commit_record(store.as_ref(), &a).await;
    complete_erasure(store.as_ref(), q).await;
    (store, warm, clock, a, (r_key, r))
}

/// E1: the warm node's commit-record cache still holds A after A was erased
/// and swept. A token read must serve P and never A.
#[tokio::test]
async fn e1_erased_input_cached_on_the_node_is_not_served() {
    let (_store, warm, clock, a, (_, r)) = erased_after_warm_read().await;

    age_out_head(&clock);
    let (snapshot, _) = resolve_on(&warm, sealed_window(0), &[token(&a)]).await;
    assert!(
        !holds(&snapshot, &l0_key(&a)),
        "the erased input A is never served: {:?}",
        sorted_keys(&snapshot)
    );
    assert!(
        holds(&snapshot, &rewrite_part_key(&r)),
        "P serves A's surviving rows"
    );
    assert_eq!(rows(&snapshot), A_ROWS - 1);
}

/// E2: E1, then a second erasure rewrite R2 supersedes R as a whole (part
/// P2), is folded, and the superseded-input sweep deletes R. Compaction
/// cannot run here: it refuses a bucket that holds a rewrite record, and no
/// compaction input identity can name a rewrite's output, so R2 is how a
/// rewritten bucket's output is replaced.
#[tokio::test]
#[ignore = "fails until #4570 is fixed"]
async fn e2_erased_input_cached_on_the_node_after_a_rewrite_of_the_rewrite() {
    let (store, warm, clock, a, (r_key, _)) = erased_after_warm_read().await;

    let q2 = Uuid::from_u128(REQUEST_Q2);
    put_dreq(store.as_ref(), q2, "subject-s2").await;
    let (_, r2) = put_superseding_rewrite(store.as_ref(), &r_key, q2, A_ROWS - 2).await;
    fold_hour(&store, 1).await;
    assert_eq!(head_keys(&store, 1).await, vec![rewrite_part_key(&r2)]);
    store.delete(&r_key).await.expect("sweep R");
    complete_erasure(store.as_ref(), q2).await;

    age_out_head(&clock);
    let (snapshot, _) = resolve_on(&warm, sealed_window(1), &[token(&a)]).await;
    assert!(
        !holds(&snapshot, &l0_key(&a)),
        "the erased input A is never served: {:?}",
        sorted_keys(&snapshot)
    );
    assert!(
        holds(&snapshot, &rewrite_part_key(&r2)),
        "P2 serves A's surviving rows"
    );
    assert_eq!(rows(&snapshot), A_ROWS - 2);
}

/// Exactly one of A and its compaction part L serves A's rows.
fn assert_served_once(snapshot: &Snapshot, a: &CommitRecord, c: &CompactionRecord) {
    let held_a = holds(snapshot, &l0_key(a));
    let held_l = holds(snapshot, &part_key(c));
    assert!(
        held_a != held_l,
        "exactly one of A and L serves A's rows; A held: {held_a}, L held: {held_l}"
    );
}

/// D1: the cold-record node serves a HEAD naming A from its HEAD cache after
/// C compacted A into L and the sweep deleted A's commit record. The token's
/// fallback must not add L beside A.
#[tokio::test]
#[ignore = "fails until #4570 is fixed"]
async fn d1_swept_token_over_a_cached_head_naming_its_input_serves_it_once() {
    let store = Arc::new(MemoryStore::new());
    let node = cold_record_node(&store);
    let a = record_a();
    put_l0(store.as_ref(), &a).await;
    fold_hour(&store, 0).await;
    let (plain, _) = resolve_on(&node, sealed_window(0), &[]).await;
    assert_eq!(sorted_keys(&plain), vec![l0_key(&a)]);

    let c = put_compaction(store.as_ref(), &[&a], A_ROWS).await;
    fold_hour(&store, 1).await;
    assert_eq!(head_keys(&store, 1).await, vec![part_key(&c)]);
    delete_commit_record(store.as_ref(), &a).await;

    let (snapshot, _) = resolve_on(&node, sealed_window(1), &[token(&a)]).await;
    assert_served_once(&snapshot, &a, &c);
    assert_eq!(rows(&snapshot), A_ROWS);
}

/// D2 and D3: the cached HEAD holds `other`, a flush of the same bucket that
/// is not A, and A was written after that HEAD. The fallback must add L.
async fn swept_token_beside_another_l0_adds_the_part(other: CommitRecord) {
    let store = Arc::new(MemoryStore::new());
    let node = cold_record_node(&store);
    put_l0(store.as_ref(), &other).await;
    fold_hour(&store, 0).await;
    let (plain, _) = resolve_on(&node, sealed_window(0), &[]).await;
    assert_eq!(sorted_keys(&plain), vec![l0_key(&other)]);

    let a = record_a();
    assert_ne!(l0_key(&a), l0_key(&other));
    put_l0(store.as_ref(), &a).await;
    let c = put_compaction(store.as_ref(), &[&a], A_ROWS).await;
    fold_hour(&store, 1).await;
    delete_commit_record(store.as_ref(), &a).await;

    let (snapshot, _) = resolve_on(&node, sealed_window(1), &[token(&a)]).await;
    assert!(
        holds(&snapshot, &part_key(&c)),
        "the fallback adds L: {:?}",
        sorted_keys(&snapshot)
    );
    assert_served_once(&snapshot, &a, &c);
    assert!(holds(&snapshot, &l0_key(&other)));
    assert_eq!(rows(&snapshot), other.sample_count + A_ROWS);
}

#[tokio::test]
async fn d2_swept_token_beside_another_writers_l0_adds_the_part() {
    swept_token_beside_another_l0_adds_the_part(l0_record(WRITER_B, 1, 3, B_ROWS, 0xb0)).await;
}

/// The other flush is A's writer before a restart: same writer id and seq,
/// a lower epoch.
#[tokio::test]
async fn d3_swept_token_beside_its_writers_earlier_epoch_adds_the_part() {
    let a = record_a();
    swept_token_beside_another_l0_adds_the_part(l0_record(
        WRITER_A,
        a.writer_epoch - 1,
        a.writer_seq,
        B_ROWS,
        0xb1,
    ))
    .await;
}

/// D4: D1, but the node read A with its token before the compaction, so its
/// commit-record cache holds A when the second token read runs.
#[tokio::test]
async fn d4_cached_token_over_a_cached_head_naming_its_input_serves_it_once() {
    let store = Arc::new(MemoryStore::new());
    let node = cold_record_node(&store);
    let a = record_a();
    put_l0(store.as_ref(), &a).await;
    fold_hour(&store, 0).await;
    let (first, _) = resolve_on(&node, sealed_window(0), &[token(&a)]).await;
    assert_eq!(sorted_keys(&first), vec![l0_key(&a)]);

    let c = put_compaction(store.as_ref(), &[&a], A_ROWS).await;
    fold_hour(&store, 1).await;
    assert_eq!(head_keys(&store, 1).await, vec![part_key(&c)]);
    delete_commit_record(store.as_ref(), &a).await;

    let (snapshot, _) = resolve_on(&node, sealed_window(1), &[token(&a)]).await;
    assert_served_once(&snapshot, &a, &c);
    assert_eq!(rows(&snapshot), A_ROWS);
}
