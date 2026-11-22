//! The swept-token fallback and the present-token step against a set that
//! already serves the token's rows (issue #4570).
//!
//! `tests/token_cache_erasure_4570.rs` runs D1 to D4 for logs with the same
//! assertions; this file runs them for metrics and spans, where the
//! fallback's set check and the present-token replacement check also apply.
//! It also pins the per-token cost of re-validating a cached commit record,
//! and covers a metrics token whose present input a completed erasure's live
//! rewrite replaced.
//!
//! The "cold-record" catalog keeps a HEAD for an hour on a clock that never
//! moves, so it serves the HEAD it first read while its commit-record cache
//! stays empty for inputs it only saw folded. The catalog never reads data
//! objects, so none are written.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, Snapshot,
};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{erasure, keys, signal};
use ravel_cpu_gate::MonotonicClock;
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Sequence, SequenceStep};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord,
    ErasurePredicateMatcher, ErasureRequest, RetentionTombstone, RewriteDrop, RewriteRecord,
};
use ravel_segment::VERSION_V7;
use ravel_types::{CommitToken, Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const HOUR: u32 = 500_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

const A_ROWS: u64 = 3;
const B_ROWS: u64 = 4;
const WRITER_A: u128 = 0x4570_100a;
const WRITER_B: u128 = 0x4570_100b;
/// Matching GETs a probe sequence counts before it stops counting; far
/// above what any resolve here issues.
const PROBE_STEPS: usize = 64;

#[derive(Default)]
struct FrozenClock(AtomicU64);

impl MonotonicClock for FrozenClock {
    fn now_nanos(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn tenant() -> TenantHash {
    TenantHash([0x46; 16])
}

fn hour_start(hour: u32) -> i64 {
    i64::from(hour) * NS_PER_HOUR
}

fn hour_range(hour: u32) -> TimeRange {
    TimeRange {
        start_ns: hour_start(hour),
        end_ns: hour_start(hour) + NS_PER_HOUR,
    }
}

/// While `HOUR` is open: its bucket is listed.
fn live_now() -> i64 {
    hour_start(HOUR) + 30 * 60_000_000_000
}

/// The `now` of fold `step`, which seals `HOUR + step`.
fn sealed_now(step: u32) -> i64 {
    i64::from(HOUR + 1 + step) * NS_PER_HOUR + MARGIN_NS + 1
}

fn catalog_on(store: Arc<dyn ObjectStoreBackend>, head_cache_ttl_ns: Option<i64>) -> Catalog {
    let mut config = CatalogConfig {
        shard_count: 1,
        ..Default::default()
    };
    if let Some(ttl) = head_cache_ttl_ns {
        config.head_cache_ttl_ns = ttl;
    }
    Catalog::new(store, config)
        .expect("catalog")
        .with_monotonic_clock(Arc::new(FrozenClock::default()))
}

/// A one-hour HEAD TTL on a clock that never moves.
fn cold_record_node(store: &Arc<MemoryStore>) -> Catalog {
    catalog_on(store.clone(), Some(NS_PER_HOUR))
}

fn l0_record(signal: Signal, writer: u128, epoch: u64, seq: u64, rows: u64, c: u8) -> CommitRecord {
    record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal,
        shard: 0,
        writer_id: Uuid::from_u128(writer),
        writer_epoch: epoch,
        writer_seq: seq,
        object_size: 100,
        content_hash: [c; 32],
        sample_count: rows,
        series_count: 1,
        min_event_ts_ns: hour_start(HOUR),
        max_event_ts_ns: hour_start(HOUR) + 100,
        min_ingest_ts_ns: hour_start(HOUR),
        max_ingest_ts_ns: hour_start(HOUR) + 100,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: hour_start(HOUR) + 1_000 + i64::from(c),
        ingest_hour_bucket: HOUR,
    })
    .expect("valid record")
}

/// The token's flush: writer A, epoch 2, seq 7.
fn record_a(signal: Signal) -> CommitRecord {
    l0_record(signal, WRITER_A, 2, 7, A_ROWS, 0xa0)
}

fn identity(record: &CommitRecord) -> CompactionInputIdentity {
    CompactionInputIdentity {
        writer_id: record.writer_id.clone(),
        writer_epoch: record.writer_epoch,
        writer_seq: record.writer_seq,
    }
}

fn part(seed: u8, rows: u64) -> CompactionPart {
    CompactionPart {
        part_index: 0,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xff; 16],
        content_hash: vec![seed; 32],
        object_size: 4096,
        sample_count: rows,
        series_count: 1,
        run_count: 1,
        min_event_ts_ns: hour_start(HOUR),
        max_event_ts_ns: hour_start(HOUR) + 100,
        segment_format_version: u32::from(VERSION_V7),
        declared_column_stats: Vec::new(),
    }
}

fn token(record: &CommitRecord) -> CommitToken {
    record::token_for(record).expect("token")
}

fn commit_key(record: &CommitRecord) -> String {
    keys::commit_key_for_record(record).expect("commit key")
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

async fn put(store: &dyn ObjectStoreBackend, key: &str, body: Vec<u8>) {
    store
        .put(key, body.into(), PutOptions::create_if_absent())
        .await
        .expect("put");
}

async fn put_l0(store: &dyn ObjectStoreBackend, record: &CommitRecord) {
    put(store, &commit_key(record), record::encode(record).to_vec()).await;
}

async fn delete(store: &dyn ObjectStoreBackend, key: &str) {
    store.delete(key).await.expect("delete");
}

/// A version 1 compaction record over `inputs` with one part.
async fn put_compaction(
    store: &dyn ObjectStoreBackend,
    signal: Signal,
    inputs: &[&CommitRecord],
    rows: u64,
) -> CompactionRecord {
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        level: 1,
        inputs: inputs.iter().map(|r| identity(r)).collect(),
        input_set_hash: vec![0x46; 32],
        parts: vec![part(0xc1, rows)],
        created_unix_ns: hour_start(HOUR) + 2_000,
        superseded_record_key: String::new(),
    };
    put(
        store,
        &keys::compaction_record_key_for(&record).expect("record key"),
        record::encode_compaction(&record).to_vec(),
    )
    .await;
    record
}

async fn fold(store: &Arc<MemoryStore>, signal: Signal, step: u32) {
    let report = catalog_on(store.clone(), None)
        .fold(
            &tenant(),
            signal,
            Uuid::from_u128(0x4570_f100 + u128::from(step)),
            sealed_now(step),
            &[],
            None,
        )
        .await
        .expect("fold");
    assert_eq!(report.watermark_hour, Some(HOUR + step));
}

async fn resolve(catalog: &Catalog, signal: Signal, now: i64, tokens: &[CommitToken]) -> Snapshot {
    catalog
        .resolve(&tenant(), signal, hour_range(HOUR), tokens, now)
        .await
        .expect("resolve")
}

/// The HEAD a fresh reader serves for `HOUR` after fold `step`.
async fn head_keys(store: &Arc<MemoryStore>, signal: Signal, step: u32) -> Vec<String> {
    let snapshot = resolve(&cold_record_node(store), signal, sealed_now(step), &[]).await;
    sorted_keys(&snapshot)
}

fn assert_served_once(snapshot: &Snapshot, a: &CommitRecord, c: &CompactionRecord) {
    let held_a = holds(snapshot, &l0_key(a));
    let held_l = holds(snapshot, &part_key(c));
    assert!(
        held_a != held_l,
        "exactly one of A and L serves A's rows; A held: {held_a}, L held: {held_l}"
    );
}

/// D1: the cold-record node serves a cached HEAD naming A after C compacted A
/// into L and the sweep deleted A's commit record.
async fn d1_swept_token_over_a_cached_head(signal: Signal) {
    let store = Arc::new(MemoryStore::new());
    let node = cold_record_node(&store);
    let a = record_a(signal);
    put_l0(store.as_ref(), &a).await;
    fold(&store, signal, 0).await;
    let plain = resolve(&node, signal, sealed_now(0), &[]).await;
    assert_eq!(sorted_keys(&plain), vec![l0_key(&a)]);

    let c = put_compaction(store.as_ref(), signal, &[&a], A_ROWS).await;
    fold(&store, signal, 1).await;
    assert_eq!(head_keys(&store, signal, 1).await, vec![part_key(&c)]);
    delete(store.as_ref(), &commit_key(&a)).await;

    let snapshot = resolve(&node, signal, sealed_now(1), &[token(&a)]).await;
    assert_served_once(&snapshot, &a, &c);
    assert_eq!(rows(&snapshot), A_ROWS);
}

#[tokio::test]
async fn metrics_d1_swept_token_over_a_cached_head_serves_it_once() {
    d1_swept_token_over_a_cached_head(Signal::Metrics).await;
}

#[tokio::test]
async fn spans_d1_swept_token_over_a_cached_head_serves_it_once() {
    d1_swept_token_over_a_cached_head(Signal::Spans).await;
}

/// D2 and D3: the cached HEAD holds `other`, a flush of the same bucket that
/// is not A, and A was written after that HEAD. The fallback must add L.
async fn swept_token_beside_another_l0(signal: Signal, other: CommitRecord) {
    let store = Arc::new(MemoryStore::new());
    let node = cold_record_node(&store);
    put_l0(store.as_ref(), &other).await;
    fold(&store, signal, 0).await;
    let plain = resolve(&node, signal, sealed_now(0), &[]).await;
    assert_eq!(sorted_keys(&plain), vec![l0_key(&other)]);

    let a = record_a(signal);
    assert_ne!(l0_key(&a), l0_key(&other));
    put_l0(store.as_ref(), &a).await;
    let c = put_compaction(store.as_ref(), signal, &[&a], A_ROWS).await;
    fold(&store, signal, 1).await;
    delete(store.as_ref(), &commit_key(&a)).await;

    let snapshot = resolve(&node, signal, sealed_now(1), &[token(&a)]).await;
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
async fn metrics_d2_swept_token_beside_another_writers_l0_adds_the_part() {
    let other = l0_record(Signal::Metrics, WRITER_B, 1, 3, B_ROWS, 0xb0);
    swept_token_beside_another_l0(Signal::Metrics, other).await;
}

#[tokio::test]
async fn spans_d2_swept_token_beside_another_writers_l0_adds_the_part() {
    let other = l0_record(Signal::Spans, WRITER_B, 1, 3, B_ROWS, 0xb0);
    swept_token_beside_another_l0(Signal::Spans, other).await;
}

/// A's writer before a restart: same writer id and seq, a lower epoch.
fn restarted_writer(signal: Signal) -> CommitRecord {
    let a = record_a(signal);
    l0_record(
        signal,
        WRITER_A,
        a.writer_epoch - 1,
        a.writer_seq,
        B_ROWS,
        0xb1,
    )
}

#[tokio::test]
async fn metrics_d3_swept_token_beside_its_writers_earlier_epoch_adds_the_part() {
    swept_token_beside_another_l0(Signal::Metrics, restarted_writer(Signal::Metrics)).await;
}

#[tokio::test]
async fn spans_d3_swept_token_beside_its_writers_earlier_epoch_adds_the_part() {
    swept_token_beside_another_l0(Signal::Spans, restarted_writer(Signal::Spans)).await;
}

/// D4: D1, but the node read A with its token before the compaction, so its
/// commit-record cache holds A when the second token read runs.
async fn d4_cached_token_over_a_cached_head(signal: Signal) {
    let store = Arc::new(MemoryStore::new());
    let node = cold_record_node(&store);
    let a = record_a(signal);
    put_l0(store.as_ref(), &a).await;
    fold(&store, signal, 0).await;
    let first = resolve(&node, signal, sealed_now(0), &[token(&a)]).await;
    assert_eq!(sorted_keys(&first), vec![l0_key(&a)]);

    let c = put_compaction(store.as_ref(), signal, &[&a], A_ROWS).await;
    fold(&store, signal, 1).await;
    assert_eq!(head_keys(&store, signal, 1).await, vec![part_key(&c)]);
    delete(store.as_ref(), &commit_key(&a)).await;

    let snapshot = resolve(&node, signal, sealed_now(1), &[token(&a)]).await;
    assert_served_once(&snapshot, &a, &c);
    assert_eq!(rows(&snapshot), A_ROWS);
}

#[tokio::test]
async fn metrics_d4_cached_token_over_a_cached_head_serves_it_once() {
    d4_cached_token_over_a_cached_head(Signal::Metrics).await;
}

#[tokio::test]
async fn spans_d4_cached_token_over_a_cached_head_serves_it_once() {
    d4_cached_token_over_a_cached_head(Signal::Spans).await;
}

/// A catalog on a store that counts every GET of `record`'s commit key.
fn counting_store(
    inner: &Arc<MemoryStore>,
    record: &CommitRecord,
) -> Arc<FaultStore<Arc<MemoryStore>>> {
    let probe = Sequence::new(Op::Get)
        .with_key_contains(commit_key(record))
        .with_steps(vec![SequenceStep::Passthrough; PROBE_STEPS]);
    Arc::new(FaultStore::new(
        inner.clone(),
        FaultPlan::empty().with_sequence(probe),
    ))
}

/// A store that records the range of every GET, by key.
struct RangeLog {
    inner: Arc<MemoryStore>,
    gets: std::sync::Mutex<Vec<(String, GetRange)>>,
}

impl RangeLog {
    fn new(inner: Arc<MemoryStore>) -> Arc<Self> {
        Arc::new(RangeLog {
            inner,
            gets: std::sync::Mutex::default(),
        })
    }

    fn ranges_of(&self, key: &str) -> Vec<GetRange> {
        self.gets
            .lock()
            .expect("gets")
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, range)| *range)
            .collect()
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for RangeLog {
    async fn put(&self, k: &str, d: Bytes, o: PutOptions) -> Result<PutOutcome, StoreError> {
        self.inner.put(k, d, o).await
    }
    async fn get(&self, k: &str, r: GetRange) -> Result<GetOutcome, StoreError> {
        self.gets.lock().expect("gets").push((k.to_string(), r));
        self.inner.get(k, r).await
    }
    async fn head(&self, k: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(k).await
    }
    async fn list(&self, p: &str, t: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(p, t).await
    }
    async fn list_after(
        &self,
        p: &str,
        s: Option<&str>,
        t: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.inner.list_after(p, s, t).await
    }
    async fn list_delimited(&self, p: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(p).await
    }
    async fn delete(&self, k: &str) -> Result<(), StoreError> {
        self.inner.delete(k).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// Re-validating a cached commit record reads one byte of it: only the
/// record's presence is in question, and the cached decode is reused. The
/// listing's first, uncached GET reads the record whole, since it decodes it.
#[tokio::test]
async fn warm_cache_revalidation_reads_one_byte_of_the_commit_record() {
    let signal = Signal::Logs;
    let inner = Arc::new(MemoryStore::new());
    let a = record_a(signal);
    let store = RangeLog::new(inner.clone());
    let warm = catalog_on(store.clone(), None);
    put_l0(inner.as_ref(), &a).await;

    let first = resolve(&warm, signal, live_now(), &[token(&a)]).await;
    assert_eq!(sorted_keys(&first), vec![l0_key(&a)]);
    assert_eq!(store.ranges_of(&commit_key(&a)), vec![GetRange::Full]);

    // C replaces A with L; the set holds L, not A, so the cached record is
    // re-validated.
    let c = put_compaction(inner.as_ref(), signal, &[&a], A_ROWS).await;
    let compacted = resolve(&warm, signal, live_now(), &[token(&a)]).await;
    assert_eq!(sorted_keys(&compacted), vec![part_key(&c)]);
    assert_eq!(
        store.ranges_of(&commit_key(&a)),
        vec![GetRange::Full, GetRange::Range(0, 1)]
    );
}

/// A warm node's cached commit record costs one GET of its key per token
/// read that does not already hold the token's object, and none when the set
/// holds it.
#[tokio::test]
async fn warm_cache_token_read_costs_one_get_of_the_commit_key() {
    let signal = Signal::Logs;
    let inner = Arc::new(MemoryStore::new());
    let a = record_a(signal);
    let store = counting_store(&inner, &a);
    let warm = catalog_on(store.clone(), None);
    put_l0(inner.as_ref(), &a).await;

    let first = resolve(&warm, signal, live_now(), &[token(&a)]).await;
    assert_eq!(sorted_keys(&first), vec![l0_key(&a)]);
    let warmed = store.sequence_progress(0);
    assert_eq!(warmed, 1, "the listing read A's commit record once");

    // The listing serves A from the cache and the set holds A: no GET.
    let again = resolve(&warm, signal, live_now(), &[token(&a)]).await;
    assert_eq!(sorted_keys(&again), vec![l0_key(&a)]);
    assert_eq!(store.sequence_progress(0), warmed);

    // C replaces A with L; the set holds L, not A, so the cached record is
    // re-validated by exactly one GET before the step reads the bucket.
    let c = put_compaction(inner.as_ref(), signal, &[&a], A_ROWS).await;
    let compacted = resolve(&warm, signal, live_now(), &[token(&a)]).await;
    assert_eq!(sorted_keys(&compacted), vec![part_key(&c)]);
    assert_eq!(store.sequence_progress(0), warmed + 1);
}

/// A metrics token whose commit record is still present while a completed
/// erasure's live rewrite R replaced its object A: Q's `.dreq` is gone and its
/// `.done` written, but the sweep has not yet deleted A's commit record. The
/// query window excludes A's hour, so the set holds nothing of the bucket
/// and no erasure is pending; the live rewrite alone must keep A out.
#[tokio::test]
async fn metrics_present_token_replaced_by_a_completed_erasure_serves_the_rewrite() {
    let signal = Signal::Metrics;
    let store = Arc::new(MemoryStore::new());
    let a = record_a(signal);
    put_l0(store.as_ref(), &a).await;

    let q = Uuid::from_u128(0x4570_d101);
    let request = ErasureRequest {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        request_id: q.to_string(),
        created_unix_ns: hour_start(HOUR) + 4_000,
        predicate: vec![ErasurePredicateMatcher {
            key: "user".to_string(),
            value: "s-4570".to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    };
    let dreq_key = keys::erasure_request_key(&tenant(), signal, q).expect("dreq key");
    put(
        store.as_ref(),
        &dreq_key,
        erasure::encode_request(&request).to_vec(),
    )
    .await;
    let request_id = q.to_string();
    let inputs = vec![identity(&a)];
    let r = RewriteRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &inputs,
            None,
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        inputs,
        parts: vec![part(0xe0, A_ROWS - 1)],
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 1,
        }],
        created_unix_ns: hour_start(HOUR) + 5_000,
        superseded_record_key: String::new(),
    };
    put(
        store.as_ref(),
        &keys::rewrite_record_key_for(&r).expect("rewrite key"),
        erasure::encode_rewrite(&r).to_vec(),
    )
    .await;
    put(
        store.as_ref(),
        &keys::erasure_completion_key(&tenant(), signal, q).expect("done key"),
        b"done".to_vec(),
    )
    .await;
    delete(store.as_ref(), &dreq_key).await;

    let other_hour = HOUR + 5;
    let snapshot = catalog_on(store.clone(), None)
        .resolve(
            &tenant(),
            signal,
            hour_range(other_hour),
            &[token(&a)],
            hour_start(other_hour) + 60_000_000_000,
        )
        .await
        .expect("resolve");
    assert!(snapshot.pending_erasure.is_empty(), "Q is not pending");
    assert_eq!(
        sorted_keys(&snapshot),
        vec![rewrite_part_key(&r)],
        "R's part serves A's surviving rows; A, which holds the erased row, is not added"
    );
    assert_eq!(rows(&snapshot), A_ROWS - 1);
}

/// A token whose commit record is still present, in a bucket retention has
/// tombstoned, on a tenant that has a completed erasure request (`.done`)
/// unrelated to the bucket. The query window excludes the token's hour, so
/// the set holds nothing of the bucket and no erasure is pending: the bucket
/// LIST runs only because of the `.done`, and it shows the tombstone. The
/// token is satisfied with zero segments, as the absent-record fallback
/// answers a tombstoned bucket, so A, which retention is deleting, is not
/// added.
async fn present_token_in_a_tombstoned_bucket_after_a_completed_erasure(signal: Signal) {
    let store = Arc::new(MemoryStore::new());
    let a = record_a(signal);
    put_l0(store.as_ref(), &a).await;
    let q = Uuid::from_u128(0x4573_d001);
    put(
        store.as_ref(),
        &keys::erasure_completion_key(&tenant(), signal, q).expect("done key"),
        b"done".to_vec(),
    )
    .await;
    let tombstone = RetentionTombstone {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        retired_at_ns: 0,
        retention_window_ns: 0,
        record_count_observed: 0,
    };
    put(
        store.as_ref(),
        &keys::retention_tombstone_key_for(&tombstone).expect("tombstone key"),
        record::encode_tombstone(&tombstone).to_vec(),
    )
    .await;

    let other_hour = HOUR + 5;
    let snapshot = catalog_on(store.clone(), None)
        .resolve(
            &tenant(),
            signal,
            hour_range(other_hour),
            &[token(&a)],
            hour_start(other_hour) + 60_000_000_000,
        )
        .await
        .expect("resolve");
    assert!(snapshot.pending_erasure.is_empty(), "nothing is pending");
    assert_eq!(
        sorted_keys(&snapshot),
        Vec::<String>::new(),
        "A's bucket is tombstoned: A is not added"
    );
}

#[tokio::test]
async fn logs_present_token_in_a_tombstoned_bucket_after_a_completed_erasure_adds_nothing() {
    present_token_in_a_tombstoned_bucket_after_a_completed_erasure(Signal::Logs).await;
}

#[tokio::test]
async fn metrics_present_token_in_a_tombstoned_bucket_after_a_completed_erasure_adds_nothing() {
    present_token_in_a_tombstoned_bucket_after_a_completed_erasure(Signal::Metrics).await;
}
