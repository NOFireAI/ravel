//! Erasure across signals on a node whose record cache still holds the
//! token's pre-erasure L0 commit record (issue #4570).
//!
//! L0 object A (token T) carries two rows: one of subject S and one that does
//! not match S. A warm catalog resolves T once, so its record cache holds A's
//! commit record; the record cache has no TTL. Then erasure request Q for S
//! runs to completion: rewrite record R over {A} publishes part P (the
//! surviving row), a fold names P and not A, the sweep deletes A's commit
//! record and data object, Q's `.done` is written, the clock moves past the
//! 25 h 5 min protection horizon and Q's `.dreq` is deleted. The snapshot then
//! carries no pending-erasure predicate, so if A is in the resolved set
//! nothing filters S's row out of it.
//!
//! X1 resolves T on the warm catalog right there. X2 first compacts P into P2
//! under a compaction record whose inputs name R's output by the identity the
//! resolver gives a rewrite part (nil writer id, epoch 0, seq 0: a rewrite
//! part has no writer identity of its own), folds a HEAD naming P2 and deletes
//! R and P, then resolves T. The rewrite-chain case instead publishes R2,
//! which supersedes R as a whole (the successor a second erasure writes),
//! folds a HEAD naming R2's part, deletes R and P, then resolves T.
//!
//! ravel-catalog has no helper that turns a resolved set into rows, so set
//! membership is the evidence: A in the set means S's row is served. Every
//! final resolve also asserts it issued exactly two GETs of A's commit key:
//! the record-cache hit is re-validated against the store, and the record is
//! absent after the one NotFound retry, so the token takes the swept-record
//! fallback.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use ravel_catalog::{
    Catalog, CatalogConfig, CatalogError, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_FOLD_SAFETY_MARGIN_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS, Snapshot,
};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{erasure, keys, signal};
use ravel_cpu_gate::MonotonicClock;
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Sequence, SequenceStep};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord,
    ErasurePredicateMatcher, ErasureRequest, RewriteDrop, RewriteRecord,
};
use ravel_segment::VERSION_V7;
use ravel_types::{CommitToken, Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const HOUR: u32 = 500_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;
/// ravel-maintain's default protection horizon: max_query_duration (1 h) +
/// grace (24 h) + clock_skew_allowance (5 min).
const PROTECTION_HORIZON_NS: i64 = 25 * NS_PER_HOUR + 5 * 60_000_000_000;
/// Rows A carries: one of subject S, one not matching S.
const A_ROWS: u64 = 2;
/// Rows the rewrite keeps: the one not matching S.
const SURVIVING_ROWS: u64 = 1;
/// Matching GETs of A's commit key the probe sequence counts before it stops
/// counting; far above what any resolve here issues.
const PROBE_STEPS: usize = 64;
/// GETs of A's commit key a token read issues once A's record is swept: the
/// re-validation of the cached record and its one NotFound retry.
const SWEPT_RECORD_GETS: u64 = 2;

#[derive(Default)]
struct ManualMonoClock(AtomicU64);

impl MonotonicClock for ManualMonoClock {
    fn now_nanos(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn tenant() -> TenantHash {
    TenantHash([0x45; 16])
}

fn hour_start() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn hour_range() -> TimeRange {
    TimeRange {
        start_ns: hour_start(),
        end_ns: hour_start() + NS_PER_HOUR,
    }
}

/// While the hour is open: the bucket is listed.
fn live_now() -> i64 {
    hour_start() + 30 * 60_000_000_000
}

/// The hour sealed: folded HEAD serves it. Q completes here.
fn sealed_now() -> i64 {
    i64::from(HOUR + 1) * NS_PER_HOUR + MARGIN_NS + 1
}

/// Past Q's protection horizon, when the sweep may delete its `.dreq`.
fn after_horizon_now() -> i64 {
    sealed_now() + PROTECTION_HORIZON_NS + 1
}

fn catalog_on(store: Arc<dyn ObjectStoreBackend>) -> Catalog {
    Catalog::new(
        store,
        CatalogConfig {
            shard_count: 1,
            ..Default::default()
        },
    )
    .expect("catalog")
}

fn l0_a(signal: Signal) -> CommitRecord {
    record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal,
        shard: 0,
        writer_id: Uuid::from_u128(0x4570_0000_0001),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: 100,
        content_hash: [0xa1; 32],
        sample_count: A_ROWS,
        series_count: 2,
        min_event_ts_ns: hour_start(),
        max_event_ts_ns: hour_start() + 100,
        min_ingest_ts_ns: hour_start(),
        max_ingest_ts_ns: hour_start() + 100,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: hour_start() + 1_000,
        ingest_hour_bucket: HOUR,
    })
    .expect("valid record")
}

fn identity(record: &CommitRecord) -> CompactionInputIdentity {
    CompactionInputIdentity {
        writer_id: record.writer_id.clone(),
        writer_epoch: record.writer_epoch,
        writer_seq: record.writer_seq,
    }
}

/// The identity the resolver gives a rewrite's output part
/// (`build_rewrite_l1_segment_ref`): a part has no writer of its own.
fn rewrite_output_identity() -> CompactionInputIdentity {
    CompactionInputIdentity {
        writer_id: Uuid::nil().to_string(),
        writer_epoch: 0,
        writer_seq: 0,
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
        min_event_ts_ns: hour_start(),
        max_event_ts_ns: hour_start() + 100,
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

fn rewrite_part_key(record: &RewriteRecord) -> String {
    keys::reconstruct_rewrite_part_key(record, &record.parts[0]).expect("rewrite part key")
}

fn part_key(record: &CompactionRecord) -> String {
    keys::reconstruct_l1_part_key(record, &record.parts[0]).expect("part key")
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

async fn put(store: &dyn ObjectStoreBackend, key: &str, body: Vec<u8>) {
    store
        .put(key, body.into(), PutOptions::create_if_absent())
        .await
        .expect("put");
}

async fn delete(store: &dyn ObjectStoreBackend, key: &str) {
    store.delete(key).await.expect("delete");
}

fn request(signal: Signal, request_id: Uuid) -> ErasureRequest {
    ErasureRequest {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        request_id: request_id.to_string(),
        created_unix_ns: hour_start() + 4_000,
        predicate: vec![ErasurePredicateMatcher {
            key: "user".to_string(),
            value: "s-4570".to_string(),
        }],
        window_start_ns: 0,
        window_end_ns: 0,
        reason: String::new(),
    }
}

/// Rewrite R over {A} applying `request_id`, keeping the row not matching S.
fn rewrite(signal: Signal, a: &CommitRecord, request_id: Uuid) -> RewriteRecord {
    let request_id = request_id.to_string();
    let inputs = vec![identity(a)];
    RewriteRecord {
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
        parts: vec![part(0xe0, SURVIVING_ROWS)],
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: A_ROWS - SURVIVING_ROWS,
        }],
        created_unix_ns: hour_start() + 5_000,
        superseded_record_key: String::new(),
    }
}

/// Compaction C of R's output part P into P2, naming P by the identity the
/// resolver gives a rewrite part.
fn compaction_of_rewrite_output(signal: Signal) -> CompactionRecord {
    let inputs = vec![rewrite_output_identity()];
    CompactionRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        level: 1,
        input_set_hash: erasure::compute_compaction_input_set_hash(&inputs).to_vec(),
        inputs,
        parts: vec![part(0xc2, SURVIVING_ROWS)],
        created_unix_ns: hour_start() + 6_000,
        superseded_record_key: String::new(),
    }
}

/// R2: a second erasure rewrite superseding R as a whole, with part P3.
fn superseding_rewrite(signal: Signal, r_key: &str, request_id: Uuid) -> RewriteRecord {
    let request_id = request_id.to_string();
    RewriteRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(signal) as i32,
        shard: 0,
        ingest_hour_bucket: HOUR,
        input_set_hash: erasure::compute_rewrite_input_set_hash(
            &[],
            Some(r_key),
            std::slice::from_ref(&request_id),
        )
        .to_vec(),
        inputs: Vec::new(),
        parts: vec![part(0xe3, SURVIVING_ROWS)],
        drops: vec![RewriteDrop {
            request_id,
            dropped_count: 0,
        }],
        created_unix_ns: hour_start() + 7_000,
        superseded_record_key: r_key.to_string(),
    }
}

/// The warm query node and the store it reads, which counts every GET of A's
/// commit key.
struct Node {
    inner: Arc<MemoryStore>,
    store: Arc<FaultStore<Arc<MemoryStore>>>,
    clock: Arc<ManualMonoClock>,
    catalog: Catalog,
    signal: Signal,
    a: CommitRecord,
    r: RewriteRecord,
    token_read_gets: Cell<u64>,
}

impl Node {
    fn a_record_gets(&self) -> u64 {
        self.store.sequence_progress(0)
    }

    async fn fold(&self, now: i64) {
        catalog_on(self.inner.clone())
            .fold(&tenant(), self.signal, Uuid::new_v4(), now, &[], None)
            .await
            .expect("fold");
    }

    /// Resolve on a fresh catalog: what a node that never cached A sees.
    async fn cold_plain_resolve(&self, now: i64) -> Snapshot {
        catalog_on(self.inner.clone())
            .resolve(&tenant(), self.signal, hour_range(), &[], now)
            .await
            .expect("cold resolve")
    }

    /// Resolve T on the warm node, after its HEAD cache has expired, noting
    /// the GETs of A's commit key it issued for
    /// [`Self::assert_revalidated`].
    async fn warm_token_resolve_result(&self) -> Result<Snapshot, CatalogError> {
        self.clock.0.fetch_add(
            u64::try_from(NS_PER_HOUR).expect("positive"),
            Ordering::SeqCst,
        );
        let before = self.a_record_gets();
        let answer = self
            .catalog
            .resolve(
                &tenant(),
                self.signal,
                hour_range(),
                &[token(&self.a)],
                after_horizon_now(),
            )
            .await;
        self.token_read_gets.set(self.a_record_gets() - before);
        answer
    }

    /// The last token read re-validated A's cached record against the store:
    /// the GET and its one NotFound retry.
    fn assert_revalidated(&self) {
        assert_eq!(
            self.token_read_gets.get(),
            SWEPT_RECORD_GETS,
            "the token step re-validated A's cached record and found it swept"
        );
    }

    async fn warm_token_resolve(&self) -> Snapshot {
        self.warm_token_resolve_result()
            .await
            .expect("warm token resolve")
    }
}

/// Runs everything up to X1's final resolve: A written and cached on the warm
/// node, Q completed, A swept, Q's `.dreq` deleted past the horizon.
async fn erased_and_swept(signal: Signal) -> Node {
    let inner = Arc::new(MemoryStore::new());
    let a = l0_a(signal);
    let probe = Sequence::new(Op::Get)
        .with_key_contains(commit_key(&a))
        .with_steps(vec![SequenceStep::Passthrough; PROBE_STEPS]);
    let store = Arc::new(FaultStore::new(
        inner.clone(),
        FaultPlan::empty().with_sequence(probe),
    ));
    let clock = Arc::new(ManualMonoClock::default());
    let catalog = catalog_on(store.clone()).with_monotonic_clock(clock.clone());

    put(inner.as_ref(), &commit_key(&a), record::encode(&a).to_vec()).await;
    put(
        inner.as_ref(),
        &l0_key(&a),
        b"A: row of S, row not of S".to_vec(),
    )
    .await;

    let warm = catalog
        .resolve(&tenant(), signal, hour_range(), &[token(&a)], live_now())
        .await
        .expect("warm-up resolve");
    assert_eq!(sorted_keys(&warm), vec![l0_key(&a)]);
    assert!(
        store.sequence_progress(0) >= 1,
        "the warm-up read A's commit record into the record cache"
    );

    let q = Uuid::from_u128(0x4570_d0e5);
    let dreq_key = keys::erasure_request_key(&tenant(), signal, q).expect("dreq key");
    put(
        inner.as_ref(),
        &dreq_key,
        erasure::encode_request(&request(signal, q)).to_vec(),
    )
    .await;
    let r = rewrite(signal, &a, q);
    put(
        inner.as_ref(),
        &keys::rewrite_record_key_for(&r).expect("rewrite key"),
        erasure::encode_rewrite(&r).to_vec(),
    )
    .await;
    put(
        inner.as_ref(),
        &rewrite_part_key(&r),
        b"P: row not of S".to_vec(),
    )
    .await;

    let node = Node {
        inner,
        store,
        clock,
        catalog,
        signal,
        a,
        r,
        token_read_gets: Cell::new(0),
    };
    node.fold(sealed_now()).await;

    // The sweep, then completion, then the `.dreq` past the horizon.
    delete(node.inner.as_ref(), &commit_key(&node.a)).await;
    delete(node.inner.as_ref(), &l0_key(&node.a)).await;
    put(
        node.inner.as_ref(),
        &keys::erasure_completion_key(&tenant(), signal, q).expect("done key"),
        b"done".to_vec(),
    )
    .await;
    delete(node.inner.as_ref(), &dreq_key).await;

    let plain = node.cold_plain_resolve(after_horizon_now()).await;
    assert_eq!(
        sorted_keys(&plain),
        vec![rewrite_part_key(&node.r)],
        "HEAD names P and not A"
    );
    assert!(plain.pending_erasure.is_empty(), "Q is no longer pending");
    node
}

async fn x1_record_cache_hit_after_horizon(signal: Signal) {
    let node = erased_and_swept(signal).await;
    let snapshot = node.warm_token_resolve().await;
    assert!(
        snapshot.pending_erasure.is_empty(),
        "no predicate would filter S"
    );
    let keys = sorted_keys(&snapshot);
    assert!(
        keys.contains(&rewrite_part_key(&node.r)),
        "P serves T's surviving row: {keys:?}"
    );
    assert!(
        !keys.contains(&l0_key(&node.a)),
        "A, which holds S's erased row, is served from the cached pre-erasure record: {keys:?}"
    );
    node.assert_revalidated();
}

async fn x2_rewrite_output_compacted(signal: Signal) {
    let node = erased_and_swept(signal).await;
    let c = compaction_of_rewrite_output(signal);
    put(
        node.inner.as_ref(),
        &keys::compaction_record_key_for(&c).expect("compaction key"),
        record::encode_compaction(&c).to_vec(),
    )
    .await;
    put(
        node.inner.as_ref(),
        &part_key(&c),
        b"P2: row not of S".to_vec(),
    )
    .await;
    delete(
        node.inner.as_ref(),
        &keys::rewrite_record_key_for(&node.r).expect("rewrite key"),
    )
    .await;
    delete(node.inner.as_ref(), &rewrite_part_key(&node.r)).await;
    // A later fold, so the reconcile pass revisits the already-folded hour.
    node.fold(after_horizon_now()).await;

    let plain = node.cold_plain_resolve(after_horizon_now()).await;
    assert_eq!(
        sorted_keys(&plain),
        vec![part_key(&c)],
        "HEAD names P2 alone"
    );

    // No production writer creates this state: compaction refuses a bucket
    // with a live rewrite record, so only "A is not served" is asserted.
    match node.warm_token_resolve_result().await {
        Ok(snapshot) => {
            let keys = sorted_keys(&snapshot);
            assert!(
                !keys.contains(&l0_key(&node.a)),
                "A, which holds S's erased row, is not served: {keys:?}"
            );
        }
        Err(CatalogError::UnsatisfiableToken { writer_id, seq, .. }) => {
            assert_eq!(
                (writer_id, seq),
                (node.a.writer_id.clone(), node.a.writer_seq)
            );
        }
        Err(other) => panic!("resolve failed for another reason: {other}"),
    }
    node.assert_revalidated();
}

async fn rewrite_chain(signal: Signal) {
    let node = erased_and_swept(signal).await;
    let r_key = keys::rewrite_record_key_for(&node.r).expect("rewrite key");
    let r2 = superseding_rewrite(signal, &r_key, Uuid::from_u128(0x4570_d0e6));
    put(
        node.inner.as_ref(),
        &keys::rewrite_record_key_for(&r2).expect("rewrite key"),
        erasure::encode_rewrite(&r2).to_vec(),
    )
    .await;
    put(
        node.inner.as_ref(),
        &rewrite_part_key(&r2),
        b"P3: row not of S".to_vec(),
    )
    .await;
    node.fold(after_horizon_now()).await;
    delete(node.inner.as_ref(), &r_key).await;
    delete(node.inner.as_ref(), &rewrite_part_key(&node.r)).await;

    let plain = node.cold_plain_resolve(after_horizon_now()).await;
    assert_eq!(
        sorted_keys(&plain),
        vec![rewrite_part_key(&r2)],
        "HEAD names R2's part alone"
    );

    let snapshot = node.warm_token_resolve().await;
    let keys = sorted_keys(&snapshot);
    assert!(
        !keys.contains(&l0_key(&node.a)),
        "A, which holds S's erased row, is not served: {keys:?}"
    );
    assert_eq!(
        keys,
        vec![rewrite_part_key(&r2)],
        "R2's part serves T's surviving row"
    );
    node.assert_revalidated();
}

#[tokio::test]
async fn metrics_x1_record_cache_hit_after_horizon() {
    x1_record_cache_hit_after_horizon(Signal::Metrics).await;
}

#[tokio::test]
async fn logs_x1_record_cache_hit_after_horizon() {
    x1_record_cache_hit_after_horizon(Signal::Logs).await;
}

#[tokio::test]
async fn spans_x1_record_cache_hit_after_horizon() {
    x1_record_cache_hit_after_horizon(Signal::Spans).await;
}

#[tokio::test]
async fn metrics_x2_rewrite_output_compacted() {
    x2_rewrite_output_compacted(Signal::Metrics).await;
}

#[tokio::test]
async fn logs_x2_rewrite_output_compacted() {
    x2_rewrite_output_compacted(Signal::Logs).await;
}

#[tokio::test]
async fn spans_x2_rewrite_output_compacted() {
    x2_rewrite_output_compacted(Signal::Spans).await;
}

#[tokio::test]
async fn metrics_rewrite_chain() {
    rewrite_chain(Signal::Metrics).await;
}

#[tokio::test]
async fn logs_rewrite_chain() {
    rewrite_chain(Signal::Logs).await;
}

#[tokio::test]
async fn spans_rewrite_chain() {
    rewrite_chain(Signal::Spans).await;
}
