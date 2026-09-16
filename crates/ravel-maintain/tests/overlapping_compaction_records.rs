//! The superseded-input sweep and the erasure completion gate follow the same
//! authoritative-record rule the resolver does (issue #1070).
//!
//! When one sealed bucket holds two compaction records whose input sets
//! overlap, the catalog picks one authoritative record per overlap component
//! and serves any input the winner does not name as a raw L0 segment. An input
//! only a LOSER names is therefore the sole server of its rows, and both
//! maintenance paths here have to see it that way: the sweep must not delete
//! it as superseded, and the completion gate must not treat it as invisible.
//!
//! The same "one bucket, one record set" rule constrains the producer side:
//! a bucket already holding a live erasure rewrite record must never gain a
//! compaction record over the same inputs, because a rewrite's outputs
//! deliberately lack records its inputs contain (ADR-0064 decision 3 point 5)
//! and a snapshot naming both part sets resurrects the erased records. The
//! last two fixtures pin that refusal, in `compact_bucket` and in the
//! maintenance pass the server's tick runs.
//!
//! Every fixture drives the production entries (`sweep_superseded`,
//! `sweep_unreferenced_parts`, `bucket_erasure_completion`, `compact_bucket`,
//! `scan_and_maintain_with_memo`) against a `MemoryStore`. Each test's doc
//! comment names the assertion that fails without the fix.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use prost::Message;
use ravel_commit::{erasure, keys, signal};
use ravel_maintain::read::list_bucket;
use ravel_maintain::scan::{MaintainMemo, TerminalState, scan_and_maintain_with_memo};
use ravel_maintain::{
    Bucket, Clock, CompactionOutcome, CompactorConfig, FixedClock, NoLeases, PendingErasureRequest,
    RetentionConfig, bucket_erasure_completion, compact_bucket, sweep_superseded,
    sweep_unreferenced_parts,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{
    CompactionInputIdentity, CompactionPart, CompactionRecord, ErasurePredicateMatcher,
    ErasureRequest, RewriteDrop, RewriteRecord,
};
use ravel_types::{Signal, TimeRange};
use uuid::Uuid;

/// Event-time base of the logs bucket the fixtures use.
fn hour_ns() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn cfg() -> CompactorConfig {
    CompactorConfig::default()
}

/// Well past every record's protection horizon, so a deleting sweep pass is
/// not gated on age.
fn past_horizon_ns() -> i64 {
    hour_ns() + cfg().protection_horizon_ns + NS_PER_HOUR
}

/// Every object key in the store, for exact before/after set comparisons.
async fn all_keys(store: &dyn ObjectStoreBackend) -> BTreeSet<String> {
    list_all(store, "t/")
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .collect()
}

/// The identity of a seeded L0 input, as a compaction record names it.
fn input_identity(writer_id: Uuid, epoch: u64, seq: u64) -> CompactionInputIdentity {
    CompactionInputIdentity {
        writer_id: writer_id.to_string(),
        writer_epoch: epoch,
        writer_seq: seq,
    }
}

/// An L1 part covering `[min_ts, max_ts]`. `seed` distinguishes two records'
/// part objects by content hash, so their reconstructed keys differ.
fn part(seed: u8, min_ts: i64, max_ts: i64) -> CompactionPart {
    CompactionPart {
        part_index: 0,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xff; 16],
        content_hash: vec![seed; 32],
        object_size: 4096,
        sample_count: 2,
        series_count: 1,
        run_count: 1,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Vec::new(),
    }
}

/// Publish a compaction record over `inputs` into `bucket`, plus a real object
/// at each of its parts' reconstructed keys so the unreferenced-part sweep has
/// something to list. `seed` drives the record's `input_set_hash` (and so its
/// key and the resolver's hash tie-break) independently of `inputs`. Returns
/// the record key and the record.
async fn put_compaction_record(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    inputs: Vec<CompactionInputIdentity>,
    parts: Vec<CompactionPart>,
    created_unix_ns: i64,
    seed: &[u8],
) -> (String, CompactionRecord) {
    let input_set_hash = *blake3::hash(seed).as_bytes();
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal: signal::to_proto(bucket.signal) as i32,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        level: 1,
        inputs,
        input_set_hash: input_set_hash.to_vec(),
        parts,
        created_unix_ns,
    };
    for p in &record.parts {
        let part_key = keys::reconstruct_l1_part_key(&record, p).expect("part key");
        store
            .put(
                &part_key,
                bytes::Bytes::from_static(b"l1-part"),
                PutOptions::default(),
            )
            .await
            .expect("put part object");
    }
    let hash16 = hex_prefix(&input_set_hash);
    let key = keys::compaction_record_key(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
        &hash16,
    )
    .expect("record key");
    store
        .put(
            &key,
            record.encode_to_vec().into(),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put compaction record");
    (key, record)
}

fn hex_prefix(hash: &[u8; 32]) -> String {
    hash[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// Publish a rewrite record into `bucket`. Exactly one of `inputs` (raw-L0
/// supersession) and `superseded_record_key` (whole-record supersession) is
/// set, as the format requires. Every applied request id lands in `drops`, so
/// the completion gate treats the output as safe for those requests.
#[allow(clippy::too_many_arguments)]
async fn put_rewrite_record(
    store: &dyn ObjectStoreBackend,
    bucket: &Bucket,
    inputs: Vec<CompactionInputIdentity>,
    superseded_record_key: &str,
    parts: Vec<CompactionPart>,
    applied: &[Uuid],
    created_unix_ns: i64,
) -> String {
    let request_ids: Vec<String> = applied.iter().map(Uuid::to_string).collect();
    let superseded = if superseded_record_key.is_empty() {
        None
    } else {
        Some(superseded_record_key)
    };
    let input_set_hash = erasure::compute_rewrite_input_set_hash(&inputs, superseded, &request_ids);
    let record = RewriteRecord {
        format_version: 1,
        tenant_hash: bucket.tenant_hash.0.to_vec(),
        signal: signal::to_proto(bucket.signal) as i32,
        shard: bucket.shard,
        ingest_hour_bucket: bucket.ingest_hour_bucket,
        inputs,
        input_set_hash: input_set_hash.to_vec(),
        parts,
        drops: request_ids
            .iter()
            .map(|id| RewriteDrop {
                request_id: id.clone(),
                dropped_count: 1,
            })
            .collect(),
        created_unix_ns,
        superseded_record_key: superseded_record_key.to_string(),
    };
    for p in &record.parts {
        let part_key = keys::reconstruct_rewrite_part_key(&record, p).expect("rewrite part key");
        store
            .put(
                &part_key,
                bytes::Bytes::from_static(b"rw-part"),
                PutOptions::default(),
            )
            .await
            .expect("put rewrite part object");
    }
    let key = keys::rewrite_record_key_for(&record).expect("rewrite record key");
    store
        .put(
            &key,
            record.encode_to_vec().into(),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put rewrite record");
    key
}

/// The data-object key of the L0 input at `commit_key`.
async fn data_key_of(store: &dyn ObjectStoreBackend, commit_key: &str) -> String {
    let bytes = get_full(store, commit_key).await;
    let record = ravel_commit::record::decode(&bytes).expect("decode commit record");
    keys::reconstruct_data_key(&record).expect("data key")
}

/// Order two seeds by their `input_set_hash`: `(smaller, larger)`. Between two
/// records of equal input-set cardinality the smaller hash wins.
fn order_by_hash<'a>(x: &'a [u8], y: &'a [u8]) -> (&'a [u8], &'a [u8]) {
    if blake3::hash(x).as_bytes() <= blake3::hash(y).as_bytes() {
        (x, y)
    } else {
        (y, x)
    }
}

/// The superseded-input sweep deletes only what an AUTHORITATIVE compaction
/// record supersedes. The bucket holds two overlapping records: the winner
/// names `[a, b]` and the loser names `[a, d]`, so `d` is served as a raw L0
/// and must survive. The clock is past the protection horizon and no HEAD
/// object exists, so the reachability gate clears everything the pass gathers.
///
/// The failing assertion before the fix is the first one: the sweep gathers
/// every compaction record's inputs, so it deletes `d`'s data object and
/// commit record too, and the surviving-key set does not contain them
/// (`records_deleted` is 3 rather than 2). Turning the sole server of `d`'s
/// rows into a missing object is the finding this test pins.
///
/// The two rule-3 assertions at the end pin the honest current behaviour: the
/// unreferenced-part sweep marks EVERY compaction record's parts as
/// referenced, so the loser's part survives while the loser's record does, and
/// is collected on the pass after that record is gone.
#[tokio::test]
async fn sweep_keeps_the_input_only_a_losing_record_names() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();

    let a_writer = Uuid::from_u128(0xA1);
    let b_writer = Uuid::from_u128(0xA2);
    let d_writer = Uuid::from_u128(0xA3);
    let a_commit = seed_rlog_input(
        store.as_ref(),
        a_writer,
        1,
        1,
        &[log_record(1, base + 1_000, "a")],
    )
    .await;
    let b_commit = seed_rlog_input(
        store.as_ref(),
        b_writer,
        1,
        2,
        &[log_record(1, base + 2_000, "b")],
    )
    .await;
    let d_commit = seed_rlog_input(
        store.as_ref(),
        d_writer,
        1,
        3,
        &[log_record(2, base + 3_000, "d")],
    )
    .await;
    let a_data = data_key_of(store.as_ref(), &a_commit).await;
    let b_data = data_key_of(store.as_ref(), &b_commit).await;
    let d_data = data_key_of(store.as_ref(), &d_commit).await;

    // Equal input-set cardinality, so the hash decides. The winner names
    // [a, b]; the loser names [a, d], overlapping on a and adding d.
    let (win_seed, lose_seed) = order_by_hash(b"sweep-win", b"sweep-lose");
    put_compaction_record(
        store.as_ref(),
        &b,
        vec![
            input_identity(a_writer, 1, 1),
            input_identity(b_writer, 1, 2),
        ],
        vec![part(0x11, base + 1_000, base + 2_000)],
        base,
        win_seed,
    )
    .await;
    let (loser_key, loser) = put_compaction_record(
        store.as_ref(),
        &b,
        vec![
            input_identity(a_writer, 1, 1),
            input_identity(d_writer, 1, 3),
        ],
        vec![part(0x22, base + 1_000, base + 3_000)],
        base,
        lose_seed,
    )
    .await;
    let loser_part_key =
        keys::reconstruct_l1_part_key(&loser, &loser.parts[0]).expect("loser part key");

    let before = all_keys(store.as_ref()).await;
    let clock = FixedClock::new(past_horizon_ns());
    let outcome = sweep_superseded(
        store.as_ref(),
        &clock,
        &cfg(),
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("sweep");

    let after = all_keys(store.as_ref()).await;
    let deleted: BTreeSet<String> = before.difference(&after).cloned().collect();
    assert_eq!(
        deleted,
        BTreeSet::from([
            a_commit.clone(),
            a_data.clone(),
            b_commit.clone(),
            b_data.clone()
        ]),
        "only the winner's inputs are superseded"
    );
    assert!(
        after.contains(&d_commit) && after.contains(&d_data),
        "the input only the loser names still serves its rows"
    );
    assert_eq!(outcome.records_deleted, 2);
    assert_eq!(outcome.data_deleted, 2);
    assert_eq!(outcome.held_by_snapshot, 0);
    assert_eq!(outcome.held_by_unreadable_head, 0);

    // Rule 3 while the loser's record is present: its parts are referenced by
    // that record, so nothing is collected.
    let collected = sweep_unreferenced_parts(
        store.as_ref(),
        &clock,
        &cfg(),
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("rule 3");
    assert_eq!(collected, 0, "a record's own parts are referenced");

    // With the loser's record gone, its part is unreferenced and collected.
    store.delete(&loser_key).await.expect("delete loser record");
    let collected = sweep_unreferenced_parts(
        store.as_ref(),
        &clock,
        &cfg(),
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("rule 3");
    assert_eq!(collected, 1, "exactly the loser's one part");
    let after = all_keys(store.as_ref()).await;
    assert!(!after.contains(&loser_part_key));
}

/// An erasure request whose subject lives in a raw L0 only a LOSING compaction
/// record names is not complete until that L0 is superseded, and IS complete
/// once it has been.
///
/// The bucket holds the winner over `[a, b]`, whose part sits outside the
/// request window, and the loser over `[a, d]`, whose part covers `d`'s rows
/// inside the window. The resolver serves `d` as a raw L0, so while `d` is
/// there the subject is still returnable and the bucket is incomplete. The
/// first assertion pins that: without the `excluded` half of the fix, `d` is
/// invisible to the gate, and only the loser's part -- which no query reads --
/// keeps the answer accidentally right.
///
/// The failing assertion before the fix is the second phase's
/// `blocked.len() == 0`. Once a rewrite has superseded `d` and named the
/// request, nothing live serves the subject; but the gate kept the loser's
/// record in its live view, that record's part overlaps the window, and the
/// bucket reported blocked forever. A request that never completes retains its
/// `.dreq`, and its subject with it, which is the outcome ADR-0064 decision 5
/// exists to prevent.
#[tokio::test]
async fn erasure_completion_sees_the_l0_only_a_losing_record_names() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    let request_id = Uuid::from_u128(0x0E45);

    let a_writer = Uuid::from_u128(0xB1);
    let b_writer = Uuid::from_u128(0xB2);
    let d_writer = Uuid::from_u128(0xB3);
    // a and b carry no subject and sit early in the hour; d carries the
    // subject and sits inside the request window.
    seed_rlog_input(
        store.as_ref(),
        a_writer,
        1,
        1,
        &[log_record(1, base + 1_000, "keep")],
    )
    .await;
    seed_rlog_input(
        store.as_ref(),
        b_writer,
        1,
        2,
        &[log_record(1, base + 2_000, "keep")],
    )
    .await;
    seed_rlog_input(
        store.as_ref(),
        d_writer,
        1,
        3,
        &[log_record(2, base + 9_000, "victim")],
    )
    .await;

    let (win_seed, lose_seed) = order_by_hash(b"erase-win", b"erase-lose");
    put_compaction_record(
        store.as_ref(),
        &b,
        vec![
            input_identity(a_writer, 1, 1),
            input_identity(b_writer, 1, 2),
        ],
        vec![part(0x33, base + 1_000, base + 2_000)],
        base,
        win_seed,
    )
    .await;
    put_compaction_record(
        store.as_ref(),
        &b,
        vec![
            input_identity(a_writer, 1, 1),
            input_identity(d_writer, 1, 3),
        ],
        vec![part(0x44, base + 1_000, base + 9_000)],
        base,
        lose_seed,
    )
    .await;

    let pending = vec![PendingErasureRequest {
        request_key: keys::erasure_request_key(&b.tenant_hash, Signal::Logs, request_id)
            .expect("dreq key"),
        request: ErasureRequest {
            format_version: 1,
            tenant_hash: b.tenant_hash.0.to_vec(),
            signal: signal::to_proto(Signal::Logs) as i32,
            request_id: request_id.to_string(),
            created_unix_ns: base,
            predicate: vec![ErasurePredicateMatcher {
                key: "service.name".to_string(),
                value: "svc2".to_string(),
            }],
            window_start_ns: base + 5_000,
            window_end_ns: base + 20_000,
            reason: String::new(),
        },
    }];

    let clock = FixedClock::new(sealed_now_ns());
    let completion =
        bucket_erasure_completion(store.as_ref(), &clock, &cfg(), &NoLeases, &b, &pending)
            .await
            .expect("completion");
    assert!(
        completion.blocked.contains(&request_id.to_string()),
        "the raw L0 only the loser names still serves the subject"
    );
    assert_eq!(completion.blocked.len(), 1);
    assert!(!completion.unresolved);

    // Once a rewrite supersedes that raw L0 as well, nothing live serves the
    // subject and the bucket is complete.
    put_rewrite_record(
        store.as_ref(),
        &b,
        vec![input_identity(d_writer, 1, 3)],
        "",
        vec![part(0x66, base + 1_000, base + 2_000)],
        &[request_id],
        base + 2_000_000,
    )
    .await;
    let completion =
        bucket_erasure_completion(store.as_ref(), &clock, &cfg(), &NoLeases, &b, &pending)
            .await
            .expect("completion");
    assert_eq!(
        completion.blocked.len(),
        0,
        "the subject is provably gone from the bucket"
    );
    assert!(!completion.unresolved);
}

/// A bucket that already holds a live erasure rewrite record serves exactly
/// one part set, because compaction refuses it. The producer-side refusal is
/// what makes that true: a rewrite's outputs deliberately lack records its
/// inputs contain, so a compaction record over the same inputs is not
/// overlap-harmless against it (ADR-0064 decision 3 point 5), and a snapshot
/// naming both part sets resurrects the erased records.
///
/// The guarded production line is the `RewritePresent` early return in
/// `compact_bucket_scoped` (crates/ravel-maintain/src/compact.rs). With that
/// return removed, `compact_bucket` reports `Compacted { parts: 1, .. }`, the
/// bucket holds one compaction record, and the resolved key set is the union
/// of both record sets rather than the rewrite's parts alone.
#[tokio::test]
async fn resolve_serves_exactly_one_part_set_after_a_rewrite() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();
    let request_id = Uuid::from_u128(0x0E46);

    let a_writer = Uuid::from_u128(0xC1);
    let b_writer = Uuid::from_u128(0xC2);
    seed_rlog_input(
        store.as_ref(),
        a_writer,
        1,
        1,
        &[log_record(1, base + 1_000, "a")],
    )
    .await;
    seed_rlog_input(
        store.as_ref(),
        b_writer,
        1,
        2,
        &[log_record(1, base + 2_000, "b")],
    )
    .await;

    // The erasure pass has already rewritten both L0 inputs into one part.
    let rewrite_key = put_rewrite_record(
        store.as_ref(),
        &b,
        vec![
            input_identity(a_writer, 1, 1),
            input_identity(b_writer, 1, 2),
        ],
        "",
        vec![part(0x77, base + 1_000, base + 2_000)],
        &[request_id],
        base + 3_000,
    )
    .await;
    let rewrite: RewriteRecord =
        RewriteRecord::decode(get_full(store.as_ref(), &rewrite_key).await.as_ref())
            .expect("decode rewrite record");
    let rewrite_part_keys: BTreeSet<String> = rewrite
        .parts
        .iter()
        .map(|p| keys::reconstruct_rewrite_part_key(&rewrite, p).expect("rewrite part key"))
        .collect();
    assert_eq!(rewrite_part_keys.len(), 1, "the rewrite published one part");

    let clock = FixedClock::new(sealed_now_ns());
    let outcome = compact_bucket(store.as_ref(), &clock, &cfg(), &b)
        .await
        .expect("compact");
    assert_eq!(
        outcome,
        CompactionOutcome::RewritePresent,
        "compaction refuses the rewritten bucket"
    );

    // What a query would see: exactly the rewrite's parts, nothing else. An
    // exact key set, never a count: a second record set of the same size would
    // pass a count check.
    let dyn_store: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = ravel_catalog::Catalog::new(
        dyn_store,
        ravel_catalog::CatalogConfig {
            shard_count: SHARD + 1,
            ..Default::default()
        },
    )
    .expect("catalog");
    let snapshot = catalog
        .resolve(
            &b.tenant_hash,
            Signal::Logs,
            TimeRange {
                start_ns: base,
                end_ns: base + NS_PER_HOUR,
            },
            &[],
            clock.now_ns(),
        )
        .await
        .expect("resolve");
    let served: BTreeSet<String> = snapshot
        .segments
        .iter()
        .map(|s| s.data_object_key.clone())
        .collect();
    assert_eq!(
        served, rewrite_part_keys,
        "the resolved snapshot serves the rewrite's part set and nothing else"
    );

    // Nothing was published to collect, and nothing the rewrite owns is
    // unreferenced.
    let collected = sweep_unreferenced_parts(
        store.as_ref(),
        &clock,
        &cfg(),
        &NoLeases,
        &b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("rule 3");
    assert_eq!(
        collected, 0,
        "the rewrite's part is referenced by its own record"
    );
    let listing = list_bucket(store.as_ref(), &b).await.expect("list");
    assert_eq!(
        listing.compaction_record_keys.len(),
        0,
        "the bucket holds zero compaction records"
    );
    assert_eq!(listing.rewrite_record_keys, vec![rewrite_key]);
}

/// Reachability: the maintenance pass the server's tick runs per owned unit
/// (`scan_and_maintain_with_memo`, the single ravel-maintain entry point
/// `run_tick_with_clock` calls for compaction and retention) leaves a bucket
/// holding a live erasure rewrite record uncompacted, and records the
/// refusal in the memo as a terminal state.
///
/// The guarded production line is the same `RewritePresent` early return in
/// `compact_bucket_scoped`. With that return removed the pass reports
/// `compacted: 1` and the bucket ends the tick holding a compaction record
/// beside the rewrite record.
#[tokio::test]
async fn the_maintenance_tick_leaves_a_rewritten_bucket_uncompacted() {
    let store = Arc::new(MemoryStore::new());
    let b = logs_bucket();
    let base = hour_ns();

    let a_writer = Uuid::from_u128(0xD1);
    let b_writer = Uuid::from_u128(0xD2);
    seed_rlog_input(
        store.as_ref(),
        a_writer,
        1,
        1,
        &[log_record(1, base + 1_000, "a")],
    )
    .await;
    seed_rlog_input(
        store.as_ref(),
        b_writer,
        1,
        2,
        &[log_record(1, base + 2_000, "b")],
    )
    .await;
    put_rewrite_record(
        store.as_ref(),
        &b,
        vec![
            input_identity(a_writer, 1, 1),
            input_identity(b_writer, 1, 2),
        ],
        "",
        vec![part(0x88, base + 1_000, base + 2_000)],
        &[Uuid::from_u128(0x0E47)],
        base + 3_000,
    )
    .await;

    let clock = FixedClock::new(sealed_now_ns());
    let mut memo = MaintainMemo::new(0);
    let report = scan_and_maintain_with_memo(
        &mut memo,
        store.as_ref(),
        &clock,
        &cfg(),
        &RetentionConfig::default(),
        &NoLeases,
        b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("tick");

    assert_eq!(report.compacted, 0, "the tick compacts nothing");
    assert_eq!(
        report.already_done, 1,
        "the rewritten bucket is counted as needing no work"
    );
    let listing = list_bucket(store.as_ref(), &b).await.expect("list");
    assert_eq!(
        listing.compaction_record_keys.len(),
        0,
        "the tick published no second record set"
    );
    assert_eq!(listing.commit_keys.len(), 2, "both L0 inputs stay live");

    // The refusal is terminal, so the tick memoizes it: a live rewrite record
    // is never deleted and a sealed bucket's L0 set is frozen, so the state
    // cannot go stale, and a lost memo costs one re-list.
    assert_eq!(
        memo.terminal_state(b.tenant_hash, b.signal, b.shard, b.ingest_hour_bucket),
        Some(TerminalState::Compacted),
        "the tick memoizes the refusal as terminal"
    );
}
