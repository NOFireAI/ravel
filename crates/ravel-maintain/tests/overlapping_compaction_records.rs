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
//! Both fixtures drive the production entries (`sweep_superseded`,
//! `sweep_unreferenced_parts`, `bucket_erasure_completion`) against a
//! `MemoryStore`. Each test's doc comment names the assertion that fails
//! without the fix.
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use prost::Message;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::{erasure, keys, signal};
use ravel_maintain::config::DEFAULT_MAX_INGEST_LAG_NS;
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, NoLeases, PendingErasureRequest,
    RetentionConfig, RetentionPolicy, bucket_erasure_completion, compact_bucket, scan_and_maintain,
    sweep_superseded, sweep_unreferenced_parts,
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

/// The exact set of part object keys the single rewrite record in `bucket`
/// names, reconstructed from the stored record (ADR-0010 §7 key discipline).
async fn rewrite_part_keys(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> BTreeSet<String> {
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .expect("prefix");
    let mut rkeys: Vec<String> = list_all(store, &prefix)
        .await
        .expect("list")
        .into_iter()
        .map(|m| m.key)
        .filter(|k| {
            matches!(
                keys::partition_bucket_entry(k),
                Ok(keys::BucketEntry::RewriteRecord(_))
            )
        })
        .collect();
    rkeys.sort();
    assert_eq!(rkeys.len(), 1, "expected exactly one rewrite record");
    let bytes = get_full(store, &rkeys[0]).await;
    let record = RewriteRecord::decode(bytes.as_ref()).expect("decode rewrite record");
    record
        .parts
        .iter()
        .map(|p| keys::reconstruct_rewrite_part_key(&record, p).expect("rewrite part key"))
        .collect()
}

/// Number of compaction records present in `bucket`.
async fn compaction_record_count(store: &dyn ObjectStoreBackend, bucket: &Bucket) -> usize {
    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .expect("prefix");
    list_all(store, &prefix)
        .await
        .expect("list")
        .into_iter()
        .filter(|m| {
            matches!(
                keys::partition_bucket_entry(&m.key),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        })
        .count()
}

/// After compaction refuses a bucket holding a live erasure rewrite record, the
/// catalog serves EXACTLY the rewrite's part set (ADR-0064 decision 3 point 5).
/// This is the whole point of the producer-side refusal: had compaction run, the
/// bucket would hold both a compaction record and the rewrite record over the
/// same L0 inputs, and the resolver would serve both part sets, resurrecting the
/// records the rewrite dropped.
///
/// The exact key-set assertion (never a count) pins that no raw L0 leaks in and
/// the compaction's would-be parts are absent. `sweep_unreferenced_parts`
/// returning 0 pins that the rewrite's parts stay referenced, and the zero
/// compaction-record assertion pins the refusal itself.
#[tokio::test]
async fn resolve_serves_exactly_one_part_set_after_a_rewrite() {
    let store = Arc::new(MemoryStore::new());
    let b = seed_rlog_two_inputs(store.as_ref()).await;
    let base = hour_ns();

    // A RawL0 rewrite record superseding both L0 inputs (writers 1 and 2, epoch
    // 10, seq 1 and 2, as seed_rlog_two_inputs seeds them). Its single part
    // covers rows inside the hour.
    let request_id = Uuid::from_u128(0x0EEE);
    put_rewrite_record(
        store.as_ref(),
        &b,
        vec![
            input_identity(Uuid::from_u128(1), 10, 1),
            input_identity(Uuid::from_u128(2), 10, 2),
        ],
        "",
        vec![part(0x77, base + 1_000, base + 2_000)],
        &[request_id],
        base + 2_000_000,
    )
    .await;

    // Compaction refuses: the bucket already holds a live rewrite record.
    let clock = FixedClock::new(sealed_now_ns());
    let outcome = compact_bucket(store.as_ref(), &clock, &cfg(), &b)
        .await
        .expect("compact");
    assert_eq!(outcome, CompactionOutcome::RewritePresent);
    assert_eq!(
        compaction_record_count(store.as_ref(), &b).await,
        0,
        "the refusal published no compaction record"
    );

    // The catalog serves EXACTLY the rewrite's part set — one record set, not
    // two. The rewrite's inputs (both L0s) are excluded, so no raw L0 leaks in.
    let expected = rewrite_part_keys(store.as_ref(), &b).await;
    let catalog = Catalog::new(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        CatalogConfig {
            shard_count: SHARD + 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let served: BTreeSet<String> = catalog
        .resolve(
            &tenant_hash(),
            Signal::Logs,
            TimeRange {
                start_ns: base,
                end_ns: base + NS_PER_HOUR,
            },
            &[],
            sealed_now_ns(),
        )
        .await
        .expect("resolve")
        .segments
        .into_iter()
        .map(|s| s.data_object_key)
        .collect();
    assert_eq!(
        served, expected,
        "the bucket serves exactly one part set: the rewrite's"
    );

    // The rewrite's parts are referenced by the rewrite record, so the
    // unreferenced-part sweep collects nothing.
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
    .expect("sweep unreferenced parts");
    assert_eq!(collected, 0, "the rewrite's parts stay referenced");
}

/// Reachability: the server maintenance tick (`run_tick_with_clock`, which
/// drives `scan_and_maintain_with_memo`) leaves a bucket holding a live rewrite
/// record uncompacted. Driven through `scan_and_maintain`, the same maintain
/// entry point the tick calls, so the new refusal is exercised on the reachable
/// production path without editing the server file.
#[tokio::test]
async fn server_tick_leaves_a_rewritten_bucket_uncompacted() {
    let store = Arc::new(MemoryStore::new());
    let b = seed_rlog_two_inputs(store.as_ref()).await;
    let base = hour_ns();
    put_rewrite_record(
        store.as_ref(),
        &b,
        vec![
            input_identity(Uuid::from_u128(1), 10, 1),
            input_identity(Uuid::from_u128(2), 10, 2),
        ],
        "",
        vec![part(0x88, base + 1_000, base + 2_000)],
        &[Uuid::from_u128(0x0EEF)],
        base + 2_000_000,
    )
    .await;

    // No retention policy, so retention leaves the bucket live and the tick's
    // compaction stage runs.
    let config = cfg();
    let retention = RetentionConfig::from_policy(
        RetentionPolicy {
            default: None,
            tenants: Vec::new(),
        },
        &config,
        DEFAULT_MAX_INGEST_LAG_NS,
    )
    .expect("retention config");
    let clock = FixedClock::new(sealed_now_ns());
    let report = scan_and_maintain(
        store.as_ref(),
        &clock,
        &config,
        &retention,
        &NoLeases,
        b.tenant_hash,
        b.signal,
        b.shard,
    )
    .await
    .expect("maintain tick");

    assert_eq!(
        report.compacted, 0,
        "the rewritten bucket must not be compacted"
    );
    assert_eq!(
        report.already_done, 1,
        "the rewritten bucket is counted done via the refusal"
    );
    assert_eq!(
        compaction_record_count(store.as_ref(), &b).await,
        0,
        "the tick published no compaction record over the rewritten inputs"
    );
}
