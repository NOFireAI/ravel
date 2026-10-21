//! Query-worker record authentication (ADR-1918), through the public API
//! over `MemoryStore`: a sibling enters [`QueryWorkers::live_set`] only when
//! its record carries a MAC under a derived record key, and every other record
//! is skipped and counted by reason.

#![allow(clippy::expect_used)]

use std::time::Duration;

use ravel_fleet::query_workers::{
    MAX_RECORD_BYTES, NoRecordKeys, QueryWorkerRecord, QueryWorkers, RECORD_KEY_CONTEXT,
    RecordKeys, RecordReject, query_worker_key,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions};
use uuid::Uuid;

const H: Duration = Duration::from_secs(60);
const NOW: i64 = 1_000 * 60 * 1_000_000_000;
const KEY_A: [u8; 32] = [0xA1; 32];
const KEY_B: [u8; 32] = [0xB2; 32];

fn keys(file_keys: &[[u8; 32]]) -> RecordKeys {
    RecordKeys::from_file_keys(file_keys).expect("non-empty key list")
}

fn worker(file_keys: &[[u8; 32]]) -> QueryWorkers {
    QueryWorkers::new("10.0.0.1:9443", 5, keys(file_keys), H, 3)
}

async fn put(store: &MemoryStore, key: &str, bytes: Vec<u8>) {
    store
        .put(
            key,
            bytes.into(),
            PutOptions {
                mode: PutMode::Overwrite,
                checksum: None,
            },
        )
        .await
        .expect("put");
}

async fn object(store: &MemoryStore, worker: &QueryWorkers) -> Vec<u8> {
    store
        .get(
            &query_worker_key(&worker.process_id().to_string()),
            GetRange::Full,
        )
        .await
        .expect("get record")
        .data
        .to_vec()
}

fn ids(live: &[QueryWorkerRecord]) -> Vec<String> {
    live.iter().map(|r| r.process_id.clone()).collect()
}

/// The reader's reject counters, in `RecordReject::ALL` order:
/// `[missing_mac, bad_mac, oversize, malformed, stale_stamp]`.
fn rejects(reader: &QueryWorkers) -> [u64; RecordReject::ALL.len()] {
    reader.record_rejects().by_reason().map(|(_, n)| n)
}

/// The ADR-1918 claim, built here independently of the crate so the format
/// is pinned: u64-BE length + bytes of `process_id`, the same for
/// `fragment_endpoint`, then `protocol_version` u32 BE and `started_unix_ns`
/// i64 BE.
fn claim(record: &QueryWorkerRecord) -> Vec<u8> {
    let mut claim = Vec::new();
    for field in [&record.process_id, &record.fragment_endpoint] {
        claim.extend_from_slice(&(field.len() as u64).to_be_bytes());
        claim.extend_from_slice(field.as_bytes());
    }
    claim.extend_from_slice(&record.protocol_version.to_be_bytes());
    claim.extend_from_slice(&record.started_unix_ns.to_be_bytes());
    claim
}

fn envelope(record: &QueryWorkerRecord, mac: Option<String>) -> Vec<u8> {
    let mut value = serde_json::to_value(record).expect("record to json");
    if let Some(mac) = mac {
        value
            .as_object_mut()
            .expect("record is an object")
            .insert("mac".into(), serde_json::Value::String(mac));
    }
    serde_json::to_vec(&value).expect("encode envelope")
}

fn record_at(id: Uuid, endpoint: &str) -> QueryWorkerRecord {
    QueryWorkerRecord {
        process_id: id.to_string(),
        fragment_endpoint: endpoint.to_string(),
        protocol_version: 5,
        started_unix_ns: NOW,
    }
}

/// A self-consistent record with no MAC, written at a fresh UUID key by a
/// writer holding the prefix's `PutObject` grant but not the key file, does
/// not enter the live set, and is counted as `missing_mac`.
#[tokio::test]
async fn forged_unsigned_record_at_fresh_uuid_is_excluded() {
    let store = MemoryStore::new();
    let reader = worker(&[KEY_A]);
    let forged_id = Uuid::new_v4();
    let forged = record_at(forged_id, "10.9.9.9:9443");
    put(
        &store,
        &query_worker_key(&forged.process_id),
        envelope(&forged, None),
    )
    .await;

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(rejects(&reader), [1, 0, 0, 0, 0], "exactly one missing_mac");
}

/// A sibling signed under the shared key is admitted; the same object with
/// one hex digit of its MAC flipped is skipped as `bad_mac`.
#[tokio::test]
async fn signed_sibling_is_admitted_and_a_bad_mac_is_rejected() {
    let store = MemoryStore::new();
    let reader = worker(&[KEY_A]);
    let sibling = worker(&[KEY_A]);
    sibling.write_heartbeat(&store, NOW).await.expect("beat");

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert!(ids(&live).contains(&sibling.process_id().to_string()));
    assert_eq!(live.len(), 2);
    assert_eq!(rejects(&reader), [0, 0, 0, 0, 0]);

    let mut value: serde_json::Value =
        serde_json::from_slice(&object(&store, &sibling).await).expect("decode");
    let mac = value["mac"].as_str().expect("mac present").to_string();
    assert_eq!(mac.len(), 64, "the MAC is 64 hex characters");
    let first = mac.chars().next().expect("non-empty");
    let flipped = if first == '0' { '1' } else { '0' };
    value["mac"] = serde_json::Value::String(format!("{flipped}{}", &mac[1..]));
    put(
        &store,
        &query_worker_key(&sibling.process_id().to_string()),
        serde_json::to_vec(&value).expect("encode"),
    )
    .await;

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(rejects(&reader), [0, 1, 0, 0, 0], "exactly one bad_mac");
}

/// The MAC covers the endpoint: a signed record whose `fragment_endpoint`
/// was edited after signing is rejected, so a writer without the key cannot
/// redirect an existing member.
#[tokio::test]
async fn signed_record_with_an_edited_endpoint_is_rejected() {
    let store = MemoryStore::new();
    let reader = worker(&[KEY_A]);
    let sibling = worker(&[KEY_A]);
    sibling.write_heartbeat(&store, NOW).await.expect("beat");

    let mut value: serde_json::Value =
        serde_json::from_slice(&object(&store, &sibling).await).expect("decode");
    value["fragment_endpoint"] = serde_json::Value::String("10.9.9.9:9443".into());
    put(
        &store,
        &query_worker_key(&sibling.process_id().to_string()),
        serde_json::to_vec(&value).expect("encode"),
    )
    .await;

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(rejects(&reader), [0, 1, 0, 0, 0]);
}

/// The record key is derived, not the file key itself: a MAC over the exact
/// claim under the derived key is admitted, while the same claim MAC'd under
/// the raw file key is rejected as `bad_mac`.
#[tokio::test]
async fn record_maced_under_the_raw_file_key_is_rejected() {
    let store = MemoryStore::new();
    let reader = worker(&[KEY_A]);

    let derived = record_at(Uuid::new_v4(), "10.0.0.2:9443");
    let derived_key = blake3::derive_key(RECORD_KEY_CONTEXT, &KEY_A);
    let mac = blake3::keyed_hash(&derived_key, &claim(&derived))
        .to_hex()
        .to_string();
    put(
        &store,
        &query_worker_key(&derived.process_id),
        envelope(&derived, Some(mac)),
    )
    .await;

    let raw = record_at(Uuid::new_v4(), "10.0.0.3:9443");
    let mac = blake3::keyed_hash(&KEY_A, &claim(&raw))
        .to_hex()
        .to_string();
    put(
        &store,
        &query_worker_key(&raw.process_id),
        envelope(&raw, Some(mac)),
    )
    .await;

    let live = reader.live_set(&store, NOW).await.expect("live set");
    let live = ids(&live);
    assert!(
        live.contains(&derived.process_id),
        "a hand-built MAC under the derived key verifies"
    );
    assert!(!live.contains(&raw.process_id), "the raw file key does not");
    assert_eq!(live.len(), 2);
    assert_eq!(rejects(&reader), [0, 1, 0, 0, 0]);
}

/// Key rotation: a record minted under B (B listed first) verifies at a
/// reader holding `[A, B]`, and is rejected by a reader holding only A.
#[tokio::test]
async fn record_minted_under_the_second_key_verifies_during_rotation() {
    let store = MemoryStore::new();
    let rotated = worker(&[KEY_B, KEY_A]);
    rotated.write_heartbeat(&store, NOW).await.expect("beat");

    let both = worker(&[KEY_A, KEY_B]);
    let live = both.live_set(&store, NOW).await.expect("live set");
    assert!(ids(&live).contains(&rotated.process_id().to_string()));
    assert_eq!(rejects(&both), [0, 0, 0, 0, 0]);

    let only_a = worker(&[KEY_A]);
    let live = only_a.live_set(&store, NOW).await.expect("live set");
    assert!(!ids(&live).contains(&rotated.process_id().to_string()));
    assert_eq!(rejects(&only_a), [0, 1, 0, 0, 0]);
}

/// A 5000-byte object is skipped as `oversize` without being decoded. Its
/// first 4200 bytes are a valid signed record padded with spaces and the
/// rest is not JSON: a reader that decoded the whole object would count it
/// `malformed`, and one that decoded only the first 4097 bytes would admit
/// it.
#[tokio::test]
async fn five_thousand_byte_object_is_oversize_and_never_decoded() {
    let store = MemoryStore::new();
    let reader = worker(&[KEY_A]);
    let sibling = worker(&[KEY_A]);
    sibling.write_heartbeat(&store, NOW).await.expect("beat");

    let mut bytes = object(&store, &sibling).await;
    assert!((bytes.len() as u64) < MAX_RECORD_BYTES);
    bytes.resize(4200, b' ');
    bytes.resize(5000, b'x');
    put(
        &store,
        &query_worker_key(&sibling.process_id().to_string()),
        bytes,
    )
    .await;

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(rejects(&reader), [0, 0, 1, 0, 0], "exactly one oversize");
}

/// A valid signed record padded with trailing JSON whitespace to exactly
/// `len` bytes, written over `sibling`'s key.
async fn pad_signed_record(store: &MemoryStore, sibling: &QueryWorkers, len: usize) {
    let mut bytes = object(store, sibling).await;
    assert!(bytes.len() < len);
    bytes.resize(len, b' ');
    put(
        store,
        &query_worker_key(&sibling.process_id().to_string()),
        bytes,
    )
    .await;
}

/// The bound is inclusive: a signed record padded to exactly
/// `MAX_RECORD_BYTES` (4096) is decoded, verifies and is admitted.
#[tokio::test]
async fn record_of_exactly_the_bound_is_decoded_and_admitted() {
    assert_eq!(MAX_RECORD_BYTES, 4096);
    let store = MemoryStore::new();
    let reader = worker(&[KEY_A]);
    let sibling = worker(&[KEY_A]);
    sibling.write_heartbeat(&store, NOW).await.expect("beat");
    pad_signed_record(&store, &sibling, 4096).await;
    assert_eq!(object(&store, &sibling).await.len(), 4096);

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert!(ids(&live).contains(&sibling.process_id().to_string()));
    assert_eq!(live.len(), 2);
    assert_eq!(rejects(&reader), [0, 0, 0, 0, 0]);
}

/// One byte past the bound is `oversize`. The object is a valid signed
/// record padded with whitespace, so a reader that decoded it, whole or cut
/// to 4096 bytes, would admit it instead.
#[tokio::test]
async fn record_one_byte_past_the_bound_is_oversize_and_never_decoded() {
    let store = MemoryStore::new();
    let reader = worker(&[KEY_A]);
    let sibling = worker(&[KEY_A]);
    sibling.write_heartbeat(&store, NOW).await.expect("beat");
    pad_signed_record(&store, &sibling, 4097).await;

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(rejects(&reader), [0, 0, 1, 0, 0], "exactly one oversize");
}

/// The MAC check runs before the stamp check: an unsigned record with a
/// stamp outside the window counts `missing_mac`, while the same stamp under
/// a valid MAC counts `stale_stamp`. `MemoryStore` reports no modification
/// time, so both are fetched.
#[tokio::test]
async fn stale_stamp_is_counted_only_after_the_mac_verifies() {
    let store = MemoryStore::new();
    let reader = worker(&[KEY_A]);
    let stale = NOW - 4 * 60 * 1_000_000_000;

    let mut unsigned = record_at(Uuid::new_v4(), "10.9.9.9:9443");
    unsigned.started_unix_ns = stale;
    put(
        &store,
        &query_worker_key(&unsigned.process_id),
        envelope(&unsigned, None),
    )
    .await;
    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(
        rejects(&reader),
        [1, 0, 0, 0, 0],
        "missing_mac, not stale_stamp"
    );

    let replayed = worker(&[KEY_A]);
    replayed.write_heartbeat(&store, stale).await.expect("beat");
    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(
        rejects(&reader),
        [2, 0, 0, 0, 1],
        "the unsigned record again, plus one stale_stamp"
    );
}

/// A drained sibling, verified, is skipped without a count: a graceful
/// shutdown is not a reject.
#[tokio::test]
async fn drained_sibling_counts_no_reject() {
    let store = MemoryStore::new();
    let reader = worker(&[KEY_A]);
    let sibling = worker(&[KEY_A]);
    sibling.write_heartbeat(&store, NOW).await.expect("beat");
    sibling.mark_drained(&store).await.expect("drain");

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(rejects(&reader), [0, 0, 0, 0, 0]);
}

/// The drain exception applies only to a verified record: an unsigned record
/// carrying the drained stamp is a forgery and counts `missing_mac`, so a
/// writer without the key file cannot evict a member uncounted by forging a
/// drain.
#[tokio::test]
async fn unsigned_record_with_the_drained_stamp_counts_missing_mac() {
    let store = MemoryStore::new();
    let reader = worker(&[KEY_A]);
    let mut forged = record_at(Uuid::new_v4(), "10.9.9.9:9443");
    forged.started_unix_ns = ravel_fleet::query_workers::DRAINED_STAMP_NS;
    put(
        &store,
        &query_worker_key(&forged.process_id),
        envelope(&forged, None),
    )
    .await;

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(rejects(&reader), [1, 0, 0, 0, 0], "a forged drain");
}

/// The drain exception is the exact drained stamp, not a range of very old
/// stamps: a verified record one nanosecond after `DRAINED_STAMP_NS` counts
/// `stale_stamp`.
#[tokio::test]
async fn stamp_next_to_the_drained_stamp_counts_stale_stamp() {
    let store = MemoryStore::new();
    let reader = worker(&[KEY_A]);
    let sibling = worker(&[KEY_A]);
    let near_drained = ravel_fleet::query_workers::DRAINED_STAMP_NS + 1;
    sibling
        .write_heartbeat(&store, near_drained)
        .await
        .expect("beat");

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(rejects(&reader), [0, 0, 0, 0, 1], "not a drain");
}

/// An empty key list cannot build a `RecordKeys`.
#[test]
fn empty_key_list_fails_construction() {
    assert_eq!(
        RecordKeys::from_file_keys(&[]).map(|k| k.len()),
        Err(NoRecordKeys)
    );
    assert_eq!(keys(&[KEY_A, KEY_B]).len(), 2);
}

/// Every reason renders under its closed label value.
#[test]
fn reject_reasons_have_stable_labels() {
    assert_eq!(
        RecordReject::ALL.map(RecordReject::reason),
        [
            "missing_mac",
            "bad_mac",
            "oversize",
            "malformed",
            "stale_stamp"
        ]
    );
}

/// `by_reason` lists every reason once, in `ALL` order.
#[test]
fn by_reason_follows_all_order() {
    let reader = worker(&[KEY_A]);
    assert_eq!(
        reader
            .record_rejects()
            .by_reason()
            .map(|(reason, _)| reason),
        RecordReject::ALL
    );
}
