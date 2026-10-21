//! ADR-1918: query-worker heartbeat records carry a MAC under a key derived
//! from the fragment key file, and `live_set` admits a sibling only when that
//! MAC verifies. Every case here goes through the public API against
//! `MemoryStore`, and every forged object is written as raw bytes, which is
//! exactly what a query-role principal without the key file can PUT.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::time::Duration;

use ravel_fleet::query_workers::{
    MAX_RECORD_BYTES, QueryWorkers, RecordKeys, RecordKeysError, RecordReject, query_worker_key,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions};
use uuid::Uuid;

const H: Duration = Duration::from_secs(60);
const NOW: i64 = 1_000 * 60 * 1_000_000_000;
const KEY_A: [u8; 32] = [0xa1; 32];
const KEY_B: [u8; 32] = [0xb2; 32];

fn keys(file_keys: &[[u8; 32]]) -> RecordKeys {
    RecordKeys::from_file_keys(file_keys).expect("record keys")
}

fn worker_with(endpoint: &str, file_keys: &[[u8; 32]]) -> QueryWorkers {
    QueryWorkers::new(endpoint, 5, H, 3, keys(file_keys))
}

async fn put_raw(store: &dyn ObjectStoreBackend, key: &str, bytes: Vec<u8>) {
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
        .expect("put raw record");
}

async fn get_json(store: &dyn ObjectStoreBackend, key: &str) -> serde_json::Value {
    let got = store.get(key, GetRange::Full).await.expect("get record");
    serde_json::from_slice(&got.data).expect("record is JSON")
}

fn ids(live: &[ravel_fleet::query_workers::QueryWorkerRecord]) -> Vec<String> {
    live.iter().map(|r| r.process_id.clone()).collect()
}

/// The claim layout ADR-1918 decision 1 fixes, spelled out independently of
/// the crate so a change to it fails here.
fn claim(process_id: &str, endpoint: &str, protocol_version: u32, stamp: i64) -> Vec<u8> {
    let mut out = Vec::new();
    for field in [process_id, endpoint] {
        out.extend_from_slice(&(field.len() as u64).to_be_bytes());
        out.extend_from_slice(field.as_bytes());
    }
    out.extend_from_slice(&protocol_version.to_be_bytes());
    out.extend_from_slice(&stamp.to_be_bytes());
    out
}

fn record_json(pid: &str, endpoint: &str, mac: Option<&str>) -> Vec<u8> {
    let mut value = serde_json::json!({
        "process_id": pid,
        "fragment_endpoint": endpoint,
        "protocol_version": 5,
        "started_unix_ns": NOW,
    });
    if let Some(mac) = mac {
        value["mac"] = serde_json::Value::String(mac.to_string());
    }
    serde_json::to_vec(&value).expect("encode")
}

/// A self-consistent record with no MAC at a fresh UUID: before ADR-1918 this
/// entered the live set, since its key parses, its body names its key, and
/// its stamp is fresh.
#[tokio::test]
async fn forged_unsigned_record_at_fresh_uuid_is_excluded() {
    let store = MemoryStore::new();
    let reader = worker_with("10.0.0.1:9443", &[KEY_A]);
    let forged = Uuid::new_v4().to_string();
    put_raw(
        &store,
        &query_worker_key(&forged),
        record_json(&forged, "10.6.6.6:9443", None),
    )
    .await;

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(reader.record_rejects().count(RecordReject::MissingMac), 1);
    assert_eq!(reader.record_rejects().count(RecordReject::BadMac), 0);
}

/// A sibling holding the same file key is admitted; the same object with one
/// hex digit of its MAC changed is not.
#[tokio::test]
async fn signed_sibling_is_admitted_and_a_bad_mac_is_rejected() {
    let store = MemoryStore::new();
    let reader = worker_with("10.0.0.1:9443", &[KEY_A]);
    let sibling = worker_with("10.0.0.2:9443", &[KEY_A]);
    sibling.write_heartbeat(&store, NOW).await.expect("beat");

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert!(ids(&live).contains(&sibling.process_id().to_string()));
    assert_eq!(live.len(), 2);
    assert_eq!(
        reader.record_rejects().by_reason(),
        [
            (RecordReject::MissingMac, 0),
            (RecordReject::BadMac, 0),
            (RecordReject::Oversize, 0),
        ]
    );

    let key = query_worker_key(&sibling.process_id().to_string());
    let mut value = get_json(&store, &key).await;
    let mac = value["mac"].as_str().expect("mac is a string").to_string();
    assert_eq!(mac.len(), 64, "the mac is 64 hex digits: {mac}");
    let mut flipped: Vec<char> = mac.chars().collect();
    flipped[17] = if flipped[17] == '0' { '1' } else { '0' };
    value["mac"] = serde_json::Value::String(flipped.into_iter().collect());
    put_raw(&store, &key, serde_json::to_vec(&value).expect("encode")).await;

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(reader.record_rejects().count(RecordReject::BadMac), 1);
    assert_eq!(reader.record_rejects().count(RecordReject::MissingMac), 0);
}

/// A correctly signed record whose endpoint was edited afterwards no longer
/// verifies: the MAC covers `fragment_endpoint`, so a writer without the key
/// cannot redirect an existing member's traffic.
#[tokio::test]
async fn signed_record_with_an_edited_endpoint_is_rejected() {
    let store = MemoryStore::new();
    let reader = worker_with("10.0.0.1:9443", &[KEY_A]);
    let sibling = worker_with("10.0.0.2:9443", &[KEY_A]);
    sibling.write_heartbeat(&store, NOW).await.expect("beat");

    let key = query_worker_key(&sibling.process_id().to_string());
    let mut value = get_json(&store, &key).await;
    assert_eq!(value["fragment_endpoint"], "10.0.0.2:9443");
    value["fragment_endpoint"] = serde_json::Value::String("10.6.6.6:9443".into());
    put_raw(&store, &key, serde_json::to_vec(&value).expect("encode")).await;

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(reader.record_rejects().count(RecordReject::BadMac), 1);
}

/// The record key is derived from the file key, never the file key itself: a
/// MAC over the right claim under the RAW file key is rejected, and the same
/// claim under the derived key is admitted, which also pins the claim layout
/// and the derivation context.
#[tokio::test]
async fn record_maced_with_the_raw_file_key_is_rejected() {
    let store = MemoryStore::new();
    let reader = worker_with("10.0.0.1:9443", &[KEY_A]);
    let pid = Uuid::new_v4().to_string();
    let endpoint = "10.0.0.3:9443";
    let claim = claim(&pid, endpoint, 5, NOW);

    let raw_mac = blake3::keyed_hash(&KEY_A, &claim).to_hex().to_string();
    put_raw(
        &store,
        &query_worker_key(&pid),
        record_json(&pid, endpoint, Some(&raw_mac)),
    )
    .await;
    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(reader.record_rejects().count(RecordReject::BadMac), 1);

    let derived = blake3::derive_key("ravel query worker record v1", &KEY_A);
    let derived_mac = blake3::keyed_hash(&derived, &claim).to_hex().to_string();
    put_raw(
        &store,
        &query_worker_key(&pid),
        record_json(&pid, endpoint, Some(&derived_mac)),
    )
    .await;
    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert!(
        ids(&live).contains(&pid),
        "the same claim under the derived key is admitted"
    );
    assert_eq!(reader.record_rejects().count(RecordReject::BadMac), 1);
}

/// Rotation: a record minted under key B verifies at a reader holding
/// `[A, B]`, and is rejected by a reader holding only A.
#[tokio::test]
async fn record_minted_under_the_second_key_verifies_after_rotation() {
    let store = MemoryStore::new();
    let rotated = worker_with("10.0.0.1:9443", &[KEY_A, KEY_B]);
    let a_only = worker_with("10.0.0.4:9443", &[KEY_A]);
    let writer = worker_with("10.0.0.2:9443", &[KEY_B]);
    writer.write_heartbeat(&store, NOW).await.expect("beat");

    let live = rotated.live_set(&store, NOW).await.expect("live set");
    assert!(ids(&live).contains(&writer.process_id().to_string()));
    assert_eq!(rotated.record_rejects().count(RecordReject::BadMac), 0);

    let live = a_only.live_set(&store, NOW).await.expect("live set");
    assert!(!ids(&live).contains(&writer.process_id().to_string()));
    assert_eq!(a_only.record_rejects().count(RecordReject::BadMac), 1);
}

/// A 5000-byte object is skipped as `oversize` before decoding. It is a
/// correctly signed record padded out through its endpoint, so a reader that
/// fetched and decoded the whole object would admit it.
#[tokio::test]
async fn oversize_record_is_skipped_without_being_decoded() {
    let store = MemoryStore::new();
    let reader = worker_with("10.0.0.1:9443", &[KEY_A]);
    let probe = worker_with("", &[KEY_A]);
    probe
        .write_heartbeat(&store, NOW)
        .await
        .expect("probe beat");
    let probe_key = query_worker_key(&probe.process_id().to_string());
    let base_len = store
        .get(&probe_key, GetRange::Full)
        .await
        .expect("probe")
        .data
        .len();
    store.delete(&probe_key).await.expect("drop probe");

    let padded = "a".repeat(5000 - base_len);
    let big = worker_with(&padded, &[KEY_A]);
    big.write_heartbeat(&store, NOW).await.expect("big beat");
    let big_key = query_worker_key(&big.process_id().to_string());
    let stored = store.get(&big_key, GetRange::Full).await.expect("big");
    assert_eq!(stored.data.len(), 5000);
    const { assert!(5000 > MAX_RECORD_BYTES) };

    let live = reader.live_set(&store, NOW).await.expect("live set");
    assert_eq!(ids(&live), vec![reader.process_id().to_string()]);
    assert_eq!(
        reader.record_rejects().by_reason(),
        [
            (RecordReject::MissingMac, 0),
            (RecordReject::BadMac, 0),
            (RecordReject::Oversize, 1),
        ]
    );
}

/// A rejected record does not fail the read: a valid sibling beside forged,
/// oversized and empty objects is still admitted.
#[tokio::test]
async fn a_rejected_record_does_not_fail_the_whole_read() {
    let store = MemoryStore::new();
    let reader = worker_with("10.0.0.1:9443", &[KEY_A]);
    let sibling = worker_with("10.0.0.2:9443", &[KEY_A]);
    sibling.write_heartbeat(&store, NOW).await.expect("beat");
    let forged = Uuid::new_v4().to_string();
    put_raw(
        &store,
        &query_worker_key(&forged),
        record_json(&forged, "10.6.6.6:9443", None),
    )
    .await;
    put_raw(
        &store,
        &query_worker_key(&Uuid::new_v4().to_string()),
        vec![b'x'; 5000],
    )
    .await;
    put_raw(
        &store,
        &query_worker_key(&Uuid::new_v4().to_string()),
        Vec::new(),
    )
    .await;

    let live = reader.live_set(&store, NOW).await.expect("live set");
    let mut expected = vec![
        reader.process_id().to_string(),
        sibling.process_id().to_string(),
    ];
    expected.sort();
    assert_eq!(ids(&live), expected);
    assert_eq!(
        reader.record_rejects().by_reason(),
        [
            (RecordReject::MissingMac, 1),
            (RecordReject::BadMac, 0),
            (RecordReject::Oversize, 1),
        ]
    );
}

/// No key, no record keys: the typed error, never a set that signs nothing.
#[test]
fn empty_key_list_fails_construction() {
    let err = RecordKeys::from_file_keys(&[]).expect_err("an empty key list is refused");
    assert_eq!(err, RecordKeysError::Empty);
}
