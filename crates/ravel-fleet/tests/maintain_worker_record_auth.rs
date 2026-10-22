//! Maintain-worker record authentication (ADR-3130), through the public API
//! over `MemoryStore`: a sibling enters [`WorkerSet::live_set`] only when its
//! `WorkerHeartbeat` is version 2 and carries a MAC under a derived maintain
//! record key, and every other record is skipped and counted by reason.

#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::Duration;

use prost::Message;
use ravel_fleet::query_workers::RECORD_KEY_CONTEXT as QUERY_RECORD_KEY_CONTEXT;
use ravel_fleet::worker_set::{
    DEFAULT_UNIT_CONCURRENCY, HEARTBEAT_FORMAT_VERSION, MAINTAIN_RECORD_KEY_CONTEXT,
    MAX_RECORD_BYTES, MaintainMembership, MaintainRecordKeys, NoMaintainRecordKeys, RecordReject,
    RecordRejectMetrics, WorkerSet, heartbeat_key,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutMode, PutOptions};
use ravel_proto::sys::v1::WorkerHeartbeat;
use uuid::Uuid;

const H: Duration = Duration::from_secs(60);
const H_NS: i64 = 60 * 1_000_000_000;
const NOW: i64 = 1_000 * H_NS;
const KEY_A: [u8; 32] = [0xA1; 32];
const KEY_B: [u8; 32] = [0xB2; 32];

fn membership(file_keys: &[[u8; 32]]) -> MaintainMembership {
    MaintainMembership::new(
        MaintainRecordKeys::from_file_keys(file_keys).expect("non-empty key list"),
    )
}

fn worker() -> WorkerSet {
    WorkerSet::new(NOW, H, 3, DEFAULT_UNIT_CONCURRENCY)
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

async fn object(store: &MemoryStore, worker: &WorkerSet) -> Vec<u8> {
    store
        .get(&heartbeat_key(&worker.process_id()), GetRange::Full)
        .await
        .expect("get record")
        .data
        .to_vec()
}

/// The reader's reject counters, in `RecordReject::ALL` order:
/// `[missing_mac, bad_mac, oversize, malformed, stale_stamp]`.
fn rejects(membership: &MaintainMembership) -> [u64; RecordReject::ALL.len()] {
    membership.record_rejects().by_reason().map(|(_, n)| n)
}

/// The ADR-3130 claim, built here independently of the crate so the format
/// is pinned: `format_version` u32 BE, `process_id` as a u64-BE length and the
/// bytes, `started_unix_ns` i64 BE, `heartbeat_unix_ns` i64 BE.
fn claim(record: &WorkerHeartbeat) -> Vec<u8> {
    let mut claim = Vec::new();
    claim.extend_from_slice(&record.format_version.to_be_bytes());
    claim.extend_from_slice(&(record.process_id.len() as u64).to_be_bytes());
    claim.extend_from_slice(&record.process_id);
    claim.extend_from_slice(&record.started_unix_ns.to_be_bytes());
    claim.extend_from_slice(&record.heartbeat_unix_ns.to_be_bytes());
    claim
}

fn record_at(id: Uuid, format_version: u32, heartbeat_unix_ns: i64) -> WorkerHeartbeat {
    WorkerHeartbeat {
        format_version,
        process_id: id.as_bytes().to_vec(),
        started_unix_ns: NOW,
        heartbeat_unix_ns,
        mac: Vec::new(),
    }
}

/// `record` with `mac = blake3::keyed_hash(derive_key(context, file_key),
/// claim)`.
fn signed(mut record: WorkerHeartbeat, context: &str, file_key: &[u8; 32]) -> WorkerHeartbeat {
    let record_key = blake3::derive_key(context, file_key);
    record.mac = blake3::keyed_hash(&record_key, &claim(&record))
        .as_bytes()
        .to_vec();
    record
}

async fn put_record(store: &MemoryStore, id: Uuid, record: &WorkerHeartbeat) {
    put(store, &heartbeat_key(&id), record.encode_to_vec()).await;
}

/// The encoded record with an unknown length-delimited field (number 15)
/// appended so the object is exactly `len` bytes. A protobuf decoder skips an
/// unknown field, and the MAC covers field values only, so the padded object
/// still decodes to the same verified record.
fn pad_to(mut bytes: Vec<u8>, len: usize) -> Vec<u8> {
    let tag = (15 << 3) | 2;
    let mut payload = len - bytes.len() - 1 - 1;
    if payload >= 128 {
        payload -= 1;
    }
    bytes.push(tag);
    prost::encoding::encode_varint(payload as u64, &mut bytes);
    bytes.resize(bytes.len() + payload, 0x20);
    assert_eq!(bytes.len(), len, "padding arithmetic");
    bytes
}

/// A self-consistent version-2 record with no MAC, written at a fresh UUID
/// key by a writer holding the prefix's `PutObject` grant but not the key
/// file, does not enter the live set, and is counted as `missing_mac`.
#[tokio::test]
async fn forged_unsigned_record_at_fresh_uuid_is_excluded() {
    let store = MemoryStore::new();
    let reader = worker();
    let keys = membership(&[KEY_A]);
    let forged = Uuid::new_v4();
    put_record(
        &store,
        forged,
        &record_at(forged, HEARTBEAT_FORMAT_VERSION, NOW),
    )
    .await;

    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(rejects(&keys), [1, 0, 0, 0, 0], "exactly one missing_mac");
}

/// A version-1 record is `missing_mac` even when it carries a MAC that would
/// verify over its own fields: readers accept exactly version 2 and never
/// decode version 1 through a second path.
#[tokio::test]
async fn version_one_record_is_missing_mac_even_with_a_valid_mac() {
    assert_eq!(HEARTBEAT_FORMAT_VERSION, 2);
    let store = MemoryStore::new();
    let reader = worker();
    let keys = membership(&[KEY_A]);

    let unsigned_v1 = Uuid::new_v4();
    put_record(&store, unsigned_v1, &record_at(unsigned_v1, 1, NOW)).await;
    let maced_v1 = Uuid::new_v4();
    put_record(
        &store,
        maced_v1,
        &signed(
            record_at(maced_v1, 1, NOW),
            MAINTAIN_RECORD_KEY_CONTEXT,
            &KEY_A,
        ),
    )
    .await;

    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(rejects(&keys), [2, 0, 0, 0, 0], "both are missing_mac");
}

/// A record the crate writes, and one this test signs with its own claim
/// builder, are both admitted; one with a flipped MAC byte, or a MAC that is
/// not 32 bytes, is `bad_mac`.
#[tokio::test]
async fn signed_sibling_is_admitted_and_a_bad_mac_is_rejected() {
    let store = MemoryStore::new();
    let reader = worker();
    let keys = membership(&[KEY_A]);
    let sibling = worker();
    sibling
        .write_heartbeat(&store, &membership(&[KEY_A]), NOW)
        .await
        .expect("beat");
    let hand = Uuid::new_v4();
    put_record(
        &store,
        hand,
        &signed(
            record_at(hand, HEARTBEAT_FORMAT_VERSION, NOW),
            MAINTAIN_RECORD_KEY_CONTEXT,
            &KEY_A,
        ),
    )
    .await;

    let mut want = vec![reader.process_id(), sibling.process_id(), hand];
    want.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, want);
    assert_eq!(rejects(&keys), [0, 0, 0, 0, 0]);

    let mut flipped = WorkerHeartbeat::decode(object(&store, &sibling).await.as_slice())
        .expect("decode the written record");
    assert_eq!(flipped.mac.len(), 32);
    flipped.mac[0] ^= 1;
    put_record(&store, sibling.process_id(), &flipped).await;
    let mut short = signed(
        record_at(hand, HEARTBEAT_FORMAT_VERSION, NOW),
        MAINTAIN_RECORD_KEY_CONTEXT,
        &KEY_A,
    );
    short.mac.truncate(31);
    put_record(&store, hand, &short).await;

    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(rejects(&keys), [0, 2, 0, 0, 0], "two bad_mac");
}

/// A real signed record replayed with its `heartbeat_unix_ns` moved forward
/// no longer verifies: the stamp is covered by the MAC, so a captured record
/// cannot be kept fresh without the key.
#[tokio::test]
async fn refreshed_stamp_on_a_captured_record_is_bad_mac() {
    let store = MemoryStore::new();
    let reader = worker();
    let keys = membership(&[KEY_A]);
    let sibling = worker();
    sibling
        .write_heartbeat(&store, &membership(&[KEY_A]), NOW - 10 * H_NS)
        .await
        .expect("beat");

    let mut replayed = WorkerHeartbeat::decode(object(&store, &sibling).await.as_slice())
        .expect("decode the written record");
    replayed.heartbeat_unix_ns = NOW;
    put_record(&store, sibling.process_id(), &replayed).await;

    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(rejects(&keys), [0, 1, 0, 0, 0], "exactly one bad_mac");
}

/// A record whose body names a different process id than its key is
/// `malformed`, even when its MAC verifies over the body it carries. Copying a
/// real member's record to a fresh key does not add a member.
#[tokio::test]
async fn body_process_id_differing_from_the_key_is_malformed() {
    let store = MemoryStore::new();
    let reader = worker();
    let keys = membership(&[KEY_A]);
    let sibling = worker();
    sibling
        .write_heartbeat(&store, &membership(&[KEY_A]), NOW)
        .await
        .expect("beat");
    let copy_at = Uuid::new_v4();
    put(
        &store,
        &heartbeat_key(&copy_at),
        object(&store, &sibling).await,
    )
    .await;

    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    let mut want = vec![reader.process_id(), sibling.process_id()];
    want.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
    assert_eq!(live, want, "the copy at a fresh key is not admitted");
    assert_eq!(rejects(&keys), [0, 0, 0, 1, 0], "exactly one malformed");
}

/// Bytes that do not decode as a `WorkerHeartbeat`, and an empty object, are
/// `malformed`.
#[tokio::test]
async fn undecodable_and_empty_records_are_malformed() {
    let store = MemoryStore::new();
    let reader = worker();
    let keys = membership(&[KEY_A]);
    put(&store, &heartbeat_key(&Uuid::new_v4()), vec![0xFF; 7]).await;
    put(&store, &heartbeat_key(&Uuid::new_v4()), Vec::new()).await;

    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(rejects(&keys), [0, 0, 0, 2, 0], "two malformed");
}

/// A MAC computed under the raw file key, not the derived record key, does
/// not verify.
#[tokio::test]
async fn record_maced_under_the_raw_file_key_is_rejected() {
    let store = MemoryStore::new();
    let reader = worker();
    let keys = membership(&[KEY_A]);
    let id = Uuid::new_v4();
    let mut record = record_at(id, HEARTBEAT_FORMAT_VERSION, NOW);
    record.mac = blake3::keyed_hash(&KEY_A, &claim(&record))
        .as_bytes()
        .to_vec();
    put_record(&store, id, &record).await;

    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(rejects(&keys), [0, 1, 0, 0, 0], "exactly one bad_mac");
}

/// A record MACed under the query-worker registry's derive context, from the
/// same file key, never verifies as a maintain record; the same record under
/// the maintain context does.
#[tokio::test]
async fn query_record_key_never_verifies_a_maintain_record() {
    assert_ne!(QUERY_RECORD_KEY_CONTEXT, MAINTAIN_RECORD_KEY_CONTEXT);
    let store = MemoryStore::new();
    let reader = worker();
    let keys = membership(&[KEY_A]);
    let id = Uuid::new_v4();
    put_record(
        &store,
        id,
        &signed(
            record_at(id, HEARTBEAT_FORMAT_VERSION, NOW),
            QUERY_RECORD_KEY_CONTEXT,
            &KEY_A,
        ),
    )
    .await;
    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(rejects(&keys), [0, 1, 0, 0, 0], "exactly one bad_mac");

    put_record(
        &store,
        id,
        &signed(
            record_at(id, HEARTBEAT_FORMAT_VERSION, NOW),
            MAINTAIN_RECORD_KEY_CONTEXT,
            &KEY_A,
        ),
    )
    .await;
    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert!(live.contains(&id), "the maintain-context MAC verifies");
}

/// Rotation step 1: a record minted under B (a pod already on the new file,
/// B first) verifies at a reader holding `[A, B]`, and a reader still holding
/// only `[A]` rejects it.
#[tokio::test]
async fn record_minted_under_the_second_key_verifies_during_rotation() {
    let store = MemoryStore::new();
    let sibling = worker();
    sibling
        .write_heartbeat(&store, &membership(&[KEY_B, KEY_A]), NOW)
        .await
        .expect("beat");

    let reader = worker();
    let both = membership(&[KEY_A, KEY_B]);
    let live = reader.live_set(&store, &both, NOW).await.expect("live set");
    assert!(live.contains(&sibling.process_id()));
    assert_eq!(rejects(&both), [0, 0, 0, 0, 0]);

    let old = membership(&[KEY_A]);
    let live = reader.live_set(&store, &old, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(rejects(&old), [0, 1, 0, 0, 0], "exactly one bad_mac");
}

/// The bound is inclusive: a signed record padded to exactly
/// `MAX_RECORD_BYTES` (4096) is decoded, verifies and is admitted.
#[tokio::test]
async fn record_of_exactly_the_bound_is_decoded_and_admitted() {
    assert_eq!(MAX_RECORD_BYTES, 4096);
    let store = MemoryStore::new();
    let reader = worker();
    let keys = membership(&[KEY_A]);
    let sibling = worker();
    sibling
        .write_heartbeat(&store, &keys, NOW)
        .await
        .expect("beat");
    let padded = pad_to(object(&store, &sibling).await, 4096);
    put(&store, &heartbeat_key(&sibling.process_id()), padded).await;
    assert_eq!(object(&store, &sibling).await.len(), 4096);

    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert!(live.contains(&sibling.process_id()));
    assert_eq!(live.len(), 2);
    assert_eq!(rejects(&keys), [0, 0, 0, 0, 0]);
}

/// One byte past the bound is `oversize`. The object is a valid signed
/// record padded with an unknown field, so a reader that decoded it, whole or
/// cut to 4096 bytes, would admit it or count it `malformed` instead.
#[tokio::test]
async fn record_one_byte_past_the_bound_is_oversize_and_never_decoded() {
    let store = MemoryStore::new();
    let reader = worker();
    let keys = membership(&[KEY_A]);
    let sibling = worker();
    sibling
        .write_heartbeat(&store, &keys, NOW)
        .await
        .expect("beat");
    let padded = pad_to(object(&store, &sibling).await, 4097);
    assert!(
        WorkerHeartbeat::decode(padded.as_slice()).is_ok(),
        "the padded object is a valid record"
    );
    put(&store, &heartbeat_key(&sibling.process_id()), padded).await;

    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(rejects(&keys), [0, 0, 1, 0, 0], "exactly one oversize");
}

/// The MAC check runs before the stamp check: an unsigned record with a stamp
/// outside the window counts `missing_mac`, while the same stamp under a
/// valid MAC counts `stale_stamp`. `MemoryStore` reports no modification time
/// by default, so both are fetched.
#[tokio::test]
async fn stale_stamp_is_counted_only_after_the_mac_verifies() {
    let store = MemoryStore::new();
    let reader = worker();
    let keys = membership(&[KEY_A]);
    let stale = NOW - 4 * H_NS;

    let unsigned = Uuid::new_v4();
    put_record(
        &store,
        unsigned,
        &record_at(unsigned, HEARTBEAT_FORMAT_VERSION, stale),
    )
    .await;
    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(
        rejects(&keys),
        [1, 0, 0, 0, 0],
        "missing_mac, not stale_stamp"
    );

    let replayed = worker();
    replayed
        .write_heartbeat(&store, &keys, stale)
        .await
        .expect("beat");
    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(
        rejects(&keys),
        [2, 0, 0, 0, 1],
        "the unsigned record again, plus one stale_stamp"
    );
}

/// The three uncounted skips: the reader's own key (whatever it holds), a key
/// that is not a UUID, and a key whose LIST modification time is past the
/// liveness window. None of them is a reject.
#[tokio::test]
async fn own_key_non_uuid_key_and_mtime_stale_key_are_not_counted() {
    let store = MemoryStore::new();
    store.set_clock_ms(1_000 * 60 * 1_000 - 10 * 60 * 1_000);
    put(&store, &heartbeat_key(&Uuid::new_v4()), vec![0xFF; 7]).await;
    store.set_clock_ms(1_000 * 60 * 1_000);

    let reader = worker();
    let keys = membership(&[KEY_A]);
    put(&store, &heartbeat_key(&reader.process_id()), vec![0xFF; 7]).await;
    put(&store, "sys/maintain/workers/not-a-uuid", vec![0xFF; 7]).await;

    let live = reader.live_set(&store, &keys, NOW).await.expect("live set");
    assert_eq!(live, vec![reader.process_id()]);
    assert_eq!(rejects(&keys), [0, 0, 0, 0, 0], "no reject counted");
}

/// Clones of one membership handle, and a handle given the same counters,
/// count into one family: the heartbeat task and scrub report together.
#[tokio::test]
async fn readers_sharing_a_handle_count_into_the_same_counters() {
    let store = MemoryStore::new();
    let forged = Uuid::new_v4();
    put_record(
        &store,
        forged,
        &record_at(forged, HEARTBEAT_FORMAT_VERSION, NOW),
    )
    .await;

    let shared = Arc::new(RecordRejectMetrics::new());
    let heartbeat_task = membership(&[KEY_A]).with_record_rejects(shared.clone());
    let scrub = heartbeat_task.clone();
    let reader = worker();
    reader
        .live_set(&store, &heartbeat_task, NOW)
        .await
        .expect("live set");
    reader
        .live_set(&store, &scrub, NOW)
        .await
        .expect("live set");
    assert_eq!(shared.get(RecordReject::MissingMac), 2);
    assert!(Arc::ptr_eq(scrub.record_rejects(), &shared));
}

/// The record the crate writes carries version 2, its own process id, and a
/// 32-byte MAC equal to the one this test computes from the pinned claim
/// format under the first key.
#[tokio::test]
async fn written_record_matches_the_pinned_format() {
    let store = MemoryStore::new();
    let writer = worker();
    writer
        .write_heartbeat(&store, &membership(&[KEY_B, KEY_A]), NOW)
        .await
        .expect("beat");
    let record = WorkerHeartbeat::decode(object(&store, &writer).await.as_slice())
        .expect("decode the written record");
    assert_eq!(record.format_version, 2);
    assert_eq!(record.process_id, writer.process_id().as_bytes().to_vec());
    assert_eq!(record.heartbeat_unix_ns, NOW);
    let mut unsigned = record.clone();
    unsigned.mac.clear();
    assert_eq!(
        record.mac,
        signed(unsigned, MAINTAIN_RECORD_KEY_CONTEXT, &KEY_B).mac,
        "minted under the first key"
    );
}

#[test]
fn empty_key_list_fails_construction() {
    assert_eq!(
        MaintainRecordKeys::from_file_keys(&[]).map(|_| ()),
        Err(NoMaintainRecordKeys)
    );
    let keys = MaintainRecordKeys::from_file_keys(&[KEY_A, KEY_B]).expect("two keys");
    assert_eq!(keys.len(), 2);
    assert!(!keys.is_empty());
}

#[test]
fn record_keys_debug_never_prints_key_material() {
    let keys = MaintainRecordKeys::from_file_keys(&[KEY_A]).expect("one key");
    let rendered = format!("{keys:?} {:?}", membership(&[KEY_A]));
    assert!(!rendered.contains("161"), "{rendered}");
    assert!(!rendered.to_lowercase().contains("a1a1"), "{rendered}");
}
