//! RLOG determinism: the same L0 `.rlog` inputs, compacted twice from
//! scratch, produce the same compaction record bytes, the same part object
//! keys, and the same part bytes (the log analogue of `determinism.rs`, which
//! pins the RSEG side).
//!
//! This is the test that actually exercises the merge's ts tie-break. The
//! merge sorts each stream's gathered records with a *stable* sort by `ts_ns`
//! (`recs.sort_by_key(|r| r.ts_ns)` in `rlog.rs`), so when two records across
//! different inputs share the same post-remap `(stream_ref, ts)` pair the tie
//! is broken by the order `gather_stream` iterates the readers, which is the
//! inputs' canonical order. If that order were not deterministic, the two
//! runs' blocks would differ and the part bytes would diverge; asserting
//! byte-identical output across two independent runs pins the behaviour that
//! crash-recovery convergence depends on (plan §3.4).
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;

use prost::Message;
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{LogRecord, RlogConfig, RlogWriter, writer::ObjectIdentity};
use ravel_maintain::{Bucket, CompactorConfig, FixedClock, compact_bucket};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_types::logstream::{AttrValue, LogStreamId, log_stream_id};
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

const TENANT: &str = "acme";
const SHARD: u32 = 7;
const HOUR: u32 = 495_000;
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const EPOCH: u64 = 10;
const OUTPUT_FORMAT_VERSION: u32 = 2;

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn bucket() -> Bucket {
    Bucket::new(tenant_hash(), Signal::Logs, SHARD, HOUR)
}

/// Past the seal margin for [`HOUR`] under default config.
fn sealed_now_ns() -> i64 {
    (i64::from(HOUR) + 1) * NS_PER_HOUR + 2 * NS_PER_HOUR
}

/// A synthetic stream `n`'s id and canonical resource+scope blob, identical to
/// the crate's own RLOG test helper: the id is the true hash of the blob.
fn stream_ident(n: u32) -> (LogStreamId, Vec<u8>) {
    let res = vec![(
        "service.name".to_string(),
        AttrValue::Str(format!("svc{n}")),
    )];
    let id = log_stream_id(&res, "scope", "1", &[]);
    let blob = ravel_logseg::stream_attrs_bytes(&res, "scope", "1", &[]);
    (id, blob)
}

fn record(stream_n: u32, ts: i64, body: &str) -> LogRecord {
    let (stream_id, stream_attrs) = stream_ident(stream_n);
    LogRecord {
        stream_id,
        stream_attrs,
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// Seed one L0 `.rlog` input (data object + commit record), as the ingest log
/// shard would.
async fn seed(store: &dyn ObjectStoreBackend, writer_id: Uuid, seq: u64, records: &[LogRecord]) {
    let th = tenant_hash();
    let identity = ObjectIdentity {
        tenant_hash: th.0,
        shard: SHARD,
        writer_id: writer_id.into_bytes(),
        writer_epoch: EPOCH,
        writer_seq: seq,
    };
    let mut w = RlogWriter::new(RlogConfig::default(), identity);
    for r in records {
        w.push(r.clone()).expect("push");
    }
    let bytes = bytes::Bytes::from(w.finish().expect("finish L0"));
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
    let data_key = keys::data_key(
        &th,
        Signal::Logs,
        SHARD,
        writer_id,
        EPOCH,
        seq,
        &content_hash,
    )
    .expect("data key");
    store
        .put(&data_key, bytes.clone(), PutOptions::default())
        .await
        .expect("put data");

    let mut ids: BTreeSet<LogStreamId> = BTreeSet::new();
    let mut min_ts = i64::MAX;
    let mut max_ts = i64::MIN;
    for r in records {
        ids.insert(r.stream_id);
        min_ts = min_ts.min(r.ts_ns);
        max_ts = max_ts.max(r.ts_ns);
    }
    let created = i64::from(HOUR) * NS_PER_HOUR + (seq as i64) * 1_000_000;
    let rec = record::build(NewCommitRecord {
        tenant_hash: th,
        signal: Signal::Logs,
        shard: SHARD,
        writer_id,
        writer_epoch: EPOCH,
        writer_seq: seq,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: records.len() as u64,
        series_count: ids.len() as u64,
        min_event_ts_ns: min_ts,
        max_event_ts_ns: max_ts,
        min_ingest_ts_ns: created,
        max_ingest_ts_ns: created,
        segment_format_version: OUTPUT_FORMAT_VERSION,
        created_unix_ns: created,
        ingest_hour_bucket: HOUR,
    })
    .expect("build commit record");
    let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
    store
        .put(&commit_key, record::encode(&rec), PutOptions::default())
        .await
        .expect("put commit");
}

/// Three L0 inputs seeded so that several records across *different* inputs
/// share the same post-remap `(stream_ref, ts)` pair, forcing the merge's
/// stable ts tie-break to matter:
///
/// - stream 0, ts 1000 is carried by all three inputs (bodies "a-1000",
///   "b-1000", "c-1000") -- three records that tie on `(stream_ref, ts)`;
/// - stream 1, ts 1500 is carried by inputs A and B (bodies "a-1500",
///   "b-1500") -- two more that tie.
///
/// No dedup happens (distinct bodies are distinct records), so all of them
/// survive into the L1 output and their relative order is decided purely by
/// the tie-break. Distinct writer ids/seqs give the inputs a canonical order.
fn seed_specs() -> Vec<(Uuid, u64, Vec<LogRecord>)> {
    vec![
        (
            Uuid::from_u128(42),
            1,
            vec![
                record(0, 1000, "a-1000"),
                record(0, 2000, "a-2000"),
                record(1, 1500, "a-1500"),
            ],
        ),
        (
            Uuid::from_u128(7),
            2,
            vec![record(0, 1000, "b-1000"), record(1, 1500, "b-1500")],
        ),
        (
            Uuid::from_u128(7),
            1,
            vec![record(0, 1000, "c-1000"), record(2, 500, "c-500")],
        ),
    ]
}

/// Compact a fresh store seeded from [`seed_specs`] and return the compaction
/// record key, its encoded bytes, and every `(part key, part bytes)`.
async fn compact_once() -> (String, Vec<u8>, Vec<(String, Vec<u8>)>) {
    let store = MemoryStore::new();
    for (writer_id, seq, records) in seed_specs() {
        seed(&store, writer_id, seq, &records).await;
    }
    let clock = FixedClock::new(sealed_now_ns());
    let bucket = bucket();
    compact_bucket(&store, &clock, &CompactorConfig::default(), &bucket)
        .await
        .expect("compact");

    let prefix = keys::commit_shard_hour_prefix(
        &bucket.tenant_hash,
        bucket.signal,
        bucket.shard,
        bucket.ingest_hour_bucket,
    )
    .unwrap();
    let record_key = list_all(&store, &prefix)
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.key)
        .find(|k| {
            matches!(
                keys::partition_bucket_entry(k),
                Ok(keys::BucketEntry::CompactionRecord(_))
            )
        })
        .expect("record key");
    let record_bytes = store
        .get(&record_key, GetRange::Full)
        .await
        .unwrap()
        .data
        .to_vec();
    let record =
        ravel_proto::commit::v1::CompactionRecord::decode(record_bytes.as_slice()).unwrap();
    let mut parts = Vec::new();
    for p in &record.parts {
        let key = keys::reconstruct_l1_part_key(&record, p).unwrap();
        let bytes = store.get(&key, GetRange::Full).await.unwrap().data.to_vec();
        parts.push((key, bytes));
    }
    (record_key, record_bytes, parts)
}

#[tokio::test]
async fn same_inputs_same_bytes_and_keys() {
    let (rk_a, rb_a, parts_a) = compact_once().await;
    let (rk_b, rb_b, parts_b) = compact_once().await;

    assert_eq!(rk_a, rk_b, "record key deterministic");
    assert_eq!(rb_a, rb_b, "record bytes deterministic");
    assert_eq!(parts_a.len(), parts_b.len());
    for ((ka, ba), (kb, bb)) in parts_a.iter().zip(parts_b.iter()) {
        assert_eq!(ka, kb, "part key deterministic");
        assert_eq!(
            ba, bb,
            "part bytes deterministic (stable ts tie-break over canonical input order)"
        );
    }
    assert!(!parts_a.is_empty());
}
