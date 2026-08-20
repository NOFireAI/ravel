//! Differential proof for compaction/retention folding: for a population that mixes a plain
//! L0 bucket, a compacted bucket, and a tombstoned bucket, `Catalog::resolve`
//! must return the exact same segment set whether it runs before any fold
//! (pure Phase 1 listing, `Catalog::list_hour_bucket`) or after one (served
//! from the snapshot part the fold wrote). The two paths share no code past
//! `Catalog::resolve` itself, so agreement here proves the fold reproduces the
//! resolver's compaction/retention rules exactly: L1 parts added at level 1,
//! input-listed L0s removed, tombstoned buckets excluded.
//!
//! Mirrors the differential pattern in `snapshot_resolve.rs`
//! (`resolve_after_fold_matches_resolve_before_fold`), extended with the
//! compaction and retention records `compaction_resolution.rs` exercises.
//! Resolution never fetches L0 data or L1 part objects, and the fold skips the
//! postings segment fetch once a level-1 entry is present, so this test needs
//! no real segment bytes: it PUTs commit, compaction, and tombstone records
//! directly.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use prost::Message;
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS,
    DEFAULT_MAX_FLUSH_LIFETIME_NS, SegmentLevel,
};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::{keys, signal};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionInputIdentity, CompactionPart, CompactionRecord,
};
use ravel_segment::VERSION_V7;
use ravel_types::{Signal, TenantHash, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const MARGIN_NS: i64 =
    DEFAULT_MAX_FLUSH_LIFETIME_NS + DEFAULT_CLOCK_SKEW_ALLOWANCE_NS + DEFAULT_FOLD_SAFETY_MARGIN_NS;

fn tenant() -> TenantHash {
    TenantHash([0x7a; 16])
}

fn config(shard_count: u32) -> CatalogConfig {
    CatalogConfig {
        shard_count,
        ..Default::default()
    }
}

/// `now_ns` at which ingest hour `hour` has just sealed under default margins.
fn now_at_seal(hour: u32) -> i64 {
    (i64::from(hour) + 1) * NS_PER_HOUR + MARGIN_NS
}

fn full_window_range(start_hour: u32, now_ns: i64) -> TimeRange {
    TimeRange {
        start_ns: i64::from(start_hour) * NS_PER_HOUR,
        end_ns: now_ns,
    }
}

/// A self-consistent L0 commit record; resolution and the fold read only the
/// record, so no segment bytes are written.
fn l0_record(
    writer_id: Uuid,
    shard: u32,
    seq: u64,
    hour: u32,
    created_unix_ns: i64,
) -> CommitRecord {
    let event = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
    record::build(NewCommitRecord {
        tenant_hash: tenant(),
        signal: Signal::Metrics,
        shard,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: 100,
        content_hash: [seq as u8 ^ 0x5a; 32],
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: event,
        max_event_ts_ns: event + 100,
        min_ingest_ts_ns: event,
        max_ingest_ts_ns: event + 100,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns,
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

fn part(part_index: u32, hour: u32, seed: u8) -> CompactionPart {
    let event = i64::from(hour) * NS_PER_HOUR + 60_000_000_000;
    CompactionPart {
        part_index,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0xff; 16],
        content_hash: vec![seed; 32],
        object_size: 4096,
        sample_count: 10,
        series_count: 2,
        run_count: 3,
        min_event_ts_ns: event,
        max_event_ts_ns: event + 100,
        segment_format_version: u32::from(VERSION_V7),
    }
}

/// Build and PUT a compaction record naming `inputs`, at its own reconstructed
/// key. `seed` makes distinct records get distinct `input_set_hash`.
async fn put_compaction_record(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    hour: u32,
    inputs: &[&CommitRecord],
    parts: Vec<CompactionPart>,
    created_unix_ns: i64,
    seed: &[u8],
) {
    let input_set_hash = *blake3::hash(seed).as_bytes();
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: tenant().0.to_vec(),
        signal: signal::to_proto(Signal::Metrics) as i32,
        shard,
        ingest_hour_bucket: hour,
        level: 1,
        inputs: inputs
            .iter()
            .map(|r| CompactionInputIdentity {
                writer_id: r.writer_id.clone(),
                writer_epoch: r.writer_epoch,
                writer_seq: r.writer_seq,
            })
            .collect(),
        input_set_hash: input_set_hash.to_vec(),
        parts,
        created_unix_ns,
    };
    let hash16: String = input_set_hash[..8]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    let key = keys::compaction_record_key(&tenant(), Signal::Metrics, shard, hour, &hash16)
        .expect("record key");
    store
        .put(
            &key,
            record.encode_to_vec().into(),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put compaction record");
}

async fn put_tombstone(store: &dyn ObjectStoreBackend, shard: u32, hour: u32) {
    let key =
        keys::retention_tombstone_key(&tenant(), Signal::Metrics, shard, hour).expect("tomb key");
    store
        .put(
            &key,
            Bytes::from_static(b"tmb"),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put tombstone");
}

// ---------------------------------------------------------------------------
// Counting store: records every LIST prefix so the test can prove the sealed
// compacted and tombstoned buckets were served from the snapshot, not relisted.
// ---------------------------------------------------------------------------

#[derive(Clone, Default)]
struct RequestLog {
    lists: Arc<Mutex<Vec<String>>>,
}

impl RequestLog {
    fn list_count_for(&self, needle: &str) -> usize {
        self.lists
            .lock()
            .unwrap()
            .iter()
            .filter(|k| k.contains(needle))
            .count()
    }
}

struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    log: RequestLog,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> (Arc<Self>, RequestLog) {
        let log = RequestLog::default();
        (
            Arc::new(CountingStore {
                inner,
                log: log.clone(),
            }),
            log,
        )
    }
}

#[async_trait]
impl ObjectStoreBackend for CountingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }
    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.inner.get(key, range).await
    }
    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }
    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.log.lists.lock().unwrap().push(prefix.to_string());
        self.inner.list(prefix, page).await
    }
    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }
    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

/// A population with a plain L0 bucket, a compacted bucket (two inputs
/// superseded, one unlisted survivor, two output parts), and a tombstoned
/// bucket: fold-then-resolve must equal resolve-before-fold, and the sealed
/// compacted/tombstoned buckets must be snapshot-served, never relisted.
#[tokio::test]
async fn fold_then_resolve_matches_direct_listing_with_compaction_and_tombstone() {
    let inner = Arc::new(MemoryStore::new());
    let base = 500_000u32;
    let h_plain = base;
    let h_comp = base + 1;
    let h_tomb = base + 2;
    let now = now_at_seal(base + 3);

    // Plain L0 bucket: two independent L0 segments, no transactions.
    let p1 = l0_record(Uuid::new_v4(), 0, 1, h_plain, now - 5 * NS_PER_HOUR);
    let p2 = l0_record(Uuid::new_v4(), 0, 2, h_plain, now - 5 * NS_PER_HOUR + 1);
    put_l0(inner.as_ref(), &p1).await;
    put_l0(inner.as_ref(), &p2).await;

    // Compacted bucket: a and b are superseded inputs, c is an unlisted
    // survivor, and the record publishes two L1 parts.
    let a = l0_record(Uuid::new_v4(), 0, 1, h_comp, now - 4 * NS_PER_HOUR);
    let b = l0_record(Uuid::new_v4(), 0, 2, h_comp, now - 4 * NS_PER_HOUR + 1);
    let c = l0_record(Uuid::new_v4(), 0, 3, h_comp, now - 4 * NS_PER_HOUR + 2);
    put_l0(inner.as_ref(), &a).await;
    put_l0(inner.as_ref(), &b).await;
    put_l0(inner.as_ref(), &c).await;
    put_compaction_record(
        inner.as_ref(),
        0,
        h_comp,
        &[&a, &b],
        vec![part(0, h_comp, 0x11), part(1, h_comp, 0x22)],
        now - 4 * NS_PER_HOUR + 10,
        b"comp-set",
    )
    .await;

    // Tombstoned bucket: an L0 record plus a retention tombstone; contributes
    // nothing.
    let d = l0_record(Uuid::new_v4(), 0, 1, h_tomb, now - 3 * NS_PER_HOUR);
    put_l0(inner.as_ref(), &d).await;
    put_tombstone(inner.as_ref(), 0, h_tomb).await;

    let range = full_window_range(base, now);

    // Direct listing (no fold has run).
    let before_catalog = Catalog::new(inner.clone(), config(1)).expect("catalog");
    let before = before_catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve before fold");

    // Fold, then resolve from the snapshot through a store that counts LISTs.
    let fold_catalog = Catalog::new(inner.clone(), config(1)).expect("catalog");
    let report = fold_catalog
        .fold(&tenant(), Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");
    assert!(!report.no_op);
    assert_eq!(report.watermark_hour, Some(base + 3));

    let (counting, log) = CountingStore::new(inner.clone());
    let after_catalog = Catalog::new(counting, config(1)).expect("catalog");
    let after = after_catalog
        .resolve(&tenant(), Signal::Metrics, range, &[], now)
        .await
        .expect("resolve after fold");

    // The load-bearing differential: both paths agree exactly.
    assert_eq!(before, after);

    // The population is non-trivial: two L1 parts and three surviving L0s
    // (the two plain segments plus the unlisted compaction survivor).
    let l1_count = before
        .segments
        .iter()
        .filter(|s| matches!(s.level, SegmentLevel::L1 { .. }))
        .count();
    let l0_count = before
        .segments
        .iter()
        .filter(|s| matches!(s.level, SegmentLevel::L0))
        .count();
    assert_eq!(
        l1_count, 2,
        "the compacted bucket contributes its two parts"
    );
    assert_eq!(l0_count, 3, "p1, p2, and the unlisted survivor c");
    assert_eq!(before.segments.len(), 5);

    // The superseded inputs and the tombstoned record are gone.
    let a_key = keys::reconstruct_data_key(&a).expect("a data key");
    let b_key = keys::reconstruct_data_key(&b).expect("b data key");
    let c_key = keys::reconstruct_data_key(&c).expect("c data key");
    let d_key = keys::reconstruct_data_key(&d).expect("d data key");
    let present: std::collections::HashSet<&str> = after
        .segments
        .iter()
        .map(|s| s.data_object_key.as_str())
        .collect();
    assert!(!present.contains(a_key.as_str()), "input a is superseded");
    assert!(!present.contains(b_key.as_str()), "input b is superseded");
    assert!(present.contains(c_key.as_str()), "unlisted c survives");
    assert!(
        !present.contains(d_key.as_str()),
        "d's bucket is tombstoned"
    );

    // The compacted and tombstoned buckets sit at or below the watermark, so a
    // resolve after the fold serves them straight from the snapshot part and
    // never lists them: that is what proves the L1 parts and the exclusions
    // came through the snapshot, not a listing fallback.
    let comp_prefix =
        keys::commit_shard_hour_prefix(&tenant(), Signal::Metrics, 0, h_comp).unwrap();
    let tomb_prefix =
        keys::commit_shard_hour_prefix(&tenant(), Signal::Metrics, 0, h_tomb).unwrap();
    assert_eq!(
        log.list_count_for(&comp_prefix),
        0,
        "the compacted bucket must be snapshot-served, not relisted"
    );
    assert_eq!(
        log.list_count_for(&tomb_prefix),
        0,
        "the tombstoned bucket must be snapshot-served, not relisted"
    );
}
