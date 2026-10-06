//! In-process tests for `ravel-cli maintain` (P8): the subcommands drive
//! ravel-maintain against a shared MemoryStore. These exercise the CLI glue
//! and its output paths on an empty store (the compaction/sweep/retention
//! decision logic itself is tested in ravel-maintain); the decode tests pin
//! the proto field printing.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use ravel_cli::hold;
use ravel_cli::maintain::{
    ClaimOptions, MigrateSwitches, SignalArg, audit_versions, compact, decode_compaction_record,
    decode_retention_tombstone, migrate, status, sweep, verify_custody,
};
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_fleet::claim::{WorkIdentity, compaction_claim_key};
use ravel_logseg::{AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::{
    CompactionInputIdentity, CompactionPart, CompactionRecord, RetentionTombstone,
};
use ravel_proto::sys::v1::{ClaimState, CompactionClaim};
use ravel_segment::VERSION_V7;
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

/// Nanoseconds per unix hour, the unit `ingest_hour_bucket` counts in.
const NS_PER_HOUR: i64 = 3_600_000_000_000;

fn store() -> Arc<dyn ObjectStoreBackend> {
    Arc::new(MemoryStore::new())
}

/// These tests build their own `MemoryStore`, which is the explicit
/// `--store memory` case (issue #1024): each report header reads
/// `store: memory`, and the empty-store cases below stay successes rather than
/// becoming the defaulted-store refusal.
const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

/// Publish one L0 metrics segment (data object + commit record) and return the
/// content-addressed data-object key, so a test can later corrupt the object
/// at that key. Content is `seg-<shard>-<seq>`; its blake3 is what the key's
/// hash16 embeds, which is exactly what `verify-custody` re-derives.
async fn publish_l0(
    store: &MemoryStore,
    tenant: &str,
    shard: u32,
    seq: u64,
    created_unix_ns: i64,
) -> String {
    let tenant_hash = TenantId::new(tenant).hash();
    let ingest_hour_bucket = u32::try_from(created_unix_ns / 3_600_000_000_000).expect("fits u32");
    let payload = format!("seg-{shard}-{seq}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id: Uuid::new_v4(),
        writer_epoch: 1,
        writer_seq: seq,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: created_unix_ns - 1_000,
        max_event_ts_ns: created_unix_ns,
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    data_key
}

/// Seed one metrics compaction record (shard 0, hour 100) with a single L1
/// part written at its reconstructed key, plus the given input identities. The
/// L1 part's content matches its key's hash16 (so the part itself is never an
/// anomaly); whether each input object exists is up to the caller.
async fn seed_compaction(store: &MemoryStore, tenant: &str, inputs: &[(Uuid, u64, u64)]) {
    let tenant_hash = TenantId::new(tenant).hash();
    let part_payload = b"l1-part-content".to_vec();
    let part_hash = *blake3::hash(&part_payload).as_bytes();
    let part = CompactionPart {
        part_index: 0,
        first_series_id: vec![0u8; 16],
        last_series_id: vec![0u8; 16],
        content_hash: part_hash.to_vec(),
        object_size: part_payload.len() as u64,
        sample_count: 1,
        series_count: 1,
        run_count: 1,
        min_event_ts_ns: 100,
        max_event_ts_ns: 200,
        segment_format_version: u32::from(VERSION_V7),
        declared_column_stats: Vec::new(),
    };
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: tenant_hash.0.to_vec(),
        signal: ravel_commit::signal::to_proto(Signal::Metrics) as i32,
        shard: 0,
        ingest_hour_bucket: 100,
        level: 1,
        inputs: inputs
            .iter()
            .map(|(id, epoch, seq)| CompactionInputIdentity {
                writer_id: id.to_string(),
                writer_epoch: *epoch,
                writer_seq: *seq,
            })
            .collect(),
        input_set_hash: vec![7u8; 32],
        parts: vec![part.clone()],
        created_unix_ns: 999,
        superseded_record_key: String::new(),
    };
    let part_key = keys::reconstruct_l1_part_key(&record, &part).expect("part key");
    store
        .put(&part_key, Bytes::from(part_payload), PutOptions::default())
        .await
        .expect("put l1 part");
    let record_key = keys::compaction_record_key_for(&record).expect("record key");
    store
        .put(
            &record_key,
            Bytes::from(record.encode_to_vec()),
            PutOptions::default(),
        )
        .await
        .expect("put compaction record");
}

/// Put an L0 data object at the content-addressed key for `content_hash`, but
/// store `stored_content` as its bytes. When `stored_content` does not hash to
/// `content_hash`, the object's content no longer matches the hash16 its key
/// embeds: exactly the post-write corruption `verify-custody` must catch.
async fn put_l0_object_at(
    store: &MemoryStore,
    tenant: &str,
    shard: u32,
    identity: (Uuid, u64, u64),
    content_hash: [u8; 32],
    stored_content: &[u8],
) {
    let tenant_hash = TenantId::new(tenant).hash();
    let key = keys::data_key(
        &tenant_hash,
        Signal::Metrics,
        shard,
        identity.0,
        identity.1,
        identity.2,
        &content_hash,
    )
    .expect("data key");
    store
        .put(
            &key,
            Bytes::copy_from_slice(stored_content),
            PutOptions::default(),
        )
        .await
        .expect("put l0 object");
}

/// Like `publish_l0`, but with a caller-controlled writer identity so a test
/// can seed a compaction record whose `inputs` name this exact object.
async fn publish_l0_with_identity(
    store: &MemoryStore,
    tenant: &str,
    shard: u32,
    identity: (Uuid, u64, u64),
    created_unix_ns: i64,
) -> String {
    let tenant_hash = TenantId::new(tenant).hash();
    let ingest_hour_bucket = u32::try_from(created_unix_ns / 3_600_000_000_000).expect("fits u32");
    let payload = format!("seg-{shard}-{}", identity.2).into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard,
        writer_id: identity.0,
        writer_epoch: identity.1,
        writer_seq: identity.2,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: created_unix_ns - 1_000,
        max_event_ts_ns: created_unix_ns,
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket,
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    publish::put_data_object(store, &data_key, Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
    data_key
}

/// Put a record-less `l0/` data object directly, for orphan-GC tests. Content
/// need not match any embedded hash16 verification path, since orphan GC
/// never re-hashes: it deletes on record absence and age alone.
async fn seed_orphan_l0(store: &MemoryStore, tenant: &str, shard: u32, seq: u64) {
    let tenant_hash = TenantId::new(tenant).hash();
    let payload = format!("orphan-{shard}-{seq}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let key = keys::data_key(
        &tenant_hash,
        Signal::Metrics,
        shard,
        Uuid::new_v4(),
        1,
        seq,
        &content_hash,
    )
    .expect("data key");
    store
        .put(&key, Bytes::from(payload), PutOptions::default())
        .await
        .expect("put orphan data object");
}

#[tokio::test]
async fn compact_empty_bucket_is_below_min() {
    // Hour 0 is long sealed; an empty bucket has zero inputs, below the
    // min-inputs trigger, so a dry run reports it and writes nothing.
    compact(
        store(),
        MEMORY,
        "acme",
        SignalArg::Metrics,
        0,
        0,
        true,
        None,
        None,
        &ClaimOptions::fresh(),
    )
    .await
    .expect("compact dry-run runs");
}

fn logs_record(stream: u8, ts_ns: i64) -> LogRecord {
    let mut id = [0u8; 16];
    id[0] = stream;
    LogRecord {
        stream_id: LogStreamId(id),
        stream_attrs: ravel_logseg::stream_attrs_bytes(
            &[(
                "service.name".into(),
                AttrValue::Str(format!("svc-{stream}")),
            )],
            "scope",
            "1",
            &[],
        ),
        ts_ns,
        observed_ts_ns: ts_ns,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: "get /api ok".into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("code".into(), AttrValue::I64(200))],
    }
}

/// Seed two L0 `.rlog` objects (the default `min_compaction_inputs`) into one
/// (shard, hour) bucket, exactly as an ingest log shard would, so
/// `compact-bucket` actually compacts rather than reporting `BelowMinInputs`.
/// `hour` is chosen by the caller far enough in the past that it is sealed
/// against the CLI's own real wall clock (`compact` takes no injected
/// `now_ns`; see `wall_clock` in `src/maintain.rs`).
async fn seed_two_l0_logs(store: &MemoryStore, tenant: &str, shard: u32, hour: u32) {
    let tenant_hash = TenantId::new(tenant).hash();
    let base_ns = i64::from(hour) * NS_PER_HOUR;
    for seq in 1..=2u64 {
        let records: Vec<LogRecord> = (0..4)
            .map(|i| {
                logs_record(
                    u8::try_from(i % 2).expect("fits u8"),
                    base_ns + i64::from(i) * 1_000_000 + i64::try_from(seq).expect("fits i64"),
                )
            })
            .collect();
        let writer_id = Uuid::new_v4();
        let identity = ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard,
            writer_id: writer_id.into_bytes(),
            writer_epoch: 1,
            writer_seq: seq,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity);
        for r in &records {
            writer.push(r.clone()).expect("push");
        }
        let bytes = Bytes::from(writer.finish().expect("finish L0"));
        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data_key = keys::data_key(
            &tenant_hash,
            Signal::Logs,
            shard,
            writer_id,
            1,
            seq,
            &content_hash,
        )
        .expect("data key");
        store
            .put(&data_key, bytes.clone(), PutOptions::default())
            .await
            .expect("put data object");

        let streams: BTreeSet<LogStreamId> = records.iter().map(|r| r.stream_id).collect();
        let min_ts = records.iter().map(|r| r.ts_ns).min().expect("nonempty");
        let max_ts = records.iter().map(|r| r.ts_ns).max().expect("nonempty");
        let created = base_ns + i64::try_from(seq).expect("fits i64") * 1_000_000;
        let rec = record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard,
            writer_id,
            writer_epoch: 1,
            writer_seq: seq,
            object_size: bytes.len() as u64,
            content_hash,
            sample_count: records.len() as u64,
            series_count: streams.len() as u64,
            min_event_ts_ns: min_ts,
            max_event_ts_ns: max_ts,
            min_ingest_ts_ns: created,
            max_ingest_ts_ns: created,
            segment_format_version: u32::from(ravel_logseg::footer::VERSION),
            created_unix_ns: created,
            ingest_hour_bucket: hour,
        })
        .expect("build commit record");
        let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put commit record");
    }
}

/// The CLI `compact-bucket` path publishes a compaction record with no
/// worker-ownership check and no heartbeat write: `sys/maintain/workers/` (the
/// worker-set liveness prefix, docs/catalog-and-mvcc.md) stays empty across the
/// whole run. The compaction claim the path takes on every bucket (#1034,
/// #2199) is a different keyspace and says nothing about worker
/// membership, which is exactly the separation this pins.
#[tokio::test]
async fn cli_compact_bucket_publishes_without_holding_ownership() {
    let store = MemoryStore::new();
    let tenant = "acme";
    seed_two_l0_logs(&store, tenant, 0, 100).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);

    compact(
        store.clone(),
        MEMORY,
        tenant,
        SignalArg::Logs,
        0,
        100,
        false,
        None,
        None,
        &ClaimOptions::fresh(),
    )
    .await
    .expect("compaction runs");

    let tenant_hash = TenantId::new(tenant).hash();
    let bucket = ravel_maintain::Bucket::new(tenant_hash, Signal::Logs, 0, 100);
    let listing = ravel_maintain::read::list_bucket(store.as_ref(), &bucket)
        .await
        .expect("list bucket");
    assert_eq!(
        listing.compaction_record_keys.len(),
        1,
        "the CLI compaction published a compaction record"
    );

    let heartbeats = list_all(store.as_ref(), "sys/maintain/workers/")
        .await
        .expect("list workers prefix");
    assert!(
        heartbeats.is_empty(),
        "the CLI compaction path took no worker heartbeat: {heartbeats:?}"
    );
}

/// A `compact-bucket` run below the retired claim cost gate still takes the
/// bucket's compaction claim (`sys/maintain/claims/compaction/`, ADR-1029 and
/// its 2026-10-03 amendment): the two-record fixture here is a few hundred
/// bytes, far under the 64 MiB `claim_min_input_bytes` default, which no
/// longer decides anything, so the run claims the bucket, merges it, and
/// leaves exactly one claim, completed, under its own process id.
#[tokio::test]
async fn cli_compact_bucket_below_the_retired_claim_gate_takes_the_claim() {
    let store = MemoryStore::new();
    let tenant = "acme";
    seed_two_l0_logs(&store, tenant, 0, 100).await;
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(store);
    let claims = ClaimOptions::fresh();

    compact(
        store.clone(),
        MEMORY,
        tenant,
        SignalArg::Logs,
        0,
        100,
        false,
        None,
        None,
        &claims,
    )
    .await
    .expect("compaction runs");

    let tenant_hash = TenantId::new(tenant).hash();
    let bucket = ravel_maintain::Bucket::new(tenant_hash, Signal::Logs, 0, 100);
    let listing = ravel_maintain::read::list_bucket(store.as_ref(), &bucket)
        .await
        .expect("list bucket");
    assert_eq!(
        listing.compaction_record_keys.len(),
        1,
        "the CLI compaction published a compaction record"
    );

    let claim_keys = list_all(store.as_ref(), "sys/maintain/claims/compaction/")
        .await
        .expect("list claims prefix");
    let work_id = WorkIdentity::new(tenant_hash, Signal::Logs, 0, 100).work_id();
    assert_eq!(
        claim_keys
            .iter()
            .map(|meta| meta.key.clone())
            .collect::<Vec<_>>(),
        vec![compaction_claim_key(&work_id)],
        "a bucket below the retired cost gate wrote exactly its own compaction claim"
    );
    let got = store
        .get(&claim_keys[0].key, GetRange::Full)
        .await
        .expect("get claim");
    let claim = CompactionClaim::decode(got.data).expect("decode claim");
    assert_eq!(
        claim.owner_process_id,
        claims.process_id.as_bytes().to_vec(),
        "the claim is this run's"
    );
    assert_eq!(
        claim.state,
        ClaimState::Completed as i32,
        "and the run marked it completed after publishing"
    );
}

#[tokio::test]
async fn sweep_empty_shard_dry_run_is_clean() {
    sweep(store(), MEMORY, "acme", SignalArg::Logs, 0, true, false)
        .await
        .expect("sweep dry-run runs");
}

/// A superseded input's commit record and data object survive a real sweep
/// pass when the shard's L0 prefix is under legal hold, even though the
/// compaction record's `created_unix_ns` (999, far in the past) is well
/// beyond the default protection horizon and would otherwise make the input
/// immediately eligible for deletion.
#[tokio::test]
async fn sweep_does_not_delete_data_under_legal_hold() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "acme";
    let identity = (Uuid::new_v4(), 1, 1);
    let data_key =
        publish_l0_with_identity(&store, tenant, 0, identity, 100 * 3_600_000_000_000).await;
    seed_compaction(&store, tenant, &[identity]).await;

    let tenant_hash = TenantId::new(tenant).hash();
    let commit_key = keys::commit_key(
        &tenant_hash,
        Signal::Metrics,
        0,
        100,
        identity.0,
        identity.1,
        identity.2,
    )
    .expect("commit key");

    // Hold the shard's three prefixes via the --signal/--shard sugar before
    // sweeping.
    hold::set(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        tenant,
        None,
        Some(SignalArg::Metrics),
        Some(0),
        "litigation hold",
    )
    .await
    .expect("hold set succeeds");

    sweep(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SignalArg::Metrics,
        0,
        false,
        false,
    )
    .await
    .expect("sweep runs");

    assert!(
        store.get(&data_key, GetRange::Full).await.is_ok(),
        "held data object must survive sweep"
    );
    assert!(
        store.get(&commit_key, GetRange::Full).await.is_ok(),
        "held commit record must survive sweep"
    );
}

/// `--override-orphan-breaker` forces exactly the one pass it is given for:
/// it does not persist, so a tripped breaker trips again on the very next
/// invocation with a fresh batch of orphan candidates.
#[tokio::test]
async fn override_orphan_breaker_runs_exactly_one_forced_pass() {
    let store = Arc::new(MemoryStore::new());
    let tenant = "acme";
    let tenant_hash = TenantId::new(tenant).hash();
    let prefix = format!(
        "t/{}/{}/l0/0000/",
        tenant_hash.to_hex(),
        Signal::Metrics.key_prefix()
    );

    for seq in 0..60u64 {
        seed_orphan_l0(&store, tenant, 0, seq).await;
    }

    // Without override: the mass-orphan breaker trips (60 candidates, 100% of
    // the shard's listed L0 objects), deleting nothing.
    sweep(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SignalArg::Metrics,
        0,
        false,
        false,
    )
    .await
    .expect("sweep runs even though the breaker trips");
    let after_tripped = list_all(store.as_ref(), &prefix).await.expect("list");
    assert_eq!(after_tripped.len(), 60, "a tripped breaker deletes nothing");

    // With override: this one pass deletes despite the breaker's threshold.
    sweep(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SignalArg::Metrics,
        0,
        false,
        true,
    )
    .await
    .expect("overridden sweep runs");
    let after_override = list_all(store.as_ref(), &prefix).await.expect("list");
    assert_eq!(
        after_override.len(),
        0,
        "the override must delete every orphan candidate"
    );

    // A fresh batch, swept again without the override: the breaker must trip
    // again, proving the prior override did not persist.
    for seq in 100..160u64 {
        seed_orphan_l0(&store, tenant, 0, seq).await;
    }
    sweep(
        store.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        tenant,
        SignalArg::Metrics,
        0,
        false,
        false,
    )
    .await
    .expect("sweep runs");
    let after_second_round = list_all(store.as_ref(), &prefix).await.expect("list");
    assert_eq!(
        after_second_round.len(),
        60,
        "the breaker override must not persist across invocations"
    );
}

#[tokio::test]
async fn status_empty_bucket_is_clean() {
    status(store(), MEMORY, "acme", SignalArg::Metrics, 0, 0)
        .await
        .expect("status runs");
}

#[tokio::test]
async fn audit_versions_empty_store_finds_no_anomaly() {
    audit_versions(store(), MEMORY, "acme", 4)
        .await
        .expect("audit over an empty store reports no live objects and no anomaly");
}

#[tokio::test]
async fn verify_custody_empty_store_is_clean() {
    verify_custody(store(), MEMORY, "acme", 4, false)
        .await
        .expect("empty store has no live objects and no anomaly");
}

#[tokio::test]
async fn verify_custody_clean_store_verifies_every_object() {
    let store = Arc::new(MemoryStore::new());
    publish_l0(&store, "acme", 0, 1, 100 * 3_600_000_000_000).await;
    publish_l0(&store, "acme", 0, 2, 100 * 3_600_000_000_000).await;
    // A compaction record whose L1 part is present and matches, with an input
    // that was legitimately swept (no L0 object seeded for it).
    seed_compaction(&store, "acme", &[(Uuid::new_v4(), 1, 1)]).await;

    verify_custody(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        "acme",
        1,
        false,
    )
    .await
    .expect("a clean store passes custody verification");
}

#[tokio::test]
async fn verify_custody_catches_a_corrupted_data_object() {
    let store = Arc::new(MemoryStore::new());
    let data_key = publish_l0(&store, "acme", 0, 1, 100 * 3_600_000_000_000).await;
    // Overwrite the object's bytes so its content no longer hashes to the
    // hash16 embedded in its key: post-write corruption.
    store
        .put(
            &data_key,
            Bytes::from_static(b"corrupted-after-write"),
            PutOptions::default(),
        )
        .await
        .expect("overwrite the data object");

    let err = verify_custody(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        "acme",
        1,
        false,
    )
    .await
    .expect_err("a corrupted data object must fail verification");
    assert!(
        err.to_string().contains("content-hash mismatch"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn verify_custody_treats_a_legitimately_swept_input_as_no_anomaly() {
    let store = Arc::new(MemoryStore::new());
    // A compaction record referencing an input identity for which no L0 object
    // exists: the sweeper reclaimed it past its protection horizon. The L1
    // part is present and matches. This must not be an anomaly.
    seed_compaction(&store, "acme", &[(Uuid::new_v4(), 1, 1)]).await;

    verify_custody(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        "acme",
        1,
        false,
    )
    .await
    .expect("a swept (missing) compaction input is expected, not an anomaly");
}

#[tokio::test]
async fn verify_custody_catches_a_compaction_input_with_a_mismatched_hash() {
    let store = Arc::new(MemoryStore::new());
    let identity = (Uuid::new_v4(), 1, 1);
    // The input object exists but its content does not match the hash16 its
    // key embeds (the key is derived from `good`, the stored bytes are not).
    let good = b"good-input-content";
    let content_hash = *blake3::hash(good).as_bytes();
    put_l0_object_at(&store, "acme", 0, identity, content_hash, b"corrupted").await;
    seed_compaction(&store, "acme", &[identity]).await;

    let err = verify_custody(
        store as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        "acme",
        1,
        false,
    )
    .await
    .expect_err("an existing-but-mismatched input must fail verification");
    assert!(
        err.to_string().contains("content-hash mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn decode_compaction_record_prints_fields() {
    let record = CompactionRecord {
        format_version: 1,
        tenant_hash: vec![0u8; 16],
        signal: 1,
        shard: 3,
        ingest_hour_bucket: 42,
        level: 1,
        inputs: vec![CompactionInputIdentity {
            writer_id: "00000000-0000-0000-0000-000000000001".to_string(),
            writer_epoch: 10,
            writer_seq: 1,
        }],
        input_set_hash: vec![0xabu8; 32],
        parts: vec![CompactionPart {
            part_index: 0,
            first_series_id: vec![1u8; 16],
            last_series_id: vec![2u8; 16],
            content_hash: vec![3u8; 32],
            object_size: 1234,
            sample_count: 5,
            series_count: 2,
            run_count: 3,
            min_event_ts_ns: 100,
            max_event_ts_ns: 200,
            segment_format_version: u32::from(VERSION_V7),
            declared_column_stats: Vec::new(),
        }],
        created_unix_ns: 999,
        superseded_record_key: String::new(),
    };
    decode_compaction_record(&record.encode_to_vec()).expect("decode + print");
}

/// `maintain migrate` glue: on a provisioned tenant with no data below the
/// target, the walk drains, the fresh re-audit is clean, and the floor is
/// raised to the signal's current version (the default target). This exercises
/// the CLI arg defaulting and the verify-then-raise path end to end; the
/// resumability and race logic themselves are tested in ravel-maintain.
#[tokio::test]
async fn migrate_raises_floor_on_a_clean_tenant() {
    let store = store();
    let tenant = "cli-migrate-clean";
    let tenant_hash = TenantId::new(tenant).hash();
    ravel_catalog::validate_or_adopt(
        store.as_ref(),
        &tenant_hash,
        Signal::Metrics,
        4,
        0,
        ravel_catalog::AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");

    migrate(
        store.clone(),
        MEMORY,
        tenant,
        SignalArg::Metrics,
        4,
        None,
        None,
        0,
        MigrateSwitches::default(),
        &ClaimOptions::fresh(),
    )
    .await
    .expect("migrate raises the floor on a clean tenant");

    let floor = ravel_catalog::current_floor_from_store(
        store.as_ref(),
        &tenant_hash,
        Signal::Metrics,
        "rseg",
    )
    .await
    .expect("read floor");
    assert_eq!(
        floor,
        Some(u32::from(VERSION_V7)),
        "the rseg floor is raised to the current version"
    );
}

/// `maintain migrate` glue: a below-target record that the walk cannot migrate
/// (here, one still in the current, unsealed ingest hour) is caught by the
/// fresh re-audit, so migrate exits nonzero and does NOT raise the floor. This
/// is the CLI surface of the race-safety guarantee.
#[tokio::test]
async fn migrate_exits_nonzero_and_holds_the_floor_when_a_straggler_survives() {
    let mem = Arc::new(MemoryStore::new());
    let store: Arc<dyn ObjectStoreBackend> = mem.clone();
    let tenant = "cli-migrate-straggler";
    let tenant_hash = TenantId::new(tenant).hash();
    ravel_catalog::validate_or_adopt(
        store.as_ref(),
        &tenant_hash,
        Signal::Metrics,
        4,
        0,
        ravel_catalog::AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");

    // A below-target (version 1 < VERSION_V7) commit record in the current,
    // still-unsealed ingest hour: the walk examines but cannot migrate it, and
    // the fresh re-audit counts it. `migrate` reads only the record's recorded
    // version here (never decodes the object), so a placeholder L0 payload is
    // enough.
    let now = ravel_cli::now_ns().expect("wall clock");
    publish_l0(mem.as_ref(), tenant, 0, 1, now).await;

    let err = migrate(
        store.clone(),
        MEMORY,
        tenant,
        SignalArg::Metrics,
        4,
        None,
        None,
        0,
        MigrateSwitches::default(),
        &ClaimOptions::fresh(),
    )
    .await
    .expect_err("a fresh straggler must make migrate exit nonzero");
    assert!(
        err.to_string().contains("refused to raise"),
        "the error must report the refused floor raise: {err}"
    );

    let floor = ravel_catalog::current_floor_from_store(
        store.as_ref(),
        &tenant_hash,
        Signal::Metrics,
        "rseg",
    )
    .await
    .expect("read floor");
    assert_eq!(floor, None, "a refused verify raises no floor");
}

#[test]
fn decode_retention_tombstone_prints_fields() {
    let tombstone = RetentionTombstone {
        format_version: 1,
        tenant_hash: vec![0u8; 16],
        signal: 2,
        shard: 1,
        ingest_hour_bucket: 7,
        retired_at_ns: 555,
        retention_window_ns: 2_592_000_000_000_000,
        record_count_observed: 12,
    };
    decode_retention_tombstone(&tombstone.encode_to_vec()).expect("decode + print");
}

/// The injected `now` the `sys/gc` sweep tests run at: far enough past the
/// MemoryStore's epoch-zero `last_modified` that a seeded orphan is past every
/// age gate.
const SWEEP_NOW_NS: i64 = 100 * NS_PER_HOUR;

/// A `sys/gc` proposal whose `max_query_duration` (2h) and HEAD cache TTL
/// (10s) differ from the compiled defaults, with the horizon raised to cover
/// them and the default skew allowance.
fn non_default_gc_proposal() -> ravel_maintain::GcConfigProposal {
    let defaults = ravel_maintain::GcConfigValues::maintain_defaults();
    let max_query_duration_ns = 2 * NS_PER_HOUR;
    ravel_maintain::GcConfigProposal {
        protection_horizon_ns: max_query_duration_ns
            + defaults.grace_ns
            + ravel_maintain::config::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
        max_query_duration_ns,
        max_flush_lifetime_ns: 6 * NS_PER_HOUR,
        head_cache_ttl_ns: Some(10_000_000_000),
        ..defaults.into()
    }
}

/// ADR-1133 decision 4: `maintain sweep` takes `protection_horizon`, `grace`,
/// `max_query_duration` and `head_cache_ttl` from `sys/gc`, not from
/// `CompactorConfig::default()`.
#[tokio::test]
async fn sweep_config_carries_the_stored_sys_gc_values() {
    let store = store();
    let proposal = non_default_gc_proposal();
    ravel_maintain::set_gc_config(
        store.as_ref(),
        proposal,
        ravel_maintain::config::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
        1,
    )
    .await
    .expect("set sys/gc");
    let compiled = ravel_maintain::CompactorConfig::default();
    assert_ne!(
        compiled.max_query_duration_ns,
        proposal.max_query_duration_ns
    );

    let config =
        ravel_cli::maintain::sweep_compactor_config(store.as_ref(), false, false, SWEEP_NOW_NS)
            .await
            .expect("a skew-covering sys/gc passes");
    assert_eq!(config.max_query_duration_ns, 2 * NS_PER_HOUR);
    assert_eq!(config.head_cache_ttl_ns, 10_000_000_000);
    assert_eq!(config.protection_horizon_ns, proposal.protection_horizon_ns);
    assert_eq!(config.grace_ns, proposal.grace_ns);
    assert_eq!(config.max_flush_lifetime_ns, 6 * NS_PER_HOUR);
    assert_eq!(
        config.orphan_age_gate_ns(),
        proposal.grace_ns + 6 * NS_PER_HOUR,
        "the orphan gate sums grace and max_flush_lifetime, both from sys/gc"
    );
    assert_eq!(
        config.clock_skew_allowance_ns,
        ravel_maintain::config::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
    );
}

/// On a bucket with no `sys/gc`, a sweep bootstraps it at version 1 from the
/// maintain defaults, as the server's maintain mode does, and a dry run uses
/// the same defaults without writing.
#[tokio::test]
async fn sweep_config_bootstraps_an_absent_sys_gc_and_a_dry_run_writes_none() {
    let store = store();
    let dry = ravel_cli::maintain::sweep_compactor_config(store.as_ref(), true, false, 1)
        .await
        .expect("dry run on an absent sys/gc");
    assert!(
        store
            .get(ravel_maintain::GC_CONFIG_KEY, GetRange::Full)
            .await
            .is_err(),
        "a dry run writes no sys/gc"
    );
    let real = ravel_cli::maintain::sweep_compactor_config(store.as_ref(), false, false, 1)
        .await
        .expect("bootstrap on an absent sys/gc");
    let (stored, _version) = ravel_maintain::read_gc_config(store.as_ref())
        .await
        .expect("read")
        .expect("bootstrapped");
    assert_eq!(stored, ravel_maintain::GcConfigValues::maintain_defaults());
    for config in [dry, real] {
        assert_eq!(config.protection_horizon_ns, stored.protection_horizon_ns);
        assert_eq!(config.max_query_duration_ns, stored.max_query_duration_ns);
        assert_eq!(config.head_cache_ttl_ns, stored.head_cache_ttl_ns);
    }
}

/// A stored `sys/gc` whose horizon does not cover the sweep's own clock-skew
/// allowance (written with `--clock-skew-allowance 0s`) makes `maintain sweep`
/// refuse before any store write. Every put and delete through the store is
/// scripted to fault, so the fault counters count every write the sweep
/// attempts; the control run against a skew-covering `sys/gc` shows the same
/// store and orphan do draw a write.
#[tokio::test]
async fn sweep_refuses_a_skew_uncovered_sys_gc_before_any_store_write() {
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};

    async fn seeded(horizon_covers_skew: bool) -> MemoryStore {
        let store = MemoryStore::new();
        seed_orphan_l0(&store, "acme", 0, 1).await;
        let defaults = ravel_maintain::GcConfigValues::maintain_defaults();
        let skew = if horizon_covers_skew {
            ravel_maintain::config::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS
        } else {
            0
        };
        ravel_maintain::set_gc_config(
            &store,
            ravel_maintain::GcConfigProposal {
                protection_horizon_ns: defaults.max_query_duration_ns + defaults.grace_ns + skew,
                ..defaults.into()
            },
            skew,
            1,
        )
        .await
        .expect("set sys/gc");
        store
    }
    fn faulting(inner: MemoryStore) -> Arc<FaultStore<MemoryStore>> {
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(
                Op::Put,
                ScriptedFault::Permanent("no put".into()),
            ))
            .with_rule(Rule::new(
                Op::Delete,
                ScriptedFault::Permanent("no delete".into()),
            ));
        Arc::new(FaultStore::new(inner, plan))
    }
    async fn run(store: &Arc<FaultStore<MemoryStore>>) -> anyhow::Result<()> {
        ravel_cli::maintain::sweep_at(
            store.clone() as Arc<dyn ObjectStoreBackend>,
            MEMORY,
            "acme",
            SignalArg::Metrics,
            0,
            false,
            false,
            ravel_maintain::FixedClock::new(SWEEP_NOW_NS),
        )
        .await
    }
    let writes = |store: &FaultStore<MemoryStore>| {
        store.fault_count(Op::Put, FaultKind::Permanent)
            + store.fault_count(Op::Delete, FaultKind::Permanent)
    };

    let uncovered = faulting(seeded(false).await);
    let err = run(&uncovered)
        .await
        .expect_err("a skew-uncovered sys/gc must refuse the sweep");
    assert!(
        err.to_string()
            .contains("refusing to enter the maintain sweep loop"),
        "the refusal is the skew validation: {err}"
    );
    assert_eq!(writes(&uncovered), 0, "the sweep refused before any write");

    let covered = faulting(seeded(true).await);
    let _ = run(&covered).await;
    assert!(
        writes(&covered) > 0,
        "control: a skew-covering sys/gc lets the same sweep reach a write"
    );
}

/// A stored `sys/gc` horizon one nanosecond below
/// `max_compaction_lifetime + 4 * clock_skew_allowance` for this sweep's
/// compiled config (ADR-1133) makes `maintain sweep` refuse before any store
/// write, though the skew-covering bound accepts it. The horizon is written
/// with a zero write-time skew, which the write fence then accepts. The
/// control at the bound reaches a write on the same store and orphan.
///
/// Flip to watch it fail: drop the `validate_maintain_compaction_lifetime`
/// call in `sweep_compactor_config`; the `expect_err` panics.
#[tokio::test]
async fn sweep_refuses_a_horizon_a_compaction_run_can_outlive_before_any_store_write() {
    use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};

    let compiled = ravel_maintain::CompactorConfig::default();
    let bound = compiled.max_compaction_lifetime_ns + 4 * compiled.clock_skew_allowance_ns;
    async fn seeded(protection_horizon_ns: i64) -> MemoryStore {
        let store = MemoryStore::new();
        seed_orphan_l0(&store, "acme", 0, 1).await;
        ravel_maintain::set_gc_config(
            &store,
            ravel_maintain::GcConfigProposal {
                protection_horizon_ns,
                grace_ns: 60_000_000_000,
                max_query_duration_ns: 60_000_000_000,
                ..ravel_maintain::GcConfigValues::maintain_defaults().into()
            },
            0,
            1,
        )
        .await
        .expect("set sys/gc");
        store
    }
    fn faulting(inner: MemoryStore) -> Arc<FaultStore<MemoryStore>> {
        let plan = FaultPlan::empty()
            .with_rule(Rule::new(
                Op::Put,
                ScriptedFault::Permanent("no put".into()),
            ))
            .with_rule(Rule::new(
                Op::Delete,
                ScriptedFault::Permanent("no delete".into()),
            ));
        Arc::new(FaultStore::new(inner, plan))
    }
    async fn run(store: &Arc<FaultStore<MemoryStore>>) -> anyhow::Result<()> {
        ravel_cli::maintain::sweep_at(
            store.clone() as Arc<dyn ObjectStoreBackend>,
            MEMORY,
            "acme",
            SignalArg::Metrics,
            0,
            false,
            false,
            ravel_maintain::FixedClock::new(SWEEP_NOW_NS),
        )
        .await
    }
    let writes = |store: &FaultStore<MemoryStore>| {
        store.fault_count(Op::Put, FaultKind::Permanent)
            + store.fault_count(Op::Delete, FaultKind::Permanent)
    };

    let short = faulting(seeded(bound - 1).await);
    let err = run(&short)
        .await
        .expect_err("a horizon a compaction run can outlive must refuse the sweep");
    assert!(
        err.to_string().contains("max_compaction_lifetime"),
        "the refusal is the compaction-lifetime validation: {err}"
    );
    assert_eq!(writes(&short), 0, "the sweep refused before any write");

    let at_bound = faulting(seeded(bound).await);
    let _ = run(&at_bound).await;
    assert!(
        writes(&at_bound) > 0,
        "control: a horizon at the bound lets the same sweep reach a write"
    );
}

/// `maintain sweep` holds a superseded input on the stored `sys/gc` protection
/// horizon, not the compiled default: with a stored horizon of 200h and the
/// compaction record 150h old, the input survives, and the control bucket on
/// the default 25h05m horizon is past its horizon at the same instant. The
/// control's first run writes the input's unnamed-since marker and holds it
/// (ADR-1133), and a second run once the pinned-query window has passed
/// deletes it.
#[tokio::test]
async fn sweep_holds_a_superseded_input_on_the_stored_horizon() {
    const NOW_NS: i64 = 999 + 150 * NS_PER_HOUR;
    let default_horizon = ravel_maintain::CompactorConfig::default().protection_horizon_ns;
    let stored_horizon = 200 * NS_PER_HOUR;
    assert!(999 + default_horizon < NOW_NS && NOW_NS < 999 + stored_horizon);

    async fn run(stored_horizon_ns: Option<i64>) -> (Arc<MemoryStore>, String) {
        let store = Arc::new(MemoryStore::new());
        let identity = (Uuid::new_v4(), 1, 1);
        let data_key =
            publish_l0_with_identity(&store, "acme", 0, identity, 100 * NS_PER_HOUR).await;
        seed_compaction(&store, "acme", &[identity]).await;
        if let Some(protection_horizon_ns) = stored_horizon_ns {
            ravel_maintain::set_gc_config(
                store.as_ref(),
                ravel_maintain::GcConfigProposal {
                    protection_horizon_ns,
                    ..ravel_maintain::GcConfigValues::maintain_defaults().into()
                },
                ravel_maintain::config::DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
                1,
            )
            .await
            .expect("set sys/gc");
        }
        ravel_cli::maintain::sweep_at(
            store.clone() as Arc<dyn ObjectStoreBackend>,
            MEMORY,
            "acme",
            SignalArg::Metrics,
            0,
            false,
            false,
            ravel_maintain::FixedClock::new(NOW_NS),
        )
        .await
        .expect("sweep runs");
        (store, data_key)
    }

    let (held, held_key) = run(Some(stored_horizon)).await;
    assert!(
        held.get(&held_key, GetRange::Full).await.is_ok(),
        "an input inside the stored horizon must survive the sweep"
    );

    let (control, control_key) = run(None).await;
    assert!(
        control.get(&control_key, GetRange::Full).await.is_ok(),
        "control: the first run past the default horizon writes the marker and holds"
    );
    let defaults = ravel_maintain::CompactorConfig::default();
    let window_ns = defaults.max_query_duration_ns
        + defaults.head_cache_ttl_ns
        + 4 * defaults.clock_skew_allowance_ns;
    ravel_cli::maintain::sweep_at(
        control.clone() as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        "acme",
        SignalArg::Metrics,
        0,
        false,
        false,
        ravel_maintain::FixedClock::new(NOW_NS + window_ns),
    )
    .await
    .expect("second sweep runs");
    assert!(
        control.get(&control_key, GetRange::Full).await.is_err(),
        "control: once the marker has aged past the window the same input is deleted"
    );
}

/// The tenant, shard and hour of the force 2 `migrate` fixture below.
const REENCODE_TENANT: &str = "cli-migrate-reencode";
const REENCODE_SHARD: u32 = 0;
const REENCODE_HOUR: u32 = 100;

/// Every object in the store with its bytes, so a run that must write nothing
/// is checked against the whole store rather than a handful of keys.
async fn all_objects(store: &dyn ObjectStoreBackend) -> std::collections::BTreeMap<String, Bytes> {
    let mut objects = std::collections::BTreeMap::new();
    for meta in list_all(store, "").await.expect("list the store") {
        let got = store.get(&meta.key, GetRange::Full).await.expect("get");
        objects.insert(meta.key, got.data);
    }
    objects
}

/// The fixture bucket's compaction records, decoded, by key.
async fn reencode_bucket_records(
    store: &dyn ObjectStoreBackend,
) -> Vec<(String, CompactionRecord)> {
    let bucket = ravel_maintain::Bucket::new(
        TenantId::new(REENCODE_TENANT).hash(),
        Signal::Logs,
        REENCODE_SHARD,
        REENCODE_HOUR,
    );
    let listing = ravel_maintain::read::list_bucket(store, &bucket)
        .await
        .expect("list bucket");
    let mut records = Vec::new();
    for key in listing.compaction_record_keys {
        let got = store.get(&key, GetRange::Full).await.expect("get record");
        records.push((
            key,
            record::decode_compaction(&got.data).expect("decode record"),
        ));
    }
    records
}

/// A provisioned logs tenant with one sealed bucket compacted into one
/// compaction record whose one part is recorded a version below the target,
/// which is the bucket the force 2 re-encode exists for. The part bytes stay
/// at the current version, because only the current RLOG version is writable;
/// the record is overwritten in place to say otherwise, which only a fixture
/// may do. The store's clock is the wall clock, which is what the CLI's claim
/// participant reads a claim's expiry against.
async fn seed_below_target_compaction() -> Arc<MemoryStore> {
    let mem = MemoryStore::new();
    let wall_ms = ravel_cli::now_ns().expect("wall clock") / 1_000_000;
    mem.set_clock_ms(u64::try_from(wall_ms).expect("positive wall clock"));
    let tenant_hash = TenantId::new(REENCODE_TENANT).hash();
    ravel_catalog::validate_or_adopt(
        &mem,
        &tenant_hash,
        Signal::Logs,
        4,
        0,
        ravel_catalog::AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    seed_two_l0_logs(&mem, REENCODE_TENANT, REENCODE_SHARD, REENCODE_HOUR).await;
    let bucket =
        ravel_maintain::Bucket::new(tenant_hash, Signal::Logs, REENCODE_SHARD, REENCODE_HOUR);
    let outcome = ravel_maintain::compact_bucket(
        &mem,
        &ravel_maintain::FixedClock::new(ravel_cli::now_ns().expect("wall clock")),
        &ravel_maintain::CompactorConfig::default(),
        &bucket,
    )
    .await
    .expect("compact");
    assert!(
        matches!(outcome, ravel_maintain::CompactionOutcome::Compacted { .. }),
        "the fixture compacts: {outcome:?}"
    );
    let mut records = reencode_bucket_records(&mem).await;
    assert_eq!(records.len(), 1, "one compaction record to stamp");
    let (key, mut rec) = records.remove(0);
    assert_eq!(rec.parts.len(), 1, "the record has one part");
    for part in &mut rec.parts {
        assert_eq!(
            part.segment_format_version,
            u32::from(ravel_logseg::footer::VERSION),
            "the part is written at the target the CLI migrates logs to"
        );
        part.segment_format_version -= 1;
    }
    mem.put(&key, record::encode_compaction(&rec), PutOptions::default())
        .await
        .expect("overwrite the fixture record");
    Arc::new(mem)
}

/// Run `maintain migrate` over the fixture tenant with the default target and
/// family, returning its result and everything it printed after the store
/// header.
async fn run_reencode_migrate(
    store: Arc<dyn ObjectStoreBackend>,
    switches: MigrateSwitches,
    claims: &ClaimOptions,
) -> (anyhow::Result<()>, String) {
    let mut out: Vec<u8> = Vec::new();
    let result = ravel_cli::maintain::migrate_to(
        &mut out,
        store,
        MEMORY,
        REENCODE_TENANT,
        SignalArg::Logs,
        4,
        None,
        None,
        0,
        switches,
        claims,
    )
    .await;
    (result, String::from_utf8(out).expect("utf-8 output"))
}

/// The report lines every fixture run prints after the claims line.
fn reencode_report_head(dry_run: bool, reencode: bool) -> String {
    format!(
        "dry_run: {dry_run}\n\
         reencode_compaction_parts: {reencode}\n\
         tenant: {REENCODE_TENANT}\n\
         signal: Logs\n\
         family: rlog\n\
         target_version: {}\n",
        ravel_logseg::footer::VERSION
    )
}

/// The output after the claims line, which carries a fresh process id, and
/// that line itself.
fn split_claims_line(text: &str) -> (&str, &str) {
    text.split_once('\n').expect("a claims line and a report")
}

/// With `--reencode-compaction-parts` off, a bucket whose one compaction record
/// holds a part below the target is reported as reencode_blocked with the
/// switch reason and its count, nothing is written, and the run exits nonzero.
///
/// Non-vacuity: delete `out.push_str(&reencode_blocked_report(&report.reencode_blocked));`
/// in `migrate_report_text` and the exact-report `assert_eq!` fails with the
/// `reencode_blocked:` line and its comment missing. Set
/// `reencode_writer_enabled: true` in `migrate_to` in place of
/// `reencode_writer_enabled: switches.reencode_compaction_parts,` and the
/// same assertion fails (`buckets_migrated: 1`, no reencode_blocked line), as
/// does the write-nothing assertion.
#[tokio::test]
async fn migrate_without_the_reencode_flag_reports_the_bucket_blocked_and_writes_nothing() {
    let mem = seed_below_target_compaction().await;
    let before = all_objects(mem.as_ref()).await;

    let (result, text) = run_reencode_migrate(
        mem.clone(),
        MigrateSwitches::default(),
        &ClaimOptions::fresh(),
    )
    .await;

    let (claims_line, report) = split_claims_line(&text);
    assert!(
        claims_line.starts_with("claims: on process_id="),
        "{claims_line}"
    );
    assert_eq!(
        report,
        format!(
            "{}budget_records: 0 (0 = unlimited)\n\
             buckets_examined: 1\n\
             buckets_migrated: 0\n\
             buckets_blocked: 0\n\
             records_migrated: 0\n\
             buckets_reencode_blocked: 1\n\
             reencode_blocked: shard=0 hour=100 reason=writer_disabled below_target=1\n\
             # A writer_disabled bucket's one compaction record holds below_target parts under \
             the target. A run with --reencode-compaction-parts re-encodes it, once every reader \
             and maintainer runs a build that reads version 2 compaction records.\n\
             buckets_not_migrated: 0\n\
             buckets_unwritable_skipped: 0\n\
             walk_complete: true\n\
             verification: FOUND STRAGGLERS l0_commit_records=0 l1_compaction_parts=1 \
             rewrite_record_parts=0\n",
            reencode_report_head(false, false)
        )
    );
    let err = result.expect_err("a reencode_blocked bucket makes migrate exit nonzero");
    let err = err.to_string();
    assert!(err.contains("refused to raise the rlog floor"), "{err}");
    assert!(
        err.contains("The 1 reencode_blocked line(s) name the buckets"),
        "{err}"
    );
    assert_eq!(
        all_objects(mem.as_ref()).await,
        before,
        "the switch off writes nothing"
    );
}

/// With `--reencode-compaction-parts` on, the same bucket is re-encoded: a
/// version 2 compaction record superseding the stamped one is published, the
/// bucket is reported migrated with the record's two inputs, and the superseded
/// record's part still refuses the floor until sweep deletes it.
///
/// Non-vacuity: delete `reencode_writer_enabled: switches.reencode_compaction_parts,`
/// in `migrate_to` and the exact-report `assert_eq!` fails with
/// `buckets_migrated: 0` and a `reencode_blocked: ... reason=writer_disabled`
/// line, and the bucket keeps one record.
#[tokio::test]
async fn migrate_with_the_reencode_flag_reencodes_the_bucket_and_reports_it_migrated() {
    let mem = seed_below_target_compaction().await;
    let (stamped_key, _) = reencode_bucket_records(mem.as_ref()).await.remove(0);

    let (result, text) = run_reencode_migrate(
        mem.clone(),
        MigrateSwitches {
            dry_run: false,
            reencode_compaction_parts: true,
        },
        &ClaimOptions::fresh(),
    )
    .await;

    let (_, report) = split_claims_line(&text);
    assert_eq!(
        report,
        format!(
            "{}budget_records: 0 (0 = unlimited)\n\
             buckets_examined: 1\n\
             buckets_migrated: 1\n\
             buckets_blocked: 0\n\
             records_migrated: 2\n\
             buckets_reencode_blocked: 0\n\
             buckets_not_migrated: 0\n\
             buckets_unwritable_skipped: 0\n\
             walk_complete: true\n\
             verification: FOUND STRAGGLERS l0_commit_records=0 l1_compaction_parts=1 \
             rewrite_record_parts=0\n",
            reencode_report_head(false, true)
        )
    );
    let err = result
        .expect_err("the superseded record's part keeps the floor down until sweep")
        .to_string();
    assert!(
        err.contains(
            "the first sweep pass after that writes its unnamed-since marker, a pass at least \
             the pinned-query window later deletes the record and its parts, and the floor is \
             raised by the first migrate run after that"
        ),
        "{err}"
    );

    let records = reencode_bucket_records(mem.as_ref()).await;
    assert_eq!(records.len(), 2, "the old record and its successor");
    let successor = records
        .iter()
        .find(|(key, _)| *key != stamped_key)
        .map(|(_, rec)| rec)
        .expect("a new record");
    assert_eq!(successor.superseded_record_key, stamped_key);
    assert!(
        successor
            .parts
            .iter()
            .all(|p| p.segment_format_version == u32::from(ravel_logseg::footer::VERSION)),
        "the successor's parts are at the target: {successor:?}"
    );
}

/// With the flag on and another process holding the bucket's claim, the
/// re-encode builds nothing and the bucket is printed as not_migrated with the
/// claim's reason and the note that the next run, which starts over because
/// this one drained the walk, retries it; the run exits nonzero.
///
/// Non-vacuity: delete `out.push_str(&not_migrated_report(..));`
/// in `migrate_report_text` and the exact-report `assert_eq!` fails with the
/// `not_migrated:` line and its comment missing.
#[tokio::test]
async fn migrate_prints_a_bucket_whose_claim_another_process_holds() {
    let mem = seed_below_target_compaction().await;
    let acquired = ravel_fleet::claim::acquire(
        mem.as_ref(),
        &WorkIdentity::new(
            TenantId::new(REENCODE_TENANT).hash(),
            Signal::Logs,
            REENCODE_SHARD,
            REENCODE_HOUR,
        ),
        &ravel_fleet::claim::ClaimOwner::new(Uuid::new_v4(), Uuid::new_v4(), 0),
        &ravel_fleet::claim::ClaimConfig::default(),
    )
    .await
    .expect("foreign acquire");
    assert!(
        matches!(acquired, ravel_fleet::claim::Acquisition::Acquired { .. }),
        "the foreign claim must be fresh: {acquired:?}"
    );

    let (result, text) = run_reencode_migrate(
        mem.clone(),
        MigrateSwitches {
            dry_run: false,
            reencode_compaction_parts: true,
        },
        &ClaimOptions::fresh(),
    )
    .await;

    let (_, report) = split_claims_line(&text);
    assert_eq!(
        report,
        format!(
            "{}budget_records: 0 (0 = unlimited)\n\
             buckets_examined: 1\n\
             buckets_migrated: 0\n\
             buckets_blocked: 0\n\
             records_migrated: 0\n\
             buckets_reencode_blocked: 0\n\
             buckets_not_migrated: 1\n\
             not_migrated: shard=0 hour=100 path=reencode reason=claim_skipped \
             claim_reason=held_by_another\n\
             # Each not_migrated bucket published nothing this run. This run drained the walk \
             and cleared its cursor, so the next migrate run starts over and retries every one \
             of them.\n\
             buckets_unwritable_skipped: 0\n\
             walk_complete: true\n\
             verification: FOUND STRAGGLERS l0_commit_records=0 l1_compaction_parts=1 \
             rewrite_record_parts=0\n",
            reencode_report_head(false, true)
        )
    );
    let err = result
        .expect_err("a not_migrated bucket makes migrate exit nonzero")
        .to_string();
    assert!(
        err.contains("the 1 not_migrated line(s) the buckets the next migrate run retries"),
        "{err}"
    );
    assert_eq!(
        reencode_bucket_records(mem.as_ref()).await.len(),
        1,
        "nothing was published"
    );
}

/// `--dry-run` with the flag on runs the read-only re-audit, prints what is
/// below the target, takes no claim and writes nothing.
///
/// Non-vacuity: delete the `if switches.dry_run { return migrate_dry_run(..) }`
/// block in `migrate_to` and the walk runs with the switch on, re-encodes the
/// bucket, and refuses the floor over the superseded record's part, so
/// `result.expect` fails on the straggler error.
#[tokio::test]
async fn migrate_dry_run_with_the_reencode_flag_writes_nothing() {
    let mem = seed_below_target_compaction().await;
    let before = all_objects(mem.as_ref()).await;

    let (result, text) = run_reencode_migrate(
        mem.clone(),
        MigrateSwitches {
            dry_run: true,
            reencode_compaction_parts: true,
        },
        &ClaimOptions::fresh(),
    )
    .await;

    result.expect("a dry run reports and exits zero");
    assert_eq!(
        text,
        format!(
            "claims: off (--dry-run)\n\
             {}l0_commit_records: 0\n\
             l1_compaction_parts: 1\n\
             rewrite_record_parts: 0\n\
             buckets_blocked: 0\n\
             # Dry run: the walk did not run and nothing was written. The figures above are \
             what is below the target now; a run without --dry-run migrates what it can and \
             re-audits.\n",
            reencode_report_head(true, true)
        )
    );
    assert_eq!(
        all_objects(mem.as_ref()).await,
        before,
        "a dry run writes nothing"
    );
}

/// The tenant of the raised-floor fixture below.
const RESOLVED_TENANT: &str = "cli-migrate-resolved";

/// A compaction holds the claim on a bucket whose two L0 records are recorded
/// a version below the target, parked at its first part PUT. `migrate` skips
/// the bucket on the claim and names it not_migrated, then parks at its cursor
/// delete, after the walk and before the fresh re-audit. The compaction is
/// released and publishes at the target, so the re-audit is clean and the
/// floor rises. The run succeeded: it exits zero and says another writer
/// carried the bucket to the target.
///
/// Non-vacuity: make the `Verification::FloorRaised` arm of `migrate_verdict`
/// return an error when `not_migrated` is non-empty in place of `Ok(())` and
/// the `expect` on the result fails; drop the `FloorRaised` arm of
/// `NotMigratedRetry::of` and the exact-report `assert_eq!` fails on the note.
#[tokio::test]
async fn migrate_exits_zero_when_another_writer_resolved_a_not_migrated_bucket() {
    use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op};

    let mem = MemoryStore::new();
    let wall_ms = ravel_cli::now_ns().expect("wall clock") / 1_000_000;
    mem.set_clock_ms(u64::try_from(wall_ms).expect("positive wall clock"));
    let tenant_hash = TenantId::new(RESOLVED_TENANT).hash();
    ravel_catalog::validate_or_adopt(
        &mem,
        &tenant_hash,
        Signal::Logs,
        4,
        0,
        ravel_catalog::AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    seed_two_l0_logs(&mem, RESOLVED_TENANT, REENCODE_SHARD, REENCODE_HOUR).await;
    let bucket =
        ravel_maintain::Bucket::new(tenant_hash, Signal::Logs, REENCODE_SHARD, REENCODE_HOUR);
    let listing = ravel_maintain::read::list_bucket(&mem, &bucket)
        .await
        .expect("list bucket");
    assert_eq!(listing.commit_keys.len(), 2, "two L0 records to stamp");
    for key in &listing.commit_keys {
        let got = mem.get(key, GetRange::Full).await.expect("get record");
        let mut rec = record::decode(&got.data).expect("decode record");
        rec.segment_format_version -= 1;
        mem.put(key, record::encode(&rec), PutOptions::default())
            .await
            .expect("overwrite the fixture record");
    }

    let faults = Arc::new(FaultStore::new(mem, FaultPlan::empty()));
    let part_gate = faults.hold(Op::Put, Some("/l1/".to_string()), Occurrence::Nth(1));
    let cursor_gate = faults.hold(
        Op::Delete,
        Some("/migrate/".to_string()),
        Occurrence::Nth(1),
    );
    let store: Arc<dyn ObjectStoreBackend> = faults.clone();

    let (compacted_tx, compacted_rx) = tokio::sync::oneshot::channel();
    let compaction = async {
        let result = compact(
            store.clone(),
            MEMORY,
            RESOLVED_TENANT,
            SignalArg::Logs,
            REENCODE_SHARD,
            REENCODE_HOUR,
            false,
            None,
            None,
            &ClaimOptions::fresh(),
        )
        .await;
        compacted_tx
            .send(())
            .expect("the driver waits for the compaction");
        result
    };
    let mut out: Vec<u8> = Vec::new();
    let migration = async {
        part_gate.wait_until_held(1).await;
        ravel_cli::maintain::migrate_to(
            &mut out,
            store.clone(),
            MEMORY,
            RESOLVED_TENANT,
            SignalArg::Logs,
            4,
            None,
            None,
            0,
            MigrateSwitches::default(),
            &ClaimOptions::fresh(),
        )
        .await
    };
    // Both gates park into the store's one registry, so the second held call
    // is the migrate run's cursor delete.
    let driver = async {
        cursor_gate.wait_until_held(2).await;
        let parked = cursor_gate.held_details();
        let id_of = |op: Op| {
            let ids: Vec<u64> = parked
                .iter()
                .filter(|(_, held_op, _)| *held_op == op)
                .map(|(id, ..)| *id)
                .collect();
            assert_eq!(ids.len(), 1, "one parked {op:?}: {parked:?}");
            ids[0]
        };
        let (part, cursor) = (id_of(Op::Put), id_of(Op::Delete));
        assert!(part_gate.release(part), "release the compaction");
        compacted_rx.await.expect("the compaction finished");
        assert!(cursor_gate.release(cursor), "release the migrate run");
    };
    let (compacted, result, ()) = tokio::join!(compaction, migration, driver);
    compacted.expect("the compaction publishes");
    result.expect("a raised floor exits zero");
    let listing = ravel_maintain::read::list_bucket(store.as_ref(), &bucket)
        .await
        .expect("list bucket");
    assert_eq!(
        listing.compaction_record_keys.len(),
        1,
        "the compaction's record, and none from migrate"
    );

    let text = String::from_utf8(out).expect("utf-8 output");
    let (_, report) = split_claims_line(&text);
    let version = ravel_logseg::footer::VERSION;
    assert_eq!(
        report,
        format!(
            "dry_run: false\n\
             reencode_compaction_parts: false\n\
             tenant: {RESOLVED_TENANT}\n\
             signal: Logs\n\
             family: rlog\n\
             target_version: {version}\n\
             budget_records: 0 (0 = unlimited)\n\
             buckets_examined: 1\n\
             buckets_migrated: 0\n\
             buckets_blocked: 0\n\
             records_migrated: 0\n\
             buckets_reencode_blocked: 0\n\
             buckets_not_migrated: 1\n\
             not_migrated: shard={REENCODE_SHARD} hour={REENCODE_HOUR} path=l0_migration \
             reason=claim_skipped claim_reason=held_by_another\n\
             # Each not_migrated bucket published nothing this run, and the fresh re-audit \
             found nothing below the target: another writer carried it to the target after \
             this run passed it. Nothing is left to retry.\n\
             buckets_unwritable_skipped: 0\n\
             walk_complete: true\n\
             verification: clean (no records below target)\n\
             floor_raised_to: {version}\n"
        )
    );
}
