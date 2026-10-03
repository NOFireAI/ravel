//! `--tenant-kms-config` on the commands that write tenant data under the
//! Maintain credential (issue #2363): each one runs against a fake S3
//! endpoint through [`build_tenant_data_store`], and the test reads the
//! `x-amz-server-side-encryption-aws-kms-key-id` header each PUT under the
//! tenant's `t/<tenant_hash>/` prefix actually carried. The routing store's
//! per-tenant builder always constructs a real `S3Store`, so the header on
//! the wire is the only observable that proves which key a write used.

use std::collections::BTreeSet;
use std::io::Write;

use bytes::Bytes;
use clap::Parser;
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

use crate::fake_s3::{Echo, FakeS3, SeenPut, spawn};
use crate::maintain::{self, ClaimOptions, MigrateSwitches, SignalArg};
use crate::store::{
    StoreArgs, StoreKind, StoreSelection, TenantKmsArgs, build_store, build_tenant_data_store,
};

const TENANT: &str = "acme";
const TENANT_KEY: &str = "arn:aws:kms:us-east-1:111122223333:key/acme-key";
const OTHER_KEY: &str = "arn:aws:kms:us-east-1:111122223333:key/globex-key";
const SHARD: u32 = 0;
const HOUR: u32 = 100;
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const S3: StoreSelection = StoreSelection::explicit(StoreKind::S3);

fn s3_args(endpoint: &str) -> StoreArgs {
    StoreArgs::try_parse_from([
        "ravel-cli",
        "--store",
        "s3",
        "--s3-bucket",
        "ravel-test",
        "--s3-endpoint",
        endpoint,
        "--s3-access-key",
        "test",
        "--s3-secret-key",
        "test",
    ])
    .expect("flags parse")
}

/// A `--tenant-kms-config` file naming `acme` and one other tenant, so a test
/// also sees that only the command's own tenant is configured.
fn kms_file() -> tempfile::NamedTempFile {
    let mut file = tempfile::NamedTempFile::new().expect("temp file");
    write!(
        file,
        "[tenants]\n{TENANT} = \"{TENANT_KEY}\"\nglobex = \"{OTHER_KEY}\"\n"
    )
    .expect("write kms file");
    file
}

fn kms_args(file: &tempfile::NamedTempFile) -> TenantKmsArgs {
    TenantKmsArgs {
        tenant_kms_config: Some(file.path().to_path_buf()),
    }
}

fn tenant_prefix(tenant: &str) -> String {
    format!("t/{}/", TenantId::new(tenant).hash().to_hex())
}

/// The PUTs from `start` on whose key is under `tenant`'s prefix.
fn tenant_puts_since(fake: &FakeS3, start: usize, tenant: &str) -> Vec<SeenPut> {
    let prefix = tenant_prefix(tenant);
    fake.puts()
        .into_iter()
        .skip(start)
        .filter(|put| put.key.starts_with(&prefix))
        .collect()
}

/// Every PUT the command made under the tenant's prefix carried the tenant's
/// key, except the key-epoch record: it is written before the key is
/// registered, exactly as ravel-server's startup writes it, and is asserted
/// to be written once under the default. Returns the routed keys.
fn assert_tenant_writes_routed(puts: &[SeenPut]) -> BTreeSet<String> {
    let enc = format!("{}enc", tenant_prefix(TENANT));
    let (epoch, data): (Vec<&SeenPut>, Vec<&SeenPut>) = puts.iter().partition(|put| put.key == enc);
    assert_eq!(
        epoch
            .iter()
            .map(|put| put.sse_kms_key_id.as_deref())
            .collect::<Vec<_>>(),
        vec![None, None],
        "the first configuration writes epoch 0 then epoch 1, both before the key routes"
    );
    let unrouted: Vec<(&str, Option<&str>)> = data
        .iter()
        .filter(|put| put.sse_kms_key_id.as_deref() != Some(TENANT_KEY))
        .map(|put| (put.key.as_str(), put.sse_kms_key_id.as_deref()))
        .collect();
    assert!(
        unrouted.is_empty(),
        "every tenant write must carry {TENANT_KEY}; these did not: {unrouted:?}"
    );
    data.iter().map(|put| put.key.clone()).collect()
}

/// No PUT anywhere carried the other tenant's key, and none landed under its
/// prefix: its entry in the file was validated, not applied.
fn assert_other_tenant_untouched(fake: &FakeS3) {
    assert!(
        fake.puts()
            .iter()
            .all(|put| put.sse_kms_key_id.as_deref() != Some(OTHER_KEY)),
        "the other tenant's key is never used"
    );
    assert!(
        tenant_puts_since(fake, 0, "globex").is_empty(),
        "the other tenant's epoch record is not bootstrapped by a command that writes acme"
    );
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

/// Two L0 `.rlog` objects and their commit records in one sealed logs bucket,
/// enough for a compaction to merge rather than report `BelowMinInputs`.
async fn seed_two_l0_logs(store: &dyn ObjectStoreBackend) {
    let tenant_hash = TenantId::new(TENANT).hash();
    let base_ns = i64::from(HOUR) * NS_PER_HOUR;
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
            shard: SHARD,
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
            SHARD,
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
            shard: SHARD,
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
            ingest_hour_bucket: HOUR,
        })
        .expect("build commit record");
        let commit_key = keys::commit_key_for_record(&rec).expect("commit key");
        store
            .put(&commit_key, record::encode(&rec), PutOptions::default())
            .await
            .expect("put commit record");
    }
}

/// The bucket's compaction record keys, read back through the plain store.
async fn compaction_record_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    let bucket =
        ravel_maintain::Bucket::new(TenantId::new(TENANT).hash(), Signal::Logs, SHARD, HOUR);
    ravel_maintain::read::list_bucket(store, &bucket)
        .await
        .expect("list bucket")
        .compaction_record_keys
}

/// The routed writes include the bucket's compaction record and at least one
/// L1 part: the data the routing exists for, not only bookkeeping.
fn assert_compaction_output_routed(routed: &BTreeSet<String>, records: &[String]) {
    assert!(!records.is_empty(), "the command published a record");
    for record in records {
        assert!(
            routed.contains(record),
            "compaction record {record} must be a routed write: {routed:?}"
        );
    }
    assert!(
        routed.iter().any(|key| key.contains("/l1/")),
        "an L1 part must be a routed write: {routed:?}"
    );
}

/// `maintain compact-bucket`: the L1 parts and the compaction record go out
/// under the tenant's key.
///
/// Non-vacuity: make `build_tenant_data_store` return `build_store(args)` in
/// place of `Ok(kms)` and `assert_tenant_writes_routed` fails, naming every
/// L1 part and the compaction record with `None` as their key.
#[tokio::test]
async fn compact_bucket_writes_l1_parts_and_its_record_under_the_tenant_key() {
    let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
    let args = s3_args(&endpoint);
    let plain = build_store(&args).expect("plain store");
    seed_two_l0_logs(plain.as_ref()).await;
    let start = fake.puts().len();
    let file = kms_file();

    let store = build_tenant_data_store(&args, &kms_args(&file), TENANT, true, 1_000)
        .await
        .expect("routed store");
    maintain::compact(
        store,
        S3,
        TENANT,
        SignalArg::Logs,
        SHARD,
        HOUR,
        false,
        None,
        None,
        &ClaimOptions::fresh(),
    )
    .await
    .expect("compaction runs");

    let routed = assert_tenant_writes_routed(&tenant_puts_since(&fake, start, TENANT));
    assert_compaction_output_routed(&routed, &compaction_record_keys(plain.as_ref()).await);
    assert_other_tenant_untouched(&fake);
}

/// `maintain compact-tenant`: the same, through the whole-tenant walk.
///
/// Non-vacuity: the same edit as the compact-bucket test fails this one the
/// same way.
#[tokio::test]
async fn compact_tenant_writes_l1_parts_and_its_records_under_the_tenant_key() {
    let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
    let args = s3_args(&endpoint);
    let plain = build_store(&args).expect("plain store");
    seed_two_l0_logs(plain.as_ref()).await;
    let start = fake.puts().len();
    let file = kms_file();

    let store = build_tenant_data_store(&args, &kms_args(&file), TENANT, true, 1_000)
        .await
        .expect("routed store");
    maintain::compact_tenant(
        store,
        S3,
        TENANT,
        SignalArg::Logs,
        Some(1),
        Some(HOUR),
        Some(HOUR),
        false,
        None,
        None,
        None,
        None,
        1,
        None,
        crate::now_ns().expect("wall clock"),
        &ClaimOptions::fresh(),
    )
    .await
    .expect("compact-tenant runs");

    let routed = assert_tenant_writes_routed(&tenant_puts_since(&fake, start, TENANT));
    assert_compaction_output_routed(&routed, &compaction_record_keys(plain.as_ref()).await);
    assert_other_tenant_untouched(&fake);
}

/// `maintain migrate --reencode-compaction-parts`: the re-encoded L1 parts and
/// the version 2 compaction record that supersedes the stamped one go out
/// under the tenant's key. The fixture compacts the bucket through the plain
/// store, then stamps its one part a version below the target, which is the
/// bucket the re-encode exists for (the same fixture `tests/maintain.rs`
/// builds against a `MemoryStore`).
///
/// Non-vacuity: the same edit as the compact-bucket test fails this one,
/// naming the new record and its parts.
#[tokio::test]
async fn migrate_reencode_writes_l1_parts_and_its_record_under_the_tenant_key() {
    let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
    let args = s3_args(&endpoint);
    let plain = build_store(&args).expect("plain store");
    let tenant_hash = TenantId::new(TENANT).hash();
    ravel_catalog::validate_or_adopt(
        plain.as_ref(),
        &tenant_hash,
        Signal::Logs,
        1,
        0,
        ravel_catalog::AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    seed_two_l0_logs(plain.as_ref()).await;
    let bucket = ravel_maintain::Bucket::new(tenant_hash, Signal::Logs, SHARD, HOUR);
    let outcome = ravel_maintain::compact_bucket(
        plain.as_ref(),
        &ravel_maintain::FixedClock::new(crate::now_ns().expect("wall clock")),
        &ravel_maintain::CompactorConfig::default(),
        &bucket,
    )
    .await
    .expect("compact");
    assert!(
        matches!(outcome, ravel_maintain::CompactionOutcome::Compacted { .. }),
        "the fixture compacts: {outcome:?}"
    );
    let stamped = compaction_record_keys(plain.as_ref()).await;
    assert_eq!(stamped.len(), 1, "one compaction record to stamp");
    let got = plain
        .get(&stamped[0], GetRange::Full)
        .await
        .expect("get record");
    let mut rec = record::decode_compaction(&got.data).expect("decode record");
    for part in &mut rec.parts {
        part.segment_format_version -= 1;
    }
    plain
        .put(
            &stamped[0],
            record::encode_compaction(&rec),
            PutOptions::default(),
        )
        .await
        .expect("overwrite the fixture record");
    let start = fake.puts().len();
    let file = kms_file();

    let store = build_tenant_data_store(&args, &kms_args(&file), TENANT, true, 1_000)
        .await
        .expect("routed store");
    let mut out = Vec::new();
    // The superseded record's part keeps the floor down until a sweep, so the
    // run reports stragglers and exits nonzero after writing its output.
    let _ = maintain::migrate_to(
        &mut out,
        store,
        S3,
        TENANT,
        SignalArg::Logs,
        1,
        None,
        None,
        0,
        MigrateSwitches {
            dry_run: false,
            reencode_compaction_parts: true,
        },
        &ClaimOptions::fresh(),
    )
    .await;
    let report = String::from_utf8(out).expect("utf-8 report");
    assert!(
        report.contains("buckets_migrated: 1"),
        "the bucket is re-encoded: {report}"
    );

    let routed = assert_tenant_writes_routed(&tenant_puts_since(&fake, start, TENANT));
    let successors: Vec<String> = compaction_record_keys(plain.as_ref())
        .await
        .into_iter()
        .filter(|key| *key != stamped[0])
        .collect();
    assert_eq!(
        successors.len(),
        1,
        "one superseding record: {successors:?}"
    );
    assert_compaction_output_routed(&routed, &successors);
    assert_other_tenant_untouched(&fake);
}

/// Publish one sealed metrics commit record and its placeholder data object.
async fn publish_metrics_l0(store: &dyn ObjectStoreBackend, seq: u64, created_unix_ns: i64) {
    let tenant_hash = TenantId::new(TENANT).hash();
    let ingest_hour_bucket = u32::try_from(created_unix_ns / NS_PER_HOUR).expect("fits u32");
    let payload = format!("seg-{SHARD}-{seq}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: SHARD,
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
}

/// `catalog fold`: the snapshot part and `HEAD` it publishes go out under the
/// tenant's key.
///
/// Non-vacuity: the same edit as the compact-bucket test fails this one,
/// naming the snapshot part and `HEAD`.
#[tokio::test]
async fn catalog_fold_writes_its_snapshot_under_the_tenant_key() {
    let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
    let args = s3_args(&endpoint);
    let plain = build_store(&args).expect("plain store");
    let now = crate::now_ns().expect("wall clock");
    publish_metrics_l0(plain.as_ref(), 1, now - 3 * NS_PER_HOUR).await;
    let start = fake.puts().len();
    let file = kms_file();

    let store = build_tenant_data_store(&args, &kms_args(&file), TENANT, true, 1_000)
        .await
        .expect("routed store");
    crate::catalog::fold(store, S3, TENANT, 1, SignalArg::Metrics, None, now, false)
        .await
        .expect("fold runs");

    let routed = assert_tenant_writes_routed(&tenant_puts_since(&fake, start, TENANT));
    let catalog = format!("{}catalog/m/", tenant_prefix(TENANT));
    assert!(
        routed.contains(&format!("{catalog}HEAD")),
        "the fold's HEAD must be a routed write: {routed:?}"
    );
    assert!(
        routed
            .iter()
            .any(|key| key.starts_with(&format!("{catalog}snap/"))),
        "a snapshot part must be a routed write: {routed:?}"
    );
    assert_other_tenant_untouched(&fake);
}

/// A tenant the file does not name is written exactly as ravel-server writes
/// it: no route, so the default store and the bucket's default encryption, and
/// no key-epoch record bootstrapped for it.
#[tokio::test]
async fn a_tenant_the_file_does_not_name_writes_under_the_bucket_default() {
    let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
    let args = s3_args(&endpoint);
    let mut file = tempfile::NamedTempFile::new().expect("temp file");
    write!(file, "[tenants]\nglobex = \"{OTHER_KEY}\"\n").expect("write kms file");

    let store = build_tenant_data_store(&args, &kms_args(&file), TENANT, true, 1_000)
        .await
        .expect("routed store");
    let key = format!("{}l/l1/probe", tenant_prefix(TENANT));
    store
        .put(&key, Bytes::from_static(b"x"), PutOptions::default())
        .await
        .expect("put");

    let puts = tenant_puts_since(&fake, 0, TENANT);
    assert_eq!(
        puts.iter()
            .map(|put| (put.key.as_str(), put.sse_kms_key_id.as_deref()))
            .collect::<Vec<_>>(),
        vec![(key.as_str(), None)],
        "the one write is unrouted and no epoch record was written"
    );
    assert_other_tenant_untouched(&fake);
}

/// A dry run reads and validates the file and writes nothing, not even the
/// key-epoch record.
#[tokio::test]
async fn a_dry_run_validates_the_file_and_writes_nothing() {
    let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
    let args = s3_args(&endpoint);
    let file = kms_file();
    build_tenant_data_store(&args, &kms_args(&file), TENANT, false, 1_000)
        .await
        .expect("a dry run builds the plain store");
    assert!(fake.puts().is_empty(), "a dry run writes nothing");

    let mut bad = tempfile::NamedTempFile::new().expect("temp file");
    write!(bad, "[tenants]\n{TENANT} = \"\"\n").expect("write kms file");
    let err = build_tenant_data_store(&args, &kms_args(&bad), TENANT, false, 1_000)
        .await
        .err()
        .expect("an invalid file fails a dry run too");
    assert!(
        err.to_string().starts_with(&format!(
            "invalid --tenant-kms-config {}",
            bad.path().display()
        )),
        "{err}"
    );
}

/// The same refusal ravel-server's `Cli::validate` makes: the routing store
/// builds a real `S3Store` per tenant, which `--store memory` cannot.
#[tokio::test]
async fn the_flag_under_store_memory_is_refused() {
    let args = StoreArgs::try_parse_from(["ravel-cli", "--store", "memory"]).expect("flags parse");
    let file = kms_file();
    let err = build_tenant_data_store(&args, &kms_args(&file), TENANT, true, 1_000)
        .await
        .err()
        .expect("memory plus the flag is refused");
    assert_eq!(
        err.to_string(),
        "--tenant-kms-config requires --store s3: KmsRoutingStore's per-tenant builder always \
         constructs a real S3Store, which --store memory has no S3Config to build one from."
    );
}
