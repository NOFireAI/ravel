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
use ravel_commit::record;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_types::{Signal, TenantId};

use crate::fake_s3::seed::{self, NS_PER_HOUR};
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
const S3: StoreSelection = StoreSelection::explicit(StoreKind::S3);
/// `build_tenant_data_store`'s `dry_run` argument.
const REAL_RUN: bool = false;
const DRY_RUN: bool = true;

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

/// The tenant's key-epoch record as ravel-server's startup leaves it, with
/// the file's key current: the state a routed command requires, since
/// ravel-cli writes no key epoch.
async fn seed_server_epochs(store: &dyn ObjectStoreBackend) {
    seed::key_epochs(store, TENANT, TENANT_KEY).await;
}

/// The command wrote no key epoch, and every PUT it made under the tenant's
/// prefix carried the tenant's key. Returns the routed keys.
fn assert_tenant_writes_routed(puts: &[SeenPut]) -> BTreeSet<String> {
    let enc = format!("{}enc", tenant_prefix(TENANT));
    assert_eq!(
        puts.iter().filter(|put| put.key == enc).count(),
        0,
        "ravel-cli writes no key epoch"
    );
    let unrouted: Vec<(&str, Option<&str>)> = puts
        .iter()
        .filter(|put| put.sse_kms_key_id.as_deref() != Some(TENANT_KEY))
        .map(|put| (put.key.as_str(), put.sse_kms_key_id.as_deref()))
        .collect();
    assert!(
        unrouted.is_empty(),
        "every tenant write must carry {TENANT_KEY}; these did not: {unrouted:?}"
    );
    puts.iter().map(|put| put.key.clone()).collect()
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
        "nothing is written under the other tenant's prefix by a command that writes acme"
    );
}

async fn seed_two_l0_logs(store: &dyn ObjectStoreBackend) {
    seed::two_l0_logs(store, TENANT, SHARD, HOUR).await;
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
/// Non-vacuity: make the real-run branch of `build_tenant_data_store` yield
/// `build_store(args)?` in place of `kms` and `assert_tenant_writes_routed`
/// fails, naming every
/// L1 part and the compaction record with `None` as their key.
#[tokio::test]
async fn compact_bucket_writes_l1_parts_and_its_record_under_the_tenant_key() {
    let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
    let args = s3_args(&endpoint);
    let plain = build_store(&args).expect("plain store");
    seed_server_epochs(plain.as_ref()).await;
    seed_two_l0_logs(plain.as_ref()).await;
    let start = fake.puts().len();
    let file = kms_file();

    let store = build_tenant_data_store(
        &args,
        &kms_args(&file),
        TENANT,
        REAL_RUN,
        1_000,
        &mut std::io::sink(),
    )
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
    seed_server_epochs(plain.as_ref()).await;
    seed_two_l0_logs(plain.as_ref()).await;
    let start = fake.puts().len();
    let file = kms_file();

    let store = build_tenant_data_store(
        &args,
        &kms_args(&file),
        TENANT,
        REAL_RUN,
        1_000,
        &mut std::io::sink(),
    )
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
    seed_server_epochs(plain.as_ref()).await;
    let start = fake.puts().len();
    let file = kms_file();

    let store = build_tenant_data_store(
        &args,
        &kms_args(&file),
        TENANT,
        REAL_RUN,
        1_000,
        &mut std::io::sink(),
    )
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
    seed::metrics_l0(plain.as_ref(), TENANT, SHARD, 1, now - 3 * NS_PER_HOUR).await;
    seed_server_epochs(plain.as_ref()).await;
    let start = fake.puts().len();
    let file = kms_file();

    let store = build_tenant_data_store(
        &args,
        &kms_args(&file),
        TENANT,
        REAL_RUN,
        1_000,
        &mut std::io::sink(),
    )
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

    let store = build_tenant_data_store(
        &args,
        &kms_args(&file),
        TENANT,
        REAL_RUN,
        1_000,
        &mut std::io::sink(),
    )
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

/// The refusal for a tenant with no key-epoch record, as the command reports
/// it.
fn absent_record_refusal() -> String {
    format!(
        "failed to configure per-tenant SSE-KMS routing (--tenant-kms-config): tenant \
         \"{TENANT}\" has no key-epoch record at t/{}/enc, but --tenant-kms-config names \
         \"{TENANT_KEY}\" for it: a tenant's key epochs are recorded by ravel-server at startup, \
         never by this command. Refusing before any write; start ravel-server with this \
         --tenant-kms-config file first, then rerun this command",
        TenantId::new(TENANT).hash().to_hex()
    )
}

/// A dry run validates the file and checks the key-epoch record as the real
/// run would: an absent record is refused, a record holding the file's key
/// logs the routing line. Neither writes anything.
///
/// Non-vacuity: drop the `check_tenant_kms_records` call from the dry-run
/// branch of `build_tenant_data_store` and the absent-record dry run
/// succeeds.
#[tokio::test]
async fn a_dry_run_checks_the_record_and_writes_nothing() {
    let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
    let args = s3_args(&endpoint);
    let file = kms_file();
    let err = build_tenant_data_store(
        &args,
        &kms_args(&file),
        TENANT,
        DRY_RUN,
        1_000,
        &mut std::io::sink(),
    )
    .await
    .err()
    .expect("an absent record fails a dry run too");
    assert_eq!(err.to_string(), absent_record_refusal());
    assert!(fake.puts().is_empty(), "a refused dry run writes nothing");

    seed_server_epochs(build_store(&args).expect("plain store").as_ref()).await;
    let start = fake.puts().len();
    let mut log = Vec::new();
    build_tenant_data_store(&args, &kms_args(&file), TENANT, DRY_RUN, 1_000, &mut log)
        .await
        .expect("a dry run builds the plain store");
    assert_eq!(fake.puts().len(), start, "a dry run writes nothing");
    assert_eq!(
        String::from_utf8(log).expect("utf-8 log"),
        format!("tenant-kms: tenant \"{TENANT}\" writes are encrypted under {TENANT_KEY}\n")
    );

    let mut bad = tempfile::NamedTempFile::new().expect("temp file");
    write!(bad, "[tenants]\n{TENANT} = \"\"\n").expect("write kms file");
    let err = build_tenant_data_store(
        &args,
        &kms_args(&bad),
        TENANT,
        DRY_RUN,
        1_000,
        &mut std::io::sink(),
    )
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

/// A key-epoch record whose current key is not the file's refuses the
/// command: only ravel-server's startup records a key change, and the record
/// is append-only, so the command writes nothing under the tenant's prefix,
/// neither an epoch nor any compaction output.
///
/// Non-vacuity: pass `KeyChangePolicy::RecordRotation` in
/// `build_tenant_data_store` and the compaction runs, appending an epoch and
/// writing its L1 parts.
#[tokio::test]
async fn a_recorded_key_that_differs_from_the_file_refuses_the_command() {
    const RECORDED_KEY: &str = "arn:aws:kms:us-east-1:111122223333:key/acme-old-key";
    let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
    let args = s3_args(&endpoint);
    let plain = build_store(&args).expect("plain store");
    let tenant_hash = TenantId::new(TENANT).hash();
    seed::key_epochs(plain.as_ref(), TENANT, RECORDED_KEY).await;
    seed_two_l0_logs(plain.as_ref()).await;
    let start = fake.puts().len();
    let enc = format!("{}enc", tenant_prefix(TENANT));
    let reads_before = fake.gets().iter().filter(|key| **key == enc).count();
    let file = kms_file();

    let err = async {
        let store = build_tenant_data_store(
            &args,
            &kms_args(&file),
            TENANT,
            REAL_RUN,
            1_000,
            &mut std::io::sink(),
        )
        .await?;
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
    }
    .await
    .expect_err("a differing recorded key refuses the command");

    assert_eq!(
        err.to_string(),
        format!(
            "failed to configure per-tenant SSE-KMS routing (--tenant-kms-config): tenant \
             \"{TENANT}\" has key \"{RECORDED_KEY}\" recorded as its current key epoch at \
             t/{}/enc, but --tenant-kms-config names \"{TENANT_KEY}\": a key change is recorded \
             by ravel-server at startup, never by this command. Refusing before any write; start \
             ravel-server with the new key first, or run this command with the file the servers \
             run with",
            tenant_hash.to_hex()
        )
    );
    assert!(
        fake.gets().iter().filter(|key| **key == enc).count() > reads_before,
        "the bootstrap read the epoch record"
    );
    assert_eq!(
        tenant_puts_since(&fake, start, TENANT),
        Vec::new(),
        "nothing is written under the tenant's prefix after the bootstrap read"
    );
    assert!(compaction_record_keys(plain.as_ref()).await.is_empty());
    assert_other_tenant_untouched(&fake);
}

/// A tenant with no key-epoch record refuses the command: ravel-cli records
/// no key, so a file that reaches a job before the servers run with it
/// cannot leave a permanent epoch the servers are not using. Nothing is
/// written under the tenant's prefix, neither an epoch nor any compaction
/// output.
///
/// Non-vacuity: make `epoch_action` return `Ok(EpochAction::Bootstrap)` for
/// an absent record under `KeyChangePolicy::Refuse` and the compaction runs,
/// writing epochs 0 and 1 and its L1 parts.
#[tokio::test]
async fn an_absent_key_epoch_record_refuses_the_command() {
    let (endpoint, fake) = spawn(Echo::Stored, &[]).await;
    let args = s3_args(&endpoint);
    let plain = build_store(&args).expect("plain store");
    seed_two_l0_logs(plain.as_ref()).await;
    let start = fake.puts().len();
    let file = kms_file();

    let err = async {
        let store = build_tenant_data_store(
            &args,
            &kms_args(&file),
            TENANT,
            REAL_RUN,
            1_000,
            &mut std::io::sink(),
        )
        .await?;
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
    }
    .await
    .expect_err("an absent record refuses the command");

    assert_eq!(err.to_string(), absent_record_refusal());
    assert_eq!(
        tenant_puts_since(&fake, start, TENANT),
        Vec::new(),
        "nothing is written under the tenant's prefix"
    );
    assert!(compaction_record_keys(plain.as_ref()).await.is_empty());
    assert_other_tenant_untouched(&fake);
}

/// The same refusal ravel-server's `Cli::validate` makes: the routing store
/// builds a real `S3Store` per tenant, which `--store memory` cannot.
#[tokio::test]
async fn the_flag_under_store_memory_is_refused() {
    let args = StoreArgs::try_parse_from(["ravel-cli", "--store", "memory"]).expect("flags parse");
    let file = kms_file();
    let err = build_tenant_data_store(
        &args,
        &kms_args(&file),
        TENANT,
        REAL_RUN,
        1_000,
        &mut std::io::sink(),
    )
    .await
    .err()
    .expect("memory plus the flag is refused");
    assert_eq!(
        err.to_string(),
        "--tenant-kms-config requires --store s3: KmsRoutingStore's per-tenant builder always \
         constructs a real S3Store, which --store memory has no S3Config to build one from."
    );
}
