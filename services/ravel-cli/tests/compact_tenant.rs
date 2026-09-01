//! In-process tests for `ravel-cli maintain compact-tenant` (issue #680).
//!
//! Unlike the rest of `tests/maintain.rs`, which exercises the CLI glue on an
//! empty store, these need buckets that actually compact, so each seeds real
//! RLOG L0 objects with their commit records exactly as a log ingest shard
//! would (the shape `ravel_maintain::rlog`'s own `seed_l0` fixture uses) and
//! then reads the L1 parts and compaction records back out of the store.
//!
//! Time is injected: `compact_tenant` takes `now_ns`, so "which hours are
//! sealed" is a property of the fixture, not of when the suite runs.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use ravel_cli::maintain::{CompactTenantError, SignalArg, compact_tenant};
use ravel_cli::store::{DefaultedMemoryEmptyWalk, StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const TENANT: &str = "clickbench";
/// The older of the two seeded hours; `HOUR_OLD + 1` is the newer one.
const HOUR_OLD: u32 = 500_000;
const SHARDS: u32 = 2;
const EPOCH: u64 = 1;

/// The seeded fixtures below build their own `MemoryStore`, which is the
/// explicit `--store memory` case (issue #1024): the report header reads
/// `store: memory` and an empty walk stays a zero-count success. The defaulted
/// case is [`StoreSelection::defaulted_memory`], exercised on its own below.
const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

/// Ten minutes past the end of the newer hour. Under the default
/// `max_flush_lifetime` (1 h) the seal margin is 1 h 5 min, so only `HOUR_OLD`
/// is sealed; under `--max-flush-lifetime 0s` the margin is the bare 5 min
/// clock-skew allowance, so both hours are.
fn now_ns() -> i64 {
    (i64::from(HOUR_OLD) + 2) * NS_PER_HOUR + 10 * 60 * 1_000_000_000
}

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn log_record(stream: u8, ts_ns: i64, body: &str) -> LogRecord {
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
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: vec![("code".into(), AttrValue::I64(200))],
    }
}

/// Seed one L0 `.rlog` object plus its commit record into `(shard, hour)`,
/// exactly as the ingest log shard would.
async fn seed_l0(store: &dyn ObjectStoreBackend, shard: u32, hour: u32, seq: u64) {
    let th = tenant_hash();
    let writer_id = Uuid::new_v4();
    let base_ns = i64::from(hour) * NS_PER_HOUR;
    let records: Vec<LogRecord> = (0..4)
        .map(|i| {
            log_record(
                u8::try_from(i % 2).unwrap(),
                base_ns + i64::from(i) * 1_000_000 + i64::try_from(seq).unwrap(),
                "get /api ok",
            )
        })
        .collect();

    let identity = ObjectIdentity {
        tenant_hash: th.0,
        shard,
        writer_id: writer_id.into_bytes(),
        writer_epoch: EPOCH,
        writer_seq: seq,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for r in &records {
        writer.push(r.clone()).expect("push");
    }
    let bytes = Bytes::from(writer.finish().expect("finish L0"));
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
    let data_key = keys::data_key(
        &th,
        Signal::Logs,
        shard,
        writer_id,
        EPOCH,
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
    let created = base_ns + i64::try_from(seq).unwrap() * 1_000_000;
    let rec = record::build(NewCommitRecord {
        tenant_hash: th,
        signal: Signal::Logs,
        shard,
        writer_id,
        writer_epoch: EPOCH,
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

/// A 2-shard logs tenant with two L0 objects in each of two consecutive hours
/// per shard: eight L0 objects, four buckets, every bucket at or above the
/// default `min_compaction_inputs` of 2.
async fn seed_tenant() -> Arc<dyn ObjectStoreBackend> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    for shard in 0..SHARDS {
        for hour in [HOUR_OLD, HOUR_OLD + 1] {
            for seq in 1..=2u64 {
                seed_l0(store.as_ref(), shard, hour, seq).await;
            }
        }
    }
    store
}

/// Every object key under the tenant prefix, for the dry-run no-write check.
async fn all_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    let mut keys: Vec<String> = list_all(store, &format!("t/{}/", tenant_hash().to_hex()))
        .await
        .expect("list tenant prefix")
        .into_iter()
        .map(|m| m.key)
        .collect();
    keys.sort();
    keys
}

/// `(compaction record count, L1 part count)` actually present in one bucket.
async fn bucket_output(store: &dyn ObjectStoreBackend, shard: u32, hour: u32) -> (usize, usize) {
    let th = tenant_hash();
    let bucket = ravel_maintain::Bucket::new(th, Signal::Logs, shard, hour);
    let listing = ravel_maintain::read::list_bucket(store, &bucket)
        .await
        .expect("list bucket");
    let l1_prefix = format!(
        "t/{}/{}/{}/{:04}/{}/",
        th.to_hex(),
        Signal::Logs.key_prefix(),
        keys::L1_DIR,
        shard,
        keys::ingest_hour_string(hour),
    );
    let parts = list_all(store, &l1_prefix).await.expect("list l1").len();
    (listing.compaction_record_keys.len(), parts)
}

#[tokio::test]
async fn compact_tenant_compacts_only_the_sealed_hour_of_every_shard() {
    let store = seed_tenant().await;

    let report = compact_tenant(
        store.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        false,
        None,
        None,
        None,
        None,
        now_ns(),
    )
    .await
    .expect("compact-tenant runs");

    assert_eq!(report.shards, 2, "walked both shards");
    assert_eq!(
        report.store, MEMORY,
        "the report carries the store the walk ran against"
    );
    assert_eq!(
        report.store.header(),
        "store: memory",
        "an explicit --store memory is reported without the (default) marker"
    );
    assert_eq!(report.compacted, 2, "one sealed hour per shard");
    assert_eq!(
        report.not_sealed, 2,
        "the newer hour of each shard is inside the 1h5m seal margin"
    );
    assert_eq!(report.already, 0);
    assert_eq!(report.below_min, 0);
    assert_eq!(report.tombstoned, 0);
    assert_eq!(report.parts_written, 2, "one L1 part per compacted bucket");

    for shard in 0..SHARDS {
        assert_eq!(
            bucket_output(store.as_ref(), shard, HOUR_OLD).await,
            (1, 1),
            "sealed bucket of shard {shard} holds its compaction record and L1 part"
        );
        assert_eq!(
            bucket_output(store.as_ref(), shard, HOUR_OLD + 1).await,
            (0, 0),
            "unsealed bucket of shard {shard} was left untouched"
        );
    }
}

#[tokio::test]
async fn max_flush_lifetime_zero_seals_and_compacts_every_hour() {
    let store = seed_tenant().await;

    let report = compact_tenant(
        store.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        false,
        // `0s`, the operator override for a quiescent tenant: the seal margin
        // collapses to the 5m clock-skew allowance, so the hour that ended ten
        // minutes ago is sealed too. Ignoring this argument (dropping the
        // `config.max_flush_lifetime_ns = ns` assignment in
        // `maintain::build_compactor_config`) flips `report.compacted` back to 2
        // and fails this assertion.
        Some(0),
        None,
        None,
        None,
        now_ns(),
    )
    .await
    .expect("compact-tenant runs");

    assert_eq!(
        report.compacted, 4,
        "both hours of both shards seal at max_flush_lifetime=0"
    );
    assert_eq!(report.not_sealed, 0);
    assert_eq!(report.parts_written, 4);

    for shard in 0..SHARDS {
        for hour in [HOUR_OLD, HOUR_OLD + 1] {
            assert_eq!(
                bucket_output(store.as_ref(), shard, hour).await,
                (1, 1),
                "shard {shard} hour {hour} was compacted"
            );
        }
    }
}

#[tokio::test]
async fn dry_run_reports_the_same_plan_and_writes_nothing() {
    let store = seed_tenant().await;
    let before = all_keys(store.as_ref()).await;

    let dry = compact_tenant(
        store.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        true,
        None,
        None,
        None,
        None,
        now_ns(),
    )
    .await
    .expect("dry run runs");

    let after = all_keys(store.as_ref()).await;
    assert_eq!(
        before.len(),
        after.len(),
        "a dry run wrote objects: {:?}",
        after
            .iter()
            .filter(|k| !before.contains(k))
            .collect::<Vec<_>>()
    );
    assert_eq!(before, after, "a dry run changed the store's key set");

    // Same plan: the real run over the same fixture reports exactly what the
    // dry run said it would.
    let wet = compact_tenant(
        store.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        false,
        None,
        None,
        None,
        None,
        now_ns(),
    )
    .await
    .expect("real run runs");
    assert_eq!(dry, wet, "dry run and real run disagreed on the plan");
}

#[tokio::test]
async fn missing_shards_and_no_provisioning_record_is_a_typed_error_naming_the_tenant() {
    let store = seed_tenant().await;

    let err = compact_tenant(
        store,
        MEMORY,
        TENANT,
        SignalArg::Logs,
        None,
        None,
        None,
        true,
        None,
        None,
        None,
        None,
        now_ns(),
    )
    .await
    .expect_err("no --shards and no provisioning record must refuse");

    let typed = err
        .downcast_ref::<CompactTenantError>()
        .expect("typed CompactTenantError");
    assert_eq!(
        *typed,
        CompactTenantError::NoProvisioningRecord {
            tenant: TENANT.to_string(),
            signal: Signal::Logs,
        }
    );
    assert!(
        err.to_string().contains(TENANT),
        "error must name the tenant: {err}"
    );
}

#[tokio::test]
async fn from_hour_and_to_hour_bound_the_walk() {
    let store = seed_tenant().await;

    // Ask for only the newer hour, with the override that seals it. The older
    // hour is below `--from-hour` and is never visited.
    let report = compact_tenant(
        store.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        Some(HOUR_OLD + 1),
        Some(HOUR_OLD + 1),
        false,
        Some(0),
        None,
        None,
        None,
        now_ns(),
    )
    .await
    .expect("bounded run runs");

    assert_eq!(report.compacted, 2, "only the newer hour of each shard");
    assert_eq!(report.not_sealed, 0);
    for shard in 0..SHARDS {
        assert_eq!(
            bucket_output(store.as_ref(), shard, HOUR_OLD).await,
            (0, 0),
            "shard {shard}'s older hour was outside [--from-hour, --to-hour]"
        );
        assert_eq!(
            bucket_output(store.as_ref(), shard, HOUR_OLD + 1).await,
            (1, 1)
        );
    }
}

/// Knob validation precedes shard resolution: a zero byte target on a tenant
/// with NO provisioning record surfaces as its `CompactorKnobError`, never
/// masked by `NoProvisioningRecord` (review finding on PR #1005).
///
/// Non-vacuity (prove-the-test): move the `build_compactor_config` call in
/// `maintain::compact_tenant` back below `resolve_shard_count` and this
/// downcast fails with `CompactTenantError::NoProvisioningRecord` instead.
#[tokio::test]
async fn zero_knob_is_refused_before_shard_resolution() {
    let store = seed_tenant().await;

    let err = compact_tenant(
        store,
        MEMORY,
        TENANT,
        SignalArg::Logs,
        None,
        None,
        None,
        true,
        None,
        Some(0),
        None,
        None,
        now_ns(),
    )
    .await
    .expect_err("a zero byte target must be refused before any store access");

    let typed = err
        .downcast_ref::<ravel_cli::maintain::CompactorKnobError>()
        .expect("typed CompactorKnobError, not NoProvisioningRecord");
    assert_eq!(
        *typed,
        ravel_cli::maintain::CompactorKnobError::ZeroL1PartMemoryTarget
    );
}

/// Issue #1024: `--store` omitted means the empty in-process memory store, and
/// a walk over a tenant that holds nothing there is refused with a typed error
/// naming the tenant and the remedy, instead of the healthy-looking
/// `compacted: 0, not_sealed: 0` and exit 0 that burned two measurement runs
/// against a 100M-row S3 tenant.
///
/// Non-vacuity (prove-the-test): the pre-change tree had no store selection at
/// all and returned `Ok(CompactTenantReport { compacted: 0, .. })` here; this
/// `expect_err` fails against it. Against the post-change tree, deleting the
/// `require_tenant_data_present` call in `maintain::compact_tenant` flips this
/// back to that `Ok` (the `--shards 2` override means shard resolution does not
/// refuse first), and deleting only the `DefaultedMemoryEmptyWalk::check` call
/// leaves this test passing but fails
/// `defaulted_store_refuses_when_no_hour_resolves_under_a_provisioned_tenant`.
#[tokio::test]
async fn defaulted_store_with_nothing_under_the_tenant_prefix_is_a_typed_refusal() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    let err = compact_tenant(
        store,
        StoreSelection::defaulted_memory(),
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        false,
        None,
        None,
        None,
        None,
        now_ns(),
    )
    .await
    .expect_err("a walk over an unchosen empty store must refuse, not report zeros");

    let typed = err
        .downcast_ref::<DefaultedMemoryEmptyWalk>()
        .expect("typed DefaultedMemoryEmptyWalk");
    assert_eq!(
        *typed,
        DefaultedMemoryEmptyWalk {
            command: "maintain compact-tenant",
            tenant: TENANT.to_string(),
            searched: "objects",
        }
    );
    let text = err.to_string();
    assert!(
        text.contains(TENANT),
        "the error must name the tenant: {text}"
    );
    assert!(
        text.contains("--store defaulted to memory") && text.contains("--store s3"),
        "the error must name the situation and the remedy: {text}"
    );
}

/// The same refusal for the exact condition issue #1024 states: zero present
/// ingest-hour buckets resolved across every shard. The tenant prefix here is
/// NOT empty (it holds the shard-count provisioning record the walk resolves
/// its shard range from), so this is the post-walk check firing, not the
/// precondition above.
///
/// Non-vacuity (prove-the-test): delete the `DefaultedMemoryEmptyWalk::check`
/// call at the end of `maintain::compact_tenant` and this returns
/// `Ok(CompactTenantReport { shards: 2, compacted: 0, .. })`; the `expect_err`
/// fails. The `searched` field is what separates the two guards: swap it for
/// `"objects"` and the `assert_eq!` below fails.
#[tokio::test]
async fn defaulted_store_refuses_when_no_hour_resolves_under_a_provisioned_tenant() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    ravel_catalog::validate_or_adopt(
        store.as_ref(),
        &tenant_hash(),
        Signal::Logs,
        SHARDS,
        0,
        ravel_catalog::AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("write the shard-count provisioning record");

    let err = compact_tenant(
        store,
        StoreSelection::defaulted_memory(),
        TENANT,
        SignalArg::Logs,
        // No --shards: the shard count resolves from the provisioning record,
        // so the walk reaches the per-shard hour listing and finds nothing.
        None,
        None,
        None,
        false,
        None,
        None,
        None,
        None,
        now_ns(),
    )
    .await
    .expect_err("zero present hours across every shard must refuse on the defaulted store");

    let typed = err
        .downcast_ref::<DefaultedMemoryEmptyWalk>()
        .expect("typed DefaultedMemoryEmptyWalk");
    assert_eq!(
        *typed,
        DefaultedMemoryEmptyWalk {
            command: "maintain compact-tenant",
            tenant: TENANT.to_string(),
            searched: "present ingest-hour buckets",
        }
    );
}

/// An EXPLICIT `--store memory` keeps today's behavior on the same emptiness:
/// a zero-counter report, exit 0, with the header naming the store the operator
/// chose. This is what every in-process test does, and the reason the refusal
/// keys on the defaulted store rather than on emptiness alone.
///
/// Non-vacuity (prove-the-test): make `StoreSelection::is_defaulted_memory`
/// return `self.kind == StoreKind::Memory` (dropping the `defaulted` term) and
/// this `expect` fails with the #1024 refusal.
#[tokio::test]
async fn explicit_memory_with_an_empty_store_still_reports_zero_counters() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    let report = compact_tenant(
        store,
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        false,
        None,
        None,
        None,
        None,
        now_ns(),
    )
    .await
    .expect("an explicitly chosen empty memory store is a zero-count success");

    assert_eq!(
        report,
        ravel_cli::maintain::CompactTenantReport {
            store: MEMORY,
            shards: SHARDS,
            compacted: 0,
            already: 0,
            not_sealed: 0,
            below_min: 0,
            tombstoned: 0,
            parts_written: 0,
        },
        "the zero-count report is unchanged from before issue #1024"
    );
    assert_eq!(report.store.header(), "store: memory");
}

/// The refusal keys on emptiness, not on the flag being omitted: the same
/// defaulted selection over a populated store walks and compacts exactly as an
/// explicit `--store memory` does, and its header carries the `(default)`
/// marker so the choice is visible in the output either way.
#[tokio::test]
async fn a_defaulted_store_that_does_hold_data_walks_normally() {
    let store = seed_tenant().await;
    let defaulted = StoreSelection::defaulted_memory();

    let report = compact_tenant(
        store,
        defaulted,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        false,
        None,
        None,
        None,
        None,
        now_ns(),
    )
    .await
    .expect("a defaulted store holding data is walked, not refused");

    assert_eq!(report.compacted, 2, "one sealed hour per shard, as always");
    assert_eq!(report.not_sealed, 2);
    assert_eq!(report.store, defaulted);
    assert_eq!(
        report.store.header(),
        "store: memory (default)",
        "the header marks a store nobody chose, even when the walk finds data"
    );
}

/// The report header text for every effective store, including the
/// s3-configured invocation: `StoreSelection` is the report builder unit the
/// walk commands print their first line from, so this pins all three spellings
/// in one place.
///
/// Non-vacuity (prove-the-test): drop the `(default)` branch from
/// `StoreSelection::header` and the first assertion fails.
#[test]
fn the_report_header_names_the_effective_store() {
    assert_eq!(
        StoreSelection::defaulted_memory().header(),
        "store: memory (default)"
    );
    assert_eq!(
        StoreSelection::explicit(StoreKind::Memory).header(),
        "store: memory"
    );
    assert_eq!(
        StoreSelection::explicit(StoreKind::S3).header(),
        "store: s3"
    );
}
