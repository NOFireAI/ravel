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

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use prost::Message;
use ravel_cli::maintain::{
    ClaimOptions, CompactTenantError, CompactTenantReport, SignalArg, compact_tenant,
    compact_tenant_to, compact_to, per_bucket_config,
};
use ravel_cli::store::{DefaultedMemoryEmptyWalk, StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_fleet::claim::{
    Acquisition, COMPACTION_CLAIMS_PREFIX, ClaimConfig, ClaimOwner, WorkIdentity,
    compaction_claim_key,
};
use ravel_logseg::{AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_maintain::config::DEFAULT_MERGE_CURSOR_BUDGET_BYTES;
use ravel_maintain::{CompactorConfig, FixedClock};
use ravel_object_store::fault::{FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, MultipartUpload, ObjectMeta,
    ObjectStoreBackend, PageToken, Pin, PinnedRead, PutOptions, PutOutcome, StoreError, list_all,
};
use ravel_proto::sys::v1::{ClaimState, CompactionClaim};
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
/// exactly as the ingest log shard would. The writer id is random, so two
/// independently seeded stores are NOT byte-identical; use [`seed_l0_with`] for
/// a deterministic writer id when a cross-run object comparison needs it.
async fn seed_l0(store: &dyn ObjectStoreBackend, shard: u32, hour: u32, seq: u64) {
    seed_l0_with(store, shard, hour, seq, Uuid::new_v4()).await;
}

/// [`seed_l0`] with an explicit `writer_id`, so two stores seeded with the same
/// ids hold byte-identical L0 objects and therefore compact to byte-identical
/// L1 output (the concurrency-determinism test relies on this).
async fn seed_l0_with(
    store: &dyn ObjectStoreBackend,
    shard: u32,
    hour: u32,
    seq: u64,
    writer_id: Uuid,
) {
    let th = tenant_hash();
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

/// A deterministic writer id for `(shard, hour, seq)`, so two independently
/// seeded stores are byte-identical.
fn det_writer_id(shard: u32, hour: u32, seq: u64) -> Uuid {
    Uuid::from_u128((u128::from(shard) << 96) | (u128::from(hour) << 32) | u128::from(seq))
}

/// The same 2-shard, 4-bucket logs fixture [`seed_tenant`] builds, but with
/// DETERMINISTIC writer ids, so two stores seeded this way hold byte-identical
/// L0 objects and compact to byte-identical L1 output. Returns the concrete
/// `MemoryStore` so a caller can wrap it (e.g. in a `FaultStore`) before the run.
async fn seed_tenant_det() -> MemoryStore {
    let store = MemoryStore::new();
    for shard in 0..SHARDS {
        for hour in [HOUR_OLD, HOUR_OLD + 1] {
            for seq in 1..=2u64 {
                seed_l0_with(&store, shard, hour, seq, det_writer_id(shard, hour, seq)).await;
            }
        }
    }
    store
}

/// Every object under the tenant prefix as a `key -> bytes` map: the whole
/// physical state, for the concurrency-determinism comparison.
async fn all_objects(store: &dyn ObjectStoreBackend) -> BTreeMap<String, Vec<u8>> {
    let mut objects = BTreeMap::new();
    for meta in list_all(store, &format!("t/{}/", tenant_hash().to_hex()))
        .await
        .expect("list tenant prefix")
    {
        let got = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get object");
        objects.insert(meta.key, got.data.to_vec());
    }
    objects
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
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
            rewrite_present: 0,
            not_sealed: 0,
            below_min: 0,
            tombstoned: 0,
            claim_skipped: 0,
            claim_cancelled: 0,
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
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

/// `--bucket-concurrency 1` (the default) is today's behavior byte-for-byte on
/// a multi-bucket fixture: the exact same report struct the pre-flag sequential
/// walk returned (two shards, one sealed hour compacted per shard, one unsealed
/// hour reported not-sealed per shard, one L1 part per compacted bucket). The
/// per-bucket lines are emitted in walk order (shard then hour) by construction,
/// since N=1 dispatches and joins one bucket at a time.
///
/// Non-vacuity (prove-the-test): change the `bucket_concurrency` argument below
/// from 1 to 0 and the call returns `Err(ZeroBucketConcurrency)` instead of this
/// `Ok`; change `report.compacted`'s expected value to 1 and the struct
/// assertion fails.
#[tokio::test]
async fn bucket_concurrency_one_is_todays_sequential_report() {
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
        1,
        now_ns(),
        &ClaimOptions::fresh(),
    )
    .await
    .expect("N=1 compact-tenant runs");

    assert_eq!(
        report,
        ravel_cli::maintain::CompactTenantReport {
            store: MEMORY,
            shards: SHARDS,
            compacted: 2,
            already: 0,
            rewrite_present: 0,
            not_sealed: 2,
            below_min: 0,
            tombstoned: 0,
            claim_skipped: 0,
            claim_cancelled: 0,
            parts_written: 2,
        },
        "N=1 must report exactly today's sequential outcome"
    );
}

/// `--bucket-concurrency 4` over a 4-bucket fixture (both hours sealed via
/// `--max-flush-lifetime 0s`) compacts all four and, critically, writes the
/// EXACT SAME stored objects as the N=1 run: content-addressed keys and bytes
/// are byte-identical, proving concurrency changes no part of the output. Both
/// stores are seeded with deterministic writer ids so their pre-compaction L0
/// state is identical; if compaction were order-sensitive the L1 parts or
/// records would diverge.
///
/// Non-vacuity (prove-the-test): in `run_bucket_walk`, drop the
/// `collected.sort_by_key(|(idx, ..)| *idx)` line so outcomes come back in
/// completion order -- the reports still match (counts are order-free) but this
/// stays green because objects are content-addressed; instead, to see it fail,
/// give every concurrent bucket the SAME shared tracker (replace the
/// per-bucket `MergeMemoryTracker::new()` with a clone of `base`'s) and the
/// determinism argument the doc makes no longer holds. The strongest flip:
/// change the N=4 argument to 4 while asserting `report_n4 != report_n1` and the
/// equality assertion fails, or change `all_objects` equality to `!=` and it
/// fails, showing the objects really are identical.
///
/// Runs on a MULTI-THREAD runtime with four worker threads: under the default
/// `#[tokio::test]` current-thread runtime every `MemoryStore` future resolves on
/// its first poll, so the four `JoinSet` tasks complete in dispatch order and the
/// N=4 run is operationally serial -- the concurrency this test exists to
/// exercise is never exercised. The flavor line below is the fix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_concurrency_four_writes_identical_objects_to_n1() {
    let store_n4: Arc<dyn ObjectStoreBackend> = Arc::new(seed_tenant_det().await);
    let store_n1: Arc<dyn ObjectStoreBackend> = Arc::new(seed_tenant_det().await);

    // Identical fixtures before either run.
    assert_eq!(
        all_objects(store_n4.as_ref()).await,
        all_objects(store_n1.as_ref()).await,
        "the two deterministic fixtures must start byte-identical"
    );

    let report_n4 = compact_tenant(
        store_n4.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        false,
        Some(0),
        None,
        None,
        None,
        4,
        now_ns(),
        &ClaimOptions::fresh(),
    )
    .await
    .expect("N=4 compact-tenant runs");

    let report_n1 = compact_tenant(
        store_n1.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        false,
        Some(0),
        None,
        None,
        None,
        1,
        now_ns(),
        &ClaimOptions::fresh(),
    )
    .await
    .expect("N=1 compact-tenant runs");

    assert_eq!(report_n4.compacted, 4, "all four buckets compacted at N=4");
    assert_eq!(report_n4.parts_written, 4);
    assert_eq!(
        report_n4, report_n1,
        "N=4 and N=1 must report identical counters"
    );

    assert_eq!(
        all_objects(store_n4.as_ref()).await,
        all_objects(store_n1.as_ref()).await,
        "N=4 must write byte-identical objects (content-addressed keys and bytes) to N=1"
    );
}

/// One bucket rigged to fail (a `FaultStore` Permanent error on every PUT under
/// shard 0's older hour) does NOT abort its siblings: the other three sealed
/// buckets still publish their L1 part and compaction record, the run exits
/// non-zero with a typed `BucketsFailed { failed: 1, succeeded: 3 }`, and the
/// failed bucket's own typed error is carried in the aggregate detail.
///
/// Non-vacuity (prove-the-test): in `run_bucket_walk`, propagate the first
/// bucket error with `?` instead of capturing it as the slot's `Err` (i.e.
/// abort the run on the first failure) and the three siblings no longer publish
/// -- `bucket_output(1, HOUR_OLD)` flips from `(1, 1)` to `(0, 0)`. Change the
/// expected `failed`/`succeeded` to `2`/`2` and the field assertions fail.
#[tokio::test]
async fn one_bucket_failure_does_not_abort_its_siblings() {
    let mem = seed_tenant_det().await;
    // Fail every PUT under shard 0's older-hour bucket: its L1-part PUT (the
    // first PUT compaction issues for that bucket) errors, so that bucket alone
    // fails while the other three sealed buckets publish normally.
    let bucket_path = format!("/{:04}/{}/", 0, keys::ingest_hour_string(HOUR_OLD));
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("injected compaction fault".into()),
        )
        .with_key_contains(bucket_path),
    );
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(mem, plan));

    let err = compact_tenant(
        store.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        false,
        // Seal both hours of both shards, so all four buckets are compacted
        // (except the rigged one).
        Some(0),
        None,
        None,
        None,
        4,
        now_ns(),
        &ClaimOptions::fresh(),
    )
    .await
    .expect_err("a failed bucket must make the run exit non-zero");

    let typed = err
        .downcast_ref::<CompactTenantError>()
        .expect("typed CompactTenantError::BucketsFailed");
    match typed {
        CompactTenantError::BucketsFailed {
            failed,
            succeeded,
            details,
        } => {
            assert_eq!(*failed, 1, "exactly one bucket failed");
            assert_eq!(*succeeded, 3, "the other three buckets succeeded");
            assert!(
                details.contains(&format!("shard 0 hour {HOUR_OLD}")),
                "the failed bucket must be named in the aggregate: {details}"
            );
            assert!(
                details.contains("injected compaction fault"),
                "the failed bucket's typed error must be carried: {details}"
            );
        }
        other => panic!("expected BucketsFailed, got {other:?}"),
    }

    // The failed bucket published nothing; the three siblings all published.
    assert_eq!(
        bucket_output(store.as_ref(), 0, HOUR_OLD).await,
        (0, 0),
        "the rigged bucket wrote no compaction record or L1 part"
    );
    for (shard, hour) in [(0, HOUR_OLD + 1), (1, HOUR_OLD), (1, HOUR_OLD + 1)] {
        assert_eq!(
            bucket_output(store.as_ref(), shard, hour).await,
            (1, 1),
            "sibling bucket shard {shard} hour {hour} published despite the failure"
        );
    }
}

/// `--bucket-concurrency 0` is refused with its typed error before any bucket
/// runs. A fan-out of zero compacts nothing, which is an operator mistake, not a
/// no-op success.
///
/// Non-vacuity (prove-the-test): delete the `if bucket_concurrency == 0` guard
/// in `maintain::compact_tenant` and this `expect_err` fails -- the walk then
/// runs with a zero-slot dispatch loop that spawns nothing and returns an empty,
/// misleadingly-clean report.
#[tokio::test]
async fn zero_bucket_concurrency_is_refused_typed() {
    let store = seed_tenant().await;

    let err = compact_tenant(
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
        0,
        now_ns(),
        &ClaimOptions::fresh(),
    )
    .await
    .expect_err("--bucket-concurrency 0 must be refused");

    let typed = err
        .downcast_ref::<CompactTenantError>()
        .expect("typed CompactTenantError");
    assert_eq!(*typed, CompactTenantError::ZeroBucketConcurrency);
}

/// One fixture closing three spec properties at once, over the 4-bucket corpus
/// (both hours sealed via `--max-flush-lifetime 0s`) at `--bucket-concurrency 2`:
///
/// - shard 0's OLD-hour bucket (walk index 0) is rigged to FAIL (a `FaultStore`
///   Permanent error on every PUT under its path), and it is dispatched in the
///   first N=2 batch, so it fails early and frees a slot before the walk ends.
/// - shard 0's NEW-hour bucket (walk index 1) has its FIRST GET HELD by a
///   `FaultStore` gate, so it cannot complete until the test releases it. It is
///   also in the first batch. The test releases it only AFTER both shard-1
///   buckets (walk indices 2 and 3, dispatched by the refill path once the
///   failed bucket freed its slot) have already published. So completion order
///   is provably 0, 2, 3, 1 -- index 1 finishes LAST -- while walk order is
///   0, 1, 2, 3.
///
/// (a) The captured per-bucket lines are in walk order (shard then hour) despite
///     that differing completion order: contiguous-prefix streaming holds index
///     1's line (and, behind it, 2 and 3) until index 1 completes, then flushes
///     all three in order.
/// (b) The failed bucket prints its OWN line in the exact `shard={} hour={}
///     outcome=Failed error=...` shape carrying its typed error, not only the
///     aggregate.
/// (c) The refill path works across a failed join: indices 2 and 3, dispatched
///     only after index 0's failed join freed a slot, both start and publish.
///
/// Prove-the-test (each flip names the line it breaks):
/// - Drop the contiguous-prefix drain in `run_bucket_walk` (emit in completion
///   order instead): the captured lines become 0, 2, 3, 1, so `lines[1]` is
///   `shard=1 hour={HOUR_OLD} ...` and the `lines[1]` walk-order assertion fails.
/// - Break the Failed line shape in `emit_bucket_outcome` (e.g. drop the
///   `error={err}` tail): the `lines[0]` Failed-shape assertion fails.
/// - Abort the walk on the first `Err` (propagate with `?` and drop the
///   `JoinSet` instead of capturing the slot): indices 2 and 3 are never spawned,
///   so `bucket_output(1, HOUR_OLD)` stays `(0, 0)` and the refill assertion
///   fails (and the released index-1 bucket is aborted, never publishing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_preserves_walk_order_across_out_of_order_completion() {
    let mem = seed_tenant_det().await;
    // Fail every PUT under shard 0's OLD-hour bucket: walk index 0 fails on its
    // first L1-part PUT.
    let failed_path = format!("/{:04}/{}/", 0, keys::ingest_hour_string(HOUR_OLD));
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("injected compaction fault".into()),
        )
        .with_key_contains(failed_path),
    );
    let fault = FaultStore::new(mem, plan);
    // Hold the FIRST GET under shard 0's NEW-hour bucket (walk index 1), so that
    // bucket parks until released. Nth(1) holds only the first match, so later
    // reads (including the test's own `bucket_output` probes) pass through.
    let held_path = format!("/{:04}/{}/", 0, keys::ingest_hour_string(HOUR_OLD + 1));
    let gate = fault.hold(Op::Get, Some(held_path), Occurrence::Nth(1));
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(fault);
    let claims = ClaimOptions::fresh();

    let mut out: Vec<u8> = Vec::new();
    let walk = compact_tenant_to(
        &mut out,
        store.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        false,
        // Seal both hours so all four buckets are compacted (bar the rigged one).
        Some(0),
        None,
        None,
        None,
        2,
        now_ns(),
        &claims,
    );

    let releaser = async {
        // Wait until index 1's first GET is parked on the gate.
        gate.wait_until_held(1).await;
        // Let the two shard-1 buckets (refilled after index 0's failed join)
        // run to completion while index 1 stays parked. Bounded so a broken
        // premise fails the test instead of hanging it forever.
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                let s1_old = bucket_output(store.as_ref(), 1, HOUR_OLD).await;
                let s1_new = bucket_output(store.as_ref(), 1, HOUR_OLD + 1).await;
                if s1_old == (1, 1) && s1_new == (1, 1) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("shard-1 buckets did not complete while index 1 was held");
        // Completion order provably differs from walk order at this instant:
        // the later-walk-order shard-1 buckets have published, but index 1
        // (walk-order second) has not, because it is still held.
        assert_eq!(
            bucket_output(store.as_ref(), 0, HOUR_OLD + 1).await,
            (0, 0),
            "held bucket (walk index 1) must not have completed while later-walk-order buckets did"
        );
        let held = gate.held();
        assert_eq!(held.len(), 1, "exactly index 1's GET is held");
        assert!(gate.release(held[0]), "releasing the held GET succeeds");
    };

    let (result, ()) = tokio::join!(walk, releaser);

    // The run exits non-zero: one bucket failed.
    let err = result.expect_err("a failed bucket makes the run exit non-zero");
    match err
        .downcast_ref::<CompactTenantError>()
        .expect("typed BucketsFailed")
    {
        CompactTenantError::BucketsFailed {
            failed, succeeded, ..
        } => {
            assert_eq!(*failed, 1);
            assert_eq!(*succeeded, 3);
        }
        other => panic!("expected BucketsFailed, got {other:?}"),
    }

    let text = String::from_utf8(out).expect("output is utf-8");
    let lines: Vec<&str> = text.lines().filter(|l| l.starts_with("shard=")).collect();
    assert_eq!(lines.len(), 4, "one per-bucket line each: {text}");

    // (a) Emitted in walk order (shard then hour), NOT completion order 0,2,3,1.
    assert!(
        lines[0].starts_with(&format!("shard=0 hour={HOUR_OLD} outcome=")),
        "line 0 is shard 0's old hour: {}",
        lines[0]
    );
    assert!(
        lines[1].starts_with(&format!("shard=0 hour={} outcome=", HOUR_OLD + 1)),
        "line 1 is shard 0's new hour despite completing last: {}",
        lines[1]
    );
    assert!(
        lines[2].starts_with(&format!("shard=1 hour={HOUR_OLD} outcome=")),
        "line 2 is shard 1's old hour: {}",
        lines[2]
    );
    assert!(
        lines[3].starts_with(&format!("shard=1 hour={} outcome=", HOUR_OLD + 1)),
        "line 3 is shard 1's new hour: {}",
        lines[3]
    );

    // (b) The failed bucket's own line, exact shape, carrying its typed error.
    assert!(
        lines[0].starts_with(&format!("shard=0 hour={HOUR_OLD} outcome=Failed error=")),
        "failed bucket prints its own Failed line: {}",
        lines[0]
    );
    assert!(
        lines[0].contains("injected compaction fault"),
        "the failed bucket's typed error rides its line: {}",
        lines[0]
    );
    // The three survivors each print a Compacted line, exact shape.
    assert_eq!(
        lines[1],
        format!(
            "shard=0 hour={} outcome=Compacted parts=1 publish=Published",
            HOUR_OLD + 1
        )
    );
    assert_eq!(
        lines[2],
        format!("shard=1 hour={HOUR_OLD} outcome=Compacted parts=1 publish=Published")
    );
    assert_eq!(
        lines[3],
        format!(
            "shard=1 hour={} outcome=Compacted parts=1 publish=Published",
            HOUR_OLD + 1
        )
    );

    // (c) Refill across the failed join: indices 2 and 3 (dispatched only after
    // index 0's failed join freed a slot) both published; the held index 1
    // published after release; the failed index 0 published nothing.
    assert_eq!(
        bucket_output(store.as_ref(), 1, HOUR_OLD).await,
        (1, 1),
        "refill bucket (walk index 2) published"
    );
    assert_eq!(
        bucket_output(store.as_ref(), 1, HOUR_OLD + 1).await,
        (1, 1),
        "refill bucket (walk index 3) published"
    );
    assert_eq!(
        bucket_output(store.as_ref(), 0, HOUR_OLD + 1).await,
        (1, 1),
        "held bucket (walk index 1) published after release"
    );
    assert_eq!(
        bucket_output(store.as_ref(), 0, HOUR_OLD).await,
        (0, 0),
        "failed bucket (walk index 0) published nothing"
    );
}

/// F8 (ADR-0979 whole-box sizing under concurrency): with no operator-configured
/// merge cursor budget, each concurrent bucket's `CompactorConfig` carries a
/// per-bucket SHARE of the default 20 GiB budget, `DEFAULT_MERGE_CURSOR_BUDGET_BYTES
/// / N` (integer floor), so N buckets' merges still fit the one reference box the
/// default was sized against. This pins the exact figure each bucket receives.
///
/// Prove-the-test: change the divisor in `per_bucket_config` from `concurrency`
/// to `1` (no division) and the N=4 assertion fails (5 GiB expected, 20 GiB
/// seen); change the expected `/ 4` here to `/ 2` and it fails too.
#[test]
fn per_bucket_merge_budget_is_the_default_divided_by_n() {
    let base = CompactorConfig::default();
    // N=1 is unchanged: the full default budget.
    assert_eq!(
        per_bucket_config(&base, 1).merge_cursor_budget_bytes,
        DEFAULT_MERGE_CURSOR_BUDGET_BYTES,
        "N=1 carries the full default budget"
    );
    // N=4 splits it exactly four ways (20 GiB / 4 = 5 GiB).
    assert_eq!(
        per_bucket_config(&base, 4).merge_cursor_budget_bytes,
        DEFAULT_MERGE_CURSOR_BUDGET_BYTES / 4,
        "each of four concurrent buckets holds a quarter of the default budget"
    );
    // A fresh per-bucket tracker is installed (never the shared base tracker).
    assert!(
        per_bucket_config(&base, 4).merge_memory_tracker.is_some(),
        "each bucket carries its own tracker"
    );
    // An explicitly configured budget is a whole-box decision and passes
    // through undivided at any N; only the box-sized default is split.
    let explicit = CompactorConfig {
        merge_cursor_budget_bytes: 7 * 1024 * 1024 * 1024,
        ..CompactorConfig::default()
    };
    assert_eq!(
        per_bucket_config(&explicit, 4).merge_cursor_budget_bytes,
        7 * 1024 * 1024 * 1024,
        "an operator-configured budget is not divided"
    );
    // A zero concurrency cannot reach here through the CLI (refused typed
    // upstream); the division clamps rather than panicking.
    assert_eq!(
        per_bucket_config(&base, 0).merge_cursor_budget_bytes,
        DEFAULT_MERGE_CURSOR_BUDGET_BYTES,
        "concurrency zero clamps to one instead of dividing by zero"
    );
}

// ---- Advisory compaction claims (ADR-1029 decision 5, issue #1034) ----

/// The walk's shard/hour of the bucket a foreign process claims in the claim
/// tests: the third bucket in walk order, so buckets on both sides of it run.
const CLAIMED_SHARD: u32 = 1;

/// Every request served, as `(operation, key or prefix)`, so a test can pin
/// the claim protocol's requests (every key under `COMPACTION_CLAIMS_PREFIX`:
/// the requests the compactor's ledger books under its coordinate phase) and
/// the L1 part PUTs separately.
struct CountingStore {
    inner: Arc<dyn ObjectStoreBackend>,
    log: Mutex<Vec<(&'static str, String)>>,
}

impl CountingStore {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
        Arc::new(CountingStore {
            inner,
            log: Mutex::new(Vec::new()),
        })
    }

    fn note(&self, op: &'static str, key: &str) {
        self.log
            .lock()
            .expect("log lock")
            .push((op, key.to_string()));
    }

    fn entries(&self) -> Vec<(&'static str, String)> {
        self.log.lock().expect("log lock").clone()
    }

    /// Requests of any kind against the claim key space.
    fn claim_requests(&self) -> usize {
        self.entries()
            .iter()
            .filter(|(_, key)| key.starts_with(COMPACTION_CLAIMS_PREFIX))
            .count()
    }

    /// Requests of kind `op` against one claim key.
    fn requests_on(&self, op: &str, key: &str) -> usize {
        self.entries()
            .iter()
            .filter(|(o, k)| *o == op && k == key)
            .count()
    }

    /// PUTs (single or multipart) of L1 parts.
    fn part_puts(&self) -> usize {
        let l1 = format!("/{}/", keys::L1_DIR);
        self.entries()
            .iter()
            .filter(|(o, k)| (*o == "put" || *o == "multipart") && k.contains(&l1))
            .count()
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
        self.note("put", key);
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.note("get", key);
        self.inner.get(key, range).await
    }

    async fn get_pinned(
        &self,
        key: &str,
        range: GetRange,
        pin: &Pin,
    ) -> Result<PinnedRead, StoreError> {
        self.note("get", key);
        self.inner.get_pinned(key, range, pin).await
    }

    async fn get_with_pin(&self, key: &str, range: GetRange) -> Result<PinnedRead, StoreError> {
        self.note("get", key);
        self.inner.get_with_pin(key, range).await
    }

    async fn pin_of(&self, key: &str) -> Result<(ObjectMeta, Pin), StoreError> {
        self.note("head", key);
        self.inner.pin_of(key).await
    }

    async fn put_multipart<'a>(
        &'a self,
        key: &str,
    ) -> Result<Box<dyn MultipartUpload + 'a>, StoreError> {
        self.note("multipart", key);
        self.inner.put_multipart(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.note("head", key);
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.note("list", prefix);
        self.inner.list(prefix, page).await
    }

    async fn list_after(
        &self,
        prefix: &str,
        start_after: Option<&str>,
        page: Option<PageToken>,
    ) -> Result<ListPage, StoreError> {
        self.note("list", prefix);
        self.inner.list_after(prefix, start_after, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.note("list", prefix);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.note("delete", key);
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    fn observed_store_time_ns(&self) -> Option<i64> {
        self.inner.observed_store_time_ns()
    }
}

/// Claims on, with the cost gate at one byte so every fixture bucket (a few
/// hundred bytes of input) is claimed.
fn claiming() -> ClaimOptions {
    ClaimOptions {
        min_input_bytes: Some(1),
        ..ClaimOptions::fresh()
    }
}

/// The claim key of `(shard, hour)` in the fixture tenant's logs signal.
fn claim_key(shard: u32, hour: u32) -> String {
    compaction_claim_key(&WorkIdentity::new(tenant_hash(), Signal::Logs, shard, hour).work_id())
}

/// Every claim object in the store, decoded.
async fn claim_objects(store: &dyn ObjectStoreBackend) -> BTreeMap<String, CompactionClaim> {
    let mut claims = BTreeMap::new();
    for meta in list_all(store, COMPACTION_CLAIMS_PREFIX)
        .await
        .expect("list claims")
    {
        let got = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get claim");
        let claim = CompactionClaim::decode(got.data).expect("decode claim");
        claims.insert(meta.key, claim);
    }
    claims
}

/// The four-bucket deterministic fixture with the store's own clock (the
/// `last_modified` a claim's expiry is judged from) set to the real wall
/// clock, which is what the CLI's claim participant reads. Returns the store
/// and that clock reading in milliseconds.
async fn claim_fixture() -> (Arc<MemoryStore>, i64) {
    let mem = seed_tenant_det().await;
    let wall_ms = ravel_cli::now_ns().expect("wall clock") / 1_000_000;
    mem.set_clock_ms(u64::try_from(wall_ms).expect("positive wall clock"));
    (Arc::new(mem), wall_ms)
}

/// A live claim on `(shard, hour)` written by a different process, through the
/// same primitive a supervisor uses, at the default 300 s lease.
async fn claim_as_foreign(store: &dyn ObjectStoreBackend, shard: u32, hour: u32) -> Uuid {
    let foreign = Uuid::new_v4();
    let acquired = ravel_fleet::claim::acquire(
        store,
        &WorkIdentity::new(tenant_hash(), Signal::Logs, shard, hour),
        &ClaimOwner::new(foreign, Uuid::new_v4(), 0),
        &ClaimConfig::default(),
    )
    .await
    .expect("foreign acquire");
    assert!(
        matches!(acquired, Acquisition::Acquired { .. }),
        "the foreign claim must be fresh: {acquired:?}"
    );
    foreign
}

/// Run the whole walk (both hours sealed) at `concurrency`, capturing its
/// output.
async fn run_claim_walk(
    store: Arc<dyn ObjectStoreBackend>,
    dry_run: bool,
    concurrency: usize,
    claims: &ClaimOptions,
) -> (anyhow::Result<CompactTenantReport>, String) {
    let mut out: Vec<u8> = Vec::new();
    let result = compact_tenant_to(
        &mut out,
        store,
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        None,
        None,
        dry_run,
        Some(0),
        None,
        None,
        None,
        concurrency,
        now_ns(),
        claims,
    )
    .await;
    (result, String::from_utf8(out).expect("utf-8 output"))
}

/// Acceptance for issue #1034: a bucket another process holds a live claim on
/// is skipped, reported with the reason, the holder and the claim expiry, and
/// counted as `claim_skipped`, while every other bucket of the walk is claimed
/// and compacted.
///
/// Distinguishing: a CLI that claims but prints nothing for the skipped bucket
/// fails the skip-line assertion (three bucket lines instead of four); one
/// that treats the refused claim as an error fails at `expect`, since the walk
/// then exits non-zero with `BucketsFailed`. Against HEAD, which never claims,
/// the claimed bucket is compacted and `report.compacted` is 4, not 3.
#[tokio::test]
async fn compact_tenant_skips_a_bucket_a_foreign_process_has_claimed() {
    let (mem, wall_ms) = claim_fixture().await;
    let foreign = claim_as_foreign(mem.as_ref(), CLAIMED_SHARD, HOUR_OLD).await;
    let store = CountingStore::new(mem.clone());
    let claims = claiming();

    let (result, text) = run_claim_walk(store.clone(), false, 1, &claims).await;
    let report = result.expect("a claim-skipped bucket is not a failure");

    assert_eq!(
        report.compacted, 3,
        "every unclaimed bucket compacts: {text}"
    );
    assert_eq!(
        report.claim_skipped, 1,
        "exactly the claimed bucket is skipped"
    );
    assert_eq!(report.claim_cancelled, 0);
    assert_eq!(report.parts_written, 3);

    let lines: Vec<&str> = text.lines().filter(|l| l.starts_with("shard=")).collect();
    assert_eq!(lines.len(), 4, "one line per bucket: {text}");
    // The foreign claim's expiry is the store's `last_modified` (the clock set
    // above) plus the 300 s default lease, and the retry point one ms past it.
    assert_eq!(
        lines[2],
        format!(
            "shard={CLAIMED_SHARD} hour={HOUR_OLD} outcome=ClaimSkipped reason=held_by_another \
             holder={foreign} claim_expiry_unix_ms={} retry_after_unix_ms={}",
            wall_ms + 300_000,
            wall_ms + 300_001
        ),
        "the skipped bucket's line: {text}"
    );
    for line in [lines[0], lines[1], lines[3]] {
        assert!(
            line.ends_with("outcome=Compacted parts=1 publish=Published"),
            "the other buckets compact: {line}"
        );
    }
    assert!(text.contains("\nclaim_skipped: 1\n"), "summary: {text}");
    assert!(text.contains("\nclaim_cancelled: 0\n"), "summary: {text}");
    assert!(text.contains("\ncompacted: 3\n"), "summary: {text}");
    assert!(
        text.contains(&format!(
            "\nclaims: on process_id={} min_input_bytes=1 lease_ms=300000\n",
            claims.process_id
        )),
        "header: {text}"
    );

    assert_eq!(
        bucket_output(mem.as_ref(), CLAIMED_SHARD, HOUR_OLD).await,
        (0, 0),
        "the skipped bucket was not merged"
    );
    for (shard, hour) in [(0, HOUR_OLD), (0, HOUR_OLD + 1), (1, HOUR_OLD + 1)] {
        assert_eq!(bucket_output(mem.as_ref(), shard, hour).await, (1, 1));
    }

    // The foreign claim is untouched, and each compacted bucket carries its
    // own completed claim written under this invocation's one process id.
    let objects = claim_objects(mem.as_ref()).await;
    assert_eq!(objects.len(), 4, "one claim per bucket");
    for (key, claim) in &objects {
        if *key == claim_key(CLAIMED_SHARD, HOUR_OLD) {
            assert_eq!(claim.owner_process_id, foreign.as_bytes().to_vec());
            assert_eq!(claim.state, ClaimState::Running as i32);
        } else {
            assert_eq!(
                claim.owner_process_id,
                claims.process_id.as_bytes().to_vec()
            );
            assert_eq!(claim.state, ClaimState::Completed as i32);
        }
    }
    // Three uncontended buckets pay a create and a completion each; the
    // refused one pays the rejected create, one GET and one HEAD.
    assert_eq!(store.claim_requests(), 3 * 2 + 3, "claim requests");
    assert_eq!(
        store.part_puts(),
        3,
        "no bucket merged twice, none skipped merged"
    );
}

/// `--no-claim` over the same store: the claimed bucket is compacted too, and
/// the walk issues zero requests against the claim key space.
///
/// Against HEAD the no-claim switch does not exist; against a build that
/// ignores it, the claimed bucket is skipped and `compacted` is 3.
#[tokio::test]
async fn no_claim_compacts_the_claimed_bucket_and_issues_no_claim_requests() {
    let (mem, _) = claim_fixture().await;
    claim_as_foreign(mem.as_ref(), CLAIMED_SHARD, HOUR_OLD).await;
    let store = CountingStore::new(mem.clone());
    let claims = ClaimOptions {
        no_claim: true,
        ..claiming()
    };

    let (result, text) = run_claim_walk(store.clone(), false, 1, &claims).await;
    let report = result.expect("no-claim walk runs");

    assert_eq!(
        report.compacted, 4,
        "the claimed bucket compacts too: {text}"
    );
    assert_eq!(report.claim_skipped, 0);
    assert_eq!(
        bucket_output(mem.as_ref(), CLAIMED_SHARD, HOUR_OLD).await,
        (1, 1)
    );
    assert_eq!(store.claim_requests(), 0, "no coordinate-phase request");
    assert!(
        text.contains("\nclaims: off (--no-claim)\n"),
        "header: {text}"
    );
    assert_eq!(
        claim_objects(mem.as_ref()).await.len(),
        1,
        "only the foreign claim"
    );
}

/// `--bucket-concurrency 4` over four claimable buckets: exactly four claims
/// are acquired and four completed, each on its own bucket's key under the one
/// process id, and each bucket is merged exactly once.
///
/// Multi-thread runtime so the four buckets genuinely run at once (see
/// `bucket_concurrency_four_writes_identical_objects_to_n1`). Against HEAD no
/// claim is written and the claim-object count is 0.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_concurrency_four_takes_one_claim_per_bucket() {
    let (mem, _) = claim_fixture().await;
    let store = CountingStore::new(mem.clone());
    let claims = claiming();

    let (result, text) = run_claim_walk(store.clone(), false, 4, &claims).await;
    let report = result.expect("N=4 walk runs");
    assert_eq!(report.compacted, 4, "{text}");
    assert_eq!(report.claim_skipped, 0);

    let objects = claim_objects(mem.as_ref()).await;
    assert_eq!(
        objects.len(),
        4,
        "four claims acquired: {:?}",
        objects.keys()
    );
    for shard in 0..SHARDS {
        for hour in [HOUR_OLD, HOUR_OLD + 1] {
            let key = claim_key(shard, hour);
            let claim = objects.get(&key).expect("each bucket has its own claim");
            assert_eq!(
                claim.owner_process_id,
                claims.process_id.as_bytes().to_vec()
            );
            assert_eq!(claim.state, ClaimState::Completed as i32, "completed");
            assert_eq!(
                store.requests_on("put", &key),
                2,
                "one create, one completion"
            );
            assert_eq!(bucket_output(mem.as_ref(), shard, hour).await, (1, 1));
        }
    }
    assert_eq!(
        store.claim_requests(),
        8,
        "four creates and four completions only"
    );
    assert_eq!(store.part_puts(), 4, "no bucket merged twice");
}

/// `--dry-run` takes no claim: zero claim objects written and zero claim
/// requests, even with the cost gate at one byte. The same store run for real
/// afterwards writes four, so the dry run's zero is not the fixture's.
#[tokio::test]
async fn dry_run_takes_no_claim() {
    let (mem, _) = claim_fixture().await;
    let store = CountingStore::new(mem.clone());
    let claims = claiming();

    let (result, text) = run_claim_walk(store.clone(), true, 1, &claims).await;
    result.expect("dry run runs");
    assert_eq!(
        claim_objects(mem.as_ref()).await.len(),
        0,
        "no claim object"
    );
    assert_eq!(store.claim_requests(), 0, "no claim request");
    assert!(
        text.contains("\nclaims: off (--dry-run)\n"),
        "header: {text}"
    );

    let (result, _) = run_claim_walk(store.clone(), false, 1, &claims).await;
    result.expect("real run runs");
    assert_eq!(
        claim_objects(mem.as_ref()).await.len(),
        4,
        "the real run claims"
    );
}

/// The claim participant reads its own injected clock, not the walk's frozen
/// one: a merge that outlives a third of the lease renews its claim.
///
/// Deterministic and sleep-free. The lease is 3 s, so the renewal cadence is
/// 1 s, and renewal is evaluated only at a checkpoint. The first L0 data GET of
/// shard 0's older bucket, which comes after its claim is taken, is held on a
/// `FaultStore` gate; with the merge parked there the test moves the claim
/// clock 1.5 s forward and releases the gate, so the next checkpoint renews
/// exactly once.
///
/// Non-vacuity (prove-the-test): make `ClaimOptions::clock` ignore the
/// override and return a frozen clock, which is what a participant built on
/// the walk's own `FixedClock` would see, and the `renewed_count` assertion
/// fails 0 against 1 (two claim PUTs instead of three).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_merge_longer_than_a_third_of_the_lease_renews_its_claim() {
    let (mem, wall_ms) = claim_fixture().await;
    let fault = FaultStore::new(mem.clone(), FaultPlan::empty());
    let gate = fault.hold(Op::Get, Some("/l0/0000/".to_string()), Occurrence::Nth(1));
    let store = CountingStore::new(Arc::new(fault));
    let claim_clock = FixedClock::new(wall_ms * 1_000_000);
    let claims = ClaimOptions {
        lease_duration: Some(Duration::from_secs(3)),
        clock: Some(Arc::new(claim_clock.clone()) as Arc<dyn ravel_maintain::Clock>),
        ..claiming()
    };
    let key = claim_key(0, HOUR_OLD);

    let mut out: Vec<u8> = Vec::new();
    let walk = compact_tenant_to(
        &mut out,
        store.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        Some(HOUR_OLD),
        Some(HOUR_OLD),
        false,
        Some(0),
        None,
        None,
        None,
        1,
        now_ns(),
        &claims,
    );
    let releaser = async {
        // Bounded, so a read that never reaches the gate fails the test
        // instead of hanging it.
        tokio::time::timeout(Duration::from_secs(60), gate.wait_until_held(1))
            .await
            .expect("the data read was never held");
        let held = gate.held_details();
        assert_eq!(held.len(), 1);
        assert_eq!(
            store.requests_on("put", &key),
            1,
            "the claim is taken before the held read"
        );
        // Half a renewal cadence past one: exactly one renewal is due, and the
        // merge cannot advance past the gate until it is set.
        claim_clock.set(wall_ms * 1_000_000 + 1_500_000_000);
        assert!(gate.release(held[0].0));
    };
    let (result, ()) = tokio::join!(walk, releaser);
    let text = String::from_utf8(out).expect("utf-8");
    let report = result.expect("walk runs");
    assert_eq!(report.compacted, 2, "{text}");

    let objects = claim_objects(mem.as_ref()).await;
    let claim = objects.get(&key).expect("shard 0 claim");
    assert_eq!(claim.renewed_count, 1, "the slow merge renewed once");
    assert_eq!(claim.state, ClaimState::Completed as i32);
    assert_eq!(
        store.requests_on("put", &key),
        3,
        "create, one renewal, completion"
    );
}

/// A bucket whose claim is taken over mid-merge stops without publishing,
/// prints `outcome=ClaimCancelled` with the checkpoint, and is counted in
/// `claim_cancelled`, not `compacted`; the walk goes on and exits zero.
///
/// Same deterministic shape as the renewal test: while shard 0's first data
/// read is held on the gate, the test overwrites that bucket's claim as another
/// process would and moves the claim clock past the renewal cadence, so the
/// renewal due at the next checkpoint (the merge loop head) loses its CAS. No
/// real time passes. Against a walk that counted a cancelled run as compacted,
/// `compacted` is 2; against one that treated it as an error, the walk exits
/// non-zero. Making `ClaimOptions::clock` ignore the override and return a
/// frozen clock fails the `claim_cancelled` assertion 0 against 1: with no
/// time passing no renewal is due, so the stolen claim is never noticed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_bucket_whose_claim_is_taken_over_mid_merge_is_reported_cancelled() {
    let (mem, wall_ms) = claim_fixture().await;
    let fault = FaultStore::new(mem.clone(), FaultPlan::empty());
    let gate = fault.hold(Op::Get, Some("/l0/0000/".to_string()), Occurrence::Nth(1));
    let store = CountingStore::new(Arc::new(fault));
    let claim_clock = FixedClock::new(wall_ms * 1_000_000);
    let claims = ClaimOptions {
        lease_duration: Some(Duration::from_secs(3)),
        clock: Some(Arc::new(claim_clock.clone()) as Arc<dyn ravel_maintain::Clock>),
        ..claiming()
    };
    let key = claim_key(0, HOUR_OLD);
    let thief = Uuid::new_v4();

    let mut out: Vec<u8> = Vec::new();
    let walk = compact_tenant_to(
        &mut out,
        store.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        Some(SHARDS),
        Some(HOUR_OLD),
        Some(HOUR_OLD),
        false,
        Some(0),
        None,
        None,
        None,
        1,
        now_ns(),
        &claims,
    );
    let thief_writes = async {
        tokio::time::timeout(Duration::from_secs(60), gate.wait_until_held(1))
            .await
            .expect("the data read was never held");
        let stolen = CompactionClaim {
            owner_process_id: thief.as_bytes().to_vec(),
            ..CompactionClaim::default()
        };
        mem.put(&key, stolen.encode_to_vec().into(), PutOptions::default())
            .await
            .expect("overwrite the claim");
        claim_clock.set(wall_ms * 1_000_000 + 1_500_000_000);
        let held = gate.held();
        assert_eq!(held.len(), 1);
        assert!(gate.release(held[0]));
    };
    let (result, ()) = tokio::join!(walk, thief_writes);
    let text = String::from_utf8(out).expect("utf-8");
    let report = result.expect("a cancelled bucket is not a failure");

    assert_eq!(report.claim_cancelled, 1, "{text}");
    assert_eq!(report.compacted, 1, "shard 1's bucket still compacts");
    let lines: Vec<&str> = text.lines().filter(|l| l.starts_with("shard=")).collect();
    assert_eq!(
        lines,
        vec![
            format!("shard=0 hour={HOUR_OLD} outcome=ClaimCancelled checkpoint=merge_loop parts=0"),
            format!("shard=1 hour={HOUR_OLD} outcome=Compacted parts=1 publish=Published"),
        ],
        "{text}"
    );
    assert!(text.contains("\nclaim_cancelled: 1\n"), "summary: {text}");
    assert_eq!(bucket_output(mem.as_ref(), 0, HOUR_OLD).await, (0, 0));
    let objects = claim_objects(mem.as_ref()).await;
    assert_eq!(
        objects.get(&key).expect("claim").owner_process_id,
        thief.as_bytes().to_vec(),
        "the cancelled run left the thief's claim in place"
    );
    // Create, then the renewal that lost its CAS; no completion.
    assert_eq!(store.requests_on("put", &key), 2);
}

/// A bucket below the claim threshold is merged unclaimed: at the default
/// 64 MiB gate, the fixture's few-hundred-byte buckets issue zero requests
/// against the claim key space and all four compact.
#[tokio::test]
async fn a_bucket_below_the_claim_threshold_is_merged_unclaimed() {
    let (mem, _) = claim_fixture().await;
    let store = CountingStore::new(mem.clone());

    let (result, text) = run_claim_walk(store.clone(), false, 1, &ClaimOptions::fresh()).await;
    let report = result.expect("walk runs");
    assert_eq!(report.compacted, 4, "{text}");
    assert_eq!(store.claim_requests(), 0, "no claim request below the gate");
    assert!(
        text.contains("\nclaims: on process_id=") && text.contains(" min_input_bytes=67108864 "),
        "header names the default gate: {text}"
    );
}

/// `compact-bucket` goes through the same claim path: a bucket a foreign
/// process holds is reported as `ClaimSkipped` with the holder and exits zero,
/// `--dry-run` and `--no-claim` issue no claim request, and `--no-claim`
/// compacts the held bucket.
///
/// Against HEAD `compact-bucket` never claims, so the first run compacts the
/// held bucket and prints `outcome: Compacted`.
#[tokio::test]
async fn compact_bucket_skips_a_claimed_bucket_and_no_claim_compacts_it() {
    let (mem, wall_ms) = claim_fixture().await;
    let foreign = claim_as_foreign(mem.as_ref(), CLAIMED_SHARD, HOUR_OLD).await;
    let store = CountingStore::new(mem.clone());

    let run = |dry_run: bool, claims: ClaimOptions| {
        let store: Arc<dyn ObjectStoreBackend> = store.clone();
        async move {
            let mut out: Vec<u8> = Vec::new();
            compact_to(
                &mut out,
                store,
                MEMORY,
                TENANT,
                SignalArg::Logs,
                CLAIMED_SHARD,
                HOUR_OLD,
                dry_run,
                Some(0),
                FixedClock::new(now_ns()),
                &claims,
            )
            .await
            .expect("compact-bucket exits zero");
            String::from_utf8(out).expect("utf-8")
        }
    };

    let text = run(false, claiming()).await;
    assert!(
        text.contains(&format!(
            "\noutcome: ClaimSkipped (another process holds this bucket's compaction claim; \
             nothing was merged) reason=held_by_another holder={foreign} \
             claim_expiry_unix_ms={} retry_after_unix_ms={}\n",
            wall_ms + 300_000,
            wall_ms + 300_001
        )),
        "skip line: {text}"
    );
    assert_eq!(
        bucket_output(mem.as_ref(), CLAIMED_SHARD, HOUR_OLD).await,
        (0, 0)
    );
    assert_eq!(store.claim_requests(), 3, "rejected create, GET, HEAD");

    let text = run(true, claiming()).await;
    assert!(text.contains("\nclaims: off (--dry-run)\n"), "{text}");
    assert_eq!(
        store.claim_requests(),
        3,
        "the dry run adds no claim request"
    );

    let text = run(
        false,
        ClaimOptions {
            no_claim: true,
            ..claiming()
        },
    )
    .await;
    assert!(text.contains("\noutcome: Compacted\n"), "{text}");
    assert_eq!(
        store.claim_requests(),
        3,
        "--no-claim adds no claim request"
    );
    assert_eq!(
        bucket_output(mem.as_ref(), CLAIMED_SHARD, HOUR_OLD).await,
        (1, 1)
    );
}

/// The claim clock the BINARY runs on is the live wall clock, not the frozen
/// instant the walk judges sealing at.
///
/// Renewal cadence and lease expiry are elapsed-time decisions, so a
/// participant built on the walk's `FixedClock` would never renew a merge
/// longer than a third of the lease and would let another maintainer steal the
/// bucket mid-merge. The two claim-timing tests above inject their own clock;
/// this is what pins the default the binary actually gets, and it does so
/// without depending on how much wall time passes: the walk's frozen instant
/// (`now_ns()`, some months away from today) and the real wall clock are hours
/// apart under any scheduling.
///
/// Distinguishing: against `ClaimOptions::clock` returning the walk's
/// `FixedClock`, or any fixed instant, the first assertion fails.
#[test]
fn the_default_claim_clock_is_the_live_wall_clock() {
    let real_ns = ravel_cli::now_ns().expect("wall clock");
    let default_ns = ClaimOptions::fresh().clock().now_ns();
    assert!(
        (default_ns - real_ns).abs() < 60 * 1_000_000_000,
        "the default claim clock reads the real wall clock: {default_ns} against {real_ns}"
    );
    assert!(
        (default_ns - now_ns()).abs() > NS_PER_HOUR,
        "and is not the walk's frozen seal clock: {default_ns} against {}",
        now_ns()
    );

    let injected = FixedClock::new(1_234_567_890);
    let overridden = ClaimOptions {
        clock: Some(Arc::new(injected) as Arc<dyn ravel_maintain::Clock>),
        ..ClaimOptions::fresh()
    };
    assert_eq!(
        overridden.clock().now_ns(),
        1_234_567_890,
        "an injected clock replaces the live one"
    );
}

/// `compact-bucket` prints the `ClaimCancelled` outcome line for a run whose
/// claim is taken over mid-merge, publishes nothing, and exits zero.
///
/// The `compact-tenant` walk's own cancellation case is covered above; this
/// pins the single-bucket command's own line and its exit status, which is the
/// surface an operator scripting `compact-bucket` sees. Deterministic and
/// sleep-free by the same mechanism: the first L0 data GET is held on a gate,
/// the claim is overwritten as another process would, the injected claim clock
/// moves half a cadence past one renewal interval, and the gate is released, so
/// the renewal at the merge-loop checkpoint loses its CAS.
///
/// Distinguishing: against a `compact_to` that fell through to the pipeline's
/// own `CompactionOutcome` instead of returning on the `Cancelled` arm, the run
/// prints `outcome: Compacted` and the `ClaimCancelled` assertion fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_bucket_reports_a_claim_lost_mid_merge_and_exits_zero() {
    let (mem, wall_ms) = claim_fixture().await;
    let fault = FaultStore::new(mem.clone(), FaultPlan::empty());
    let gate = fault.hold(Op::Get, Some("/l0/0000/".to_string()), Occurrence::Nth(1));
    let store = CountingStore::new(Arc::new(fault));
    let claim_clock = FixedClock::new(wall_ms * 1_000_000);
    let claims = ClaimOptions {
        lease_duration: Some(Duration::from_secs(3)),
        clock: Some(Arc::new(claim_clock.clone()) as Arc<dyn ravel_maintain::Clock>),
        ..claiming()
    };
    let key = claim_key(0, HOUR_OLD);
    let thief = Uuid::new_v4();

    let mut out: Vec<u8> = Vec::new();
    let run = compact_to(
        &mut out,
        store.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        0,
        HOUR_OLD,
        false,
        Some(0),
        FixedClock::new(now_ns()),
        &claims,
    );
    let thief_writes = async {
        tokio::time::timeout(Duration::from_secs(60), gate.wait_until_held(1))
            .await
            .expect("the data read was never held");
        assert_eq!(
            store.requests_on("put", &key),
            1,
            "the claim is taken before the held read"
        );
        let stolen = CompactionClaim {
            owner_process_id: thief.as_bytes().to_vec(),
            ..CompactionClaim::default()
        };
        mem.put(&key, stolen.encode_to_vec().into(), PutOptions::default())
            .await
            .expect("overwrite the claim");
        claim_clock.set(wall_ms * 1_000_000 + 1_500_000_000);
        let held = gate.held();
        assert_eq!(held.len(), 1);
        assert!(gate.release(held[0]));
    };
    let (result, ()) = tokio::join!(run, thief_writes);
    result.expect("a cancelled run is not a failure: compact-bucket exits zero");
    let text = String::from_utf8(out).expect("utf-8");

    assert!(
        text.contains(
            "\noutcome: ClaimCancelled (the claim was lost mid-merge; nothing was \
             published) checkpoint=merge_loop parts=0\n"
        ),
        "the cancelled outcome line: {text}"
    );
    assert!(
        !text.contains("\npublish: "),
        "nothing was published: {text}"
    );
    assert_eq!(
        bucket_output(mem.as_ref(), 0, HOUR_OLD).await,
        (0, 0),
        "no L1 part and no compaction record"
    );
    assert_eq!(
        claim_objects(mem.as_ref())
            .await
            .get(&key)
            .expect("claim")
            .owner_process_id,
        thief.as_bytes().to_vec(),
        "the cancelled run left the thief's claim in place"
    );
    // Create, then the renewal that lost its CAS; no completion.
    assert_eq!(store.requests_on("put", &key), 2);
}
