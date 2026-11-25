//! `catalog inspect --entries` prints each snapshot entry's data object key.
//! The keys are derived from the entry's identity fields, so this checks them
//! against the store a real logs tenant leaves behind: L0 `.rlog` objects
//! seeded as the ingest log shard writes them, one sealed hour compacted to
//! L1 parts by `maintain compact-tenant`, then a `catalog fold`. The printed
//! keys must equal, as a set, the data object keys the catalog resolves for
//! that snapshot, and each must name an object the store holds.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::catalog;
use ravel_cli::maintain::{ClaimOptions, ScratchOptions, SignalArg, compact_tenant};
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{AttrValue, LogRecord, LogStreamId, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_types::{Signal, TenantHash, TenantId, TimeRange};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
const TENANT: &str = "inspect-entry-keys";
const HOUR_OLD: u32 = 500_000;
const SHARDS: u32 = 2;
const EPOCH: u64 = 1;
const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

/// Ten minutes past the end of the newer hour: under the default seal margin
/// only `HOUR_OLD` is sealed, so compaction writes L1 parts for it alone.
fn compact_now_ns() -> i64 {
    (i64::from(HOUR_OLD) + 2) * NS_PER_HOUR + 10 * 60 * 1_000_000_000
}

/// Late enough that the fold seals both hours, so the snapshot holds the L1
/// parts of `HOUR_OLD` and the L0 objects of `HOUR_OLD + 1`.
fn fold_now_ns() -> i64 {
    (i64::from(HOUR_OLD) + 6) * NS_PER_HOUR
}

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn log_record(stream: u8, ts_ns: i64) -> LogRecord {
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

/// One L0 `.rlog` object and its commit record in `(shard, hour)`, as the
/// ingest log shard writes them.
async fn seed_l0(store: &dyn ObjectStoreBackend, shard: u32, hour: u32, seq: u64) {
    let th = tenant_hash();
    let writer_id = Uuid::new_v4();
    let base_ns = i64::from(hour) * NS_PER_HOUR;
    let records: Vec<LogRecord> = (0..4)
        .map(|i| {
            log_record(
                u8::try_from(i % 2).unwrap(),
                base_ns + i64::from(i) * 1_000_000 + i64::try_from(seq).unwrap(),
            )
        })
        .collect();
    let mut writer = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: th.0,
            shard,
            writer_id: writer_id.into_bytes(),
            writer_epoch: EPOCH,
            writer_seq: seq,
        },
    );
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
        min_event_ts_ns: records.iter().map(|r| r.ts_ns).min().expect("nonempty"),
        max_event_ts_ns: records.iter().map(|r| r.ts_ns).max().expect("nonempty"),
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

/// Every `entry[i]` line's `key=` value (the rest of the line), mapped to the
/// entry's level.
fn printed_keys(report: &str) -> BTreeMap<String, String> {
    report
        .lines()
        .filter(|line| line.trim_start().starts_with("entry["))
        .map(|line| {
            let level = line
                .split(' ')
                .find_map(|field| field.strip_prefix("level="))
                .unwrap_or_else(|| panic!("no level in {line:?}"))
                .to_string();
            let key = line
                .split_once(" key=")
                .unwrap_or_else(|| panic!("no key in {line:?}"))
                .1
                .to_string();
            (key, level)
        })
        .collect()
}

#[tokio::test]
async fn inspect_entries_prints_the_keys_the_catalog_resolves_for_a_logs_snapshot() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    for shard in 0..SHARDS {
        for hour in [HOUR_OLD, HOUR_OLD + 1] {
            for seq in 1..=2u64 {
                seed_l0(store.as_ref(), shard, hour, seq).await;
            }
        }
    }

    let compacted = compact_tenant(
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
        None,
        compact_now_ns(),
        &ClaimOptions::fresh(),
        &ScratchOptions::default(),
    )
    .await
    .expect("compact-tenant runs");
    assert_eq!(compacted.compacted, 2, "the sealed hour of each shard");
    assert_eq!(compacted.parts_written, 2, "one L1 part per bucket");

    catalog::fold(
        store.clone(),
        MEMORY,
        TENANT,
        SHARDS,
        SignalArg::Logs,
        None,
        fold_now_ns(),
        false,
    )
    .await
    .expect("fold succeeds");

    let mut report = String::new();
    catalog::render_inspect_with_entries(
        store.clone(),
        MEMORY,
        TENANT,
        SignalArg::Logs,
        true,
        &mut report,
    )
    .await
    .expect("inspect renders");

    let printed = printed_keys(&report);
    let levels: Vec<&str> = printed.values().map(String::as_str).collect();
    assert_eq!(
        (
            levels.iter().filter(|l| **l == "0").count(),
            levels.iter().filter(|l| **l == "1").count(),
            printed.len(),
        ),
        (4, 2, 6),
        "the newer hour's four L0 objects and the older hour's two L1 parts:\n{report}"
    );

    let resolved: BTreeSet<String> = Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count: SHARDS,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog")
    .resolve(
        &tenant_hash(),
        Signal::Logs,
        TimeRange {
            start_ns: i64::from(HOUR_OLD) * NS_PER_HOUR,
            end_ns: (i64::from(HOUR_OLD) + 2) * NS_PER_HOUR,
        },
        &[],
        fold_now_ns(),
    )
    .await
    .expect("resolve")
    .segments
    .into_iter()
    .map(|segment| segment.data_object_key)
    .collect();
    let printed_set: BTreeSet<String> = printed.keys().cloned().collect();
    assert_eq!(printed_set, resolved, "{report}");

    for key in &printed_set {
        store
            .get(key, GetRange::Full)
            .await
            .unwrap_or_else(|err| panic!("printed key {key} is not in the store: {err}"));
    }
}
