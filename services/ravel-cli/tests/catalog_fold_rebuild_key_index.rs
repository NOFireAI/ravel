//! `ravel-cli catalog fold --rebuild-key-index` (ADR-2707 decision 4,
//! "Lifecycle"): on a spans tenant folded with `--key-index-leaves` off, the
//! rebuild attaches a leaf to the part without re-encoding it and prints every
//! report field, `largest_unindexed_part_entries` first after the store and signal.
//!
//! Driven in-process against one `MemoryStore` per case, for the reason
//! `tests/catalog_signal.rs` gives.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use bytes::Bytes;
use ravel_catalog::KeyIndexRebuildReport;
use ravel_cli::catalog;
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_proto::catalog::v1::SnapshotHead;
use ravel_rspan::{ObjectIdentity, RspanConfig, RspanWriter, SpanRecord, StatusCode};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

const MEMORY: StoreSelection = StoreSelection::explicit(StoreKind::Memory);

const TENANT: &str = "cli-fold-rebuild-key-index";

/// Old enough that the default seal margin seals its hour whatever minute the
/// test runs in (the same constant as `tests/catalog_signal.rs`).
const SEALED_AGE_NS: i64 = 3 * NS_PER_HOUR;

/// Publishes one RSPAN object, which always carries a KEY_IDX section over
/// `trace_id`, with one span per trace id. Returns the object's size and key.
async fn publish_spans(store: &MemoryStore, now_ns: i64) -> (u64, String) {
    let tenant_hash = TenantId::new(TENANT).hash();
    let writer_id = Uuid::from_u128(1);
    let mut writer = RspanWriter::new(
        RspanConfig::default(),
        ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: 0,
            writer_id: *writer_id.as_bytes(),
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    let base_ts = now_ns - SEALED_AGE_NS;
    for i in 0..3u8 {
        writer.push(SpanRecord {
            trace_id: [i + 1; 16],
            span_id: [i + 1; 8],
            parent_span_id: None,
            name: "op".to_string(),
            start_ts_ns: base_ts + i64::from(i),
            end_ts_ns: base_ts + i64::from(i) + 10,
            status_code: StatusCode::Unset,
            status_message: None,
            attrs: Vec::new(),
        });
    }
    let bytes = Bytes::from(writer.finish().expect("finish"));
    let size = bytes.len() as u64;
    let commit = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Spans,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: size,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: 3,
        series_count: 1,
        min_event_ts_ns: base_ts,
        max_event_ts_ns: base_ts + 100,
        min_ingest_ts_ns: base_ts,
        max_ingest_ts_ns: base_ts + 100,
        segment_format_version: 1,
        created_unix_ns: base_ts,
        ingest_hour_bucket: u32::try_from(base_ts.div_euclid(NS_PER_HOUR)).expect("fits u32"),
    })
    .expect("valid record");
    let data_key = keys::reconstruct_data_key(&commit).expect("data key");
    publish::put_data_object(store, &data_key, bytes)
        .await
        .expect("put data object");
    publish::publish(store, &commit, &RetryPolicy::default())
        .await
        .expect("publish");
    (size, data_key)
}

fn catalog_prefix() -> String {
    format!(
        "t/{}/catalog/{}/",
        TenantId::new(TENANT).hash().to_hex(),
        Signal::Spans.key_prefix()
    )
}

async fn catalog_keys(store: &MemoryStore) -> BTreeSet<String> {
    list_all(store, &catalog_prefix())
        .await
        .expect("list the catalog prefix")
        .into_iter()
        .map(|meta| meta.key)
        .collect()
}

async fn read_head(store: &MemoryStore) -> SnapshotHead {
    let key = format!("{}HEAD", catalog_prefix());
    let got = store.get(&key, GetRange::Full).await.expect("HEAD");
    ravel_catalog::decode_head(&got.data).expect("HEAD decodes")
}

/// A spans tenant folded with the leaf build off, so its one part has no
/// `key_index` ref. Returns the store, the object size and the object key.
async fn folded_without_leaves() -> (Arc<MemoryStore>, u64, String) {
    let now = ravel_cli::now_ns().expect("system clock readable");
    let store = Arc::new(MemoryStore::new());
    let (object_size, data_key) = publish_spans(&store, now).await;
    let (report, _printed) = catalog::fold_with_writers_stopped(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        TENANT,
        1,
        SignalArg::Spans,
        None,
        false,
        false,
        now,
        true,
    )
    .await
    .expect("fold succeeds");
    assert_eq!(report.entry_count, 1, "the spans object is folded");
    assert_eq!(report.key_index_leaves_written, 0, "folded with leaves off");
    (store, object_size, data_key)
}

async fn rebuild(
    store: &Arc<MemoryStore>,
    field: Option<&str>,
    json: bool,
) -> anyhow::Result<(KeyIndexRebuildReport, String)> {
    rebuild_on(
        Arc::clone(store) as Arc<dyn ObjectStoreBackend>,
        SignalArg::Spans,
        field,
        json,
    )
    .await
}

async fn rebuild_on(
    store: Arc<dyn ObjectStoreBackend>,
    signal: SignalArg,
    field: Option<&str>,
    json: bool,
) -> anyhow::Result<(KeyIndexRebuildReport, String)> {
    catalog::rebuild_key_index(
        store,
        MEMORY,
        TENANT,
        1,
        signal,
        field.map(str::to_string),
        ravel_catalog::DEFAULT_KEY_INDEX_REBUILD_BATCH_ENTRIES,
        json,
    )
    .await
}

/// The rebuild attaches one `trace_id` leaf to the stripped part, writes the
/// leaf and HEAD and nothing else under the catalog prefix, leaves every other
/// field of the part ref unchanged, and prints each report field with its
/// value, `largest_unindexed_part_entries` first after the store and signal. One entry costs
/// two ranged GETs: the object is under the 8 KiB footer suffix, so the
/// suffix GET returns all of it and the section bytes read exceed its size.
#[tokio::test]
async fn rebuild_attaches_a_leaf_and_prints_every_report_field() {
    let (store, object_size, _) = folded_without_leaves().await;
    let before_head = read_head(&store).await;
    assert!(before_head.parts[0].key_index.is_empty());
    let before_keys = catalog_keys(&store).await;

    let (report, printed) = rebuild(&store, None, false).await.expect("rebuild");
    let head = read_head(&store).await;
    let part = &head.parts[0];
    assert_eq!(part.key_index.len(), 1);
    let leaf = &part.key_index[0];
    assert_eq!(leaf.field, "trace_id");
    assert_eq!(leaf.part_blake3, part.blake3);
    let mut stripped = head.clone();
    stripped.parts[0].key_index.clear();
    assert_eq!(stripped, before_head, "only key_index changed on HEAD");
    let added: BTreeSet<String> = catalog_keys(&store)
        .await
        .difference(&before_keys)
        .cloned()
        .collect();
    assert_eq!(added, BTreeSet::from([leaf.key.clone()]), "one new object");

    assert!(
        report.section_bytes_read > object_size,
        "{} > {object_size}",
        report.section_bytes_read
    );
    let expected = format!(
        "store: memory\n\
         signal: spans\n\
         largest_unindexed_part_entries: 1\n\
         fields: trace_id\n\
         parts_unindexed: 1\n\
         parts_rebuilt: 1\n\
         parts_deferred: 0\n\
         leaves_written: 1\n\
         leaf_bytes_written: {}\n\
         section_bytes_read: {}\n\
         ranged_gets: 2\n\
         uncovered_entries: 0\n\
         attempt_count: 1\n\
         attempt 1: parts=1 part_gets=1 ranged_gets=2 leaf_puts=1 head_cas_tries=1 \
         parts_published=1\n",
        leaf.size, report.section_bytes_read
    );
    assert_eq!(printed, expected);

    // A rerun finds every part indexed: nothing to read, nothing published.
    let (rerun, json) = rebuild(&store, Some("trace_id"), true)
        .await
        .expect("rerun");
    assert_eq!(rerun.parts_unindexed, 0);
    assert_eq!(rerun.attempt_count, 0);
    assert_eq!(read_head(&store).await, head);
    let value: serde_json::Value = serde_json::from_str(&json).expect("one JSON document");
    let mut want: BTreeSet<String> = serde_json::to_value(&rerun)
        .expect("serialize")
        .as_object()
        .expect("object")
        .keys()
        .cloned()
        .collect();
    want.extend(["store".to_string(), "signal".to_string()]);
    let got: BTreeSet<String> = value.as_object().expect("object").keys().cloned().collect();
    assert_eq!(got, want, "--json carries every report field");
    assert_eq!(value["signal"], "spans");
    assert_eq!(value["largest_unindexed_part_entries"], 0);
}

/// `--field` naming a field the tenant does not declare is refused with the
/// field and the declared set named, and publishes nothing.
#[tokio::test]
async fn rebuild_refuses_an_undeclared_field() {
    let (store, _, _) = folded_without_leaves().await;
    let before = catalog_keys(&store).await;
    let err = rebuild(&store, Some("user_id"), false)
        .await
        .expect_err("user_id is not a spans key-index field");
    let message = err.to_string();
    assert!(message.contains("\"user_id\""), "{message}");
    assert!(message.contains("trace_id"), "{message}");
    assert_eq!(catalog_keys(&store).await, before);
}

/// A rebuild whose every HEAD CAS is lost fails, and the text the command
/// prints for it is the report so far: the leaf it wrote, the eight CAS tries
/// the attempt spent, and no part published.
#[tokio::test]
async fn rebuild_that_exhausts_its_cas_tries_prints_the_report_so_far() {
    let (store, _, _) = folded_without_leaves().await;
    let faulty = Arc::new(FaultStore::new(
        Arc::clone(&store),
        FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::FailedConditionalWrite).with_key_contains("/HEAD"),
        ),
    ));
    let (result, printed) = catalog::rebuild_key_index_outcome(
        faulty as Arc<dyn ObjectStoreBackend>,
        MEMORY,
        TENANT,
        1,
        SignalArg::Spans,
        None,
        ravel_catalog::DEFAULT_KEY_INDEX_REBUILD_BATCH_ENTRIES,
        false,
    )
    .await
    .expect("the rebuild runs");
    let err = result.expect_err("every HEAD CAS is lost");
    assert!(
        matches!(
            err,
            ravel_catalog::KeyIndexRebuildError::CasRetriesExhausted {
                attempts: 8,
                batch: 1,
                ..
            }
        ),
        "{err:?}"
    );
    assert!(printed.contains("parts_rebuilt: 0\n"), "{printed}");
    assert!(printed.contains("leaves_written: 1\n"), "{printed}");
    assert!(
        printed.contains(
            "attempt 1: parts=1 part_gets=1 ranged_gets=2 leaf_puts=1 head_cas_tries=8 \
             parts_published=0\n"
        ),
        "{printed}"
    );
    assert!(read_head(&store).await.parts[0].key_index.is_empty());
}

/// A ranged GET that fails for the part's one entry defers the part: the
/// command prints `parts_deferred: 1` and publishes no ref, and a rerun once
/// the store answers attaches the leaf.
#[tokio::test]
async fn rebuild_prints_a_deferred_part_and_a_rerun_attaches_it() {
    let (store, _, data_key) = folded_without_leaves().await;
    let faulty = Arc::new(FaultStore::new(
        Arc::clone(&store),
        FaultPlan::empty()
            .with_rule(Rule::new(Op::Get, ScriptedFault::Timeout).with_key_contains(data_key)),
    ));
    let (report, printed) = rebuild_on(
        Arc::clone(&faulty) as Arc<dyn ObjectStoreBackend>,
        SignalArg::Spans,
        None,
        false,
    )
    .await
    .expect("a deferred part is not an error");
    assert_eq!(faulty.fault_count(Op::Get, FaultKind::Timeout), 1);
    assert_eq!((report.parts_rebuilt, report.parts_deferred), (0, 1));
    assert!(printed.contains("parts_rebuilt: 0\n"), "{printed}");
    assert!(printed.contains("parts_deferred: 1\n"), "{printed}");
    assert!(read_head(&store).await.parts[0].key_index.is_empty());

    let (rerun, printed) = rebuild(&store, None, false).await.expect("rerun");
    assert_eq!((rerun.parts_rebuilt, rerun.parts_deferred), (1, 0));
    assert!(printed.contains("parts_deferred: 0\n"), "{printed}");
    assert_eq!(read_head(&store).await.parts[0].key_index.len(), 1);
}

/// A logs tenant that declares no key-index field is refused with the tenant
/// named, and nothing is written.
#[tokio::test]
async fn rebuild_refuses_a_logs_tenant_declaring_no_key_index_field() {
    let (store, _, _) = folded_without_leaves().await;
    let before = catalog_keys(&store).await;
    let err = rebuild_on(
        Arc::clone(&store) as Arc<dyn ObjectStoreBackend>,
        SignalArg::Logs,
        None,
        false,
    )
    .await
    .expect_err("no logs key-index field is declared");
    let message = format!("{err:#}");
    assert!(message.contains(TENANT), "{message}");
    assert!(
        message.contains(&TenantId::new(TENANT).hash().to_hex()),
        "{message}"
    );
    assert!(message.contains("declares no key-index field"), "{message}");
    assert_eq!(catalog_keys(&store).await, before);
}
