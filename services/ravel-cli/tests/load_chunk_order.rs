//! `ravel-cli load --chunk-order` (ADR-3217 decision 8, issue #3251).
//!
//! Drives `ravel_cli::load::run`, the function `main.rs` dispatches `load` to,
//! in-process, and reads the written object's FIELD_DIR: the flag reaches the
//! router `build_router` builds, and so the RLOG writer of every flush. The
//! parse test in `main.rs` stops at clap and the shard test in `ravel-ingest`
//! starts at `IngestConfig`; this is the path between them.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use ravel_cli::load::{self, DEFAULT_DECODE_QUEUE_BATCHES, PER_BATCH_TARGET_BYTES};
use ravel_cli::maintain::SignalArg;
use ravel_ingest::{RlogChunkOrder, RlogGroupTargetBlocks, RlogZstdLevel};
use ravel_logseg::field_dir::FieldDir;
use ravel_logseg::footer::{self, kind};
use ravel_logseg::{RlogConfig, read_section};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};

const ROWS: usize = 2_000;

/// Three per-record attributes whose alphabetical order is age, counter,
/// user.
const MAPPING: &str = r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[attribute]]
key = "age"
column = "age"
type = "i64"

[[attribute]]
key = "counter"
column = "counter"
type = "i64"

[[attribute]]
key = "user"
column = "user"
type = "str"
"#;

fn write_fixture(path: &Path, base_ts: i64) {
    let ts: Vec<i64> = (0..ROWS as i64).map(|i| base_ts + i * 1_000_000).collect();
    let body: Vec<String> = (0..ROWS).map(|i| format!("request {i} served")).collect();
    let age: Vec<i64> = (0..ROWS as i64).map(|i| i % 90).collect();
    let counter: Vec<i64> = (0..ROWS as i64).map(|i| i % 7).collect();
    let user: Vec<String> = (0..ROWS).map(|i| format!("user-{}", i % 13)).collect();
    let batch = RecordBatch::try_from_iter(vec![
        ("ts", Arc::new(Int64Array::from(ts)) as ArrayRef),
        ("body", Arc::new(StringArray::from(body)) as ArrayRef),
        ("age", Arc::new(Int64Array::from(age)) as ArrayRef),
        ("counter", Arc::new(Int64Array::from(counter)) as ArrayRef),
        ("user", Arc::new(StringArray::from(user)) as ArrayRef),
    ])
    .expect("batch");
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
}

/// The `(name, column_id)` of every FIELD_DIR entry of the one RLOG object a
/// one-shard, one-batch load of the fixture writes, in FIELD_DIR order.
async fn field_dir_ids_at(
    chunk_order: RlogChunkOrder,
    parquet: &Path,
    mapping: &Path,
    now_ns: i64,
) -> Vec<(String, u32)> {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load::run(
        Arc::clone(&store),
        parquet,
        "acme",
        mapping,
        SignalArg::Logs,
        1,
        ROWS,
        0,
        None,
        Some(1),
        1,
        DEFAULT_DECODE_QUEUE_BATCHES,
        Some(PER_BATCH_TARGET_BYTES),
        None,
        None,
        RlogZstdLevel::DEFAULT,
        RlogGroupTargetBlocks::DEFAULT,
        chunk_order,
        None,
        false,
        &ravel_cli::ipcrypt::IpcryptPolicy::empty(),
        now_ns,
    )
    .await
    .expect("load succeeds");
    let mut objects = Vec::new();
    for meta in list_all(store.as_ref(), "").await.expect("list") {
        let data = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get")
            .data;
        if footer::open(&data).is_ok() {
            objects.push(data);
        }
    }
    assert_eq!(objects.len(), 1, "one shard, one batch, one object");
    let data = &objects[0];
    let f = footer::open(data).expect("footer");
    let desc = f.section(kind::FIELD_DIR).expect("FIELD_DIR");
    let raw = read_section(data, desc, &RlogConfig::default()).expect("read FIELD_DIR");
    FieldDir::decode(&raw, 10_000)
        .expect("decode FIELD_DIR")
        .entries()
        .iter()
        .map(|e| (e.name.clone(), e.column_id))
        .collect()
}

fn named(pairs: &[(&str, u32)]) -> Vec<(String, u32)> {
    pairs.iter().map(|(n, id)| (n.to_string(), *id)).collect()
}

/// The loader's `chunk_order` reaches the written object: `user,age` gives
/// those names ids 10 and 11 in that order and `counter` follows at 12; the
/// default gives the alphabetical ids. FIELD_DIR itself stays sorted by name
/// on both, so the entry order is the same and only the ids move.
///
/// Non-vacuity: passing `RlogChunkOrder::DEFAULT` in place of the flag at any
/// of `main.rs`'s `unwrap_or_default()`, `load/cli.rs`'s `chunk_order` or
/// `load/logs.rs`'s `rlog_chunk_order: chunk_order` writes the ordered load
/// with the alphabetical ids and the first assertion fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_chunk_order_reaches_the_written_column_ids() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet = dir.path().join("logs.parquet");
    let mapping = dir.path().join("mapping.toml");
    std::fs::write(&mapping, MAPPING).expect("write mapping");
    // Pinned so the written corpus is the same on every run.
    let now_ns: i64 = 1_790_000_000_000_000_000;
    write_fixture(&parquet, now_ns - 60_000_000_000);

    let order =
        RlogChunkOrder::new(vec!["user".to_string(), "age".to_string()]).expect("valid order");
    assert_eq!(
        field_dir_ids_at(order, &parquet, &mapping, now_ns).await,
        named(&[("age", 11), ("counter", 12), ("user", 10)]),
        "--chunk-order user,age: user 10, age 11, the rest alphabetical from 12"
    );
    assert_eq!(
        field_dir_ids_at(RlogChunkOrder::DEFAULT, &parquet, &mapping, now_ns).await,
        named(&[("age", 10), ("counter", 11), ("user", 12)]),
        "the default: alphabetical ids"
    );
}
