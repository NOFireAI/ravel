//! `ravel-cli load --group-target-blocks` (ADR-3217 decision 5, issue #3250).
//!
//! Drives `ravel_cli::load::run`, the function `main.rs` dispatches `load` to,
//! in-process, and reads the written object's PAGE_DIR: the flag reaches the
//! router `build_router` builds, and so the RLOG writer of every flush.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use ravel_cli::load::{self, DEFAULT_DECODE_QUEUE_BATCHES, PER_BATCH_TARGET_BYTES};
use ravel_cli::maintain::SignalArg;
use ravel_ingest::{RlogChunkOrder, RlogGroupTargetBlocks, RlogZstdLevel};
use ravel_logseg::footer::{self, kind};
use ravel_logseg::page_dir::PageDir;
use ravel_logseg::{RlogConfig, read_section};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};

/// Three blocks at the writer's 8,192-record block target: 8,192, 8,192 and
/// 3,616 records.
const ROWS: usize = 20_000;

const MAPPING: &str = r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"
"#;

fn write_fixture(path: &Path, base_ts: i64) {
    let ts: Vec<i64> = (0..ROWS as i64).map(|i| base_ts + i * 1_000_000).collect();
    let body: Vec<String> = (0..ROWS).map(|i| format!("request {i} served")).collect();
    let svc: Vec<&str> = vec!["api"; ROWS];
    let batch = RecordBatch::try_from_iter(vec![
        ("ts", Arc::new(Int64Array::from(ts)) as ArrayRef),
        ("body", Arc::new(StringArray::from(body)) as ArrayRef),
        ("svc", Arc::new(StringArray::from(svc)) as ArrayRef),
    ])
    .expect("batch");
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
}

/// The `(first_block, block_count)` of every PAGE_DIR row group of the one RLOG
/// object a one-shard, one-batch load of the fixture writes.
async fn row_groups_at(
    group_target_blocks: RlogGroupTargetBlocks,
    parquet: &Path,
    mapping: &Path,
    now_ns: i64,
) -> Vec<(u32, u32)> {
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
        group_target_blocks,
        RlogChunkOrder::DEFAULT,
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
    let desc = f.section(kind::PAGE_DIR).expect("PAGE_DIR");
    let raw = read_section(data, desc, &RlogConfig::default()).expect("read PAGE_DIR");
    PageDir::decode(&raw)
        .expect("decode PAGE_DIR")
        .groups
        .iter()
        .map(|g| (g.first_block, g.block_count))
        .collect()
}

/// The loader's `group_target_blocks` reaches the written object: at 1 every
/// block is its own row group, at the default 32 the three blocks are one.
///
/// Non-vacuity: dropping `rlog_group_target_blocks` from the `IngestConfig`
/// `build_router` builds writes the 1-block load as one group of three.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn load_group_target_blocks_reaches_the_written_row_groups() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet = dir.path().join("logs.parquet");
    let mapping = dir.path().join("mapping.toml");
    std::fs::write(&mapping, MAPPING).expect("write mapping");
    // Pinned so the written corpus is the same on every run.
    let now_ns: i64 = 1_790_000_000_000_000_000;
    write_fixture(&parquet, now_ns - 60_000_000_000);

    let one = RlogGroupTargetBlocks::new(1).expect("nonzero");
    assert_eq!(
        row_groups_at(one, &parquet, &mapping, now_ns).await,
        vec![(0, 1), (1, 1), (2, 1)],
        "--group-target-blocks 1: one row group per block"
    );
    assert_eq!(
        row_groups_at(RlogGroupTargetBlocks::DEFAULT, &parquet, &mapping, now_ns).await,
        vec![(0, 3)],
        "the default: every block in one row group"
    );
}
