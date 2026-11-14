//! `ravel-cli load --decode-workers` (issue #4210) and the decode and write
//! waits in the load summary (issue #4206).
//!
//! The object comparison drives `ravel_cli::load::run`, the function
//! `main.rs` dispatches `load` to, in-process, since a subprocess against
//! `--store memory` cannot hand its objects back to the test. The summary
//! test runs the binary.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use ravel_cli::load::{
    self, DEFAULT_DECODE_QUEUE_BATCHES, LOAD_REPORT_JSON_PREFIX, PER_BATCH_TARGET_BYTES,
};
use ravel_cli::maintain::SignalArg;
use ravel_ingest::{RlogChunkOrder, RlogGroupTargetBlocks, RlogZstdLevel};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};

const CLOCK_NS: i64 = 1_700_000_000_000_000_000;

/// Row groups of unequal size, so four cursors deal spans of unequal length.
const GROUP_ROWS: [i64; 5] = [3_000, 700, 5_000, 2_300, 4_000];

const BATCH_ROWS: usize = 1_000;

const MAPPING: &str = r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "row"
column = "row"
type = "i64"
"#;

/// Every row distinguishable by its `row` attribute and body, spread over
/// eight services.
fn write_fixture(path: &Path) {
    let mut groups = Vec::new();
    let mut first = 0i64;
    for rows in GROUP_ROWS {
        let row: Vec<i64> = (first..first + rows).collect();
        groups.push(
            RecordBatch::try_from_iter(vec![
                (
                    "ts",
                    Arc::new(Int64Array::from_iter_values(
                        row.iter().map(|r| CLOCK_NS + r * 1_000),
                    )) as ArrayRef,
                ),
                (
                    "body",
                    Arc::new(StringArray::from_iter_values(
                        row.iter().map(|r| format!("request served id={r:06}")),
                    )) as ArrayRef,
                ),
                (
                    "svc",
                    Arc::new(StringArray::from_iter_values(
                        row.iter().map(|r| format!("svc-{}", r % 8)),
                    )) as ArrayRef,
                ),
                ("row", Arc::new(Int64Array::from(row)) as ArrayRef),
            ])
            .expect("batch"),
        );
        first += rows;
    }
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, groups[0].schema(), None).expect("arrow writer");
    for group in &groups {
        writer.write(group).expect("write row group");
        writer.flush().expect("flush row group");
    }
    writer.close().expect("close writer");
}

fn fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("rows.parquet");
    let mapping = dir.path().join("mapping.toml");
    write_fixture(&pq);
    std::fs::write(&mapping, MAPPING).expect("write mapping");
    (dir, pq, mapping)
}

/// One hash per RLOG object in sorted order, and the rows the objects hold.
/// The hash covers every section's stored bytes and the footer's content
/// fields, leaving out the per-run random writer id the footer also carries.
async fn rlog_objects(store: &Arc<dyn ObjectStoreBackend>) -> (Vec<[u8; 32]>, u64) {
    let mut hashes = Vec::new();
    let mut rows = 0;
    for meta in list_all(store.as_ref(), "").await.expect("list") {
        let data = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get")
            .data;
        let Ok(footer) = ravel_logseg::footer::open(&data) else {
            continue;
        };
        rows += footer.record_count;
        let mut hasher = blake3::Hasher::new();
        for field in [
            u64::from(footer.shard),
            footer.min_ts_ns as u64,
            footer.max_ts_ns as u64,
            footer.min_observed_ts_ns as u64,
            footer.max_observed_ts_ns as u64,
            footer.record_count,
            footer.block_count,
            footer.stream_count,
            footer.sections.len() as u64,
        ] {
            hasher.update(&field.to_le_bytes());
        }
        for section in &footer.sections {
            hasher.update(&section.kind.to_le_bytes());
            hasher.update(&[section.comp]);
            hasher.update(&section.uncomp_len.to_le_bytes());
            let start = section.offset as usize;
            hasher.update(&data[start..start + section.len as usize]);
        }
        hashes.push(*hasher.finalize().as_bytes());
    }
    hashes.sort_unstable();
    (hashes, rows)
}

async fn load_with_workers(
    pq: &Path,
    mapping: &Path,
    decode_workers: usize,
) -> (Vec<[u8; 32]>, u64) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load::run(
        Arc::clone(&store),
        pq,
        "acme",
        mapping,
        SignalArg::Logs,
        2,
        BATCH_ROWS,
        0,
        Some(4),
        Some(1),
        1,
        DEFAULT_DECODE_QUEUE_BATCHES,
        Some(decode_workers),
        Some(PER_BATCH_TARGET_BYTES),
        None,
        None,
        RlogZstdLevel::DEFAULT,
        RlogGroupTargetBlocks::DEFAULT,
        RlogChunkOrder::DEFAULT,
        None,
        false,
        &ravel_cli::ipcrypt::IpcryptPolicy::empty(),
        None,
        CLOCK_NS,
    )
    .await
    .expect("load succeeds");
    rlog_objects(&store).await
}

/// The same file loaded with one decode worker and with four writes the same
/// objects: the same count, the same rows, and every object's content hash
/// equal.
///
/// Prove-the-test: emit each built batch as it arrives
/// (`ready.pop_first()` for `ready.remove(&self.next_emit)` in
/// `DecodeCoordinator::run`) and the four-worker load writes fewer rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_and_four_decode_workers_write_the_same_objects() {
    let (_dir, pq, mapping) = fixture();
    let (one, one_rows) = load_with_workers(&pq, &mapping, 1).await;
    let (four, four_rows) = load_with_workers(&pq, &mapping, 4).await;
    let total: i64 = GROUP_ROWS.iter().sum();
    assert_eq!(
        one_rows, total as u64,
        "the one-worker load wrote every row"
    );
    assert_eq!(four_rows, one_rows, "both loads wrote the same rows");
    assert!(one.len() > 2, "several objects: {}", one.len());
    assert_eq!(four.len(), one.len(), "both loads wrote the same objects");
    assert_eq!(four, one, "every object's content hash is the same");
}

fn cli_load(pq: &Path, mapping: &Path, decode_workers: usize) -> String {
    let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_ravel-cli"));
    for (key, _) in std::env::vars() {
        if key.starts_with("RAVEL_") {
            cmd.env_remove(key);
        }
    }
    let out = cmd
        .args([
            "--store",
            "memory",
            "--tenant-hash-unkeyed",
            "load",
            "--parquet",
        ])
        .arg(pq)
        .args(["--tenant", "acme", "--mapping"])
        .arg(mapping)
        .args(["--shards", "2", "--read-cursors", "4", "--batch-rows"])
        .arg(BATCH_ROWS.to_string())
        .arg("--decode-workers")
        .arg(decode_workers.to_string())
        .output()
        .expect("ravel-cli runs");
    let stdout = String::from_utf8(out.stdout).expect("utf-8 stdout");
    assert!(
        out.status.success(),
        "load failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    stdout
}

/// The seconds on the one summary line labelled `label`.
fn summary_seconds(stdout: &str, label: &str) -> f64 {
    let lines: Vec<&str> = stdout
        .lines()
        .filter(|l| l.trim_start().starts_with(label))
        .collect();
    assert_eq!(lines.len(), 1, "one {label} line in:\n{stdout}");
    let (_, value) = lines[0].split_once(':').expect("a labelled line");
    let secs = value
        .trim()
        .strip_suffix('s')
        .unwrap_or_else(|| panic!("{label} is in seconds: {}", lines[0]))
        .parse::<f64>()
        .unwrap_or_else(|e| panic!("{label} parses ({e}): {}", lines[0]));
    assert!(secs >= 0.0, "{label} is non-negative: {secs}");
    secs
}

/// The summary prints `decode_wait` and `write_wait` once each, in seconds,
/// and the report JSON carries both in whole milliseconds, at either worker
/// count.
///
/// Prove-the-test: remove the two `println!`s from `print_summary` and the
/// one-line assertions fail; drop the fields from `LoadReport::layout_report`
/// and the JSON assertions fail.
#[test]
fn the_summary_prints_the_decode_and_write_waits_once_each() {
    let (_dir, pq, mapping) = fixture();
    for workers in [1, 4] {
        let stdout = cli_load(&pq, &mapping, workers);
        let decode_wait = summary_seconds(&stdout, "decode_wait");
        let write_wait = summary_seconds(&stdout, "write_wait");
        let json_lines: Vec<&str> = stdout
            .lines()
            .filter_map(|l| l.strip_prefix(LOAD_REPORT_JSON_PREFIX))
            .collect();
        assert_eq!(json_lines.len(), 1, "one report json line in:\n{stdout}");
        let json: serde_json::Value = serde_json::from_str(json_lines[0]).expect("report json");
        for (key, secs) in [
            ("decode_wait_ms", decode_wait),
            ("write_wait_ms", write_wait),
        ] {
            let ms = json[key]
                .as_u64()
                .unwrap_or_else(|| panic!("{key} is a whole number in {}", json_lines[0]));
            // The summary rounds to the millisecond and the JSON truncates.
            assert!(
                (ms as f64 / 1_000.0 - secs).abs() <= 0.001 + 1e-9,
                "{key} {ms} agrees with the summary's {secs}s"
            );
        }
    }
}
