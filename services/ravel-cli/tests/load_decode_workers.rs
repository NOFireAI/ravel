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
    self, DEFAULT_DECODE_QUEUE_BATCHES, DEFAULT_MAX_FLUSH_DELAY, LOAD_REPORT_JSON_PREFIX,
    PER_BATCH_TARGET_BYTES,
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

/// Rows whose bodies are [`SLOW_BODY_BYTES`] longer. Four cursors deal a
/// quarter of each batch from each region, so these are the first cursor's
/// share of batch 1, which then takes longer to build than the batches after
/// it.
const SLOW_ROWS: std::ops::Range<i64> = 250..500;

const SLOW_BODY_BYTES: usize = 60 * 1024;

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
                    Arc::new(StringArray::from_iter_values(row.iter().map(|&r| {
                        if SLOW_ROWS.contains(&r) {
                            format!("request served id={r:06} {}", "x".repeat(SLOW_BODY_BYTES))
                        } else {
                            format!("request served id={r:06}")
                        }
                    }))) as ArrayRef,
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

/// Where a logs data object sits in its shard's write order, read off its key
/// (`.../l0/<shard>/<writer_id>.<epoch>.<seq>.<hash>.rseg`): the shard and
/// the writer's sequence number. The writer id is random per run and the
/// epoch is read from the wall clock, so both are left out.
type WriteSlot = (String, u64);

fn write_slot(key: &str) -> WriteSlot {
    let (dir, name) = key.rsplit_once('/').expect("a key with a directory");
    let shard = dir.rsplit('/').next().expect("a shard directory");
    let parts: Vec<&str> = name.split('.').collect();
    assert_eq!(parts.len(), 5, "a logs data key: {key}");
    assert_eq!(parts[4], "rseg", "a logs data key: {key}");
    let seq = parts[2]
        .parse::<u64>()
        .unwrap_or_else(|e| panic!("the sequence number in {key} parses: {e}"));
    (shard.to_string(), seq)
}

/// One hash per RLOG object, in each shard's write order, and the rows the
/// objects hold. The hash covers every section's stored bytes and the
/// footer's content fields, leaving out the per-run random writer id the
/// footer also carries.
async fn rlog_objects(store: &Arc<dyn ObjectStoreBackend>) -> (Vec<(WriteSlot, [u8; 32])>, u64) {
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
        hashes.push((write_slot(&meta.key), *hasher.finalize().as_bytes()));
    }
    hashes.sort_unstable();
    let slots = hashes.len();
    hashes.dedup_by(|a, b| a.0 == b.0);
    assert_eq!(hashes.len(), slots, "one object per write slot");
    (hashes, rows)
}

/// The flush geometry of one comparison: in both, no flush depends on
/// timing.
#[derive(Debug, Clone, Copy)]
enum Geometry {
    /// `--target-bytes` at the per-batch target: every batch flushes on its
    /// own, whatever the budget.
    PerBatchTarget,
    /// The derived target under an explicit `--pipeline-depth` and an
    /// explicit `--load-memory-bytes` far above what the load holds, so the
    /// depth is not derived from the budget and the decoder never waits on it.
    ExplicitDepthAndMemory,
}

impl Geometry {
    fn pipeline_depth(self) -> Option<usize> {
        match self {
            Self::PerBatchTarget => Some(1),
            Self::ExplicitDepthAndMemory => Some(3),
        }
    }

    fn target_bytes(self) -> Option<usize> {
        match self {
            Self::PerBatchTarget => Some(PER_BATCH_TARGET_BYTES),
            Self::ExplicitDepthAndMemory => None,
        }
    }

    fn load_memory_bytes(self) -> Option<u64> {
        match self {
            Self::PerBatchTarget => None,
            Self::ExplicitDepthAndMemory => Some(4 << 30),
        }
    }
}

async fn load_with_workers(
    pq: &Path,
    mapping: &Path,
    decode_workers: usize,
    geometry: Geometry,
) -> (Vec<(WriteSlot, [u8; 32])>, u64) {
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
        geometry.pipeline_depth(),
        1,
        DEFAULT_DECODE_QUEUE_BATCHES,
        Some(decode_workers),
        geometry.target_bytes(),
        // The CLI's 60 s age trigger, not the library's 2 s: an age flush on
        // a loaded host would place a boundary by wall clock.
        Some(DEFAULT_MAX_FLUSH_DELAY),
        None,
        RlogZstdLevel::DEFAULT,
        RlogGroupTargetBlocks::DEFAULT,
        RlogChunkOrder::DEFAULT,
        ravel_cli::load::LoadPageChoice::default(),
        geometry.load_memory_bytes(),
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
/// objects in the same order when no flush depends on timing: the same count,
/// the same rows, and in each shard the object at each write slot has the
/// same content hash. Run under a per-batch target, and under the derived
/// target with an explicit `--pipeline-depth`, a `--load-memory-bytes` the
/// load never fills, and the CLI's age trigger.
///
/// Prove-the-test: in `DecodeCoordinator::run`, emit the lowest ready batch
/// instead of `ready.remove(&self.next_emit)` (completion order, terminals
/// still last). The four-worker load then writes the same set of objects in a
/// different order, which the slot comparison catches and a sorted set of
/// hashes does not, but only when builds finish out of order, which nothing
/// at this level forces. The [`SLOW_ROWS`] batch makes that likely: the
/// mutation was caught in 250 of 250 runs on a 4-core aarch64 host, against
/// 46 of 50 without it. The deterministic proof that the coordinator emits in
/// deal order is `parallel_cursor_decode_yields_the_serial_batch_sequence` in
/// the decode unit tests, which holds builds back through the build hook.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_and_four_decode_workers_write_the_same_objects() {
    let (_dir, pq, mapping) = fixture();
    let total: i64 = GROUP_ROWS.iter().sum();
    for geometry in [Geometry::PerBatchTarget, Geometry::ExplicitDepthAndMemory] {
        let (one, one_rows) = load_with_workers(&pq, &mapping, 1, geometry).await;
        let (four, four_rows) = load_with_workers(&pq, &mapping, 4, geometry).await;
        assert_eq!(
            one_rows, total as u64,
            "{geometry:?}: the one-worker load wrote every row"
        );
        assert_eq!(
            four_rows, one_rows,
            "{geometry:?}: both wrote the same rows"
        );
        assert!(
            one.len() > 2,
            "{geometry:?}: several objects: {}",
            one.len()
        );
        assert_eq!(
            four.len(),
            one.len(),
            "{geometry:?}: both wrote the same objects"
        );
        for (slot, (four, one)) in four.iter().zip(&one).enumerate() {
            assert_eq!(
                four, one,
                "{geometry:?}: object {slot} in write order differs"
            );
        }
    }
}

/// `--decode-workers 0` is refused on every signal before anything is read
/// or written, including the metrics and spans loads that otherwise only
/// warn that they ignore the flag.
///
/// Prove-the-test: delete the `decode_workers == Some(0)` refusal in
/// `run_load_reporting` and the metrics and spans loads fail on the logs
/// mapping instead, with a different message.
#[tokio::test]
async fn a_zero_decode_workers_is_refused_on_every_signal() {
    let (_dir, pq, mapping) = fixture();
    for signal in [SignalArg::Logs, SignalArg::Metrics, SignalArg::Spans] {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let err = load::run(
            Arc::clone(&store),
            &pq,
            "acme",
            &mapping,
            signal,
            2,
            BATCH_ROWS,
            0,
            None,
            None,
            1,
            DEFAULT_DECODE_QUEUE_BATCHES,
            Some(0),
            None,
            None,
            None,
            RlogZstdLevel::DEFAULT,
            RlogGroupTargetBlocks::DEFAULT,
            RlogChunkOrder::DEFAULT,
            ravel_cli::load::LoadPageChoice::default(),
            None,
            false,
            &ravel_cli::ipcrypt::IpcryptPolicy::empty(),
            None,
            CLOCK_NS,
        )
        .await
        .expect_err("a zero worker count is refused");
        let message = format!("{err:#}");
        assert!(
            message.starts_with("--decode-workers must be at least 1"),
            "{signal:?}: {message}"
        );
        assert!(
            list_all(store.as_ref(), "").await.expect("list").is_empty(),
            "{signal:?}: nothing written"
        );
    }
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
