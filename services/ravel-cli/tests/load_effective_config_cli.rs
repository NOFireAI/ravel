//! Subprocess coverage that `ravel-cli load --parquet` prints the effective
//! configuration it ran with (issue #680).
//!
//! Driven through the built binary, not the library entry point, because the
//! deliverable is that an operator can reconstruct a load's object layout from
//! what the command printed. A library-level assertion on `LoadReport` proves
//! the values are computed; only this proves they are emitted. `--store memory`
//! is fine here: the assertion is on stdout, not on the loaded data, so the
//! process-local store the subprocess writes into is never read back.
#![allow(clippy::expect_used)]

use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

/// Every knob the load reports, under the spelling it would be passed back in.
/// A knob added to the loader without being reported fails here by name.
const REPORTED_KNOBS: &[&str] = &[
    "--shards",
    "--batch-rows",
    "--read-cursors",
    "--pipeline-depth",
    "--max-inflight-flushes",
    "--decode-queue-batches",
    "--target-bytes",
];

#[test]
fn load_prints_the_effective_configuration_it_ran_with() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("logs.parquet");
    let mapping_path = dir.path().join("logs.toml");

    // Real wall clock: the binary anchors its future-skew check on its own
    // `now`, and this test cannot inject one.
    let now_ns = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("nanos fit in i64");

    let ts: ArrayRef = std::sync::Arc::new(Int64Array::from(vec![now_ns; 4]));
    let body: ArrayRef = std::sync::Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let svc: ArrayRef = std::sync::Arc::new(StringArray::from(vec!["api"; 4]));
    let batch = RecordBatch::try_from_iter(vec![
        ("ts".to_string(), ts),
        ("body".to_string(), body),
        ("svc".to_string(), svc),
    ])
    .expect("batch");
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    std::fs::write(
        &mapping_path,
        "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n\n\
         [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n",
    )
    .expect("write mapping");

    let output = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args([
            "--store",
            "memory",
            "load",
            "--parquet",
            pq.to_str().expect("utf-8 path"),
            "--tenant",
            "acme",
            "--mapping",
            mapping_path.to_str().expect("utf-8 path"),
            "--shards",
            "4",
        ])
        .output()
        .expect("ravel-cli runs");

    assert!(
        output.status.success(),
        "the load must succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        stdout.contains("effective configuration:"),
        "the summary must carry an effective-configuration block; got:\n{stdout}"
    );
    for knob in REPORTED_KNOBS {
        assert!(
            stdout.contains(knob),
            "the effective configuration must report {knob}; got:\n{stdout}"
        );
    }

    // With neither flag given, both derived knobs are reported with their
    // chosen values and labelled as the loader's choice, not the operator's:
    // one cursor per shard, and the historical batch size for an input this
    // small.
    assert!(
        stdout.contains("--read-cursors         : 1 (derived)"),
        "4 shards over a single-row-group file clamps to one cursor, derived; got:\n{stdout}"
    );
    assert!(
        stdout.contains("--batch-rows           : 10000 (derived)"),
        "a 4-row input keeps the historical batch size, derived; got:\n{stdout}"
    );
    assert!(
        stdout.contains("--shards               : 4"),
        "the shard count the load ran with is reported; got:\n{stdout}"
    );
}

/// A flag the operator passed is reported back as theirs, so the block is not
/// mistaken for a list of defaults.
#[test]
fn an_explicit_flag_is_reported_as_explicit() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("logs.parquet");
    let mapping_path = dir.path().join("logs.toml");

    let now_ns = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("nanos fit in i64");

    let ts: ArrayRef = std::sync::Arc::new(Int64Array::from(vec![now_ns; 4]));
    let body: ArrayRef = std::sync::Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let batch = RecordBatch::try_from_iter(vec![("ts".to_string(), ts), ("body".to_string(), body)])
        .expect("batch");
    let file = std::fs::File::create(&pq).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");

    std::fs::write(
        &mapping_path,
        "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n",
    )
    .expect("write mapping");

    let output = Command::new(env!("CARGO_BIN_EXE_ravel-cli"))
        .args([
            "--store",
            "memory",
            "load",
            "--parquet",
            pq.to_str().expect("utf-8 path"),
            "--tenant",
            "acme",
            "--mapping",
            mapping_path.to_str().expect("utf-8 path"),
            "--batch-rows",
            "2",
        ])
        .output()
        .expect("ravel-cli runs");

    assert!(
        output.status.success(),
        "the load must succeed; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("--batch-rows           : 2 (explicit)"),
        "an explicit --batch-rows is reported verbatim and labelled explicit; got:\n{stdout}"
    );
}
