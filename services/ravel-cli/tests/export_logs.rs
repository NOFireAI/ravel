//! Acceptance coverage for `ravel-cli export --signal logs` (ADR-1751
//! decision 4, issue #1712).
//!
//! Drives the real library entry points in-process against a shared
//! `MemoryStore`, the same reason `tests/load.rs` does: a subprocess against
//! `--store memory` would give each invocation its own empty store, so a
//! load-then-export round trip could never see the loaded data.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array,
    StringArray,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use ravel_cli::export;
use ravel_cli::load::{self, Mapping};
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_ingest::Clock;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;

/// A fixed, plausible (post-2020) base clock, matching `tests/load.rs`'s
/// convention: the RLOG flush buckets by this reading, so a window near it
/// reaches a known hour and `Catalog::resolve` fans out only a couple of
/// LISTs.
const BASE_NS: i64 = 1_700_000_000_000_000_000; // 2023-11-14T22:13:20Z
const ONE_SEC_NS: i64 = 1_000_000_000;

struct FixedClock(i64);
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

fn read_parquet(path: &Path) -> RecordBatch {
    let file = std::fs::File::open(path).expect("open exported parquet");
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("reader builder");
    let mut reader = builder.build().expect("build reader");
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(batch) = reader.next() {
        batches.push(batch.expect("read batch"));
    }
    assert_eq!(batches.len(), 1, "expected exactly one output batch");
    batches.into_iter().next().expect("one batch")
}

fn i64_col(vals: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(vals))
}

fn str_col(vals: Vec<&str>) -> ArrayRef {
    Arc::new(StringArray::from(vals))
}

fn f64_col(vals: Vec<f64>) -> ArrayRef {
    Arc::new(Float64Array::from(vals))
}

fn bool_col(vals: Vec<bool>) -> ArrayRef {
    Arc::new(BooleanArray::from(vals))
}

fn binary_col(vals: Vec<Vec<u8>>) -> ArrayRef {
    let refs: Vec<&[u8]> = vals.iter().map(|v| v.as_slice()).collect();
    Arc::new(BinaryArray::from_vec(refs))
}

fn fixed_bin_col(width: i32, vals: Vec<Option<Vec<u8>>>) -> ArrayRef {
    let refs: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref()).collect();
    Arc::new(
        FixedSizeBinaryArray::try_from_sparse_iter_with_size(refs.into_iter(), width)
            .expect("fixed size binary column"),
    )
}

fn mapping(text: &str) -> Mapping {
    load::parse_mapping(text).expect("valid mapping")
}

const SHARED_MAPPING: &str = r#"
ts_column = "ts"
ts_unit = "nanos"
body_column = "body"
severity_number_column = "sev_num"
severity_text_column = "sev_text"
trace_id_column = "trace_id"
span_id_column = "span_id"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "tag"
column = "tag_col"
type = "str"

[[attribute]]
key = "count"
column = "count_col"
type = "i64"

[[attribute]]
key = "ratio"
column = "ratio_col"
type = "f64"

[[attribute]]
key = "flag"
column = "flag_col"
type = "bool"

[[attribute]]
key = "blob"
column = "blob_col"
type = "bytes"
"#;

/// Four source rows: three inside the export window (`T0`, `T1`, `T2`,
/// spread across two resources so `export_logs` decodes and caches
/// `StreamAttrs` for more than one stream) and one before it (`T_OUT`),
/// written to the source Parquet in a shuffled row order so a passing test
/// proves the export sorts its output by event time rather than by accident
/// preserving load order. `T2` carries a null trace/span id and an empty
/// (not null) `blob` attribute, to prove those are round-tripped as
/// null-vs-empty rather than collapsed to the same thing.
#[tokio::test]
async fn load_then_export_round_trips_logs_field_by_field() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source_pq = dir.path().join("source.parquet");
    let export_pq = dir.path().join("export.parquet");

    let t0 = BASE_NS;
    let t1 = BASE_NS + ONE_SEC_NS;
    let t2 = BASE_NS + 2 * ONE_SEC_NS;
    let t_out = BASE_NS - 10 * ONE_SEC_NS;

    // Row order on disk: T1, T0, T_OUT, T2.
    let batch = RecordBatch::try_from_iter(vec![
        ("ts".to_string(), i64_col(vec![t1, t0, t_out, t2])),
        (
            "body".to_string(),
            str_col(vec!["b-body", "a-body", "out-body", "c-body"]),
        ),
        ("sev_num".to_string(), i64_col(vec![5, 9, 9, 13])),
        (
            "sev_text".to_string(),
            str_col(vec!["DEBUG", "INFO", "INFO", "WARN"]),
        ),
        (
            "trace_id".to_string(),
            fixed_bin_col(
                16,
                vec![
                    Some(vec![0xBB; 16]),
                    Some(vec![0xAA; 16]),
                    Some(vec![0xCC; 16]),
                    None,
                ],
            ),
        ),
        (
            "span_id".to_string(),
            fixed_bin_col(
                8,
                vec![
                    Some(vec![0xBB; 8]),
                    Some(vec![0xAA; 8]),
                    Some(vec![0xCC; 8]),
                    None,
                ],
            ),
        ),
        (
            "svc".to_string(),
            str_col(vec!["api", "api", "api", "worker"]),
        ),
        (
            "tag_col".to_string(),
            str_col(vec!["beta", "alpha", "delta", "gamma"]),
        ),
        ("count_col".to_string(), i64_col(vec![2, 1, 4, 3])),
        (
            "ratio_col".to_string(),
            f64_col(vec![2.5, 1.5, 4.5, 3.5]),
        ),
        (
            "flag_col".to_string(),
            bool_col(vec![false, true, false, true]),
        ),
        (
            "blob_col".to_string(),
            binary_col(vec![vec![4, 5], vec![1, 2, 3], vec![9], vec![]]),
        ),
    ])
    .expect("batch");
    write_parquet(&source_pq, &batch);

    let m = mapping(SHARED_MAPPING);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let load_now_ns = t2;
    let report = load::load(
        Arc::clone(&store),
        &source_pq,
        "acme",
        &m,
        1,
        10_000,
        None,
        1,
        load_now_ns,
        Arc::new(FixedClock(load_now_ns)),
    )
    .await
    .expect("load succeeds");
    assert_eq!(report.rows_processed, 4);

    let export_report = export::export_logs(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        t0,
        t2 + 1,
        &m,
        &export_pq,
        1,
        load_now_ns,
    )
    .await
    .expect("export succeeds");
    assert_eq!(
        export_report.rows_written, 3,
        "T_OUT sits before the window and must not be exported"
    );

    let out = read_parquet(&export_pq);
    assert_eq!(out.num_rows(), 3);

    let ts = out
        .column_by_name("ts")
        .expect("ts")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ts is Int64");
    assert_eq!(ts.values(), &[t0, t1, t2], "sorted by event time");

    let body = out
        .column_by_name("body")
        .expect("body")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("body is Utf8");
    assert_eq!(
        (0..3).map(|i| body.value(i)).collect::<Vec<_>>(),
        vec!["a-body", "b-body", "c-body"]
    );

    let sev_num = out
        .column_by_name("sev_num")
        .expect("sev_num")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sev_num is Int64");
    assert_eq!(sev_num.values(), &[9, 5, 13]);

    let sev_text = out
        .column_by_name("sev_text")
        .expect("sev_text")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("sev_text is Utf8");
    assert_eq!(
        (0..3).map(|i| sev_text.value(i)).collect::<Vec<_>>(),
        vec!["INFO", "DEBUG", "WARN"]
    );

    let trace_id = out
        .column_by_name("trace_id")
        .expect("trace_id")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("trace_id is FixedSizeBinary");
    assert_eq!(trace_id.value(0), [0xAA; 16]);
    assert_eq!(trace_id.value(1), [0xBB; 16]);
    assert!(
        trace_id.is_null(2),
        "T2's trace_id was never set and must round-trip as null"
    );

    let span_id = out
        .column_by_name("span_id")
        .expect("span_id")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("span_id is FixedSizeBinary");
    assert_eq!(span_id.value(0), [0xAA; 8]);
    assert_eq!(span_id.value(1), [0xBB; 8]);
    assert!(span_id.is_null(2));

    let svc = out
        .column_by_name("svc")
        .expect("svc")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("svc is Utf8");
    assert_eq!(
        (0..3).map(|i| svc.value(i)).collect::<Vec<_>>(),
        vec!["api", "api", "worker"]
    );

    let tag = out
        .column_by_name("tag_col")
        .expect("tag_col")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("tag_col is Utf8");
    assert_eq!(
        (0..3).map(|i| tag.value(i)).collect::<Vec<_>>(),
        vec!["alpha", "beta", "gamma"]
    );

    let count = out
        .column_by_name("count_col")
        .expect("count_col")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count_col is Int64");
    assert_eq!(count.values(), &[1, 2, 3]);

    let ratio = out
        .column_by_name("ratio_col")
        .expect("ratio_col")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("ratio_col is Float64");
    assert_eq!(ratio.values(), &[1.5, 2.5, 3.5]);

    let flag = out
        .column_by_name("flag_col")
        .expect("flag_col")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("flag_col is Boolean");
    assert_eq!(
        (0..3).map(|i| flag.value(i)).collect::<Vec<_>>(),
        vec![true, false, true]
    );

    let blob = out
        .column_by_name("blob_col")
        .expect("blob_col")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("blob_col is Binary");
    assert_eq!(blob.value(0), &[1u8, 2, 3][..]);
    assert_eq!(blob.value(1), &[4u8, 5][..]);
    assert!(
        !blob.is_null(2) && blob.value(2).is_empty(),
        "T2's blob attribute was set to an empty (not null) byte string"
    );

    // The out-of-window row's distinguishing values are absent everywhere.
    assert!(!tag.iter().flatten().any(|v| v == "delta"));
    assert!(!count.values().contains(&4));
}

/// `--signal metrics` (and, by the same code path, `spans`) is refused by
/// name rather than attempted: ADR-1751's follow-up order lands metrics load
/// and spans load first, and export has nothing to round-trip either against
/// until then.
#[tokio::test]
async fn export_refuses_unsupported_signal_metrics() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let err = export::run(
        store,
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        SignalArg::Metrics,
        BASE_NS,
        BASE_NS + ONE_SEC_NS,
        Path::new("/nonexistent/mapping.toml"),
        Path::new("/nonexistent/out.parquet"),
        1,
        BASE_NS,
    )
    .await
    .expect_err("metrics export is refused");
    assert_eq!(
        err.to_string(),
        export::unsupported_signal_message(SignalArg::Metrics).expect("metrics is unsupported")
    );
}
