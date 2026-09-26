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
use ravel_cli::maintain::{SignalArg, compact_tenant};
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_cli::{erase, maintain};
use ravel_commit::keys;
use ravel_ingest::Clock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::RetentionTombstone;
use ravel_types::Signal;
use uuid::Uuid;

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
    let reader = builder.build().expect("build reader");
    let mut batches: Vec<RecordBatch> = Vec::new();
    for batch in reader {
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

/// The three in-window event times the fixture below uses, and the one
/// before the window.
const T0: i64 = BASE_NS;
const T1: i64 = BASE_NS + ONE_SEC_NS;
const T2: i64 = BASE_NS + 2 * ONE_SEC_NS;
const T_OUT: i64 = BASE_NS - 10 * ONE_SEC_NS;

/// Four source rows for [`SHARED_MAPPING`]: three inside the export window
/// (`T0`, `T1`, `T2`, spread across two resources so `export_logs` decodes
/// and caches `StreamAttrs` for more than one stream) and one before it
/// (`T_OUT`), in a shuffled row order so a passing test proves the export
/// sorts its output by event time rather than by accident preserving load
/// order. `T2` carries a null trace/span id and an empty (not null) `blob`
/// attribute, to prove those are round-tripped as null-vs-empty rather than
/// collapsed to the same thing.
fn shared_source_batch() -> RecordBatch {
    let (t0, t1, t2, t_out) = (T0, T1, T2, T_OUT);
    // Row order on disk: T1, T0, T_OUT, T2.
    RecordBatch::try_from_iter(vec![
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
        ("ratio_col".to_string(), f64_col(vec![2.5, 1.5, 4.5, 3.5])),
        (
            "flag_col".to_string(),
            bool_col(vec![false, true, false, true]),
        ),
        (
            "blob_col".to_string(),
            binary_col(vec![vec![4, 5], vec![1, 2, 3], vec![9], vec![]]),
        ),
    ])
    .expect("batch")
}

/// The whole-file `load` -> `export` path over [`shared_source_batch`],
/// asserting every column of the output by value.
#[tokio::test]
async fn load_then_export_round_trips_logs_field_by_field() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source_pq = dir.path().join("source.parquet");
    let export_pq = dir.path().join("export.parquet");

    let (t0, t1, t2) = (T0, T1, T2);
    write_parquet(&source_pq, &shared_source_batch());

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
        export::CatalogWindow::default(),
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

/// Every column [`SHARED_MAPPING`] writes, in the order `build_batch`
/// emits them.
const SHARED_MAPPING_COLUMNS: [&str; 12] = [
    "ts",
    "body",
    "sev_num",
    "sev_text",
    "trace_id",
    "span_id",
    "svc",
    "tag_col",
    "count_col",
    "ratio_col",
    "flag_col",
    "blob_col",
];

/// ADR-1751 follow-up 3's `load(export(x))` round trip, end to end: the file
/// the export writes is fed back through `ravel-cli load` into a second,
/// fresh tenant, and that tenant is exported again.
///
/// This is the claim the CHANGELOG, the `--mapping` flag help and the ingest
/// guide all make -- that `load` reads an exported file back -- and nothing
/// was asserting it. The first export was only ever opened with an Arrow
/// reader, which proves the file is well-formed Parquet and nothing about
/// whether the loader accepts it.
///
/// Both exports are asserted column by column against each other, so a field
/// that survives the first trip and not the second fails by name, and the
/// second tenant's export is asserted by value as well, so a round trip that
/// agreed on being wrong twice would still fail. The null trace and span ids
/// and the empty-but-not-null `blob` are carried through deliberately: null
/// and absent are the two values a lossy reload is most likely to collapse.
#[tokio::test]
async fn export_reloads_into_a_second_tenant_field_for_field() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source_pq = dir.path().join("source.parquet");
    let first_pq = dir.path().join("first.parquet");
    let second_pq = dir.path().join("second.parquet");

    write_parquet(&source_pq, &shared_source_batch());
    let m = mapping(SHARED_MAPPING);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    let first_load = load::load(
        Arc::clone(&store),
        &source_pq,
        "acme",
        &m,
        1,
        10_000,
        None,
        1,
        T2,
        Arc::new(FixedClock(T2)),
    )
    .await
    .expect("the source file loads");
    assert_eq!(first_load.rows_processed, 4);

    let first_export = export::export_logs(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        T0,
        T2 + 1,
        &m,
        &first_pq,
        1,
        export::CatalogWindow::default(),
        T2,
    )
    .await
    .expect("the first export succeeds");
    assert_eq!(first_export.rows_written, 3);

    // The claim under test: `load` reads the exported file back, under the
    // same mapping, with no conversion step in between.
    let reload = load::load(
        Arc::clone(&store),
        &first_pq,
        "acme-copy",
        &m,
        1,
        10_000,
        None,
        1,
        T2,
        Arc::new(FixedClock(T2)),
    )
    .await
    .expect("the exported file loads back");
    assert_eq!(
        reload.rows_processed, 3,
        "every exported row must be readable by load, not merely most of them"
    );

    let second_export = export::export_logs(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme-copy",
        T0,
        T2 + 1,
        &m,
        &second_pq,
        1,
        export::CatalogWindow::default(),
        T2,
    )
    .await
    .expect("the second export succeeds");
    assert_eq!(second_export.rows_written, 3);

    let first = read_parquet(&first_pq);
    let second = read_parquet(&second_pq);
    assert_eq!(first.num_rows(), 3);
    assert_eq!(second.num_rows(), 3);

    for name in SHARED_MAPPING_COLUMNS {
        let a = first
            .column_by_name(name)
            .unwrap_or_else(|| panic!("the first export has no column {name}"));
        let b = second
            .column_by_name(name)
            .unwrap_or_else(|| panic!("the second export has no column {name}"));
        assert_eq!(
            a.to_data(),
            b.to_data(),
            "column {name} differs between the two tenants' exports"
        );
    }

    // ... and the value the two agree on is the right one, per column.
    let ts = second
        .column_by_name("ts")
        .expect("ts")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ts is Int64");
    assert_eq!(ts.values(), &[T0, T1, T2]);

    assert_eq!(
        exported_bodies(&second_pq),
        vec![
            "a-body".to_string(),
            "b-body".to_string(),
            "c-body".to_string()
        ]
    );

    let sev_num = second
        .column_by_name("sev_num")
        .expect("sev_num")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sev_num is Int64");
    assert_eq!(sev_num.values(), &[9, 5, 13]);

    let sev_text = second
        .column_by_name("sev_text")
        .expect("sev_text")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("sev_text is Utf8");
    assert_eq!(
        (0..3).map(|i| sev_text.value(i)).collect::<Vec<_>>(),
        vec!["INFO", "DEBUG", "WARN"]
    );

    let trace_id = second
        .column_by_name("trace_id")
        .expect("trace_id")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("trace_id is FixedSizeBinary");
    assert_eq!(trace_id.value(0), [0xAA; 16]);
    assert_eq!(trace_id.value(1), [0xBB; 16]);
    assert!(
        trace_id.is_null(2),
        "a null trace_id must survive the reload as null, not as 16 zero bytes"
    );

    let span_id = second
        .column_by_name("span_id")
        .expect("span_id")
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .expect("span_id is FixedSizeBinary");
    assert_eq!(span_id.value(0), [0xAA; 8]);
    assert_eq!(span_id.value(1), [0xBB; 8]);
    assert!(span_id.is_null(2));

    let svc = second
        .column_by_name("svc")
        .expect("svc")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("svc is Utf8");
    assert_eq!(
        (0..3).map(|i| svc.value(i)).collect::<Vec<_>>(),
        vec!["api", "api", "worker"],
        "the resource attribute that decides stream identity survives the reload"
    );

    let tag = second
        .column_by_name("tag_col")
        .expect("tag_col")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("tag_col is Utf8");
    assert_eq!(
        (0..3).map(|i| tag.value(i)).collect::<Vec<_>>(),
        vec!["alpha", "beta", "gamma"]
    );

    let count = second
        .column_by_name("count_col")
        .expect("count_col")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count_col is Int64");
    assert_eq!(count.values(), &[1, 2, 3]);

    let ratio = second
        .column_by_name("ratio_col")
        .expect("ratio_col")
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("ratio_col is Float64");
    assert_eq!(ratio.values(), &[1.5, 2.5, 3.5]);

    let flag = second
        .column_by_name("flag_col")
        .expect("flag_col")
        .as_any()
        .downcast_ref::<BooleanArray>()
        .expect("flag_col is Boolean");
    assert_eq!(
        (0..3).map(|i| flag.value(i)).collect::<Vec<_>>(),
        vec![true, false, true]
    );

    let blob = second
        .column_by_name("blob_col")
        .expect("blob_col")
        .as_any()
        .downcast_ref::<BinaryArray>()
        .expect("blob_col is Binary");
    assert_eq!(blob.value(0), &[1u8, 2, 3][..]);
    assert_eq!(blob.value(1), &[4u8, 5][..]);
    assert!(
        !blob.is_null(2) && blob.value(2).is_empty(),
        "an empty byte string must not come back as null after the reload"
    );

    // The row outside the first export's window never entered the copy.
    assert!(!tag.iter().flatten().any(|v| v == "delta"));
    assert!(!count.values().contains(&4));
}

/// The smallest mapping that still names a body, for fixtures whose subject
/// is the window or the output file rather than the column set.
const TS_BODY_MAPPING: &str = "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n";

/// Load `(ts, body)` rows into `tenant` under [`TS_BODY_MAPPING`].
///
/// `batch_rows` decides the object layout, which several tests below assert
/// on: at `1`, the loader's default `--target-bytes 1` makes every row its
/// own Strict flush and therefore its own RLOG object, so a segment-level
/// bound is observable in `segments_read`. At a batch size covering every
/// row, all rows land in one object and one block, so only a post-decode
/// filter can separate them.
async fn load_ts_body_rows(
    store: &Arc<dyn ObjectStoreBackend>,
    dir: &Path,
    tenant: &str,
    rows: &[(i64, &str)],
    batch_rows: usize,
    load_now_ns: i64,
) -> std::path::PathBuf {
    let source_pq = dir.join(format!("source-{tenant}-{load_now_ns}.parquet"));
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts".to_string(),
            i64_col(rows.iter().map(|(ts, _)| *ts).collect()),
        ),
        (
            "body".to_string(),
            str_col(rows.iter().map(|(_, body)| *body).collect()),
        ),
    ])
    .expect("batch");
    write_parquet(&source_pq, &batch);
    let report = load::load(
        Arc::clone(store),
        &source_pq,
        tenant,
        &mapping(TS_BODY_MAPPING),
        1,
        batch_rows,
        None,
        1,
        load_now_ns,
        Arc::new(FixedClock(load_now_ns)),
    )
    .await
    .expect("load succeeds");
    assert_eq!(report.rows_processed, rows.len() as u64);
    source_pq
}

/// The `body` column of an exported file, in the order the file holds it.
fn exported_bodies(path: &Path) -> Vec<String> {
    let out = read_parquet(path);
    let body = out
        .column_by_name("body")
        .expect("body")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("body is Utf8");
    (0..body.len()).map(|i| body.value(i).to_string()).collect()
}

/// The exported `ts` column, in file order.
fn exported_timestamps(path: &Path) -> Vec<i64> {
    let out = read_parquet(path);
    out.column_by_name("ts")
        .expect("ts")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("ts is Int64")
        .values()
        .to_vec()
}

/// `[start, end)` is exact to the nanosecond at the end bound: a record at
/// exactly `--end` is excluded and the one a nanosecond earlier is kept.
///
/// All three rows are loaded in one batch, so they share one object and one
/// block. Block pruning therefore cannot separate them -- the fetch returns
/// all three -- and only the post-decode `ts_ns < end_ns` filter can, which
/// is what this pins. The sibling test below pins the fetch bound instead.
#[tokio::test]
async fn a_record_at_exactly_the_window_end_is_not_exported() {
    let dir = tempfile::tempdir().expect("tempdir");
    let export_pq = dir.path().join("export.parquet");

    let start_ns = BASE_NS;
    let end_ns = BASE_NS + ONE_SEC_NS;
    let load_now_ns = end_ns;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load_ts_body_rows(
        &store,
        dir.path(),
        "acme",
        &[
            (start_ns, "at-start"),
            (end_ns - 1, "one-before-end"),
            (end_ns, "at-end"),
        ],
        10_000,
        load_now_ns,
    )
    .await;

    let report = export::export_logs(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        start_ns,
        end_ns,
        &mapping(TS_BODY_MAPPING),
        &export_pq,
        1,
        export::CatalogWindow::default(),
        load_now_ns,
    )
    .await
    .expect("export succeeds");

    assert_eq!(
        report.segments_read, 1,
        "the fixture must put all three rows in one object, or the post-decode filter is not \
         what separates them"
    );
    assert_eq!(
        report.rows_written, 2,
        "the row at exactly --end is outside the half-open window"
    );
    assert_eq!(
        exported_timestamps(&export_pq),
        vec![start_ns, end_ns - 1],
        "--start is included and --end - 1 is the last exportable nanosecond"
    );
    assert_eq!(
        exported_bodies(&export_pq),
        vec!["at-start".to_string(), "one-before-end".to_string()]
    );
}

/// The half-open end reaches the fetch bound too, not only the filter over
/// the decoded rows: an object holding nothing but a record at `--end` is
/// never fetched.
///
/// `LogQuery`'s range is inclusive on both ends, so the export asks for
/// `[start, end - 1]`. One row per object makes that `- 1` observable:
/// without it the third object's block survives pruning, is fetched and
/// decoded, and only then has its single row thrown away by the post-decode
/// filter -- the same output file for one more GET and one more decode.
/// `segments_pruned` is asserted alongside `segments_read` so the count means
/// what it says: all three objects reached the fetcher, and the third was
/// rejected there rather than never resolved.
#[tokio::test]
async fn an_object_holding_only_the_record_at_the_window_end_is_not_fetched() {
    let dir = tempfile::tempdir().expect("tempdir");
    let export_pq = dir.path().join("export.parquet");

    let start_ns = BASE_NS;
    let end_ns = BASE_NS + ONE_SEC_NS;
    let load_now_ns = end_ns;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load_ts_body_rows(
        &store,
        dir.path(),
        "acme",
        &[
            (start_ns, "at-start"),
            (end_ns - 1, "one-before-end"),
            (end_ns, "at-end"),
        ],
        1,
        load_now_ns,
    )
    .await;

    let report = export::export_logs(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        start_ns,
        end_ns,
        &mapping(TS_BODY_MAPPING),
        &export_pq,
        1,
        export::CatalogWindow::default(),
        load_now_ns,
    )
    .await
    .expect("export succeeds");

    assert_eq!(
        report.segments_pruned, 0,
        "all three objects overlap the resolve range and reach the fetcher"
    );
    assert_eq!(
        report.segments_read, 2,
        "the object holding only the row at --end must not be fetched at all"
    );
    assert_eq!(report.rows_written, 2);
    assert_eq!(exported_timestamps(&export_pq), vec![start_ns, end_ns - 1]);
}

/// A mid-export failure leaves an existing `--parquet` file byte-identical,
/// and leaves no temporary file behind either.
///
/// The failure is a real one from this path: the data is loaded with `tag`
/// declared `str`, and the export is asked for a mapping that declares the
/// same key `i64`, which `build_batch` refuses by name on the first row. That
/// is after the output file would have been opened, which is the whole point
/// of the fixture.
#[tokio::test]
async fn a_failing_export_leaves_an_existing_output_file_byte_identical() {
    let dir = tempfile::tempdir().expect("tempdir");
    let source_pq = dir.path().join("source.parquet");
    let export_pq = dir.path().join("export.parquet");

    let load_mapping = mapping(
        "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n\n\
         [[attribute]]\nkey = \"tag\"\ncolumn = \"tag_col\"\ntype = \"str\"\n",
    );
    let export_mapping = mapping(
        "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n\n\
         [[attribute]]\nkey = \"tag\"\ncolumn = \"tag_col\"\ntype = \"i64\"\n",
    );

    let batch = RecordBatch::try_from_iter(vec![
        ("ts".to_string(), i64_col(vec![BASE_NS])),
        ("body".to_string(), str_col(vec!["only"])),
        ("tag_col".to_string(), str_col(vec!["alpha"])),
    ])
    .expect("batch");
    write_parquet(&source_pq, &batch);

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load::load(
        Arc::clone(&store),
        &source_pq,
        "acme",
        &load_mapping,
        1,
        10_000,
        None,
        1,
        BASE_NS,
        Arc::new(FixedClock(BASE_NS)),
    )
    .await
    .expect("load succeeds");

    // A previous export's output, standing where the failing one will aim.
    let previous = b"an earlier export, not this one".to_vec();
    std::fs::write(&export_pq, &previous).expect("seed the existing output file");

    let err = export::export_logs(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        BASE_NS,
        BASE_NS + ONE_SEC_NS,
        &export_mapping,
        &export_pq,
        1,
        export::CatalogWindow::default(),
        BASE_NS + ONE_SEC_NS,
    )
    .await
    .expect_err("a declared type the stored value does not match is refused");
    assert_eq!(
        err.to_string(),
        "attribute \"tag\" is declared i64 in the mapping but the stored value is str; fix the \
         mapping's type for this key, or drop the column and let attrs_map_column carry it"
    );

    assert_eq!(
        std::fs::read(&export_pq).expect("the existing output file is still readable"),
        previous,
        "a failed export must not touch the file it was aiming at"
    );

    let left_behind: Vec<String> = std::fs::read_dir(dir.path())
        .expect("read the output directory")
        .map(|entry| {
            entry
                .expect("directory entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .filter(|name| name != "source.parquet" && name != "export.parquet")
        .collect();
    assert_eq!(
        left_behind,
        Vec::<String>::new(),
        "the temporary file a failed export wrote into must be removed"
    );
}

const NS_PER_HOUR: i64 = 3_600_000_000_000;

/// A mapping with one string attribute, which is the shape an ADR-0064
/// erasure predicate matches on: its matchers compare string-valued record
/// attributes.
const TS_BODY_SUBJECT_MAPPING: &str = "ts_column = \"ts\"\nts_unit = \"nanos\"\n\
     body_column = \"body\"\n\n\
     [[attribute]]\nkey = \"subject\"\ncolumn = \"subject_col\"\ntype = \"str\"\n";

/// The exported `subject_col` column, in file order.
fn exported_subjects(path: &Path) -> Vec<String> {
    let out = read_parquet(path);
    let col = out
        .column_by_name("subject_col")
        .expect("subject_col")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("subject_col is Utf8");
    (0..col.len()).map(|i| col.value(i).to_string()).collect()
}

/// Load `(ts, body, subject)` rows under [`TS_BODY_SUBJECT_MAPPING`].
async fn load_subject_rows(
    store: &Arc<dyn ObjectStoreBackend>,
    dir: &Path,
    tenant: &str,
    rows: &[(i64, &str, &str)],
    batch_rows: usize,
    load_now_ns: i64,
) {
    let source_pq = dir.join(format!("subjects-{tenant}-{load_now_ns}.parquet"));
    let batch = RecordBatch::try_from_iter(vec![
        (
            "ts".to_string(),
            i64_col(rows.iter().map(|(ts, _, _)| *ts).collect()),
        ),
        (
            "body".to_string(),
            str_col(rows.iter().map(|(_, body, _)| *body).collect()),
        ),
        (
            "subject_col".to_string(),
            str_col(rows.iter().map(|(_, _, subject)| *subject).collect()),
        ),
    ])
    .expect("batch");
    write_parquet(&source_pq, &batch);
    let report = load::load(
        Arc::clone(store),
        &source_pq,
        tenant,
        &mapping(TS_BODY_SUBJECT_MAPPING),
        1,
        batch_rows,
        None,
        1,
        load_now_ns,
        Arc::new(FixedClock(load_now_ns)),
    )
    .await
    .expect("load succeeds");
    assert_eq!(report.rows_processed, rows.len() as u64);
}

/// A pending selective-erasure request (ADR-0064) excludes exactly the
/// records its predicate matches, and leaves every other record in place.
///
/// This is the guide's "Deleted data does not come back" section and the
/// CHANGELOG's "a record a query cannot see is a record the export does not
/// write", and nothing was failing if `export_logs` stopped attaching the
/// snapshot's predicates to its fetch. The raw RLOG objects still hold the
/// erased rows here -- `erase submit` writes a request, it does not rewrite
/// any object -- so the export reading them back is exactly the exposure this
/// pins.
#[tokio::test]
async fn a_pending_erasure_request_excludes_exactly_its_subject() {
    let dir = tempfile::tempdir().expect("tempdir");
    let export_pq = dir.path().join("export.parquet");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    load_subject_rows(
        &store,
        dir.path(),
        "acme",
        &[
            (T0, "first", "keeper"),
            (T1, "second", "erase-me"),
            (T2, "third", "keeper"),
            (T2 + 1, "fourth", "erase-me"),
        ],
        10_000,
        T2 + 1,
    )
    .await;

    // Unbounded window: the predicate is the subject alone, so the test is
    // about matcher exclusion rather than about the request's own time range.
    erase::submit(
        Arc::clone(&store),
        "acme",
        SignalArg::Logs,
        vec![("subject".to_string(), "erase-me".to_string())],
        0,
        0,
        "issue #1712 test".to_string(),
        Uuid::from_u128(0x1712),
        T2 + 1,
    )
    .await
    .expect("the erasure request is recorded");

    let report = export::export_logs(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        T0,
        T2 + 2,
        &mapping(TS_BODY_SUBJECT_MAPPING),
        &export_pq,
        1,
        export::CatalogWindow::default(),
        T2 + 1,
    )
    .await
    .expect("export succeeds");

    assert_eq!(
        report.erasure_predicates, 1,
        "the snapshot's pending request must reach the fetch as one predicate"
    );
    assert_eq!(
        report.rows_written, 2,
        "both matching records are excluded and both others are kept"
    );
    assert_eq!(
        exported_bodies(&export_pq),
        vec!["first".to_string(), "third".to_string()],
        "exactly the two records the predicate does not match"
    );
    assert_eq!(
        exported_subjects(&export_pq),
        vec!["keeper".to_string(), "keeper".to_string()]
    );
    assert_eq!(exported_timestamps(&export_pq), vec![T0, T2]);
}

/// A retention tombstone (ADR-0019 decision 3) removes exactly its own
/// bucket's records from the export, and leaves another bucket's alone.
///
/// The two loads run an hour apart on the injected clock, so they land in
/// different ingest-hour buckets while their event times stay inside one
/// export window. Tombstoning the first bucket is then observable as the
/// disappearance of its records and nothing else. The tombstone is written
/// exactly as ravel-maintain's retention sweep writes one; the objects it
/// retires are still in the store, which is what makes this a visibility
/// assertion rather than a deletion one.
#[tokio::test]
async fn a_retention_tombstone_removes_exactly_its_buckets_records() {
    let dir = tempfile::tempdir().expect("tempdir");
    let export_pq = dir.path().join("export.parquet");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    let first_load_ns = T0;
    let second_load_ns = T0 + NS_PER_HOUR;
    let first_hour = u32::try_from(first_load_ns / NS_PER_HOUR).expect("hour fits u32");
    let second_hour = u32::try_from(second_load_ns / NS_PER_HOUR).expect("hour fits u32");
    assert_ne!(
        first_hour, second_hour,
        "the fixture needs the two loads in different ingest-hour buckets"
    );

    load_ts_body_rows(
        &store,
        dir.path(),
        "acme",
        &[(T0, "older-bucket-a"), (T1, "older-bucket-b")],
        10_000,
        first_load_ns,
    )
    .await;
    load_ts_body_rows(
        &store,
        dir.path(),
        "acme",
        &[(T2, "newer-bucket-a"), (T2 + 1, "newer-bucket-b")],
        10_000,
        second_load_ns,
    )
    .await;

    let window = export::CatalogWindow::default();
    let export_now_ns = second_load_ns + NS_PER_HOUR;

    // Baseline: without the tombstone all four records export.
    let before = export::export_logs(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        T0,
        T2 + 2,
        &mapping(TS_BODY_MAPPING),
        &export_pq,
        1,
        window,
        export_now_ns,
    )
    .await
    .expect("export succeeds");
    assert_eq!(
        before.rows_written, 4,
        "the fixture must reach both buckets before the tombstone is written"
    );

    write_retention_tombstone(store.as_ref(), "acme", 0, first_hour).await;

    let after = export::export_logs(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        T0,
        T2 + 2,
        &mapping(TS_BODY_MAPPING),
        &export_pq,
        1,
        window,
        export_now_ns,
    )
    .await
    .expect("export succeeds");

    assert_eq!(
        after.rows_written, 2,
        "exactly the tombstoned bucket's two records are gone"
    );
    assert_eq!(
        exported_bodies(&export_pq),
        vec!["newer-bucket-a".to_string(), "newer-bucket-b".to_string()],
        "the untombstoned bucket's records are untouched"
    );
    assert_eq!(exported_timestamps(&export_pq), vec![T2, T2 + 1]);
}

/// Write a retention tombstone into one (shard, ingest-hour) bucket, the way
/// ravel-maintain's retention sweep does.
async fn write_retention_tombstone(
    store: &dyn ObjectStoreBackend,
    tenant: &str,
    shard: u32,
    ingest_hour_bucket: u32,
) {
    let tombstone = RetentionTombstone {
        format_version: 1,
        tenant_hash: ravel_types::TenantId::new(tenant).hash().0.to_vec(),
        signal: ravel_commit::signal::to_proto(Signal::Logs) as i32,
        shard,
        ingest_hour_bucket,
        retired_at_ns: 0,
        retention_window_ns: 0,
        record_count_observed: 0,
    };
    let key = keys::retention_tombstone_key_for(&tombstone).expect("tombstone key");
    store
        .put(
            &key,
            ravel_commit::record::encode_tombstone(&tombstone),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put tombstone");
}

/// After a compaction, an L1 part and the L0 objects it merged are both in
/// the store, and the export writes each record exactly once.
///
/// Supersession is `Catalog::resolve`'s job, not the export's, but the export
/// is what a duplicate would be visible in, and the guide claims it: "objects
/// superseded by compaction are already absent from the snapshot the export
/// resolves". The fixture asserts the shape it rests on -- the compaction
/// really ran, and the L0 inputs really are still there -- so a run where
/// nothing compacted cannot pass as a supersession result.
#[tokio::test]
async fn a_compacted_bucket_exports_each_record_exactly_once() {
    let dir = tempfile::tempdir().expect("tempdir");
    let export_pq = dir.path().join("export.parquet");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    let load_now_ns = T0;
    load_ts_body_rows(
        &store,
        dir.path(),
        "acme",
        &[(T0, "row-a"), (T1, "row-b"), (T2, "row-c")],
        1,
        load_now_ns,
    )
    .await;

    let tenant_hash = ravel_types::TenantId::new("acme").hash();
    let l0_prefix = format!(
        "t/{}/{}/l0/",
        tenant_hash.to_hex(),
        Signal::Logs.key_prefix()
    );
    let l1_prefix = format!(
        "t/{}/{}/l1/",
        tenant_hash.to_hex(),
        Signal::Logs.key_prefix()
    );
    let l0_before = list_all(store.as_ref(), &l0_prefix)
        .await
        .expect("list the tenant's L0 objects");
    assert_eq!(
        l0_before.len(),
        3,
        "one object per row, so the compaction has more than one input"
    );

    // An hour past the loaded hour's end, with the flush-lifetime override at
    // zero, so the bucket is sealed and compactable.
    let compact_now_ns = (load_now_ns / NS_PER_HOUR + 2) * NS_PER_HOUR;
    let report = compact_tenant(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        SignalArg::Logs,
        Some(1),
        None,
        None,
        false,
        Some(0),
        None,
        None,
        None,
        1,
        compact_now_ns,
    )
    .await
    .expect("compaction runs");
    assert_eq!(
        report.compacted, 1,
        "exactly the loaded bucket must compact, or this test proves nothing"
    );
    assert_eq!(report.parts_written, 1);

    let l0_after = list_all(store.as_ref(), &l0_prefix)
        .await
        .expect("list the tenant's L0 objects");
    let l1_after = list_all(store.as_ref(), &l1_prefix)
        .await
        .expect("list the tenant's L1 parts");
    assert_eq!(
        l1_after.len(),
        1,
        "the compaction wrote its L1 part where the resolve will find it"
    );
    assert_eq!(
        l0_after.len(),
        3,
        "compaction never deletes: all three L0 inputs are still in the store beside the L1 \
         part, which is what makes a double-counting resolve visible here"
    );
    for l0 in &l0_before {
        assert!(
            l0_after.iter().any(|meta| meta.key == l0.key),
            "L0 input {} must still be in the store for this test to mean anything",
            l0.key
        );
    }

    let export_report = export::export_logs(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        T0,
        T2 + 1,
        &mapping(TS_BODY_MAPPING),
        &export_pq,
        1,
        export::CatalogWindow::default(),
        compact_now_ns,
    )
    .await
    .expect("export succeeds");

    assert_eq!(
        export_report.rows_written, 3,
        "each record exactly once: the L0 inputs are superseded, not added to the L1 part"
    );
    assert_eq!(
        exported_bodies(&export_pq),
        vec![
            "row-a".to_string(),
            "row-b".to_string(),
            "row-c".to_string()
        ]
    );
    assert_eq!(exported_timestamps(&export_pq), vec![T0, T1, T2]);
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
        export::CatalogWindow::default(),
        BASE_NS,
    )
    .await
    .expect_err("metrics export is refused");
    assert_eq!(
        err.to_string(),
        export::unsupported_signal_message(SignalArg::Metrics).expect("metrics is unsupported")
    );
}
