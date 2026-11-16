//! Acceptance coverage for the Parquet bloom filters `ravel-cli export`
//! writes (issue #3229).
//!
//! Drives the real load and export entry points in-process against a shared
//! `MemoryStore`, for the reason `tests/export_logs.rs` gives, and reads every
//! exported file back with the parquet crate's own reader and its bloom filter
//! API, so the assertions are about what is in the file, not about what the
//! writer was asked for.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BinaryArray, FixedSizeBinaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::properties::ReaderProperties;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::{ReadOptionsBuilder, SerializedFileReader};
use ravel_cli::export::{self, BloomColumns};
use ravel_cli::load::{self, SpansMapping};
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_ingest::Clock;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::instrument::{InstrumentedStore, StoreMetricsSnapshot};
use ravel_object_store::memory::MemoryStore;

const BASE_NS: i64 = 1_700_000_000_000_000_000; // 2023-11-14T22:13:20Z
const ONE_SEC_NS: i64 = 1_000_000_000;
const T0: i64 = BASE_NS;
const T1: i64 = BASE_NS + ONE_SEC_NS;
/// The clock every load and export runs at: after every event time above,
/// well inside the future-skew bound.
const LOAD_NS: i64 = BASE_NS + 10 * ONE_SEC_NS;

/// Spans in the shared fixture: three per trace, the first of each a root.
const SPAN_COUNT: usize = 3_000;
/// Absent ids probed per filtered column.
const ABSENT_PROBES: usize = 200_000;
/// Highest absent-id hit rate accepted per column: the 1% target plus room
/// for sampling error and the variance of a blocked filter.
const MAX_ABSENT_HIT_RATE: f64 = 0.015;
/// Seed of the absent-id probes, distinct from [`span_rows`]'s.
const ABSENT_SEED: u64 = 0x5eed_ab5e;

/// Output column names that differ from every default spelling, so an
/// implementation that hard-codes `trace_id`/`span_id` writes no filter here.
const TRACE_COL: &str = "tid_out";
const SPAN_COL: &str = "sid_out";
const PARENT_COL: &str = "psid_out";

const SPANS_MAPPING: &str = r#"
[spans]
trace_id_column       = "tid_out"
span_id_column        = "sid_out"
parent_span_id_column = "psid_out"
name_column           = "op"
start_ts_column       = "start_ns"
start_ts_unit         = "nanos"
end_ts_column         = "end_ns"
end_ts_unit           = "nanos"
"#;

struct FixedClock(i64);
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

/// splitmix64: a seeded, dependency-free source of ids, so a failure replays.
struct Ids(u64);
impl Ids {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn trace_id(&mut self) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[..8].copy_from_slice(&self.next_u64().to_le_bytes());
        id[8..].copy_from_slice(&self.next_u64().to_le_bytes());
        id
    }
    fn span_id(&mut self) -> [u8; 8] {
        // A zero span id is not a valid id; the chance is negligible but
        // the fixture must not depend on it.
        loop {
            let id = self.next_u64().to_le_bytes();
            if id != [0u8; 8] {
                return id;
            }
        }
    }
}

struct SpanRow {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent: Option<[u8; 8]>,
    start_ns: i64,
}

fn span_rows() -> Vec<SpanRow> {
    let mut ids = Ids(0x3229);
    let mut rows = Vec::with_capacity(SPAN_COUNT);
    let mut trace_id = [0u8; 16];
    let mut previous = [0u8; 8];
    for i in 0..SPAN_COUNT {
        let root = i % 3 == 0;
        if root {
            trace_id = ids.trace_id();
        }
        let span_id = ids.span_id();
        rows.push(SpanRow {
            trace_id,
            span_id,
            parent: (!root).then_some(previous),
            start_ns: T0 + i as i64 * 1_000,
        });
        previous = span_id;
    }
    rows
}

fn spans_mapping() -> SpansMapping {
    load::parse_spans_mapping(SPANS_MAPPING).expect("valid spans mapping")
}

fn write_source(path: &Path, batch: &RecordBatch) {
    let file = File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

/// A store holding [`span_rows`] for tenant `acme`, loaded under
/// [`SPANS_MAPPING`].
async fn loaded_spans_store(dir: &Path) -> Arc<dyn ObjectStoreBackend> {
    let rows = span_rows();
    let bin = |values: Vec<Option<Vec<u8>>>| -> ArrayRef {
        let refs: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
        Arc::new(BinaryArray::from(refs))
    };
    let batch = RecordBatch::try_from_iter(vec![
        (
            TRACE_COL,
            bin(rows.iter().map(|r| Some(r.trace_id.to_vec())).collect()),
        ),
        (
            SPAN_COL,
            bin(rows.iter().map(|r| Some(r.span_id.to_vec())).collect()),
        ),
        (
            PARENT_COL,
            bin(rows.iter().map(|r| r.parent.map(|p| p.to_vec())).collect()),
        ),
        (
            "op",
            Arc::new(StringArray::from(vec!["op"; rows.len()])) as ArrayRef,
        ),
        (
            "start_ns",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.start_ns).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "end_ns",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.start_ns + 500).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .expect("source batch");
    let path = dir.join("source-spans.parquet");
    write_source(&path, &batch);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load::load_spans(
        Arc::clone(&store),
        &path,
        "acme",
        &spans_mapping(),
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        LOAD_NS,
        Arc::new(FixedClock(LOAD_NS)),
    )
    .await
    .expect("the spans load succeeds");
    assert_eq!(report.rows_processed, SPAN_COUNT as u64);
    store
}

async fn export_spans(
    store: &Arc<dyn ObjectStoreBackend>,
    out: &Path,
    bloom: &BloomColumns,
) -> anyhow::Result<export::SpansExportReport> {
    export::export_spans(
        Arc::clone(store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        T0,
        T1,
        &spans_mapping(),
        out,
        1,
        None,
        LOAD_NS,
        bloom,
    )
    .await
}

fn open_with_filters(path: &Path) -> SerializedFileReader<File> {
    let options = ReadOptionsBuilder::new()
        .with_reader_properties(
            ReaderProperties::builder()
                .set_read_bloom_filter(true)
                .build(),
        )
        .build();
    SerializedFileReader::new_with_options(File::open(path).expect("open export"), options)
        .expect("parquet reader")
}

/// Per row group, the set of columns that carry a bloom filter, and the full
/// column list, so a caller asserts presence and absence per column.
fn filtered_columns(path: &Path) -> (Vec<String>, Vec<BTreeSet<String>>) {
    let reader = open_with_filters(path);
    let schema = reader.metadata().file_metadata().schema_descr_ptr();
    let columns: Vec<String> = (0..schema.num_columns())
        .map(|i| schema.column(i).path().string())
        .collect();
    let mut per_group = Vec::new();
    for rg in 0..reader.num_row_groups() {
        let group = reader.get_row_group(rg).expect("row group");
        per_group.push(
            columns
                .iter()
                .enumerate()
                .filter(|(i, _)| group.get_column_bloom_filter(*i).is_some())
                .map(|(_, name)| name.clone())
                .collect(),
        );
    }
    (columns, per_group)
}

/// Asserts every row group of `path` carries a filter on exactly `expected`,
/// column by column, and that the file has at least one row group.
fn assert_filters_exactly(path: &Path, expected: &[&str]) {
    let (columns, per_group) = filtered_columns(path);
    assert!(
        !per_group.is_empty(),
        "the export has at least one row group"
    );
    for name in expected {
        assert!(
            columns.iter().any(|c| c == name),
            "{name} is an output column"
        );
    }
    for (rg, filtered) in per_group.iter().enumerate() {
        for column in &columns {
            let want = expected.contains(&column.as_str());
            assert_eq!(
                filtered.contains(column),
                want,
                "row group {rg} column {column}: bloom filter present must be {want}"
            );
        }
    }
}

fn column_index(reader: &SerializedFileReader<File>, name: &str) -> usize {
    let schema = reader.metadata().file_metadata().schema_descr_ptr();
    (0..schema.num_columns())
        .find(|i| schema.column(*i).path().string() == name)
        .unwrap_or_else(|| panic!("no column {name}"))
}

/// The non-null values of a `FixedSizeBinary` column in row group `rg`.
fn row_group_ids(path: &Path, rg: usize, column: &str) -> Vec<Vec<u8>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).expect("open"))
        .expect("reader builder")
        .with_row_groups(vec![rg])
        .build()
        .expect("reader");
    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.expect("batch");
        let array = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("no column {column}"))
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("FixedSizeBinary id column");
        for i in 0..array.len() {
            if array.is_valid(i) {
                values.push(array.value(i).to_vec());
            }
        }
    }
    values
}

#[tokio::test]
async fn a_spans_export_filters_exactly_the_mapped_id_columns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = loaded_spans_store(dir.path()).await;
    let out = dir.path().join("out.parquet");
    let report = export_spans(&store, &out, &BloomColumns::Default)
        .await
        .expect("export");
    assert_eq!(report.rows_written, SPAN_COUNT as u64);
    assert_filters_exactly(&out, &[TRACE_COL, SPAN_COL, PARENT_COL]);
}

#[tokio::test]
async fn every_exported_id_passes_its_own_row_groups_filter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = loaded_spans_store(dir.path()).await;
    let out = dir.path().join("out.parquet");
    export_spans(&store, &out, &BloomColumns::Default)
        .await
        .expect("export");
    let reader = open_with_filters(&out);
    let mut checked: BTreeMap<&str, usize> = BTreeMap::new();
    for rg in 0..reader.num_row_groups() {
        let group = reader.get_row_group(rg).expect("row group");
        for column in [TRACE_COL, SPAN_COL, PARENT_COL] {
            let filter = group
                .get_column_bloom_filter(column_index(&reader, column))
                .unwrap_or_else(|| panic!("row group {rg} has a filter on {column}"));
            for value in row_group_ids(&out, rg, column) {
                assert!(
                    filter.check(&value[..]),
                    "row group {rg} column {column}: stored id {value:02x?} fails its filter"
                );
                *checked.entry(column).or_default() += 1;
            }
        }
    }
    // Every span has a trace and span id; two of every three have a parent.
    assert_eq!(checked.get(TRACE_COL), Some(&SPAN_COUNT));
    assert_eq!(checked.get(SPAN_COL), Some(&SPAN_COUNT));
    assert_eq!(checked.get(PARENT_COL), Some(&(SPAN_COUNT / 3 * 2)));
}

#[tokio::test]
async fn absent_ids_hit_each_filter_at_no_more_than_the_band() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = loaded_spans_store(dir.path()).await;
    let out = dir.path().join("out.parquet");
    export_spans(&store, &out, &BloomColumns::Default)
        .await
        .expect("export");

    let rows = span_rows();
    let present_traces: HashSet<[u8; 16]> = rows.iter().map(|r| r.trace_id).collect();
    let present_spans: HashSet<[u8; 8]> = rows.iter().map(|r| r.span_id).collect();

    let reader = open_with_filters(&out);
    assert_eq!(
        reader.num_row_groups(),
        1,
        "the fixture fits one row group, so one filter per column decides the rate"
    );
    let group = reader.get_row_group(0).expect("row group");
    let mut ids = Ids(ABSENT_SEED);
    let mut rates: BTreeMap<&str, f64> = BTreeMap::new();
    for column in [TRACE_COL, SPAN_COL, PARENT_COL] {
        let filter = group
            .get_column_bloom_filter(column_index(&reader, column))
            .expect("filter");
        let mut probes = 0usize;
        let mut hits = 0usize;
        while probes < ABSENT_PROBES {
            let hit = if column == TRACE_COL {
                let id = ids.trace_id();
                if present_traces.contains(&id) {
                    continue;
                }
                filter.check(&id[..])
            } else {
                let id = ids.span_id();
                if present_spans.contains(&id) {
                    continue;
                }
                filter.check(&id[..])
            };
            probes += 1;
            hits += usize::from(hit);
        }
        let rate = hits as f64 / probes as f64;
        eprintln!("absent-id hit rate {column}: {hits}/{probes} = {rate:.5}");
        assert!(
            rates.insert(column, rate).is_none(),
            "{column} rate computed once"
        );
    }
    assert_eq!(
        rates.keys().copied().collect::<Vec<_>>(),
        {
            let mut want = vec![TRACE_COL, SPAN_COL, PARENT_COL];
            want.sort_unstable();
            want
        },
        "exactly one rate per filtered column"
    );
    for (column, rate) in &rates {
        assert!(
            *rate <= MAX_ABSENT_HIT_RATE,
            "{column}: absent-id hit rate {rate} exceeds {MAX_ABSENT_HIT_RATE}"
        );
    }
}

/// Rows per row group in the folding fixture.
const FOLD_ROWS: usize = 4_096;

/// Writes two row groups through the export's own writer properties: one
/// holding `first` trace ids, one holding `second`, and returns each row
/// group's trace id filter length in bytes.
fn fold_fixture_filter_bytes(first: &[[u8; 16]], second: &[[u8; 16]]) -> Vec<i32> {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fold.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        TRACE_COL,
        DataType::FixedSizeBinary(16),
        false,
    )]));
    let props = export::export_writer_properties(&[TRACE_COL.to_string()]);
    let mut writer = ArrowWriter::try_new(
        File::create(&path).expect("create"),
        Arc::clone(&schema),
        Some(props),
    )
    .expect("writer");
    for ids in [first, second] {
        let array = FixedSizeBinaryArray::try_from_iter(ids.iter()).expect("ids");
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(array) as ArrayRef])
            .expect("batch");
        writer.write(&batch).expect("write");
        writer.flush().expect("end the row group");
    }
    writer.close().expect("close");
    let reader = open_with_filters(&path);
    assert_eq!(reader.num_row_groups(), 2);
    (0..2)
        .map(|rg| {
            reader
                .metadata()
                .row_group(rg)
                .column(0)
                .bloom_filter_length()
                .unwrap_or_else(|| panic!("row group {rg} has a filter"))
        })
        .collect()
}

#[test]
fn a_single_trace_row_group_folds_its_filter_far_below_an_all_distinct_one() {
    let mut ids = Ids(0xf01d);
    let single = vec![ids.trace_id(); FOLD_ROWS];
    let distinct: Vec<[u8; 16]> = (0..FOLD_ROWS).map(|_| ids.trace_id()).collect();
    let bytes = fold_fixture_filter_bytes(&single, &distinct);
    eprintln!(
        "trace id filter bytes: single-trace {}, all-distinct {}",
        bytes[0], bytes[1]
    );
    assert!(
        i64::from(bytes[0]) * 16 <= i64::from(bytes[1]),
        "single-trace filter {} bytes is not at least 16x smaller than all-distinct {} bytes",
        bytes[0],
        bytes[1]
    );
}

#[tokio::test]
async fn bloom_columns_none_writes_no_filter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = loaded_spans_store(dir.path()).await;
    let out = dir.path().join("out.parquet");
    export_spans(&store, &out, &BloomColumns::None)
        .await
        .expect("export");
    assert_filters_exactly(&out, &[]);
}

#[tokio::test]
async fn bloom_columns_naming_one_column_writes_exactly_that_filter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = loaded_spans_store(dir.path()).await;
    let out = dir.path().join("out.parquet");
    export_spans(
        &store,
        &out,
        &BloomColumns::Named(vec![SPAN_COL.to_string()]),
    )
    .await
    .expect("export");
    assert_filters_exactly(&out, &[SPAN_COL]);
}

#[tokio::test]
async fn an_unknown_bloom_column_is_refused_before_any_store_request() {
    let dir = tempfile::tempdir().expect("tempdir");
    let instrumented = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let metrics = instrumented.metrics();
    let store: Arc<dyn ObjectStoreBackend> = instrumented;
    let out = dir.path().join("out.parquet");
    let err = export_spans(
        &store,
        &out,
        &BloomColumns::Named(vec![SPAN_COL.to_string(), "trace_id".to_string()]),
    )
    .await
    .expect_err("an unknown column is refused");
    assert_eq!(
        err.to_string(),
        "--parquet-bloom-columns names \"trace_id\", which is not an output column of this \
         export; its output columns are: tid_out, sid_out, psid_out, op, start_ns, end_ns"
    );
    assert_eq!(metrics.snapshot(), StoreMetricsSnapshot::default());
    assert!(!out.exists(), "a refused export writes no file");
}

/// A map column has no single leaf to filter, so naming one is refused
/// rather than accepted and silently written without a filter.
#[tokio::test]
async fn a_nested_bloom_column_is_refused_before_any_store_request() {
    let dir = tempfile::tempdir().expect("tempdir");
    let instrumented = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let metrics = instrumented.metrics();
    let store: Arc<dyn ObjectStoreBackend> = instrumented;
    let mapping =
        load::parse_spans_mapping(&format!("{SPANS_MAPPING}attrs_map_column = \"attrs\"\n"))
            .expect("valid spans mapping");
    let out = dir.path().join("out.parquet");
    let err = export::export_spans(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        T0,
        T1,
        &mapping,
        &out,
        1,
        None,
        LOAD_NS,
        &BloomColumns::Named(vec!["attrs".to_string()]),
    )
    .await
    .expect_err("a nested column is refused");
    assert_eq!(
        err.to_string(),
        "--parquet-bloom-columns names \"attrs\", a nested column; a bloom filter is written \
         only on a flat column"
    );
    assert_eq!(metrics.snapshot(), StoreMetricsSnapshot::default());
}

#[test]
fn the_flag_reads_none_alone_and_refuses_it_beside_a_name() {
    assert_eq!(
        BloomColumns::from_flag(None).unwrap(),
        BloomColumns::Default
    );
    assert_eq!(
        BloomColumns::from_flag(Some(vec!["none".into()])).unwrap(),
        BloomColumns::None
    );
    assert_eq!(
        BloomColumns::from_flag(Some(vec!["a".into(), "b".into()])).unwrap(),
        BloomColumns::Named(vec!["a".into(), "b".into()])
    );
    let err = BloomColumns::from_flag(Some(vec!["none".into(), "a".into()]))
        .expect_err("none beside a name is refused");
    assert!(
        err.to_string().contains("none must be the only value"),
        "{err}"
    );
}

/// Loads two log records, each with a trace and span id, into `acme` from a
/// source with `ts`, `body`, `log_tid` and `log_sid` columns.
async fn loaded_logs_store(dir: &Path, mapping_text: &str) -> Arc<dyn ObjectStoreBackend> {
    let mut columns: Vec<(&str, ArrayRef)> = vec![
        ("ts", Arc::new(Int64Array::from(vec![T0, T0 + 1]))),
        ("body", Arc::new(StringArray::from(vec!["a", "b"]))),
    ];
    let mut ids = Ids(0x1095);
    let (t1, t2) = (ids.trace_id(), ids.trace_id());
    let (s1, s2) = (ids.span_id(), ids.span_id());
    columns.push((
        "log_tid",
        Arc::new(BinaryArray::from(vec![&t1[..], &t2[..]])),
    ));
    columns.push((
        "log_sid",
        Arc::new(BinaryArray::from(vec![&s1[..], &s2[..]])),
    ));
    let batch = RecordBatch::try_from_iter(columns).expect("source batch");
    let path = dir.join("source-logs.parquet");
    write_source(&path, &batch);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load::load(
        Arc::clone(&store),
        &path,
        "acme",
        &load::parse_mapping(mapping_text).expect("logs mapping"),
        1,
        10_000,
        None,
        1,
        LOAD_NS,
        Arc::new(FixedClock(LOAD_NS)),
    )
    .await
    .expect("the logs load succeeds");
    assert_eq!(report.rows_processed, 2);
    store
}

async fn export_logs(
    store: &Arc<dyn ObjectStoreBackend>,
    mapping_text: &str,
    out: &Path,
) -> export::ExportReport {
    export::export_logs(
        Arc::clone(store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        T0,
        T1,
        &load::parse_mapping(mapping_text).expect("logs mapping"),
        out,
        1,
        None,
        LOAD_NS,
        &BloomColumns::Default,
    )
    .await
    .expect("the logs export succeeds")
}

const LOGS_WITH_IDS: &str = "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n\
     trace_id_column = \"log_tid\"\nspan_id_column = \"log_sid\"\n";
const LOGS_WITHOUT_IDS: &str = "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"body\"\n";

#[tokio::test]
async fn a_logs_export_filters_the_mapped_trace_and_span_id_columns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = loaded_logs_store(dir.path(), LOGS_WITH_IDS).await;
    let out = dir.path().join("out.parquet");
    let report = export_logs(&store, LOGS_WITH_IDS, &out).await;
    assert_eq!(report.rows_written, 2);
    assert_filters_exactly(&out, &["log_tid", "log_sid"]);
}

#[tokio::test]
async fn a_logs_export_whose_mapping_sets_no_id_column_writes_no_filter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = loaded_logs_store(dir.path(), LOGS_WITH_IDS).await;
    let out = dir.path().join("out.parquet");
    let report = export_logs(&store, LOGS_WITHOUT_IDS, &out).await;
    assert_eq!(report.rows_written, 2);
    assert_filters_exactly(&out, &[]);
}

#[tokio::test]
async fn a_metrics_export_writes_no_filter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mapping_text = "[metrics]\nname_column = \"name\"\nvalue_column = \"value\"\n\
                        ts_column = \"ts\"\nts_unit = \"nanos\"\n";
    let mapping = load::parse_metrics_mapping(mapping_text).expect("metrics mapping");
    let batch = RecordBatch::try_from_iter(vec![
        (
            "name",
            Arc::new(StringArray::from(vec!["up", "up"])) as ArrayRef,
        ),
        (
            "ts",
            Arc::new(Int64Array::from(vec![T0, T0 + 1])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(arrow::array::Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
        ),
    ])
    .expect("source batch");
    let path = dir.path().join("source-metrics.parquet");
    write_source(&path, &batch);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load::load_metrics(
        Arc::clone(&store),
        &path,
        "acme",
        &mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        LOAD_NS,
        Arc::new(FixedClock(LOAD_NS)),
    )
    .await
    .expect("the metrics load succeeds");
    let out = dir.path().join("out.parquet");
    let report = export::export_metrics(
        Arc::clone(&store),
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        T0,
        T1,
        &mapping,
        &out,
        1,
        None,
        LOAD_NS,
        &BloomColumns::Default,
    )
    .await
    .expect("the metrics export succeeds");
    assert_eq!(report.rows_written, 2);
    assert_filters_exactly(&out, &[]);
}
