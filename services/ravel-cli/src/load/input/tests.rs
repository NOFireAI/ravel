use super::*;
use crate::load::test_support::*;

/// A `ChunkReader` over the input bytes that counts Parquet footer parses.
///
/// Every metadata parse issues exactly one `get_read` at `len - 8`: that
/// eight-byte footer tail carries the metadata length and the `PAR1` magic,
/// and `parse_metadata` reads it before anything else (parquet
/// `file::metadata::reader`). A data-page reader built from already-parsed
/// metadata (`new_with_metadata`) never touches that tail. Counting reads at
/// that offset therefore counts footer parses and nothing else.
struct CountingReader {
    inner: bytes::Bytes,
    footer_reads: Arc<std::sync::atomic::AtomicUsize>,
}

impl parquet::file::reader::Length for CountingReader {
    fn len(&self) -> u64 {
        self.inner.len() as u64
    }
}

impl parquet::file::reader::ChunkReader for CountingReader {
    type T = <bytes::Bytes as parquet::file::reader::ChunkReader>::T;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        if start == self.inner.len() as u64 - parquet::file::FOOTER_SIZE as u64 {
            self.footer_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
        parquet::file::reader::ChunkReader::get_read(&self.inner, start)
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        parquet::file::reader::ChunkReader::get_bytes(&self.inner, start, length)
    }
}

/// An [`InputReaders`] that hands out [`CountingReader`]s over the same file
/// bytes, all sharing one footer-parse counter. Every `open` (the initial
/// metadata read plus each stride cursor's data reader) increments the same
/// counter, so `footer_reads` is the total footer parses for the whole load
/// setup.
struct CountingInput {
    bytes: bytes::Bytes,
    footer_reads: Arc<std::sync::atomic::AtomicUsize>,
}

impl CountingInput {
    fn new(pq: &Path) -> Self {
        let bytes = bytes::Bytes::from(std::fs::read(pq).expect("read fixture bytes"));
        Self {
            bytes,
            footer_reads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn footer_reads(&self) -> usize {
        self.footer_reads.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl InputReaders for CountingInput {
    type Reader = CountingReader;

    fn open(&self) -> Result<CountingReader, LoadError> {
        Ok(CountingReader {
            inner: self.bytes.clone(),
            footer_reads: Arc::clone(&self.footer_reads),
        })
    }
}

/// A Parquet fixture forced to `groups` row groups (one row each), so a load
/// over it opens one stride cursor per row group. Returns the temp dir (kept
/// alive), the path, and a mapping that reads the string column as a resource
/// attribute.
fn multi_row_group_fixture(groups: usize) -> (tempfile::TempDir, std::path::PathBuf, Mapping) {
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("groups.parquet");
    let ts: Vec<i64> = (0..groups as i64).map(|k| NOW_NS + k).collect();
    let svc: Vec<&str> = (0..groups).map(|k| ["api", "web"][k % 2]).collect();
    let b = batch(vec![("ts", i64_col(ts)), ("svc", str_col(svc))]);
    let file = std::fs::File::create(&pq).expect("create parquet");
    // One row per row group: `groups` rows written with a max group size of 1
    // flush a fresh row group each row.
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .build();
    let mut w = ArrowWriter::try_new(file, b.schema(), Some(props)).expect("arrow writer");
    w.write(&b).expect("write batch");
    w.close().expect("close writer");

    let mut m = base_mapping();
    m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
    (dir, pq, m)
}

/// The load setup parses the input's Parquet footer exactly once, no matter
/// how many stride cursors it opens.
///
/// The counter is the footer-tail read (`CountingReader`). The fixture has 8
/// row groups and the load requests 8 read cursors, so
/// `resolve_read_cursors` gives one cursor per row group; every one of them
/// takes the shared `ArrowReaderMetadata` through the `new_with_metadata`
/// builder line in `open_stride_cursors`, which is what holds the count to
/// exactly one. Flip that builder to `try_new`/`try_new_with_options` and
/// the count becomes `cursors + 1`; stop sharing the metadata with
/// `row_group_row_counts` and `load_reader_schema` too and it becomes
/// `cursors + 2`.
#[test]
fn load_setup_parses_the_footer_once() {
    const GROUPS: usize = 8;
    let (_dir, pq, _m) = multi_row_group_fixture(GROUPS);
    let source = CountingInput::new(&pq);

    // The exact setup sequence `run_load` runs, driven through the counting
    // input instead of a file on disk.
    let metadata = read_input_metadata(&source).expect("read metadata");
    let row_group_lens = row_group_row_counts(&metadata);
    assert_eq!(
        row_group_lens.len(),
        GROUPS,
        "the fixture is forced to one row group per row"
    );
    let cursor_count = resolve_read_cursors(Some(8), 4, row_group_lens.len());
    assert_eq!(
        cursor_count, GROUPS,
        "8 requested cursors over 8 row groups gives 8 cursors"
    );
    let cursors = open_stride_cursors(
        &source,
        &metadata,
        &row_group_lens,
        cursor_count,
        1024,
        1024,
    )
    .expect("cursors");
    assert_eq!(cursors.len(), GROUPS, "one cursor per row group");

    assert_eq!(
        source.footer_reads(),
        1,
        "the whole load setup parses the footer exactly once; before #773 it \
             parsed cursors + 2 = {} times",
        cursor_count + 2
    );
}

/// #773: sharing the parsed footer changes nothing the load writes. The same
/// fixture loaded through the (changed) stride-cursor path produces the exact
/// same rows, objects, and columnar batches it did before.
#[tokio::test]
async fn shared_footer_load_output_is_unchanged() {
    use ravel_object_store::memory::MemoryStore;

    const GROUPS: usize = 8;
    let (_dir, pq, m) = multi_row_group_fixture(GROUPS);

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load(
        Arc::clone(&store),
        &pq,
        "acme",
        &m,
        4,
        1,
        Some(8),
        1,
        NOW_NS,
        Arc::new(FixedClock(NOW_NS)),
    )
    .await
    .expect("load succeeds");

    // Exact figures, pre-registered from the fixture shape: 8 rows, and with
    // batch_rows == 1 one columnar batch (hence one RLOG object) per row.
    assert_eq!(report.rows_processed, GROUPS as u64, "every row is written");
    assert_eq!(
        report.columnar_batches_built, GROUPS as u64,
        "batch_rows == 1 builds one columnar batch per row"
    );
    assert_eq!(
        report.objects_written(),
        GROUPS,
        "one shard flush per batch is one object per row"
    );
}

/// The encoding of every DATA page recorded for `column`'s chunk in each row
/// group of `pq`, read out of the footer's page encoding statistics. Used to
/// prove whether the writer dictionary-encoded a column or fell back to
/// plain, rather than assuming it: the chunk-level `encodings` list shows
/// `RLE_DICTIONARY` in both cases, since a fallback keeps the pages it wrote
/// before overflowing.
fn data_page_encodings(pq: &Path, column: &str) -> Vec<Vec<parquet::basic::Encoding>> {
    let f = std::fs::File::open(pq).expect("open parquet");
    let builder = ParquetRecordBatchReaderBuilder::try_new(f).expect("reader builder");
    let md = builder.metadata();
    let leaf = md
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|c| c.path().parts().len() == 1 && c.path().parts()[0] == *column)
        .expect("column is a top-level leaf");
    md.row_groups()
        .iter()
        .map(|rg| {
            rg.column(leaf)
                .page_encoding_stats_mask()
                .expect("the footer records page encoding statistics")
                .encodings()
                .collect()
        })
        .collect()
}

/// #660: a plain `StringArray` with repeated values, written by
/// `ArrowWriter` (which dictionary-encodes `BYTE_ARRAY` by default), now
/// comes back through the loader's reader as a `Dictionary` and reaches the
/// `StrColumnDict` fast path with the file's exact distinct set.
///
/// This deliberately flips #605's expectation. Before the loader supplied a
/// reader schema, the embedded Arrow schema said Utf8, arrow-rs fused the
/// Parquet dictionary away, and the column took the plain per-row path;
/// that is exactly what the `loader_schema = false` half still shows, and it
/// is what the whole test asserted before this change. Its red form is the
/// `assert_eq!` on `on.column(cat_idx).data_type()`: against the pre-#660
/// reader it reads `Utf8` where `Dictionary(Int32, Utf8)` is expected.
///
/// Prove-the-test: confirmed by making `load_reader_schema` return `None`
/// unconditionally, which is exactly the pre-#660 reader. That assertion
/// tripped with `left: Utf8, right: Dictionary(Int32, Utf8)`.
#[test]
fn repeated_value_string_column_reaches_the_dictionary_path() {
    const ROWS: usize = 1_000;
    const DISTINCT: usize = 3;
    let values = ["alpha", "beta", "gamma"];

    let ts: Vec<i64> = (0..ROWS as i64).map(|i| NOW_NS + i).collect();
    let cat: Vec<&str> = (0..ROWS).map(|i| values[i % DISTINCT]).collect();
    let b = batch(vec![("ts", i64_col(ts)), ("cat", str_col(cat))]);
    let (_dir, pq) = write_parquet(&b);

    // The premise: the writer really did dictionary-encode every data page.
    let encodings = data_page_encodings(&pq, "cat");
    assert_eq!(encodings.len(), 1, "one row group");
    assert!(
        !encodings[0].is_empty() && encodings[0].iter().copied().all(is_dictionary_encoding),
        "the writer dictionary-encoded every data page of `cat`: {:?}",
        encodings[0]
    );

    let mut m = base_mapping();
    m.attributes = vec![attr("cat", "cat", ColType::Str)];

    let on = read_parquet(&pq, true);
    let cat_idx = on.schema().index_of("cat").expect("cat present");
    assert_eq!(
        on.column(cat_idx).data_type(),
        &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        "the loader's reader yields the file's dictionary for a repeated-value string column"
    );

    let col_on = assert_paths_match(&on, &m);
    assert_eq!(col_on.num_rows, ROWS, "every row is built");
    let pos = col_on
        .dyn_columns
        .iter()
        .position(|c| c.name == "cat")
        .expect("cat column");
    let dict = col_dict(&col_on, pos).expect("the column carries a StrColumnDict");
    assert_eq!(
        dict.distinct.len(),
        DISTINCT,
        "the StrColumnDict holds exactly the 3 distinct values"
    );
    assert_eq!(dict.ids.len(), ROWS, "one dictionary id per present cell");

    // The pre-#660 reader on the same file, for contrast: Utf8, plain path.
    let off = read_parquet(&pq, false);
    assert!(
        matches!(off.column(cat_idx).data_type(), DataType::Utf8),
        "plain inference fuses the Parquet dictionary away"
    );
    let col_off = assert_paths_match(&off, &m);
    assert!(
        col_dict(&col_off, pos).is_none(),
        "the plain path attaches no StrColumnDict"
    );
}

/// #660: a unique-per-row string column is left Utf8 and takes the plain
/// path. Its dictionary outgrows the writer's default 1 MiB dictionary page
/// limit, so the writer falls back to plain encoding, and the loader's rule
/// preserves only an encoding the file carries -- it never forces one.
///
/// The fallback is read out of the footer here rather than assumed: if a
/// future writer default kept the column dictionary-encoded, the first
/// assertion fails instead of the test silently proving nothing.
///
/// Prove-the-test: confirmed by making `chunk_is_dictionary_encoded` return
/// `true` unconditionally, the shape of the mistake this guards against. The
/// `load_reader_schema(&pq).is_none()` assertion tripped.
#[test]
fn unique_per_row_string_column_stays_plain() {
    const ROWS: usize = 8_000;

    let ts: Vec<i64> = (0..ROWS as i64).map(|i| NOW_NS + i).collect();
    // ~256 bytes per value, so the dictionary passes 1 MiB well before the
    // last row and the writer falls back.
    let owned: Vec<String> = (0..ROWS).map(|i| format!("{i:0>256}")).collect();
    let uniq: Vec<&str> = owned.iter().map(String::as_str).collect();
    let b = batch(vec![("ts", i64_col(ts)), ("uniq", str_col(uniq))]);
    let (_dir, pq) = write_parquet(&b);

    let encodings = data_page_encodings(&pq, "uniq");
    assert_eq!(encodings.len(), 1, "one row group");
    assert!(
        encodings[0].iter().any(|e| !is_dictionary_encoding(*e)),
        "the writer's dictionary overflowed and it fell back to plain data pages: {:?}",
        encodings[0]
    );

    // The derivation leaves the column alone, so no schema is supplied at
    // all for this file.
    assert!(
        reader_schema_for(&pq).is_none(),
        "no column qualifies, so the loader opens the reader with default options"
    );

    let on = read_parquet(&pq, true);
    let idx = on.schema().index_of("uniq").expect("uniq present");
    assert!(
        matches!(on.column(idx).data_type(), DataType::Utf8),
        "a column the file does not dictionary-encode stays Utf8"
    );

    let mut m = base_mapping();
    m.attributes = vec![attr("uniq", "uniq", ColType::Str)];
    let col = build_columnar_or_panic(&on, &m);
    assert_eq!(col.num_rows, ROWS, "every row is built");
    let pos = col
        .dyn_columns
        .iter()
        .position(|c| c.name == "uniq")
        .expect("uniq column");
    assert!(
        col_dict(&col, pos).is_none(),
        "no StrColumnDict is built for a plain column"
    );
}

/// #660: the rule is scoped to string columns. `ArrowWriter`
/// dictionary-encodes a low-cardinality `Int64` column too, and that column
/// must keep the type the reader infers.
#[test]
fn dictionary_encoded_non_string_column_keeps_its_type() {
    const ROWS: usize = 1_000;
    const DISTINCT: i64 = 4;

    let ts: Vec<i64> = (0..ROWS as i64).map(|i| NOW_NS + i).collect();
    let nums: Vec<i64> = (0..ROWS as i64).map(|i| i % DISTINCT).collect();
    let cat: Vec<&str> = (0..ROWS).map(|i| ["a", "b"][i % 2]).collect();
    let b = batch(vec![
        ("ts", i64_col(ts)),
        ("num", i64_col(nums)),
        ("cat", str_col(cat)),
    ]);
    let (_dir, pq) = write_parquet(&b);

    // The premise: the Int64 column really is dictionary encoded in the file.
    let encodings = data_page_encodings(&pq, "num");
    assert_eq!(encodings.len(), 1, "one row group");
    assert!(
        !encodings[0].is_empty() && encodings[0].iter().copied().all(is_dictionary_encoding),
        "the writer dictionary-encoded every data page of `num`: {:?}",
        encodings[0]
    );

    // A schema IS supplied (the string column qualifies), so this proves the
    // rule skipped `num` rather than that it never ran.
    let schema =
        reader_schema_for(&pq).expect("the string column qualifies, so a schema is supplied");
    assert_eq!(
        schema
            .field_with_name("num")
            .expect("num field")
            .data_type(),
        &DataType::Int64,
        "a dictionary-encoded non-string column keeps its inferred type"
    );
    assert_eq!(
        schema
            .field_with_name("cat")
            .expect("cat field")
            .data_type(),
        &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        "the string column beside it is retyped"
    );
    assert_eq!(
        schema.field_with_name("ts").expect("ts field").data_type(),
        &DataType::Int64,
        "the ts column keeps its inferred type"
    );

    let on = read_parquet(&pq, true);
    assert_eq!(
        on.column(on.schema().index_of("num").expect("num present"))
            .data_type(),
        &DataType::Int64,
        "the Int64 column is read back as Int64"
    );
    assert_eq!(on.num_rows(), ROWS, "every row is read back");
}
