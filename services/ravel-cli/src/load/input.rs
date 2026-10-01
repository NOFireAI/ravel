//! Opening the Parquet input: metadata, row-group partitioning across read
//! cursors, and the dictionary-preserving reader schema.

use super::*;

/// Opens a fresh reader over the load input for each independent read. The
/// stride cursors read disjoint row-group partitions concurrently, so each
/// needs its own reader (its own file offset), but they all share one parsed
/// footer: only the data pages are re-read, never the metadata (issue #773).
pub(super) trait InputReaders {
    type Reader: parquet::file::reader::ChunkReader + 'static;
    fn open(&self) -> Result<Self::Reader, LoadError>;
}

/// The production input: a Parquet file on disk. Each `open` is a new file
/// handle over the same path.
pub(super) struct FileInput<'a> {
    pub(super) path: &'a Path,
}

impl InputReaders for FileInput<'_> {
    type Reader = std::fs::File;

    fn open(&self) -> Result<std::fs::File, LoadError> {
        std::fs::File::open(self.path)
            .map_err(|e| LoadError::Setup(format!("failed to open {}: {e}", self.path.display())))
    }
}

/// Parse the input's Parquet footer once and return the metadata every setup
/// site reuses (issue #773). This is the single footer decode per load input;
/// `row_group_row_counts`, `load_reader_schema`, and each stride cursor's
/// builder all take the result rather than re-reading it.
pub(super) fn read_input_metadata<S: InputReaders>(
    source: &S,
) -> Result<ArrowReaderMetadata, LoadError> {
    let reader = source.open()?;
    ArrowReaderMetadata::load(&reader, ArrowReaderOptions::default())
        .map_err(|e| LoadError::Setup(format!("failed to read Parquet metadata: {e}")))
}

/// Read each row group's row count from the already-parsed footer, in
/// row-group order, without decoding any data. Used to size and partition the
/// stride cursors (issue #560) before any reader is opened.
pub(super) fn row_group_row_counts(metadata: &ArrowReaderMetadata) -> Vec<u64> {
    metadata
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as u64)
        .collect()
}

/// Resolve `--read-cursors` (issue #560): absent means auto-sized to
/// `min(shard count, row-group count)`, floored at 1; an explicit value is
/// clamped to `[1, row_group_count.max(1)]` (more cursors than row groups
/// cannot each get a distinct contiguous partition). Zero is rejected by the
/// caller before this is reached, never clamped up silently.
pub(super) fn resolve_read_cursors(
    read_cursors: Option<usize>,
    shards: u32,
    row_group_count: usize,
) -> usize {
    let max_cursors = row_group_count.max(1);
    match read_cursors {
        Some(k) => k.clamp(1, max_cursors),
        None => (shards as usize).min(row_group_count).max(1),
    }
}

/// Split `n` row groups into `k` contiguous, near-even ranges (the first
/// `n % k` ranges get one extra row group). Used to give each stride cursor
/// its own disjoint partition of row groups.
fn partition_row_group_ranges(n: usize, k: usize) -> Vec<std::ops::Range<usize>> {
    let base = n / k;
    let extra = n % k;
    let mut ranges = Vec::with_capacity(k);
    let mut start = 0;
    for i in 0..k {
        let len = base + usize::from(i < extra);
        ranges.push(start..start + len);
        start += len;
    }
    ranges
}

/// The Arrow key type every preserved Parquet dictionary is read back with.
/// Parquet dictionary indices are `i32`, so `Int32` is the exact key width and
/// no narrowing or widening happens on the way in.
pub(super) const DICT_KEY_TYPE: DataType = DataType::Int32;

/// A dictionary data-page encoding: `RLE_DICTIONARY`, or `PLAIN_DICTIONARY` in
/// the pre-2.4 spelling.
fn is_dictionary_encoding(e: parquet::basic::Encoding) -> bool {
    matches!(
        e,
        parquet::basic::Encoding::RLE_DICTIONARY | parquet::basic::Encoding::PLAIN_DICTIONARY
    )
}

/// Is every one of `chunk`'s data pages dictionary encoded?
///
/// The chunk-level `encodings` list cannot answer this. A writer whose
/// dictionary outgrows its page-size limit falls back to plain part way through
/// the chunk, and the result lists `RLE_DICTIONARY` (the pages written before
/// the fallback) alongside `PLAIN` (the ones after) -- which is also what a
/// fully dictionary-encoded chunk lists, because its dictionary page is itself
/// `PLAIN`. The footer's page encoding statistics separate the two: they are
/// per page type, so the data pages can be read on their own.
///
/// When a file records no page statistics at all, this falls back to the
/// chunk-level list. That over-reports a fallback chunk as dictionary encoded
/// rather than under-reporting the ordinary case; the values read back are the
/// same either way, only the per-block work differs.
fn chunk_is_dictionary_encoded(chunk: &parquet::file::metadata::ColumnChunkMetaData) -> bool {
    // The reader condenses the statistics to a data-page-only encoding mask by
    // default, and keeps the full per-page list only when asked to.
    if let Some(mask) = chunk.page_encoding_stats_mask() {
        return data_page_encodings_are_all_dictionary(mask.encodings());
    }
    if let Some(stats) = chunk.page_encoding_stats() {
        return data_page_encodings_are_all_dictionary(
            stats
                .iter()
                .filter(|s| {
                    matches!(
                        s.page_type,
                        parquet::basic::PageType::DATA_PAGE
                            | parquet::basic::PageType::DATA_PAGE_V2
                    )
                })
                .map(|s| s.encoding),
        );
    }
    chunk.encodings().any(is_dictionary_encoding)
}

/// True when `encodings` is non-empty and every encoding in it is a dictionary
/// encoding. Empty means the footer recorded no data page for the chunk, which
/// is not evidence of a dictionary.
fn data_page_encodings_are_all_dictionary(
    encodings: impl Iterator<Item = parquet::basic::Encoding>,
) -> bool {
    let mut seen = false;
    for e in encodings {
        if !is_dictionary_encoding(e) {
            return false;
        }
        seen = true;
    }
    seen
}

/// Derive the Arrow schema the loader drives its data reader with, so that a
/// Parquet file's own string dictionaries survive into the Arrow batches and
/// ADR-0109 decision 3 engages (issue #660).
///
/// The rule, applied per top-level column of `inferred` (the schema the reader
/// would infer on its own, embedded Arrow metadata included):
///
/// - the column is retyped `Dictionary(Int32, Utf8)` when all of: the inferred
///   type is `Utf8`; the column is a top-level Parquet leaf of physical type
///   `BYTE_ARRAY` with the `String`/`UTF8` logical type; and *every* column
///   chunk for it, in every row group, is dictionary encoded on every data page
///   ([`chunk_is_dictionary_encoded`]);
/// - every other column keeps the type the reader would infer, unchanged. That
///   includes a non-string column the writer happened to dictionary-encode
///   (only string columns feed decision 3's per-distinct-value work), a column
///   the embedded Arrow metadata already types as a dictionary, and above all
///   a string column whose chunks are *not* dictionary encoded because the
///   writer's dictionary outgrew its page limit and it fell back to plain, as a
///   unique-per-row column does.
///
/// The rule only preserves an encoding the file already carries; it never
/// forces a dictionary onto a column that has none, which would move per-row
/// work into the reader instead of removing it.
///
/// Returns `None` when no column qualifies, which is the caller's signal to
/// open the reader with default options and infer as before.
fn dictionary_preserving_schema(
    inferred: &SchemaRef,
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Option<SchemaRef> {
    let descr = metadata.file_metadata().schema_descr();
    let row_groups = metadata.row_groups();
    if row_groups.is_empty() {
        return None;
    }

    let mut changed = false;
    let fields: Vec<Field> = inferred
        .fields()
        .iter()
        .map(|field| {
            let f = field.as_ref().clone();
            if *f.data_type() != DataType::Utf8 {
                return f;
            }
            // Only a top-level Parquet leaf (path length 1) maps one-to-one to
            // a top-level Arrow field; anything nested keeps its inferred type.
            let Some(leaf) = descr
                .columns()
                .iter()
                .position(|c| c.path().parts().len() == 1 && c.path().parts()[0] == *f.name())
            else {
                return f;
            };
            let col = descr.column(leaf);
            let is_utf8_byte_array = col.physical_type() == parquet::basic::Type::BYTE_ARRAY
                && (matches!(
                    col.logical_type_ref(),
                    Some(parquet::basic::LogicalType::String)
                ) || col.converted_type() == parquet::basic::ConvertedType::UTF8);
            if !is_utf8_byte_array {
                return f;
            }
            if !row_groups
                .iter()
                .all(|rg| chunk_is_dictionary_encoded(rg.column(leaf)))
            {
                return f;
            }
            changed = true;
            f.with_data_type(DataType::Dictionary(
                Box::new(DICT_KEY_TYPE),
                Box::new(DataType::Utf8),
            ))
        })
        .collect();

    changed.then(|| {
        Arc::new(Schema::new_with_metadata(
            fields,
            inferred.metadata().clone(),
        )) as SchemaRef
    })
}

/// Derive the reader schema [`dictionary_preserving_schema`] describes from the
/// already-parsed footer, or `None` when no column qualifies. The metadata is
/// the shared one from [`read_input_metadata`]; nothing is re-read here.
pub(super) fn load_reader_schema(metadata: &ArrowReaderMetadata) -> Option<SchemaRef> {
    dictionary_preserving_schema(metadata.schema(), metadata.metadata())
}

/// The schema every load of `path` opens its readers with: the
/// dictionary-preserving one [`load_reader_schema`] derives, or `None` when no
/// column qualifies and the reader infers as usual.
///
/// Public because the types a load actually SEES are not the types the file's
/// own schema declares, and a test asserting behaviour on a dictionary-encoded
/// column has to be able to say that the column really reached the loader as a
/// `Dictionary`. Parses the footer the same way the loader does.
pub fn reader_schema_for_path(path: &Path) -> Result<Option<SchemaRef>, LoadError> {
    let metadata = read_input_metadata(&FileInput { path })?;
    Ok(load_reader_schema(&metadata))
}

/// Open one [`BatchReader`](super::logs::BatchReader) per stride cursor (issue #560), each restricted to
/// its own contiguous partition of `parquet_path`'s row groups, with
/// `partition_base` set to that partition's first row's file-absolute index.
/// An empty partition (only possible when `row_group_lens` is empty, the
/// degenerate zero-row-group case, which forces `k == 1`) yields an
/// already-exhausted cursor with no reader opened, rather than asking Parquet
/// to build a reader over zero row groups.
pub(super) fn open_stride_cursors<S: InputReaders>(
    source: &S,
    metadata: &ArrowReaderMetadata,
    row_group_lens: &[u64],
    k: usize,
    batch_rows: usize,
) -> Result<Vec<CursorState>, LoadError> {
    let mut group_file_base = Vec::with_capacity(row_group_lens.len());
    let mut running = 0u64;
    for &len in row_group_lens {
        group_file_base.push(running);
        running += len;
    }

    // Derived once from the shared footer, then applied to every cursor: each
    // cursor reads a disjoint partition of the same file, so they must all
    // agree on the column types (issue #660).
    let reader_schema = load_reader_schema(metadata);

    // The `ArrowReaderMetadata` every cursor's builder is constructed from: the
    // shared footer, with the dictionary-preserving schema applied when one was
    // derived. Building it from the already-parsed metadata (issue #773) means
    // no cursor re-parses the footer; it only opens a reader for the data pages.
    let cursor_metadata = match &reader_schema {
        Some(schema) => ArrowReaderMetadata::try_new(
            Arc::clone(metadata.metadata()),
            ArrowReaderOptions::new().with_schema(Arc::clone(schema)),
        )
        .map_err(|e| LoadError::Setup(format!("failed to apply reader schema: {e}")))?,
        None => metadata.clone(),
    };

    let mut cursors = Vec::with_capacity(k);
    for range in partition_row_group_ranges(row_group_lens.len(), k) {
        if range.is_empty() {
            cursors.push(CursorState {
                reader: None,
                buffered: None,
                partition_base: running,
                consumed: 0,
            });
            continue;
        }
        let partition_base = group_file_base[range.start];
        let file = source.open()?;
        let builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, cursor_metadata.clone());
        let reader = builder
            .with_row_groups(range.collect())
            .with_batch_size(batch_rows)
            .build()
            .map_err(|e| LoadError::Setup(format!("failed to build Parquet reader: {e}")))?;
        cursors.push(CursorState {
            reader: Some(reader),
            buffered: None,
            partition_base,
            consumed: 0,
        });
    }
    Ok(cursors)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests;
