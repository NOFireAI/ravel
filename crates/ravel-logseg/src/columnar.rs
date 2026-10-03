//! A borrowed columnar view over one decoded block (ADR-0099 decision 1).
//!
//! [`crate::BlockScan::next_block`] turns a decoded block into one
//! [`LogRecord`](crate::LogRecord) per surviving row: an owned `String` for
//! `body` and `severity_text`, a cloned STREAM_DIR blob, and a cloned key per
//! present attribute. A caller that is about to build columnar output again
//! (Arrow arrays, in `ravel-sql`) pays for that row form and then throws it
//! away.
//!
//! [`ColumnarBlockView`] is the second exit: the same surviving rows, in the
//! same order, read straight out of the decoded columns.
//!
//! # Accessors only, never the storage type
//!
//! Every method here returns a value or a borrowed slice of *one cell*. None
//! returns the decoded block, and none returns a column's storage vector. That
//! is deliberate and load-bearing: ADR-0099 decision 4 changes how string
//! columns are stored (a dictionary plus ids instead of one `Vec<u8>` per row)
//! without touching a caller, and a view handing out `&[Option<Vec<u8>>]` would
//! either block that change or force it to materialize exactly the allocations
//! it exists to delete.
//!
//! # Row addressing
//!
//! Every `_at`/per-row accessor takes an index into the *surviving* rows,
//! `0..`[`surviving_count`](ColumnarBlockView::surviving_count) -- not a raw
//! block row position. Surviving row `i` is the `i`-th row of this block that
//! matched the scan's exact content predicate, which is the `i`-th
//! [`LogRecord`](crate::LogRecord) `next_block` would have returned for the
//! same block. An index at or past `surviving_count` reads `None`, as does a
//! cell whose column is absent from the block or was projected away by the
//! scan's [`ColumnSelection`](crate::ColumnSelection).

use crate::block::{BytesColRef, DecodedBlock};
use crate::field_dir::FieldDir;
use crate::record::{
    COL_BODY, COL_FLAGS, COL_OBSERVED_TS, COL_SEVERITY_NUM, COL_SEVERITY_TEXT, COL_SPAN_ID,
    COL_STREAM_REF, COL_TRACE_ID, COL_TS, FieldType,
};
use crate::stream_dir::StreamDir;
use ravel_types::logstream::LogStreamId;

/// A dynamic attribute column resolved out of FIELD_DIR: the id to address it
/// by and the stored type that says which accessor reads it.
///
/// Resolved once per block through
/// [`ColumnarBlockView::resolve_attr`] or
/// [`ColumnarBlockView::attr_columns`], instead of per row.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct AttrColumn {
    pub column_id: u32,
    pub ty: FieldType,
}

/// The dictionary form of a string column, restricted to a view's surviving
/// rows (ADR-0099 decision 4).
///
/// Handed out only for a column whose page was dictionary-encoded, by
/// [`ColumnarBlockView::str_dict`]. It exposes the distinct values and one id
/// per surviving row, indexed by surviving row position exactly as every other
/// per-row accessor is -- never the block's storage vectors.
pub struct StrDictColumn<'a> {
    dict: &'a [Vec<u8>],
    /// One entry per block row, `Some(id)` indexing `dict`, `None` for an absent
    /// row. Addressed through `rows` so callers see surviving-row indices only.
    ids: &'a [Option<u32>],
    /// Block row positions of the surviving rows, ascending (the view's `rows`).
    rows: &'a [usize],
}

impl<'a> StrDictColumn<'a> {
    /// The distinct values, in the page's dictionary order. Id `k` from
    /// [`id_at`](Self::id_at) addresses `dict()[k as usize]`.
    pub fn dict(&self) -> &'a [Vec<u8>] {
        self.dict
    }

    /// The number of surviving rows: the length [`id_at`](Self::id_at) and
    /// [`iter_ids`](Self::iter_ids) range over.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether no row survives.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// The dictionary id of surviving row `i`, or `None` when that row has no
    /// value for the column (absent per the presence bitmap) or `i` is out of
    /// range. `Some(id)` always indexes [`dict`](Self::dict) in bounds.
    pub fn id_at(&self, i: usize) -> Option<u32> {
        let row = *self.rows.get(i)?;
        self.ids.get(row).copied().flatten()
    }

    /// The dictionary ids of the surviving rows in order (see
    /// [`id_at`](Self::id_at)).
    pub fn iter_ids(&self) -> impl Iterator<Item = Option<u32>> + '_ {
        (0..self.rows.len()).map(move |i| self.id_at(i))
    }

    /// The bytes of surviving row `i`, resolved through the dictionary. This is
    /// the same value [`ColumnarBlockView::bytes_at`] returns for the same
    /// surviving row; it exists so a test can prove the dictionary form fuses
    /// back to the per-row form.
    pub fn value_at(&self, i: usize) -> Option<&'a [u8]> {
        let id = self.id_at(i)? as usize;
        self.dict.get(id).map(Vec::as_slice)
    }
}

/// A single column resolved once for the whole block, walked per surviving row.
///
/// The scan's row loop read one cell with one `HashMap<u32, _>` lookup per cell
/// (`i64_at` was 7% of warm-pass self time, its `RandomState` hashing another
/// 6%); a cursor resolves the column's storage once, then indexes the resolved
/// slice per row with no lookup at all (#875). It preserves ADR-0099 decision
/// 4: like every accessor here it hands out a *value* per surviving row and
/// never the storage vector, so the string-storage change decision 4 makes stays
/// invisible to callers -- the string cursor is [`BytesCursor`], which yields
/// cell bytes through the same opaque [`BytesColRef`] the plain and dictionary
/// shapes both read through.
///
/// A cursor's `at` distinguishes an absent column from a present column whose
/// cell is null the same way the per-cell accessor did -- both read as `None` at
/// the value level, byte-identical to the prior path -- while
/// [`is_column_present`](I64Cursor::is_column_present) exposes which case it is
/// for a caller that needs to tell them apart.
macro_rules! value_cursor {
    ($name:ident, $val:ty, $doc:literal) => {
        #[doc = $doc]
        pub struct $name<'a> {
            /// The resolved column slice, one entry per block row, or `None` when
            /// the block carries no such column (absent or projected away).
            col: Option<&'a [Option<$val>]>,
            /// Block row positions of the surviving rows (the view's `rows`).
            rows: &'a [usize],
        }

        impl<'a> $name<'a> {
            /// The cell at surviving row `i`, or `None` when `i` is out of range,
            /// the column is absent, or the cell is null.
            #[inline]
            pub fn at(&self, i: usize) -> Option<$val> {
                let row = *self.rows.get(i)?;
                self.col?.get(row).copied().flatten()
            }

            /// Whether the block carries this column at all, distinct from a
            /// present column whose cell at some row is null: both read as `None`
            /// from [`at`](Self::at), this tells them apart.
            pub fn is_column_present(&self) -> bool {
                self.col.is_some()
            }

            /// Number of surviving rows the cursor ranges over.
            pub fn len(&self) -> usize {
                self.rows.len()
            }

            /// Whether no row survives.
            pub fn is_empty(&self) -> bool {
                self.rows.is_empty()
            }
        }
    };
}

value_cursor!(
    I64Cursor,
    i64,
    "An i64 column resolved once per block (a fixed i64 column or an I64 attribute)."
);
value_cursor!(
    F64BitsCursor,
    u64,
    "An f64 column resolved once per block, yielding `to_bits` patterns so NaN \
     payloads and `-0.0` stay bit-exact (see \
     [`ColumnarBlockView::f64_bits_at`])."
);
value_cursor!(BoolCursor, bool, "A bool column resolved once per block.");

/// A byte-valued column (string or fixed-width id) resolved once per block,
/// walked per surviving row. Yields cell bytes through [`BytesColRef`], never the
/// storage vector, so it satisfies ADR-0099 decision 4 exactly as
/// [`ColumnarBlockView::bytes_at`] does.
pub struct BytesCursor<'a> {
    col: Option<BytesColRef<'a>>,
    rows: &'a [usize],
}

impl<'a> BytesCursor<'a> {
    /// The bytes at surviving row `i`, or `None` when `i` is out of range, the
    /// column is absent, or the cell is null.
    #[inline]
    pub fn at(&self, i: usize) -> Option<&'a [u8]> {
        let row = *self.rows.get(i)?;
        self.col.as_ref()?.cell(row)
    }

    /// The bytes at surviving row `i` as `&str`, or `None` when the cell is
    /// absent **or** its bytes are not UTF-8. Treating invalid UTF-8 as no value
    /// matches the row path (`String::from_utf8(..).ok()`), which lets a
    /// resource/scope fallback show through for that row.
    #[inline]
    pub fn str_at(&self, i: usize) -> Option<&'a str> {
        std::str::from_utf8(self.at(i)?).ok()
    }

    /// Whether the block carries this column at all (see
    /// [`I64Cursor::is_column_present`]).
    pub fn is_column_present(&self) -> bool {
        self.col.is_some()
    }

    /// Number of surviving rows the cursor ranges over.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Whether no row survives.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

/// A borrowed columnar view of one decoded block, restricted to the rows that
/// survived the scan's exact content predicate.
///
/// See the [module docs](self) for what this does and does not expose, and for
/// how rows are addressed.
pub struct ColumnarBlockView<'a> {
    stream_dir: &'a StreamDir,
    field_dir: &'a FieldDir,
    block: &'a DecodedBlock,
    /// Block row positions of the surviving rows, ascending.
    rows: &'a [usize],
}

/// Shape only. Formatting a block's cells would defeat the point of a view that
/// exists to avoid materializing them.
impl std::fmt::Debug for ColumnarBlockView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnarBlockView")
            .field("record_count", &self.record_count())
            .field("surviving_count", &self.surviving_count())
            .field("pages_decoded", &self.pages_decoded())
            .field("pages_skipped", &self.pages_skipped())
            .field("has_attrs_raw_page", &self.has_attrs_raw_page())
            .finish()
    }
}

impl<'a> ColumnarBlockView<'a> {
    pub(crate) fn new(
        stream_dir: &'a StreamDir,
        field_dir: &'a FieldDir,
        block: &'a DecodedBlock,
        rows: &'a [usize],
    ) -> Self {
        ColumnarBlockView {
            stream_dir,
            field_dir,
            block,
            rows,
        }
    }

    /// The block's total row count, before the content predicate.
    pub fn record_count(&self) -> usize {
        self.block.record_count()
    }

    /// Rows of this block that matched the content predicate: the length of
    /// every column this view walks, and the number of records `next_block`
    /// would have returned for the same block.
    pub fn surviving_count(&self) -> usize {
        self.rows.len()
    }

    /// Pages this block's decode decompressed and decoded.
    pub fn pages_decoded(&self) -> usize {
        self.block.pages_decoded()
    }

    /// Pages this block's decode skipped because the column filter excluded
    /// them.
    pub fn pages_skipped(&self) -> usize {
        self.block.pages_skipped()
    }

    /// Whether this block carries any page for the `attrs_raw` overflow column.
    ///
    /// Answered from the block header's page descriptors: no `attrs_raw` page
    /// is decoded to find out, and the answer holds even when the scan's column
    /// selection excluded `attrs_raw` entirely. A caller whose fast path cannot
    /// see attributes that spilled past the dynamic-column budget uses this as
    /// an eligibility check (ADR-0099 decision 2), and decoding the column to
    /// discover it is empty would cost exactly what the check avoids.
    pub fn has_attrs_raw_page(&self) -> bool {
        self.block.has_attrs_raw_page()
    }

    /// The heap bytes the decoded block behind this view holds resident: every
    /// decoded column vector plus every present string and fixed-width cell's
    /// own allocation.
    ///
    /// This view borrows the block, and the block stays resident for as long as
    /// the view (and anything built from it while the cursor has not moved on)
    /// is alive. A caller charging a memory pool for what it concurrently holds
    /// therefore has to charge this in addition to whatever it builds
    /// (ADR-0087 decision 2). It is an accessor like every other method here:
    /// it reports a size, and exposes neither the block nor any column's
    /// storage type, so ADR-0099 decision 4 can change how string columns are
    /// stored without touching a caller.
    ///
    /// The figure covers the whole block, not only the surviving rows: a
    /// content predicate that drops rows does not shrink the decoded columns.
    pub fn decoded_bytes(&self) -> usize {
        self.block.decoded_heap_bytes()
    }

    // --- fixed columns ------------------------------------------------------

    /// Event timestamp of surviving row `i`.
    pub fn ts(&self, i: usize) -> Option<i64> {
        self.i64_at(COL_TS, i)
    }

    /// Observed (ingest) timestamp of surviving row `i`.
    pub fn observed_ts(&self, i: usize) -> Option<i64> {
        self.i64_at(COL_OBSERVED_TS, i)
    }

    /// Severity number of surviving row `i`, as stored. The row path narrows
    /// this to the record's `u8`; the stored column is an i64 and this reports
    /// it unnarrowed.
    pub fn severity_num(&self, i: usize) -> Option<i64> {
        self.i64_at(COL_SEVERITY_NUM, i)
    }

    /// Flags of surviving row `i`, as stored. The row path narrows this to the
    /// record's `u32`.
    pub fn flags(&self, i: usize) -> Option<i64> {
        self.i64_at(COL_FLAGS, i)
    }

    /// The dense STREAM_DIR reference of surviving row `i`.
    ///
    /// `None` only when the row index is out of range or `stream_ref` was not
    /// decoded; a stored value outside `u32` is rejected before this view is
    /// handed out (see [`crate::BlockScan::next_block_columnar`]), so it cannot
    /// surface here as a missing value.
    pub fn stream_ref(&self, i: usize) -> Option<u32> {
        u32::try_from(self.i64_at(COL_STREAM_REF, i)?).ok()
    }

    /// Severity text of surviving row `i`, as stored bytes. Not validated as
    /// UTF-8: the row path validates when it builds a `String`, and a caller
    /// building its own strings decides what to do about a violation.
    pub fn severity_text(&self, i: usize) -> Option<&'a [u8]> {
        self.bytes_at(COL_SEVERITY_TEXT, i)
    }

    /// Body of surviving row `i`, as stored bytes (see
    /// [`severity_text`](Self::severity_text) on UTF-8).
    pub fn body(&self, i: usize) -> Option<&'a [u8]> {
        self.bytes_at(COL_BODY, i)
    }

    /// The 16-byte trace id of surviving row `i`, or `None` when the row
    /// carries none.
    pub fn trace_id(&self, i: usize) -> Option<&'a [u8]> {
        self.bytes_at(COL_TRACE_ID, i)
    }

    /// The 8-byte span id of surviving row `i`, or `None` when the row carries
    /// none.
    pub fn span_id(&self, i: usize) -> Option<&'a [u8]> {
        self.bytes_at(COL_SPAN_ID, i)
    }

    // --- stream identity ----------------------------------------------------

    /// The stream identity of surviving row `i`, borrowed from STREAM_DIR.
    pub fn stream_id(&self, i: usize) -> Option<&'a LogStreamId> {
        self.stream_id_of(self.stream_ref(i)?)
    }

    /// The canonical resource+scope attribute blob of surviving row `i`,
    /// borrowed from STREAM_DIR. This is the same bytes the row path clones
    /// into every record's `stream_attrs`.
    pub fn stream_attrs(&self, i: usize) -> Option<&'a [u8]> {
        self.stream_attrs_of(self.stream_ref(i)?)
    }

    /// The stream identity behind a `stream_ref`, for a caller that groups rows
    /// by reference and resolves each reference once.
    pub fn stream_id_of(&self, stream_ref: u32) -> Option<&'a LogStreamId> {
        self.stream_entry(stream_ref).map(|e| &e.stream_id)
    }

    /// The canonical resource+scope blob behind a `stream_ref`.
    pub fn stream_attrs_of(&self, stream_ref: u32) -> Option<&'a [u8]> {
        self.stream_entry(stream_ref).map(|e| e.blob.as_slice())
    }

    /// How many distinct `stream_ref` values this object's STREAM_DIR defines.
    pub fn stream_count(&self) -> usize {
        self.stream_dir.entries().len()
    }

    // --- dynamic attribute columns -----------------------------------------

    /// The FIELD_DIR column for attribute `key` at stored type `ty`, resolved
    /// once for the whole block.
    ///
    /// One key can have a column per type (a name written as both a string and
    /// an integer splits into two columns), which is why the type is part of the
    /// lookup. Use [`attr_columns_for`](Self::attr_columns_for) to find every
    /// column a key has.
    pub fn resolve_attr(&self, key: &str, ty: FieldType) -> Option<AttrColumn> {
        self.field_dir.column(key, ty).map(|e| AttrColumn {
            column_id: e.column_id,
            ty: e.ty,
        })
    }

    /// Every FIELD_DIR column carrying attribute `key`, one per stored type.
    pub fn attr_columns_for<'k>(
        &self,
        key: &'k str,
    ) -> impl Iterator<Item = AttrColumn> + use<'a, 'k> {
        self.field_dir
            .entries()
            .iter()
            .filter(move |e| e.name == key)
            .map(|e| AttrColumn {
                column_id: e.column_id,
                ty: e.ty,
            })
    }

    /// Every dynamic attribute column this object defines, as
    /// `(attribute name, column)`, in FIELD_DIR order.
    ///
    /// A column here is not necessarily decoded: the scan's column selection
    /// may have skipped it, in which case every cell of it reads `None`.
    pub fn attr_columns(&self) -> impl Iterator<Item = (&'a str, AttrColumn)> + '_ {
        self.field_dir.entries().iter().map(|e| {
            (
                e.name.as_str(),
                AttrColumn {
                    column_id: e.column_id,
                    ty: e.ty,
                },
            )
        })
    }

    // --- per-column cursors -------------------------------------------------

    /// Resolve column `column_id` as an i64 column once, for a cursor that walks
    /// the surviving rows with no further lookup (#875). The resolution is the
    /// one `HashMap`/`Vec` lookup the per-cell [`i64_at`](Self::i64_at) did on
    /// every cell.
    pub fn i64_cursor(&self, column_id: u32) -> I64Cursor<'a> {
        I64Cursor {
            col: self.block.i64_col(column_id),
            rows: self.rows,
        }
    }

    /// Resolve column `column_id` as an f64 column once (bits form, see
    /// [`f64_bits_at`](Self::f64_bits_at)).
    pub fn f64_bits_cursor(&self, column_id: u32) -> F64BitsCursor<'a> {
        F64BitsCursor {
            col: self.block.f64_col(column_id),
            rows: self.rows,
        }
    }

    /// Resolve column `column_id` as a bool column once.
    pub fn bool_cursor(&self, column_id: u32) -> BoolCursor<'a> {
        BoolCursor {
            col: self.block.bool_col(column_id),
            rows: self.rows,
        }
    }

    /// Resolve column `column_id` as a byte-valued column once (a string column
    /// or a fixed-width id column; see [`bytes_at`](Self::bytes_at)).
    pub fn bytes_cursor(&self, column_id: u32) -> BytesCursor<'a> {
        BytesCursor {
            col: self.block.bytes_col(column_id),
            rows: self.rows,
        }
    }

    /// A cursor over the event-timestamp column.
    pub fn ts_cursor(&self) -> I64Cursor<'a> {
        self.i64_cursor(COL_TS)
    }

    /// A cursor over the observed-timestamp column.
    pub fn observed_ts_cursor(&self) -> I64Cursor<'a> {
        self.i64_cursor(COL_OBSERVED_TS)
    }

    /// A cursor over the severity-number column.
    pub fn severity_num_cursor(&self) -> I64Cursor<'a> {
        self.i64_cursor(COL_SEVERITY_NUM)
    }

    /// A cursor over the flags column.
    pub fn flags_cursor(&self) -> I64Cursor<'a> {
        self.i64_cursor(COL_FLAGS)
    }

    /// A cursor over the dense STREAM_DIR reference column, as its stored i64
    /// (the caller narrows to `u32`, exactly as [`stream_ref`](Self::stream_ref)
    /// does).
    pub fn stream_ref_cursor(&self) -> I64Cursor<'a> {
        self.i64_cursor(COL_STREAM_REF)
    }

    /// A cursor over the severity-text column (stored bytes, not UTF-8 validated;
    /// see [`severity_text`](Self::severity_text)).
    pub fn severity_text_cursor(&self) -> BytesCursor<'a> {
        self.bytes_cursor(COL_SEVERITY_TEXT)
    }

    /// A cursor over the body column.
    pub fn body_cursor(&self) -> BytesCursor<'a> {
        self.bytes_cursor(COL_BODY)
    }

    /// A cursor over the trace-id column.
    pub fn trace_id_cursor(&self) -> BytesCursor<'a> {
        self.bytes_cursor(COL_TRACE_ID)
    }

    /// A cursor over the span-id column.
    pub fn span_id_cursor(&self) -> BytesCursor<'a> {
        self.bytes_cursor(COL_SPAN_ID)
    }

    /// Column resolutions by id since this block was decoded (see
    /// [`DecodedBlock::column_lookups`](crate::block::DecodedBlock::column_lookups)).
    /// A cursor bumps this once per column; a per-cell accessor bumps it once
    /// per cell. Exposed so a test can pin that the scan resolves O(columns) per
    /// block, not O(rows x columns).
    pub fn column_lookups(&self) -> u64 {
        self.block.column_lookups()
    }

    // --- by column id -------------------------------------------------------

    /// The i64 cell at surviving row `i` of column `column_id`.
    pub fn i64_at(&self, column_id: u32, i: usize) -> Option<i64> {
        let row = self.row(i)?;
        self.block.i64_col(column_id)?.get(row).copied().flatten()
    }

    /// The f64 cell at surviving row `i` of column `column_id`, as its
    /// `to_bits` pattern. Bits rather than an `f64` because NaN payloads and
    /// `-0.0` are significant in this format and comparison here is bit-exact.
    pub fn f64_bits_at(&self, column_id: u32, i: usize) -> Option<u64> {
        let row = self.row(i)?;
        self.block.f64_col(column_id)?.get(row).copied().flatten()
    }

    /// [`f64_bits_at`](Self::f64_bits_at) as an `f64`. Lossless
    /// (`f64::from_bits`), but two distinct stored NaNs compare equal as
    /// values; use the bits form to distinguish them.
    pub fn f64_at(&self, column_id: u32, i: usize) -> Option<f64> {
        self.f64_bits_at(column_id, i).map(f64::from_bits)
    }

    /// The bool cell at surviving row `i` of column `column_id`.
    pub fn bool_at(&self, column_id: u32, i: usize) -> Option<bool> {
        let row = self.row(i)?;
        self.block.bool_col(column_id)?.get(row).copied().flatten()
    }

    /// The byte cell at surviving row `i` of column `column_id`: a string or
    /// bytes attribute, `body`, `severity_text`, or a fixed-width id column.
    ///
    /// String-typed and fixed-width columns are deliberately one accessor. They
    /// are stored differently and no column id is both, so a caller never needs
    /// to know which of the two a column is; that is exactly the coupling this
    /// view exists to avoid.
    pub fn bytes_at(&self, column_id: u32, i: usize) -> Option<&'a [u8]> {
        let row = self.row(i)?;
        let block = self.block;
        if let Some(bytes) = block.str_at(column_id, row) {
            return Some(bytes);
        }
        block.fixed_col(column_id)?.get(row)?.as_deref()
    }

    /// The dictionary form of string column `column_id`, restricted to the
    /// surviving rows (ADR-0099 decision 4): the distinct values plus one id per
    /// surviving row. `Some` only for a column whose page was dictionary-encoded;
    /// `None` for a plain page, an absent column, or one projected away -- a
    /// plain page stays readable through [`bytes_at`](Self::bytes_at) and is
    /// never forced into a dictionary here.
    ///
    /// Issue #417 consumes this to build an Arrow `Dictionary(Int32, Utf8)`
    /// array without rebuilding a dictionary row by row. Like every accessor
    /// here it returns values, never the block's storage type: `dict()` borrows
    /// the distinct byte values and the ids address them, so the caller learns
    /// nothing of how the column is stored beyond that it deduplicates.
    pub fn str_dict(&self, column_id: u32) -> Option<StrDictColumn<'a>> {
        let (dict, ids) = self.block.str_dict(column_id)?;
        Some(StrDictColumn {
            dict,
            ids,
            rows: self.rows,
        })
    }

    // --- gather iterators ---------------------------------------------------

    /// Walks column `column_id` over the surviving rows in order, as i64 cells.
    pub fn iter_i64(&self, column_id: u32) -> impl Iterator<Item = Option<i64>> + '_ {
        (0..self.rows.len()).map(move |i| self.i64_at(column_id, i))
    }

    /// Walks column `column_id` over the surviving rows in order, as f64 bit
    /// patterns (see [`f64_bits_at`](Self::f64_bits_at)).
    pub fn iter_f64_bits(&self, column_id: u32) -> impl Iterator<Item = Option<u64>> + '_ {
        (0..self.rows.len()).map(move |i| self.f64_bits_at(column_id, i))
    }

    /// Walks column `column_id` over the surviving rows in order, as bool cells.
    pub fn iter_bool(&self, column_id: u32) -> impl Iterator<Item = Option<bool>> + '_ {
        (0..self.rows.len()).map(move |i| self.bool_at(column_id, i))
    }

    /// Walks column `column_id` over the surviving rows in order, as byte cells
    /// (see [`bytes_at`](Self::bytes_at)).
    pub fn iter_bytes(&self, column_id: u32) -> impl Iterator<Item = Option<&'a [u8]>> + '_ {
        (0..self.rows.len()).map(move |i| self.bytes_at(column_id, i))
    }

    /// Walks `ts` over the surviving rows in order.
    pub fn iter_ts(&self) -> impl Iterator<Item = Option<i64>> + '_ {
        self.iter_i64(COL_TS)
    }

    /// Walks `observed_ts` over the surviving rows in order.
    pub fn iter_observed_ts(&self) -> impl Iterator<Item = Option<i64>> + '_ {
        self.iter_i64(COL_OBSERVED_TS)
    }

    /// Walks `severity_num` over the surviving rows in order.
    pub fn iter_severity_num(&self) -> impl Iterator<Item = Option<i64>> + '_ {
        self.iter_i64(COL_SEVERITY_NUM)
    }

    /// Walks `flags` over the surviving rows in order.
    pub fn iter_flags(&self) -> impl Iterator<Item = Option<i64>> + '_ {
        self.iter_i64(COL_FLAGS)
    }

    /// Walks `stream_ref` over the surviving rows in order.
    pub fn iter_stream_ref(&self) -> impl Iterator<Item = Option<u32>> + '_ {
        (0..self.rows.len()).map(move |i| self.stream_ref(i))
    }

    /// Walks `severity_text` over the surviving rows in order.
    pub fn iter_severity_text(&self) -> impl Iterator<Item = Option<&'a [u8]>> + '_ {
        self.iter_bytes(COL_SEVERITY_TEXT)
    }

    /// Walks `body` over the surviving rows in order.
    pub fn iter_body(&self) -> impl Iterator<Item = Option<&'a [u8]>> + '_ {
        self.iter_bytes(COL_BODY)
    }

    /// Walks `trace_id` over the surviving rows in order.
    pub fn iter_trace_id(&self) -> impl Iterator<Item = Option<&'a [u8]>> + '_ {
        self.iter_bytes(COL_TRACE_ID)
    }

    /// Walks `span_id` over the surviving rows in order.
    pub fn iter_span_id(&self) -> impl Iterator<Item = Option<&'a [u8]>> + '_ {
        self.iter_bytes(COL_SPAN_ID)
    }

    // --- internals ----------------------------------------------------------

    /// The block row position of surviving row `i`. Every accessor goes through
    /// this: reading `i` as a block row position would silently return another
    /// row's data whenever the content predicate dropped anything.
    fn row(&self, i: usize) -> Option<usize> {
        self.rows.get(i).copied()
    }

    fn stream_entry(&self, stream_ref: u32) -> Option<&'a crate::stream_dir::StreamEntry> {
        self.stream_dir.entries().get(stream_ref as usize)
    }
}
