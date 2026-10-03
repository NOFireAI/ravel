//! A borrowed columnar view over one decoded block (docs/adrs/0110 decision 1).
//!
//! [`crate::block::DecodedBlock::record`] rebuilds one
//! [`SpanRecord`](crate::SpanRecord) per row: the merged attribute map
//! reassembled from the per-key columns, the `attrs_raw` overflow decoded, the
//! lifted `service.name` re-inserted, and the `_events_raw` blob reconstructed
//! from the nested event columns. A caller that is about to build columnar
//! output again (Arrow arrays, in `ravel-sql`) pays for that row form and then
//! throws it away.
//!
//! [`ColumnarBlockView`] is the second exit: the block's columns read straight
//! out, keyed by column id, gathered over a caller-supplied slice of surviving
//! row indices.
//!
//! # Accessors only, never the storage type
//!
//! Every method here returns a typed cell or a small typed column handle. None
//! returns the decoded block, and none returns a column's storage vector or the
//! backing `HashMap`. That is deliberate and load-bearing: it lets a later
//! change to how string columns are stored (a dictionary form rather than one
//! `Vec<u8>` per row) land without touching a caller. ADR-0099 decision 1
//! placed the same constraint on `ravel-logseg`'s block view, for the same
//! reason.
//!
//! # Requested vs absent
//!
//! A projected decode ([`crate::block::read_block_projected`]) materializes only
//! the columns it was asked for. The view distinguishes two cases the backing
//! map cannot tell apart on its own:
//!
//! - a column that is **absent from this block** (no page was written, because
//!   no row had a value) is a legitimate `NULL` for every row, and its accessor
//!   returns a column whose every cell reads `None`;
//! - a column that was **not requested by this decode** is a caller bug (a
//!   mis-specified projection), and its accessor returns
//!   [`SpanSegError::ColumnNotRequested`], never a silent column of nulls. A
//!   mis-specified projection fails loudly rather than answering a query with an
//!   all-`NULL` column.
//!
//! A full decode ([`crate::block::read_block`]) requests every column, so no
//! accessor ever returns that error for a block it produced.
//!
//! # Row addressing
//!
//! The per-cell accessors take a block row position, `0..record_count`. The
//! gather iterators take a caller-supplied slice of such positions (the rows
//! that survived the query's predicates) and yield one cell per index, in the
//! order given. An index at or past `record_count`, or a cell whose column has
//! no value in that row, reads `None`.

use crate::block::DecodedBlock;
use crate::error::SpanSegError;

/// A decoded i64 column, borrowed from the block (docs/adrs/0110 decision 1).
///
/// `values` is `None` when the column is absent from the block, in which case
/// every cell reads `None`: a legitimate `NULL` for every row, not an error.
#[derive(Clone, Copy)]
pub struct I64Column<'a> {
    values: Option<&'a [Option<i64>]>,
    record_count: usize,
}

impl<'a> I64Column<'a> {
    /// The number of rows in the block this column spans.
    pub fn len(&self) -> usize {
        self.record_count
    }

    /// Whether the block has no rows.
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// The value at block row `row`, or `None` when the row has no value (a
    /// nullable column absent here, or `row` out of range).
    pub fn value_at(&self, row: usize) -> Option<i64> {
        self.values?.get(row).copied().flatten()
    }

    /// Gathers the column over `rows` (surviving row indices), one cell per
    /// index in the order given.
    pub fn gather<'i>(&self, rows: &'i [usize]) -> impl Iterator<Item = Option<i64>> + use<'a, 'i> {
        let col = *self;
        rows.iter().map(move |&r| col.value_at(r))
    }
}

/// A decoded byte-valued column, borrowed from the block: either a string
/// column ([`ColumnarBlockView::str_column`]) or a fixed-width binary column
/// ([`ColumnarBlockView::fixed_column`]). The two are stored separately but read
/// identically, so they share one handle; the accessor that produced it fixes
/// which storage it reads.
///
/// `values` is `None` when the column is absent from the block, in which case
/// every cell reads `None`.
#[derive(Clone, Copy)]
pub struct BytesColumn<'a> {
    values: Option<&'a [Option<Vec<u8>>]>,
    record_count: usize,
}

impl<'a> BytesColumn<'a> {
    /// The number of rows in the block this column spans.
    pub fn len(&self) -> usize {
        self.record_count
    }

    /// Whether the block has no rows.
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// The bytes at block row `row`, or `None` when the row has no value. Not
    /// validated as UTF-8: the row path validates when it builds a `String`,
    /// and a caller building its own values decides what to do about a
    /// violation.
    pub fn value_at(&self, row: usize) -> Option<&'a [u8]> {
        self.values?.get(row)?.as_deref()
    }

    /// Gathers the column over `rows` (surviving row indices), one cell per
    /// index in the order given.
    pub fn gather<'i>(
        &self,
        rows: &'i [usize],
    ) -> impl Iterator<Item = Option<&'a [u8]>> + use<'a, 'i> {
        let col = *self;
        rows.iter().map(move |&r| col.value_at(r))
    }
}

/// A borrowed columnar view of one decoded block. See the [module docs](self)
/// for what it does and does not expose, how requested-vs-absent is decided,
/// and how rows are addressed.
pub struct ColumnarBlockView<'a> {
    block: &'a DecodedBlock,
}

/// Shape only. Formatting a block's cells would defeat the point of a view that
/// exists to avoid materializing them.
impl std::fmt::Debug for ColumnarBlockView<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnarBlockView")
            .field("record_count", &self.record_count())
            .field("pages_decoded", &self.block.pages_decoded())
            .field("pages_skipped", &self.block.pages_skipped())
            .field("has_attrs_raw_page", &self.has_attrs_raw_page())
            .finish()
    }
}

impl<'a> ColumnarBlockView<'a> {
    pub(crate) fn new(block: &'a DecodedBlock) -> Self {
        ColumnarBlockView { block }
    }

    /// The block's row count.
    pub fn record_count(&self) -> usize {
        self.block.record_count()
    }

    /// Whether this block carries a `COL_ATTRS_RAW` overflow page, answered from
    /// the page descriptors without decoding it (docs/adrs/0110 decision 1).
    pub fn has_attrs_raw_page(&self) -> bool {
        self.block.has_attrs_raw_page()
    }

    /// The i64 column `col`. `Err` when `col` was not requested by this decode;
    /// otherwise a column whose cells read `None` when `col` is absent from the
    /// block.
    pub fn i64_column(&self, col: u32) -> Result<I64Column<'a>, SpanSegError> {
        self.check_requested(col)?;
        Ok(I64Column {
            values: self.block.i64_col(col),
            record_count: self.block.record_count(),
        })
    }

    /// The string column `col`, read from the block's string columns. `Err`
    /// when `col` was not requested; otherwise a column whose cells read `None`
    /// when `col` is absent from the block.
    pub fn str_column(&self, col: u32) -> Result<BytesColumn<'a>, SpanSegError> {
        self.check_requested(col)?;
        Ok(BytesColumn {
            values: self.block.str_col(col),
            record_count: self.block.record_count(),
        })
    }

    /// The fixed-width binary column `col` (trace id, span id, parent span id),
    /// read from the block's fixed-width columns. `Err` when `col` was not
    /// requested; otherwise a column whose cells read `None` when `col` is
    /// absent from the block.
    pub fn fixed_column(&self, col: u32) -> Result<BytesColumn<'a>, SpanSegError> {
        self.check_requested(col)?;
        Ok(BytesColumn {
            values: self.block.fixed_col(col),
            record_count: self.block.record_count(),
        })
    }

    /// Fails with a typed [`SpanSegError::ColumnNotRequested`] when `col` was
    /// not named by the decode that produced this block. This is what keeps a
    /// mis-specified projection from being answered as an all-`NULL` column.
    fn check_requested(&self, col: u32) -> Result<(), SpanSegError> {
        if self.block.is_requested(col) {
            Ok(())
        } else {
            Err(SpanSegError::ColumnNotRequested(col))
        }
    }
}
