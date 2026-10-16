//! The `samples` table schema
//! and the internal provenance-carrying scan schema.
//!
//! Public schema (what any DataFusion operator above the dedup sees):
//!
//! | column    | arrow type                       |
//! |-----------|----------------------------------|
//! | ts        | Timestamp(Nanosecond, None)      |
//! | value     | Float64                          |
//! | series_id | FixedSizeBinary(16)              |
//! | labels    | Dictionary(Int32, Map(Utf8,Utf8))|
//!
//! `labels` is dictionary-encoded: one dictionary entry per distinct series
//! in the batch, an Int32 key per row, so the per-row cost is one 4-byte key
//! regardless of label-set size (F10 / plan Table model).
//!
//! The internal scan schema appends four provenance columns used only by
//! [`crate::dedup`]: created_unix_ns Int64, writer_epoch UInt64, writer_seq
//! UInt64, in_page_index UInt32. They are stripped before the public schema
//! is emitted.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

pub const COL_TS: usize = 0;
pub const COL_VALUE: usize = 1;
pub const COL_SERIES_ID: usize = 2;
pub const COL_LABELS: usize = 3;
pub const COL_CREATED_UNIX_NS: usize = 4;
pub const COL_WRITER_EPOCH: usize = 5;
pub const COL_WRITER_SEQ: usize = 6;
pub const COL_IN_PAGE_INDEX: usize = 7;

/// Number of public (non-provenance) columns; the public schema is the
/// first [`PUBLIC_COLUMNS`] fields of the internal schema.
pub const PUBLIC_COLUMNS: usize = 4;

/// Width, in bytes, of a canonical series id (ADR-0005).
pub const SERIES_ID_WIDTH: i32 = 16;

/// The `Map(Utf8, Utf8)` type backing a label set. Field/child names match
/// what `arrow`'s `MapBuilder` produces by default, so a built array and
/// this declared type agree exactly (a mismatch would panic in
/// `RecordBatch::try_new`).
pub fn label_map_type() -> DataType {
    let keys = Field::new("keys", DataType::Utf8, false);
    let values = Field::new("values", DataType::Utf8, true);
    let entries = Field::new(
        "entries",
        DataType::Struct(vec![keys, values].into()),
        false,
    );
    DataType::Map(Arc::new(entries), false)
}

/// The dictionary-encoded label column type: `Dictionary(Int32, Map)`.
pub fn labels_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(label_map_type()))
}

fn public_fields() -> Vec<Field> {
    vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Float64, false),
        Field::new(
            "series_id",
            DataType::FixedSizeBinary(SERIES_ID_WIDTH),
            false,
        ),
        Field::new("labels", labels_type(), false),
    ]
}

/// The public `samples` schema: ts, value, series_id, labels.
pub fn public_schema() -> SchemaRef {
    Arc::new(Schema::new(public_fields()))
}

/// The internal scan schema: the public columns plus the four provenance
/// columns consumed by the dedup operator.
pub fn internal_schema() -> SchemaRef {
    let mut fields = public_fields();
    fields.push(Field::new("created_unix_ns", DataType::Int64, false));
    fields.push(Field::new("writer_epoch", DataType::UInt64, false));
    fields.push(Field::new("writer_seq", DataType::UInt64, false));
    fields.push(Field::new("in_page_index", DataType::UInt32, false));
    Arc::new(Schema::new(fields))
}
