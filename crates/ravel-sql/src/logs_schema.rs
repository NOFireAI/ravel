//! The `logs` table schema (ADR-0033 "Build a `logs` table in `ravel-sql`").
//!
//! Structurally the log-signal sibling of [`crate::schema`]'s `samples` table:
//! a set of fixed columns plus one `attrs` `Map(Utf8, Utf8)` column for the
//! record's dynamic attributes, mirroring how the metrics table exposes
//! `labels` as a map rather than per-key columns (ADR-0033, "same reason: a
//! per-tenant, per-key column schema is a v-next refinement").
//!
//! | column        | arrow type                    | source ([`ravel_logseg::LogRecord`]) |
//! |---------------|-------------------------------|--------------------------------------|
//! | ts            | Timestamp(Nanosecond, None)   | `ts_ns`                              |
//! | observed_ts   | Timestamp(Nanosecond, None)   | `observed_ts_ns`                     |
//! | severity_num  | UInt8                         | `severity_num` (a `u8`)              |
//! | severity_text | Utf8                          | `severity_text`                      |
//! | body          | Utf8                          | `body`                               |
//! | trace_id      | FixedSizeBinary(16), nullable | `trace_id` (`Option<[u8; 16]>`)      |
//! | span_id       | FixedSizeBinary(8), nullable  | `span_id` (`Option<[u8; 8]>`)        |
//! | flags         | UInt32                        | `flags`                              |
//! | attrs         | Map(Utf8, Utf8)               | `attrs` (dynamic per-record attrs)   |
//!
//! The `attrs` column carries the record's **dynamic** attributes
//! ([`ravel_logseg::LogRecord::attrs`]). A SQL predicate written as
//! `attrs['k'] = 'v'` is nonetheless resolved as a stream-identifying
//! (resource/scope) attribute filter at fetch time (ADR-0033,
//! "Stream-identifying predicates resolve per-object"); see
//! [`crate::logs_pushdown`] for how that predicate is recognized and
//! [`crate::logs_scan`] for the mandatory post-fetch re-verification against
//! each record's genuine resource/scope attributes.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::schema::label_map_type;

pub const LOG_COL_TS: usize = 0;
pub const LOG_COL_OBSERVED_TS: usize = 1;
pub const LOG_COL_SEVERITY_NUM: usize = 2;
pub const LOG_COL_SEVERITY_TEXT: usize = 3;
pub const LOG_COL_BODY: usize = 4;
pub const LOG_COL_TRACE_ID: usize = 5;
pub const LOG_COL_SPAN_ID: usize = 6;
pub const LOG_COL_FLAGS: usize = 7;
pub const LOG_COL_ATTRS: usize = 8;

/// Byte width of a trace id (`ravel_logseg::record::TRACE_ID_WIDTH`).
pub const TRACE_ID_WIDTH: i32 = 16;
/// Byte width of a span id (`ravel_logseg::record::SPAN_ID_WIDTH`).
pub const SPAN_ID_WIDTH: i32 = 8;

fn logs_fields() -> Vec<Field> {
    vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new(
            "observed_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("severity_num", DataType::UInt8, false),
        Field::new("severity_text", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        // trace_id/span_id are optional on a `LogRecord`, so both columns are
        // nullable: an absent id is a NULL cell, never a zero-filled one.
        Field::new("trace_id", DataType::FixedSizeBinary(TRACE_ID_WIDTH), true),
        Field::new("span_id", DataType::FixedSizeBinary(SPAN_ID_WIDTH), true),
        Field::new("flags", DataType::UInt32, false),
        // Same `Map(Utf8, Utf8)` type the `samples` table's `labels` column is
        // built on (the child field naming that arrow's `MapBuilder` produces
        // by default), so a built array and this declared type agree exactly.
        Field::new("attrs", label_map_type(), false),
    ]
}

/// The public `logs` table schema.
pub fn logs_schema() -> SchemaRef {
    Arc::new(Schema::new(logs_fields()))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn schema_columns_are_in_the_documented_order_and_type() {
        let s = logs_schema();
        assert_eq!(s.fields().len(), 9);
        assert_eq!(s.field(LOG_COL_TS).name(), "ts");
        assert_eq!(
            s.field(LOG_COL_TS).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(s.field(LOG_COL_OBSERVED_TS).name(), "observed_ts");
        assert_eq!(s.field(LOG_COL_SEVERITY_NUM).data_type(), &DataType::UInt8);
        assert_eq!(s.field(LOG_COL_SEVERITY_TEXT).data_type(), &DataType::Utf8);
        assert_eq!(s.field(LOG_COL_BODY).data_type(), &DataType::Utf8);
        assert_eq!(
            s.field(LOG_COL_TRACE_ID).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert!(s.field(LOG_COL_TRACE_ID).is_nullable());
        assert_eq!(
            s.field(LOG_COL_SPAN_ID).data_type(),
            &DataType::FixedSizeBinary(8)
        );
        assert!(s.field(LOG_COL_SPAN_ID).is_nullable());
        assert_eq!(s.field(LOG_COL_FLAGS).data_type(), &DataType::UInt32);
        assert_eq!(s.field(LOG_COL_ATTRS).name(), "attrs");
        assert_eq!(s.field(LOG_COL_ATTRS).data_type(), &label_map_type());
    }
}
