//! The `spans` table schema (ADR-0041 "RSPAN v1 span segment format", phase 5).
//!
//! Structurally the span-signal sibling of [`crate::logs_schema`]'s `logs`
//! table and [`crate::schema`]'s `samples` table: a set of fixed columns plus
//! one `attrs` `Map(Utf8, Utf8)` column for the span's merged
//! resource+scope+span attributes (RSPAN stores that merged map directly, so
//! unlike `logs` there is no separate stream-identity blob to fold in at scan
//! time).
//!
//! | column         | arrow type                    | source ([`ravel_rspan::SpanRecord`]) |
//! |----------------|-------------------------------|--------------------------------------|
//! | trace_id       | FixedSizeBinary(16)           | `trace_id`                           |
//! | span_id        | FixedSizeBinary(8)            | `span_id`                            |
//! | parent_span_id | FixedSizeBinary(8), nullable  | `parent_span_id` (`Option<[u8; 8]>`) |
//! | name           | Utf8                          | `name`                               |
//! | start_ts       | Timestamp(Nanosecond, None)   | `start_ts_ns`                        |
//! | end_ts         | Timestamp(Nanosecond, None)   | `end_ts_ns`                          |
//! | status_code    | UInt8                         | `status_code` (`StatusCode as u8`)   |
//! | status_message | Utf8, nullable                | `status_message` (`Option<String>`)  |
//! | attrs          | Map(Utf8, Utf8)               | `attrs` (already-merged map)         |
//! | service_name   | Utf8, nullable                | `attrs["service.name"]`              |
//! | duration_ns    | Int64                         | computed: `end_ts - start_ts`        |
//!
//! `status_code` is a small integer (`0=Unset`, `1=Ok`, `2=Error`), the exact
//! byte [`ravel_rspan::StatusCode::to_u8`] emits, chosen over a
//! dictionary-encoded string for the same reason `logs.severity_num` is a
//! `UInt8`: it is a tiny fixed enum, a plain integer round-trips it exactly,
//! and a caller wanting the text can map the three values in plain SQL.
//!
//! `service_name` (ADR-0045 decision 5) is exposed as a plain column so
//! `WHERE service_name = '...'` is pushdown-eligible. RSPAN v1 (the only
//! version this crate currently reads) still stores `service.name` inside
//! the merged `attrs` map rather than as ADR-0045's dictionary-encoded
//! column id 9, so this column is populated from the map at scan time
//! (`spans_scan.rs`) and is nullable: a span with no `service.name`
//! attribute yields NULL, not an empty string.
//!
//! `duration_ns` is a computed column, never a stored one (ADR-0045 decision
//! 5, rejected alternative 3): it is exactly `end_ts - start_ts`, both of
//! which are already stored, so materializing it as a column would add bytes
//! to every row to answer a question the block bounds already answer for
//! free. Exposing it as a SQL column (rather than requiring every caller to
//! write `end_ts - start_ts` by hand) is what makes `WHERE duration_ns >
//! 5e8` pushdown-eligible in `spans_pushdown.rs`.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::schema::label_map_type;

pub const SPAN_COL_TRACE_ID: usize = 0;
pub const SPAN_COL_SPAN_ID: usize = 1;
pub const SPAN_COL_PARENT_SPAN_ID: usize = 2;
pub const SPAN_COL_NAME: usize = 3;
pub const SPAN_COL_START_TS: usize = 4;
pub const SPAN_COL_END_TS: usize = 5;
pub const SPAN_COL_STATUS_CODE: usize = 6;
pub const SPAN_COL_STATUS_MESSAGE: usize = 7;
pub const SPAN_COL_ATTRS: usize = 8;
pub const SPAN_COL_SERVICE_NAME: usize = 9;
pub const SPAN_COL_DURATION_NS: usize = 10;

/// Byte width of a trace id (`ravel_rspan::record::TRACE_ID_WIDTH`).
pub const TRACE_ID_WIDTH: i32 = 16;
/// Byte width of a span id, also a parent span id
/// (`ravel_rspan::record::SPAN_ID_WIDTH`).
pub const SPAN_ID_WIDTH: i32 = 8;

fn spans_fields() -> Vec<Field> {
    vec![
        // trace_id and span_id are always present on a span (RSPAN's primary
        // key and identity), so both are non-nullable.
        Field::new("trace_id", DataType::FixedSizeBinary(TRACE_ID_WIDTH), false),
        Field::new("span_id", DataType::FixedSizeBinary(SPAN_ID_WIDTH), false),
        // A root span has no parent, so `parent_span_id` is nullable: an absent
        // parent is a NULL cell, never a zero-filled one.
        Field::new(
            "parent_span_id",
            DataType::FixedSizeBinary(SPAN_ID_WIDTH),
            true,
        ),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "start_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "end_ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        // The OTLP status code as its stored byte (0=Unset, 1=Ok, 2=Error).
        Field::new("status_code", DataType::UInt8, false),
        // OTLP status message is optional, so this column is nullable.
        Field::new("status_message", DataType::Utf8, true),
        // Same `Map(Utf8, Utf8)` type the `samples`/`logs` maps are built on
        // (the child field naming that arrow's `MapBuilder` produces by
        // default), so a built array and this declared type agree exactly.
        Field::new("attrs", label_map_type(), false),
        // Populated from attrs["service.name"] at scan time; NULL when the
        // span has no such attribute (see the module doc for why this is a
        // plain column rather than RSPAN v1's dictionary-encoded column 9).
        Field::new("service_name", DataType::Utf8, true),
        // Computed end_ts - start_ts (ADR-0045 decision 5), never stored.
        Field::new("duration_ns", DataType::Int64, false),
    ]
}

/// The public `spans` table schema.
pub fn spans_schema() -> SchemaRef {
    Arc::new(Schema::new(spans_fields()))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn schema_columns_are_in_the_documented_order_and_type() {
        let s = spans_schema();
        assert_eq!(s.fields().len(), 11);

        assert_eq!(s.field(SPAN_COL_TRACE_ID).name(), "trace_id");
        assert_eq!(
            s.field(SPAN_COL_TRACE_ID).data_type(),
            &DataType::FixedSizeBinary(16)
        );
        assert!(!s.field(SPAN_COL_TRACE_ID).is_nullable());

        assert_eq!(s.field(SPAN_COL_SPAN_ID).name(), "span_id");
        assert_eq!(
            s.field(SPAN_COL_SPAN_ID).data_type(),
            &DataType::FixedSizeBinary(8)
        );
        assert!(!s.field(SPAN_COL_SPAN_ID).is_nullable());

        assert_eq!(s.field(SPAN_COL_PARENT_SPAN_ID).name(), "parent_span_id");
        assert_eq!(
            s.field(SPAN_COL_PARENT_SPAN_ID).data_type(),
            &DataType::FixedSizeBinary(8)
        );
        assert!(s.field(SPAN_COL_PARENT_SPAN_ID).is_nullable());

        assert_eq!(s.field(SPAN_COL_NAME).data_type(), &DataType::Utf8);
        assert!(!s.field(SPAN_COL_NAME).is_nullable());

        assert_eq!(
            s.field(SPAN_COL_START_TS).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            s.field(SPAN_COL_END_TS).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        assert_eq!(s.field(SPAN_COL_STATUS_CODE).data_type(), &DataType::UInt8);
        assert!(!s.field(SPAN_COL_STATUS_CODE).is_nullable());

        assert_eq!(
            s.field(SPAN_COL_STATUS_MESSAGE).data_type(),
            &DataType::Utf8
        );
        assert!(s.field(SPAN_COL_STATUS_MESSAGE).is_nullable());

        assert_eq!(s.field(SPAN_COL_ATTRS).name(), "attrs");
        assert_eq!(s.field(SPAN_COL_ATTRS).data_type(), &label_map_type());
        assert!(!s.field(SPAN_COL_ATTRS).is_nullable());

        assert_eq!(s.field(SPAN_COL_SERVICE_NAME).name(), "service_name");
        assert_eq!(
            s.field(SPAN_COL_SERVICE_NAME).data_type(),
            &DataType::Utf8
        );
        assert!(s.field(SPAN_COL_SERVICE_NAME).is_nullable());

        assert_eq!(s.field(SPAN_COL_DURATION_NS).name(), "duration_ns");
        assert_eq!(s.field(SPAN_COL_DURATION_NS).data_type(), &DataType::Int64);
        assert!(!s.field(SPAN_COL_DURATION_NS).is_nullable());
    }
}
