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

use crate::declared::{DeclaredColumn, DeclaredType};
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

/// The first schema index a declared typed attribute column occupies (ADR-0090
/// decision 3). Declared columns are appended after `attrs` (index
/// [`LOG_COL_ATTRS`]), so the first one is index 9 and the `k`-th declared
/// column in list order is index `FIRST_DECLARED_COL + k`.
pub const FIRST_DECLARED_COL: usize = LOG_COL_ATTRS + 1;

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

/// The public `logs` table schema: the zero-declaration base (ADR-0090
/// decision 3). Unchanged by this ADR, so every existing caller and the public
/// re-export in `lib.rs` see exactly the nine fixed fields.
pub fn logs_schema() -> SchemaRef {
    Arc::new(Schema::new(logs_fields()))
}

/// The Arrow type a declared column of `ty` is projected as (ADR-0090
/// decision 3). Every declared column is nullable: a row whose value is absent
/// or whose decoded variant does not match the declared type reads NULL, never
/// a cast (ADR-0090 decision 7).
fn declared_arrow_type(ty: DeclaredType) -> DataType {
    match ty {
        // `Str` is dictionary-encoded end to end (ADR-0099 decision 5): a
        // dict-encoded page reaches Arrow as its dictionary and ids with no
        // per-row allocation, and the client receives it as
        // `Dictionary(Int32, Utf8)` over the public Flight statement path.
        DeclaredType::Str => {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        }
        DeclaredType::I64 => DataType::Int64,
        DeclaredType::Bool => DataType::Boolean,
        DeclaredType::Bytes => DataType::Binary,
    }
}

/// The `logs` table schema for a tenant with `declared` typed attribute columns
/// (ADR-0090 decision 3). The nine fixed fields of [`logs_schema`] are followed
/// by one nullable Arrow field per declared column, appended in `declared`'s
/// order (index [`FIRST_DECLARED_COL`] onward) and never re-sorted, so a
/// tenant's schema does not reorder between one plan and the next.
///
/// With an empty `declared` this is exactly [`logs_schema`]: the zero-
/// declaration base path is provably unchanged.
pub fn logs_schema_with_declared(declared: &[DeclaredColumn]) -> SchemaRef {
    let mut fields = logs_fields();
    for dc in declared {
        fields.push(Field::new(dc.key.clone(), declared_arrow_type(dc.ty), true));
    }
    Arc::new(Schema::new(fields))
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

    /// ADR-0090 decision 3: the zero-declaration path is provably unchanged.
    /// `logs_schema_with_declared(&[])` is the same `Schema` shape as the
    /// unchanged `logs_schema()`, so a tenant with no declared columns gets
    /// byte-for-byte the base schema this task did not touch.
    #[test]
    fn empty_declared_schema_equals_the_base_schema() {
        let base = logs_schema();
        let with_none = logs_schema_with_declared(&[]);
        assert_eq!(base.fields().len(), 9);
        assert_eq!(
            base.as_ref(),
            with_none.as_ref(),
            "zero declared columns must reproduce the base logs schema exactly"
        );
    }

    /// Declared columns are appended after `attrs` in declaration order, each
    /// nullable, mapped to the Arrow type its logical type declares (ADR-0090
    /// decision 3), and the order is preserved verbatim (never re-sorted).
    #[test]
    fn declared_columns_append_in_order_after_attrs() {
        let declared = vec![
            DeclaredColumn::new("z.last", DeclaredType::Str),
            DeclaredColumn::new("duration_ms", DeclaredType::I64),
            DeclaredColumn::new("ok", DeclaredType::Bool),
            DeclaredColumn::new("payload", DeclaredType::Bytes),
        ];
        let s = logs_schema_with_declared(&declared);
        assert_eq!(s.fields().len(), 9 + declared.len());
        // The first nine are still the fixed columns, ending at `attrs`.
        assert_eq!(s.field(LOG_COL_ATTRS).name(), "attrs");
        // Declared columns follow, in list order, each nullable.
        assert_eq!(s.field(FIRST_DECLARED_COL).name(), "z.last");
        assert_eq!(
            s.field(FIRST_DECLARED_COL).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
        assert_eq!(s.field(FIRST_DECLARED_COL + 1).name(), "duration_ms");
        assert_eq!(
            s.field(FIRST_DECLARED_COL + 1).data_type(),
            &DataType::Int64
        );
        assert_eq!(s.field(FIRST_DECLARED_COL + 2).name(), "ok");
        assert_eq!(
            s.field(FIRST_DECLARED_COL + 2).data_type(),
            &DataType::Boolean
        );
        assert_eq!(s.field(FIRST_DECLARED_COL + 3).name(), "payload");
        assert_eq!(
            s.field(FIRST_DECLARED_COL + 3).data_type(),
            &DataType::Binary
        );
        for i in FIRST_DECLARED_COL..s.fields().len() {
            assert!(
                s.field(i).is_nullable(),
                "declared column {i} must be nullable"
            );
        }
    }
}
