//! The `audit` table schema (ADR-0040 "`Signal::Alerts` and `Signal::Audit`,
//! sharing RLOG's format", epic #333 / issue #383).
//!
//! An audit record is structurally a log record (ADR-0040 decision 2): it rides
//! RLOG v1 verbatim. Unlike the `alerts` table, this table promotes *no*
//! record-specific fields into typed columns. It is deliberately generic,
//! exposing only the fields every RLOG record has -- event time, severity text,
//! body -- plus the whole merged attribute map through one `attrs`
//! `Map(Utf8, Utf8)` column.
//!
//! | column        | arrow type                    | source                              |
//! |---------------|-------------------------------|-------------------------------------|
//! | ts_ns         | Timestamp(Nanosecond, None)   | `LogRecord::ts_ns`                  |
//! | severity_text | Utf8                          | `LogRecord::severity_text`          |
//! | body          | Utf8                          | `LogRecord::body`                   |
//! | attrs         | Map(Utf8, Utf8)               | full merged resource/scope + record |
//!
//! # Why generic, not legal-hold-specific
//!
//! The one audit record kind shipped so far is the legal-hold record
//! (`kind=legal_hold`, `hold.op`, `hold.scope`, `hold.reason`), but audit is a
//! general signal: query-audit and admin-action records (ADR-0042) will land
//! later and write the same table. Modelling the table on the generic RLOG
//! record shape -- every record-specific field flowing through the `attrs` map
//! rather than a fixed set of legal-hold columns -- means those later kinds need
//! no schema change. A predicate over a specific kind's fields
//! (`attrs['kind'] = 'legal_hold'`, `attrs['hold.scope'] = '...'`) is evaluated
//! by DataFusion's residual over the `attrs` column, exactly as an arbitrary
//! attribute predicate is on the `logs` table.

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::schema::label_map_type;

pub const AUDIT_COL_TS: usize = 0;
pub const AUDIT_COL_SEVERITY_TEXT: usize = 1;
pub const AUDIT_COL_BODY: usize = 2;
pub const AUDIT_COL_ATTRS: usize = 3;

fn audit_fields() -> Vec<Field> {
    vec![
        Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("severity_text", DataType::Utf8, false),
        Field::new("body", DataType::Utf8, false),
        // Same `Map(Utf8, Utf8)` type the `logs` table's `attrs` column is built
        // on, so a built array and this declared type agree exactly.
        Field::new("attrs", label_map_type(), false),
    ]
}

/// The public `audit` table schema.
pub fn audit_schema() -> SchemaRef {
    Arc::new(Schema::new(audit_fields()))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn schema_columns_are_in_the_documented_order_and_type() {
        let s = audit_schema();
        assert_eq!(s.fields().len(), 4);
        assert_eq!(s.field(AUDIT_COL_TS).name(), "ts_ns");
        assert_eq!(
            s.field(AUDIT_COL_TS).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(s.field(AUDIT_COL_SEVERITY_TEXT).name(), "severity_text");
        assert_eq!(
            s.field(AUDIT_COL_SEVERITY_TEXT).data_type(),
            &DataType::Utf8
        );
        assert_eq!(s.field(AUDIT_COL_BODY).name(), "body");
        assert_eq!(s.field(AUDIT_COL_BODY).data_type(), &DataType::Utf8);
        assert_eq!(s.field(AUDIT_COL_ATTRS).name(), "attrs");
        assert_eq!(s.field(AUDIT_COL_ATTRS).data_type(), &label_map_type());
    }
}
