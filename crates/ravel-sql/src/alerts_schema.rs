//! The `alerts` table schema (ADR-0040 "`Signal::Alerts` and `Signal::Audit`,
//! sharing RLOG's format").
//!
//! An alert record is structurally a log record (ADR-0040 decision 2): it rides
//! RLOG v1 verbatim, with the alert-specific structured fields carried in the
//! record's `attrs` map (`alert_id`, `rule_id`, `state`, `generation`, and the
//! open-ended `label.<name>`/`annotation.<name>` set). This table promotes the
//! four always-present scalar fields into typed columns for ergonomic predicates
//! and joins, and exposes the *whole* merged attribute map (labels, annotations,
//! and the promoted keys themselves) through one `attrs` `Map(Utf8, Utf8)`
//! column.
//!
//! | column       | arrow type                    | source                                    |
//! |--------------|-------------------------------|-------------------------------------------|
//! | ts_ns        | Timestamp(Nanosecond, None)   | `LogRecord::ts_ns` (transition event time)|
//! | alert_id     | Utf8, nullable                | `attrs["alert_id"]` (hex stable hash)     |
//! | rule_id      | Utf8, nullable                | `attrs["rule_id"]`                        |
//! | state        | Utf8, nullable                | `attrs["state"]` (firing/resolved/...)    |
//! | generation   | Int64, nullable               | `attrs["generation"]` (recursion guard)   |
//! | writer_id    | Utf8                          | `SegmentRef::writer_id` of the object     |
//! | writer_epoch | UInt64                        | `SegmentRef::writer_epoch`                |
//! | writer_seq   | UInt64                        | `SegmentRef::writer_seq`                  |
//! | attrs        | Map(Utf8, Utf8)               | full merged resource/scope + record attrs |
//!
//! # Why the write identity is a column
//!
//! The table is raw history, one row per transition (ADR-1101 decision 1), and
//! current state is a SQL fold over it. `ts_ns` alone cannot order that fold:
//! two evaluators can overlap briefly at a lease handover and write the same
//! `alert_id` at the same `ts_ns`, which is why the evaluator's own fold orders
//! by `(ts_ns, epoch, seq)`. The three identity columns expose the same keys
//! plus `writer_id`, so `ORDER BY ts_ns DESC, writer_epoch DESC, writer_seq
//! DESC, writer_id DESC` is a total order and a `ROW_NUMBER() = 1` fold can
//! never return two "current" rows for one alert. They are stamped per record
//! from the commit record of the object the record was read from
//! (crate::alerts_scan), never from a record attribute, so a record cannot
//! misreport its own writer.
//!
//! # Why a `Map` column for labels/annotations, not per-name columns
//!
//! The label and annotation key sets are per-rule and open-ended: a fixed
//! `label_<name>`/`annotation_<name>` column schema cannot represent a set the
//! table does not know at construction time, and would differ row to row. This
//! is the same reason the `logs` table (ADR-0033) and the metrics `samples`
//! table expose dynamic attributes as one `Map(Utf8, Utf8)` rather than per-key
//! columns, and reusing [`label_map_type`] keeps a single map representation
//! across every table. The four scalar fields are promoted to typed columns
//! precisely because they are *not* open-ended: every well-formed alert record
//! carries exactly those keys (ADR-0040 decision 2).

use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::schema::label_map_type;

pub const ALERT_COL_TS: usize = 0;
pub const ALERT_COL_ALERT_ID: usize = 1;
pub const ALERT_COL_RULE_ID: usize = 2;
pub const ALERT_COL_STATE: usize = 3;
pub const ALERT_COL_GENERATION: usize = 4;
pub const ALERT_COL_WRITER_ID: usize = 5;
pub const ALERT_COL_WRITER_EPOCH: usize = 6;
pub const ALERT_COL_WRITER_SEQ: usize = 7;
pub const ALERT_COL_ATTRS: usize = 8;

/// The attribute key an alert record's stable id is carried under.
pub const ALERT_ID_KEY: &str = "alert_id";
/// The attribute key an alert record's owning rule id is carried under.
pub const RULE_ID_KEY: &str = "rule_id";
/// The attribute key an alert record's state is carried under.
pub const STATE_KEY: &str = "state";
/// The attribute key an alert record's generation counter is carried under.
pub const GENERATION_KEY: &str = "generation";

fn alerts_fields() -> Vec<Field> {
    vec![
        Field::new(
            "ts_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        // The four promoted scalar fields are nullable: a well-formed alert
        // record always carries them, but a record missing or mistyping one
        // yields a NULL cell rather than a decode failure or a fabricated value.
        Field::new("alert_id", DataType::Utf8, true),
        Field::new("rule_id", DataType::Utf8, true),
        Field::new("state", DataType::Utf8, true),
        // `generation` is stored as an I64 attribute (ADR-0040 decision 4); the
        // column mirrors that type exactly rather than a UInt, so a value read
        // back never has to be range-checked or reinterpreted.
        Field::new("generation", DataType::Int64, true),
        // The write identity of the object the record was read from, stamped by
        // the scan (ADR-1101 decision 1's row contract). Non-null: every
        // resolved segment carries all three, so a NULL here would mean a
        // record with no provenance rather than a missing attribute.
        Field::new("writer_id", DataType::Utf8, false),
        Field::new("writer_epoch", DataType::UInt64, false),
        Field::new("writer_seq", DataType::UInt64, false),
        // Same `Map(Utf8, Utf8)` type the `logs` table's `attrs` column is built
        // on, so a built array and this declared type agree exactly.
        Field::new("attrs", label_map_type(), false),
    ]
}

/// The public `alerts` table schema.
pub fn alerts_schema() -> SchemaRef {
    Arc::new(Schema::new(alerts_fields()))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn schema_columns_are_in_the_documented_order_and_type() {
        let s = alerts_schema();
        assert_eq!(s.fields().len(), 9);
        assert_eq!(s.field(ALERT_COL_TS).name(), "ts_ns");
        assert_eq!(
            s.field(ALERT_COL_TS).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert!(!s.field(ALERT_COL_TS).is_nullable());
        assert_eq!(s.field(ALERT_COL_ALERT_ID).name(), "alert_id");
        assert_eq!(s.field(ALERT_COL_ALERT_ID).data_type(), &DataType::Utf8);
        assert!(s.field(ALERT_COL_ALERT_ID).is_nullable());
        assert_eq!(s.field(ALERT_COL_RULE_ID).name(), "rule_id");
        assert_eq!(s.field(ALERT_COL_STATE).name(), "state");
        assert_eq!(s.field(ALERT_COL_GENERATION).name(), "generation");
        assert_eq!(s.field(ALERT_COL_GENERATION).data_type(), &DataType::Int64);
        assert!(s.field(ALERT_COL_GENERATION).is_nullable());
        // The write-identity columns (ADR-1101 decision 1) are non-null and
        // typed to the commit record's own fields.
        assert_eq!(s.field(ALERT_COL_WRITER_ID).name(), "writer_id");
        assert_eq!(s.field(ALERT_COL_WRITER_ID).data_type(), &DataType::Utf8);
        assert!(!s.field(ALERT_COL_WRITER_ID).is_nullable());
        assert_eq!(s.field(ALERT_COL_WRITER_EPOCH).name(), "writer_epoch");
        assert_eq!(
            s.field(ALERT_COL_WRITER_EPOCH).data_type(),
            &DataType::UInt64
        );
        assert!(!s.field(ALERT_COL_WRITER_EPOCH).is_nullable());
        assert_eq!(s.field(ALERT_COL_WRITER_SEQ).name(), "writer_seq");
        assert_eq!(s.field(ALERT_COL_WRITER_SEQ).data_type(), &DataType::UInt64);
        assert!(!s.field(ALERT_COL_WRITER_SEQ).is_nullable());
        assert_eq!(s.field(ALERT_COL_ATTRS).name(), "attrs");
        assert_eq!(s.field(ALERT_COL_ATTRS).data_type(), &label_map_type());
        assert!(!s.field(ALERT_COL_ATTRS).is_nullable());
    }
}
