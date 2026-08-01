//! The crate's single typed error. Every fallible path here returns
//! [`AlertError`]; nothing in this crate panics, unwraps, or expects on a
//! production path (CLAUDE.md invariants).

/// An alerting-engine error.
#[derive(Debug, thiserror::Error)]
pub enum AlertError {
    /// A produced record's computed `generation` exceeded the effective
    /// `max_alert_generation` circuit breaker (ADR-0040 decision 4). This is
    /// the recursion guard on alerts-on-alerts, returned instead of silently
    /// dropping the record or looping forever.
    #[error("alert generation {generation} exceeds max_alert_generation {limit}")]
    GenerationExceeded { generation: u32, limit: u32 },

    /// A rule's condition does not match the shape of the query result it was
    /// handed: a threshold condition against a tabular (row-count) result, or a
    /// nonempty-result condition against a numeric (per-series) result. The
    /// caller paired a PromQL-shaped condition with a SQL-shaped result or vice
    /// versa.
    #[error("condition {condition} cannot evaluate a {result} query result")]
    ResultShapeMismatch {
        condition: &'static str,
        result: &'static str,
    },

    /// A stored alert record could not be decoded back into an [`AlertRecord`]:
    /// a required attr was missing or carried the wrong value type. The message
    /// names the offending field.
    #[error("malformed alert record: {0}")]
    MalformedRecord(String),

    /// The underlying RLOG writer or reader rejected the record bytes. Carries
    /// `ravel-logseg`'s own typed error unchanged.
    #[error(transparent)]
    Segment(#[from] ravel_logseg::LogSegError),
}
