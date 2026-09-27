//! The crate's single typed error. Every fallible path here returns
//! [`AlertError`]; nothing in this crate panics, unwraps, or expects on a
//! production path (CLAUDE.md invariants).

use crate::record::AlertId;

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

    /// A [`crate::Rule`] failed a structural validation check
    /// ([`crate::Rule::validate`]) and cannot be evaluated safely: an empty
    /// `rule_id` or query text, or a condition whose result shape cannot apply
    /// to the rule's query language. `reason` names the specific invariant.
    #[error("invalid rule {rule_id:?}: {reason}")]
    InvalidRule {
        rule_id: String,
        reason: &'static str,
    },

    /// A rule's query matched more series than one rule may raise alerts for
    /// ([`crate::MAX_ALERTS_PER_RULE`], ADR-0117 decision 3). The rule fails
    /// the tick: no record is written and its prior state is left as it was.
    /// `count` is the number of matched series, not the result vector size.
    #[error("rule {rule_id:?} matched {count} series, over the limit of {limit} alerts per rule")]
    TooManyAlerts {
        rule_id: String,
        count: usize,
        limit: usize,
    },

    /// Two series of one rule produced the same merged label set, and so the
    /// same alert identity, in one evaluation (ADR-0117 decision 2): typically
    /// a query selecting several metric names, which differ only in the
    /// dropped `__name__`, or a series label overridden by a rule label. The
    /// rule fails the tick rather than let one series silently overwrite the
    /// other. The message names the label names and the colliding `alert_id`
    /// but no label value, because it is logged on every failing tick.
    #[error(
        "rule {rule_id:?} produced alert {} with label names [{}] from more than one series",
        alert_id.to_hex(),
        label_names(labels)
    )]
    DuplicateAlertIdentity {
        rule_id: String,
        alert_id: AlertId,
        labels: Vec<(String, String)>,
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

/// The names of `labels`, comma-separated, for a message that must not carry
/// label values.
fn label_names(labels: &[(String, String)]) -> String {
    labels
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}
