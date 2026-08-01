//! The generic rule shape (ADR-0043 decision 1).
//!
//! One [`Rule`] type covers both observability alert rules (a PromQL query
//! plus a numeric threshold) and security detection rules (a SQL query plus a
//! nonempty-result condition). There is deliberately no second rule engine:
//! the query language and the condition vary, everything else is shared.

use std::time::Duration;

/// The query a rule evaluates. This crate never executes the query itself
/// (that is Wave 2's `QueryEngine`/`SqlExecutor` wiring); the variant only
/// records the language and text so the evaluator can route it and so the
/// resulting record can name its source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RuleQuery {
    /// A PromQL expression, evaluated to an instant vector of per-series
    /// values.
    Promql(String),
    /// A SQL query over one of the signal tables (`samples`, `logs`, or the
    /// `alerts` table for alerts-on-alerts), evaluated to a row set.
    Sql(String),
}

impl RuleQuery {
    /// True when this rule reads the `alerts` table as input, the only case
    /// where [`crate::compute_generation`] applies (ADR-0043 decision 5).
    /// Heuristic on the query text; Wave 2's planner resolves the real table
    /// set. Ordinary metric/log rules always compute `generation = 0`.
    pub fn targets_alerts_table(&self) -> bool {
        match self {
            // PromQL has no `alerts` table; it reads metric samples only.
            RuleQuery::Promql(_) => false,
            RuleQuery::Sql(text) => text
                .to_ascii_lowercase()
                .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .any(|tok| tok == "alerts"),
        }
    }
}

/// The comparison a threshold condition applies between each series value and
/// the rule's threshold. Covers the standard PromQL alerting comparators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThresholdOp {
    /// `value > threshold`
    Gt,
    /// `value >= threshold`
    Ge,
    /// `value < threshold`
    Lt,
    /// `value <= threshold`
    Le,
    /// `value == threshold` (exact IEEE-754 equality)
    Eq,
    /// `value != threshold`
    Ne,
}

/// How a query result maps to firing (ADR-0043 decision 1). Two variants, one
/// per result shape; the minimal set that covers both rule families without
/// over-generalizing.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RuleCondition {
    /// PromQL-shaped numeric results: the rule fires when *any* series value
    /// satisfies `value <op> threshold`. This is "any series exceeds a
    /// threshold" generalized over the six comparators.
    Threshold { op: ThresholdOp, threshold: f64 },
    /// SQL-shaped tabular results: the rule fires when the result has at least
    /// one row (`row count > 0`). This is the detection-rule condition.
    NonEmptyResult,
}

impl RuleCondition {
    /// A short static name for error messages.
    pub fn kind(&self) -> &'static str {
        match self {
            RuleCondition::Threshold { .. } => "threshold",
            RuleCondition::NonEmptyResult => "nonempty-result",
        }
    }
}

/// A single alerting or detection rule. Static per-tenant config in v1
/// (ADR-0043 decision 2); this crate holds only the shape and the pure logic
/// over it, not the config-loading or scheduling.
#[derive(Clone, Debug, PartialEq)]
pub struct Rule {
    /// Stable operator-chosen identifier for the rule. Part of every alert's
    /// identity (see [`crate::compute_alert_id`]).
    pub rule_id: String,
    /// The query whose result the condition is evaluated against.
    pub query: RuleQuery,
    /// How the query result maps to firing.
    pub condition: RuleCondition,
    /// Labels attached to every alert this rule produces. Together with
    /// `rule_id` these form the alert's stable identity.
    pub labels: Vec<(String, String)>,
    /// Human-facing annotations (summary, runbook, ...) carried on the record
    /// for notification. Not part of alert identity.
    pub annotations: Vec<(String, String)>,
    /// Pending-before-firing delay (PromQL alerting's `for`). `None` (or zero)
    /// fires on the first tick the condition holds; `Some(d)` requires the
    /// condition to hold continuously for `d` before firing.
    pub for_duration: Option<Duration>,
    /// Per-rule override of the generation circuit breaker. `None` uses
    /// [`crate::DEFAULT_MAX_ALERT_GENERATION`] (ADR-0040's global default).
    pub max_alert_generation: Option<u32>,
}
