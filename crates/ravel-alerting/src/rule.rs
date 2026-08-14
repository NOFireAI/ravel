//! The generic rule shape (ADR-0043 decision 1).
//!
//! One [`Rule`] type covers both observability alert rules (a PromQL query
//! plus a numeric threshold) and security detection rules (a SQL query plus a
//! nonempty-result condition). There is deliberately no second rule engine:
//! the query language and the condition vary, everything else is shared.

use std::time::Duration;

use crate::error::AlertError;

/// The query a rule evaluates. This crate never executes the query itself
/// (that is the caller's `QueryEngine`/`SqlExecutor` wiring); the variant only
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
    /// Heuristic on the query text; the caller's planner resolves the real table
    /// set. Ordinary metric/log rules always compute `generation = 0`.
    pub fn targets_alerts_table(&self) -> bool {
        match self {
            // PromQL has no `alerts` table; it reads metric samples only.
            RuleQuery::Promql(_) => false,
            // Scan for the `alerts` identifier only after removing string
            // literals and comments, so the word appearing inside a quoted
            // string (`WHERE body = 'alerts'`) or a comment is not misread as a
            // table reference. See [`strip_sql_literals_and_comments`].
            RuleQuery::Sql(text) => strip_sql_literals_and_comments(text)
                .to_ascii_lowercase()
                .split(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                .any(|tok| tok == "alerts"),
        }
    }
}

/// Replaces every SQL string literal and comment in `text` with a single space,
/// so a token scan over the result never sees a word that appears only inside a
/// quoted string or a comment rather than as an identifier.
///
/// This is deliberately a small heuristic, not a SQL parser. This crate is pure
/// logic with no SQL parser dependency and must not gain one (ADR-0043 keeps
/// the real table-set resolution in the caller's planner); the reachable
/// `ravel-sql` crate offers no lightweight lexer to reuse (its parsing is
/// DataFusion's, behind a heavy dependency this crate does not take). Removing
/// single- and double-quoted literals (honoring both a backslash escape and the
/// SQL `''` doubled-quote escape) and `--` line and `/* */` block comments is
/// enough to keep a literal or comment containing a table name from being read
/// as a reference to that table. Each removed span becomes one space so tokens
/// on either side of it never merge.
fn strip_sql_literals_and_comments(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            // `--` line comment: drop through end of line.
            '-' if chars.peek() == Some(&'-') => {
                chars.next();
                for c in chars.by_ref() {
                    if c == '\n' {
                        break;
                    }
                }
                out.push(' ');
            }
            // `/* ... */` block comment: drop through the closing `*/`.
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = '\0';
                for c in chars.by_ref() {
                    if prev == '*' && c == '/' {
                        break;
                    }
                    prev = c;
                }
                out.push(' ');
            }
            // Single- or double-quoted string literal: drop through the
            // matching unescaped closing quote.
            '\'' | '"' => {
                let quote = c;
                while let Some(c) = chars.next() {
                    if c == '\\' {
                        // Backslash escapes the next character, if any.
                        chars.next();
                    } else if c == quote {
                        if chars.peek() == Some(&quote) {
                            // A doubled quote is an escaped quote: stay inside.
                            chars.next();
                        } else {
                            break;
                        }
                    }
                }
                out.push(' ');
            }
            _ => out.push(c),
        }
    }
    out
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

impl Rule {
    /// Checks the structural invariants a `Rule` must hold to be evaluated
    /// safely, so this crate's own callers (an evaluator constructing or
    /// loading rules) can reject a bad rule up front rather than have it fail
    /// every tick. Checks:
    ///
    /// - a non-empty `rule_id` (it is part of every alert's identity, see
    ///   [`crate::compute_alert_id`]);
    /// - non-empty query text (an empty query has nothing to evaluate);
    /// - a condition whose result shape can apply to the query language: a
    ///   PromQL query yields per-series numeric values and takes a
    ///   [`RuleCondition::Threshold`]; a SQL query yields a row set and takes a
    ///   [`RuleCondition::NonEmptyResult`]. The mismatched pairing would return
    ///   [`AlertError::ResultShapeMismatch`] from [`crate::condition_met`] on
    ///   every tick, so it is caught once here.
    ///
    /// This is the in-crate equivalent of the startup check `ravel-server`'s
    /// config parser performs at the config layer; the two currently duplicate
    /// the query/condition-shape rule.
    pub fn validate(&self) -> Result<(), AlertError> {
        let invalid = |reason: &'static str| AlertError::InvalidRule {
            rule_id: self.rule_id.clone(),
            reason,
        };

        if self.rule_id.trim().is_empty() {
            return Err(invalid("empty rule_id"));
        }

        let query_text = match &self.query {
            RuleQuery::Promql(text) | RuleQuery::Sql(text) => text,
        };
        if query_text.trim().is_empty() {
            return Err(invalid("empty query text"));
        }

        match (&self.query, &self.condition) {
            (RuleQuery::Promql(_), RuleCondition::NonEmptyResult) => Err(invalid(
                "a PromQL query yields per-series values and takes a threshold condition, \
                 not a nonempty-result condition",
            )),
            (RuleQuery::Sql(_), RuleCondition::Threshold { .. }) => Err(invalid(
                "a SQL query yields a row set and takes a nonempty-result condition, \
                 not a threshold condition",
            )),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sql(text: &str) -> RuleQuery {
        RuleQuery::Sql(text.into())
    }

    #[test]
    fn sql_reading_the_alerts_table_is_classified() {
        assert!(sql("SELECT * FROM alerts WHERE state = 'firing'").targets_alerts_table());
        // Case-insensitive and quoted/aliased identifiers still count.
        assert!(sql("select count(*) from ALERTS").targets_alerts_table());
        assert!(
            sql("SELECT * FROM logs JOIN alerts ON logs.id = alerts.id").targets_alerts_table()
        );
    }

    #[test]
    fn alerts_only_inside_a_string_literal_is_not_classified() {
        // The word appears as a string literal value, not a table reference.
        assert!(!sql("SELECT 1 WHERE body = 'alerts'").targets_alerts_table());
        assert!(!sql(r#"SELECT 1 FROM logs WHERE msg = "alerts""#).targets_alerts_table());
        // A doubled-quote escape inside the literal must not end the string
        // early and expose the word to the scan.
        assert!(!sql("SELECT 1 WHERE body = 'the ''alerts'' table'").targets_alerts_table());
        // A backslash-escaped quote likewise keeps the scan inside the literal.
        assert!(!sql(r#"SELECT 1 WHERE body = 'a\'alerts\'b'"#).targets_alerts_table());
    }

    #[test]
    fn alerts_only_inside_a_comment_is_not_classified() {
        assert!(!sql("SELECT 1 FROM logs -- read from alerts later\n").targets_alerts_table());
        assert!(!sql("SELECT 1 FROM logs /* alerts */ WHERE x = 1").targets_alerts_table());
    }

    #[test]
    fn a_real_reference_alongside_a_literal_still_classifies() {
        // A genuine table reference is not masked by an unrelated literal that
        // also contains the word.
        assert!(sql("SELECT * FROM alerts WHERE body = 'alerts'").targets_alerts_table());
    }

    #[test]
    fn promql_never_targets_the_alerts_table() {
        assert!(!RuleQuery::Promql("alerts_total > 0".into()).targets_alerts_table());
    }

    fn rule(query: RuleQuery, condition: RuleCondition) -> Rule {
        Rule {
            rule_id: "r1".into(),
            query,
            condition,
            labels: vec![],
            annotations: vec![],
            for_duration: None,
            max_alert_generation: None,
        }
    }

    #[test]
    fn validate_accepts_well_formed_rules() {
        let promql = rule(
            RuleQuery::Promql("cpu > 0.9".into()),
            RuleCondition::Threshold {
                op: ThresholdOp::Gt,
                threshold: 0.9,
            },
        );
        assert!(promql.validate().is_ok());

        let sql_rule = rule(sql("SELECT * FROM alerts"), RuleCondition::NonEmptyResult);
        assert!(sql_rule.validate().is_ok());
    }

    #[test]
    fn validate_rejects_empty_rule_id() {
        let mut r = rule(
            RuleQuery::Promql("cpu".into()),
            RuleCondition::Threshold {
                op: ThresholdOp::Gt,
                threshold: 1.0,
            },
        );
        r.rule_id = "   ".into();
        assert!(matches!(r.validate(), Err(AlertError::InvalidRule { .. })));
    }

    #[test]
    fn validate_rejects_empty_query_text() {
        let r = rule(RuleQuery::Sql("   ".into()), RuleCondition::NonEmptyResult);
        assert!(matches!(r.validate(), Err(AlertError::InvalidRule { .. })));
    }

    #[test]
    fn validate_rejects_condition_that_cannot_apply_to_the_query_shape() {
        // PromQL with a nonempty-result condition.
        let r = rule(
            RuleQuery::Promql("cpu".into()),
            RuleCondition::NonEmptyResult,
        );
        assert!(matches!(r.validate(), Err(AlertError::InvalidRule { .. })));

        // SQL with a threshold condition.
        let r = rule(
            sql("SELECT * FROM logs"),
            RuleCondition::Threshold {
                op: ThresholdOp::Gt,
                threshold: 1.0,
            },
        );
        assert!(matches!(r.validate(), Err(AlertError::InvalidRule { .. })));
    }
}
