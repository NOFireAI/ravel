//! Per-series alert identity (ADR-0117).
//!
//! A PromQL rule raises one alert per matched series. Each alert's label set
//! is the series labels without `__name__`, overlaid by the rule's labels, and
//! its identity is [`compute_alert_id`] over that merged set. A rule whose
//! query returns a scalar, and every SQL rule, matches with an empty series
//! label set, so its identity is the rule-labels-only identity it always had.

use std::collections::BTreeMap;

use ravel_types::{LabelSet, METRIC_NAME_LABEL};

use crate::condition::{QueryResultSummary, matching_series};
use crate::error::AlertError;
use crate::record::{AlertId, AlertRecord, compute_alert_id};
use crate::rule::Rule;

/// The most alerts one rule may raise in one evaluation (ADR-0117 decision 3).
/// It counts matched series, not the size of the result vector. A crate
/// constant rather than a setting, so the alert state memo, sink fan-out and
/// per-tick publish cost of every tenant share one known bound.
pub const MAX_ALERTS_PER_RULE: usize = 1000;

/// One alert of a rule: its identity and the label set that identity hashes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlertInstance {
    /// `compute_alert_id(rule_id, labels)` for an instance built by
    /// [`AlertInstance::new`]; the record's own id for one built by
    /// [`AlertInstance::of_record`].
    pub alert_id: AlertId,
    /// The alert's labels, sorted by `(name, value)`.
    pub labels: Vec<(String, String)>,
}

impl AlertInstance {
    /// The instance `rule_id` raises for `labels`.
    pub fn new(rule_id: &str, mut labels: Vec<(String, String)>) -> AlertInstance {
        labels.sort_unstable();
        AlertInstance {
            alert_id: compute_alert_id(rule_id, &labels),
            labels,
        }
    }

    /// The instance a stored record belongs to, keeping the record's own
    /// `alert_id` so a transition written for it folds onto the same alert.
    pub fn of_record(record: &AlertRecord) -> AlertInstance {
        let mut labels = record.labels.clone();
        labels.sort_unstable();
        AlertInstance {
            alert_id: record.alert_id,
            labels,
        }
    }
}

/// The label set of the alert a rule raises for one series (ADR-0117 decision
/// 2): the series labels without `__name__`, overlaid by `rule_labels`. A rule
/// label wins on a name clash. Sorted by name.
pub fn merged_alert_labels(
    rule_labels: &[(String, String)],
    series: &LabelSet,
) -> Vec<(String, String)> {
    let mut merged: BTreeMap<&str, &str> = series
        .iter()
        .filter(|l| l.name != METRIC_NAME_LABEL)
        .map(|l| (l.name.as_str(), l.value.as_str()))
        .collect();
    for (name, value) in rule_labels {
        merged.insert(name.as_str(), value.as_str());
    }
    merged
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect()
}

/// The alerts `rule` raises for `result`: one per series [`matching_series`]
/// returns, sorted by label set.
///
/// Fails with [`AlertError::TooManyAlerts`] when more than
/// [`MAX_ALERTS_PER_RULE`] series match, and with
/// [`AlertError::DuplicateAlertIdentity`] when two matched series merge to the
/// same label set. Both are checked before the caller writes anything, so a
/// failing rule leaves its prior state untouched.
pub fn alert_instances(
    rule: &Rule,
    result: &QueryResultSummary,
) -> Result<Vec<AlertInstance>, AlertError> {
    let matched = matching_series(&rule.condition, result)?;
    if matched.len() > MAX_ALERTS_PER_RULE {
        return Err(AlertError::TooManyAlerts {
            rule_id: rule.rule_id.clone(),
            count: matched.len(),
            limit: MAX_ALERTS_PER_RULE,
        });
    }
    let mut instances: Vec<AlertInstance> = matched
        .iter()
        .map(|(series, _)| {
            AlertInstance::new(&rule.rule_id, merged_alert_labels(&rule.labels, series))
        })
        .collect();
    instances.sort_unstable_by(|a, b| a.labels.cmp(&b.labels));
    if let Some(pair) = instances.windows(2).find(|w| w[0].labels == w[1].labels) {
        return Err(AlertError::DuplicateAlertIdentity {
            rule_id: rule.rule_id.clone(),
            labels: pair[0].labels.clone(),
        });
    }
    Ok(instances)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::rule::{RuleCondition, RuleQuery, ThresholdOp};
    use ravel_types::Label;

    fn series(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(name, value)| Label {
                    name: (*name).to_string(),
                    value: (*value).to_string(),
                })
                .collect(),
        )
        .expect("distinct label names")
    }

    fn pairs(ps: &[(&str, &str)]) -> Vec<(String, String)> {
        ps.iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn down_rule(labels: &[(&str, &str)]) -> Rule {
        Rule {
            rule_id: "instance-down".into(),
            query: RuleQuery::Promql("up".into()),
            condition: RuleCondition::Threshold {
                op: ThresholdOp::Eq,
                threshold: 0.0,
            },
            labels: pairs(labels),
            annotations: vec![],
            for_duration: None,
            max_alert_generation: None,
            repeat_interval: None,
        }
    }

    /// `n` down `up` series, `host-0000` onward.
    fn down_series(n: usize) -> QueryResultSummary {
        QueryResultSummary::Numeric(
            (0..n)
                .map(|i| {
                    let instance = format!("host-{i:04}");
                    (
                        series(&[("__name__", "up"), ("instance", instance.as_str())]),
                        0.0,
                    )
                })
                .collect(),
        )
    }

    #[test]
    fn merged_labels_drop_metric_name_and_rule_labels_win() {
        let merged = merged_alert_labels(
            &pairs(&[("severity", "page"), ("team", "rule-side")]),
            &series(&[
                ("__name__", "up"),
                ("instance", "a"),
                ("team", "series-side"),
            ]),
        );
        assert_eq!(
            merged,
            pairs(&[
                ("instance", "a"),
                ("severity", "page"),
                ("team", "rule-side")
            ])
        );
    }

    #[test]
    fn a_scalar_result_keeps_the_rule_labels_only_identity() {
        // An empty series label set must reproduce the identity a rule had
        // before per-series evaluation, byte for byte.
        let rule = down_rule(&[("severity", "page")]);
        let instances = alert_instances(
            &rule,
            &QueryResultSummary::Numeric(vec![(LabelSet::default(), 0.0)]),
        )
        .expect("one match");
        assert_eq!(
            instances,
            vec![AlertInstance {
                alert_id: compute_alert_id("instance-down", &rule.labels),
                labels: pairs(&[("severity", "page")]),
            }]
        );
    }

    #[test]
    fn a_sql_rule_raises_one_alert_with_the_rule_labels() {
        let rule = Rule {
            rule_id: "denied".into(),
            query: RuleQuery::Sql("select 1".into()),
            condition: RuleCondition::NonEmptyResult,
            labels: pairs(&[("severity", "ticket")]),
            annotations: vec![],
            for_duration: None,
            max_alert_generation: None,
            repeat_interval: None,
        };
        let fired = alert_instances(&rule, &QueryResultSummary::RowCount(7)).expect("rows");
        assert_eq!(
            fired,
            vec![AlertInstance::new(
                "denied",
                pairs(&[("severity", "ticket")])
            )]
        );
        let quiet = alert_instances(&rule, &QueryResultSummary::RowCount(0)).expect("no rows");
        assert_eq!(quiet, Vec::new());
    }

    #[test]
    fn each_matched_series_is_its_own_alert() {
        let rule = down_rule(&[("severity", "page")]);
        let instances = alert_instances(&rule, &down_series(3)).expect("three matches");
        let expected: Vec<AlertInstance> = ["host-0000", "host-0001", "host-0002"]
            .iter()
            .map(|host| {
                AlertInstance::new(
                    "instance-down",
                    pairs(&[("instance", host), ("severity", "page")]),
                )
            })
            .collect();
        assert_eq!(instances, expected);
        assert_ne!(instances[0].alert_id, instances[1].alert_id);
        assert_ne!(instances[1].alert_id, instances[2].alert_id);
    }

    #[test]
    fn the_cap_admits_exactly_max_alerts_per_rule() {
        let rule = down_rule(&[]);
        let instances =
            alert_instances(&rule, &down_series(MAX_ALERTS_PER_RULE)).expect("at the cap");
        assert_eq!(instances.len(), 1000);
    }

    #[test]
    fn one_series_over_the_cap_is_too_many_alerts() {
        let rule = down_rule(&[]);
        let err = alert_instances(&rule, &down_series(MAX_ALERTS_PER_RULE + 1))
            .expect_err("over the cap");
        match err {
            AlertError::TooManyAlerts {
                rule_id,
                count,
                limit,
            } => {
                assert_eq!(rule_id, "instance-down");
                assert_eq!(count, 1001);
                assert_eq!(limit, 1000);
            }
            other => panic!("expected TooManyAlerts, got {other:?}"),
        }
    }

    #[test]
    fn the_cap_counts_matched_series_not_the_result_vector() {
        // 1001 series of which only 1000 match: under the cap.
        let rule = down_rule(&[]);
        let QueryResultSummary::Numeric(mut all) = down_series(MAX_ALERTS_PER_RULE + 1) else {
            unreachable!("down_series builds a numeric summary");
        };
        all[0].1 = 1.0;
        let instances =
            alert_instances(&rule, &QueryResultSummary::Numeric(all)).expect("1000 match");
        assert_eq!(instances.len(), 1000);
    }

    #[test]
    fn two_series_merging_to_one_label_set_is_a_duplicate_identity() {
        // Two metric names differing only in the dropped `__name__`.
        let rule = down_rule(&[]);
        let result = QueryResultSummary::Numeric(vec![
            (series(&[("__name__", "up"), ("instance", "a")]), 0.0),
            (series(&[("__name__", "probe_up"), ("instance", "a")]), 0.0),
        ]);
        let err = alert_instances(&rule, &result).expect_err("duplicate");
        match err {
            AlertError::DuplicateAlertIdentity { rule_id, labels } => {
                assert_eq!(rule_id, "instance-down");
                assert_eq!(labels, pairs(&[("instance", "a")]));
            }
            other => panic!("expected DuplicateAlertIdentity, got {other:?}"),
        }
    }
}
