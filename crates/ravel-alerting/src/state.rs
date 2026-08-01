//! Alert states and the pure state-transition function (deliverable 3).
//!
//! State is derived by fold, never mutated (ADR-0040 decision 3): the caller
//! folds the records sharing an `alert_id` to the greatest-`ts` one, hands it
//! here as `prior`, and this function decides the next state and whether a new
//! record is warranted. A new record is written *only on an actual transition*
//! (ADR-0043 decision 4), never on a tick that merely re-confirms the current
//! state.
//!
//! The function is pure: it takes the current clock reading `now_ns` as a
//! parameter and never reads a clock, so pending-duration elapse is computed
//! from `prior.ts_ns` and every case is unit-testable with no I/O.

use crate::record::AlertRecord;
use crate::rule::Rule;

/// The lifecycle state of an alert.
///
/// `Pending` is written as a record (not just an in-memory phase) because the
/// evaluator is disposable: ADR-0043 decision 3 derives "how long has this been
/// pending" from the pending record's own `ts`, so a restarted evaluator
/// resumes correctly. This extends ADR-0040 decision 2's listed state
/// vocabulary (`firing`/`resolved`/`suppressed`) with `pending`; see the crate
/// doc comment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlertState {
    /// Condition holds but `for_duration` has not yet elapsed.
    Pending,
    /// Condition holds and has held long enough; the alert is active.
    Firing,
    /// Condition has cleared (or a pending alert was cancelled before firing).
    Resolved,
    /// Suppressed by an external silence. This pure function never enters or
    /// leaves this state on its own; it preserves it until an external action
    /// clears it.
    Suppressed,
}

impl AlertState {
    /// The `state` attr string written into the record.
    pub fn as_str(self) -> &'static str {
        match self {
            AlertState::Pending => "pending",
            AlertState::Firing => "firing",
            AlertState::Resolved => "resolved",
            AlertState::Suppressed => "suppressed",
        }
    }

    /// Parses a stored `state` attr string back into an [`AlertState`].
    pub fn parse(s: &str) -> Option<AlertState> {
        Some(match s {
            "pending" => AlertState::Pending,
            "firing" => AlertState::Firing,
            "resolved" => AlertState::Resolved,
            "suppressed" => AlertState::Suppressed,
            _ => return None,
        })
    }

    /// Maps the state onto `severity_num`, reused per ADR-0040 decision 2
    /// (higher = more urgent). The authoritative state lives in the `state`
    /// attr; this is a mirror for consumers that sort by severity.
    pub fn severity_num(self) -> u8 {
        match self {
            AlertState::Resolved => 0,
            AlertState::Suppressed => 1,
            AlertState::Pending => 2,
            AlertState::Firing => 3,
        }
    }
}

/// The outcome of evaluating one rule against one tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StateTransition {
    /// The state the alert is in after this evaluation.
    pub next_state: AlertState,
    /// `true` only when `next_state` is a genuine transition away from the
    /// prior record's state, meaning a new record must be written. `false` when
    /// this tick merely re-confirms the existing state (no record, per ADR-0043
    /// decision 4).
    pub write_record: bool,
}

impl StateTransition {
    fn write(next_state: AlertState) -> Self {
        StateTransition {
            next_state,
            write_record: true,
        }
    }

    fn keep(next_state: AlertState) -> Self {
        StateTransition {
            next_state,
            write_record: false,
        }
    }
}

/// Decides the next state and whether to write a record, given the rule, the
/// most recent prior record for this alert's `alert_id` (`None` if the alert
/// has never been seen), whether the condition is currently met, and the
/// current clock reading.
///
/// Standard Prometheus-style semantics:
/// - condition met, no prior/resolved, no `for`: fire immediately (write).
/// - condition met, no prior/resolved, with `for`: enter pending (write).
/// - condition met, pending, `for` elapsed since the pending record's `ts`:
///   fire (write); not yet elapsed: stay pending (no write).
/// - condition met, already firing: stay firing (no write).
/// - condition clears while pending or firing: resolve (write). A pending
///   alert that clears before firing still writes a `resolved` record so the
///   fold reflects that it is no longer pending; a sink may choose not to
///   notify on a resolve with no preceding firing.
/// - condition clears while already resolved or never seen: nothing to write.
/// - suppressed: preserved unchanged, never written by this function.
pub fn evaluate_transition(
    rule: &Rule,
    prior: Option<&AlertRecord>,
    condition_met: bool,
    now_ns: i64,
) -> StateTransition {
    let prior_state = prior.map(|r| r.state);

    // A silence overrides the condition-driven machine. Clearing it is an
    // external action, so this pure function neither enters nor leaves the
    // suppressed state on its own.
    if prior_state == Some(AlertState::Suppressed) {
        return StateTransition::keep(AlertState::Suppressed);
    }

    if !condition_met {
        return match prior_state {
            Some(AlertState::Firing | AlertState::Pending) => {
                StateTransition::write(AlertState::Resolved)
            }
            // Already resolved, or an alert that never existed: nothing active.
            _ => StateTransition::keep(AlertState::Resolved),
        };
    }

    // Condition is met. `for_duration` of None or zero fires on this tick.
    let fires_immediately = rule.for_duration.is_none_or(|d| d.is_zero());

    match prior_state {
        Some(AlertState::Firing) => StateTransition::keep(AlertState::Firing),
        Some(AlertState::Pending) => {
            if fires_immediately || pending_elapsed(rule, prior, now_ns) {
                StateTransition::write(AlertState::Firing)
            } else {
                StateTransition::keep(AlertState::Pending)
            }
        }
        // No prior record, or a prior resolved record: this is a fresh onset.
        _ => {
            if fires_immediately {
                StateTransition::write(AlertState::Firing)
            } else {
                StateTransition::write(AlertState::Pending)
            }
        }
    }
}

/// True when `for_duration` has elapsed since the pending record's timestamp.
/// Clock skew (a `now_ns` earlier than the pending record) counts as zero
/// elapsed, never negative.
fn pending_elapsed(rule: &Rule, prior: Option<&AlertRecord>, now_ns: i64) -> bool {
    let Some(d) = rule.for_duration else {
        return true;
    };
    let pending_ts = prior.map_or(now_ns, |r| r.ts_ns);
    let elapsed_ns = now_ns.saturating_sub(pending_ts).max(0) as u128;
    elapsed_ns >= d.as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{AlertId, build_transition_record};
    use crate::rule::{RuleCondition, RuleQuery, ThresholdOp};
    use std::time::Duration;

    const SEC: i64 = 1_000_000_000;

    fn rule(for_duration: Option<Duration>) -> Rule {
        Rule {
            rule_id: "high-cpu".into(),
            query: RuleQuery::Promql("cpu > 0.9".into()),
            condition: RuleCondition::Threshold {
                op: ThresholdOp::Gt,
                threshold: 0.9,
            },
            labels: vec![("severity".into(), "page".into())],
            annotations: vec![],
            for_duration,
            max_alert_generation: None,
        }
    }

    /// A prior record in `state` stamped at `ts_ns`.
    fn prior(state: AlertState, ts_ns: i64) -> AlertRecord {
        AlertRecord {
            alert_id: AlertId([0u8; 16]),
            rule_id: "high-cpu".into(),
            state,
            generation: 0,
            ts_ns,
            labels: vec![],
            annotations: vec![],
            body: String::new(),
        }
    }

    #[test]
    fn no_for_duration_fires_immediately_from_nothing() {
        let t = evaluate_transition(&rule(None), None, true, 100 * SEC);
        assert_eq!(
            t,
            StateTransition {
                next_state: AlertState::Firing,
                write_record: true,
            }
        );
    }

    #[test]
    fn met_condition_opens_pending_when_for_is_set() {
        let t = evaluate_transition(&rule(Some(Duration::from_secs(30))), None, true, 100 * SEC);
        assert_eq!(
            t,
            StateTransition {
                next_state: AlertState::Pending,
                write_record: true,
            }
        );
    }

    #[test]
    fn pending_does_not_fire_before_for_elapses() {
        let r = rule(Some(Duration::from_secs(30)));
        let p = prior(AlertState::Pending, 100 * SEC);
        // 29s later: still pending, no new record.
        let t = evaluate_transition(&r, Some(&p), true, 129 * SEC);
        assert_eq!(
            t,
            StateTransition {
                next_state: AlertState::Pending,
                write_record: false,
            }
        );
    }

    #[test]
    fn pending_fires_once_for_has_elapsed() {
        let r = rule(Some(Duration::from_secs(30)));
        let p = prior(AlertState::Pending, 100 * SEC);
        // Exactly 30s later: fire, write a record.
        let t = evaluate_transition(&r, Some(&p), true, 130 * SEC);
        assert_eq!(
            t,
            StateTransition {
                next_state: AlertState::Firing,
                write_record: true,
            }
        );
    }

    #[test]
    fn firing_stays_firing_without_a_new_record() {
        let r = rule(Some(Duration::from_secs(30)));
        let p = prior(AlertState::Firing, 100 * SEC);
        let t = evaluate_transition(&r, Some(&p), true, 1_000 * SEC);
        assert_eq!(
            t,
            StateTransition {
                next_state: AlertState::Firing,
                write_record: false,
            }
        );
    }

    #[test]
    fn firing_resolves_when_condition_clears() {
        let p = prior(AlertState::Firing, 100 * SEC);
        let t = evaluate_transition(&rule(None), Some(&p), false, 200 * SEC);
        assert_eq!(
            t,
            StateTransition {
                next_state: AlertState::Resolved,
                write_record: true,
            }
        );
    }

    #[test]
    fn pending_that_clears_before_firing_resolves() {
        // A pending alert whose condition clears writes a resolved record so the
        // fold no longer shows it pending.
        let r = rule(Some(Duration::from_secs(30)));
        let p = prior(AlertState::Pending, 100 * SEC);
        let t = evaluate_transition(&r, Some(&p), false, 110 * SEC);
        assert_eq!(
            t,
            StateTransition {
                next_state: AlertState::Resolved,
                write_record: true,
            }
        );
    }

    #[test]
    fn resolved_stays_resolved_without_a_record() {
        let p = prior(AlertState::Resolved, 100 * SEC);
        let t = evaluate_transition(&rule(None), Some(&p), false, 200 * SEC);
        assert_eq!(
            t,
            StateTransition {
                next_state: AlertState::Resolved,
                write_record: false,
            }
        );
    }

    #[test]
    fn never_seen_and_not_met_writes_nothing() {
        let t = evaluate_transition(&rule(None), None, false, 100 * SEC);
        assert_eq!(
            t,
            StateTransition {
                next_state: AlertState::Resolved,
                write_record: false,
            }
        );
    }

    #[test]
    fn resolved_then_met_re_arms_pending() {
        let r = rule(Some(Duration::from_secs(30)));
        let p = prior(AlertState::Resolved, 100 * SEC);
        let t = evaluate_transition(&r, Some(&p), true, 200 * SEC);
        assert_eq!(
            t,
            StateTransition {
                next_state: AlertState::Pending,
                write_record: true,
            }
        );
    }

    #[test]
    fn suppressed_is_preserved_and_never_written_by_this_function() {
        let p = prior(AlertState::Suppressed, 100 * SEC);
        // Condition met.
        let met = evaluate_transition(&rule(None), Some(&p), true, 200 * SEC);
        assert_eq!(
            met,
            StateTransition {
                next_state: AlertState::Suppressed,
                write_record: false,
            }
        );
        // Condition cleared.
        let cleared = evaluate_transition(&rule(None), Some(&p), false, 200 * SEC);
        assert_eq!(
            cleared,
            StateTransition {
                next_state: AlertState::Suppressed,
                write_record: false,
            }
        );
    }

    #[test]
    fn clock_skew_earlier_than_pending_does_not_fire() {
        // now_ns before the pending record: zero elapsed, never negative.
        let r = rule(Some(Duration::from_secs(30)));
        let p = prior(AlertState::Pending, 100 * SEC);
        let t = evaluate_transition(&r, Some(&p), true, 90 * SEC);
        assert_eq!(t.next_state, AlertState::Pending);
        assert!(!t.write_record);
    }

    #[test]
    fn build_transition_record_stamps_state_generation_and_time() {
        let r = rule(None);
        let rec = build_transition_record(&r, AlertState::Firing, 2, 500 * SEC);
        assert_eq!(rec.state, AlertState::Firing);
        assert_eq!(rec.generation, 2);
        assert_eq!(rec.ts_ns, 500 * SEC);
        assert_eq!(rec.rule_id, "high-cpu");
        assert_eq!(rec.labels, r.labels);
    }
}
