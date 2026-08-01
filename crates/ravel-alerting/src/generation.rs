//! Generation computation and the recursion guard for alerts-on-alerts
//! (deliverable 4, ADR-0040 decision 4 / ADR-0043 decision 5).
//!
//! Generation is a property of the alert *entity* (one `alert_id`'s whole
//! lifecycle), not of any single tick's evaluation: it is computed once, when
//! an alert is newly created (no prior record for its `alert_id`), and every
//! later transition of that same alert (fires again, resolves, is
//! suppressed) carries the same value forward unchanged. Recomputing it fresh
//! from each tick's inputs would make a `resolved` record's generation depend
//! on how many alert rows *this tick's* query happened to return, so the same
//! alert's own history could read generation 5 then 1 then 5 again -- a
//! foldable field flip-flopping on nothing but query timing, which defeats
//! the point of a recursion-depth counter.
//!
//! For a brand new alert, two ADR texts describe two different cases:
//! ADR-0043 decision 5 says an ordinary metric/log rule "always computes
//! `generation = 0`" (it structurally never consumes alert records, a static
//! property of the rule's query, [`crate::rule::RuleQuery::targets_alerts_table`]).
//! ADR-0040 decision 4's formula, `max(consumed, default 0) + 1`, is for the
//! other case: a rule that DOES target the alerts table. The two are not in
//! tension once the discriminator is explicit: pass `false` and this returns
//! `0` unconditionally (ADR-0043); pass `true` and it applies ADR-0040's
//! formula, including when that rule's query currently matches zero alert
//! rows (still generation 1, because the rule is structurally one hop removed
//! from raw signal data regardless of what matched on any given tick).

use crate::error::AlertError;

/// ADR-0040's global default cap on alerts-on-alerts recursion depth. A rule's
/// `max_alert_generation` override replaces this; `None` uses it.
pub const DEFAULT_MAX_ALERT_GENERATION: u32 = 8;

/// The generation for an alert record. `prior_generation` is the most recent
/// existing record's generation for this `alert_id` (`None` for a brand new
/// alert); when `Some`, it is returned unchanged -- generation is fixed at
/// creation, never recomputed on a later transition. For a new alert,
/// `targets_alerts_table` selects the formula: `false` (an ordinary
/// metric/log rule) always yields `0`; `true` yields `max(input_alert_
/// generations, default 0) + 1`.
///
/// Saturating so a pathological `u32::MAX` input cannot wrap; a value that high
/// is rejected by [`guard_generation`] against any sane cap anyway.
pub fn compute_generation(
    prior_generation: Option<u32>,
    targets_alerts_table: bool,
    input_alert_generations: &[u32],
) -> u32 {
    if let Some(generation) = prior_generation {
        return generation;
    }
    if !targets_alerts_table {
        return 0;
    }
    input_alert_generations
        .iter()
        .copied()
        .max()
        .unwrap_or(0)
        .saturating_add(1)
}

/// Rejects a record whose computed `generation` exceeds the effective cap
/// (`max` when `Some`, else [`DEFAULT_MAX_ALERT_GENERATION`]). The hard circuit
/// breaker on self-triggering chains: a typed error, never a panic or a silent
/// drop (ADR-0040 decision 4).
pub fn guard_generation(generation: u32, max: Option<u32>) -> Result<(), AlertError> {
    let limit = max.unwrap_or(DEFAULT_MAX_ALERT_GENERATION);
    if generation > limit {
        Err(AlertError::GenerationExceeded { generation, limit })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_rule_is_always_generation_zero() {
        // targets_alerts_table = false: always 0, regardless of inputs (which
        // should be empty for such a rule anyway, but 0 holds even if not).
        assert_eq!(compute_generation(None, false, &[]), 0);
        assert_eq!(compute_generation(None, false, &[3, 4]), 0);
    }

    #[test]
    fn alerts_table_rule_with_no_matching_rows_is_generation_one() {
        // targets_alerts_table = true, but this tick's query matched zero
        // alert rows: still generation 1, the rule's structural hop depth,
        // not 0 (which would misrepresent it as an ordinary rule).
        assert_eq!(compute_generation(None, true, &[]), 1);
    }

    #[test]
    fn new_alert_generation_is_max_of_inputs_plus_one() {
        assert_eq!(compute_generation(None, true, &[0]), 1);
        assert_eq!(compute_generation(None, true, &[0, 3, 1]), 4);
        assert_eq!(compute_generation(None, true, &[2, 2, 2]), 3);
    }

    #[test]
    fn existing_alert_keeps_its_generation_regardless_of_this_ticks_inputs() {
        // The bug this fixes: a firing record at generation 5, then the same
        // alert_id resolves on a tick whose query returned zero alert rows.
        // Without stickiness this would recompute to 1; with it, the
        // resolved record keeps generation 5, matching its own history.
        assert_eq!(compute_generation(Some(5), true, &[]), 5);
        assert_eq!(compute_generation(Some(5), true, &[9, 9, 9]), 5);
        assert_eq!(compute_generation(Some(0), false, &[]), 0);
    }

    #[test]
    fn generation_saturates_instead_of_wrapping() {
        assert_eq!(compute_generation(None, true, &[u32::MAX]), u32::MAX);
    }

    #[test]
    fn chain_at_the_limit_is_accepted_one_over_is_rejected() {
        // A chain whose consumed max generation is exactly at the cap produces a
        // record one below the cap and is accepted; the record that would land
        // one over the cap is rejected with a typed error, never a panic.
        let limit = DEFAULT_MAX_ALERT_GENERATION; // 8

        // Inputs at generation 7 -> produced generation 8 == limit: accepted.
        let at_limit = compute_generation(None, true, &[limit - 1]);
        assert_eq!(at_limit, limit);
        assert!(guard_generation(at_limit, None).is_ok());

        // Inputs at generation 8 -> produced generation 9 > limit: rejected.
        let over = compute_generation(None, true, &[limit]);
        assert_eq!(over, limit + 1);
        match guard_generation(over, None) {
            Err(AlertError::GenerationExceeded {
                generation,
                limit: lim,
            }) => {
                assert_eq!(generation, limit + 1);
                assert_eq!(lim, limit);
            }
            other => panic!("expected GenerationExceeded, got {other:?}"),
        }
    }

    #[test]
    fn per_rule_override_replaces_the_default() {
        // A tighter override rejects what the default would accept.
        assert!(guard_generation(2, Some(1)).is_err());
        assert!(guard_generation(1, Some(1)).is_ok());
        // A looser override accepts what the default would reject.
        assert!(guard_generation(DEFAULT_MAX_ALERT_GENERATION + 1, Some(100)).is_ok());
    }
}
