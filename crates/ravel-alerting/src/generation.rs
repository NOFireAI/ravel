//! Generation computation and the recursion guard for alerts-on-alerts
//! (deliverable 4, ADR-0040 decision 4 / ADR-0043 decision 5).
//!
//! Only rules whose query reads the `alerts` table consume other alert records
//! as input; those are the only rules where generation is non-trivial. An
//! ordinary metric or log rule consumes no alert records, so
//! `compute_generation(&[])` yields `1` for its first alerts-on-alerts hop and
//! callers treat metric/log rules as generation `0` (they never route through
//! here).

use crate::error::AlertError;

/// ADR-0040's global default cap on alerts-on-alerts recursion depth. A rule's
/// `max_alert_generation` override replaces this; `None` uses it.
pub const DEFAULT_MAX_ALERT_GENERATION: u32 = 8;

/// The generation a new alert record gets, given the generations of every
/// alert record its rule's query consumed as input: `max(inputs, default 0)`
/// plus one. With no alert inputs (an ordinary metric/log rule, or the first
/// hop of an alerts-on-alerts chain seeded from non-alert data) the max is 0
/// and the result is 1.
///
/// Saturating so a pathological `u32::MAX` input cannot wrap; a value that high
/// is rejected by [`guard_generation`] against any sane cap anyway.
pub fn compute_generation(input_alert_generations: &[u32]) -> u32 {
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
    fn no_inputs_is_generation_one() {
        // An ordinary metric/log rule consumes no alert records: max(∅) = 0,
        // so the first alerts-on-alerts hop is generation 1.
        assert_eq!(compute_generation(&[]), 1);
    }

    #[test]
    fn generation_is_max_of_inputs_plus_one() {
        assert_eq!(compute_generation(&[0]), 1);
        assert_eq!(compute_generation(&[0, 3, 1]), 4);
        assert_eq!(compute_generation(&[2, 2, 2]), 3);
    }

    #[test]
    fn generation_saturates_instead_of_wrapping() {
        assert_eq!(compute_generation(&[u32::MAX]), u32::MAX);
    }

    #[test]
    fn chain_at_the_limit_is_accepted_one_over_is_rejected() {
        // A chain whose consumed max generation is exactly at the cap produces a
        // record one below the cap and is accepted; the record that would land
        // one over the cap is rejected with a typed error, never a panic.
        let limit = DEFAULT_MAX_ALERT_GENERATION; // 8

        // Inputs at generation 7 -> produced generation 8 == limit: accepted.
        let at_limit = compute_generation(&[limit - 1]);
        assert_eq!(at_limit, limit);
        assert!(guard_generation(at_limit, None).is_ok());

        // Inputs at generation 8 -> produced generation 9 > limit: rejected.
        let over = compute_generation(&[limit]);
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
