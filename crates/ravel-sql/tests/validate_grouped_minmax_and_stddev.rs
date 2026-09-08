//! Validation of grouped `MIN`/`MAX` and the `stddev`/`var` family in the v1
//! SQL subset.
//!
//! These probe only `ravel_sql::validate`, which is a pure function of the
//! request text: no DataFusion session, catalog, or fixture is required, so
//! the tests are fully deterministic.

#![allow(clippy::expect_used)]

use ravel_sql::validate;

/// A grouped `MIN`/`MAX` reachable only through a query-level `ORDER BY` (a
/// field of `Query`, not `Select`) once escaped the `reject_grouped_min_max`
/// walk, reaching the grouped hash accumulator the rejection existed to avoid.
/// ADR-0023 removed the rejection entirely and made grouped MIN/MAX correct:
/// crate::session registers a total-order MIN/MAX UDAF that uses
/// `f64::total_cmp` for grouped and ungrouped execution alike, so the
/// accumulator is no longer wrong for NaN, signed zero, or all-infinite
/// groups. The guard moved from a fragile walk to a structural registry
/// replacement, the same shift the `avg` deregistration backstop already made.
///
/// This test asserts these shapes are now accepted. The correctness half --
/// that they execute to the right results, including pinned NaN, signed-zero,
/// and infinity bits -- lives in the differential gate, which needs a
/// DataFusion session this validate-only file deliberately avoids.
#[test]
fn grouped_min_max_in_order_by_is_now_accepted() {
    validate("SELECT series_id FROM samples GROUP BY series_id ORDER BY max(value)")
        .expect("grouped max() in ORDER BY is in the v1 subset (ADR-0023)");
    // Symmetric MIN case, and the same shape reached through arithmetic on the
    // aggregate rather than a bare call.
    validate("SELECT series_id FROM samples GROUP BY series_id ORDER BY min(value) + 1")
        .expect("grouped min() in ORDER BY is in the v1 subset (ADR-0023)");
}

/// The exactness-exclusion list rejects `avg`/`mean` (whose floating mean has
/// no bit-identical naive reference) but must also reject the
/// `stddev`/`var`/`stddev_pop`/`var_pop` family, which computes the same
/// floating mean internally and therefore shares the exact property that
/// justified excluding `avg`. Without the exclusion these functions are
/// registered by `with_default_features()` and NOT deregistered in
/// `crate::session::build_session` (only `avg`/`mean` are), so they would plan
/// and execute unverified by the exactness regime. `validate` now excludes the
/// family exactly as `avg` is, so each call returns `Err`.
#[test]
fn stddev_and_variance_family_must_be_handled_like_avg() {
    for sql in [
        "SELECT stddev(value) FROM samples",
        "SELECT var(value) FROM samples",
        "SELECT stddev_pop(value) FROM samples",
        "SELECT var_pop(value) FROM samples",
    ] {
        assert!(
            validate(sql).is_err(),
            "{sql}: shares avg's floating-mean intermediate and must be excluded \
             from the v1 subset (or the reference executor must cover it)"
        );
    }
}
