//! Audit SQL-4 reproducers (issue #145, baseline origin/main a7c95ef).
//!
//! Each test documents one confirmed finding from
//! docs/reviews/2026-07-28-ravel-sql-audit/sql4-validate-error-tenant.md and
//! FAILS when un-ignored against the audited code. They are `#[ignore]`d so
//! the crate's normal `cargo test -p ravel-sql` stays green; run with
//! `cargo test -p ravel-sql --test audit_sql4_validate -- --ignored`.
//!
//! These probe only `ravel_sql::validate`, which is a pure function of the
//! request text: no DataFusion session, catalog, or fixture is required, so
//! the reproducers are fully deterministic.

#![allow(clippy::expect_used)]

use ravel_sql::validate;

/// sql4-F01 (S2), resolved by ADR-0023: a grouped `MIN`/`MAX` reachable only
/// through a query-level `ORDER BY` (a field of `Query`, not `Select`) once
/// escaped the `reject_grouped_min_max` walk (issue #159), reaching the
/// grouped hash accumulator the rejection existed to avoid. Rather than keep
/// patching a syntactic walk, ADR-0023 removed the rejection entirely and made
/// grouped MIN/MAX correct: crate::session registers a total-order MIN/MAX
/// UDAF that uses `f64::total_cmp` for grouped and ungrouped execution alike,
/// so the accumulator is no longer wrong for NaN, signed zero, or all-infinite
/// groups. The guard moved from a fragile walk to a structural registry
/// replacement, the same shift the `avg` deregistration backstop already made.
///
/// This reproducer therefore converts from "must be rejected" to "must be
/// accepted". The correctness half -- that these exact shapes now execute to
/// the right results -- lives in the differential gate, which needs a
/// DataFusion session this validate-only file deliberately avoids:
/// `differential.rs::grouped_min_max_issue_159_shapes_execute_correctly`
/// (the `ORDER BY max(value)` and `HAVING min(value)` shapes) and
/// `golden_grouped_min_max_use_total_order` (the pinned NaN/signed-zero/
/// infinity bits).
#[test]
fn grouped_min_max_in_order_by_is_now_accepted() {
    validate("SELECT series_id FROM samples GROUP BY series_id ORDER BY max(value)")
        .expect("grouped max() in ORDER BY is in the v1 subset (ADR-0023)");
    // Symmetric MIN case, and the same shape reached through arithmetic on the
    // aggregate rather than a bare call.
    validate("SELECT series_id FROM samples GROUP BY series_id ORDER BY min(value) + 1")
        .expect("grouped min() in ORDER BY is in the v1 subset (ADR-0023)");
}

/// sql4-F02 (S2): the exactness-exclusion list rejects `avg`/`mean` (whose
/// floating mean has no bit-identical naive reference, review F7) but not the
/// `stddev`/`var`/`stddev_pop`/`var_pop` family, which computes the same
/// floating mean internally and therefore shares the exact property that
/// justified excluding `avg`. These functions are registered by
/// `with_default_features()` and are NOT deregistered in
/// `crate::session::build_session` (only `avg`/`mean` are), so they plan and
/// execute unverified by the exactness regime.
///
/// Expected (post-fix): rejected the same way `avg` is (or explicitly covered
/// by the differential/reference executor). Fails today: `validate` returns
/// `Ok(())`.
#[test]
#[ignore = "audit sql4-F02: documents an exactness-exclusion gap, fails against baseline"]
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
