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

use ravel_sql::{ValidationError, validate};

/// sql4-F01 (S2): a grouped `MIN`/`MAX` that appears only in a query-level
/// `ORDER BY` escapes `reject_grouped_min_max`. The walk tracks "is the
/// innermost SELECT grouped?" on a stack that is pushed in `pre_visit_select`
/// and popped in `post_visit_select`; the query-level `ORDER BY` is visited
/// *after* the SELECT has been popped, so the stack is empty and the
/// `min`/`max` call is not rejected. DataFusion still binds the aggregate to
/// the grouped hash accumulator, which the crate's own docs
/// (crates/ravel-sql/src/validate.rs:38) say returns a wrong extreme for NaN,
/// signed zero, or all-infinite groups. Unlike `avg`, grouped min/max has no
/// session-level backstop, so this walk is the only guard.
///
/// Expected (post-fix): rejected with `GroupedMinMaxUnsupported`, exactly as
/// the `HAVING max(value)` case already is. Fails today: `validate` returns
/// `Ok(())`.
#[test]
#[ignore = "audit sql4-F01: documents an acceptance gap, fails against baseline"]
fn grouped_min_max_in_order_by_must_be_rejected() {
    assert_eq!(
        validate("SELECT series_id FROM samples GROUP BY series_id ORDER BY max(value)")
            .expect_err("grouped max() in ORDER BY must be rejected"),
        ValidationError::GroupedMinMaxUnsupported,
    );
    // Symmetric MIN case, and the same hole reached through arithmetic on the
    // aggregate rather than a bare call.
    assert_eq!(
        validate("SELECT series_id FROM samples GROUP BY series_id ORDER BY min(value) + 1")
            .expect_err("grouped min() in ORDER BY must be rejected"),
        ValidationError::GroupedMinMaxUnsupported,
    );
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
