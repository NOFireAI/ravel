//! Does the stddev/variance aggregate family belong in the v1 SQL subset, or
//! must it be excluded like `avg`?
//!
//! `crate::validate` originally rejected only `avg`/`mean` and grouped
//! `min`/`max`, and `crate::session::build_session` deregistered only the
//! `avg`/`mean` UDAFs. `stddev`, `var`, `stddev_pop`, `var_pop` (and
//! `covar_*`/`corr`) were registered by DataFusion's `with_default_features()`
//! and left in place, so they planned and executed.
//!
//! Each of them computes a floating mean internally -- exactly the property
//! that disqualified `avg`. `avg` was excluded on exactness grounds because
//! DataFusion's avg accumulator has its own intermediate typing and no naive
//! reference is bit-identical to it. The differential gate
//! (tests/differential.rs) only admits an operator to the v1 subset when an
//! *independent* reference reproduces DataFusion's output f64-bit-for-bit;
//! anything that cannot be reproduced that way is rejected at validation
//! instead, because "exact semantics by default" leaves no room for a
//! silently-different-but-plausible answer.
//!
//! # Why a naive reference cannot match
//!
//! DataFusion's `VarianceAccumulator` computes variance with Welford's online
//! algorithm: it folds one value at a time, maintaining a running `mean` and a
//! running sum of squared deviations `m2` via
//! `delta = v - mean; mean += delta / count; m2 += delta * (v - mean)`. The
//! textbook naive reference is two-pass: `mean = (sum v) / n`, then
//! `m2 = sum (v - mean)^2`. Welford's incremental `mean` and the two-pass
//! batch `mean` are different f64 values on the same inputs (a running divide
//! per element versus one final divide), so the two `m2` accumulations differ
//! in their low bits. On cancellation-prone, large-magnitude data the
//! divergence is well above one ULP. Sample vs population only changes the
//! final divisor (`n-1` vs `n`); the same `m2` gap flows through, and `sqrt`
//! of two different variances is two different stddevs.
//!
//! This is the same shape of argument that excluded `avg`, made concrete for
//! the variance family: the operator is exact and well-defined, but its exact
//! output is not reproducible by an independent naive computation, so a
//! differential gate over it would either be a second copy of Welford (not
//! independent) or would have to weaken to a tolerance (the exact failure mode
//! exactness forbids).
//!
//! # Resolution
//!
//! The decision was REJECT, mirroring `avg`. `crate::validate` now rejects the
//! whole family with `ValidationError::StddevVarUnsupported`, and
//! `crate::session::build_session` deregisters the family's UDAFs as a
//! backstop. The family no longer plans or executes, so there is no live
//! DataFusion value left to compare against. The tests below assert the family
//! is rejected.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

// ---------------------------------------------------------------------------
// Dataset
// ---------------------------------------------------------------------------

/// Cancellation-prone, large-magnitude, finite values in one series with
/// unique timestamps. This is the regime where a running-mean fold and a
/// batch-mean fold visibly disagree (see the module docs); it is kept so the
/// rejection is exercised against real fixture data rather than an empty scan.
fn cancellation_dataset() -> Vec<SegSpec> {
    let base = 1.0e9f64;
    let values = [
        base + 1.0,
        base + 2.0,
        base + 4.0,
        base + 8.0,
        base + 16.0,
        base + 32.0,
        base + 64.0,
        base + 128.0,
        base + 0.5,
        base + 0.25,
        -base,
        1.0,
        1.0e-8,
        3.0,
        base + 1.0 / 3.0,
        -base + 2.0 / 7.0,
    ];
    let samples: Vec<(i64, f64)> = values
        .iter()
        .enumerate()
        .map(|(i, &v)| (i as i64 + 1, v))
        .collect();
    vec![SegSpec::new(
        1,
        1,
        1,
        vec![SeriesSpec::new("cancel", samples)],
    )]
}

// ---------------------------------------------------------------------------
// The new fact: the family is rejected, exactly as `avg` is
// ---------------------------------------------------------------------------

/// Every member of the stddev/variance/covariance/correlation family is now
/// rejected before execution: `crate::validate` refuses the query text and,
/// as a backstop, `crate::session::build_session` deregisters the UDAFs. It
/// runs in the normal suite.
#[tokio::test]
async fn stddev_var_family_is_rejected() {
    let tenant = tenant_id("probe");
    let specs = cancellation_dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let single_arg = ["stddev", "var", "stddev_pop", "var_pop"];
    let double_arg = ["covar_samp", "covar_pop", "corr"];

    for func in single_arg {
        let sql = format!("SELECT {func}(value) FROM samples");
        let result = fixture
            .executor
            .execute(tenant.hash(), &request(&sql))
            .await;
        assert!(
            result.is_err(),
            "{func} must be rejected (validate + UDAF deregistration), but it executed"
        );
    }
    for func in double_arg {
        let sql = format!("SELECT {func}(value, value) FROM samples");
        let result = fixture
            .executor
            .execute(tenant.hash(), &request(&sql))
            .await;
        assert!(
            result.is_err(),
            "{func} must be rejected (validate + UDAF deregistration), but it executed"
        );
    }
}

/// The rejection is purely syntactic (it inspects the query text before any
/// planning or scan), so it fires regardless of the data -- including a
/// dataset carrying a NaN. The non-finite corner is closed by rejection, not
/// by any NaN-specific handling.
#[tokio::test]
async fn stddev_var_family_is_rejected_even_over_nan_input() {
    const NAN_POS: u64 = 0x7ff8_0000_0000_0001;
    let specs = vec![SegSpec::new(
        1,
        1,
        1,
        vec![SeriesSpec::new(
            "nan",
            vec![
                (1, 1.0e12),
                (2, f64::from_bits(NAN_POS)),
                (3, -1.0e12),
                (4, 2.5),
            ],
        )],
    )];
    let tenant = tenant_id("probe");
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    for func in ["stddev", "var", "stddev_pop", "var_pop"] {
        let sql = format!("SELECT {func}(value) FROM samples");
        let result = fixture
            .executor
            .execute(tenant.hash(), &request(&sql))
            .await;
        assert!(
            result.is_err(),
            "{func} over NaN input must still be rejected, not executed"
        );
    }
}
