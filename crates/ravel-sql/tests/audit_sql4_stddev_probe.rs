//! Audit SQL-4, finding sql4-F02: does the stddev/variance aggregate family
//! belong in the v1 SQL subset, or must it be excluded like `avg`?
//!
//! docs/reviews/2026-07-28-ravel-sql-audit/sql4-validate-error-tenant.md
//! confirmed the *validation* half of sql4-F02: `crate::validate` rejects only
//! `avg`/`mean` and grouped `min`/`max`, and `crate::session::build_session`
//! deregisters only the `avg`/`mean` UDAFs. `stddev`, `var`, `stddev_pop`,
//! `var_pop` (and `covar_*`/`corr`) are registered by DataFusion's
//! `with_default_features()` and left in place, so they plan and execute.
//!
//! Each of them computes a floating mean internally -- exactly the property
//! that disqualified `avg`. `avg` was excluded (docs/arrow-datafusion-plan.md
//! section 2 "Exactness", review F7) because "DataFusion's avg accumulator has
//! its own intermediate typing and no naive reference is bit-identical to it".
//! The differential gate (tests/differential.rs) only admits an operator to
//! the v1 subset when an *independent* reference reproduces DataFusion's output
//! f64-bit-for-bit; anything that cannot be reproduced that way is rejected at
//! validation instead, because "exact semantics by default" leaves no room for
//! a silently-different-but-plausible answer.
//!
//! This probe answers the deciding question the audit left open (its
//! "mis-execution half"): **can a naive reference of the kind the differential
//! suite's reference executor uses be bit-identical to DataFusion's
//! stddev/variance accumulators?** If not, the family shares `avg`'s exact
//! disqualifying property and the correct fix is to REJECT it (add it to the
//! `reject_*` walk and the session UDAF deregistration, mirroring `avg`), not
//! to gate it.
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
//! review F7 forbids).
//!
//! # Reading the result
//!
//! [`stddev_var_family_is_not_bit_identical_to_a_naive_reference`] is
//! `#[ignore]`d: it asserts the bit-identity that would be *required* to bring
//! the family under the differential gate, and it fails, so it documents the
//! gap rather than gating on it (the same convention the sql4 reproducers use).
//! Run it explicitly to see the differing bit patterns:
//!
//! ```text
//! cargo test -p ravel-sql --test audit_sql4_stddev_probe -- --ignored --nocapture
//! ```
//!
//! [`stddev_var_family_is_registered_and_executes`] is *not* ignored: it runs
//! in the normal suite and pins the live fact behind the finding -- that all
//! four functions plan and execute today, unguarded by validation.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use datafusion::arrow::array::{Array, Float64Array};
use ravel_sql::QueryOutput;
use ravel_types::TenantId;
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

// ---------------------------------------------------------------------------
// The naive two-pass reference (what an independent reference executor would
// implement -- the differential suite's reference is naive by design)
// ---------------------------------------------------------------------------

/// The four members of the family, computed two-pass in one place.
struct NaiveStats {
    var_samp: f64,
    var_pop: f64,
    stddev_samp: f64,
    stddev_pop: f64,
}

/// Textbook two-pass variance over `values` in the given order: one pass for
/// the mean, one pass for the sum of squared deviations. This is the naive
/// reference the exactness policy weighs `avg` (and now this family) against;
/// it is deliberately *not* Welford's online algorithm, because a reference
/// that re-implements Welford would not be independent of the accumulator
/// under test.
fn naive_stats(values: &[f64]) -> NaiveStats {
    let n = values.len() as f64;
    let mut sum = 0.0f64;
    for &v in values {
        sum += v;
    }
    let mean = sum / n;
    let mut m2 = 0.0f64;
    for &v in values {
        let d = v - mean;
        m2 += d * d;
    }
    let var_pop = m2 / n;
    let var_samp = m2 / (n - 1.0);
    NaiveStats {
        var_samp,
        var_pop,
        stddev_samp: var_samp.sqrt(),
        stddev_pop: var_pop.sqrt(),
    }
}

// ---------------------------------------------------------------------------
// Running one scalar aggregate through the real SQL path
// ---------------------------------------------------------------------------

/// Execute a single-row, single-column `Float64` query and return the cell.
async fn scalar_f64(fixture: &Fixture, tenant: &TenantId, sql: &str) -> f64 {
    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request(sql))
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    cell_f64(&outcome.output, sql)
}

fn cell_f64(output: &QueryOutput, sql: &str) -> f64 {
    assert_eq!(output.num_rows(), 1, "expected exactly one row for: {sql}");
    let batch = output
        .batches()
        .iter()
        .find(|b| b.num_rows() == 1)
        .unwrap_or_else(|| panic!("no single-row batch for: {sql}"));
    let col = batch
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("column 0 is not Float64 for: {sql}"));
    col.value(0)
}

/// Pull every deduplicated value the ungrouped aggregate sees, in the
/// canonical `(series_id, ts)` order the scan emits them -- so the naive
/// reference folds the identical values in the identical order the
/// accumulator does. Any bit divergence is then attributable to the algorithm
/// alone, not to input order.
async fn aggregated_values(fixture: &Fixture, tenant: &TenantId) -> Vec<f64> {
    let snapshot = fixture.snapshot(tenant).await;
    util::reference_rows(&fixture.fetcher, tenant.hash(), &snapshot)
        .await
        .into_iter()
        .map(|r| r.value)
        .collect()
}

// ---------------------------------------------------------------------------
// Datasets
// ---------------------------------------------------------------------------

/// Cancellation-prone, large-magnitude, finite values in one series with
/// unique timestamps (so dedup is the identity and the reference values equal
/// the raw samples). Mixing `+1e9` and `-1e9` drives the mean near a small
/// value while the squared deviations are near `1e18`, the regime where a
/// running-mean fold and a batch-mean fold visibly disagree.
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

/// A dataset that additionally carries a NaN, to document how the family
/// behaves on non-finite input.
fn nan_dataset() -> Vec<SegSpec> {
    const NAN_POS: u64 = 0x7ff8_0000_0000_0001;
    vec![SegSpec::new(
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
    )]
}

// ---------------------------------------------------------------------------
// (b) The live fact: the family is unguarded and executes today
// ---------------------------------------------------------------------------

/// All four aggregates plan and execute against the current session -- neither
/// validation (`crate::validate`) nor the session UDAF deregistration
/// (`crate::session::build_session`) blocks them, unlike `avg`. This is the
/// standing state finding sql4-F02 describes; it runs in the normal suite.
#[tokio::test]
async fn stddev_var_family_is_registered_and_executes() {
    let tenant = tenant_id("probe");
    let specs = cancellation_dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    for func in ["stddev", "var", "stddev_pop", "var_pop"] {
        let sql = format!("SELECT {func}(value) FROM samples");
        let outcome = fixture
            .executor
            .execute(tenant.hash(), &request(&sql))
            .await
            .unwrap_or_else(|e| {
                panic!("{func} should execute (it is registered and unvalidated): {e}")
            });
        assert_eq!(
            outcome.output.num_rows(),
            1,
            "{func} must return one aggregate row"
        );
    }
}

/// NaN input propagates to a NaN result on both the DataFusion path and the
/// naive reference: neither errors, both return NaN. This documents the
/// non-finite corner (part of the sql4-F02 dataset requirement) and shows the
/// family is not merely broken on NaN -- it is silently *un-gated* on ordinary
/// finite data, which is the real hazard the ignored probe below pins down.
#[tokio::test]
async fn nan_input_yields_nan_on_both_paths() {
    let tenant = tenant_id("probe");
    let specs = nan_dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let values = aggregated_values(&fixture, &tenant).await;
    let reference = naive_stats(&values);
    assert!(
        reference.var_samp.is_nan() && reference.stddev_pop.is_nan(),
        "naive reference over NaN input must be NaN"
    );

    for func in ["stddev", "var", "stddev_pop", "var_pop"] {
        let got = scalar_f64(
            &fixture,
            &tenant,
            &format!("SELECT {func}(value) FROM samples"),
        )
        .await;
        assert!(got.is_nan(), "{func} over NaN input must be NaN, got {got}");
    }
}

// ---------------------------------------------------------------------------
// (a) The deciding probe: DataFusion vs a naive reference, bit-for-bit
// ---------------------------------------------------------------------------

/// The bit-identity a differential gate would require, asserted against a naive
/// two-pass reference over cancellation-prone, large-magnitude finite data.
///
/// It FAILS: DataFusion's Welford accumulator and the naive two-pass reference
/// produce different f64 bits for every member of the family. That is the
/// answer to finding sql4-F02's open question -- the family is *not*
/// reproducible by an independent naive reference, so it shares `avg`'s
/// disqualifying property and must be REJECTED, not gated.
///
/// `#[ignore]`d so the standing suite stays green while this documents the gap.
/// Run with `--ignored --nocapture` to see the differing bit patterns.
#[tokio::test]
#[ignore = "documents sql4-F02: stddev/var are not bit-identical to a naive reference; \
            the recommended fix is REJECT (see module docs), not to weaken this assertion"]
async fn stddev_var_family_is_not_bit_identical_to_a_naive_reference() {
    let tenant = tenant_id("probe");
    let specs = cancellation_dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let values = aggregated_values(&fixture, &tenant).await;
    let reference = naive_stats(&values);

    let cases = [
        ("var", reference.var_samp),
        ("var_pop", reference.var_pop),
        ("stddev", reference.stddev_samp),
        ("stddev_pop", reference.stddev_pop),
    ];

    for (func, want) in cases {
        let got = scalar_f64(
            &fixture,
            &tenant,
            &format!("SELECT {func}(value) FROM samples"),
        )
        .await;
        assert_eq!(
            got.to_bits(),
            want.to_bits(),
            "{func}: DataFusion {got:?} (bits {:#018x}) vs naive reference {want:?} \
             (bits {:#018x}) -- a naive reference is not bit-identical, so {func} \
             cannot be brought under the differential gate and must be rejected",
            got.to_bits(),
            want.to_bits(),
        );
    }
}
