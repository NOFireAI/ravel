//! B3 layer-2 differential gate (issue #22):
//! docs/arrow-datafusion-plan.md section 2 "Exactness", review F5 and F7.
//!
//! Layer 1 (B1, tests/pipeline.rs) gates the *scan* against an independent
//! greatest-wins oracle. This file gates the *operator layer*: a reference
//! executor for the v1 SQL subset (project, filter, count/sum, group by,
//! order by, limit, ungrouped min/max) evaluates over rows it pulls through
//! `util::reference_rows` -- an independent merge over `SegmentFetcher::fetch`
//! -- and the result is compared f64-bit-identical against the same query run
//! through the real SQL path.
//!
//! The subset grammar itself -- the query shapes, the predicate language, the
//! dataset strategies, the comparable [`util::gate::Cell`] form, and the
//! reference aggregate folds -- lives in [`util::gate`], shared with the
//! Flight-vs-HTTP transport parity gate (tests/flight_differential.rs, issue
//! #153) so the two gates cannot disagree about what the subset is. This file
//! owns the oracle comparison against `reference_rows`; the parity gate owns
//! the transport comparison.
//!
//! # Why the reference is not "naive"
//!
//! Review F7's whole point is that "DataFusion == naive scalar arithmetic" is
//! false. Everything in [`util::gate`] was verified against the pinned
//! datafusion 54.1.0 and arrow 58.4.0 in the workspace lockfile, reading the
//! accumulators rather than guessing:
//!
//! - **Comparisons are total-order, not IEEE.** arrow's compare kernels
//!   (`arrow_ord::cmp`) use `f64::total_cmp`, so in SQL `value > 5.0` is
//!   *true* for NaN and `-0.0 < 0.0` is *true*. A reference using Rust's `>`
//!   would disagree on every NaN row. `Pred::eval` uses `total_cmp`.
//!
//! - **min/max use one total-order implementation** for floating point,
//!   grouped and ungrouped alike (ADR-0023). DataFusion's built-ins disagree
//!   between the two paths: the ungrouped `MinAccumulator`/`MaxAccumulator`
//!   combine with `f64::total_cmp` (negative NaN is the minimum, positive NaN
//!   the maximum, -0.0 < 0.0), but the grouped `PrimitiveGroupsAccumulator`
//!   folds `partial_cmp` from an `f64::MAX`/`f64::MIN` seed, which is
//!   order-dependent, lets a NaN poison later comparisons, treats `-0.0` and
//!   `0.0` as `Equal`, and returns the unseeded `f64::MAX`/`f64::MIN` for an
//!   all-infinite group. crate::session replaces both built-ins with a
//!   total-order UDAF (crate::minmax) that uses `f64::total_cmp` everywhere,
//!   so grouped and ungrouped MIN/MAX over the same multiset agree bit for
//!   bit. Defining the semantics as a total order is what makes an
//!   *independent* scalar reference possible: `min_total_order` and
//!   `max_total_order` implement that order once, and the gate applies them
//!   both ungrouped (over the whole selection) and per group. This is the
//!   grouped differential gate the interim design (issue #143) called
//!   impossible while the semantics were only the accumulator's fold order.
//!   Non-float MIN/MAX (for example `min(ts)` over a Timestamp column)
//!   delegate to the built-in, whose `partial_cmp` is already total there;
//!   [`golden_grouped_min_max_over_ts_delegates`] pins that path against an
//!   integer reference fold.
//!
//! - **`avg` is out of the v1 subset** and rejected at validation
//!   (crate::validate), so it never reaches this gate.
//!
//! # The one restriction, and why
//!
//! The plan says of `sum`: "both sides add plain f64 in the same
//! deterministic input order". Against the pinned versions that premise
//! holds for GROUP BY sum (`PrimitiveGroupsAccumulator` folds `+=`
//! sequentially in row order) but **not** for ungrouped sum: it calls
//! `arrow::compute::sum`, which runs `PREFERRED_VECTOR_SIZE_NON_NULL /
//! size_of::<f64>()` independent lane accumulators and tree-reduces them.
//! The lane count is architecture-dependent (4 on aarch64, 8 with AVX), so
//! no portable sequential reference can be bit-identical to it for values
//! where float addition is not associative.
//!
//! Rather than weaken the gate to a tolerance -- the exact failure mode F7
//! warns about -- ungrouped `sum` is proptested only over values whose
//! partial sums are exactly representable (integer-valued f64 with a bounded
//! magnitude), where every association order provably yields identical bits.
//! Grouped `sum` keeps the full value pool. NaN and infinity in ungrouped
//! `sum` are covered by golden cases that assert the properties that *are*
//! well defined (a NaN result is NaN; an infinite result has the right sign)
//! rather than a payload the lane order chooses. This is F7's amendment (a)
//! applied to the one operator where amendment (b) is not portably possible,
//! and it is flagged as a deviation in the B3 report.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::cmp::Ordering;
use std::collections::BTreeMap;

use proptest::prelude::*;
use ravel_types::TenantId;
use util::gate::{
    Cell, Pred, Query, Row, Shape, ValuePool, actual_rows, arb_dataset, arb_pred,
    arb_single_group_dataset, block_on, exact_sum_value, group_by_series, interesting_value,
    max_total_order, min_total_order,
};
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

// ---------------------------------------------------------------------------
// The gate itself
// ---------------------------------------------------------------------------

/// Run `query` both ways and assert the results are bit-identical.
async fn assert_bit_identical(fixture: &Fixture, tenant: &TenantId, query: &Query) {
    let snapshot = fixture.snapshot(tenant).await;
    let reference = util::reference_rows(&fixture.fetcher, tenant.hash(), &snapshot).await;

    let sql = query.sql();
    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request(&sql))
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));

    let want = query.eval(&reference);
    let got = actual_rows(&outcome.output);

    assert_eq!(
        got.len(),
        want.len(),
        "row count differs for: {sql}\ngot  {got:?}\nwant {want:?}"
    );
    for (i, (g, w)) in got.iter().zip(want.iter()).enumerate() {
        assert_eq!(
            g, w,
            "row {i} differs for: {sql}\ngot  {got:?}\nwant {want:?}"
        );
    }
}

async fn assert_all(fixture: &Fixture, tenant: &TenantId, queries: &[Query]) {
    for query in queries {
        assert_bit_identical(fixture, tenant, query).await;
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// Projection, filter, count, and min/max over the full float edge-case
    /// pool: NaN payloads (both signs), infinities, -0.0, denormals,
    /// duplicate timestamps, multi-segment overlap.
    #[test]
    fn projection_filter_count_and_min_max_are_bit_identical(
        specs in arb_dataset(interesting_value as ValuePool),
        pred in arb_pred(),
        limit in prop::option::of(1usize..8),
    ) {
        block_on(async move {
            let tenant = tenant_id("gate");
            let fixture = Fixture::memory(&[(&tenant, &specs)]).await;
            let queries = vec![
                Query { shape: Shape::Projection { limit }, pred: pred.clone() },
                Query { shape: Shape::Projection { limit: None }, pred: pred.clone() },
                Query { shape: Shape::SelectionAggregates, pred: pred.clone() },
                Query { shape: Shape::GroupedSelectionAggregates, pred: pred.clone() },
                // Grouped sum folds sequentially in row order, so the full
                // value pool is fair game for it.
                Query { shape: Shape::GroupedSum, pred },
            ];
            assert_all(&fixture, &tenant, &queries).await;
        });
    }

    /// Ungrouped `sum` over exactly-representable values, where arrow's
    /// lane-parallel accumulation is provably bit-identical to a sequential
    /// fold. Every other shape rides along on the same datasets.
    #[test]
    fn sum_is_bit_identical_over_exactly_representable_values(
        specs in arb_dataset(exact_sum_value as ValuePool),
        pred in arb_pred(),
    ) {
        block_on(async move {
            let tenant = tenant_id("gate");
            let fixture = Fixture::memory(&[(&tenant, &specs)]).await;
            let queries = vec![
                Query { shape: Shape::Sum, pred: pred.clone() },
                Query { shape: Shape::GroupedSum, pred: pred.clone() },
                Query { shape: Shape::SelectionAggregates, pred },
            ];
            assert_all(&fixture, &tenant, &queries).await;
        });
    }

    /// ADR-0023 decision 1: grouped and ungrouped MIN/MAX over the same
    /// multiset agree bit for bit. On a single-group dataset the grouped query
    /// returns one `[min, max]` row that must equal the ungrouped one over the
    /// full float edge-case pool (NaN payloads of both signs, both infinities,
    /// signed zeros, denormals). Ungrouped MIN/MAX is itself gated against the
    /// independent reference above, so agreement here ties the grouped path to
    /// the same total-order oracle.
    #[test]
    fn grouped_and_ungrouped_min_max_agree_bit_for_bit(
        specs in arb_single_group_dataset(),
    ) {
        block_on(async move {
            let tenant = tenant_id("gate");
            let fixture = Fixture::memory(&[(&tenant, &specs)]).await;
            let grouped = scalar(
                &fixture,
                &tenant,
                "SELECT min(value), max(value) FROM samples GROUP BY series_id",
            )
            .await;
            let ungrouped = scalar(
                &fixture,
                &tenant,
                "SELECT min(value), max(value) FROM samples",
            )
            .await;
            assert_eq!(
                grouped, ungrouped,
                "grouped and ungrouped min/max must agree bit for bit"
            );
        });
    }
}

// ---------------------------------------------------------------------------
// Adversarial pushdown cases (review F8)
// ---------------------------------------------------------------------------

/// The `ts` predicate shape the widen-only extractor *does* recognize (bare
/// `ts` against a timestamp literal), in OR, BETWEEN, and mixed forms. A
/// pruning bug that narrowed instead of widened would drop rows the residual
/// filter can never bring back, and the reference -- which never prunes --
/// would catch it.
#[tokio::test]
async fn ts_literal_predicates_are_lossless() {
    let tenant = tenant_id("gate");
    let specs = overlap_dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let literal = |ns: i64| format!("TIMESTAMP '1970-01-01 00:00:00.{:09}'", ns);

    let cases = vec![
        // Recognized conjunctive bounds: pruning may fire, must not narrow.
        format!(
            "SELECT ts, value FROM samples WHERE ts >= {} AND ts < {} \
             ORDER BY series_id, ts",
            literal(1),
            literal(4)
        ),
        // BETWEEN over the recognized shape.
        format!(
            "SELECT ts, value FROM samples WHERE ts BETWEEN {} AND {} \
             ORDER BY series_id, ts",
            literal(1),
            literal(4)
        ),
        // OR: nothing may be pruned, because neither branch bounds the query.
        format!(
            "SELECT ts, value FROM samples WHERE (ts >= {} AND ts < {}) OR value > 100.0 \
             ORDER BY series_id, ts",
            literal(1),
            literal(3)
        ),
        // Function-wrapped ts: unrecognized, so no pruning at all.
        "SELECT ts, value FROM samples WHERE CAST(ts AS BIGINT) % 2 = 0 \
         ORDER BY series_id, ts"
            .to_string(),
    ];

    let snapshot = fixture.snapshot(&tenant).await;
    let reference = util::reference_rows(&fixture.fetcher, tenant.hash(), &snapshot).await;

    for sql in cases {
        let outcome = fixture
            .executor
            .execute(tenant.hash(), &request(&sql))
            .await
            .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
        let got = actual_rows(&outcome.output);

        // The reference applies the same predicate with no pruning at all.
        let want: Vec<Row> = reference
            .iter()
            .filter(|r| match sql.as_str() {
                s if s.contains("BETWEEN") => r.ts >= 1 && r.ts <= 4,
                s if s.contains("% 2 = 0") => r.ts % 2 == 0,
                s if s.contains("value > 100.0") => {
                    (r.ts >= 1 && r.ts < 3) || r.value.total_cmp(&100.0) == Ordering::Greater
                }
                _ => r.ts >= 1 && r.ts < 4,
            })
            .map(|r| vec![Cell::Int(r.ts), Cell::float(r.value)])
            .collect();

        assert_eq!(got, want, "pruning lost or invented rows for: {sql}");
    }
}

// ---------------------------------------------------------------------------
// Golden cases pinned against the workspace's datafusion 54.1.0 / arrow 58.4.0
// ---------------------------------------------------------------------------
//
// These encode DataFusion's *observed* NaN and signed-zero behavior as
// literal expected bits. They are not derived from the reference functions in
// util::gate, so they pin both sides: an upstream semantics change on a version
// bump fails here, and a drift in the reference fails against the proptests.

fn overlap_dataset() -> Vec<SegSpec> {
    vec![
        SegSpec::new(
            10,
            1,
            1,
            vec![
                SeriesSpec::new("a", vec![(1, 1.0), (2, 2.0), (2, 20.0)]),
                SeriesSpec::new("b", vec![(1, -1.0), (3, 3.0)]),
            ],
        ),
        SegSpec::new(
            20,
            1,
            1,
            vec![SeriesSpec::new("a", vec![(2, 200.0), (4, 4.0)])],
        ),
    ]
}

/// Fetch one scalar cell from a single-row result.
async fn scalar(fixture: &Fixture, tenant: &TenantId, sql: &str) -> Vec<Cell> {
    let outcome = fixture
        .executor
        .execute(tenant.hash(), &request(sql))
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n{e}"));
    let rows = actual_rows(&outcome.output);
    assert_eq!(rows.len(), 1, "expected exactly one row for: {sql}");
    rows.into_iter().next().expect("row")
}

const NAN_POS: u64 = 0x7ff8_0000_0000_0001;
const NAN_NEG: u64 = 0xfff8_0000_0000_0001;

/// Ungrouped min/max follow `f64::total_cmp`: negative NaN is below
/// -infinity and positive NaN is above +infinity.
#[tokio::test]
async fn golden_ungrouped_min_max_use_total_order_nan_semantics() {
    let tenant = tenant_id("golden");
    let specs = vec![SegSpec::new(
        1,
        1,
        1,
        vec![SeriesSpec::new(
            "nan",
            vec![
                (1, f64::from_bits(NAN_NEG)),
                (2, f64::NEG_INFINITY),
                (3, -1.0),
                (4, 1.0),
                (5, f64::INFINITY),
                (6, f64::from_bits(NAN_POS)),
            ],
        )],
    )];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let row = scalar(
        &fixture,
        &tenant,
        "SELECT min(value), max(value) FROM samples",
    )
    .await;
    assert_eq!(
        row,
        vec![Cell::FloatBits(NAN_NEG), Cell::FloatBits(NAN_POS)],
        "ungrouped min/max must place negative NaN lowest and positive NaN highest"
    );
}

/// Grouped MIN/MAX use the `f64::total_cmp` total order (ADR-0023), pinned by
/// bits against datafusion 54.1.0. Each series is one group, so a grouped
/// query filtered to that series returns a single `[min, max]` row. The bits
/// are literal, not derived from the reference folds, so this pins both the
/// UDAF (crate::minmax) and, transitively, the reference the proptests trust.
///
/// The four behaviors the interim rejection existed to avoid (issue #143) are
/// all correct here, where the old `PrimitiveGroupsAccumulator` was wrong:
/// a NaN survives as the group's extreme with its payload intact; a NaN plus a
/// smaller value does not poison MIN; `-0.0`/`0.0` resolve by sign in both
/// arrival orders; and an all-infinite group returns the infinity, never the
/// `f64::MAX`/`f64::MIN` seed.
#[tokio::test]
async fn golden_grouped_min_max_use_total_order() {
    let tenant = tenant_id("golden");
    let specs = vec![SegSpec::new(
        1,
        1,
        1,
        vec![
            // Positive NaN is above +Inf: it is the MAX and its payload is
            // preserved.
            SeriesSpec::new(
                "nanmax",
                vec![(1, 1.0), (2, f64::from_bits(NAN_POS)), (3, 2.0)],
            ),
            // Negative NaN is below -Inf: it is the MIN, payload preserved.
            SeriesSpec::new(
                "nanmin",
                vec![(1, f64::from_bits(NAN_NEG)), (2, -1.0), (3, 5.0)],
            ),
            // NaN first, then a smaller finite value: MIN is the finite value,
            // not poisoned to NaN or to the newest row.
            SeriesSpec::new("nanpoison", vec![(1, f64::from_bits(NAN_POS)), (2, -3.0)]),
            // Signed zero in both arrival orders: MIN is -0.0, MAX is 0.0.
            SeriesSpec::new("zeropf", vec![(1, 0.0), (2, -0.0)]),
            SeriesSpec::new("zeronf", vec![(1, -0.0), (2, 0.0)]),
            // All-infinite groups return the infinity, never the seed.
            SeriesSpec::new("allpinf", vec![(1, f64::INFINITY), (2, f64::INFINITY)]),
            SeriesSpec::new(
                "allninf",
                vec![(1, f64::NEG_INFINITY), (2, f64::NEG_INFINITY)],
            ),
        ],
    )];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    // (metric, expected MIN cell, expected MAX cell), pinned by bits.
    let cases = [
        ("nanmax", Cell::float(1.0), Cell::FloatBits(NAN_POS)),
        ("nanmin", Cell::FloatBits(NAN_NEG), Cell::float(5.0)),
        ("nanpoison", Cell::float(-3.0), Cell::FloatBits(NAN_POS)),
        ("zeropf", Cell::float(-0.0), Cell::float(0.0)),
        ("zeronf", Cell::float(-0.0), Cell::float(0.0)),
        (
            "allpinf",
            Cell::float(f64::INFINITY),
            Cell::float(f64::INFINITY),
        ),
        (
            "allninf",
            Cell::float(f64::NEG_INFINITY),
            Cell::float(f64::NEG_INFINITY),
        ),
    ];

    for (metric, want_min, want_max) in cases {
        let sql = format!(
            "SELECT min(value), max(value) FROM samples \
             WHERE label(labels, '__name__') = '{metric}' GROUP BY series_id"
        );
        let row = scalar(&fixture, &tenant, &sql).await;
        assert_eq!(
            row,
            vec![want_min.clone(), want_max.clone()],
            "grouped min/max for {metric}"
        );
    }
}

/// The issue #159 shapes: a grouped `min`/`max` reachable only through a
/// query-level `ORDER BY` (a field of `Query`, not `Select`) and through
/// `HAVING`. The interim validation walk had a hole for the `ORDER BY` case
/// (#159) and rejected the `HAVING` case; both now execute to correct results
/// against a per-group reference fold.
#[tokio::test]
async fn grouped_min_max_issue_159_shapes_execute_correctly() {
    let tenant = tenant_id("golden");
    let specs = overlap_dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;
    let snapshot = fixture.snapshot(&tenant).await;
    let reference = util::reference_rows(&fixture.fetcher, tenant.hash(), &snapshot).await;
    let groups = group_by_series(&reference.iter().collect::<Vec<_>>());

    // `GROUP BY ... ORDER BY max(value)`: rows ordered by each group's
    // total-order max. overlap_dataset's two groups have distinct maxes, so
    // the order is unambiguous.
    let mut by_max: Vec<([u8; 16], f64)> = groups
        .iter()
        .map(|(sid, values)| (*sid, max_total_order(values).expect("non-empty group")))
        .collect();
    by_max.sort_by(|a, b| a.1.total_cmp(&b.1));
    let want_order_by: Vec<Row> = by_max
        .iter()
        .map(|(sid, m)| vec![Cell::Bytes(sid.to_vec()), Cell::float(*m)])
        .collect();
    let got_order_by = actual_rows(
        &fixture
            .executor
            .execute(
                tenant.hash(),
                &request(
                    "SELECT series_id, max(value) FROM samples \
                     GROUP BY series_id ORDER BY max(value)",
                ),
            )
            .await
            .expect("order by max(value)")
            .output,
    );
    assert_eq!(
        got_order_by, want_order_by,
        "GROUP BY ... ORDER BY max(value)"
    );

    // `HAVING min(value) > 0`: keep only groups whose total-order min is
    // strictly greater than zero, ordered by series_id.
    let want_having: Vec<Row> = groups
        .iter()
        .filter_map(|(sid, values)| {
            let min = min_total_order(values).expect("non-empty group");
            (min.total_cmp(&0.0) == Ordering::Greater)
                .then(|| vec![Cell::Bytes(sid.to_vec()), Cell::float(min)])
        })
        .collect();
    let got_having = actual_rows(
        &fixture
            .executor
            .execute(
                tenant.hash(),
                &request(
                    "SELECT series_id, min(value) FROM samples \
                     GROUP BY series_id HAVING min(value) > 0 ORDER BY series_id",
                ),
            )
            .await
            .expect("having min(value)")
            .output,
    );
    assert_eq!(
        got_having, want_having,
        "GROUP BY ... HAVING min(value) > 0"
    );
}

/// Non-float grouped MIN/MAX delegate to the built-in accumulator, whose
/// `partial_cmp` is already a total order for integers. `min(ts)`/`max(ts)`
/// over the Timestamp column must match an integer reference fold per group
/// (ADR-0023 decision 4).
#[tokio::test]
async fn golden_grouped_min_max_over_ts_delegates() {
    let tenant = tenant_id("golden");
    let specs = overlap_dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;
    let snapshot = fixture.snapshot(&tenant).await;
    let reference = util::reference_rows(&fixture.fetcher, tenant.hash(), &snapshot).await;

    let mut groups: BTreeMap<[u8; 16], Vec<i64>> = BTreeMap::new();
    for row in &reference {
        groups.entry(row.series_id).or_default().push(row.ts);
    }
    let want: Vec<Row> = groups
        .iter()
        .map(|(sid, ts)| {
            vec![
                Cell::Bytes(sid.to_vec()),
                Cell::Int(*ts.iter().min().expect("non-empty group")),
                Cell::Int(*ts.iter().max().expect("non-empty group")),
            ]
        })
        .collect();

    let got = actual_rows(
        &fixture
            .executor
            .execute(
                tenant.hash(),
                &request(
                    "SELECT series_id, min(ts), max(ts) FROM samples \
                     GROUP BY series_id ORDER BY series_id",
                ),
            )
            .await
            .expect("min/max over ts")
            .output,
    );
    assert_eq!(got, want, "grouped min/max over ts");
}

/// `sum` with non-finite input. The properties pinned here are the ones that
/// are well defined regardless of arrow's lane order: an infinite sum keeps
/// its sign, mixed infinities produce NaN, and a NaN input produces NaN. The
/// NaN *payload* is deliberately not pinned, because which lane sees the NaN
/// first is architecture-dependent (util::gate docs).
#[tokio::test]
async fn golden_sum_with_infinities_and_nan() {
    let tenant = tenant_id("golden");
    let specs = vec![SegSpec::new(
        1,
        1,
        1,
        vec![
            SeriesSpec::new("pinf", vec![(1, 1.0), (2, f64::INFINITY)]),
            SeriesSpec::new("mixed", vec![(1, f64::INFINITY), (2, f64::NEG_INFINITY)]),
            SeriesSpec::new("nan", vec![(1, 1.0), (2, f64::from_bits(NAN_POS))]),
        ],
    )];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    for (metric, check) in [("pinf", "infinity"), ("mixed", "nan"), ("nan", "nan")] {
        let sql =
            format!("SELECT sum(value) FROM samples WHERE label(labels, '__name__') = '{metric}'");
        let row = scalar(&fixture, &tenant, &sql).await;
        let Cell::FloatBits(bits) = row[0] else {
            panic!("expected a float for {metric}, got {row:?}");
        };
        let value = f64::from_bits(bits);
        match check {
            "infinity" => assert_eq!(
                value,
                f64::INFINITY,
                "sum over a finite value and +Inf must be +Inf"
            ),
            _ => assert!(value.is_nan(), "sum for {metric} must be NaN, got {value}"),
        }
    }
}

/// `avg` never reaches the gate: it is rejected at validation with a message
/// naming the admitted aggregate set (ADR-0022 decision 2, the allowlist).
#[tokio::test]
async fn avg_is_rejected_before_planning() {
    let tenant = tenant_id("golden");
    let specs = overlap_dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let err = fixture
        .executor
        .execute(tenant.hash(), &request("SELECT avg(value) FROM samples"))
        .await
        .expect_err("avg must be rejected");
    let message = err.client_message();
    assert!(message.contains("avg"), "{message}");
    for admitted in ["count", "sum", "min", "max"] {
        assert!(
            message.contains(admitted),
            "message must name the admitted aggregate {admitted}: {message}"
        );
    }
}

/// Duplicate timestamps and multi-segment overlap under the canonical dedup
/// ordering, with the reference agreeing sample for sample. This is the
/// operator-layer counterpart to layer 1's scan oracle.
#[tokio::test]
async fn golden_multi_segment_overlap_and_duplicate_ts() {
    let tenant = tenant_id("golden");
    let specs = overlap_dataset();
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let queries = vec![
        Query {
            shape: Shape::Projection { limit: None },
            pred: Pred::True,
        },
        Query {
            shape: Shape::Projection { limit: Some(3) },
            pred: Pred::True,
        },
        Query {
            shape: Shape::SelectionAggregates,
            pred: Pred::True,
        },
        Query {
            shape: Shape::GroupedSelectionAggregates,
            pred: Pred::True,
        },
        Query {
            shape: Shape::Sum,
            pred: Pred::TsGe(2),
        },
        Query {
            shape: Shape::GroupedSum,
            pred: Pred::TsLt(4),
        },
    ];
    assert_all(&fixture, &tenant, &queries).await;
}
