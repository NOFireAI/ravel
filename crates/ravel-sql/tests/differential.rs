//! B3 layer-2 differential gate (issue #22):
//! docs/arrow-datafusion-plan.md section 2 "Exactness", review F5 and F7.
//!
//! Layer 1 (B1, tests/pipeline.rs) gates the *scan* against an independent
//! greatest-wins oracle. This file gates the *operator layer*: a reference
//! executor for the v1 SQL subset (project, filter, count/sum/min/max, group
//! by, order by, limit) evaluates over rows it pulls through
//! `util::reference_rows` -- an independent merge over `SegmentFetcher::fetch`
//! -- and the result is compared f64-bit-identical against the same query run
//! through the real SQL path.
//!
//! # Why the reference is not "naive"
//!
//! Review F7's whole point is that "DataFusion == naive scalar arithmetic" is
//! false. Everything below was verified against the pinned datafusion 54.1.0
//! and arrow 58.4.0 in the workspace lockfile, reading the accumulators
//! rather than guessing:
//!
//! - **Comparisons are total-order, not IEEE.** arrow's compare kernels
//!   (`arrow_ord::cmp`) use `f64::total_cmp`, so in SQL `value > 5.0` is
//!   *true* for NaN and `-0.0 < 0.0` is *true*. A reference using Rust's `>`
//!   would disagree on every NaN row. [`Pred::eval`] uses `total_cmp`.
//!
//! - **min/max have two different implementations**, chosen by whether the
//!   query has a GROUP BY:
//!   - No GROUP BY: `AggregateExec` takes the `AggregateStream` path, whose
//!     `MinAccumulator`/`MaxAccumulator` call arrow's `min`/`max` kernels and
//!     combine partials with `f64::total_cmp`. Result: a true total order,
//!     where negative NaN is the minimum and positive NaN the maximum, and
//!     -0.0 < 0.0.
//!   - With GROUP BY: `GroupedHashAggregateStream` uses
//!     `PrimitiveGroupsAccumulator` with `|cur, new| if
//!     new.partial_cmp(cur) is Some(Less) | None { cur = new }`, seeded with
//!     `f64::MAX` (min) / `f64::MIN` (max). That is *not* a total order: it
//!     is order-dependent, NaN latches then releases, and `-0.0` never
//!     displaces `0.0` because they compare `Equal`.
//!
//!   [`min_total_order`] and [`min_grouped`] implement the two exactly.
//!
//! - **The grouped seed leaks into results.** Because the seed is `f64::MAX`
//!   and `partial_cmp(+Inf, f64::MAX)` is `Greater`, a GROUP BY min over a
//!   group whose only value is `+Inf` returns `f64::MAX`, not `+Inf`. That is
//!   a DataFusion quirk, not a Ravel one; it is pinned as a golden case below
//!   so an upstream fix trips CI instead of silently changing SQL results.
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

use datafusion::arrow::array::{
    Array, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use proptest::prelude::*;
use ravel_sql::QueryOutput;
use ravel_types::TenantId;
use util::{Fixture, RefRow, SegSpec, SeriesSpec, request, tenant_id};

// ---------------------------------------------------------------------------
// Comparable cells
// ---------------------------------------------------------------------------

/// One result cell, reduced to something comparable bit-for-bit. Floats are
/// held as `to_bits`, never as `f64`, so NaN payloads and the sign of zero
/// are part of the comparison instead of being erased by `==`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Cell {
    Null,
    Int(i64),
    FloatBits(u64),
    Bytes(Vec<u8>),
    Text(String),
}

impl Cell {
    fn float(v: f64) -> Cell {
        Cell::FloatBits(v.to_bits())
    }
}

type Row = Vec<Cell>;

/// Reduce the real query's batches to comparable rows, driven by the output
/// schema so any column type the v1 subset can produce is handled.
fn actual_rows(output: &QueryOutput) -> Vec<Row> {
    let mut rows = Vec::with_capacity(output.num_rows());
    for batch in output.batches() {
        for row in 0..batch.num_rows() {
            rows.push(actual_row(batch, row));
        }
    }
    rows
}

fn actual_row(batch: &RecordBatch, row: usize) -> Row {
    batch
        .columns()
        .iter()
        .map(|col| {
            if col.is_null(row) {
                return Cell::Null;
            }
            match col.data_type() {
                DataType::Int64 => Cell::Int(
                    col.as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("int64")
                        .value(row),
                ),
                DataType::Float64 => Cell::float(
                    col.as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("float64")
                        .value(row),
                ),
                DataType::Timestamp(TimeUnit::Nanosecond, _) => Cell::Int(
                    col.as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .expect("timestamp")
                        .value(row),
                ),
                DataType::FixedSizeBinary(_) => Cell::Bytes(
                    col.as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .expect("fixed size binary")
                        .value(row)
                        .to_vec(),
                ),
                DataType::Utf8 => Cell::Text(
                    col.as_any()
                        .downcast_ref::<StringArray>()
                        .expect("utf8")
                        .value(row)
                        .to_string(),
                ),
                other => panic!("differential gate has no cell mapping for {other}"),
            }
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The v1 predicate grammar
// ---------------------------------------------------------------------------

/// A predicate in the v1 subset, in both its SQL and its evaluable form.
///
/// `ts` is compared through `CAST(ts AS BIGINT)` so the literal is an
/// unambiguous nanosecond integer. That shape is deliberately *not* one the
/// widen-only pushdown extractor recognizes (crate::pushdown only matches
/// bare `ts` against a timestamp literal), which makes every proptest case an
/// adversarial no-pushdown case; the recognized shape is covered separately
/// by [`ts_literal_predicates_are_lossless`].
#[derive(Clone, Debug)]
enum Pred {
    True,
    TsGe(i64),
    TsLt(i64),
    TsBetween(i64, i64),
    ValueGt(f64),
    And(Box<Pred>, Box<Pred>),
    Or(Box<Pred>, Box<Pred>),
}

impl Pred {
    fn sql(&self) -> String {
        match self {
            Pred::True => "1 = 1".to_string(),
            Pred::TsGe(l) => format!("CAST(ts AS BIGINT) >= {l}"),
            Pred::TsLt(u) => format!("CAST(ts AS BIGINT) < {u}"),
            Pred::TsBetween(l, u) => format!("CAST(ts AS BIGINT) BETWEEN {l} AND {u}"),
            Pred::ValueGt(c) => format!("value > {c:?}"),
            Pred::And(a, b) => format!("({}) AND ({})", a.sql(), b.sql()),
            Pred::Or(a, b) => format!("({}) OR ({})", a.sql(), b.sql()),
        }
    }

    /// Evaluate against one reference row.
    ///
    /// `value > c` uses `total_cmp`, matching arrow's compare kernels: NaN is
    /// greater than every finite value and `-0.0` is strictly less than
    /// `0.0`. Rust's own `>` would be wrong here.
    fn eval(&self, row: &RefRow) -> bool {
        match self {
            Pred::True => true,
            Pred::TsGe(l) => row.ts >= *l,
            Pred::TsLt(u) => row.ts < *u,
            Pred::TsBetween(l, u) => row.ts >= *l && row.ts <= *u,
            Pred::ValueGt(c) => row.value.total_cmp(c) == Ordering::Greater,
            Pred::And(a, b) => a.eval(row) && b.eval(row),
            Pred::Or(a, b) => a.eval(row) || b.eval(row),
        }
    }
}

// ---------------------------------------------------------------------------
// Replicated aggregate semantics
// ---------------------------------------------------------------------------

/// Ungrouped `min`: arrow's kernel plus DataFusion's scalar combine, both on
/// `f64::total_cmp`. Empty input is SQL NULL.
fn min_total_order(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(|a, b| {
        if b.total_cmp(&a) == Ordering::Less {
            b
        } else {
            a
        }
    })
}

/// Ungrouped `max`, same construction.
fn max_total_order(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(|a, b| {
        if b.total_cmp(&a) == Ordering::Greater {
            b
        } else {
            a
        }
    })
}

/// The grouped accumulator's single update step: take `new` when it compares
/// `wanted` against the running value, and *also* when `partial_cmp` returns
/// `None` (a NaN was involved on either side).
fn grouped_step(cur: f64, new: f64, wanted: Ordering) -> f64 {
    match new.partial_cmp(&cur) {
        Some(order) if order == wanted => new,
        None => new,
        _ => cur,
    }
}

/// One grouped aggregation, seed to result, including the second phase.
///
/// `AggregateExec` runs grouped min/max in two phases even on a single
/// partition: `Partial` folds the rows into a per-group value, then `Final`
/// merges that value into a **freshly seeded** accumulator using the same
/// step. The second phase is not a no-op, and modelling it is not optional:
/// for a group whose partial result is `+Inf`, `Partial` already leaves the
/// `f64::MAX` seed in place for `min`, and even when `Partial` does produce a
/// value, `Final` re-applies the seed comparison to it. Folding only the rows
/// disagrees with DataFusion on exactly the infinity cases.
fn grouped_aggregate(values: &[f64], seed: f64, wanted: Ordering) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let partial = values
        .iter()
        .fold(seed, |cur, &new| grouped_step(cur, new, wanted));
    Some(grouped_step(seed, partial, wanted))
}

/// Grouped `min`: `PrimitiveGroupsAccumulator` seeded with `f64::MAX`,
/// folding `partial_cmp` in row order, then merged through a second seeded
/// pass.
fn min_grouped(values: &[f64]) -> Option<f64> {
    grouped_aggregate(values, f64::MAX, Ordering::Less)
}

/// Grouped `max`, seeded with `f64::MIN`.
fn max_grouped(values: &[f64]) -> Option<f64> {
    grouped_aggregate(values, f64::MIN, Ordering::Greater)
}

/// `sum`: a sequential fold from `0.0` in input order. Exact for the grouped
/// accumulator by construction, and exact for the ungrouped arrow kernel
/// whenever every partial sum is exactly representable (see the module docs).
fn sum_sequential(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().fold(0.0f64, |acc, v| acc + v))
}

// ---------------------------------------------------------------------------
// The reference executor
// ---------------------------------------------------------------------------

/// The query shapes the v1 subset covers.
#[derive(Clone, Debug)]
enum Shape {
    /// `SELECT ts, value ... ORDER BY series_id, ts [LIMIT n]`
    Projection { limit: Option<usize> },
    /// `SELECT count(value), min(value), max(value) ...`
    SelectionAggregates,
    /// `SELECT series_id, count(value), min(value), max(value) ... GROUP BY
    /// series_id ORDER BY series_id`
    GroupedSelectionAggregates,
    /// `SELECT sum(value) ...`
    Sum,
    /// `SELECT series_id, sum(value) ... GROUP BY series_id ORDER BY
    /// series_id`
    GroupedSum,
}

#[derive(Clone, Debug)]
struct Query {
    shape: Shape,
    pred: Pred,
}

impl Query {
    fn sql(&self) -> String {
        let where_clause = format!("WHERE {}", self.pred.sql());
        match &self.shape {
            Shape::Projection { limit } => {
                let limit = limit.map_or(String::new(), |n| format!(" LIMIT {n}"));
                format!(
                    "SELECT ts, value FROM samples {where_clause} \
                     ORDER BY series_id, ts{limit}"
                )
            }
            Shape::SelectionAggregates => {
                format!("SELECT count(value), min(value), max(value) FROM samples {where_clause}")
            }
            Shape::GroupedSelectionAggregates => format!(
                "SELECT series_id, count(value), min(value), max(value) \
                 FROM samples {where_clause} GROUP BY series_id ORDER BY series_id"
            ),
            Shape::Sum => format!("SELECT sum(value) FROM samples {where_clause}"),
            Shape::GroupedSum => format!(
                "SELECT series_id, sum(value) FROM samples {where_clause} \
                 GROUP BY series_id ORDER BY series_id"
            ),
        }
    }

    /// Evaluate over the independent reference rows, which arrive already in
    /// the canonical `(series_id, ts)` dedup order.
    fn eval(&self, rows: &[RefRow]) -> Vec<Row> {
        let kept: Vec<&RefRow> = rows.iter().filter(|r| self.pred.eval(r)).collect();

        match &self.shape {
            Shape::Projection { limit } => {
                let n = limit.unwrap_or(kept.len());
                kept.iter()
                    .take(n)
                    .map(|r| vec![Cell::Int(r.ts), Cell::float(r.value)])
                    .collect()
            }
            Shape::SelectionAggregates => {
                let values: Vec<f64> = kept.iter().map(|r| r.value).collect();
                vec![vec![
                    Cell::Int(values.len() as i64),
                    optional_float(min_total_order(&values)),
                    optional_float(max_total_order(&values)),
                ]]
            }
            Shape::GroupedSelectionAggregates => group_by_series(&kept)
                .into_iter()
                .map(|(sid, values)| {
                    vec![
                        Cell::Bytes(sid.to_vec()),
                        Cell::Int(values.len() as i64),
                        optional_float(min_grouped(&values)),
                        optional_float(max_grouped(&values)),
                    ]
                })
                .collect(),
            Shape::Sum => {
                let values: Vec<f64> = kept.iter().map(|r| r.value).collect();
                vec![vec![optional_float(sum_sequential(&values))]]
            }
            Shape::GroupedSum => group_by_series(&kept)
                .into_iter()
                .map(|(sid, values)| {
                    vec![
                        Cell::Bytes(sid.to_vec()),
                        optional_float(sum_sequential(&values)),
                    ]
                })
                .collect(),
        }
    }
}

fn optional_float(value: Option<f64>) -> Cell {
    value.map_or(Cell::Null, Cell::float)
}

/// Group the kept rows by series id, preserving `(series_id, ts)` order
/// within each group -- the order the grouped accumulators see, and the one
/// their order-dependent min/max semantics depend on. `BTreeMap` also gives
/// the `ORDER BY series_id` output order for free.
fn group_by_series(rows: &[&RefRow]) -> Vec<([u8; 16], Vec<f64>)> {
    let mut groups: BTreeMap<[u8; 16], Vec<f64>> = BTreeMap::new();
    for row in rows {
        groups.entry(row.series_id).or_default().push(row.value);
    }
    groups.into_iter().collect()
}

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

// ---------------------------------------------------------------------------
// Value pools and dataset strategies
// ---------------------------------------------------------------------------

/// A value pool, named by constructor so the dataset strategies can be
/// parameterized by it (a `BoxedStrategy` is not `Clone`, a `fn` pointer is).
type ValuePool = fn() -> BoxedStrategy<f64>;

/// The full float edge-case pool: NaN with distinct payloads (both signs),
/// both infinities, both zeros, a denormal, and ordinary values.
fn interesting_value() -> BoxedStrategy<f64> {
    prop_oneof![
        Just(0.0f64),
        Just(-0.0f64),
        Just(1.0f64),
        Just(-1.0f64),
        Just(2.5f64),
        Just(f64::INFINITY),
        Just(f64::NEG_INFINITY),
        Just(f64::from_bits(0x7ff8_0000_0000_0001)),
        Just(f64::from_bits(0x7ff8_0000_0000_00aa)),
        Just(f64::from_bits(0xfff8_0000_0000_0001)), // negative NaN
        Just(f64::from_bits(0x0000_0000_0000_0001)), // denormal
        Just(f64::MIN_POSITIVE),
    ]
    .boxed()
}

/// Values whose partial sums are exactly representable in f64, so every
/// association order (including arrow's lane-parallel one) yields identical
/// bits. See the module docs for why ungrouped `sum` needs this.
fn exact_sum_value() -> BoxedStrategy<f64> {
    prop_oneof![
        Just(0.0f64),
        Just(-0.0f64),
        (-64i64..64).prop_map(|n| n as f64),
        (-8i64..8).prop_map(|n| (n * 1024) as f64),
    ]
    .boxed()
}

fn arb_series(value: ValuePool) -> impl Strategy<Value = SeriesSpec> {
    let metric = prop_oneof![Just("a"), Just("b"), Just("c")].prop_map(|s| s.to_string());
    // A small ts range forces duplicate timestamps within a segment and
    // overlap across segments.
    let samples = prop::collection::vec((0i64..6, value()), 1..8);
    (metric, samples).prop_map(|(metric, samples)| SeriesSpec::new(&metric, samples))
}

fn arb_segment(value: ValuePool) -> impl Strategy<Value = SegSpec> {
    let series = prop::collection::vec(arb_series(value), 1..4);
    (0i64..4, 1u64..3, 1u64..3, series).prop_map(
        |(created_unix_ns, writer_epoch, writer_seq, series)| {
            SegSpec::new(created_unix_ns, writer_epoch, writer_seq, series)
        },
    )
}

fn arb_dataset(value: ValuePool) -> impl Strategy<Value = Vec<SegSpec>> {
    prop::collection::vec(arb_segment(value), 1..4).prop_map(|specs| {
        // The segment writer rejects duplicate series ids inside one
        // segment; collapse duplicate metrics per segment. Cross-segment
        // duplicates are the point and are kept.
        specs
            .into_iter()
            .map(|mut seg| {
                let mut seen = std::collections::HashSet::new();
                seg.series.retain(|s| seen.insert(s.metric.clone()));
                seg
            })
            .collect()
    })
}

fn arb_pred() -> impl Strategy<Value = Pred> {
    let leaf = prop_oneof![
        Just(Pred::True),
        (0i64..6).prop_map(Pred::TsGe),
        (0i64..7).prop_map(Pred::TsLt),
        (0i64..4, 2i64..7).prop_map(|(l, u)| Pred::TsBetween(l, u)),
        prop_oneof![
            Just(Pred::ValueGt(0.0)),
            Just(Pred::ValueGt(-1.0)),
            Just(Pred::ValueGt(1.0)),
        ],
    ];
    leaf.prop_recursive(2, 6, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Pred::And(Box::new(a), Box::new(b))),
            (inner.clone(), inner).prop_map(|(a, b)| Pred::Or(Box::new(a), Box::new(b))),
        ]
    })
}

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(fut)
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
// literal expected bits. They are not derived from the reference functions
// above, so they pin both sides: an upstream semantics change on a version
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

/// Grouped min/max do *not* use a total order: the accumulator folds
/// `partial_cmp` in row order, so a NaN latches the running value and the
/// next comparison (also `None`) replaces it again. The pinned expectation is
/// the last value in `(series_id, ts)` order for both min and max.
#[tokio::test]
async fn golden_grouped_min_max_pin_the_partial_cmp_accumulator() {
    let tenant = tenant_id("golden");
    let specs = vec![SegSpec::new(
        1,
        1,
        1,
        vec![SeriesSpec::new(
            "nan",
            vec![(1, -1.0), (2, f64::from_bits(NAN_POS)), (3, 5.0)],
        )],
    )];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let row = scalar(
        &fixture,
        &tenant,
        "SELECT min(value), max(value) FROM samples GROUP BY series_id",
    )
    .await;
    assert_eq!(
        row,
        vec![Cell::float(5.0), Cell::float(5.0)],
        "after a NaN, every later comparison is None and the accumulator \
         takes the newest value, so both min and max end at the last row"
    );
}

/// The signed-zero split between the two implementations, pinned in both
/// directions. Ungrouped uses `total_cmp`, where `-0.0 < 0.0`; grouped uses
/// `partial_cmp`, where they are `Equal` and the first one seen wins.
#[tokio::test]
async fn golden_negative_zero_differs_between_grouped_and_ungrouped_min() {
    let tenant = tenant_id("golden");
    let specs = vec![SegSpec::new(
        1,
        1,
        1,
        vec![SeriesSpec::new("z", vec![(1, 0.0), (2, -0.0)])],
    )];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let ungrouped = scalar(&fixture, &tenant, "SELECT min(value) FROM samples").await;
    assert_eq!(
        ungrouped,
        vec![Cell::float(-0.0)],
        "total_cmp orders -0.0 below 0.0"
    );

    let grouped = scalar(
        &fixture,
        &tenant,
        "SELECT min(value) FROM samples GROUP BY series_id",
    )
    .await;
    assert_eq!(
        grouped,
        vec![Cell::float(0.0)],
        "partial_cmp calls 0.0 and -0.0 equal, so the first row's +0.0 stands"
    );
}

/// The grouped accumulator's seed leaks into the result: `partial_cmp(+Inf,
/// f64::MAX)` is `Greater`, so a group whose only value is `+Inf` yields
/// `f64::MAX` for `min` (and symmetrically `f64::MIN` for `max` over
/// `-Inf`). This is a DataFusion quirk, pinned so an upstream fix is a
/// visible CI failure rather than a silent change in SQL results.
#[tokio::test]
async fn golden_grouped_min_max_leak_the_accumulator_seed_for_infinities() {
    let tenant = tenant_id("golden");
    let specs = vec![SegSpec::new(
        1,
        1,
        1,
        vec![
            SeriesSpec::new("pos", vec![(1, f64::INFINITY)]),
            SeriesSpec::new("neg", vec![(1, f64::NEG_INFINITY)]),
        ],
    )];
    let fixture = Fixture::memory(&[(&tenant, &specs)]).await;

    let outcome = fixture
        .executor
        .execute(
            tenant.hash(),
            &request(
                "SELECT label(labels, '__name__') AS name, min(value), max(value) \
                 FROM samples GROUP BY series_id, name ORDER BY name",
            ),
        )
        .await
        .expect("grouped query");
    let rows = actual_rows_with_names(&outcome);

    let neg = rows.get("neg").expect("neg group");
    assert_eq!(
        neg[0],
        Cell::float(f64::NEG_INFINITY),
        "min over -Inf is -Inf: partial_cmp(-Inf, f64::MAX) is Less"
    );
    assert_eq!(
        neg[1],
        Cell::float(f64::MIN),
        "max over -Inf leaks the f64::MIN seed"
    );

    let pos = rows.get("pos").expect("pos group");
    assert_eq!(
        pos[0],
        Cell::float(f64::MAX),
        "min over +Inf leaks the f64::MAX seed"
    );
    assert_eq!(
        pos[1],
        Cell::float(f64::INFINITY),
        "max over +Inf is +Inf: partial_cmp(+Inf, f64::MIN) is Greater"
    );
}

/// `sum` with non-finite input. The properties pinned here are the ones that
/// are well defined regardless of arrow's lane order: an infinite sum keeps
/// its sign, mixed infinities produce NaN, and a NaN input produces NaN. The
/// NaN *payload* is deliberately not pinned, because which lane sees the NaN
/// first is architecture-dependent (module docs).
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
/// naming the workaround (plan section 2 "Exactness", review F7).
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
    assert!(message.contains("AVG"), "{message}");
    assert!(message.contains("SUM"), "{message}");
    assert!(message.contains("COUNT"), "{message}");
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

/// Helper for the grouped-infinity golden case: index result rows by their
/// first column, a metric name string.
fn actual_rows_with_names(
    outcome: &ravel_sql::SqlOutcome,
) -> std::collections::HashMap<String, Vec<Cell>> {
    let mut out = std::collections::HashMap::new();
    for batch in outcome.output.batches() {
        let names = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name column");
        for row in 0..batch.num_rows() {
            let cells = actual_row(batch, row);
            out.insert(names.value(row).to_string(), cells[1..].to_vec());
        }
    }
    out
}
