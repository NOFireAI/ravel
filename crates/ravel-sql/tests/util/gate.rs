//! The v1 SQL subset's differential grammar, shared between the HTTP-path
//! oracle gate (tests/differential.rs) and the Flight-vs-HTTP transport parity
//! gate (tests/flight_differential.rs, issue #153).
//!
//! Everything here is transport-agnostic: the query grammar ([`Pred`],
//! [`Shape`], [`Query`]), the proptest dataset strategies, the reduced
//! comparable [`Cell`]/[`Row`] form, and the independent reference aggregate
//! folds. tests/differential.rs evaluates [`Query::eval`] against the
//! independent [`super::reference_rows`] oracle; tests/flight_differential.rs
//! runs [`Query::sql`] through both transports and compares [`rows_from_batches`]
//! output bit for bit. Sharing this one grammar is what makes "the same
//! datasets and queries" a fact rather than a claim: neither gate can drift
//! from the other's notion of the subset.
//!
//! The reference semantics (total-order comparisons, total-order MIN/MAX, the
//! exactly-representable restriction on ungrouped `sum`) are documented in full
//! in tests/differential.rs; this module carries the executable form.

#![allow(dead_code)]

use std::cmp::Ordering;
use std::collections::BTreeMap;

use datafusion::arrow::array::{
    Array, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use proptest::prelude::*;
use ravel_promql::LabelMatcher;
use ravel_sql::QueryOutput;

use super::{RefRow, SegSpec, SeriesSpec};

// ---------------------------------------------------------------------------
// Comparable cells
// ---------------------------------------------------------------------------

/// One result cell, reduced to something comparable bit-for-bit. Floats are
/// held as `to_bits`, never as `f64`, so NaN payloads and the sign of zero
/// are part of the comparison instead of being erased by `==`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cell {
    Null,
    Int(i64),
    FloatBits(u64),
    Bytes(Vec<u8>),
    Text(String),
}

impl Cell {
    pub fn float(v: f64) -> Cell {
        Cell::FloatBits(v.to_bits())
    }
}

pub type Row = Vec<Cell>;

/// Reduce a query's batches to comparable rows, driven by the output schema so
/// any column type the v1 subset can produce is handled.
pub fn rows_from_batches(batches: &[RecordBatch]) -> Vec<Row> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            rows.push(actual_row(batch, row));
        }
    }
    rows
}

/// [`rows_from_batches`] over a [`QueryOutput`] (the HTTP path's return type).
pub fn actual_rows(output: &QueryOutput) -> Vec<Row> {
    rows_from_batches(output.batches())
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
/// widen-only pushdown extractor recognizes, which makes every proptest case an
/// adversarial no-pushdown case.
#[derive(Clone, Debug)]
pub enum Pred {
    True,
    TsGe(i64),
    TsLt(i64),
    TsBetween(i64, i64),
    ValueGt(f64),
    /// `label(labels, '__name__') != 'v'`, pushed into the fetcher's
    /// SERIES_TABLE prune as a negation matcher.
    LabelNe(&'static str),
    /// `label_match(labels, '__name__', 'pat')`, an anchored `^(?:pat)$` regex
    /// matcher pushed into the same prune.
    LabelMatch(&'static str),
    And(Box<Pred>, Box<Pred>),
    Or(Box<Pred>, Box<Pred>),
}

impl Pred {
    pub fn sql(&self) -> String {
        match self {
            Pred::True => "1 = 1".to_string(),
            Pred::TsGe(l) => format!("CAST(ts AS BIGINT) >= {l}"),
            Pred::TsLt(u) => format!("CAST(ts AS BIGINT) < {u}"),
            Pred::TsBetween(l, u) => format!("CAST(ts AS BIGINT) BETWEEN {l} AND {u}"),
            Pred::ValueGt(c) => format!("value > {c:?}"),
            Pred::LabelNe(v) => format!("label(labels, '__name__') != '{v}'"),
            Pred::LabelMatch(p) => format!("label_match(labels, '__name__', '{p}')"),
            Pred::And(a, b) => format!("({}) AND ({})", a.sql(), b.sql()),
            Pred::Or(a, b) => format!("({}) OR ({})", a.sql(), b.sql()),
        }
    }

    /// Evaluate against one reference row.
    ///
    /// `value > c` uses `total_cmp`, matching arrow's compare kernels: NaN is
    /// greater than every finite value and `-0.0` is strictly less than
    /// `0.0`. Rust's own `>` would be wrong here.
    pub fn eval(&self, row: &RefRow) -> bool {
        match self {
            Pred::True => true,
            Pred::TsGe(l) => row.ts >= *l,
            Pred::TsLt(u) => row.ts < *u,
            Pred::TsBetween(l, u) => row.ts >= *l && row.ts <= *u,
            Pred::ValueGt(c) => row.value.total_cmp(c) == Ordering::Greater,
            Pred::LabelNe(v) => match row.labels.get("__name__") {
                Some(name) => name != *v,
                None => false,
            },
            Pred::LabelMatch(p) => LabelMatcher::regex("__name__", *p)
                .expect("valid test regex")
                .is_match(&row.labels),
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
pub fn min_total_order(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(|a, b| {
        if b.total_cmp(&a) == Ordering::Less {
            b
        } else {
            a
        }
    })
}

/// Ungrouped `max`, same construction.
pub fn max_total_order(values: &[f64]) -> Option<f64> {
    values.iter().copied().reduce(|a, b| {
        if b.total_cmp(&a) == Ordering::Greater {
            b
        } else {
            a
        }
    })
}

/// `sum`: a sequential fold from `0.0` in input order. Exact for the grouped
/// accumulator by construction, and exact for the ungrouped arrow kernel
/// whenever every partial sum is exactly representable.
pub fn sum_sequential(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().fold(0.0f64, |acc, v| acc + v))
}

/// `avg`: the sequential-fold UDAF's reference (crate::avg, ADR-0022 decisions
/// 3, 4). The numerator is a plain f64 left fold *seeded with the first value*
/// (not a `0.0` seed, so all-`-0.0` folds to `-0.0`) over the non-null values
/// in input order, divided by the non-null count in one IEEE division. Empty
/// input is SQL NULL; a zero count never yields NaN or infinity. This is
/// independent of [`sum_sequential`]: it owns its own numerator fold and does
/// not divide `sum_sequential` by a count.
pub fn avg_sequential(values: &[f64]) -> Option<f64> {
    let mut it = values.iter().copied();
    let first = it.next()?;
    let numerator = it.fold(first, |acc, v| acc + v);
    Some(numerator / values.len() as f64)
}

// ---------------------------------------------------------------------------
// The reference executor
// ---------------------------------------------------------------------------

/// The query shapes the v1 subset covers.
#[derive(Clone, Debug)]
pub enum Shape {
    /// `SELECT ts, value ... ORDER BY series_id, ts [LIMIT n]`
    Projection { limit: Option<usize> },
    /// `SELECT count(value), min(value), max(value) ...`
    SelectionAggregates,
    /// `SELECT series_id, count(value), min(value), max(value) ... GROUP BY
    /// series_id ORDER BY series_id`.
    GroupedSelectionAggregates,
    /// `SELECT sum(value) ...`
    Sum,
    /// `SELECT series_id, sum(value) ... GROUP BY series_id ORDER BY series_id`
    GroupedSum,
    /// `SELECT avg(value) ...`. The sequential-fold UDAF (crate::avg) takes the
    /// full float pool, ungrouped included, because it is bit-identical to
    /// [`avg_sequential`] rather than to arrow's lane-parallel batch sum.
    Avg,
    /// `SELECT series_id, avg(value) ... GROUP BY series_id ORDER BY series_id`
    GroupedAvg,
}

#[derive(Clone, Debug)]
pub struct Query {
    pub shape: Shape,
    pub pred: Pred,
}

impl Query {
    pub fn sql(&self) -> String {
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
            Shape::Avg => format!("SELECT avg(value) FROM samples {where_clause}"),
            Shape::GroupedAvg => format!(
                "SELECT series_id, avg(value) FROM samples {where_clause} \
                 GROUP BY series_id ORDER BY series_id"
            ),
        }
    }

    /// Evaluate over the independent reference rows, which arrive already in
    /// the canonical `(series_id, ts)` dedup order.
    pub fn eval(&self, rows: &[RefRow]) -> Vec<Row> {
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
                        optional_float(min_total_order(&values)),
                        optional_float(max_total_order(&values)),
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
            Shape::Avg => {
                let values: Vec<f64> = kept.iter().map(|r| r.value).collect();
                vec![vec![optional_float(avg_sequential(&values))]]
            }
            Shape::GroupedAvg => group_by_series(&kept)
                .into_iter()
                .map(|(sid, values)| {
                    vec![
                        Cell::Bytes(sid.to_vec()),
                        optional_float(avg_sequential(&values)),
                    ]
                })
                .collect(),
        }
    }
}

pub fn optional_float(value: Option<f64>) -> Cell {
    value.map_or(Cell::Null, Cell::float)
}

/// Group the kept rows by series id, preserving `(series_id, ts)` order within
/// each group. `BTreeMap` also gives the `ORDER BY series_id` output order.
pub fn group_by_series(rows: &[&RefRow]) -> Vec<([u8; 16], Vec<f64>)> {
    let mut groups: BTreeMap<[u8; 16], Vec<f64>> = BTreeMap::new();
    for row in rows {
        groups.entry(row.series_id).or_default().push(row.value);
    }
    groups.into_iter().collect()
}

// ---------------------------------------------------------------------------
// Value pools and dataset strategies
// ---------------------------------------------------------------------------

/// A value pool, named by constructor so the dataset strategies can be
/// parameterized by it (a `BoxedStrategy` is not `Clone`, a `fn` pointer is).
pub type ValuePool = fn() -> BoxedStrategy<f64>;

/// The full float edge-case pool: NaN with distinct payloads (both signs),
/// both infinities, both zeros, a denormal, and ordinary values.
pub fn interesting_value() -> BoxedStrategy<f64> {
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
/// bits.
pub fn exact_sum_value() -> BoxedStrategy<f64> {
    prop_oneof![
        Just(0.0f64),
        Just(-0.0f64),
        (-64i64..64).prop_map(|n| n as f64),
        (-8i64..8).prop_map(|n| (n * 1024) as f64),
    ]
    .boxed()
}

pub fn arb_series(value: ValuePool) -> impl Strategy<Value = SeriesSpec> {
    let metric = prop_oneof![Just("a"), Just("b"), Just("c")].prop_map(|s| s.to_string());
    // A small ts range forces duplicate timestamps within a segment and
    // overlap across segments.
    let samples = prop::collection::vec((0i64..6, value()), 1..8);
    (metric, samples).prop_map(|(metric, samples)| SeriesSpec::new(&metric, samples))
}

pub fn arb_segment(value: ValuePool) -> impl Strategy<Value = SegSpec> {
    let series = prop::collection::vec(arb_series(value), 1..4);
    (0i64..4, 1u64..3, 1u64..3, series).prop_map(
        |(created_unix_ns, writer_epoch, writer_seq, series)| {
            SegSpec::new(created_unix_ns, writer_epoch, writer_seq, series)
        },
    )
}

pub fn arb_dataset(value: ValuePool) -> impl Strategy<Value = Vec<SegSpec>> {
    prop::collection::vec(arb_segment(value), 1..4).prop_map(|specs| {
        // The segment writer rejects duplicate series ids inside one segment;
        // collapse duplicate metrics per segment. Cross-segment duplicates are
        // the point and are kept.
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

/// A dataset that collapses to exactly one series (one group): every segment
/// carries a single series under the same metric, drawn from the full float
/// edge-case pool.
pub fn arb_single_group_dataset() -> impl Strategy<Value = Vec<SegSpec>> {
    let samples = prop::collection::vec((0i64..6, interesting_value()), 1..8);
    let segment = (0i64..4, 1u64..3, 1u64..3, samples).prop_map(
        |(created_unix_ns, writer_epoch, writer_seq, samples)| {
            SegSpec::new(
                created_unix_ns,
                writer_epoch,
                writer_seq,
                vec![SeriesSpec::new("solo", samples)],
            )
        },
    );
    prop::collection::vec(segment, 1..4)
}

pub fn arb_pred() -> impl Strategy<Value = Pred> {
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
        // Label predicates over `__name__` (the datasets' metric is a/b/c),
        // exercising the negation and regex matcher pushdown.
        prop_oneof![
            Just(Pred::LabelNe("a")),
            Just(Pred::LabelNe("b")),
            Just(Pred::LabelNe("z")),
            Just(Pred::LabelMatch("a")),
            Just(Pred::LabelMatch("a|b")),
            Just(Pred::LabelMatch("b.*")),
            Just(Pred::LabelMatch(".*")),
        ],
    ];
    leaf.prop_recursive(2, 6, 2, |inner| {
        prop_oneof![
            (inner.clone(), inner.clone()).prop_map(|(a, b)| Pred::And(Box::new(a), Box::new(b))),
            (inner.clone(), inner).prop_map(|(a, b)| Pred::Or(Box::new(a), Box::new(b))),
        ]
    })
}

/// Drive an async future to completion on a fresh current-thread runtime, the
/// shape proptest cases need (proptest bodies are synchronous).
pub fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(fut)
}
