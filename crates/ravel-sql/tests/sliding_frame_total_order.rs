//! Moving-frame `min`/`max` float-safety (ADR-0097 decision 5).
//!
//! A moving window frame (any frame whose start is not UNBOUNDED PRECEDING)
//! routes an aggregate-over-window through DataFusion's *sliding* accumulator,
//! not Ravel's `TotalOrderAccumulator`: `min(value) OVER (ORDER BY ts ROWS
//! BETWEEN 2 PRECEDING AND CURRENT ROW)` runs upstream's `MovingMin`/`MovingMax`
//! (`crate::minmax::create_sliding_accumulator` delegates to the built-in with
//! no float guard). That is safe only because `ScalarValue`'s `PartialOrd` is
//! `f64::total_cmp` for float types, the same total order ADR-0023 mandates.
//! Nothing else in this workspace enters that path, so this test pins the
//! guarantee against an upstream comparator change.
//!
//! The routing fact -- that a moving frame constructs the sliding accumulator
//! while an UNBOUNDED PRECEDING frame constructs Ravel's own -- is established
//! directly by `crate::minmax`'s `moving_frame_routes_to_the_sliding_accumulator`
//! unit test, which counts each constructor call. This suite exercises that
//! established path end to end through `SqlExecutor::execute` over a real RSEG
//! fixture and asserts every moving frame's result is bit-identical to a total-
//! order reference computed over that frame's own rows.

#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use std::sync::Arc;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use util::gate::{Cell, actual_rows, max_total_order, min_total_order};
use util::{Fixture, SegSpec, SeriesSpec, request, tenant_id};

/// A quiet NaN carrying a non-default payload (ADR-0022 decision 1's adversarial
/// pool). A `==` comparison can never see this survive a fold; only a
/// bit-pattern comparison can, and only a total order keeps it as the extreme.
const NAN_PAYLOAD: f64 = f64::from_bits(0x7ff8_0000_dead_beef);

/// The adversarial value pool, in `ts` order (the sliding frame's own order).
///
/// Laid out so a width-3 moving frame slides across every hazard ADR-0022
/// decision 1 names: `+0.0` ordered before `-0.0` (indices 1, 2), a NaN with a
/// non-default payload that a later frame must *retract* without poisoning the
/// deque (index 3, gone by index 6), an all-`+inf` frame (indices 5-7), an
/// all-`-inf` frame (indices 8-10), and ordinary finite values throughout.
fn pool() -> Vec<f64> {
    vec![
        5.0,               // 0
        0.0,               // 1  (+0.0, ordered before the -0.0 below)
        -0.0,              // 2
        NAN_PAYLOAD,       // 3
        2.0,               // 4
        f64::INFINITY,     // 5
        f64::INFINITY,     // 6
        f64::INFINITY,     // 7  frame {5,6,7} is all +inf
        f64::NEG_INFINITY, // 8
        f64::NEG_INFINITY, // 9
        f64::NEG_INFINITY, // 10 frame {8,9,10} is all -inf
        -7.5,              // 11
        3.0,               // 12
    ]
}

/// Build a single-series, single-segment metrics fixture whose one series
/// carries [`pool`] at `ts = 1..=n`, so the row order the SQL window sees
/// (sorted by `ts`) is exactly the pool order.
async fn pool_fixture() -> Fixture {
    let tenant = tenant_id("sliding-frame");
    let samples: Vec<(i64, f64)> = pool()
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i as i64 + 1, v))
        .collect();
    let spec = SegSpec::new(10, 1, 1, vec![SeriesSpec::new("m", samples)]);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    Fixture::build(
        store,
        &[(&tenant, &[spec])],
        ravel_sql::SqlConfig::default(),
        1 << 30,
    )
    .await
}

/// The row indices the frame `[i - start_off, i - end_off]` covers for output
/// row `i`, clamped to the rows that exist at or before `i`. Empty when the
/// whole range is before row 0.
fn frame_indices(i: usize, start_off: usize, end_off: usize) -> Vec<usize> {
    // `hi` is `i - end_off`; if `end_off > i` the whole frame is before row 0.
    let Some(hi) = i.checked_sub(end_off) else {
        return Vec::new();
    };
    let lo = i.saturating_sub(start_off);
    (lo..=hi).collect()
}

/// The reference cell for one frame: the total-order `min`/`max` over the
/// frame's own values as a bit-compared [`Cell`], or `Cell::Null` for an empty
/// frame (SQL `min`/`max` over no rows is NULL).
fn reference_cell(values: &[f64], indices: &[usize], want_max: bool) -> Cell {
    if indices.is_empty() {
        return Cell::Null;
    }
    let frame: Vec<f64> = indices.iter().map(|&j| values[j]).collect();
    let extreme = if want_max {
        max_total_order(&frame)
    } else {
        min_total_order(&frame)
    };
    match extreme {
        Some(v) => Cell::float(v),
        None => Cell::Null,
    }
}

/// The acceptance test (ADR-0097 decision 5): moving-frame `min`/`max` over the
/// adversarial pool, run through the real pipeline, matches a total-order
/// reference computed over each frame's own rows, bit for bit.
///
/// Two frame specifications are checked, both of which route to the sliding
/// accumulator (frame start is not UNBOUNDED PRECEDING; see the module-level
/// note and `crate::minmax`'s routing unit test):
///
/// - `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`: a width-3 moving frame that
///   slides across the pool, so every value enters and later *retracts* --
///   across the NaN (indices 3 -> gone by 6) and across `+0.0` (index 1 ->
///   gone by 4). A retract that poisoned the deque, or one that lost the sign
///   of zero or the NaN payload, diverges here.
/// - `ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING`: a frame that is empty for the
///   first two rows (SQL NULL) and holds two historical rows thereafter.
///
/// Every comparison is by `f64::to_bits` (via [`Cell`]), never `==`: `-0.0 ==
/// +0.0` is true and `NaN != NaN`, so an `==` comparison would pass on exactly
/// the inputs this test exists to catch.
#[tokio::test]
async fn moving_frame_minmax_matches_total_order_reference() {
    let fixture = pool_fixture().await;
    let tenant = tenant_id("sliding-frame");
    let values = pool();
    let n = values.len();

    // (frame SQL, start offset, end offset) for the moving and empty frames.
    let frames = [
        ("ROWS BETWEEN 2 PRECEDING AND CURRENT ROW", 2usize, 0usize),
        ("ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING", 3usize, 2usize),
    ];

    for (frame_sql, start_off, end_off) in frames {
        let sql = format!(
            "SELECT CAST(ts AS BIGINT) AS t, \
             min(value) OVER (ORDER BY ts {frame_sql}) AS mn, \
             max(value) OVER (ORDER BY ts {frame_sql}) AS mx \
             FROM samples ORDER BY t"
        );
        let outcome = fixture
            .executor
            .execute(tenant.hash(), &request(&sql))
            .await
            .unwrap_or_else(|e| panic!("moving-frame query must execute ({frame_sql}): {e}"));
        let rows = actual_rows(&outcome.output);
        assert_eq!(
            rows.len(),
            n,
            "one output row per input row for frame {frame_sql}"
        );

        for (i, row) in rows.iter().enumerate() {
            let indices = frame_indices(i, start_off, end_off);
            let want_mn = reference_cell(&values, &indices, false);
            let want_mx = reference_cell(&values, &indices, true);
            let expected = vec![Cell::Int(i as i64 + 1), want_mn, want_mx];
            assert_eq!(
                row,
                &expected,
                "frame {frame_sql} row {i} (ts {}) diverged from the total-order reference \
                 over frame rows {indices:?}",
                i + 1
            );
        }
    }
}
