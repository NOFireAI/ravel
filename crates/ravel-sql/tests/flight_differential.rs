//! Differential gate parity between the Flight SQL and HTTP transports
//! (issue #153, docs/arrow-datafusion-plan.md Phase C "differential gate
//! results identical over Flight SQL and HTTP SQL").
//!
//! tests/differential.rs gates the HTTP path against an independent
//! greatest-wins oracle. This file gates the *Flight* path against the *HTTP*
//! path: it draws the same proptest datasets and the same query grammar from
//! [`util::gate`] -- the very ones differential.rs runs -- and asserts that
//! `GetFlightInfo` + `DoGet` returns f64-bit-identical rows to
//! `SqlExecutor::execute` on the same snapshot. Because the HTTP path is
//! already pinned to the oracle by differential.rs, transitivity ties the
//! Flight path to the oracle too, without re-deriving it here.
//!
//! What makes "the same snapshot" true: nothing commits between the two runs,
//! so the Flight ticket's pinned segment set and the HTTP path's fresh resolve
//! see the identical committed set. What makes the comparison independent of
//! chunking or transport framing: both sides are reduced to
//! [`util::gate::Row`] and compared as whole result sets, and every query
//! carries a total `ORDER BY` so row order is itself part of the contract.
//!
//! Bit-for-bit is the whole point (review F7): the Flight encoder round-trips
//! the Arrow IPC frames a NaN payload or a signed zero lives in, and a lossy
//! step anywhere on the DoGet path -- the ticket codec, the pinned re-plan, the
//! stream encoder -- would surface here as a bit mismatch, not a silent
//! rounding the oracle gate could never see because it never crossed the wire.

#![cfg(feature = "flight-sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

mod util;

use proptest::prelude::*;
use ravel_types::TenantId;
use util::flight_harness::Harness;
use util::gate::{
    Pred, Query, Row, Shape, ValuePool, actual_rows, arb_dataset, arb_pred,
    arb_single_group_dataset, block_on, exact_sum_value, interesting_value, rows_from_batches,
};
use util::{SegSpec, SeriesSpec, request, tenant_id};

/// The token the harness's [`util::flight_harness::TestAuth`] maps back to
/// `tenant`: it is the tenant's own name.
const TENANT: &str = "gate";

/// Run one query through both transports on `harness` and assert the two
/// result sets are bit-identical.
async fn assert_transports_agree(harness: &Harness, tenant: &TenantId, query: &Query) {
    let sql = query.sql();

    let http = harness
        .executor
        .execute(tenant.hash(), &request(&sql))
        .await
        .unwrap_or_else(|e| panic!("http execute failed: {sql}\n{e}"));
    let http_rows: Vec<Row> = actual_rows(&http.output);

    let ticket = harness
        .get_flight_info(TENANT, &sql)
        .await
        .unwrap_or_else(|e| panic!("GetFlightInfo failed: {sql}\n{e}"));
    let flight = harness
        .do_get(TENANT, &ticket)
        .await
        .unwrap_or_else(|e| panic!("DoGet failed: {sql}\n{e}"));
    let flight_rows: Vec<Row> = rows_from_batches(&flight);

    assert_eq!(
        flight_rows.len(),
        http_rows.len(),
        "row count differs between transports for: {sql}\n\
         flight {flight_rows:?}\nhttp   {http_rows:?}"
    );
    for (i, (f, h)) in flight_rows.iter().zip(http_rows.iter()).enumerate() {
        assert_eq!(
            f, h,
            "row {i} differs between transports for: {sql}\n\
             flight {flight_rows:?}\nhttp   {http_rows:?}"
        );
    }
}

async fn assert_all(harness: &Harness, tenant: &TenantId, queries: &[Query]) {
    for query in queries {
        assert_transports_agree(harness, tenant, query).await;
    }
}

proptest! {
    // Fewer cases than the oracle gate: each query here is three RPCs
    // (execute, GetFlightInfo, DoGet) plus a fresh service/executor per
    // dataset, so the per-case cost is higher. The grammar and datasets are
    // identical to differential.rs, so coverage of the *subset* is the same;
    // only the sampled count of that subset is smaller.
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Projection, filter, count, min/max, and grouped sum over the full float
    /// edge-case pool: NaN payloads (both signs), infinities, -0.0, denormals,
    /// duplicate timestamps, multi-segment overlap -- every value the IPC frame
    /// must carry across the wire unchanged.
    #[test]
    fn projection_filter_count_and_min_max_agree_across_transports(
        specs in arb_dataset(interesting_value as ValuePool),
        pred in arb_pred(),
        limit in prop::option::of(1usize..8),
    ) {
        block_on(async move {
            let tenant = tenant_id(TENANT);
            let harness = Harness::memory(&[(&tenant, &specs)]).await;
            let queries = vec![
                Query { shape: Shape::Projection { limit }, pred: pred.clone() },
                Query { shape: Shape::Projection { limit: None }, pred: pred.clone() },
                Query { shape: Shape::SelectionAggregates, pred: pred.clone() },
                Query { shape: Shape::GroupedSelectionAggregates, pred: pred.clone() },
                Query { shape: Shape::GroupedSum, pred },
            ];
            assert_all(&harness, &tenant, &queries).await;
        });
    }

    /// Ungrouped and grouped `sum` over exactly-representable values, where the
    /// arrow accumulation is deterministic; both transports run the identical
    /// engine, so any disagreement is a transport bug, not a float-order one.
    #[test]
    fn sum_agrees_across_transports(
        specs in arb_dataset(exact_sum_value as ValuePool),
        pred in arb_pred(),
    ) {
        block_on(async move {
            let tenant = tenant_id(TENANT);
            let harness = Harness::memory(&[(&tenant, &specs)]).await;
            let queries = vec![
                Query { shape: Shape::Sum, pred: pred.clone() },
                Query { shape: Shape::GroupedSum, pred: pred.clone() },
                Query { shape: Shape::SelectionAggregates, pred },
            ];
            assert_all(&harness, &tenant, &queries).await;
        });
    }

    /// Single-group datasets over the full float pool, exercising grouped and
    /// ungrouped MIN/MAX across the wire.
    #[test]
    fn single_group_min_max_agrees_across_transports(
        specs in arb_single_group_dataset(),
    ) {
        block_on(async move {
            let tenant = tenant_id(TENANT);
            let harness = Harness::memory(&[(&tenant, &specs)]).await;
            let queries = vec![
                Query { shape: Shape::GroupedSelectionAggregates, pred: Pred::True },
                Query { shape: Shape::SelectionAggregates, pred: Pred::True },
            ];
            assert_all(&harness, &tenant, &queries).await;
        });
    }
}

/// A fixed multi-segment overlap dataset with duplicate timestamps, run
/// deterministically (no proptest) so a transport regression has a stable
/// repro independent of a sampled seed.
#[tokio::test]
async fn golden_multi_segment_overlap_agrees_across_transports() {
    let tenant = tenant_id(TENANT);
    let specs = vec![
        SegSpec::new(
            10,
            1,
            1,
            vec![
                SeriesSpec::new("a", vec![(1, 1.0), (2, 2.0), (2, 20.0)]),
                SeriesSpec::new(
                    "b",
                    vec![(1, f64::from_bits(0x7ff8_0000_0000_0001)), (3, -0.0)],
                ),
            ],
        ),
        SegSpec::new(
            20,
            1,
            1,
            vec![SeriesSpec::new(
                "a",
                vec![(2, 200.0), (4, f64::NEG_INFINITY)],
            )],
        ),
    ];
    let harness = Harness::memory(&[(&tenant, &specs)]).await;

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
    assert_all(&harness, &tenant, &queries).await;
}
