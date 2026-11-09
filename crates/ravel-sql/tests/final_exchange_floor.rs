//! Issue #4014: the first-attempt partition floor widens only the hash
//! exchange under a final grouped aggregate (ADR-1195 amendment of
//! 2026-11-09). These tests plan and run statements through the real executor
//! on an 8-partition session:
//!
//! - `a_distinct_count_widens_both_final_exchanges_and_keeps_the_scan_count`:
//!   ClickBench statement 9's shape (a grouped `COUNT(DISTINCT)` under a top-k)
//!   plans two final aggregates; both exchanges below them read 16 partitions,
//!   while the scan and the inner partial aggregate keep the session's 8.
//! - `a_grouped_statement_answers_the_same_at_floor_0_and_16`: several
//!   aggregates over the same groups return the same rows with the floor off
//!   and on.
//!
//! The logs fast path keeps its derived count with the floor on in
//! `logs_limit_hint.rs`, which runs on the default config.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use crate::util;

use ravel_object_store::memory::MemoryStore;
use ravel_query::EngineConfig;
use ravel_sql::SqlConfig;
use ravel_types::TenantId;
use std::sync::Arc;

use util::gate::{self, Row};
use util::{Fixture, SegSpec, SeriesSpec, request};

fn tenant() -> TenantId {
    util::tenant_id("issue-4014-final-exchange-floor")
}

/// Forty metrics over three segments with distinct provenance, each sample on
/// its own `ts`, so a group's distinct `ts` count is its sample count.
fn dataset() -> Vec<SegSpec> {
    (0..3_i64)
        .map(|segment| {
            let series = (0..40_i64)
                .map(|metric| {
                    let samples = (0..=metric % 5)
                        .map(|i| (segment * 1_000 + metric * 10 + i, (metric * 7 + i) as f64))
                        .collect();
                    SeriesSpec::new(&format!("m{metric:02}"), samples)
                })
                .collect();
            let seq = u64::try_from(segment + 1).expect("small");
            SegSpec::new(10 * (segment + 1), 1, seq, series)
        })
        .collect()
}

/// A fixture over [`dataset`] on an 8-partition session with aggregate
/// repartitioning armed (ADR-0094) and the given floor.
async fn fixture(floor: usize) -> Fixture {
    let config = SqlConfig {
        engine: EngineConfig {
            fetch_concurrency: 8,
            ..EngineConfig::default()
        },
        parallel_final_aggregation: true,
        first_attempt_min_partitions: floor,
        ..SqlConfig::default()
    };
    let t = tenant();
    let specs = dataset();
    Fixture::build(
        Arc::new(MemoryStore::new()),
        &[(&t, &specs[..])],
        config,
        1 << 30,
    )
    .await
}

async fn plan_text(fixture: &Fixture, sql: &str) -> String {
    fixture
        .executor
        .explain(tenant().hash(), &request(sql))
        .await
        .expect("statement explains")
        .plan_text
}

/// Every hash `RepartitionExec` line of `plan`.
fn hash_exchanges(plan: &str) -> Vec<&str> {
    plan.lines()
        .filter(|l| l.contains("RepartitionExec: partitioning=Hash("))
        .collect()
}

/// ClickBench statement 9's shape over `samples`.
const DISTINCT_TOP_SQL: &str = "SELECT label(labels,'__name__') AS metric, \
     COUNT(DISTINCT ts) AS u FROM samples GROUP BY metric ORDER BY u DESC LIMIT 10";

/// Statement 9's shape plans a `FinalPartitioned` aggregate for the distinct
/// keys and, above it, a partial and a final aggregate for the outer count.
/// At floor 16 on an 8-partition session both exchanges widen to 16. The
/// scan's round-robin exchange and the inner partial aggregate keep the
/// session's 8, so the inner exchange reads `16), input_partitions=8`; the
/// outer partial aggregate runs on the widened inner final's 16 streams, so
/// the outer exchange reads `16), input_partitions=16`. At floor 0 both read
/// `8), input_partitions=8`.
///
/// FLIP: raise the session's `target_partitions` to the floor instead of
/// installing the rule, and the scan's exchange reads `RoundRobinBatch(16)`
/// and the inner exchange `input_partitions=16`.
#[tokio::test]
async fn a_distinct_count_widens_both_final_exchanges_and_keeps_the_scan_count() {
    for (floor, inner, outer) in [
        (16, "16), input_partitions=8", "16), input_partitions=16"),
        (0, "8), input_partitions=8", "8), input_partitions=8"),
    ] {
        let plan = plan_text(&fixture(floor).await, DISTINCT_TOP_SQL).await;
        assert_eq!(
            plan.matches("AggregateExec: mode=FinalPartitioned").count(),
            2,
            "floor {floor}: two final aggregates:\n{plan}"
        );
        assert!(
            plan.contains("RepartitionExec: partitioning=RoundRobinBatch(8)"),
            "floor {floor}: the scan fans out to the session's 8:\n{plan}"
        );
        // The plan is printed top-down, so the outer exchange comes first.
        let exchanges = hash_exchanges(&plan);
        assert_eq!(exchanges.len(), 2, "floor {floor}:\n{plan}");
        assert!(exchanges[0].ends_with(outer), "floor {floor}:\n{plan}");
        assert!(exchanges[1].ends_with(inner), "floor {floor}:\n{plan}");
    }
}

/// Group key, an exact integer sum, a count, the `ts` range and a distinct
/// count, all exact-typed so the final aggregation repartitions.
const MULTI_AGGREGATE_SQL: &str = "SELECT label(labels,'__name__') AS metric, \
     sum(CAST(value AS BIGINT)) AS s, count(*) AS c, min(ts) AS lo, max(ts) AS hi, \
     count(DISTINCT CAST(value AS BIGINT)) AS dv \
     FROM samples GROUP BY metric";

async fn sorted_rows(fixture: &Fixture, sql: &str) -> Vec<Row> {
    let outcome = fixture
        .executor
        .execute(tenant().hash(), &request(sql))
        .await
        .expect("statement executes");
    let mut rows = gate::actual_rows(&outcome.output);
    rows.sort_by_key(|row| format!("{:?}", row[0]));
    rows
}

/// The same rows, bit for bit, with the floor off and at 16, over a plan whose
/// exchange the floor really widened.
#[tokio::test]
async fn a_grouped_statement_answers_the_same_at_floor_0_and_16() {
    let off = fixture(0).await;
    let on = fixture(16).await;
    let plan = plan_text(&on, MULTI_AGGREGATE_SQL).await;
    let exchanges = hash_exchanges(&plan);
    assert!(
        !exchanges.is_empty() && exchanges.iter().all(|l| l.contains(", 16), ")),
        "the floored plan widens its exchanges:\n{plan}"
    );

    let expected = sorted_rows(&off, MULTI_AGGREGATE_SQL).await;
    assert_eq!(expected.len(), 40, "one row per metric");
    assert_eq!(sorted_rows(&on, MULTI_AGGREGATE_SQL).await, expected);
}
