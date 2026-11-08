//! Issue #3985 (ADR-3985): the first-groups bound under an unordered
//! `GROUP BY ... LIMIT`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::MemoryPool;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use ravel_catalog::Snapshot;
use ravel_object_store::memory::MemoryStore;
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    LogsTableProvider, SessionPrincipal, SessionTable, SpillDecision, SqlConfig,
    TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

fn empty_logs() -> SessionTable {
    SessionTable::Logs(Arc::new(LogsTableProvider::new(
        Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TenantHash([0u8; 16]),
        LogSegmentFetcher::new(Arc::new(MemoryStore::new())),
        PhaseAccounting::new(),
    )))
}

/// The production session over `pool`, with `parts` registered as `t`.
fn session(config: &SqlConfig, pool: Arc<dyn MemoryPool>, parts: Vec<Vec<RecordBatch>>) -> SessionContext {
    let ctx = build_session(
        config,
        pool,
        empty_logs(),
        true,
        SpillDecision::Disabled,
        &SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
    )
    .expect("session builds");
    let schema = parts[0][0].schema();
    let table = MemTable::try_new(schema, parts).expect("mem table builds");
    ctx.register_table("t", Arc::new(table))
        .expect("table registers");
    ctx
}

async fn plan_of(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
    ctx.sql(sql)
        .await
        .expect("statement plans")
        .create_physical_plan()
        .await
        .expect("physical plan builds")
}

fn render(plan: &Arc<dyn ExecutionPlan>) -> String {
    format!("{}", displayable(plan.as_ref()).indent(false))
}

fn two_key_parts() -> Vec<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("k1", DataType::Int64, false),
        Field::new("k2", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    (0..4)
        .map(|p| {
            let rows: Vec<i64> = (0..1_000).map(|i| i * 4 + p).collect();
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r % 97))),
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r % 89))),
                Arc::new(Int64Array::from_iter_values(rows.iter().copied())),
            ];
            vec![RecordBatch::try_new(Arc::clone(&schema), columns).expect("batch builds")]
        })
        .collect()
}

#[tokio::test]
async fn explore_plans() {
    let config = SqlConfig::default();
    let (pool, _breach) = config.query_pool(TenantMemoryAccountant::new(1 << 30), QueryAccounting::new());
    let ctx = session(&config, pool, two_key_parts());
    for sql in [
        "SELECT k1, k2, count(*) FROM t GROUP BY k1, k2 LIMIT 10",
        "SELECT k1, k2, count(*) FROM t GROUP BY k1, k2 OFFSET 5 LIMIT 10",
        "SELECT k1, k2, count(*) FROM t GROUP BY k1, k2 ORDER BY count(*) LIMIT 10",
        "SELECT k1, k2, count(*) FROM t GROUP BY k1, k2 HAVING count(*) > 1 LIMIT 10",
        "SELECT k1, k2, count(*) FROM t GROUP BY GROUPING SETS ((k1), (k2)) LIMIT 10",
        "SELECT k1 FROM t GROUP BY k1 LIMIT 10",
        "SELECT k1, k2, count(*) FROM t GROUP BY k1, k2 LIMIT 2000",
    ] {
        let plan = plan_of(&ctx, sql).await;
        println!("{sql}\n{}", render(&plan));
    }
}
