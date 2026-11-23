//! Issue #4660 investigation scaffold.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use ravel_logseg::RlogConfig;
use ravel_object_store::memory::MemoryStore;
use ravel_sql::{
    CeilingBreach, DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SpillDecision,
    SqlConfig, TenantDelegatingPool, TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

use crate::logs_str_not_equal_row_filter::{Stream, encode, provider, put, record, run, s};

fn session(provider: LogsTableProvider) -> SessionContext {
    let pool = Arc::new(TenantDelegatingPool::new(
        1 << 30,
        TenantMemoryAccountant::new(1 << 30),
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));
    build_session(
        &SqlConfig::default(),
        pool,
        SessionTable::Logs(Arc::new(provider)),
        false,
        SpillDecision::Disabled,
        &ravel_sql::SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
    )
    .expect("session")
}

#[tokio::test]
async fn investigate_plans() {
    let store = Arc::new(MemoryStore::new());
    let rows = vec![
        record(100, Stream::Bare, &[("s", s("a"))]),
        record(101, Stream::Bare, &[("s", s(""))]),
        record(102, Stream::Bare, &[]),
    ];
    let bytes = encode(&rows, RlogConfig::default());
    let segments = vec![put(&store, "logs/a.rlog", bytes, &rows).await];
    let declared = vec![DeclaredColumn::new("s", DeclaredType::Str)];
    let ctx = session(provider(&store, segments, &declared));
    for sql in [
        "SELECT s FROM logs WHERE s <> '' ORDER BY s LIMIT 10",
        "SELECT s FROM logs WHERE s = '' ORDER BY s LIMIT 10",
        "SELECT ts FROM logs WHERE s = ''",
    ] {
        let (plan, _) = run(&ctx, sql).await;
        println!("== {sql}\n{}", displayable(plan.as_ref()).indent(true));
        let df = ctx.sql(&format!("EXPLAIN {sql}")).await.expect("explain");
        let batches = df.collect().await.expect("collect");
        println!("{}", crate::logs_str_not_equal_row_filter::pretty(&batches));
    }
}
