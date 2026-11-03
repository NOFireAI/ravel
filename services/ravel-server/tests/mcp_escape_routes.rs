//! The SQL escape routes through `ravel_query_sql` on the shipping start path
//! (issue #1384, ADR-1374 decision 7): a statement that would reach outside
//! the tenant's tables (an external table, a file write, a file-reading table
//! function, the engine's own catalog, a session setting, a plan dump) is
//! refused before anything runs. "Nothing ran" is asserted four ways: the
//! call's exact object-store call count, no PUT or DELETE, an unchanged store
//! key listing, and a local path the statement names left uncreated.
//!
//! Issue #1384 asks for the `validation` class on every route. None of the
//! six answers it, and this file pins what each does answer:
//! - CREATE EXTERNAL TABLE, COPY, SET and EXPLAIN are `unsupported`:
//!   `validate_query` reports them as `NotReadOnly`, which ravel-mcp's
//!   `query_sql::validation_refusal` maps to `unsupported`, as ADR-1374's
//!   `ravel_query_sql` amendment ("a write or any other non-query statement
//!   is `unsupported`") specifies.
//! - `read_parquet(...)` passes `validate_query` and is refused by the
//!   executor's unreadable-table check as a planning failure: `unsupported`.
//! - `information_schema.tables` passes `validate_query` and is refused by
//!   the executor's Parquet resolve (`resolve_parquet_target`) as
//!   `invalid_argument`, because a name that is not a signal table is read
//!   as a Parquet table and the tool's required `time_range` cannot apply to
//!   one. That check, not the session's disabled `information_schema`,
//!   decides this route: turning the session flag on changes nothing here.
//! - SET is refused twice, by `validate_query` and again by the page
//!   planner's `parse_query`, in the same class; only `validate_query`'s
//!   message names the statement kind, so each route also pins its message.
//!
//! The issue to file: issue #1384's `validation` expectation contradicts
//! ADR-1374's amendment for the four non-query statements, and the two
//! `SELECT` routes answer refusal classes that name a planner reason rather
//! than the escape route.
//!
//! The audit pipeline is off here so that any store write would be the
//! statement's own; `mcp_audit.rs` covers the audit rows.

#![cfg(feature = "mcp")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

#[path = "support/mcp.rs"]
mod support;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Arc;

use ravel_object_store::{ObjectStoreBackend, StoreMetrics, list_all};
use serde_json::json;
use support::*;

/// One escape route.
struct Route {
    name: &'static str,
    /// The statement; `{path}` is replaced by a local path the statement
    /// would create or read if it ran.
    statement: &'static str,
    /// The failure class the route answers today (see the module doc).
    class: &'static str,
    /// Text the failure message carries, naming the check that refused it:
    /// two checks can answer the same class, and only one names the
    /// statement kind.
    message: &'static str,
    /// The object-store calls the refused call makes.
    store_calls: u64,
}

const ROUTES: [Route; 6] = [
    Route {
        name: "create_external_table",
        statement: "CREATE EXTERNAL TABLE escape STORED AS PARQUET LOCATION '{path}'",
        class: "unsupported",
        message: "CREATE EXTERNAL TABLE is not permitted",
        store_calls: 0,
    },
    Route {
        name: "copy",
        statement: "COPY (SELECT value FROM samples) TO '{path}'",
        class: "unsupported",
        message: "COPY is not permitted",
        store_calls: 0,
    },
    Route {
        name: "read_parquet",
        statement: "SELECT * FROM read_parquet('{path}')",
        class: "unsupported",
        message: "could not be planned",
        store_calls: 0,
    },
    Route {
        name: "information_schema",
        statement: "SELECT table_name FROM information_schema.tables",
        class: "invalid_argument",
        message: "a row window cannot be applied to a Parquet table",
        store_calls: 0,
    },
    Route {
        name: "set",
        statement: "SET datafusion.execution.batch_size = 1",
        class: "unsupported",
        message: "SET is not permitted",
        store_calls: 0,
    },
    Route {
        name: "explain",
        statement: "EXPLAIN SELECT value FROM samples",
        class: "unsupported",
        message: "EXPLAIN is not permitted",
        store_calls: 0,
    },
];

struct Fixture {
    running: ravel_server::Running,
    store: Arc<dyn ObjectStoreBackend>,
    metrics: Arc<StoreMetrics>,
    client: Client,
}

async fn fixture() -> Fixture {
    let (store, metrics) = instrumented_memory();
    seed_fixture(store.as_ref()).await;
    let mut config = server_config();
    config.audit_pipeline = None;
    let running = start_with(config, Arc::clone(&store), Arc::clone(&metrics)).await;
    let client = connect(&running).await;
    Fixture {
        running,
        store,
        metrics,
        client,
    }
}

impl Fixture {
    async fn stop(self) {
        self.client.cancel().await.expect("client stops");
        self.running.shutdown().await.expect("server stops");
    }

    async fn keys(&self) -> BTreeSet<String> {
        list_all(self.store.as_ref(), "")
            .await
            .expect("list the store")
            .into_iter()
            .map(|meta| meta.key.to_string())
            .collect()
    }

    /// Run `route` through `ravel_query_sql` and assert it is refused with
    /// its pinned class and nothing run.
    async fn assert_refused(&self, route: &str) {
        let route = ROUTES
            .iter()
            .find(|candidate| candidate.name == route)
            .expect("a known route");
        let target = local_target(route.name);
        let statement = route.statement.replace("{path}", &target.to_string_lossy());

        // A control first: the same tool answers an ordinary statement over
        // the fixture, so the refusal below is the route's and not the tool's.
        let (is_error, ok) = call(
            &self.client,
            "ravel_query_sql",
            json!({"query": "SELECT value FROM samples", "time_range": window()}),
        )
        .await;
        assert_eq!(is_error, Some(false), "{ok}");
        assert!(ok["data"]["row_count"].as_u64() > Some(0), "{ok}");

        let keys_before = self.keys().await;
        let before = self.metrics.snapshot();
        let (is_error, envelope) = call(
            &self.client,
            "ravel_query_sql",
            json!({"query": statement, "time_range": window()}),
        )
        .await;
        let after = self.metrics.snapshot();
        let keys_after = self.keys().await;

        let name = route.name;
        let calls = |snapshot: &ravel_object_store::StoreMetricsSnapshot| {
            snapshot.put.calls
                + snapshot.get.calls
                + snapshot.head.calls
                + snapshot.list.calls
                + snapshot.list_delimited.calls
                + snapshot.delete.calls
        };
        assert_eq!(after.put.calls, before.put.calls, "{name} wrote an object");
        assert_eq!(after.delete.calls, before.delete.calls, "{name} deleted");
        assert_eq!(keys_after, keys_before, "{name} changed the store");
        assert!(
            !target.exists(),
            "{name} created {}",
            target.to_string_lossy()
        );
        assert_eq!(
            calls(&after) - calls(&before),
            route.store_calls,
            "{name}: object-store calls"
        );

        assert_eq!(is_error, Some(true), "{name}: {envelope}");
        assert_eq!(envelope["status"], json!("error"), "{name}");
        assert_eq!(envelope["data"]["row_count"], json!(0), "{name}");
        assert_eq!(
            envelope["failure"]["class"],
            json!(route.class),
            "{name}: {}",
            envelope["failure"]
        );
        let message = envelope["failure"]["message"].as_str().unwrap_or_default();
        assert!(
            message.contains(route.message),
            "{name}: {message:?} does not carry {:?}",
            route.message
        );
    }
}

/// A local path no earlier run created, under this process's temp dir.
fn local_target(route: &str) -> PathBuf {
    let target = std::env::temp_dir().join(format!(
        "ravel-mcp-escape-{}-{route}.parquet",
        std::process::id()
    ));
    assert!(
        !target.exists(),
        "{} is left over",
        target.to_string_lossy()
    );
    target
}

/// Every route in one table against one server, in order, so a route that
/// left state behind (a registered table, a changed setting) would be seen by
/// the routes after it.
///
/// FLIP: each per-route test's wrong implementation below fails its row here
/// too; with all six applied at once every row fails.
#[tokio::test]
async fn every_sql_escape_route_is_refused_and_runs_nothing() {
    let fixture = fixture().await;
    for route in &ROUTES {
        fixture.assert_refused(route.name).await;
    }
    fixture.stop().await;
}

/// FLIP: the `DFStatement::CreateExternalTable` arm of `validate_query`
/// (`crates/ravel-sql/src/validate.rs`) returning `Ok(())` instead of
/// `NotReadOnly` changes the answer to `validation` ("the SQL request
/// contains no statement", from the page planner).
#[tokio::test]
async fn create_external_table_is_refused() {
    let fixture = fixture().await;
    fixture.assert_refused("create_external_table").await;
    fixture.stop().await;
}

/// FLIP: the `DFStatement::CopyTo` arm of `validate_query` returning
/// `Ok(())` changes the answer to `validation`.
#[tokio::test]
async fn copy_to_is_refused() {
    let fixture = fixture().await;
    fixture.assert_refused("copy").await;
    fixture.stop().await;
}

/// FLIP: `unreadable_table_reference` (`crates/ravel-sql/src/validate.rs`)
/// returning `Ok(None)` lets the statement past the executor's
/// unreadable-table check to the row-window check, which answers
/// `invalid_argument`.
#[tokio::test]
async fn read_parquet_is_refused() {
    let fixture = fixture().await;
    fixture.assert_refused("read_parquet").await;
    fixture.stop().await;
}

/// FLIP: deleting the `if row_window && tables.signals.is_empty()` refusal in
/// `resolve_parquet_target` (`crates/ravel-sql/src/executor.rs`) lets the
/// call resolve Parquet tables, and it makes 5 object-store calls.
/// `.with_information_schema(true)` in `crates/ravel-sql/src/session.rs` on
/// its own does not change the answer (see the module doc).
#[tokio::test]
async fn information_schema_is_refused() {
    let fixture = fixture().await;
    fixture.assert_refused("information_schema").await;
    fixture.stop().await;
}

/// FLIP: a `Statement::Set(_) => return Ok(())` arm ahead of `other` in
/// `validate_query` leaves the class `unsupported` (the page planner refuses
/// SET too) but changes the message to "the statement is not permitted".
#[tokio::test]
async fn set_is_refused() {
    let fixture = fixture().await;
    fixture.assert_refused("set").await;
    fixture.stop().await;
}

/// FLIP: the `DFStatement::Explain` arm of `validate_query` returning
/// `Ok(())` changes the answer to `validation`.
#[tokio::test]
async fn explain_is_refused() {
    let fixture = fixture().await;
    fixture.assert_refused("explain").await;
    fixture.stop().await;
}
