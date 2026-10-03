//! The engine seam a later task's acceptance test drives (ADR-2040, issue
//! #2055 task T5b). [`SuiteEngine`] is the only contract this module
//! defines: no implementation lives here. A later task wires a concrete
//! engine (an in-process DataFusion session, a running `ravel-server`'s
//! Flight SQL endpoint, an upstream reference engine) against the suite and
//! fixture this crate already ships.

use datafusion::arrow::record_batch::RecordBatch;

/// What a `CREATE EXTERNAL TABLE` (or equivalent DDL) call reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DdlReceipt {
    /// The engine's own human-readable outcome string (a status line, a
    /// row count message); not interpreted by callers beyond logging it.
    pub outcome: String,
    /// Number of files the engine reports it mounted, when the engine
    /// exposes that count.
    pub files: Option<u64>,
}

/// Everything a [`SuiteEngine`] call can fail on.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EngineError {
    /// The DDL statement was rejected.
    #[error("ddl rejected: {0}")]
    Ddl(String),
    /// The query statement was rejected or failed during execution.
    #[error("query failed: {0}")]
    Query(String),
    /// The engine could not be reached at all (a transport error, a closed
    /// connection), as distinct from the engine reaching back with a
    /// rejection.
    #[error("engine unreachable: {0}")]
    Unreachable(String),
}

/// One query engine under test. A later task implements this against each
/// engine the acceptance test compares (ADR-2040 section D7): every
/// implementation runs the exact same suite statements and DDL template,
/// so the comparator in [`super::comparator`] is comparing engines, not
/// comparing a hand-written harness against itself.
#[async_trait::async_trait]
pub trait SuiteEngine: Send + Sync {
    /// Runs one DDL statement (the rendered `suite.toml` table template)
    /// against the engine, mounting the fixture for subsequent `query`
    /// calls.
    async fn ddl(&self, sql: &str) -> Result<DdlReceipt, EngineError>;

    /// Runs one suite statement and returns its result rows.
    async fn query(&self, sql: &str) -> Result<Vec<RecordBatch>, EngineError>;
}
