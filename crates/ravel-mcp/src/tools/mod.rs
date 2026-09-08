//! Tool dispatch: the one place a catalog name becomes a call (ADR-1374 D1).
//!
//! The adapter hands a name, the raw argument object, and the per-call
//! [`ToolContext`] to [`dispatch`], which routes to the tool body. Everything
//! the body needs that is not in its own arguments (the resolved tenant, the
//! call deadline, the clamped budgets, the process cursor key) travels in the
//! context, so a body never reads process state of its own.
//!
//! Two failures are the caller's protocol error rather than a result: a name
//! outside the catalog ([`ToolError::UnknownTool`]) and a catalog name whose
//! body this build does not carry ([`ToolError::NotShipped`]). Everything
//! else is an [`Envelope`] with a D4 failure class, because ADR-1374 D4
//! reserves protocol errors for malformed JSON-RPC and unknown tools.

pub mod capabilities;

use crate::budget::McpEffectiveBudgets;
use crate::cursor::CursorKey;
use crate::envelope::Envelope;
use crate::service::QueryBackend;
use ravel_types::TenantHash;

/// Everything a tool body needs about the call it is serving, beyond the
/// arguments the caller sent.
#[derive(Debug, Clone, Copy)]
pub struct ToolContext<'a> {
    /// The tenant the transport authenticated. Every store read a body issues
    /// is scoped to it, and a cursor minted under it is only redeemable by it.
    pub tenant_hash: TenantHash,
    /// Wall-clock instant the call must be finished by, as a nanosecond epoch.
    pub deadline_ns: i64,
    /// The oldest ingest instant the deployment still guarantees is readable,
    /// as a nanosecond epoch. A cursor or evidence reference pointing before
    /// it can no longer be honored.
    pub protection_horizon_ns: i64,
    /// The D6 budgets this call runs under, already clamped.
    pub budgets: McpEffectiveBudgets,
    /// The process-local MAC key cursors and evidence references are minted
    /// and redeemed with (D5).
    pub cursor_key: &'a CursorKey,
}

/// A tool call that never reaches a result envelope.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ToolError {
    /// A catalog name whose body is not in this build. The adapter renders
    /// the name so a caller learns which tool, not merely that one is
    /// missing.
    #[error("tool {0} is in the catalog but is not served by this build")]
    NotShipped(&'static str),
    /// A name outside the catalog.
    #[error("unknown tool: {0}")]
    UnknownTool(String),
}

/// Route one tool call to its body.
///
/// `backend` is the port the query-serving bodies execute through; the two
/// tools that read nothing (today only `ravel_capabilities`) never touch it.
pub async fn dispatch<B: QueryBackend>(
    name: &str,
    args: serde_json::Value,
    ctx: &ToolContext<'_>,
    backend: &B,
) -> Result<Envelope, ToolError> {
    let _ = backend;
    match name {
        "ravel_capabilities" => Ok(capabilities::run(args, ctx)),
        "ravel_describe_data" => Err(ToolError::NotShipped("ravel_describe_data")),
        "ravel_find_labels" => Err(ToolError::NotShipped("ravel_find_labels")),
        "ravel_explain_query" => Err(ToolError::NotShipped("ravel_explain_query")),
        "ravel_query_sql" => Err(ToolError::NotShipped("ravel_query_sql")),
        "ravel_query_promql" => Err(ToolError::NotShipped("ravel_query_promql")),
        "ravel_search_logs" => Err(ToolError::NotShipped("ravel_search_logs")),
        "ravel_get_trace" => Err(ToolError::NotShipped("ravel_get_trace")),
        "ravel_analyze_timeseries" => Err(ToolError::NotShipped("ravel_analyze_timeseries")),
        other => Err(ToolError::UnknownTool(other.to_string())),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    use super::*;
    use crate::budget::{McpBudgetConfig, McpRequestBudgets};
    use crate::catalog::tool_catalog;
    use crate::cursor::CURSOR_KEY_LEN;
    use crate::envelope::{AnyJson, Failure};
    use crate::service::QueryBackend;
    use ravel_query::EngineConfig;
    use ravel_query::http::{InstantRequest, MetadataRequest, RangeRequest};
    use ravel_types::TenantId;

    /// A backend that answers nothing. `dispatch` must not reach it for any
    /// name this build serves, so every method panicking would be equally
    /// correct; returning a failure keeps the test's own assertions about the
    /// returned envelope honest rather than aborting the run.
    struct NoBackend;

    fn unreached(method: &str) -> Failure {
        Failure {
            class: crate::envelope::FailureClass::Internal,
            message: format!("{method} must not be reached"),
            counter: None,
        }
    }

    impl QueryBackend for NoBackend {
        async fn promql_instant(
            &self,
            _tenant_hash: TenantHash,
            _request: &InstantRequest,
        ) -> Result<Envelope, Failure> {
            Err(unreached("promql_instant"))
        }

        async fn promql_range(
            &self,
            _tenant_hash: TenantHash,
            _request: &RangeRequest,
        ) -> Result<Envelope, Failure> {
            Err(unreached("promql_range"))
        }

        async fn labels(
            &self,
            _tenant_hash: TenantHash,
            _request: &MetadataRequest,
        ) -> Result<Envelope, Failure> {
            Err(unreached("labels"))
        }

        async fn label_values(
            &self,
            _tenant_hash: TenantHash,
            _request: &MetadataRequest,
            _name: &str,
            _include_log_metric_names: bool,
        ) -> Result<Envelope, Failure> {
            Err(unreached("label_values"))
        }

        async fn series(
            &self,
            _tenant_hash: TenantHash,
            _request: &MetadataRequest,
        ) -> Result<Envelope, Failure> {
            Err(unreached("series"))
        }

        async fn analytics(
            &self,
            _tenant_hash: TenantHash,
            _request: &AnyJson,
        ) -> Result<Envelope, Failure> {
            Err(unreached("analytics"))
        }

        async fn exemplars(
            &self,
            _tenant_hash: TenantHash,
            _request: &AnyJson,
        ) -> Result<Envelope, Failure> {
            Err(unreached("exemplars"))
        }

        async fn sql_execute(
            &self,
            _tenant_hash: TenantHash,
            _request: &ravel_sql::SqlRequest,
        ) -> Result<Envelope, Failure> {
            Err(unreached("sql_execute"))
        }

        async fn sql_explain(
            &self,
            _tenant_hash: TenantHash,
            _request: &ravel_sql::SqlRequest,
        ) -> Result<Envelope, Failure> {
            Err(unreached("sql_explain"))
        }
    }

    pub(crate) fn test_context(key: &CursorKey) -> ToolContext<'_> {
        ToolContext {
            tenant_hash: TenantId::new("acme").hash(),
            deadline_ns: 4 * 3_600_000_000_000,
            protection_horizon_ns: 0,
            budgets: McpRequestBudgets::default()
                .clamp(&EngineConfig::default(), &McpBudgetConfig::default()),
            cursor_key: key,
        }
    }

    /// Every name in the catalog reaches `dispatch`, and `dispatch` refuses
    /// anything else. The counts are exact on purpose: the eight bodies that
    /// land in #1380 and #1382 must each answer `NotShipped` today rather than
    /// a placeholder envelope, and the one body this build carries must
    /// answer an envelope.
    /// Drive a future that is ready on its first poll, so this crate needs no
    /// async runtime to test dispatch. Every path `dispatch` takes in this
    /// build is immediate: `ravel_capabilities` reads no store, and the eight
    /// refusals return before awaiting anything.
    fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("dispatch awaited; no tool in this build reads a store"),
        }
    }

    #[test]
    fn dispatch_routes_every_catalog_name_and_refuses_unknown_names() {
        let key = CursorKey::from_process_secret([7u8; CURSOR_KEY_LEN]);
        let ctx = test_context(&key);

        let catalog = tool_catalog();
        assert_eq!(catalog.len(), 9);

        let mut served = Vec::new();
        let mut not_shipped = Vec::new();
        for tool in &catalog {
            let name = tool.name.to_string();
            match block_on(dispatch(&name, serde_json::json!({}), &ctx, &NoBackend)) {
                Ok(envelope) => {
                    assert_eq!(envelope.status, crate::envelope::Status::Ok);
                    served.push(name);
                }
                Err(ToolError::NotShipped(shipped_name)) => {
                    assert_eq!(shipped_name, name);
                    not_shipped.push(name);
                }
                Err(ToolError::UnknownTool(unknown)) => {
                    panic!("catalog name {unknown} did not route");
                }
            }
        }

        assert_eq!(served, vec!["ravel_capabilities".to_string()]);
        assert_eq!(not_shipped.len(), 8);

        let unknown = block_on(dispatch(
            "ravel_drop_tenant",
            serde_json::json!({}),
            &ctx,
            &NoBackend,
        ))
        .expect_err("a name outside the catalog is refused");
        assert_eq!(
            unknown,
            ToolError::UnknownTool("ravel_drop_tenant".to_string())
        );
    }
}
