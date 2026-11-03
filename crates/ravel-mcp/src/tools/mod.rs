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

pub mod agents;
pub mod analyze_timeseries;
pub mod capabilities;
pub mod describe_data;
pub mod explain_query;
pub mod find_labels;
pub mod get_trace;
pub mod promql_range;
pub mod query_promql;
pub mod query_sql;
pub mod search_logs;

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
    /// The wall-clock instant the call started, as a nanosecond epoch: what a
    /// SQL body resolves at and mints its cursor at, and what a cursor's
    /// deadline is tested against on redemption and confirmation.
    pub now_ns: i64,
    /// Wall-clock instant the call must be finished by, as a nanosecond epoch.
    pub deadline_ns: i64,
    /// The instant, as a nanosecond epoch, through which the deployment
    /// still protects a pinned snapshot from the sweeper. A cursor or
    /// evidence reference whose effective deadline falls after it can no
    /// longer be honored.
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

/// The catalog names [`dispatch`] routes to a body in this build.
///
/// This build serves all nine catalog names. A catalog name missing from this
/// list would answer [`ToolError::NotShipped`], and `ravel_capabilities`
/// reports such names under `catalogued`, apart from `enabled`: a caller that
/// reads one list of names could not tell which of them it can actually call. `dispatch_routes_every_catalog_name_and_refuses_unknown_names` pins
/// this list against what `dispatch` does, so adding a body without adding it
/// here fails the test rather than under-reporting the surface.
pub const SERVED_TOOLS: &[&str] = &[
    "ravel_capabilities",
    "ravel_describe_data",
    "ravel_find_labels",
    "ravel_explain_query",
    "ravel_query_sql",
    "ravel_query_promql",
    "ravel_search_logs",
    "ravel_get_trace",
    "ravel_analyze_timeseries",
];

/// Route one tool call to its body.
///
/// `backend` is the port the query-serving bodies execute through; a tool
/// that reads nothing (today only `ravel_capabilities`) never touches it.
pub async fn dispatch<B: QueryBackend>(
    name: &str,
    args: serde_json::Value,
    ctx: &ToolContext<'_>,
    backend: &B,
) -> Result<Envelope, ToolError> {
    match name {
        "ravel_capabilities" => Ok(capabilities::run(args, ctx)),
        "ravel_describe_data" => Ok(describe_data::run(args, ctx, backend).await),
        "ravel_find_labels" => Ok(find_labels::run(args, ctx, backend).await),
        "ravel_explain_query" => Ok(explain_query::run(args, ctx, backend).await),
        "ravel_query_sql" => Ok(query_sql::run(args, ctx, backend).await),
        "ravel_query_promql" => Ok(query_promql::run(args, ctx, backend).await),
        "ravel_search_logs" => Ok(search_logs::run(args, ctx, backend).await),
        "ravel_get_trace" => Ok(get_trace::run(args, ctx, backend).await),
        "ravel_analyze_timeseries" => Ok(analyze_timeseries::run(args, ctx, backend).await),
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
            _resolve_at_ns: i64,
            _order_columns: &[String],
        ) -> Result<crate::draft::Draft, Failure> {
            Err(unreached("sql_execute"))
        }

        async fn trace_lookup(
            &self,
            _tenant_hash: TenantHash,
            _request: &crate::service::TraceLookupRequest,
        ) -> Result<Envelope, Failure> {
            Err(unreached("trace_lookup"))
        }

        async fn sql_explain(
            &self,
            _tenant_hash: TenantHash,
            _request: &ravel_sql::SqlRequest,
        ) -> Result<crate::draft::ExplainOutcome, Failure> {
            Err(unreached("sql_explain"))
        }

        async fn describe_data(
            &self,
            _tenant_hash: TenantHash,
            _request: &crate::service::DescribeRequest,
        ) -> Result<crate::service::DescribeOutcome, Failure> {
            Err(unreached("describe_data"))
        }
    }

    /// One metadata call: the request, and for `label_values` the label name
    /// and the `include_log_metric_names` flag.
    pub(crate) type MetadataCall = (MetadataRequest, Option<(String, bool)>);

    /// One `sql_execute` call: the request, the resolve instant and the order
    /// columns it was handed.
    #[derive(Debug, Clone)]
    pub(crate) struct SqlCall {
        pub(crate) request: ravel_sql::SqlRequest,
        pub(crate) resolve_at_ns: i64,
        pub(crate) order_columns: Vec<String>,
    }

    /// A backend whose `sql_explain`, `sql_execute`, `labels`, `label_values`,
    /// `describe_data`, `promql_instant`, `promql_range` and `analytics`
    /// answer what the test configured and record the request they were
    /// handed. Every other method, and any of those left unconfigured,
    /// panics: a test built on it proves the body never reached that call.
    /// `describe`, `sql`, `promql` and `analytics` are queues, one answer per
    /// call, so a paging test configures each page's answer in order; the two
    /// PromQL methods share `promql`.
    #[derive(Default)]
    pub(crate) struct Stub {
        pub(crate) promql:
            std::cell::RefCell<std::collections::VecDeque<Result<Envelope, Failure>>>,
        pub(crate) analytics:
            std::cell::RefCell<std::collections::VecDeque<Result<Envelope, Failure>>>,
        pub(crate) analyzed: std::cell::RefCell<Vec<AnyJson>>,
        pub(crate) instants: std::cell::RefCell<Vec<InstantRequest>>,
        pub(crate) ranges: std::cell::RefCell<Vec<RangeRequest>>,
        pub(crate) explain: Option<crate::draft::ExplainOutcome>,
        pub(crate) listed: Option<Envelope>,
        pub(crate) explained: std::cell::RefCell<Vec<ravel_sql::SqlRequest>>,
        pub(crate) metadata: std::cell::RefCell<Vec<MetadataCall>>,
        pub(crate) describe:
            std::cell::RefCell<std::collections::VecDeque<crate::service::DescribeOutcome>>,
        pub(crate) described: std::cell::RefCell<Vec<crate::service::DescribeRequest>>,
        pub(crate) sql: std::cell::RefCell<std::collections::VecDeque<crate::draft::Draft>>,
        pub(crate) executed: std::cell::RefCell<Vec<SqlCall>>,
    }

    impl Stub {
        fn listed(&self, method: &str) -> Result<Envelope, Failure> {
            match &self.listed {
                Some(envelope) => Ok(envelope.clone()),
                None => panic!("{method} must not be reached"),
            }
        }
    }

    impl QueryBackend for Stub {
        async fn promql_instant(
            &self,
            _tenant_hash: TenantHash,
            request: &InstantRequest,
        ) -> Result<Envelope, Failure> {
            let Some(answer) = self.promql.borrow_mut().pop_front() else {
                panic!("promql_instant must not be reached");
            };
            self.instants.borrow_mut().push(request.clone());
            answer
        }

        async fn promql_range(
            &self,
            _tenant_hash: TenantHash,
            request: &RangeRequest,
        ) -> Result<Envelope, Failure> {
            let Some(answer) = self.promql.borrow_mut().pop_front() else {
                panic!("promql_range must not be reached");
            };
            self.ranges.borrow_mut().push(request.clone());
            answer
        }

        async fn labels(
            &self,
            _tenant_hash: TenantHash,
            request: &MetadataRequest,
        ) -> Result<Envelope, Failure> {
            let listed = self.listed("labels");
            self.metadata.borrow_mut().push((request.clone(), None));
            listed
        }

        async fn label_values(
            &self,
            _tenant_hash: TenantHash,
            request: &MetadataRequest,
            name: &str,
            include_log_metric_names: bool,
        ) -> Result<Envelope, Failure> {
            let listed = self.listed("label_values");
            self.metadata.borrow_mut().push((
                request.clone(),
                Some((name.to_string(), include_log_metric_names)),
            ));
            listed
        }

        async fn series(
            &self,
            _tenant_hash: TenantHash,
            _request: &MetadataRequest,
        ) -> Result<Envelope, Failure> {
            panic!("series must not be reached")
        }

        async fn analytics(
            &self,
            _tenant_hash: TenantHash,
            request: &AnyJson,
        ) -> Result<Envelope, Failure> {
            let Some(answer) = self.analytics.borrow_mut().pop_front() else {
                panic!("analytics must not be reached");
            };
            self.analyzed.borrow_mut().push(request.clone());
            answer
        }

        async fn exemplars(
            &self,
            _tenant_hash: TenantHash,
            _request: &AnyJson,
        ) -> Result<Envelope, Failure> {
            panic!("exemplars must not be reached")
        }

        async fn sql_execute(
            &self,
            _tenant_hash: TenantHash,
            request: &ravel_sql::SqlRequest,
            resolve_at_ns: i64,
            order_columns: &[String],
        ) -> Result<crate::draft::Draft, Failure> {
            let Some(draft) = self.sql.borrow_mut().pop_front() else {
                panic!("sql_execute must not be reached");
            };
            self.executed.borrow_mut().push(SqlCall {
                request: request.clone(),
                resolve_at_ns,
                order_columns: order_columns.to_vec(),
            });
            Ok(draft)
        }

        async fn trace_lookup(
            &self,
            _tenant_hash: TenantHash,
            _request: &crate::service::TraceLookupRequest,
        ) -> Result<Envelope, Failure> {
            panic!("trace_lookup must not be reached")
        }

        async fn sql_explain(
            &self,
            _tenant_hash: TenantHash,
            request: &ravel_sql::SqlRequest,
        ) -> Result<crate::draft::ExplainOutcome, Failure> {
            let Some(outcome) = &self.explain else {
                panic!("sql_explain must not be reached");
            };
            self.explained.borrow_mut().push(request.clone());
            Ok(outcome.clone())
        }

        async fn describe_data(
            &self,
            _tenant_hash: TenantHash,
            request: &crate::service::DescribeRequest,
        ) -> Result<crate::service::DescribeOutcome, Failure> {
            let Some(outcome) = self.describe.borrow_mut().pop_front() else {
                panic!("describe_data must not be reached");
            };
            self.described.borrow_mut().push(request.clone());
            Ok(outcome)
        }
    }

    pub(crate) fn test_context(key: &CursorKey) -> ToolContext<'_> {
        ToolContext {
            tenant_hash: TenantId::new("acme").hash(),
            now_ns: 3 * 3_600_000_000_000,
            deadline_ns: 4 * 3_600_000_000_000,
            protection_horizon_ns: 30 * 3_600_000_000_000,
            budgets: McpRequestBudgets::default()
                .clamp(&EngineConfig::default(), &McpBudgetConfig::default()),
            cursor_key: key,
        }
    }

    /// Drive a future that is ready on its first poll, so this crate needs no
    /// async runtime to test dispatch. Every path `dispatch` takes in this
    /// build on an empty argument object is immediate: `ravel_capabilities`
    /// reads no store, the seven query bodies refuse an empty argument object
    /// before reaching their backend, and the one refusal returns before
    /// awaiting anything. The stub backends in this crate's tests answer
    /// without awaiting too.
    pub(crate) fn block_on<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("dispatch awaited; no tool in this build reads a store"),
        }
    }

    /// Every name in the catalog reaches `dispatch`, and `dispatch` refuses
    /// anything else. The counts are exact on purpose: all nine bodies this
    /// build carries must answer an envelope, and none may answer
    /// `NotShipped`.
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
                Ok(envelope) if name == "ravel_get_trace" || name == "ravel_describe_data" => {
                    // Served, and an empty argument object names no trace
                    // (no signal, for describe): a deserialization refusal.
                    assert_eq!(envelope.status, crate::envelope::Status::Error);
                    assert_eq!(
                        envelope.failure.map(|failure| failure.class),
                        Some(crate::envelope::FailureClass::InvalidArgument)
                    );
                    served.push(name);
                }
                Ok(envelope)
                    if name == "ravel_find_labels"
                        || name == "ravel_explain_query"
                        || name == "ravel_query_sql"
                        || name == "ravel_query_promql"
                        || name == "ravel_search_logs"
                        || name == "ravel_analyze_timeseries" =>
                {
                    // Served, and an empty argument object has no time_range
                    // (and, for the SQL, PromQL and analyze tools, no query):
                    // D4's missing_argument.
                    assert_eq!(envelope.status, crate::envelope::Status::Error);
                    assert_eq!(
                        envelope.failure.map(|failure| failure.class),
                        Some(crate::envelope::FailureClass::MissingArgument)
                    );
                    served.push(name);
                }
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

        assert_eq!(
            served,
            vec![
                "ravel_capabilities".to_string(),
                "ravel_describe_data".to_string(),
                "ravel_find_labels".to_string(),
                "ravel_explain_query".to_string(),
                "ravel_query_sql".to_string(),
                "ravel_query_promql".to_string(),
                "ravel_search_logs".to_string(),
                "ravel_get_trace".to_string(),
                "ravel_analyze_timeseries".to_string()
            ]
        );
        assert_eq!(not_shipped, Vec::<String>::new());

        // What `ravel_capabilities` reports is the same split, name for name:
        // `enabled` is what dispatch runs, `catalogued` is what it refuses. A
        // body added to dispatch without being added to `SERVED_TOOLS` is
        // reported as unavailable while answering calls, and one removed
        // without the reverse edit is advertised as callable; either way this
        // assertion fails rather than the report drifting.
        let capabilities = block_on(dispatch(
            "ravel_capabilities",
            serde_json::json!({}),
            &ctx,
            &NoBackend,
        ))
        .expect("ravel_capabilities answers an envelope");
        let reported = serde_json::to_value(&capabilities).expect("envelope serializes");
        let tools = &reported["data"]["rows"][1][1];
        assert_eq!(tools["enabled"], serde_json::json!(served));
        assert_eq!(tools["catalogued"], serde_json::json!(not_shipped));

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
