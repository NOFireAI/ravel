//! The `POST /mcp` route and the [`ServerHandler`] behind it (ADR-1374
//! decision 7).
//!
//! The route is one axum handler rather than a mounted tower service, because
//! the checks in [`super::auth`] must run on the request before the protocol
//! layer parses it. The handler runs them, then hands the request to a cloned
//! [`StreamableHttpService`], which is the only thing in this file that speaks
//! Streamable HTTP.
//!
//! Nothing here spawns. A tool call is awaited on the task rmcp already
//! dispatched the request on, alongside the request's own cancellation token:
//! when the client cancels (a `notifications/cancelled` on the legacy
//! revision, a closed stream on the current one) the tool future is dropped,
//! and the usage guard inside the query service bills what the dropped call
//! had already spent.

use std::borrow::Cow;
use std::sync::{Arc, OnceLock};

use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::Request;
use axum::response::Response;
use axum::routing::post;
use rand::TryRng;
use rand::rngs::SysRng;
use ravel_ingest::Clock;
use ravel_mcp::budget::{McpBudgetConfig, McpEffectiveBudgets, McpRequestBudgets};
use ravel_mcp::catalog::tool_catalog;
use ravel_mcp::compact;
use ravel_mcp::cursor::{CURSOR_KEY_LEN, CursorKey};
use ravel_mcp::envelope::{Envelope, NextStep, Status};
use ravel_mcp::tools::{ToolContext, ToolError, dispatch};
use ravel_query::{ByteLimit, EngineConfig, RequestBudgets, RequestLimit};
use ravel_types::TenantHash;
use rmcp::model::{
    CallToolRequestParams, CallToolResponse, CallToolResult, CancelledNotificationParam,
    ContentBlock, ErrorCode, Implementation, InitializeResult, ListToolsResult,
    PaginatedRequestParams, ProtocolVersion, ServerCapabilities, ServerInfo,
};
use rmcp::service::{NotificationContext, RequestContext, RoleServer};
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use rmcp::{ErrorData as McpError, ServerHandler};
use serde_json::Value;
use tower::Service as _;

use crate::service::QueryService;

use super::auth::{self, AuthenticatedTenant, McpAuth};
use super::envelope as d4;
use super::service_impl::{ProgressReporter, ServiceBackend};

/// The one path this adapter serves. D7 mounts no GET stream and no DELETE:
/// every message is a POST, and the two revisions this build speaks need
/// nothing else.
const MCP_PATH: &str = "/mcp";

/// What the deployment configures about this route. The engine ceilings and
/// the clock are passed in rather than read from the query service, which
/// exposes neither.
pub struct McpSettings {
    /// Browser origins the route accepts, as `scheme://host[:port]`.
    pub allowed_origins: Vec<String>,
    /// Cap on a request body, in bytes.
    pub max_body_bytes: usize,
    /// The query-engine ceilings every tool call's budgets clamp down to.
    pub engine_config: EngineConfig,
    /// The MCP-layer ceilings (today only the response-byte ceiling).
    pub budget_config: McpBudgetConfig,
    /// The deployment's GC protection horizon, as a duration in nanoseconds.
    /// Subtracted from the call's own instant to get the oldest readable one.
    pub protection_horizon_ns: i64,
    pub clock: Arc<dyn Clock>,
}

/// The per-listener state the route handler needs: the checks, the resolver
/// that runs them, and the protocol layer they gate.
struct McpRoute {
    auth: McpAuth,
    service: QueryService,
    transport: StreamableHttpService<RavelMcp, LocalSessionManager>,
}

/// Mount the MCP route on its own router, for the caller to merge into the
/// listener's.
///
/// `service` is the listener's own [`QueryService`], so the credential this
/// route authenticates is the one that listener issues: a bearer token on the
/// primary listener, a peer certificate on the mTLS one.
///
/// Fails only if the OS entropy source cannot produce this process's cursor
/// key, which is a deployment that cannot mint a cursor at all and so must
/// not start.
pub fn router(service: QueryService, settings: McpSettings) -> anyhow::Result<Router> {
    let handler = RavelMcp {
        service: service.clone(),
        engine_config: settings.engine_config,
        budget_config: settings.budget_config,
        protection_horizon_ns: settings.protection_horizon_ns,
        clock: settings.clock,
        cursor_key: cursor_key()?,
    };

    // `max_request_body_bytes` repeats the cap `auth` already enforces, so a
    // request that somehow reaches the protocol layer unchecked is still
    // bounded. `disable_allowed_hosts` turns off rmcp's own loopback-only
    // `Host` allowlist, which would refuse every request to a deployment
    // reachable under its own name; `allowed_origins` stays empty for the
    // same division of labour, because `auth` owns the origin check and a
    // second allowlist would refuse or admit on rules the operator did not
    // configure.
    let config = StreamableHttpServerConfig::default()
        .with_max_request_body_bytes(settings.max_body_bytes)
        .disable_allowed_hosts();
    let transport = StreamableHttpService::new(
        move || Ok(handler.clone()),
        Arc::new(LocalSessionManager::default()),
        config,
    );

    let state = Arc::new(McpRoute {
        auth: McpAuth {
            allowed_origins: settings.allowed_origins,
            max_body_bytes: settings.max_body_bytes,
        },
        service,
        transport,
    });
    Ok(Router::new()
        .route(MCP_PATH, post(handle))
        .with_state(state))
}

/// Run the pre-protocol checks, then serve the request.
async fn handle(State(state): State<Arc<McpRoute>>, request: Request<Body>) -> Response {
    let request = match auth::check(&state.auth, &state.service, request).await {
        Ok(request) => request,
        Err(refusal) => return refusal,
    };
    // Cloning the service is how rmcp's tower adapter is used: the clone
    // shares the session manager and the configuration, and `call` needs it
    // by value.
    let mut transport = state.transport.clone();
    match transport.call(request).await {
        Ok(response) => response.map(Body::new),
        // The transport's error type is `Infallible`, so this arm has no
        // value to handle.
        Err(never) => match never {},
    }
}

/// The process's cursor MAC key (D5), drawn once from the OS entropy source.
///
/// Never derived from a configured or shared secret: two processes deriving
/// one key would each honour cursors the other minted, and a restarted
/// process would redeem cursors describing a snapshot it no longer has. A
/// fresh key per process makes both cases a `cursor_invalid` refusal.
static CURSOR_KEY: OnceLock<CursorKey> = OnceLock::new();

fn cursor_key() -> anyhow::Result<&'static CursorKey> {
    if let Some(key) = CURSOR_KEY.get() {
        return Ok(key);
    }
    let mut secret = [0u8; CURSOR_KEY_LEN];
    SysRng.try_fill_bytes(&mut secret)?;
    Ok(CURSOR_KEY.get_or_init(|| CursorKey::from_process_secret(secret)))
}

/// The MCP server this process is. One instance per session (rmcp builds it
/// from the factory), all of them sharing the process's cursor key and the
/// listener's query service.
#[derive(Clone)]
struct RavelMcp {
    service: QueryService,
    engine_config: EngineConfig,
    budget_config: McpBudgetConfig,
    protection_horizon_ns: i64,
    clock: Arc<dyn Clock>,
    cursor_key: &'static CursorKey,
}

impl ServerHandler for RavelMcp {
    fn get_info(&self) -> ServerInfo {
        InitializeResult::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new(
                "ravel-server",
                env!("CARGO_PKG_VERSION"),
            ))
            .with_instructions(
                "Ravel telemetry database. Every tool returns one result envelope: \
                 `data` holds the rows, `budget` what the call was allowed and what \
                 it spent, `coverage` whether the answer is complete, and \
                 `next_steps` what to call next. Start with ravel_capabilities.",
            )
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(auth::SUPPORTED_REVISIONS.as_slice())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, McpError> {
        Ok(ListToolsResult::with_all_items(tool_catalog()))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, McpError> {
        let tenant_hash = tenant(&context)?;
        let args = Value::Object(request.arguments.unwrap_or_default());
        let budgets = match requested_budgets(&args) {
            Ok(requested) => requested.clamp(&self.engine_config, &self.budget_config),
            // Nothing runs on a budget block this layer could not read. The
            // refusal is serialized under the deployment's own default
            // response cap, since the caller's cap is part of what failed.
            Err(message) => {
                let default =
                    McpRequestBudgets::default().clamp(&self.engine_config, &self.budget_config);
                return tool_result(&invalid_budget(message, &default));
            }
        };
        let now_ns = self.clock.now_ns();
        let ctx = ToolContext {
            tenant_hash,
            deadline_ns: now_ns.saturating_add(deadline_ns(&budgets)),
            protection_horizon_ns: now_ns.saturating_sub(self.protection_horizon_ns),
            budgets,
            cursor_key: self.cursor_key,
        };

        // A notification stream the client did not ask for is traffic it did
        // not ask for, so the reporter exists only when a `progressToken`
        // does.
        let progress = context
            .meta
            .get_progress_token()
            .map(|token| ProgressReporter::new(context.peer.clone(), token));
        let backend = ServiceBackend::new(self.service.clone(), budgets).with_progress(progress);

        // The cancellation arm is biased first so a token already cancelled
        // drops the tool future before it is ever polled, rather than
        // spending a store request on a call whose client is gone.
        let dispatched = tokio::select! {
            biased;
            () = context.ct.cancelled() => {
                return Err(McpError::new(
                    ErrorCode::INTERNAL_ERROR,
                    "the tool call was cancelled",
                    None,
                ));
            }
            dispatched = dispatch(&request.name, args, &ctx, &backend) => dispatched,
        };

        match dispatched {
            Ok(envelope) => tool_result(&envelope),
            // D4 reserves protocol errors for malformed JSON-RPC and for a
            // tool that cannot be routed. Both of these are the latter: a
            // name outside the catalog, and a catalog name whose body this
            // build does not carry. Neither ran, so neither has an envelope.
            Err(error @ (ToolError::UnknownTool(_) | ToolError::NotShipped(_))) => Err(
                McpError::new(ErrorCode::METHOD_NOT_FOUND, error.to_string(), None),
            ),
        }
    }

    async fn on_cancelled(
        &self,
        notification: CancelledNotificationParam,
        _context: NotificationContext<RoleServer>,
    ) {
        // rmcp cancels the request's own token before calling this, which is
        // what drops the tool future awaiting in `call_tool`. Nothing to do
        // here but record which call went away, and why the client said so.
        tracing::debug!(
            request_id = ?notification.request_id,
            reason = ?notification.reason,
            "mcp tool call cancelled",
        );
    }
}

/// The tenant [`super::auth`] resolved, from the HTTP parts rmcp carries into
/// the request extensions.
///
/// Absent means the request did not pass through the checks, which cannot
/// happen on this route. It is an internal error rather than a fallback to an
/// anonymous tenant: guessing one would serve another tenant's data.
fn tenant(context: &RequestContext<RoleServer>) -> Result<TenantHash, McpError> {
    context
        .extensions
        .get::<axum::http::request::Parts>()
        .and_then(|parts| parts.extensions.get::<AuthenticatedTenant>())
        .map(|tenant| tenant.0)
        .ok_or_else(|| {
            McpError::internal_error("the request carries no authenticated tenant", None)
        })
}

/// One optional budget field, by name, out of arguments whose remaining
/// fields belong to the tool.
///
/// An absent or null field is the caller declining to lower that budget. A
/// field that is present but not of the declared type is named and refused,
/// never dropped: running the call at a default the caller did not ask for
/// and then reporting that default as `budget.effective` is a silent
/// substitution the caller has no way to notice.
fn budget_field<T: serde::de::DeserializeOwned>(
    args: &Value,
    name: &str,
) -> Result<Option<T>, String> {
    match args.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|error| format!("{name}: {error}")),
    }
}

/// The six lowerable budget knobs, as every data-returning tool's input
/// schema declares them (docs/reference/mcp.md). The first field that does
/// not parse is the reported one.
fn requested_budgets(args: &Value) -> Result<McpRequestBudgets, String> {
    Ok(McpRequestBudgets {
        max_rows: budget_field(args, "max_rows")?,
        max_response_bytes: budget_field(args, "max_response_bytes")?,
        deadline_ms: budget_field(args, "deadline_ms")?,
        query: RequestBudgets {
            max_bytes_scanned: budget_field::<u64>(args, "max_bytes_scanned")?
                .map(ByteLimit::Bounded),
            max_store_requests: budget_field::<u64>(args, "max_store_requests")?
                .map(RequestLimit::Bounded),
            max_segments: budget_field(args, "max_segments")?,
        },
    })
}

/// A budget block this adapter could not read, as a D4 `invalid_argument`
/// envelope.
///
/// The `budget` block stays null rather than reporting the defaults: no
/// budget was resolved and no operation ran, so a ceiling there would claim
/// the call was allowed something. The response is still serialized under the
/// default response cap, which is the cap actually in force for it.
fn invalid_budget(message: String, budgets: &McpEffectiveBudgets) -> Envelope {
    let envelope = Envelope {
        status: Status::Error,
        failure: Some(d4::invalid_argument(format!("budget argument {message}"))),
        next_steps: vec![NextStep {
            action: "correct_the_budget_argument".to_string(),
            detail: "send the named field with the type this tool's input schema declares, or \
                     omit it to run at the server's effective ceiling"
                .to_string(),
        }],
        ..Envelope::default()
    };
    envelope.fit(budgets.max_response_bytes).finish(false)
}

/// The deadline as a nanosecond offset. A duration too large for an `i64` is
/// the largest one that fits: the clamp already lowered it to the engine
/// ceiling, so this only guards the arithmetic.
fn deadline_ns(budgets: &McpEffectiveBudgets) -> i64 {
    i64::try_from(budgets.deadline.as_nanos()).unwrap_or(i64::MAX)
}

/// One envelope, as an MCP tool result.
///
/// Both forms carry the same envelope: `structured_content` for a client that
/// reads fields, and the compact text rendering for a model that reads the
/// result as text. An error envelope sets `isError`, so a client does not
/// have to inspect the envelope's own `status` to know the call failed.
fn tool_result(envelope: &Envelope) -> Result<CallToolResponse, McpError> {
    let structured = serde_json::to_value(envelope).map_err(|error| {
        McpError::internal_error(
            "the result envelope could not be serialized",
            Some(serde_json::json!({ "reason": error.to_string() })),
        )
    })?;
    let mut result = match envelope.status {
        Status::Error => CallToolResult::structured_error(structured),
        Status::Ok | Status::OkBounded | Status::OkPage => CallToolResult::structured(structured),
    };
    result.content = vec![ContentBlock::text(compact::render(envelope))];
    Ok(result.into())
}
