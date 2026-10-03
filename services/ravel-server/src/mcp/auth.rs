//! Everything that must hold before rmcp sees a byte (ADR-1374 decision 7).
//!
//! Four checks, in this order, because each one's refusal must not depend on
//! work a later one does:
//!
//! 1. `Origin`, against the deployment's allowlist. A browser page on another
//!    site can reach this route with the user's ambient credentials, so the
//!    origin is refused before the credential is even looked at.
//! 2. The tenant credential, through the router's own [`TenantResolver`] --
//!    the primary listener's bearer-token resolver or the mTLS listener's
//!    peer-certificate resolver, whichever router this route was mounted on.
//!    A request with no resolvable tenant is refused here, so an anonymous
//!    caller cannot make this process read object storage, take an admission
//!    permit, or mint a cursor. The refusal is the same 401 body every HTTP
//!    query route returns, and it applies to a legacy `initialize` too: the
//!    handshake is as much a request as a tool call.
//! 3. The body size, against the configured cap. Buffered here rather than
//!    streamed into the protocol layer, because check 4 needs the body and a
//!    request that fails the cap must not be parsed at all.
//! 4. The SEP-2243 standard headers, on the current revision only. A
//!    `2026-07-28` request states its own method and tool name in headers so
//!    an intermediary can route and authorize it without parsing JSON-RPC; a
//!    header that disagrees with the body is a request whose two statements of
//!    intent differ, and serving either one would be a guess. The legacy
//!    revision defines no such headers, so requiring them there would refuse
//!    every conforming legacy client.
//!
//! The resolved tenant leaves here in the request extensions as
//! [`AuthenticatedTenant`], which is the only way the handler can obtain one:
//! it holds no resolver of its own, so a tool call whose request skipped this
//! module has no tenant to run as and fails closed.

use axum::body::{Body, Bytes};
use axum::http::header::ORIGIN;
use axum::http::{HeaderMap, HeaderName, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use ravel_types::TenantHash;
use rmcp::model::ProtocolVersion;
use serde_json::Value;

use crate::service::{ApiError, QueryService, ServiceError};

/// SEP-2243's per-request protocol revision.
const PROTOCOL_VERSION: HeaderName = HeaderName::from_static("mcp-protocol-version");
/// SEP-2243's JSON-RPC method, restated as a header.
const MCP_METHOD: HeaderName = HeaderName::from_static("mcp-method");
/// SEP-2243's target name (for `tools/call`, the tool), restated as a header.
const MCP_NAME: HeaderName = HeaderName::from_static("mcp-name");

/// The revisions this adapter serves: the current one and the last legacy one.
/// A request naming any other revision is refused rather than served under a
/// revision it did not ask for.
pub(crate) static SUPPORTED_REVISIONS: [ProtocolVersion; 2] =
    [ProtocolVersion::V_2026_07_28, ProtocolVersion::V_2025_11_25];

/// The tenant this request's credential resolved to, carried in the request
/// extensions from this module to the tool handler.
///
/// A newtype rather than a bare [`TenantHash`] so that nothing else in the
/// extensions can be mistaken for an authentication result.
#[derive(Debug, Clone, Copy)]
pub struct AuthenticatedTenant(pub TenantHash);

/// The per-deployment inputs to the checks above.
#[derive(Debug, Clone)]
pub(crate) struct McpAuth {
    /// Allowed browser origins, as `scheme://host[:port]`. Empty disables the
    /// check, which startup validation permits only on a loopback listener no
    /// page on another site can reach.
    pub allowed_origins: Vec<String>,
    pub max_body_bytes: usize,
}

/// Run every check and return the request the protocol layer may serve, or the
/// response that refuses it.
pub(crate) async fn check(
    auth: &McpAuth,
    service: &QueryService,
    request: Request<Body>,
) -> Result<Request<Body>, Response> {
    let (mut parts, body) = request.into_parts();

    check_origin(auth, &parts.headers).map_err(IntoResponse::into_response)?;
    let tenant = service.authenticate(&parts.headers).map_err(refuse)?;
    let bytes = read_body(auth, body)
        .await
        .map_err(IntoResponse::into_response)?;
    check_standard_headers(&parts.headers, &bytes).map_err(IntoResponse::into_response)?;

    parts.extensions.insert(AuthenticatedTenant(tenant));
    Ok(Request::from_parts(parts, Body::from(bytes)))
}

/// An `Origin` outside the allowlist is refused. A request without the header
/// is not a browser request and carries no ambient credential to abuse, so it
/// passes; an allowlist entry is compared whole, so a host that merely ends
/// with an allowed name does not match it.
fn check_origin(auth: &McpAuth, headers: &HeaderMap) -> Result<(), ApiError> {
    if auth.allowed_origins.is_empty() {
        return Ok(());
    }
    let Some(origin) = headers.get(ORIGIN) else {
        return Ok(());
    };
    let origin = origin
        .to_str()
        .map_err(|_| forbidden("the Origin header is not valid text".to_string()))?;
    if auth
        .allowed_origins
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(origin))
    {
        Ok(())
    } else {
        Err(forbidden(format!("origin {origin:?} is not allowed")))
    }
}

/// Buffer the body, refusing anything past the cap.
///
/// The cap is enforced on what was actually read, not on `Content-Length`, so
/// a chunked body that understates its size is still bounded.
async fn read_body(auth: &McpAuth, body: Body) -> Result<Bytes, ApiError> {
    axum::body::to_bytes(body, auth.max_body_bytes)
        .await
        .map_err(|_| ApiError {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            error_type: "payload_too_large",
            message: format!(
                "request body exceeds the {} byte limit",
                auth.max_body_bytes
            ),
        })
}

/// SEP-2243: on the current revision, the headers restating the request's
/// method and target must agree with the body.
///
/// A request that names no revision is a legacy client (the header is
/// `2026-07-28`'s own addition), and an `initialize` is exempt on every
/// revision: it is the message that establishes which revision applies, so it
/// cannot be judged against one.
fn check_standard_headers(headers: &HeaderMap, body: &Bytes) -> Result<(), ApiError> {
    let revision = match headers.get(PROTOCOL_VERSION) {
        None => return Ok(()),
        Some(value) => value
            .to_str()
            .map_err(|_| bad_request("the MCP-Protocol-Version header is not valid text"))?,
    };
    if !SUPPORTED_REVISIONS
        .iter()
        .any(|supported| supported.as_str() == revision)
    {
        return Err(bad_request(&format!(
            "unsupported MCP protocol version {revision:?}"
        )));
    }
    if revision != ProtocolVersion::V_2026_07_28.as_str() {
        return Ok(());
    }

    let Some((method, name)) = intent(body) else {
        // Not a single JSON-RPC request object: there is no stated method to
        // compare a header against, and rmcp's own parse is the authority on
        // whether the body is a legal message at all.
        return Ok(());
    };
    if method == "initialize" {
        return Ok(());
    }

    match header(headers, &MCP_METHOD) {
        Some(stated) if stated == method => {}
        Some(stated) => {
            return Err(bad_request(&format!(
                "Mcp-Method header {stated:?} does not match the request method {method:?}"
            )));
        }
        None => return Err(bad_request("the Mcp-Method header is required")),
    }
    let Some(name) = name else {
        return Ok(());
    };
    match header(headers, &MCP_NAME) {
        Some(stated) if stated == name => Ok(()),
        Some(stated) => Err(bad_request(&format!(
            "Mcp-Name header {stated:?} does not match the requested name {name:?}"
        ))),
        None => Err(bad_request("the Mcp-Name header is required")),
    }
}

/// The method the body states, and the target name when it states one. `None`
/// when the body is not a single JSON-RPC request object.
fn intent(body: &Bytes) -> Option<(String, Option<String>)> {
    let message: Value = serde_json::from_slice(body).ok()?;
    let method = message.get("method")?.as_str()?.to_string();
    let name = message
        .get("params")
        .and_then(|params| params.get("name"))
        .and_then(Value::as_str)
        .map(str::to_string);
    Some((method, name))
}

fn header(headers: &HeaderMap, name: &HeaderName) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

/// The refusal the HTTP query routes return for the same failure, so a caller
/// reading both surfaces sees one error contract.
fn refuse(error: ServiceError) -> Response {
    ApiError::from(error).into_response()
}

fn forbidden(message: String) -> ApiError {
    ApiError {
        status: StatusCode::FORBIDDEN,
        error_type: "forbidden",
        message,
    }
}

fn bad_request(message: &str) -> ApiError {
    ApiError::bad_request(message.to_string())
}
