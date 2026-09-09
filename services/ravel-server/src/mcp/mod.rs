//! The native MCP adapter: `POST /mcp` on the query router (ADR-1374).
//!
//! One file per boundary the request crosses:
//!
//! - [`auth`] is everything that must happen before rmcp sees a byte: the
//!   deployment's own tenant resolver, the `Origin` allowlist, the body cap,
//!   and the per-revision standard-header check. A request that fails any of
//!   them never reaches the protocol layer, so an unauthenticated caller
//!   cannot make this process read object storage or take an admission
//!   permit.
//! - [`adapter`] is the [`rmcp::ServerHandler`]: it turns a `tools/call` into
//!   a [`ravel_mcp::tools::dispatch`] call and the resulting envelope into an
//!   MCP result, and it owns the process-local cursor key and the effective
//!   budget ceilings.
//! - [`service_impl`] is the [`ravel_mcp::service::QueryBackend`] port over
//!   [`crate::service::QueryService`], so a tool body reaches the same seven
//!   controls (admission, deadline, cost, usage, audit, partial gate,
//!   redaction) every HTTP query route runs through, exactly once.
//! - [`envelope`] turns one service outcome into one D4 envelope: the rows,
//!   the coverage, and the budget block whose figures name the basis they
//!   were measured on.
//!
//! Nothing in this module spawns between the transport and the engine call:
//! the tool future is awaited on whichever task rmcp dispatched the request
//! on, beside that request's cancellation token (ADR-1374 decision 7). What
//! cancels that token differs by revision. A current-revision request is
//! routed statelessly and rmcp arms a disconnect guard on its token until the
//! handler emits its first message, so a client that drops the stream mid-call
//! drops the tool future. A legacy-revision request runs under
//! `LocalSessionManager`, which serves the session on a spawned worker whose
//! lifetime is the session's rather than the response stream's, so only a
//! `notifications/cancelled` drops the tool future: a legacy client that just
//! disconnects holds its admission permit until the call's deadline. Either
//! way, when the future is dropped the usage guard inside the query service
//! bills what the call had already spent rather than losing it.

mod adapter;
mod auth;
mod envelope;
mod service_impl;

pub use adapter::{McpSettings, router};
pub use auth::AuthenticatedTenant;
pub use service_impl::ServiceBackend;
