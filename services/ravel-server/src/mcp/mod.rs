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
//! Nothing here spawns. The tool future runs on the transport's own task, so
//! a dropped stream drops the engine call, and the usage guard inside the
//! query service bills the dropped future rather than losing it (ADR-1374
//! decision 7).

mod adapter;
mod auth;
mod envelope;
mod service_impl;

pub use adapter::{McpSettings, router};
pub use auth::AuthenticatedTenant;
pub use service_impl::ServiceBackend;
