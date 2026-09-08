//! Transport-independent MCP tool layer (ADR-1374).
//!
//! This crate owns the result envelope, the cursor and evidence-reference
//! codec, the effective-budget clamp, the compact text renderer, and the
//! nine-tool catalog. It touches no transport and no object storage: the
//! adapter that mounts these tools on `rmcp::StreamableHttpService` and the
//! service layer that resolves tenants and calls the query engines both
//! land in a later wave (#1381).

pub mod budget;
pub mod catalog;
pub mod compact;
pub mod cursor;
pub mod envelope;
pub mod service;
