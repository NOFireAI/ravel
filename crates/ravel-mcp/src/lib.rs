//! Transport-independent MCP tool layer (ADR-1374).
//!
//! This crate owns the result envelope, the cursor and evidence-reference
//! codec, the effective-budget clamp, the compact text renderer, the
//! nine-tool catalog, and the dispatch that routes a catalog name to its
//! tool body. It touches no transport and no object storage: the adapter
//! that mounts these tools on `rmcp::StreamableHttpService` lives in
//! `ravel-server`, and the store reads a tool body needs go through the
//! [`service::QueryBackend`] port it is handed.

pub mod budget;
pub mod catalog;
pub mod compact;
pub mod cursor;
pub mod envelope;
pub mod service;
pub mod tools;
