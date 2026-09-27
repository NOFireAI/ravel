//! Parquet tables queried in place (ADR-2040): the persistent format and the
//! object-storage layer, with no Arrow, DataFusion or Parquet dependency.
//!
//! - [`names`] validates dataset paths and table names (D1, D2).
//! - [`keys`] builds and parses the two key shapes under `t/<tenant_hash>/pq/`:
//!   content-addressed data objects and table manifest versions (D1).
//! - [`manifest`] encodes and decodes `ParquetTableManifest` behind a strict
//!   `format_version` read window (ADR-0066 decision 4, R1 amendment).
//! - [`resolve`] finds a table's newest manifest and lists a dataset.
//! - [`writer`] applies a CREATE / CREATE OR REPLACE / DROP intent as the next
//!   manifest version, replaying the intent when it loses a race (D2).
//! - [`upload`] writes a local file as a content-addressed data object.
//! - [`sweep`] plans and executes deletion of superseded manifest versions and
//!   unreferenced data objects past a grace period.

pub mod keys;
pub mod manifest;
pub mod names;
pub mod resolve;
pub mod sweep;
pub mod upload;
pub mod writer;

#[cfg(test)]
pub(crate) mod test_util;
