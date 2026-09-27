//! Parquet tables queried in place (ADR-2040): the persistent format and the
//! object-storage layer, with no Arrow, DataFusion or Parquet dependency.
//!
//! Ravel never copies a tenant's Parquet files. A table names a LOCATION in
//! the tenant's own bucket, an operator's grant admits that location, and a
//! manifest version pins each file by (profile, bucket, raw key) plus its ETag
//! and store version.
//!
//! - [`names`] validates table names (D1, D2).
//! - [`keys`] builds and parses the two key shapes under `t/<tenant_hash>/pq/`:
//!   the grants record and table manifest versions (D1).
//! - [`grants`] reads and updates the location grants record, and resolves a
//!   LOCATION URL to the single grant that admits it.
//! - [`manifest`] encodes and decodes `ParquetTableManifest` behind a strict
//!   `format_version` read window (ADR-0066 decision 4, R1 amendment).
//! - [`resolve`] finds a table's newest manifest version.
//! - [`writer`] applies a CREATE / CREATE OR REPLACE / DROP intent as the next
//!   manifest version, replaying the intent when it loses a race (D2).
//! - [`sweep`] plans and executes deletion of manifest versions superseded for
//!   longer than a grace period.
//! - [`clock`] is the injected time source the writer and grants use.

pub mod clock;
pub mod grants;
pub mod keys;
pub mod manifest;
pub mod names;
pub mod resolve;
pub mod sweep;
pub mod writer;

#[cfg(test)]
pub(crate) mod test_util;
