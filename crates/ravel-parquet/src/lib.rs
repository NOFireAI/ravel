//! Parquet tables as DataFusion tables (ADR-2040 decision D3).
//!
//! This crate is the only one that needs DataFusion's Parquet support;
//! `ravel-query` stays free of it. It provides:
//!
//! - [`PinnedParquetReader`], an `AsyncFileReader` for one manifest file that
//!   reads through the process-wide `GetLimiter` and `ReadCache`, pins every
//!   read to the manifest's ETag and version, and charges the footer read to
//!   the Probe phase and every other read to the Scan phase;
//!   [`PinnedReaderFactory`] builds one per file DataFusion opens;
//! - [`MetadataCache`], a decoded-footer cache owned by Ravel, bounded in
//!   bytes, keyed by tenant hash and pinned identity, held outside any
//!   per-query session;
//! - [`TenantParquetStore`] (the `object_store` 0.13 trait) that serves `head`
//!   from the manifest's sizes and refuses every other path, every read,
//!   every write and every list, and [`SingleStoreRegistry`], which answers
//!   only that store's URL;
//! - [`ParquetTableProvider`], which builds its scan from a manifest resolved
//!   by `ravel-pqtable` before the session is built, and never lists the
//!   store while planning.
//!
//! `ravel-sql`'s executor builds one [`ParquetTableProvider`] per Parquet
//! table a statement names, after resolving its manifest, and a session over
//! them whose registry is a [`SingleStoreRegistry`].

mod error;
mod metadata_cache;
mod provider;
mod reader;
mod store;

#[cfg(test)]
mod test_support;

pub use error::{ParquetReadError, ParquetTableError};
pub use metadata_cache::{MetadataCache, MetadataKey};
pub use provider::{Cast, ParquetTableProvider, TableOptions};
pub use reader::{PinnedFile, PinnedParquetReader, PinnedReaderFactory, ReadServices};
pub use store::{SingleStoreRegistry, TenantParquetStore, file_path, store_url};
