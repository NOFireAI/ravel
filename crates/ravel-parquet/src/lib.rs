//! Parquet tables as DataFusion tables (ADR-2040 decision D3).
//!
//! This crate is the only one that will need DataFusion's Parquet support;
//! `ravel-query` stays free of it. It holds nothing yet. Per D3 it will
//! provide:
//!
//! - a `ParquetFileReaderFactory` and `AsyncFileReader` that read through the
//!   process-wide `GetLimiter` and `ReadCache`, charging footer reads to the
//!   Probe phase and data reads to the Scan phase;
//! - a decoded-metadata cache owned by Ravel, bounded in bytes, keyed by
//!   tenant hash and content hash, outliving the per-query session;
//! - a `TenantParquetStore` (the `object_store` 0.13 trait) that serves
//!   `head` from the manifest's sizes and refuses every path outside the
//!   manifest, every write and every list;
//! - a `ParquetTableProvider` that builds its scan from a manifest resolved
//!   by `ravel-pqtable` before the session is built, and never lists the
//!   store while planning.
