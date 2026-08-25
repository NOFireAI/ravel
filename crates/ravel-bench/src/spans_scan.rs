//! Spans SQL scan bench lane (ADR-0110 decision 7): measure the columnar and
//! row paths of `SpansScanExec` over one corpus.
//!
//! Two projections run over the same generated spans: one that excludes the
//! `attrs` map column (the columnar fast path) and one that includes it (the
//! row path). The lane reports rows/second and `pages_decoded` for each shape
//! plus their ratio through the standard bench provenance and report machinery,
//! and asserts each shape actually took the path it claims via the scan's
//! `columnar_batches` / `rowpath_batches` partition metrics.
