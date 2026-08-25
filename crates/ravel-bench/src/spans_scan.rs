//! Spans SQL scan bench lane: measures the columnar fast path against the row
//! path over one shared corpus (issue #641, epic #630, ADR-0110 decision 7).
//!
//! Epic #630 gave the SQL spans scan a columnar fast path. It is eligible when
//! the projection excludes the `attrs` map column, no pending erasure predicate
//! applies, and the block carries no `attrs_raw` overflow page; otherwise the
//! row path runs unchanged. This lane drives a `SpansScanExec` twice over the
//! same block: once with a projection that EXCLUDES `attrs` (columnar path) and
//! once with a projection that INCLUDES it (row path), reporting rows/second
//! and `pages_decoded` for each shape plus the ratio between them through the
//! standard bench provenance and report machinery.
