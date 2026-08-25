//! Differential proptest proving the spans columnar fast path and the row
//! fallback path produce identical Arrow batches over every projection subset.
//!
//! See docs/adrs/0110-columnar-spans-scan.md decisions 3 and 7.
