//! Differential proptest proving the spans columnar fast path and the row
//! fallback path produce identical record batches over every projection
//! subset, including erasure-active scenarios where the fast path must not
//! run.
