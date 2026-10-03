//! Re-export of the span fetch surface, promoted to `ravel-query`
//! (`ravel_query::span_fetcher`) for the distributed Spans fan-out (#285,
//! ADR-0071 "log and span distributed fan-out" amendment).
//!
//! `SpanSegmentFetcher` originally lived here because ADR-0041 phase 5 was the
//! `spans` SQL table alone and no span query surface existed in `ravel-query`.
//! The distributed fan-out worker lives in `ravel-query`, and `ravel-sql`
//! depends on `ravel-query`, so the worker could not use a fetcher that lived
//! here; the fetcher was moved into `ravel-query` unchanged. This module keeps
//! the `crate::spans_fetcher::*` path (and the crate's public
//! `SpanSegmentFetcher`/`SpanRow`/`SpanFetchOutput`/`SpanFetchError` exports)
//! working through a thin re-export, so no `spans` scan/session/executor call
//! site had to change.

pub use ravel_query::span_fetcher::{SpanFetchError, SpanFetchOutput, SpanRow, SpanSegmentFetcher};
