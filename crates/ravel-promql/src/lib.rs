//! PromQL front end and evaluator (ADR-0007).
//!
//! Phase 1 scope: instant and range queries over vector selectors (all
//! matcher types, offset), 5m lookback, staleness-aware iteration. The
//! evaluator consumes a storage-agnostic series stream trait.

mod eval;
mod matchers;
mod source;
pub mod testsource;

pub use eval::{
    DEFAULT_MAX_RANGE_POINTS, Error, Evaluator, InstantSample, InstantVector, RangeMatrix,
    ms_to_ns, ns_to_ms_floor,
};
pub use matchers::{from_ast_matcher, from_ast_matchers, has_or_group, matches_series};
pub use source::{LabelMatcher, MatchOp, MatcherError, SeriesData, SeriesSource, SourceError};
