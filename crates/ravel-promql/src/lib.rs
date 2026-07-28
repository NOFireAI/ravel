//! PromQL front end and evaluator (ADR-0007, ADR-0021).
//!
//! A typed recursive-descent evaluator over the promql-parser AST: paren
//! expressions, unary minus, number/string literals, and vector/matrix
//! selectors (all matcher types, offset, the `@` modifier), 5m default
//! lookback, staleness-aware iteration, and a resolution cap on range
//! queries. The counter/regression function family (`rate`, `irate`,
//! `increase`, `delta`, `idelta`, `resets`, `changes`, `deriv`,
//! `predict_linear`) is evaluated via [`functions`]; binary operators
//! (arithmetic, comparison, vector matching, set operators) are evaluated
//! via `binop`; aggregation operators (`sum`, `avg`, `min`, `max`, `count`,
//! `group`, `stddev`, `stdvar`, `topk`, `bottomk`, `quantile`,
//! `count_values`) are evaluated via `aggregate`. Subqueries and every other
//! function are not yet evaluated (`Error::Unsupported`), but
//! [`plan_selectors`] still reports every selector reachable through them,
//! for a future phase's prefetch. The evaluator consumes a storage-agnostic
//! series stream trait.

mod aggregate;
mod binop;
mod eval;
mod functions;
mod matchers;
mod plan;
mod source;
pub mod testsource;

pub use eval::{
    DEFAULT_LOOKBACK_NS, DEFAULT_MAX_RANGE_POINTS, Error, Evaluator, InstantSample, InstantVector,
    RangeMatrix, Value, ms_to_ns, ns_to_ms_floor,
};
pub use matchers::{from_ast_matcher, from_ast_matchers, has_or_group, matches_series};
pub use plan::{PlanAnchor, SelectorPlan, plan_selectors};
pub use source::{LabelMatcher, MatchOp, MatcherError, SeriesData, SeriesSource, SourceError};
