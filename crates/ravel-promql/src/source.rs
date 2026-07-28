//! Storage abstraction the evaluator queries against (ADR-0006).
//!
//! `SeriesSource` is deliberately synchronous. Phase 1's query engine
//! resolves a snapshot, prunes segments and series by matchers/time range,
//! and reads the matched pages into memory *before* handing anything to the
//! PromQL evaluator (docs/query-engine.md "Flow"): by the time `query` is
//! called, the window is already materialized as plain Rust structs, so
//! there is nothing left to await. A future streaming or Arrow-backed
//! engine (ADR-0006 Phase 3) can still implement this trait; only the
//! materialization strategy changes, not the contract.

use ravel_types::{LabelSet, Sample};
use regex::Regex;

use crate::histogram::FloatHistogram;

/// One matched series: its label set and samples.
///
/// `samples` MUST be sorted ascending by [`Sample::ts_ns`]. Implementors are
/// responsible for the sort; the evaluator relies on it (binary search) and
/// does not re-sort.
///
/// Implementors SHOULD deliver at most one sample per `(series, ts)`, already
/// resolved under the normative commit total order (ADR-0010 §5:
/// `created_unix_ns`, `writer_epoch`, `writer_seq`, in-page index; greatest
/// `value.to_bits()` on a full tie). That resolution belongs upstream: in the
/// real pipeline the query engine's lazy k-way merge (docs/query-engine.md)
/// dedups to one sample per ts *before* building any `SeriesData`, because it
/// is the only layer that carries the commit provenance the total order ranks
/// on. A [`Sample`] here carries only `ts_ns` and `value`, so this surface
/// cannot reconstruct that order and does not define a competing one.
///
/// Should an implementor nonetheless pass raw duplicates, the evaluator
/// resolves them by the one component of the normative order the values alone
/// carry, greatest `value.to_bits()` (see `eval::pick_sample`), never by
/// vector position. That is defense-in-depth, not a second tiebreak rule: it
/// is the normative order's own final comparison, so it can never contradict
/// the deduped input the engine hands over.
#[derive(Debug, Clone, PartialEq)]
pub struct SeriesData {
    pub labels: LabelSet,
    pub samples: Vec<Sample>,
}

/// One native-histogram sample: an event timestamp paired with a native
/// histogram value, the histogram counterpart of [`Sample`]
/// (`ravel_segment::HistogramSample` converted to the evaluator's float
/// working form, P11). `value` is already the float model the evaluator
/// operates on; the read path converts integer histograms to float before
/// building these, exactly as Prometheus does before evaluation.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSample {
    pub ts_ns: i64,
    pub value: FloatHistogram,
}

/// One matched native-histogram series: its label set and histogram samples,
/// the histogram counterpart of [`SeriesData`] (P11). Kept a separate type,
/// and surfaced through a separate [`SeriesSource::query_histograms`] method,
/// rather than widening [`SeriesData`]: `SeriesData`'s two-field shape is a
/// struct literal in `ravel-query`'s merge path, so adding a field there would
/// be a cross-crate breaking change, whereas a new type and a defaulted trait
/// method are additive.
///
/// `samples` MUST be sorted ascending by `ts_ns`, the same contract
/// [`SeriesData`] carries.
#[derive(Debug, Clone, PartialEq)]
pub struct HistogramSeriesData {
    pub labels: LabelSet,
    pub samples: Vec<HistogramSample>,
}

/// Prometheus label-matcher operator. `Re`/`Nre` carry the compiled,
/// fully-anchored pattern (`^(?:value)$`, exactly Prometheus' own anchoring
/// rule) so matching never recompiles a regex per series.
#[derive(Debug, Clone)]
pub enum MatchOp {
    Eq,
    Ne,
    Re(Regex),
    Nre(Regex),
}

impl PartialEq for MatchOp {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (MatchOp::Eq, MatchOp::Eq) | (MatchOp::Ne, MatchOp::Ne) => true,
            (MatchOp::Re(a), MatchOp::Re(b)) | (MatchOp::Nre(a), MatchOp::Nre(b)) => {
                a.as_str() == b.as_str()
            }
            _ => false,
        }
    }
}

impl Eq for MatchOp {}

/// One label matcher. `value` is always the literal operand text as written
/// in the query (the raw pattern for `Re`/`Nre`, not modified by anchoring).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    pub name: String,
    pub op: MatchOp,
    pub value: String,
}

/// A regex pattern failed to compile as a fully-anchored Prometheus matcher.
#[derive(Debug, thiserror::Error)]
#[error("invalid regex pattern {pattern:?}: {reason}")]
pub struct MatcherError {
    pub pattern: String,
    pub reason: String,
}

impl LabelMatcher {
    pub fn equal(name: impl Into<String>, value: impl Into<String>) -> Self {
        LabelMatcher {
            name: name.into(),
            op: MatchOp::Eq,
            value: value.into(),
        }
    }

    pub fn not_equal(name: impl Into<String>, value: impl Into<String>) -> Self {
        LabelMatcher {
            name: name.into(),
            op: MatchOp::Ne,
            value: value.into(),
        }
    }

    /// Compile `pattern` anchored exactly as Prometheus does
    /// (`^(?:pattern)$`), delegating to promql-parser's own matcher
    /// construction so directly-built matchers anchor identically to
    /// matchers parsed from query text (including its Go-repeat-escaping
    /// fallback for patterns like `a{3}` written the Go way).
    pub fn regex(
        name: impl Into<String>,
        pattern: impl Into<String>,
    ) -> Result<Self, MatcherError> {
        Self::compiled(
            name,
            pattern,
            promql_parser::parser::token::T_EQL_REGEX,
            MatchOp::Re,
        )
    }

    /// As [`LabelMatcher::regex`] but for `!~`.
    pub fn not_regex(
        name: impl Into<String>,
        pattern: impl Into<String>,
    ) -> Result<Self, MatcherError> {
        Self::compiled(
            name,
            pattern,
            promql_parser::parser::token::T_NEQ_REGEX,
            MatchOp::Nre,
        )
    }

    fn compiled(
        name: impl Into<String>,
        pattern: impl Into<String>,
        token: promql_parser::parser::token::TokenId,
        wrap: impl FnOnce(Regex) -> MatchOp,
    ) -> Result<Self, MatcherError> {
        let name = name.into();
        let pattern = pattern.into();
        let matcher =
            promql_parser::label::Matcher::new_matcher(token, name.clone(), pattern.clone())
                .map_err(|reason| MatcherError {
                    pattern: pattern.clone(),
                    reason,
                })?;
        match matcher.op {
            promql_parser::label::MatchOp::Re(re) | promql_parser::label::MatchOp::NotRe(re) => {
                Ok(LabelMatcher {
                    name,
                    op: wrap(re),
                    value: pattern,
                })
            }
            _ => Err(MatcherError {
                pattern,
                reason: "promql-parser did not return a regex match op".to_string(),
            }),
        }
    }
}

/// Errors from a `SeriesSource` backend. Deliberately coarse in Phase 1;
/// real backends (segment reads, catalog resolution) will refine this once
/// those layers land.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SourceError {
    #[error("series source error: {0}")]
    Backend(String),
}

/// Storage abstraction queried by [`crate::eval::Evaluator`]. See the module
/// doc for the synchronous-by-design rationale.
///
/// `Send + Sync` so implementations can be held behind an `Arc` and shared
/// across the concurrency-bounded segment fetches the query engine performs
/// per query (docs/query-engine.md), matching the same bound on this
/// workspace's other storage trait, `ObjectStoreBackend`.
pub trait SeriesSource: Send + Sync {
    /// Return every series matching all of `matchers`, with samples
    /// restricted to `window` (inclusive on both ends, matching
    /// [`ravel_types::TimeRange`]'s own contract).
    fn query(
        &self,
        matchers: &[LabelMatcher],
        window: ravel_types::TimeRange,
    ) -> Result<Vec<SeriesData>, SourceError>;

    /// Return every native-histogram series matching all of `matchers`, with
    /// samples restricted to `window` (P11). Defaulted to empty so existing
    /// scalar-only sources (e.g. `ravel-query`'s `MergedSource`, which does
    /// not decode RSEG HIST_PAGES yet) need no change; a source that carries
    /// native histograms overrides it. A native-histogram series and a float
    /// series never share a `(series, ts)`: storage keeps a series' value kind
    /// fixed, so the evaluator can union the two result sets without a
    /// tiebreak.
    fn query_histograms(
        &self,
        _matchers: &[LabelMatcher],
        _window: ravel_types::TimeRange,
    ) -> Result<Vec<HistogramSeriesData>, SourceError> {
        Ok(Vec::new())
    }
}
