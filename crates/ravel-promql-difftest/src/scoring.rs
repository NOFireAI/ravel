//! Conformance scoring for the PromQL surface (ADR-0035).
//!
//! [`REGISTRY`] enumerates every stable Prometheus PromQL function and every
//! major promql-parser AST construct category, each mapped to exactly one
//! [`ConstructState`]: ADR-0035's three-state classification plus the
//! accepted-divergence state that keeps ADR-0025's and ADR-0030's closed
//! investigations out of the miss bucket.
//!
//! The published state is not the registered state. [`ConformanceReport`]
//! recomputes it from a run: which corpus entries actually exercise each
//! construct ([`Probe`] against the corpus' own query text), whether those
//! entries actually passed on the Ravel side ([`run_ravel_only`]) and against
//! the pinned Prometheus binary ([`ConformanceReport::apply_run_report`]), and
//! whether each intentionally-rejected construct actually rejected
//! ([`ConformanceReport::apply_rejection_results`]). A construct registered as
//! supported whose evidence is missing or failing publishes as
//! [`ConstructState::Unclassified`], so the table cannot claim coverage a run
//! does not show. That is ADR-0035's "the score is computed, not
//! hand-maintained".
//!
//! Scope of the enumerated surface: the 72 functions promql-parser 0.10 marks
//! non-experimental, the 12 non-experimental aggregation operators, all 16
//! binary operators, and the AST node and modifier categories below.
//! Prometheus' experimental functions and aggregators (`limitk`,
//! `limit_ratio`, `info`, `double_exponential_smoothing`, the `*_of_*` and
//! `ts_of_*` family, `sort_by_label*`, `mad_over_time`, `first_over_time`,
//! `histogram_quantiles`) are out of the scored surface: they are not part of
//! the stable language, so scoring against them would move the denominator
//! every time Prometheus promotes one. `Expr::Extension` is likewise out of
//! scope: it is promql-parser's own hook for downstream dialects, has no
//! Prometheus counterpart, and Ravel rejects it with a typed
//! `Error::Unsupported` at `eval.rs`'s `unsupported_construct_error`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use axum::Router;
use serde_json::Value as Json;

use crate::corpus::{ComparisonMode, CorpusEntry, Eval};
use crate::runner::{RunReport, ravel_instant_query, ravel_range_query};

/// The marker opening the generated block in `docs/query-engine.md`.
pub const BEGIN_MARKER: &str = "<!-- BEGIN GENERATED PROMQL CONFORMANCE TABLE -->";
/// The marker closing the generated block in `docs/query-engine.md`.
pub const END_MARKER: &str = "<!-- END GENERATED PROMQL CONFORMANCE TABLE -->";

/// Prefix of a [`ConstructState::Supported`] `test` naming a corpus file in
/// this crate. The run must show at least one entry from that exact file
/// exercising the construct, or the row publishes as unclassified.
pub const CORPUS_TEST_PREFIX: &str = "corpus/";
/// Prefix of a [`ConstructState::Supported`] `test` naming a `ravel-promql`
/// unit test instead of a corpus file: the construct is proven there, but this
/// crate's differential corpus does not exercise it, which the generated table
/// reports as a corpus gap.
pub const UNIT_TEST_PREFIX: &str = "ravel-promql:";

/// The stable `errorType` tags `ravel-query`'s HTTP layer emits. A rejection
/// carrying anything else is not the typed error the table claims, and a
/// registry row declaring anything else names an error the endpoint cannot
/// produce.
pub const TYPED_ERROR_TAGS: &[&str] = &[
    "bad_data",
    "execution",
    "internal",
    "unavailable",
    "timeout",
    "unauthorized",
];

/// The machine-checkable half of a [`ConstructState::IntentionallyRejected`]
/// row's `typed_error` string: the HTTP status the endpoint answers with and
/// the `errorType` tag its envelope carries.
///
/// The prose half (`"Unsupported: subquery over native histograms"`) is what a
/// reader of the table needs; this pair is what a run can compare against, so
/// a construct that regresses from one typed-error variant to another (422
/// `execution` to 500 `internal`, say) fails the rejection check instead of
/// passing as "some typed error came back".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TypedErrorTag {
    /// The HTTP status code the rejection must answer with.
    pub status: u16,
    /// The `errorType` tag the error envelope must carry.
    pub error_type: &'static str,
}

impl fmt::Display for TypedErrorTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.status, self.error_type)
    }
}

/// Parses the `(<status> <errorType>)` suffix every `typed_error` string ends
/// with, e.g. `"Unsupported: function call (422 execution)"`.
///
/// Strict on purpose: an unparseable suffix, a status outside the 4xx/5xx
/// range, or an `errorType` outside [`TYPED_ERROR_TAGS`] is a malformed
/// registry row, not a row to check loosely.
pub fn parse_typed_error(typed_error: &'static str) -> Result<TypedErrorTag, ScoringError> {
    let malformed = |why: &str| {
        ScoringError::MalformedTypedError(format!(
            "typed error '{typed_error}' {why}; expected a trailing \
             '(<status> <errorType>)', e.g. '(422 execution)'"
        ))
    };
    let trimmed = typed_error.trim_end();
    let body = trimmed
        .strip_suffix(')')
        .ok_or_else(|| malformed("does not end with ')'"))?;
    let open = body
        .rfind('(')
        .ok_or_else(|| malformed("has no opening '('"))?;
    let inside = &body[open + 1..];
    let (status, tag) = inside
        .split_once(' ')
        .ok_or_else(|| malformed("does not carry '<status> <errorType>'"))?;
    let status: u16 = status
        .trim()
        .parse()
        .map_err(|_| malformed("does not start with a numeric HTTP status"))?;
    if !(400..600).contains(&status) {
        return Err(malformed("declares a status outside the 4xx/5xx range"));
    }
    let tag = tag.trim();
    let error_type = TYPED_ERROR_TAGS
        .iter()
        .find(|known| **known == tag)
        .ok_or_else(|| malformed("names an errorType that is not a stable tag"))?;
    Ok(TypedErrorTag { status, error_type })
}

/// The [`REGISTRY`] row with this name, or `None`.
pub fn registry_entry(name: &str) -> Option<&'static Construct> {
    REGISTRY.iter().find(|c| c.name == name)
}

/// The [`TypedErrorTag`] a [`RejectionCase`]'s construct declares.
///
/// A rejection case whose construct is missing from the registry, or whose
/// declared `typed_error` does not parse, is an error rather than an absent
/// expectation: without a declared tag there is nothing to compare the
/// observed rejection against.
pub fn declared_typed_error(construct: &str) -> Result<TypedErrorTag, ScoringError> {
    let entry = registry_entry(construct)
        .ok_or_else(|| ScoringError::UnknownRejectionConstruct(construct.to_string()))?;
    match entry.state {
        ConstructState::IntentionallyRejected { typed_error } => parse_typed_error(typed_error),
        other => Err(ScoringError::MalformedTypedError(format!(
            "construct '{construct}' is '{}', not intentionally rejected, so \
             it declares no typed error",
            other.slug()
        ))),
    }
}

/// Which part of the PromQL surface a construct belongs to. Only used to
/// group the generated table's rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Category {
    /// A promql-parser AST node category (`VectorSelector`, `Subquery`, ...).
    AstNode,
    /// A selector, aggregation, or vector-matching modifier.
    Modifier,
    /// A binary operator.
    BinaryOp,
    /// An aggregation operator.
    AggregationOp,
    /// A Prometheus function.
    Function,
    /// An accepted-divergence record (ADR-0025, ADR-0030).
    Divergence,
}

impl Category {
    /// The slug used in the generated table's category column.
    pub fn slug(self) -> &'static str {
        match self {
            Category::AstNode => "ast node",
            Category::Modifier => "modifier",
            Category::BinaryOp => "binary operator",
            Category::AggregationOp => "aggregation operator",
            Category::Function => "function",
            Category::Divergence => "accepted divergence",
        }
    }
}

impl fmt::Display for Category {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.slug())
    }
}

/// ADR-0035's classification. Exactly one per [`Construct`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConstructState {
    /// State 1: implemented, with a passing test proving it. `test` names
    /// that test: `corpus/<file>` for an entry in this crate's differential
    /// corpus, or `ravel-promql:<fn>` for a `ravel-promql` unit test when the
    /// corpus does not reach the construct.
    Supported { test: &'static str },
    /// State 2: refused with a typed error, never a panic and never silently
    /// wrong data. `typed_error` names the error the refusal produces.
    IntentionallyRejected { typed_error: &'static str },
    /// Neither a miss nor plain support: a divergence from Prometheus that an
    /// ADR already investigated and accepted. `adr` cites it.
    AcceptedDivergence { adr: &'static str },
    /// State 3: implemented but untested, or claimed-supported but actually
    /// wrong. The actionable miss category; every row here is a ticket.
    Unclassified,
}

impl ConstructState {
    /// The slug used in the generated table's state column.
    pub fn slug(&self) -> &'static str {
        match self {
            ConstructState::Supported { .. } => "supported",
            ConstructState::IntentionallyRejected { .. } => "intentionally rejected",
            ConstructState::AcceptedDivergence { .. } => "accepted divergence",
            ConstructState::Unclassified => "unclassified",
        }
    }

    /// Whether this state counts toward ADR-0035's score: states 1 and 2 plus
    /// the accepted-divergence state are all positions Ravel has taken
    /// deliberately and verified; only [`ConstructState::Unclassified`] is a
    /// miss.
    pub fn is_conformant(&self) -> bool {
        !matches!(self, ConstructState::Unclassified)
    }
}

/// How a corpus query's token stream is asked whether it exercises a
/// construct. Every probe runs against [`lex`]'s output, never a raw
/// substring search, so a label-matcher `!=` is never mistaken for the binary
/// operator and a metric named `rate_limit` never counts as a `rate` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Probe {
    /// A function call: this identifier immediately followed by `(`.
    Call(&'static str),
    /// An aggregation operator: this identifier followed by `(`, `by`, or
    /// `without`.
    Aggregation(&'static str),
    /// A bare keyword identifier outside a label-matcher brace group
    /// (`by`, `without`, `on`, `ignoring`, `group_left`, `bool`, `and`, ...).
    Keyword(&'static str),
    /// A binary operator symbol in infix position outside a brace group.
    InfixOp(&'static str),
    /// A label-matcher operator inside a brace group.
    MatcherOp(&'static str),
    /// A structural shape of the token stream.
    Shape(Shape),
    /// Any entry carrying a `tolerance:` field (ADR-0025's allowlist).
    ToleranceEntry,
    /// Any entry using `mode: ravel_error_prom_success` whose `name` also
    /// contains this marker substring. The mode alone is not distinguishing:
    /// more than one accepted divergence can use it (ADR-0030's per-node
    /// point cap and issue #524's histogram-binop guard both do), so a
    /// bare mode match would double-count one divergence's corpus entries
    /// as evidence for another's `Construct` row. The marker scopes each
    /// registry entry to only the corpus entries actually naming it (e.g.
    /// `"subquery"` for ADR-0030's `error_*subquery*` entries).
    DivergenceModeEntry(&'static str),
    /// Not expressible as corpus query text. Intentionally-rejected
    /// constructs use this: their evidence is a rejection case in
    /// [`REJECTION_CASES`], not a corpus entry.
    None,
}

/// Structural token-stream shapes [`Probe::Shape`] can ask about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shape {
    /// A metric-name identifier or a bare `{...}` selector.
    VectorSelector,
    /// A `[<duration>]` range without a `:` step, i.e. a matrix selector.
    MatrixSelector,
    /// A `[<duration>:<step>]` range, i.e. a subquery.
    Subquery,
    /// A `(` that groups rather than opening a call's argument list.
    GroupingParen,
    /// A prefix `-`/`+` applied to something other than a numeric literal.
    UnaryExpr,
    /// A numeric literal that is not a duration.
    NumberLiteral,
    /// A quoted string outside a label-matcher brace group.
    StringLiteral,
    /// Any call at all.
    FunctionCall,
    /// Any infix binary operator at all.
    BinaryExpr,
    /// Any aggregation operator at all.
    AggregateExpr,
    /// An `offset` modifier.
    Offset,
    /// An `offset` modifier with a negative duration.
    NegativeOffset,
    /// An `@` modifier with a literal timestamp.
    AtTimestamp,
    /// An `@ start()` modifier.
    AtStart,
    /// An `@ end()` modifier.
    AtEnd,
}

/// One row of the conformance table.
#[derive(Debug, Clone, Copy)]
pub struct Construct {
    /// The construct's name, as PromQL spells it.
    pub name: &'static str,
    /// Which part of the surface it belongs to.
    pub category: Category,
    /// Its registered state. The published state is
    /// [`ConstructOutcome::published_state`], recomputed from a run.
    pub state: ConstructState,
    /// How a corpus query is recognized as exercising it.
    pub probe: Probe,
    /// Per-row rationale. ADR-0035 limits hand-written prose in the table to
    /// [`ConstructState::IntentionallyRejected`] rows and the divergence
    /// records; every other row leaves this empty.
    pub note: &'static str,
}

/// A `Supported` state proven by a corpus file in this crate.
const fn corpus(file: &'static str) -> ConstructState {
    ConstructState::Supported { test: file }
}

/// A `Supported` state proven by a `ravel-promql` unit test because this
/// crate's differential corpus does not reach the construct.
const fn unit(test: &'static str) -> ConstructState {
    ConstructState::Supported { test }
}

/// A `Function`-category row.
const fn func(name: &'static str, state: ConstructState) -> Construct {
    Construct {
        name,
        category: Category::Function,
        state,
        probe: Probe::Call(name),
        note: "",
    }
}

/// A rejected `Function`-category row: the name parses (promql-parser knows
/// it) but `ravel-promql`'s function registry has no entry, so dispatch
/// returns `Error::Unsupported { construct: "function call: <name>" }`, which
/// the HTTP layer renders as 422 `execution`.
const fn rejected_func(name: &'static str) -> Construct {
    Construct {
        name,
        category: Category::Function,
        state: ConstructState::IntentionallyRejected {
            typed_error: "Unsupported: function call (422 execution)",
        },
        probe: Probe::None,
        note: "not in ravel-promql's function registry; native-histogram \
               dispersion is not implemented",
    }
}

/// An `AggregationOp`-category row.
const fn agg(name: &'static str) -> Construct {
    Construct {
        name,
        category: Category::AggregationOp,
        state: corpus("corpus/aggregate.txt"),
        probe: Probe::Aggregation(name),
        note: "",
    }
}

/// A `BinaryOp`-category row matched as an infix symbol.
const fn binop(name: &'static str, state: ConstructState) -> Construct {
    Construct {
        name,
        category: Category::BinaryOp,
        state,
        probe: Probe::InfixOp(name),
        note: "",
    }
}

/// A `BinaryOp`-category row matched as a keyword (`and`, `or`, `unless`,
/// `atan2`).
const fn word_binop(name: &'static str) -> Construct {
    Construct {
        name,
        category: Category::BinaryOp,
        state: corpus("corpus/binop.txt"),
        probe: Probe::Keyword(name),
        note: "",
    }
}

/// An `AstNode`-category row matched by a structural shape.
const fn ast(name: &'static str, shape: Shape, state: ConstructState) -> Construct {
    Construct {
        name,
        category: Category::AstNode,
        state,
        probe: Probe::Shape(shape),
        note: "",
    }
}

/// A `Modifier`-category row.
const fn modifier(name: &'static str, probe: Probe, state: ConstructState) -> Construct {
    Construct {
        name,
        category: Category::Modifier,
        state,
        probe,
        note: "",
    }
}

/// A `Modifier`-category row Ravel refuses with a typed error.
const fn rejected_modifier(
    name: &'static str,
    typed_error: &'static str,
    note: &'static str,
) -> Construct {
    Construct {
        name,
        category: Category::Modifier,
        state: ConstructState::IntentionallyRejected { typed_error },
        probe: Probe::None,
        note,
    }
}

/// Every enumerated construct, one state each.
///
/// Ordered function-last so the generated table reads AST nodes, modifiers,
/// operators, then the long function list. `Category`'s `Ord` is what
/// actually groups the rendered table, so this order is only a convenience
/// for reading the source.
pub static REGISTRY: &[Construct] = &[
    // ---- AST node categories (promql-parser's `Expr` variants) ----
    ast(
        "vector selector",
        Shape::VectorSelector,
        corpus("corpus/selectors.txt"),
    ),
    ast(
        "matrix selector",
        Shape::MatrixSelector,
        corpus("corpus/selectors.txt"),
    ),
    ast("subquery", Shape::Subquery, corpus("corpus/subquery.txt")),
    ast(
        "paren expression",
        Shape::GroupingParen,
        corpus("corpus/selectors.txt"),
    ),
    ast(
        "unary expression",
        Shape::UnaryExpr,
        unit("ravel-promql:unary_minus_negates_value_and_drops_metric_name"),
    ),
    ast(
        "binary expression",
        Shape::BinaryExpr,
        corpus("corpus/binop.txt"),
    ),
    ast(
        "aggregate expression",
        Shape::AggregateExpr,
        corpus("corpus/aggregate.txt"),
    ),
    ast(
        "function call",
        Shape::FunctionCall,
        corpus("corpus/transform.txt"),
    ),
    ast(
        "number literal",
        Shape::NumberLiteral,
        corpus("corpus/binop.txt"),
    ),
    ast(
        "string literal",
        Shape::StringLiteral,
        corpus("corpus/transform.txt"),
    ),
    // ---- Selector, aggregation, and vector-matching modifiers ----
    modifier(
        "label matcher =",
        Probe::MatcherOp("="),
        corpus("corpus/selectors.txt"),
    ),
    modifier(
        "label matcher !=",
        Probe::MatcherOp("!="),
        corpus("corpus/selectors.txt"),
    ),
    modifier(
        "label matcher =~",
        Probe::MatcherOp("=~"),
        corpus("corpus/selectors.txt"),
    ),
    modifier(
        "label matcher !~",
        Probe::MatcherOp("!~"),
        corpus("corpus/selectors.txt"),
    ),
    modifier(
        "offset",
        Probe::Shape(Shape::Offset),
        corpus("corpus/selectors.txt"),
    ),
    modifier(
        "negative offset",
        Probe::Shape(Shape::NegativeOffset),
        corpus("corpus/selectors.txt"),
    ),
    modifier(
        "@ <timestamp>",
        Probe::Shape(Shape::AtTimestamp),
        corpus("corpus/selectors.txt"),
    ),
    modifier(
        "@ start()",
        Probe::Shape(Shape::AtStart),
        corpus("corpus/selectors.txt"),
    ),
    modifier(
        "@ end()",
        Probe::Shape(Shape::AtEnd),
        corpus("corpus/selectors.txt"),
    ),
    modifier("by", Probe::Keyword("by"), corpus("corpus/aggregate.txt")),
    modifier(
        "without",
        Probe::Keyword("without"),
        corpus("corpus/aggregate.txt"),
    ),
    modifier("on", Probe::Keyword("on"), corpus("corpus/binop.txt")),
    modifier(
        "ignoring",
        Probe::Keyword("ignoring"),
        corpus("corpus/binop.txt"),
    ),
    modifier(
        "group_left",
        Probe::Keyword("group_left"),
        corpus("corpus/binop.txt"),
    ),
    modifier(
        "group_right",
        Probe::Keyword("group_right"),
        corpus("corpus/binop.txt"),
    ),
    modifier("bool", Probe::Keyword("bool"), corpus("corpus/binop.txt")),
    rejected_modifier(
        "label matcher or-group",
        "Unsupported: label matcher or-group (422 execution)",
        "`{a=\"1\" or b=\"2\"}` is a Prometheus experimental matcher form; \
         ravel-promql's `matchers::has_or_group` refuses it before any \
         matching runs rather than silently dropping a branch",
    ),
    rejected_modifier(
        "vector matching fill values",
        "Unsupported: vector matching fill-in values (422 execution)",
        "`fill`/`fill_left`/`fill_right` are a promql-parser dialect \
         extension with no Prometheus counterpart; ravel-promql's `binop` \
         refuses the modifier rather than evaluating it as plain matching",
    ),
    rejected_modifier(
        "subquery over native histograms",
        "Unsupported: subquery over native histograms (422 execution)",
        "the subquery grid reducer keeps only each step's float value, so a \
         histogram element would be silently dropped; the trigger is \
         matched histogram data, not the syntactic shape",
    ),
    rejected_modifier(
        "instant matrix selector over native histograms",
        "Unsupported: instant query returning a range vector of native \
         histograms (422 execution)",
        "a bare matrix selector is a valid instant query whose top-level \
         result is a range vector; Ravel's instant `Value::Matrix` is \
         float-only (the histogram-aware channel is the range endpoint's \
         `RangeValue`, ADR-0108 decision 8), so a selector matching \
         native-histogram data refuses in `eval_expr`'s `MatrixSelector` arm \
         (issue #643) rather than silently dropping the histograms at HTTP \
         200; distinct from the subquery-over-histograms refusal, which \
         triggers inside `eval_subquery_matrix`",
    ),
    rejected_modifier(
        "binary operator over native histograms",
        "Unsupported: binary operator over native histograms (422 execution)",
        "the binop evaluator (`combine_value` and its callers) only ever \
         reads a sample's plain float `value`, which is a meaningless 0.0 \
         placeholder for a histogram element (issue #524); guarded before \
         any value combination so the fabricated-zero result is never \
         produced. `corpus/binop.txt`'s `error_*_over_histogram` entries \
         (mode: ravel_error_prom_success) additionally pin that Prometheus \
         itself succeeds here, so this is a real capability gap, not a \
         shared limitation",
    ),
    // ---- Binary operators ----
    binop("+", corpus("corpus/binop.txt")),
    binop("-", corpus("corpus/binop.txt")),
    binop("*", corpus("corpus/binop.txt")),
    binop(
        "/",
        unit("ravel-promql:scalar_scalar_arithmetic_and_bool_comparison"),
    ),
    binop("%", corpus("corpus/binop.txt")),
    binop("^", corpus("corpus/binop.txt")),
    word_binop("atan2"),
    binop("==", corpus("corpus/binop.txt")),
    binop("!=", corpus("corpus/binop.txt")),
    binop(">", corpus("corpus/binop.txt")),
    binop(
        "<",
        unit("ravel-promql:scalar_vector_filter_and_bool_both_directions"),
    ),
    binop(">=", corpus("corpus/binop.txt")),
    binop("<=", corpus("corpus/binop.txt")),
    word_binop("and"),
    word_binop("or"),
    word_binop("unless"),
    // ---- Aggregation operators (the 12 non-experimental ones) ----
    agg("sum"),
    agg("avg"),
    agg("min"),
    agg("max"),
    agg("count"),
    agg("count_values"),
    agg("group"),
    agg("stddev"),
    agg("stdvar"),
    agg("topk"),
    agg("bottomk"),
    agg("quantile"),
    // ---- Functions (the 72 non-experimental ones) ----
    func("abs", corpus("corpus/transform.txt")),
    func("absent", corpus("corpus/transform.txt")),
    func("absent_over_time", corpus("corpus/over_time.txt")),
    func("acos", corpus("corpus/transform.txt")),
    func("acosh", corpus("corpus/transform.txt")),
    func("asin", corpus("corpus/transform.txt")),
    func("asinh", corpus("corpus/transform.txt")),
    func("atan", corpus("corpus/transform.txt")),
    func("atanh", corpus("corpus/transform.txt")),
    func("avg_over_time", corpus("corpus/over_time.txt")),
    func("ceil", corpus("corpus/transform.txt")),
    func("changes", corpus("corpus/rate.txt")),
    func("clamp", corpus("corpus/transform.txt")),
    func("clamp_max", corpus("corpus/transform.txt")),
    func("clamp_min", corpus("corpus/transform.txt")),
    func("cos", corpus("corpus/transform.txt")),
    func("cosh", corpus("corpus/transform.txt")),
    func("count_over_time", corpus("corpus/over_time.txt")),
    func("day_of_month", corpus("corpus/transform.txt")),
    func("day_of_week", corpus("corpus/transform.txt")),
    func("day_of_year", corpus("corpus/transform.txt")),
    func("days_in_month", corpus("corpus/transform.txt")),
    func("deg", corpus("corpus/transform.txt")),
    func("delta", corpus("corpus/rate.txt")),
    func("deriv", corpus("corpus/rate.txt")),
    func("exp", corpus("corpus/transform.txt")),
    func("floor", corpus("corpus/transform.txt")),
    func("histogram_avg", corpus("corpus/histogram_native.txt")),
    func("histogram_count", corpus("corpus/histogram_native.txt")),
    func("histogram_fraction", corpus("corpus/histogram_classic.txt")),
    func("histogram_quantile", corpus("corpus/histogram_classic.txt")),
    rejected_func("histogram_stddev"),
    rejected_func("histogram_stdvar"),
    func("histogram_sum", corpus("corpus/histogram_native.txt")),
    func("hour", corpus("corpus/transform.txt")),
    func("idelta", corpus("corpus/rate.txt")),
    func("increase", corpus("corpus/rate.txt")),
    func("irate", corpus("corpus/rate.txt")),
    func("label_join", corpus("corpus/transform.txt")),
    func("label_replace", corpus("corpus/transform.txt")),
    func("last_over_time", corpus("corpus/over_time.txt")),
    func("ln", corpus("corpus/transform.txt")),
    func("log10", corpus("corpus/transform.txt")),
    func("log2", corpus("corpus/transform.txt")),
    func("max_over_time", corpus("corpus/over_time.txt")),
    func("min_over_time", corpus("corpus/over_time.txt")),
    func("minute", corpus("corpus/transform.txt")),
    func("month", corpus("corpus/transform.txt")),
    func("pi", corpus("corpus/transform.txt")),
    func("predict_linear", corpus("corpus/rate.txt")),
    func("present_over_time", corpus("corpus/over_time.txt")),
    func("quantile_over_time", corpus("corpus/over_time.txt")),
    func("rad", corpus("corpus/transform.txt")),
    func("rate", corpus("corpus/rate.txt")),
    func("resets", corpus("corpus/rate.txt")),
    func("round", corpus("corpus/transform.txt")),
    func("scalar", corpus("corpus/transform.txt")),
    func("sgn", corpus("corpus/transform.txt")),
    func("sin", corpus("corpus/transform.txt")),
    func("sinh", corpus("corpus/transform.txt")),
    func("sort", corpus("corpus/transform.txt")),
    func("sort_desc", corpus("corpus/transform.txt")),
    func("sqrt", corpus("corpus/transform.txt")),
    func("stddev_over_time", corpus("corpus/over_time.txt")),
    func("stdvar_over_time", corpus("corpus/over_time.txt")),
    func("sum_over_time", corpus("corpus/over_time.txt")),
    func("tan", corpus("corpus/transform.txt")),
    func("tanh", corpus("corpus/transform.txt")),
    func("time", corpus("corpus/transform.txt")),
    func("timestamp", corpus("corpus/transform.txt")),
    func("vector", corpus("corpus/transform.txt")),
    func("year", corpus("corpus/transform.txt")),
    // ---- Accepted divergences (ADR-0035: not state 3, not re-litigated) ----
    Construct {
        name: "float-precision residue (ULP tolerance)",
        category: Category::Divergence,
        state: ConstructState::AcceptedDivergence { adr: "ADR-0025" },
        probe: Probe::ToleranceEntry,
        note: "arithmetic and transcendental residue between two independent \
               implementations of the same formula; each entry carries a \
               written justification for its tolerance in the corpus file",
    },
    Construct {
        name: "subquery per-node point cap",
        category: Category::Divergence,
        state: ConstructState::AcceptedDivergence { adr: "ADR-0030" },
        probe: Probe::DivergenceModeEntry("subquery"),
        note: "Ravel's per-subquery-node 11,000-point budget has no \
               Prometheus counterpart, so Ravel rejects by design where \
               Prometheus accepts; the comparator asserts exactly that shape",
    },
];

/// Whether a rejection case drives the instant or the range endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectionEval {
    /// `/api/v1/query`.
    Instant,
    /// `/api/v1/query_range`.
    Range,
}

/// A query proving one [`ConstructState::IntentionallyRejected`] construct
/// actually rejects. ADR-0035's Consequences: "the real value of this work is
/// proving state-2 rows fail cleanly", so these run in the ordinary
/// `cargo test -p ravel-promql-difftest`, with no pinned Prometheus binary.
#[derive(Debug, Clone, Copy)]
pub struct RejectionCase {
    /// The [`Construct::name`] this case exercises.
    pub construct: &'static str,
    /// A query that reaches the rejection.
    pub query: &'static str,
    /// Which endpoint to drive.
    pub eval: RejectionEval,
    /// Milliseconds past the dataset's `base_ts_ms` to evaluate at, so a case
    /// that needs matched data in a lookback or subquery window can reach it
    /// without hard-coding an absolute timestamp (CLAUDE.md "time is
    /// injected").
    pub time_offset_ms: i64,
    /// A substring the rejection's message must contain, or empty when only
    /// the typed-rejection shape is asserted (a construct the parser refuses
    /// outright still rejects cleanly, just with `bad_data` rather than the
    /// evaluator's own message).
    pub message_contains: &'static str,
}

/// One case per intentionally-rejected construct. The conformance test
/// asserts that mapping is total in both directions.
pub static REJECTION_CASES: &[RejectionCase] = &[
    RejectionCase {
        construct: "histogram_stddev",
        query: "histogram_stddev(diff_native_hist)",
        eval: RejectionEval::Instant,
        time_offset_ms: 0,
        message_contains: "histogram_stddev",
    },
    RejectionCase {
        construct: "histogram_stdvar",
        query: "histogram_stdvar(diff_native_hist)",
        eval: RejectionEval::Instant,
        time_offset_ms: 0,
        message_contains: "histogram_stdvar",
    },
    RejectionCase {
        construct: "label matcher or-group",
        query: "diff_gauge_walk{job=\"api\" or job=\"db\"}",
        eval: RejectionEval::Instant,
        time_offset_ms: 0,
        message_contains: "label matcher or-group",
    },
    RejectionCase {
        construct: "vector matching fill values",
        query: "diff_match_left + fill(0) diff_match_right",
        eval: RejectionEval::Instant,
        time_offset_ms: 0,
        message_contains: "fill-in values",
    },
    RejectionCase {
        // +600s so the subquery's epoch-aligned grid lands inside the
        // generated histogram series' own sample range: the rejection
        // triggers on matched histogram data, so a grid that matches nothing
        // would answer with an empty vector and prove nothing.
        construct: "subquery over native histograms",
        query: "avg_over_time(diff_native_hist[5m:1m])",
        eval: RejectionEval::Instant,
        time_offset_ms: 600_000,
        message_contains: "subquery over native histograms",
    },
    RejectionCase {
        construct: "binary operator over native histograms",
        query: "diff_native_hist * 2",
        eval: RejectionEval::Instant,
        time_offset_ms: 0,
        message_contains: "binary operator over native histograms",
    },
    RejectionCase {
        // +600s so the instant query's window matches the generated histogram
        // series (its samples span the dataset's own range); a bare matrix
        // selector matching histogram data is the #643 refusal.
        construct: "instant matrix selector over native histograms",
        query: "diff_native_hist[5m]",
        eval: RejectionEval::Instant,
        time_offset_ms: 600_000,
        message_contains: "range vector of native histograms",
    },
];

// ---------------------------------------------------------------------------
// Lexing
// ---------------------------------------------------------------------------

/// A PromQL token, carrying only what the probes need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Tok {
    /// An identifier: a metric name, a function name, or a keyword.
    Ident(String),
    /// A numeric literal that is not a duration.
    Number,
    /// A duration literal (`5m`, `1h30m`).
    Duration,
    /// A quoted string.
    Str,
    /// An operator symbol.
    Op(String),
    /// One of `( ) [ ] { } , : @`.
    Punct(char),
}

/// A token plus the nesting it appeared at, so a probe can tell a
/// label-matcher `!=` from the binary operator without re-walking the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    /// The token itself.
    pub tok: Tok,
    /// How many enclosing `{` groups the token sits inside.
    pub brace_depth: u32,
    /// How many enclosing `[` groups the token sits inside.
    pub bracket_depth: u32,
}

/// Two-character operators, longest-match first.
const TWO_CHAR_OPS: &[&str] = &["=~", "!~", "==", "!=", ">=", "<="];
/// One-character operators.
const ONE_CHAR_OPS: &[char] = &['=', '!', '<', '>', '+', '-', '*', '/', '%', '^'];
/// Duration unit suffixes, longest-match first.
const DURATION_UNITS: &[&str] = &["ms", "s", "m", "h", "d", "w", "y"];

/// Identifiers that are PromQL keywords rather than metric names.
const KEYWORDS: &[&str] = &[
    "by",
    "without",
    "on",
    "ignoring",
    "group_left",
    "group_right",
    "bool",
    "and",
    "or",
    "unless",
    "atan2",
    "offset",
    "start",
    "end",
    "step",
    "fill",
    "fill_left",
    "fill_right",
    "limitk",
    "limit_ratio",
];

/// Identifiers that are float literals, not metric names.
const FLOAT_WORDS: &[&str] = &["Inf", "inf", "Infinity", "NaN", "nan", "NAN"];

/// The 12 non-experimental aggregation operators.
const AGGREGATORS: &[&str] = &[
    "sum",
    "avg",
    "min",
    "max",
    "count",
    "count_values",
    "group",
    "stddev",
    "stdvar",
    "topk",
    "bottomk",
    "quantile",
];

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_' || c == ':'
}

/// A `:` is an identifier *continuation* (recording-rule names like
/// `job:requests:rate5m`) but never an identifier *start*: a leading `:` in
/// this grammar is a subquery's step separator, and treating it as the head of
/// an identifier would swallow `[5m:1m]`'s step into one token.
fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

/// Lexes just enough PromQL for the probes: identifiers, numbers, durations,
/// strings, operators, and punctuation, each tagged with its brace and
/// bracket nesting.
///
/// This is deliberately not a PromQL parser. It never needs to be: every
/// input is a corpus entry whose query promql-parser already accepted (or, for
/// the `mode: error` entries, deliberately rejected), and the probes only ask
/// which token shapes are present.
pub fn lex(query: &str) -> Vec<Token> {
    let chars: Vec<char> = query.chars().collect();
    let mut out: Vec<Token> = Vec::new();
    let mut brace_depth: u32 = 0;
    let mut bracket_depth: u32 = 0;
    let mut i = 0usize;

    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }

        // Depth changes apply to the delimiter itself in the natural way: an
        // opening delimiter is recorded at the outer depth, a closing one at
        // the outer depth too, so `{` and its matching `}` agree.
        let tok = if is_ident_start(c) {
            let start = i;
            while i < chars.len() && is_ident_char(chars[i]) {
                i += 1;
            }
            Tok::Ident(chars[start..i].iter().collect())
        } else if c.is_ascii_digit()
            || (c == '.' && matches!(chars.get(i + 1), Some(d) if d.is_ascii_digit()))
        {
            lex_number(&chars, &mut i)
        } else if c == '"' || c == '\'' || c == '`' {
            lex_string(&chars, &mut i, c);
            Tok::Str
        } else if let Some(op) = two_char_op(&chars, i) {
            i += 2;
            Tok::Op(op.to_string())
        } else if ONE_CHAR_OPS.contains(&c) {
            i += 1;
            Tok::Op(c.to_string())
        } else if matches!(c, '(' | ')' | '[' | ']' | '{' | '}' | ',' | ':' | '@') {
            i += 1;
            match c {
                '{' => brace_depth += 1,
                '}' => brace_depth = brace_depth.saturating_sub(1),
                '[' => bracket_depth += 1,
                ']' => bracket_depth = bracket_depth.saturating_sub(1),
                _ => {}
            }
            let depths = match c {
                '{' => (brace_depth.saturating_sub(1), bracket_depth),
                '[' => (brace_depth, bracket_depth.saturating_sub(1)),
                _ => (brace_depth, bracket_depth),
            };
            out.push(Token {
                tok: Tok::Punct(c),
                brace_depth: depths.0,
                bracket_depth: depths.1,
            });
            continue;
        } else {
            // Anything else (a stray character in a deliberately malformed
            // `mode: error` entry) is skipped rather than derailing the lex.
            i += 1;
            continue;
        };

        out.push(Token {
            tok,
            brace_depth,
            bracket_depth,
        });
    }
    out
}

fn two_char_op(chars: &[char], i: usize) -> Option<&'static str> {
    let pair: String = chars.get(i..i + 2)?.iter().collect();
    TWO_CHAR_OPS.iter().copied().find(|op| *op == pair)
}

/// Reads a numeric literal, returning [`Tok::Duration`] when a duration unit
/// suffix follows the digits (`5m`, `1h30m`) and [`Tok::Number`] otherwise.
fn lex_number(chars: &[char], i: &mut usize) -> Tok {
    let mut duration = false;
    loop {
        let start = *i;
        while *i < chars.len() && (chars[*i].is_ascii_digit() || chars[*i] == '.') {
            *i += 1;
        }
        if *i == start {
            break;
        }
        match duration_unit(chars, *i) {
            Some(unit) => {
                *i += unit.len();
                duration = true;
            }
            None => break,
        }
    }
    // An exponent (`1e3`) is a number, never a duration: `e` is not a
    // duration unit, so the loop above stops before it. Consume it here so
    // the exponent digits are not re-lexed as a second number.
    if !duration
        && matches!(chars.get(*i), Some('e') | Some('E'))
        && let Some(rest) = exponent_len(chars, *i)
    {
        *i += rest;
    }
    if duration { Tok::Duration } else { Tok::Number }
}

/// The duration unit starting at `i`, if any. A unit only counts when what
/// follows it is not an identifier character, so a metric name is never
/// mistaken for a unit.
fn duration_unit(chars: &[char], i: usize) -> Option<&'static str> {
    for unit in DURATION_UNITS {
        // A unit longer than what is left cannot match; keep trying the
        // shorter ones rather than giving up on the whole lookup.
        let Some(slice) = chars.get(i..i + unit.len()) else {
            continue;
        };
        let candidate: String = slice.iter().collect();
        if candidate == *unit {
            let next = chars.get(i + unit.len());
            // `5m` in `[5m]` and `1h30m`: a following digit continues the
            // duration and a following `:` is a subquery's step separator,
            // but a following letter or `_` means this was never a unit.
            let ok = match next {
                None => true,
                Some(c) => !c.is_ascii_alphabetic() && *c != '_',
            };
            if ok {
                return Some(unit);
            }
        }
    }
    None
}

/// The length of an exponent suffix (`e3`, `E+10`) starting at `i`, if one is
/// actually there.
fn exponent_len(chars: &[char], i: usize) -> Option<usize> {
    let mut j = i + 1;
    if matches!(chars.get(j), Some('+') | Some('-')) {
        j += 1;
    }
    let digits_start = j;
    while matches!(chars.get(j), Some(c) if c.is_ascii_digit()) {
        j += 1;
    }
    if j > digits_start { Some(j - i) } else { None }
}

/// Consumes a quoted string, honouring backslash escapes. An unterminated
/// string (a deliberately malformed `mode: error` entry) consumes to the end.
fn lex_string(chars: &[char], i: &mut usize, quote: char) {
    *i += 1;
    while *i < chars.len() {
        let c = chars[*i];
        if c == '\\' {
            *i += 2;
            continue;
        }
        *i += 1;
        if c == quote {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Probing
// ---------------------------------------------------------------------------

/// The token before `idx`, skipping nothing (the lexer already dropped
/// whitespace).
fn prev(tokens: &[Token], idx: usize) -> Option<&Tok> {
    idx.checked_sub(1)
        .and_then(|p| tokens.get(p))
        .map(|t| &t.tok)
}

fn next_tok(tokens: &[Token], idx: usize) -> Option<&Tok> {
    tokens.get(idx + 1).map(|t| &t.tok)
}

/// Whether the token at `idx` is an identifier immediately followed by `(`.
fn is_call(tokens: &[Token], idx: usize) -> bool {
    matches!(next_tok(tokens, idx), Some(Tok::Punct('(')))
}

/// Whether an operator at `idx` sits in infix position: something that can
/// end an operand precedes it.
fn is_infix(tokens: &[Token], idx: usize) -> bool {
    matches!(
        prev(tokens, idx),
        Some(Tok::Ident(_))
            | Some(Tok::Number)
            | Some(Tok::Duration)
            | Some(Tok::Str)
            | Some(Tok::Punct(')'))
            | Some(Tok::Punct(']'))
            | Some(Tok::Punct('}'))
    )
}

/// Whether the identifier at `idx` names a metric rather than a function, an
/// aggregator, a keyword, or a float word.
fn is_metric_name(tokens: &[Token], idx: usize, name: &str) -> bool {
    if is_call(tokens, idx) {
        return false;
    }
    if KEYWORDS.contains(&name) || FLOAT_WORDS.contains(&name) || AGGREGATORS.contains(&name) {
        return false;
    }
    // A metric name never follows `@` (that position holds `start`/`end`,
    // both already keywords) and never sits inside a label-matcher group.
    tokens
        .get(idx)
        .is_some_and(|t| t.brace_depth == 0 && t.bracket_depth == 0)
}

/// Whether `tokens` contains a `[` group whose contents include a `:` at that
/// same bracket depth (a subquery) or do not (a matrix selector).
fn has_bracket_group(tokens: &[Token], with_step: bool) -> bool {
    for (idx, token) in tokens.iter().enumerate() {
        if token.tok != Tok::Punct('[') {
            continue;
        }
        let inner_depth = token.bracket_depth + 1;
        let mut stepped = false;
        for later in &tokens[idx + 1..] {
            match (&later.tok, later.bracket_depth) {
                (Tok::Punct(']'), d) if d == token.bracket_depth => break,
                (Tok::Punct(':'), d) if d == inner_depth => stepped = true,
                _ => {}
            }
        }
        if stepped == with_step {
            return true;
        }
    }
    false
}

/// Whether the query's token stream exercises `probe`.
pub fn exercises(probe: Probe, tokens: &[Token], entry: &CorpusEntry) -> bool {
    match probe {
        Probe::Call(name) => tokens.iter().enumerate().any(|(idx, t)| {
            matches!(&t.tok, Tok::Ident(id) if id == name) && is_call(tokens, idx)
        }),
        Probe::Aggregation(name) => tokens.iter().enumerate().any(|(idx, t)| {
            matches!(&t.tok, Tok::Ident(id) if id == name)
                && (is_call(tokens, idx)
                    || matches!(next_tok(tokens, idx), Some(Tok::Ident(k)) if k == "by" || k == "without"))
        }),
        // A following `(` is not disqualifying: `on(job)`, `ignoring(job)`,
        // and `by (job)` all take a label list, and no Prometheus function
        // shares a name with a keyword.
        Probe::Keyword(name) => tokens
            .iter()
            .any(|t| t.brace_depth == 0 && matches!(&t.tok, Tok::Ident(id) if id == name)),
        Probe::InfixOp(sym) => tokens.iter().enumerate().any(|(idx, t)| {
            t.brace_depth == 0 && matches!(&t.tok, Tok::Op(op) if op == sym) && is_infix(tokens, idx)
        }),
        Probe::MatcherOp(sym) => tokens
            .iter()
            .any(|t| t.brace_depth > 0 && matches!(&t.tok, Tok::Op(op) if op == sym)),
        Probe::Shape(shape) => exercises_shape(shape, tokens),
        Probe::ToleranceEntry => entry.tolerance_ulps.is_some(),
        Probe::DivergenceModeEntry(marker) => {
            entry.mode == ComparisonMode::RavelErrorPromSuccess && entry.name.contains(marker)
        }
        Probe::None => false,
    }
}

fn exercises_shape(shape: Shape, tokens: &[Token]) -> bool {
    match shape {
        Shape::VectorSelector => tokens.iter().enumerate().any(|(idx, t)| {
            matches!(&t.tok, Tok::Ident(id) if is_metric_name(tokens, idx, id))
        }),
        Shape::MatrixSelector => has_bracket_group(tokens, false),
        Shape::Subquery => has_bracket_group(tokens, true),
        Shape::GroupingParen => tokens.iter().enumerate().any(|(idx, t)| {
            t.tok == Tok::Punct('(')
                && !matches!(prev(tokens, idx), Some(Tok::Ident(_)))
        }),
        Shape::UnaryExpr => tokens.iter().enumerate().any(|(idx, t)| {
            matches!(&t.tok, Tok::Op(op) if op == "-" || op == "+")
                && !is_infix(tokens, idx)
                && match next_tok(tokens, idx) {
                    Some(Tok::Ident(id)) => !FLOAT_WORDS.contains(&id.as_str()),
                    Some(Tok::Punct('(')) => true,
                    _ => false,
                }
        }),
        Shape::NumberLiteral => tokens
            .iter()
            .any(|t| t.tok == Tok::Number && t.brace_depth == 0 && t.bracket_depth == 0),
        Shape::StringLiteral => tokens.iter().any(|t| t.tok == Tok::Str && t.brace_depth == 0),
        Shape::FunctionCall => tokens.iter().enumerate().any(|(idx, t)| {
            matches!(&t.tok, Tok::Ident(id) if !AGGREGATORS.contains(&id.as_str()))
                && is_call(tokens, idx)
                && !matches!(&t.tok, Tok::Ident(id) if id == "start" || id == "end")
        }),
        Shape::BinaryExpr => tokens.iter().enumerate().any(|(idx, t)| {
            t.brace_depth == 0
                && (matches!(&t.tok, Tok::Op(_)) && is_infix(tokens, idx)
                    || matches!(&t.tok, Tok::Ident(id)
                        if id == "and" || id == "or" || id == "unless" || id == "atan2"))
        }),
        Shape::AggregateExpr => tokens.iter().enumerate().any(|(idx, t)| {
            matches!(&t.tok, Tok::Ident(id) if AGGREGATORS.contains(&id.as_str()))
                && (is_call(tokens, idx)
                    || matches!(next_tok(tokens, idx), Some(Tok::Ident(k)) if k == "by" || k == "without"))
        }),
        Shape::Offset => tokens
            .iter()
            .any(|t| matches!(&t.tok, Tok::Ident(id) if id == "offset")),
        Shape::NegativeOffset => tokens.iter().enumerate().any(|(idx, t)| {
            matches!(&t.tok, Tok::Ident(id) if id == "offset")
                && matches!(next_tok(tokens, idx), Some(Tok::Op(op)) if op == "-")
        }),
        Shape::AtTimestamp => tokens.iter().enumerate().any(|(idx, t)| {
            t.tok == Tok::Punct('@')
                && matches!(
                    next_tok(tokens, idx),
                    Some(Tok::Number) | Some(Tok::Op(_)) | Some(Tok::Duration)
                )
        }),
        Shape::AtStart => at_preprocessor(tokens, "start"),
        Shape::AtEnd => at_preprocessor(tokens, "end"),
    }
}

fn at_preprocessor(tokens: &[Token], name: &str) -> bool {
    tokens.iter().enumerate().any(|(idx, t)| {
        t.tok == Tok::Punct('@')
            && matches!(next_tok(tokens, idx), Some(Tok::Ident(id)) if id == name)
    })
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

/// One corpus file's parsed entries, tagged with the path a
/// [`ConstructState::Supported`] row cites.
#[derive(Debug, Clone, Copy)]
pub struct CorpusFile<'a> {
    /// The cited path, e.g. `corpus/rate.txt`.
    pub path: &'static str,
    /// That file's parsed entries.
    pub entries: &'a [CorpusEntry],
}

/// One corpus entry exercising one construct.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Evidence {
    /// The corpus file the entry came from.
    pub path: &'static str,
    /// The entry's `name:` field.
    pub entry: String,
}

/// What a run showed about one construct.
#[derive(Debug, Clone)]
pub struct ConstructOutcome {
    /// The registry row.
    pub construct: &'static Construct,
    /// Every corpus entry whose query exercises the construct.
    pub evidence: Vec<Evidence>,
    /// Exercising entries that did not behave as their mode requires, either
    /// on the Ravel side alone or against the pinned Prometheus binary.
    pub failures: Vec<String>,
    /// For an [`ConstructState::IntentionallyRejected`] row: whether its
    /// rejection cases actually rejected. `None` until
    /// [`ConformanceReport::apply_rejection_results`] runs.
    pub rejection_confirmed: Option<bool>,
    /// For a [`ConstructState::Supported`] row citing a `ravel-promql` unit
    /// test: whether a function by that name still exists in that crate.
    /// `None` until [`ConformanceReport::apply_unit_test_names`] runs, and a
    /// row citing a unit test publishes as supported only on `Some(true)`.
    pub unit_test_confirmed: Option<bool>,
}

impl ConstructOutcome {
    /// Whether the construct cites a corpus file the evidence does not
    /// actually cover.
    pub fn cited_corpus_file_missing(&self) -> bool {
        match self.construct.state {
            ConstructState::Supported { test } => match test.strip_prefix(CORPUS_TEST_PREFIX) {
                Some(_) => !self.evidence.iter().any(|e| e.path == test),
                None => false,
            },
            _ => false,
        }
    }

    /// Whether the construct is registered as supported but proven only by a
    /// `ravel-promql` unit test, i.e. this crate's corpus has a gap.
    pub fn is_corpus_gap(&self) -> bool {
        matches!(
            self.construct.state,
            ConstructState::Supported { test } if test.starts_with(UNIT_TEST_PREFIX)
        )
    }

    /// Whether the construct cites a `ravel-promql` unit test the run did not
    /// find: a renamed or deleted test, or a run that never looked.
    pub fn cited_unit_test_missing(&self) -> bool {
        self.is_corpus_gap() && self.unit_test_confirmed != Some(true)
    }

    /// The state the table publishes, recomputed from the run rather than
    /// taken from the registry.
    pub fn published_state(&self) -> ConstructState {
        match self.construct.state {
            ConstructState::Supported { test } => {
                if !self.failures.is_empty() {
                    return ConstructState::Unclassified;
                }
                if test.starts_with(CORPUS_TEST_PREFIX)
                    && (self.evidence.is_empty() || self.cited_corpus_file_missing())
                {
                    return ConstructState::Unclassified;
                }
                // A `ravel-promql:<fn>` citation needs the same treatment a
                // corpus citation gets: the named test has to still exist. A
                // row whose unit test was renamed or deleted is an unverified
                // claim, not evidence.
                if self.cited_unit_test_missing() {
                    return ConstructState::Unclassified;
                }
                ConstructState::Supported { test }
            }
            ConstructState::IntentionallyRejected { typed_error } => {
                match self.rejection_confirmed {
                    Some(false) => ConstructState::Unclassified,
                    _ => ConstructState::IntentionallyRejected { typed_error },
                }
            }
            ConstructState::AcceptedDivergence { adr } => {
                if self.evidence.is_empty() {
                    ConstructState::Unclassified
                } else {
                    ConstructState::AcceptedDivergence { adr }
                }
            }
            ConstructState::Unclassified => ConstructState::Unclassified,
        }
    }
}

/// Per-state totals over a [`ConformanceReport`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StateCounts {
    /// State 1 rows.
    pub supported: usize,
    /// State 2 rows.
    pub intentionally_rejected: usize,
    /// Accepted-divergence rows.
    pub accepted_divergence: usize,
    /// State 3 rows: the actionable misses.
    pub unclassified: usize,
}

impl StateCounts {
    /// The full enumerated surface.
    pub fn total(&self) -> usize {
        self.supported + self.intentionally_rejected + self.accepted_divergence + self.unclassified
    }

    /// Rows Ravel has taken a deliberate, verified position on.
    pub fn conformant(&self) -> usize {
        self.supported + self.intentionally_rejected + self.accepted_divergence
    }

    /// [`Self::conformant`] over [`Self::total`], as a percentage rounded to
    /// one decimal place. Zero when the surface is empty.
    pub fn score_percent(&self) -> f64 {
        let total = self.total();
        if total == 0 {
            return 0.0;
        }
        let raw = self.conformant() as f64 * 1000.0 / total as f64;
        raw.round() / 10.0
    }
}

/// Errors the report and the doc splice can produce.
#[derive(Debug, thiserror::Error)]
pub enum ScoringError {
    /// A construct name appears twice in [`REGISTRY`].
    #[error("duplicate construct '{0}' in REGISTRY")]
    DuplicateConstruct(String),
    /// A [`RejectionCase`] names a construct that is not in [`REGISTRY`].
    #[error("rejection case names unknown construct '{0}'")]
    UnknownRejectionConstruct(String),
    /// A [`ConstructState::IntentionallyRejected`] row's `typed_error` string
    /// does not name a checkable `(<status> <errorType>)` pair.
    #[error("{0}")]
    MalformedTypedError(String),
    /// Walking a crate's sources for its test function names failed.
    #[error("scanning '{path}' for unit test names: {message}")]
    UnitTestScan {
        /// The path being read when the walk failed.
        path: String,
        /// The underlying I/O error.
        message: String,
    },
    /// The generated-table markers are missing or out of order.
    #[error("{0}")]
    Markers(String),
}

// ---------------------------------------------------------------------------
// Cited unit tests
// ---------------------------------------------------------------------------

/// Every `#[test]`/`#[tokio::test]` function name in one Rust source file.
///
/// A `ravel-promql:<fn>` citation is only evidence while that function still
/// exists, so the run has to look. The scan reads the attribute rather than
/// every zero-argument function, so a helper named like a deleted test cannot
/// stand in for it.
pub fn collect_test_fn_names(source: &str) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    let mut saw_test_attr = false;
    for line in source.lines() {
        let line = line.trim();
        if line.starts_with("#[test]")
            || line.starts_with("#[tokio::test")
            || line.starts_with("#[test(")
        {
            saw_test_attr = true;
            continue;
        }
        let signature = line
            .strip_prefix("async fn ")
            .or_else(|| line.strip_prefix("fn "))
            .or_else(|| line.strip_prefix("pub async fn "))
            .or_else(|| line.strip_prefix("pub fn "));
        if let Some(signature) = signature {
            if saw_test_attr && let Some(name) = signature.split('(').next() {
                names.insert(name.trim().to_string());
            }
            saw_test_attr = false;
            continue;
        }
        // Attributes and doc comments may sit between `#[test]` and the
        // signature; anything else ends the run (a blank line, an item, a
        // closing brace), so a stray attribute cannot leak onto a later
        // function.
        if !line.is_empty() && !line.starts_with('#') && !line.starts_with("//") {
            saw_test_attr = false;
        }
    }
    names
}

/// Every test function name under `root`, walking `.rs` files recursively.
///
/// Pointed at `crates/ravel-promql`, this is the set a
/// `ravel-promql:<fn>` citation must be found in.
pub fn scan_test_fn_names(root: &Path) -> Result<BTreeSet<String>, ScoringError> {
    let mut names = BTreeSet::new();
    let mut stack = vec![root.to_path_buf()];
    let io = |path: &Path, e: std::io::Error| ScoringError::UnitTestScan {
        path: path.display().to_string(),
        message: e.to_string(),
    };
    while let Some(path) = stack.pop() {
        let meta = std::fs::metadata(&path).map_err(|e| io(&path, e))?;
        if meta.is_dir() {
            for entry in std::fs::read_dir(&path).map_err(|e| io(&path, e))? {
                let entry = entry.map_err(|e| io(&path, e))?;
                stack.push(entry.path());
            }
            continue;
        }
        if path.extension().is_some_and(|ext| ext == "rs") {
            let text = std::fs::read_to_string(&path).map_err(|e| io(&path, e))?;
            names.extend(collect_test_fn_names(&text));
        }
    }
    Ok(names)
}

/// The per-construct breakdown ADR-0035's Consequences asks
/// [`RunReport`](crate::runner::RunReport) to grow, kept as a sibling type so
/// `runner.rs` stays a plain corpus-vs-Prometheus differ.
#[derive(Debug, Clone)]
pub struct ConformanceReport {
    /// One outcome per [`REGISTRY`] row, in registry order.
    pub outcomes: Vec<ConstructOutcome>,
    /// How many corpus entries the report was built from.
    pub corpus_entries: usize,
    /// How many corpus files the report was built from.
    pub corpus_files: usize,
}

impl ConformanceReport {
    /// Builds the breakdown by probing every corpus entry's query against
    /// every registry row. No pass/fail information yet: that comes from
    /// [`Self::apply_ravel_outcomes`], [`Self::apply_run_report`], and
    /// [`Self::apply_rejection_results`].
    pub fn from_corpus(files: &[CorpusFile<'_>]) -> Self {
        let mut outcomes: Vec<ConstructOutcome> = REGISTRY
            .iter()
            .map(|construct| ConstructOutcome {
                construct,
                evidence: Vec::new(),
                failures: Vec::new(),
                rejection_confirmed: None,
                unit_test_confirmed: None,
            })
            .collect();

        let mut corpus_entries = 0usize;
        for file in files {
            for entry in file.entries {
                corpus_entries += 1;
                let tokens = lex(&entry.query);
                for outcome in &mut outcomes {
                    if exercises(outcome.construct.probe, &tokens, entry) {
                        outcome.evidence.push(Evidence {
                            path: file.path,
                            entry: entry.name.clone(),
                        });
                    }
                }
            }
        }

        ConformanceReport {
            outcomes,
            corpus_entries,
            corpus_files: files.len(),
        }
    }

    /// Folds in a Ravel-only run ([`run_ravel_only`]): every entry whose
    /// Ravel-side behaviour contradicted its mode marks each construct it
    /// exercises as failing.
    pub fn apply_ravel_outcomes(&mut self, outcomes: &[RavelOutcome]) {
        let failed: BTreeSet<&str> = outcomes
            .iter()
            .filter(|o| !o.as_expected)
            .map(|o| o.entry.as_str())
            .collect();
        self.mark_failures(&failed);
    }

    /// Folds in a full differential run against the pinned Prometheus binary.
    /// Only the entry names are read, so an archived report is enough.
    pub fn apply_run_report(&mut self, report: &RunReport) {
        let failed: BTreeSet<&str> = report
            .failures
            .iter()
            .map(|f| f.entry_name.as_str())
            .collect();
        self.mark_failures(&failed);
    }

    fn mark_failures(&mut self, failed: &BTreeSet<&str>) {
        for outcome in &mut self.outcomes {
            for evidence in &outcome.evidence {
                if failed.contains(evidence.entry.as_str())
                    && !outcome.failures.contains(&evidence.entry)
                {
                    outcome.failures.push(evidence.entry.clone());
                }
            }
        }
    }

    /// Folds in the state-2 verification: `results` maps a
    /// [`Construct::name`] to whether its rejection cases all rejected
    /// cleanly. A construct claimed rejected that actually answered publishes
    /// as [`ConstructState::Unclassified`].
    pub fn apply_rejection_results(&mut self, results: &BTreeMap<&str, bool>) {
        for outcome in &mut self.outcomes {
            if let Some(ok) = results.get(outcome.construct.name) {
                outcome.rejection_confirmed = Some(*ok);
            }
        }
    }

    /// Folds in the unit-test citations: `names` is every test function name
    /// the `ravel-promql` crate still defines (see [`scan_test_fn_names`]).
    /// A row citing `ravel-promql:<fn>` publishes as supported only when `<fn>`
    /// is in that set, so a renamed or deleted test drops the row to
    /// [`ConstructState::Unclassified`] in the same run.
    pub fn apply_unit_test_names(&mut self, names: &BTreeSet<String>) {
        for outcome in &mut self.outcomes {
            if let ConstructState::Supported { test } = outcome.construct.state
                && let Some(unit_test) = test.strip_prefix(UNIT_TEST_PREFIX)
            {
                outcome.unit_test_confirmed = Some(names.contains(unit_test));
            }
        }
    }

    /// Every construct citing a `ravel-promql` unit test the run did not find,
    /// with the citation, so a failure message can name the renamed test
    /// rather than only the construct.
    pub fn missing_unit_tests(&self) -> Vec<(&'static str, &'static str)> {
        self.outcomes
            .iter()
            .filter(|o| o.cited_unit_test_missing())
            .filter_map(|o| match o.construct.state {
                ConstructState::Supported { test } => Some((o.construct.name, test)),
                _ => None,
            })
            .collect()
    }

    /// Per-state totals over the published states.
    pub fn counts(&self) -> StateCounts {
        let mut counts = StateCounts::default();
        for outcome in &self.outcomes {
            match outcome.published_state() {
                ConstructState::Supported { .. } => counts.supported += 1,
                ConstructState::IntentionallyRejected { .. } => {
                    counts.intentionally_rejected += 1;
                }
                ConstructState::AcceptedDivergence { .. } => counts.accepted_divergence += 1,
                ConstructState::Unclassified => counts.unclassified += 1,
            }
        }
        counts
    }

    /// Every construct publishing as [`ConstructState::Unclassified`], in
    /// registry order. ADR-0035's state-3 bucket: each of these is a ticket.
    pub fn unclassified(&self) -> Vec<&ConstructOutcome> {
        self.outcomes
            .iter()
            .filter(|o| o.published_state() == ConstructState::Unclassified)
            .collect()
    }

    /// Checks the registry's own well-formedness: no duplicate construct
    /// names, every [`ConstructState::IntentionallyRejected`] row declaring a
    /// checkable `(<status> <errorType>)` pair, and every [`RejectionCase`]
    /// naming a real construct.
    pub fn validate_registry() -> Result<(), ScoringError> {
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for construct in REGISTRY {
            if !seen.insert(construct.name) {
                return Err(ScoringError::DuplicateConstruct(construct.name.to_string()));
            }
            if let ConstructState::IntentionallyRejected { typed_error } = construct.state {
                parse_typed_error(typed_error)?;
            }
        }
        for case in REJECTION_CASES {
            if !seen.contains(case.construct) {
                return Err(ScoringError::UnknownRejectionConstruct(
                    case.construct.to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Renders the generated block: the score summary plus the table, without
    /// the surrounding markers.
    pub fn to_markdown(&self) -> String {
        let counts = self.counts();
        let mut out = String::new();
        // Prose lines stay inside the repo's 80-column doc convention; the
        // table rows themselves cannot, like every other table in docs/.
        out.push_str(
            "Generated by `cargo test -p ravel-promql-difftest --test\n\
             conformance_table`; regenerate that same command with\n\
             `RAVEL_UPDATE_CONFORMANCE_TABLE=1`. Do not edit the block between\n\
             the markers by hand.\n\n",
        );
        out.push_str(&format!(
            "Surface: {} constructs over {} corpus entries in {} corpus files.\n\n",
            counts.total(),
            self.corpus_entries,
            self.corpus_files
        ));
        out.push_str("| State | Constructs |\n| --- | --- |\n");
        out.push_str(&format!("| supported | {} |\n", counts.supported));
        out.push_str(&format!(
            "| intentionally rejected | {} |\n",
            counts.intentionally_rejected
        ));
        out.push_str(&format!(
            "| accepted divergence | {} |\n",
            counts.accepted_divergence
        ));
        out.push_str(&format!("| unclassified | {} |\n", counts.unclassified));
        out.push_str(&format!(
            "| **score** (supported + intentionally rejected + accepted \
             divergence / total) | **{}/{} = {}%** |\n\n",
            counts.conformant(),
            counts.total(),
            counts.score_percent()
        ));

        out.push_str("| Construct | Category | State | Evidence |\n| --- | --- | --- | --- |\n");
        let mut rows: Vec<&ConstructOutcome> = self.outcomes.iter().collect();
        rows.sort_by(|a, b| {
            a.construct
                .category
                .cmp(&b.construct.category)
                .then_with(|| a.construct.name.cmp(b.construct.name))
        });
        for outcome in rows {
            out.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                render_name(outcome.construct),
                outcome.construct.category,
                outcome.published_state().slug(),
                self.render_evidence(outcome)
            ));
        }
        out
    }

    fn render_evidence(&self, outcome: &ConstructOutcome) -> String {
        let mut parts: Vec<String> = Vec::new();
        match outcome.published_state() {
            ConstructState::Supported { test } => {
                if let Some(unit_test) = test.strip_prefix(UNIT_TEST_PREFIX) {
                    parts.push(format!(
                        "no difftest corpus entry; proven by `ravel-promql`'s \
                         `{unit_test}`"
                    ));
                } else {
                    let n = self.count_in(outcome, test);
                    parts.push(format!("`{test}`, {n} {}", plural_entries(n)));
                }
            }
            ConstructState::IntentionallyRejected { typed_error } => {
                parts.push(format!("`{typed_error}`"));
                if outcome.rejection_confirmed == Some(true) {
                    parts.push("rejection verified".to_string());
                }
            }
            ConstructState::AcceptedDivergence { adr } => {
                let n = outcome.evidence.len();
                parts.push(format!("{adr}, {n} corpus {}", plural_entries(n)));
            }
            ConstructState::Unclassified => {
                if outcome.rejection_confirmed == Some(false) {
                    parts.push("claimed rejected but the query was answered".to_string());
                } else if outcome.cited_unit_test_missing() {
                    parts.push(
                        "the cited `ravel-promql` unit test was not found in that crate"
                            .to_string(),
                    );
                } else if !outcome.failures.is_empty() {
                    parts.push(format!(
                        "{} exercising entries failed: {}",
                        outcome.failures.len(),
                        outcome.failures.join(", ")
                    ));
                } else if outcome.evidence.is_empty() {
                    parts.push("no test exercises this construct".to_string());
                } else if outcome.cited_corpus_file_missing() {
                    parts.push("cited corpus file does not exercise it".to_string());
                }
            }
        }
        if !outcome.construct.note.is_empty() {
            parts.push(collapse_whitespace(outcome.construct.note));
        }
        parts.join("; ")
    }

    fn count_in(&self, outcome: &ConstructOutcome, path: &str) -> usize {
        outcome.evidence.iter().filter(|e| e.path == path).count()
    }
}

/// Backticks a construct name unless it is prose (a multi-word category name
/// like "vector selector"), so operator rows read as code and category rows
/// read as text.
fn render_name(construct: &Construct) -> String {
    match construct.category {
        Category::Function | Category::AggregationOp | Category::BinaryOp => {
            format!("`{}`", construct.name)
        }
        Category::Divergence => construct.name.to_string(),
        Category::AstNode | Category::Modifier => {
            if construct.name.contains(' ') {
                construct.name.to_string()
            } else {
                format!("`{}`", construct.name)
            }
        }
    }
}

fn collapse_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn plural_entries(n: usize) -> &'static str {
    if n == 1 { "entry" } else { "entries" }
}

/// Replaces whatever sits between [`BEGIN_MARKER`] and [`END_MARKER`] in
/// `doc` with `block`, leaving every hand-written line outside the markers
/// exactly as it was.
pub fn splice_generated_block(doc: &str, block: &str) -> Result<String, ScoringError> {
    let begin = doc
        .find(BEGIN_MARKER)
        .ok_or_else(|| ScoringError::Markers(format!("document is missing '{BEGIN_MARKER}'")))?;
    let end = doc
        .find(END_MARKER)
        .ok_or_else(|| ScoringError::Markers(format!("document is missing '{END_MARKER}'")))?;
    if end < begin {
        return Err(ScoringError::Markers(
            "the END marker precedes the BEGIN marker".to_string(),
        ));
    }
    let head = &doc[..begin + BEGIN_MARKER.len()];
    let tail = &doc[end..];
    Ok(format!("{head}\n\n{}\n{tail}", block.trim_end()))
}

// ---------------------------------------------------------------------------
// Ravel-only corpus run
// ---------------------------------------------------------------------------

/// What Ravel alone did with one corpus entry. No Prometheus binary involved,
/// so this runs in the ordinary `cargo test -p ravel-promql-difftest`.
#[derive(Debug, Clone)]
pub struct RavelOutcome {
    /// The entry's `name:` field.
    pub entry: String,
    /// Whether Ravel's answer matched what the entry's mode requires: an
    /// error for `mode: error` and `mode: ravel_error_prom_success`, and for
    /// `mode: unordered`/`mode: ordered` a success *carrying values*, not
    /// merely `status: "success"`.
    pub as_expected: bool,
    /// What actually happened, for a failure message.
    pub detail: String,
}

/// What a successful Prometheus-envelope response actually carried.
///
/// A `status: "success"` envelope is not evidence a construct works: an empty
/// vector is a perfectly successful answer to a query that matched nothing, and
/// a construct scored off status alone counts that as coverage. So the run reads
/// the payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PayloadVerdict {
    /// The result carried `samples` well-formed values across `series`
    /// elements (a scalar or string result counts as one of each).
    Values {
        /// How many result elements the payload carried.
        series: usize,
        /// How many timestamp/value pairs those elements carried in total.
        samples: usize,
    },
    /// A successful envelope whose result set is empty.
    Empty,
    /// A successful envelope whose payload does not have the shape its
    /// `resultType` promises.
    Malformed(String),
}

impl fmt::Display for PayloadVerdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PayloadVerdict::Values { series, samples } => {
                write!(f, "{series} series, {samples} samples")
            }
            PayloadVerdict::Empty => f.write_str("the result set is empty"),
            PayloadVerdict::Malformed(why) => write!(f, "malformed payload: {why}"),
        }
    }
}

/// Reads a Prometheus `data` payload: how many elements and values it carries,
/// or why it is empty or malformed.
///
/// Every value is parsed, not just counted: a sample is a `[<ts>, "<float>"]`
/// pair whose float actually parses (`NaN` and `Inf` included, which is how
/// Prometheus spells them), or a native-histogram element. A payload whose
/// values are unparseable strings is `Malformed`, not `Values`.
pub fn check_payload(json: &Json) -> PayloadVerdict {
    let Some(data) = json.get("data") else {
        return PayloadVerdict::Malformed("no 'data' member".to_string());
    };
    let result_type = data.get("resultType").and_then(Json::as_str).unwrap_or("");
    let Some(result) = data.get("result") else {
        return PayloadVerdict::Malformed("no 'data.result' member".to_string());
    };

    match result_type {
        "vector" | "matrix" => {
            let Some(elements) = result.as_array() else {
                return PayloadVerdict::Malformed(format!(
                    "resultType '{result_type}' but 'result' is not an array"
                ));
            };
            if elements.is_empty() {
                return PayloadVerdict::Empty;
            }
            let mut samples = 0usize;
            for element in elements {
                if !element.get("metric").is_some_and(Json::is_object) {
                    return PayloadVerdict::Malformed(
                        "a result element carries no 'metric' object".to_string(),
                    );
                }
                // A float element carries `value`/`values`; a native-histogram
                // element carries `histogram`/`histograms` instead.
                let float_points: Vec<&Json> = match (element.get("value"), element.get("values")) {
                    (Some(point), None) => vec![point],
                    (None, Some(points)) => match points.as_array() {
                        Some(points) => points.iter().collect(),
                        None => {
                            return PayloadVerdict::Malformed(
                                "'values' is not an array".to_string(),
                            );
                        }
                    },
                    _ => Vec::new(),
                };
                let histogram_points = match (element.get("histogram"), element.get("histograms")) {
                    (Some(_), None) => 1,
                    (None, Some(points)) => points.as_array().map(Vec::len).unwrap_or(0),
                    _ => 0,
                };
                if float_points.is_empty() && histogram_points == 0 {
                    return PayloadVerdict::Malformed(
                        "a result element carries neither values nor histograms".to_string(),
                    );
                }
                for point in &float_points {
                    if let Err(why) = parse_sample(point) {
                        return PayloadVerdict::Malformed(why);
                    }
                }
                samples += float_points.len() + histogram_points;
            }
            PayloadVerdict::Values {
                series: elements.len(),
                samples,
            }
        }
        "scalar" => match parse_sample(result) {
            Ok(()) => PayloadVerdict::Values {
                series: 1,
                samples: 1,
            },
            Err(why) => PayloadVerdict::Malformed(why),
        },
        "string" => match result.as_array() {
            Some(pair) if pair.len() == 2 && pair[1].is_string() => PayloadVerdict::Values {
                series: 1,
                samples: 1,
            },
            _ => {
                PayloadVerdict::Malformed("a string result is not a [ts, string] pair".to_string())
            }
        },
        other => PayloadVerdict::Malformed(format!("unknown resultType '{other}'")),
    }
}

/// Checks one `[<ts>, "<float>"]` sample pair, parsing the value.
fn parse_sample(point: &Json) -> Result<(), String> {
    let Some(pair) = point.as_array() else {
        return Err("a sample is not a [ts, value] pair".to_string());
    };
    if pair.len() != 2 {
        return Err(format!("a sample has {} members, not 2", pair.len()));
    }
    if !pair[0].is_number() {
        return Err("a sample's timestamp is not a number".to_string());
    }
    let raw = pair[1]
        .as_str()
        .ok_or_else(|| "a sample's value is not a string".to_string())?;
    // Prometheus spells the non-finite values `NaN`, `+Inf`, `-Inf`, all of
    // which `f64::from_str` accepts.
    raw.parse::<f64>()
        .map(|_| ())
        .map_err(|_| format!("a sample's value '{raw}' does not parse as a float"))
}

/// Classifies one entry's Ravel-side answer: the mode says whether an error or
/// an answer is expected, and an expected answer must carry values unless the
/// entry declared `expect: empty`.
///
/// The empty declaration is checked in both directions: an undeclared empty
/// answer fails (it is not evidence the construct works), and a declared-empty
/// entry that starts returning data fails too (the semantics it pins moved).
pub fn classify_ravel_answer(
    entry: &str,
    expects_error: bool,
    expects_empty: bool,
    body: &Json,
) -> RavelOutcome {
    let status = body
        .get("status")
        .and_then(Json::as_str)
        .unwrap_or("<missing>");
    let errored = status == "error";
    let message = body.get("error").and_then(Json::as_str);
    let mut detail = match message {
        Some(message) => format!("status={status} error={message}"),
        None => format!("status={status}"),
    };

    if errored != expects_error {
        return RavelOutcome {
            entry: entry.to_string(),
            as_expected: false,
            detail,
        };
    }
    if expects_error {
        return RavelOutcome {
            entry: entry.to_string(),
            as_expected: true,
            detail,
        };
    }

    let payload = check_payload(body);
    let as_expected = if expects_empty {
        payload == PayloadVerdict::Empty
    } else {
        matches!(payload, PayloadVerdict::Values { samples, .. } if samples > 0)
    };
    detail = if expects_empty {
        format!("{detail} payload={payload} (expect: empty)")
    } else {
        format!("{detail} payload={payload}")
    };
    RavelOutcome {
        entry: entry.to_string(),
        as_expected,
        detail,
    }
}

/// Runs every entry against the Ravel side only and reports whether each
/// behaved as its mode requires.
///
/// This is the half of the differential run that needs no pinned Prometheus
/// binary, and it is what makes the generated table derived-from-a-run in
/// every environment: a construct whose entries error out on the Ravel side
/// publishes as unclassified even when nobody can run the Prometheus half.
pub async fn run_ravel_only(
    files: &[CorpusFile<'_>],
    base_ts_ms: i64,
    ravel_app: &Router,
    ravel_token: &str,
) -> Vec<RavelOutcome> {
    let mut out = Vec::new();
    for file in files {
        for entry in file.entries {
            let expects_error = matches!(
                entry.mode,
                ComparisonMode::ExpectError | ComparisonMode::RavelErrorPromSuccess
            );
            let body = match entry.eval {
                Eval::Instant { time_offset_ms } => {
                    ravel_instant_query(
                        ravel_app,
                        ravel_token,
                        &entry.query,
                        base_ts_ms + time_offset_ms,
                    )
                    .await
                }
                Eval::Range {
                    start_offset_ms,
                    end_offset_ms,
                    step_ms,
                } => {
                    ravel_range_query(
                        ravel_app,
                        ravel_token,
                        &entry.query,
                        base_ts_ms + start_offset_ms,
                        base_ts_ms + end_offset_ms,
                        step_ms,
                    )
                    .await
                }
            };
            out.push(match body {
                Ok(json) => {
                    classify_ravel_answer(&entry.name, expects_error, entry.expects_empty, &json)
                }
                // A transport error is never the answer any mode asks for: a
                // `mode: error` entry expects Ravel's own typed rejection
                // envelope, not a dropped request.
                Err(err) => RavelOutcome {
                    entry: entry.name.clone(),
                    as_expected: false,
                    detail: format!("transport error: {err}"),
                },
            });
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::corpus::parse_corpus;

    fn entry(query: &str) -> CorpusEntry {
        let text = format!("name: t\nquery: {query}\nkind: instant\ntime: +0s\nmode: unordered\n");
        parse_corpus(&text)
            .expect("parse")
            .pop()
            .expect("one entry")
    }

    fn hits(query: &str, probe: Probe) -> bool {
        let e = entry(query);
        exercises(probe, &lex(&e.query), &e)
    }

    #[test]
    fn registry_is_well_formed() {
        ConformanceReport::validate_registry().expect("registry must be well formed");
    }

    #[test]
    fn a_call_probe_needs_the_exact_name_followed_by_a_paren() {
        assert!(hits("rate(up[5m])", Probe::Call("rate")));
        // A longer name sharing the prefix is not a `rate` call, and neither
        // is a metric whose name merely contains it.
        assert!(!hits("rate_limit_hits", Probe::Call("rate")));
        assert!(!hits("irate(up[5m])", Probe::Call("rate")));
        assert!(!hits("sum_over_time(up[5m])", Probe::Call("time")));
    }

    #[test]
    fn an_aggregation_probe_accepts_both_call_and_grouping_forms() {
        assert!(hits("sum(up)", Probe::Aggregation("sum")));
        assert!(hits("sum by (job) (up)", Probe::Aggregation("sum")));
        assert!(hits("sum without (job) (up)", Probe::Aggregation("sum")));
        assert!(!hits("sum_over_time(up[5m])", Probe::Aggregation("sum")));
    }

    #[test]
    fn a_matcher_op_is_never_confused_with_the_binary_operator() {
        // `!=` inside braces is a matcher, not the binary operator.
        assert!(hits("up{job!=\"api\"}", Probe::MatcherOp("!=")));
        assert!(!hits("up{job!=\"api\"}", Probe::InfixOp("!=")));
        // ...and outside braces it is the binary operator, not a matcher.
        assert!(hits("a != b", Probe::InfixOp("!=")));
        assert!(!hits("a != b", Probe::MatcherOp("!=")));
        assert!(hits("up{job=~\"a.*\"}", Probe::MatcherOp("=~")));
        assert!(hits("up{job!~\"a.*\"}", Probe::MatcherOp("!~")));
    }

    #[test]
    fn a_matcher_value_containing_an_operator_is_not_an_operator() {
        // The `>` lives inside a string literal, so it is never a token.
        assert!(!hits("up{note=\"a > b\"}", Probe::InfixOp(">")));
        assert!(hits("up{note=\"a > b\"}", Probe::MatcherOp("=")));
    }

    #[test]
    fn a_prefix_minus_on_a_number_is_not_a_unary_expression() {
        // promql-parser folds `-1` into a number literal; only a prefix sign
        // applied to a vector or a parenthesized expression is `Expr::Unary`.
        assert!(!hits("clamp(up, -1, 1)", Probe::Shape(Shape::UnaryExpr)));
        assert!(!hits(
            "histogram_fraction(-Inf, +Inf, up)",
            Probe::Shape(Shape::UnaryExpr)
        ));
        assert!(hits("-up", Probe::Shape(Shape::UnaryExpr)));
        assert!(hits("-(up + 1)", Probe::Shape(Shape::UnaryExpr)));
    }

    #[test]
    fn an_infix_minus_is_not_a_unary_expression() {
        assert!(hits("up - 1", Probe::InfixOp("-")));
        assert!(!hits("up - 1", Probe::Shape(Shape::UnaryExpr)));
    }

    #[test]
    fn a_matrix_selector_and_a_subquery_are_told_apart_by_the_step_colon() {
        assert!(hits("up[5m]", Probe::Shape(Shape::MatrixSelector)));
        assert!(!hits("up[5m]", Probe::Shape(Shape::Subquery)));
        assert!(hits(
            "avg_over_time(up[5m:1m])",
            Probe::Shape(Shape::Subquery)
        ));
        assert!(!hits(
            "avg_over_time(up[5m:1m])",
            Probe::Shape(Shape::MatrixSelector)
        ));
        // A subquery with no explicit step is still a subquery.
        assert!(hits(
            "avg_over_time(up[5m:])",
            Probe::Shape(Shape::Subquery)
        ));
        // A nested shape reports both.
        let q = "max_over_time(rate(up[2m])[5m:1m])";
        assert!(hits(q, Probe::Shape(Shape::MatrixSelector)));
        assert!(hits(q, Probe::Shape(Shape::Subquery)));
    }

    #[test]
    fn a_grouping_paren_is_not_a_call_paren() {
        assert!(hits("(up)", Probe::Shape(Shape::GroupingParen)));
        assert!(!hits("rate(up[5m])", Probe::Shape(Shape::GroupingParen)));
        // `sum by (job) (up)`: the argument-list paren follows `by`, so the
        // trailing operand paren is what makes this a grouping paren.
        assert!(hits(
            "sum by (job) (up)",
            Probe::Shape(Shape::GroupingParen)
        ));
    }

    #[test]
    fn a_duration_is_not_a_number_literal() {
        assert!(!hits("up[5m]", Probe::Shape(Shape::NumberLiteral)));
        assert!(!hits("up offset 1h", Probe::Shape(Shape::NumberLiteral)));
        assert!(hits("2 + 3", Probe::Shape(Shape::NumberLiteral)));
        assert!(hits(
            "histogram_quantile(0.9, up)",
            Probe::Shape(Shape::NumberLiteral)
        ));
    }

    #[test]
    fn a_matcher_value_is_not_a_string_literal_expression() {
        assert!(!hits("up{job=\"api\"}", Probe::Shape(Shape::StringLiteral)));
        assert!(hits(
            "label_replace(up, \"a\", \"b\", \"job\", \"(.*)\")",
            Probe::Shape(Shape::StringLiteral)
        ));
    }

    #[test]
    fn a_metric_name_is_a_vector_selector_but_a_function_name_is_not() {
        assert!(hits("up", Probe::Shape(Shape::VectorSelector)));
        assert!(hits("rate(up[5m])", Probe::Shape(Shape::VectorSelector)));
        assert!(!hits("pi()", Probe::Shape(Shape::VectorSelector)));
        assert!(!hits("2 + 3", Probe::Shape(Shape::VectorSelector)));
        // `nan` is a float literal and a matcher's label name lives inside
        // braces, so the only metric name here is `up`.
        assert!(hits(
            "histogram_quantile(nan, up{le!=\"+Inf\"})",
            Probe::Shape(Shape::VectorSelector)
        ));
        assert!(!hits(
            "histogram_quantile(nan, {le!=\"+Inf\"})",
            Probe::Shape(Shape::VectorSelector)
        ));
    }

    #[test]
    fn offset_and_at_modifiers_are_recognized_including_the_negative_form() {
        assert!(hits("up offset 5m", Probe::Shape(Shape::Offset)));
        assert!(!hits("up offset 5m", Probe::Shape(Shape::NegativeOffset)));
        assert!(hits("up offset -1m", Probe::Shape(Shape::NegativeOffset)));
        assert!(hits(
            "up @ 1700000000.000",
            Probe::Shape(Shape::AtTimestamp)
        ));
        assert!(hits("up @ start()", Probe::Shape(Shape::AtStart)));
        assert!(hits("up @ end()", Probe::Shape(Shape::AtEnd)));
        assert!(!hits("up @ start()", Probe::Shape(Shape::AtEnd)));
    }

    #[test]
    fn word_operators_are_keywords_not_calls() {
        assert!(hits("a and b", Probe::Keyword("and")));
        assert!(hits("a and b", Probe::Shape(Shape::BinaryExpr)));
        assert!(hits("1 atan2 2", Probe::Keyword("atan2")));
        assert!(hits("a > bool 0", Probe::Keyword("bool")));
        assert!(hits(
            "a * on(job) group_left b",
            Probe::Keyword("group_left")
        ));
        assert!(hits("a * ignoring(job) b", Probe::Keyword("ignoring")));
    }

    #[test]
    fn tolerance_and_divergence_probes_read_the_entrys_own_fields() {
        let toleranced = parse_corpus(
            "name: t\nquery: rate(up[5m])\nkind: instant\ntime: +0s\nmode: unordered\ntolerance: 2\n",
        )
        .expect("parse")
        .pop()
        .expect("one entry");
        assert!(exercises(
            Probe::ToleranceEntry,
            &lex(&toleranced.query),
            &toleranced
        ));
        assert!(!exercises(
            Probe::DivergenceModeEntry(""),
            &lex(&toleranced.query),
            &toleranced
        ));

        let diverging = parse_corpus(
            "name: error_subquery_over_x\nquery: up\nkind: instant\ntime: +0s\nmode: ravel_error_prom_success\n",
        )
        .expect("parse")
        .pop()
        .expect("one entry");
        assert!(exercises(
            Probe::DivergenceModeEntry("subquery"),
            &lex(&diverging.query),
            &diverging
        ));
    }

    #[test]
    fn divergence_probe_marker_does_not_cross_match_a_different_divergence() {
        // Two entries can both use `mode: ravel_error_prom_success` for
        // unrelated reasons (ADR-0030's subquery point cap, issue #524's
        // histogram-binop guard); a registry row's marker must not count
        // the other's corpus entries as its own evidence.
        let histogram_entry = parse_corpus(
            "name: error_binop_over_histogram\nquery: h * 2\nkind: instant\ntime: +0s\nmode: ravel_error_prom_success\n",
        )
        .expect("parse")
        .pop()
        .expect("one entry");
        assert!(!exercises(
            Probe::DivergenceModeEntry("subquery"),
            &lex(&histogram_entry.query),
            &histogram_entry
        ));
        assert!(exercises(
            Probe::DivergenceModeEntry("histogram"),
            &lex(&histogram_entry.query),
            &histogram_entry
        ));
    }

    #[test]
    fn a_supported_row_with_no_evidence_publishes_as_unclassified() {
        let files: [CorpusFile<'_>; 0] = [];
        let report = ConformanceReport::from_corpus(&files);
        let rate = report
            .outcomes
            .iter()
            .find(|o| o.construct.name == "rate")
            .expect("rate is registered");
        assert_eq!(rate.published_state(), ConstructState::Unclassified);
    }

    #[test]
    fn a_failing_exercising_entry_demotes_a_supported_row() {
        let entries = vec![entry("rate(up[5m])")];
        let files = [CorpusFile {
            path: "corpus/rate.txt",
            entries: &entries,
        }];
        let mut report = ConformanceReport::from_corpus(&files);
        let rate_index = report
            .outcomes
            .iter()
            .position(|o| o.construct.name == "rate")
            .expect("rate is registered");
        assert!(matches!(
            report.outcomes[rate_index].published_state(),
            ConstructState::Supported { .. }
        ));

        report.apply_ravel_outcomes(&[RavelOutcome {
            entry: "t".to_string(),
            as_expected: false,
            detail: "status=error".to_string(),
        }]);
        assert_eq!(
            report.outcomes[rate_index].published_state(),
            ConstructState::Unclassified
        );
    }

    #[test]
    fn a_supported_row_citing_the_wrong_corpus_file_publishes_as_unclassified() {
        let entries = vec![entry("rate(up[5m])")];
        // `rate` cites `corpus/rate.txt`; the evidence here comes from a
        // different file, so the citation is not confirmed.
        let files = [CorpusFile {
            path: "corpus/selectors.txt",
            entries: &entries,
        }];
        let report = ConformanceReport::from_corpus(&files);
        let rate = report
            .outcomes
            .iter()
            .find(|o| o.construct.name == "rate")
            .expect("rate is registered");
        assert!(rate.cited_corpus_file_missing());
        assert_eq!(rate.published_state(), ConstructState::Unclassified);
    }

    #[test]
    fn an_unconfirmed_rejection_publishes_as_unclassified() {
        let files: [CorpusFile<'_>; 0] = [];
        let mut report = ConformanceReport::from_corpus(&files);
        let index = report
            .outcomes
            .iter()
            .position(|o| o.construct.name == "histogram_stddev")
            .expect("histogram_stddev is registered");
        // Registered state survives an absent verification...
        assert!(matches!(
            report.outcomes[index].published_state(),
            ConstructState::IntentionallyRejected { .. }
        ));
        // ...but a verification that says the query was answered does not.
        let mut results = BTreeMap::new();
        results.insert("histogram_stddev", false);
        report.apply_rejection_results(&results);
        assert_eq!(
            report.outcomes[index].published_state(),
            ConstructState::Unclassified
        );
    }

    /// A row proven by a
    /// `ravel-promql` unit test used to need no evidence at all, so a renamed
    /// or deleted test kept publishing "supported". The synthetic regression
    /// here is a name set that no longer carries the cited test.
    #[test]
    fn a_supported_row_citing_a_renamed_unit_test_publishes_as_unclassified() {
        let files: [CorpusFile<'_>; 0] = [];
        let mut report = ConformanceReport::from_corpus(&files);
        let index = report
            .outcomes
            .iter()
            .position(|o| o.construct.name == "unary expression")
            .expect("unary expression is registered");
        let cited = match report.outcomes[index].construct.state {
            ConstructState::Supported { test } => test
                .strip_prefix(UNIT_TEST_PREFIX)
                .expect("unary expression cites a ravel-promql unit test")
                .to_string(),
            other => panic!("expected a supported row, got {other:?}"),
        };

        // The run finds every cited test: the rows publish supported.
        let present: BTreeSet<String> = REGISTRY
            .iter()
            .filter_map(|c| match c.state {
                ConstructState::Supported { test } => test.strip_prefix(UNIT_TEST_PREFIX),
                _ => None,
            })
            .map(str::to_string)
            .collect();
        report.apply_unit_test_names(&present);
        assert!(matches!(
            report.outcomes[index].published_state(),
            ConstructState::Supported { .. }
        ));
        assert!(report.missing_unit_tests().is_empty());

        // That one test renamed: nothing proves its row any more.
        let renamed: BTreeSet<String> = present
            .iter()
            .map(|name| {
                if *name == cited {
                    format!("{name}_renamed")
                } else {
                    name.clone()
                }
            })
            .collect();
        report.apply_unit_test_names(&renamed);
        assert!(report.outcomes[index].cited_unit_test_missing());
        assert_eq!(
            report.outcomes[index].published_state(),
            ConstructState::Unclassified
        );
        assert_eq!(
            report.missing_unit_tests(),
            vec![(
                "unary expression",
                "ravel-promql:unary_minus_negates_value_and_drops_metric_name"
            )]
        );
        // And the table says why, rather than rendering a bare miss.
        assert!(
            report
                .render_evidence(&report.outcomes[index])
                .contains("cited `ravel-promql` unit test was not found")
        );
    }

    /// A run that never looked for the cited unit tests may not publish them
    /// as supported either: absent evidence is not evidence.
    #[test]
    fn a_unit_test_citation_is_unverified_until_the_run_looks() {
        let files: [CorpusFile<'_>; 0] = [];
        let report = ConformanceReport::from_corpus(&files);
        let outcome = report
            .outcomes
            .iter()
            .find(|o| o.construct.name == "unary expression")
            .expect("unary expression is registered");
        assert_eq!(outcome.unit_test_confirmed, None);
        assert_eq!(outcome.published_state(), ConstructState::Unclassified);
    }

    #[test]
    fn the_test_name_scan_reads_test_attributes_not_every_function() {
        let source = "\
#[test]
fn a_plain_test() {}

#[tokio::test(flavor = \"multi_thread\")]
async fn an_async_test() {}

#[test]
#[allow(clippy::expect_used)]
fn a_test_behind_another_attribute() {}

fn a_bare_helper() {}

#[allow(dead_code)]
fn an_attributed_helper() {}
";
        let names = collect_test_fn_names(source);
        assert!(names.contains("a_plain_test"));
        assert!(names.contains("an_async_test"));
        assert!(names.contains("a_test_behind_another_attribute"));
        assert!(!names.contains("a_bare_helper"));
        assert!(!names.contains("an_attributed_helper"));
    }

    /// At the registry end: every state-2 row must declare
    /// a status/`errorType` pair a run can actually compare against.
    #[test]
    fn every_rejected_row_declares_a_parseable_typed_error() {
        for construct in REGISTRY {
            if let ConstructState::IntentionallyRejected { typed_error } = construct.state {
                let tag = parse_typed_error(typed_error)
                    .unwrap_or_else(|e| panic!("{}: {e}", construct.name));
                assert!(TYPED_ERROR_TAGS.contains(&tag.error_type));
            }
        }
        for case in REJECTION_CASES {
            declared_typed_error(case.construct)
                .unwrap_or_else(|e| panic!("{}: {e}", case.construct));
        }
    }

    #[test]
    fn a_typed_error_string_without_a_checkable_tag_is_rejected() {
        assert_eq!(
            parse_typed_error("Unsupported: function call (422 execution)").expect("parses"),
            TypedErrorTag {
                status: 422,
                error_type: "execution",
            }
        );
        // No suffix, no status, a status outside 4xx/5xx, and an errorType the
        // HTTP layer never emits are all malformed rows.
        for bad in [
            "Unsupported: function call",
            "Unsupported: function call (execution)",
            "Unsupported: function call (200 execution)",
            "Unsupported: function call (422 nonsense)",
        ] {
            assert!(
                matches!(
                    parse_typed_error(bad),
                    Err(ScoringError::MalformedTypedError(_))
                ),
                "'{bad}' must not parse"
            );
        }
        // A row that is not state 2 declares no typed error at all.
        assert!(matches!(
            declared_typed_error("rate"),
            Err(ScoringError::MalformedTypedError(_))
        ));
        assert!(matches!(
            declared_typed_error("no such construct"),
            Err(ScoringError::UnknownRejectionConstruct(_))
        ));
    }

    /// A `status: "success"` envelope carrying an empty
    /// result set used to count as a passing entry, so a construct that
    /// silently stopped returning data still published as supported.
    #[test]
    fn an_empty_but_successful_answer_does_not_count_as_a_pass() {
        let empty = serde_json::json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []},
        });
        let outcome = classify_ravel_answer("selector_simple_match", false, false, &empty);
        assert!(!outcome.as_expected);
        assert!(
            outcome.detail.contains("the result set is empty"),
            "detail was '{}'",
            outcome.detail
        );
        assert_eq!(check_payload(&empty), PayloadVerdict::Empty);

        // The same entry answered with a value passes.
        let answered = serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {"__name__": "up"}, "value": [1.0, "1"]}],
            },
        });
        let outcome = classify_ravel_answer("selector_simple_match", false, false, &answered);
        assert!(outcome.as_expected, "detail was '{}'", outcome.detail);
        assert_eq!(
            check_payload(&answered),
            PayloadVerdict::Values {
                series: 1,
                samples: 1,
            }
        );

        // An entry declaring `expect: empty` is judged the other way round:
        // the empty answer is the point, and returning data means the
        // semantics it pins moved.
        assert!(
            classify_ravel_answer(
                "instant_absent_over_present_series_is_empty",
                false,
                true,
                &empty
            )
            .as_expected
        );
        assert!(
            !classify_ravel_answer(
                "instant_absent_over_present_series_is_empty",
                false,
                true,
                &answered
            )
            .as_expected
        );
    }

    /// A demoted entry demotes every construct it exercises, so the empty
    /// answer above reaches the published table.
    #[test]
    fn an_empty_but_successful_answer_demotes_the_constructs_it_exercises() {
        let entries = vec![entry("rate(up[5m])")];
        let files = [CorpusFile {
            path: "corpus/rate.txt",
            entries: &entries,
        }];
        let mut report = ConformanceReport::from_corpus(&files);
        let index = report
            .outcomes
            .iter()
            .position(|o| o.construct.name == "rate")
            .expect("rate is registered");
        let empty = serde_json::json!({
            "status": "success",
            "data": {"resultType": "matrix", "result": []},
        });
        report.apply_ravel_outcomes(&[classify_ravel_answer("t", false, false, &empty)]);
        assert_eq!(
            report.outcomes[index].published_state(),
            ConstructState::Unclassified
        );
    }

    #[test]
    fn a_payload_whose_values_do_not_parse_is_malformed_not_covered() {
        let matrix = serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [{"metric": {}, "values": [[1.0, "nonsense"]]}],
            },
        });
        assert!(matches!(
            check_payload(&matrix),
            PayloadVerdict::Malformed(_)
        ));
        assert!(!classify_ravel_answer("t", false, false, &matrix).as_expected);

        // Prometheus' own spellings of the non-finite values are values.
        let non_finite = serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [{"metric": {}, "values": [[1.0, "NaN"], [2.0, "+Inf"], [3.0, "-Inf"]]}],
            },
        });
        assert_eq!(
            check_payload(&non_finite),
            PayloadVerdict::Values {
                series: 1,
                samples: 3,
            }
        );

        // A native-histogram element carries `histograms`, not `values`.
        let histograms = serde_json::json!({
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [{"metric": {}, "histogram": [1.0, {"count": "3", "sum": "6"}]}],
            },
        });
        assert_eq!(
            check_payload(&histograms),
            PayloadVerdict::Values {
                series: 1,
                samples: 1,
            }
        );

        // A scalar and a string result are one value each.
        let scalar = serde_json::json!({
            "status": "success",
            "data": {"resultType": "scalar", "result": [1.0, "2"]},
        });
        assert_eq!(
            check_payload(&scalar),
            PayloadVerdict::Values {
                series: 1,
                samples: 1,
            }
        );
        let string = serde_json::json!({
            "status": "success",
            "data": {"resultType": "string", "result": [1.0, "hello"]},
        });
        assert_eq!(
            check_payload(&string),
            PayloadVerdict::Values {
                series: 1,
                samples: 1,
            }
        );
    }

    /// An error envelope is judged by its mode, not its payload: a
    /// `mode: error` entry passes on the rejection and fails on an answer.
    #[test]
    fn an_error_mode_entry_is_judged_on_the_rejection() {
        let rejected = serde_json::json!({
            "status": "error",
            "errorType": "bad_data",
            "error": "unexpected end of input",
        });
        assert!(classify_ravel_answer("bad_paren", true, false, &rejected).as_expected);
        let answered = serde_json::json!({
            "status": "success",
            "data": {"resultType": "vector", "result": []},
        });
        assert!(!classify_ravel_answer("bad_paren", true, false, &answered).as_expected);
    }

    #[test]
    fn score_counts_states_one_two_and_accepted_divergence() {
        let counts = StateCounts {
            supported: 6,
            intentionally_rejected: 2,
            accepted_divergence: 1,
            unclassified: 1,
        };
        assert_eq!(counts.total(), 10);
        assert_eq!(counts.conformant(), 9);
        assert_eq!(counts.score_percent(), 90.0);
        assert_eq!(StateCounts::default().score_percent(), 0.0);
    }

    #[test]
    fn splicing_replaces_only_the_marked_block() {
        let doc = format!("head\n\n{BEGIN_MARKER}\n\nold table\n{END_MARKER}\n\ntail\n");
        let spliced = splice_generated_block(&doc, "new table").expect("splice");
        assert!(spliced.starts_with("head\n"));
        assert!(spliced.ends_with("tail\n"));
        assert!(spliced.contains("new table"));
        assert!(!spliced.contains("old table"));
        // Splicing is idempotent.
        let again = splice_generated_block(&spliced, "new table").expect("splice again");
        assert_eq!(again, spliced);
    }

    #[test]
    fn splicing_a_document_without_markers_is_a_typed_error() {
        let err = splice_generated_block("no markers here", "table")
            .expect_err("must reject a document with no markers");
        assert!(matches!(err, ScoringError::Markers(_)));
    }

    #[test]
    fn every_corpus_file_probes_cleanly_and_the_registry_covers_it() {
        // Every entry in every corpus file lexes without panicking and the
        // registry recognizes at least one construct in it. An entry no
        // construct claims would be corpus coverage the table cannot see.
        let files: Vec<(&'static str, Vec<CorpusEntry>)> = vec![
            (
                "corpus/selectors.txt",
                parse_corpus(include_str!("../corpus/selectors.txt")).expect("selectors"),
            ),
            (
                "corpus/errors.txt",
                parse_corpus(include_str!("../corpus/errors.txt")).expect("errors"),
            ),
        ];
        for (path, entries) in &files {
            for entry in entries {
                // `mode: error` entries are deliberately malformed queries
                // (`sum(`, `{}`); they exercise the rejection path, not a
                // construct, so the registry is not expected to claim them.
                if entry.mode == ComparisonMode::ExpectError {
                    continue;
                }
                let tokens = lex(&entry.query);
                let claimed = REGISTRY.iter().any(|c| exercises(c.probe, &tokens, entry));
                assert!(
                    claimed,
                    "{path} entry '{}' ({}) matches no registry construct",
                    entry.name, entry.query
                );
            }
        }
    }
}
