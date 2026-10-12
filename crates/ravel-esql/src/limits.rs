//! Structural limits enforced before and during parsing.

use std::fmt;

/// Structural limits on one statement.
///
/// [`crate::parse`] checks them in this order: the statement's byte length
/// before lexing, the token count while lexing, then bracket nesting depth
/// and pipeline stage count over the token stream before parsing starts.
/// Nesting that comes from operators rather than brackets is checked by the
/// parser before it descends a level or builds a node one level deeper.
///
/// The defaults are sized so that the deepest tree they admit parses, prints
/// and drops on a thread with a 2 MiB stack; `tests/limits.rs` runs each
/// worst-case shape there. The tightest shape, nested function calls, still
/// fitted at a depth of 164 in an unoptimised build when the default of 128
/// was chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Maximum statement length in bytes.
    pub max_statement_bytes: usize,
    /// Maximum number of tokens, not counting whitespace and comments.
    pub max_tokens: usize,
    /// Maximum expression nesting depth. A parenthesis, bracket or argument
    /// list opens one level, each operator operand the parser descends into
    /// opens one level, and the syntax tree of an expression may be no taller
    /// than this many nodes. An opening bracket at bracket depth `d` counts
    /// as level `d + 1`, the expression around it being level 1.
    pub max_depth: usize,
    /// Maximum number of pipeline stages, the source command included.
    pub max_pipeline_stages: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_statement_bytes: 256 * 1024,
            max_tokens: 65_536,
            max_depth: 128,
            max_pipeline_stages: 100,
        }
    }
}

/// Which structural limit a statement exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LimitKind {
    /// [`Limits::max_statement_bytes`].
    StatementBytes,
    /// [`Limits::max_tokens`].
    Tokens,
    /// [`Limits::max_depth`].
    NestingDepth,
    /// [`Limits::max_pipeline_stages`].
    PipelineStages,
}

impl fmt::Display for LimitKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            LimitKind::StatementBytes => "statement byte",
            LimitKind::Tokens => "token",
            LimitKind::NestingDepth => "nesting depth",
            LimitKind::PipelineStages => "pipeline stage",
        })
    }
}
