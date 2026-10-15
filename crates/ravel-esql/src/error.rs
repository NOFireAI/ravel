//! Typed errors for lexing, structural limits and parsing.

use crate::limits::LimitKind;
use crate::span::{LineCol, Span};

/// Why a statement was refused. Every variant carries the [`Span`] of the
/// offending text; [`Error::line_col`] turns it into a 1-based line and
/// column against the statement.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A structural limit was exceeded. For [`LimitKind::NestingDepth`] the
    /// message adds that the limit bounds syntax tree height, so a flat chain
    /// such as `a OR a OR ...` with more operands than the limit is refused.
    #[error("{limit} limit of {max} exceeded at byte {}{}", .span.start, .limit.note())]
    LimitExceeded {
        /// Which limit.
        limit: LimitKind,
        /// The limit's configured value.
        max: usize,
        /// The first text past the limit.
        span: Span,
    },

    /// A string literal has no closing quote before the end of its line (for
    /// `"..."`) or of the statement (for `"""..."""`).
    #[error("unterminated string literal starting at byte {}", .span.start)]
    UnterminatedString {
        /// The opening quote or quotes.
        span: Span,
    },

    /// A backtick-quoted identifier has no closing backtick.
    #[error("unterminated quoted identifier starting at byte {}", .span.start)]
    UnterminatedQuotedIdentifier {
        /// The opening backtick.
        span: Span,
    },

    /// A block comment has no matching `*/`.
    #[error("unterminated block comment starting at byte {}", .span.start)]
    UnterminatedComment {
        /// The outermost opening `/*`.
        span: Span,
    },

    /// The query of a `PROMQL` command has no `)` matching its `(`, or ends
    /// inside a PromQL string.
    #[error("unterminated PROMQL query starting at byte {}", .span.start)]
    UnterminatedPromql {
        /// The query's opening parenthesis.
        span: Span,
    },

    /// A backslash escape in a string literal other than `\"`, `\\`, `\n`,
    /// `\r` or `\t`.
    #[error("invalid escape sequence at byte {}", .span.start)]
    InvalidEscape {
        /// The backslash and the character after it.
        span: Span,
    },

    /// A character that starts no token.
    #[error("unexpected character {found:?} at byte {}", .span.start)]
    UnexpectedCharacter {
        /// The character.
        found: char,
        /// The character's bytes.
        span: Span,
    },

    /// A token, or the end of input, where the grammar expects something else.
    #[error("unexpected {found} at byte {}; expected {expected}", .span.start)]
    UnexpectedToken {
        /// The token's text in backticks (truncated), or `end of input`.
        found: String,
        /// What the grammar admits here.
        expected: &'static str,
        /// The token, or an empty span at the end of input.
        span: Span,
    },

    /// An integer literal that must fit in an unsigned 64-bit value does not.
    #[error("integer literal {text} at byte {} does not fit in 64 bits", .span.start)]
    IntegerOutOfRange {
        /// The literal's digits.
        text: String,
        /// The literal.
        span: Span,
    },

    /// An ES|QL command this parser recognises by keyword and refuses: one
    /// that exists only in development builds of Elasticsearch, or that the
    /// released language this parser follows does not include.
    #[error("the {command} command is not supported yet (byte {})", .span.start)]
    UnsupportedSyntax {
        /// The command's keywords, upper-cased and separated by one space,
        /// such as `GROK` or `LOOKUP JOIN`.
        command: String,
        /// From the command's first keyword to its last.
        span: Span,
    },
}

impl Error {
    /// The span of the offending text.
    pub fn span(&self) -> Span {
        match self {
            Error::LimitExceeded { span, .. }
            | Error::UnterminatedString { span }
            | Error::UnterminatedQuotedIdentifier { span }
            | Error::UnterminatedComment { span }
            | Error::UnterminatedPromql { span }
            | Error::InvalidEscape { span }
            | Error::UnexpectedCharacter { span, .. }
            | Error::UnexpectedToken { span, .. }
            | Error::IntegerOutOfRange { span, .. }
            | Error::UnsupportedSyntax { span, .. } => *span,
        }
    }

    /// The 1-based line and column of the error in `source`, the statement
    /// it was produced from.
    // guarded-esql-parse-allow: counts lines and characters; reads no syntax.
    pub fn line_col(&self, source: &str) -> LineCol {
        self.span().line_col(source)
    }
}
