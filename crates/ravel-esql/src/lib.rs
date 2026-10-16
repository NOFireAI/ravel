//! ES|QL front end (ADR-2887): a command-aware lexer, a structural guard, a
//! syntax tree, a parser for the released ES|QL language, and a printer back
//! to ES|QL text.
//!
//! [`parse`] and [`lex`] are the only entries that take source text. Both
//! refuse a statement over its byte limit before lexing and count tokens
//! while lexing. [`parse`] then checks bracket nesting and pipeline stages
//! over the token stream before parsing, checks nesting before each level the
//! parser descends, and counts the commands of nested pipelines as stages.
//! The limits are in [`Limits`]; the parser's own recursion is bounded by
//! [`Limits::max_depth`].
//!
//! A statement is optional `SET name = value;` directives, then a source
//! command (`FROM`, `ROW`, `TS`, `SHOW INFO` or `PROMQL`), then processing
//! commands: `WHERE`, `EVAL`, `KEEP`, `DROP`, `RENAME`, `STATS`,
//! `INLINE STATS`, `SORT`, `LIMIT`, `DISSECT`, `GROK`, `MV_EXPAND`,
//! `LOOKUP JOIN`, `ENRICH`, `FORK`, `FUSE`, `CHANGE_POINT`, `SAMPLE`,
//! `COMPLETION` and `RERANK`. `FROM` may list subqueries among its sources,
//! and `IN` may take a subquery. The query of `PROMQL` is handed whole to
//! the guarded PromQL parser, and the tree keeps the outcome. Commands that
//! exist only in development builds, such as `LOOKUP` without `JOIN` or
//! `LEFT JOIN`, are refused with [`Error::UnsupportedSyntax`]; a word the
//! parser does not know there is an [`Error::UnexpectedToken`].
//!
//! [`print`] renders a tree back to ES|QL text. When that text is itself
//! within the limits it parses to the same tree ignoring spans
//! ([`ast::Query::eq_ignoring_spans`]); the printer adds spaces and escapes,
//! so text printed from a statement at a limit can exceed it.
//!
//! [`manifest`] loads the release manifest, `manifest/esql-9.5.toml`, which
//! records each released form in Ravel's inventory and the state of each of
//! Ravel's layers for it, and renders the conformance report generated from it. Nothing at
//! runtime reads the manifest yet; only tests do.
//!
//! `ravel-sql`'s `esql` module binds and lowers the tree, and `ravel-server`
//! serves it at `POST /api/v1/esql` behind its `esql` feature.

pub mod ast;
pub mod error;
mod guard;
pub mod lexer;
pub mod limits;
pub mod manifest;
mod parser;
pub mod span;

pub use ast::print;
pub use error::Error;
pub use lexer::{Token, TokenKind, lex};
pub use limits::{LimitKind, Limits};
pub use parser::parse;
pub use span::{LineCol, Span};
