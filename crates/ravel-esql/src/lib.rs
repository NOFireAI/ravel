//! ES|QL front end (ADR-2887): a command-aware lexer, a structural guard, a
//! syntax tree, a parser for the core pipeline, and a printer back to ES|QL
//! text.
//!
//! [`parse`] is the only entry that turns text into a tree. It refuses a
//! statement over its byte limit before lexing, counts tokens while lexing,
//! checks bracket nesting and pipeline stages over the token stream before
//! parsing, and checks operator nesting before each level the parser
//! descends. The limits are in [`Limits`]; the parser's own recursion is
//! bounded by [`Limits::max_depth`].
//!
//! The parsed pipeline is a source command (`FROM` or `ROW`) followed by the
//! processing commands `WHERE`, `EVAL`, `KEEP`, `DROP`, `RENAME`, `STATS`,
//! `SORT` and `LIMIT`. Other ES|QL commands the parser knows by keyword, such
//! as `GROK`, `SHOW` or `LOOKUP JOIN`, are refused with
//! [`Error::UnsupportedSyntax`]; a word it does not know there is an
//! [`Error::UnexpectedToken`].
//!
//! [`print`] renders a tree back to ES|QL text. When that text is itself
//! within the limits it parses to the same tree ignoring spans
//! ([`ast::Query::eq_ignoring_spans`]); the printer adds spaces and escapes,
//! so text printed from a statement at a limit can exceed it.
//!
//! No production caller reaches this crate yet: binding and lowering the tree
//! into SQL plans, and serving it over HTTP, land in later changes.

pub mod ast;
pub mod error;
mod guard;
pub mod lexer;
pub mod limits;
pub mod parser;
pub mod span;

pub use ast::print;
pub use error::Error;
pub use limits::{LimitKind, Limits};
pub use parser::parse;
pub use span::{LineCol, Span};
