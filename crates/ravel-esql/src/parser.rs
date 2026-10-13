//! Recursive-descent parser for the core pipeline, with a Pratt parser for
//! expressions.
//!
//! Expression binding, loosest first: `OR`; `AND`; prefix `NOT`; the
//! comparisons, `:`, `IN`, `LIKE`, `RLIKE` and `IS NULL`, which do not chain;
//! `+ -`; `* / %`; prefix `-` and `+`; the postfix `::` cast. The binary
//! operators that chain associate to the left.
//!
//! An infix operator takes the expression before it as its left operand only
//! when that expression binds more tightly, or as tightly and the operator
//! chains. Because `IN`, `LIKE`, `RLIKE`, `IS NULL` and `:` end at a list,
//! a pattern, `NULL` or a constant rather than at a parsed operand, an
//! arithmetic operator, a cast or a comparison can follow one directly; it is
//! refused there with [`Error::UnexpectedToken`] at the operator, so a
//! comparison-level node is an operand of those operators only when the
//! source parenthesised it.
//!
//! Every descent into a nested expression level increments a depth counter
//! that is checked against [`Limits::max_depth`] before the level is parsed,
//! and every node is checked against the same limit as it is built, so
//! neither the parser's recursion nor the finished tree exceeds the limit.

use crate::ast::{
    Aggregate, ArrayLiteral, Binary, BinaryOp, Call, CallArgs, Cast, Command, DropCommand,
    EvalCommand, Expr, Field, FromCommand, InList, Interval, IsNull, KeepCommand, Like, LikeKind,
    LimitCommand, Literal, LiteralValue, Metadata, NamePart, NullsOrder, Parameter, QualifiedName,
    Query, RenameCommand, RenamePair, RowCommand, SortCommand, SortKey, SortOrder, SourcePattern,
    StatsCommand, Unary, UnaryOp, WhereCommand, interval_unit_kind, prec,
};
use crate::error::Error;
use crate::guard;
use crate::lexer::{Token, TokenKind, tokenize};
use crate::limits::{LimitKind, Limits};
use crate::span::Span;

/// Words that cannot be used as unquoted names.
const RESERVED: &[&str] = &[
    "AND", "AS", "ASC", "BY", "DESC", "FALSE", "IN", "IS", "LIKE", "NOT", "NULL", "NULLS", "OR",
    "RLIKE", "TRUE", "WHERE",
];

/// Source commands recognised by keyword and not parsed yet, each as the
/// words that name it.
const UNSUPPORTED_SOURCE: &[&[&str]] = &[&["EXPLAIN"], &["METRICS"], &["SET"], &["SHOW"], &["TS"]];

/// Processing commands recognised by keyword and not parsed yet, each as the
/// words that name it. The first entry whose words all match is reported, so
/// a two-word form precedes a one-word form sharing its first word.
const UNSUPPORTED_PROCESSING: &[&[&str]] = &[
    &["CHANGE_POINT"],
    &["COMPLETION"],
    &["DISSECT"],
    &["ENRICH"],
    &["FORK"],
    &["FUSE"],
    &["GROK"],
    &["INLINE", "STATS"],
    &["INLINESTATS"],
    &["JOIN"],
    &["LOOKUP", "JOIN"],
    &["LOOKUP"],
    &["MV_EXPAND"],
    &["RERANK"],
    &["SAMPLE"],
];

const FOUND_MAX_CHARS: usize = 40;

/// Parses one ES|QL statement.
///
/// Checks run in this order: the statement's byte length against
/// [`Limits::max_statement_bytes`]; lexing under [`Limits::max_tokens`];
/// bracket depth and pipeline stages over the tokens; then parsing, which
/// bounds operator nesting by [`Limits::max_depth`].
pub fn parse(source: &str, limits: &Limits) -> Result<Query, Error> {
    if source.len() > limits.max_statement_bytes {
        return Err(Error::LimitExceeded {
            limit: LimitKind::StatementBytes,
            max: limits.max_statement_bytes,
            span: Span::new(limits.max_statement_bytes, source.len()),
        });
    }
    let tokens = tokenize(source, limits)?;
    guard::check(&tokens, limits)?;
    let mut parser = Parser {
        source,
        tokens,
        pos: 0,
        depth: 0,
        limits,
    };
    parser.query()
}

/// A parsed expression with the bookkeeping the Pratt loop needs.
struct Parsed {
    expr: Expr,
    /// Nesting levels, parentheses included.
    height: usize,
    /// The binding power of the outermost operator; a parenthesised
    /// expression binds like a primary.
    bound: u8,
}

/// An infix operator recognised at the current token.
enum Infix {
    Binary(BinaryOp),
    In { negated: bool },
    Like { kind: LikeKind, negated: bool },
    IsNull,
    Cast,
}

impl Infix {
    fn precedence(&self) -> u8 {
        match self {
            Infix::Binary(op) => op.precedence(),
            Infix::In { .. } | Infix::Like { .. } | Infix::IsNull => prec::COMPARISON,
            Infix::Cast => prec::CAST,
        }
    }
}

/// Whether an operator binding at `lbp` may take an unparenthesised left
/// operand whose outermost operator binds at `bound`: one that binds more
/// tightly, or as tightly when the operator chains. Comparisons do not chain.
fn takes_left_operand(lbp: u8, bound: u8) -> bool {
    bound > lbp || (bound == lbp && lbp != prec::COMPARISON)
}

/// What [`takes_left_operand`] refusing an operator leaves admissible. Only a
/// comparison-level left operand is refused: every other operand was parsed
/// at a binding power that already consumed the tighter operators.
const AFTER_COMPARISON: &str = "`AND`, `OR`, `|` or end of input (a comparison, `:`, `IN`, \
     `LIKE`, `RLIKE` or `IS NULL` does not chain and is not an operand of arithmetic or `::` \
     unless parenthesised)";

/// What kind of constant an array holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ConstantKind {
    Number,
    Boolean,
    String,
    Other,
}

struct Parser<'a> {
    source: &'a str,
    tokens: Vec<Token>,
    pos: usize,
    depth: usize,
    limits: &'a Limits,
}

impl<'a> Parser<'a> {
    // ---- token helpers ----

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn peek_kind(&self) -> Option<&TokenKind> {
        self.peek().map(|t| &t.kind)
    }

    fn peek_at(&self, offset: usize) -> Option<&Token> {
        self.tokens.get(self.pos + offset)
    }

    fn text(&self, span: Span) -> &'a str {
        span.slice(self.source).unwrap_or("")
    }

    fn bump(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    fn token_is_word(&self, token: Option<&Token>, keyword: &str) -> bool {
        token.is_some_and(|t| {
            t.kind == TokenKind::Word && self.text(t.span).eq_ignore_ascii_case(keyword)
        })
    }

    fn is_word(&self, keyword: &str) -> bool {
        self.token_is_word(self.peek(), keyword)
    }

    fn eat_word(&mut self, keyword: &str) -> Option<Span> {
        if self.is_word(keyword) {
            self.bump().map(|t| t.span)
        } else {
            None
        }
    }

    fn expect_word(&mut self, keyword: &str, expected: &'static str) -> Result<Span, Error> {
        self.eat_word(keyword)
            .ok_or_else(|| self.unexpected(expected))
    }

    fn eat(&mut self, kind: &TokenKind) -> Option<Span> {
        if self.peek_kind() == Some(kind) {
            self.bump().map(|t| t.span)
        } else {
            None
        }
    }

    fn expect(&mut self, kind: &TokenKind, expected: &'static str) -> Result<Span, Error> {
        self.eat(kind).ok_or_else(|| self.unexpected(expected))
    }

    fn at_command_end(&self) -> bool {
        matches!(self.peek_kind(), None | Some(TokenKind::Pipe))
    }

    /// The span of the current token, or an empty span at the end of input.
    fn here(&self) -> Span {
        self.peek()
            .map_or(Span::new(self.source.len(), self.source.len()), |t| t.span)
    }

    fn unexpected(&self, expected: &'static str) -> Error {
        match self.peek() {
            Some(token) => {
                let text: String = self
                    .text(token.span)
                    .chars()
                    .take(FOUND_MAX_CHARS)
                    .collect();
                Error::UnexpectedToken {
                    found: format!("`{text}`"),
                    expected,
                    span: token.span,
                }
            }
            None => Error::UnexpectedToken {
                found: "end of input".to_owned(),
                expected,
                span: self.here(),
            },
        }
    }

    fn depth_exceeded(&self, span: Span) -> Error {
        Error::LimitExceeded {
            limit: LimitKind::NestingDepth,
            max: self.limits.max_depth,
            span,
        }
    }

    // ---- query and commands ----

    fn query(&mut self) -> Result<Query, Error> {
        let source = self.source_command()?;
        let mut processing = Vec::new();
        loop {
            if self.eat(&TokenKind::Pipe).is_some() {
                processing.push(self.processing_command()?);
            } else if self.peek().is_some() {
                return Err(self.unexpected("`|` or end of input"));
            } else {
                break;
            }
        }
        let end = processing.last().map_or(source.span(), Command::span);
        Ok(Query {
            span: source.span().to(end),
            source,
            processing,
        })
    }

    /// The upper-cased keyword at the current token, if it is a word.
    fn keyword(&self) -> Option<(String, Span)> {
        let token = self.peek()?;
        (token.kind == TokenKind::Word)
            .then(|| (self.text(token.span).to_ascii_uppercase(), token.span))
    }

    fn source_command(&mut self) -> Result<Command, Error> {
        const EXPECTED: &str = "a source command (`FROM` or `ROW`)";
        let Some((keyword, span)) = self.keyword() else {
            return Err(self.unexpected(EXPECTED));
        };
        match keyword.as_str() {
            "FROM" => self.parse_from_command(span),
            "ROW" => self.row_command(span),
            _ => Err(self
                .unsupported(UNSUPPORTED_SOURCE)
                .unwrap_or_else(|| self.unexpected(EXPECTED))),
        }
    }

    fn processing_command(&mut self) -> Result<Command, Error> {
        const EXPECTED: &str = "a processing command";
        let Some((keyword, span)) = self.keyword() else {
            return Err(self.unexpected(EXPECTED));
        };
        match keyword.as_str() {
            "WHERE" => self.where_command(span),
            "EVAL" => self.eval_command(span),
            "KEEP" => self.keep_command(span),
            "DROP" => self.drop_command(span),
            "RENAME" => self.rename_command(span),
            "STATS" => self.stats_command(span),
            "SORT" => self.sort_command(span),
            "LIMIT" => self.limit_command(span),
            _ => Err(self
                .unsupported(UNSUPPORTED_PROCESSING)
                .unwrap_or_else(|| self.unexpected(EXPECTED))),
        }
    }

    /// The [`Error::UnsupportedSyntax`] for the first of `commands` whose
    /// words start at the current token, spanning all of them.
    fn unsupported(&self, commands: &[&[&str]]) -> Option<Error> {
        commands.iter().find_map(|words| {
            let mut span: Option<Span> = None;
            for (offset, word) in words.iter().enumerate() {
                let token = self.peek_at(offset)?;
                if !self.token_is_word(Some(token), word) {
                    return None;
                }
                span = Some(span.map_or(token.span, |s| s.to(token.span)));
            }
            Some(Error::UnsupportedSyntax {
                command: words.join(" "),
                span: span?,
            })
        })
    }

    /// Parses `item (, item)*`.
    fn comma_list<T>(
        &mut self,
        mut item: impl FnMut(&mut Self) -> Result<T, Error>,
    ) -> Result<Vec<T>, Error> {
        let mut items = vec![item(self)?];
        while self.eat(&TokenKind::Comma).is_some() {
            items.push(item(self)?);
        }
        Ok(items)
    }

    fn parse_from_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let sources = self.comma_list(|p| p.source_pattern())?;
        let mut end = sources.last().map_or(keyword, |s| s.span);
        let metadata = match self.eat_word("METADATA") {
            Some(start) => {
                let fields = self.comma_list(|p| p.qualified_name())?;
                let span = start.to(fields.last().map_or(start, |f| f.span));
                end = span;
                Some(Metadata { fields, span })
            }
            None => None,
        };
        Ok(Command::From(FromCommand {
            sources,
            metadata,
            span: keyword.to(end),
        }))
    }

    fn source_pattern(&mut self) -> Result<SourcePattern, Error> {
        match self.peek() {
            Some(Token {
                kind: TokenKind::SourcePattern,
                span,
            }) => {
                let span = *span;
                self.bump();
                Ok(SourcePattern {
                    text: self.text(span).to_owned(),
                    quoted: false,
                    span,
                })
            }
            Some(Token {
                kind: TokenKind::String(value),
                span,
            }) => {
                let pattern = SourcePattern {
                    text: value.clone(),
                    quoted: true,
                    span: *span,
                };
                self.bump();
                Ok(pattern)
            }
            _ => Err(self.unexpected("a source pattern")),
        }
    }

    fn row_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let fields = self.comma_list(|p| p.field())?;
        let end = fields.last().map_or(keyword, |f| f.span);
        Ok(Command::Row(RowCommand {
            fields,
            span: keyword.to(end),
        }))
    }

    fn where_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let condition = self.expr()?;
        Ok(Command::Where(WhereCommand {
            span: keyword.to(condition.span()),
            condition,
        }))
    }

    fn eval_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let fields = self.comma_list(|p| p.field())?;
        let end = fields.last().map_or(keyword, |f| f.span);
        Ok(Command::Eval(EvalCommand {
            fields,
            span: keyword.to(end),
        }))
    }

    fn keep_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let patterns = self.comma_list(|p| p.name_pattern())?;
        let end = patterns.last().map_or(keyword, |n| n.span);
        Ok(Command::Keep(KeepCommand {
            patterns,
            span: keyword.to(end),
        }))
    }

    fn drop_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let patterns = self.comma_list(|p| p.name_pattern())?;
        let end = patterns.last().map_or(keyword, |n| n.span);
        Ok(Command::Drop(DropCommand {
            patterns,
            span: keyword.to(end),
        }))
    }

    fn rename_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let renames = self.comma_list(|p| {
            let old = p.qualified_name()?;
            p.expect_word("AS", "`AS`")?;
            let new = p.qualified_name()?;
            Ok(RenamePair {
                span: old.span.to(new.span),
                old,
                new,
            })
        })?;
        let end = renames.last().map_or(keyword, |r| r.span);
        Ok(Command::Rename(RenameCommand {
            renames,
            span: keyword.to(end),
        }))
    }

    fn stats_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let mut end = keyword;
        let mut aggregates = Vec::new();
        if !self.at_command_end() && !self.is_word("BY") {
            aggregates = self.comma_list(|p| {
                let field = p.field()?;
                let filter = match p.eat_word("WHERE") {
                    Some(_) => Some(p.expr()?),
                    None => None,
                };
                let span = field
                    .span
                    .to(filter.as_ref().map_or(field.span, Expr::span));
                Ok(Aggregate {
                    field,
                    filter,
                    span,
                })
            })?;
            end = aggregates.last().map_or(end, |a| a.span);
        }
        let mut groupings = Vec::new();
        if self.eat_word("BY").is_some() {
            groupings = self.comma_list(|p| p.field())?;
            end = groupings.last().map_or(end, |g| g.span);
        }
        if aggregates.is_empty() && groupings.is_empty() {
            return Err(self.unexpected("an aggregate or `BY`"));
        }
        Ok(Command::Stats(StatsCommand {
            aggregates,
            groupings,
            span: keyword.to(end),
        }))
    }

    fn sort_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let keys = self.comma_list(|p| {
            let expr = p.expr()?;
            let mut span = expr.span();
            let order = if let Some(s) = p.eat_word("ASC") {
                span = span.to(s);
                Some(SortOrder::Asc)
            } else if let Some(s) = p.eat_word("DESC") {
                span = span.to(s);
                Some(SortOrder::Desc)
            } else {
                None
            };
            let nulls = if p.eat_word("NULLS").is_some() {
                if let Some(s) = p.eat_word("FIRST") {
                    span = span.to(s);
                    Some(NullsOrder::First)
                } else {
                    let s = p.expect_word("LAST", "`FIRST` or `LAST`")?;
                    span = span.to(s);
                    Some(NullsOrder::Last)
                }
            } else {
                None
            };
            Ok(SortKey {
                expr,
                order,
                nulls,
                span,
            })
        })?;
        let end = keys.last().map_or(keyword, |k| k.span);
        Ok(Command::Sort(SortCommand {
            keys,
            span: keyword.to(end),
        }))
    }

    fn limit_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let Some(Token {
            kind: TokenKind::Integer,
            span,
        }) = self.peek().cloned()
        else {
            return Err(self.unexpected("an integer row count"));
        };
        let text = self.text(span);
        let count = text.parse::<u64>().map_err(|_| Error::IntegerOutOfRange {
            text: text.to_owned(),
            span,
        })?;
        self.bump();
        Ok(Command::Limit(LimitCommand {
            count,
            span: keyword.to(span),
        }))
    }

    // ---- names and fields ----

    fn name_part(&mut self, allow_pattern: bool) -> Result<NamePart, Error> {
        match self.peek() {
            Some(Token {
                kind: TokenKind::Word,
                span,
            }) => {
                let span = *span;
                let text = self.text(span);
                if RESERVED.iter().any(|r| r.eq_ignore_ascii_case(text)) {
                    return Err(self.unexpected("a name"));
                }
                self.bump();
                Ok(NamePart {
                    text: text.to_owned(),
                    quoted: false,
                    span,
                })
            }
            Some(Token {
                kind: TokenKind::NamePattern,
                span,
            }) if allow_pattern => {
                let span = *span;
                self.bump();
                Ok(NamePart {
                    text: self.text(span).to_owned(),
                    quoted: false,
                    span,
                })
            }
            Some(Token {
                kind: TokenKind::QuotedIdent(value),
                span,
            }) => {
                let part = NamePart {
                    text: value.clone(),
                    quoted: true,
                    span: *span,
                };
                self.bump();
                Ok(part)
            }
            _ => Err(self.unexpected("a name")),
        }
    }

    fn dotted_name(&mut self, allow_pattern: bool) -> Result<QualifiedName, Error> {
        let mut parts = vec![self.name_part(allow_pattern)?];
        while self.eat(&TokenKind::Dot).is_some() {
            parts.push(self.name_part(allow_pattern)?);
        }
        let span = match (parts.first(), parts.last()) {
            (Some(first), Some(last)) => first.span.to(last.span),
            _ => self.here(),
        };
        Ok(QualifiedName { parts, span })
    }

    fn qualified_name(&mut self) -> Result<QualifiedName, Error> {
        self.dotted_name(false)
    }

    fn name_pattern(&mut self) -> Result<QualifiedName, Error> {
        self.dotted_name(true)
    }

    /// `name = expr` or `expr`.
    fn field(&mut self) -> Result<Field, Error> {
        let save = self.pos;
        if let Ok(name) = self.qualified_name()
            && self.eat(&TokenKind::Assign).is_some()
        {
            let expr = self.expr()?;
            return Ok(Field {
                span: name.span.to(expr.span()),
                name: Some(name),
                expr,
            });
        }
        self.pos = save;
        let expr = self.expr()?;
        Ok(Field {
            name: None,
            span: expr.span(),
            expr,
        })
    }

    // ---- expressions ----

    fn expr(&mut self) -> Result<Expr, Error> {
        Ok(self.expr_bp(0)?.expr)
    }

    /// Parses an expression whose operators bind at least as tightly as
    /// `min_bp`, one nesting level below the caller.
    fn expr_bp(&mut self, min_bp: u8) -> Result<Parsed, Error> {
        self.depth += 1;
        if self.depth > self.limits.max_depth {
            return Err(self.depth_exceeded(self.here()));
        }
        let result = self.expr_loop(min_bp);
        self.depth -= 1;
        result
    }

    /// Wraps `expr` as a node one level above its tallest child.
    fn node(&self, expr: Expr, child_height: usize, bound: u8) -> Result<Parsed, Error> {
        let height = child_height + 1;
        if height > self.limits.max_depth {
            return Err(self.depth_exceeded(expr.span()));
        }
        Ok(Parsed {
            expr,
            height,
            bound,
        })
    }

    fn infix(&self) -> Option<Infix> {
        let token = self.peek()?;
        let op = match token.kind {
            TokenKind::Eq => BinaryOp::Eq,
            TokenKind::NotEq => BinaryOp::NotEq,
            TokenKind::Lt => BinaryOp::Lt,
            TokenKind::LtEq => BinaryOp::LtEq,
            TokenKind::Gt => BinaryOp::Gt,
            TokenKind::GtEq => BinaryOp::GtEq,
            TokenKind::Colon => BinaryOp::Match,
            TokenKind::Cast => return Some(Infix::Cast),
            TokenKind::Plus => BinaryOp::Add,
            TokenKind::Minus => BinaryOp::Sub,
            TokenKind::Star => BinaryOp::Mul,
            TokenKind::Slash => BinaryOp::Div,
            TokenKind::Percent => BinaryOp::Mod,
            TokenKind::Word => {
                let word = self.text(token.span);
                let next = self.peek_at(1);
                return if word.eq_ignore_ascii_case("OR") {
                    Some(Infix::Binary(BinaryOp::Or))
                } else if word.eq_ignore_ascii_case("AND") {
                    Some(Infix::Binary(BinaryOp::And))
                } else if word.eq_ignore_ascii_case("IN") {
                    Some(Infix::In { negated: false })
                } else if word.eq_ignore_ascii_case("LIKE") {
                    Some(Infix::Like {
                        kind: LikeKind::Like,
                        negated: false,
                    })
                } else if word.eq_ignore_ascii_case("RLIKE") {
                    Some(Infix::Like {
                        kind: LikeKind::RLike,
                        negated: false,
                    })
                } else if word.eq_ignore_ascii_case("IS") {
                    Some(Infix::IsNull)
                } else if word.eq_ignore_ascii_case("NOT") {
                    if self.token_is_word(next, "IN") {
                        Some(Infix::In { negated: true })
                    } else if self.token_is_word(next, "LIKE") {
                        Some(Infix::Like {
                            kind: LikeKind::Like,
                            negated: true,
                        })
                    } else if self.token_is_word(next, "RLIKE") {
                        Some(Infix::Like {
                            kind: LikeKind::RLike,
                            negated: true,
                        })
                    } else {
                        None
                    }
                } else {
                    None
                };
            }
            _ => return None,
        };
        Some(Infix::Binary(op))
    }

    fn expr_loop(&mut self, min_bp: u8) -> Result<Parsed, Error> {
        let mut lhs = self.prefix(min_bp)?;
        while let Some(infix) = self.infix() {
            let lbp = infix.precedence();
            if lbp < min_bp {
                break;
            }
            if !takes_left_operand(lbp, lhs.bound) {
                return Err(self.unexpected(AFTER_COMPARISON));
            }
            lhs = match infix {
                Infix::Binary(BinaryOp::Match) => self.match_rhs(lhs)?,
                Infix::Binary(op) => {
                    self.bump();
                    let rhs = self.expr_bp(lbp + 1)?;
                    let span = lhs.expr.span().to(rhs.expr.span());
                    let height = lhs.height.max(rhs.height);
                    let expr = Expr::Binary(Binary {
                        op,
                        left: Box::new(lhs.expr),
                        right: Box::new(rhs.expr),
                        span,
                    });
                    self.node(expr, height, lbp)?
                }
                Infix::In { negated } => self.in_rhs(lhs, negated)?,
                Infix::Like { kind, negated } => self.like_rhs(lhs, kind, negated)?,
                Infix::IsNull => self.is_null_rhs(lhs)?,
                Infix::Cast => self.cast_rhs(lhs)?,
            };
        }
        Ok(lhs)
    }

    fn cast_rhs(&mut self, lhs: Parsed) -> Result<Parsed, Error> {
        self.bump();
        let type_span = match self.peek() {
            Some(Token {
                kind: TokenKind::Word,
                span,
            }) => *span,
            _ => return Err(self.unexpected("a type name")),
        };
        self.bump();
        let span = lhs.expr.span().to(type_span);
        let cast = Expr::Cast(Cast {
            expr: Box::new(lhs.expr),
            type_name: self.text(type_span).to_owned(),
            span,
        });
        self.node(cast, lhs.height, prec::CAST)
    }

    fn match_rhs(&mut self, lhs: Parsed) -> Result<Parsed, Error> {
        let is_field = match &lhs.expr {
            Expr::Field(_) => true,
            Expr::Cast(cast) => matches!(*cast.expr, Expr::Field(_)),
            _ => false,
        };
        if !is_field || lhs.bound < prec::CAST {
            return Err(self.unexpected("an operator; `:` needs a field on its left"));
        }
        self.bump();
        let (rhs, _) = self.constant(true)?;
        let span = lhs.expr.span().to(rhs.expr.span());
        let height = lhs.height.max(rhs.height);
        let expr = Expr::Binary(Binary {
            op: BinaryOp::Match,
            left: Box::new(lhs.expr),
            right: Box::new(rhs.expr),
            span,
        });
        self.node(expr, height, prec::COMPARISON)
    }

    fn in_rhs(&mut self, lhs: Parsed, negated: bool) -> Result<Parsed, Error> {
        if negated {
            self.bump();
        }
        self.bump();
        self.expect(&TokenKind::LParen, "`(`")?;
        let mut height = lhs.height;
        let mut list = Vec::new();
        loop {
            let element = self.expr_bp(prec::COMPARISON)?;
            height = height.max(element.height);
            list.push(element.expr);
            if self.eat(&TokenKind::Comma).is_none() {
                break;
            }
        }
        let close = self.expect(&TokenKind::RParen, "`,` or `)`")?;
        let span = lhs.expr.span().to(close);
        let expr = Expr::In(InList {
            expr: Box::new(lhs.expr),
            list,
            negated,
            span,
        });
        self.node(expr, height, prec::COMPARISON)
    }

    fn like_rhs(&mut self, lhs: Parsed, kind: LikeKind, negated: bool) -> Result<Parsed, Error> {
        if negated {
            self.bump();
        }
        self.bump();
        let Some(Token {
            kind: TokenKind::String(pattern),
            span,
        }) = self.peek().cloned()
        else {
            return Err(self.unexpected("a pattern string"));
        };
        self.bump();
        let full = lhs.expr.span().to(span);
        let expr = Expr::Like(Like {
            expr: Box::new(lhs.expr),
            kind,
            pattern,
            negated,
            span: full,
        });
        self.node(expr, lhs.height, prec::COMPARISON)
    }

    fn is_null_rhs(&mut self, lhs: Parsed) -> Result<Parsed, Error> {
        self.bump();
        let negated = self.eat_word("NOT").is_some();
        let null = self.expect_word("NULL", "`NULL` or `NOT NULL`")?;
        let span = lhs.expr.span().to(null);
        let expr = Expr::IsNull(IsNull {
            expr: Box::new(lhs.expr),
            negated,
            span,
        });
        self.node(expr, lhs.height, prec::COMPARISON)
    }

    fn prefix(&mut self, min_bp: u8) -> Result<Parsed, Error> {
        let Some(token) = self.peek().cloned() else {
            return Err(self.unexpected("an expression"));
        };
        let op = match token.kind {
            TokenKind::Word if self.token_is_word(Some(&token), "NOT") => {
                if min_bp > prec::NOT {
                    return Err(self.unexpected("an expression (parenthesise a `NOT` here)"));
                }
                Some((UnaryOp::Not, prec::NOT))
            }
            TokenKind::Minus => Some((UnaryOp::Minus, prec::UNARY)),
            TokenKind::Plus => Some((UnaryOp::Plus, prec::UNARY)),
            _ => None,
        };
        let Some((op, bp)) = op else {
            return self.primary();
        };
        self.bump();
        let operand = self.expr_bp(bp)?;
        let span = token.span.to(operand.expr.span());
        let expr = Expr::Unary(Unary {
            op,
            operand: Box::new(operand.expr),
            span,
        });
        self.node(expr, operand.height, bp)
    }

    fn leaf(&self, expr: Expr) -> Result<Parsed, Error> {
        self.node(expr, 0, prec::PRIMARY)
    }

    fn primary(&mut self) -> Result<Parsed, Error> {
        let Some(token) = self.peek().cloned() else {
            return Err(self.unexpected("an expression"));
        };
        match &token.kind {
            TokenKind::Integer => {
                self.bump();
                let text = self.text(token.span).to_owned();
                if let Some(unit) = self.peek().cloned() {
                    let unit_text = self.text(unit.span);
                    if unit.kind == TokenKind::Word && interval_unit_kind(unit_text).is_some() {
                        self.bump();
                        return self.leaf(Expr::Interval(Interval {
                            value: text,
                            unit: unit_text.to_owned(),
                            span: token.span.to(unit.span),
                        }));
                    }
                }
                self.leaf(Expr::Literal(Literal {
                    value: LiteralValue::Integer(text),
                    span: token.span,
                }))
            }
            TokenKind::Decimal => {
                self.bump();
                self.leaf(Expr::Literal(Literal {
                    value: LiteralValue::Decimal(self.text(token.span).to_owned()),
                    span: token.span,
                }))
            }
            TokenKind::String(value) => {
                self.bump();
                self.leaf(Expr::Literal(Literal {
                    value: LiteralValue::String(value.clone()),
                    span: token.span,
                }))
            }
            TokenKind::Param(name) | TokenKind::IdentParam(name) => {
                self.bump();
                self.leaf(Expr::Param(Parameter {
                    name: name.clone(),
                    identifier: matches!(token.kind, TokenKind::IdentParam(_)),
                    span: token.span,
                }))
            }
            TokenKind::LParen => {
                self.bump();
                let mut inner = self.expr_bp(0)?;
                let close = self.expect(&TokenKind::RParen, "`)`")?;
                *inner.expr.span_mut() = token.span.to(close);
                let height = inner.height;
                self.node(inner.expr, height, prec::PRIMARY)
            }
            TokenKind::LBracket => self.array(token.span),
            TokenKind::QuotedIdent(_) => {
                let name = self.qualified_name()?;
                self.leaf(Expr::Field(name))
            }
            TokenKind::Word => {
                let text = self.text(token.span);
                let literal = if text.eq_ignore_ascii_case("NULL") {
                    Some(LiteralValue::Null)
                } else if text.eq_ignore_ascii_case("TRUE") {
                    Some(LiteralValue::Boolean(true))
                } else if text.eq_ignore_ascii_case("FALSE") {
                    Some(LiteralValue::Boolean(false))
                } else {
                    None
                };
                if let Some(value) = literal {
                    self.bump();
                    return self.leaf(Expr::Literal(Literal {
                        value,
                        span: token.span,
                    }));
                }
                if self.peek_at(1).map(|t| &t.kind) == Some(&TokenKind::LParen)
                    && !RESERVED.iter().any(|r| r.eq_ignore_ascii_case(text))
                {
                    return self.call(token.span);
                }
                if RESERVED.iter().any(|r| r.eq_ignore_ascii_case(text)) {
                    return Err(self.unexpected("an expression"));
                }
                let name = self.qualified_name()?;
                self.leaf(Expr::Field(name))
            }
            _ => Err(self.unexpected("an expression")),
        }
    }

    fn call(&mut self, name_span: Span) -> Result<Parsed, Error> {
        self.bump();
        self.bump();
        let mut height = 0;
        let args = if self.eat(&TokenKind::Star).is_some() {
            CallArgs::Star
        } else if self.peek_kind() == Some(&TokenKind::RParen) {
            CallArgs::List(Vec::new())
        } else {
            let mut args = Vec::new();
            loop {
                let arg = self.expr_bp(0)?;
                height = height.max(arg.height);
                args.push(arg.expr);
                if self.eat(&TokenKind::Comma).is_none() {
                    break;
                }
            }
            CallArgs::List(args)
        };
        let close = self.expect(&TokenKind::RParen, "`,` or `)`")?;
        let expr = Expr::Call(Call {
            name: self.text(name_span).to_owned(),
            args,
            span: name_span.to(close),
        });
        self.node(expr, height, prec::PRIMARY)
    }

    /// A constant: an optionally signed number, a boolean or a string, and,
    /// when `scalar_only` is set, also `null` or a parameter.
    fn constant(&mut self, scalar_only: bool) -> Result<(Parsed, ConstantKind), Error> {
        let Some(token) = self.peek().cloned() else {
            return Err(self.unexpected("a constant"));
        };
        let sign = match token.kind {
            TokenKind::Minus => Some(UnaryOp::Minus),
            TokenKind::Plus => Some(UnaryOp::Plus),
            _ => None,
        };
        if let Some(op) = sign {
            self.bump();
            let Some(number) = self.peek().cloned() else {
                return Err(self.unexpected("a number"));
            };
            let value = match number.kind {
                TokenKind::Integer => LiteralValue::Integer(self.text(number.span).to_owned()),
                TokenKind::Decimal => LiteralValue::Decimal(self.text(number.span).to_owned()),
                _ => return Err(self.unexpected("a number")),
            };
            self.bump();
            let literal = self.leaf(Expr::Literal(Literal {
                value,
                span: number.span,
            }))?;
            let expr = Expr::Unary(Unary {
                op,
                operand: Box::new(literal.expr),
                span: token.span.to(number.span),
            });
            return Ok((
                self.node(expr, literal.height, prec::UNARY)?,
                ConstantKind::Number,
            ));
        }
        let (expr, kind) = match &token.kind {
            TokenKind::Integer => (
                Expr::Literal(Literal {
                    value: LiteralValue::Integer(self.text(token.span).to_owned()),
                    span: token.span,
                }),
                ConstantKind::Number,
            ),
            TokenKind::Decimal => (
                Expr::Literal(Literal {
                    value: LiteralValue::Decimal(self.text(token.span).to_owned()),
                    span: token.span,
                }),
                ConstantKind::Number,
            ),
            TokenKind::String(value) => (
                Expr::Literal(Literal {
                    value: LiteralValue::String(value.clone()),
                    span: token.span,
                }),
                ConstantKind::String,
            ),
            TokenKind::Word if self.token_is_word(Some(&token), "TRUE") => (
                Expr::Literal(Literal {
                    value: LiteralValue::Boolean(true),
                    span: token.span,
                }),
                ConstantKind::Boolean,
            ),
            TokenKind::Word if self.token_is_word(Some(&token), "FALSE") => (
                Expr::Literal(Literal {
                    value: LiteralValue::Boolean(false),
                    span: token.span,
                }),
                ConstantKind::Boolean,
            ),
            TokenKind::Word if scalar_only && self.token_is_word(Some(&token), "NULL") => (
                Expr::Literal(Literal {
                    value: LiteralValue::Null,
                    span: token.span,
                }),
                ConstantKind::Other,
            ),
            TokenKind::Param(name) if scalar_only => (
                Expr::Param(Parameter {
                    name: name.clone(),
                    identifier: false,
                    span: token.span,
                }),
                ConstantKind::Other,
            ),
            _ => return Err(self.unexpected("a constant")),
        };
        self.bump();
        Ok((self.leaf(expr)?, kind))
    }

    fn array(&mut self, open: Span) -> Result<Parsed, Error> {
        self.bump();
        let mut elements = Vec::new();
        let mut height = 0;
        let mut first_kind = None;
        loop {
            let at = self.pos;
            let (element, kind) = self.constant(false)?;
            match first_kind {
                None => first_kind = Some(kind),
                Some(k) if k != kind => {
                    self.pos = at;
                    return Err(self.unexpected("a constant of the same kind as the first"));
                }
                Some(_) => {}
            }
            height = height.max(element.height);
            elements.push(element.expr);
            if self.eat(&TokenKind::Comma).is_none() {
                break;
            }
        }
        let close = self.expect(&TokenKind::RBracket, "`,` or `]`")?;
        let expr = Expr::Array(ArrayLiteral {
            elements,
            span: open.to(close),
        });
        self.node(expr, height, prec::PRIMARY)
    }
}
