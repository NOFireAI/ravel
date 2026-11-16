//! Recursive-descent parser for ES|QL statements, with a Pratt parser for
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
//! source parenthesised it. An element of an `IN` list is a value or one
//! comparison; `IN`, `LIKE`, `RLIKE`, `IS NULL` and `:` need parentheses
//! there.
//!
//! Every descent into a nested expression level, and into a nested pipeline
//! (a `FORK` branch, a subquery among the sources of `FROM`, or the subquery
//! of `IN`), increments a depth counter that is checked against
//! [`Limits::max_depth`] before the level is parsed, and every node is
//! checked against the same limit as it is built; a nested pipeline is one
//! level taller than its tallest expression. Every command counts as a
//! pipeline stage against [`Limits::max_pipeline_stages`], the commands of
//! nested pipelines included.

use ravel_promql::complexity_guard::parse_guarded;

use crate::ast::{
    Aggregate, ArrayLiteral, Binary, BinaryOp, Call, CallArgs, Cast, ChangePointCommand,
    ChangePointOutput, Command, CompletionCommand, DissectCommand, DropCommand, EnrichCommand,
    EnrichField, EvalCommand, Expr, Field, ForkBranch, ForkCommand, FromCommand,
    FromQueriesCommand, FromSource, FuseCommand, FuseMethod, GrokCommand, InList, InSubquery,
    Interval, IsNull, KeepCommand, Like, LikeKind, LimitByCommand, LimitCommand, Literal,
    LiteralValue, LookupJoinCommand, MapEntry, MapLiteral, Metadata, MvExpandCommand, NamePart,
    NullsOrder, Parameter, PromqlCommand, PromqlError, PromqlParam, PromqlQuery, PromqlValue,
    QualifiedName, Query, RenameCommand, RenamePair, RerankCommand, RowCommand, SampleCommand,
    Setting, ShowCommand, SortCommand, SortKey, SortOrder, SourcePattern, StatsCommand, Subquery,
    Unary, UnaryOp, WhereCommand, interval_unit_kind, prec,
};
use crate::error::Error;
use crate::guard;
use crate::lexer::{Token, TokenKind, lex};
use crate::limits::{LimitKind, Limits};
use crate::span::Span;

/// Words that cannot be used as unquoted names.
const RESERVED: &[&str] = &[
    "AND", "AS", "ASC", "BY", "DESC", "FALSE", "IN", "IS", "LIKE", "NOT", "NULL", "NULLS", "OR",
    "RLIKE", "TRUE", "WHERE",
];

/// Source commands recognised by keyword and refused, each as the words that
/// name it. `EXPLAIN` and `METRICS` exist only in development builds of
/// Elasticsearch.
const UNSUPPORTED_SOURCE: &[&[&str]] = &[&["EXPLAIN"], &["METRICS"]];

/// Processing commands recognised by keyword and refused, each as the words
/// that name it. The first entry whose words all match is reported, so a
/// two-word form precedes a one-word form sharing its first word.
///
/// Development-only commands: `DEDUP`, `HIGHLIGHT`, the bare `LOOKUP`, and
/// the `JOIN`, `LEFT JOIN`, `RIGHT JOIN` and `FULL JOIN` forms. The others
/// (`IP_LOCATION`, `METRICS_INFO`, `REGISTERED_DOMAIN`, `TS_COLLAPSE`,
/// `TS_INFO`, `URI_PARTS`, `USER_AGENT`) are not in a released 9.5 language
/// reference this parser follows, so they are refused the same way.
const UNSUPPORTED_PROCESSING: &[&[&str]] = &[
    &["DEDUP"],
    &["FULL", "JOIN"],
    &["HIGHLIGHT"],
    &["IP_LOCATION"],
    &["JOIN"],
    &["LEFT", "JOIN"],
    &["LOOKUP"],
    &["METRICS_INFO"],
    &["REGISTERED_DOMAIN"],
    &["RIGHT", "JOIN"],
    &["TS_COLLAPSE"],
    &["TS_INFO"],
    &["URI_PARTS"],
    &["USER_AGENT"],
];

const FOUND_MAX_CHARS: usize = 40;

const SOURCE_EXPECTED: &str = "`SET` or a source command (`FROM`, `ROW`, `TS`, `SHOW` or `PROMQL`)";
const NESTED_SOURCE_EXPECTED: &str = "`FROM`, which starts a subquery";
const PROCESSING_EXPECTED: &str = "a processing command";

/// Parses one ES|QL statement.
///
/// Checks run in this order: the statement's byte length against
/// [`Limits::max_statement_bytes`]; lexing under [`Limits::max_tokens`];
/// bracket depth and pipeline stages over the tokens; then parsing, which
/// bounds operator and pipeline nesting by [`Limits::max_depth`] and counts
/// the commands of nested pipelines against
/// [`Limits::max_pipeline_stages`].
///
/// The query of a `PROMQL` source is handed, unsplit, to the guarded PromQL
/// parser; its refusal is kept in the tree rather than returned.
pub fn parse(source: &str, limits: &Limits) -> Result<Query, Error> {
    let tokens = lex(source, limits)?;
    guard::check(&tokens, limits)?;
    let mut parser = Parser {
        source,
        tokens,
        pos: 0,
        depth: 0,
        stages: 0,
        pipelines: 0,
        height: 0,
        ctx: Ctx::Where,
        limits,
    };
    parser.statement()
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

    /// Whether the operator builds a predicate an `IN` list element may not
    /// hold unparenthesised.
    fn is_predicate(&self) -> bool {
        matches!(
            self,
            Infix::In { .. } | Infix::Like { .. } | Infix::IsNull | Infix::Binary(BinaryOp::Match)
        )
    }
}

/// Whether an operator binding at `lbp` may take an unparenthesised left
/// operand whose outermost operator binds at `bound`: one that binds more
/// tightly, or as tightly when the operator chains. Comparisons do not chain.
fn takes_left_operand(lbp: u8, bound: u8) -> bool {
    bound > lbp || (bound == lbp && lbp != prec::COMPARISON)
}

/// Where the expression being parsed sits, which decides what may follow a
/// comparison-level operand that [`takes_left_operand`] refuses to extend.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Ctx {
    /// A `WHERE` condition.
    Where,
    /// A field of `ROW` or `EVAL`, or a `BY` grouping.
    Fields,
    /// A `STATS` aggregate, which a `WHERE` filter may follow.
    Aggregate,
    /// The `WHERE` filter of a `STATS` aggregate.
    AggregateFilter,
    /// A `SORT` key.
    Sort,
    /// Inside parentheses.
    Paren,
    /// A call argument.
    Args,
    /// An element of an `IN` list.
    InElement,
}

/// What [`takes_left_operand`] refusing an operator leaves admissible, given
/// the tokens that may follow the expression. Only a comparison-level left
/// operand is refused: every other operand was parsed at a binding power that
/// already consumed the tighter operators.
macro_rules! after_comparison {
    ($followers:literal) => {
        concat!(
            $followers,
            " (a comparison, `:`, `IN`, `LIKE`, `RLIKE` or `IS NULL` does not chain and is not \
             an operand of arithmetic or `::` unless parenthesised)"
        )
    };
}

const IN_ELEMENT_PREDICATE: &str = "`,` or `)` (an `IN` list element is a value or one \
     comparison; parenthesise an `IN`, `LIKE`, `RLIKE`, `IS NULL` or `:` there)";

/// What kind of constant an array holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ConstantKind {
    Number,
    Boolean,
    String,
    Other,
}

type CommandParser<'a> = fn(&mut Parser<'a>, Span) -> Result<Command, Error>;

struct Parser<'a> {
    source: &'a str,
    tokens: Vec<Token>,
    pos: usize,
    /// Expression levels and nested pipelines being parsed.
    depth: usize,
    /// Commands parsed so far, those of nested pipelines included.
    stages: usize,
    /// Nested pipelines being parsed.
    pipelines: usize,
    /// The tallest expression or nested pipeline of the current pipeline.
    height: usize,
    ctx: Ctx,
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

    /// Consumes the current token and returns its span.
    fn bump_span(&mut self) -> Span {
        let span = self.here();
        if self.pos < self.tokens.len() {
            self.pos += 1;
        }
        span
    }

    fn token_is_word(&self, token: Option<&Token>, keyword: &str) -> bool {
        token.is_some_and(|t| {
            t.kind == TokenKind::Word && self.text(t.span).eq_ignore_ascii_case(keyword)
        })
    }

    fn is_word(&self, keyword: &str) -> bool {
        self.token_is_word(self.peek(), keyword)
    }

    fn next_is_word(&self, keyword: &str) -> bool {
        self.token_is_word(self.peek_at(1), keyword)
    }

    fn eat_word(&mut self, keyword: &str) -> Option<Span> {
        if self.is_word(keyword) {
            Some(self.bump_span())
        } else {
            None
        }
    }

    /// Consumes the two words `first second`, returning their span.
    fn eat_words(&mut self, first: &str, second: &str) -> Option<Span> {
        if self.is_word(first) && self.next_is_word(second) {
            let start = self.bump_span();
            Some(start.to(self.bump_span()))
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
            Some(self.bump_span())
        } else {
            None
        }
    }

    fn expect(&mut self, kind: &TokenKind, expected: &'static str) -> Result<Span, Error> {
        self.eat(kind).ok_or_else(|| self.unexpected(expected))
    }

    fn at_command_end(&self) -> bool {
        matches!(
            self.peek_kind(),
            None | Some(TokenKind::Pipe | TokenKind::RParen)
        )
    }

    /// The span of the current token, or an empty span at the end of input.
    fn here(&self) -> Span {
        self.peek()
            .map_or(Span::new(self.source.len(), self.source.len()), |t| t.span)
    }

    #[inline(never)]
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

    /// What may follow the command just parsed.
    fn pipeline_followers(&self) -> &'static str {
        if self.pipelines > 0 {
            "`|` or `)`"
        } else {
            "`|` or end of input"
        }
    }

    fn after_comparison(&self) -> &'static str {
        let nested = self.pipelines > 0;
        match (self.ctx, nested) {
            (Ctx::Where, false) => after_comparison!("`AND`, `OR`, `|` or end of input"),
            (Ctx::Where, true) => after_comparison!("`AND`, `OR`, `|` or `)`"),
            (Ctx::Fields, false) => after_comparison!("`AND`, `OR`, `,`, `|` or end of input"),
            (Ctx::Fields, true) => after_comparison!("`AND`, `OR`, `,`, `|` or `)`"),
            (Ctx::Aggregate, false) => {
                after_comparison!("`AND`, `OR`, `WHERE`, `BY`, `,`, `|` or end of input")
            }
            (Ctx::Aggregate, true) => {
                after_comparison!("`AND`, `OR`, `WHERE`, `BY`, `,`, `|` or `)`")
            }
            (Ctx::AggregateFilter, false) => {
                after_comparison!("`AND`, `OR`, `BY`, `,`, `|` or end of input")
            }
            (Ctx::AggregateFilter, true) => {
                after_comparison!("`AND`, `OR`, `BY`, `,`, `|` or `)`")
            }
            (Ctx::Sort, false) => {
                after_comparison!("`AND`, `OR`, `ASC`, `DESC`, `NULLS`, `,`, `|` or end of input")
            }
            (Ctx::Sort, true) => {
                after_comparison!("`AND`, `OR`, `ASC`, `DESC`, `NULLS`, `,`, `|` or `)`")
            }
            (Ctx::Paren, _) => after_comparison!("`AND`, `OR` or `)`"),
            (Ctx::Args, _) => after_comparison!("`AND`, `OR`, `,` or `)`"),
            (Ctx::InElement, _) => after_comparison!("`,` or `)`"),
        }
    }

    /// Counts one more command against the pipeline stage limit.
    fn count_stage(&mut self, keyword: Span) -> Result<(), Error> {
        self.stages += 1;
        if self.stages > self.limits.max_pipeline_stages {
            return Err(Error::LimitExceeded {
                limit: LimitKind::PipelineStages,
                max: self.limits.max_pipeline_stages,
                span: keyword,
            });
        }
        Ok(())
    }

    // ---- statement, pipelines and nesting ----

    fn statement(&mut self) -> Result<Query, Error> {
        let mut settings = Vec::new();
        while self.is_word("SET") {
            settings.push(self.setting()?);
        }
        let mut query = self.pipeline()?;
        query.settings = settings;
        Ok(query)
    }

    /// `SET name = value;`
    fn setting(&mut self) -> Result<Setting, Error> {
        let start = self.bump_span();
        let name = self.name_part(false)?;
        self.expect(&TokenKind::Assign, "`=`")?;
        let value = if self.peek_kind() == Some(&TokenKind::LBrace) {
            self.map()?.expr
        } else {
            self.constant(true)?.0.expr
        };
        let end = self.expect(&TokenKind::Semicolon, "`;`")?;
        Ok(Setting {
            name,
            value,
            span: start.to(end),
        })
    }

    /// A source command and its processing commands, ending at the end of
    /// input, or at `)` inside a nested pipeline.
    fn pipeline(&mut self) -> Result<Query, Error> {
        match self.source_command() {
            Ok(source) => self.processing_commands(source),
            Err(error) => Err(error),
        }
    }

    fn processing_commands(&mut self, source: Command) -> Result<Query, Error> {
        let mut processing = Vec::new();
        while self.eat(&TokenKind::Pipe).is_some() {
            processing.push(self.processing_command()?);
        }
        self.expect_pipeline_end()?;
        let end = processing.last().map_or(source.span(), Command::span);
        Ok(Query {
            span: source.span().to(end),
            source,
            processing,
            settings: Vec::new(),
        })
    }

    fn expect_pipeline_end(&self) -> Result<(), Error> {
        let at_end = match self.peek_kind() {
            None => self.pipelines == 0,
            Some(TokenKind::RParen) => self.pipelines > 0,
            Some(_) => false,
        };
        if at_end {
            Ok(())
        } else {
            Err(self.unexpected(self.pipeline_followers()))
        }
    }

    /// Parses a nested pipeline with `parse`, one nesting level below the
    /// current one, after its opening `(`. Returns the result and its
    /// height: one more than its tallest expression.
    fn nested<T>(
        &mut self,
        open: Span,
        parse: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<(T, usize), Error> {
        self.depth += 1;
        if self.depth > self.limits.max_depth {
            return Err(self.depth_exceeded(open));
        }
        self.pipelines += 1;
        let outer_height = std::mem::replace(&mut self.height, 0);
        let outer_ctx = self.ctx;
        let result = parse(self);
        let inner_height = std::mem::replace(&mut self.height, outer_height);
        self.ctx = outer_ctx;
        self.pipelines -= 1;
        self.depth -= 1;
        match result {
            Ok(value) => self
                .nested_height(open, inner_height)
                .map(|height| (value, height)),
            Err(error) => Err(error),
        }
    }

    /// The height of a nested pipeline whose tallest expression is
    /// `inner_height`, raising the enclosing pipeline's height to it.
    fn nested_height(&mut self, open: Span, inner_height: usize) -> Result<usize, Error> {
        let height = inner_height + 1;
        if height > self.limits.max_depth {
            return Err(self.depth_exceeded(open));
        }
        self.height = self.height.max(height);
        Ok(height)
    }

    /// `(FROM ... | ...)`, from its `(`.
    fn subquery(&mut self) -> Result<(Subquery, usize), Error> {
        let open = self.bump_span();
        match self.nested(open, Self::boxed_pipeline) {
            Ok((query, height)) => self.close_subquery(open, query, height),
            Err(error) => Err(error),
        }
    }

    fn boxed_pipeline(&mut self) -> Result<Box<Query>, Error> {
        self.pipeline().map(Box::new)
    }

    fn close_subquery(
        &mut self,
        open: Span,
        query: Box<Query>,
        height: usize,
    ) -> Result<(Subquery, usize), Error> {
        let close = self.expect(&TokenKind::RParen, "`)`")?;
        let span = open.to(close);
        Ok((Subquery { query, span }, height))
    }

    /// The upper-cased keyword at the current token, if it is a word.
    fn keyword(&self) -> Option<(String, Span)> {
        let token = self.peek()?;
        (token.kind == TokenKind::Word)
            .then(|| (self.text(token.span).to_ascii_uppercase(), token.span))
    }

    fn source_command(&mut self) -> Result<Command, Error> {
        let expected = if self.pipelines > 0 {
            NESTED_SOURCE_EXPECTED
        } else {
            SOURCE_EXPECTED
        };
        let Some((keyword, span)) = self.keyword() else {
            return Err(self.unexpected(expected));
        };
        let parse: CommandParser<'a> = match (keyword.as_str(), self.pipelines > 0) {
            ("FROM", _) => Self::parse_from_command,
            ("ROW", false) => Self::row_command,
            ("TS", false) => Self::ts_command,
            ("SHOW", false) => Self::show_command,
            ("PROMQL", false) => Self::promql_command,
            (_, false) => {
                return Err(self
                    .unsupported(UNSUPPORTED_SOURCE)
                    .unwrap_or_else(|| self.unexpected(expected)));
            }
            (_, true) => return Err(self.unexpected(expected)),
        };
        self.count_stage(span)?;
        parse(self, span)
    }

    fn processing_command(&mut self) -> Result<Command, Error> {
        let Some((keyword, span)) = self.keyword() else {
            return Err(self.unexpected(PROCESSING_EXPECTED));
        };
        let parse: CommandParser<'a> = match keyword.as_str() {
            "WHERE" => Self::where_command,
            "EVAL" => Self::eval_command,
            "KEEP" => Self::keep_command,
            "DROP" => Self::drop_command,
            "RENAME" => Self::rename_command,
            "STATS" => Self::stats_command,
            "SORT" => Self::sort_command,
            "LIMIT" => Self::limit_command,
            "DISSECT" => Self::dissect_command,
            "GROK" => Self::grok_command,
            "MV_EXPAND" => Self::mv_expand_command,
            "LOOKUP" if self.next_is_word("JOIN") => Self::lookup_join_command,
            "ENRICH" => Self::enrich_command,
            "INLINE" if self.next_is_word("STATS") => Self::inline_stats_command,
            "INLINESTATS" => Self::inline_stats_command,
            "FORK" => Self::fork_command,
            "FUSE" => Self::fuse_command,
            "CHANGE_POINT" => Self::change_point_command,
            "SAMPLE" => Self::sample_command,
            "COMPLETION" => Self::completion_command,
            "RERANK" => Self::rerank_command,
            _ => {
                return Err(self
                    .unsupported(UNSUPPORTED_PROCESSING)
                    .unwrap_or_else(|| self.unexpected(PROCESSING_EXPECTED)));
            }
        };
        self.count_stage(span)?;
        parse(self, span)
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

    // ---- source commands ----

    fn parse_from_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        match self.parse_from_sources() {
            Ok((patterns, sources, end)) => self.close_from(keyword, patterns, sources, end),
            Err(error) => Err(error),
        }
    }

    /// The sources of `FROM`: patterns only, or, once a subquery appears,
    /// every source in order.
    fn parse_from_sources(&mut self) -> Result<(Vec<SourcePattern>, Vec<FromSource>, Span), Error> {
        let first = self.source_pattern()?;
        let mut end = first.span;
        let mut patterns = vec![first];
        let mut sources: Vec<FromSource> = Vec::new();
        while self.eat(&TokenKind::Comma).is_some() {
            let source = self.parse_from_source()?;
            end = match &source {
                FromSource::Pattern(pattern) => pattern.span,
                FromSource::Query(subquery) => subquery.span,
            };
            match source {
                FromSource::Pattern(pattern) if sources.is_empty() => patterns.push(pattern),
                source => {
                    if sources.is_empty() {
                        sources = patterns.drain(..).map(FromSource::Pattern).collect();
                    }
                    sources.push(source);
                }
            }
        }
        Ok((patterns, sources, end))
    }

    fn parse_from_source(&mut self) -> Result<FromSource, Error> {
        if self.peek_kind() == Some(&TokenKind::LParen) {
            self.subquery()
                .map(|(subquery, _)| FromSource::Query(subquery))
        } else {
            self.source_pattern().map(FromSource::Pattern)
        }
    }

    fn close_from(
        &mut self,
        keyword: Span,
        patterns: Vec<SourcePattern>,
        sources: Vec<FromSource>,
        mut end: Span,
    ) -> Result<Command, Error> {
        if !sources.is_empty() {
            if self.is_word("METADATA") {
                return Err(self.unexpected(if self.pipelines > 0 {
                    "`,`, `|` or `)` (`METADATA` is not admitted beside a subquery)"
                } else {
                    "`,`, `|` or end of input (`METADATA` is not admitted beside a subquery)"
                }));
            }
            return Ok(Command::FromQueries(FromQueriesCommand {
                sources,
                span: keyword.to(end),
            }));
        }
        let metadata = self.metadata()?;
        if let Some(m) = &metadata {
            end = m.span;
        }
        Ok(Command::From(FromCommand {
            sources: patterns,
            metadata,
            span: keyword.to(end),
        }))
    }

    fn ts_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let sources = self.comma_list(|p| p.source_pattern())?;
        let mut end = sources.last().map_or(keyword, |s| s.span);
        let metadata = self.metadata()?;
        if let Some(m) = &metadata {
            end = m.span;
        }
        Ok(Command::Ts(FromCommand {
            sources,
            metadata,
            span: keyword.to(end),
        }))
    }

    fn metadata(&mut self) -> Result<Option<Metadata>, Error> {
        let Some(start) = self.eat_word("METADATA") else {
            return Ok(None);
        };
        let fields = self.comma_list(|p| p.qualified_name())?;
        let span = start.to(fields.last().map_or(start, |f| f.span));
        Ok(Some(Metadata { fields, span }))
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

    /// One source name without wildcards: an `ENRICH` policy or a
    /// `LOOKUP JOIN` index.
    fn single_source(&mut self, expected: &'static str) -> Result<SourcePattern, Error> {
        let at = self.pos;
        let pattern = self
            .source_pattern()
            .map_err(|_| self.unexpected(expected))?;
        if pattern.text.contains('*') {
            self.pos = at;
            return Err(self.unexpected(expected));
        }
        Ok(pattern)
    }

    fn row_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let fields = self.fields()?;
        let end = fields.last().map_or(keyword, |f| f.span);
        Ok(Command::Row(RowCommand {
            fields,
            span: keyword.to(end),
        }))
    }

    fn show_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let info = self.expect_word("INFO", "`INFO`")?;
        Ok(Command::Show(ShowCommand {
            span: keyword.to(info),
        }))
    }

    fn promql_command(&mut self, keyword: Span) -> Result<Command, Error> {
        const EXPECTED: &str = "a `name = value` parameter or the parenthesised PromQL query";
        self.bump();
        let mut params = Vec::new();
        let region = loop {
            let Some(token) = self.peek().cloned() else {
                return Err(self.unexpected(EXPECTED));
            };
            match token.kind {
                TokenKind::PromqlRegion => {
                    self.bump();
                    break token.span;
                }
                TokenKind::SourcePattern if is_identifier(self.text(token.span)) => {
                    self.bump();
                    self.expect(&TokenKind::Assign, "`=`")?;
                    let value = self.promql_value()?;
                    let end = match &value {
                        PromqlValue::Text(text) => text.span,
                        PromqlValue::Param(param) => param.span,
                    };
                    params.push(PromqlParam {
                        name: self.text(token.span).to_owned(),
                        value,
                        span: token.span.to(end),
                    });
                }
                _ => return Err(self.unexpected(EXPECTED)),
            }
        };
        let inner = Span::new(region.start + 1, region.end.saturating_sub(1));
        let text = self.text(inner).to_owned();
        let error = promql_error(&text, inner);
        Ok(Command::Promql(Box::new(PromqlCommand {
            params,
            query: PromqlQuery {
                text,
                span: inner,
                error,
            },
            span: keyword.to(region),
        })))
    }

    fn promql_value(&mut self) -> Result<PromqlValue, Error> {
        if let Some(Token {
            kind: TokenKind::Param(name),
            span,
        }) = self.peek().cloned()
        {
            self.bump();
            return Ok(PromqlValue::Param(Parameter {
                name,
                identifier: false,
                span,
            }));
        }
        self.source_pattern()
            .map(PromqlValue::Text)
            .map_err(|_| self.unexpected("a parameter value"))
    }

    // ---- processing commands ----

    fn where_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        self.ctx = Ctx::Where;
        let condition = self.expr()?;
        Ok(Command::Where(WhereCommand {
            span: keyword.to(condition.span()),
            condition,
        }))
    }

    fn eval_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let fields = self.fields()?;
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

    /// `RENAME old AS new, ...` or `RENAME new = old, ...`, mixed freely.
    fn rename_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let renames = self.comma_list(|p| {
            let first = p.name_or_param()?;
            if p.eat(&TokenKind::Assign).is_some() {
                let old = p.name_or_param()?;
                return Ok(RenamePair {
                    span: first.span.to(old.span),
                    old,
                    new: first,
                });
            }
            p.expect_word("AS", "`AS` or `=`")?;
            let new = p.name_or_param()?;
            Ok(RenamePair {
                span: first.span.to(new.span),
                old: first,
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
        Ok(Command::Stats(self.stats_body(keyword)?))
    }

    /// `INLINE STATS ...` or `INLINESTATS ...`.
    fn inline_stats_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let keyword = match self.eat_word("STATS") {
            Some(stats) if self.token_is_word(self.tokens.get(self.pos - 2), "INLINE") => {
                keyword.to(stats)
            }
            Some(_) => {
                self.pos -= 1;
                keyword
            }
            None => keyword,
        };
        Ok(Command::InlineStats(self.stats_body(keyword)?))
    }

    fn stats_body(&mut self, keyword: Span) -> Result<StatsCommand, Error> {
        let mut end = keyword;
        let mut aggregates = Vec::new();
        if !self.at_command_end() && !self.is_word("BY") {
            aggregates = self.comma_list(|p| {
                p.ctx = Ctx::Aggregate;
                let field = p.field()?;
                let filter = match p.eat_word("WHERE") {
                    Some(_) => {
                        p.ctx = Ctx::AggregateFilter;
                        Some(p.expr()?)
                    }
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
            groupings = self.fields()?;
            end = groupings.last().map_or(end, |g| g.span);
        }
        if aggregates.is_empty() && groupings.is_empty() {
            return Err(self.unexpected("an aggregate or `BY`"));
        }
        Ok(StatsCommand {
            aggregates,
            groupings,
            span: keyword.to(end),
        })
    }

    fn sort_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let keys = self.comma_list(|p| {
            p.ctx = Ctx::Sort;
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

    /// `LIMIT n` or `LIMIT n BY fields`.
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
        if self.eat_word("BY").is_some() {
            let groupings = self.comma_list(|p| p.qualified_name())?;
            let end = groupings.last().map_or(span, |g| g.span);
            return Ok(Command::LimitBy(LimitByCommand {
                count,
                groupings,
                span: keyword.to(end),
            }));
        }
        Ok(Command::Limit(LimitCommand {
            count,
            span: keyword.to(span),
        }))
    }

    fn dissect_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let input = self.operand()?;
        let (pattern, mut end) = self.string("a dissect pattern string")?;
        let append_separator = if self.eat_word("APPEND_SEPARATOR").is_some() {
            self.expect(&TokenKind::Assign, "`=`")?;
            let (separator, span) = self.string("a separator string")?;
            end = span;
            Some(separator)
        } else {
            None
        };
        Ok(Command::Dissect(Box::new(DissectCommand {
            input,
            pattern,
            append_separator,
            span: keyword.to(end),
        })))
    }

    fn grok_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let input = self.operand()?;
        let (pattern, end) = self.string("a grok pattern string")?;
        Ok(Command::Grok(Box::new(GrokCommand {
            input,
            pattern,
            span: keyword.to(end),
        })))
    }

    fn mv_expand_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let field = self.name_or_param()?;
        Ok(Command::MvExpand(MvExpandCommand {
            span: keyword.to(field.span),
            field,
        }))
    }

    fn lookup_join_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let join = self.bump_span();
        let index = self.single_source("a lookup index name without `*`")?;
        self.expect_word("ON", "`ON`")?;
        let on = self.comma_list(|p| p.name_or_param())?;
        let end = on.last().map_or(join, |n| n.span);
        Ok(Command::LookupJoin(Box::new(LookupJoinCommand {
            index,
            on,
            span: keyword.to(end),
        })))
    }

    fn enrich_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let policy = self.single_source("an enrich policy name without `*`")?;
        let mut end = policy.span;
        let on = match self.eat_word("ON") {
            Some(_) => {
                let name = self.name_or_param()?;
                end = name.span;
                Some(name)
            }
            None => None,
        };
        let mut with = Vec::new();
        if self.eat_word("WITH").is_some() {
            with = self.comma_list(|p| {
                let first = p.name_or_param()?;
                if p.eat(&TokenKind::Assign).is_some() {
                    let field = p.name_or_param()?;
                    return Ok(EnrichField {
                        span: first.span.to(field.span),
                        name: Some(first),
                        field,
                    });
                }
                Ok(EnrichField {
                    span: first.span,
                    name: None,
                    field: first,
                })
            })?;
            end = with.last().map_or(end, |f| f.span);
        }
        Ok(Command::Enrich(Box::new(EnrichCommand {
            policy,
            on,
            with,
            span: keyword.to(end),
        })))
    }

    fn fork_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let mut branches = Vec::new();
        while self.peek_kind() == Some(&TokenKind::LParen) {
            let open = self.bump_span();
            let (commands, _) = self.nested(open, Self::fork_branch)?;
            let close = self.expect(&TokenKind::RParen, "`)`")?;
            branches.push(ForkBranch {
                commands,
                span: open.to(close),
            });
        }
        if branches.len() < 2 {
            return Err(self.unexpected("`(` opening a `FORK` branch; `FORK` takes at least two"));
        }
        let end = branches.last().map_or(keyword, |b| b.span);
        Ok(Command::Fork(ForkCommand {
            branches,
            span: keyword.to(end),
        }))
    }

    fn fork_branch(&mut self) -> Result<Vec<Command>, Error> {
        let mut commands = vec![self.processing_command()?];
        while self.eat(&TokenKind::Pipe).is_some() {
            commands.push(self.processing_command()?);
        }
        self.expect_pipeline_end()?;
        Ok(commands)
    }

    /// `FUSE [RRF | LINEAR] [SCORE BY f] [KEY BY f, ...] [GROUP BY f]
    /// [WITH options]`, its clauses in that order.
    fn fuse_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let mut end = keyword;
        let method = if let Some(s) = self.eat_word("RRF") {
            end = s;
            Some(FuseMethod::Rrf)
        } else if let Some(s) = self.eat_word("LINEAR") {
            end = s;
            Some(FuseMethod::Linear)
        } else {
            None
        };
        let score_by = match self.eat_words("SCORE", "BY") {
            Some(_) => {
                let name = self.qualified_name()?;
                end = name.span;
                Some(name)
            }
            None => None,
        };
        let mut key_by = Vec::new();
        if self.eat_words("KEY", "BY").is_some() {
            key_by = self.comma_list(|p| p.qualified_name())?;
            end = key_by.last().map_or(end, |n| n.span);
        }
        let group_by = match self.eat_words("GROUP", "BY") {
            Some(_) => {
                let name = self.qualified_name()?;
                end = name.span;
                Some(name)
            }
            None => None,
        };
        let options = match self.eat_word("WITH") {
            Some(_) => {
                let map = self.options()?;
                end = map.span;
                Some(map)
            }
            None => None,
        };
        Ok(Command::Fuse(Box::new(FuseCommand {
            method,
            score_by,
            key_by,
            group_by,
            options,
            span: keyword.to(end),
        })))
    }

    /// `CHANGE_POINT value [ON key] [AS type_name, pvalue_name]`
    fn change_point_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let value = self.qualified_name()?;
        let mut end = value.span;
        let key = match self.eat_word("ON") {
            Some(_) => {
                let name = self.qualified_name()?;
                end = name.span;
                Some(name)
            }
            None => None,
        };
        let output = match self.eat_word("AS") {
            Some(start) => {
                let type_name = self.qualified_name()?;
                self.expect(&TokenKind::Comma, "`,` and the p-value column")?;
                let pvalue_name = self.qualified_name()?;
                end = pvalue_name.span;
                Some(ChangePointOutput {
                    span: start.to(pvalue_name.span),
                    type_name,
                    pvalue_name,
                })
            }
            None => None,
        };
        Ok(Command::ChangePoint(Box::new(ChangePointCommand {
            value,
            key,
            output,
            span: keyword.to(end),
        })))
    }

    fn sample_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let Some(Token {
            kind: TokenKind::Decimal,
            span,
        }) = self.peek().cloned()
        else {
            return Err(self.unexpected("a decimal probability such as `0.25`"));
        };
        self.bump();
        Ok(Command::Sample(SampleCommand {
            probability: self.text(span).to_owned(),
            span: keyword.to(span),
        }))
    }

    /// `COMPLETION [target =] prompt WITH options`
    fn completion_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let target = self.target();
        let prompt = self.operand()?;
        self.expect_word("WITH", "`WITH` and an option map")?;
        let options = self.options()?;
        Ok(Command::Completion(Box::new(CompletionCommand {
            target,
            prompt,
            span: keyword.to(options.span),
            options,
        })))
    }

    /// `RERANK [target =] query ON fields WITH options`
    fn rerank_command(&mut self, keyword: Span) -> Result<Command, Error> {
        self.bump();
        let target = self.target();
        let query = match self.peek().cloned() {
            Some(Token {
                kind: TokenKind::String(value),
                span,
            }) => Expr::Literal(Literal {
                value: LiteralValue::String(value),
                span,
            }),
            Some(Token {
                kind: TokenKind::Param(name),
                span,
            }) => Expr::Param(Parameter {
                name,
                identifier: false,
                span,
            }),
            _ => return Err(self.unexpected("a query string or parameter")),
        };
        self.bump();
        self.expect_word("ON", "`ON`")?;
        let on = self.comma_list(|p| p.qualified_name())?;
        self.expect_word("WITH", "`,` or `WITH` and an option map")?;
        let options = self.options()?;
        Ok(Command::Rerank(Box::new(RerankCommand {
            target,
            query,
            on,
            span: keyword.to(options.span),
            options,
        })))
    }

    /// `target =` before a `COMPLETION` prompt or a `RERANK` query, when
    /// written.
    fn target(&mut self) -> Option<QualifiedName> {
        let save = self.pos;
        if let Ok(name) = self.name_or_param()
            && self.eat(&TokenKind::Assign).is_some()
        {
            return Some(name);
        }
        self.pos = save;
        None
    }

    /// The option map after `WITH`.
    fn options(&mut self) -> Result<MapLiteral, Error> {
        if self.peek_kind() != Some(&TokenKind::LBrace) {
            return Err(self.unexpected("an option map `{...}`"));
        }
        let parsed = self.map()?;
        self.height = self.height.max(parsed.height);
        match parsed.expr {
            Expr::Map(map) => Ok(map),
            _ => Err(self.unexpected("an option map `{...}`")),
        }
    }

    // ---- names, fields and strings ----

    fn string(&mut self, expected: &'static str) -> Result<(String, Span), Error> {
        match self.peek() {
            Some(Token {
                kind: TokenKind::String(value),
                span,
            }) => {
                let found = (value.clone(), *span);
                self.bump();
                Ok(found)
            }
            _ => Err(self.unexpected(expected)),
        }
    }

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
                    param: None,
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
                    param: None,
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
                    param: None,
                };
                self.bump();
                Ok(part)
            }
            _ => Err(self.unexpected("a name")),
        }
    }

    /// A dotted name. With `allow_param`, a parameter (`?name`, `??name`)
    /// may stand for the whole name.
    fn dotted_name(
        &mut self,
        allow_pattern: bool,
        allow_param: bool,
    ) -> Result<QualifiedName, Error> {
        if allow_param
            && let Some(Token {
                kind: kind @ (TokenKind::Param(_) | TokenKind::IdentParam(_)),
                span,
            }) = self.peek().cloned()
        {
            self.bump();
            let (name, identifier) = match kind {
                TokenKind::IdentParam(name) => (name, true),
                TokenKind::Param(name) => (name, false),
                _ => return Err(self.unexpected("a name")),
            };
            return Ok(QualifiedName {
                parts: vec![NamePart {
                    text: self.text(span).to_owned(),
                    quoted: false,
                    span,
                    param: Some(Parameter {
                        name,
                        identifier,
                        span,
                    }),
                }],
                span,
            });
        }
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
        self.dotted_name(false, false)
    }

    /// A name, or a parameter standing for one.
    fn name_or_param(&mut self) -> Result<QualifiedName, Error> {
        self.dotted_name(false, true)
    }

    /// A name pattern, or a parameter standing for one.
    fn name_pattern(&mut self) -> Result<QualifiedName, Error> {
        self.dotted_name(true, true)
    }

    fn fields(&mut self) -> Result<Vec<Field>, Error> {
        self.comma_list(|p| {
            p.ctx = Ctx::Fields;
            p.field()
        })
    }

    /// `name = expr` or `expr`.
    fn field(&mut self) -> Result<Field, Error> {
        let save = self.pos;
        if let Ok(name) = self.name_or_param()
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

    fn note_height(&mut self, parsed: Parsed) -> Expr {
        self.height = self.height.max(parsed.height);
        parsed.expr
    }

    fn expr(&mut self) -> Result<Expr, Error> {
        let parsed = self.expr_bp(0)?;
        Ok(self.note_height(parsed))
    }

    /// The input of `DISSECT` or `GROK` and the prompt of `COMPLETION`: an
    /// expression binding at least as tightly as a cast, so an operator or a
    /// sign needs parentheses there.
    fn operand(&mut self) -> Result<Expr, Error> {
        let parsed = self.expr_bp(prec::CAST)?;
        Ok(self.note_height(parsed))
    }

    /// Parses an expression whose operators bind at least as tightly as
    /// `min_bp`, one nesting level below the caller.
    fn expr_bp(&mut self, min_bp: u8) -> Result<Parsed, Error> {
        self.level(min_bp, false)
    }

    /// One nesting level below the caller; with `value_only`, the level is an
    /// `IN` list element and refuses the predicate operators.
    fn level(&mut self, min_bp: u8, value_only: bool) -> Result<Parsed, Error> {
        self.depth += 1;
        if self.depth > self.limits.max_depth {
            return Err(self.depth_exceeded(self.here()));
        }
        let result = self.expr_loop(min_bp, value_only);
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
            TokenKind::Word => return self.word_infix(token),
            _ => return None,
        };
        Some(Infix::Binary(op))
    }

    fn word_infix(&self, token: &Token) -> Option<Infix> {
        let word = self.text(token.span);
        let next = self.peek_at(1);
        if word.eq_ignore_ascii_case("OR") {
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
        }
    }

    // Every function on the recursive descent path keeps one `?` site and
    // returns its callee's result as is: an unoptimised build gives each
    // temporary its own stack slot, and these frames repeat per nesting level.
    fn expr_loop(&mut self, min_bp: u8, value_only: bool) -> Result<Parsed, Error> {
        let mut lhs = self.prefix(min_bp)?;
        loop {
            match self.next_infix(min_bp, value_only, lhs.bound) {
                Ok(Some(infix)) => lhs = self.infix_rhs(lhs, infix)?,
                Ok(None) => return Ok(lhs),
                Err(error) => return Err(error),
            }
        }
    }

    /// The infix operator that continues an operand bound at `bound`, if one
    /// binds at least as tightly as `min_bp`.
    fn next_infix(&self, min_bp: u8, value_only: bool, bound: u8) -> Result<Option<Infix>, Error> {
        let Some(infix) = self.infix() else {
            return Ok(None);
        };
        let lbp = infix.precedence();
        if lbp < min_bp {
            return Ok(None);
        }
        if value_only && infix.is_predicate() {
            return Err(self.unexpected(IN_ELEMENT_PREDICATE));
        }
        if !takes_left_operand(lbp, bound) {
            return Err(self.unexpected(self.after_comparison()));
        }
        Ok(Some(infix))
    }

    fn infix_rhs(&mut self, lhs: Parsed, infix: Infix) -> Result<Parsed, Error> {
        match infix {
            Infix::Binary(BinaryOp::Match) => self.match_rhs(lhs),
            Infix::Binary(op) => self.binary_rhs(lhs, op, op.precedence()),
            Infix::In { negated } => self.in_rhs(lhs, negated),
            Infix::Like { kind, negated } => self.like_rhs(lhs, kind, negated),
            Infix::IsNull => self.is_null_rhs(lhs),
            Infix::Cast => self.cast_rhs(lhs),
        }
    }

    fn binary_rhs(&mut self, lhs: Parsed, op: BinaryOp, lbp: u8) -> Result<Parsed, Error> {
        self.bump();
        match self.expr_bp(lbp + 1) {
            Ok(rhs) => self.binary_node(lhs, op, lbp, rhs),
            Err(error) => Err(error),
        }
    }

    fn binary_node(
        &self,
        lhs: Parsed,
        op: BinaryOp,
        lbp: u8,
        rhs: Parsed,
    ) -> Result<Parsed, Error> {
        let span = lhs.expr.span().to(rhs.expr.span());
        let height = lhs.height.max(rhs.height);
        let expr = Expr::Binary(Binary {
            op,
            left: Box::new(lhs.expr),
            right: Box::new(rhs.expr),
            span,
        });
        self.node(expr, height, lbp)
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
        if self.peek_kind() != Some(&TokenKind::LParen) {
            return Err(self.unexpected("`(`"));
        }
        // The first word alone decides, as in the lexer: `(from, b)` is
        // refused, since `(FROM a, b | ...)` only differs after the comma.
        if self.token_is_word(self.peek_at(1), "FROM") {
            return self.in_subquery_rhs(lhs, negated);
        }
        self.bump();
        match self.in_elements() {
            Ok((list, height)) => self.close_in(lhs, list, height, negated),
            Err(error) => Err(error),
        }
    }

    /// The elements after `IN (`, with the tallest one's height.
    fn in_elements(&mut self) -> Result<(Vec<Expr>, usize), Error> {
        let outer_ctx = std::mem::replace(&mut self.ctx, Ctx::InElement);
        let mut height = 0;
        let mut list = Vec::new();
        loop {
            let element = self.level(prec::COMPARISON, true)?;
            height = height.max(element.height);
            list.push(element.expr);
            if self.eat(&TokenKind::Comma).is_none() {
                break;
            }
        }
        self.ctx = outer_ctx;
        Ok((list, height))
    }

    fn close_in(
        &mut self,
        lhs: Parsed,
        list: Vec<Expr>,
        height: usize,
        negated: bool,
    ) -> Result<Parsed, Error> {
        let height = height.max(lhs.height);
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

    fn in_subquery_rhs(&mut self, lhs: Parsed, negated: bool) -> Result<Parsed, Error> {
        match self.subquery() {
            Ok((query, nested_height)) => self.in_subquery_node(lhs, negated, query, nested_height),
            Err(error) => Err(error),
        }
    }

    fn in_subquery_node(
        &self,
        lhs: Parsed,
        negated: bool,
        query: Subquery,
        nested_height: usize,
    ) -> Result<Parsed, Error> {
        let span = lhs.expr.span().to(query.span);
        let height = lhs.height.max(nested_height);
        let expr = Expr::InSubquery(InSubquery {
            expr: Box::new(lhs.expr),
            query,
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
        let (pattern, span) = self.string("a pattern string")?;
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
        match self.prefix_op(min_bp) {
            Ok(Some((op, bp))) => self.unary(op, bp),
            Ok(None) => self.primary(),
            Err(error) => Err(error),
        }
    }

    fn prefix_op(&self, min_bp: u8) -> Result<Option<(UnaryOp, u8)>, Error> {
        let Some(token) = self.peek() else {
            return Err(self.unexpected("an expression"));
        };
        Ok(match token.kind {
            TokenKind::Word if self.token_is_word(Some(token), "NOT") => {
                if min_bp > prec::NOT {
                    return Err(self.unexpected("an expression (parenthesise a `NOT` here)"));
                }
                Some((UnaryOp::Not, prec::NOT))
            }
            TokenKind::Minus | TokenKind::Plus if min_bp > prec::UNARY => {
                return Err(self.unexpected("an expression (parenthesise a signed value here)"));
            }
            TokenKind::Minus => Some((UnaryOp::Minus, prec::UNARY)),
            TokenKind::Plus => Some((UnaryOp::Plus, prec::UNARY)),
            _ => None,
        })
    }

    fn unary(&mut self, op: UnaryOp, bp: u8) -> Result<Parsed, Error> {
        let start = self.bump_span();
        let operand = self.expr_bp(bp)?;
        let span = start.to(operand.expr.span());
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
        let Some(token) = self.peek() else {
            return Err(self.unexpected("an expression"));
        };
        match token.kind {
            TokenKind::LParen => self.parenthesised(),
            TokenKind::LBracket => self.array(),
            TokenKind::Word
                if self.peek_at(1).map(|t| &t.kind) == Some(&TokenKind::LParen)
                    && !is_reserved(self.text(token.span)) =>
            {
                self.call()
            }
            _ => self.atom(),
        }
    }

    /// A primary that holds no nested expression.
    #[inline(never)]
    fn atom(&mut self) -> Result<Parsed, Error> {
        let Some(token) = self.peek().cloned() else {
            return Err(self.unexpected("an expression"));
        };
        match token.kind {
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
                    value: LiteralValue::String(value),
                    span: token.span,
                }))
            }
            TokenKind::Param(name) => {
                self.bump();
                self.leaf(Expr::Param(Parameter {
                    name,
                    identifier: false,
                    span: token.span,
                }))
            }
            TokenKind::IdentParam(name) => {
                self.bump();
                self.leaf(Expr::Param(Parameter {
                    name,
                    identifier: true,
                    span: token.span,
                }))
            }
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
                if is_reserved(text) {
                    return Err(self.unexpected("an expression"));
                }
                let name = self.qualified_name()?;
                self.leaf(Expr::Field(name))
            }
            _ => Err(self.unexpected("an expression")),
        }
    }

    fn parenthesised(&mut self) -> Result<Parsed, Error> {
        let open = self.bump_span();
        let outer_ctx = std::mem::replace(&mut self.ctx, Ctx::Paren);
        let inner = self.expr_bp(0);
        self.ctx = outer_ctx;
        match inner {
            Ok(inner) => self.close_paren(open, inner),
            Err(error) => Err(error),
        }
    }

    fn close_paren(&mut self, open: Span, mut inner: Parsed) -> Result<Parsed, Error> {
        let close = self.expect(&TokenKind::RParen, "`)`")?;
        *inner.expr.span_mut() = open.to(close);
        let height = inner.height;
        self.node(inner.expr, height, prec::PRIMARY)
    }

    fn call(&mut self) -> Result<Parsed, Error> {
        let name_span = self.bump_span();
        self.bump();
        match self.call_args() {
            Ok((args, height)) => self.close_call(name_span, args, height),
            Err(error) => Err(error),
        }
    }

    /// The arguments after `name(`, with the tallest one's height.
    fn call_args(&mut self) -> Result<(CallArgs, usize), Error> {
        if self.eat(&TokenKind::Star).is_some() {
            return Ok((CallArgs::Star, 0));
        }
        if self.peek_kind() == Some(&TokenKind::RParen) {
            return Ok((CallArgs::List(Vec::new()), 0));
        }
        let outer_ctx = std::mem::replace(&mut self.ctx, Ctx::Args);
        let mut height = 0;
        let mut args = Vec::new();
        loop {
            let arg = self.call_arg()?;
            height = height.max(arg.height);
            args.push(arg.expr);
            if self.eat(&TokenKind::Comma).is_none() {
                break;
            }
        }
        self.ctx = outer_ctx;
        Ok((CallArgs::List(args), height))
    }

    fn call_arg(&mut self) -> Result<Parsed, Error> {
        if self.peek_kind() == Some(&TokenKind::LBrace) {
            self.last_map_arg()
        } else {
            self.expr_bp(0)
        }
    }

    fn last_map_arg(&mut self) -> Result<Parsed, Error> {
        let map = self.map()?;
        if self.peek_kind() != Some(&TokenKind::RParen) {
            return Err(self.unexpected("`)` (an option map is the last argument)"));
        }
        Ok(map)
    }

    fn close_call(
        &mut self,
        name_span: Span,
        args: CallArgs,
        height: usize,
    ) -> Result<Parsed, Error> {
        let close = self.expect(&TokenKind::RParen, "`,` or `)`")?;
        let expr = Expr::Call(Call {
            name: self.text(name_span).to_owned(),
            args,
            span: name_span.to(close),
        });
        self.node(expr, height, prec::PRIMARY)
    }

    /// `{"key": value, ...}`, one nesting level below the caller. A value is
    /// a constant, an array of constants or a nested map.
    fn map(&mut self) -> Result<Parsed, Error> {
        self.depth += 1;
        if self.depth > self.limits.max_depth {
            return Err(self.depth_exceeded(self.here()));
        }
        let result = self.map_body();
        self.depth -= 1;
        result
    }

    fn map_body(&mut self) -> Result<Parsed, Error> {
        let open = self.bump_span();
        let mut entries: Vec<MapEntry> = Vec::new();
        let mut height = 0;
        loop {
            let (key, key_span) = self.string("a string key")?;
            if entries.iter().any(|e| e.key == key) {
                self.pos -= 1;
                return Err(self.unexpected("a key not already in this map"));
            }
            self.expect(&TokenKind::Colon, "`:`")?;
            let value = match self.peek_kind() {
                Some(TokenKind::LBrace) => self.map()?,
                Some(TokenKind::LBracket) => self.array()?,
                _ => self.constant(true)?.0,
            };
            height = height.max(value.height);
            entries.push(MapEntry {
                key,
                span: key_span.to(value.expr.span()),
                key_span,
                value: value.expr,
            });
            if self.eat(&TokenKind::Comma).is_none() {
                break;
            }
        }
        let close = self.expect(&TokenKind::RBrace, "`,` or `}`")?;
        let expr = Expr::Map(MapLiteral {
            entries,
            span: open.to(close),
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

    fn array(&mut self) -> Result<Parsed, Error> {
        let open = self.bump_span();
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

fn is_reserved(word: &str) -> bool {
    RESERVED.iter().any(|r| r.eq_ignore_ascii_case(word))
}

/// Whether `text` is a plain identifier: a letter or `_`, then letters,
/// digits and `_`.
fn is_identifier(text: &str) -> bool {
    let mut bytes = text.bytes();
    bytes
        .next()
        .is_some_and(|b| b.is_ascii_alphabetic() || b == b'_')
        && bytes.all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

/// Hands the text of a `PROMQL` query to the guarded PromQL parser and keeps
/// only its refusal, located at the query text.
#[inline(never)]
fn promql_error(text: &str, span: Span) -> Option<PromqlError> {
    parse_guarded(text).err().map(|error| PromqlError {
        message: error.to_string(),
        span,
    })
}
